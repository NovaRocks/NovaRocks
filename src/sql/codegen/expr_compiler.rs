use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::exprs;
use crate::lower::thrift::type_lowering::scalar_type_desc;
use crate::opcodes;
use crate::types;

use super::resolve::{ColumnBinding, ExprScope};
use super::type_infer::{arithmetic_result_type_with_op, arrow_type_to_type_desc, wider_type};
use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr, UnOp};
use crate::sql::planner::plan::AggregateCall;

/// Compiles sqlparser expressions into Thrift TExpr (flattened pre-order TExprNode list).
pub(crate) struct ExprCompiler<'a> {
    scope: &'a ExprScope,
    nodes: Vec<exprs::TExprNode>,
    last_type: DataType,
    last_nullable: bool,
}

impl<'a> ExprCompiler<'a> {
    pub fn new(scope: &'a ExprScope) -> Self {
        Self {
            scope,
            nodes: Vec::new(),
            last_type: DataType::Null,
            last_nullable: true,
        }
    }

    /// Compile a TypedExpr (from the analyzer/planner IR) into a TExpr.
    /// Unlike `compile()`, types are already resolved on the expression.
    pub fn compile_typed(&mut self, expr: &TypedExpr) -> Result<exprs::TExpr, String> {
        self.nodes.clear();
        self.compile_typed_inner(expr)?;
        Ok(exprs::TExpr::new(std::mem::take(&mut self.nodes)))
    }

    /// Compile an AggregateCall from the logical plan into a TExpr suitable
    /// for TAggregationNode.aggregate_functions.
    pub fn compile_aggregate_call_typed(
        &mut self,
        agg_call: &AggregateCall,
    ) -> Result<exprs::TExpr, String> {
        self.nodes.clear();

        let _is_count_star = agg_call.name == "count" && agg_call.args.is_empty();
        let is_distinct = agg_call.distinct;

        // Remap DISTINCT aggregates to their dedicated function names.
        // StarRocks execution layer uses separate functions for distinct agg.
        let effective_name = if is_distinct {
            match agg_call.name.as_str() {
                "count" => "multi_distinct_count".to_string(),
                "sum" => "multi_distinct_sum".to_string(),
                _ => agg_call.name.clone(),
            }
        } else {
            agg_call.name.clone()
        };

        let parent_idx = self.nodes.len();
        self.nodes.push(default_expr_node()); // placeholder

        let mut arg_types = Vec::new();
        for arg in &agg_call.args {
            self.compile_typed_inner(arg)?;
            arg_types.push(arg.data_type.clone());
        }
        let mut agg_input_types = arg_types.clone();

        // Compile ORDER BY expressions as additional children (for group_concat etc.)
        let num_order_by = agg_call.order_by.len();
        for ob in &agg_call.order_by {
            self.compile_typed_inner(&ob.expr)?;
            agg_input_types.push(ob.expr.data_type.clone());
        }

        let total_children = agg_call.args.len() + num_order_by;

        let return_type = agg_call.result_type.clone();
        let type_desc = semantic_aggregate_type_desc(&agg_call.name, &agg_call.args, &return_type)?;

        let (_, intermediate_type) =
            infer_agg_function_types(&effective_name, &agg_input_types, is_distinct)?;
        let intermediate_type_desc = match &intermediate_type {
            Some(it) => arrow_type_to_type_desc(it)?,
            None => types::TTypeDesc { types: None },
        };

        let fn_arg_types: Vec<types::TTypeDesc> = arg_types
            .iter()
            .map(arrow_type_to_type_desc)
            .collect::<Result<Vec<_>, _>>()?;

        self.nodes[parent_idx] = exprs::TExprNode {
            node_type: exprs::TExprNodeType::FUNCTION_CALL,
            type_: type_desc.clone(),
            num_children: total_children as i32,
            agg_expr: Some(exprs::TAggregateExpr {
                is_merge_agg: false,
            }),
            fn_: Some(types::TFunction {
                name: types::TFunctionName {
                    db_name: None,
                    function_name: effective_name,
                },
                binary_type: types::TFunctionBinaryType::BUILTIN,
                arg_types: fn_arg_types,
                ret_type: type_desc,
                has_var_args: false,
                comment: None,
                signature: None,
                hdfs_location: None,
                scalar_fn: None,
                aggregate_fn: Some({
                    let asc_order: Vec<bool> = agg_call.order_by.iter().map(|s| s.asc).collect();
                    let nf: Vec<bool> = agg_call.order_by.iter().map(|s| s.nulls_first).collect();
                    types::TAggregateFunction {
                        intermediate_type: intermediate_type_desc,
                        update_fn_symbol: None,
                        init_fn_symbol: None,
                        serialize_fn_symbol: None,
                        merge_fn_symbol: None,
                        finalize_fn_symbol: None,
                        get_value_fn_symbol: None,
                        remove_fn_symbol: None,
                        is_analytic_only_fn: None,
                        symbol: None,
                        is_asc_order: if asc_order.is_empty() {
                            None
                        } else {
                            Some(asc_order)
                        },
                        nulls_first: if nf.is_empty() { None } else { Some(nf) },
                        is_distinct: if is_distinct { Some(true) } else { None },
                    }
                }),
                id: None,
                checksum: None,
                agg_state_desc: None,
                fid: None,
                table_fn: None,
                could_apply_dict_optimize: None,
                ignore_nulls: None,
                isolated: None,
                input_type: None,
                content: None,
            }),
            ..default_expr_node()
        };
        self.last_type = return_type;
        self.last_nullable = true;
        Ok(exprs::TExpr::new(std::mem::take(&mut self.nodes)))
    }

    /// Compile a merge-phase aggregate call for two-phase distributed aggregation.
    ///
    /// Instead of compiling the original args, this generates a single SlotRef
    /// child pointing to the intermediate column from the Local phase's output.
    /// The root node has `is_merge_agg: true` so the execution layer calls
    /// merge+finalize instead of update+serialize.
    pub fn compile_merge_aggregate_call(
        &mut self,
        agg_call: &AggregateCall,
        input_slot_id: i32,
        input_tuple_id: i32,
        input_type: &DataType,
    ) -> Result<exprs::TExpr, String> {
        self.nodes.clear();

        let is_distinct = agg_call.distinct;
        let effective_name = if is_distinct {
            match agg_call.name.as_str() {
                "count" => "multi_distinct_count".to_string(),
                "sum" => "multi_distinct_sum".to_string(),
                _ => agg_call.name.clone(),
            }
        } else {
            agg_call.name.clone()
        };

        let parent_idx = self.nodes.len();
        self.nodes.push(default_expr_node()); // placeholder

        // Single child: SlotRef to the intermediate column from Local phase.
        let input_type_desc = arrow_type_to_type_desc(input_type)?;
        self.nodes.push(slot_ref_node(
            input_slot_id,
            input_tuple_id,
            input_type_desc,
        ));

        let arg_types = if agg_call.args.is_empty() {
            // count(*): no original args, but merge needs the intermediate type
            vec![input_type.clone()]
        } else {
            agg_call.args.iter().map(|a| a.data_type.clone()).collect()
        };
        let mut agg_input_types = arg_types.clone();
        agg_input_types.extend(agg_call.order_by.iter().map(|ob| ob.expr.data_type.clone()));

        let return_type = agg_call.result_type.clone();
        let type_desc = semantic_aggregate_type_desc(&agg_call.name, &agg_call.args, &return_type)?;

        let (_, intermediate_type) =
            infer_agg_function_types(&effective_name, &agg_input_types, is_distinct)?;
        let intermediate_type_desc = match &intermediate_type {
            Some(it) => arrow_type_to_type_desc(it)?,
            None => types::TTypeDesc { types: None },
        };

        let fn_arg_types: Vec<types::TTypeDesc> = arg_types
            .iter()
            .map(arrow_type_to_type_desc)
            .collect::<Result<Vec<_>, _>>()?;

        self.nodes[parent_idx] = exprs::TExprNode {
            node_type: exprs::TExprNodeType::FUNCTION_CALL,
            type_: type_desc.clone(),
            num_children: 1, // single SlotRef child
            agg_expr: Some(exprs::TAggregateExpr { is_merge_agg: true }),
            fn_: Some(types::TFunction {
                name: types::TFunctionName {
                    db_name: None,
                    function_name: effective_name,
                },
                binary_type: types::TFunctionBinaryType::BUILTIN,
                arg_types: fn_arg_types,
                ret_type: type_desc,
                has_var_args: false,
                comment: None,
                signature: None,
                hdfs_location: None,
                scalar_fn: None,
                aggregate_fn: Some(types::TAggregateFunction {
                    intermediate_type: intermediate_type_desc,
                    update_fn_symbol: None,
                    init_fn_symbol: None,
                    serialize_fn_symbol: None,
                    merge_fn_symbol: None,
                    finalize_fn_symbol: None,
                    get_value_fn_symbol: None,
                    remove_fn_symbol: None,
                    is_analytic_only_fn: None,
                    symbol: None,
                    is_asc_order: if agg_call.order_by.is_empty() {
                        None
                    } else {
                        Some(agg_call.order_by.iter().map(|s| s.asc).collect())
                    },
                    nulls_first: if agg_call.order_by.is_empty() {
                        None
                    } else {
                        Some(agg_call.order_by.iter().map(|s| s.nulls_first).collect())
                    },
                    is_distinct: if is_distinct { Some(true) } else { None },
                }),
                id: None,
                checksum: None,
                agg_state_desc: None,
                fid: None,
                table_fn: None,
                could_apply_dict_optimize: None,
                ignore_nulls: None,
                isolated: None,
                input_type: None,
                content: None,
            }),
            ..default_expr_node()
        };
        self.last_type = return_type;
        self.last_nullable = true;
        Ok(exprs::TExpr::new(std::mem::take(&mut self.nodes)))
    }

    /// Compile an expression, inserting a CAST wrapper if its type differs from the target.
    fn compile_with_cast_if_needed(
        &mut self,
        expr: &TypedExpr,
        target_type: &DataType,
    ) -> Result<DataType, String> {
        if expr.data_type != *target_type && needs_comparison_cast(&expr.data_type, target_type) {
            let cast_type_desc = arrow_type_to_type_desc(target_type)?;
            self.nodes.push(exprs::TExprNode {
                node_type: exprs::TExprNodeType::CAST_EXPR,
                type_: cast_type_desc,
                num_children: 1,
                opcode: None,
                ..default_expr_node()
            });
            self.compile_typed_inner(expr)?;
            Ok(target_type.clone())
        } else {
            self.compile_typed_inner(expr)
        }
    }

    fn compile_typed_inner(&mut self, expr: &TypedExpr) -> Result<DataType, String> {
        match &expr.kind {
            ExprKind::ColumnRef { qualifier, column } => {
                let binding = self.scope.resolve_column(qualifier.as_deref(), column)?;
                let type_desc = binding_type_desc(binding)?;
                self.nodes
                    .push(slot_ref_node(binding.slot_id, binding.tuple_id, type_desc));
                self.last_type = binding.data_type.clone();
                self.last_nullable = binding.nullable;
                Ok(binding.data_type.clone())
            }
            ExprKind::Literal(lit) => self.compile_literal(lit, &expr.data_type),
            ExprKind::BinaryOp { left, op, right } => {
                self.compile_typed_binary_op(left, *op, right)
            }
            ExprKind::UnaryOp { op, expr: inner } => match op {
                UnOp::Not => {
                    let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                    self.nodes.push(exprs::TExprNode {
                        node_type: exprs::TExprNodeType::COMPOUND_PRED,
                        type_: type_desc,
                        opcode: Some(opcodes::TExprOpcode::COMPOUND_NOT),
                        num_children: 1,
                        ..default_expr_node()
                    });
                    self.compile_typed_inner(inner)?;
                    self.last_type = DataType::Boolean;
                    Ok(DataType::Boolean)
                }
                UnOp::Negate => {
                    let result_type = inner.data_type.clone();
                    let type_desc = arrow_type_to_type_desc(&result_type)?;
                    self.nodes.push(exprs::TExprNode {
                        node_type: exprs::TExprNodeType::ARITHMETIC_EXPR,
                        type_: type_desc,
                        opcode: Some(opcodes::TExprOpcode::MULTIPLY),
                        num_children: 2,
                        ..default_expr_node()
                    });
                    self.nodes.push(int_literal_node(-1));
                    self.compile_typed_inner(inner)?;
                    self.last_type = result_type.clone();
                    Ok(result_type)
                }
                UnOp::BitwiseNot => {
                    // Emit as bitnot(expr) function call
                    let result_type = inner.data_type.clone();
                    let type_desc = arrow_type_to_type_desc(&result_type)?;
                    self.nodes.push(exprs::TExprNode {
                        node_type: exprs::TExprNodeType::FUNCTION_CALL,
                        type_: type_desc.clone(),
                        num_children: 1,
                        fn_: Some(types::TFunction {
                            name: types::TFunctionName {
                                db_name: None,
                                function_name: "bitnot".to_string(),
                            },
                            binary_type: types::TFunctionBinaryType::BUILTIN,
                            arg_types: vec![type_desc.clone()],
                            ret_type: type_desc,
                            has_var_args: false,
                            comment: None,
                            signature: None,
                            hdfs_location: None,
                            scalar_fn: None,
                            aggregate_fn: None,
                            id: None,
                            checksum: None,
                            agg_state_desc: None,
                            fid: None,
                            table_fn: None,
                            could_apply_dict_optimize: None,
                            ignore_nulls: None,
                            isolated: None,
                            input_type: None,
                            content: None,
                        }),
                        ..default_expr_node()
                    });
                    self.compile_typed_inner(inner)?;
                    self.last_type = result_type.clone();
                    Ok(result_type)
                }
            },
            ExprKind::IsNull {
                expr: inner,
                negated,
            } => {
                let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                self.nodes.push(exprs::TExprNode {
                    node_type: exprs::TExprNodeType::IS_NULL_PRED,
                    type_: type_desc,
                    num_children: 1,
                    is_null_pred: Some(exprs::TIsNullPredicate {
                        is_not_null: *negated,
                    }),
                    ..default_expr_node()
                });
                self.compile_typed_inner(inner)?;
                self.last_type = DataType::Boolean;
                Ok(DataType::Boolean)
            }
            ExprKind::InList {
                expr: inner,
                list,
                negated,
            } => {
                let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                self.nodes.push(exprs::TExprNode {
                    node_type: exprs::TExprNodeType::IN_PRED,
                    type_: type_desc,
                    num_children: (1 + list.len()) as i32,
                    in_predicate: Some(exprs::TInPredicate {
                        is_not_in: *negated,
                    }),
                    ..default_expr_node()
                });
                self.compile_typed_inner(inner)?;
                for item in list {
                    self.compile_typed_inner(item)?;
                }
                self.last_type = DataType::Boolean;
                Ok(DataType::Boolean)
            }
            ExprKind::Between {
                expr: inner,
                low,
                high,
                negated,
            } => {
                if *negated {
                    let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                    self.nodes.push(exprs::TExprNode {
                        node_type: exprs::TExprNodeType::COMPOUND_PRED,
                        type_: type_desc.clone(),
                        opcode: Some(opcodes::TExprOpcode::COMPOUND_OR),
                        num_children: 2,
                        ..default_expr_node()
                    });
                    // LT: expr < low
                    let lt_child_type = wider_type(&inner.data_type, &low.data_type);
                    let lt_idx = self.nodes.len();
                    self.nodes.push(default_expr_node());
                    self.compile_with_cast_if_needed(inner, &lt_child_type)?;
                    self.compile_with_cast_if_needed(low, &lt_child_type)?;
                    self.nodes[lt_idx] = exprs::TExprNode {
                        node_type: exprs::TExprNodeType::BINARY_PRED,
                        type_: type_desc.clone(),
                        opcode: Some(opcodes::TExprOpcode::LT),
                        num_children: 2,
                        child_type_desc: arrow_type_to_type_desc(&lt_child_type).ok(),
                        ..default_expr_node()
                    };
                    // GT: expr > high
                    let gt_child_type = wider_type(&inner.data_type, &high.data_type);
                    let gt_idx = self.nodes.len();
                    self.nodes.push(default_expr_node());
                    self.compile_with_cast_if_needed(inner, &gt_child_type)?;
                    self.compile_with_cast_if_needed(high, &gt_child_type)?;
                    self.nodes[gt_idx] = exprs::TExprNode {
                        node_type: exprs::TExprNodeType::BINARY_PRED,
                        type_: type_desc,
                        opcode: Some(opcodes::TExprOpcode::GT),
                        num_children: 2,
                        child_type_desc: arrow_type_to_type_desc(&gt_child_type).ok(),
                        ..default_expr_node()
                    };
                } else {
                    let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                    self.nodes.push(exprs::TExprNode {
                        node_type: exprs::TExprNodeType::COMPOUND_PRED,
                        type_: type_desc.clone(),
                        opcode: Some(opcodes::TExprOpcode::COMPOUND_AND),
                        num_children: 2,
                        ..default_expr_node()
                    });
                    // GE: expr >= low
                    let ge_child_type = wider_type(&inner.data_type, &low.data_type);
                    let ge_idx = self.nodes.len();
                    self.nodes.push(default_expr_node());
                    self.compile_with_cast_if_needed(inner, &ge_child_type)?;
                    self.compile_with_cast_if_needed(low, &ge_child_type)?;
                    self.nodes[ge_idx] = exprs::TExprNode {
                        node_type: exprs::TExprNodeType::BINARY_PRED,
                        type_: type_desc.clone(),
                        opcode: Some(opcodes::TExprOpcode::GE),
                        num_children: 2,
                        child_type_desc: arrow_type_to_type_desc(&ge_child_type).ok(),
                        ..default_expr_node()
                    };
                    // LE: expr <= high
                    let le_child_type = wider_type(&inner.data_type, &high.data_type);
                    let le_idx = self.nodes.len();
                    self.nodes.push(default_expr_node());
                    self.compile_with_cast_if_needed(inner, &le_child_type)?;
                    self.compile_with_cast_if_needed(high, &le_child_type)?;
                    self.nodes[le_idx] = exprs::TExprNode {
                        node_type: exprs::TExprNodeType::BINARY_PRED,
                        type_: type_desc,
                        opcode: Some(opcodes::TExprOpcode::LE),
                        num_children: 2,
                        child_type_desc: arrow_type_to_type_desc(&le_child_type).ok(),
                        ..default_expr_node()
                    };
                }
                self.last_type = DataType::Boolean;
                Ok(DataType::Boolean)
            }
            ExprKind::Like {
                expr: inner,
                pattern,
                negated,
            } => {
                let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                // For NOT LIKE, wrap the LIKE_PRED in a COMPOUND_NOT node
                if *negated {
                    self.nodes.push(exprs::TExprNode {
                        node_type: exprs::TExprNodeType::COMPOUND_PRED,
                        type_: type_desc.clone(),
                        opcode: Some(opcodes::TExprOpcode::COMPOUND_NOT),
                        num_children: 1,
                        ..default_expr_node()
                    });
                }
                self.nodes.push(exprs::TExprNode {
                    node_type: exprs::TExprNodeType::LIKE_PRED,
                    type_: type_desc,
                    opcode: None,
                    num_children: 2,
                    like_pred: Some(exprs::TLikePredicate {
                        escape_char: "\\".to_string(),
                    }),
                    ..default_expr_node()
                });
                self.compile_typed_inner(inner)?;
                self.compile_typed_inner(pattern)?;
                self.last_type = DataType::Boolean;
                Ok(DataType::Boolean)
            }
            ExprKind::Cast {
                expr: inner,
                target,
            } => {
                let type_desc = arrow_type_to_type_desc(target)?;
                self.nodes.push(exprs::TExprNode {
                    node_type: exprs::TExprNodeType::CAST_EXPR,
                    type_: type_desc,
                    num_children: 1,
                    ..default_expr_node()
                });
                self.compile_typed_inner(inner)?;
                self.last_type = target.clone();
                Ok(target.clone())
            }
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                let has_operand = operand.is_some();
                let has_else = else_expr.is_some();
                let num_children = if has_operand { 1 } else { 0 }
                    + when_then.len() * 2
                    + if has_else { 1 } else { 0 };

                let parent_idx = self.nodes.len();
                self.nodes.push(default_expr_node()); // placeholder

                if let Some(op) = operand {
                    self.compile_typed_inner(op)?;
                }
                let mut result_type = DataType::Null;
                for (when, then) in when_then {
                    self.compile_typed_inner(when)?;
                    let t = self.compile_typed_inner(then)?;
                    if result_type == DataType::Null {
                        result_type = t;
                    }
                }
                if let Some(el) = else_expr {
                    let t = self.compile_typed_inner(el)?;
                    if result_type == DataType::Null {
                        result_type = t;
                    }
                }
                if result_type == DataType::Null {
                    result_type = expr.data_type.clone();
                }

                let type_desc = arrow_type_to_type_desc(&result_type)?;
                self.nodes[parent_idx] = exprs::TExprNode {
                    node_type: exprs::TExprNodeType::CASE_EXPR,
                    type_: type_desc,
                    num_children: num_children as i32,
                    case_expr: Some(exprs::TCaseExpr {
                        has_case_expr: has_operand,
                        has_else_expr: has_else,
                    }),
                    ..default_expr_node()
                };
                self.last_type = result_type.clone();
                self.last_nullable = true;
                Ok(result_type)
            }
            ExprKind::FunctionCall {
                name,
                args,
                distinct: _,
            } => {
                // In a project-over-aggregate context, the scope may have a
                // GROUP BY expression registered by display name (e.g. "mod(k, Int(2))").
                // Try scope lookup first to emit a slot ref instead of recompiling.
                use crate::sql::codegen::helpers::typed_expr_display_name;
                let display = typed_expr_display_name(expr);
                if let Ok(binding) = self.scope.resolve_column(None, &display) {
                    let type_desc = binding_type_desc(binding)?;
                    self.nodes
                        .push(slot_ref_node(binding.slot_id, binding.tuple_id, type_desc));
                    self.last_type = binding.data_type.clone();
                    self.last_nullable = binding.nullable;
                    Ok(binding.data_type.clone())
                } else {
                    // Use the analyzer's data_type as override hint for the
                    // return type. This handles cases like round(decimal, 2)
                    // where the analyzer computed Decimal128(38, 2) but the
                    // physical layer would re-infer Decimal128(38, 8) from
                    // the input type alone.
                    self.compile_typed_function_call_with_hint(name, args, &expr.data_type)
                }
            }
            ExprKind::AggregateCall {
                name,
                args,
                distinct,
                order_by,
            } => {
                // In a project-over-aggregate context, the scope has aggregate
                // output columns registered by display name. Try to look up
                // as a slot reference first.
                let display = super::helpers::agg_call_display_name_from_parts(
                    name, args, *distinct, order_by,
                );
                if let Ok(binding) = self.scope.resolve_column(None, &display) {
                    let type_desc = binding_type_desc(binding)?;
                    self.nodes
                        .push(slot_ref_node(binding.slot_id, binding.tuple_id, type_desc));
                    self.last_type = binding.data_type.clone();
                    self.last_nullable = binding.nullable;
                    Ok(binding.data_type.clone())
                } else {
                    // Fallback: compile as function call (scan-scope context)
                    self.compile_typed_function_call(name, args)
                }
            }
            ExprKind::IsTruthValue {
                expr: inner,
                value,
                negated,
            } => {
                if *value && !negated {
                    // IS TRUE => just compile the inner expression
                    self.compile_typed_inner(inner)
                } else {
                    // IS FALSE, IS NOT TRUE, IS NOT FALSE => NOT(inner) or NOT(NOT(inner))
                    let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                    self.nodes.push(exprs::TExprNode {
                        node_type: exprs::TExprNodeType::COMPOUND_PRED,
                        type_: type_desc,
                        opcode: Some(opcodes::TExprOpcode::COMPOUND_NOT),
                        num_children: 1,
                        ..default_expr_node()
                    });
                    self.compile_typed_inner(inner)?;
                    self.last_type = DataType::Boolean;
                    Ok(DataType::Boolean)
                }
            }
            ExprKind::Nested(inner) => self.compile_typed_inner(inner),
            ExprKind::WindowCall { name, args: _, .. } => {
                // Window calls should not appear here — they are compiled
                // separately via compile_aggregate_call_typed in emit_window.
                Err(format!(
                    "unexpected window function call in expression context: {name}"
                ))
            }
            ExprKind::SubqueryPlaceholder { id, .. } => {
                // SubqueryPlaceholder should have been rewritten to JOINs by the
                // analyzer before reaching the physical compilation stage.
                Err(format!(
                    "unexpected SubqueryPlaceholder (id={id}) in expression compilation; \
                     subquery rewriting may have failed"
                ))
            }
        }
    }

    fn compile_typed_binary_op(
        &mut self,
        left: &TypedExpr,
        op: BinOp,
        right: &TypedExpr,
    ) -> Result<DataType, String> {
        match op {
            // Comparison operators
            BinOp::Eq
            | BinOp::Ne
            | BinOp::Lt
            | BinOp::Le
            | BinOp::Gt
            | BinOp::Ge
            | BinOp::EqForNull => {
                let opcode = match op {
                    BinOp::Eq => opcodes::TExprOpcode::EQ,
                    BinOp::Ne => opcodes::TExprOpcode::NE,
                    BinOp::Lt => opcodes::TExprOpcode::LT,
                    BinOp::Le => opcodes::TExprOpcode::LE,
                    BinOp::Gt => opcodes::TExprOpcode::GT,
                    BinOp::Ge => opcodes::TExprOpcode::GE,
                    BinOp::EqForNull => opcodes::TExprOpcode::EQ_FOR_NULL,
                    _ => unreachable!(),
                };
                let compare_type = wider_type(&left.data_type, &right.data_type);
                let parent_idx = self.nodes.len();
                self.nodes.push(default_expr_node()); // placeholder

                // Compile left, inserting cast if needed
                if left.data_type != compare_type
                    && needs_comparison_cast(&left.data_type, &compare_type)
                {
                    let cast_type_desc = arrow_type_to_type_desc(&compare_type)?;
                    self.nodes.push(exprs::TExprNode {
                        node_type: exprs::TExprNodeType::CAST_EXPR,
                        type_: cast_type_desc,
                        num_children: 1,
                        opcode: None,
                        ..default_expr_node()
                    });
                }
                self.compile_typed_inner(left)?;

                // Compile right, inserting cast if needed
                if right.data_type != compare_type
                    && needs_comparison_cast(&right.data_type, &compare_type)
                {
                    let cast_type_desc = arrow_type_to_type_desc(&compare_type)?;
                    self.nodes.push(exprs::TExprNode {
                        node_type: exprs::TExprNodeType::CAST_EXPR,
                        type_: cast_type_desc,
                        num_children: 1,
                        opcode: None,
                        ..default_expr_node()
                    });
                }
                self.compile_typed_inner(right)?;

                let child_type_desc = arrow_type_to_type_desc(&compare_type).ok();
                let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                self.nodes[parent_idx] = exprs::TExprNode {
                    node_type: exprs::TExprNodeType::BINARY_PRED,
                    type_: type_desc,
                    opcode: Some(opcode),
                    num_children: 2,
                    child_type_desc,
                    ..default_expr_node()
                };
                self.last_type = DataType::Boolean;
                self.last_nullable = false;
                Ok(DataType::Boolean)
            }
            // Logical operators
            BinOp::And => {
                let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                self.nodes.push(exprs::TExprNode {
                    node_type: exprs::TExprNodeType::COMPOUND_PRED,
                    type_: type_desc,
                    opcode: Some(opcodes::TExprOpcode::COMPOUND_AND),
                    num_children: 2,
                    ..default_expr_node()
                });
                self.compile_typed_inner(left)?;
                self.compile_typed_inner(right)?;
                self.last_type = DataType::Boolean;
                Ok(DataType::Boolean)
            }
            BinOp::Or => {
                let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                self.nodes.push(exprs::TExprNode {
                    node_type: exprs::TExprNodeType::COMPOUND_PRED,
                    type_: type_desc,
                    opcode: Some(opcodes::TExprOpcode::COMPOUND_OR),
                    num_children: 2,
                    ..default_expr_node()
                });
                self.compile_typed_inner(left)?;
                self.compile_typed_inner(right)?;
                self.last_type = DataType::Boolean;
                Ok(DataType::Boolean)
            }
            // Arithmetic operators
            BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
                let opcode = match op {
                    BinOp::Add => opcodes::TExprOpcode::ADD,
                    BinOp::Sub => opcodes::TExprOpcode::SUBTRACT,
                    BinOp::Mul => opcodes::TExprOpcode::MULTIPLY,
                    BinOp::Div => opcodes::TExprOpcode::DIVIDE,
                    BinOp::Mod => opcodes::TExprOpcode::MOD,
                    _ => unreachable!(),
                };
                // Use op-aware result type for correct Decimal precision/scale.
                let op_str = match op {
                    BinOp::Mul => "mul",
                    BinOp::Div => "div",
                    _ => "add",
                };
                let result_type =
                    arithmetic_result_type_with_op(&left.data_type, &right.data_type, op_str);

                let parent_idx = self.nodes.len();
                self.nodes.push(default_expr_node()); // placeholder

                // Compile left, wrapping with implicit CAST if needed
                if needs_arithmetic_cast(&left.data_type, &result_type) {
                    let cast_type_desc = arrow_type_to_type_desc(&result_type)?;
                    self.nodes.push(exprs::TExprNode {
                        node_type: exprs::TExprNodeType::CAST_EXPR,
                        type_: cast_type_desc,
                        num_children: 1,
                        ..default_expr_node()
                    });
                }
                self.compile_typed_inner(left)?;

                // Compile right, wrapping with implicit CAST if needed
                if needs_arithmetic_cast(&right.data_type, &result_type) {
                    let cast_type_desc = arrow_type_to_type_desc(&result_type)?;
                    self.nodes.push(exprs::TExprNode {
                        node_type: exprs::TExprNodeType::CAST_EXPR,
                        type_: cast_type_desc,
                        num_children: 1,
                        ..default_expr_node()
                    });
                }
                self.compile_typed_inner(right)?;

                let type_desc = arrow_type_to_type_desc(&result_type)?;
                self.nodes[parent_idx] = exprs::TExprNode {
                    node_type: exprs::TExprNodeType::ARITHMETIC_EXPR,
                    type_: type_desc,
                    opcode: Some(opcode),
                    num_children: 2,
                    ..default_expr_node()
                };
                self.last_type = result_type.clone();
                Ok(result_type)
            }
        }
    }

    fn compile_literal(
        &mut self,
        lit: &LiteralValue,
        expr_type: &DataType,
    ) -> Result<DataType, String> {
        match lit {
            LiteralValue::Null => {
                let type_desc = scalar_type_desc(types::TPrimitiveType::NULL_TYPE);
                self.nodes.push(exprs::TExprNode {
                    node_type: exprs::TExprNodeType::NULL_LITERAL,
                    type_: type_desc,
                    num_children: 0,
                    ..default_expr_node()
                });
                self.last_type = DataType::Null;
                self.last_nullable = true;
                Ok(DataType::Null)
            }
            LiteralValue::Bool(b) => {
                let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                self.nodes.push(exprs::TExprNode {
                    node_type: exprs::TExprNodeType::BOOL_LITERAL,
                    type_: type_desc,
                    num_children: 0,
                    bool_literal: Some(exprs::TBoolLiteral { value: *b }),
                    ..default_expr_node()
                });
                self.last_type = DataType::Boolean;
                self.last_nullable = false;
                Ok(DataType::Boolean)
            }
            LiteralValue::Int(v) => {
                // When the typed expression has Date32 type, emit a DATE_LITERAL
                if *expr_type == DataType::Date32 {
                    let type_desc = scalar_type_desc(types::TPrimitiveType::DATE);
                    self.nodes.push(exprs::TExprNode {
                        node_type: exprs::TExprNodeType::DATE_LITERAL,
                        type_: type_desc,
                        num_children: 0,
                        date_literal: Some(exprs::TDateLiteral {
                            value: {
                                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                                let date = epoch + chrono::Duration::days(*v);
                                date.format("%Y-%m-%d").to_string()
                            },
                        }),
                        ..default_expr_node()
                    });
                    self.last_type = DataType::Date32;
                    self.last_nullable = false;
                    return Ok(DataType::Date32);
                }
                self.nodes.push(int_literal_node(*v));
                self.last_type = DataType::Int64;
                self.last_nullable = false;
                Ok(DataType::Int64)
            }
            LiteralValue::Float(v) => {
                let type_desc = scalar_type_desc(types::TPrimitiveType::DOUBLE);
                self.nodes.push(exprs::TExprNode {
                    node_type: exprs::TExprNodeType::FLOAT_LITERAL,
                    type_: type_desc,
                    num_children: 0,
                    float_literal: Some(exprs::TFloatLiteral {
                        value: thrift::OrderedFloat(*v),
                    }),
                    ..default_expr_node()
                });
                self.last_type = DataType::Float64;
                self.last_nullable = false;
                Ok(DataType::Float64)
            }
            LiteralValue::Decimal(s) => {
                let decimal_type = expr_type.clone();
                let type_desc = arrow_type_to_type_desc(&decimal_type)?;
                self.nodes.push(exprs::TExprNode {
                    node_type: exprs::TExprNodeType::DECIMAL_LITERAL,
                    type_: type_desc,
                    num_children: 0,
                    decimal_literal: Some(exprs::TDecimalLiteral::new(s.clone(), None::<Vec<u8>>)),
                    ..default_expr_node()
                });
                self.last_type = decimal_type.clone();
                self.last_nullable = false;
                Ok(decimal_type)
            }
            LiteralValue::String(s) => {
                let type_desc = scalar_type_desc(types::TPrimitiveType::VARCHAR);
                self.nodes.push(exprs::TExprNode {
                    node_type: exprs::TExprNodeType::STRING_LITERAL,
                    type_: type_desc,
                    num_children: 0,
                    string_literal: Some(exprs::TStringLiteral { value: s.clone() }),
                    ..default_expr_node()
                });
                self.last_type = DataType::Utf8;
                self.last_nullable = false;
                Ok(DataType::Utf8)
            }
        }
    }

    fn compile_typed_function_call(
        &mut self,
        name: &str,
        args: &[TypedExpr],
    ) -> Result<DataType, String> {
        self.compile_typed_function_call_with_hint(name, args, &DataType::Null)
    }

    fn compile_typed_function_call_with_hint(
        &mut self,
        name: &str,
        args: &[TypedExpr],
        type_hint: &DataType,
    ) -> Result<DataType, String> {
        let parent_idx = self.nodes.len();
        self.nodes.push(default_expr_node()); // placeholder

        if name == "map" {
            return self.compile_map_function_call_with_hint(parent_idx, args, type_hint);
        }

        let mut arg_types = Vec::new();
        for arg in args {
            let t = self.compile_typed_inner(arg)?;
            arg_types.push(t);
        }

        if name == "__array_literal" {
            let return_type = if *type_hint != DataType::Null {
                type_hint.clone()
            } else {
                let item_type = arg_types
                    .iter()
                    .cloned()
                    .reduce(|acc, ty| wider_type(&acc, &ty))
                    .unwrap_or(DataType::Null);
                DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "item", item_type, true,
                )))
            };
            let type_desc = arrow_type_to_type_desc(&return_type)?;
            self.nodes[parent_idx] = exprs::TExprNode {
                node_type: exprs::TExprNodeType::ARRAY_EXPR,
                type_: type_desc,
                num_children: args.len() as i32,
                ..default_expr_node()
            };
            self.last_type = return_type.clone();
            self.last_nullable = false;
            return Ok(return_type);
        }

        let inferred = infer_scalar_function_return_type(name, &arg_types)?;
        // Use the analyzer's type hint if available and more specific than inferred
        let return_type = if *type_hint != DataType::Null {
            type_hint.clone()
        } else {
            inferred
        };
        let type_desc = semantic_function_type_desc(name, args, &return_type)?;
        let ret_type_desc = type_desc.clone();

        let fn_arg_types: Vec<types::TTypeDesc> = arg_types
            .iter()
            .map(arrow_type_to_type_desc)
            .collect::<Result<Vec<_>, _>>()?;

        self.nodes[parent_idx] = exprs::TExprNode {
            node_type: exprs::TExprNodeType::FUNCTION_CALL,
            type_: type_desc,
            num_children: args.len() as i32,
            fn_: Some(types::TFunction {
                name: types::TFunctionName {
                    db_name: None,
                    function_name: name.to_string(),
                },
                binary_type: types::TFunctionBinaryType::BUILTIN,
                arg_types: fn_arg_types,
                ret_type: ret_type_desc,
                has_var_args: false,
                comment: None,
                signature: None,
                hdfs_location: None,
                scalar_fn: None,
                aggregate_fn: None,
                id: None,
                checksum: None,
                agg_state_desc: None,
                fid: None,
                table_fn: None,
                could_apply_dict_optimize: None,
                ignore_nulls: None,
                isolated: None,
                input_type: None,
                content: None,
            }),
            ..default_expr_node()
        };
        self.last_type = return_type.clone();
        self.last_nullable = true;
        Ok(return_type)
    }

    fn compile_map_function_call_with_hint(
        &mut self,
        parent_idx: usize,
        args: &[TypedExpr],
        type_hint: &DataType,
    ) -> Result<DataType, String> {
        let arg_types = args
            .iter()
            .map(|arg| arg.data_type.clone())
            .collect::<Vec<_>>();
        let inferred = infer_map_constructor_return_type(&arg_types);
        let return_type = if *type_hint != DataType::Null {
            type_hint.clone()
        } else {
            inferred
        };
        let (key_type, value_type) = map_key_value_types(&return_type)?;
        let key_desc = map_side_type_desc(
            args.iter().step_by(2).cloned().collect::<Vec<_>>(),
            &key_type,
        )?;
        let value_desc = map_side_type_desc(
            args.iter().skip(1).step_by(2).cloned().collect::<Vec<_>>(),
            &value_type,
        )?;
        let key_array_desc = list_type_desc(key_desc.clone())?;
        let value_array_desc = list_type_desc(value_desc.clone())?;

        let key_array_idx = self.nodes.len();
        self.nodes.push(default_expr_node());
        for arg in args.iter().step_by(2) {
            self.compile_with_cast_if_needed(arg, &key_type)?;
        }
        self.nodes[key_array_idx] = exprs::TExprNode {
            node_type: exprs::TExprNodeType::ARRAY_EXPR,
            type_: key_array_desc.clone(),
            num_children: args.iter().step_by(2).count() as i32,
            ..default_expr_node()
        };

        let value_array_idx = self.nodes.len();
        self.nodes.push(default_expr_node());
        for arg in args.iter().skip(1).step_by(2) {
            self.compile_with_cast_if_needed(arg, &value_type)?;
        }
        self.nodes[value_array_idx] = exprs::TExprNode {
            node_type: exprs::TExprNodeType::ARRAY_EXPR,
            type_: value_array_desc.clone(),
            num_children: args.iter().skip(1).step_by(2).count() as i32,
            ..default_expr_node()
        };

        let ret_type_desc = map_type_desc(key_desc, value_desc)?;
        self.nodes[parent_idx] = exprs::TExprNode {
            node_type: exprs::TExprNodeType::FUNCTION_CALL,
            type_: ret_type_desc.clone(),
            num_children: 2,
            fn_: Some(types::TFunction {
                name: types::TFunctionName {
                    db_name: None,
                    function_name: "map".to_string(),
                },
                binary_type: types::TFunctionBinaryType::BUILTIN,
                arg_types: vec![key_array_desc, value_array_desc],
                ret_type: ret_type_desc,
                has_var_args: false,
                comment: None,
                signature: None,
                hdfs_location: None,
                scalar_fn: None,
                aggregate_fn: None,
                id: None,
                checksum: None,
                agg_state_desc: None,
                fid: None,
                table_fn: None,
                could_apply_dict_optimize: None,
                ignore_nulls: None,
                isolated: None,
                input_type: None,
                content: None,
            }),
            ..default_expr_node()
        };
        self.last_type = return_type.clone();
        self.last_nullable = false;
        Ok(return_type)
    }
}

// Legacy ExprCompiler methods (compile_expr, compile_value, compile_binary_op,
// compile_case, compile_function, compile_function_call, compile_aggregate_function,
// peek_type) have been deleted — they were the pre-cascades SQL-to-Thrift path,
// fully replaced by compile_typed / compile_typed_inner / compile_aggregate_call_typed.

// ---------------------------------------------------------------------------
// Node construction helpers
// ---------------------------------------------------------------------------

pub(crate) fn build_slot_ref_texpr(
    slot_id: i32,
    tuple_id: i32,
    type_desc: types::TTypeDesc,
) -> exprs::TExpr {
    exprs::TExpr::new(vec![slot_ref_node(slot_id, tuple_id, type_desc)])
}

pub(crate) fn binding_type_desc(binding: &ColumnBinding) -> Result<types::TTypeDesc, String> {
    binding
        .type_desc
        .clone()
        .map(Ok)
        .unwrap_or_else(|| arrow_type_to_type_desc(&binding.data_type))
}

/// Wrap a TExpr in a CAST node to the given target type.
pub(crate) fn build_cast_texpr(child: exprs::TExpr, target_type: types::TTypeDesc) -> exprs::TExpr {
    let mut cast_node = exprs::TExprNode {
        node_type: exprs::TExprNodeType::CAST_EXPR,
        type_: target_type,
        num_children: 1,
        ..default_expr_node()
    };
    cast_node.opcode = Some(crate::opcodes::TExprOpcode::CAST);
    let mut nodes = vec![cast_node];
    nodes.extend(child.nodes);
    exprs::TExpr::new(nodes)
}

fn slot_ref_node(slot_id: i32, tuple_id: i32, type_desc: types::TTypeDesc) -> exprs::TExprNode {
    exprs::TExprNode {
        node_type: exprs::TExprNodeType::SLOT_REF,
        type_: type_desc,
        num_children: 0,
        slot_ref: Some(exprs::TSlotRef { slot_id, tuple_id }),
        ..default_expr_node()
    }
}

fn int_literal_node(value: i64) -> exprs::TExprNode {
    let type_desc = scalar_type_desc(types::TPrimitiveType::BIGINT);
    exprs::TExprNode {
        node_type: exprs::TExprNodeType::INT_LITERAL,
        type_: type_desc,
        num_children: 0,
        int_literal: Some(exprs::TIntLiteral { value }),
        ..default_expr_node()
    }
}

fn json_scalar_function_returns_json(name: &str) -> bool {
    matches!(
        name,
        "json_query"
            | "json_extract"
            | "get_json_object"
            | "json_object"
            | "json_array"
            | "to_json"
            | "parse_json"
    )
}

fn semantic_function_type_desc(
    name: &str,
    args: &[TypedExpr],
    return_type: &DataType,
) -> Result<types::TTypeDesc, String> {
    if json_scalar_function_returns_json(name) {
        return Ok(scalar_type_desc(types::TPrimitiveType::JSON));
    }
    if name == "map" {
        let (key_type, value_type) = map_key_value_types(return_type)?;
        let key_desc = map_side_type_desc(args.iter().step_by(2).cloned().collect(), &key_type)?;
        let value_desc = map_side_type_desc(
            args.iter().skip(1).step_by(2).cloned().collect(),
            &value_type,
        )?;
        return map_type_desc(key_desc, value_desc);
    }
    if name == "__array_literal"
        && let Some(item) = args.first()
    {
        return list_type_desc(typed_expr_type_desc(item)?);
    }
    arrow_type_to_type_desc(return_type)
}

fn semantic_aggregate_type_desc(
    name: &str,
    args: &[TypedExpr],
    return_type: &DataType,
) -> Result<types::TTypeDesc, String> {
    if matches!(name, "array_agg" | "array_agg_distinct")
        && let Some(item) = args.first()
    {
        return list_type_desc(typed_expr_type_desc(item)?);
    }
    if name == "map_agg" && args.len() >= 2 {
        return map_type_desc(
            typed_expr_type_desc(&args[0])?,
            typed_expr_type_desc(&args[1])?,
        );
    }
    if name == "approx_top_k"
        && let Some(item) = args.first()
    {
        let struct_type = DataType::Struct(
            vec![
                Arc::new(arrow::datatypes::Field::new(
                    "item",
                    item.data_type.clone(),
                    true,
                )),
                Arc::new(arrow::datatypes::Field::new("count", DataType::Int64, true)),
            ]
            .into(),
        );
        return list_type_desc(arrow_type_to_type_desc(&struct_type)?);
    }
    arrow_type_to_type_desc(return_type)
}

fn typed_expr_type_desc(expr: &TypedExpr) -> Result<types::TTypeDesc, String> {
    match &expr.kind {
        ExprKind::FunctionCall { name, args, .. } => {
            semantic_function_type_desc(name, args, &expr.data_type)
        }
        ExprKind::AggregateCall { name, args, .. } => {
            semantic_aggregate_type_desc(name, args, &expr.data_type)
        }
        ExprKind::Nested(inner) => typed_expr_type_desc(inner),
        _ => arrow_type_to_type_desc(&expr.data_type),
    }
}

fn list_type_desc(item_type: types::TTypeDesc) -> Result<types::TTypeDesc, String> {
    let item_nodes = item_type
        .types
        .ok_or_else(|| "list item type desc is empty".to_string())?;
    let mut nodes = Vec::with_capacity(1 + item_nodes.len());
    nodes.push(types::TTypeNode {
        type_: types::TTypeNodeType::ARRAY,
        scalar_type: None,
        is_named: None,
        struct_fields: None,
    });
    nodes.extend(item_nodes);
    Ok(types::TTypeDesc::new(nodes))
}

fn map_type_desc(
    key_type: types::TTypeDesc,
    value_type: types::TTypeDesc,
) -> Result<types::TTypeDesc, String> {
    let key_nodes = key_type
        .types
        .ok_or_else(|| "map key type desc is empty".to_string())?;
    let value_nodes = value_type
        .types
        .ok_or_else(|| "map value type desc is empty".to_string())?;
    let mut nodes = Vec::with_capacity(1 + key_nodes.len() + value_nodes.len());
    nodes.push(types::TTypeNode {
        type_: types::TTypeNodeType::MAP,
        scalar_type: None,
        is_named: None,
        struct_fields: None,
    });
    nodes.extend(key_nodes);
    nodes.extend(value_nodes);
    Ok(types::TTypeDesc::new(nodes))
}

fn map_key_value_types(return_type: &DataType) -> Result<(DataType, DataType), String> {
    let DataType::Map(entries, _) = return_type else {
        return Err(format!("map must return MAP type, got {:?}", return_type));
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return Err("map entries type must be STRUCT".to_string());
    };
    if fields.len() != 2 {
        return Err("map entries type must have 2 fields".to_string());
    }
    Ok((fields[0].data_type().clone(), fields[1].data_type().clone()))
}

fn map_side_type_desc(
    exprs: Vec<TypedExpr>,
    fallback_type: &DataType,
) -> Result<types::TTypeDesc, String> {
    match exprs.as_slice() {
        [] => arrow_type_to_type_desc(fallback_type),
        [expr] => typed_expr_type_desc(expr),
        _ => arrow_type_to_type_desc(fallback_type),
    }
}

pub(super) fn default_expr_node() -> exprs::TExprNode {
    exprs::TExprNode {
        node_type: exprs::TExprNodeType::INT_LITERAL,
        type_: scalar_type_desc(types::TPrimitiveType::INT),
        opcode: None,
        num_children: 0,
        agg_expr: None,
        bool_literal: None,
        case_expr: None,
        date_literal: None,
        float_literal: None,
        int_literal: None,
        in_predicate: None,
        is_null_pred: None,
        like_pred: None,
        literal_pred: None,
        slot_ref: None,
        string_literal: None,
        tuple_is_null_pred: None,
        info_func: None,
        decimal_literal: None,
        output_scale: 0,
        fn_call_expr: None,
        large_int_literal: None,
        output_column: None,
        output_type: None,
        vector_opcode: None,
        fn_: None,
        vararg_start_idx: None,
        child_type: None,
        vslot_ref: None,
        used_subfield_names: None,
        binary_literal: None,
        copy_flag: None,
        check_is_out_of_bounds: None,
        use_vectorized: None,
        has_nullable_child: None,
        is_nullable: None,
        child_type_desc: None,
        is_monotonic: None,
        dict_query_expr: None,
        dictionary_get_expr: None,
        is_index_only_filter: None,
        is_nondeterministic: None,
    }
}

/// Check whether an operand with `source` type needs an implicit CAST to
/// `target` type for arithmetic operations.  This handles cases like
/// Int64 * Decimal128 where the integer operand must be cast to Decimal.
fn needs_comparison_cast(source: &DataType, target: &DataType) -> bool {
    source != target
        && matches!(
            (source, target),
            (
                DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8,
                DataType::Decimal128(_, _)
            ) | (
                DataType::Float64 | DataType::Float32,
                DataType::Decimal128(_, _)
            ) | (DataType::Decimal128(_, _), DataType::Float64)
                | (
                    DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8,
                    DataType::Float64
                )
        )
}

fn needs_arithmetic_cast(source: &DataType, target: &DataType) -> bool {
    source != target
        && matches!(
            (source, target),
            (
                DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8,
                DataType::Decimal128(_, _)
            ) | (DataType::Decimal128(_, _), DataType::Float64)
        )
}

fn infer_scalar_function_return_type(
    name: &str,
    arg_types: &[DataType],
) -> Result<DataType, String> {
    match name {
        // String functions
        "upper"
        | "lower"
        | "trim"
        | "ltrim"
        | "rtrim"
        | "reverse"
        | "replace"
        | "lpad"
        | "rpad"
        | "concat"
        | "concat_ws"
        | "substr"
        | "substring"
        | "left"
        | "right"
        | "repeat"
        | "space"
        | "hex"
        | "unhex"
        | "md5"
        | "sha2"
        | "to_base64"
        | "from_base64"
        | "url_encode"
        | "url_decode"
        | "translate"
        | "initcap"
        | "split_part"
        | "regexp_extract"
        | "regexp_replace"
        | "append_trailing_char_if_absent"
        | "money_format"
        | "char"
        | "elt"
        | "format"
        | "strleft"
        | "strright"
        | "md5sum"
        | "md5sum_numeric"
        | "sm3"
        | "group_concat"
        | "string_agg"
        | "substring_index"
        | "parse_url"
        | "str_to_map" => Ok(DataType::Utf8),

        // Numeric functions
        "abs" | "negative" => Ok(arg_types.first().cloned().unwrap_or(DataType::Float64)),
        "ceil" | "ceiling" | "floor" => Ok(DataType::Int64),
        // round/truncate:
        // - Decimal input -> Decimal128 with adjusted scale
        // - Non-decimal without explicit scale -> Int64
        // - Non-decimal with explicit scale -> Float64
        "round" | "truncate" => Ok(match arg_types.first() {
            Some(DataType::Decimal128(_, s)) => {
                // If second arg is a constant integer (target decimal places),
                // use it as the output scale. Otherwise keep input scale.
                let out_scale = if arg_types.len() >= 2 {
                    match arg_types.get(1) {
                        Some(
                            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
                        ) => {
                            // Can't see the actual value here, use the analyzer's
                            // output type which already has the correct scale.
                            // Return the input scale as default; the analyzer's
                            // DataType on the FunctionCall node will override.
                            *s
                        }
                        _ => *s,
                    }
                } else {
                    0 // round(x) → integer
                };
                DataType::Decimal128(38, out_scale)
            }
            _ if arg_types.len() >= 2 => DataType::Float64,
            _ => DataType::Int64,
        }),
        "mod"
        | "fmod"
        | "pow"
        | "power"
        | "sqrt"
        | "cbrt"
        | "exp"
        | "ln"
        | "log"
        | "log2"
        | "log10"
        | "sin"
        | "cos"
        | "tan"
        | "asin"
        | "acos"
        | "atan"
        | "atan2"
        | "radians"
        | "degrees"
        | "pi"
        | "e"
        | "sign"
        | "cot"
        | "cosine_similarity"
        | "cosine_similarity_norm"
        | "l2_distance" => Ok(DataType::Float64),
        "rand" | "random" => Ok(DataType::Float64),
        "crc32" => Ok(DataType::Int64),

        // String length/position
        "length" | "char_length" | "character_length" | "bit_length" | "instr" | "locate"
        | "position" | "find_in_set" | "strcmp" | "ascii" | "ord" | "field" => Ok(DataType::Int32),

        // Conditional
        "if" | "ifnull" | "nullif" | "coalesce" | "nvl" | "case" => {
            if arg_types.is_empty() {
                Ok(DataType::Null)
            } else {
                let mut result = arg_types[0].clone();
                for t in &arg_types[1..] {
                    result = wider_type(&result, t);
                }
                Ok(result)
            }
        }

        // Date/time
        "now" | "current_timestamp" | "current_date" | "curdate" | "convert_tz" => Ok(
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        ),
        "date_format" | "from_unixtime" | "time_format" => Ok(DataType::Utf8),
        "date_add" | "date_sub" | "adddate" | "subdate" | "days_add" | "days_sub" | "weeks_add"
        | "weeks_sub" | "months_add" | "months_sub" | "years_add" | "years_sub" | "date_trunc"
        | "timestampadd" | "sec_to_time" | "hours_add" | "hours_sub" | "minutes_add"
        | "minutes_sub" | "seconds_add" | "seconds_sub" | "microseconds_add"
        | "microseconds_sub" => {
            let input_type = arg_types.first().cloned().unwrap_or(DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                None,
            ));
            Ok(match input_type {
                DataType::Date32 => DataType::Date32,
                DataType::Timestamp(u, tz) => DataType::Timestamp(u, tz),
                _ => DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            })
        }
        "year" | "month" | "day" | "dayofmonth" | "hour" | "minute" | "second" | "dayofweek"
        | "yearweek" | "dayofyear" | "weekofyear" | "quarter" | "hour_from_unixtime" => {
            Ok(DataType::Int32)
        }
        "unix_timestamp" | "to_unix_timestamp" | "datediff" | "timestampdiff" | "months_diff"
        | "years_diff" | "weeks_diff" | "days_diff" | "hours_diff" | "minutes_diff"
        | "seconds_diff" | "to_days" | "time_to_sec" => Ok(DataType::Int64),
        "to_date" | "str_to_date" | "from_days" | "makedate" | "last_day" | "next_day" => {
            Ok(DataType::Date32)
        }
        // Type
        "cast" => arg_types
            .first()
            .cloned()
            .ok_or("cast requires argument".into()),

        // Bitwise
        "bitnot" | "bitand" | "bitor" | "bitxor" | "bit_shift_left" | "bit_shift_right" => {
            Ok(arg_types.first().cloned().unwrap_or(DataType::Int64))
        }

        // Window/analytic functions
        "rank" | "dense_rank" | "row_number" | "ntile" | "cume_dist" | "percent_rank" => {
            Ok(DataType::Int64)
        }
        "lag" | "lead" | "first_value" | "last_value" => {
            Ok(arg_types.first().cloned().unwrap_or(DataType::Null))
        }

        // Aggregate functions that may appear in expression context
        "max_by" | "min_by" | "any_value" => {
            Ok(arg_types.first().cloned().unwrap_or(DataType::Null))
        }
        "bool_or" | "bool_and" | "every" => Ok(DataType::Boolean),
        "corr" | "covar_pop" | "covar_samp" | "var_pop" | "var_samp" | "variance"
        | "variance_pop" | "variance_samp" | "stddev" | "stddev_pop" | "stddev_samp" => {
            Ok(DataType::Float64)
        }
        "percentile_cont"
        | "percentile_disc"
        | "percentile_disc_lc"
        | "percentile_approx"
        | "percentile_approx_weighted" => Ok(DataType::Float64),
        "approx_top_k" => Ok(approx_top_k_output_type(
            arg_types.first().cloned().unwrap_or(DataType::Null),
        )),
        "min_n" | "max_n" => Ok(arg_types.first().cloned().unwrap_or(DataType::Null)),
        "bitmap_agg" | "bitmap_union" => Ok(DataType::Binary),
        "bitmap_union_int" | "bitmap_count" | "bitmap_union_count" => Ok(DataType::Int64),
        "hll_union_agg"
        | "hll_cardinality"
        | "ndv"
        | "approx_count_distinct"
        | "approx_count_distinct_hll_sketch"
        | "ds_hll_count_distinct"
        | "ds_hll_count_distinct_merge" => Ok(DataType::Int64),
        "hll_union" | "hll_raw_agg" | "ds_hll_count_distinct_union" => Ok(DataType::Binary),

        // Misc
        "version" | "database" | "current_user" | "user" | "bitmap_to_string" | "from_binary" => {
            Ok(DataType::Utf8)
        }
        "sleep" => Ok(DataType::Boolean),
        "uuid" | "typeof" => Ok(DataType::Utf8),
        "murmur_hash3_32" => Ok(DataType::Int32),
        "hll_hash" | "ds_hll_count_distinct_state" | "to_bitmap" => Ok(DataType::Binary),
        "xx_hash3_64" | "xx_hash3_128" => Ok(DataType::Int64),
        "to_binary" | "encode_row_id" => Ok(DataType::Binary),
        "to_datetime_ntz" => Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            None,
        )),
        "date" => Ok(DataType::Date32),
        "greatest" | "least" => Ok(arg_types.first().cloned().unwrap_or(DataType::Null)),
        "array_length" | "array_position" | "cardinality" | "map_size" => Ok(DataType::Int32),
        "grouping" | "grouping_id" => Ok(DataType::Int64),
        "array_min" | "array_max" => match arg_types.first() {
            Some(DataType::List(item)) => Ok(item.data_type().clone()),
            _ => Ok(DataType::Null),
        },
        "array_contains" | "array_distinct" => {
            Ok(arg_types.first().cloned().unwrap_or(DataType::Null))
        }
        "array_sort" | "array_sortby" | "array_reverse" | "array_slice" | "array_remove"
        | "array_filter" | "array_map" | "array_flatten" | "array_concat" => {
            Ok(arg_types.first().cloned().unwrap_or(DataType::Null))
        }
        "__array_element_at" => match arg_types.first() {
            Some(DataType::List(item)) => Ok(item.data_type().clone()),
            _ => Ok(DataType::Null),
        },
        "array_join" | "array_to_string" => Ok(DataType::Utf8),
        "__map_element_at" => match arg_types.first() {
            Some(DataType::Map(entries, _)) => match entries.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => Ok(fields[1].data_type().clone()),
                _ => Ok(DataType::Null),
            },
            _ => Ok(DataType::Null),
        },
        "percentile_hash" | "percentile_empty" => Ok(DataType::Binary),
        "percentile_approx_raw" => Ok(DataType::Float64),
        "map_keys" => match arg_types.first() {
            Some(DataType::Map(entries, _)) => match entries.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => Ok(DataType::List(Arc::new(
                    arrow::datatypes::Field::new("item", fields[0].data_type().clone(), true),
                ))),
                _ => Ok(DataType::Null),
            },
            _ => Ok(DataType::Null),
        },
        "map_values" => match arg_types.first() {
            Some(DataType::Map(entries, _)) => match entries.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => Ok(DataType::List(Arc::new(
                    arrow::datatypes::Field::new("item", fields[1].data_type().clone(), true),
                ))),
                _ => Ok(DataType::Null),
            },
            _ => Ok(DataType::Null),
        },
        "map" => Ok(infer_map_constructor_return_type(arg_types)),
        "map_from_arrays" => match (arg_types.first(), arg_types.get(1)) {
            (Some(DataType::List(keys)), Some(DataType::List(values))) => Ok(DataType::Map(
                Arc::new(arrow::datatypes::Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(arrow::datatypes::Field::new(
                                "key",
                                keys.data_type().clone(),
                                true,
                            )),
                            Arc::new(arrow::datatypes::Field::new(
                                "value",
                                values.data_type().clone(),
                                true,
                            )),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )),
            _ => Ok(DataType::Null),
        },
        "json_query" | "json_extract" | "get_json_string" | "get_json_int" | "get_json_double"
        | "get_json_object" | "json_object" | "json_array" | "to_json" | "parse_json" => {
            Ok(DataType::Utf8)
        }
        "__struct_subfield" | "__array_struct_subfield" => Ok(DataType::Null),
        "row" | "struct" => Ok(infer_struct_constructor_return_type(arg_types)),
        "named_struct" => Ok(infer_named_struct_return_type(arg_types)),

        _ => Err(format!("unknown scalar function: {name}")),
    }
}

fn infer_struct_constructor_return_type(arg_types: &[DataType]) -> DataType {
    let fields = arg_types
        .iter()
        .enumerate()
        .map(|(idx, data_type)| {
            Arc::new(arrow::datatypes::Field::new(
                format!("col{}", idx + 1),
                data_type.clone(),
                true,
            ))
        })
        .collect::<Vec<_>>();
    DataType::Struct(arrow::datatypes::Fields::from(fields))
}

fn infer_named_struct_return_type(arg_types: &[DataType]) -> DataType {
    let fields = arg_types
        .iter()
        .skip(1)
        .step_by(2)
        .enumerate()
        .map(|(idx, data_type)| {
            Arc::new(arrow::datatypes::Field::new(
                format!("col{}", idx + 1),
                data_type.clone(),
                true,
            ))
        })
        .collect::<Vec<_>>();
    DataType::Struct(arrow::datatypes::Fields::from(fields))
}

fn infer_map_constructor_return_type(arg_types: &[DataType]) -> DataType {
    let key_type = arg_types
        .iter()
        .step_by(2)
        .cloned()
        .reduce(|acc, ty| wider_type(&acc, &ty))
        .unwrap_or(DataType::Null);
    let value_type = arg_types
        .iter()
        .skip(1)
        .step_by(2)
        .cloned()
        .reduce(|acc, ty| wider_type(&acc, &ty))
        .unwrap_or(DataType::Null);
    DataType::Map(
        Arc::new(arrow::datatypes::Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(arrow::datatypes::Field::new("key", key_type, true)),
                    Arc::new(arrow::datatypes::Field::new("value", value_type, true)),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Aggregate function type inference
// ---------------------------------------------------------------------------

/// Returns (output_type, intermediate_type) for aggregate functions.
/// `None` as intermediate_type means the execution layer should use its default.
fn infer_agg_function_types(
    name: &str,
    arg_types: &[DataType],
    _is_distinct: bool,
) -> Result<(DataType, Option<DataType>), String> {
    let first_arg = arg_types.first().cloned().unwrap_or(DataType::Null);
    match name {
        "count" => Ok((DataType::Int64, Some(DataType::Int64))),
        "sum" => {
            let out = match &first_arg {
                DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64 => DataType::Int64,
                DataType::Float32 | DataType::Float64 => DataType::Float64,
                DataType::Decimal128(_p, s) => DataType::Decimal128(38, *s),
                _ => DataType::Float64,
            };
            Ok((out.clone(), Some(out)))
        }
        "avg" => {
            // avg(decimal(p,s)) uses division scale rule (sum/count):
            // s <= 6  => result_scale = s + 6
            // s <= 12 => result_scale = 12
            // else    => result_scale = s
            let out = match &first_arg {
                DataType::Decimal128(_p, s) => {
                    let new_scale = if *s <= 6 {
                        *s + 6
                    } else if *s <= 12 {
                        12
                    } else {
                        *s
                    };
                    DataType::Decimal128(38, new_scale)
                }
                _ => DataType::Float64,
            };
            Ok((out, Some(DataType::Utf8))) // intermediate is serialized state
        }
        "min" | "max" => Ok((first_arg.clone(), Some(first_arg))),
        "any_value" => Ok((first_arg.clone(), Some(first_arg))),
        "group_concat" | "string_agg" => {
            let intermediate = {
                let fields = arg_types
                    .iter()
                    .enumerate()
                    .map(|(idx, data_type)| {
                        Arc::new(arrow::datatypes::Field::new(
                            format!("c{idx}"),
                            DataType::List(Arc::new(arrow::datatypes::Field::new(
                                "item",
                                data_type.clone(),
                                true,
                            ))),
                            true,
                        ))
                    })
                    .collect::<Vec<_>>();
                DataType::Struct(arrow::datatypes::Fields::from(fields))
            };
            Ok((DataType::Utf8, Some(intermediate)))
        }
        "count_if" => Ok((DataType::Int64, Some(DataType::Int64))),
        "bool_or" | "bool_and" | "boolor_agg" | "booland_agg" | "every" => {
            Ok((DataType::Boolean, Some(DataType::Boolean)))
        }
        "array_agg" | "array_agg_distinct" => {
            let elem = first_arg.clone();
            let list = DataType::List(Arc::new(arrow::datatypes::Field::new("item", elem, true)));
            let intermediate = if arg_types.len() <= 1 {
                list.clone()
            } else {
                let fields = arg_types
                    .iter()
                    .enumerate()
                    .map(|(idx, data_type)| {
                        Arc::new(arrow::datatypes::Field::new(
                            format!("c{idx}"),
                            DataType::List(Arc::new(arrow::datatypes::Field::new(
                                "item",
                                data_type.clone(),
                                true,
                            ))),
                            true,
                        ))
                    })
                    .collect::<Vec<_>>();
                DataType::Struct(arrow::datatypes::Fields::from(fields))
            };
            Ok((list, Some(intermediate)))
        }
        "array_unique_agg" => Ok((first_arg.clone(), Some(first_arg))),
        "sum_map" => {
            let map = if first_arg == DataType::Null {
                null_map_output_type()
            } else {
                first_arg.clone()
            };
            Ok((map.clone(), Some(map)))
        }
        "map_agg" => {
            let key_type = arg_types.first().cloned().unwrap_or(DataType::Null);
            let value_type = arg_types.get(1).cloned().unwrap_or(DataType::Null);
            let map = DataType::Map(
                Arc::new(arrow::datatypes::Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(arrow::datatypes::Field::new("key", key_type, true)),
                            Arc::new(arrow::datatypes::Field::new("value", value_type, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            );
            Ok((map.clone(), Some(map)))
        }
        "bitmap_agg" | "bitmap_union" => Ok((DataType::Binary, Some(DataType::Binary))),
        "bitmap_union_count" => Ok((DataType::Int64, Some(DataType::Binary))),
        "approx_count_distinct"
        | "ndv"
        | "approx_count_distinct_hll_sketch"
        | "ds_hll_count_distinct"
        | "ds_hll_count_distinct_merge" => Ok((DataType::Int64, Some(DataType::Binary))),
        "hll_union_agg" => Ok((DataType::Int64, Some(DataType::Binary))),
        "hll_union" | "hll_raw_agg" | "ds_hll_count_distinct_union" => {
            Ok((DataType::Binary, Some(DataType::Binary)))
        }
        "multi_distinct_count" => Ok((DataType::Int64, Some(DataType::Binary))),
        "multi_distinct_sum" => {
            let out = match &first_arg {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    DataType::Int64
                }
                DataType::Float32 | DataType::Float64 => DataType::Float64,
                DataType::Decimal128(p, s) => DataType::Decimal128(*p, *s),
                _ => DataType::Float64,
            };
            Ok((out, Some(DataType::Binary)))
        }
        "bitmap_union_int" => Ok((DataType::Int64, Some(DataType::Binary))),
        "dict_merge" => Ok((DataType::Utf8, Some(DataType::Utf8))),
        "mann_whitney_u_test" => Ok((DataType::Utf8, Some(DataType::Binary))),
        "max_by" | "min_by" => {
            // max_by(value, key) -> type of value (first arg).
            // Intermediate is serialized binary state.
            Ok((first_arg, Some(DataType::Binary)))
        }
        "covar_pop" | "covar_samp" | "corr" | "var_pop" | "var_samp" | "variance"
        | "variance_pop" | "variance_samp" | "stddev" | "stddev_pop" | "stddev_samp" => {
            Ok((DataType::Float64, Some(DataType::Binary)))
        }
        "percentile_cont" | "percentile_disc" | "percentile_disc_lc" => {
            Ok((first_arg, Some(DataType::Binary)))
        }
        "percentile_union" => Ok((DataType::Binary, Some(DataType::Binary))),
        "percentile_approx" => {
            let output = if matches!(arg_types.get(1), Some(DataType::List(_))) {
                DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "item",
                    DataType::Float64,
                    true,
                )))
            } else {
                DataType::Float64
            };
            Ok((output, Some(DataType::Binary)))
        }
        "percentile_approx_weighted" => {
            let output = if matches!(arg_types.get(2), Some(DataType::List(_))) {
                DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "item",
                    DataType::Float64,
                    true,
                )))
            } else {
                DataType::Float64
            };
            Ok((output, Some(DataType::Binary)))
        }
        "approx_top_k" => Ok((approx_top_k_output_type(first_arg), Some(DataType::Binary))),
        "min_n" | "max_n" => Ok((list_output_type(first_arg), Some(DataType::Binary))),
        _ => {
            // Default: assume output same as first arg, intermediate same as output
            let out = if arg_types.is_empty() {
                DataType::Int64
            } else {
                first_arg
            };
            Ok((out.clone(), Some(out)))
        }
    }
}

fn approx_top_k_output_type(item_type: DataType) -> DataType {
    DataType::List(Arc::new(arrow::datatypes::Field::new(
        "item",
        DataType::Struct(
            vec![
                Arc::new(arrow::datatypes::Field::new("item", item_type, true)),
                Arc::new(arrow::datatypes::Field::new("count", DataType::Int64, true)),
            ]
            .into(),
        ),
        true,
    )))
}

fn null_map_output_type() -> DataType {
    DataType::Map(
        Arc::new(arrow::datatypes::Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(arrow::datatypes::Field::new("key", DataType::Null, true)),
                    Arc::new(arrow::datatypes::Field::new("value", DataType::Null, true)),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

fn list_output_type(item_type: DataType) -> DataType {
    DataType::List(Arc::new(arrow::datatypes::Field::new(
        "item", item_type, true,
    )))
}

#[cfg(test)]
mod tests {
    use super::infer_agg_function_types;
    use arrow::datatypes::DataType;

    #[test]
    fn percentile_family_uses_binary_intermediate_state() {
        let (_, exact_intermediate) = infer_agg_function_types(
            "percentile_cont",
            &[DataType::Int64, DataType::Float64],
            false,
        )
        .expect("percentile_cont type inference");
        assert_eq!(exact_intermediate, Some(DataType::Binary));

        let (_, approx_intermediate) = infer_agg_function_types(
            "percentile_approx",
            &[DataType::Float64, DataType::Float64],
            false,
        )
        .expect("percentile_approx type inference");
        assert_eq!(approx_intermediate, Some(DataType::Binary));

        let (_, union_intermediate) =
            infer_agg_function_types("percentile_union", &[DataType::Binary], false)
                .expect("percentile_union type inference");
        assert_eq!(union_intermediate, Some(DataType::Binary));
    }
}
