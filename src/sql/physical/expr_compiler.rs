use std::sync::Arc;

use arrow::datatypes::DataType;
use sqlparser::ast as sqlast;

use crate::exprs;
use crate::lower::thrift::type_lowering::scalar_type_desc;
use crate::opcodes;
use crate::types;

use super::resolve::ExprScope;
use super::type_infer::{
    arithmetic_result_type, arithmetic_result_type_with_op, arrow_type_to_type_desc, wider_type,
};
use crate::sql::ir::{BinOp, ExprKind, LiteralValue, TypedExpr, UnOp};
use crate::sql::plan::AggregateCall;

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

    pub fn last_type(&self) -> &DataType {
        &self.last_type
    }

    pub fn last_nullable(&self) -> bool {
        self.last_nullable
    }

    /// Compile an expression and return the complete TExpr.
    pub fn compile(&mut self, expr: &sqlast::Expr) -> Result<exprs::TExpr, String> {
        self.nodes.clear();
        self.compile_expr(expr)?;
        Ok(exprs::TExpr::new(std::mem::take(&mut self.nodes)))
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

        // Compile ORDER BY expressions as additional children (for group_concat etc.)
        let num_order_by = agg_call.order_by.len();
        for ob in &agg_call.order_by {
            self.compile_typed_inner(&ob.expr)?;
        }

        let total_children = agg_call.args.len() + num_order_by;

        let return_type = agg_call.result_type.clone();
        let type_desc = arrow_type_to_type_desc(&return_type)?;

        let (_, intermediate_type) =
            infer_agg_function_types(&effective_name, &arg_types, is_distinct)?;
        let intermediate_type_desc = match &intermediate_type {
            Some(it) => arrow_type_to_type_desc(it)?,
            None => types::TTypeDesc { types: None },
        };

        let fn_arg_types: Vec<types::TTypeDesc> = arg_types
            .iter()
            .map(|t| arrow_type_to_type_desc(t))
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
                let type_desc = arrow_type_to_type_desc(&binding.data_type)?;
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
                use crate::sql::physical::emitter::helpers::typed_expr_display_name;
                let display = typed_expr_display_name(expr);
                if let Ok(binding) = self.scope.resolve_column(None, &display) {
                    let type_desc = arrow_type_to_type_desc(&binding.data_type)?;
                    self.nodes
                        .push(slot_ref_node(binding.slot_id, binding.tuple_id, type_desc));
                    self.last_type = binding.data_type.clone();
                    self.last_nullable = binding.nullable;
                    Ok(binding.data_type.clone())
                } else {
                    self.compile_typed_function_call(name, args)
                }
            }
            ExprKind::AggregateCall {
                name,
                args,
                distinct,
                ..
            } => {
                // In a project-over-aggregate context, the scope has aggregate
                // output columns registered by display name. Try to look up
                // as a slot reference first.
                let display =
                    super::emitter::agg_call_display_name_from_parts(name, args, *distinct);
                if let Ok(binding) = self.scope.resolve_column(None, &display) {
                    let type_desc = arrow_type_to_type_desc(&binding.data_type)?;
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
            ExprKind::WindowCall { name, args, .. } => {
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
        let parent_idx = self.nodes.len();
        self.nodes.push(default_expr_node()); // placeholder

        let mut arg_types = Vec::new();
        for arg in args {
            let t = self.compile_typed_inner(arg)?;
            arg_types.push(t);
        }

        let return_type = infer_scalar_function_return_type(name, &arg_types)?;
        let type_desc = arrow_type_to_type_desc(&return_type)?;
        let ret_type_desc = type_desc.clone();

        let fn_arg_types: Vec<types::TTypeDesc> = arg_types
            .iter()
            .map(|t| arrow_type_to_type_desc(t))
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

    fn compile_expr(&mut self, expr: &sqlast::Expr) -> Result<DataType, String> {
        match expr {
            // Column reference: simple identifier
            sqlast::Expr::Identifier(ident) => {
                let binding = self.scope.resolve_column(None, &ident.value)?;
                let type_desc = arrow_type_to_type_desc(&binding.data_type)?;
                self.nodes
                    .push(slot_ref_node(binding.slot_id, binding.tuple_id, type_desc));
                self.last_type = binding.data_type.clone();
                self.last_nullable = binding.nullable;
                Ok(binding.data_type.clone())
            }

            // Qualified column reference: table.column
            sqlast::Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let qualifier = &parts[0].value;
                let col_name = &parts[1].value;
                let binding = self.scope.resolve_column(Some(qualifier), col_name)?;
                let type_desc = arrow_type_to_type_desc(&binding.data_type)?;
                self.nodes
                    .push(slot_ref_node(binding.slot_id, binding.tuple_id, type_desc));
                self.last_type = binding.data_type.clone();
                self.last_nullable = binding.nullable;
                Ok(binding.data_type.clone())
            }

            // Literals
            sqlast::Expr::Value(sqlast::ValueWithSpan { value, .. }) => self.compile_value(value),

            // Unary NOT
            sqlast::Expr::UnaryOp {
                op: sqlast::UnaryOperator::Not,
                expr: inner,
            } => {
                let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                let placeholder_idx = self.nodes.len();
                self.nodes.push(exprs::TExprNode {
                    node_type: exprs::TExprNodeType::COMPOUND_PRED,
                    type_: type_desc,
                    opcode: Some(opcodes::TExprOpcode::COMPOUND_NOT),
                    num_children: 1,
                    ..default_expr_node()
                });
                self.compile_expr(inner)?;
                self.last_type = DataType::Boolean;
                Ok(DataType::Boolean)
            }

            // Unary minus
            sqlast::Expr::UnaryOp {
                op: sqlast::UnaryOperator::Minus,
                expr: inner,
            } => {
                let inner_type = self.peek_type(inner)?;
                let result_type = inner_type.clone();
                let type_desc = arrow_type_to_type_desc(&result_type)?;
                let placeholder_idx = self.nodes.len();
                self.nodes.push(exprs::TExprNode {
                    node_type: exprs::TExprNodeType::ARITHMETIC_EXPR,
                    type_: type_desc,
                    opcode: Some(opcodes::TExprOpcode::MULTIPLY),
                    num_children: 2,
                    ..default_expr_node()
                });
                // emit -1 * expr
                self.nodes.push(int_literal_node(-1));
                self.compile_expr(inner)?;
                self.last_type = result_type.clone();
                Ok(result_type)
            }

            // Binary operations
            sqlast::Expr::BinaryOp { left, op, right } => self.compile_binary_op(left, op, right),

            // IS NULL / IS NOT NULL
            sqlast::Expr::IsNull(inner) => {
                let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                self.nodes.push(exprs::TExprNode {
                    node_type: exprs::TExprNodeType::IS_NULL_PRED,
                    type_: type_desc,
                    num_children: 1,
                    is_null_pred: Some(exprs::TIsNullPredicate { is_not_null: false }),
                    ..default_expr_node()
                });
                self.compile_expr(inner)?;
                self.last_type = DataType::Boolean;
                Ok(DataType::Boolean)
            }
            sqlast::Expr::IsNotNull(inner) => {
                let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                self.nodes.push(exprs::TExprNode {
                    node_type: exprs::TExprNodeType::IS_NULL_PRED,
                    type_: type_desc,
                    num_children: 1,
                    is_null_pred: Some(exprs::TIsNullPredicate { is_not_null: true }),
                    ..default_expr_node()
                });
                self.compile_expr(inner)?;
                self.last_type = DataType::Boolean;
                Ok(DataType::Boolean)
            }

            // IN list
            sqlast::Expr::InList {
                expr,
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
                self.compile_expr(expr)?;
                for item in list {
                    self.compile_expr(item)?;
                }
                self.last_type = DataType::Boolean;
                Ok(DataType::Boolean)
            }

            // BETWEEN
            sqlast::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                // Desugar: expr BETWEEN low AND high => (expr >= low) AND (expr <= high)
                // NOT BETWEEN => (expr < low) OR (expr > high)
                if *negated {
                    let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                    self.nodes.push(exprs::TExprNode {
                        node_type: exprs::TExprNodeType::COMPOUND_PRED,
                        type_: type_desc.clone(),
                        opcode: Some(opcodes::TExprOpcode::COMPOUND_OR),
                        num_children: 2,
                        ..default_expr_node()
                    });
                    // expr < low
                    self.nodes.push(exprs::TExprNode {
                        node_type: exprs::TExprNodeType::BINARY_PRED,
                        type_: type_desc.clone(),
                        opcode: Some(opcodes::TExprOpcode::LT),
                        num_children: 2,
                        ..default_expr_node()
                    });
                    self.compile_expr(expr)?;
                    self.compile_expr(low)?;
                    // expr > high
                    self.nodes.push(exprs::TExprNode {
                        node_type: exprs::TExprNodeType::BINARY_PRED,
                        type_: type_desc,
                        opcode: Some(opcodes::TExprOpcode::GT),
                        num_children: 2,
                        ..default_expr_node()
                    });
                    self.compile_expr(expr)?;
                    self.compile_expr(high)?;
                } else {
                    let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                    self.nodes.push(exprs::TExprNode {
                        node_type: exprs::TExprNodeType::COMPOUND_PRED,
                        type_: type_desc.clone(),
                        opcode: Some(opcodes::TExprOpcode::COMPOUND_AND),
                        num_children: 2,
                        ..default_expr_node()
                    });
                    // expr >= low
                    self.nodes.push(exprs::TExprNode {
                        node_type: exprs::TExprNodeType::BINARY_PRED,
                        type_: type_desc.clone(),
                        opcode: Some(opcodes::TExprOpcode::GE),
                        num_children: 2,
                        ..default_expr_node()
                    });
                    self.compile_expr(expr)?;
                    self.compile_expr(low)?;
                    // expr <= high
                    self.nodes.push(exprs::TExprNode {
                        node_type: exprs::TExprNodeType::BINARY_PRED,
                        type_: type_desc,
                        opcode: Some(opcodes::TExprOpcode::LE),
                        num_children: 2,
                        ..default_expr_node()
                    });
                    self.compile_expr(expr)?;
                    self.compile_expr(high)?;
                }
                self.last_type = DataType::Boolean;
                Ok(DataType::Boolean)
            }

            // LIKE
            sqlast::Expr::Like {
                negated,
                expr,
                pattern,
                escape_char,
                ..
            } => {
                let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                let esc = escape_char
                    .as_ref()
                    .map(|v| format!("{v}"))
                    .unwrap_or_else(|| "\\".to_string());
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
                    like_pred: Some(exprs::TLikePredicate { escape_char: esc }),
                    ..default_expr_node()
                });
                self.compile_expr(expr)?;
                self.compile_expr(pattern)?;
                self.last_type = DataType::Boolean;
                Ok(DataType::Boolean)
            }

            // CAST
            sqlast::Expr::Cast {
                expr,
                data_type: target_sql_type,
                ..
            } => {
                let target = sql_type_to_arrow(target_sql_type)?;
                let type_desc = arrow_type_to_type_desc(&target)?;
                self.nodes.push(exprs::TExprNode {
                    node_type: exprs::TExprNodeType::CAST_EXPR,
                    type_: type_desc,
                    num_children: 1,
                    ..default_expr_node()
                });
                self.compile_expr(expr)?;
                self.last_type = target.clone();
                Ok(target)
            }

            // CASE WHEN
            sqlast::Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => self.compile_case(operand.as_deref(), conditions, else_result.as_deref()),

            // Function call
            sqlast::Expr::Function(func) => self.compile_function(func),

            // Nested (parenthesized)
            sqlast::Expr::Nested(inner) => self.compile_expr(inner),

            // Boolean literals
            sqlast::Expr::IsTrue(inner) => self.compile_expr(inner),
            sqlast::Expr::IsFalse(inner) => {
                let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                self.nodes.push(exprs::TExprNode {
                    node_type: exprs::TExprNodeType::COMPOUND_PRED,
                    type_: type_desc,
                    opcode: Some(opcodes::TExprOpcode::COMPOUND_NOT),
                    num_children: 1,
                    ..default_expr_node()
                });
                self.compile_expr(inner)?;
                self.last_type = DataType::Boolean;
                Ok(DataType::Boolean)
            }

            other => Err(format!(
                "ThriftPlanBuilder: unsupported expression: {}",
                other
            )),
        }
    }

    fn compile_value(&mut self, value: &sqlast::Value) -> Result<DataType, String> {
        match value {
            sqlast::Value::Number(n, _) => {
                if let Ok(v) = n.parse::<i64>() {
                    self.nodes.push(int_literal_node(v));
                    self.last_type = DataType::Int64;
                    self.last_nullable = false;
                    Ok(DataType::Int64)
                } else if let Ok(v) = n.parse::<f64>() {
                    let type_desc = scalar_type_desc(types::TPrimitiveType::DOUBLE);
                    self.nodes.push(exprs::TExprNode {
                        node_type: exprs::TExprNodeType::FLOAT_LITERAL,
                        type_: type_desc,
                        num_children: 0,
                        float_literal: Some(exprs::TFloatLiteral {
                            value: thrift::OrderedFloat(v),
                        }),
                        ..default_expr_node()
                    });
                    self.last_type = DataType::Float64;
                    self.last_nullable = false;
                    Ok(DataType::Float64)
                } else {
                    Err(format!("invalid numeric literal: {n}"))
                }
            }
            sqlast::Value::SingleQuotedString(s) | sqlast::Value::DoubleQuotedString(s) => {
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
            sqlast::Value::Boolean(b) => {
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
            sqlast::Value::Null => {
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
            other => Err(format!("unsupported literal value: {:?}", other)),
        }
    }

    fn compile_binary_op(
        &mut self,
        left: &sqlast::Expr,
        op: &sqlast::BinaryOperator,
        right: &sqlast::Expr,
    ) -> Result<DataType, String> {
        match op {
            // Comparison operators
            sqlast::BinaryOperator::Eq
            | sqlast::BinaryOperator::NotEq
            | sqlast::BinaryOperator::Lt
            | sqlast::BinaryOperator::LtEq
            | sqlast::BinaryOperator::Gt
            | sqlast::BinaryOperator::GtEq
            | sqlast::BinaryOperator::Spaceship => {
                let opcode = match op {
                    sqlast::BinaryOperator::Eq => opcodes::TExprOpcode::EQ,
                    sqlast::BinaryOperator::NotEq => opcodes::TExprOpcode::NE,
                    sqlast::BinaryOperator::Lt => opcodes::TExprOpcode::LT,
                    sqlast::BinaryOperator::LtEq => opcodes::TExprOpcode::LE,
                    sqlast::BinaryOperator::Gt => opcodes::TExprOpcode::GT,
                    sqlast::BinaryOperator::GtEq => opcodes::TExprOpcode::GE,
                    sqlast::BinaryOperator::Spaceship => opcodes::TExprOpcode::EQ_FOR_NULL,
                    _ => unreachable!(),
                };
                let parent_idx = self.nodes.len();
                self.nodes.push(default_expr_node()); // placeholder
                let left_type = self.compile_expr(left)?;
                let right_type = self.compile_expr(right)?;
                // Determine the comparison type (wider of both sides)
                let compare_type = super::type_infer::wider_type(&left_type, &right_type);
                let child_type_desc = arrow_type_to_type_desc(&compare_type).ok();
                let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                self.nodes[parent_idx] = exprs::TExprNode {
                    node_type: exprs::TExprNodeType::BINARY_PRED,
                    type_: type_desc,
                    opcode: Some(opcode),
                    num_children: 2,
                    child_type_desc: child_type_desc,
                    ..default_expr_node()
                };
                self.last_type = DataType::Boolean;
                self.last_nullable = false;
                Ok(DataType::Boolean)
            }

            // Logical operators
            sqlast::BinaryOperator::And => {
                let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                self.nodes.push(exprs::TExprNode {
                    node_type: exprs::TExprNodeType::COMPOUND_PRED,
                    type_: type_desc,
                    opcode: Some(opcodes::TExprOpcode::COMPOUND_AND),
                    num_children: 2,
                    ..default_expr_node()
                });
                self.compile_expr(left)?;
                self.compile_expr(right)?;
                self.last_type = DataType::Boolean;
                Ok(DataType::Boolean)
            }
            sqlast::BinaryOperator::Or => {
                let type_desc = scalar_type_desc(types::TPrimitiveType::BOOLEAN);
                self.nodes.push(exprs::TExprNode {
                    node_type: exprs::TExprNodeType::COMPOUND_PRED,
                    type_: type_desc,
                    opcode: Some(opcodes::TExprOpcode::COMPOUND_OR),
                    num_children: 2,
                    ..default_expr_node()
                });
                self.compile_expr(left)?;
                self.compile_expr(right)?;
                self.last_type = DataType::Boolean;
                Ok(DataType::Boolean)
            }

            // Arithmetic operators
            sqlast::BinaryOperator::Plus
            | sqlast::BinaryOperator::Minus
            | sqlast::BinaryOperator::Multiply
            | sqlast::BinaryOperator::Divide
            | sqlast::BinaryOperator::Modulo => {
                let opcode = match op {
                    sqlast::BinaryOperator::Plus => opcodes::TExprOpcode::ADD,
                    sqlast::BinaryOperator::Minus => opcodes::TExprOpcode::SUBTRACT,
                    sqlast::BinaryOperator::Multiply => opcodes::TExprOpcode::MULTIPLY,
                    sqlast::BinaryOperator::Divide => opcodes::TExprOpcode::DIVIDE,
                    sqlast::BinaryOperator::Modulo => opcodes::TExprOpcode::MOD,
                    _ => unreachable!(),
                };
                // Reserve position for the parent node, we'll fill in after children
                let parent_idx = self.nodes.len();
                self.nodes.push(default_expr_node()); // placeholder
                let left_type = self.compile_expr(left)?;
                let right_type = self.compile_expr(right)?;
                let result_type = arithmetic_result_type(&left_type, &right_type);
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

            // String concatenation ||
            sqlast::BinaryOperator::StringConcat => {
                self.compile_function_call("concat", &[left.clone(), right.clone()])
            }

            other => Err(format!("unsupported binary operator: {:?}", other)),
        }
    }

    fn compile_case(
        &mut self,
        operand: Option<&sqlast::Expr>,
        conditions: &[sqlast::CaseWhen],
        else_result: Option<&sqlast::Expr>,
    ) -> Result<DataType, String> {
        // CASE has children: [operand?] [when1, then1, when2, then2, ...] [else?]
        let has_operand = operand.is_some();
        let has_else = else_result.is_some();
        let num_children =
            if has_operand { 1 } else { 0 } + conditions.len() * 2 + if has_else { 1 } else { 0 };

        let parent_idx = self.nodes.len();
        self.nodes.push(default_expr_node()); // placeholder

        if let Some(op) = operand {
            self.compile_expr(op)?;
        }

        let mut result_type = DataType::Null;
        for case_when in conditions {
            self.compile_expr(&case_when.condition)?;
            let t = self.compile_expr(&case_when.result)?;
            if result_type == DataType::Null {
                result_type = t;
            }
        }
        if let Some(el) = else_result {
            let t = self.compile_expr(el)?;
            if result_type == DataType::Null {
                result_type = t;
            }
        }
        if result_type == DataType::Null {
            result_type = DataType::Utf8; // fallback
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

    fn compile_function(&mut self, func: &sqlast::Function) -> Result<DataType, String> {
        let name = func.name.to_string().to_lowercase();

        // Extract arguments
        let args = match &func.args {
            sqlast::FunctionArguments::List(list) => {
                let mut arg_exprs = Vec::new();
                for arg in &list.args {
                    match arg {
                        sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(e)) => {
                            arg_exprs.push(e.clone());
                        }
                        sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Wildcard) => {
                            // e.g. count(*) - handled specially
                        }
                        other => {
                            return Err(format!("unsupported function argument: {:?}", other));
                        }
                    }
                }
                arg_exprs
            }
            sqlast::FunctionArguments::None => vec![],
            other => return Err(format!("unsupported function arguments style: {:?}", other)),
        };

        self.compile_function_call(&name, &args)
    }

    fn compile_function_call(
        &mut self,
        name: &str,
        args: &[sqlast::Expr],
    ) -> Result<DataType, String> {
        let parent_idx = self.nodes.len();
        self.nodes.push(default_expr_node()); // placeholder

        let mut arg_types = Vec::new();
        for arg in args {
            let t = self.compile_expr(arg)?;
            arg_types.push(t);
        }

        let return_type = infer_scalar_function_return_type(name, &arg_types)?;
        let type_desc = arrow_type_to_type_desc(&return_type)?;
        let ret_type_desc = type_desc.clone();

        let fn_arg_types: Vec<types::TTypeDesc> = arg_types
            .iter()
            .map(|t| arrow_type_to_type_desc(t))
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

    /// Compile an aggregate function call into a TExpr suitable for TAggregationNode.aggregate_functions.
    /// The root node uses FUNCTION_CALL with agg_expr and aggregate_fn set.
    pub fn compile_aggregate_function(
        &mut self,
        func: &sqlast::Function,
    ) -> Result<exprs::TExpr, String> {
        self.nodes.clear();
        let name = func.name.to_string().to_lowercase();

        let is_distinct = matches!(
            &func.args,
            sqlast::FunctionArguments::List(list) if list.duplicate_treatment == Some(sqlast::DuplicateTreatment::Distinct)
        );
        let is_count_star = matches!(
            &func.args,
            sqlast::FunctionArguments::List(list) if list.args.len() == 1
                && matches!(&list.args[0], sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Wildcard))
        );

        // Extract argument expressions
        let args: Vec<sqlast::Expr> = match &func.args {
            sqlast::FunctionArguments::List(list) => list
                .args
                .iter()
                .filter_map(|arg| match arg {
                    sqlast::FunctionArg::Unnamed(sqlast::FunctionArgExpr::Expr(e)) => {
                        Some(e.clone())
                    }
                    _ => None,
                })
                .collect(),
            _ => vec![],
        };

        let parent_idx = self.nodes.len();
        self.nodes.push(default_expr_node()); // placeholder

        let mut arg_types = Vec::new();
        for arg in &args {
            let t = self.compile_expr(arg)?;
            arg_types.push(t);
        }

        let (return_type, intermediate_type) =
            infer_agg_function_types(&name, &arg_types, is_distinct)?;
        let type_desc = arrow_type_to_type_desc(&return_type)?;
        // For group_concat/string_agg, use an empty intermediate type descriptor
        // so the execution layer falls back to its default STRUCT intermediate type.
        let intermediate_type_desc = match &intermediate_type {
            Some(it) => arrow_type_to_type_desc(it)?,
            None => types::TTypeDesc { types: None },
        };

        let fn_arg_types: Vec<types::TTypeDesc> = arg_types
            .iter()
            .map(|t| arrow_type_to_type_desc(t))
            .collect::<Result<Vec<_>, _>>()?;

        self.nodes[parent_idx] = exprs::TExprNode {
            node_type: exprs::TExprNodeType::FUNCTION_CALL,
            type_: type_desc.clone(),
            num_children: args.len() as i32,
            agg_expr: Some(exprs::TAggregateExpr {
                is_merge_agg: false,
            }),
            fn_: Some(types::TFunction {
                name: types::TFunctionName {
                    db_name: None,
                    function_name: name.clone(),
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
                    is_asc_order: None,
                    nulls_first: None,
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

    /// Peek the type of an expression without actually emitting nodes.
    fn peek_type(&self, expr: &sqlast::Expr) -> Result<DataType, String> {
        match expr {
            sqlast::Expr::Identifier(ident) => {
                let binding = self.scope.resolve_column(None, &ident.value)?;
                Ok(binding.data_type.clone())
            }
            sqlast::Expr::Value(sqlast::ValueWithSpan {
                value: sqlast::Value::Number(n, _),
                ..
            }) => {
                if n.parse::<i64>().is_ok() {
                    Ok(DataType::Int64)
                } else {
                    Ok(DataType::Float64)
                }
            }
            _ => Ok(DataType::Float64), // conservative default
        }
    }
}

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

// ---------------------------------------------------------------------------
// SQL type conversion
// ---------------------------------------------------------------------------

fn sql_type_to_arrow(sql_type: &sqlast::DataType) -> Result<DataType, String> {
    match sql_type {
        sqlast::DataType::TinyInt(_) => Ok(DataType::Int8),
        sqlast::DataType::SmallInt(_) => Ok(DataType::Int16),
        sqlast::DataType::Int(_) | sqlast::DataType::Integer(_) => Ok(DataType::Int32),
        sqlast::DataType::BigInt(_) => Ok(DataType::Int64),
        sqlast::DataType::Float(_) => Ok(DataType::Float32),
        sqlast::DataType::Double(_) | sqlast::DataType::DoublePrecision => Ok(DataType::Float64),
        sqlast::DataType::Boolean => Ok(DataType::Boolean),
        sqlast::DataType::Varchar(_)
        | sqlast::DataType::CharVarying(_)
        | sqlast::DataType::Text => Ok(DataType::Utf8),
        sqlast::DataType::Char(_)
        | sqlast::DataType::Character(_)
        | sqlast::DataType::String(_) => Ok(DataType::Utf8),
        sqlast::DataType::Date => Ok(DataType::Date32),
        sqlast::DataType::Datetime(_) | sqlast::DataType::Timestamp(_, _) => Ok(
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        ),
        sqlast::DataType::Time(_, _) => {
            Ok(DataType::Time64(arrow::datatypes::TimeUnit::Microsecond))
        }
        sqlast::DataType::Decimal(info)
        | sqlast::DataType::Dec(info)
        | sqlast::DataType::Numeric(info) => match info {
            sqlast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                Ok(DataType::Decimal128(*p as u8, *s as i8))
            }
            sqlast::ExactNumberInfo::Precision(p) => Ok(DataType::Decimal128(*p as u8, 0)),
            sqlast::ExactNumberInfo::None => Ok(DataType::Decimal128(38, 0)),
        },
        // StarRocks STRING type often parsed as Custom
        sqlast::DataType::Custom(name, _) => {
            let type_name = name.to_string().to_lowercase();
            match type_name.as_str() {
                "string" => Ok(DataType::Utf8),
                "largeint" => Ok(DataType::Int64), // approximate
                "json" | "jsonb" => Ok(DataType::Utf8),
                _ => Err(format!("unsupported SQL type: {}", name)),
            }
        }
        other => Err(format!("unsupported CAST target type: {:?}", other)),
    }
}

// ---------------------------------------------------------------------------
// Scalar function return type inference
// ---------------------------------------------------------------------------

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
        // round/truncate: Decimal input → Decimal128(38, scale); otherwise Float64
        "round" | "truncate" => Ok(match arg_types.first() {
            Some(DataType::Decimal128(_, s)) => DataType::Decimal128(38, *s),
            _ => DataType::Float64,
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
        "date_format" | "from_unixtime" | "date_add" | "date_sub" | "adddate" | "subdate"
        | "time_format" => Ok(DataType::Utf8),
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
        "days_add" | "days_sub" | "date_trunc" | "timestampadd" | "sec_to_time" | "months_add"
        | "months_sub" | "years_add" | "years_sub" | "hours_add" | "hours_sub" | "minutes_add"
        | "minutes_sub" | "seconds_add" | "seconds_sub" | "microseconds_add"
        | "microseconds_sub" | "weeks_add" | "weeks_sub" => {
            Ok(arg_types.first().cloned().unwrap_or(DataType::Date32))
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
        "corr" | "covar_pop" | "covar_samp" | "var_pop" | "var_samp" | "variance" | "stddev"
        | "stddev_pop" | "stddev_samp" => Ok(DataType::Float64),
        "percentile_cont"
        | "percentile_disc"
        | "percentile_disc_lc"
        | "percentile_approx"
        | "percentile_approx_weighted" => Ok(DataType::Float64),
        "approx_top_k" | "min_n" | "max_n" => {
            Ok(arg_types.first().cloned().unwrap_or(DataType::Null))
        }
        "bitmap_union_int" | "bitmap_count" | "bitmap_union_count" => Ok(DataType::Int64),
        "hll_union_agg" | "hll_cardinality" | "ndv" | "approx_count_distinct" => {
            Ok(DataType::Int64)
        }

        // Misc
        "version" | "database" | "current_user" | "user" => Ok(DataType::Utf8),
        "sleep" => Ok(DataType::Boolean),
        "uuid" | "typeof" => Ok(DataType::Utf8),
        "murmur_hash3_32" => Ok(DataType::Int32),
        "xx_hash3_64" | "xx_hash3_128" => Ok(DataType::Int64),
        "to_binary" | "encode_row_id" => Ok(DataType::Binary),
        "to_datetime_ntz" => Ok(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            None,
        )),
        "date" | "to_date" => Ok(DataType::Date32),
        "greatest" | "least" => Ok(arg_types.first().cloned().unwrap_or(DataType::Null)),
        "array_length" | "array_position" | "cardinality" => Ok(DataType::Int32),
        "array_contains" | "array_distinct" => {
            Ok(arg_types.first().cloned().unwrap_or(DataType::Null))
        }
        "array_sort" | "array_reverse" | "array_slice" | "array_remove" | "array_filter"
        | "array_map" | "array_flatten" | "array_concat" => {
            Ok(arg_types.first().cloned().unwrap_or(DataType::Null))
        }
        "array_join" | "array_to_string" => Ok(DataType::Utf8),
        "map_keys" | "map_values" | "map_from_arrays" => {
            Ok(arg_types.first().cloned().unwrap_or(DataType::Null))
        }
        "json_query" | "json_extract" | "get_json_string" | "get_json_int" | "get_json_double"
        | "get_json_object" | "json_object" | "json_array" | "to_json" | "parse_json" => {
            Ok(DataType::Utf8)
        }
        "named_struct" | "struct" => Ok(arg_types.first().cloned().unwrap_or(DataType::Null)),

        _ => Err(format!("unknown scalar function: {name}")),
    }
}

// ---------------------------------------------------------------------------
// Aggregate function type inference
// ---------------------------------------------------------------------------

/// Returns (output_type, intermediate_type) for aggregate functions.
/// `None` as intermediate_type means the execution layer should use its default.
fn infer_agg_function_types(
    name: &str,
    arg_types: &[DataType],
    is_distinct: bool,
) -> Result<(DataType, Option<DataType>), String> {
    let first_arg = arg_types.first().cloned().unwrap_or(DataType::Null);
    match name {
        "count" => Ok((DataType::Int64, Some(DataType::Int64))),
        "sum" => {
            let out = match &first_arg {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    DataType::Int64
                }
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
        // group_concat/string_agg: use None to let the execution layer build the correct
        // STRUCT intermediate type from the actual argument types.
        "group_concat" | "string_agg" => Ok((DataType::Utf8, None)),
        "count_if" => Ok((DataType::Int64, Some(DataType::Int64))),
        "bool_or" | "bool_and" => Ok((DataType::Boolean, Some(DataType::Boolean))),
        "array_agg" => {
            let elem = first_arg.clone();
            let list = DataType::List(Arc::new(arrow::datatypes::Field::new("item", elem, true)));
            Ok((list.clone(), Some(list)))
        }
        "bitmap_union_count" => Ok((DataType::Int64, Some(DataType::Int64))),
        "approx_count_distinct" | "ndv" => Ok((DataType::Int64, Some(DataType::Binary))),
        "hll_union_agg" | "hll_raw_agg" => Ok((DataType::Int64, Some(DataType::Int64))),
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
        "bitmap_union_int" => Ok((DataType::Int64, Some(DataType::Int64))),
        "max_by" | "min_by" => {
            // max_by(value, key) -> type of value (first arg).
            // Intermediate is serialized binary state.
            Ok((first_arg, Some(DataType::Binary)))
        }
        "covar_pop" | "covar_samp" | "corr" | "var_pop" | "var_samp" | "variance" | "stddev"
        | "stddev_pop" | "stddev_samp" => Ok((DataType::Float64, None)),
        "percentile_cont"
        | "percentile_disc"
        | "percentile_disc_lc"
        | "percentile_approx"
        | "percentile_approx_weighted"
        | "percentile_union" => Ok((DataType::Float64, None)),
        "approx_top_k" | "min_n" | "max_n" => Ok((first_arg.clone(), None)),
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

/// Check if a function name is a known aggregate function.
pub(crate) fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name,
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "any_value"
            | "group_concat"
            | "string_agg"
            | "count_if"
            | "bool_or"
            | "bool_and"
            | "array_agg"
            | "array_unique_agg"
            | "bitmap_union_count"
            | "bitmap_union_int"
            | "approx_count_distinct"
            | "ndv"
            | "hll_union_agg"
            | "hll_raw_agg"
            | "hll_cardinality"
            | "multi_distinct_count"
            | "covar_pop"
            | "covar_samp"
            | "corr"
            | "variance"
            | "var_pop"
            | "var_samp"
            | "stddev"
            | "stddev_pop"
            | "stddev_samp"
            | "dict_merge"
            | "max_by"
            | "min_by"
            | "percentile_cont"
            | "percentile_disc"
            | "percentile_disc_lc"
            | "percentile_approx"
            | "percentile_approx_weighted"
            | "approx_top_k"
            | "min_n"
            | "max_n"
            | "percentile_union"
    )
}
