// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{BTreeSet, HashSet};

use crate::column_id::ColumnId;
use crate::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::optimizer::scalar_expr;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PredicateKey(String);

impl PredicateKey {
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PredicateOrigin {
    Filter,
    JoinCondition,
    Derived,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PredicateDerivedKind {
    None,
    Equivalence,
    Range,
    RangeEnvelope,
    OrSideFilter,
    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
    NotNull,
}

#[derive(Clone, Debug)]
pub(crate) struct PredicateGroup {
    pub(crate) expr: ScalarId,
    pub(crate) referenced_ids: BTreeSet<ColumnId>,
    pub(crate) key: PredicateKey,
    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
    pub(crate) origin: PredicateOrigin,
    #[allow(
        dead_code,
        reason = "Retained for staged SQL planner migration consumers and test helpers."
    )]
    pub(crate) derived: PredicateDerivedKind,
    pub(crate) deterministic: bool,
    pub(crate) can_fail: bool,
}

impl PredicateGroup {
    pub(crate) fn new(
        arena: &ScalarArena,
        expr: ScalarId,
        origin: PredicateOrigin,
        derived: PredicateDerivedKind,
    ) -> Self {
        let referenced_ids = scalar_expr::collect_column_ids_strict(arena, expr)
            .unwrap_or_default()
            .into_iter()
            .collect();
        let key = predicate_key(arena, expr);
        let deterministic = !scalar_expr::contains_non_deterministic_function(arena, expr);
        Self {
            expr,
            referenced_ids,
            key,
            origin,
            derived,
            deterministic,
            can_fail: scalar_expr::can_fail(arena, expr),
        }
    }

    pub(crate) fn from_predicate(
        arena: &ScalarArena,
        expr: ScalarId,
        origin: PredicateOrigin,
    ) -> Vec<Self> {
        let mut conjuncts = Vec::new();
        scalar_expr::split_conjuncts(arena, expr, &mut conjuncts);
        conjuncts
            .into_iter()
            .map(|expr| Self::new(arena, expr, origin, PredicateDerivedKind::None))
            .collect()
    }
}

pub(crate) fn predicate_key(arena: &ScalarArena, expr: ScalarId) -> PredicateKey {
    PredicateKey(
        signed_column_literal_equality_key(arena, expr)
            .unwrap_or_else(|| canonical_expr_key(arena, expr)),
    )
}

/// A propagated signed column equality can return in the optimizer's binary
/// representation. Recognize the original bound equality without changing
/// either expression or comparing the semantics of other operator domains.
fn signed_column_literal_equality_key(arena: &ScalarArena, expr: ScalarId) -> Option<String> {
    use arrow::datatypes::DataType;
    use novarocks_functions::{FunctionKind, FunctionResultType};
    use novarocks_type_contract::{
        FunctionArgumentType, FunctionValueType, OperatorFunction, ValueLogicalType,
    };

    let result = arena.value_type(expr);
    let operands = match arena.node(expr) {
        ScalarNode::FunctionCall {
            args,
            binding,
            distinct: false,
            volatility,
            ..
        } if binding.function_id.as_str() == OperatorFunction::Equal.function_id_text()
            && Some(binding.selected.overload.as_str())
                == OperatorFunction::Equal.comparison_overload_text()
            && binding.kind == FunctionKind::Scalar
            && binding.logical_argument_count == 2
            && volatility.is_replica_deterministic()
            && binding.semantics.volatility.is_replica_deterministic()
            && !scalar_expr::scalar_call_can_fail(binding) =>
        {
            let [left, right] = args.as_slice() else {
                return None;
            };
            let [
                FunctionArgumentType::Value(selected_left),
                FunctionArgumentType::Value(selected_right),
            ] = binding.selected.argument_types.as_slice()
            else {
                return None;
            };
            let FunctionResultType::Scalar(selected_result) = &binding.selected.result_type else {
                return None;
            };
            if selected_left != arena.value_type(*left)
                || selected_right != arena.value_type(*right)
                || selected_result != result
            {
                return None;
            }
            [*left, *right]
        }
        _ => return None,
    };

    let [left, right] = operands;
    let (column, literal) = if scalar_expr::column_id(arena, left).is_some() {
        (left, right)
    } else {
        (right, left)
    };
    let column_id = scalar_expr::column_id(arena, column)?;
    let column_type = arena.value_type(column);
    let literal_type = arena.value_type(literal);
    if column_type.logical_type() != ValueLogicalType::Physical
        || literal_type.logical_type() != ValueLogicalType::Physical
        || column_type.data_type() != literal_type.data_type()
        || result
            != &FunctionValueType::new(
                DataType::Boolean,
                column_type.nullable() || literal_type.nullable(),
            )
        || scalar_expr::can_fail(arena, literal)
    {
        return None;
    }
    let value = scalar_expr::signed_integer_literal(arena, literal)?;
    Some(format!(
        "SignedColumnLiteralEquality({column_id:?},{column_type:?},{result:?},{value})"
    ))
}

fn canonical_expr_key(arena: &ScalarArena, expr: ScalarId) -> String {
    match arena.node(expr) {
        ScalarNode::Let {
            binding,
            initializer,
            body,
        } => format!(
            "Let({}, {}, {})",
            binding.0,
            canonical_expr_key(arena, *initializer),
            canonical_expr_key(arena, *body),
        ),
        ScalarNode::LocalValue(binding) => format!("LocalValue({})", binding.0),
        ScalarNode::Nested(inner) => canonical_expr_key(arena, *inner),
        ScalarNode::BooleanConnective {
            left,
            op: crate::common::BooleanOp::And,
            right,
            ..
        } => canonical_bool_key(arena, "AND", *left, *right),
        ScalarNode::BooleanConnective {
            left,
            op: crate::common::BooleanOp::Or,
            right,
            ..
        } => canonical_bool_key(arena, "OR", *left, *right),
        ScalarNode::BooleanConnective { left, op, right } => {
            let left = canonical_expr_key(arena, *left);
            let right = canonical_expr_key(arena, *right);
            format!("BooleanConnective({op:?},{left},{right})")
        }
        ScalarNode::FunctionCall {
            name,
            args,
            distinct,
            ..
        } => format!(
            "FunctionCall({name},{distinct},{})",
            canonical_expr_list_key(arena, args)
        ),
        ScalarNode::LambdaFunction { params, body } => {
            format!(
                "LambdaFunction({params:?},{})",
                canonical_expr_key(arena, *body)
            )
        }
        ScalarNode::AggregateCall {
            name,
            args,
            distinct,
            order_by,
            ..
        } => format!(
            "AggregateCall({name},{distinct},{},{order_by:?})",
            canonical_expr_list_key(arena, args)
        ),
        ScalarNode::Cast {
            cast_mode,
            child,
            target,
            decimal_overflow_policy,
        } => {
            format!(
                "Cast({},{target:?},{decimal_overflow_policy:?},{cast_mode:?})",
                canonical_expr_key(arena, *child)
            )
        }
        ScalarNode::IsNull { child, negated } => {
            format!("IsNull({},{negated})", canonical_expr_key(arena, *child))
        }
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
            ..
        } => {
            let operand_key = operand
                .as_ref()
                .map(|expr| canonical_expr_key(arena, *expr))
                .unwrap_or_else(|| "None".to_string());
            let when_then_key = when_then
                .iter()
                .map(|(when, then)| {
                    format!(
                        "{}=>{}",
                        canonical_expr_key(arena, *when),
                        canonical_expr_key(arena, *then)
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            let else_key = else_expr
                .as_ref()
                .map(|expr| canonical_expr_key(arena, *expr))
                .unwrap_or_else(|| "None".to_string());
            format!("Case({operand_key},{when_then_key},{else_key})")
        }
        ScalarNode::WindowCall {
            name,
            args,
            distinct,
            binding,
            function_order_by,
            aggregate_binding,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => format!(
            "WindowCall({name},{distinct},{binding:?},{},{function_order_by:?},{aggregate_binding:?},{},{order_by:?},{window_frame:?},{ignore_nulls})",
            canonical_expr_list_key(arena, args),
            canonical_expr_list_key(arena, partition_by)
        ),
        ScalarNode::Lambda { params, body } => {
            format!("Lambda({params:?},{})", canonical_expr_key(arena, *body))
        }
        ScalarNode::ColumnRef(_)
        | ScalarNode::LambdaParamRef { .. }
        | ScalarNode::Literal(_)
        | ScalarNode::Constant(_) => {
            format!("{:?}", arena.node(expr))
        }
    }
}

fn canonical_bool_key(
    arena: &ScalarArena,
    op_name: &str,
    left: ScalarId,
    right: ScalarId,
) -> String {
    let mut terms = Vec::new();
    collect_bool_terms(arena, left, op_name, &mut terms);
    collect_bool_terms(arena, right, op_name, &mut terms);
    if [left, right].into_iter().all(|expr| {
        !scalar_expr::can_fail(arena, expr)
            && !scalar_expr::contains_non_replica_deterministic_function(arena, expr)
    }) {
        terms.sort();
    }
    format!("{op_name}({})", terms.join(","))
}

fn collect_bool_terms(arena: &ScalarArena, expr: ScalarId, op_name: &str, out: &mut Vec<String>) {
    match (arena.node(expr), op_name) {
        (
            ScalarNode::BooleanConnective {
                left,
                op: crate::common::BooleanOp::And,
                right,
            },
            "AND",
        )
        | (
            ScalarNode::BooleanConnective {
                left,
                op: crate::common::BooleanOp::Or,
                right,
            },
            "OR",
        ) => {
            collect_bool_terms(arena, *left, op_name, out);
            collect_bool_terms(arena, *right, op_name, out);
        }
        (ScalarNode::Nested(inner), _) => collect_bool_terms(arena, *inner, op_name, out),
        _ => out.push(canonical_expr_key(arena, expr)),
    }
}

fn canonical_expr_list_key(arena: &ScalarArena, exprs: &[ScalarId]) -> String {
    exprs
        .iter()
        .map(|expr| canonical_expr_key(arena, *expr))
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn dedupe_groups(groups: Vec<PredicateGroup>) -> Vec<PredicateGroup> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for group in groups {
        if seen.insert(group.key.clone()) {
            out.push(group);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::optimizer::scalar::ScalarArena;

    use crate::optimizer::scalar_expr;
    use crate::planner::optimizer_bridge::scalar::{intern_typed, materialize};
    use arrow::datatypes::DataType;

    fn col(name: &str, id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some("t".to_string()),
                column: name.to_string(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int32, true),
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        }
    }

    fn bool_expr(
        left: TypedExpr,
        op: impl Into<crate::common::binary_operator::TestOperator>,
        right: TypedExpr,
    ) -> TypedExpr {
        crate::common::binary_operator::test_operator(
            op,
            left,
            right,
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        )
    }

    fn func(
        name: &str,
        args: Vec<TypedExpr>,
        volatility: crate::functions::FunctionVolatility,
    ) -> TypedExpr {
        let binding =
            crate::analysis::test_function_binding(name, &args, DataType::Int64, true, volatility);
        TypedExpr {
            kind: ExprKind::FunctionCall {
                binding,
                volatility,
                name: name.to_string(),
                args: (args).into(),
                distinct: false,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
        }
    }

    fn groups_from_typed(arena: &mut ScalarArena, expr: TypedExpr) -> Vec<PredicateGroup> {
        let id = intern_typed(
            arena,
            &expr,
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();
        PredicateGroup::from_predicate(arena, id, PredicateOrigin::Filter)
    }

    fn group_from_typed(
        arena: &mut ScalarArena,
        expr: TypedExpr,
        origin: PredicateOrigin,
        derived: PredicateDerivedKind,
    ) -> PredicateGroup {
        let id = intern_typed(
            arena,
            &expr,
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();
        PredicateGroup::new(arena, id, origin, derived)
    }

    #[test]
    fn top_level_and_is_split_but_or_stays_atomic() {
        let expr = bool_expr(
            bool_expr(col("a", 1), BinOp::Eq, int_lit(1)),
            crate::common::BooleanOp::And,
            bool_expr(
                bool_expr(col("b", 2), BinOp::Eq, int_lit(2)),
                crate::common::BooleanOp::Or,
                bool_expr(col("b", 2), BinOp::Eq, int_lit(3)),
            ),
        );

        let mut arena = ScalarArena::new();
        let groups = groups_from_typed(&mut arena, expr);

        assert_eq!(groups.len(), 2);
        assert!(
            groups[0]
                .referenced_ids
                .contains(&ColumnId::new_for_test(1))
        );
        assert!(
            groups[1]
                .referenced_ids
                .contains(&ColumnId::new_for_test(2))
        );
        let second_expr = materialize(&arena, groups[1].expr);
        assert!(matches!(
            second_expr.kind,
            ExprKind::BooleanConnective {
                op: crate::common::BooleanOp::Or,
                ..
            }
        ));
    }

    #[test]
    fn nested_top_level_and_is_split_into_groups() {
        let expr = TypedExpr {
            kind: ExprKind::Nested(novarocks_parser::ast::AstBox::new(bool_expr(
                bool_expr(col("a", 1), BinOp::Eq, int_lit(1)),
                crate::common::BooleanOp::And,
                bool_expr(col("b", 2), BinOp::Eq, int_lit(2)),
            ))),
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Boolean, true),
        };

        let mut arena = ScalarArena::new();
        let groups = groups_from_typed(&mut arena, expr);

        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn dedupe_keeps_first_group_for_same_canonical_key() {
        let mut arena = ScalarArena::new();
        let first = group_from_typed(
            &mut arena,
            bool_expr(col("a", 1), BinOp::Eq, int_lit(1)),
            PredicateOrigin::Filter,
            PredicateDerivedKind::None,
        );
        let second = group_from_typed(
            &mut arena,
            bool_expr(col("a", 1), BinOp::Eq, int_lit(1)),
            PredicateOrigin::Derived,
            PredicateDerivedKind::Equivalence,
        );

        let deduped = dedupe_groups(vec![first.clone(), second]);

        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped[0].origin, first.origin);
    }

    #[test]
    fn split_or_refs_flattens_nested_or() {
        let expr = bool_expr(
            bool_expr(col("a", 1), BinOp::Eq, int_lit(1)),
            crate::common::BooleanOp::Or,
            bool_expr(
                bool_expr(col("a", 1), BinOp::Eq, int_lit(2)),
                crate::common::BooleanOp::Or,
                bool_expr(col("a", 1), BinOp::Eq, int_lit(3)),
            ),
        );

        let mut arena = ScalarArena::new();
        let id = intern_typed(
            &mut arena,
            &expr,
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();
        let mut branches = Vec::new();
        scalar_expr::split_disjuncts(&arena, id, &mut branches);
        assert_eq!(branches.len(), 3);
    }

    #[test]
    fn combine_or_round_trips_to_left_to_right_or_branches() {
        let mut arena = ScalarArena::new();
        let exprs = vec![
            intern_typed(
                &mut arena,
                &bool_expr(col("a", 1), BinOp::Eq, int_lit(1)),
                crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
            )
            .unwrap(),
            intern_typed(
                &mut arena,
                &bool_expr(col("a", 1), BinOp::Eq, int_lit(2)),
                crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
            )
            .unwrap(),
            intern_typed(
                &mut arena,
                &bool_expr(col("a", 1), BinOp::Eq, int_lit(3)),
                crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
            )
            .unwrap(),
        ];
        let expr = scalar_expr::combine_disjuncts(
            &mut arena,
            exprs,
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap()
        .unwrap();
        let mut branches = Vec::new();
        scalar_expr::split_disjuncts(&arena, expr, &mut branches);
        let branch_debugs: Vec<String> = branches
            .into_iter()
            .map(|branch| format!("{:?}", materialize(&arena, branch).kind))
            .collect();

        assert_eq!(branch_debugs.len(), 3);
        assert!(branch_debugs[0].contains("Int(1)"));
        assert!(branch_debugs[1].contains("Int(2)"));
        assert!(branch_debugs[2].contains("Int(3)"));
    }

    #[test]
    fn non_deterministic_function_is_detected() {
        let expr = TypedExpr {
            kind: ExprKind::FunctionCall {
                binding: crate::analysis::test_function_binding(
                    "rand",
                    &[],
                    DataType::Float64,
                    false,
                    crate::functions::FunctionVolatility::Volatile,
                ),
                volatility: crate::functions::FunctionVolatility::Volatile,
                name: "rand".to_string(),
                args: (vec![]).into(),
                distinct: false,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Float64, false),
        };

        let mut arena = ScalarArena::new();
        let id = intern_typed(
            &mut arena,
            &expr,
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();
        assert!(scalar_expr::contains_non_deterministic_function(&arena, id));
    }

    #[test]
    fn curdate_is_detected_as_non_deterministic() {
        let mut arena = ScalarArena::new();
        let id = intern_typed(
            &mut arena,
            &func(
                "curdate",
                vec![],
                crate::functions::FunctionVolatility::Volatile,
            ),
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();
        assert!(scalar_expr::contains_non_deterministic_function(&arena, id));
    }

    #[test]
    fn nested_scalar_function_argument_is_checked_for_non_determinism() {
        let expr = func(
            "if",
            vec![
                col("a", 1),
                func(
                    "curdate",
                    vec![],
                    crate::functions::FunctionVolatility::Volatile,
                ),
                int_lit(1),
            ],
            crate::functions::FunctionVolatility::Immutable,
        );

        let mut arena = ScalarArena::new();
        let id = intern_typed(
            &mut arena,
            &expr,
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap();
        assert!(scalar_expr::contains_non_deterministic_function(&arena, id));
    }
}
