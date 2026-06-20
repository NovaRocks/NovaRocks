use std::collections::{BTreeSet, HashSet};

use crate::sql::column_id::ColumnId;
use crate::sql::common::BinOp;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};
use crate::sql::optimizer::scalar_expr;

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
    NotNull,
}

#[derive(Clone, Debug)]
pub(crate) struct PredicateGroup {
    pub(crate) expr: ScalarId,
    pub(crate) referenced_ids: BTreeSet<ColumnId>,
    pub(crate) key: PredicateKey,
    pub(crate) origin: PredicateOrigin,
    pub(crate) derived: PredicateDerivedKind,
    pub(crate) deterministic: bool,
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
    PredicateKey(canonical_expr_key(arena, expr))
}

fn canonical_expr_key(arena: &ScalarArena, expr: ScalarId) -> String {
    match arena.node(expr) {
        ScalarNode::Nested(inner) => canonical_expr_key(arena, *inner),
        ScalarNode::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => canonical_bool_key(arena, "AND", *left, *right),
        ScalarNode::BinaryOp {
            left,
            op: BinOp::Or,
            right,
        } => canonical_bool_key(arena, "OR", *left, *right),
        ScalarNode::BinaryOp { left, op, right } => {
            // For commutative ops (Eq, Ne) canonicalize operand order so that
            // `a = b` and `b = a` produce the same key regardless of which
            // direction the arena normalization swapped them.
            let lk = canonical_expr_key(arena, *left);
            let rk = canonical_expr_key(arena, *right);
            let (lk, rk) = if matches!(op, BinOp::Eq | BinOp::Ne) && lk > rk {
                (rk, lk)
            } else {
                (lk, rk)
            };
            format!("BinaryOp({op:?},{lk},{rk})")
        }
        ScalarNode::UnaryOp { op, child } => {
            format!("UnaryOp({op:?},{})", canonical_expr_key(arena, *child))
        }
        ScalarNode::FunctionCall {
            name,
            args,
            distinct,
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
        } => format!(
            "AggregateCall({name},{distinct},{},{order_by:?})",
            canonical_expr_list_key(arena, args)
        ),
        ScalarNode::Cast { child, target } => {
            format!("Cast({},{target:?})", canonical_expr_key(arena, *child))
        }
        ScalarNode::IsNull { child, negated } => {
            format!("IsNull({},{negated})", canonical_expr_key(arena, *child))
        }
        ScalarNode::InList {
            child,
            list,
            negated,
        } => format!(
            "InList({},{},{negated})",
            canonical_expr_key(arena, *child),
            canonical_expr_list_key(arena, list)
        ),
        ScalarNode::Between {
            child,
            low,
            high,
            negated,
        } => format!(
            "Between({},{},{},{negated})",
            canonical_expr_key(arena, *child),
            canonical_expr_key(arena, *low),
            canonical_expr_key(arena, *high)
        ),
        ScalarNode::Like {
            child,
            pattern,
            negated,
        } => format!(
            "Like({},{},{negated})",
            canonical_expr_key(arena, *child),
            canonical_expr_key(arena, *pattern)
        ),
        ScalarNode::Case {
            operand,
            when_then,
            else_expr,
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
        ScalarNode::IsTruthValue {
            child,
            value,
            negated,
        } => format!(
            "IsTruthValue({},{value},{negated})",
            canonical_expr_key(arena, *child)
        ),
        ScalarNode::WindowCall {
            name,
            args,
            distinct,
            partition_by,
            order_by,
            window_frame,
            ignore_nulls,
        } => format!(
            "WindowCall({name},{distinct},{},{},{order_by:?},{window_frame:?},{ignore_nulls})",
            canonical_expr_list_key(arena, args),
            canonical_expr_list_key(arena, partition_by)
        ),
        ScalarNode::Lambda { params, body } => {
            format!("Lambda({params:?},{})", canonical_expr_key(arena, *body))
        }
        ScalarNode::ColumnRef(_) | ScalarNode::LambdaParamRef { .. } | ScalarNode::Literal(_) => {
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
    terms.sort();
    format!("{op_name}({})", terms.join(","))
}

fn collect_bool_terms(arena: &ScalarArena, expr: ScalarId, op_name: &str, out: &mut Vec<String>) {
    match (arena.node(expr), op_name) {
        (
            ScalarNode::BinaryOp {
                left,
                op: BinOp::And,
                right,
            },
            "AND",
        )
        | (
            ScalarNode::BinaryOp {
                left,
                op: BinOp::Or,
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
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::scalar::{ScalarArena, intern_typed, materialize};
    use crate::sql::optimizer::scalar_expr;
    use arrow::datatypes::DataType;

    fn col(name: &str, id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some("t".to_string()),
                column: name.to_string(),
            },
            data_type: DataType::Int32,
            nullable: true,
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn bool_expr(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn func(name: &str, args: Vec<TypedExpr>) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::FunctionCall {
                name: name.to_string(),
                args,
                distinct: false,
            },
            data_type: DataType::Int64,
            nullable: true,
        }
    }

    fn groups_from_typed(arena: &mut ScalarArena, expr: TypedExpr) -> Vec<PredicateGroup> {
        let id = intern_typed(arena, &expr);
        PredicateGroup::from_predicate(arena, id, PredicateOrigin::Filter)
    }

    fn group_from_typed(
        arena: &mut ScalarArena,
        expr: TypedExpr,
        origin: PredicateOrigin,
        derived: PredicateDerivedKind,
    ) -> PredicateGroup {
        let id = intern_typed(arena, &expr);
        PredicateGroup::new(arena, id, origin, derived)
    }

    #[test]
    fn top_level_and_is_split_but_or_stays_atomic() {
        let expr = bool_expr(
            bool_expr(col("a", 1), BinOp::Eq, int_lit(1)),
            BinOp::And,
            bool_expr(
                bool_expr(col("b", 2), BinOp::Eq, int_lit(2)),
                BinOp::Or,
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
            ExprKind::BinaryOp { op: BinOp::Or, .. }
        ));
    }

    #[test]
    fn nested_top_level_and_is_split_into_groups() {
        let expr = TypedExpr {
            kind: ExprKind::Nested(Box::new(bool_expr(
                bool_expr(col("a", 1), BinOp::Eq, int_lit(1)),
                BinOp::And,
                bool_expr(col("b", 2), BinOp::Eq, int_lit(2)),
            ))),
            data_type: DataType::Boolean,
            nullable: true,
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
            BinOp::Or,
            bool_expr(
                bool_expr(col("a", 1), BinOp::Eq, int_lit(2)),
                BinOp::Or,
                bool_expr(col("a", 1), BinOp::Eq, int_lit(3)),
            ),
        );

        let mut arena = ScalarArena::new();
        let id = intern_typed(&mut arena, &expr);
        let mut branches = Vec::new();
        scalar_expr::split_disjuncts(&arena, id, &mut branches);
        assert_eq!(branches.len(), 3);
    }

    #[test]
    fn combine_or_round_trips_to_left_to_right_or_branches() {
        let mut arena = ScalarArena::new();
        let exprs = vec![
            intern_typed(&mut arena, &bool_expr(col("a", 1), BinOp::Eq, int_lit(1))),
            intern_typed(&mut arena, &bool_expr(col("a", 1), BinOp::Eq, int_lit(2))),
            intern_typed(&mut arena, &bool_expr(col("a", 1), BinOp::Eq, int_lit(3))),
        ];
        let expr = scalar_expr::combine_disjuncts(&mut arena, exprs).unwrap();
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
                name: "rand".to_string(),
                args: vec![],
                distinct: false,
            },
            data_type: DataType::Float64,
            nullable: false,
        };

        let mut arena = ScalarArena::new();
        let id = intern_typed(&mut arena, &expr);
        assert!(scalar_expr::contains_non_deterministic_function(&arena, id));
    }

    #[test]
    fn curdate_is_detected_as_non_deterministic() {
        let mut arena = ScalarArena::new();
        let id = intern_typed(&mut arena, &func("curdate", vec![]));
        assert!(scalar_expr::contains_non_deterministic_function(&arena, id));
    }

    #[test]
    fn nested_scalar_function_argument_is_checked_for_non_determinism() {
        let expr = func("if", vec![col("a", 1), func("curdate", vec![]), int_lit(1)]);

        let mut arena = ScalarArena::new();
        let id = intern_typed(&mut arena, &expr);
        assert!(scalar_expr::contains_non_deterministic_function(&arena, id));
    }
}
