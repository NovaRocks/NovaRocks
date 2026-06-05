use std::collections::{BTreeSet, HashSet};

use arrow::datatypes::DataType;

use crate::sql::analysis::{BinOp, ExprKind, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::rules::utils::collect_column_id_refs;

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
    pub(crate) expr: TypedExpr,
    pub(crate) referenced_ids: BTreeSet<ColumnId>,
    pub(crate) key: PredicateKey,
    pub(crate) origin: PredicateOrigin,
    pub(crate) derived: PredicateDerivedKind,
    pub(crate) deterministic: bool,
}

impl PredicateGroup {
    pub(crate) fn new(
        expr: TypedExpr,
        origin: PredicateOrigin,
        derived: PredicateDerivedKind,
    ) -> Self {
        let referenced_ids = collect_column_id_refs(&expr).into_iter().collect();
        let key = predicate_key(&expr);
        let deterministic = !contains_non_deterministic_function(&expr);
        Self {
            expr,
            referenced_ids,
            key,
            origin,
            derived,
            deterministic,
        }
    }

    pub(crate) fn from_predicate(expr: TypedExpr, origin: PredicateOrigin) -> Vec<Self> {
        split_and_owned(expr)
            .into_iter()
            .map(|expr| Self::new(expr, origin, PredicateDerivedKind::None))
            .collect()
    }
}

pub(crate) fn predicate_key(expr: &TypedExpr) -> PredicateKey {
    PredicateKey(format!("{:?}", expr.kind))
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

pub(crate) fn exprs_from_groups(groups: Vec<PredicateGroup>) -> Vec<TypedExpr> {
    groups.into_iter().map(|group| group.expr).collect()
}

pub(crate) fn combine_or(mut exprs: Vec<TypedExpr>) -> TypedExpr {
    assert!(!exprs.is_empty());
    let mut result = exprs.pop().unwrap();
    while let Some(left) = exprs.pop() {
        result = TypedExpr {
            data_type: DataType::Boolean,
            nullable: left.nullable || result.nullable,
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Or,
                right: Box::new(result),
            },
        };
    }
    result
}

pub(crate) fn split_or_refs(expr: &TypedExpr) -> Vec<&TypedExpr> {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::Or,
            right,
        } => {
            let mut out = split_or_refs(left);
            out.extend(split_or_refs(right));
            out
        }
        ExprKind::Nested(inner) => split_or_refs(inner),
        _ => vec![expr],
    }
}

pub(crate) fn split_and_refs(expr: &TypedExpr) -> Vec<&TypedExpr> {
    match &expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let mut out = split_and_refs(left);
            out.extend(split_and_refs(right));
            out
        }
        ExprKind::Nested(inner) => split_and_refs(inner),
        _ => vec![expr],
    }
}

fn split_and_owned(expr: TypedExpr) -> Vec<TypedExpr> {
    match expr.kind {
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            let mut out = split_and_owned(*left);
            out.extend(split_and_owned(*right));
            out
        }
        ExprKind::Nested(inner) => split_and_owned(*inner),
        _ => vec![expr],
    }
}

pub(crate) fn contains_non_deterministic_function(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::FunctionCall { name, args, .. } => {
            let lower = name.to_lowercase();
            matches!(
                lower.as_str(),
                "rand"
                    | "random"
                    | "uuid"
                    | "now"
                    | "current_timestamp"
                    | "current_date"
                    | "curdate"
                    | "current_time"
                    | "curtime"
                    | "localtime"
                    | "localtimestamp"
                    | "utc_timestamp"
                    | "utc_time"
            ) || args.iter().any(contains_non_deterministic_function)
        }
        ExprKind::AggregateCall { args, order_by, .. } => {
            args.iter().any(contains_non_deterministic_function)
                || order_by
                    .iter()
                    .any(|item| contains_non_deterministic_function(&item.expr))
        }
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter().any(contains_non_deterministic_function)
                || partition_by.iter().any(contains_non_deterministic_function)
                || order_by
                    .iter()
                    .any(|item| contains_non_deterministic_function(&item.expr))
        }
        ExprKind::BinaryOp { left, right, .. } => {
            contains_non_deterministic_function(left) || contains_non_deterministic_function(right)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr) => contains_non_deterministic_function(expr),
        ExprKind::InList { expr, list, .. } => {
            contains_non_deterministic_function(expr)
                || list.iter().any(contains_non_deterministic_function)
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            contains_non_deterministic_function(expr)
                || contains_non_deterministic_function(low)
                || contains_non_deterministic_function(high)
        }
        ExprKind::Like { expr, pattern, .. } => {
            contains_non_deterministic_function(expr)
                || contains_non_deterministic_function(pattern)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| contains_non_deterministic_function(expr))
                || when_then.iter().any(|(when, then)| {
                    contains_non_deterministic_function(when)
                        || contains_non_deterministic_function(then)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| contains_non_deterministic_function(expr))
        }
        ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
            contains_non_deterministic_function(body)
        }
        ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue};
    use crate::sql::column_id::ColumnId;
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

        let groups = PredicateGroup::from_predicate(expr, PredicateOrigin::Filter);

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
        assert!(matches!(
            groups[1].expr.kind,
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

        let groups = PredicateGroup::from_predicate(expr, PredicateOrigin::Filter);

        assert_eq!(groups.len(), 2);
    }

    #[test]
    fn dedupe_keeps_first_group_for_same_canonical_key() {
        let first = PredicateGroup::new(
            bool_expr(col("a", 1), BinOp::Eq, int_lit(1)),
            PredicateOrigin::Filter,
            PredicateDerivedKind::None,
        );
        let second = PredicateGroup::new(
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

        assert_eq!(split_or_refs(&expr).len(), 3);
    }

    #[test]
    fn combine_or_round_trips_to_left_to_right_or_branches() {
        let expr = combine_or(vec![
            bool_expr(col("a", 1), BinOp::Eq, int_lit(1)),
            bool_expr(col("a", 1), BinOp::Eq, int_lit(2)),
            bool_expr(col("a", 1), BinOp::Eq, int_lit(3)),
        ]);

        let branch_debugs: Vec<String> = split_or_refs(&expr)
            .into_iter()
            .map(|branch| format!("{:?}", branch.kind))
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

        assert!(contains_non_deterministic_function(&expr));
    }

    #[test]
    fn curdate_is_detected_as_non_deterministic() {
        assert!(contains_non_deterministic_function(&func(
            "curdate",
            vec![]
        )));
    }

    #[test]
    fn nested_scalar_function_argument_is_checked_for_non_determinism() {
        let expr = func("if", vec![col("a", 1), func("curdate", vec![]), int_lit(1)]);

        assert!(contains_non_deterministic_function(&expr));
    }
}
