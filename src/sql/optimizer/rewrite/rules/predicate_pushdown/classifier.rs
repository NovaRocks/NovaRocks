use std::collections::HashSet;

use crate::sql::analysis::JoinKind;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::rewrite::rules::predicate_pushdown::predicate_group::PredicateGroup;

#[derive(Clone, Debug, Default)]
pub(crate) struct ClassifiedPredicates {
    pub(crate) left_pushdown: Vec<PredicateGroup>,
    pub(crate) right_pushdown: Vec<PredicateGroup>,
    pub(crate) join_residual: Vec<PredicateGroup>,
    pub(crate) remain_above_join: Vec<PredicateGroup>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SideTarget {
    Left,
    Right,
    Both,
    Neither,
    Outside,
}

pub(crate) fn classify_predicate_groups(
    join_type: JoinKind,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
    groups: Vec<PredicateGroup>,
) -> ClassifiedPredicates {
    let mut out = ClassifiedPredicates::default();
    for group in groups {
        match classify_group(&group, left_ids, right_ids) {
            SideTarget::Left if may_push_left(join_type) => out.left_pushdown.push(group),
            SideTarget::Left => out.remain_above_join.push(group),
            SideTarget::Right if may_push_right(join_type) => out.right_pushdown.push(group),
            SideTarget::Right => out.remain_above_join.push(group),
            SideTarget::Both if may_place_cross_side_in_join(join_type) => {
                out.join_residual.push(group)
            }
            SideTarget::Both => out.remain_above_join.push(group),
            SideTarget::Neither if may_push_left(join_type) => out.left_pushdown.push(group),
            SideTarget::Neither => out.remain_above_join.push(group),
            SideTarget::Outside => out.remain_above_join.push(group),
        }
    }
    out
}

fn classify_group(
    group: &PredicateGroup,
    left_ids: &HashSet<ColumnId>,
    right_ids: &HashSet<ColumnId>,
) -> SideTarget {
    if group.referenced_ids.is_empty() {
        return SideTarget::Neither;
    }
    let mut left = false;
    let mut right = false;
    for id in &group.referenced_ids {
        match (left_ids.contains(id), right_ids.contains(id)) {
            (true, false) => left = true,
            (false, true) => right = true,
            (true, true) => return SideTarget::Outside,
            (false, false) => return SideTarget::Outside,
        }
    }
    match (left, right) {
        (true, false) => SideTarget::Left,
        (false, true) => SideTarget::Right,
        (true, true) => SideTarget::Both,
        (false, false) => SideTarget::Neither,
    }
}

fn may_push_left(join_type: JoinKind) -> bool {
    matches!(
        join_type,
        JoinKind::Inner
            | JoinKind::Cross
            | JoinKind::LeftOuter
            | JoinKind::LeftSemi
            | JoinKind::LeftAnti
            | JoinKind::NullAwareLeftAnti
    )
}

fn may_push_right(join_type: JoinKind) -> bool {
    matches!(
        join_type,
        JoinKind::Inner
            | JoinKind::Cross
            | JoinKind::RightOuter
            | JoinKind::RightSemi
            | JoinKind::RightAnti
    )
}

fn may_place_cross_side_in_join(join_type: JoinKind) -> bool {
    matches!(join_type, JoinKind::Inner | JoinKind::Cross)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::rules::predicate_pushdown::predicate_group::{
        PredicateDerivedKind, PredicateGroup, PredicateOrigin,
    };
    use crate::sql::optimizer::scalar::{ScalarArena, intern_typed};
    use arrow::datatypes::DataType;
    use std::collections::HashSet;

    fn col(name: &str, id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: Some(name.chars().next().unwrap().to_string()),
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

    fn eq(left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op: BinOp::Eq,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    fn group(arena: &mut ScalarArena, expr: TypedExpr) -> PredicateGroup {
        let expr = intern_typed(arena, &expr);
        PredicateGroup::new(
            arena,
            expr,
            PredicateOrigin::Filter,
            PredicateDerivedKind::None,
        )
    }

    fn ids(values: &[u32]) -> HashSet<ColumnId> {
        values.iter().copied().map(ColumnId::new_for_test).collect()
    }

    #[test]
    fn inner_join_classifies_left_right_and_join_groups_by_column_id() {
        let mut arena = ScalarArena::new();
        let placement = classify_predicate_groups(
            JoinKind::Inner,
            &ids(&[1]),
            &ids(&[2]),
            vec![
                group(&mut arena, eq(col("a", 1), int_lit(10))),
                group(&mut arena, eq(col("b", 2), int_lit(20))),
                group(&mut arena, eq(col("a", 1), col("b", 2))),
            ],
        );

        assert_eq!(placement.left_pushdown.len(), 1);
        assert_eq!(placement.right_pushdown.len(), 1);
        assert_eq!(placement.join_residual.len(), 1);
        assert!(placement.remain_above_join.is_empty());
    }

    #[test]
    fn left_outer_keeps_right_filter_above_join() {
        let mut arena = ScalarArena::new();
        let placement = classify_predicate_groups(
            JoinKind::LeftOuter,
            &ids(&[1]),
            &ids(&[2]),
            vec![group(&mut arena, eq(col("b", 2), int_lit(20)))],
        );

        assert!(placement.right_pushdown.is_empty());
        assert_eq!(placement.remain_above_join.len(), 1);
    }

    #[test]
    fn full_outer_keeps_single_side_filters_above_join() {
        let mut arena = ScalarArena::new();
        let placement = classify_predicate_groups(
            JoinKind::FullOuter,
            &ids(&[1]),
            &ids(&[2]),
            vec![
                group(&mut arena, eq(col("a", 1), int_lit(10))),
                group(&mut arena, eq(col("b", 2), int_lit(20))),
            ],
        );

        assert_eq!(placement.remain_above_join.len(), 2);
    }

    #[test]
    fn null_aware_left_anti_pushes_left_filter_to_probe_side() {
        let mut arena = ScalarArena::new();
        let placement = classify_predicate_groups(
            JoinKind::NullAwareLeftAnti,
            &ids(&[1]),
            &ids(&[2]),
            vec![group(&mut arena, eq(col("a", 1), int_lit(10)))],
        );

        assert_eq!(placement.left_pushdown.len(), 1);
        assert!(placement.remain_above_join.is_empty());
    }

    #[test]
    fn constants_follow_left_pushdown_guard() {
        let mut arena = ScalarArena::new();
        let inner = classify_predicate_groups(
            JoinKind::Inner,
            &ids(&[1]),
            &ids(&[2]),
            vec![group(&mut arena, eq(int_lit(1), int_lit(1)))],
        );
        assert_eq!(inner.left_pushdown.len(), 1);
        assert!(inner.remain_above_join.is_empty());

        let left_outer = classify_predicate_groups(
            JoinKind::LeftOuter,
            &ids(&[1]),
            &ids(&[2]),
            vec![group(&mut arena, eq(int_lit(1), int_lit(1)))],
        );
        assert_eq!(left_outer.left_pushdown.len(), 1);
        assert!(left_outer.remain_above_join.is_empty());

        let full_outer = classify_predicate_groups(
            JoinKind::FullOuter,
            &ids(&[1]),
            &ids(&[2]),
            vec![group(&mut arena, eq(int_lit(1), int_lit(1)))],
        );
        assert!(full_outer.left_pushdown.is_empty());
        assert_eq!(full_outer.remain_above_join.len(), 1);
    }

    #[test]
    fn unknown_and_overlapping_column_ids_remain_above_join() {
        let mut arena = ScalarArena::new();
        let unknown = classify_predicate_groups(
            JoinKind::Inner,
            &ids(&[1]),
            &ids(&[2]),
            vec![group(&mut arena, eq(col("c", 3), int_lit(30)))],
        );
        assert_eq!(unknown.remain_above_join.len(), 1);
        assert!(unknown.left_pushdown.is_empty());
        assert!(unknown.right_pushdown.is_empty());
        assert!(unknown.join_residual.is_empty());

        let overlapping = classify_predicate_groups(
            JoinKind::Inner,
            &ids(&[1]),
            &ids(&[1]),
            vec![group(&mut arena, eq(col("a", 1), int_lit(10)))],
        );
        assert_eq!(overlapping.remain_above_join.len(), 1);
        assert!(overlapping.left_pushdown.is_empty());
        assert!(overlapping.right_pushdown.is_empty());
        assert!(overlapping.join_residual.is_empty());
    }
}
