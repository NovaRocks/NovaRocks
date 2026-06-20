//! Shared helpers for scalar subquery decorrelation rules.

use std::collections::HashSet;

use crate::sql::analysis::BinOp;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};

use super::scalar_utils;

pub(super) fn partition_conjuncts_opt(
    arena: &ScalarArena,
    predicate: ScalarId,
    corr_ids: &HashSet<ColumnId>,
) -> (Vec<ScalarId>, Vec<ScalarId>) {
    let mut correlated = Vec::new();
    let mut residual = Vec::new();
    for conjunct in scalar_utils::split_and(arena, predicate) {
        if scalar_utils::collect_column_ids(arena, conjunct).is_disjoint(corr_ids) {
            residual.push(conjunct);
        } else {
            correlated.push(conjunct);
        }
    }
    (correlated, residual)
}

pub(super) fn all_binary_eq_opt(arena: &ScalarArena, conjuncts: &[ScalarId]) -> bool {
    conjuncts.iter().all(|conjunct| {
        matches!(
            arena.node(*conjunct),
            ScalarNode::BinaryOp { op: BinOp::Eq, .. }
        )
    })
}

pub(super) fn orient_eq_opt(
    arena: &ScalarArena,
    conjunct: ScalarId,
    corr_ids: &HashSet<ColumnId>,
) -> Option<(ScalarId, ScalarId)> {
    scalar_utils::orient_eq(arena, conjunct, corr_ids)
}

#[cfg(test)]
mod legacy {
    use std::collections::HashSet;

    use crate::sql::analysis::{BinOp, ExprKind, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::rules::utils::{collect_column_id_refs, split_and};

    /// Split a predicate's AND-conjuncts into (correlated, residual): a conjunct is
    /// correlated iff it references any column id in `corr_ids` (an outer column).
    pub(crate) fn partition_conjuncts(
        predicate: TypedExpr,
        corr_ids: &HashSet<ColumnId>,
    ) -> (Vec<TypedExpr>, Vec<TypedExpr>) {
        let mut correlated = Vec::new();
        let mut residual = Vec::new();
        for c in split_and(predicate) {
            if collect_column_id_refs(&c).is_disjoint(corr_ids) {
                residual.push(c);
            } else {
                correlated.push(c);
            }
        }
        (correlated, residual)
    }

    /// True iff every conjunct is a binary `=` comparison (the only correlation
    /// shape decorrelation supports; mirrors StarRocks checkAllIsBinaryEQ).
    pub(crate) fn all_binary_eq(conjuncts: &[TypedExpr]) -> bool {
        conjuncts
            .iter()
            .all(|c| matches!(&c.kind, ExprKind::BinaryOp { op: BinOp::Eq, .. }))
    }

    /// For a correlated EQ conjunct `a == b`, return (outer_side, inner_side) by
    /// testing which side references an outer (corr) id. The inner side becomes a
    /// GROUP BY key; the outer side becomes the join-condition outer operand.
    pub(crate) fn orient_eq<'a>(
        conjunct: &'a TypedExpr,
        corr_ids: &HashSet<ColumnId>,
    ) -> Option<(&'a TypedExpr, &'a TypedExpr)> {
        let ExprKind::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } = &conjunct.kind
        else {
            return None;
        };
        let left_outer = !collect_column_id_refs(left).is_disjoint(corr_ids);
        let right_outer = !collect_column_id_refs(right).is_disjoint(corr_ids);
        match (left_outer, right_outer) {
            (true, false) => Some((left, right)), // (outer, inner)
            (false, true) => Some((right, left)), // swap: (outer, inner)
            _ => None, // both/neither outer → not a clean correlation key
        }
    }
}

#[cfg(test)]
pub(super) use legacy::{all_binary_eq, orient_eq, partition_conjuncts};

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::{ExprKind, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::rewrite::rules::utils::collect_column_id_refs;

    fn col_ref(id: ColumnId) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: format!("col_{}", id.0),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn binary_op(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    #[test]
    fn partition_splits_correlated_and_residual() {
        // pred = (inner.k == OUTER) AND (inner.v > 5)
        // corr_ids = {OUTER}
        // expect correlated=[inner.k==OUTER], residual=[inner.v>5]
        let inner_k = ColumnId(1); // inner.k
        let outer = ColumnId(100); // OUTER (outer column)
        let inner_v = ColumnId(2); // inner.v

        let lit_5 = TypedExpr {
            kind: ExprKind::Literal(crate::sql::analysis::LiteralValue::Int(5)),
            data_type: DataType::Int64,
            nullable: false,
        };

        // inner.k == OUTER  (correlated)
        let eq_pred = binary_op(col_ref(inner_k), BinOp::Eq, col_ref(outer));
        // inner.v > 5  (residual)
        let gt_pred = binary_op(col_ref(inner_v), BinOp::Gt, lit_5);
        // combined: (inner.k == OUTER) AND (inner.v > 5)
        let predicate = binary_op(eq_pred.clone(), BinOp::And, gt_pred.clone());

        let corr_ids: HashSet<ColumnId> = [outer].into();
        let (correlated, residual) = partition_conjuncts(predicate, &corr_ids);

        assert_eq!(correlated.len(), 1, "expected 1 correlated conjunct");
        assert_eq!(residual.len(), 1, "expected 1 residual conjunct");

        // The correlated conjunct references OUTER
        let corr_refs = collect_column_id_refs(&correlated[0]);
        assert!(
            corr_refs.contains(&outer),
            "correlated conjunct must reference OUTER"
        );

        // The residual conjunct does NOT reference OUTER
        let resid_refs = collect_column_id_refs(&residual[0]);
        assert!(
            !resid_refs.contains(&outer),
            "residual conjunct must not reference OUTER"
        );
    }

    #[test]
    fn all_binary_eq_true_for_eq_conjuncts() {
        let a = ColumnId(1);
        let b = ColumnId(2);
        let eq = binary_op(col_ref(a), BinOp::Eq, col_ref(b));
        assert!(all_binary_eq(&[eq]));
    }

    #[test]
    fn all_binary_eq_false_for_non_eq() {
        let a = ColumnId(1);
        let b = ColumnId(2);
        let gt = binary_op(col_ref(a), BinOp::Gt, col_ref(b));
        assert!(!all_binary_eq(&[gt]));
    }

    #[test]
    fn orient_eq_left_outer() {
        let outer = ColumnId(100);
        let inner = ColumnId(1);
        let eq = binary_op(col_ref(outer), BinOp::Eq, col_ref(inner));
        let corr_ids: HashSet<ColumnId> = [outer].into();
        let result = orient_eq(&eq, &corr_ids);
        assert!(result.is_some());
        let (outer_side, inner_side) = result.unwrap();
        // outer_side should reference outer, inner_side should reference inner
        assert!(collect_column_id_refs(outer_side).contains(&outer));
        assert!(collect_column_id_refs(inner_side).contains(&inner));
    }

    #[test]
    fn orient_eq_right_outer() {
        let outer = ColumnId(100);
        let inner = ColumnId(1);
        // inner == OUTER (outer is on the right side)
        let eq = binary_op(col_ref(inner), BinOp::Eq, col_ref(outer));
        let corr_ids: HashSet<ColumnId> = [outer].into();
        let result = orient_eq(&eq, &corr_ids);
        assert!(result.is_some());
        let (outer_side, inner_side) = result.unwrap();
        // orient_eq normalizes so outer is first
        assert!(collect_column_id_refs(outer_side).contains(&outer));
        assert!(collect_column_id_refs(inner_side).contains(&inner));
    }

    #[test]
    fn orient_eq_none_both_outer() {
        // OUTER_A == OUTER_B: both sides reference corr ids → None
        let outer_a = ColumnId(100);
        let outer_b = ColumnId(101);
        let eq = binary_op(col_ref(outer_a), BinOp::Eq, col_ref(outer_b));
        let corr_ids: HashSet<ColumnId> = [outer_a, outer_b].into();
        assert!(orient_eq(&eq, &corr_ids).is_none());
    }

    #[test]
    fn orient_eq_none_neither_outer() {
        // inner_a == inner_b: neither side references a corr id → None
        let inner_a = ColumnId(1);
        let inner_b = ColumnId(2);
        let outer = ColumnId(100);
        let eq = binary_op(col_ref(inner_a), BinOp::Eq, col_ref(inner_b));
        let corr_ids: HashSet<ColumnId> = [outer].into();
        assert!(orient_eq(&eq, &corr_ids).is_none());
    }
}
