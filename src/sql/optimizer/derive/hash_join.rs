//! HashJoin: Shuffle / Broadcast / Colocate.
//! Refactor-phase: Broadcast / Colocate output is Any (today's behaviour).
//! Tasks 16–18 replace those branches with preserves-left output and
//! Task 19 adds required pushdown.

use crate::sql::analysis::TypedExpr;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{
    JoinDistribution, PhysicalHashJoinEqCondition, PhysicalHashJoinOp,
};
use crate::sql::optimizer::property::{DistributionSpec, OrderingSpec, PhysicalPropertySet};

use super::{DeriveOutput, DeriveRequired};

/// Join types whose output rows are streamed from the left side intact —
/// the join only filters/augments by attaching right-side data on a per-
/// row basis. For these, output distribution follows the left child.
///
/// For RightOuter / RightSemi / RightAnti / FullOuter the output is NOT
/// preserved-left (see hash_join.rs derive_output's else branch).
fn preserves_left(jk: &crate::sql::analysis::JoinKind) -> bool {
    use crate::sql::analysis::JoinKind::*;
    matches!(jk, Inner | LeftOuter | LeftSemi | LeftAnti | Cross)
}

#[allow(dead_code)]
enum Side {
    Left,
    Right,
}

fn typed_expr_to_column_id(expr: &TypedExpr) -> Option<ColumnId> {
    match &expr.kind {
        crate::sql::analysis::ExprKind::ColumnRef { column_id, .. } => Some(*column_id),
        _ => None,
    }
}

fn eq_keys_to_column_ids(
    eq_conditions: &[PhysicalHashJoinEqCondition],
    side: Side,
) -> Vec<ColumnId> {
    eq_conditions
        .iter()
        .filter_map(|eq| {
            let expr = match side {
                Side::Left => &eq.left,
                Side::Right => &eq.right,
            };
            typed_expr_to_column_id(expr)
        })
        .collect()
}

/// Given a set of column ids representing a HashPartitioned key, and the
/// join's `eq_conditions`, return the input extended with the
/// equivalence-class partner from each matching eq pair.
///
/// Rationale: after `JOIN ON L = R`, output rows satisfy `L == R`, so the
/// output is HashPartitioned by `L` iff it is HashPartitioned by `R`.
/// Carrying both ids in the output's HashPartitioned vector lets the
/// optimizer's containAll `satisfies` check accept a downstream requirement
/// on either side — even when join children get re-oriented (commutativity
/// or `orient_eq_pair` ambiguity from colliding column names) so that the
/// physical hash key is on the other side.
fn expand_with_eq_equivalents(
    cols: &[ColumnId],
    eq_conditions: &[PhysicalHashJoinEqCondition],
) -> Vec<ColumnId> {
    let mut out: Vec<ColumnId> = cols.to_vec();
    for eq in eq_conditions {
        let (Some(lc), Some(rc)) = (
            typed_expr_to_column_id(&eq.left),
            typed_expr_to_column_id(&eq.right),
        ) else {
            continue;
        };
        if out.contains(&lc) && !out.contains(&rc) {
            out.push(rc);
        }
        if out.contains(&rc) && !out.contains(&lc) {
            out.push(lc);
        }
    }
    out
}

impl DeriveOutput for PhysicalHashJoinOp {
    fn derive_output(&self, children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        match self.distribution {
            JoinDistribution::Shuffle => {
                // Symmetric over both sides of each eq pair — a shuffle join
                // partitions both inputs on their respective eq columns, so
                // its output's HashPartitioned key is an equivalence class
                // containing every eq column from either side.
                let mut cols = eq_keys_to_column_ids(&self.eq_conditions, Side::Left);
                for rc in eq_keys_to_column_ids(&self.eq_conditions, Side::Right) {
                    if !cols.contains(&rc) {
                        cols.push(rc);
                    }
                }
                PhysicalPropertySet {
                    distribution: if cols.is_empty() {
                        DistributionSpec::Any
                    } else {
                        DistributionSpec::HashPartitioned(cols)
                    },
                    ordering: OrderingSpec::Any,
                }
            }
            JoinDistribution::Broadcast | JoinDistribution::Colocate => {
                if preserves_left(&self.join_type) {
                    let left = children
                        .first()
                        .copied()
                        .cloned()
                        .unwrap_or_else(PhysicalPropertySet::any);
                    // Enrich left's HashPartitioned key with eq-equivalents
                    // so a downstream requirement keyed on the OTHER side
                    // of an eq pair (e.g. after JoinCommutativity put the
                    // hash-providing side on the right) is also satisfied.
                    let distribution = match left.distribution {
                        DistributionSpec::HashPartitioned(cols) => {
                            DistributionSpec::HashPartitioned(expand_with_eq_equivalents(
                                &cols,
                                &self.eq_conditions,
                            ))
                        }
                        other => other,
                    };
                    PhysicalPropertySet {
                        distribution,
                        ordering: OrderingSpec::Any,
                    }
                } else {
                    PhysicalPropertySet::any()
                }
            }
        }
    }
}

impl DeriveRequired for PhysicalHashJoinOp {
    fn derive_required(
        &self,
        _parent_required: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        match self.distribution {
            JoinDistribution::Shuffle => {
                let all_cols: Vec<ColumnId> = self
                    .eq_conditions
                    .iter()
                    .flat_map(|eq| {
                        let mut v = Vec::new();
                        if let Some(c) = typed_expr_to_column_id(&eq.left) {
                            v.push(c);
                        }
                        if let Some(c) = typed_expr_to_column_id(&eq.right) {
                            v.push(c);
                        }
                        v
                    })
                    .collect();
                vec![
                    PhysicalPropertySet {
                        distribution: if all_cols.is_empty() {
                            DistributionSpec::Any
                        } else {
                            DistributionSpec::HashPartitioned(all_cols.clone())
                        },
                        ordering: OrderingSpec::Any,
                    },
                    PhysicalPropertySet {
                        distribution: if all_cols.is_empty() {
                            DistributionSpec::Any
                        } else {
                            DistributionSpec::HashPartitioned(all_cols)
                        },
                        ordering: OrderingSpec::Any,
                    },
                ]
            }
            JoinDistribution::Broadcast => {
                // Do NOT propagate the parent's HashPartitioned requirement
                // into the LEFT child. Pushing HashPart([X]) down can place
                // a Distribution enforcer over a child whose logical scope
                // does not contain X (after JoinCommutativity swaps children
                // or when the parent's required col is the RIGHT side of an
                // eq pair — `orient_eq_pair` cannot distinguish these when
                // both children share a column name like `c0`). Instead,
                // rely on `derive_output`'s eq-equivalent enrichment to let
                // the join's output satisfy the parent natively; if not,
                // the optimizer places a single Distribution enforcer ON
                // TOP of the join, where every column the parent named is
                // in scope.
                vec![PhysicalPropertySet::any(), PhysicalPropertySet::gather()]
            }
            JoinDistribution::Colocate => {
                vec![PhysicalPropertySet::any(), PhysicalPropertySet::any()]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, JoinKind, TypedExpr};
    use crate::sql::column_id::ColumnId;

    fn col(id: u32) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: format!("c{id}"),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        }
    }

    fn broadcast_inner(eq_left: u32, eq_right: u32) -> PhysicalHashJoinOp {
        PhysicalHashJoinOp {
            join_type: JoinKind::Inner,
            eq_conditions: vec![PhysicalHashJoinEqCondition {
                left: col(eq_left),
                right: col(eq_right),
                null_safe: false,
            }],
            other_condition: None,
            distribution: JoinDistribution::Broadcast,
        }
    }

    #[test]
    fn hash_join_broadcast_inner_preserves_left_distribution() {
        let op = broadcast_inner(10, 20);
        let left_out = PhysicalPropertySet {
            distribution: DistributionSpec::HashPartitioned(vec![ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let right_out = PhysicalPropertySet::gather();
        let out = op.derive_output(&[&left_out, &right_out]);
        // The join's output carries the eq-equivalence class: a left-side
        // HashPartitioned([10]) becomes HashPartitioned([10, 20]) because the
        // eq `10 = 20` makes both columns equivalent on the join output.
        match &out.distribution {
            DistributionSpec::HashPartitioned(cols) => {
                let ids: std::collections::HashSet<ColumnId> = cols.iter().copied().collect();
                assert!(ids.contains(&ColumnId(10)), "expected ColumnId(10), got {ids:?}");
                assert!(ids.contains(&ColumnId(20)), "expected ColumnId(20), got {ids:?}");
            }
            other => panic!("expected HashPartitioned([10, 20]), got {other:?}"),
        }
        assert_eq!(out.ordering, OrderingSpec::Any);
    }

    #[test]
    fn required_input_shuffle_join() {
        let left_key = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(6),
                qualifier: Some("a".into()),
                column: "id".into(),
            },
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
        };
        let right_key = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(7),
                qualifier: Some("b".into()),
                column: "id".into(),
            },
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
        };
        let op = PhysicalHashJoinOp {
            join_type: crate::sql::analysis::JoinKind::Inner,
            eq_conditions: vec![PhysicalHashJoinEqCondition {
                left: left_key,
                right: right_key,
                null_safe: false,
            }],
            other_condition: None,
            distribution: JoinDistribution::Shuffle,
        };
        let reqs = op.derive_required(&PhysicalPropertySet::any(), 2);
        assert_eq!(reqs.len(), 2);

        // Design note (mirrors the production code path in this file): a
        // shuffle join's required_input_properties provides ALL eq column
        // refs (from both sides) to each child. Fragment builder resolves
        // only those that exist in each child's scope. This gives the
        // optimizer freedom when JoinCommutativity swaps children and the
        // eq_condition pair order becomes ambiguous. We therefore check
        // that both "a.id" and "b.id" appear on each side, regardless of
        // index order.
        for (side_label, req) in [("left", &reqs[0]), ("right", &reqs[1])] {
            match &req.distribution {
                DistributionSpec::HashPartitioned(cols) => {
                    assert_eq!(
                        cols.len(),
                        2,
                        "{} side should receive both eq column ids",
                        side_label
                    );
                    // Both sides should get ColumnId(6) (a.id) and ColumnId(7) (b.id).
                    let ids: std::collections::HashSet<ColumnId> = cols.iter().copied().collect();
                    assert!(
                        ids.contains(&ColumnId(6)),
                        "{} side missing ColumnId(6), got {:?}",
                        side_label,
                        ids
                    );
                    assert!(
                        ids.contains(&ColumnId(7)),
                        "{} side missing ColumnId(7), got {:?}",
                        side_label,
                        ids
                    );
                }
                other => panic!(
                    "expected HashPartitioned for {} side, got {:?}",
                    side_label, other
                ),
            }
        }
    }

    // ── Task 17: Broadcast non-preserves-left → output stays Any ─────────────

    fn broadcast_with_type(jk: crate::sql::analysis::JoinKind) -> PhysicalHashJoinOp {
        PhysicalHashJoinOp {
            join_type: jk,
            eq_conditions: vec![PhysicalHashJoinEqCondition {
                left: col(10),
                right: col(20),
                null_safe: false,
            }],
            other_condition: None,
            distribution: JoinDistribution::Broadcast,
        }
    }

    #[test]
    fn hash_join_broadcast_right_outer_returns_any() {
        let op = broadcast_with_type(crate::sql::analysis::JoinKind::RightOuter);
        let left_out = PhysicalPropertySet {
            distribution: DistributionSpec::HashPartitioned(vec![ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let out = op.derive_output(&[&left_out, &PhysicalPropertySet::gather()]);
        assert_eq!(out.distribution, DistributionSpec::Any);
    }

    #[test]
    fn hash_join_broadcast_right_semi_returns_any() {
        let op = broadcast_with_type(crate::sql::analysis::JoinKind::RightSemi);
        let left_out = PhysicalPropertySet {
            distribution: DistributionSpec::HashPartitioned(vec![ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let out = op.derive_output(&[&left_out, &PhysicalPropertySet::gather()]);
        assert_eq!(out.distribution, DistributionSpec::Any);
    }

    #[test]
    fn hash_join_broadcast_right_anti_returns_any() {
        let op = broadcast_with_type(crate::sql::analysis::JoinKind::RightAnti);
        let left_out = PhysicalPropertySet {
            distribution: DistributionSpec::HashPartitioned(vec![ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let out = op.derive_output(&[&left_out, &PhysicalPropertySet::gather()]);
        assert_eq!(out.distribution, DistributionSpec::Any);
    }

    #[test]
    fn hash_join_broadcast_full_outer_returns_any() {
        let op = broadcast_with_type(crate::sql::analysis::JoinKind::FullOuter);
        let left_out = PhysicalPropertySet {
            distribution: DistributionSpec::HashPartitioned(vec![ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let out = op.derive_output(&[&left_out, &PhysicalPropertySet::gather()]);
        assert_eq!(out.distribution, DistributionSpec::Any);
    }

    // ── Task 18: Colocate preserves-left + negative ───────────────────────────

    fn colocate_inner(eq_left: u32, eq_right: u32) -> PhysicalHashJoinOp {
        PhysicalHashJoinOp {
            join_type: crate::sql::analysis::JoinKind::Inner,
            eq_conditions: vec![PhysicalHashJoinEqCondition {
                left: col(eq_left),
                right: col(eq_right),
                null_safe: false,
            }],
            other_condition: None,
            distribution: JoinDistribution::Colocate,
        }
    }

    #[test]
    fn hash_join_colocate_inner_preserves_left_distribution() {
        let op = colocate_inner(10, 20);
        let left_out = PhysicalPropertySet {
            distribution: DistributionSpec::HashPartitioned(vec![ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let right_out = PhysicalPropertySet {
            distribution: DistributionSpec::HashPartitioned(vec![ColumnId(20)]),
            ordering: OrderingSpec::Any,
        };
        let out = op.derive_output(&[&left_out, &right_out]);
        // Like Broadcast preserves-left, Colocate enriches the left's
        // HashPartitioned with its eq-equivalence partner so a downstream
        // requirement on either side of the eq pair is satisfied.
        match &out.distribution {
            DistributionSpec::HashPartitioned(cols) => {
                let ids: std::collections::HashSet<ColumnId> = cols.iter().copied().collect();
                assert!(ids.contains(&ColumnId(10)), "expected ColumnId(10), got {ids:?}");
                assert!(ids.contains(&ColumnId(20)), "expected ColumnId(20), got {ids:?}");
            }
            other => panic!("expected HashPartitioned([10, 20]), got {other:?}"),
        }
    }

    #[test]
    fn hash_join_colocate_right_outer_returns_any() {
        let mut op = colocate_inner(10, 20);
        op.join_type = crate::sql::analysis::JoinKind::RightOuter;
        let left_out = PhysicalPropertySet {
            distribution: DistributionSpec::HashPartitioned(vec![ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let right_out = PhysicalPropertySet {
            distribution: DistributionSpec::HashPartitioned(vec![ColumnId(20)]),
            ordering: OrderingSpec::Any,
        };
        let out = op.derive_output(&[&left_out, &right_out]);
        assert_eq!(out.distribution, DistributionSpec::Any);
    }

    // ── Broadcast required: never propagates HashPart to left ───────────────
    //
    // A Broadcast join's left-required is now always Any (gather-on-right).
    // Pushing parent's HashPart([X]) into the LEFT child was unsafe: when
    // CBO swapped the children (or when the parent's required col is the
    // RIGHT side of the eq under ambiguous orient_eq_pair), the enforcer
    // ended up over a child whose logical scope did not contain X.
    // derive_output's eq-equivalence enrichment lets the join's output
    // satisfy the parent natively; if not, the enforcer is placed on the
    // join's output (where every column the parent named is in scope).

    #[test]
    fn hash_join_broadcast_required_never_pushes_hash_to_left() {
        let op = broadcast_inner(10, 20);
        let parent = PhysicalPropertySet {
            distribution: DistributionSpec::HashPartitioned(vec![ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let reqs = op.derive_required(&parent, 2);
        assert_eq!(reqs[0].distribution, DistributionSpec::Any);
        assert_eq!(reqs[1].distribution, DistributionSpec::Gather);
    }

    #[test]
    fn hash_join_broadcast_required_does_not_push_gather() {
        let op = broadcast_inner(10, 20);
        let parent = PhysicalPropertySet::gather();
        let reqs = op.derive_required(&parent, 2);
        assert_eq!(reqs[0].distribution, DistributionSpec::Any);
        assert_eq!(reqs[1].distribution, DistributionSpec::Gather);
    }

    #[test]
    fn hash_join_broadcast_required_right_outer_returns_any_gather() {
        let op = broadcast_with_type(crate::sql::analysis::JoinKind::RightOuter);
        let parent = PhysicalPropertySet {
            distribution: DistributionSpec::HashPartitioned(vec![ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let reqs = op.derive_required(&parent, 2);
        assert_eq!(reqs[0].distribution, DistributionSpec::Any);
        assert_eq!(reqs[1].distribution, DistributionSpec::Gather);
    }

    #[test]
    fn hash_join_colocate_required_returns_any_any() {
        let op = colocate_inner(10, 20);
        let parent = PhysicalPropertySet {
            distribution: DistributionSpec::HashPartitioned(vec![ColumnId(10)]),
            ordering: OrderingSpec::Any,
        };
        let reqs = op.derive_required(&parent, 2);
        assert_eq!(reqs[0].distribution, DistributionSpec::Any);
        assert_eq!(reqs[1].distribution, DistributionSpec::Any);
    }

    // ── eq-equivalence enrichment of derive_output ─────────────────────────

    #[test]
    fn broadcast_output_left_any_stays_any() {
        // No HashPartitioned to enrich → output should pass through unchanged.
        let op = broadcast_inner(10, 20);
        let left_out = PhysicalPropertySet::any();
        let right_out = PhysicalPropertySet::gather();
        let out = op.derive_output(&[&left_out, &right_out]);
        assert_eq!(out.distribution, DistributionSpec::Any);
    }

    #[test]
    fn broadcast_output_enrichment_is_idempotent() {
        // When left already contains both eq columns, enrichment must not
        // duplicate ids.
        let op = broadcast_inner(10, 20);
        let left_out = PhysicalPropertySet {
            distribution: DistributionSpec::HashPartitioned(vec![ColumnId(10), ColumnId(20)]),
            ordering: OrderingSpec::Any,
        };
        let right_out = PhysicalPropertySet::gather();
        let out = op.derive_output(&[&left_out, &right_out]);
        match &out.distribution {
            DistributionSpec::HashPartitioned(cols) => {
                assert_eq!(cols.len(), 2, "no duplicates expected, got {cols:?}");
                let ids: std::collections::HashSet<ColumnId> = cols.iter().copied().collect();
                assert!(ids.contains(&ColumnId(10)));
                assert!(ids.contains(&ColumnId(20)));
            }
            other => panic!("expected HashPartitioned, got {other:?}"),
        }
    }

    #[test]
    fn broadcast_output_enrichment_via_right_eq_id() {
        // Mirrors the failing-test shape: after CBO swap, LEFT child provides
        // HashPartitioned([RIGHT eq column id]). Enrichment must add the
        // LEFT eq column id so a downstream requirement keyed on the LEFT
        // side of the original SQL eq is still satisfied.
        let op = broadcast_inner(10, 20);
        let left_out = PhysicalPropertySet {
            distribution: DistributionSpec::HashPartitioned(vec![ColumnId(20)]),
            ordering: OrderingSpec::Any,
        };
        let right_out = PhysicalPropertySet::gather();
        let out = op.derive_output(&[&left_out, &right_out]);
        match &out.distribution {
            DistributionSpec::HashPartitioned(cols) => {
                let ids: std::collections::HashSet<ColumnId> = cols.iter().copied().collect();
                assert!(ids.contains(&ColumnId(10)));
                assert!(ids.contains(&ColumnId(20)));
            }
            other => panic!("expected HashPartitioned([10, 20]), got {other:?}"),
        }
    }

    #[test]
    fn shuffle_output_is_symmetric_over_eq_cols() {
        // Shuffle's output's HashPartitioned vector must contain BOTH sides
        // of every eq pair — a shuffle partitions both inputs on their
        // respective eq columns, so the output is hash-equivalent in either.
        let op = PhysicalHashJoinOp {
            join_type: crate::sql::analysis::JoinKind::Inner,
            eq_conditions: vec![PhysicalHashJoinEqCondition {
                left: col(10),
                right: col(20),
                null_safe: false,
            }],
            other_condition: None,
            distribution: JoinDistribution::Shuffle,
        };
        let out = op.derive_output(&[&PhysicalPropertySet::any(), &PhysicalPropertySet::any()]);
        match &out.distribution {
            DistributionSpec::HashPartitioned(cols) => {
                let ids: std::collections::HashSet<ColumnId> = cols.iter().copied().collect();
                assert!(ids.contains(&ColumnId(10)));
                assert!(ids.contains(&ColumnId(20)));
            }
            other => panic!("expected HashPartitioned([10, 20]), got {other:?}"),
        }
    }
}
