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

impl DeriveOutput for PhysicalHashJoinOp {
    fn derive_output(&self, children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        match self.distribution {
            JoinDistribution::Shuffle => {
                let cols = eq_keys_to_column_ids(&self.eq_conditions, Side::Left);
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
                    PhysicalPropertySet {
                        distribution: left.distribution,
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
        _parent: &PhysicalPropertySet,
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
                // Refactor-phase placeholder. Task 19 adds pushdown.
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
        assert_eq!(
            out.distribution,
            DistributionSpec::HashPartitioned(vec![ColumnId(10)])
        );
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
}
