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
    fn derive_output(&self, _children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
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
                // Refactor-phase placeholder. Replaced in Tasks 16–18.
                PhysicalPropertySet::any()
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
