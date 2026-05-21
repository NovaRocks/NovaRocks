//! HashAggregate output / required derivation. Four modes:
//! Single / Local / Global / DistinctLocal / DistinctGlobal.

use crate::sql::analysis::TypedExpr;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{AggMode, PhysicalHashAggregateOp};
use crate::sql::optimizer::property::{DistributionSpec, OrderingSpec, PhysicalPropertySet};

use super::{DeriveOutput, DeriveRequired};

fn typed_expr_to_column_id(expr: &TypedExpr) -> Option<ColumnId> {
    match &expr.kind {
        crate::sql::analysis::ExprKind::ColumnRef { column_id, .. } => Some(*column_id),
        _ => None,
    }
}
fn typed_exprs_to_column_ids(exprs: &[TypedExpr]) -> Vec<ColumnId> {
    exprs.iter().filter_map(typed_expr_to_column_id).collect()
}

impl DeriveOutput for PhysicalHashAggregateOp {
    fn derive_output(&self, _children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        let cols = typed_exprs_to_column_ids(&self.group_by);
        if cols.is_empty() {
            PhysicalPropertySet::gather()
        } else {
            PhysicalPropertySet {
                distribution: DistributionSpec::HashPartitioned(cols),
                ordering: OrderingSpec::Any,
            }
        }
    }
}

impl DeriveRequired for PhysicalHashAggregateOp {
    fn derive_required(
        &self,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        match self.mode {
            AggMode::Single => {
                if self.group_by.is_empty() {
                    vec![PhysicalPropertySet::gather()]
                } else {
                    vec![PhysicalPropertySet::any()]
                }
            }
            AggMode::Local => vec![PhysicalPropertySet::any()],
            AggMode::Global => {
                let cols = typed_exprs_to_column_ids(&self.group_by);
                if cols.is_empty() {
                    vec![PhysicalPropertySet::gather()]
                } else {
                    vec![PhysicalPropertySet {
                        distribution: DistributionSpec::HashPartitioned(cols),
                        ordering: OrderingSpec::Any,
                    }]
                }
            }
            AggMode::DistinctGlobal => {
                let cols = typed_exprs_to_column_ids(&self.group_by);
                if cols.is_empty() {
                    vec![PhysicalPropertySet::gather()]
                } else {
                    vec![PhysicalPropertySet {
                        distribution: DistributionSpec::HashPartitioned(cols),
                        ordering: OrderingSpec::Any,
                    }]
                }
            }
            AggMode::DistinctLocal => vec![PhysicalPropertySet::any()],
        }
    }
}
