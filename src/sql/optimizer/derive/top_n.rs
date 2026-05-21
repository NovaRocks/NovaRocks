//! TopN: Partial / Final[split]. See search.rs comments for the rationale.

use crate::sql::analysis::TypedExpr;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{PhysicalTopNOp, TopNPhase};
use crate::sql::optimizer::property::{
    DistributionSpec, OrderingSpec, PhysicalPropertySet, SortKey,
};

use super::{DeriveOutput, DeriveRequired};

fn typed_expr_to_column_id(expr: &TypedExpr) -> Option<ColumnId> {
    match &expr.kind {
        crate::sql::analysis::ExprKind::ColumnRef { column_id, .. } => Some(*column_id),
        _ => None,
    }
}

impl DeriveOutput for PhysicalTopNOp {
    fn derive_output(&self, _children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        let sort_keys: Vec<SortKey> = self
            .items
            .iter()
            .filter_map(|item| {
                typed_expr_to_column_id(&item.expr).map(|col| SortKey {
                    column: col,
                    asc: item.asc,
                    nulls_first: item.nulls_first,
                })
            })
            .collect();
        let ordering = if sort_keys.is_empty() {
            OrderingSpec::Any
        } else {
            OrderingSpec::Required(sort_keys)
        };
        let distribution = match self.phase {
            TopNPhase::Partial => DistributionSpec::Any,
            TopNPhase::Final => DistributionSpec::Gather,
        };
        PhysicalPropertySet {
            distribution,
            ordering,
        }
    }
}

impl DeriveRequired for PhysicalTopNOp {
    fn derive_required(
        &self,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        let req = match (self.phase, self.is_split) {
            (TopNPhase::Partial, _) => PhysicalPropertySet::any(),
            (TopNPhase::Final, true) => PhysicalPropertySet::any(),
            (TopNPhase::Final, false) => PhysicalPropertySet::gather(),
        };
        vec![req]
    }
}
