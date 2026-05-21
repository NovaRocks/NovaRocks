//! Window operator. Output Hash(partition_cols) iff all PARTITION BY entries
//! resolve to column-refs; else Any. Required Hash(partition_cols) or Gather.

use crate::sql::analysis::TypedExpr;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::PhysicalWindowOp;
use crate::sql::optimizer::property::{DistributionSpec, OrderingSpec, PhysicalPropertySet};

use super::{DeriveOutput, DeriveRequired};

fn typed_expr_to_column_id(expr: &TypedExpr) -> Option<ColumnId> {
    match &expr.kind {
        crate::sql::analysis::ExprKind::ColumnRef { column_id, .. } => Some(*column_id),
        _ => None,
    }
}

impl DeriveOutput for PhysicalWindowOp {
    fn derive_output(&self, _children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        let mut partition_cols = Vec::new();
        let mut all_columns = true;
        for we in &self.window_exprs {
            for pbe in &we.partition_by {
                if let Some(col) = typed_expr_to_column_id(pbe) {
                    if !partition_cols.contains(&col) {
                        partition_cols.push(col);
                    }
                } else {
                    all_columns = false;
                }
            }
        }
        if !all_columns || partition_cols.is_empty() {
            PhysicalPropertySet::any()
        } else {
            PhysicalPropertySet {
                distribution: DistributionSpec::HashPartitioned(partition_cols),
                ordering: OrderingSpec::Any,
            }
        }
    }
}

impl DeriveRequired for PhysicalWindowOp {
    fn derive_required(
        &self,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        let mut partition_cols = Vec::new();
        for we in &self.window_exprs {
            for pbe in &we.partition_by {
                if let Some(col) = typed_expr_to_column_id(pbe)
                    && !partition_cols.contains(&col)
                {
                    partition_cols.push(col);
                }
            }
        }
        if partition_cols.is_empty() {
            vec![PhysicalPropertySet::gather()]
        } else {
            vec![PhysicalPropertySet {
                distribution: DistributionSpec::HashPartitioned(partition_cols),
                ordering: OrderingSpec::Any,
            }]
        }
    }
}
