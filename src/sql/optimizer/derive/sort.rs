//! Sort: top-level ORDER BY (Gather + Required) or analytic precursor
//! (Hash(partition_cols) + Required).

use crate::sql::analysis::TypedExpr;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::PhysicalSortOp;
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
fn typed_exprs_to_column_ids(exprs: &[TypedExpr]) -> Vec<ColumnId> {
    exprs.iter().filter_map(typed_expr_to_column_id).collect()
}

impl DeriveOutput for PhysicalSortOp {
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
        let distribution = if self.analytic_partition_exprs.is_empty() {
            DistributionSpec::Gather
        } else {
            let partition_cols = typed_exprs_to_column_ids(&self.analytic_partition_exprs);
            if partition_cols.len() == self.analytic_partition_exprs.len() {
                DistributionSpec::HashPartitioned(partition_cols)
            } else {
                DistributionSpec::Gather
            }
        };
        PhysicalPropertySet {
            distribution,
            ordering: if sort_keys.is_empty() {
                OrderingSpec::Any
            } else {
                OrderingSpec::Required(sort_keys)
            },
        }
    }
}

impl DeriveRequired for PhysicalSortOp {
    fn derive_required(
        &self,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        let partition_cols = typed_exprs_to_column_ids(&self.analytic_partition_exprs);
        if partition_cols.is_empty() || partition_cols.len() != self.analytic_partition_exprs.len()
        {
            vec![PhysicalPropertySet::gather()]
        } else {
            vec![PhysicalPropertySet {
                distribution: DistributionSpec::HashPartitioned(partition_cols),
                ordering: OrderingSpec::Any,
            }]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::SortItem;
    use crate::sql::column_id::ColumnId;

    #[test]
    fn output_properties_sort_has_gather_and_ordering() {
        let col_ref = crate::sql::analysis::TypedExpr {
            kind: crate::sql::analysis::ExprKind::ColumnRef {
                column_id: ColumnId(1),
                qualifier: None,
                column: "id".into(),
            },
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
        };
        let op = PhysicalSortOp {
            items: vec![SortItem {
                expr: col_ref,
                asc: true,
                nulls_first: false,
            }],
            analytic_partition_exprs: Vec::new(),
        };
        let props = op.derive_output(&[]);
        assert_eq!(props.distribution, DistributionSpec::Gather);
        assert!(matches!(props.ordering, OrderingSpec::Required(_)));
    }
}
