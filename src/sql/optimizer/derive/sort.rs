//! Sort: top-level ORDER BY (Gather + Required) or analytic precursor
//! (Hash(partition_cols) + Required).

use crate::sql::optimizer::operator::PhysicalSortOp;
use crate::sql::optimizer::property::{
    DistributionSpec, OrderingSpec, PhysicalPropertySet, typed_exprs_to_column_ids,
};

use super::{DeriveOutput, DeriveRequired};

impl DeriveOutput for PhysicalSortOp {
    fn derive_output(&self, _children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        let distribution = if self.analytic_partition_exprs.is_empty() {
            DistributionSpec::Gather
        } else {
            typed_exprs_to_column_ids(&self.analytic_partition_exprs)
                .map(DistributionSpec::shuffle_agg)
                .unwrap_or(DistributionSpec::Gather)
        };
        PhysicalPropertySet {
            distribution,
            ordering: OrderingSpec::from_sort_items(&self.items),
        }
    }
}

impl DeriveRequired for PhysicalSortOp {
    fn derive_required(
        &self,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        if let Some(partition_cols) = typed_exprs_to_column_ids(&self.analytic_partition_exprs)
            && !partition_cols.is_empty()
        {
            vec![PhysicalPropertySet {
                distribution: DistributionSpec::shuffle_agg(partition_cols),
                ordering: OrderingSpec::Any,
            }]
        } else {
            vec![PhysicalPropertySet::gather()]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, SortItem, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::property::HashSource;

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

    #[test]
    fn analytic_sort_requires_shuffle_agg_on_partition_columns() {
        let partition = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(7),
                qualifier: None,
                column: "k".into(),
            },
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
        };
        let op = PhysicalSortOp {
            items: vec![],
            analytic_partition_exprs: vec![partition],
        };

        let reqs = op.derive_required(&PhysicalPropertySet::any(), 1);
        assert_eq!(reqs.len(), 1);
        match &reqs[0].distribution {
            DistributionSpec::HashPartitioned { cols, source } => {
                assert_eq!(*source, HashSource::ShuffleAgg);
                assert_eq!(cols.as_slice(), &[ColumnId(7)]);
            }
            other => panic!("expected ShuffleAgg([c7]), got {other:?}"),
        }
    }
}
