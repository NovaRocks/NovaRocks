//! Sort: top-level ORDER BY (Gather + Required) or analytic precursor
//! (Hash(partition_cols) + Required).

use crate::sql::optimizer::operator::SortOp;
use crate::sql::optimizer::property::{
    DistributionSpec, OrderingSpec, PhysicalPropertySet, typed_exprs_to_column_ids,
};
use crate::sql::optimizer::scalar::ScalarArena;
use crate::sql::optimizer::scalar_bridge::{materialize_exprs, materialize_sort_keys};

use super::{DeriveOutput, DeriveRequired};

impl DeriveOutput for SortOp {
    fn derive_output(
        &self,
        scalars: &ScalarArena,
        _children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        let distribution = if self.analytic_partition_exprs.is_empty() {
            DistributionSpec::Gather
        } else {
            let partition_exprs = materialize_exprs(scalars, &self.analytic_partition_exprs);
            typed_exprs_to_column_ids(&partition_exprs)
                .map(DistributionSpec::shuffle_agg)
                .unwrap_or(DistributionSpec::Gather)
        };
        let items = materialize_sort_keys(scalars, &self.items);
        PhysicalPropertySet {
            distribution,
            ordering: OrderingSpec::from_sort_items(&items),
        }
    }
}

impl DeriveRequired for SortOp {
    fn derive_required(
        &self,
        scalars: &ScalarArena,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        let partition_exprs = materialize_exprs(scalars, &self.analytic_partition_exprs);
        if let Some(partition_cols) = typed_exprs_to_column_ids(&partition_exprs)
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
    use crate::sql::optimizer::scalar::intern_typed;
    use crate::sql::optimizer::scalar_bridge::intern_sort_items;

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
        let mut scalars = ScalarArena::new();
        let op = SortOp {
            items: intern_sort_items(
                &mut scalars,
                &[SortItem {
                    expr: col_ref,
                    asc: true,
                    nulls_first: false,
                }],
            ),
            analytic_partition_exprs: Vec::new(),
            partition_limit: None,
            topn_type: None,
        };
        let props = op.derive_output(&scalars, &[]);
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
        let mut scalars = ScalarArena::new();
        let partition = intern_typed(&mut scalars, &partition);
        let op = SortOp {
            items: vec![],
            analytic_partition_exprs: vec![partition],
            partition_limit: None,
            topn_type: None,
        };

        let reqs = op.derive_required(&scalars, &PhysicalPropertySet::any(), 1);
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
