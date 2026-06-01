//! Window operator. Output preserves child ordering and carries
//! Hash(partition_cols) iff all PARTITION BY entries resolve to column refs.
//! Required properties include both partition distribution and the physical
//! ordering required by the first window signature.

use crate::sql::optimizer::operator::PhysicalWindowOp;
use crate::sql::optimizer::property::{
    DistributionSpec, OrderingSpec, PhysicalPropertySet, typed_expr_to_column_id,
    window_ordering_spec,
};

use super::{DeriveOutput, DeriveRequired};

impl DeriveOutput for PhysicalWindowOp {
    fn derive_output(&self, children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        let ordering = children
            .first()
            .map(|props| props.ordering.clone())
            .unwrap_or(OrderingSpec::Any);
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
            PhysicalPropertySet {
                distribution: DistributionSpec::Any,
                ordering,
            }
        } else {
            PhysicalPropertySet {
                distribution: DistributionSpec::shuffle_agg(partition_cols),
                ordering,
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
        let mut all_partition_columns = true;
        for we in &self.window_exprs {
            for pbe in &we.partition_by {
                match typed_expr_to_column_id(pbe) {
                    Some(col) => {
                        if !partition_cols.contains(&col) {
                            partition_cols.push(col);
                        }
                    }
                    None => all_partition_columns = false,
                }
            }
        }
        let ordering = self
            .window_exprs
            .first()
            .map(|win| window_ordering_spec(&win.partition_by, &win.order_by))
            .unwrap_or(OrderingSpec::Any);
        if partition_cols.is_empty() || !all_partition_columns {
            vec![PhysicalPropertySet {
                distribution: DistributionSpec::Gather,
                ordering,
            }]
        } else {
            vec![PhysicalPropertySet {
                distribution: DistributionSpec::shuffle_agg(partition_cols),
                ordering,
            }]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, SortItem, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::property::{HashSource, OrderingSpec};
    use crate::sql::planner::plan::WindowExpr;

    #[test]
    fn output_properties_window_propagates_partition_distribution() {
        let col_c0 = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(2),
                qualifier: None,
                column: "c0".into(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        };
        let window_expr = WindowExpr {
            name: "max".into(),
            args: vec![],
            partition_by: vec![col_c0.clone()],
            order_by: vec![],
            window_frame: None,
            ignore_nulls: false,
            distinct: false,
            output_name: "win".into(),
            result_type: arrow::datatypes::DataType::Int64,
        };
        let op = PhysicalWindowOp {
            window_exprs: vec![window_expr],
            output_columns: vec![],
        };
        let props = op.derive_output(&[]);
        match &props.distribution {
            DistributionSpec::HashPartitioned { cols, source } => {
                assert_eq!(*source, HashSource::ShuffleAgg);
                assert_eq!(cols.as_slice(), &[ColumnId(2)]);
            }
            other => panic!("expected ShuffleAgg([c0]), got {other:?}"),
        }
    }

    #[test]
    fn output_properties_window_without_partition_by_is_any() {
        let window_expr = WindowExpr {
            name: "row_number".into(),
            args: vec![],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
            ignore_nulls: false,
            distinct: false,
            output_name: "win".into(),
            result_type: arrow::datatypes::DataType::Int64,
        };
        let op = PhysicalWindowOp {
            window_exprs: vec![window_expr],
            output_columns: vec![],
        };
        let props = op.derive_output(&[]);
        assert_eq!(props.distribution, DistributionSpec::Any);
    }

    #[test]
    fn required_properties_window_include_partition_and_ordering_keys() {
        let partition = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(2),
                qualifier: None,
                column: "k".into(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        };
        let order = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(3),
                qualifier: None,
                column: "ts".into(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        };
        let window_expr = WindowExpr {
            name: "sum".into(),
            args: vec![],
            partition_by: vec![partition],
            order_by: vec![SortItem {
                expr: order,
                asc: false,
                nulls_first: false,
            }],
            window_frame: None,
            ignore_nulls: false,
            distinct: false,
            output_name: "win".into(),
            result_type: arrow::datatypes::DataType::Int64,
        };
        let op = PhysicalWindowOp {
            window_exprs: vec![window_expr],
            output_columns: vec![],
        };

        let reqs = op.derive_required(&PhysicalPropertySet::any(), 1);

        assert_eq!(reqs.len(), 1);
        match &reqs[0].ordering {
            OrderingSpec::Required(keys) => {
                assert_eq!(keys.len(), 2);
                assert_eq!(keys[0].column, ColumnId(2));
                assert!(keys[0].asc);
                assert!(keys[0].nulls_first);
                assert_eq!(keys[1].column, ColumnId(3));
                assert!(!keys[1].asc);
                assert!(!keys[1].nulls_first);
            }
            other => panic!("expected required window ordering, got {other:?}"),
        }
    }
}
