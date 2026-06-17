//! Window operator. Output preserves child ordering. Global window signatures
//! (empty PARTITION BY) require Gather. Partitioned signatures may use the
//! common subset of column-ref PARTITION BY keys shared by all window exprs;
//! disjoint keys or non-column partition expressions fall back to Gather.
//! Required properties include both partition distribution and the physical
//! ordering required by the first window signature.

use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::WindowOp;
use crate::sql::optimizer::property::{
    DistributionSpec, OrderingSpec, PhysicalPropertySet, typed_expr_to_column_id,
    window_ordering_spec,
};
use crate::sql::optimizer::scalar::ScalarArena;
use crate::sql::optimizer::scalar_bridge::materialize_window_exprs;
use crate::sql::planner::plan::WindowExpr;

use super::{DeriveOutput, DeriveRequired};

impl DeriveOutput for WindowOp {
    fn derive_output(
        &self,
        scalars: &ScalarArena,
        children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        let ordering = children
            .first()
            .map(|props| props.ordering.clone())
            .unwrap_or(OrderingSpec::Any);
        let window_exprs =
            materialize_window_exprs(scalars, &self.window_exprs, &self.output_columns);
        let distribution = common_window_partition_distribution(&window_exprs);
        PhysicalPropertySet {
            distribution,
            ordering,
        }
    }
}

impl DeriveRequired for WindowOp {
    fn derive_required(
        &self,
        scalars: &ScalarArena,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        let window_exprs =
            materialize_window_exprs(scalars, &self.window_exprs, &self.output_columns);
        let ordering = window_exprs
            .first()
            .map(|win| window_ordering_spec(&win.partition_by, &win.order_by))
            .unwrap_or(OrderingSpec::Any);
        let distribution = common_window_partition_distribution(&window_exprs);
        vec![PhysicalPropertySet {
            distribution,
            ordering,
        }]
    }
}

fn common_window_partition_distribution(window_exprs: &[WindowExpr]) -> DistributionSpec {
    let Some(cols) = common_window_partition_cols(window_exprs) else {
        return DistributionSpec::Gather;
    };
    DistributionSpec::shuffle_agg(cols)
}

fn common_window_partition_cols(window_exprs: &[WindowExpr]) -> Option<Vec<ColumnId>> {
    let mut common: Option<Vec<_>> = None;
    for win in window_exprs {
        if win.partition_by.is_empty() {
            return None;
        }
        let mut current = Vec::new();
        for expr in &win.partition_by {
            let column = typed_expr_to_column_id(expr)?;
            if !current.contains(&column) {
                current.push(column);
            }
        }
        if current.is_empty() {
            return None;
        }
        match &mut common {
            None => common = Some(current),
            Some(common_cols) => {
                common_cols.retain(|column| current.contains(column));
                if common_cols.is_empty() {
                    return None;
                }
            }
        }
    }
    common
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn, SortItem, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::property::{HashSource, OrderingSpec};
    use crate::sql::optimizer::scalar_bridge::intern_window_exprs;
    use crate::sql::planner::plan::WindowExpr;

    fn test_col(column_id: ColumnId, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id,
                qualifier: None,
                column: name.into(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        }
    }

    fn test_window(output_name: &str, partition_by: Vec<TypedExpr>) -> WindowExpr {
        WindowExpr {
            name: "cume_dist".into(),
            args: vec![],
            partition_by,
            order_by: vec![],
            window_frame: None,
            ignore_nulls: false,
            distinct: false,
            output_name: output_name.into(),
            output_column_id: ColumnId::UNSET,
            result_type: arrow::datatypes::DataType::Float64,
        }
    }

    fn window_op(scalars: &mut ScalarArena, exprs: Vec<WindowExpr>) -> WindowOp {
        let output_columns = exprs
            .iter()
            .enumerate()
            .map(|(idx, expr)| OutputColumn {
                column_id: ColumnId(1000 + idx as u32),
                name: expr.output_name.clone(),
                data_type: expr.result_type.clone(),
                nullable: true,
                is_internal: false,
            })
            .collect::<Vec<_>>();
        let window_exprs = intern_window_exprs(scalars, &exprs);
        WindowOp {
            window_exprs,
            output_columns,
        }
    }

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
            output_column_id: ColumnId::UNSET,
            result_type: arrow::datatypes::DataType::Int64,
        };
        let mut scalars = ScalarArena::new();
        let op = window_op(&mut scalars, vec![window_expr]);
        let props = op.derive_output(&scalars, &[]);
        match &props.distribution {
            DistributionSpec::HashPartitioned { cols, source } => {
                assert_eq!(*source, HashSource::ShuffleAgg);
                assert_eq!(cols.as_slice(), &[ColumnId(2)]);
            }
            other => panic!("expected ShuffleAgg([c0]), got {other:?}"),
        }
    }

    #[test]
    fn output_properties_window_without_partition_by_is_gather() {
        let window_expr = WindowExpr {
            name: "row_number".into(),
            args: vec![],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
            ignore_nulls: false,
            distinct: false,
            output_name: "win".into(),
            output_column_id: ColumnId::UNSET,
            result_type: arrow::datatypes::DataType::Int64,
        };
        let mut scalars = ScalarArena::new();
        let op = window_op(&mut scalars, vec![window_expr]);
        let props = op.derive_output(&scalars, &[]);
        assert_eq!(props.distribution, DistributionSpec::Gather);
    }

    #[test]
    fn required_properties_disjoint_partition_windows_gather() {
        let mut scalars = ScalarArena::new();
        let op = window_op(
            &mut scalars,
            vec![
                test_window("by_a", vec![test_col(ColumnId(1), "a")]),
                test_window("by_b", vec![test_col(ColumnId(2), "b")]),
            ],
        );

        let reqs = op.derive_required(&scalars, &PhysicalPropertySet::any(), 1);

        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0].distribution, DistributionSpec::Gather);
    }

    #[test]
    fn output_properties_disjoint_partition_windows_gather() {
        let mut scalars = ScalarArena::new();
        let op = window_op(
            &mut scalars,
            vec![
                test_window("by_a", vec![test_col(ColumnId(1), "a")]),
                test_window("by_b", vec![test_col(ColumnId(2), "b")]),
            ],
        );

        let props = op.derive_output(&scalars, &[]);

        assert_eq!(props.distribution, DistributionSpec::Gather);
    }

    #[test]
    fn required_properties_nested_partition_windows_use_common_subset() {
        let mut scalars = ScalarArena::new();
        let op = window_op(
            &mut scalars,
            vec![
                test_window("by_a", vec![test_col(ColumnId(1), "a")]),
                test_window(
                    "by_a_b",
                    vec![test_col(ColumnId(1), "a"), test_col(ColumnId(2), "b")],
                ),
            ],
        );

        let reqs = op.derive_required(&scalars, &PhysicalPropertySet::any(), 1);

        assert_eq!(reqs.len(), 1);
        match &reqs[0].distribution {
            DistributionSpec::HashPartitioned { cols, source } => {
                assert_eq!(*source, HashSource::ShuffleAgg);
                assert_eq!(cols.as_slice(), &[ColumnId(1)]);
            }
            other => panic!("expected ShuffleAgg([a]), got {other:?}"),
        }
    }

    #[test]
    fn required_properties_mixed_partition_and_global_window_gathers() {
        let partition = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(2),
                qualifier: None,
                column: "k".into(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        };
        let partitioned = WindowExpr {
            name: "cume_dist".into(),
            args: vec![],
            partition_by: vec![partition],
            order_by: vec![],
            window_frame: None,
            ignore_nulls: false,
            distinct: false,
            output_name: "partitioned".into(),
            output_column_id: ColumnId::UNSET,
            result_type: arrow::datatypes::DataType::Float64,
        };
        let global = WindowExpr {
            name: "percent_rank".into(),
            args: vec![],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
            ignore_nulls: false,
            distinct: false,
            output_name: "global".into(),
            output_column_id: ColumnId::UNSET,
            result_type: arrow::datatypes::DataType::Float64,
        };
        let mut scalars = ScalarArena::new();
        let op = window_op(&mut scalars, vec![partitioned, global]);

        let reqs = op.derive_required(&scalars, &PhysicalPropertySet::any(), 1);

        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0].distribution, DistributionSpec::Gather);
    }

    #[test]
    fn output_properties_mixed_partition_and_global_window_gathers() {
        let partition = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(2),
                qualifier: None,
                column: "k".into(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        };
        let partitioned = WindowExpr {
            name: "cume_dist".into(),
            args: vec![],
            partition_by: vec![partition],
            order_by: vec![],
            window_frame: None,
            ignore_nulls: false,
            distinct: false,
            output_name: "partitioned".into(),
            output_column_id: ColumnId::UNSET,
            result_type: arrow::datatypes::DataType::Float64,
        };
        let global = WindowExpr {
            name: "percent_rank".into(),
            args: vec![],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
            ignore_nulls: false,
            distinct: false,
            output_name: "global".into(),
            output_column_id: ColumnId::UNSET,
            result_type: arrow::datatypes::DataType::Float64,
        };
        let mut scalars = ScalarArena::new();
        let op = window_op(&mut scalars, vec![partitioned, global]);

        let props = op.derive_output(&scalars, &[]);

        assert_eq!(props.distribution, DistributionSpec::Gather);
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
            output_column_id: ColumnId::UNSET,
            result_type: arrow::datatypes::DataType::Int64,
        };
        let mut scalars = ScalarArena::new();
        let op = window_op(&mut scalars, vec![window_expr]);

        let reqs = op.derive_required(&scalars, &PhysicalPropertySet::any(), 1);

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
