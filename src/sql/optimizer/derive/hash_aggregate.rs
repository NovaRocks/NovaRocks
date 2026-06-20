//! HashAggregate output / required derivation. Five modes:
//! Single / Local / Global / DistinctLocal / DistinctGlobal.

use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{AggMode, PhysicalHashAggregateOp};
use crate::sql::optimizer::property::{DistributionSpec, OrderingSpec, PhysicalPropertySet};
use crate::sql::optimizer::scalar::ScalarArena;

use super::{DeriveOutput, DeriveRequired, scalar_exprs_to_column_ids};

impl DeriveOutput for PhysicalHashAggregateOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        _children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        match self.mode {
            AggMode::Local | AggMode::DistinctLocal => return PhysicalPropertySet::any(),
            AggMode::Single => return PhysicalPropertySet::gather(),
            AggMode::Global | AggMode::DistinctGlobal => {}
        }

        // For Single / Global the group_by may contain non-ColumnRef
        // expressions (e.g. `GROUP BY mod(k, 2)`); reading the column id
        // from `output_columns` instead of the typed group_by exprs is the
        // only way to recover the planner-minted ColumnId for those synthetic
        // slots so the emitted distribution matches downstream requirements.
        // Mirrors the G1 fix that previously lived in
        // search.rs::output_properties.
        let cols: Vec<ColumnId> = self
            .output_columns
            .iter()
            .take(self.group_by.len())
            .map(|oc| oc.column_id)
            .filter(|id| *id != ColumnId::UNSET)
            .collect();
        if cols.is_empty() {
            PhysicalPropertySet::gather()
        } else {
            PhysicalPropertySet {
                distribution: DistributionSpec::shuffle_agg(cols),
                ordering: OrderingSpec::Any,
            }
        }
    }
}

impl DeriveRequired for PhysicalHashAggregateOp {
    fn derive_required(
        &self,
        scalars: &ScalarArena,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        match self.mode {
            AggMode::Single => {
                vec![PhysicalPropertySet::gather()]
            }
            AggMode::Local => vec![PhysicalPropertySet::any()],
            AggMode::Global => {
                let cols = scalar_exprs_to_column_ids(scalars, &self.group_by).unwrap_or_default();
                if cols.is_empty() {
                    vec![PhysicalPropertySet::gather()]
                } else {
                    vec![PhysicalPropertySet {
                        distribution: DistributionSpec::shuffle_agg(cols),
                        ordering: OrderingSpec::Any,
                    }]
                }
            }
            AggMode::DistinctGlobal => {
                let cols = scalar_exprs_to_column_ids(scalars, &self.group_by).unwrap_or_default();
                if cols.is_empty() {
                    vec![PhysicalPropertySet::gather()]
                } else {
                    vec![PhysicalPropertySet {
                        distribution: DistributionSpec::shuffle_agg(cols),
                        ordering: OrderingSpec::Any,
                    }]
                }
            }
            AggMode::DistinctLocal => vec![PhysicalPropertySet::any()],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::property::HashSource;
    use crate::sql::optimizer::scalar::ScalarId;

    use crate::sql::planner::optimizer_bridge::scalar::intern_typed;

    fn intern_group_by(scalars: &mut ScalarArena, exprs: Vec<TypedExpr>) -> Vec<ScalarId> {
        exprs
            .iter()
            .map(|expr| intern_typed(scalars, expr))
            .collect()
    }

    #[test]
    fn single_grouped_aggregate_gathers_input_and_output() {
        let col_ref = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(3),
                qualifier: Some("t".into()),
                column: "city".into(),
            },
            data_type: arrow::datatypes::DataType::Utf8,
            nullable: false,
        };
        let mut scalars = ScalarArena::new();
        let op = PhysicalHashAggregateOp {
            mode: AggMode::Single,
            group_by: intern_group_by(&mut scalars, vec![col_ref]),
            aggregates: vec![],
            output_columns: vec![OutputColumn {
                column_id: ColumnId(3),
                name: "city".into(),
                data_type: arrow::datatypes::DataType::Utf8,
                nullable: false,
                is_internal: false,
            }],
            is_merge: vec![],
        };
        let props = op.derive_output(&scalars, &[]);
        assert_eq!(props.distribution, DistributionSpec::Gather);

        let reqs = op.derive_required(&scalars, &PhysicalPropertySet::any(), 1);
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0].distribution, DistributionSpec::Gather);
    }

    #[test]
    fn global_grouped_aggregate_outputs_shuffle_by_group_key() {
        let col_ref = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(3),
                qualifier: Some("t".into()),
                column: "city".into(),
            },
            data_type: arrow::datatypes::DataType::Utf8,
            nullable: false,
        };
        let mut scalars = ScalarArena::new();
        let op = PhysicalHashAggregateOp {
            mode: AggMode::Global,
            group_by: intern_group_by(&mut scalars, vec![col_ref]),
            aggregates: vec![],
            output_columns: vec![OutputColumn {
                column_id: ColumnId(3),
                name: "city".into(),
                data_type: arrow::datatypes::DataType::Utf8,
                nullable: false,
                is_internal: false,
            }],
            is_merge: vec![],
        };
        let props = op.derive_output(&scalars, &[]);
        match &props.distribution {
            DistributionSpec::HashPartitioned { cols, source } => {
                assert_eq!(*source, HashSource::ShuffleAgg);
                assert_eq!(cols.as_slice(), &[ColumnId(3)]);
            }
            other => panic!("expected ShuffleAgg([c3]), got {other:?}"),
        }
    }

    #[test]
    fn distinct_global_requires_hash_on_group_by() {
        let col_g = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(4),
                qualifier: None,
                column: "g".into(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        };
        let col_x = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(5),
                qualifier: None,
                column: "x".into(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        };
        let mut scalars = ScalarArena::new();
        let op = PhysicalHashAggregateOp {
            mode: AggMode::DistinctGlobal,
            group_by: intern_group_by(&mut scalars, vec![col_g, col_x]),
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        };
        let reqs = op.derive_required(&scalars, &PhysicalPropertySet::any(), 1);
        assert_eq!(reqs.len(), 1);
        match &reqs[0].distribution {
            DistributionSpec::HashPartitioned { cols, source } => {
                assert_eq!(*source, HashSource::ShuffleAgg);
                assert_eq!(cols.as_slice(), &[ColumnId(4), ColumnId(5)]);
            }
            other => panic!("expected ShuffleAgg on both g and x, got {other:?}"),
        }
    }

    #[test]
    fn distinct_local_requires_any() {
        let op = PhysicalHashAggregateOp {
            mode: AggMode::DistinctLocal,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        };
        let scalars = ScalarArena::new();
        let reqs = op.derive_required(&scalars, &PhysicalPropertySet::gather(), 1);
        assert_eq!(reqs.len(), 1);
        assert!(matches!(reqs[0].distribution, DistributionSpec::Any));
    }

    #[test]
    fn local_scalar_aggregate_outputs_any_distribution() {
        let op = PhysicalHashAggregateOp {
            mode: AggMode::Local,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        };
        let scalars = ScalarArena::new();
        let props = op.derive_output(&scalars, &[]);
        assert!(matches!(props.distribution, DistributionSpec::Any));
    }

    #[test]
    fn local_grouped_aggregate_outputs_any_distribution() {
        let col_ref = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(7),
                qualifier: None,
                column: "k".into(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        };
        let mut scalars = ScalarArena::new();
        for mode in [AggMode::Local, AggMode::DistinctLocal] {
            let op = PhysicalHashAggregateOp {
                mode,
                group_by: intern_group_by(&mut scalars, vec![col_ref.clone()]),
                aggregates: vec![],
                output_columns: vec![OutputColumn {
                    column_id: ColumnId(7),
                    name: "k".into(),
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: false,
                    is_internal: false,
                }],
                is_merge: vec![],
            };

            let props = op.derive_output(&scalars, &[]);
            assert!(matches!(props.distribution, DistributionSpec::Any));
        }
    }

    #[test]
    fn global_scalar_aggregate_outputs_and_requires_gather() {
        let op = PhysicalHashAggregateOp {
            mode: AggMode::Global,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        };
        let scalars = ScalarArena::new();
        let out = op.derive_output(&scalars, &[]);
        assert!(matches!(out.distribution, DistributionSpec::Gather));

        let reqs = op.derive_required(&scalars, &PhysicalPropertySet::any(), 1);
        assert_eq!(reqs.len(), 1);
        assert!(matches!(reqs[0].distribution, DistributionSpec::Gather));
    }

    #[test]
    fn single_scalar_aggregate_keeps_gather_output_and_requirement() {
        let op = PhysicalHashAggregateOp {
            mode: AggMode::Single,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        };
        let scalars = ScalarArena::new();
        let out = op.derive_output(&scalars, &[]);
        assert!(matches!(out.distribution, DistributionSpec::Gather));

        let reqs = op.derive_required(&scalars, &PhysicalPropertySet::any(), 1);
        assert_eq!(reqs.len(), 1);
        assert!(matches!(reqs[0].distribution, DistributionSpec::Gather));
    }
}
