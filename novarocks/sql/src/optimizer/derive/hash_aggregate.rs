// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! HashAggregate output / required derivation. Five modes:
//! Single / Local / Global / DistinctLocal / DistinctGlobal.

use crate::column_id::ColumnId;
use crate::optimizer::operator::{AggMode, PhysicalHashAggregateOp};
use novarocks_type_contract::PartitionHashAlgorithm;

use crate::optimizer::property::{DistributionSpec, HashSource, OrderingSpec, PhysicalPropertySet};
use crate::optimizer::scalar::ScalarArena;

use super::{DeriveOutput, DeriveRequired, scalar_exprs_to_column_ids};

fn grouping_distribution<'a>(
    cols: Vec<ColumnId>,
    types: impl IntoIterator<Item = &'a arrow::datatypes::DataType>,
) -> DistributionSpec {
    let source = match PartitionHashAlgorithm::for_grouping_keys(types) {
        PartitionHashAlgorithm::NativeExchangeV2 => HashSource::MapGrouping,
        _ => HashSource::ShuffleAgg,
    };
    DistributionSpec::hash_partitioned(cols, source)
}

impl PhysicalHashAggregateOp {
    fn routes_raw_groups(&self) -> bool {
        self.aggregates
            .iter()
            .any(|call| call.source.requires_raw_group_input())
    }
}

impl DeriveOutput for PhysicalHashAggregateOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        _children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        match self.mode {
            AggMode::Local | AggMode::DistinctLocal => return PhysicalPropertySet::any(),
            AggMode::Single if !self.routes_raw_groups() => return PhysicalPropertySet::gather(),
            AggMode::Single | AggMode::Global | AggMode::DistinctGlobal => {}
        }

        // For Single / Global the group_by may contain non-ColumnRef
        // expressions (e.g. `GROUP BY mod(k, 2)`); reading the column id
        // from `output_layout` instead of the typed group_by exprs is the
        // only way to recover the planner-minted ColumnId for those synthetic
        // slots so the emitted distribution matches downstream requirements.
        // Mirrors the G1 fix that previously lived in
        // search.rs::output_properties.
        let cols: Vec<ColumnId> = self
            .output_layout
            .group_key_columns
            .iter()
            .map(|oc| oc.column_id)
            .filter(|id| *id != ColumnId::UNSET)
            .collect();
        if cols.is_empty() {
            PhysicalPropertySet::gather()
        } else {
            PhysicalPropertySet {
                distribution: grouping_distribution(
                    cols,
                    self.output_layout
                        .group_key_columns
                        .iter()
                        .map(|column| column.value_type.data_type()),
                ),
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
            AggMode::Single if !self.routes_raw_groups() => vec![PhysicalPropertySet::gather()],
            AggMode::Local | AggMode::DistinctLocal => vec![PhysicalPropertySet::any()],
            AggMode::Single | AggMode::Global | AggMode::DistinctGlobal => {
                let cols = scalar_exprs_to_column_ids(scalars, &self.group_by).unwrap_or_default();
                if cols.is_empty() {
                    vec![PhysicalPropertySet::gather()]
                } else {
                    vec![PhysicalPropertySet {
                        distribution: grouping_distribution(
                            cols,
                            self.group_by
                                .iter()
                                .map(|id| scalars.value_type(*id).data_type()),
                        ),
                        ordering: OrderingSpec::Any,
                    }]
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::column_id::ColumnId;
    use crate::optimizer::operator::AggregateOutputLayout;
    use crate::optimizer::scalar::ScalarId;

    use crate::planner::optimizer_bridge::scalar::intern_typed;

    fn intern_group_by(scalars: &mut ScalarArena, exprs: Vec<TypedExpr>) -> Vec<ScalarId> {
        exprs
            .iter()
            .map(|expr| {
                intern_typed(scalars, expr, crate::optimizer::test_optimizer_control()).unwrap()
            })
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
            value_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Utf8,
                false,
            ),
        };
        let mut scalars = ScalarArena::new();
        let op = PhysicalHashAggregateOp {
            mode: AggMode::Single,
            group_by: intern_group_by(&mut scalars, vec![col_ref]),
            aggregates: vec![],
            output_layout: AggregateOutputLayout::new(
                vec![OutputColumn {
                    column_id: ColumnId(3),
                    name: "city".into(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        arrow::datatypes::DataType::Utf8,
                        false,
                    ),

                    is_internal: false,
                }],
                vec![],
            ),
            output_columns: vec![OutputColumn {
                column_id: ColumnId(3),
                name: "city".into(),
                value_type: novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Utf8,
                    false,
                ),

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
            value_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Utf8,
                false,
            ),
        };
        let mut scalars = ScalarArena::new();
        let op = PhysicalHashAggregateOp {
            mode: AggMode::Global,
            group_by: intern_group_by(&mut scalars, vec![col_ref]),
            aggregates: vec![],
            output_layout: AggregateOutputLayout::new(
                vec![OutputColumn {
                    column_id: ColumnId(3),
                    name: "city".into(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        arrow::datatypes::DataType::Utf8,
                        false,
                    ),

                    is_internal: false,
                }],
                vec![],
            ),
            output_columns: vec![OutputColumn {
                column_id: ColumnId(3),
                name: "city".into(),
                value_type: novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Utf8,
                    false,
                ),

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
    fn global_grouped_aggregate_distribution_uses_layout_not_visible_outputs() {
        let input_col = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(3),
                qualifier: Some("t".into()),
                column: "city".into(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Utf8,
                false,
            ),
        };
        let mut scalars = ScalarArena::new();
        let op = PhysicalHashAggregateOp {
            mode: AggMode::Global,
            group_by: intern_group_by(&mut scalars, vec![input_col]),
            aggregates: vec![],
            output_layout: AggregateOutputLayout::new(
                vec![OutputColumn {
                    column_id: ColumnId(30),
                    name: "city".into(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        arrow::datatypes::DataType::Utf8,
                        false,
                    ),

                    is_internal: false,
                }],
                vec![],
            ),
            output_columns: vec![],
            is_merge: vec![],
        };
        let props = op.derive_output(&scalars, &[]);
        match &props.distribution {
            DistributionSpec::HashPartitioned { cols, source } => {
                assert_eq!(*source, HashSource::ShuffleAgg);
                assert_eq!(cols.as_slice(), &[ColumnId(30)]);
            }
            other => panic!("expected ShuffleAgg([c30]), got {other:?}"),
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
            value_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            ),
        };
        let col_x = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(5),
                qualifier: None,
                column: "x".into(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            ),
        };
        let mut scalars = ScalarArena::new();
        let op = PhysicalHashAggregateOp {
            mode: AggMode::DistinctGlobal,
            group_by: intern_group_by(&mut scalars, vec![col_g, col_x]),
            aggregates: vec![],
            output_layout: AggregateOutputLayout::new(
                vec![
                    OutputColumn {
                        column_id: ColumnId(4),
                        name: "g".into(),
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            arrow::datatypes::DataType::Int64,
                            false,
                        ),

                        is_internal: false,
                    },
                    OutputColumn {
                        column_id: ColumnId(5),
                        name: "x".into(),
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            arrow::datatypes::DataType::Int64,
                            false,
                        ),

                        is_internal: false,
                    },
                ],
                vec![],
            ),
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
            output_layout: AggregateOutputLayout::new(vec![], vec![]),
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
            output_layout: AggregateOutputLayout::new(vec![], vec![]),
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
            value_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            ),
        };
        let mut scalars = ScalarArena::new();
        for mode in [AggMode::Local, AggMode::DistinctLocal] {
            let op = PhysicalHashAggregateOp {
                mode,
                group_by: intern_group_by(&mut scalars, vec![col_ref.clone()]),
                aggregates: vec![],
                output_layout: AggregateOutputLayout::new(
                    vec![OutputColumn {
                        column_id: ColumnId(7),
                        name: "k".into(),
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            arrow::datatypes::DataType::Int64,
                            false,
                        ),

                        is_internal: false,
                    }],
                    vec![],
                ),
                output_columns: vec![OutputColumn {
                    column_id: ColumnId(7),
                    name: "k".into(),
                    value_type: novarocks_type_contract::FunctionValueType::new(
                        arrow::datatypes::DataType::Int64,
                        false,
                    ),

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
            output_layout: AggregateOutputLayout::new(vec![], vec![]),
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
            output_layout: AggregateOutputLayout::new(vec![], vec![]),
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
