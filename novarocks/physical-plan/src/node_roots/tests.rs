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

//! Raw field-purpose projection at its actual owner. These cases do not
//! claim complete relational admission or installed function/provider capability.
use super::*;
use crate::*;
use arrow_schema::DataType;
use novarocks_connector_contract::*;
use novarocks_expression_contract::BoundFunction;
use novarocks_type_contract::{
    AggregateStateArgumentContract, AggregateStateFormatId, FunctionId, FunctionKind,
    FunctionOverloadId, OrderedComparisonAlgorithm, TypedFactSlice,
};
use std::collections::BTreeMap;
fn r(n: u32) -> RootOrdinal {
    RootOrdinal::new(n)
}
fn v(n: u32) -> ValueId {
    ValueId::new(n)
}
fn id() -> NodeId {
    NodeId::new(91)
}
fn ty() -> ValueType {
    ValueType::new(DataType::Boolean, false)
}
fn properties() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn source(kind: NodeKind, inputs: usize, values: BTreeMap<ValueId, ValueDef>) -> Fragment {
    let fragment = FragmentId::new(19);
    Fragment::from(crate::plan::FragmentParts {
        id: fragment,
        root: id(),
        values,
        nodes: BTreeMap::from([(
            id(),
            PhysicalNode {
                id: id(),
                inputs: (0..inputs).map(|n| NodeId::new(n as u32)).collect(),
                required_inputs: vec![properties(); inputs].into(),
                output_properties: properties(),
                output: OutputPort {
                    node: id(),
                    columns: Box::default(),
                },
                kind,
                expressions: None,
            },
        )]),
        sink: FragmentSink::Noop,
        summaries: Box::default(),
        dop_domain: PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        stage_placement: StagePlacementPolicy::Singleton,
        runtime_filters: Box::default(),
        call_requests: FragmentCallRequests::unpublished_empty(fragment),
        calls: FrozenFragmentCalls::unpublished_empty(fragment),
    })
}
type Projection = (RootOrdinal, EvaluationDemand, RootScope, bool);
fn projected(fragment: &Fragment) -> Result<Vec<Projection>, NodeRootError> {
    let mut actual = Vec::new();
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        &Control,
        novarocks_type_contract::CompilePhase::Validate,
    )?;
    visit_node_roots(
        fragment,
        &fragment.nodes()[&id()],
        &mut work,
        |root, _work| {
            actual.push((root.root, root.demand, root.scope, root.constant_only));
            Ok(())
        },
    )?;
    work.finish()?;
    Ok(actual)
}
fn check(kind: NodeKind, inputs: usize, expected: &[(u32, EvaluationDemand, RootScope, bool)]) {
    let source = source(kind, inputs, BTreeMap::new());
    assert_eq!(
        projected(&source).unwrap(),
        expected
            .iter()
            .map(|(root, demand, scope, constant)| (r(*root), *demand, *scope, *constant))
            .collect::<Vec<_>>()
    );
}
fn key(root: u32) -> SortExpr {
    SortExpr {
        expr: r(root),
        direction: SortDirection::Descending,
        null_ordering: NullOrdering::Last,
    }
}
fn function(kind: FunctionKind) -> BoundFunction {
    BoundFunction::from_exact_signature(
        FunctionId::try_new("fixture/node-root/signature").unwrap(),
        FunctionOverloadId::try_new("fixture/node-root/overload").unwrap(),
        kind,
        TypedFactSlice::from_pure_boxed(Box::default()),
        ty(),
    )
}
fn aggregate() -> AggregateCall {
    AggregateCall {
        filter_root: None,
        id: AggregateCallId::new(0),
        binding: AggregateBinding {
            state_interpretation: None,
            state_argument_contract: AggregateStateArgumentContract::ExactSignature,
            function: function(FunctionKind::Aggregate),
            phase: AggregatePhase::Single,
            logical_argument_count: 0,
            intermediate_type: ty(),
            state_format: AggregateStateFormatId::try_new("fixture/node-root/state").unwrap(),
        },
        arguments: Box::new([r(2), r(3)]),
        distinct: false,
        order_by: Box::new([key(4), key(5)]),
        output: v(0),
    }
}
fn unpivot() -> UnpivotSpec {
    // Collection addresses are deliberately not scalar roots. This pure field
    // author authenticates neither their pools nor any runtime capability.
    let address = ConstantReference {
        pool: ConstantPoolId::new(77),
        ordinal: 3,
    };
    UnpivotSpec {
        passthrough: Box::default(),
        value_output: v(0),
        literal_outputs: Box::default(),
        mappings: Box::new([
            UnpivotValueMapping {
                input: v(1),
                constants: Box::default(),
            },
            UnpivotValueMapping {
                input: v(2),
                constants: Box::new([
                    UnpivotConstant::Int32List(address),
                    UnpivotConstant::Scalar(r(9)),
                    UnpivotConstant::Utf8Map(address),
                    UnpivotConstant::Scalar(r(4)),
                ]),
            },
        ]),
        max_output_rows: 64,
        max_output_bytes: 1024,
    }
}
#[test]
fn filter_and_project_fields_preserve_order_and_repeated_root_references() {
    use EvaluationDemand::*;
    use RootScope::*;
    check(
        NodeKind::Filter {
            predicates: Box::new([r(9), r(9), r(2)]),
        },
        1,
        &[
            (9, TruthOnly, Port(0), false),
            (9, TruthOnly, Port(0), false),
            (2, TruthOnly, Port(0), false),
        ],
    );
    check(
        NodeKind::Project {
            expressions: Box::new([(r(8), v(0)), (r(2), v(1))]),
        },
        1,
        &[(8, Value, Port(0), false), (2, Value, Port(0), false)],
    );
}
#[test]
fn aggregate_and_grouped_topn_keep_group_argument_and_order_fields() {
    use EvaluationDemand::*;
    use RootScope::*;
    let expected = [
        (0, Value, Port(0), false),
        (1, Value, Port(0), false),
        (2, Value, Port(0), false),
        (3, Value, Port(0), false),
        (4, Value, Port(0), false),
        (5, Value, Port(0), false),
    ];
    check(
        NodeKind::Aggregate {
            group_by: Box::new([(r(0), v(0)), (r(1), v(1))]),
            calls: Box::new([aggregate()]),
            grouping: AggregateGrouping::Complete,
        },
        1,
        &expected,
    );
    let mut top = vec![(7, Value, Port(0), false), (8, Value, Port(0), false)];
    top.extend(expected);
    check(
        NodeKind::TopN {
            order_by: Box::new([key(7), key(8)]),
            limit: 2,
            offset: 1,
            phase: TopNPhase::Single,
            reduction: TopNReduction::GroupedStates {
                group_by: Box::new([(r(0), v(0)), (r(1), v(1))]),
                calls: Box::new([aggregate()]),
                comparator: OrderedComparisonAlgorithm::NativeScalarOrderV1,
            },
        },
        1,
        &top,
    );
    check(
        NodeKind::TopN {
            order_by: Box::new([key(8)]),
            limit: 2,
            offset: 0,
            phase: TopNPhase::Single,
            reduction: TopNReduction::Rows,
        },
        1,
        &[(8, Value, Port(0), false)],
    );
}
fn hash(condition: HashJoinCondition<Box<[JoinKey]>, RootOrdinal>, kind: JoinKind) -> NodeKind {
    NodeKind::HashJoin {
        kind,
        condition,
        build_side: JoinSide::Right,
        distribution: JoinDistribution::BroadcastBuild,
        null_extended: Box::default(),
    }
}
#[test]
fn ordinary_hash_join_keys_keep_each_side_and_residual_truth_scope() {
    use EvaluationDemand::*;
    use RootScope::*;
    check(
        hash(
            HashJoinCondition::Ordinary {
                keys: Box::new([
                    JoinKey {
                        left: r(9),
                        right: r(3),
                        null_safe: false,
                        match_relation: novarocks_type_contract::JoinKeyMatch::Grouping,
                    },
                    JoinKey {
                        left: r(2),
                        right: r(8),
                        null_safe: true,
                        match_relation: novarocks_type_contract::JoinKeyMatch::Grouping,
                    },
                ]),
                residual: Some(r(5)),
            },
            JoinKind::Inner,
        ),
        2,
        &[
            (9, Value, Port(0), false),
            (3, Value, Port(1), false),
            (2, Value, Port(0), false),
            (8, Value, Port(1), false),
            (5, TruthOnly, AllPorts, false),
        ],
    );
    check(
        hash(
            HashJoinCondition::Ordinary {
                keys: Box::default(),
                residual: None,
            },
            JoinKind::Inner,
        ),
        2,
        &[],
    );
}
#[test]
fn naa_value_keys_scope_keys_and_eligibility_keep_separate_order_and_value_demand() {
    use EvaluationDemand::*;
    use RootScope::*;
    check(
        hash(
            HashJoinCondition::NullAwareEqual {
                value_equal_keys: Box::new([JoinKey {
                    left: r(9),
                    right: r(2),
                    null_safe: false,
                    match_relation: novarocks_type_contract::JoinKeyMatch::Grouping,
                }]),
                scope_keys: Box::new([JoinKey {
                    left: r(7),
                    right: r(3),
                    null_safe: true,
                    match_relation: novarocks_type_contract::JoinKeyMatch::Grouping,
                }]),
                eligibility: Some(r(5)),
            },
            JoinKind::NullAwareLeftAnti,
        ),
        2,
        &[
            (9, Value, Port(0), false),
            (2, Value, Port(1), false),
            (7, Value, Port(0), false),
            (3, Value, Port(1), false),
            (5, Value, AllPorts, false),
        ],
    );
}
#[test]
fn nested_loop_predicate_keeps_original_join_kind_demand_and_optional_absence() {
    use EvaluationDemand::*;
    use RootScope::*;
    for (kind, demand) in [
        (JoinKind::Inner, TruthOnly),
        (JoinKind::LeftOuter, TruthOnly),
        (JoinKind::NullAwareLeftAnti, Value),
        (JoinKind::LeftMark, Value),
    ] {
        check(
            NodeKind::NestLoopJoin {
                kind,
                distribution: NestLoopJoinDistribution::Singleton,
                predicate: Some(r(3)),
                null_extended: Box::default(),
            },
            2,
            &[(3, demand, AllPorts, false)],
        );
    }
    check(
        NodeKind::NestLoopJoin {
            kind: JoinKind::Inner,
            distribution: NestLoopJoinDistribution::Singleton,
            predicate: None,
            null_extended: Box::default(),
        },
        2,
        &[],
    );
}
#[test]
fn sort_order_and_both_partition_modes_keep_original_port_and_field_order() {
    use EvaluationDemand::*;
    use RootScope::*;
    check(
        NodeKind::Sort {
            order_by: Box::new([key(9), key(3)]),
            mode: SortMode::Global,
        },
        1,
        &[(9, Value, Port(0), false), (3, Value, Port(0), false)],
    );
    for mode in [
        SortMode::Analytic {
            partition_by: Box::new([key(2), key(7)]),
        },
        SortMode::PartitionTopN {
            partition_by: Box::new([key(2), key(7)]),
            limit: 5,
            kind: PartitionTopNType::Rank,
        },
    ] {
        check(
            NodeKind::Sort {
                order_by: Box::new([key(9), key(3)]),
                mode,
            },
            1,
            &[
                (9, Value, Port(0), false),
                (3, Value, Port(0), false),
                (2, Value, Port(0), false),
                (7, Value, Port(0), false),
            ],
        );
    }
}
#[test]
fn window_partitions_order_and_real_call_arguments_keep_scalar_roots_only() {
    use EvaluationDemand::*;
    use RootScope::*;
    check(
        NodeKind::Window(WindowSpec {
            partition_by: Box::new([key(9)]),
            order_by: Box::new([key(3)]),
            calls: Box::new([
                WindowCallSpec {
                    binding: function(FunctionKind::Window),
                    distinct: false,
                    arguments: Box::default(),
                    order_by: Box::default(),
                    frame: None,
                    ignore_nulls: false,
                    aggregate_binding: None,
                    output: v(0),
                },
                WindowCallSpec {
                    binding: function(FunctionKind::Window),
                    distinct: false,
                    arguments: Box::new([r(2), r(7)]),
                    order_by: Box::new([key(5), key(8)]),
                    frame: None,
                    ignore_nulls: false,
                    aggregate_binding: None,
                    output: v(1),
                },
            ]),
        }),
        1,
        &[
            (9, Value, Port(0), false),
            (3, Value, Port(0), false),
            (2, Value, Port(0), false),
            (7, Value, Port(0), false),
            (5, Value, Port(0), false),
            (8, Value, Port(0), false),
        ],
    );
}
#[test]
fn values_keep_actual_row_and_cell_order_including_empty_outer_rows() {
    use EvaluationDemand::*;
    use RootScope::*;
    check(
        NodeKind::Values {
            rows: Box::new([
                Box::default(),
                Box::new([r(9), r(3)]),
                Box::default(),
                Box::new([r(2)]),
            ]),
        },
        0,
        &[
            (9, Value, AllPorts, false),
            (3, Value, AllPorts, false),
            (2, Value, AllPorts, false),
        ],
    );
}
#[test]
fn series_start_stop_and_optional_step_keep_order_without_input_ports() {
    use EvaluationDemand::*;
    use RootScope::*;
    for step in [None, Some(r(7))] {
        let mut expected = vec![(9, Value, AllPorts, false), (3, Value, AllPorts, false)];
        if step.is_some() {
            expected.push((7, Value, AllPorts, false));
        }
        check(
            NodeKind::GenerateSeries {
                start: r(9),
                stop: r(3),
                step,
            },
            0,
            &expected,
        );
    }
}
#[test]
fn table_function_arguments_keep_zero_port_and_one_port_scopes() {
    use EvaluationDemand::*;
    use RootScope::*;
    for inputs in [0, 1] {
        let function = BoundTableFunction::from_exact_signature(
            FunctionId::try_new("fixture/node-root/table").unwrap(),
            FunctionOverloadId::try_new("fixture/node-root/table/zero").unwrap(),
            TypedFactSlice::from_pure_boxed(Box::default()),
            TypedFactSlice::from_pure_boxed(Box::default()),
        );
        let scope = if inputs == 0 { AllPorts } else { Port(0) };
        check(
            NodeKind::TableFunction {
                function,
                arguments: Box::new([r(9), r(3)]),
                outputs: Box::default(),
                left_outer: false,
            },
            inputs,
            &[(9, Value, scope, false), (3, Value, scope, false)],
        );
    }
}
#[test]
fn unpivot_skips_collection_addresses_and_keeps_scalar_occurrences_in_actual_mapping_order() {
    use EvaluationDemand::*;
    use RootScope::*;
    check(
        NodeKind::Unpivot { spec: unpivot() },
        1,
        &[(9, Value, Port(0), true), (4, Value, Port(0), true)],
    );
}
#[test]
fn change_events_keep_optional_predicates_and_assignment_holes_in_actual_order() {
    use EvaluationDemand::*;
    use RootScope::*;
    check(
        NodeKind::ChangeEventExpand {
            effect_output: v(9),
            events: Box::new([
                ChangeEventSpec {
                    predicate: None,
                    effect: ConnectorRowMutationEffect::Insert,
                    assignments: Box::new([
                        ChangeAssignment::exact(v(0), None),
                        ChangeAssignment::exact(v(1), Some(r(9))),
                    ]),
                },
                ChangeEventSpec {
                    predicate: Some(r(3)),
                    effect: ConnectorRowMutationEffect::Delete,
                    assignments: Box::new([
                        ChangeAssignment::exact(v(2), Some(r(7))),
                        ChangeAssignment::exact(v(3), None),
                    ]),
                },
            ]),
        },
        1,
        &[
            (9, Value, Port(0), false),
            (3, TruthOnly, Port(0), false),
            (7, Value, Port(0), false),
        ],
    );
}
#[test]
fn finish_unpivot_scalars_use_the_actual_writer_results_port() {
    use EvaluationDemand::*;
    use RootScope::*;
    let unpivot = unpivot();
    let mappings = unpivot
        .mappings
        .into_vec()
        .into_iter()
        .map(|m| WriterGroupedUnpivotMapping {
            write_target_ordinal: WriteTargetOrdinal::try_new(0).unwrap(),
            input: m.input,
            constants: m.constants,
        })
        .collect();
    check(
        NodeKind::TableFinish(WriterFinishSpec {
            count_mode: FinishCountMode::WriterReported,
            inputs: FinishInputs {
                writer_results: 3,
                summaries: Default::default(),
            },
            summaries: Default::default(),
            expected_target_ordinals: Box::default(),
            input_schema: WriterRelationSchema {
                revision: 1,
                fields: Box::default(),
            },
            output_schema: WriterRelationSchema {
                revision: 1,
                fields: Box::default(),
            },
            final_aggregates: Box::default(),
            grouped_unpivot: Some(WriterGroupedUnpivotSpec {
                statistics_target_ordinals: Box::default(),
                grouping_input: v(0),
                grouping_output: v(1),
                passthrough_output: v(2),
                value_output: v(3),
                literal_outputs: Box::default(),
                mappings,
                max_output_rows: 64,
                max_output_bytes: 1024,
            }),
        }),
        4,
        &[(9, Value, Port(3), true), (4, Value, Port(3), true)],
    );
}
fn scan_relation() -> (Relation, ProviderColumnReference) {
    let instance = ConnectorInstanceId::parse("node-roots").unwrap();
    let binding = ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: instance.clone(),
        },
        CatalogHandle::new(instance, CatalogVersion::from_bytes([3; 32])),
    );
    let payload = |category, byte| {
        ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                binding.descriptor().provider_id.clone(),
                binding.catalog_handle().clone(),
                category,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            vec![byte].into(),
        )
    };
    let column = ProviderColumnReference {
        column_payload: payload(ConnectorCodecCategory::ReadColumn, 33),
    };
    let relation = Relation::Data(DataRelation {
        read: ProviderReadReference {
            binding: binding.clone(),
            input_version: ExactInputVersion::try_new(vec![9]).unwrap(),
            relation: ConnectorReadRelationPayload::new(
                ConnectorReadRelationKind::Table,
                payload(ConnectorCodecCategory::ReadTable, 1),
                payload(ConnectorCodecCategory::ReadView, 2),
            ),
        },
        selection_digest: [8; 32],
        schema: Box::new([RelationField {
            column: column.clone(),
            ty: ty(),
        }]),
        predicate_guarantees: Box::new([PredicateGuarantee {
            predicate: r(77),
            kind: PredicateGuaranteeKind::Exact,
        }]),
        provided_properties: properties(),
    });
    (relation, column)
}
fn scan(origin: Option<ValueOrigin>) -> Fragment {
    let (relation, column) = scan_relation();
    let values = origin
        .map(|origin| {
            (
                v(5),
                ValueDef {
                    id: v(5),
                    ty: ty(),
                    origin,
                },
            )
        })
        .into_iter()
        .collect();
    source(
        NodeKind::Scan {
            occurrence: ProviderReadOccurrenceId::new(1),
            relation: Box::new(relation),
            provider_outputs: Box::new([(column, v(0))]),
            residuals: Box::new([r(9)]),
            derived_values: Box::new([v(5)]),
        },
        0,
        values,
    )
}
#[test]
fn scan_derived_fields_use_the_actual_value_origin_and_do_not_enumerate_provider_guarantees() {
    let source = scan(Some(ValueOrigin::Expr {
        node: id(),
        root: r(3),
    }));
    assert_eq!(
        projected(&source).unwrap(),
        vec![
            (r(9), EvaluationDemand::TruthOnly, RootScope::Port(0), false),
            (r(3), EvaluationDemand::Value, RootScope::Port(0), false)
        ]
    );
    for origin in [
        None,
        Some(ValueOrigin::NodeOutput {
            node: id(),
            output_ordinal: 0,
        }),
        Some(ValueOrigin::Expr {
            node: NodeId::new(92),
            root: r(3),
        }),
    ] {
        assert!(
            matches!(projected(&scan(origin)),Err(NodeRootError::InvalidScanDerived(value)) if value==v(5))
        );
    }
}

struct Control;
impl novarocks_type_contract::PureCompileControl for Control {
    fn checkpoint(
        &self,
        _: novarocks_type_contract::CompilePhase,
        _: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }
}
#[test]
fn actual_scan_unit_root_is_rechecked_after_the_value_origin_changes() {
    use novarocks_expression_contract::{BoundExprSetBuilder, ExprKind, ExprPort, UnitLimits};
    use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
    let mut parts = scan(Some(ValueOrigin::Expr {
        node: id(),
        root: r(1),
    }))
    .into_parts();
    let node = parts.nodes.get_mut(&id()).unwrap();
    let NodeKind::Scan {
        relation,
        provider_outputs,
        residuals,
        ..
    } = &mut node.kind
    else {
        unreachable!()
    };
    let Relation::Data(relation) = relation.as_mut() else {
        unreachable!()
    };
    relation.predicate_guarantees = Box::default();
    *residuals = Box::new([r(0)]);
    parts.values.insert(
        v(0),
        ValueDef {
            id: v(0),
            ty: ty(),
            origin: ValueOrigin::ProviderField {
                scan_node: id(),
                field: provider_outputs[0].0.clone(),
            },
        },
    );
    node.output.columns = Box::new([v(0), v(5)]);
    let mut builder = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: PlanLimits::FROZEN.expression_semantic_depth,
    });
    let definition = builder.add(None, ty(), ExprKind::Value(v(0))).unwrap();
    assert_eq!(
        builder
            .root(definition, EvaluationDemand::TruthOnly, RootScope::Port(0))
            .unwrap(),
        r(0)
    );
    assert_eq!(
        builder
            .root(definition, EvaluationDemand::Value, RootScope::Port(0))
            .unwrap(),
        r(1)
    );
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
    let unit = builder
        .seal(
            vec![ExprPort {
                values: Box::new([(v(0), ty())]),
            }],
            &mut work,
        )
        .unwrap()
        .finish(BTreeMap::new(), &mut work)
        .unwrap();
    work.finish().unwrap();
    node.expressions = Some(std::sync::Arc::new(unit));
    let fragment = Fragment::from(parts);
    crate::validation::validate_fragment_definition(&fragment).unwrap();
    // Raw field extraction preserves repeated references. Actual node/unit
    // admission separately refuses two fields reusing one ordinal.
    let mut duplicate = fragment.clone().into_parts();
    let NodeKind::Scan { residuals, .. } = &mut duplicate.nodes.get_mut(&id()).unwrap().kind else {
        unreachable!()
    };
    *residuals = Box::new([r(0), r(0)]);
    let error =
        crate::validation::validate_fragment_definition(&Fragment::from(duplicate)).unwrap_err();
    assert!(error.errors().iter().any(|error| error.category()
        == ValidationErrorCategory::StructuralInvariant
        && error.message()
            == "two node fields share one root ordinal; preserve each ordered occurrence"));
    let mut parts = fragment.into_parts();
    parts.values.get_mut(&v(5)).unwrap().origin = ValueOrigin::Expr {
        node: id(),
        root: r(99),
    };
    let error =
        crate::validation::validate_fragment_definition(&Fragment::from(parts)).unwrap_err();
    assert!(error.errors().iter().any(|error| error.category()
        == ValidationErrorCategory::StructuralInvariant
        && error.message() == "node references an absent unit root"));
}
