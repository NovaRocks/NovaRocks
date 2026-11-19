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

use super::*;
#[cfg(test)]
use crate::tests::unit_fixture::DefinitionFixture;

const SEQUENCE: AggregateSequenceId = AggregateSequenceId::new(71);

/// Plans with original call requests publish only on an observed caller
/// control; this one never refuses.
struct Unbounded;
impl novarocks_type_contract::PureCompileControl for Unbounded {
    fn checkpoint(
        &self,
        _: novarocks_type_contract::CompilePhase,
        _: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }
}

/// Every fixture fragment that enters a plan carries the original requests of
/// its aggregate calls, so the sequence and grouping laws below are what
/// publication actually checks.
fn with_requests(fragment: Fragment) -> Fragment {
    with_requests_checked(fragment).expect("positive sequence fixture has complete local facts")
}

fn with_requests_checked(fragment: Fragment) -> Result<Fragment, ValidationErrors> {
    let fragment =
        super::original_call_requests::with_original_relational_requests(fragment, &Unbounded);
    let fragment = super::unit_fixture::with_immutable_relation_claims(fragment);
    crate::validate_fragment_definition(&fragment)?;
    Ok(fragment)
}

#[derive(Clone, Copy)]
enum BindingDrift {
    None,
    Function,
    Overload,
    StateFormat,
}

fn properties(distribution: Distribution) -> PhysicalProperties {
    PhysicalProperties {
        distribution,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

fn aggregate_binding(phase: AggregatePhase, drift: BindingDrift) -> AggregateBinding {
    let function_id = match drift {
        BindingDrift::Function => "builtin/test_count_other/v1",
        BindingDrift::None | BindingDrift::Overload | BindingDrift::StateFormat => {
            "builtin/test_count/v1"
        }
    };
    let overload = match drift {
        BindingDrift::Overload => "i64-state-other",
        BindingDrift::None | BindingDrift::Function | BindingDrift::StateFormat => "i64-state",
    };
    let state_format = match drift {
        BindingDrift::StateFormat => "test_count/state-v2",
        BindingDrift::None | BindingDrift::Function | BindingDrift::Overload => {
            "test_count/state-v1"
        }
    };
    AggregateBinding {
        state_interpretation: None,
        state_argument_contract:
            novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
        function: BoundFunction {
            legacy_metadata: Some(crate::LegacyBindingMetadata {
                semantic_parameters: Box::default(),
                volatility: FunctionVolatility::Immutable,
                argument_evaluation: FunctionArgumentEvaluation::Eager,
                failure_behavior: FunctionFailureBehavior::Propagate,
                intrinsic_row_error:
                    novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated,
            }),
            function_id: FunctionId::try_new(function_id).unwrap(),
            overload: FunctionOverloadId::try_new(overload).unwrap(),
            kind: FunctionKind::Aggregate,
            argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
                FunctionArgumentType::Value(ty(DataType::Int64, false)),
            ])),
            result_type: ty(DataType::Int64, false),
        },
        phase,
        logical_argument_count: 1,
        intermediate_type: ty(DataType::Binary, false),
        state_format: AggregateStateFormatId::try_new(state_format).unwrap(),
    }
}

fn add_provider_scan(
    builder: &mut FragmentBuilder,
    field_types: &[ValueType],
    output_properties: PhysicalProperties,
) -> (NodeId, Vec<ValueId>) {
    let binding = connector_binding();
    let scan = builder.reserve_node_id().unwrap();
    let columns = field_types
        .iter()
        .enumerate()
        .map(|(ordinal, _)| ProviderColumnReference {
            column_payload: encoded(
                &binding,
                ConnectorCodecCategory::ReadColumn,
                u8::try_from(ordinal + 31).unwrap(),
            ),
        })
        .collect::<Vec<_>>();
    let values = columns
        .iter()
        .zip(field_types)
        .map(|(column, value_type)| {
            builder
                .add_value(
                    value_type.clone(),
                    ValueOrigin::ProviderField {
                        scan_node: scan,
                        field: column.clone(),
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    let mut relation = metadata_relation(&binding, columns[0].clone());
    let Relation::Metadata(metadata) = &mut relation else {
        unreachable!();
    };
    metadata.schema = columns
        .iter()
        .zip(field_types)
        .map(|(column, value_type)| RelationField {
            column: column.clone(),
            ty: value_type.clone(),
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    metadata.provided_properties = output_properties.clone();
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: scan,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties,
            output: OutputPort {
                node: scan,
                columns: values.clone().into_boxed_slice(),
            },
            kind: NodeKind::Scan {
                occurrence: ProviderReadOccurrenceId::new(scan.get()),
                relation: Box::new(relation),
                read_budget: scan_budget(),
                provider_outputs: columns
                    .into_iter()
                    .zip(values.iter().copied())
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
                residuals: Box::default(),
                derived_values: Box::default(),
            },
        })
        .unwrap();
    (scan, values)
}

fn add_exchange_source(
    builder: &mut FragmentBuilder,
    edge: EdgeId,
    source_values: &[ValueId],
    value_types: &[ValueType],
    output_properties: PhysicalProperties,
) -> (NodeId, Vec<ValueId>) {
    let node = builder.reserve_node_id().unwrap();
    let imports = source_values
        .iter()
        .zip(value_types)
        .map(|(source_value, value_type)| {
            builder
                .add_value(
                    value_type.clone(),
                    ValueOrigin::ExchangeImport {
                        edge,
                        source_value: *source_value,
                    },
                )
                .unwrap()
        })
        .collect::<Vec<_>>();
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties,
            output: OutputPort {
                node,
                columns: imports.clone().into_boxed_slice(),
            },
            kind: NodeKind::ExchangeSource {
                edge,
                imports: source_values
                    .iter()
                    .copied()
                    .zip(imports.iter().copied())
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            },
        })
        .unwrap();
    (node, imports)
}

fn add_aggregate(
    builder: &mut FragmentBuilder,
    input: NodeId,
    group_inputs: &[ValueId],
    argument: ValueId,
    phase: AggregatePhase,
    drift: BindingDrift,
    distinct: bool,
    call_id: AggregateCallId,
    input_properties: PhysicalProperties,
) -> (NodeId, Vec<ValueId>) {
    add_aggregate_with_grouping(
        builder,
        input,
        group_inputs,
        argument,
        phase,
        drift,
        distinct,
        call_id,
        input_properties,
        if phase.produces_final_result() {
            AggregateGrouping::Complete
        } else {
            AggregateGrouping::Partial
        },
    )
}

fn add_aggregate_with_grouping(
    builder: &mut FragmentBuilder,
    input: NodeId,
    group_inputs: &[ValueId],
    argument: ValueId,
    phase: AggregatePhase,
    drift: BindingDrift,
    distinct: bool,
    call_id: AggregateCallId,
    input_properties: PhysicalProperties,
    grouping: AggregateGrouping,
) -> (NodeId, Vec<ValueId>) {
    let node = builder.reserve_node_id().unwrap();
    let group_by = group_inputs
        .iter()
        .map(|value| {
            let expression = builder
                .define(
                    node,
                    builder.value(*value).unwrap().ty.clone(),
                    ExprKind::Value(*value),
                )
                .unwrap();
            let root = builder.root_definition(
                node,
                expression,
                novarocks_type_contract::EvaluationDemand::Value,
                crate::RootScope::Port(0),
            );
            (root, *value)
        })
        .collect::<Vec<_>>();
    let argument_type = if phase.consumes_logical_arguments() {
        ty(DataType::Int64, false)
    } else {
        ty(DataType::Binary, false)
    };
    let argument_expression = builder
        .define(node, argument_type, ExprKind::Value(argument))
        .unwrap();
    let argument_expression = builder.root_definition(
        node,
        argument_expression,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::Port(0),
    );
    let binding = aggregate_binding(phase, drift);
    let output_type = if phase.produces_final_result() {
        binding.function.result_type.clone()
    } else {
        binding.intermediate_type.clone()
    };
    let output_origin = if phase.produces_final_result() {
        ValueOrigin::AggregateResult { call: call_id }
    } else {
        ValueOrigin::AggregateState {
            call: call_id,
            phase,
        }
    };
    let output = builder.add_value(output_type, output_origin).unwrap();
    let output_distribution = match input_properties.distribution {
        Distribution::Singleton => Distribution::Singleton,
        Distribution::Unconstrained
        | Distribution::RoundRobin
        | Distribution::Broadcast
        | Distribution::Hash { .. }
        | Distribution::ConditionalKey { .. }
        | Distribution::BucketShuffle { .. } => Distribution::Unconstrained,
    };
    let mut columns = group_inputs.to_vec();
    columns.push(output);
    builder
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([input_properties]),
            output_properties: properties(output_distribution),
            output: OutputPort {
                node,
                columns: columns.clone().into_boxed_slice(),
            },
            kind: NodeKind::Aggregate {
                group_by: group_by.into_boxed_slice(),
                calls: Box::from([AggregateCall {
                    id: call_id,
                    binding,
                    arguments: Box::from([argument_expression]),
                    distinct,
                    order_by: Box::default(),
                    output,
                }]),
                grouping,
            },
        })
        .unwrap();
    (node, columns)
}

fn add_edge(
    plan: &mut PlanBuilder,
    edge: EdgeId,
    source_fragment: FragmentId,
    source_values: &[ValueId],
    destination_fragment: FragmentId,
    destination_node: NodeId,
    destination_values: &[ValueId],
    distribution: Distribution,
) {
    plan.add_edge(Edge {
        role: crate::OutputRole::Relational,
        delivery: crate::DeliveryRequirement::DemandDriven,
        id: edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            port: crate::FragmentOutputPort::Root,
            fragment: source_fragment,
            projection: source_values.to_vec().into_boxed_slice(),
        },
        destination: EdgeDestination {
            fragment: destination_fragment,
            node: destination_node,
            receive_mapping: source_values
                .iter()
                .copied()
                .zip(destination_values.iter().copied())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        },
        partitioning: EdgePartitioning {
            source: distribution.clone(),
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: distribution,
            destination_multiplicity: RowMultiplicity::SingleCopy,
            routing_only: Box::default(),
        },
    })
    .unwrap();
}

/// How the final phase's grouping relates to the partial's in the fixture.
#[derive(Clone, Copy)]
enum FinalGroups {
    /// The same keys in the same order.
    Same,
    /// The same keys, written the other way round.
    Reordered,
    /// One of the two keys, so the partial's groups are finer than the ones
    /// the final reads and the final rolls them up.
    Coarser,
}

fn finish_two_stage_sequence(drift: BindingDrift, final_groups: FinalGroups) -> String {
    let edge = EdgeId::new(1);
    let source_id = FragmentId::new(1);
    let destination_id = FragmentId::new(2);
    let singleton = properties(Distribution::Singleton);

    let mut source = FragmentBuilder::new(source_id);
    let (scan, inputs) = add_provider_scan(
        &mut source,
        &[
            ty(DataType::Int64, false),
            ty(DataType::Int64, false),
            ty(DataType::Int64, false),
        ],
        singleton.clone(),
    );
    let (partial, partial_output) = add_aggregate(
        &mut source,
        scan,
        &inputs[..2],
        inputs[2],
        AggregatePhase::Partial { sequence: SEQUENCE },
        BindingDrift::None,
        false,
        AggregateCallId::new(1),
        singleton.clone(),
    );
    let source = with_requests(
        source
            .finish_fixture_structure(
                partial,
                FragmentSink::Stream { edge },
                dop(),
                crate::StagePlacementPolicy::Singleton,
            )
            .unwrap(),
    );

    let mut destination = FragmentBuilder::new(destination_id);
    let (exchange, imports) = add_exchange_source(
        &mut destination,
        edge,
        &partial_output,
        &[
            ty(DataType::Int64, false),
            ty(DataType::Int64, false),
            ty(DataType::Binary, false),
        ],
        singleton.clone(),
    );
    let group_inputs = match final_groups {
        FinalGroups::Same => imports[..2].to_vec(),
        FinalGroups::Reordered => vec![imports[1], imports[0]],
        FinalGroups::Coarser => vec![imports[0]],
    };
    let (final_node, _) = add_aggregate(
        &mut destination,
        exchange,
        &group_inputs,
        imports[2],
        AggregatePhase::Final { sequence: SEQUENCE },
        drift,
        false,
        AggregateCallId::new(2),
        singleton,
    );
    let destination = with_requests(
        destination
            .finish_fixture_structure(
                final_node,
                FragmentSink::Noop,
                dop(),
                crate::StagePlacementPolicy::Singleton,
            )
            .unwrap(),
    );

    let mut plan = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    plan.add_fragment(source).unwrap();
    plan.add_fragment(destination).unwrap();
    add_edge(
        &mut plan,
        edge,
        source_id,
        &partial_output,
        destination_id,
        exchange,
        &imports,
        Distribution::Singleton,
    );
    match plan.finish_observed(&Unbounded) {
        Ok(_) => String::new(),
        Err(error) => error.to_string(),
    }
}

#[test]
fn partial_stream_value_remap_reaches_its_exact_final() {
    assert_eq!(
        finish_two_stage_sequence(BindingDrift::None, FinalGroups::Same),
        ""
    );
}

#[test]
fn same_binary_state_type_cannot_hide_aggregate_binding_drift() {
    for drift in [
        BindingDrift::Function,
        BindingDrift::Overload,
        BindingDrift::StateFormat,
    ] {
        assert!(
            finish_two_stage_sequence(drift, FinalGroups::Same)
                .contains("aggregate state paths do not reduce exactly into their matching final")
        );
    }
}

#[test]
fn aggregate_sequence_rejects_an_orphan_partial() {
    let singleton = properties(Distribution::Singleton);
    let mut fragment = FragmentBuilder::new(FragmentId::new(10));
    let (scan, values) = add_provider_scan(
        &mut fragment,
        &[ty(DataType::Int64, false)],
        singleton.clone(),
    );
    let (partial, _) = add_aggregate(
        &mut fragment,
        scan,
        &[],
        values[0],
        AggregatePhase::Partial { sequence: SEQUENCE },
        BindingDrift::None,
        false,
        AggregateCallId::new(10),
        singleton,
    );
    let fragment = with_requests(
        fragment
            .finish_fixture_structure(
                partial,
                FragmentSink::Noop,
                dop(),
                crate::StagePlacementPolicy::Singleton,
            )
            .unwrap(),
    );
    let mut plan = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    plan.add_fragment(fragment).unwrap();
    assert!(
        plan.finish_observed(&Unbounded)
            .unwrap_err()
            .to_string()
            .contains("aggregate sequence must have exactly one final call")
    );
}

#[test]
fn aggregate_sequence_rejects_duplicate_finals() {
    let edge = EdgeId::new(11);
    let source_id = FragmentId::new(11);
    let destination_id = FragmentId::new(12);
    let singleton = properties(Distribution::Singleton);
    let mut source = FragmentBuilder::new(source_id);
    let (scan, inputs) = add_provider_scan(
        &mut source,
        &[ty(DataType::Int64, false)],
        singleton.clone(),
    );
    let (partial, partial_output) = add_aggregate(
        &mut source,
        scan,
        &[],
        inputs[0],
        AggregatePhase::Partial { sequence: SEQUENCE },
        BindingDrift::None,
        false,
        AggregateCallId::new(11),
        singleton.clone(),
    );
    let source = with_requests(
        source
            .finish_fixture_structure(
                partial,
                FragmentSink::Stream { edge },
                dop(),
                crate::StagePlacementPolicy::Singleton,
            )
            .unwrap(),
    );

    let mut destination = FragmentBuilder::new(destination_id);
    let (exchange, imports) = add_exchange_source(
        &mut destination,
        edge,
        &partial_output,
        &[ty(DataType::Binary, false)],
        singleton.clone(),
    );
    let final_node = destination.reserve_node_id().unwrap();
    let state = destination
        .define(
            final_node,
            ty(DataType::Binary, false),
            ExprKind::Value(imports[0]),
        )
        .unwrap();
    let calls = [AggregateCallId::new(12), AggregateCallId::new(13)]
        .into_iter()
        .map(|call_id| {
            let output = destination
                .add_value(
                    ty(DataType::Int64, false),
                    ValueOrigin::AggregateResult { call: call_id },
                )
                .unwrap();
            AggregateCall {
                id: call_id,
                binding: aggregate_binding(
                    AggregatePhase::Final { sequence: SEQUENCE },
                    BindingDrift::None,
                ),
                arguments: Box::from([destination.root_definition(
                    final_node,
                    state,
                    novarocks_type_contract::EvaluationDemand::Value,
                    crate::RootScope::Port(0),
                )]),
                distinct: false,
                order_by: Box::default(),
                output,
            }
        })
        .collect::<Vec<_>>();
    destination
        .insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: final_node,
            inputs: Box::from([exchange]),
            required_inputs: Box::from([singleton.clone()]),
            output_properties: singleton,
            output: OutputPort {
                node: final_node,
                columns: calls
                    .iter()
                    .map(|call| call.output)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            },
            kind: NodeKind::Aggregate {
                group_by: Box::default(),
                calls: calls.into_boxed_slice(),
                grouping: AggregateGrouping::Complete,
            },
        })
        .unwrap();
    let destination = with_requests(
        destination
            .finish_fixture_structure(
                final_node,
                FragmentSink::Noop,
                dop(),
                crate::StagePlacementPolicy::Singleton,
            )
            .unwrap(),
    );

    let mut plan = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    plan.add_fragment(source).unwrap();
    plan.add_fragment(destination).unwrap();
    add_edge(
        &mut plan,
        edge,
        source_id,
        &partial_output,
        destination_id,
        exchange,
        &imports,
        Distribution::Singleton,
    );
    assert!(
        plan.finish_observed(&Unbounded)
            .unwrap_err()
            .to_string()
            .contains("aggregate sequence must have exactly one final call")
    );
}

#[test]
fn aggregate_sequence_lets_a_final_roll_up_finer_partial_groups() {
    // The phase below may group by more than the one above it reads: that is
    // what a DISTINCT chain is, a dedup on the distinct column whose states
    // the rollup above combines once the column has done its work.
    assert_eq!(
        finish_two_stage_sequence(BindingDrift::None, FinalGroups::Coarser),
        ""
    );
}

#[test]
fn aggregate_sequence_reads_the_same_groups_written_in_either_order() {
    // Grouping by (a, b) and by (b, a) is the same grouping, so the order a
    // phase writes its keys in is not drift.
    assert_eq!(
        finish_two_stage_sequence(BindingDrift::None, FinalGroups::Reordered),
        ""
    );
}

#[test]
fn intermediate_state_chain_reaches_its_partial_and_final() {
    let first_edge = EdgeId::new(21);
    let second_edge = EdgeId::new(22);
    let source_id = FragmentId::new(21);
    let middle_id = FragmentId::new(22);
    let destination_id = FragmentId::new(23);
    let singleton = properties(Distribution::Singleton);

    let mut source = FragmentBuilder::new(source_id);
    let (scan, inputs) = add_provider_scan(
        &mut source,
        &[ty(DataType::Int64, false), ty(DataType::Int64, false)],
        singleton.clone(),
    );
    let (partial, partial_output) = add_aggregate(
        &mut source,
        scan,
        &inputs[..1],
        inputs[1],
        AggregatePhase::Partial { sequence: SEQUENCE },
        BindingDrift::None,
        false,
        AggregateCallId::new(21),
        singleton.clone(),
    );
    let source = with_requests(
        source
            .finish_fixture_structure(
                partial,
                FragmentSink::Stream { edge: first_edge },
                dop(),
                crate::StagePlacementPolicy::Singleton,
            )
            .unwrap(),
    );

    let mut middle = FragmentBuilder::new(middle_id);
    let (first_exchange, first_imports) = add_exchange_source(
        &mut middle,
        first_edge,
        &partial_output,
        &[ty(DataType::Int64, false), ty(DataType::Binary, false)],
        singleton.clone(),
    );
    let (intermediate, intermediate_output) = add_aggregate(
        &mut middle,
        first_exchange,
        &first_imports[..1],
        first_imports[1],
        AggregatePhase::Intermediate { sequence: SEQUENCE },
        BindingDrift::None,
        false,
        AggregateCallId::new(22),
        singleton.clone(),
    );
    let middle = with_requests(
        middle
            .finish_fixture_structure(
                intermediate,
                FragmentSink::Stream { edge: second_edge },
                dop(),
                crate::StagePlacementPolicy::Singleton,
            )
            .unwrap(),
    );

    let mut destination = FragmentBuilder::new(destination_id);
    let (second_exchange, second_imports) = add_exchange_source(
        &mut destination,
        second_edge,
        &intermediate_output,
        &[ty(DataType::Int64, false), ty(DataType::Binary, false)],
        singleton.clone(),
    );
    let (final_node, _) = add_aggregate(
        &mut destination,
        second_exchange,
        &second_imports[..1],
        second_imports[1],
        AggregatePhase::Final { sequence: SEQUENCE },
        BindingDrift::None,
        false,
        AggregateCallId::new(23),
        singleton,
    );
    let destination = with_requests(
        destination
            .finish_fixture_structure(
                final_node,
                FragmentSink::Noop,
                dop(),
                crate::StagePlacementPolicy::Singleton,
            )
            .unwrap(),
    );

    let mut plan = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    plan.add_fragment(source).unwrap();
    plan.add_fragment(middle).unwrap();
    plan.add_fragment(destination).unwrap();
    add_edge(
        &mut plan,
        first_edge,
        source_id,
        &partial_output,
        middle_id,
        first_exchange,
        &first_imports,
        Distribution::Singleton,
    );
    add_edge(
        &mut plan,
        second_edge,
        middle_id,
        &intermediate_output,
        destination_id,
        second_exchange,
        &second_imports,
        Distribution::Singleton,
    );
    plan.finish_observed(&Unbounded).unwrap();
}

fn invalid_finalization(
    phase: AggregatePhase,
    group_types: &[ValueType],
    state_type: ValueType,
    distribution: Distribution,
    distinct: bool,
) -> String {
    let input_properties = properties(distribution);
    let mut builder = FragmentBuilder::new(FragmentId::new(31));
    let mut field_types = group_types.to_vec();
    field_types.push(state_type);
    let (scan, values) = add_provider_scan(&mut builder, &field_types, input_properties.clone());
    let group_count = group_types.len();
    let (aggregate, _) = add_aggregate(
        &mut builder,
        scan,
        &values[..group_count],
        values[group_count],
        phase,
        BindingDrift::None,
        distinct,
        AggregateCallId::new(31),
        input_properties,
    );
    builder
        .finish_fixture_definition(
            aggregate,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap_err()
        .to_string()
}

#[test]
fn global_single_and_final_require_singleton_input() {
    assert!(
        invalid_finalization(
            AggregatePhase::Single,
            &[],
            ty(DataType::Int64, false),
            Distribution::RoundRobin,
            false,
        )
        .contains("aggregate finalization lacks complete group co-location")
    );
    assert!(
        invalid_finalization(
            AggregatePhase::Final { sequence: SEQUENCE },
            &[],
            ty(DataType::Binary, false),
            Distribution::RoundRobin,
            false,
        )
        .contains("aggregate finalization lacks complete group co-location")
    );
}

#[test]
fn grouped_final_rejects_round_robin_input() {
    assert!(
        invalid_finalization(
            AggregatePhase::Final { sequence: SEQUENCE },
            &[ty(DataType::Int64, false)],
            ty(DataType::Binary, false),
            Distribution::RoundRobin,
            false,
        )
        .contains("aggregate finalization lacks complete group co-location")
    );
}

#[test]
fn state_consuming_phase_rejects_distinct() {
    assert!(
        invalid_finalization(
            AggregatePhase::Final { sequence: SEQUENCE },
            &[],
            ty(DataType::Binary, false),
            Distribution::Singleton,
            true,
        )
        .contains("state-consuming aggregate phase cannot apply DISTINCT again")
    );
}

#[test]
fn aggregate_call_identity_is_unique_across_a_fragment() {
    let singleton = properties(Distribution::Singleton);
    let mut builder = FragmentBuilder::new(FragmentId::new(32));
    let (scan, inputs) = add_provider_scan(
        &mut builder,
        &[ty(DataType::Int64, false)],
        singleton.clone(),
    );
    let call = AggregateCallId::new(41);
    let (first, first_output) = add_aggregate(
        &mut builder,
        scan,
        &[],
        inputs[0],
        AggregatePhase::Single,
        BindingDrift::None,
        false,
        call,
        singleton.clone(),
    );
    let (second, _) = add_aggregate(
        &mut builder,
        first,
        &[],
        *first_output.last().unwrap(),
        AggregatePhase::Single,
        BindingDrift::None,
        false,
        call,
        singleton,
    );

    let error = builder
        .finish_fixture_definition(
            second,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap_err()
        .to_string();
    assert!(error.contains("aggregate call identity must be unique within its fragment"));
}

#[derive(Clone, Copy, Debug)]
enum GroupedReductionFixture {
    Valid,
    Utf8Key(bool),
    MissingState,
    WrongSequence,
    ReusedState,
    StateRanking,
    Offset,
    RowBudget,
    DuplicateState,
    FloatKey,
    BinaryCarrier,
    LargeIntKey,
    RepeatedPort,
    DroppedGroupingKey,
    RenamedDroppedGroupingKey,
    RenamedRowBudget,
    StagedRowBudget,
    UnionCompleteRowBudget,
    UnionGroupedStates,
    CompleteRowBudget,
    DistinctState,
}
fn grouped_reduction_plan(fixture: GroupedReductionFixture) -> Result<PhysicalPlan, String> {
    let row_budget = matches!(
        fixture,
        GroupedReductionFixture::RowBudget
            | GroupedReductionFixture::RenamedRowBudget
            | GroupedReductionFixture::UnionCompleteRowBudget
            | GroupedReductionFixture::CompleteRowBudget
    );
    let mut b = FragmentBuilder::new(FragmentId::new(811));
    let singleton = properties(Distribution::Singleton);
    let key_type = match fixture {
        GroupedReductionFixture::Utf8Key(nullable) => ty(DataType::Utf8, nullable),
        GroupedReductionFixture::FloatKey => ty(DataType::Float64, false),
        GroupedReductionFixture::BinaryCarrier => ty(DataType::FixedSizeBinary(16), false),
        GroupedReductionFixture::LargeIntKey => ValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            novarocks_type_contract::ValueLogicalType::LargeInt,
        )
        .unwrap(),
        _ => ty(DataType::Int64, false),
    };
    let field_types = if matches!(
        fixture,
        GroupedReductionFixture::DroppedGroupingKey
            | GroupedReductionFixture::RenamedDroppedGroupingKey
    ) {
        vec![
            key_type.clone(),
            ty(DataType::Int64, false),
            ty(DataType::Int64, false),
        ]
    } else {
        vec![key_type.clone(), ty(DataType::Int64, false)]
    };
    let (scan, values) = add_provider_scan(&mut b, &field_types, singleton.clone());
    let grouping_inputs = if matches!(
        fixture,
        GroupedReductionFixture::DroppedGroupingKey
            | GroupedReductionFixture::RenamedDroppedGroupingKey
    ) {
        vec![values[0], values[1]]
    } else {
        vec![values[0]]
    };
    let (partial, values) = add_aggregate_with_grouping(
        &mut b,
        scan,
        &grouping_inputs,
        *values.last().unwrap(),
        AggregatePhase::Partial { sequence: SEQUENCE },
        BindingDrift::None,
        matches!(fixture, GroupedReductionFixture::DistinctState),
        AggregateCallId::new(811),
        singleton.clone(),
        if matches!(
            fixture,
            GroupedReductionFixture::UnionCompleteRowBudget
                | GroupedReductionFixture::CompleteRowBudget
        ) {
            AggregateGrouping::Complete
        } else {
            AggregateGrouping::Partial
        },
    );
    let (partial, values) = if matches!(
        fixture,
        GroupedReductionFixture::UnionCompleteRowBudget
            | GroupedReductionFixture::UnionGroupedStates
    ) {
        let (left, left_values) = (partial, values);
        let (right_scan, right_input) = add_provider_scan(
            &mut b,
            &[key_type.clone(), ty(DataType::Int64, false)],
            singleton.clone(),
        );
        let (right, right_values) = add_aggregate_with_grouping(
            &mut b,
            right_scan,
            &[right_input[0]],
            right_input[1],
            AggregatePhase::Partial { sequence: SEQUENCE },
            BindingDrift::None,
            false,
            AggregateCallId::new(815),
            singleton.clone(),
            AggregateGrouping::Complete,
        );
        let union = b.reserve_node_id().unwrap();
        let key = b
            .add_value(
                key_type.clone(),
                ValueOrigin::NodeOutput {
                    node: union,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        let state = b
            .add_value(
                ty(DataType::Binary, false),
                ValueOrigin::NodeOutput {
                    node: union,
                    output_ordinal: 1,
                },
            )
            .unwrap();
        b.insert_node_unchecked(PhysicalNode {
            expressions: None,
            id: union,
            inputs: Box::from([left, right]),
            required_inputs: Box::from([singleton.clone(), singleton.clone()]),
            output_properties: singleton.clone(),
            output: OutputPort {
                node: union,
                columns: Box::from([key, state]),
            },
            kind: NodeKind::SetOp {
                kind: SetOperationKind::UnionAll,
                input_mappings: Box::from([
                    left_values.into_boxed_slice(),
                    right_values.into_boxed_slice(),
                ]),
            },
        })
        .unwrap();
        (union, vec![key, state])
    } else {
        (partial, values)
    };
    let mut values = values;
    let partial = if matches!(
        fixture,
        GroupedReductionFixture::RepeatedPort
            | GroupedReductionFixture::DroppedGroupingKey
            | GroupedReductionFixture::RenamedDroppedGroupingKey
            | GroupedReductionFixture::RenamedRowBudget
    ) {
        values = vec![values[0], *values.last().unwrap()];
        let projection = b.reserve_node_id().unwrap();
        let key = b
            .define(projection, key_type.clone(), ExprKind::Value(values[0]))
            .unwrap();
        let state = b
            .define(
                projection,
                ty(DataType::Binary, false),
                ExprKind::Value(values[1]),
            )
            .unwrap();
        let key = b.root_definition(
            projection,
            key,
            novarocks_type_contract::EvaluationDemand::Value,
            crate::RootScope::Port(0),
        );
        let state = b.root_definition(
            projection,
            state,
            novarocks_type_contract::EvaluationDemand::Value,
            crate::RootScope::Port(0),
        );
        let output_key = if matches!(
            fixture,
            GroupedReductionFixture::RenamedDroppedGroupingKey
                | GroupedReductionFixture::RenamedRowBudget
        ) {
            b.add_value(
                key_type.clone(),
                ValueOrigin::Expr {
                    node: projection,
                    root: key,
                },
            )
            .unwrap()
        } else {
            values[0]
        };
        b.add_project(
            projection,
            partial,
            Box::from([(key, output_key), (state, values[1])]),
            if matches!(fixture, GroupedReductionFixture::RepeatedPort) {
                Box::from([output_key, values[1], values[1]])
            } else {
                Box::from([output_key, values[1]])
            },
        )
        .unwrap();
        values[0] = output_key;
        projection
    } else {
        partial
    };
    let topn = b.reserve_node_id().unwrap();
    let key_expr = b
        .define(topn, key_type.clone(), ExprKind::Value(values[0]))
        .unwrap();
    // MissingState intentionally has no consuming call, so do not author an
    // unreachable scalar definition that would mask the relation-path fault.
    let state_expr = if row_budget || matches!(fixture, GroupedReductionFixture::MissingState) {
        key_expr
    } else {
        b.define(
            topn,
            ty(DataType::Binary, false),
            ExprKind::Value(values[1]),
        )
        .unwrap()
    };
    let call_id = AggregateCallId::new(812);
    let phase = AggregatePhase::Intermediate {
        sequence: if matches!(fixture, GroupedReductionFixture::WrongSequence) {
            AggregateSequenceId::new(999)
        } else {
            SEQUENCE
        },
    };
    let state = if row_budget || matches!(fixture, GroupedReductionFixture::ReusedState) {
        values[1]
    } else {
        b.add_value(
            ty(DataType::Binary, false),
            ValueOrigin::AggregateState {
                call: call_id,
                phase,
            },
        )
        .unwrap()
    };
    let state_root = (!row_budget && !matches!(fixture, GroupedReductionFixture::MissingState))
        .then(|| {
            b.root_definition(
                topn,
                state_expr,
                novarocks_type_contract::EvaluationDemand::Value,
                crate::RootScope::Port(0),
            )
        });
    let call = AggregateCall {
        id: call_id,
        binding: aggregate_binding(phase, BindingDrift::None),
        arguments: state_root.into_iter().collect(),
        distinct: false,
        order_by: Box::default(),
        output: state,
    };
    let calls = if matches!(fixture, GroupedReductionFixture::MissingState) {
        Box::default()
    } else if matches!(fixture, GroupedReductionFixture::DuplicateState) {
        let mut duplicate = call.clone();
        // Duplicate contribution identity/output is the tested defect; each
        // ordered argument field must still own its actual root occurrence.
        duplicate.arguments = Box::from([b.root_definition(
            topn,
            state_expr,
            novarocks_type_contract::EvaluationDemand::Value,
            crate::RootScope::Port(0),
        )]);
        Box::from([call, duplicate])
    } else {
        Box::from([call])
    };
    let reduction = if row_budget {
        TopNReduction::Rows
    } else {
        TopNReduction::GroupedStates {
            group_by: Box::from([(
                b.root_definition(
                    topn,
                    key_expr,
                    novarocks_type_contract::EvaluationDemand::Value,
                    crate::RootScope::Port(0),
                ),
                values[0],
            )]),
            calls,
            comparator: OrderedComparisonAlgorithm::NativeScalarOrderV1,
        }
    };
    let order = if matches!(fixture, GroupedReductionFixture::StateRanking) {
        state_expr
    } else {
        key_expr
    };
    let order = b.root_definition(
        topn,
        order,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::Port(0),
    );
    let output_state = if row_budget { values[1] } else { state };
    let ordered_value = if matches!(fixture, GroupedReductionFixture::StateRanking) {
        values[1]
    } else {
        values[0]
    };
    b.insert_node_unchecked(PhysicalNode {
        expressions: None,
        id: topn,
        inputs: Box::from([partial]),
        required_inputs: Box::from([singleton.clone()]),
        output_properties: PhysicalProperties {
            ordering: Box::from([OrderingKey {
                value: ordered_value,
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::Last,
            }]),
            ..singleton.clone()
        },
        output: OutputPort {
            node: topn,
            columns: Box::from([values[0], output_state]),
        },
        kind: NodeKind::TopN {
            order_by: Box::from([SortExpr {
                expr: order,
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::Last,
            }]),
            limit: 1,
            offset: u64::from(matches!(fixture, GroupedReductionFixture::Offset)),
            phase: TopNPhase::Partial {
                sequence: TopNSequenceId::new(811),
            },
            reduction,
        },
    })
    .unwrap();
    let mut topn_input = singleton.clone();
    topn_input.ordering = Box::from([OrderingKey {
        value: ordered_value,
        direction: SortDirection::Ascending,
        null_ordering: NullOrdering::Last,
    }]);
    let topn = if matches!(fixture, GroupedReductionFixture::StagedRowBudget) {
        let rows = b.reserve_node_id().unwrap();
        let order = b
            .define(rows, key_type.clone(), ExprKind::Value(values[0]))
            .unwrap();
        let order = b.root_definition(
            rows,
            order,
            novarocks_type_contract::EvaluationDemand::Value,
            crate::RootScope::Port(0),
        );
        b.add_top_n(
            rows,
            topn,
            Box::from([SortExpr {
                expr: order,
                direction: SortDirection::Ascending,
                null_ordering: NullOrdering::Last,
            }]),
            1,
            0,
            TopNPhase::Partial {
                sequence: TopNSequenceId::new(811),
            },
        )
        .unwrap();
        rows
    } else {
        topn
    };
    let (final_agg, outputs) = add_aggregate(
        &mut b,
        topn,
        &[values[0]],
        output_state,
        AggregatePhase::Final { sequence: SEQUENCE },
        BindingDrift::None,
        false,
        AggregateCallId::new(813),
        topn_input,
    );
    let final_topn = b.reserve_node_id().unwrap();
    let order = b
        .define(final_topn, key_type.clone(), ExprKind::Value(outputs[0]))
        .unwrap();
    let order = b.root_definition(
        final_topn,
        order,
        novarocks_type_contract::EvaluationDemand::Value,
        crate::RootScope::Port(0),
    );
    b.add_top_n(
        final_topn,
        final_agg,
        Box::from([SortExpr {
            expr: order,
            direction: SortDirection::Ascending,
            null_ordering: NullOrdering::Last,
        }]),
        1,
        0,
        TopNPhase::Final {
            sequence: TopNSequenceId::new(811),
        },
    )
    .unwrap();
    let fragment = with_requests_checked(
        b.finish_fixture_structure(
            final_topn,
            FragmentSink::Noop,
            dop(),
            crate::StagePlacementPolicy::Singleton,
        )
        .map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    let mut plan = PlanBuilder::new(version(), crate::tests::constant_fixture::policy());
    plan.add_fragment(fragment).unwrap();
    plan.finish_observed(&Unbounded).map_err(|e| e.to_string())
}
#[test]
fn grouped_topn_merges_new_exact_intermediate_states_in_both_sequences() {
    grouped_reduction_plan(GroupedReductionFixture::LargeIntKey).unwrap();
    grouped_reduction_plan(GroupedReductionFixture::CompleteRowBudget).unwrap();
    grouped_reduction_plan(GroupedReductionFixture::UnionGroupedStates).unwrap();
    let plan = grouped_reduction_plan(GroupedReductionFixture::Valid).unwrap();
    let fragment = plan.fragments().values().next().unwrap();
    let grouped = fragment
        .nodes()
        .values()
        .find(|n| {
            matches!(
                n.kind,
                NodeKind::TopN {
                    reduction: TopNReduction::GroupedStates { .. },
                    ..
                }
            )
        })
        .unwrap();
    let (_, calls) = grouped.kind.aggregate_contract().unwrap();
    assert_eq!(calls.len(), 1);
    assert_ne!(
        calls[0].output,
        crate::expression_value(fragment, grouped.id, calls[0].arguments[0]).unwrap()
    );
}
#[test]
fn grouped_topn_rejects_missing_contributions_row_budgets_and_binding_drift() {
    for fixture in [
        GroupedReductionFixture::MissingState,
        GroupedReductionFixture::WrongSequence,
        GroupedReductionFixture::ReusedState,
        GroupedReductionFixture::StateRanking,
        GroupedReductionFixture::Offset,
        GroupedReductionFixture::RowBudget,
        GroupedReductionFixture::DuplicateState,
        GroupedReductionFixture::FloatKey,
        GroupedReductionFixture::BinaryCarrier,
        GroupedReductionFixture::RepeatedPort,
        GroupedReductionFixture::DroppedGroupingKey,
        GroupedReductionFixture::RenamedDroppedGroupingKey,
        GroupedReductionFixture::RenamedRowBudget,
        GroupedReductionFixture::StagedRowBudget,
        GroupedReductionFixture::UnionCompleteRowBudget,
        GroupedReductionFixture::DistinctState,
    ] {
        let error = grouped_reduction_plan(fixture).unwrap_err();
        // Complete original requests must not mask the fixture's own defect.
        assert!(
            !error.contains("invalid original call requests"),
            "{fixture:?}: {error}"
        );
        if matches!(
            fixture,
            GroupedReductionFixture::DroppedGroupingKey
                | GroupedReductionFixture::RenamedDroppedGroupingKey
                | GroupedReductionFixture::RowBudget
                | GroupedReductionFixture::RenamedRowBudget
                | GroupedReductionFixture::StagedRowBudget
                | GroupedReductionFixture::UnionCompleteRowBudget
        ) {
            assert!(error.contains("TopN partial paths"), "{fixture:?}: {error}");
        } else if matches!(fixture, GroupedReductionFixture::DistinctState) {
            assert!(
                error.contains(
                    "aggregate state paths do not reduce exactly into their matching final"
                ),
                "{fixture:?}: {error}"
            );
        }
    }
}

/// Reuse the original complete, authenticated Partial -> Intermediate -> Final
/// sequence. RF tests add only their own exact producer/consumer witness.
pub(super) fn runtime_filter_grouped_utf8_fragment(nullable: bool) -> Fragment {
    let plan = grouped_reduction_plan(GroupedReductionFixture::Utf8Key(nullable)).unwrap();
    plan.fragments().values().next().unwrap().clone()
}

#[test]
fn grouped_state_topn_key_domain_matches_comparison_and_partitioning() {
    let comparator = OrderedComparisonAlgorithm::NativeScalarOrderV1;
    for data_type in [
        DataType::Boolean,
        DataType::Int64,
        DataType::Utf8,
        DataType::Date32,
    ] {
        assert!(grouped_state_topn_key_supported(
            comparator,
            &ty(data_type, true)
        ));
    }
    for data_type in [
        DataType::Float64,
        DataType::Binary,
        DataType::FixedSizeBinary(16),
    ] {
        assert!(!grouped_state_topn_key_supported(
            comparator,
            &ty(data_type, false)
        ));
    }
}
