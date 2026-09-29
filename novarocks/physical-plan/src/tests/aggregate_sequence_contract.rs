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

const SEQUENCE: AggregateSequenceId = AggregateSequenceId::new(71);

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
        function: BoundFunction {
            function_id: FunctionId::try_new(function_id).unwrap(),
            overload: FunctionOverloadId::try_new(overload).unwrap(),
            kind: FunctionKind::Aggregate,
            argument_types: Box::from([FunctionArgumentType::Value(ty(DataType::Int64, false))]),
            result_type: ty(DataType::Int64, false),
            volatility: FunctionVolatility::Immutable,
            argument_evaluation: FunctionArgumentEvaluation::Eager,
            failure_behavior: FunctionFailureBehavior::Propagate,
            intrinsic_row_error:
                novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated,
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
            id: scan,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties,
            output: OutputPort {
                node: scan,
                columns: values.clone().into_boxed_slice(),
            },
            kind: NodeKind::Scan {
                occurrence: ProviderReadOccurrenceId::new(0),
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

#[allow(clippy::too_many_arguments)]
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
    let node = builder.reserve_node_id().unwrap();
    let group_by = group_inputs
        .iter()
        .map(|value| {
            let expression = builder
                .add_expression(node, ty(DataType::Int64, false), ExprKind::Value(*value))
                .unwrap();
            (expression, *value)
        })
        .collect::<Vec<_>>();
    let argument_type = if phase.consumes_logical_arguments() {
        ty(DataType::Int64, false)
    } else {
        ty(DataType::Binary, false)
    };
    let argument_expression = builder
        .add_expression(node, argument_type, ExprKind::Value(argument))
        .unwrap();
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
        | Distribution::BucketShuffle { .. } => Distribution::Unconstrained,
    };
    let mut columns = group_inputs.to_vec();
    columns.push(output);
    builder
        .insert_node_unchecked(PhysicalNode {
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
                grouping: match phase {
                    AggregatePhase::Single | AggregatePhase::Final { .. } => {
                        AggregateGrouping::Complete
                    }
                    AggregatePhase::Partial { .. } | AggregatePhase::Intermediate { .. } => {
                        AggregateGrouping::Partial
                    }
                },
            },
        })
        .unwrap();
    (node, columns)
}

#[allow(clippy::too_many_arguments)]
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
        id: edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
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
    let source = source
        .finish_definition(partial, FragmentSink::Stream { edge }, dop())
        .unwrap();

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
    let destination = destination
        .finish_definition(final_node, FragmentSink::Noop, dop())
        .unwrap();

    let mut plan = PlanBuilder::new(version());
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
    match plan.finish() {
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
    let fragment = fragment
        .finish_definition(partial, FragmentSink::Noop, dop())
        .unwrap();
    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(fragment).unwrap();
    assert!(
        plan.finish()
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
    let source = source
        .finish_definition(partial, FragmentSink::Stream { edge }, dop())
        .unwrap();

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
        .add_expression(
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
                arguments: Box::from([state]),
                distinct: false,
                order_by: Box::default(),
                output,
            }
        })
        .collect::<Vec<_>>();
    destination
        .insert_node_unchecked(PhysicalNode {
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
    let destination = destination
        .finish_definition(final_node, FragmentSink::Noop, dop())
        .unwrap();

    let mut plan = PlanBuilder::new(version());
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
        plan.finish()
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
    let source = source
        .finish_definition(partial, FragmentSink::Stream { edge: first_edge }, dop())
        .unwrap();

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
    let middle = middle
        .finish_definition(
            intermediate,
            FragmentSink::Stream { edge: second_edge },
            dop(),
        )
        .unwrap();

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
    let destination = destination
        .finish_definition(final_node, FragmentSink::Noop, dop())
        .unwrap();

    let mut plan = PlanBuilder::new(version());
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
    plan.finish().unwrap();
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
        .finish_definition(aggregate, FragmentSink::Noop, dop())
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
        .finish_definition(second, FragmentSink::Noop, dop())
        .unwrap_err()
        .to_string();
    assert!(error.contains("aggregate call identity must be unique within its fragment"));
}
