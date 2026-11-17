// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::physical_properties_v2::PhysicalPropertyProjectionLimits;
use crate::{
    physical_connector_payload_v2::{
        ConnectorPayloadProjectionLimits, decode_connector_payloads, encode_connector_payloads,
    },
    physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table_sources},
    physical_value_origin_v2::ValueOriginProjectionLimits,
    physical_value_v2::{ValueProjectionLimits, ValueSource, decode_values, encode_values},
};
use arrow::datatypes::DataType;
use novarocks_proto_models::physical_control_v2::Empty;
use novarocks_type_contract::{
    CompileControlError, FunctionValueType, PartitionCountParameterId, PartitionHashAlgorithm,
    PartitionSpaceId, PureCompileControl,
};
use std::sync::Mutex;

const SOURCE: usize = 1024 * 1024;
const PRIOR: usize = 64 * 1024;
// The Value phase coexists with the prepared payload owners. Its invoice
// includes their retained state in addition to the original backing.
const VALUE_SOURCE: usize = 256 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Mutex<Option<(usize, CompileControlError)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        let stop = self.stop.lock().unwrap().clone();
        if let Some((stop, _)) = stop {
            assert!(at <= stop, "callback after original refusal");
        }
        trace.push((phase, units));
        match stop {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn arm(&self, stop: Option<(usize, CompileControlError)>) {
        self.trace.lock().unwrap().clear();
        *self.stop.lock().unwrap() = stop;
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
fn limits() -> RepeatNodeProjectionLimits {
    RepeatNodeProjectionLimits {
        max_input_nodes: 1024,
        max_value_references: 8192,
        max_list_items: 8192,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 1024 * 1024,
        max_coexisting_source_and_request_bytes: 4 * 1024 * 1024,
        max_work: 64 * 1024 * 1024,
        properties: PhysicalPropertyProjectionLimits {
            max_value_references: 8192,
            max_allocation_requests: 8192,
            max_allocation_request_bytes: 1024 * 1024,
            max_coexisting_source_and_request_bytes: 4 * 1024 * 1024,
            max_work: 64 * 1024 * 1024,
        },
    }
}
fn payload_limits() -> ConnectorPayloadProjectionLimits {
    ConnectorPayloadProjectionLimits {
        max_definitions: 16,
        max_payload_bytes: 4096,
        max_allocation_requests: 64,
        max_allocation_request_bytes: 64 * 1024,
        max_coexisting_source_and_request_bytes: SOURCE,
        max_work: 1024 * 1024,
    }
}
fn type_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 4096,
        max_expanded_nodes: 4096,
        max_string_bytes: 1024 * 1024,
    }
}
fn value_limits() -> ValueProjectionLimits {
    ValueProjectionLimits {
        max_definitions: 1024,
        max_origin_references: 8192,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 1024 * 1024,
        max_coexisting_source_and_request_bytes: SOURCE,
        max_work: 64 * 1024 * 1024,
        origins: ValueOriginProjectionLimits {
            max_allocation_requests: 64,
            max_allocation_request_bytes: 64 * 1024,
            max_coexisting_source_and_request_bytes: SOURCE,
            max_work: 1024 * 1024,
        },
    }
}
fn definitions() -> Vec<p::ValueDef> {
    [0, 1, 2, 3, u32::MAX]
        .into_iter()
        .map(|id| p::ValueDef {
            id: p::ValueId::new(id),
            ty: FunctionValueType::new(DataType::Int64, id == 2),
            origin: if id == 2 {
                p::ValueOrigin::NullExtended {
                    node: p::NodeId::new(u32::MAX),
                    of: p::ValueId::new(0),
                }
            } else {
                p::ValueOrigin::NodeOutput {
                    node: p::NodeId::new(if id == 3 { u32::MAX } else { 0 }),
                    output_ordinal: if id == 3 { 2 } else { id },
                }
            },
        })
        .collect()
}
fn with_namespaces<R>(
    definitions: &[p::ValueDef],
    control: &Control,
    run: impl FnOnce(&EncodedValues<'_, '_, '_>, &DecodedValues<'_, '_, '_>) -> R,
) -> R {
    let roots: Vec<_> = definitions
        .iter()
        .enumerate()
        .map(|(i, value)| (u32::try_from(i).unwrap(), value.ty.clone()))
        .collect();
    let types = encode_type_table_sources(&roots, &[], type_limits(), control).unwrap();
    let decoded_types = decode_type_table(types.as_wire(), type_limits(), control).unwrap();
    let payloads = encode_connector_payloads(&[], PRIOR, payload_limits(), control).unwrap();
    let decoded_payloads =
        decode_connector_payloads(payloads.as_wire(), PRIOR, payload_limits(), control).unwrap();
    let inputs: Vec<_> = definitions
        .iter()
        .enumerate()
        .map(|(i, source)| ValueSource {
            source,
            value_type_id: u32::try_from(i).unwrap(),
        })
        .collect();
    let encoded = encode_values(&inputs, &payloads, &types, VALUE_SOURCE, value_limits()).unwrap();
    let decoded = decode_values(
        encoded.as_wire(),
        &decoded_payloads,
        &decoded_types,
        VALUE_SOURCE,
        value_limits(),
    )
    .unwrap();
    control.arm(None);
    run(&encoded, &decoded)
}
fn property() -> p::PhysicalProperties {
    p::PhysicalProperties {
        distribution: p::Distribution::Singleton,
        row_multiplicity: p::RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn required_property() -> p::PhysicalProperties {
    p::passthrough_requirement(&property())
}
fn source() -> p::PhysicalNode {
    p::PhysicalNode {
        expressions: None,
        id: p::NodeId::new(u32::MAX),
        inputs: Box::from([p::NodeId::new(0)]),
        required_inputs: Box::from([required_property()]),
        output_properties: property(),
        output: p::OutputPort {
            node: p::NodeId::new(u32::MAX),
            columns: Box::from([p::ValueId::new(2), p::ValueId::new(1), p::ValueId::new(3)]),
        },
        kind: p::NodeKind::Repeat {
            rollup_keys: Box::from([p::ValueId::new(0)]),
            grouping_sets: Box::from([
                Box::from([p::ValueId::new(0)]),
                Box::<[p::ValueId]>::default(),
            ]),
            grouping_values: Box::from([(p::ValueId::new(0), p::ValueId::new(2))]),
            grouping_outputs: Box::from([p::GroupingOutput {
                output: p::ValueId::new(3),
                arguments: Box::from([p::ValueId::new(0)]),
            }]),
        },
    }
}
fn expected_property() -> wire::PhysicalProperties {
    wire::PhysicalProperties {
        distribution: Some(wire::Distribution {
            kind: Some(wire::distribution::Kind::Singleton(Empty {})),
        }),
        row_multiplicity: wire::RowMultiplicity::SingleCopy as i32,
        ordering: vec![],
    }
}
fn expected_required_property() -> wire::PhysicalProperties {
    wire::PhysicalProperties {
        distribution: Some(wire::Distribution {
            kind: Some(wire::distribution::Kind::Unconstrained(Empty {})),
        }),
        row_multiplicity: wire::RowMultiplicity::SingleCopy as i32,
        ordering: vec![],
    }
}
fn expected() -> wire::PhysicalNode {
    wire::PhysicalNode {
        expressions: None,
        id: u32::MAX,
        input_node_ids: vec![0],
        required_inputs: vec![expected_required_property()],
        output_properties: Some(expected_property()),
        output: Some(wire::OutputPort {
            node_id: Some(u32::MAX),
            value_ids: vec![2, 1, 3],
        }),
        kind: Some(wire::physical_node::Kind::Repeat(wire::RepeatNode {
            rollup_key_value_ids: vec![0],
            grouping_sets: vec![
                wire::ValueIds { value_ids: vec![0] },
                wire::ValueIds { value_ids: vec![] },
            ],
            grouping_values: vec![wire::ValueMapping {
                source_value_id: Some(0),
                destination_value_id: Some(2),
            }],
            grouping_outputs: vec![wire::GroupingOutput {
                output_value_id: Some(3),
                argument_value_ids: vec![0],
            }],
        })),
    }
}
fn repeat_mut(input: &mut wire::PhysicalNode) -> &mut wire::RepeatNode {
    match input.kind.as_mut().unwrap() {
        wire::physical_node::Kind::Repeat(repeat) => repeat,
        _ => panic!("fixture kind"),
    }
}
fn assert_prefix<T>(
    control: &Control,
    phase: CompilePhase,
    operation: impl Fn() -> Result<T, Error>,
) {
    control.arm(None);
    operation().unwrap_or_else(|e| panic!("success fixture: {e}"));
    let baseline = control.trace();
    assert!(baseline.len() >= 2);
    assert!(baseline.iter().all(|(p, _)| *p == phase));
    for at in 0..baseline.len() {
        for cause in CAUSES {
            control.arm(Some((at, cause.clone())));
            assert!(matches!(operation(), Err(Error::Control(actual)) if actual == cause));
            assert_eq!(control.trace(), baseline[..=at]);
        }
    }
    control.arm(None);
}
fn assert_ordinary_prefix<T>(
    control: &Control,
    phase: CompilePhase,
    operation: impl Fn() -> Result<T, Error>,
) {
    control.arm(None);
    assert!(matches!(operation(), Err(error) if !matches!(error, Error::Control(_))));
    let baseline = control.trace();
    assert!(baseline.len() >= 2);
    assert_eq!(baseline[0], (phase, 0));
    assert_eq!(baseline.last().unwrap().0, phase);
    // A zero-unit tail is legal after delegates flush completed work.
    for at in 0..baseline.len() {
        for cause in CAUSES {
            control.arm(Some((at, cause.clone())));
            assert!(matches!(operation(), Err(Error::Control(actual)) if actual == cause));
            assert_eq!(control.trace(), baseline[..=at]);
        }
    }
    control.arm(None);
}

#[test]
fn complete_repeat_node_matches_independent_wire_and_real_builder_structure() {
    let control = Control::default();
    let mut builder = p::FragmentBuilder::new(p::FragmentId::new(0));
    let defs = definitions();
    for value in defs.iter().filter(|v| v.id.get() != u32::MAX) {
        builder.insert_value(value.clone()).unwrap();
    }
    builder
        .add_values(
            p::NodeId::new(0),
            Box::default(),
            Box::from([p::ValueId::new(0), p::ValueId::new(1)]),
        )
        .unwrap();
    let p::NodeKind::Repeat {
        rollup_keys,
        grouping_sets,
        grouping_values,
        grouping_outputs,
    } = source().kind
    else {
        unreachable!()
    };
    builder
        .add_repeat(
            p::NodeId::new(u32::MAX),
            p::NodeId::new(0),
            rollup_keys,
            grouping_sets,
            grouping_values,
            grouping_outputs,
            source().output.columns,
        )
        .unwrap();
    let fragment = builder
        .finish_structure(
            p::NodeId::new(u32::MAX),
            p::FragmentSink::Noop,
            p::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            p::PlanLimits::FROZEN,
            &control,
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let original = fragment.nodes().get(&p::NodeId::new(u32::MAX)).unwrap();
    let actual_defs: Vec<_> = fragment.values().values().cloned().collect();
    with_namespaces(&actual_defs, &control, |encoded, decoded| {
        let (wire, ef) = encode_repeat_node(original, encoded, SOURCE, limits()).unwrap();
        assert_eq!(wire, expected());
        let (received, df) = decode_repeat_node(&expected(), decoded, SOURCE, limits()).unwrap();
        assert_eq!(&received, original);
        assert_eq!(ef.input_node_count, 1);
        assert_eq!(ef.value_reference_count, 9);
        assert_eq!(ef.list_item_count, 11);
        assert_eq!(df.value_reference_count, 9);
        assert_eq!(df.list_item_count, 11);
        let mut receiving_builder = p::FragmentBuilder::new(p::FragmentId::new(0));
        for value in &actual_defs {
            receiving_builder.insert_value(value.clone()).unwrap();
        }
        receiving_builder
            .insert_node_unchecked(fragment.nodes().get(&p::NodeId::new(0)).unwrap().clone())
            .unwrap();
        receiving_builder.insert_node_unchecked(received).unwrap();
        receiving_builder
            .finish_structure(
                p::NodeId::new(u32::MAX),
                p::FragmentSink::Noop,
                p::PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
                p::PlanLimits::FROZEN,
                &control,
                novarocks_physical_plan::StagePlacementPolicy::Singleton,
            )
            .unwrap();
    });
}

#[test]
fn sparse_zero_max_ids_ordered_repetitions_and_exact_property_identity_survive() {
    let control = Control::default();
    with_namespaces(&definitions(), &control, |encoded, decoded| {
        let mut input = source();
        input.id = p::NodeId::new(0);
        input.output.node = p::NodeId::new(0);
        input.inputs = Box::from([p::NodeId::new(u32::MAX), p::NodeId::new(0)]);
        input.required_inputs = Box::from([property(), property()]);
        input.output.columns = Box::from([
            p::ValueId::new(u32::MAX),
            p::ValueId::new(0),
            p::ValueId::new(u32::MAX),
        ]);
        input.output_properties.distribution = p::Distribution::Hash {
            keys: Box::from([p::ValueId::new(u32::MAX), p::ValueId::new(0)]),
            scheme: p::HashPartitionScheme {
                space: PartitionSpaceId::try_new([9; 32]).unwrap(),
                count: p::PartitionCountParameter {
                    id: PartitionCountParameterId::try_new([7; 32]).unwrap(),
                    admissible: p::PartitionCountDomain {
                        min: 2,
                        max: 8,
                        requires_power_of_two: true,
                    },
                },
                definition: p::HashDefinition {
                    algorithm: PartitionHashAlgorithm::NativeExchangeV1,
                },
            },
        };
        input.output_properties.ordering = Box::from([p::OrderingKey {
            value: p::ValueId::new(u32::MAX),
            direction: p::SortDirection::Descending,
            null_ordering: p::NullOrdering::First,
        }]);
        let p::NodeKind::Repeat {
            rollup_keys,
            grouping_sets,
            grouping_outputs,
            ..
        } = &mut input.kind
        else {
            unreachable!()
        };
        *rollup_keys = Box::from([p::ValueId::new(0), p::ValueId::new(u32::MAX)]);
        *grouping_sets = Box::from([
            Box::from([
                p::ValueId::new(u32::MAX),
                p::ValueId::new(0),
                p::ValueId::new(u32::MAX),
            ]),
            Box::<[p::ValueId]>::default(),
        ]);
        grouping_outputs[0].arguments = Box::from([
            p::ValueId::new(u32::MAX),
            p::ValueId::new(0),
            p::ValueId::new(u32::MAX),
        ]);
        let (wire, _) = encode_repeat_node(&input, encoded, SOURCE, limits()).unwrap();
        assert_eq!(wire.id, 0);
        assert_eq!(wire.input_node_ids, [u32::MAX, 0]);
        assert_eq!(
            wire.output.as_ref().unwrap().value_ids,
            [u32::MAX, 0, u32::MAX]
        );
        let repeat = match wire.kind.as_ref().unwrap() {
            wire::physical_node::Kind::Repeat(v) => v,
            _ => unreachable!(),
        };
        assert_eq!(repeat.grouping_sets[0].value_ids, [u32::MAX, 0, u32::MAX]);
        assert_eq!(
            repeat.grouping_outputs[0].argument_value_ids,
            [u32::MAX, 0, u32::MAX]
        );
        let hash = match wire
            .output_properties
            .as_ref()
            .unwrap()
            .distribution
            .as_ref()
            .unwrap()
            .kind
            .as_ref()
            .unwrap()
        {
            wire::distribution::Kind::Hash(v) => v,
            _ => unreachable!(),
        };
        assert_eq!(hash.scheme.as_ref().unwrap().partition_space, [9; 32]);
        assert_eq!(
            hash.scheme.as_ref().unwrap().count.as_ref().unwrap().id,
            [7; 32]
        );
        assert_eq!(
            decode_repeat_node(&wire, decoded, SOURCE, limits())
                .unwrap()
                .0,
            input
        );
        // This component preserves occurrence shape, but deliberately does not
        // authenticate self-inputs or grouping/nullable semantics as a graph.
    });
}

#[test]
fn mandatory_wire_presence_and_every_payload_namespace_reference_refuse() {
    let control = Control::default();
    with_namespaces(&definitions(), &control, |encoded, decoded| {
        let mut bads = Vec::new();
        let mut bad = expected();
        bad.kind = None;
        bads.push(bad);
        let mut bad = expected();
        bad.output = None;
        bads.push(bad);
        let mut bad = expected();
        bad.output.as_mut().unwrap().node_id = None;
        bads.push(bad);
        let mut bad = expected();
        bad.output_properties = None;
        bads.push(bad);
        let mut bad = expected();
        repeat_mut(&mut bad).grouping_values[0].source_value_id = None;
        bads.push(bad);
        let mut bad = expected();
        repeat_mut(&mut bad).grouping_values[0].destination_value_id = None;
        bads.push(bad);
        let mut bad = expected();
        repeat_mut(&mut bad).grouping_outputs[0].output_value_id = None;
        bads.push(bad);
        for target in 0..8 {
            let mut bad = expected();
            match target {
                0 => bad.output.as_mut().unwrap().value_ids[0] = 99,
                1 => repeat_mut(&mut bad).rollup_key_value_ids[0] = 99,
                2 => repeat_mut(&mut bad).grouping_sets[0].value_ids[0] = 99,
                3 => repeat_mut(&mut bad).grouping_values[0].source_value_id = Some(99),
                4 => repeat_mut(&mut bad).grouping_values[0].destination_value_id = Some(99),
                5 => repeat_mut(&mut bad).grouping_outputs[0].output_value_id = Some(99),
                6 => repeat_mut(&mut bad).grouping_outputs[0].argument_value_ids[0] = 99,
                _ => bad
                    .output_properties
                    .as_mut()
                    .unwrap()
                    .ordering
                    .push(wire::OrderingKey {
                        value_id: Some(99),
                        direction: 1,
                        null_ordering: 1,
                    }),
            }
            bads.push(bad);
        }
        for bad in bads {
            assert!(decode_repeat_node(&bad, decoded, SOURCE, limits()).is_err());
        }
        let mut bad = source();
        bad.kind = p::NodeKind::Limit {
            limit: None,
            offset: 0,
        };
        assert!(matches!(
            encode_repeat_node(&bad, encoded, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
        let mut bad = source();
        bad.output.columns = Box::from([p::ValueId::new(99)]);
        assert!(matches!(
            encode_repeat_node(&bad, encoded, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
    });
}

#[test]
fn whole_node_resource_caps_are_exact_and_each_one_below_refuses() {
    let control = Control::default();
    with_namespaces(&definitions(), &control, |encoded, decoded| {
        let original = source();
        let wire = expected();
        for receiving in [false, true] {
            let facts = if receiving {
                decode_repeat_node(&wire, decoded, SOURCE, limits())
                    .unwrap()
                    .1
            } else {
                encode_repeat_node(&original, encoded, SOURCE, limits())
                    .unwrap()
                    .1
            };
            let exact = RepeatNodeProjectionLimits {
                max_input_nodes: facts.input_node_count,
                max_value_references: facts.value_reference_count,
                max_list_items: facts.list_item_count,
                max_allocation_requests: facts.allocation_requests_upper_bound,
                max_allocation_request_bytes: facts.allocation_request_bytes_upper_bound,
                max_coexisting_source_and_request_bytes: facts
                    .coexisting_source_and_request_bytes_upper_bound,
                max_work: facts.cumulative_work_upper_bound,
                properties: limits().properties,
            };
            let run = |l| {
                if receiving {
                    decode_repeat_node(&wire, decoded, SOURCE, l).map(|(_, f)| f)
                } else {
                    encode_repeat_node(&original, encoded, SOURCE, l).map(|(_, f)| f)
                }
            };
            assert_eq!(run(exact).unwrap(), facts);
            for cap in 0..7 {
                let mut l = exact;
                match cap {
                    0 => l.max_input_nodes -= 1,
                    1 => l.max_value_references -= 1,
                    2 => l.max_list_items -= 1,
                    3 => l.max_allocation_requests -= 1,
                    4 => l.max_allocation_request_bytes -= 1,
                    5 => l.max_coexisting_source_and_request_bytes -= 1,
                    _ => l.max_work -= 1,
                }
                assert!(
                    matches!(
                        run(l),
                        Err(Error::Control(CompileControlError::ResourceExhausted))
                    ),
                    "cap {cap}"
                );
            }
            assert!(
                facts.coexisting_source_and_request_bytes_upper_bound
                    == SOURCE + facts.allocation_request_bytes_upper_bound
            );
            assert!(
                control
                    .trace()
                    .iter()
                    .map(|(_, n)| *n as usize)
                    .sum::<usize>()
                    >= 1
            );
        }
        // Independent output Layout oracle: required properties, input IDs,
        // output IDs, rollup IDs, outer/inner sets, mapping and grouping output.
        let own = bytes::<u32>(1).unwrap()
            + bytes::<wire::PhysicalProperties>(1).unwrap()
            + bytes::<u32>(3).unwrap()
            + bytes::<u32>(1).unwrap()
            + bytes::<wire::ValueIds>(2).unwrap()
            + bytes::<u32>(1).unwrap()
            + bytes::<wire::ValueMapping>(1).unwrap()
            + bytes::<wire::GroupingOutput>(1).unwrap()
            + bytes::<u32>(1).unwrap();
        let ef = encode_repeat_node(&original, encoded, SOURCE, limits())
            .unwrap()
            .1;
        assert_eq!(ef.allocation_request_bytes_upper_bound, own);
        assert_eq!(ef.allocation_requests_upper_bound, 9);
        let df = decode_repeat_node(&wire, decoded, SOURCE, limits())
            .unwrap()
            .1;
        let receiving_own = bytes::<p::NodeId>(1).unwrap()
            + bytes::<p::PhysicalProperties>(1).unwrap()
            + bytes::<p::ValueId>(3).unwrap()
            + bytes::<p::ValueId>(1).unwrap()
            + bytes::<Box<[p::ValueId]>>(2).unwrap()
            + bytes::<p::ValueId>(1).unwrap()
            + bytes::<(p::ValueId, p::ValueId)>(1).unwrap()
            + bytes::<p::GroupingOutput>(1).unwrap()
            + bytes::<p::ValueId>(1).unwrap();
        assert_eq!(df.allocation_request_bytes_upper_bound, 2 * receiving_own);
        assert_eq!(df.allocation_requests_upper_bound, 18);
    });
}

#[test]
fn repeat_known_resource_refusal_precedes_late_control_and_never_runs_footer() {
    let control = Control::default();
    with_namespaces(&definitions(), &control, |encoded, decoded| {
        let original = source();
        let wire = expected();
        assert_eq!(original.inputs.len(), 1);
        assert_eq!(wire.input_node_ids.len(), 1);
        let mut cap = limits();
        cap.max_input_nodes = 0;
        for cause in CAUSES {
            control.arm(Some((1, cause.clone())));
            assert!(matches!(
                encode_repeat_node(&original, encoded, SOURCE, cap),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(control.trace(), vec![(CompilePhase::Encode, 0)]);
            control.arm(Some((1, cause)));
            assert!(matches!(
                decode_repeat_node(&wire, decoded, SOURCE, cap),
                Err(Error::Control(CompileControlError::ResourceExhausted))
            ));
            assert_eq!(control.trace(), vec![(CompilePhase::Decode, 0)]);
        }
    });
}

#[test]
fn original_source_floor_and_received_capacity_are_checked_before_emission() {
    let control = Control::default();
    with_namespaces(&definitions(), &control, |encoded, decoded| {
        let namespace_floor = decoded.retained_invoice_floor().unwrap();
        assert!(matches!(
            encode_repeat_node(&source(), encoded, 0, limits()),
            Err(Error::InvalidShape(_))
        ));
        assert!(matches!(
            decode_repeat_node(&expected(), decoded, 0, limits()),
            Err(Error::InvalidShape(_))
        ));
        assert_eq!(
            decode_repeat_node(&expected(), decoded, namespace_floor, limits())
                .unwrap()
                .0,
            source()
        );
        let mut sparse = expected();
        sparse.input_node_ids.reserve(131072);
        let mut l = limits();
        l.max_coexisting_source_and_request_bytes = 4 * SOURCE;
        assert!(matches!(
            decode_repeat_node(&sparse, decoded, namespace_floor, l),
            Err(Error::InvalidShape(_))
        ));
        assert_eq!(
            decode_repeat_node(&sparse, decoded, SOURCE, l).unwrap().0,
            source()
        );
        let mut nested_capacity = expected();
        repeat_mut(&mut nested_capacity).grouping_sets[0]
            .value_ids
            .reserve(131072);
        assert!(matches!(
            decode_repeat_node(&nested_capacity, decoded, namespace_floor, l),
            Err(Error::InvalidShape(_))
        ));
        assert_eq!(
            decode_repeat_node(&nested_capacity, decoded, SOURCE, l)
                .unwrap()
                .0,
            source()
        );
    });
}

#[test]
fn prepared_tokens_emit_exact_fields_without_repeating_namespace_lookup() {
    let control = Control::default();
    with_namespaces(&definitions(), &control, |encoded, decoded| {
        let original = source();
        let wire = expected();
        control.arm(None);
        let token = prepare_repeat_node_encode(&original, encoded, SOURCE, limits()).unwrap();
        let facts = *token.facts();
        let prepare_trace = control.trace();
        assert_eq!(prepare_trace.first(), Some(&(CompilePhase::Encode, 0)));
        control.arm(None);
        let (actual, actual_facts) = token.emit().unwrap();
        assert_eq!(actual, wire);
        assert_eq!(facts, actual_facts);
        let emission = control.trace();
        assert_eq!(emission.first(), Some(&(CompilePhase::Encode, 0)));
        let split_work = prepare_trace
            .iter()
            .chain(&emission)
            .map(|(_, n)| *n as usize)
            .sum::<usize>();
        control.arm(None);
        assert_eq!(
            encode_repeat_node(&original, encoded, SOURCE, limits())
                .unwrap()
                .0,
            wire
        );
        assert_eq!(
            control
                .trace()
                .iter()
                .map(|(_, n)| *n as usize)
                .sum::<usize>(),
            split_work
        );
        let token = prepare_repeat_node_decode(&wire, decoded, SOURCE, limits()).unwrap();
        let facts = *token.facts();
        assert_eq!(token.emit().unwrap(), (original, facts));
    });
}

#[test]
fn actual_small_success_and_ordinary_failure_preserve_every_original_control_prefix() {
    let control = Control::default();
    with_namespaces(&definitions(), &control, |encoded, decoded| {
        let original = source();
        let wire = expected();
        assert_prefix(&control, CompilePhase::Encode, || {
            encode_repeat_node(&original, encoded, SOURCE, limits())
        });
        assert_prefix(&control, CompilePhase::Decode, || {
            decode_repeat_node(&wire, decoded, SOURCE, limits())
        });
        assert_prefix(&control, CompilePhase::Encode, || {
            prepare_repeat_node_encode(&original, encoded, SOURCE, limits())?.emit()
        });
        assert_prefix(&control, CompilePhase::Decode, || {
            prepare_repeat_node_decode(&wire, decoded, SOURCE, limits())?.emit()
        });
        let mut bad = wire.clone();
        repeat_mut(&mut bad).grouping_values[0].destination_value_id = None;
        assert_ordinary_prefix(&control, CompilePhase::Decode, || {
            decode_repeat_node(&bad, decoded, SOURCE, limits())
        });
        let mut bad_property = wire.clone();
        bad_property
            .output_properties
            .as_mut()
            .unwrap()
            .row_multiplicity = i32::MAX;
        assert_ordinary_prefix(&control, CompilePhase::Decode, || {
            decode_repeat_node(&bad_property, decoded, SOURCE, limits())
        });
        assert_ordinary_prefix(&control, CompilePhase::Decode, || {
            prepare_repeat_node_decode(&bad_property, decoded, SOURCE, limits())?.emit()
        });
        let mut bad = original.clone();
        bad.output.columns = Box::from([p::ValueId::new(99)]);
        assert_ordinary_prefix(&control, CompilePhase::Encode, || {
            encode_repeat_node(&bad, encoded, SOURCE, limits())
        });
    });
}

#[test]
fn real_wide_lists_observe_quantum_and_original_refusal_before_further_work() {
    let control = Control::default();
    with_namespaces(&definitions(), &control, |encoded, decoded| {
        let mut original = source();
        let p::NodeKind::Repeat {
            grouping_sets,
            grouping_outputs,
            ..
        } = &mut original.kind
        else {
            unreachable!()
        };
        *grouping_sets = (0..320)
            .map(|_| Box::<[p::ValueId]>::default())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        grouping_outputs[0].arguments = vec![p::ValueId::new(0); 320].into_boxed_slice();
        let (wire, _) = encode_repeat_node(&original, encoded, SOURCE, limits()).unwrap();
        for receiving in [false, true] {
            let run = || {
                if receiving {
                    decode_repeat_node(&wire, decoded, SOURCE, limits()).map(|(_, f)| f)
                } else {
                    encode_repeat_node(&original, encoded, SOURCE, limits()).map(|(_, f)| f)
                }
            };
            control.arm(None);
            let facts = run().unwrap();
            let baseline = control.trace();
            assert!(baseline.iter().any(|(_, units)| *units == 256));
            assert!(
                baseline.iter().map(|(_, n)| *n as usize).sum::<usize>()
                    <= facts.cumulative_work_upper_bound
            );
            for at in (0..baseline.len())
                .filter(|i| *i == 0 || *i + 1 == baseline.len() || baseline[*i].1 == 256)
            {
                for cause in CAUSES {
                    control.arm(Some((at, cause.clone())));
                    assert!(matches!(run(), Err(Error::Control(actual)) if actual == cause));
                    assert_eq!(control.trace(), baseline[..=at]);
                }
            }
            control.arm(None);
        }
    });
}

#[test]
fn original_repeat_envelope_trace_remains_exact_after_shared_author_extraction() {
    // Literal traces were recorded by the original sole Repeat author before extraction.
    let control = Control::default();
    with_namespaces(&definitions(), &control, |encoded, decoded| {
        let original = source();
        let wire = expected();
        control.arm(None);
        assert_eq!(
            encode_repeat_node(&original, encoded, SOURCE, limits())
                .unwrap()
                .0,
            wire
        );
        assert_eq!(
            control.trace(),
            [
                (CompilePhase::Encode, 0),
                (CompilePhase::Encode, 60),
                (CompilePhase::Encode, 0),
                (CompilePhase::Encode, 1),
                (CompilePhase::Encode, 0),
                (CompilePhase::Encode, 5),
                (CompilePhase::Encode, 0),
                (CompilePhase::Encode, 1),
                (CompilePhase::Encode, 0),
                (CompilePhase::Encode, 6),
                (CompilePhase::Encode, 0),
                (CompilePhase::Encode, 1),
                (CompilePhase::Encode, 0),
                (CompilePhase::Encode, 0),
                (CompilePhase::Encode, 0),
                (CompilePhase::Encode, 3),
                (CompilePhase::Encode, 0),
                (CompilePhase::Encode, 1),
                (CompilePhase::Encode, 0),
                (CompilePhase::Encode, 0),
                (CompilePhase::Encode, 0),
                (CompilePhase::Encode, 2),
                (CompilePhase::Encode, 0),
                (CompilePhase::Encode, 1),
                (CompilePhase::Encode, 0),
                (CompilePhase::Encode, 1),
                (CompilePhase::Encode, 0),
                (CompilePhase::Encode, 0),
                (CompilePhase::Encode, 0),
                (CompilePhase::Encode, 3)
            ]
        );
        control.arm(None);
        assert_eq!(
            decode_repeat_node(&wire, decoded, SOURCE, limits())
                .unwrap()
                .0,
            original
        );
        assert_eq!(
            control.trace(),
            [
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 60),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 1),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 6),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 1),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 1),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 6),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 1),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 3),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 1),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 1),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 1),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 1),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 1),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 1),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 1),
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 1)
            ]
        );
        let mut bad = wire.clone();
        repeat_mut(&mut bad).grouping_values[0].destination_value_id = None;
        control.arm(None);
        assert!(matches!(
            decode_repeat_node(&bad, decoded, SOURCE, limits()),
            Err(Error::InvalidShape(
                "Repeat grouping destination value is absent"
            ))
        ));
        assert_eq!(
            control.trace(),
            [(CompilePhase::Decode, 0), (CompilePhase::Decode, 46)]
        );
    });
}

#[path = "owned_tests.rs"]
mod owned_tests;
