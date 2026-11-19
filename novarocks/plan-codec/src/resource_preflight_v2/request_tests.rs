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
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_proto_models::resource_layout::GeneratedResourceLayout;
use prost::Message;
use prost::encoding::{WireType, encode_key, encode_varint};
use std::mem::{align_of, size_of};
use std::sync::Mutex;

#[derive(Default)]
struct Control {
    events: Mutex<Vec<(CompilePhase, u32)>>,
    failure: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
        assert!(units <= 256);
        let mut events = self.events.lock().unwrap();
        let ordinal = events.len();
        events.push((phase, units));
        match self.failure.clone() {
            Some((at, cause)) if at == ordinal => Err(cause),
            _ => Ok(()),
        }
    }
}
struct ModelControl;
impl PureCompileControl for ModelControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        Ok(())
    }
}
fn model() -> FragmentDecodeResourceModel {
    FragmentDecodeResourceModel::try_new(&ModelControl).unwrap()
}
fn limits() -> DecodeProjectionLimits {
    DecodeProjectionLimits {
        max_input_bytes: 1 << 20,
        max_requested_heap_bytes: 64 << 20,
        max_message_occurrences: 100_000,
        max_scalar_elements: 100_000,
        max_field_occurrences: 100_000,
        max_copied_bytes: 64 << 20,
        max_initialization_bytes: 64 << 20,
        max_wire_depth: 100,
    }
}
fn zero_policy() -> wire::SourceConstantPolicy {
    wire::SourceConstantPolicy {
        max_rows: Some(0),
        max_array_nodes: Some(0),
        max_logical_elements: Some(0),
        max_retained_buffer_bytes: Some(0),
        max_type_depth: Some(0),
        max_type_nodes: Some(0),
        max_dictionary_depth: Some(0),
        max_metadata_bytes: Some(0),
        max_library_validation_work: Some(0),
        max_library_validation_bytes: Some(0),
    }
}
fn request() -> wire::UnitCallRequest {
    wire::UnitCallRequest {
        definition_id: Some(0),
        arguments: vec![
            wire::OriginalFunctionArgument {
                kind: Some(wire::original_function_argument::Kind::Value(
                    wire::OriginalValueArgument {
                        value_type_id: Some(0),
                        constant: None,
                    },
                )),
            },
            wire::OriginalFunctionArgument {
                kind: Some(wire::original_function_argument::Kind::Value(
                    wire::OriginalValueArgument {
                        value_type_id: Some(u32::MAX),
                        constant: Some(wire::ConstantReference {
                            pool_id: Some(0),
                            row_ordinal: 7,
                        }),
                    },
                )),
            },
            wire::OriginalFunctionArgument {
                kind: Some(wire::original_function_argument::Kind::Lambda(
                    wire::LambdaArgumentType {
                        parameter_value_type_ids: vec![0, u32::MAX, 7],
                        result_value_type_id: Some(0),
                    },
                )),
            },
        ],
        logical_argument_count: Some(2),
        expected_result_value_type_id: Some(u32::MAX),
        constant_policy: Some(zero_policy()),
    }
}
// Resource-only syntax fixture, not a semantically published unit. The real
// unit author checks definitions, requests and claims after carrier admission.
fn package(entries: Vec<wire::UnitCallRequest>) -> wire::FragmentPackage {
    wire::FragmentPackage {
        fragment: Some(wire::Fragment {
            nodes: vec![wire::PhysicalNode {
                expressions: Some(wire::ExpressionUnit {
                    requests: entries,
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}
fn decoded_requests(package: &wire::FragmentPackage) -> &Vec<wire::UnitCallRequest> {
    &package.fragment.as_ref().unwrap().nodes[0]
        .expressions
        .as_ref()
        .unwrap()
        .requests
}
fn ld(tag: u32, payload: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();
    encode_key(tag, WireType::LengthDelimited, &mut result);
    encode_varint(payload.len() as u64, &mut result);
    result.extend_from_slice(payload);
    result
}
fn wrap_request(raw_request: &[u8]) -> Vec<u8> {
    // Independent wire geometry: Package.fragment / Fragment.nodes /
    // PhysicalNode.expressions / ExpressionUnit.requests. No descriptor walk.
    ld(3, &ld(5, &ld(30, &ld(6, raw_request))))
}
fn size<T: GeneratedResourceLayout>() -> usize {
    assert_eq!(T::RESOURCE_LAYOUT.size, size_of::<T>());
    assert_eq!(T::RESOURCE_LAYOUT.alignment, align_of::<T>());
    size_of::<T>()
}
fn causes() -> [CompileControlError; 3] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ]
}
fn assert_prefixes(
    model: &FragmentDecodeResourceModel,
    raw: &[u8],
    envelope: DecodeProjectionLimits,
    trace: &[(CompilePhase, u32)],
    positions: &[usize],
) {
    for &at in positions {
        for cause in causes() {
            let control = Control {
                failure: Some((at, cause.clone())),
                ..Default::default()
            };
            assert_eq!(
                model.preflight(raw, envelope, &control),
                Err(ResourceModelError::Control(cause))
            );
            assert_eq!(*control.events.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn request_payload_presence_and_order_have_independent_wire_counts_and_storage_floor() {
    let original = request();
    let raw = package(vec![original.clone()]).encode_to_vec();
    assert_eq!(raw, wrap_request(&original.encode_to_vec()));
    let projection = model()
        .preflight(&raw, limits(), &Control::default())
        .unwrap();
    assert_eq!(projection.status, ResourceCursorStatus::Complete);
    // Hand count: four outer messages, then one unit request, three argument
    // wrappers, two Value payloads, one reference, one Lambda and one policy.
    assert_eq!(projection.usage.message_occurrences, 13);
    assert_eq!(projection.usage.field_occurrences, 31);
    assert_eq!(projection.usage.scalar_elements, 21);
    let message_initialization_floor = size::<wire::FragmentPackage>()
        + size::<wire::Fragment>()
        + size::<wire::PhysicalNode>()
        + size::<wire::ExpressionUnit>()
        + size::<wire::UnitCallRequest>()
        + 3 * size::<wire::OriginalFunctionArgument>()
        + 2 * size::<wire::OriginalValueArgument>()
        + size::<wire::ConstantReference>()
        + size::<wire::LambdaArgumentType>()
        + size::<wire::SourceConstantPolicy>();
    assert!(projection.usage.initialization_bytes_upper >= message_initialization_floor);

    let decoded = wire::FragmentPackage::decode(raw.as_slice()).unwrap();
    let entries = decoded_requests(&decoded);
    assert_eq!(entries, &vec![original]);
    let actual = &entries[0];
    let Some(wire::original_function_argument::Kind::Lambda(lambda)) =
        actual.arguments[2].kind.as_ref()
    else {
        panic!("ordered Lambda was lost");
    };
    assert_eq!(lambda.parameter_value_type_ids, [0, u32::MAX, 7]);
    assert_eq!(lambda.result_value_type_id, Some(0));
    assert_eq!(actual.constant_policy, Some(zero_policy()));
    let live_vec_backing = entries.capacity() * size_of::<wire::UnitCallRequest>()
        + decoded.fragment.as_ref().unwrap().nodes.capacity() * size_of::<wire::PhysicalNode>()
        + actual.arguments.capacity() * size_of::<wire::OriginalFunctionArgument>()
        + lambda.parameter_value_type_ids.capacity() * size_of::<u32>();
    assert!(live_vec_backing > 0);
    assert!(
        projection.usage.cumulative_requested_heap_bytes_upper
            >= projection.usage.error_requested_heap_bytes_upper + live_vec_backing
    );
}

#[test]
fn relation_request_carrier_keeps_the_real_site_and_original_argument_payload() {
    use novarocks_proto_models::physical_semantics_v2 as semantics;
    let scalar = request();
    let relation = wire::OriginalCallRequest {
        definition: Some(wire::CallRequestDefinition {
            kind: Some(wire::call_request_definition::Kind::Relational(
                semantics::CallSite {
                    kind: Some(semantics::call_site::Kind::Window(
                        semantics::NodeCallSite {
                            node_id: Some(0),
                            call: u32::MAX,
                        },
                    )),
                },
            )),
        }),
        arguments: scalar.arguments,
        logical_argument_count: scalar.logical_argument_count,
        expected_result_value_type_id: scalar.expected_result_value_type_id,
        constant_policy: scalar.constant_policy,
    };
    let package = wire::FragmentPackage {
        fragment: Some(wire::Fragment {
            call_requests: Some(wire::FragmentCallRequests {
                entries: vec![relation.clone()],
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    let raw = package.encode_to_vec();
    assert_eq!(raw, ld(3, &ld(9, &ld(1, &relation.encode_to_vec()))));
    let projection = model()
        .preflight(&raw, limits(), &Control::default())
        .unwrap();
    assert_eq!(projection.status, ResourceCursorStatus::Complete);
    let decoded = wire::FragmentPackage::decode(raw.as_slice()).unwrap();
    assert_eq!(
        decoded
            .fragment
            .as_ref()
            .unwrap()
            .call_requests
            .as_ref()
            .unwrap()
            .entries,
        vec![relation]
    );
    let relation_floor = size::<wire::OriginalCallRequest>()
        + size::<wire::CallRequestDefinition>()
        + size::<semantics::CallSite>()
        + size::<semantics::NodeCallSite>();
    assert!(projection.usage.initialization_bytes_upper >= relation_floor);
}

#[test]
fn request_each_receiving_limit_accepts_exact_boundary_and_refuses_one_under() {
    let model = model();
    let raw = package(vec![request()]).encode_to_vec();
    let usage = model
        .preflight(&raw, limits(), &Control::default())
        .unwrap()
        .usage;
    // Seven descents reach the Value's ConstantReference; these source-policy
    // zeros never replace any receiving limit.
    let exact = DecodeProjectionLimits {
        max_input_bytes: raw.len(),
        max_requested_heap_bytes: usage.cumulative_requested_heap_bytes_upper,
        max_message_occurrences: 13,
        max_scalar_elements: 21,
        max_field_occurrences: 31,
        max_copied_bytes: usage.copied_bytes_upper,
        max_initialization_bytes: usage.initialization_bytes_upper,
        max_wire_depth: 7,
    };
    assert_eq!(
        model
            .preflight(&raw, exact, &Control::default())
            .unwrap()
            .usage,
        usage
    );
    let under = [
        DecodeProjectionLimits {
            max_input_bytes: raw.len() - 1,
            ..exact
        },
        DecodeProjectionLimits {
            max_requested_heap_bytes: usage.cumulative_requested_heap_bytes_upper - 1,
            ..exact
        },
        DecodeProjectionLimits {
            max_message_occurrences: 12,
            ..exact
        },
        DecodeProjectionLimits {
            max_scalar_elements: 20,
            ..exact
        },
        DecodeProjectionLimits {
            max_field_occurrences: 30,
            ..exact
        },
        DecodeProjectionLimits {
            max_copied_bytes: usage.copied_bytes_upper - 1,
            ..exact
        },
        DecodeProjectionLimits {
            max_initialization_bytes: usage.initialization_bytes_upper - 1,
            ..exact
        },
        DecodeProjectionLimits {
            max_wire_depth: 6,
            ..exact
        },
    ];
    for envelope in under {
        assert!(matches!(
            model.preflight(&raw, envelope, &Control::default()),
            Err(ResourceModelError::Limit(_))
        ));
    }
}

#[test]
fn repeated_singular_request_policy_counts_both_occurrences_before_prost_merges() {
    let model = model();
    let mut original = request();
    original.constant_policy.as_mut().unwrap().max_rows = Some(9);
    let once = wrap_request(&original.encode_to_vec());
    let first = model
        .preflight(&once, limits(), &Control::default())
        .unwrap()
        .usage;
    let mut raw_request = original.encode_to_vec();
    raw_request.extend(ld(5, &zero_policy().encode_to_vec()));
    let twice = wrap_request(&raw_request);
    let second = model
        .preflight(&twice, limits(), &Control::default())
        .unwrap()
        .usage;
    assert_eq!(second.message_occurrences, 14);
    assert_eq!(second.scalar_elements, 31);
    assert_eq!(second.field_occurrences, 42);
    assert!(
        second.initialization_bytes_upper
            >= first.initialization_bytes_upper + size_of::<wire::SourceConstantPolicy>()
    );
    let decoded = wire::FragmentPackage::decode(twice.as_slice()).unwrap();
    assert_eq!(
        decoded_requests(&decoded)[0].constant_policy,
        Some(zero_policy())
    );
    // Charging only the final logical object would incorrectly accept this.
    assert!(matches!(
        model.preflight(
            &twice,
            DecodeProjectionLimits {
                max_scalar_elements: 21,
                ..limits()
            },
            &Control::default()
        ),
        Err(ResourceModelError::Limit(_))
    ));
}

#[test]
fn request_unknown_fields_and_malformed_argument_keep_the_accounted_prefix() {
    let model = model();
    let original = request();
    let raw = wrap_request(&original.encode_to_vec());
    let baseline = model
        .preflight(&raw, limits(), &Control::default())
        .unwrap()
        .usage;
    let mut unknown_request = original.encode_to_vec();
    unknown_request.extend(ld(100, &[0xff; 513]));
    let unknown = wrap_request(&unknown_request);
    let projection = model
        .preflight(&unknown, limits(), &Control::default())
        .unwrap();
    assert_eq!(projection.status, ResourceCursorStatus::Complete);
    assert_eq!(projection.usage.field_occurrences, 32);
    assert_eq!(projection.usage.message_occurrences, 13);
    assert_eq!(projection.usage.scalar_elements, 21);
    assert_eq!(
        projection.usage.cumulative_requested_heap_bytes_upper,
        baseline.cumulative_requested_heap_bytes_upper
    );
    assert_eq!(
        wire::FragmentPackage::decode(unknown.as_slice()).unwrap(),
        package(vec![original.clone()])
    );
    assert!(matches!(
        model.preflight(
            &unknown,
            DecodeProjectionLimits {
                max_field_occurrences: 31,
                ..limits()
            },
            &Control::default()
        ),
        Err(ResourceModelError::Limit(_))
    ));

    let mut malformed_request = original.encode_to_vec();
    // Known argument message construction is charged before its truncated
    // length varint fails. This is not acceptance of any request semantics.
    malformed_request.extend([0x12, 0x80]);
    let malformed = wrap_request(&malformed_request);
    let projection = model
        .preflight(&malformed, limits(), &Control::default())
        .unwrap();
    assert_eq!(projection.status, ResourceCursorStatus::MalformedPrefix);
    assert_eq!(projection.usage.message_occurrences, 14);
    assert_eq!(projection.usage.field_occurrences, 32);
    assert_eq!(projection.usage.scalar_elements, 21);
    assert!(
        projection.usage.cumulative_requested_heap_bytes_upper
            > baseline.cumulative_requested_heap_bytes_upper
    );
    assert!(wire::FragmentPackage::decode(malformed.as_slice()).is_err());
}

#[test]
fn request_small_success_malformed_and_limit_keep_every_original_control_prefix() {
    let model = model();
    let mut malformed_request = request().encode_to_vec();
    malformed_request.extend([0x12, 0x80]);
    let inputs = [
        (package(vec![request()]).encode_to_vec(), limits(), false),
        (wrap_request(&malformed_request), limits(), false),
        (
            package(vec![request()]).encode_to_vec(),
            DecodeProjectionLimits {
                max_message_occurrences: 12,
                ..limits()
            },
            true,
        ),
    ];
    for (raw, envelope, is_limit) in inputs {
        let control = Control::default();
        let result = model.preflight(&raw, envelope, &control);
        if is_limit {
            assert!(matches!(result, Err(ResourceModelError::Limit(_))));
        } else {
            assert!(result.is_ok());
        }
        let trace = control.events.into_inner().unwrap();
        assert_eq!(trace.first(), Some(&(CompilePhase::Decode, 0)));
        assert!(trace.len() >= 2);
        assert_prefixes(
            &model,
            &raw,
            envelope,
            &trace,
            &(0..trace.len()).collect::<Vec<_>>(),
        );
    }
}

#[test]
fn wide_request_wire_is_refused_before_prost_and_observes_real_decode_quanta() {
    let model = model();
    let raw = package(vec![request(); 320]).encode_to_vec();
    let control = Control::default();
    let projection = model.preflight(&raw, limits(), &control).unwrap();
    assert_eq!(projection.status, ResourceCursorStatus::Complete);
    assert_eq!(projection.usage.message_occurrences, 4 + 9 * 320);
    assert_eq!(projection.usage.scalar_elements, 21 * 320);
    assert_eq!(projection.usage.field_occurrences, 3 + 28 * 320);
    // Independent entry-count floor, even before argument/Lambda backing.
    assert!(
        projection.usage.cumulative_requested_heap_bytes_upper
            >= 320 * size_of::<wire::UnitCallRequest>()
    );
    assert!(matches!(
        model.preflight(
            &raw,
            DecodeProjectionLimits {
                max_message_occurrences: 4 + 9 * 320 - 1,
                ..limits()
            },
            &Control::default()
        ),
        Err(ResourceModelError::Limit(_))
    ));
    let trace = control.events.into_inner().unwrap();
    let quanta: Vec<_> = trace
        .iter()
        .enumerate()
        .filter_map(|(index, (_, units))| (*units == 256).then_some(index))
        .collect();
    assert!(quanta.len() > 2);
    let positions = [
        0,
        quanta[0],
        quanta[quanta.len() / 2],
        *quanta.last().unwrap(),
        trace.len() - 1,
    ];
    assert_prefixes(&model, &raw, limits(), &trace, &positions);
}
