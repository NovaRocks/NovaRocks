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
use prost::encoding::{WireType, encode_key, encode_varint};
use std::sync::Mutex;

const _: () = assert!(novarocks_type_contract::owned_resources::profile::LOCKED_TOOLCHAIN);
const _: () = assert!(novarocks_proto_models::resource_layout::RESOURCE_LAYOUT_REVISION == 1);
const _: () = assert!(std::mem::size_of::<prost::encoding::DecodeContext>() != 0);

#[derive(Default)]
struct Control {
    events: Mutex<Vec<(CompilePhase, u32)>>,
    fail: Option<(CompilePhase, usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut events = self.events.lock().unwrap();
        let ordinal = events.iter().filter(|(seen, _)| *seen == phase).count();
        events.push((phase, units));
        match self.fail.clone() {
            Some((target, at, error)) if target == phase && at == ordinal => Err(error),
            _ => Ok(()),
        }
    }
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
fn ld(tag: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    encode_key(tag, WireType::LengthDelimited, &mut out);
    encode_varint(payload.len() as u64, &mut out);
    out.extend_from_slice(payload);
    out
}
fn wide() -> Vec<u8> {
    // Independent raw oracle: Package.types tag4; TypeTable.carriers tag1.
    let mut table = Vec::new();
    for _ in 0..320 {
        table.extend(ld(1, &[]));
    }
    ld(4, &table)
}
fn errors() -> [CompileControlError; 3] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ]
}

#[test]
fn library_profile_is_the_actual_locked_workspace_decoder_and_container_version() {
    let lock = include_str!("../../../../Cargo.lock");
    let block = |name: &str| {
        lock.split("[[package]]")
            .find(|part| {
                part.lines()
                    .any(|line| line == format!("name = \"{name}\""))
            })
            .unwrap()
    };
    let models = block("novarocks-proto-models");
    assert!(models.contains("\"prost 0.13.5\""));
    assert!(block("bytes").contains("version = \"1.11.1\""));
}

#[test]
fn actual_generated_root_depth_and_empty_input_have_explicit_allocation_domains() {
    let model = FragmentDecodeResourceModel::try_new(&Control::default()).unwrap();
    assert_eq!(model.max_message_depth, 12);
    let result = model.preflight(&[], limits(), &Control::default()).unwrap();
    assert_eq!(result.status, ResourceCursorStatus::Complete);
    assert_eq!(result.usage.input_bytes, 0);
    assert_eq!(result.usage.message_occurrences, 1);
    assert_eq!(result.usage.field_occurrences, 0);
    assert_eq!(
        result.usage.root_inline_bytes,
        std::mem::size_of::<novarocks_proto_models::physical_package_v2::FragmentPackage>()
    );
    assert_eq!(
        result.usage.error_requested_heap_bytes_upper,
        allocation::error_heap(12).unwrap()
    );
    assert!(
        result.usage.cumulative_requested_heap_bytes_upper
            >= result.usage.error_requested_heap_bytes_upper
    );
    assert!(result.usage.copied_bytes_upper >= result.usage.error_requested_heap_bytes_upper);
    assert!(
        result.usage.initialization_bytes_upper >= result.usage.error_requested_heap_bytes_upper
    );
}

#[test]
fn every_caller_envelope_is_independent_and_refuses_before_dto_allocation() {
    let model = FragmentDecodeResourceModel::try_new(&Control::default()).unwrap();
    let raw = wide();
    let baseline = model
        .preflight(&raw, limits(), &Control::default())
        .unwrap()
        .usage;
    let variants = [
        DecodeProjectionLimits {
            max_input_bytes: raw.len() - 1,
            ..limits()
        },
        DecodeProjectionLimits {
            max_requested_heap_bytes: baseline.cumulative_requested_heap_bytes_upper - 1,
            ..limits()
        },
        DecodeProjectionLimits {
            max_message_occurrences: baseline.message_occurrences - 1,
            ..limits()
        },
        DecodeProjectionLimits {
            max_field_occurrences: baseline.field_occurrences - 1,
            ..limits()
        },
        DecodeProjectionLimits {
            max_copied_bytes: baseline.copied_bytes_upper - 1,
            ..limits()
        },
        DecodeProjectionLimits {
            max_initialization_bytes: baseline.initialization_bytes_upper - 1,
            ..limits()
        },
        DecodeProjectionLimits {
            max_wire_depth: 0,
            ..limits()
        },
    ];
    for envelope in variants {
        assert!(matches!(
            model.preflight(&raw, envelope, &Control::default()),
            Err(ResourceModelError::Limit(_))
        ));
    }
    assert!(matches!(
        model.preflight(
            &raw,
            DecodeProjectionLimits {
                max_wire_depth: 101,
                ..limits()
            },
            &Control::default()
        ),
        Err(ResourceModelError::Schema(_))
    ));
    // plan_version is one opaque bytes scalar, independently of its byte count.
    let bytes = ld(1, b"0123456789abcdef");
    assert!(matches!(
        model.preflight(
            &bytes,
            DecodeProjectionLimits {
                max_scalar_elements: 0,
                ..limits()
            },
            &Control::default()
        ),
        Err(ResourceModelError::Limit(_))
    ));
}

#[test]
fn unknown_group_internal_keys_are_observed_and_counted_against_the_same_envelope() {
    let model = FragmentDecodeResourceModel::try_new(&Control::default()).unwrap();
    let mut raw = Vec::new();
    encode_key(100, WireType::StartGroup, &mut raw);
    for _ in 0..320 {
        encode_key(101, WireType::Varint, &mut raw);
        encode_varint(0, &mut raw);
    }
    encode_key(100, WireType::EndGroup, &mut raw);
    let result = model
        .preflight(&raw, limits(), &Control::default())
        .unwrap();
    assert_eq!(result.status, ResourceCursorStatus::Complete);
    assert_eq!(result.usage.field_occurrences, 322);
    assert_eq!(result.usage.message_occurrences, 1);
    assert!(matches!(
        model.preflight(
            &raw,
            DecodeProjectionLimits {
                max_field_occurrences: 321,
                ..limits()
            },
            &Control::default()
        ),
        Err(ResourceModelError::Limit(_))
    ));
}

#[test]
fn malformed_prefix_preserves_completed_resource_charges_and_ordinary_error_tail() {
    let model = FragmentDecodeResourceModel::try_new(&Control::default()).unwrap();
    let mut raw = wide();
    // A second types message declares bytes that are not available.
    raw.extend([0x22, 0xff, 0x7f]);
    let control = Control::default();
    let result = model.preflight(&raw, limits(), &control).unwrap();
    assert_eq!(result.status, ResourceCursorStatus::MalformedPrefix);
    assert!(result.usage.message_occurrences >= 322);
    assert!(result.usage.field_occurrences >= 321);
    let trace = control.events.into_inner().unwrap();
    assert!(trace.iter().any(|(_, units)| *units == 256));
    for error in errors() {
        let stop = Control {
            fail: Some((CompilePhase::Decode, trace.len() - 1, error.clone())),
            ..Control::default()
        };
        assert!(
            matches!(model.preflight(&raw,limits(),&stop),Err(ResourceModelError::Control(actual)) if actual==error)
        );
    }
}

#[test]
fn original_control_refuses_model_and_raw_scan_entry_quantum_tail_before_publication() {
    let baseline = Control::default();
    let model = FragmentDecodeResourceModel::try_new(&baseline).unwrap();
    let build_trace = baseline.events.into_inner().unwrap();
    let raw = wide();
    let baseline = Control::default();
    model.preflight(&raw, limits(), &baseline).unwrap();
    let decode_trace = baseline.events.into_inner().unwrap();
    for trace in [&build_trace, &decode_trace] {
        assert_eq!(trace[0].1, 0);
        assert!(trace.iter().any(|(_, units)| *units == 256));
        assert!(trace.iter().any(|(_, units)| *units > 0 && *units < 256));
    }
    for error in errors() {
        for at in 0..build_trace.len() {
            let stop = Control {
                fail: Some((CompilePhase::Validate, at, error.clone())),
                ..Control::default()
            };
            assert!(
                matches!(FragmentDecodeResourceModel::try_new(&stop),Err(ResourceModelError::Control(actual)) if actual==error)
            );
        }
        for at in 0..decode_trace.len() {
            let stop = Control {
                fail: Some((CompilePhase::Decode, at, error.clone())),
                ..Control::default()
            };
            assert!(
                matches!(model.preflight(&raw,limits(),&stop),Err(ResourceModelError::Control(actual)) if actual==error)
            );
        }
    }
}

#[test]
fn byte_replacements_bound_old_capacity_moves_and_error_work_independently() {
    let model = FragmentDecodeResourceModel::try_new(&Control::default()).unwrap();
    let mut raw = ld(1, b"x");
    raw.extend(ld(1, b"123456789"));
    let proof = model
        .preflight(&raw, limits(), &Control::default())
        .unwrap();
    // The first replacement requests capacity eight; the next growth may copy
    // all eight old bytes in addition to its temporary and destination copies.
    let dto_copy_lower = 2 * (1 + 9) + 8;
    assert!(
        proof.usage.copied_bytes_upper
            >= proof.usage.error_requested_heap_bytes_upper + dto_copy_lower
    );
    let limits = DecodeProjectionLimits {
        max_copied_bytes: proof.usage.copied_bytes_upper - 1,
        ..limits()
    };
    assert!(matches!(
        model.preflight(&raw, limits, &Control::default()),
        Err(ResourceModelError::Limit(_))
    ));
}
