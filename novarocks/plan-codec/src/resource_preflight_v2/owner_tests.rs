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
use std::{mem::size_of, sync::Mutex};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    fail: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        trace.push((phase, units));
        if let Some((target, cause)) = self.fail.clone()
            && at == target
        {
            return Err(cause);
        }
        Ok(())
    }
}
impl Control {
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
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
fn model() -> FragmentDecodeResourceModel {
    FragmentDecodeResourceModel::try_new(&Control::default()).unwrap()
}
fn ld(tag: u32, bytes: &[u8]) -> Vec<u8> {
    let mut raw = Vec::new();
    encode_key(tag, WireType::LengthDelimited, &mut raw);
    encode_varint(bytes.len() as u64, &mut raw);
    raw.extend_from_slice(bytes);
    raw
}
fn varint(tag: u32, value: u64) -> Vec<u8> {
    let mut raw = Vec::new();
    encode_key(tag, WireType::Varint, &mut raw);
    encode_varint(value, &mut raw);
    raw
}
fn causes() -> [CompileControlError; 3] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ]
}
fn complete(
    model: &FragmentDecodeResourceModel,
    raw: &[u8],
    limits: DecodeProjectionLimits,
    control: &Control,
    snapshots: &mut Vec<DecodeResourceUsage>,
) -> Result<DecodeResourceProjection, ResourceModelError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = model.preflight_in(
        raw,
        limits,
        &mut |facts| {
            snapshots.push(*facts);
            Ok(())
        },
        &mut work,
    );
    finish(work, result)
}

#[test]
fn caller_projection_preserves_original_raw_occurrences_and_plain_trace() {
    let model = model();
    // Package.required uses the real generated message. Its revision varint
    // deliberately keeps a lawful noncanonical representation.
    let noncanonical = ld(2, &[0x08, 0x81, 0x00]);
    let mut duplicate = ld(1, b"a");
    duplicate.extend(ld(1, b"bc"));
    let mut unknown_group = Vec::new();
    encode_key(9900, WireType::StartGroup, &mut unknown_group);
    unknown_group.extend(varint(9901, u64::MAX));
    encode_key(9900, WireType::EndGroup, &mut unknown_group);
    let malformed = [0x0a, 0x04, 1];
    for raw in [
        &[][..],
        &noncanonical,
        &duplicate,
        &unknown_group,
        &malformed,
    ] {
        let plain = Control::default();
        let caller = Control::default();
        let original = model.preflight(raw, limits(), &plain).unwrap();
        let mut snapshots = Vec::new();
        let projected = complete(&model, raw, limits(), &caller, &mut snapshots).unwrap();
        assert_eq!(projected, original);
        assert_eq!(caller.trace(), plain.trace());
        assert!(snapshots.iter().all(|f| f.input_bytes == raw.len()));
        assert_eq!(
            projected.usage.root_inline_bytes,
            size_of::<novarocks_proto_models::physical_package_v2::FragmentPackage>()
        );
    }
}

#[test]
fn original_bytes_request_and_copy_geometry_have_independent_delta_oracle() {
    let model = model();
    let empty = complete(&model, &[], limits(), &Control::default(), &mut Vec::new())
        .unwrap()
        .usage;
    let raw = ld(1, &[7; 16]);
    let bytes = complete(&model, &raw, limits(), &Control::default(), &mut Vec::new())
        .unwrap()
        .usage;
    // Actual bytes::merge temporary16 plus original destination growth8+4*16.
    assert_eq!(
        bytes.cumulative_requested_heap_bytes_upper - empty.cumulative_requested_heap_bytes_upper,
        16 + 8 + 4 * 16
    );
    assert_eq!(bytes.copied_bytes_upper - empty.copied_bytes_upper, 3 * 16);
    assert_eq!(
        bytes.initialization_bytes_upper - empty.initialization_bytes_upper,
        size_of::<novarocks_proto_models::physical_package_v2::FragmentPackage>()
            + size_of::<Vec<u8>>()
    );
    assert_eq!(bytes.scalar_elements, 1);
    assert_eq!(bytes.message_occurrences, 1);
    assert_eq!(bytes.field_occurrences, 1);
    assert_eq!(
        bytes.error_requested_heap_bytes_upper,
        empty.error_requested_heap_bytes_upper
    );
}

#[test]
fn caller_owns_entry_and_tail_and_every_real_small_callback_preserves_first_cause() {
    let model = model();
    for raw in [ld(1, b"real payload"), vec![0x0a, 0x08, 1]] {
        let baseline = Control::default();
        let mut work = CompileCheckpoints::try_new(&baseline, CompilePhase::Decode).unwrap();
        let projection = model
            .preflight_in(&raw, limits(), &mut |_| Ok(()), &mut work)
            .unwrap();
        assert_eq!(baseline.trace(), vec![(CompilePhase::Decode, 0)]);
        assert!(projection.usage.field_occurrences > 0);
        work.finish().unwrap();
        let trace = baseline.trace();
        for at in 0..trace.len() {
            for cause in causes() {
                let control = Control {
                    fail: Some((at, cause.clone())),
                    ..Default::default()
                };
                assert_eq!(
                    complete(&model, &raw, limits(), &control, &mut Vec::new()),
                    Err(ResourceModelError::Control(cause))
                );
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut bad = limits();
    bad.max_wire_depth = 101;
    assert!(matches!(
        model.preflight_in(&[], bad, &mut |_| Ok(()), &mut work),
        Err(ResourceModelError::Schema(_))
    ));
    assert_eq!(control.trace(), vec![(CompilePhase::Decode, 0)]);
    work.finish().unwrap();
    assert_eq!(
        control.trace(),
        vec![(CompilePhase::Decode, 0), (CompilePhase::Decode, 0)]
    );
    for at in 0..2 {
        for cause in causes() {
            let refused = Control {
                fail: Some((at, cause.clone())),
                ..Default::default()
            };
            assert_eq!(
                complete(&model, &[], bad, &refused, &mut Vec::new()),
                Err(ResourceModelError::Control(cause))
            );
            assert_eq!(refused.trace(), control.trace()[..=at]);
        }
    }
}

#[test]
fn all_original_numeric_axes_are_primary_before_late_control_and_parent_refusal() {
    let model = model();
    let raw = ld(2, &varint(1, 9));
    let usage = complete(&model, &raw, limits(), &Control::default(), &mut Vec::new())
        .unwrap()
        .usage;
    for axis in 0..8 {
        let mut bound = limits();
        match axis {
            0 => bound.max_input_bytes = raw.len() - 1,
            1 => bound.max_requested_heap_bytes = usage.cumulative_requested_heap_bytes_upper - 1,
            2 => bound.max_message_occurrences = usage.message_occurrences - 1,
            3 => bound.max_scalar_elements = usage.scalar_elements - 1,
            4 => bound.max_field_occurrences = usage.field_occurrences - 1,
            5 => bound.max_copied_bytes = usage.copied_bytes_upper - 1,
            6 => bound.max_initialization_bytes = usage.initialization_bytes_upper - 1,
            _ => bound.max_wire_depth = 0,
        }
        assert!(matches!(
            model.preflight(&raw, bound, &Control::default()),
            Err(ResourceModelError::Limit(_))
        ));
        for cause in causes() {
            let control = Control {
                fail: Some((1, cause)),
                ..Default::default()
            };
            assert_eq!(
                complete(&model, &raw, bound, &control, &mut Vec::new()),
                Err(ResourceModelError::Control(
                    CompileControlError::ResourceExhausted
                ))
            );
            assert_eq!(control.trace(), vec![(CompilePhase::Decode, 0)]);
        }
    }
    for cause in causes() {
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut calls = 0;
        assert_eq!(
            model.preflight_in(
                &raw,
                limits(),
                &mut |_| {
                    calls += 1;
                    Err(cause.clone())
                },
                &mut work
            ),
            Err(ResourceModelError::Control(cause))
        );
        assert_eq!(calls, 1);
        assert_eq!(control.trace(), vec![(CompilePhase::Decode, 0)]);
    }
}

#[test]
fn captured_payload_known_request_refuses_before_real_fixed_progress_at_256() {
    let model = model();
    let mut raw = Vec::new();
    // Each real unknown-varint occurrence uses loop/key/scalar three original
    // steps. 84 such fields plus payload loop/key/length leave pending255.
    for _ in 0..84 {
        raw.extend(varint(9900, 1));
    }
    raw.extend(ld(1, &[5; 16]));
    let baseline = Control::default();
    let usage = complete(&model, &raw, limits(), &baseline, &mut Vec::new())
        .unwrap()
        .usage;
    assert_eq!(
        baseline.trace(),
        vec![
            (CompilePhase::Decode, 0),
            (CompilePhase::Decode, 256),
            (CompilePhase::Decode, 1)
        ]
    );
    let bound = DecodeProjectionLimits {
        max_requested_heap_bytes: usage.cumulative_requested_heap_bytes_upper - 1,
        ..limits()
    };
    for cause in causes() {
        let control = Control {
            fail: Some((1, cause)),
            ..Default::default()
        };
        assert_eq!(
            complete(&model, &raw, bound, &control, &mut Vec::new()),
            Err(ResourceModelError::Control(
                CompileControlError::ResourceExhausted
            ))
        );
        assert_eq!(control.trace(), vec![(CompilePhase::Decode, 0)]);
    }
}

#[test]
fn real_320_message_occurrences_keep_monotonic_snapshots_and_actual_quantum() {
    let model = model();
    let mut table = Vec::new();
    for _ in 0..320 {
        table.extend(ld(1, &[]));
    }
    let raw = ld(4, &table);
    let baseline = Control::default();
    let mut snapshots = Vec::new();
    let projected = complete(&model, &raw, limits(), &baseline, &mut snapshots).unwrap();
    assert_eq!(projected.usage.message_occurrences, 322);
    assert_eq!(projected.usage.field_occurrences, 321);
    assert_eq!(snapshots.last(), Some(&projected.usage));
    for pair in snapshots.windows(2) {
        assert!(pair[0].message_occurrences <= pair[1].message_occurrences);
        assert!(
            pair[0].cumulative_requested_heap_bytes_upper
                <= pair[1].cumulative_requested_heap_bytes_upper
        );
        assert!(pair[0].initialization_bytes_upper <= pair[1].initialization_bytes_upper);
    }
    let trace = baseline.trace();
    assert!(trace.iter().any(|(_, n)| *n == 256));
    for at in [0, 1, trace.len() - 1] {
        for cause in causes() {
            let control = Control {
                fail: Some((at, cause.clone())),
                ..Default::default()
            };
            assert_eq!(
                complete(&model, &raw, limits(), &control, &mut Vec::new()),
                Err(ResourceModelError::Control(cause))
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn captured_unknown_group_depth_is_numeric_before_parent_snapshot_and_progress() {
    let model = model();
    let mut raw = Vec::new();
    encode_key(9900, WireType::StartGroup, &mut raw);
    encode_key(9900, WireType::EndGroup, &mut raw);
    let bound = DecodeProjectionLimits {
        max_wire_depth: 0,
        ..limits()
    };
    for cause in causes() {
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut observed = Vec::new();
        assert_eq!(
            model.preflight_in(
                &raw,
                bound,
                &mut |facts| {
                    observed.push(*facts);
                    if facts.field_occurrences > 0 {
                        Err(cause.clone())
                    } else {
                        Ok(())
                    }
                },
                &mut work
            ),
            Err(ResourceModelError::Control(
                CompileControlError::ResourceExhausted
            ))
        );
        assert_eq!(observed.len(), 1);
        assert_eq!(control.trace(), vec![(CompilePhase::Decode, 0)]);
    }
}

#[test]
fn captured_enclosing_initialization_admits_before_real_length_progress_at_256() {
    let model = model();
    let mut raw = Vec::new();
    // Real unknown varints249 + empty Required message4 + payload loop/key2
    // leave pending255 at the actual captured enclosing-init snapshot.
    for _ in 0..83 {
        raw.extend(varint(9900, 1));
    }
    raw.extend(ld(2, &[]));
    raw.extend(ld(1, &[3; 16]));
    let baseline = Control::default();
    let mut work = CompileCheckpoints::try_new(&baseline, CompilePhase::Decode).unwrap();
    let mut enclosing = None;
    let result = model.preflight_in(
        &raw,
        limits(),
        &mut |facts| {
            if facts.field_occurrences == 85 && facts.scalar_elements == 0 {
                assert_eq!(baseline.trace(), vec![(CompilePhase::Decode, 0)]);
                enclosing = Some(*facts);
            }
            Ok(())
        },
        &mut work,
    );
    let projected = finish(work, result).unwrap();
    let enclosing = enclosing.expect("real enclosing initializer before length read");
    assert_eq!(enclosing.message_occurrences, 2);
    assert!(enclosing.initialization_bytes_upper < projected.usage.initialization_bytes_upper);
    assert_eq!(baseline.trace()[1], (CompilePhase::Decode, 256));
    let maximum = enclosing.initialization_bytes_upper - 1;
    for cause in causes() {
        let control = Control {
            fail: Some((1, cause)),
            ..Default::default()
        };
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut last = DecodeResourceUsage::default();
        let result = model.preflight_in(
            &raw,
            limits(),
            &mut |facts| {
                last = *facts;
                if facts.initialization_bytes_upper > maximum {
                    Err(CompileControlError::ResourceExhausted)
                } else {
                    Ok(())
                }
            },
            &mut work,
        );
        assert_eq!(
            result,
            Err(ResourceModelError::Control(
                CompileControlError::ResourceExhausted
            ))
        );
        assert_eq!(last, enclosing);
        assert_eq!(control.trace(), vec![(CompilePhase::Decode, 0)]);
    }
}

#[test]
fn typed_writer_references_charge_each_known_raw_scalar_and_optional_wrapper() {
    use novarocks_proto_models::physical_package_v2 as wire;
    let model = model();
    // Independent actual wire paths from FragmentPackage to each inline
    // reference owner. IDs are sparse references, never allocation lengths.
    for (path, tag, inline) in [
        (
            &[20, 4, 1, 1][..],
            4,
            size_of::<wire::ConnectorWriteFieldBinding>(),
        ),
        (
            &[3, 5, 23, 1, 3][..],
            3,
            size_of::<wire::ChangeAssignment>(),
        ),
        (
            &[3, 5, 23, 1, 3][..],
            4,
            size_of::<wire::ChangeAssignment>(),
        ),
        (&[3, 5, 25, 5][..], 6, size_of::<wire::WriterTargetField>()),
        (&[3, 5, 25, 5][..], 7, size_of::<wire::WriterTargetField>()),
    ] {
        let wrap = |mut payload: Vec<u8>| {
            for &tag in path.iter().rev() {
                payload = ld(tag, &payload);
            }
            payload
        };
        let empty = model
            .preflight(&wrap(Vec::new()), limits(), &Control::default())
            .unwrap()
            .usage;
        let mut payload = Vec::new();
        for id in [0, u32::MAX, 0] {
            payload.extend(varint(tag, u64::from(id)));
        }
        let raw = wrap(payload.clone());
        let full = model
            .preflight(&raw, limits(), &Control::default())
            .unwrap();
        assert_eq!(full.status, ResourceCursorStatus::Complete);
        assert_eq!(full.usage.message_occurrences, path.len() + 1);
        assert_eq!(full.usage.scalar_elements, 3);
        assert_eq!(full.usage.field_occurrences, path.len() + 3);
        assert_eq!(
            full.usage.initialization_bytes_upper - empty.initialization_bytes_upper,
            3 * (inline + size_of::<Option<u32>>())
        );
        for axis in 0..3 {
            let mut bound = limits();
            match axis {
                0 => bound.max_scalar_elements = 2,
                1 => bound.max_field_occurrences = path.len() + 2,
                _ => bound.max_initialization_bytes = full.usage.initialization_bytes_upper - 1,
            }
            for cause in causes() {
                let control = Control {
                    fail: Some((1, cause)),
                    ..Default::default()
                };
                assert_eq!(
                    complete(&model, &raw, bound, &control, &mut Vec::new()),
                    Err(ResourceModelError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                );
                assert_eq!(control.trace(), vec![(CompilePhase::Decode, 0)]);
            }
        }
        // A recognized scalar's original charges survive its malformed value.
        encode_key(tag, WireType::Varint, &mut payload);
        let prefix = model
            .preflight(&wrap(payload), limits(), &Control::default())
            .unwrap();
        assert_eq!(prefix.status, ResourceCursorStatus::MalformedPrefix);
        assert_eq!(prefix.usage.scalar_elements, 4);
        assert_eq!(prefix.usage.field_occurrences, path.len() + 4);
    }
}
