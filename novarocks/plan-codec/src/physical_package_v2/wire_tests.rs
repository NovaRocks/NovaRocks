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
use novarocks_type_contract::CompilePhase;
use prost::encoding::{WireType, encode_key, encode_varint};
use std::sync::Mutex;

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    fail: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let ordinal = trace.len();
        trace.push((phase, units));
        if let Some((target, cause)) = self.fail.clone()
            && ordinal == target
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
    // Generated schema construction is a separate host owner, not part of the
    // Decode entry whose exact trace these tests inspect.
    FragmentDecodeResourceModel::try_new(&Control::default()).unwrap()
}
fn length_delimited(tag: u32, bytes: &[u8]) -> Vec<u8> {
    let mut raw = Vec::new();
    encode_key(tag, WireType::LengthDelimited, &mut raw);
    encode_varint(bytes.len() as u64, &mut raw);
    raw.extend_from_slice(bytes);
    raw
}
fn causes() -> [CompileControlError; 3] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ]
}
fn decode(
    raw: &[u8],
    model: &FragmentDecodeResourceModel,
    control: &Control,
    snapshots: &mut Vec<DecodeResourceUsage>,
) -> Result<wire::FragmentPackage, PackageWireError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let mut admit = |facts: &DecodeResourceUsage| {
        snapshots.push(*facts);
        Ok(())
    };
    let result = prepare_package_wire_in(raw, model, limits(), &mut admit, &mut work)
        .and_then(|prepared| prepared.materialize_in(&mut admit, &mut work));
    if matches!(&result, Err(PackageWireError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[test]
fn generated_dto_presence_survives_byte_admission_without_semantic_publication() {
    let model = model();
    let source = wire::FragmentPackage {
        plan_version: vec![7; 16],
        required: Some(wire::RequiredContracts {
            plan_contract_revision: 2,
        }),
        fragment: Some(wire::Fragment {
            id: u32::MAX,
            root_node_id: Some(0),
            call_requests: Some(Default::default()),
            ..Default::default()
        }),
        types: Some(Default::default()),
        source_constant_policy: Some(Default::default()),
        calls: Some(Default::default()),
        pruning: Some(Default::default()),
        parameters: Some(Default::default()),
        cuts: Some(Default::default()),
        layouts: vec![wire::FrozenWriteLayoutRecipe {
            node_id: Some(0),
            provider_binding_id: Some(7),
            recipe_payload_id: Some(u32::MAX),
            input_field_tokens: [9_u8; 32].to_vec(),
        }],
        result: Some(Default::default()),
        ..Default::default()
    };
    let raw = source.encode_to_vec();
    let mut snapshots = Vec::new();
    let decoded = decode(&raw, &model, &Control::default(), &mut snapshots).unwrap();
    assert_eq!(decoded, source);
    assert_eq!(
        decoded,
        wire::FragmentPackage::decode(raw.as_slice()).unwrap()
    );
    assert!(snapshots.len() > 1);
    assert_eq!(snapshots.last(), snapshots.get(snapshots.len() - 2));
    assert!(snapshots.iter().all(|facts| facts.input_bytes == raw.len()));
    // An empty protobuf is a valid untrusted DTO, but cannot satisfy the
    // required semantic tables. No checked PhysicalPlan package is returned.
    let empty = decode(&[], &model, &Control::default(), &mut Vec::new()).unwrap();
    assert!(empty.fragment.is_none());
    assert!(empty.required.is_none());
    assert!(empty.calls.is_none());
}

#[test]
fn original_prost_unknown_groups_noncanonical_and_overwritten_fields_remain_authoritative() {
    let model = model();
    let mut raw = length_delimited(1, &[3; 16]);
    raw.extend(length_delimited(1, &[9; 16]));
    // Real RequiredContracts revision with a legal noncanonical varint.
    raw.extend(length_delimited(2, &[0x08, 0x82, 0x00]));
    encode_key(9900, WireType::StartGroup, &mut raw);
    encode_key(9901, WireType::Varint, &mut raw);
    encode_varint(u64::MAX, &mut raw);
    encode_key(9900, WireType::EndGroup, &mut raw);
    let mut snapshots = Vec::new();
    let actual = decode(&raw, &model, &Control::default(), &mut snapshots).unwrap();
    assert_eq!(
        actual,
        wire::FragmentPackage::decode(raw.as_slice()).unwrap()
    );
    assert_eq!(actual.plan_version, vec![9; 16]);
    assert_eq!(actual.required.unwrap().plan_contract_revision, 2);
    let projection = model
        .preflight(&raw, limits(), &Control::default())
        .unwrap();
    assert_eq!(snapshots.last(), Some(&projection.usage));
    assert!(projection.usage.scalar_elements >= 3);
    assert!(projection.usage.copied_bytes_upper >= 2 * 3 * 16);
}

#[test]
fn malformed_prefix_uses_original_library_error_and_caller_ordinary_tail() {
    let model = model();
    let raw = [0x0a, 0x04, 1];
    let original = wire::FragmentPackage::decode(raw.as_slice()).unwrap_err();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let prepared =
        prepare_package_wire_in(&raw, &model, limits(), &mut |_| Ok(()), &mut work).unwrap();
    assert_eq!(
        prepared.projection().status,
        ResourceCursorStatus::MalformedPrefix
    );
    let error = prepared
        .materialize_in(&mut |_| Ok(()), &mut work)
        .unwrap_err();
    let PackageWireError::Protobuf(error) = error else {
        panic!("malformed bytes must retain the original Prost error");
    };
    assert_eq!(error.to_string(), original.to_string());
    let before = control.trace();
    work.finish().unwrap();
    assert_eq!(control.trace().len(), before.len() + 1);
    assert_eq!(control.trace().last(), Some(&(CompilePhase::Decode, 0)));
}

#[test]
fn final_parent_refusal_precedes_progress_and_library_materialization() {
    let model = model();
    let raw = length_delimited(1, &[7; 16]);
    let baseline = Control::default();
    let mut baseline_work = CompileCheckpoints::try_new(&baseline, CompilePhase::Decode).unwrap();
    prepare_package_wire_in(&raw, &model, limits(), &mut |_| Ok(()), &mut baseline_work).unwrap();
    let before = baseline.trace();
    for late_cause in causes() {
        let control = Control {
            fail: Some((before.len(), late_cause)),
            ..Default::default()
        };
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let prepared =
            prepare_package_wire_in(&raw, &model, limits(), &mut |_| Ok(()), &mut work).unwrap();
        let expected = prepared.projection().usage;
        let mut calls = 0;
        let result = prepared.materialize_in(
            &mut |facts| {
                calls += 1;
                assert_eq!(*facts, expected);
                Err(CompileControlError::ResourceExhausted)
            },
            &mut work,
        );
        assert!(matches!(
            result,
            Err(PackageWireError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(calls, 1);
        assert_eq!(control.trace(), before);
    }
}

#[test]
fn prepared_bytes_cannot_be_materialized_under_another_actual_controller() {
    let model = model();
    let original = Control::default();
    let other = Control::default();
    let raw = length_delimited(1, &[1; 16]);
    let mut original_work = CompileCheckpoints::try_new(&original, CompilePhase::Decode).unwrap();
    let prepared =
        prepare_package_wire_in(&raw, &model, limits(), &mut |_| Ok(()), &mut original_work)
            .unwrap();
    let before = original.trace();
    let mut other_work = CompileCheckpoints::try_new(&other, CompilePhase::Decode).unwrap();
    let mut calls = 0;
    let result = prepared.materialize_in(
        &mut |_| {
            calls += 1;
            Ok(())
        },
        &mut other_work,
    );
    assert!(matches!(result, Err(PackageWireError::InvalidSource(_))));
    assert_eq!(calls, 0);
    assert_eq!(original.trace(), before);
    assert_eq!(other.trace(), vec![(CompilePhase::Decode, 0)]);
}

#[test]
fn every_actual_decode_callback_preserves_control_identity_without_late_footer() {
    let model = model();
    for raw in [length_delimited(1, &[6; 16]), vec![0x0a, 0x08, 1]] {
        let baseline = Control::default();
        let result = decode(&raw, &model, &baseline, &mut Vec::new());
        assert!(result.is_ok() || matches!(result, Err(PackageWireError::Protobuf(_))));
        let trace = baseline.trace();
        assert!(trace.len() >= 3);
        assert!(
            trace
                .iter()
                .all(|(phase, _)| *phase == CompilePhase::Decode)
        );
        for at in 0..trace.len() {
            for cause in causes() {
                let control = Control {
                    fail: Some((at, cause.clone())),
                    ..Default::default()
                };
                assert!(
                    matches!(decode(&raw, &model, &control, &mut Vec::new()), Err(PackageWireError::Control(actual)) if actual == cause)
                );
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
}
