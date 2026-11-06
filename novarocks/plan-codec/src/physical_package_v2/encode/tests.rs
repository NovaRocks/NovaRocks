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
use crate::physical_package_v2::definition_sources::tests::{
    change_stream_packages, cv_package, repeated_change_stream_packages, rich_package,
    writer_constant_package, writer_package,
};
use crate::physical_package_v2::prepare_package_wire_in;
use crate::physical_package_v2::provider_sources::tests::checked_read;
use crate::physical_package_v2::test_support::encode_limits;
use crate::resource_preflight_v2::{DecodeProjectionLimits, FragmentDecodeResourceModel};
use std::sync::Mutex;

#[derive(Default)]
struct Control {
    stop: Option<(usize, CompileControlError)>,
    trace: Mutex<Vec<u32>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        trace.push(units);
        match self.stop.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}

fn fixtures() -> Vec<(&'static str, p::FragmentPackage)> {
    let mut fixtures = vec![
        ("rich", rich_package()),
        ("cv", cv_package()),
        ("writer", writer_constant_package()),
        ("data-read", checked_read(false)),
        ("metadata-read", checked_read(true)),
    ];
    for package in change_stream_packages() {
        let name = match package.fragment().id().get() {
            1 => "change-stream-writer",
            2 => "logical-change-finish",
            3 => "independent-router-summary",
            _ => panic!("the fixture must have exactly its three declared fragments"),
        };
        fixtures.push((name, package));
    }
    for package in repeated_change_stream_packages() {
        let name = match package.fragment().id().get() {
            1 => "repeated-change-stream-writer",
            2 => "repeated-logical-change-finish",
            3 => "repeated-independent-router-summary",
            _ => unreachable!(),
        };
        fixtures.push((name, package));
    }
    fixtures
}

fn decode_limits() -> DecodeProjectionLimits {
    DecodeProjectionLimits {
        max_input_bytes: 64 << 20,
        max_requested_heap_bytes: 512 << 20,
        max_message_occurrences: 1_000_000,
        max_scalar_elements: 1_000_000,
        max_field_occurrences: 1_000_000,
        max_copied_bytes: 512 << 20,
        max_initialization_bytes: 512 << 20,
        max_wire_depth: 100,
    }
}

#[test]
fn whole_sender_emits_every_component_of_actual_checked_packages() {
    for (name, package) in fixtures() {
        let out = encode_fragment_package(&package, &encode_limits(), &Control::default())
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let fragment = out.fragment.as_ref().expect("fragment");
        let original = package.fragment();
        assert_eq!(out.plan_version.len(), 16, "{name}");
        assert!(out.required.is_some(), "{name}");
        assert!(out.types.is_some(), "{name}");
        assert!(out.expression_control.is_some(), "{name}");
        assert!(out.calls.is_some(), "{name}");
        assert!(out.pruning.is_some(), "{name}");
        assert!(out.parameters.is_some(), "{name}");
        assert!(out.cuts.is_some(), "{name}");
        assert!(
            fragment.call_requests.is_some(),
            "{name}: present even when empty"
        );
        assert_eq!(out.result.is_some(), package.result().is_some(), "{name}");
        assert_eq!(fragment.id, original.id().get(), "{name}");
        assert_eq!(fragment.nodes.len(), original.nodes().len(), "{name}");
        assert_eq!(fragment.values.len(), original.values().len(), "{name}");
        assert_eq!(
            fragment.expressions.len(),
            original.expressions().len(),
            "{name}"
        );
        assert_eq!(
            fragment.call_requests.as_ref().unwrap().entries.len(),
            original.call_requests().entries().len(),
            "{name}"
        );
        assert_eq!(
            out.constants.len(),
            package.constants().entries().len(),
            "{name}"
        );
        assert_eq!(out.scans.len(), package.scans().len(), "{name}");
        assert_eq!(out.writes.len(), package.writes().len(), "{name}");
        assert_eq!(out.annotations.len(), package.annotations().len(), "{name}");
    }
}

#[test]
fn whole_sender_output_passes_generated_preflight_and_is_deterministic() {
    let model = FragmentDecodeResourceModel::try_new(&Control::default()).unwrap();
    for (name, package) in fixtures() {
        let first = encode_fragment_package(&package, &encode_limits(), &Control::default())
            .unwrap()
            .encode_to_vec();
        let second = encode_fragment_package(&package, &encode_limits(), &Control::default())
            .unwrap()
            .encode_to_vec();
        assert_eq!(first, second, "{name}: same package, same bytes");
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let decoded =
            prepare_package_wire_in(&first, &model, decode_limits(), &mut |_| Ok(()), &mut work)
                .and_then(|prepared| prepared.materialize_in(&mut |_| Ok(()), &mut work))
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        work.finish().unwrap();
        assert_eq!(decoded.encode_to_vec(), first, "{name}");
    }
}

#[test]
fn whole_sender_refuses_over_coarse_bound_component_limit_and_control() {
    for (_name, package) in fixtures() {
        let size = encode_fragment_package(&package, &encode_limits(), &Control::default())
            .unwrap()
            .encoded_len();
        let exact = PackageEncodeLimits {
            max_wire_bytes: size,
            ..encode_limits()
        };
        encode_fragment_package(&package, &exact, &Control::default()).unwrap();
        let under = PackageEncodeLimits {
            max_wire_bytes: size - 1,
            ..encode_limits()
        };
        assert!(matches!(
            encode_fragment_package(&package, &under, &Control::default()),
            Err(PackageEncodeError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        let mut tight = encode_limits();
        tight.definition_sources.max_values = package.fragment().values().len() - 1;
        assert!(matches!(
            encode_fragment_package(&package, &tight, &Control::default()),
            Err(PackageEncodeError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        // Representative control: the caller's cause stays primary at the entry,
        // in the middle and at the final footer of the one Encode scope.
        let control = Control::default();
        encode_fragment_package(&package, &encode_limits(), &control).unwrap();
        let callbacks = control.trace.lock().unwrap().len();
        for at in [0, callbacks / 2, callbacks - 1] {
            let stop = Control {
                stop: Some((at, CompileControlError::Cancelled)),
                ..Default::default()
            };
            assert!(matches!(
                encode_fragment_package(&package, &encode_limits(), &stop),
                Err(PackageEncodeError::Control(CompileControlError::Cancelled))
            ));
            assert_eq!(stop.trace.lock().unwrap().len(), at + 1);
        }
    }
}

// The checked writer fixture still carries a legacy Literal in its Values
// row. The v2 sender refuses it as an ordinary expression error: literals
// must be authored as constant pool references before publication.
#[test]
fn legacy_literal_package_is_an_ordinary_refusal_not_a_silent_conversion() {
    let package = writer_package();
    assert!(matches!(
        encode_fragment_package(&package, &encode_limits(), &Control::default()),
        Err(PackageEncodeError::Expression(_))
    ));
}

#[test]
fn original_sender_refusal_precedes_late_footer_control() {
    use novarocks_failure_contract::FailureProjection;
    let package = writer_package();
    let original_control = Control::default();
    let mut work = CompileCheckpoints::try_new(&original_control, CompilePhase::Encode).unwrap();
    let original = encode_fragment_package_in(&package, &encode_limits(), &mut work).unwrap_err();
    assert!(matches!(original, PackageEncodeError::Expression(_)));
    let original_trace = original_control.trace.lock().unwrap().clone();
    assert!(!original_trace.is_empty());
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let stop = Control {
            stop: Some((original_trace.len(), cause)),
            ..Default::default()
        };
        let refused = encode_fragment_package(&package, &encode_limits(), &stop).unwrap_err();
        assert!(matches!(refused, PackageEncodeError::Expression(_)));
        assert_eq!(refused.to_string(), original.to_string());
        assert_eq!(refused.failure_fact(), original.failure_fact());
        assert_eq!(*stop.trace.lock().unwrap(), original_trace);
        assert!(std::error::Error::source(&refused).is_some());
    }
}

#[test]
fn whole_package_preserves_declared_keyed_assertion_violation() {
    use crate::physical_package_v2::{decode_fragment_package, test_support};
    use novarocks_failure_contract::sql_reason;
    let control = Control::default();
    let model = FragmentDecodeResourceModel::try_new(&control).unwrap();
    for reason in [
        sql_reason::MERGE_TARGET_ROW_MULTIPLE_MATCHES,
        sql_reason::UPDATE_TARGET_ROW_MULTIPLE_MATCHES,
    ] {
        let mut wire = encode_fragment_package(&cv_package(), &encode_limits(), &control).unwrap();
        let fragment = wire.fragment.as_mut().unwrap();
        let source = fragment.root_node_id.unwrap();
        let mut assertion = fragment
            .nodes
            .iter()
            .find(|node| node.id == source)
            .unwrap()
            .clone();
        let id = u32::MAX - 1;
        assertion.id = id;
        assertion.input_node_ids = vec![source];
        assertion.required_inputs = vec![assertion.output_properties.clone().unwrap()];
        assertion.output.as_mut().unwrap().node_id = Some(id);
        assertion.kind = Some(wire::physical_node::Kind::AssertOneRow(
            wire::RowCountAssertionNode {
                kind: Some(wire::row_count_assertion_node::Kind::PerKeyAtMostOne(
                    wire::PerKeyRowCountAssertion {
                        key_value_ids: assertion.output.as_ref().unwrap().value_ids.clone(),
                        labels: vec!["file".into(), "position".into()],
                        message: "declared target uniqueness".into(),
                        applicability: Some(wire::AssertionApplicability {
                            kind: Some(wire::assertion_applicability::Kind::AllRows(
                                novarocks_proto_models::physical_control_v2::Empty {},
                            )),
                        }),
                        declared_sql_violation: Some(u32::from(reason.code())),
                    },
                )),
            },
        ));
        fragment.nodes.push(assertion);
        fragment.root_node_id = Some(id);
        wire.result
            .as_mut()
            .unwrap()
            .output
            .as_mut()
            .unwrap()
            .node_id = Some(id);
        let decoded = decode_fragment_package(
            &wire.encode_to_vec(),
            &model,
            &test_support::decode_limits(),
            &control,
        )
        .unwrap();
        let node = &decoded.fragment().nodes()[&p::NodeId::new(id)];
        assert!(
            matches!(&node.kind, p::NodeKind::AssertOneRow(p::RowCountAssertionSpec::PerKeyAtMostOne { declared_violation: Some(actual), .. }) if *actual == reason)
        );
        let encoded = encode_fragment_package(&decoded, &encode_limits(), &control).unwrap();
        let roundtrip = decode_fragment_package(
            &encoded.encode_to_vec(),
            &model,
            &test_support::decode_limits(),
            &control,
        )
        .unwrap();
        assert_eq!(roundtrip.fragment().nodes()[&p::NodeId::new(id)], *node);
    }
}
