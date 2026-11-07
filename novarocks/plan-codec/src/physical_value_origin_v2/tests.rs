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
use crate::physical_connector_payload_v2::{
    ConnectorPayloadProjectionLimits, decode_connector_payloads, encode_connector_payloads,
};
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceId, ConnectorProviderId,
};
use novarocks_type_contract::PureCompileControl;
use std::sync::Mutex;
const NAMESPACE_SOURCE: usize = 1024 * 1024;
const SOURCE: usize = 2 * 1024 * 1024;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after refusal");
        }
        trace.push((phase, units));
        match self.stop.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn trace(control: &Control) -> Vec<(CompilePhase, u32)> {
    control.trace.lock().unwrap().clone()
}
fn ns_limits() -> ConnectorPayloadProjectionLimits {
    ConnectorPayloadProjectionLimits {
        max_definitions: 1024,
        max_payload_bytes: 1024 * 1024,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 4 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 8 * 1024 * 1024,
        max_work: 64 * 1024 * 1024,
    }
}
fn limits() -> ValueOriginProjectionLimits {
    ValueOriginProjectionLimits {
        max_allocation_requests: 1,
        max_allocation_request_bytes: 128,
        max_coexisting_source_and_request_bytes: 4 * 1024 * 1024,
        max_work: 1024 * 1024,
    }
}
fn payload(category: ConnectorCodecCategory, bytes: &[u8]) -> ConnectorEncodedPayload {
    ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            CatalogHandle::new(
                ConnectorInstanceId::try_from_canonical("catalog_a").unwrap(),
                CatalogVersion::from_bytes([3; 32]),
            ),
            category,
            ConnectorCodecRevision::try_new(7).unwrap(),
        ),
        bytes.to_vec().into(),
    )
}
fn provider(category: ConnectorCodecCategory, bytes: &[u8]) -> physical::ValueOrigin {
    physical::ValueOrigin::ProviderField {
        scan_node: physical::NodeId::new(0),
        field: physical::ProviderColumnReference {
            column_payload: payload(category, bytes),
        },
    }
}
fn original(origin: &physical::ValueOrigin) -> &ConnectorEncodedPayload {
    match origin {
        physical::ValueOrigin::ProviderField { field, .. } => &field.column_payload,
        _ => panic!("fixture is not ProviderField"),
    }
}
fn wrap(kind: wire::value_origin::Kind) -> wire::ValueOrigin {
    wire::ValueOrigin { kind: Some(kind) }
}
fn nonprovider_cases() -> Vec<(physical::ValueOrigin, wire::ValueOrigin)> {
    vec![
        (
            physical::ValueOrigin::JoinMark {
                node: physical::NodeId::new(19),
            },
            wrap(wire::value_origin::Kind::JoinMarkNodeId(19)),
        ),
        (
            physical::ValueOrigin::Summary {
                field: physical::SummaryField::Inserted,
            },
            wrap(wire::value_origin::Kind::Summary(wire::SummaryField {
                kind: 1,
                ordinal: Some(0),
            })),
        ),
        (
            physical::ValueOrigin::Summary {
                field: physical::SummaryField::Replaced,
            },
            wrap(wire::value_origin::Kind::Summary(wire::SummaryField {
                kind: 1,
                ordinal: Some(1),
            })),
        ),
        (
            physical::ValueOrigin::Summary {
                field: physical::SummaryField::Deleted,
            },
            wrap(wire::value_origin::Kind::Summary(wire::SummaryField {
                kind: 1,
                ordinal: Some(2),
            })),
        ),
        (
            physical::ValueOrigin::Summary {
                field: physical::SummaryField::Requested,
            },
            wrap(wire::value_origin::Kind::Summary(wire::SummaryField {
                kind: 2,
                ordinal: Some(0),
            })),
        ),
        (
            physical::ValueOrigin::Summary {
                field: physical::SummaryField::Matched,
            },
            wrap(wire::value_origin::Kind::Summary(wire::SummaryField {
                kind: 2,
                ordinal: Some(1),
            })),
        ),
        (
            physical::ValueOrigin::Summary {
                field: physical::SummaryField::InsufficientKeys,
            },
            wrap(wire::value_origin::Kind::Summary(wire::SummaryField {
                kind: 2,
                ordinal: Some(2),
            })),
        ),
        (
            physical::ValueOrigin::Summary {
                field: physical::SummaryField::InsufficientFingerprint,
            },
            wrap(wire::value_origin::Kind::Summary(wire::SummaryField {
                kind: 2,
                ordinal: Some(3),
            })),
        ),
        (
            physical::ValueOrigin::Expr {
                node: physical::NodeId::new(u32::MAX),
                expr: physical::ExprId::new(0),
            },
            wrap(wire::value_origin::Kind::Expression(
                wire::ExpressionOrigin {
                    node_id: Some(u32::MAX),
                    expr_id: Some(0),
                },
            )),
        ),
        (
            physical::ValueOrigin::NullExtended {
                node: physical::NodeId::new(0),
                of: physical::ValueId::new(u32::MAX),
            },
            wrap(wire::value_origin::Kind::NullExtended(
                wire::NullExtendedOrigin {
                    node_id: Some(0),
                    original_value_id: Some(u32::MAX),
                },
            )),
        ),
        (
            physical::ValueOrigin::AggregateState {
                call: physical::AggregateCallId::new(0),
                phase: physical::AggregatePhase::Partial {
                    sequence: physical::AggregateSequenceId::new(u32::MAX),
                },
            },
            wrap(wire::value_origin::Kind::AggregateState(
                wire::AggregateStateOrigin {
                    call_id: Some(0),
                    phase: Some(wire::AggregatePhase {
                        kind: Some(wire::aggregate_phase::Kind::PartialSequenceId(u32::MAX)),
                    }),
                },
            )),
        ),
        (
            physical::ValueOrigin::AggregateResult {
                call: physical::AggregateCallId::new(u32::MAX),
            },
            wrap(wire::value_origin::Kind::AggregateResultCallId(u32::MAX)),
        ),
        (
            physical::ValueOrigin::NodeOutput {
                node: physical::NodeId::new(0),
                output_ordinal: u32::MAX,
            },
            wrap(wire::value_origin::Kind::NodeOutput(
                wire::NodeOutputOrigin {
                    node_id: Some(0),
                    output_ordinal: u32::MAX,
                },
            )),
        ),
        (
            physical::ValueOrigin::ExchangeImport {
                edge: physical::EdgeId::new(u32::MAX),
                source_value: physical::ValueId::new(0),
            },
            wrap(wire::value_origin::Kind::ExchangeImport(
                wire::ExchangeImportOrigin {
                    edge_id: Some(u32::MAX),
                    source_value_id: Some(0),
                },
            )),
        ),
        (
            physical::ValueOrigin::CteImport {
                edge: physical::EdgeId::new(0),
                producer_fragment: physical::FragmentId::new(u32::MAX),
                producer_value: physical::ValueId::new(0),
            },
            wrap(wire::value_origin::Kind::CteImport(wire::CteImportOrigin {
                edge_id: Some(0),
                producer_fragment_id: Some(u32::MAX),
                producer_value_id: Some(0),
            })),
        ),
        (
            physical::ValueOrigin::WriterDerived {
                writer_node: physical::NodeId::new(u32::MAX),
                kind: physical::WriterDerivedKind::CommitFragment,
            },
            wrap(wire::value_origin::Kind::WriterDerived(
                wire::WriterDerivedOrigin {
                    writer_node_id: Some(u32::MAX),
                    kind: 3,
                },
            )),
        ),
    ]
}
#[test]
fn value_origin_complete_nine_variants_have_independent_wire_and_typed_receiving_oracles() {
    let control = Control::default();
    let empty = encode_connector_payloads(&[], 0, ns_limits(), &control).unwrap();
    let received_empty = decode_connector_payloads(&[], 0, ns_limits(), &control).unwrap();
    for (source, expected) in nonprovider_cases() {
        let (wire, encoded_facts) = encode_value_origin(&source, &empty, SOURCE, limits()).unwrap();
        assert_eq!(wire, expected);
        let (decoded, decoded_facts) =
            decode_value_origin(&expected, &received_empty, SOURCE, limits()).unwrap();
        assert_eq!(decoded, source);
        assert_eq!(encoded_facts.reference_count, decoded_facts.reference_count);
        assert_eq!(encoded_facts.allocation_requests_upper_bound, 0);
        assert_eq!(decoded_facts.allocation_requests_upper_bound, 0);
    }
    let source = provider(ConnectorCodecCategory::ReadColumn, &[255, 0, 17]);
    let inputs = [(u32::MAX, original(&source))];
    let namespace =
        encode_connector_payloads(&inputs, NAMESPACE_SOURCE, ns_limits(), &control).unwrap();
    let expected = wrap(wire::value_origin::Kind::ProviderField(
        wire::ProviderFieldOrigin {
            scan_node_id: Some(0),
            column_payload_id: Some(u32::MAX),
        },
    ));
    let (wire, facts) = encode_value_origin(&source, &namespace, SOURCE, limits()).unwrap();
    assert_eq!(wire, expected);
    assert_eq!(facts.reference_count, 2);
    let received =
        decode_connector_payloads(namespace.as_wire(), NAMESPACE_SOURCE, ns_limits(), &control)
            .unwrap();
    let (decoded, facts) = decode_value_origin(&expected, &received, SOURCE, limits()).unwrap();
    assert_eq!(decoded, source);
    assert_eq!(
        facts.allocation_request_bytes_upper_bound,
        bytes_shared_upper().unwrap()
    );
    let namespace_payload = received.payload(u32::MAX).unwrap().unwrap();
    assert_eq!(
        original(&decoded).payload().as_ptr(),
        namespace_payload.payload().as_ptr()
    );
    assert_eq!(
        original(&decoded).header().provider_id().as_str().as_ptr(),
        namespace_payload.header().provider_id().as_str().as_ptr()
    );
}
#[test]
fn value_origin_all_four_aggregate_phases_and_seven_writer_kinds_are_exact() {
    let control = Control::default();
    let encoded = encode_connector_payloads(&[], 0, ns_limits(), &control).unwrap();
    let decoded = decode_connector_payloads(&[], 0, ns_limits(), &control).unwrap();
    let phases = [
        (
            physical::AggregatePhase::Single,
            wire::aggregate_phase::Kind::Single(Empty {}),
        ),
        (
            physical::AggregatePhase::Partial {
                sequence: physical::AggregateSequenceId::new(0),
            },
            wire::aggregate_phase::Kind::PartialSequenceId(0),
        ),
        (
            physical::AggregatePhase::Intermediate {
                sequence: physical::AggregateSequenceId::new(u32::MAX),
            },
            wire::aggregate_phase::Kind::IntermediateSequenceId(u32::MAX),
        ),
        (
            physical::AggregatePhase::Final {
                sequence: physical::AggregateSequenceId::new(0),
            },
            wire::aggregate_phase::Kind::FinalSequenceId(0),
        ),
    ];
    for (phase, expected_phase) in phases {
        let source = physical::ValueOrigin::AggregateState {
            call: physical::AggregateCallId::new(u32::MAX),
            phase,
        };
        let expected = wrap(wire::value_origin::Kind::AggregateState(
            wire::AggregateStateOrigin {
                call_id: Some(u32::MAX),
                phase: Some(wire::AggregatePhase {
                    kind: Some(expected_phase),
                }),
            },
        ));
        assert_eq!(
            encode_value_origin(&source, &encoded, SOURCE, limits())
                .unwrap()
                .0,
            expected
        );
        assert_eq!(
            decode_value_origin(&expected, &decoded, SOURCE, limits())
                .unwrap()
                .0,
            source
        );
    }
    let kinds = [
        physical::WriterDerivedKind::RelationKind,
        physical::WriterDerivedKind::AffectedRows,
        physical::WriterDerivedKind::CommitFragment,
        physical::WriterDerivedKind::ChangeEvent,
        physical::WriterDerivedKind::RelationAuxiliary,
        physical::WriterDerivedKind::WriteTargetOrdinal,
        physical::WriterDerivedKind::GroupingKey,
    ];
    for (at, kind) in kinds.into_iter().enumerate() {
        let source = physical::ValueOrigin::WriterDerived {
            writer_node: physical::NodeId::new(0),
            kind,
        };
        let expected = wrap(wire::value_origin::Kind::WriterDerived(
            wire::WriterDerivedOrigin {
                writer_node_id: Some(0),
                kind: (at + 1) as i32,
            },
        ));
        assert_eq!(
            encode_value_origin(&source, &encoded, SOURCE, limits())
                .unwrap()
                .0,
            expected
        );
        assert_eq!(
            decode_value_origin(&expected, &decoded, SOURCE, limits())
                .unwrap()
                .0,
            source
        );
    }
}
#[test]
fn value_origin_provider_encoding_requires_exact_unique_original_owner_not_equal_payload_or_first_alias()
 {
    let source = provider(ConnectorCodecCategory::ReadColumn, &[9]);
    let equal_other_owner = original(&source).clone();
    assert_eq!(&equal_other_owner, original(&source));
    let control = Control::default();
    let other = [(0, &equal_other_owner)];
    let namespace =
        encode_connector_payloads(&other, NAMESPACE_SOURCE, ns_limits(), &control).unwrap();
    assert!(matches!(
        encode_value_origin(&source, &namespace, SOURCE, limits()),
        Err(Error::Payload(ConnectorPayloadCodecError::InvalidShape(
            "connector payload source owner is not in this namespace"
        )))
    ));
    let aliases = [(0, original(&source)), (u32::MAX, original(&source))];
    let namespace =
        encode_connector_payloads(&aliases, NAMESPACE_SOURCE, ns_limits(), &control).unwrap();
    assert!(matches!(
        encode_value_origin(&source, &namespace, SOURCE, limits()),
        Err(Error::Payload(ConnectorPayloadCodecError::InvalidShape(
            "connector payload source association is ambiguous"
        )))
    ));
    // Neutral purpose is preserved, not authenticated or retagged by origin
    // projection. The mandatory Fragment owner rejects an invalid read purpose.
    let source = provider(ConnectorCodecCategory::WriteHandle, &[]);
    let inputs = [(0, original(&source))];
    let namespace =
        encode_connector_payloads(&inputs, NAMESPACE_SOURCE, ns_limits(), &control).unwrap();
    let wire = encode_value_origin(&source, &namespace, SOURCE, limits())
        .unwrap()
        .0;
    let received =
        decode_connector_payloads(namespace.as_wire(), NAMESPACE_SOURCE, ns_limits(), &control)
            .unwrap();
    assert_eq!(
        original(
            &decode_value_origin(&wire, &received, SOURCE, limits())
                .unwrap()
                .0
        )
        .header()
        .category(),
        ConnectorCodecCategory::WriteHandle
    );
}
#[test]
fn value_origin_every_required_reference_kind_phase_and_writer_enum_refuses_without_defaults() {
    let control = Control::default();
    let namespace = decode_connector_payloads(&[], 0, ns_limits(), &control).unwrap();
    let mut invalids = vec![
        wire::ValueOrigin { kind: None },
        wrap(wire::value_origin::Kind::ProviderField(
            wire::ProviderFieldOrigin {
                scan_node_id: None,
                column_payload_id: Some(0),
            },
        )),
        wrap(wire::value_origin::Kind::ProviderField(
            wire::ProviderFieldOrigin {
                scan_node_id: Some(0),
                column_payload_id: None,
            },
        )),
        wrap(wire::value_origin::Kind::ProviderField(
            wire::ProviderFieldOrigin {
                scan_node_id: Some(0),
                column_payload_id: Some(u32::MAX),
            },
        )),
        wrap(wire::value_origin::Kind::Expression(
            wire::ExpressionOrigin {
                node_id: None,
                expr_id: Some(0),
            },
        )),
        wrap(wire::value_origin::Kind::Expression(
            wire::ExpressionOrigin {
                node_id: Some(0),
                expr_id: None,
            },
        )),
        wrap(wire::value_origin::Kind::NullExtended(
            wire::NullExtendedOrigin {
                node_id: None,
                original_value_id: Some(0),
            },
        )),
        wrap(wire::value_origin::Kind::NullExtended(
            wire::NullExtendedOrigin {
                node_id: Some(0),
                original_value_id: None,
            },
        )),
        wrap(wire::value_origin::Kind::AggregateState(
            wire::AggregateStateOrigin {
                call_id: None,
                phase: Some(wire::AggregatePhase {
                    kind: Some(wire::aggregate_phase::Kind::Single(Empty {})),
                }),
            },
        )),
        wrap(wire::value_origin::Kind::AggregateState(
            wire::AggregateStateOrigin {
                call_id: Some(0),
                phase: None,
            },
        )),
        wrap(wire::value_origin::Kind::AggregateState(
            wire::AggregateStateOrigin {
                call_id: Some(0),
                phase: Some(wire::AggregatePhase { kind: None }),
            },
        )),
        wrap(wire::value_origin::Kind::NodeOutput(
            wire::NodeOutputOrigin {
                node_id: None,
                output_ordinal: 0,
            },
        )),
        wrap(wire::value_origin::Kind::ExchangeImport(
            wire::ExchangeImportOrigin {
                edge_id: None,
                source_value_id: Some(0),
            },
        )),
        wrap(wire::value_origin::Kind::ExchangeImport(
            wire::ExchangeImportOrigin {
                edge_id: Some(0),
                source_value_id: None,
            },
        )),
        wrap(wire::value_origin::Kind::CteImport(wire::CteImportOrigin {
            edge_id: None,
            producer_fragment_id: Some(0),
            producer_value_id: Some(0),
        })),
        wrap(wire::value_origin::Kind::CteImport(wire::CteImportOrigin {
            edge_id: Some(0),
            producer_fragment_id: None,
            producer_value_id: Some(0),
        })),
        wrap(wire::value_origin::Kind::CteImport(wire::CteImportOrigin {
            edge_id: Some(0),
            producer_fragment_id: Some(0),
            producer_value_id: None,
        })),
        wrap(wire::value_origin::Kind::WriterDerived(
            wire::WriterDerivedOrigin {
                writer_node_id: None,
                kind: 1,
            },
        )),
    ];
    for kind in [0, -1, 8, i32::MAX] {
        invalids.push(wrap(wire::value_origin::Kind::WriterDerived(
            wire::WriterDerivedOrigin {
                writer_node_id: Some(0),
                kind,
            },
        )));
    }
    for (kind, ordinal) in [
        (0, Some(0)),
        (-1, Some(0)),
        (3, Some(0)),
        (1, None),
        (1, Some(3)),
        (2, Some(4)),
    ] {
        invalids.push(wrap(wire::value_origin::Kind::Summary(
            wire::SummaryField { kind, ordinal },
        )));
    }
    for invalid in invalids {
        assert!(decode_value_origin(&invalid, &namespace, SOURCE, limits()).is_err());
    }
}
fn exact(facts: ValueOriginProjectionFacts) -> ValueOriginProjectionLimits {
    ValueOriginProjectionLimits {
        max_allocation_requests: facts.allocation_requests_upper_bound,
        max_allocation_request_bytes: facts.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: facts
            .coexisting_source_and_request_bytes_upper_bound,
        max_work: facts.cumulative_work_upper_bound,
    }
}
#[test]
fn value_origin_all_request_work_and_source_limits_precede_clone_and_cover_empty_nonprovider() {
    let control = Control::default();
    let source = provider(ConnectorCodecCategory::ReadColumn, &[1, 2, 3]);
    let inputs = [(0, original(&source))];
    let encoded =
        encode_connector_payloads(&inputs, NAMESPACE_SOURCE, ns_limits(), &control).unwrap();
    let wire = encode_value_origin(&source, &encoded, SOURCE, limits())
        .unwrap()
        .0;
    let decoded =
        decode_connector_payloads(encoded.as_wire(), NAMESPACE_SOURCE, ns_limits(), &control)
            .unwrap();
    let facts = decode_value_origin(&wire, &decoded, SOURCE, limits())
        .unwrap()
        .1;
    assert!(decode_value_origin(&wire, &decoded, SOURCE, exact(facts)).is_ok());
    for at in 0..4 {
        let mut limits = exact(facts);
        let field = match at {
            0 => &mut limits.max_allocation_requests,
            1 => &mut limits.max_allocation_request_bytes,
            2 => &mut limits.max_coexisting_source_and_request_bytes,
            _ => &mut limits.max_work,
        };
        *field -= 1;
        assert!(decode_value_origin(&wire, &decoded, SOURCE, limits).is_err());
    }
    assert!(encode_value_origin(&source, &encoded, 0, limits()).is_err());
    assert!(decode_value_origin(&wire, &decoded, NAMESPACE_SOURCE, limits()).is_err());
    assert!(work_bound(usize::MAX, false).is_err());
    assert!(add(usize::MAX, 1).is_err());
    let source = provider(ConnectorCodecCategory::ReadColumn, &[]);
    let inputs = [(0, original(&source))];
    let encoded =
        encode_connector_payloads(&inputs, NAMESPACE_SOURCE, ns_limits(), &control).unwrap();
    let wire = encode_value_origin(&source, &encoded, SOURCE, limits())
        .unwrap()
        .0;
    let decoded =
        decode_connector_payloads(encoded.as_wire(), NAMESPACE_SOURCE, ns_limits(), &control)
            .unwrap();
    let (origin, facts) = decode_value_origin(&wire, &decoded, SOURCE, limits()).unwrap();
    assert_eq!(origin, source);
    assert_eq!(facts.allocation_requests_upper_bound, 0);
    assert!(decode_value_origin(&wire, &decoded, SOURCE, exact(facts)).is_ok());
}
fn every_prefix(mut invoke: impl FnMut(&Control) -> Result<(), Error>, ordinary: bool) {
    let baseline = Control::default();
    let result = invoke(&baseline);
    assert_eq!(result.is_err(), ordinary);
    let positive = trace(&baseline);
    for at in 0..positive.len() {
        for cause in CAUSES {
            let control = Control {
                stop: Some((at, cause.clone())),
                ..Control::default()
            };
            assert!(matches!(invoke(&control), Err(Error::Control(actual)) if actual == cause));
            assert_eq!(trace(&control), positive[..=at]);
        }
    }
}
#[test]
fn value_origin_both_directions_and_ordinary_tails_keep_every_original_namespace_control_prefix() {
    let source = provider(ConnectorCodecCategory::ReadColumn, &[0, 255]);
    let inputs = [(u32::MAX, original(&source))];
    every_prefix(
        |control| {
            let namespace =
                encode_connector_payloads(&inputs, NAMESPACE_SOURCE, ns_limits(), control)?;
            encode_value_origin(&source, &namespace, SOURCE, limits()).map(|_| ())
        },
        false,
    );
    let control = Control::default();
    let namespace =
        encode_connector_payloads(&inputs, NAMESPACE_SOURCE, ns_limits(), &control).unwrap();
    let expected = wrap(wire::value_origin::Kind::ProviderField(
        wire::ProviderFieldOrigin {
            scan_node_id: Some(0),
            column_payload_id: Some(u32::MAX),
        },
    ));
    every_prefix(
        |control| {
            let namespace = decode_connector_payloads(
                namespace.as_wire(),
                NAMESPACE_SOURCE,
                ns_limits(),
                control,
            )?;
            decode_value_origin(&expected, &namespace, SOURCE, limits()).map(|_| ())
        },
        false,
    );
    let aliases = [(0, original(&source)), (u32::MAX, original(&source))];
    every_prefix(
        |control| {
            let namespace =
                encode_connector_payloads(&aliases, NAMESPACE_SOURCE, ns_limits(), control)?;
            encode_value_origin(&source, &namespace, SOURCE, limits()).map(|_| ())
        },
        true,
    );
    let missing = wrap(wire::value_origin::Kind::ProviderField(
        wire::ProviderFieldOrigin {
            scan_node_id: Some(0),
            column_payload_id: Some(7),
        },
    ));
    every_prefix(
        |control| {
            let namespace = decode_connector_payloads(
                namespace.as_wire(),
                NAMESPACE_SOURCE,
                ns_limits(),
                control,
            )?;
            decode_value_origin(&missing, &namespace, SOURCE, limits()).map(|_| ())
        },
        true,
    );
}
#[test]
fn value_origin_real_namespace_floor_and_unique_source_scan_observe_quantum_without_max_id_storage()
{
    let source = provider(ConnectorCodecCategory::ReadColumn, &[9]);
    let others: Vec<_> = (0..320)
        .map(|_| payload(ConnectorCodecCategory::ReadColumn, &[9]))
        .collect();
    let mut inputs: Vec<_> = others
        .iter()
        .enumerate()
        .map(|(at, value)| (at as u32, value))
        .collect();
    inputs.push((u32::MAX, original(&source)));
    let baseline = Control::default();
    let namespace =
        encode_connector_payloads(&inputs, NAMESPACE_SOURCE, ns_limits(), &baseline).unwrap();
    let prior = trace(&baseline).len();
    let result = encode_value_origin(&source, &namespace, SOURCE, limits())
        .unwrap()
        .0;
    assert_eq!(
        result,
        wrap(wire::value_origin::Kind::ProviderField(
            wire::ProviderFieldOrigin {
                scan_node_id: Some(0),
                column_payload_id: Some(u32::MAX)
            }
        ))
    );
    let positive = trace(&baseline);
    let at = prior
        + positive[prior..]
            .iter()
            .position(|(_, units)| *units == 256)
            .unwrap();
    for cause in CAUSES {
        let control = Control {
            stop: Some((at, cause.clone())),
            ..Control::default()
        };
        let namespace =
            encode_connector_payloads(&inputs, NAMESPACE_SOURCE, ns_limits(), &control).unwrap();
        assert!(
            matches!(encode_value_origin(&source, &namespace, SOURCE, limits()), Err(Error::Control(actual)) if actual == cause)
        );
        assert_eq!(trace(&control), positive[..=at]);
    }
}

#[test]
fn value_origin_composing_preflight_keeps_provider_unique_until_admitted_emission() {
    let control = Control::default();
    let origin = provider(ConnectorCodecCategory::ReadColumn, &[0, 255, 1]);
    let inputs = [(u32::MAX, original(&origin))];
    let encoded =
        encode_connector_payloads(&inputs, NAMESPACE_SOURCE, ns_limits(), &control).unwrap();
    let mut meter = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let before =
        preflight_encode_observed(&origin, &encoded, SOURCE, limits(), &mut meter).unwrap();
    let (wire, after) = encode_observed(&origin, &encoded, SOURCE, limits(), &mut meter).unwrap();
    assert_eq!(before, after);
    assert_eq!(before.allocation_requests_upper_bound, 0);
    meter.finish().unwrap();

    let decoded =
        decode_connector_payloads(encoded.as_wire(), NAMESPACE_SOURCE, ns_limits(), &control)
            .unwrap();
    let payload = decoded.payload(u32::MAX).unwrap().unwrap();
    assert!(payload.payload().is_unique());
    let mut meter = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let before = preflight_decode_observed(&wire, &decoded, SOURCE, limits(), &mut meter).unwrap();
    assert!(
        payload.payload().is_unique(),
        "preflight must not clone or promote the payload"
    );
    assert_eq!(before.allocation_requests_upper_bound, 1);
    let mut refused = limits();
    refused.max_allocation_requests = 0;
    assert!(matches!(
        preflight_decode_observed(&wire, &decoded, SOURCE, refused, &mut meter),
        Err(Error::InvalidShape(_))
    ));
    assert!(
        payload.payload().is_unique(),
        "ordinary numerical refusal cannot request the clone"
    );
    let (actual, after) = decode_observed(&wire, &decoded, SOURCE, limits(), &mut meter).unwrap();
    assert_eq!(actual, origin);
    assert_eq!(before, after);
    assert!(
        !payload.payload().is_unique(),
        "only emission shares the actual payload backing"
    );
    meter.finish().unwrap();

    for (origin, expected) in nonprovider_cases() {
        let mut encode_meter = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let before =
            preflight_encode_observed(&origin, &encoded, SOURCE, limits(), &mut encode_meter)
                .unwrap();
        let (actual, after) =
            encode_observed(&origin, &encoded, SOURCE, limits(), &mut encode_meter).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(before, after);
        encode_meter.finish().unwrap();
        let mut decode_meter = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let before =
            preflight_decode_observed(&expected, &decoded, SOURCE, limits(), &mut decode_meter)
                .unwrap();
        let (actual, after) =
            decode_observed(&expected, &decoded, SOURCE, limits(), &mut decode_meter).unwrap();
        assert_eq!(actual, origin);
        assert_eq!(before, after);
        assert_eq!(before.allocation_requests_upper_bound, 0);
        decode_meter.finish().unwrap();
    }
}

#[test]
fn value_origin_composing_original_meter_keeps_all_preflight_and_emission_control_prefixes() {
    let origin = provider(ConnectorCodecCategory::ReadColumn, &[0, 255]);
    let inputs = [(u32::MAX, original(&origin))];
    every_prefix(
        |control| {
            let encoded =
                encode_connector_payloads(&inputs, NAMESPACE_SOURCE, ns_limits(), control)?;
            let mut meter = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
            let result = (|| {
                preflight_encode_observed(&origin, &encoded, SOURCE, limits(), &mut meter)?;
                encode_observed(&origin, &encoded, SOURCE, limits(), &mut meter).map(|_| ())
            })();
            finish(result, meter)
        },
        false,
    );
    let control = Control::default();
    let encoded =
        encode_connector_payloads(&inputs, NAMESPACE_SOURCE, ns_limits(), &control).unwrap();
    let wire = encode_value_origin(&origin, &encoded, SOURCE, limits())
        .unwrap()
        .0;
    for ordinary in [false, true] {
        let mut input = wire;
        if ordinary {
            input.kind = Some(wire::value_origin::Kind::ProviderField(
                wire::ProviderFieldOrigin {
                    scan_node_id: Some(0),
                    column_payload_id: Some(7),
                },
            ));
        }
        every_prefix(
            |control| {
                let decoded = decode_connector_payloads(
                    encoded.as_wire(),
                    NAMESPACE_SOURCE,
                    ns_limits(),
                    control,
                )?;
                let mut meter = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
                let result = (|| {
                    preflight_decode_observed(&input, &decoded, SOURCE, limits(), &mut meter)?;
                    decode_observed(&input, &decoded, SOURCE, limits(), &mut meter).map(|_| ())
                })();
                finish(result, meter)
            },
            ordinary,
        );
    }
}

#[path = "owned_tests.rs"]
mod owned_tests;
