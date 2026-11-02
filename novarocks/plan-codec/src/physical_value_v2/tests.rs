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
use crate::{
    physical_connector_payload_v2::{
        ConnectorPayloadProjectionLimits, bytes_shared_upper, decode_connector_payloads,
        encode_connector_payloads,
    },
    physical_type_v2::{TypeProjectionLimits, decode_type_table, encode_type_table_sources},
};
use arrow::datatypes::{DataType, Field};
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceId, ConnectorProviderId,
};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::sync::{Arc, Mutex};

const PRIOR: usize = 64 * 1024;
const SOURCE: usize = 2 * 1024 * 1024;
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
            Some((stop, cause)) if stop == at => Err(cause),
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
fn limits() -> ValueProjectionLimits {
    ValueProjectionLimits {
        max_definitions: 1024,
        max_origin_references: 4096,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 4 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 8 * 1024 * 1024,
        max_work: 256 * 1024 * 1024,
        origins: ValueOriginProjectionLimits {
            max_allocation_requests: 16,
            max_allocation_request_bytes: 64 * 1024,
            max_coexisting_source_and_request_bytes: 8 * 1024 * 1024,
            max_work: 64 * 1024 * 1024,
        },
    }
}
fn payload_limits() -> ConnectorPayloadProjectionLimits {
    ConnectorPayloadProjectionLimits {
        max_definitions: 1024,
        max_payload_bytes: 1024 * 1024,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 4 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 8 * 1024 * 1024,
        max_work: 64 * 1024 * 1024,
    }
}
fn type_limits() -> TypeProjectionLimits {
    TypeProjectionLimits {
        max_definitions: 100000,
        max_expanded_nodes: 100000,
        max_string_bytes: 1024 * 1024,
    }
}
fn ty(data_type: DataType) -> FunctionValueType {
    FunctionValueType::new(data_type, true)
}
fn nested() -> FunctionValueType {
    ty(DataType::Struct(
        vec![Arc::new(
            Field::new("child", DataType::Utf8, false).with_metadata(std::collections::HashMap::<
                String,
                String,
            >::from([(
                String::from("unknown"),
                String::from("retained"),
            )])),
        )]
        .into(),
    ))
}
fn provider() -> p::ValueOrigin {
    // Projection preserves this neutral category; purpose is Fragment-owned.
    p::ValueOrigin::ProviderField {
        scan_node: p::NodeId::new(0),
        field: p::ProviderColumnReference {
            column_payload: ConnectorEncodedPayload::new(
                ConnectorEnvelopeHeader::new(
                    ConnectorProviderId::parse("iceberg").unwrap(),
                    CatalogHandle::new(
                        ConnectorInstanceId::try_from_canonical("lake").unwrap(),
                        CatalogVersion::from_bytes([5; 32]),
                    ),
                    ConnectorCodecCategory::ReadTable,
                    ConnectorCodecRevision::try_new(1).unwrap(),
                ),
                vec![255, 0, 17].into(),
            ),
        },
    }
}
fn payload(value: &p::ValueDef) -> &ConnectorEncodedPayload {
    match &value.origin {
        p::ValueOrigin::ProviderField { field, .. } => &field.column_payload,
        _ => panic!("fixture requires actual provider origin"),
    }
}
fn def(id: u32, ty: FunctionValueType, origin: p::ValueOrigin) -> p::ValueDef {
    p::ValueDef {
        id: p::ValueId::new(id),
        ty,
        origin,
    }
}
fn expression() -> p::ValueOrigin {
    p::ValueOrigin::Expr {
        node: p::NodeId::new(u32::MAX),
        expr: p::ExprId::new(0),
    }
}
fn w(kind: wire::value_origin::Kind) -> wire::ValueOrigin {
    wire::ValueOrigin { kind: Some(kind) }
}
fn expected(id: u32, type_id: u32, kind: wire::value_origin::Kind) -> wire::ValueDefinition {
    wire::ValueDefinition {
        id,
        value_type_id: Some(type_id),
        origin: Some(w(kind)),
    }
}
fn all_origins() -> Vec<(p::ValueOrigin, wire::value_origin::Kind)> {
    vec![
        (
            provider(),
            wire::value_origin::Kind::ProviderField(wire::ProviderFieldOrigin {
                scan_node_id: Some(0),
                column_payload_id: Some(u32::MAX),
            }),
        ),
        (
            expression(),
            wire::value_origin::Kind::Expression(wire::ExpressionOrigin {
                node_id: Some(u32::MAX),
                expr_id: Some(0),
            }),
        ),
        (
            p::ValueOrigin::NullExtended {
                node: p::NodeId::new(0),
                of: p::ValueId::new(u32::MAX),
            },
            wire::value_origin::Kind::NullExtended(wire::NullExtendedOrigin {
                node_id: Some(0),
                original_value_id: Some(u32::MAX),
            }),
        ),
        (
            p::ValueOrigin::AggregateState {
                call: p::AggregateCallId::new(0),
                phase: p::AggregatePhase::Final {
                    sequence: p::AggregateSequenceId::new(u32::MAX),
                },
            },
            wire::value_origin::Kind::AggregateState(wire::AggregateStateOrigin {
                call_id: Some(0),
                phase: Some(wire::AggregatePhase {
                    kind: Some(wire::aggregate_phase::Kind::FinalSequenceId(u32::MAX)),
                }),
            }),
        ),
        (
            p::ValueOrigin::AggregateResult {
                call: p::AggregateCallId::new(u32::MAX),
            },
            wire::value_origin::Kind::AggregateResultCallId(u32::MAX),
        ),
        (
            p::ValueOrigin::NodeOutput {
                node: p::NodeId::new(0),
                output_ordinal: u32::MAX,
            },
            wire::value_origin::Kind::NodeOutput(wire::NodeOutputOrigin {
                node_id: Some(0),
                output_ordinal: u32::MAX,
            }),
        ),
        (
            p::ValueOrigin::ExchangeImport {
                edge: p::EdgeId::new(u32::MAX),
                source_value: p::ValueId::new(0),
            },
            wire::value_origin::Kind::ExchangeImport(wire::ExchangeImportOrigin {
                edge_id: Some(u32::MAX),
                source_value_id: Some(0),
            }),
        ),
        (
            p::ValueOrigin::CteImport {
                edge: p::EdgeId::new(0),
                producer_fragment: p::FragmentId::new(u32::MAX),
                producer_value: p::ValueId::new(0),
            },
            wire::value_origin::Kind::CteImport(wire::CteImportOrigin {
                edge_id: Some(0),
                producer_fragment_id: Some(u32::MAX),
                producer_value_id: Some(0),
            }),
        ),
        (
            p::ValueOrigin::WriterDerived {
                writer_node: p::NodeId::new(u32::MAX),
                kind: p::WriterDerivedKind::GroupingKey,
            },
            wire::value_origin::Kind::WriterDerived(wire::WriterDerivedOrigin {
                writer_node_id: Some(u32::MAX),
                kind: 7,
            }),
        ),
    ]
}

#[test]
fn value_namespace_all_nine_origins_have_independent_wire_and_typed_source_oracles() {
    let control = Control::default();
    let types = [
        (0, nested()),
        (
            u32::MAX,
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
        ),
    ];
    let type_sources = encode_type_table_sources(&types, &[], type_limits(), &control).unwrap();
    let receiving_types =
        decode_type_table(type_sources.as_wire(), type_limits(), &control).unwrap();
    let cases = all_origins();
    let values: Vec<_> = cases
        .iter()
        .enumerate()
        .map(|(i, (origin, _))| {
            let id = if i == 0 { u32::MAX } else { (i - 1) as u32 };
            def(id, types[usize::from(i == 0)].1.clone(), origin.clone())
        })
        .collect();
    let payload_sources = [(u32::MAX, payload(&values[0]))];
    let encoded_payloads =
        encode_connector_payloads(&payload_sources, PRIOR, payload_limits(), &control).unwrap();
    let decoded_payloads = decode_connector_payloads(
        encoded_payloads.as_wire(),
        PRIOR,
        payload_limits(),
        &control,
    )
    .unwrap();
    let inputs: Vec<_> = values
        .iter()
        .enumerate()
        .map(|(i, source)| ValueSource {
            source,
            value_type_id: if i == 0 { u32::MAX } else { 0 },
        })
        .collect();
    let wire_expected: Vec<_> = cases
        .into_iter()
        .enumerate()
        .map(|(i, (_, origin))| {
            expected(
                if i == 0 { u32::MAX } else { (i - 1) as u32 },
                if i == 0 { u32::MAX } else { 0 },
                origin,
            )
        })
        .collect();
    let encoded =
        encode_values(&inputs, &encoded_payloads, &type_sources, SOURCE, limits()).unwrap();
    assert_eq!(encoded.as_wire(), wire_expected);
    assert!(std::ptr::eq(encoded.types(), &type_sources));
    assert!(std::ptr::eq(encoded.payloads(), &encoded_payloads));
    assert!(std::ptr::eq(
        encoded.original_control(),
        &control as &dyn PureCompileControl
    ));
    for value in &values {
        assert!(std::ptr::eq(
            encoded.value(value.id.get()).unwrap().unwrap(),
            value
        ));
        assert_eq!(encoded.source_id(value).unwrap(), value.id.get());
    }
    assert!(encoded.value(100).unwrap().is_none());
    assert!(encoded.source_id(&values[0].clone()).is_err());
    let received = decode_values(
        &wire_expected,
        &decoded_payloads,
        &receiving_types,
        SOURCE,
        limits(),
    )
    .unwrap();
    assert!(std::ptr::eq(received.types(), &receiving_types));
    assert!(std::ptr::eq(received.payloads(), &decoded_payloads));
    assert!(std::ptr::eq(
        received.original_control(),
        &control as &dyn PureCompileControl
    ));
    for value in &values {
        assert_eq!(received.value(value.id.get()).unwrap().unwrap(), value);
    }
    assert_eq!(received.into_values(), values);
    // These projections intentionally do not claim closed Node/Expr/Call IDs.
}

#[test]
fn value_namespace_exact_type_and_source_binding_refuse_foreign_or_missing_facts() {
    let control = Control::default();
    let base = nested();
    let mut other_nullable = base.clone();
    other_nullable = other_nullable.with_nullable(false).unwrap();
    let different_child_nullability = ty(DataType::Struct(
        vec![Arc::new(Field::new("child", DataType::Utf8, true))].into(),
    ));
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let storage = novarocks_type_contract::ValueTypeStorageLifetime::pure();
    let json_child = novarocks_type_contract::ValueTypeRef::try_new(
        novarocks_type_contract::ValueKind::Nominal(ValueLogicalType::Json),
        false,
        storage.clone(),
    )
    .unwrap();
    let nested_json = FunctionValueType::try_from_value(
        novarocks_type_contract::ValueTypeRef::try_new(
            novarocks_type_contract::ValueKind::Struct(Box::from([
                novarocks_type_contract::ValueField::new("child", json_child, storage.clone()),
            ])),
            true,
            storage.clone(),
        )
        .unwrap(),
        novarocks_type_contract::ValueLayout::try_new(base.data_type(), storage).unwrap(),
    )
    .unwrap();
    let roots = [
        (0, base.clone()),
        (1, other_nullable),
        (2, different_child_nullability),
        (3, ty(DataType::Utf8)),
        (4, json.clone()),
        (5, nested_json),
    ];
    let types = encode_type_table_sources(&roots, &[], type_limits(), &control).unwrap();
    let decoded_types = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let plain = def(0, base, expression());
    let nominal = def(u32::MAX, json, expression());
    let payloads = encode_connector_payloads(&[], 0, payload_limits(), &control).unwrap();
    let decoded_payloads = decode_connector_payloads(&[], 0, payload_limits(), &control).unwrap();
    for type_id in [1, 2, 5, u32::MAX] {
        assert!(
            encode_values(
                &[ValueSource {
                    source: &plain,
                    value_type_id: type_id
                }],
                &payloads,
                &types,
                SOURCE,
                limits()
            )
            .is_err()
        );
    }
    assert!(
        encode_values(
            &[ValueSource {
                source: &nominal,
                value_type_id: 3
            }],
            &payloads,
            &types,
            SOURCE,
            limits()
        )
        .is_err()
    );
    let original = def(1, ty(DataType::Utf8), provider());
    let foreign = original.clone();
    let raw_payloads = [(0, payload(&foreign))];
    let foreign_namespace =
        encode_connector_payloads(&raw_payloads, PRIOR, payload_limits(), &control).unwrap();
    assert!(
        encode_values(
            &[ValueSource {
                source: &original,
                value_type_id: 3
            }],
            &foreign_namespace,
            &types,
            SOURCE,
            limits()
        )
        .is_err()
    );
    let aliases = [(0, payload(&original)), (u32::MAX, payload(&original))];
    let ambiguous = encode_connector_payloads(&aliases, PRIOR, payload_limits(), &control).unwrap();
    assert!(
        encode_values(
            &[ValueSource {
                source: &original,
                value_type_id: 3
            }],
            &ambiguous,
            &types,
            SOURCE,
            limits()
        )
        .is_err()
    );
    let good = expected(
        0,
        0,
        wire::value_origin::Kind::Expression(wire::ExpressionOrigin {
            node_id: Some(u32::MAX),
            expr_id: Some(0),
        }),
    );
    let mut invalids = Vec::new();
    let mut bad = good;
    bad.value_type_id = None;
    invalids.push(bad);
    let mut bad = good;
    bad.value_type_id = Some(u32::MAX);
    invalids.push(bad);
    let mut bad = good;
    bad.origin = None;
    invalids.push(bad);
    let mut bad = good;
    bad.origin = Some(wire::ValueOrigin { kind: None });
    invalids.push(bad);
    invalids.push(expected(
        0,
        0,
        wire::value_origin::Kind::ProviderField(wire::ProviderFieldOrigin {
            scan_node_id: Some(0),
            column_payload_id: Some(0),
        }),
    ));
    invalids.push(expected(
        0,
        0,
        wire::value_origin::Kind::Expression(wire::ExpressionOrigin {
            node_id: None,
            expr_id: Some(0),
        }),
    ));
    invalids.push(expected(
        0,
        0,
        wire::value_origin::Kind::WriterDerived(wire::WriterDerivedOrigin {
            writer_node_id: Some(0),
            kind: 0,
        }),
    ));
    invalids.push(expected(
        0,
        0,
        wire::value_origin::Kind::AggregateState(wire::AggregateStateOrigin {
            call_id: Some(0),
            phase: Some(wire::AggregatePhase { kind: None }),
        }),
    ));
    for bad in invalids {
        assert!(
            decode_values(&[bad], &decoded_payloads, &decoded_types, SOURCE, limits()).is_err()
        );
    }
    assert!(
        encode_values(
            &[ValueSource {
                source: &plain,
                value_type_id: 0
            }; 2],
            &payloads,
            &types,
            SOURCE,
            limits()
        )
        .is_err()
    );
    assert!(
        decode_values(
            &[good, good],
            &decoded_payloads,
            &decoded_types,
            SOURCE,
            limits()
        )
        .is_err()
    );
}

fn exact(facts: ValueProjectionFacts) -> ValueProjectionLimits {
    ValueProjectionLimits {
        max_definitions: facts.definition_count,
        max_origin_references: facts.origin_reference_count,
        max_allocation_requests: facts.allocation_requests_upper_bound,
        max_allocation_request_bytes: facts.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: facts
            .coexisting_source_and_request_bytes_upper_bound,
        max_work: facts.cumulative_work_upper_bound,
        origins: limits().origins,
    }
}
fn smaller(limits: ValueProjectionLimits, field: usize) -> ValueProjectionLimits {
    let mut l = limits;
    match field {
        0 => l.max_definitions -= 1,
        1 => l.max_origin_references -= 1,
        2 => l.max_allocation_requests -= 1,
        3 => l.max_allocation_request_bytes -= 1,
        4 => l.max_coexisting_source_and_request_bytes -= 1,
        5 => l.max_work -= 1,
        _ => unreachable!("fixture field count"),
    }
    l
}
#[test]
fn value_namespace_all_six_caps_aggregate_dictionary_and_origin_requests_before_output() {
    let control = Control::default();
    let source = def(
        u32::MAX,
        ty(DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Utf8),
        )),
        provider(),
    );
    let roots = [(u32::MAX, source.ty.clone())];
    let types = encode_type_table_sources(&roots, &[], type_limits(), &control).unwrap();
    let decoded_types = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let payload_sources = [(0, payload(&source))];
    let payloads =
        encode_connector_payloads(&payload_sources, PRIOR, payload_limits(), &control).unwrap();
    let received_payloads =
        decode_connector_payloads(payloads.as_wire(), PRIOR, payload_limits(), &control).unwrap();
    let inputs = [ValueSource {
        source: &source,
        value_type_id: u32::MAX,
    }];
    let encoded = encode_values(&inputs, &payloads, &types, SOURCE, limits()).unwrap();
    let received = decode_values(
        encoded.as_wire(),
        &received_payloads,
        &decoded_types,
        SOURCE,
        limits(),
    )
    .unwrap();
    let f = *received.facts();
    assert_eq!(f.allocation_requests_upper_bound, 3); // index, values, potential Bytes Shared; binding clones share both owners.
    assert_eq!(
        f.allocation_request_bytes_upper_bound,
        size_of::<usize>() + size_of::<p::ValueDef>() + bytes_shared_upper().unwrap()
    );
    assert_eq!(f.origin_reference_count, 2);
    assert_eq!(encoded.facts().allocation_requests_upper_bound, 2);
    assert!(encode_values(&inputs, &payloads, &types, SOURCE, exact(*encoded.facts())).is_ok());
    assert!(
        decode_values(
            encoded.as_wire(),
            &received_payloads,
            &decoded_types,
            SOURCE,
            exact(f)
        )
        .is_ok()
    );
    for field in 0..6 {
        assert!(
            encode_values(
                &inputs,
                &payloads,
                &types,
                SOURCE,
                smaller(exact(*encoded.facts()), field)
            )
            .is_err(),
            "encode cap {field}"
        );
        assert!(
            decode_values(
                encoded.as_wire(),
                &received_payloads,
                &decoded_types,
                SOURCE,
                smaller(exact(f), field)
            )
            .is_err(),
            "decode cap {field}"
        );
    }
    let mut l = limits();
    l.origins.max_allocation_requests = 0;
    assert!(
        decode_values(
            encoded.as_wire(),
            &received_payloads,
            &decoded_types,
            SOURCE,
            l
        )
        .is_err()
    );
    assert!(
        encode_values(
            &inputs,
            &payloads,
            &types,
            size_of::<ValueSource<'_>>(),
            limits()
        )
        .is_err()
    );
    assert!(
        decode_values(
            encoded.as_wire(),
            &received_payloads,
            &decoded_types,
            0,
            limits()
        )
        .is_err()
    );
    assert!(add(usize::MAX, 1).is_err());
    assert!(mul(usize::MAX, 2).is_err());
    assert!(bytes::<wire::ValueDefinition>(usize::MAX).is_err());
    let retained = received.retained_invoice_floor().unwrap();
    assert!(retained >= SOURCE + size_of::<p::ValueDef>() + 2 * size_of::<DataType>());
}

fn every_callback(
    control: &Control,
    expected_phase: CompilePhase,
    ordinary: bool,
    mut action: impl FnMut() -> Result<(), Error>,
) {
    control.arm(None);
    let result = action();
    assert_eq!(result.is_err(), ordinary, "baseline: {result:?}");
    let baseline = control.trace();
    assert!(!baseline.is_empty());
    assert!(baseline.iter().all(|(phase, _)| *phase == expected_phase));
    for at in 0..baseline.len() {
        for cause in CAUSES {
            control.arm(Some((at, cause.clone())));
            assert!(
                matches!(action(), Err(Error::Control(c)) if c == cause),
                "at={at}, cause={cause:?}"
            );
            assert_eq!(control.trace(), baseline[..=at]);
        }
    }
    control.arm(None);
}
#[test]
fn value_namespace_original_control_at_every_success_ordinary_and_lookup_boundary() {
    let control = Control::default();
    let source = def(0, ty(DataType::Int64), expression());
    let roots = [(u32::MAX, source.ty.clone())];
    let types = encode_type_table_sources(&roots, &[], type_limits(), &control).unwrap();
    let decoded_types = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let payloads = encode_connector_payloads(&[], 0, payload_limits(), &control).unwrap();
    let received_payloads = decode_connector_payloads(&[], 0, payload_limits(), &control).unwrap();
    let inputs = [ValueSource {
        source: &source,
        value_type_id: u32::MAX,
    }];
    let encoded = encode_values(&inputs, &payloads, &types, SOURCE, limits()).unwrap();
    let received = decode_values(
        encoded.as_wire(),
        &received_payloads,
        &decoded_types,
        SOURCE,
        limits(),
    )
    .unwrap();
    every_callback(&control, CompilePhase::Encode, false, || {
        encode_values(&inputs, &payloads, &types, SOURCE, limits()).map(|_| ())
    });
    let bad_type = [ValueSource {
        source: &source,
        value_type_id: 0,
    }];
    every_callback(&control, CompilePhase::Encode, true, || {
        encode_values(&bad_type, &payloads, &types, SOURCE, limits()).map(|_| ())
    });
    every_callback(&control, CompilePhase::Encode, true, || {
        encode_values(&[inputs[0]; 2], &payloads, &types, SOURCE, limits()).map(|_| ())
    });
    every_callback(&control, CompilePhase::Decode, false, || {
        decode_values(
            encoded.as_wire(),
            &received_payloads,
            &decoded_types,
            SOURCE,
            limits(),
        )
        .map(|_| ())
    });
    let mut bad = encoded.as_wire().to_vec();
    bad[0].origin.as_mut().unwrap().kind = Some(wire::value_origin::Kind::Expression(
        wire::ExpressionOrigin {
            node_id: Some(0),
            expr_id: None,
        },
    ));
    every_callback(&control, CompilePhase::Decode, true, || {
        decode_values(&bad, &received_payloads, &decoded_types, SOURCE, limits()).map(|_| ())
    });
    let duplicate = vec![encoded.as_wire()[0]; 2];
    every_callback(&control, CompilePhase::Decode, true, || {
        decode_values(
            &duplicate,
            &received_payloads,
            &decoded_types,
            SOURCE,
            limits(),
        )
        .map(|_| ())
    });
    every_callback(&control, CompilePhase::Encode, false, || {
        encoded.value(0).map(|_| ())
    });
    every_callback(&control, CompilePhase::Decode, false, || {
        received.value(u32::MAX).map(|_| ())
    });
}

#[test]
fn value_namespace_real_320_occurrences_observe_quantum_and_retain_order() {
    // Complete small primitive-source fixture backing, including both wire
    // namespaces, fits this explicit invoice; no profile cap is increased.
    const WIDE_SOURCE: usize = 128 * 1024;
    let control = Control::default();
    let roots = [(0, ty(DataType::Int32))];
    let types = encode_type_table_sources(&roots, &[], type_limits(), &control).unwrap();
    let decoded_types = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let payloads = encode_connector_payloads(&[], 0, payload_limits(), &control).unwrap();
    let received_payloads = decode_connector_payloads(&[], 0, payload_limits(), &control).unwrap();
    let values: Vec<_> = (0..320)
        .rev()
        .map(|id| {
            def(
                id,
                roots[0].1.clone(),
                p::ValueOrigin::NodeOutput {
                    node: p::NodeId::new(0),
                    output_ordinal: id,
                },
            )
        })
        .collect();
    let inputs: Vec<_> = values
        .iter()
        .map(|source| ValueSource {
            source,
            value_type_id: 0,
        })
        .collect();
    control.arm(None);
    let encoded = encode_values(&inputs, &payloads, &types, WIDE_SOURCE, limits()).unwrap();
    let encode_trace = control.trace();
    assert!(encode_trace.iter().any(|(_, units)| *units == 256));
    control.arm(None);
    let decoded = decode_values(
        encoded.as_wire(),
        &received_payloads,
        &decoded_types,
        WIDE_SOURCE,
        limits(),
    )
    .unwrap();
    let decode_trace = control.trace();
    assert!(decode_trace.iter().any(|(_, units)| *units == 256));
    assert_eq!(decoded.into_values(), values);
    assert_eq!(
        encoded.as_wire().iter().map(|v| v.id).collect::<Vec<_>>(),
        (0..320).rev().collect::<Vec<_>>()
    );
    for (phase, baseline) in [
        (CompilePhase::Encode, encode_trace),
        (CompilePhase::Decode, decode_trace),
    ] {
        let quantum = baseline
            .iter()
            .position(|(_, units)| *units == 256)
            .unwrap();
        for at in [0, quantum, baseline.len() - 1] {
            for cause in CAUSES {
                control.arm(Some((at, cause.clone())));
                let result = match phase {
                    CompilePhase::Encode => {
                        encode_values(&inputs, &payloads, &types, WIDE_SOURCE, limits()).map(|_| ())
                    }
                    CompilePhase::Decode => decode_values(
                        encoded.as_wire(),
                        &received_payloads,
                        &decoded_types,
                        WIDE_SOURCE,
                        limits(),
                    )
                    .map(|_| ()),
                    _ => unreachable!("fixture codec phases"),
                };
                assert!(matches!(result, Err(Error::Control(c)) if c == cause));
                assert_eq!(control.trace(), baseline[..=at]);
            }
        }
    }
}

#[test]
fn value_namespace_empty_and_full_fvt_receiving_do_not_guess_source_metadata() {
    let control = Control::default();
    let roots = [(0, nested())];
    let types = encode_type_table_sources(&roots, &[], type_limits(), &control).unwrap();
    let decoded_types = decode_type_table(types.as_wire(), type_limits(), &control).unwrap();
    let payloads = encode_connector_payloads(&[], 0, payload_limits(), &control).unwrap();
    let received_payloads = decode_connector_payloads(&[], 0, payload_limits(), &control).unwrap();
    let encoded = encode_values(&[], &payloads, &types, SOURCE, limits()).unwrap();
    assert!(encoded.as_wire().is_empty());
    assert_eq!(encoded.facts().allocation_requests_upper_bound, 0);
    let decoded = decode_values(&[], &received_payloads, &decoded_types, SOURCE, limits()).unwrap();
    assert_eq!(decoded.source_count(), 0);
    assert_eq!(decoded.facts().allocation_request_bytes_upper_bound, 0);
    let raw = [expected(
        u32::MAX,
        0,
        wire::value_origin::Kind::NodeOutput(wire::NodeOutputOrigin {
            node_id: Some(0),
            output_ordinal: u32::MAX,
        }),
    )];
    let decoded =
        decode_values(&raw, &received_payloads, &decoded_types, SOURCE, limits()).unwrap();
    let actual = decoded.value(u32::MAX).unwrap().unwrap();
    assert_eq!(actual.ty, roots[0].1);
    match (
        &actual.ty.data_type(),
        &decoded_types.value_type(0).unwrap().data_type(),
    ) {
        (DataType::Struct(actual), DataType::Struct(source)) => {
            assert!(Arc::ptr_eq(&actual[0], &source[0]))
        }
        _ => panic!("fixture nested storage type"),
    }
    assert_eq!(decoded.facts().allocation_requests_upper_bound, 2); // shared nested FieldRef, no Dictionary box.
}

#[path = "owned_tests.rs"]
mod owned_tests;
