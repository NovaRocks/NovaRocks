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
#[path = "source_reference_tests.rs"]
mod source_reference_tests;
use novarocks_connector_contract::ConnectorCodecCategory;
use std::sync::{Mutex, atomic::AtomicUsize};
const SOURCE: usize = 1024 * 1024;
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
fn limits() -> ConnectorPayloadProjectionLimits {
    ConnectorPayloadProjectionLimits {
        max_definitions: 1024,
        max_payload_bytes: 1024 * 1024,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 4 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 8 * 1024 * 1024,
        max_work: 64 * 1024 * 1024,
    }
}
fn payload(category: ConnectorCodecCategory, body: &[u8]) -> ConnectorEncodedPayload {
    ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            CatalogHandle::new(
                ConnectorInstanceId::try_from_canonical("catalog_a.v2").unwrap(),
                CatalogVersion::from_bytes([0; 32]),
            ),
            category,
            ConnectorCodecRevision::try_new(u32::MAX).unwrap(),
        ),
        body.to_vec().into(),
    )
}
fn expected(id: u32, category: i32, body: &[u8]) -> wire::ConnectorPayloadDefinition {
    wire::ConnectorPayloadDefinition {
        id,
        payload: Some(dto::ConnectorEncodedPayload {
            header: Some(dto::ConnectorEnvelopeHeader {
                provider_id: "iceberg".into(),
                catalog: Some(catalog::CatalogHandle {
                    catalog_name: "catalog_a.v2".into(),
                    version: vec![0; 32],
                }),
                category,
                codec_revision: u32::MAX,
            }),
            payload: body.to_vec(),
        }),
    }
}
fn trace(control: &Control) -> Vec<(CompilePhase, u32)> {
    control.trace.lock().unwrap().clone()
}
fn full_prefixes(mut invoke: impl FnMut(&Control) -> Result<(), Error>) {
    let baseline = Control::default();
    invoke(&baseline).unwrap();
    let positive = trace(&baseline);
    assert!(!positive.is_empty());
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
fn payload_namespace_categories_have_independent_wire_and_typed_receiving_oracles() {
    let categories = [
        ConnectorCodecCategory::ReadTable,
        ConnectorCodecCategory::ReadView,
        ConnectorCodecCategory::ReadColumn,
        ConnectorCodecCategory::ReadSplit,
        ConnectorCodecCategory::WriteHandle,
        ConnectorCodecCategory::CommitFragment,
        ConnectorCodecCategory::WriteLayout,
    ];
    let values: Vec<_> = categories
        .into_iter()
        .map(|c| payload(c, &[0, 255, 9]))
        .collect();
    let ids = [u32::MAX, 0, 17, 9, 42, 3, 56];
    let inputs: Vec<_> = ids.into_iter().zip(values.iter()).collect();
    let control = Control::default();
    let encoded = encode_connector_payloads(&inputs, SOURCE, limits(), &control).unwrap();
    let expected: Vec<_> = ids
        .into_iter()
        .enumerate()
        .map(|(at, id)| expected(id, (at + 1) as i32, &[0, 255, 9]))
        .collect();
    assert_eq!(encoded.as_wire(), expected);
    for (id, original) in &inputs {
        assert!(std::ptr::eq(
            encoded.payload(*id).unwrap().unwrap(),
            *original
        ));
    }
    let decoded = decode_connector_payloads(encoded.as_wire(), SOURCE, limits(), &control).unwrap();
    assert!(std::ptr::eq(decoded.as_wire(), encoded.as_wire()));
    for (id, original) in &inputs {
        assert_eq!(decoded.payload(*id).unwrap(), Some(*original));
    }
    assert!(encoded.payload(100).unwrap().is_none());
    assert!(decoded.payload(100).unwrap().is_none());
    // A zero catalog version is a legal neutral identity, never defaulted or
    // rejected as a guessed provider-private rule.
    assert_eq!(
        decoded
            .payload(0)
            .unwrap()
            .unwrap()
            .header()
            .catalog()
            .version()
            .as_bytes(),
        &[0; 32]
    );
}
#[test]
fn payload_namespace_sparse_aliases_duplicates_empty_and_original_lookup_loan_are_exact() {
    let value = payload(ConnectorCodecCategory::ReadColumn, &[]);
    let inputs = [(u32::MAX, &value), (0, &value)];
    let control = Control::default();
    let encoded = encode_connector_payloads(&inputs, SOURCE, limits(), &control).unwrap();
    assert!(std::ptr::eq(encoded.payload(0).unwrap().unwrap(), &value));
    assert!(std::ptr::eq(
        encoded.payload(u32::MAX).unwrap().unwrap(),
        &value
    ));
    let duplicate = [(0, &value), (0, &value)];
    assert!(matches!(
        encode_connector_payloads(&duplicate, SOURCE, limits(), &control),
        Err(Error::InvalidShape(_))
    ));
    let duplicate_wire = [expected(u32::MAX, 3, &[]), expected(u32::MAX, 3, &[])];
    assert!(matches!(
        decode_connector_payloads(&duplicate_wire, SOURCE, limits(), &control),
        Err(Error::InvalidShape(_))
    ));
    let empty = encode_connector_payloads(&[], 0, limits(), &control).unwrap();
    assert!(empty.as_wire().is_empty());
    assert_eq!(empty.facts().allocation_requests_upper_bound, 0);
    let empty = decode_connector_payloads(&[], 0, limits(), &control).unwrap();
    assert!(empty.payload(0).unwrap().is_none());
}
#[test]
fn payload_namespace_receiving_uses_sole_identity_grammar_and_rejects_required_header_failures() {
    let control = Control::default();
    let mut invalids = Vec::new();
    let mut value = expected(0, 1, &[]);
    value.payload = None;
    invalids.push(value);
    let mut value = expected(0, 1, &[]);
    value.payload.as_mut().unwrap().header = None;
    invalids.push(value);
    let mut value = expected(0, 1, &[]);
    value
        .payload
        .as_mut()
        .unwrap()
        .header
        .as_mut()
        .unwrap()
        .catalog = None;
    invalids.push(value);
    for category in [0, -1, 8, i32::MAX] {
        invalids.push(expected(0, category, &[]));
    }
    let mut value = expected(0, 1, &[]);
    value
        .payload
        .as_mut()
        .unwrap()
        .header
        .as_mut()
        .unwrap()
        .codec_revision = 0;
    invalids.push(value);
    for n in [0, 31, 33] {
        let mut value = expected(0, 1, &[]);
        value
            .payload
            .as_mut()
            .unwrap()
            .header
            .as_mut()
            .unwrap()
            .catalog
            .as_mut()
            .unwrap()
            .version = vec![0; n];
        invalids.push(value);
    }
    for value in invalids {
        assert!(decode_connector_payloads(&[value], SOURCE, limits(), &control).is_err());
    }
    for provider in ["", "Iceberg", "bad|id", "a.b"] {
        let mut value = expected(0, 1, &[]);
        value
            .payload
            .as_mut()
            .unwrap()
            .header
            .as_mut()
            .unwrap()
            .provider_id = provider.into();
        assert!(matches!(
            decode_connector_payloads(&[value], SOURCE, limits(), &control),
            Err(Error::Identity(ConnectorIdentityError::InvalidProviderId))
        ));
    }
    for name in ["", "Catalog", "a|b", "é"] {
        let mut value = expected(0, 1, &[]);
        value
            .payload
            .as_mut()
            .unwrap()
            .header
            .as_mut()
            .unwrap()
            .catalog
            .as_mut()
            .unwrap()
            .catalog_name = name.into();
        assert!(matches!(
            decode_connector_payloads(&[value], SOURCE, limits(), &control),
            Err(Error::Identity(
                ConnectorIdentityError::InvalidCanonicalInstanceId
            ))
        ));
    }
    let mut value = expected(0, 1, &[]);
    value
        .payload
        .as_mut()
        .unwrap()
        .header
        .as_mut()
        .unwrap()
        .catalog
        .as_mut()
        .unwrap()
        .catalog_name = "_catalog-x.y".into();
    let decoded =
        decode_connector_payloads(std::slice::from_ref(&value), SOURCE, limits(), &control)
            .unwrap();
    assert_eq!(
        decoded
            .payload(0)
            .unwrap()
            .unwrap()
            .header()
            .catalog()
            .catalog_name()
            .as_str(),
        "_catalog-x.y"
    );
}
fn exact_limits(facts: &ConnectorPayloadProjectionFacts) -> ConnectorPayloadProjectionLimits {
    ConnectorPayloadProjectionLimits {
        max_definitions: facts.definition_count,
        max_payload_bytes: facts.payload_bytes,
        max_allocation_requests: facts.allocation_requests_upper_bound,
        max_allocation_request_bytes: facts.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: facts
            .coexisting_source_and_request_bytes_upper_bound,
        max_work: facts.cumulative_work_upper_bound,
    }
}
fn one_under(
    mut limits: ConnectorPayloadProjectionLimits,
    which: usize,
) -> ConnectorPayloadProjectionLimits {
    let field = match which {
        0 => &mut limits.max_definitions,
        1 => &mut limits.max_payload_bytes,
        2 => &mut limits.max_allocation_requests,
        3 => &mut limits.max_allocation_request_bytes,
        4 => &mut limits.max_coexisting_source_and_request_bytes,
        _ => &mut limits.max_work,
    };
    *field -= 1;
    limits
}
#[test]
fn payload_namespace_all_six_numeric_limits_source_capacities_and_layout_overflow_are_checked() {
    let value = payload(ConnectorCodecCategory::WriteHandle, &[1, 2, 3]);
    let inputs = [(0, &value)];
    let control = Control::default();
    let encoded = encode_connector_payloads(&inputs, SOURCE, limits(), &control).unwrap();
    let exact = exact_limits(encoded.facts());
    assert!(encode_connector_payloads(&inputs, SOURCE, exact, &control).is_ok());
    for at in 0..6 {
        assert!(
            encode_connector_payloads(&inputs, SOURCE, one_under(exact, at), &control).is_err()
        );
    }
    let decoded = decode_connector_payloads(encoded.as_wire(), SOURCE, limits(), &control).unwrap();
    let exact = exact_limits(decoded.facts());
    assert!(decode_connector_payloads(encoded.as_wire(), SOURCE, exact, &control).is_ok());
    for at in 0..6 {
        assert!(
            decode_connector_payloads(encoded.as_wire(), SOURCE, one_under(exact, at), &control)
                .is_err()
        );
    }
    assert!(encode_connector_payloads(&inputs, 0, limits(), &control).is_err());
    let mut raw = expected(0, 1, &[1]);
    raw.payload.as_mut().unwrap().payload.reserve(8192);
    let known_roots_only = size_of::<wire::ConnectorPayloadDefinition>();
    assert!(decode_connector_payloads(&[raw], known_roots_only, limits(), &control).is_err());
    assert!(bytes::<usize>(usize::MAX).is_err());
    assert!(add(usize::MAX, 1).is_err());
    assert!(mul(usize::MAX, 2).is_err());
    assert!(arc_str_bytes(usize::MAX).is_err());
}
#[test]
fn payload_namespace_both_directions_keep_every_original_callback_cause_and_prefix() {
    let value = payload(ConnectorCodecCategory::ReadTable, &[1, 2, 3]);
    let inputs = [(u32::MAX, &value), (0, &value)];
    full_prefixes(|control| {
        encode_connector_payloads(&inputs, SOURCE, limits(), control).map(|_| ())
    });
    let raw = [expected(u32::MAX, 1, &[1, 2, 3]), expected(0, 6, &[])];
    full_prefixes(|control| decode_connector_payloads(&raw, SOURCE, limits(), control).map(|_| ()));
}
#[test]
fn payload_namespace_ordinary_failure_and_lookup_tails_keep_primary_control_without_publication() {
    let value = payload(ConnectorCodecCategory::ReadTable, &[]);
    let duplicate = [(0, &value), (0, &value)];
    let raw = [expected(0, 0, &[])];
    for decode in [false, true] {
        let invoke = |control: &Control| {
            if decode {
                decode_connector_payloads(&raw, SOURCE, limits(), control).map(|_| ())
            } else {
                encode_connector_payloads(&duplicate, SOURCE, limits(), control).map(|_| ())
            }
        };
        let baseline = Control::default();
        assert!(invoke(&baseline).is_err());
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
    let source = [(0, &value)];
    for id in [0, 1] {
        let baseline = Control::default();
        let token = encode_connector_payloads(&source, SOURCE, limits(), &baseline).unwrap();
        let prepared_callbacks = trace(&baseline).len();
        assert_eq!(token.payload(id).unwrap().is_some(), id == 0);
        let positive = trace(&baseline);
        for at in prepared_callbacks..positive.len() {
            for cause in CAUSES {
                let control = Control {
                    stop: Some((at, cause.clone())),
                    ..Control::default()
                };
                let token = encode_connector_payloads(&source, SOURCE, limits(), &control).unwrap();
                assert!(
                    matches!(token.payload(id), Err(Error::Control(actual)) if actual == cause)
                );
                assert_eq!(trace(&control), positive[..=at]);
            }
        }
    }
}
#[test]
fn payload_namespace_real_wide_index_and_payload_copy_observe_actual_quantum() {
    let value = payload(ConnectorCodecCategory::ReadSplit, &vec![0x7f; 2048]);
    let inputs: Vec<_> = (0..320).rev().map(|id| (id, &value)).collect();
    let baseline = Control::default();
    let token = encode_connector_payloads(&inputs, SOURCE, limits(), &baseline).unwrap();
    assert_eq!(token.as_wire().len(), 320);
    let positive = trace(&baseline);
    let at = positive
        .iter()
        .position(|(_, units)| *units == 256)
        .unwrap();
    for cause in CAUSES {
        let control = Control {
            stop: Some((at, cause.clone())),
            ..Control::default()
        };
        assert!(
            matches!(encode_connector_payloads(&inputs, SOURCE, limits(), &control), Err(Error::Control(actual)) if actual == cause)
        );
        assert_eq!(trace(&control), positive[..=at]);
    }
    assert_eq!(token.payload(319).unwrap().unwrap().payload().len(), 2048);
}
#[test]
fn payload_namespace_bytes_and_arc_request_formulas_cover_exact_independent_layout_boundaries() {
    request_model().unwrap();
    let align = align_of::<AtomicUsize>();
    for n in [0, 1, 64, 128, 1024] {
        let raw = size_of::<[AtomicUsize; 2]>() + n;
        let expected = raw.div_ceil(align) * align;
        assert_eq!(arc_str_bytes(n).unwrap(), expected);
    }
    assert!(
        bytes_shared_upper().unwrap()
            >= size_of::<*mut u8>() + size_of::<usize>() + size_of::<AtomicUsize>()
    );
    let control = Control::default();
    let empty = [expected(0, 1, &[])];
    let nonempty = [expected(0, 1, &[9])];
    let a = decode_connector_payloads(&empty, SOURCE, limits(), &control).unwrap();
    let b = decode_connector_payloads(&nonempty, SOURCE, limits(), &control).unwrap();
    assert_eq!(
        b.facts().allocation_requests_upper_bound - a.facts().allocation_requests_upper_bound,
        2
    );
    assert_eq!(
        b.facts().allocation_request_bytes_upper_bound
            - a.facts().allocation_request_bytes_upper_bound,
        1 + bytes_shared_upper().unwrap()
    );
}

#[test]
fn original_namespace_header_resource_floor_uses_actual_layout_without_observation() {
    let control = Control::default();
    let encoded = encode_connector_payloads(&[], SOURCE, limits(), &control).unwrap();
    let decoded = decode_connector_payloads(&[], SOURCE, limits(), &control).unwrap();
    let before = trace(&control);
    assert_eq!(
        encoded.retained_floor_header_admitted().unwrap(),
        SOURCE + std::mem::size_of_val(&encoded)
    );
    assert_eq!(
        decoded.retained_floor_header_admitted().unwrap(),
        SOURCE + std::mem::size_of_val(&decoded)
    );
    assert_eq!(trace(&control), before);
    let unlimited = ConnectorPayloadProjectionLimits {
        max_definitions: usize::MAX,
        max_payload_bytes: usize::MAX,
        max_allocation_requests: usize::MAX,
        max_allocation_request_bytes: usize::MAX,
        max_coexisting_source_and_request_bytes: usize::MAX,
        max_work: usize::MAX,
    };
    let encoded = encode_connector_payloads(&[], usize::MAX, unlimited, &control).unwrap();
    let decoded = decode_connector_payloads(&[], usize::MAX, unlimited, &control).unwrap();
    let before = trace(&control);
    assert_eq!(
        encoded.retained_floor_header_admitted(),
        Err(CompileControlError::ResourceExhausted)
    );
    assert_eq!(
        decoded.retained_floor_header_admitted(),
        Err(CompileControlError::ResourceExhausted)
    );
    assert_eq!(trace(&control), before);
}
