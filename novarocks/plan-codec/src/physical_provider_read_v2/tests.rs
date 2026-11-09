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
use crate::physical_provider_binding_v2::{
    ProviderBindingProjectionLimits, decode_provider_bindings, encode_provider_bindings,
};
use novarocks_connector_contract::{
    CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorErrorKind,
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId, ConnectorReadBinding,
};
use std::sync::Mutex;
const PRIOR_SOURCE: usize = 64 * 1024;
const SOURCE: usize = 1024 * 1024;
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
            assert!(at <= stop, "callback after refusal");
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
}
fn trace(c: &Control) -> Vec<(CompilePhase, u32)> {
    c.trace.lock().unwrap().clone()
}
fn limits() -> ProviderReadProjectionLimits {
    ProviderReadProjectionLimits {
        max_definitions: 1024,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 4 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 8 * 1024 * 1024,
        max_work: 64 * 1024 * 1024,
    }
}
fn binding_limits() -> ProviderBindingProjectionLimits {
    ProviderBindingProjectionLimits {
        max_definitions: 1024,
        max_allocation_requests: 8192,
        max_allocation_request_bytes: 4 * 1024 * 1024,
        max_coexisting_source_and_request_bytes: 8 * 1024 * 1024,
        max_work: 64 * 1024 * 1024,
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
fn binding() -> ConnectorReadBinding {
    ConnectorReadBinding::new(
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::try_from_canonical("lake").unwrap(),
        },
        CatalogHandle::new(
            ConnectorInstanceId::try_from_canonical("lake").unwrap(),
            CatalogVersion::from_bytes([0; 32]),
        ),
    )
}
fn payload(category: ConnectorCodecCategory, body: &[u8]) -> ConnectorEncodedPayload {
    ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            CatalogHandle::new(
                ConnectorInstanceId::try_from_canonical("lake").unwrap(),
                CatalogVersion::from_bytes([0; 32]),
            ),
            category,
            ConnectorCodecRevision::try_new(u32::MAX).unwrap(),
        ),
        body.to_vec().into(),
    )
}
fn read(kind: ConnectorReadRelationKind) -> ProviderReadReference {
    ProviderReadReference {
        binding: binding(),
        input_version: ConnectorReadInputVersion::try_new(Arc::<[u8]>::from(
            [0, 255, 1].as_slice(),
        ))
        .unwrap(),
        relation: ConnectorReadRelationPayload::new(
            kind,
            payload(ConnectorCodecCategory::ReadTable, b"table"),
            payload(ConnectorCodecCategory::ReadView, b"view"),
        ),
    }
}
fn expected(id: u32, kind: i32) -> wire::ProviderReadReferenceDefinition {
    wire::ProviderReadReferenceDefinition {
        id,
        provider_binding_id: Some(u32::MAX),
        input_version: vec![0, 255, 1],
        kind,
        table_payload_id: Some(0),
        view_payload_id: Some(u32::MAX),
    }
}
const KINDS: [ConnectorReadRelationKind; 6] = [
    ConnectorReadRelationKind::Table,
    ConnectorReadRelationKind::TableFunction,
    ConnectorReadRelationKind::ChangeWindow,
    ConnectorReadRelationKind::SystemTable,
    ConnectorReadRelationKind::TableExecute,
    ConnectorReadRelationKind::MergeTable,
];

#[test]
fn provider_read_namespace_all_six_kinds_independent_wire_sparse_ids_and_original_sources() {
    for (i, kind) in KINDS.into_iter().enumerate() {
        let source = read(kind);
        let ctrl = Control::default();
        let binding_inputs = [(u32::MAX, &source.binding)];
        let payload_inputs = [
            (0, source.relation.table()),
            (u32::MAX, source.relation.view()),
        ];
        let bindings =
            encode_provider_bindings(&binding_inputs, PRIOR_SOURCE, binding_limits(), &ctrl)
                .unwrap();
        let payloads =
            encode_connector_payloads(&payload_inputs, PRIOR_SOURCE, payload_limits(), &ctrl)
                .unwrap();
        let inputs = [(u32::MAX, &source), (0, &source)];
        let encoded =
            encode_provider_reads(&inputs, &bindings, &payloads, SOURCE, limits()).unwrap();
        assert_eq!(
            encoded.as_wire(),
            [expected(u32::MAX, i as i32 + 1), expected(0, i as i32 + 1)]
        );
        assert_eq!(encoded.source_count(), 2);
        assert!(std::ptr::eq(encoded.bindings(), &bindings));
        assert!(std::ptr::eq(encoded.payloads(), &payloads));
        assert!(std::ptr::eq(encoded.read(0).unwrap().unwrap(), &source));
        assert!(encoded.read(7).unwrap().is_none());
        assert!(
            encoded.source_id(&source).is_err(),
            "one source at two IDs must be ambiguous"
        );
        let b = decode_provider_bindings(bindings.as_wire(), PRIOR_SOURCE, binding_limits(), &ctrl)
            .unwrap();
        let p =
            decode_connector_payloads(payloads.as_wire(), PRIOR_SOURCE, payload_limits(), &ctrl)
                .unwrap();
        // Hand-authored expected definitions, not a roundtrip-only oracle.
        let raw = [expected(0, i as i32 + 1), expected(u32::MAX, i as i32 + 1)];
        let received = decode_provider_reads(&raw, &b, &p, SOURCE, limits()).unwrap();
        assert_eq!(received.read(0).unwrap(), Some(&source));
        assert_eq!(received.read(u32::MAX).unwrap(), Some(&source));
        assert!(std::ptr::eq(received.as_wire(), raw.as_slice()));
        assert!(std::ptr::eq(received.bindings(), &b));
        assert!(std::ptr::eq(received.payloads(), &p));
        assert!(encoded.retained_invoice_floor().unwrap() > SOURCE);
        assert!(received.retained_invoice_floor().unwrap() > SOURCE);
    }
}
#[test]
fn provider_read_namespace_requires_unique_exact_owner_and_same_original_control() {
    let source = read(ConnectorReadRelationKind::Table);
    let foreign = source.clone();
    let ctrl = Control::default();
    let other = Control::default();
    let binding_inputs = [(u32::MAX, &source.binding)];
    let payload_inputs = [
        (0, source.relation.table()),
        (u32::MAX, source.relation.view()),
    ];
    let bindings =
        encode_provider_bindings(&binding_inputs, PRIOR_SOURCE, binding_limits(), &ctrl).unwrap();
    let payloads =
        encode_connector_payloads(&payload_inputs, PRIOR_SOURCE, payload_limits(), &ctrl).unwrap();
    let inputs = [(0, &source)];
    let encoded = encode_provider_reads(&inputs, &bindings, &payloads, SOURCE, limits()).unwrap();
    assert_eq!(encoded.source_id(&source).unwrap(), 0);
    assert!(encoded.source_id(&foreign).is_err());
    let foreign_inputs = [(0, &foreign)];
    assert!(matches!(
        encode_provider_reads(&foreign_inputs, &bindings, &payloads, SOURCE, limits()),
        Err(Error::Binding(_))
    ));
    let aliases = [
        (0, source.relation.table()),
        (2, source.relation.table()),
        (u32::MAX, source.relation.view()),
    ];
    let p_alias =
        encode_connector_payloads(&aliases, PRIOR_SOURCE, payload_limits(), &ctrl).unwrap();
    assert!(matches!(
        encode_provider_reads(&inputs, &bindings, &p_alias, SOURCE, limits()),
        Err(Error::Payload(_))
    ));
    let b_alias_inputs = [(0, &source.binding), (u32::MAX, &source.binding)];
    let b_alias =
        encode_provider_bindings(&b_alias_inputs, PRIOR_SOURCE, binding_limits(), &ctrl).unwrap();
    assert!(matches!(
        encode_provider_reads(&inputs, &b_alias, &payloads, SOURCE, limits()),
        Err(Error::Binding(_))
    ));
    let replacement =
        encode_connector_payloads(&payload_inputs, PRIOR_SOURCE, payload_limits(), &other).unwrap();
    ctrl.arm(None);
    let other_before = trace(&other);
    assert!(matches!(
        encode_provider_reads(&inputs, &bindings, &replacement, SOURCE, limits()),
        Err(Error::InvalidShape(
            "provider read namespaces have different original controls"
        ))
    ));
    assert_eq!(
        trace(&other),
        other_before,
        "replacement callback owner was invoked"
    );
    let b = decode_provider_bindings(bindings.as_wire(), PRIOR_SOURCE, binding_limits(), &ctrl)
        .unwrap();
    let p = decode_connector_payloads(payloads.as_wire(), PRIOR_SOURCE, payload_limits(), &other)
        .unwrap();
    let raw = [expected(0, 1)];
    ctrl.arm(None);
    let other_before = trace(&other);
    assert!(matches!(
        decode_provider_reads(&raw, &b, &p, SOURCE, limits()),
        Err(Error::InvalidShape(
            "provider read namespaces have different original controls"
        ))
    ));
    assert_eq!(trace(&other), other_before);
}
#[test]
fn provider_read_namespace_receiving_presence_kind_ids_and_sole_version_constructor() {
    let source = read(ConnectorReadRelationKind::Table);
    let ctrl = Control::default();
    let bi = [(u32::MAX, &source.binding)];
    let pi = [
        (0, source.relation.table()),
        (u32::MAX, source.relation.view()),
    ];
    let eb = encode_provider_bindings(&bi, PRIOR_SOURCE, binding_limits(), &ctrl).unwrap();
    let ep = encode_connector_payloads(&pi, PRIOR_SOURCE, payload_limits(), &ctrl).unwrap();
    let b = decode_provider_bindings(eb.as_wire(), PRIOR_SOURCE, binding_limits(), &ctrl).unwrap();
    let p = decode_connector_payloads(ep.as_wire(), PRIOR_SOURCE, payload_limits(), &ctrl).unwrap();
    for which in 0..3 {
        let mut def = expected(0, 1);
        match which {
            0 => def.provider_binding_id = None,
            1 => def.table_payload_id = None,
            _ => def.view_payload_id = None,
        };
        assert!(matches!(
            decode_provider_reads(&[def], &b, &p, SOURCE, limits()),
            Err(Error::InvalidShape("provider read reference ID is absent"))
        ));
    }
    for which in 0..3 {
        let mut def = expected(0, 1);
        match which {
            0 => def.provider_binding_id = Some(42),
            1 => def.table_payload_id = Some(42),
            _ => def.view_payload_id = Some(42),
        };
        assert!(matches!(
            decode_provider_reads(&[def], &b, &p, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
    }
    for kind in [0, -1, 7, i32::MAX] {
        assert!(matches!(
            decode_provider_reads(&[expected(0, kind)], &b, &p, SOURCE, limits()),
            Err(Error::InvalidShape(_))
        ));
    }
    for version in [vec![], vec![0; 4097]] {
        let mut def = expected(0, 1);
        def.input_version = version;
        let error = decode_provider_reads(&[def], &b, &p, SOURCE, limits())
            .err()
            .unwrap();
        match error {
            Error::Contract(error) => {
                assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
                assert_eq!(
                    error.message(),
                    "connector read input version must be non-empty and bounded"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
    let mut max = expected(u32::MAX, 6);
    max.input_version = vec![255; 4096];
    let raw = [max];
    let received = decode_provider_reads(&raw, &b, &p, SOURCE, limits()).unwrap();
    assert_eq!(
        received
            .read(u32::MAX)
            .unwrap()
            .unwrap()
            .input_version
            .as_bytes(),
        &[255; 4096]
    );
    let duplicates = [expected(0, 1), expected(0, 2)];
    assert!(matches!(
        decode_provider_reads(&duplicates, &b, &p, SOURCE, limits()),
        Err(Error::Index(_))
    ));
}
#[test]
fn provider_read_namespace_neutral_purpose_and_binding_mismatch_are_preserved_for_fragment_owner() {
    let mut source = read(ConnectorReadRelationKind::Table);
    source.binding = ConnectorReadBinding::new(
        source.binding.descriptor().clone(),
        CatalogHandle::new(
            ConnectorInstanceId::try_from_canonical("different_catalog").unwrap(),
            CatalogVersion::from_bytes([0; 32]),
        ),
    );
    source.relation = ConnectorReadRelationPayload::new(
        ConnectorReadRelationKind::Table,
        payload(ConnectorCodecCategory::WriteHandle, b"not a read table"),
        payload(ConnectorCodecCategory::CommitFragment, b"not a read view"),
    );
    let ctrl = Control::default();
    let bi = [(u32::MAX, &source.binding)];
    let pi = [
        (0, source.relation.table()),
        (u32::MAX, source.relation.view()),
    ];
    let b = encode_provider_bindings(&bi, PRIOR_SOURCE, binding_limits(), &ctrl).unwrap();
    let p = encode_connector_payloads(&pi, PRIOR_SOURCE, payload_limits(), &ctrl).unwrap();
    let inputs = [(0, &source)];
    let encoded = encode_provider_reads(&inputs, &b, &p, SOURCE, limits()).unwrap();
    assert_eq!(encoded.as_wire(), [expected(0, 1)]);
    let db = decode_provider_bindings(b.as_wire(), PRIOR_SOURCE, binding_limits(), &ctrl).unwrap();
    let dp = decode_connector_payloads(p.as_wire(), PRIOR_SOURCE, payload_limits(), &ctrl).unwrap();
    let raw = [expected(0, 1)];
    let decoded = decode_provider_reads(&raw, &db, &dp, SOURCE, limits()).unwrap();
    assert_eq!(
        decoded
            .read(0)
            .unwrap()
            .unwrap()
            .relation
            .table()
            .header()
            .category(),
        ConnectorCodecCategory::WriteHandle
    );
    assert_eq!(
        decoded
            .read(0)
            .unwrap()
            .unwrap()
            .relation
            .view()
            .header()
            .category(),
        ConnectorCodecCategory::CommitFragment
    );
    let read = decoded.read(0).unwrap().unwrap();
    assert_ne!(
        read.binding.catalog_handle(),
        read.relation.table().header().catalog()
    );
}
fn constrained(f: &ProviderReadProjectionFacts) -> ProviderReadProjectionLimits {
    ProviderReadProjectionLimits {
        max_definitions: f.definition_count,
        max_allocation_requests: f.allocation_requests_upper_bound,
        max_allocation_request_bytes: f.allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: f.coexisting_source_and_request_bytes_upper_bound,
        max_work: f.cumulative_work_upper_bound,
    }
}
fn under(mut l: ProviderReadProjectionLimits, which: usize) -> ProviderReadProjectionLimits {
    match which {
        0 => l.max_definitions -= 1,
        1 => l.max_allocation_requests -= 1,
        2 => l.max_allocation_request_bytes -= 1,
        3 => l.max_coexisting_source_and_request_bytes -= 1,
        _ => l.max_work -= 1,
    };
    l
}
#[test]
fn provider_read_namespace_five_exact_caps_source_capacity_and_checked_arithmetic_precede_requests()
{
    let source = read(ConnectorReadRelationKind::Table);
    let ctrl = Control::default();
    let bi = [(u32::MAX, &source.binding)];
    let pi = [
        (0, source.relation.table()),
        (u32::MAX, source.relation.view()),
    ];
    let b = encode_provider_bindings(&bi, PRIOR_SOURCE, binding_limits(), &ctrl).unwrap();
    let p = encode_connector_payloads(&pi, PRIOR_SOURCE, payload_limits(), &ctrl).unwrap();
    let inputs = [(0, &source)];
    let encoded = encode_provider_reads(&inputs, &b, &p, SOURCE, limits()).unwrap();
    assert_eq!(encoded.facts().allocation_requests_upper_bound, 3);
    let exact = constrained(encoded.facts());
    encode_provider_reads(&inputs, &b, &p, SOURCE, exact).unwrap();
    for which in 0..5 {
        assert!(matches!(
            encode_provider_reads(&inputs, &b, &p, SOURCE, under(exact, which)),
            Err(Error::InvalidShape(_))
        ));
    }
    let db = decode_provider_bindings(b.as_wire(), PRIOR_SOURCE, binding_limits(), &ctrl).unwrap();
    let dp = decode_connector_payloads(p.as_wire(), PRIOR_SOURCE, payload_limits(), &ctrl).unwrap();
    let raw = [expected(0, 1)];
    let decoded = decode_provider_reads(&raw, &db, &dp, SOURCE, limits()).unwrap();
    assert_eq!(decoded.facts().allocation_requests_upper_bound, 6);
    let exact = constrained(decoded.facts());
    decode_provider_reads(&raw, &db, &dp, SOURCE, exact).unwrap();
    for which in 0..5 {
        assert!(matches!(
            decode_provider_reads(&raw, &db, &dp, SOURCE, under(exact, which)),
            Err(Error::InvalidShape(_))
        ));
    }
    assert!(matches!(
        encode_provider_reads(&inputs, &b, &p, 0, limits()),
        Err(Error::InvalidShape(_))
    ));
    let mut spare = expected(0, 1);
    spare.input_version.reserve_exact(2 * SOURCE);
    assert!(spare.input_version.capacity() > SOURCE);
    assert!(matches!(
        decode_provider_reads(&[spare], &db, &dp, SOURCE, limits()),
        Err(Error::InvalidShape(
            "provider read source invoice omits original backing"
        ))
    ));
    assert!(bound(usize::MAX, 1, 1, 0, &Requests::default()).is_err());
    assert!(bytes::<ProviderReadReference>(usize::MAX).is_err());
    assert!(mul(usize::MAX, 2).is_err());
    assert!(add(usize::MAX, 1).is_err());
    let empty = encode_provider_reads(&[], &b, &p, SOURCE, limits()).unwrap();
    assert_eq!(empty.source_count(), 0);
    let empty_decoded = decode_provider_reads(&[], &db, &dp, SOURCE, limits()).unwrap();
    assert_eq!(empty_decoded.source_count(), 0);
}

#[test]
fn provider_read_namespace_frozen_profile_preserves_wide_version_facts_in_both_paths() {
    let sources: Vec<_> = (0..370_u64)
        .map(|ordinal| {
            let mut source = read(ConnectorReadRelationKind::Table);
            let mut version = [0x5a; 32];
            version[..8].copy_from_slice(&ordinal.to_be_bytes());
            source.input_version = ConnectorReadInputVersion::try_new(version.as_slice()).unwrap();
            source
        })
        .collect();
    let bindings: Vec<_> = sources
        .iter()
        .enumerate()
        .map(|(ordinal, source)| (ordinal as u32, &source.binding))
        .collect();
    let payloads: Vec<_> = sources
        .iter()
        .enumerate()
        .flat_map(|(ordinal, source)| {
            [
                (ordinal as u32 * 2, source.relation.table()),
                (ordinal as u32 * 2 + 1, source.relation.view()),
            ]
        })
        .collect();
    let inputs: Vec<_> = sources
        .iter()
        .enumerate()
        .map(|(ordinal, source)| (u32::MAX - ordinal as u32, source))
        .collect();
    let control = Control::default();
    let bindings =
        encode_provider_bindings(&bindings, PRIOR_SOURCE, binding_limits(), &control).unwrap();
    let payloads =
        encode_connector_payloads(&payloads, PRIOR_SOURCE, payload_limits(), &control).unwrap();
    let limits = crate::physical_package_v2::PackageEncodeLimits::FROZEN.reads;
    let receiver_limits = crate::physical_package_v2::PackageDecodeLimits::FROZEN.reads;
    let total_version_bytes = sources
        .iter()
        .map(|source| source.input_version.as_bytes().len())
        .sum::<usize>();
    assert_eq!(total_version_bytes, 11_840);
    assert!(total_version_bytes > 1024);

    let encoded = encode_provider_reads(&inputs, &bindings, &payloads, SOURCE, limits).unwrap();
    assert_eq!(encoded.facts().input_version_bytes, total_version_bytes);
    assert_eq!(encoded.source_count(), sources.len());
    for (id, original) in &inputs {
        assert!(std::ptr::eq(encoded.read(*id).unwrap().unwrap(), *original));
    }
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let mut snapshots = Vec::new();
    let observed = encode_provider_reads_in(
        &inputs,
        &bindings,
        &payloads,
        SOURCE,
        limits,
        &mut |facts| {
            snapshots.push(*facts);
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    assert_eq!(observed.as_wire(), encoded.as_wire());
    assert_eq!(snapshots.last(), Some(observed.facts()));
    assert_eq!(observed.facts().input_version_bytes, total_version_bytes);
    work.finish().unwrap();

    let decoded_bindings =
        decode_provider_bindings(bindings.as_wire(), PRIOR_SOURCE, binding_limits(), &control)
            .unwrap();
    let decoded_payloads =
        decode_connector_payloads(payloads.as_wire(), PRIOR_SOURCE, payload_limits(), &control)
            .unwrap();
    let decoded = decode_provider_reads(
        encoded.as_wire(),
        &decoded_bindings,
        &decoded_payloads,
        SOURCE,
        receiver_limits,
    )
    .unwrap();
    assert_eq!(decoded.facts().input_version_bytes, total_version_bytes);
    for (id, original) in &inputs {
        assert_eq!(decoded.read(*id).unwrap(), Some(*original));
    }
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut snapshots = Vec::new();
    let observed = decode_provider_reads_in(
        encoded.as_wire(),
        &decoded_bindings,
        &decoded_payloads,
        SOURCE,
        receiver_limits,
        &mut |facts| {
            snapshots.push(*facts);
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    assert_eq!(snapshots.last(), Some(observed.facts()));
    assert_eq!(observed.facts().input_version_bytes, total_version_bytes);
    for (id, original) in &inputs {
        assert_eq!(observed.read_in(*id, &mut work).unwrap(), Some(*original));
    }
    work.finish().unwrap();
}

fn run(
    control: &Control,
    stop: Option<(usize, CompileControlError)>,
    decode: bool,
    bad_version: bool,
    bad_kind: bool,
) -> Result<(), Error> {
    let source = read(ConnectorReadRelationKind::Table);
    let bi = [(u32::MAX, &source.binding)];
    let pi = [
        (0, source.relation.table()),
        (u32::MAX, source.relation.view()),
    ];
    let b = encode_provider_bindings(&bi, PRIOR_SOURCE, binding_limits(), control).unwrap();
    let p = encode_connector_payloads(&pi, PRIOR_SOURCE, payload_limits(), control).unwrap();
    if decode {
        let db =
            decode_provider_bindings(b.as_wire(), PRIOR_SOURCE, binding_limits(), control).unwrap();
        let dp = decode_connector_payloads(p.as_wire(), PRIOR_SOURCE, payload_limits(), control)
            .unwrap();
        let mut raw = expected(0, if bad_kind { 0 } else { 1 });
        if bad_version {
            raw.input_version.clear();
        }
        let defs = [raw];
        control.arm(stop);
        decode_provider_reads(&defs, &db, &dp, SOURCE, limits()).map(|_| ())
    } else {
        let foreign = source.clone();
        let inputs = [(0, if bad_kind { &foreign } else { &source })];
        control.arm(stop);
        encode_provider_reads(&inputs, &b, &p, SOURCE, limits()).map(|_| ())
    }
}
#[test]
fn provider_read_namespace_every_actual_callback_three_causes_success_ordinary_and_ctor_tail() {
    for (decode, bad_version, bad_kind) in [
        (false, false, false),
        (false, false, true),
        (true, false, false),
        (true, false, true),
        (true, true, false),
    ] {
        let ctrl = Control::default();
        let outcome = run(&ctrl, None, decode, bad_version, bad_kind);
        assert_eq!(outcome.is_ok(), !bad_version && !bad_kind);
        let positive = trace(&ctrl);
        assert!(!positive.is_empty());
        assert!(positive.iter().all(|(phase, _)| *phase
            == if decode {
                CompilePhase::Decode
            } else {
                CompilePhase::Encode
            }));
        for at in 0..positive.len() {
            for cause in CAUSES {
                let refusing = Control::default();
                let outcome = run(
                    &refusing,
                    Some((at, cause.clone())),
                    decode,
                    bad_version,
                    bad_kind,
                );
                assert!(
                    matches!(&outcome,Err(Error::Control(c)) if c==&cause),
                    "{outcome:?}"
                );
                assert_eq!(trace(&refusing), positive[..=at]);
            }
        }
    }
}
#[test]
fn provider_read_namespace_real_wide_original_quantum_and_lookup_tail_keep_exact_cause() {
    let ctrl = Control::default();
    let source = read(ConnectorReadRelationKind::Table);
    let bi = [(u32::MAX, &source.binding)];
    let pi = [
        (0, source.relation.table()),
        (u32::MAX, source.relation.view()),
    ];
    let b = encode_provider_bindings(&bi, PRIOR_SOURCE, binding_limits(), &ctrl).unwrap();
    let p = encode_connector_payloads(&pi, PRIOR_SOURCE, payload_limits(), &ctrl).unwrap();
    let inputs = (0..320)
        .map(|i| (u32::MAX - i, &source))
        .collect::<Vec<_>>();
    ctrl.arm(None);
    let encoded = encode_provider_reads(&inputs, &b, &p, SOURCE, limits()).unwrap();
    let positive = trace(&ctrl);
    let at = positive
        .iter()
        .position(|(_, units)| *units == 256)
        .expect("genuine read namespace quantum");
    for cause in CAUSES {
        ctrl.arm(Some((at, cause.clone())));
        assert!(
            matches!(encode_provider_reads(&inputs,&b,&p,SOURCE,limits()),Err(Error::Control(c)) if c==cause)
        );
        assert_eq!(trace(&ctrl), positive[..=at]);
    }
    ctrl.arm(None);
    assert!(std::ptr::eq(
        encoded.read(u32::MAX).unwrap().unwrap(),
        &source
    ));
    let lookup = trace(&ctrl);
    for stop in 0..lookup.len() {
        for cause in CAUSES {
            ctrl.arm(Some((stop, cause.clone())));
            assert!(matches!(encoded.read(u32::MAX),Err(Error::Control(c)) if c==cause));
            assert_eq!(trace(&ctrl), lookup[..=stop]);
        }
    }
    ctrl.arm(None);
    let db = decode_provider_bindings(b.as_wire(), PRIOR_SOURCE, binding_limits(), &ctrl).unwrap();
    let dp = decode_connector_payloads(p.as_wire(), PRIOR_SOURCE, payload_limits(), &ctrl).unwrap();
    let defs = (0..320)
        .map(|i| expected(u32::MAX - i, 1))
        .collect::<Vec<_>>();
    ctrl.arm(None);
    decode_provider_reads(&defs, &db, &dp, SOURCE, limits()).unwrap();
    let positive = trace(&ctrl);
    let at = positive
        .iter()
        .position(|(_, units)| *units == 256)
        .expect("genuine receiving quantum");
    for cause in CAUSES {
        ctrl.arm(Some((at, cause.clone())));
        assert!(
            matches!(decode_provider_reads(&defs,&db,&dp,SOURCE,limits()),Err(Error::Control(c)) if c==cause)
        );
        assert_eq!(trace(&ctrl), positive[..=at]);
    }
}

#[test]
fn provider_read_namespace_consumes_joint_read_write_ids_without_rebinding_read_sources() {
    use crate::physical_provider_binding_v2::{
        ProviderBindingSource, decode_joint_provider_bindings, encode_joint_provider_bindings,
    };
    use novarocks_connector_contract::ConnectorWriteBinding;

    for (at, kind) in KINDS.into_iter().enumerate() {
        let source = read(kind);
        let write = ConnectorWriteBinding::new(
            source.binding.descriptor().clone(),
            source.binding.catalog_handle().clone(),
        );
        let control = Control::default();
        let mixed = [
            (0, ProviderBindingSource::Write(&write)),
            (u32::MAX, ProviderBindingSource::Read(&source.binding)),
        ];
        let payload_inputs = [
            (0, source.relation.table()),
            (u32::MAX, source.relation.view()),
        ];
        let bindings =
            encode_joint_provider_bindings(&mixed, PRIOR_SOURCE, binding_limits(), &control)
                .unwrap();
        let payloads =
            encode_connector_payloads(&payload_inputs, PRIOR_SOURCE, payload_limits(), &control)
                .unwrap();
        let inputs = [(0, &source)];
        let projected =
            encode_provider_reads(&inputs, &bindings, &payloads, SOURCE, limits()).unwrap();
        let raw = [expected(0, at as i32 + 1)];
        assert_eq!(projected.as_wire(), raw);
        assert!(std::ptr::eq(projected.bindings(), &bindings));
        assert!(std::ptr::eq(
            bindings.write_binding(0).unwrap().unwrap(),
            &write
        ));
        assert!(bindings.binding(0).unwrap().is_none());

        let received_bindings = decode_joint_provider_bindings(
            bindings.as_wire(),
            PRIOR_SOURCE,
            binding_limits(),
            &control,
        )
        .unwrap();
        let received_payloads =
            decode_connector_payloads(payloads.as_wire(), PRIOR_SOURCE, payload_limits(), &control)
                .unwrap();
        let received = decode_provider_reads(
            &raw,
            &received_bindings,
            &received_payloads,
            SOURCE,
            limits(),
        )
        .unwrap();
        assert_eq!(received.read(0).unwrap(), Some(&source));
        assert!(std::ptr::eq(received.bindings(), &received_bindings));
        // Equal neutral metadata at a write source is not the original read
        // source loan. The encoder cannot select it as a convenient fallback.
        let only_write = [(u32::MAX, ProviderBindingSource::Write(&write))];
        let foreign_role =
            encode_joint_provider_bindings(&only_write, PRIOR_SOURCE, binding_limits(), &control)
                .unwrap();
        assert!(
            encode_provider_reads(&inputs, &foreign_role, &payloads, SOURCE, limits(),).is_err()
        );
    }
}

#[path = "tests/caller_owned_tests.rs"]
mod caller_owned_tests;
