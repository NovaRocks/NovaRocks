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

use super::{envelope::*, keys::*, limits::*, record::*, view::*};
use crate::iceberg::spec::{
    FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, TableMetadata,
    TableMetadataBuilder, Type,
};
use bytes::Bytes;
use novarocks_spi::connector::*;
use std::collections::{BTreeMap, HashMap};

const GOLDEN_ID: &str = "b4e51690c34c67348567bf924b8bb5d41ed7f2dc43621d985e352facb425b17e";
const GOLDEN_VERSION: &str = r#"{"v":2,"owner":"novarocks.mv","name":"definition","format":{"owner":"novarocks.mv","name":"definition","version":1},"refs":[],"content":"YWJj","id":"b4e51690c34c67348567bf924b8bb5d41ed7f2dc43621d985e352facb425b17e"}"#;
const GOLDEN_SLOT: &str = r#"{"v":2,"owner":"novarocks.mv","name":"configuration","format":{"owner":"novarocks.mv","name":"definition","version":1},"writer":7,"content":"YWJj","digest":"cc51b62de09961dd9e155df07e266acbf053af1195934292386054f37fdfe689"}"#;
const GOLDEN_MARKER: &str = r#"{"v":2,"owner":"novarocks.mv","name":"pending","binding":"attempt-1","format":{"owner":"novarocks.mv","name":"definition","version":1},"content":"YWJj","digest":"52ba21e0ca6e5859ebcd762f8817d0ee782471aaff67999b086c5df63d9b0f39"}"#;

fn format() -> ConnectorDocumentFormat {
    ConnectorDocumentFormat::try_new("novarocks.mv", "definition", 1).unwrap()
}
fn version() -> ConnectorDocumentVersion {
    ConnectorDocumentVersion::try_new(
        "novarocks.mv",
        "definition",
        format(),
        vec![],
        Bytes::from_static(b"abc"),
    )
    .unwrap()
}
fn record(binding: Option<DocumentIdentity>) -> ConnectorSnapshotRecord {
    ConnectorSnapshotRecord {
        owner: "novarocks.mv".into(),
        name: "publication".into(),
        binding,
        payload: Bytes::from_static(b"abc"),
    }
}
fn slot_name() -> ConnectorSlotName {
    ConnectorSlotName::try_new("novarocks.mv", "configuration").unwrap()
}
fn marker_name() -> ConnectorMarkerKey {
    ConnectorMarkerKey {
        owner: "novarocks.mv".into(),
        name: "pending".into(),
        binding: "attempt-1".into(),
    }
}
fn metadata(properties: HashMap<String, String>) -> TableMetadata {
    let schema = Schema::builder()
        .with_fields(vec![std::sync::Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .unwrap();
    TableMetadataBuilder::new(
        schema,
        PartitionSpec::unpartition_spec(),
        SortOrder::unsorted_order(),
        "memory://warehouse/table".into(),
        FormatVersion::V2,
        properties,
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata
}
fn with_snapshots(
    metadata: TableMetadata,
    summaries: Vec<HashMap<String, String>>,
) -> TableMetadata {
    let mut json = serde_json::to_value(metadata).unwrap();
    let snapshots:Vec<_>=summaries.into_iter().enumerate().map(|(i,summary)| {
        let mut summary=serde_json::to_value(summary).unwrap(); summary["operation"]=serde_json::json!("append");
        let mut snapshot=serde_json::json!({"snapshot-id":(i+1) as i64,"sequence-number":(i+1) as i64,"timestamp-ms":(i+1) as i64,"manifest-list":"memory://manifest-list","summary":summary});
        if i>0 {snapshot["parent-snapshot-id"]=serde_json::json!(i as i64);}
        snapshot
    }).collect();
    let last = snapshots.len() as i64;
    json["snapshots"] = serde_json::json!(snapshots);
    json["current-snapshot-id"] = serde_json::json!(last);
    json["last-sequence-number"] = serde_json::json!(last);
    json["refs"] = serde_json::json!({"main":{"snapshot-id":last,"type":"branch"}});
    serde_json::from_value(json).unwrap()
}
fn request(metadata: &TableMetadata) -> KeyedDocumentObservationRequest {
    let binding = ConnectorProviderBinding::iceberg("test", [1; 16], "test").unwrap();
    KeyedDocumentObservationRequest {
        owner: binding.binding_key().clone(),
        catalog_handle: CatalogHandle::new(
            binding.binding_key().instance_id.clone(),
            CatalogVersion::from_bytes([2; 32]),
        ),
        target: ConnectorTableIdentity {
            instance_id: binding.binding_key().instance_id.clone(),
            namespace: "ns".into(),
            table: "table".into(),
        },
        object_id: ConnectorTableObjectId::try_new(Bytes::from(metadata.uuid().to_string()))
            .unwrap(),
        context: ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            ConnectorStopOwner::default().view(),
            1024,
            4096,
        )
        .unwrap(),
    }
}
fn committed(metadata: &TableMetadata) -> ConnectorCommittedVersion {
    ConnectorCommittedVersion::try_new(
        Bytes::from_static(b"metadata-version"),
        metadata.current_snapshot_id(),
    )
    .unwrap()
}
fn properties() -> HashMap<String, String> {
    HashMap::from([
        (version_key(version().identity()), GOLDEN_VERSION.into()),
        (root_key("novarocks.mv"), GOLDEN_ID.into()),
    ])
}

#[test]
fn manual_golden_version_and_root() {
    let v = version();
    assert_eq!(v.identity().to_hex(), GOLDEN_ID);
    assert_eq!(encode_version(&v).unwrap(), GOLDEN_VERSION);
    assert_eq!(
        decode_version(&format!("novarocks.doc.v2.rev.{GOLDEN_ID}"), GOLDEN_VERSION).unwrap(),
        v
    );
    assert_eq!(
        root_key("novarocks.mv"),
        "novarocks.doc.v2.root.novarocks.mv"
    );
}
#[test]
fn manual_golden_slot_and_marker() {
    let slot = ConnectorSlotValue {
        format: format(),
        content: Bytes::from_static(b"abc"),
    };
    assert_eq!(
        encode_slot(&slot_name(), &slot, Some(7)).unwrap(),
        GOLDEN_SLOT
    );
    assert_eq!(
        decode_slot(&slot_key(&slot_name()), GOLDEN_SLOT).unwrap().1,
        ConnectorObservedSlot {
            value: slot,
            writer: Some(7)
        }
    );
    let marker = ConnectorMarkerValue {
        format: format(),
        content: Bytes::from_static(b"abc"),
    };
    assert_eq!(
        encode_marker(&marker_name(), &marker).unwrap(),
        GOLDEN_MARKER
    );
    assert_eq!(
        decode_marker(&marker_key(&marker_name()), GOLDEN_MARKER)
            .unwrap()
            .1,
        marker
    );
}
#[test]
fn manual_golden_record_frame() {
    let record = record(Some(version().identity()));
    let frame = format!("2.{GOLDEN_ID}.YWJj");
    assert_eq!(encode_record(&record).unwrap(), frame);
    assert_eq!(
        decode_record("novarocks.doc.v2.record.novarocks.mv~publication", &frame).unwrap(),
        record
    );
    let unbound = super::tests::record(None);
    assert_eq!(encode_record(&unbound).unwrap(), "2.-.YWJj");
    assert_eq!(
        decode_record(&record_key(&unbound.owner, &unbound.name), "2.-.YWJj").unwrap(),
        unbound
    );
}
#[test]
fn identity_and_encoding_include_ordered_references() {
    let a = ConnectorDocumentVersion::try_new(
        "novarocks.mv",
        "binding",
        format(),
        vec![("definition".into(), version().identity())],
        Bytes::new(),
    )
    .unwrap();
    let b = ConnectorDocumentVersion::try_new(
        "novarocks.mv",
        "binding",
        format(),
        vec![("definition".into(), DocumentIdentity::from_bytes([4; 32]))],
        Bytes::new(),
    )
    .unwrap();
    assert_ne!(a.identity(), b.identity());
    assert_ne!(version_key(a.identity()), version_key(b.identity()));
    assert_eq!(
        encode_version(&a).unwrap(),
        encode_version(&a.clone()).unwrap()
    );
}
#[test]
fn invalid_keys_and_locations_fail_closed() {
    for key in [
        "novarocks.doc.v3.root.owner",
        "novarocks.doc.v2.unknown.a",
        "novarocks.doc.v2.slot.owner~bad~extra",
        "novarocks.doc.v2.root.Owner",
        "novarocks.doc.v2.root.a~b",
        "novarocks.doc.v2.rev.AA",
    ] {
        assert!(parse_key(key, false).is_err(), "{key}");
    }
    assert!(parse_key("novarocks.doc.v2.record.owner~name", false).is_err());
    assert!(parse_key("novarocks.doc.v2.root.owner", true).is_err());
    assert!(
        parse_key("novarocks.ctas.operation-id", false)
            .unwrap()
            .is_none()
    );
}
#[test]
fn envelopes_reject_noncanonical_and_tampered_bytes() {
    let key = version_key(version().identity());
    for bytes in [
        format!(" {GOLDEN_VERSION}"),
        GOLDEN_VERSION.replace("\"v\":2", "\"v\":3"),
        GOLDEN_VERSION.replace("YWJj", "YWJk"),
        GOLDEN_VERSION.replace("YWJj", "YWJj="),
        GOLDEN_VERSION.replace("\"v\":2", "\"v\":2,\"unknown\":1"),
        GOLDEN_VERSION.replace("\"v\":2", "\"v\":2,\"v\":2"),
    ] {
        assert!(decode_version(&key, &bytes).is_err(), "{bytes}");
    }
    assert!(
        decode_version(
            &version_key(DocumentIdentity::from_bytes([9; 32])),
            GOLDEN_VERSION
        )
        .is_err()
    );
    assert!(
        decode_slot(
            &slot_key(&slot_name()),
            &GOLDEN_SLOT.replace("\"writer\":7", "\"writer\":8")
        )
        .is_err()
    );
    assert!(
        decode_marker(
            &marker_key(&marker_name()),
            &GOLDEN_MARKER.replace("attempt-1", "attempt-2")
        )
        .is_err()
    );
}
#[test]
fn frames_reject_unknown_trailing_or_noncanonical_bytes() {
    for frame in [
        "3.-.YWJj",
        "2.-.YWJj.extra",
        "2.-.YWJj=",
        "2.-.Zh",
        "2.AA.YWJj",
    ] {
        assert!(
            decode_record("novarocks.doc.v2.record.a~b", frame).is_err(),
            "{frame}"
        );
    }
}
#[test]
fn current_closure_missing_is_fatal_and_root_missing_is_dangling() {
    let missing = DocumentIdentity::from_bytes([8; 32]);
    let mut props = properties();
    props.insert(root_key("novarocks.mv"), missing.to_hex());
    let base = metadata(props);
    let view = project_view(&base, &request(&base), committed(&base)).unwrap();
    assert!(view.roots["novarocks.mv"].dangling);
    let base = with_snapshots(
        base,
        vec![HashMap::from([(
            record_key("novarocks.mv", "publication"),
            encode_record(&record(Some(missing))).unwrap(),
        )])],
    );
    let err = project_view(&base, &request(&base), committed(&base)).unwrap_err();
    assert!(
        err.to_string().contains("CorruptData") || err.to_string().contains("missing referenced")
    );
}
#[test]
fn historical_missing_reference_is_diagnostic_and_token_is_exact() {
    let missing = DocumentIdentity::from_bytes([8; 32]);
    let base = with_snapshots(
        metadata(properties()),
        vec![
            HashMap::from([(
                record_key("novarocks.mv", "publication"),
                encode_record(&record(Some(missing))).unwrap(),
            )]),
            HashMap::from([(
                record_key("novarocks.mv", "publication"),
                encode_record(&record(Some(version().identity()))).unwrap(),
            )]),
        ],
    );
    let request = request(&base);
    let view = project_full_view(&base, &request, committed(&base)).unwrap();
    assert_eq!(view.diagnostics.len(), 1);
    assert!(view.diagnostics[0].contains("snapshot 1"));
    assert_eq!(view.current.current_records.len(), 1);
    assert_eq!(view.token.owner(), &request.owner);
    assert_eq!(view.token.table(), &request.target);
    assert_eq!(view.token.object_id(), &request.object_id);
    // The payload is exactly this loaded metadata's serialization. The SDK's
    // internal snapshot HashMap may serialize in a different order after decode,
    // while TableMetadata equality correctly compares those snapshots by ID.
    assert_eq!(
        view.token.payload().as_ref(),
        serde_json::to_vec(&base).unwrap()
    );
    assert_eq!(metadata_from_token(&view.token).unwrap(), base);
}
#[test]
fn historical_structural_damage_is_fatal() {
    let base = with_snapshots(
        metadata(properties()),
        vec![
            HashMap::from([(record_key("a", "b"), "bad-frame".into())]),
            HashMap::new(),
        ],
    );
    assert!(project_full_view(&base, &request(&base), committed(&base)).is_err());
}
#[test]
fn legacy_is_typed_on_properties_and_current_summary() {
    for key in [
        "novarocks.documents.v1",
        "novarocks.internal.pending-documents.v1",
        "novarocks.documents.preparation-digest.v1",
        "novarocks.documents.update.operation-id.v1",
        "novarocks.managed.owner",
    ] {
        let base = metadata(HashMap::from([(key.into(), "legacy".into())]));
        assert!(matches!(
            project_view(&base, &request(&base), committed(&base)),
            Err(KeyedDocumentReadError::LegacyFormat { .. })
        ));
        let base = with_snapshots(
            metadata(HashMap::new()),
            vec![HashMap::from([(key.into(), "legacy".into())])],
        );
        assert!(matches!(
            project_view(&base, &request(&base), committed(&base)),
            Err(KeyedDocumentReadError::LegacyFormat { .. })
        ));
    }
}
#[test]
fn totals_need_both_fields_and_external_current_record_is_empty() {
    let base = with_snapshots(
        metadata(properties()),
        vec![HashMap::from([
            ("total-data-files".into(), "0".into()),
            ("total-delete-files".into(), "0".into()),
        ])],
    );
    let view = project_view(&base, &request(&base), committed(&base)).unwrap();
    assert!(view.current_snapshot_file_totals.is_empty());
    assert!(view.current_records.is_empty());
    let base = with_snapshots(
        metadata(properties()),
        vec![HashMap::from([("total-data-files".into(), "0".into())])],
    );
    assert!(
        !project_view(&base, &request(&base), committed(&base))
            .unwrap()
            .current_snapshot_file_totals
            .is_empty()
    );
}
#[test]
fn management_content_is_exact_lp32_and_rejects_trailing() {
    let mut body = Vec::new();
    for value in ["materialized-view", "owner", "incarnation"] {
        body.extend_from_slice(&(value.len() as u32).to_be_bytes());
        body.extend_from_slice(value.as_bytes());
    }
    let name = ConnectorSlotName::try_new("novarocks.storage", "management").unwrap();
    let value = ConnectorSlotValue {
        format: format(),
        content: Bytes::from(body.clone()),
    };
    let base = metadata(HashMap::from([(
        slot_key(&name),
        encode_slot(&name, &value, None).unwrap(),
    )]));
    let view = project_view(&base, &request(&base), committed(&base)).unwrap();
    assert_eq!(view.management.unwrap().owner(), "owner");
    body.push(0);
    assert!(decode_management(&slot_key(&name), &body).is_err());
    assert!(decode_management(&slot_key(&name), &body[..2]).is_err());
}
#[test]
fn scope_mismatch_is_rejected() {
    let base = metadata(properties());
    let mut request = request(&base);
    request.object_id =
        ConnectorTableObjectId::try_new(Bytes::from_static(b"other-table")).unwrap();
    assert!(project_view(&base, &request, committed(&base)).is_err());
}
#[test]
fn closure_retains_shared_links_and_rejects_missing() {
    let a = version();
    let b = ConnectorDocumentVersion::try_new(
        "novarocks.mv",
        "binding",
        format(),
        vec![("definition".into(), a.identity())],
        Bytes::new(),
    )
    .unwrap();
    let versions = BTreeMap::from([(a.identity(), a.clone()), (b.identity(), b.clone())]);
    assert_eq!(
        reference_closure(&versions, [a.identity(), b.identity()])
            .unwrap()
            .len(),
        2
    );
    assert!(reference_closure(&versions, [DocumentIdentity::from_bytes([1; 32])]).is_err());
}
#[test]
fn private_property_byte_observation_ignores_unrelated_properties() {
    let props = HashMap::from([
        ("novarocks.doc.v2.root.a".into(), GOLDEN_ID.into()),
        ("unrelated".into(), "large".repeat(100)),
    ]);
    assert_eq!(
        private_property_bytes(&props),
        "novarocks.doc.v2.root.a".len() + 64
    );
}

#[test]
fn keyed_create_keeps_versions_above_former_encoded_limits() {
    // These sizes cross the former per-version and total-property quotas;
    // they are test inputs, not replacement product limits.
    let versions = (0..4)
        .map(|index| {
            ConnectorDocumentVersion::try_new(
                "novarocks.mv",
                format!("definition-{index}"),
                format(),
                vec![],
                Bytes::from(vec![b'x'; 50_000]),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let documents = KeyedCreate {
        versions: versions.clone(),
        roots: BTreeMap::from([("novarocks.mv".into(), versions[0].identity())]),
        ..Default::default()
    };
    let properties = super::write::create_properties(&documents).unwrap();
    for version in &versions {
        let value = &properties[&version_key(version.identity())];
        assert!(value.len() > 64 * 1024);
        assert_eq!(value, &encode_version(version).unwrap());
    }
    assert!(private_property_bytes(&properties) > 256 * 1024);
    let observed = parse_properties(&properties).unwrap();
    for version in versions {
        assert_eq!(observed.versions[&version.identity()], version);
    }
    assert_eq!(
        properties[&root_key("novarocks.mv")],
        documents.roots["novarocks.mv"].to_hex()
    );
}

#[test]
fn present_invalid_file_totals_are_corrupt_instead_of_unknown() {
    for name in ["total-data-files", "total-delete-files"] {
        let base = with_snapshots(
            metadata(properties()),
            vec![HashMap::from([(name.into(), "invalid".into())])],
        );
        let error = project_view(&base, &request(&base), committed(&base)).unwrap_err();
        let KeyedDocumentReadError::Invalid(error) = error else {
            panic!("expected corrupt data")
        };
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert!(error.message().contains(name));
    }
}

#[test]
fn keyed_create_has_six_exact_document_properties_and_null_writers() {
    let definition = version();
    let layout = ConnectorDocumentVersion::try_new(
        "novarocks.mv",
        "interpretation",
        format(),
        vec![],
        Bytes::from_static(b"layout"),
    )
    .unwrap();
    let binding = ConnectorDocumentVersion::try_new(
        "novarocks.mv",
        "binding",
        format(),
        vec![
            ("definition".into(), definition.identity()),
            ("interpretation".into(), layout.identity()),
        ],
        Bytes::from_static(b"binding"),
    )
    .unwrap();
    let configuration = ConnectorSlotValue {
        format: format(),
        content: Bytes::from_static(b"C"),
    };
    let documents = KeyedCreate {
        versions: vec![definition, layout, binding.clone()],
        slots: BTreeMap::from([
            (slot_name(), configuration.clone()),
            (
                ConnectorSlotName::try_new("novarocks.storage", "management").unwrap(),
                configuration,
            ),
        ]),
        roots: BTreeMap::from([("novarocks.mv".into(), binding.identity())]),
    };
    let properties = super::write::create_properties(&documents).unwrap();
    assert_eq!(properties.len(), 6);
    assert_eq!(properties.len() + 1, 7); // CTAS operation marker; expectation bound is 32.
    assert!(properties.len() + 1 < 32);
    assert_eq!(parse_properties(&properties).unwrap().slots.len(), 2);
    assert!(
        parse_properties(&properties)
            .unwrap()
            .slots
            .values()
            .all(|slot| slot.writer.is_none())
    );
}

#[test]
fn marker_writes_only_its_exact_key_and_replaying_it_is_identical() {
    let definition = version();
    let base = metadata(HashMap::from([
        (
            version_key(definition.identity()),
            encode_version(&definition).unwrap(),
        ),
        ("comment".into(), "unrelated".into()),
    ]));
    let value = ConnectorMarkerValue {
        format: format(),
        content: Bytes::from_static(b"intent"),
    };
    let documents = KeyedMarkerWrite {
        markers: BTreeMap::from([(marker_name(), value)]),
    };
    let writes = super::write::marker_properties(&base, &documents).unwrap();
    assert_eq!(writes.len(), 1);
    assert!(writes.contains_key(&marker_key(&marker_name())));
    let mut after = base.properties().clone();
    after.extend(writes.clone());
    let replay = super::write::marker_properties(&metadata(after.clone()), &documents).unwrap();
    assert_eq!(replay, writes);
    after.extend(replay);
    assert_eq!(after.len(), 3);
}

#[test]
fn publication_and_marker_writes_keep_closure_with_large_history() {
    let definition = version();
    let mut properties = HashMap::from([(
        version_key(definition.identity()),
        encode_version(&definition).unwrap(),
    )]);
    for index in 0..8 {
        let historical = ConnectorDocumentVersion::try_new(
            "novarocks.mv",
            format!("history-{index}"),
            format(),
            vec![],
            Bytes::from(vec![b'x'; 30_000]),
        )
        .unwrap();
        properties.insert(
            version_key(historical.identity()),
            encode_version(&historical).unwrap(),
        );
    }
    assert!(private_property_bytes(&properties) > 256 * 1024);
    let base = metadata(properties);
    let documents = KeyedPublication {
        records: vec![record(Some(definition.identity()))],
        // The MV supplies existing D/L/B bytes for closure reassertion too.
        versions: vec![definition.clone()],
        ..Default::default()
    };
    let result = super::write::publication_updates(&base, &documents).unwrap();
    assert_eq!(result.summary.len(), 1);
    assert!(
        matches!(&result.updates[..], [crate::iceberg::TableUpdate::SetProperties{updates}] if updates.len()==1 && updates[&version_key(definition.identity())]==encode_version(&definition).unwrap())
    );
    let marker = KeyedMarkerWrite {
        markers: BTreeMap::from([(
            marker_name(),
            ConnectorMarkerValue {
                format: format(),
                content: Bytes::from_static(b"new"),
            },
        )]),
    };
    let writes = super::write::marker_properties(&base, &marker).unwrap();
    let (key, value) = marker.markers.iter().next().unwrap();
    assert_eq!(writes[&marker_key(key)], encode_marker(key, value).unwrap());
}

#[test]
fn keyed_writes_keep_large_records_and_reject_dangling_roots() {
    let oversized = ConnectorDocumentVersion::try_new(
        "novarocks.mv",
        "large",
        format(),
        vec![],
        Bytes::from(vec![0; 50_000]),
    )
    .unwrap();
    let create = KeyedCreate {
        versions: vec![oversized],
        ..Default::default()
    };
    let properties = super::write::create_properties(&create).unwrap();
    let version = &create.versions[0];
    assert!(properties[&version_key(version.identity())].len() > 64 * 1024);
    assert_eq!(
        properties[&version_key(version.identity())],
        encode_version(version).unwrap()
    );
    let documents = KeyedPublication {
        records: vec![ConnectorSnapshotRecord {
            payload: Bytes::from(vec![0; 4_096]),
            ..record(None)
        }],
        ..Default::default()
    };
    let result = super::write::publication_updates(&metadata(HashMap::new()), &documents).unwrap();
    let record = &documents.records[0];
    let key = record_key(&record.owner, &record.name);
    assert!(key.len() + result.summary[&key].len() > 4 * 1024);
    assert_eq!(result.summary[&key], encode_record(record).unwrap());
    let summary = result.summary.into_iter().collect::<HashMap<_, _>>();
    assert_eq!(
        parse_snapshot_records(&summary, true).unwrap(),
        documents.records
    );
    let dangling = KeyedCreate {
        roots: BTreeMap::from([("novarocks.mv".into(), DocumentIdentity::from_bytes([3; 32]))]),
        ..Default::default()
    };
    assert_eq!(
        super::write::create_properties(&dangling)
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::CorruptData
    );
}
