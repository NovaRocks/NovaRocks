use bytes::Bytes;
use novarocks::mv::dependency::model::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
};
use novarocks::mv::persistence::definition::{StoredMvDefinition, StoredMvRefreshPolicy};
use novarocks::mv::persistence::refresh::{
    FrontendMvRefreshAction, FrontendMvRefreshActionPhase, FrontendMvRefreshActionState,
    FrontendMvRefreshCommittedVersion, FrontendMvRefreshEvidence, FrontendMvRefreshLedger,
    MvRefreshLifecycleOwner, MvRefreshState, StoredMvRefresh,
};
use novarocks::mv::repository::MvTargetLookup;
use novarocks_frontend::mv::repository::catalog::schema_catalog;
use novarocks_frontend::mv::repository::codec::{
    DecodedMvRecord, MvRecordKind, MvSequence, decode_definition, decode_record, encode_definition,
    encode_record,
};
use novarocks_frontend::mv::repository::key::{
    MvKeyKind, decode_key, definition_by_id_key, dependency_by_downstream_key,
    dependency_by_upstream_key, partition_by_mv_key, sequence_key, target_lookup_key,
};
use novarocks_spi::state_store::{Key, Value};
use std::collections::BTreeMap;
use uuid::Uuid;

fn sha256_bytes(payload: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};

    Sha256::digest(payload).to_vec()
}

fn upstream() -> MvDependencyObjectRef {
    MvDependencyObjectRef {
        catalog: Some("ICE".to_string()),
        database_or_namespace: "Sales".to_string(),
        name: "Orders".to_string(),
        object_type: MvDependencyObjectType::Table,
        storage_engine: MvDependencyStorageEngine::Iceberg,
    }
}

#[test]
fn keys_are_canonical_range_ordered_and_round_trip() {
    let low = definition_by_id_key(9).expect("low definition key");
    let high = definition_by_id_key(10).expect("high definition key");
    assert!(
        low < high,
        "fixed-width hexadecimal IDs must preserve order"
    );
    assert_eq!(
        std::str::from_utf8(low.as_bytes()).expect("key UTF-8"),
        "novarocks/frontend/mv/v1/definition/by-id/0000000000000009"
    );
    assert_eq!(
        decode_key(&low).expect("decode key").kind,
        MvKeyKind::Definition
    );

    let target = target_lookup_key("`ICE`", " Sales ", "Orders").expect("target key");
    assert_eq!(
        std::str::from_utf8(target.as_bytes()).expect("key UTF-8"),
        "novarocks/frontend/mv/v1/definition/by-target/696365/73616c6573/6f7264657273"
    );
    assert_eq!(
        decode_key(&target).expect("decode target").kind,
        MvKeyKind::TargetLookup
    );

    let partition = partition_by_mv_key(10, "spec=7;region=s:us/east").expect("partition key");
    assert_eq!(
        decode_key(&partition).expect("decode partition").kind,
        MvKeyKind::Partition
    );
}

#[test]
fn dependency_indexes_share_canonical_identity_and_reject_separator() {
    let dependency = upstream();
    let downstream = dependency_by_downstream_key(7, &dependency).expect("downstream key");
    let upstream_key = dependency_by_upstream_key(&dependency, 7).expect("upstream key");
    let downstream_key = std::str::from_utf8(downstream.as_bytes()).expect("UTF-8");
    let upstream_key = std::str::from_utf8(upstream_key.as_bytes()).expect("UTF-8");
    assert!(
        downstream_key.ends_with("696365626572677c7461626c657c6963657c73616c65737c6f7264657273")
    );
    assert!(upstream_key.contains("696365626572677c7461626c657c6963657c73616c65737c6f7264657273"));

    let mut invalid = dependency;
    invalid.name = "orders|bad".to_string();
    let error = dependency_by_downstream_key(7, &invalid).expect_err("separator must fail");
    assert!(error.contains("must not contain '|'"));
}

#[test]
fn malformed_and_noncanonical_keys_fail_loudly() {
    for raw in [
        "novarocks/frontend/mv/v1/definition/by-id/9",
        "novarocks/frontend/mv/v1/definition/by-id/0000000000000000",
        "novarocks/frontend/mv/v1/definition/by-id/000000000000000A",
        "novarocks/frontend/mv/v1/definition/by-target/not-hex/73616c6573/6f7264657273",
        "novarocks/frontend/mv/v1/refresh/by-id/0000000000000001/extra",
    ] {
        let key = Key::try_from(Bytes::from(raw)).expect("SPI accepts bounded raw key");
        assert!(decode_key(&key).is_err(), "{raw} must fail");
    }
}

#[test]
fn key_limit_is_enforced_by_spi_constructor() {
    let oversized = "x".repeat(512);
    let error = partition_by_mv_key(1, &oversized).expect_err("512-byte MV key limit");
    assert!(error.contains("512-byte limit"));
}

#[test]
fn envelope_round_trips_and_rejects_corruption() {
    let key = target_lookup_key("ice", "sales", "orders").expect("target key");
    let operation_id = Uuid::now_v7();
    let value = encode_record(
        MvRecordKind::TargetLookup,
        operation_id,
        &MvTargetLookup { mv_id: 42 },
    )
    .expect("encode record");
    let decoded: DecodedMvRecord<MvTargetLookup> =
        decode_record(&key, &value).expect("decode record");
    assert_eq!(decoded.operation_id, operation_id);
    assert_eq!(decoded.value, MvTargetLookup { mv_id: 42 });

    let mut bytes = value.into_bytes().to_vec();
    bytes[0] = b'X';
    let malformed = Value::try_from(Bytes::from(bytes)).expect("bounded malformed value");
    assert!(decode_record::<MvTargetLookup>(&key, &malformed).is_err());
}

#[test]
fn sequence_v2_round_trips_the_frontend_refresh_high_water_mark() {
    let key = sequence_key().expect("sequence key");
    let sequence = MvSequence {
        last_allocated_id: 42,
        last_refresh_id: 99,
    };
    let value = encode_record(MvRecordKind::Sequence, Uuid::now_v7(), &sequence)
        .expect("encode sequence v2");
    let decoded: DecodedMvRecord<MvSequence> = decode_record(&key, &value).expect("decode v2");
    assert_eq!(decoded.value, sequence);
}

#[test]
fn envelope_rejects_key_kind_unknown_schema_and_trailing_bytes() {
    let operation_id = Uuid::now_v7();
    let value = encode_record(
        MvRecordKind::TargetLookup,
        operation_id,
        &MvTargetLookup { mv_id: 7 },
    )
    .expect("encode record");
    let wrong_key = definition_by_id_key(7).expect("definition key");
    assert!(decode_record::<MvTargetLookup>(&wrong_key, &value).is_err());

    let target = target_lookup_key("ice", "sales", "orders").expect("target key");
    let mut unknown_schema = value.clone().into_bytes().to_vec();
    unknown_schema[6..10].copy_from_slice(&999_i32.to_be_bytes());
    let unknown_schema = Value::try_from(Bytes::from(unknown_schema)).expect("bounded value");
    assert!(decode_record::<MvTargetLookup>(&target, &unknown_schema).is_err());

    let mut trailing = value.into_bytes().to_vec();
    trailing.push(0);
    let trailing = Value::try_from(Bytes::from(trailing)).expect("bounded value");
    assert!(decode_record::<MvTargetLookup>(&target, &trailing).is_err());
}

#[test]
fn envelope_rejects_version_kind_fingerprint_and_payload_length_corruption() {
    let key = target_lookup_key("ice", "sales", "orders").expect("target key");
    let value = encode_record(
        MvRecordKind::TargetLookup,
        Uuid::now_v7(),
        &MvTargetLookup { mv_id: 7 },
    )
    .expect("encode record");
    let source = value.into_bytes().to_vec();

    let mut version = source.clone();
    version[4] = 2;
    let version = Value::try_from(Bytes::from(version)).expect("bounded value");
    assert!(decode_record::<MvTargetLookup>(&key, &version).is_err());

    let mut kind = source.clone();
    kind[5] = 255;
    let kind = Value::try_from(Bytes::from(kind)).expect("bounded value");
    assert!(decode_record::<MvTargetLookup>(&key, &kind).is_err());

    let mut fingerprint = source.clone();
    fingerprint[12] = b'0';
    let fingerprint = Value::try_from(Bytes::from(fingerprint)).expect("bounded value");
    assert!(decode_record::<MvTargetLookup>(&key, &fingerprint).is_err());

    let mut payload_length = source;
    let fingerprint_len =
        u16::from_be_bytes(payload_length[10..12].try_into().expect("length")) as usize;
    let length_offset = 12 + fingerprint_len + 16;
    let length = u32::from_be_bytes(
        payload_length[length_offset..length_offset + 4]
            .try_into()
            .expect("payload length"),
    );
    payload_length[length_offset..length_offset + 4].copy_from_slice(&(length + 1).to_be_bytes());
    let payload_length = Value::try_from(Bytes::from(payload_length)).expect("bounded value");
    assert!(decode_record::<MvTargetLookup>(&key, &payload_length).is_err());
}

#[test]
fn mv_catalog_validates_all_historical_schemas_transitively() {
    let catalog = schema_catalog().expect("MV-only schema catalog");
    catalog.validate_unique_entries().expect("unique entries");
    catalog
        .validate_full_transitive()
        .expect("full transitive compatibility");
    assert_eq!(
        catalog
            .latest("mv.definition")
            .expect("definition schema")
            .id(),
        2
    );
    assert_eq!(
        catalog.latest("mv.refresh").expect("refresh schema").id(),
        3
    );
    assert_eq!(
        catalog.latest("mv.sequence").expect("sequence schema").id(),
        2
    );
}

#[test]
fn refresh_v3_round_trips_frontend_owned_opaque_ledger() {
    let request_id = Uuid::now_v7().into_bytes().to_vec();
    let staging_create_operation_id = Uuid::now_v7().into_bytes().to_vec();
    let write_operation_id = Uuid::now_v7().into_bytes().to_vec();
    let publication_operation_id = Uuid::now_v7().into_bytes().to_vec();
    let staging_drop_operation_id = Uuid::now_v7().into_bytes().to_vec();
    let committed_payload = b"provider-version".to_vec();
    let evidence_payload = b"provider-evidence".to_vec();
    let refresh = StoredMvRefresh {
        refresh_id: 7,
        mv_id: 3,
        operation_id: None,
        state: MvRefreshState::IntentCreated,
        target_catalog: Some("ice".to_string()),
        target_namespace: Some("sales".to_string()),
        target_table: Some("daily".to_string()),
        staging_branch: Some("mv-7".to_string()),
        expected_main_snapshot_id: Some(11),
        staging_snapshot_id: None,
        published_snapshot_id: None,
        target_snapshots: BTreeMap::new(),
        base_table_uuids: BTreeMap::new(),
        rows: None,
        marker: None,
        external_outcome: None,
        lifecycle_owner: MvRefreshLifecycleOwner::FrontendCurrent,
        frontend_ledger: Some(FrontendMvRefreshLedger {
            request_id,
            provider_id: "iceberg".to_string(),
            instance_id: "rest".to_string(),
            incarnation: Uuid::now_v7().into_bytes().to_vec(),
            expected_target_version: Some(
                FrontendMvRefreshCommittedVersion::try_new(committed_payload.clone(), Some(12))
                    .expect("committed version"),
            ),
            staging_create_operation_id: staging_create_operation_id.clone(),
            write_operation_id: write_operation_id.clone(),
            publication_operation_id: publication_operation_id.clone(),
            staging_drop_operation_id: staging_drop_operation_id.clone(),
            cohort_ids: vec!["cohort-a".to_string()],
            actions: vec![
                FrontendMvRefreshAction {
                    phase: FrontendMvRefreshActionPhase::StagingCreate,
                    state: FrontendMvRefreshActionState::Prepared,
                    operation_id: staging_create_operation_id,
                    receipt: None,
                    committed_version: None,
                    external_evidence: None,
                    provider_finalized: false,
                },
                FrontendMvRefreshAction {
                    phase: FrontendMvRefreshActionPhase::Write,
                    state: FrontendMvRefreshActionState::KnownCommitted,
                    operation_id: write_operation_id,
                    receipt: Some(FrontendMvRefreshEvidence {
                        payload: evidence_payload.clone(),
                        digest: sha256_bytes(&evidence_payload),
                    }),
                    committed_version: Some(
                        FrontendMvRefreshCommittedVersion::try_new(
                            committed_payload.clone(),
                            Some(12),
                        )
                        .expect("committed version"),
                    ),
                    external_evidence: None,
                    provider_finalized: false,
                },
                FrontendMvRefreshAction {
                    phase: FrontendMvRefreshActionPhase::Publication,
                    state: FrontendMvRefreshActionState::Prepared,
                    operation_id: publication_operation_id,
                    receipt: None,
                    committed_version: None,
                    external_evidence: None,
                    provider_finalized: false,
                },
                FrontendMvRefreshAction {
                    phase: FrontendMvRefreshActionPhase::StagingDrop,
                    state: FrontendMvRefreshActionState::Prepared,
                    operation_id: staging_drop_operation_id,
                    receipt: None,
                    committed_version: None,
                    external_evidence: None,
                    provider_finalized: false,
                },
            ],
            cleanup_pending: false,
        }),
    };
    let key = Key::try_from(Bytes::from_static(
        b"novarocks/frontend/mv/v1/refresh/by-id/0000000000000007",
    ))
    .expect("refresh key");
    let encoded =
        encode_record(MvRecordKind::Refresh, Uuid::now_v7(), &refresh).expect("encode v3 refresh");
    let decoded: DecodedMvRecord<StoredMvRefresh> =
        decode_record(&key, &encoded).expect("decode v3 refresh");
    assert_eq!(decoded.value, refresh);
}

#[test]
fn definition_uses_frontend_private_avro_projection() {
    let definition = StoredMvDefinition {
        mv_id: 9,
        select_sql: "SELECT 1".to_string(),
        base_table_refs: Vec::new(),
        primary_key_columns: Vec::new(),
        storage_engine: "iceberg".to_string(),
        target_catalog: Some("ice".to_string()),
        target_namespace: Some("sales".to_string()),
        target_table: Some("daily".to_string()),
        schema_contract: None,
        partition_spec: None,
        partition_state_complete: false,
        last_refresh_ms: None,
        last_refresh_rows: None,
        last_refresh_snapshots: BTreeMap::new(),
        last_refresh_table_uuids: BTreeMap::new(),
        last_refreshed_iceberg_snapshot_id: None,
        refresh_in_progress: false,
        active_refresh_id: None,
        refresh_target_snapshots: BTreeMap::new(),
        refresh_policy: StoredMvRefreshPolicy::Manual,
        refresh_paused: false,
        refresh_interval_ms: None,
        max_staleness_ms: None,
        last_scheduler_error: None,
        next_refresh_after_ms: None,
        created_at_ms: 1,
    };
    let operation_id = Uuid::now_v7();
    let key = definition_by_id_key(definition.mv_id).expect("definition key");
    let encoded = encode_definition(operation_id, &definition).expect("encode definition");
    let decoded = decode_definition(&key, &encoded).expect("decode definition");
    assert_eq!(decoded.operation_id, operation_id);
    assert_eq!(decoded.value, definition);
}
