use std::collections::BTreeMap;

use novarocks::meta::avro::{decode_payload, encode_payload, schema_catalog};
use serde::{Deserialize, Serialize};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TestEvolutionV1 {
    id: i64,
    name: String,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TestEvolutionV2 {
    id: i64,
    name: String,
    tags: Vec<String>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct IcebergCatalogPropertiesAvro {
    properties: Vec<StringPair>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StringPair {
    key: String,
    value: String,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StoredMvDefinitionAvro {
    mv_id: i64,
    select_sql: String,
    base_table_refs: Vec<String>,
    primary_key_columns: Vec<String>,
    storage_engine: String,
    target_catalog: Option<String>,
    target_namespace: Option<String>,
    target_table: Option<String>,
    schema_contract: Option<String>,
    partition_spec: Option<String>,
    last_refresh_ms: Option<i64>,
    last_refresh_rows: Option<i64>,
    last_refresh_snapshots: BTreeMap<String, i64>,
    last_refresh_table_uuids: BTreeMap<String, String>,
    last_refreshed_iceberg_snapshot_id: Option<i64>,
    refresh_in_progress: bool,
    active_refresh_id: Option<i64>,
    refresh_target_snapshots: BTreeMap<String, i64>,
    refresh_policy: StoredMvRefreshPolicyAvro,
    refresh_paused: bool,
    refresh_interval_ms: Option<i64>,
    max_staleness_ms: Option<i64>,
    last_scheduler_error: Option<String>,
    next_refresh_after_ms: Option<i64>,
    created_at_ms: i64,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum StoredMvRefreshPolicyAvro {
    Manual,
    AsyncOnChange,
    AsyncInterval,
}

#[test]
fn avro_catalog_has_unique_subject_ids_and_fingerprints() -> TestResult {
    let catalog = schema_catalog()?;
    catalog.validate_unique_entries()?;
    let latest = catalog.latest("test.evolution")?;
    assert_eq!(latest.subject(), "test.evolution");
    assert_eq!(latest.id(), 2);
    assert_eq!(latest.fingerprint().len(), 16);
    Ok(())
}

#[test]
fn avro_catalog_enforces_full_transitive_compatibility() -> TestResult {
    schema_catalog()?.validate_full_transitive()?;
    Ok(())
}

#[test]
fn avro_codec_round_trips_latest_schema() -> TestResult {
    let payload = encode_payload(
        "test.evolution",
        &TestEvolutionV2 {
            id: 7,
            name: "mv".to_string(),
            tags: vec!["fast".to_string(), "safe".to_string()],
        },
    )?;
    let decoded: TestEvolutionV2 = decode_payload("test.evolution", &payload)?;
    assert_eq!(
        decoded,
        TestEvolutionV2 {
            id: 7,
            name: "mv".to_string(),
            tags: vec!["fast".to_string(), "safe".to_string()],
        }
    );
    Ok(())
}

#[test]
fn avro_codec_reads_older_writer_schema_with_latest_reader_defaults() -> TestResult {
    let catalog = schema_catalog()?;
    let writer = catalog.entry("test.evolution", 1)?;
    let payload = novarocks::meta::avro::encode_payload_with_schema(
        writer,
        &TestEvolutionV1 {
            id: 9,
            name: "old".to_string(),
        },
    )?;

    let decoded: TestEvolutionV2 = decode_payload("test.evolution", &payload)?;
    assert_eq!(
        decoded,
        TestEvolutionV2 {
            id: 9,
            name: "old".to_string(),
            tags: Vec::new(),
        }
    );
    Ok(())
}

#[test]
fn iceberg_catalog_properties_round_trip_as_string_pairs() -> TestResult {
    let expected = IcebergCatalogPropertiesAvro {
        properties: vec![
            StringPair {
                key: "type".to_string(),
                value: "rest".to_string(),
            },
            StringPair {
                key: "uri".to_string(),
                value: "http://localhost:8181".to_string(),
            },
            StringPair {
                key: "warehouse".to_string(),
                value: "s3://warehouse".to_string(),
            },
        ],
    };

    let payload = encode_payload("iceberg.catalog", &expected)?;
    assert_eq!(payload.schema_id, 1);
    let decoded: IcebergCatalogPropertiesAvro = decode_payload("iceberg.catalog", &payload)?;

    assert_eq!(decoded, expected);
    Ok(())
}

#[test]
fn mv_definition_round_trip_uses_json_string_contract_dto() -> TestResult {
    let expected = StoredMvDefinitionAvro {
        mv_id: 42,
        select_sql: "select id, sum(v) from db.orders group by id".to_string(),
        base_table_refs: vec!["iceberg.rest.db.orders".to_string()],
        primary_key_columns: vec!["id".to_string()],
        storage_engine: "iceberg".to_string(),
        target_catalog: Some("managed".to_string()),
        target_namespace: Some("mv".to_string()),
        target_table: Some("mv_orders".to_string()),
        schema_contract: Some(
            r#"{"columns":[{"name":"id","type":"BIGINT"},{"name":"total","type":"BIGINT"}]}"#
                .to_string(),
        ),
        partition_spec: Some(r#"{"fields":[{"source":"id","transform":"identity"}]}"#.to_string()),
        last_refresh_ms: Some(1_771_891_200_000),
        last_refresh_rows: Some(10),
        last_refresh_snapshots: BTreeMap::from([("iceberg.rest.db.orders".to_string(), 101)]),
        last_refresh_table_uuids: BTreeMap::from([(
            "iceberg.rest.db.orders".to_string(),
            "table-uuid-1".to_string(),
        )]),
        last_refreshed_iceberg_snapshot_id: Some(101),
        refresh_in_progress: true,
        active_refresh_id: Some(7),
        refresh_target_snapshots: BTreeMap::from([("iceberg.rest.db.orders".to_string(), 102)]),
        refresh_policy: StoredMvRefreshPolicyAvro::AsyncOnChange,
        refresh_paused: false,
        refresh_interval_ms: None,
        max_staleness_ms: Some(60_000),
        last_scheduler_error: None,
        next_refresh_after_ms: Some(1_771_891_260_000),
        created_at_ms: 1_771_891_100_000,
    };

    let payload = encode_payload("mv.definition", &expected)?;
    assert_eq!(payload.schema_id, 1);
    let decoded: StoredMvDefinitionAvro = decode_payload("mv.definition", &payload)?;

    assert_eq!(decoded, expected);
    Ok(())
}

#[test]
fn avro_codec_rejects_fingerprint_mismatch() -> TestResult {
    let mut payload = encode_payload(
        "test.evolution",
        &TestEvolutionV2 {
            id: 1,
            name: "bad".to_string(),
            tags: Vec::new(),
        },
    )?;
    payload.schema_fingerprint = "ffffffffffffffff".to_string();

    let err = decode_payload::<TestEvolutionV2>("test.evolution", &payload)
        .expect_err("fingerprint mismatch must fail");
    assert!(err.to_string().contains("fingerprint mismatch"), "{err}");
    Ok(())
}
