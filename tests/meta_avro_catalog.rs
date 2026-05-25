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
