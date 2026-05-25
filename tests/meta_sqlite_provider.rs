mod common;

use common::meta_provider_conformance as conformance;
use novarocks::meta::{
    ExpectedRevision, MetaKey, MetaPayload, MetaRecordKind, MetaRecordPut, MetaStoreProvider,
    SqliteMetaStoreProvider,
};
use rusqlite::Connection;

type TestResult = Result<(), Box<dyn std::error::Error>>;
type SqliteProviderFixture = (tempfile::TempDir, SqliteMetaStoreProvider);

fn new_sqlite_provider() -> Result<SqliteProviderFixture, Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let provider = SqliteMetaStoreProvider::open(dir.path().join("meta.sqlite"))?;
    Ok((dir, provider))
}

#[test]
fn sqlite_provider_put_not_exists_commits_visible_record() -> TestResult {
    conformance::put_not_exists_commits_visible_record(new_sqlite_provider)
}

#[test]
fn sqlite_provider_exact_revision_updates_record_and_advances_revision() -> TestResult {
    conformance::exact_revision_updates_record_and_advances_revision(new_sqlite_provider)
}

#[test]
fn sqlite_provider_delete_exists_hides_committed_record() -> TestResult {
    conformance::delete_exists_hides_committed_record(new_sqlite_provider)
}

#[test]
fn sqlite_provider_scan_prefix_returns_records_in_key_order() -> TestResult {
    conformance::scan_prefix_returns_records_in_key_order(new_sqlite_provider)
}

#[test]
fn sqlite_provider_allocate_id_is_scoped_and_persists_after_commit() -> TestResult {
    conformance::allocate_id_is_scoped_and_persists_after_commit(new_sqlite_provider)
}

#[test]
fn sqlite_provider_put_exists_updates_existing_and_rejects_missing_record() -> TestResult {
    conformance::put_exists_updates_existing_and_rejects_missing_record(new_sqlite_provider)
}

#[test]
fn sqlite_provider_read_txn_keeps_snapshot_from_begin() -> TestResult {
    conformance::read_txn_keeps_snapshot_from_begin(new_sqlite_provider)
}

#[test]
fn sqlite_provider_abort_discards_record_and_id_mutations() -> TestResult {
    conformance::abort_discards_record_and_id_mutations(new_sqlite_provider)
}

#[test]
fn sqlite_provider_stale_exact_revision_returns_conflict() -> TestResult {
    conformance::stale_exact_revision_returns_conflict(new_sqlite_provider)
}

#[test]
fn sqlite_provider_any_upserts_missing_and_existing_records() -> TestResult {
    conformance::any_upserts_missing_and_existing_records(new_sqlite_provider)
}

#[test]
fn sqlite_provider_initializes_avro_store_markers() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("meta.sqlite");
    let provider = SqliteMetaStoreProvider::open(&path)?;
    drop(provider);

    let conn = Connection::open(&path)?;
    let store_format: String = conn.query_row(
        "SELECT CAST(value AS TEXT) FROM meta_provider_schema WHERE key = 'store_format'",
        [],
        |row| row.get(0),
    )?;
    let metadata_epoch: String = conn.query_row(
        "SELECT CAST(value AS TEXT) FROM meta_provider_schema WHERE key = 'metadata_epoch'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(store_format, "avro");
    assert_eq!(metadata_epoch, "1");
    Ok(())
}

#[test]
fn sqlite_provider_rejects_nonempty_legacy_json_store() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("meta.sqlite");
    {
        let conn = Connection::open(&path)?;
        conn.execute_batch(
            r#"
            CREATE TABLE meta_records (
                namespace TEXT NOT NULL,
                key TEXT NOT NULL,
                kind TEXT NOT NULL,
                revision INTEGER NOT NULL,
                payload_encoding TEXT NOT NULL,
                payload_schema_version INTEGER NOT NULL,
                payload BLOB NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY(namespace, key)
            );
            CREATE TABLE meta_id_scopes (
                scope TEXT PRIMARY KEY,
                next_id INTEGER NOT NULL
            );
            INSERT INTO meta_records(
                namespace, key, kind, revision, payload_encoding,
                payload_schema_version, payload, created_at_ms, updated_at_ms
            )
            VALUES('mv', 'by-id/1', 'mv.definition', 1, 'json', 1, X'7B7D', 1, 1);
            "#,
        )?;
    }

    let err = SqliteMetaStoreProvider::open(&path).expect_err("legacy JSON store must fail");
    assert_eq!(
        err.kind(),
        novarocks::meta::MetaErrorKind::ProviderCorruption
    );
    assert!(
        err.to_string()
            .contains("legacy or unsupported metadata store"),
        "{err}"
    );
    Ok(())
}

#[test]
fn sqlite_provider_reinitializes_empty_legacy_json_store_with_provider_schema() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("meta.sqlite");
    {
        let conn = Connection::open(&path)?;
        conn.execute_batch(
            r#"
            CREATE TABLE meta_provider_schema (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );
            CREATE TABLE meta_records (
                namespace TEXT NOT NULL,
                key TEXT NOT NULL,
                kind TEXT NOT NULL,
                revision INTEGER NOT NULL,
                payload_encoding TEXT NOT NULL,
                payload_schema_version INTEGER NOT NULL,
                payload BLOB NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY(namespace, key)
            );
            CREATE TABLE meta_id_scopes (
                scope TEXT PRIMARY KEY,
                next_id INTEGER NOT NULL
            );
            "#,
        )?;
    }

    let provider = SqliteMetaStoreProvider::open(&path)?;
    let key = MetaKey::new("mv", ["by-id", "1"])?;
    let kind = MetaRecordKind::new("mv.definition")?;
    let payload = MetaPayload::avro(1, "0000000000000000", bytes::Bytes::from_static(b"mv"));

    {
        let mut txn = provider.begin_write("write after legacy reinit")?;
        txn.put(MetaRecordPut::new(
            key.clone(),
            kind,
            ExpectedRevision::NotExists,
            payload.clone(),
        ))?;
        txn.commit()?;
    }

    let record = provider
        .begin_read()?
        .get(&key)?
        .expect("record should be writable after reinit");
    assert_eq!(record.payload, payload);
    Ok(())
}

#[test]
fn sqlite_provider_rejects_nonempty_legacy_json_store_with_provider_schema() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("meta.sqlite");
    {
        let conn = Connection::open(&path)?;
        conn.execute_batch(
            r#"
            CREATE TABLE meta_provider_schema (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );
            CREATE TABLE meta_records (
                namespace TEXT NOT NULL,
                key TEXT NOT NULL,
                kind TEXT NOT NULL,
                revision INTEGER NOT NULL,
                payload_encoding TEXT NOT NULL,
                payload_schema_version INTEGER NOT NULL,
                payload BLOB NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY(namespace, key)
            );
            CREATE TABLE meta_id_scopes (
                scope TEXT PRIMARY KEY,
                next_id INTEGER NOT NULL
            );
            INSERT INTO meta_records(
                namespace, key, kind, revision, payload_encoding,
                payload_schema_version, payload, created_at_ms, updated_at_ms
            )
            VALUES('mv', 'by-id/1', 'mv.definition', 1, 'json', 1, X'7B7D', 1, 1);
            "#,
        )?;
    }

    let err = SqliteMetaStoreProvider::open(&path).expect_err("legacy JSON store must fail");
    assert_eq!(
        err.kind(),
        novarocks::meta::MetaErrorKind::ProviderCorruption
    );
    assert!(
        err.to_string()
            .contains("legacy or unsupported metadata store"),
        "{err}"
    );
    Ok(())
}

#[test]
fn sqlite_provider_rejects_avro_store_with_json_payload_rows() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("meta.sqlite");
    {
        let conn = Connection::open(&path)?;
        conn.execute_batch(
            r#"
            CREATE TABLE meta_provider_schema (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );
            INSERT INTO meta_provider_schema(key, value)
            VALUES('store_format', 'avro'), ('metadata_epoch', '1');
            CREATE TABLE meta_records (
                namespace TEXT NOT NULL,
                key TEXT NOT NULL,
                kind TEXT NOT NULL,
                revision INTEGER NOT NULL,
                payload_encoding TEXT NOT NULL,
                payload_schema_id INTEGER NOT NULL,
                payload_schema_fingerprint TEXT NOT NULL,
                payload BLOB NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY(namespace, key)
            );
            CREATE TABLE meta_id_scopes (
                scope TEXT PRIMARY KEY,
                next_id INTEGER NOT NULL
            );
            INSERT INTO meta_records(
                namespace, key, kind, revision, payload_encoding,
                payload_schema_id, payload_schema_fingerprint, payload,
                created_at_ms, updated_at_ms
            )
            VALUES('mv', 'by-id/1', 'mv.definition', 1, 'json', 1, '0000000000000000', X'7B7D', 1, 1);
            "#,
        )?;
    }

    let err = SqliteMetaStoreProvider::open(&path).expect_err("JSON payload row must fail");
    assert_eq!(
        err.kind(),
        novarocks::meta::MetaErrorKind::ProviderCorruption
    );
    assert!(
        err.to_string()
            .contains("unsupported metadata payload encoding"),
        "{err}"
    );
    Ok(())
}

#[test]
fn sqlite_provider_rejects_empty_legacy_json_store_with_id_scopes() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("meta.sqlite");
    {
        let conn = Connection::open(&path)?;
        conn.execute_batch(
            r#"
            CREATE TABLE meta_provider_schema (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );
            CREATE TABLE meta_records (
                namespace TEXT NOT NULL,
                key TEXT NOT NULL,
                kind TEXT NOT NULL,
                revision INTEGER NOT NULL,
                payload_encoding TEXT NOT NULL,
                payload_schema_version INTEGER NOT NULL,
                payload BLOB NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY(namespace, key)
            );
            CREATE TABLE meta_id_scopes (
                scope TEXT PRIMARY KEY,
                next_id INTEGER NOT NULL
            );
            INSERT INTO meta_id_scopes(scope, next_id) VALUES('mv.id', 42);
            "#,
        )?;
    }

    let err = SqliteMetaStoreProvider::open(&path).expect_err("legacy ID scope state must fail");
    assert_eq!(
        err.kind(),
        novarocks::meta::MetaErrorKind::ProviderCorruption
    );
    assert!(
        err.to_string()
            .contains("legacy or unsupported metadata store"),
        "{err}"
    );
    Ok(())
}

#[test]
fn sqlite_provider_rejects_unknown_metadata_epoch() -> TestResult {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("meta.sqlite");
    {
        let conn = Connection::open(&path)?;
        conn.execute_batch(
            r#"
            CREATE TABLE meta_provider_schema (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );
            INSERT INTO meta_provider_schema(key, value)
            VALUES('store_format', 'avro'), ('metadata_epoch', '999');
            CREATE TABLE meta_records (
                namespace TEXT NOT NULL,
                key TEXT NOT NULL,
                kind TEXT NOT NULL,
                revision INTEGER NOT NULL,
                payload_encoding TEXT NOT NULL,
                payload_schema_id INTEGER NOT NULL,
                payload_schema_fingerprint TEXT NOT NULL,
                payload BLOB NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY(namespace, key)
            );
            CREATE TABLE meta_id_scopes (
                scope TEXT PRIMARY KEY,
                next_id INTEGER NOT NULL
            );
            "#,
        )?;
    }

    let err = SqliteMetaStoreProvider::open(&path).expect_err("future epoch must fail");
    assert_eq!(
        err.kind(),
        novarocks::meta::MetaErrorKind::ProviderCorruption
    );
    assert!(err
        .to_string()
        .contains("metadata epoch 999 is newer than supported epoch 1"));
    Ok(())
}
