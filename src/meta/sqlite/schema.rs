use rusqlite::{Connection, OptionalExtension, params};

use crate::meta::{MetaError, MetaErrorKind};

const STORE_FORMAT_KEY: &str = "store_format";
const STORE_FORMAT_AVRO: &str = "avro";
const METADATA_EPOCH_KEY: &str = "metadata_epoch";
const SUPPORTED_METADATA_EPOCH: i64 = 1;

pub(super) fn init_schema(conn: &Connection) -> Result<(), MetaError> {
    let has_records = table_exists(conn, "meta_records")?;

    if has_records && has_legacy_record_layout(conn)? {
        if legacy_provider_state_count(conn)? > 0 {
            return Err(MetaError::new(
                MetaErrorKind::ProviderCorruption,
                "legacy or unsupported metadata store is non-empty; JSON metadata is not migrated",
            ));
        }
        drop_provider_tables(conn)?;
    }

    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS meta_provider_schema (
            key TEXT PRIMARY KEY,
            value BLOB NOT NULL
        );

        CREATE TABLE IF NOT EXISTS meta_records (
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

        CREATE TABLE IF NOT EXISTS meta_id_scopes (
            scope TEXT PRIMARY KEY,
            next_id INTEGER NOT NULL
        );
        "#,
    )
    .map_err(super::txn::sqlite_error)?;

    ensure_store_marker(conn)?;
    ensure_supported_epoch(conn)?;
    ensure_avro_payload_encodings(conn)?;
    Ok(())
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool, MetaError> {
    conn.query_row(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
        params![table],
        |_| Ok(()),
    )
    .optional()
    .map(|row| row.is_some())
    .map_err(super::txn::sqlite_error)
}

fn has_legacy_record_layout(conn: &Connection) -> Result<bool, MetaError> {
    let has_schema_version = column_exists(conn, "meta_records", "payload_schema_version")?;
    let has_schema_id = column_exists(conn, "meta_records", "payload_schema_id")?;
    let has_schema_fingerprint = column_exists(conn, "meta_records", "payload_schema_fingerprint")?;
    Ok(has_schema_version || !has_schema_id || !has_schema_fingerprint)
}

fn column_exists(conn: &Connection, table: &str, column: &str) -> Result<bool, MetaError> {
    let mut stmt = conn
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(super::txn::sqlite_error)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(super::txn::sqlite_error)?;
    for name in rows {
        if name.map_err(super::txn::sqlite_error)? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

fn legacy_record_count(conn: &Connection) -> Result<i64, MetaError> {
    conn.query_row("SELECT COUNT(*) FROM meta_records", [], |row| row.get(0))
        .optional()
        .map(|count| count.unwrap_or(0))
        .map_err(super::txn::sqlite_error)
}

fn legacy_provider_state_count(conn: &Connection) -> Result<i64, MetaError> {
    Ok(legacy_record_count(conn)? + id_scope_count(conn)?)
}

fn id_scope_count(conn: &Connection) -> Result<i64, MetaError> {
    if !table_exists(conn, "meta_id_scopes")? {
        return Ok(0);
    }
    conn.query_row("SELECT COUNT(*) FROM meta_id_scopes", [], |row| row.get(0))
        .optional()
        .map(|count| count.unwrap_or(0))
        .map_err(super::txn::sqlite_error)
}

fn drop_provider_tables(conn: &Connection) -> Result<(), MetaError> {
    conn.execute_batch(
        r#"
        DROP TABLE IF EXISTS meta_provider_schema;
        DROP TABLE IF EXISTS meta_records;
        DROP TABLE IF EXISTS meta_id_scopes;
        "#,
    )
    .map_err(super::txn::sqlite_error)
}

fn ensure_avro_payload_encodings(conn: &Connection) -> Result<(), MetaError> {
    let row: Option<(String, String, String)> = conn
        .query_row(
            r#"
            SELECT namespace, key, payload_encoding
            FROM meta_records
            WHERE payload_encoding <> ?1
            LIMIT 1
            "#,
            params![STORE_FORMAT_AVRO],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(super::txn::sqlite_error)?;
    if let Some((namespace, key, encoding)) = row {
        return Err(MetaError::new(
            MetaErrorKind::ProviderCorruption,
            format!(
                "unsupported metadata payload encoding `{encoding}` in record `{namespace}/{key}`"
            ),
        ));
    }
    Ok(())
}

fn ensure_store_marker(conn: &Connection) -> Result<(), MetaError> {
    let value: Option<String> = conn
        .query_row(
            "SELECT CAST(value AS TEXT) FROM meta_provider_schema WHERE key = ?1",
            params![STORE_FORMAT_KEY],
            |row| row.get(0),
        )
        .optional()
        .map_err(super::txn::sqlite_error)?;
    match value.as_deref() {
        Some(STORE_FORMAT_AVRO) => Ok(()),
        Some(other) => Err(MetaError::new(
            MetaErrorKind::ProviderCorruption,
            format!("unsupported metadata store format `{other}`"),
        )),
        None => {
            let records: i64 = conn
                .query_row("SELECT COUNT(*) FROM meta_records", [], |row| row.get(0))
                .map_err(super::txn::sqlite_error)?;
            if records != 0 {
                return Err(MetaError::new(
                    MetaErrorKind::ProviderCorruption,
                    "legacy or unsupported metadata store is non-empty; missing store_format marker",
                ));
            }
            conn.execute(
                "INSERT INTO meta_provider_schema(key, value) VALUES(?1, ?2)",
                params![STORE_FORMAT_KEY, STORE_FORMAT_AVRO],
            )
            .map_err(super::txn::sqlite_error)?;
            Ok(())
        }
    }
}

fn ensure_supported_epoch(conn: &Connection) -> Result<(), MetaError> {
    let value: Option<String> = conn
        .query_row(
            "SELECT CAST(value AS TEXT) FROM meta_provider_schema WHERE key = ?1",
            params![METADATA_EPOCH_KEY],
            |row| row.get(0),
        )
        .optional()
        .map_err(super::txn::sqlite_error)?;
    match value {
        Some(value) => {
            let epoch = value.parse::<i64>().map_err(|err| {
                MetaError::new(
                    MetaErrorKind::ProviderCorruption,
                    format!("metadata epoch `{value}` is not an integer: {err}"),
                )
            })?;
            if epoch > SUPPORTED_METADATA_EPOCH {
                return Err(MetaError::new(
                    MetaErrorKind::ProviderCorruption,
                    format!(
                        "metadata epoch {epoch} is newer than supported epoch {SUPPORTED_METADATA_EPOCH}"
                    ),
                ));
            }
            Ok(())
        }
        None => {
            conn.execute(
                "INSERT INTO meta_provider_schema(key, value) VALUES(?1, ?2)",
                params![METADATA_EPOCH_KEY, SUPPORTED_METADATA_EPOCH.to_string()],
            )
            .map_err(super::txn::sqlite_error)?;
            Ok(())
        }
    }
}
