use rusqlite::Connection;

use crate::meta::MetaError;

pub(super) fn init_schema(conn: &Connection) -> Result<(), MetaError> {
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
            payload_schema_version INTEGER NOT NULL,
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
    .map_err(super::txn::sqlite_error)
}
