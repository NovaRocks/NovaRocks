use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use rusqlite::{Connection, OptionalExtension, params};

use crate::meta::{
    ExpectedRevision, IdScope, MetaCommitOutcome, MetaError, MetaErrorKind, MetaKey, MetaKeyPrefix,
    MetaPayload, MetaPayloadEncoding, MetaReadTxn, MetaRecord, MetaRecordKind, MetaRecordPut,
    MetaRevision, MetaWriteTxn,
};

pub(super) struct SqliteReadTxn {
    conn: Connection,
    active: bool,
}

impl SqliteReadTxn {
    pub(super) fn begin(conn: Connection) -> Result<Self, MetaError> {
        conn.execute_batch("BEGIN DEFERRED").map_err(sqlite_error)?;
        let _: i64 = conn
            .query_row("SELECT COUNT(*) FROM meta_records", [], |row| row.get(0))
            .map_err(sqlite_error)?;
        Ok(Self { conn, active: true })
    }
}

impl MetaReadTxn for SqliteReadTxn {
    fn get(&self, key: &MetaKey) -> Result<Option<MetaRecord>, MetaError> {
        get_record(&self.conn, key)
    }

    fn scan(
        &self,
        prefix: &MetaKeyPrefix,
        limit: Option<usize>,
    ) -> Result<Vec<MetaRecord>, MetaError> {
        scan_records(&self.conn, prefix, limit)
    }
}

impl Drop for SqliteReadTxn {
    fn drop(&mut self) {
        if self.active {
            let _ = self.conn.execute_batch("ROLLBACK");
        }
    }
}

pub(super) struct SqliteWriteTxn {
    conn: Connection,
    active: bool,
}

impl SqliteWriteTxn {
    pub(super) fn begin(conn: Connection, _purpose: &str) -> Result<Self, MetaError> {
        conn.execute_batch("BEGIN IMMEDIATE")
            .map_err(sqlite_error)?;
        Ok(Self { conn, active: true })
    }
}

impl MetaReadTxn for SqliteWriteTxn {
    fn get(&self, key: &MetaKey) -> Result<Option<MetaRecord>, MetaError> {
        get_record(&self.conn, key)
    }

    fn scan(
        &self,
        prefix: &MetaKeyPrefix,
        limit: Option<usize>,
    ) -> Result<Vec<MetaRecord>, MetaError> {
        scan_records(&self.conn, prefix, limit)
    }
}

impl MetaWriteTxn for SqliteWriteTxn {
    fn put(&mut self, record: MetaRecordPut) -> Result<(), MetaError> {
        match record.expected.clone() {
            ExpectedRevision::Any => put_record_any(&self.conn, record),
            ExpectedRevision::NotExists => insert_record(&self.conn, record),
            ExpectedRevision::Exists => update_record_exists(&self.conn, record),
            ExpectedRevision::Exact(revision) => update_record_exact(&self.conn, record, revision),
        }
    }

    fn delete(&mut self, key: &MetaKey, expected: ExpectedRevision) -> Result<(), MetaError> {
        delete_record(&self.conn, key, expected)
    }

    fn allocate_id(&mut self, scope: IdScope) -> Result<i64, MetaError> {
        allocate_id(&self.conn, scope)
    }

    fn commit(mut self: Box<Self>) -> Result<MetaCommitOutcome, MetaError> {
        self.conn.execute_batch("COMMIT").map_err(sqlite_error)?;
        self.active = false;
        Ok(MetaCommitOutcome {
            provider_revision: None,
            committed_at_ms: now_ms(),
        })
    }

    fn abort(mut self: Box<Self>) -> Result<(), MetaError> {
        self.conn.execute_batch("ROLLBACK").map_err(sqlite_error)?;
        self.active = false;
        Ok(())
    }
}

impl Drop for SqliteWriteTxn {
    fn drop(&mut self) {
        if self.active {
            let _ = self.conn.execute_batch("ROLLBACK");
        }
    }
}

fn get_record(conn: &Connection, key: &MetaKey) -> Result<Option<MetaRecord>, MetaError> {
    conn.query_row(
        r#"
        SELECT kind, revision, payload_encoding, payload_schema_version, payload,
               created_at_ms, updated_at_ms
        FROM meta_records
        WHERE namespace = ?1 AND key = ?2
        "#,
        params![key.namespace(), key.canonical_path()],
        |row| {
            let kind = MetaRecordKind::new(row.get::<_, String>(0)?).map_err(to_sql_error)?;
            let revision = MetaRevision::from_sqlite_i64(row.get::<_, i64>(1)?);
            let encoding =
                MetaPayloadEncoding::parse(&row.get::<_, String>(2)?).map_err(to_sql_error)?;
            let schema_version = row.get::<_, i32>(3)?;
            let payload = Bytes::from(row.get::<_, Vec<u8>>(4)?);
            Ok(MetaRecord {
                key: key.clone(),
                kind,
                revision,
                payload: MetaPayload {
                    encoding,
                    schema_version,
                    bytes: payload,
                },
                created_at_ms: row.get(5)?,
                updated_at_ms: row.get(6)?,
            })
        },
    )
    .optional()
    .map_err(sqlite_error)
}

fn scan_records(
    conn: &Connection,
    prefix: &MetaKeyPrefix,
    limit: Option<usize>,
) -> Result<Vec<MetaRecord>, MetaError> {
    let prefix_path = prefix.canonical_path();
    let mut records = if prefix_path.is_empty() {
        let mut stmt = conn
            .prepare(
                r#"
                SELECT key, kind, revision, payload_encoding, payload_schema_version, payload,
                       created_at_ms, updated_at_ms
                FROM meta_records
                WHERE namespace = ?1
                ORDER BY key
                "#,
            )
            .map_err(sqlite_error)?;
        let rows = stmt
            .query_map(params![prefix.namespace()], |row| {
                row_to_record(prefix.namespace(), row)
            })
            .map_err(sqlite_error)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(sqlite_error)?
    } else {
        let subtree_lower = format!("{prefix_path}/");
        let subtree_upper = next_ascii_prefix(&subtree_lower).ok_or_else(|| {
            MetaError::new(
                MetaErrorKind::InvalidRequest,
                format!("metadata scan prefix `{prefix_path}` has no finite upper bound"),
            )
        })?;
        let mut stmt = conn
            .prepare(
                r#"
                SELECT key, kind, revision, payload_encoding, payload_schema_version, payload,
                       created_at_ms, updated_at_ms
                FROM meta_records
                WHERE namespace = ?1
                  AND (key = ?2 OR (key >= ?3 AND key < ?4))
                ORDER BY key
                "#,
            )
            .map_err(sqlite_error)?;
        let rows = stmt
            .query_map(
                params![
                    prefix.namespace(),
                    prefix_path,
                    subtree_lower,
                    subtree_upper
                ],
                |row| row_to_record(prefix.namespace(), row),
            )
            .map_err(sqlite_error)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(sqlite_error)?
    };
    if let Some(limit) = limit {
        records.truncate(limit);
    }
    Ok(records)
}

fn row_to_record(namespace: &str, row: &rusqlite::Row<'_>) -> Result<MetaRecord, rusqlite::Error> {
    let key = MetaKey::from_canonical(namespace, row.get::<_, String>(0)?).map_err(to_sql_error)?;
    let kind = MetaRecordKind::new(row.get::<_, String>(1)?).map_err(to_sql_error)?;
    let revision = MetaRevision::from_sqlite_i64(row.get::<_, i64>(2)?);
    let encoding = MetaPayloadEncoding::parse(&row.get::<_, String>(3)?).map_err(to_sql_error)?;
    let schema_version = row.get::<_, i32>(4)?;
    let payload = Bytes::from(row.get::<_, Vec<u8>>(5)?);
    Ok(MetaRecord {
        key,
        kind,
        revision,
        payload: MetaPayload {
            encoding,
            schema_version,
            bytes: payload,
        },
        created_at_ms: row.get(6)?,
        updated_at_ms: row.get(7)?,
    })
}

fn insert_record(conn: &Connection, record: MetaRecordPut) -> Result<(), MetaError> {
    let now = now_ms();
    conn.execute(
        r#"
        INSERT INTO meta_records(
            namespace, key, kind, revision, payload_encoding, payload_schema_version,
            payload, created_at_ms, updated_at_ms
        )
        VALUES (?1, ?2, ?3, 1, ?4, ?5, ?6, ?7, ?7)
        "#,
        params![
            record.key.namespace(),
            record.key.canonical_path(),
            record.kind.as_str(),
            record.payload.encoding.as_str(),
            record.payload.schema_version,
            record.payload.bytes.as_ref(),
            now,
        ],
    )
    .map(|_| ())
    .map_err(|err| {
        if is_constraint_error(&err) {
            MetaError::new(
                MetaErrorKind::AlreadyExists,
                format!(
                    "metadata record `{}` already exists",
                    record.key.canonical_path()
                ),
            )
        } else {
            sqlite_error(err)
        }
    })
}

fn update_record_exact(
    conn: &Connection,
    record: MetaRecordPut,
    revision: MetaRevision,
) -> Result<(), MetaError> {
    let expected_revision = revision.to_sqlite_i64()?;
    let now = now_ms();
    let rows = conn
        .execute(
            r#"
            UPDATE meta_records
               SET kind = ?1,
                   revision = revision + 1,
                   payload_encoding = ?2,
                   payload_schema_version = ?3,
                   payload = ?4,
                   updated_at_ms = ?5
             WHERE namespace = ?6 AND key = ?7 AND revision = ?8
            "#,
            params![
                record.kind.as_str(),
                record.payload.encoding.as_str(),
                record.payload.schema_version,
                record.payload.bytes.as_ref(),
                now,
                record.key.namespace(),
                record.key.canonical_path(),
                expected_revision,
            ],
        )
        .map_err(sqlite_error)?;
    if rows == 1 {
        Ok(())
    } else {
        Err(MetaError::new(
            MetaErrorKind::Conflict,
            format!(
                "metadata record `{}` revision did not match",
                record.key.canonical_path()
            ),
        ))
    }
}

fn put_record_any(conn: &Connection, mut record: MetaRecordPut) -> Result<(), MetaError> {
    if get_record(conn, &record.key)?.is_some() {
        record.expected = ExpectedRevision::Exists;
        update_record_exists(conn, record)
    } else {
        record.expected = ExpectedRevision::NotExists;
        insert_record(conn, record)
    }
}

fn update_record_exists(conn: &Connection, record: MetaRecordPut) -> Result<(), MetaError> {
    let now = now_ms();
    let rows = conn
        .execute(
            r#"
            UPDATE meta_records
               SET kind = ?1,
                   revision = revision + 1,
                   payload_encoding = ?2,
                   payload_schema_version = ?3,
                   payload = ?4,
                   updated_at_ms = ?5
             WHERE namespace = ?6 AND key = ?7
            "#,
            params![
                record.kind.as_str(),
                record.payload.encoding.as_str(),
                record.payload.schema_version,
                record.payload.bytes.as_ref(),
                now,
                record.key.namespace(),
                record.key.canonical_path(),
            ],
        )
        .map_err(sqlite_error)?;
    if rows == 1 {
        Ok(())
    } else {
        Err(MetaError::new(
            MetaErrorKind::NotFound,
            format!(
                "metadata record `{}` was not found",
                record.key.canonical_path()
            ),
        ))
    }
}

fn delete_record(
    conn: &Connection,
    key: &MetaKey,
    expected: ExpectedRevision,
) -> Result<(), MetaError> {
    match expected {
        ExpectedRevision::Any => conn
            .execute(
                "DELETE FROM meta_records WHERE namespace = ?1 AND key = ?2",
                params![key.namespace(), key.canonical_path()],
            )
            .map(|_| ())
            .map_err(sqlite_error),
        ExpectedRevision::Exists => {
            let rows = conn
                .execute(
                    "DELETE FROM meta_records WHERE namespace = ?1 AND key = ?2",
                    params![key.namespace(), key.canonical_path()],
                )
                .map_err(sqlite_error)?;
            if rows == 1 {
                Ok(())
            } else {
                Err(MetaError::new(
                    MetaErrorKind::NotFound,
                    format!("metadata record `{}` was not found", key.canonical_path()),
                ))
            }
        }
        ExpectedRevision::Exact(revision) => {
            let rows = conn
                .execute(
                    "DELETE FROM meta_records WHERE namespace = ?1 AND key = ?2 AND revision = ?3",
                    params![
                        key.namespace(),
                        key.canonical_path(),
                        revision.to_sqlite_i64()?,
                    ],
                )
                .map_err(sqlite_error)?;
            if rows == 1 {
                Ok(())
            } else {
                Err(MetaError::new(
                    MetaErrorKind::Conflict,
                    format!(
                        "metadata record `{}` revision did not match",
                        key.canonical_path()
                    ),
                ))
            }
        }
        ExpectedRevision::NotExists => Err(MetaError::new(
            MetaErrorKind::InvalidRequest,
            "ExpectedRevision::NotExists is invalid for metadata delete",
        )),
    }
}

fn allocate_id(conn: &Connection, scope: IdScope) -> Result<i64, MetaError> {
    let current = conn
        .query_row(
            "SELECT next_id FROM meta_id_scopes WHERE scope = ?1",
            params![scope.as_str()],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(sqlite_error)?;
    match current {
        Some(next_id) if next_id > 0 => {
            conn.execute(
                "UPDATE meta_id_scopes SET next_id = ?1 WHERE scope = ?2",
                params![next_id + 1, scope.as_str()],
            )
            .map_err(sqlite_error)?;
            Ok(next_id)
        }
        Some(next_id) => Err(MetaError::new(
            MetaErrorKind::ProviderCorruption,
            format!(
                "metadata id scope `{}` has invalid next_id {next_id}",
                scope.as_str()
            ),
        )),
        None => {
            conn.execute(
                "INSERT INTO meta_id_scopes(scope, next_id) VALUES (?1, 2)",
                params![scope.as_str()],
            )
            .map_err(sqlite_error)?;
            Ok(1)
        }
    }
}

pub(super) fn sqlite_error(err: rusqlite::Error) -> MetaError {
    match &err {
        rusqlite::Error::SqliteFailure(code, _)
            if matches!(
                code.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            ) =>
        {
            MetaError::new(MetaErrorKind::Transient, err.to_string())
        }
        rusqlite::Error::SqliteFailure(_, _) => {
            MetaError::new(MetaErrorKind::ProviderCorruption, err.to_string())
        }
        _ => MetaError::new(MetaErrorKind::ProviderCorruption, err.to_string()),
    }
}

fn is_constraint_error(err: &rusqlite::Error) -> bool {
    matches!(
        err,
        rusqlite::Error::SqliteFailure(code, _)
            if code.code == rusqlite::ErrorCode::ConstraintViolation
    )
}

fn next_ascii_prefix(value: &str) -> Option<String> {
    let mut bytes = value.as_bytes().to_vec();
    for idx in (0..bytes.len()).rev() {
        if bytes[idx] < 0x7f {
            bytes[idx] += 1;
            bytes.truncate(idx + 1);
            return String::from_utf8(bytes).ok();
        }
    }
    None
}

fn to_sql_error(err: MetaError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
