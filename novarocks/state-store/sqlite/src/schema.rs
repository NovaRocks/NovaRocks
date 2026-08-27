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

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::{Uuid, Version};

use novarocks_spi::state_store::{StateStoreError, StateStoreErrorKind, StoreIdentity};

use super::sqlite_error;

pub(super) const CURRENT_SCHEMA_VERSION: u32 = 2;
pub(super) const SCHEMA_VERSION_KEY: &[u8] = b"schema_version";
pub(super) const CLUSTER_ID_KEY: &[u8] = b"cluster_id";
pub(super) const STORE_ID_KEY: &[u8] = b"store_id";
pub(super) const CURRENT_REVISION_KEY: &[u8] = b"current_revision";
pub(super) const CHANGE_RETENTION_FLOOR_KEY: &[u8] = b"change_retention_floor";
pub(super) const CHANGE_ROW_COUNT_KEY: &[u8] = b"change_row_count";
pub(super) const COMMIT_RECEIPT_COUNT_KEY: &[u8] = b"commit_receipt_count";
pub(super) const RETIRED_TRANSACTION_ID_MIN_KEY: &[u8] = b"retired_transaction_id_min";
pub(super) const RETIRED_TRANSACTION_ID_MAX_KEY: &[u8] = b"retired_transaction_id_max";
pub(super) const LAST_HISTORY_MAINTENANCE_MS_KEY: &[u8] = b"last_history_maintenance_ms";
pub(super) const PHYSICAL_RECLAIM_PENDING_KEY: &[u8] = b"physical_reclaim_pending";

const INITIAL_REVISION: u64 = 0;
const INITIAL_CHANGE_RETENTION_FLOOR: [u8; 12] = [0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff];
const INITIAL_METADATA_COUNT: u64 = 0;
const INITIAL_PHYSICAL_RECLAIM_PENDING: [u8; 1] = [0];

const META_SCHEMA_SQL: &str = r#"
    CREATE TABLE state_store_meta (
        key BLOB PRIMARY KEY,
        value BLOB NOT NULL
    )
"#;
const KV_SCHEMA_SQL: &str = r#"
    CREATE TABLE state_store_kv (
        key BLOB PRIMARY KEY,
        value BLOB NOT NULL,
        version INTEGER NOT NULL
    )
"#;
const CHANGES_SCHEMA_SQL: &str = r#"
    CREATE TABLE state_store_changes (
        revision INTEGER NOT NULL,
        sequence INTEGER NOT NULL,
        key BLOB NOT NULL,
        committed_at_ms INTEGER NOT NULL,
        PRIMARY KEY(revision, sequence)
    )
"#;
const COMMITS_SCHEMA_SQL: &str = r#"
    CREATE TABLE state_store_commits (
        transaction_id BLOB PRIMARY KEY,
        revision INTEGER NOT NULL,
        committed_at_ms INTEGER NOT NULL
    )
"#;
const CHANGES_COMMITTED_AT_INDEX_SQL: &str = r#"
    CREATE INDEX state_store_changes_committed_at_revision_sequence
    ON state_store_changes(committed_at_ms, revision, sequence)
"#;
const COMMITS_COMMITTED_AT_INDEX_SQL: &str = r#"
    CREATE INDEX state_store_commits_committed_at_revision
    ON state_store_commits(committed_at_ms, revision)
"#;

#[derive(Debug, Eq, PartialEq)]
struct SchemaColumn {
    name: String,
    declared_type: String,
    not_null: bool,
    primary_key_position: i64,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SchemaObject {
    name: String,
    object_type: String,
    table_name: String,
}

const EXPECTED_TABLES: [(&str, &str); 4] = [
    ("state_store_changes", CHANGES_SCHEMA_SQL),
    ("state_store_commits", COMMITS_SCHEMA_SQL),
    ("state_store_kv", KV_SCHEMA_SQL),
    ("state_store_meta", META_SCHEMA_SQL),
];
const EXPECTED_INDEXES: [(&str, &str, &str); 2] = [
    (
        "state_store_changes_committed_at_revision_sequence",
        "state_store_changes",
        CHANGES_COMMITTED_AT_INDEX_SQL,
    ),
    (
        "state_store_commits_committed_at_revision",
        "state_store_commits",
        COMMITS_COMMITTED_AT_INDEX_SQL,
    ),
];
const EXPECTED_META_KEYS: [&[u8]; 11] = [
    SCHEMA_VERSION_KEY,
    CLUSTER_ID_KEY,
    STORE_ID_KEY,
    CURRENT_REVISION_KEY,
    CHANGE_RETENTION_FLOOR_KEY,
    CHANGE_ROW_COUNT_KEY,
    COMMIT_RECEIPT_COUNT_KEY,
    RETIRED_TRANSACTION_ID_MIN_KEY,
    RETIRED_TRANSACTION_ID_MAX_KEY,
    LAST_HISTORY_MAINTENANCE_MS_KEY,
    PHYSICAL_RECLAIM_PENDING_KEY,
];

pub(super) fn initialize(
    connection: &mut Connection,
    cluster_id: &[u8],
) -> Result<StoreIdentity, StateStoreError> {
    let existing_objects = state_store_objects(connection)?;
    if existing_objects.is_empty() {
        configure_auto_vacuum(connection)?;
    } else {
        let version = inspect_schema_version(connection, &existing_objects)?;
        validate_schema_version(&version)?;
        validate_auto_vacuum(connection)?;
    }
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Internal,
                "failed to start SQLite initialization transaction",
            )
        })?;
    let identity = if existing_objects.is_empty() {
        for (_, sql) in EXPECTED_TABLES {
            transaction.execute_batch(sql).map_err(|error| {
                sqlite_error(
                    &error,
                    StateStoreErrorKind::Internal,
                    "failed to create SQLite state store schema",
                )
            })?;
        }
        for (_, _, sql) in EXPECTED_INDEXES {
            transaction.execute_batch(sql).map_err(|error| {
                sqlite_error(
                    &error,
                    StateStoreErrorKind::Internal,
                    "failed to create SQLite state store schema",
                )
            })?;
        }
        initialize_identity(&transaction, cluster_id)?
    } else {
        validate_schema(&transaction, &existing_objects)?;
        load_identity(&transaction, cluster_id)?
    };
    transaction.commit().map_err(|error| {
        sqlite_error(
            &error,
            StateStoreErrorKind::Internal,
            "failed to commit SQLite initialization transaction",
        )
    })?;
    Ok(identity)
}

fn state_store_objects(connection: &Connection) -> Result<Vec<SchemaObject>, StateStoreError> {
    let mut statement = connection
        .prepare(
            "SELECT name, type, tbl_name FROM sqlite_schema \
             WHERE lower(name) GLOB 'state_store_*' \
                OR lower(tbl_name) IN (\
                    'state_store_changes', 'state_store_commits', \
                    'state_store_kv', 'state_store_meta'\
                ) \
                OR (lower(type) = 'view' AND lower(COALESCE(sql, '')) GLOB '*state_store_*') \
             ORDER BY name, type, tbl_name",
        )
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to inspect SQLite state store schema inventory",
            )
        })?;
    statement
        .query_map([], |row| {
            Ok(SchemaObject {
                name: row.get(0)?,
                object_type: row.get(1)?,
                table_name: row.get(2)?,
            })
        })
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to inspect SQLite state store schema inventory",
            )
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to inspect SQLite state store schema inventory",
            )
        })
}

fn inspect_schema_version(
    connection: &Connection,
    objects: &[SchemaObject],
) -> Result<Vec<u8>, StateStoreError> {
    if !objects.iter().any(|object| {
        object.name == "state_store_meta"
            && object.object_type == "table"
            && object.table_name == "state_store_meta"
    }) {
        return Err(schema_error(
            "SQLite state store schema is missing metadata for version detection",
        ));
    }
    connection
        .query_row(
            "SELECT value FROM state_store_meta WHERE key = ?1",
            params![SCHEMA_VERSION_KEY],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to inspect SQLite state store schema version",
            )
        })?
        .ok_or_else(|| schema_error("SQLite state store schema version is missing"))
}

fn configure_auto_vacuum(connection: &Connection) -> Result<(), StateStoreError> {
    connection
        .pragma_update(None, "auto_vacuum", "INCREMENTAL")
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::ProviderUnavailable,
                "failed to configure SQLite state store auto vacuum",
            )
        })?;
    connection.execute_batch("VACUUM").map_err(|error| {
        sqlite_error(
            &error,
            StateStoreErrorKind::ProviderUnavailable,
            "failed to initialize SQLite state store auto vacuum",
        )
    })?;
    validate_auto_vacuum(connection)
}

fn validate_auto_vacuum(connection: &Connection) -> Result<(), StateStoreError> {
    let auto_vacuum = connection
        .pragma_query_value(None, "auto_vacuum", |row| row.get::<_, i64>(0))
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::ProviderUnavailable,
                "failed to inspect SQLite state store auto vacuum",
            )
        })?;
    if auto_vacuum != 2 {
        return Err(StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "SQLite state store auto vacuum mode is not incremental",
        ));
    }
    Ok(())
}

fn validate_schema(
    transaction: &Transaction<'_>,
    objects: &[SchemaObject],
) -> Result<(), StateStoreError> {
    let mut expected_objects = EXPECTED_TABLES
        .iter()
        .flat_map(|(name, _)| {
            [
                SchemaObject {
                    name: (*name).to_owned(),
                    object_type: "table".to_owned(),
                    table_name: (*name).to_owned(),
                },
                SchemaObject {
                    name: format!("sqlite_autoindex_{name}_1"),
                    object_type: "index".to_owned(),
                    table_name: (*name).to_owned(),
                },
            ]
        })
        .collect::<Vec<_>>();
    expected_objects.extend(
        EXPECTED_INDEXES
            .iter()
            .map(|(name, table_name, _)| SchemaObject {
                name: (*name).to_owned(),
                object_type: "index".to_owned(),
                table_name: (*table_name).to_owned(),
            }),
    );
    expected_objects.sort();
    if objects != expected_objects {
        return Err(schema_error(
            "SQLite state store schema inventory is incomplete or unexpected",
        ));
    }

    validate_table(
        transaction,
        "state_store_meta",
        &[("key", "BLOB", false, 1), ("value", "BLOB", true, 0)],
    )?;
    validate_table_sql(transaction, "state_store_meta", META_SCHEMA_SQL)?;
    validate_table(
        transaction,
        "state_store_kv",
        &[
            ("key", "BLOB", false, 1),
            ("value", "BLOB", true, 0),
            ("version", "INTEGER", true, 0),
        ],
    )?;
    validate_table_sql(transaction, "state_store_kv", KV_SCHEMA_SQL)?;
    validate_table(
        transaction,
        "state_store_changes",
        &[
            ("revision", "INTEGER", true, 1),
            ("sequence", "INTEGER", true, 2),
            ("key", "BLOB", true, 0),
            ("committed_at_ms", "INTEGER", true, 0),
        ],
    )?;
    validate_table_sql(transaction, "state_store_changes", CHANGES_SCHEMA_SQL)?;
    validate_index_sql(
        transaction,
        "state_store_changes_committed_at_revision_sequence",
        CHANGES_COMMITTED_AT_INDEX_SQL,
    )?;
    validate_table(
        transaction,
        "state_store_commits",
        &[
            ("transaction_id", "BLOB", false, 1),
            ("revision", "INTEGER", true, 0),
            ("committed_at_ms", "INTEGER", true, 0),
        ],
    )?;
    validate_table_sql(transaction, "state_store_commits", COMMITS_SCHEMA_SQL)?;
    validate_index_sql(
        transaction,
        "state_store_commits_committed_at_revision",
        COMMITS_COMMITTED_AT_INDEX_SQL,
    )
}

fn validate_table_sql(
    transaction: &Transaction<'_>,
    table: &'static str,
    expected: &str,
) -> Result<(), StateStoreError> {
    let actual = transaction
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
            params![table],
            |row| row.get::<_, String>(0),
        )
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to inspect SQLite state store table definition",
            )
        })?;
    if normalize_schema_sql(&actual) != normalize_schema_sql(expected) {
        return Err(schema_error(
            "SQLite state store table constraints are malformed",
        ));
    }
    Ok(())
}

fn validate_index_sql(
    transaction: &Transaction<'_>,
    index: &'static str,
    expected: &str,
) -> Result<(), StateStoreError> {
    let actual = transaction
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = ?1",
            params![index],
            |row| row.get::<_, String>(0),
        )
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to inspect SQLite state store index definition",
            )
        })?;
    if normalize_schema_sql(&actual) != normalize_schema_sql(expected) {
        return Err(schema_error(
            "SQLite state store index constraints are malformed",
        ));
    }
    Ok(())
}

fn normalize_schema_sql(sql: &str) -> String {
    sql.chars()
        .filter(|character| !character.is_ascii_whitespace() && *character != ';')
        .flat_map(char::to_uppercase)
        .collect()
}

fn validate_table(
    transaction: &Transaction<'_>,
    table: &'static str,
    expected: &[(&str, &str, bool, i64)],
) -> Result<(), StateStoreError> {
    let sql = format!("PRAGMA table_info({table})");
    let mut statement = transaction.prepare(&sql).map_err(|error| {
        sqlite_error(
            &error,
            StateStoreErrorKind::Corruption,
            "failed to inspect SQLite state store table schema",
        )
    })?;
    let actual = statement
        .query_map([], |row| {
            Ok(SchemaColumn {
                name: row.get(1)?,
                declared_type: row.get::<_, String>(2)?.to_ascii_uppercase(),
                not_null: row.get::<_, i64>(3)? != 0,
                primary_key_position: row.get(5)?,
            })
        })
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to inspect SQLite state store table schema",
            )
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to inspect SQLite state store table schema",
            )
        })?;
    let expected = expected
        .iter()
        .map(
            |(name, declared_type, not_null, primary_key_position)| SchemaColumn {
                name: (*name).to_owned(),
                declared_type: (*declared_type).to_owned(),
                not_null: *not_null,
                primary_key_position: *primary_key_position,
            },
        )
        .collect::<Vec<_>>();
    if actual != expected {
        return Err(schema_error("SQLite state store table schema is malformed"));
    }
    Ok(())
}

fn initialize_identity(
    transaction: &Transaction<'_>,
    cluster_id: &[u8],
) -> Result<StoreIdentity, StateStoreError> {
    let existing_rows: i64 = transaction
        .query_row("SELECT COUNT(*) FROM state_store_meta", [], |row| {
            row.get(0)
        })
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to inspect SQLite state store identity",
            )
        })?;
    if existing_rows != 0 {
        return Err(schema_error(
            "SQLite state store identity is partially initialized",
        ));
    }

    let store_id = Uuid::now_v7();
    insert_meta(
        transaction,
        SCHEMA_VERSION_KEY,
        &CURRENT_SCHEMA_VERSION.to_be_bytes(),
    )?;
    insert_meta(transaction, CLUSTER_ID_KEY, cluster_id)?;
    insert_meta(transaction, STORE_ID_KEY, store_id.as_bytes())?;
    insert_meta(
        transaction,
        CURRENT_REVISION_KEY,
        &INITIAL_REVISION.to_be_bytes(),
    )?;
    insert_meta(
        transaction,
        CHANGE_RETENTION_FLOOR_KEY,
        &INITIAL_CHANGE_RETENTION_FLOOR,
    )?;
    insert_meta(
        transaction,
        CHANGE_ROW_COUNT_KEY,
        &INITIAL_METADATA_COUNT.to_be_bytes(),
    )?;
    insert_meta(
        transaction,
        COMMIT_RECEIPT_COUNT_KEY,
        &INITIAL_METADATA_COUNT.to_be_bytes(),
    )?;
    insert_meta(transaction, RETIRED_TRANSACTION_ID_MIN_KEY, &[])?;
    insert_meta(transaction, RETIRED_TRANSACTION_ID_MAX_KEY, &[])?;
    insert_meta(
        transaction,
        LAST_HISTORY_MAINTENANCE_MS_KEY,
        &INITIAL_METADATA_COUNT.to_be_bytes(),
    )?;
    insert_meta(
        transaction,
        PHYSICAL_RECLAIM_PENDING_KEY,
        &INITIAL_PHYSICAL_RECLAIM_PENDING,
    )?;

    Ok(StoreIdentity {
        store_id,
        cluster_id: String::from_utf8(cluster_id.to_vec())
            .map_err(|_| schema_error("configured SQLite cluster id is not UTF-8"))?,
    })
}

fn load_identity(
    transaction: &Transaction<'_>,
    cluster_id: &[u8],
) -> Result<StoreIdentity, StateStoreError> {
    let stored_cluster_id = load_required(transaction, CLUSTER_ID_KEY)?;
    if stored_cluster_id != cluster_id {
        return Err(StateStoreError::new(
            StateStoreErrorKind::InvalidConfiguration,
            "SQLite state store cluster id does not match configuration",
        ));
    }

    let store_id = Uuid::from_slice(&load_required(transaction, STORE_ID_KEY)?)
        .map_err(|_| schema_error("SQLite state store id is malformed"))?;
    if store_id.get_version() != Some(Version::SortRand) {
        return Err(schema_error("SQLite state store id is not UUIDv7"));
    }

    let current_revision = decode_u64(
        &load_required(transaction, CURRENT_REVISION_KEY)?,
        "SQLite current revision is malformed",
    )?;
    if current_revision > i64::MAX as u64 {
        return Err(schema_error(
            "SQLite current revision exceeds the supported integer range",
        ));
    }
    let retention_floor =
        decode_change_retention_floor(&load_required(transaction, CHANGE_RETENTION_FLOOR_KEY)?)?;
    validate_change_retention_floor(retention_floor, current_revision)?;
    validate_metadata_inventory(transaction)?;
    let change_row_count = validate_u64_metadata(
        transaction,
        CHANGE_ROW_COUNT_KEY,
        "SQLite change row count is malformed",
    )?;
    let commit_receipt_count = validate_u64_metadata(
        transaction,
        COMMIT_RECEIPT_COUNT_KEY,
        "SQLite commit receipt count is malformed",
    )?;
    validate_history_row_counts(transaction, change_row_count, commit_receipt_count)?;
    validate_retired_transaction_bounds(transaction)?;
    validate_u64_metadata(
        transaction,
        LAST_HISTORY_MAINTENANCE_MS_KEY,
        "SQLite history maintenance timestamp is malformed",
    )?;
    validate_physical_reclaim_pending(transaction)?;

    let cluster_id = String::from_utf8(stored_cluster_id)
        .map_err(|_| schema_error("SQLite cluster id is not UTF-8"))?;
    Ok(StoreIdentity {
        store_id,
        cluster_id,
    })
}

fn validate_history_row_counts(
    transaction: &Transaction<'_>,
    expected_changes: u64,
    expected_commits: u64,
) -> Result<(), StateStoreError> {
    let actual_changes = load_table_count(transaction, "state_store_changes")?;
    let actual_commits = load_table_count(transaction, "state_store_commits")?;
    if actual_changes != expected_changes || actual_commits != expected_commits {
        return Err(schema_error(
            "SQLite history row counters do not match persisted rows",
        ));
    }
    Ok(())
}

fn load_table_count(
    transaction: &Transaction<'_>,
    table: &'static str,
) -> Result<u64, StateStoreError> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    let count = transaction
        .query_row(&sql, [], |row| row.get::<_, i64>(0))
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to inspect SQLite state store history rows",
            )
        })?;
    u64::try_from(count).map_err(|_| schema_error("SQLite history row count is malformed"))
}

fn validate_metadata_inventory(transaction: &Transaction<'_>) -> Result<(), StateStoreError> {
    let mut statement = transaction
        .prepare("SELECT key FROM state_store_meta ORDER BY key")
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to inspect SQLite state store metadata",
            )
        })?;
    let actual = statement
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to inspect SQLite state store metadata",
            )
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to inspect SQLite state store metadata",
            )
        })?;
    let mut expected = EXPECTED_META_KEYS
        .iter()
        .map(|key| key.to_vec())
        .collect::<Vec<_>>();
    expected.sort();
    if actual != expected {
        return Err(schema_error(
            "SQLite state store metadata inventory is incomplete or unexpected",
        ));
    }
    Ok(())
}

fn validate_u64_metadata(
    transaction: &Transaction<'_>,
    key: &[u8],
    message: &'static str,
) -> Result<u64, StateStoreError> {
    decode_u64(&load_required(transaction, key)?, message)
}

fn validate_retired_transaction_bounds(
    transaction: &Transaction<'_>,
) -> Result<(), StateStoreError> {
    let min = load_required(transaction, RETIRED_TRANSACTION_ID_MIN_KEY)?;
    let max = load_required(transaction, RETIRED_TRANSACTION_ID_MAX_KEY)?;
    let min = decode_optional_transaction_id(&min)?;
    let max = decode_optional_transaction_id(&max)?;
    if min.is_some() != max.is_some() || min.zip(max).is_some_and(|(min, max)| min > max) {
        return Err(schema_error(
            "SQLite retired transaction bounds are malformed",
        ));
    }
    Ok(())
}

fn decode_optional_transaction_id(value: &[u8]) -> Result<Option<Uuid>, StateStoreError> {
    if value.is_empty() {
        return Ok(None);
    }
    let transaction_id = Uuid::from_slice(value)
        .map_err(|_| schema_error("SQLite retired transaction id is malformed"))?;
    Ok(Some(transaction_id))
}

fn validate_physical_reclaim_pending(transaction: &Transaction<'_>) -> Result<(), StateStoreError> {
    match load_required(transaction, PHYSICAL_RECLAIM_PENDING_KEY)?.as_slice() {
        [0] | [1] => Ok(()),
        _ => Err(schema_error("SQLite physical reclaim marker is malformed")),
    }
}

pub(super) fn load_change_retention_floor(
    connection: &Connection,
    current_revision: u64,
) -> Result<(u64, u32), StateStoreError> {
    let value = connection
        .query_row(
            "SELECT value FROM state_store_meta WHERE key = ?1",
            params![CHANGE_RETENTION_FLOOR_KEY],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to read SQLite change retention floor",
            )
        })?
        .ok_or_else(|| schema_error("SQLite change retention floor is missing"))?;
    let retention_floor = decode_change_retention_floor(&value)?;
    validate_change_retention_floor(retention_floor, current_revision)?;
    Ok(retention_floor)
}

fn decode_change_retention_floor(value: &[u8]) -> Result<(u64, u32), StateStoreError> {
    let bytes: [u8; 12] = value
        .try_into()
        .map_err(|_| schema_error("SQLite change retention floor is malformed"))?;
    let revision = u64::from_be_bytes(bytes[..8].try_into().expect("fixed revision bytes"));
    if i64::try_from(revision).is_err() {
        return Err(schema_error(
            "SQLite change retention floor revision is out of range",
        ));
    }
    let sequence = u32::from_be_bytes(bytes[8..].try_into().expect("fixed sequence bytes"));
    Ok((revision, sequence))
}

fn validate_change_retention_floor(
    retention_floor: (u64, u32),
    current_revision: u64,
) -> Result<(), StateStoreError> {
    if retention_floor.0 > current_revision {
        return Err(schema_error(
            "SQLite change retention floor is ahead of current revision",
        ));
    }
    Ok(())
}

fn validate_schema_version(value: &[u8]) -> Result<(), StateStoreError> {
    let bytes: [u8; 4] = value
        .try_into()
        .map_err(|_| schema_error("SQLite state store schema version is malformed"))?;
    if u32::from_be_bytes(bytes) != CURRENT_SCHEMA_VERSION {
        return Err(StateStoreError::new(
            StateStoreErrorKind::UnsupportedFormat,
            "SQLite state store schema version is unsupported",
        ));
    }
    Ok(())
}

fn decode_u64(value: &[u8], message: &'static str) -> Result<u64, StateStoreError> {
    let bytes: [u8; 8] = value.try_into().map_err(|_| schema_error(message))?;
    Ok(u64::from_be_bytes(bytes))
}

fn insert_meta(
    transaction: &Transaction<'_>,
    key: &[u8],
    value: &[u8],
) -> Result<(), StateStoreError> {
    transaction
        .execute(
            "INSERT INTO state_store_meta(key, value) VALUES (?1, ?2)",
            params![key, value],
        )
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Internal,
                "failed to initialize SQLite state store identity",
            )
        })?;
    Ok(())
}

fn load_required(transaction: &Transaction<'_>, key: &[u8]) -> Result<Vec<u8>, StateStoreError> {
    load_optional(transaction, key)?
        .ok_or_else(|| schema_error("SQLite state store identity is missing required metadata"))
}

fn load_optional(
    transaction: &Transaction<'_>,
    key: &[u8],
) -> Result<Option<Vec<u8>>, StateStoreError> {
    transaction
        .query_row(
            "SELECT value FROM state_store_meta WHERE key = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to read SQLite state store identity",
            )
        })
}

const fn schema_error(message: &'static str) -> StateStoreError {
    StateStoreError::new(StateStoreErrorKind::Corruption, message)
}
