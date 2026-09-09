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

// Design: ADR-0122 (docs/adr/ADR-0122-sqlite-is-the-only-production-state-store.md)

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::{Uuid, Version};

use novarocks_state_store_api::{StateStoreError, StateStoreErrorKind, StoreIdentity};

use super::sqlite_error;

pub(super) const CURRENT_SCHEMA_VERSION: u32 = 3;
pub(super) const SCHEMA_VERSION_KEY: &[u8] = b"schema_version";
pub(super) const CLUSTER_ID_KEY: &[u8] = b"cluster_id";
pub(super) const STORE_ID_KEY: &[u8] = b"store_id";
pub(super) const CURRENT_REVISION_KEY: &[u8] = b"current_revision";

const INITIAL_REVISION: u64 = 0;

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
const COMMITS_SCHEMA_SQL: &str = r#"
    CREATE TABLE state_store_commits (
        attempt BLOB PRIMARY KEY,
        revision INTEGER NOT NULL
    )
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

const EXPECTED_TABLES: [(&str, &str); 3] = [
    ("state_store_commits", COMMITS_SCHEMA_SQL),
    ("state_store_kv", KV_SCHEMA_SQL),
    ("state_store_meta", META_SCHEMA_SQL),
];
const EXPECTED_META_KEYS: [&[u8]; 4] = [
    SCHEMA_VERSION_KEY,
    CLUSTER_ID_KEY,
    STORE_ID_KEY,
    CURRENT_REVISION_KEY,
];

pub(super) fn initialize(
    connection: &mut Connection,
    cluster_id: &[u8],
) -> Result<StoreIdentity, StateStoreError> {
    let existing_objects = state_store_objects(connection)?;
    if existing_objects.is_empty() {
        // An empty file becomes a store; a file holding somebody else's tables
        // does not. Adopting one would vacuum it and add tables to it, which is
        // the same class of damage as migrating a version this build cannot
        // read -- and a mistyped path is how it would happen.
        reject_foreign_database(connection)?;
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
                    'state_store_commits', 'state_store_kv', 'state_store_meta'\
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

/// Refuses a database that already holds something this build did not write.
fn reject_foreign_database(connection: &Connection) -> Result<(), StateStoreError> {
    let objects = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to inspect SQLite database contents",
            )
        })?;
    if objects != 0 {
        return Err(StateStoreError::new(
            StateStoreErrorKind::UnsupportedFormat,
            "SQLite path holds a database that is not a NovaRocks state store",
        ));
    }
    Ok(())
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
    // Schema v3 declares no explicit index. Both former indexes only served
    // sweeping history by age; commit evidence is released by attempt through
    // its primary key, and the change feed is gone. Every object below is
    // therefore a table or its primary-key autoindex, and anything else in the
    // file is unexpected.
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
        "state_store_commits",
        &[
            ("attempt", "BLOB", false, 1),
            ("revision", "INTEGER", true, 0),
        ],
    )?;
    validate_table_sql(transaction, "state_store_commits", COMMITS_SCHEMA_SQL)
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
    validate_metadata_inventory(transaction)?;

    let cluster_id = String::from_utf8(stored_cluster_id)
        .map_err(|_| schema_error("SQLite cluster id is not UTF-8"))?;
    Ok(StoreIdentity {
        store_id,
        cluster_id,
    })
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
// Design: ADR-0122 (docs/adr/ADR-0122-sqlite-is-the-only-production-state-store.md)
