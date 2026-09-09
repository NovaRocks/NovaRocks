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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use mysql_async::{Conn, Params, prelude::Queryable};
use sha2::{Digest, Sha256};
use tokio::time::Instant;
use uuid::Uuid;

use super::MysqlOpenCancellation;
use super::client::{
    MysqlPoolConnection, PoolLifecycle, checkout_hygienic_connection, execute_owned_with_deadline,
};
use super::codec::MysqlCodec;
use super::identity::{
    CHANGE_RETENTION_FLOOR_KEY, CLUSTER_ID_KEY, CURRENT_REVISION_KEY, INITIAL_INCARNATION_KEY,
    MysqlIdentitySnapshot, SCHEMA_DIGEST_KEY, SCHEMA_VERSION_KEY, STORE_ID_KEY, advisory_lock_name,
    decode_meta_rows, initial_meta_rows, validate_cluster_id,
};
#[cfg(feature = "state-store-test-hooks")]
use super::open_test_hooks::{MysqlOpenGatePhase, take_mysql_open_gate};
use novarocks_state_store_api::{StateStoreError, StateStoreErrorKind};

const SCHEMA_MANIFEST: &str = concat!(
    "CREATE TABLE state_store_meta (\n",
    "    meta_key VARBINARY(64) NOT NULL,\n",
    "    meta_value VARBINARY(4096) NOT NULL,\n",
    "    PRIMARY KEY (meta_key)\n",
    ") ENGINE=InnoDB ROW_FORMAT=DYNAMIC;\n",
    "CREATE TABLE state_store_kv (\n",
    "    key_bytes VARBINARY(3072) NOT NULL,\n",
    "    value_bytes MEDIUMBLOB NOT NULL,\n",
    "    version_bytes BINARY(12) NOT NULL,\n",
    "    PRIMARY KEY (key_bytes)\n",
    ") ENGINE=InnoDB ROW_FORMAT=DYNAMIC;\n",
    "CREATE TABLE state_store_changes (\n",
    "    revision BIGINT UNSIGNED NOT NULL,\n",
    "    sequence INT UNSIGNED NOT NULL,\n",
    "    key_bytes VARBINARY(3072) NOT NULL,\n",
    "    PRIMARY KEY (revision, sequence)\n",
    ") ENGINE=InnoDB ROW_FORMAT=DYNAMIC;\n",
    "CREATE TABLE state_store_commits (\n",
    "    transaction_id BINARY(16) NOT NULL,\n",
    "    state TINYINT UNSIGNED NOT NULL,\n",
    "    reservation_token BINARY(16) NULL,\n",
    "    revision BIGINT UNSIGNED NULL,\n",
    "    updated_at_ms BIGINT UNSIGNED NOT NULL,\n",
    "    PRIMARY KEY (transaction_id)\n",
    ") ENGINE=InnoDB ROW_FORMAT=DYNAMIC;\n",
    "meta:schema_version=u32be(1),schema_digest=lower_hex_sha256,",
    "store_id=uuidv7_raw16,cluster_id=utf8,initial_incarnation=u64be(1),",
    "current_revision=u64be(0),change_retention_floor=cursor_be(0,4294967295)\n",
);

const META_SCHEMA_SQL: &str = "CREATE TABLE state_store_meta (
    meta_key VARBINARY(64) NOT NULL,
    meta_value VARBINARY(4096) NOT NULL,
    PRIMARY KEY (meta_key)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC";
const KV_SCHEMA_SQL: &str = "CREATE TABLE state_store_kv (
    key_bytes VARBINARY(3072) NOT NULL,
    value_bytes MEDIUMBLOB NOT NULL,
    version_bytes BINARY(12) NOT NULL,
    PRIMARY KEY (key_bytes)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC";
const CHANGES_SCHEMA_SQL: &str = "CREATE TABLE state_store_changes (
    revision BIGINT UNSIGNED NOT NULL,
    sequence INT UNSIGNED NOT NULL,
    key_bytes VARBINARY(3072) NOT NULL,
    PRIMARY KEY (revision, sequence)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC";
const COMMITS_SCHEMA_SQL: &str = "CREATE TABLE state_store_commits (
    transaction_id BINARY(16) NOT NULL,
    state TINYINT UNSIGNED NOT NULL,
    reservation_token BINARY(16) NULL,
    revision BIGINT UNSIGNED NULL,
    updated_at_ms BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (transaction_id)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC";

const TABLES_SQL: &str = "SELECT TABLE_NAME, TABLE_TYPE, ENGINE, ROW_FORMAT
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = DATABASE()
    ORDER BY TABLE_NAME";
const COLUMNS_SQL: &str = "SELECT c.COLUMN_NAME, c.COLUMN_TYPE, c.IS_NULLABLE,
        COALESCE(p.SEQ_IN_INDEX, 0)
    FROM information_schema.COLUMNS c
    LEFT JOIN information_schema.STATISTICS p
      ON p.TABLE_SCHEMA = c.TABLE_SCHEMA
     AND p.TABLE_NAME = c.TABLE_NAME
     AND p.INDEX_NAME = 'PRIMARY'
     AND p.COLUMN_NAME = c.COLUMN_NAME
    WHERE c.TABLE_SCHEMA = DATABASE() AND c.TABLE_NAME = ?
    ORDER BY c.ORDINAL_POSITION";
const INDEXES_SQL: &str = "SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX,
        COLUMN_NAME, SUB_PART, INDEX_TYPE
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
    ORDER BY INDEX_NAME, SEQ_IN_INDEX";
const TRIGGERS_SQL: &str = "SELECT TRIGGER_NAME
    FROM information_schema.TRIGGERS
    WHERE TRIGGER_SCHEMA = DATABASE()
    ORDER BY TRIGGER_NAME";
const META_ROWS_SQL: &str = "SELECT meta_key, meta_value FROM state_store_meta ORDER BY meta_key";
const ADVISORY_LOCK_CLEANUP_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaColumnSnapshot {
    pub name: String,
    pub column_type: String,
    pub nullable: bool,
    pub primary_key_position: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaTableSnapshot {
    pub name: String,
    pub engine: String,
    pub row_format: String,
    pub columns: Vec<SchemaColumnSnapshot>,
    pub primary_key: Vec<String>,
    pub secondary_indexes: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
pub struct SchemaSnapshot {
    pub tables: Vec<SchemaTableSnapshot>,
    pub views: Vec<String>,
    pub triggers: Vec<String>,
    pub meta_keys: Vec<String>,
    pub schema_version: u32,
    pub schema_digest: String,
    pub store_id: Uuid,
    pub cluster_id: String,
    pub initial_incarnation: u64,
    pub current_revision: u64,
    pub change_retention_floor: (u64, u32),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoreReadinessSnapshot {
    pub read_only_started_and_rolled_back: bool,
    pub write_started_and_rolled_back: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
pub enum SchemaMutation {
    CreatePartialMetaTable,
    CreateExtraTable,
    DriftEngine,
    DriftRowFormat,
    DriftColumn,
    DriftIndex,
    DeleteSchemaVersion,
    MalformedSchemaVersion,
    OlderSchemaVersion,
    NewerSchemaVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SchemaInventory {
    tables: Vec<SchemaTableSnapshot>,
    views: Vec<String>,
    triggers: Vec<String>,
}

struct SchemaSession {
    connection: Option<MysqlPoolConnection>,
    deadline: Instant,
}

pub(super) async fn bootstrap_and_validate(
    pool: Arc<dyn PoolLifecycle>,
    database: &str,
    cluster_id: &str,
    max_key_bytes: usize,
    deadline: Instant,
    cancellation: &MysqlOpenCancellation,
) -> Result<MysqlIdentitySnapshot, StateStoreError> {
    validate_cluster_id(cluster_id)?;
    let codec = MysqlCodec::new(max_key_bytes)?;
    let connection = checkout_hygienic_connection(pool, deadline).await?;
    let mut session = SchemaSession::new(connection, deadline);
    let lock_name = session.acquire_advisory_lock(database).await?;
    #[cfg(feature = "state-store-test-hooks")]
    let open_gate = take_mysql_open_gate(database, MysqlOpenGatePhase::AfterAdvisoryLock);
    #[cfg(feature = "state-store-test-hooks")]
    if let Some(gate) = open_gate.as_ref() {
        gate.pause(session.connection_id().await?).await;
    }
    if let Err(error) = cancellation.check() {
        session.destroy_connection().await;
        return Err(error);
    }
    let result = bootstrap_locked(&mut session, &codec, cluster_id).await;
    if session.has_connection() {
        let release = session
            .release_advisory_lock(&lock_name, Instant::now() + ADVISORY_LOCK_CLEANUP_TIMEOUT)
            .await;
        match (result, release) {
            (Ok(identity), Ok(())) => Ok(identity),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    } else {
        result
    }
}

pub(crate) async fn validate_store_readiness(
    pool: Arc<dyn PoolLifecycle>,
    database: &str,
    cluster_id: &str,
    max_key_bytes: usize,
    deadline: Instant,
    cancellation: &MysqlOpenCancellation,
) -> Result<(MysqlIdentitySnapshot, StoreReadinessSnapshot), StateStoreError> {
    let identity = bootstrap_and_validate(
        Arc::clone(&pool),
        database,
        cluster_id,
        max_key_bytes,
        deadline,
        cancellation,
    )
    .await?;
    run_transaction_readiness(
        Arc::clone(&pool),
        database,
        "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY",
        true,
        deadline,
        cancellation,
    )
    .await?;
    run_transaction_readiness(
        pool,
        database,
        "START TRANSACTION WITH CONSISTENT SNAPSHOT",
        false,
        deadline,
        cancellation,
    )
    .await?;
    Ok((
        identity,
        StoreReadinessSnapshot {
            read_only_started_and_rolled_back: true,
            write_started_and_rolled_back: true,
        },
    ))
}

async fn run_transaction_readiness(
    pool: Arc<dyn PoolLifecycle>,
    database: &str,
    start_sql: &'static str,
    is_read_only: bool,
    deadline: Instant,
    cancellation: &MysqlOpenCancellation,
) -> Result<(), StateStoreError> {
    let connection = checkout_hygienic_connection(pool, deadline).await?;
    let mut session = SchemaSession::new(connection, deadline);
    session
        .run(|connection| {
            Box::pin(connection.query_drop("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        })
        .await?;
    session
        .run(move |connection| Box::pin(connection.query_drop(start_sql)))
        .await?;
    #[cfg(feature = "state-store-test-hooks")]
    let open_gate = is_read_only
        .then(|| take_mysql_open_gate(database, MysqlOpenGatePhase::AfterReadOnlyStart))
        .flatten();
    #[cfg(feature = "state-store-test-hooks")]
    if let Some(gate) = open_gate.as_ref() {
        gate.pause(session.connection_id().await?).await;
    }
    #[cfg(not(feature = "state-store-test-hooks"))]
    let _ = (database, is_read_only);
    if let Err(error) = cancellation.check() {
        session.destroy_connection().await;
        return Err(error);
    }
    if let Err(error) = session
        .run(|connection| Box::pin(connection.query_drop("ROLLBACK")))
        .await
    {
        session.destroy_connection().await;
        return Err(error);
    }
    Ok(())
}

async fn bootstrap_locked(
    session: &mut SchemaSession,
    codec: &MysqlCodec,
    cluster_id: &str,
) -> Result<MysqlIdentitySnapshot, StateStoreError> {
    let inventory = load_inventory(session).await?;
    if inventory.tables.is_empty() && inventory.views.is_empty() && inventory.triggers.is_empty() {
        create_schema(session, codec, cluster_id).await?;
    } else {
        validate_inventory(&inventory)?;
    }

    let inventory = load_inventory(session).await?;
    validate_inventory(&inventory)?;
    let rows = load_meta_rows(session).await?;
    decode_meta_rows(codec, rows, cluster_id, &schema_digest())
}

async fn create_schema(
    session: &mut SchemaSession,
    codec: &MysqlCodec,
    cluster_id: &str,
) -> Result<(), StateStoreError> {
    for sql in [
        META_SCHEMA_SQL,
        KV_SCHEMA_SQL,
        CHANGES_SCHEMA_SQL,
        COMMITS_SCHEMA_SQL,
    ] {
        session
            .run(move |connection| Box::pin(connection.query_drop(sql)))
            .await?;
    }

    session
        .run(|connection| Box::pin(connection.query_drop("START TRANSACTION")))
        .await?;
    let rows = initial_meta_rows(codec, cluster_id, &schema_digest());
    for (key, value) in rows {
        let insert = session
            .run(move |connection| {
                Box::pin(connection.exec_drop(
                    "INSERT INTO state_store_meta (meta_key, meta_value) VALUES (?, ?)",
                    (key, value),
                ))
            })
            .await;
        if let Err(error) = insert {
            if session.has_connection() {
                session.rollback_after_failure().await;
            }
            return Err(error);
        }
    }
    if let Err(error) = session
        .run(|connection| Box::pin(connection.query_drop("COMMIT")))
        .await
    {
        if session.has_connection() {
            session.rollback_after_failure().await;
        }
        return Err(error);
    }
    Ok(())
}

async fn load_inventory(session: &mut SchemaSession) -> Result<SchemaInventory, StateStoreError> {
    let rows: Vec<(String, String, Option<String>, Option<String>)> = session
        .run(|connection| Box::pin(connection.query(TABLES_SQL)))
        .await?;
    let mut tables = Vec::new();
    let mut views = Vec::new();
    for (name, table_type, engine, row_format) in rows {
        if table_type == "VIEW" {
            views.push(name);
            continue;
        }
        if table_type != "BASE TABLE" {
            return Err(schema_corruption());
        }
        tables.push(
            load_table(
                session,
                name,
                engine.unwrap_or_default(),
                row_format.unwrap_or_default(),
            )
            .await?,
        );
    }
    let triggers: Vec<String> = session
        .run(|connection| Box::pin(connection.query(TRIGGERS_SQL)))
        .await?;
    Ok(SchemaInventory {
        tables,
        views,
        triggers,
    })
}

async fn load_table(
    session: &mut SchemaSession,
    name: String,
    engine: String,
    row_format: String,
) -> Result<SchemaTableSnapshot, StateStoreError> {
    let column_name = name.clone();
    let column_rows: Vec<(String, String, String, u64)> = session
        .run(move |connection| Box::pin(connection.exec(COLUMNS_SQL, (column_name,))))
        .await?;
    let columns = column_rows
        .into_iter()
        .map(|(name, column_type, nullable, primary_key_position)| {
            Ok(SchemaColumnSnapshot {
                name,
                column_type,
                nullable: nullable == "YES",
                primary_key_position: usize::try_from(primary_key_position)
                    .map_err(|_| schema_corruption())?,
            })
        })
        .collect::<Result<Vec<_>, StateStoreError>>()?;

    let index_name = name.clone();
    let index_rows: Vec<(String, u8, u64, String, Option<u64>, String)> = session
        .run(move |connection| Box::pin(connection.exec(INDEXES_SQL, (index_name,))))
        .await?;
    let mut primary_key = Vec::new();
    let mut secondary_indexes = BTreeSet::new();
    for (index_name, non_unique, sequence, column_name, sub_part, index_type) in index_rows {
        if index_name == "PRIMARY" {
            let expected_sequence =
                u64::try_from(primary_key.len() + 1).map_err(|_| schema_corruption())?;
            if non_unique != 0
                || sequence != expected_sequence
                || sub_part.is_some()
                || !index_type.eq_ignore_ascii_case("BTREE")
            {
                return Err(schema_corruption());
            }
            primary_key.push(column_name);
        } else {
            secondary_indexes.insert(index_name);
        }
    }

    Ok(SchemaTableSnapshot {
        name,
        engine,
        row_format,
        columns,
        primary_key,
        secondary_indexes: secondary_indexes.into_iter().collect(),
    })
}

async fn load_meta_rows(
    session: &mut SchemaSession,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, StateStoreError> {
    session
        .run(|connection| Box::pin(connection.query(META_ROWS_SQL)))
        .await
}

fn validate_inventory(inventory: &SchemaInventory) -> Result<(), StateStoreError> {
    if inventory.tables != expected_tables()
        || !inventory.views.is_empty()
        || !inventory.triggers.is_empty()
    {
        return Err(schema_corruption());
    }
    Ok(())
}

fn expected_tables() -> Vec<SchemaTableSnapshot> {
    vec![
        SchemaTableSnapshot {
            name: "state_store_changes".to_owned(),
            engine: "InnoDB".to_owned(),
            row_format: "Dynamic".to_owned(),
            columns: vec![
                SchemaColumnSnapshot::new("revision", "bigint unsigned", false, 1),
                SchemaColumnSnapshot::new("sequence", "int unsigned", false, 2),
                SchemaColumnSnapshot::new("key_bytes", "varbinary(3072)", false, 0),
            ],
            primary_key: vec!["revision".to_owned(), "sequence".to_owned()],
            secondary_indexes: Vec::new(),
        },
        SchemaTableSnapshot {
            name: "state_store_commits".to_owned(),
            engine: "InnoDB".to_owned(),
            row_format: "Dynamic".to_owned(),
            columns: vec![
                SchemaColumnSnapshot::new("transaction_id", "binary(16)", false, 1),
                SchemaColumnSnapshot::new("state", "tinyint unsigned", false, 0),
                SchemaColumnSnapshot::new("reservation_token", "binary(16)", true, 0),
                SchemaColumnSnapshot::new("revision", "bigint unsigned", true, 0),
                SchemaColumnSnapshot::new("updated_at_ms", "bigint unsigned", false, 0),
            ],
            primary_key: vec!["transaction_id".to_owned()],
            secondary_indexes: Vec::new(),
        },
        SchemaTableSnapshot {
            name: "state_store_kv".to_owned(),
            engine: "InnoDB".to_owned(),
            row_format: "Dynamic".to_owned(),
            columns: vec![
                SchemaColumnSnapshot::new("key_bytes", "varbinary(3072)", false, 1),
                SchemaColumnSnapshot::new("value_bytes", "mediumblob", false, 0),
                SchemaColumnSnapshot::new("version_bytes", "binary(12)", false, 0),
            ],
            primary_key: vec!["key_bytes".to_owned()],
            secondary_indexes: Vec::new(),
        },
        SchemaTableSnapshot {
            name: "state_store_meta".to_owned(),
            engine: "InnoDB".to_owned(),
            row_format: "Dynamic".to_owned(),
            columns: vec![
                SchemaColumnSnapshot::new("meta_key", "varbinary(64)", false, 1),
                SchemaColumnSnapshot::new("meta_value", "varbinary(4096)", false, 0),
            ],
            primary_key: vec!["meta_key".to_owned()],
            secondary_indexes: Vec::new(),
        },
    ]
}

impl SchemaColumnSnapshot {
    pub fn new(name: &str, column_type: &str, nullable: bool, primary_key_position: usize) -> Self {
        Self {
            name: name.to_owned(),
            column_type: column_type.to_owned(),
            nullable,
            primary_key_position,
        }
    }
}

pub(super) fn schema_digest() -> String {
    hex::encode(Sha256::digest(SCHEMA_MANIFEST.as_bytes()))
}

impl SchemaSession {
    fn new(connection: MysqlPoolConnection, deadline: Instant) -> Self {
        Self {
            connection: Some(connection),
            deadline,
        }
    }

    fn has_connection(&self) -> bool {
        self.connection.is_some()
    }

    #[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
    fn into_connection(mut self) -> Option<MysqlPoolConnection> {
        self.connection.take()
    }

    async fn run<T>(
        &mut self,
        operation: impl for<'a> FnOnce(&'a mut Conn) -> BoxFuture<'a, Result<T, mysql_async::Error>>,
    ) -> Result<T, StateStoreError> {
        let connection = self.connection.take().ok_or_else(deadline_error)?;
        let (connection, result) =
            execute_owned_with_deadline(connection, self.deadline, operation).await?;
        self.connection = Some(connection);
        result.map_err(super::error::MysqlNativeError::into_public)
    }

    #[cfg(feature = "state-store-test-hooks")]
    async fn connection_id(&mut self) -> Result<u64, StateStoreError> {
        let row: Option<(u64,)> = self
            .run(|connection| Box::pin(connection.query_first("SELECT CONNECTION_ID()")))
            .await?;
        row.map(|(connection_id,)| connection_id)
            .ok_or_else(provider_error)
    }

    async fn acquire_advisory_lock(&mut self, database: &str) -> Result<String, StateStoreError> {
        let lock_name = advisory_lock_name(database);
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(deadline_error());
        }
        let lock_wait = remaining
            .saturating_sub(Duration::from_millis(20))
            .as_secs_f64()
            .max(0.001);
        let params = Params::Positional(vec![lock_name.clone().into(), lock_wait.into()]);
        let row: Option<(Option<u8>,)> = self
            .run(move |connection| Box::pin(connection.exec_first("SELECT GET_LOCK(?, ?)", params)))
            .await?;
        match row.and_then(|(result,)| result) {
            Some(1) => Ok(lock_name),
            Some(0) => Err(deadline_error()),
            _ => Err(provider_error()),
        }
    }

    async fn release_advisory_lock(
        &mut self,
        lock_name: &str,
        cleanup_deadline: Instant,
    ) -> Result<(), StateStoreError> {
        self.deadline = cleanup_deadline;
        let lock_name = lock_name.to_owned();
        let row: Option<(Option<u8>,)> = match self
            .run(move |connection| {
                Box::pin(connection.exec_first("SELECT RELEASE_LOCK(?)", (lock_name,)))
            })
            .await
        {
            Ok(row) => row,
            Err(error) => {
                self.destroy_connection().await;
                return Err(error);
            }
        };
        if row.and_then(|(result,)| result) != Some(1) {
            self.destroy_connection().await;
            return Err(provider_error());
        }
        Ok(())
    }

    async fn destroy_connection(&mut self) {
        if let Some(connection) = self.connection.take() {
            connection.destroy().await;
        }
    }

    async fn rollback_after_failure(&mut self) {
        self.deadline = Instant::now() + ADVISORY_LOCK_CLEANUP_TIMEOUT;
        if self
            .run(|connection| Box::pin(connection.query_drop("ROLLBACK")))
            .await
            .is_err()
        {
            self.destroy_connection().await;
        }
    }
}

#[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
pub(crate) async fn snapshot_for_test(
    pool: Arc<dyn PoolLifecycle>,
    deadline: Instant,
) -> Result<SchemaSnapshot, StateStoreError> {
    let connection = checkout_hygienic_connection(pool, deadline).await?;
    let mut session = SchemaSession::new(connection, deadline);
    let inventory = load_inventory(&mut session).await?;
    let rows = if inventory
        .tables
        .iter()
        .any(|table| table.name == "state_store_meta")
    {
        load_meta_rows(&mut session).await?
    } else {
        Vec::new()
    };
    Ok(test_snapshot_from_rows(inventory, rows))
}

#[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
fn test_snapshot_from_rows(
    inventory: SchemaInventory,
    rows: Vec<(Vec<u8>, Vec<u8>)>,
) -> SchemaSnapshot {
    let meta = rows.into_iter().collect::<BTreeMap<_, _>>();
    let get = |key: &[u8]| meta.get(key).map(Vec::as_slice).unwrap_or_default();
    let u32_value = |key| {
        get(key)
            .try_into()
            .map(u32::from_be_bytes)
            .unwrap_or_default()
    };
    let u64_value = |key| {
        get(key)
            .try_into()
            .map(u64::from_be_bytes)
            .unwrap_or_default()
    };
    let cursor = get(CHANGE_RETENTION_FLOOR_KEY);
    let change_retention_floor = if let Ok(cursor) = <[u8; 12]>::try_from(cursor) {
        (
            u64::from_be_bytes(cursor[..8].try_into().expect("eight-byte revision")),
            u32::from_be_bytes(cursor[8..].try_into().expect("four-byte sequence")),
        )
    } else {
        (0, 0)
    };
    SchemaSnapshot {
        tables: inventory.tables,
        views: inventory.views,
        triggers: inventory.triggers,
        meta_keys: meta
            .keys()
            .map(|key| String::from_utf8_lossy(key).into_owned())
            .collect(),
        schema_version: u32_value(SCHEMA_VERSION_KEY),
        schema_digest: String::from_utf8_lossy(get(SCHEMA_DIGEST_KEY)).into_owned(),
        store_id: <[u8; 16]>::try_from(get(STORE_ID_KEY))
            .map(Uuid::from_bytes)
            .unwrap_or_else(|_| Uuid::nil()),
        cluster_id: String::from_utf8_lossy(get(CLUSTER_ID_KEY)).into_owned(),
        initial_incarnation: u64_value(INITIAL_INCARNATION_KEY),
        current_revision: u64_value(CURRENT_REVISION_KEY),
        change_retention_floor,
    }
}

#[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
pub(crate) async fn apply_mutation_for_test(
    pool: Arc<dyn PoolLifecycle>,
    mutation: SchemaMutation,
    deadline: Instant,
) -> Result<(), StateStoreError> {
    let connection = checkout_hygienic_connection(pool, deadline).await?;
    let mut session = SchemaSession::new(connection, deadline);
    match mutation {
        SchemaMutation::CreatePartialMetaTable => {
            session
                .run(|connection| Box::pin(connection.query_drop(META_SCHEMA_SQL)))
                .await
        }
        SchemaMutation::CreateExtraTable => {
            session
                .run(|connection| {
                    Box::pin(connection.query_drop(
                        "CREATE TABLE fixture_readiness (
                        id INT NOT NULL,
                        PRIMARY KEY (id)
                    ) ENGINE=InnoDB ROW_FORMAT=DYNAMIC",
                    ))
                })
                .await
        }
        SchemaMutation::DriftEngine => {
            session
                .run(|connection| {
                    Box::pin(connection.query_drop("ALTER TABLE state_store_meta ENGINE=MyISAM"))
                })
                .await
        }
        SchemaMutation::DriftRowFormat => {
            session
                .run(|connection| {
                    Box::pin(
                        connection.query_drop("ALTER TABLE state_store_meta ROW_FORMAT=COMPACT"),
                    )
                })
                .await
        }
        SchemaMutation::DriftColumn => {
            session
                .run(|connection| {
                    Box::pin(connection.query_drop(
                        "ALTER TABLE state_store_meta
                     MODIFY meta_value VARBINARY(4095) NOT NULL",
                    ))
                })
                .await
        }
        SchemaMutation::DriftIndex => {
            session
                .run(|connection| {
                    Box::pin(connection.query_drop(
                        "ALTER TABLE state_store_meta
                     ADD INDEX state_store_meta_extra (meta_value(8))",
                    ))
                })
                .await
        }
        SchemaMutation::DeleteSchemaVersion => {
            session
                .run(|connection| {
                    Box::pin(connection.exec_drop(
                        "DELETE FROM state_store_meta WHERE meta_key = ?",
                        (SCHEMA_VERSION_KEY,),
                    ))
                })
                .await
        }
        SchemaMutation::MalformedSchemaVersion => {
            replace_schema_version(&mut session, vec![1]).await
        }
        SchemaMutation::OlderSchemaVersion => {
            replace_schema_version(&mut session, 0_u32.to_be_bytes().to_vec()).await
        }
        SchemaMutation::NewerSchemaVersion => {
            replace_schema_version(&mut session, 2_u32.to_be_bytes().to_vec()).await
        }
    }
}

#[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
async fn replace_schema_version(
    session: &mut SchemaSession,
    value: Vec<u8>,
) -> Result<(), StateStoreError> {
    session
        .run(move |connection| {
            Box::pin(connection.exec_drop(
                "UPDATE state_store_meta SET meta_value = ? WHERE meta_key = ?",
                (value, SCHEMA_VERSION_KEY),
            ))
        })
        .await
}

#[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
pub(crate) async fn acquire_lock_for_test(
    pool: Arc<dyn PoolLifecycle>,
    database: &str,
    deadline: Instant,
) -> Result<(MysqlPoolConnection, String), StateStoreError> {
    let connection = checkout_hygienic_connection(pool, deadline).await?;
    let mut session = SchemaSession::new(connection, deadline);
    let lock_name = session.acquire_advisory_lock(database).await?;
    let connection = session.into_connection().ok_or_else(deadline_error)?;
    Ok((connection, lock_name))
}

#[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
pub(crate) async fn release_lock_for_test(
    connection: MysqlPoolConnection,
    lock_name: &str,
    deadline: Instant,
) -> Result<(), StateStoreError> {
    let mut session = SchemaSession::new(connection, deadline);
    session.release_advisory_lock(lock_name, deadline).await
}

#[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
pub(crate) async fn is_lock_free_for_test(
    pool: Arc<dyn PoolLifecycle>,
    database: &str,
    deadline: Instant,
) -> Result<bool, StateStoreError> {
    let connection = checkout_hygienic_connection(pool, deadline).await?;
    let mut session = SchemaSession::new(connection, deadline);
    let lock_name = advisory_lock_name(database);
    let row: Option<(Option<u8>,)> = session
        .run(move |connection| {
            Box::pin(connection.exec_first("SELECT IS_FREE_LOCK(?)", (lock_name,)))
        })
        .await?;
    row.and_then(|(result,)| result)
        .map(|result| result == 1)
        .ok_or_else(provider_error)
}

#[cfg_attr(not(feature = "state-store-test-hooks"), allow(dead_code))]
pub(crate) async fn timeout_connection_is_destroyed_for_test(
    pool: Arc<dyn PoolLifecycle>,
    timeout_deadline: Instant,
    checkout_deadline: Instant,
) -> Result<bool, StateStoreError> {
    let connection = checkout_hygienic_connection(Arc::clone(&pool), timeout_deadline).await?;
    let mut session = SchemaSession::new(connection, timeout_deadline);
    let before: Option<(u64,)> = session
        .run(|connection| Box::pin(connection.query_first("SELECT CONNECTION_ID()")))
        .await?;
    let error = match session
        .run(|connection| Box::pin(connection.query_drop("SELECT SLEEP(10)")))
        .await
    {
        Ok(()) => return Ok(false),
        Err(error) => error,
    };
    if error.kind() != StateStoreErrorKind::DeadlineExceeded || session.has_connection() {
        return Ok(false);
    }

    let connection = checkout_hygienic_connection(pool, checkout_deadline).await?;
    let mut replacement = SchemaSession::new(connection, checkout_deadline);
    let after: Option<(u64,)> = replacement
        .run(|connection| Box::pin(connection.query_first("SELECT CONNECTION_ID()")))
        .await?;
    Ok(before.is_some() && after.is_some() && before != after)
}

fn schema_corruption() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::Corruption,
        "MySQL state store schema inventory is incomplete or unexpected",
    )
}

fn deadline_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::DeadlineExceeded,
        "MySQL state store schema operation exceeded its deadline",
    )
}

fn provider_error() -> StateStoreError {
    StateStoreError::new(
        StateStoreErrorKind::ProviderUnavailable,
        "MySQL state store schema operation failed",
    )
}
