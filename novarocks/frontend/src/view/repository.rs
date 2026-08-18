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
use std::fmt;
use std::sync::Arc;

use crate::view::ViewSqlDialect;
use bytes::Bytes;
use novarocks_catalog::identifier::normalize_identifier;
use novarocks_spi::state_store::{
    Direction, Key, KeyRange, Precondition, RangeRequest, StateRecord, StateStore,
    StateStoreLimits, Value, WriteTransaction,
};
use novarocks_state_store::metrics::StateStoreMetrics;
use novarocks_state_store::{OperationId, RunFailure, run_side_effect_free};
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use sqlparser::ast::Statement;
use sqlparser::parser::Parser;
use tokio::runtime::Handle;
use uuid::Uuid;

use crate::durable::{DurableRecord, DurableRecordStore};

const SCHEMA_VERSION: u8 = 1;
const VIEW_PREFIX: &[u8] = b"novarocks/frontend/views/v1/";
const VIEW_RECORD_ENCODED_LIMIT: usize = 60 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredDatabaseViewsV1 {
    pub schema_version: u8,
    pub catalog: String,
    pub database: String,
    pub last_operation_id: Uuid,
    #[serde(deserialize_with = "deserialize_views")]
    pub views: BTreeMap<String, String>,
}

impl DurableRecord for StoredDatabaseViewsV1 {
    const RECORD_KIND: &'static str = "frontend-view-database";
    const SCHEMA_VERSION: u8 = SCHEMA_VERSION;
    // View SQL and names are variable-length, so the full encoded candidate is
    // the authority. Reserve headroom below the StateStore 64 KiB ceiling for
    // a stable record budget rather than accepting values at the transport cap.
    const ENCODED_LIMIT: usize = VIEW_RECORD_ENCODED_LIMIT;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DatabaseMutation {
    Create {
        view: String,
        sql: String,
        or_replace: bool,
    },
    DropView {
        view: String,
    },
    DropDatabase,
}

#[derive(Clone)]
pub struct ViewRepository {
    store: Arc<dyn StateStore>,
    durable: DurableRecordStore,
    runtime: Handle,
    metrics: Arc<StateStoreMetrics>,
}

impl fmt::Debug for ViewRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ViewRepository")
            .field("provider", &self.metrics.provider())
            .finish_non_exhaustive()
    }
}

impl ViewRepository {
    pub async fn open(store: Arc<dyn StateStore>, runtime: Handle) -> Result<Self, String> {
        let provider_id = store.metrics_snapshot().provider;
        let repository = Self {
            metrics: Arc::new(StateStoreMetrics::new(provider_id)),
            durable: DurableRecordStore::new(Arc::clone(&store)),
            store,
            runtime,
        };
        repository.load_all().await?;
        Ok(repository)
    }

    pub fn runtime_handle(&self) -> &Handle {
        &self.runtime
    }

    pub async fn load_all(&self) -> Result<Vec<StoredDatabaseViewsV1>, String> {
        let prefix = Key::try_from(Bytes::from_static(VIEW_PREFIX))
            .map_err(|error| format!("build frontend view database range failed: {error}"))?;
        let range = KeyRange::for_prefix(prefix)
            .map_err(|error| format!("build frontend view database range failed: {error}"))?;
        let mut transaction = self
            .store
            .begin_read()
            .await
            .map_err(|error| format!("begin frontend view database load failed: {error}"))?;
        let mut request = RangeRequest {
            range,
            direction: Direction::Forward,
            page_size: self.store.limits().max_page_size,
            continuation: None,
        };
        let mut records = Vec::new();
        let mut identities = BTreeSet::new();

        loop {
            let page = transaction
                .range(&request)
                .await
                .map_err(|error| format!("load frontend view database page failed: {error}"))?;
            for record in page.records {
                let record = decode_record(record.key, record.value)?;
                let identity = (record.catalog.clone(), record.database.clone());
                if !identities.insert(identity) {
                    return Err(format!(
                        "duplicate frontend view database identity: {}.{}",
                        record.catalog, record.database
                    ));
                }
                records.push(record);
            }
            let Some(continuation) = page.continuation else {
                break;
            };
            request.continuation = Some(continuation);
        }

        transaction
            .abort()
            .await
            .map_err(|error| format!("finish frontend view database load failed: {error}"))?;
        Ok(records)
    }

    pub async fn mutate_database(
        &self,
        catalog: &str,
        database: &str,
        mutation: DatabaseMutation,
    ) -> Result<StoredDatabaseViewsV1, String> {
        let catalog = normalize_identity("catalog", catalog)?;
        let database = normalize_identity("database", database)?;
        let mutation = prepare_mutation(mutation)?;
        let key = database_key(&catalog, &database)?;
        let operation_id = OperationId::new_v7();

        let result = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            operation_id,
            "persist frontend view database",
            |transaction| {
                let key = key.clone();
                let catalog = catalog.clone();
                let database = database.clone();
                let mutation = mutation.clone();
                let durable = self.durable.clone();
                Box::pin(async move {
                    apply_mutation(
                        transaction,
                        &durable,
                        &key,
                        operation_id,
                        &catalog,
                        &database,
                        &mutation,
                    )
                    .await
                })
            },
        )
        .await;

        match result {
            Ok(success) => success.value,
            Err(RunFailure::CommitUnknown { .. }) => {
                let authoritative = self.load_database(&key).await?;
                match authoritative {
                    Some(record) if record.last_operation_id == *operation_id.as_uuid() => {
                        Ok(record)
                    }
                    _ => Err(format!(
                        "frontend view database mutation commit outcome is unresolved: {}.{}",
                        catalog, database
                    )),
                }
            }
            Err(failure) => Err(format_run_failure(&catalog, &database, failure)),
        }
    }

    async fn load_database(&self, key: &Key) -> Result<Option<StoredDatabaseViewsV1>, String> {
        let mut transaction = self.store.begin_read().await.map_err(|error| {
            format!("begin authoritative frontend view database read failed: {error}")
        })?;
        let record = transaction.get(key).await.map_err(|error| {
            format!("authoritative frontend view database read failed: {error}")
        })?;
        transaction.abort().await.map_err(|error| {
            format!("finish authoritative frontend view database read failed: {error}")
        })?;
        record
            .map(|record| decode_record(record.key, record.value))
            .transpose()
    }
}

pub fn database_key(catalog: &str, database: &str) -> Result<Key, String> {
    let encoded = format!(
        "{}{}/{}",
        std::str::from_utf8(VIEW_PREFIX).expect("view prefix is UTF-8"),
        hex::encode(catalog.as_bytes()),
        hex::encode(database.as_bytes())
    );
    Key::try_from(Bytes::from(encoded))
        .map_err(|error| format!("encode frontend view database key failed: {error}"))
}

pub fn encode_record(record: StoredDatabaseViewsV1) -> Result<Value, String> {
    validate_record(&record)?;
    DurableRecordStore::with_limits(StateStoreLimits::default())
        .encode_compat_value(&record)
        .map_err(|error| {
            format!(
                "encode frontend view database {}.{} failed: {error}",
                record.catalog, record.database
            )
        })
}

pub fn decode_record(key: Key, value: Value) -> Result<StoredDatabaseViewsV1, String> {
    let (key_catalog, key_database) = decode_database_key(&key)?;
    let record: StoredDatabaseViewsV1 =
        serde_json::from_slice(value.as_bytes()).map_err(|error| {
            format!(
                "decode frontend view database {}.{} failed: {error}",
                key_catalog, key_database
            )
        })?;
    validate_record(&record)?;
    if record.catalog != key_catalog || record.database != key_database {
        return Err(format!(
            "view record identity mismatch: key is {}.{}, value is {}.{}",
            key_catalog, key_database, record.catalog, record.database
        ));
    }
    Ok(record)
}

fn decode_database_key(key: &Key) -> Result<(String, String), String> {
    let suffix = key
        .as_bytes()
        .strip_prefix(VIEW_PREFIX)
        .ok_or_else(|| "frontend view database key has an unknown prefix".to_string())?;
    let mut parts = suffix.split(|byte| *byte == b'/');
    let catalog = parts
        .next()
        .ok_or_else(|| "frontend view database key is malformed".to_string())?;
    let database = parts
        .next()
        .ok_or_else(|| "frontend view database key is malformed".to_string())?;
    if catalog.is_empty() || database.is_empty() || parts.next().is_some() {
        return Err("frontend view database key is malformed".to_string());
    }
    let catalog = decode_key_part(catalog, "catalog")?;
    let database = decode_key_part(database, "database")?;
    let canonical = database_key(&catalog, &database)?;
    if canonical != *key {
        return Err("frontend view database key is not canonical".to_string());
    }
    Ok((catalog, database))
}

fn decode_key_part(encoded: &[u8], identity: &str) -> Result<String, String> {
    let raw = hex::decode(encoded)
        .map_err(|_| format!("frontend view database key has invalid {identity} encoding"))?;
    String::from_utf8(raw)
        .map_err(|_| format!("frontend view database key has non-UTF-8 {identity}"))
}

fn deserialize_views<'de, D>(deserializer: D) -> Result<BTreeMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct ViewsVisitor;

    impl<'de> Visitor<'de> for ViewsVisitor {
        type Value = BTreeMap<String, String>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a map of normalized frontend view names to query SQL")
        }

        fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut views = BTreeMap::new();
            let mut normalized_names = BTreeSet::new();
            while let Some((view, sql)) = access.next_entry::<String, String>()? {
                let normalized = normalize_identifier(&view).unwrap_or_else(|_| view.clone());
                if !normalized_names.insert(normalized.clone()) {
                    return Err(serde::de::Error::custom(format!(
                        "duplicate normalized frontend view name `{normalized}`"
                    )));
                }
                views.insert(view, sql);
            }
            Ok(views)
        }
    }

    deserializer.deserialize_map(ViewsVisitor)
}

fn validate_record(record: &StoredDatabaseViewsV1) -> Result<(), String> {
    if record.schema_version != SCHEMA_VERSION {
        return Err(format!(
            "unsupported frontend view database schema version: {}",
            record.schema_version
        ));
    }
    validate_normalized_identity("catalog", &record.catalog)?;
    validate_normalized_identity("database", &record.database)?;
    for (view, sql) in &record.views {
        validate_normalized_identity("view", view)?;
        parse_query(sql).map_err(|error| {
            format!(
                "invalid frontend view definition {}.{}.{}: {error}",
                record.catalog, record.database, view
            )
        })?;
    }
    Ok(())
}

fn validate_normalized_identity(kind: &str, value: &str) -> Result<(), String> {
    let normalized = normalize_identity(kind, value)?;
    if normalized != value {
        return Err(format!("frontend view {kind} is not normalized: `{value}`"));
    }
    Ok(())
}

fn normalize_identity(kind: &str, value: &str) -> Result<String, String> {
    normalize_identifier(value)
        .map_err(|error| format!("invalid frontend view {kind} `{value}`: {error}"))
}

fn parse_query(sql: &str) -> Result<(), String> {
    canonical_query(sql).map(|_| ())
}

fn canonical_query(sql: &str) -> Result<String, String> {
    let statements = Parser::parse_sql(&ViewSqlDialect, sql)
        .map_err(|error| format!("query parse failed: {error}"))?;
    match statements.as_slice() {
        [Statement::Query(query)] => Ok(query.to_string()),
        _ => Err("view SQL must contain exactly one query statement".to_string()),
    }
}

fn prepare_mutation(mutation: DatabaseMutation) -> Result<DatabaseMutation, String> {
    match mutation {
        DatabaseMutation::Create {
            view,
            sql,
            or_replace,
        } => Ok(DatabaseMutation::Create {
            view: normalize_identity("view", &view)?,
            sql: canonical_query(&sql)?,
            or_replace,
        }),
        DatabaseMutation::DropView { view } => Ok(DatabaseMutation::DropView {
            view: normalize_identity("view", &view)?,
        }),
        DatabaseMutation::DropDatabase => Ok(DatabaseMutation::DropDatabase),
    }
}

async fn apply_mutation(
    transaction: &mut dyn WriteTransaction,
    durable: &DurableRecordStore,
    key: &Key,
    operation_id: OperationId,
    catalog: &str,
    database: &str,
    mutation: &DatabaseMutation,
) -> Result<Result<StoredDatabaseViewsV1, String>, novarocks_spi::state_store::StateStoreError> {
    let existing = transaction.get(key).await?;
    let (mut record, precondition) = match existing {
        Some(StateRecord {
            key,
            value,
            version,
        }) => {
            let record = match decode_record(key, value) {
                Ok(record) => record,
                Err(error) => return Ok(Err(error)),
            };
            (record, Precondition::Version(version))
        }
        None => (
            StoredDatabaseViewsV1 {
                schema_version: SCHEMA_VERSION,
                catalog: catalog.to_string(),
                database: database.to_string(),
                last_operation_id: *operation_id.as_uuid(),
                views: BTreeMap::new(),
            },
            Precondition::Absent,
        ),
    };

    match mutation {
        DatabaseMutation::Create {
            view,
            sql,
            or_replace,
        } => {
            if record.views.contains_key(view) && !or_replace {
                return Ok(Err(format!("view already exists: {database}.{view}")));
            }
            record.views.insert(view.clone(), sql.clone());
        }
        DatabaseMutation::DropView { view } => {
            record.views.remove(view);
        }
        DatabaseMutation::DropDatabase => record.views.clear(),
    }
    record.last_operation_id = *operation_id.as_uuid();
    if let Err(error) = validate_record(&record) {
        return Ok(Err(error));
    }
    let encoded = match durable.encode(&record) {
        Ok(encoded) => encoded,
        Err(error) => {
            return Ok(Err(format!(
                "encode frontend view database {}.{} failed: {error}",
                record.catalog, record.database
            )));
        }
    };
    durable
        .put_record(transaction, key.clone(), encoded, precondition)
        .await?;
    Ok(Ok(record))
}

fn format_run_failure(catalog: &str, database: &str, failure: RunFailure) -> String {
    let detail = match failure {
        RunFailure::Begin(error) => format!("begin failed: {error}"),
        RunFailure::Operation(error) => format!("operation failed: {error}"),
        RunFailure::RetryExhausted(error) => format!("retry exhausted: {error}"),
        RunFailure::DefiniteFailure(error) => format!("commit failed: {error}"),
        RunFailure::CommitUnknown { error, .. } => format!("commit unknown: {error}"),
        RunFailure::DeadlineExceeded => "deadline exceeded".to_string(),
    };
    format!(
        "persist frontend view database {}.{} failed: {}",
        catalog, database, detail
    )
}
