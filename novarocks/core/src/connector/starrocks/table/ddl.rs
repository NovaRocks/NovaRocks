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

use std::collections::HashSet;
use std::sync::Arc;

use crate::connector::starrocks::ObjectStoreProfile;
use crate::connector::starrocks::lake::context::{get_tablet_runtime, remove_tablet_runtime};
use crate::connector::starrocks::lake::create_lake_tablet_from_req;
use crate::connector::starrocks::lake::schema::create_lake_tablet_from_req_with_schema_patch;
use crate::connector::starrocks::lake::storage_schema_wire::encode_tablet_schema_bytes;
use crate::connector::starrocks::lake::transactions::delete_tablet;
use crate::connector::starrocks::schema::StarRocksTabletSchema;
use crate::formats::starrocks::metadata::load_tablet_snapshot;
use crate::service::grpc_client::proto::starrocks::DeleteTabletRequest;
use crate::sql::parser::ast::{
    ColumnAggregation, ObjectName, TableColumnDef, TableKeyDesc, TableKeyKind,
};
use novarocks_catalog::schema::SqlType;

use super::catalog::{
    StarRocksTableCatalog, StarRocksTableRuntime, register_starrocks_table_in_catalog,
};
use super::model::{StarRocksPartitionState, StoredStarRocksColumn};
use crate::connector::starrocks::table::config::StarRocksTableConfig;
use crate::connector::starrocks::table::schema_adapter::{
    build_create_tablet_request, build_tablet_schema, request_schema_from_runtime,
};
use crate::engine::{StandaloneState, StatementResult};
use crate::meta::repository::starrocks_table::{
    CreateStarRocksColumnRequest, CreateStarRocksTableLayoutRequest, StageStarRocksTruncateRequest,
    StagedStarRocksTruncate, StarRocksTableKind as RepoStarRocksTableKind,
};
use crate::mv::aggregate_state::physical_column::StarRocksPhysicalColumn;
use novarocks_catalog::identifier::normalize_identifier;

/// Default bucket count when the user omits `DISTRIBUTED BY ... BUCKETS <n>`.
const DEFAULT_STARROCKS_BUCKET_COUNT: u32 = 1;
/// Mirrors StarRocks `SHORTKEY_MAX_COLUMN_COUNT`: at most 3 columns in the short-key.
const SHORT_KEY_MAX_COLUMN_COUNT: usize = 3;
/// Mirrors StarRocks `SHORTKEY_MAXSIZE_BYTES`: at most 36 bytes in the short-key.
const SHORT_KEY_MAX_SIZE_BYTES: usize = 36;

pub(crate) fn table_columns_from_physical_columns(
    columns: &[StarRocksPhysicalColumn],
) -> Vec<TableColumnDef> {
    columns.iter().map(|column| column.column.clone()).collect()
}

pub(crate) fn stored_columns_from_physical_columns(
    schema_id: i64,
    key_desc: &TableKeyDesc,
    columns: &[StarRocksPhysicalColumn],
) -> Vec<StoredStarRocksColumn> {
    let key_column_set = key_desc
        .columns
        .iter()
        .map(|column| normalize_identifier(column).unwrap_or_else(|_| column.to_ascii_lowercase()))
        .collect::<HashSet<_>>();
    columns
        .iter()
        .enumerate()
        .map(|(ordinal, physical)| {
            let column_name = normalize_identifier(&physical.column.name)
                .unwrap_or_else(|_| physical.column.name.to_ascii_lowercase());
            StoredStarRocksColumn {
                schema_id,
                ordinal: ordinal as i64,
                is_key: physical.is_key || key_column_set.contains(&column_name),
                column_name,
                logical_type: logical_type_name(&physical.column.data_type),
                nullable: physical.column.nullable,
                visible: physical.visible,
            }
        })
        .collect()
}

pub(crate) fn patch_tablet_schema_column_flags(
    schema: &mut StarRocksTabletSchema,
    columns: &[StarRocksPhysicalColumn],
) -> Result<(), String> {
    if schema.column.len() != columns.len() {
        return Err(format!(
            "StarRocks tablet schema column count mismatch: schema_columns={} physical_columns={}",
            schema.column.len(),
            columns.len()
        ));
    }
    for (schema_column, physical_column) in schema.column.iter_mut().zip(columns.iter()) {
        schema_column.visible = Some(physical_column.visible);
        schema_column.is_key = Some(physical_column.is_key);
    }
    Ok(())
}

pub(crate) fn create_starrocks_table(
    state: &StandaloneState,
    name: &ObjectName,
    current_database: &str,
    columns: &[TableColumnDef],
    key_desc: Option<&TableKeyDesc>,
    bucket_count: Option<u32>,
) -> Result<StatementResult, String> {
    let resolved = resolve_local_starrocks_table_name(name, current_database)?;
    let catalog = state
        .catalog_service
        .local()
        .read()
        .expect("standalone catalog read lock");
    if !catalog.database_exists(&resolved.database)? {
        return Err(format!("unknown database: {}", resolved.database));
    }
    if catalog.get(&resolved.database, &resolved.table).is_ok() {
        return Err(format!(
            "table already exists: {}.{}",
            resolved.database, resolved.table
        ));
    }
    drop(catalog);

    let starrocks_table_config = state
        .starrocks_table_config
        .clone()
        .ok_or_else(|| "standalone StarRocks table config is missing".to_string())?;
    let defaults = resolve_starrocks_create_defaults(columns, key_desc, bucket_count)?;
    let provider = state.metadata_provider.as_ref().ok_or_else(|| {
        "StarRocks standalone CREATE TABLE requires metadata provider".to_string()
    })?;

    let mut guard = state
        .starrocks_table
        .write()
        .expect("standalone StarRocks table write lock");
    if guard.contains_table(&resolved.database, &resolved.table)? {
        return Err(format!(
            "table already exists: {}.{}",
            resolved.database, resolved.table
        ));
    }

    let key_column_set = defaults
        .key_desc
        .columns
        .iter()
        .map(|column| normalize_identifier(column))
        .collect::<Result<HashSet<_>, _>>()?;
    let physical_columns = columns
        .iter()
        .map(|column| {
            let column_name = normalize_identifier(&column.name)?;
            Ok(StarRocksPhysicalColumn {
                column: column.clone(),
                visible: true,
                is_key: key_column_set.contains(&column_name),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let request_columns = table_columns_from_physical_columns(&physical_columns);
    let stored_columns =
        stored_columns_from_physical_columns(0, &defaults.key_desc, &physical_columns)
            .into_iter()
            .map(|column| CreateStarRocksColumnRequest {
                column_name: column.column_name,
                logical_type: column.logical_type,
                nullable: column.nullable,
                visible: column.visible,
                is_key: column.is_key,
            })
            .collect::<Vec<_>>();

    let mut txn = provider
        .begin_write("create StarRocks table")
        .map_err(|e| format!("open StarRocks table create transaction failed: {e}"))?;
    let database = state
        .starrocks_table_repo
        .get_or_create_database(txn.as_mut(), &resolved.database)
        .map_err(|e| format!("create StarRocks database metadata failed: {e}"))?;
    let reclaimed = state
        .starrocks_table_repo
        .purge_dropping_table_for_reuse(txn.as_mut(), database.db_id, &resolved.table)
        .map_err(|e| format!("reclaim dropping StarRocks table metadata failed: {e}"))?;
    for table_id in &reclaimed {
        state
            .starrocks_txn_repo
            .delete_for_table(txn.as_mut(), *table_id)
            .map_err(|e| format!("delete reclaimed StarRocks txns failed: {e}"))?;
        state
            .job_repo
            .delete_for_table(txn.as_mut(), *table_id)
            .map_err(|e| format!("delete reclaimed erase jobs failed: {e}"))?;
    }

    let created = state
        .starrocks_table_repo
        .create_table_layout(
            txn.as_mut(),
            CreateStarRocksTableLayoutRequest {
                db_id: database.db_id,
                table_name: resolved.table.clone(),
                keys_type: keys_type_name(defaults.key_desc.kind).to_string(),
                bucket_num: defaults.bucket_num,
                kind: RepoStarRocksTableKind::Table,
                schema_version: 0,
                tablet_schema_pb: Vec::new(),
                columns: stored_columns,
                partition_name: "p0".to_string(),
                warehouse_uri: starrocks_table_config.warehouse_uri.clone(),
            },
        )
        .map_err(|e| format!("create StarRocks table metadata failed: {e}"))?;
    let request_schema = build_tablet_schema(
        &request_columns,
        &defaults.key_desc,
        created.schema.schema_id,
    )?;
    let mut tablet_schema_pb =
        crate::connector::starrocks::lake::schema_adapter::build_tablet_schema_pb_from_thrift(
            &request_schema,
        )?;
    patch_tablet_schema_column_flags(&mut tablet_schema_pb, &physical_columns)?;
    state
        .starrocks_table_repo
        .update_schema_payload(
            txn.as_mut(),
            created.schema.schema_id,
            encode_tablet_schema_bytes(&tablet_schema_pb),
        )
        .map_err(|e| format!("update StarRocks table schema metadata failed: {e}"))?;
    state
        .starrocks_txn_repo
        .record_visible_bootstrap(
            txn.as_mut(),
            created.table.table_id,
            created.partition.partition_id,
        )
        .map_err(|e| format!("create StarRocks table bootstrap txn metadata failed: {e}"))?;

    let object_store_profile =
        ObjectStoreProfile::from_s3_store_config(&starrocks_table_config.s3)?;
    let mut bootstrapped_tablet_ids = Vec::new();
    for tablet in &created.tablets {
        let request = crate::thrift::agent_service::TCreateTabletReq {
            tablet_id: tablet.tablet_id,
            tablet_schema: request_schema.clone(),
            version: None,
            version_hash: None,
            storage_medium: None,
            in_restore_mode: None,
            base_tablet_id: None,
            base_schema_hash: None,
            table_id: Some(created.table.table_id),
            partition_id: Some(created.partition.partition_id),
            allocation_term: None,
            is_eco_mode: None,
            storage_format: None,
            tablet_type: None,
            enable_persistent_index: Some(false),
            compression_type: Some(crate::thrift::types::TCompressionType::LZ4_FRAME),
            binlog_config: None,
            persistent_index_type: None,
            primary_index_cache_expire_sec: None,
            create_schema_file: Some(false),
            compression_level: None,
            enable_tablet_creation_optimization: Some(false),
            timeout_ms: None,
            gtid: Some(0),
            flat_json_config: None,
            compaction_strategy: None,
        };
        if let Err(err) = create_lake_tablet_from_req_with_schema_patch(
            &request,
            &tablet.tablet_root_path,
            Some(starrocks_table_config.s3.clone()),
            |schema| patch_tablet_schema_column_flags(schema, &physical_columns),
        ) {
            cleanup_bootstrapped_tablets(&bootstrapped_tablet_ids);
            let _ = txn.abort();
            return Err(err);
        }
        bootstrapped_tablet_ids.push(tablet.tablet_id);
        let runtime_schema = match get_tablet_runtime(tablet.tablet_id) {
            Ok(runtime) => runtime.schema,
            Err(err) => {
                cleanup_bootstrapped_tablets(&bootstrapped_tablet_ids);
                let _ = txn.abort();
                return Err(err);
            }
        };
        let loaded = match load_tablet_snapshot(
            tablet.tablet_id,
            1,
            &tablet.tablet_root_path,
            Some(&object_store_profile),
        ) {
            Ok(loaded) => loaded,
            Err(err) => {
                cleanup_bootstrapped_tablets(&bootstrapped_tablet_ids);
                let _ = txn.abort();
                return Err(err);
            }
        };
        if loaded.tablet_schema != runtime_schema {
            cleanup_bootstrapped_tablets(&bootstrapped_tablet_ids);
            let _ = txn.abort();
            return Err(format!(
                "StarRocks tablet schema mismatch after bootstrap: tablet_id={}",
                tablet.tablet_id
            ));
        }
    }
    if let Err(err) = txn.commit() {
        cleanup_bootstrapped_tablets(&bootstrapped_tablet_ids);
        return Err(format!("commit StarRocks table metadata failed: {err}"));
    }

    let read = provider
        .begin_read()
        .map_err(|e| format!("open StarRocks table reload transaction failed: {e}"))?;
    let snapshot = state
        .starrocks_table_repo
        .load_snapshot(read.as_ref())
        .map_err(|e| format!("reload StarRocks table metadata failed: {e}"))?;
    let rebuilt = StarRocksTableCatalog::rebuild_from_repository(
        Some(starrocks_table_config),
        snapshot.clone(),
    )?;
    rebuilt.re_register_active_tablet_runtimes()?;
    *guard = rebuilt;
    let runtime = guard.table(&resolved.database, &resolved.table)?.clone();
    drop(guard);

    let mut catalog = state
        .catalog_service
        .local()
        .write()
        .expect("standalone catalog write lock");
    register_starrocks_table_in_catalog(&mut catalog, &runtime)?;
    Ok(StatementResult::Ok)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct StarRocksCreateDefaults {
    key_desc: TableKeyDesc,
    bucket_num: i64,
}

/// Resolve StarRocks-style defaults for `CREATE TABLE` on StarRocks table:
/// - KEY description defaults to DUP KEY on leading non-float columns (short-key rules).
/// - BUCKETS defaults to 1.
fn resolve_starrocks_create_defaults(
    columns: &[TableColumnDef],
    key_desc: Option<&TableKeyDesc>,
    bucket_count: Option<u32>,
) -> Result<StarRocksCreateDefaults, String> {
    let key_desc = match key_desc {
        Some(key_desc) => key_desc.clone(),
        None => TableKeyDesc {
            kind: TableKeyKind::Duplicate,
            columns: choose_default_dup_key_columns(columns)?,
        },
    };
    // BITMAP / HLL columns cannot appear in any user-declared key
    // (DUPLICATE / AGGREGATE / UNIQUE / PRIMARY). `key_eligible_type` already
    // returns false for them; surface a BITMAP/HLL-aware error message.
    for key_col_name in &key_desc.columns {
        let normalized = normalize_identifier(key_col_name)?;
        let Some(column) = columns
            .iter()
            .find(|c| normalize_identifier(&c.name).ok().as_deref() == Some(normalized.as_str()))
        else {
            // Missing column will be caught downstream in `build_tablet_schema`.
            continue;
        };
        if matches!(column.data_type, SqlType::Bitmap | SqlType::Hll) {
            let key_kind = match key_desc.kind {
                TableKeyKind::Primary => "PRIMARY KEY",
                TableKeyKind::Unique => "UNIQUE KEY",
                TableKeyKind::Aggregate => "AGGREGATE KEY",
                TableKeyKind::Duplicate => "DUPLICATE KEY",
            };
            return Err(format!(
                "BITMAP/HLL columns cannot be part of {key_kind} (column `{}` has type {:?})",
                column.name, column.data_type
            ));
        }
    }
    let bucket_num = i64::from(bucket_count.unwrap_or(DEFAULT_STARROCKS_BUCKET_COUNT));
    if bucket_num <= 0 {
        return Err("StarRocks standalone CREATE TABLE requires BUCKETS > 0".to_string());
    }
    Ok(StarRocksCreateDefaults {
        key_desc,
        bucket_num,
    })
}

/// Mirrors StarRocks `CreateTableAnalyzer.chooseKeysType` short-key selection:
/// take leading columns, skip FLOAT/DOUBLE/complex types, stop at first string
/// column (include it and stop), stop once column/byte limits reached. If no
/// keyable column is found, return an error matching StarRocks' wording.
fn choose_default_dup_key_columns(columns: &[TableColumnDef]) -> Result<Vec<String>, String> {
    if columns.is_empty() {
        return Err("StarRocks standalone CREATE TABLE requires at least one column".to_string());
    }

    let mut key_columns = Vec::new();
    let mut key_size = 0usize;
    for column in columns {
        key_size += short_key_index_size(&column.data_type);
        if key_columns.len() >= SHORT_KEY_MAX_COLUMN_COUNT || key_size > SHORT_KEY_MAX_SIZE_BYTES {
            if key_columns.is_empty() && is_string_family(&column.data_type) {
                key_columns.push(column.name.clone());
            }
            break;
        }
        if !key_eligible_type(&column.data_type) {
            break;
        }
        key_columns.push(column.name.clone());
        if is_string_family(&column.data_type) {
            break;
        }
    }

    if key_columns.is_empty() {
        return Err(format!(
            "StarRocks standalone CREATE TABLE data type of first column `{}` cannot be a key column",
            columns[0].name
        ));
    }
    Ok(key_columns)
}

fn key_eligible_type(data_type: &SqlType) -> bool {
    !matches!(
        data_type,
        SqlType::Float
            | SqlType::Double
            | SqlType::Json
            | SqlType::Binary
            | SqlType::Bitmap
            | SqlType::Hll
            | SqlType::Array(_)
            | SqlType::Map(_, _)
            | SqlType::Struct(_)
            | SqlType::Variant
    )
}

fn short_key_index_size(data_type: &SqlType) -> usize {
    match data_type {
        SqlType::Boolean | SqlType::TinyInt => 1,
        SqlType::SmallInt => 2,
        SqlType::Int | SqlType::Date => 4,
        SqlType::BigInt | SqlType::DateTime | SqlType::DateTimeNs | SqlType::Time => 8,
        SqlType::LargeInt | SqlType::Decimal { .. } => 16,
        SqlType::String | SqlType::Binary => 20,
        SqlType::Json => 16,
        SqlType::Bitmap | SqlType::Hll => SHORT_KEY_MAX_SIZE_BYTES + 1,
        SqlType::Float => 4,
        SqlType::Double => 8,
        SqlType::Array(_) | SqlType::Map(_, _) | SqlType::Struct(_) | SqlType::Variant => {
            SHORT_KEY_MAX_SIZE_BYTES + 1
        }
    }
}

fn is_string_family(data_type: &SqlType) -> bool {
    matches!(data_type, SqlType::String)
}

pub(crate) fn drop_starrocks_table(
    state: &Arc<StandaloneState>,
    database_name: &str,
    table_name: &str,
) -> Result<StatementResult, String> {
    drop_starrocks_table_with_metadata(state, database_name, table_name, |_, _| Ok(()))
}

pub(crate) fn drop_starrocks_table_with_metadata<F>(
    state: &Arc<StandaloneState>,
    database_name: &str,
    table_name: &str,
    update_metadata: F,
) -> Result<StatementResult, String>
where
    F: FnOnce(&mut dyn crate::meta::MetaWriteTxn, i64) -> Result<(), String>,
{
    let mut starrocks = state
        .starrocks_table
        .write()
        .expect("standalone StarRocks table write lock");
    let runtime = starrocks.table(database_name, table_name)?.clone();
    let starrocks_table_config = state
        .starrocks_table_config
        .as_ref()
        .ok_or_else(|| "standalone StarRocks table config is missing".to_string())?;
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "StarRocks standalone DROP TABLE requires metadata provider".to_string())?;
    let table_root_path = starrocks_table_root_path(
        &starrocks_table_config.warehouse_uri,
        runtime.table.db_id,
        runtime.table.table_id,
    );
    let mut txn = provider
        .begin_write("drop StarRocks table")
        .map_err(|e| format!("open StarRocks table drop transaction failed: {e}"))?;
    state
        .starrocks_txn_repo
        .ensure_no_inflight_for_table(txn.as_ref(), runtime.table.table_id)
        .map_err(|e| format!("validate StarRocks table drop failed: {e}"))?;
    update_metadata(txn.as_mut(), runtime.table.table_id)?;
    state
        .starrocks_table_repo
        .mark_table_dropping(txn.as_mut(), runtime.table.table_id)
        .map_err(|e| format!("mark StarRocks table dropping failed: {e}"))?;
    state
        .job_repo
        .create_erase_job(
            txn.as_mut(),
            crate::meta::repository::job::CreateEraseJobRequest {
                table_id: runtime.table.table_id,
                partition_id: None,
                root_path: table_root_path,
                now_ms: current_time_ms(),
            },
        )
        .map_err(|e| format!("enqueue StarRocks table erase job failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit StarRocks table drop metadata failed: {e}"))?;
    for tablet in &runtime.tablets {
        remove_tablet_runtime(tablet.tablet_id)?;
    }

    let read = provider
        .begin_read()
        .map_err(|e| format!("open StarRocks table drop reload transaction failed: {e}"))?;
    let snapshot = state
        .starrocks_table_repo
        .load_snapshot(read.as_ref())
        .map_err(|e| format!("reload StarRocks table metadata failed: {e}"))?;
    let rebuilt = StarRocksTableCatalog::rebuild_from_repository(
        state.starrocks_table_config.clone(),
        snapshot,
    )?;
    *starrocks = rebuilt;
    let mut catalog = state
        .catalog_service
        .local()
        .write()
        .expect("standalone catalog write lock");
    let _ = catalog.drop_table(database_name, table_name);
    Ok(StatementResult::Ok)
}

/// Remove the persisted `databases` entry for `database_name` after all of
/// its tables have been cascaded through `drop_starrocks_table`. This frees
/// the `db_id` so the next `CREATE DATABASE` allocates a fresh id, letting
/// `CREATE TABLE` on the same name succeed without colliding with the old
/// `(db_id, name)` UNIQUE rows left behind by tables still in the
/// `DROPPING` state (the erase worker cleans those asynchronously).
pub(crate) fn drop_starrocks_database_entry(
    state: &Arc<StandaloneState>,
    database_name: &str,
) -> Result<(), String> {
    let starrocks_table_config = state
        .starrocks_table_config
        .clone()
        .ok_or_else(|| "standalone StarRocks table config is missing".to_string())?;
    let provider = state.metadata_provider.as_ref().ok_or_else(|| {
        "StarRocks standalone DROP DATABASE requires metadata provider".to_string()
    })?;

    let mut guard = state
        .starrocks_table
        .write()
        .expect("standalone StarRocks table write lock");
    let mut txn = provider
        .begin_write("drop StarRocks table database entry")
        .map_err(|e| format!("open StarRocks database drop transaction failed: {e}"))?;
    let dropped = state
        .starrocks_table_repo
        .drop_database_entry(txn.as_mut(), database_name)
        .map_err(|e| format!("drop StarRocks database metadata failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit StarRocks database drop metadata failed: {e}"))?;
    if !dropped {
        return Ok(());
    }
    let read = provider
        .begin_read()
        .map_err(|e| format!("open StarRocks database reload transaction failed: {e}"))?;
    let snapshot = state
        .starrocks_table_repo
        .load_snapshot(read.as_ref())
        .map_err(|e| format!("reload StarRocks database metadata failed: {e}"))?;
    let rebuilt =
        StarRocksTableCatalog::rebuild_from_repository(Some(starrocks_table_config), snapshot)?;
    *guard = rebuilt;
    Ok(())
}

pub(crate) fn truncate_starrocks_table(
    state: &Arc<StandaloneState>,
    database_name: &str,
    table_name: &str,
) -> Result<StatementResult, String> {
    truncate_starrocks_table_with_hooks(
        state,
        database_name,
        table_name,
        bootstrap_truncated_partition,
        |rebuilt| rebuilt.re_register_active_tablet_runtimes(),
    )
}

#[derive(Clone, Debug)]
struct ResolvedStarRocksTableName {
    database: String,
    table: String,
}

fn resolve_local_starrocks_table_name(
    name: &ObjectName,
    current_database: &str,
) -> Result<ResolvedStarRocksTableName, String> {
    match name.parts.as_slice() {
        [table] => Ok(ResolvedStarRocksTableName {
            database: normalize_identifier(current_database)?,
            table: normalize_identifier(table)?,
        }),
        [database, table] => Ok(ResolvedStarRocksTableName {
            database: normalize_identifier(database)?,
            table: normalize_identifier(table)?,
        }),
        _ => Err(format!(
            "StarRocks table name must be `<table>` or `<database>.<table>`, got `{}`",
            name.parts.join(".")
        )),
    }
}

fn truncate_starrocks_table_with_hooks<Bootstrap, Refresh>(
    state: &Arc<StandaloneState>,
    database_name: &str,
    table_name: &str,
    bootstrap: Bootstrap,
    refresh_runtimes: Refresh,
) -> Result<StatementResult, String>
where
    Bootstrap: FnOnce(
        &StarRocksTableRuntime,
        &StarRocksTableConfig,
        &StagedStarRocksTruncate,
    ) -> Result<(), String>,
    Refresh: FnOnce(&StarRocksTableCatalog) -> Result<(), String>,
{
    let mut starrocks = state
        .starrocks_table
        .write()
        .expect("standalone StarRocks table write lock");
    let runtime = starrocks.table(database_name, table_name)?.clone();
    let starrocks_table_config = state
        .starrocks_table_config
        .as_ref()
        .ok_or_else(|| "standalone StarRocks table config is missing".to_string())?;
    let provider = state.metadata_provider.as_ref().ok_or_else(|| {
        "StarRocks standalone TRUNCATE TABLE requires metadata provider".to_string()
    })?;
    let active_partition = runtime
        .partitions
        .iter()
        .find(|partition| partition.state == StarRocksPartitionState::Active)
        .cloned()
        .ok_or_else(|| {
            format!(
                "StarRocks table {}.{} does not have an active partition",
                database_name, table_name
            )
        })?;
    let staged = {
        let mut txn = provider
            .begin_write("stage StarRocks table truncate partition")
            .map_err(|e| format!("open StarRocks truncate stage transaction failed: {e}"))?;
        state
            .starrocks_txn_repo
            .ensure_no_inflight_for_table(txn.as_ref(), runtime.table.table_id)
            .map_err(|e| format!("validate StarRocks truncate failed: {e}"))?;
        let staged = state
            .starrocks_table_repo
            .stage_truncate_partition(
                txn.as_mut(),
                StageStarRocksTruncateRequest {
                    table_id: runtime.table.table_id,
                    db_id: runtime.table.db_id,
                    bucket_num: runtime.table.bucket_num,
                    partition_name: active_partition.name.clone(),
                    warehouse_uri: starrocks_table_config.warehouse_uri.clone(),
                },
            )
            .map_err(|e| format!("stage StarRocks truncate metadata failed: {e}"))?;
        txn.commit()
            .map_err(|e| format!("commit StarRocks truncate stage metadata failed: {e}"))?;
        staged
    };
    if let Err(err) = bootstrap(&runtime, starrocks_table_config, &staged) {
        cleanup_staged_truncate(state, &staged)?;
        return Err(format!(
            "bootstrap truncate partition failed for {}.{}: {err}",
            database_name, table_name
        ));
    }
    let retired_root_path = starrocks_table_config.tablet_root_path(
        runtime.table.db_id,
        runtime.table.table_id,
        active_partition.partition_id,
    );
    if let Err(err) = (|| {
        let mut txn = provider
            .begin_write("activate StarRocks table truncate partition")
            .map_err(|e| format!("open StarRocks truncate activate transaction failed: {e}"))?;
        state
            .starrocks_table_repo
            .activate_truncate_partition(
                txn.as_mut(),
                runtime.table.table_id,
                active_partition.partition_id,
                staged.partition_id,
                staged.index_id,
            )
            .map_err(|e| format!("activate StarRocks truncate metadata failed: {e}"))?;
        state
            .job_repo
            .create_erase_job(
                txn.as_mut(),
                crate::meta::repository::job::CreateEraseJobRequest {
                    table_id: runtime.table.table_id,
                    partition_id: Some(active_partition.partition_id),
                    root_path: retired_root_path.clone(),
                    now_ms: current_time_ms(),
                },
            )
            .map_err(|e| format!("enqueue StarRocks truncate erase job failed: {e}"))?;
        txn.commit()
            .map_err(|e| format!("commit StarRocks truncate activate metadata failed: {e}"))?;
        Ok::<(), String>(())
    })() {
        cleanup_staged_truncate(state, &staged)?;
        return Err(format!(
            "activate truncate partition failed for {}.{}: {err}",
            database_name, table_name
        ));
    }
    for tablet in &runtime.tablets {
        remove_tablet_runtime(tablet.tablet_id)?;
    }

    let read = provider
        .begin_read()
        .map_err(|e| format!("open StarRocks truncate reload transaction failed: {e}"))?;
    let rebuilt_snapshot = state
        .starrocks_table_repo
        .load_snapshot(read.as_ref())
        .map_err(|e| format!("reload StarRocks truncate metadata failed: {e}"))?;
    let rebuilt = StarRocksTableCatalog::rebuild_from_repository(
        state.starrocks_table_config.clone(),
        rebuilt_snapshot,
    )?;
    refresh_runtimes(&rebuilt)?;
    let updated_runtime = rebuilt.table(database_name, table_name)?.clone();
    *starrocks = rebuilt;
    let mut catalog = state
        .catalog_service
        .local()
        .write()
        .expect("standalone catalog write lock");
    register_starrocks_table_in_catalog(&mut catalog, &updated_runtime)?;
    Ok(StatementResult::Ok)
}

fn current_time_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn cleanup_bootstrapped_tablets(tablet_ids: &[i64]) {
    if tablet_ids.is_empty() {
        return;
    }
    if let Err(err) = delete_tablet(&DeleteTabletRequest {
        tablet_ids: tablet_ids.to_vec(),
    }) {
        tracing::warn!(
            "StarRocks table create cleanup failed to delete bootstrapped tablets: tablet_ids={:?} error={}",
            tablet_ids,
            err
        );
        for tablet_id in tablet_ids {
            let _ = remove_tablet_runtime(*tablet_id);
        }
    }
}

fn cleanup_staged_truncate(
    state: &Arc<StandaloneState>,
    staged: &StagedStarRocksTruncate,
) -> Result<(), String> {
    for tablet_id in &staged.tablet_ids {
        let _ = remove_tablet_runtime(*tablet_id);
    }
    let provider = state.metadata_provider.as_ref().ok_or_else(|| {
        "StarRocks standalone TRUNCATE TABLE cleanup requires metadata provider".to_string()
    })?;
    let mut txn = provider
        .begin_write("cleanup StarRocks table truncate partition")
        .map_err(|e| format!("open StarRocks truncate cleanup transaction failed: {e}"))?;
    state
        .starrocks_table_repo
        .delete_creating_partition(txn.as_mut(), staged.partition_id)
        .map_err(|e| format!("delete creating truncate partition failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit StarRocks truncate cleanup failed: {e}"))?;
    Ok(())
}

fn bootstrap_truncated_partition(
    runtime: &StarRocksTableRuntime,
    starrocks_table_config: &StarRocksTableConfig,
    staged: &StagedStarRocksTruncate,
) -> Result<(), String> {
    let request_schema = request_schema_from_runtime(runtime)?;
    let object_store_profile =
        ObjectStoreProfile::from_s3_store_config(&starrocks_table_config.s3)?;
    let tablet_root_path = starrocks_table_config.tablet_root_path(
        runtime.table.db_id,
        runtime.table.table_id,
        staged.partition_id,
    );
    for tablet_id in &staged.tablet_ids {
        let request = build_create_tablet_request(
            *tablet_id,
            runtime.table.table_id,
            staged.partition_id,
            request_schema.clone(),
        );
        create_lake_tablet_from_req(
            &request,
            &tablet_root_path,
            Some(starrocks_table_config.s3.clone()),
        )?;
        let runtime_schema = get_tablet_runtime(*tablet_id)?.schema;
        let loaded = load_tablet_snapshot(
            *tablet_id,
            1,
            &tablet_root_path,
            Some(&object_store_profile),
        )?;
        if loaded.tablet_schema != runtime_schema {
            return Err(format!(
                "StarRocks truncate bootstrap schema mismatch after bootstrap: tablet_id={tablet_id}"
            ));
        }
    }
    Ok(())
}

pub(crate) fn bootstrap_empty_partition_for_tablets(
    runtime: &StarRocksTableRuntime,
    starrocks_table_config: &StarRocksTableConfig,
    partition_id: i64,
    tablet_ids: &[i64],
) -> Result<(), String> {
    let request_schema = request_schema_from_runtime(runtime)?;
    let object_store_profile =
        ObjectStoreProfile::from_s3_store_config(&starrocks_table_config.s3)?;
    let tablet_root_path = starrocks_table_config.tablet_root_path(
        runtime.table.db_id,
        runtime.table.table_id,
        partition_id,
    );
    for tablet_id in tablet_ids {
        let request = build_create_tablet_request(
            *tablet_id,
            runtime.table.table_id,
            partition_id,
            request_schema.clone(),
        );
        create_lake_tablet_from_req(
            &request,
            &tablet_root_path,
            Some(starrocks_table_config.s3.clone()),
        )?;
        let runtime_schema = get_tablet_runtime(*tablet_id)?.schema;
        let loaded = load_tablet_snapshot(
            *tablet_id,
            1,
            &tablet_root_path,
            Some(&object_store_profile),
        )?;
        if loaded.tablet_schema != runtime_schema {
            return Err(format!(
                "StarRocks bootstrap schema mismatch after bootstrap: tablet_id={tablet_id}"
            ));
        }
    }
    Ok(())
}

/// Maps the string aggregation representation stored in `StarRocksColumnSchema.aggregation`
/// back to the parser-level `ColumnAggregation` enum.
///
/// Returns `None` for `"NONE"` (no aggregation modifier).  Returns an error
/// for values that are unrecognised or unsupported in this context.
pub(crate) fn aggregation_string_to_column_aggregation(
    agg: &str,
) -> Result<Option<ColumnAggregation>, String> {
    match agg.trim().to_ascii_uppercase().as_str() {
        "NONE" => Ok(None),
        "SUM" => Ok(Some(ColumnAggregation::Sum)),
        "MIN" => Ok(Some(ColumnAggregation::Min)),
        "MAX" => Ok(Some(ColumnAggregation::Max)),
        "REPLACE" => Ok(Some(ColumnAggregation::Replace)),
        "REPLACE_IF_NOT_NULL" => Ok(Some(ColumnAggregation::ReplaceIfNotNull)),
        "BITMAP_UNION" => Ok(Some(ColumnAggregation::BitmapUnion)),
        "HLL_UNION" => Ok(Some(ColumnAggregation::HllUnion)),
        other => Err(format!(
            "unrecognised column aggregation string in tablet schema PB: `{other}`"
        )),
    }
}

pub(crate) fn logical_type_name(data_type: &SqlType) -> String {
    match data_type {
        SqlType::TinyInt => "TINYINT".to_string(),
        SqlType::SmallInt => "SMALLINT".to_string(),
        SqlType::Int => "INT".to_string(),
        SqlType::BigInt => "BIGINT".to_string(),
        SqlType::LargeInt => "LARGEINT".to_string(),
        SqlType::Float => "FLOAT".to_string(),
        SqlType::Double => "DOUBLE".to_string(),
        SqlType::String => "STRING".to_string(),
        SqlType::Json => "JSON".to_string(),
        SqlType::Boolean => "BOOLEAN".to_string(),
        SqlType::Date => "DATE".to_string(),
        SqlType::DateTime => "DATETIME".to_string(),
        SqlType::DateTimeNs => "TIMESTAMP_NS".to_string(),
        SqlType::Time => "TIME".to_string(),
        SqlType::Decimal { precision, scale } => format!("DECIMAL({precision},{scale})"),
        SqlType::Array(inner) => format!("ARRAY<{}>", logical_type_name(inner)),
        SqlType::Binary => "BINARY".to_string(),
        SqlType::Bitmap => "BITMAP".to_string(),
        SqlType::Hll => "HLL".to_string(),
        SqlType::Map(k, v) => format!("MAP<{},{}>", logical_type_name(k), logical_type_name(v)),
        SqlType::Struct(fields) => {
            let mut parts = Vec::with_capacity(fields.len());
            for (name, ty) in fields {
                parts.push(format!("{} {}", name, logical_type_name(ty)));
            }
            format!("STRUCT<{}>", parts.join(","))
        }
        SqlType::Variant => "VARIANT".to_string(),
    }
}

pub(crate) fn to_keys_type(kind: TableKeyKind) -> crate::thrift::types::TKeysType {
    match kind {
        TableKeyKind::Duplicate => crate::thrift::types::TKeysType::DUP_KEYS,
        TableKeyKind::Unique => crate::thrift::types::TKeysType::UNIQUE_KEYS,
        TableKeyKind::Aggregate => crate::thrift::types::TKeysType::AGG_KEYS,
        TableKeyKind::Primary => crate::thrift::types::TKeysType::PRIMARY_KEYS,
    }
}

pub(crate) fn keys_type_name(kind: TableKeyKind) -> &'static str {
    match kind {
        TableKeyKind::Duplicate => "DUP_KEYS",
        TableKeyKind::Unique => "UNIQUE_KEYS",
        TableKeyKind::Aggregate => "AGG_KEYS",
        TableKeyKind::Primary => "PRIMARY_KEYS",
    }
}

fn starrocks_table_root_path(warehouse_uri: &str, db_id: i64, table_id: i64) -> String {
    format!("{warehouse_uri}/db_{db_id}/table_{table_id}")
}

pub(crate) fn parse_keys_type(raw: &str) -> Result<TableKeyKind, String> {
    match raw.trim().to_ascii_uppercase().as_str() {
        "DUP_KEYS" => Ok(TableKeyKind::Duplicate),
        "UNIQUE_KEYS" => Ok(TableKeyKind::Unique),
        "AGG_KEYS" => Ok(TableKeyKind::Aggregate),
        "PRIMARY_KEYS" => Ok(TableKeyKind::Primary),
        other => Err(format!("unsupported StarRocks keys type `{other}`")),
    }
}

pub(crate) fn parse_starrocks_logical_type(raw: &str) -> Result<SqlType, String> {
    let normalized = raw.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "TINYINT" => Ok(SqlType::TinyInt),
        "SMALLINT" => Ok(SqlType::SmallInt),
        "INT" => Ok(SqlType::Int),
        "BIGINT" => Ok(SqlType::BigInt),
        "LARGEINT" => Ok(SqlType::LargeInt),
        "FLOAT" => Ok(SqlType::Float),
        "DOUBLE" => Ok(SqlType::Double),
        "STRING" => Ok(SqlType::String),
        "JSON" => Ok(SqlType::Json),
        "BITMAP" => Ok(SqlType::Bitmap),
        "HLL" => Ok(SqlType::Hll),
        "BOOLEAN" => Ok(SqlType::Boolean),
        "DATE" => Ok(SqlType::Date),
        "DATETIME" => Ok(SqlType::DateTime),
        "TIMESTAMP_NS" | "DATETIME_NS" => Ok(SqlType::DateTimeNs),
        "TIME" => Ok(SqlType::Time),
        _ => parse_decimal_logical_type(&normalized)
            .or_else(|_| parse_complex_starrocks_logical_type(raw.trim())),
    }
}

fn parse_decimal_logical_type(raw: &str) -> Result<SqlType, String> {
    let body = raw
        .strip_prefix("DECIMAL(")
        .and_then(|value| value.strip_suffix(')'))
        .ok_or_else(|| format!("unsupported StarRocks logical type `{raw}`"))?;
    let (precision, scale) = body
        .split_once(',')
        .ok_or_else(|| format!("invalid StarRocks DECIMAL logical type `{raw}`"))?;
    let precision = precision
        .trim()
        .parse::<u8>()
        .map_err(|e| format!("parse DECIMAL precision from `{raw}` failed: {e}"))?;
    let scale = scale
        .trim()
        .parse::<i8>()
        .map_err(|e| format!("parse DECIMAL scale from `{raw}` failed: {e}"))?;
    Ok(SqlType::Decimal { precision, scale })
}

fn parse_complex_starrocks_logical_type(raw: &str) -> Result<SqlType, String> {
    crate::sql::parser::dialect::create_table::parse_sql_type_string(raw)
        .map_err(|_| format!("unsupported StarRocks logical type `{raw}`"))
}
