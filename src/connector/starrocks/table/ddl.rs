use prost::Message;
use std::collections::HashSet;
use std::sync::Arc;

use crate::connector::starrocks::ObjectStoreProfile;
use crate::connector::starrocks::lake::context::{get_tablet_runtime, remove_tablet_runtime};
use crate::connector::starrocks::lake::create_lake_tablet_from_req;
use crate::connector::starrocks::lake::schema::create_lake_tablet_from_req_with_schema_patch;
use crate::connector::starrocks::lake::transactions::delete_tablet;
use crate::formats::starrocks::metadata::load_tablet_snapshot;
use crate::service::grpc_client::proto::starrocks::DeleteTabletRequest;
use crate::sql::parser::ast::{
    ColumnAggregation, ObjectName, SqlType, TableColumnDef, TableKeyDesc, TableKeyKind,
};

use super::catalog::{
    StarRocksTableCatalog, StarRocksTableRuntime, register_starrocks_table_in_catalog,
};
use super::model::{StarRocksPartitionState, StoredStarRocksColumn};
use crate::connector::starrocks::table::config::StarRocksTableConfig;
use crate::engine::catalog::normalize_identifier;
use crate::engine::{StandaloneState, StatementResult};
use crate::meta::repository::starrocks_table::{
    CreateStarRocksColumnRequest, CreateStarRocksTableLayoutRequest, StageStarRocksTruncateRequest,
    StagedStarRocksTruncate, StarRocksTableKind as RepoStarRocksTableKind,
};

/// Default bucket count when the user omits `DISTRIBUTED BY ... BUCKETS <n>`.
const DEFAULT_STARROCKS_BUCKET_COUNT: u32 = 1;
/// Mirrors StarRocks `SHORTKEY_MAX_COLUMN_COUNT`: at most 3 columns in the short-key.
const SHORT_KEY_MAX_COLUMN_COUNT: usize = 3;
/// Mirrors StarRocks `SHORTKEY_MAXSIZE_BYTES`: at most 36 bytes in the short-key.
const SHORT_KEY_MAX_SIZE_BYTES: usize = 36;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StarRocksPhysicalColumn {
    pub(crate) column: TableColumnDef,
    pub(crate) visible: bool,
    pub(crate) is_key: bool,
}

pub(crate) fn starrocks_physical_column(
    name: String,
    data_type: SqlType,
    nullable: bool,
    visible: bool,
    is_key: bool,
) -> StarRocksPhysicalColumn {
    StarRocksPhysicalColumn {
        column: TableColumnDef {
            name,
            data_type,
            nullable,
            aggregation: None,
            default: None,
        },
        visible,
        is_key,
    }
}

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
    schema: &mut crate::service::grpc_client::proto::starrocks::TabletSchemaPb,
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
    let catalog = state.catalog.read().expect("standalone catalog read lock");
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
        crate::connector::starrocks::lake::schema::build_tablet_schema_pb_from_thrift(
            &request_schema,
        )?;
    patch_tablet_schema_column_flags(&mut tablet_schema_pb, &physical_columns)?;
    state
        .starrocks_table_repo
        .update_schema_payload(
            txn.as_mut(),
            created.schema.schema_id,
            tablet_schema_pb.encode_to_vec(),
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
        let request = crate::agent_service::TCreateTabletReq {
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
            compression_type: Some(crate::types::TCompressionType::LZ4_FRAME),
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
        .catalog
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
        .catalog
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
        .catalog
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

pub(crate) fn request_schema_from_runtime(
    runtime: &StarRocksTableRuntime,
) -> Result<crate::agent_service::TTabletSchema, String> {
    // Build a name -> aggregation-string lookup from the tablet schema PB so
    // we can restore the ColumnAggregation modifier (BITMAP_UNION, HLL_UNION,
    // SUM, …) that StoredStarRocksColumn does not carry.
    let pb_agg_by_name: std::collections::HashMap<String, Option<String>> = runtime
        .tablet_schema
        .column
        .iter()
        .filter_map(|col| {
            let name = col.name.as_deref()?;
            let key = normalize_identifier(name).ok()?;
            Some((key, col.aggregation.clone()))
        })
        .collect();

    let columns = runtime
        .columns
        .iter()
        .map(|column| {
            let normalized = normalize_identifier(&column.column_name)?;
            let aggregation = match pb_agg_by_name.get(&normalized) {
                Some(agg_opt) => {
                    aggregation_string_to_column_aggregation(agg_opt.as_deref().unwrap_or("NONE"))?
                }
                None => None,
            };
            Ok(TableColumnDef {
                name: column.column_name.clone(),
                data_type: parse_starrocks_logical_type(&column.logical_type)?,
                nullable: column.nullable,
                aggregation,
                default: None,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let key_columns = runtime
        .columns
        .iter()
        .filter(|column| column.is_key)
        .map(|column| column.column_name.clone())
        .collect::<Vec<_>>();
    build_tablet_schema(
        &columns,
        &TableKeyDesc {
            kind: parse_keys_type(&runtime.table.keys_type)?,
            columns: key_columns,
        },
        runtime.table.current_schema_id,
    )
}

/// Maps the string aggregation representation stored in `ColumnPb.aggregation`
/// back to the parser-level `ColumnAggregation` enum.
///
/// Returns `None` for `"NONE"` (no aggregation modifier).  Returns an error
/// for values that are unrecognised or unsupported in this context.
fn aggregation_string_to_column_aggregation(
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

pub(crate) fn build_create_tablet_request(
    tablet_id: i64,
    table_id: i64,
    partition_id: i64,
    tablet_schema: crate::agent_service::TTabletSchema,
) -> crate::agent_service::TCreateTabletReq {
    crate::agent_service::TCreateTabletReq {
        tablet_id,
        tablet_schema,
        version: None,
        version_hash: None,
        storage_medium: None,
        in_restore_mode: None,
        base_tablet_id: None,
        base_schema_hash: None,
        table_id: Some(table_id),
        partition_id: Some(partition_id),
        allocation_term: None,
        is_eco_mode: None,
        storage_format: None,
        tablet_type: None,
        enable_persistent_index: Some(false),
        compression_type: Some(crate::types::TCompressionType::LZ4_FRAME),
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
    }
}

pub(crate) fn build_tablet_schema(
    columns: &[TableColumnDef],
    key_desc: &TableKeyDesc,
    schema_id: i64,
) -> Result<crate::agent_service::TTabletSchema, String> {
    let key_columns = key_desc
        .columns
        .iter()
        .map(|column| normalize_identifier(column))
        .collect::<Result<Vec<_>, _>>()?;
    let mut key_column_set = HashSet::with_capacity(key_columns.len());
    for key_column in &key_columns {
        if !key_column_set.insert(key_column.clone()) {
            return Err(format!(
                "duplicate key column `{key_column}` in StarRocks standalone CREATE TABLE"
            ));
        }
    }

    let mut key_indices = Vec::with_capacity(key_columns.len());
    let mut thrift_columns = Vec::with_capacity(columns.len());
    for (idx, column) in columns.iter().enumerate() {
        let normalized = normalize_identifier(&column.name)?;
        let is_key = key_column_set.contains(&normalized);
        if is_key {
            key_indices.push(idx as i32);
        }
        let complex = is_complex_type(&column.data_type);
        if complex && is_key {
            return Err(format!(
                "StarRocks standalone CREATE TABLE key column `{normalized}` cannot be a complex type ({:?})",
                column.data_type
            ));
        }
        let (column_type, type_desc) = if complex {
            (None, Some(sql_type_to_ttype_desc(&column.data_type)?))
        } else {
            (Some(sql_type_to_tcolumn_type(&column.data_type)?), None)
        };
        let aggregation_type = if is_key {
            if column.aggregation.is_some() {
                return Err(format!(
                    "StarRocks standalone CREATE TABLE key column `{normalized}` cannot have aggregation"
                ));
            }
            None
        } else {
            match key_desc.kind {
                TableKeyKind::Duplicate => None,
                TableKeyKind::Unique | TableKeyKind::Primary => {
                    Some(crate::types::TAggregationType::REPLACE)
                }
                TableKeyKind::Aggregate => {
                    let aggregation = column.aggregation.ok_or_else(|| {
                        format!(
                            "StarRocks standalone CREATE TABLE aggregate value column `{normalized}` requires aggregation"
                        )
                    })?;
                    Some(column_aggregation_to_thrift(aggregation))
                }
            }
        };
        thrift_columns.push(crate::descriptors::TColumn {
            column_name: normalized,
            column_type,
            aggregation_type,
            is_key: Some(is_key),
            is_allow_null: Some(column.nullable),
            default_value: None,
            default_expr: None,
            is_bloom_filter_column: Some(false),
            define_expr: None,
            is_auto_increment: Some(false),
            col_unique_id: Some(idx as i32),
            has_bitmap_index: Some(false),
            agg_state_desc: None,
            index_len: index_length_for_sql_type(&column.data_type),
            type_desc,
        });
    }
    if key_columns.is_empty() {
        return Err(
            "StarRocks standalone CREATE TABLE requires at least one key column".to_string(),
        );
    }
    if key_indices.len() != key_columns.len() {
        let missing = key_columns
            .into_iter()
            .filter(|key| {
                !thrift_columns
                    .iter()
                    .any(|column| column.column_name == *key)
            })
            .collect::<Vec<_>>();
        return Err(format!(
            "StarRocks standalone CREATE TABLE key columns are missing from table schema: {}",
            missing.join(", ")
        ));
    }
    if key_indices.is_empty() {
        return Err(
            "StarRocks standalone CREATE TABLE requires at least one key column".to_string(),
        );
    }
    let expected_prefix = (0..key_indices.len())
        .map(|idx| idx as i32)
        .collect::<Vec<_>>();
    if key_indices != expected_prefix {
        return Err(
            "StarRocks standalone CREATE TABLE requires key columns to be a leading column prefix"
                .to_string(),
        );
    }
    let key_count = key_indices.len();
    Ok(crate::agent_service::TTabletSchema {
        short_key_column_count: i16::try_from(key_count)
            .map_err(|_| "too many key columns for tablet schema".to_string())?,
        schema_hash: 1,
        keys_type: to_keys_type(key_desc.kind),
        storage_type: crate::types::TStorageType::COLUMN,
        columns: thrift_columns,
        bloom_filter_fpp: None,
        indexes: None,
        is_in_memory: Some(false),
        id: Some(schema_id),
        sort_key_idxes: Some(key_indices.clone()),
        sort_key_unique_ids: Some(key_indices),
        schema_version: Some(0),
        compression_type: Some(crate::types::TCompressionType::LZ4_FRAME),
        compression_level: None,
    })
}

fn column_aggregation_to_thrift(aggregation: ColumnAggregation) -> crate::types::TAggregationType {
    match aggregation {
        ColumnAggregation::Sum => crate::types::TAggregationType::SUM,
        ColumnAggregation::Min => crate::types::TAggregationType::MIN,
        ColumnAggregation::Max => crate::types::TAggregationType::MAX,
        ColumnAggregation::Replace => crate::types::TAggregationType::REPLACE,
        ColumnAggregation::ReplaceIfNotNull => crate::types::TAggregationType::REPLACE_IF_NOT_NULL,
        ColumnAggregation::BitmapUnion => crate::types::TAggregationType::BITMAP_UNION,
        ColumnAggregation::HllUnion => crate::types::TAggregationType::HLL_UNION,
    }
}

fn is_complex_type(data_type: &SqlType) -> bool {
    matches!(
        data_type,
        SqlType::Array(_) | SqlType::Map(_, _) | SqlType::Struct(_)
    )
}

fn sql_type_to_tcolumn_type(data_type: &SqlType) -> Result<crate::types::TColumnType, String> {
    let (primitive, len, precision, scale) = match data_type {
        SqlType::TinyInt => (crate::types::TPrimitiveType::TINYINT, Some(1), None, None),
        SqlType::SmallInt => (crate::types::TPrimitiveType::SMALLINT, Some(2), None, None),
        SqlType::Int => (crate::types::TPrimitiveType::INT, Some(4), None, None),
        SqlType::BigInt => (crate::types::TPrimitiveType::BIGINT, Some(8), None, None),
        SqlType::LargeInt => (crate::types::TPrimitiveType::LARGEINT, Some(16), None, None),
        SqlType::Float => (crate::types::TPrimitiveType::FLOAT, Some(4), None, None),
        SqlType::Double => (crate::types::TPrimitiveType::DOUBLE, Some(8), None, None),
        SqlType::String => (
            crate::types::TPrimitiveType::VARCHAR,
            Some(65_533),
            None,
            None,
        ),
        SqlType::Json => (crate::types::TPrimitiveType::JSON, Some(16), None, None),
        SqlType::Bitmap => (crate::types::TPrimitiveType::OBJECT, None, None, None),
        SqlType::Hll => (crate::types::TPrimitiveType::HLL, None, None, None),
        SqlType::Boolean => (crate::types::TPrimitiveType::BOOLEAN, Some(1), None, None),
        SqlType::Date => (crate::types::TPrimitiveType::DATE, Some(4), None, None),
        SqlType::DateTime => (crate::types::TPrimitiveType::DATETIME, Some(8), None, None),
        SqlType::DateTimeNs => (crate::types::TPrimitiveType::DATETIME, Some(8), None, None),
        SqlType::Time => (crate::types::TPrimitiveType::TIME, Some(8), None, None),
        SqlType::Decimal { precision, scale } => (
            crate::types::TPrimitiveType::DECIMAL128,
            None,
            Some(i32::from(*precision)),
            Some(i32::from(*scale)),
        ),
        SqlType::Binary => (
            crate::types::TPrimitiveType::VARBINARY,
            Some(65_533),
            None,
            None,
        ),
        SqlType::Array(_) | SqlType::Map(_, _) | SqlType::Struct(_) => {
            return Err(format!(
                "sql_type_to_tcolumn_type called on complex type {data_type:?}; callers must use sql_type_to_ttype_desc instead"
            ));
        }
        SqlType::Variant => {
            return Err(
                "VARIANT columns are only supported on iceberg tables; StarRocks table CREATE TABLE rejects VARIANT".to_string(),
            );
        }
    };
    Ok(crate::types::TColumnType {
        type_: primitive,
        len,
        index_len: len,
        precision,
        scale,
    })
}

/// Build a flat DFS list of `TTypeNode` that describes `data_type`.
/// Handles nested ARRAY/MAP/STRUCT so they round-trip through the
/// `create_tablet` protobuf path (`build_create_tablet_column_pb_from_type_desc`).
fn sql_type_to_ttype_desc(data_type: &SqlType) -> Result<crate::types::TTypeDesc, String> {
    let mut nodes = Vec::new();
    append_sql_type_nodes(data_type, &mut nodes)?;
    Ok(crate::types::TTypeDesc { types: Some(nodes) })
}

fn append_sql_type_nodes(
    data_type: &SqlType,
    nodes: &mut Vec<crate::types::TTypeNode>,
) -> Result<(), String> {
    match data_type {
        SqlType::Array(element) => {
            nodes.push(crate::types::TTypeNode {
                type_: crate::types::TTypeNodeType::ARRAY,
                scalar_type: None,
                is_named: None,
                struct_fields: None,
            });
            append_sql_type_nodes(element, nodes)
        }
        SqlType::Map(key, value) => {
            nodes.push(crate::types::TTypeNode {
                type_: crate::types::TTypeNodeType::MAP,
                scalar_type: None,
                is_named: None,
                struct_fields: None,
            });
            append_sql_type_nodes(key, nodes)?;
            append_sql_type_nodes(value, nodes)
        }
        SqlType::Struct(fields) => {
            let struct_fields = fields
                .iter()
                .map(|(name, _)| {
                    crate::types::TStructField::new(
                        Some(name.clone()),
                        None::<String>,
                        None::<i32>,
                        None::<String>,
                    )
                })
                .collect();
            nodes.push(crate::types::TTypeNode {
                type_: crate::types::TTypeNodeType::STRUCT,
                scalar_type: None,
                is_named: None,
                struct_fields: Some(struct_fields),
            });
            for (_, field_type) in fields {
                append_sql_type_nodes(field_type, nodes)?;
            }
            Ok(())
        }
        _ => {
            let scalar = sql_type_to_tcolumn_type(data_type)?;
            nodes.push(crate::types::TTypeNode {
                type_: crate::types::TTypeNodeType::SCALAR,
                scalar_type: Some(crate::types::TScalarType {
                    type_: scalar.type_,
                    len: scalar.len,
                    precision: scalar.precision,
                    scale: scalar.scale,
                    time_unit: None,
                }),
                is_named: None,
                struct_fields: None,
            });
            Ok(())
        }
    }
}

fn index_length_for_sql_type(data_type: &SqlType) -> Option<i32> {
    match data_type {
        SqlType::String => Some(10),
        SqlType::Json => None,
        SqlType::TinyInt => Some(1),
        SqlType::SmallInt => Some(2),
        SqlType::Int => Some(4),
        SqlType::BigInt | SqlType::DateTime | SqlType::DateTimeNs | SqlType::Time => Some(8),
        SqlType::LargeInt => Some(16),
        SqlType::Float => Some(4),
        SqlType::Double => Some(8),
        SqlType::Boolean => Some(1),
        SqlType::Date => Some(4),
        SqlType::Decimal { .. }
        | SqlType::Array(_)
        | SqlType::Binary
        | SqlType::Bitmap
        | SqlType::Hll
        | SqlType::Map(_, _)
        | SqlType::Struct(_)
        | SqlType::Variant => None,
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

fn to_keys_type(kind: TableKeyKind) -> crate::types::TKeysType {
    match kind {
        TableKeyKind::Duplicate => crate::types::TKeysType::DUP_KEYS,
        TableKeyKind::Unique => crate::types::TKeysType::UNIQUE_KEYS,
        TableKeyKind::Aggregate => crate::types::TKeysType::AGG_KEYS,
        TableKeyKind::Primary => crate::types::TKeysType::PRIMARY_KEYS,
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

fn parse_keys_type(raw: &str) -> Result<TableKeyKind, String> {
    match raw.trim().to_ascii_uppercase().as_str() {
        "DUP_KEYS" => Ok(TableKeyKind::Duplicate),
        "UNIQUE_KEYS" => Ok(TableKeyKind::Unique),
        "AGG_KEYS" => Ok(TableKeyKind::Aggregate),
        "PRIMARY_KEYS" => Ok(TableKeyKind::Primary),
        other => Err(format!("unsupported StarRocks keys type `{other}`")),
    }
}

fn parse_starrocks_logical_type(raw: &str) -> Result<SqlType, String> {
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use prost::Message;

    use crate::connector::starrocks::table::catalog::{
        StarRocksTableRuntime, register_starrocks_table_in_catalog,
    };
    use crate::connector::starrocks::table::model::{
        StarRocksGlobalMeta, StarRocksIndexState, StarRocksPartitionState, StarRocksTableKind,
        StarRocksTableSnapshot, StarRocksTableState, StarRocksTxnState, StoredStarRocksColumn,
        StoredStarRocksDatabase, StoredStarRocksIndex, StoredStarRocksPartition,
        StoredStarRocksSchema, StoredStarRocksTable, StoredStarRocksTablet, StoredStarRocksTxn,
    };
    use crate::connector::starrocks::table::{StarRocksTableCatalog, StarRocksTableConfig};
    use crate::engine::StandaloneState;
    use crate::engine::catalog::{DEFAULT_DATABASE, InMemoryCatalog};
    use crate::meta::repository::{id_scopes, test_avro_seed::encode_seed_payload};
    use crate::meta::{
        ExpectedRevision, MetaKey, MetaRecordKind, MetaRecordPut, MetaStoreProvider,
        SqliteMetaStoreProvider,
    };
    use crate::runtime::starlet_shard_registry::S3StoreConfig;
    use crate::sql::parser::ast::{
        ColumnAggregation, SqlType, TableColumnDef, TableKeyDesc, TableKeyKind,
    };

    use super::{
        build_tablet_schema, choose_default_dup_key_columns, drop_starrocks_table,
        key_eligible_type, logical_type_name, parse_starrocks_logical_type,
        patch_tablet_schema_column_flags, request_schema_from_runtime,
        resolve_starrocks_create_defaults, sql_type_to_tcolumn_type, sql_type_to_ttype_desc,
        starrocks_physical_column, stored_columns_from_physical_columns,
        table_columns_from_physical_columns, truncate_starrocks_table_with_hooks,
    };

    fn test_starrocks_table_config() -> StarRocksTableConfig {
        StarRocksTableConfig {
            warehouse_uri: "s3://test/warehouse".to_string(),
            s3: S3StoreConfig {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "test".to_string(),
                access_key_id: "ak".to_string(),
                access_key_secret: "sk".to_string(),
                region: Some("us-east-1".to_string()),
                enable_path_style_access: Some(true),
            },
            mv_default_storage_engine: "starrocks".to_string(),
        }
    }

    #[test]
    fn build_tablet_schema_sets_aggregate_value_column_aggregation() {
        let schema = build_tablet_schema(
            &[
                TableColumnDef {
                    name: "k1".to_string(),
                    data_type: SqlType::Int,
                    nullable: false,
                    aggregation: None,
                    default: None,
                },
                TableColumnDef {
                    name: "k2".to_string(),
                    data_type: SqlType::Int,
                    nullable: true,
                    aggregation: Some(ColumnAggregation::Sum),
                    default: None,
                },
            ],
            &TableKeyDesc {
                kind: TableKeyKind::Aggregate,
                columns: vec!["k1".to_string()],
            },
            100,
        )
        .expect("build aggregate-key schema");

        assert_eq!(schema.keys_type, crate::types::TKeysType::AGG_KEYS);
        assert_eq!(schema.columns[0].aggregation_type, None);
        assert_eq!(
            schema.columns[1].aggregation_type,
            Some(crate::types::TAggregationType::SUM)
        );
    }

    #[test]
    fn parse_starrocks_logical_type_round_trips_complex_types() {
        let cases = [
            SqlType::Array(Box::new(SqlType::BigInt)),
            SqlType::Map(Box::new(SqlType::String), Box::new(SqlType::Int)),
        ];

        for data_type in cases {
            let raw = logical_type_name(&data_type);
            let reparsed =
                parse_starrocks_logical_type(&raw).expect("reparse StarRocks logical type");
            assert_eq!(reparsed, data_type, "raw={raw}");
        }
    }

    fn snapshot_seed() -> StarRocksTableSnapshot {
        let request_schema = build_tablet_schema(
            &[
                TableColumnDef {
                    name: "k1".to_string(),
                    data_type: SqlType::Int,
                    nullable: false,
                    aggregation: None,
                    default: None,
                },
                TableColumnDef {
                    name: "v1".to_string(),
                    data_type: SqlType::String,
                    nullable: true,
                    aggregation: None,
                    default: None,
                },
            ],
            &TableKeyDesc {
                kind: TableKeyKind::Duplicate,
                columns: vec!["k1".to_string()],
            },
            100,
        )
        .expect("build request schema");
        let tablet_schema_pb =
            crate::connector::starrocks::lake::schema::build_tablet_schema_pb_from_thrift(
                &request_schema,
            )
            .expect("build tablet schema pb")
            .encode_to_vec();
        StarRocksTableSnapshot {
            global: StarRocksGlobalMeta {
                warehouse_uri: "s3://test/warehouse".to_string(),
                next_db_id: 2,
                next_table_id: 11,
                next_partition_id: 21,
                next_index_id: 31,
                next_tablet_id: 41,
                next_txn_id: 51,
            },
            databases: vec![StoredStarRocksDatabase {
                db_id: 1,
                name: DEFAULT_DATABASE.to_string(),
            }],
            tables: vec![StoredStarRocksTable {
                table_id: 10,
                db_id: 1,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 1,
                current_schema_id: 100,
                state: StarRocksTableState::Active,
                kind: StarRocksTableKind::Table,
            }],
            schemas: vec![StoredStarRocksSchema {
                schema_id: 100,
                table_id: 10,
                schema_version: 0,
                tablet_schema_pb,
            }],
            columns: vec![
                StoredStarRocksColumn {
                    schema_id: 100,
                    ordinal: 0,
                    column_name: "k1".to_string(),
                    logical_type: "INT".to_string(),
                    nullable: false,
                    visible: true,
                    is_key: true,
                },
                StoredStarRocksColumn {
                    schema_id: 100,
                    ordinal: 1,
                    column_name: "v1".to_string(),
                    logical_type: "STRING".to_string(),
                    nullable: true,
                    visible: true,
                    is_key: false,
                },
            ],
            partitions: vec![StoredStarRocksPartition {
                partition_id: 20,
                table_id: 10,
                name: "p0".to_string(),
                visible_version: 2,
                next_version: 3,
                state: StarRocksPartitionState::Active,
            }],
            indexes: vec![StoredStarRocksIndex {
                index_id: 30,
                table_id: 10,
                partition_id: 20,
                index_type: "BASE".to_string(),
                state: StarRocksIndexState::Active,
            }],
            tablets: vec![StoredStarRocksTablet {
                tablet_id: 40,
                partition_id: 20,
                index_id: 30,
                bucket_seq: 0,
                tablet_root_path: "s3://test/warehouse/db_1/table_10/partition_20".to_string(),
            }],
            txns: vec![StoredStarRocksTxn {
                txn_id: 50,
                table_id: 10,
                partition_id: 20,
                base_version: 1,
                commit_version: 2,
                state: StarRocksTxnState::Visible,
                retry_at_ms: None,
                updated_at_ms: 0,
            }],
            erase_jobs: Vec::new(),
            materialized_views: Vec::new(),
        }
    }

    fn seed_repository_snapshot(
        provider: &SqliteMetaStoreProvider,
        snapshot: &StarRocksTableSnapshot,
    ) -> Result<(), String> {
        let mut txn = provider
            .begin_write("seed StarRocks DDL test repositories")
            .map_err(|e| format!("begin seed txn failed: {e}"))?;
        for database in &snapshot.databases {
            put_seed_record(
                txn.as_mut(),
                "starrocks",
                vec!["database".to_string(), database.db_id.to_string()],
                "starrocks.database",
                serde_json::json!({"db_id": database.db_id, "name": database.name}),
            )?;
        }
        for table in &snapshot.tables {
            put_seed_record(
                txn.as_mut(),
                "starrocks",
                vec!["table".to_string(), table.table_id.to_string()],
                "starrocks.table",
                serde_json::json!({
                    "table_id": table.table_id,
                    "db_id": table.db_id,
                    "name": table.name,
                    "keys_type": table.keys_type,
                    "bucket_num": table.bucket_num,
                    "current_schema_id": table.current_schema_id,
                    "state": table_state(table.state),
                    "kind": table_kind(table.kind),
                }),
            )?;
        }
        for schema in &snapshot.schemas {
            put_seed_record(
                txn.as_mut(),
                "starrocks",
                vec!["schema".to_string(), schema.schema_id.to_string()],
                "starrocks.schema",
                serde_json::json!({
                    "schema_id": schema.schema_id,
                    "table_id": schema.table_id,
                    "schema_version": schema.schema_version,
                    "tablet_schema_pb": schema.tablet_schema_pb,
                }),
            )?;
        }
        for column in &snapshot.columns {
            put_seed_record(
                txn.as_mut(),
                "starrocks",
                vec![
                    "column".to_string(),
                    column.schema_id.to_string(),
                    column.ordinal.to_string(),
                ],
                "starrocks.column",
                serde_json::json!({
                    "schema_id": column.schema_id,
                    "ordinal": column.ordinal,
                    "column_name": column.column_name,
                    "logical_type": column.logical_type,
                    "nullable": column.nullable,
                    "visible": column.visible,
                    "is_key": column.is_key,
                }),
            )?;
        }
        for partition in &snapshot.partitions {
            put_seed_record(
                txn.as_mut(),
                "starrocks",
                vec!["partition".to_string(), partition.partition_id.to_string()],
                "starrocks.partition",
                serde_json::json!({
                    "partition_id": partition.partition_id,
                    "table_id": partition.table_id,
                    "name": partition.name,
                    "visible_version": partition.visible_version,
                    "next_version": partition.next_version,
                    "state": partition_state(partition.state),
                }),
            )?;
        }
        for index in &snapshot.indexes {
            put_seed_record(
                txn.as_mut(),
                "starrocks",
                vec!["index".to_string(), index.index_id.to_string()],
                "starrocks.index",
                serde_json::json!({
                    "index_id": index.index_id,
                    "table_id": index.table_id,
                    "partition_id": index.partition_id,
                    "index_type": index.index_type,
                    "state": index_state(index.state),
                }),
            )?;
        }
        for tablet in &snapshot.tablets {
            put_seed_record(
                txn.as_mut(),
                "starrocks",
                vec!["tablet".to_string(), tablet.tablet_id.to_string()],
                "starrocks.tablet",
                serde_json::json!({
                    "tablet_id": tablet.tablet_id,
                    "partition_id": tablet.partition_id,
                    "index_id": tablet.index_id,
                    "bucket_seq": tablet.bucket_seq,
                    "tablet_root_path": tablet.tablet_root_path,
                }),
            )?;
        }
        for starrocks_txn in &snapshot.txns {
            put_seed_record(
                txn.as_mut(),
                "starrocks.txn",
                vec![starrocks_txn.txn_id.to_string()],
                "starrocks.txn",
                serde_json::json!({
                    "txn_id": starrocks_txn.txn_id,
                    "table_id": starrocks_txn.table_id,
                    "partition_id": starrocks_txn.partition_id,
                    "base_version": starrocks_txn.base_version,
                    "commit_version": starrocks_txn.commit_version,
                    "state": txn_state(starrocks_txn.state),
                    "retry_at_ms": starrocks_txn.retry_at_ms,
                    "updated_at_ms": starrocks_txn.updated_at_ms,
                }),
            )?;
        }
        bump_id_scope(
            txn.as_mut(),
            id_scopes::starrocks_db(),
            snapshot
                .databases
                .iter()
                .map(|database| database.db_id)
                .max()
                .unwrap_or(0),
        )?;
        bump_id_scope(
            txn.as_mut(),
            id_scopes::starrocks_table(),
            snapshot
                .tables
                .iter()
                .map(|table| table.table_id)
                .max()
                .unwrap_or(0),
        )?;
        bump_id_scope(
            txn.as_mut(),
            id_scopes::starrocks_partition(),
            snapshot
                .partitions
                .iter()
                .map(|partition| partition.partition_id)
                .max()
                .unwrap_or(0),
        )?;
        bump_id_scope(
            txn.as_mut(),
            id_scopes::starrocks_index(),
            snapshot
                .indexes
                .iter()
                .map(|index| index.index_id)
                .max()
                .unwrap_or(0),
        )?;
        bump_id_scope(
            txn.as_mut(),
            id_scopes::starrocks_tablet(),
            snapshot
                .tablets
                .iter()
                .map(|tablet| tablet.tablet_id)
                .max()
                .unwrap_or(0),
        )?;
        bump_id_scope(
            txn.as_mut(),
            id_scopes::starrocks_txn(),
            snapshot
                .txns
                .iter()
                .map(|starrocks_txn| starrocks_txn.txn_id)
                .max()
                .unwrap_or(0),
        )?;
        txn.commit()
            .map_err(|e| format!("commit seed txn failed: {e}"))?;
        Ok(())
    }

    fn bump_id_scope(
        txn: &mut dyn crate::meta::MetaWriteTxn,
        scope: crate::meta::IdScope,
        max_existing: i64,
    ) -> Result<(), String> {
        for _ in 0..max_existing {
            txn.allocate_id(scope.clone()).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn put_seed_record(
        txn: &mut dyn crate::meta::MetaWriteTxn,
        namespace: &str,
        path: Vec<String>,
        kind: &str,
        payload: serde_json::Value,
    ) -> Result<(), String> {
        txn.put(MetaRecordPut::new(
            MetaKey::new(namespace, path).map_err(|e| e.to_string())?,
            MetaRecordKind::new(kind).map_err(|e| e.to_string())?,
            ExpectedRevision::NotExists,
            encode_seed_payload(kind, &payload).map_err(|e| e.to_string())?,
        ))
        .map_err(|e| e.to_string())
    }

    fn table_state(state: StarRocksTableState) -> &'static str {
        match state {
            StarRocksTableState::Creating => "CREATING",
            StarRocksTableState::Active => "ACTIVE",
            StarRocksTableState::Dropping => "DROPPING",
            StarRocksTableState::Failed => "FAILED",
        }
    }

    fn table_kind(kind: StarRocksTableKind) -> &'static str {
        match kind {
            StarRocksTableKind::Table => "TABLE",
            StarRocksTableKind::MaterializedView => "MATERIALIZED_VIEW",
        }
    }

    fn partition_state(state: StarRocksPartitionState) -> &'static str {
        match state {
            StarRocksPartitionState::Creating => "CREATING",
            StarRocksPartitionState::Active => "ACTIVE",
            StarRocksPartitionState::Retired => "RETIRED",
            StarRocksPartitionState::Failed => "FAILED",
        }
    }

    fn index_state(state: StarRocksIndexState) -> &'static str {
        match state {
            StarRocksIndexState::Creating => "CREATING",
            StarRocksIndexState::Active => "ACTIVE",
            StarRocksIndexState::Retired => "RETIRED",
            StarRocksIndexState::Failed => "FAILED",
        }
    }

    fn txn_state(state: StarRocksTxnState) -> &'static str {
        match state {
            StarRocksTxnState::Prepared => "PREPARED",
            StarRocksTxnState::Written => "WRITTEN",
            StarRocksTxnState::Visible => "VISIBLE",
            StarRocksTxnState::Aborted => "ABORTED",
        }
    }

    fn seeded_state() -> (tempfile::TempDir, Arc<StandaloneState>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let snapshot = snapshot_seed();
        let metadata_provider = SqliteMetaStoreProvider::open(dir.path().join("standalone.sqlite"))
            .expect("open provider");
        seed_repository_snapshot(&metadata_provider, &snapshot).expect("seed repositories");

        let starrocks =
            StarRocksTableCatalog::rebuild(Some(test_starrocks_table_config()), snapshot)
                .expect("rebuild StarRocks catalog");
        let runtime = starrocks
            .table(DEFAULT_DATABASE, "orders")
            .expect("StarRocks runtime")
            .clone();

        let mut catalog = InMemoryCatalog::default();
        register_starrocks_table_in_catalog(&mut catalog, &runtime)
            .expect("register StarRocks table");
        let mut state = StandaloneState {
            starrocks_table: RwLock::new(starrocks),
            starrocks_table_config: Some(test_starrocks_table_config()),
            metadata_provider: Some(Arc::new(metadata_provider)),
            ..StandaloneState::default()
        };
        state.catalog = Arc::new(RwLock::new(catalog));
        let state = Arc::new(state);
        crate::connector::register_default_catalog_mgr_entries(&state);
        (dir, state)
    }

    #[test]
    fn drop_starrocks_table_removes_catalog_entry_and_marks_metadata_dropping() {
        // `seeded_state` registers StarRocks tablets into the global shard
        // registry via `register_tablet_runtime`. Serialize with other tests
        // that read/write the same registry to avoid clobbering each other.
        let _runtime_guard = crate::connector::starrocks::lake::context::lock_runtime_test_state();
        let (_dir, state) = seeded_state();

        drop_starrocks_table(&state, DEFAULT_DATABASE, "orders").expect("drop StarRocks table");

        let catalog = state.catalog.read().expect("catalog read lock");
        let lookup = catalog.get(DEFAULT_DATABASE, "orders");
        assert!(
            lookup.is_err(),
            "dropped table should leave logical catalog"
        );
        drop(catalog);

        let starrocks = state
            .starrocks_table
            .read()
            .expect("standalone StarRocks table read lock");
        assert!(
            !starrocks
                .contains_table(DEFAULT_DATABASE, "orders")
                .expect("contains table"),
            "dropped table should leave StarRocks runtime catalog"
        );
        drop(starrocks);

        let read = state
            .metadata_provider
            .as_ref()
            .expect("provider")
            .begin_read()
            .expect("read");
        let persisted = state
            .starrocks_table_repo
            .load_snapshot(read.as_ref())
            .expect("reload snapshot");
        assert_eq!(persisted.tables.len(), 1);
        assert_eq!(
            persisted.tables[0].state,
            crate::meta::repository::starrocks_table::StarRocksTableState::Dropping
        );
        assert_eq!(
            persisted.partitions[0].state,
            crate::meta::repository::starrocks_table::StarRocksPartitionState::Retired
        );
        assert_eq!(
            persisted.indexes[0].state,
            crate::meta::repository::starrocks_table::StarRocksIndexState::Retired
        );
        let jobs = state
            .job_repo
            .list_runnable_erase_jobs(read.as_ref(), i64::MAX)
            .expect("erase jobs");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].root_path, "s3://test/warehouse/db_1/table_10");
    }

    #[test]
    fn truncate_starrocks_table_replaces_active_partition_and_updates_catalog_layout() {
        // See drop_starrocks_table_removes_catalog_entry_and_marks_metadata_dropping
        // for why we hold the runtime-test lock here.
        let _runtime_guard = crate::connector::starrocks::lake::context::lock_runtime_test_state();
        let (_dir, state) = seeded_state();

        truncate_starrocks_table_with_hooks(
            &state,
            DEFAULT_DATABASE,
            "orders",
            |_, _, _| Ok(()),
            |_| Ok(()),
        )
        .expect("truncate StarRocks table");

        let catalog = state.catalog.read().expect("catalog read lock");
        let layout = catalog
            .get_physical_layout(DEFAULT_DATABASE, "orders")
            .expect("physical layout lookup")
            .expect("StarRocks physical layout");
        assert_eq!(layout.table_id, 10);
        assert_eq!(layout.tablets.len(), 1);
        assert_eq!(layout.tablets[0].tablet_id, 41);
        assert_eq!(layout.tablets[0].partition_id, 21);
        assert_eq!(layout.tablets[0].version, 1);
        drop(catalog);

        let starrocks = state
            .starrocks_table
            .read()
            .expect("standalone StarRocks table read lock");
        let runtime = starrocks
            .table(DEFAULT_DATABASE, "orders")
            .expect("StarRocks runtime after truncate");
        assert_eq!(runtime.partitions.len(), 1);
        assert_eq!(runtime.partitions[0].partition_id, 21);
        assert_eq!(runtime.partitions[0].visible_version, 1);
        assert_eq!(runtime.tablets.len(), 1);
        assert_eq!(runtime.tablets[0].tablet_id, 41);
        assert_eq!(
            runtime.tablets[0].tablet_root_path,
            "s3://test/warehouse/db_1/table_10/partition_21"
        );
        drop(starrocks);

        let read = state
            .metadata_provider
            .as_ref()
            .expect("provider")
            .begin_read()
            .expect("read");
        let persisted = state
            .starrocks_table_repo
            .load_snapshot(read.as_ref())
            .expect("reload snapshot");
        assert_eq!(persisted.partitions.len(), 2);
        assert_eq!(
            persisted.partitions[0].state,
            crate::meta::repository::starrocks_table::StarRocksPartitionState::Retired
        );
        assert_eq!(
            persisted.partitions[1].state,
            crate::meta::repository::starrocks_table::StarRocksPartitionState::Active
        );
        let jobs = state
            .job_repo
            .list_runnable_erase_jobs(read.as_ref(), i64::MAX)
            .expect("erase jobs");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].partition_id, Some(20));
        assert_eq!(
            jobs[0].root_path,
            "s3://test/warehouse/db_1/table_10/partition_20"
        );
    }

    #[test]
    fn request_schema_from_runtime_uses_stored_key_flags_for_physical_columns() {
        let runtime = StarRocksTableRuntime {
            database_name: DEFAULT_DATABASE.to_string(),
            table: StoredStarRocksTable {
                table_id: 10,
                db_id: 1,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 1,
                current_schema_id: 100,
                state: StarRocksTableState::Active,
                kind: StarRocksTableKind::Table,
            },
            tablet_schema: Default::default(),
            columns: vec![
                StoredStarRocksColumn {
                    schema_id: 100,
                    ordinal: 0,
                    column_name: "k1".to_string(),
                    logical_type: "INT".to_string(),
                    nullable: false,
                    visible: true,
                    is_key: true,
                },
                StoredStarRocksColumn {
                    schema_id: 100,
                    ordinal: 1,
                    column_name: "__hidden".to_string(),
                    logical_type: "BIGINT".to_string(),
                    nullable: true,
                    visible: false,
                    is_key: false,
                },
            ],
            partitions: Vec::new(),
            indexes: Vec::new(),
            tablets: Vec::new(),
        };

        let request_schema = request_schema_from_runtime(&runtime).expect("request schema");

        assert_eq!(request_schema.columns.len(), 2);
        assert_eq!(request_schema.columns[0].column_name, "k1");
        assert_eq!(request_schema.columns[0].is_key, Some(true));
        assert_eq!(request_schema.columns[1].column_name, "__hidden");
        assert_eq!(request_schema.columns[1].is_key, Some(false));
        assert_eq!(request_schema.short_key_column_count, 1);
    }

    #[test]
    fn request_schema_from_runtime_preserves_aggregation() {
        // Build a synthetic tablet schema PB with:
        //   k1 INT  (key, no aggregation)
        //   v_bm BITMAP  (BITMAP_UNION value column)
        //   v_hll HLL    (HLL_UNION value column)
        //   v_sum INT    (SUM value column)
        let make_col_pb = |name: &str, ty: &str, agg: Option<&str>| {
            crate::service::grpc_client::proto::starrocks::ColumnPb {
                unique_id: 0,
                name: Some(name.to_string()),
                r#type: ty.to_string(),
                is_key: Some(false),
                aggregation: agg.map(|s| s.to_string()),
                is_nullable: Some(true),
                visible: Some(true),
                ..Default::default()
            }
        };

        let tablet_schema = crate::service::grpc_client::proto::starrocks::TabletSchemaPb {
            keys_type: None,
            column: vec![
                make_col_pb("k1", "INT", Some("NONE")),
                make_col_pb("v_bm", "OBJECT", Some("BITMAP_UNION")),
                make_col_pb("v_hll", "HLL", Some("HLL_UNION")),
                make_col_pb("v_sum", "INT", Some("SUM")),
            ],
            ..Default::default()
        };

        let runtime = StarRocksTableRuntime {
            database_name: "db".to_string(),
            table: StoredStarRocksTable {
                table_id: 1,
                db_id: 1,
                name: "agg_tbl".to_string(),
                keys_type: "AGG_KEYS".to_string(),
                bucket_num: 1,
                current_schema_id: 1,
                state: StarRocksTableState::Active,
                kind: StarRocksTableKind::Table,
            },
            tablet_schema,
            columns: vec![
                StoredStarRocksColumn {
                    schema_id: 1,
                    ordinal: 0,
                    column_name: "k1".to_string(),
                    logical_type: "INT".to_string(),
                    nullable: false,
                    visible: true,
                    is_key: true,
                },
                StoredStarRocksColumn {
                    schema_id: 1,
                    ordinal: 1,
                    column_name: "v_bm".to_string(),
                    logical_type: "BITMAP".to_string(),
                    nullable: true,
                    visible: true,
                    is_key: false,
                },
                StoredStarRocksColumn {
                    schema_id: 1,
                    ordinal: 2,
                    column_name: "v_hll".to_string(),
                    logical_type: "HLL".to_string(),
                    nullable: true,
                    visible: true,
                    is_key: false,
                },
                StoredStarRocksColumn {
                    schema_id: 1,
                    ordinal: 3,
                    column_name: "v_sum".to_string(),
                    logical_type: "INT".to_string(),
                    nullable: true,
                    visible: true,
                    is_key: false,
                },
            ],
            partitions: Vec::new(),
            indexes: Vec::new(),
            tablets: Vec::new(),
        };

        let schema = request_schema_from_runtime(&runtime)
            .expect("request_schema_from_runtime with BITMAP_UNION/HLL_UNION columns");

        assert_eq!(schema.columns.len(), 4);

        // k1: key column — no aggregation
        assert_eq!(schema.columns[0].column_name, "k1");
        assert_eq!(schema.columns[0].aggregation_type, None);

        // v_bm: BITMAP_UNION value column
        assert_eq!(schema.columns[1].column_name, "v_bm");
        assert_eq!(
            schema.columns[1].aggregation_type,
            Some(crate::types::TAggregationType::BITMAP_UNION)
        );

        // v_hll: HLL_UNION value column
        assert_eq!(schema.columns[2].column_name, "v_hll");
        assert_eq!(
            schema.columns[2].aggregation_type,
            Some(crate::types::TAggregationType::HLL_UNION)
        );

        // v_sum: SUM value column
        assert_eq!(schema.columns[3].column_name, "v_sum");
        assert_eq!(
            schema.columns[3].aggregation_type,
            Some(crate::types::TAggregationType::SUM)
        );
    }

    #[test]
    fn starrocks_json_type_uses_starrocks_json_primitive() {
        let column_type = sql_type_to_tcolumn_type(&SqlType::Json).expect("json column type");
        assert_eq!(column_type.type_, crate::types::TPrimitiveType::JSON);
        assert_eq!(column_type.len, Some(16));
        assert_eq!(logical_type_name(&SqlType::Json), "JSON");
        assert_eq!(
            parse_starrocks_logical_type("JSON").expect("logical json"),
            SqlType::Json
        );

        let desc = sql_type_to_ttype_desc(&SqlType::Array(Box::new(SqlType::Json)))
            .expect("array<json> type desc");
        let nodes = desc.types.expect("type nodes");
        assert_eq!(nodes[0].type_, crate::types::TTypeNodeType::ARRAY);
        assert_eq!(
            nodes[1].scalar_type.as_ref().expect("scalar").type_,
            crate::types::TPrimitiveType::JSON
        );
    }

    #[test]
    fn physical_column_helpers_preserve_visibility_and_key_flags() {
        let physical_columns = vec![
            starrocks_physical_column("k1".to_string(), SqlType::Int, false, true, false),
            starrocks_physical_column("__sum_v1".to_string(), SqlType::BigInt, true, false, false),
        ];
        let key_desc = TableKeyDesc {
            kind: TableKeyKind::Duplicate,
            columns: vec!["k1".to_string()],
        };

        let table_columns = table_columns_from_physical_columns(&physical_columns);
        assert_eq!(table_columns.len(), 2);
        assert_eq!(table_columns[1].name, "__sum_v1");

        let stored = stored_columns_from_physical_columns(100, &key_desc, &physical_columns);
        assert_eq!(stored.len(), 2);
        assert!(stored[0].is_key, "key_desc should mark k1 as key");
        assert!(stored[0].visible);
        assert!(!stored[1].is_key);
        assert!(!stored[1].visible);

        let patch_columns = vec![
            starrocks_physical_column("k1".to_string(), SqlType::Int, false, true, true),
            starrocks_physical_column("__sum_v1".to_string(), SqlType::BigInt, true, false, false),
        ];
        let mut tablet_schema = crate::service::grpc_client::proto::starrocks::TabletSchemaPb {
            column: vec![
                crate::service::grpc_client::proto::starrocks::ColumnPb::default(),
                crate::service::grpc_client::proto::starrocks::ColumnPb::default(),
            ],
            ..Default::default()
        };
        patch_tablet_schema_column_flags(&mut tablet_schema, &patch_columns)
            .expect("patch tablet schema flags");

        assert_eq!(tablet_schema.column[0].visible, Some(true));
        assert_eq!(tablet_schema.column[0].is_key, Some(true));
        assert_eq!(tablet_schema.column[1].visible, Some(false));
        assert_eq!(tablet_schema.column[1].is_key, Some(false));
    }

    #[test]
    fn patch_tablet_schema_column_flags_rejects_column_count_mismatch() {
        let patch_columns = vec![starrocks_physical_column(
            "k1".to_string(),
            SqlType::Int,
            false,
            true,
            true,
        )];
        let mut tablet_schema = crate::service::grpc_client::proto::starrocks::TabletSchemaPb {
            column: Vec::new(),
            ..Default::default()
        };

        let err = patch_tablet_schema_column_flags(&mut tablet_schema, &patch_columns)
            .expect_err("column count mismatch should fail");

        assert!(err.contains("StarRocks tablet schema column count mismatch"));
    }

    #[test]
    fn create_starrocks_table_defaults_dup_key_first_non_float_column() {
        // Bare `CREATE TABLE t (k BIGINT, v STRING)` should default to
        // DUP KEY (k, v) (string column included, then stop) and 1 bucket.
        let defaults = resolve_starrocks_create_defaults(
            &[
                TableColumnDef {
                    name: "k".to_string(),
                    data_type: SqlType::BigInt,
                    nullable: false,
                    aggregation: None,
                    default: None,
                },
                TableColumnDef {
                    name: "v".to_string(),
                    data_type: SqlType::String,
                    nullable: true,
                    aggregation: None,
                    default: None,
                },
            ],
            None,
            None,
        )
        .expect("resolve defaults");

        assert_eq!(
            defaults.key_desc,
            TableKeyDesc {
                kind: TableKeyKind::Duplicate,
                columns: vec!["k".to_string(), "v".to_string()],
            }
        );
        assert_eq!(defaults.bucket_num, 1);
    }

    #[test]
    fn create_starrocks_table_defaults_skip_float_as_leading_key() {
        // CREATE TABLE t (f FLOAT, k INT, v STRING). No explicit KEY — FLOAT
        // is not key-eligible and must fail with the StarRocks-style error.
        let err = choose_default_dup_key_columns(&[
            TableColumnDef {
                name: "f".to_string(),
                data_type: SqlType::Float,
                nullable: false,
                aggregation: None,
                default: None,
            },
            TableColumnDef {
                name: "k".to_string(),
                data_type: SqlType::Int,
                nullable: false,
                aggregation: None,
                default: None,
            },
            TableColumnDef {
                name: "v".to_string(),
                data_type: SqlType::String,
                nullable: true,
                aggregation: None,
                default: None,
            },
        ])
        .expect_err("float first column should fail");

        assert!(err.contains("first column `f` cannot be a key column"));
    }

    #[test]
    fn create_starrocks_table_defaults_short_key_length_cap() {
        // Five BIGINT columns (8 bytes each) — short-key caps at 3 columns.
        let keys = choose_default_dup_key_columns(&[
            TableColumnDef {
                name: "k1".to_string(),
                data_type: SqlType::BigInt,
                nullable: false,
                aggregation: None,
                default: None,
            },
            TableColumnDef {
                name: "k2".to_string(),
                data_type: SqlType::BigInt,
                nullable: false,
                aggregation: None,
                default: None,
            },
            TableColumnDef {
                name: "k3".to_string(),
                data_type: SqlType::BigInt,
                nullable: false,
                aggregation: None,
                default: None,
            },
            TableColumnDef {
                name: "k4".to_string(),
                data_type: SqlType::BigInt,
                nullable: false,
                aggregation: None,
                default: None,
            },
            TableColumnDef {
                name: "k5".to_string(),
                data_type: SqlType::BigInt,
                nullable: false,
                aggregation: None,
                default: None,
            },
        ])
        .expect("choose keys");

        assert_eq!(
            keys,
            vec!["k1".to_string(), "k2".to_string(), "k3".to_string()]
        );
    }

    #[test]
    fn create_starrocks_table_defaults_first_column_must_be_keyable() {
        // CREATE TABLE t (d DOUBLE, v INT) with no explicit KEY — DOUBLE is not
        // key-eligible, so the first-column check should fail with the StarRocks
        // "data type of first column cannot be a key column" error.
        let err = choose_default_dup_key_columns(&[
            TableColumnDef {
                name: "d".to_string(),
                data_type: SqlType::Double,
                nullable: false,
                aggregation: None,
                default: None,
            },
            TableColumnDef {
                name: "v".to_string(),
                data_type: SqlType::Int,
                nullable: false,
                aggregation: None,
                default: None,
            },
        ])
        .expect_err("double first column should fail");

        assert!(err.contains("first column `d` cannot be a key column"));
    }

    #[test]
    fn bitmap_hll_thrift_mapping() {
        let bm = sql_type_to_tcolumn_type(&SqlType::Bitmap).expect("bitmap thrift");
        assert_eq!(bm.type_, crate::types::TPrimitiveType::OBJECT);

        let hv = sql_type_to_tcolumn_type(&SqlType::Hll).expect("hll thrift");
        assert_eq!(hv.type_, crate::types::TPrimitiveType::HLL);

        assert_eq!(logical_type_name(&SqlType::Bitmap), "BITMAP");
        assert_eq!(logical_type_name(&SqlType::Hll), "HLL");

        assert_eq!(
            parse_starrocks_logical_type("BITMAP").expect("bitmap parse"),
            SqlType::Bitmap
        );
        assert_eq!(
            parse_starrocks_logical_type("HLL").expect("hll parse"),
            SqlType::Hll
        );

        // BITMAP/HLL are not eligible as key columns.
        assert!(!key_eligible_type(&SqlType::Bitmap));
        assert!(!key_eligible_type(&SqlType::Hll));
    }
}
