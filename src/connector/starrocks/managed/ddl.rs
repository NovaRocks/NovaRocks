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

use super::catalog::{ManagedLakeCatalog, ManagedTableRuntime, register_managed_table_in_catalog};
use super::model::{ManagedPartitionState, StoredManagedColumn};
use crate::connector::starrocks::managed::config::ManagedLakeConfig;
use crate::engine::catalog::normalize_identifier;
use crate::engine::{StandaloneState, StatementResult};
use crate::meta::repository::managed_lake::{
    CreateManagedColumnRequest, CreateManagedTableLayoutRequest,
    ManagedTableKind as RepoManagedTableKind, StageManagedTruncateRequest, StagedManagedTruncate,
};

/// Default bucket count when the user omits `DISTRIBUTED BY ... BUCKETS <n>`.
const DEFAULT_MANAGED_BUCKET_COUNT: u32 = 1;
/// Mirrors StarRocks `SHORTKEY_MAX_COLUMN_COUNT`: at most 3 columns in the short-key.
const SHORT_KEY_MAX_COLUMN_COUNT: usize = 3;
/// Mirrors StarRocks `SHORTKEY_MAXSIZE_BYTES`: at most 36 bytes in the short-key.
const SHORT_KEY_MAX_SIZE_BYTES: usize = 36;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ManagedPhysicalColumn {
    pub(crate) column: TableColumnDef,
    pub(crate) visible: bool,
    pub(crate) is_key: bool,
}

pub(crate) fn managed_physical_column(
    name: String,
    data_type: SqlType,
    nullable: bool,
    visible: bool,
    is_key: bool,
) -> ManagedPhysicalColumn {
    ManagedPhysicalColumn {
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
    columns: &[ManagedPhysicalColumn],
) -> Vec<TableColumnDef> {
    columns.iter().map(|column| column.column.clone()).collect()
}

pub(crate) fn stored_columns_from_physical_columns(
    schema_id: i64,
    key_desc: &TableKeyDesc,
    columns: &[ManagedPhysicalColumn],
) -> Vec<StoredManagedColumn> {
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
            StoredManagedColumn {
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
    columns: &[ManagedPhysicalColumn],
) -> Result<(), String> {
    if schema.column.len() != columns.len() {
        return Err(format!(
            "managed tablet schema column count mismatch: schema_columns={} physical_columns={}",
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

pub(crate) fn create_managed_table(
    state: &StandaloneState,
    name: &ObjectName,
    current_database: &str,
    columns: &[TableColumnDef],
    key_desc: Option<&TableKeyDesc>,
    bucket_count: Option<u32>,
) -> Result<StatementResult, String> {
    let resolved = resolve_local_managed_table_name(name, current_database)?;
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

    let managed_config = state
        .managed_lake_config
        .clone()
        .ok_or_else(|| "standalone managed lake config is missing".to_string())?;
    let defaults = resolve_managed_create_defaults(columns, key_desc, bucket_count)?;
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "managed standalone CREATE TABLE requires metadata provider".to_string())?;

    let mut guard = state
        .managed_lake
        .write()
        .expect("standalone managed lake write lock");
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
            Ok(ManagedPhysicalColumn {
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
            .map(|column| CreateManagedColumnRequest {
                column_name: column.column_name,
                logical_type: column.logical_type,
                nullable: column.nullable,
                visible: column.visible,
                is_key: column.is_key,
            })
            .collect::<Vec<_>>();

    let mut txn = provider
        .begin_write("create managed lake table")
        .map_err(|e| format!("open managed table create transaction failed: {e}"))?;
    let database = state
        .managed_repo
        .get_or_create_database(txn.as_mut(), &resolved.database)
        .map_err(|e| format!("create managed database metadata failed: {e}"))?;
    let reclaimed = state
        .managed_repo
        .purge_dropping_table_for_reuse(txn.as_mut(), database.db_id, &resolved.table)
        .map_err(|e| format!("reclaim dropping managed table metadata failed: {e}"))?;
    for table_id in &reclaimed {
        state
            .managed_txn_repo
            .delete_for_table(txn.as_mut(), *table_id)
            .map_err(|e| format!("delete reclaimed managed txns failed: {e}"))?;
        state
            .job_repo
            .delete_for_table(txn.as_mut(), *table_id)
            .map_err(|e| format!("delete reclaimed erase jobs failed: {e}"))?;
    }

    let created = state
        .managed_repo
        .create_table_layout(
            txn.as_mut(),
            CreateManagedTableLayoutRequest {
                db_id: database.db_id,
                table_name: resolved.table.clone(),
                keys_type: keys_type_name(defaults.key_desc.kind).to_string(),
                bucket_num: defaults.bucket_num,
                kind: RepoManagedTableKind::Table,
                schema_version: 0,
                tablet_schema_pb: Vec::new(),
                columns: stored_columns,
                partition_name: "p0".to_string(),
                warehouse_uri: managed_config.warehouse_uri.clone(),
            },
        )
        .map_err(|e| format!("create managed table metadata failed: {e}"))?;
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
        .managed_repo
        .update_schema_payload(
            txn.as_mut(),
            created.schema.schema_id,
            tablet_schema_pb.encode_to_vec(),
        )
        .map_err(|e| format!("update managed table schema metadata failed: {e}"))?;
    state
        .managed_txn_repo
        .record_visible_bootstrap(
            txn.as_mut(),
            created.table.table_id,
            created.partition.partition_id,
        )
        .map_err(|e| format!("create managed table bootstrap txn metadata failed: {e}"))?;

    let object_store_profile = ObjectStoreProfile::from_s3_store_config(&managed_config.s3)?;
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
            Some(managed_config.s3.clone()),
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
                "managed tablet schema mismatch after bootstrap: tablet_id={}",
                tablet.tablet_id
            ));
        }
    }
    if let Err(err) = txn.commit() {
        cleanup_bootstrapped_tablets(&bootstrapped_tablet_ids);
        return Err(format!("commit managed table metadata failed: {err}"));
    }

    let read = provider
        .begin_read()
        .map_err(|e| format!("open managed table reload transaction failed: {e}"))?;
    let snapshot = state
        .managed_repo
        .load_snapshot(read.as_ref())
        .map_err(|e| format!("reload managed table metadata failed: {e}"))?;
    let rebuilt =
        ManagedLakeCatalog::rebuild_from_repository(Some(managed_config), snapshot.clone())?;
    rebuilt.re_register_active_tablet_runtimes()?;
    *guard = rebuilt;
    let runtime = guard.table(&resolved.database, &resolved.table)?.clone();
    drop(guard);

    let mut catalog = state
        .catalog
        .write()
        .expect("standalone catalog write lock");
    register_managed_table_in_catalog(&mut catalog, &runtime)?;
    Ok(StatementResult::Ok)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ManagedCreateDefaults {
    key_desc: TableKeyDesc,
    bucket_num: i64,
}

/// Resolve StarRocks-style defaults for `CREATE TABLE` on managed lake:
/// - KEY description defaults to DUP KEY on leading non-float columns (short-key rules).
/// - BUCKETS defaults to 1.
fn resolve_managed_create_defaults(
    columns: &[TableColumnDef],
    key_desc: Option<&TableKeyDesc>,
    bucket_count: Option<u32>,
) -> Result<ManagedCreateDefaults, String> {
    let key_desc = match key_desc {
        Some(key_desc) => key_desc.clone(),
        None => TableKeyDesc {
            kind: TableKeyKind::Duplicate,
            columns: choose_default_dup_key_columns(columns)?,
        },
    };
    let bucket_num = i64::from(bucket_count.unwrap_or(DEFAULT_MANAGED_BUCKET_COUNT));
    if bucket_num <= 0 {
        return Err("managed standalone CREATE TABLE requires BUCKETS > 0".to_string());
    }
    Ok(ManagedCreateDefaults {
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
        return Err("managed standalone CREATE TABLE requires at least one column".to_string());
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
            "managed standalone CREATE TABLE data type of first column `{}` cannot be a key column",
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
        SqlType::BigInt | SqlType::DateTime | SqlType::Time => 8,
        SqlType::LargeInt | SqlType::Decimal { .. } => 16,
        SqlType::String | SqlType::Binary => 20,
        SqlType::Json => 16,
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

pub(crate) fn drop_managed_table(
    state: &Arc<StandaloneState>,
    database_name: &str,
    table_name: &str,
) -> Result<StatementResult, String> {
    drop_managed_table_with_metadata(state, database_name, table_name, |_, _| Ok(()))
}

pub(crate) fn drop_managed_table_with_metadata<F>(
    state: &Arc<StandaloneState>,
    database_name: &str,
    table_name: &str,
    update_metadata: F,
) -> Result<StatementResult, String>
where
    F: FnOnce(&mut dyn crate::meta::MetaWriteTxn, i64) -> Result<(), String>,
{
    let mut managed = state
        .managed_lake
        .write()
        .expect("standalone managed lake write lock");
    let runtime = managed.table(database_name, table_name)?.clone();
    let managed_config = state
        .managed_lake_config
        .as_ref()
        .ok_or_else(|| "standalone managed lake config is missing".to_string())?;
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "managed standalone DROP TABLE requires metadata provider".to_string())?;
    let table_root_path = managed_table_root_path(
        &managed_config.warehouse_uri,
        runtime.table.db_id,
        runtime.table.table_id,
    );
    let mut txn = provider
        .begin_write("drop managed lake table")
        .map_err(|e| format!("open managed table drop transaction failed: {e}"))?;
    state
        .managed_txn_repo
        .ensure_no_inflight_for_table(txn.as_ref(), runtime.table.table_id)
        .map_err(|e| format!("validate managed table drop failed: {e}"))?;
    update_metadata(txn.as_mut(), runtime.table.table_id)?;
    state
        .managed_repo
        .mark_table_dropping(txn.as_mut(), runtime.table.table_id)
        .map_err(|e| format!("mark managed table dropping failed: {e}"))?;
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
        .map_err(|e| format!("enqueue managed table erase job failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit managed table drop metadata failed: {e}"))?;
    for tablet in &runtime.tablets {
        remove_tablet_runtime(tablet.tablet_id)?;
    }

    let read = provider
        .begin_read()
        .map_err(|e| format!("open managed table drop reload transaction failed: {e}"))?;
    let snapshot = state
        .managed_repo
        .load_snapshot(read.as_ref())
        .map_err(|e| format!("reload managed table metadata failed: {e}"))?;
    let rebuilt =
        ManagedLakeCatalog::rebuild_from_repository(state.managed_lake_config.clone(), snapshot)?;
    *managed = rebuilt;
    let mut catalog = state
        .catalog
        .write()
        .expect("standalone catalog write lock");
    let _ = catalog.drop_table(database_name, table_name);
    Ok(StatementResult::Ok)
}

/// Remove the persisted `databases` entry for `database_name` after all of
/// its tables have been cascaded through `drop_managed_table`. This frees
/// the `db_id` so the next `CREATE DATABASE` allocates a fresh id, letting
/// `CREATE TABLE` on the same name succeed without colliding with the old
/// `(db_id, name)` UNIQUE rows left behind by tables still in the
/// `DROPPING` state (the erase worker cleans those asynchronously).
pub(crate) fn drop_managed_database_entry(
    state: &Arc<StandaloneState>,
    database_name: &str,
) -> Result<(), String> {
    let managed_config = state
        .managed_lake_config
        .clone()
        .ok_or_else(|| "standalone managed lake config is missing".to_string())?;
    let provider = state
        .metadata_provider
        .as_ref()
        .ok_or_else(|| "managed standalone DROP DATABASE requires metadata provider".to_string())?;

    let mut guard = state
        .managed_lake
        .write()
        .expect("standalone managed lake write lock");
    let mut txn = provider
        .begin_write("drop managed lake database entry")
        .map_err(|e| format!("open managed database drop transaction failed: {e}"))?;
    let dropped = state
        .managed_repo
        .drop_database_entry(txn.as_mut(), database_name)
        .map_err(|e| format!("drop managed database metadata failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit managed database drop metadata failed: {e}"))?;
    if !dropped {
        return Ok(());
    }
    let read = provider
        .begin_read()
        .map_err(|e| format!("open managed database reload transaction failed: {e}"))?;
    let snapshot = state
        .managed_repo
        .load_snapshot(read.as_ref())
        .map_err(|e| format!("reload managed database metadata failed: {e}"))?;
    let rebuilt = ManagedLakeCatalog::rebuild_from_repository(Some(managed_config), snapshot)?;
    *guard = rebuilt;
    Ok(())
}

pub(crate) fn truncate_managed_table(
    state: &Arc<StandaloneState>,
    database_name: &str,
    table_name: &str,
) -> Result<StatementResult, String> {
    truncate_managed_table_with_hooks(
        state,
        database_name,
        table_name,
        bootstrap_truncated_partition,
        |rebuilt| rebuilt.re_register_active_tablet_runtimes(),
    )
}

#[derive(Clone, Debug)]
struct ResolvedManagedTableName {
    database: String,
    table: String,
}

fn resolve_local_managed_table_name(
    name: &ObjectName,
    current_database: &str,
) -> Result<ResolvedManagedTableName, String> {
    match name.parts.as_slice() {
        [table] => Ok(ResolvedManagedTableName {
            database: normalize_identifier(current_database)?,
            table: normalize_identifier(table)?,
        }),
        [database, table] => Ok(ResolvedManagedTableName {
            database: normalize_identifier(database)?,
            table: normalize_identifier(table)?,
        }),
        _ => Err(format!(
            "managed table name must be `<table>` or `<database>.<table>`, got `{}`",
            name.parts.join(".")
        )),
    }
}

fn truncate_managed_table_with_hooks<Bootstrap, Refresh>(
    state: &Arc<StandaloneState>,
    database_name: &str,
    table_name: &str,
    bootstrap: Bootstrap,
    refresh_runtimes: Refresh,
) -> Result<StatementResult, String>
where
    Bootstrap: FnOnce(
        &ManagedTableRuntime,
        &ManagedLakeConfig,
        &StagedManagedTruncate,
    ) -> Result<(), String>,
    Refresh: FnOnce(&ManagedLakeCatalog) -> Result<(), String>,
{
    let mut managed = state
        .managed_lake
        .write()
        .expect("standalone managed lake write lock");
    let runtime = managed.table(database_name, table_name)?.clone();
    let managed_config = state
        .managed_lake_config
        .as_ref()
        .ok_or_else(|| "standalone managed lake config is missing".to_string())?;
    let provider = state.metadata_provider.as_ref().ok_or_else(|| {
        "managed standalone TRUNCATE TABLE requires metadata provider".to_string()
    })?;
    let active_partition = runtime
        .partitions
        .iter()
        .find(|partition| partition.state == ManagedPartitionState::Active)
        .cloned()
        .ok_or_else(|| {
            format!(
                "managed table {}.{} does not have an active partition",
                database_name, table_name
            )
        })?;
    let staged = {
        let mut txn = provider
            .begin_write("stage managed lake truncate partition")
            .map_err(|e| format!("open managed truncate stage transaction failed: {e}"))?;
        state
            .managed_txn_repo
            .ensure_no_inflight_for_table(txn.as_ref(), runtime.table.table_id)
            .map_err(|e| format!("validate managed truncate failed: {e}"))?;
        let staged = state
            .managed_repo
            .stage_truncate_partition(
                txn.as_mut(),
                StageManagedTruncateRequest {
                    table_id: runtime.table.table_id,
                    db_id: runtime.table.db_id,
                    bucket_num: runtime.table.bucket_num,
                    partition_name: active_partition.name.clone(),
                    warehouse_uri: managed_config.warehouse_uri.clone(),
                },
            )
            .map_err(|e| format!("stage managed truncate metadata failed: {e}"))?;
        txn.commit()
            .map_err(|e| format!("commit managed truncate stage metadata failed: {e}"))?;
        staged
    };
    if let Err(err) = bootstrap(&runtime, managed_config, &staged) {
        cleanup_staged_truncate(state, &staged)?;
        return Err(format!(
            "bootstrap truncate partition failed for {}.{}: {err}",
            database_name, table_name
        ));
    }
    let retired_root_path = managed_config.tablet_root_path(
        runtime.table.db_id,
        runtime.table.table_id,
        active_partition.partition_id,
    );
    if let Err(err) = (|| {
        let mut txn = provider
            .begin_write("activate managed lake truncate partition")
            .map_err(|e| format!("open managed truncate activate transaction failed: {e}"))?;
        state
            .managed_repo
            .activate_truncate_partition(
                txn.as_mut(),
                runtime.table.table_id,
                active_partition.partition_id,
                staged.partition_id,
                staged.index_id,
            )
            .map_err(|e| format!("activate managed truncate metadata failed: {e}"))?;
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
            .map_err(|e| format!("enqueue managed truncate erase job failed: {e}"))?;
        txn.commit()
            .map_err(|e| format!("commit managed truncate activate metadata failed: {e}"))?;
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
        .map_err(|e| format!("open managed truncate reload transaction failed: {e}"))?;
    let rebuilt_snapshot = state
        .managed_repo
        .load_snapshot(read.as_ref())
        .map_err(|e| format!("reload managed truncate metadata failed: {e}"))?;
    let rebuilt = ManagedLakeCatalog::rebuild_from_repository(
        state.managed_lake_config.clone(),
        rebuilt_snapshot,
    )?;
    refresh_runtimes(&rebuilt)?;
    let updated_runtime = rebuilt.table(database_name, table_name)?.clone();
    *managed = rebuilt;
    let mut catalog = state
        .catalog
        .write()
        .expect("standalone catalog write lock");
    register_managed_table_in_catalog(&mut catalog, &updated_runtime)?;
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
            "managed table create cleanup failed to delete bootstrapped tablets: tablet_ids={:?} error={}",
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
    staged: &StagedManagedTruncate,
) -> Result<(), String> {
    for tablet_id in &staged.tablet_ids {
        let _ = remove_tablet_runtime(*tablet_id);
    }
    let provider = state.metadata_provider.as_ref().ok_or_else(|| {
        "managed standalone TRUNCATE TABLE cleanup requires metadata provider".to_string()
    })?;
    let mut txn = provider
        .begin_write("cleanup managed lake truncate partition")
        .map_err(|e| format!("open managed truncate cleanup transaction failed: {e}"))?;
    state
        .managed_repo
        .delete_creating_partition(txn.as_mut(), staged.partition_id)
        .map_err(|e| format!("delete creating truncate partition failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit managed truncate cleanup failed: {e}"))?;
    Ok(())
}

fn bootstrap_truncated_partition(
    runtime: &ManagedTableRuntime,
    managed_config: &ManagedLakeConfig,
    staged: &StagedManagedTruncate,
) -> Result<(), String> {
    let request_schema = request_schema_from_runtime(runtime)?;
    let object_store_profile = ObjectStoreProfile::from_s3_store_config(&managed_config.s3)?;
    let tablet_root_path = managed_config.tablet_root_path(
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
        create_lake_tablet_from_req(&request, &tablet_root_path, Some(managed_config.s3.clone()))?;
        let runtime_schema = get_tablet_runtime(*tablet_id)?.schema;
        let loaded = load_tablet_snapshot(
            *tablet_id,
            1,
            &tablet_root_path,
            Some(&object_store_profile),
        )?;
        if loaded.tablet_schema != runtime_schema {
            return Err(format!(
                "managed truncate bootstrap schema mismatch after bootstrap: tablet_id={tablet_id}"
            ));
        }
    }
    Ok(())
}

pub(crate) fn bootstrap_empty_partition_for_tablets(
    runtime: &ManagedTableRuntime,
    managed_config: &ManagedLakeConfig,
    partition_id: i64,
    tablet_ids: &[i64],
) -> Result<(), String> {
    let request_schema = request_schema_from_runtime(runtime)?;
    let object_store_profile = ObjectStoreProfile::from_s3_store_config(&managed_config.s3)?;
    let tablet_root_path =
        managed_config.tablet_root_path(runtime.table.db_id, runtime.table.table_id, partition_id);
    for tablet_id in tablet_ids {
        let request = build_create_tablet_request(
            *tablet_id,
            runtime.table.table_id,
            partition_id,
            request_schema.clone(),
        );
        create_lake_tablet_from_req(&request, &tablet_root_path, Some(managed_config.s3.clone()))?;
        let runtime_schema = get_tablet_runtime(*tablet_id)?.schema;
        let loaded = load_tablet_snapshot(
            *tablet_id,
            1,
            &tablet_root_path,
            Some(&object_store_profile),
        )?;
        if loaded.tablet_schema != runtime_schema {
            return Err(format!(
                "managed bootstrap schema mismatch after bootstrap: tablet_id={tablet_id}"
            ));
        }
    }
    Ok(())
}

pub(crate) fn request_schema_from_runtime(
    runtime: &ManagedTableRuntime,
) -> Result<crate::agent_service::TTabletSchema, String> {
    let columns = runtime
        .columns
        .iter()
        .map(|column| {
            Ok(TableColumnDef {
                name: column.column_name.clone(),
                data_type: parse_managed_logical_type(&column.logical_type)?,
                nullable: column.nullable,
                aggregation: None,
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
                "duplicate key column `{key_column}` in managed standalone CREATE TABLE"
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
                "managed standalone CREATE TABLE key column `{normalized}` cannot be a complex type ({:?})",
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
                    "managed standalone CREATE TABLE key column `{normalized}` cannot have aggregation"
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
                            "managed standalone CREATE TABLE aggregate value column `{normalized}` requires aggregation"
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
        return Err("managed standalone CREATE TABLE requires at least one key column".to_string());
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
            "managed standalone CREATE TABLE key columns are missing from table schema: {}",
            missing.join(", ")
        ));
    }
    if key_indices.is_empty() {
        return Err("managed standalone CREATE TABLE requires at least one key column".to_string());
    }
    let expected_prefix = (0..key_indices.len())
        .map(|idx| idx as i32)
        .collect::<Vec<_>>();
    if key_indices != expected_prefix {
        return Err(
            "managed standalone CREATE TABLE requires key columns to be a leading column prefix"
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
        SqlType::Boolean => (crate::types::TPrimitiveType::BOOLEAN, Some(1), None, None),
        SqlType::Date => (crate::types::TPrimitiveType::DATE, Some(4), None, None),
        SqlType::DateTime => (crate::types::TPrimitiveType::DATETIME, Some(8), None, None),
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
                "VARIANT columns are only supported on iceberg tables; managed-lake CREATE TABLE rejects VARIANT".to_string(),
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
        SqlType::BigInt | SqlType::DateTime | SqlType::Time => Some(8),
        SqlType::LargeInt => Some(16),
        SqlType::Float => Some(4),
        SqlType::Double => Some(8),
        SqlType::Boolean => Some(1),
        SqlType::Date => Some(4),
        SqlType::Decimal { .. }
        | SqlType::Array(_)
        | SqlType::Binary
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
        SqlType::Time => "TIME".to_string(),
        SqlType::Decimal { precision, scale } => format!("DECIMAL({precision},{scale})"),
        SqlType::Array(inner) => format!("ARRAY<{}>", logical_type_name(inner)),
        SqlType::Binary => "BINARY".to_string(),
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

fn managed_table_root_path(warehouse_uri: &str, db_id: i64, table_id: i64) -> String {
    format!("{warehouse_uri}/db_{db_id}/table_{table_id}")
}

fn parse_keys_type(raw: &str) -> Result<TableKeyKind, String> {
    match raw.trim().to_ascii_uppercase().as_str() {
        "DUP_KEYS" => Ok(TableKeyKind::Duplicate),
        "UNIQUE_KEYS" => Ok(TableKeyKind::Unique),
        "AGG_KEYS" => Ok(TableKeyKind::Aggregate),
        "PRIMARY_KEYS" => Ok(TableKeyKind::Primary),
        other => Err(format!("unsupported managed keys type `{other}`")),
    }
}

fn parse_managed_logical_type(raw: &str) -> Result<SqlType, String> {
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
        "BOOLEAN" => Ok(SqlType::Boolean),
        "DATE" => Ok(SqlType::Date),
        "DATETIME" => Ok(SqlType::DateTime),
        "TIME" => Ok(SqlType::Time),
        _ => parse_decimal_logical_type(&normalized)
            .or_else(|_| parse_complex_managed_logical_type(raw.trim())),
    }
}

fn parse_decimal_logical_type(raw: &str) -> Result<SqlType, String> {
    let body = raw
        .strip_prefix("DECIMAL(")
        .and_then(|value| value.strip_suffix(')'))
        .ok_or_else(|| format!("unsupported managed logical type `{raw}`"))?;
    let (precision, scale) = body
        .split_once(',')
        .ok_or_else(|| format!("invalid managed DECIMAL logical type `{raw}`"))?;
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

fn parse_complex_managed_logical_type(raw: &str) -> Result<SqlType, String> {
    crate::sql::parser::dialect::create_table::parse_sql_type_string(raw)
        .map_err(|_| format!("unsupported managed logical type `{raw}`"))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use prost::Message;

    use crate::connector::starrocks::managed::catalog::{
        ManagedTableRuntime, register_managed_table_in_catalog,
    };
    use crate::connector::starrocks::managed::model::{
        ManagedGlobalMeta, ManagedIndexState, ManagedPartitionState, ManagedSnapshot,
        ManagedTableKind, ManagedTableState, ManagedTxnState, StoredManagedColumn,
        StoredManagedDatabase, StoredManagedIndex, StoredManagedPartition, StoredManagedSchema,
        StoredManagedTable, StoredManagedTablet, StoredManagedTxn,
    };
    use crate::connector::starrocks::managed::{ManagedLakeCatalog, ManagedLakeConfig};
    use crate::engine::StandaloneState;
    use crate::engine::catalog::{DEFAULT_DATABASE, InMemoryCatalog};
    use crate::meta::repository::{encode_json_payload, id_scopes};
    use crate::meta::{
        ExpectedRevision, MetaKey, MetaRecordKind, MetaRecordPut, MetaStoreProvider,
        SqliteMetaStoreProvider,
    };
    use crate::runtime::starlet_shard_registry::S3StoreConfig;
    use crate::sql::parser::ast::{
        ColumnAggregation, SqlType, TableColumnDef, TableKeyDesc, TableKeyKind,
    };

    use super::{
        build_tablet_schema, choose_default_dup_key_columns, drop_managed_table, logical_type_name,
        managed_physical_column, parse_managed_logical_type, patch_tablet_schema_column_flags,
        request_schema_from_runtime, resolve_managed_create_defaults, sql_type_to_tcolumn_type,
        sql_type_to_ttype_desc, stored_columns_from_physical_columns,
        table_columns_from_physical_columns, truncate_managed_table_with_hooks,
    };

    fn test_managed_config() -> ManagedLakeConfig {
        ManagedLakeConfig {
            warehouse_uri: "s3://test/warehouse".to_string(),
            s3: S3StoreConfig {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "test".to_string(),
                access_key_id: "ak".to_string(),
                access_key_secret: "sk".to_string(),
                region: Some("us-east-1".to_string()),
                enable_path_style_access: Some(true),
            },
            mv_default_storage_engine: "managed_lake".to_string(),
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
    fn parse_managed_logical_type_round_trips_complex_types() {
        let cases = [
            SqlType::Array(Box::new(SqlType::BigInt)),
            SqlType::Map(Box::new(SqlType::String), Box::new(SqlType::Int)),
        ];

        for data_type in cases {
            let raw = logical_type_name(&data_type);
            let reparsed = parse_managed_logical_type(&raw).expect("reparse managed logical type");
            assert_eq!(reparsed, data_type, "raw={raw}");
        }
    }

    fn snapshot_seed() -> ManagedSnapshot {
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
        ManagedSnapshot {
            global: ManagedGlobalMeta {
                warehouse_uri: "s3://test/warehouse".to_string(),
                next_db_id: 2,
                next_table_id: 11,
                next_partition_id: 21,
                next_index_id: 31,
                next_tablet_id: 41,
                next_txn_id: 51,
            },
            databases: vec![StoredManagedDatabase {
                db_id: 1,
                name: DEFAULT_DATABASE.to_string(),
            }],
            tables: vec![StoredManagedTable {
                table_id: 10,
                db_id: 1,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 1,
                current_schema_id: 100,
                state: ManagedTableState::Active,
                kind: ManagedTableKind::Table,
            }],
            schemas: vec![StoredManagedSchema {
                schema_id: 100,
                table_id: 10,
                schema_version: 0,
                tablet_schema_pb,
            }],
            columns: vec![
                StoredManagedColumn {
                    schema_id: 100,
                    ordinal: 0,
                    column_name: "k1".to_string(),
                    logical_type: "INT".to_string(),
                    nullable: false,
                    visible: true,
                    is_key: true,
                },
                StoredManagedColumn {
                    schema_id: 100,
                    ordinal: 1,
                    column_name: "v1".to_string(),
                    logical_type: "STRING".to_string(),
                    nullable: true,
                    visible: true,
                    is_key: false,
                },
            ],
            partitions: vec![StoredManagedPartition {
                partition_id: 20,
                table_id: 10,
                name: "p0".to_string(),
                visible_version: 2,
                next_version: 3,
                state: ManagedPartitionState::Active,
            }],
            indexes: vec![StoredManagedIndex {
                index_id: 30,
                table_id: 10,
                partition_id: 20,
                index_type: "BASE".to_string(),
                state: ManagedIndexState::Active,
            }],
            tablets: vec![StoredManagedTablet {
                tablet_id: 40,
                partition_id: 20,
                index_id: 30,
                bucket_seq: 0,
                tablet_root_path: "s3://test/warehouse/db_1/table_10/partition_20".to_string(),
            }],
            txns: vec![StoredManagedTxn {
                txn_id: 50,
                table_id: 10,
                partition_id: 20,
                base_version: 1,
                commit_version: 2,
                state: ManagedTxnState::Visible,
                retry_at_ms: None,
                updated_at_ms: 0,
            }],
            erase_jobs: Vec::new(),
            materialized_views: Vec::new(),
        }
    }

    fn seed_repository_snapshot(
        provider: &SqliteMetaStoreProvider,
        snapshot: &ManagedSnapshot,
    ) -> Result<(), String> {
        let mut txn = provider
            .begin_write("seed managed ddl test repositories")
            .map_err(|e| format!("begin seed txn failed: {e}"))?;
        for database in &snapshot.databases {
            put_seed_record(
                txn.as_mut(),
                "managed",
                vec!["database".to_string(), database.db_id.to_string()],
                "managed.database",
                serde_json::json!({"db_id": database.db_id, "name": database.name}),
            )?;
        }
        for table in &snapshot.tables {
            put_seed_record(
                txn.as_mut(),
                "managed",
                vec!["table".to_string(), table.table_id.to_string()],
                "managed.table",
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
                "managed",
                vec!["schema".to_string(), schema.schema_id.to_string()],
                "managed.schema",
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
                "managed",
                vec![
                    "column".to_string(),
                    column.schema_id.to_string(),
                    column.ordinal.to_string(),
                ],
                "managed.column",
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
                "managed",
                vec!["partition".to_string(), partition.partition_id.to_string()],
                "managed.partition",
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
                "managed",
                vec!["index".to_string(), index.index_id.to_string()],
                "managed.index",
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
                "managed",
                vec!["tablet".to_string(), tablet.tablet_id.to_string()],
                "managed.tablet",
                serde_json::json!({
                    "tablet_id": tablet.tablet_id,
                    "partition_id": tablet.partition_id,
                    "index_id": tablet.index_id,
                    "bucket_seq": tablet.bucket_seq,
                    "tablet_root_path": tablet.tablet_root_path,
                }),
            )?;
        }
        for managed_txn in &snapshot.txns {
            put_seed_record(
                txn.as_mut(),
                "managed.txn",
                vec![managed_txn.txn_id.to_string()],
                "managed.txn",
                serde_json::json!({
                    "txn_id": managed_txn.txn_id,
                    "table_id": managed_txn.table_id,
                    "partition_id": managed_txn.partition_id,
                    "base_version": managed_txn.base_version,
                    "commit_version": managed_txn.commit_version,
                    "state": txn_state(managed_txn.state),
                    "retry_at_ms": managed_txn.retry_at_ms,
                    "updated_at_ms": managed_txn.updated_at_ms,
                }),
            )?;
        }
        bump_id_scope(
            txn.as_mut(),
            id_scopes::managed_db(),
            snapshot
                .databases
                .iter()
                .map(|database| database.db_id)
                .max()
                .unwrap_or(0),
        )?;
        bump_id_scope(
            txn.as_mut(),
            id_scopes::managed_table(),
            snapshot
                .tables
                .iter()
                .map(|table| table.table_id)
                .max()
                .unwrap_or(0),
        )?;
        bump_id_scope(
            txn.as_mut(),
            id_scopes::managed_partition(),
            snapshot
                .partitions
                .iter()
                .map(|partition| partition.partition_id)
                .max()
                .unwrap_or(0),
        )?;
        bump_id_scope(
            txn.as_mut(),
            id_scopes::managed_index(),
            snapshot
                .indexes
                .iter()
                .map(|index| index.index_id)
                .max()
                .unwrap_or(0),
        )?;
        bump_id_scope(
            txn.as_mut(),
            id_scopes::managed_tablet(),
            snapshot
                .tablets
                .iter()
                .map(|tablet| tablet.tablet_id)
                .max()
                .unwrap_or(0),
        )?;
        bump_id_scope(
            txn.as_mut(),
            id_scopes::managed_txn(),
            snapshot
                .txns
                .iter()
                .map(|managed_txn| managed_txn.txn_id)
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
            encode_json_payload(1, &payload).map_err(|e| e.to_string())?,
        ))
        .map_err(|e| e.to_string())
    }

    fn table_state(state: ManagedTableState) -> &'static str {
        match state {
            ManagedTableState::Creating => "CREATING",
            ManagedTableState::Active => "ACTIVE",
            ManagedTableState::Dropping => "DROPPING",
            ManagedTableState::Failed => "FAILED",
        }
    }

    fn table_kind(kind: ManagedTableKind) -> &'static str {
        match kind {
            ManagedTableKind::Table => "TABLE",
            ManagedTableKind::MaterializedView => "MATERIALIZED_VIEW",
        }
    }

    fn partition_state(state: ManagedPartitionState) -> &'static str {
        match state {
            ManagedPartitionState::Creating => "CREATING",
            ManagedPartitionState::Active => "ACTIVE",
            ManagedPartitionState::Retired => "RETIRED",
            ManagedPartitionState::Failed => "FAILED",
        }
    }

    fn index_state(state: ManagedIndexState) -> &'static str {
        match state {
            ManagedIndexState::Creating => "CREATING",
            ManagedIndexState::Active => "ACTIVE",
            ManagedIndexState::Retired => "RETIRED",
            ManagedIndexState::Failed => "FAILED",
        }
    }

    fn txn_state(state: ManagedTxnState) -> &'static str {
        match state {
            ManagedTxnState::Prepared => "PREPARED",
            ManagedTxnState::Written => "WRITTEN",
            ManagedTxnState::Visible => "VISIBLE",
            ManagedTxnState::Aborted => "ABORTED",
        }
    }

    fn seeded_state() -> (tempfile::TempDir, Arc<StandaloneState>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let snapshot = snapshot_seed();
        let metadata_provider = SqliteMetaStoreProvider::open(dir.path().join("standalone.sqlite"))
            .expect("open provider");
        seed_repository_snapshot(&metadata_provider, &snapshot).expect("seed repositories");

        let managed = ManagedLakeCatalog::rebuild(Some(test_managed_config()), snapshot)
            .expect("rebuild managed catalog");
        let runtime = managed
            .table(DEFAULT_DATABASE, "orders")
            .expect("managed runtime")
            .clone();

        let mut catalog = InMemoryCatalog::default();
        register_managed_table_in_catalog(&mut catalog, &runtime).expect("register managed table");
        let state = Arc::new(StandaloneState {
            catalog: RwLock::new(catalog),
            managed_lake: RwLock::new(managed),
            managed_lake_config: Some(test_managed_config()),
            metadata_provider: Some(Arc::new(metadata_provider)),
            ..StandaloneState::default()
        });
        (dir, state)
    }

    #[test]
    fn drop_managed_table_removes_catalog_entry_and_marks_metadata_dropping() {
        // `seeded_state` registers managed tablets into the global shard
        // registry via `register_tablet_runtime`. Serialize with other tests
        // that read/write the same registry to avoid clobbering each other.
        let _runtime_guard = crate::connector::starrocks::lake::context::lock_runtime_test_state();
        let (_dir, state) = seeded_state();

        drop_managed_table(&state, DEFAULT_DATABASE, "orders").expect("drop managed table");

        let catalog = state.catalog.read().expect("catalog read lock");
        let lookup = catalog.get(DEFAULT_DATABASE, "orders");
        assert!(
            lookup.is_err(),
            "dropped table should leave logical catalog"
        );
        drop(catalog);

        let managed = state
            .managed_lake
            .read()
            .expect("standalone managed lake read lock");
        assert!(
            !managed
                .contains_table(DEFAULT_DATABASE, "orders")
                .expect("contains table"),
            "dropped table should leave managed runtime catalog"
        );
        drop(managed);

        let read = state
            .metadata_provider
            .as_ref()
            .expect("provider")
            .begin_read()
            .expect("read");
        let persisted = state
            .managed_repo
            .load_snapshot(read.as_ref())
            .expect("reload snapshot");
        assert_eq!(persisted.tables.len(), 1);
        assert_eq!(
            persisted.tables[0].state,
            crate::meta::repository::managed_lake::ManagedTableState::Dropping
        );
        assert_eq!(
            persisted.partitions[0].state,
            crate::meta::repository::managed_lake::ManagedPartitionState::Retired
        );
        assert_eq!(
            persisted.indexes[0].state,
            crate::meta::repository::managed_lake::ManagedIndexState::Retired
        );
        let jobs = state
            .job_repo
            .list_runnable_erase_jobs(read.as_ref(), i64::MAX)
            .expect("erase jobs");
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].root_path, "s3://test/warehouse/db_1/table_10");
    }

    #[test]
    fn truncate_managed_table_replaces_active_partition_and_updates_catalog_layout() {
        // See drop_managed_table_removes_catalog_entry_and_marks_metadata_dropping
        // for why we hold the runtime-test lock here.
        let _runtime_guard = crate::connector::starrocks::lake::context::lock_runtime_test_state();
        let (_dir, state) = seeded_state();

        truncate_managed_table_with_hooks(
            &state,
            DEFAULT_DATABASE,
            "orders",
            |_, _, _| Ok(()),
            |_| Ok(()),
        )
        .expect("truncate managed table");

        let catalog = state.catalog.read().expect("catalog read lock");
        let layout = catalog
            .get_physical_layout(DEFAULT_DATABASE, "orders")
            .expect("physical layout lookup")
            .expect("managed physical layout");
        assert_eq!(layout.table_id, 10);
        assert_eq!(layout.tablets.len(), 1);
        assert_eq!(layout.tablets[0].tablet_id, 41);
        assert_eq!(layout.tablets[0].partition_id, 21);
        assert_eq!(layout.tablets[0].version, 1);
        drop(catalog);

        let managed = state
            .managed_lake
            .read()
            .expect("standalone managed lake read lock");
        let runtime = managed
            .table(DEFAULT_DATABASE, "orders")
            .expect("managed runtime after truncate");
        assert_eq!(runtime.partitions.len(), 1);
        assert_eq!(runtime.partitions[0].partition_id, 21);
        assert_eq!(runtime.partitions[0].visible_version, 1);
        assert_eq!(runtime.tablets.len(), 1);
        assert_eq!(runtime.tablets[0].tablet_id, 41);
        assert_eq!(
            runtime.tablets[0].tablet_root_path,
            "s3://test/warehouse/db_1/table_10/partition_21"
        );
        drop(managed);

        let read = state
            .metadata_provider
            .as_ref()
            .expect("provider")
            .begin_read()
            .expect("read");
        let persisted = state
            .managed_repo
            .load_snapshot(read.as_ref())
            .expect("reload snapshot");
        assert_eq!(persisted.partitions.len(), 2);
        assert_eq!(
            persisted.partitions[0].state,
            crate::meta::repository::managed_lake::ManagedPartitionState::Retired
        );
        assert_eq!(
            persisted.partitions[1].state,
            crate::meta::repository::managed_lake::ManagedPartitionState::Active
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
        let runtime = ManagedTableRuntime {
            database_name: DEFAULT_DATABASE.to_string(),
            table: StoredManagedTable {
                table_id: 10,
                db_id: 1,
                name: "orders".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 1,
                current_schema_id: 100,
                state: ManagedTableState::Active,
                kind: ManagedTableKind::Table,
            },
            tablet_schema: Default::default(),
            columns: vec![
                StoredManagedColumn {
                    schema_id: 100,
                    ordinal: 0,
                    column_name: "k1".to_string(),
                    logical_type: "INT".to_string(),
                    nullable: false,
                    visible: true,
                    is_key: true,
                },
                StoredManagedColumn {
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
    fn managed_json_type_uses_starrocks_json_primitive() {
        let column_type = sql_type_to_tcolumn_type(&SqlType::Json).expect("json column type");
        assert_eq!(column_type.type_, crate::types::TPrimitiveType::JSON);
        assert_eq!(column_type.len, Some(16));
        assert_eq!(logical_type_name(&SqlType::Json), "JSON");
        assert_eq!(
            parse_managed_logical_type("JSON").expect("logical json"),
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
            managed_physical_column("k1".to_string(), SqlType::Int, false, true, false),
            managed_physical_column("__sum_v1".to_string(), SqlType::BigInt, true, false, false),
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
            managed_physical_column("k1".to_string(), SqlType::Int, false, true, true),
            managed_physical_column("__sum_v1".to_string(), SqlType::BigInt, true, false, false),
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
        let patch_columns = vec![managed_physical_column(
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

        assert!(err.contains("managed tablet schema column count mismatch"));
    }

    #[test]
    fn create_managed_table_defaults_dup_key_first_non_float_column() {
        // Bare `CREATE TABLE t (k BIGINT, v STRING)` should default to
        // DUP KEY (k, v) (string column included, then stop) and 1 bucket.
        let defaults = resolve_managed_create_defaults(
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
    fn create_managed_table_defaults_skip_float_as_leading_key() {
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
    fn create_managed_table_defaults_short_key_length_cap() {
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
    fn create_managed_table_defaults_first_column_must_be_keyable() {
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
}
