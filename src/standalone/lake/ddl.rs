use prost::Message;
use std::collections::HashSet;
use std::sync::Arc;

use crate::connector::starrocks::ObjectStoreProfile;
use crate::connector::starrocks::lake::context::{get_tablet_runtime, remove_tablet_runtime};
use crate::connector::starrocks::lake::create_lake_tablet_from_req;
use crate::formats::starrocks::metadata::load_tablet_snapshot;
use crate::sql::parser::ast::{ObjectName, SqlType, TableColumnDef, TableKeyDesc, TableKeyKind};

use super::super::engine::catalog::normalize_identifier;
use super::super::engine::{StandaloneState, StatementResult};
use super::catalog::{ManagedLakeCatalog, ManagedTableRuntime, register_managed_table_in_catalog};
use super::config::ManagedLakeConfig;
use super::store::{
    ManagedIndexState, ManagedPartitionState, ManagedSnapshot, ManagedTableState, ManagedTxnState,
    StageManagedTruncateRequest, StagedManagedTruncate, StoredManagedColumn, StoredManagedDatabase,
    StoredManagedIndex, StoredManagedPartition, StoredManagedSchema, StoredManagedTable,
    StoredManagedTablet, StoredManagedTxn,
};

/// Default bucket count when the user omits `DISTRIBUTED BY ... BUCKETS <n>`.
const DEFAULT_MANAGED_BUCKET_COUNT: u32 = 1;
/// Mirrors StarRocks `SHORTKEY_MAX_COLUMN_COUNT`: at most 3 columns in the short-key.
const SHORT_KEY_MAX_COLUMN_COUNT: usize = 3;
/// Mirrors StarRocks `SHORTKEY_MAXSIZE_BYTES`: at most 36 bytes in the short-key.
const SHORT_KEY_MAX_SIZE_BYTES: usize = 36;

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
    if defaults.key_desc.kind == TableKeyKind::Aggregate {
        return Err(
            "managed standalone CREATE TABLE does not support AGGREGATE KEY yet".to_string(),
        );
    }
    let metadata_store = state.metadata_store.as_ref().ok_or_else(|| {
        "managed standalone CREATE TABLE requires sqlite metadata store".to_string()
    })?;

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

    let mut snapshot = guard.snapshot.clone();
    initialize_global_meta_if_needed(&mut snapshot, &managed_config);
    let database = find_or_create_database(&mut snapshot, &resolved.database);
    // DROP TABLE leaves the old row in the `tables` rel with state=DROPPING so
    // an async erase worker can clean it up. The table is considered non-Active
    // (so `contains_table()` returns false above), but the row still occupies
    // `(db_id, name)`. Without removing it here the subsequent
    // `replace_managed_snapshot` would hit the UNIQUE constraint when it
    // re-inserts both the old and the new row. Reclaim the name synchronously
    // by dropping the DROPPING entry and its per-schema/column rows, since we
    // don't rely on the async erase worker in standalone mode.
    reclaim_dropping_table_for_reuse(&mut snapshot, database.db_id, &resolved.table)?;
    let table_id = alloc_id(&mut snapshot.global.next_table_id);
    let schema_id = table_id;
    let partition_id = alloc_id(&mut snapshot.global.next_partition_id);
    let index_id = alloc_id(&mut snapshot.global.next_index_id);

    let request_schema = build_tablet_schema(columns, &defaults.key_desc, schema_id)?;
    let object_store_profile = ObjectStoreProfile::from_s3_store_config(&managed_config.s3)?;
    let mut tablets = Vec::new();
    for bucket_seq in 0..defaults.bucket_num {
        let tablet_id = alloc_id(&mut snapshot.global.next_tablet_id);
        let tablet_root_path =
            managed_config.tablet_root_path(database.db_id, table_id, partition_id);
        let request = crate::agent_service::TCreateTabletReq {
            tablet_id,
            tablet_schema: request_schema.clone(),
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
        };
        create_lake_tablet_from_req(&request, &tablet_root_path, Some(managed_config.s3.clone()))?;
        let runtime_schema = get_tablet_runtime(tablet_id)?.schema;
        let loaded =
            load_tablet_snapshot(tablet_id, 1, &tablet_root_path, Some(&object_store_profile))?;
        if loaded.tablet_schema != runtime_schema {
            return Err(format!(
                "managed tablet schema mismatch after bootstrap: tablet_id={tablet_id}"
            ));
        }
        tablets.push(StoredManagedTablet {
            tablet_id,
            partition_id,
            index_id,
            bucket_seq,
            tablet_root_path,
        });
    }

    snapshot.tables.push(StoredManagedTable {
        table_id,
        db_id: database.db_id,
        name: resolved.table.clone(),
        keys_type: keys_type_name(defaults.key_desc.kind).to_string(),
        bucket_num: defaults.bucket_num,
        current_schema_id: schema_id,
        state: ManagedTableState::Active,
    });
    snapshot.schemas.push(StoredManagedSchema {
        schema_id,
        table_id,
        schema_version: 0,
        tablet_schema_pb: get_tablet_runtime(tablets[0].tablet_id)?
            .schema
            .encode_to_vec(),
    });
    snapshot
        .columns
        .extend(columns.iter().enumerate().map(|(ordinal, column)| {
            StoredManagedColumn {
                schema_id,
                ordinal: ordinal as i64,
                column_name: normalize_identifier(&column.name)
                    .unwrap_or_else(|_| column.name.to_ascii_lowercase()),
                logical_type: logical_type_name(&column.data_type),
                nullable: column.nullable,
            }
        }));
    snapshot.partitions.push(StoredManagedPartition {
        partition_id,
        table_id,
        name: "p0".to_string(),
        visible_version: 1,
        next_version: 2,
        state: ManagedPartitionState::Active,
    });
    snapshot.indexes.push(StoredManagedIndex {
        index_id,
        table_id,
        partition_id,
        index_type: "BASE".to_string(),
        state: ManagedIndexState::Active,
    });
    snapshot.tablets.extend(tablets);
    let txn_id = alloc_id(&mut snapshot.global.next_txn_id);
    snapshot.txns.push(StoredManagedTxn {
        txn_id,
        table_id,
        partition_id,
        base_version: 0,
        commit_version: 1,
        state: ManagedTxnState::Visible,
        retry_at_ms: None,
        updated_at_ms: 0,
    });

    let rebuilt = ManagedLakeCatalog::rebuild(Some(managed_config), snapshot.clone())?;
    metadata_store.replace_managed_snapshot(&snapshot)?;
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
            | SqlType::Binary
            | SqlType::Array(_)
            | SqlType::Map(_, _)
            | SqlType::Struct(_)
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
        SqlType::Float => 4,
        SqlType::Double => 8,
        SqlType::Array(_) | SqlType::Map(_, _) | SqlType::Struct(_) => SHORT_KEY_MAX_SIZE_BYTES + 1,
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
    let runtime = {
        let managed = state
            .managed_lake
            .read()
            .expect("standalone managed lake read lock");
        managed.table(database_name, table_name)?.clone()
    };
    let managed_config = state
        .managed_lake_config
        .as_ref()
        .ok_or_else(|| "standalone managed lake config is missing".to_string())?;
    let metadata_store = state.metadata_store.as_ref().ok_or_else(|| {
        "managed standalone DROP TABLE requires sqlite metadata store".to_string()
    })?;
    let table_root_path = managed_table_root_path(
        &managed_config.warehouse_uri,
        runtime.table.db_id,
        runtime.table.table_id,
    );
    metadata_store.drop_managed_table(runtime.table.table_id, &table_root_path)?;
    for tablet in &runtime.tablets {
        remove_tablet_runtime(tablet.tablet_id)?;
    }

    let snapshot = metadata_store.load_snapshot()?.managed;
    let rebuilt = ManagedLakeCatalog::rebuild(state.managed_lake_config.clone(), snapshot)?;
    {
        let mut managed = state
            .managed_lake
            .write()
            .expect("standalone managed lake write lock");
        *managed = rebuilt;
    }
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
    let metadata_store = state.metadata_store.as_ref().ok_or_else(|| {
        "managed standalone DROP DATABASE requires sqlite metadata store".to_string()
    })?;

    let normalized = normalize_identifier(database_name)?;
    let mut guard = state
        .managed_lake
        .write()
        .expect("standalone managed lake write lock");
    let mut snapshot = guard.snapshot.clone();
    let before = snapshot.databases.len();
    snapshot
        .databases
        .retain(|db| normalize_identifier(&db.name).ok().as_deref() != Some(&normalized));
    if snapshot.databases.len() == before {
        return Ok(());
    }
    metadata_store.replace_managed_snapshot(&snapshot)?;
    let rebuilt = ManagedLakeCatalog::rebuild(Some(managed_config), snapshot)?;
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
    let runtime = {
        let managed = state
            .managed_lake
            .read()
            .expect("standalone managed lake read lock");
        managed.table(database_name, table_name)?.clone()
    };
    let managed_config = state
        .managed_lake_config
        .as_ref()
        .ok_or_else(|| "standalone managed lake config is missing".to_string())?;
    let metadata_store = state.metadata_store.as_ref().ok_or_else(|| {
        "managed standalone TRUNCATE TABLE requires sqlite metadata store".to_string()
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
    let staged = metadata_store.stage_truncate_partition(StageManagedTruncateRequest {
        table_id: runtime.table.table_id,
        db_id: runtime.table.db_id,
        bucket_num: runtime.table.bucket_num,
        partition_name: active_partition.name.clone(),
        warehouse_uri: managed_config.warehouse_uri.clone(),
    })?;
    if let Err(err) = bootstrap(&runtime, managed_config, &staged) {
        cleanup_staged_truncate(metadata_store, &staged)?;
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
    if let Err(err) = metadata_store.activate_truncate_partition(
        runtime.table.table_id,
        active_partition.partition_id,
        staged.partition_id,
        staged.index_id,
        &retired_root_path,
    ) {
        cleanup_staged_truncate(metadata_store, &staged)?;
        return Err(format!(
            "activate truncate partition failed for {}.{}: {err}",
            database_name, table_name
        ));
    }
    for tablet in &runtime.tablets {
        remove_tablet_runtime(tablet.tablet_id)?;
    }

    let rebuilt_snapshot = metadata_store.load_snapshot()?.managed;
    let rebuilt = ManagedLakeCatalog::rebuild(state.managed_lake_config.clone(), rebuilt_snapshot)?;
    refresh_runtimes(&rebuilt)?;
    let updated_runtime = rebuilt.table(database_name, table_name)?.clone();
    {
        let mut managed = state
            .managed_lake
            .write()
            .expect("standalone managed lake write lock");
        *managed = rebuilt;
    }
    let mut catalog = state
        .catalog
        .write()
        .expect("standalone catalog write lock");
    register_managed_table_in_catalog(&mut catalog, &updated_runtime)?;
    Ok(StatementResult::Ok)
}

fn initialize_global_meta_if_needed(snapshot: &mut ManagedSnapshot, config: &ManagedLakeConfig) {
    if snapshot.global == Default::default() {
        snapshot.global.warehouse_uri = config.warehouse_uri.clone();
        snapshot.global.next_db_id = 1;
        snapshot.global.next_table_id = 1;
        snapshot.global.next_partition_id = 1;
        snapshot.global.next_index_id = 1;
        snapshot.global.next_tablet_id = 1;
        snapshot.global.next_txn_id = 1;
    }
}

/// If the snapshot has a `tables` row with `state=DROPPING` at
/// `(db_id, name)`, drop it (and its associated schema / column / partition /
/// index / tablet / erase_job / txn rows) so a subsequent ACTIVE insert doesn't
/// trip the `UNIQUE(db_id, name)` constraint during snapshot replay.
fn reclaim_dropping_table_for_reuse(
    snapshot: &mut ManagedSnapshot,
    db_id: i64,
    table_name: &str,
) -> Result<(), String> {
    let target = normalize_identifier(table_name)?;
    let stale_ids: Vec<i64> = snapshot
        .tables
        .iter()
        .filter(|tbl| {
            tbl.db_id == db_id
                && tbl.state == ManagedTableState::Dropping
                && normalize_identifier(&tbl.name).ok().as_deref() == Some(target.as_str())
        })
        .map(|tbl| tbl.table_id)
        .collect();
    if stale_ids.is_empty() {
        return Ok(());
    }
    let stale_set: HashSet<i64> = stale_ids.iter().copied().collect();
    snapshot.tables.retain(|tbl| !stale_set.contains(&tbl.table_id));
    let stale_partition_ids: HashSet<i64> = snapshot
        .partitions
        .iter()
        .filter(|p| stale_set.contains(&p.table_id))
        .map(|p| p.partition_id)
        .collect();
    snapshot
        .partitions
        .retain(|p| !stale_set.contains(&p.table_id));
    snapshot
        .indexes
        .retain(|i| !stale_set.contains(&i.table_id));
    snapshot
        .tablets
        .retain(|t| !stale_partition_ids.contains(&t.partition_id));
    snapshot.txns.retain(|t| !stale_set.contains(&t.table_id));
    let stale_schema_ids: HashSet<i64> = snapshot
        .schemas
        .iter()
        .filter(|s| stale_set.contains(&s.table_id))
        .map(|s| s.schema_id)
        .collect();
    snapshot
        .schemas
        .retain(|s| !stale_set.contains(&s.table_id));
    snapshot
        .columns
        .retain(|c| !stale_schema_ids.contains(&c.schema_id));
    snapshot
        .erase_jobs
        .retain(|j| !stale_set.contains(&j.table_id));
    Ok(())
}

fn find_or_create_database(
    snapshot: &mut ManagedSnapshot,
    database_name: &str,
) -> StoredManagedDatabase {
    if let Some(found) = snapshot
        .databases
        .iter()
        .find(|database| database.name == database_name)
        .cloned()
    {
        return found;
    }
    let db = StoredManagedDatabase {
        db_id: alloc_id(&mut snapshot.global.next_db_id),
        name: database_name.to_string(),
    };
    snapshot.databases.push(db.clone());
    db
}

fn alloc_id(next_id: &mut i64) -> i64 {
    if *next_id <= 0 {
        *next_id = 1;
    }
    let id = *next_id;
    *next_id += 1;
    id
}

fn cleanup_staged_truncate(
    metadata_store: &super::store::SqliteMetadataStore,
    staged: &StagedManagedTruncate,
) -> Result<(), String> {
    for tablet_id in &staged.tablet_ids {
        let _ = remove_tablet_runtime(*tablet_id);
    }
    metadata_store.delete_creating_partition(staged.partition_id)
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

fn request_schema_from_runtime(
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
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let key_columns = runtime
        .tablet_schema
        .column
        .iter()
        .filter(|column| column.visible != Some(false) && column.is_key.unwrap_or(false))
        .map(|column| {
            column
                .name
                .clone()
                .ok_or_else(|| "managed tablet schema key column missing name".to_string())
        })
        .collect::<Result<Vec<_>, String>>()?;
    build_tablet_schema(
        &columns,
        &TableKeyDesc {
            kind: parse_keys_type(&runtime.table.keys_type)?,
            columns: key_columns,
        },
        runtime.table.current_schema_id,
    )
}

fn build_create_tablet_request(
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

fn build_tablet_schema(
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
        // StarRocks value-column aggregation semantics by keys_type:
        //   DUP_KEYS     -> aggregation_type is unused (column is just stored)
        //   UNIQUE_KEYS  -> value columns default to REPLACE
        //   AGG_KEYS     -> value columns need an explicit per-column aggregation,
        //                    which we do not parse yet; fail earlier in the caller.
        let aggregation_type = if is_key {
            None
        } else {
            match key_desc.kind {
                TableKeyKind::Duplicate => None,
                TableKeyKind::Unique | TableKeyKind::Primary => {
                    Some(crate::types::TAggregationType::REPLACE)
                }
                TableKeyKind::Aggregate => None,
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
        | SqlType::Struct(_) => None,
    }
}

fn logical_type_name(data_type: &SqlType) -> String {
    match data_type {
        SqlType::TinyInt => "TINYINT".to_string(),
        SqlType::SmallInt => "SMALLINT".to_string(),
        SqlType::Int => "INT".to_string(),
        SqlType::BigInt => "BIGINT".to_string(),
        SqlType::LargeInt => "LARGEINT".to_string(),
        SqlType::Float => "FLOAT".to_string(),
        SqlType::Double => "DOUBLE".to_string(),
        SqlType::String => "STRING".to_string(),
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

fn keys_type_name(kind: TableKeyKind) -> &'static str {
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
        "BOOLEAN" => Ok(SqlType::Boolean),
        "DATE" => Ok(SqlType::Date),
        "DATETIME" => Ok(SqlType::DateTime),
        "TIME" => Ok(SqlType::Time),
        _ => parse_decimal_logical_type(&normalized),
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use prost::Message;

    use crate::runtime::starlet_shard_registry::S3StoreConfig;
    use crate::sql::parser::ast::{SqlType, TableColumnDef, TableKeyDesc, TableKeyKind};
    use crate::standalone::engine::StandaloneState;
    use crate::standalone::engine::catalog::{DEFAULT_DATABASE, InMemoryCatalog};
    use crate::standalone::lake::store::{
        ManagedGlobalMeta, ManagedIndexState, ManagedPartitionState, ManagedSnapshot,
        ManagedTableState, ManagedTxnState, SqliteMetadataStore, StoredManagedColumn,
        StoredManagedDatabase, StoredManagedIndex, StoredManagedPartition, StoredManagedSchema,
        StoredManagedTable, StoredManagedTablet, StoredManagedTxn,
    };
    use crate::standalone::lake::{
        ManagedLakeCatalog, ManagedLakeConfig, register_managed_table_in_catalog,
    };

    use super::{
        build_tablet_schema, choose_default_dup_key_columns, drop_managed_table,
        resolve_managed_create_defaults, truncate_managed_table_with_hooks,
    };

    fn test_managed_config() -> ManagedLakeConfig {
        ManagedLakeConfig {
            warehouse_uri: "s3://test/warehouse".to_string(),
            s3: S3StoreConfig {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "test".to_string(),
                root: "warehouse".to_string(),
                access_key_id: "ak".to_string(),
                access_key_secret: "sk".to_string(),
                region: Some("us-east-1".to_string()),
                enable_path_style_access: Some(true),
            },
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
                },
                TableColumnDef {
                    name: "v1".to_string(),
                    data_type: SqlType::String,
                    nullable: true,
                    aggregation: None,
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
                },
                StoredManagedColumn {
                    schema_id: 100,
                    ordinal: 1,
                    column_name: "v1".to_string(),
                    logical_type: "STRING".to_string(),
                    nullable: true,
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
        }
    }

    fn seeded_state() -> (tempfile::TempDir, Arc<StandaloneState>) {
        let dir = tempfile::tempdir().expect("tempdir");
        let metadata_store =
            SqliteMetadataStore::open(dir.path().join("standalone.sqlite")).expect("open store");
        let snapshot = snapshot_seed();
        metadata_store
            .replace_managed_snapshot(&snapshot)
            .expect("persist managed snapshot");

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
            metadata_store: Some(metadata_store),
            ..StandaloneState::default()
        });
        (dir, state)
    }

    #[test]
    fn drop_managed_table_removes_catalog_entry_and_marks_metadata_dropping() {
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

        let persisted = state
            .metadata_store
            .as_ref()
            .expect("metadata store")
            .load_snapshot()
            .expect("reload snapshot");
        assert_eq!(persisted.managed.tables.len(), 1);
        assert_eq!(
            persisted.managed.tables[0].state,
            ManagedTableState::Dropping
        );
        assert_eq!(
            persisted.managed.partitions[0].state,
            ManagedPartitionState::Retired
        );
        assert_eq!(
            persisted.managed.indexes[0].state,
            ManagedIndexState::Retired
        );
        assert_eq!(persisted.managed.erase_jobs.len(), 1);
        assert_eq!(
            persisted.managed.erase_jobs[0].root_path,
            "s3://test/warehouse/db_1/table_10"
        );
    }

    #[test]
    fn truncate_managed_table_replaces_active_partition_and_updates_catalog_layout() {
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

        let persisted = state
            .metadata_store
            .as_ref()
            .expect("metadata store")
            .load_snapshot()
            .expect("reload snapshot");
        assert_eq!(persisted.managed.partitions.len(), 2);
        assert_eq!(
            persisted.managed.partitions[0].state,
            ManagedPartitionState::Retired
        );
        assert_eq!(
            persisted.managed.partitions[1].state,
            ManagedPartitionState::Active
        );
        assert_eq!(persisted.managed.erase_jobs.len(), 1);
        assert_eq!(persisted.managed.erase_jobs[0].partition_id, Some(20));
        assert_eq!(
            persisted.managed.erase_jobs[0].root_path,
            "s3://test/warehouse/db_1/table_10/partition_20"
        );
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
                },
                TableColumnDef {
                    name: "v".to_string(),
                    data_type: SqlType::String,
                    nullable: true,
                    aggregation: None,
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
            },
            TableColumnDef {
                name: "k".to_string(),
                data_type: SqlType::Int,
                nullable: false,
                aggregation: None,
            },
            TableColumnDef {
                name: "v".to_string(),
                data_type: SqlType::String,
                nullable: true,
                aggregation: None,
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
            },
            TableColumnDef {
                name: "k2".to_string(),
                data_type: SqlType::BigInt,
                nullable: false,
                aggregation: None,
            },
            TableColumnDef {
                name: "k3".to_string(),
                data_type: SqlType::BigInt,
                nullable: false,
                aggregation: None,
            },
            TableColumnDef {
                name: "k4".to_string(),
                data_type: SqlType::BigInt,
                nullable: false,
                aggregation: None,
            },
            TableColumnDef {
                name: "k5".to_string(),
                data_type: SqlType::BigInt,
                nullable: false,
                aggregation: None,
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
            },
            TableColumnDef {
                name: "v".to_string(),
                data_type: SqlType::Int,
                nullable: false,
                aggregation: None,
            },
        ])
        .expect_err("double first column should fail");

        assert!(err.contains("first column `d` cannot be a key column"));
    }
}
