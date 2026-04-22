use prost::Message;
use std::collections::HashSet;
use std::sync::Arc;

use crate::connector::starrocks::ObjectStoreProfile;
use crate::connector::starrocks::lake::context::{get_tablet_runtime, remove_tablet_runtime};
use crate::connector::starrocks::lake::create_lake_tablet_from_req;
use crate::formats::starrocks::metadata::load_tablet_snapshot;
use crate::sql::parser::ast::{ObjectName, SqlType, TableColumnDef, TableKeyDesc, TableKeyKind};

use super::super::engine::local::normalize_identifier;
use super::super::engine::{StandaloneState, StatementResult};
use super::catalog::{ManagedLakeCatalog, ManagedTableRuntime, register_managed_table_in_catalog};
use super::config::ManagedLakeConfig;
use super::store::{
    ManagedIndexState, ManagedPartitionState, ManagedSnapshot, ManagedTableState, ManagedTxnState,
    StageManagedTruncateRequest, StagedManagedTruncate, StoredManagedColumn, StoredManagedDatabase,
    StoredManagedIndex, StoredManagedPartition, StoredManagedSchema, StoredManagedTable,
    StoredManagedTablet, StoredManagedTxn,
};

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
    let key_desc = key_desc.ok_or_else(|| {
        format!(
            "managed standalone CREATE TABLE requires an explicit key description: {}.{}",
            resolved.database, resolved.table
        )
    })?;
    let bucket_num = i64::from(bucket_count.ok_or_else(|| {
        format!(
            "managed standalone CREATE TABLE requires DISTRIBUTED BY ... BUCKETS <n>: {}.{}",
            resolved.database, resolved.table
        )
    })?);
    if bucket_num <= 0 {
        return Err(format!(
            "managed standalone CREATE TABLE requires BUCKETS > 0: {}.{}",
            resolved.database, resolved.table
        ));
    }
    if key_desc.kind == TableKeyKind::Aggregate {
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
    let table_id = alloc_id(&mut snapshot.global.next_table_id);
    let schema_id = table_id;
    let partition_id = alloc_id(&mut snapshot.global.next_partition_id);
    let index_id = alloc_id(&mut snapshot.global.next_index_id);

    let request_schema = build_tablet_schema(columns, key_desc)?;
    let object_store_profile = ObjectStoreProfile::from_s3_store_config(&managed_config.s3)?;
    let mut tablets = Vec::new();
    for bucket_seq in 0..bucket_num {
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
        keys_type: keys_type_name(key_desc.kind).to_string(),
        bucket_num,
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
        thrift_columns.push(crate::descriptors::TColumn {
            column_name: normalized,
            column_type: Some(sql_type_to_tcolumn_type(&column.data_type)?),
            aggregation_type: None,
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
            type_desc: None,
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
        id: Some(1),
        sort_key_idxes: Some(key_indices.clone()),
        sort_key_unique_ids: Some(key_indices),
        schema_version: Some(0),
        compression_type: Some(crate::types::TCompressionType::LZ4_FRAME),
        compression_level: None,
    })
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
        SqlType::Array(_) => {
            return Err("managed standalone CREATE TABLE does not support ARRAY yet".to_string());
        }
        SqlType::Binary => {
            return Err("managed standalone CREATE TABLE does not support BINARY yet".to_string());
        }
        SqlType::Map(_, _) => {
            return Err("managed standalone CREATE TABLE does not support MAP yet".to_string());
        }
        SqlType::Struct(_) => {
            return Err("managed standalone CREATE TABLE does not support STRUCT yet".to_string());
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
    use crate::standalone::engine::local::{DEFAULT_DATABASE, InMemoryCatalog};
    use crate::standalone::lake::store::{
        ManagedGlobalMeta, ManagedIndexState, ManagedPartitionState, ManagedSnapshot,
        ManagedTableState, ManagedTxnState, SqliteMetadataStore, StoredManagedColumn,
        StoredManagedDatabase, StoredManagedIndex, StoredManagedPartition, StoredManagedSchema,
        StoredManagedTable, StoredManagedTablet, StoredManagedTxn,
    };
    use crate::standalone::lake::{
        ManagedLakeCatalog, ManagedLakeConfig, register_managed_table_in_catalog,
    };

    use super::{build_tablet_schema, drop_managed_table, truncate_managed_table_with_hooks};

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
}
