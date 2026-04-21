use prost::Message;
use std::collections::HashSet;

use crate::connector::starrocks::ObjectStoreProfile;
use crate::connector::starrocks::lake::context::get_tablet_runtime;
use crate::connector::starrocks::lake::create_lake_tablet_from_req;
use crate::formats::starrocks::metadata::load_tablet_snapshot;
use crate::sql::parser::ast::{ObjectName, SqlType, TableColumnDef, TableKeyDesc, TableKeyKind};

use super::catalog::normalize_identifier;
use super::engine::{StandaloneState, StatementResult};
use super::lake_recovery::{
    ManagedLakeCatalog, ManagedLakeConfig, register_managed_table_in_catalog,
};
use super::store::{
    ManagedIndexState, ManagedPartitionState, ManagedSnapshot, ManagedTableState, ManagedTxnState,
    StoredManagedColumn, StoredManagedDatabase, StoredManagedIndex, StoredManagedPartition,
    StoredManagedSchema, StoredManagedTable, StoredManagedTablet, StoredManagedTxn,
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
        let tablet_root_path = managed_config.tablet_root_path(database.db_id, table_id, tablet_id);
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
