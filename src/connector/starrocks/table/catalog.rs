use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::Fields;
use arrow::datatypes::{DataType, Field, TimeUnit};
use prost::Message;

use crate::common::decimal::{LEGACY_DECIMALV2_PRECISION, LEGACY_DECIMALV2_SCALE};
use crate::common::largeint::LARGEINT_BYTE_WIDTH;
use crate::connector::starrocks::ObjectStoreProfile;
use crate::connector::starrocks::lake::context::{
    TabletWriteContext, get_tablet_runtime, register_tablet_runtime, remove_tablet_runtime,
};
use crate::formats::starrocks::metadata::load_tablet_snapshot;
use crate::service::grpc_client::proto::starrocks::{ColumnPb, TabletSchemaPb};

use super::model::{
    StarRocksGlobalMeta, StarRocksIndexState, StarRocksPartitionState, StarRocksTableSnapshot,
    StarRocksTableState, StoredStarRocksColumn, StoredStarRocksDatabase, StoredStarRocksIndex,
    StoredStarRocksPartition, StoredStarRocksTable, StoredStarRocksTablet,
};
use crate::connector::starrocks::table::config::StarRocksTableConfig;
use crate::engine::catalog::{
    ColumnDef, InMemoryCatalog, PhysicalTableLayout, ScanSource, StarRocksTabletRef, TableDef,
    normalize_identifier,
};

#[derive(Clone, Debug, Default)]
pub(crate) struct StarRocksTableCatalog {
    pub(crate) config: Option<StarRocksTableConfig>,
    pub(crate) snapshot: StarRocksTableSnapshot,
    tables_by_name: HashMap<(String, String), StarRocksTableRuntime>,
}

impl StarRocksTableCatalog {
    pub(crate) fn empty(config: Option<StarRocksTableConfig>) -> Self {
        Self {
            config,
            snapshot: StarRocksTableSnapshot::default(),
            tables_by_name: HashMap::new(),
        }
    }

    pub(crate) fn table(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Result<&StarRocksTableRuntime, String> {
        let db = normalize_identifier(database_name)?;
        let table = normalize_identifier(table_name)?;
        self.tables_by_name
            .get(&(db.clone(), table.clone()))
            .ok_or_else(|| format!("unknown StarRocks table: {db}.{table}"))
    }

    pub(crate) fn contains_table(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Result<bool, String> {
        let db = normalize_identifier(database_name)?;
        let table = normalize_identifier(table_name)?;
        Ok(self.tables_by_name.contains_key(&(db, table)))
    }

    /// Return the original (already-normalized) table names of all StarRocks
    /// tables registered under `database_name`. Empty if the database has no
    /// StarRocks tables or does not exist.
    pub(crate) fn list_tables_in_database(
        &self,
        database_name: &str,
    ) -> Result<Vec<String>, String> {
        let db = normalize_identifier(database_name)?;
        Ok(self
            .tables_by_name
            .keys()
            .filter(|(d, _)| d == &db)
            .map(|(_, t)| t.clone())
            .collect())
    }

    /// Bump the visible_version/next_version for `partition_id` in both the
    /// raw snapshot and the cached table runtime. Returns the table id that
    /// owns the partition so the caller can re-register the logical layout.
    pub(crate) fn advance_partition_version(
        &mut self,
        partition_id: i64,
        new_visible_version: i64,
    ) -> Result<i64, String> {
        let mut table_id = None;
        for partition in self.snapshot.partitions.iter_mut() {
            if partition.partition_id == partition_id {
                if new_visible_version <= partition.visible_version {
                    return Err(format!(
                        "refuse to advance partition {partition_id} from version {} to {}",
                        partition.visible_version, new_visible_version
                    ));
                }
                partition.visible_version = new_visible_version;
                partition.next_version = new_visible_version + 1;
                table_id = Some(partition.table_id);
                break;
            }
        }
        let table_id = table_id
            .ok_or_else(|| format!("StarRocks snapshot is missing partition {partition_id}"))?;
        for runtime in self.tables_by_name.values_mut() {
            if runtime.table.table_id != table_id {
                continue;
            }
            for partition in runtime.partitions.iter_mut() {
                if partition.partition_id == partition_id {
                    partition.visible_version = new_visible_version;
                    partition.next_version = new_visible_version + 1;
                }
            }
        }
        Ok(table_id)
    }

    pub(crate) fn runtime_by_table_id(&self, table_id: i64) -> Option<&StarRocksTableRuntime> {
        self.tables_by_name
            .values()
            .find(|runtime| runtime.table.table_id == table_id)
    }

    pub(crate) fn rebuild(
        config: Option<StarRocksTableConfig>,
        snapshot: StarRocksTableSnapshot,
    ) -> Result<Self, String> {
        if snapshot_is_empty(&snapshot) {
            return Ok(Self::empty(config));
        }

        let Some(config) = config else {
            return Err(
                "StarRocks table metadata exists but standalone StarRocks table config is missing"
                    .to_string(),
            );
        };
        if snapshot.global.warehouse_uri.trim() != config.warehouse_uri {
            return Err(format!(
                "StarRocks table warehouse mismatch: snapshot={} config={}",
                snapshot.global.warehouse_uri, config.warehouse_uri
            ));
        }

        let mut databases_by_id = HashMap::new();
        for database in &snapshot.databases {
            databases_by_id.insert(database.db_id, database.name.clone());
        }

        let mut schemas_by_id = HashMap::new();
        for schema in &snapshot.schemas {
            let decoded =
                TabletSchemaPb::decode(schema.tablet_schema_pb.as_slice()).map_err(|e| {
                    format!(
                        "decode StarRocks tablet_schema_pb failed for schema_id={}: {e}",
                        schema.schema_id
                    )
                })?;
            schemas_by_id.insert(schema.schema_id, (schema.clone(), decoded));
        }

        let mut columns_by_schema = HashMap::<i64, Vec<StoredStarRocksColumn>>::new();
        for column in &snapshot.columns {
            columns_by_schema
                .entry(column.schema_id)
                .or_default()
                .push(column.clone());
        }
        for columns in columns_by_schema.values_mut() {
            columns.sort_by_key(|column| column.ordinal);
        }

        let mut partitions_by_table = HashMap::<i64, Vec<StoredStarRocksPartition>>::new();
        for partition in &snapshot.partitions {
            partitions_by_table
                .entry(partition.table_id)
                .or_default()
                .push(partition.clone());
        }
        for partitions in partitions_by_table.values_mut() {
            partitions.sort_by_key(|partition| partition.partition_id);
        }

        let mut indexes_by_table = HashMap::<i64, Vec<StoredStarRocksIndex>>::new();
        for index in &snapshot.indexes {
            indexes_by_table
                .entry(index.table_id)
                .or_default()
                .push(index.clone());
        }
        for indexes in indexes_by_table.values_mut() {
            indexes.sort_by_key(|index| index.index_id);
        }

        let mut tablets_by_table = HashMap::<i64, Vec<StoredStarRocksTablet>>::new();
        let mut index_to_table = HashMap::<i64, i64>::new();
        for index in &snapshot.indexes {
            index_to_table.insert(index.index_id, index.table_id);
        }
        for tablet in &snapshot.tablets {
            let table_id = index_to_table
                .get(&tablet.index_id)
                .copied()
                .ok_or_else(|| {
                    format!(
                        "StarRocks tablet {} references unknown index_id={}",
                        tablet.tablet_id, tablet.index_id
                    )
                })?;
            tablets_by_table
                .entry(table_id)
                .or_default()
                .push(tablet.clone());
        }
        for tablets in tablets_by_table.values_mut() {
            tablets.sort_by_key(|tablet| (tablet.bucket_seq, tablet.tablet_id));
        }

        let mut tables_by_name = HashMap::new();
        for table in &snapshot.tables {
            if table.state != StarRocksTableState::Active {
                continue;
            }
            let database_name = databases_by_id.get(&table.db_id).cloned().ok_or_else(|| {
                format!(
                    "StarRocks table {} references unknown db_id={}",
                    table.table_id, table.db_id
                )
            })?;
            let (_, tablet_schema) = schemas_by_id
                .get(&table.current_schema_id)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "StarRocks table {} references unknown current_schema_id={}",
                        table.table_id, table.current_schema_id
                    )
                })?;
            let key = (
                normalize_identifier(&database_name)?,
                normalize_identifier(&table.name)?,
            );
            let partitions = partitions_by_table
                .remove(&table.table_id)
                .unwrap_or_default()
                .into_iter()
                .filter(|partition| {
                    matches!(
                        partition.state,
                        StarRocksPartitionState::Active | StarRocksPartitionState::Creating
                    )
                })
                .collect::<Vec<_>>();
            let live_partition_ids = partitions
                .iter()
                .map(|partition| partition.partition_id)
                .collect::<HashSet<_>>();
            let indexes = indexes_by_table
                .remove(&table.table_id)
                .unwrap_or_default()
                .into_iter()
                .filter(|index| {
                    matches!(
                        index.state,
                        StarRocksIndexState::Active | StarRocksIndexState::Creating
                    ) && live_partition_ids.contains(&index.partition_id)
                })
                .collect::<Vec<_>>();
            let live_index_ids = indexes
                .iter()
                .map(|index| index.index_id)
                .collect::<HashSet<_>>();
            tables_by_name.insert(
                key,
                StarRocksTableRuntime {
                    database_name,
                    table: table.clone(),
                    tablet_schema,
                    columns: columns_by_schema
                        .remove(&table.current_schema_id)
                        .unwrap_or_default(),
                    partitions,
                    indexes,
                    tablets: tablets_by_table
                        .remove(&table.table_id)
                        .unwrap_or_default()
                        .into_iter()
                        .filter(|tablet| {
                            live_partition_ids.contains(&tablet.partition_id)
                                && live_index_ids.contains(&tablet.index_id)
                        })
                        .collect(),
                },
            );
        }

        Ok(Self {
            config: Some(config),
            snapshot,
            tables_by_name,
        })
    }

    pub(crate) fn rebuild_from_repository(
        config: Option<StarRocksTableConfig>,
        snapshot: crate::meta::repository::starrocks_table::StarRocksTableSnapshot,
    ) -> Result<Self, String> {
        let legacy_snapshot = repository_snapshot_for_runtime(config.as_ref(), snapshot);
        Self::rebuild(config, legacy_snapshot)
    }

    pub(crate) fn re_register_active_tablet_runtimes(&self) -> Result<(), String> {
        let Some(config) = self.config.as_ref() else {
            if snapshot_is_empty(&self.snapshot) {
                return Ok(());
            }
            return Err(
                "StarRocks table metadata exists but standalone StarRocks table config is missing"
                    .to_string(),
            );
        };
        let object_store_profile = ObjectStoreProfile::from_s3_store_config(&config.s3)?;

        for runtime in self.tables_by_name.values() {
            let active_partition_ids = runtime
                .partitions
                .iter()
                .filter(|partition| partition.state == StarRocksPartitionState::Active)
                .map(|partition| partition.partition_id)
                .collect::<HashSet<_>>();
            let active_index_ids = runtime
                .indexes
                .iter()
                .filter(|index| index.state == StarRocksIndexState::Active)
                .map(|index| index.index_id)
                .collect::<HashSet<_>>();

            for tablet in &runtime.tablets {
                let active = runtime.table.state == StarRocksTableState::Active
                    && active_partition_ids.contains(&tablet.partition_id)
                    && active_index_ids.contains(&tablet.index_id);
                if !active {
                    let _ = remove_tablet_runtime(tablet.tablet_id);
                    continue;
                }
                let ctx = TabletWriteContext {
                    db_id: runtime.table.db_id,
                    table_id: runtime.table.table_id,
                    tablet_id: tablet.tablet_id,
                    tablet_root_path: tablet.tablet_root_path.clone(),
                    tablet_schema: runtime.tablet_schema.clone(),
                    s3_config: Some(config.s3.clone()),
                    partial_update: Default::default(),
                };
                register_tablet_runtime(&ctx)?;
                let visible_version = runtime
                    .partitions
                    .iter()
                    .find(|partition| partition.partition_id == tablet.partition_id)
                    .map(|partition| partition.visible_version)
                    .unwrap_or(1);
                load_tablet_snapshot(
                    tablet.tablet_id,
                    visible_version,
                    &tablet.tablet_root_path,
                    Some(&object_store_profile),
                )?;
            }
        }
        Ok(())
    }
}

// Temporary runtime bridge: StarRocks table repository rows are the source of
// truth for object metadata, while the in-memory catalog still stores the
// legacy snapshot shape until the remaining MV refresh slices stop depending
// on it.
pub(crate) fn repository_snapshot_for_runtime(
    config: Option<&StarRocksTableConfig>,
    snapshot: crate::meta::repository::starrocks_table::StarRocksTableSnapshot,
) -> StarRocksTableSnapshot {
    StarRocksTableSnapshot {
        global: StarRocksGlobalMeta {
            warehouse_uri: if snapshot.databases.is_empty()
                && snapshot.tables.is_empty()
                && snapshot.schemas.is_empty()
                && snapshot.columns.is_empty()
                && snapshot.partitions.is_empty()
                && snapshot.indexes.is_empty()
                && snapshot.tablets.is_empty()
            {
                String::new()
            } else {
                config
                    .map(|config| config.warehouse_uri.clone())
                    .unwrap_or_default()
            },
            ..StarRocksGlobalMeta::default()
        },
        databases: snapshot
            .databases
            .into_iter()
            .map(|database| StoredStarRocksDatabase {
                db_id: database.db_id,
                name: database.name,
            })
            .collect(),
        tables: snapshot
            .tables
            .into_iter()
            .map(|table| StoredStarRocksTable {
                table_id: table.table_id,
                db_id: table.db_id,
                name: table.name,
                keys_type: table.keys_type,
                bucket_num: table.bucket_num,
                current_schema_id: table.current_schema_id,
                state: match table.state {
                    crate::meta::repository::starrocks_table::StarRocksTableState::Creating => {
                        StarRocksTableState::Creating
                    }
                    crate::meta::repository::starrocks_table::StarRocksTableState::Active => {
                        StarRocksTableState::Active
                    }
                    crate::meta::repository::starrocks_table::StarRocksTableState::Dropping => {
                        StarRocksTableState::Dropping
                    }
                    crate::meta::repository::starrocks_table::StarRocksTableState::Failed => {
                        StarRocksTableState::Failed
                    }
                },
                kind: match table.kind {
                    crate::meta::repository::starrocks_table::StarRocksTableKind::Table => {
                        super::model::StarRocksTableKind::Table
                    }
                    crate::meta::repository::starrocks_table::StarRocksTableKind::MaterializedView => {
                        super::model::StarRocksTableKind::MaterializedView
                    }
                },
            })
            .collect(),
        schemas: snapshot
            .schemas
            .into_iter()
            .map(|schema| super::model::StoredStarRocksSchema {
                schema_id: schema.schema_id,
                table_id: schema.table_id,
                schema_version: schema.schema_version,
                tablet_schema_pb: schema.tablet_schema_pb,
            })
            .collect(),
        columns: snapshot
            .columns
            .into_iter()
            .map(|column| StoredStarRocksColumn {
                schema_id: column.schema_id,
                ordinal: column.ordinal,
                column_name: column.column_name,
                logical_type: column.logical_type,
                nullable: column.nullable,
                visible: column.visible,
                is_key: column.is_key,
            })
            .collect(),
        partitions: snapshot
            .partitions
            .into_iter()
            .map(|partition| StoredStarRocksPartition {
                partition_id: partition.partition_id,
                table_id: partition.table_id,
                name: partition.name,
                visible_version: partition.visible_version,
                next_version: partition.next_version,
                state: match partition.state {
                    crate::meta::repository::starrocks_table::StarRocksPartitionState::Creating => {
                        StarRocksPartitionState::Creating
                    }
                    crate::meta::repository::starrocks_table::StarRocksPartitionState::Active => {
                        StarRocksPartitionState::Active
                    }
                    crate::meta::repository::starrocks_table::StarRocksPartitionState::Retired => {
                        StarRocksPartitionState::Retired
                    }
                    crate::meta::repository::starrocks_table::StarRocksPartitionState::Failed => {
                        StarRocksPartitionState::Failed
                    }
                },
            })
            .collect(),
        indexes: snapshot
            .indexes
            .into_iter()
            .map(|index| StoredStarRocksIndex {
                index_id: index.index_id,
                table_id: index.table_id,
                partition_id: index.partition_id,
                index_type: index.index_type,
                state: match index.state {
                    crate::meta::repository::starrocks_table::StarRocksIndexState::Creating => {
                        StarRocksIndexState::Creating
                    }
                    crate::meta::repository::starrocks_table::StarRocksIndexState::Active => {
                        StarRocksIndexState::Active
                    }
                    crate::meta::repository::starrocks_table::StarRocksIndexState::Retired => {
                        StarRocksIndexState::Retired
                    }
                    crate::meta::repository::starrocks_table::StarRocksIndexState::Failed => {
                        StarRocksIndexState::Failed
                    }
                },
            })
            .collect(),
        tablets: snapshot
            .tablets
            .into_iter()
            .map(|tablet| StoredStarRocksTablet {
                tablet_id: tablet.tablet_id,
                partition_id: tablet.partition_id,
                index_id: tablet.index_id,
                bucket_seq: tablet.bucket_seq,
                tablet_root_path: tablet.tablet_root_path,
            })
            .collect(),
        #[cfg(test)]
        txns: Vec::new(),
        #[cfg(test)]
        erase_jobs: Vec::new(),
        #[cfg(test)]
        materialized_views: Vec::new(),
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StarRocksTableRuntime {
    pub(crate) database_name: String,
    pub(crate) table: StoredStarRocksTable,
    pub(crate) tablet_schema: TabletSchemaPb,
    pub(crate) columns: Vec<StoredStarRocksColumn>,
    pub(crate) partitions: Vec<StoredStarRocksPartition>,
    pub(crate) indexes: Vec<StoredStarRocksIndex>,
    pub(crate) tablets: Vec<StoredStarRocksTablet>,
}

pub(crate) fn snapshot_is_empty(snapshot: &StarRocksTableSnapshot) -> bool {
    let base = snapshot.global == Default::default()
        && snapshot.databases.is_empty()
        && snapshot.tables.is_empty()
        && snapshot.schemas.is_empty()
        && snapshot.columns.is_empty()
        && snapshot.partitions.is_empty()
        && snapshot.indexes.is_empty()
        && snapshot.tablets.is_empty();
    #[cfg(test)]
    {
        base && snapshot.txns.is_empty() && snapshot.erase_jobs.is_empty()
    }
    #[cfg(not(test))]
    {
        base
    }
}

pub(crate) fn runtime_registered(tablet_id: i64) -> bool {
    get_tablet_runtime(tablet_id).is_ok()
}

pub(crate) fn register_starrocks_table_in_catalog(
    catalog: &mut InMemoryCatalog,
    runtime: &StarRocksTableRuntime,
) -> Result<(), String> {
    let table = starrocks_table_def(runtime)?;
    let layout = starrocks_table_physical_layout(runtime)?;
    catalog.register_starrocks_table(&runtime.database_name, table, layout)
}

pub(crate) fn register_starrocks_tables_in_catalog(
    catalog: &mut InMemoryCatalog,
    starrocks: &StarRocksTableCatalog,
) -> Result<(), String> {
    let mut keys = starrocks.tables_by_name.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    for (database, table) in keys {
        let runtime = starrocks
            .tables_by_name
            .get(&(database, table))
            .ok_or_else(|| "StarRocks catalog changed during registration".to_string())?;
        register_starrocks_table_in_catalog(catalog, runtime)?;
    }
    Ok(())
}

fn starrocks_table_def(runtime: &StarRocksTableRuntime) -> Result<TableDef, String> {
    let schema_columns = visible_tablet_columns_by_name(&runtime.tablet_schema)?;
    let mut columns = Vec::with_capacity(runtime.columns.len());
    for column in &runtime.columns {
        if !column.visible {
            continue;
        }
        let schema_column = schema_columns.get(&column.column_name).ok_or_else(|| {
            format!(
                "StarRocks table {}.{} is missing schema metadata for column `{}`",
                runtime.database_name, runtime.table.name, column.column_name
            )
        })?;
        columns.push(ColumnDef {
            name: column.column_name.clone(),
            data_type: arrow_type_from_tablet_column(schema_column)?,
            nullable: column.nullable,
            write_default: None,
            logical_type: logical_type_from_tablet_column(schema_column),
        });
    }
    Ok(TableDef {
        name: runtime.table.name.clone(),
        columns,
        iceberg_row_lineage_metadata_columns: vec![],
        source: ScanSource::StarRocks {
            db_id: runtime.table.db_id,
            table_id: runtime.table.table_id,
        },
    })
}

fn starrocks_table_physical_layout(
    runtime: &StarRocksTableRuntime,
) -> Result<PhysicalTableLayout, String> {
    let active_partition_versions = runtime
        .partitions
        .iter()
        .filter(|partition| partition.state == StarRocksPartitionState::Active)
        .map(|partition| (partition.partition_id, partition.visible_version))
        .collect::<HashMap<_, _>>();
    let active_index_ids = runtime
        .indexes
        .iter()
        .filter(|index| index.state == StarRocksIndexState::Active)
        .map(|index| index.index_id)
        .collect::<HashSet<_>>();

    let tablets = runtime
        .tablets
        .iter()
        .filter(|tablet| active_index_ids.contains(&tablet.index_id))
        .filter_map(|tablet| {
            active_partition_versions
                .get(&tablet.partition_id)
                .copied()
                .map(|version| StarRocksTabletRef {
                    tablet_id: tablet.tablet_id,
                    partition_id: tablet.partition_id,
                    version,
                })
        })
        .collect();
    Ok(PhysicalTableLayout {
        db_id: runtime.table.db_id,
        table_id: runtime.table.table_id,
        schema_id: runtime.table.current_schema_id,
        tablets,
    })
}

fn visible_tablet_columns_by_name(
    tablet_schema: &TabletSchemaPb,
) -> Result<HashMap<String, ColumnPb>, String> {
    let mut columns = HashMap::new();
    for column in &tablet_schema.column {
        if column.visible == Some(false) {
            continue;
        }
        let name = column
            .name
            .as_deref()
            .ok_or_else(|| "StarRocks tablet schema column missing name".to_string())?;
        let key = normalize_identifier(name)?;
        if columns.insert(key.clone(), column.clone()).is_some() {
            return Err(format!(
                "StarRocks tablet schema has duplicate column `{key}`"
            ));
        }
    }
    Ok(columns)
}

pub(crate) fn arrow_type_from_tablet_column(column: &ColumnPb) -> Result<DataType, String> {
    let raw_type = column.r#type.trim().to_ascii_uppercase();
    let base_type = raw_type
        .split('(')
        .next()
        .unwrap_or(raw_type.as_str())
        .trim();
    match base_type {
        "BOOLEAN" => Ok(DataType::Boolean),
        "TINYINT" => Ok(DataType::Int8),
        "SMALLINT" => Ok(DataType::Int16),
        "INT" => Ok(DataType::Int32),
        "BIGINT" => Ok(DataType::Int64),
        "LARGEINT" => Ok(DataType::FixedSizeBinary(LARGEINT_BYTE_WIDTH)),
        "FLOAT" => Ok(DataType::Float32),
        "DOUBLE" => Ok(DataType::Float64),
        "DATE" | "DATE_V2" => Ok(DataType::Date32),
        "DATETIME" | "DATETIME_V2" | "TIMESTAMP" => {
            Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
        }
        "TIME" => Ok(DataType::Time64(TimeUnit::Microsecond)),
        "CHAR" | "VARCHAR" | "STRING" | "JSON" => Ok(DataType::Utf8),
        // BITMAP / HLL share `DataType::Binary` with plain BINARY at the
        // Arrow layer. Logical-type tagging via `ColumnDef.logical_type`
        // lets the analyzer distinguish them downstream.
        "BINARY" | "VARBINARY" | "OBJECT" | "BITMAP" | "HLL" => Ok(DataType::Binary),
        "DECIMAL" | "DECIMAL32" | "DECIMAL64" | "DECIMAL128" => {
            let precision = column
                .precision
                .and_then(|value| u8::try_from(value).ok())
                .ok_or_else(|| format!("StarRocks DECIMAL column missing precision: {raw_type}"))?;
            let scale = column
                .frac
                .and_then(|value| i8::try_from(value).ok())
                .ok_or_else(|| format!("StarRocks DECIMAL column missing scale: {raw_type}"))?;
            Ok(DataType::Decimal128(precision, scale))
        }
        "DECIMALV2" => Ok(DataType::Decimal128(
            LEGACY_DECIMALV2_PRECISION,
            LEGACY_DECIMALV2_SCALE,
        )),
        "ARRAY" => {
            let item_column = column.children_columns.first().ok_or_else(|| {
                format!(
                    "StarRocks ARRAY column `{}` is missing item type",
                    column.name.as_deref().unwrap_or("<unnamed>")
                )
            })?;
            let item_type = arrow_type_from_tablet_column(item_column)?;
            Ok(DataType::List(Arc::new(Field::new(
                "item",
                item_type,
                item_column.is_nullable.unwrap_or(true),
            ))))
        }
        "MAP" => {
            let key_column = column.children_columns.first().ok_or_else(|| {
                format!(
                    "StarRocks MAP column `{}` is missing key type",
                    column.name.as_deref().unwrap_or("<unnamed>")
                )
            })?;
            let value_column = column.children_columns.get(1).ok_or_else(|| {
                format!(
                    "StarRocks MAP column `{}` is missing value type",
                    column.name.as_deref().unwrap_or("<unnamed>")
                )
            })?;
            // StarRocks-style MAP semantics treat the key as nullable — a
            // `map{1:10, 2:20, null:30}` literal keeps the `null` key entry
            // and a GROUP BY on the map sees it as a distinct group. The
            // Iceberg spec marks map keys as non-nullable, but for the
            // managed-lake (StarRocks native segment) backend we mirror the
            // StarRocks semantics here. The tablet column metadata may also
            // declare an explicit `is_nullable` for the key; honour it but
            // default to nullable so legacy schemas behave correctly.
            let entries = Fields::from(vec![
                Field::new(
                    "key",
                    arrow_type_from_tablet_column(key_column)?,
                    key_column.is_nullable.unwrap_or(true),
                ),
                Field::new(
                    "value",
                    arrow_type_from_tablet_column(value_column)?,
                    value_column.is_nullable.unwrap_or(true),
                ),
            ]);
            Ok(DataType::Map(
                Arc::new(Field::new("entries", DataType::Struct(entries), false)),
                false,
            ))
        }
        "STRUCT" => {
            let mut fields = Vec::with_capacity(column.children_columns.len());
            for child in &column.children_columns {
                let child_name = child
                    .name
                    .clone()
                    .unwrap_or_else(|| format!("field_{}", fields.len()));
                fields.push(Field::new(
                    child_name,
                    arrow_type_from_tablet_column(child)?,
                    child.is_nullable.unwrap_or(true),
                ));
            }
            Ok(DataType::Struct(Fields::from(fields)))
        }
        other => Err(format!(
            "unsupported StarRocks tablet column type `{other}`"
        )),
    }
}

/// Return the StarRocks logical type tag for tablet columns whose Arrow
/// `data_type` does not uniquely identify the logical type (`JSON` collapses
/// onto `DataType::Utf8`; `BITMAP` and `HLL` collapse onto `DataType::Binary`).
/// Returns `None` for columns whose Arrow type is authoritative.
fn logical_type_from_tablet_column(column: &ColumnPb) -> Option<crate::sql::SqlType> {
    let raw_type = column.r#type.trim().to_ascii_uppercase();
    let base_type = raw_type
        .split('(')
        .next()
        .unwrap_or(raw_type.as_str())
        .trim();
    match base_type {
        "JSON" => Some(crate::sql::SqlType::Json),
        // BE schema persists BITMAP as `OBJECT` (the historical wire name).
        "OBJECT" | "BITMAP" => Some(crate::sql::SqlType::Bitmap),
        "HLL" => Some(crate::sql::SqlType::Hll),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::starrocks::table::model::{StarRocksTableKind, StoredStarRocksSchema};
    use crate::engine::catalog::DEFAULT_DATABASE;
    use crate::runtime::starlet_shard_registry::S3StoreConfig;
    use crate::service::grpc_client::proto::starrocks::ColumnPb;

    #[test]
    fn register_starrocks_tables_in_catalog_populates_logical_table_and_layout() {
        let runtime = StarRocksTableRuntime {
            database_name: DEFAULT_DATABASE.to_string(),
            table: StoredStarRocksTable {
                table_id: 20,
                db_id: 10,
                name: "starrocks_tbl".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                current_schema_id: 30,
                state: StarRocksTableState::Active,
                kind: StarRocksTableKind::Table,
            },
            tablet_schema: TabletSchemaPb {
                column: vec![
                    ColumnPb {
                        unique_id: 0,
                        name: Some("id".to_string()),
                        r#type: "INT".to_string(),
                        is_nullable: Some(false),
                        ..Default::default()
                    },
                    ColumnPb {
                        unique_id: 1,
                        name: Some("items".to_string()),
                        r#type: "ARRAY".to_string(),
                        is_nullable: Some(true),
                        children_columns: vec![ColumnPb {
                            unique_id: 2,
                            name: Some("item".to_string()),
                            r#type: "VARCHAR".to_string(),
                            is_nullable: Some(true),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    ColumnPb {
                        unique_id: 3,
                        name: Some("__hidden".to_string()),
                        r#type: "BIGINT".to_string(),
                        is_nullable: Some(true),
                        visible: Some(false),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            columns: vec![
                StoredStarRocksColumn {
                    schema_id: 30,
                    ordinal: 0,
                    column_name: "id".to_string(),
                    logical_type: "INT".to_string(),
                    nullable: false,
                    visible: true,
                    is_key: false,
                },
                StoredStarRocksColumn {
                    schema_id: 30,
                    ordinal: 1,
                    column_name: "items".to_string(),
                    logical_type: "ARRAY<STRING>".to_string(),
                    nullable: true,
                    visible: true,
                    is_key: false,
                },
                StoredStarRocksColumn {
                    schema_id: 30,
                    ordinal: 2,
                    column_name: "__hidden".to_string(),
                    logical_type: "BIGINT".to_string(),
                    nullable: true,
                    visible: false,
                    is_key: false,
                },
            ],
            partitions: vec![
                StoredStarRocksPartition {
                    partition_id: 100,
                    table_id: 20,
                    name: "p0".to_string(),
                    visible_version: 7,
                    next_version: 8,
                    state: StarRocksPartitionState::Active,
                },
                StoredStarRocksPartition {
                    partition_id: 101,
                    table_id: 20,
                    name: "p1".to_string(),
                    visible_version: 9,
                    next_version: 10,
                    state: StarRocksPartitionState::Active,
                },
            ],
            indexes: vec![StoredStarRocksIndex {
                index_id: 200,
                table_id: 20,
                partition_id: 100,
                index_type: "BASE".to_string(),
                state: StarRocksIndexState::Active,
            }],
            tablets: vec![
                StoredStarRocksTablet {
                    tablet_id: 300,
                    partition_id: 100,
                    index_id: 200,
                    bucket_seq: 0,
                    tablet_root_path: "s3://warehouse/db_10/table_20/tablet_300".to_string(),
                },
                StoredStarRocksTablet {
                    tablet_id: 301,
                    partition_id: 101,
                    index_id: 200,
                    bucket_seq: 1,
                    tablet_root_path: "s3://warehouse/db_10/table_20/tablet_301".to_string(),
                },
            ],
        };
        let starrocks = StarRocksTableCatalog {
            config: None,
            snapshot: StarRocksTableSnapshot::default(),
            tables_by_name: HashMap::from([(
                (DEFAULT_DATABASE.to_string(), "starrocks_tbl".to_string()),
                runtime,
            )]),
        };
        let mut catalog = InMemoryCatalog::default();

        register_starrocks_tables_in_catalog(&mut catalog, &starrocks)
            .expect("register StarRocks tables in catalog");

        let table = catalog
            .get(DEFAULT_DATABASE, "starrocks_tbl")
            .expect("logical table");
        assert_eq!(table.name, "starrocks_tbl");
        assert_eq!(
            table.columns,
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                },
                ColumnDef {
                    name: "items".to_string(),
                    data_type: DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                },
            ]
        );
        assert!(matches!(table.source, ScanSource::StarRocks { .. }));

        let layout = catalog
            .get_physical_layout(DEFAULT_DATABASE, "starrocks_tbl")
            .expect("physical layout")
            .expect("StarRocks layout");
        assert_eq!(
            layout,
            PhysicalTableLayout {
                db_id: 10,
                table_id: 20,
                schema_id: 30,
                tablets: vec![
                    StarRocksTabletRef {
                        tablet_id: 300,
                        partition_id: 100,
                        version: 7,
                    },
                    StarRocksTabletRef {
                        tablet_id: 301,
                        partition_id: 101,
                        version: 9,
                    },
                ],
            }
        );
    }

    #[test]
    fn arrow_type_from_tablet_column_preserves_time_semantics() {
        let column = ColumnPb {
            unique_id: 7,
            name: Some("t".to_string()),
            r#type: "TIME".to_string(),
            is_nullable: Some(true),
            ..Default::default()
        };
        assert_eq!(
            arrow_type_from_tablet_column(&column).expect("time arrow type"),
            DataType::Time64(TimeUnit::Microsecond)
        );
    }

    #[test]
    fn arrow_type_from_tablet_column_maps_json_to_utf8() {
        let column = ColumnPb {
            unique_id: 8,
            name: Some("payload".to_string()),
            r#type: "JSON".to_string(),
            is_nullable: Some(true),
            ..Default::default()
        };
        assert_eq!(
            arrow_type_from_tablet_column(&column).expect("json arrow type"),
            DataType::Utf8
        );
    }

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

    fn snapshot_seed() -> StarRocksTableSnapshot {
        use crate::connector::starrocks::table::model::{
            StarRocksGlobalMeta, StoredStarRocksDatabase, StoredStarRocksPartition,
        };
        StarRocksTableSnapshot {
            global: StarRocksGlobalMeta {
                warehouse_uri: "s3://test/warehouse".to_string(),
                next_db_id: 2,
                next_table_id: 20,
                next_partition_id: 110,
                next_index_id: 30,
                next_tablet_id: 400,
                next_txn_id: 60,
            },
            databases: vec![StoredStarRocksDatabase {
                db_id: 1,
                name: "analytics".to_string(),
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
                tablet_schema_pb: vec![],
            }],
            columns: Vec::new(),
            partitions: vec![StoredStarRocksPartition {
                partition_id: 20,
                table_id: 10,
                name: "p0".to_string(),
                visible_version: 1,
                next_version: 2,
                state: StarRocksPartitionState::Active,
            }],
            indexes: Vec::new(),
            tablets: Vec::new(),
            txns: Vec::new(),
            erase_jobs: Vec::new(),
            materialized_views: Vec::new(),
        }
    }

    #[test]
    fn starrocks_table_config_uses_partition_scoped_root() {
        let config = StarRocksTableConfig {
            warehouse_uri: "s3://bucket/warehouse".to_string(),
            s3: S3StoreConfig {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "bucket".to_string(),
                access_key_id: "ak".to_string(),
                access_key_secret: "sk".to_string(),
                region: Some("us-east-1".to_string()),
                enable_path_style_access: Some(true),
            },
            mv_default_storage_engine: "starrocks".to_string(),
        };

        assert_eq!(
            config.tablet_root_path(1, 10, 20),
            "s3://bucket/warehouse/db_1/table_10/partition_20"
        );
    }

    #[test]
    fn rebuild_ignores_dropping_tables_and_retired_partitions() {
        let mut snapshot = snapshot_seed();
        snapshot.tables[0].state = StarRocksTableState::Dropping;
        snapshot.partitions[0].state = StarRocksPartitionState::Retired;

        let rebuilt = StarRocksTableCatalog::rebuild(Some(test_starrocks_table_config()), snapshot)
            .expect("rebuild");

        assert!(
            !rebuilt
                .contains_table("analytics", "orders")
                .expect("contains table"),
            "dropping table should not remain visible"
        );
    }

    #[test]
    fn rebuild_preserves_kind_column() {
        let mut snapshot = snapshot_seed();
        // snapshot_seed creates a kind='TABLE' row by default; spot-check.
        let rebuilt =
            StarRocksTableCatalog::rebuild(Some(test_starrocks_table_config()), snapshot.clone())
                .expect("rebuild");
        let runtime = rebuilt
            .table("analytics", "orders")
            .expect("runtime")
            .clone();
        assert_eq!(runtime.table.kind, StarRocksTableKind::Table);

        snapshot.tables[0].kind = StarRocksTableKind::MaterializedView;
        let rebuilt_mv =
            StarRocksTableCatalog::rebuild(Some(test_starrocks_table_config()), snapshot)
                .expect("rebuild mv");
        let runtime_mv = rebuilt_mv
            .table("analytics", "orders")
            .expect("runtime")
            .clone();
        assert_eq!(runtime_mv.table.kind, StarRocksTableKind::MaterializedView);
    }

    /// Build a minimal-but-valid `StarRocksTableRuntime` parameterised by
    /// `(db_id, table_id)` so the constructor test can verify identity flows
    /// from runtime → `ScanSource::StarRocks { db_id, table_id }`. Mirrors
    /// the shape of the larger fixture above; one INT column is enough for
    /// `starrocks_table_def` to succeed.
    fn sample_runtime_with_ids(db_id: i64, table_id: i64) -> StarRocksTableRuntime {
        StarRocksTableRuntime {
            database_name: DEFAULT_DATABASE.to_string(),
            table: StoredStarRocksTable {
                table_id,
                db_id,
                name: "sample_tbl".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 1,
                current_schema_id: 1,
                state: StarRocksTableState::Active,
                kind: StarRocksTableKind::Table,
            },
            tablet_schema: TabletSchemaPb {
                column: vec![ColumnPb {
                    unique_id: 0,
                    name: Some("id".to_string()),
                    r#type: "INT".to_string(),
                    is_nullable: Some(false),
                    ..Default::default()
                }],
                ..Default::default()
            },
            columns: vec![StoredStarRocksColumn {
                schema_id: 1,
                ordinal: 0,
                column_name: "id".to_string(),
                logical_type: "INT".to_string(),
                nullable: false,
                visible: true,
                is_key: false,
            }],
            partitions: vec![],
            indexes: vec![],
            tablets: vec![],
        }
    }

    /// `starrocks_table_def` must populate `ScanSource::StarRocks { db_id, table_id }`
    /// from the runtime's identity fields. The dict-rewrite hot path
    /// (`DictionaryQueryProvider::owner_for`) reads these values directly to
    /// avoid taking `state.starrocks_table.read()` on every Scan column.
    #[test]
    fn starrocks_table_def_carries_runtime_ids_in_scan_source() {
        let runtime = sample_runtime_with_ids(12_345, 67_890);

        let table = super::starrocks_table_def(&runtime)
            .expect("starrocks_table_def must succeed for the sample runtime");

        match table.source {
            ScanSource::StarRocks { db_id, table_id } => {
                assert_eq!(db_id, 12_345, "db_id must come from runtime.table.db_id");
                assert_eq!(
                    table_id, 67_890,
                    "table_id must come from runtime.table.table_id"
                );
            }
            other => panic!("expected ScanSource::StarRocks, got {other:?}"),
        }
    }
}
