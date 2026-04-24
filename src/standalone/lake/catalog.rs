use std::collections::{BTreeMap, HashMap, HashSet};
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

use super::super::engine::catalog::{
    ColumnDef, InMemoryCatalog, ManagedTabletRef, PhysicalTableLayout, TableDef, TableStorage,
    normalize_identifier,
};
use super::config::ManagedLakeConfig;
use super::store::{
    ManagedIndexState, ManagedPartitionState, ManagedSnapshot, ManagedTableKind, ManagedTableState,
    ManagedTxnState, SqliteMetadataStore, StoredManagedColumn, StoredManagedIndex,
    StoredManagedPartition, StoredManagedSchema, StoredManagedTable, StoredManagedTablet,
};

#[derive(Clone, Debug, Default)]
pub(crate) struct ManagedLakeCatalog {
    pub(crate) config: Option<ManagedLakeConfig>,
    pub(crate) snapshot: ManagedSnapshot,
    tables_by_name: HashMap<(String, String), ManagedTableRuntime>,
}

impl ManagedLakeCatalog {
    pub(crate) fn empty(config: Option<ManagedLakeConfig>) -> Self {
        Self {
            config,
            snapshot: ManagedSnapshot::default(),
            tables_by_name: HashMap::new(),
        }
    }

    pub(crate) fn table(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Result<&ManagedTableRuntime, String> {
        let db = normalize_identifier(database_name)?;
        let table = normalize_identifier(table_name)?;
        self.tables_by_name
            .get(&(db.clone(), table.clone()))
            .ok_or_else(|| format!("unknown managed table: {db}.{table}"))
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

    /// Return the original (already-normalized) table names of all managed
    /// tables registered under `database_name`. Empty if the database has no
    /// managed tables or does not exist.
    pub(crate) fn list_tables_in_database(
        &self,
        database_name: &str,
    ) -> Result<Vec<String>, String> {
        let db = normalize_identifier(database_name)?;
        Ok(self
            .tables_by_name
            .keys()
            .filter_map(|(d, t)| (d == &db).then(|| t.clone()))
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
            .ok_or_else(|| format!("managed snapshot is missing partition {partition_id}"))?;
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

    pub(crate) fn runtime_by_table_id(&self, table_id: i64) -> Option<&ManagedTableRuntime> {
        self.tables_by_name
            .values()
            .find(|runtime| runtime.table.table_id == table_id)
    }

    pub(crate) fn rebuild(
        config: Option<ManagedLakeConfig>,
        snapshot: ManagedSnapshot,
    ) -> Result<Self, String> {
        if snapshot_is_empty(&snapshot) {
            return Ok(Self::empty(config));
        }

        let Some(config) = config else {
            return Err(
                "managed lake metadata exists but standalone managed lake config is missing"
                    .to_string(),
            );
        };
        if snapshot.global.warehouse_uri.trim() != config.warehouse_uri {
            return Err(format!(
                "managed lake warehouse mismatch: snapshot={} config={}",
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
                        "decode managed tablet_schema_pb failed for schema_id={}: {e}",
                        schema.schema_id
                    )
                })?;
            schemas_by_id.insert(schema.schema_id, (schema.clone(), decoded));
        }

        let mut columns_by_schema = HashMap::<i64, Vec<StoredManagedColumn>>::new();
        for column in &snapshot.columns {
            columns_by_schema
                .entry(column.schema_id)
                .or_default()
                .push(column.clone());
        }
        for columns in columns_by_schema.values_mut() {
            columns.sort_by_key(|column| column.ordinal);
        }

        let mut partitions_by_table = HashMap::<i64, Vec<StoredManagedPartition>>::new();
        for partition in &snapshot.partitions {
            partitions_by_table
                .entry(partition.table_id)
                .or_default()
                .push(partition.clone());
        }
        for partitions in partitions_by_table.values_mut() {
            partitions.sort_by_key(|partition| partition.partition_id);
        }

        let mut indexes_by_table = HashMap::<i64, Vec<StoredManagedIndex>>::new();
        for index in &snapshot.indexes {
            indexes_by_table
                .entry(index.table_id)
                .or_default()
                .push(index.clone());
        }
        for indexes in indexes_by_table.values_mut() {
            indexes.sort_by_key(|index| index.index_id);
        }

        let mut tablets_by_table = HashMap::<i64, Vec<StoredManagedTablet>>::new();
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
                        "managed tablet {} references unknown index_id={}",
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
            if table.state != ManagedTableState::Active {
                continue;
            }
            let database_name = databases_by_id.get(&table.db_id).cloned().ok_or_else(|| {
                format!(
                    "managed table {} references unknown db_id={}",
                    table.table_id, table.db_id
                )
            })?;
            let (schema, tablet_schema) = schemas_by_id
                .get(&table.current_schema_id)
                .cloned()
                .ok_or_else(|| {
                    format!(
                        "managed table {} references unknown current_schema_id={}",
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
                        ManagedPartitionState::Active | ManagedPartitionState::Creating
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
                        ManagedIndexState::Active | ManagedIndexState::Creating
                    ) && live_partition_ids.contains(&index.partition_id)
                })
                .collect::<Vec<_>>();
            let live_index_ids = indexes
                .iter()
                .map(|index| index.index_id)
                .collect::<HashSet<_>>();
            tables_by_name.insert(
                key,
                ManagedTableRuntime {
                    database_name,
                    table: table.clone(),
                    schema,
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

    pub(crate) fn re_register_active_tablet_runtimes(&self) -> Result<(), String> {
        let Some(config) = self.config.as_ref() else {
            if snapshot_is_empty(&self.snapshot) {
                return Ok(());
            }
            return Err(
                "managed lake metadata exists but standalone managed lake config is missing"
                    .to_string(),
            );
        };
        let object_store_profile = ObjectStoreProfile::from_s3_store_config(&config.s3)?;

        for runtime in self.tables_by_name.values() {
            let active_partition_ids = runtime
                .partitions
                .iter()
                .filter(|partition| partition.state == ManagedPartitionState::Active)
                .map(|partition| partition.partition_id)
                .collect::<HashSet<_>>();
            let active_index_ids = runtime
                .indexes
                .iter()
                .filter(|index| index.state == ManagedIndexState::Active)
                .map(|index| index.index_id)
                .collect::<HashSet<_>>();

            for tablet in &runtime.tablets {
                let _ = remove_tablet_runtime(tablet.tablet_id);
                if runtime.table.state != ManagedTableState::Active
                    || !active_partition_ids.contains(&tablet.partition_id)
                    || !active_index_ids.contains(&tablet.index_id)
                {
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

#[derive(Clone, Debug)]
pub(crate) struct ManagedTableRuntime {
    pub(crate) database_name: String,
    pub(crate) table: StoredManagedTable,
    pub(crate) schema: StoredManagedSchema,
    pub(crate) tablet_schema: TabletSchemaPb,
    pub(crate) columns: Vec<StoredManagedColumn>,
    pub(crate) partitions: Vec<StoredManagedPartition>,
    pub(crate) indexes: Vec<StoredManagedIndex>,
    pub(crate) tablets: Vec<StoredManagedTablet>,
}

/// Reconcile non-terminal state left behind by a crashed process.
///
/// Invoked before the in-memory catalog is rebuilt from the freshly-loaded
/// snapshot. Mutates `snapshot` in place to reflect the terminal state that
/// was persisted, so the rebuild sees a consistent view.
///
/// * `CREATING` tables are finalized as `FAILED` — their tablets may or may
///   not exist on object storage but there is no safe way to finish the
///   bootstrap, so we keep them out of the active catalog.
/// * `PREPARED` txns are aborted — the rowset write never completed.
/// * `WRITTEN` txns are replayed by `replay`, then marked `VISIBLE`. The
///   partition's visible/next version is also advanced.
pub(crate) fn reconcile_on_open<F>(
    store: &SqliteMetadataStore,
    snapshot: &mut ManagedSnapshot,
    mut replay: F,
) -> Result<(), String>
where
    F: FnMut(&ManagedSnapshot, &super::store::StoredManagedTxn) -> Result<(), String>,
{
    let failed_table_ids: Vec<i64> = snapshot
        .tables
        .iter()
        .filter(|table| table.state == ManagedTableState::Creating)
        .map(|table| table.table_id)
        .collect();
    for table_id in &failed_table_ids {
        store.mark_table_failed(*table_id)?;
    }
    for table in snapshot.tables.iter_mut() {
        if failed_table_ids.contains(&table.table_id) {
            table.state = ManagedTableState::Failed;
        }
    }

    let dangling_partition_ids = snapshot
        .partitions
        .iter()
        .filter(|partition| partition.state == ManagedPartitionState::Creating)
        .map(|partition| partition.partition_id)
        .collect::<Vec<_>>();
    for partition_id in &dangling_partition_ids {
        store.delete_creating_partition(*partition_id)?;
    }
    snapshot
        .partitions
        .retain(|partition| !dangling_partition_ids.contains(&partition.partition_id));
    snapshot
        .indexes
        .retain(|index| !dangling_partition_ids.contains(&index.partition_id));
    snapshot
        .tablets
        .retain(|tablet| !dangling_partition_ids.contains(&tablet.partition_id));

    let mut aborted = Vec::new();
    let mut replayed = Vec::new();
    for txn in &snapshot.txns {
        match txn.state {
            ManagedTxnState::Prepared => aborted.push(txn.txn_id),
            ManagedTxnState::Written => replayed.push(txn.clone()),
            _ => {}
        }
    }

    for txn_id in &aborted {
        store.mark_txn_aborted(*txn_id)?;
    }

    for txn in &replayed {
        replay(snapshot, txn)?;
        store.mark_txn_visible(txn.txn_id, txn.commit_version)?;
    }

    for txn in snapshot.txns.iter_mut() {
        if aborted.contains(&txn.txn_id) {
            txn.state = ManagedTxnState::Aborted;
            txn.retry_at_ms = None;
        } else if replayed.iter().any(|r| r.txn_id == txn.txn_id) {
            txn.state = ManagedTxnState::Visible;
        }
    }
    for txn in &replayed {
        for partition in snapshot.partitions.iter_mut() {
            if partition.partition_id == txn.partition_id
                && partition.visible_version < txn.commit_version
            {
                partition.visible_version = txn.commit_version;
                partition.next_version = txn.commit_version + 1;
            }
        }
    }
    Ok(())
}

pub(crate) fn snapshot_is_empty(snapshot: &ManagedSnapshot) -> bool {
    snapshot.global == Default::default()
        && snapshot.databases.is_empty()
        && snapshot.tables.is_empty()
        && snapshot.schemas.is_empty()
        && snapshot.columns.is_empty()
        && snapshot.partitions.is_empty()
        && snapshot.indexes.is_empty()
        && snapshot.tablets.is_empty()
        && snapshot.txns.is_empty()
        && snapshot.erase_jobs.is_empty()
}

pub(crate) fn runtime_registered(tablet_id: i64) -> bool {
    get_tablet_runtime(tablet_id).is_ok()
}

pub(crate) fn register_managed_table_in_catalog(
    catalog: &mut InMemoryCatalog,
    runtime: &ManagedTableRuntime,
) -> Result<(), String> {
    let table = managed_table_def(runtime)?;
    let layout = managed_physical_layout(runtime)?;
    catalog.register_managed_table(&runtime.database_name, table, layout)
}

pub(crate) fn register_managed_tables_in_catalog(
    catalog: &mut InMemoryCatalog,
    managed: &ManagedLakeCatalog,
) -> Result<(), String> {
    let mut keys = managed.tables_by_name.keys().cloned().collect::<Vec<_>>();
    keys.sort();
    for (database, table) in keys {
        let runtime = managed
            .tables_by_name
            .get(&(database, table))
            .ok_or_else(|| "managed catalog changed during registration".to_string())?;
        register_managed_table_in_catalog(catalog, runtime)?;
    }
    Ok(())
}

fn managed_table_def(runtime: &ManagedTableRuntime) -> Result<TableDef, String> {
    let schema_columns = visible_tablet_columns_by_name(&runtime.tablet_schema)?;
    let mut columns = Vec::with_capacity(runtime.columns.len());
    for column in &runtime.columns {
        let schema_column = schema_columns.get(&column.column_name).ok_or_else(|| {
            format!(
                "managed table {}.{} is missing schema metadata for column `{}`",
                runtime.database_name, runtime.table.name, column.column_name
            )
        })?;
        columns.push(ColumnDef {
            name: column.column_name.clone(),
            data_type: arrow_type_from_tablet_column(schema_column)?,
            nullable: column.nullable,
        });
    }
    Ok(TableDef {
        name: runtime.table.name.clone(),
        columns,
        storage: TableStorage::S3ParquetFiles {
            files: vec![],
            cloud_properties: BTreeMap::new(),
        },
    })
}

fn managed_physical_layout(runtime: &ManagedTableRuntime) -> Result<PhysicalTableLayout, String> {
    let active_partition_versions = runtime
        .partitions
        .iter()
        .filter(|partition| partition.state == ManagedPartitionState::Active)
        .map(|partition| (partition.partition_id, partition.visible_version))
        .collect::<HashMap<_, _>>();
    let active_index_ids = runtime
        .indexes
        .iter()
        .filter(|index| index.state == ManagedIndexState::Active)
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
                .map(|version| ManagedTabletRef {
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
            .ok_or_else(|| "managed tablet schema column missing name".to_string())?;
        let key = normalize_identifier(name)?;
        if columns.insert(key.clone(), column.clone()).is_some() {
            return Err(format!(
                "managed tablet schema has duplicate column `{key}`"
            ));
        }
    }
    Ok(columns)
}

fn arrow_type_from_tablet_column(column: &ColumnPb) -> Result<DataType, String> {
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
        "CHAR" | "VARCHAR" | "STRING" => Ok(DataType::Utf8),
        "BINARY" | "VARBINARY" => Ok(DataType::Binary),
        "DECIMAL" | "DECIMAL32" | "DECIMAL64" | "DECIMAL128" => {
            let precision = column
                .precision
                .and_then(|value| u8::try_from(value).ok())
                .ok_or_else(|| format!("managed DECIMAL column missing precision: {raw_type}"))?;
            let scale = column
                .frac
                .and_then(|value| i8::try_from(value).ok())
                .ok_or_else(|| format!("managed DECIMAL column missing scale: {raw_type}"))?;
            Ok(DataType::Decimal128(precision, scale))
        }
        "DECIMALV2" => Ok(DataType::Decimal128(
            LEGACY_DECIMALV2_PRECISION,
            LEGACY_DECIMALV2_SCALE,
        )),
        "ARRAY" => {
            let item_column = column.children_columns.first().ok_or_else(|| {
                format!(
                    "managed ARRAY column `{}` is missing item type",
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
                    "managed MAP column `{}` is missing key type",
                    column.name.as_deref().unwrap_or("<unnamed>")
                )
            })?;
            let value_column = column.children_columns.get(1).ok_or_else(|| {
                format!(
                    "managed MAP column `{}` is missing value type",
                    column.name.as_deref().unwrap_or("<unnamed>")
                )
            })?;
            let entries = Fields::from(vec![
                Field::new("key", arrow_type_from_tablet_column(key_column)?, false),
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
        other => Err(format!("unsupported managed tablet column type `{other}`")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::starlet_shard_registry::S3StoreConfig;
    use crate::service::grpc_client::proto::starrocks::ColumnPb;
    use crate::standalone::engine::catalog::DEFAULT_DATABASE;

    #[test]
    fn register_managed_tables_in_catalog_populates_logical_table_and_layout() {
        let runtime = ManagedTableRuntime {
            database_name: DEFAULT_DATABASE.to_string(),
            table: StoredManagedTable {
                table_id: 20,
                db_id: 10,
                name: "managed_tbl".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                current_schema_id: 30,
                state: ManagedTableState::Active,
                kind: ManagedTableKind::Table,
            },
            schema: StoredManagedSchema {
                schema_id: 30,
                table_id: 20,
                schema_version: 0,
                tablet_schema_pb: vec![],
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
                ],
                ..Default::default()
            },
            columns: vec![
                StoredManagedColumn {
                    schema_id: 30,
                    ordinal: 0,
                    column_name: "id".to_string(),
                    logical_type: "INT".to_string(),
                    nullable: false,
                },
                StoredManagedColumn {
                    schema_id: 30,
                    ordinal: 1,
                    column_name: "items".to_string(),
                    logical_type: "ARRAY<STRING>".to_string(),
                    nullable: true,
                },
            ],
            partitions: vec![
                StoredManagedPartition {
                    partition_id: 100,
                    table_id: 20,
                    name: "p0".to_string(),
                    visible_version: 7,
                    next_version: 8,
                    state: ManagedPartitionState::Active,
                },
                StoredManagedPartition {
                    partition_id: 101,
                    table_id: 20,
                    name: "p1".to_string(),
                    visible_version: 9,
                    next_version: 10,
                    state: ManagedPartitionState::Active,
                },
            ],
            indexes: vec![StoredManagedIndex {
                index_id: 200,
                table_id: 20,
                partition_id: 100,
                index_type: "BASE".to_string(),
                state: ManagedIndexState::Active,
            }],
            tablets: vec![
                StoredManagedTablet {
                    tablet_id: 300,
                    partition_id: 100,
                    index_id: 200,
                    bucket_seq: 0,
                    tablet_root_path: "s3://warehouse/db_10/table_20/tablet_300".to_string(),
                },
                StoredManagedTablet {
                    tablet_id: 301,
                    partition_id: 101,
                    index_id: 200,
                    bucket_seq: 1,
                    tablet_root_path: "s3://warehouse/db_10/table_20/tablet_301".to_string(),
                },
            ],
        };
        let managed = ManagedLakeCatalog {
            config: None,
            snapshot: ManagedSnapshot::default(),
            tables_by_name: HashMap::from([(
                (DEFAULT_DATABASE.to_string(), "managed_tbl".to_string()),
                runtime,
            )]),
        };
        let mut catalog = InMemoryCatalog::default();

        register_managed_tables_in_catalog(&mut catalog, &managed)
            .expect("register managed tables in catalog");

        let table = catalog
            .get(DEFAULT_DATABASE, "managed_tbl")
            .expect("logical table");
        assert_eq!(table.name, "managed_tbl");
        assert_eq!(
            table.columns,
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                },
                ColumnDef {
                    name: "items".to_string(),
                    data_type: DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    nullable: true,
                },
            ]
        );
        assert!(matches!(
            table.storage,
            TableStorage::S3ParquetFiles {
                files,
                cloud_properties
            } if files.is_empty() && cloud_properties == BTreeMap::new()
        ));

        let layout = catalog
            .get_physical_layout(DEFAULT_DATABASE, "managed_tbl")
            .expect("physical layout")
            .expect("managed layout");
        assert_eq!(
            layout,
            PhysicalTableLayout {
                db_id: 10,
                table_id: 20,
                schema_id: 30,
                tablets: vec![
                    ManagedTabletRef {
                        tablet_id: 300,
                        partition_id: 100,
                        version: 7,
                    },
                    ManagedTabletRef {
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

    fn test_store_with_snapshot(
        snapshot: &ManagedSnapshot,
    ) -> (tempfile::TempDir, SqliteMetadataStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store =
            SqliteMetadataStore::open(dir.path().join("standalone.sqlite")).expect("open store");
        store
            .replace_managed_snapshot(snapshot)
            .expect("persist snapshot");
        (dir, store)
    }

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
        use crate::standalone::lake::store::{
            ManagedGlobalMeta, StoredManagedDatabase, StoredManagedPartition,
        };
        ManagedSnapshot {
            global: ManagedGlobalMeta {
                warehouse_uri: "s3://test/warehouse".to_string(),
                next_db_id: 2,
                next_table_id: 20,
                next_partition_id: 110,
                next_index_id: 30,
                next_tablet_id: 400,
                next_txn_id: 60,
            },
            databases: vec![StoredManagedDatabase {
                db_id: 1,
                name: "analytics".to_string(),
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
                tablet_schema_pb: vec![],
            }],
            columns: Vec::new(),
            partitions: vec![StoredManagedPartition {
                partition_id: 20,
                table_id: 10,
                name: "p0".to_string(),
                visible_version: 1,
                next_version: 2,
                state: ManagedPartitionState::Active,
            }],
            indexes: Vec::new(),
            tablets: Vec::new(),
            txns: Vec::new(),
            erase_jobs: Vec::new(),
            materialized_views: Vec::new(),
        }
    }

    #[test]
    fn reconcile_on_open_marks_creating_tables_failed() {
        let mut snapshot = snapshot_seed();
        snapshot.tables[0].state = ManagedTableState::Creating;
        let (_dir, store) = test_store_with_snapshot(&snapshot);

        reconcile_on_open(&store, &mut snapshot, |_, _| {
            panic!("no txns should trigger replay")
        })
        .expect("reconcile");

        assert_eq!(snapshot.tables[0].state, ManagedTableState::Failed);
        let persisted = store.load_snapshot().expect("load snapshot");
        assert_eq!(persisted.managed.tables[0].state, ManagedTableState::Failed);
    }

    #[test]
    fn managed_lake_config_uses_partition_scoped_root() {
        let config = ManagedLakeConfig {
            warehouse_uri: "s3://bucket/warehouse".to_string(),
            s3: S3StoreConfig {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "bucket".to_string(),
                root: "warehouse".to_string(),
                access_key_id: "ak".to_string(),
                access_key_secret: "sk".to_string(),
                region: Some("us-east-1".to_string()),
                enable_path_style_access: Some(true),
            },
        };

        assert_eq!(
            config.tablet_root_path(1, 10, 20),
            "s3://bucket/warehouse/db_1/table_10/partition_20"
        );
    }

    #[test]
    fn rebuild_ignores_dropping_tables_and_retired_partitions() {
        let mut snapshot = snapshot_seed();
        snapshot.tables[0].state = ManagedTableState::Dropping;
        snapshot.partitions[0].state = ManagedPartitionState::Retired;

        let rebuilt =
            ManagedLakeCatalog::rebuild(Some(test_managed_config()), snapshot).expect("rebuild");

        assert!(
            !rebuilt
                .contains_table("analytics", "orders")
                .expect("contains table"),
            "dropping table should not remain visible"
        );
    }

    #[test]
    fn reconcile_on_open_drops_incomplete_creating_partition_rows() {
        let mut snapshot = snapshot_seed();
        snapshot.partitions.push(StoredManagedPartition {
            partition_id: 21,
            table_id: 10,
            name: "p0".to_string(),
            visible_version: 1,
            next_version: 2,
            state: ManagedPartitionState::Creating,
        });
        snapshot.indexes.push(StoredManagedIndex {
            index_id: 31,
            table_id: 10,
            partition_id: 21,
            index_type: "BASE".to_string(),
            state: ManagedIndexState::Creating,
        });
        snapshot.tablets.push(StoredManagedTablet {
            tablet_id: 41,
            partition_id: 21,
            index_id: 31,
            bucket_seq: 0,
            tablet_root_path: "s3://test/warehouse/db_1/table_10/partition_21".to_string(),
        });
        let (_dir, store) = test_store_with_snapshot(&snapshot);

        reconcile_on_open(&store, &mut snapshot, |_, _| Ok(())).expect("reconcile");

        assert!(
            !snapshot
                .partitions
                .iter()
                .any(|partition| partition.partition_id == 21)
        );
        assert!(
            !snapshot
                .indexes
                .iter()
                .any(|index| index.partition_id == 21)
        );
        assert!(
            !snapshot
                .tablets
                .iter()
                .any(|tablet| tablet.partition_id == 21)
        );
    }

    #[test]
    fn reconcile_on_open_aborts_prepared_txns_without_replay() {
        use crate::standalone::lake::store::StoredManagedTxn;
        let mut snapshot = snapshot_seed();
        snapshot.txns.push(StoredManagedTxn {
            txn_id: 90,
            table_id: 10,
            partition_id: 20,
            base_version: 1,
            commit_version: 2,
            state: ManagedTxnState::Prepared,
            retry_at_ms: Some(42),
            updated_at_ms: 0,
        });
        let (_dir, store) = test_store_with_snapshot(&snapshot);

        reconcile_on_open(&store, &mut snapshot, |_, _| {
            panic!("no WRITTEN txns should trigger replay")
        })
        .expect("reconcile");

        assert_eq!(snapshot.txns[0].state, ManagedTxnState::Aborted);
        assert!(snapshot.txns[0].retry_at_ms.is_none());
        assert_eq!(snapshot.partitions[0].visible_version, 1);
        let persisted = store.load_snapshot().expect("load snapshot");
        assert_eq!(persisted.managed.txns[0].state, ManagedTxnState::Aborted);
        assert_eq!(persisted.managed.partitions[0].visible_version, 1);
    }

    #[test]
    fn reconcile_on_open_replays_written_txns_and_advances_partition() {
        use crate::standalone::lake::store::StoredManagedTxn;
        let mut snapshot = snapshot_seed();
        snapshot.txns.push(StoredManagedTxn {
            txn_id: 91,
            table_id: 10,
            partition_id: 20,
            base_version: 1,
            commit_version: 2,
            state: ManagedTxnState::Written,
            retry_at_ms: None,
            updated_at_ms: 0,
        });
        let (_dir, store) = test_store_with_snapshot(&snapshot);
        let mut replay_calls = 0;

        reconcile_on_open(&store, &mut snapshot, |_, txn| {
            replay_calls += 1;
            assert_eq!(txn.txn_id, 91);
            Ok(())
        })
        .expect("reconcile");

        assert_eq!(replay_calls, 1);
        assert_eq!(snapshot.txns[0].state, ManagedTxnState::Visible);
        assert_eq!(snapshot.partitions[0].visible_version, 2);
        assert_eq!(snapshot.partitions[0].next_version, 3);
        let persisted = store.load_snapshot().expect("load snapshot");
        assert_eq!(persisted.managed.txns[0].state, ManagedTxnState::Visible);
        assert_eq!(persisted.managed.partitions[0].visible_version, 2);
    }

    #[test]
    fn rebuild_preserves_kind_column() {
        let mut snapshot = snapshot_seed();
        // snapshot_seed creates a kind='TABLE' row by default; spot-check.
        let rebuilt = ManagedLakeCatalog::rebuild(Some(test_managed_config()), snapshot.clone())
            .expect("rebuild");
        let runtime = rebuilt
            .table("analytics", "orders")
            .expect("runtime")
            .clone();
        assert_eq!(runtime.table.kind, ManagedTableKind::Table);

        snapshot.tables[0].kind = ManagedTableKind::MaterializedView;
        let rebuilt_mv =
            ManagedLakeCatalog::rebuild(Some(test_managed_config()), snapshot).expect("rebuild mv");
        let runtime_mv = rebuilt_mv
            .table("analytics", "orders")
            .expect("runtime")
            .clone();
        assert_eq!(runtime_mv.table.kind, ManagedTableKind::MaterializedView);
    }
}
