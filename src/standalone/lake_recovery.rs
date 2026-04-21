use std::collections::{HashMap, HashSet};

use prost::Message;

use crate::common::app_config::StandaloneManagedLakeConfig as AppManagedLakeConfig;
use crate::connector::starrocks::ObjectStoreProfile;
use crate::connector::starrocks::lake::context::{
    TabletWriteContext, get_tablet_runtime, register_tablet_runtime, remove_tablet_runtime,
};
use crate::formats::starrocks::metadata::load_tablet_snapshot;
use crate::runtime::starlet_shard_registry::S3StoreConfig;
use crate::service::grpc_client::proto::starrocks::TabletSchemaPb;

use super::catalog::normalize_identifier;
use super::iceberg_add_files::parse_s3_path;
use super::store::{
    ManagedIndexState, ManagedPartitionState, ManagedSnapshot, ManagedTableState,
    StoredManagedColumn, StoredManagedIndex, StoredManagedPartition, StoredManagedSchema,
    StoredManagedTable, StoredManagedTablet,
};

#[derive(Clone, Debug)]
pub(crate) struct ManagedLakeConfig {
    pub(crate) warehouse_uri: String,
    pub(crate) s3: S3StoreConfig,
}

impl ManagedLakeConfig {
    pub(crate) fn from_app_config(config: AppManagedLakeConfig) -> Result<Self, String> {
        let warehouse_uri = config
            .warehouse_uri
            .trim()
            .trim_end_matches('/')
            .to_string();
        if warehouse_uri.is_empty() {
            return Err("standalone managed lake warehouse_uri is empty".to_string());
        }
        let (bucket, root) = parse_s3_path(&warehouse_uri)?;
        Ok(Self {
            warehouse_uri,
            s3: S3StoreConfig {
                endpoint: config.endpoint.trim().to_string(),
                bucket,
                root: root.trim_matches('/').to_string(),
                access_key_id: config.access_key_id.trim().to_string(),
                access_key_secret: config.access_key_secret.trim().to_string(),
                region: config.region.as_ref().map(|value| value.trim().to_string()),
                enable_path_style_access: config.enable_path_style_access,
            },
        })
    }

    pub(crate) fn tablet_root_path(&self, db_id: i64, table_id: i64, tablet_id: i64) -> String {
        format!(
            "{}/db_{db_id}/table_{table_id}/tablet_{tablet_id}",
            self.warehouse_uri
        )
    }
}

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
                    partitions: partitions_by_table
                        .remove(&table.table_id)
                        .unwrap_or_default(),
                    indexes: indexes_by_table.remove(&table.table_id).unwrap_or_default(),
                    tablets: tablets_by_table.remove(&table.table_id).unwrap_or_default(),
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
}

pub(crate) fn runtime_registered(tablet_id: i64) -> bool {
    get_tablet_runtime(tablet_id).is_ok()
}
