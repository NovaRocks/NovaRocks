#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ManagedSnapshot {
    pub global: ManagedGlobalMeta,
    pub databases: Vec<StoredManagedDatabase>,
    pub tables: Vec<StoredManagedTable>,
    pub schemas: Vec<StoredManagedSchema>,
    pub columns: Vec<StoredManagedColumn>,
    pub partitions: Vec<StoredManagedPartition>,
    pub indexes: Vec<StoredManagedIndex>,
    pub tablets: Vec<StoredManagedTablet>,
    #[cfg(test)]
    pub txns: Vec<StoredManagedTxn>,
    #[cfg(test)]
    pub erase_jobs: Vec<StoredManagedEraseJob>,
    #[cfg(test)]
    pub materialized_views: Vec<StoredMaterializedView>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ManagedGlobalMeta {
    pub warehouse_uri: String,
    pub next_db_id: i64,
    pub next_table_id: i64,
    pub next_partition_id: i64,
    pub next_index_id: i64,
    pub next_tablet_id: i64,
    pub next_txn_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredManagedDatabase {
    pub db_id: i64,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredManagedTable {
    pub table_id: i64,
    pub db_id: i64,
    pub name: String,
    pub keys_type: String,
    pub bucket_num: i64,
    pub current_schema_id: i64,
    pub state: ManagedTableState,
    pub kind: ManagedTableKind,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ManagedTableKind {
    #[default]
    Table,
    MaterializedView,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ManagedMvRefreshMode {
    #[default]
    DeferredManual,
}

impl ManagedMvRefreshMode {
    pub(crate) fn as_sql_str(self) -> &'static str {
        match self {
            Self::DeferredManual => "DEFERRED_MANUAL",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct IcebergTableRef {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

impl IcebergTableRef {
    pub(crate) fn fqn(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.namespace, self.table)
    }
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredMaterializedView {
    pub mv_id: i64,
    pub select_sql: String,
    pub refresh_mode: ManagedMvRefreshMode,
    pub base_table_refs: Vec<IcebergTableRef>,
    pub last_refresh_ms: Option<i64>,
    pub last_refresh_rows: Option<i64>,
    pub last_refresh_snapshots: std::collections::BTreeMap<String, i64>,
    pub last_refresh_table_uuids: std::collections::BTreeMap<String, String>,
    pub primary_key_columns: Vec<String>,
    pub created_at_ms: i64,
    pub storage_engine: ManagedMvStorageEngine,
    pub iceberg_table_identifier: Option<String>,
    pub target_catalog: Option<String>,
    pub target_namespace: Option<String>,
    pub target_table: Option<String>,
    pub last_refreshed_iceberg_snapshot_id: Option<i64>,
    pub refresh_in_progress: bool,
    pub refresh_target_snapshots: std::collections::BTreeMap<String, i64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ManagedMvStorageEngine {
    ManagedLake,
    Iceberg,
}

impl ManagedMvStorageEngine {
    pub(crate) fn as_sql_str(self) -> &'static str {
        match self {
            Self::ManagedLake => "managed_lake",
            Self::Iceberg => "iceberg",
        }
    }

    pub(crate) fn parse_sql_str(value: &str) -> Result<Self, String> {
        match value.to_ascii_lowercase().as_str() {
            "managed_lake" => Ok(Self::ManagedLake),
            "iceberg" => Ok(Self::Iceberg),
            _ => Err(format!(
                "unknown materialized view storage_engine `{value}`"
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredManagedSchema {
    pub schema_id: i64,
    pub table_id: i64,
    pub schema_version: i64,
    pub tablet_schema_pb: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredManagedColumn {
    pub schema_id: i64,
    pub ordinal: i64,
    pub column_name: String,
    pub logical_type: String,
    pub nullable: bool,
    pub visible: bool,
    pub is_key: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredManagedPartition {
    pub partition_id: i64,
    pub table_id: i64,
    pub name: String,
    pub visible_version: i64,
    pub next_version: i64,
    pub state: ManagedPartitionState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredManagedIndex {
    pub index_id: i64,
    pub table_id: i64,
    pub partition_id: i64,
    pub index_type: String,
    pub state: ManagedIndexState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredManagedTablet {
    pub tablet_id: i64,
    pub partition_id: i64,
    pub index_id: i64,
    pub bucket_seq: i64,
    pub tablet_root_path: String,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredManagedTxn {
    pub txn_id: i64,
    pub table_id: i64,
    pub partition_id: i64,
    pub base_version: i64,
    pub commit_version: i64,
    pub state: ManagedTxnState,
    pub retry_at_ms: Option<i64>,
    pub updated_at_ms: i64,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredManagedEraseJob {
    pub job_id: i64,
    pub job_kind: ManagedEraseJobKind,
    pub table_id: i64,
    pub partition_id: Option<i64>,
    pub root_path: String,
    pub state: ManagedEraseJobState,
    pub retry_at_ms: Option<i64>,
    pub updated_at_ms: i64,
    pub last_error: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ManagedTableState {
    Creating,
    #[default]
    Active,
    Dropping,
    Failed,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ManagedPartitionState {
    Creating,
    #[default]
    Active,
    Retired,
    Failed,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ManagedIndexState {
    Creating,
    #[default]
    Active,
    Retired,
    Failed,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ManagedEraseJobKind {
    DropTable,
    DropPartition,
}

#[cfg(test)]
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ManagedEraseJobState {
    Pending,
    Running,
    Failed,
    Finished,
}

#[cfg(test)]
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ManagedTxnState {
    #[default]
    Prepared,
    Written,
    Visible,
    Aborted,
}
