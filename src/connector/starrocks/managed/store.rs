use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, params};

#[derive(Clone, Debug)]
pub(crate) struct SqliteMetadataStore {
    path: PathBuf,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct MetadataSnapshot {
    pub local_databases: Vec<String>,
    pub managed: ManagedSnapshot,
    pub iceberg_catalogs: Vec<StoredIcebergCatalog>,
    pub iceberg_namespaces: Vec<StoredIcebergNamespace>,
    pub iceberg_tables: Vec<StoredIcebergTable>,
}

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
    pub txns: Vec<StoredManagedTxn>,
    pub erase_jobs: Vec<StoredManagedEraseJob>,
    pub materialized_views: Vec<StoredMaterializedView>,
}

impl ManagedSnapshot {
    fn is_empty(&self) -> bool {
        self.global == ManagedGlobalMeta::default()
            && self.databases.is_empty()
            && self.tables.is_empty()
            && self.schemas.is_empty()
            && self.columns.is_empty()
            && self.partitions.is_empty()
            && self.indexes.is_empty()
            && self.tablets.is_empty()
            && self.txns.is_empty()
            && self.erase_jobs.is_empty()
            && self.materialized_views.is_empty()
    }
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

impl ManagedTableKind {
    pub(crate) fn as_sql_str(self) -> &'static str {
        match self {
            Self::Table => "TABLE",
            Self::MaterializedView => "MATERIALIZED_VIEW",
        }
    }

    fn from_sql_str(value: &str) -> Result<Self, String> {
        match value {
            "TABLE" => Ok(Self::Table),
            "MATERIALIZED_VIEW" => Ok(Self::MaterializedView),
            _ => Err(format!("unknown managed table kind `{value}`")),
        }
    }
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

    fn from_sql_str(value: &str) -> Result<Self, String> {
        match value {
            "DEFERRED_MANUAL" => Ok(Self::DeferredManual),
            _ => Err(format!("unknown managed mv refresh mode `{value}`")),
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredMaterializedView {
    pub mv_id: i64,
    pub select_sql: String,
    pub refresh_mode: ManagedMvRefreshMode,
    pub base_table_refs: Vec<IcebergTableRef>,
    pub last_refresh_ms: Option<i64>,
    pub last_refresh_rows: Option<i64>,
    pub last_refresh_snapshots: std::collections::BTreeMap<String, i64>,
    pub created_at_ms: i64,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PreparedManagedTxn {
    pub txn_id: i64,
    pub table_id: i64,
    pub partition_id: i64,
    pub base_version: i64,
    pub commit_version: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StageManagedTruncateRequest {
    pub table_id: i64,
    pub db_id: i64,
    pub bucket_num: i64,
    pub partition_name: String,
    pub warehouse_uri: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StagedManagedTruncate {
    pub partition_id: i64,
    pub index_id: i64,
    pub tablet_ids: Vec<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StageMvRefreshRequest {
    pub table_id: i64,
    pub db_id: i64,
    pub bucket_num: i64,
    pub partition_name: String,
    pub warehouse_uri: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StagedMvRefresh {
    pub partition_id: i64,
    pub index_id: i64,
    pub tablet_ids: Vec<i64>,
    pub partition_root_path: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ActivateMvRefreshRequest {
    pub table_id: i64,
    pub old_partition_id: i64,
    pub new_partition_id: i64,
    pub new_index_id: i64,
    pub retired_root_path: String,
    pub rows_written: i64,
    pub snapshots: std::collections::BTreeMap<String, i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct UpdateMvRefreshMetadataRequest {
    pub table_id: i64,
    pub last_refresh_rows: i64,
    pub snapshots: std::collections::BTreeMap<String, i64>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ManagedTableState {
    Creating,
    #[default]
    Active,
    Dropping,
    Failed,
}

impl ManagedTableState {
    fn as_sql_str(self) -> &'static str {
        match self {
            Self::Creating => "CREATING",
            Self::Active => "ACTIVE",
            Self::Dropping => "DROPPING",
            Self::Failed => "FAILED",
        }
    }

    fn from_sql_str(value: &str) -> Result<Self, String> {
        match value {
            "CREATING" => Ok(Self::Creating),
            "ACTIVE" => Ok(Self::Active),
            "DROPPING" => Ok(Self::Dropping),
            "FAILED" => Ok(Self::Failed),
            _ => Err(format!("unknown managed table state `{value}`")),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ManagedPartitionState {
    Creating,
    #[default]
    Active,
    Retired,
    Failed,
}

impl ManagedPartitionState {
    fn as_sql_str(self) -> &'static str {
        match self {
            Self::Creating => "CREATING",
            Self::Active => "ACTIVE",
            Self::Retired => "RETIRED",
            Self::Failed => "FAILED",
        }
    }

    fn from_sql_str(value: &str) -> Result<Self, String> {
        match value {
            "CREATING" => Ok(Self::Creating),
            "ACTIVE" => Ok(Self::Active),
            "RETIRED" => Ok(Self::Retired),
            "FAILED" => Ok(Self::Failed),
            _ => Err(format!("unknown managed partition state `{value}`")),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ManagedIndexState {
    Creating,
    #[default]
    Active,
    Retired,
    Failed,
}

impl ManagedIndexState {
    fn as_sql_str(self) -> &'static str {
        match self {
            Self::Creating => "CREATING",
            Self::Active => "ACTIVE",
            Self::Retired => "RETIRED",
            Self::Failed => "FAILED",
        }
    }

    fn from_sql_str(value: &str) -> Result<Self, String> {
        match value {
            "CREATING" => Ok(Self::Creating),
            "ACTIVE" => Ok(Self::Active),
            "RETIRED" => Ok(Self::Retired),
            "FAILED" => Ok(Self::Failed),
            _ => Err(format!("unknown managed index state `{value}`")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ManagedEraseJobKind {
    DropTable,
    DropPartition,
}

impl ManagedEraseJobKind {
    fn as_sql_str(self) -> &'static str {
        match self {
            Self::DropTable => "DROP_TABLE",
            Self::DropPartition => "DROP_PARTITION",
        }
    }

    fn from_sql_str(value: &str) -> Result<Self, String> {
        match value {
            "DROP_TABLE" => Ok(Self::DropTable),
            "DROP_PARTITION" => Ok(Self::DropPartition),
            _ => Err(format!("unknown managed erase job kind `{value}`")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ManagedEraseJobState {
    Pending,
    Running,
    Failed,
    Finished,
}

impl ManagedEraseJobState {
    fn as_sql_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Failed => "FAILED",
            Self::Finished => "FINISHED",
        }
    }

    fn from_sql_str(value: &str) -> Result<Self, String> {
        match value {
            "PENDING" => Ok(Self::Pending),
            "RUNNING" => Ok(Self::Running),
            "FAILED" => Ok(Self::Failed),
            "FINISHED" => Ok(Self::Finished),
            _ => Err(format!("unknown managed erase job state `{value}`")),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ManagedTxnState {
    #[default]
    Prepared,
    Written,
    Visible,
    Aborted,
}

impl ManagedTxnState {
    fn as_sql_str(self) -> &'static str {
        match self {
            Self::Prepared => "PREPARED",
            Self::Written => "WRITTEN",
            Self::Visible => "VISIBLE",
            Self::Aborted => "ABORTED",
        }
    }

    fn from_sql_str(value: &str) -> Result<Self, String> {
        match value {
            "PREPARED" => Ok(Self::Prepared),
            "WRITTEN" => Ok(Self::Written),
            "VISIBLE" => Ok(Self::Visible),
            "ABORTED" => Ok(Self::Aborted),
            _ => Err(format!("unknown managed txn state `{value}`")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredIcebergCatalog {
    pub name: String,
    pub properties: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredIcebergNamespace {
    pub catalog: String,
    pub namespace: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredIcebergTable {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

impl SqliteMetadataStore {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|e| {
                format!(
                    "create standalone metadata directory {} failed: {e}",
                    parent.display()
                )
            })?;
        }
        let store = Self { path };
        store.init_schema()?;
        Ok(store)
    }

    pub(crate) fn load_snapshot(&self) -> Result<MetadataSnapshot, String> {
        let conn = self.connection()?;
        let local_databases =
            query_single_text_column(&conn, "SELECT name FROM local_databases ORDER BY name", [])?;
        let managed = self.load_managed_snapshot(&conn)?;

        let iceberg_catalogs = {
            let mut stmt = conn
                .prepare(
                    "SELECT name, properties_json
                     FROM iceberg_catalogs
                     ORDER BY name",
                )
                .map_err(|e| format!("prepare iceberg_catalogs query failed: {e}"))?;
            let rows = stmt
                .query_map([], |row| {
                    let properties_json = row.get::<_, String>(1)?;
                    let properties =
                        serde_json::from_str::<Vec<(String, String)>>(&properties_json)
                            .map_err(json_to_sql_error)?;
                    Ok(StoredIcebergCatalog {
                        name: row.get(0)?,
                        properties,
                    })
                })
                .map_err(|e| format!("query iceberg_catalogs failed: {e}"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("read iceberg_catalogs failed: {e}"))?
        };

        let iceberg_namespaces = {
            let mut stmt = conn
                .prepare(
                    "SELECT catalog_name, namespace_name
                     FROM iceberg_namespaces
                     ORDER BY catalog_name, namespace_name",
                )
                .map_err(|e| format!("prepare iceberg_namespaces query failed: {e}"))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok(StoredIcebergNamespace {
                        catalog: row.get(0)?,
                        namespace: row.get(1)?,
                    })
                })
                .map_err(|e| format!("query iceberg_namespaces failed: {e}"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("read iceberg_namespaces failed: {e}"))?
        };

        let iceberg_tables = {
            let mut stmt = conn
                .prepare(
                    "SELECT catalog_name, namespace_name, table_name
                     FROM iceberg_tables
                     ORDER BY catalog_name, namespace_name, table_name",
                )
                .map_err(|e| format!("prepare iceberg_tables query failed: {e}"))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok(StoredIcebergTable {
                        catalog: row.get(0)?,
                        namespace: row.get(1)?,
                        table: row.get(2)?,
                    })
                })
                .map_err(|e| format!("query iceberg_tables failed: {e}"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("read iceberg_tables failed: {e}"))?
        };

        Ok(MetadataSnapshot {
            local_databases,
            managed,
            iceberg_catalogs,
            iceberg_namespaces,
            iceberg_tables,
        })
    }

    pub(crate) fn upsert_local_database(&self, database_name: &str) -> Result<(), String> {
        let conn = self.connection()?;
        conn.execute(
            "INSERT INTO local_databases(name) VALUES (?1)
             ON CONFLICT(name) DO NOTHING",
            params![database_name],
        )
        .map_err(|e| format!("persist local database failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn delete_local_database(&self, database_name: &str) -> Result<(), String> {
        let conn = self.connection()?;
        conn.execute(
            "DELETE FROM local_databases WHERE name = ?1",
            params![database_name],
        )
        .map_err(|e| format!("delete local database metadata failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn replace_managed_snapshot(
        &self,
        snapshot: &ManagedSnapshot,
    ) -> Result<(), String> {
        let conn = self.connection()?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| format!("begin managed snapshot transaction failed: {e}"))?;

        for table in [
            "materialized_views",
            "erase_jobs",
            "txns",
            "tablets",
            "indexes",
            "partitions",
            "table_columns",
            "table_schemas",
            "tables",
            "databases",
            "global_meta",
        ] {
            tx.execute(&format!("DELETE FROM {table}"), [])
                .map_err(|e| format!("clear managed table `{table}` failed: {e}"))?;
        }

        if !snapshot.is_empty() {
            tx.execute(
                "INSERT INTO global_meta(
                    singleton,
                    warehouse_uri,
                    next_db_id,
                    next_table_id,
                    next_partition_id,
                    next_index_id,
                    next_tablet_id,
                    next_txn_id
                ) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    snapshot.global.warehouse_uri,
                    snapshot.global.next_db_id,
                    snapshot.global.next_table_id,
                    snapshot.global.next_partition_id,
                    snapshot.global.next_index_id,
                    snapshot.global.next_tablet_id,
                    snapshot.global.next_txn_id,
                ],
            )
            .map_err(|e| format!("persist managed global metadata failed: {e}"))?;

            for database in &snapshot.databases {
                tx.execute(
                    "INSERT INTO databases(db_id, name) VALUES (?1, ?2)",
                    params![database.db_id, database.name],
                )
                .map_err(|e| format!("persist managed database failed: {e}"))?;
            }

            for table in &snapshot.tables {
                tx.execute(
                    "INSERT INTO tables(
                        table_id,
                        db_id,
                        name,
                        keys_type,
                        bucket_num,
                        current_schema_id,
                        state,
                        kind
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        table.table_id,
                        table.db_id,
                        table.name,
                        table.keys_type,
                        table.bucket_num,
                        table.current_schema_id,
                        table.state.as_sql_str(),
                        table.kind.as_sql_str(),
                    ],
                )
                .map_err(|e| format!("persist managed table failed: {e}"))?;
            }

            for schema in &snapshot.schemas {
                tx.execute(
                    "INSERT INTO table_schemas(schema_id, table_id, schema_version, tablet_schema_pb)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![
                        schema.schema_id,
                        schema.table_id,
                        schema.schema_version,
                        schema.tablet_schema_pb,
                    ],
                )
                .map_err(|e| format!("persist managed schema failed: {e}"))?;
            }

            for column in &snapshot.columns {
                tx.execute(
                    "INSERT INTO table_columns(
                        schema_id,
                        ordinal,
                        column_name,
                        logical_type,
                        nullable
                    ) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        column.schema_id,
                        column.ordinal,
                        column.column_name,
                        column.logical_type,
                        bool_to_sql_int(column.nullable),
                    ],
                )
                .map_err(|e| format!("persist managed column failed: {e}"))?;
            }

            for partition in &snapshot.partitions {
                tx.execute(
                    "INSERT INTO partitions(
                        partition_id,
                        table_id,
                        name,
                        visible_version,
                        next_version,
                        state
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        partition.partition_id,
                        partition.table_id,
                        partition.name,
                        partition.visible_version,
                        partition.next_version,
                        partition.state.as_sql_str(),
                    ],
                )
                .map_err(|e| format!("persist managed partition failed: {e}"))?;
            }

            for index in &snapshot.indexes {
                tx.execute(
                    "INSERT INTO indexes(index_id, table_id, partition_id, index_type, state)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        index.index_id,
                        index.table_id,
                        index.partition_id,
                        index.index_type,
                        index.state.as_sql_str(),
                    ],
                )
                .map_err(|e| format!("persist managed index failed: {e}"))?;
            }

            for tablet in &snapshot.tablets {
                tx.execute(
                    "INSERT INTO tablets(
                        tablet_id,
                        partition_id,
                        index_id,
                        bucket_seq,
                        tablet_root_path
                    ) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        tablet.tablet_id,
                        tablet.partition_id,
                        tablet.index_id,
                        tablet.bucket_seq,
                        tablet.tablet_root_path,
                    ],
                )
                .map_err(|e| format!("persist managed tablet failed: {e}"))?;
            }

            for txn in &snapshot.txns {
                tx.execute(
                    "INSERT INTO txns(
                        txn_id,
                        table_id,
                        partition_id,
                        base_version,
                        commit_version,
                        state,
                        retry_at_ms,
                        updated_at_ms
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        txn.txn_id,
                        txn.table_id,
                        txn.partition_id,
                        txn.base_version,
                        txn.commit_version,
                        txn.state.as_sql_str(),
                        txn.retry_at_ms,
                        txn.updated_at_ms,
                    ],
                )
                .map_err(|e| format!("persist managed txn failed: {e}"))?;
            }

            for erase_job in &snapshot.erase_jobs {
                tx.execute(
                    "INSERT INTO erase_jobs(
                        job_id,
                        job_kind,
                        table_id,
                        partition_id,
                        root_path,
                        state,
                        retry_at_ms,
                        updated_at_ms,
                        last_error
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        erase_job.job_id,
                        erase_job.job_kind.as_sql_str(),
                        erase_job.table_id,
                        erase_job.partition_id,
                        erase_job.root_path,
                        erase_job.state.as_sql_str(),
                        erase_job.retry_at_ms,
                        erase_job.updated_at_ms,
                        erase_job.last_error,
                    ],
                )
                .map_err(|e| format!("persist managed erase job failed: {e}"))?;
            }

            for mv in &snapshot.materialized_views {
                let base_json = serde_json::to_string(&mv.base_table_refs)
                    .map_err(|e| format!("serialize mv base refs failed: {e}"))?;
                let snapshots_json = if mv.last_refresh_snapshots.is_empty() {
                    None
                } else {
                    Some(
                        serde_json::to_string(&mv.last_refresh_snapshots)
                            .map_err(|e| format!("serialize mv snapshots failed: {e}"))?,
                    )
                };
                tx.execute(
                    "INSERT INTO materialized_views(
                        mv_id,
                        select_sql,
                        refresh_mode,
                        base_table_refs_json,
                        last_refresh_ms,
                        last_refresh_rows,
                        last_refresh_snapshots_json,
                        created_at_ms
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        mv.mv_id,
                        mv.select_sql,
                        mv.refresh_mode.as_sql_str(),
                        base_json,
                        mv.last_refresh_ms,
                        mv.last_refresh_rows,
                        snapshots_json,
                        mv.created_at_ms,
                    ],
                )
                .map_err(|e| format!("insert materialized_view failed: {e}"))?;
            }
        }

        tx.commit()
            .map_err(|e| format!("commit managed snapshot failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn drop_managed_table(&self, table_id: i64, root_path: &str) -> Result<(), String> {
        let conn = self.connection()?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| format!("begin drop_managed_table transaction failed: {e}"))?;
        let inflight_txn_count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM txns
                 WHERE table_id = ?1 AND state IN ('PREPARED', 'WRITTEN')",
                params![table_id],
                |row| row.get(0),
            )
            .map_err(|e| format!("count inflight txns for table {table_id} failed: {e}"))?;
        if inflight_txn_count > 0 {
            return Err(format!(
                "cannot drop managed table {table_id}: inflight managed txns exist"
            ));
        }
        let creating_count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM partitions
                 WHERE table_id = ?1 AND state = 'CREATING'",
                params![table_id],
                |row| row.get(0),
            )
            .map_err(|e| format!("count creating partitions for drop failed: {e}"))?;
        if creating_count > 0 {
            return Err(format!("cannot drop table {table_id}: refresh in progress"));
        }
        let next_job_id: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(job_id), 0) + 1 FROM erase_jobs",
                [],
                |row| row.get(0),
            )
            .map_err(|e| format!("allocate erase job id failed: {e}"))?;
        tx.execute(
            "UPDATE tables SET state = 'DROPPING' WHERE table_id = ?1",
            params![table_id],
        )
        .map_err(|e| format!("mark managed table dropping failed: {e}"))?;
        tx.execute(
            "UPDATE partitions SET state = 'RETIRED'
             WHERE table_id = ?1 AND state = 'ACTIVE'",
            params![table_id],
        )
        .map_err(|e| format!("retire managed partitions failed: {e}"))?;
        tx.execute(
            "UPDATE indexes SET state = 'RETIRED'
             WHERE table_id = ?1 AND state = 'ACTIVE'",
            params![table_id],
        )
        .map_err(|e| format!("retire managed indexes failed: {e}"))?;
        tx.execute(
            "INSERT INTO erase_jobs(
                job_id,
                job_kind,
                table_id,
                partition_id,
                root_path,
                state,
                retry_at_ms,
                updated_at_ms,
                last_error
            ) VALUES (?1, 'DROP_TABLE', ?2, NULL, ?3, 'PENDING', NULL, strftime('%s','now') * 1000, NULL)",
            params![next_job_id, table_id, root_path],
        )
        .map_err(|e| format!("insert drop-table erase job failed: {e}"))?;
        tx.commit()
            .map_err(|e| format!("commit drop_managed_table failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn stage_truncate_partition(
        &self,
        req: StageManagedTruncateRequest,
    ) -> Result<StagedManagedTruncate, String> {
        let conn = self.connection()?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| format!("begin stage_truncate_partition transaction failed: {e}"))?;
        let inflight_txn_count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM txns
                 WHERE table_id = ?1 AND state IN ('PREPARED', 'WRITTEN')",
                params![req.table_id],
                |row| row.get(0),
            )
            .map_err(|e| format!("count inflight txns for truncate failed: {e}"))?;
        if inflight_txn_count > 0 {
            return Err(format!(
                "cannot truncate managed table {} while inflight managed txns exist",
                req.table_id
            ));
        }
        let partition_id: i64 = tx
            .query_row(
                "SELECT next_partition_id FROM global_meta WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|e| format!("read next_partition_id failed: {e}"))?;
        let index_id: i64 = tx
            .query_row(
                "SELECT next_index_id FROM global_meta WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|e| format!("read next_index_id failed: {e}"))?;
        let first_tablet_id: i64 = tx
            .query_row(
                "SELECT next_tablet_id FROM global_meta WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|e| format!("read next_tablet_id failed: {e}"))?;
        tx.execute(
            "UPDATE global_meta
             SET next_partition_id = ?1, next_index_id = ?2, next_tablet_id = ?3
             WHERE singleton = 1",
            params![
                partition_id + 1,
                index_id + 1,
                first_tablet_id + req.bucket_num
            ],
        )
        .map_err(|e| format!("bump truncate ids failed: {e}"))?;
        tx.execute(
            "INSERT INTO partitions(
                partition_id,
                table_id,
                name,
                visible_version,
                next_version,
                state
            ) VALUES (?1, ?2, ?3, 1, 2, 'CREATING')",
            params![partition_id, req.table_id, req.partition_name],
        )
        .map_err(|e| format!("insert creating partition failed: {e}"))?;
        tx.execute(
            "INSERT INTO indexes(index_id, table_id, partition_id, index_type, state)
             VALUES (?1, ?2, ?3, 'BASE', 'CREATING')",
            params![index_id, req.table_id, partition_id],
        )
        .map_err(|e| format!("insert creating index failed: {e}"))?;
        let partition_root_path = format!(
            "{}/db_{}/table_{}/partition_{}",
            req.warehouse_uri.trim_end_matches('/'),
            req.db_id,
            req.table_id,
            partition_id
        );
        let mut tablet_ids = Vec::new();
        for bucket_seq in 0..req.bucket_num {
            let tablet_id = first_tablet_id + bucket_seq;
            tx.execute(
                "INSERT INTO tablets(tablet_id, partition_id, index_id, bucket_seq, tablet_root_path)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    tablet_id,
                    partition_id,
                    index_id,
                    bucket_seq,
                    partition_root_path,
                ],
            )
            .map_err(|e| format!("insert creating tablet failed: {e}"))?;
            tablet_ids.push(tablet_id);
        }
        tx.commit()
            .map_err(|e| format!("commit stage_truncate_partition failed: {e}"))?;
        Ok(StagedManagedTruncate {
            partition_id,
            index_id,
            tablet_ids,
        })
    }

    pub(crate) fn activate_truncate_partition(
        &self,
        table_id: i64,
        old_partition_id: i64,
        new_partition_id: i64,
        new_index_id: i64,
        retired_root_path: &str,
    ) -> Result<(), String> {
        let conn = self.connection()?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| format!("begin activate_truncate_partition transaction failed: {e}"))?;
        let next_job_id: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(job_id), 0) + 1 FROM erase_jobs",
                [],
                |row| row.get(0),
            )
            .map_err(|e| format!("allocate erase job id failed: {e}"))?;
        tx.execute(
            "UPDATE partitions
             SET state = CASE
                 WHEN partition_id = ?1 THEN 'ACTIVE'
                 WHEN partition_id = ?2 THEN 'RETIRED'
                 ELSE state
             END
             WHERE table_id = ?3",
            params![new_partition_id, old_partition_id, table_id],
        )
        .map_err(|e| format!("switch partition states failed: {e}"))?;
        tx.execute(
            "UPDATE indexes
             SET state = CASE
                 WHEN index_id = ?1 THEN 'ACTIVE'
                 WHEN partition_id = ?2 THEN 'RETIRED'
                 ELSE state
             END
             WHERE table_id = ?3",
            params![new_index_id, old_partition_id, table_id],
        )
        .map_err(|e| format!("switch index states failed: {e}"))?;
        tx.execute(
            "INSERT INTO erase_jobs(
                job_id,
                job_kind,
                table_id,
                partition_id,
                root_path,
                state,
                retry_at_ms,
                updated_at_ms,
                last_error
            ) VALUES (?1, 'DROP_PARTITION', ?2, ?3, ?4, 'PENDING', NULL, strftime('%s','now') * 1000, NULL)",
            params![next_job_id, table_id, old_partition_id, retired_root_path],
        )
        .map_err(|e| format!("insert truncate erase job failed: {e}"))?;
        tx.commit()
            .map_err(|e| format!("commit activate_truncate_partition failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn stage_mv_refresh_partition(
        &self,
        req: StageMvRefreshRequest,
    ) -> Result<StagedMvRefresh, String> {
        let conn = self.connection()?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| format!("begin stage_mv_refresh_partition transaction failed: {e}"))?;

        // Reject if MV is not active.
        let (table_state, table_kind): (String, String) = tx
            .query_row(
                "SELECT state, kind FROM tables WHERE table_id = ?1",
                params![req.table_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|e| format!("lookup mv table {} failed: {e}", req.table_id))?;
        if table_kind != "MATERIALIZED_VIEW" {
            return Err(format!("table {} is not a materialized view", req.table_id));
        }
        if table_state != "ACTIVE" {
            return Err(format!(
                "materialized view {} is not active (state={table_state})",
                req.table_id
            ));
        }

        // Reject if a refresh is already in progress (any CREATING partition).
        let creating_count: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM partitions
                 WHERE table_id = ?1 AND state = 'CREATING'",
                params![req.table_id],
                |row| row.get(0),
            )
            .map_err(|e| format!("count creating partitions failed: {e}"))?;
        if creating_count > 0 {
            return Err(format!(
                "cannot refresh materialized view {}: refresh already in progress",
                req.table_id
            ));
        }

        let partition_id: i64 = tx
            .query_row(
                "SELECT next_partition_id FROM global_meta WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|e| format!("read next_partition_id failed: {e}"))?;
        let index_id: i64 = tx
            .query_row(
                "SELECT next_index_id FROM global_meta WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|e| format!("read next_index_id failed: {e}"))?;
        let first_tablet_id: i64 = tx
            .query_row(
                "SELECT next_tablet_id FROM global_meta WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|e| format!("read next_tablet_id failed: {e}"))?;

        tx.execute(
            "UPDATE global_meta
             SET next_partition_id = ?1, next_index_id = ?2, next_tablet_id = ?3
             WHERE singleton = 1",
            params![
                partition_id + 1,
                index_id + 1,
                first_tablet_id + req.bucket_num
            ],
        )
        .map_err(|e| format!("bump mv refresh ids failed: {e}"))?;

        tx.execute(
            "INSERT INTO partitions(partition_id, table_id, name, visible_version, next_version, state)
             VALUES (?1, ?2, ?3, 1, 2, 'CREATING')",
            params![partition_id, req.table_id, req.partition_name],
        )
        .map_err(|e| format!("insert mv creating partition failed: {e}"))?;
        tx.execute(
            "INSERT INTO indexes(index_id, table_id, partition_id, index_type, state)
             VALUES (?1, ?2, ?3, 'BASE', 'CREATING')",
            params![index_id, req.table_id, partition_id],
        )
        .map_err(|e| format!("insert mv creating index failed: {e}"))?;

        let partition_root_path = format!(
            "{}/db_{}/table_{}/partition_{}",
            req.warehouse_uri.trim_end_matches('/'),
            req.db_id,
            req.table_id,
            partition_id
        );
        let mut tablet_ids = Vec::new();
        for bucket_seq in 0..req.bucket_num {
            let tablet_id = first_tablet_id + bucket_seq;
            tx.execute(
                "INSERT INTO tablets(tablet_id, partition_id, index_id, bucket_seq, tablet_root_path)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    tablet_id,
                    partition_id,
                    index_id,
                    bucket_seq,
                    partition_root_path,
                ],
            )
            .map_err(|e| format!("insert mv creating tablet failed: {e}"))?;
            tablet_ids.push(tablet_id);
        }

        tx.commit()
            .map_err(|e| format!("commit stage_mv_refresh_partition failed: {e}"))?;

        Ok(StagedMvRefresh {
            partition_id,
            index_id,
            tablet_ids,
            partition_root_path,
        })
    }

    pub(crate) fn activate_mv_refresh_partition(
        &self,
        req: ActivateMvRefreshRequest,
    ) -> Result<(), String> {
        let conn = self.connection()?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| format!("begin activate_mv_refresh_partition transaction failed: {e}"))?;

        let next_job_id: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(job_id), 0) + 1 FROM erase_jobs",
                [],
                |row| row.get(0),
            )
            .map_err(|e| format!("allocate erase job id failed: {e}"))?;

        tx.execute(
            "UPDATE partitions
             SET state = CASE
                 WHEN partition_id = ?1 THEN 'ACTIVE'
                 WHEN partition_id = ?2 THEN 'RETIRED'
                 ELSE state
             END,
                 visible_version = CASE
                 WHEN partition_id = ?1 THEN 2
                 ELSE visible_version
             END,
                 next_version = CASE
                 WHEN partition_id = ?1 THEN 3
                 ELSE next_version
             END
             WHERE table_id = ?3",
            params![req.new_partition_id, req.old_partition_id, req.table_id],
        )
        .map_err(|e| format!("switch mv partition states failed: {e}"))?;
        tx.execute(
            "UPDATE indexes
             SET state = CASE
                 WHEN index_id = ?1 THEN 'ACTIVE'
                 WHEN partition_id = ?2 THEN 'RETIRED'
                 ELSE state
             END
             WHERE table_id = ?3",
            params![req.new_index_id, req.old_partition_id, req.table_id],
        )
        .map_err(|e| format!("switch mv index states failed: {e}"))?;

        tx.execute(
            "INSERT INTO erase_jobs(
                job_id,
                job_kind,
                table_id,
                partition_id,
                root_path,
                state,
                retry_at_ms,
                updated_at_ms,
                last_error
             ) VALUES (?1, 'DROP_PARTITION', ?2, ?3, ?4, 'PENDING', NULL, strftime('%s','now') * 1000, NULL)",
            params![
                next_job_id,
                req.table_id,
                req.old_partition_id,
                req.retired_root_path,
            ],
        )
        .map_err(|e| format!("insert mv refresh erase job failed: {e}"))?;

        let snapshots_json = if req.snapshots.is_empty() {
            None
        } else {
            Some(
                serde_json::to_string(&req.snapshots)
                    .map_err(|e| format!("serialize mv activate snapshots failed: {e}"))?,
            )
        };
        tx.execute(
            "UPDATE materialized_views
             SET last_refresh_ms = strftime('%s','now') * 1000,
                 last_refresh_rows = ?1,
                 last_refresh_snapshots_json = ?2
             WHERE mv_id = ?3",
            params![req.rows_written, snapshots_json, req.table_id],
        )
        .map_err(|e| format!("update materialized_view last_refresh fields failed: {e}"))?;

        tx.commit()
            .map_err(|e| format!("commit activate_mv_refresh_partition failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn update_mv_refresh_metadata(
        &self,
        req: UpdateMvRefreshMetadataRequest,
    ) -> Result<(), String> {
        let conn = self.connection()?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| format!("begin update_mv_refresh_metadata transaction failed: {e}"))?;
        update_mv_refresh_metadata_in_tx(&tx, &req)
            .map_err(|e| format!("update mv refresh metadata failed: {e}"))?;
        tx.commit()
            .map_err(|e| format!("commit update_mv_refresh_metadata failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn enqueue_erase_job_for_partition_root(
        &self,
        table_id: i64,
        partition_id: i64,
        root_path: &str,
    ) -> Result<(), String> {
        let conn = self.connection()?;
        let tx = conn.unchecked_transaction().map_err(|e| {
            format!("begin enqueue_erase_job_for_partition_root transaction failed: {e}")
        })?;
        let next_job_id: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(job_id), 0) + 1 FROM erase_jobs",
                [],
                |row| row.get(0),
            )
            .map_err(|e| format!("allocate erase job id failed: {e}"))?;
        tx.execute(
            "INSERT INTO erase_jobs(
                job_id,
                job_kind,
                table_id,
                partition_id,
                root_path,
                state,
                retry_at_ms,
                updated_at_ms,
                last_error
             ) VALUES (?1, 'DROP_PARTITION', ?2, ?3, ?4, 'PENDING', NULL, strftime('%s','now') * 1000, NULL)",
            params![next_job_id, table_id, partition_id, root_path],
        )
        .map_err(|e| format!("insert erase job for partition root failed: {e}"))?;
        tx.commit()
            .map_err(|e| format!("commit enqueue_erase_job_for_partition_root failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn prepare_txn(
        &self,
        table_id: i64,
        partition_id: i64,
        base_version: i64,
    ) -> Result<PreparedManagedTxn, String> {
        let conn = self.connection()?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| format!("begin prepare_txn transaction failed: {e}"))?;
        let txn_id: i64 = tx
            .query_row(
                "SELECT next_txn_id FROM global_meta WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .map_err(|e| format!("read next_txn_id failed: {e}"))?;
        let commit_version = base_version + 1;
        tx.execute(
            "UPDATE global_meta SET next_txn_id = ?1 WHERE singleton = 1",
            params![txn_id + 1],
        )
        .map_err(|e| format!("bump next_txn_id failed: {e}"))?;
        tx.execute(
            "INSERT INTO txns(
                txn_id, table_id, partition_id, base_version, commit_version, state, retry_at_ms, updated_at_ms
            ) VALUES (?1, ?2, ?3, ?4, ?5, 'PREPARED', NULL, strftime('%s','now') * 1000)",
            params![txn_id, table_id, partition_id, base_version, commit_version],
        )
        .map_err(|e| format!("insert prepared txn failed: {e}"))?;
        tx.commit()
            .map_err(|e| format!("commit prepare_txn failed: {e}"))?;
        Ok(PreparedManagedTxn {
            txn_id,
            table_id,
            partition_id,
            base_version,
            commit_version,
        })
    }

    pub(crate) fn mark_txn_written(&self, txn_id: i64) -> Result<(), String> {
        self.connection()?
            .execute(
                "UPDATE txns SET state = 'WRITTEN', updated_at_ms = strftime('%s','now') * 1000
                 WHERE txn_id = ?1",
                params![txn_id],
            )
            .map_err(|e| format!("mark txn written failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn mark_txn_visible(&self, txn_id: i64, commit_version: i64) -> Result<(), String> {
        let conn = self.connection()?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| format!("begin mark_txn_visible transaction failed: {e}"))?;
        let partition_id: i64 = tx
            .query_row(
                "SELECT partition_id FROM txns WHERE txn_id = ?1",
                params![txn_id],
                |row| row.get(0),
            )
            .map_err(|e| format!("load partition for txn {txn_id} failed: {e}"))?;
        tx.execute(
            "UPDATE txns SET state = 'VISIBLE', updated_at_ms = strftime('%s','now') * 1000
             WHERE txn_id = ?1",
            params![txn_id],
        )
        .map_err(|e| format!("mark txn visible failed: {e}"))?;
        tx.execute(
            "UPDATE partitions SET visible_version = ?1, next_version = ?2
             WHERE partition_id = ?3",
            params![commit_version, commit_version + 1, partition_id],
        )
        .map_err(|e| format!("advance partition version failed: {e}"))?;
        tx.commit()
            .map_err(|e| format!("commit mark_txn_visible failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn mark_txn_visible_with_mv_refresh_metadata(
        &self,
        txn_id: i64,
        commit_version: i64,
        req: UpdateMvRefreshMetadataRequest,
    ) -> Result<(), String> {
        let conn = self.connection()?;
        let tx = conn.unchecked_transaction().map_err(|e| {
            format!("begin mark_txn_visible_with_mv_refresh_metadata transaction failed: {e}")
        })?;
        let partition_id: i64 = tx
            .query_row(
                "SELECT partition_id FROM txns WHERE txn_id = ?1",
                params![txn_id],
                |row| row.get(0),
            )
            .map_err(|e| format!("load partition for txn {txn_id} failed: {e}"))?;
        tx.execute(
            "UPDATE txns SET state = 'VISIBLE', updated_at_ms = strftime('%s','now') * 1000
             WHERE txn_id = ?1",
            params![txn_id],
        )
        .map_err(|e| format!("mark txn visible failed: {e}"))?;
        tx.execute(
            "UPDATE partitions SET visible_version = ?1, next_version = ?2
             WHERE partition_id = ?3",
            params![commit_version, commit_version + 1, partition_id],
        )
        .map_err(|e| format!("advance partition version failed: {e}"))?;
        update_mv_refresh_metadata_in_tx(&tx, &req)
            .map_err(|e| format!("update mv refresh metadata failed: {e}"))?;
        tx.commit()
            .map_err(|e| format!("commit mark_txn_visible_with_mv_refresh_metadata failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn mark_txn_aborted(&self, txn_id: i64) -> Result<(), String> {
        self.connection()?
            .execute(
                "UPDATE txns SET state = 'ABORTED', updated_at_ms = strftime('%s','now') * 1000, retry_at_ms = NULL
                 WHERE txn_id = ?1",
                params![txn_id],
            )
            .map_err(|e| format!("mark txn aborted failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn mark_table_failed(&self, table_id: i64) -> Result<(), String> {
        self.connection()?
            .execute(
                "UPDATE tables SET state = 'FAILED' WHERE table_id = ?1",
                params![table_id],
            )
            .map_err(|e| format!("mark table failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn list_runnable_erase_jobs(
        &self,
        now_ms: i64,
    ) -> Result<Vec<StoredManagedEraseJob>, String> {
        let snapshot = self.load_snapshot()?;
        Ok(snapshot
            .managed
            .erase_jobs
            .into_iter()
            .filter(|job| {
                matches!(
                    job.state,
                    ManagedEraseJobState::Pending | ManagedEraseJobState::Failed
                ) && job
                    .retry_at_ms
                    .is_none_or(|retry_at_ms| retry_at_ms <= now_ms)
            })
            .collect())
    }

    pub(crate) fn claim_erase_job(&self, job_id: i64) -> Result<bool, String> {
        let changed = self
            .connection()?
            .execute(
                "UPDATE erase_jobs
                 SET state = 'RUNNING', updated_at_ms = strftime('%s','now') * 1000
                 WHERE job_id = ?1 AND state IN ('PENDING', 'FAILED')",
                params![job_id],
            )
            .map_err(|e| format!("claim erase job failed: {e}"))?;
        Ok(changed == 1)
    }

    pub(crate) fn finish_erase_job(&self, job_id: i64) -> Result<(), String> {
        self.connection()?
            .execute(
                "UPDATE erase_jobs
                 SET state = 'FINISHED',
                     updated_at_ms = strftime('%s','now') * 1000,
                     retry_at_ms = NULL,
                     last_error = NULL
                 WHERE job_id = ?1",
                params![job_id],
            )
            .map_err(|e| format!("finish erase job failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn fail_erase_job(
        &self,
        job_id: i64,
        last_error: &str,
        retry_at_ms: i64,
    ) -> Result<(), String> {
        self.connection()?
            .execute(
                "UPDATE erase_jobs
                 SET state = 'FAILED',
                     updated_at_ms = strftime('%s','now') * 1000,
                     retry_at_ms = ?2,
                     last_error = ?3
                 WHERE job_id = ?1",
                params![job_id, retry_at_ms, last_error],
            )
            .map_err(|e| format!("fail erase job failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn purge_retired_table_metadata(&self, table_id: i64) -> Result<(), String> {
        let conn = self.connection()?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| format!("begin purge_retired_table_metadata transaction failed: {e}"))?;
        let is_dropping: bool = tx
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM tables WHERE table_id = ?1 AND state = 'DROPPING'
                 )",
                params![table_id],
                |row| row.get(0),
            )
            .map_err(|e| format!("load dropping table state failed: {e}"))?;
        if !is_dropping {
            return Err(format!(
                "cannot purge managed table {table_id}: table is not in DROPPING state"
            ));
        }
        tx.execute("DELETE FROM txns WHERE table_id = ?1", params![table_id])
            .map_err(|e| format!("delete retired table txns failed: {e}"))?;
        tx.execute(
            "DELETE FROM tablets
             WHERE partition_id IN (
                 SELECT partition_id FROM partitions WHERE table_id = ?1
             )",
            params![table_id],
        )
        .map_err(|e| format!("delete retired table tablets failed: {e}"))?;
        tx.execute(
            "DELETE FROM table_columns
             WHERE schema_id IN (
                 SELECT schema_id FROM table_schemas WHERE table_id = ?1
             )",
            params![table_id],
        )
        .map_err(|e| format!("delete retired table columns failed: {e}"))?;
        tx.execute(
            "DELETE FROM table_schemas WHERE table_id = ?1",
            params![table_id],
        )
        .map_err(|e| format!("delete retired table schemas failed: {e}"))?;
        tx.execute("DELETE FROM indexes WHERE table_id = ?1", params![table_id])
            .map_err(|e| format!("delete retired table indexes failed: {e}"))?;
        tx.execute(
            "DELETE FROM partitions WHERE table_id = ?1",
            params![table_id],
        )
        .map_err(|e| format!("delete retired table partitions failed: {e}"))?;
        tx.execute(
            "DELETE FROM materialized_views WHERE mv_id = ?1",
            params![table_id],
        )
        .map_err(|e| format!("delete materialized_view row failed: {e}"))?;
        tx.execute("DELETE FROM tables WHERE table_id = ?1", params![table_id])
            .map_err(|e| format!("delete dropping table row failed: {e}"))?;
        tx.commit()
            .map_err(|e| format!("commit purge_retired_table_metadata failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn purge_retired_partition_metadata(&self, partition_id: i64) -> Result<(), String> {
        let conn = self.connection()?;
        let tx = conn.unchecked_transaction().map_err(|e| {
            format!("begin purge_retired_partition_metadata transaction failed: {e}")
        })?;
        let is_retired: bool = tx
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM partitions WHERE partition_id = ?1 AND state = 'RETIRED'
                 )",
                params![partition_id],
                |row| row.get(0),
            )
            .map_err(|e| format!("load retired partition state failed: {e}"))?;
        if !is_retired {
            return Err(format!(
                "cannot purge managed partition {partition_id}: partition is not in RETIRED state"
            ));
        }
        tx.execute(
            "DELETE FROM txns WHERE partition_id = ?1",
            params![partition_id],
        )
        .map_err(|e| format!("delete retired partition txns failed: {e}"))?;
        tx.execute(
            "DELETE FROM tablets WHERE partition_id = ?1",
            params![partition_id],
        )
        .map_err(|e| format!("delete retired partition tablets failed: {e}"))?;
        tx.execute(
            "DELETE FROM indexes WHERE partition_id = ?1",
            params![partition_id],
        )
        .map_err(|e| format!("delete retired partition indexes failed: {e}"))?;
        tx.execute(
            "DELETE FROM partitions WHERE partition_id = ?1",
            params![partition_id],
        )
        .map_err(|e| format!("delete retired partition row failed: {e}"))?;
        tx.commit()
            .map_err(|e| format!("commit purge_retired_partition_metadata failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn delete_creating_partition(&self, partition_id: i64) -> Result<(), String> {
        let conn = self.connection()?;
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| format!("begin delete_creating_partition transaction failed: {e}"))?;
        tx.execute(
            "DELETE FROM tablets WHERE partition_id = ?1",
            params![partition_id],
        )
        .map_err(|e| format!("delete creating tablets failed: {e}"))?;
        tx.execute(
            "DELETE FROM indexes
             WHERE partition_id = ?1 AND state = 'CREATING'",
            params![partition_id],
        )
        .map_err(|e| format!("delete creating indexes failed: {e}"))?;
        tx.execute(
            "DELETE FROM partitions
             WHERE partition_id = ?1 AND state = 'CREATING'",
            params![partition_id],
        )
        .map_err(|e| format!("delete creating partition failed: {e}"))?;
        tx.commit()
            .map_err(|e| format!("commit delete_creating_partition failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn upsert_iceberg_catalog(
        &self,
        catalog_name: &str,
        properties: &[(String, String)],
    ) -> Result<(), String> {
        let conn = self.connection()?;
        let properties_json = serde_json::to_string(properties)
            .map_err(|e| format!("encode iceberg catalog properties failed: {e}"))?;
        conn.execute(
            "INSERT INTO iceberg_catalogs(name, properties_json)
             VALUES (?1, ?2)
             ON CONFLICT(name) DO UPDATE SET properties_json = excluded.properties_json",
            params![catalog_name, properties_json],
        )
        .map_err(|e| format!("persist iceberg catalog failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn upsert_iceberg_namespace(
        &self,
        catalog_name: &str,
        namespace_name: &str,
    ) -> Result<(), String> {
        let conn = self.connection()?;
        conn.execute(
            "INSERT INTO iceberg_namespaces(catalog_name, namespace_name)
             VALUES (?1, ?2)
             ON CONFLICT(catalog_name, namespace_name) DO NOTHING",
            params![catalog_name, namespace_name],
        )
        .map_err(|e| format!("persist iceberg namespace failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn upsert_iceberg_table(
        &self,
        catalog_name: &str,
        namespace_name: &str,
        table_name: &str,
    ) -> Result<(), String> {
        let conn = self.connection()?;
        conn.execute(
            "INSERT INTO iceberg_tables(catalog_name, namespace_name, table_name)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(catalog_name, namespace_name, table_name) DO NOTHING",
            params![catalog_name, namespace_name, table_name],
        )
        .map_err(|e| format!("persist iceberg table failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn delete_iceberg_table(
        &self,
        catalog_name: &str,
        namespace_name: &str,
        table_name: &str,
    ) -> Result<(), String> {
        let conn = self.connection()?;
        conn.execute(
            "DELETE FROM iceberg_tables
             WHERE catalog_name = ?1 AND namespace_name = ?2 AND table_name = ?3",
            params![catalog_name, namespace_name, table_name],
        )
        .map_err(|e| format!("delete iceberg table metadata failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn delete_iceberg_namespace(
        &self,
        catalog_name: &str,
        namespace_name: &str,
    ) -> Result<(), String> {
        let conn = self.connection()?;
        conn.execute(
            "DELETE FROM iceberg_namespaces
             WHERE catalog_name = ?1 AND namespace_name = ?2",
            params![catalog_name, namespace_name],
        )
        .map_err(|e| format!("delete iceberg namespace metadata failed: {e}"))?;
        conn.execute(
            "DELETE FROM iceberg_tables
             WHERE catalog_name = ?1 AND namespace_name = ?2",
            params![catalog_name, namespace_name],
        )
        .map_err(|e| format!("delete iceberg namespace tables metadata failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn delete_iceberg_catalog(&self, catalog_name: &str) -> Result<(), String> {
        let conn = self.connection()?;
        conn.execute(
            "DELETE FROM iceberg_catalogs WHERE name = ?1",
            params![catalog_name],
        )
        .map_err(|e| format!("delete iceberg catalog metadata failed: {e}"))?;
        conn.execute(
            "DELETE FROM iceberg_namespaces WHERE catalog_name = ?1",
            params![catalog_name],
        )
        .map_err(|e| format!("delete iceberg catalog namespaces metadata failed: {e}"))?;
        conn.execute(
            "DELETE FROM iceberg_tables WHERE catalog_name = ?1",
            params![catalog_name],
        )
        .map_err(|e| format!("delete iceberg catalog tables metadata failed: {e}"))?;
        Ok(())
    }

    fn connection(&self) -> Result<Connection, String> {
        let conn = Connection::open(&self.path).map_err(|e| {
            format!(
                "open standalone metadata db {} failed: {e}",
                self.path.display()
            )
        })?;
        // SQLite default busy_timeout = 0 returns SQLITE_BUSY immediately on
        // any write contention. Under the sql-tests runner's concurrent
        // execution this shows up as "database is locked" even though the
        // file is in WAL mode. Wait up to 10 s for the writer lock before
        // giving up, which is cheap when there's no contention and
        // essentially always wins for the short metadata transactions here.
        conn.busy_timeout(std::time::Duration::from_secs(10))
            .map_err(|e| format!("set busy_timeout on standalone metadata db failed: {e}"))?;
        // journal_mode=WAL is a one-shot pragma on the database file itself
        // (persisted after the first successful call in init_schema). Issue
        // it again on fresh connections as a belt-and-braces guard so any
        // connection opened before init runs — or against an older db
        // created with a different journal mode — still gets WAL semantics.
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(|e| format!("set journal_mode=WAL on standalone metadata db failed: {e}"))?;
        Ok(conn)
    }

    fn init_schema(&self) -> Result<(), String> {
        let conn = self.connection()?;
        let current_version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(|e| format!("read standalone metadata schema version failed: {e}"))?;
        if current_version != 0 && current_version != 4 {
            return Err(format!(
                "unsupported standalone metadata schema version {current_version}; delete the metadata db and reopen"
            ));
        }
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;
            PRAGMA synchronous = NORMAL;
            CREATE TABLE IF NOT EXISTS local_databases (
                name TEXT PRIMARY KEY
            );
            DROP TABLE IF EXISTS local_tables;
            CREATE TABLE IF NOT EXISTS global_meta (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                warehouse_uri TEXT NOT NULL,
                next_db_id INTEGER NOT NULL,
                next_table_id INTEGER NOT NULL,
                next_partition_id INTEGER NOT NULL,
                next_index_id INTEGER NOT NULL,
                next_tablet_id INTEGER NOT NULL,
                next_txn_id INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS databases (
                db_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS tables (
                table_id INTEGER PRIMARY KEY,
                db_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                keys_type TEXT NOT NULL,
                bucket_num INTEGER NOT NULL,
                current_schema_id INTEGER NOT NULL,
                state TEXT NOT NULL,
                kind TEXT NOT NULL DEFAULT 'TABLE'
                    CHECK (kind IN ('TABLE', 'MATERIALIZED_VIEW')),
                UNIQUE(db_id, name)
            );
            CREATE TABLE IF NOT EXISTS table_schemas (
                schema_id INTEGER PRIMARY KEY,
                table_id INTEGER NOT NULL,
                schema_version INTEGER NOT NULL,
                tablet_schema_pb BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS table_columns (
                schema_id INTEGER NOT NULL,
                ordinal INTEGER NOT NULL,
                column_name TEXT NOT NULL,
                logical_type TEXT NOT NULL,
                nullable INTEGER NOT NULL,
                PRIMARY KEY (schema_id, ordinal)
            );
            CREATE TABLE IF NOT EXISTS partitions (
                partition_id INTEGER PRIMARY KEY,
                table_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                visible_version INTEGER NOT NULL,
                next_version INTEGER NOT NULL,
                state TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS indexes (
                index_id INTEGER PRIMARY KEY,
                table_id INTEGER NOT NULL,
                partition_id INTEGER NOT NULL,
                index_type TEXT NOT NULL,
                state TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS tablets (
                tablet_id INTEGER PRIMARY KEY,
                partition_id INTEGER NOT NULL,
                index_id INTEGER NOT NULL,
                bucket_seq INTEGER NOT NULL,
                tablet_root_path TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS txns (
                txn_id INTEGER PRIMARY KEY,
                table_id INTEGER NOT NULL,
                partition_id INTEGER NOT NULL,
                base_version INTEGER NOT NULL,
                commit_version INTEGER NOT NULL,
                state TEXT NOT NULL,
                retry_at_ms INTEGER,
                updated_at_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS erase_jobs (
                job_id INTEGER PRIMARY KEY,
                job_kind TEXT NOT NULL,
                table_id INTEGER NOT NULL,
                partition_id INTEGER,
                root_path TEXT NOT NULL,
                state TEXT NOT NULL,
                retry_at_ms INTEGER,
                updated_at_ms INTEGER NOT NULL,
                last_error TEXT
            );
            CREATE TABLE IF NOT EXISTS materialized_views (
                mv_id INTEGER PRIMARY KEY REFERENCES tables(table_id),
                select_sql TEXT NOT NULL,
                refresh_mode TEXT NOT NULL DEFAULT 'DEFERRED_MANUAL'
                    CHECK (refresh_mode IN ('DEFERRED_MANUAL')),
                base_table_refs_json TEXT NOT NULL,
                last_refresh_ms INTEGER,
                last_refresh_rows INTEGER,
                last_refresh_snapshots_json TEXT,
                created_at_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS iceberg_catalogs (
                name TEXT PRIMARY KEY,
                properties_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS iceberg_namespaces (
                catalog_name TEXT NOT NULL,
                namespace_name TEXT NOT NULL,
                PRIMARY KEY (catalog_name, namespace_name)
            );
            CREATE TABLE IF NOT EXISTS iceberg_tables (
                catalog_name TEXT NOT NULL,
                namespace_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                PRIMARY KEY (catalog_name, namespace_name, table_name)
            );
            PRAGMA user_version = 4;
            ",
        )
        .map_err(|e| format!("initialize standalone metadata schema failed: {e}"))?;
        Ok(())
    }

    fn load_managed_snapshot(&self, conn: &Connection) -> Result<ManagedSnapshot, String> {
        let global = conn
            .query_row(
                "SELECT
                    warehouse_uri,
                    next_db_id,
                    next_table_id,
                    next_partition_id,
                    next_index_id,
                    next_tablet_id,
                    next_txn_id
                 FROM global_meta
                 WHERE singleton = 1",
                [],
                |row| {
                    Ok(ManagedGlobalMeta {
                        warehouse_uri: row.get(0)?,
                        next_db_id: row.get(1)?,
                        next_table_id: row.get(2)?,
                        next_partition_id: row.get(3)?,
                        next_index_id: row.get(4)?,
                        next_tablet_id: row.get(5)?,
                        next_txn_id: row.get(6)?,
                    })
                },
            )
            .optional()
            .map_err(|e| format!("query managed global metadata failed: {e}"))?
            .unwrap_or_default();

        let databases = {
            let mut stmt = conn
                .prepare("SELECT db_id, name FROM databases ORDER BY db_id")
                .map_err(|e| format!("prepare managed databases query failed: {e}"))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok(StoredManagedDatabase {
                        db_id: row.get(0)?,
                        name: row.get(1)?,
                    })
                })
                .map_err(|e| format!("query managed databases failed: {e}"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("read managed databases failed: {e}"))?
        };

        let tables = {
            let mut stmt = conn
                .prepare(
                    "SELECT
                        table_id,
                        db_id,
                        name,
                        keys_type,
                        bucket_num,
                        current_schema_id,
                        state,
                        kind
                     FROM tables
                     ORDER BY table_id",
                )
                .map_err(|e| format!("prepare managed tables query failed: {e}"))?;
            let rows = stmt
                .query_map([], |row| {
                    let state = ManagedTableState::from_sql_str(&row.get::<_, String>(6)?)
                        .map_err(invalid_state_sql_error)?;
                    let kind = ManagedTableKind::from_sql_str(&row.get::<_, String>(7)?)
                        .map_err(invalid_state_sql_error)?;
                    Ok(StoredManagedTable {
                        table_id: row.get(0)?,
                        db_id: row.get(1)?,
                        name: row.get(2)?,
                        keys_type: row.get(3)?,
                        bucket_num: row.get(4)?,
                        current_schema_id: row.get(5)?,
                        state,
                        kind,
                    })
                })
                .map_err(|e| format!("query managed tables failed: {e}"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("read managed tables failed: {e}"))?
        };

        let schemas = {
            let mut stmt = conn
                .prepare(
                    "SELECT schema_id, table_id, schema_version, tablet_schema_pb
                     FROM table_schemas
                     ORDER BY schema_id",
                )
                .map_err(|e| format!("prepare managed schemas query failed: {e}"))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok(StoredManagedSchema {
                        schema_id: row.get(0)?,
                        table_id: row.get(1)?,
                        schema_version: row.get(2)?,
                        tablet_schema_pb: row.get(3)?,
                    })
                })
                .map_err(|e| format!("query managed schemas failed: {e}"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("read managed schemas failed: {e}"))?
        };

        let columns = {
            let mut stmt = conn
                .prepare(
                    "SELECT schema_id, ordinal, column_name, logical_type, nullable
                     FROM table_columns
                     ORDER BY schema_id, ordinal",
                )
                .map_err(|e| format!("prepare managed columns query failed: {e}"))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok(StoredManagedColumn {
                        schema_id: row.get(0)?,
                        ordinal: row.get(1)?,
                        column_name: row.get(2)?,
                        logical_type: row.get(3)?,
                        nullable: row.get::<_, i64>(4)? != 0,
                    })
                })
                .map_err(|e| format!("query managed columns failed: {e}"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("read managed columns failed: {e}"))?
        };

        let partitions = {
            let mut stmt = conn
                .prepare(
                    "SELECT
                        partition_id,
                        table_id,
                        name,
                        visible_version,
                        next_version,
                        state
                     FROM partitions
                     ORDER BY partition_id",
                )
                .map_err(|e| format!("prepare managed partitions query failed: {e}"))?;
            let rows = stmt
                .query_map([], |row| {
                    let state = ManagedPartitionState::from_sql_str(&row.get::<_, String>(5)?)
                        .map_err(invalid_state_sql_error)?;
                    Ok(StoredManagedPartition {
                        partition_id: row.get(0)?,
                        table_id: row.get(1)?,
                        name: row.get(2)?,
                        visible_version: row.get(3)?,
                        next_version: row.get(4)?,
                        state,
                    })
                })
                .map_err(|e| format!("query managed partitions failed: {e}"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("read managed partitions failed: {e}"))?
        };

        let indexes = {
            let mut stmt = conn
                .prepare(
                    "SELECT index_id, table_id, partition_id, index_type, state
                     FROM indexes
                     ORDER BY index_id",
                )
                .map_err(|e| format!("prepare managed indexes query failed: {e}"))?;
            let rows = stmt
                .query_map([], |row| {
                    let state = ManagedIndexState::from_sql_str(&row.get::<_, String>(4)?)
                        .map_err(invalid_state_sql_error)?;
                    Ok(StoredManagedIndex {
                        index_id: row.get(0)?,
                        table_id: row.get(1)?,
                        partition_id: row.get(2)?,
                        index_type: row.get(3)?,
                        state,
                    })
                })
                .map_err(|e| format!("query managed indexes failed: {e}"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("read managed indexes failed: {e}"))?
        };

        let tablets = {
            let mut stmt = conn
                .prepare(
                    "SELECT tablet_id, partition_id, index_id, bucket_seq, tablet_root_path
                     FROM tablets
                     ORDER BY tablet_id",
                )
                .map_err(|e| format!("prepare managed tablets query failed: {e}"))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok(StoredManagedTablet {
                        tablet_id: row.get(0)?,
                        partition_id: row.get(1)?,
                        index_id: row.get(2)?,
                        bucket_seq: row.get(3)?,
                        tablet_root_path: row.get(4)?,
                    })
                })
                .map_err(|e| format!("query managed tablets failed: {e}"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("read managed tablets failed: {e}"))?
        };

        let txns = {
            let mut stmt = conn
                .prepare(
                    "SELECT
                        txn_id,
                        table_id,
                        partition_id,
                        base_version,
                        commit_version,
                        state,
                        retry_at_ms,
                        updated_at_ms
                     FROM txns
                     ORDER BY txn_id",
                )
                .map_err(|e| format!("prepare managed txns query failed: {e}"))?;
            let rows = stmt
                .query_map([], |row| {
                    let state = ManagedTxnState::from_sql_str(&row.get::<_, String>(5)?)
                        .map_err(invalid_state_sql_error)?;
                    Ok(StoredManagedTxn {
                        txn_id: row.get(0)?,
                        table_id: row.get(1)?,
                        partition_id: row.get(2)?,
                        base_version: row.get(3)?,
                        commit_version: row.get(4)?,
                        state,
                        retry_at_ms: row.get(6)?,
                        updated_at_ms: row.get(7)?,
                    })
                })
                .map_err(|e| format!("query managed txns failed: {e}"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("read managed txns failed: {e}"))?
        };

        let erase_jobs = {
            let mut stmt = conn
                .prepare(
                    "SELECT
                        job_id,
                        job_kind,
                        table_id,
                        partition_id,
                        root_path,
                        state,
                        retry_at_ms,
                        updated_at_ms,
                        last_error
                     FROM erase_jobs
                     ORDER BY job_id",
                )
                .map_err(|e| format!("prepare managed erase_jobs query failed: {e}"))?;
            let rows = stmt
                .query_map([], |row| {
                    let job_kind = ManagedEraseJobKind::from_sql_str(&row.get::<_, String>(1)?)
                        .map_err(invalid_state_sql_error)?;
                    let state = ManagedEraseJobState::from_sql_str(&row.get::<_, String>(5)?)
                        .map_err(invalid_state_sql_error)?;
                    Ok(StoredManagedEraseJob {
                        job_id: row.get(0)?,
                        job_kind,
                        table_id: row.get(2)?,
                        partition_id: row.get(3)?,
                        root_path: row.get(4)?,
                        state,
                        retry_at_ms: row.get(6)?,
                        updated_at_ms: row.get(7)?,
                        last_error: row.get(8)?,
                    })
                })
                .map_err(|e| format!("query managed erase_jobs failed: {e}"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("read managed erase_jobs failed: {e}"))?
        };

        let materialized_views = {
            let mut stmt = conn
                .prepare(
                    "SELECT
                        mv_id,
                        select_sql,
                        refresh_mode,
                        base_table_refs_json,
                        last_refresh_ms,
                        last_refresh_rows,
                        last_refresh_snapshots_json,
                        created_at_ms
                     FROM materialized_views
                     ORDER BY mv_id",
                )
                .map_err(|e| format!("prepare materialized_views query failed: {e}"))?;
            let rows = stmt
                .query_map([], |row| {
                    let refresh_mode =
                        ManagedMvRefreshMode::from_sql_str(&row.get::<_, String>(2)?)
                            .map_err(invalid_state_sql_error)?;
                    let base_json: String = row.get(3)?;
                    let base_table_refs: Vec<IcebergTableRef> =
                        serde_json::from_str(&base_json).map_err(json_to_sql_error)?;
                    let snapshots: std::collections::BTreeMap<String, i64> =
                        match row.get::<_, Option<String>>(6)? {
                            Some(s) => serde_json::from_str(&s).map_err(json_to_sql_error)?,
                            None => std::collections::BTreeMap::new(),
                        };
                    Ok(StoredMaterializedView {
                        mv_id: row.get(0)?,
                        select_sql: row.get(1)?,
                        refresh_mode,
                        base_table_refs,
                        last_refresh_ms: row.get(4)?,
                        last_refresh_rows: row.get(5)?,
                        last_refresh_snapshots: snapshots,
                        created_at_ms: row.get(7)?,
                    })
                })
                .map_err(|e| format!("query materialized_views failed: {e}"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("read materialized_views failed: {e}"))?
        };

        if global == ManagedGlobalMeta::default()
            && (!databases.is_empty()
                || !tables.is_empty()
                || !schemas.is_empty()
                || !columns.is_empty()
                || !partitions.is_empty()
                || !indexes.is_empty()
                || !tablets.is_empty()
                || !txns.is_empty()
                || !erase_jobs.is_empty()
                || !materialized_views.is_empty())
        {
            return Err("managed metadata missing global_meta row".to_string());
        }

        Ok(ManagedSnapshot {
            global,
            databases,
            tables,
            schemas,
            columns,
            partitions,
            indexes,
            tablets,
            txns,
            erase_jobs,
            materialized_views,
        })
    }
}

fn update_mv_refresh_metadata_in_tx(
    tx: &rusqlite::Transaction<'_>,
    req: &UpdateMvRefreshMetadataRequest,
) -> Result<(), String> {
    let snapshots_json = if req.snapshots.is_empty() {
        None
    } else {
        Some(
            serde_json::to_string(&req.snapshots)
                .map_err(|e| format!("serialize mv refresh snapshots failed: {e}"))?,
        )
    };
    let changed = tx
        .execute(
            "UPDATE materialized_views
             SET last_refresh_ms = strftime('%s','now') * 1000,
                 last_refresh_rows = ?1,
                 last_refresh_snapshots_json = ?2
             WHERE mv_id = ?3",
            params![req.last_refresh_rows, snapshots_json, req.table_id],
        )
        .map_err(|e| format!("update materialized_view last_refresh fields failed: {e}"))?;
    if changed != 1 {
        return Err(format!(
            "materialized view {} metadata row not found",
            req.table_id
        ));
    }
    Ok(())
}

fn json_to_sql_error(err: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
}

fn invalid_state_sql_error(err: String) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, err)),
    )
}

fn bool_to_sql_int(value: bool) -> i64 {
    if value { 1 } else { 0 }
}

fn query_single_text_column<P>(
    conn: &Connection,
    sql: &str,
    params: P,
) -> Result<Vec<String>, String>
where
    P: rusqlite::Params,
{
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("prepare query `{sql}` failed: {e}"))?;
    let rows = stmt
        .query_map(params, |row| row.get::<_, String>(0))
        .map_err(|e| format!("execute query `{sql}` failed: {e}"))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("read query `{sql}` failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::{
        ActivateMvRefreshRequest, IcebergTableRef, ManagedEraseJobKind, ManagedEraseJobState,
        ManagedGlobalMeta, ManagedIndexState, ManagedMvRefreshMode, ManagedPartitionState,
        ManagedSnapshot, ManagedTableKind, ManagedTableState, ManagedTxnState, SqliteMetadataStore,
        StageManagedTruncateRequest, StageMvRefreshRequest, StoredManagedColumn,
        StoredManagedDatabase, StoredManagedEraseJob, StoredManagedIndex, StoredManagedPartition,
        StoredManagedSchema, StoredManagedTable, StoredManagedTablet, StoredManagedTxn,
        StoredMaterializedView, UpdateMvRefreshMetadataRequest,
    };

    #[test]
    fn standalone_store_round_trips_managed_lake_snapshot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store =
            SqliteMetadataStore::open(dir.path().join("standalone.sqlite")).expect("open store");

        let expected = ManagedSnapshot {
            global: ManagedGlobalMeta {
                warehouse_uri: "s3://novarocks/standalone".to_string(),
                next_db_id: 2,
                next_table_id: 11,
                next_partition_id: 21,
                next_index_id: 31,
                next_tablet_id: 41,
                next_txn_id: 51,
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
                bucket_num: 2,
                current_schema_id: 100,
                state: ManagedTableState::Creating,
                kind: ManagedTableKind::Table,
            }],
            schemas: vec![StoredManagedSchema {
                schema_id: 100,
                table_id: 10,
                schema_version: 1,
                tablet_schema_pb: vec![1, 2, 3],
            }],
            columns: vec![StoredManagedColumn {
                schema_id: 100,
                ordinal: 0,
                column_name: "k1".to_string(),
                logical_type: "INT".to_string(),
                nullable: false,
            }],
            partitions: vec![StoredManagedPartition {
                partition_id: 20,
                table_id: 10,
                name: "p0".to_string(),
                visible_version: 1,
                next_version: 2,
                state: ManagedPartitionState::Failed,
            }],
            indexes: vec![StoredManagedIndex {
                index_id: 30,
                table_id: 10,
                partition_id: 20,
                index_type: "BASE".to_string(),
                state: ManagedIndexState::Creating,
            }],
            tablets: vec![StoredManagedTablet {
                tablet_id: 40,
                partition_id: 20,
                index_id: 30,
                bucket_seq: 0,
                tablet_root_path: "s3://novarocks/standalone/db_1/table_10/tablet_40".to_string(),
            }],
            txns: vec![StoredManagedTxn {
                txn_id: 50,
                table_id: 10,
                partition_id: 20,
                base_version: 1,
                commit_version: 2,
                state: ManagedTxnState::Written,
                retry_at_ms: Some(1_234),
                updated_at_ms: 5_678,
            }],
            erase_jobs: Vec::new(),
            materialized_views: Vec::new(),
        };

        store
            .replace_managed_snapshot(&expected)
            .expect("persist snapshot");

        let snapshot = store.load_snapshot().expect("load snapshot");
        assert_eq!(snapshot.managed, expected);
    }

    #[test]
    fn standalone_store_round_trips_lifecycle_states_and_erase_jobs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store =
            SqliteMetadataStore::open(dir.path().join("standalone.sqlite")).expect("open store");

        let expected = ManagedSnapshot {
            global: ManagedGlobalMeta {
                warehouse_uri: "s3://bucket/warehouse".to_string(),
                next_db_id: 2,
                next_table_id: 3,
                next_partition_id: 4,
                next_index_id: 5,
                next_tablet_id: 6,
                next_txn_id: 7,
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
                bucket_num: 2,
                current_schema_id: 10,
                state: ManagedTableState::Dropping,
                kind: ManagedTableKind::Table,
            }],
            schemas: vec![],
            columns: vec![],
            partitions: vec![StoredManagedPartition {
                partition_id: 20,
                table_id: 10,
                name: "p0".to_string(),
                visible_version: 3,
                next_version: 4,
                state: ManagedPartitionState::Retired,
            }],
            indexes: vec![StoredManagedIndex {
                index_id: 30,
                table_id: 10,
                partition_id: 20,
                index_type: "BASE".to_string(),
                state: ManagedIndexState::Retired,
            }],
            tablets: vec![StoredManagedTablet {
                tablet_id: 40,
                partition_id: 20,
                index_id: 30,
                bucket_seq: 0,
                tablet_root_path: "s3://bucket/warehouse/db_1/table_10/partition_20".to_string(),
            }],
            txns: vec![],
            erase_jobs: vec![StoredManagedEraseJob {
                job_id: 50,
                job_kind: ManagedEraseJobKind::DropTable,
                table_id: 10,
                partition_id: None,
                root_path: "s3://bucket/warehouse/db_1/table_10".to_string(),
                state: ManagedEraseJobState::Pending,
                retry_at_ms: None,
                updated_at_ms: 0,
                last_error: None,
            }],
            materialized_views: Vec::new(),
        };

        store
            .replace_managed_snapshot(&expected)
            .expect("persist snapshot");

        let snapshot = store.load_snapshot().expect("load snapshot");
        assert_eq!(snapshot.managed, expected);
    }

    fn bootstrapped_store_for_txn() -> (tempfile::TempDir, SqliteMetadataStore) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store =
            SqliteMetadataStore::open(dir.path().join("standalone.sqlite")).expect("open store");
        let snapshot = ManagedSnapshot {
            global: ManagedGlobalMeta {
                warehouse_uri: "s3://test/warehouse".to_string(),
                next_db_id: 2,
                next_table_id: 11,
                next_partition_id: 21,
                next_index_id: 31,
                next_tablet_id: 41,
                next_txn_id: 50,
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
        };
        store
            .replace_managed_snapshot(&snapshot)
            .expect("persist snapshot");
        (dir, store)
    }

    #[test]
    fn prepare_txn_allocates_unique_ids_and_inserts_prepared_row() {
        let (_dir, store) = bootstrapped_store_for_txn();
        let first = store.prepare_txn(10, 20, 1).expect("prepare first");
        let second = store.prepare_txn(10, 20, 1).expect("prepare second");
        assert_eq!(first.txn_id, 50);
        assert_eq!(first.table_id, 10);
        assert_eq!(first.partition_id, 20);
        assert_eq!(first.base_version, 1);
        assert_eq!(first.commit_version, 2);
        assert_eq!(second.txn_id, 51);
        let snapshot = store.load_snapshot().expect("load snapshot");
        assert_eq!(snapshot.managed.txns.len(), 2);
        assert!(
            snapshot
                .managed
                .txns
                .iter()
                .all(|txn| txn.state == ManagedTxnState::Prepared)
        );
        assert_eq!(snapshot.managed.global.next_txn_id, 52);
    }

    #[test]
    fn mark_txn_written_and_visible_advances_partition_version() {
        let (_dir, store) = bootstrapped_store_for_txn();
        let prepared = store.prepare_txn(10, 20, 1).expect("prepare txn");
        store
            .mark_txn_written(prepared.txn_id)
            .expect("mark written");
        let after_written = store.load_snapshot().expect("load after written");
        assert_eq!(
            after_written.managed.txns[0].state,
            ManagedTxnState::Written
        );

        store
            .mark_txn_visible(prepared.txn_id, prepared.commit_version)
            .expect("mark visible");
        let after_visible = store.load_snapshot().expect("load after visible");
        assert_eq!(
            after_visible.managed.txns[0].state,
            ManagedTxnState::Visible
        );
        let partition = after_visible
            .managed
            .partitions
            .iter()
            .find(|partition| partition.partition_id == 20)
            .expect("partition row");
        assert_eq!(partition.visible_version, 2);
        assert_eq!(partition.next_version, 3);
    }

    #[test]
    fn update_mv_refresh_metadata_only_updates_last_refresh_fields() {
        let (_dir, store) = bootstrapped_store_for_txn();
        let mut snapshot = store.load_snapshot().expect("load").managed;
        snapshot.tables[0].kind = ManagedTableKind::MaterializedView;
        snapshot.materialized_views.push(StoredMaterializedView {
            mv_id: 10,
            select_sql: "select k1 from ice.ns.orders".to_string(),
            refresh_mode: ManagedMvRefreshMode::DeferredManual,
            base_table_refs: vec![IcebergTableRef {
                catalog: "ice".to_string(),
                namespace: "ns".to_string(),
                table: "orders".to_string(),
            }],
            last_refresh_ms: None,
            last_refresh_rows: Some(3),
            last_refresh_snapshots: std::collections::BTreeMap::new(),
            created_at_ms: 1,
        });
        store.replace_managed_snapshot(&snapshot).expect("persist");

        let mut snapshots = std::collections::BTreeMap::new();
        snapshots.insert("ice.ns.orders".to_string(), 88);
        store
            .update_mv_refresh_metadata(UpdateMvRefreshMetadataRequest {
                table_id: 10,
                last_refresh_rows: 3,
                snapshots: snapshots.clone(),
            })
            .expect("update metadata");

        let loaded = store.load_snapshot().expect("reload").managed;
        let mv = loaded
            .materialized_views
            .iter()
            .find(|mv| mv.mv_id == 10)
            .expect("mv");
        assert_eq!(mv.last_refresh_rows, Some(3));
        assert_eq!(mv.last_refresh_snapshots, snapshots);
        assert!(mv.last_refresh_ms.is_some());
    }

    #[test]
    fn mark_txn_visible_with_mv_refresh_metadata_is_atomic() {
        let (_dir, store) = bootstrapped_store_for_txn();
        let mut snapshot = store.load_snapshot().expect("load").managed;
        snapshot.tables[0].kind = ManagedTableKind::MaterializedView;
        snapshot.materialized_views.push(StoredMaterializedView {
            mv_id: 10,
            select_sql: "select k1 from ice.ns.orders".to_string(),
            refresh_mode: ManagedMvRefreshMode::DeferredManual,
            base_table_refs: vec![],
            last_refresh_ms: Some(1),
            last_refresh_rows: Some(2),
            last_refresh_snapshots: std::collections::BTreeMap::new(),
            created_at_ms: 1,
        });
        store.replace_managed_snapshot(&snapshot).expect("persist");
        let prepared = store.prepare_txn(10, 20, 1).expect("prepare");
        store.mark_txn_written(prepared.txn_id).expect("written");

        let mut snapshots = std::collections::BTreeMap::new();
        snapshots.insert("ice.ns.orders".to_string(), 99);
        store
            .mark_txn_visible_with_mv_refresh_metadata(
                prepared.txn_id,
                prepared.commit_version,
                UpdateMvRefreshMetadataRequest {
                    table_id: 10,
                    last_refresh_rows: 4,
                    snapshots: snapshots.clone(),
                },
            )
            .expect("visible with metadata");

        let loaded = store.load_snapshot().expect("reload").managed;
        let partition = loaded
            .partitions
            .iter()
            .find(|p| p.partition_id == 20)
            .expect("partition");
        assert_eq!(partition.visible_version, 2);
        let mv = loaded
            .materialized_views
            .iter()
            .find(|mv| mv.mv_id == 10)
            .expect("mv");
        assert_eq!(mv.last_refresh_rows, Some(4));
        assert_eq!(mv.last_refresh_snapshots, snapshots);
    }

    #[test]
    fn mark_txn_aborted_does_not_touch_partition_version() {
        let (_dir, store) = bootstrapped_store_for_txn();
        let prepared = store.prepare_txn(10, 20, 1).expect("prepare txn");
        store
            .mark_txn_aborted(prepared.txn_id)
            .expect("mark aborted");
        let snapshot = store.load_snapshot().expect("load snapshot");
        assert_eq!(snapshot.managed.txns[0].state, ManagedTxnState::Aborted);
        assert!(snapshot.managed.txns[0].retry_at_ms.is_none());
        let partition = snapshot
            .managed
            .partitions
            .iter()
            .find(|partition| partition.partition_id == 20)
            .expect("partition row");
        assert_eq!(partition.visible_version, 1);
        assert_eq!(partition.next_version, 2);
    }

    #[test]
    fn drop_managed_table_rejects_inflight_txns() {
        let (_dir, store) = bootstrapped_store_for_txn();
        let _prepared = store.prepare_txn(10, 20, 1).expect("prepare txn");

        let err = store
            .drop_managed_table(10, "s3://test/warehouse/db_1/table_10")
            .expect_err("drop should reject inflight txns");
        assert!(err.contains("inflight managed txns"), "err={err}");
    }

    #[test]
    fn drop_managed_table_marks_metadata_and_enqueues_drop_job() {
        let (_dir, store) = bootstrapped_store_for_txn();

        store
            .drop_managed_table(10, "s3://test/warehouse/db_1/table_10")
            .expect("drop managed table");

        let snapshot = store.load_snapshot().expect("load snapshot");
        assert_eq!(
            snapshot.managed.tables[0].state,
            ManagedTableState::Dropping
        );
        assert_eq!(
            snapshot.managed.partitions[0].state,
            ManagedPartitionState::Retired
        );
        assert_eq!(snapshot.managed.erase_jobs.len(), 1);
        assert_eq!(
            snapshot.managed.erase_jobs[0].job_kind,
            ManagedEraseJobKind::DropTable
        );
    }

    #[test]
    fn activate_truncate_partition_switches_active_partition_and_enqueues_erase() {
        let (_dir, store) = bootstrapped_store_for_txn();
        let staged = store
            .stage_truncate_partition(StageManagedTruncateRequest {
                table_id: 10,
                db_id: 1,
                bucket_num: 2,
                partition_name: "p0".to_string(),
                warehouse_uri: "s3://test/warehouse".to_string(),
            })
            .expect("stage truncate");

        store
            .activate_truncate_partition(
                10,
                20,
                staged.partition_id,
                staged.index_id,
                "s3://test/warehouse/db_1/table_10/partition_20",
            )
            .expect("activate truncate");

        let snapshot = store.load_snapshot().expect("load snapshot");
        assert!(snapshot.managed.partitions.iter().any(|partition| {
            partition.partition_id == staged.partition_id
                && partition.state == ManagedPartitionState::Active
        }));
        assert!(snapshot.managed.partitions.iter().any(|partition| {
            partition.partition_id == 20 && partition.state == ManagedPartitionState::Retired
        }));
        assert!(snapshot.managed.erase_jobs.iter().any(|job| {
            job.job_kind == ManagedEraseJobKind::DropPartition && job.partition_id == Some(20)
        }));
    }

    #[test]
    fn list_runnable_erase_jobs_filters_by_state_and_retry_deadline() {
        let (_dir, store) = bootstrapped_store_for_txn();
        let mut snapshot = store.load_snapshot().expect("load snapshot").managed;
        snapshot.erase_jobs = vec![
            StoredManagedEraseJob {
                job_id: 1,
                job_kind: ManagedEraseJobKind::DropTable,
                table_id: 10,
                partition_id: None,
                root_path: "s3://test/warehouse/db_1/table_10".to_string(),
                state: ManagedEraseJobState::Pending,
                retry_at_ms: None,
                updated_at_ms: 0,
                last_error: None,
            },
            StoredManagedEraseJob {
                job_id: 2,
                job_kind: ManagedEraseJobKind::DropPartition,
                table_id: 10,
                partition_id: Some(20),
                root_path: "s3://test/warehouse/db_1/table_10/partition_20".to_string(),
                state: ManagedEraseJobState::Failed,
                retry_at_ms: Some(1_000),
                updated_at_ms: 0,
                last_error: Some("temporary".to_string()),
            },
            StoredManagedEraseJob {
                job_id: 3,
                job_kind: ManagedEraseJobKind::DropPartition,
                table_id: 10,
                partition_id: Some(21),
                root_path: "s3://test/warehouse/db_1/table_10/partition_21".to_string(),
                state: ManagedEraseJobState::Failed,
                retry_at_ms: Some(5_000),
                updated_at_ms: 0,
                last_error: Some("wait".to_string()),
            },
            StoredManagedEraseJob {
                job_id: 4,
                job_kind: ManagedEraseJobKind::DropPartition,
                table_id: 10,
                partition_id: Some(22),
                root_path: "s3://test/warehouse/db_1/table_10/partition_22".to_string(),
                state: ManagedEraseJobState::Running,
                retry_at_ms: None,
                updated_at_ms: 0,
                last_error: None,
            },
            StoredManagedEraseJob {
                job_id: 5,
                job_kind: ManagedEraseJobKind::DropPartition,
                table_id: 10,
                partition_id: Some(23),
                root_path: "s3://test/warehouse/db_1/table_10/partition_23".to_string(),
                state: ManagedEraseJobState::Finished,
                retry_at_ms: None,
                updated_at_ms: 0,
                last_error: None,
            },
        ];
        store
            .replace_managed_snapshot(&snapshot)
            .expect("persist snapshot");

        let runnable = store
            .list_runnable_erase_jobs(1_000)
            .expect("list runnable jobs");
        let job_ids = runnable.iter().map(|job| job.job_id).collect::<Vec<_>>();
        assert_eq!(job_ids, vec![1, 2]);
    }

    #[test]
    fn claim_finish_fail_erase_job_updates_state() {
        let (_dir, store) = bootstrapped_store_for_txn();
        let mut snapshot = store.load_snapshot().expect("load snapshot").managed;
        snapshot.erase_jobs = vec![StoredManagedEraseJob {
            job_id: 1,
            job_kind: ManagedEraseJobKind::DropTable,
            table_id: 10,
            partition_id: None,
            root_path: "s3://test/warehouse/db_1/table_10".to_string(),
            state: ManagedEraseJobState::Pending,
            retry_at_ms: None,
            updated_at_ms: 0,
            last_error: None,
        }];
        store
            .replace_managed_snapshot(&snapshot)
            .expect("persist snapshot");

        assert!(store.claim_erase_job(1).expect("claim erase job"));
        store
            .fail_erase_job(1, "temporary", 4_000)
            .expect("fail erase job");
        assert!(
            !store
                .claim_erase_job(999)
                .expect("claim missing erase job should be false")
        );
        assert!(
            store
                .claim_erase_job(1)
                .expect("reclaim failed erase job should succeed")
        );
        store.finish_erase_job(1).expect("finish erase job");

        let loaded = store.load_snapshot().expect("load snapshot");
        assert_eq!(loaded.managed.erase_jobs.len(), 1);
        let job = &loaded.managed.erase_jobs[0];
        assert_eq!(job.state, ManagedEraseJobState::Finished);
        assert!(job.retry_at_ms.is_none());
        assert!(job.last_error.is_none());
    }

    #[test]
    fn purge_retired_table_metadata_removes_table_owned_rows() {
        let (_dir, store) = bootstrapped_store_for_txn();
        let mut snapshot = store.load_snapshot().expect("load snapshot").managed;
        snapshot.tables[0].state = ManagedTableState::Dropping;
        snapshot.schemas.push(StoredManagedSchema {
            schema_id: 101,
            table_id: 10,
            schema_version: 1,
            tablet_schema_pb: vec![1, 2, 3],
        });
        snapshot.columns.push(StoredManagedColumn {
            schema_id: 101,
            ordinal: 0,
            column_name: "k1".to_string(),
            logical_type: "INT".to_string(),
            nullable: false,
        });
        snapshot.partitions[0].state = ManagedPartitionState::Retired;
        snapshot.indexes.push(StoredManagedIndex {
            index_id: 30,
            table_id: 10,
            partition_id: 20,
            index_type: "BASE".to_string(),
            state: ManagedIndexState::Retired,
        });
        snapshot.tablets.push(StoredManagedTablet {
            tablet_id: 40,
            partition_id: 20,
            index_id: 30,
            bucket_seq: 0,
            tablet_root_path: "s3://test/warehouse/db_1/table_10/partition_20".to_string(),
        });
        snapshot.txns.push(StoredManagedTxn {
            txn_id: 60,
            table_id: 10,
            partition_id: 20,
            base_version: 1,
            commit_version: 2,
            state: ManagedTxnState::Visible,
            retry_at_ms: None,
            updated_at_ms: 0,
        });
        snapshot.erase_jobs = vec![StoredManagedEraseJob {
            job_id: 1,
            job_kind: ManagedEraseJobKind::DropTable,
            table_id: 10,
            partition_id: None,
            root_path: "s3://test/warehouse/db_1/table_10".to_string(),
            state: ManagedEraseJobState::Running,
            retry_at_ms: None,
            updated_at_ms: 0,
            last_error: None,
        }];
        store
            .replace_managed_snapshot(&snapshot)
            .expect("persist snapshot");

        store
            .purge_retired_table_metadata(10)
            .expect("purge retired table");

        let loaded = store.load_snapshot().expect("load snapshot");
        assert_eq!(loaded.managed.databases.len(), 1);
        assert!(loaded.managed.tables.is_empty());
        assert!(loaded.managed.schemas.is_empty());
        assert!(loaded.managed.columns.is_empty());
        assert!(loaded.managed.partitions.is_empty());
        assert!(loaded.managed.indexes.is_empty());
        assert!(loaded.managed.tablets.is_empty());
        assert!(loaded.managed.txns.is_empty());
        assert_eq!(loaded.managed.erase_jobs.len(), 1);
    }

    #[test]
    fn purge_retired_partition_metadata_keeps_active_replacement_partition() {
        let (_dir, store) = bootstrapped_store_for_txn();
        let mut snapshot = store.load_snapshot().expect("load snapshot").managed;
        snapshot.partitions[0].state = ManagedPartitionState::Retired;
        snapshot.partitions.push(StoredManagedPartition {
            partition_id: 21,
            table_id: 10,
            name: "p0".to_string(),
            visible_version: 1,
            next_version: 2,
            state: ManagedPartitionState::Active,
        });
        snapshot.indexes = vec![
            StoredManagedIndex {
                index_id: 30,
                table_id: 10,
                partition_id: 20,
                index_type: "BASE".to_string(),
                state: ManagedIndexState::Retired,
            },
            StoredManagedIndex {
                index_id: 31,
                table_id: 10,
                partition_id: 21,
                index_type: "BASE".to_string(),
                state: ManagedIndexState::Active,
            },
        ];
        snapshot.tablets = vec![
            StoredManagedTablet {
                tablet_id: 40,
                partition_id: 20,
                index_id: 30,
                bucket_seq: 0,
                tablet_root_path: "s3://test/warehouse/db_1/table_10/partition_20".to_string(),
            },
            StoredManagedTablet {
                tablet_id: 41,
                partition_id: 21,
                index_id: 31,
                bucket_seq: 0,
                tablet_root_path: "s3://test/warehouse/db_1/table_10/partition_21".to_string(),
            },
        ];
        snapshot.txns.push(StoredManagedTxn {
            txn_id: 60,
            table_id: 10,
            partition_id: 20,
            base_version: 1,
            commit_version: 2,
            state: ManagedTxnState::Visible,
            retry_at_ms: None,
            updated_at_ms: 0,
        });
        snapshot.txns.push(StoredManagedTxn {
            txn_id: 61,
            table_id: 10,
            partition_id: 21,
            base_version: 0,
            commit_version: 1,
            state: ManagedTxnState::Visible,
            retry_at_ms: None,
            updated_at_ms: 0,
        });
        store
            .replace_managed_snapshot(&snapshot)
            .expect("persist snapshot");

        store
            .purge_retired_partition_metadata(20)
            .expect("purge retired partition");

        let loaded = store.load_snapshot().expect("load snapshot");
        assert_eq!(loaded.managed.tables.len(), 1);
        assert_eq!(loaded.managed.partitions.len(), 1);
        assert_eq!(loaded.managed.partitions[0].partition_id, 21);
        assert_eq!(loaded.managed.indexes.len(), 1);
        assert_eq!(loaded.managed.indexes[0].partition_id, 21);
        assert_eq!(loaded.managed.tablets.len(), 1);
        assert_eq!(loaded.managed.tablets[0].partition_id, 21);
        assert_eq!(loaded.managed.txns.len(), 1);
        assert_eq!(loaded.managed.txns[0].partition_id, 21);
    }

    #[test]
    fn init_schema_v4_creates_tables_with_kind_and_materialized_views_table() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = SqliteMetadataStore::open(dir.path().join("standalone.sqlite"))
            .expect("open fresh store");
        let conn = store.connection().expect("connection");

        let version: i64 = conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("user_version");
        assert_eq!(version, 4);

        // `tables` must have the new `kind` column with the expected default and check.
        let kind_col_exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('tables') WHERE name = 'kind'",
                [],
                |row| row.get(0),
            )
            .expect("pragma_table_info tables");
        assert_eq!(kind_col_exists, 1);

        // `materialized_views` must exist with mv_id primary key and the declared columns.
        let mv_cols: Vec<(String, String, i64)> = {
            let mut stmt = conn
                .prepare("SELECT name, type, \"notnull\" FROM pragma_table_info('materialized_views') ORDER BY cid")
                .expect("prepare");
            let rows = stmt
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })
                .expect("query")
                .collect::<Result<Vec<_>, _>>()
                .expect("collect");
            rows
        };
        let names: Vec<&str> = mv_cols.iter().map(|(n, _, _)| n.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "mv_id",
                "select_sql",
                "refresh_mode",
                "base_table_refs_json",
                "last_refresh_ms",
                "last_refresh_rows",
                "last_refresh_snapshots_json",
                "created_at_ms",
            ],
        );
    }

    #[test]
    fn init_schema_rejects_pre_v4_database() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("old.sqlite");
        {
            let conn = rusqlite::Connection::open(&path).expect("open");
            conn.execute_batch("PRAGMA user_version = 3;")
                .expect("set old version");
        }
        let err = SqliteMetadataStore::open(&path).expect_err("open on v3 must fail");
        assert!(err.contains("schema version 3"), "err={err}");
    }

    #[test]
    fn managed_snapshot_round_trips_mv_rows_and_kind_column() {
        use std::collections::BTreeMap;

        let dir = tempfile::tempdir().expect("tempdir");
        let store = SqliteMetadataStore::open(dir.path().join("standalone.sqlite")).expect("open");

        let mut snapshot = ManagedSnapshot {
            global: ManagedGlobalMeta {
                warehouse_uri: "s3://bucket/warehouse".to_string(),
                next_db_id: 2,
                next_table_id: 11,
                next_partition_id: 21,
                next_index_id: 31,
                next_tablet_id: 43,
                next_txn_id: 1,
            },
            databases: vec![StoredManagedDatabase {
                db_id: 1,
                name: "analytics".to_string(),
            }],
            tables: vec![StoredManagedTable {
                table_id: 10,
                db_id: 1,
                name: "orders_mv".to_string(),
                keys_type: "DUP_KEYS".to_string(),
                bucket_num: 2,
                current_schema_id: 10,
                state: ManagedTableState::Active,
                kind: ManagedTableKind::MaterializedView,
            }],
            schemas: vec![],
            columns: vec![],
            partitions: vec![StoredManagedPartition {
                partition_id: 20,
                table_id: 10,
                name: "p0".to_string(),
                visible_version: 1,
                next_version: 2,
                state: ManagedPartitionState::Active,
            }],
            indexes: vec![StoredManagedIndex {
                index_id: 30,
                table_id: 10,
                partition_id: 20,
                index_type: "BASE".to_string(),
                state: ManagedIndexState::Active,
            }],
            tablets: vec![
                StoredManagedTablet {
                    tablet_id: 40,
                    partition_id: 20,
                    index_id: 30,
                    bucket_seq: 0,
                    tablet_root_path: "s3://bucket/warehouse/db_1/table_10/partition_20"
                        .to_string(),
                },
                StoredManagedTablet {
                    tablet_id: 41,
                    partition_id: 20,
                    index_id: 30,
                    bucket_seq: 1,
                    tablet_root_path: "s3://bucket/warehouse/db_1/table_10/partition_20"
                        .to_string(),
                },
            ],
            txns: vec![],
            erase_jobs: vec![],
            materialized_views: vec![StoredMaterializedView {
                mv_id: 10,
                select_sql: "SELECT k1, sum(v2) FROM iceberg_cat.ns.orders GROUP BY k1".to_string(),
                refresh_mode: ManagedMvRefreshMode::DeferredManual,
                base_table_refs: vec![IcebergTableRef {
                    catalog: "iceberg_cat".to_string(),
                    namespace: "ns".to_string(),
                    table: "orders".to_string(),
                }],
                last_refresh_ms: Some(1_700_000_000_000),
                last_refresh_rows: Some(123),
                last_refresh_snapshots: {
                    let mut map = BTreeMap::new();
                    map.insert("iceberg_cat.ns.orders".to_string(), 7_391_842_i64);
                    map
                },
                created_at_ms: 1_699_999_999_000,
            }],
        };

        store
            .replace_managed_snapshot(&mut snapshot)
            .expect("persist");
        let loaded = store.load_snapshot().expect("reload").managed;
        assert_eq!(loaded, snapshot);
    }

    #[test]
    fn managed_snapshot_round_trips_kind_table_default() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = SqliteMetadataStore::open(dir.path().join("standalone.sqlite")).expect("open");

        let mut snapshot = ManagedSnapshot::default();
        snapshot.global.warehouse_uri = "s3://bucket/warehouse".to_string();
        snapshot.global.next_table_id = 2;
        snapshot.tables.push(StoredManagedTable {
            table_id: 1,
            db_id: 1,
            name: "orders".to_string(),
            keys_type: "DUP_KEYS".to_string(),
            bucket_num: 2,
            current_schema_id: 1,
            state: ManagedTableState::Active,
            kind: ManagedTableKind::Table,
        });
        snapshot.databases.push(StoredManagedDatabase {
            db_id: 1,
            name: "analytics".to_string(),
        });

        store
            .replace_managed_snapshot(&mut snapshot)
            .expect("persist");
        let loaded = store.load_snapshot().expect("reload").managed;
        assert_eq!(loaded.tables[0].kind, ManagedTableKind::Table);
        assert!(loaded.materialized_views.is_empty());
    }

    fn empty_mv_refresh_snapshot(warehouse: &str) -> ManagedSnapshot {
        let mut snapshot = ManagedSnapshot::default();
        snapshot.global.warehouse_uri = warehouse.to_string();
        snapshot.global.next_db_id = 2;
        snapshot.global.next_table_id = 11;
        snapshot.global.next_partition_id = 21;
        snapshot.global.next_index_id = 31;
        snapshot.global.next_tablet_id = 43;
        snapshot.global.next_txn_id = 1;
        snapshot.databases.push(StoredManagedDatabase {
            db_id: 1,
            name: "analytics".to_string(),
        });
        snapshot.tables.push(StoredManagedTable {
            table_id: 10,
            db_id: 1,
            name: "orders_mv".to_string(),
            keys_type: "DUP_KEYS".to_string(),
            bucket_num: 2,
            current_schema_id: 10,
            state: ManagedTableState::Active,
            kind: ManagedTableKind::MaterializedView,
        });
        snapshot.partitions.push(StoredManagedPartition {
            partition_id: 20,
            table_id: 10,
            name: "p0".to_string(),
            visible_version: 1,
            next_version: 2,
            state: ManagedPartitionState::Active,
        });
        snapshot.indexes.push(StoredManagedIndex {
            index_id: 30,
            table_id: 10,
            partition_id: 20,
            index_type: "BASE".to_string(),
            state: ManagedIndexState::Active,
        });
        for bucket_seq in 0..2 {
            snapshot.tablets.push(StoredManagedTablet {
                tablet_id: 40 + bucket_seq,
                partition_id: 20,
                index_id: 30,
                bucket_seq,
                tablet_root_path: "s3://bucket/warehouse/db_1/table_10/partition_20".to_string(),
            });
        }
        snapshot.materialized_views.push(StoredMaterializedView {
            mv_id: 10,
            select_sql: "SELECT k1 FROM iceberg_cat.ns.orders".to_string(),
            refresh_mode: ManagedMvRefreshMode::DeferredManual,
            base_table_refs: vec![IcebergTableRef {
                catalog: "iceberg_cat".to_string(),
                namespace: "ns".to_string(),
                table: "orders".to_string(),
            }],
            last_refresh_ms: None,
            last_refresh_rows: None,
            last_refresh_snapshots: std::collections::BTreeMap::new(),
            created_at_ms: 1_700_000_000_000,
        });
        snapshot
    }

    #[test]
    fn stage_mv_refresh_partition_rejects_when_refresh_already_in_progress() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = SqliteMetadataStore::open(dir.path().join("standalone.sqlite")).expect("open");
        let mut snapshot = empty_mv_refresh_snapshot("s3://bucket/warehouse");
        snapshot.partitions.push(StoredManagedPartition {
            partition_id: 22,
            table_id: 10,
            name: "p0".to_string(),
            visible_version: 1,
            next_version: 2,
            state: ManagedPartitionState::Creating,
        });
        store
            .replace_managed_snapshot(&mut snapshot)
            .expect("persist");

        let err = store
            .stage_mv_refresh_partition(StageMvRefreshRequest {
                table_id: 10,
                db_id: 1,
                bucket_num: 2,
                partition_name: "p0".to_string(),
                warehouse_uri: "s3://bucket/warehouse".to_string(),
            })
            .expect_err("stage should reject");
        assert!(err.contains("refresh already in progress"), "err={err}");
    }

    #[test]
    fn stage_mv_refresh_partition_rejects_when_mv_not_active() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = SqliteMetadataStore::open(dir.path().join("standalone.sqlite")).expect("open");
        let mut snapshot = empty_mv_refresh_snapshot("s3://bucket/warehouse");
        snapshot.tables[0].state = ManagedTableState::Dropping;
        store
            .replace_managed_snapshot(&mut snapshot)
            .expect("persist");

        let err = store
            .stage_mv_refresh_partition(StageMvRefreshRequest {
                table_id: 10,
                db_id: 1,
                bucket_num: 2,
                partition_name: "p0".to_string(),
                warehouse_uri: "s3://bucket/warehouse".to_string(),
            })
            .expect_err("stage should reject");
        assert!(err.contains("is not active"), "err={err}");
    }

    #[test]
    fn activate_mv_refresh_partition_swaps_and_writes_last_refresh_fields() {
        use std::collections::BTreeMap;

        let dir = tempfile::tempdir().expect("tempdir");
        let store = SqliteMetadataStore::open(dir.path().join("standalone.sqlite")).expect("open");
        let mut snapshot = empty_mv_refresh_snapshot("s3://bucket/warehouse");
        store
            .replace_managed_snapshot(&mut snapshot)
            .expect("persist");

        let staged = store
            .stage_mv_refresh_partition(StageMvRefreshRequest {
                table_id: 10,
                db_id: 1,
                bucket_num: 2,
                partition_name: "p0".to_string(),
                warehouse_uri: "s3://bucket/warehouse".to_string(),
            })
            .expect("stage");

        let mut snapshots_map = BTreeMap::new();
        snapshots_map.insert("iceberg_cat.ns.orders".to_string(), 9_999_i64);

        store
            .activate_mv_refresh_partition(ActivateMvRefreshRequest {
                table_id: 10,
                old_partition_id: 20,
                new_partition_id: staged.partition_id,
                new_index_id: staged.index_id,
                retired_root_path: "s3://bucket/warehouse/db_1/table_10/partition_20".to_string(),
                rows_written: 42,
                snapshots: snapshots_map.clone(),
            })
            .expect("activate");

        let loaded = store.load_snapshot().expect("reload").managed;
        let active_pids: Vec<i64> = loaded
            .partitions
            .iter()
            .filter(|p| p.state == ManagedPartitionState::Active)
            .map(|p| p.partition_id)
            .collect();
        assert_eq!(active_pids, vec![staged.partition_id]);
        assert!(
            loaded
                .partitions
                .iter()
                .any(|p| p.partition_id == 20 && p.state == ManagedPartitionState::Retired)
        );
        let erase_jobs: Vec<&StoredManagedEraseJob> = loaded
            .erase_jobs
            .iter()
            .filter(|j| j.partition_id == Some(20))
            .collect();
        assert_eq!(erase_jobs.len(), 1);
        assert_eq!(erase_jobs[0].job_kind, ManagedEraseJobKind::DropPartition);
        let mv = loaded
            .materialized_views
            .iter()
            .find(|mv| mv.mv_id == 10)
            .expect("mv row");
        assert_eq!(mv.last_refresh_rows, Some(42));
        assert_eq!(mv.last_refresh_snapshots, snapshots_map);
        assert!(mv.last_refresh_ms.is_some());
    }

    #[test]
    fn drop_managed_table_rejects_mv_with_inflight_refresh() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = SqliteMetadataStore::open(dir.path().join("standalone.sqlite")).expect("open");
        let mut snapshot = empty_mv_refresh_snapshot("s3://bucket/warehouse");
        snapshot.partitions.push(StoredManagedPartition {
            partition_id: 22,
            table_id: 10,
            name: "p0".to_string(),
            visible_version: 1,
            next_version: 2,
            state: ManagedPartitionState::Creating,
        });
        store
            .replace_managed_snapshot(&mut snapshot)
            .expect("persist");

        let err = store
            .drop_managed_table(10, "s3://bucket/warehouse/db_1/table_10")
            .expect_err("drop should reject");
        assert!(err.contains("refresh in progress"), "err={err}");
    }

    #[test]
    fn purge_retired_table_metadata_removes_mv_row() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = SqliteMetadataStore::open(dir.path().join("standalone.sqlite")).expect("open");
        let mut snapshot = empty_mv_refresh_snapshot("s3://bucket/warehouse");
        snapshot.tables[0].state = ManagedTableState::Dropping;
        for partition in &mut snapshot.partitions {
            partition.state = ManagedPartitionState::Retired;
        }
        for index in &mut snapshot.indexes {
            index.state = ManagedIndexState::Retired;
        }
        store
            .replace_managed_snapshot(&mut snapshot)
            .expect("persist");

        store.purge_retired_table_metadata(10).expect("purge");
        let loaded = store.load_snapshot().expect("reload").managed;
        assert!(loaded.tables.is_empty());
        assert!(loaded.materialized_views.is_empty());
    }
}
