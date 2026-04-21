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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PreparedManagedTxn {
    pub txn_id: i64,
    pub table_id: i64,
    pub partition_id: i64,
    pub base_version: i64,
    pub commit_version: i64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum ManagedTableState {
    Creating,
    #[default]
    Active,
    Failed,
}

impl ManagedTableState {
    fn as_sql_str(self) -> &'static str {
        match self {
            Self::Creating => "CREATING",
            Self::Active => "ACTIVE",
            Self::Failed => "FAILED",
        }
    }

    fn from_sql_str(value: &str) -> Result<Self, String> {
        match value {
            "CREATING" => Ok(Self::Creating),
            "ACTIVE" => Ok(Self::Active),
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
    Failed,
}

impl ManagedPartitionState {
    fn as_sql_str(self) -> &'static str {
        match self {
            Self::Creating => "CREATING",
            Self::Active => "ACTIVE",
            Self::Failed => "FAILED",
        }
    }

    fn from_sql_str(value: &str) -> Result<Self, String> {
        match value {
            "CREATING" => Ok(Self::Creating),
            "ACTIVE" => Ok(Self::Active),
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
    Failed,
}

impl ManagedIndexState {
    fn as_sql_str(self) -> &'static str {
        match self {
            Self::Creating => "CREATING",
            Self::Active => "ACTIVE",
            Self::Failed => "FAILED",
        }
    }

    fn from_sql_str(value: &str) -> Result<Self, String> {
        match value {
            "CREATING" => Ok(Self::Creating),
            "ACTIVE" => Ok(Self::Active),
            "FAILED" => Ok(Self::Failed),
            _ => Err(format!("unknown managed index state `{value}`")),
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
                        state
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        table.table_id,
                        table.db_id,
                        table.name,
                        table.keys_type,
                        table.bucket_num,
                        table.current_schema_id,
                        table.state.as_sql_str(),
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
        }

        tx.commit()
            .map_err(|e| format!("commit managed snapshot failed: {e}"))?;
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
        Connection::open(&self.path).map_err(|e| {
            format!(
                "open standalone metadata db {} failed: {e}",
                self.path.display()
            )
        })
    }

    fn init_schema(&self) -> Result<(), String> {
        let conn = self.connection()?;
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
            PRAGMA user_version = 2;
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
                        state
                     FROM tables
                     ORDER BY table_id",
                )
                .map_err(|e| format!("prepare managed tables query failed: {e}"))?;
            let rows = stmt
                .query_map([], |row| {
                    let state = ManagedTableState::from_sql_str(&row.get::<_, String>(6)?)
                        .map_err(invalid_state_sql_error)?;
                    Ok(StoredManagedTable {
                        table_id: row.get(0)?,
                        db_id: row.get(1)?,
                        name: row.get(2)?,
                        keys_type: row.get(3)?,
                        bucket_num: row.get(4)?,
                        current_schema_id: row.get(5)?,
                        state,
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

        if global == ManagedGlobalMeta::default()
            && (!databases.is_empty()
                || !tables.is_empty()
                || !schemas.is_empty()
                || !columns.is_empty()
                || !partitions.is_empty()
                || !indexes.is_empty()
                || !tablets.is_empty()
                || !txns.is_empty())
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
        })
    }
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
        ManagedGlobalMeta, ManagedIndexState, ManagedPartitionState, ManagedSnapshot,
        ManagedTableState, ManagedTxnState, SqliteMetadataStore, StoredManagedColumn,
        StoredManagedDatabase, StoredManagedIndex, StoredManagedPartition, StoredManagedSchema,
        StoredManagedTable, StoredManagedTablet, StoredManagedTxn,
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
}
