use std::path::{Path, PathBuf};

use rusqlite::{Connection, params};

#[derive(Clone, Debug)]
pub(crate) struct SqliteMetadataStore {
    path: PathBuf,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct MetadataSnapshot {
    pub local_databases: Vec<String>,
    pub local_tables: Vec<StoredLocalTable>,
    pub iceberg_catalogs: Vec<StoredIcebergCatalog>,
    pub iceberg_namespaces: Vec<StoredIcebergNamespace>,
    pub iceberg_tables: Vec<StoredIcebergTable>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StoredLocalTable {
    pub database: String,
    pub table: String,
    pub path: PathBuf,
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

        let local_tables = {
            let mut stmt = conn
                .prepare(
                    "SELECT database_name, table_name, path
                     FROM local_tables
                     ORDER BY database_name, table_name",
                )
                .map_err(|e| format!("prepare local_tables query failed: {e}"))?;
            let rows = stmt
                .query_map([], |row| {
                    Ok(StoredLocalTable {
                        database: row.get(0)?,
                        table: row.get(1)?,
                        path: PathBuf::from(row.get::<_, String>(2)?),
                    })
                })
                .map_err(|e| format!("query local_tables failed: {e}"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("read local_tables failed: {e}"))?
        };

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
            local_tables,
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

    pub(crate) fn upsert_local_table(
        &self,
        database_name: &str,
        table_name: &str,
        path: &Path,
    ) -> Result<(), String> {
        let conn = self.connection()?;
        conn.execute(
            "INSERT INTO local_tables(database_name, table_name, path)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(database_name, table_name) DO UPDATE SET path = excluded.path",
            params![database_name, table_name, path.display().to_string()],
        )
        .map_err(|e| format!("persist local table failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn delete_local_table(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Result<(), String> {
        let conn = self.connection()?;
        conn.execute(
            "DELETE FROM local_tables WHERE database_name = ?1 AND table_name = ?2",
            params![database_name, table_name],
        )
        .map_err(|e| format!("delete local table metadata failed: {e}"))?;
        Ok(())
    }

    pub(crate) fn delete_local_database(&self, database_name: &str) -> Result<(), String> {
        let conn = self.connection()?;
        conn.execute(
            "DELETE FROM local_databases WHERE name = ?1",
            params![database_name],
        )
        .map_err(|e| format!("delete local database metadata failed: {e}"))?;
        conn.execute(
            "DELETE FROM local_tables WHERE database_name = ?1",
            params![database_name],
        )
        .map_err(|e| format!("delete local database tables metadata failed: {e}"))?;
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
            PRAGMA synchronous = NORMAL;
            CREATE TABLE IF NOT EXISTS local_databases (
                name TEXT PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS local_tables (
                database_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                path TEXT NOT NULL,
                PRIMARY KEY (database_name, table_name)
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
            PRAGMA user_version = 1;
            ",
        )
        .map_err(|e| format!("initialize standalone metadata schema failed: {e}"))?;
        Ok(())
    }
}

fn json_to_sql_error(err: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(err))
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
