use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

// Re-export from sql::catalog so existing `crate::standalone::catalog::*` paths continue to work.
pub use crate::sql::catalog::{
    CatalogProvider, ColumnDef, ManagedTabletRef, PhysicalTableLayout, TableDef, TableStorage,
};

#[derive(Clone, Debug)]
struct DatabaseDef {
    tables: HashMap<String, TableDef>,
    physical_layouts: HashMap<String, PhysicalTableLayout>,
}

pub(crate) struct InMemoryCatalog {
    databases: HashMap<String, DatabaseDef>,
}

pub(crate) const DEFAULT_DATABASE: &str = "default";

impl Default for InMemoryCatalog {
    fn default() -> Self {
        let mut databases = HashMap::new();
        databases.insert(
            DEFAULT_DATABASE.to_string(),
            DatabaseDef {
                tables: HashMap::new(),
                physical_layouts: HashMap::new(),
            },
        );
        Self { databases }
    }
}

impl InMemoryCatalog {
    pub(crate) fn create_database(&mut self, database_name: &str) -> Result<(), String> {
        let key = normalize_identifier(database_name)?;
        if self.databases.contains_key(&key) {
            return Ok(()); // idempotent — matches IF NOT EXISTS semantics
        }
        self.databases.insert(
            key,
            DatabaseDef {
                tables: HashMap::new(),
                physical_layouts: HashMap::new(),
            },
        );
        Ok(())
    }

    pub(crate) fn database_exists(&self, database_name: &str) -> Result<bool, String> {
        let key = normalize_identifier(database_name)?;
        Ok(self.databases.contains_key(&key))
    }

    pub(crate) fn register(&mut self, database_name: &str, table: TableDef) -> Result<(), String> {
        let db_key = normalize_identifier(database_name)?;
        let db = self
            .databases
            .get_mut(&db_key)
            .ok_or_else(|| format!("unknown database: {database_name}"))?;
        let table_key = normalize_identifier(&table.name)?;
        if db.tables.contains_key(&table_key) {
            // Allow re-registration (overwrite) — callers use this to update storage
            db.physical_layouts.remove(&table_key);
            db.tables.insert(table_key, table);
            return Ok(());
        }
        db.physical_layouts.remove(&table_key);
        db.tables.insert(table_key, table);
        Ok(())
    }

    pub(crate) fn register_managed_table(
        &mut self,
        database_name: &str,
        table: TableDef,
        physical_layout: PhysicalTableLayout,
    ) -> Result<(), String> {
        let db_key = normalize_identifier(database_name)?;
        let db = self
            .databases
            .get_mut(&db_key)
            .ok_or_else(|| format!("unknown database: {database_name}"))?;
        let table_key = normalize_identifier(&table.name)?;
        db.tables.insert(table_key.clone(), table);
        db.physical_layouts.insert(table_key, physical_layout);
        Ok(())
    }

    pub(crate) fn drop_table(
        &mut self,
        database_name: &str,
        table_name: &str,
    ) -> Result<(), String> {
        let db_key = normalize_identifier(database_name)?;
        let db = self
            .databases
            .get_mut(&db_key)
            .ok_or_else(|| format!("unknown database: {database_name}"))?;
        let table_key = normalize_identifier(table_name)?;
        db.tables
            .remove(&table_key)
            .ok_or_else(|| format!("unknown table: {table_name}"))?;
        db.physical_layouts.remove(&table_key);
        Ok(())
    }

    pub(crate) fn drop_database(&mut self, database_name: &str) -> Result<(), String> {
        let key = normalize_identifier(database_name)?;
        if key == DEFAULT_DATABASE {
            return Err("cannot drop default database".to_string());
        }
        self.databases
            .remove(&key)
            .ok_or_else(|| format!("unknown database: {database_name}"))?;
        Ok(())
    }

    pub(crate) fn get(&self, database_name: &str, table_name: &str) -> Result<TableDef, String> {
        let db_key = normalize_identifier(database_name)?;
        let table_key = normalize_identifier(table_name)?;
        self.databases
            .get(&db_key)
            .ok_or_else(|| format!("unknown database: {database_name}"))?
            .tables
            .get(&table_key)
            .cloned()
            .ok_or_else(|| format!("unknown table: {table_name}"))
    }

    pub(crate) fn get_physical_layout(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Result<Option<PhysicalTableLayout>, String> {
        let db_key = normalize_identifier(database_name)?;
        let table_key = normalize_identifier(table_name)?;
        Ok(self
            .databases
            .get(&db_key)
            .ok_or_else(|| format!("unknown database: {database_name}"))?
            .physical_layouts
            .get(&table_key)
            .cloned())
    }
}

impl CatalogProvider for InMemoryCatalog {
    fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String> {
        self.get(database, table)
    }

    fn get_physical_layout(
        &self,
        database: &str,
        table: &str,
    ) -> Result<Option<PhysicalTableLayout>, String> {
        self.get_physical_layout(database, table)
    }
}

pub(crate) fn normalize_identifier(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    // Strip backtick quotes if present
    let trimmed = trimmed
        .strip_prefix('`')
        .and_then(|s| s.strip_suffix('`'))
        .unwrap_or(trimmed);
    if trimmed.is_empty() {
        return Err("identifier is empty".to_string());
    }
    let mut chars = trimmed.chars();
    let Some(first) = chars.next() else {
        return Err("identifier is empty".to_string());
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return Err(format!("unsupported identifier `{trimmed}`"));
    }
    if !chars.all(|c| c == '_' || c.is_ascii_alphanumeric()) {
        return Err(format!("unsupported identifier `{trimmed}`"));
    }
    Ok(trimmed.to_ascii_lowercase())
}

pub(crate) fn build_parquet_table(
    table_name: &str,
    path: impl AsRef<Path>,
) -> Result<TableDef, String> {
    let normalized_name = normalize_identifier(table_name)?;
    let path = std::fs::canonicalize(path.as_ref())
        .map_err(|e| format!("canonicalize parquet path failed: {e}"))?;
    let file = File::open(&path).map_err(|e| format!("open parquet file failed: {e}"))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("open parquet metadata failed: {e}"))?;
    let schema = builder.schema();
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        columns.push(ColumnDef {
            name: field.name().clone(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
        });
    }
    Ok(TableDef {
        name: normalized_name,
        columns,
        storage: TableStorage::LocalParquetFile { path },
    })
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;

    fn test_table(name: &str) -> TableDef {
        TableDef {
            name: name.to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
            }],
            storage: TableStorage::S3ParquetFiles {
                files: vec![],
                cloud_properties: Default::default(),
            },
        }
    }

    #[test]
    fn register_managed_table_tracks_and_clears_physical_layout() {
        let mut catalog = InMemoryCatalog::default();
        let layout = PhysicalTableLayout {
            db_id: 10,
            table_id: 20,
            schema_id: 30,
            tablets: vec![ManagedTabletRef {
                tablet_id: 40,
                partition_id: 50,
                version: 60,
            }],
        };

        catalog
            .register_managed_table(DEFAULT_DATABASE, test_table("managed_tbl"), layout.clone())
            .expect("register managed table");
        assert_eq!(
            catalog
                .get_physical_layout(DEFAULT_DATABASE, "managed_tbl")
                .expect("physical layout lookup"),
            Some(layout.clone())
        );

        catalog
            .register(DEFAULT_DATABASE, test_table("managed_tbl"))
            .expect("overwrite with logical table");
        assert_eq!(
            catalog
                .get_physical_layout(DEFAULT_DATABASE, "managed_tbl")
                .expect("physical layout cleared"),
            None
        );
    }
}
