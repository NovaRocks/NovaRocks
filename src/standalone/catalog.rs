use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};

use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TableStorage {
    LocalParquetFile { path: PathBuf },
}

#[derive(Clone, Debug, PartialEq)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub storage: TableStorage,
}

#[derive(Clone, Debug, PartialEq)]
struct DatabaseDef {
    tables: HashMap<String, TableDef>,
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
            },
        );
        Self { databases }
    }
}

impl InMemoryCatalog {
    pub(crate) fn create_database(&mut self, database_name: &str) -> Result<(), String> {
        let key = normalize_identifier(database_name)?;
        if self.databases.contains_key(&key) {
            return Err(format!("database already exists: {database_name}"));
        }
        self.databases.insert(
            key,
            DatabaseDef {
                tables: HashMap::new(),
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
        if let Some(existing) = db.tables.get(&table_key) {
            if existing == &table {
                return Ok(());
            }
            return Err(format!("table already exists: {}", table.name));
        }
        db.tables.insert(table_key, table);
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
}

pub(crate) fn normalize_identifier(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
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
