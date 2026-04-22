//! Local (in-memory + parquet-on-disk) table subsystem.
//!
//! Owns the in-memory catalog (`InMemoryCatalog`), the `LocalTableSemantics`
//! book-keeping for aggregate/primary-key tables, parquet I/O, the insert
//! path, stream load, and aggregate merge — everything that lives on disk as
//! a parquet file plus the in-memory catalog metadata pointing at it.

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::sql::parser::ast::{ColumnAggregation, ObjectName, TableColumnDef, TableKeyDesc};

// Re-export from sql::catalog so existing `crate::standalone::engine::local::*`
// paths and the old `crate::standalone::catalog::*` alias continue to work.
pub use crate::sql::catalog::{
    CatalogProvider, ColumnDef, ManagedTabletRef, PhysicalTableLayout, TableDef, TableStorage,
};

use super::sqlparse::expr::sql_type_to_arrow_type;

pub(crate) mod aggregate;
pub(crate) mod insert;
pub(crate) mod parquet;
pub(crate) mod stream_load;

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

// ---------------------------------------------------------------------------
// LocalTableSemantics and semantics helpers
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub(crate) struct LocalTableSemantics {
    pub(crate) key_desc: Option<TableKeyDesc>,
    pub(crate) column_aggregations: HashMap<String, ColumnAggregation>,
}

/// Create a local parquet table from SQL column definitions.
pub(crate) fn create_local_table_from_columns(
    state: &Arc<crate::standalone::engine::StandaloneState>,
    name: &ObjectName,
    current_database: &str,
    columns: &[TableColumnDef],
    key_desc: Option<&TableKeyDesc>,
) -> Result<crate::standalone::engine::StatementResult, String> {
    use crate::standalone::engine::{StatementResult, persist_local_table_if_needed};

    let resolved = crate::standalone::engine::resolve_local_table_name(name, current_database)?;

    // Convert SQL columns to Arrow fields
    let arrow_fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            let dt = sql_type_to_arrow_type(&col.data_type)?;
            Ok(Field::new(&col.name, dt, col.nullable))
        })
        .collect::<Result<Vec<_>, String>>()?;

    let arrow_schema = Arc::new(Schema::new(arrow_fields.clone()));

    // Build ColumnDefs for the catalog
    let catalog_columns: Vec<ColumnDef> = arrow_fields
        .iter()
        .map(|f| ColumnDef {
            name: f.name().clone(),
            data_type: f.data_type().clone(),
            nullable: f.is_nullable(),
        })
        .collect();

    // Create a temporary directory for the table data
    let data_dir = std::env::temp_dir().join("novarocks_local_tables");
    std::fs::create_dir_all(&data_dir)
        .map_err(|e| format!("create local table data directory failed: {e}"))?;
    let table_file = data_dir.join(format!("{}_{}.parquet", resolved.database, resolved.table));

    // Write an empty parquet file with the schema
    let empty_arrays: Vec<ArrayRef> = arrow_fields
        .iter()
        .map(|f| arrow::array::new_empty_array(f.data_type()))
        .collect();
    let empty_batch = RecordBatch::try_new(Arc::clone(&arrow_schema), empty_arrays)
        .map_err(|e| format!("build empty batch failed: {e}"))?;
    self::parquet::write_parquet_to_path(&table_file, &empty_batch)?;

    // Register in catalog
    let table_def = TableDef {
        name: normalize_identifier(name.leaf())?,
        columns: catalog_columns,
        storage: TableStorage::LocalParquetFile {
            path: table_file.clone(),
        },
    };
    let mut guard = state
        .catalog
        .write()
        .expect("standalone catalog write lock");
    guard.register(&resolved.database, table_def)?;
    drop(guard);
    update_local_table_semantics(
        state,
        &resolved.database,
        &resolved.table,
        columns,
        key_desc,
    )?;
    persist_local_table_if_needed(state, &resolved.database, &resolved.table, &table_file)?;
    Ok(StatementResult::Ok)
}

pub(crate) fn update_local_table_semantics(
    state: &Arc<crate::standalone::engine::StandaloneState>,
    database_name: &str,
    table_name: &str,
    columns: &[TableColumnDef],
    key_desc: Option<&TableKeyDesc>,
) -> Result<(), String> {
    let key = (
        normalize_identifier(database_name)?,
        normalize_identifier(table_name)?,
    );
    let column_aggregations = columns
        .iter()
        .filter_map(|column| {
            column.aggregation.map(|aggregation| {
                Ok::<_, String>((normalize_identifier(&column.name)?, aggregation))
            })
        })
        .collect::<Result<HashMap<_, _>, _>>()?;
    let semantics = LocalTableSemantics {
        key_desc: key_desc.cloned(),
        column_aggregations,
    };
    state
        .local_table_semantics
        .write()
        .expect("standalone local table semantics write lock")
        .insert(key, semantics);
    Ok(())
}

pub(crate) fn get_local_table_semantics(
    state: &Arc<crate::standalone::engine::StandaloneState>,
    database_name: &str,
    table_name: &str,
) -> Result<Option<LocalTableSemantics>, String> {
    let key = (
        normalize_identifier(database_name)?,
        normalize_identifier(table_name)?,
    );
    Ok(state
        .local_table_semantics
        .read()
        .expect("standalone local table semantics read lock")
        .get(&key)
        .cloned())
}

pub(crate) fn remove_local_table_semantics(
    state: &Arc<crate::standalone::engine::StandaloneState>,
    database_name: &str,
    table_name: &str,
) -> Result<(), String> {
    let key = (
        normalize_identifier(database_name)?,
        normalize_identifier(table_name)?,
    );
    state
        .local_table_semantics
        .write()
        .expect("standalone local table semantics write lock")
        .remove(&key);
    Ok(())
}

pub(crate) fn remove_local_database_semantics(
    state: &Arc<crate::standalone::engine::StandaloneState>,
    database_name: &str,
) -> Result<(), String> {
    let database_key = normalize_identifier(database_name)?;
    state
        .local_table_semantics
        .write()
        .expect("standalone local table semantics write lock")
        .retain(|(db, _), _| db != &database_key);
    Ok(())
}

pub(crate) fn apply_local_table_semantics_if_needed(
    state: &Arc<crate::standalone::engine::StandaloneState>,
    resolved: &crate::standalone::engine::ResolvedLocalTableName,
    columns: &[ColumnDef],
    batch: RecordBatch,
) -> Result<RecordBatch, String> {
    use self::aggregate::merge_aggregate_table_rows_if_needed;
    use self::insert::build_local_insert_batch;

    let Some(semantics) = get_local_table_semantics(state, &resolved.database, &resolved.table)?
    else {
        return Ok(batch);
    };
    let Some(merged_rows) = merge_aggregate_table_rows_if_needed(
        columns,
        semantics.key_desc.as_ref(),
        &semantics.column_aggregations,
        &batch,
    )?
    else {
        return Ok(batch);
    };
    build_local_insert_batch(columns, &merged_rows)
}

// ---------------------------------------------------------------------------
// Dual table (virtual 1-row table for SELECT without FROM)
// ---------------------------------------------------------------------------

pub(crate) fn ensure_dual_table(
    state: &Arc<crate::standalone::engine::StandaloneState>,
) -> Result<(), String> {
    ensure_dual_in_database(state, DEFAULT_DATABASE)
}

pub(crate) fn ensure_dual_in_database(
    state: &Arc<crate::standalone::engine::StandaloneState>,
    database: &str,
) -> Result<(), String> {
    let guard = state.catalog.read().expect("standalone catalog read lock");
    if guard.get(database, "__dual__").is_ok() {
        return Ok(());
    }
    drop(guard);

    // Create a 1-row parquet with a single dummy column
    let dir = std::env::temp_dir().join("novarocks_dual");
    std::fs::create_dir_all(&dir).map_err(|e| format!("create dual table dir failed: {e}"))?;
    let path = dir.join(format!("dual_{}.parquet", database));
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("__dummy__", arrow::datatypes::DataType::Int8, true),
    ]));
    let col = std::sync::Arc::new(arrow::array::Int8Array::from(vec![Some(0i8)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![col])
        .map_err(|e| format!("build dual batch failed: {e}"))?;
    self::parquet::write_parquet_to_path(&path, &batch)?;

    let table = TableDef {
        name: "__dual__".to_string(),
        columns: vec![ColumnDef {
            name: "__dummy__".to_string(),
            data_type: arrow::datatypes::DataType::Int8,
            nullable: true,
        }],
        storage: TableStorage::LocalParquetFile { path },
    };
    let mut guard = state
        .catalog
        .write()
        .expect("standalone catalog write lock");
    guard.register(database, table).ok(); // ignore if already exists
    Ok(())
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
