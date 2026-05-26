//! In-memory database/table catalog and shared catalog utilities.
//!
//! Holds the logical `InMemoryCatalog` (databases -> tables + physical
//! layouts) and the `normalize_identifier` helper used across the SQL
//! and engine layers. Everything here is backend-agnostic — the
//! managed-lake and iceberg subsystems both query this catalog for
//! table metadata.

use std::collections::HashMap;

// Re-export from sql::catalog so callers can use either
// `crate::engine::catalog::*` or `crate::sql::catalog::*`
// interchangeably without double-defining the types.
use crate::sql::catalog::LegacyRangePartition;
pub use crate::sql::catalog::{
    CatalogProvider, ColumnDef, ManagedTabletRef, PhysicalTableLayout, ScanSource, TableDef,
};

#[derive(Clone, Debug)]
struct DatabaseDef {
    tables: HashMap<String, TableDef>,
    physical_layouts: HashMap<String, PhysicalTableLayout>,
}

#[derive(Clone, Debug)]
pub(crate) struct InMemoryCatalog {
    databases: HashMap<String, DatabaseDef>,
    legacy_range_partitions: HashMap<(String, String), Vec<LegacyRangePartition>>,
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
        Self {
            databases,
            legacy_range_partitions: HashMap::new(),
        }
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

    pub(crate) fn database_names(&self) -> impl Iterator<Item = &str> {
        self.databases.keys().map(String::as_str)
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
        self.legacy_range_partitions.remove(&(db_key, table_key));
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

    pub(crate) fn set_legacy_range_partitions(
        &mut self,
        database_name: &str,
        table_name: &str,
        partitions: Vec<LegacyRangePartition>,
    ) -> Result<(), String> {
        let db_key = normalize_identifier(database_name)?;
        let table_key = normalize_identifier(table_name)?;
        if partitions.is_empty() {
            self.legacy_range_partitions.remove(&(db_key, table_key));
        } else {
            self.legacy_range_partitions
                .insert((db_key, table_key), partitions);
        }
        Ok(())
    }

    pub(crate) fn add_legacy_range_partition(
        &mut self,
        database_name: &str,
        table_name: &str,
        partition: LegacyRangePartition,
    ) -> Result<(), String> {
        let db_key = normalize_identifier(database_name)?;
        let table_key = normalize_identifier(table_name)?;
        let partition_key = normalize_identifier(&partition.name)?;
        let entries = self
            .legacy_range_partitions
            .entry((db_key, table_key))
            .or_default();
        entries.retain(|existing| {
            normalize_identifier(&existing.name).ok().as_deref() != Some(&partition_key)
        });
        entries.push(partition);
        Ok(())
    }

    pub(crate) fn rename_column(
        &mut self,
        database_name: &str,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), String> {
        let db_key = normalize_identifier(database_name)?;
        let table_key = normalize_identifier(table_name)?;
        let old_key = normalize_identifier(old_name)?;
        let new_key = normalize_identifier(new_name)?;
        let db = self
            .databases
            .get_mut(&db_key)
            .ok_or_else(|| format!("unknown database: {database_name}"))?;
        let table = db
            .tables
            .get_mut(&table_key)
            .ok_or_else(|| format!("unknown table: {table_name}"))?;
        if table
            .columns
            .iter()
            .any(|column| normalize_identifier(&column.name).ok().as_deref() == Some(&new_key))
        {
            return Err(format!("column `{new_name}` already exists"));
        }
        let column = table
            .columns
            .iter_mut()
            .find(|column| normalize_identifier(&column.name).ok().as_deref() == Some(&old_key))
            .ok_or_else(|| format!("unknown column `{old_name}`"))?;
        column.name = new_key.clone();

        if let Some(partitions) = self
            .legacy_range_partitions
            .get_mut(&(db_key.clone(), table_key.clone()))
        {
            for partition in partitions {
                if normalize_identifier(&partition.column).ok().as_deref() == Some(&old_key) {
                    partition.column = new_key.clone();
                }
            }
        }
        Ok(())
    }
}

impl CatalogProvider for InMemoryCatalog {
    fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String> {
        self.get(database, table)
    }

    fn get_legacy_range_partition(
        &self,
        database: &str,
        table: &str,
        partition: &str,
    ) -> Result<Option<LegacyRangePartition>, String> {
        let db_key = normalize_identifier(database)?;
        let table_key = normalize_identifier(table)?;
        let partition_key = normalize_identifier(partition)?;
        Ok(self
            .legacy_range_partitions
            .get(&(db_key, table_key))
            .and_then(|partitions| {
                partitions
                    .iter()
                    .find(|p| normalize_identifier(&p.name).ok().as_deref() == Some(&partition_key))
                    .cloned()
            }))
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
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            iceberg_table: None,
            source: ScanSource::StarRocks,
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
