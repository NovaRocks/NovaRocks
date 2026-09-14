// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Query-application-owned `information_schema` virtual-table registry.
//!
//! The query application owns both the narrow materialization contract and the
//! built-in providers. Role-local catalog readers supply only the immutable
//! names needed for a scan, so neither a provider nor a caller can obtain a
//! Frontend application aggregate through this boundary.

use std::collections::HashMap;
use std::sync::Arc;

use crate::api::{build_utf8_query_result, build_utf8_table_query_result};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use novarocks_types::schema::ColumnDef;

const INFORMATION_SCHEMA_DB: &str = "information_schema";

/// Minimal facts a system-table rewriter gathers from role-local catalog
/// readers. Providers receive no engine state or catalog-control capability.
pub struct SystemCatalogInputs<'a> {
    /// Value for the `catalog_name` column.
    pub catalog_name: &'a str,
    /// Sorted, deduplicated namespace names for the selected catalog.
    pub schema_names: &'a [String],
    /// Enumerated `(schema, table)` pairs only when the selected provider
    /// requires table facts.
    pub table_names: &'a [(String, String)],
}

/// Columns and materialized batches for one resolved system table.
pub struct SystemTableData {
    pub columns: Vec<ColumnDef>,
    pub batches: Vec<RecordBatch>,
}

/// Resolves a system-table reference from caller-supplied, read-only facts.
pub trait SystemCatalog: Send + Sync {
    /// `Ok(None)` leaves an unregistered reference for downstream resolution.
    fn resolve(
        &self,
        db: &str,
        tbl: &str,
        inputs: &SystemCatalogInputs<'_>,
    ) -> Result<Option<SystemTableData>, String>;
}

/// No-op catalog for query paths that deliberately do not install providers.
pub struct EmptySystemCatalog;

impl SystemCatalog for EmptySystemCatalog {
    fn resolve(
        &self,
        _db: &str,
        _tbl: &str,
        _inputs: &SystemCatalogInputs<'_>,
    ) -> Result<Option<SystemTableData>, String> {
        Ok(None)
    }
}

/// Contract for a single information_schema virtual table. Unlike the former
/// core trait, `scan` receives only the narrow inputs it needs — never engine
/// state.
trait VirtualTableProvider: Send + Sync {
    fn database(&self) -> &str;
    fn table(&self) -> &str;
    fn columns(&self) -> Vec<ColumnDef>;
    fn scan(&self, inputs: &SystemCatalogInputs<'_>) -> Result<Vec<RecordBatch>, String>;
}

struct VirtualTableRegistry {
    providers: HashMap<(String, String), Arc<dyn VirtualTableProvider>>,
}

impl VirtualTableRegistry {
    fn with_defaults() -> Self {
        let mut registry = Self {
            providers: HashMap::new(),
        };
        registry.register(Arc::new(SchemataProvider));
        registry.register(Arc::new(TablesProvider));
        registry
    }

    fn register(&mut self, provider: Arc<dyn VirtualTableProvider>) {
        let key = (
            provider.database().to_ascii_lowercase(),
            provider.table().to_ascii_lowercase(),
        );
        self.providers.insert(key, provider);
    }

    fn lookup(&self, database: &str, table: &str) -> Option<Arc<dyn VirtualTableProvider>> {
        self.providers
            .get(&(database.to_ascii_lowercase(), table.to_ascii_lowercase()))
            .cloned()
    }
}

const SCHEMATA_COLUMNS: &[(&str, bool)] = &[
    ("catalog_name", false),
    ("schema_name", false),
    ("default_character_set_name", false),
    ("default_collation_name", false),
    ("sql_path", true),
];

fn schemata_columns() -> Vec<ColumnDef> {
    SCHEMATA_COLUMNS
        .iter()
        .map(|(name, nullable)| ColumnDef {
            name: (*name).to_string(),
            data_type: DataType::Utf8,
            nullable: *nullable,
            write_default: None,
            logical_type: None,
        })
        .collect()
}

/// Build the single schemata `RecordBatch`: one row per schema, `catalog_name`
/// fixed to `catalog`. Byte-identical to the former core `build_schemata_batch`
/// (`information_schema.rs`); schema exactly matches `schemata_columns()`.
fn build_schemata_batch(catalog: &str, databases: &[String]) -> Result<Vec<RecordBatch>, String> {
    let rows = databases
        .iter()
        .map(|database| {
            vec![
                Some(catalog.to_owned()),
                Some(database.clone()),
                Some("utf8".to_owned()),
                Some("utf8_general_ci".to_owned()),
                None,
            ]
        })
        .collect();
    build_utf8_table_query_result(
        &[
            ("catalog_name", false),
            ("schema_name", false),
            ("default_character_set_name", false),
            ("default_collation_name", false),
            ("sql_path", true),
        ],
        rows,
    )
    .map(|result| result.into_batches())
    .map_err(|error| format!("build information_schema.schemata batch failed: {error}"))
}

const TABLES_COLUMNS: &[(&str, bool)] = &[
    ("table_catalog", false),
    ("table_schema", false),
    ("table_name", false),
    ("table_type", false),
];

fn tables_columns() -> Vec<ColumnDef> {
    TABLES_COLUMNS
        .iter()
        .map(|(name, nullable)| ColumnDef {
            name: (*name).to_string(),
            data_type: DataType::Utf8,
            nullable: *nullable,
            write_default: None,
            logical_type: None,
        })
        .collect()
}

/// One row per table, so a namespace's contents are knowable to SQL.
///
/// Without this a caller can only drop children it can already name, which is
/// no help to anyone who did not create them -- and on a catalog that cannot
/// enumerate views, `DROP DATABASE ... FORCE` is refused, so naming them is the
/// only way through.
fn build_tables_batch(
    catalog: &str,
    tables: &[(String, String)],
) -> Result<Vec<RecordBatch>, String> {
    // Every row is a base table: this listing comes from the catalog's table
    // enumeration, and a catalog that also holds views reports those through
    // the view metadata surface instead.
    let rows = tables
        .iter()
        .map(|(schema, name)| {
            vec![
                catalog.to_owned(),
                schema.clone(),
                name.clone(),
                "BASE TABLE".to_owned(),
            ]
        })
        .collect();
    build_utf8_query_result(
        &["table_catalog", "table_schema", "table_name", "table_type"],
        rows,
    )
    .map(|result| result.into_batches())
    .map_err(|error| format!("build information_schema.tables batch failed: {error}"))
}

struct TablesProvider;

impl VirtualTableProvider for TablesProvider {
    fn database(&self) -> &str {
        INFORMATION_SCHEMA_DB
    }

    fn table(&self) -> &str {
        "tables"
    }

    fn columns(&self) -> Vec<ColumnDef> {
        tables_columns()
    }

    fn scan(&self, inputs: &SystemCatalogInputs<'_>) -> Result<Vec<RecordBatch>, String> {
        build_tables_batch(inputs.catalog_name, inputs.table_names)
    }
}

struct SchemataProvider;

impl VirtualTableProvider for SchemataProvider {
    fn database(&self) -> &str {
        INFORMATION_SCHEMA_DB
    }

    fn table(&self) -> &str {
        "schemata"
    }

    fn columns(&self) -> Vec<ColumnDef> {
        schemata_columns()
    }

    fn scan(&self, inputs: &SystemCatalogInputs<'_>) -> Result<Vec<RecordBatch>, String> {
        build_schemata_batch(inputs.catalog_name, inputs.schema_names)
    }
}

/// The frontend system-catalog domain service.
pub struct SystemCatalogService {
    registry: VirtualTableRegistry,
}

impl SystemCatalogService {
    pub fn with_defaults() -> Self {
        Self {
            registry: VirtualTableRegistry::with_defaults(),
        }
    }
}

impl SystemCatalog for SystemCatalogService {
    fn resolve(
        &self,
        db: &str,
        tbl: &str,
        inputs: &SystemCatalogInputs<'_>,
    ) -> Result<Option<SystemTableData>, String> {
        match self.registry.lookup(db, tbl) {
            Some(provider) => Ok(Some(SystemTableData {
                columns: provider.columns(),
                batches: provider.scan(inputs)?,
            })),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray};

    #[test]
    fn empty_system_catalog_returns_none() {
        let names = vec!["a".to_string()];
        let inputs = SystemCatalogInputs {
            catalog_name: "default_catalog",
            schema_names: &names,
            table_names: &[],
        };

        assert!(
            EmptySystemCatalog
                .resolve("information_schema", "schemata", &inputs)
                .expect("empty catalog resolution must succeed")
                .is_none()
        );
    }

    fn inputs<'a>(catalog_name: &'a str, schema_names: &'a [String]) -> SystemCatalogInputs<'a> {
        SystemCatalogInputs {
            catalog_name,
            schema_names,
            table_names: &[],
        }
    }

    fn table_inputs<'a>(
        catalog_name: &'a str,
        table_names: &'a [(String, String)],
    ) -> SystemCatalogInputs<'a> {
        SystemCatalogInputs {
            catalog_name,
            schema_names: &[],
            table_names,
        }
    }

    #[test]
    fn resolve_schemata_returns_exact_columns() {
        let schema_names = vec!["db_a".to_string(), "db_b".to_string()];
        let resolved = SystemCatalogService::with_defaults()
            .resolve(
                "information_schema",
                "schemata",
                &inputs("default_catalog", &schema_names),
            )
            .expect("schemata resolution must succeed")
            .expect("schemata must be registered");

        let actual: Vec<_> = resolved
            .columns
            .iter()
            .map(|column| (column.name.as_str(), &column.data_type, column.nullable))
            .collect();
        assert_eq!(
            actual,
            vec![
                ("catalog_name", &DataType::Utf8, false),
                ("schema_name", &DataType::Utf8, false),
                ("default_character_set_name", &DataType::Utf8, false),
                ("default_collation_name", &DataType::Utf8, false),
                ("sql_path", &DataType::Utf8, true),
            ]
        );
    }

    #[test]
    fn resolve_schemata_rows_match_inputs() {
        let schema_names = vec!["db_a".to_string(), "db_b".to_string()];
        let resolved = SystemCatalogService::with_defaults()
            .resolve(
                "information_schema",
                "schemata",
                &inputs("default_catalog", &schema_names),
            )
            .expect("schemata resolution must succeed")
            .expect("schemata must be registered");

        assert_eq!(resolved.batches.len(), 1);
        let batch = &resolved.batches[0];
        assert_eq!(batch.num_rows(), 2);

        let catalog_names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("catalog_name must be Utf8");
        assert_eq!(catalog_names.value(0), "default_catalog");
        assert_eq!(catalog_names.value(1), "default_catalog");

        let actual_schema_names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("schema_name must be Utf8");
        assert_eq!(actual_schema_names.value(0), "db_a");
        assert_eq!(actual_schema_names.value(1), "db_b");
    }

    #[test]
    fn resolve_schemata_uses_input_catalog_name() {
        let schema_names = vec!["analytics".to_string(), "staging".to_string()];
        let resolved = SystemCatalogService::with_defaults()
            .resolve(
                "information_schema",
                "schemata",
                &inputs("myice", &schema_names),
            )
            .expect("schemata resolution must succeed")
            .expect("schemata must be registered");

        let catalog_names = resolved.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("catalog_name must be Utf8");
        assert_eq!(catalog_names.value(0), "myice");
        assert_eq!(catalog_names.value(1), "myice");
    }

    #[test]
    fn resolve_unknown_table_returns_none() {
        let schema_names = vec!["db_a".to_string()];
        let resolved = SystemCatalogService::with_defaults()
            .resolve(
                "information_schema",
                "columns",
                &inputs("default_catalog", &schema_names),
            )
            .expect("unknown table resolution must succeed");

        assert!(resolved.is_none());
    }

    #[test]
    fn resolve_is_case_insensitive() {
        let schema_names = vec!["db_a".to_string()];
        let resolved = SystemCatalogService::with_defaults()
            .resolve(
                "INFORMATION_SCHEMA",
                "SCHEMATA",
                &inputs("default_catalog", &schema_names),
            )
            .expect("schemata resolution must succeed");

        assert!(resolved.is_some());
    }

    #[test]
    fn resolve_tables_reports_one_row_per_table() {
        let tables = vec![
            ("db_a".to_string(), "t1".to_string()),
            ("db_a".to_string(), "t2".to_string()),
            ("db_b".to_string(), "t3".to_string()),
        ];
        let resolved = SystemCatalogService::with_defaults()
            .resolve(
                "information_schema",
                "tables",
                &table_inputs("ice_cat", &tables),
            )
            .expect("tables resolution must succeed")
            .expect("tables must be registered");

        let names: Vec<&str> = resolved
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec!["table_catalog", "table_schema", "table_name", "table_type"]
        );
        assert!(
            resolved
                .columns
                .iter()
                .all(|column| column.data_type == DataType::Utf8)
        );

        let batch = resolved.batches.first().expect("one batch");
        assert_eq!(batch.num_rows(), 3);
        let schema = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("schema column");
        let table = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("table column");
        assert_eq!(schema.value(0), "db_a");
        assert_eq!(table.value(0), "t1");
        assert_eq!(schema.value(2), "db_b");
        assert_eq!(table.value(2), "t3");
    }

    #[test]
    fn resolve_tables_on_an_empty_namespace_is_an_empty_listing_not_a_failure() {
        let resolved = SystemCatalogService::with_defaults()
            .resolve(
                "information_schema",
                "tables",
                &table_inputs("ice_cat", &[]),
            )
            .expect("tables resolution must succeed")
            .expect("tables must be registered");
        assert_eq!(
            resolved.batches.first().expect("one batch").num_rows(),
            0,
            "a namespace with no tables lists none, which is a real answer"
        );
    }
}
