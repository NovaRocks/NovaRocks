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

use std::collections::HashMap;

use crate::identifier::normalize_identifier;
use crate::partition::LegacyRangePartition;
use crate::provider::CatalogProvider;
use crate::table::CatalogTable;

pub const DEFAULT_DATABASE: &str = "default";
const DEFAULT_CATALOG: &str = "default_catalog";

pub trait MemoryCatalogEntry: Clone {
    fn table_name(&self) -> &str;
    fn to_catalog_table(&self, catalog: &str, database: &str) -> CatalogTable;
}

#[derive(Clone, Debug)]
struct DatabaseDef<T> {
    tables: HashMap<String, T>,
}

#[derive(Clone, Debug)]
pub struct MemoryCatalog<T: MemoryCatalogEntry> {
    databases: HashMap<String, DatabaseDef<T>>,
    legacy_range_partitions: HashMap<(String, String), Vec<LegacyRangePartition>>,
}

impl<T: MemoryCatalogEntry> Default for MemoryCatalog<T> {
    fn default() -> Self {
        let mut databases = HashMap::new();
        databases.insert(
            DEFAULT_DATABASE.to_string(),
            DatabaseDef {
                tables: HashMap::new(),
            },
        );
        Self {
            databases,
            legacy_range_partitions: HashMap::new(),
        }
    }
}

impl<T: MemoryCatalogEntry> MemoryCatalog<T> {
    pub fn create_database(&mut self, database_name: &str) -> Result<(), String> {
        let key = normalize_identifier(database_name)?;
        if self.databases.contains_key(&key) {
            return Ok(());
        }
        self.databases.insert(
            key,
            DatabaseDef {
                tables: HashMap::new(),
            },
        );
        Ok(())
    }

    pub fn database_exists(&self, database_name: &str) -> Result<bool, String> {
        let key = normalize_identifier(database_name)?;
        Ok(self.databases.contains_key(&key))
    }

    pub fn database_names(&self) -> impl Iterator<Item = &str> {
        self.databases.keys().map(String::as_str)
    }

    pub fn table_names_in_database(&self, database_name: &str) -> Vec<String> {
        let Ok(db_key) = normalize_identifier(database_name) else {
            return Vec::new();
        };
        self.databases
            .get(&db_key)
            .map(|database| database.tables.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub fn register(&mut self, database_name: &str, table: T) -> Result<(), String> {
        let db_key = normalize_identifier(database_name)?;
        let database = self
            .databases
            .get_mut(&db_key)
            .ok_or_else(|| format!("unknown database: {database_name}"))?;
        let table_key = normalize_identifier(table.table_name())?;
        database.tables.insert(table_key, table);
        Ok(())
    }

    pub fn drop_table(&mut self, database_name: &str, table_name: &str) -> Result<(), String> {
        let db_key = normalize_identifier(database_name)?;
        let database = self
            .databases
            .get_mut(&db_key)
            .ok_or_else(|| format!("unknown database: {database_name}"))?;
        let table_key = normalize_identifier(table_name)?;
        database
            .tables
            .remove(&table_key)
            .ok_or_else(|| format!("unknown table: {table_name}"))?;
        self.legacy_range_partitions.remove(&(db_key, table_key));
        Ok(())
    }

    pub fn drop_database(&mut self, database_name: &str) -> Result<(), String> {
        let key = normalize_identifier(database_name)?;
        if key == DEFAULT_DATABASE {
            return Err("cannot drop default database".to_string());
        }
        self.databases
            .remove(&key)
            .ok_or_else(|| format!("unknown database: {database_name}"))?;
        Ok(())
    }

    pub fn get(&self, database_name: &str, table_name: &str) -> Result<T, String> {
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

    pub fn set_legacy_range_partitions(
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

    pub fn add_legacy_range_partition(
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

    pub fn get_legacy_range_partition(
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
                    .find(|entry| {
                        normalize_identifier(&entry.name).ok().as_deref() == Some(&partition_key)
                    })
                    .cloned()
            }))
    }
}

impl<T: MemoryCatalogEntry> CatalogProvider for MemoryCatalog<T> {
    fn get_table(&self, database: &str, table: &str) -> Result<CatalogTable, String> {
        self.get(database, table)
            .map(|entry| entry.to_catalog_table(DEFAULT_CATALOG, database))
    }

    fn get_table_in_catalog(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<CatalogTable, String> {
        self.get(database, table)
            .map(|entry| entry.to_catalog_table(catalog.unwrap_or(DEFAULT_CATALOG), database))
    }

    fn get_legacy_range_partition(
        &self,
        database: &str,
        table: &str,
        partition: &str,
    ) -> Result<Option<LegacyRangePartition>, String> {
        MemoryCatalog::get_legacy_range_partition(self, database, table, partition)
    }
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_DATABASE, MemoryCatalog, MemoryCatalogEntry};
    use crate::identifier::TableIdentity;
    use crate::partition::LegacyRangePartition;
    use crate::provider::CatalogProvider;
    use crate::table::CatalogTable;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestEntry {
        name: String,
        revision: u64,
    }

    impl TestEntry {
        fn new(name: &str, revision: u64) -> Self {
            Self {
                name: name.to_string(),
                revision,
            }
        }
    }

    impl MemoryCatalogEntry for TestEntry {
        fn table_name(&self) -> &str {
            &self.name
        }

        fn to_catalog_table(&self, catalog: &str, database: &str) -> CatalogTable {
            CatalogTable {
                identity: TableIdentity::new(catalog, database, &self.name),
                columns: vec![],
                hidden_columns: vec![],
            }
        }
    }

    fn partition(name: &str, column: &str, lower: &str, upper: &str) -> LegacyRangePartition {
        LegacyRangePartition {
            name: name.to_string(),
            column: column.to_string(),
            lower_sql: lower.to_string(),
            upper_sql: upper.to_string(),
        }
    }

    #[test]
    fn creates_lists_and_drops_databases_with_normalized_names() {
        let mut catalog = MemoryCatalog::<TestEntry>::default();

        assert!(
            catalog
                .database_exists("DEFAULT")
                .expect("default database")
        );
        catalog
            .create_database("  `Sales_2026`  ")
            .expect("create normalized database");
        catalog
            .create_database("sales_2026")
            .expect("idempotent create");

        let mut names = catalog.database_names().collect::<Vec<_>>();
        names.sort_unstable();
        assert_eq!(names, vec![DEFAULT_DATABASE, "sales_2026"]);

        catalog
            .drop_database("SALES_2026")
            .expect("drop normalized database");
        assert!(
            !catalog
                .database_exists("sales_2026")
                .expect("database absent")
        );
        assert_eq!(
            catalog.drop_database(DEFAULT_DATABASE),
            Err("cannot drop default database".to_string())
        );
        assert_eq!(
            catalog.drop_database("Missing"),
            Err("unknown database: Missing".to_string())
        );
    }

    #[test]
    fn registers_overwrites_lists_gets_and_drops_tables() {
        let mut catalog = MemoryCatalog::<TestEntry>::default();
        catalog.create_database("Sales").expect("create database");

        catalog
            .register("SALES", TestEntry::new("  `Orders_2026`  ", 1))
            .expect("register normalized table");
        assert_eq!(
            catalog
                .get("sales", "orders_2026")
                .expect("registered table"),
            TestEntry::new("  `Orders_2026`  ", 1)
        );
        assert_eq!(
            catalog.table_names_in_database("`Sales`"),
            vec!["orders_2026".to_string()]
        );

        catalog
            .register("sales", TestEntry::new("ORDERS_2026", 2))
            .expect("overwrite table");
        assert_eq!(
            catalog.get("SALES", "Orders_2026").expect("replacement"),
            TestEntry::new("ORDERS_2026", 2)
        );

        catalog
            .drop_table("sales", "ORDERS_2026")
            .expect("drop table");
        assert!(catalog.table_names_in_database("sales").is_empty());
    }

    #[test]
    fn preserves_exact_unknown_database_and_table_errors() {
        let mut catalog = MemoryCatalog::<TestEntry>::default();

        assert_eq!(
            catalog.register("MissingDb", TestEntry::new("t", 1)),
            Err("unknown database: MissingDb".to_string())
        );
        assert_eq!(
            catalog.get("MissingDb", "t"),
            Err("unknown database: MissingDb".to_string())
        );
        assert_eq!(
            catalog.drop_table("MissingDb", "t"),
            Err("unknown database: MissingDb".to_string())
        );

        catalog.create_database("db").expect("create database");
        assert_eq!(
            catalog.get("db", "MissingTable"),
            Err("unknown table: MissingTable".to_string())
        );
        assert_eq!(
            catalog.drop_table("db", "MissingTable"),
            Err("unknown table: MissingTable".to_string())
        );
        assert!(catalog.table_names_in_database("missing").is_empty());
        assert!(catalog.table_names_in_database("bad-name").is_empty());
    }

    #[test]
    fn neutral_provider_uses_requested_or_default_catalog_identity() {
        let mut catalog = MemoryCatalog::<TestEntry>::default();
        catalog
            .register(DEFAULT_DATABASE, TestEntry::new("Orders", 1))
            .expect("register table");

        let local = CatalogProvider::get_table(&catalog, "DEFAULT", "orders")
            .expect("default catalog lookup");
        assert_eq!(
            local.identity,
            TableIdentity::new("default_catalog", "DEFAULT", "Orders")
        );

        let named =
            CatalogProvider::get_table_in_catalog(&catalog, Some("analytics"), "default", "ORDERS")
                .expect("named catalog lookup");
        assert_eq!(
            named.identity,
            TableIdentity::new("analytics", "default", "Orders")
        );
    }

    #[test]
    fn stores_replaces_and_removes_legacy_range_partitions_case_insensitively() {
        let mut catalog = MemoryCatalog::<TestEntry>::default();
        catalog
            .set_legacy_range_partitions(
                "Default",
                "Orders",
                vec![partition("P0", "order_id", "0", "10")],
            )
            .expect("set partitions");

        assert_eq!(
            catalog
                .get_legacy_range_partition("DEFAULT", "orders", "p0")
                .expect("lookup partition"),
            Some(partition("P0", "order_id", "0", "10"))
        );

        catalog
            .add_legacy_range_partition(
                "default",
                "ORDERS",
                partition("p0", "order_id", "10", "20"),
            )
            .expect("replace partition");
        assert_eq!(
            CatalogProvider::get_legacy_range_partition(&catalog, "default", "orders", "P0")
                .expect("provider lookup"),
            Some(partition("p0", "order_id", "10", "20"))
        );

        catalog
            .set_legacy_range_partitions("default", "orders", vec![])
            .expect("clear partitions");
        assert_eq!(
            catalog
                .get_legacy_range_partition("default", "orders", "p0")
                .expect("cleared lookup"),
            None
        );
    }

    #[test]
    fn dropping_table_removes_legacy_range_partitions() {
        let mut catalog = MemoryCatalog::<TestEntry>::default();
        catalog
            .register(DEFAULT_DATABASE, TestEntry::new("orders", 1))
            .expect("register table");
        catalog
            .add_legacy_range_partition(
                DEFAULT_DATABASE,
                "orders",
                partition("p0", "order_id", "0", "10"),
            )
            .expect("add partition");

        catalog
            .drop_table(DEFAULT_DATABASE, "orders")
            .expect("drop table");
        assert_eq!(
            catalog
                .get_legacy_range_partition(DEFAULT_DATABASE, "orders", "p0")
                .expect("lookup after drop"),
            None
        );
    }
}
