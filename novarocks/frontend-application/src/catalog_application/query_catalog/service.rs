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

use std::sync::{Arc, RwLock};

use novarocks_sql::planning::catalog::PlannerMemoryCatalog;

use super::registry::{Catalog, CatalogRegistry};

pub struct QueryCatalogService {
    local: Arc<RwLock<PlannerMemoryCatalog>>,
    registry: RwLock<CatalogRegistry>,
}

impl QueryCatalogService {
    pub fn new(local: Arc<RwLock<PlannerMemoryCatalog>>, registry: CatalogRegistry) -> Self {
        Self {
            local,
            registry: RwLock::new(registry),
        }
    }

    pub fn local(&self) -> &Arc<RwLock<PlannerMemoryCatalog>> {
        &self.local
    }

    pub fn registry(&self) -> &RwLock<CatalogRegistry> {
        &self.registry
    }

    pub fn local_snapshot(&self) -> PlannerMemoryCatalog {
        self.local
            .read()
            .expect("catalog service local read lock")
            .clone()
    }

    pub fn registry_snapshot(&self) -> CatalogRegistry {
        self.registry
            .read()
            .expect("catalog service registry read lock")
            .clone()
    }

    pub fn register_catalog(&self, catalog: Arc<dyn Catalog>) {
        self.registry
            .write()
            .expect("catalog service registry write lock")
            .register(catalog);
    }

    pub fn unregister_catalog(&self, name: &str) {
        self.registry
            .write()
            .expect("catalog service registry write lock")
            .unregister(name);
    }

    pub fn invalidate_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Result<(), String> {
        self.registry
            .read()
            .expect("catalog service registry read lock")
            .invalidate_table(catalog, namespace, table)
    }
}

#[cfg(test)]
mod current_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, RwLock};

    use novarocks_types::naming::{DEFAULT_DATABASE, TableIdentity};
    use novarocks_types::schema::{CatalogTable, ColumnDef};

    use super::super::registry::{Catalog, CatalogRegistry};
    use super::QueryCatalogService;
    use crate::catalog_application::query_catalog::CatalogRuntimeMetadata;
    use novarocks_sql::planning::catalog::{
        PlannerMemoryCatalog, local_catalog_table, register_test_connector_read_table,
    };

    fn metadata(catalog: &str, namespace: &str, table: &str) -> CatalogRuntimeMetadata {
        CatalogRuntimeMetadata {
            table: CatalogTable {
                identity: TableIdentity::new(catalog, namespace, table),
                columns: Vec::new(),
                hidden_columns: Vec::new(),
            },
        }
    }

    fn column(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    struct TestCatalog;

    impl Catalog for TestCatalog {
        fn name(&self) -> &str {
            "named"
        }

        fn get_table_metadata(
            &self,
            namespace: &str,
            table: &str,
        ) -> Result<CatalogRuntimeMetadata, String> {
            Ok(metadata("named", namespace, table))
        }
    }

    struct InvalidatingCatalog {
        invalidations: Arc<AtomicUsize>,
    }

    impl Catalog for InvalidatingCatalog {
        fn name(&self) -> &str {
            "invalidating"
        }

        fn get_table_metadata(
            &self,
            namespace: &str,
            table: &str,
        ) -> Result<CatalogRuntimeMetadata, String> {
            Ok(metadata("invalidating", namespace, table))
        }

        fn invalidate_table(&self, _namespace: &str, _table: &str) {
            self.invalidations.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn service() -> QueryCatalogService {
        let local = Arc::new(RwLock::new(PlannerMemoryCatalog::default()));
        let mut registry = CatalogRegistry::new();
        registry.register(Arc::new(TestCatalog));
        QueryCatalogService::new(local, registry)
    }

    fn register_local(service: &QueryCatalogService, column_name: &str) {
        register_test_connector_read_table(
            &mut service.local().write().expect("local write lock"),
            DEFAULT_DATABASE,
            "orders",
            vec![column(column_name)],
        )
        .expect("register local table");
    }

    #[test]
    fn exposes_the_shared_local_catalog_and_named_registry() {
        let service = service();
        register_local(&service, "revision_1");
        let local = service.local().read().expect("local read lock");
        assert_eq!(
            local_catalog_table(&local, DEFAULT_DATABASE, "orders")
                .expect("local table")
                .columns[0]
                .name,
            "revision_1"
        );
        assert_eq!(
            service
                .registry()
                .read()
                .expect("registry read lock")
                .resolve("NAMED", "ns", "t")
                .expect("named metadata")
                .table
                .identity
                .fqn(),
            "named.ns.t"
        );
    }

    #[test]
    fn local_snapshot_is_an_independent_point_in_time_clone() {
        let service = service();
        register_local(&service, "revision_1");
        let snapshot = service.local_snapshot();
        register_local(&service, "revision_2");

        assert_eq!(
            local_catalog_table(&snapshot, DEFAULT_DATABASE, "orders")
                .expect("snapshot table")
                .columns[0]
                .name,
            "revision_1"
        );
        assert_eq!(
            local_catalog_table(
                &service.local().read().expect("local read lock"),
                DEFAULT_DATABASE,
                "orders",
            )
            .expect("live table")
            .columns[0]
                .name,
            "revision_2"
        );
    }

    #[test]
    fn registry_snapshot_clones_registry_membership() {
        let service = service();
        let mut snapshot = service.registry_snapshot();
        snapshot.unregister("named");

        assert_eq!(
            snapshot.resolve("named", "ns", "t").map(|_| ()),
            Err("unknown catalog: named".to_string())
        );
        assert!(
            service
                .registry()
                .read()
                .expect("registry read lock")
                .resolve("named", "ns", "t")
                .is_ok()
        );
    }

    #[test]
    fn unregister_catalog_removes_the_live_service_entry() {
        let service = service();
        service.unregister_catalog("named");
        assert_eq!(
            service
                .registry()
                .read()
                .expect("registry read lock")
                .resolve("named", "ns", "t")
                .map(|_| ()),
            Err("unknown catalog: named".to_string())
        );
    }

    #[test]
    fn invalidate_table_is_forwarded_by_the_catalog_service() {
        let local = Arc::new(RwLock::new(PlannerMemoryCatalog::default()));
        let invalidations = Arc::new(AtomicUsize::new(0));
        let mut registry = CatalogRegistry::new();
        registry.register(Arc::new(InvalidatingCatalog {
            invalidations: Arc::clone(&invalidations),
        }));
        let service = QueryCatalogService::new(local, registry);

        service
            .invalidate_table("invalidating", "ns", "orders")
            .expect("invalidate");
        assert_eq!(invalidations.load(Ordering::SeqCst), 1);
    }
}
