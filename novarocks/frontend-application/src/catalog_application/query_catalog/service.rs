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

use super::registry::CatalogRegistry;

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

    pub fn register_catalog(&self, name: &str) {
        self.registry
            .write()
            .expect("catalog service registry write lock")
            .register(name);
    }

    pub fn unregister_catalog(&self, name: &str) {
        self.registry
            .write()
            .expect("catalog service registry write lock")
            .unregister(name);
    }
}

#[cfg(test)]
mod current_tests {
    use std::sync::{Arc, RwLock};

    use novarocks_types::naming::DEFAULT_DATABASE;
    use novarocks_types::schema::ColumnDef;

    use super::super::registry::CatalogRegistry;
    use super::QueryCatalogService;
    use novarocks_sql::planning::catalog::{
        PlannerMemoryCatalog, local_catalog_table, register_test_connector_read_table,
    };

    fn column(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    fn service() -> QueryCatalogService {
        let local = Arc::new(RwLock::new(PlannerMemoryCatalog::default()));
        let mut registry = CatalogRegistry::new();
        registry.register("named");
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
        assert!(
            service
                .registry()
                .read()
                .expect("registry read lock")
                .require_catalog("NAMED")
                .is_ok()
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
            snapshot.require_catalog("named"),
            Err("unknown catalog: named".to_string())
        );
        assert!(
            service
                .registry()
                .read()
                .expect("registry read lock")
                .require_catalog("named")
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
                .require_catalog("named")
                .map(|_| ()),
            Err("unknown catalog: named".to_string())
        );
    }
}
