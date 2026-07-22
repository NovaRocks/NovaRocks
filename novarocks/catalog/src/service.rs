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

use crate::memory::{MemoryCatalog, MemoryCatalogEntry};
use crate::registry::{Catalog, CatalogRegistry};

pub struct CatalogService<L, M>
where
    L: MemoryCatalogEntry,
{
    local: Arc<RwLock<MemoryCatalog<L>>>,
    registry: RwLock<CatalogRegistry<M>>,
}

impl<L, M> CatalogService<L, M>
where
    L: MemoryCatalogEntry,
{
    pub fn new(local: Arc<RwLock<MemoryCatalog<L>>>, registry: CatalogRegistry<M>) -> Self {
        Self {
            local,
            registry: RwLock::new(registry),
        }
    }

    pub fn local(&self) -> &Arc<RwLock<MemoryCatalog<L>>> {
        &self.local
    }

    pub fn registry(&self) -> &RwLock<CatalogRegistry<M>> {
        &self.registry
    }

    pub fn local_snapshot(&self) -> MemoryCatalog<L> {
        self.local
            .read()
            .expect("catalog service local read lock")
            .clone()
    }

    pub fn registry_snapshot(&self) -> CatalogRegistry<M> {
        self.registry
            .read()
            .expect("catalog service registry read lock")
            .clone()
    }

    pub fn register_catalog(&self, catalog: Arc<dyn Catalog<M>>) {
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
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, RwLock};

    use super::CatalogService;
    use crate::identifier::TableIdentity;
    use crate::memory::{DEFAULT_DATABASE, MemoryCatalog, MemoryCatalogEntry};
    use crate::registry::{Catalog, CatalogRegistry};
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

    struct TestCatalog;

    impl Catalog<u64> for TestCatalog {
        fn name(&self) -> &str {
            "named"
        }

        fn get_table_metadata(&self, _namespace: &str, _table: &str) -> Result<u64, String> {
            Ok(7)
        }
    }

    struct InvalidatingCatalog {
        invalidations: Arc<AtomicUsize>,
    }

    impl Catalog<u64> for InvalidatingCatalog {
        fn name(&self) -> &str {
            "invalidating"
        }

        fn get_table_metadata(&self, _namespace: &str, _table: &str) -> Result<u64, String> {
            Ok(11)
        }

        fn invalidate_table(&self, _namespace: &str, _table: &str) {
            self.invalidations.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn service() -> CatalogService<TestEntry, u64> {
        let local = Arc::new(RwLock::new(MemoryCatalog::default()));
        let mut registry = CatalogRegistry::new();
        registry.register(Arc::new(TestCatalog));
        CatalogService::new(local, registry)
    }

    #[test]
    fn exposes_the_shared_local_catalog_and_named_registry() {
        let service = service();
        let local = Arc::clone(service.local());

        local
            .write()
            .expect("local write lock")
            .register(DEFAULT_DATABASE, TestEntry::new("orders", 1))
            .expect("register local table");
        assert_eq!(
            service
                .local()
                .read()
                .expect("local read lock")
                .get(DEFAULT_DATABASE, "orders")
                .expect("local table"),
            TestEntry::new("orders", 1)
        );
        assert_eq!(
            service
                .registry()
                .read()
                .expect("registry read lock")
                .resolve("NAMED", "ns", "t"),
            Ok(7)
        );
    }

    #[test]
    fn local_snapshot_is_an_independent_point_in_time_clone() {
        let service = service();
        service
            .local()
            .write()
            .expect("local write lock")
            .register(DEFAULT_DATABASE, TestEntry::new("orders", 1))
            .expect("register first revision");

        let snapshot = service.local_snapshot();
        service
            .local()
            .write()
            .expect("local write lock")
            .register(DEFAULT_DATABASE, TestEntry::new("orders", 2))
            .expect("register second revision");

        assert_eq!(
            snapshot
                .get(DEFAULT_DATABASE, "orders")
                .expect("snapshot table"),
            TestEntry::new("orders", 1)
        );
        assert_eq!(
            service
                .local()
                .read()
                .expect("local read lock")
                .get(DEFAULT_DATABASE, "orders")
                .expect("live table"),
            TestEntry::new("orders", 2)
        );
    }

    #[test]
    fn registry_snapshot_clones_registry_membership() {
        let service = service();
        let mut snapshot = service.registry_snapshot();
        snapshot.unregister("named");

        assert_eq!(
            snapshot.resolve("named", "ns", "t"),
            Err("unknown catalog: named".to_string())
        );
        assert_eq!(
            service
                .registry()
                .read()
                .expect("registry read lock")
                .resolve("named", "ns", "t"),
            Ok(7)
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
                .resolve("named", "ns", "t"),
            Err("unknown catalog: named".to_string())
        );
    }

    #[test]
    fn invalidate_table_is_forwarded_by_the_catalog_service() {
        let local = Arc::new(RwLock::new(MemoryCatalog::<TestEntry>::default()));
        let invalidations = Arc::new(AtomicUsize::new(0));
        let mut registry = CatalogRegistry::new();
        registry.register(Arc::new(InvalidatingCatalog {
            invalidations: Arc::clone(&invalidations),
        }));
        let service = CatalogService::new(local, registry);

        service
            .invalidate_table("invalidating", "ns", "orders")
            .expect("invalidate");

        assert_eq!(invalidations.load(Ordering::SeqCst), 1);
    }
}
