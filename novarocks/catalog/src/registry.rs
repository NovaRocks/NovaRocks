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
use std::sync::Arc;

pub trait Catalog<M>: Send + Sync {
    fn name(&self) -> &str;

    fn get_table_metadata(&self, namespace: &str, table: &str) -> Result<M, String>;

    fn invalidate_table(&self, _namespace: &str, _table: &str) {}
}

pub struct CatalogRegistry<M> {
    catalogs: HashMap<String, Arc<dyn Catalog<M>>>,
}

impl<M> Clone for CatalogRegistry<M> {
    fn clone(&self) -> Self {
        Self {
            catalogs: self.catalogs.clone(),
        }
    }
}

impl<M> Default for CatalogRegistry<M> {
    fn default() -> Self {
        Self {
            catalogs: HashMap::new(),
        }
    }
}

impl<M> CatalogRegistry<M> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, catalog: Arc<dyn Catalog<M>>) {
        self.catalogs
            .insert(catalog.name().to_ascii_lowercase(), catalog);
    }

    pub fn unregister(&mut self, name: &str) {
        self.catalogs.remove(&name.to_ascii_lowercase());
    }

    pub fn invalidate_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Result<(), String> {
        self.get_catalog(catalog)?
            .invalidate_table(namespace, table);
        Ok(())
    }

    pub fn get_catalog(&self, name: &str) -> Result<Arc<dyn Catalog<M>>, String> {
        self.catalogs
            .get(&name.to_ascii_lowercase())
            .cloned()
            .ok_or_else(|| format!("unknown catalog: {name}"))
    }

    pub fn resolve(&self, catalog: &str, namespace: &str, table: &str) -> Result<M, String> {
        self.get_catalog(catalog)?
            .get_table_metadata(namespace, table)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{Catalog, CatalogRegistry};

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestMetadata {
        catalog: String,
        namespace: String,
        table: String,
        revision: u64,
    }

    struct TestCatalog {
        name: String,
        revision: u64,
        invalidations: Arc<AtomicUsize>,
    }

    impl TestCatalog {
        fn new(name: &str, revision: u64) -> Self {
            Self {
                name: name.to_string(),
                revision,
                invalidations: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl Catalog<TestMetadata> for TestCatalog {
        fn name(&self) -> &str {
            &self.name
        }

        fn get_table_metadata(&self, namespace: &str, table: &str) -> Result<TestMetadata, String> {
            Ok(TestMetadata {
                catalog: self.name.clone(),
                namespace: namespace.to_string(),
                table: table.to_string(),
                revision: self.revision,
            })
        }

        fn invalidate_table(&self, _namespace: &str, _table: &str) {
            self.invalidations.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn registers_and_resolves_catalog_names_case_insensitively() {
        let mut registry = CatalogRegistry::new();
        registry.register(Arc::new(TestCatalog::new("Ice", 1)));

        let handle = registry.get_catalog("iCE").expect("get mixed-case catalog");
        assert_eq!(handle.name(), "Ice");
        assert_eq!(
            registry.resolve("ICE", "Analytics", "Orders"),
            Ok(TestMetadata {
                catalog: "Ice".to_string(),
                namespace: "Analytics".to_string(),
                table: "Orders".to_string(),
                revision: 1,
            })
        );
    }

    #[test]
    fn registration_overwrites_and_unregister_removes_the_named_catalog() {
        let mut registry = CatalogRegistry::new();
        registry.register(Arc::new(TestCatalog::new("Ice", 1)));
        registry.register(Arc::new(TestCatalog::new("ICE", 2)));

        assert_eq!(
            registry
                .resolve("ice", "ns", "t")
                .expect("resolve replacement")
                .revision,
            2
        );

        registry.unregister("iCe");
        assert_eq!(
            registry.resolve("ICE", "ns", "t"),
            Err("unknown catalog: ICE".to_string())
        );
    }

    #[test]
    fn unknown_catalog_preserves_exact_error_text() {
        let registry = CatalogRegistry::<TestMetadata>::new();

        match registry.get_catalog("Missing") {
            Ok(_) => panic!("missing catalog unexpectedly resolved"),
            Err(error) => assert_eq!(error, "unknown catalog: Missing"),
        }
        assert_eq!(
            registry.resolve("Missing", "ns", "t"),
            Err("unknown catalog: Missing".to_string())
        );
        assert_eq!(
            registry.invalidate_table("Missing", "ns", "t"),
            Err("unknown catalog: Missing".to_string())
        );
    }

    #[test]
    fn invalidation_is_forwarded_to_the_registered_catalog() {
        let catalog = Arc::new(TestCatalog::new("Ice", 1));
        let invalidations = Arc::clone(&catalog.invalidations);
        let mut registry = CatalogRegistry::new();
        registry.register(catalog);

        registry
            .invalidate_table("ICE", "ns", "t")
            .expect("invalidate table");
        assert_eq!(invalidations.load(Ordering::SeqCst), 1);
    }
}
