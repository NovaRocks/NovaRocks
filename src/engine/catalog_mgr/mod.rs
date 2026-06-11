//! Connector metadata layer (FE-side). See
//! docs/design/specs/2026-06-01-connector-metadata-layer-design.md

pub(crate) mod catalog;
pub(crate) mod iceberg;
pub(crate) mod internal;
pub(crate) mod metadata;
pub(crate) mod provider;
pub(crate) mod schema_cache;

use std::collections::HashMap;
use std::sync::Arc;

use crate::engine::catalog_mgr::catalog::Catalog;
use crate::engine::catalog_mgr::metadata::TableMetadata;

/// Registry of named catalogs (FE-side). Replaces the scattered resolution
/// across `InMemoryCatalog` / `IcebergCatalogRegistry` / `StarRocksTableCatalog`
/// with a single catalog-aware entry point.
#[derive(Clone, Default)]
pub(crate) struct CatalogMgr {
    catalogs: HashMap<String, Arc<dyn Catalog>>,
}

impl CatalogMgr {
    pub(crate) fn new() -> Self {
        Self {
            catalogs: HashMap::new(),
        }
    }

    /// Register (or overwrite) a named catalog. Keyed by `catalog.name()`.
    pub(crate) fn register(&mut self, catalog: Arc<dyn Catalog>) {
        self.catalogs
            .insert(catalog.name().to_ascii_lowercase(), catalog);
    }

    pub(crate) fn unregister(&mut self, name: &str) {
        self.catalogs.remove(&name.to_ascii_lowercase());
    }

    pub(crate) fn invalidate_table(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Result<(), String> {
        self.get_catalog(catalog)?
            .invalidate_table(namespace, table);
        Ok(())
    }

    /// Look up a named catalog handle.
    pub(crate) fn get_catalog(&self, name: &str) -> Result<Arc<dyn Catalog>, String> {
        self.catalogs
            .get(&name.to_ascii_lowercase())
            .cloned()
            .ok_or_else(|| format!("unknown catalog: {name}"))
    }

    /// Resolve schema-level metadata for `catalog.namespace.table`.
    pub(crate) fn resolve(
        &self,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Result<TableMetadata, String> {
        self.get_catalog(catalog)?
            .get_table_metadata(namespace, table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::catalog_mgr::catalog::Catalog;
    use crate::engine::catalog_mgr::metadata::{TableBinding, TableIdentity, TableMetadata};
    use std::sync::Arc;

    struct OneTableCatalog {
        name: String,
    }

    impl Catalog for OneTableCatalog {
        fn name(&self) -> &str {
            &self.name
        }
        fn get_table_metadata(
            &self,
            namespace: &str,
            table: &str,
        ) -> Result<TableMetadata, String> {
            Ok(TableMetadata {
                identity: TableIdentity::new(&self.name, namespace, table),
                columns: vec![],
                iceberg_row_lineage_columns: vec![],
                binding: TableBinding::Internal {
                    db_id: 1,
                    table_id: 1,
                },
            })
        }
    }

    #[test]
    fn mgr_registers_and_resolves() {
        let mut mgr = CatalogMgr::new();
        mgr.register(Arc::new(OneTableCatalog {
            name: "ice".to_string(),
        }));

        let meta = mgr.resolve("ice", "ns", "t").expect("resolve");
        assert_eq!(meta.identity.catalog, "ice");
        assert_eq!(meta.identity.table, "t");
    }

    #[test]
    fn mgr_unknown_catalog_errors() {
        let mgr = CatalogMgr::new();
        let err = mgr.resolve("nope", "ns", "t").expect_err("unknown catalog");
        assert!(err.contains("unknown catalog"), "got: {err}");
    }

    #[test]
    fn mgr_get_catalog_returns_handle() {
        let mut mgr = CatalogMgr::new();
        mgr.register(Arc::new(OneTableCatalog {
            name: "ice".to_string(),
        }));
        let cat = mgr.get_catalog("ice").expect("get");
        assert_eq!(cat.name(), "ice");
        assert!(mgr.get_catalog("missing").is_err());
    }

    #[test]
    fn mgr_resolves_catalog_names_case_insensitively() {
        let mut mgr = CatalogMgr::new();
        mgr.register(Arc::new(OneTableCatalog {
            name: "Ice".to_string(),
        }));

        let cat = mgr.get_catalog("iCE").expect("get mixed-case catalog");
        assert_eq!(cat.name(), "Ice");

        let meta = mgr
            .resolve("ICE", "ns", "t")
            .expect("resolve mixed-case catalog");
        assert_eq!(meta.identity.catalog, "Ice");
        assert_eq!(meta.identity.table, "t");
    }
}
