#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::connector::ConnectorRegistry;
    use crate::connector::backend::{CatalogBackend, CreateTableRequest, ResolvedTable};

    struct DummyCatalog;

    impl CatalogBackend for DummyCatalog {
        fn name(&self) -> &'static str {
            "dummy"
        }

        fn namespace_exists(&self, _catalog: &str, _namespace: &str) -> Result<bool, String> {
            Ok(false)
        }

        fn create_namespace(&self, _catalog: &str, _namespace: &str) -> Result<(), String> {
            Ok(())
        }

        fn drop_namespace(
            &self,
            _catalog: &str,
            _namespace: &str,
            _force: bool,
        ) -> Result<(), String> {
            Ok(())
        }

        fn create_table(&self, _req: CreateTableRequest) -> Result<(), String> {
            Ok(())
        }

        fn drop_table(
            &self,
            _catalog: &str,
            _namespace: &str,
            _table: &str,
            _if_exists: bool,
        ) -> Result<(), String> {
            Ok(())
        }

        fn load_table(
            &self,
            _catalog: &str,
            _namespace: &str,
            _table: &str,
        ) -> Result<ResolvedTable, String> {
            Err("dummy".to_string())
        }
    }

    #[test]
    fn registry_registers_and_resolves_catalog_backend() {
        let mut registry = ConnectorRegistry::default();
        registry.register_catalog_backend(Arc::new(DummyCatalog));

        let backend = registry.catalog_backend("dummy").expect("resolve backend");
        assert_eq!(backend.name(), "dummy");
        assert!(registry.catalog_backend("missing").is_err());
    }
}
