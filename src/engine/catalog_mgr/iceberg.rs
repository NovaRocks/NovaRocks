//! `IcebergCatalog`: a `Catalog` over an Iceberg backend. Resolves schema-level
//! metadata via the existing `CatalogBackend` + `TableSource` abstractions and
//! caches it in a `SchemaCache`. Scan-binding (data files) is NOT resolved here;
//! it happens at codegen time (P2). The cache validates Iceberg entries against
//! the backend's current schema id so remote schema evolution rebuilds metadata.

use std::sync::Arc;

use crate::connector::backend::{CatalogBackend, TableSource};
use crate::engine::catalog_mgr::catalog::Catalog;
use crate::engine::catalog_mgr::metadata::{TableIdentity, TableMetadata};
use crate::engine::catalog_mgr::schema_cache::SchemaCache;

pub(crate) struct IcebergCatalog {
    name: String,
    backend: Arc<dyn CatalogBackend>,
    source: Arc<dyn TableSource>,
    cache: SchemaCache,
}

impl IcebergCatalog {
    pub(crate) fn new(
        name: &str,
        backend: Arc<dyn CatalogBackend>,
        source: Arc<dyn TableSource>,
    ) -> Self {
        Self {
            name: name.to_string(),
            backend,
            source,
            cache: SchemaCache::new(),
        }
    }

    /// Drop the cached schema for one table (used by local write/DDL paths in
    /// later phases).
    pub(crate) fn invalidate(&self, namespace: &str, table: &str) {
        let id = TableIdentity::new(&self.name, namespace, table);
        self.cache.invalidate(&id);
    }
}

impl Catalog for IcebergCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    fn get_table_metadata(&self, namespace: &str, table: &str) -> Result<TableMetadata, String> {
        let id = TableIdentity::new(&self.name, namespace, table);
        let current_schema_id = self
            .backend
            .current_schema_id(&self.name, namespace, table)?;
        self.cache
            .get_or_build_validated(&id, current_schema_id, || {
                let resolved = self.backend.load_table(&self.name, namespace, table)?;
                let td = self.source.build_schema_table_def(&resolved)?;
                TableMetadata::from_table_def(id.clone(), &td)
            })
    }

    fn invalidate_table(&self, namespace: &str, table: &str) {
        self.invalidate(namespace, table);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::backend::{
        CatalogBackend, CreateTableRequest, ResolvedTable, TableSource,
    };
    use crate::engine::catalog_mgr::CatalogMgr;
    use crate::engine::catalog_mgr::catalog::Catalog;
    use crate::engine::catalog_mgr::metadata::TableBinding;
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableDef,
    };
    use crate::sql::parser::ast::AlterIcebergPartitionSpecStmt;
    use arrow::datatypes::DataType;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct MockBackend {
        loads: Arc<AtomicUsize>,
        schema_id: Arc<AtomicUsize>,
    }
    impl CatalogBackend for MockBackend {
        fn name(&self) -> &'static str {
            "iceberg"
        }
        fn namespace_exists(&self, _c: &str, _n: &str) -> Result<bool, String> {
            unimplemented!()
        }
        fn create_namespace(&self, _c: &str, _n: &str) -> Result<(), String> {
            unimplemented!()
        }
        fn drop_namespace(&self, _c: &str, _n: &str, _f: bool) -> Result<(), String> {
            unimplemented!()
        }
        fn create_table(&self, _req: CreateTableRequest) -> Result<(), String> {
            unimplemented!()
        }
        fn table_exists(&self, _c: &str, _n: &str, _t: &str) -> Result<bool, String> {
            unimplemented!()
        }
        fn alter_iceberg_partition_spec(
            &self,
            _c: &str,
            _n: &str,
            _t: &str,
            _s: AlterIcebergPartitionSpecStmt,
        ) -> Result<(), String> {
            unimplemented!()
        }
        fn drop_table(&self, _c: &str, _n: &str, _t: &str, _e: bool) -> Result<(), String> {
            unimplemented!()
        }
        fn load_table(
            &self,
            catalog: &str,
            namespace: &str,
            table: &str,
        ) -> Result<ResolvedTable, String> {
            self.loads.fetch_add(1, Ordering::SeqCst);
            Ok(ResolvedTable {
                catalog: catalog.to_string(),
                namespace: namespace.to_string(),
                table: table.to_string(),
                columns: vec![ColumnDef {
                    name: "a".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                }],
            })
        }
        fn current_schema_id(&self, _c: &str, _n: &str, _t: &str) -> Result<Option<i32>, String> {
            Ok(Some(self.schema_id.load(Ordering::SeqCst) as i32))
        }
    }

    struct MockSource {
        full_defs: Arc<AtomicUsize>,
        schema_defs: Arc<AtomicUsize>,
    }
    impl MockSource {
        fn new() -> Self {
            Self {
                full_defs: Arc::new(AtomicUsize::new(0)),
                schema_defs: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn table_def(&self, table: &ResolvedTable) -> TableDef {
            TableDef {
                name: table.table.clone(),
                columns: table.columns.clone(),
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::IcebergDataFiles {
                    table: IcebergTableInfo {
                        catalog: table.catalog.clone(),
                        namespace: table.namespace.clone(),
                        table: table.table.clone(),
                        table_uuid: None,
                        current_snapshot_id: Some(1),
                        schema_id: 0,
                        location: "s3://w/t".to_string(),
                        schema: IcebergSchemaDef { fields: vec![] },
                        serialized_metadata: None,
                        serialized_metadata_rows: None,
                    },
                    files: vec![],
                    cloud_properties: Default::default(),
                    binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
                },
            }
        }
    }
    impl TableSource for MockSource {
        fn name(&self) -> &'static str {
            "iceberg"
        }
        fn build_table_def(&self, table: &ResolvedTable) -> Result<TableDef, String> {
            self.full_defs.fetch_add(1, Ordering::SeqCst);
            Ok(self.table_def(table))
        }

        fn build_schema_table_def(&self, table: &ResolvedTable) -> Result<TableDef, String> {
            self.schema_defs.fetch_add(1, Ordering::SeqCst);
            Ok(self.table_def(table))
        }
    }

    #[test]
    fn resolves_iceberg_table_and_caches() {
        let loads = Arc::new(AtomicUsize::new(0));
        let schema_id = Arc::new(AtomicUsize::new(1));
        let cat = IcebergCatalog::new(
            "ice",
            Arc::new(MockBackend {
                loads: Arc::clone(&loads),
                schema_id: Arc::clone(&schema_id),
            }),
            Arc::new(MockSource::new()),
        );

        let meta = cat.get_table_metadata("ns", "t").expect("resolve");
        assert_eq!(meta.identity.catalog, "ice");
        assert_eq!(meta.columns.len(), 1);
        assert!(matches!(meta.binding, TableBinding::Iceberg { .. }));

        // Second resolve must hit the cache (no extra backend load).
        let _ = cat.get_table_metadata("ns", "t").expect("hit");
        assert_eq!(
            loads.load(Ordering::SeqCst),
            1,
            "second resolve must hit cache"
        );
    }

    #[test]
    fn catalog_mgr_invalidation_clears_iceberg_schema_cache() {
        let loads = Arc::new(AtomicUsize::new(0));
        let schema_id = Arc::new(AtomicUsize::new(1));
        let mut mgr = CatalogMgr::new();
        mgr.register(Arc::new(IcebergCatalog::new(
            "ice",
            Arc::new(MockBackend {
                loads: Arc::clone(&loads),
                schema_id: Arc::clone(&schema_id),
            }),
            Arc::new(MockSource::new()),
        )));

        let _ = mgr.resolve("ice", "ns", "t").expect("resolve");
        let _ = mgr.resolve("ice", "ns", "t").expect("hit");
        assert_eq!(
            loads.load(Ordering::SeqCst),
            1,
            "second resolve must hit cache"
        );

        mgr.invalidate_table("ice", "ns", "t").expect("invalidate");
        let _ = mgr.resolve("ice", "ns", "t").expect("reload");
        assert_eq!(
            loads.load(Ordering::SeqCst),
            2,
            "resolve after invalidation must reload schema"
        );
    }

    #[test]
    fn iceberg_catalog_rebuilds_when_remote_schema_id_changes() {
        let loads = Arc::new(AtomicUsize::new(0));
        let schema_id = Arc::new(AtomicUsize::new(1));
        let cat = IcebergCatalog::new(
            "ice",
            Arc::new(MockBackend {
                loads: Arc::clone(&loads),
                schema_id: Arc::clone(&schema_id),
            }),
            Arc::new(MockSource::new()),
        );

        let first = cat.get_table_metadata("ns", "t").expect("first");
        let second = cat.get_table_metadata("ns", "t").expect("cached");
        assert_eq!(first.columns.len(), second.columns.len());
        assert_eq!(loads.load(Ordering::SeqCst), 1);

        schema_id.store(2, Ordering::SeqCst);
        let _ = cat.get_table_metadata("ns", "t").expect("rebuild");
        assert_eq!(loads.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn iceberg_catalog_builds_schema_only_table_def_for_cached_metadata() {
        let loads = Arc::new(AtomicUsize::new(0));
        let schema_id = Arc::new(AtomicUsize::new(1));
        let source = Arc::new(MockSource::new());
        let cat = IcebergCatalog::new(
            "ice",
            Arc::new(MockBackend {
                loads: Arc::clone(&loads),
                schema_id: Arc::clone(&schema_id),
            }),
            Arc::clone(&source) as Arc<dyn TableSource>,
        );

        let _ = cat.get_table_metadata("ns", "t").expect("resolve");
        assert_eq!(source.schema_defs.load(Ordering::SeqCst), 1);
        assert_eq!(source.full_defs.load(Ordering::SeqCst), 0);
    }
}
