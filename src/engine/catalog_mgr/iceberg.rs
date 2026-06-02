//! `IcebergCatalog`: a `Catalog` over an Iceberg backend. Resolves schema-level
//! metadata via the existing `CatalogBackend` + `TableSource` abstractions and
//! caches it in a `SchemaCache`. Scan-binding (data files) is NOT resolved here;
//! it happens at codegen time (P2). P1 passes `current_schema_id = None`
//! (no remote schema probe yet); the probe is wired in P3.

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
        // P1: current_schema_id = None (no remote probe yet; wired in P3).
        self.cache.get_or_build_validated(&id, None, || {
            let resolved = self.backend.load_table(&self.name, namespace, table)?;
            let td = self.source.build_table_def(&resolved)?;
            TableMetadata::from_table_def(id.clone(), &td)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::backend::{
        CatalogBackend, CreateTableRequest, ResolvedTable, TableSource,
    };
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
    }

    struct MockSource;
    impl TableSource for MockSource {
        fn name(&self) -> &'static str {
            "iceberg"
        }
        fn build_table_def(&self, table: &ResolvedTable) -> Result<TableDef, String> {
            Ok(TableDef {
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
                    },
                    files: vec![],
                    cloud_properties: Default::default(),
                },
            })
        }
    }

    #[test]
    fn resolves_iceberg_table_and_caches() {
        let loads = Arc::new(AtomicUsize::new(0));
        let cat = IcebergCatalog::new(
            "ice",
            Arc::new(MockBackend {
                loads: Arc::clone(&loads),
            }),
            Arc::new(MockSource),
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
}
