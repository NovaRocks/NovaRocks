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

use std::sync::Arc;

use crate::connector::backend::{CatalogBackend, TableSource};
use crate::sql::catalog::CatalogRuntimeMetadata;
use novarocks_catalog::identifier::TableIdentity;
use novarocks_catalog::registry::Catalog;
use novarocks_catalog::schema_cache::SchemaCache;

pub(super) struct IcebergCatalog {
    name: String,
    backend: Arc<dyn CatalogBackend>,
    source: Arc<dyn TableSource>,
    cache: SchemaCache<CatalogRuntimeMetadata>,
}

impl IcebergCatalog {
    pub(super) fn new(
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

    fn invalidate(&self, namespace: &str, table: &str) {
        self.cache
            .invalidate(&TableIdentity::new(&self.name, namespace, table));
    }
}

impl Catalog<CatalogRuntimeMetadata> for IcebergCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    fn get_table_metadata(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<CatalogRuntimeMetadata, String> {
        let identity = TableIdentity::new(&self.name, namespace, table);
        let (_resolved_table, current_schema_id) = self
            .backend
            .current_schema_id_for_read(&self.name, namespace, table)?;
        self.cache
            .get_or_build_validated(&identity, current_schema_id, || {
                let resolved = self
                    .backend
                    .load_table_for_read(&self.name, namespace, table)?;
                let table_def = self.source.build_schema_table_def(&resolved)?;
                CatalogRuntimeMetadata::from_table_def(identity.clone(), &table_def)
            })
    }

    fn invalidate_table(&self, namespace: &str, table: &str) {
        self.invalidate(namespace, table);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::datatypes::DataType;

    use super::IcebergCatalog;
    use crate::connector::backend::{
        CatalogBackend, CreateTableRequest, ResolvedTable, TableSource,
    };
    use crate::connector::iceberg::scan_model::{IcebergSchemaDef, IcebergTableInfo};
    use crate::sql::catalog::{CatalogRuntimeBinding, CatalogRuntimeMetadata};
    use crate::sql::parser::ast::AlterIcebergPartitionSpecStmt;
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::registry::{Catalog, CatalogRegistry};
    use novarocks_catalog::schema::ColumnDef;

    struct TrackingBackend {
        loads: Arc<AtomicUsize>,
        schema_id: Arc<AtomicUsize>,
        schema_probes: Arc<AtomicUsize>,
    }

    impl CatalogBackend for TrackingBackend {
        fn name(&self) -> &'static str {
            "iceberg"
        }

        fn namespace_exists(&self, _: &str, _: &str) -> Result<bool, String> {
            unimplemented!()
        }

        fn create_namespace(&self, _: &str, _: &str) -> Result<(), String> {
            unimplemented!()
        }

        fn drop_namespace(&self, _: &str, _: &str, _: bool) -> Result<(), String> {
            unimplemented!()
        }

        fn create_table(&self, _: CreateTableRequest) -> Result<(), String> {
            unimplemented!()
        }

        fn table_exists(&self, _: &str, _: &str, _: &str) -> Result<bool, String> {
            unimplemented!()
        }

        fn alter_iceberg_partition_spec(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: AlterIcebergPartitionSpecStmt,
        ) -> Result<(), String> {
            unimplemented!()
        }

        fn drop_table(&self, _: &str, _: &str, _: &str, _: bool) -> Result<(), String> {
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
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: true,
                    write_default: None,
                    logical_type: None,
                }],
            })
        }

        fn current_schema_id_for_read(
            &self,
            _catalog: &str,
            _namespace: &str,
            table: &str,
        ) -> Result<(String, Option<i32>), String> {
            self.schema_probes.fetch_add(1, Ordering::SeqCst);
            Ok((
                table.to_string(),
                Some(self.schema_id.load(Ordering::SeqCst) as i32),
            ))
        }
    }

    struct TrackingSource {
        full_defs: Arc<AtomicUsize>,
        schema_defs: Arc<AtomicUsize>,
    }

    impl TrackingSource {
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
                        table_uuid: Some("uuid-1".to_string()),
                        current_snapshot_id: Some(9),
                        schema_id: 1,
                        location: "s3://warehouse/ns/orders".to_string(),
                        schema: IcebergSchemaDef { fields: vec![] },
                        serialized_metadata: Some("snapshot-payload".to_string()),
                        serialized_metadata_rows: None,
                    },
                    files: vec![],
                    cloud_properties: Default::default(),
                    binding: crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
                },
            }
        }
    }

    impl TableSource for TrackingSource {
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

    fn catalog(
        loads: &Arc<AtomicUsize>,
        schema_id: &Arc<AtomicUsize>,
        schema_probes: &Arc<AtomicUsize>,
        source: Arc<TrackingSource>,
    ) -> IcebergCatalog {
        IcebergCatalog::new(
            "ice",
            Arc::new(TrackingBackend {
                loads: Arc::clone(loads),
                schema_id: Arc::clone(schema_id),
                schema_probes: Arc::clone(schema_probes),
            }),
            source,
        )
    }

    #[test]
    fn resolves_schema_only_metadata_and_hits_validated_cache() {
        let loads = Arc::new(AtomicUsize::new(0));
        let schema_id = Arc::new(AtomicUsize::new(1));
        let schema_probes = Arc::new(AtomicUsize::new(0));
        let source = Arc::new(TrackingSource::new());
        let catalog = catalog(&loads, &schema_id, &schema_probes, Arc::clone(&source));

        let metadata: CatalogRuntimeMetadata =
            catalog.get_table_metadata("ns", "orders").expect("resolve");
        assert_eq!(metadata.table.identity.catalog, "ice");
        assert!(matches!(
            metadata.binding,
            CatalogRuntimeBinding::Iceberg { .. }
        ));
        let _ = catalog
            .get_table_metadata("ns", "orders")
            .expect("cache hit");

        assert_eq!(schema_probes.load(Ordering::SeqCst), 2);
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        assert_eq!(source.schema_defs.load(Ordering::SeqCst), 1);
        assert_eq!(source.full_defs.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn remote_schema_id_change_rebuilds_cached_metadata() {
        let loads = Arc::new(AtomicUsize::new(0));
        let schema_id = Arc::new(AtomicUsize::new(1));
        let schema_probes = Arc::new(AtomicUsize::new(0));
        let source = Arc::new(TrackingSource::new());
        let catalog = catalog(&loads, &schema_id, &schema_probes, source);

        let _ = catalog
            .get_table_metadata("ns", "orders")
            .expect("first resolve");
        schema_id.store(2, Ordering::SeqCst);
        let _ = catalog
            .get_table_metadata("ns", "orders")
            .expect("schema rebuild");

        assert_eq!(loads.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn registry_invalidation_forces_schema_reload() {
        let loads = Arc::new(AtomicUsize::new(0));
        let schema_id = Arc::new(AtomicUsize::new(1));
        let schema_probes = Arc::new(AtomicUsize::new(0));
        let source = Arc::new(TrackingSource::new());
        let mut registry = CatalogRegistry::<CatalogRuntimeMetadata>::new();
        registry.register(Arc::new(catalog(
            &loads,
            &schema_id,
            &schema_probes,
            source,
        )));

        let _ = registry.resolve("ice", "ns", "orders").expect("resolve");
        let _ = registry.resolve("ice", "ns", "orders").expect("cache hit");
        registry
            .invalidate_table("ice", "ns", "orders")
            .expect("invalidate");
        let _ = registry.resolve("ice", "ns", "orders").expect("reload");

        assert_eq!(loads.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn catalog_service_registers_iceberg_entry_from_backend_and_source() {
        let loads = Arc::new(AtomicUsize::new(0));
        let schema_id = Arc::new(AtomicUsize::new(1));
        let schema_probes = Arc::new(AtomicUsize::new(0));
        let backend: Arc<dyn CatalogBackend> = Arc::new(TrackingBackend {
            loads: Arc::clone(&loads),
            schema_id: Arc::clone(&schema_id),
            schema_probes: Arc::clone(&schema_probes),
        });
        let source: Arc<dyn TableSource> = Arc::new(TrackingSource::new());
        let service = crate::sql::catalog::new_standalone_catalog_service();

        service.register_catalog(Arc::new(IcebergCatalog::new("ice", backend, source)));

        assert!(
            service
                .registry()
                .read()
                .expect("catalog service registry")
                .get_catalog("ice")
                .is_ok()
        );
    }
}
