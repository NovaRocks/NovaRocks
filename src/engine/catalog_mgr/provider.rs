//! Analyzer-facing adapter over CatalogMgr plus the local InMemoryCatalog.

use crate::connector::ConnectorRegistry;
use crate::engine::catalog::InMemoryCatalog;
use crate::engine::catalog_mgr::CatalogMgr;
use crate::sql::catalog::{CatalogProvider, TableDef, TableLookupMode};

pub(crate) struct CatalogMgrProvider<'a> {
    current_catalog: Option<&'a str>,
    local: &'a InMemoryCatalog,
    catalog_mgr: &'a CatalogMgr,
    connectors: &'a ConnectorRegistry,
    default_mode: TableLookupMode,
}

impl<'a> CatalogMgrProvider<'a> {
    pub(crate) fn new(
        current_catalog: Option<&'a str>,
        local: &'a InMemoryCatalog,
        catalog_mgr: &'a CatalogMgr,
        connectors: &'a ConnectorRegistry,
        default_mode: TableLookupMode,
    ) -> Self {
        Self {
            current_catalog,
            local,
            catalog_mgr,
            connectors,
            default_mode,
        }
    }

    fn effective_catalog<'b>(&'b self, override_catalog: Option<&'b str>) -> Option<&'b str> {
        override_catalog.or(self.current_catalog)
    }

    fn iceberg_table_def(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
        mode: &TableLookupMode,
    ) -> Result<TableDef, String> {
        match mode {
            TableLookupMode::SchemaOnly => self
                .catalog_mgr
                .resolve(catalog, database, table)
                .map(|metadata| metadata.to_table_def()),
            TableLookupMode::ExplainStats => {
                let backend = self.connectors.catalog_backend("iceberg")?;
                let source = self.connectors.table_source("iceberg")?;
                let resolved = backend.load_table(catalog, database, table)?;
                source.build_table_def(&resolved)
            }
            TableLookupMode::IcebergMetadata {
                metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType::Partitions,
            } => {
                let backend = self.connectors.catalog_backend("iceberg")?;
                let source = self.connectors.table_source("iceberg")?;
                let resolved = backend.load_table(catalog, database, table)?;
                source.build_table_def(&resolved)
            }
            TableLookupMode::IcebergMetadata { .. } => {
                let backend = self.connectors.catalog_backend("iceberg")?;
                let source = self.connectors.table_source("iceberg")?;
                let resolved = backend.load_table(catalog, database, table)?;
                source.build_schema_table_def(&resolved)
            }
        }
    }
}

impl CatalogProvider for CatalogMgrProvider<'_> {
    fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String> {
        self.get_table_with_mode(None, database, table, self.default_mode.clone())
    }

    fn get_table_in_catalog(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<TableDef, String> {
        self.get_table_with_mode(catalog, database, table, self.default_mode.clone())
    }

    fn get_table_with_mode(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        mode: TableLookupMode,
    ) -> Result<TableDef, String> {
        match self.effective_catalog(catalog) {
            Some("default_catalog") | None => self.local.get_table(database, table),
            Some(catalog) => self.iceberg_table_def(catalog, database, table, &mode),
        }
    }

    fn get_legacy_range_partition(
        &self,
        database: &str,
        table: &str,
        partition: &str,
    ) -> Result<Option<crate::sql::catalog::LegacyRangePartition>, String> {
        self.local
            .get_legacy_range_partition(database, table, partition)
    }

    fn get_physical_layout(
        &self,
        database: &str,
        table: &str,
    ) -> Result<Option<crate::sql::catalog::PhysicalTableLayout>, String> {
        self.local.get_physical_layout(database, table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::backend::{
        CatalogBackend, CreateTableRequest, ResolvedTable, TableSource,
    };
    use crate::connector::iceberg::IcebergMetadataTableType;
    use crate::engine::catalog::InMemoryCatalog;
    use crate::engine::catalog_mgr::catalog::Catalog;
    use crate::engine::catalog_mgr::metadata::{TableBinding, TableIdentity, TableMetadata};
    use crate::sql::catalog::{
        ColumnDef, IcebergSchemaDef, IcebergTableInfo, ScanSource, TableLookupMode,
    };
    use crate::sql::parser::ast::AlterIcebergPartitionSpecStmt;
    use arrow::datatypes::DataType;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct FixedIceCatalog;
    impl Catalog for FixedIceCatalog {
        fn name(&self) -> &str {
            "ice"
        }

        fn get_table_metadata(
            &self,
            namespace: &str,
            table: &str,
        ) -> Result<TableMetadata, String> {
            Ok(TableMetadata {
                identity: TableIdentity::new("ice", namespace, table),
                columns: vec![ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_columns: vec![],
                binding: TableBinding::Iceberg {
                    info: iceberg_info(),
                    cloud_properties: Default::default(),
                },
            })
        }
    }

    fn iceberg_info() -> IcebergTableInfo {
        IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "orders".to_string(),
            table_uuid: Some("uuid-1".to_string()),
            current_snapshot_id: Some(7),
            schema_id: 3,
            location: "s3://warehouse/db/orders".to_string(),
            schema: IcebergSchemaDef { fields: vec![] },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn resolved_table(catalog: &str, namespace: &str, table: &str) -> ResolvedTable {
        ResolvedTable {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
        }
    }

    fn connector_table_def(table: &ResolvedTable, serialized_metadata: &str) -> TableDef {
        let mut info = iceberg_info();
        info.catalog = table.catalog.clone();
        info.namespace = table.namespace.clone();
        info.table = table.table.clone();
        info.serialized_metadata = Some(serialized_metadata.to_string());
        TableDef {
            name: table.table.clone(),
            columns: table.columns.clone(),
            iceberg_row_lineage_metadata_columns: vec![],
            source: ScanSource::IcebergDataFiles {
                table: info,
                files: vec![],
                cloud_properties: BTreeMap::from([(
                    "aws.s3.endpoint".to_string(),
                    "http://minio:9000".to_string(),
                )]),
                binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
            },
        }
    }

    struct TrackingBackend {
        loads: Arc<AtomicUsize>,
    }

    impl CatalogBackend for TrackingBackend {
        fn name(&self) -> &'static str {
            "iceberg"
        }

        fn namespace_exists(&self, _: &str, _: &str) -> Result<bool, String> {
            Err("unused".to_string())
        }

        fn create_namespace(&self, _: &str, _: &str) -> Result<(), String> {
            Err("unused".to_string())
        }

        fn drop_namespace(&self, _: &str, _: &str, _: bool) -> Result<(), String> {
            Err("unused".to_string())
        }

        fn create_table(&self, _: CreateTableRequest) -> Result<(), String> {
            Err("unused".to_string())
        }

        fn table_exists(&self, _: &str, _: &str, _: &str) -> Result<bool, String> {
            Err("unused".to_string())
        }

        fn drop_table(&self, _: &str, _: &str, _: &str, _: bool) -> Result<(), String> {
            Err("unused".to_string())
        }

        fn load_table(
            &self,
            catalog: &str,
            namespace: &str,
            table: &str,
        ) -> Result<ResolvedTable, String> {
            self.loads.fetch_add(1, Ordering::SeqCst);
            Ok(resolved_table(catalog, namespace, table))
        }

        fn alter_iceberg_partition_spec(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: AlterIcebergPartitionSpecStmt,
        ) -> Result<(), String> {
            Err("unused".to_string())
        }
    }

    struct TrackingSource {
        full_calls: Arc<AtomicUsize>,
        schema_calls: Arc<AtomicUsize>,
    }

    impl TableSource for TrackingSource {
        fn name(&self) -> &'static str {
            "iceberg"
        }

        fn build_table_def(&self, table: &ResolvedTable) -> Result<TableDef, String> {
            self.full_calls.fetch_add(1, Ordering::SeqCst);
            Ok(connector_table_def(table, "full-metadata"))
        }

        fn build_schema_table_def(&self, table: &ResolvedTable) -> Result<TableDef, String> {
            self.schema_calls.fetch_add(1, Ordering::SeqCst);
            Ok(connector_table_def(table, "schema-metadata"))
        }
    }

    fn provider_with_tracking_connectors<'a>(
        local: &'a InMemoryCatalog,
        mgr: &'a CatalogMgr,
        connectors: &'a mut crate::connector::ConnectorRegistry,
    ) -> (CatalogMgrProvider<'a>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let full_calls = Arc::new(AtomicUsize::new(0));
        let schema_calls = Arc::new(AtomicUsize::new(0));
        connectors.register_catalog_backend(Arc::new(TrackingBackend {
            loads: Arc::new(AtomicUsize::new(0)),
        }));
        connectors.register_table_source(Arc::new(TrackingSource {
            full_calls: Arc::clone(&full_calls),
            schema_calls: Arc::clone(&schema_calls),
        }));
        (
            CatalogMgrProvider::new(
                Some("ice"),
                local,
                mgr,
                connectors,
                TableLookupMode::SchemaOnly,
            ),
            full_calls,
            schema_calls,
        )
    }

    #[test]
    fn provider_resolves_current_catalog_without_mutating_local_catalog() {
        let local = InMemoryCatalog::default();
        let mut mgr = CatalogMgr::new();
        mgr.register(Arc::new(FixedIceCatalog));
        let connectors = crate::connector::ConnectorRegistry::default();
        let provider = CatalogMgrProvider::new(
            Some("ice"),
            &local,
            &mgr,
            &connectors,
            TableLookupMode::SchemaOnly,
        );

        let table = provider.get_table("db", "orders").expect("resolve");

        assert_eq!(table.name, "orders");
        assert!(matches!(table.source, ScanSource::IcebergDataFiles { .. }));
        assert!(local.get("db", "orders").is_err());
    }

    #[test]
    fn provider_uses_schema_table_def_for_non_partitions_metadata_lookup() {
        let local = InMemoryCatalog::default();
        let mut mgr = CatalogMgr::new();
        mgr.register(Arc::new(FixedIceCatalog));
        let mut connectors = crate::connector::ConnectorRegistry::default();
        let (provider, full_calls, schema_calls) =
            provider_with_tracking_connectors(&local, &mgr, &mut connectors);

        let table = provider
            .get_table_with_mode(
                None,
                "db",
                "orders",
                TableLookupMode::IcebergMetadata {
                    metadata_table_type: IcebergMetadataTableType::Snapshots,
                },
            )
            .expect("resolve metadata table");

        assert_eq!(full_calls.load(Ordering::SeqCst), 0);
        assert_eq!(schema_calls.load(Ordering::SeqCst), 1);
        let ScanSource::IcebergDataFiles {
            table,
            cloud_properties,
            ..
        } = table.source
        else {
            panic!("expected iceberg source");
        };
        assert_eq!(
            table.serialized_metadata.as_deref(),
            Some("schema-metadata")
        );
        assert_eq!(
            cloud_properties.get("aws.s3.endpoint").map(String::as_str),
            Some("http://minio:9000")
        );
    }

    #[test]
    fn provider_uses_full_table_def_for_partitions_metadata_lookup() {
        let local = InMemoryCatalog::default();
        let mut mgr = CatalogMgr::new();
        mgr.register(Arc::new(FixedIceCatalog));
        let mut connectors = crate::connector::ConnectorRegistry::default();
        let (provider, full_calls, schema_calls) =
            provider_with_tracking_connectors(&local, &mgr, &mut connectors);

        let table = provider
            .get_table_with_mode(
                None,
                "db",
                "orders",
                TableLookupMode::IcebergMetadata {
                    metadata_table_type: IcebergMetadataTableType::Partitions,
                },
            )
            .expect("resolve partitions metadata table");

        assert_eq!(full_calls.load(Ordering::SeqCst), 1);
        assert_eq!(schema_calls.load(Ordering::SeqCst), 0);
        let ScanSource::IcebergDataFiles { table, .. } = table.source else {
            panic!("expected iceberg source");
        };
        assert_eq!(table.serialized_metadata.as_deref(), Some("full-metadata"));
    }
}
