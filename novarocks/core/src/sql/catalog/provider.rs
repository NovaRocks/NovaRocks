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

use crate::connector::ConnectorRegistry;
use crate::sql::catalog::{
    CatalogRuntimeMetadata, IcebergMetadataTableProvider, PlannerTableProvider,
    ResolvedAnalyzerTable, TableLookupMode,
};
use crate::sql::planner::table::TableDef;
use novarocks_catalog::partition::LegacyRangePartition;
use novarocks_catalog::provider::CatalogProvider;
use novarocks_catalog::service::CatalogService;
use novarocks_catalog::table::CatalogTable;

pub(crate) struct CatalogServiceProvider<'a> {
    current_catalog: Option<&'a str>,
    service: &'a CatalogService<TableDef, CatalogRuntimeMetadata>,
    connectors: &'a ConnectorRegistry,
    lookup_mode: TableLookupMode,
}

impl<'a> CatalogServiceProvider<'a> {
    pub(crate) fn new(
        current_catalog: Option<&'a str>,
        service: &'a CatalogService<TableDef, CatalogRuntimeMetadata>,
        connectors: &'a ConnectorRegistry,
        lookup_mode: TableLookupMode,
    ) -> Self {
        Self {
            current_catalog,
            service,
            connectors,
            lookup_mode,
        }
    }

    fn effective_catalog<'b>(&'b self, override_catalog: Option<&'b str>) -> Option<&'b str> {
        override_catalog.or(self.current_catalog)
    }

    fn resolve_table_for_analysis_once(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        match self.effective_catalog(catalog) {
            Some("default_catalog") | None => {
                let planner = self
                    .service
                    .local()
                    .read()
                    .expect("catalog service local read lock")
                    .get(database, table)?;
                Ok(ResolvedAnalyzerTable::from_planner(
                    Some("default_catalog"),
                    database,
                    planner,
                ))
            }
            Some(catalog) => match self.lookup_mode {
                TableLookupMode::SchemaOnly => {
                    let metadata = self
                        .service
                        .registry()
                        .read()
                        .expect("catalog service registry read lock")
                        .resolve(catalog, database, table)?;
                    let planner = metadata.to_table_def();
                    Ok(ResolvedAnalyzerTable {
                        catalog: metadata.table,
                        planner,
                    })
                }
                TableLookupMode::ExplainStats => {
                    let backend = self.connectors.catalog_backend("iceberg")?;
                    let source = self.connectors.table_source("iceberg")?;
                    let resolved = backend.load_table_for_read(catalog, database, table)?;
                    let planner = source.build_schema_table_def(&resolved)?;
                    Ok(ResolvedAnalyzerTable::from_planner(
                        Some(catalog),
                        database,
                        planner,
                    ))
                }
            },
        }
    }

    fn iceberg_metadata_table_def(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType,
    ) -> Result<TableDef, String> {
        match self.effective_catalog(catalog) {
            Some("default_catalog") | None => self
                .service
                .local()
                .read()
                .expect("catalog service local read lock")
                .get(database, table),
            Some(catalog)
                if metadata_table_type
                    == crate::connector::iceberg::IcebergMetadataTableType::Partitions =>
            {
                let backend = self.connectors.catalog_backend("iceberg")?;
                let source = self.connectors.table_source("iceberg")?;
                let resolved = backend.load_table_for_read(catalog, database, table)?;
                source.build_table_def(&resolved)
            }
            Some(catalog)
                if matches!(
                    metadata_table_type,
                    crate::connector::iceberg::IcebergMetadataTableType::Files
                        | crate::connector::iceberg::IcebergMetadataTableType::Manifests
                        | crate::connector::iceberg::IcebergMetadataTableType::LogicalIcebergMetadata
                ) =>
            {
                let backend = self.connectors.catalog_backend("iceberg")?;
                let source = self.connectors.table_source("iceberg")?;
                let resolved = backend.load_table_for_read(catalog, database, table)?;
                source.build_metadata_rows_table_def(&resolved, metadata_table_type)
            }
            Some(catalog) => {
                let backend = self.connectors.catalog_backend("iceberg")?;
                let source = self.connectors.table_source("iceberg")?;
                let resolved = backend.load_table_for_read(catalog, database, table)?;
                source.build_schema_table_def(&resolved)
            }
        }
    }
}

impl CatalogProvider for CatalogServiceProvider<'_> {
    fn get_table(&self, database: &str, table: &str) -> Result<CatalogTable, String> {
        self.resolve_table_for_analysis_once(None, database, table)
            .map(|resolved| resolved.catalog)
    }

    fn get_table_in_catalog(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<CatalogTable, String> {
        self.resolve_table_for_analysis_once(catalog, database, table)
            .map(|resolved| resolved.catalog)
    }

    fn get_legacy_range_partition(
        &self,
        database: &str,
        table: &str,
        partition: &str,
    ) -> Result<Option<LegacyRangePartition>, String> {
        self.service
            .local()
            .read()
            .expect("catalog service local read lock")
            .get_legacy_range_partition(database, table, partition)
    }
}

impl PlannerTableProvider for CatalogServiceProvider<'_> {
    fn resolve_table_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        self.resolve_table_for_analysis_once(catalog, database, table)
    }

    fn iceberg_metadata_provider(&self) -> Option<&dyn IcebergMetadataTableProvider> {
        Some(self)
    }
}

impl IcebergMetadataTableProvider for CatalogServiceProvider<'_> {
    fn get_iceberg_metadata_table(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType,
    ) -> Result<TableDef, String> {
        self.iceberg_metadata_table_def(catalog, database, table, metadata_table_type)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, RwLock};

    use arrow::datatypes::DataType;

    use super::CatalogServiceProvider;
    use crate::connector::backend::{
        CatalogBackend, CreateTableRequest, ResolvedTable, TableSource,
    };
    use crate::connector::iceberg::IcebergMetadataTableType;
    use crate::connector::iceberg::scan_model::{IcebergSchemaDef, IcebergTableInfo};
    use crate::sql::catalog::{
        CatalogRuntimeMetadata, IcebergMetadataTableProvider, PlannerTableProvider, TableLookupMode,
    };
    use crate::sql::parser::ast::AlterIcebergPartitionSpecStmt;
    use crate::sql::planner::table::{ScanSource, TableDef};
    use novarocks_catalog::identifier::TableIdentity;
    use novarocks_catalog::memory::MemoryCatalog;
    use novarocks_catalog::provider::CatalogProvider;
    use novarocks_catalog::registry::{Catalog, CatalogRegistry};
    use novarocks_catalog::schema::ColumnDef;
    use novarocks_catalog::service::CatalogService;

    fn column(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
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

    fn runtime_metadata() -> CatalogRuntimeMetadata {
        CatalogRuntimeMetadata::from_table_def(
            TableIdentity::new("ice", "db", "orders"),
            &TableDef {
                name: "orders".to_string(),
                columns: vec![column("id")],
                iceberg_row_lineage_metadata_columns: vec![column("_row_id")],
                source: ScanSource::IcebergDataFiles {
                    table: iceberg_info(),
                    files: vec![],
                    cloud_properties: Default::default(),
                    binding:
                        crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
                },
            },
        )
        .expect("runtime metadata")
    }

    struct TrackingCatalog {
        resolutions: Arc<AtomicUsize>,
    }

    impl Catalog<CatalogRuntimeMetadata> for TrackingCatalog {
        fn name(&self) -> &str {
            "ice"
        }

        fn get_table_metadata(
            &self,
            namespace: &str,
            table: &str,
        ) -> Result<CatalogRuntimeMetadata, String> {
            self.resolutions.fetch_add(1, Ordering::SeqCst);
            assert_eq!(namespace, "db");
            assert_eq!(table, "orders");
            Ok(runtime_metadata())
        }
    }

    fn service(resolutions: Arc<AtomicUsize>) -> CatalogService<TableDef, CatalogRuntimeMetadata> {
        let mut registry = CatalogRegistry::new();
        registry.register(Arc::new(TrackingCatalog { resolutions }));
        CatalogService::new(Arc::new(RwLock::new(MemoryCatalog::default())), registry)
    }

    #[test]
    fn ordinary_external_lookup_resolves_registry_exactly_once() {
        let resolutions = Arc::new(AtomicUsize::new(0));
        let service = service(Arc::clone(&resolutions));
        let connectors = crate::connector::ConnectorRegistry::default();
        let provider = CatalogServiceProvider::new(
            Some("ice"),
            &service,
            &connectors,
            TableLookupMode::SchemaOnly,
        );

        let resolved =
            PlannerTableProvider::resolve_table_for_analysis(&provider, None, "db", "orders")
                .expect("resolve table");

        assert_eq!(resolutions.load(Ordering::SeqCst), 1);
        assert_eq!(
            resolved.catalog.identity,
            TableIdentity::new("ice", "db", "orders")
        );
        assert_eq!(resolved.catalog.hidden_columns[0].name, "_row_id");
        assert_eq!(resolved.planner.name, "orders");
        assert!(matches!(
            resolved.planner.source,
            ScanSource::IcebergDataFiles { .. }
        ));
        assert!(
            service
                .local()
                .read()
                .expect("local read lock")
                .get("db", "orders")
                .is_err()
        );
    }

    #[test]
    fn neutral_external_lookup_uses_the_same_authoritative_resolution() {
        let resolutions = Arc::new(AtomicUsize::new(0));
        let service = service(Arc::clone(&resolutions));
        let connectors = crate::connector::ConnectorRegistry::default();
        let provider = CatalogServiceProvider::new(
            Some("ice"),
            &service,
            &connectors,
            TableLookupMode::SchemaOnly,
        );

        let table =
            CatalogProvider::get_table(&provider, "db", "orders").expect("resolve neutral table");

        assert_eq!(resolutions.load(Ordering::SeqCst), 1);
        assert_eq!(table.identity, TableIdentity::new("ice", "db", "orders"));
        assert_eq!(table.columns[0].name, "id");
        assert_eq!(table.hidden_columns[0].name, "_row_id");
    }

    fn resolved_table(catalog: &str, namespace: &str, table: &str) -> ResolvedTable {
        ResolvedTable {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            table: table.to_string(),
            columns: vec![column("id")],
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
                binding:
                    crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
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
        metadata_row_calls: Arc<AtomicUsize>,
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

        fn build_metadata_rows_table_def(
            &self,
            table: &ResolvedTable,
            _metadata_table_type: IcebergMetadataTableType,
        ) -> Result<TableDef, String> {
            self.metadata_row_calls.fetch_add(1, Ordering::SeqCst);
            Ok(connector_table_def(table, "metadata-rows"))
        }
    }

    fn tracking_connectors() -> (
        crate::connector::ConnectorRegistry,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let loads = Arc::new(AtomicUsize::new(0));
        let full_calls = Arc::new(AtomicUsize::new(0));
        let schema_calls = Arc::new(AtomicUsize::new(0));
        let metadata_row_calls = Arc::new(AtomicUsize::new(0));
        let mut connectors = crate::connector::ConnectorRegistry::default();
        connectors.register_catalog_backend(Arc::new(TrackingBackend {
            loads: Arc::clone(&loads),
        }));
        connectors.register_table_source(Arc::new(TrackingSource {
            full_calls: Arc::clone(&full_calls),
            schema_calls: Arc::clone(&schema_calls),
            metadata_row_calls: Arc::clone(&metadata_row_calls),
        }));
        (
            connectors,
            loads,
            full_calls,
            schema_calls,
            metadata_row_calls,
        )
    }

    #[test]
    fn metadata_lookup_preserves_explicit_connector_builder_lanes() {
        for (metadata_type, expected_metadata, expected_calls) in [
            (
                IcebergMetadataTableType::Partitions,
                "full-metadata",
                (1, 0, 0),
            ),
            (IcebergMetadataTableType::Files, "metadata-rows", (0, 0, 1)),
            (
                IcebergMetadataTableType::Manifests,
                "metadata-rows",
                (0, 0, 1),
            ),
            (
                IcebergMetadataTableType::LogicalIcebergMetadata,
                "metadata-rows",
                (0, 0, 1),
            ),
            (
                IcebergMetadataTableType::Snapshots,
                "schema-metadata",
                (0, 1, 0),
            ),
            (
                IcebergMetadataTableType::History,
                "schema-metadata",
                (0, 1, 0),
            ),
            (IcebergMetadataTableType::Refs, "schema-metadata", (0, 1, 0)),
        ] {
            let resolutions = Arc::new(AtomicUsize::new(0));
            let service = service(Arc::clone(&resolutions));
            let (connectors, loads, full_calls, schema_calls, metadata_row_calls) =
                tracking_connectors();
            let provider = CatalogServiceProvider::new(
                Some("ice"),
                &service,
                &connectors,
                TableLookupMode::SchemaOnly,
            );

            let table = IcebergMetadataTableProvider::get_iceberg_metadata_table(
                &provider,
                None,
                "db",
                "orders",
                metadata_type,
            )
            .expect("resolve metadata table");

            assert_eq!(resolutions.load(Ordering::SeqCst), 0);
            assert_eq!(loads.load(Ordering::SeqCst), 1);
            assert_eq!(full_calls.load(Ordering::SeqCst), expected_calls.0);
            assert_eq!(schema_calls.load(Ordering::SeqCst), expected_calls.1);
            assert_eq!(metadata_row_calls.load(Ordering::SeqCst), expected_calls.2);
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
                Some(expected_metadata)
            );
            assert_eq!(
                cloud_properties.get("aws.s3.endpoint").map(String::as_str),
                Some("http://minio:9000")
            );
        }
    }

    #[test]
    fn explain_stats_external_lookup_uses_schema_metadata_builder() {
        let resolutions = Arc::new(AtomicUsize::new(0));
        let service = service(Arc::clone(&resolutions));
        let (connectors, loads, full_calls, schema_calls, metadata_row_calls) =
            tracking_connectors();
        let provider = CatalogServiceProvider::new(
            Some("ice"),
            &service,
            &connectors,
            TableLookupMode::ExplainStats,
        );

        let resolved =
            PlannerTableProvider::resolve_table_for_analysis(&provider, None, "db", "orders")
                .expect("resolve table for EXPLAIN stats");

        assert_eq!(resolutions.load(Ordering::SeqCst), 0);
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        assert_eq!(full_calls.load(Ordering::SeqCst), 0);
        assert_eq!(schema_calls.load(Ordering::SeqCst), 1);
        assert_eq!(metadata_row_calls.load(Ordering::SeqCst), 0);
        let ScanSource::IcebergDataFiles { table, .. } = resolved.planner.source else {
            panic!("expected iceberg source");
        };
        assert_eq!(
            table.serialized_metadata.as_deref(),
            Some("schema-metadata")
        );
    }
}
