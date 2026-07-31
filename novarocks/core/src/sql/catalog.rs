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

use crate::connector::ConnectorRegistry;
use crate::sql::planner::table::TableDef;
use novarocks_catalog::identifier::TableIdentity;
use novarocks_catalog::registry::{Catalog, CatalogRegistry};
use novarocks_catalog::service::CatalogService;
use novarocks_catalog::table::CatalogTable;

mod conversion;
mod iceberg;
mod internal;
pub(crate) mod local;
mod metadata;
pub(crate) mod provider;

#[cfg(test)]
use metadata::CatalogRuntimeBinding;
use metadata::CatalogRuntimeMetadata;

pub(crate) type StandaloneCatalogService = CatalogService<TableDef, CatalogRuntimeMetadata>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TableLookupMode {
    SchemaOnly,
    ExplainStats,
}

pub(crate) fn build_internal_catalog(
    name: &str,
    local: Arc<RwLock<local::PlannerMemoryCatalog>>,
) -> Arc<dyn Catalog<CatalogRuntimeMetadata>> {
    Arc::new(internal::InternalCatalog::new(name, local))
}

pub(crate) fn build_iceberg_catalog(
    name: &str,
    controls: std::sync::Arc<dyn novarocks_spi::connector::ConnectorControlResolver>,
) -> Arc<dyn Catalog<CatalogRuntimeMetadata>> {
    Arc::new(iceberg::IcebergCatalog::new(name, controls))
}

pub(crate) fn new_standalone_catalog_service() -> StandaloneCatalogService {
    let local = Arc::new(RwLock::new(local::PlannerMemoryCatalog::default()));
    let service = CatalogService::new(Arc::clone(&local), CatalogRegistry::new());
    service.register_catalog(build_internal_catalog(
        "default_catalog",
        Arc::clone(&local),
    ));
    service
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedAnalyzerTable {
    pub(crate) catalog: CatalogTable,
    pub(crate) planner: TableDef,
}

impl ResolvedAnalyzerTable {
    pub(crate) fn from_planner(catalog: Option<&str>, database: &str, planner: TableDef) -> Self {
        let identity = TableIdentity::new(
            catalog.unwrap_or("default_catalog"),
            database,
            &planner.name,
        );
        let table = CatalogTable {
            identity,
            columns: planner.columns.clone(),
            hidden_columns: planner.iceberg_row_lineage_metadata_columns.clone(),
        };
        Self {
            catalog: table,
            planner,
        }
    }
}

/// Planner-facing table materialization extension.
///
/// This is the only ordinary analyzer/planner lookup seam: implementations
/// must return the neutral schema and planner binding from one authoritative
/// resolution.
pub(crate) trait PlannerTableProvider {
    fn resolve_table_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String>;

    fn iceberg_metadata_provider(&self) -> Option<&dyn IcebergMetadataTableProvider> {
        None
    }

    /// Statistics pins captured while this provider resolved the query's
    /// tables. A statistics read must use this exact resolution rather than
    /// resolving `latest` a second time during optimization.
    fn statistics_pins(&self) -> Option<provider::QueryStatisticsPins> {
        None
    }
}

pub(crate) trait IcebergMetadataTableProvider {
    fn get_iceberg_metadata_table(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: crate::connector::iceberg::IcebergMetadataTableType,
    ) -> Result<TableDef, String>;
}

#[cfg(test)]
mod visibility_tests {
    #[test]
    fn exposes_canonical_standalone_catalog_service_alias() {
        let source = include_str!("catalog.rs");

        assert!(
            source.contains(
                "pub(crate) type StandaloneCatalogService = CatalogService<TableDef, CatalogRuntimeMetadata>;"
            ),
            "sql::catalog must expose the canonical standalone catalog service specialization"
        );
    }

    #[test]
    fn runtime_metadata_module_remains_private_to_sql_catalog() {
        let source = include_str!("catalog.rs");

        assert!(
            source.lines().any(|line| line.trim() == "mod metadata;"),
            "CatalogRuntimeMetadata must remain behind a private sql::catalog module"
        );
        assert!(
            !source
                .lines()
                .any(|line| line.trim() == "pub(crate) mod metadata;"),
            "sql::catalog siblings must not name the runtime metadata module"
        );
        assert!(
            !source.lines().any(|line| {
                let line = line.trim();
                (line.starts_with("pub use ") || line.starts_with("pub(crate) use "))
                    && line.contains("CatalogRuntimeMetadata")
            }),
            "sql::catalog must not re-export CatalogRuntimeMetadata"
        );
    }
}
