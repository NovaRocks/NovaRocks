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

//! Application catalog registry and connector materialization.
//!
//! The SQL catalog vocabulary exposes neutral `ResolvedAnalyzerTable` facts.
//! This module owns catalog registry cache entries and all connector control
//! acquisition needed to materialize them.

use std::sync::{Arc, RwLock};

use novarocks_catalog::identifier::TableIdentity;
use novarocks_catalog::registry::{Catalog, CatalogRegistry};
use novarocks_catalog::schema_cache::SchemaCache;
use novarocks_catalog::service::CatalogService;
use novarocks_catalog::table::CatalogTable;

use crate::sql::catalog::local::PlannerMemoryCatalog;
use crate::sql::planner::table::TableDef;

#[derive(Clone, Debug)]
pub struct CatalogRuntimeMetadata {
    table: CatalogTable,
}

impl CatalogRuntimeMetadata {
    fn from_table_def(identity: TableIdentity, table_def: &TableDef) -> Self {
        // Registry cache entries describe only durable SQL catalog facts.
        // They intentionally retain neither a scan source nor a connector
        // descriptor: every query receives fresh provider authority through
        // the query-local materialization envelope and binding store.
        Self {
            table: CatalogTable {
                identity,
                columns: table_def.columns.clone(),
                hidden_columns: table_def.iceberg_row_lineage_metadata_columns.clone(),
            },
        }
    }

    fn from_connector_materialization(
        identity: TableIdentity,
        materialization: &crate::engine::query_planning::catalog_materializer::ConnectorQueryTableMaterialization,
    ) -> Self {
        Self {
            table: CatalogTable {
                identity,
                columns: materialization.columns.clone(),
                hidden_columns: materialization.row_lineage_metadata_columns.clone(),
            },
        }
    }
}

/// Query catalog registry owned by the Frontend composition root.
///
/// The registry keeps durable SQL catalog facts only. Provider authority is
/// supplied separately through the exact Connector control lease captured for
/// each request.
pub type QueryCatalogService = CatalogService<TableDef, CatalogRuntimeMetadata>;

struct InternalCatalog {
    name: String,
    local: Arc<RwLock<PlannerMemoryCatalog>>,
}

impl InternalCatalog {
    fn new(name: &str, local: Arc<RwLock<PlannerMemoryCatalog>>) -> Self {
        Self {
            name: name.to_string(),
            local,
        }
    }
}

impl Catalog<CatalogRuntimeMetadata> for InternalCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    fn get_table_metadata(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<CatalogRuntimeMetadata, String> {
        let table_def = self
            .local
            .read()
            .expect("internal catalog read lock")
            .get(namespace, table)?;
        Ok(CatalogRuntimeMetadata::from_table_def(
            TableIdentity::new(&self.name, namespace, table),
            &table_def,
        ))
    }
}

/// Registry entry for one admitted external catalog runtime.
///
/// It holds no provider concrete: every schema fact is materialized through the
/// exact connector control lease the Frontend published for this SQL name.
struct ConnectorCatalog {
    name: String,
    controls: Arc<dyn novarocks_spi::connector::ConnectorControlResolver>,
    cache: SchemaCache<CatalogRuntimeMetadata>,
}

impl ConnectorCatalog {
    fn new(
        name: &str,
        controls: Arc<dyn novarocks_spi::connector::ConnectorControlResolver>,
    ) -> Self {
        Self {
            name: name.to_string(),
            controls,
            cache: SchemaCache::new(),
        }
    }
}

impl Catalog<CatalogRuntimeMetadata> for ConnectorCatalog {
    fn name(&self) -> &str {
        &self.name
    }

    fn get_table_metadata(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<CatalogRuntimeMetadata, String> {
        let identity = TableIdentity::new(&self.name, namespace, table);
        let materialization =
            crate::engine::query_planning::catalog_materializer::load_connector_table_materialization_with_lease(
                self.controls.as_ref(),
                crate::connector::connector_request_context(
                    None,
                    Arc::new(std::sync::atomic::AtomicBool::new(false)),
                )?,
                &self.name,
                namespace,
                table,
            )?;
        self.cache
            .get_or_build_validated(&identity, materialization.schema_version.clone(), || {
                Ok(CatalogRuntimeMetadata::from_connector_materialization(
                    identity.clone(),
                    &materialization,
                ))
            })
    }

    fn invalidate_table(&self, namespace: &str, table: &str) {
        self.cache
            .invalidate(&TableIdentity::new(&self.name, namespace, table));
    }
}

/// Create an empty Frontend query-catalog runtime with the built-in local
/// catalog registered. External catalog runtimes must be bound through
/// [`crate::catalog_application::CatalogRuntimeProjection`] after the
/// Frontend has installed their Connector control generation.
pub fn new_query_catalog_service() -> QueryCatalogService {
    let local = Arc::new(RwLock::new(PlannerMemoryCatalog::default()));
    let service = CatalogService::new(Arc::clone(&local), CatalogRegistry::new());
    service.register_catalog(Arc::new(InternalCatalog::new("default_catalog", local)));
    service
}

pub(crate) fn build_connector_catalog(
    name: &str,
    controls: Arc<dyn novarocks_spi::connector::ConnectorControlResolver>,
) -> Arc<dyn Catalog<CatalogRuntimeMetadata>> {
    Arc::new(ConnectorCatalog::new(name, controls))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use novarocks_catalog::schema::ColumnDef;

    use super::CatalogRuntimeMetadata;
    use crate::sql::planner::table::TableDef;
    use novarocks_catalog::identifier::TableIdentity;

    #[test]
    fn sqlx2_catalog_runtime_keeps_only_schema_facts() {
        let table = TableDef {
            name: "orders".to_string(),
            columns: vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
            iceberg_row_lineage_metadata_columns: vec![],
            source: crate::sql::planner::table::test_sql_scan_source(
                crate::sql::planner::table::SqlScanKind::ConnectorRead,
            ),
        };
        let metadata = CatalogRuntimeMetadata::from_table_def(
            TableIdentity::new("default_catalog", "db", "orders"),
            &table,
        );
        assert_eq!(metadata.table.identity.fqn(), "default_catalog.db.orders");
        assert_eq!(metadata.table.columns, table.columns);
    }
}
