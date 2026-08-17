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

use novarocks_sql::planning::catalog::PlannerMemoryCatalog;

#[derive(Clone, Debug)]
pub struct CatalogRuntimeMetadata {
    table: CatalogTable,
}

impl CatalogRuntimeMetadata {
    fn from_local_catalog_table(identity: TableIdentity, table: CatalogTable) -> Self {
        // Registry cache entries describe only durable SQL catalog facts.
        // They intentionally retain neither a scan source nor a connector
        // descriptor: every query receives fresh provider authority through
        // the query-local materialization envelope and binding store.
        Self {
            table: CatalogTable { identity, ..table },
        }
    }

    fn from_connector_materialization(
        identity: TableIdentity,
        materialization: &crate::query_execution::planning::catalog_materializer::ConnectorQueryTableMaterialization,
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
pub type QueryCatalogService =
    CatalogService<novarocks_sql::planning::catalog::SqlLocalCatalogEntry, CatalogRuntimeMetadata>;

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
        let local = self.local.read().expect("internal catalog read lock");
        let catalog_table =
            novarocks_sql::planning::catalog::local_catalog_table(&local, namespace, table)?;
        Ok(CatalogRuntimeMetadata::from_local_catalog_table(
            TableIdentity::new(&self.name, namespace, table),
            catalog_table,
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
            crate::query_execution::planning::catalog_materializer::load_connector_table_materialization_with_lease(
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

/// Narrow source for request-local catalog snapshots.
///
/// Compilation and command handlers receive a frozen catalog view rather than
/// the aggregate application state that historically owned it. The trait lives
/// beside `QueryCatalogService` — the only type it exposes — so that catalog
/// consumers do not have to reach into query assembly to name it. Implementors
/// are the composition-side capability values.
pub trait CatalogServiceSource {
    fn catalog_service(&self) -> &Arc<QueryCatalogService>;
}

/// Freezes a catalog source into an owned, request-local catalog view.
///
/// Both the source trait and the produced service belong to this module, so
/// the snapshot operation does too: it reads catalog state and writes catalog
/// state, and knows nothing about query compilation.
pub fn catalog_service_snapshot(source: &impl CatalogServiceSource) -> QueryCatalogService {
    QueryCatalogService::new(
        Arc::new(RwLock::new(source.catalog_service().local_snapshot())),
        source.catalog_service().registry_snapshot(),
    )
}

/// Query-local overlays never call this helper: they are scoped to their
/// binding store and are not registered in the shared catalog in the first
/// place.
pub fn drop_local_table_registration_if_exists(
    source: &impl CatalogServiceSource,
    namespace: &str,
    table: &str,
) -> Result<(), String> {
    let mut guard = source
        .catalog_service()
        .local()
        .write()
        .map_err(|error| format!("standalone catalog write lock: {error}"))?;
    match guard.drop_table(namespace, table) {
        Ok(()) => Ok(()),
        Err(error) if error.contains("unknown") => Ok(()),
        Err(error) => Err(format!("drop local table metadata: {error}")),
    }
}
