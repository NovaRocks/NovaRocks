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
//!
//! The seam against query assembly runs through
//! [`ConnectorQueryTableMaterialization`]: acquiring the connector control
//! lease and projecting provider metadata into neutral catalog facts is a
//! catalog responsibility and lives here, while turning those facts into a
//! request-local SQL binding belongs to query assembly. The dependency is
//! therefore one-way: query assembly reads this module, and this module never
//! reaches back into it.

use std::sync::{Arc, RwLock};

use novarocks_catalog::identifier::TableIdentity;
use novarocks_catalog::registry::{Catalog, CatalogRegistry};
use novarocks_catalog::schema_cache::SchemaCache;
use novarocks_catalog::service::CatalogService;
use novarocks_catalog::table::CatalogTable;

use novarocks_sql::planning::catalog::PlannerMemoryCatalog;

/// Provider-neutral table facts admitted for one request.  Core projects the
/// typed SPI metadata into SQL facts, preserves the opaque scan authority, and
/// never decodes a provider table handle or metadata payload.
#[derive(Clone)]
pub struct ConnectorQueryTableMaterialization {
    pub schema_version: Option<Vec<u8>>,
    pub columns: Vec<novarocks_catalog::schema::ColumnDef>,
    pub row_lineage_metadata_columns: Vec<novarocks_catalog::schema::ColumnDef>,
    pub read_table: novarocks_spi::connector::ConnectorTableHandle,
    pub read_schema: arrow::datatypes::SchemaRef,
    pub read_selector: novarocks_spi::connector::ConnectorReadSelector,
    pub sql_planning_facts: novarocks_spi::connector::ConnectorTablePlanningFacts,
    pub statistics_pin: Option<crate::connector::backend::ResolvedTableStatisticsPin>,
    pub planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
}

pub fn load_connector_table_materialization_with_lease(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> Result<ConnectorQueryTableMaterialization, String> {
    load_connector_table_materialization_with_resolution(
        controls,
        context,
        catalog,
        namespace,
        table,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
    )
}

/// Load one provider-defined read alias through the same opaque metadata
/// contract used for base tables. The alias syntax is application-owned, but
/// Core neither decodes the returned table handle nor names a provider type.
pub fn load_connector_table_alias_materialization_with_lease(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    alias: &str,
) -> Result<ConnectorQueryTableMaterialization, String> {
    load_connector_table_materialization_with_resolution(
        controls,
        context,
        catalog,
        namespace,
        alias,
        novarocks_spi::connector::ConnectorTableResolution::ProviderReadAlias,
    )
}

fn load_connector_table_materialization_with_resolution(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
    resolution: novarocks_spi::connector::ConnectorTableResolution,
) -> Result<ConnectorQueryTableMaterialization, String> {
    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorTableIdentity, ConnectorTableRequest,
    };

    let instance_id = ConnectorInstanceId::parse(catalog).map_err(|error| error.to_string())?;
    let planning_lease = controls
        .acquire_current(&instance_id)
        .map_err(|error| error.to_string())?;
    let metadata = planning_lease
        .binding()
        .metadata()
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            resolution,
            context,
        })
        // An absent relation is a SQL name-resolution failure, not a provider
        // incident: render the vocabulary the rest of the engine already
        // recognizes instead of leaking the provider's own wording.
        .map_err(|error| match error.kind() {
            novarocks_spi::connector::ConnectorErrorKind::NotFound => {
                format!("unknown table: {namespace}.{table}")
            }
            _ => error.to_string(),
        })?;
    connector_table_materialization_from_metadata(metadata, planning_lease)
}

pub fn connector_table_materialization_from_metadata(
    metadata: novarocks_spi::connector::ConnectorTableMetadata,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
) -> Result<ConnectorQueryTableMaterialization, String> {
    use novarocks_spi::connector::{
        ConnectorTableColumnRole, ConnectorTableColumnSemanticKind, ConnectorTableColumnVisibility,
    };

    let mut columns = Vec::new();
    let mut row_lineage_metadata_columns = Vec::new();
    for (ordinal, field) in metadata.schema.fields().iter().enumerate() {
        let fact = metadata.planning_facts.column_facts().get(ordinal);
        let logical_type = match fact.map(|fact| fact.semantic_kind()) {
            Some(ConnectorTableColumnSemanticKind::Bitmap) => {
                Some(novarocks_catalog::schema::SqlType::Bitmap)
            }
            Some(ConnectorTableColumnSemanticKind::Hll) => {
                Some(novarocks_catalog::schema::SqlType::Hll)
            }
            _ => None,
        };
        let column = novarocks_catalog::schema::ColumnDef {
            name: field.name().to_string(),
            data_type: field.data_type().clone(),
            nullable: field.is_nullable(),
            write_default: crate::connector::connector_write_default_at(
                &metadata.planning_facts,
                ordinal,
            ),
            logical_type,
        };
        match fact.map(|fact| fact.role()) {
            Some(ConnectorTableColumnRole::RowLineageSystem) => {
                row_lineage_metadata_columns.push(column)
            }
            _ if matches!(
                fact.map(|fact| fact.visibility()),
                Some(ConnectorTableColumnVisibility::Hidden)
            ) => {}
            _ => columns.push(column),
        }
    }
    let statistics_pin = metadata
        .statistics_data_version
        .clone()
        .map(
            |data_version| crate::connector::backend::ResolvedTableStatisticsPin {
                table: metadata.table.clone(),
                data_version,
            },
        );
    Ok(ConnectorQueryTableMaterialization {
        schema_version: metadata.version.map(|version| version.to_vec()),
        columns,
        row_lineage_metadata_columns,
        read_table: metadata.table,
        read_schema: metadata.schema.clone(),
        read_selector: novarocks_spi::connector::ConnectorReadSelector::Current,
        sql_planning_facts: metadata.planning_facts,
        statistics_pin,
        planning_lease,
    })
}

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
        materialization: &ConnectorQueryTableMaterialization,
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
        let materialization = load_connector_table_materialization_with_lease(
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

pub fn build_connector_catalog(
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
