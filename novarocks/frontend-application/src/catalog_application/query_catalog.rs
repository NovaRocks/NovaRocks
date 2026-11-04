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

use crate::connector::schema_creation::FrontendSchemaLoad;
use std::sync::{Arc, RwLock};

use novarocks_types::naming::TableIdentity;
use novarocks_types::schema::CatalogTable;

use novarocks_sql::planning::catalog::PlannerMemoryCatalog;

mod registry;
pub(crate) mod relation;
mod schema_cache;
mod service;
#[cfg(test)]
mod variant_identity;

use registry::{Catalog, CatalogRegistry};
use schema_cache::SchemaCache;
pub use service::QueryCatalogService;

/// Typed result of resolving one catalog relation for an admitted request.
///
/// `Missing` is the only variant that a SQL completion adapter may project as
/// an absent relation. Every other failure terminates completion instead of
/// making an unavailable or inconsistent catalog look like a missing table.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogResolutionError {
    Missing {
        reason: String,
    },
    Failed {
        message: String,
        error: novarocks_sql::compiler::SqlCompileError,
        original_provider: Option<
            Box<(
                novarocks_spi::connector::ConnectorError,
                novarocks_failure_contract::SharedFailureCause,
            )>,
        >,
    },
}

impl CatalogResolutionError {
    pub fn missing(reason: impl Into<String>) -> Self {
        Self::Missing {
            reason: reason.into(),
        }
    }

    pub fn failed(message: impl Into<String>) -> Self {
        let message = message.into();
        Self::Failed {
            error: novarocks_sql::compiler::SqlCompileError::Compilation(message.clone()),
            original_provider: None,
            message,
        }
    }

    /// Retain the producer's selected cause across statement memoization and
    /// SQL completion. A message never selects a failure classification.
    pub fn from_connector(error: novarocks_spi::connector::ConnectorError) -> Self {
        use novarocks_spi::connector::ConnectorErrorKind;
        use novarocks_sql::compiler::SqlCompileError;
        let message = error.to_string();
        let source = std::error::Error::source(&error)
            .and_then(|source| {
                source.downcast_ref::<novarocks_failure_contract::SharedFailureCause>()
            })
            .cloned();
        let original_source = source
            .clone()
            .unwrap_or_else(|| novarocks_failure_contract::SharedFailureCause::new(error.clone()));
        let original_provider = Some(Box::new((error.clone(), original_source.clone())));
        let error = if let Some(source) = source {
            match source.downcast_ref::<novarocks_type_contract::CompileControlError>() {
                Some(control) => SqlCompileError::from(control.clone()),
                None => SqlCompileError::CompilationSource(source),
            }
        } else {
            match error.kind() {
                ConnectorErrorKind::Cancelled => SqlCompileError::Cancelled,
                ConnectorErrorKind::DeadlineExceeded => SqlCompileError::DeadlineExceeded,
                ConnectorErrorKind::ResourceExhausted => SqlCompileError::ResourceExhausted,
                _ => SqlCompileError::CompilationSource(original_source),
            }
        };
        Self::Failed {
            message,
            error,
            original_provider,
        }
    }

    pub(crate) fn original_provider(
        &self,
    ) -> Option<(
        &novarocks_spi::connector::ConnectorError,
        &novarocks_failure_contract::SharedFailureCause,
    )> {
        match self {
            Self::Failed {
                original_provider, ..
            } => original_provider
                .as_deref()
                .map(|(error, source)| (error, source)),
            Self::Missing { .. } => None,
        }
    }

    pub fn into_compile(self) -> novarocks_sql::compiler::SqlCompileError {
        match self {
            Self::Missing { reason } => {
                novarocks_sql::compiler::SqlCompileError::Compilation(reason)
            }
            Self::Failed { error, .. } => error,
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::Missing { reason } => reason,
            Self::Failed { message, .. } => message,
        }
    }

    pub fn into_message(self) -> String {
        match self {
            Self::Missing { reason } => reason,
            Self::Failed { message, .. } => message,
        }
    }
}

impl From<novarocks_sql::compiler::SqlCompileError> for CatalogResolutionError {
    fn from(error: novarocks_sql::compiler::SqlCompileError) -> Self {
        Self::Failed {
            message: error.to_string(),
            error,
            original_provider: None,
        }
    }
}

impl std::fmt::Display for CatalogResolutionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.message())
    }
}

impl std::error::Error for CatalogResolutionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Missing { .. } => None,
            Self::Failed { error, .. } => Some(error),
        }
    }
}

pub type CatalogResolutionResult<T> = Result<T, CatalogResolutionError>;

/// Provider-neutral table facts admitted for one request.  Core projects the
/// typed SPI metadata into SQL facts, preserves the opaque scan authority, and
/// never decodes a provider table handle or metadata payload.
#[derive(Clone)]
pub struct ConnectorQueryTableMaterialization {
    /// Frozen desired-state identity shared by every execution artifact this
    /// query derives from the admitted control lease.
    pub schema_version: Option<Vec<u8>>,
    /// The exact sealed metadata used to materialize this SQL relation. It
    /// remains request-local so CREATE can ask the provider for opaque source
    /// field identities without resolving a newer generation.
    pub source_metadata: novarocks_spi::connector::ConnectorTableMetadata,
    pub columns: Vec<novarocks_types::schema::ColumnDef>,
    pub row_lineage_metadata_columns: Vec<novarocks_types::schema::ColumnDef>,
    pub read_table: novarocks_spi::connector::ConnectorTableHandle,
    pub read_schema: arrow::datatypes::SchemaRef,
    pub read_selector: novarocks_spi::connector::ConnectorReadSelector,
    pub sql_planning_facts: novarocks_spi::connector::ConnectorTablePlanningFacts,
    pub statistics_pin: Option<crate::connector::backend::ResolvedTableStatisticsPin>,
    pub planning_pin: novarocks_spi::connector::ConnectorControlPlanningPin,
}

pub fn load_connector_table_materialization_with_lease(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> Result<ConnectorQueryTableMaterialization, String> {
    load_connector_table_materialization_with_lease_typed(
        controls, context, catalog, namespace, table,
    )
    .map_err(CatalogResolutionError::into_message)
}

/// Resolve a base table while retaining the distinction between an absent
/// relation and a catalog/control failure.
pub fn load_connector_table_materialization_with_lease_typed(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
) -> CatalogResolutionResult<ConnectorQueryTableMaterialization> {
    load_connector_table_materialization_with_resolution_typed(
        controls,
        context,
        catalog,
        namespace,
        table,
        novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
    )
}

/// Resolve a provider-defined alias while retaining typed absence semantics.
pub fn load_connector_table_alias_materialization_with_lease_typed(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    alias: &str,
) -> CatalogResolutionResult<ConnectorQueryTableMaterialization> {
    load_connector_table_materialization_with_resolution_typed(
        controls,
        context,
        catalog,
        namespace,
        alias,
        novarocks_spi::connector::ConnectorTableResolution::ProviderReadAlias,
    )
}

pub(crate) fn load_connector_table_materialization_with_resolution_typed(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
    resolution: novarocks_spi::connector::ConnectorTableResolution,
) -> CatalogResolutionResult<ConnectorQueryTableMaterialization> {
    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorTableIdentity, ConnectorTableRequest,
    };

    let instance_id = ConnectorInstanceId::parse(catalog)
        .map_err(|error| CatalogResolutionError::failed(error.to_string()))?;
    let planning_pin = controls
        .acquire_current(&instance_id)
        .map_err(CatalogResolutionError::from_connector)?;
    // The query-wide collector is attached at admitted query preparation, but
    // only this exact planning pin knows which durable catalog generation a
    // metadata response belongs to. Decorate this one request after acquiring
    // the lease so a multi-catalog query cannot misattribute a vended seed.
    let context = if context.vended_credential_lease_sink().is_some() {
        context
            .with_vended_credential_lease_collection(
                planning_pin
                    .binding()
                    .catalog_properties()
                    .map_err(CatalogResolutionError::from_connector)?
                    .clone(),
            )
            .map_err(|error| CatalogResolutionError::failed(error.to_string()))?
    } else {
        context
    };
    let metadata = planning_pin
        .binding()
        .metadata()
        .load_frontend_table(ConnectorTableRequest {
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
        .map_err(|error| connector_table_resolution_error(error, namespace, table))?;
    connector_table_materialization_for_resolution(metadata, planning_pin, resolution)
        .map_err(CatalogResolutionError::failed)
}

pub(crate) async fn load_connector_table_materialization_with_resolution_async(
    controls: &dyn novarocks_spi::connector::ConnectorControlResolver,
    context: novarocks_spi::connector::ConnectorRequestContext,
    catalog: &str,
    namespace: &str,
    table: &str,
    resolution: novarocks_spi::connector::ConnectorTableResolution,
) -> CatalogResolutionResult<ConnectorQueryTableMaterialization> {
    use novarocks_spi::connector::{
        ConnectorInstanceId, ConnectorTableIdentity, ConnectorTableRequest,
    };

    let instance_id = ConnectorInstanceId::parse(catalog)
        .map_err(|error| CatalogResolutionError::failed(error.to_string()))?;
    let planning_pin = controls
        .acquire_current(&instance_id)
        .map_err(CatalogResolutionError::from_connector)?;
    // The query-wide collector is attached at admitted query preparation, but
    // only this exact planning pin knows which durable catalog generation a
    // metadata response belongs to. Decorate this one request after acquiring
    // the lease so a multi-catalog query cannot misattribute a vended seed.
    let context = if context.vended_credential_lease_sink().is_some() {
        context
            .with_vended_credential_lease_collection(
                planning_pin
                    .binding()
                    .catalog_properties()
                    .map_err(CatalogResolutionError::from_connector)?
                    .clone(),
            )
            .map_err(|error| CatalogResolutionError::failed(error.to_string()))?
    } else {
        context
    };
    let metadata = crate::connector::schema_creation::load_frontend_table_async(
        planning_pin.binding().metadata().as_ref(),
        ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from(namespace),
                table: Arc::from(table),
            },
            resolution,
            context,
        },
    )
    .await
    // An absent relation is a SQL name-resolution failure, not a provider
    // incident: render the vocabulary the rest of the engine already
    // recognizes instead of leaking the provider's own wording.
    .map_err(|error| connector_table_resolution_error(error, namespace, table))?;
    connector_table_materialization_for_resolution(metadata, planning_pin, resolution)
        .map_err(CatalogResolutionError::failed)
}

pub(super) fn connector_table_resolution_error(
    error: novarocks_spi::connector::ConnectorError,
    namespace: &str,
    table: &str,
) -> CatalogResolutionError {
    match error.kind() {
        novarocks_spi::connector::ConnectorErrorKind::NotFound => {
            CatalogResolutionError::missing(format!("unknown table: {namespace}.{table}"))
        }
        _ => CatalogResolutionError::from_connector(error),
    }
}

/// Keep the metadata schema and executable read selector from the same request.
/// Callers must never bind a Historical schema and accidentally read Current.
fn connector_table_materialization_for_resolution(
    metadata: novarocks_spi::connector::ConnectorTableMetadata,
    planning_pin: novarocks_spi::connector::ConnectorControlPlanningPin,
    resolution: novarocks_spi::connector::ConnectorTableResolution,
) -> Result<ConnectorQueryTableMaterialization, String> {
    let mut materialization =
        connector_table_materialization_from_metadata(metadata, planning_pin)?;
    if let novarocks_spi::connector::ConnectorTableResolution::HistoricalSnapshot(snapshot) =
        resolution
    {
        materialization.read_selector =
            novarocks_spi::connector::ConnectorReadSelector::SnapshotId(snapshot);
    }
    Ok(materialization)
}

pub fn connector_table_materialization_from_metadata(
    metadata: novarocks_spi::connector::ConnectorTableMetadata,
    planning_pin: novarocks_spi::connector::ConnectorControlPlanningPin,
) -> Result<ConnectorQueryTableMaterialization, String> {
    use novarocks_spi::connector::{ConnectorTableColumnRole, ConnectorTableColumnVisibility};

    let mut columns = Vec::new();
    let mut row_lineage_metadata_columns = Vec::new();
    for (ordinal, field) in metadata.schema().fields().iter().enumerate() {
        let fact = metadata.planning_facts.column_facts().get(ordinal);
        let column = crate::connector::sql_column_from_connector_binding(
            field.name(),
            metadata
                .execution_schema()
                .binding(ordinal)
                .ok_or("catalog execution schema is missing an authored column")?,
            &metadata.planning_facts,
            ordinal,
        )?;
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
    let catalog_handle = planning_pin
        .binding()
        .catalog_handle()
        .map_err(|error| error.to_string())?
        .clone();
    if catalog_handle.catalog_name() != metadata.table.owner() {
        return Err(
            "connector metadata owner does not match the admitted catalog handle".to_string(),
        );
    }
    Ok(ConnectorQueryTableMaterialization {
        schema_version: metadata.version.as_ref().map(|version| version.to_vec()),
        source_metadata: metadata.clone(),
        columns,
        row_lineage_metadata_columns,
        read_table: metadata.table.clone(),
        read_schema: Arc::new(metadata.schema().clone()),
        read_selector: novarocks_spi::connector::ConnectorReadSelector::Current,
        sql_planning_facts: metadata.planning_facts.clone(),
        statistics_pin,
        planning_pin,
    })
}

#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "Retained for target-specific frontend integration and regression coverage."
)]
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

impl Catalog for InternalCatalog {
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
    cache: SchemaCache,
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

impl Catalog for ConnectorCatalog {
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
                novarocks_spi::connector::ConnectorStopOwner::new().view(),
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
    let service = QueryCatalogService::new(Arc::clone(&local), CatalogRegistry::new());
    service.register_catalog(Arc::new(InternalCatalog::new("default_catalog", local)));
    service
}

pub(crate) fn build_connector_catalog(
    name: &str,
    controls: Arc<dyn novarocks_spi::connector::ConnectorControlResolver>,
) -> Arc<dyn Catalog> {
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

#[cfg(test)]
mod typed_resolution_tests {
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

    use super::{CatalogResolutionError, connector_table_resolution_error};

    #[test]
    fn catalog_memo_preserves_original_controls_and_domain_causes() {
        use novarocks_sql::compiler::SqlCompileError;
        use novarocks_type_contract::{
            CompileControlError, owned_resources::layout::LayoutResourceError,
        };
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
            CompileControlError::from_domain(LayoutResourceError::SourceModel),
        ] {
            let error = CatalogResolutionError::from_connector(
                ConnectorError::compile_interruption(control.clone()),
            );
            assert_eq!(error.clone().into_compile(), SqlCompileError::from(control));
        }
        let provider = ConnectorError::new(ConnectorErrorKind::Unsupported, "unknown table: db.t");
        let error = connector_table_resolution_error(provider.clone(), "db", "t");
        let SqlCompileError::CompilationSource(source) = error.into_compile() else {
            panic!("message text cannot turn a real capability refusal into absence");
        };
        assert_eq!(source.downcast_ref::<ConnectorError>(), Some(&provider));
    }

    #[test]
    fn connector_not_found_is_the_only_missing_resolution() {
        let missing = connector_table_resolution_error(
            ConnectorError::new(ConnectorErrorKind::NotFound, "provider-specific absence"),
            "sales",
            "orders",
        );
        let unavailable = connector_table_resolution_error(
            ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                "catalog service unavailable",
            ),
            "sales",
            "orders",
        );

        assert_eq!(
            missing,
            CatalogResolutionError::Missing {
                reason: "unknown table: sales.orders".to_string(),
            }
        );
        assert_eq!(
            unavailable.message(),
            "Unavailable: catalog service unavailable"
        );
        let novarocks_sql::compiler::SqlCompileError::CompilationSource(source) =
            unavailable.into_compile()
        else {
            panic!("an unavailable provider must retain its original cause");
        };
        assert_eq!(
            source.downcast_ref::<ConnectorError>().unwrap().kind(),
            ConnectorErrorKind::Unavailable,
        );
    }
}
