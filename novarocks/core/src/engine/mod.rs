#![allow(dead_code)]
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

use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::OnceLock;
use std::sync::{Arc, Mutex, RwLock, Weak};
#[cfg(test)]
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::runtime::Handle;

use crate::mv::refresh::execution_context::MvRefreshPruningLimits;
use crate::novarocks_config;
use crate::query_execution::prepared_write::PreparedDistributedWriteRequest;
use crate::runtime::global_async_runtime::data_block_on;
use crate::runtime::query_result::{
    QueryResult, QueryResultColumn, build_string_query_result, record_batch_to_chunk,
};
use novarocks_execution::runtime::query_options::QueryOptions;

use crate::catalog_attachment::{CatalogAttachmentProperties, CatalogAttachmentRepository};
use crate::connector::IcebergCatalogRegistry;
use crate::engine::query_planning::catalog_runtime::QueryCatalogService;
use crate::meta::repository::iceberg_operation::IcebergOperationRepository;
use crate::meta::repository::job::JobMetaRepository;
#[cfg(test)]
use crate::mv::application::UnavailableMvApplicationService;
use crate::mv::application::{MvApplicationService, MvRefreshProviderActivation};
use crate::mv::repository::MvRepository;
#[cfg(test)]
use crate::mv::repository::UnavailableMvRepository;
use crate::sql::catalog::TableLookupMode;
#[cfg(test)]
use crate::sql::catalog::local::PlannerMemoryCatalog;
use novarocks_catalog::identifier::normalize_identifier;
#[cfg(test)]
use novarocks_catalog::memory::DEFAULT_DATABASE;

pub mod add_files_engine;
pub(crate) mod aggregate;
pub(crate) mod backend_resolver;
pub mod ctas_engine;
pub mod delete_engine;
pub(crate) mod iceberg_ctas;
pub(crate) mod iceberg_maintenance;
pub(crate) mod iceberg_ref_flow;
pub(crate) mod information_schema;
pub mod insert_engine;
pub mod mutation_engine;
pub(crate) mod mutation_flow;
pub(crate) mod mv;
pub(crate) mod mv_background;
pub(crate) mod mv_first_refresh_staging;
pub(crate) mod mv_flow;
pub(crate) mod mv_maintenance;
pub(crate) mod mv_rewrite_prep;
pub(crate) mod query_planning;
pub(crate) mod query_prep;
pub(crate) mod query_stats;
pub mod row_mutation;
pub(crate) mod statement;
pub mod statistics;
pub mod statistics_application;
pub mod system_catalog;
pub mod table_maintenance;
pub mod truncate_engine;
pub mod view;
pub(crate) mod virtual_table;
pub(crate) mod write_operation_lifecycle;
mod write_transaction;
use self::statement::{
    execute_create_database_statement, execute_create_table_statement,
    execute_drop_catalog_statement, execute_drop_database_statement, execute_drop_table_statement,
    looks_like_add_equality_delete, looks_like_alter_iceberg_properties,
    looks_like_alter_iceberg_schema, looks_like_alter_partition_column,
    looks_like_show_create_table, parse_alter_iceberg_properties_sql,
    parse_alter_partition_column_sql, parse_show_create_table,
};
use crate::engine::query_prep::{has_time_travel_refs, rewrite_time_travel_refs};
#[cfg(test)]
use crate::sql::literal::{sql_type_to_arrow_type, sqlparser_expr_to_literal};

#[derive(Clone, Debug, Default)]
pub struct StandaloneOptions {
    pub config_path: Option<PathBuf>,
}

use novarocks_catalog::partition::LegacyRangePartition;

pub(crate) fn catalog_service_snapshot(state: &Arc<StandaloneState>) -> QueryCatalogService {
    QueryCatalogService::new(
        Arc::new(RwLock::new(state.catalog_service.local_snapshot())),
        state.catalog_service.registry_snapshot(),
    )
}

pub(crate) fn build_catalog_service_provider<'a>(
    current_catalog: Option<&'a str>,
    catalog_service: &'a QueryCatalogService,
    controls: &'a dyn novarocks_spi::connector::ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    _lookup_mode: TableLookupMode,
) -> crate::engine::query_planning::catalog_materializer::CatalogServiceMaterializer<'a> {
    build_catalog_service_provider_with_query_local_overlays(
        current_catalog,
        catalog_service,
        controls,
        connector_context,
        _lookup_mode,
        Vec::new(),
    )
}

/// Build the application catalog facade for one admitted query, optionally
/// supplying generated relations that are scoped to that request.  These
/// overlays are projected into SQL binding tokens before analysis and never
/// enter the shared local catalog.
pub(crate) fn build_catalog_service_provider_with_query_local_overlays<'a>(
    current_catalog: Option<&'a str>,
    catalog_service: &'a QueryCatalogService,
    controls: &'a dyn novarocks_spi::connector::ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    _lookup_mode: TableLookupMode,
    overlays: Vec<crate::engine::query_planning::catalog_materializer::QueryLocalTableOverlay>,
) -> crate::engine::query_planning::catalog_materializer::CatalogServiceMaterializer<'a> {
    let bindings = Arc::new(
        crate::engine::query_planning::bindings::QueryTableBindingStore::try_new()
            .expect("query table binding scope allocation must not fail"),
    );
    build_catalog_service_provider_with_bindings_and_query_local_overlays(
        current_catalog,
        catalog_service,
        controls,
        connector_context,
        bindings,
        overlays,
    )
}

pub(crate) fn build_catalog_service_provider_with_bindings_and_query_local_overlays<'a>(
    current_catalog: Option<&'a str>,
    catalog_service: &'a QueryCatalogService,
    controls: &'a dyn novarocks_spi::connector::ConnectorControlResolver,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    bindings: Arc<crate::engine::query_planning::bindings::QueryTableBindingStore>,
    overlays: Vec<crate::engine::query_planning::catalog_materializer::QueryLocalTableOverlay>,
) -> crate::engine::query_planning::catalog_materializer::CatalogServiceMaterializer<'a> {
    let loader = query_stats::iceberg_table_binding_loader(controls, connector_context);
    crate::engine::query_planning::catalog_materializer::CatalogServiceMaterializer::new_with_query_local_overlays(
        current_catalog,
        catalog_service,
        bindings,
        loader,
        overlays,
    )
}

#[derive(Clone, Debug)]
pub enum StatementResult {
    Query(QueryResult),
    Ok,
}

/// An opaque, one-shot result of core query compilation.
///
/// The frontend owns submission to `QueryExecutionService`, while the core
/// keeps the native request construction and result formatting capabilities.
/// This prevents a second router from reconstructing query artifacts or
/// interpreting coordinator-specific state.
pub enum PreparedQueryOperation {
    Immediate(PreparedImmediateQuery),
    Distributed(PreparedDistributedQuery),
}

pub struct PreparedImmediateQuery {
    result: StatementResult,
}

impl PreparedImmediateQuery {
    pub fn into_result(self) -> StatementResult {
        self.result
    }
}

pub struct PreparedDistributedQuery {
    request: crate::query_execution::contract::DistributedQueryRequest,
    completion: PreparedQueryCompletion,
}

impl PreparedDistributedQuery {
    pub fn into_parts(
        self,
    ) -> (
        crate::query_execution::contract::DistributedQueryRequest,
        PreparedQueryCompletion,
    ) {
        (self.request, self.completion)
    }
}

/// Core-owned completion formatter paired with a distributed request.
///
/// It is deliberately not constructible outside core: frontend can submit the
/// request exactly once, but cannot substitute a formatter for a different
/// distributed-query intent.
pub struct PreparedQueryCompletion {
    formatter: PreparedQueryFormatter,
}

enum PreparedQueryFormatter {
    Result,
    Profile(PreparedProfileFormatter),
}

struct PreparedProfileFormatter {
    distributed_plan: crate::sql::planner::distributed::DistributedPlan,
    planning_elapsed: std::time::Duration,
    execution_started_at: std::time::Instant,
    connector_static_planning: crate::query_execution::profile::ConnectorStaticPlanningMetrics,
}

impl PreparedQueryCompletion {
    pub fn complete(
        self,
        outcome: crate::query_execution::contract::DistributedQueryOutcome,
    ) -> Result<StatementResult, String> {
        match self.formatter {
            PreparedQueryFormatter::Result => outcome
                .into_result()
                .map(crate::query_execution::outcome::ResultExecutionOutcome::into_query_result)
                .map(StatementResult::Query)
                .map_err(|error| error.to_string()),
            PreparedQueryFormatter::Profile(formatter) => {
                let outcome = outcome
                    .into_profile()
                    .map(crate::query_execution::outcome::ProfileExecutionOutcome::into_parts)
                    .map_err(|error| error.to_string())?;
                let (query_result, fragment_profiles) = outcome;
                let fragment_profiles = fragment_profiles.into_profiles();
                if fragment_profiles.is_empty() {
                    return Err(
                        "EXPLAIN ANALYZE completed without fragment runtime profiles".into(),
                    );
                }
                let actuals = crate::query_execution::profile::collect_actuals_by_plan_node_id_from_profile_trees(
                    &fragment_profiles,
                );
                let profile_summary = crate::query_execution::profile::collect_distributed_profile_summary_from_profile_trees(
                    &fragment_profiles,
                );
                let per_fragment =
                    crate::query_execution::profile::collect_per_fragment_profile_summaries(
                        &fragment_profiles,
                    );
                let mut lines = Vec::new();
                lines.push(format!(
                    "Planning: {} / Execution: {} / Rows: {}",
                    format_explain_analyze_duration(formatter.planning_elapsed),
                    format_explain_analyze_duration(formatter.execution_started_at.elapsed()),
                    query_result.row_count()
                ));
                lines.push(format_distributed_profile_summary(&profile_summary));
                if let Some(apply) = crate::query_execution::profile::collect_native_runtime_filter_apply_from_profile_trees(
                    &fragment_profiles,
                ) {
                    lines.push(apply.to_string());
                }
                if let Some(apply) = crate::query_execution::profile::collect_native_scan_conjunct_apply_from_profile_trees(
                    &fragment_profiles,
                ) {
                    lines.push(apply.to_string());
                }
                if !formatter.connector_static_planning.is_empty() {
                    lines.push(formatter.connector_static_planning.to_string());
                }
                if let Some(counters) =
                    crate::query_execution::profile::format_counter_sums_from_profile_trees(
                        &fragment_profiles,
                        ICEBERG_RUNTIME_FILE_PRUNING_COUNTER_NAMES,
                        "ProfileCounters",
                    )
                {
                    lines.push(counters);
                }
                if let Some(counters) =
                    crate::query_execution::profile::format_counter_sums_from_profile_trees(
                        &fragment_profiles,
                        RUNTIME_FILTER_SCAN_UNIT_COUNTER_NAMES,
                        "RuntimeFilterScanUnits",
                    )
                {
                    lines.push(counters);
                }
                if let Some(counters) =
                    crate::query_execution::profile::format_counter_sums_from_profile_trees(
                        &fragment_profiles,
                        CONNECTOR_FILE_ROW_GROUP_COUNTER_NAMES,
                        "ConnectorFileMetrics",
                    )
                {
                    lines.push(counters);
                }
                lines.extend(
                    crate::sql::explain::distributed::explain_distributed_plan_analyze(
                        &formatter.distributed_plan,
                        crate::sql::explain::ExplainLevel::Analyze,
                        &actuals,
                        Some(&per_fragment),
                    ),
                );
                build_string_query_result("Explain String", lines).map(StatementResult::Query)
            }
        }
    }
}

pub(crate) struct StandaloneState {
    /// Role supplied by the frontend composition root. Maintenance execution
    /// captures it with the same topology snapshot instead of reading config
    /// during a request.
    pub(crate) execution_role: crate::common::app_config::ClusterRole,
    pub(crate) catalog_service: Arc<QueryCatalogService>,
    pub(crate) iceberg_catalogs: Arc<RwLock<IcebergCatalogRegistry>>,
    pub(crate) statistics_service: Arc<dyn statistics::StatisticsService>,
    /// Frontend-owned durable application boundary for typed statistics commands.
    pub(crate) statistics_application: Arc<dyn statistics_application::StatisticsApplicationPort>,
    /// Frontend composition owns logical connector generations. The engine
    /// only consumes this SPI lifecycle port.
    pub(crate) connector_control: Arc<dyn novarocks_spi::connector::ConnectorControlRegistry>,
    /// Frontend-owned provider factory resolver. Core submits durable
    /// attachment facts here and never constructs a concrete generation.
    pub(crate) connector_control_factory_resolver:
        Arc<dyn novarocks_spi::connector::ConnectorControlFactoryResolver>,
    /// Process-local filesystem resources supplied by the composition root.
    /// During the Phase 1 checkpoint the legacy Core Iceberg control owner
    /// consumes them without discovering a runtime or credentials globally.
    pub(crate) connector_file_planning_resources: Option<novarocks_fs::FsAccessResources>,
    /// Process-local cache of immutable evidence returned by connector
    /// statistics readers. Query compilation still consumes only the pin
    /// captured during table resolution.
    pub(crate) unified_statistics:
        Arc<crate::connector::unified_statistics::UnifiedStatisticsResolver>,
    pub(crate) connectors: Arc<RwLock<crate::connector::ConnectorRegistry>>,
    pub(crate) mv_refresh_pruning_limits: MvRefreshPruningLimits,
    /// `[standalone_server] mv_partition_state_max_entries`, frozen at engine
    /// open so MV refresh does not reach for a process-global config.
    pub(crate) mv_partition_state_max_entries: usize,
    pub(crate) metadata_provider: Option<Arc<dyn crate::meta::MetaStoreProvider>>,
    /// Provider-neutral MV metadata boundary. Production wiring is installed by
    /// the frontend host; the core default deliberately rejects MV operations.
    pub(crate) mv_repository: Arc<dyn MvRepository>,
    /// Frontend-owned MV statement application boundary.
    pub(crate) mv_application_service: Arc<dyn MvApplicationService>,
    /// Server-composed exact-generation storage observation boundary.
    pub(crate) mv_storage_observation:
        Arc<dyn crate::mv::storage_observation::MvStorageObservationPort>,
    pub(crate) catalog_attachment_repo: CatalogAttachmentRepository,
    /// Serializes durable attachment and exact control-generation lifecycle
    /// transitions. Provider construction is intentionally performed outside
    /// this lock; only persistence, publication, retirement, and SQL catalog
    /// projection transitions are fenced here.
    pub(crate) catalog_attachment_lifecycle: Mutex<()>,
    pub(crate) iceberg_operation_repo: IcebergOperationRepository,
    pub(crate) job_repo: JobMetaRepository,
    pub(crate) exchange_port: u16,
    /// Frontend-owned view application service, injected at engine open.
    pub(crate) view_service: std::sync::Arc<dyn crate::engine::view::ViewService>,
    /// Frontend-owned table-maintenance application service, injected at engine open.
    pub(crate) table_maintenance_service:
        std::sync::Arc<dyn crate::engine::table_maintenance::TableMaintenanceService>,
    /// Instance-local weak handle used by the borrowed maintenance engine port
    /// to enter connector execution paths that require the shared state Arc.
    pub(crate) self_weak: Weak<StandaloneState>,
    /// Frontend-owned system catalog (information_schema). Injected at open;
    /// see `engine::system_catalog`.
    pub(crate) system_catalog: std::sync::Arc<dyn system_catalog::SystemCatalog>,
    /// Frontend-owned distributed query execution service. Every distributed
    /// engine path must cross this injected value; core never locates or
    /// constructs a coordinator concrete.
    pub(crate) query_execution: crate::query_execution::service::QueryExecutionService,
    /// Frontend-owned query activity used by backend lifecycle management.
    pub(crate) backend_query_events:
        std::sync::Arc<dyn crate::query_execution::backend::BackendQueryEventSink>,
    /// Frontend-owned topology and management controller. Core consumes this
    /// explicit port and never locates a process-global registry.
    pub(crate) backend_topology: crate::query_execution::backend::BackendTopologyService,
    pub(crate) coordinator_report_endpoint:
        std::sync::Arc<dyn crate::query_execution::backend::CoordinatorReportEndpointSink>,
    #[cfg(test)]
    pub(crate) _test_guard: Option<TestSerializationGuard>,
}

#[cfg(test)]
fn test_connector_file_planning_resources() -> Option<novarocks_fs::FsAccessResources> {
    let runtime = crate::runtime::global_async_runtime::data_runtime_handle().ok()?;
    Some(novarocks_fs::FsAccessResources::new(
        None,
        novarocks_fs::FsAccessResolver::new(),
        Arc::new(novarocks_fs::TokioFileIoRuntime::new(runtime.clone())),
        Arc::new(novarocks_fs::TokioFileTaskSpawner::new(runtime)),
    ))
}

#[cfg(test)]
impl Default for StandaloneState {
    fn default() -> Self {
        let connector_control = Arc::new(TestConnectorControlRegistry::default());
        Self {
            execution_role: crate::common::app_config::ClusterRole::AllInOne,
            catalog_service: Arc::new(
                crate::engine::query_planning::catalog_runtime::new_query_catalog_service(),
            ),
            iceberg_catalogs: Arc::new(RwLock::new(IcebergCatalogRegistry::default())),
            statistics_service: Arc::new(statistics::EmptyStatisticsService),
            statistics_application: Arc::new(
                statistics_application::UnavailableStatisticsApplicationPort,
            ),
            connector_control: Arc::clone(&connector_control)
                as Arc<dyn novarocks_spi::connector::ConnectorControlRegistry>,
            connector_control_factory_resolver: connector_control
                as Arc<dyn novarocks_spi::connector::ConnectorControlFactoryResolver>,
            connector_file_planning_resources: test_connector_file_planning_resources(),
            unified_statistics: Arc::new(
                crate::connector::unified_statistics::UnifiedStatisticsResolver::default(),
            ),
            connectors: Arc::new(RwLock::new(crate::connector::ConnectorRegistry::default())),
            mv_refresh_pruning_limits: MvRefreshPruningLimits::default(),
            mv_partition_state_max_entries: DEFAULT_MV_PARTITION_STATE_MAX_ENTRIES,
            metadata_provider: None,
            mv_repository: Arc::new(UnavailableMvRepository),
            mv_application_service: Arc::new(UnavailableMvApplicationService),
            mv_storage_observation: Arc::new(
                crate::mv::storage_observation::UnavailableMvStorageObservationPort,
            ),
            catalog_attachment_repo: CatalogAttachmentRepository,
            catalog_attachment_lifecycle: Mutex::new(()),
            iceberg_operation_repo: IcebergOperationRepository,
            job_repo: JobMetaRepository,
            exchange_port: 0,
            view_service: std::sync::Arc::new(crate::engine::view::EmptyViewService),
            table_maintenance_service: std::sync::Arc::new(
                crate::engine::table_maintenance::EmptyTableMaintenanceService,
            ),
            self_weak: Weak::new(),
            system_catalog: std::sync::Arc::new(system_catalog::EmptySystemCatalog),
            query_execution: test_query_execution_service(),
            backend_query_events: std::sync::Arc::new(
                crate::query_execution::backend::NoopBackendQueryEventSink,
            ),
            backend_topology: std::sync::Arc::new(
                crate::query_execution::backend::NoopBackendTopologyPort,
            ),
            coordinator_report_endpoint: std::sync::Arc::new(
                crate::query_execution::backend::NoopCoordinatorReportEndpointSink,
            ),
            #[cfg(test)]
            _test_guard: None,
        }
    }
}

#[cfg(test)]
struct TestConnectorControlRegistry {
    active: std::sync::Mutex<
        std::collections::HashMap<
            novarocks_spi::connector::ConnectorInstanceId,
            Arc<novarocks_spi::connector::ConnectorControlBinding>,
        >,
    >,
    factories: std::collections::HashMap<
        novarocks_spi::connector::ConnectorProviderId,
        Arc<dyn novarocks_spi::connector::ConnectorControlFactory>,
    >,
}

#[cfg(test)]
impl Default for TestConnectorControlRegistry {
    fn default() -> Self {
        let runtime = crate::runtime::global_async_runtime::data_runtime_handle()
            .expect("test connector control runtime");
        let resources = test_connector_file_planning_resources()
            .expect("test connector file planning resources");
        let factory = novarocks_connector_iceberg::control_factory::IcebergControlFactory::new(
            novarocks_connector_iceberg::resources::IcebergControlResources::new(
                novarocks_connector_iceberg::access_binding::IcebergReadBinding::from_resources(
                    resources,
                ),
                runtime,
            ),
        );
        let provider_id = factory.provider_id().clone();
        Self {
            active: std::sync::Mutex::new(std::collections::HashMap::new()),
            factories: std::collections::HashMap::from([(
                provider_id,
                Arc::new(factory) as Arc<dyn novarocks_spi::connector::ConnectorControlFactory>,
            )]),
        }
    }
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorControlResolver for TestConnectorControlRegistry {
    fn observe_current_binding(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorExecutionBindingKey,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` is not active",
                        instance_id.as_str()
                    ),
                )
            })?;
        Ok(novarocks_spi::connector::ConnectorExecutionBindingKey {
            instance_id: binding.descriptor().instance_id.clone(),
            incarnation: binding.incarnation(),
        })
    }

    fn acquire_current(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorControlPlanningLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` is not active",
                        instance_id.as_str()
                    ),
                )
            })?;
        Ok(novarocks_spi::connector::ConnectorControlPlanningLease::new(binding, || {}))
    }
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorCatalogMutationResolver for TestConnectorControlRegistry {
    fn acquire_current_mutation(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorCatalogMutationLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` has no active mutation binding",
                        instance_id.as_str()
                    ),
                )
            })?;
        let mutation = binding.mutation().cloned().ok_or_else(|| {
            novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unsupported,
                "test connector control binding has no mutation capability",
            )
        })?;
        novarocks_spi::connector::ConnectorCatalogMutationLease::new(
            binding.descriptor().clone(),
            binding.incarnation(),
            mutation,
            || {},
        )
    }
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorDataMutationResolver for TestConnectorControlRegistry {
    fn acquire_current_data_mutation(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorDataMutationLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` has no active data mutation binding",
                        instance_id.as_str()
                    ),
                )
            })?;
        test_data_mutation_lease(binding)
    }

    fn acquire_exact_data_mutation(
        &self,
        key: &novarocks_spi::connector::ConnectorExecutionBindingKey,
    ) -> Result<
        novarocks_spi::connector::ConnectorDataMutationLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(&key.instance_id)
            .filter(|binding| binding.incarnation() == key.incarnation)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    "exact connector data mutation generation is unavailable",
                )
            })?;
        test_data_mutation_lease(binding)
    }
}

#[cfg(test)]
fn test_data_mutation_lease(
    binding: Arc<novarocks_spi::connector::ConnectorControlBinding>,
) -> Result<
    novarocks_spi::connector::ConnectorDataMutationLease,
    novarocks_spi::connector::ConnectorError,
> {
    let mutation = binding.data_mutation().cloned().ok_or_else(|| {
        novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "test connector control binding has no data mutation capability",
        )
    })?;
    let key = novarocks_spi::connector::ConnectorExecutionBindingKey {
        instance_id: binding.descriptor().instance_id.clone(),
        incarnation: binding.incarnation(),
    };
    novarocks_spi::connector::ConnectorDataMutationLease::new(
        binding.descriptor().clone(),
        key,
        Arc::clone(binding.metadata()),
        mutation,
        || {},
    )
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorMetadataMaintenanceResolver
    for TestConnectorControlRegistry
{
    fn acquire_current_metadata_maintenance(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorMetadataMaintenanceLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` has no active metadata maintenance binding",
                        instance_id.as_str()
                    ),
                )
            })?;
        test_metadata_maintenance_lease(binding)
    }

    fn acquire_exact_metadata_maintenance(
        &self,
        key: &novarocks_spi::connector::ConnectorExecutionBindingKey,
    ) -> Result<
        novarocks_spi::connector::ConnectorMetadataMaintenanceLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(&key.instance_id)
            .filter(|binding| binding.incarnation() == key.incarnation)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    "exact connector metadata maintenance generation is unavailable",
                )
            })?;
        test_metadata_maintenance_lease(binding)
    }
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorCleanupMaintenanceResolver
    for TestConnectorControlRegistry
{
    fn acquire_current_cleanup_maintenance(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorCleanupMaintenanceLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    "test connector cleanup binding is not active",
                )
            })?;
        test_cleanup_maintenance_lease(binding)
    }

    fn acquire_exact_cleanup_maintenance(
        &self,
        key: &novarocks_spi::connector::ConnectorExecutionBindingKey,
    ) -> Result<
        novarocks_spi::connector::ConnectorCleanupMaintenanceLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(&key.instance_id)
            .filter(|binding| binding.incarnation() == key.incarnation)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    "exact test connector cleanup generation is unavailable",
                )
            })?;
        test_cleanup_maintenance_lease(binding)
    }
}

#[cfg(test)]
fn test_cleanup_maintenance_lease(
    binding: Arc<novarocks_spi::connector::ConnectorControlBinding>,
) -> Result<
    novarocks_spi::connector::ConnectorCleanupMaintenanceLease,
    novarocks_spi::connector::ConnectorError,
> {
    let cleanup = binding.cleanup_maintenance().cloned().ok_or_else(|| {
        novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "test connector control binding has no cleanup maintenance capability",
        )
    })?;
    let key = novarocks_spi::connector::ConnectorExecutionBindingKey {
        instance_id: binding.descriptor().instance_id.clone(),
        incarnation: binding.incarnation(),
    };
    novarocks_spi::connector::ConnectorCleanupMaintenanceLease::new(
        binding.descriptor().clone(),
        key,
        Arc::clone(binding.metadata()),
        cleanup,
        || {},
    )
}

#[cfg(test)]
fn test_metadata_maintenance_lease(
    binding: Arc<novarocks_spi::connector::ConnectorControlBinding>,
) -> Result<
    novarocks_spi::connector::ConnectorMetadataMaintenanceLease,
    novarocks_spi::connector::ConnectorError,
> {
    let maintenance = binding.metadata_maintenance().cloned().ok_or_else(|| {
        novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "test connector control binding has no metadata maintenance capability",
        )
    })?;
    let key = novarocks_spi::connector::ConnectorExecutionBindingKey {
        instance_id: binding.descriptor().instance_id.clone(),
        incarnation: binding.incarnation(),
    };
    novarocks_spi::connector::ConnectorMetadataMaintenanceLease::new(
        binding.descriptor().clone(),
        key,
        Arc::clone(binding.metadata()),
        maintenance,
        || {},
    )
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorDistributedRewriteResolver
    for TestConnectorControlRegistry
{
    fn acquire_current_distributed_rewrite(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorDistributedRewriteLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` has no active distributed rewrite binding",
                        instance_id.as_str()
                    ),
                )
            })?;
        test_distributed_rewrite_lease(binding)
    }

    fn acquire_exact_distributed_rewrite(
        &self,
        key: &novarocks_spi::connector::ConnectorExecutionBindingKey,
    ) -> Result<
        novarocks_spi::connector::ConnectorDistributedRewriteLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(&key.instance_id)
            .filter(|binding| binding.incarnation() == key.incarnation)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    "exact connector distributed rewrite generation is unavailable",
                )
            })?;
        test_distributed_rewrite_lease(binding)
    }
}

#[cfg(test)]
fn test_distributed_rewrite_lease(
    binding: Arc<novarocks_spi::connector::ConnectorControlBinding>,
) -> Result<
    novarocks_spi::connector::ConnectorDistributedRewriteLease,
    novarocks_spi::connector::ConnectorError,
> {
    let rewrite = binding.distributed_rewrite().cloned().ok_or_else(|| {
        novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "test connector control binding has no distributed rewrite capability",
        )
    })?;
    let write = binding.write().cloned().ok_or_else(|| {
        novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "test connector control binding has no distributed write capability",
        )
    })?;
    let key = novarocks_spi::connector::ConnectorExecutionBindingKey {
        instance_id: binding.descriptor().instance_id.clone(),
        incarnation: binding.incarnation(),
    };
    novarocks_spi::connector::ConnectorDistributedRewriteLease::new(
        binding.descriptor().clone(),
        key,
        novarocks_spi::connector::ConnectorControlPlanningLease::new(binding.clone(), || {}),
        Arc::clone(binding.metadata()),
        Arc::clone(binding.planning()),
        rewrite,
        write,
        binding.execution_distribution().clone(),
        || {},
    )
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorStatisticsResolver for TestConnectorControlRegistry {
    fn acquire_current_statistics(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorStatisticsLease,
        novarocks_spi::connector::ConnectorError,
    > {
        let binding = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .get(instance_id)
            .cloned()
            .ok_or_else(|| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::NotFound,
                    format!(
                        "connector control instance `{}` has no active statistics binding",
                        instance_id.as_str()
                    ),
                )
            })?;
        let statistics = binding.statistics().cloned().ok_or_else(|| {
            novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unsupported,
                "test connector control binding has no statistics capability",
            )
        })?;
        novarocks_spi::connector::ConnectorStatisticsLease::new(
            binding.descriptor().clone(),
            binding.incarnation(),
            statistics,
            || {},
        )
    }
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorControlRegistry for TestConnectorControlRegistry {
    fn register(
        &self,
        binding: novarocks_spi::connector::ConnectorControlBinding,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        let instance_id = binding.descriptor().instance_id.clone();
        let incarnation = binding.incarnation();
        let mut active = self.active.lock().map_err(|_| {
            novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Internal,
                "test connector control registry lock poisoned",
            )
        })?;
        if let Some(existing) = active.get(&instance_id) {
            if existing.incarnation() == incarnation {
                return Ok(());
            }
            return Err(novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                format!(
                    "connector control instance `{}` already has an active generation",
                    instance_id.as_str()
                ),
            ));
        }
        active.insert(instance_id, Arc::new(binding));
        Ok(())
    }

    fn retire_current(
        &self,
        instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<(), novarocks_spi::connector::ConnectorError> {
        let removed = self
            .active
            .lock()
            .map_err(|_| {
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Internal,
                    "test connector control registry lock poisoned",
                )
            })?
            .remove(instance_id);
        removed.map(|_| ()).ok_or_else(|| {
            novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::NotFound,
                format!(
                    "connector control instance `{}` is not active",
                    instance_id.as_str()
                ),
            )
        })
    }
}

#[cfg(test)]
impl novarocks_spi::connector::ConnectorControlFactoryResolver for TestConnectorControlRegistry {
    fn create_control(
        &self,
        request: novarocks_spi::connector::ConnectorControlFactoryRequest,
    ) -> Result<
        novarocks_spi::connector::ConnectorControlCreation,
        novarocks_spi::connector::ConnectorError,
    > {
        let factory = self.factories.get(request.provider_id()).ok_or_else(|| {
            novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::NotFound,
                "test connector control factory is not installed",
            )
        })?;
        factory.create_control(request)
    }
}

#[cfg(test)]
struct TestDistributedQueryCoordinator {
    connector_control:
        Option<std::sync::Arc<dyn novarocks_spi::connector::ConnectorControlRegistry>>,
}

#[cfg(test)]
impl crate::query_execution::contract::DistributedQueryCoordinator
    for TestDistributedQueryCoordinator
{
    fn begin_write_operation(
        &self,
        registration: crate::query_execution::contract::ConnectorWriteOperationRegistration,
        lease: novarocks_spi::connector::ConnectorWriteLease,
    ) -> Result<
        crate::query_execution::write_operation::ConnectorWriteOperationSession,
        crate::query_execution::contract::DistributedQueryError,
    > {
        crate::query_execution::write_operation::ConnectorWriteOperationSession::try_begin(
            registration,
            lease,
        )
        .map_err(|error| {
            crate::query_execution::contract::DistributedQueryError::new(
                crate::query_execution::contract::DistributedQueryErrorKind::Failed,
                format!("seal test connector write operation cohorts: {error}"),
            )
        })
    }

    fn execute(
        &self,
        request: crate::query_execution::contract::DistributedQueryRequest,
    ) -> Result<
        crate::query_execution::contract::DistributedQueryOutcome,
        crate::query_execution::contract::DistributedQueryError,
    > {
        if request.intent() == crate::query_execution::contract::DistributedQueryIntent::Statistics
        {
            return Err(
                crate::query_execution::contract::DistributedQueryError::new(
                    crate::query_execution::contract::DistributedQueryErrorKind::Rejected,
                    "test query coordinator does not provide a statistics collection sink",
                ),
            );
        }
        crate::query_execution::in_process_test::execute(request)
    }
}

#[cfg(test)]
fn test_query_execution_service() -> crate::query_execution::service::QueryExecutionService {
    test_query_execution_service_with_connector_control(None)
}

#[cfg(test)]
pub(crate) fn test_query_execution_service_with_connector_control(
    connector_control: Option<
        std::sync::Arc<dyn novarocks_spi::connector::ConnectorControlRegistry>,
    >,
) -> crate::query_execution::service::QueryExecutionService {
    crate::query_execution::service::QueryExecutionService::new(std::sync::Arc::new(
        TestDistributedQueryCoordinator { connector_control },
    ))
}

#[cfg(test)]
pub(crate) struct TestSerializationGuard {
    _guard: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
unsafe impl Send for TestSerializationGuard {}

#[cfg(test)]
unsafe impl Sync for TestSerializationGuard {}

#[cfg(test)]
pub(crate) fn acquire_standalone_test_guard() -> TestSerializationGuard {
    use std::sync::{Mutex, OnceLock};
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let guard = LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    TestSerializationGuard { _guard: guard }
}

#[cfg(test)]
pub(crate) fn test_mv_repository() -> Arc<dyn MvRepository> {
    Arc::new(crate::mv::test_repository::InMemoryMvRepository::default())
}

#[derive(Clone)]
pub struct StandaloneNovaRocks {
    inner: Arc<StandaloneState>,
}

#[derive(Clone)]
pub struct StandaloneSession {
    inner: Arc<StandaloneState>,
}

/// Narrow core compiler kernel consumed by frontend QueryService.
///
/// It deliberately exposes neither `StandaloneState` nor connector internals.
/// Design: ADR-0012 (docs/adr/ADR-0012-frontend-query-session-router.md)
#[derive(Clone)]
pub struct StandaloneQueryCompiler {
    session: StandaloneSession,
}

impl StandaloneQueryCompiler {
    pub fn prepare(
        &self,
        sql: &str,
        context: &crate::query_execution::request_context::RequestContext,
        query_opts: Option<QueryOptions>,
    ) -> Result<PreparedQueryOperation, String> {
        self.session
            .prepare_query_with_context(sql, context, query_opts)
    }
}

/// Core command kernel for statement families whose application owner has not
/// moved in this cutover (notably DML and MV maintenance).
#[derive(Clone)]
pub struct StandaloneCommandExecutor {
    session: StandaloneSession,
}

impl StandaloneCommandExecutor {
    pub fn execute(
        &self,
        sql: &str,
        context: &crate::query_execution::request_context::RequestContext,
        query_opts: Option<QueryOptions>,
    ) -> Result<StatementResult, String> {
        self.session
            .execute_command_with_context(sql, context, query_opts)
    }
}

/// Explicit application services required to open the core SQL engine.
///
/// The value has no default: the owning frontend must construct every
/// application-level service, including distributed query execution.
pub struct StandaloneOpenServices {
    /// Deployment role validated by the frontend composition root. Request
    /// execution must not rediscover it from global process configuration.
    pub execution_role: crate::common::app_config::ClusterRole,
    pub system_catalog: std::sync::Arc<dyn system_catalog::SystemCatalog>,
    pub view_service: std::sync::Arc<dyn crate::engine::view::ViewService>,
    pub statistics_service: std::sync::Arc<dyn statistics::StatisticsService>,
    /// Frontend-owned durable application boundary. `new` defaults to an
    /// unavailable implementation so non-frontend compositions fail closed.
    pub statistics_application:
        std::sync::Arc<dyn statistics_application::StatisticsApplicationPort>,
    /// Receives the Core-owned target resolver once connector control is
    /// ready. It is intentionally distinct from command dispatch.
    pub statistics_target_resolver_sink:
        Option<std::sync::Arc<dyn statistics_application::StatisticsTargetResolverSink>>,
    /// Receives Core's generation-fenced read-only statistics reader after
    /// connector control is ready. It does not imply durable job ownership.
    pub statistics_table_reader_sink:
        Option<std::sync::Arc<dyn statistics_application::StatisticsTableReaderSink>>,
    /// Receives the Core-owned distributed collection/publish executor after
    /// connector control and the coordinator are ready. The frontend starts a
    /// durable worker only when it also owns a StateStore repository.
    pub statistics_attempt_executor_sink:
        Option<std::sync::Arc<dyn statistics_application::StatisticsAttemptExecutorSink>>,
    /// Receives the Core-owned provider activation adapter after the engine
    /// has its connector registry. The frontend remains the owner of every
    /// durable and external refresh transition.
    pub mv_refresh_provider_activation_sink:
        Option<std::sync::Arc<dyn crate::mv::application::MvRefreshProviderActivationSink>>,
    /// Receives the provider-neutral MV background adapter after restore and
    /// table-maintenance recovery. The frontend owns all worker lifecycle.
    pub mv_background_engine_sink:
        Option<std::sync::Arc<dyn crate::mv::background::MvBackgroundEngineSink>>,
    pub table_maintenance_service:
        std::sync::Arc<dyn crate::engine::table_maintenance::TableMaintenanceService>,
    pub mv_repository: std::sync::Arc<dyn MvRepository>,
    pub mv_application_service: std::sync::Arc<dyn MvApplicationService>,
    /// Server-composed provider storage inspector adapter. The default is
    /// intentionally unavailable so missing composition fails closed.
    pub mv_storage_observation:
        std::sync::Arc<dyn crate::mv::storage_observation::MvStorageObservationPort>,
    pub query_execution: crate::query_execution::service::QueryExecutionService,
    pub backend_query_events:
        std::sync::Arc<dyn crate::query_execution::backend::BackendQueryEventSink>,
    pub backend_topology: crate::query_execution::backend::BackendTopologyService,
    pub coordinator_report_endpoint:
        std::sync::Arc<dyn crate::query_execution::backend::CoordinatorReportEndpointSink>,
    pub query_control: crate::query_execution::control::QueryControlService,
    /// Frontend-owned lifecycle port for logical connector control bindings.
    pub connector_control: std::sync::Arc<dyn novarocks_spi::connector::ConnectorControlRegistry>,
    /// Frontend-owned provider factory resolver used by catalog create and
    /// durable attachment restore.
    pub connector_control_factory_resolver:
        std::sync::Arc<dyn novarocks_spi::connector::ConnectorControlFactoryResolver>,
    /// Connector-neutral FE filesystem resources. The legacy Core Iceberg
    /// control owner consumes this only until the follow-up factory cut.
    pub connector_file_planning_resources: Option<novarocks_fs::FsAccessResources>,
    /// Bound by the server composition root before engine open. Zero means no
    /// local fragment endpoint is available to this engine instance.
    pub exchange_port: u16,
}

impl StandaloneOpenServices {
    pub fn new(
        execution_role: crate::common::app_config::ClusterRole,
        system_catalog: std::sync::Arc<dyn system_catalog::SystemCatalog>,
        view_service: std::sync::Arc<dyn crate::engine::view::ViewService>,
        statistics_service: std::sync::Arc<dyn statistics::StatisticsService>,
        table_maintenance_service: std::sync::Arc<
            dyn crate::engine::table_maintenance::TableMaintenanceService,
        >,
        mv_repository: std::sync::Arc<dyn MvRepository>,
        mv_application_service: std::sync::Arc<dyn MvApplicationService>,
        query_execution: crate::query_execution::service::QueryExecutionService,
        backend_query_events: std::sync::Arc<
            dyn crate::query_execution::backend::BackendQueryEventSink,
        >,
        backend_topology: crate::query_execution::backend::BackendTopologyService,
        coordinator_report_endpoint: std::sync::Arc<
            dyn crate::query_execution::backend::CoordinatorReportEndpointSink,
        >,
        query_control: crate::query_execution::control::QueryControlService,
        connector_control: std::sync::Arc<dyn novarocks_spi::connector::ConnectorControlRegistry>,
        connector_control_factory_resolver: std::sync::Arc<
            dyn novarocks_spi::connector::ConnectorControlFactoryResolver,
        >,
        exchange_port: u16,
    ) -> Self {
        Self {
            execution_role,
            system_catalog,
            view_service,
            statistics_service,
            statistics_application: std::sync::Arc::new(
                statistics_application::UnavailableStatisticsApplicationPort,
            ),
            statistics_target_resolver_sink: None,
            statistics_table_reader_sink: None,
            statistics_attempt_executor_sink: None,
            mv_refresh_provider_activation_sink: None,
            mv_background_engine_sink: None,
            connector_control,
            connector_control_factory_resolver,
            connector_file_planning_resources: {
                #[cfg(test)]
                {
                    test_connector_file_planning_resources()
                }
                #[cfg(not(test))]
                {
                    None
                }
            },
            table_maintenance_service,
            mv_repository,
            mv_application_service,
            mv_storage_observation: std::sync::Arc::new(
                crate::mv::storage_observation::UnavailableMvStorageObservationPort,
            ),
            query_execution,
            backend_query_events,
            backend_topology,
            coordinator_report_endpoint,
            query_control,
            exchange_port,
        }
    }

    pub fn with_mv_storage_observation(
        mut self,
        observation: std::sync::Arc<dyn crate::mv::storage_observation::MvStorageObservationPort>,
    ) -> Self {
        self.mv_storage_observation = observation;
        self
    }

    pub fn with_connector_file_planning_resources(
        mut self,
        resources: Option<novarocks_fs::FsAccessResources>,
    ) -> Self {
        self.connector_file_planning_resources = resources;
        self
    }

    pub fn with_statistics_application(
        mut self,
        statistics_application: std::sync::Arc<
            dyn statistics_application::StatisticsApplicationPort,
        >,
    ) -> Self {
        self.statistics_application = statistics_application;
        self
    }

    pub fn with_statistics_target_resolver_sink(
        mut self,
        sink: std::sync::Arc<dyn statistics_application::StatisticsTargetResolverSink>,
    ) -> Self {
        self.statistics_target_resolver_sink = Some(sink);
        self
    }

    pub fn with_statistics_table_reader_sink(
        mut self,
        sink: std::sync::Arc<dyn statistics_application::StatisticsTableReaderSink>,
    ) -> Self {
        self.statistics_table_reader_sink = Some(sink);
        self
    }

    pub fn with_statistics_attempt_executor_sink(
        mut self,
        sink: std::sync::Arc<dyn statistics_application::StatisticsAttemptExecutorSink>,
    ) -> Self {
        self.statistics_attempt_executor_sink = Some(sink);
        self
    }

    pub fn with_mv_refresh_provider_activation_sink(
        mut self,
        sink: Option<std::sync::Arc<dyn crate::mv::application::MvRefreshProviderActivationSink>>,
    ) -> Self {
        self.mv_refresh_provider_activation_sink = sink;
        self
    }

    pub fn with_mv_background_engine_sink(
        mut self,
        sink: Option<std::sync::Arc<dyn crate::mv::background::MvBackgroundEngineSink>>,
    ) -> Self {
        self.mv_background_engine_sink = sink;
        self
    }
}

impl StandaloneNovaRocks {
    pub fn mv_refresh_provider_activation(&self) -> Arc<dyn MvRefreshProviderActivation> {
        Arc::new(
            crate::engine::mv::iceberg_activation::StandaloneMvRefreshProviderActivation::new(
                Arc::downgrade(&self.inner),
            ),
        )
    }

    pub fn open(opts: StandaloneOptions, services: StandaloneOpenServices) -> Result<Self, String> {
        #[cfg(test)]
        let _test_guard = Some(acquire_standalone_test_guard());
        let cfg = match opts.config_path.as_deref() {
            Some(path) => novarocks_config::load_from_path(path)
                .map_err(|e| format!("load config failed: {e}"))?,
            None => {
                #[cfg(test)]
                {
                    novarocks_config::NovaRocksConfig::default()
                }
                #[cfg(not(test))]
                {
                    novarocks_config::load_from_env_or_default()
                        .map_err(|e| format!("load config failed: {e}"))?
                }
            }
        };
        #[cfg(test)]
        return Self::open_body(opts, &cfg, services, _test_guard);
        #[cfg(not(test))]
        Self::open_body(opts, &cfg, services)
    }

    /// Open the engine using an already-loaded, validated config.
    ///
    /// Installs `cfg` as the process-wide active config (replacing any prior
    /// global config) and then proceeds with the normal engine-open body.
    /// `opts.config_path` is preserved for resolving relative paths (e.g.
    /// SQLite metadata DB paths) but is **not** re-read from disk.
    pub fn open_with_config(
        opts: StandaloneOptions,
        cfg: novarocks_config::NovaRocksConfig,
        services: StandaloneOpenServices,
    ) -> Result<Self, String> {
        #[cfg(test)]
        let _test_guard = Some(acquire_standalone_test_guard());
        #[cfg(test)]
        return Self::open_body(opts, &cfg, services, _test_guard);
        #[cfg(not(test))]
        Self::open_body(opts, &cfg, services)
    }

    /// Common engine-open body.  Called after the process-wide config has
    /// already been installed by the caller.
    fn open_body(
        opts: StandaloneOptions,
        cfg: &novarocks_config::NovaRocksConfig,
        services: StandaloneOpenServices,
        #[cfg(test)] _test_guard: Option<TestSerializationGuard>,
    ) -> Result<Self, String> {
        let metadata_backend = resolve_metadata_backend(&opts, cfg)?;
        let metadata_provider = metadata_backend
            .as_ref()
            .map(open_metadata_provider)
            .transpose()?;
        let mv_refresh_pruning_limits = resolve_mv_refresh_pruning_limits(cfg);
        let mv_partition_state_max_entries = cfg
            .standalone_server
            .as_ref()
            .map(|standalone| standalone.mv_partition_state_max_entries)
            .unwrap_or(DEFAULT_MV_PARTITION_STATE_MAX_ENTRIES);
        let StandaloneOpenServices {
            execution_role,
            system_catalog,
            view_service,
            statistics_service,
            statistics_application,
            statistics_target_resolver_sink,
            statistics_table_reader_sink,
            statistics_attempt_executor_sink,
            mv_refresh_provider_activation_sink,
            mv_background_engine_sink,
            connector_control,
            connector_control_factory_resolver,
            connector_file_planning_resources,
            table_maintenance_service,
            mv_repository,
            mv_application_service,
            mv_storage_observation,
            query_execution,
            backend_query_events,
            backend_topology,
            coordinator_report_endpoint,
            query_control: _,
            exchange_port,
        } = services;
        let inner = Arc::new_cyclic(|self_weak| StandaloneState {
            execution_role,
            catalog_service: Arc::new(
                crate::engine::query_planning::catalog_runtime::new_query_catalog_service(),
            ),
            mv_refresh_pruning_limits,
            mv_partition_state_max_entries,
            metadata_provider: metadata_provider.clone(),
            mv_repository,
            mv_application_service,
            mv_storage_observation,
            catalog_attachment_repo: CatalogAttachmentRepository,
            catalog_attachment_lifecycle: Mutex::new(()),
            job_repo: JobMetaRepository,
            exchange_port,
            system_catalog,
            view_service,
            statistics_service,
            statistics_application,
            connector_control,
            connector_control_factory_resolver,
            connector_file_planning_resources,
            unified_statistics: Arc::new(
                crate::connector::unified_statistics::UnifiedStatisticsResolver::default(),
            ),
            table_maintenance_service,
            self_weak: self_weak.clone(),
            query_execution,
            backend_query_events,
            backend_topology,
            coordinator_report_endpoint,
            #[cfg(test)]
            _test_guard,
            iceberg_catalogs: Arc::new(RwLock::new(IcebergCatalogRegistry::default())),
            connectors: Arc::new(RwLock::new(crate::connector::ConnectorRegistry::default())),
            iceberg_operation_repo: IcebergOperationRepository,
        });
        register_connector_backends(&inner);
        if let Some(sink) = &mv_refresh_provider_activation_sink {
            sink.bind_mv_refresh_provider_activation(Arc::new(
                crate::engine::mv::iceberg_activation::StandaloneMvRefreshProviderActivation::new(
                    Arc::downgrade(&inner),
                ),
            ))?;
        }
        restore_metadata_if_needed(&inner)?;
        let engine = Self { inner };
        if let Some(sink) = statistics_target_resolver_sink {
            sink.bind_statistics_target_resolver(engine.statistics_target_resolver())?;
        }
        if let Some(sink) = statistics_table_reader_sink {
            sink.bind_statistics_table_reader(engine.statistics_table_reader())?;
        }
        if let Some(sink) = statistics_attempt_executor_sink {
            sink.bind_statistics_attempt_executor(engine.statistics_attempt_executor())?;
        }
        let engine_port =
            Arc::clone(&engine.inner) as Arc<dyn table_maintenance::TableMaintenanceEngine>;
        if let Err(error) = engine
            .inner
            .table_maintenance_service
            .start(Arc::clone(&engine_port))
        {
            let primary = format!("start table maintenance service failed: {error}");
            return match engine.inner.table_maintenance_service.shutdown() {
                Ok(()) => Err(primary),
                Err(cleanup_error) => Err(format!("{primary}; cleanup failed: {cleanup_error}")),
            };
        }
        if let Some(sink) = mv_background_engine_sink {
            let bindings = crate::mv::background::MvBackgroundBindings {
                engine: Arc::new(
                    crate::engine::mv_background::StandaloneMvBackgroundEngine::new(Arc::clone(
                        &engine.inner,
                    )),
                ),
                table_maintenance_engine: engine_port,
            };
            if let Err(error) = sink.bind_mv_background_engine(bindings) {
                let primary = format!("bind frontend MV background engine failed: {error}");
                return match engine.inner.table_maintenance_service.shutdown() {
                    Ok(()) => Err(primary),
                    Err(cleanup_error) => {
                        Err(format!("{primary}; cleanup failed: {cleanup_error}"))
                    }
                };
            }
        }
        Ok(engine)
    }

    pub fn session(&self) -> StandaloneSession {
        StandaloneSession {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn query_compiler(&self) -> StandaloneQueryCompiler {
        StandaloneQueryCompiler {
            session: self.session(),
        }
    }

    pub fn command_executor(&self) -> StandaloneCommandExecutor {
        StandaloneCommandExecutor {
            session: self.session(),
        }
    }

    pub fn insert_engine(&self) -> Arc<dyn insert_engine::InsertEngine> {
        Arc::new(Arc::clone(&self.inner))
    }

    pub fn delete_engine(&self) -> Arc<dyn delete_engine::DeleteEngine> {
        Arc::new(Arc::clone(&self.inner))
    }

    /// UPDATE/MERGE's narrow reverse port.  Frontend owns the durable DML
    /// lifecycle; the returned core capability keeps parser-private mutation
    /// planning and exact connector sessions opaque.
    pub fn mutation_engine(&self) -> Arc<dyn mutation_engine::MutationEngine> {
        Arc::new(Arc::clone(&self.inner))
    }

    pub fn ctas_engine(&self) -> Arc<dyn ctas_engine::CtasEngine> {
        Arc::new(Arc::clone(&self.inner))
    }

    pub fn truncate_engine(&self) -> Arc<dyn truncate_engine::TruncateEngine> {
        Arc::new(Arc::clone(&self.inner))
    }

    /// ADD FILES' narrow reverse port. The frontend owns durable operation
    /// lifecycle and source-scope policy; core retains target resolution and
    /// the exact connector data-mutation session.
    pub fn add_files_engine(&self) -> Arc<dyn add_files_engine::AddFilesEngine> {
        Arc::new(Arc::clone(&self.inner))
    }

    /// Resolve an ANALYZE target once through the current connector control
    /// generation. The frontend persists the returned opaque pin before it
    /// creates a durable job; workers never receive this resolver.
    pub fn statistics_target_resolver(
        &self,
    ) -> Arc<dyn statistics_application::StatisticsTargetResolver> {
        Arc::new(
            statistics_application::ConnectorStatisticsTargetResolver::new(Arc::clone(
                &self.inner.connector_control,
            )),
        )
    }

    /// Read-only generation-fenced statistics reader. It does not require a
    /// durable job repository and remains valid without StateStore.
    pub fn statistics_table_reader(
        &self,
    ) -> Arc<dyn statistics_application::StatisticsTableReader> {
        Arc::new(statistics_application::ConnectorStatisticsTableReader::new(
            Arc::clone(&self.inner.connector_control),
        ))
    }

    /// Native connector statistics execution exposed only to the frontend
    /// durable job worker. It shares this engine's connector registry,
    /// coordinator and live backend topology; no standalone/local fallback is
    /// constructed for ANALYZE.
    pub fn statistics_attempt_executor(
        &self,
    ) -> Arc<dyn statistics_application::StatisticsAttemptExecutor> {
        Arc::new(
            statistics_application::ConnectorStatisticsAttemptExecutor::new(Arc::downgrade(
                &self.inner,
            )),
        )
    }

    pub(crate) fn publish_coordinator_report_bound_port(&self, port: u16) {
        self.inner.coordinator_report_endpoint.set_bound_port(port);
    }

    #[cfg(test)]
    pub(crate) fn state_for_test(&self) -> Arc<StandaloneState> {
        Arc::clone(&self.inner)
    }

    pub fn database_exists(&self, database_name: &str) -> Result<bool, String> {
        let guard = self
            .inner
            .catalog_service
            .local()
            .read()
            .expect("standalone catalog read lock");
        guard.database_exists(database_name)
    }

    pub fn iceberg_catalog_exists(&self, catalog_name: &str) -> Result<bool, String> {
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(catalog_name)
            .map_err(|error| format!("invalid Iceberg connector instance ID: {error}"))?;
        match self
            .inner
            .connector_control
            .observe_current_binding(&instance_id)
        {
            Ok(_) => Ok(true),
            Err(error)
                if error.kind() == novarocks_spi::connector::ConnectorErrorKind::NotFound =>
            {
                Ok(false)
            }
            Err(error) => Err(format!("resolve Iceberg catalog `{catalog_name}`: {error}")),
        }
    }

    pub fn iceberg_namespace_exists(
        &self,
        catalog_name: &str,
        namespace_name: &str,
    ) -> Result<bool, String> {
        let context = crate::connector::connector_request_context(
            None,
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )?;
        crate::connector::metadata_namespace_exists(
            self.inner.connector_control.as_ref(),
            context,
            catalog_name,
            namespace_name,
        )
    }

    pub(crate) fn has_local_table(&self, database_name: &str, table_name: &str) -> bool {
        let Ok(database_name) = normalize_identifier(database_name) else {
            return false;
        };
        let Ok(table_name) = normalize_identifier(table_name) else {
            return false;
        };
        let guard = self
            .inner
            .catalog_service
            .local()
            .read()
            .expect("standalone catalog read lock");
        guard.get(&database_name, &table_name).is_ok()
    }
}

fn register_connector_backends(state: &Arc<StandaloneState>) {
    crate::connector::register_standalone_backends(state);
}

impl StandaloneSession {
    #[cfg(test)]
    pub fn execute(&self, sql: &str) -> Result<(), String> {
        match self.execute_in_context(sql, None, DEFAULT_DATABASE, None)? {
            StatementResult::Ok => Ok(()),
            StatementResult::Query(_) => Err("statement returned rows".to_string()),
        }
    }

    #[cfg(test)]
    pub fn query(&self, sql: &str) -> Result<QueryResult, String> {
        match self.execute_in_context(sql, None, DEFAULT_DATABASE, None)? {
            StatementResult::Query(result) => Ok(result),
            StatementResult::Ok => Err("statement did not return rows".to_string()),
        }
    }

    #[cfg(test)]
    pub(crate) fn execute_in_database(
        &self,
        sql: &str,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        self.execute_in_context(sql, None, current_database, None)
    }

    /// Legacy test seam retained only while core parser fixtures migrate to
    /// frontend QuerySession fixtures. Production callers use the compiler or
    /// command kernels above.
    #[cfg(test)]
    pub fn execute_with_context(
        &self,
        sql: &str,
        context: &crate::query_execution::request_context::RequestContext,
        query_opts: Option<QueryOptions>,
    ) -> Result<StatementResult, String> {
        let connector_context = crate::connector::connector_request_context_for_query(
            query_opts.as_ref(),
            context.execution().cancellation().clone(),
        )?;
        self.execute_in_context_inner(sql, context, query_opts, connector_context)
    }

    fn execute_command_with_context(
        &self,
        sql: &str,
        context: &crate::query_execution::request_context::RequestContext,
        query_opts: Option<QueryOptions>,
    ) -> Result<StatementResult, String> {
        if Self::is_query_sql(sql) {
            return Err("query statements must be compiled through StandaloneQueryCompiler".into());
        }
        let connector_context = crate::connector::connector_request_context_for_query(
            query_opts.as_ref(),
            context.execution().cancellation().clone(),
        )?;
        self.execute_in_context_inner(sql, context, query_opts, connector_context)
    }

    fn prepare_query_with_context(
        &self,
        sql: &str,
        request_context: &crate::query_execution::request_context::RequestContext,
        query_opts: Option<QueryOptions>,
    ) -> Result<PreparedQueryOperation, String> {
        let connector_context = crate::connector::connector_request_context_for_query(
            query_opts.as_ref(),
            request_context.execution().cancellation().clone(),
        )?;
        self.prepare_query_with_context_and_connector_context(
            sql,
            request_context,
            query_opts,
            connector_context,
        )
    }

    fn prepare_query_with_context_and_connector_context(
        &self,
        sql: &str,
        request_context: &crate::query_execution::request_context::RequestContext,
        query_opts: Option<QueryOptions>,
        connector_context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<PreparedQueryOperation, String> {
        if !Self::is_query_sql(sql) {
            return Err(
                "non-query statements must be executed through StandaloneCommandExecutor".into(),
            );
        }
        use sqlparser::ast as sqlast;

        let current_catalog = request_context.session().current_catalog();
        let current_database = request_context.session().current_database();
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
        let (parse_sql, forced_explain_level, force_logical_explain) =
            if let Some((rewritten, level)) = split_explain_logical_sql(&normalized) {
                (rewritten, Some(level), true)
            } else if let Some((rewritten, level)) = split_explain_costs_sql(&normalized) {
                (rewritten, Some(level), false)
            } else {
                (normalized.clone(), None, false)
            };
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&parse_sql)
            .map_err(|error| format_parser_error(&error.to_string()))?;
        match stmt {
            sqlast::Statement::Explain {
                statement,
                verbose,
                analyze: false,
                ..
            } => {
                let sqlast::Statement::Query(ref query) = *statement else {
                    return Err("EXPLAIN only supports SELECT queries".to_string());
                };
                let prepared = prepare_explain_query(
                    &self.inner,
                    current_catalog,
                    current_database,
                    query,
                    &connector_context,
                )?;
                let level = forced_explain_level.unwrap_or(if verbose {
                    crate::sql::explain::ExplainLevel::Verbose
                } else {
                    crate::sql::explain::ExplainLevel::Normal
                });
                let catalog_service_snapshot = catalog_service_snapshot(&self.inner);
                let analyzer_provider = build_catalog_service_provider(
                    current_catalog,
                    &catalog_service_snapshot,
                    self.inner.connector_control.as_ref(),
                    connector_context.clone(),
                    TableLookupMode::ExplainStats,
                );
                let result = explain_query_with_sql_compiler_kernel(
                    &prepared,
                    &analyzer_provider,
                    current_catalog,
                    current_database,
                    &self.inner,
                    &connector_context,
                    request_context.execution(),
                    level,
                    force_logical_explain,
                )?;
                Ok(PreparedQueryOperation::Immediate(PreparedImmediateQuery {
                    result: StatementResult::Query(result),
                }))
            }
            sqlast::Statement::Explain {
                statement,
                analyze: true,
                ..
            } => {
                let sqlast::Statement::Query(ref query) = *statement else {
                    return Err("EXPLAIN ANALYZE only supports SELECT queries".to_string());
                };
                self.prepare_explain_analyze_query(
                    query,
                    current_catalog,
                    current_database,
                    query_opts,
                    &connector_context,
                    request_context.execution(),
                )
            }
            sqlast::Statement::Query(ref query) => {
                if let Some(result) =
                    self::information_schema::try_query_materialized_views(&self.inner, query)?
                {
                    return Ok(PreparedQueryOperation::Immediate(PreparedImmediateQuery {
                        result,
                    }));
                }
                let mut prepared = query.as_ref().clone();
                self.inner.view_service.rewrite_query(
                    self.inner.as_ref(),
                    &mut prepared,
                    crate::engine::view::ViewRequestContext {
                        current_catalog,
                        current_database,
                        connector_context: Some(&connector_context),
                    },
                )?;
                self::virtual_table::rewrite_query(&self.inner, &mut prepared)?;
                if has_time_travel_refs(&prepared) {
                    rewrite_time_travel_refs(
                        &self.inner,
                        current_catalog,
                        current_database,
                        &mut prepared,
                        &connector_context,
                    )?;
                }
                let catalog_service_snapshot = catalog_service_snapshot(&self.inner);
                let analyzer_provider = build_catalog_service_provider(
                    current_catalog,
                    &catalog_service_snapshot,
                    self.inner.connector_control.as_ref(),
                    connector_context.clone(),
                    TableLookupMode::SchemaOnly,
                );
                let (request, _, _) = prepare_query_with_sql_compiler_kernel(
                    &prepared,
                    &analyzer_provider,
                    current_catalog,
                    current_database,
                    &self.inner,
                    &connector_context,
                    query_opts,
                    request_context.execution(),
                    crate::sql::compiler::SqlCompileIntent::Query,
                    true,
                )?;
                Ok(PreparedQueryOperation::Distributed(
                    PreparedDistributedQuery {
                        request,
                        completion: PreparedQueryCompletion {
                            formatter: PreparedQueryFormatter::Result,
                        },
                    },
                ))
            }
            _ => Err("query compiler only supports SELECT and EXPLAIN statements".to_string()),
        }
    }

    fn prepare_explain_analyze_query(
        &self,
        query: &sqlparser::ast::Query,
        current_catalog: Option<&str>,
        current_database: &str,
        query_opts: Option<QueryOptions>,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
        execution: &crate::query_execution::request_context::QueryExecutionContext,
    ) -> Result<PreparedQueryOperation, String> {
        let query = prepare_explain_query(
            &self.inner,
            current_catalog,
            current_database,
            query,
            connector_context,
        )?;
        let catalog_service_snapshot = catalog_service_snapshot(&self.inner);
        let analyzer_provider = build_catalog_service_provider(
            current_catalog,
            &catalog_service_snapshot,
            self.inner.connector_control.as_ref(),
            connector_context.clone(),
            TableLookupMode::ExplainStats,
        );
        let planning_start = std::time::Instant::now();
        let (request, distributed_plan, connector_static_planning) =
            prepare_query_with_sql_compiler_kernel(
                &query,
                &analyzer_provider,
                current_catalog,
                current_database,
                &self.inner,
                connector_context,
                Some(query_options_for_explain_analyze(query_opts)),
                execution,
                crate::sql::compiler::SqlCompileIntent::Explain {
                    level: crate::sql::explain::ExplainLevel::Analyze,
                    analyze: true,
                },
                true,
            )?;
        Ok(PreparedQueryOperation::Distributed(
            PreparedDistributedQuery {
                request,
                completion: PreparedQueryCompletion {
                    formatter: PreparedQueryFormatter::Profile(PreparedProfileFormatter {
                        distributed_plan,
                        planning_elapsed: planning_start.elapsed(),
                        execution_started_at: std::time::Instant::now(),
                        connector_static_planning,
                    }),
                },
            },
        ))
    }

    #[cfg(test)]
    pub(crate) fn execute_in_context(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
        query_opts: Option<QueryOptions>,
    ) -> Result<StatementResult, String> {
        let context = test_request_context(current_catalog, current_database);
        let connector_context = crate::connector::connector_request_context_for_query(
            query_opts.as_ref(),
            context.execution().cancellation().clone(),
        )?;
        self.execute_in_context_inner(sql, &context, query_opts, connector_context)
    }

    #[cfg(test)]
    pub(crate) fn execute_in_context_with_connector_context(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
        query_opts: Option<QueryOptions>,
        connector_context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        let context = test_request_context(current_catalog, current_database);
        self.execute_in_context_inner(sql, &context, query_opts, connector_context)
    }

    fn execute_in_context_inner(
        &self,
        sql: &str,
        request_context: &crate::query_execution::request_context::RequestContext,
        query_opts: Option<QueryOptions>,
        connector_context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        let current_catalog = request_context.session().current_catalog();
        let current_database = request_context.session().current_database();
        // Query compilation has one canonical admission path. Keep this legacy
        // session seam for tests/embedded callers, but never rebuild a second
        // analyzer/optimizer/preparation pipeline from it.
        if Self::is_query_sql(sql) {
            return match self.prepare_query_with_context_and_connector_context(
                sql,
                request_context,
                query_opts,
                connector_context,
            )? {
                PreparedQueryOperation::Immediate(operation) => Ok(operation.into_result()),
                PreparedQueryOperation::Distributed(operation) => {
                    let (request, completion) = operation.into_parts();
                    let outcome = self
                        .inner
                        .query_execution
                        .execute(request)
                        .map_err(|error| error.to_string())?;
                    completion.complete(outcome)
                }
            };
        }
        use crate::sql::parser::dialect::{
            StarRocksDialect, looks_like_create_catalog, looks_like_create_database,
            looks_like_create_table, looks_like_drop_statement,
        };
        use sqlparser::ast as sqlast;

        let mut normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
        normalized =
            rewrite_legacy_partition_references(&self.inner, &normalized, current_database)?;
        normalized = rewrite_named_partition_insert_overwrite(&normalized)?;
        if let Some(result) = self.inner.view_service.try_handle_statement(
            self.inner.as_ref(),
            &normalized,
            crate::engine::view::ViewRequestContext {
                current_catalog,
                current_database,
                connector_context: Some(&connector_context),
            },
        )? {
            return Ok(match result {
                crate::engine::view::ViewStatementResult::Ok => StatementResult::Ok,
                crate::engine::view::ViewStatementResult::Query(result) => {
                    StatementResult::Query(result)
                }
            });
        }
        if crate::sql::parser::procedure::looks_like_call_procedure(&normalized) {
            let statement = crate::sql::parser::procedure::parse_call_procedure_sql(&normalized)?;
            if statement.procedure == crate::engine::mv::stateless_rebuild::PROCEDURE_NAME {
                return crate::engine::mv::stateless_rebuild::execute_novarocks_imv_stateless_rebuild(
                    &self.inner,
                    &statement,
                    current_database,
                    connector_context,
                );
            }
        }
        if let Some(result) = self.inner.table_maintenance_service.try_handle_statement(
            self.inner.as_ref(),
            &normalized,
            crate::engine::table_maintenance::MaintenanceRequestContext {
                current_catalog,
                current_database,
            },
        )? {
            return Ok(match result {
                crate::engine::table_maintenance::MaintenanceStatementResult::Ok => {
                    StatementResult::Ok
                }
                crate::engine::table_maintenance::MaintenanceStatementResult::Query(result) => {
                    StatementResult::Query(result)
                }
            });
        }
        // Statistics syntax has one typed parse and one typed application
        // dispatch.  It must precede the legacy service so valid commands do
        // not enter a raw-SQL interceptor while the old path is being removed.
        {
            let sr_dialect = StarRocksDialect;
            if let Ok(ref peek_parser) =
                sqlparser::parser::Parser::new(&sr_dialect).try_with_sql(&normalized)
            {
                use crate::sql::parser::dialect::statistics::{
                    looks_like_analyze_table, looks_like_cancel_analyze,
                    looks_like_show_analyze_jobs, looks_like_show_table_stats,
                };
                if looks_like_analyze_table(peek_parser)
                    || looks_like_show_analyze_jobs(peek_parser)
                    || looks_like_cancel_analyze(peek_parser)
                    || looks_like_show_table_stats(peek_parser)
                {
                    let statement = crate::sql::parser::parse_sql(&normalized)?
                        .pop()
                        .ok_or_else(|| "custom parser returned no statements".to_string())?;
                    return dispatch_statement(
                        &self.inner,
                        current_catalog,
                        current_database,
                        statement,
                        request_context,
                        &connector_context,
                    );
                }
            }
        }
        if let Some((target, source)) = parse_create_table_like(&normalized)? {
            return self.handle_create_table_like(
                target,
                source,
                current_catalog,
                current_database,
                &connector_context,
            );
        }
        // For MV DDL (CREATE/DROP/REFRESH/SHOW MATERIALIZED VIEW) we must
        // propagate errors from our custom parser rather than falling through to
        // the generic sqlparser-rs path, which would emit confusing diagnostics
        // like "Expected AS, found DISTRIBUTED" for invalid PRIMARY KEY clauses.
        {
            let sr_dialect = StarRocksDialect;
            if let Ok(ref peek_parser) =
                sqlparser::parser::Parser::new(&sr_dialect).try_with_sql(&normalized)
            {
                use crate::sql::parser::dialect::backend::{
                    looks_like_add_backend, looks_like_drop_backend, looks_like_show_backends,
                };
                use crate::sql::parser::dialect::materialized_view::{
                    looks_like_create_materialized_view, looks_like_drop_materialized_view,
                    looks_like_refresh_materialized_view, looks_like_show_materialized_views,
                };
                use crate::sql::parser::dialect::truncate::looks_like_truncate_table;
                if looks_like_create_materialized_view(peek_parser)
                    || looks_like_drop_materialized_view(peek_parser)
                    || looks_like_refresh_materialized_view(peek_parser)
                    || looks_like_show_materialized_views(peek_parser)
                    // TRUNCATE TABLE: propagate our parser's reject errors
                    // (PARTITION/WHERE not supported, tag refs read-only)
                    // instead of falling through to sqlparser-rs's permissive
                    // builtin which would silently accept and bypass our checks.
                    || looks_like_truncate_table(peek_parser)
                    || looks_like_add_backend(peek_parser)
                    || looks_like_drop_backend(peek_parser)
                    || looks_like_show_backends(peek_parser)
                {
                    let mut statements = crate::sql::parser::parse_sql(&normalized)?;
                    let statement = statements
                        .pop()
                        .ok_or_else(|| "custom parser returned no statements".to_string())?;
                    return dispatch_statement(
                        &self.inner,
                        current_catalog,
                        current_database,
                        statement,
                        request_context,
                        &connector_context,
                    );
                }
            }
        }
        if let Ok(mut statements) = crate::sql::parser::parse_sql(&normalized) {
            let statement = statements
                .pop()
                .ok_or_else(|| "custom parser returned no statements".to_string())?;
            return dispatch_statement(
                &self.inner,
                current_catalog,
                current_database,
                statement,
                request_context,
                &connector_context,
            );
        }
        let parse_sql = normalized.clone();

        if let Some(parsed) = parse_explain_refresh_materialized_view(&normalized) {
            let (stmt, level, analyze) = parsed?;
            if analyze {
                return Err(
                    "EXPLAIN ANALYZE REFRESH MATERIALIZED VIEW is not supported".to_string()
                );
            }
            let lines =
                crate::engine::mv::iceberg_refresh::explain_iceberg_mv_refresh_rewrite_plan(
                    &self.inner,
                    current_catalog,
                    current_database,
                    &stmt,
                    level,
                    &connector_context,
                )?;
            return Ok(StatementResult::Query(build_string_query_result(
                "Explain String",
                lines,
            )?));
        }

        let dialect = StarRocksDialect;
        let mut parser = sqlparser::parser::Parser::new(&dialect)
            .try_with_sql(&parse_sql)
            .map_err(|e| format_parser_error(&e.to_string()))?;

        // StarRocks DDL: token-level parsing (sqlparser cannot handle these)
        if looks_like_create_table(&parser) {
            let result = crate::sql::parser::dialect::create_table::parse_create_table_statement(
                &mut parser,
            )?;
            return execute_create_table_statement(
                &self.inner,
                result,
                current_catalog,
                current_database,
                &connector_context,
            );
        }
        if looks_like_create_catalog(&parser) {
            let result =
                crate::sql::parser::dialect::create_catalog::parse_create_catalog_statement(
                    &mut parser,
                )?;
            return self.handle_create_catalog(result);
        }
        if looks_like_create_database(&parser) {
            let (db_name, if_not_exists) =
                crate::sql::parser::dialect::parse_create_database_name(&mut parser)?;
            return execute_create_database_statement(
                &self.inner,
                &db_name,
                if_not_exists,
                current_catalog,
                &connector_context,
            );
        }
        if looks_like_drop_statement(&parser) {
            let drop = crate::sql::parser::dialect::drop::parse_drop_statement(&mut parser)?;
            return self.handle_drop(drop, current_catalog, current_database, &connector_context);
        }

        // ALTER TABLE ... SET / UNSET TBLPROPERTIES
        if looks_like_alter_iceberg_properties(&normalized) {
            return self.handle_alter_iceberg_properties(
                &normalized,
                current_catalog,
                current_database,
                &connector_context,
            );
        }

        // ALTER TABLE ... ADD/DROP/RENAME/MODIFY COLUMN
        if looks_like_alter_iceberg_schema(&normalized) {
            return self.handle_alter_iceberg_schema(
                &normalized,
                current_catalog,
                current_database,
                &connector_context,
            );
        }

        // ALTER TABLE ... ADD/DROP PARTITION COLUMN ...
        if looks_like_alter_partition_column(&normalized) {
            let stmt = parse_alter_partition_column_sql(&normalized)?;
            return self.handle_alter_partition_spec(
                stmt,
                current_catalog,
                current_database,
                &connector_context,
            );
        }

        // SHOW CREATE TABLE ...
        if looks_like_show_create_table(&normalized) {
            return self.handle_show_create_table(
                &normalized,
                current_catalog,
                current_database,
                &connector_context,
            );
        }

        // ALTER TABLE ... ADD EQUALITY DELETE (...) VALUES (...)
        if looks_like_add_equality_delete(&normalized) {
            return self.handle_add_equality_delete(
                &normalized,
                current_catalog,
                current_database,
                &connector_context,
            );
        }

        // Standard SQL: let sqlparser parse the full statement
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&parse_sql)
            .map_err(|e| format_parser_error(&e.to_string()))?;
        match stmt {
            sqlast::Statement::Insert(_) => {
                Err("INSERT must be routed by frontend DML service".to_string())
            }
            sqlast::Statement::Delete(_) => {
                Err("DELETE must be routed by frontend DML service".to_string())
            }
            sqlast::Statement::Truncate(_) => {
                Err("TRUNCATE must be routed by frontend DML service".to_string())
            }
            sqlast::Statement::Update(_) => {
                Err("UPDATE must be routed by frontend DML service".to_string())
            }
            sqlast::Statement::Merge(_) => {
                Err("MERGE must be routed by frontend DML service".to_string())
            }
            _ => Err(format!(
                "unsupported sql: {}",
                sql.chars().take(50).collect::<String>()
            )),
        }
    }

    fn is_query_sql(sql: &str) -> bool {
        let mut words = sql.split_whitespace();
        match words.next().map(|word| word.to_ascii_lowercase()) {
            Some(keyword) if matches!(keyword.as_str(), "select" | "with") => true,
            Some(keyword) if keyword == "explain" => {
                let mut target = words.next().map(|word| word.to_ascii_lowercase());
                while matches!(
                    target.as_deref(),
                    Some("analyze" | "verbose" | "costs" | "logical")
                ) {
                    target = words.next().map(|word| word.to_ascii_lowercase());
                }
                matches!(target.as_deref(), Some("select" | "with"))
            }
            _ => false,
        }
    }

    fn handle_show_create_table(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        let table_name = parse_show_create_table(sql)?;
        let target = crate::engine::backend_resolver::resolve_existing_table_target(
            &self.inner,
            &table_name,
            current_catalog,
            current_database,
        )?;
        if target.backend_name != "iceberg" {
            return Err(format!(
                "SHOW CREATE TABLE only supports Iceberg tables, got `{}` backend",
                target.backend_name
            ));
        }
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| error.to_string())?;
        let lease = self
            .inner
            .connector_control
            .acquire_current(&instance_id)
            .map_err(|error| error.to_string())?;
        let identity = novarocks_spi::connector::ConnectorTableIdentity {
            instance_id,
            namespace: Arc::from(target.namespace.as_str()),
            table: Arc::from(target.table.as_str()),
        };
        let loaded = lease
            .binding()
            .metadata()
            .load_table(novarocks_spi::connector::ConnectorTableRequest {
                table: identity.clone(),
                resolution: novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
                context: connector_context.clone(),
            })
            .map_err(|error| error.to_string())?;
        if loaded.identity != identity || loaded.table.owner() != &identity.instance_id {
            return Err(
                "SHOW CREATE TABLE received corrupt metadata for a different connector table"
                    .to_string(),
            );
        }
        let ddl = build_iceberg_create_table_ddl(
            &target.catalog,
            &target.namespace,
            &target.table,
            &loaded,
        )?;
        let fields = vec![
            Field::new("Table", DataType::Utf8, false),
            Field::new("Create Table", DataType::Utf8, false),
        ];
        let arrays: Vec<Arc<dyn arrow::array::Array>> = vec![
            Arc::new(StringArray::from(vec![target.table.clone()])),
            Arc::new(StringArray::from(vec![ddl])),
        ];
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
            .map_err(|e| format!("build SHOW CREATE TABLE result failed: {e}"))?;
        Ok(StatementResult::Query(QueryResult {
            columns: vec![
                QueryResultColumn {
                    name: "Table".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    logical_type: None,
                },
                QueryResultColumn {
                    name: "Create Table".to_string(),
                    data_type: DataType::Utf8,
                    nullable: false,
                    logical_type: None,
                },
            ],
            chunks: vec![record_batch_to_chunk(batch)?],
        }))
    }

    fn handle_alter_iceberg_properties(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        let stmt = parse_alter_iceberg_properties_sql(sql)?;
        let target = crate::engine::backend_resolver::resolve_existing_table_target(
            &self.inner,
            &stmt.table,
            current_catalog,
            current_database,
        )?;
        if target.backend_name == "iceberg" {
            crate::engine::mv::iceberg_guard::reject_if_iceberg_mv_table(
                &self.inner,
                &target,
                crate::engine::mv::iceberg_guard::IcebergMvUserMutation::AlterTable,
            )?;
        }
        if target.backend_name != "iceberg" {
            return Err(
                "ALTER TABLE TBLPROPERTIES only supports standalone iceberg catalogs".to_string(),
            );
        }
        let changes = match stmt.op {
            crate::engine::statement::PropertiesOp::Set { entries } => entries
                .into_iter()
                .map(
                    |(key, value)| novarocks_spi::connector::ConnectorPropertyChange::Set {
                        key: Arc::from(key),
                        value: Arc::from(value),
                    },
                )
                .collect(),
            crate::engine::statement::PropertiesOp::Unset { keys, if_exists } => keys
                .into_iter()
                .map(
                    |key| novarocks_spi::connector::ConnectorPropertyChange::Unset {
                        key: Arc::from(key),
                        if_exists,
                    },
                )
                .collect(),
        };
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| error.to_string())?;
        crate::connector::mutation::execute_catalog_mutation(
            self.inner.connector_control.as_ref(),
            &instance_id,
            novarocks_spi::connector::ConnectorCatalogMutationOperation::AlterProperties {
                table: novarocks_spi::connector::ConnectorTableIdentity {
                    instance_id: instance_id.clone(),
                    namespace: Arc::from(target.namespace.as_str()),
                    table: Arc::from(target.table.as_str()),
                },
                changes,
                authority: novarocks_spi::connector::ConnectorPropertyAuthority::UserStatement,
                expected_committed_partitioning: None,
            },
            connector_context.clone(),
        )?;
        crate::engine::iceberg_writer::invalidate_iceberg_caches(&self.inner, &target)?;
        Ok(StatementResult::Ok)
    }

    fn handle_alter_iceberg_schema(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        let stmt = crate::engine::statement::parse_alter_iceberg_schema_sql(sql)?;
        let target = crate::engine::backend_resolver::resolve_existing_table_target(
            &self.inner,
            &stmt.table,
            current_catalog,
            current_database,
        )?;
        if target.backend_name != "iceberg" {
            return self.handle_local_schema_change(stmt, target);
        }
        crate::engine::mv::iceberg_guard::reject_if_iceberg_mv_table(
            &self.inner,
            &target,
            crate::engine::mv::iceberg_guard::IcebergMvUserMutation::AlterTable,
        )?;
        crate::connector::iceberg::catalog::schema_update::validate_schema_change_application_guard(
            &self.inner,
            &target,
            &stmt.change,
        )?;
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| error.to_string())?;
        let change = match stmt.change {
            crate::engine::statement::IcebergSchemaChange::AddColumn {
                parent,
                name,
                data_type,
                default,
                position,
            } => {
                let column = crate::sql::parser::ast::TableColumnDef {
                    name,
                    data_type,
                    nullable: true,
                    aggregation: None,
                    default,
                };
                novarocks_spi::connector::ConnectorSchemaChange::AddColumn {
                    parent: novarocks_spi::connector::ConnectorColumnPath {
                        segments: parent
                            .segments()
                            .iter()
                            .map(|segment| Arc::from(segment.as_str()))
                            .collect(),
                    },
                    column: crate::engine::statement::connector_column(&column)?,
                    position: connector_schema_position(position),
                }
            }
            crate::engine::statement::IcebergSchemaChange::DropColumn { path } => {
                novarocks_spi::connector::ConnectorSchemaChange::DropColumn {
                    path: connector_schema_path(path),
                }
            }
            crate::engine::statement::IcebergSchemaChange::RenameColumn { path, new_name } => {
                novarocks_spi::connector::ConnectorSchemaChange::RenameColumn {
                    path: connector_schema_path(path),
                    to: Arc::from(new_name),
                }
            }
            crate::engine::statement::IcebergSchemaChange::ModifyColumn { path, new_type } => {
                novarocks_spi::connector::ConnectorSchemaChange::ModifyColumn {
                    path: connector_schema_path(path),
                    data_type: crate::engine::statement::connector_data_type(&new_type)?,
                }
            }
            crate::engine::statement::IcebergSchemaChange::SetNullable { path, nullable } => {
                novarocks_spi::connector::ConnectorSchemaChange::SetColumnNullability {
                    path: connector_schema_path(path),
                    nullable,
                }
            }
            crate::engine::statement::IcebergSchemaChange::Reorder { path, position } => {
                novarocks_spi::connector::ConnectorSchemaChange::ReorderColumn {
                    path: connector_schema_path(path),
                    position: connector_schema_position(position),
                }
            }
            crate::engine::statement::IcebergSchemaChange::UpdateComment { path, comment } => {
                novarocks_spi::connector::ConnectorSchemaChange::SetColumnComment {
                    path: connector_schema_path(path),
                    comment: Arc::from(comment),
                }
            }
        };
        crate::connector::mutation::execute_catalog_mutation(
            self.inner.connector_control.as_ref(),
            &instance_id,
            novarocks_spi::connector::ConnectorCatalogMutationOperation::AlterSchema {
                table: novarocks_spi::connector::ConnectorTableIdentity {
                    instance_id: instance_id.clone(),
                    namespace: Arc::from(target.namespace.as_str()),
                    table: Arc::from(target.table.as_str()),
                },
                changes: vec![change],
            },
            connector_context.clone(),
        )?;
        crate::engine::iceberg_writer::invalidate_iceberg_caches(&self.inner, &target)?;
        Ok(StatementResult::Ok)
    }

    fn handle_local_schema_change(
        &self,
        stmt: crate::engine::statement::AlterIcebergSchemaStmt,
        target: crate::engine::backend_resolver::TargetBackend,
    ) -> Result<StatementResult, String> {
        match stmt.change {
            crate::engine::statement::IcebergSchemaChange::RenameColumn { path, .. }
                if path.segments().len() == 1 =>
            {
                Ok(StatementResult::Ok)
            }
            _ => Err(format!(
                "ALTER TABLE schema change only supports top-level RENAME COLUMN for `{}` tables",
                target.backend_name
            )),
        }
    }

    /// Handle ALTER TABLE ... ADD/DROP PARTITION COLUMN ...
    fn handle_alter_partition_spec(
        &self,
        stmt: crate::sql::parser::ast::AlterIcebergPartitionSpecStmt,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        let table_name = match &stmt {
            crate::sql::parser::ast::AlterIcebergPartitionSpecStmt::AddPartitionColumn {
                table,
                ..
            }
            | crate::sql::parser::ast::AlterIcebergPartitionSpecStmt::DropPartitionColumn {
                table,
                ..
            } => table,
        };
        let target = crate::engine::backend_resolver::resolve_table_target(
            &self.inner,
            table_name,
            current_catalog,
            current_database,
        )?;
        if target.backend_name != "iceberg" {
            return Err(format!(
                "ALTER TABLE ADD/DROP PARTITION COLUMN only supports iceberg backends, got `{}`",
                target.backend_name
            ));
        }
        crate::engine::mv::iceberg_guard::reject_if_iceberg_mv_table(
            &self.inner,
            &target,
            crate::engine::mv::iceberg_guard::IcebergMvUserMutation::AlterTable,
        )?;
        let adding = matches!(
            &stmt,
            crate::sql::parser::ast::AlterIcebergPartitionSpecStmt::AddPartitionColumn { .. }
        );
        let partition_field = match &stmt {
            crate::sql::parser::ast::AlterIcebergPartitionSpecStmt::AddPartitionColumn {
                field,
                ..
            }
            | crate::sql::parser::ast::AlterIcebergPartitionSpecStmt::DropPartitionColumn {
                field,
                ..
            } => field,
        };
        // Partition-transform admission is an Iceberg fact, so it belongs to the
        // provider's `AlterPartitionSpec` operation below: that path evolves the
        // spec from the same current schema and default spec, and rejects a
        // variant source column, a non-temporal source under a temporal
        // transform, an already-present field, and an unbuildable spec. The
        // rejection set is unchanged, so this layer no longer loads a table to
        // pre-answer it.
        let transform = connector_partition_transform(partition_field);
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&target.catalog)
            .map_err(|error| error.to_string())?;
        crate::connector::mutation::execute_catalog_mutation(
            self.inner.connector_control.as_ref(),
            &instance_id,
            novarocks_spi::connector::ConnectorCatalogMutationOperation::AlterPartitionSpec {
                table: novarocks_spi::connector::ConnectorTableIdentity {
                    instance_id: instance_id.clone(),
                    namespace: Arc::from(target.namespace.as_str()),
                    table: Arc::from(target.table.as_str()),
                },
                add: if adding {
                    vec![transform.clone()]
                } else {
                    Vec::new()
                },
                drop: if adding { Vec::new() } else { vec![transform] },
            },
            connector_context.clone(),
        )?;
        crate::engine::iceberg_writer::invalidate_iceberg_caches(&self.inner, &target)?;
        Ok(StatementResult::Ok)
    }

    /// Handle ALTER TABLE ... ADD EQUALITY DELETE (...) VALUES (...)
    fn handle_add_equality_delete(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        let _ = (sql, current_catalog, current_database, connector_context);
        Err("ADD EQUALITY DELETE must be routed by frontend DML service".to_string())
    }

    /// Handle CREATE CATALOG result.
    fn handle_create_catalog(
        &self,
        stmt: crate::sql::parser::ast::CreateCatalogStmt,
    ) -> Result<StatementResult, String> {
        let _lifecycle = self
            .inner
            .catalog_attachment_lifecycle
            .lock()
            .map_err(|error| format!("catalog attachment lifecycle lock: {error}"))?;
        let normalized_catalog = normalize_identifier(&stmt.name)?;
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(&normalized_catalog)
            .map_err(|error| format!("invalid Iceberg connector instance ID: {error}"))?;
        match self
            .inner
            .connector_control
            .observe_current_binding(&instance_id)
        {
            Ok(_) => return Ok(StatementResult::Ok),
            Err(error)
                if error.kind() == novarocks_spi::connector::ConnectorErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "resolve Iceberg catalog `{normalized_catalog}` before create: {error}"
                ));
            }
        }
        let persisted_properties =
            create_iceberg_control_binding(&self.inner, &normalized_catalog, stmt.properties)?;
        self.inner.catalog_service.register_catalog(
            crate::engine::query_planning::catalog_runtime::build_iceberg_catalog(
                &stmt.name,
                Arc::clone(&self.inner.connector_control)
                    as Arc<dyn novarocks_spi::connector::ConnectorControlResolver>,
            ),
        );
        if let Err(error) = persist_catalog_attachment_if_needed(
            &self.inner,
            &normalized_catalog,
            &persisted_properties,
        ) {
            retire_iceberg_control_binding(&self.inner, &normalized_catalog)?;
            self.inner
                .catalog_service
                .unregister_catalog(&normalized_catalog);
            return Err(error);
        }
        Ok(StatementResult::Ok)
    }

    fn handle_create_table_like(
        &self,
        target: crate::sql::parser::ast::ObjectName,
        source: crate::sql::parser::ast::ObjectName,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        let source_target = crate::engine::backend_resolver::resolve_existing_table_target(
            &self.inner,
            &source,
            current_catalog,
            current_database,
        )?;
        let source_table = crate::connector::metadata_load_table(
            self.inner.connector_control.as_ref(),
            connector_context.clone(),
            &source_target.catalog,
            &source_target.namespace,
            &source_target.table,
            novarocks_spi::connector::ConnectorTableResolution::StrictBaseTable,
        )?
        .0;
        let columns = source_table
            .columns
            .iter()
            .map(|column| {
                Ok(crate::sql::parser::ast::TableColumnDef {
                    name: column.name.clone(),
                    data_type: crate::engine::iceberg_ctas::arrow_data_type_to_sql_type(
                        &column.data_type,
                    )?,
                    nullable: column.nullable,
                    aggregation: None,
                    default: None,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        execute_create_table_statement(
            &self.inner,
            crate::sql::parser::ast::CreateTableStmt {
                name: target,
                kind: crate::sql::parser::ast::CreateTableKind::Iceberg {
                    columns,
                    key_desc: None,
                    bucket_count: None,
                    distribution_columns: Vec::new(),
                    partition_fields: Vec::new(),
                    properties: Vec::new(),
                },
                legacy_range_partitions: Vec::new(),
                as_select: None,
                if_not_exists: false,
            },
            current_catalog,
            current_database,
            connector_context,
        )
    }

    /// Handle DROP TABLE/DATABASE/CATALOG result.
    fn handle_drop(
        &self,
        drop: crate::sql::parser::dialect::drop::DropResult,
        current_catalog: Option<&str>,
        current_database: &str,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        use crate::sql::parser::dialect::drop::DropResult;
        match drop {
            DropResult::Catalog(stmt) => {
                execute_drop_catalog_statement(&self.inner, &stmt.name, stmt.if_exists)
            }
            DropResult::Database(stmt) => {
                if let Some(database) = resolve_default_view_database(&stmt.name, current_catalog)?
                {
                    self.inner
                        .view_service
                        .drop_database("default_catalog", &database)?;
                    return Ok(StatementResult::Ok);
                }
                let target = crate::engine::backend_resolver::resolve_namespace_target(
                    &self.inner,
                    &stmt.name,
                    current_catalog,
                )?;
                let result = execute_drop_database_statement(
                    &self.inner,
                    &stmt.name,
                    current_catalog,
                    stmt.if_exists,
                    stmt.force,
                    connector_context,
                )?;
                self.inner
                    .view_service
                    .drop_database(&target.catalog, &target.namespace)?;
                Ok(result)
            }
            DropResult::Table(stmt) => {
                let result = execute_drop_table_statement(
                    &self.inner,
                    &stmt.name,
                    current_catalog,
                    current_database,
                    stmt.if_exists,
                    stmt.force,
                    connector_context,
                )?;
                Ok(result)
            }
        }
    }
}

fn connector_schema_path(
    path: crate::engine::statement::ColumnPath,
) -> novarocks_spi::connector::ConnectorColumnPath {
    novarocks_spi::connector::ConnectorColumnPath {
        segments: path
            .segments()
            .iter()
            .map(|segment| Arc::from(segment.as_str()))
            .collect(),
    }
}

fn connector_schema_position(
    position: crate::engine::statement::AddPosition,
) -> novarocks_spi::connector::ConnectorColumnPosition {
    match position {
        crate::engine::statement::AddPosition::Default => {
            novarocks_spi::connector::ConnectorColumnPosition::Default
        }
        crate::engine::statement::AddPosition::First => {
            novarocks_spi::connector::ConnectorColumnPosition::First
        }
        crate::engine::statement::AddPosition::After(column) => {
            novarocks_spi::connector::ConnectorColumnPosition::After {
                column: Arc::from(column),
            }
        }
        crate::engine::statement::AddPosition::Before(column) => {
            novarocks_spi::connector::ConnectorColumnPosition::Before {
                column: Arc::from(column),
            }
        }
    }
}

fn connector_partition_transform(
    field: &crate::sql::parser::ast::IcebergPartitionFieldExpr,
) -> novarocks_spi::connector::ConnectorPartitionTransform {
    use crate::sql::parser::ast::IcebergPartitionFieldExpr;
    use novarocks_spi::connector::ConnectorPartitionTransform;

    match field {
        IcebergPartitionFieldExpr::Identity { column } => ConnectorPartitionTransform::Identity {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Year { column } => ConnectorPartitionTransform::Year {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Month { column } => ConnectorPartitionTransform::Month {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Day { column } => ConnectorPartitionTransform::Day {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Hour { column } => ConnectorPartitionTransform::Hour {
            column: Arc::from(column.as_str()),
        },
        IcebergPartitionFieldExpr::Bucket {
            column,
            num_buckets,
        } => ConnectorPartitionTransform::Bucket {
            column: Arc::from(column.as_str()),
            num_buckets: *num_buckets,
        },
        IcebergPartitionFieldExpr::Truncate { column, width } => {
            ConnectorPartitionTransform::Truncate {
                column: Arc::from(column.as_str()),
                width: *width,
            }
        }
        IcebergPartitionFieldExpr::Void { column } => ConnectorPartitionTransform::Void {
            column: Arc::from(column.as_str()),
        },
    }
}

#[cfg(test)]
fn test_request_context(
    current_catalog: Option<&str>,
    current_database: &str,
) -> crate::query_execution::request_context::RequestContext {
    test_request_context_with_role(
        current_catalog,
        current_database,
        crate::common::app_config::ClusterRole::AllInOne,
    )
}

#[cfg(test)]
fn test_request_context_with_role(
    current_catalog: Option<&str>,
    current_database: &str,
    role: crate::common::app_config::ClusterRole,
) -> crate::query_execution::request_context::RequestContext {
    use crate::query_execution::backend::{BackendTopologySnapshot, LiveBackendTarget};
    use crate::query_execution::cancellation::QueryCancellationSource;
    use crate::query_execution::request_context::{
        QueryExecutionContext, RequestContext, RequestSessionContext,
    };

    let cancellation = QueryCancellationSource::new();
    RequestContext::new(
        RequestSessionContext::new(
            current_catalog.map(str::to_string),
            current_database.to_string(),
            crate::sql::optimizer::options::SessionOptimizerSettings::default(),
        ),
        QueryExecutionContext::new(
            role,
            // Test composition supplies an admitted loopback backend explicitly.
            // Production compilation still rejects an empty topology instead of
            // inferring one from the all-in-one role.
            BackendTopologySnapshot::try_new(
                0,
                vec![LiveBackendTarget::new(
                    0,
                    "127.0.0.1:9030".parse().expect("loopback test backend"),
                    1,
                )],
            )
            .expect("non-empty test topology"),
            None,
            cancellation.view(),
            crate::sql::optimizer::options::SessionOptimizerSettings::default(),
        ),
    )
}

fn resolve_default_view_database(
    name: &crate::sql::parser::ast::ObjectName,
    current_catalog: Option<&str>,
) -> Result<Option<String>, String> {
    let database = match name.parts.as_slice() {
        [database]
            if current_catalog
                .is_some_and(|catalog| catalog.eq_ignore_ascii_case("default_catalog")) =>
        {
            database
        }
        [catalog, database] if catalog.eq_ignore_ascii_case("default_catalog") => database,
        _ => return Ok(None),
    };
    normalize_identifier(database).map(Some)
}

fn standalone_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

fn rewrite_named_partition_insert_overwrite(sql: &str) -> Result<String, String> {
    let re = regex::Regex::new(
        r#"(?is)^\s*insert\s+overwrite\s+(?:table\s+)?(?P<table>(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)(?:\.(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)){0,2})\s+partition\s*\([^)]*\)\s+(?P<rest>.*)$"#,
    )
    .map_err(|e| format!("compile INSERT OVERWRITE partition rewrite regex failed: {e}"))?;
    let Some(captures) = re.captures(sql) else {
        return Ok(sql.to_string());
    };
    let table = captures.name("table").expect("table capture").as_str();
    let rest = captures.name("rest").expect("rest capture").as_str();
    Ok(format!("INSERT OVERWRITE PARTITIONS {table} {rest}"))
}

fn parse_create_table_like(
    sql: &str,
) -> Result<
    Option<(
        crate::sql::parser::ast::ObjectName,
        crate::sql::parser::ast::ObjectName,
    )>,
    String,
> {
    let re = regex::Regex::new(
        r#"(?is)^\s*create\s+table\s+(?P<target>(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)(?:\.(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)){0,2})\s+like\s+(?P<source>(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)(?:\.(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)){0,2})\s*$"#,
    )
    .map_err(|e| format!("compile CREATE TABLE LIKE regex failed: {e}"))?;
    let Some(captures) = re.captures(sql) else {
        return Ok(None);
    };
    let target = parse_simple_object_name(captures.name("target").expect("target").as_str())?;
    let source = parse_simple_object_name(captures.name("source").expect("source").as_str())?;
    Ok(Some((target, source)))
}

fn parse_simple_object_name(token: &str) -> Result<crate::sql::parser::ast::ObjectName, String> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut in_backtick = false;
    for ch in token.chars() {
        match ch {
            '`' => in_backtick = !in_backtick,
            '.' if !in_backtick => {
                if cur.is_empty() {
                    return Err(format!("empty object name segment in `{token}`"));
                }
                parts.push(cur.clone());
                cur.clear();
            }
            _ => cur.push(ch),
        }
    }
    if !cur.is_empty() {
        parts.push(cur);
    }
    if parts.is_empty() {
        return Err(format!("empty object name `{token}`"));
    }
    Ok(crate::sql::parser::ast::ObjectName { parts })
}

/// Generate a `CREATE TABLE` DDL string from exact-generation connector facts.
fn build_iceberg_create_table_ddl(
    catalog: &str,
    namespace: &str,
    table: &str,
    loaded: &novarocks_spi::connector::ConnectorTableMetadata,
) -> Result<String, String> {
    use novarocks_spi::connector::ConnectorTableDefinitionType;

    fn definition_type_to_sql(ty: &ConnectorTableDefinitionType) -> String {
        match ty {
            ConnectorTableDefinitionType::Boolean => "BOOLEAN".to_string(),
            ConnectorTableDefinitionType::Int => "INT".to_string(),
            ConnectorTableDefinitionType::BigInt => "BIGINT".to_string(),
            ConnectorTableDefinitionType::Float => "FLOAT".to_string(),
            ConnectorTableDefinitionType::Double => "DOUBLE".to_string(),
            ConnectorTableDefinitionType::Decimal { precision, scale } => {
                format!("DECIMAL({precision},{scale})")
            }
            ConnectorTableDefinitionType::Date => "DATE".to_string(),
            ConnectorTableDefinitionType::Time => "TIME".to_string(),
            ConnectorTableDefinitionType::DateTime => "DATETIME".to_string(),
            ConnectorTableDefinitionType::DateTimeNs => "TIMESTAMP_NS".to_string(),
            ConnectorTableDefinitionType::String => "STRING".to_string(),
            ConnectorTableDefinitionType::Binary {
                fixed_length: Some(length),
            } => format!("BINARY({length})"),
            ConnectorTableDefinitionType::Binary { fixed_length: None } => "BINARY".to_string(),
            ConnectorTableDefinitionType::Variant => "VARIANT".to_string(),
            ConnectorTableDefinitionType::Array(element) => {
                format!("ARRAY<{}>", definition_type_to_sql(element))
            }
            ConnectorTableDefinitionType::Map(key, value) => format!(
                "MAP<{},{}>",
                definition_type_to_sql(key),
                definition_type_to_sql(value)
            ),
            ConnectorTableDefinitionType::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|field| {
                        format!(
                            "{} {}",
                            field.name(),
                            definition_type_to_sql(field.data_type())
                        )
                    })
                    .collect::<Vec<_>>();
                format!("STRUCT<{}>", fields.join(", "))
            }
        }
    }

    if loaded.definition_facts.is_empty() {
        return Err(
            "SHOW CREATE TABLE is unsupported because the connector returned no table definition facts"
                .to_string(),
        );
    }
    let mut col_defs = Vec::with_capacity(loaded.definition_facts.columns().len());
    for column in loaded.definition_facts.columns() {
        let field = loaded.schema.field(column.field_ordinal() as usize);
        let nullable = if column.nullable() { "" } else { " NOT NULL" };
        let comment = if let Some(doc) = column.comment() {
            let escaped = doc.replace('\'', "\\'");
            format!(" COMMENT '{escaped}'")
        } else {
            String::new()
        };
        col_defs.push(format!(
            "  `{}` {}{}{}",
            field.name(),
            definition_type_to_sql(column.data_type()),
            nullable,
            comment
        ));
    }

    let table_comment = loaded
        .definition_facts
        .table_comment()
        .filter(|v| !v.is_empty())
        .map(|v| {
            let escaped = v.replace('\'', "\\'");
            format!("\nCOMMENT '{escaped}'")
        })
        .unwrap_or_default();

    Ok(format!(
        "CREATE TABLE `{catalog}`.`{namespace}`.`{table}` (\n{}\n){table_comment}",
        col_defs.join(",\n")
    ))
}

// ---------------------------------------------------------------------------
// Custom statement dispatch
// ---------------------------------------------------------------------------

pub(crate) mod delete_predicate_translate;
pub(crate) mod iceberg_writer;

pub(crate) fn dispatch_statement(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    statement: crate::sql::parser::ast::Statement,
    request_context: &crate::query_execution::request_context::RequestContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    use crate::sql::parser::ast::Statement;

    if connector_context.cancellation().is_cancelled() {
        return Err("connector request was cancelled".to_string());
    }
    if std::time::Instant::now() >= connector_context.deadline() {
        return Err("connector request deadline elapsed".to_string());
    }

    match statement {
        Statement::CreateMaterializedView(stmt) => {
            let engine = crate::engine::mv::iceberg_refresh::StandaloneMvEngine::new(
                Arc::clone(state),
                connector_context.clone(),
            );
            let statement = crate::mv::application::MvApplicationStatement::Create(
                crate::mv::application::MvCreateStatement::from(&stmt),
            );
            match state.mv_application_service.try_handle_statement(
                &engine,
                &statement,
                crate::mv::application::MvRequestContext {
                    current_catalog,
                    current_database,
                },
            ) {
                Ok(Some(crate::mv::application::MvStatementResult::Ok)) => Ok(StatementResult::Ok),
                Ok(Some(crate::mv::application::MvStatementResult::Query(result))) => {
                    Ok(StatementResult::Query(result))
                }
                Ok(None) => crate::engine::mv_flow::create_mv(
                    state,
                    current_catalog,
                    current_database,
                    &stmt,
                    connector_context,
                ),
                Err(error) => Err(error.to_string()),
            }
        }
        Statement::DropMaterializedView(stmt) => crate::engine::mv_flow::drop_mv(
            state,
            current_catalog,
            current_database,
            &stmt,
            connector_context,
        ),
        Statement::AlterMaterializedView(stmt)
            if matches!(
                stmt.action,
                crate::sql::parser::ast::AlterMaterializedViewAction::Repartition(_)
            ) =>
        {
            dispatch_frontend_mv_repartition(
                state,
                current_catalog,
                current_database,
                &stmt,
                request_context,
                connector_context,
            )
        }
        Statement::AlterMaterializedView(stmt) => {
            crate::engine::mv_flow::alter_mv_with_connector_context(
                state,
                current_catalog,
                current_database,
                &stmt,
                connector_context,
            )
        }
        Statement::RefreshMaterializedView(stmt) => dispatch_frontend_mv_refresh(
            state,
            current_catalog,
            current_database,
            &stmt,
            request_context,
            connector_context,
        ),
        Statement::ShowMaterializedViews(stmt) => {
            crate::engine::mv_flow::list_mvs(state, current_catalog, &stmt)
        }
        Statement::AlterIcebergRef(stmt) => crate::engine::iceberg_ref_flow::execute(
            state,
            current_database,
            &stmt,
            connector_context,
        ),
        Statement::Truncate { .. } => {
            Err("TRUNCATE must be routed by frontend DML service".to_string())
        }
        Statement::AddBackend(stmt) => {
            require_backend_management_role("ADD BACKEND", request_context.execution().role())?;
            let endpoint = stmt
                .addr
                .parse()
                .map_err(|error| format!("invalid backend address '{}': {error}", stmt.addr))?;
            state.backend_topology.add_backend(endpoint)?;
            Ok(StatementResult::Ok)
        }
        Statement::DropBackend(stmt) => {
            require_backend_management_role("DROP BACKEND", request_context.execution().role())?;
            let endpoint = stmt
                .addr
                .parse()
                .map_err(|error| format!("invalid backend address '{}': {error}", stmt.addr))?;
            state.backend_topology.drop_backend(endpoint, stmt.force)?;
            Ok(StatementResult::Ok)
        }
        Statement::ShowBackends(_) => {
            let role = request_context.execution().role();
            if role == crate::common::app_config::ClusterRole::Be {
                return Err("SHOW BACKENDS is not available in role=be".to_string());
            }
            state
                .backend_topology
                .show_backends()
                .map(StatementResult::Query)
        }
        Statement::AnalyzeTable(stmt) => execute_statistics_application_command(
            state,
            statistics_application::StatisticsApplicationCommand::AnalyzeTable {
                target: statistics_application_target(
                    &stmt.name,
                    current_catalog,
                    current_database,
                )?,
                columns: stmt.columns,
            },
        ),
        Statement::ShowAnalyzeJobs(_) => execute_statistics_application_command(
            state,
            statistics_application::StatisticsApplicationCommand::ShowAnalyzeJobs,
        ),
        Statement::CancelAnalyze(stmt) => execute_statistics_application_command(
            state,
            statistics_application::StatisticsApplicationCommand::CancelAnalyze {
                job_id: uuid::Uuid::parse_str(&stmt.job_id).map_err(|error| {
                    format!("invalid ANALYZE job ID '{}': {error}", stmt.job_id)
                })?,
            },
        ),
        Statement::ShowTableStats(stmt) => execute_statistics_application_command(
            state,
            statistics_application::StatisticsApplicationCommand::ShowTableStats {
                target: statistics_application_target(
                    &stmt.name,
                    current_catalog,
                    current_database,
                )?,
            },
        ),
    }
}

fn dispatch_frontend_mv_repartition(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    statement: &crate::sql::parser::ast::AlterMaterializedViewStmt,
    request_context: &crate::query_execution::request_context::RequestContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    let crate::sql::parser::ast::AlterMaterializedViewAction::Repartition(fields) =
        &statement.action
    else {
        return Err("frontend MV repartition route received a non-repartition action".to_string());
    };
    let target = crate::engine::mv::iceberg_refresh::resolve_refresh_target(
        current_catalog,
        current_database,
        &statement.name,
    )?;
    let target = crate::mv::repository::MvTarget {
        catalog: Some(target.catalog),
        database: target.namespace,
        name: target.table,
    };
    let refresh_statement = crate::sql::parser::ast::RefreshMaterializedViewStmt {
        name: statement.name.clone(),
        full: false,
    };
    let preparation =
        crate::engine::mv::iceberg_refresh::StandaloneMvRefreshPreparationService::new_repartition(
            state,
            current_catalog,
            current_database,
            &refresh_statement,
            fields,
            connector_context,
        );
    state
        .mv_application_service
        .prepare_and_execute_refresh(
            &preparation,
            crate::mv::application::MvApplicationStatement::Refresh(
                crate::sql::mv_refresh::MvRefreshStatement::from(&refresh_statement),
            ),
            target,
            connector_context.clone(),
            request_context.execution(),
        )
        .map(|result| match result {
            crate::mv::application::MvStatementResult::Ok => StatementResult::Ok,
            crate::mv::application::MvStatementResult::Query(result) => {
                StatementResult::Query(result)
            }
        })
        .map_err(|error| error.to_string())
}

/// Execute every dependency-ordered `REFRESH MATERIALIZED VIEW` step through
/// the frontend lifecycle.  Dependency discovery remains side-effect free;
/// no step may fall back to the old `MvBackend` plan/execute/commit surface.
fn dispatch_frontend_mv_refresh(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    stmt: &crate::sql::parser::ast::RefreshMaterializedViewStmt,
    request_context: &crate::query_execution::request_context::RequestContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<StatementResult, String> {
    let refresh_statement = crate::sql::mv_refresh::MvRefreshStatement::from(stmt);
    refresh_statement.validate_supported()?;
    let iceberg_target = crate::engine::mv::iceberg_refresh::resolve_refresh_target(
        current_catalog,
        current_database,
        &stmt.name,
    )?;
    let requested_object = crate::mv::dependency::model::iceberg_mv_dependency_ref(
        &iceberg_target.catalog,
        &iceberg_target.namespace,
        &iceberg_target.table,
    );
    let steps =
        crate::engine::mv::dependency::build_upstream_refresh_steps(state, &requested_object)?;
    let mut last_result = None;

    for step in steps {
        if step.storage_engine != crate::mv::model::MvStorageEngine::Iceberg {
            return Err(format!(
                "REFRESH MATERIALIZED VIEW is only supported for Iceberg-backed materialized views: {}",
                step.object.display_name().trim_start_matches("mv:")
            ));
        }
        let step_statement = crate::sql::parser::ast::RefreshMaterializedViewStmt {
            name: crate::sql::parser::ast::ObjectName {
                parts: vec![step.target.database.clone(), step.target.name.clone()],
            },
            full: false,
        };
        let preparation =
            crate::engine::mv::iceberg_refresh::StandaloneMvRefreshPreparationService::new(
                state,
                step.target.catalog.as_deref(),
                &step.target.database,
                &step_statement,
                connector_context,
            );
        let result = state
            .mv_application_service
            .prepare_and_execute_refresh(
                &preparation,
                crate::mv::application::MvApplicationStatement::Refresh(
                    crate::sql::mv_refresh::MvRefreshStatement::from(&step_statement),
                ),
                step.target.clone(),
                connector_context.clone(),
                request_context.execution(),
            )
            .map_err(|error| {
                if step.object != requested_object {
                    format!(
                        "cannot refresh materialized view {}: upstream materialized view {} failed: {error}",
                        requested_object.display_name().trim_start_matches("mv:"),
                        step.object.display_name().trim_start_matches("mv:")
                    )
                } else {
                    error.to_string()
                }
            })?;
        last_result = Some(match result {
            crate::mv::application::MvStatementResult::Ok => StatementResult::Ok,
            crate::mv::application::MvStatementResult::Query(result) => {
                StatementResult::Query(result)
            }
        });
    }

    let result = last_result.ok_or_else(|| {
        "MV refresh dependency planning produced no target refresh step".to_string()
    })?;
    Ok(result)
}

fn statistics_application_target(
    name: &crate::sql::parser::ast::ObjectName,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<statistics_application::StatisticsTableTarget, String> {
    let default_catalog = current_catalog.unwrap_or("default_catalog");
    let (catalog, namespace, table) = match name.parts.as_slice() {
        [table] => (default_catalog, current_database, table.as_str()),
        [namespace, table] => (default_catalog, namespace.as_str(), table.as_str()),
        [catalog, namespace, table] => (catalog.as_str(), namespace.as_str(), table.as_str()),
        _ => {
            return Err(format!(
                "statistics table name must be table, db.table, or catalog.db.table: {}",
                name.parts.join(".")
            ));
        }
    };
    Ok(statistics_application::StatisticsTableTarget {
        catalog: normalize_identifier(catalog)?,
        namespace: normalize_identifier(namespace)?,
        table: normalize_identifier(table)?,
    })
}

fn execute_statistics_application_command(
    state: &Arc<StandaloneState>,
    command: statistics_application::StatisticsApplicationCommand,
) -> Result<StatementResult, String> {
    let result = state
        .statistics_application
        .execute(command)
        .map_err(|error| error.to_string())?;
    statistics_application_result(result)
}

fn statistics_application_result(
    result: statistics_application::StatisticsApplicationResult,
) -> Result<StatementResult, String> {
    use statistics_application::StatisticsApplicationResult;

    match result {
        StatisticsApplicationResult::JobSubmitted(_)
        | StatisticsApplicationResult::JobCancellationRequested(_) => Ok(StatementResult::Ok),
        StatisticsApplicationResult::AnalyzeJobs(jobs) => statistics_string_result(
            &[
                "job_id",
                "operation_id",
                "state",
                "attempt",
                "catalog",
                "namespace",
                "table",
            ],
            jobs.into_iter()
                .map(|job| {
                    vec![
                        Some(job.job_id.to_string()),
                        Some(job.operation_id.to_string()),
                        Some(job.state),
                        Some(job.attempt.to_string()),
                        Some(job.target.catalog),
                        Some(job.target.namespace),
                        Some(job.target.table),
                    ]
                })
                .collect(),
        ),
        StatisticsApplicationResult::TableStats(rows) => statistics_string_result(
            &["metric", "value", "status"],
            rows.into_iter()
                .map(|row| vec![Some(row.metric), row.value, Some(row.status)])
                .collect(),
        ),
    }
}

fn statistics_string_result(
    names: &[&str],
    rows: Vec<Vec<Option<String>>>,
) -> Result<StatementResult, String> {
    if rows.iter().any(|row| row.len() != names.len()) {
        return Err("statistics application returned malformed tabular result".to_string());
    }
    let columns = names
        .iter()
        .map(|name| QueryResultColumn {
            name: (*name).to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            logical_type: None,
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, true))
            .collect::<Vec<_>>(),
    ));
    let arrays = (0..names.len())
        .map(|column| {
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row[column].clone())
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(schema, arrays)
        .map_err(|error| format!("build statistics application result failed: {error}"))?;
    Ok(StatementResult::Query(QueryResult {
        columns,
        chunks: vec![record_batch_to_chunk(batch)?],
    }))
}

fn require_backend_management_role(
    statement: &str,
    role: crate::common::app_config::ClusterRole,
) -> Result<(), String> {
    match role {
        crate::common::app_config::ClusterRole::Fe => Ok(()),
        crate::common::app_config::ClusterRole::Be => Err(format!(
            "{statement} is not available in role=be; backend management is owned by StarRocks FE"
        )),
        crate::common::app_config::ClusterRole::AllInOne => {
            Err(format!("{statement} requires role=fe"))
        }
    }
}

// ---------------------------------------------------------------------------
// Local parquet table helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Metadata persistence
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
struct ResolvedMetadataBackend {
    provider: crate::common::app_config::MetadataProviderConfig,
    path: PathBuf,
}

fn resolve_metadata_backend(
    opts: &StandaloneOptions,
    cfg: &novarocks_config::NovaRocksConfig,
) -> Result<Option<ResolvedMetadataBackend>, String> {
    if let Some(metadata) = cfg.metadata.as_ref() {
        return Ok(Some(ResolvedMetadataBackend {
            provider: metadata.provider,
            path: resolve_relative_path(&metadata.path, opts.config_path.as_deref())?,
        }));
    }
    Ok(None)
}

fn open_metadata_provider(
    backend: &ResolvedMetadataBackend,
) -> Result<Arc<dyn crate::meta::MetaStoreProvider>, String> {
    match backend.provider {
        crate::common::app_config::MetadataProviderConfig::Sqlite => {
            let provider = crate::meta::SqliteMetaStoreProvider::open(&backend.path)
                .map_err(|err| format!("open sqlite metadata provider failed: {err}"))?;
            Ok(Arc::new(provider))
        }
    }
}

/// Matches the `[standalone_server] mv_partition_state_max_entries` default.
pub(crate) const DEFAULT_MV_PARTITION_STATE_MAX_ENTRIES: usize = 10_000;

fn resolve_mv_refresh_pruning_limits(
    cfg: &novarocks_config::NovaRocksConfig,
) -> MvRefreshPruningLimits {
    cfg.standalone_server
        .as_ref()
        .map(|config| MvRefreshPruningLimits {
            max_touched_groups: config.mv_refresh_max_touched_groups,
            max_affected_partitions: config.mv_refresh_max_affected_partitions,
        })
        .unwrap_or_default()
}

fn resolve_relative_path(path: &Path, config_path: Option<&Path>) -> Result<PathBuf, String> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    if let Some(config_path) = config_path
        && let Some(base_dir) = config_path.parent()
    {
        return Ok(base_dir.join(path));
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(path))
        .map_err(|e| format!("read current directory failed: {e}"))
}

fn restore_metadata_if_needed(state: &Arc<StandaloneState>) -> Result<(), String> {
    restore_iceberg_catalogs(state)?;
    // W4 statelessness: rediscover lake-native Iceberg MV packages that are
    // present on the lake but missing from a fresh `[metadata]` (SQLite) cache,
    // and persist their rebuilt definitions.
    crate::engine::mv::lake_rebuild::rebuild_imv_cache_from_lake(state)?;
    crate::engine::mv::iceberg_refresh::restore_iceberg_mv_targets(state)?;
    // Recovery is a frontend application decision. At this point catalog
    // bindings and target descriptors have both been restored, so the service
    // can acquire one current-generation inspection lease per fenced attempt.
    state
        .mv_application_service
        .recover_startup_mv_refreshes()
        .map_err(|error| format!("frontend MV startup recovery failed: {error}"))?;
    Ok(())
}

fn restore_iceberg_catalogs(state: &Arc<StandaloneState>) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open metadata read transaction failed: {e}"))?;
    let catalogs = state
        .catalog_attachment_repo
        .list(read.as_ref())
        .map_err(|e| format!("load catalog attachment metadata failed: {e}"))?;
    for catalog in &catalogs {
        let normalized_catalog = normalize_identifier(&catalog.catalog)?;
        create_iceberg_control_binding(
            state,
            &normalized_catalog,
            catalog.properties.properties.clone(),
        )?;
        state.catalog_service.register_catalog(
            crate::engine::query_planning::catalog_runtime::build_iceberg_catalog(
                &catalog.catalog,
                Arc::clone(&state.connector_control)
                    as Arc<dyn novarocks_spi::connector::ConnectorControlResolver>,
            ),
        );
    }

    Ok(())
}

fn create_iceberg_control_binding(
    state: &Arc<StandaloneState>,
    normalized_catalog: &str,
    properties: Vec<(String, String)>,
) -> Result<Vec<(String, String)>, String> {
    let provider_id = novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
        .map_err(|error| format!("invalid Iceberg connector provider ID: {error}"))?;
    let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(normalized_catalog)
        .map_err(|error| format!("invalid Iceberg connector instance ID: {error}"))?;
    let request = novarocks_spi::connector::ConnectorControlFactoryRequest::try_new(
        provider_id,
        instance_id,
        properties,
    )
    .map_err(|error| format!("prepare Iceberg connector control factory request: {error}"))?;
    let creation = state
        .connector_control_factory_resolver
        .create_control(request)
        .map_err(|error| format!("create Iceberg connector control binding: {error}"))?;
    let (binding, durable_properties) = creation.into_parts();
    state
        .connector_control
        .register(binding)
        .map_err(|error| format!("register Iceberg connector control binding: {error}"))?;
    Ok(durable_properties)
}

#[cfg(test)]
/// Temporary bridge for provider-semantic tests that SPI-5P T6 must relocate
/// before the final Core provider dependency cut.
pub(crate) fn register_iceberg_control_binding(
    state: &Arc<StandaloneState>,
    normalized_catalog: &str,
) -> Result<(), String> {
    let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(normalized_catalog)
        .map_err(|error| format!("invalid Iceberg connector instance ID: {error}"))?;
    let planning_binding = state
        .connector_file_planning_resources
        .clone()
        .map(novarocks_connector_iceberg::access_binding::IcebergReadBinding::from_resources);
    let binding = crate::connector::iceberg::provider::IcebergControlProvider::new_control_with_planning_binding(
        instance_id,
        Arc::clone(&state.iceberg_catalogs),
        planning_binding,
    )
    .map_err(|error| format!("create test Iceberg connector control binding: {error}"))?;
    state
        .connector_control
        .register(binding)
        .map_err(|error| format!("register test Iceberg connector control binding: {error}"))
}

fn retire_iceberg_control_binding(
    state: &Arc<StandaloneState>,
    normalized_catalog: &str,
) -> Result<(), String> {
    let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(normalized_catalog)
        .map_err(|error| format!("invalid Iceberg connector instance ID: {error}"))?;
    state
        .connector_control
        .retire_current(&instance_id)
        .map_err(|error| format!("retire Iceberg connector control binding: {error}"))
}

pub(crate) fn persist_catalog_attachment_if_needed(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
    properties: &[(String, String)],
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let mut txn = provider
        .begin_write("persist catalog attachment")
        .map_err(|e| format!("open metadata write transaction failed: {e}"))?;
    state
        .catalog_attachment_repo
        .create(
            txn.as_mut(),
            catalog_name,
            CatalogAttachmentProperties {
                properties: properties.to_vec(),
            },
        )
        .map_err(|e| format!("persist catalog attachment metadata failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit catalog attachment metadata failed: {e}"))?;
    Ok(())
}

pub(crate) fn delete_catalog_attachment_if_needed(
    state: &Arc<StandaloneState>,
    catalog_name: &str,
) -> Result<(), String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(());
    };
    let mut txn = provider
        .begin_write("delete catalog attachment")
        .map_err(|e| format!("open metadata write transaction failed: {e}"))?;
    state
        .catalog_attachment_repo
        .delete_current(txn.as_mut(), catalog_name)
        .map_err(|e| format!("delete catalog attachment metadata failed: {e}"))?;
    txn.commit()
        .map_err(|e| format!("commit catalog attachment metadata failed: {e}"))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

pub(crate) fn block_on_standalone_async<F>(future: F) -> Result<F::Output, String>
where
    F: std::future::Future,
{
    if let Ok(handle) = Handle::try_current() {
        return Ok(handle.block_on(future));
    }
    data_block_on(future)
}

// ---------------------------------------------------------------------------
// Query plan build + execute (delegates to crate::sql::*)
// ---------------------------------------------------------------------------

fn ensure_mainline_distributed_execution(
    has_terminal_sink: bool,
    has_iceberg_catalogs: bool,
    exchange_port: u16,
) -> Result<(), String> {
    if has_terminal_sink {
        return Err(
            "terminal sink execution requires mainline DistributedPlan sink support; direct execution fallback was removed"
                .to_string(),
        );
    }
    if has_iceberg_catalogs {
        return Err(
            "local Iceberg registry execution requires mainline DistributedPlan write support; direct execution fallback was removed"
                .to_string(),
        );
    }
    if exchange_port == 0 {
        return Err(
            "distributed execution requires an exchange backend; tests must install a loopback exchange backend instead of direct fallback"
                .to_string(),
        );
    }
    Ok(())
}

fn optimizer_settings_for_execution(
    execution: Option<&crate::query_execution::request_context::QueryExecutionContext>,
) -> crate::sql::optimizer::options::SessionOptimizerSettings {
    let mut settings = execution
        .map(|execution| execution.optimizer_settings().clone())
        .unwrap_or_default();
    if settings.cbo_broadcast_backend_count.is_none() {
        if let Some(execution) = execution {
            settings.effective_backend_count = Some(execution.topology().targets().len() as f64);
        }
    }
    settings
}

fn scan_preparation_options(
    settings: &crate::sql::optimizer::options::SessionOptimizerSettings,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
) -> Result<crate::query_execution::preparation::ScanPreparationOptions, String> {
    let target_parallelism = std::num::NonZeroUsize::new(execution.topology().targets().len())
        .or_else(|| {
            // Unit fixtures deliberately use an empty synthetic topology. A
            // production request must instead fail before provider planning.
            #[cfg(test)]
            {
                Some(std::num::NonZeroUsize::new(1).expect("one is non-zero"))
            }
            #[cfg(not(test))]
            {
                None
            }
        })
        .ok_or_else(|| {
            "connector split preparation requires a non-empty admitted backend topology".to_string()
        })?;
    Ok(
        crate::query_execution::preparation::ScanPreparationOptions::new(
            settings.connector_static_predicate_pushdown_enabled(),
            target_parallelism,
            None,
        ),
    )
}

fn connector_static_planning_metrics(
    prepared: &crate::query_execution::preparation::PreparedFragmentSet,
) -> Result<crate::query_execution::profile::ConnectorStaticPlanningMetrics, String> {
    let mut metrics = crate::query_execution::profile::ConnectorStaticPlanningMetrics::default();
    for read in prepared.scan_bindings().connector_reads() {
        metrics.record(read.planning_metrics)?;
    }
    Ok(metrics)
}

pub(crate) fn capture_maintenance_request_context(
    state: &StandaloneState,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<crate::query_execution::request_context::RequestContext, String> {
    let topology = state
        .backend_topology
        .snapshot()
        .map_err(|error| error.to_string())?;
    let cancellation = crate::query_execution::cancellation::QueryCancellationSource::new();
    Ok(
        crate::query_execution::request_context::RequestContext::admit(
            crate::query_execution::request_context::RequestAdmission::new(
                current_catalog.map(str::to_string),
                current_database.to_string(),
                state.execution_role,
                topology,
                None,
                cancellation.view(),
                crate::sql::optimizer::options::SessionOptimizerSettings::default(),
            ),
        ),
    )
}

pub(crate) fn capture_maintenance_execution(
    state: &StandaloneState,
) -> Result<crate::query_execution::request_context::QueryExecutionContext, String> {
    Ok(capture_maintenance_request_context(state, None, "default")?
        .execution()
        .clone())
}

/// Common preparation pipeline shared by `EXPLAIN` and `EXPLAIN ANALYZE`.
fn prepare_explain_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<sqlparser::ast::Query, String> {
    let mut prepared = query.clone();
    state.view_service.rewrite_query(
        state.as_ref(),
        &mut prepared,
        crate::engine::view::ViewRequestContext {
            current_catalog,
            current_database,
            connector_context: Some(connector_context),
        },
    )?;

    // Time-travel refs become synthetic local tables. Ordinary Iceberg refs
    // remain untouched and resolve through the query catalog materializer during analysis.
    if has_time_travel_refs(&prepared) {
        rewrite_time_travel_refs(
            state,
            current_catalog,
            current_database,
            &mut prepared,
            connector_context,
        )?;
    }

    Ok(prepared)
}

fn query_options_for_explain_analyze(query_options: Option<QueryOptions>) -> QueryOptions {
    let mut query_options = query_options.unwrap_or_default();
    query_options.enable_profile = true;
    query_options
}

const ICEBERG_RUNTIME_FILE_PRUNING_COUNTER_NAMES: &[&str] = &[
    "IcebergRuntimeFilePruning/FilesTotal",
    "IcebergRuntimeFilePruning/FilesSelected",
    "IcebergRuntimeFilePruning/FilesPruned",
    "IcebergRuntimeFilePruning/Predicates",
    "IcebergRuntimeFilePruning/Unsupported",
    "IcebergRuntimeFilePruning/Unavailable",
];

const RUNTIME_FILTER_SCAN_UNIT_COUNTER_NAMES: &[&str] = &[
    "RuntimeFilterScanUnitsPruned",
    "RuntimeFilterScanUnitsKept",
    "RuntimeFilterScanUnitsNotEvaluated",
    "RuntimeFilterScanUnitsNotEvaluatedUnitFactsMissing",
    "RuntimeFilterScanUnitsNotEvaluatedColumnFactsMissing",
    "RuntimeFilterScanUnitsNotEvaluatedDataTypeUnsupported",
    "RuntimeFilterScanUnitsNotEvaluatedPredicateCapabilityUnsupported",
    "RuntimeFilterScanUnitsNotEvaluatedResourceUnavailable",
    "RuntimeFilterScanUnitsNotEvaluatedSnapshotUnavailable",
    "RuntimeFilterScanUnitsNotEvaluatedSnapshotTimedOut",
    "RuntimeFilterScanUnitsNotEvaluatedSnapshotNotPublished",
];

const CONNECTOR_FILE_ROW_GROUP_COUNTER_NAMES: &[&str] = &[
    "ConnectorFileRowGroupsRead",
    "ConnectorFileRowGroupsPruned",
    "ConnectorUnitReadersOpened",
];

fn format_distributed_profile_summary(
    summary: &crate::query_execution::profile::DistributedProfileSummary,
) -> String {
    format!(
        "Profile: fragments={} fragment_wall_max={} fragment_wall_sum={} driver_total={} driver_blocked={} source_wait={} sink_wait={} dependency_wait={} operator_active={} exchange_wait={} exchange_process={} network={} scan_io={}",
        summary.fragment_instance_count,
        format_explain_analyze_duration_ns(summary.fragment_wall_max_ns),
        format_explain_analyze_duration_ns(summary.fragment_wall_sum_ns),
        format_explain_analyze_duration_ns(summary.driver_total_time_ns),
        format_explain_analyze_duration_ns(summary.driver_blocked_time_ns),
        format_explain_analyze_duration_ns(summary.source_wait_time_ns),
        format_explain_analyze_duration_ns(summary.sink_wait_time_ns),
        format_explain_analyze_duration_ns(summary.dependency_wait_time_ns),
        format_explain_analyze_duration_ns(summary.operator_active_time_ns),
        format_explain_analyze_duration_ns(summary.exchange_wait_time_ns),
        format_explain_analyze_duration_ns(summary.exchange_process_time_ns),
        format_explain_analyze_duration_ns(summary.network_time_ns),
        format_explain_analyze_duration_ns(summary.scan_io_time_ns)
    )
}

fn format_explain_analyze_duration_ns(ns: i64) -> String {
    format_explain_analyze_duration(std::time::Duration::from_nanos(ns.max(0) as u64))
}

fn format_explain_analyze_duration(duration: std::time::Duration) -> String {
    let ms = duration.as_secs_f64() * 1000.0;
    if ms < 1.0 {
        format!("{ms:.3}ms")
    } else if ms < 1000.0 {
        format!("{ms:.1}ms")
    } else {
        format!("{:.2}s", duration.as_secs_f64())
    }
}

pub(crate) fn execute_query_with_catalog_service(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    query_opts: Option<QueryOptions>,
) -> Result<QueryResult, String> {
    let connector_context = crate::connector::connector_request_context(
        query_opts.as_ref(),
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )?;
    execute_query_with_catalog_service_with_connector_context(
        state,
        current_catalog,
        current_database,
        query,
        query_opts,
        &connector_context,
    )
}

pub(crate) fn execute_query_with_catalog_service_with_connector_context(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    query_opts: Option<QueryOptions>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<QueryResult, String> {
    let catalog_service_snapshot = catalog_service_snapshot(state);
    let analyzer_provider = build_catalog_service_provider(
        current_catalog,
        &catalog_service_snapshot,
        state.connector_control.as_ref(),
        connector_context.clone(),
        TableLookupMode::SchemaOnly,
    );
    let execution = capture_maintenance_execution(state)?;
    let (request, _, _) = prepare_query_with_sql_compiler_kernel(
        query,
        &analyzer_provider,
        current_catalog,
        current_database,
        state,
        connector_context,
        query_opts,
        &execution,
        crate::sql::compiler::SqlCompileIntent::Query,
        true,
    )?;
    state
        .query_execution
        .execute(request)
        .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_result)
        .map(crate::query_execution::outcome::ResultExecutionOutcome::into_query_result)
        .map_err(|error| error.to_string())
}

pub(crate) fn execute_query_with_catalog_service_with_execution(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    query_opts: Option<QueryOptions>,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<QueryResult, String> {
    let catalog_service_snapshot = catalog_service_snapshot(state);
    let analyzer_provider = build_catalog_service_provider(
        current_catalog,
        &catalog_service_snapshot,
        state.connector_control.as_ref(),
        connector_context.clone(),
        TableLookupMode::SchemaOnly,
    );
    let (request, _, _) = prepare_query_with_sql_compiler_kernel(
        query,
        &analyzer_provider,
        current_catalog,
        current_database,
        state,
        connector_context,
        query_opts,
        execution,
        crate::sql::compiler::SqlCompileIntent::Query,
        true,
    )?;
    state
        .query_execution
        .execute(request)
        .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_result)
        .map(crate::query_execution::outcome::ResultExecutionOutcome::into_query_result)
        .map_err(|error| error.to_string())
}

/// Execute a refresh query whose SQL has already been expanded by the MV
/// refresh path. These reads must not be considered for automatic MV rewrite:
/// doing so can rewrite a first refresh back to its own still-empty target.
pub(crate) fn execute_preexpanded_mv_refresh_query_with_catalog_service_with_connector_context(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    query_opts: Option<QueryOptions>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<QueryResult, String> {
    let catalog_service_snapshot = catalog_service_snapshot(state);
    let analyzer_provider = build_catalog_service_provider(
        current_catalog,
        &catalog_service_snapshot,
        state.connector_control.as_ref(),
        connector_context.clone(),
        TableLookupMode::SchemaOnly,
    );
    let maintenance_execution = capture_maintenance_execution(state)?;
    let (request, _, _) = prepare_query_with_sql_compiler_kernel(
        query,
        &analyzer_provider,
        current_catalog,
        current_database,
        state,
        connector_context,
        query_opts,
        &maintenance_execution,
        crate::sql::compiler::SqlCompileIntent::Query,
        false,
    )?;
    state
        .query_execution
        .execute(request)
        .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_result)
        .map(crate::query_execution::outcome::ResultExecutionOutcome::into_query_result)
        .map_err(|error| error.to_string())
}

pub(crate) fn iceberg_write_shuffle_by_output_name(
    output_name: impl Into<String>,
) -> crate::sql::compiler::RootDistributionRequirement {
    crate::sql::compiler::RootDistributionRequirement::ShuffleOutputName(output_name.into())
}

pub(crate) fn iceberg_write_shuffle_by_output_index(
    output_index: usize,
) -> crate::sql::compiler::RootDistributionRequirement {
    crate::sql::compiler::RootDistributionRequirement::ShuffleOutputOrdinal(output_index)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_query_as_iceberg_write(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    sink: crate::sql::planner::distributed::write::contract::SqlWritePlanInput,
    table_bindings: Arc<crate::engine::query_planning::bindings::QueryTableBindingStore>,
    query_opts: Option<QueryOptions>,
    root_distribution: crate::sql::compiler::RootDistributionRequirement,
    execution: Option<&crate::query_execution::request_context::QueryExecutionContext>,
) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
    // This public write helper is also used by non-session transaction executors,
    // so it owns an operation-scoped context when no request signal is available.
    let connector_context = crate::connector::connector_request_context(
        query_opts.as_ref(),
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )?;
    execute_query_as_iceberg_write_with_connector_context(
        state,
        current_catalog,
        current_database,
        query,
        sink,
        table_bindings,
        query_opts,
        root_distribution,
        execution,
        &connector_context,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_query_as_iceberg_write_with_connector_context(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    sink: crate::sql::planner::distributed::write::contract::SqlWritePlanInput,
    table_bindings: Arc<crate::engine::query_planning::bindings::QueryTableBindingStore>,
    query_opts: Option<QueryOptions>,
    root_distribution: crate::sql::compiler::RootDistributionRequirement,
    execution: Option<&crate::query_execution::request_context::QueryExecutionContext>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    connector_write: Option<crate::query_execution::contract::ConnectorWritePlanningTemplate>,
) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
    execute_query_as_iceberg_write_with_connector_binding(
        state,
        current_catalog,
        current_database,
        query,
        sink,
        table_bindings,
        query_opts,
        root_distribution,
        execution,
        connector_context,
        connector_write.map(DistributedConnectorWrite::Begin),
        &[],
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_query_as_iceberg_write_in_operation_with_connector_context(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    sink: crate::sql::planner::distributed::write::contract::SqlWritePlanInput,
    table_bindings: Arc<crate::engine::query_planning::bindings::QueryTableBindingStore>,
    query_opts: Option<QueryOptions>,
    root_distribution: crate::sql::compiler::RootDistributionRequirement,
    execution: Option<&crate::query_execution::request_context::QueryExecutionContext>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    connector_write: crate::query_execution::contract::ConnectorWriteExecutionRegistration,
) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
    execute_query_as_iceberg_write_with_connector_binding(
        state,
        current_catalog,
        current_database,
        query,
        sink,
        table_bindings,
        query_opts,
        root_distribution,
        execution,
        connector_context,
        Some(DistributedConnectorWrite::Sealed(connector_write)),
        &[],
    )
}

/// Execute one generated write query with request-local relation overlays.
/// This is used by COW rewrite slices whose frozen file input is not a durable
/// catalog table.  The overlay is consumed by the application materializer
/// and is never registered in the shared local catalog.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_query_as_iceberg_write_in_operation_with_query_local_overlays(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    sink: crate::sql::planner::distributed::write::contract::SqlWritePlanInput,
    table_bindings: Arc<crate::engine::query_planning::bindings::QueryTableBindingStore>,
    query_opts: Option<QueryOptions>,
    root_distribution: crate::sql::compiler::RootDistributionRequirement,
    execution: Option<&crate::query_execution::request_context::QueryExecutionContext>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    connector_write: crate::query_execution::contract::ConnectorWriteExecutionRegistration,
    overlays: &[crate::engine::query_planning::catalog_materializer::QueryLocalTableOverlay],
) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
    execute_query_as_iceberg_write_with_connector_binding(
        state,
        current_catalog,
        current_database,
        query,
        sink,
        table_bindings,
        query_opts,
        root_distribution,
        execution,
        connector_context,
        Some(DistributedConnectorWrite::Sealed(connector_write)),
        overlays,
    )
}

/// Prepare a typed MV logical append as the same inert distributed write
/// request used by SQL-shaped connector writes.  It performs no submission or
/// external mutation; the supplied template is activated from the frontend's
/// retained exact lease and will be sealed there into one write session.  The
/// SQL compiler receives the same admitted bindings, statistics, topology,
/// deadline, and cancellation observation as every other production write.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_logical_plan_as_iceberg_write_with_connector_binding(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    logical_plan: crate::sql::planner::logical::LogicalPlanNode,
    factory: crate::sql::column_id::ColumnRefFactory,
    sink: crate::sql::planner::distributed::write::contract::SqlWritePlanInput,
    root_distribution: crate::sql::compiler::RootDistributionRequirement,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    table_bindings: Arc<crate::engine::query_planning::bindings::QueryTableBindingStore>,
    connector_write: crate::query_execution::contract::ConnectorWritePlanningTemplate,
) -> Result<crate::query_execution::prepared_write::PreparedDistributedWriteRequest, String> {
    crate::connector::validate_request_context(connector_context)?;
    let optimizer_settings = optimizer_settings_for_execution(Some(execution));
    let statistics = query_stats::QueryStatisticsContext::from_standalone_state_with_bindings(
        state,
        Arc::clone(&table_bindings),
    );
    let backend_count = std::num::NonZeroUsize::new(execution.topology().targets().len())
        .ok_or_else(|| {
            "MV first-refresh write requires a non-empty admitted backend topology".to_string()
        })?;
    let request = crate::sql::compiler::SqlCompileRequest::new_logical(
        logical_plan,
        factory,
        crate::sql::compiler::SqlCompileIntent::IcebergWrite { root_distribution },
        crate::sql::compiler::SqlSessionContext {
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
            optimizer_settings: execution.optimizer_settings().clone(),
        },
        crate::sql::compiler::SqlPlanningEnvironment::Distributed { backend_count },
        &statistics,
        crate::sql::compiler::SqlCompileControl::new(
            execution.deadline(),
            crate::engine::query_planning::sql_cancellation_observation(
                execution.cancellation().clone(),
            ),
        ),
    );
    let crate::sql::compiler::SqlCompileOutput::Optimized(compiled) =
        crate::sql::compiler::SqlCompiler::compile(request).map_err(|error| error.to_string())?
    else {
        return Err(
            "MV first-refresh logical write did not produce optimized SQL facts".to_string(),
        );
    };
    let physical_plan =
        crate::sql::planner::optimizer_bridge::to_physical_plan(&compiled.optimized_tree)?;
    let distributed_plan =
        crate::sql::planner::pipeline::build_sql_write_distributed_plan_with_settings(
            physical_plan,
            sink,
            &optimizer_settings,
        )?;
    let scan_resolver =
        crate::engine::query_planning::delta_scan::QueryTableBindingScanResolver::new(
            table_bindings.as_ref(),
        );
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed_plan,
        state.connector_control.as_ref(),
        connector_context,
        Some(table_bindings.as_ref()),
        Some(&scan_resolver),
        scan_preparation_options(&optimizer_settings, execution)?,
    )?;
    let native_bundle = crate::protocol::native::encode::encode_native_fragment_bundle(
        &distributed_plan,
        &prepared,
    )?;
    prepare_distributed_write_request_with_execution(
        prepared,
        native_bundle,
        None,
        execution,
        Some(DistributedConnectorWrite::Begin(connector_write)),
    )
}

/// Execute one provider-frozen rewrite source through the ordinary C1 sink.
/// The physical source is deliberately `ConnectorPinned`: its one-shot
/// resolver supplies opaque splits already planned by the exact composite
/// rewrite lease, so this path cannot reopen a current catalog binding.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_frozen_rewrite_physical_plan_as_iceberg_staging(
    state: &Arc<StandaloneState>,
    physical_plan: crate::sql::planner::physical::PhysicalPlanNode,
    sink: crate::sql::planner::distributed::write::contract::SqlWritePlanInput,
    execution: Option<&crate::query_execution::request_context::QueryExecutionContext>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    table_bindings: &crate::engine::query_planning::bindings::QueryTableBindingStore,
    scan_resolver: &dyn crate::query_execution::preparation::scan::ScanBindingResolver,
    connector_write: crate::query_execution::contract::ConnectorWriteExecutionRegistration,
) -> Result<
    (
        crate::query_execution::ConnectorWriteCompletion,
        crate::query_execution::ConnectorWriteStagingSummary,
    ),
    String,
> {
    crate::connector::validate_request_context(connector_context)?;
    let maintenance_execution;
    let execution = match execution {
        Some(execution) => execution,
        None => {
            maintenance_execution = capture_maintenance_execution(state)?;
            &maintenance_execution
        }
    };
    let optimizer_settings = optimizer_settings_for_execution(Some(execution));
    let distributed_plan =
        crate::sql::planner::pipeline::build_sql_write_distributed_plan_with_settings(
            physical_plan,
            sink,
            &optimizer_settings,
        )?;
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed_plan,
        state.connector_control.as_ref(),
        connector_context,
        Some(table_bindings),
        Some(scan_resolver),
        scan_preparation_options(&optimizer_settings, execution)?,
    )?;
    let native_bundle = crate::protocol::native::encode::encode_native_fragment_bundle(
        &distributed_plan,
        &prepared,
    )?;
    let result = execute_distributed_write_with_execution(
        &state.query_execution,
        prepared,
        native_bundle,
        None,
        execution,
        Some(DistributedConnectorWrite::Sealed(connector_write)),
    )?;
    connector_staging_completion_from_result(result)
}

fn connector_staging_completion_from_result(
    result: crate::query_execution::outcome::QueryExecutionResult,
) -> Result<
    (
        crate::query_execution::ConnectorWriteCompletion,
        crate::query_execution::ConnectorWriteStagingSummary,
    ),
    String,
> {
    if !result.query_result.columns.is_empty() || !result.query_result.chunks.is_empty() {
        return Err("connector staging terminal returned a result payload".to_string());
    }
    if let Some(abort) = result.write_abort {
        return Err(format!(
            "connector staging terminal aborted: {}",
            abort.reason
        ));
    }
    let completion = result.connector_completion.ok_or_else(|| {
        "connector staging terminal has no accepted connector completion".to_string()
    })?;
    let summary = completion
        .staging_summary()
        .map_err(|error| error.to_string())?;
    Ok((completion, summary))
}

pub(crate) enum DistributedConnectorWrite {
    Begin(crate::query_execution::contract::ConnectorWritePlanningTemplate),
    Sealed(crate::query_execution::contract::ConnectorWriteExecutionRegistration),
}

#[allow(clippy::too_many_arguments)]
fn execute_query_as_iceberg_write_with_connector_binding(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    sink: crate::sql::planner::distributed::write::contract::SqlWritePlanInput,
    table_bindings: Arc<crate::engine::query_planning::bindings::QueryTableBindingStore>,
    query_opts: Option<QueryOptions>,
    root_distribution: crate::sql::compiler::RootDistributionRequirement,
    execution: Option<&crate::query_execution::request_context::QueryExecutionContext>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    connector_write: Option<DistributedConnectorWrite>,
    query_local_overlays: &[crate::engine::query_planning::catalog_materializer::QueryLocalTableOverlay],
) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
    let maintenance_execution;
    let execution = match execution {
        Some(execution) => execution,
        None => {
            maintenance_execution = capture_maintenance_execution(state)?;
            &maintenance_execution
        }
    };
    let optimizer_settings = optimizer_settings_for_execution(Some(execution));
    // Time-travel: a branch DML write's scan carries `FOR VERSION AS OF '<branch>'`
    // (delete_flow's DV position scan; the MOR-UPDATE branch row scan). Resolve those
    // version-bearing refs to synthetic per-snapshot tables bound to the BRANCH head
    // BEFORE snapshotting the catalog, exactly as the read path does. Without this the
    // analyzer silently drops the version clause and the scan reads the table's current
    // (main) snapshot, so a branch DELETE/UPDATE finds rows in the wrong data files and
    // no-ops on the branch. No-op when the query has no version ref (INSERT / main
    // writes), so those paths are unchanged.
    let mut prepared = query.clone();
    if has_time_travel_refs(&prepared) {
        rewrite_time_travel_refs(
            state,
            current_catalog,
            current_database,
            &mut prepared,
            connector_context,
        )?;
    }

    let catalog_service_snapshot = catalog_service_snapshot(state);
    let analyzer_provider = build_catalog_service_provider_with_bindings_and_query_local_overlays(
        current_catalog,
        &catalog_service_snapshot,
        state.connector_control.as_ref(),
        connector_context.clone(),
        Arc::clone(&table_bindings),
        query_local_overlays.to_vec(),
    );

    let resolved_bindings = analyzer_provider.query_table_bindings();
    if !Arc::ptr_eq(&table_bindings, &resolved_bindings) {
        return Err(
            "SQL write catalog materializer replaced the admitted binding store".to_string(),
        );
    }
    let statistics = query_stats::QueryStatisticsContext::from_standalone_state_with_bindings(
        state,
        Arc::clone(&table_bindings),
    );
    let catalog_snapshot = crate::sql::compiler::SqlPlannerTableSnapshot::new(&analyzer_provider);
    let backend_count = std::num::NonZeroUsize::new(execution.topology().targets().len())
        .ok_or_else(|| {
            "Iceberg write requires a non-empty admitted backend topology".to_string()
        })?;
    let compiler_request = crate::sql::compiler::SqlCompileRequest::new(
        crate::sql::compiler::SqlStatementInput::ParsedQuery(Box::new(prepared)),
        crate::sql::compiler::SqlCompileIntent::IcebergWrite { root_distribution },
        crate::sql::compiler::SqlSessionContext {
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
            optimizer_settings: execution.optimizer_settings().clone(),
        },
        crate::sql::compiler::SqlPlanningEnvironment::Distributed { backend_count },
        &catalog_snapshot,
        &statistics,
        crate::sql::functions::builtin_sql_function_catalog(),
        None,
        crate::sql::compiler::SqlCompileControl::new(
            execution.deadline(),
            crate::engine::query_planning::sql_cancellation_observation(
                execution.cancellation().clone(),
            ),
        ),
    );
    let crate::sql::compiler::SqlCompileOutput::Optimized(compiled) =
        crate::sql::compiler::SqlCompiler::compile(compiler_request)
            .map_err(|error| error.to_string())?
    else {
        return Err("Iceberg write intent did not produce optimized SQL facts".to_string());
    };
    let optimized_tree = compiled.optimized_tree;
    let physical_plan = crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)?;
    let distributed_plan =
        crate::sql::planner::pipeline::build_sql_write_distributed_plan_with_settings(
            physical_plan,
            sink,
            &optimizer_settings,
        )?;
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed_plan,
        state.connector_control.as_ref(),
        &connector_context,
        Some(table_bindings.as_ref()),
        None,
        scan_preparation_options(&optimizer_settings, execution)?,
    )?;
    let native_bundle = crate::protocol::native::encode::encode_native_fragment_bundle(
        &distributed_plan,
        &prepared,
    )?;
    execute_distributed_write_with_execution(
        &state.query_execution,
        prepared,
        native_bundle,
        query_opts,
        execution,
        connector_write,
    )
}

/// Freeze a native connector-write request without starting a writer. The
/// application owner later seals it through the exact retained write lease.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_query_as_iceberg_write_with_connector_binding(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    sink: crate::sql::planner::distributed::write::contract::SqlWritePlanInput,
    table_bindings: Arc<crate::engine::query_planning::bindings::QueryTableBindingStore>,
    query_opts: Option<QueryOptions>,
    root_distribution: Option<crate::sql::compiler::RootDistributionRequirement>,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    connector_write: crate::query_execution::contract::ConnectorWritePlanningTemplate,
) -> Result<PreparedDistributedWriteRequest, String> {
    let optimizer_settings = optimizer_settings_for_execution(Some(execution));
    let mut prepared_query = query.clone();
    if has_time_travel_refs(&prepared_query) {
        rewrite_time_travel_refs(
            state,
            current_catalog,
            current_database,
            &mut prepared_query,
            connector_context,
        )?;
    }
    let catalog_service_snapshot = catalog_service_snapshot(state);
    let analyzer_provider = build_catalog_service_provider_with_bindings_and_query_local_overlays(
        current_catalog,
        &catalog_service_snapshot,
        state.connector_control.as_ref(),
        connector_context.clone(),
        Arc::clone(&table_bindings),
        Vec::new(),
    );
    let resolved_bindings = analyzer_provider.query_table_bindings();
    if !Arc::ptr_eq(&table_bindings, &resolved_bindings) {
        return Err(
            "SQL write catalog materializer replaced the admitted binding store".to_string(),
        );
    }
    let statistics = query_stats::QueryStatisticsContext::from_standalone_state_with_bindings(
        state,
        Arc::clone(&table_bindings),
    );
    let catalog_snapshot = crate::sql::compiler::SqlPlannerTableSnapshot::new(&analyzer_provider);
    let backend_count = std::num::NonZeroUsize::new(execution.topology().targets().len())
        .ok_or_else(|| {
            "Iceberg write requires a non-empty admitted backend topology".to_string()
        })?;
    let compiler_request = crate::sql::compiler::SqlCompileRequest::new(
        crate::sql::compiler::SqlStatementInput::ParsedQuery(Box::new(prepared_query)),
        crate::sql::compiler::SqlCompileIntent::IcebergWrite {
            root_distribution: root_distribution
                .unwrap_or(crate::sql::compiler::RootDistributionRequirement::Any),
        },
        crate::sql::compiler::SqlSessionContext {
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
            optimizer_settings: execution.optimizer_settings().clone(),
        },
        crate::sql::compiler::SqlPlanningEnvironment::Distributed { backend_count },
        &catalog_snapshot,
        &statistics,
        crate::sql::functions::builtin_sql_function_catalog(),
        None,
        crate::sql::compiler::SqlCompileControl::new(
            execution.deadline(),
            crate::engine::query_planning::sql_cancellation_observation(
                execution.cancellation().clone(),
            ),
        ),
    );
    let crate::sql::compiler::SqlCompileOutput::Optimized(compiled) =
        crate::sql::compiler::SqlCompiler::compile(compiler_request)
            .map_err(|error| error.to_string())?
    else {
        return Err("Iceberg write intent did not produce optimized SQL facts".to_string());
    };
    let physical_plan =
        crate::sql::planner::optimizer_bridge::to_physical_plan(&compiled.optimized_tree)?;
    let distributed_plan =
        crate::sql::planner::pipeline::build_sql_write_distributed_plan_with_settings(
            physical_plan,
            sink,
            &optimizer_settings,
        )?;
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed_plan,
        state.connector_control.as_ref(),
        connector_context,
        Some(table_bindings.as_ref()),
        None,
        scan_preparation_options(&optimizer_settings, execution)?,
    )?;
    let native_bundle = crate::protocol::native::encode::encode_native_fragment_bundle(
        &distributed_plan,
        &prepared,
    )?;
    let cohort_id = connector_write.cohort_id();
    let exact_lease = connector_write.lease();
    PreparedDistributedWriteRequest::new(
        prepared,
        native_bundle,
        query_opts,
        crate::query_execution::contract::ConnectorWriteOperationRegistration::single(
            connector_write,
        ),
        cohort_id,
        exact_lease,
    )
    .map_err(|error| error.to_string())
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChangeStreamWriteEntrypoint {
    PhysicalPlan,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
struct ChangeStreamWriteBuildObservation {
    entrypoint: ChangeStreamWriteEntrypoint,
    effects: Vec<novarocks_spi::connector::ConnectorRowMutationEffect>,
    writer_fragment_ids: Vec<Option<crate::sql::planner::distributed::FragmentId>>,
}

#[cfg(test)]
#[derive(Debug)]
struct ChangeStreamWriteTestObserverState {
    short_circuit_after_build: bool,
    observations: Vec<ChangeStreamWriteBuildObservation>,
}

#[cfg(test)]
fn change_stream_write_test_observer()
-> &'static std::sync::Mutex<Option<ChangeStreamWriteTestObserverState>> {
    static OBSERVER: std::sync::OnceLock<
        std::sync::Mutex<Option<ChangeStreamWriteTestObserverState>>,
    > = std::sync::OnceLock::new();
    OBSERVER.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
pub(crate) struct ChangeStreamWriteTestObserverGuard;

#[cfg(test)]
impl ChangeStreamWriteTestObserverGuard {
    fn take_observations(&self) -> Vec<ChangeStreamWriteBuildObservation> {
        change_stream_write_test_observer()
            .lock()
            .expect("change-stream write test observer lock")
            .as_mut()
            .expect("change-stream write test observer installed")
            .observations
            .drain(..)
            .collect()
    }
}

#[cfg(test)]
impl Drop for ChangeStreamWriteTestObserverGuard {
    fn drop(&mut self) {
        *change_stream_write_test_observer()
            .lock()
            .expect("change-stream write test observer lock") = None;
    }
}

#[cfg(test)]
pub(crate) fn install_change_stream_write_test_observer(
    short_circuit_after_build: bool,
) -> ChangeStreamWriteTestObserverGuard {
    let mut observer = change_stream_write_test_observer()
        .lock()
        .expect("change-stream write test observer lock");
    assert!(
        observer.is_none(),
        "change-stream write test observer already installed"
    );
    *observer = Some(ChangeStreamWriteTestObserverState {
        short_circuit_after_build,
        observations: Vec::new(),
    });
    ChangeStreamWriteTestObserverGuard
}

#[cfg(test)]
pub(crate) fn observe_change_stream_write_build_for_test(
    topology: &crate::sql::planner::distributed::write::change_stream::SqlChangeStreamWriteTopology,
) -> Option<crate::query_execution::outcome::QueryExecutionResult> {
    let mut observer = change_stream_write_test_observer()
        .lock()
        .expect("change-stream write test observer lock");
    let observer = observer.as_mut()?;
    observer
        .observations
        .push(ChangeStreamWriteBuildObservation {
            entrypoint: ChangeStreamWriteEntrypoint::PhysicalPlan,
            effects: topology
                .writer_routes
                .iter()
                .flat_map(|route| route.accepted_effects.iter().copied())
                .collect(),
            writer_fragment_ids: topology
                .writer_routes
                .iter()
                .map(|route| Some(route.writer_fragment_id))
                .collect(),
        });
    if observer.short_circuit_after_build {
        Some(crate::query_execution::outcome::QueryExecutionResult {
            query_result: crate::runtime::query_result::QueryResult::empty(),
            write_commit: None,
            write_abort: None,
            connector_completion: None,
            fragment_profiles: Vec::new(),
        })
    } else {
        None
    }
}

pub(crate) struct PlannedIcebergChangeStreamWrite {
    pub(crate) prepared: crate::query_execution::preparation::PreparedFragmentSet,
    pub(crate) native_bundle: crate::protocol::native::encode::NativeFragmentBundle,
    pub(crate) topology:
        crate::sql::planner::distributed::write::change_stream::SqlChangeStreamWriteTopology,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_physical_plan_as_iceberg_change_stream_write(
    state: &Arc<StandaloneState>,
    _current_catalog: Option<&str>,
    current_database: &str,
    optimized_tree: &crate::sql::optimizer::OptimizedOperatorNode,
    query_table_bindings: Option<&crate::engine::query_planning::bindings::QueryTableBindingStore>,
    dag: &mut crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec,
    pre_expand_keyed_assert: Option<crate::sql::planner::physical::PreExpandKeyedAssertSpec>,
) -> Result<PlannedIcebergChangeStreamWrite, String> {
    // Change-stream planning can run from an MV worker without a client request.
    // Its caller-visible boundary therefore owns this bounded operation context.
    let connector_context = crate::connector::connector_request_context(
        None,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )?;
    build_physical_plan_as_iceberg_change_stream_write_with_connector_context(
        state,
        _current_catalog,
        current_database,
        optimized_tree,
        query_table_bindings,
        dag,
        pre_expand_keyed_assert,
        &connector_context,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_physical_plan_as_iceberg_change_stream_write_with_connector_context(
    state: &Arc<StandaloneState>,
    _current_catalog: Option<&str>,
    _current_database: &str,
    optimized_tree: &crate::sql::optimizer::OptimizedOperatorNode,
    query_table_bindings: Option<&crate::engine::query_planning::bindings::QueryTableBindingStore>,
    dag: &mut crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec,
    pre_expand_keyed_assert: Option<crate::sql::planner::physical::PreExpandKeyedAssertSpec>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PlannedIcebergChangeStreamWrite, String> {
    crate::connector::validate_request_context(connector_context)?;
    let optimizer_settings = change_stream_write_optimizer_settings();
    let physical_plan = crate::sql::planner::optimizer_bridge::to_physical_plan(optimized_tree)?;
    let planned_dp =
        crate::sql::planner::pipeline::build_sql_change_stream_distributed_plan_with_settings(
            physical_plan,
            dag.clone(),
            pre_expand_keyed_assert,
            &optimizer_settings,
        )?;
    let distributed_plan = planned_dp.distributed_plan;
    let topology = planned_dp.topology;
    let maintenance_execution = capture_maintenance_execution(state)?;
    let scan_resolver = query_table_bindings
        .map(crate::engine::query_planning::delta_scan::QueryTableBindingScanResolver::new);
    let scan_binding_resolver = scan_resolver.as_ref().map(|resolver| {
        resolver as &dyn crate::query_execution::preparation::scan::ScanBindingResolver
    });
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed_plan,
        state.connector_control.as_ref(),
        connector_context,
        query_table_bindings,
        scan_binding_resolver,
        scan_preparation_options(&optimizer_settings, &maintenance_execution)?,
    )?;
    let native_bundle = crate::protocol::native::encode::encode_native_fragment_bundle(
        &distributed_plan,
        &prepared,
    )?;
    Ok(PlannedIcebergChangeStreamWrite {
        prepared,
        native_bundle,
        topology,
    })
}

pub(crate) fn execute_planned_iceberg_change_stream_write(
    state: &Arc<StandaloneState>,
    prepared: crate::query_execution::preparation::PreparedFragmentSet,
    native_bundle: crate::protocol::native::encode::NativeFragmentBundle,
    query_opts: Option<QueryOptions>,
    execution: Option<&crate::query_execution::request_context::QueryExecutionContext>,
    connector_write: Option<crate::query_execution::contract::ConnectorWritePlanningTemplate>,
) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
    let maintenance_execution;
    let execution = match execution {
        Some(execution) => execution,
        None => {
            maintenance_execution = capture_maintenance_execution(state)?;
            &maintenance_execution
        }
    };
    let prepared_request = prepare_planned_iceberg_change_stream_write(
        prepared,
        native_bundle,
        query_opts,
        execution,
        connector_write.map(DistributedConnectorWrite::Begin),
    )?;
    let bound = match bind_prepared_distributed_write_request(
        &state.query_execution,
        execution,
        prepared_request,
    )? {
        BoundDistributedWriteBinding::Bound(bound) => bound,
        BoundDistributedWriteBinding::AbortRequired { session, reason } => {
            let _ = session.abort(crate::connector::connector_request_context_for_execution(
                None, execution,
            )?);
            return Err(reason);
        }
    };
    execute_bound_distributed_write_request(&state.query_execution, bound.request)
}

/// Convert an already planned change-stream writer into SQL's inert native
/// write handoff.  The caller supplies the admitted execution context and
/// retains responsibility for binding the exact connector write lease before
/// submitting the request.
pub(crate) fn prepare_planned_iceberg_change_stream_write(
    prepared: crate::query_execution::preparation::PreparedFragmentSet,
    native_bundle: crate::protocol::native::encode::NativeFragmentBundle,
    query_opts: Option<QueryOptions>,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
    connector_write: Option<DistributedConnectorWrite>,
) -> Result<PreparedDistributedWriteRequest, String> {
    prepare_distributed_write_request_with_execution(
        prepared,
        native_bundle,
        query_opts,
        execution,
        connector_write,
    )
}

fn prepare_distributed_write_request_with_execution(
    prepared: crate::query_execution::preparation::PreparedFragmentSet,
    native_bundle: crate::protocol::native::encode::NativeFragmentBundle,
    query_options: Option<QueryOptions>,
    _execution: &crate::query_execution::request_context::QueryExecutionContext,
    connector_write: Option<DistributedConnectorWrite>,
) -> Result<PreparedDistributedWriteRequest, String> {
    let Some(DistributedConnectorWrite::Begin(template)) = connector_write else {
        return Err("prepared connector write requires an unsealed write template".to_string());
    };
    let cohort_id = template.cohort_id();
    let exact_lease = template.lease();
    PreparedDistributedWriteRequest::new(
        prepared,
        native_bundle,
        query_options,
        crate::query_execution::contract::ConnectorWriteOperationRegistration::single(template),
        cohort_id,
        exact_lease,
    )
    .map_err(|error| error.to_string())
}

/// The request is bound to a newly-created exact connector operation session.
/// Callers that need typed abort certainty retain `session` until a terminal
/// commit or abort decision instead of letting an intermediate error discard
/// that provider-owned capability.
pub(crate) struct BoundDistributedWriteRequest {
    pub(crate) request: crate::query_execution::contract::DistributedQueryRequest,
    pub(crate) session: crate::query_execution::write_operation::ConnectorWriteOperationSession,
}

/// A request-construction failure after `begin_write_operation` still owns an
/// exact provider session.  The caller must issue its terminal abort through
/// that session rather than dropping it as an ordinary planning error.
pub(crate) enum BoundDistributedWriteBinding {
    Bound(BoundDistributedWriteRequest),
    AbortRequired {
        session: crate::query_execution::write_operation::ConnectorWriteOperationSession,
        reason: String,
    },
}

pub(crate) fn bind_prepared_distributed_write_request(
    query_execution: &crate::query_execution::service::QueryExecutionService,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
    prepared: PreparedDistributedWriteRequest,
) -> Result<BoundDistributedWriteBinding, String> {
    let cohort_id = prepared.write_cohort_id();
    let session = query_execution
        .begin_write_operation(prepared.registration(), prepared.lease())
        .map_err(|error| error.to_string())?;
    let registration =
        match crate::query_execution::contract::ConnectorWriteExecutionRegistration::try_new(
            session.clone(),
            cohort_id,
        ) {
            Ok(registration) => registration,
            Err(error) => {
                return Ok(BoundDistributedWriteBinding::AbortRequired {
                    session,
                    reason: error.to_string(),
                });
            }
        };
    let request = match prepared.into_request(execution, registration) {
        Ok(request) => request,
        Err(error) => {
            return Ok(BoundDistributedWriteBinding::AbortRequired {
                session,
                reason: error.to_string(),
            });
        }
    };
    Ok(BoundDistributedWriteBinding::Bound(
        BoundDistributedWriteRequest { request, session },
    ))
}

fn execute_bound_distributed_write_request(
    query_execution: &crate::query_execution::service::QueryExecutionService,
    request: crate::query_execution::contract::DistributedQueryRequest,
) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
    let (query_result, write_commit, write_abort, connector_completion) = query_execution
        .execute(request)
        .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_write)
        .map(crate::query_execution::outcome::WriteExecutionOutcome::into_parts_with_connector)
        .map_err(|error| error.to_string())?;
    Ok(crate::query_execution::outcome::QueryExecutionResult {
        query_result,
        write_commit,
        write_abort,
        connector_completion,
        fragment_profiles: Vec::new(),
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_physical_plan_as_iceberg_change_stream_write(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    optimized_tree: &crate::sql::optimizer::OptimizedOperatorNode,
    dag: &mut crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec,
    query_opts: Option<QueryOptions>,
) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
    let planned = build_physical_plan_as_iceberg_change_stream_write(
        state,
        current_catalog,
        current_database,
        optimized_tree,
        None,
        dag,
        None,
    )?;
    #[cfg(test)]
    if let Some(result) = observe_change_stream_write_build_for_test(&planned.topology) {
        return Ok(result);
    }
    execute_planned_iceberg_change_stream_write(
        state,
        planned.prepared,
        planned.native_bundle,
        query_opts,
        None,
        None,
    )
}

#[allow(clippy::too_many_arguments)]

pub(crate) struct PlannedIcebergChangeStreamRefreshQuery {
    pub(crate) optimized_tree: crate::sql::optimizer::OptimizedOperatorNode,
    pub(crate) output_columns: Vec<crate::sql::analysis::OutputColumn>,
    pub(crate) change_stream:
        crate::sql::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor,
    pub(crate) table_bindings:
        Option<Arc<crate::engine::query_planning::bindings::QueryTableBindingStore>>,
}

fn change_stream_write_optimizer_settings()
-> crate::sql::optimizer::options::SessionOptimizerSettings {
    let mut settings = crate::sql::optimizer::options::SessionOptimizerSettings::default();
    // A change-stream write carries old/new row pairs and target locators across
    // independent fragments. A query runtime filter may describe only one data
    // branch, so pushing it into a locator scan can suppress rows required by a
    // DELETE. Keep this system-generated mutation plan free of runtime filters;
    // its explicit predicates and connector pruning remain enabled.
    settings.enable_global_runtime_filter = Some(false);
    settings
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn plan_query_for_iceberg_change_stream_refresh(
    state: &Arc<StandaloneState>,
    query: &sqlparser::ast::Query,
    analyzer_catalog: &dyn crate::sql::catalog::PlannerTableProvider,
    current_database: &str,
    imv_rewrite: Option<&crate::sql::compiler::SqlImvPlanningInput>,
    table_bindings: Arc<crate::engine::query_planning::bindings::QueryTableBindingStore>,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
) -> Result<PlannedIcebergChangeStreamRefreshQuery, String> {
    let backend_count = std::num::NonZeroUsize::new(execution.topology().targets().len())
        .ok_or_else(|| {
            "distributed SQL compilation requires a frozen non-zero backend count".to_string()
        })?;
    let catalog = crate::sql::compiler::SqlPlannerTableSnapshot::new(analyzer_catalog);
    let statistics = query_stats::QueryStatisticsContext::from_standalone_state_with_bindings(
        state,
        Arc::clone(&table_bindings),
    );
    let request = crate::sql::compiler::SqlCompileRequest::new(
        crate::sql::compiler::SqlStatementInput::ParsedQuery(Box::new(query.clone())),
        crate::sql::compiler::SqlCompileIntent::ChangeStreamWrite,
        crate::sql::compiler::SqlSessionContext {
            current_catalog: None,
            current_database: current_database.to_string(),
            optimizer_settings: change_stream_write_optimizer_settings(),
        },
        crate::sql::compiler::SqlPlanningEnvironment::Distributed { backend_count },
        &catalog,
        &statistics,
        crate::sql::functions::builtin_sql_function_catalog(),
        None,
        crate::sql::compiler::SqlCompileControl::new(
            execution.deadline(),
            crate::engine::query_planning::sql_cancellation_observation(
                execution.cancellation().clone(),
            ),
        ),
    );
    let request = match imv_rewrite {
        Some(input) => request.with_imv_rewrite(input),
        None => request,
    };
    let crate::sql::compiler::SqlCompileOutput::Optimized(compiled) =
        crate::sql::compiler::SqlCompiler::compile(request).map_err(|error| error.to_string())?
    else {
        return Err("change-stream intent did not produce an optimized SQL plan".to_string());
    };
    Ok(PlannedIcebergChangeStreamRefreshQuery {
        output_columns: compiled.optimized_tree.output_columns.clone(),
        optimized_tree: compiled.optimized_tree,
        change_stream: compiled.change_stream,
        table_bindings: Some(table_bindings),
    })
}

pub(crate) fn plan_logical_for_iceberg_change_stream_refresh(
    logical_plan: crate::sql::planner::logical::LogicalPlanNode,
    factory: crate::sql::column_id::ColumnRefFactory,
) -> Result<PlannedIcebergChangeStreamRefreshQuery, String> {
    let statistics = crate::sql::compiler::SqlUnavailableStatisticsSnapshot;
    let request = crate::sql::compiler::SqlCompileRequest::new_logical(
        logical_plan,
        factory,
        crate::sql::compiler::SqlCompileIntent::ChangeStreamWrite,
        crate::sql::compiler::SqlSessionContext {
            current_catalog: None,
            current_database: String::new(),
            optimizer_settings: change_stream_write_optimizer_settings(),
        },
        crate::sql::compiler::SqlPlanningEnvironment::NotApplicable,
        &statistics,
        crate::sql::compiler::SqlCompileControl::unbounded(),
    );
    let crate::sql::compiler::SqlCompileOutput::Optimized(compiled) =
        crate::sql::compiler::SqlCompiler::compile(request).map_err(|error| error.to_string())?
    else {
        return Err(
            "logical change-stream input did not produce an optimized SQL plan".to_string(),
        );
    };
    let optimized_tree = compiled.optimized_tree;
    let output_columns = optimized_tree.output_columns.clone();
    Ok(PlannedIcebergChangeStreamRefreshQuery {
        optimized_tree,
        output_columns,
        change_stream: compiled.change_stream,
        table_bindings: None,
    })
}

/// Application-owned post-compile assembly for the canonical SQL kernel.
///
/// View/virtual rewrites and topology admission happened before this point.
/// The compiler receives only their immutable SQL projection; preparation and
/// native encoding receive the exact binding store returned by that same
/// compilation request.
#[allow(clippy::too_many_arguments)]
fn prepare_query_with_sql_compiler_kernel(
    query: &sqlparser::ast::Query,
    analyzer_catalog: &crate::engine::query_planning::catalog_materializer::CatalogServiceMaterializer<'_>,
    current_catalog: Option<&str>,
    current_database: &str,
    state: &Arc<StandaloneState>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    query_opts: Option<QueryOptions>,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
    intent: crate::sql::compiler::SqlCompileIntent,
    allow_mv_rewrite_candidates: bool,
) -> Result<
    (
        crate::query_execution::contract::DistributedQueryRequest,
        crate::sql::planner::distributed::DistributedPlan,
        crate::query_execution::profile::ConnectorStaticPlanningMetrics,
    ),
    String,
> {
    let backend_count = std::num::NonZeroUsize::new(execution.topology().targets().len())
        .ok_or_else(|| {
            "SQL compilation requires a non-empty admitted backend topology".to_string()
        })?;
    let table_bindings = analyzer_catalog.query_table_bindings();
    let statistics = query_stats::QueryStatisticsContext::from_standalone_state_with_bindings(
        state,
        table_bindings.clone(),
    );
    let catalog_snapshot = crate::sql::compiler::SqlPlannerTableSnapshot::new(analyzer_catalog);
    // MV rewrite is an optional SQL optimization. An application composition
    // without an MV repository supplies no snapshot; a repository that is
    // available but fails to freeze remains a planning error.
    let mv_definitions =
        if allow_mv_rewrite_candidates && state.mv_repository.availability().is_available() {
            Some(crate::engine::mv_rewrite_prep::freeze_mv_rewrite_definition_index(state)?)
        } else {
            None
        };
    let distributed_intent = match &intent {
        crate::sql::compiler::SqlCompileIntent::Explain { analyze: true, .. } => {
            crate::query_execution::contract::DistributedQueryIntent::Profile
        }
        _ => crate::query_execution::contract::DistributedQueryIntent::Result,
    };
    let compiler_request = crate::sql::compiler::SqlCompileRequest::new(
        crate::sql::compiler::SqlStatementInput::ParsedQuery(Box::new(query.clone())),
        intent,
        crate::sql::compiler::SqlSessionContext {
            current_catalog: current_catalog.map(str::to_string),
            current_database: current_database.to_string(),
            optimizer_settings: execution.optimizer_settings().clone(),
        },
        crate::sql::compiler::SqlPlanningEnvironment::Distributed { backend_count },
        &catalog_snapshot,
        &statistics,
        crate::sql::functions::builtin_sql_function_catalog(),
        mv_definitions.as_ref(),
        crate::sql::compiler::SqlCompileControl::new(
            execution.deadline(),
            crate::engine::query_planning::sql_cancellation_observation(
                execution.cancellation().clone(),
            ),
        ),
    );
    let planning_inputs = crate::engine::query_planning::QueryPlanningInputs {
        compile_request: compiler_request,
        post_compile: crate::engine::query_planning::PostCompilePlanningContext {
            table_bindings,
            connector_controls: state.connector_control.as_ref(),
            connector_context,
        },
    };
    let crate::sql::compiler::SqlCompileOutput::Distributed(compiled) =
        crate::sql::compiler::SqlCompiler::compile(planning_inputs.compile_request)
            .map_err(|error| error.to_string())?
    else {
        return Err("query intent did not produce a distributed SQL plan".to_string());
    };
    ensure_mainline_distributed_execution(
        false,
        // SQLX-1 has already lowered a connector-neutral distributed read.
        // A populated Iceberg registry is catalog metadata, not a request for
        // the removed local direct-write execution path.
        false,
        state.exchange_port,
    )?;
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &compiled.distributed_plan,
        planning_inputs.post_compile.connector_controls,
        planning_inputs.post_compile.connector_context,
        Some(planning_inputs.post_compile.table_bindings.as_ref()),
        None,
        scan_preparation_options(execution.optimizer_settings(), execution)?,
    )?;
    let native_bundle = crate::protocol::native::encode::encode_native_fragment_bundle(
        &compiled.distributed_plan,
        &prepared,
    )?;
    let connector_static_planning = connector_static_planning_metrics(&prepared)?;
    let request = crate::query_execution::contract::build_distributed_query_request_with_execution(
        prepared,
        native_bundle,
        query_opts,
        distributed_intent,
        execution,
    )
    .map_err(|error| error.to_string())?;
    Ok((
        request,
        compiled.distributed_plan,
        connector_static_planning,
    ))
}

#[allow(clippy::too_many_arguments)]
fn explain_query_with_sql_compiler_kernel(
    query: &sqlparser::ast::Query,
    analyzer_catalog: &crate::engine::query_planning::catalog_materializer::CatalogServiceMaterializer<'_>,
    current_catalog: Option<&str>,
    current_database: &str,
    state: &Arc<StandaloneState>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
    level: crate::sql::explain::ExplainLevel,
    logical: bool,
) -> Result<QueryResult, String> {
    let backend_count = std::num::NonZeroUsize::new(execution.topology().targets().len())
        .ok_or_else(|| {
            "SQL compilation requires a non-empty admitted backend topology".to_string()
        })?;
    let table_bindings = analyzer_catalog.query_table_bindings();
    let statistics = query_stats::QueryStatisticsContext::from_standalone_state_with_bindings(
        state,
        table_bindings.clone(),
    );
    let catalog_snapshot = crate::sql::compiler::SqlPlannerTableSnapshot::new(analyzer_catalog);
    let mv_definitions = crate::engine::mv_rewrite_prep::freeze_mv_rewrite_definition_index(state)?;
    let intent = if logical {
        crate::sql::compiler::SqlCompileIntent::LogicalOnly
    } else {
        crate::sql::compiler::SqlCompileIntent::Explain {
            level,
            analyze: false,
        }
    };
    let planning_inputs = crate::engine::query_planning::QueryPlanningInputs {
        compile_request: crate::sql::compiler::SqlCompileRequest::new(
            crate::sql::compiler::SqlStatementInput::ParsedQuery(Box::new(query.clone())),
            intent,
            crate::sql::compiler::SqlSessionContext {
                current_catalog: current_catalog.map(str::to_string),
                current_database: current_database.to_string(),
                optimizer_settings: execution.optimizer_settings().clone(),
            },
            crate::sql::compiler::SqlPlanningEnvironment::Distributed { backend_count },
            &catalog_snapshot,
            &statistics,
            crate::sql::functions::builtin_sql_function_catalog(),
            Some(&mv_definitions),
            crate::sql::compiler::SqlCompileControl::new(
                execution.deadline(),
                crate::engine::query_planning::sql_cancellation_observation(
                    execution.cancellation().clone(),
                ),
            ),
        ),
        post_compile: crate::engine::query_planning::PostCompilePlanningContext {
            table_bindings,
            connector_controls: state.connector_control.as_ref(),
            connector_context,
        },
    };
    let compiled = crate::sql::compiler::SqlCompiler::compile(planning_inputs.compile_request)
        .map_err(|error| error.to_string())?;
    let lines = match compiled {
        crate::sql::compiler::SqlCompileOutput::Logical(compiled) if logical => {
            crate::sql::explain::explain_plan_checked(&compiled.logical_plan, level)?
        }
        crate::sql::compiler::SqlCompileOutput::ImmediateExplain(lines) if !logical => lines,
        _ => return Err("EXPLAIN intent produced unexpected SQL facts".to_string()),
    };
    build_string_query_result("Explain String", lines)
}

fn execute_distributed_result_with_execution(
    query_execution: &crate::query_execution::service::QueryExecutionService,
    prepared: crate::query_execution::preparation::PreparedFragmentSet,
    native_bundle: crate::protocol::native::encode::NativeFragmentBundle,
    query_options: Option<QueryOptions>,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
) -> Result<QueryResult, String> {
    let request = crate::query_execution::contract::build_distributed_query_request_with_execution(
        prepared,
        native_bundle,
        query_options,
        crate::query_execution::contract::DistributedQueryIntent::Result,
        execution,
    )
    .map_err(|error| error.to_string())?;
    query_execution
        .execute(request)
        .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_result)
        .map(crate::query_execution::outcome::ResultExecutionOutcome::into_query_result)
        .map_err(|error| error.to_string())
}

fn execute_distributed_write_with_execution(
    query_execution: &crate::query_execution::service::QueryExecutionService,
    prepared: crate::query_execution::preparation::PreparedFragmentSet,
    native_bundle: crate::protocol::native::encode::NativeFragmentBundle,
    query_options: Option<QueryOptions>,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
    connector_write: Option<DistributedConnectorWrite>,
) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
    let request = crate::query_execution::contract::build_distributed_query_request_with_execution(
        prepared,
        native_bundle,
        query_options,
        crate::query_execution::contract::DistributedQueryIntent::Write,
        execution,
    )
    .map_err(|error| error.to_string())?;
    let request = match connector_write {
        Some(DistributedConnectorWrite::Begin(template)) => {
            let cohort_id = template.cohort_id();
            let exact_lease = template.lease();
            let session = query_execution
                .begin_write_operation(
                    crate::query_execution::contract::ConnectorWriteOperationRegistration::single(
                        template,
                    ),
                    exact_lease,
                )
                .map_err(|error| error.to_string())?;
            let registration =
                crate::query_execution::contract::ConnectorWriteExecutionRegistration::try_new(
                    session, cohort_id,
                )
                .map_err(|error| error.to_string())?;
            crate::query_execution::contract::with_connector_write_operation(request, registration)
                .map_err(|error| error.to_string())?
        }
        Some(DistributedConnectorWrite::Sealed(registration)) => {
            crate::query_execution::contract::with_connector_write_operation(request, registration)
                .map_err(|error| error.to_string())?
        }
        None => request,
    };
    let (query_result, write_commit, write_abort, connector_completion) = query_execution
        .execute(request)
        .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_write)
        .map(crate::query_execution::outcome::WriteExecutionOutcome::into_parts_with_connector)
        .map_err(|error| error.to_string())?;
    Ok(crate::query_execution::outcome::QueryExecutionResult {
        query_result,
        write_commit,
        write_abort,
        connector_completion,
        fragment_profiles: Vec::new(),
    })
}

fn execute_distributed_profile_with_execution(
    query_execution: &crate::query_execution::service::QueryExecutionService,
    prepared: crate::query_execution::preparation::PreparedFragmentSet,
    native_bundle: crate::protocol::native::encode::NativeFragmentBundle,
    query_options: Option<QueryOptions>,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
    let request = crate::query_execution::contract::build_distributed_query_request_with_execution(
        prepared,
        native_bundle,
        query_options,
        crate::query_execution::contract::DistributedQueryIntent::Profile,
        execution,
    )
    .map_err(|error| error.to_string())?;
    let (query_result, fragment_profiles) = query_execution
        .execute(request)
        .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_profile)
        .map(crate::query_execution::outcome::ProfileExecutionOutcome::into_parts)
        .map_err(|error| error.to_string())?;
    Ok(crate::query_execution::outcome::QueryExecutionResult {
        query_result,
        write_commit: None,
        write_abort: None,
        connector_completion: None,
        fragment_profiles: fragment_profiles.into_profiles(),
    })
}

#[cfg(test)]
pub(crate) struct StandaloneLoopbackTestBackend {
    pub(crate) exchange_port: u16,
    _test_guard: TestSerializationGuard,
}

#[cfg(test)]
pub(crate) fn install_all_in_one_loopback_backend_for_test() -> StandaloneLoopbackTestBackend {
    let test_guard = acquire_standalone_test_guard();
    StandaloneLoopbackTestBackend {
        exchange_port: in_process_exchange_endpoint_sentinel(),
        _test_guard: test_guard,
    }
}

#[cfg(test)]
const fn in_process_exchange_endpoint_sentinel() -> u16 {
    // The test coordinator is in-process and never opens a native listener.
    // The lifetime-held TestSerializationGuard keeps this nonzero topology
    // marker isolated from concurrent standalone semantic tests.
    1
}

// ---------------------------------------------------------------------------
// EXPLAIN COSTS helper
// ---------------------------------------------------------------------------

fn rewrite_legacy_partition_references(
    state: &Arc<StandaloneState>,
    sql: &str,
    current_database: &str,
) -> Result<String, String> {
    // TRUNCATE TABLE ... PARTITION (...) is intentionally rejected by the
    // TRUNCATE parser with a "PARTITION (...) is not supported" error. The
    // legacy-partition rewriter must not see TRUNCATE's PARTITION clause as a
    // legacy SELECT/INSERT partition reference — it would try to resolve
    // `id=1` as an identifier and surface a confusing "unsupported
    // identifier" diagnostic before the parser even runs.
    let trimmed = sql.trim_start();
    if trimmed.len() >= 8 && trimmed[..8].eq_ignore_ascii_case("truncate") {
        return Ok(sql.to_string());
    }
    let sql = rewrite_insert_partition_target(sql);
    rewrite_select_partition_table_refs(state, &sql, current_database)
}

fn rewrite_insert_partition_target(sql: &str) -> String {
    let trimmed = sql.trim_start();
    let prefix_len = sql.len() - trimmed.len();
    let lower = trimmed.to_ascii_lowercase();
    if !(lower.starts_with("insert into ") || lower.starts_with("insert overwrite ")) {
        return sql.to_string();
    }
    let Some(marker) = find_partition_marker(trimmed, 0) else {
        return sql.to_string();
    };
    let mut rewritten = String::with_capacity(sql.len());
    rewritten.push_str(&sql[..prefix_len + marker.marker_start]);
    rewritten.push_str(&trimmed[marker.end..]);
    rewritten
}

fn rewrite_select_partition_table_refs(
    state: &Arc<StandaloneState>,
    sql: &str,
    current_database: &str,
) -> Result<String, String> {
    let mut out = String::with_capacity(sql.len());
    let mut cursor = 0;
    while let Some(marker) = find_partition_marker(sql, cursor) {
        let table_end = marker.marker_start;
        let table_end_trimmed = trim_ascii_whitespace_end(sql, table_end);
        let table_start = find_table_ref_start(sql, table_end_trimmed);
        if table_start == table_end_trimmed {
            cursor = marker.end;
            continue;
        }
        let table_ref = sql[table_start..table_end_trimmed].trim();
        let Some((database, table, alias)) =
            resolve_partition_table_ref(table_ref, current_database)
        else {
            cursor = marker.end;
            continue;
        };
        let partition = {
            let catalog = state
                .catalog_service
                .local()
                .read()
                .expect("standalone catalog read lock");
            catalog
                .get_legacy_range_partition(&database, &table, &marker.partition_name)?
                .ok_or_else(|| {
                    format!(
                        "unknown partition `{}` for table {}.{}",
                        marker.partition_name, database, table
                    )
                })?
        };
        out.push_str(&sql[cursor..table_start]);
        out.push_str(&legacy_partition_subquery(table_ref, &alias, &partition));
        cursor = marker.end;
    }
    out.push_str(&sql[cursor..]);
    Ok(out)
}

#[derive(Clone, Debug)]
struct PartitionMarker {
    marker_start: usize,
    end: usize,
    partition_name: String,
}

fn find_partition_marker(sql: &str, start: usize) -> Option<PartitionMarker> {
    let bytes = sql.as_bytes();
    let lower = sql.to_ascii_lowercase();
    let mut cursor = start;
    while let Some(rel) = lower[cursor..].find("partition") {
        let partition_start = cursor + rel;
        let partition_end = partition_start + "partition".len();
        if is_identifier_byte(bytes.get(partition_start.wrapping_sub(1)).copied())
            || is_identifier_byte(bytes.get(partition_end).copied())
        {
            cursor = partition_end;
            continue;
        }
        let paren = skip_ascii_whitespace(bytes, partition_end);
        if bytes.get(paren) != Some(&b'(') {
            cursor = partition_end;
            continue;
        }
        let marker_start =
            temporary_partition_marker_start(sql, partition_start).unwrap_or(partition_start);
        let close = find_matching_paren(sql, paren)?;
        let partition_name = sql[paren + 1..close]
            .trim()
            .trim_matches('`')
            .to_ascii_lowercase();
        return Some(PartitionMarker {
            marker_start,
            end: close + 1,
            partition_name,
        });
    }
    None
}

fn temporary_partition_marker_start(sql: &str, partition_start: usize) -> Option<usize> {
    let before_partition = trim_ascii_whitespace_end(sql, partition_start);
    let temp_end = before_partition;
    let temp_start = find_word_start(sql, temp_end);
    if temp_start == temp_end {
        return None;
    }
    if sql[temp_start..temp_end].eq_ignore_ascii_case("temporary") {
        Some(temp_start)
    } else {
        None
    }
}

fn legacy_partition_subquery(
    table_ref: &str,
    alias: &str,
    partition: &LegacyRangePartition,
) -> String {
    format!(
        "(SELECT * FROM {table_ref} WHERE {column} >= {lower} AND {column} < {upper}) AS {alias}",
        column = partition.column,
        lower = partition.lower_sql,
        upper = partition.upper_sql
    )
}

fn resolve_partition_table_ref(
    table_ref: &str,
    current_database: &str,
) -> Option<(String, String, String)> {
    let parts = table_ref
        .split('.')
        .map(|part| part.trim().trim_matches('`').to_ascii_lowercase())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    let (database, table) = match parts.as_slice() {
        [table] => (current_database.to_ascii_lowercase(), table.clone()),
        [database, table] => (database.clone(), table.clone()),
        [.., database, table] => (database.clone(), table.clone()),
        _ => return None,
    };
    Some((database, table.clone(), table))
}

fn trim_ascii_whitespace_end(sql: &str, mut idx: usize) -> usize {
    let bytes = sql.as_bytes();
    while idx > 0 && bytes[idx - 1].is_ascii_whitespace() {
        idx -= 1;
    }
    idx
}

fn find_table_ref_start(sql: &str, mut idx: usize) -> usize {
    let bytes = sql.as_bytes();
    while idx > 0 {
        let b = bytes[idx - 1];
        if b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$' | b'.' | b'`') {
            idx -= 1;
        } else {
            break;
        }
    }
    idx
}

fn find_word_start(sql: &str, mut idx: usize) -> usize {
    let bytes = sql.as_bytes();
    while idx > 0 && is_identifier_byte(Some(bytes[idx - 1])) {
        idx -= 1;
    }
    idx
}

fn skip_ascii_whitespace(bytes: &[u8], mut idx: usize) -> usize {
    while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
        idx += 1;
    }
    idx
}

fn is_identifier_byte(byte: Option<u8>) -> bool {
    byte.is_some_and(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$'))
}

fn find_matching_paren(sql: &str, open: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    if bytes.get(open) != Some(&b'(') {
        return None;
    }
    let mut depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut idx = open;
    while idx < bytes.len() {
        let b = bytes[idx];
        if in_single {
            if b == b'\'' {
                in_single = false;
            }
            idx += 1;
            continue;
        }
        if in_double {
            if b == b'"' {
                in_double = false;
            }
            idx += 1;
            continue;
        }
        match b {
            b'\'' => in_single = true,
            b'"' => in_double = true,
            b'(' => depth += 1,
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(idx);
                }
            }
            _ => {}
        }
        idx += 1;
    }
    None
}

/// Wrap a sqlparser error message in the `sql parser error: ...` envelope
/// and append a StarRocks-style `Unexpected input '<token>'` clause when
/// the underlying error mentions the offending token (`found: <token>`),
/// so tests can assert against the StarRocks-FE-style wording.
fn format_parser_error(raw: &str) -> String {
    let mut out = format!("sql parser error: {raw}");
    if let Some(start) = raw.find("found: ") {
        let after = &raw[start + "found: ".len()..];
        let token = after
            .split(|c: char| c.is_whitespace() || c == ',')
            .next()
            .unwrap_or("")
            .trim()
            .trim_matches(|c: char| c == '`' || c == '"');
        if !token.is_empty() {
            out.push_str(&format!(" Unexpected input '{token}'."));
        }
    }
    out
}

fn split_explain_costs_sql(sql: &str) -> Option<(String, crate::sql::explain::ExplainLevel)> {
    let body = consume_leading_keyword(consume_leading_keyword(sql, "EXPLAIN")?, "COSTS")?;
    Some((
        format!("EXPLAIN {}", body.trim_start()),
        crate::sql::explain::ExplainLevel::Costs,
    ))
}

fn split_explain_logical_sql(sql: &str) -> Option<(String, crate::sql::explain::ExplainLevel)> {
    let mut body = consume_leading_keyword(consume_leading_keyword(sql, "EXPLAIN")?, "LOGICAL")?;
    let mut level = crate::sql::explain::ExplainLevel::Normal;
    for (keyword, candidate) in [
        ("VERBOSE", crate::sql::explain::ExplainLevel::Verbose),
        ("COSTS", crate::sql::explain::ExplainLevel::Costs),
    ] {
        if let Some(rest) = consume_leading_keyword(body, keyword) {
            level = candidate;
            body = rest;
            break;
        }
    }

    Some((format!("EXPLAIN {}", body.trim_start()), level))
}

fn consume_leading_keyword<'a>(sql: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = sql.trim_start();
    let head = trimmed.as_bytes().get(..keyword.len())?;
    if !head.eq_ignore_ascii_case(keyword.as_bytes()) {
        return None;
    }

    let rest = &trimmed[keyword.len()..];
    if rest
        .chars()
        .next()
        .is_some_and(|ch| !ch.is_ascii_whitespace())
    {
        return None;
    }
    Some(rest)
}

fn parse_explain_refresh_materialized_view(
    sql: &str,
) -> Option<
    Result<
        (
            crate::sql::parser::ast::RefreshMaterializedViewStmt,
            crate::sql::explain::ExplainLevel,
            bool,
        ),
        String,
    >,
> {
    let trimmed = sql.trim_start();
    let prefixes = [
        (
            "EXPLAIN ANALYZE REFRESH ",
            crate::sql::explain::ExplainLevel::Analyze,
            true,
        ),
        (
            "EXPLAIN VERBOSE REFRESH ",
            crate::sql::explain::ExplainLevel::Verbose,
            false,
        ),
        (
            "EXPLAIN COSTS REFRESH ",
            crate::sql::explain::ExplainLevel::Costs,
            false,
        ),
        (
            "EXPLAIN REFRESH ",
            crate::sql::explain::ExplainLevel::Normal,
            false,
        ),
    ];
    for (prefix, level, analyze) in prefixes {
        if trimmed
            .as_bytes()
            .get(..prefix.len())
            .is_some_and(|head| head.eq_ignore_ascii_case(prefix.as_bytes()))
        {
            let body = format!("REFRESH {}", trimmed[prefix.len()..].trim_start());
            let mut statements = match crate::sql::parser::parse_sql(&body) {
                Ok(statements) => statements,
                Err(e) => return Some(Err(e)),
            };
            let Some(statement) = statements.pop() else {
                return Some(Err("EXPLAIN REFRESH parsed no statement".to_string()));
            };
            let crate::sql::parser::ast::Statement::RefreshMaterializedView(stmt) = statement
            else {
                return Some(Err(
                    "EXPLAIN REFRESH only supports REFRESH MATERIALIZED VIEW".to_string(),
                ));
            };
            return Some(Ok((stmt, level, analyze)));
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod build_iceberg_create_table_ddl_tests {
    use super::build_iceberg_create_table_ddl;
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorRequestContext,
        ConnectorTableDefinitionColumn, ConnectorTableDefinitionFacts,
        ConnectorTableDefinitionStructField, ConnectorTableDefinitionType, ConnectorTableHandle,
        ConnectorTableIdentity, ConnectorTableMetadata, ConnectorTablePlanningFacts,
    };
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn request_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(60),
            Arc::new(NeverCancelled),
            1_024,
            64 * 1_024,
        )
        .expect("request context")
    }

    fn loaded_table(
        table_comment: Option<&str>,
        column_comment: Option<&str>,
        data_type: ConnectorTableDefinitionType,
        nullable: bool,
    ) -> ConnectorTableMetadata {
        let instance_id = ConnectorInstanceId::parse("cat").expect("instance ID");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int32,
            nullable,
        )]));
        let planning_facts = ConnectorTablePlanningFacts::empty();
        let definition_facts = ConnectorTableDefinitionFacts::try_new(
            &schema,
            &planning_facts,
            vec![ConnectorTableDefinitionColumn::new(
                0,
                data_type,
                nullable,
                column_comment.map(Arc::from),
            )],
            table_comment.map(Arc::from),
            &request_context(),
        )
        .expect("definition facts");
        ConnectorTableMetadata {
            identity: ConnectorTableIdentity {
                instance_id: instance_id.clone(),
                namespace: Arc::from("ns"),
                table: Arc::from("tbl"),
            },
            schema,
            planning_facts,
            definition_facts,
            version: None,
            statistics_data_version: None,
            table: ConnectorTableHandle::try_new(instance_id, Bytes::from_static(b"table"))
                .expect("table handle"),
        }
    }

    #[test]
    fn emits_table_and_column_comments_with_escaping() {
        let loaded = loaded_table(
            Some("it's great"),
            Some("owner's id"),
            ConnectorTableDefinitionType::Int,
            false,
        );
        let ddl = build_iceberg_create_table_ddl("cat", "ns", "tbl", &loaded).expect("build ddl");
        assert!(ddl.contains("`id` INT NOT NULL COMMENT 'owner\\'s id'"));
        assert!(ddl.contains("COMMENT 'it\\'s great'"));
    }

    #[test]
    fn renders_fixed_and_nested_definition_types() {
        let loaded = loaded_table(
            None,
            None,
            ConnectorTableDefinitionType::Array(Box::new(ConnectorTableDefinitionType::Struct(
                vec![ConnectorTableDefinitionStructField::new(
                    "payload",
                    ConnectorTableDefinitionType::Map(
                        Box::new(ConnectorTableDefinitionType::String),
                        Box::new(ConnectorTableDefinitionType::Binary {
                            fixed_length: Some(16),
                        }),
                    ),
                )],
            ))),
            true,
        );
        let ddl = build_iceberg_create_table_ddl("cat", "ns", "tbl", &loaded).expect("build ddl");
        assert!(ddl.contains("ARRAY<STRUCT<payload MAP<STRING,BINARY(16)>>>"));
    }

    #[test]
    fn no_comment_clause_when_comment_is_empty() {
        let loaded = loaded_table(Some(""), None, ConnectorTableDefinitionType::Int, true);
        let ddl = build_iceberg_create_table_ddl("cat", "ns", "tbl", &loaded).expect("build ddl");
        assert!(!ddl.contains("COMMENT"));
    }

    #[test]
    fn empty_definition_facts_fail_closed() {
        let mut loaded = loaded_table(None, None, ConnectorTableDefinitionType::Int, true);
        loaded.definition_facts = ConnectorTableDefinitionFacts::empty();
        let error = build_iceberg_create_table_ddl("cat", "ns", "tbl", &loaded)
            .expect_err("empty definition facts must fail");
        assert!(error.contains("unsupported"));
    }
}

#[cfg(test)]
mod tests {
    use super::{
        QueryResult, StandaloneNovaRocks, StandaloneOpenServices, StandaloneOptions,
        StandaloneSession, StandaloneState, StatementResult, dispatch_statement,
        register_connector_backends,
    };
    use crate::engine::statistics::{
        CatalogTableStatistics, StatisticsEngine, StatisticsInsertObservation,
        StatisticsRequestContext, StatisticsService, StatisticsStatementResult,
    };
    use crate::engine::statistics_application::{
        StatisticsApplicationCommand, StatisticsApplicationError, StatisticsApplicationPort,
        StatisticsApplicationResult, StatisticsJobView, StatisticsTableStatView,
        StatisticsTableTarget,
    };
    use crate::engine::system_catalog::{SystemCatalog, SystemCatalogInputs, SystemTableData};
    use crate::engine::view::{ViewEngine, ViewRequestContext, ViewService, ViewStatementResult};
    use crate::mv::application::{
        MvApplicationError, MvApplicationErrorKind, MvApplicationService, MvApplicationStatement,
        MvEngine, MvRequestContext,
    };
    use crate::mv::repository::MvTarget;
    use crate::query_execution::backend::{BackendTopologyPort, LiveBackendTarget};
    use crate::query_execution::contract::{
        DistributedQueryCoordinator, DistributedQueryError, DistributedQueryOutcome,
        DistributedQueryRequest,
    };
    use crate::query_execution::service::QueryExecutionService;
    use arrow::array::{Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_execution::exec::spill::{SpillConfig, SpillMode};
    use novarocks_execution::runtime::query_options::QueryOptions;
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tempfile::TempDir;

    trait IntoTestLiteral {
        fn into_test_literal(self) -> crate::sql::parser::ast::Literal;
    }

    impl IntoTestLiteral for i32 {
        fn into_test_literal(self) -> crate::sql::parser::ast::Literal {
            crate::sql::parser::ast::Literal::Int(i64::from(self))
        }
    }

    impl IntoTestLiteral for i64 {
        fn into_test_literal(self) -> crate::sql::parser::ast::Literal {
            crate::sql::parser::ast::Literal::Int(self)
        }
    }

    impl IntoTestLiteral for &str {
        fn into_test_literal(self) -> crate::sql::parser::ast::Literal {
            crate::sql::parser::ast::Literal::String(self.to_string())
        }
    }

    impl IntoTestLiteral for String {
        fn into_test_literal(self) -> crate::sql::parser::ast::Literal {
            crate::sql::parser::ast::Literal::String(self)
        }
    }

    impl<T> IntoTestLiteral for Option<T>
    where
        T: IntoTestLiteral,
    {
        fn into_test_literal(self) -> crate::sql::parser::ast::Literal {
            self.map_or(
                crate::sql::parser::ast::Literal::Null,
                IntoTestLiteral::into_test_literal,
            )
        }
    }

    macro_rules! insert_rows {
        ($session:expr, $target:expr; $([$($value:expr),* $(,)?]),+ $(,)?) => {
            insert_iceberg_fixture_rows(
                $session,
                &$target,
                &[
                    $(vec![$($value.into_test_literal()),*]),+
                ],
            )
        };
    }

    macro_rules! nullable_i64 {
        (NULL) => {
            None
        };
        ($value:expr) => {
            Some($value as i64)
        };
    }

    macro_rules! kv_rows {
        ($(($key:tt, $value:tt)),* $(,)?) => {
            &[$((nullable_i64!($key), nullable_i64!($value))),*]
        };
    }

    fn insert_iceberg_fixture_rows(
        session: &StandaloneSession,
        target_parts: &[&str],
        rows: &[Vec<crate::sql::parser::ast::Literal>],
    ) {
        let [catalog, namespace, table] = target_parts else {
            panic!("Iceberg row fixture requires catalog.namespace.table");
        };
        let registry = session
            .inner
            .iceberg_catalogs
            .read()
            .expect("Iceberg catalog registry");
        let entry = registry.get(catalog).expect("fixture Iceberg catalog");
        crate::connector::iceberg::catalog::registry::insert_rows(&entry, namespace, table, rows)
            .expect("insert Iceberg fixture rows");
    }

    #[derive(Default)]
    struct RecordingStatisticsApplicationPort {
        commands: Mutex<Vec<StatisticsApplicationCommand>>,
    }

    impl RecordingStatisticsApplicationPort {
        fn commands(&self) -> Vec<StatisticsApplicationCommand> {
            self.commands.lock().expect("statistics commands").clone()
        }
    }

    impl StatisticsApplicationPort for RecordingStatisticsApplicationPort {
        fn execute(
            &self,
            command: StatisticsApplicationCommand,
        ) -> Result<StatisticsApplicationResult, StatisticsApplicationError> {
            self.commands
                .lock()
                .expect("statistics commands")
                .push(command.clone());
            match command {
                StatisticsApplicationCommand::AnalyzeTable { target, .. } => Ok(
                    StatisticsApplicationResult::JobSubmitted(StatisticsJobView {
                        job_id: uuid::Uuid::nil(),
                        operation_id: uuid::Uuid::nil(),
                        state: "SUBMITTED".into(),
                        attempt: 0,
                        target,
                    }),
                ),
                StatisticsApplicationCommand::ShowAnalyzeJobs => {
                    Ok(StatisticsApplicationResult::AnalyzeJobs(vec![
                        StatisticsJobView {
                            job_id: uuid::Uuid::nil(),
                            operation_id: uuid::Uuid::nil(),
                            state: "SUBMITTED".into(),
                            attempt: 0,
                            target: StatisticsTableTarget {
                                catalog: "ice".into(),
                                namespace: "analytics".into(),
                                table: "orders".into(),
                            },
                        },
                    ]))
                }
                StatisticsApplicationCommand::CancelAnalyze { .. } => {
                    Ok(StatisticsApplicationResult::AnalyzeJobs(Vec::new()))
                }
                StatisticsApplicationCommand::ShowTableStats { .. } => {
                    Ok(StatisticsApplicationResult::TableStats(vec![
                        StatisticsTableStatView {
                            metric: "row_count".into(),
                            value: Some("42".into()),
                            status: "FULL_EXACT".into(),
                        },
                    ]))
                }
            }
        }
    }

    struct AlwaysUnavailableMvApplicationService;

    impl MvApplicationService for AlwaysUnavailableMvApplicationService {
        fn try_handle_statement(
            &self,
            _engine: &dyn MvEngine,
            _statement: &MvApplicationStatement,
            _context: MvRequestContext<'_>,
        ) -> Result<Option<crate::mv::application::MvStatementResult>, MvApplicationError> {
            Err(MvApplicationError::new(
                MvApplicationErrorKind::Unavailable,
                "injected frontend MV service is unavailable",
            ))
        }
    }

    #[derive(Default)]
    struct RefreshRouteRecordingMvApplicationService {
        refreshes: Mutex<
            Vec<(
                MvApplicationStatement,
                MvTarget,
                crate::query_execution::request_context::QueryExecutionContext,
            )>,
        >,
    }

    impl MvApplicationService for RefreshRouteRecordingMvApplicationService {
        fn try_handle_statement(
            &self,
            _engine: &dyn MvEngine,
            _statement: &MvApplicationStatement,
            _context: MvRequestContext<'_>,
        ) -> Result<Option<crate::mv::application::MvStatementResult>, MvApplicationError> {
            Ok(None)
        }

        fn prepare_and_execute_refresh(
            &self,
            _preparation: &dyn crate::mv::application::MvRefreshPreparationService,
            statement: MvApplicationStatement,
            target: MvTarget,
            _connector_context: novarocks_spi::connector::ConnectorRequestContext,
            execution: &crate::query_execution::request_context::QueryExecutionContext,
        ) -> Result<crate::mv::application::MvStatementResult, MvApplicationError> {
            self.refreshes.lock().expect("refresh route calls").push((
                statement,
                target,
                execution.clone(),
            ));
            Err(MvApplicationError::new(
                MvApplicationErrorKind::Unavailable,
                "recorded frontend refresh route",
            ))
        }
    }

    struct PassthroughMvApplicationService;

    impl MvApplicationService for PassthroughMvApplicationService {
        fn try_handle_statement(
            &self,
            _engine: &dyn MvEngine,
            _statement: &MvApplicationStatement,
            _context: MvRequestContext<'_>,
        ) -> Result<Option<crate::mv::application::MvStatementResult>, MvApplicationError> {
            Ok(None)
        }

        fn prepare_and_execute_refresh(
            &self,
            _preparation: &dyn crate::mv::application::MvRefreshPreparationService,
            _statement: MvApplicationStatement,
            _target: MvTarget,
            connector_context: novarocks_spi::connector::ConnectorRequestContext,
            _execution: &crate::query_execution::request_context::QueryExecutionContext,
        ) -> Result<crate::mv::application::MvStatementResult, MvApplicationError> {
            if connector_context.cancellation().is_cancelled() {
                return Err(MvApplicationError::new(
                    MvApplicationErrorKind::ShutdownCancelled,
                    "connector request was cancelled",
                ));
            }
            Err(MvApplicationError::new(
                MvApplicationErrorKind::Unavailable,
                "frontend MV refresh lifecycle is unavailable",
            ))
        }
    }

    #[derive(Clone)]
    struct FixedCatalogStatisticsService {
        tables: std::collections::HashMap<(String, String), CatalogTableStatistics>,
    }

    impl StatisticsService for FixedCatalogStatisticsService {
        fn try_handle_statement(
            &self,
            _engine: &dyn StatisticsEngine,
            _sql: &str,
            _context: StatisticsRequestContext<'_>,
        ) -> Result<Option<StatisticsStatementResult>, String> {
            Ok(None)
        }

        fn try_query(
            &self,
            _sql: &str,
            _query: &sqlparser::ast::Query,
            _context: StatisticsRequestContext<'_>,
        ) -> Result<Option<QueryResult>, String> {
            Ok(None)
        }

        fn observe_query(
            &self,
            _query: &sqlparser::ast::Query,
            _current_database: &str,
        ) -> Result<(), String> {
            Ok(())
        }

        fn observe_insert(
            &self,
            _engine: &dyn StatisticsEngine,
            _observation: StatisticsInsertObservation<'_>,
        ) -> Result<(), String> {
            Ok(())
        }

        fn observe_update(&self, _sql: &str, _current_database: &str) -> Result<(), String> {
            Ok(())
        }

        fn drop_table(&self, _database: &str, _table: &str) {}

        fn drop_database(&self, _database: &str) {}

        fn catalog_table_statistics(
            &self,
            database: &str,
            table: &str,
        ) -> Result<Option<CatalogTableStatistics>, String> {
            Ok(self
                .tables
                .get(&(database.to_string(), table.to_string()))
                .cloned())
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum TestBackendStatus {
        Registering,
        Live,
    }

    impl TestBackendStatus {
        const fn as_str(self) -> &'static str {
            match self {
                Self::Registering => "Registering",
                Self::Live => "Live",
            }
        }
    }

    #[derive(Clone)]
    struct TestBackendTopologyEntry {
        endpoint: SocketAddr,
        state: TestBackendStatus,
        scheduled_fragments: u64,
        start_epoch: u64,
    }

    #[derive(Default)]
    struct TestBackendTopologyState {
        entries: BTreeMap<usize, TestBackendTopologyEntry>,
        next_backend_idx: usize,
    }

    #[derive(Default)]
    struct TestBackendTopologyPort {
        state: Mutex<TestBackendTopologyState>,
    }

    impl TestBackendTopologyPort {
        fn with_live_backend(endpoint: SocketAddr) -> Self {
            let mut entries = BTreeMap::new();
            entries.insert(
                0,
                TestBackendTopologyEntry {
                    endpoint,
                    state: TestBackendStatus::Live,
                    scheduled_fragments: 0,
                    start_epoch: 1,
                },
            );
            Self {
                state: Mutex::new(TestBackendTopologyState {
                    entries,
                    next_backend_idx: 1,
                }),
            }
        }
    }

    impl BackendTopologyPort for TestBackendTopologyPort {
        fn snapshot(
            &self,
        ) -> Result<
            crate::query_execution::backend::BackendTopologySnapshot,
            crate::query_execution::backend::BackendTopologyError,
        > {
            let targets = self
                .state
                .lock()
                .unwrap()
                .entries
                .iter()
                .filter_map(|(backend_idx, entry)| {
                    (entry.state == TestBackendStatus::Live).then_some(LiveBackendTarget::new(
                        *backend_idx,
                        entry.endpoint,
                        entry.start_epoch,
                    ))
                })
                .collect();
            crate::query_execution::backend::BackendTopologySnapshot::try_new(0, targets)
        }

        fn validate_snapshot(
            &self,
            expected: &crate::query_execution::backend::BackendTopologySnapshot,
        ) -> Result<(), crate::query_execution::backend::BackendTopologyValidationError> {
            let current = self.snapshot().map_err(
                crate::query_execution::backend::BackendTopologyValidationError::Unavailable,
            )?;
            if current == *expected {
                Ok(())
            } else {
                Err(crate::query_execution::backend::BackendTopologyValidationError::ContentChangedWithoutRevision {
                    revision: expected.revision(),
                })
            }
        }

        fn record_successful_stage(&self, backend_idx: usize, fragment_count: usize) {
            if let Some(entry) = self.state.lock().unwrap().entries.get_mut(&backend_idx) {
                entry.scheduled_fragments = entry
                    .scheduled_fragments
                    .saturating_add(fragment_count as u64);
            }
        }

        fn add_backend(&self, endpoint: SocketAddr) -> Result<(), String> {
            let mut state = self.state.lock().unwrap();
            if state
                .entries
                .values()
                .any(|entry| entry.endpoint == endpoint)
            {
                return Ok(());
            }
            let backend_idx = state.next_backend_idx;
            state.next_backend_idx = state
                .next_backend_idx
                .checked_add(1)
                .ok_or_else(|| "test backend id overflow".to_string())?;
            state.entries.insert(
                backend_idx,
                TestBackendTopologyEntry {
                    endpoint,
                    state: TestBackendStatus::Registering,
                    scheduled_fragments: 0,
                    start_epoch: 1,
                },
            );
            Ok(())
        }

        fn drop_backend(&self, endpoint: SocketAddr, _force: bool) -> Result<(), String> {
            let mut state = self.state.lock().unwrap();
            let backend_idx = state
                .entries
                .iter()
                .find_map(|(backend_idx, entry)| {
                    (entry.endpoint == endpoint).then_some(*backend_idx)
                })
                .ok_or_else(|| format!("backend {endpoint} not found"))?;
            state.entries.remove(&backend_idx);
            Ok(())
        }

        fn show_backends(&self) -> Result<QueryResult, String> {
            let column_names = [
                "BackendId",
                "Host",
                "GrpcPort",
                "State",
                "ScheduledFragments",
                "StartEpoch",
            ];
            let mut columns = vec![Vec::<String>::new(); column_names.len()];
            for (backend_idx, entry) in &self.state.lock().unwrap().entries {
                columns[0].push(backend_idx.to_string());
                columns[1].push(entry.endpoint.ip().to_string());
                columns[2].push(entry.endpoint.port().to_string());
                columns[3].push(entry.state.as_str().to_string());
                columns[4].push(entry.scheduled_fragments.to_string());
                columns[5].push(entry.start_epoch.to_string());
            }
            let fields = column_names
                .iter()
                .map(|name| Field::new(*name, DataType::Utf8, false))
                .collect::<Vec<_>>();
            let arrays = columns
                .into_iter()
                .map(|values| Arc::new(StringArray::from(values)) as Arc<dyn arrow::array::Array>)
                .collect::<Vec<_>>();
            let batch =
                arrow::record_batch::RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
                    .map_err(|error| format!("build test SHOW BACKENDS result failed: {error}"))?;
            Ok(QueryResult {
                columns: column_names
                    .iter()
                    .map(|name| crate::runtime::query_result::QueryResultColumn {
                        name: (*name).to_string(),
                        data_type: DataType::Utf8,
                        nullable: false,
                        logical_type: None,
                    })
                    .collect(),
                chunks: vec![crate::runtime::query_result::record_batch_to_chunk(batch)?],
            })
        }
    }

    fn test_open_services() -> StandaloneOpenServices {
        test_open_services_with(
            Arc::new(crate::engine::system_catalog::EmptySystemCatalog),
            Arc::new(crate::engine::view::EmptyViewService),
        )
    }

    fn test_open_services_with(
        system_catalog: Arc<dyn SystemCatalog>,
        view_service: Arc<dyn ViewService>,
    ) -> StandaloneOpenServices {
        test_open_services_with_statistics(
            system_catalog,
            view_service,
            Arc::new(crate::engine::statistics::EmptyStatisticsService),
        )
    }

    fn test_open_services_with_statistics(
        system_catalog: Arc<dyn SystemCatalog>,
        view_service: Arc<dyn ViewService>,
        statistics_service: Arc<dyn StatisticsService>,
    ) -> StandaloneOpenServices {
        test_open_services_with_statistics_and_topology(
            system_catalog,
            view_service,
            statistics_service,
            Arc::new(TestBackendTopologyPort::default()),
        )
    }

    fn test_open_services_with_topology(
        system_catalog: Arc<dyn SystemCatalog>,
        view_service: Arc<dyn ViewService>,
        backend_topology: Arc<dyn BackendTopologyPort>,
    ) -> StandaloneOpenServices {
        test_open_services_with_statistics_and_topology(
            system_catalog,
            view_service,
            Arc::new(crate::engine::statistics::EmptyStatisticsService),
            backend_topology,
        )
    }

    fn test_open_services_with_statistics_and_topology(
        system_catalog: Arc<dyn SystemCatalog>,
        view_service: Arc<dyn ViewService>,
        statistics_service: Arc<dyn StatisticsService>,
        backend_topology: Arc<dyn BackendTopologyPort>,
    ) -> StandaloneOpenServices {
        let connector_control = Arc::new(super::TestConnectorControlRegistry::default());
        StandaloneOpenServices::new(
            crate::common::app_config::ClusterRole::AllInOne,
            system_catalog,
            view_service,
            statistics_service,
            Arc::new(crate::engine::table_maintenance::EmptyTableMaintenanceService),
            super::test_mv_repository(),
            Arc::new(PassthroughMvApplicationService),
            super::test_query_execution_service(),
            Arc::new(crate::query_execution::backend::NoopBackendQueryEventSink),
            backend_topology,
            Arc::new(crate::query_execution::backend::NoopCoordinatorReportEndpointSink),
            crate::query_execution::control::QueryControlService::for_test(),
            Arc::clone(&connector_control)
                as Arc<dyn novarocks_spi::connector::ConnectorControlRegistry>,
            connector_control
                as Arc<dyn novarocks_spi::connector::ConnectorControlFactoryResolver>,
            1,
        )
        // Production installs the provider-specific inspector from the Server
        // composition root; `new` leaves a fail-closed port so a composition
        // that forgets it cannot silently observe nothing. Tests need the same
        // observation the Server would provide, otherwise any statement that
        // consults the MV guard fails on the port rather than on its own
        // behaviour.
        .with_mv_storage_observation(Arc::new(
            crate::engine::mv::schema_validation_adapter::TestIcebergMvStorageObservationAdapter::default(),
        ))
    }

    struct RecordingQueryExecutionCoordinator {
        calls: Arc<AtomicUsize>,
    }

    impl DistributedQueryCoordinator for RecordingQueryExecutionCoordinator {
        fn execute(
            &self,
            request: DistributedQueryRequest,
        ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            request
                .into_parts()
                .completion
                .result(crate::runtime::query_result::QueryResult::empty())
        }
    }

    #[test]
    fn standalone_query_uses_injected_query_execution_service() {
        let calls = Arc::new(AtomicUsize::new(0));
        let state = Arc::new(StandaloneState {
            exchange_port: 1,
            query_execution: QueryExecutionService::new(Arc::new(
                RecordingQueryExecutionCoordinator {
                    calls: Arc::clone(&calls),
                },
            )),
            ..Default::default()
        });
        register_connector_backends(&state);
        let session = StandaloneSession { inner: state };

        session
            .query("SELECT 1")
            .expect("injected coordinator serves the planned query");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn compiler_prepares_once_and_frontend_can_submit_once() {
        let calls = Arc::new(AtomicUsize::new(0));
        let state = Arc::new(StandaloneState {
            exchange_port: 1,
            query_execution: QueryExecutionService::new(Arc::new(
                RecordingQueryExecutionCoordinator {
                    calls: Arc::clone(&calls),
                },
            )),
            ..Default::default()
        });
        register_connector_backends(&state);
        let engine = StandaloneNovaRocks {
            inner: Arc::clone(&state),
        };
        let context = super::test_request_context(None, super::DEFAULT_DATABASE);
        let operation = engine
            .query_compiler()
            .prepare("SELECT 1", &context, None)
            .expect("compiler prepares distributed query");

        let super::PreparedQueryOperation::Distributed(operation) = operation else {
            panic!("SELECT must produce one distributed operation");
        };
        let (request, completion) = operation.into_parts();
        let outcome = state
            .query_execution
            .execute(request)
            .expect("frontend submission succeeds");
        assert!(matches!(
            completion
                .complete(outcome)
                .expect("completion formats result"),
            StatementResult::Query(_)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn standalone_state_default_has_empty_system_catalog() {
        let names = vec!["a".to_string()];
        let inputs = SystemCatalogInputs {
            catalog_name: "default_catalog",
            schema_names: &names,
        };
        assert!(
            StandaloneState::default()
                .system_catalog
                .resolve("information_schema", "schemata", &inputs)
                .unwrap()
                .is_none()
        );
    }

    #[derive(Default)]
    struct RecordingViewService {
        statements: AtomicUsize,
        rewrites: AtomicUsize,
        dropped_databases: Mutex<Vec<(String, String)>>,
    }

    impl ViewService for RecordingViewService {
        fn try_handle_statement(
            &self,
            _engine: &dyn ViewEngine,
            sql: &str,
            _context: ViewRequestContext<'_>,
        ) -> Result<Option<ViewStatementResult>, String> {
            if sql.to_ascii_lowercase().starts_with("create view ") {
                self.statements.fetch_add(1, Ordering::SeqCst);
                return Ok(Some(ViewStatementResult::Ok));
            }
            Ok(None)
        }

        fn rewrite_query(
            &self,
            _engine: &dyn ViewEngine,
            _query: &mut sqlparser::ast::Query,
            _context: ViewRequestContext<'_>,
        ) -> Result<(), String> {
            self.rewrites.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn drop_database(&self, catalog: &str, database: &str) -> Result<(), String> {
            self.dropped_databases
                .lock()
                .expect("dropped databases")
                .push((catalog.to_string(), database.to_string()));
            Ok(())
        }
    }

    #[test]
    fn engine_delegates_view_statements_rewrites_and_database_cleanup_once() {
        let service = Arc::new(RecordingViewService::default());
        let engine = StandaloneNovaRocks::open_with_config(
            StandaloneOptions::default(),
            crate::common::app_config::NovaRocksConfig::default(),
            test_open_services_with(
                Arc::new(crate::engine::system_catalog::EmptySystemCatalog),
                Arc::clone(&service) as Arc<dyn ViewService>,
            ),
        )
        .expect("open engine with recording view service");
        let session = engine.session();

        session
            .execute("CREATE VIEW delegated AS SELECT 1")
            .expect("view DDL must delegate");
        session.query("SELECT 1").expect("SELECT must execute");
        session
            .query("EXPLAIN SELECT 1")
            .expect("EXPLAIN must execute");
        let warehouse = TempDir::new().expect("view delegation warehouse");
        session
            .execute(&format!(
                r#"CREATE EXTERNAL CATALOG delegated_catalog PROPERTIES("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
                warehouse.path().display()
            ))
            .expect("catalog create");
        session
            .execute_in_context(
                "CREATE DATABASE delegated_db",
                Some("delegated_catalog"),
                "",
                None,
            )
            .expect("database create");
        session
            .execute_in_context(
                "DROP DATABASE delegated_db",
                Some("delegated_catalog"),
                "",
                None,
            )
            .expect("database drop");
        let missing_catalog_error = session
            .execute_in_context("DROP DATABASE unqualified_without_catalog", None, "", None)
            .expect_err("unqualified database drop without a catalog must keep failing");
        assert!(
            missing_catalog_error.contains("requires an Iceberg catalog"),
            "unexpected missing-catalog error: {missing_catalog_error}"
        );
        session
            .execute_in_context(
                "DROP DATABASE default_catalog.delegated_view_db",
                Some("default_catalog"),
                "",
                None,
            )
            .expect("default-catalog view database drop");

        assert_eq!(service.statements.load(Ordering::SeqCst), 1);
        assert_eq!(service.rewrites.load(Ordering::SeqCst), 2);
        assert_eq!(
            service
                .dropped_databases
                .lock()
                .expect("dropped databases")
                .as_slice(),
            [
                ("delegated_catalog".to_string(), "delegated_db".to_string()),
                (
                    "default_catalog".to_string(),
                    "delegated_view_db".to_string()
                ),
            ]
        );
    }

    struct TestSchemataCatalog;

    impl SystemCatalog for TestSchemataCatalog {
        fn resolve(
            &self,
            db: &str,
            tbl: &str,
            _inputs: &SystemCatalogInputs<'_>,
        ) -> Result<Option<SystemTableData>, String> {
            if !(db.eq_ignore_ascii_case("information_schema")
                && tbl.eq_ignore_ascii_case("schemata"))
            {
                return Ok(None);
            }
            let columns = vec![
                ("catalog_name", false),
                ("schema_name", false),
                ("default_character_set_name", false),
                ("default_collation_name", false),
                ("sql_path", true),
            ]
            .into_iter()
            .map(|(name, nullable)| novarocks_catalog::schema::ColumnDef {
                name: name.to_string(),
                data_type: DataType::Utf8,
                nullable,
                write_default: None,
                logical_type: None,
            })
            .collect();
            let schema = Arc::new(Schema::new(vec![
                Field::new("catalog_name", DataType::Utf8, false),
                Field::new("schema_name", DataType::Utf8, false),
                Field::new("default_character_set_name", DataType::Utf8, false),
                Field::new("default_collation_name", DataType::Utf8, false),
                Field::new("sql_path", DataType::Utf8, true),
            ]));
            let batch = arrow::record_batch::RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["default_catalog"])),
                    Arc::new(StringArray::from(vec!["injected_schema"])),
                    Arc::new(StringArray::from(vec!["utf8"])),
                    Arc::new(StringArray::from(vec!["utf8_general_ci"])),
                    Arc::new(StringArray::from(vec![None::<&str>])),
                ],
            )
            .map_err(|error| error.to_string())?;
            Ok(Some(SystemTableData {
                columns,
                batches: vec![batch],
            }))
        }
    }

    #[test]
    fn open_with_config_injected_system_catalog_serves_schemata() {
        let cfg = crate::common::app_config::NovaRocksConfig::default();
        let engine = StandaloneNovaRocks::open_with_config(
            StandaloneOptions::default(),
            cfg,
            test_open_services_with(
                Arc::new(TestSchemataCatalog),
                Arc::new(crate::engine::view::EmptyViewService),
            ),
        )
        .expect("open engine with injected system catalog");

        let result = engine
            .session()
            .execute_in_context(
                "SELECT schema_name FROM information_schema.schemata",
                None,
                "",
                None,
            )
            .expect("query injected information_schema.schemata");
        let StatementResult::Query(result) = result else {
            panic!("expected query result");
        };
        assert!(result.row_count() > 0);
    }

    #[test]
    fn explain_analyze_query_options_only_enable_profile() {
        assert_eq!(
            super::query_options_for_explain_analyze(None),
            QueryOptions {
                enable_profile: true,
                ..Default::default()
            }
        );

        let spill = SpillConfig {
            enable_spill: true,
            spill_mode: SpillMode::Auto,
            spill_mem_limit_threshold: Some(0.7),
            spill_operator_min_bytes: Some(64),
            spill_operator_max_bytes: Some(1024),
            spill_encode_level: Some(3),
            enable_spill_buffer_read: Some(true),
            max_spill_read_buffer_bytes_per_driver: Some(4096),
            spill_mem_table_size: Some(256),
            spill_mem_table_num: Some(4),
        };
        let options = QueryOptions {
            pipeline_dop: Some(3),
            query_timeout: Some(90),
            spill: Some(spill),
            ..Default::default()
        };
        let mut expected = options.clone();
        expected.enable_profile = true;

        assert_eq!(
            super::query_options_for_explain_analyze(Some(options)),
            expected
        );
    }

    fn string_cell(result: &QueryResult, row: usize, col: usize) -> String {
        let batch = &result.chunks[0].batch;
        let array = batch
            .column(col)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray column");
        array.value(row).to_string()
    }

    fn string_column(result: &QueryResult, col: usize) -> Vec<String> {
        let mut out = Vec::new();
        for chunk in &result.chunks {
            let array = chunk
                .batch
                .column(col)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("StringArray column");
            for row in 0..array.len() {
                out.push(array.value(row).to_string());
            }
        }
        out
    }

    fn write_test_metadata_config(dir: &TempDir, metadata_path: &str) -> PathBuf {
        let config_path = dir.path().join("novarocks.toml");
        std::fs::write(
            &config_path,
            format!(
                r#"[metadata]
provider = "sqlite"
path = "{metadata_path}"
"#
            ),
        )
        .expect("write metadata config");
        config_path
    }

    fn assert_backend_topology_column_contract(result: &QueryResult) {
        assert_eq!(
            result
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            [
                "BackendId",
                "Host",
                "GrpcPort",
                "State",
                "ScheduledFragments",
                "StartEpoch",
            ]
        );
    }

    fn lock_runtime_test_state() -> super::TestSerializationGuard {
        super::acquire_standalone_test_guard()
    }

    #[test]
    fn backend_management_sql_add_show_drop_force() {
        let mut cfg = crate::common::app_config::NovaRocksConfig::default();
        cfg.cluster.role = crate::common::app_config::ClusterRole::Fe;
        cfg.cluster.backends.clear();
        let engine = StandaloneNovaRocks::open_with_config(
            StandaloneOptions::default(),
            cfg,
            test_open_services(),
        )
        .expect("open FE engine");
        let context = super::test_request_context_with_role(
            None,
            "default",
            crate::common::app_config::ClusterRole::Fe,
        );
        let commands = engine.command_executor();

        commands
            .execute("ADD BACKEND '127.0.0.1:19170'", &context, None)
            .expect("ADD BACKEND");
        let StatementResult::Query(result) = commands
            .execute("SHOW BACKENDS", &context, None)
            .expect("SHOW BACKENDS")
        else {
            panic!("SHOW BACKENDS must return rows");
        };
        assert_backend_topology_column_contract(&result);
        assert_eq!(result.row_count(), 1);
        assert_eq!(string_cell(&result, 0, 1), "127.0.0.1");
        assert_eq!(string_cell(&result, 0, 2), "19170");
        assert_eq!(string_cell(&result, 0, 3), "Registering");

        commands
            .execute("DROP BACKEND '127.0.0.1:19170' FORCE", &context, None)
            .expect("DROP BACKEND FORCE");
        let StatementResult::Query(result) = commands
            .execute("SHOW BACKENDS", &context, None)
            .expect("SHOW BACKENDS")
        else {
            panic!("SHOW BACKENDS must return rows");
        };
        assert_eq!(result.row_count(), 0);
    }

    #[test]
    fn add_backend_requires_fe_role_but_show_backends_works_in_all_in_one() {
        let mut cfg = crate::common::app_config::NovaRocksConfig::default();
        cfg.cluster.role = crate::common::app_config::ClusterRole::AllInOne;
        let services = test_open_services_with_topology(
            Arc::new(crate::engine::system_catalog::EmptySystemCatalog),
            Arc::new(crate::engine::view::EmptyViewService),
            Arc::new(TestBackendTopologyPort::with_live_backend(
                "127.0.0.1:1".parse().unwrap(),
            )),
        );
        let engine =
            StandaloneNovaRocks::open_with_config(StandaloneOptions::default(), cfg, services)
                .expect("open all-in-one engine");
        let context = super::test_request_context_with_role(
            None,
            "default",
            crate::common::app_config::ClusterRole::AllInOne,
        );
        let commands = engine.command_executor();

        let err = commands
            .execute("ADD BACKEND '127.0.0.1:19171'", &context, None)
            .expect_err("ADD BACKEND must require FE role");
        assert!(err.contains("requires role=fe"), "{err}");

        let StatementResult::Query(result) = commands
            .execute("SHOW BACKENDS", &context, None)
            .expect("SHOW BACKENDS")
        else {
            panic!("SHOW BACKENDS must return rows");
        };
        assert_backend_topology_column_contract(&result);
        assert_eq!(result.row_count(), 1);
        assert_eq!(string_cell(&result, 0, 0), "0");
        assert_eq!(string_cell(&result, 0, 1), "127.0.0.1");
        assert_eq!(
            string_cell(&result, 0, 2),
            engine.inner.exchange_port.to_string()
        );
        assert_eq!(string_cell(&result, 0, 3), "Live");
        assert_eq!(string_cell(&result, 0, 4), "0");
    }

    #[test]
    fn create_catalog_registers_catalog_service_entry() {
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default(), test_open_services())
            .expect("open");
        let warehouse = TempDir::new().expect("warehouse");
        let sql = format!(
            r#"CREATE EXTERNAL CATALOG ice PROPERTIES("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        engine.session().execute(&sql).expect("create catalog");
        engine
            .session()
            .execute(&sql.replacen(
                "CREATE EXTERNAL CATALOG",
                "CREATE EXTERNAL CATALOG IF NOT EXISTS",
                1,
            ))
            .expect("CREATE CATALOG IF NOT EXISTS must keep the active control binding");

        let registry = engine
            .inner
            .catalog_service
            .registry()
            .read()
            .expect("catalog service registry");
        assert!(registry.get_catalog("ice").is_ok());
    }

    #[test]
    fn metadata_backend_resolves_metadata_path_relative_to_config_parent() {
        let _runtime_guard = lock_runtime_test_state();
        let dir = TempDir::new().expect("create config dir");
        let config_dir = dir.path().join("conf");
        std::fs::create_dir_all(&config_dir).expect("create config dir");
        let config_path = config_dir.join("novarocks.toml");
        std::fs::write(
            &config_path,
            r#"[metadata]
provider = "sqlite"
path = "meta/catalog.db"
"#,
        )
        .expect("write config");

        let cfg = crate::novarocks_config::load_from_path(&config_path).expect("load config");
        let backend = super::resolve_metadata_backend(
            &StandaloneOptions {
                config_path: Some(config_path.clone()),
            },
            &cfg,
        )
        .expect("resolve backend")
        .expect("metadata backend");

        assert_eq!(
            backend.provider,
            crate::common::app_config::MetadataProviderConfig::Sqlite
        );
        assert_eq!(backend.path, config_dir.join("meta/catalog.db"));
    }

    #[test]
    fn metadata_backend_is_absent_without_metadata_config() {
        let _runtime_guard = lock_runtime_test_state();
        let dir = TempDir::new().expect("create config dir");
        let config_path = dir.path().join("novarocks.toml");
        std::fs::write(
            &config_path,
            r#"[standalone_server]
mysql_port = 19030
"#,
        )
        .expect("write config");

        let cfg = crate::novarocks_config::load_from_path(&config_path).expect("load config");
        let backend = super::resolve_metadata_backend(
            &StandaloneOptions {
                config_path: Some(config_path.clone()),
            },
            &cfg,
        )
        .expect("resolve backend");
        assert!(backend.is_none());
    }

    #[test]
    fn standalone_state_retains_metadata_provider_from_metadata_config() {
        let dir = TempDir::new().expect("create config dir");
        let config_path = dir.path().join("novarocks.toml");
        std::fs::write(
            &config_path,
            r#"[metadata]
provider = "sqlite"
path = "meta/catalog.db"
"#,
        )
        .expect("write config");

        let engine = StandaloneNovaRocks::open(
            StandaloneOptions {
                config_path: Some(config_path),
            },
            test_open_services(),
        )
        .expect("open engine");

        assert!(engine.inner.metadata_provider.is_some());
        assert_eq!(
            engine
                .inner
                .metadata_provider
                .as_ref()
                .expect("metadata provider")
                .provider_name(),
            "sqlite"
        );
    }

    // I1: open_with_config must use the supplied NovaRocksConfig instead of
    // re-reading from disk.  If the file is overwritten with invalid TOML after
    // load but before open_with_config, the call must still succeed.
    #[test]
    fn open_with_config_does_not_reread_config_file() {
        let dir = TempDir::new().expect("create config dir");
        let config_path = dir.path().join("novarocks.toml");

        // Write a valid config to disk.
        std::fs::write(
            &config_path,
            r#"[standalone_server]
mysql_port = 47892
"#,
        )
        .expect("write sentinel config");

        // Load the config before corrupting the file.
        let cfg = crate::novarocks_config::NovaRocksConfig::load_from_file(&config_path)
            .expect("load sentinel config");
        assert_eq!(
            cfg.standalone_server.as_ref().map(|s| s.mysql_port),
            Some(47892),
            "preloaded config must contain sentinel port"
        );

        // Overwrite the file with invalid TOML — a reload from disk would fail.
        std::fs::write(&config_path, "NOT VALID TOML !!!").expect("corrupt config file");

        // open_with_config must use the preloaded cfg, not re-read the corrupted file.
        let result = StandaloneNovaRocks::open_with_config(
            StandaloneOptions {
                config_path: Some(config_path),
            },
            cfg,
            test_open_services(),
        );
        assert!(
            result.is_ok(),
            "open_with_config must succeed with preloaded config even when the config file is \
             invalid; got: {:?}",
            result.err()
        );
    }

    #[test]
    fn alter_iceberg_schema_dispatches_before_generic_sqlparser() {
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default(), test_open_services())
            .expect("engine");
        let err = engine
            .session()
            .execute("ALTER TABLE missing.db.t ADD COLUMN c INT")
            .expect_err("unknown catalog");
        // What this pins is the dispatch, not the wording: ALTER TABLE must
        // reach the Iceberg schema path and fail there on the unknown catalog,
        // rather than falling through to the generic sqlparser statement
        // handler. Naming the catalog in the message is what tells the two
        // apart -- generic parsing never resolves a connector instance.
        assert!(
            err.contains("missing") && err.contains("connector control instance"),
            "ALTER TABLE must fail while resolving the Iceberg catalog; got: {err}"
        );
    }

    fn query_result_contains_string(result: &QueryResult, expected: &str) -> bool {
        result.chunks.iter().any(|chunk| {
            chunk.batch.columns().iter().any(|column| {
                column
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .is_some_and(|values| {
                        (0..values.len())
                            .any(|idx| !values.is_null(idx) && values.value(idx).contains(expected))
                    })
            })
        })
    }

    fn parse_query_for_engine_test(sql: &str) -> sqlparser::ast::Query {
        let normalized =
            crate::sql::parser::dialect::normalize_for_raw_parse(sql).expect("normalize sql");
        let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
            .expect("parse query statement");
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("expected query statement");
        };
        *query
    }

    #[test]
    fn sqlx2_application_change_stream_kernel_retains_admission_binding_store() {
        use crate::common::app_config::ClusterRole;
        use crate::engine::query_planning::bindings::QueryTableBindingStore;
        use crate::query_execution::backend::BackendTopologySnapshot;
        use crate::query_execution::cancellation::QueryCancellationSource;
        use crate::query_execution::request_context::QueryExecutionContext;

        let query = parse_query_for_engine_test("select 1");
        let catalog = super::PlannerMemoryCatalog::default();
        let bindings = Arc::new(QueryTableBindingStore::try_new().expect("binding store"));
        let topology = BackendTopologySnapshot::try_new(
            7,
            vec![LiveBackendTarget::new(
                3,
                "127.0.0.1:9030".parse().expect("backend endpoint"),
                11,
            )],
        )
        .expect("frozen topology");
        let cancellation = QueryCancellationSource::new();
        let execution = QueryExecutionContext::new(
            ClusterRole::Fe,
            topology,
            None,
            cancellation.view(),
            crate::sql::optimizer::options::SessionOptimizerSettings::default(),
        );

        let planned = super::plan_query_for_iceberg_change_stream_refresh(
            &Arc::new(super::StandaloneState::default()),
            &query,
            &catalog,
            "default",
            None,
            Arc::clone(&bindings),
            &execution,
        )
        .expect("change-stream compilation");

        assert!(Arc::ptr_eq(
            planned
                .table_bindings
                .as_ref()
                .expect("kernel must retain admission bindings"),
            &bindings,
        ));
    }

    #[test]
    fn change_stream_write_planning_disables_query_runtime_filters() {
        assert!(!super::change_stream_write_optimizer_settings().global_runtime_filter_enabled());
    }

    #[test]
    fn sqlparser_insert_values_preserves_array_literals() {
        use crate::sql::parser::dialect::StarRocksDialect;

        let statements = sqlparser::parser::Parser::parse_sql(
            &StarRocksDialect,
            "INSERT INTO t VALUES (1, [1, NULL, 3], ['a', NULL, 'c'])",
        )
        .expect("parse insert");
        let sqlparser::ast::Statement::Insert(insert) = &statements[0] else {
            panic!("expected insert statement");
        };
        let source = insert.source.as_ref().expect("insert source");
        let sqlparser::ast::SetExpr::Values(values) = source.body.as_ref() else {
            panic!("expected values source");
        };
        let row = &values.rows[0];

        assert_eq!(
            super::sqlparser_expr_to_literal(&row[1]).expect("parse int array"),
            crate::sql::parser::ast::Literal::Array(vec![
                crate::sql::parser::ast::Literal::Int(1),
                crate::sql::parser::ast::Literal::Null,
                crate::sql::parser::ast::Literal::Int(3),
            ])
        );
        assert_eq!(
            super::sqlparser_expr_to_literal(&row[2]).expect("parse string array"),
            crate::sql::parser::ast::Literal::Array(vec![
                crate::sql::parser::ast::Literal::String("a".to_string()),
                crate::sql::parser::ast::Literal::Null,
                crate::sql::parser::ast::Literal::String("c".to_string()),
            ])
        );
    }

    #[test]
    fn sqlparser_insert_values_preserves_large_integer_literals() {
        use crate::sql::parser::ast::Literal;
        use crate::sql::parser::dialect::StarRocksDialect;

        let statements = sqlparser::parser::Parser::parse_sql(
            &StarRocksDialect,
            "INSERT INTO t VALUES (-9223372036854775808, -170141183460469231731687303715884105728)",
        )
        .expect("parse insert");
        let sqlparser::ast::Statement::Insert(insert) = &statements[0] else {
            panic!("expected insert statement");
        };
        let source = insert.source.as_ref().expect("insert source");
        let sqlparser::ast::SetExpr::Values(values) = source.body.as_ref() else {
            panic!("expected values source");
        };
        let row = &values.rows[0];

        assert_eq!(
            super::sqlparser_expr_to_literal(&row[0]).expect("parse BIGINT literal"),
            Literal::String("-9223372036854775808".to_string())
        );
        assert_eq!(
            super::sqlparser_expr_to_literal(&row[1]).expect("parse LARGEINT literal"),
            Literal::String("-170141183460469231731687303715884105728".to_string())
        );
    }

    #[test]
    fn sql_type_to_arrow_type_maps_largeint_to_fixed_size_binary() {
        assert_eq!(
            super::sql_type_to_arrow_type(&novarocks_catalog::schema::SqlType::LargeInt)
                .expect("map largeint type"),
            DataType::FixedSizeBinary(novarocks_types::largeint::LARGEINT_BYTE_WIDTH)
        );
    }

    #[test]
    fn embedded_query_executes_inline_values_cte_without_catalog_table() {
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default(), test_open_services())
            .expect("open engine");

        let session = engine.session();
        let result = session
            .query("WITH t AS (SELECT 1 AS id UNION ALL SELECT 2) SELECT id FROM t ORDER BY id")
            .expect("execute inline values CTE");
        assert_eq!(result.row_count(), 2);
    }

    #[test]
    fn embedded_query_math_function_accepts_negative_decimal_literal() {
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default(), test_open_services())
            .expect("open engine");

        let session = engine.session();
        let result = session
            .query("SELECT SQRT(-1.0) AS v")
            .expect("execute math function with negative decimal literal");
        assert_eq!(result.row_count(), 1);
    }

    #[test]
    fn embedded_query_rejects_unknown_table() {
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default(), test_open_services())
            .expect("open engine");
        let session = engine.session();
        let err = session
            .query("select * from missing")
            .expect_err("missing table");
        assert!(err.contains("unknown table"));
    }

    #[test]
    fn embedded_session_supports_minimal_iceberg_flow() {
        let warehouse = TempDir::new().expect("create iceberg warehouse");
        let engine = open_test_engine_with_metadata(&warehouse);
        let session = engine.session();

        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        let create_catalog = session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create iceberg catalog");
        assert!(matches!(create_catalog, StatementResult::Ok));

        let create_database = session
            .execute_in_database("create database ice.db1", "default")
            .expect("create iceberg database");
        assert!(matches!(create_database, StatementResult::Ok));

        let create_table = session
            .execute_in_database("create table ice.db1.tbl (id int, name string)", "default")
            .expect("create iceberg table");
        assert!(matches!(create_table, StatementResult::Ok));

        let empty_result = session
            .query("select id, name from ice.db1.tbl limit 0")
            .expect("query empty iceberg table");
        assert_eq!(empty_result.row_count(), 0);
        assert_eq!(empty_result.columns[0].name, "id");
        assert_eq!(empty_result.columns[1].name, "name");

        insert_rows!(&session, ["ice", "db1", "tbl"]; [1, "a"], [2, "b"]);

        let result = session
            .query("select name from ice.db1.tbl where id = 2")
            .expect("query iceberg table");
        assert_eq!(result.row_count(), 1);
        let chunk = &result.chunks[0];
        let names = chunk.batch.column(0);
        let names = names
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("string array");
        assert_eq!(names.value(0), "b");
    }

    #[test]
    fn embedded_session_preserves_iceberg_projection_order() {
        let warehouse = TempDir::new().expect("create iceberg warehouse");
        let engine = open_test_engine_with_metadata(&warehouse);
        let session = engine.session();

        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        let create_catalog = session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create iceberg catalog");
        assert!(matches!(create_catalog, StatementResult::Ok));

        let create_database = session
            .execute_in_database("create database ice.db1", "default")
            .expect("create iceberg database");
        assert!(matches!(create_database, StatementResult::Ok));

        let create_table = session
            .execute_in_database("create table ice.db1.tbl (id int, name string)", "default")
            .expect("create iceberg table");
        assert!(matches!(create_table, StatementResult::Ok));

        insert_rows!(&session, ["ice", "db1", "tbl"]; [1, "a"], [2, "b"]);

        let result = session
            .query("select name, id from ice.db1.tbl where id = 2")
            .expect("query iceberg table");
        assert_eq!(result.row_count(), 1);
        let chunk = &result.chunks[0];
        assert_eq!(chunk.schema().field(0).name(), "name");
        assert_eq!(chunk.schema().field(1).name(), "id");
        let names = chunk.batch.column(0);
        let names = names
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("string array");
        assert_eq!(names.value(0), "b");
        let ids = chunk.batch.column(1);
        let ids = ids
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("int32 array");
        assert_eq!(ids.value(0), 2);
    }

    #[test]
    fn iceberg_refresh_load_failure_does_not_use_stale_external_metadata() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        insert_rows!(&session, ["ice", "db1", "t"]; [1, "a"]);
        session
            .query("select id from ice.db1.t")
            .expect("query iceberg table");
        assert!(
            !engine.has_local_table("db1", "t"),
            "ordinary iceberg SELECT should not register a local catalog table"
        );

        let entry = {
            let registry = engine.inner.iceberg_catalogs.read().expect("registry");
            registry.get("ice").expect("catalog entry")
        };
        crate::connector::iceberg::catalog::registry::drop_table(&entry, "db1", "t")
            .expect("drop backing iceberg table");

        let err = session
            .query("select id from ice.db1.t")
            .expect_err("dropped backing table should not use stale local table");
        assert!(
            err.contains("unknown iceberg table") || err.contains("unknown table"),
            "err={err}"
        );
        assert!(
            !engine.has_local_table("db1", "t"),
            "failed refresh should not leave a local catalog table"
        );
    }

    #[test]
    fn drop_iceberg_table_invalidates_external_metadata_without_local_registration() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        insert_rows!(&session, ["ice", "db1", "t"]; [1, "a"]);
        session
            .query("select id from ice.db1.t")
            .expect("query iceberg table");
        assert!(
            !engine.has_local_table("db1", "t"),
            "ordinary iceberg SELECT should not register a local catalog table"
        );

        let drop = session
            .execute_in_database("drop table ice.db1.t", "default")
            .expect("drop iceberg table");
        assert!(matches!(drop, StatementResult::Ok));
        assert!(
            !engine.has_local_table("db1", "t"),
            "drop table should keep the local catalog clear"
        );
        let err = session
            .query("select id from ice.db1.t")
            .expect_err("dropped iceberg table should not be queryable");
        assert!(
            err.contains("unknown iceberg table") || err.contains("unknown table"),
            "err={err}"
        );
    }

    #[test]
    fn embedded_session_preserves_projection_order_with_current_catalog_context() {
        let warehouse = TempDir::new().expect("create iceberg warehouse");
        let engine = open_test_engine_with_metadata(&warehouse);
        let session = engine.session();

        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        let create_catalog = session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create iceberg catalog");
        assert!(matches!(create_catalog, StatementResult::Ok));

        let create_database = session
            .execute_in_database("create database ice.db1", "default")
            .expect("create iceberg database");
        assert!(matches!(create_database, StatementResult::Ok));

        let create_table = session
            .execute_in_database(
                "create table ice.db1.nums (c1 tinyint, c2 smallint)",
                "default",
            )
            .expect("create iceberg table");
        assert!(matches!(create_table, StatementResult::Ok));

        insert_rows!(&session, ["ice", "db1", "nums"]; [1, 101], [2, 102]);

        let result = session
            .execute_in_context(
                "select c2, c1 from nums order by 1, 2",
                Some("ice"),
                "db1",
                None,
            )
            .expect("query iceberg table in current catalog context");
        let StatementResult::Query(result) = result else {
            panic!("expected query result");
        };
        assert_eq!(result.columns[0].name, "c2");
        assert_eq!(result.columns[1].name, "c1");
        assert_eq!(result.row_count(), 2);

        let chunk = &result.chunks[0];
        assert_eq!(chunk.schema().field(0).name(), "c2");
        assert_eq!(chunk.schema().field(1).name(), "c1");
        assert_eq!(chunk.batch.column(0).data_type(), &DataType::Int32);
        assert_eq!(chunk.batch.column(1).data_type(), &DataType::Int32);
        let c2 = chunk.batch.column(0);
        let c2 = c2
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("int32 array");
        assert_eq!(c2.value(0), 101);
        assert_eq!(c2.value(1), 102);
        let c1 = chunk.batch.column(1);
        let c1 = c1
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("int32 array");
        assert_eq!(c1.value(0), 1);
        assert_eq!(c1.value(1), 2);
    }

    #[test]
    fn embedded_session_restores_catalog_attachment_and_reads_external_table() {
        let warehouse = TempDir::new().expect("create iceberg warehouse");
        let metadata_dir = TempDir::new().expect("create metadata dir");
        let config_path = write_test_metadata_config(&metadata_dir, "standalone.sqlite");

        {
            let engine = StandaloneNovaRocks::open(
                StandaloneOptions {
                    config_path: Some(config_path.clone()),
                },
                test_open_services(),
            )
            .expect("open engine");
            let session = engine.session();

            let create_catalog_sql = format!(
                r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
                warehouse.path().display()
            );
            let create_catalog = session
                .execute_in_database(&create_catalog_sql, "default")
                .expect("create iceberg catalog");
            assert!(matches!(create_catalog, StatementResult::Ok));

            let create_database = session
                .execute_in_database("create database ice.db1", "default")
                .expect("create iceberg database");
            assert!(matches!(create_database, StatementResult::Ok));

            let create_table = session
                .execute_in_database("create table ice.db1.tbl (id int, name string)", "default")
                .expect("create iceberg table");
            assert!(matches!(create_table, StatementResult::Ok));
        }

        let restored = StandaloneNovaRocks::open(
            StandaloneOptions {
                config_path: Some(config_path),
            },
            test_open_services(),
        )
        .expect("reopen engine");
        let result = restored
            .session()
            .execute_in_database("select id, name from ice.db1.tbl", "default")
            .expect("read restored external table");
        let StatementResult::Query(result) = result else {
            panic!("restored external table read must return rows");
        };
        assert_eq!(result.row_count(), 0);
    }

    #[test]
    fn restart_does_not_recreate_externally_deleted_iceberg_objects() {
        let warehouse = TempDir::new().expect("iceberg warehouse");
        let metadata_dir = TempDir::new().expect("metadata dir");
        let config_path = write_test_metadata_config(&metadata_dir, "standalone.sqlite");

        let entry = {
            let engine = StandaloneNovaRocks::open(
                StandaloneOptions {
                    config_path: Some(config_path.clone()),
                },
                test_open_services(),
            )
            .expect("open engine");
            let session = engine.session();
            let sql = format!(
                r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
                warehouse.path().display()
            );
            session
                .execute_in_database(&sql, "default")
                .expect("catalog");
            session
                .execute_in_database("create database ice.db1", "default")
                .expect("database");
            session
                .execute_in_database("create table ice.db1.tbl (id int)", "default")
                .expect("table");
            engine
                .inner
                .iceberg_catalogs
                .read()
                .expect("registry")
                .get("ice")
                .expect("catalog entry")
        };

        crate::connector::iceberg::catalog::registry::drop_table(&entry, "db1", "tbl")
            .expect("external table drop");
        crate::connector::iceberg::catalog::registry::drop_namespace(&entry, "db1")
            .expect("external namespace drop");

        let restored = StandaloneNovaRocks::open(
            StandaloneOptions {
                config_path: Some(config_path),
            },
            test_open_services(),
        )
        .expect("reopen without replay");
        let restored_entry = restored
            .inner
            .iceberg_catalogs
            .read()
            .expect("registry")
            .get("ice")
            .expect("restored attachment");
        assert!(
            !crate::connector::iceberg::catalog::namespace_exists(&restored_entry, "db1")
                .expect("namespace existence")
        );
        assert!(crate::connector::load_iceberg_table(&restored_entry, "db1", "tbl").is_err());
    }

    #[test]
    fn restore_metadata_registers_iceberg_mv_target_from_relationship() {
        let warehouse = TempDir::new().expect("create iceberg warehouse");
        let metadata_dir = TempDir::new().expect("create metadata dir");
        let config_path = write_test_metadata_config(&metadata_dir, "standalone.sqlite");

        {
            let engine = StandaloneNovaRocks::open(
                StandaloneOptions {
                    config_path: Some(config_path.clone()),
                },
                test_open_services(),
            )
            .expect("open engine");
            let session = engine.session();

            let create_catalog_sql = format!(
                r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
                warehouse.path().display()
            );
            assert!(matches!(
                session
                    .execute_in_database(&create_catalog_sql, "default")
                    .expect("create iceberg catalog"),
                StatementResult::Ok
            ));
            assert!(matches!(
                session
                    .execute_in_database("create database ice.analytics", "default")
                    .expect("create target namespace"),
                StatementResult::Ok
            ));
            assert!(matches!(
                session
                    .execute_in_database("create database ice.sales", "default")
                    .expect("create source namespace"),
                StatementResult::Ok
            ));
            assert!(matches!(
                session
                    .execute_in_database(
                        "create table ice.sales.orders (id int, name string) \
                         tblproperties(\"format-version\"=\"3\", \"write.row-lineage\"=\"true\")",
                        "default",
                    )
                    .expect("create base table"),
                StatementResult::Ok
            ));
            assert!(matches!(
                session
                    .execute_in_context(
                        "CREATE MATERIALIZED VIEW mv_orders \
                         DISTRIBUTED BY HASH(id) BUCKETS 1 \
                         PROPERTIES('storage_engine'='iceberg') \
                         AS SELECT id, name FROM ice.sales.orders",
                        Some("ice"),
                        "analytics",
                        None,
                    )
                    .expect("create iceberg mv"),
                StatementResult::Ok
            ));
            let drop_column_err = session
                .execute_in_database("ALTER TABLE ice.sales.orders DROP COLUMN name", "default")
                .expect_err("MV base column dependency must block DROP COLUMN");
            assert!(
                drop_column_err.contains("materialized view"),
                "err={drop_column_err}"
            );

            assert!(
                engine
                    .inner
                    .mv_repository
                    .find_by_target(&MvTarget {
                        catalog: Some("ice".to_string()),
                        database: "analytics".to_string(),
                        name: "mv_orders".to_string()
                    })
                    .expect("find iceberg mv definition")
                    .is_some()
            );
        }

        let restored = StandaloneNovaRocks::open(
            StandaloneOptions {
                config_path: Some(config_path),
            },
            test_open_services(),
        )
        .expect("reopen engine");
        let session = restored.session();
        let show = session
            .execute_in_context("SHOW MATERIALIZED VIEWS", Some("ice"), "analytics", None)
            .expect("show restored mv");
        let StatementResult::Query(show) = show else {
            panic!("expected show query result");
        };
        assert!(query_result_contains_string(&show, "mv_orders"));
        assert!(query_result_contains_string(&show, "iceberg"));

        let info_schema = session
            .execute_in_context(
                "SELECT TABLE_NAME, IS_ACTIVE \
                 FROM information_schema.materialized_views \
                 WHERE TABLE_SCHEMA = 'analytics'",
                Some("ice"),
                "analytics",
                None,
            )
            .expect("query information_schema materialized views");
        let StatementResult::Query(info_schema) = info_schema else {
            panic!("expected information_schema query result");
        };
        assert!(query_result_contains_string(&info_schema, "mv_orders"));

        let select = session
            .execute_in_context("SELECT * FROM mv_orders", Some("ice"), "analytics", None)
            .expect("select restored mv target");
        let StatementResult::Query(select) = select else {
            panic!("expected select query result");
        };
        assert_eq!(select.row_count(), 0);
    }

    #[test]
    fn dispatch_statement_routes_materialized_view_ast_variants() {
        // This test's only goal is to confirm `Statement::RefreshMaterializedView`
        // is routed to the materialized-view dispatch path (not, say, an iceberg
        // flow or a generic statement handler). The specific error message is
        // incidental — any error surfaced from inside the MV refresh handler
        // proves correct routing. Accept several signposts because the exact
        // failure point depends on which precondition is checked first
        // (catalog lookup vs. metadata-store availability) and that order has
        // shifted over time.
        let state = Arc::new(StandaloneState::default());
        register_connector_backends(&state);
        let err = dispatch_statement(
            &state,
            None,
            "analytics",
            crate::sql::parser::ast::Statement::RefreshMaterializedView(
                crate::sql::parser::ast::RefreshMaterializedViewStmt {
                    name: crate::sql::parser::ast::ObjectName {
                        parts: vec!["analytics".to_string(), "orders_mv".to_string()],
                    },
                    full: false,
                },
            ),
            &super::test_request_context(None, "analytics"),
            &crate::connector::test_request_context(),
        )
        .expect_err("refresh should fail without MV runtime prerequisites");
        assert!(
            err.contains("requires current Iceberg catalog context")
                || err.contains("sqlite metadata store")
                || err.contains("materialized view"),
            "unexpected dispatch error: {err}"
        );
    }

    #[test]
    fn refresh_dispatch_uses_frontend_refresh_entrypoint_with_admitted_context() {
        let service = Arc::new(RefreshRouteRecordingMvApplicationService::default());
        let state = Arc::new(StandaloneState {
            mv_application_service: service.clone(),
            mv_repository: super::test_mv_repository(),
            ..Default::default()
        });
        let request_context = super::test_request_context(Some("ice"), "analytics");

        let error = dispatch_statement(
            &state,
            Some("ice"),
            "analytics",
            crate::sql::parser::ast::Statement::RefreshMaterializedView(
                crate::sql::parser::ast::RefreshMaterializedViewStmt {
                    name: crate::sql::parser::ast::ObjectName {
                        parts: vec!["orders_mv".to_string()],
                    },
                    full: false,
                },
            ),
            &request_context,
            &crate::connector::test_request_context(),
        )
        .expect_err("recording frontend service returns its route marker");

        assert_eq!(error, "recorded frontend refresh route");
        let refreshes = service.refreshes.lock().expect("refresh route calls");
        assert_eq!(refreshes.len(), 1);
        let (statement, target, execution) = &refreshes[0];
        // Dispatch resolves each dependency step to a fully qualified
        // `database.name` before routing, so the frontend never has to
        // re-derive the current database from an abbreviated statement.
        assert!(matches!(
            statement,
            MvApplicationStatement::Refresh(refresh)
                if refresh.name_parts == ["analytics", "orders_mv"] && !refresh.full
        ));
        assert_eq!(target.catalog.as_deref(), Some("ice"));
        assert_eq!(target.database, "analytics");
        assert_eq!(target.name, "orders_mv");
        assert_eq!(execution.role(), request_context.execution().role());
        assert_eq!(execution.topology(), request_context.execution().topology());
    }

    #[test]
    fn create_mv_surfaces_frontend_unavailable_without_legacy_fallback() {
        let mut state = StandaloneState::default();
        state.mv_repository = Arc::new(crate::mv::test_repository::InMemoryMvRepository::default());
        state.mv_application_service = Arc::new(AlwaysUnavailableMvApplicationService);
        let state = Arc::new(state);
        register_connector_backends(&state);

        let statement = crate::sql::parser::parse_sql(
            "CREATE MATERIALIZED VIEW orders_mv DISTRIBUTED BY HASH(id) BUCKETS 1 \
                 AS SELECT 1 AS id",
        )
        .expect("parse materialized-view create")
        .pop()
        .expect("one materialized-view statement");
        let err = dispatch_statement(
            &state,
            None,
            "analytics",
            statement,
            &super::test_request_context(None, "analytics"),
            &crate::connector::test_request_context(),
        )
        .expect_err("frontend unavailable error must surface directly");

        assert_eq!(err, "injected frontend MV service is unavailable");
        assert!(
            state
                .mv_repository
                .find_by_target(&MvTarget {
                    catalog: None,
                    database: "analytics".to_string(),
                    name: "orders_mv".to_string(),
                })
                .expect("read available MV repository")
                .is_none(),
            "frontend service errors must not fall back to legacy target creation or metadata writes"
        );
    }

    #[test]
    fn custom_statement_dispatch_honors_caller_cancellation() {
        let state = Arc::new(StandaloneState::default());
        register_connector_backends(&state);
        let cancellation = Arc::new(AtomicBool::new(true));
        let context =
            crate::connector::connector_request_context(None, cancellation).expect("context");

        let error = dispatch_statement(
            &state,
            None,
            "analytics",
            crate::sql::parser::ast::Statement::RefreshMaterializedView(
                crate::sql::parser::ast::RefreshMaterializedViewStmt {
                    name: crate::sql::parser::ast::ObjectName {
                        parts: vec!["analytics".to_string(), "orders_mv".to_string()],
                    },
                    full: false,
                },
            ),
            &super::test_request_context(None, "analytics"),
            &context,
        )
        .expect_err("cancelled caller must stop custom statement dispatch");

        assert_eq!(error, "connector request was cancelled");
    }

    struct CancelAfterDispatch {
        polls: AtomicUsize,
    }

    impl novarocks_spi::connector::ConnectorCancellation for CancelAfterDispatch {
        fn is_cancelled(&self) -> bool {
            self.polls.fetch_add(1, Ordering::SeqCst) > 0
        }
    }

    fn cancel_after_dispatch_context() -> novarocks_spi::connector::ConnectorRequestContext {
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            Arc::new(CancelAfterDispatch {
                polls: AtomicUsize::new(0),
            }),
            novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .expect("connector request context")
    }

    #[test]
    fn materialized_view_dispatch_observes_cancellation_after_entry() {
        let state = Arc::new(StandaloneState {
            mv_repository: super::test_mv_repository(),
            mv_application_service: Arc::new(PassthroughMvApplicationService),
            ..Default::default()
        });
        register_connector_backends(&state);

        let error = dispatch_statement(
            &state,
            Some("ice"),
            "analytics",
            crate::sql::parser::ast::Statement::RefreshMaterializedView(
                crate::sql::parser::ast::RefreshMaterializedViewStmt {
                    name: crate::sql::parser::ast::ObjectName {
                        parts: vec!["analytics".to_string(), "orders_mv".to_string()],
                    },
                    full: false,
                },
            ),
            &super::test_request_context(Some("ice"), "analytics"),
            &cancel_after_dispatch_context(),
        )
        .expect_err("MV work dispatched by a cancelled caller must stop");

        assert_eq!(error, "connector request was cancelled");
    }

    #[test]
    fn create_materialized_view_observes_cancellation_after_dispatch() {
        let state = Arc::new(StandaloneState {
            mv_repository: super::test_mv_repository(),
            mv_application_service: Arc::new(PassthroughMvApplicationService),
            ..Default::default()
        });
        register_connector_backends(&state);
        let statement = crate::sql::parser::parse_sql(
            "CREATE MATERIALIZED VIEW orders_mv
             DISTRIBUTED BY HASH(id) BUCKETS 1
             PROPERTIES('storage_engine'='iceberg')
             AS SELECT id FROM ice.analytics.orders",
        )
        .expect("parse CREATE MATERIALIZED VIEW")
        .remove(0);

        let error = dispatch_statement(
            &state,
            Some("ice"),
            "analytics",
            statement,
            &super::test_request_context(Some("ice"), "analytics"),
            &cancel_after_dispatch_context(),
        )
        .expect_err("CREATE MV work dispatched by a cancelled caller must stop");

        assert_eq!(error, "connector request was cancelled");
    }

    #[test]
    fn iceberg_ref_dispatch_observes_cancellation_after_entry() {
        let state = Arc::new(StandaloneState::default());
        register_connector_backends(&state);

        let error = dispatch_statement(
            &state,
            Some("ice"),
            "analytics",
            crate::sql::parser::ast::Statement::AlterIcebergRef(
                crate::sql::parser::ast::AlterIcebergRefStmt {
                    table: crate::sql::parser::ast::ObjectName {
                        parts: vec![
                            "ice".to_string(),
                            "analytics".to_string(),
                            "orders".to_string(),
                        ],
                    },
                    action: crate::sql::parser::ast::AlterIcebergRefAction::CreateBranch {
                        name: "cancelled".to_string(),
                        anchor: crate::sql::parser::ast::SnapshotAnchor::CurrentMain,
                        if_not_exists: false,
                        replace: false,
                        ignored_options: Vec::new(),
                    },
                },
            ),
            &super::test_request_context(Some("ice"), "analytics"),
            &cancel_after_dispatch_context(),
        )
        .expect_err("Iceberg ref work dispatched by a cancelled caller must stop");

        assert_eq!(error, "connector request was cancelled");
    }

    // -----------------------------------------------------------------------
    // Iceberg INSERT-SELECT / INSERT OVERWRITE / DELETE round-trips
    // (Plan Tasks 15-17 — IT-INS-1..4 / IT-OW-1..3 / IT-DEL-1..4 / NEG-*)
    // -----------------------------------------------------------------------

    fn open_test_engine_with_metadata(warehouse: &TempDir) -> StandaloneNovaRocks {
        open_test_engine_with_metadata_and_statistics(
            warehouse,
            Arc::new(crate::engine::statistics::EmptyStatisticsService),
        )
    }

    fn open_test_engine_with_metadata_and_statistics(
        warehouse: &TempDir,
        statistics_service: Arc<dyn StatisticsService>,
    ) -> StandaloneNovaRocks {
        let config_path = warehouse.path().join("novarocks.toml");
        std::fs::create_dir_all(warehouse.path().join("meta")).expect("create metadata dir");
        std::fs::write(
            &config_path,
            r#"[metadata]
provider = "sqlite"
path = "meta/operations.sqlite"
"#,
        )
        .expect("write metadata config");
        StandaloneNovaRocks::open(
            StandaloneOptions {
                config_path: Some(config_path),
            },
            test_open_services_with_statistics(
                Arc::new(crate::engine::system_catalog::EmptySystemCatalog),
                Arc::new(crate::engine::view::EmptyViewService),
                statistics_service,
            ),
        )
        .expect("open engine")
    }

    fn open_iceberg_session_with_table(
        warehouse: &TempDir,
        format_version: &str,
    ) -> (StandaloneNovaRocks, StandaloneSession) {
        open_iceberg_session_with_table_and_statistics(
            warehouse,
            format_version,
            Arc::new(crate::engine::statistics::EmptyStatisticsService),
        )
    }

    fn open_iceberg_session_with_table_and_statistics(
        warehouse: &TempDir,
        format_version: &str,
        statistics_service: Arc<dyn StatisticsService>,
    ) -> (StandaloneNovaRocks, StandaloneSession) {
        let engine = open_test_engine_with_metadata_and_statistics(warehouse, statistics_service);
        let session = engine.session();
        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create catalog");
        session
            .execute_in_database("create database ice.db1", "default")
            .expect("create database");
        let create_table_sql = format!(
            r#"create table ice.db1.t (id int, v string) tblproperties("format-version"="{format_version}")"#
        );
        session
            .execute_in_database(&create_table_sql, "default")
            .expect("create table");
        (engine, session)
    }

    #[test]
    fn iceberg_catalog_lifecycle_registers_and_retires_its_control_binding() {
        let warehouse = tempfile::tempdir().expect("warehouse tempdir");
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default(), test_open_services())
            .expect("open engine");
        let session = engine.session();
        let create_catalog_sql = format!(
            r#"create external catalog Ice_One properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create catalog");
        let state = engine.state_for_test();
        assert!(
            engine
                .iceberg_catalog_exists("ice_one")
                .expect("catalog exists")
        );
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse("ice_one")
            .expect("connector instance ID");
        let planning_lease = state
            .connector_control
            .acquire_current(&instance_id)
            .expect("catalog creation registers its connector control binding");
        assert_eq!(
            planning_lease.binding().descriptor().instance_id,
            instance_id
        );
        drop(planning_lease);

        session
            .execute_in_database("drop catalog Ice_One", "default")
            .expect("drop catalog");
        assert!(
            !engine
                .iceberg_catalog_exists("ice_one")
                .expect("catalog removed")
        );
        let retired_error = match state.connector_control.acquire_current(&instance_id) {
            Ok(_) => panic!("catalog drop must retire its connector control binding"),
            Err(error) => error,
        };
        assert_eq!(
            retired_error.kind(),
            novarocks_spi::connector::ConnectorErrorKind::NotFound
        );
    }

    fn open_row_lineage_iceberg_session_with_table(
        warehouse: &TempDir,
    ) -> (StandaloneNovaRocks, StandaloneSession) {
        open_row_lineage_iceberg_session_with_table_extra_props(warehouse, &[])
    }

    fn open_row_lineage_iceberg_session_with_table_extra_props(
        warehouse: &TempDir,
        extra_props: &[(&str, &str)],
    ) -> (StandaloneNovaRocks, StandaloneSession) {
        use novarocks_connector_iceberg::iceberg::Catalog;

        let engine = open_test_engine_with_metadata(warehouse);
        let session = engine.session();
        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create catalog");
        let catalog = {
            let registry = engine.inner.iceberg_catalogs.read().expect("registry");
            let entry = registry.get("ice").expect("entry");
            crate::connector::iceberg::catalog::registry::build_hadoop_catalog(&entry)
                .expect("build hadoop catalog")
        };
        let namespace =
            novarocks_connector_iceberg::iceberg::NamespaceIdent::new("db1".to_string());
        let schema = novarocks_connector_iceberg::iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(
                    novarocks_connector_iceberg::iceberg::spec::NestedField::required(
                        1,
                        "id",
                        novarocks_connector_iceberg::iceberg::spec::Type::Primitive(
                            novarocks_connector_iceberg::iceberg::spec::PrimitiveType::Int,
                        ),
                    ),
                ),
                Arc::new(
                    novarocks_connector_iceberg::iceberg::spec::NestedField::required(
                        2,
                        "v",
                        novarocks_connector_iceberg::iceberg::spec::Type::Primitive(
                            novarocks_connector_iceberg::iceberg::spec::PrimitiveType::String,
                        ),
                    ),
                ),
            ])
            .build()
            .expect("build schema");
        let mut props: Vec<(String, String)> =
            vec![("write.row-lineage".to_string(), "true".to_string())];
        for (k, v) in extra_props {
            props.push(((*k).to_string(), (*v).to_string()));
        }
        let table_creation = novarocks_connector_iceberg::iceberg::TableCreation::builder()
            .name("t".to_string())
            .schema(schema)
            .format_version(novarocks_connector_iceberg::iceberg::spec::FormatVersion::V3)
            .properties(props)
            .build();
        crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
            catalog
                .create_namespace(&namespace, Default::default())
                .await
                .expect("create namespace");
            catalog
                .create_table(&namespace, table_creation)
                .await
                .expect("create row-lineage table");
        })
        .expect("create row-lineage table runtime");
        (engine, session)
    }

    fn collect_id_v(session: &StandaloneSession, sql: &str) -> Vec<(i32, String)> {
        let result = session.query(sql).expect("query");
        collect_id_v_from_result(result)
    }

    fn collect_id_v_from_result(result: QueryResult) -> Vec<(i32, String)> {
        let mut out = Vec::new();
        for chunk in &result.chunks {
            let ids = chunk
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .expect("id i32");
            let names = chunk
                .batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("v utf8");
            for i in 0..chunk.batch.num_rows() {
                out.push((ids.value(i), names.value(i).to_string()));
            }
        }
        out
    }

    #[test]
    fn time_travel_select_with_current_iceberg_catalog_resolves_synthetic_local_table() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        insert_rows!(&session, ["ice", "db1", "t"]; [1, "a"]);

        let result = session
            .execute_in_context(
                "select id, v from t for version as of 'main'",
                Some("ice"),
                "db1",
                None,
            )
            .expect("time-travel select");
        let StatementResult::Query(result) = result else {
            panic!("expected query result");
        };

        assert_eq!(collect_id_v_from_result(result), vec![(1, "a".to_string())]);
    }

    #[test]
    fn time_travel_explain_with_current_iceberg_catalog_resolves_synthetic_local_table() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        insert_rows!(&session, ["ice", "db1", "t"]; [1, "a"]);

        let result = session
            .execute_in_context(
                "explain select id from t for version as of 'main'",
                Some("ice"),
                "db1",
                None,
            )
            .expect("time-travel explain");

        assert!(matches!(result, StatementResult::Query(_)));
    }

    #[test]
    fn select_resolves_join_on_subquery_iceberg_table_without_local_registration() {
        // Engine gap #4 (join_apply_to_join q7): a table referenced ONLY inside
        // a subquery nested in a JOIN ON predicate must resolve through the
        // catalog-aware provider, exactly like FROM/WHERE subqueries. This
        // SELECT must not materialize ordinary Iceberg tables into the local
        // in-memory catalog.
        let warehouse = TempDir::new().expect("warehouse");
        let engine = open_test_engine_with_metadata(&warehouse);
        let session = engine.session();
        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create catalog");
        session
            .execute_in_database("create database ice.db1", "default")
            .expect("create database");
        for (name, col) in [("t0", "v1"), ("t1", "v5"), ("t2", "v7")] {
            session
                .execute_in_database(
                    &format!(
                        r#"create table ice.db1.{name} ({col} bigint) tblproperties("format-version"="2")"#
                    ),
                    "default",
                )
                .expect("create table");
            insert_rows!(&session, ["ice", "db1", name]; [1], [2], [3]);
        }

        // `t2` is referenced ONLY inside the ON-clause IN-subquery. Sanity:
        // it is not in the in-memory catalog before the SELECT runs.
        assert!(
            !engine.has_local_table("db1", "t2"),
            "iceberg table t2 must not be pre-registered before the SELECT",
        );

        let result = session
            .execute_in_context(
                "select count(*) from db1.t0 \
                 left join db1.t1 on v1 in (select v7 from db1.t2) or v1 < v5",
                Some("ice"),
                "db1",
                None,
            )
            .expect("SELECT with ON-clause subquery table must resolve, not error unknown table");

        // The ON-clause-only table resolved through CatalogMgrProvider without
        // being materialized in the in-memory catalog, and the query produced a
        // count row.
        assert!(
            !engine.has_local_table("db1", "t2"),
            "ON-clause-subquery table t2 must not be locally registered",
        );
        match result {
            StatementResult::Query(query_result) => {
                assert_eq!(
                    query_result.row_count(),
                    1,
                    "count(*) over a LEFT JOIN must return exactly one row",
                );
            }
            StatementResult::Ok => panic!("SELECT must return rows"),
        }
    }

    fn current_iceberg_snapshot_id(
        engine: &StandaloneNovaRocks,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Option<i64> {
        let registry = engine.inner.iceberg_catalogs.read().expect("registry");
        let entry = registry.get(catalog).expect("entry");
        // `load_table` in the registry caches per-entry; force-bypass by
        // invalidating first so we read disk.
        entry.invalidate_table_cache(namespace, table);
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, namespace, table).expect("load");
        loaded
            .table
            .metadata()
            .current_snapshot()
            .map(|s| s.snapshot_id())
    }

    fn iceberg_ref_snapshot_id(
        engine: &StandaloneNovaRocks,
        catalog: &str,
        namespace: &str,
        table: &str,
        ref_name: &str,
    ) -> Option<i64> {
        let registry = engine.inner.iceberg_catalogs.read().expect("registry");
        let entry = registry.get(catalog).expect("entry");
        entry.invalidate_table_cache(namespace, table);
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, namespace, table).expect("load");
        loaded
            .table
            .metadata()
            .refs()
            .get(ref_name)
            .map(|r| r.snapshot_id)
    }

    fn load_iceberg_operation(
        engine: &StandaloneNovaRocks,
        operation_id: i64,
    ) -> crate::meta::repository::iceberg_operation::StoredIcebergOperation {
        let provider = engine
            .inner
            .metadata_provider
            .as_ref()
            .expect("metadata provider");
        let read = provider.begin_read().expect("read operation metadata");
        engine
            .inner
            .iceberg_operation_repo
            .load_operation(read.as_ref(), operation_id)
            .expect("load iceberg operation")
            .expect("iceberg operation present")
    }

    fn assert_iceberg_operation_finalized(
        engine: &StandaloneNovaRocks,
        operation_id: i64,
        expected_kind: crate::meta::repository::iceberg_operation::IcebergOperationKind,
        expected_snapshot_id: Option<i64>,
    ) {
        let operation = load_iceberg_operation(engine, operation_id);
        assert_eq!(operation.operation_kind, expected_kind);
        assert_eq!(
            operation.state,
            crate::meta::repository::iceberg_operation::IcebergOperationState::Finalized
        );
        assert_eq!(
            operation.commit_outcome.as_ref().map(|c| c.snapshot_id),
            expected_snapshot_id
        );
    }

    fn assert_iceberg_operation_finalized_any_snapshot(
        engine: &StandaloneNovaRocks,
        operation_id: i64,
        expected_kind: crate::meta::repository::iceberg_operation::IcebergOperationKind,
    ) -> Option<i64> {
        let operation = load_iceberg_operation(engine, operation_id);
        assert_eq!(operation.operation_kind, expected_kind);
        assert_eq!(
            operation.state,
            crate::meta::repository::iceberg_operation::IcebergOperationState::Finalized
        );
        let snapshot_id = operation.commit_outcome.as_ref().map(|c| c.snapshot_id);
        assert!(
            snapshot_id.is_some(),
            "finalized write operation must record committed snapshot id"
        );
        snapshot_id
    }

    fn assert_iceberg_operation_absent(engine: &StandaloneNovaRocks, operation_id: i64) {
        let provider = engine
            .inner
            .metadata_provider
            .as_ref()
            .expect("metadata provider");
        let read = provider.begin_read().expect("read operation metadata");
        let operation = engine
            .inner
            .iceberg_operation_repo
            .load_operation(read.as_ref(), operation_id)
            .expect("load iceberg operation");
        assert!(
            operation.is_none(),
            "expected no iceberg operation #{operation_id}, found {operation:?}"
        );
    }

    fn current_iceberg_default_spec_fields(
        engine: &StandaloneNovaRocks,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Vec<(
        String,
        novarocks_connector_iceberg::iceberg::spec::Transform,
    )> {
        let registry = engine.inner.iceberg_catalogs.read().expect("registry");
        let entry = registry.get(catalog).expect("entry");
        entry.invalidate_table_cache(namespace, table);
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, namespace, table).expect("load");
        loaded
            .table
            .metadata()
            .default_partition_spec()
            .fields()
            .iter()
            .map(|field| (field.name.clone(), field.transform.clone()))
            .collect()
    }

    #[test]
    fn iceberg_alter_partition_spec_accepts_add_and_drop() {
        let warehouse = TempDir::new().expect("warehouse tempdir");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        session
            .execute_in_database(
                r#"create table ice.db1.t_evolved
                   (id bigint, ts datetime)
                   partition by month(ts)
                   tblproperties("format-version"="2")"#,
                "default",
            )
            .expect("create partitioned table");
        assert_eq!(
            current_iceberg_default_spec_fields(&engine, "ice", "db1", "t_evolved"),
            vec![(
                "ts_month".to_string(),
                novarocks_connector_iceberg::iceberg::spec::Transform::Month
            )]
        );

        session
            .execute_in_database(
                "alter table ice.db1.t_evolved drop partition column month(ts)",
                "default",
            )
            .expect("drop partition column");
        assert_eq!(
            current_iceberg_default_spec_fields(&engine, "ice", "db1", "t_evolved"),
            Vec::<(
                String,
                novarocks_connector_iceberg::iceberg::spec::Transform
            )>::new()
        );

        session
            .execute_in_database(
                "alter table ice.db1.t_evolved add partition column bucket(id, 8)",
                "default",
            )
            .expect("add partition column");
        assert_eq!(
            current_iceberg_default_spec_fields(&engine, "ice", "db1", "t_evolved"),
            vec![(
                "id_bucket_8".to_string(),
                novarocks_connector_iceberg::iceberg::spec::Transform::Bucket(8)
            )]
        );
    }

    fn current_iceberg_row_lineage(
        engine: &StandaloneNovaRocks,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> (u64, Option<(u64, u64)>) {
        let registry = engine.inner.iceberg_catalogs.read().expect("registry");
        let entry = registry.get(catalog).expect("entry");
        entry.invalidate_table_cache(namespace, table);
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, namespace, table).expect("load");
        let metadata = loaded.table.metadata();
        (
            metadata.next_row_id(),
            metadata.current_snapshot().and_then(|s| s.row_range()),
        )
    }

    fn current_live_data_file_first_row_ids(
        engine: &StandaloneNovaRocks,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> Vec<Option<i64>> {
        let registry = engine.inner.iceberg_catalogs.read().expect("registry");
        let entry = registry.get(catalog).expect("entry");
        entry.invalidate_table_cache(namespace, table);
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, namespace, table).expect("load");
        crate::connector::iceberg::catalog::registry::extract_data_files_with_stats(&loaded.table)
            .expect("extract data files")
            .into_iter()
            .map(|file| file.first_row_id)
            .collect()
    }

    fn current_snapshot_has_position_delete_parquet(
        engine: &StandaloneNovaRocks,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> bool {
        let registry = engine.inner.iceberg_catalogs.read().expect("registry");
        let entry = registry.get(catalog).expect("entry");
        entry.invalidate_table_cache(namespace, table);
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, namespace, table).expect("load");
        let metadata = loaded.table.metadata();
        let Some(snapshot) = metadata.current_snapshot() else {
            return false;
        };
        let file_io = loaded.table.file_io().clone();
        crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
            let manifest_list = snapshot
                .load_manifest_list(&file_io, metadata)
                .await
                .expect("load manifest list");
            for manifest_file in manifest_list.entries() {
                if manifest_file.content != novarocks_connector_iceberg::iceberg::spec::ManifestContentType::Deletes {
                    continue;
                }
                let manifest = manifest_file
                    .load_manifest(&file_io)
                    .await
                    .expect("load delete manifest");
                for entry in manifest.entries() {
                    let data_file = entry.data_file();
                    if entry.is_alive()
                        && data_file.content_type()
                            == novarocks_connector_iceberg::iceberg::spec::DataContentType::PositionDeletes
                        && data_file.file_format() == novarocks_connector_iceberg::iceberg::spec::DataFileFormat::Parquet
                    {
                        return true;
                    }
                }
            }
            false
        })
        .expect("inspect delete manifests")
    }

    // ---------------------------------------------------------------------------
    // Helper: read (first_row_id, data_sequence_number) for the current snapshot
    // directly from the iceberg catalog registry.  Used by the row-lineage SELECT
    // integration tests below to build dynamic assertions without querying
    // $snapshots (not yet supported in NovaRocks).
    // ---------------------------------------------------------------------------
    fn current_snapshot_lineage_info(
        engine: &StandaloneNovaRocks,
        catalog: &str,
        namespace: &str,
        table: &str,
    ) -> (u64, i64) {
        let registry = engine.inner.iceberg_catalogs.read().expect("registry");
        let entry = registry.get(catalog).expect("catalog entry");
        entry.invalidate_table_cache(namespace, table);
        let loaded = crate::connector::iceberg::catalog::load_table(&entry, namespace, table)
            .expect("load table");
        let metadata = loaded.table.metadata();
        let snapshot = metadata
            .current_snapshot()
            .expect("table must have a current snapshot");
        let first_row_id = snapshot
            .first_row_id()
            .expect("V3 row-lineage snapshot must carry first_row_id");
        let seq = snapshot.sequence_number();
        (first_row_id, seq)
    }

    // Collect (id, _row_id, _last_updated_sequence_number) tuples from a SELECT
    // that returns exactly those three BIGINT columns.
    fn collect_id_rowid_seq(session: &StandaloneSession, sql: &str) -> Vec<(i64, i64, i64)> {
        let result = session.query(sql).expect("query");
        let mut out = Vec::new();
        for chunk in &result.chunks {
            let ids_col = arrow::compute::cast(chunk.batch.column(0), &DataType::Int64)
                .expect("cast id column to Int64");
            let ids = ids_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column must be Int64");
            let row_ids = chunk
                .batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("_row_id column must be Int64");
            let seqs = chunk
                .batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("_last_updated_sequence_number column must be Int64");
            for i in 0..chunk.batch.num_rows() {
                out.push((ids.value(i), row_ids.value(i), seqs.value(i)));
            }
        }
        out.sort_by_key(|row| row.0);
        out
    }

    // -------------------------------------------------------------------------
    // Task 5: end-to-end SELECT _row_id / _last_updated_sequence_number on a V3
    // row-lineage Iceberg table.
    // -------------------------------------------------------------------------

    // Build a V3 row-lineage table with bigint id and string name columns via
    // the iceberg catalog API (bypassing SQL DDL which defaults to V2).
    fn open_v3_row_lineage_session_bigint(
        warehouse: &TempDir,
    ) -> (StandaloneNovaRocks, StandaloneSession) {
        use novarocks_connector_iceberg::iceberg::Catalog;
        use novarocks_connector_iceberg::iceberg::spec::{NestedField, PrimitiveType, Type};

        let engine = open_test_engine_with_metadata(warehouse);
        let session = engine.session();
        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create iceberg catalog");
        let catalog = {
            let registry = engine.inner.iceberg_catalogs.read().expect("registry");
            let entry = registry.get("ice").expect("entry");
            crate::connector::iceberg::catalog::registry::build_hadoop_catalog(&entry)
                .expect("build hadoop catalog")
        };
        let namespace = novarocks_connector_iceberg::iceberg::NamespaceIdent::new("ns".to_string());
        let schema = novarocks_connector_iceberg::iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .expect("build schema");
        let table_creation = novarocks_connector_iceberg::iceberg::TableCreation::builder()
            .name("t".to_string())
            .schema(schema)
            .format_version(novarocks_connector_iceberg::iceberg::spec::FormatVersion::V3)
            .properties([("write.row-lineage".to_string(), "true".to_string())])
            .build();
        crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
            catalog
                .create_namespace(&namespace, Default::default())
                .await
                .expect("create namespace");
            catalog
                .create_table(&namespace, table_creation)
                .await
                .expect("create V3 row-lineage table");
        })
        .expect("create table runtime");
        (engine, session)
    }

    #[test]
    fn select_row_id_fails_on_v2_iceberg_table() {
        let warehouse = TempDir::new().expect("warehouse tempdir");
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default(), test_open_services())
            .expect("open standalone engine");
        let session = engine.session();
        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create catalog");
        session
            .execute_in_database("create database ice.ns", "default")
            .expect("create namespace");
        session
            .execute_in_database(
                r#"create table ice.ns.t2 (id bigint) tblproperties("format-version"="2")"#,
                "default",
            )
            .expect("create V2 iceberg table");

        let err = session
            .execute_in_database("select _row_id from ice.ns.t2", "default")
            .expect_err("selecting _row_id from a V2 table must fail");
        assert!(
            err.contains("only available on Iceberg V3 row-lineage tables"),
            "expected row-lineage error, got: {err}"
        );

        drop(engine);
    }

    #[test]
    fn select_row_id_fails_on_v3_table_without_row_lineage() {
        let warehouse = TempDir::new().expect("warehouse tempdir");
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default(), test_open_services())
            .expect("open standalone engine");
        let session = engine.session();
        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create catalog");
        session
            .execute_in_database("create database ice.ns", "default")
            .expect("create namespace");
        session
            .execute_in_database(
                r#"create table ice.ns.t3 (id bigint) tblproperties("format-version"="3","write.row-lineage"="false")"#,
                "default",
            )
            .expect("create V3 iceberg table with row-lineage disabled");

        let err = session
            .execute_in_database("select _row_id from ice.ns.t3", "default")
            .expect_err("selecting _row_id from a V3 non-row-lineage table must fail");
        assert!(
            err.contains("only available on Iceberg V3 row-lineage tables"),
            "expected row-lineage error, got: {err}"
        );

        drop(engine);
    }

    #[test]
    fn select_last_updated_sequence_number_fails_on_non_row_lineage_iceberg_table() {
        // Tests that _last_updated_sequence_number fails on a regular V3 iceberg
        // table without write.row-lineage=true (same fail-fast path as unsupported
        // source tables).
        let warehouse = TempDir::new().expect("warehouse tempdir");
        let engine = StandaloneNovaRocks::open(StandaloneOptions::default(), test_open_services())
            .expect("open standalone engine");
        let session = engine.session();
        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create catalog");
        session
            .execute_in_database("create database ice.ns", "default")
            .expect("create namespace");
        session
            .execute_in_database(
                r#"create table ice.ns.t4 (id bigint) tblproperties("format-version"="2")"#,
                "default",
            )
            .expect("create V2 iceberg table (no row-lineage)");

        let err = session
            .execute_in_database(
                "select _last_updated_sequence_number from ice.ns.t4",
                "default",
            )
            .expect_err("must fail on table without row-lineage");
        assert!(
            err.contains("only available on Iceberg V3 row-lineage tables"),
            "expected row-lineage error, got: {err}"
        );

        drop(engine);
    }

    fn test_sql_write_plan_input(
        _bindings: &crate::engine::query_planning::bindings::QueryTableBindingStore,
    ) -> crate::sql::planner::distributed::write::contract::SqlWritePlanInput {
        crate::sql::planner::distributed::write::contract::test_support::simple_sql_write_plan_input(
            crate::sql::planner::distributed::write::contract::ConnectorWriteInputBinding::RootOutputByOrdinal,
        )
    }

    fn single_bucket_partition_metadata_json() -> String {
        let schema = novarocks_connector_iceberg::iceberg::spec::Schema::builder()
            .with_fields(vec![Arc::new(
                novarocks_connector_iceberg::iceberg::spec::NestedField::required(
                    1,
                    "id",
                    novarocks_connector_iceberg::iceberg::spec::Type::Primitive(
                        novarocks_connector_iceberg::iceberg::spec::PrimitiveType::Int,
                    ),
                ),
            )])
            .build()
            .expect("schema");
        let partition_spec =
            novarocks_connector_iceberg::iceberg::spec::PartitionSpec::builder(schema.clone())
                .add_partition_field(
                    "id",
                    "id_bucket",
                    novarocks_connector_iceberg::iceberg::spec::Transform::Bucket(16),
                )
                .expect("partition field")
                .build()
                .expect("partition spec");
        let metadata = novarocks_connector_iceberg::iceberg::spec::TableMetadataBuilder::new(
            schema,
            partition_spec,
            novarocks_connector_iceberg::iceberg::spec::SortOrder::unsorted_order(),
            "file:///warehouse/target_orders".to_string(),
            novarocks_connector_iceberg::iceberg::spec::FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata");
        serde_json::to_string(&metadata.metadata).expect("serialize metadata")
    }

    #[test]
    fn iceberg_write_root_shuffle_by_output_name_is_a_typed_sql_requirement() {
        assert_eq!(
            super::iceberg_write_shuffle_by_output_name("_file"),
            crate::sql::compiler::RootDistributionRequirement::ShuffleOutputName(
                "_file".to_string()
            )
        );
    }

    #[test]
    fn execute_query_as_iceberg_write_requires_admitted_topology() {
        let query = parse_query_for_engine_test("SELECT 1 AS payload, 'file-a' AS _file");
        let mut state = StandaloneState::default();
        state.exchange_port = 1;
        let state = Arc::new(state);
        let table_bindings = Arc::new(
            crate::engine::query_planning::bindings::QueryTableBindingStore::try_new()
                .expect("test binding store"),
        );
        let sink = test_sql_write_plan_input(table_bindings.as_ref());

        let result = super::execute_query_as_iceberg_write(
            &state,
            None,
            "default",
            &query,
            sink,
            table_bindings,
            None,
            crate::sql::compiler::RootDistributionRequirement::ShuffleOutputName(
                "missing".to_string(),
            ),
            None,
        );

        let err = match result {
            Ok(_) => panic!("write without admission topology should not execute"),
            Err(err) => err,
        };
        assert!(
            err.contains("non-empty admitted backend topology"),
            "expected admission error, got: {err}"
        );
    }

    #[test]
    fn planned_change_stream_write_uses_physical_plan_entrypoint() {
        use crate::sql::column_id::ColumnId;
        use crate::sql::optimizer::operator::{Operator, ValuesOp};
        use crate::sql::optimizer::optimized_tree::{
            OptimizedOperatorNode, PlanExecutionProps, attach_scalar_arena,
        };
        use crate::sql::optimizer::scalar::ScalarArena;
        use crate::sql::optimizer::statistics::Statistics;
        use crate::sql::planner::distributed::write::change_stream::{
            ChangeStreamWriteDagSpec, ChangeStreamWriteRouteSpec,
        };
        use novarocks_spi::connector::{
            ConnectorMutationRouteInput, ConnectorRowMutationEffect, ConnectorWriteCohortId,
            ConnectorWriteFieldToken, ConnectorWriteRouteId,
        };

        let _test_guard = super::acquire_standalone_test_guard();
        let observer = super::install_change_stream_write_test_observer(true);
        let output_columns = vec![
            crate::sql::analysis::OutputColumn {
                column_id: ColumnId::new_for_test(1),
                name: crate::sql::common::change_stream::ROW_MUTATION_EFFECT_COLUMN.to_string(),
                data_type: DataType::Int8,
                nullable: false,
                is_internal: true,
            },
            crate::sql::analysis::OutputColumn {
                column_id: ColumnId::new_for_test(2),
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: true,
            },
            crate::sql::analysis::OutputColumn {
                column_id: ColumnId::new_for_test(3),
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: false,
            },
        ];
        let mut optimized_tree = OptimizedOperatorNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: Vec::new(),
                columns: output_columns.clone(),
            }),
            children: Vec::new(),
            stats: Statistics {
                output_row_count: 0.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            explain_stats: crate::sql::optimizer::optimized_tree::OptimizerExplainStats::default(),
            output_columns,
            execution_props: PlanExecutionProps::default(),
        };
        attach_scalar_arena(&mut optimized_tree, Arc::new(ScalarArena::new()));
        let state = Arc::new(StandaloneState::default());
        let mut dag = ChangeStreamWriteDagSpec::for_test(
            0,
            vec![ChangeStreamWriteRouteSpec {
                route_id: ConnectorWriteRouteId::from_bytes([7; 32]),
                cohort_id: ConnectorWriteCohortId::from_bytes([8; 32]),
                accepted_effects: vec![ConnectorRowMutationEffect::Replace],
                input_ordinals: vec![ConnectorMutationRouteInput::new(
                    ConnectorWriteFieldToken::from_bytes([9; 32]),
                    1,
                )],
                output_partition_ordinals: Vec::new(),
                sink: crate::sql::planner::distributed::write::contract::test_support::simple_sql_write_plan_input(
                    crate::sql::planner::distributed::write::contract::ConnectorWriteInputBinding::RootOutputByOrdinal,
                ),
            }],
        );

        let result = super::execute_physical_plan_as_iceberg_change_stream_write(
            &state,
            None,
            "default",
            &optimized_tree,
            &mut dag,
            None,
        )
        .expect("planned physical change-stream write build");

        assert!(result.write_commit.is_none());
        let observations = observer.take_observations();
        assert_eq!(observations.len(), 1);
        let observation = &observations[0];
        assert_eq!(
            observation.entrypoint,
            super::ChangeStreamWriteEntrypoint::PhysicalPlan
        );
        assert_eq!(
            observation.effects,
            vec![ConnectorRowMutationEffect::Replace]
        );
        assert_eq!(observation.writer_fragment_ids.len(), 1);
        assert!(
            observation.writer_fragment_ids[0].is_some(),
            "fragment builder must assign the writer fragment before execution"
        );
    }

    #[test]
    fn parse_explain_refresh_materialized_view_supports_verbose_and_costs() {
        let verbose = super::parse_explain_refresh_materialized_view(
            "EXPLAIN VERBOSE REFRESH MATERIALIZED VIEW mv1",
        )
        .expect("recognized")
        .expect("parsed");
        assert_eq!(verbose.0.name.parts, vec!["mv1"]);
        assert_eq!(verbose.1, crate::sql::explain::ExplainLevel::Verbose);
        assert!(!verbose.2);

        let costs = super::parse_explain_refresh_materialized_view(
            "EXPLAIN COSTS REFRESH MATERIALIZED VIEW db.mv1",
        )
        .expect("recognized")
        .expect("parsed");
        assert_eq!(costs.0.name.parts, vec!["db", "mv1"]);
        assert_eq!(costs.1, crate::sql::explain::ExplainLevel::Costs);
        assert!(!costs.2);
    }

    #[test]
    fn parse_explain_refresh_materialized_view_marks_analyze() {
        let parsed = super::parse_explain_refresh_materialized_view(
            "EXPLAIN ANALYZE REFRESH MATERIALIZED VIEW mv1",
        )
        .expect("recognized")
        .expect("parsed");
        assert_eq!(parsed.1, crate::sql::explain::ExplainLevel::Analyze);
        assert!(parsed.2);
    }

    #[test]
    fn split_explain_logical_sql_rewrites_to_plain_explain() {
        let (rewritten, level) =
            super::split_explain_logical_sql(" EXPLAIN LOGICAL SELECT * FROM t")
                .expect("recognized");
        assert_eq!(rewritten, "EXPLAIN SELECT * FROM t");
        assert_eq!(level, crate::sql::explain::ExplainLevel::Normal);

        let (rewritten, level) =
            super::split_explain_logical_sql("explain logical verbose select k from t")
                .expect("recognized");
        assert_eq!(rewritten, "EXPLAIN select k from t");
        assert_eq!(level, crate::sql::explain::ExplainLevel::Verbose);

        let (rewritten, level) =
            super::split_explain_logical_sql("EXPLAIN\nLOGICAL\nSELECT k FROM t")
                .expect("recognized");
        assert_eq!(rewritten, "EXPLAIN SELECT k FROM t");
        assert_eq!(level, crate::sql::explain::ExplainLevel::Normal);
    }

    // -------------------------------------------------------------------------
    // Scalar subquery decorrelation — end-to-end correctness tests (Task 5).
    //
    // These tests run subquery shapes against the same Iceberg-backed
    // in-memory tables and assert the Apply framework's concrete semantics.
    //
    // Setup: t1(k BIGINT, v BIGINT) and t2(k BIGINT, v BIGINT) in an Iceberg
    // in-memory catalog, populated with controlled data per test.
    //
    // The migration-time subquery routing switch has been removed; analyzer
    // routing must now be explicit and unsupported shapes must fail clearly.
    // -------------------------------------------------------------------------

    /// Open an Iceberg-backed test engine and return (engine, session, warehouse_dir).
    /// Tables are NOT yet created; the caller creates and populates them.
    fn open_scalar_subquery_test_engine(
        warehouse: &TempDir,
    ) -> (StandaloneNovaRocks, StandaloneSession) {
        let engine = open_test_engine_with_metadata(warehouse);
        let session = engine.session();
        let create_catalog_sql = format!(
            r#"create external catalog ice properties("type"="iceberg","iceberg.catalog.type"="hadoop","iceberg.catalog.warehouse"="{}")"#,
            warehouse.path().display()
        );
        session
            .execute_in_database(&create_catalog_sql, "default")
            .expect("create iceberg catalog");
        session
            .execute_in_database("create database ice.db1", "default")
            .expect("create iceberg database");
        (engine, session)
    }

    /// Run a SELECT query and return the first column's values
    /// as a `Vec<Option<i64>>` (None = SQL NULL). Expects every row to have
    /// exactly one column of type BIGINT/INT.
    fn run_scalar_query_i64(
        session: &StandaloneSession,
        sql: &str,
    ) -> Result<Vec<Option<i64>>, String> {
        let result = session.execute_in_context(sql, Some("ice"), "db1", None)?;
        let qr = match result {
            StatementResult::Query(qr) => qr,
            StatementResult::Ok => {
                return Err("query returned no rows (StatementResult::Ok)".to_string());
            }
        };
        let mut values = Vec::new();
        for chunk in &qr.chunks {
            let col = chunk.batch.column(0);
            // Try Int64 first (BIGINT), then Int32 (INT).
            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                for i in 0..arr.len() {
                    values.push(if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    });
                }
            } else if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                for i in 0..arr.len() {
                    values.push(if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i) as i64)
                    });
                }
            } else {
                return Err(format!(
                    "expected Int64 or Int32 column, got {:?}",
                    col.data_type()
                ));
            }
        }
        Ok(values)
    }

    /// Run a SELECT query and return all columns' values as a
    /// `Vec<Vec<Option<i64>>>` (outer = rows, inner = columns). Expects every
    /// column to be BIGINT/INT.
    fn run_scalar_query_multi_col(
        session: &StandaloneSession,
        sql: &str,
    ) -> Result<Vec<Vec<Option<i64>>>, String> {
        let result = session.execute_in_context(sql, Some("ice"), "db1", None)?;
        let qr = match result {
            StatementResult::Query(qr) => qr,
            StatementResult::Ok => {
                return Err("query returned no rows (StatementResult::Ok)".to_string());
            }
        };
        let num_cols = qr.columns.len();
        // Build row-major result: collect per-column arrays, then transpose.
        let mut col_values: Vec<Vec<Option<i64>>> = vec![Vec::new(); num_cols];
        for chunk in &qr.chunks {
            for col_idx in 0..num_cols {
                let col = chunk.batch.column(col_idx);
                if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    for i in 0..arr.len() {
                        col_values[col_idx].push(if arr.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i))
                        });
                    }
                } else if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                    for i in 0..arr.len() {
                        col_values[col_idx].push(if arr.is_null(i) {
                            None
                        } else {
                            Some(arr.value(i) as i64)
                        });
                    }
                } else {
                    return Err(format!(
                        "col {col_idx}: expected Int64 or Int32, got {:?}",
                        col.data_type()
                    ));
                }
            }
        }
        // Transpose to row-major.
        let num_rows = col_values.first().map(|v| v.len()).unwrap_or(0);
        let rows = (0..num_rows)
            .map(|r| (0..num_cols).map(|c| col_values[c][r]).collect())
            .collect();
        Ok(rows)
    }

    /// Run a SELECT query and expect an error containing `needle`.
    fn expect_subquery_error(session: &StandaloneSession, sql: &str, needle: &str) {
        let result = session.execute_in_context(sql, Some("ice"), "db1", None);
        let err = result.expect_err(&format!(
            "expected subquery error containing '{needle}', but query succeeded"
        ));
        assert!(
            err.contains(needle),
            "expected error containing '{needle}'; got: {err}"
        );
    }

    fn create_kv_tables(
        session: &StandaloneSession,
        t1_rows: &[(Option<i64>, Option<i64>)],
        t2_rows: &[(Option<i64>, Option<i64>)],
    ) {
        session
            .execute_in_database("create table ice.db1.t1 (k bigint, v bigint)", "default")
            .expect("create t1");
        session
            .execute_in_database("create table ice.db1.t2 (k bigint, v bigint)", "default")
            .expect("create t2");
        insert_kv_fixture_rows(session, "t1", t1_rows);
        insert_kv_fixture_rows(session, "t2", t2_rows);
    }

    fn insert_kv_fixture_rows(
        session: &StandaloneSession,
        table: &str,
        rows: &[(Option<i64>, Option<i64>)],
    ) {
        if rows.is_empty() {
            return;
        }
        let rows = rows
            .iter()
            .map(|(key, value)| vec![(*key).into_test_literal(), (*value).into_test_literal()])
            .collect::<Vec<_>>();
        insert_iceberg_fixture_rows(session, &["ice", "db1", table], &rows);
    }

    fn assert_subquery_result_i64(
        session: &StandaloneSession,
        sql: &str,
        expected: Vec<Option<i64>>,
    ) {
        let result = run_scalar_query_i64(session, sql).expect("subquery query");

        assert_eq!(result, expected, "unexpected SQL result for query: {sql}");
    }

    // ---- Test 1: correlated aggregate (q17-shape) ----------------------------

    /// Correlated aggregate scalar: `WHERE v = (SELECT min(t2.v) FROM t2 WHERE t2.k = t1.k)`.
    #[test]
    fn scalar_subquery_correlated_agg_returns_expected_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);

        session
            .execute_in_database("create table ice.db1.t1 (k bigint, v bigint)", "default")
            .expect("create t1");
        session
            .execute_in_database("create table ice.db1.t2 (k bigint, v bigint)", "default")
            .expect("create t2");
        // t1: (1,10),(2,20),(3,30)
        // t2: (1,10),(1,5),(2,20),(2,15)  -> min for k=1 is 5, k=2 is 15, k=3 has no match
        insert_rows!(&session, ["ice", "db1", "t1"]; [1_i64, 10_i64], [2_i64, 20_i64], [3_i64, 30_i64]);
        insert_rows!(&session, ["ice", "db1", "t2"];
            [1_i64, 10_i64], [1_i64, 5_i64], [2_i64, 20_i64], [2_i64, 15_i64]);

        // The subquery: SELECT t1.k FROM t1 WHERE t1.v = (SELECT min(t2.v) FROM t2 WHERE t2.k = t1.k)
        // k=1: min(t2.v) for k=1 = 5; t1.v=10 != 5 -> not selected
        // k=2: min(t2.v) for k=2 = 15; t1.v=20 != 15 -> not selected
        // k=3: no t2 rows -> NULL; t1.v=30 != NULL -> not selected
        // Result: no rows (empty)
        //
        // Alternatively use a query where some rows DO match:
        // WHERE t1.v = (SELECT min(t2.v) FROM t2 WHERE t2.k = t1.k)
        // Let's insert a t1 row where v=5 (k=1) so it matches min(t2.v)=5.
        insert_rows!(&session, ["ice", "db1", "t1"]; [1_i64, 5_i64]);

        let sql = "SELECT t1.k FROM t1 WHERE t1.v = (SELECT min(t2.v) FROM t2 WHERE t2.k = t1.k) ORDER BY 1";

        let result = run_scalar_query_i64(&session, sql).expect("corr-agg query");
        // k=1, v=5 matches min(t2.v for k=1)=5 — exactly one row
        assert_eq!(result, vec![Some(1)]);
    }

    // ---- Test 2: uncorrelated scalar ----------------------------------------

    #[test]
    fn scalar_subquery_uncorrelated_returns_expected_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);

        session
            .execute_in_database("create table ice.db1.t1 (k bigint, v bigint)", "default")
            .expect("create t1");
        session
            .execute_in_database("create table ice.db1.t2 (k bigint, v bigint)", "default")
            .expect("create t2");
        insert_rows!(&session, ["ice", "db1", "t1"]; [1_i64, 10_i64], [2_i64, 20_i64], [3_i64, 30_i64]);
        insert_rows!(&session, ["ice", "db1", "t2"];
            [1_i64, 100_i64], [2_i64, 200_i64], [3_i64, 300_i64]);

        // Uncorrelated scalar: v > (SELECT min(v) FROM t2). min(t2.v)=100 and all
        // t1.v are < 100, so the result is empty.
        let sql = "SELECT t1.k FROM t1 WHERE t1.v > (SELECT min(v) FROM t2) ORDER BY 1";

        let result = run_scalar_query_i64(&session, sql).expect("uncorrelated query");
        // t1.v > 100: v=20 no, v=30 no — wait, t1.v values are 10,20,30; min(t2.v)=100
        // None qualify. Let's verify.
        assert_eq!(result, vec![]);
    }

    // ---- Test 3: empty group -> NULL ----------------------------------------

    /// When some outer rows have no matching inner group, the correlated
    /// aggregate returns NULL (LEFT OUTER JOIN null-extension).
    #[test]
    fn scalar_subquery_empty_group_yields_null() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);

        session
            .execute_in_database("create table ice.db1.t1 (k bigint, v bigint)", "default")
            .expect("create t1");
        session
            .execute_in_database("create table ice.db1.t2 (k bigint, v bigint)", "default")
            .expect("create t2");
        // t1: k=1, k=2, k=3. t2 has only k=1 and k=2.
        // k=3 has no match → scalar is NULL.
        insert_rows!(&session, ["ice", "db1", "t1"]; [1_i64, 0_i64], [2_i64, 0_i64], [3_i64, 0_i64]);
        insert_rows!(&session, ["ice", "db1", "t2"]; [1_i64, 10_i64], [2_i64, 20_i64]);

        // Project the scalar result (may be NULL) for each t1 row.
        let sql = "SELECT (SELECT min(t2.v) FROM t2 WHERE t2.k = t1.k) FROM t1 ORDER BY t1.k";

        let result = run_scalar_query_i64(&session, sql).expect("empty-group query");
        // k=1 → Some(10), k=2 → Some(20), k=3 → None (NULL)
        assert_eq!(result, vec![Some(10), Some(20), None]);
    }

    // ---- Test 4: count -> 0, not NULL ---------------------------------------

    /// Correlated count(*) scalar: must return 0 (not NULL) for outer
    /// rows with no matching inner group, thanks to the `ifnull(count,0)`
    /// normalization in ScalarApplyToJoin.
    #[test]
    fn scalar_subquery_count_zero_normalizes_correctly() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);

        session
            .execute_in_database("create table ice.db1.t1 (k bigint, v bigint)", "default")
            .expect("create t1");
        session
            .execute_in_database("create table ice.db1.t2 (k bigint, v bigint)", "default")
            .expect("create t2");
        insert_rows!(&session, ["ice", "db1", "t1"]; [1_i64, 0_i64], [2_i64, 0_i64], [3_i64, 0_i64]);
        insert_rows!(&session, ["ice", "db1", "t2"]; [1_i64, 10_i64], [1_i64, 20_i64]);

        // count(*) for k=1 → 2, k=2 → 0, k=3 → 0 (not NULL)
        let sql = "SELECT (SELECT count(*) FROM t2 WHERE t2.k = t1.k) FROM t1 ORDER BY t1.k";

        let result = run_scalar_query_i64(&session, sql).expect("count-zero query");

        assert_eq!(
            result,
            vec![Some(2), Some(0), Some(0)],
            "count(*) must return 0 (not NULL) for unmatched outer rows (ifnull normalization)"
        );
    }

    // ---- Test 5: NULL correlation key -> NULL scalar ------------------------

    #[test]
    fn scalar_subquery_null_correlation_key_yields_null() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);

        session
            .execute_in_database("create table ice.db1.t1 (k bigint, v bigint)", "default")
            .expect("create t1");
        session
            .execute_in_database("create table ice.db1.t2 (k bigint, v bigint)", "default")
            .expect("create t2");
        // t1 has a NULL k; the correlated scalar must also be NULL.
        insert_rows!(&session, ["ice", "db1", "t1"];
            [Some(1_i64), 0_i64], [None::<i64>, 0_i64]);
        insert_rows!(&session, ["ice", "db1", "t2"]; [1_i64, 10_i64], [1_i64, 5_i64]);

        let sql =
            "SELECT (SELECT min(t2.v) FROM t2 WHERE t2.k = t1.k) FROM t1 ORDER BY t1.k NULLS LAST";

        let result = run_scalar_query_i64(&session, sql).expect("null-key query");
        // k=1 → Some(5), k=NULL → None (NULL: no match because NULL != NULL in the join)
        assert_eq!(result, vec![Some(5), None]);
    }

    // ---- Test 6: correlated non-agg single-row ------------------------------

    /// Correlated NON-aggregate scalar where the inner key is unique (≤1 row
    /// per outer key). The with-check path (count(1)/any_value/assert_true)
    /// must return the correct value without raising the row-check error.
    #[test]
    fn scalar_subquery_correlated_nonagg_single_row_ok() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);

        session
            .execute_in_database("create table ice.db1.t1 (k bigint, v bigint)", "default")
            .expect("create t1");
        session
            .execute_in_database("create table ice.db1.t2 (k bigint, v bigint)", "default")
            .expect("create t2");
        // t2.k is effectively unique: each k appears exactly once.
        insert_rows!(&session, ["ice", "db1", "t1"]; [1_i64, 0_i64], [2_i64, 0_i64], [3_i64, 0_i64]);
        insert_rows!(&session, ["ice", "db1", "t2"]; [1_i64, 100_i64], [2_i64, 200_i64]);

        // Correlated non-aggregate: (SELECT t2.v FROM t2 WHERE t2.k = t1.k)
        // k=1 → 100, k=2 → 200, k=3 → NULL (no match)
        let sql = "SELECT (SELECT t2.v FROM t2 WHERE t2.k = t1.k) FROM t1 ORDER BY t1.k";

        let result =
            run_scalar_query_i64(&session, sql).expect("non-agg single-row query must succeed");

        assert_eq!(
            result,
            vec![Some(100), Some(200), None],
            "non-agg single-row must return correct values"
        );
    }

    // ---- Test 7: correlated non-agg MULTI-ROW -> must ERROR -----------------
    //
    // This is the most important test: the assert_true(cnt IS NULL OR cnt <= 1,
    // 'correlate scalar subquery result must 1 row') check must fire at runtime.
    #[test]
    fn scalar_subquery_correlated_nonagg_multirow_errors_with_apply_guard() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);

        session
            .execute_in_database("create table ice.db1.t1 (k bigint, v bigint)", "default")
            .expect("create t1");
        session
            .execute_in_database("create table ice.db1.t2 (k bigint, v bigint)", "default")
            .expect("create t2");
        // t2 has TWO rows with k=1, so the correlated scalar for t1.k=1 returns >1 row.
        insert_rows!(&session, ["ice", "db1", "t1"]; [1_i64, 0_i64]);
        insert_rows!(&session, ["ice", "db1", "t2"]; [1_i64, 100_i64], [1_i64, 200_i64]);

        let sql = "SELECT (SELECT t2.v FROM t2 WHERE t2.k = t1.k) FROM t1";
        expect_subquery_error(&session, sql, "correlate scalar subquery result must 1 row");
    }

    // ---- Test 8: two correlated scalar subqueries in one query ---------------

    /// Two correlated scalar subqueries over different tables in the same SELECT
    /// list. M1a stacks them as left-deep Apply nodes; M1b decorrelates each one
    /// to a LEFT OUTER JOIN. This test verifies that both decorrelations succeed
    /// and that results include NULL extension for
    /// outer rows that only match one of the two subqueries.
    ///
    /// Schema:
    ///   t1(k BIGINT, v BIGINT) — outer table
    ///   t2(k BIGINT, v BIGINT) — has matches for k=1 and k=2, but NOT k=3
    ///   t3(k BIGINT, v BIGINT) — has matches for k=1 and k=3, but NOT k=2
    ///
    /// Query:
    ///   SELECT t1.k,
    ///          (SELECT min(t2.v) FROM t2 WHERE t2.k = t1.k),
    ///          (SELECT max(t3.v) FROM t3 WHERE t3.k = t1.k)
    ///   FROM t1 ORDER BY t1.k
    ///
    /// Expected results (k, min_t2, max_t3):
    ///   k=1 → (1, 5,  90)   — both subqueries match
    ///   k=2 → (2, 20, NULL) — only t2 matches; t3 NULL-extends
    ///   k=3 → (3, NULL, 30) — only t3 matches; t2 NULL-extends
    #[test]
    fn scalar_subquery_multiple_in_one_query_returns_expected_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);

        session
            .execute_in_database("create table ice.db1.t1 (k bigint, v bigint)", "default")
            .expect("create t1");
        session
            .execute_in_database("create table ice.db1.t2 (k bigint, v bigint)", "default")
            .expect("create t2");
        session
            .execute_in_database("create table ice.db1.t3 (k bigint, v bigint)", "default")
            .expect("create t3");

        // t1: three outer rows with distinct keys
        insert_rows!(&session, ["ice", "db1", "t1"];
            [1_i64, 0_i64], [2_i64, 0_i64], [3_i64, 0_i64]);
        // t2: k=1 has two rows (min=5), k=2 has one row (min=20), k=3 absent
        insert_rows!(&session, ["ice", "db1", "t2"];
            [1_i64, 5_i64], [1_i64, 10_i64], [2_i64, 20_i64]);
        // t3: k=1 has one row (max=90), k=2 absent, k=3 has one row (max=30)
        insert_rows!(&session, ["ice", "db1", "t3"]; [1_i64, 90_i64], [3_i64, 30_i64]);

        let sql = "SELECT t1.k, \
                   (SELECT min(t2.v) FROM t2 WHERE t2.k = t1.k), \
                   (SELECT max(t3.v) FROM t3 WHERE t3.k = t1.k) \
                   FROM t1 ORDER BY t1.k";

        let result = run_scalar_query_multi_col(&session, sql).expect("multi-scalar query");

        // Verify the concrete expected values:
        //   k=1: min(t2.v for k=1)=5,  max(t3.v for k=1)=90
        //   k=2: min(t2.v for k=2)=20, max(t3.v for k=2)=NULL (no t3 rows)
        //   k=3: min(t2.v for k=3)=NULL (no t2 rows), max(t3.v for k=3)=30
        assert_eq!(
            result,
            vec![
                vec![Some(1), Some(5), Some(90)],
                vec![Some(2), Some(20), None],
                vec![Some(3), None, Some(30)],
            ],
            "unexpected result values for multiple scalar subqueries"
        );
    }

    // -------------------------------------------------------------------------
    // EXISTS / IN Apply-to-Join — end-to-end parity tests (Task 7).
    // -------------------------------------------------------------------------

    #[test]
    fn not_exists_correlated_returns_expected_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(
            &session,
            kv_rows!((1, 10), (2, 20), (3, 30), (NULL, 40)),
            kv_rows!((1, 100), (3, 300), (NULL, 999)),
        );

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.k = t1.k) ORDER BY t1.k NULLS LAST",
            vec![Some(2), None],
        );
    }

    #[test]
    fn exists_uncorrelated_returns_expected_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(
            &session,
            kv_rows!((1, 10), (2, 20), (3, 30)),
            kv_rows!((1, 101), (2, 200)),
        );

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.v > 100) ORDER BY 1",
            vec![Some(1), Some(2), Some(3)],
        );
        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.v > 1000) ORDER BY 1",
            vec![],
        );
    }

    #[test]
    fn in_inside_or_with_build_null_preserves_unknown() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(
            &session,
            kv_rows!((1, 10), (2, 20), (3, NULL)),
            kv_rows!((9, 10), (9, NULL)),
        );

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 \
             WHERE (t1.v IN (SELECT t2.v FROM t2)) OR false \
             ORDER BY 1",
            vec![Some(1)],
        );
    }

    #[test]
    fn in_projection_with_build_null_preserves_unknown() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(
            &session,
            kv_rows!((1, 10), (2, 20), (3, NULL)),
            kv_rows!((9, 10), (9, NULL)),
        );

        assert_subquery_result_i64(
            &session,
            "SELECT CASE \
                    WHEN t1.v IN (SELECT t2.v FROM t2) THEN 1 \
                    WHEN (t1.v IN (SELECT t2.v FROM t2)) IS NULL THEN NULL \
                    ELSE 0 \
                END \
             FROM t1 ORDER BY t1.k",
            vec![Some(1), None, None],
        );
    }

    #[test]
    fn not_in_uncorrelated_no_null() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(
            &session,
            kv_rows!((1, 10), (2, 20), (3, 30)),
            kv_rows!((9, 20), (9, 40)),
        );

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE t1.v NOT IN (SELECT t2.v FROM t2) ORDER BY 1",
            vec![Some(1), Some(3)],
        );
    }

    #[test]
    fn not_in_uncorrelated_build_null() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(
            &session,
            kv_rows!((1, 10), (2, 20), (3, 30)),
            kv_rows!((9, 20), (9, NULL)),
        );

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE t1.v NOT IN (SELECT t2.v FROM t2) ORDER BY 1",
            vec![],
        );
    }

    #[test]
    fn not_in_inside_or_with_build_null_is_unknown() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(&session, kv_rows!((1, 10), (2, 20)), kv_rows!((9, NULL)));

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 \
             WHERE (t1.v NOT IN (SELECT t2.v FROM t2)) OR false \
             ORDER BY 1",
            vec![],
        );
    }

    #[test]
    fn not_in_inside_or_with_probe_null_and_empty_build_is_true() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(&session, kv_rows!((1, NULL), (2, 20)), &[]);

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 \
             WHERE (t1.v NOT IN (SELECT t2.v FROM t2)) OR false \
             ORDER BY 1",
            vec![Some(1), Some(2)],
        );
    }

    #[test]
    fn not_in_join_on_with_build_null_is_unknown() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(&session, kv_rows!((1, 10), (2, 20)), kv_rows!((9, NULL)));
        session
            .execute_in_database("create table ice.db1.t3 (k bigint, v bigint)", "default")
            .expect("create t3");
        insert_rows!(&session, ["ice", "db1", "t3"]; [100_i64, 0_i64]);

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 JOIN t3 \
             ON t1.v NOT IN (SELECT t2.v FROM t2) \
             ORDER BY 1",
            vec![],
        );
    }

    #[test]
    fn not_in_join_on_with_probe_null_is_unknown() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(&session, kv_rows!((1, NULL), (2, 20)), kv_rows!((9, 10)));
        session
            .execute_in_database("create table ice.db1.t3 (k bigint, v bigint)", "default")
            .expect("create t3");
        insert_rows!(&session, ["ice", "db1", "t3"]; [100_i64, 0_i64]);

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 JOIN t3 \
             ON t1.v NOT IN (SELECT t2.v FROM t2) \
             ORDER BY 1",
            vec![Some(2)],
        );
    }

    #[test]
    fn not_in_uncorrelated_probe_null() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(
            &session,
            kv_rows!((1, 10), (2, NULL), (3, 30)),
            kv_rows!((9, 20), (9, 40)),
        );

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE t1.v NOT IN (SELECT t2.v FROM t2) ORDER BY 1",
            vec![Some(1), Some(3)],
        );
    }

    #[test]
    fn not_in_correlated_conjunct() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(
            &session,
            kv_rows!((1, 10), (2, 20), (3, 30), (4, 40)),
            kv_rows!((1, 20), (1, 30), (2, 20), (2, NULL), (3, NULL), (3, 40)),
        );

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE t1.v NOT IN (SELECT t2.v FROM t2 WHERE t2.k = t1.k) ORDER BY 1",
            vec![Some(1), Some(4)],
        );
    }

    #[test]
    fn typed_statistics_statements_use_the_injected_application_port() {
        let port = Arc::new(RecordingStatisticsApplicationPort::default());
        let engine = StandaloneNovaRocks::open(
            StandaloneOptions::default(),
            test_open_services().with_statistics_application(port.clone()),
        )
        .expect("open engine");
        let session = engine.session();

        session
            .execute("ANALYZE TABLE ice.analytics.orders (order_id)")
            .expect("submit typed analyze");
        let show_stats = session
            .query("SHOW TABLE STATS ice.analytics.orders")
            .expect("show typed table stats");
        assert_eq!(show_stats.columns[0].name, "metric");
        assert_eq!(show_stats.columns[1].name, "value");
        assert_eq!(string_cell(&show_stats, 0, 0), "row_count");
        assert_eq!(string_cell(&show_stats, 0, 1), "42");

        assert_eq!(
            port.commands(),
            vec![
                StatisticsApplicationCommand::AnalyzeTable {
                    target: StatisticsTableTarget {
                        catalog: "ice".into(),
                        namespace: "analytics".into(),
                        table: "orders".into(),
                    },
                    columns: vec!["order_id".into()],
                },
                StatisticsApplicationCommand::ShowTableStats {
                    target: StatisticsTableTarget {
                        catalog: "ice".into(),
                        namespace: "analytics".into(),
                        table: "orders".into(),
                    },
                },
            ]
        );
    }
}
