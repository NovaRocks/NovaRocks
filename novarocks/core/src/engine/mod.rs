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

#[cfg(feature = "compat")]
use std::collections::{HashMap, HashSet};
#[cfg(test)]
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::OnceLock;
use std::sync::{Arc, RwLock, Weak};
#[cfg(test)]
use std::time::{Duration, Instant};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use tokio::runtime::Handle;

use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::mv::refresh::execution_context::MvRefreshPruningLimits;
use crate::novarocks_config;
use crate::runtime::global_async_runtime::data_block_on;
use crate::runtime::query_options::QueryOptions;
use crate::runtime::query_result::{
    QueryResult, QueryResultColumn, build_string_query_result, record_batch_to_chunk,
};

use crate::catalog_attachment::{CatalogAttachmentProperties, CatalogAttachmentRepository};
use crate::connector::{
    IcebergCatalogRegistry, StarRocksTableCatalog, StarRocksTableConfig, iceberg_namespace_exists,
};
#[cfg(feature = "compat")]
use crate::connector::{register_starrocks_tables_in_catalog, runtime_registered};
use crate::meta::repository::iceberg_operation::IcebergOperationRepository;
use crate::meta::repository::job::JobMetaRepository;
use crate::meta::repository::starrocks_table::StarRocksTableMetaRepository;
use crate::meta::repository::starrocks_txn::StarRocksTxnRepository;
use crate::mv::application::{MvApplicationService, UnavailableMvApplicationService};
use crate::mv::repository::{MvRepository, UnavailableMvRepository};
use crate::sql::catalog::local::PlannerMemoryCatalog;
use crate::sql::catalog::{StandaloneCatalogService, TableLookupMode};
use novarocks_catalog::identifier::normalize_identifier;
use novarocks_catalog::memory::DEFAULT_DATABASE;

pub(crate) mod aggregate;
pub(crate) mod backend_resolver;
pub(crate) mod dml_change_stream;
pub(crate) mod iceberg_change_stream_write;
pub(crate) mod iceberg_ctas;
pub(crate) mod iceberg_maintenance;
pub(crate) mod iceberg_ref_flow;
pub(crate) mod information_schema;
pub(crate) mod insert;
pub(crate) mod insert_flow;
pub(crate) mod mutation_flow;
pub(crate) mod mv;
pub(crate) mod mv_flow;
pub(crate) mod mv_maintenance;
pub(crate) mod mv_rewrite_prep;
pub(crate) mod mv_scheduler;
pub(crate) mod query_prep;
mod query_stats;
pub(crate) mod statement;
pub mod statistics;
pub mod system_catalog;
pub mod table_maintenance;
pub mod view;
pub(crate) mod virtual_table;
pub(crate) mod write_operation_lifecycle;
mod write_transaction;

pub(crate) use self::insert::{build_local_insert_batch, reorder_insert_rows};
use self::statement::{
    convert_sqlparser_insert_to_custom, execute_create_database_statement,
    execute_create_table_statement, execute_drop_catalog_statement,
    execute_drop_database_statement, execute_drop_table_statement, execute_insert_statement,
    execute_truncate_table_statement, looks_like_add_equality_delete, looks_like_add_files,
    looks_like_add_legacy_range_partition, looks_like_alter_iceberg_properties,
    looks_like_alter_iceberg_schema, looks_like_alter_partition_column,
    looks_like_show_create_table, parse_add_legacy_range_partition_sql,
    parse_alter_iceberg_properties_sql, parse_alter_partition_column_sql, parse_show_create_table,
};
use crate::engine::query_prep::{has_time_travel_refs, rewrite_time_travel_refs};
#[cfg(test)]
use crate::sql::literal::{sql_type_to_arrow_type, sqlparser_expr_to_literal};

#[derive(Clone, Debug, Default)]
pub struct StandaloneOptions {
    pub config_path: Option<PathBuf>,
}

use crate::sql::parser::procedure::{looks_like_call_procedure, parse_call_procedure_sql};
use novarocks_catalog::partition::LegacyRangePartition;

#[cfg(feature = "compat")]
pub(crate) fn recover_starrocks_tablet_paths_from_current_engine(
    table: &crate::connector::starrocks::fe_v2_meta::LakeTableIdentity,
    tablet_ids: &[i64],
) -> Result<HashMap<i64, String>, String> {
    recover_starrocks_tablet_paths_from_installed_config(table, tablet_ids)
}

#[cfg(feature = "compat")]
pub(crate) fn recover_starrocks_tablet_paths_from_installed_config(
    table: &crate::connector::starrocks::fe_v2_meta::LakeTableIdentity,
    tablet_ids: &[i64],
) -> Result<HashMap<i64, String>, String> {
    if tablet_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let cfg = match novarocks_config::config() {
        Ok(cfg) => cfg,
        Err(_) => return Ok(HashMap::new()),
    };
    let Some(metadata) = cfg.metadata.as_ref() else {
        return Ok(HashMap::new());
    };
    let Some(standalone) = cfg.standalone_server.as_ref() else {
        return Ok(HashMap::new());
    };
    let Some(app_cfg) = standalone.starrocks_table_config()? else {
        return Ok(HashMap::new());
    };
    let starrocks_table_config = StarRocksTableConfig::from_app_config(app_cfg)?;
    let provider = open_metadata_provider(&ResolvedMetadataBackend {
        provider: metadata.provider,
        path: metadata.path.clone(),
    })?;
    let read = provider.begin_read().map_err(|e| {
        format!("open StarRocks table metadata recovery read transaction failed: {e}")
    })?;
    let snapshot = StarRocksTableMetaRepository
        .load_snapshot(read.as_ref())
        .map_err(|e| {
            format!("load StarRocks table metadata during tablet path recovery failed: {e}")
        })?;
    let rebuilt = StarRocksTableCatalog::rebuild_from_repository(
        Some(starrocks_table_config.clone()),
        snapshot,
    )?;
    let paths = select_starrocks_tablet_paths_from_catalog(&rebuilt, table, tablet_ids)?;
    register_starrocks_shard_infos(&starrocks_table_config.s3, &paths);
    Ok(paths)
}

#[cfg(feature = "compat")]
pub(crate) fn recover_starrocks_tablet_paths_from_state(
    state: &Arc<StandaloneState>,
    table: &crate::connector::starrocks::fe_v2_meta::LakeTableIdentity,
    tablet_ids: &[i64],
) -> Result<HashMap<i64, String>, String> {
    if tablet_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut paths = {
        let catalog = state
            .starrocks_table
            .read()
            .expect("standalone StarRocks table read lock");
        select_starrocks_tablet_paths_from_catalog(&catalog, table, tablet_ids)?
    };
    if starrocks_tablet_paths_cover(tablet_ids, &paths) {
        register_starrocks_shard_infos_from_paths(state, &paths);
        return Ok(paths);
    }

    let Some(provider) = state.metadata_provider.as_ref() else {
        register_starrocks_shard_infos_from_paths(state, &paths);
        return Ok(paths);
    };

    let read = provider.begin_read().map_err(|e| {
        format!("open StarRocks table metadata recovery read transaction failed: {e}")
    })?;
    let snapshot = state
        .starrocks_table_repo
        .load_snapshot(read.as_ref())
        .map_err(|e| {
            format!("load StarRocks table metadata during tablet path recovery failed: {e}")
        })?;
    let rebuilt = StarRocksTableCatalog::rebuild_from_repository(
        state.starrocks_table_config.clone(),
        snapshot.clone(),
    )?;
    let recovered = select_starrocks_tablet_paths_from_catalog(&rebuilt, table, tablet_ids)?;
    if !recovered.is_empty() {
        register_starrocks_shard_infos_from_paths(state, &recovered);
        paths.extend(recovered);
    }

    {
        let mut catalog = state
            .catalog_service
            .local()
            .write()
            .expect("standalone catalog write lock");
        for database in &snapshot.databases {
            catalog.create_database(&database.name)?;
        }
        register_starrocks_tables_in_catalog(&mut catalog, &rebuilt)?;
    }
    let mut guard = state
        .starrocks_table
        .write()
        .expect("standalone StarRocks table write lock");
    *guard = rebuilt;

    Ok(paths)
}

#[cfg(feature = "compat")]
fn select_starrocks_tablet_paths_from_catalog(
    catalog: &StarRocksTableCatalog,
    table: &crate::connector::starrocks::fe_v2_meta::LakeTableIdentity,
    tablet_ids: &[i64],
) -> Result<HashMap<i64, String>, String> {
    let requested = tablet_ids.iter().copied().collect::<HashSet<_>>();
    let Some(runtime) = catalog
        .runtime_by_table_id(table.table_id)
        .or_else(|| catalog.table(&table.db_name, &table.table_name).ok())
    else {
        return Ok(HashMap::new());
    };
    let mut paths = HashMap::with_capacity(requested.len());
    for tablet in &runtime.tablets {
        if requested.contains(&tablet.tablet_id) {
            paths.insert(tablet.tablet_id, tablet.tablet_root_path.clone());
        }
    }
    Ok(paths)
}

#[cfg(feature = "compat")]
fn starrocks_tablet_paths_cover(tablet_ids: &[i64], paths: &HashMap<i64, String>) -> bool {
    tablet_ids.iter().all(|tablet_id| {
        paths
            .get(tablet_id)
            .is_some_and(|path| !path.trim().is_empty())
    })
}

#[cfg(feature = "compat")]
fn register_starrocks_shard_infos_from_paths(
    state: &StandaloneState,
    paths: &HashMap<i64, String>,
) -> usize {
    let Some(config) = state.starrocks_table_config.as_ref() else {
        return 0;
    };
    register_starrocks_shard_infos(&config.s3, paths)
}

#[cfg(feature = "compat")]
fn register_starrocks_shard_infos(
    s3: &crate::runtime::starlet_shard_registry::S3StoreConfig,
    paths: &HashMap<i64, String>,
) -> usize {
    if paths.is_empty() {
        return 0;
    }
    crate::runtime::starlet_shard_registry::upsert_many_infos(paths.iter().map(
        |(tablet_id, full_path)| {
            (
                *tablet_id,
                crate::runtime::starlet_shard_registry::StarletShardInfo {
                    full_path: full_path.clone(),
                    s3: Some(s3.clone()),
                },
            )
        },
    ))
}

pub(crate) fn catalog_service_snapshot(state: &Arc<StandaloneState>) -> StandaloneCatalogService {
    StandaloneCatalogService::new(
        Arc::new(RwLock::new(state.catalog_service.local_snapshot())),
        state.catalog_service.registry_snapshot(),
    )
}

pub(crate) fn build_catalog_service_provider<'a>(
    current_catalog: Option<&'a str>,
    catalog_service: &'a StandaloneCatalogService,
    connectors: &'a crate::connector::ConnectorRegistry,
    connector_context: novarocks_spi::connector::ConnectorRequestContext,
    lookup_mode: TableLookupMode,
) -> crate::sql::catalog::provider::CatalogServiceProvider<'a> {
    crate::sql::catalog::provider::CatalogServiceProvider::new(
        current_catalog,
        catalog_service,
        connectors,
        connector_context,
        lookup_mode,
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StandaloneStarRocksTabletInfo {
    pub tablet_id: i64,
    pub bucket_seq: i64,
    pub tablet_root_path: String,
    pub runtime_registered: bool,
    pub snapshot_version: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StandaloneStarRocksTableInfo {
    pub database_name: String,
    pub table_name: String,
    pub table_id: i64,
    pub current_schema_id: i64,
    pub keys_type: String,
    pub bucket_num: i64,
    pub visible_version: i64,
    pub tablets: Vec<StandaloneStarRocksTabletInfo>,
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
                if let Some(counters) =
                    crate::query_execution::profile::format_counter_sums_from_profile_trees(
                        &fragment_profiles,
                        ICEBERG_RUNTIME_FILE_PRUNING_COUNTER_NAMES,
                        "ProfileCounters",
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
    pub(crate) catalog_service: Arc<StandaloneCatalogService>,
    pub(crate) iceberg_catalogs: Arc<RwLock<IcebergCatalogRegistry>>,
    pub(crate) starrocks_table: RwLock<StarRocksTableCatalog>,
    pub(crate) statistics_service: Arc<dyn statistics::StatisticsService>,
    pub(crate) connectors: Arc<RwLock<crate::connector::ConnectorRegistry>>,
    pub(crate) starrocks_table_config: Option<StarRocksTableConfig>,
    pub(crate) mv_refresh_pruning_limits: MvRefreshPruningLimits,
    pub(crate) metadata_provider: Option<Arc<dyn crate::meta::MetaStoreProvider>>,
    pub(crate) starrocks_table_repo: StarRocksTableMetaRepository,
    pub(crate) starrocks_txn_repo: StarRocksTxnRepository,
    /// Provider-neutral MV metadata boundary. Production wiring is installed by
    /// the frontend host; the core default deliberately rejects MV operations.
    pub(crate) mv_repository: Arc<dyn MvRepository>,
    /// Frontend-owned MV statement application boundary.
    pub(crate) mv_application_service: Arc<dyn MvApplicationService>,
    pub(crate) catalog_attachment_repo: CatalogAttachmentRepository,
    pub(crate) iceberg_operation_repo: IcebergOperationRepository,
    pub(crate) job_repo: JobMetaRepository,
    pub(crate) exchange_port: u16,
    /// Wake-up channel for the iceberg maintenance coordinator; injected by
    /// the server after the coordinator thread starts, None otherwise.
    pub(crate) maintenance_signal_tx: std::sync::Mutex<
        Option<std::sync::mpsc::Sender<crate::engine::mv_maintenance::MaintenanceSignal>>,
    >,
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
impl Default for StandaloneState {
    fn default() -> Self {
        Self {
            execution_role: crate::common::app_config::ClusterRole::AllInOne,
            catalog_service: Arc::new(crate::sql::catalog::new_standalone_catalog_service()),
            iceberg_catalogs: Arc::new(RwLock::new(IcebergCatalogRegistry::default())),
            starrocks_table: RwLock::new(StarRocksTableCatalog::default()),
            statistics_service: Arc::new(statistics::EmptyStatisticsService),
            connectors: Arc::new(RwLock::new(crate::connector::ConnectorRegistry::default())),
            starrocks_table_config: None,
            mv_refresh_pruning_limits: MvRefreshPruningLimits::default(),
            metadata_provider: None,
            starrocks_table_repo: StarRocksTableMetaRepository,
            starrocks_txn_repo: StarRocksTxnRepository,
            mv_repository: Arc::new(UnavailableMvRepository),
            mv_application_service: Arc::new(UnavailableMvApplicationService),
            catalog_attachment_repo: CatalogAttachmentRepository,
            iceberg_operation_repo: IcebergOperationRepository,
            job_repo: JobMetaRepository,
            exchange_port: 0,
            maintenance_signal_tx: std::sync::Mutex::new(None),
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
struct TestDistributedQueryCoordinator;

#[cfg(test)]
struct TestNativeReportHandler;

#[cfg(test)]
impl crate::query_execution::report::NativeReportHandler for TestNativeReportHandler {
    fn handle_native_report(
        &self,
        _report: crate::proto::novarocks::ExecStatusReport,
    ) -> Result<(), crate::query_execution::report::NativeReportHandlerError> {
        Ok(())
    }
}

#[cfg(test)]
impl crate::query_execution::contract::DistributedQueryCoordinator
    for TestDistributedQueryCoordinator
{
    fn execute(
        &self,
        request: crate::query_execution::contract::DistributedQueryRequest,
    ) -> Result<
        crate::query_execution::contract::DistributedQueryOutcome,
        crate::query_execution::contract::DistributedQueryError,
    > {
        let parts = request.into_parts();
        match parts.completion.intent() {
            crate::query_execution::contract::DistributedQueryIntent::Profile => {
                parts.completion.profile(
                    crate::runtime::query_result::QueryResult::empty(),
                    crate::query_execution::outcome::FragmentProfileSet::new(Vec::new()),
                )
            }
            crate::query_execution::contract::DistributedQueryIntent::Result => parts
                .completion
                .result(crate::runtime::query_result::QueryResult::empty()),
            crate::query_execution::contract::DistributedQueryIntent::Write => {
                parts.completion.write(
                    crate::runtime::query_result::QueryResult::empty(),
                    None,
                    None,
                )
            }
        }
    }
}

#[cfg(test)]
fn test_query_execution_service() -> crate::query_execution::service::QueryExecutionService {
    crate::query_execution::service::QueryExecutionService::new(std::sync::Arc::new(
        TestDistributedQueryCoordinator,
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
    pub table_maintenance_service:
        std::sync::Arc<dyn crate::engine::table_maintenance::TableMaintenanceService>,
    pub mv_repository: std::sync::Arc<dyn MvRepository>,
    pub mv_application_service: std::sync::Arc<dyn MvApplicationService>,
    pub query_execution: crate::query_execution::service::QueryExecutionService,
    pub backend_query_events:
        std::sync::Arc<dyn crate::query_execution::backend::BackendQueryEventSink>,
    pub backend_topology: crate::query_execution::backend::BackendTopologyService,
    pub coordinator_report_endpoint:
        std::sync::Arc<dyn crate::query_execution::backend::CoordinatorReportEndpointSink>,
    pub native_report_handler:
        std::sync::Arc<dyn crate::query_execution::report::NativeReportHandler>,
    pub query_control: crate::query_execution::control::QueryControlService,
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
        native_report_handler: std::sync::Arc<
            dyn crate::query_execution::report::NativeReportHandler,
        >,
        query_control: crate::query_execution::control::QueryControlService,
        exchange_port: u16,
    ) -> Self {
        Self {
            execution_role,
            system_catalog,
            view_service,
            statistics_service,
            table_maintenance_service,
            mv_repository,
            mv_application_service,
            query_execution,
            backend_query_events,
            backend_topology,
            coordinator_report_endpoint,
            native_report_handler,
            query_control,
            exchange_port,
        }
    }
}

impl StandaloneNovaRocks {
    pub fn open(opts: StandaloneOptions, services: StandaloneOpenServices) -> Result<Self, String> {
        #[cfg(test)]
        let _test_guard = Some(acquire_standalone_test_guard());
        match opts.config_path.as_deref() {
            Some(path) => {
                novarocks_config::init_from_path(path)
                    .map_err(|e| format!("load config failed: {e}"))?;
            }
            None => {
                #[cfg(test)]
                {
                    novarocks_config::install_default_for_test();
                }
                #[cfg(not(test))]
                {
                    novarocks_config::init_from_env_or_default()
                        .map_err(|e| format!("load config failed: {e}"))?;
                }
            }
        }
        #[cfg(test)]
        return Self::open_body(opts, services, _test_guard);
        #[cfg(not(test))]
        Self::open_body(opts, services)
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
        novarocks_config::install_preloaded_config(cfg);
        #[cfg(test)]
        return Self::open_body(opts, services, _test_guard);
        #[cfg(not(test))]
        Self::open_body(opts, services)
    }

    /// Common engine-open body.  Called after the process-wide config has
    /// already been installed by the caller.
    fn open_body(
        opts: StandaloneOptions,
        services: StandaloneOpenServices,
        #[cfg(test)] _test_guard: Option<TestSerializationGuard>,
    ) -> Result<Self, String> {
        let cfg =
            crate::novarocks_config::config().map_err(|e| format!("read config failed: {e}"))?;
        let metadata_backend = resolve_metadata_backend(&opts)?;
        let metadata_provider = metadata_backend
            .as_ref()
            .map(open_metadata_provider)
            .transpose()?;
        let starrocks_table_config = match cfg.standalone_server.as_ref() {
            Some(standalone) => standalone
                .starrocks_table_config()?
                .map(StarRocksTableConfig::from_app_config)
                .transpose()?,
            None => None,
        };
        let mv_refresh_pruning_limits = resolve_mv_refresh_pruning_limits()?;
        let StandaloneOpenServices {
            execution_role,
            system_catalog,
            view_service,
            statistics_service,
            table_maintenance_service,
            mv_repository,
            mv_application_service,
            query_execution,
            backend_query_events,
            backend_topology,
            coordinator_report_endpoint,
            native_report_handler: _,
            query_control: _,
            exchange_port,
        } = services;
        let inner = Arc::new_cyclic(|self_weak| StandaloneState {
            execution_role,
            catalog_service: Arc::new(crate::sql::catalog::new_standalone_catalog_service()),
            starrocks_table: RwLock::new(StarRocksTableCatalog::empty(
                starrocks_table_config.clone(),
            )),
            starrocks_table_config,
            mv_refresh_pruning_limits,
            metadata_provider: metadata_provider.clone(),
            starrocks_table_repo: StarRocksTableMetaRepository,
            starrocks_txn_repo: StarRocksTxnRepository,
            mv_repository,
            mv_application_service,
            catalog_attachment_repo: CatalogAttachmentRepository,
            job_repo: JobMetaRepository,
            exchange_port,
            system_catalog,
            view_service,
            statistics_service,
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
            maintenance_signal_tx: std::sync::Mutex::new(None),
        });
        register_connector_backends(&inner);
        restore_metadata_if_needed(&inner)?;
        let engine = Self { inner };
        let engine_port =
            Arc::clone(&engine.inner) as Arc<dyn table_maintenance::TableMaintenanceEngine>;
        if let Err(error) = engine.inner.table_maintenance_service.start(engine_port) {
            let primary = format!("start table maintenance service failed: {error}");
            return match engine.inner.table_maintenance_service.shutdown() {
                Ok(()) => Err(primary),
                Err(cleanup_error) => Err(format!("{primary}; cleanup failed: {cleanup_error}")),
            };
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

    pub(crate) fn publish_coordinator_report_bound_port(&self, port: u16) {
        self.inner.coordinator_report_endpoint.set_bound_port(port);
    }

    #[cfg(test)]
    pub(crate) fn state_for_test(&self) -> Arc<StandaloneState> {
        Arc::clone(&self.inner)
    }

    #[cfg(feature = "compat")]
    pub fn starrocks_table_info(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Result<StandaloneStarRocksTableInfo, String> {
        let starrocks = self
            .inner
            .starrocks_table
            .read()
            .expect("standalone StarRocks table read lock");
        let runtime = starrocks.table(database_name, table_name)?;
        let visible_version = runtime
            .partitions
            .iter()
            .map(|partition| partition.visible_version)
            .max()
            .unwrap_or(1);
        let object_store_profile = starrocks
            .config
            .as_ref()
            .map(|config| {
                crate::connector::starrocks::ObjectStoreProfile::from_s3_store_config(&config.s3)
            })
            .transpose()?;
        let tablets = runtime
            .tablets
            .iter()
            .map(|tablet| {
                let snapshot_version = object_store_profile.as_ref().and_then(|profile| {
                    crate::formats::starrocks::metadata::load_tablet_snapshot(
                        tablet.tablet_id,
                        visible_version,
                        &tablet.tablet_root_path,
                        Some(profile),
                    )
                    .ok()
                    .map(|snapshot| snapshot.version)
                });
                StandaloneStarRocksTabletInfo {
                    tablet_id: tablet.tablet_id,
                    bucket_seq: tablet.bucket_seq,
                    tablet_root_path: tablet.tablet_root_path.clone(),
                    runtime_registered: runtime_registered(tablet.tablet_id),
                    snapshot_version,
                }
            })
            .collect();
        Ok(StandaloneStarRocksTableInfo {
            database_name: runtime.database_name.clone(),
            table_name: runtime.table.name.clone(),
            table_id: runtime.table.table_id,
            current_schema_id: runtime.table.current_schema_id,
            keys_type: runtime.table.keys_type.clone(),
            bucket_num: runtime.table.bucket_num,
            visible_version,
            tablets,
        })
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
        let guard = self
            .inner
            .iceberg_catalogs
            .read()
            .expect("standalone iceberg catalog read lock");
        guard.contains_catalog(catalog_name)
    }

    pub fn iceberg_namespace_exists(
        &self,
        catalog_name: &str,
        namespace_name: &str,
    ) -> Result<bool, String> {
        let guard = self
            .inner
            .iceberg_catalogs
            .read()
            .expect("standalone iceberg catalog read lock");
        let entry = guard.get(catalog_name)?;
        iceberg_namespace_exists(&entry, namespace_name)
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
        if !Self::is_query_sql(sql) {
            return Err(
                "non-query statements must be executed through StandaloneCommandExecutor".into(),
            );
        }
        use crate::sql::parser::dialect::StarRocksDialect;
        use sqlparser::ast as sqlast;

        let current_catalog = request_context.session().current_catalog();
        let current_database = request_context.session().current_database();
        let connector_context = crate::connector::connector_request_context_for_query(
            query_opts.as_ref(),
            request_context.execution().cancellation().clone(),
        )?;
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
                let catalog_snapshot = catalog_service_snapshot
                    .local()
                    .read()
                    .expect("catalog service snapshot local read lock");
                let connectors_snapshot = self
                    .inner
                    .connectors
                    .read()
                    .expect("standalone connector registry read lock")
                    .clone();
                let analyzer_provider = build_catalog_service_provider(
                    current_catalog,
                    &catalog_service_snapshot,
                    &connectors_snapshot,
                    connector_context.clone(),
                    TableLookupMode::ExplainStats,
                );
                let result = if force_logical_explain {
                    explain_logical_query(&prepared, &analyzer_provider, current_database, level)?
                } else {
                    explain_query(
                        &prepared,
                        &analyzer_provider,
                        &catalog_snapshot,
                        &connectors_snapshot,
                        current_database,
                        level,
                        Some(&self.inner),
                        &optimizer_settings_for_execution(Some(request_context.execution())),
                    )?
                };
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
                if let Some(result) = self.inner.statistics_service.try_query(
                    &normalized,
                    query,
                    statistics::StatisticsRequestContext {
                        current_catalog,
                        current_database,
                    },
                )? {
                    return Ok(PreparedQueryOperation::Immediate(PreparedImmediateQuery {
                        result: StatementResult::Query(result),
                    }));
                }
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
                let catalog_snapshot = catalog_service_snapshot
                    .local()
                    .read()
                    .expect("catalog service snapshot local read lock");
                let connectors_snapshot = self
                    .inner
                    .connectors
                    .read()
                    .expect("standalone connector registry read lock")
                    .clone();
                let analyzer_provider = build_catalog_service_provider(
                    current_catalog,
                    &catalog_service_snapshot,
                    &connectors_snapshot,
                    connector_context.clone(),
                    TableLookupMode::SchemaOnly,
                );
                self.inner
                    .statistics_service
                    .observe_query(&prepared, current_database)?;
                let request = prepare_query_with_options_and_imv_validator_with_catalog_provider(
                    &prepared,
                    &analyzer_provider,
                    &catalog_snapshot,
                    &connectors_snapshot,
                    current_database,
                    self.inner.exchange_port,
                    query_opts,
                    &connector_context,
                    None,
                    None,
                    None,
                    None,
                    Some(&self.inner),
                    false,
                    Some(request_context.execution()),
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
        let catalog_snapshot = catalog_service_snapshot
            .local()
            .read()
            .expect("catalog service snapshot local read lock");
        let connectors_snapshot = self
            .inner
            .connectors
            .read()
            .expect("standalone connector registry read lock")
            .clone();
        let analyzer_provider = build_catalog_service_provider(
            current_catalog,
            &catalog_service_snapshot,
            &connectors_snapshot,
            connector_context.clone(),
            TableLookupMode::ExplainStats,
        );
        let planning_start = std::time::Instant::now();
        let (resolved, cte_registry, mut factory) =
            crate::sql::analyzer::analyze(&query, &analyzer_provider, current_database)?;
        let logical_plan = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)?;
        let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
        let mut optimizer_expr =
            crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr(
                &logical_plan,
                &mut scalar_arena,
            )?;
        let mut query_stats = query_stats::QueryStatsCollector::new(
            query_stats::QueryStatsProviders::from_standalone_state(&self.inner),
        )
        .collect(&mut optimizer_expr);
        let optimizer_settings = optimizer_settings_for_execution(Some(execution));
        let mv_candidates = crate::engine::mv_rewrite_prep::prepare_mv_rewrite_candidates(
            &self.inner,
            &analyzer_provider,
            current_database,
            &logical_plan,
            &mut factory,
            &mut query_stats,
            &optimizer_settings,
        );
        let optimized_tree = crate::sql::optimizer::optimize(
            optimizer_expr,
            scalar_arena,
            &query_stats.snapshot,
            factory,
            mv_candidates,
            &optimizer_settings,
        )?;
        let physical_plan =
            crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)?;
        let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan_with_settings(
            physical_plan,
            &optimizer_settings,
        )?;
        let prepared = crate::query_execution::preparation::prepare_fragments(
            &distributed_plan,
            &connectors_snapshot,
            connector_context,
            None,
        )?;
        let native_bundle = crate::protocol::native::encode::encode_native_fragment_bundle(
            &distributed_plan,
            &prepared,
        )?;
        let request =
            crate::query_execution::contract::build_distributed_query_request_with_execution(
                prepared,
                native_bundle,
                Some(query_options_for_explain_analyze(query_opts)),
                crate::query_execution::contract::DistributedQueryIntent::Profile,
                execution,
            )
            .map_err(|error| error.to_string())?;
        Ok(PreparedQueryOperation::Distributed(
            PreparedDistributedQuery {
                request,
                completion: PreparedQueryCompletion {
                    formatter: PreparedQueryFormatter::Profile(PreparedProfileFormatter {
                        distributed_plan,
                        planning_elapsed: planning_start.elapsed(),
                        execution_started_at: std::time::Instant::now(),
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
        use crate::sql::parser::dialect::{
            StarRocksDialect, looks_like_create_catalog, looks_like_create_database,
            looks_like_create_table, looks_like_drop_statement,
        };
        use sqlparser::ast as sqlast;

        let mut normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)?;
        if looks_like_add_legacy_range_partition(&normalized) {
            let stmt = parse_add_legacy_range_partition_sql(&normalized)?;
            return self.handle_add_legacy_range_partition(stmt, current_catalog, current_database);
        }
        normalized =
            rewrite_legacy_partition_references(&self.inner, &normalized, current_database)?;
        normalized = rewrite_named_partition_insert_overwrite(&normalized)?;
        if let Some(result) = self.inner.view_service.try_handle_statement(
            self.inner.as_ref(),
            &normalized,
            crate::engine::view::ViewRequestContext {
                current_catalog,
                current_database,
            },
        )? {
            return Ok(match result {
                crate::engine::view::ViewStatementResult::Ok => StatementResult::Ok,
                crate::engine::view::ViewStatementResult::Query(result) => {
                    StatementResult::Query(result)
                }
            });
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
        if let Some(result) = self.inner.statistics_service.try_handle_statement(
            &self.inner,
            &normalized,
            statistics::StatisticsRequestContext {
                current_catalog,
                current_database,
            },
        )? {
            return Ok(match result {
                statistics::StatisticsStatementResult::Ok => StatementResult::Ok,
                statistics::StatisticsStatementResult::Query(result) => {
                    StatementResult::Query(result)
                }
            });
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
        let (parse_sql, forced_explain_level, force_logical_explain) =
            if let Some((rewritten, level)) = split_explain_logical_sql(&normalized) {
                (rewritten, Some(level), true)
            } else if let Some((rewritten, level)) = split_explain_costs_sql(&normalized) {
                (rewritten, Some(level), false)
            } else {
                (normalized.clone(), None, false)
            };

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
            );
        }

        // ALTER TABLE ... ADD/DROP/RENAME/MODIFY COLUMN
        if looks_like_alter_iceberg_schema(&normalized) {
            return self.handle_alter_iceberg_schema(
                &normalized,
                current_catalog,
                current_database,
            );
        }

        // ALTER TABLE ... ADD/DROP PARTITION COLUMN ...
        if looks_like_alter_partition_column(&normalized) {
            let stmt = parse_alter_partition_column_sql(&normalized)?;
            return self.handle_alter_partition_spec(stmt, current_catalog, current_database);
        }

        // SHOW CREATE TABLE ...
        if looks_like_show_create_table(&normalized) {
            return self.handle_show_create_table(&normalized, current_catalog, current_database);
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

        // ALTER TABLE ... ADD FILES FROM '...'
        if looks_like_add_files(&normalized) {
            return self.handle_add_files(&normalized, current_catalog, current_database);
        }

        // Standard SQL: let sqlparser parse the full statement
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&parse_sql)
            .map_err(|e| format_parser_error(&e.to_string()))?;
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
                let level = forced_explain_level.unwrap_or({
                    if verbose {
                        crate::sql::explain::ExplainLevel::Verbose
                    } else {
                        crate::sql::explain::ExplainLevel::Normal
                    }
                });
                let catalog_service_snapshot = catalog_service_snapshot(&self.inner);
                let catalog_snapshot = catalog_service_snapshot
                    .local()
                    .read()
                    .expect("catalog service snapshot local read lock");
                let connectors_snapshot = self
                    .inner
                    .connectors
                    .read()
                    .expect("standalone connector registry read lock")
                    .clone();
                let analyzer_provider = build_catalog_service_provider(
                    current_catalog,
                    &catalog_service_snapshot,
                    &connectors_snapshot,
                    connector_context.clone(),
                    TableLookupMode::ExplainStats,
                );
                let result = if force_logical_explain {
                    explain_logical_query(&prepared, &analyzer_provider, current_database, level)?
                } else {
                    explain_query(
                        &prepared,
                        &analyzer_provider,
                        &catalog_snapshot,
                        &connectors_snapshot,
                        current_database,
                        level,
                        Some(&self.inner),
                        &optimizer_settings_for_execution(Some(request_context.execution())),
                    )?
                };
                Ok(StatementResult::Query(result))
            }
            sqlast::Statement::Explain {
                statement,
                analyze: true,
                ..
            } => {
                let sqlast::Statement::Query(ref query) = *statement else {
                    return Err("EXPLAIN ANALYZE only supports SELECT queries".to_string());
                };
                let prepared = prepare_explain_query(
                    &self.inner,
                    current_catalog,
                    current_database,
                    query,
                    &connector_context,
                )?;
                let catalog_service_snapshot = catalog_service_snapshot(&self.inner);
                let catalog_snapshot = catalog_service_snapshot
                    .local()
                    .read()
                    .expect("catalog service snapshot local read lock");
                let connectors_snapshot = self
                    .inner
                    .connectors
                    .read()
                    .expect("standalone connector registry read lock")
                    .clone();
                let analyzer_provider = build_catalog_service_provider(
                    current_catalog,
                    &catalog_service_snapshot,
                    &connectors_snapshot,
                    connector_context.clone(),
                    TableLookupMode::ExplainStats,
                );
                let result = explain_analyze_query(
                    &prepared,
                    &analyzer_provider,
                    &catalog_snapshot,
                    &connectors_snapshot,
                    current_database,
                    None,
                    &connector_context,
                    Some(&self.inner),
                    &self.inner.query_execution,
                    request_context.execution(),
                )?;
                Ok(StatementResult::Query(result))
            }
            sqlast::Statement::Query(ref query) => {
                if let Some(result) = self.inner.statistics_service.try_query(
                    &normalized,
                    query,
                    statistics::StatisticsRequestContext {
                        current_catalog,
                        current_database,
                    },
                )? {
                    return Ok(StatementResult::Query(result));
                }
                if let Some(result) =
                    self::information_schema::try_query_materialized_views(&self.inner, query)?
                {
                    return Ok(result);
                }

                let mut prepared = query.as_ref().clone();
                self.inner.view_service.rewrite_query(
                    self.inner.as_ref(),
                    &mut prepared,
                    crate::engine::view::ViewRequestContext {
                        current_catalog,
                        current_database,
                    },
                )?;
                // Materialize information_schema virtual tables (e.g. `schemata`)
                // into VALUES-backed derived tables. Run after view expansion
                // because a view may project from a virtual table.
                self::virtual_table::rewrite_query(&self.inner, &mut prepared)?;

                // Time-travel: `SELECT ... FROM t FOR VERSION AS OF <v>`.
                // Rewrite version-bearing table refs to synthetic per-snapshot
                // names and register only those synthetic TableDefs. Ordinary
                // Iceberg refs are resolved by CatalogServiceProvider during analysis.
                if has_time_travel_refs(&prepared) {
                    rewrite_time_travel_refs(
                        &self.inner,
                        current_catalog,
                        current_database,
                        &mut prepared,
                        &connector_context,
                    )?;
                }

                // Clone-then-release: do not hold the catalog read lock
                // across pipeline execution. Pipeline execution can run for
                // many seconds and would otherwise starve writers (e.g.
                // INSERT cleanup taking `state.catalog_service.local().write()` in
                // `invalidate_iceberg_caches`) on the std::sync::RwLock
                // writer queue.
                let catalog_service_snapshot = catalog_service_snapshot(&self.inner);
                let catalog_snapshot = catalog_service_snapshot
                    .local()
                    .read()
                    .expect("catalog service snapshot local read lock");
                let connectors_snapshot = self
                    .inner
                    .connectors
                    .read()
                    .expect("standalone connector registry read lock")
                    .clone();
                let analyzer_provider = build_catalog_service_provider(
                    current_catalog,
                    &catalog_service_snapshot,
                    &connectors_snapshot,
                    connector_context.clone(),
                    TableLookupMode::SchemaOnly,
                );
                self.inner
                    .statistics_service
                    .observe_query(&prepared, current_database)?;
                let result = execute_query_with_catalog_provider_with_execution(
                    &prepared,
                    &analyzer_provider,
                    &catalog_snapshot,
                    &connectors_snapshot,
                    current_database,
                    self.inner.exchange_port,
                    query_opts.clone(),
                    &self.inner.query_execution,
                    &connector_context,
                    Some(&self.inner),
                    request_context.execution(),
                )?;
                Ok(StatementResult::Query(result))
            }
            sqlast::Statement::Insert(ref insert) => self.handle_sqlparser_insert(
                insert,
                current_catalog,
                current_database,
                query_opts.as_ref(),
                Some(request_context.execution()),
                &connector_context,
            ),
            sqlast::Statement::Delete(ref delete) => {
                let stmt = crate::engine::statement::convert_sqlparser_delete_to_custom(delete)?;
                crate::engine::delete_flow::execute_delete_statement(
                    &self.inner,
                    &stmt,
                    current_catalog,
                    current_database,
                    request_context.execution(),
                    &connector_context,
                )
            }
            ref update_stmt @ sqlast::Statement::Update(_) => {
                if let Some(result) = self::information_schema::try_update_be_configs(update_stmt)?
                {
                    return Ok(result);
                }
                let stmt =
                    crate::engine::statement::convert_sqlparser_update_to_custom(update_stmt)?;
                let result = crate::engine::mutation_flow::execute_update_statement(
                    &self.inner,
                    &stmt,
                    current_catalog,
                    current_database,
                    request_context.execution(),
                    &connector_context,
                )?;
                self.inner
                    .statistics_service
                    .observe_update(&normalized, current_database)?;
                Ok(result)
            }
            ref merge_stmt @ sqlast::Statement::Merge(_) => {
                let stmt = crate::engine::statement::convert_sqlparser_merge_to_custom(merge_stmt)?;
                crate::engine::mutation_flow::execute_merge_statement(
                    &self.inner,
                    &stmt,
                    current_catalog,
                    current_database,
                    request_context.execution(),
                    &connector_context,
                )
            }
            sqlast::Statement::Truncate(truncate) => {
                for truncate_table in &truncate.table_names {
                    let table_name = crate::sql::parser::dialect::convert_object_name(
                        truncate_table.name.clone(),
                    )?;
                    execute_truncate_table_statement(
                        &self.inner,
                        &table_name,
                        "main",
                        current_catalog,
                        current_database,
                        &connector_context,
                    )?;
                }
                Ok(StatementResult::Ok)
            }
            _ => Err(format!(
                "unsupported sql: {}",
                sql.chars().take(50).collect::<String>()
            )),
        }
    }

    fn is_query_sql(sql: &str) -> bool {
        matches!(
            sql.split_whitespace()
                .next()
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str(),
            "select" | "with" | "explain"
        )
    }

    /// Handle ALTER TABLE ... ADD FILES FROM '...'
    fn handle_add_files(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        crate::engine::query_prep::add_files(&self.inner, sql, current_catalog, current_database)
    }

    fn handle_add_legacy_range_partition(
        &self,
        stmt: crate::engine::statement::AlterLegacyRangePartitionStmt,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        let target = crate::engine::backend_resolver::resolve_existing_table_target(
            &self.inner,
            &stmt.table,
            current_catalog,
            current_database,
        )?;
        if target.backend_name == "iceberg" {
            return Err(
                "ALTER TABLE ADD PARTITION only supports standalone StarRocks tables".into(),
            );
        }
        let mut catalog = self
            .inner
            .catalog_service
            .local()
            .write()
            .expect("standalone catalog write lock");
        let table_def = catalog.get(&target.namespace, &target.table)?;
        let mut partition = stmt.partition;
        if partition.column.is_empty() {
            partition.column = table_def
                .columns
                .first()
                .map(|column| column.name.clone())
                .ok_or_else(|| {
                    format!(
                        "cannot infer range partition column for empty table schema {}.{}",
                        target.namespace, target.table
                    )
                })?;
        }
        catalog.add_legacy_range_partition(&target.namespace, &target.table, partition)?;
        Ok(StatementResult::Ok)
    }

    fn handle_show_create_table(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
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
        let entry = {
            let registry = self
                .inner
                .iceberg_catalogs
                .read()
                .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
            registry.get(&target.catalog)?
        };
        entry.invalidate_table_cache(&target.namespace, &target.table);
        let loaded = crate::connector::iceberg::catalog::registry::load_table(
            &entry,
            &target.namespace,
            &target.table,
        )?;
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
        crate::connector::iceberg::catalog::alter_table_properties(
            &self.inner,
            &stmt,
            current_catalog,
            current_database,
        )?;
        Ok(StatementResult::Ok)
    }

    fn handle_alter_iceberg_schema(
        &self,
        sql: &str,
        current_catalog: Option<&str>,
        current_database: &str,
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
        crate::connector::iceberg::catalog::alter_table_schema(
            &self.inner,
            &stmt,
            current_catalog,
            current_database,
        )?;
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
        let backend = self
            .inner
            .connectors
            .read()
            .expect("connector registry read")
            .catalog_backend(target.backend_name)?;
        backend.alter_iceberg_partition_spec(
            &target.catalog,
            &target.namespace,
            &target.table,
            stmt,
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
        let stmt = crate::engine::statement::parse_add_equality_delete_sql(sql)?;
        crate::engine::equality_delete_flow::execute_add_equality_delete_statement(
            &self.inner,
            &stmt,
            current_catalog,
            current_database,
            connector_context,
        )
    }

    /// Handle CREATE CATALOG result.
    fn handle_create_catalog(
        &self,
        stmt: crate::sql::parser::ast::CreateCatalogStmt,
    ) -> Result<StatementResult, String> {
        let normalized_catalog = normalize_identifier(&stmt.name)?;
        let mut guard = self
            .inner
            .iceberg_catalogs
            .write()
            .expect("standalone iceberg catalog write lock");
        let created = !guard.contains_catalog(&stmt.name)?;
        guard.create_catalog(&stmt.name, &stmt.properties)?;
        let persisted_properties = guard.get(&stmt.name)?.properties().to_vec();
        drop(guard);
        if let Err(error) = register_iceberg_connector_instance(&self.inner, &normalized_catalog) {
            if created {
                self.inner
                    .iceberg_catalogs
                    .write()
                    .expect("standalone iceberg catalog write lock")
                    .drop_catalog(&normalized_catalog)?;
            }
            return Err(error);
        }
        let connectors = self
            .inner
            .connectors
            .read()
            .expect("connector registry read lock")
            .clone();
        self.inner
            .catalog_service
            .register_catalog(crate::sql::catalog::build_iceberg_catalog(
                &stmt.name, connectors,
            ));
        if let Err(error) = persist_catalog_attachment_if_needed(
            &self.inner,
            &normalized_catalog,
            &persisted_properties,
        ) {
            unregister_iceberg_connector_instance(&self.inner, &normalized_catalog)?;
            if created {
                self.inner
                    .iceberg_catalogs
                    .write()
                    .expect("standalone iceberg catalog write lock")
                    .drop_catalog(&normalized_catalog)?;
            }
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
        let connectors = self
            .inner
            .connectors
            .read()
            .expect("connector registry read");
        let source_table = crate::connector::metadata_load_table(
            &connectors,
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
                    self.inner.statistics_service.drop_database(&database);
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
                self.inner
                    .statistics_service
                    .drop_database(&target.namespace);
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
                )?;
                match stmt.name.parts.as_slice() {
                    [table] => self
                        .inner
                        .statistics_service
                        .drop_table(current_database, table),
                    [database, table] => self.inner.statistics_service.drop_table(database, table),
                    [_, database, table] => {
                        self.inner.statistics_service.drop_table(database, table)
                    }
                    _ => {}
                }
                Ok(result)
            }
        }
    }

    /// Consolidated INSERT handler using sqlparser AST. All INSERT targets
    /// flow through the custom parser so the shared dispatch in
    /// `execute_insert_statement` chooses between standalone table backends.
    fn handle_sqlparser_insert(
        &self,
        insert: &sqlparser::ast::Insert,
        current_catalog: Option<&str>,
        current_database: &str,
        query_opts: Option<&QueryOptions>,
        execution: Option<&crate::query_execution::request_context::QueryExecutionContext>,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        self.execute_insert_via_custom_parser(
            insert,
            current_catalog,
            current_database,
            query_opts,
            execution,
            connector_context,
        )
    }

    /// Convert sqlparser INSERT to our custom InsertStmt and delegate to the
    /// shared dispatcher in `execute_insert_statement`.
    fn execute_insert_via_custom_parser(
        &self,
        insert: &sqlparser::ast::Insert,
        current_catalog: Option<&str>,
        current_database: &str,
        query_opts: Option<&QueryOptions>,
        execution: Option<&crate::query_execution::request_context::QueryExecutionContext>,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, String> {
        let insert_stmt = convert_sqlparser_insert_to_custom(insert)?;
        execute_insert_statement(
            &self.inner,
            &insert_stmt.table,
            &insert_stmt.columns,
            &insert_stmt.source,
            insert_stmt.overwrite_mode,
            current_catalog,
            current_database,
            query_opts,
            execution,
            connector_context,
        )
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
    use crate::query_execution::backend::BackendTopologySnapshot;
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
            BackendTopologySnapshot::empty(0),
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

/// Generate a `CREATE TABLE` DDL string from a loaded Iceberg table's current
/// schema.  Column doc strings are emitted as `COMMENT '...'` clauses.
fn build_iceberg_create_table_ddl(
    catalog: &str,
    namespace: &str,
    table: &str,
    loaded: &crate::connector::iceberg::catalog::registry::IcebergLoadedTable,
) -> Result<String, String> {
    use iceberg::spec::{PrimitiveType, Type};

    fn iceberg_type_to_sql(ty: &Type) -> String {
        match ty {
            Type::Primitive(PrimitiveType::Boolean) => "BOOLEAN".to_string(),
            Type::Primitive(PrimitiveType::Int) => "INT".to_string(),
            Type::Primitive(PrimitiveType::Long) => "BIGINT".to_string(),
            Type::Primitive(PrimitiveType::Float) => "FLOAT".to_string(),
            Type::Primitive(PrimitiveType::Double) => "DOUBLE".to_string(),
            Type::Primitive(PrimitiveType::Decimal { precision, scale }) => {
                format!("DECIMAL({precision},{scale})")
            }
            Type::Primitive(PrimitiveType::Date) => "DATE".to_string(),
            Type::Primitive(PrimitiveType::Time) => "TIME".to_string(),
            Type::Primitive(PrimitiveType::Timestamp)
            | Type::Primitive(PrimitiveType::Timestamptz) => "DATETIME".to_string(),
            Type::Primitive(PrimitiveType::TimestampNs)
            | Type::Primitive(PrimitiveType::TimestamptzNs) => "TIMESTAMP_NS".to_string(),
            Type::Primitive(PrimitiveType::String) => "STRING".to_string(),
            Type::Primitive(PrimitiveType::Uuid) => "STRING".to_string(),
            Type::Primitive(PrimitiveType::Fixed(n)) => format!("BINARY({n})"),
            Type::Primitive(PrimitiveType::Binary) => "BINARY".to_string(),
            Type::Primitive(PrimitiveType::Variant) => "VARIANT".to_string(),
            Type::List(l) => format!(
                "ARRAY<{}>",
                iceberg_type_to_sql(&l.element_field.field_type)
            ),
            Type::Map(m) => format!(
                "MAP<{},{}>",
                iceberg_type_to_sql(&m.key_field.field_type),
                iceberg_type_to_sql(&m.value_field.field_type)
            ),
            Type::Struct(s) => {
                let fields: Vec<String> = s
                    .fields()
                    .iter()
                    .map(|f| format!("{} {}", f.name, iceberg_type_to_sql(&f.field_type)))
                    .collect();
                format!("STRUCT<{}>", fields.join(", "))
            }
        }
    }

    let schema = loaded.table.metadata().current_schema();
    let mut col_defs: Vec<String> = Vec::new();
    for field in schema.as_struct().fields() {
        let nullable = if field.required { " NOT NULL" } else { "" };
        let comment = if let Some(doc) = &field.doc {
            let escaped = doc.replace('\'', "\\'");
            format!(" COMMENT '{escaped}'")
        } else {
            String::new()
        };
        col_defs.push(format!(
            "  `{}` {}{}{}",
            field.name,
            iceberg_type_to_sql(&field.field_type),
            nullable,
            comment
        ));
    }

    // Emit table-level COMMENT if the "comment" property is set and non-empty.
    let table_comment = loaded
        .table
        .metadata()
        .properties()
        .get("comment")
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

pub(crate) mod delete_flow;
pub(crate) mod delete_predicate_translate;
pub(crate) mod equality_delete_flow;
pub(crate) mod iceberg_truncate;
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
        Statement::DropMaterializedView(stmt) => {
            crate::engine::mv_flow::drop_mv(state, current_catalog, current_database, &stmt)
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
        Statement::RefreshMaterializedView(stmt) => {
            crate::engine::mv_flow::refresh_mv_with_connector_context(
                state,
                current_catalog,
                current_database,
                &stmt,
                connector_context,
            )
        }
        Statement::ShowMaterializedViews(stmt) => {
            crate::engine::mv_flow::list_mvs(state, current_catalog, &stmt)
        }
        Statement::AlterIcebergRef(stmt) => crate::engine::iceberg_ref_flow::execute(
            state,
            current_database,
            &stmt,
            connector_context,
        ),
        Statement::Truncate { name, target_ref } => {
            crate::engine::statement::execute_truncate_table_statement(
                state,
                &name,
                &target_ref,
                current_catalog,
                current_database,
                connector_context,
            )
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
    }
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
) -> Result<Option<ResolvedMetadataBackend>, String> {
    let cfg = novarocks_config::config().map_err(|e| format!("read config failed: {e}"))?;
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

fn resolve_mv_refresh_pruning_limits() -> Result<MvRefreshPruningLimits, String> {
    let cfg = novarocks_config::config().map_err(|e| format!("read config failed: {e}"))?;
    Ok(cfg
        .standalone_server
        .as_ref()
        .map(|config| MvRefreshPruningLimits {
            max_touched_groups: config.mv_refresh_max_touched_groups,
            max_affected_partitions: config.mv_refresh_max_affected_partitions,
        })
        .unwrap_or_default())
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
    // and persist their rebuilt definitions. Runs after catalog attachments are
    // installed and before refresh recovery (so W3b recovery sees the
    // rediscovered target tables).
    crate::engine::mv::lake_rebuild::rebuild_imv_cache_from_lake(state)?;
    crate::engine::mv::iceberg_refresh::recover_iceberg_mv_refreshes(state)?;
    crate::engine::mv::iceberg_refresh::restore_iceberg_mv_targets(state)?;
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
    let connectors = state
        .connectors
        .read()
        .expect("connector registry read lock")
        .clone();

    for catalog in &catalogs {
        {
            let mut guard = state
                .iceberg_catalogs
                .write()
                .expect("standalone iceberg catalog write lock");
            guard.create_catalog(&catalog.catalog, &catalog.properties.properties)?;
        }
        let normalized_catalog = normalize_identifier(&catalog.catalog)?;
        if let Err(error) = register_iceberg_connector_instance(state, &normalized_catalog) {
            state
                .iceberg_catalogs
                .write()
                .expect("standalone iceberg catalog write lock")
                .drop_catalog(&normalized_catalog)?;
            return Err(error);
        }
        state
            .catalog_service
            .register_catalog(crate::sql::catalog::build_iceberg_catalog(
                &catalog.catalog,
                connectors.clone(),
            ));
    }

    Ok(())
}

fn register_iceberg_connector_instance(
    state: &Arc<StandaloneState>,
    normalized_catalog: &str,
) -> Result<(), String> {
    let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(normalized_catalog)
        .map_err(|error| format!("invalid Iceberg connector instance ID: {error}"))?;
    let instance = crate::connector::iceberg::provider::IcebergConnectorInstance::new(
        instance_id,
        Arc::clone(&state.iceberg_catalogs),
    )
    .map_err(|error| format!("create Iceberg connector instance: {error}"))?;
    state
        .connectors
        .write()
        .expect("connector registry write lock")
        .register_connector_instance(instance)
        .map_err(|error| format!("register Iceberg connector instance: {error}"))
}

fn unregister_iceberg_connector_instance(
    state: &Arc<StandaloneState>,
    normalized_catalog: &str,
) -> Result<(), String> {
    let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse(normalized_catalog)
        .map_err(|error| format!("invalid Iceberg connector instance ID: {error}"))?;
    state
        .connectors
        .write()
        .expect("connector registry write lock")
        .unregister_connector_instance(&instance_id)
        .map(|_| ())
        .map_err(|error| format!("unregister Iceberg connector instance: {error}"))
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
        .upsert(
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
        .delete(txn.as_mut(), catalog_name)
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

fn capture_maintenance_execution(
    state: &StandaloneState,
) -> Result<crate::query_execution::request_context::QueryExecutionContext, String> {
    let topology = state
        .backend_topology
        .snapshot()
        .map_err(|error| error.to_string())?;
    let cancellation = crate::query_execution::cancellation::QueryCancellationSource::new();
    Ok(
        crate::query_execution::request_context::QueryExecutionContext::new(
            state.execution_role,
            topology,
            None,
            cancellation.view(),
            crate::sql::optimizer::options::SessionOptimizerSettings::default(),
        ),
    )
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
        },
    )?;

    // Time-travel refs become synthetic local tables. Ordinary Iceberg refs
    // remain untouched and resolve through CatalogServiceProvider during analysis.
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

/// Execute the DistributedPlan, then produce an EXPLAIN-style result whose
/// first row is `Planning: <ms> / Execution: <ms> / Rows: <N>` followed by
/// the profiled plan body.
#[allow(clippy::too_many_arguments)]
fn explain_analyze_query(
    query: &sqlparser::ast::Query,
    analyzer_catalog: &dyn crate::sql::catalog::PlannerTableProvider,
    _codegen_catalog: &PlannerMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    query_opts: Option<QueryOptions>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    mv_rewrite_state: Option<&Arc<StandaloneState>>,
    query_execution: &crate::query_execution::service::QueryExecutionService,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
) -> Result<QueryResult, String> {
    use crate::sql::explain::ExplainLevel;
    use crate::sql::explain::distributed::explain_distributed_plan_analyze;

    let planning_start = std::time::Instant::now();
    let (resolved, cte_registry, mut factory) =
        crate::sql::analyzer::analyze(query, analyzer_catalog, current_database)?;
    let logical_plan = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)?;
    let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
    let mut optimizer_expr = crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr(
        &logical_plan,
        &mut scalar_arena,
    )?;
    let providers = mv_rewrite_state
        .map(query_stats::QueryStatsProviders::from_standalone_state)
        .unwrap_or_else(|| query_stats::QueryStatsProviders::from_connectors(connectors));
    let mut query_stats =
        query_stats::QueryStatsCollector::new(providers).collect(&mut optimizer_expr);
    let optimizer_settings = optimizer_settings_for_execution(Some(execution));
    let mv_candidates = match mv_rewrite_state {
        Some(state) => crate::engine::mv_rewrite_prep::prepare_mv_rewrite_candidates(
            state,
            analyzer_catalog,
            current_database,
            &logical_plan,
            &mut factory,
            &mut query_stats,
            &optimizer_settings,
        ),
        None => Vec::new(),
    };
    let optimized_tree = crate::sql::optimizer::optimize(
        optimizer_expr,
        scalar_arena,
        &query_stats.snapshot,
        factory,
        mv_candidates,
        &optimizer_settings,
    )?;

    let physical_plan = crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)?;
    let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan_with_settings(
        physical_plan,
        &optimizer_settings,
    )?;
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed_plan,
        connectors,
        connector_context,
        None,
    )?;
    let native_bundle = crate::protocol::native::encode::encode_native_fragment_bundle(
        &distributed_plan,
        &prepared,
    )?;
    let planning_elapsed = planning_start.elapsed();

    let query_opts = query_options_for_explain_analyze(query_opts);
    let execution_start = std::time::Instant::now();
    let outcome = execute_distributed_profile_with_execution(
        query_execution,
        prepared,
        native_bundle,
        Some(query_opts),
        execution,
    )?;
    let execution_elapsed = execution_start.elapsed();
    if let Some(abort) = outcome.write_abort.as_ref() {
        return Err(abort.reason.clone());
    }
    if outcome.fragment_profiles.is_empty() {
        return Err("EXPLAIN ANALYZE completed without fragment runtime profiles".to_string());
    }

    let actuals =
        crate::query_execution::profile::collect_actuals_by_plan_node_id_from_profile_trees(
            &outcome.fragment_profiles,
        );
    let profile_summary =
        crate::query_execution::profile::collect_distributed_profile_summary_from_profile_trees(
            &outcome.fragment_profiles,
        );
    let per_fragment = crate::query_execution::profile::collect_per_fragment_profile_summaries(
        &outcome.fragment_profiles,
    );
    let mut lines = Vec::new();
    lines.push(format!(
        "Planning: {} / Execution: {} / Rows: {}",
        format_explain_analyze_duration(planning_elapsed),
        format_explain_analyze_duration(execution_elapsed),
        outcome.query_result.row_count()
    ));
    lines.push(format_distributed_profile_summary(&profile_summary));
    if let Some(apply) =
        crate::query_execution::profile::collect_native_runtime_filter_apply_from_profile_trees(
            &outcome.fragment_profiles,
        )
    {
        lines.push(apply.to_string());
    }
    if let Some(counters) = crate::query_execution::profile::format_counter_sums_from_profile_trees(
        &outcome.fragment_profiles,
        ICEBERG_RUNTIME_FILE_PRUNING_COUNTER_NAMES,
        "ProfileCounters",
    ) {
        lines.push(counters);
    }
    lines.extend(explain_distributed_plan_analyze(
        &distributed_plan,
        ExplainLevel::Analyze,
        &actuals,
        Some(&per_fragment),
    ));
    build_string_query_result("Explain String", lines)
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

/// Produce non-distributed logical EXPLAIN output for a query without
/// optimizing or building the DistributedPlan IR.
fn explain_logical_query(
    query: &sqlparser::ast::Query,
    analyzer_catalog: &dyn crate::sql::catalog::PlannerTableProvider,
    current_database: &str,
    level: crate::sql::explain::ExplainLevel,
) -> Result<QueryResult, String> {
    let (resolved, cte_registry, mut factory) =
        crate::sql::analyzer::analyze(query, analyzer_catalog, current_database)?;
    let logical_plan = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)?;
    let lines = crate::sql::explain::explain_plan_checked(&logical_plan, level)?;
    build_string_query_result("Explain String", lines)
}

/// Produce EXPLAIN output for a query without executing it.
fn explain_query(
    query: &sqlparser::ast::Query,
    analyzer_catalog: &dyn crate::sql::catalog::PlannerTableProvider,
    _codegen_catalog: &PlannerMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    level: crate::sql::explain::ExplainLevel,
    mv_rewrite_state: Option<&Arc<StandaloneState>>,
    optimizer_settings: &crate::sql::optimizer::options::SessionOptimizerSettings,
) -> Result<QueryResult, String> {
    use crate::sql::explain::ExplainLevel;
    use crate::sql::explain::distributed::explain_distributed_plan;

    let (resolved, cte_registry, mut factory) =
        crate::sql::analyzer::analyze(query, analyzer_catalog, current_database)?;
    let logical_plan = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)?;
    let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
    let mut optimizer_expr = crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr(
        &logical_plan,
        &mut scalar_arena,
    )?;
    let providers = mv_rewrite_state
        .map(query_stats::QueryStatsProviders::from_standalone_state)
        .unwrap_or_else(|| query_stats::QueryStatsProviders::from_connectors(connectors));
    let mut query_stats =
        query_stats::QueryStatsCollector::new(providers).collect(&mut optimizer_expr);
    // MV query rewrite candidate prep (plain EXPLAIN has no MV refresh
    // context, so the gate is only `mv_rewrite_state.is_some()`).
    let mv_candidates = match mv_rewrite_state {
        Some(state) => crate::engine::mv_rewrite_prep::prepare_mv_rewrite_candidates(
            state,
            analyzer_catalog,
            current_database,
            &logical_plan,
            &mut factory,
            &mut query_stats,
            optimizer_settings,
        ),
        None => Vec::new(),
    };
    let optimized_tree = crate::sql::optimizer::optimize(
        optimizer_expr,
        scalar_arena,
        &query_stats.snapshot,
        factory,
        mv_candidates,
        optimizer_settings,
    )?;

    let mut lines = Vec::new();
    if matches!(level, ExplainLevel::Costs) {
        lines.extend(query_stats.snapshot.display_rows());
    }
    let physical_plan = crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)?;
    let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan_with_settings(
        physical_plan,
        optimizer_settings,
    )?;
    lines.extend(explain_distributed_plan(&distributed_plan, level));

    build_string_query_result("Explain String", lines)
}

pub(crate) fn execute_query(
    query: &sqlparser::ast::Query,
    catalog: &PlannerMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<QueryOptions>,
    query_execution: &crate::query_execution::service::QueryExecutionService,
) -> Result<QueryResult, String> {
    let connector_context = crate::connector::connector_request_context(
        query_opts.as_ref(),
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )?;
    execute_query_with_catalog_provider(
        query,
        catalog,
        catalog,
        connectors,
        current_database,
        exchange_port,
        query_opts,
        query_execution,
        &connector_context,
        None,
    )
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
    let catalog_snapshot = catalog_service_snapshot
        .local()
        .read()
        .expect("catalog service snapshot local read lock");
    let connectors_snapshot = state
        .connectors
        .read()
        .expect("standalone connector registry read lock")
        .clone();
    let analyzer_provider = build_catalog_service_provider(
        current_catalog,
        &catalog_service_snapshot,
        &connectors_snapshot,
        connector_context.clone(),
        TableLookupMode::SchemaOnly,
    );
    execute_query_with_catalog_provider(
        query,
        &analyzer_provider,
        &catalog_snapshot,
        &connectors_snapshot,
        current_database,
        state.exchange_port,
        query_opts,
        &state.query_execution,
        connector_context,
        Some(state),
    )
}

pub(crate) type IcebergWriteRootDistributionResolver = Box<
    dyn FnOnce(
        &crate::sql::planner::logical::LogicalPlanNode,
    ) -> Result<Option<crate::sql::optimizer::property::DistributionSpec>, String>,
>;

#[allow(dead_code)]
pub(crate) fn iceberg_write_shuffle_by_output_name(
    output_name: impl Into<String>,
) -> IcebergWriteRootDistributionResolver {
    let output_name = output_name.into();
    Box::new(move |logical| {
        let output_columns = crate::sql::planner::plan_output_columns(logical)?;
        let mut matches = output_columns
            .iter()
            .filter(|column| column.name == output_name);
        let column = matches.next().ok_or_else(|| {
            format!(
                "cannot derive Iceberg write root shuffle: output column '{output_name}' not found"
            )
        })?;
        if matches.next().is_some() {
            return Err(format!(
                "cannot derive Iceberg write root shuffle: output column '{output_name}' is ambiguous"
            ));
        }
        iceberg_write_shuffle_for_output_column(column)
    })
}

#[allow(dead_code)]
pub(crate) fn iceberg_write_shuffle_by_output_index(
    output_index: usize,
) -> IcebergWriteRootDistributionResolver {
    Box::new(move |logical| {
        let output_columns = crate::sql::planner::plan_output_columns(logical)?;
        let column = output_columns.get(output_index).ok_or_else(|| {
            format!(
                "cannot derive Iceberg write root shuffle: output column index {output_index} out of range ({} columns)",
                output_columns.len()
            )
        })?;
        iceberg_write_shuffle_for_output_column(column)
    })
}

fn iceberg_write_shuffle_for_output_column(
    column: &crate::sql::analysis::OutputColumn,
) -> Result<Option<crate::sql::optimizer::property::DistributionSpec>, String> {
    if column.column_id == crate::sql::column_id::ColumnId::UNSET {
        return Err(format!(
            "cannot derive Iceberg write root shuffle: output column '{}' has no ColumnId",
            column.name
        ));
    }
    Ok(Some(
        crate::sql::optimizer::property::DistributionSpec::shuffle_agg([column.column_id]),
    ))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_query_as_iceberg_write(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    sink_spec: crate::sql::planner::distributed::write::sink::IcebergWriteSinkSpec,
    query_opts: Option<QueryOptions>,
    root_distribution_resolver: Option<IcebergWriteRootDistributionResolver>,
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
        sink_spec,
        query_opts,
        root_distribution_resolver,
        execution,
        &connector_context,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_query_as_iceberg_write_with_connector_context(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    sink_spec: crate::sql::planner::distributed::write::sink::IcebergWriteSinkSpec,
    query_opts: Option<QueryOptions>,
    root_distribution_resolver: Option<IcebergWriteRootDistributionResolver>,
    execution: Option<&crate::query_execution::request_context::QueryExecutionContext>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
    let optimizer_settings = optimizer_settings_for_execution(execution);
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
    let connectors_snapshot = state
        .connectors
        .read()
        .expect("standalone connector registry read lock")
        .clone();
    let analyzer_provider = build_catalog_service_provider(
        current_catalog,
        &catalog_service_snapshot,
        &connectors_snapshot,
        connector_context.clone(),
        TableLookupMode::SchemaOnly,
    );

    let (resolved, cte_registry, mut factory) =
        crate::sql::analyzer::analyze(&prepared, &analyzer_provider, current_database)?;
    let logical_plan = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)?;
    let root_distribution = match root_distribution_resolver {
        Some(resolve_root_distribution) => resolve_root_distribution(&logical_plan)?,
        None => None,
    };
    let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
    let mut optimizer_expr = crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr(
        &logical_plan,
        &mut scalar_arena,
    )?;
    let providers = query_stats::QueryStatsProviders::from_standalone_state(state);
    let query_stats = query_stats::QueryStatsCollector::new(providers).collect(&mut optimizer_expr);
    let optimized_tree = match root_distribution {
        Some(root_distribution) => crate::sql::optimizer::optimize_with_root_distribution(
            optimizer_expr,
            scalar_arena,
            &query_stats.snapshot,
            factory,
            root_distribution,
            &optimizer_settings,
        )?,
        None => crate::sql::optimizer::optimize(
            optimizer_expr,
            scalar_arena,
            &query_stats.snapshot,
            factory,
            Vec::new(),
            &optimizer_settings,
        )?,
    };
    let physical_plan = crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)?;
    let distributed_plan = crate::sql::planner::pipeline::build_iceberg_write_distributed_plan_with_settings(
        physical_plan,
        crate::sql::planner::distributed::write::sink::IcebergWriteFragmentSink {
            descriptor_database: current_database.to_string(),
            spec: sink_spec,
            input: crate::sql::planner::distributed::write::sink::IcebergWriteInputBinding::RootOutputByOrdinal,
        },
        &optimizer_settings,
    )?;
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed_plan,
        &connectors_snapshot,
        &connector_context,
        None,
    )?;
    let native_bundle = crate::protocol::native::encode::encode_native_fragment_bundle(
        &distributed_plan,
        &prepared,
    )?;
    let maintenance_execution;
    let execution = match execution {
        Some(execution) => execution,
        None => {
            maintenance_execution = capture_maintenance_execution(state)?;
            &maintenance_execution
        }
    };
    execute_distributed_write_with_execution(
        &state.query_execution,
        prepared,
        native_bundle,
        query_opts,
        execution,
    )
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
    branch_kinds: Vec<crate::sql::common::ChangeStreamBranchKind>,
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
    topology: &crate::sql::planner::distributed::write::change_stream::IcebergChangeStreamWriteTopology,
) -> Option<crate::query_execution::outcome::QueryExecutionResult> {
    let mut observer = change_stream_write_test_observer()
        .lock()
        .expect("change-stream write test observer lock");
    let observer = observer.as_mut()?;
    observer
        .observations
        .push(ChangeStreamWriteBuildObservation {
            entrypoint: ChangeStreamWriteEntrypoint::PhysicalPlan,
            branch_kinds: topology
                .writer_branches
                .iter()
                .map(|branch| branch.branch_kind)
                .collect(),
            writer_fragment_ids: topology
                .writer_branches
                .iter()
                .map(|branch| Some(branch.writer_fragment_id))
                .collect(),
        });
    if observer.short_circuit_after_build {
        Some(crate::query_execution::outcome::QueryExecutionResult {
            query_result: crate::runtime::query_result::QueryResult::empty(),
            write_commit: None,
            write_abort: None,
            fragment_profiles: Vec::new(),
        })
    } else {
        None
    }
}

pub(crate) struct PlannedIcebergChangeStreamWrite {
    pub(crate) prepared: crate::query_execution::preparation::PreparedFragmentSet,
    pub(crate) native_bundle: crate::protocol::native::encode::NativeFragmentBundle,
    pub(crate) commit_plan:
        crate::connector::iceberg::change_stream_routing::ChangeStreamWriterCommitPlan,
    #[cfg(test)]
    pub(crate) topology:
        crate::sql::planner::distributed::write::change_stream::IcebergChangeStreamWriteTopology,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_physical_plan_as_iceberg_change_stream_write(
    state: &Arc<StandaloneState>,
    _current_catalog: Option<&str>,
    current_database: &str,
    optimized_tree: &crate::sql::optimizer::OptimizedOperatorNode,
    dag: &mut crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec,
    mv_refresh_ctx: Option<&crate::mv::refresh::execution_context::IcebergMvRefreshContext>,
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
        dag,
        mv_refresh_ctx,
        pre_expand_keyed_assert,
        &connector_context,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_physical_plan_as_iceberg_change_stream_write_with_connector_context(
    state: &Arc<StandaloneState>,
    _current_catalog: Option<&str>,
    current_database: &str,
    optimized_tree: &crate::sql::optimizer::OptimizedOperatorNode,
    dag: &mut crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec,
    mv_refresh_ctx: Option<&crate::mv::refresh::execution_context::IcebergMvRefreshContext>,
    pre_expand_keyed_assert: Option<crate::sql::planner::physical::PreExpandKeyedAssertSpec>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<PlannedIcebergChangeStreamWrite, String> {
    crate::connector::validate_request_context(connector_context)?;
    let connectors_snapshot = state
        .connectors
        .read()
        .expect("standalone connector registry read lock")
        .clone();
    let physical_plan = crate::sql::planner::optimizer_bridge::to_physical_plan(optimized_tree)?;
    let planned_dp =
        crate::sql::planner::pipeline::build_iceberg_change_stream_distributed_plan_with_settings(
            physical_plan,
            current_database,
            dag.clone(),
            pre_expand_keyed_assert,
            &crate::sql::optimizer::options::SessionOptimizerSettings::default(),
        )?;
    let distributed_plan = planned_dp.distributed_plan;
    let topology = planned_dp.topology;
    let scan_binding_resolver = mv_refresh_ctx
        .map(|ctx| ctx as &dyn crate::query_execution::preparation::scan::ScanBindingResolver);
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed_plan,
        &connectors_snapshot,
        connector_context,
        scan_binding_resolver,
    )?;
    let native_bundle = crate::protocol::native::encode::encode_native_fragment_bundle(
        &distributed_plan,
        &prepared,
    )?;
    let commit_plan = crate::connector::iceberg::change_stream_routing::ChangeStreamWriterCommitPlan::from_topology(
        &topology,
    )?;
    Ok(PlannedIcebergChangeStreamWrite {
        prepared,
        native_bundle,
        commit_plan,
        #[cfg(test)]
        topology,
    })
}

pub(crate) fn execute_planned_iceberg_change_stream_write(
    state: &Arc<StandaloneState>,
    prepared: crate::query_execution::preparation::PreparedFragmentSet,
    native_bundle: crate::protocol::native::encode::NativeFragmentBundle,
    query_opts: Option<QueryOptions>,
    execution: Option<&crate::query_execution::request_context::QueryExecutionContext>,
) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
    let maintenance_execution;
    let execution = match execution {
        Some(execution) => execution,
        None => {
            maintenance_execution = capture_maintenance_execution(state)?;
            &maintenance_execution
        }
    };
    execute_distributed_write_with_execution(
        &state.query_execution,
        prepared,
        native_bundle,
        query_opts,
        execution,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_physical_plan_as_iceberg_change_stream_write(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    optimized_tree: &crate::sql::optimizer::OptimizedOperatorNode,
    dag: &mut crate::sql::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec,
    query_opts: Option<QueryOptions>,
    mv_refresh_ctx: Option<&crate::mv::refresh::execution_context::IcebergMvRefreshContext>,
) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
    let planned = build_physical_plan_as_iceberg_change_stream_write(
        state,
        current_catalog,
        current_database,
        optimized_tree,
        dag,
        mv_refresh_ctx,
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
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_query_with_catalog_provider(
    query: &sqlparser::ast::Query,
    analyzer_catalog: &dyn crate::sql::catalog::PlannerTableProvider,
    codegen_catalog: &PlannerMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<QueryOptions>,
    query_execution: &crate::query_execution::service::QueryExecutionService,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    mv_rewrite_state: Option<&Arc<StandaloneState>>,
) -> Result<QueryResult, String> {
    execute_query_with_options_and_imv_validator_with_catalog_provider(
        query,
        analyzer_catalog,
        codegen_catalog,
        connectors,
        current_database,
        exchange_port,
        query_opts,
        query_execution,
        connector_context,
        None,
        None,
        None,
        None,
        mv_rewrite_state,
        false,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_query_with_catalog_provider_with_execution(
    query: &sqlparser::ast::Query,
    analyzer_catalog: &dyn crate::sql::catalog::PlannerTableProvider,
    codegen_catalog: &PlannerMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<QueryOptions>,
    query_execution: &crate::query_execution::service::QueryExecutionService,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    mv_rewrite_state: Option<&Arc<StandaloneState>>,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
) -> Result<QueryResult, String> {
    execute_query_with_options_and_imv_validator_with_catalog_provider(
        query,
        analyzer_catalog,
        codegen_catalog,
        connectors,
        current_database,
        exchange_port,
        query_opts,
        query_execution,
        connector_context,
        None,
        None,
        None,
        None,
        mv_rewrite_state,
        false,
        Some(execution),
    )
}

/// Extended `execute_query` entry used while refresh call sites are moving to
/// the mainline distributed execution path. `terminal_sink` and
/// `iceberg_catalogs` remain in the signature during that transition, but
/// non-`None` values are rejected at the mainline execution boundary until
/// `DistributedPlan` has native sink and write support.
///
/// `execute_query_with_options(..., mv_refresh_ctx = Some(ctx))` runs the
/// IMV rewrite pipeline before optimization and also passes the refresh
/// context into codegen. Pre-expanded MV refresh SQL that only needs the
/// codegen context must use `execute_preexpanded_mv_refresh_query_with_options`.
pub(crate) type ImvRewriteValidator<'a> = dyn Fn(&crate::sql::planner::imv_rewrite::entrypoint::ImvRewriteOutcome) -> Result<(), String>
    + 'a;

pub(crate) fn execute_query_with_options(
    query: &sqlparser::ast::Query,
    catalog: &PlannerMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<QueryOptions>,
    query_execution: &crate::query_execution::service::QueryExecutionService,
    terminal_sink: Option<Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory>>,
    iceberg_catalogs: Option<&crate::connector::iceberg::catalog::IcebergCatalogRegistry>,
    mv_refresh_ctx: Option<&crate::mv::refresh::execution_context::IcebergMvRefreshContext>,
) -> Result<QueryResult, String> {
    let connector_context = crate::connector::connector_request_context(
        query_opts.as_ref(),
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )?;
    execute_query_with_options_and_imv_validator(
        query,
        catalog,
        connectors,
        current_database,
        exchange_port,
        query_opts,
        query_execution,
        terminal_sink,
        iceberg_catalogs,
        mv_refresh_ctx,
        &connector_context,
        None,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_query_with_options_and_imv_validator(
    query: &sqlparser::ast::Query,
    catalog: &PlannerMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<QueryOptions>,
    query_execution: &crate::query_execution::service::QueryExecutionService,
    terminal_sink: Option<Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory>>,
    iceberg_catalogs: Option<&crate::connector::iceberg::catalog::IcebergCatalogRegistry>,
    mv_refresh_ctx: Option<&crate::mv::refresh::execution_context::IcebergMvRefreshContext>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    imv_rewrite_validator: Option<&ImvRewriteValidator<'_>>,
    mv_rewrite_state: Option<&Arc<StandaloneState>>,
) -> Result<QueryResult, String> {
    execute_query_with_options_and_imv_validator_with_catalog_provider(
        query,
        catalog,
        catalog,
        connectors,
        current_database,
        exchange_port,
        query_opts,
        query_execution,
        connector_context,
        terminal_sink,
        iceberg_catalogs,
        mv_refresh_ctx,
        imv_rewrite_validator,
        mv_rewrite_state,
        mv_refresh_ctx.is_some(),
        None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_preexpanded_mv_refresh_query_with_options(
    query: &sqlparser::ast::Query,
    catalog: &PlannerMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<QueryOptions>,
    query_execution: &crate::query_execution::service::QueryExecutionService,
    terminal_sink: Option<Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory>>,
    iceberg_catalogs: Option<&crate::connector::iceberg::catalog::IcebergCatalogRegistry>,
    mv_refresh_ctx: Option<&crate::mv::refresh::execution_context::IcebergMvRefreshContext>,
) -> Result<QueryResult, String> {
    let connector_context = crate::connector::connector_request_context(
        query_opts.as_ref(),
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )?;
    execute_query_with_options_and_imv_validator_with_catalog_provider(
        query,
        catalog,
        catalog,
        connectors,
        current_database,
        exchange_port,
        query_opts,
        query_execution,
        &connector_context,
        terminal_sink,
        iceberg_catalogs,
        mv_refresh_ctx,
        None,
        None,
        false,
        None,
    )
}

pub(crate) struct PlannedIcebergChangeStreamRefreshQuery {
    pub(crate) optimized_tree: crate::sql::optimizer::OptimizedOperatorNode,
    pub(crate) output_columns: Vec<crate::sql::analysis::OutputColumn>,
    pub(crate) change_stream:
        crate::sql::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn plan_query_for_iceberg_change_stream_refresh(
    query: &sqlparser::ast::Query,
    analyzer_catalog: &dyn crate::sql::catalog::PlannerTableProvider,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    mv_refresh_ctx: Option<&crate::mv::refresh::execution_context::IcebergMvRefreshContext>,
    imv_rewrite_validator: Option<&ImvRewriteValidator<'_>>,
    run_imv_rewrite: bool,
) -> Result<PlannedIcebergChangeStreamRefreshQuery, String> {
    let (resolved, cte_registry, mut factory) =
        crate::sql::analyzer::analyze(query, analyzer_catalog, current_database)?;
    let mut logical_plan = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)?;
    let mut change_stream =
        crate::sql::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor::default();
    if run_imv_rewrite {
        let mv_ctx =
            mv_refresh_ctx.ok_or_else(|| "IMV rewrite requires MV refresh context".to_string())?;
        logical_plan =
            crate::engine::mv::iceberg_refresh::normalize_imv_rewrite_root_project(logical_plan);
        let factory_cell = std::rc::Rc::new(std::cell::RefCell::new(factory));
        let outcome = crate::sql::planner::imv_rewrite::entrypoint::run_imv_rewrite(
            crate::sql::planner::imv_rewrite::entrypoint::ImvRewriteInput {
                plan: logical_plan,
                disabled_rules: crate::sql::optimizer::options::SessionOptimizerSettings::default()
                    .disabled_rules,
                mv_ctx: std::sync::Arc::clone(&mv_ctx.rewrite),
                deadline: None,
                column_ref_factory: std::rc::Rc::clone(&factory_cell),
            },
        )
        .map_err(|e| format!("imv rewrite: {e}"))?;
        if let Some(validator) = imv_rewrite_validator {
            validator(&outcome)?;
        }
        factory = std::rc::Rc::try_unwrap(factory_cell)
            .map_err(|_| "IMV rewrite leaked ColumnRefFactory references".to_string())?
            .into_inner();
        change_stream = outcome.annotation.change_stream.clone();
        logical_plan = outcome.plan;
    } else if imv_rewrite_validator.is_some() {
        return Err("IMV rewrite validator requires MV refresh context".to_string());
    }

    let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
    let mut optimizer_expr = crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr(
        &logical_plan,
        &mut scalar_arena,
    )?;
    let providers = query_stats::QueryStatsProviders::from_connectors(connectors);
    let query_stats = query_stats::QueryStatsCollector::new(providers).collect(&mut optimizer_expr);
    let optimized_tree = crate::sql::optimizer::optimize(
        optimizer_expr,
        scalar_arena,
        &query_stats.snapshot,
        factory,
        Vec::new(),
        &crate::sql::optimizer::options::SessionOptimizerSettings::default(),
    )?;
    let output_columns = optimized_tree.output_columns.clone();
    Ok(PlannedIcebergChangeStreamRefreshQuery {
        optimized_tree,
        output_columns,
        change_stream,
    })
}

pub(crate) fn plan_logical_for_iceberg_change_stream_refresh(
    logical_plan: crate::sql::planner::logical::LogicalPlanNode,
    factory: crate::sql::column_id::ColumnRefFactory,
    connectors: &crate::connector::ConnectorRegistry,
) -> Result<PlannedIcebergChangeStreamRefreshQuery, String> {
    let change_stream =
        crate::sql::planner::imv_rewrite::change_stream::build_change_stream_descriptor(
            &logical_plan,
        );
    let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
    let mut optimizer_expr = crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr(
        &logical_plan,
        &mut scalar_arena,
    )?;
    let providers = query_stats::QueryStatsProviders::from_connectors(connectors);
    let query_stats = query_stats::QueryStatsCollector::new(providers).collect(&mut optimizer_expr);
    let optimized_tree = crate::sql::optimizer::optimize(
        optimizer_expr,
        scalar_arena,
        &query_stats.snapshot,
        factory,
        Vec::new(),
        &crate::sql::optimizer::options::SessionOptimizerSettings::default(),
    )?;
    let output_columns = optimized_tree.output_columns.clone();
    Ok(PlannedIcebergChangeStreamRefreshQuery {
        optimized_tree,
        output_columns,
        change_stream,
    })
}

#[allow(clippy::too_many_arguments)]
fn execute_query_with_options_and_imv_validator_with_catalog_provider(
    query: &sqlparser::ast::Query,
    analyzer_catalog: &dyn crate::sql::catalog::PlannerTableProvider,
    _codegen_catalog: &PlannerMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<QueryOptions>,
    query_execution: &crate::query_execution::service::QueryExecutionService,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    terminal_sink: Option<Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory>>,
    iceberg_catalogs: Option<&crate::connector::iceberg::catalog::IcebergCatalogRegistry>,
    mv_refresh_ctx: Option<&crate::mv::refresh::execution_context::IcebergMvRefreshContext>,
    imv_rewrite_validator: Option<&ImvRewriteValidator<'_>>,
    mv_rewrite_state: Option<&Arc<StandaloneState>>,
    run_imv_rewrite: bool,
    execution: Option<&crate::query_execution::request_context::QueryExecutionContext>,
) -> Result<QueryResult, String> {
    let request = prepare_query_with_options_and_imv_validator_with_catalog_provider(
        query,
        analyzer_catalog,
        _codegen_catalog,
        connectors,
        current_database,
        exchange_port,
        query_opts,
        connector_context,
        terminal_sink,
        iceberg_catalogs,
        mv_refresh_ctx,
        imv_rewrite_validator,
        mv_rewrite_state,
        run_imv_rewrite,
        execution,
    )?;
    query_execution
        .execute(request)
        .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_result)
        .map(crate::query_execution::outcome::ResultExecutionOutcome::into_query_result)
        .map_err(|error| error.to_string())
}

#[allow(clippy::too_many_arguments)]
fn prepare_query_with_options_and_imv_validator_with_catalog_provider(
    query: &sqlparser::ast::Query,
    analyzer_catalog: &dyn crate::sql::catalog::PlannerTableProvider,
    _codegen_catalog: &PlannerMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    current_database: &str,
    exchange_port: u16,
    query_opts: Option<QueryOptions>,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    terminal_sink: Option<Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory>>,
    iceberg_catalogs: Option<&crate::connector::iceberg::catalog::IcebergCatalogRegistry>,
    mv_refresh_ctx: Option<&crate::mv::refresh::execution_context::IcebergMvRefreshContext>,
    imv_rewrite_validator: Option<&ImvRewriteValidator<'_>>,
    mv_rewrite_state: Option<&Arc<StandaloneState>>,
    run_imv_rewrite: bool,
    execution: Option<&crate::query_execution::request_context::QueryExecutionContext>,
) -> Result<crate::query_execution::contract::DistributedQueryRequest, String> {
    let optimizer_settings = optimizer_settings_for_execution(execution);
    let (resolved, cte_registry, mut factory) =
        crate::sql::analyzer::analyze(query, analyzer_catalog, current_database)?;
    let mut logical_plan = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)?;
    if run_imv_rewrite {
        let mv_ctx =
            mv_refresh_ctx.ok_or_else(|| "IMV rewrite requires MV refresh context".to_string())?;
        logical_plan =
            crate::engine::mv::iceberg_refresh::normalize_imv_rewrite_root_project(logical_plan);
        let factory_cell = std::rc::Rc::new(std::cell::RefCell::new(factory));
        let outcome = crate::sql::planner::imv_rewrite::entrypoint::run_imv_rewrite(
            crate::sql::planner::imv_rewrite::entrypoint::ImvRewriteInput {
                plan: logical_plan,
                disabled_rules: optimizer_settings.disabled_rules.clone(),
                mv_ctx: std::sync::Arc::clone(&mv_ctx.rewrite),
                deadline: None,
                column_ref_factory: std::rc::Rc::clone(&factory_cell),
            },
        )
        .map_err(|e| format!("imv rewrite: {e}"))?;
        if let Some(validator) = imv_rewrite_validator {
            validator(&outcome)?;
        }
        factory = std::rc::Rc::try_unwrap(factory_cell)
            .map_err(|_| "IMV rewrite leaked ColumnRefFactory references".to_string())?
            .into_inner();
        logical_plan = outcome.plan;
    } else if imv_rewrite_validator.is_some() {
        return Err("IMV rewrite validator requires MV refresh context".to_string());
    }
    let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
    let mut optimizer_expr = crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr(
        &logical_plan,
        &mut scalar_arena,
    )?;
    let providers = mv_rewrite_state
        .map(query_stats::QueryStatsProviders::from_standalone_state)
        .unwrap_or_else(|| query_stats::QueryStatsProviders::from_connectors(connectors));
    let mut query_stats =
        query_stats::QueryStatsCollector::new(providers).collect(&mut optimizer_expr);
    // MV query rewrite: discover fresh Iceberg MV candidates and inject their
    // target-table stats. Gated on a standalone-rewrite path (`Some(state)`)
    // and disabled during MV refresh (`mv_refresh_ctx.is_some()`) so refresh
    // queries never rewrite onto the MV they are computing.
    let mv_candidates = match mv_rewrite_state {
        Some(state) if mv_refresh_ctx.is_none() => {
            crate::engine::mv_rewrite_prep::prepare_mv_rewrite_candidates(
                state,
                analyzer_catalog,
                current_database,
                &logical_plan,
                &mut factory,
                &mut query_stats,
                &optimizer_settings,
            )
        }
        _ => Vec::new(),
    };
    let optimized_tree = crate::sql::optimizer::optimize(
        optimizer_expr,
        scalar_arena,
        &query_stats.snapshot,
        factory,
        mv_candidates,
        &optimizer_settings,
    )?;

    ensure_mainline_distributed_execution(
        terminal_sink.is_some(),
        iceberg_catalogs.is_some(),
        exchange_port,
    )?;

    let physical_plan = crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)?;
    let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan_with_settings(
        physical_plan,
        &optimizer_settings,
    )?;
    let scan_binding_resolver = mv_refresh_ctx
        .map(|ctx| ctx as &dyn crate::query_execution::preparation::scan::ScanBindingResolver);
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed_plan,
        connectors,
        connector_context,
        scan_binding_resolver,
    )?;
    let native_bundle = crate::protocol::native::encode::encode_native_fragment_bundle(
        &distributed_plan,
        &prepared,
    )?;
    let maintenance_execution;
    let execution = match execution {
        Some(execution) => execution,
        None => {
            let state = mv_rewrite_state.ok_or_else(|| {
                "distributed execution requires a request execution context".to_string()
            })?;
            maintenance_execution = capture_maintenance_execution(state)?;
            &maintenance_execution
        }
    };
    crate::query_execution::contract::build_distributed_query_request_with_execution(
        prepared,
        native_bundle,
        query_opts,
        crate::query_execution::contract::DistributedQueryIntent::Result,
        execution,
    )
    .map_err(|error| error.to_string())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_logical_plan_with_options(
    logical_plan: crate::sql::planner::logical::LogicalPlanNode,
    factory: crate::sql::column_id::ColumnRefFactory,
    _codegen_catalog: &PlannerMemoryCatalog,
    connectors: &crate::connector::ConnectorRegistry,
    _current_database: &str,
    exchange_port: u16,
    query_opts: Option<QueryOptions>,
    query_execution: &crate::query_execution::service::QueryExecutionService,
    terminal_sink: Option<Box<dyn crate::exec::pipeline::operator_factory::OperatorFactory>>,
    iceberg_catalogs: Option<&crate::connector::iceberg::catalog::IcebergCatalogRegistry>,
    mv_refresh_ctx: Option<&crate::mv::refresh::execution_context::IcebergMvRefreshContext>,
    mv_rewrite_state: Option<&Arc<StandaloneState>>,
    execution: &crate::query_execution::request_context::QueryExecutionContext,
) -> Result<QueryResult, String> {
    let optimizer_settings = optimizer_settings_for_execution(Some(execution));
    let connector_context = crate::connector::connector_request_context(
        query_opts.as_ref(),
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
    )?;
    let mut scalar_arena = crate::sql::optimizer::scalar::ScalarArena::new();
    let mut optimizer_expr = crate::sql::planner::optimizer_bridge::logical::try_to_optimizer_expr(
        &logical_plan,
        &mut scalar_arena,
    )?;
    let providers = mv_rewrite_state
        .map(query_stats::QueryStatsProviders::from_standalone_state)
        .unwrap_or_else(|| query_stats::QueryStatsProviders::from_connectors(connectors));
    let query_stats = query_stats::QueryStatsCollector::new(providers).collect(&mut optimizer_expr);
    let optimized_tree = crate::sql::optimizer::optimize(
        optimizer_expr,
        scalar_arena,
        &query_stats.snapshot,
        factory,
        Vec::new(),
        &optimizer_settings,
    )?;
    ensure_mainline_distributed_execution(
        terminal_sink.is_some(),
        iceberg_catalogs.is_some(),
        exchange_port,
    )?;

    let physical_plan = crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized_tree)?;
    let distributed_plan = crate::sql::planner::pipeline::build_distributed_plan_with_settings(
        physical_plan,
        &optimizer_settings,
    )?;
    let scan_binding_resolver = mv_refresh_ctx
        .map(|ctx| ctx as &dyn crate::query_execution::preparation::scan::ScanBindingResolver);
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed_plan,
        connectors,
        &connector_context,
        scan_binding_resolver,
    )?;
    let native_bundle = crate::protocol::native::encode::encode_native_fragment_bundle(
        &distributed_plan,
        &prepared,
    )?;
    execute_distributed_result_with_execution(
        query_execution,
        prepared,
        native_bundle,
        query_opts,
        execution,
    )
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
) -> Result<crate::query_execution::outcome::QueryExecutionResult, String> {
    let request = crate::query_execution::contract::build_distributed_query_request_with_execution(
        prepared,
        native_bundle,
        query_options,
        crate::query_execution::contract::DistributedQueryIntent::Write,
        execution,
    )
    .map_err(|error| error.to_string())?;
    let (query_result, write_commit, write_abort) = query_execution
        .execute(request)
        .and_then(crate::query_execution::contract::DistributedQueryOutcome::into_write)
        .map(crate::query_execution::outcome::WriteExecutionOutcome::into_parts)
        .map_err(|error| error.to_string())?;
    Ok(crate::query_execution::outcome::QueryExecutionResult {
        query_result,
        write_commit,
        write_abort,
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
        fragment_profiles: fragment_profiles.into_profiles(),
    })
}

#[cfg(test)]
pub(crate) struct StandaloneLoopbackTestBackend {
    pub(crate) exchange_port: u16,
    _test_guard: TestSerializationGuard,
}

#[cfg(test)]
pub(crate) fn install_all_in_one_loopback_backend_for_test()
-> Result<StandaloneLoopbackTestBackend, String> {
    let test_guard = acquire_standalone_test_guard();
    crate::novarocks_config::install_default_for_test();
    let exchange_port = ensure_standalone_exchange_server(Arc::new(TestNativeReportHandler))?;
    Ok(StandaloneLoopbackTestBackend {
        exchange_port,
        _test_guard: test_guard,
    })
}

#[cfg(test)]
fn ensure_standalone_exchange_server(
    native_report_handler: Arc<dyn crate::query_execution::report::NativeReportHandler>,
) -> Result<u16, String> {
    static STANDALONE_EXCHANGE_PORT: OnceLock<u16> = OnceLock::new();

    if let Some(port) = STANDALONE_EXCHANGE_PORT.get() {
        return Ok(*port);
    }

    let default_port = crate::common::config::grpc_port();
    let started_port = match crate::service::grpc_server::start_grpc_exchange_server(
        "127.0.0.1",
        default_port,
        crate::service::grpc_server::rejecting_test_native_fragment_ingress(),
        Arc::clone(&native_report_handler),
    ) {
        Ok(()) => crate::service::grpc_server::grpc_server_bound_port()
            .map_err(|e| format!("read standalone grpc exchange server port failed: {e}"))?,
        Err(e) if e.contains("Address already in use") || e.contains("os error 48") => {
            let listener = TcpListener::bind(("127.0.0.1", 0)).map_err(|bind_err| {
                format!("reserve standalone grpc exchange port failed: {bind_err}")
            })?;
            let fallback_port = listener
                .local_addr()
                .map_err(|addr_err| {
                    format!("read standalone grpc exchange port failed: {addr_err}")
                })?
                .port();
            drop(listener);
            crate::service::grpc_server::start_grpc_exchange_server(
                "127.0.0.1",
                fallback_port,
                crate::service::grpc_server::rejecting_test_native_fragment_ingress(),
                native_report_handler,
            )
            .map_err(|start_err| {
                format!(
                    "start standalone grpc exchange server failed on fallback port {}: {}",
                    fallback_port, start_err
                )
            })?;
            crate::service::grpc_server::grpc_server_bound_port().map_err(|e| {
                format!("read standalone grpc exchange server fallback port failed: {e}")
            })?
        }
        Err(e) => return Err(format!("start standalone grpc exchange server failed: {e}")),
    };

    wait_for_standalone_exchange_server(started_port)?;

    if STANDALONE_EXCHANGE_PORT.set(started_port).is_err() {
        return Ok(*STANDALONE_EXCHANGE_PORT
            .get()
            .expect("standalone exchange port initialized"));
    }
    Ok(started_port)
}

#[cfg(test)]
fn wait_for_standalone_exchange_server(port: u16) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match TcpStream::connect(("127.0.0.1", port)) {
            Ok(stream) => {
                drop(stream);
                return Ok(());
            }
            Err(err) if Instant::now() < deadline => {
                let _ = err;
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(err) => {
                return Err(format!(
                    "standalone grpc exchange server on 127.0.0.1:{} did not become ready: {}",
                    port, err
                ));
            }
        }
    }
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
    use crate::connector::iceberg::catalog::registry::IcebergLoadedTable;
    use iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, Type,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    fn loaded_table_with_props(props: HashMap<String, String>) -> IcebergLoadedTable {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .expect("build schema");
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "/tmp/test".to_string(),
            FormatVersion::V2,
            props,
        )
        .expect("builder")
        .build()
        .expect("metadata")
        .metadata;
        let table = iceberg::table::Table::builder()
            .identifier(iceberg::TableIdent::from_strs(["db", "t"]).unwrap())
            .file_io(iceberg::io::FileIO::new_with_fs())
            .metadata(metadata)
            .build()
            .expect("table");
        IcebergLoadedTable {
            table,
            columns: vec![],
            logical_types: HashMap::new(),
            key_desc: None,
            column_aggregations: HashMap::new(),
            object_store_config: None,
        }
    }

    #[test]
    fn emits_comment_when_property_is_set() {
        let mut props = HashMap::new();
        props.insert("comment".to_string(), "my table comment".to_string());
        let loaded = loaded_table_with_props(props);
        let ddl = build_iceberg_create_table_ddl("cat", "ns", "tbl", &loaded).expect("build ddl");
        assert!(
            ddl.contains("COMMENT 'my table comment'"),
            "expected COMMENT clause in DDL, got: {ddl}"
        );
    }

    #[test]
    fn no_comment_clause_when_property_absent() {
        let loaded = loaded_table_with_props(HashMap::new());
        let ddl = build_iceberg_create_table_ddl("cat", "ns", "tbl", &loaded).expect("build ddl");
        assert!(
            !ddl.contains("COMMENT"),
            "expected no COMMENT clause when property absent, got: {ddl}"
        );
    }

    #[test]
    fn no_comment_clause_when_property_empty() {
        let mut props = HashMap::new();
        props.insert("comment".to_string(), String::new());
        let loaded = loaded_table_with_props(props);
        let ddl = build_iceberg_create_table_ddl("cat", "ns", "tbl", &loaded).expect("build ddl");
        assert!(
            !ddl.contains("COMMENT"),
            "expected no COMMENT clause when property is empty string, got: {ddl}"
        );
    }

    #[test]
    fn comment_with_single_quote_is_escaped() {
        let mut props = HashMap::new();
        props.insert("comment".to_string(), "it's great".to_string());
        let loaded = loaded_table_with_props(props);
        let ddl = build_iceberg_create_table_ddl("cat", "ns", "tbl", &loaded).expect("build ddl");
        assert!(
            ddl.contains("COMMENT 'it\\'s great'"),
            "expected escaped single quote in COMMENT, got: {ddl}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{
        QueryResult, StandaloneNovaRocks, StandaloneOpenServices, StandaloneOptions,
        StandaloneSession, StandaloneState, StatementResult, dispatch_statement,
        register_connector_backends,
    };
    #[cfg(feature = "compat")]
    use super::{
        recover_starrocks_tablet_paths_from_installed_config,
        recover_starrocks_tablet_paths_from_state,
    };
    #[cfg(feature = "compat")]
    use crate::connector::starrocks::fe_v2_meta::LakeTableIdentity;
    #[cfg(feature = "compat")]
    use crate::connector::starrocks::lake::context::lock_runtime_test_state;
    #[cfg(feature = "compat")]
    use crate::connector::starrocks::table::config::StarRocksTableConfig;
    use crate::engine::statistics::{
        CatalogColumnStatistics, CatalogTableStatistics, StatisticsEngine,
        StatisticsInsertObservation, StatisticsRequestContext, StatisticsService,
        StatisticsStatementResult,
    };
    use crate::engine::system_catalog::{SystemCatalog, SystemCatalogInputs, SystemTableData};
    use crate::engine::view::{ViewEngine, ViewRequestContext, ViewService, ViewStatementResult};
    use crate::exec::spill::{SpillConfig, SpillMode};
    use crate::meta::MetaStoreProvider;
    use crate::mv::application::{
        MvApplicationError, MvApplicationErrorKind, MvApplicationService, MvApplicationStatement,
        MvEngine, MvRequestContext, UnavailableMvApplicationService,
    };
    use crate::mv::repository::{MvTarget, UnavailableMvRepository};
    use crate::query_execution::backend::{BackendTopologyPort, LiveBackendTarget};
    use crate::query_execution::contract::{
        DistributedQueryCoordinator, DistributedQueryError, DistributedQueryOutcome,
        DistributedQueryRequest,
    };
    use crate::query_execution::service::QueryExecutionService;
    use crate::runtime::query_options::QueryOptions;
    use arrow::array::{
        Array, FixedSizeBinaryArray, Int32Array, Int64Array, ListArray, StringArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tempfile::TempDir;

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

    #[derive(Default)]
    struct RecordingStatisticsService {
        statements: Mutex<Vec<String>>,
    }

    impl RecordingStatisticsService {
        fn statements(&self) -> Vec<String> {
            self.statements
                .lock()
                .expect("statistics statements")
                .clone()
        }
    }

    impl StatisticsService for RecordingStatisticsService {
        fn try_handle_statement(
            &self,
            _engine: &dyn StatisticsEngine,
            sql: &str,
            _context: StatisticsRequestContext<'_>,
        ) -> Result<Option<StatisticsStatementResult>, String> {
            if sql.to_ascii_lowercase().starts_with("analyze ") {
                self.statements
                    .lock()
                    .expect("statistics statements")
                    .push(sql.to_string());
                return Ok(Some(StatisticsStatementResult::Ok));
            }
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
            _database: &str,
            _table: &str,
        ) -> Result<Option<CatalogTableStatistics>, String> {
            Ok(None)
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
                        1,
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

        fn record_successful_fragment_submission(&self, backend_idx: usize) {
            if let Some(entry) = self.state.lock().unwrap().entries.get_mut(&backend_idx) {
                entry.scheduled_fragments = entry.scheduled_fragments.saturating_add(1);
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
            ];
            let mut columns = vec![Vec::<String>::new(); column_names.len()];
            for (backend_idx, entry) in &self.state.lock().unwrap().entries {
                columns[0].push(backend_idx.to_string());
                columns[1].push(entry.endpoint.ip().to_string());
                columns[2].push(entry.endpoint.port().to_string());
                columns[3].push(entry.state.as_str().to_string());
                columns[4].push(entry.scheduled_fragments.to_string());
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
        StandaloneOpenServices::new(
            crate::common::app_config::ClusterRole::AllInOne,
            system_catalog,
            view_service,
            statistics_service,
            Arc::new(crate::engine::table_maintenance::EmptyTableMaintenanceService),
            Arc::new(UnavailableMvRepository),
            Arc::new(UnavailableMvApplicationService),
            super::test_query_execution_service(),
            Arc::new(crate::query_execution::backend::NoopBackendQueryEventSink),
            backend_topology,
            Arc::new(crate::query_execution::backend::NoopCoordinatorReportEndpointSink),
            Arc::new(super::TestNativeReportHandler),
            crate::query_execution::control::QueryControlService::for_test(),
            0,
        )
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
    fn open_with_config_uses_injected_statistics_service() {
        let service = Arc::new(RecordingStatisticsService::default());
        let engine = StandaloneNovaRocks::open_with_config(
            StandaloneOptions::default(),
            crate::common::app_config::NovaRocksConfig::default(),
            test_open_services_with_statistics(
                Arc::new(crate::engine::system_catalog::EmptySystemCatalog),
                Arc::new(crate::engine::view::EmptyViewService),
                Arc::clone(&service) as Arc<dyn StatisticsService>,
            ),
        )
        .expect("open");
        engine
            .session()
            .execute_in_database("ANALYZE TABLE t1", "db1")
            .expect("injected service handles statement");
        assert_eq!(service.statements(), vec!["ANALYZE TABLE t1"]);
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
            ]
        );
    }

    #[cfg(not(feature = "compat"))]
    fn lock_runtime_test_state() -> super::TestSerializationGuard {
        super::acquire_standalone_test_guard()
    }

    #[cfg(feature = "compat")]
    fn test_starrocks_table_config() -> StarRocksTableConfig {
        StarRocksTableConfig {
            warehouse_uri: "s3://test/warehouse".to_string(),
            s3: crate::runtime::starlet_shard_registry::S3StoreConfig {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: "test".to_string(),
                access_key_id: "ak".to_string(),
                access_key_secret: "sk".to_string(),
                region: None,
                enable_path_style_access: Some(true),
            },
            mv_default_storage_engine: "starrocks".to_string(),
        }
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
                "127.0.0.1:0".parse().unwrap(),
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

        crate::novarocks_config::init_from_path(&config_path).expect("load config");
        let backend = super::resolve_metadata_backend(&StandaloneOptions {
            config_path: Some(config_path.clone()),
        })
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

        crate::novarocks_config::init_from_path(&config_path).expect("load config");
        let backend = super::resolve_metadata_backend(&StandaloneOptions {
            config_path: Some(config_path.clone()),
        })
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
        assert!(err.contains("unknown catalog"));
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
    fn query_without_exchange_backend_fails_instead_of_direct_exec() {
        let query = parse_query_for_engine_test("select 1");
        let catalog = super::PlannerMemoryCatalog::default();
        let connectors = crate::connector::ConnectorRegistry::default();
        let query_execution = super::test_query_execution_service();

        let err = super::execute_query_with_options(
            &query,
            &catalog,
            &connectors,
            "default",
            0,
            None,
            &query_execution,
            None,
            None,
            None,
        )
        .expect_err("exchange_port=0 must not execute through direct fallback");

        assert!(
            err.contains("distributed execution requires an exchange backend"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn query_with_loopback_exchange_backend_uses_distributed_path() {
        let backend = super::install_all_in_one_loopback_backend_for_test()
            .expect("install loopback backend");
        let query = parse_query_for_engine_test("select 1");
        let catalog = super::PlannerMemoryCatalog::default();
        let connectors = crate::connector::ConnectorRegistry::default();
        let query_execution = super::test_query_execution_service();

        let result = super::execute_query_with_options(
            &query,
            &catalog,
            &connectors,
            "default",
            backend.exchange_port,
            None,
            &query_execution,
            None,
            None,
            None,
        )
        .expect("query should execute through mainline coordinator");

        assert_eq!(result.row_count(), 1);
    }

    fn dummy_mv_refresh_context_for_validator_test()
    -> crate::mv::refresh::execution_context::IcebergMvRefreshContext {
        use iceberg::spec::{
            FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
            TableMetadataBuilder, Type,
        };
        use iceberg::table::Table;
        use iceberg::{NamespaceIdent, TableIdent};

        let warehouse_dir = tempfile::TempDir::new()
            .expect("target warehouse tempdir")
            .keep();
        let warehouse = format!("file://{}", warehouse_dir.join("warehouse").display());

        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "k", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema");
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            format!("{warehouse}/validator-target/table"),
            FormatVersion::V3,
            std::collections::HashMap::from([(
                "write.row-lineage".to_string(),
                "true".to_string(),
            )]),
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata;
        let target_table = Table::builder()
            .file_io(iceberg::io::FileIO::new_with_fs())
            .metadata(metadata)
            .identifier(TableIdent::new(
                NamespaceIdent::new("db".to_string()),
                "mv".to_string(),
            ))
            .build()
            .expect("target table");
        let target_entry = Arc::new(
            crate::connector::iceberg::catalog::registry::build_catalog_entry(
                "tgt",
                &[
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    ("iceberg.catalog.warehouse".to_string(), warehouse),
                ],
            )
            .expect("catalog entry"),
        );
        let iceberg_catalog: Arc<dyn iceberg::Catalog> = Arc::new(
            crate::connector::iceberg::catalog::registry::build_hadoop_catalog(&target_entry)
                .expect("build hadoop catalog"),
        );

        let rewrite = crate::mv::refresh::execution_context::tests_support::rewrite_context_for_target_fixture(
            &target_table,
            None,
        );
        crate::mv::refresh::execution_context::tests_support::refresh_context_for_handles(
            rewrite,
            target_entry,
            iceberg_catalog,
            target_table,
        )
    }

    #[test]
    fn execute_query_with_imv_validator_propagates_validator_error() {
        let query = parse_query_for_engine_test("select k, v from ice.db.b");
        let mut catalog = super::PlannerMemoryCatalog::default();
        catalog.create_database("db").expect("create db");
        catalog
            .register(
                "db",
                crate::sql::planner::table::TableDef {
                    name: "b".to_string(),
                    columns: vec![
                        novarocks_catalog::schema::ColumnDef {
                            name: "k".to_string(),
                            data_type: DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        novarocks_catalog::schema::ColumnDef {
                            name: "v".to_string(),
                            data_type: DataType::Int64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: crate::sql::planner::table::ScanSource::IcebergDataFiles {
                        table: crate::connector::iceberg::scan_model::IcebergTableInfo {
                            catalog: "ice".to_string(),
                            namespace: "db".to_string(),
                            table: "b".to_string(),
                            table_uuid: Some("uuid-b".to_string()),
                            current_snapshot_id: Some(22),
                            schema_id: 1,
                            location: "file:///ice/db/b".to_string(),
                            schema: crate::connector::iceberg::scan_model::IcebergSchemaDef {
                                fields: vec![
                                    crate::connector::iceberg::scan_model::IcebergSchemaFieldDef {
                                        field_id: 1,
                                        name: "k".to_string(),
                                        initial_default: None,
                                        write_default: None,
                                        initial_default_json: None,
                                        write_default_json: None,
                                        children: Vec::new(),
                                    },
                                    crate::connector::iceberg::scan_model::IcebergSchemaFieldDef {
                                        field_id: 2,
                                        name: "v".to_string(),
                                        initial_default: None,
                                        write_default: None,
                                        initial_default_json: None,
                                        write_default_json: None,
                                        children: Vec::new(),
                                    },
                                ],
                            },
                            serialized_metadata: None,
                            serialized_metadata_rows: None,
                        },
                        files: Vec::new(),
                        cloud_properties: Default::default(),
                        binding:
                            crate::connector::iceberg::scan_model::IcebergDataFileBinding::CurrentSnapshot,
                    },
                },
            )
            .expect("register base table");
        let connectors = crate::connector::ConnectorRegistry::default();
        let mv_ctx = dummy_mv_refresh_context_for_validator_test();
        let query_execution = super::test_query_execution_service();
        let validator =
            |_outcome: &crate::sql::planner::imv_rewrite::entrypoint::ImvRewriteOutcome| {
                Err("sentinel IMV validator error".to_string())
            };

        let err = super::execute_query_with_options_and_imv_validator(
            &query,
            &catalog,
            &connectors,
            "default",
            0,
            None,
            &query_execution,
            None,
            None,
            Some(&mv_ctx),
            &crate::connector::test_request_context(),
            Some(&validator),
            None,
        )
        .expect_err("validator errors must abort refresh query execution");

        assert_eq!(err, "sentinel IMV validator error");
    }

    #[test]
    fn preexpanded_mv_refresh_query_skips_imv_rewrite() {
        let query = parse_query_for_engine_test("select 1");
        let catalog = super::PlannerMemoryCatalog::default();
        let connectors = crate::connector::ConnectorRegistry::default();
        let mv_ctx = dummy_mv_refresh_context_for_validator_test();
        let query_execution = super::test_query_execution_service();

        let rewrite_err = super::execute_query_with_options(
            &query,
            &catalog,
            &connectors,
            "default",
            0,
            None,
            &query_execution,
            None,
            None,
            Some(&mv_ctx),
        )
        .expect_err("regular MV refresh entrypoint should run IMV rewrite");
        assert!(
            rewrite_err
                .starts_with("imv rewrite: IVM rewrite failed to resolve incremental markers:"),
            "unexpected rewrite error: {rewrite_err}"
        );

        let backend = super::install_all_in_one_loopback_backend_for_test()
            .expect("install loopback backend");
        let result = super::execute_preexpanded_mv_refresh_query_with_options(
            &query,
            &catalog,
            &connectors,
            "default",
            backend.exchange_port,
            None,
            &query_execution,
            None,
            None,
            Some(&mv_ctx),
        )
        .expect("pre-expanded MV refresh query should skip IMV rewrite");
        assert_eq!(result.row_count(), 1);
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
    fn convert_insert_values_accepts_map_and_row_literals() {
        use crate::sql::parser::dialect::StarRocksDialect;

        let statements = sqlparser::parser::Parser::parse_sql(
            &StarRocksDialect,
            "INSERT INTO t VALUES (1, map('key', 5.5), row(100, 'abc'))",
        )
        .expect("parse insert");
        let sqlparser::ast::Statement::Insert(insert) = &statements[0] else {
            panic!("expected insert statement");
        };

        let converted = super::convert_sqlparser_insert_to_custom(insert);
        assert!(
            converted.is_ok(),
            "expected complex literals to convert: {converted:?}"
        );
    }

    #[test]
    fn build_local_insert_batch_supports_array_columns() {
        use crate::sql::parser::ast::Literal;
        use novarocks_catalog::schema::ColumnDef;

        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int32,
                nullable: false,
                write_default: None,
                logical_type: None,
            },
            ColumnDef {
                name: "score_items".to_string(),
                data_type: DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                nullable: true,
                write_default: None,
                logical_type: None,
            },
            ColumnDef {
                name: "tags".to_string(),
                data_type: DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                nullable: true,
                write_default: None,
                logical_type: None,
            },
        ];
        let rows = vec![
            vec![
                Literal::Int(1),
                Literal::Array(vec![Literal::Int(90), Literal::Null, Literal::Int(80)]),
                Literal::Array(vec![
                    Literal::String("a".to_string()),
                    Literal::Null,
                    Literal::String("c".to_string()),
                ]),
            ],
            vec![Literal::Int(2), Literal::Null, Literal::Array(vec![])],
        ];

        let batch = super::build_local_insert_batch(&columns, &rows).expect("build local batch");
        let scores = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("score_items list array");
        let tags = batch
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("tags list array");

        assert_eq!(scores.len(), 2);
        assert_eq!(scores.value(0).len(), 3);
        assert!(scores.is_null(1));

        assert_eq!(tags.len(), 2);
        assert_eq!(tags.value(0).len(), 3);
        assert_eq!(tags.value(1).len(), 0);
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
    fn build_local_insert_batch_supports_largeint_columns() {
        use crate::sql::parser::ast::Literal;
        use novarocks_catalog::schema::ColumnDef;
        use novarocks_types::largeint;

        let columns = vec![ColumnDef {
            name: "v".to_string(),
            data_type: DataType::FixedSizeBinary(largeint::LARGEINT_BYTE_WIDTH),
            nullable: true,
            write_default: None,
            logical_type: None,
        }];
        let rows = vec![
            vec![Literal::String(
                "-170141183460469231731687303715884105728".to_string(),
            )],
            vec![Literal::String("0".to_string())],
            vec![Literal::Null],
            vec![Literal::String(
                "170141183460469231731687303715884105727".to_string(),
            )],
        ];

        let batch = super::build_local_insert_batch(&columns, &rows).expect("build local batch");
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("largeint array");

        assert_eq!(
            largeint::value_at(values, 0).expect("decode min"),
            i128::MIN
        );
        assert_eq!(largeint::value_at(values, 1).expect("decode zero"), 0);
        assert!(values.is_null(2));
        assert_eq!(
            largeint::value_at(values, 3).expect("decode max"),
            i128::MAX
        );
    }

    #[test]
    fn build_local_insert_batch_accepts_integral_float_literals_for_bigint_arrays() {
        use crate::sql::parser::ast::Literal;
        use novarocks_catalog::schema::ColumnDef;

        let columns = vec![ColumnDef {
            name: "nums".to_string(),
            data_type: DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            nullable: true,
            write_default: None,
            logical_type: None,
        }];
        let rows = vec![vec![Literal::Array(vec![
            Literal::Float(1.0),
            Literal::Float(2.0),
        ])]];

        let batch = super::build_local_insert_batch(&columns, &rows).expect("build local batch");
        let nums = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("nums list array");
        let values_ref = nums.value(0);
        let values = values_ref
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 values");

        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);
    }

    #[test]
    fn build_local_insert_batch_drops_null_map_keys() {
        use crate::sql::parser::ast::Literal;
        use novarocks_catalog::schema::ColumnDef;

        // Arrow's Map layout requires `entries.key` to be non-nullable; map
        // literals with NULL keys must drop those kv-pairs so that the output
        // array matches the catalog schema.
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Int32, false)),
                    Arc::new(Field::new("value", DataType::Utf8, true)),
                ]
                .into(),
            ),
            false,
        ));
        let columns = vec![ColumnDef {
            name: "m".to_string(),
            data_type: DataType::Map(entries_field, false),
            nullable: true,
            write_default: None,
            logical_type: None,
        }];
        let rows = vec![vec![Literal::Map(vec![
            (Literal::Null, Literal::String("dropped".to_string())),
            (Literal::Int(7), Literal::String("kept".to_string())),
        ])]];

        let batch = super::build_local_insert_batch(&columns, &rows).expect("build local batch");
        let map = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .expect("map array");
        assert_eq!(map.len(), 1);
        assert_eq!(map.value_length(0), 1);
        let entries = map.entries();
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("key array");
        assert_eq!(keys.null_count(), 0);
        assert_eq!(keys.value(0), 7);

        let schema = batch.schema();
        let DataType::Map(entries_field, _) = schema.field(0).data_type() else {
            panic!("expected map field");
        };
        let DataType::Struct(entry_fields) = entries_field.data_type() else {
            panic!("expected struct entries");
        };
        assert!(!entry_fields[0].is_nullable());
    }

    #[test]
    fn cast_batch_to_schema_relaxes_map_key_nullability() {
        use crate::sql::parser::ast::Literal;
        use novarocks_catalog::schema::ColumnDef;

        let source_entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Int32, false)),
                    Arc::new(Field::new("value", DataType::Utf8, true)),
                ]
                .into(),
            ),
            false,
        ));
        let source_columns = vec![ColumnDef {
            name: "m".to_string(),
            data_type: DataType::Map(source_entries_field, false),
            nullable: true,
            write_default: None,
            logical_type: None,
        }];
        let rows = vec![vec![Literal::Map(vec![(
            Literal::Int(1),
            Literal::String("v".to_string()),
        )])]];
        let source_batch =
            super::build_local_insert_batch(&source_columns, &rows).expect("build source batch");

        let target_entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Int32, true)),
                    Arc::new(Field::new("value", DataType::Utf8, true)),
                ]
                .into(),
            ),
            false,
        ));
        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "m",
            DataType::Map(target_entries_field, false),
            true,
        )]));

        let casted =
            crate::formats::parquet::local_io::cast_batch_to_schema(&source_batch, &target_schema)
                .expect("cast batch");
        let casted_schema = casted.schema();
        let DataType::Map(entries_field, _) = casted_schema.field(0).data_type() else {
            panic!("expected MAP column");
        };
        let DataType::Struct(entry_fields) = entries_field.data_type() else {
            panic!("expected MAP entries to be STRUCT");
        };

        assert!(
            entry_fields[0].is_nullable(),
            "expected casted map key field to become nullable"
        );
    }

    #[test]
    fn local_parquet_round_trip_drops_null_map_keys() {
        use crate::sql::parser::ast::Literal;
        use novarocks_catalog::schema::ColumnDef;

        // Arrow's Map layout requires non-null keys; when a literal carries a
        // NULL key, the insert path drops the kv-pair and the resulting
        // parquet round trip must preserve that (no null keys).
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Int32, false)),
                    Arc::new(Field::new("value", DataType::Utf8, true)),
                ]
                .into(),
            ),
            false,
        ));
        let columns = vec![ColumnDef {
            name: "m".to_string(),
            data_type: DataType::Map(entries_field, false),
            nullable: true,
            write_default: None,
            logical_type: None,
        }];
        let rows = vec![vec![Literal::Map(vec![
            (Literal::Null, Literal::String("dropped".to_string())),
            (Literal::Int(5), Literal::String("kept".to_string())),
        ])]];
        let batch = super::build_local_insert_batch(&columns, &rows).expect("build local batch");
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("map_round_trip.parquet");

        crate::formats::parquet::local_io::write_parquet_to_path(&path, &batch)
            .expect("write local parquet");
        let round_tripped =
            crate::formats::parquet::local_io::read_local_parquet_data(&path, &batch.schema())
                .expect("read local parquet");
        let map = round_tripped
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .expect("map array");
        assert_eq!(map.len(), 1);
        assert_eq!(map.value_length(0), 1);
        let entries = map.entries();
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("key array");
        assert_eq!(keys.null_count(), 0);
        assert_eq!(keys.value(0), 5);

        let round_schema = round_tripped.schema();
        let DataType::Map(entries_field, _) = round_schema.field(0).data_type() else {
            panic!("expected map field");
        };
        let DataType::Struct(entry_fields) = entries_field.data_type() else {
            panic!("expected struct entries");
        };
        assert!(!entry_fields[0].is_nullable());
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
    fn explain_analyze_runs_distributed_plan_and_renders_actuals() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(&session, "(1,10),(2,20),(3,30)", "(1,100),(2,200),(4,400)");

        let result = session
            .execute_in_context(
                "EXPLAIN ANALYZE \
                 SELECT count(*) \
                 FROM t1 JOIN t2 ON t1.k = t2.k",
                Some("ice"),
                "db1",
                None,
            )
            .expect("EXPLAIN ANALYZE must execute and render profile actuals");

        let StatementResult::Query(result) = result else {
            panic!("EXPLAIN ANALYZE must return rows");
        };
        let text = string_column(&result, 0).join("\n");
        assert!(text.starts_with("Planning: "), "{text}");
        assert!(text.contains(" / Execution: "), "{text}");
        assert!(text.contains(" / Rows: 1"), "{text}");
        assert!(text.contains("Profile: fragments="), "{text}");
        assert!(text.contains("operator_active="), "{text}");
        assert!(text.contains("source_wait="), "{text}");
        assert!(text.contains("sink_wait="), "{text}");
        assert!(text.contains("exchange_wait="), "{text}");
        assert!(!text.contains("RuntimeFilterDormancy:"), "{text}");
        let apply = text
            .lines()
            .find(|line| line.starts_with("RuntimeFilterApply: input_rows="))
            .expect("active native runtime filter must render apply evidence");
        let (input, output) = apply
            .strip_prefix("RuntimeFilterApply: input_rows=")
            .and_then(|rest| rest.split_once(" output_rows="))
            .expect("stable runtime-filter apply header");
        let input = input.parse::<i64>().expect("numeric input rows");
        let output = output.parse::<i64>().expect("numeric output rows");
        assert!(input > output, "{apply}");
        assert!(output >= 0, "{apply}");
        assert!(text.contains("PLAN FRAGMENT 0"), "{text}");
        assert!(text.contains("stats={rows="), "{text}");
        assert!(text.contains("act={rows="), "{text}");
        // W0': join node act trailer includes phase timing from probe/build sides.
        assert!(text.contains("search="), "{text}");
        assert!(text.contains("out_build="), "{text}");
        assert!(text.contains("out_probe="), "{text}");
        assert!(text.contains("build_ht="), "{text}");
        // W0'b: per-fragment active/blocked Profile line under each PLAN FRAGMENT header.
        assert!(text.contains("Profile: active="), "{text}");
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

        let insert = session
            .execute_in_database(
                "insert into ice.db1.tbl values (1, 'a'), (2, 'b')",
                "default",
            )
            .expect("insert iceberg rows");
        assert!(matches!(insert, StatementResult::Ok));

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

        let insert = session
            .execute_in_database(
                "insert into ice.db1.tbl values (1, 'a'), (2, 'b')",
                "default",
            )
            .expect("insert iceberg rows");
        assert!(matches!(insert, StatementResult::Ok));

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
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
            .expect("insert iceberg row");
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
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
            .expect("insert iceberg row");
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

        let insert = session
            .execute_in_database(
                "insert into ice.db1.nums values (1, 101), (2, 102)",
                "default",
            )
            .expect("insert iceberg rows");
        assert!(matches!(insert, StatementResult::Ok));

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

            assert!(matches!(
                session
                    .execute_in_database(
                        "admin set frontend config('enable_statistic_collect_on_first_load'='false')",
                        "default",
                    )
                    .expect("disable first-load stats for iceberg insert"),
                StatementResult::Ok
            ));

            let insert = session
                .execute_in_database(
                    "insert into ice.db1.tbl values (1, 'a'), (2, 'b')",
                    "default",
                )
                .expect("insert iceberg rows");
            assert!(matches!(insert, StatementResult::Ok));
        }

        let restored = StandaloneNovaRocks::open(
            StandaloneOptions {
                config_path: Some(config_path),
            },
            test_open_services(),
        )
        .expect("reopen engine");
        let entry = {
            let registry = restored
                .inner
                .iceberg_catalogs
                .read()
                .expect("iceberg registry read lock");
            registry.get("ice").expect("load restored iceberg catalog")
        };
        let loaded = crate::connector::load_iceberg_table(&entry, "db1", "tbl")
            .expect("load restored table");
        assert!(
            loaded.table.metadata().current_snapshot().is_some(),
            "restored iceberg table should retain inserted snapshot"
        );
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

            let provider = engine
                .inner
                .metadata_provider
                .as_ref()
                .expect("metadata provider");
            let read = provider.begin_read().expect("open read txn");
            let starrocks_table_snapshot = engine
                .inner
                .starrocks_table_repo
                .load_snapshot(read.as_ref())
                .expect("load StarRocks snapshot");
            assert!(
                !starrocks_table_snapshot
                    .tables
                    .iter()
                    .any(|table| table.name == "mv_orders")
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
                || err.contains("StarRocks table config is missing")
                || err.contains("sqlite metadata store")
                || err.contains("materialized view")
                || err.contains("StarRocks table"),
            "unexpected dispatch error: {err}"
        );
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
        let state = Arc::new(StandaloneState::default());
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
        let state = Arc::new(StandaloneState::default());
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
            test_open_services(),
        )
        .expect("open engine")
    }

    fn open_iceberg_session_with_table(
        warehouse: &TempDir,
        format_version: &str,
    ) -> (StandaloneNovaRocks, StandaloneSession) {
        let engine = open_test_engine_with_metadata(warehouse);
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
    fn mysql_request_cancellation_reaches_insert_metadata_lookup() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        let cancellation = Arc::new(AtomicBool::new(true));
        let connector_context =
            crate::connector::connector_request_context(None, cancellation).expect("context");

        let error = session
            .execute_in_context_with_connector_context(
                "insert into ice.db1.t values (1, 'cancelled')",
                None,
                "default",
                None,
                connector_context,
            )
            .expect_err("cancelled MySQL request must abort INSERT metadata lookup");

        assert!(
            error.contains("cancel"),
            "unexpected cancellation error: {error}"
        );
    }

    #[test]
    fn iceberg_catalog_lifecycle_registers_and_unregisters_its_connector_instance() {
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
        let instance_id = novarocks_spi::connector::ConnectorInstanceId::parse("ice_one")
            .expect("connector instance ID");
        let state = engine.state_for_test();
        {
            let connectors = state
                .connectors
                .read()
                .expect("connector registry read lock");
            connectors
                .connector_instance(&instance_id)
                .expect("Iceberg connector instance must be registered");
        }

        session
            .execute_in_database("drop catalog Ice_One", "default")
            .expect("drop catalog");
        let connectors = state
            .connectors
            .read()
            .expect("connector registry read lock");
        assert!(connectors.connector_instance(&instance_id).is_err());
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
        use iceberg::Catalog;

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
        let namespace = iceberg::NamespaceIdent::new("db1".to_string());
        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(iceberg::spec::NestedField::required(
                    1,
                    "id",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Int),
                )),
                Arc::new(iceberg::spec::NestedField::required(
                    2,
                    "v",
                    iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String),
                )),
            ])
            .build()
            .expect("build schema");
        let mut props: Vec<(String, String)> =
            vec![("write.row-lineage".to_string(), "true".to_string())];
        for (k, v) in extra_props {
            props.push(((*k).to_string(), (*v).to_string()));
        }
        let table_creation = iceberg::TableCreation::builder()
            .name("t".to_string())
            .schema(schema)
            .format_version(iceberg::spec::FormatVersion::V3)
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
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
            .expect("seed");

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
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
            .expect("seed");

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
    fn iceberg_insert_select_drives_a_new_snapshot() {
        // INSERT INTO ... SELECT writes data files + a new snapshot. The
        // standalone iceberg backend's local-FS path historically only
        // registered the *first* data file for local-FS tables (see
        // `connector/iceberg/catalog/backend.rs`'s data-files branch), so
        // a SELECT-side verification would only see the seed file even
        // though the new snapshot includes both. This is a separate
        // NovaRocks-side gap tracked outside Phase 1; here we verify the
        // iceberg layer's snapshot chain advanced as expected via the
        // registry.
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "3");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b')", "default")
            .expect("seed");
        let snap_before = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        session
            .execute_in_database(
                "insert into ice.db1.t select id, upper(v) from ice.db1.t where id <= 2",
                "default",
            )
            .expect("insert select");
        let snap_after = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        assert_ne!(
            snap_before, snap_after,
            "INSERT INTO ... SELECT must advance the iceberg snapshot id"
        );
        assert_iceberg_operation_finalized(
            &engine,
            2,
            crate::meta::repository::iceberg_operation::IcebergOperationKind::InsertAppend,
            snap_after,
        );
    }

    #[test]
    fn iceberg_branch_writes_record_branch_head_base_snapshot() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "3");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'main-1')", "default")
            .expect("seed main");
        let branch_base =
            current_iceberg_snapshot_id(&engine, "ice", "db1", "t").expect("seed snapshot");
        session
            .execute_in_database("alter table ice.db1.t create branch dev", "default")
            .expect("create branch");
        session
            .execute_in_database("insert into ice.db1.t values (2, 'main-2')", "default")
            .expect("advance main after branch creation");
        let main_after_branch = current_iceberg_snapshot_id(&engine, "ice", "db1", "t")
            .expect("main advanced snapshot");
        assert_ne!(
            branch_base, main_after_branch,
            "main must advance after the branch was created"
        );

        session
            .execute_in_database(
                "insert into ice.db1.t.branch_dev values (3, 'dev-3')",
                "default",
            )
            .expect("branch insert");
        let dev_after = iceberg_ref_snapshot_id(&engine, "ice", "db1", "t", "dev")
            .expect("dev branch snapshot");
        assert_ne!(
            branch_base, dev_after,
            "branch insert must advance the branch head"
        );

        let operation = load_iceberg_operation(&engine, 3);
        assert_eq!(
            operation.operation_kind,
            crate::meta::repository::iceberg_operation::IcebergOperationKind::InsertAppend
        );
        assert_eq!(operation.target.ref_name.as_deref(), Some("dev"));
        assert_eq!(
            operation.base_snapshot_id,
            Some(branch_base),
            "branch write operation must record the branch head before commit, not main"
        );
        assert_eq!(
            operation
                .commit_outcome
                .as_ref()
                .map(|outcome| outcome.snapshot_id),
            Some(dev_after)
        );
        assert_eq!(
            current_iceberg_snapshot_id(&engine, "ice", "db1", "t"),
            Some(main_after_branch),
            "branch insert must not advance main"
        );

        session
            .execute_in_database(
                "insert overwrite ice.db1.t.branch_dev values (4, 'dev-4')",
                "default",
            )
            .expect("branch overwrite");
        let dev_after_overwrite = iceberg_ref_snapshot_id(&engine, "ice", "db1", "t", "dev")
            .expect("dev branch snapshot after overwrite");
        assert_ne!(
            dev_after, dev_after_overwrite,
            "branch overwrite must advance the branch head"
        );

        let operation = load_iceberg_operation(&engine, 4);
        assert_eq!(
            operation.operation_kind,
            crate::meta::repository::iceberg_operation::IcebergOperationKind::InsertOverwrite
        );
        assert_eq!(operation.target.ref_name.as_deref(), Some("dev"));
        assert_eq!(
            operation.base_snapshot_id,
            Some(dev_after),
            "branch overwrite operation must record the branch head before commit, not main"
        );
        assert_eq!(
            operation
                .commit_outcome
                .as_ref()
                .map(|outcome| outcome.snapshot_id),
            Some(dev_after_overwrite)
        );
        assert_eq!(
            current_iceberg_snapshot_id(&engine, "ice", "db1", "t"),
            Some(main_after_branch),
            "branch overwrite must not advance main"
        );
    }

    // -----------------------------------------------------------------------
    // ANALYZE TABLE / ANALYZE FULL TABLE against iceberg external-catalog
    // tables. Ordinary SELECT resolves external tables through
    // CatalogMgrProvider and no longer pre-registers them in the local catalog,
    // so the statistics path must materialize the table itself before reading
    // local metadata (regression: ANALYZE failed with "unknown table").
    // -----------------------------------------------------------------------

    fn iceberg_column_stat_row_count(session: &StandaloneSession, table_name: &str) -> usize {
        let sql = format!(
            "select column_name from _statistics_.column_statistics where table_name = '{table_name}'"
        );
        session
            .query(&sql)
            .expect("query column statistics")
            .row_count()
    }

    #[test]
    fn analyze_table_resolves_iceberg_table_via_session_catalog() {
        // Faithful reproduction of the SQL-suite scenario: `SET catalog <ice>`
        // (current_catalog set) followed by a 2-part `db.table` ANALYZE before
        // any SELECT against the table.
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "3");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b')", "default")
            .expect("seed");

        // Sanity: the table is NOT in the in-memory catalog yet (never SELECTed).
        assert!(
            !engine.has_local_table("db1", "t"),
            "iceberg table must not be pre-registered before ANALYZE",
        );

        session
            .execute_in_context("analyze table db1.t", Some("ice"), "db1", None)
            .expect("ANALYZE TABLE must resolve iceberg external table via session catalog");

        // The table is now materialized and column statistics were recorded
        // for both columns.
        assert!(engine.has_local_table("db1", "t"));
        assert_eq!(
            iceberg_column_stat_row_count(&session, "db1.t"),
            2,
            "ANALYZE TABLE must record stats for both iceberg columns",
        );
    }

    #[test]
    fn analyze_full_table_resolves_iceberg_table_via_three_part_name() {
        // The 3-part `catalog.db.table` form (current_catalog = None) must also
        // resolve the iceberg table before stats collection.
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "3");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b')", "default")
            .expect("seed");

        assert!(!engine.has_local_table("db1", "t"));

        session
            .execute_in_database("analyze full table ice.db1.t", "default")
            .expect("ANALYZE FULL TABLE must resolve iceberg external table via 3-part name");

        assert!(engine.has_local_table("db1", "t"));
        assert_eq!(iceberg_column_stat_row_count(&session, "db1.t"), 2);
    }

    #[test]
    fn analyze_table_unknown_iceberg_table_errors() {
        // A genuinely missing iceberg table named explicitly by ANALYZE must
        // surface a hard error rather than silently succeeding.
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_iceberg_session_with_table(&warehouse, "3");
        let err = session
            .execute_in_context("analyze table db1.does_not_exist", Some("ice"), "db1", None)
            .expect_err("ANALYZE of a missing iceberg table must fail");
        assert!(
            err.to_ascii_lowercase().contains("does_not_exist")
                || err.to_ascii_lowercase().contains("load iceberg table"),
            "expected a load/resolution error, got: {err}",
        );
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
            session
                .execute_in_database(
                    &format!("insert into ice.db1.{name} values (1), (2), (3)"),
                    "default",
                )
                .expect("seed table");
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
    ) -> Vec<(String, iceberg::spec::Transform)> {
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
            vec![("ts_month".to_string(), iceberg::spec::Transform::Month)]
        );

        session
            .execute_in_database(
                "alter table ice.db1.t_evolved drop partition column month(ts)",
                "default",
            )
            .expect("drop partition column");
        assert_eq!(
            current_iceberg_default_spec_fields(&engine, "ice", "db1", "t_evolved"),
            Vec::<(String, iceberg::spec::Transform)>::new()
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
                iceberg::spec::Transform::Bucket(8)
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
                if manifest_file.content != iceberg::spec::ManifestContentType::Deletes {
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
                            == iceberg::spec::DataContentType::PositionDeletes
                        && data_file.file_format() == iceberg::spec::DataFileFormat::Parquet
                    {
                        return true;
                    }
                }
            }
            false
        })
        .expect("inspect delete manifests")
    }

    #[test]
    fn iceberg_insert_overwrite_replaces_all_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_iceberg_session_with_table(&warehouse, "3");
        session
            .execute_in_database(
                "insert into ice.db1.t values (1, 'a'), (2, 'b'), (3, 'c')",
                "default",
            )
            .expect("seed");
        // INSERT OVERWRITE replaces every row in the table with the SELECT
        // output (Task 13 OverwriteCommit path).
        session
            .execute_in_database(
                "insert overwrite ice.db1.t select id, upper(v) from ice.db1.t where id <= 2",
                "default",
            )
            .expect("overwrite select");
        let mut rows = collect_id_v(&session, "select id, v from ice.db1.t");
        rows.sort_by_key(|(id, _)| *id);
        assert_eq!(
            rows,
            vec![(1, "A".to_string()), (2, "B".to_string())],
            "overwrite must replace ALL rows, not append"
        );
    }

    #[test]
    fn iceberg_delete_where_removes_matching_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        session
            .execute_in_database(
                "insert into ice.db1.t values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
                "default",
            )
            .expect("seed");
        let snap_before = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        session
            .execute_in_database("delete from ice.db1.t where id = 2", "default")
            .expect("delete eq");
        let snap_after = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        assert_ne!(
            snap_before, snap_after,
            "DELETE WHERE id = 2 must advance the iceberg snapshot id"
        );
        assert_iceberg_operation_finalized(
            &engine,
            2,
            crate::meta::repository::iceberg_operation::IcebergOperationKind::RowDelta,
            snap_after,
        );
        // DELETE with IN list still advances the snapshot.
        session
            .execute_in_database("delete from ice.db1.t where id in (1, 4)", "default")
            .expect("delete in list");
        let snap_after2 = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        assert_ne!(
            snap_after, snap_after2,
            "DELETE WHERE id IN (1,4) must advance the iceberg snapshot id again"
        );
    }

    #[test]
    fn iceberg_legacy_delete_still_uses_position_delete_path() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b')", "default")
            .expect("seed");
        session
            .execute_in_database("delete from ice.db1.t where id = 1", "default")
            .expect("legacy delete");
        let snap_after = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        assert!(snap_after.is_some(), "legacy DELETE must still commit");
        assert!(
            current_snapshot_has_position_delete_parquet(&engine, "ice", "db1", "t"),
            "legacy DELETE must commit at least one live Parquet position-delete file"
        );
    }

    #[test]
    fn iceberg_row_lineage_insert_select_advances_next_row_id() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b')", "default")
            .expect("seed");
        let (before_next_row_id, _) = current_iceberg_row_lineage(&engine, "ice", "db1", "t");
        session
            .execute_in_database(
                "insert into ice.db1.t select id, upper(v) from ice.db1.t where id <= 2",
                "default",
            )
            .expect("row-lineage insert select");
        let (after_next_row_id, row_range) =
            current_iceberg_row_lineage(&engine, "ice", "db1", "t");
        assert_eq!(
            after_next_row_id,
            before_next_row_id + 2,
            "row-lineage INSERT SELECT must advance next-row-id by written rows"
        );
        assert_eq!(
            row_range,
            Some((before_next_row_id, 2)),
            "row-lineage INSERT SELECT snapshot must record its row range"
        );
    }

    #[test]
    fn iceberg_row_lineage_optimize_does_not_advance_next_row_id() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);

        // Seed 3 INSERTs so OPTIMIZE has multiple input data files to coalesce.
        for i in 1..=3 {
            session
                .execute_in_database(
                    &format!("insert into ice.db1.t values ({i}, '{i}')"),
                    "default",
                )
                .expect("seed");
        }
        let (next_row_id_before, _) = current_iceberg_row_lineage(&engine, "ice", "db1", "t");

        // Locate the current snapshot id so the rewrite target matches the
        // table's live state — that's the precondition for
        // `validate_base_snapshot` inside the rewrite executor.
        let base_snapshot_id = {
            let registry = engine.inner.iceberg_catalogs.read().expect("registry");
            let entry = registry.get("ice").expect("entry");
            entry.invalidate_table_cache("db1", "t");
            let loaded = crate::connector::iceberg::catalog::load_table(&entry, "db1", "t")
                .expect("load table");
            loaded
                .table
                .metadata()
                .current_snapshot()
                .expect("table must have a current snapshot after INSERTs")
                .snapshot_id()
        };

        let target = crate::connector::iceberg::compact::WholeTableRewriteTarget {
            catalog: "ice".to_string(),
            namespace: "db1".to_string(),
            table: "t".to_string(),
            base_snapshot_id,
            job_id: None,
        };
        let outcome = crate::connector::iceberg::compact::execute_whole_table_rewrite_for_target(
            &engine.inner,
            &target,
        )
        .expect("rewrite whole table");
        assert!(
            outcome.target_snapshot_id.is_some(),
            "OPTIMIZE on a non-empty row-lineage table must commit a Replace snapshot"
        );

        let (next_row_id_after, row_range_after) =
            current_iceberg_row_lineage(&engine, "ice", "db1", "t");
        assert_eq!(
            next_row_id_after, next_row_id_before,
            "OPTIMIZE must not advance next_row_id on row-lineage tables (preserve mode)"
        );
        // V3 snapshots require first-row-id to be non-null per Iceberg spec
        // (iceberg-rs vendor rejects null on add_snapshot). Preserve-mode
        // expresses "no new rows allocated" by stamping `(next_row_id, 0)`
        // — same first_row_id as before, zero added rows so next_row_id
        // does not advance. Future INSERTs continue from the same id.
        assert_eq!(
            row_range_after,
            Some((next_row_id_before, 0)),
            "Preserve-mode OPTIMIZE must record (next_row_id, 0) row_range"
        );
        let first_row_ids = current_live_data_file_first_row_ids(&engine, "ice", "db1", "t");
        assert!(
            !first_row_ids.is_empty(),
            "OPTIMIZE must leave at least one live data file"
        );
        assert!(
            first_row_ids.iter().all(Option::is_some),
            "OPTIMIZE preserve-mode data files must keep effective first_row_id values: {first_row_ids:?}"
        );
    }

    #[test]
    fn iceberg_row_lineage_optimize_preserves_row_identity() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
        session
            .execute_in_database(
                "insert into ice.db1.t values (1, '10'), (2, '20')",
                "default",
            )
            .expect("seed first files");
        session
            .execute_in_database(
                "insert into ice.db1.t values (3, '30'), (4, '40')",
                "default",
            )
            .expect("seed second files");
        session
            .execute_in_database(
                "insert into ice.db1.t values (5, '50'), (6, '60')",
                "default",
            )
            .expect("seed third files");
        session
            .execute_in_database("update ice.db1.t set v = '99' where id = 2", "default")
            .expect("update before optimize");
        session
            .execute_in_database("delete from ice.db1.t where id = 4", "default")
            .expect("delete before optimize");

        let before = collect_id_rowid_seq(
            &session,
            "select id, _row_id, _last_updated_sequence_number from ice.db1.t order by id",
        );
        assert_eq!(before.len(), 5, "test setup should leave five live rows");

        let base_snapshot_id =
            current_iceberg_snapshot_id(&engine, "ice", "db1", "t").expect("base snapshot");
        let target = crate::connector::iceberg::compact::WholeTableRewriteTarget {
            catalog: "ice".to_string(),
            namespace: "db1".to_string(),
            table: "t".to_string(),
            base_snapshot_id,
            job_id: None,
        };
        let outcome = crate::connector::iceberg::compact::execute_whole_table_rewrite_for_target(
            &engine.inner,
            &target,
        )
        .expect("rewrite whole table");
        assert!(
            outcome.target_snapshot_id.is_some(),
            "OPTIMIZE on a non-empty row-lineage table must commit"
        );

        let after = collect_id_rowid_seq(
            &session,
            "select id, _row_id, _last_updated_sequence_number from ice.db1.t order by id",
        );
        assert_eq!(
            after, before,
            "OPTIMIZE must preserve row identity and last-updated sequence for live rows"
        );
    }

    #[test]
    fn iceberg_row_lineage_overwrite_writes_row_range() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
        session
            .execute_in_database(
                "insert into ice.db1.t values (1, 'a'), (2, 'b'), (3, 'c')",
                "default",
            )
            .expect("seed");
        let (before_next_row_id, _) = current_iceberg_row_lineage(&engine, "ice", "db1", "t");
        session
            .execute_in_database(
                "insert overwrite ice.db1.t select id, upper(v) from ice.db1.t where id <= 2",
                "default",
            )
            .expect("row-lineage overwrite");
        let (after_next_row_id, row_range) =
            current_iceberg_row_lineage(&engine, "ice", "db1", "t");
        assert_eq!(
            after_next_row_id,
            before_next_row_id + 2,
            "row-lineage OVERWRITE must advance next-row-id by added rows"
        );
        assert_eq!(
            row_range,
            Some((before_next_row_id, 2)),
            "row-lineage OVERWRITE snapshot must record its row range"
        );
    }

    #[test]
    fn iceberg_delete_no_match_is_a_noop() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
            .expect("seed");
        // No row matches → must succeed without committing a delete snapshot.
        session
            .execute_in_database("delete from ice.db1.t where id = 999", "default")
            .expect("delete no-match");
        let rows = collect_id_v(&session, "select id, v from ice.db1.t");
        assert_eq!(rows, vec![(1, "a".to_string())]);
    }

    #[test]
    fn iceberg_delete_without_where_is_rejected() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
            .expect("seed");
        let err = session
            .execute_in_database("delete from ice.db1.t", "default")
            .expect_err("delete without WHERE must be rejected");
        assert!(
            err.contains("WHERE") || err.contains("INSERT OVERWRITE"),
            "expected WHERE-required error, got {err}"
        );
    }

    #[test]
    fn iceberg_delete_unsupported_predicate_is_rejected() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
            .expect("seed");
        // LIKE is not in the Phase 1 predicate translator's supported set.
        let err = session
            .execute_in_database("delete from ice.db1.t where v like 'a%'", "default")
            .expect_err("LIKE is not supported in phase 1 DELETE WHERE");
        assert!(
            err.contains("phase 1 DELETE WHERE") || err.contains("Like"),
            "expected unsupported-predicate error, got {err}"
        );
    }

    #[test]
    fn iceberg_row_lineage_delete_writes_puffin_dv_and_merges_second_delete() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
        session
            .execute_in_database(
                "insert into ice.db1.t values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
                "default",
            )
            .expect("seed");
        session
            .execute_in_database("delete from ice.db1.t where id = 2", "default")
            .expect("first row-lineage delete");
        session
            .execute_in_database("delete from ice.db1.t where id = 3", "default")
            .expect("second row-lineage delete");

        let registry = engine.inner.iceberg_catalogs.read().expect("registry");
        let entry = registry.get("ice").expect("entry");
        entry.invalidate_table_cache("db1", "t");
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, "db1", "t").expect("load");
        let table = loaded.table;
        let metadata = table.metadata();
        let file_io = table.file_io().clone();
        let (live_dv_count, live_dv_cardinality, live_dv_format_is_puffin) =
            crate::connector::iceberg::catalog::registry::block_on_iceberg(async {
                let snapshot = metadata.current_snapshot().expect("current snapshot");
                let manifests = snapshot
                    .load_manifest_list(&file_io, metadata)
                    .await
                    .expect("manifest list");
                let mut dv_count = 0u64;
                let mut total_cardinality = 0u64;
                let mut all_puffin = true;
                for mf in manifests.entries() {
                    if mf.content != iceberg::spec::ManifestContentType::Deletes {
                        continue;
                    }
                    let manifest = mf.load_manifest(&file_io).await.expect("delete manifest");
                    for entry in manifest.entries() {
                        if !entry.is_alive() {
                            continue;
                        }
                        let data_file = entry.data_file();
                        if data_file.content_type()
                            != iceberg::spec::DataContentType::PositionDeletes
                        {
                            continue;
                        }
                        if data_file.file_format() != iceberg::spec::DataFileFormat::Puffin {
                            all_puffin = false;
                            continue;
                        }
                        assert!(
                            data_file.referenced_data_file().is_some(),
                            "Puffin DV must record referenced_data_file"
                        );
                        assert!(
                            data_file.content_offset().is_some(),
                            "Puffin DV must record content_offset"
                        );
                        assert!(
                            data_file.content_size_in_bytes().is_some(),
                            "Puffin DV must record content_size_in_bytes"
                        );
                        dv_count += 1;
                        total_cardinality += data_file.record_count();
                    }
                }
                (dv_count, total_cardinality, all_puffin)
            })
            .expect("inspect manifests");

        assert!(
            live_dv_format_is_puffin,
            "row-lineage DELETE must not commit any non-Puffin position-delete files"
        );
        assert_eq!(
            live_dv_count, 1,
            "two DELETEs against the same data file must merge into one live Puffin DV (count={live_dv_count})"
        );
        assert_eq!(
            live_dv_cardinality, 2,
            "merged DV must record both deleted rows (got {live_dv_cardinality})"
        );
        let snap_after = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        assert_iceberg_operation_finalized(
            &engine,
            3,
            crate::meta::repository::iceberg_operation::IcebergOperationKind::RowDelta,
            snap_after,
        );
    }

    #[test]
    fn iceberg_add_equality_delete_drives_operation_lifecycle() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_iceberg_session_with_table(&warehouse, "2");
        session
            .execute_in_database(
                "insert into ice.db1.t values (1, 'a'), (2, 'b'), (3, 'b')",
                "default",
            )
            .expect("seed");
        session
            .execute_in_database(
                "alter table ice.db1.t add equality delete (v) values ('b')",
                "default",
            )
            .expect("add equality delete");
        let snap_after = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        assert_iceberg_operation_finalized(
            &engine,
            2,
            crate::meta::repository::iceberg_operation::IcebergOperationKind::RowDelta,
            snap_after,
        );
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

    #[test]
    fn iceberg_v3_cow_update_preserves_row_id() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b')", "default")
            .expect("insert");
        let previous_snapshot_id =
            current_iceberg_snapshot_id(&engine, "ice", "db1", "t").expect("previous snapshot");
        let before = collect_id_rowid_seq(
            &session,
            "select id, _row_id, _last_updated_sequence_number from ice.db1.t order by id",
        );
        session
            .execute_in_database(
                "update ice.db1.t as t set v = 'bb' where t.id = 2",
                "default",
            )
            .expect("update");
        let snap_after = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        assert_iceberg_operation_finalized(
            &engine,
            2,
            crate::meta::repository::iceberg_operation::IcebergOperationKind::RowDelta,
            snap_after,
        );
        let after = collect_id_rowid_seq(
            &session,
            "select id, _row_id, _last_updated_sequence_number from ice.db1.t order by id",
        );
        assert_eq!(before[0].1, after[0].1);
        assert_eq!(before[1].1, after[1].1);
        assert_ne!(
            before[1].2, after[1].2,
            "updated row sequence should advance"
        );

        let registry = engine.inner.iceberg_catalogs.read().expect("registry");
        let entry = registry.get("ice").expect("entry");
        entry.invalidate_table_cache("db1", "t");
        let loaded =
            crate::connector::iceberg::catalog::load_table(&entry, "db1", "t").expect("load");
        let current = loaded
            .table
            .metadata()
            .current_snapshot()
            .expect("current snapshot");
        let summary_props = &current.summary().additional_properties;
        assert!(
            !summary_props.contains_key("novarocks.row-level-op"),
            "COW UPDATE must not publish NovaRocks private row-level markers"
        );
        assert!(
            !summary_props.contains_key("novarocks.update.mode"),
            "COW UPDATE must not publish NovaRocks private update-mode markers"
        );
        assert!(
            summary_props.keys().all(|key| !key.contains("sidecar")),
            "COW UPDATE must not publish private sidecar metadata"
        );
        let change_batch = crate::connector::iceberg::changes::plan_changes(
            &loaded.table,
            previous_snapshot_id,
            None,
            &[],
        )
        .expect("plan COW update from standard manifest diff");
        assert!(!change_batch.inserts.is_empty());
        assert!(!change_batch.deleted_data_files.is_empty());
    }

    #[test]
    fn iceberg_v3_mor_update_preserves_row_id() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_row_lineage_iceberg_session_with_table_extra_props(
            &warehouse,
            &[("novarocks.update.mode", "merge-on-read")],
        );
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b')", "default")
            .expect("insert");
        let before = collect_id_rowid_seq(
            &session,
            "select id, _row_id, _last_updated_sequence_number from ice.db1.t order by id",
        );
        session
            .execute_in_database(
                "update ice.db1.t as t set v = 'aa' where t.id = 1",
                "default",
            )
            .expect("mor update");
        let snap_after = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        assert_iceberg_operation_finalized(
            &engine,
            2,
            crate::meta::repository::iceberg_operation::IcebergOperationKind::RowDelta,
            snap_after,
        );
        let after = collect_id_rowid_seq(
            &session,
            "select id, _row_id, _last_updated_sequence_number from ice.db1.t order by id",
        );
        assert_eq!(after.len(), 2, "MOR UPDATE must not duplicate rows");
        assert_eq!(before[0].1, after[0].1, "_row_id of updated row preserved");
        assert_eq!(
            before[1].1, after[1].1,
            "_row_id of unchanged row preserved"
        );
        assert_ne!(
            before[0].2, after[0].2,
            "updated row sequence number should advance"
        );
        assert_eq!(
            before[1].2, after[1].2,
            "unchanged row sequence number should be stable"
        );
        let v_after = collect_id_v(&session, "select id, v from ice.db1.t order by id");
        assert_eq!(
            v_after,
            vec![(1, "aa".to_string()), (2, "b".to_string())],
            "MOR UPDATE applied new value exactly once"
        );
    }

    #[test]
    fn iceberg_v3_update_from_source_table() {
        // Use a second iceberg table (in the same catalog/namespace) as the
        // source so the test does not depend on StarRocks table configuration.
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
        session
            .execute_in_database(
                r#"create table ice.db1.src (id int, new_v string) tblproperties("format-version"="3","write.row-lineage"="true")"#,
                "default",
            )
            .expect("create source iceberg table");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b')", "default")
            .expect("insert target");
        session
            .execute_in_database("insert into ice.db1.src values (2, 'bb')", "default")
            .expect("insert source");
        session
            .execute_in_database(
                "update ice.db1.t as t set v = s.new_v from ice.db1.src as s where t.id = s.id",
                "default",
            )
            .expect("update from source");
        let rows = collect_id_v(&session, "select id, v from ice.db1.t order by id");
        assert_eq!(rows, vec![(1, "a".to_string()), (2, "bb".to_string())]);
    }

    #[test]
    fn iceberg_v3_merge_upsert_drives_operation_lifecycle() {
        let warehouse = TempDir::new().expect("warehouse");
        let (engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
        session
            .execute_in_database(
                r#"create table ice.db1.src (id int, new_v string) tblproperties("format-version"="3","write.row-lineage"="true")"#,
                "default",
            )
            .expect("create source iceberg table");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a'), (2, 'b')", "default")
            .expect("insert target");
        session
            .execute_in_database(
                "insert into ice.db1.src values (2, 'bb'), (3, 'c')",
                "default",
            )
            .expect("insert source");
        session
            .execute_in_database(
                "merge into ice.db1.t as t using ice.db1.src as s on t.id = s.id \
                 when matched then update set v = s.new_v \
                 when not matched then insert (id, v) values (s.id, s.new_v)",
                "default",
            )
            .expect("merge upsert");
        let rows = collect_id_v(&session, "select id, v from ice.db1.t order by id");
        assert_eq!(
            rows,
            vec![
                (1, "a".to_string()),
                (2, "bb".to_string()),
                (3, "c".to_string())
            ]
        );
        // Phase 3 (commit 6e21eab0) folds all MERGE branches into one
        // collector + one commit, so an upsert MERGE
        // (WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT) now produces a
        // SINGLE iceberg operation: one folded RowDelta commit. Pre-fold it
        // produced two operations — a separate InsertAppend (not-matched
        // INSERT FastAppend) plus a RowDelta (matched UPDATE).
        //
        // Here the two preceding inserts take operation ids #1 and #2
        // (InsertAppend each), so the folded MERGE occupies the single
        // operation id #3 — the slot the not-matched INSERT branch used to
        // take — and no operation #4 exists.
        let snap_after = current_iceberg_snapshot_id(&engine, "ice", "db1", "t");
        assert_iceberg_operation_finalized(
            &engine,
            3,
            crate::meta::repository::iceberg_operation::IcebergOperationKind::RowDelta,
            snap_after,
        );
        // Prove the fold: the MERGE committed exactly one operation, so the
        // pre-fold second operation (#4) must not exist.
        assert_iceberg_operation_absent(&engine, 4);
    }

    #[test]
    fn iceberg_v3_mor_update_from_rejects_duplicate_source_match_with_keyed_assert() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_row_lineage_iceberg_session_with_table_extra_props(
            &warehouse,
            &[("novarocks.update.mode", "merge-on-read")],
        );
        session
            .execute_in_database(
                r#"create table ice.db1.src (id int, new_v string) tblproperties("format-version"="3","write.row-lineage"="true")"#,
                "default",
            )
            .expect("create source iceberg table");
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
            .expect("insert target");
        session
            .execute_in_database(
                "insert into ice.db1.src values (1, 'x'), (1, 'y')",
                "default",
            )
            .expect("insert source");
        let err = session
            .execute_in_database(
                "update ice.db1.t as t set v = s.new_v from ice.db1.src as s where t.id = s.id",
                "default",
            )
            .expect_err("duplicate source rows must fail");
        assert!(
            err.contains("MOR UPDATE matched target row: duplicate _row_id="),
            "expected MOR keyed assert duplicate _row_id error, got: {err}"
        );
        assert!(
            !err.contains("more than once"),
            "MOR duplicate check must not use host-side validation: {err}"
        );
    }

    #[test]
    fn iceberg_v3_cow_update_multiple_files_preserves_row_id() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_row_lineage_iceberg_session_with_table(&warehouse);
        session
            .execute_in_database("insert into ice.db1.t values (1, 'a')", "default")
            .expect("insert first file");
        session
            .execute_in_database("insert into ice.db1.t values (2, 'b')", "default")
            .expect("insert second file");
        let before = collect_id_rowid_seq(
            &session,
            "select id, _row_id, _last_updated_sequence_number from ice.db1.t order by id",
        );
        session
            .execute_in_database(
                "update ice.db1.t set v = 'updated' where id in (1, 2)",
                "default",
            )
            .expect("update two files");
        let after = collect_id_rowid_seq(
            &session,
            "select id, _row_id, _last_updated_sequence_number from ice.db1.t order by id",
        );
        assert_eq!(before[0].1, after[0].1);
        assert_eq!(before[1].1, after[1].1);
        assert_ne!(before[0].2, after[0].2);
        assert_ne!(before[1].2, after[1].2);
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
        use iceberg::Catalog;
        use iceberg::spec::{NestedField, PrimitiveType, Type};

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
        let namespace = iceberg::NamespaceIdent::new("ns".to_string());
        let schema = iceberg::spec::Schema::builder()
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
        let table_creation = iceberg::TableCreation::builder()
            .name("t".to_string())
            .schema(schema)
            .format_version(iceberg::spec::FormatVersion::V3)
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
    fn select_row_id_and_last_updated_seq_on_v3_row_lineage_table() {
        let warehouse = TempDir::new().expect("warehouse tempdir");
        let (engine, session) = open_v3_row_lineage_session_bigint(&warehouse);

        // Snapshot S1: 3 rows.
        session
            .execute_in_database(
                "insert into ice.ns.t values (1, 'A'), (2, 'B'), (3, 'C')",
                "default",
            )
            .expect("seed S1");
        let (s1_first_row_id, s1_seq) = current_snapshot_lineage_info(&engine, "ice", "ns", "t");

        let pre_rows = collect_id_rowid_seq(
            &session,
            "select id, _row_id, _last_updated_sequence_number from ice.ns.t",
        );
        assert_eq!(pre_rows.len(), 3, "S1 must have 3 rows");
        assert_eq!(
            pre_rows[0],
            (1_i64, s1_first_row_id as i64, s1_seq),
            "row 0 (id=1)"
        );
        assert_eq!(
            pre_rows[1],
            (2_i64, s1_first_row_id as i64 + 1, s1_seq),
            "row 1 (id=2)"
        );
        assert_eq!(
            pre_rows[2],
            (3_i64, s1_first_row_id as i64 + 2, s1_seq),
            "row 2 (id=3)"
        );

        // Snapshot S2: 2 more rows.
        session
            .execute_in_database("insert into ice.ns.t values (4, 'D'), (5, 'E')", "default")
            .expect("seed S2");
        let (s2_first_row_id, s2_seq) = current_snapshot_lineage_info(&engine, "ice", "ns", "t");

        // S2 must be a later sequence number than S1.
        assert!(
            s2_seq > s1_seq,
            "S2 sequence_number ({s2_seq}) must be greater than S1 ({s1_seq})"
        );
        // S2 first_row_id must follow the 3 rows from S1.
        assert_eq!(
            s2_first_row_id,
            s1_first_row_id + 3,
            "S2 first_row_id must continue from S1 (expected {}, got {s2_first_row_id})",
            s1_first_row_id + 3,
        );

        let post_rows = collect_id_rowid_seq(
            &session,
            "select id, _row_id, _last_updated_sequence_number from ice.ns.t",
        );
        assert_eq!(post_rows.len(), 5, "after S2 must have 5 rows");
        // Old rows keep their S1 row_ids and S1 sequence_numbers.
        assert_eq!(post_rows[0], (1_i64, s1_first_row_id as i64, s1_seq));
        assert_eq!(post_rows[1], (2_i64, s1_first_row_id as i64 + 1, s1_seq));
        assert_eq!(post_rows[2], (3_i64, s1_first_row_id as i64 + 2, s1_seq));
        // New rows get S2 row_ids and S2 sequence_numbers.
        assert_eq!(post_rows[3], (4_i64, s2_first_row_id as i64, s2_seq));
        assert_eq!(post_rows[4], (5_i64, s2_first_row_id as i64 + 1, s2_seq));

        // Delete id=2 via Phase 2a Puffin DV; surviving rows keep their lineage.
        session
            .execute_in_database("delete from ice.ns.t where id = 2", "default")
            .expect("delete row id=2");
        let after_rows = collect_id_rowid_seq(
            &session,
            "select id, _row_id, _last_updated_sequence_number from ice.ns.t",
        );
        assert_eq!(after_rows.len(), 4, "after delete must have 4 rows");
        assert!(
            after_rows.iter().all(|(id, _, _)| *id != 2),
            "id=2 must not appear after DELETE"
        );
        // id=1 preserves its original S1 row_id and sequence_number.
        assert_eq!(
            after_rows[0],
            (1_i64, s1_first_row_id as i64, s1_seq),
            "id=1 must keep S1 lineage after unrelated DELETE"
        );

        drop(engine);
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
        // table without write.row-lineage=true (same fail-fast path as non-iceberg
        // tables, verified without needing StarRocks table config).
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

    #[test]
    fn coordinated_iceberg_insert_requires_exchange_server() {
        let query = parse_query_for_engine_test("SELECT id FROM missing_table");
        let state = Arc::new(StandaloneState::default());
        let mut sink_spec =
            crate::sql::planner::distributed::write::sink::test_support::simple_sink_spec();
        sink_spec.iceberg.serialized_metadata = Some(
            crate::sql::planner::distributed::write::sink::test_support::single_bucket_partition_metadata_json(),
        );

        let result = super::execute_query_as_iceberg_write(
            &state, None, "default", &query, sink_spec, None, None, None,
        );

        let err = result.expect_err("default state should fail before executing the sink");
        assert!(
            err.contains("missing_table"),
            "error should come from analyzer/catalog lookup, got: {err}"
        );
    }

    #[test]
    fn iceberg_write_root_shuffle_by_output_name_uses_logical_output_column_id() {
        use crate::sql::catalog::PlannerTableProvider;
        use crate::sql::column_id::ColumnId;
        use crate::sql::optimizer::property::{DistributionSpec, HashSource};

        struct EmptyCatalog;
        impl PlannerTableProvider for EmptyCatalog {
            fn resolve_table_for_analysis(
                &self,
                _catalog: Option<&str>,
                _database: &str,
                table: &str,
            ) -> Result<crate::sql::catalog::ResolvedAnalyzerTable, String> {
                Err(format!("table not found: {table}"))
            }
        }

        let stmt = crate::sql::parser::parse_sql_raw("SELECT 1 AS payload, 'file-a' AS _file")
            .expect("parse query");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let (resolved, cte_registry, mut factory) =
            crate::sql::analyzer::analyze(&query, &EmptyCatalog, "default").expect("analyze query");
        let logical_plan = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)
            .expect("plan query");
        let planned_file_col = crate::sql::planner::plan_output_columns(&logical_plan)
            .expect("planned output columns")
            .into_iter()
            .find(|column| column.name == "_file")
            .expect("planned _file output")
            .column_id;
        assert_ne!(planned_file_col, ColumnId::UNSET);

        let distribution = super::iceberg_write_shuffle_by_output_name("_file")(&logical_plan)
            .expect("resolve root distribution")
            .expect("root distribution");

        match distribution {
            DistributionSpec::HashPartitioned { cols, source } => {
                assert_eq!(cols, vec![planned_file_col]);
                assert_eq!(source, HashSource::ShuffleAgg);
            }
            other => panic!("expected shuffle distribution, got {other:?}"),
        }
    }

    #[test]
    fn execute_query_as_iceberg_write_invokes_root_distribution_resolver_after_planning() {
        let query = parse_query_for_engine_test("SELECT 1 AS payload, 'file-a' AS _file");
        let mut state = StandaloneState::default();
        state.exchange_port = 1;
        let state = Arc::new(state);
        let mut sink_spec =
            crate::sql::planner::distributed::write::sink::test_support::simple_sink_spec();
        sink_spec.iceberg.serialized_metadata = Some(
            crate::sql::planner::distributed::write::sink::test_support::single_bucket_partition_metadata_json(),
        );

        let result = super::execute_query_as_iceberg_write(
            &state,
            None,
            "default",
            &query,
            sink_spec,
            None,
            Some(Box::new(|logical| {
                let saw_file_output = crate::sql::planner::plan_output_columns(logical)?
                    .into_iter()
                    .any(|column| {
                        column.name == "_file"
                            && column.column_id != crate::sql::column_id::ColumnId::UNSET
                    });
                if !saw_file_output {
                    return Err("resolver did not see planned _file output".to_string());
                }
                Err("resolver saw planned _file output".to_string())
            })),
            None,
        );

        let err = match result {
            Ok(_) => panic!("resolver error should stop write planning before execution"),
            Err(err) => err,
        };
        assert!(
            err.contains("resolver saw planned _file output"),
            "expected resolver error, got: {err}"
        );
    }

    #[test]
    fn planned_change_stream_write_uses_physical_plan_entrypoint() {
        use crate::sql::column_id::ColumnId;
        use crate::sql::common::ChangeStreamBranchKind;
        use crate::sql::optimizer::operator::{Operator, ValuesOp};
        use crate::sql::optimizer::optimized_tree::{
            OptimizedOperatorNode, PlanExecutionProps, attach_scalar_arena,
        };
        use crate::sql::optimizer::scalar::ScalarArena;
        use crate::sql::optimizer::statistics::Statistics;
        use crate::sql::planner::distributed::write::change_stream::{
            ChangeStreamWriteBranchSpec, ChangeStreamWriteDagSpec,
        };

        let _test_guard = super::acquire_standalone_test_guard();
        let observer = super::install_change_stream_write_test_observer(true);
        let output_columns = vec![
            crate::sql::analysis::OutputColumn {
                column_id: ColumnId::new_for_test(1),
                name: crate::exec::change_op::CHANGE_OP_COLUMN.to_string(),
                data_type: DataType::Int32,
                nullable: false,
                is_internal: true,
            },
            crate::sql::analysis::OutputColumn {
                column_id: ColumnId::new_for_test(2),
                name: "__change_data_route".to_string(),
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
        let branch = ChangeStreamWriteBranchSpec::reuse_data_for_test(vec![2]);
        let mut dag = ChangeStreamWriteDagSpec::for_test(Some(0), Some(1), vec![branch]);

        let result = super::execute_physical_plan_as_iceberg_change_stream_write(
            &state,
            None,
            "default",
            &optimized_tree,
            &mut dag,
            None,
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
            observation.branch_kinds,
            vec![ChangeStreamBranchKind::ReuseData]
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

    fn create_kv_tables(session: &StandaloneSession, t1_values: &str, t2_values: &str) {
        session
            .execute_in_database("create table ice.db1.t1 (k bigint, v bigint)", "default")
            .expect("create t1");
        session
            .execute_in_database("create table ice.db1.t2 (k bigint, v bigint)", "default")
            .expect("create t2");
        if !t1_values.trim().is_empty() {
            session
                .execute_in_database(
                    &format!("insert into ice.db1.t1 values {t1_values}"),
                    "default",
                )
                .expect("insert t1");
        }
        if !t2_values.trim().is_empty() {
            session
                .execute_in_database(
                    &format!("insert into ice.db1.t2 values {t2_values}"),
                    "default",
                )
                .expect("insert t2");
        }
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
        session
            .execute_in_database(
                "insert into ice.db1.t1 values (1,10),(2,20),(3,30)",
                "default",
            )
            .expect("insert t1");
        session
            .execute_in_database(
                "insert into ice.db1.t2 values (1,10),(1,5),(2,20),(2,15)",
                "default",
            )
            .expect("insert t2");

        // The subquery: SELECT t1.k FROM t1 WHERE t1.v = (SELECT min(t2.v) FROM t2 WHERE t2.k = t1.k)
        // k=1: min(t2.v) for k=1 = 5; t1.v=10 != 5 -> not selected
        // k=2: min(t2.v) for k=2 = 15; t1.v=20 != 15 -> not selected
        // k=3: no t2 rows -> NULL; t1.v=30 != NULL -> not selected
        // Result: no rows (empty)
        //
        // Alternatively use a query where some rows DO match:
        // WHERE t1.v = (SELECT min(t2.v) FROM t2 WHERE t2.k = t1.k)
        // Let's insert a t1 row where v=5 (k=1) so it matches min(t2.v)=5.
        session
            .execute_in_database("insert into ice.db1.t1 values (1,5)", "default")
            .expect("insert matching t1 row");

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
        session
            .execute_in_database(
                "insert into ice.db1.t1 values (1,10),(2,20),(3,30)",
                "default",
            )
            .expect("insert t1");
        session
            .execute_in_database(
                "insert into ice.db1.t2 values (1,100),(2,200),(3,300)",
                "default",
            )
            .expect("insert t2");

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
        session
            .execute_in_database("insert into ice.db1.t1 values (1,0),(2,0),(3,0)", "default")
            .expect("insert t1");
        session
            .execute_in_database("insert into ice.db1.t2 values (1,10),(2,20)", "default")
            .expect("insert t2");

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
        session
            .execute_in_database("insert into ice.db1.t1 values (1,0),(2,0),(3,0)", "default")
            .expect("insert t1");
        session
            .execute_in_database("insert into ice.db1.t2 values (1,10),(1,20)", "default")
            .expect("insert t2");

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
        session
            .execute_in_database("insert into ice.db1.t1 values (1,0),(NULL,0)", "default")
            .expect("insert t1");
        session
            .execute_in_database("insert into ice.db1.t2 values (1,10),(1,5)", "default")
            .expect("insert t2");

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
        session
            .execute_in_database("insert into ice.db1.t1 values (1,0),(2,0),(3,0)", "default")
            .expect("insert t1");
        session
            .execute_in_database("insert into ice.db1.t2 values (1,100),(2,200)", "default")
            .expect("insert t2");

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
        session
            .execute_in_database("insert into ice.db1.t1 values (1,0)", "default")
            .expect("insert t1");
        session
            .execute_in_database("insert into ice.db1.t2 values (1,100),(1,200)", "default")
            .expect("insert t2");

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
        session
            .execute_in_database("insert into ice.db1.t1 values (1,0),(2,0),(3,0)", "default")
            .expect("insert t1");
        // t2: k=1 has two rows (min=5), k=2 has one row (min=20), k=3 absent
        session
            .execute_in_database(
                "insert into ice.db1.t2 values (1,5),(1,10),(2,20)",
                "default",
            )
            .expect("insert t2");
        // t3: k=1 has one row (max=90), k=2 absent, k=3 has one row (max=30)
        session
            .execute_in_database("insert into ice.db1.t3 values (1,90),(3,30)", "default")
            .expect("insert t3");

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
    fn exists_correlated_returns_expected_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(
            &session,
            "(1,10),(2,20),(3,30),(NULL,40)",
            "(1,100),(1,101),(3,300),(NULL,999)",
        );

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.k = t1.k) ORDER BY 1",
            vec![Some(1), Some(3)],
        );
    }

    #[test]
    fn not_exists_correlated_returns_expected_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(
            &session,
            "(1,10),(2,20),(3,30),(NULL,40)",
            "(1,100),(3,300),(NULL,999)",
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
        create_kv_tables(&session, "(1,10),(2,20),(3,30)", "(1,101),(2,200)");

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
    fn in_correlated_returns_expected_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(
            &session,
            "(1,10),(2,20),(3,30),(4,NULL)",
            "(1,10),(1,11),(2,99),(3,30),(4,NULL)",
        );

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE t1.v IN (SELECT t2.v FROM t2 WHERE t2.k = t1.k) ORDER BY 1",
            vec![Some(1), Some(3)],
        );
    }

    #[test]
    fn in_uncorrelated_returns_expected_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(
            &session,
            "(1,10),(2,20),(3,30),(4,NULL)",
            "(9,10),(9,30),(9,NULL)",
        );

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE t1.v IN (SELECT t2.v FROM t2) ORDER BY 1",
            vec![Some(1), Some(3)],
        );
    }

    #[test]
    fn in_inside_or_with_build_null_preserves_unknown() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(&session, "(1,10),(2,20),(3,NULL)", "(9,10),(9,NULL)");

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
        create_kv_tables(&session, "(1,10),(2,20),(3,NULL)", "(9,10),(9,NULL)");

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
        create_kv_tables(&session, "(1,10),(2,20),(3,30)", "(9,20),(9,40)");

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
        create_kv_tables(&session, "(1,10),(2,20),(3,30)", "(9,20),(9,NULL)");

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
        create_kv_tables(&session, "(1,10),(2,20)", "(9,NULL)");

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
        create_kv_tables(&session, "(1,NULL),(2,20)", "");

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
        create_kv_tables(&session, "(1,10),(2,20)", "(9,NULL)");
        session
            .execute_in_database("create table ice.db1.t3 (k bigint, v bigint)", "default")
            .expect("create t3");
        session
            .execute_in_database("insert into ice.db1.t3 values (100,0)", "default")
            .expect("insert t3");

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
        create_kv_tables(&session, "(1,NULL),(2,20)", "(9,10)");
        session
            .execute_in_database("create table ice.db1.t3 (k bigint, v bigint)", "default")
            .expect("create t3");
        session
            .execute_in_database("insert into ice.db1.t3 values (100,0)", "default")
            .expect("insert t3");

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
        create_kv_tables(&session, "(1,10),(2,NULL),(3,30)", "(9,20),(9,40)");

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
            "(1,10),(2,20),(3,30),(4,40)",
            "(1,20),(1,30),(2,20),(2,NULL),(3,NULL),(3,40)",
        );

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 WHERE t1.v NOT IN (SELECT t2.v FROM t2 WHERE t2.k = t1.k) ORDER BY 1",
            vec![Some(1), Some(4)],
        );
    }

    #[test]
    fn multi_subquery_in_and_exists_returns_expected_rows() {
        let warehouse = TempDir::new().expect("warehouse");
        let (_engine, session) = open_scalar_subquery_test_engine(&warehouse);
        create_kv_tables(
            &session,
            "(1,10),(2,20),(3,30),(4,40)",
            "(1,10),(1,150),(2,20),(2,99),(3,300),(4,400)",
        );

        assert_subquery_result_i64(
            &session,
            "SELECT t1.k FROM t1 \
             WHERE t1.v IN (SELECT t2.v FROM t2 WHERE t2.k = t1.k) \
               AND EXISTS (SELECT 1 FROM t2 WHERE t2.k = t1.k AND t2.v > 100) \
             ORDER BY 1",
            vec![Some(1)],
        );
    }
}
