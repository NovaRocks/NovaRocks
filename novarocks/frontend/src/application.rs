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

use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Handle;

use crate::query_execution::service::QueryExecutionService;
use novarocks_execution_contract::{MaxWait, ResultByteLimit};
use novarocks_query_application::api::{QueryExecutionClient, QueryExecutionErrorKind};
use novarocks_query_application::coordination::{
    CoordinationBudgets, LogicalExecutionRowsConfig, LogicalExecutionSupervisor,
    LogicalExecutionSupervisorConfig, LogicalExecutionSupervisorShutdownError,
    RootResultDecodeRuntime, RootResultDecodeRuntimeOwner,
};
use novarocks_task_codec::TransportBudget;
use novarocks_workload_control::{
    LocalResourceAuthority, ResourceConfig, RootAdmissionHandle, WorkloadConfig, WorkloadControl,
    WorkloadObservationHandle, WorkloadShutdownError,
};

use crate::query_execution::split_assignment::TaskUpdateRetryPolicy;
use crate::state_store::{StateStoreHost, StateStoreHostInput, StateStoreProviderRegistry};
use crate::task_execution::ConnectorBlockingIoBudget;
use novarocks_native_trust::NativeTrust;
use novarocks_spi::connector::ConnectorControlRoleBindingFactory;
use novarocks_state_store_api::{StateStore, StateStoreProviderId};
use novarocks_types::{FrontendProcessId, NativeCompatibilityId, QueryProcessNamespace};

use crate::catalog_application::desired_state::{
    CatalogDesiredStateSource, CatalogDesiredStateSourceMode,
};
use crate::catalog_application::{
    CatalogDesiredStateSnapshot, CatalogDesiredStateSourceInput, FrontendCatalogApplicationPort,
    frontend_port::CatalogMaterializationConfig,
};
use crate::catalog_attachment::CatalogAttachmentRepository;
use crate::catalog_controller::{CatalogProjectionConfig, FrontendCatalogController};
use crate::catalog_prune::{CatalogPruneConfig, FrontendCatalogPruneService};
use crate::common::admitted_query_context::LakePublicationRuntimePolicy;
use crate::connector::ConnectorControlHost;
use crate::coordinator::{FrontendDistributedQueryCoordinator, QueryLifecycleConvergenceReader};
use crate::dml::DmlService;
use crate::mv::maintenance::MaintenanceCoordinatorConfig;
use crate::mv::scheduler::FrontendMvSchedulerConfig;
use crate::mv::{
    FrontendMvRefreshProviderActivationPort, FrontendMvService, repository::StateStoreMvRepository,
};
use crate::native::data_runtime::FrontendDataRuntime;
use crate::native::transport::FrontendNativeTransport;
use crate::query_control::FrontendQueryControl;
use crate::query_execution::maintenance::TableMaintenanceService;
use crate::query_execution::native_execution_adapter::FrontendLogicalExecutionNativePort;
use crate::statistics::FrontendStatisticsService;
use crate::statistics_jobs::service::{
    FrontendStatisticsApplicationPort, StatisticsApplicationService,
};
use crate::table_maintenance::FrontendTableMaintenanceService;
use crate::topology::{ClusterBackendOpenConfig, ClusterBackendService};
use crate::view::FrontendViewService;
use crate::workload_lifecycle::{
    FrontendCatalogCounts, FrontendCatalogSnapshotIdentity, FrontendCatalogSourceMode,
    FrontendServingLifecycle,
};

const STATE_STORE_OPEN_TIMEOUT: Duration = Duration::from_secs(5);
const STATE_STORE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_RESULT_DECODE_WORKER_COUNT: NonZeroUsize = NonZeroUsize::new(2).unwrap();
const DEFAULT_RESULT_DECODE_QUEUE_CAPACITY: NonZeroUsize = NonZeroUsize::new(32).unwrap();
const DEFAULT_RESULT_DELIVERY_CAPACITY: NonZeroUsize = NonZeroUsize::new(32).unwrap();
const DEFAULT_LOGICAL_EXECUTION_MAX_ATTEMPTS: NonZeroU32 = NonZeroU32::new(3).unwrap();
const DEFAULT_REPLACEMENT_RESERVATION_VALID_FOR: Duration = Duration::from_secs(30);
const DEFAULT_RESULT_FETCH_MAX_WAIT: Duration = Duration::from_millis(200);
const DEFAULT_LOGICAL_EXECUTION_START_CAPACITY: NonZeroUsize = NonZeroUsize::new(256).unwrap();
const DEFAULT_LOGICAL_EXECUTION_MAILBOX_CAPACITY: NonZeroUsize = NonZeroUsize::new(64).unwrap();
const DEFAULT_LOGICAL_EXECUTION_CONTEXT_ISSUE_CAPACITY: NonZeroUsize =
    NonZeroUsize::new(16).unwrap();
const TEST_WORKLOAD_TOTAL_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const TEST_WORKLOAD_CONTROL_BYTES: u64 = 64 * 1024 * 1024;
const TEST_WORKLOAD_PER_SCOPE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

#[cfg(test)]
fn test_native_trust() -> Arc<NativeTrust> {
    use novarocks_native_trust::{
        DeploymentId, NativeCallerSubject, NativeTransportMode, ValidatedSharedSecret,
    };

    Arc::new(NativeTrust::new(
        DeploymentId::parse("frontend-application-test").expect("fixed test deployment id"),
        ValidatedSharedSecret::new(novarocks_secret::SecretValue::new(
            "0123456789abcdef0123456789abcdef",
        ))
        .expect("fixed test shared secret"),
        NativeCallerSubject::parse("fe@127.0.0.1:19040").expect("fixed test caller subject"),
        NativeTransportMode::Disabled,
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrontendApplicationErrorKind {
    DeploymentSource,
    StateStoreHost,
    ViewServiceOpen,
    TableMaintenanceServiceOpen,
    MvServiceOpen,
    StatisticsApplicationServiceOpen,
    CatalogApplicationServiceOpen,
    CatalogControllerOpen,
    ConnectorControlHost,
    ClusterBackendOpen,
    CoordinatorOpen,
    ResultDecodeRuntimeOpen,
    WorkloadControlOpen,
    Server,
    Shutdown,
}

#[derive(Debug)]
pub struct FrontendApplicationError {
    kind: FrontendApplicationErrorKind,
    message: String,
}

impl FrontendApplicationError {
    pub(crate) fn new(kind: FrontendApplicationErrorKind, error: impl fmt::Display) -> Self {
        Self {
            kind,
            message: error.to_string(),
        }
    }

    pub(crate) fn server(error: impl fmt::Display) -> Self {
        Self::new(FrontendApplicationErrorKind::Server, error)
    }

    pub(crate) fn with_cleanup_context(mut self, cleanup_error: impl fmt::Display) -> Self {
        self.message
            .push_str(&format!("; cleanup failed: {cleanup_error}"));
        self
    }

    pub const fn kind(&self) -> FrontendApplicationErrorKind {
        self.kind
    }
}

impl fmt::Display for FrontendApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for FrontendApplicationError {}

/// Unique owner aggregate for the new query-application runtime.
///
/// The legacy SQL entrypoint is intentionally not wired to these handles until
/// T08-C3. Keeping all process owners here still makes startup and shutdown
/// ownership concrete: services may clone only the narrow capabilities below,
/// never the supervisor, Registry, workload control, or decode join handles.
struct FrontendExecutionRuntimeOwner {
    supervisor: LogicalExecutionSupervisor,
    logical_execution_client: QueryExecutionClient,
    workload: Option<WorkloadControl>,
    root_admission: RootAdmissionHandle,
    workload_observation: WorkloadObservationHandle,
    resources: LocalResourceAuthority,
    decode: RootResultDecodeRuntimeOwner,
    decode_runtime: RootResultDecodeRuntime,
    terminal_error: Option<String>,
    shutdown_complete: bool,
}

impl FrontendExecutionRuntimeOwner {
    fn try_new(
        runtime: Handle,
        supervisor_config: LogicalExecutionSupervisorConfig,
        workload_config: WorkloadConfig,
        resource_config: ResourceConfig,
        decode_worker_count: NonZeroUsize,
        decode_queue_capacity: NonZeroUsize,
    ) -> Result<Self, FrontendApplicationError> {
        let workload =
            WorkloadControl::try_new_split(workload_config, resource_config).map_err(|error| {
                FrontendApplicationError::new(
                    FrontendApplicationErrorKind::WorkloadControlOpen,
                    error,
                )
            })?;
        let decode =
            RootResultDecodeRuntimeOwner::try_new(decode_worker_count, decode_queue_capacity)
                .map_err(|error| {
                    FrontendApplicationError::new(
                        FrontendApplicationErrorKind::ResultDecodeRuntimeOpen,
                        error,
                    )
                })?;
        let decode_runtime = decode.runtime();
        let frontend_process_id = FrontendProcessId::new_v7();
        let process_bytes = frontend_process_id.to_bytes();
        let namespace = QueryProcessNamespace::new(
            u64::from_be_bytes(process_bytes[..8].try_into().expect("UUID high half"))
                ^ u64::from_be_bytes(process_bytes[8..].try_into().expect("UUID low half")),
        );
        tracing::info!(
            query_process_namespace = %namespace,
            frontend_process_id = %frontend_process_id,
            "frontend logical execution runtime initialized"
        );
        let (supervisor, logical_execution_client) = LogicalExecutionSupervisor::new(
            runtime,
            Arc::new(FrontendLogicalExecutionNativePort),
            workload.resources.clone(),
            namespace,
            frontend_process_id,
            supervisor_config,
        );
        Ok(Self {
            supervisor,
            logical_execution_client,
            workload: Some(workload.owner),
            root_admission: workload.root_admission,
            workload_observation: workload.observation,
            resources: workload.resources,
            decode,
            decode_runtime,
            terminal_error: None,
            shutdown_complete: false,
        })
    }

    fn mark_ready(&self) -> Result<(), novarocks_workload_control::WorkError> {
        self.workload
            .as_ref()
            .expect("workload owner exists until execution runtime shutdown")
            .mark_ready()
    }

    fn close_admission(&self) {
        if let Some(workload) = self.workload.as_ref() {
            workload.close_admission();
        }
    }

    async fn shutdown_until(&mut self, deadline: Instant) -> Result<(), String> {
        self.close_admission();
        if self.shutdown_complete {
            return Ok(());
        }

        let mut registry_errors = 0;
        loop {
            match self.supervisor.shutdown_until(deadline).await {
                Ok(()) => break,
                Err(LogicalExecutionSupervisorShutdownError::DeadlineExceeded) => {
                    return Err(
                        "frontend logical execution supervisor shutdown deadline exceeded"
                            .to_string(),
                    );
                }
                Err(LogicalExecutionSupervisorShutdownError::Registry(error)) => {
                    let message = error.to_string();
                    self.record_terminal_error(message);
                    registry_errors += 1;
                    if registry_errors == 1 {
                        // A concurrent Registry transition, or a one-shot
                        // observation failure, may already have made the exact
                        // owner convergent. Retry once in place while retaining
                        // the first invariant failure for the final report.
                        tokio::task::yield_now().await;
                        continue;
                    }
                    return Err(self
                        .terminal_error
                        .clone()
                        .expect("the Registry error is retained"));
                }
                Err(error) => {
                    self.record_terminal_error(error.to_string());
                    break;
                }
            }
        }

        if self.workload.is_some() {
            self.shutdown_workload_until(deadline).await?;
        }

        if let Err(error) = self.decode.shutdown_until(deadline).await {
            if error.kind() == QueryExecutionErrorKind::DeadlineExceeded {
                return Err(error.to_string());
            }
            self.record_terminal_error(error.to_string());
        }

        self.shutdown_complete = true;
        self.terminal_error.take().map_or(Ok(()), Err)
    }

    fn abandon_for_process_exit(&mut self) {
        self.close_admission();
        self.supervisor.abandon_for_process_exit();
        self.decode.request_shutdown_for_process_exit();
        self.workload.take();
        self.shutdown_complete = true;
    }

    fn record_terminal_error(&mut self, error: String) {
        match self.terminal_error.as_mut() {
            Some(primary) if !primary.contains(&error) => {
                primary.push_str(&format!("; cleanup failed: {error}"));
            }
            Some(_) => {}
            None => self.terminal_error = Some(error),
        }
    }

    async fn shutdown_workload_until(&mut self, deadline: Instant) -> Result<(), String> {
        loop {
            let revision = self
                .workload
                .as_ref()
                .expect("workload owner remains until a successful shutdown proof")
                .progress_revision();
            let owner = self
                .workload
                .take()
                .expect("workload owner remains until a successful shutdown proof");
            match owner.shutdown() {
                Ok(_) => return Ok(()),
                Err(failure) => {
                    let (error, owner) = failure.into_parts();
                    self.workload = Some(owner);
                    if error == WorkloadShutdownError::AdmissionOpen {
                        return Err(
                            "frontend workload admission remained open during shutdown".to_string()
                        );
                    }
                }
            }

            let wait = self
                .workload
                .as_ref()
                .expect("failed workload shutdown returns the exact owner")
                .wait_progress(revision);
            if tokio::time::timeout_at(deadline.into(), wait)
                .await
                .is_err()
            {
                return Err("frontend workload shutdown deadline exceeded before drain".to_string());
            }
        }
    }

    fn logical_execution_client(&self) -> QueryExecutionClient {
        self.logical_execution_client.clone()
    }

    fn root_admission(&self) -> RootAdmissionHandle {
        self.root_admission.clone()
    }

    fn workload_observation(&self) -> WorkloadObservationHandle {
        self.workload_observation.clone()
    }

    fn resources(&self) -> LocalResourceAuthority {
        self.resources.clone()
    }

    fn decode_runtime(&self) -> RootResultDecodeRuntime {
        self.decode_runtime.clone()
    }

    fn is_shutdown_complete(&self) -> bool {
        self.shutdown_complete
    }
}

pub struct FrontendApplicationHost {
    connector_control: Arc<ConnectorControlHost>,
    catalog_runtime_projection: Arc<crate::catalog_application::CatalogRuntimeProjection>,
    serving_lifecycle: Arc<FrontendServingLifecycle>,
    statistics_service: Option<Arc<FrontendStatisticsService>>,
    dml_service: Option<Arc<DmlService>>,
    statistics_application_service: Option<Arc<StatisticsApplicationService>>,
    statistics_application_port: Option<Arc<FrontendStatisticsApplicationPort>>,
    catalog_application_port: Option<Arc<FrontendCatalogApplicationPort>>,
    /// Meets the attempt contract's host obligation to return abandoned
    /// attempts; see `state_store::sweeper`.
    abandoned_attempt_sweeper: Option<Arc<crate::state_store::AbandonedAttemptSweeper>>,
    catalog_controller: Option<Arc<FrontendCatalogController>>,
    catalog_prune: Option<Arc<FrontendCatalogPruneService>>,
    view_service: Option<Arc<dyn crate::view::ViewService>>,
    table_maintenance_service: Option<Arc<dyn TableMaintenanceService>>,
    mv_repository: Option<Arc<dyn crate::mv::domain::repository::MvRepository>>,
    mv_application_service: Option<Arc<dyn crate::mv::domain::application::MvApplicationService>>,
    mv_service: Option<Arc<FrontendMvService>>,
    mv_refresh_provider_activation: Option<Arc<FrontendMvRefreshProviderActivationPort>>,
    mv_background_engine_sink: Option<Arc<dyn crate::mv::background::MvBackgroundEngineSink>>,
    state_store_host: Option<StateStoreHost>,
    query_execution: Option<QueryExecutionService>,
    query_control: crate::query_execution::control::QueryControlService,
    coordinator: Option<Arc<FrontendDistributedQueryCoordinator>>,
    execution_runtime_owner: FrontendExecutionRuntimeOwner,
    execution_role: novarocks_types::ClusterRole,
    data_runtime: FrontendDataRuntime,
    topology: Option<Arc<ClusterBackendService>>,
    optimizer_query_mem_limit_bytes: u64,
    lake_publication_runtime_policy: LakePublicationRuntimePolicy,
    function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
}

/// Matches the historical `[runtime] optimizer_query_mem_limit_bytes` default.
const DEFAULT_OPTIMIZER_QUERY_MEM_LIMIT_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const DEFAULT_CONNECTOR_SPLIT_INITIAL_DYNAMIC_FILTER_WAIT_CAP: Duration = Duration::from_secs(1);

/// Query-control timeouts frozen from `[runtime]` at startup.
///
/// Coordinator code receives these; it never reads a process-global config
/// while admitting a query. Defaults mirror `RuntimeConfig`'s serde defaults so
/// a `FrontendExecutionConfig` built without a config file still validates.
#[derive(Clone, Copy, Debug)]
pub struct FrontendQueryControlTimeouts {
    pub pre_start_timeout_ms: u64,
}

impl Default for FrontendQueryControlTimeouts {
    fn default() -> Self {
        Self {
            pre_start_timeout_ms: 30_000,
        }
    }
}

#[derive(Clone)]
pub struct FrontendLogicalExecutionRuntimeConfig {
    supervisor: LogicalExecutionSupervisorConfig,
    workload: WorkloadConfig,
    resources: ResourceConfig,
    decode_worker_count: NonZeroUsize,
    decode_queue_capacity: NonZeroUsize,
}

impl FrontendLogicalExecutionRuntimeConfig {
    pub fn new(
        supervisor: LogicalExecutionSupervisorConfig,
        workload: WorkloadConfig,
        resources: ResourceConfig,
        decode_worker_count: NonZeroUsize,
        decode_queue_capacity: NonZeroUsize,
    ) -> Self {
        Self {
            supervisor,
            workload,
            resources,
            decode_worker_count,
            decode_queue_capacity,
        }
    }

    fn for_test() -> Self {
        Self::new(
            LogicalExecutionSupervisorConfig::new(
                DEFAULT_LOGICAL_EXECUTION_START_CAPACITY,
                DEFAULT_LOGICAL_EXECUTION_MAILBOX_CAPACITY,
                DEFAULT_LOGICAL_EXECUTION_CONTEXT_ISSUE_CAPACITY,
                DEFAULT_LOGICAL_EXECUTION_CONTEXT_ISSUE_CAPACITY,
                LogicalExecutionRowsConfig::new(
                    DEFAULT_RESULT_DELIVERY_CAPACITY,
                    DEFAULT_LOGICAL_EXECUTION_MAX_ATTEMPTS,
                    DEFAULT_REPLACEMENT_RESERVATION_VALID_FOR,
                    MaxWait::new(DEFAULT_RESULT_FETCH_MAX_WAIT)
                        .expect("the test result fetch wait is representable"),
                    ResultByteLimit::new(16 * 1024 * 1024)
                        .expect("the test result fetch byte limit is nonzero"),
                ),
            ),
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: TEST_WORKLOAD_TOTAL_BYTES,
                control_bytes: TEST_WORKLOAD_CONTROL_BYTES,
                per_scope_bytes: TEST_WORKLOAD_PER_SCOPE_BYTES,
            },
            DEFAULT_RESULT_DECODE_WORKER_COUNT,
            DEFAULT_RESULT_DECODE_QUEUE_CAPACITY,
        )
    }
}

#[derive(Clone)]
pub struct FrontendExecutionConfig {
    advertised_report_host: String,
    configured_report_port: u16,
    runtime_filter_worker_count: NonZeroUsize,
    native_compatibility_id: NativeCompatibilityId,
    function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
    mv_scheduler: FrontendMvSchedulerConfig,
    mv_maintenance: MaintenanceCoordinatorConfig,
    /// Cost budget frozen from `[runtime]` and handed to statement admission.
    ///
    /// SQL costing only ever sees the value admission froze; it never consults
    /// a process-global configuration.
    optimizer_query_mem_limit_bytes: u64,
    /// Query-control timeouts frozen from `[runtime]` and handed to the
    /// coordinator, which validates them once at startup instead of per query.
    query_control_timeouts: FrontendQueryControlTimeouts,
    task_update_retry_policy: TaskUpdateRetryPolicy,
    /// Every budget the task protocol runs one attempt with, frozen from
    /// `[runtime]` and validated once at startup.
    ///
    /// Held here rather than read per attempt so a deployment's bounds cannot
    /// change while the process runs, and so an attempt never has to invent
    /// one that configuration failed to supply.
    coordination_budgets: CoordinationBudgets,
    transport_budget: TransportBudget,
    connector_blocking_io_budget: ConnectorBlockingIoBudget,
    /// Positive root-result payload credit placed on every Native fetch.
    result_fetch_byte_limit: ResultByteLimit,
    /// Fixed process-wide worker and waiting bounds for synchronous result decode.
    result_decode_worker_count: NonZeroUsize,
    result_decode_queue_capacity: NonZeroUsize,
    logical_execution_supervisor: LogicalExecutionSupervisorConfig,
    workload: WorkloadConfig,
    workload_resources: ResourceConfig,
    /// Connector split enumeration's bounded, server-owned initial feedback
    /// wait. This is frozen at startup and deliberately has no SQL override.
    connector_split_initial_dynamic_filter_wait_cap: Duration,
    lake_publication_runtime_policy: LakePublicationRuntimePolicy,
    catalog_projection: CatalogProjectionConfig,
    catalog_materialization: CatalogMaterializationConfig,
    catalog_prune: CatalogPruneConfig,
    /// The deployment's catalog desired-state authority, selected and validated
    /// once before this application opens any runtime resource.
    ///
    /// Frozen here rather than consulted per statement so that "which authority
    /// owns catalog desired state" cannot change while the process runs, and so
    /// an unimplemented mode is rejected before startup opens anything.
    catalog_desired_state_source: CatalogDesiredStateSourceInput,
}

impl FrontendExecutionConfig {
    pub fn new(
        advertised_report_host: impl Into<String>,
        configured_report_port: u16,
        runtime_filter_worker_count: NonZeroUsize,
        native_compatibility_id: NativeCompatibilityId,
        function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
        logical_runtime: FrontendLogicalExecutionRuntimeConfig,
    ) -> Self {
        Self {
            advertised_report_host: advertised_report_host.into(),
            configured_report_port,
            runtime_filter_worker_count,
            native_compatibility_id,
            function_catalog,
            mv_scheduler: FrontendMvSchedulerConfig::default(),
            mv_maintenance: MaintenanceCoordinatorConfig::default(),
            optimizer_query_mem_limit_bytes: DEFAULT_OPTIMIZER_QUERY_MEM_LIMIT_BYTES,
            query_control_timeouts: FrontendQueryControlTimeouts::default(),
            task_update_retry_policy: TaskUpdateRetryPolicy::default(),
            coordination_budgets: CoordinationBudgets::DEFAULT,
            transport_budget: TransportBudget::DEFAULT,
            connector_blocking_io_budget: ConnectorBlockingIoBudget::default(),
            result_fetch_byte_limit: ResultByteLimit::new(16 * 1024 * 1024)
                .expect("the test result fetch byte limit is nonzero"),
            result_decode_worker_count: logical_runtime.decode_worker_count,
            result_decode_queue_capacity: logical_runtime.decode_queue_capacity,
            logical_execution_supervisor: logical_runtime.supervisor,
            workload: logical_runtime.workload,
            workload_resources: logical_runtime.resources,
            connector_split_initial_dynamic_filter_wait_cap:
                DEFAULT_CONNECTOR_SPLIT_INITIAL_DYNAMIC_FILTER_WAIT_CAP,
            lake_publication_runtime_policy: LakePublicationRuntimePolicy::try_new(
                Duration::from_secs(30 * 60),
                Duration::from_secs(45 * 60),
                Duration::from_secs(60),
                Duration::from_secs(5 * 60),
                Duration::from_secs(60),
            )
            .expect("default lake publication policy is safe"),
            catalog_projection: CatalogProjectionConfig::default(),
            catalog_materialization: CatalogMaterializationConfig::default(),
            catalog_prune: CatalogPruneConfig::try_new(
                Duration::from_secs(30),
                Duration::from_secs(5),
                16,
            )
            .expect("default catalog prune configuration is safe"),
            // This constructor remains an in-process test convenience. Server
            // composition always replaces it with its preflighted closed input.
            catalog_desired_state_source: CatalogDesiredStateSourceInput::StaticFile(
                CatalogDesiredStateSnapshot::try_new(CatalogDesiredStateSourceMode::StaticFile, [])
                    .expect("an empty static catalog snapshot is valid"),
            ),
        }
    }

    /// In-process test convenience with deterministic local governance bounds.
    #[doc(hidden)]
    pub fn new_for_test(
        advertised_report_host: impl Into<String>,
        configured_report_port: u16,
        runtime_filter_worker_count: NonZeroUsize,
        native_compatibility_id: NativeCompatibilityId,
        function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
    ) -> Self {
        Self::new(
            advertised_report_host,
            configured_report_port,
            runtime_filter_worker_count,
            native_compatibility_id,
            function_catalog,
            FrontendLogicalExecutionRuntimeConfig::for_test(),
        )
    }

    pub(crate) const fn native_compatibility_id(&self) -> NativeCompatibilityId {
        self.native_compatibility_id
    }

    pub(crate) fn function_catalog(&self) -> Arc<novarocks_functions::EngineFunctionCatalog> {
        Arc::clone(&self.function_catalog)
    }

    pub fn with_query_control_timeouts(mut self, timeouts: FrontendQueryControlTimeouts) -> Self {
        self.query_control_timeouts = timeouts;
        self
    }

    pub fn with_task_update_retry_policy(mut self, policy: TaskUpdateRetryPolicy) -> Self {
        self.task_update_retry_policy = policy;
        self
    }

    pub fn with_task_execution_budgets(
        mut self,
        coordination: CoordinationBudgets,
        transport: TransportBudget,
    ) -> Self {
        self.coordination_budgets = coordination;
        self.transport_budget = transport;
        self
    }

    pub fn with_connector_blocking_io_budget(mut self, budget: ConnectorBlockingIoBudget) -> Self {
        self.connector_blocking_io_budget = budget;
        self
    }

    pub fn with_result_fetch_byte_limit(mut self, limit: ResultByteLimit) -> Self {
        self.result_fetch_byte_limit = limit;
        self
    }

    pub fn with_connector_split_initial_dynamic_filter_wait_cap(mut self, cap: Duration) -> Self {
        self.connector_split_initial_dynamic_filter_wait_cap = cap;
        self
    }

    pub fn with_lake_publication_runtime_policy(
        mut self,
        policy: LakePublicationRuntimePolicy,
    ) -> Self {
        self.lake_publication_runtime_policy = policy;
        self
    }

    pub(crate) const fn lake_publication_runtime_policy(&self) -> LakePublicationRuntimePolicy {
        self.lake_publication_runtime_policy
    }

    pub fn with_optimizer_query_mem_limit_bytes(mut self, bytes: u64) -> Self {
        self.optimizer_query_mem_limit_bytes = bytes;
        self
    }

    pub(crate) fn optimizer_query_mem_limit_bytes(&self) -> u64 {
        self.optimizer_query_mem_limit_bytes
    }

    pub fn with_mv_scheduler_config(mut self, config: FrontendMvSchedulerConfig) -> Self {
        self.mv_scheduler = config;
        self
    }

    pub fn with_mv_maintenance_config(mut self, config: MaintenanceCoordinatorConfig) -> Self {
        self.mv_maintenance = config;
        self
    }

    #[allow(
        dead_code,
        reason = "Retained for frontend application-builder coverage that injects projection timing."
    )]
    pub(crate) fn with_catalog_projection_config(
        mut self,
        config: CatalogProjectionConfig,
    ) -> Self {
        self.catalog_projection = config;
        self
    }

    pub fn with_catalog_materialization_config(
        mut self,
        config: CatalogMaterializationConfig,
    ) -> Self {
        self.catalog_materialization = config;
        self
    }

    pub fn with_catalog_prune_config(mut self, config: CatalogPruneConfig) -> Self {
        self.catalog_prune = config;
        self
    }

    /// Supplies the already preflighted, closed desired-state source input.
    pub fn with_catalog_desired_state_source(
        mut self,
        source: CatalogDesiredStateSourceInput,
    ) -> Self {
        self.catalog_desired_state_source = source;
        self
    }

    pub const fn catalog_desired_state_source(&self) -> &CatalogDesiredStateSourceInput {
        &self.catalog_desired_state_source
    }
}

impl FrontendApplicationHost {
    pub async fn open(
        state_store: Option<StateStoreHostInput>,
        execution: FrontendExecutionConfig,
        backend: ClusterBackendOpenConfig,
        data_runtime: Handle,
        native_trust: Arc<NativeTrust>,
        native_transport: FrontendNativeTransport,
    ) -> Result<Self, FrontendApplicationError> {
        Self::open_with_role_factories_and_state_store_registry(
            state_store,
            &StateStoreProviderRegistry::new(),
            execution,
            backend,
            Vec::new(),
            data_runtime,
            native_trust,
            native_transport,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn open_with_role_factories_and_state_store_registry(
        state_store: Option<StateStoreHostInput>,
        state_store_registry: &StateStoreProviderRegistry,
        execution: FrontendExecutionConfig,
        backend: ClusterBackendOpenConfig,
        connector_role_factories: Vec<Arc<dyn ConnectorControlRoleBindingFactory>>,
        data_runtime: Handle,
        native_trust: Arc<NativeTrust>,
        native_transport: FrontendNativeTransport,
    ) -> Result<Self, FrontendApplicationError> {
        let connector_control = Arc::new(
            ConnectorControlHost::with_role_factories(connector_role_factories).map_err(
                |error| {
                    FrontendApplicationError::new(
                        FrontendApplicationErrorKind::ConnectorControlHost,
                        error,
                    )
                },
            )?,
        );
        Self::open_with_connector_control_host_and_state_store_registry(
            state_store,
            state_store_registry,
            execution,
            backend,
            connector_control,
            data_runtime,
            native_trust,
            native_transport,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn open_with_connector_control_host_and_state_store_registry(
        state_store: Option<StateStoreHostInput>,
        state_store_registry: &StateStoreProviderRegistry,
        execution: FrontendExecutionConfig,
        backend: ClusterBackendOpenConfig,
        connector_control: Arc<ConnectorControlHost>,
        data_runtime: Handle,
        native_trust: Arc<NativeTrust>,
        native_transport: FrontendNativeTransport,
    ) -> Result<Self, FrontendApplicationError> {
        // The selected catalog desired-state source mode is decided here, ahead
        // of every startup side effect: nothing is open yet, no StateStore host
        // exists, no controller is running, so a mode this build implements no
        // authority for fails with nothing to unwind. Doing this later would
        // mean either a partially opened frontend to clean up or — worse — a
        // path that quietly serves an unimplemented mode from the dynamic
        // StateStore authority.
        let catalog_source_input = execution.catalog_desired_state_source().clone();
        let catalog_source_mode = catalog_source_input.mode();
        if let Err(error) = catalog_source_mode.require_implemented() {
            return Err(FrontendApplicationError::new(
                FrontendApplicationErrorKind::CatalogApplicationServiceOpen,
                error,
            ));
        }
        let query_runtime = data_runtime.clone();
        let data_runtime = FrontendDataRuntime::new_with_native_trust(
            data_runtime,
            native_trust,
            native_transport,
            execution.transport_budget,
            execution.connector_blocking_io_budget,
        )
        .map_err(|error| {
            FrontendApplicationError::new(FrontendApplicationErrorKind::CoordinatorOpen, error)
        })?;
        let execution_runtime_owner = FrontendExecutionRuntimeOwner::try_new(
            query_runtime,
            execution.logical_execution_supervisor,
            execution.workload.clone(),
            execution.workload_resources.clone(),
            execution.result_decode_worker_count,
            execution.result_decode_queue_capacity,
        )?;
        let catalog_runtime_projection =
            crate::catalog_application::CatalogRuntimeProjection::new();
        let mut host = Self {
            connector_control,
            catalog_runtime_projection,
            serving_lifecycle: Arc::new(FrontendServingLifecycle::new()),
            statistics_service: None,
            dml_service: None,
            statistics_application_service: None,
            statistics_application_port: None,
            catalog_application_port: None,
            abandoned_attempt_sweeper: None,
            catalog_controller: None,
            catalog_prune: None,
            view_service: None,
            table_maintenance_service: None,
            mv_repository: None,
            mv_application_service: None,
            mv_service: None,
            mv_refresh_provider_activation: None,
            mv_background_engine_sink: None,
            state_store_host: None,
            query_execution: None,
            query_control: FrontendQueryControl::service(),
            coordinator: None,
            execution_runtime_owner,
            execution_role: backend.role(),
            data_runtime: data_runtime.clone(),
            topology: None,
            optimizer_query_mem_limit_bytes: DEFAULT_OPTIMIZER_QUERY_MEM_LIMIT_BYTES,
            lake_publication_runtime_policy: execution.lake_publication_runtime_policy(),
            function_catalog: execution.function_catalog(),
        };

        if let Some(state_store) = state_store
            && let Err(error) = host
                .open_configured(state_store, state_store_registry)
                .await
        {
            return Err(host.cleanup_open_error(error).await);
        }
        let attachments = if catalog_source_mode == CatalogDesiredStateSourceMode::DynamicStateStore
        {
            let store = match host.state_store() {
                Some(store) => store,
                None => {
                    return Err(host
                        .cleanup_open_error(FrontendApplicationError::new(
                            FrontendApplicationErrorKind::CatalogApplicationServiceOpen,
                            "the dynamic StateStore catalog source requires a configured Frontend StateStore",
                        ))
                        .await);
                }
            };
            match CatalogAttachmentRepository::open(store, host.run_policy()).await {
                Ok(repository) => Some(repository),
                Err(error) => {
                    return Err(host
                        .cleanup_open_error(FrontendApplicationError::new(
                            FrontendApplicationErrorKind::CatalogApplicationServiceOpen,
                            error,
                        ))
                        .await);
                }
            }
        } else {
            None
        };
        let catalog_source =
            match CatalogDesiredStateSource::from_input(catalog_source_input, attachments) {
                Ok(source) => source,
                Err(error) => {
                    return Err(host
                        .cleanup_open_error(FrontendApplicationError::new(
                            FrontendApplicationErrorKind::CatalogApplicationServiceOpen,
                            error,
                        ))
                        .await);
                }
            };
        host.catalog_application_port = Some(Arc::new(
            FrontendCatalogApplicationPort::new_with_materialization_config(
                catalog_source,
                Arc::clone(&host.connector_control),
                host.catalog_runtime_projection.publisher(),
                tokio::runtime::Handle::current(),
                execution.catalog_materialization,
            ),
        ));
        if catalog_source_mode == CatalogDesiredStateSourceMode::DynamicStateStore {
            let store = host
                .state_store()
                .expect("dynamic catalog source required a StateStore above");
            let controller = match FrontendCatalogController::new(
                store,
                Arc::clone(
                    host.catalog_application_port
                        .as_ref()
                        .expect("catalog application port is installed"),
                ),
                execution.catalog_projection.clone(),
            ) {
                Ok(controller) => controller,
                Err(error) => {
                    return Err(host
                        .cleanup_open_error(FrontendApplicationError::new(
                            FrontendApplicationErrorKind::CatalogControllerOpen,
                            error,
                        ))
                        .await);
                }
            };
            if let Err(error) = controller.bootstrap().await {
                return Err(host
                    .cleanup_open_error(FrontendApplicationError::new(
                        FrontendApplicationErrorKind::CatalogControllerOpen,
                        error,
                    ))
                    .await);
            }
            let counts = host
                .catalog_application_port
                .as_ref()
                .expect("catalog application port is installed")
                .projection_counts();
            host.serving_lifecycle.publish_catalog_bootstrap(
                FrontendCatalogSourceMode::DynamicStateStore,
                true,
                None,
                FrontendCatalogCounts {
                    desired: counts.ready + counts.unavailable,
                    ready: counts.ready,
                    unavailable: counts.unavailable,
                },
            );
            if let Err(error) = controller.start() {
                return Err(host
                    .cleanup_open_error(FrontendApplicationError::new(
                        FrontendApplicationErrorKind::CatalogControllerOpen,
                        error,
                    ))
                    .await);
            }
            host.catalog_controller = Some(controller);
        } else if catalog_source_mode == CatalogDesiredStateSourceMode::StaticFile {
            let projection = Arc::clone(
                host.catalog_application_port
                    .as_ref()
                    .expect("catalog application port is installed"),
            );
            match projection
                .reconcile_snapshot_with_page_size(
                    execution.catalog_projection.page_size,
                    execution.catalog_projection.worker_count,
                )
                .await
            {
                Ok((snapshot, counts)) => {
                    host.serving_lifecycle.publish_catalog_bootstrap(
                        FrontendCatalogSourceMode::StaticFile,
                        true,
                        Some(
                            FrontendCatalogSnapshotIdentity::try_new(
                                snapshot.identity().catalog_count(),
                                snapshot.identity().short_digest(),
                            )
                            .expect("catalog snapshot identity has a fixed short digest"),
                        ),
                        FrontendCatalogCounts {
                            desired: snapshot.identity().catalog_count(),
                            ready: counts.ready,
                            unavailable: counts.unavailable,
                        },
                    );
                }
                Err(error) => {
                    return Err(host
                        .cleanup_open_error(FrontendApplicationError::new(
                            FrontendApplicationErrorKind::CatalogApplicationServiceOpen,
                            error,
                        ))
                        .await);
                }
            }
        }
        match ClusterBackendService::open(backend, tokio::runtime::Handle::current(), data_runtime)
            .await
        {
            Ok(topology) => host.topology = Some(topology),
            Err(error) => {
                return Err(host
                    .cleanup_open_error(FrontendApplicationError::new(
                        FrontendApplicationErrorKind::ClusterBackendOpen,
                        error,
                    ))
                    .await);
            }
        }
        let catalog_prune = FrontendCatalogPruneService::new(
            Arc::clone(
                host.catalog_application_port
                    .as_ref()
                    .expect("catalog application is installed"),
            ),
            host.backend_topology_port(),
            host.data_runtime.clone(),
            execution.catalog_prune.clone(),
        );
        if let Err(error) = catalog_prune.start() {
            return Err(host
                .cleanup_open_error(FrontendApplicationError::new(
                    FrontendApplicationErrorKind::CatalogApplicationServiceOpen,
                    error,
                ))
                .await);
        }
        host.catalog_prune = Some(catalog_prune);

        // Abandoned write attempts hold a capacity slot and a row of provider
        // evidence until someone hands them back. The attempt contract makes
        // that a host obligation on purpose, so this is where the obligation is
        // met; without it the mechanism has no caller and the instance leaks.
        if let Some(store) = host.state_store() {
            let sweeper =
                std::sync::Arc::new(crate::state_store::AbandonedAttemptSweeper::new(store));
            if let Err(error) = sweeper.start() {
                return Err(host
                    .cleanup_open_error(FrontendApplicationError::new(
                        FrontendApplicationErrorKind::StateStoreHost,
                        error,
                    ))
                    .await);
            }
            host.abandoned_attempt_sweeper = Some(sweeper);
        }
        host.statistics_service = Some(Arc::new(FrontendStatisticsService::new()));
        let statistics = host.statistics_service();
        host.dml_service = Some(Arc::new(DmlService::new(statistics)));
        // Local views are process runtime state, so this service has nothing to
        // load and no store to fail against.
        host.view_service = Some(Arc::new(FrontendViewService::new()));
        let table_maintenance_open = FrontendTableMaintenanceService::open(
            host.durable(),
            tokio::runtime::Handle::current(),
        )
        .await
        .map(|service| {
            service
                .with_lake_publication_runtime_policy(host.lake_publication_runtime_policy())
                .with_workload_lifecycle((*host.serving_lifecycle()).clone())
        });
        match table_maintenance_open {
            Ok(service) => host.table_maintenance_service = Some(Arc::new(service)),
            Err(error) => {
                let error = FrontendApplicationError::new(
                    FrontendApplicationErrorKind::TableMaintenanceServiceOpen,
                    error,
                );
                return Err(host.cleanup_open_error(error).await);
            }
        }
        // The coordinator owns the immutable execution and connector-control
        // context consumed by frontend application services. Install it before
        // constructing those services so MV refresh never observes an
        // all-in-one-only direct execution fallback.
        host.optimizer_query_mem_limit_bytes = execution.optimizer_query_mem_limit_bytes();
        host.lake_publication_runtime_policy = execution.lake_publication_runtime_policy();
        if let Err(error) = host.open_coordinator(execution.clone()) {
            return Err(host.cleanup_open_error(error).await);
        }
        match host.state_store() {
            Some(store) => match StateStoreMvRepository::open(store, host.run_policy()).await {
                Ok(repository) => {
                    let repository: Arc<dyn crate::mv::domain::repository::MvRepository> =
                        repository;
                    let provider_activation =
                        Arc::new(FrontendMvRefreshProviderActivationPort::new());
                    let service = Arc::new(
                        FrontendMvService::with_refresh_dependencies(
                            Arc::clone(&repository),
                            host.query_execution_service(),
                            Arc::clone(&host.connector_control)
                                as Arc<dyn novarocks_spi::connector::ConnectorControlRegistry>,
                            Arc::clone(&provider_activation),
                            host.execution_role,
                            host.backend_topology_port(),
                            execution.mv_scheduler.clone(),
                            execution.mv_maintenance.clone(),
                            host.table_maintenance_service(),
                            host.optimizer_query_mem_limit_bytes,
                            Duration::from_secs(30 * 60),
                        )
                        .with_workload_lifecycle((*host.serving_lifecycle()).clone()),
                    );
                    let application_service: Arc<
                        dyn crate::mv::domain::application::MvApplicationService,
                    > = Arc::clone(&service)
                        as Arc<dyn crate::mv::domain::application::MvApplicationService>;
                    host.mv_background_engine_sink = Some(
                        FrontendMvService::background_engine_sink(Arc::clone(&service)),
                    );
                    host.mv_refresh_provider_activation = Some(provider_activation);
                    host.mv_application_service = Some(application_service);
                    host.mv_service = Some(service);
                    host.mv_repository = Some(repository);
                }
                Err(error) => {
                    return Err(host
                        .cleanup_open_error(FrontendApplicationError::new(
                            FrontendApplicationErrorKind::MvServiceOpen,
                            error,
                        ))
                        .await);
                }
            },
            None => {
                return Err(host
                    .cleanup_open_error(FrontendApplicationError::new(
                        FrontendApplicationErrorKind::MvServiceOpen,
                        "frontend MV Accelerator requires StateStore",
                    ))
                    .await);
            }
        }
        if let Err(error) = host.topology().start_heartbeat_manager().map_err(|error| {
            FrontendApplicationError::new(FrontendApplicationErrorKind::ClusterBackendOpen, error)
        }) {
            return Err(host.cleanup_open_error(error).await);
        }
        host.statistics_application_service = Some(Arc::new(StatisticsApplicationService::new()));
        let statistics_application_port = Ok(FrontendStatisticsApplicationPort::new(
            host.statistics_application_service().as_ref().clone(),
            tokio::runtime::Handle::current(),
        )
        .with_workload_lifecycle((*host.serving_lifecycle()).clone()));
        match statistics_application_port {
            Ok(port) => host.statistics_application_port = Some(Arc::new(port)),
            Err(error) => return Err(host.cleanup_open_error(error).await),
        }

        Ok(host)
    }

    pub fn view_service(&self) -> Arc<dyn crate::view::ViewService> {
        Arc::clone(
            self.view_service
                .as_ref()
                .expect("frontend view service is installed before host open returns"),
        )
    }

    pub fn statistics_service(&self) -> Arc<FrontendStatisticsService> {
        self.statistics_service
            .as_ref()
            .expect("frontend statistics service is installed before host open returns")
            .clone()
    }

    pub fn dml_service(&self) -> Arc<DmlService> {
        Arc::clone(
            self.dml_service
                .as_ref()
                .expect("frontend DML service is installed before host open returns"),
        )
    }

    pub fn statistics_application_service(&self) -> Arc<StatisticsApplicationService> {
        Arc::clone(
            self.statistics_application_service
                .as_ref()
                .expect("statistics application service is installed before host open returns"),
        )
    }

    pub fn statistics_application_port(&self) -> Arc<FrontendStatisticsApplicationPort> {
        Arc::clone(
            self.statistics_application_port
                .as_ref()
                .expect("statistics application port is installed before host open returns"),
        )
    }

    pub fn catalog_application_port(
        &self,
    ) -> Arc<dyn crate::catalog_application::CatalogApplicationPort> {
        let application = Arc::clone(
            self.catalog_application_port
                .as_ref()
                .expect("catalog application port is installed before host open returns"),
        ) as Arc<dyn crate::catalog_application::CatalogApplicationPort>;
        self.catalog_runtime_projection
            .bind_application(application)
    }

    /// The publication set Core binds its query catalog registry to.
    ///
    /// It is handed out alongside `catalog_application_port` so a durable
    /// attachment only becomes a resolvable SQL name after this process
    /// published its exact local runtime generation.
    pub fn catalog_runtime_projection(
        &self,
    ) -> Arc<crate::catalog_application::CatalogRuntimeProjection> {
        Arc::clone(&self.catalog_runtime_projection)
    }

    /// FE-local serving lifecycle shared by SQL and background admission
    /// owners. Server orchestration alone owns its Ready/Draining transitions.
    pub fn serving_lifecycle(&self) -> Arc<FrontendServingLifecycle> {
        Arc::clone(&self.serving_lifecycle)
    }

    /// Opens both the legacy serving gate and the new governed root gate after
    /// Server composition has installed every required service.
    pub(crate) fn mark_ready(&self) -> Result<(), FrontendApplicationError> {
        self.execution_runtime_owner.mark_ready().map_err(|error| {
            FrontendApplicationError::server(format!(
                "mark frontend workload authority ready after bootstrap: {error}"
            ))
        })?;
        if let Err(error) = self.serving_lifecycle.mark_ready() {
            self.execution_runtime_owner.close_admission();
            return Err(FrontendApplicationError::server(format!(
                "mark frontend serving lifecycle ready after bootstrap: {error:?}"
            )));
        }
        Ok(())
    }

    pub fn table_maintenance_service(&self) -> Arc<dyn TableMaintenanceService> {
        Arc::clone(
            self.table_maintenance_service
                .as_ref()
                .expect("frontend table-maintenance service is installed before host open returns"),
        )
    }

    pub fn mv_repository(&self) -> Arc<dyn crate::mv::domain::repository::MvRepository> {
        Arc::clone(
            self.mv_repository
                .as_ref()
                .expect("frontend MV repository is installed before host open returns"),
        )
    }

    pub fn mv_application_service(
        &self,
    ) -> Arc<dyn crate::mv::domain::application::MvApplicationService> {
        Arc::clone(
            self.mv_application_service
                .as_ref()
                .expect("frontend MV application service is installed before host open returns"),
        )
    }

    pub fn mv_service(&self) -> Arc<FrontendMvService> {
        Arc::clone(
            self.mv_service
                .as_ref()
                .expect("frontend MV service is installed before host open returns"),
        )
    }

    pub fn mv_refresh_provider_activation_sink(
        &self,
    ) -> Option<Arc<dyn crate::query_execution::mv_native_write::MvRefreshProviderActivationSink>>
    {
        self.mv_refresh_provider_activation.as_ref().map(|port| {
            Arc::clone(port) as Arc<dyn crate::query_execution::mv_native_write::MvRefreshProviderActivationSink>
        })
    }

    pub(crate) fn mv_background_engine_sink(
        &self,
    ) -> Option<Arc<dyn crate::mv::background::MvBackgroundEngineSink>> {
        self.mv_background_engine_sink.as_ref().map(Arc::clone)
    }

    pub fn state_store(&self) -> Option<Arc<dyn StateStore>> {
        self.state_store_host
            .as_ref()
            .and_then(StateStoreHost::state_store)
    }

    /// The policy this application applies to its own durable operations.
    ///
    /// Falls back to the built-in default when no store is configured, so a
    /// consumer built without durable storage still has a coherent policy
    /// rather than an absent one.
    pub fn run_policy(&self) -> crate::state_store::StateStoreRunPolicy {
        self.state_store_host
            .as_ref()
            .map(StateStoreHost::run_policy)
            .unwrap_or_default()
    }

    /// The store together with the policy governing its use.
    ///
    /// Durable consumers take the pair, so none of them can be constructed
    /// holding storage without an agreed retry budget for it.
    pub fn durable(
        &self,
    ) -> Option<(Arc<dyn StateStore>, crate::state_store::StateStoreRunPolicy)> {
        self.state_store_host
            .as_ref()
            .and_then(StateStoreHost::durable)
    }

    pub fn execution_role(&self) -> novarocks_types::ClusterRole {
        self.execution_role
    }

    /// Cost budget frozen from `[runtime]`, handed to statement admission.
    pub fn optimizer_query_mem_limit_bytes(&self) -> u64 {
        self.optimizer_query_mem_limit_bytes
    }

    pub fn lake_publication_runtime_policy(&self) -> LakePublicationRuntimePolicy {
        self.lake_publication_runtime_policy
    }

    pub fn function_catalog(&self) -> Arc<novarocks_functions::EngineFunctionCatalog> {
        Arc::clone(&self.function_catalog)
    }

    pub fn connector_control_registry(
        &self,
    ) -> Arc<dyn novarocks_spi::connector::ConnectorControlRegistry> {
        Arc::clone(&self.connector_control)
            as Arc<dyn novarocks_spi::connector::ConnectorControlRegistry>
    }

    /// Typed-read planning is carried by the same complete control host
    /// generation as generic planning. There is no parallel registry.
    pub fn typed_connector_control(&self) -> Arc<ConnectorControlHost> {
        Arc::clone(&self.connector_control)
    }

    pub fn state_store_provider_id(&self) -> Option<StateStoreProviderId> {
        self.state_store_host
            .as_ref()
            .map(StateStoreHost::provider_id)
    }

    pub fn query_execution_service(&self) -> QueryExecutionService {
        self.query_execution
            .as_ref()
            .expect("frontend query execution service is installed before host open returns")
            .clone()
    }

    /// Cloneable query handle for the one process-owned result decode runtime.
    #[allow(
        dead_code,
        reason = "The T08 coordinator cutover consumes this process-owned query handle."
    )]
    pub(crate) fn result_decode_runtime(&self) -> RootResultDecodeRuntime {
        self.execution_runtime_owner.decode_runtime()
    }

    #[allow(
        dead_code,
        reason = "The T08-C3 SQL cutover consumes this bounded start handle."
    )]
    pub(crate) fn logical_execution_client(&self) -> QueryExecutionClient {
        self.execution_runtime_owner.logical_execution_client()
    }

    #[allow(
        dead_code,
        reason = "The T08-C3 SQL cutover consumes this governed root admission handle."
    )]
    pub(crate) fn workload_root_admission(&self) -> RootAdmissionHandle {
        self.execution_runtime_owner.root_admission()
    }

    #[allow(
        dead_code,
        reason = "Management observation is wired after the T08-C3 execution cutover."
    )]
    pub(crate) fn workload_observation(&self) -> WorkloadObservationHandle {
        self.execution_runtime_owner.workload_observation()
    }

    #[allow(
        dead_code,
        reason = "The T08-C3 result path consumes this local resource authority."
    )]
    pub(crate) fn workload_resources(&self) -> LocalResourceAuthority {
        self.execution_runtime_owner.resources()
    }

    pub fn query_control_service(&self) -> crate::query_execution::control::QueryControlService {
        self.query_control.clone()
    }

    pub(crate) fn backend_membership_ingress(&self) -> Arc<ClusterBackendService> {
        Arc::clone(self.topology())
    }

    pub(crate) fn backend_island_snapshot_reader(
        &self,
    ) -> Arc<dyn crate::topology::BackendIslandSnapshotReader> {
        Arc::clone(self.topology()) as Arc<dyn crate::topology::BackendIslandSnapshotReader>
    }

    pub fn start_report_server(
        &self,
        bind_addr: std::net::SocketAddr,
        native_trust: Arc<NativeTrust>,
        native_transport: FrontendNativeTransport,
    ) -> Result<crate::native::report_server::FrontendReportServerHandle, FrontendApplicationError>
    {
        crate::native::report_server::FrontendReportServerHandle::start(
            bind_addr,
            self.backend_membership_ingress(),
            self.lifecycle_convergence_reader(),
            native_trust,
            native_transport,
        )
        .map_err(FrontendApplicationError::server)
    }

    pub(crate) fn start_report_server_from_host(
        &self,
        host: &str,
        port: u16,
        native_trust: Arc<NativeTrust>,
        native_transport: FrontendNativeTransport,
    ) -> Result<crate::native::report_server::FrontendReportServerHandle, FrontendApplicationError>
    {
        crate::native::report_server::FrontendReportServerHandle::start_from_host(
            host,
            port,
            self.backend_membership_ingress(),
            self.lifecycle_convergence_reader(),
            native_trust,
            native_transport,
        )
        .map_err(FrontendApplicationError::server)
    }

    pub(crate) fn lifecycle_convergence_reader(&self) -> Arc<dyn QueryLifecycleConvergenceReader> {
        self.coordinator
            .as_ref()
            .expect("frontend coordinator is installed before host open returns")
            .convergence_reader()
    }

    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "Retained as the integration-test entry point for the frontend coordinator."
    )]
    pub(crate) fn execute_distributed_query_for_test(
        &self,
        request: crate::query_execution::contract::DistributedQueryRequest,
    ) -> Result<
        crate::query_execution::contract::DistributedQueryOutcome,
        crate::query_execution::contract::DistributedQueryError,
    > {
        crate::query_execution::contract::DistributedQueryCoordinator::execute(
            self.coordinator
                .as_ref()
                .expect("frontend coordinator is installed before host open returns")
                .as_ref(),
            request,
        )
    }

    /// Frontend composition-time topology leaf used by FE-owned services.
    pub fn backend_topology_port(&self) -> crate::common::backend_topology::BackendTopologyService {
        Arc::clone(self.topology()) as crate::common::backend_topology::BackendTopologyService
    }

    pub async fn shutdown(&mut self) -> Result<(), FrontendApplicationError> {
        self.shutdown_until(Instant::now() + STATE_STORE_SHUTDOWN_TIMEOUT)
            .await
    }

    /// Releases FE-owned resources under one absolute deadline. Individual
    /// cleanup owners must not start a fresh full timeout after another owner
    /// has already consumed the shutdown budget.
    pub async fn shutdown_until(
        &mut self,
        deadline: Instant,
    ) -> Result<(), FrontendApplicationError> {
        self.release_resources_until(deadline)
            .await
            .map_err(|error| {
                FrontendApplicationError::new(FrontendApplicationErrorKind::Shutdown, error)
            })
    }

    /// Releases process-local join ownership after bounded graceful shutdown
    /// failed and the production runner has irrevocably committed to FE process
    /// exit. This is not a reusable shutdown path: admission remains closed and
    /// the Host must be dropped immediately after the runner returns.
    pub(crate) fn abandon_for_process_exit(&mut self) {
        self.serving_lifecycle.mark_stopping();
        if let Some(service) = self.mv_service.as_ref() {
            service.request_background_stop_for_process_exit();
        }
        if let Some(port) = self.statistics_application_port.as_ref() {
            port.request_worker_stop_for_process_exit();
        }
        if let Some(service) = self.table_maintenance_service.as_ref() {
            service.request_shutdown_for_process_exit();
        }
        if let Some(topology) = self.topology.as_ref() {
            if let Err(error) = topology.request_heartbeat_stop_for_process_exit() {
                tracing::error!(
                    %error,
                    "failed to signal frontend heartbeat manager before process exit"
                );
            }
        }
        self.execution_runtime_owner.abandon_for_process_exit();
    }

    async fn open_configured(
        &mut self,
        input: StateStoreHostInput,
        registry: &StateStoreProviderRegistry,
    ) -> Result<(), FrontendApplicationError> {
        self.state_store_host = Some(
            StateStoreHost::open(registry, input, Instant::now() + STATE_STORE_OPEN_TIMEOUT)
                .await
                .map_err(|error| {
                    FrontendApplicationError::new(
                        FrontendApplicationErrorKind::StateStoreHost,
                        error,
                    )
                })?,
        );

        Ok(())
    }

    fn open_coordinator(
        &mut self,
        execution: FrontendExecutionConfig,
    ) -> Result<(), FrontendApplicationError> {
        let native_compatibility_id = execution.native_compatibility_id();
        let coordinator = Arc::new(
            FrontendDistributedQueryCoordinator::new(
                execution.runtime_filter_worker_count,
                native_compatibility_id,
                execution.task_update_retry_policy,
                execution.connector_split_initial_dynamic_filter_wait_cap,
                execution.coordination_budgets,
                execution.transport_budget,
                execution.result_fetch_byte_limit,
                self.backend_topology_port(),
                self.data_runtime.clone(),
            )
            .map_err(FrontendApplicationError::server)?,
        );
        self.query_execution = Some(QueryExecutionService::new(coordinator.clone()));
        self.coordinator = Some(coordinator);
        Ok(())
    }

    async fn cleanup_open_error(
        &mut self,
        primary: FrontendApplicationError,
    ) -> FrontendApplicationError {
        match self
            .release_resources_until(Instant::now() + STATE_STORE_SHUTDOWN_TIMEOUT)
            .await
        {
            Ok(()) => primary,
            Err(cleanup_error) => primary.with_cleanup_context(cleanup_error),
        }
    }

    async fn release_resources_until(&mut self, deadline: Instant) -> Result<(), String> {
        self.serving_lifecycle.mark_stopping();
        self.execution_runtime_owner.close_admission();
        // The worker owns durable attempt activity and must stop before the
        // coordinator/topology/StateStore it depends on are released.
        let mv_worker_error = match self.mv_service.as_ref() {
            Some(service) => service
                .shutdown_background_workers_until(deadline)
                .await
                .err()
                .map(|error| format!("shutdown frontend MV background workers failed: {error}")),
            None => None,
        };
        let statistics_worker_error = match self.statistics_application_port.as_ref() {
            Some(port) => port
                .shutdown_worker_until(deadline)
                .await
                .err()
                .map(|error| format!("shutdown statistics analyze worker failed: {error}")),
            None => None,
        };
        let preserve_background_owners =
            mv_worker_error.is_some() || statistics_worker_error.is_some();
        let mut primary_error = mv_worker_error;
        if let Some(statistics_worker_error) = statistics_worker_error {
            if let Some(primary) = primary_error.as_mut() {
                primary.push_str(&format!("; cleanup failed: {statistics_worker_error}"));
            } else {
                primary_error = Some(statistics_worker_error);
            }
        }
        if preserve_background_owners {
            // Background workers still own request/topology/StateStore references.
            // Preserve those owners so a caller sees the explicit shutdown
            // failure rather than pretending teardown completed. Query intake
            // is already closed; a later call resumes this same owner graph.
            return Err(primary_error.expect("the MV shutdown error is retained"));
        }
        if let Err(error) = self.execution_runtime_owner.shutdown_until(deadline).await {
            if !self.execution_runtime_owner.is_shutdown_complete() {
                return match primary_error {
                    Some(primary) => Err(format!("{primary}; cleanup failed: {error}")),
                    None => Err(error),
                };
            }
            if let Some(primary) = primary_error.as_mut() {
                primary.push_str(&format!("; cleanup failed: {error}"));
            } else {
                primary_error = Some(error);
            }
        }
        self.query_execution.take();
        self.coordinator.take();
        let table_maintenance_error = match self.table_maintenance_service.as_ref() {
            Some(service) => service.shutdown_until(deadline).await.err().map(|error| {
                format!("shutdown frontend table-maintenance service failed: {error}")
            }),
            None => None,
        };
        if let Some(table_maintenance_error) = table_maintenance_error {
            if let Some(primary) = primary_error.as_mut() {
                primary.push_str(&format!("; cleanup failed: {table_maintenance_error}"));
            } else {
                primary_error = Some(table_maintenance_error);
            }
            return Err(primary_error.expect("table-maintenance shutdown error is retained"));
        }
        let heartbeat_error = match self.topology.as_ref() {
            Some(topology) => topology.stop_heartbeat_manager_until(deadline).await.err(),
            None => None,
        };
        if let Some(heartbeat_error) = heartbeat_error {
            if let Some(primary) = primary_error.as_mut() {
                primary.push_str(&format!("; cleanup failed: {heartbeat_error}"));
            } else {
                primary_error = Some(heartbeat_error);
            }
            return Err(primary_error.expect("heartbeat shutdown error is retained"));
        }
        self.topology.take();
        self.dml_service.take();
        self.table_maintenance_service.take();
        self.statistics_service.take();
        // Process-local job services do not own StateStore job records. Release
        // their workers before closing the host's remaining durable owners.
        self.statistics_application_port.take();
        self.statistics_application_service.take();
        // Stop the cadence before the store closes. The store host performs one
        // final drain of its own, so nothing is lost here and no sweep races the
        // instance going away.
        if let Some(sweeper) = self.abandoned_attempt_sweeper.take() {
            sweeper.shutdown().await;
        }
        let had_catalog_controller = self.catalog_controller.is_some();
        let catalog_controller_error = match self.catalog_controller.as_ref() {
            Some(controller) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    Some(
                        "frontend cleanup deadline elapsed before catalog controller shutdown"
                            .to_string(),
                    )
                } else {
                    tokio::time::timeout(remaining, controller.shutdown())
                        .await
                        .map_err(|_| {
                            "frontend cleanup deadline elapsed shutting down catalog controller"
                                .to_string()
                        })
                        .and_then(|result| result)
                        .err()
                }
            }
            None => None,
        };
        if had_catalog_controller && catalog_controller_error.is_none() {
            self.catalog_controller.take();
        }
        // The controller owns the durable desired-state projection. The prune
        // worker is best effort and may be asleep between rounds, so it must
        // not consume the whole shared cleanup deadline before the controller
        // gets a chance to stop and unpublish that projection.
        if let Some(prune) = self.catalog_prune.take() {
            prune
                .shutdown(deadline.saturating_duration_since(Instant::now()))
                .await;
        }
        if let Some(catalog_controller_error) = catalog_controller_error {
            let error = format!("shutdown catalog controller failed: {catalog_controller_error}");
            if let Some(primary) = primary_error.as_mut() {
                primary.push_str(&format!("; cleanup failed: {error}"));
            } else {
                primary_error = Some(error);
            }
            return Err(primary_error.expect("catalog controller shutdown error is retained"));
        }
        self.catalog_application_port.take();
        self.view_service.take();
        self.mv_application_service.take();
        self.mv_service.take();
        self.mv_refresh_provider_activation.take();
        self.mv_background_engine_sink.take();
        self.mv_repository.take();
        if let Some(host) = self.state_store_host.as_mut() {
            match host.shutdown(deadline).await {
                Ok(()) => {
                    self.state_store_host.take();
                }
                Err(error) => {
                    let host_error = format!("shutdown frontend StateStore host failed: {error}");
                    if let Some(primary) = primary_error.as_mut() {
                        primary.push_str(&format!("; cleanup failed: {host_error}"));
                    } else {
                        primary_error = Some(host_error);
                    }
                }
            }
        }
        primary_error.map_or(Ok(()), Err)
    }

    fn topology(&self) -> &Arc<ClusterBackendService> {
        self.topology
            .as_ref()
            .expect("frontend cluster backend service is installed before host open returns")
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::time::{Duration, Instant};

    use crate::state_store::{
        StateStoreHost, StateStoreProviderRegistration, StateStoreProviderRegistry,
        testing::{
            TEST_STATE_STORE_PROVIDER_ID, input as test_state_store_input,
            registry as test_state_store_registry,
        },
    };
    use async_trait::async_trait;
    use novarocks_state_store_api::{
        StateStoreError, StateStoreErrorKind, StateStoreOpenRequest, StateStoreProviderDescriptor,
        StateStoreProviderFactory, StateStoreProviderInstance,
    };
    use novarocks_workload_control::{ResourceConfig, WorkClass, WorkRequest, WorkloadConfig};

    use super::{
        FrontendApplicationError, FrontendApplicationErrorKind, FrontendApplicationHost,
        FrontendExecutionConfig, FrontendExecutionRuntimeOwner, FrontendNativeTransport,
        LogicalExecutionRowsConfig, LogicalExecutionSupervisorConfig, MaxWait, ResultByteLimit,
        test_native_trust,
    };

    const DESCRIPTOR: StateStoreProviderDescriptor = StateStoreProviderDescriptor::new(
        TEST_STATE_STORE_PROVIDER_ID,
        novarocks_state_store_api::MAX_KEY_BYTES,
    );

    struct FailingFactory;

    #[tokio::test]
    async fn execution_runtime_shutdown_deadline_retains_the_same_workload_owner_for_retry() {
        let mut runtime = FrontendExecutionRuntimeOwner::try_new(
            tokio::runtime::Handle::current(),
            LogicalExecutionSupervisorConfig::new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
                LogicalExecutionRowsConfig::new(
                    NonZeroUsize::new(1).unwrap(),
                    NonZeroU32::new(1).unwrap(),
                    Duration::from_secs(1),
                    MaxWait::new(Duration::from_millis(20)).unwrap(),
                    ResultByteLimit::new(1024).unwrap(),
                ),
            ),
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 20,
                control_bytes: 1 << 10,
                per_scope_bytes: 1 << 18,
            },
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        )
        .expect("execution runtime opens");
        runtime.mark_ready().expect("workload authority ready");
        let work = runtime
            .root_admission()
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("root work admitted");

        let error = runtime
            .shutdown_until(Instant::now() + Duration::from_millis(20))
            .await
            .expect_err("live work must keep the owner graph open");
        assert!(error.contains("workload shutdown deadline exceeded"));

        work.owner.complete();
        work.business.release();
        runtime
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("retry converges the exact retained owner graph");
    }

    #[async_trait]
    impl StateStoreProviderFactory for FailingFactory {
        fn descriptor(&self) -> &StateStoreProviderDescriptor {
            &DESCRIPTOR
        }

        async fn open(
            self: Box<Self>,
            _request: StateStoreOpenRequest,
        ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError> {
            Err(StateStoreError::new(
                StateStoreErrorKind::Corruption,
                "injected provider primary failure",
            )
            .with_cleanup_context(StateStoreError::new(
                StateStoreErrorKind::DeadlineExceeded,
                "injected provider cleanup failure",
            )))
        }
    }

    #[tokio::test]
    async fn frontend_stringification_preserves_host_primary_and_cleanup_context() {
        let mut registry = StateStoreProviderRegistry::new();
        registry
            .register(StateStoreProviderRegistration::new(DESCRIPTOR, |_| {
                Ok(Box::new(FailingFactory))
            }))
            .expect("register diagnostic provider");
        let host_error = match StateStoreHost::open(
            &registry,
            test_state_store_input("diagnostic-cluster"),
            Instant::now() + Duration::from_secs(1),
        )
        .await
        {
            Ok(_) => panic!("injected provider failure must reject host open"),
            Err(error) => error,
        };

        assert_eq!(
            host_error.primary().map(StateStoreError::kind),
            Some(StateStoreErrorKind::Corruption)
        );
        let frontend_error =
            FrontendApplicationError::new(FrontendApplicationErrorKind::StateStoreHost, host_error);
        let diagnostic = frontend_error.to_string();

        assert!(diagnostic.contains("StateStoreHost"));
        assert!(diagnostic.contains("Open (frontend-unit-test)"));
        assert!(diagnostic.contains("injected provider primary failure"));
        assert!(diagnostic.contains("injected provider cleanup failure"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_bootstraps_and_stops_catalog_projection_with_state_store() {
        let state_store = test_state_store_input("catalog-controller-host-test");
        let registry = test_state_store_registry();
        let backend = crate::topology::ClusterBackendOpenConfig::new(
            novarocks_types::ClusterRole::Fe,
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            Duration::from_secs(1),
            1,
            Duration::from_secs(1),
        )
        .expect("valid frontend backend config");
        let mut host = FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
            Some(state_store),
            &registry,
            FrontendExecutionConfig::new_for_test(
                "127.0.0.1",
                0,
                NonZeroUsize::new(1).expect("non-zero runtime-filter workers"),
                novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                std::sync::Arc::new(
                    novarocks_sql::compiler::build_builtin_engine_function_catalog()
                        .expect("builtin function catalog"),
                ),
            ),
            backend,
            Vec::new(),
            tokio::runtime::Handle::current(),
            test_native_trust(),
            FrontendNativeTransport::plaintext(),
        )
        .await
        .expect("host opens with the catalog controller");
        let instance_id =
            novarocks_spi::connector::ConnectorInstanceId::parse("warehouse").expect("instance id");
        assert!(matches!(
            crate::catalog_application::CatalogApplicationPort::admit_catalog(
                host.catalog_application_port().as_ref(),
                &instance_id,
            ),
            crate::catalog_application::CatalogAdmission::Absent
        ));
        host.shutdown().await.expect("host shutdown");
    }
}
