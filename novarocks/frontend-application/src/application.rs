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
#[cfg(test)]
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Handle;

pub(crate) mod workload_progress;
use workload_progress::{FrontendWorkloadDeadlineSupervisor, FrontendWorkloadProgressPolicy};

use crate::query_execution::internal_result_cpu::{InternalResultCpu, InternalResultCpuOwner};
use crate::task_protocol_config::CoordinationBudgets;
use novarocks_native_adapter::FrontendTaskTransportBudget;
use novarocks_query_application::api::QueryExecutionClient;
#[cfg(test)]
use novarocks_query_application::coordination::LogicalExecutionRowsConfig;
use novarocks_query_application::coordination::{
    LogicalExecutionSupervisor, LogicalExecutionSupervisorConfig,
    LogicalExecutionSupervisorShutdownError,
};
use novarocks_query_application::cpu::{
    QueryBlockingExecutor, QueryBlockingExecutorConfig, QueryBlockingExecutorOwner,
    QueryCpuExecutorConfig, QueryCpuExecutorOwner,
};
use novarocks_workload_control::{
    CancellationReason, RootAdmissionHandle, WorkloadConfig, WorkloadControl,
    WorkloadObservationHandle, WorkloadShutdownError,
};

use crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor;
use crate::task_protocol_config::TaskUpdateRetryPolicy;
use novarocks_catalog_application::CatalogAttachmentRepository;
use novarocks_mv_application::maintenance::MaintenanceCoordinatorConfig;
use novarocks_mv_application::scheduler::MvSchedulerConfig;
use novarocks_native_trust::{NativeProcessIdentity, NativeTrust};
use novarocks_spi::connector::ConnectorControlRoleBindingFactory;
use novarocks_state_store_api::{StateStore, StateStoreProviderId};
use novarocks_state_store_runtime::{
    PreparedStateStoreHost, StateStoreHost, StateStoreHostInput, StateStoreProviderRegistry,
    StateStoreRunPolicy, validate_persistent_state_families,
};
use novarocks_types::{FrontendProcessId, NativeCompatibilityId, QueryProcessNamespace};

use crate::catalog_application::MvCatalogReferenceReader;
use crate::catalog_controller::{CatalogProjectionConfig, FrontendCatalogController};
use crate::catalog_prune::{CatalogPruneConfig, FrontendCatalogPruneService};
use crate::native::data_runtime::FrontendDataRuntime;
use crate::query_execution::lifecycle_diagnostics::{
    FrontendLifecycleDiagnostics, QueryLifecycleConvergenceReader,
};
use crate::query_execution::logical_execution::LogicalExecutionLauncher;
use crate::query_execution::native_execution_adapter::{
    FrontendLogicalExecutionNativePort, FrontendNativeLogicalExecutionLauncher,
    FrontendNativeLogicalExecutionRuntime,
};
use crate::topology::{ClusterBackendOpenConfig, ClusterBackendService};
use crate::workload_lifecycle::{
    FrontendCatalogCounts, FrontendCatalogSnapshotIdentity, FrontendCatalogSourceMode,
    FrontendServingLifecycle, FrontendServingSnapshotReader, FrontendServingWorkloadSnapshotReader,
};
use novarocks_catalog_application::ConnectorControlHost;
use novarocks_catalog_application::{
    CatalogApplicationService, CatalogDesiredStateSnapshot, CatalogDesiredStateSource,
    CatalogDesiredStateSourceInput, CatalogDesiredStateSourceMode, CatalogMaterializationConfig,
};
use novarocks_mv_application::state_store_repository::StateStoreMvRepository;
use novarocks_native_adapter::FrontendNativeTransport;
use novarocks_query_application::publication::LakePublicationRuntimePolicy;

const STATE_STORE_OPEN_TIMEOUT: Duration = Duration::from_secs(5);
const STATE_STORE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
/// Cached planning workers retire after one minute without an admitted job.
/// This is a reuse window, not a Frontend CPU or query-capacity limit.
const DEFAULT_QUERY_CPU_IDLE_KEEPALIVE: Duration = Duration::from_secs(60);
const DEFAULT_QUERY_BLOCKING_WORKER_COUNT: NonZeroUsize = NonZeroUsize::new(2).unwrap();
const DEFAULT_QUERY_BLOCKING_QUEUE_CAPACITY: NonZeroUsize = NonZeroUsize::new(32).unwrap();
#[cfg(test)]
const DEFAULT_RESULT_DELIVERY_CAPACITY: NonZeroUsize = NonZeroUsize::new(32).unwrap();
#[cfg(test)]
const DEFAULT_LOGICAL_EXECUTION_MAX_ATTEMPTS: NonZeroU32 = NonZeroU32::new(3).unwrap();
#[cfg(test)]
const DEFAULT_REPLACEMENT_RESERVATION_VALID_FOR: Duration = Duration::from_secs(30);
#[cfg(test)]
const DEFAULT_LOGICAL_EXECUTION_MAILBOX_CAPACITY: NonZeroUsize = NonZeroUsize::new(64).unwrap();
#[cfg(test)]
const DEFAULT_LOGICAL_EXECUTION_CONTEXT_ISSUE_CAPACITY: NonZeroUsize =
    NonZeroUsize::new(16).unwrap();
#[cfg(test)]
const DEFAULT_LOGICAL_ABORT_EFFECT_CAPACITY: NonZeroUsize = NonZeroUsize::new(16).unwrap();

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
    StateFamilyRegistration,
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
    QueryCpuExecutorOpen,
    QueryBlockingExecutorOpen,
    InternalResultCpuOpen,
    WorkloadControlOpen,
    Server,
    Shutdown,
}

#[cfg(feature = "mem-1-m07-root-observation")]
struct RootObservationIoSource(std::io::Error);
#[cfg(feature = "mem-1-m07-root-observation")]
impl fmt::Debug for RootObservationIoSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OriginalRootObservationIo { source_retained: true }")
    }
}

#[cfg(feature = "mem-1-m07-closing-pressure")]
enum ClosingPressureRoleSource {
    Fixture(novarocks_mysql_adapter::closing_pressure_fixture::ClosingPressureFixtureError),
    OriginalIo(std::io::Error),
}
#[cfg(feature = "mem-1-m07-closing-pressure")]
impl fmt::Debug for ClosingPressureRoleSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OriginalClosingPressureSource { source_retained: true }")
    }
}
#[cfg(feature = "mem-1-m07-closing-pressure")]
impl ClosingPressureRoleSource {
    fn original(&self) -> &(dyn std::error::Error + 'static) {
        match self {
            Self::Fixture(source) => source,
            Self::OriginalIo(source) => source,
        }
    }
}

#[derive(Debug)]
pub struct FrontendApplicationError {
    kind: FrontendApplicationErrorKind,
    message: String,
    original: Option<novarocks_state_store_api::StateStoreExecutionFailure>,
    original_cleanup: Option<novarocks_state_store_api::StateStoreExecutionFailure>,
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    pressure_failures: [Option<Box<ClosingPressureRoleSource>>; 4],
    #[cfg(feature = "mem-1-m07-root-observation")]
    root_observation_io: Option<Box<RootObservationIoSource>>,
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    fixture_failures: [Option<
        Box<novarocks_mysql_adapter::exact_mysql_write_fixture::MysqlWriteFixtureError>,
    >; 4],
}

impl FrontendApplicationError {
    pub(crate) fn new(kind: FrontendApplicationErrorKind, error: impl fmt::Display) -> Self {
        Self {
            kind,
            message: error.to_string(),
            original: None,
            original_cleanup: None,
            #[cfg(feature = "mem-1-m07-closing-pressure")]
            pressure_failures: std::array::from_fn(|_| None),
            #[cfg(feature = "mem-1-m07-root-observation")]
            root_observation_io: None,
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            fixture_failures: std::array::from_fn(|_| None),
        }
    }

    fn original_state_store(cause: novarocks_state_store_api::StateStoreExecutionFailure) -> Self {
        let mut error = Self::new(FrontendApplicationErrorKind::StateStoreHost, cause.detail());
        error.original = Some(cause);
        error
    }
    fn state_store(error: novarocks_state_store_runtime::StateStoreHostError) -> Self {
        let cause = error.original_failure().cloned();
        let mut result = Self::new(FrontendApplicationErrorKind::StateStoreHost, error);
        result.original = cause;
        result
    }
    pub fn original_state_store_failure(
        &self,
    ) -> Option<&novarocks_state_store_api::StateStoreExecutionFailure> {
        self.original.as_ref()
    }

    fn preserve_original_cleanup(&mut self, cleanup: &Self) {
        if self.original.is_none() {
            self.original = cleanup.original.clone();
        } else if self.original_cleanup.is_none() {
            self.original_cleanup = cleanup
                .original
                .clone()
                .or_else(|| cleanup.original_cleanup.clone());
        }
    }
    pub fn with_application_cleanup(mut self, cleanup: Self) -> Self {
        self.preserve_original_cleanup(&cleanup);
        self.message
            .push_str(&format!("; cleanup failed: {cleanup}"));
        self
    }

    /// Constructs a role-composition failure for the outer Server owner.
    pub fn server(error: impl fmt::Display) -> Self {
        Self::new(FrontendApplicationErrorKind::Server, error)
    }
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    pub(crate) fn server_fixture(
        error: novarocks_mysql_adapter::exact_mysql_write_fixture::MysqlWriteFixtureError,
    ) -> Self {
        let mut result = Self::server(&error);
        result.fixture_failures[0] = Some(Box::new(error));
        result
    }
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    pub(crate) fn server_pressure(
        error: novarocks_mysql_adapter::closing_pressure_fixture::ClosingPressureFixtureError,
    ) -> Self {
        let mut result = Self::server("original Closing pressure fixture failed");
        result.pressure_failures[0] = Some(Box::new(ClosingPressureRoleSource::Fixture(error)));
        result
    }
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    pub(crate) fn server_pressure_registry(error: std::io::Error) -> Self {
        let mut result = Self::server("original Closing pressure connection drain failed");
        result.pressure_failures[0] = Some(Box::new(ClosingPressureRoleSource::OriginalIo(error)));
        result
    }
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    pub(crate) fn server_pressure_binding(error: std::io::Error) -> Self {
        let mut result = Self::server("original Closing pressure listener binding failed");
        result.pressure_failures[0] = Some(Box::new(ClosingPressureRoleSource::OriginalIo(error)));
        result
    }
    #[cfg(feature = "mem-1-m07-root-observation")]
    pub(crate) fn server_root_observation(error: std::io::Error) -> Self {
        let mut result = Self::server("root observation stdout IO failed");
        result.root_observation_io = Some(Box::new(RootObservationIoSource(error)));
        result
    }
    #[cfg(any(
        feature = "mem-1-m07-exact-mysql-write",
        feature = "mem-1-m07-root-observation",
        feature = "mem-1-m07-closing-pressure"
    ))]
    pub fn with_role_cleanup(mut self, cleanup: Self) -> Self {
        self.preserve_original_cleanup(&cleanup);
        self.message
            .push_str(&format!("; cleanup failed: {cleanup}"));
        #[cfg(feature = "mem-1-m07-root-observation")]
        if let Some(source) = cleanup.root_observation_io {
            // Only the one startup emission can supply this original IO source.
            assert!(self.root_observation_io.is_none());
            self.root_observation_io = Some(source);
        }
        #[cfg(feature = "mem-1-m07-closing-pressure")]
        for source in cleanup.pressure_failures.into_iter().flatten() {
            // Startup is mutually exclusive with the four serving verdict stages:
            // control, socket close, registry verification and original-owner finish.
            let slot = self
                .pressure_failures
                .iter_mut()
                .find(|slot| slot.is_none())
                .expect("fixed Closing pressure verdict stages");
            *slot = Some(source);
        }
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        for source in cleanup.fixture_failures.into_iter().flatten() {
            // The fixture has exactly four possible verdict stages: control,
            // socket cleanup, registry verification and original-owner finish.
            let slot = self
                .fixture_failures
                .iter_mut()
                .find(|slot| slot.is_none())
                .expect("fixed MySQL fixture verdict stages");
            *slot = Some(source);
        }
        self
    }

    /// Retains the first role failure while recording bounded cleanup failure.
    pub fn with_cleanup_context(mut self, cleanup_error: impl fmt::Display) -> Self {
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

impl std::error::Error for FrontendApplicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        if let Some(cause) = self.original.as_ref().or(self.original_cleanup.as_ref()) {
            return Some(cause);
        }
        #[cfg(feature = "mem-1-m07-root-observation")]
        if let Some(error) = &self.root_observation_io {
            return Some(&error.0);
        }
        #[cfg(feature = "mem-1-m07-closing-pressure")]
        if let Some(error) = self.pressure_failures.iter().flatten().next() {
            return Some(error.original());
        }
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        if let Some(error) = self.fixture_failures.iter().flatten().next() {
            return Some(error.as_ref());
        }
        None
    }
}

/// Unique owner aggregate for the query-application runtime.
///
/// Keeping all process owners here makes startup and shutdown ownership
/// concrete: services may clone only the narrow capabilities below, never the
/// supervisor, Registry, workload control, or decode join handles.
// Design: ADR-0147 (docs/adr/ADR-0147-process-local-work-governance-separates-responsibility-and-resources.md)
struct FrontendExecutionRuntimeOwner {
    #[cfg(novarocks_dev_controls)]
    namespace: QueryProcessNamespace,
    native_capacity_profile: crate::native_workload::FrontendWorkloadCapacityProfile,
    clock: Arc<dyn novarocks_coordination_contract::MonotonicClock>,
    #[expect(
        dead_code,
        reason = "Retains the process runtime's installed original executor and Provider CPU capabilities at their original owner Drop boundary."
    )]
    connector_blocking_io: ConnectorBlockingIoSupervisor,
    supervisor: LogicalExecutionSupervisor,
    logical_execution_client: QueryExecutionClient,
    lifecycle_diagnostics: Arc<FrontendLifecycleDiagnostics>,
    workload: Option<WorkloadControl>,
    deadline_supervisor: FrontendWorkloadDeadlineSupervisor,
    root_admission: RootAdmissionHandle,
    workload_observation: WorkloadObservationHandle,
    query_cpu: QueryCpuExecutorOwner,
    query_blocking: QueryBlockingExecutorOwner,
    query_blocking_executor: QueryBlockingExecutor,
    internal_result_cpu: InternalResultCpuOwner,
    /// The original strong pool capability outlives the worker owner.
    _retained_internal_result_cpu_runtime: InternalResultCpu,
    /// The complete result-window profile, installed before readiness.
    /// Statements take windows through admission; closing deliveries take
    /// their separate positions here.
    #[allow(dead_code, reason = "closing deliveries acquire through this handle")]
    result_capacity: novarocks_workload_control::ResultCapacityHandle,
    terminal_error: Option<String>,
    shutdown_complete: bool,
    statement_control_installed: bool,
}

pub use crate::query_execution::internal_result_cpu::FrontendResultCapacityProfile;

/// Frozen native-read policy consumed while Server composes one Frontend role
/// graph.
///
/// This belongs with the process execution runtime that supplies the client,
/// decode owner and lifecycle diagnostics. The resulting launcher is held by
/// the immutable role-product graph, never by the lifecycle Host.
#[derive(Clone, Copy)]
struct FrontendQueryRuntimeConfig {
    native_compatibility_id: NativeCompatibilityId,
    runtime_filter_worker_count: NonZeroUsize,
    task_update_retry_policy: TaskUpdateRetryPolicy,
    split_initial_wait_cap: Duration,
    coordination_budgets: CoordinationBudgets,
    transport_budget: FrontendTaskTransportBudget,
    abort_capacity: NonZeroUsize,
}

impl FrontendExecutionRuntimeOwner {
    fn try_new(
        runtime: Handle,
        connector_blocking_io: ConnectorBlockingIoSupervisor,
        frontend_process_id: FrontendProcessId,
        supervisor_config: LogicalExecutionSupervisorConfig,
        workload_config: WorkloadConfig,
        result_profile: FrontendResultCapacityProfile,
        query_cpu_config: QueryCpuExecutorConfig,
        query_blocking_config: QueryBlockingExecutorConfig,
        native_capacity_profile: crate::native_workload::FrontendWorkloadCapacityProfile,
    ) -> Result<Self, FrontendApplicationError> {
        native_capacity_profile
            .validate_pool_declarations(&workload_config, query_blocking_config)
            .map_err(|error| {
                FrontendApplicationError::new(
                    FrontendApplicationErrorKind::WorkloadControlOpen,
                    error,
                )
            })?;
        let mut workload =
            WorkloadControl::try_new_counted(workload_config.clone()).map_err(|error| {
                FrontendApplicationError::new(
                    FrontendApplicationErrorKind::WorkloadControlOpen,
                    error,
                )
            })?;
        // The frozen result profile must cover computation admission; a
        // concurrency limit beyond its client positions is refused here.
        let result_capacity = workload
            .owner
            .configure_result_capacity(result_profile.declared())
            .map_err(|error| {
                FrontendApplicationError::new(
                    FrontendApplicationErrorKind::WorkloadControlOpen,
                    format!("install result window capacity: {error}"),
                )
            })?;
        let query_cpu = QueryCpuExecutorOwner::try_new(query_cpu_config).map_err(|error| {
            FrontendApplicationError::new(FrontendApplicationErrorKind::QueryCpuExecutorOpen, error)
        })?;
        let query_cpu_executor = query_cpu.executor();
        let query_blocking =
            QueryBlockingExecutorOwner::try_new(query_blocking_config).map_err(|error| {
                FrontendApplicationError::new(
                    FrontendApplicationErrorKind::QueryBlockingExecutorOpen,
                    error,
                )
            })?;
        let query_blocking_executor = query_blocking.executor();
        connector_blocking_io
            .install_original_executors(query_cpu_executor, query_blocking_executor.clone())
            .map_err(|error| {
                FrontendApplicationError::new(
                    FrontendApplicationErrorKind::WorkloadControlOpen,
                    format!("install original connector executors: {error}"),
                )
            })?;
        let internal_result_cpu =
            InternalResultCpuOwner::try_new(query_blocking_config, &mut workload.owner).map_err(
                |error| {
                    FrontendApplicationError::new(
                        FrontendApplicationErrorKind::InternalResultCpuOpen,
                        error,
                    )
                },
            )?;
        let internal_result_cpu_runtime = internal_result_cpu.runtime();
        connector_blocking_io
            .install_provider_creator_cpu(internal_result_cpu_runtime.clone())
            .map_err(|error| {
                FrontendApplicationError::new(
                    FrontendApplicationErrorKind::InternalResultCpuOpen,
                    error.to_string(),
                )
            })?;
        // Actual pool/factory capture precedes starting the logical-role service.
        let native_capacity_sources = native_capacity_profile
            .capture_sources(
                &workload_config,
                result_profile,
                &query_blocking_executor,
                &internal_result_cpu_runtime,
            )
            .map_err(|error| {
                FrontendApplicationError::new(
                    FrontendApplicationErrorKind::WorkloadControlOpen,
                    error,
                )
            })?;
        let progress_policy = FrontendWorkloadProgressPolicy::validate(
            native_capacity_profile.policy().recheck_interval(),
        )
        .map_err(|error| {
            FrontendApplicationError::new(FrontendApplicationErrorKind::WorkloadControlOpen, error)
        })?;
        let (_joint_admission, joint_driver) = native_capacity_profile
            .install(&mut workload.owner, &native_capacity_sources)
            .map_err(|error| {
                FrontendApplicationError::new(
                    FrontendApplicationErrorKind::WorkloadControlOpen,
                    error,
                )
            })?;
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
        let lifecycle_diagnostics = Arc::new(FrontendLifecycleDiagnostics::default());
        let clock: Arc<dyn novarocks_coordination_contract::MonotonicClock> =
            Arc::new(novarocks_query_application::coordination::ProcessMonotonicClock::new());
        let (supervisor, logical_execution_client) = LogicalExecutionSupervisor::try_with_clock(
            &mut workload.owner,
            0,
            native_capacity_profile.policy().turn_budget(),
            runtime.clone(),
            Arc::new(FrontendLogicalExecutionNativePort),
            namespace,
            frontend_process_id,
            supervisor_config,
            Arc::clone(&clock),
        )
        .map_err(|error| {
            FrontendApplicationError::new(FrontendApplicationErrorKind::WorkloadControlOpen, error)
        })?;
        let deadline_supervisor = FrontendWorkloadDeadlineSupervisor::start(
            &runtime,
            workload.owner.deadline_expiry_handle(),
            joint_driver,
            progress_policy,
        );
        Ok(Self {
            native_capacity_profile,
            #[cfg(novarocks_dev_controls)]
            namespace,
            clock,
            connector_blocking_io,
            supervisor,
            logical_execution_client,
            lifecycle_diagnostics,
            workload: Some(workload.owner),
            deadline_supervisor,
            root_admission: workload.root_admission,
            workload_observation: workload.observation,
            query_cpu,
            query_blocking,
            query_blocking_executor,
            internal_result_cpu,
            _retained_internal_result_cpu_runtime: internal_result_cpu_runtime,
            result_capacity,
            terminal_error: None,
            shutdown_complete: false,
            statement_control_installed: false,
        })
    }

    fn mark_ready(&self) -> Result<(), novarocks_workload_control::WorkError> {
        if self.deadline_supervisor.has_exited()
            || self.supervisor.service_has_exited()
            || self.supervisor.process_failure().is_some()
        {
            self.close_admission();
            return Err(novarocks_workload_control::WorkError::Closed);
        }
        self.workload
            .as_ref()
            .expect("workload owner exists until execution runtime shutdown")
            .mark_ready()
    }

    fn close_root_admission(&self) {
        if let Some(workload) = self.workload.as_ref() {
            workload.close_root_admission();
        }
    }

    fn close_admission(&self) {
        if let Some(workload) = self.workload.as_ref() {
            workload.close_admission();
        }
    }

    fn cancel_active_roots_at_drain_deadline(&self) -> usize {
        self.workload.as_ref().map_or(0, |workload| {
            workload.cancel_active_roots(CancellationReason::FrontendDrainDeadlineExceeded)
        })
    }

    async fn drain_workload_until(&mut self, deadline: Instant) -> Result<(), String> {
        self.close_admission();
        if self.shutdown_complete {
            return Ok(());
        }

        // Once the same WC has actually drained, later executor-join retries
        // must not classify the already requested service stop as a failure.
        if self.workload.is_some() {
            let mut registry_errors = 0;
            loop {
                let outcome = tokio::select! {
                    outcome = self.supervisor.shutdown_until(deadline) => outcome,
                    _ = std::future::poll_fn(|cx| self.deadline_supervisor.poll_exit(cx)) => {
                        return Err("frontend joint progress service terminated before workload drain".to_string());
                    }
                };
                match outcome {
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

            self.shutdown_workload_until(deadline).await?;
        }
        self.deadline_supervisor
            .shutdown_after_workload_drained()
            .await
            .map_err(|_| "frontend joint progress service failed before actual join".to_string())?;

        Ok(())
    }

    #[cfg(test)]
    async fn shutdown_until(&mut self, deadline: Instant) -> Result<(), String> {
        self.drain_workload_until(deadline).await?;
        self.shutdown_executors_until(deadline).await
    }

    /// Durable Host tails may still need the original ordinary executor after
    /// statement Roots have drained. Join it only after those tails actually
    /// close, using the same Host deadline and the same unique process owners.
    async fn shutdown_executors_until(&mut self, deadline: Instant) -> Result<(), String> {
        self.query_cpu.shutdown_until(deadline).await?;

        self.query_blocking.shutdown_until(deadline).await?;

        self.internal_result_cpu.shutdown_until(deadline).await?;

        self.shutdown_complete = true;
        self.terminal_error.take().map_or(Ok(()), Err)
    }

    fn abandon_for_process_exit(&mut self) {
        self.close_admission();
        self.deadline_supervisor.abort_for_process_exit();
        self.supervisor.abandon_for_process_exit();
        self.query_cpu.request_shutdown_for_process_exit();
        self.query_blocking.request_shutdown_for_process_exit();
        self.internal_result_cpu.request_shutdown_for_process_exit();
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
            if self.deadline_supervisor.has_exited() {
                let _ = std::future::poll_fn(|cx| self.deadline_supervisor.poll_exit(cx)).await;
                return Err(
                    "frontend joint progress service terminated before workload drain".to_string(),
                );
            }
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
                    // `wait_progress` may resolve immediately when a
                    // concurrent owner/control transition advanced the
                    // revision. Check the shared deadline between shutdown
                    // attempts as well, so a stream of immediate revisions
                    // cannot bypass the bounded process-exit contract.
                    if Instant::now() >= deadline {
                        return Err(
                            "frontend workload shutdown deadline exceeded before drain".to_string()
                        );
                    }
                }
            }

            // The logical supervisor has already driven every query owner to
            // its terminal boundary before workload teardown begins. Consume
            // the matching role-owned control notifications now: they record
            // that cancellation was delivered, but cannot manufacture task,
            // context, or resource-release facts.
            while let Some(control) = self
                .workload
                .as_ref()
                .expect("failed workload shutdown returns the exact owner")
                .next_control()
            {
                control.acknowledge();
            }

            // Control traversal can advance the revision even when the next
            // wait is immediately ready. Yield so the SAME Host can poll its
            // original failed-subscription recovery alongside this drain.
            tokio::task::yield_now().await;

            let wait = self
                .workload
                .as_ref()
                .expect("failed workload shutdown returns the exact owner")
                .wait_progress(revision);
            tokio::select! {
                _ = wait => {},
                _ = std::future::poll_fn(|cx| self.deadline_supervisor.poll_exit(cx)) => {
                    return Err("frontend joint progress service terminated before workload drain".to_string());
                }
                _ = tokio::time::sleep_until(deadline.into()) => {
                    return Err("frontend workload shutdown deadline exceeded before drain".to_string());
                }
            }
        }
    }

    fn logical_execution_client(&self) -> QueryExecutionClient {
        self.logical_execution_client.clone()
    }

    fn lifecycle_diagnostics(&self) -> Arc<FrontendLifecycleDiagnostics> {
        Arc::clone(&self.lifecycle_diagnostics)
    }

    fn root_admission(&self) -> RootAdmissionHandle {
        self.root_admission.clone()
    }

    fn workload_observation(&self) -> WorkloadObservationHandle {
        self.workload_observation.clone()
    }

    fn query_blocking_executor(&self) -> QueryBlockingExecutor {
        self.query_blocking_executor.clone()
    }

    fn is_shutdown_complete(&self) -> bool {
        self.shutdown_complete
    }
}

/// The catalog-specific mutable role state that is transferred to the
/// completed Frontend role graph before SQL admission opens.
///
/// Bootstrap installs this owner while the Host still owns reverse cleanup.
/// Once Server has assembled every other role product, the owner moves as one
/// value into that graph.  The query catalog projection deliberately remains
/// outside: it is query-side state bound through the publisher port below.
pub(crate) struct FrontendCatalogRoleRuntime {
    connector_control: Arc<ConnectorControlHost>,
    application: Arc<CatalogApplicationService>,
    controller: Option<Arc<FrontendCatalogController>>,
    prune: Option<Arc<FrontendCatalogPruneService>>,
}

impl FrontendCatalogRoleRuntime {
    fn new(
        connector_control: Arc<ConnectorControlHost>,
        application: Arc<CatalogApplicationService>,
    ) -> Self {
        Self {
            connector_control,
            application,
            controller: None,
            prune: None,
        }
    }

    pub(crate) fn catalog_application_port(
        &self,
    ) -> Arc<dyn novarocks_catalog_application::CatalogApplicationPort> {
        Arc::clone(&self.application)
            as Arc<dyn novarocks_catalog_application::CatalogApplicationPort>
    }

    pub(crate) fn connector_control_registry(
        &self,
    ) -> Arc<dyn novarocks_spi::connector::ConnectorControlRegistry> {
        Arc::clone(&self.connector_control)
            as Arc<dyn novarocks_spi::connector::ConnectorControlRegistry>
    }

    pub(crate) fn typed_connector_control(&self) -> Arc<ConnectorControlHost> {
        Arc::clone(&self.connector_control)
    }

    pub(crate) async fn shutdown_until(&mut self, deadline: Instant) -> Result<(), String> {
        let had_catalog_controller = self.controller.is_some();
        let catalog_controller_error = match self.controller.as_ref() {
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
            self.controller.take();
        }
        // The controller owns the durable desired-state projection. The prune
        // worker is best effort and may be asleep between rounds, so it must
        // not consume the whole shared cleanup deadline before the controller
        // gets a chance to stop and unpublish that projection.
        if let Some(prune) = self.prune.take() {
            prune
                .shutdown(deadline.saturating_duration_since(Instant::now()))
                .await;
        }
        if let Some(error) = catalog_controller_error {
            Err(format!("shutdown catalog controller failed: {error}"))
        } else {
            Ok(())
        }
    }

    pub(crate) fn request_stop_for_process_exit(&self) {
        if let Some(controller) = self.controller.as_ref() {
            controller.request_stop_for_process_exit();
        }
        if let Some(prune) = self.prune.as_ref() {
            prune.request_stop_for_process_exit();
        }
    }
}

mod state_store_opening;
use state_store_opening::FrontendStateStoreOpening;

struct FrontendStartupInput {
    backend: ClusterBackendOpenConfig,
    connector_control: Arc<ConnectorControlHost>,
    catalog_source_input: CatalogDesiredStateSourceInput,
    catalog_materialization: CatalogMaterializationConfig,
    catalog_projection: CatalogProjectionConfig,
    catalog_prune: CatalogPruneConfig,
}

pub struct FrontendApplicationHost {
    #[cfg(novarocks_dev_controls)]
    pub(crate) test_session: Option<crate::test_session::FrontendTestSession>,
    catalog_role_runtime: Option<FrontendCatalogRoleRuntime>,
    catalog_runtime_projection: Arc<crate::catalog_application::CatalogRuntimeProjection>,
    serving_lifecycle: Arc<FrontendServingLifecycle>,
    /// Meets the attempt contract's host obligation to return abandoned
    /// attempts; see `state_store::sweeper`.
    abandoned_attempt_sweeper: Option<crate::state_store::AbandonedAttemptSweeper>,
    mv_repository:
        Option<Arc<novarocks_mv_application::state_store_repository::StateStoreMvRepository>>,
    state_store_host: Option<StateStoreHost>,
    state_store_opening: Option<FrontendStateStoreOpening>,
    startup: Option<FrontendStartupInput>,
    query_runtime: FrontendQueryRuntimeConfig,
    execution_runtime_owner: FrontendExecutionRuntimeOwner,
    execution_role: novarocks_types::ClusterRole,
    data_runtime: FrontendDataRuntime,
    topology: Option<Arc<ClusterBackendService>>,
    optimizer_query_mem_limit_bytes: u64,
    lake_publication_runtime_policy: LakePublicationRuntimePolicy,
    mv_scheduler_config: MvSchedulerConfig,
    mv_maintenance_config: MaintenanceCoordinatorConfig,
    mv_remote_effect_policy: novarocks_mv_application::management::RemoteEffectPolicy,
    mv_management_audit: Option<Arc<dyn novarocks_mv_application::management::ManagementAuditSink>>,
    mv_startup_isolation: Option<crate::mv::startup_isolation_file::StartupIsolationSource>,
    function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
    constant_policy: novarocks_functions::ConstantPolicy,
    static_plan_carrier: crate::query_execution::package_freeze::CompiledPackageCarrier,
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
    native_capacity_profile: crate::native_workload::FrontendWorkloadCapacityProfile,
    supervisor: LogicalExecutionSupervisorConfig,
    workload: WorkloadConfig,
    result_profile: FrontendResultCapacityProfile,
    abort_effect_capacity: NonZeroUsize,
}

impl FrontendLogicalExecutionRuntimeConfig {
    pub fn new(
        supervisor: LogicalExecutionSupervisorConfig,
        workload: WorkloadConfig,
        abort_effect_capacity: NonZeroUsize,
        result_profile: FrontendResultCapacityProfile,
        native_capacity_profile: crate::native_workload::FrontendWorkloadCapacityProfile,
    ) -> Self {
        Self {
            native_capacity_profile,
            supervisor,
            workload,
            result_profile,
            abort_effect_capacity,
        }
    }

    #[cfg(test)]
    fn test_capacity_profile(
        query_blocking: QueryBlockingExecutorConfig,
    ) -> crate::native_workload::FrontendWorkloadCapacityProfile {
        crate::native_workload::original_owner_test_profile(query_blocking)
    }

    #[cfg(test)]
    fn for_test() -> Self {
        Self::new(
            LogicalExecutionSupervisorConfig::new(
                DEFAULT_LOGICAL_EXECUTION_MAILBOX_CAPACITY,
                DEFAULT_LOGICAL_EXECUTION_CONTEXT_ISSUE_CAPACITY,
                DEFAULT_LOGICAL_EXECUTION_CONTEXT_ISSUE_CAPACITY,
                LogicalExecutionRowsConfig::new(
                    DEFAULT_RESULT_DELIVERY_CAPACITY,
                    DEFAULT_LOGICAL_EXECUTION_MAX_ATTEMPTS,
                    DEFAULT_REPLACEMENT_RESERVATION_VALID_FOR,
                ),
            ),
            crate::native_workload::original_owner_test_workload(),
            DEFAULT_LOGICAL_ABORT_EFFECT_CAPACITY,
            FrontendResultCapacityProfile::try_new(
                novarocks_workload_control::ResultCapacityConfig::V1,
            )
            .expect("test result profile"),
            Self::test_capacity_profile(QueryBlockingExecutorConfig::new(
                DEFAULT_QUERY_BLOCKING_WORKER_COUNT,
                DEFAULT_QUERY_BLOCKING_QUEUE_CAPACITY,
            )),
        )
    }
}

#[derive(Clone)]
pub struct FrontendExecutionConfig {
    native_capacity_profile: crate::native_workload::FrontendWorkloadCapacityProfile,
    #[cfg(novarocks_dev_controls)]
    test_session: Option<crate::test_session::FrontendTestSessionConfig>,
    runtime_filter_worker_count: NonZeroUsize,
    native_compatibility_id: NativeCompatibilityId,
    function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
    constant_policy: novarocks_functions::ConstantPolicy,
    /// The one static carrier every completed plan of this process is frozen
    /// into. It travels with the function catalog to every owner that
    /// encodes a plan, and is never chosen per statement.
    static_plan_carrier: crate::query_execution::package_freeze::CompiledPackageCarrier,
    mv_scheduler: MvSchedulerConfig,
    mv_maintenance: MaintenanceCoordinatorConfig,
    mv_remote_effect_policy: novarocks_mv_application::management::RemoteEffectPolicy,
    mv_management_audit: Option<Arc<dyn novarocks_mv_application::management::ManagementAuditSink>>,
    mv_startup_isolation: Option<crate::mv::startup_isolation_file::StartupIsolationSource>,
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
    transport_budget: FrontendTaskTransportBudget,
    /// Positive root-result payload credit placed on every Native fetch.
    /// Elastic process-wide workers for already-admitted CPU preparation.
    query_cpu_executor_config: QueryCpuExecutorConfig,
    /// Fixed process-wide workers for legacy synchronous query command edges.
    query_blocking_executor_config: QueryBlockingExecutorConfig,
    /// Fixed process-wide worker and waiting bounds for synchronous result decode.
    logical_execution_supervisor: LogicalExecutionSupervisorConfig,
    workload: WorkloadConfig,
    result_profile: FrontendResultCapacityProfile,
    logical_abort_effect_capacity: NonZeroUsize,
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
    #[cfg(novarocks_dev_controls)]
    pub fn with_test_session(
        mut self,
        config: crate::test_session::FrontendTestSessionConfig,
    ) -> Self {
        self.test_session = Some(config);
        self
    }

    pub fn new(
        runtime_filter_worker_count: NonZeroUsize,
        native_compatibility_id: NativeCompatibilityId,
        function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
        logical_runtime: FrontendLogicalExecutionRuntimeConfig,
        constant_policy: novarocks_functions::ConstantPolicy,
        static_plan_carrier: crate::query_execution::package_freeze::CompiledPackageCarrier,
    ) -> Self {
        Self {
            #[cfg(novarocks_dev_controls)]
            test_session: None,
            runtime_filter_worker_count,
            native_compatibility_id,
            function_catalog,
            constant_policy,
            static_plan_carrier,
            mv_scheduler: MvSchedulerConfig::default(),
            mv_maintenance: MaintenanceCoordinatorConfig::default(),
            mv_remote_effect_policy:
                novarocks_mv_application::management::RemoteEffectPolicy::default(),
            mv_management_audit: None,
            mv_startup_isolation: None,
            optimizer_query_mem_limit_bytes: DEFAULT_OPTIMIZER_QUERY_MEM_LIMIT_BYTES,
            query_control_timeouts: FrontendQueryControlTimeouts::default(),
            task_update_retry_policy: TaskUpdateRetryPolicy::default(),
            coordination_budgets: CoordinationBudgets::DEFAULT,
            transport_budget: FrontendTaskTransportBudget::DEFAULT,
            query_cpu_executor_config: QueryCpuExecutorConfig::with_idle_keepalive(
                DEFAULT_QUERY_CPU_IDLE_KEEPALIVE,
            ),
            query_blocking_executor_config: QueryBlockingExecutorConfig::new(
                DEFAULT_QUERY_BLOCKING_WORKER_COUNT,
                DEFAULT_QUERY_BLOCKING_QUEUE_CAPACITY,
            ),
            native_capacity_profile: logical_runtime.native_capacity_profile,
            logical_execution_supervisor: logical_runtime.supervisor,
            workload: logical_runtime.workload,
            result_profile: logical_runtime.result_profile,
            logical_abort_effect_capacity: logical_runtime.abort_effect_capacity,
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
    #[cfg(test)]
    pub fn new_for_test(
        runtime_filter_worker_count: NonZeroUsize,
        native_compatibility_id: NativeCompatibilityId,
        function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
        constant_policy: novarocks_functions::ConstantPolicy,
    ) -> Self {
        Self::new(
            runtime_filter_worker_count,
            native_compatibility_id,
            function_catalog,
            FrontendLogicalExecutionRuntimeConfig::for_test(),
            constant_policy,
            crate::query_execution::package_freeze::CompiledPackageCarrier::for_test(
                novarocks_plan_codec::physical_package_v2::FROZEN_PACKAGE_ADMISSION,
                novarocks_plan_codec::physical_package_v2::PackageEncodeLimits::FROZEN,
            ),
        )
    }

    /// Full original-source composition for the three real statement-entry
    /// tests whose host has no Connector factories and an empty BE topology.
    /// Other lifecycle-only tests keep their explicit original profile.
    #[cfg(test)]
    pub(crate) fn try_with_empty_provider_read_profile_for_test(
        mut self,
        ingress: crate::native_workload::FrontendStatementIngress,
    ) -> Result<Self, novarocks_workload_control::WorkError> {
        use crate::native_workload::{
            FrontendNativeCapacityInputs, FrontendWorkloadCapacityConfig,
            FrontendWorkloadCapacityProfile,
        };
        let policy = FrontendWorkloadCapacityConfig::try_new(
            self.workload.scope_records_limit,
            NonZeroUsize::new(7).unwrap(),
            Duration::from_millis(25),
        )?;
        let inputs =
            FrontendNativeCapacityInputs::try_new(policy, self.query_blocking_executor_config)?;
        self.native_capacity_profile = FrontendWorkloadCapacityProfile::try_for_native(
            inputs,
            &self.workload,
            self.result_profile,
            self.transport_budget,
            ingress,
        )?;
        Ok(self)
    }

    pub(crate) fn function_catalog(&self) -> Arc<novarocks_functions::EngineFunctionCatalog> {
        Arc::clone(&self.function_catalog)
    }

    pub const fn constant_policy(&self) -> novarocks_functions::ConstantPolicy {
        self.constant_policy
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
        transport: FrontendTaskTransportBudget,
    ) -> Self {
        self.coordination_budgets = coordination;
        self.transport_budget = transport;
        self
    }

    /// Server composition freezes the elastic planning worker reuse window
    /// before the Frontend opens any process runtime. This is not a query
    /// capacity setting.
    pub fn with_query_cpu_executor_config(mut self, config: QueryCpuExecutorConfig) -> Self {
        self.query_cpu_executor_config = config;
        self
    }

    /// Server composition freezes the bounded legacy command-worker capacity
    /// before opening any Frontend runtime.
    pub fn with_query_blocking_executor_config(
        mut self,
        config: QueryBlockingExecutorConfig,
    ) -> Self {
        self.query_blocking_executor_config = config;
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

    pub fn with_mv_scheduler_config(mut self, config: MvSchedulerConfig) -> Self {
        self.mv_scheduler = config;
        self
    }

    pub fn with_mv_maintenance_config(mut self, config: MaintenanceCoordinatorConfig) -> Self {
        self.mv_maintenance = config;
        self
    }

    /// The deployment's own claims about MV management continuation.
    ///
    /// Both are absent by default, and absence is the conservative answer: no
    /// remote-effect guarantee means a lost outcome waits for an operator, and
    /// no audit sink means the operator's declaration is refused rather than
    /// applied unrecorded.
    pub fn with_mv_management(
        mut self,
        remote_effect_policy: novarocks_mv_application::management::RemoteEffectPolicy,
        audit_sink: Option<Arc<dyn novarocks_mv_application::management::ManagementAuditSink>>,
        startup_isolation: Option<crate::mv::startup_isolation_file::StartupIsolationSource>,
    ) -> Self {
        self.mv_remote_effect_policy = remote_effect_policy;
        self.mv_management_audit = audit_sink;
        self.mv_startup_isolation = startup_isolation;
        self
    }

    pub fn with_catalog_materialization_config(
        mut self,
        config: CatalogMaterializationConfig,
    ) -> Self {
        self.catalog_materialization = config;
        self
    }

    /// Validates the Server-resolved catalog-prune policy before the FE role
    /// opens its role-local worker. The concrete policy type stays internal to
    /// the Frontend owner; Server supplies only configuration values.
    pub fn try_with_catalog_prune_config(
        mut self,
        interval: Duration,
        rpc_timeout: Duration,
        max_inflight: usize,
    ) -> Result<Self, String> {
        self.catalog_prune = CatalogPruneConfig::try_new(interval, rpc_timeout, max_inflight)?;
        Ok(self)
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
    #[cfg(feature = "mem-1-m07-hms-listing-observe")]
    pub fn hms_listing_test_observation(
        &self,
    ) -> Option<crate::server::HmsListingObservationSetup> {
        self.test_session
            .as_ref()
            .and_then(|session| session.hms_listing_observation())
    }

    /// Installs the original aggregate in the caller before any asynchronous
    /// bootstrap. Failure leaves that same aggregate in receiving for teardown.
    pub async fn open(
        receiving: &mut Option<Self>,
        state_store: Option<StateStoreHostInput>,
        execution: FrontendExecutionConfig,
        backend: ClusterBackendOpenConfig,
        data_runtime: Handle,
        native_trust: Arc<NativeTrust>,
        native_transport: FrontendNativeTransport,
    ) -> Result<Self, FrontendApplicationError> {
        Self::open_with_role_factories_and_state_store_registry(
            receiving,
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

    pub async fn open_with_role_factories_and_state_store_registry(
        receiving: &mut Option<Self>,
        state_store: Option<StateStoreHostInput>,
        state_store_registry: &StateStoreProviderRegistry,
        execution: FrontendExecutionConfig,
        backend: ClusterBackendOpenConfig,
        connector_role_factories: Vec<Arc<dyn ConnectorControlRoleBindingFactory>>,
        data_runtime: Handle,
        native_trust: Arc<NativeTrust>,
        native_transport: FrontendNativeTransport,
    ) -> Result<Self, FrontendApplicationError> {
        if receiving.is_some() {
            return Err(FrontendApplicationError::server(
                "frontend Host receiving slot is occupied",
            ));
        }
        *receiving = Some(Self::prepare_with_role_factories_and_state_store_registry(
            state_store,
            state_store_registry,
            execution,
            backend,
            connector_role_factories,
            data_runtime,
            native_trust,
            native_transport,
        )?);
        receiving
            .as_mut()
            .expect("installed original Host")
            .open_prepared()
            .await?;
        Ok(receiving.take().expect("normal bootstrap handoff"))
    }

    pub fn prepare_with_role_factories_and_state_store_registry(
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
        Self::prepare_with_connector_control_host_and_state_store_registry(
            state_store,
            state_store_registry,
            execution,
            backend,
            connector_control,
            data_runtime,
            native_trust,
            native_transport,
        )
    }

    fn prepare_with_connector_control_host_and_state_store_registry(
        state_store: Option<StateStoreHostInput>,
        state_store_registry: &StateStoreProviderRegistry,
        execution: FrontendExecutionConfig,
        backend: ClusterBackendOpenConfig,
        connector_control: Arc<ConnectorControlHost>,
        data_runtime: Handle,
        native_trust: Arc<NativeTrust>,
        native_transport: FrontendNativeTransport,
    ) -> Result<Self, FrontendApplicationError> {
        let mut durable_families =
            vec![novarocks_mv_application::state_family::MV_ACCELERATOR_STATE_FAMILY];
        durable_families.push(novarocks_catalog_application::CATALOG_DESIRED_STATE_FAMILY);
        durable_families.push(
            novarocks_table_maintenance::gc_observation::GC_OWNED_REF_OBSERVATION_STATE_FAMILY,
        );
        validate_persistent_state_families(&durable_families).map_err(|error| {
            FrontendApplicationError::new(
                FrontendApplicationErrorKind::StateFamilyRegistration,
                error,
            )
        })?;

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
        let state_store_opening = state_store
            .map(|input| {
                StateStoreHost::prepare_original_open(
                    state_store_registry,
                    input,
                    Instant::now() + STATE_STORE_OPEN_TIMEOUT,
                )
                .map(FrontendStateStoreOpening::new)
                .map_err(FrontendApplicationError::state_store)
            })
            .transpose()?;
        // The role composition root mints one identity for both signed Native
        // calls and logical execution. Transport subjects remain diagnostic.
        let frontend_process_id = FrontendProcessId::new_v7();
        native_trust
            .bind_process_identity(NativeProcessIdentity::Frontend(frontend_process_id))
            .map_err(|error| {
                FrontendApplicationError::new(FrontendApplicationErrorKind::CoordinatorOpen, error)
            })?;
        #[cfg(novarocks_dev_controls)]
        let test_session = execution
            .test_session
            .as_ref()
            .map(|config| config.bind(frontend_process_id))
            .transpose()
            .map_err(|error| {
                FrontendApplicationError::new(FrontendApplicationErrorKind::CoordinatorOpen, error)
            })?;
        let query_runtime = data_runtime.clone();
        execution.workload.validate().map_err(|error| {
            FrontendApplicationError::new(
                FrontendApplicationErrorKind::CoordinatorOpen,
                error.to_string(),
            )
        })?;
        let data_runtime = FrontendDataRuntime::new_with_native_trust(
            data_runtime,
            native_trust,
            native_transport,
            execution.transport_budget.into_codec(),
            execution.workload.scope_records_limit,
        )
        .map_err(|error| {
            FrontendApplicationError::new(FrontendApplicationErrorKind::CoordinatorOpen, error)
        })?;
        // Same original Native owners, checked before any logical-role service.
        let native_capacity_profile = execution
            .native_capacity_profile
            .clone()
            .with_native_sources(&data_runtime)
            .map_err(|error| {
                FrontendApplicationError::new(
                    FrontendApplicationErrorKind::WorkloadControlOpen,
                    error,
                )
            })?;
        let execution_runtime_owner = FrontendExecutionRuntimeOwner::try_new(
            query_runtime,
            data_runtime.connector_blocking_io().clone(),
            frontend_process_id,
            execution.logical_execution_supervisor,
            execution.workload.clone(),
            execution.result_profile,
            execution.query_cpu_executor_config,
            execution.query_blocking_executor_config,
            native_capacity_profile,
        )?;
        let catalog_runtime_projection =
            crate::catalog_application::CatalogRuntimeProjection::new();
        let execution_role = backend.role();
        let host = Self {
            #[cfg(novarocks_dev_controls)]
            test_session,
            catalog_role_runtime: None,
            catalog_runtime_projection,
            serving_lifecycle: Arc::new(FrontendServingLifecycle::new()),
            abandoned_attempt_sweeper: None,
            mv_repository: None,
            state_store_host: None,
            state_store_opening,
            startup: Some(FrontendStartupInput {
                backend,
                connector_control,
                catalog_source_input,
                catalog_materialization: execution.catalog_materialization,
                catalog_projection: execution.catalog_projection.clone(),
                catalog_prune: execution.catalog_prune.clone(),
            }),
            query_runtime: FrontendQueryRuntimeConfig {
                native_compatibility_id: execution.native_compatibility_id,
                runtime_filter_worker_count: execution.runtime_filter_worker_count,
                task_update_retry_policy: execution.task_update_retry_policy,
                split_initial_wait_cap: execution.connector_split_initial_dynamic_filter_wait_cap,
                coordination_budgets: execution.coordination_budgets,
                transport_budget: execution.transport_budget,
                abort_capacity: execution.logical_abort_effect_capacity,
            },
            execution_runtime_owner,
            execution_role,
            data_runtime,
            topology: None,
            optimizer_query_mem_limit_bytes: execution.optimizer_query_mem_limit_bytes(),
            lake_publication_runtime_policy: execution.lake_publication_runtime_policy(),
            mv_scheduler_config: execution.mv_scheduler.clone(),
            mv_maintenance_config: execution.mv_maintenance.clone(),
            mv_remote_effect_policy: execution.mv_remote_effect_policy.clone(),
            mv_management_audit: execution.mv_management_audit.clone(),
            mv_startup_isolation: execution.mv_startup_isolation.clone(),
            function_catalog: execution.function_catalog(),
            constant_policy: execution.constant_policy,
            static_plan_carrier: execution.static_plan_carrier,
        };

        Ok(host)
    }

    /// Borrows the aggregate already installed by its process/role owner.
    /// Cancelling this future cannot consume the original opening receiver.
    pub async fn open_prepared(&mut self) -> Result<(), FrontendApplicationError> {
        let FrontendStartupInput {
            backend,
            connector_control,
            catalog_source_input,
            catalog_materialization,
            catalog_projection,
            catalog_prune,
        } = self.startup.take().ok_or_else(|| {
            FrontendApplicationError::server("frontend bootstrap was already started")
        })?;
        let catalog_source_mode = catalog_source_input.mode();
        let host = self;
        // Every fallible observer activation runs after the original process
        // owner aggregate is in Host, so startup failure uses its real drain.
        #[cfg(novarocks_dev_controls)]
        if let Some(owner) = host
            .test_session
            .as_ref()
            .and_then(|session| session.statement.as_ref())
        {
            if let Err(error) = host
                .execution_runtime_owner
                .lifecycle_diagnostics
                .activate(owner)
            {
                return Err(host
                    .cleanup_open_error(FrontendApplicationError::new(
                        FrontendApplicationErrorKind::CoordinatorOpen,
                        error,
                    ))
                    .await);
            }
            // Retain the existing startup attribution evidence only for the
            // explicitly installed, live statement observation owner.
            if owner.is_active() {
                let namespace = host.execution_runtime_owner.namespace;
                eprintln!("NOVAROCKS_QUERY_PROCESS_NAMESPACE query_process_namespace={namespace}");
            }
        }

        #[cfg(feature = "mem-1-m07-hms-listing-observe")]
        if let Some(owner) = host
            .test_session
            .as_ref()
            .and_then(|session| session.hms_admission_owner.as_ref())
        {
            if let Err(error) = owner.install(&host.catalog_runtime_projection) {
                return Err(host
                    .cleanup_open_error(FrontendApplicationError::new(
                        FrontendApplicationErrorKind::CoordinatorOpen,
                        error,
                    ))
                    .await);
            }
        }
        #[cfg(novarocks_dev_controls)]
        if let Some(session) = &mut host.test_session
            && let Err(error) = session.start_cleanup(
                host.execution_runtime_owner.lifecycle_diagnostics(),
                host.catalog_runtime_projection.clone(),
            )
        {
            return Err(host
                .cleanup_open_error(FrontendApplicationError::new(
                    FrontendApplicationErrorKind::CoordinatorOpen,
                    error,
                ))
                .await);
        }

        // Unique original Apply owner is now in Host. No Native caller has
        // received a projection yet; early try_new failure had no reaper.
        if let Err(error) = host
            .data_runtime
            .start_original_apply_reaper()
            .and_then(|()| host.data_runtime.start_original_subscription_reaper())
        {
            return Err(host
                .cleanup_open_error(FrontendApplicationError::new(
                    FrontendApplicationErrorKind::CoordinatorOpen,
                    error,
                ))
                .await);
        }

        if let Err(error) = host.open_configured().await {
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
            let policy = host.run_policy();
            let context = match host.catalog_bootstrap_read_execution() {
                Ok(context) => context,
                Err(error) => return Err(host.cleanup_open_error(error).await),
            };
            match host
                .drive_catalog_bootstrap(async {
                    let transaction = novarocks_state_store_api::StateStoreReadExecution::read_transaction_context(&context)?;
                    CatalogAttachmentRepository::open_in(store, policy, &transaction)
                        .await
                        .map_err(novarocks_failure_contract::SharedFailureCause::new)
                })
                .await
            {
                Ok(repository) => Some(repository),
                Err(error) => return Err(host.cleanup_open_error(error).await),
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
        let catalog_application =
            Arc::new(CatalogApplicationService::new_with_materialization_config(
                catalog_source,
                Arc::clone(&connector_control),
                host.catalog_runtime_projection.publisher(),
                tokio::runtime::Handle::current(),
                catalog_materialization,
                Arc::new(MvCatalogReferenceReader),
            ));
        host.catalog_role_runtime = Some(FrontendCatalogRoleRuntime::new(
            connector_control,
            catalog_application,
        ));
        if catalog_source_mode == CatalogDesiredStateSourceMode::DynamicStateStore {
            let store = host
                .state_store()
                .expect("dynamic catalog source required a StateStore above");
            let binding = host
                .state_store_original_binding()
                .expect("dynamic store has original binding");
            let controller = match FrontendCatalogController::new_with_original(
                store,
                Arc::clone(&host.catalog_role_runtime().application),
                catalog_projection.clone(),
                host.workload_root_admission(),
                binding,
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
            host.catalog_role_runtime_mut().controller = Some(controller.clone());
            let context = match host.catalog_bootstrap_read_execution() {
                Ok(context) => context,
                Err(error) => return Err(host.cleanup_open_error(error).await),
            };
            if let Err(error) = host
                .drive_catalog_bootstrap(async {
                    controller
                        .bootstrap_in(&context)
                        .await
                        .map_err(novarocks_failure_contract::SharedFailureCause::new)
                })
                .await
            {
                return Err(host.cleanup_open_error(error).await);
            }
            let counts = host.catalog_role_runtime().application.projection_counts();
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
            // Background rounds start only after the real workload Ready cut.
            if let Err(error) = host.close_catalog_bootstrap().await {
                return Err(host.cleanup_open_error(error).await);
            }
        } else if catalog_source_mode == CatalogDesiredStateSourceMode::StaticFile {
            let projection = Arc::clone(&host.catalog_role_runtime().application);
            match projection
                .reconcile_snapshot_with_page_size(
                    catalog_projection.page_size,
                    catalog_projection.worker_count,
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
        match ClusterBackendService::open(
            backend,
            tokio::runtime::Handle::current(),
            host.data_runtime.clone(),
        )
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
            Arc::clone(&host.catalog_role_runtime().application),
            host.backend_topology_port(),
            host.data_runtime.clone(),
            catalog_prune.clone(),
        );
        if let Err(error) = catalog_prune.start() {
            return Err(host
                .cleanup_open_error(FrontendApplicationError::new(
                    FrontendApplicationErrorKind::CatalogApplicationServiceOpen,
                    error,
                ))
                .await);
        }
        host.catalog_role_runtime_mut().prune = Some(catalog_prune);

        // Freeze the policy consumed by the role-product coordinator before
        // constructing any product service, so no path can observe an
        // all-in-one-only direct execution fallback.
        match host.state_store() {
            Some(store) => match StateStoreMvRepository::open(store, host.run_policy()).await {
                Ok(repository) => {
                    let repository: Arc<
                        novarocks_mv_application::state_store_repository::StateStoreMvRepository,
                    > = repository;
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
        Ok(())
    }

    pub fn catalog_application_port(
        &self,
    ) -> Arc<dyn novarocks_catalog_application::CatalogApplicationPort> {
        let application = self.catalog_role_runtime().catalog_application_port();
        self.catalog_runtime_projection
            .bind_application(application)
    }

    /// Observe the role-owned service without retaining its StateStore inputs
    /// through Host shutdown. The published query wrapper is not the owner.
    pub(crate) fn catalog_activity_observer(
        &self,
    ) -> std::sync::Weak<dyn novarocks_catalog_application::CatalogApplicationPort> {
        Arc::downgrade(&self.catalog_role_runtime().catalog_application_port())
    }

    fn catalog_role_runtime(&self) -> &FrontendCatalogRoleRuntime {
        self.catalog_role_runtime
            .as_ref()
            .expect("catalog role runtime is installed before host open returns")
    }

    fn catalog_role_runtime_mut(&mut self) -> &mut FrontendCatalogRoleRuntime {
        self.catalog_role_runtime
            .as_mut()
            .expect("catalog role runtime is installed before host open returns")
    }

    /// Transfers the complete catalog lifecycle owner only after Server has
    /// built every fallible role product. Until this point, Host cleanup keeps
    /// the same owner for startup rollback.
    pub(crate) fn take_catalog_role_runtime(
        &mut self,
    ) -> Result<FrontendCatalogRoleRuntime, FrontendApplicationError> {
        self.catalog_role_runtime.take().ok_or_else(|| {
            FrontendApplicationError::server(
                "frontend catalog role runtime was already transferred or was never installed",
            )
        })
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

    /// The deployment identity is server-owned configuration, retained through
    /// the authenticated native-trust capability. Frontend management uses it
    /// only as its process-local owner identity; it never reads StateStore or
    /// target metadata to infer an owner.
    pub(crate) fn frontend_process_id(&self) -> FrontendProcessId {
        match self.data_runtime.native_trust().local_process_identity() {
            Some(NativeProcessIdentity::Frontend(process)) => process,
            _ => unreachable!("frontend role binds its process identity before composition"),
        }
    }

    pub(crate) fn native_deployment_id(&self) -> &str {
        self.data_runtime.native_trust().deployment_id().as_str()
    }

    /// FE-local serving lifecycle shared by SQL and background admission
    /// owners. Server orchestration alone owns its Ready/Draining transitions.
    pub fn serving_lifecycle(&self) -> Arc<FrontendServingLifecycle> {
        Arc::clone(&self.serving_lifecycle)
    }

    /// The sanitized management reader joins lifecycle facts with the sole
    /// workload authority's read-only observation. It grants neither owner
    /// the other's mutation authority.
    pub fn serving_snapshot_reader(&self) -> Arc<dyn FrontendServingSnapshotReader> {
        Arc::new(FrontendServingWorkloadSnapshotReader::new(
            Arc::clone(&self.serving_lifecycle),
            self.workload_observation(),
        ))
    }

    /// Begins the one-way serving drain. Close governed-root admission before
    /// publishing Draining so no new workload root can enter after the role
    /// has started refusing statements.
    pub fn begin_serving_drain(&self, timeout: Duration) {
        self.execution_runtime_owner.close_root_admission();
        self.serving_lifecycle.begin_drain(timeout);
    }

    /// Requests drain-deadline cancellation through the sole governed
    /// workload authority. The serving lifecycle observes the transition but
    /// never owns a business root or cancellation bridge.
    pub fn cancel_governed_work_at_drain_deadline(&self) -> usize {
        self.execution_runtime_owner
            .cancel_active_roots_at_drain_deadline()
    }

    /// Pure level observation; original joins and causes stay with their owners.
    pub(crate) fn workload_progress_has_failed(&self) -> bool {
        self.execution_runtime_owner
            .deadline_supervisor
            .has_exited()
            || self.execution_runtime_owner.supervisor.service_has_exited()
            || self
                .execution_runtime_owner
                .supervisor
                .process_failure()
                .is_some()
    }

    /// Diagnostic borrowing never consumes or disposes the original cause.
    pub fn original_workload_progress_failure(&self) -> Option<&tokio::task::JoinError> {
        self.execution_runtime_owner
            .deadline_supervisor
            .original_failure()
    }

    /// Bind the same session-control owner before any statement or Ready cut.
    pub(crate) fn install_statement_control(
        &mut self,
        control: novarocks_query_application::session_control::QueryControlService,
    ) -> Result<(), FrontendApplicationError> {
        let source_slots = std::num::NonZeroUsize::new(
            self.execution_runtime_owner
                .native_capacity_profile
                .policy()
                .scope_limit(),
        )
        .ok_or_else(|| FrontendApplicationError::server("statement source inventory is empty"))?;
        self.execution_runtime_owner
            .supervisor
            .install_statement_control(control, source_slots)
            .map_err(|error| {
                FrontendApplicationError::server(format!("install statement control: {error:?}"))
            })?;
        self.execution_runtime_owner.statement_control_installed = true;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn install_statement_control_for_test(&mut self) {
        let owner = Arc::new(
            novarocks_query_application::query_control::QueryApplicationControl::default(),
        );
        self.install_statement_control(
            novarocks_query_application::session_control::QueryControlService::new(owner),
        )
        .expect("install the original test statement-control owner");
    }

    /// Permit governed startup work after all original sources are installed.
    /// Public SQL/serving readiness remains closed until mandatory restore exits.
    pub(crate) fn mark_workload_ready(&self) -> Result<(), FrontendApplicationError> {
        if !self.execution_runtime_owner.statement_control_installed {
            self.execution_runtime_owner.close_admission();
            return Err(FrontendApplicationError::server(
                "original statement control was not installed",
            ));
        }
        self.execution_runtime_owner.mark_ready().map_err(|error| {
            FrontendApplicationError::server(format!(
                "mark frontend workload authority ready after bootstrap: {error}"
            ))
        })?;
        Ok(())
    }

    /// Publish serving readiness only after the role has finished mandatory startup.
    pub(crate) fn mark_ready(&self) -> Result<(), FrontendApplicationError> {
        self.mark_workload_ready()?;
        if let Some(controller) = self
            .catalog_role_runtime
            .as_ref()
            .and_then(|role| role.controller.as_ref())
        {
            controller.start().map_err(|error| {
                self.execution_runtime_owner.close_admission();
                FrontendApplicationError::new(
                    FrontendApplicationErrorKind::CatalogControllerOpen,
                    error,
                )
            })?;
        }
        if let Err(error) = self.serving_lifecycle.mark_ready() {
            self.execution_runtime_owner.close_admission();
            return Err(FrontendApplicationError::server(format!(
                "mark frontend serving lifecycle ready after bootstrap: {error:?}"
            )));
        }
        Ok(())
    }

    /// Transfers the repository to the immutable MV product graph. Before
    /// this point Host owns it for startup rollback; afterwards no Host path
    /// retains a parallel MV repository capability.
    pub(crate) fn take_mv_repository_for_role_product_construction(
        &mut self,
    ) -> Result<
        Arc<novarocks_mv_application::state_store_repository::StateStoreMvRepository>,
        FrontendApplicationError,
    > {
        self.mv_repository.take().ok_or_else(|| {
            FrontendApplicationError::server(
                "frontend MV repository was already transferred or was never installed",
            )
        })
    }

    pub(crate) fn mv_scheduler_config(&self) -> MvSchedulerConfig {
        self.mv_scheduler_config.clone()
    }

    pub(crate) fn mv_maintenance_config(&self) -> MaintenanceCoordinatorConfig {
        self.mv_maintenance_config.clone()
    }

    pub(crate) fn mv_remote_effect_policy(
        &self,
    ) -> novarocks_mv_application::management::RemoteEffectPolicy {
        self.mv_remote_effect_policy.clone()
    }

    pub(crate) fn mv_management_audit_sink(
        &self,
    ) -> Option<Arc<dyn novarocks_mv_application::management::ManagementAuditSink>> {
        self.mv_management_audit.clone()
    }

    pub(crate) fn mv_startup_isolation(
        &self,
    ) -> Option<crate::mv::startup_isolation_file::StartupIsolationSource> {
        self.mv_startup_isolation.clone()
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
    pub fn run_policy(&self) -> StateStoreRunPolicy {
        self.state_store_host
            .as_ref()
            .map(StateStoreHost::run_policy)
            .unwrap_or_default()
    }

    /// The store together with the policy governing its use.
    ///
    /// Durable consumers take the pair, so none of them can be constructed
    /// holding storage without an agreed retry budget for it.
    pub fn durable(&self) -> Option<(Arc<dyn StateStore>, StateStoreRunPolicy)> {
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

    /// The immutable deployment profile used by every FE constant author.
    /// This is an admission ceiling, not a memory allocation capability.
    pub const fn constant_policy(&self) -> novarocks_functions::ConstantPolicy {
        self.constant_policy
    }

    pub fn function_catalog(&self) -> Arc<novarocks_functions::EngineFunctionCatalog> {
        Arc::clone(&self.function_catalog)
    }

    /// The static carrier every owner that encodes a completed plan receives
    /// beside the function catalog.
    pub(crate) fn static_plan_carrier(
        &self,
    ) -> crate::query_execution::package_freeze::CompiledPackageCarrier {
        self.static_plan_carrier.clone()
    }

    pub fn connector_control_registry(
        &self,
    ) -> Arc<dyn novarocks_spi::connector::ConnectorControlRegistry> {
        self.catalog_role_runtime().connector_control_registry()
    }

    /// Typed-read planning is carried by the same complete control host
    /// generation as generic planning. There is no parallel registry.
    pub fn typed_connector_control(&self) -> Arc<ConnectorControlHost> {
        self.catalog_role_runtime().typed_connector_control()
    }

    pub(crate) fn state_store_original_binding(
        &self,
    ) -> Option<crate::state_store::execution::StateStoreOriginalBinding> {
        let host = self.state_store_host.as_ref()?;
        let (vocabulary, instance) = host.original_blocking_sources()?;
        let dependencies = host.original_blocking_dependency_facts()?.to_vec();
        Some(crate::state_store::execution::StateStoreOriginalBinding {
            transaction_deadline: host.state_store()?.limits().transaction_deadline,
            dependencies,
            vocabulary,
            instance,
            executor: self.execution_runtime_owner.query_blocking_executor(),
            policy: host.run_policy(),
        })
    }

    pub fn state_store_provider_id(&self) -> Option<StateStoreProviderId> {
        self.state_store_host
            .as_ref()
            .map(StateStoreHost::provider_id)
    }

    /// Cloneable submission handle for synchronous command edges whose
    /// provider contracts are not asynchronous yet.
    pub(crate) fn query_blocking_executor(&self) -> QueryBlockingExecutor {
        self.execution_runtime_owner.query_blocking_executor()
    }

    /// Cloneable handle for the one process-owned Connector blocking-I/O
    /// supervisor. SQL session initialization submits external catalog
    /// metadata here, never local catalog lookups.
    /// The same role-owned Native data view; this creates no new cache/root.
    pub(crate) fn write_data_runtime(&self) -> FrontendDataRuntime {
        self.data_runtime.clone()
    }

    pub(crate) fn connector_blocking_io_supervisor(&self) -> ConnectorBlockingIoSupervisor {
        self.data_runtime.connector_blocking_io().clone()
    }

    #[allow(
        dead_code,
        reason = "The application host retains this bounded start handle for role integration."
    )]
    pub(crate) fn logical_execution_client(&self) -> QueryExecutionClient {
        self.execution_runtime_owner.logical_execution_client()
    }

    /// Materializes the native-read launcher while Server constructs its
    /// immutable role products. The returned launcher owns no process
    /// lifecycle state; it only joins the Host-owned execution runtime with
    /// the role's fixed topology and data-plane capabilities.
    pub(crate) fn build_logical_execution_launcher(&self) -> Arc<dyn LogicalExecutionLauncher> {
        let topology = Arc::clone(self.topology());
        let config = self.query_runtime;
        let native_runtime = FrontendNativeLogicalExecutionRuntime::new(
            Arc::clone(&topology) as novarocks_query_application::api::BackendTopologyService,
            topology as novarocks_query_application::api::BackendProcessObservationService,
            self.data_runtime.clone(),
            config.native_compatibility_id,
            config.runtime_filter_worker_count,
            config.task_update_retry_policy,
            config.split_initial_wait_cap,
            config.coordination_budgets,
            config.transport_budget.into_codec(),
            config.abort_capacity,
            self.execution_runtime_owner.lifecycle_diagnostics(),
            Arc::clone(&self.execution_runtime_owner.clock),
        );
        #[cfg(novarocks_dev_controls)]
        let native_runtime = match &self.test_session {
            Some(session) => native_runtime.with_test_controls(
                session.native.clone(),
                session.observation.clone(),
                session.statement.as_ref().map(Arc::downgrade),
            ),
            None => native_runtime,
        };
        Arc::new(FrontendNativeLogicalExecutionLauncher::new(
            self.logical_execution_client(),
            native_runtime,
        )) as Arc<dyn LogicalExecutionLauncher>
    }

    #[allow(
        dead_code,
        reason = "Product integrations consume this governed root admission handle."
    )]
    pub(crate) fn workload_root_admission(&self) -> RootAdmissionHandle {
        self.execution_runtime_owner.root_admission()
    }

    #[allow(
        dead_code,
        reason = "Management integrations consume this read-only observation handle."
    )]
    /// Read-only governed-root observation consumed by the Server role runner
    /// while it drains admitted Frontend work.
    pub fn workload_observation(&self) -> WorkloadObservationHandle {
        self.execution_runtime_owner.workload_observation()
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
        native_ingress: novarocks_native_adapter::native_server::NativeIngressConfig,
    ) -> Result<crate::native::report_server::FrontendReportServerHandle, FrontendApplicationError>
    {
        crate::native::report_server::FrontendReportServerHandle::start(
            bind_addr,
            self.backend_membership_ingress(),
            self.lifecycle_convergence_reader(),
            native_trust,
            native_transport,
            native_ingress,
            self.data_runtime.transport_admission().clone(),
        )
        .map_err(FrontendApplicationError::server)
    }

    /// Starts the Frontend report listener from Server-composed bind and trust
    /// material.
    pub fn start_report_server_from_host(
        &self,
        host: &str,
        port: u16,
        native_trust: Arc<NativeTrust>,
        native_transport: FrontendNativeTransport,
        native_ingress: novarocks_native_adapter::native_server::NativeIngressConfig,
    ) -> Result<crate::native::report_server::FrontendReportServerHandle, FrontendApplicationError>
    {
        crate::native::report_server::FrontendReportServerHandle::start_from_host(
            host,
            port,
            self.backend_membership_ingress(),
            self.lifecycle_convergence_reader(),
            native_trust,
            native_transport,
            native_ingress,
            self.data_runtime.transport_admission().clone(),
        )
        .map_err(FrontendApplicationError::server)
    }

    pub(crate) fn lifecycle_convergence_reader(&self) -> Arc<dyn QueryLifecycleConvergenceReader> {
        self.execution_runtime_owner.lifecycle_diagnostics()
            as Arc<dyn QueryLifecycleConvergenceReader>
    }

    /// Frontend composition-time topology leaf used by FE-owned services.
    pub fn backend_topology_port(
        &self,
    ) -> novarocks_query_application::api::BackendTopologyService {
        Arc::clone(self.topology()) as novarocks_query_application::api::BackendTopologyService
    }

    /// Frontend composition-time read-only backend command leaf.
    pub fn backend_topology_command_port(
        &self,
    ) -> Arc<dyn novarocks_query_application::api::BackendTopologyCommandPort> {
        Arc::clone(self.topology())
            as Arc<dyn novarocks_query_application::api::BackendTopologyCommandPort>
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
        #[cfg(novarocks_dev_controls)]
        let controls = match &mut self.test_session {
            Some(session) => session.close_and_join(),
            None => Ok(()),
        };
        let resources = self.release_resources_until(deadline).await;
        #[cfg(novarocks_dev_controls)]
        let resources = match (controls, resources) {
            (Ok(()), result) => result,
            (Err(error), Ok(())) => Err(error),
            (Err(control), Err(resource)) => {
                Err(format!("{resource}; test-session cleanup: {control}"))
            }
        };
        resources.map_err(|error| {
            let mut result =
                FrontendApplicationError::new(FrontendApplicationErrorKind::Shutdown, error);
            result.original = self
                .state_store_opening
                .as_ref()
                .and_then(FrontendStateStoreOpening::original_failure)
                .cloned();
            result
        })
    }

    /// Releases process-local join ownership after bounded graceful shutdown
    /// failed and the production runner has irrevocably committed to FE process
    /// exit. This is not a reusable shutdown path: admission remains closed and
    /// the Host must be dropped immediately after the runner returns.
    /// Releases process-local join ownership only after the Server role runner
    /// has exhausted its bounded convergence attempts and committed to exit.
    pub fn abandon_for_process_exit(&mut self) {
        self.serving_lifecycle.mark_stopping();
        if let Some(topology) = self.topology.as_ref()
            && let Err(error) = topology.request_heartbeat_stop_for_process_exit()
        {
            tracing::error!(
                %error,
                "failed to signal frontend heartbeat manager before process exit"
            );
        }
        self.execution_runtime_owner.abandon_for_process_exit();
    }

    fn catalog_bootstrap_read_execution(
        &mut self,
    ) -> Result<
        crate::state_store::execution::StateStoreOriginalReadExecution,
        FrontendApplicationError,
    > {
        let executor = self.execution_runtime_owner.query_blocking_executor();
        let ready = self
            .state_store_host
            .as_ref()
            .ok_or_else(|| FrontendApplicationError::server("catalog startup has no StateStore"))?;
        self.state_store_opening
            .as_mut()
            .ok_or_else(|| {
                FrontendApplicationError::server("catalog startup has no original receiver")
            })?
            .bootstrap_read_execution(ready, &executor)
            .map_err(FrontendApplicationError::original_state_store)
    }
    async fn drive_catalog_bootstrap<T>(
        &mut self,
        future: impl std::future::Future<
            Output = Result<T, novarocks_failure_contract::SharedFailureCause>,
        >,
    ) -> Result<T, FrontendApplicationError> {
        let opening = self.state_store_opening.as_mut().ok_or_else(|| {
            FrontendApplicationError::server("catalog startup original receiver is unavailable")
        })?;
        tokio::pin!(future);
        std::future::poll_fn(|cx| {
            // F may register a new phase during this turn; always poll the
            // original inventory again before returning Pending or its value.
            if let Err(cause) = opening.poll_bootstrap_read(cx) {
                return std::task::Poll::Ready(Err(
                    FrontendApplicationError::original_state_store(cause),
                ));
            }
            let result = future.as_mut().poll(cx);
            if let Err(cause) = opening.poll_bootstrap_read(cx) {
                return std::task::Poll::Ready(Err(
                    FrontendApplicationError::original_state_store(cause),
                ));
            }
            result.map(|value| value.map_err(FrontendApplicationError::original_state_store))
        })
        .await
    }
    async fn close_catalog_bootstrap(&mut self) -> Result<(), FrontendApplicationError> {
        let opening = self.state_store_opening.as_mut().ok_or_else(|| {
            FrontendApplicationError::server("catalog startup original receiver is unavailable")
        })?;
        std::future::poll_fn(|cx| opening.poll_bootstrap_read_closed(cx))
            .await
            .map_err(FrontendApplicationError::original_state_store)
    }

    async fn open_configured(&mut self) -> Result<(), FrontendApplicationError> {
        let Some(opening) = self.state_store_opening.as_mut() else {
            return Ok(());
        };
        let executor = self.execution_runtime_owner.query_blocking_executor();
        opening
            .start(&executor)
            .map_err(FrontendApplicationError::original_state_store)?;
        let deadline = opening.deadline();
        let open = std::future::poll_fn(|cx| opening.poll_open(cx, &mut self.state_store_host));
        match tokio::time::timeout_at(deadline.into(), open).await {
            Ok(result) => result?,
            Err(_) => {
                return Err(FrontendApplicationError::original_state_store(
                    novarocks_state_store_api::StateStoreError::new(
                        novarocks_state_store_api::StateStoreErrorKind::DeadlineExceeded,
                        "frontend state store opening deadline exceeded",
                    )
                    .into_original_failure(),
                ));
            }
        }
        if let Some(store) = self.state_store() {
            let ready = self
                .state_store_host
                .as_ref()
                .expect("validated original instance");
            let progress = self
                .state_store_opening
                .as_mut()
                .expect("original opening custody")
                .evidence_progress(ready, &executor)
                .map_err(FrontendApplicationError::original_state_store)?;
            let sweeper = crate::state_store::AbandonedAttemptSweeper::new(
                store,
                progress,
                ready.run_policy(),
            );
            // Install custody before starting the existing cadence or any
            // subsequent bootstrap operation can abandon a dispatched attempt.
            self.abandoned_attempt_sweeper = Some(sweeper);
            self.abandoned_attempt_sweeper
                .as_mut()
                .unwrap()
                .start()
                .map_err(|error| {
                    FrontendApplicationError::new(
                        FrontendApplicationErrorKind::StateStoreHost,
                        error,
                    )
                })?;
        }
        Ok(())
    }

    async fn cleanup_open_error(
        &mut self,
        primary: FrontendApplicationError,
    ) -> FrontendApplicationError {
        match self
            .shutdown_until(Instant::now() + STATE_STORE_SHUTDOWN_TIMEOUT)
            .await
        {
            Ok(()) => primary,
            Err(cleanup_error) => primary.with_application_cleanup(cleanup_error),
        }
    }

    async fn release_resources_until(&mut self, deadline: Instant) -> Result<(), String> {
        self.serving_lifecycle.mark_stopping();
        self.execution_runtime_owner.close_admission();
        let mut primary_error: Option<String> = None;
        // Stop and physically settle the current Management round before WC
        // drain waits for that same Root. Keep both receivers on timeout.
        if let Some(controller) = self
            .catalog_role_runtime
            .as_ref()
            .and_then(|role| role.controller.as_ref())
        {
            tokio::time::timeout_at(deadline.into(), controller.shutdown())
                .await
                .map_err(|_| {
                    "frontend deadline elapsed joining catalog controller".to_string()
                })??;
        }
        let execution_result = {
            let shutdown = self.execution_runtime_owner.drain_workload_until(deadline);
            tokio::pin!(shutdown);
            // The healthy continuous reaper stays active throughout logical
            // teardown. If it fails, borrow the SAME owner here so its original
            // children cannot strand the Workload drain that precedes the tail.
            tokio::select! {
                result = &mut shutdown => result,
                recovery = self.data_runtime.recover_failed_subscription_reaper_until(deadline) => {
                    if let Err(error) = recovery { primary_error = Some(error); }
                    shutdown.await
                }
            }
        };
        if let Err(error) = execution_result {
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
        // Stop the cadence before the store closes. The store host performs one
        // final drain of its own, so nothing is lost here and no sweep races the
        // instance going away.
        if let Some(sweeper) = self.abandoned_attempt_sweeper.as_mut() {
            sweeper.shutdown().await;
        }
        let catalog_runtime_error = match self.catalog_role_runtime.as_mut() {
            Some(runtime) => runtime.shutdown_until(deadline).await.err(),
            None => None,
        };
        if catalog_runtime_error.is_none() {
            self.catalog_role_runtime.take();
        }
        if let Some(catalog_runtime_error) = catalog_runtime_error {
            let error = catalog_runtime_error;
            if let Some(primary) = primary_error.as_mut() {
                primary.push_str(&format!("; cleanup failed: {error}"));
            } else {
                primary_error = Some(error);
            }
            return Err(primary_error.expect("catalog role runtime shutdown error is retained"));
        }
        // Execution and heartbeat shutdown precede Apply close. Retain this
        // original role owner in Host on timeout so a shutdown retry joins
        // the SAME records and handles.
        if let Err(error) = self.data_runtime.drain_original_apply_until(deadline).await {
            if !self.data_runtime.original_apply_joined() {
                return Err(match primary_error {
                    Some(primary) => format!("{primary}; cleanup failed: {error}"),
                    None => error,
                });
            }
            if let Some(primary) = primary_error.as_mut() {
                primary.push_str(&format!("; cleanup failed: {error}"));
            } else {
                primary_error = Some(error);
            }
        }
        if let Err(error) = self
            .data_runtime
            .drain_original_subscriptions_until(deadline)
            .await
        {
            if !self.data_runtime.original_subscriptions_joined() {
                return Err(match primary_error {
                    Some(primary) => format!("{primary}; cleanup failed: {error}"),
                    None => error,
                });
            }
            if let Some(primary) = primary_error.as_mut() {
                primary.push_str(&format!("; cleanup failed: {error}"));
            } else {
                primary_error = Some(error);
            }
        }
        // Role-owned callers have reached the Native tail. Close future calls,
        // retire cache aliases and wait for the original public IO/body exits
        // before StateStore, using the same absolute Host deadline.
        // This observation does not replace original caller task/F joins or
        // prove every escaped clone's undispatched stream reservation exited.
        if let Err(error) = self
            .data_runtime
            .drain_native_outgoing_until(deadline)
            .await
        {
            return Err(match primary_error {
                Some(primary) => format!("{primary}; cleanup failed: {error}"),
                None => error,
            });
        }
        // The same cadence's final pass borrows the Host-owned evidence
        // inventory. A waiting timeout retains that inventory and queued debt.
        if let Some(sweeper) = self.abandoned_attempt_sweeper.as_ref() {
            match tokio::time::timeout_at(deadline.into(), sweeper.sweep_once()).await {
                Ok(Ok(_)) => {}
                Ok(Err(cause)) => {
                    tracing::warn!(%cause, "abandoned state store attempts were not released before shutdown");
                    if let Some(opening) = self.state_store_opening.as_mut() {
                        opening.retain_evidence_failure(cause);
                    }
                }
                Err(_) => {
                    return Err("frontend original StateStore evidence remains pending".into());
                }
            }
        }
        // No running cadence remains. Its store alias must leave before the
        // actual instance Store/File destruction; its physical sources stay
        // with the same opening/evidence owner below.
        self.abandoned_attempt_sweeper.take();
        drop(self.mv_repository.take());
        if let Some(opening) = self.state_store_opening.as_mut() {
            let executor = self.execution_runtime_owner.query_blocking_executor();
            let close = std::future::poll_fn(|cx| {
                opening.poll_close(&mut self.state_store_host, &executor, deadline, cx)
            });
            match tokio::time::timeout_at(deadline.into(), close).await {
                Ok(Ok(())) => {
                    self.state_store_opening.take();
                }
                Ok(Err(error)) => {
                    return Err(match primary_error {
                        Some(primary) => format!("{primary}; cleanup failed: {error}"),
                        None => error.to_string(),
                    });
                }
                Err(_) => return Err("frontend original StateStore close remains pending".into()),
            }
        }
        if self.state_store_host.is_none()
            && self.state_store_opening.is_none()
            && let Err(error) = self
                .execution_runtime_owner
                .shutdown_executors_until(deadline)
                .await
        {
            if let Some(primary) = primary_error.as_mut() {
                primary.push_str(&format!("; cleanup failed: {error}"));
            } else {
                primary_error = Some(error);
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

/// Finite constant admission used only by explicit Frontend unit fixtures.
#[cfg(test)]
pub(crate) fn test_constant_policy() -> novarocks_functions::ConstantPolicy {
    novarocks_functions::ConstantPolicy {
        max_rows: 1 << 20,
        max_array_nodes: 1 << 20,
        max_logical_elements: 1 << 24,
        max_retained_buffer_bytes: 1 << 30,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 30,
        max_library_validation_bytes: 1 << 32,
    }
}

#[cfg(test)]
#[path = "application/tests_host.rs"]
mod host_tests;

#[cfg(test)]
#[path = "application/tests_mv_host.rs"]
mod mv_host_tests;

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::time::{Duration, Instant};

    use crate::state_store::testing::{
        TEST_STATE_STORE_PROVIDER_ID, input as test_state_store_input,
        registry as test_state_store_registry,
    };
    use crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor;
    use async_trait::async_trait;
    use novarocks_query_application::cpu::{QueryBlockingExecutorConfig, QueryCpuExecutorConfig};
    use novarocks_state_store_api::{
        StateStoreError, StateStoreErrorKind, StateStoreOpenRequest, StateStoreProviderDescriptor,
        StateStoreProviderFactory, StateStoreProviderInstance,
    };
    use novarocks_state_store_runtime::{
        StateStoreHost, StateStoreProviderRegistration, StateStoreProviderRegistry,
    };
    use novarocks_workload_control::{WorkClass, WorkRequest};

    use super::{
        FrontendApplicationError, FrontendApplicationErrorKind, FrontendApplicationHost,
        FrontendExecutionConfig, FrontendExecutionRuntimeOwner, FrontendResultCapacityProfile,
        LogicalExecutionRowsConfig, LogicalExecutionSupervisorConfig, test_native_trust,
    };
    use novarocks_native_adapter::FrontendNativeTransport;

    const DESCRIPTOR: StateStoreProviderDescriptor = StateStoreProviderDescriptor::new(
        TEST_STATE_STORE_PROVIDER_ID,
        novarocks_state_store_api::MAX_KEY_BYTES,
    );

    struct FailingFactory;

    #[tokio::test]
    async fn deadline_supervisor_keeps_an_observed_join_terminal_on_shutdown_retry() {
        let profile =
            crate::native_workload::original_owner_test_profile(QueryBlockingExecutorConfig::new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            ));
        let mut workload = novarocks_workload_control::WorkloadControl::try_new_counted(
            crate::native_workload::original_owner_test_workload(),
        )
        .unwrap();
        let mut blocking = novarocks_query_application::cpu::QueryBlockingExecutorOwner::try_new(
            profile.inputs().query_blocking_capacity(),
        )
        .unwrap();
        let mut cpu = crate::query_execution::internal_result_cpu::InternalResultCpuOwner::try_new(
            profile.inputs().query_blocking_capacity(),
            &mut workload.owner,
        )
        .unwrap();
        let sources = profile
            .capture_sources(
                &crate::native_workload::original_owner_test_workload(),
                FrontendResultCapacityProfile::for_test(),
                &blocking.executor(),
                &cpu.runtime(),
            )
            .unwrap();
        let (_, driver) = profile.install(&mut workload.owner, &sources).unwrap();
        let mut supervisor = super::FrontendWorkloadDeadlineSupervisor::start(
            &tokio::runtime::Handle::current(),
            workload.owner.deadline_expiry_handle(),
            driver,
            super::FrontendWorkloadProgressPolicy::validate(Duration::from_millis(25)).unwrap(),
        );
        workload.owner.close_admission();
        assert!(workload.owner.shutdown().is_ok());
        // Cancel a borrowed wait after requesting stop; the same owner still
        // holds its original join and driver until the subsequent actual join.
        let mut first = Box::pin(supervisor.shutdown_after_workload_drained());
        let was_pending = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(std::future::Future::poll(first.as_mut(), cx).is_pending())
        })
        .await;
        drop(first);
        let joined = supervisor.shutdown_after_workload_drained().await;
        let retried = supervisor.shutdown_after_workload_drained().await;
        let deadline = Instant::now() + Duration::from_secs(1);
        let cpu_exit = cpu.shutdown_until(deadline).await;
        let blocking_exit = blocking.shutdown_until(deadline).await;
        // Observe all original exits before assertions can unwind the fixture.
        assert!(was_pending);
        assert_eq!(joined, Ok(()));
        assert_eq!(retried, Ok(()));
        assert!(cpu_exit.is_ok());
        assert!(blocking_exit.is_ok());
    }

    #[tokio::test]
    async fn execution_runtime_shutdown_deadline_retains_the_same_workload_owner_for_retry() {
        let mut runtime = FrontendExecutionRuntimeOwner::try_new(
            tokio::runtime::Handle::current(),
            ConnectorBlockingIoSupervisor::new(tokio::runtime::Handle::current()),
            novarocks_types::FrontendProcessId::new_v7(),
            LogicalExecutionSupervisorConfig::new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
                LogicalExecutionRowsConfig::new(
                    NonZeroUsize::new(1).unwrap(),
                    NonZeroU32::new(1).unwrap(),
                    Duration::from_secs(1),
                ),
            ),
            crate::native_workload::original_owner_test_workload(),
            FrontendResultCapacityProfile::for_test(),
            QueryCpuExecutorConfig::with_idle_keepalive(Duration::from_millis(20)),
            QueryBlockingExecutorConfig::new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            ),
            super::FrontendLogicalExecutionRuntimeConfig::test_capacity_profile(
                QueryBlockingExecutorConfig::new(
                    NonZeroUsize::new(1).unwrap(),
                    NonZeroUsize::new(1).unwrap(),
                ),
            ),
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

    #[tokio::test]
    async fn execution_runtime_supervises_admitted_statement_deadlines() {
        let mut runtime = FrontendExecutionRuntimeOwner::try_new(
            tokio::runtime::Handle::current(),
            ConnectorBlockingIoSupervisor::new(tokio::runtime::Handle::current()),
            novarocks_types::FrontendProcessId::new_v7(),
            LogicalExecutionSupervisorConfig::new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
                LogicalExecutionRowsConfig::new(
                    NonZeroUsize::new(1).unwrap(),
                    NonZeroU32::new(1).unwrap(),
                    Duration::from_secs(1),
                ),
            ),
            crate::native_workload::original_owner_test_workload(),
            FrontendResultCapacityProfile::for_test(),
            QueryCpuExecutorConfig::with_idle_keepalive(Duration::from_millis(20)),
            QueryBlockingExecutorConfig::new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            ),
            super::FrontendLogicalExecutionRuntimeConfig::test_capacity_profile(
                QueryBlockingExecutorConfig::new(
                    NonZeroUsize::new(1).unwrap(),
                    NonZeroUsize::new(1).unwrap(),
                ),
            ),
        )
        .expect("execution runtime opens");
        runtime.mark_ready().expect("workload authority ready");
        let mut request = WorkRequest::new(WorkClass::Query);
        request.deadline = Some(tokio::time::Instant::from_std(
            Instant::now() + Duration::from_millis(20),
        ));
        let work = runtime
            .root_admission()
            .try_begin_root(request)
            .expect("deadline-bound root work admitted");
        let cancellation = work
            .owner
            .scope()
            .cancellation()
            .expect("root cancellation view");

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), cancellation.cancelled())
                .await
                .expect("role deadline supervisor delivers cancellation"),
            novarocks_workload_control::CancellationReason::DeadlineExceeded
        );

        drop(cancellation);
        work.owner.complete();
        work.business.release();
        runtime
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("deadline-cancelled root still converges through its owner");
    }

    #[tokio::test]
    async fn execution_runtime_shutdown_consumes_terminal_control_notifications() {
        let mut runtime = FrontendExecutionRuntimeOwner::try_new(
            tokio::runtime::Handle::current(),
            ConnectorBlockingIoSupervisor::new(tokio::runtime::Handle::current()),
            novarocks_types::FrontendProcessId::new_v7(),
            LogicalExecutionSupervisorConfig::new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
                LogicalExecutionRowsConfig::new(
                    NonZeroUsize::new(1).unwrap(),
                    NonZeroU32::new(1).unwrap(),
                    Duration::from_secs(1),
                ),
            ),
            crate::native_workload::original_owner_test_workload(),
            FrontendResultCapacityProfile::for_test(),
            QueryCpuExecutorConfig::with_idle_keepalive(Duration::from_millis(20)),
            QueryBlockingExecutorConfig::new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(1).unwrap(),
            ),
            super::FrontendLogicalExecutionRuntimeConfig::test_capacity_profile(
                QueryBlockingExecutorConfig::new(
                    NonZeroUsize::new(1).unwrap(),
                    NonZeroUsize::new(1).unwrap(),
                ),
            ),
        )
        .expect("execution runtime opens");
        runtime.mark_ready().expect("workload authority ready");
        let work = runtime
            .root_admission()
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("root work admitted");
        work.owner
            .cancel(novarocks_workload_control::CancellationReason::Requested);
        work.owner.complete();
        work.business.release();

        runtime
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("terminal control notification cannot retain a completed root");
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

    async fn blocking_test_host(name: &str) -> FrontendApplicationHost {
        let registry = test_state_store_registry();
        let backend = crate::topology::ClusterBackendOpenConfig::new(
            novarocks_types::ClusterRole::Fe,
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            Duration::from_secs(1),
            1,
            Duration::from_secs(1),
        )
        .unwrap();
        let mut host_receiving_11 = None;
        let mut host = FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
            &mut host_receiving_11,
            Some(test_state_store_input(name)),
            &registry,
            FrontendExecutionConfig::new_for_test(
                NonZeroUsize::new(1).unwrap(),
                novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                std::sync::Arc::new(
                    novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap(),
                ),
                crate::application::test_constant_policy(),
            ),
            backend,
            Vec::new(),
            tokio::runtime::Handle::current(),
            test_native_trust(),
            FrontendNativeTransport::plaintext(),
        )
        .await
        .unwrap();
        host.install_statement_control_for_test();
        host.mark_ready()
            .expect("original Host admission becomes ready");
        host
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_drain_retains_the_same_admitted_connector_call_until_actual_join() {
        use crate::task_execution::blocking_io::ConnectorBlockingIoResponsibility;
        let mut host = blocking_test_host("connector-held-host").await;
        let (root, window) = host
            .execution_runtime_owner
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                novarocks_workload_control::ResultWindowClass::Internal,
            )
            .unwrap();
        let responsibility =
            ConnectorBlockingIoResponsibility::admit(&root.owner.scope(), &window.retain_alias())
                .unwrap();
        let pin = responsibility.join_pin();
        let (release, held) = std::sync::mpsc::channel();
        let (started, entered) = tokio::sync::oneshot::channel();
        let (returned, exited) = tokio::sync::oneshot::channel();
        let mut job = host.connector_blocking_io_supervisor().prepare_cpu_owned(
            vec![pin],
            (responsibility, Some(started), held, Some(returned)),
            |input, _| {
                let _ = input.1.take().unwrap().send(());
                input.2.recv().expect("release original Connector call");
                let _ = input.3.take().unwrap().send(());
                Some(())
            },
        );
        std::future::poll_fn(|cx| {
            let _ = job.poll_progress(cx);
            std::task::Poll::Ready(())
        })
        .await;
        tokio::time::timeout(Duration::from_secs(1), entered)
            .await
            .unwrap()
            .unwrap();
        root.owner.complete();
        root.business.release();
        drop(window);
        let first = host
            .shutdown_until(Instant::now() + Duration::from_millis(40))
            .await;
        let remained_held = exited.is_empty();
        let retained = host.execution_runtime_owner.workload_observation.snapshot();
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), exited)
            .await
            .unwrap()
            .unwrap();
        let mut outcome = None;
        job.finish_in(&mut outcome).await;
        assert!(matches!(outcome, Some(Ok(()))));
        drop(job);
        let retry = host
            .shutdown_until(Instant::now() + Duration::from_secs(2))
            .await;
        assert!(first.is_err() && remained_held);
        assert_eq!(retained.root_responsibilities, 1);
        assert_eq!(retained.result_windows.held_positions, [0, 0, 1, 0]);
        retry.expect("same Host drains after original join");
        assert!(
            host.execution_runtime_owner
                .workload_observation
                .snapshot()
                .scopes
                .is_empty()
        );
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
        let mut host_receiving_10 = None;
        let mut host = FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
            &mut host_receiving_10,
            Some(state_store),
            &registry,
            FrontendExecutionConfig::new_for_test(
                NonZeroUsize::new(1).expect("non-zero runtime-filter workers"),
                novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                std::sync::Arc::new(
                    novarocks_sql::compiler::build_builtin_engine_function_catalog()
                        .expect("builtin function catalog"),
                ),
                crate::application::test_constant_policy(),
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
            novarocks_catalog_application::CatalogApplicationPort::admit_catalog(
                host.catalog_application_port().as_ref(),
                &instance_id,
            ),
            novarocks_catalog_application::CatalogAdmission::Absent
        ));
        host.shutdown().await.expect("host shutdown");
    }
}

#[cfg(all(test, feature = "mem-1-m07-closing-pressure"))]
mod closing_pressure_role_error_tests {
    use super::*;

    #[test]
    fn four_original_verdict_boxes_preserve_primary_and_each_cleanup_source() {
        let sources: [std::sync::Arc<()>; 4] = std::array::from_fn(|_| std::sync::Arc::new(()));
        #[derive(Debug)]
        struct Actual(std::sync::Arc<()>);
        impl fmt::Display for Actual {
            fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
                panic!("unknown original source cannot be formatted");
            }
        }
        impl std::error::Error for Actual {}
        let mut primary = FrontendApplicationError::server("original primary");
        for source in &sources {
            primary =
                primary.with_role_cleanup(FrontendApplicationError::server_pressure_registry(
                    std::io::Error::other(Actual(source.clone())),
                ));
        }
        assert!(primary.message.starts_with("original primary"));
        assert_eq!(primary.pressure_failures.iter().flatten().count(), 4);
        for (slot, expected) in primary.pressure_failures.iter().flatten().zip(&sources) {
            let actual = slot
                .original()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .get_ref()
                .unwrap()
                .downcast_ref::<Actual>()
                .unwrap();
            assert!(std::sync::Arc::ptr_eq(&actual.0, expected));
        }
        let _finite = format!("{primary:?} {primary}");
    }
}
