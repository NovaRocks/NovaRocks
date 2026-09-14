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

use novarocks_workload_control::{
    BusinessPermit, RootAdmissionHandle, RootWork, WorkClass, WorkOwner, WorkRequest,
};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::background::{MvBackgroundBindings, MvBackgroundEngine};
use crate::mv::domain::application::{MvApplicationError, MvEngine, MvRequestContext};
use crate::mv::domain::readiness::MvReadinessPort;
use crate::query_execution::maintenance::{TableMaintenanceEngine, TableMaintenanceService};
use crate::query_execution::mv_assembly::refresh_handoff::{
    MvRefreshPreparationRequest, MvRefreshPreparationService, PreparedMvRefresh,
    PreparedMvRefreshWork,
};
use crate::query_execution::service::QueryExecutionService;
use novarocks_mv_application::{
    activity::{MvActivityAdmissionError, MvActivityLease, MvActivityOwner},
    maintenance::{
        MaintenanceCoordinatorConfig, MvBackgroundEngineError, MvBackgroundEngineErrorKind,
    },
    process_runtime::{MvBackgroundRuntime, MvBackgroundStop, MvBackgroundTasks},
    service::MvProductService,
};
use novarocks_query_application::admitted_query_context::{RequestAdmission, RequestContext};
use novarocks_query_application::api::BackendTopologyService;
use novarocks_spi::connector::{ConnectorControlRegistry, ConnectorRequestContext};
use novarocks_sql::compiler::SessionOptimizerSettings;

use super::{
    activity::canonical_mv_target,
    create,
    maintenance_worker::{FrontendMaintenanceWorker, FrontendMaintenanceWorkerDependencies},
    refresh,
    scheduler::{self, ScheduledRefreshDisposition, ScheduledRefreshRequest},
};

/// Frontend composition and statement adapter for materialized-view products.
///
/// MVX-1 owns only Iceberg CREATE sequencing. Other MV statement classes
/// deliberately return `None` so their existing core routes remain active.
pub struct FrontendMvProductAdapter {
    readiness: Arc<MvReadinessPort>,
    refresh: refresh::FrontendMvRefreshDependencies,
    product_service: Arc<MvProductService>,
    maintenance_config: MaintenanceCoordinatorConfig,
    table_maintenance_service: Arc<dyn TableMaintenanceService>,
    execution_role: novarocks_types::ClusterRole,
    topology: BackendTopologyService,
    /// Cost budget frozen from `[runtime]`; the MV worker has no session, so it
    /// carries the value that statement admission would otherwise have to guess.
    optimizer_query_mem_limit_bytes: u64,
    attempt_timeout: Duration,
    root_admission: RootAdmissionHandle,
}

impl FrontendMvProductAdapter {
    #[expect(
        clippy::too_many_arguments,
        reason = "Frontend MV composition keeps independently owned ports explicit at the application boundary."
    )]
    pub(crate) fn with_refresh_dependencies(
        readiness: Arc<MvReadinessPort>,
        query_execution: QueryExecutionService,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        provider_activation: Arc<
            dyn crate::query_execution::mv_native_write::MvRefreshProviderActivation,
        >,
        execution_role: novarocks_types::ClusterRole,
        topology: BackendTopologyService,
        product_service: Arc<MvProductService>,
        maintenance_config: MaintenanceCoordinatorConfig,
        table_maintenance_service: Arc<dyn TableMaintenanceService>,
        optimizer_query_mem_limit_bytes: u64,
        attempt_timeout: Duration,
        root_admission: RootAdmissionHandle,
    ) -> Self {
        Self {
            refresh: refresh::FrontendMvRefreshDependencies {
                query_execution,
                connector_control: Arc::clone(&connector_control),
                provider_activation: Arc::clone(&provider_activation),
                readiness: Arc::clone(&readiness),
            },
            readiness: Arc::clone(&readiness),
            product_service,
            maintenance_config,
            table_maintenance_service,
            execution_role,
            topology,
            optimizer_query_mem_limit_bytes,
            attempt_timeout,
            root_admission,
        }
    }

    pub(crate) fn readiness_port(&self) -> Arc<MvReadinessPort> {
        Arc::clone(&self.readiness)
    }

    pub(crate) fn product_service(&self) -> &MvProductService {
        self.product_service.as_ref()
    }

    pub(crate) async fn shutdown_background_workers_until(
        &self,
        deadline: Instant,
    ) -> Result<(), String> {
        self.product_service.begin_stopping();
        self.product_service
            .shutdown_background_workers_until(deadline)
            .await
    }

    pub(crate) fn request_background_stop_for_process_exit(&self) {
        self.product_service.begin_stopping();
        self.product_service
            .request_background_stop_for_process_exit();
    }

    pub(crate) fn start_background_workers(
        &self,
        bindings: MvBackgroundBindings,
    ) -> Result<(), MvBackgroundEngineError> {
        let reservation = self
            .product_service
            .begin_background_start()
            .map_err(lifecycle_error)?;
        let dependencies = self.refresh.clone();
        let topology = self.topology.clone();
        let table_maintenance_service = Arc::clone(&self.table_maintenance_service);
        let runtime = start_background_workers(RefreshWorkerDependencies {
            readiness: Arc::clone(&self.readiness),
            refresh: dependencies,
            background_engine: bindings.engine,
            topology,
            role: self.execution_role,
            maintenance_config: self.maintenance_config.clone(),
            table_maintenance_engine: bindings.table_maintenance_engine,
            table_maintenance_service,
            product_service: Arc::clone(&self.product_service),
            root_admission: self.root_admission.clone(),
            optimizer_query_mem_limit_bytes: self.optimizer_query_mem_limit_bytes,
            attempt_timeout: self.attempt_timeout,
        })?;
        reservation.install(runtime).map_err(lifecycle_error)
    }
}

impl FrontendMvProductAdapter {
    pub(crate) fn execute_create(
        &self,
        engine: &dyn MvEngine,
        statement: &crate::mv::domain::application::MvCreateStatement,
        context: MvRequestContext<'_>,
    ) -> Result<(), MvApplicationError> {
        create::handle_create(&self.product_service, engine, statement, context)
    }

    pub fn execute_prepared_refresh(
        &self,
        refresh_plan: PreparedMvRefresh,
        connector_context: ConnectorRequestContext,
        execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
    ) -> Result<(), MvApplicationError> {
        refresh::execute(
            self.product_service.as_ref(),
            &self.refresh,
            refresh_plan,
            connector_context,
            execution,
        )
    }

    pub(crate) fn prepare_and_execute_refresh(
        &self,
        preparation: &dyn MvRefreshPreparationService,
        statement: novarocks_sql::planning::mv::MvRefreshStatement,
        target: novarocks_sql::planning::mv::SqlMvTarget,
        owner: MvActivityOwner,
        connector_context: ConnectorRequestContext,
        execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
    ) -> Result<(), MvApplicationError> {
        let _gate_lease = self.acquire_activity_lease(&target, owner, execution)?;
        let attempt = self.product_service.reserve_refresh_attempt();
        let publication_id = attempt.publication_id.as_uuid();
        let _diagnostic_scope = crate::preparation_diagnostics::enter_product_work(
            format!("mv-publication:{publication_id}"),
            format!("mv-publication:{publication_id}"),
        );
        let prepared = preparation
            .prepare_step(MvRefreshPreparationRequest {
                statement,
                target,
                attempt: attempt.clone(),
            })
            .map_err(preparation_application_error)?;
        if prepared.attempt != attempt {
            return Err(MvApplicationError::new(
                crate::mv::domain::application::MvApplicationErrorKind::InvalidRequest,
                "MV refresh preparation changed the frontend-reserved attempt identity",
            ));
        }
        self.execute_prepared_refresh(prepared, connector_context, execution)
    }

    /// Run one foreground DDL path under the same per-target FIFO gate as
    /// refresh and background maintenance. The SQL session cancellation scope
    /// remains the authority while the statement waits for its turn.
    pub(crate) fn execute_serialized<T>(
        &self,
        target: &novarocks_sql::planning::mv::SqlMvTarget,
        owner: MvActivityOwner,
        execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
        action: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, String> {
        let _gate_lease = self
            .acquire_activity_lease(target, owner, execution)
            .map_err(|error| error.to_string())?;
        action()
    }

    fn acquire_activity_lease(
        &self,
        target: &novarocks_sql::planning::mv::SqlMvTarget,
        owner: MvActivityOwner,
        execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
    ) -> Result<MvActivityLease, MvApplicationError> {
        self.product_service
            .acquire_foreground(canonical_mv_target(target), owner, || {
                execution.cancellation().is_cancelled()
            })
            .map_err(|error| match error {
                MvActivityAdmissionError::Cancelled => MvApplicationError::new(
                    crate::mv::domain::application::MvApplicationErrorKind::ShutdownCancelled,
                    "MV statement was cancelled while waiting for activity gate",
                ),
                MvActivityAdmissionError::Stopping => MvApplicationError::new(
                    crate::mv::domain::application::MvApplicationErrorKind::ShutdownCancelled,
                    "frontend MV activity admission is closed",
                ),
            })
    }
}

fn preparation_application_error(
    error: crate::mv::domain::lifecycle::RefreshError,
) -> MvApplicationError {
    use crate::mv::domain::application::MvApplicationErrorKind;
    use crate::mv::domain::lifecycle::RefreshErrorKind;

    let kind = match error.kind {
        RefreshErrorKind::PreCommitFailed => MvApplicationErrorKind::Unavailable,
        RefreshErrorKind::UserError => MvApplicationErrorKind::InvalidRequest,
        RefreshErrorKind::CommitFailedKnownUncommitted => MvApplicationErrorKind::TerminalFailure,
        RefreshErrorKind::CommitFailedKnownCommitted | RefreshErrorKind::MetadataFinalizeFailed => {
            MvApplicationErrorKind::KnownCommittedFinalizeFailed
        }
        RefreshErrorKind::CommitUnknown => MvApplicationErrorKind::CommitUnknown,
    };
    MvApplicationError::new(kind, error.message)
}

#[derive(Clone)]
struct RefreshWorkerDependencies {
    readiness: Arc<MvReadinessPort>,
    refresh: refresh::FrontendMvRefreshDependencies,
    background_engine: Arc<dyn MvBackgroundEngine>,
    topology: BackendTopologyService,
    role: novarocks_types::ClusterRole,
    maintenance_config: MaintenanceCoordinatorConfig,
    table_maintenance_engine: Arc<dyn TableMaintenanceEngine>,
    table_maintenance_service: Arc<dyn TableMaintenanceService>,
    product_service: Arc<MvProductService>,
    root_admission: RootAdmissionHandle,
    optimizer_query_mem_limit_bytes: u64,
    attempt_timeout: Duration,
}

fn start_background_workers(
    dependencies: RefreshWorkerDependencies,
) -> Result<MvBackgroundRuntime, MvBackgroundEngineError> {
    let interval = Duration::from_millis(
        dependencies
            .product_service
            .scheduler_tick_interval_ms()
            .max(1),
    );
    let maintenance_interval =
        Duration::from_millis(dependencies.maintenance_config.tick_interval_ms.max(1));
    let maintenance = Arc::new(FrontendMaintenanceWorker::new(
        FrontendMaintenanceWorkerDependencies {
            readiness: Arc::clone(&dependencies.readiness),
            background_engine: Arc::clone(&dependencies.background_engine),
            table_maintenance_engine: Arc::clone(&dependencies.table_maintenance_engine),
            table_maintenance_service: Arc::clone(&dependencies.table_maintenance_service),
            product_service: Arc::clone(&dependencies.product_service),
            root_admission: dependencies.root_admission.clone(),
            coordinator_config: dependencies.maintenance_config.clone(),
            attempt_timeout: dependencies.attempt_timeout,
            runtime: tokio::runtime::Handle::current(),
        },
    ));
    let refresh_task_dependencies = dependencies;
    MvBackgroundRuntime::start(
        interval,
        maintenance_interval,
        MvBackgroundTasks::new(
            Box::new(move |stop, maintenance_wakeup_tx| {
                run_refresh_event(&refresh_task_dependencies, stop, maintenance_wakeup_tx);
            }),
            Box::new(move |_| {
                if let Err(error) = maintenance.run_once(now_unix_millis()) {
                    tracing::warn!(error = %error, "frontend MV maintenance inventory failed");
                }
            }),
        ),
    )
    .map_err(|error| {
        MvBackgroundEngineError::new(MvBackgroundEngineErrorKind::TransientUnavailable, error)
    })
}

fn lifecycle_error(error: impl std::fmt::Display) -> MvBackgroundEngineError {
    MvBackgroundEngineError::new(
        MvBackgroundEngineErrorKind::InvariantViolation,
        error.to_string(),
    )
}

fn run_refresh_event(
    dependencies: &RefreshWorkerDependencies,
    stop: &MvBackgroundStop,
    maintenance_wakeup_tx: &std::sync::mpsc::SyncSender<()>,
) {
    let now_ms = now_unix_millis();
    match scheduler::poll(
        dependencies.product_service.as_ref(),
        dependencies.readiness.as_ref(),
        dependencies.background_engine.as_ref(),
        now_ms,
    ) {
        Ok(requests) => {
            run_scheduled_refreshes(dependencies, requests, stop, maintenance_wakeup_tx);
        }
        Err(error) => tracing::warn!(error = %error, "frontend MV scheduler poll failed"),
    }
}

fn run_scheduled_refreshes(
    dependencies: &RefreshWorkerDependencies,
    requests: Vec<ScheduledRefreshRequest>,
    stop: &MvBackgroundStop,
    maintenance_wakeup_tx: &std::sync::mpsc::SyncSender<()>,
) {
    for request in requests {
        if stop.is_requested() {
            scheduler::requeue(dependencies.product_service.as_ref(), request);
            break;
        }
        let RootWork { owner, business } = match dependencies
            .root_admission
            .try_begin_root(WorkRequest::new(WorkClass::MaterializedView))
        {
            Ok(work) => work,
            Err(_) => {
                // Draining is terminal for this process runtime. Preserve the
                // coalesced request without creating a new refresh attempt.
                scheduler::requeue(dependencies.product_service.as_ref(), request);
                break;
            }
        };
        let cancellation = match owner.scope().cancellation() {
            Ok(view) => novarocks_query_application::cancellation::QueryCancellationView::governed(
                view, None,
            ),
            Err(_) => {
                finish_background_root(owner, business);
                scheduler::requeue(dependencies.product_service.as_ref(), request);
                break;
            }
        };
        let mut ticket = match dependencies.product_service.request_activity(
            canonical_mv_target(&request.target),
            MvActivityOwner::ScheduledRefresh,
        ) {
            Ok(ticket) => ticket,
            Err(_) => {
                finish_background_root(owner, business);
                continue;
            }
        };
        let lease = match ticket.try_acquire() {
            Ok(Some(lease)) => lease,
            Ok(None) => {
                finish_background_root(owner, business);
                scheduler::requeue(dependencies.product_service.as_ref(), request);
                continue;
            }
            Err(_) => {
                finish_background_root(owner, business);
                continue;
            }
        };
        if scheduler::mark_started(
            dependencies.product_service.as_ref(),
            request.definition().mv_id,
        ) {
            // The scheduler has already bounded this batch. Execute its
            // transitions on this event loop rather than creating an OS thread
            // for every due MV. Keeping the activity lease and governed root local
            // to the same transition makes `complete` the exact terminal
            // observation before the next event starts.
            let disposition = execute_scheduled_refresh(dependencies, &request, cancellation);
            let completed = matches!(disposition, ScheduledRefreshDisposition::Completed);
            if let Some((disposition_kind, reason)) = scheduler_outcome_log_fields(&disposition) {
                tracing::warn!(
                    mv_id = request.definition().mv_id,
                    target = %request.target.display_name(),
                    disposition_kind,
                    reason = %reason,
                    "frontend MV scheduler refresh did not complete"
                );
            }
            let _decision = scheduler::complete(
                dependencies.product_service.as_ref(),
                &request,
                disposition,
                now_unix_millis(),
            );
            if completed {
                let _ = maintenance_wakeup_tx.try_send(());
            }
            // Release the activity lease only after the scheduler terminal is
            // durable, then complete the governed root.
            drop(lease);
            finish_background_root(owner, business);
        } else {
            finish_background_root(owner, business);
            scheduler::requeue(dependencies.product_service.as_ref(), request);
        }
    }
}

fn finish_background_root(owner: WorkOwner, business: BusinessPermit) {
    drop(business);
    owner.complete();
}

fn scheduler_outcome_log_fields(
    disposition: &ScheduledRefreshDisposition,
) -> Option<(&'static str, &str)> {
    match disposition {
        ScheduledRefreshDisposition::TransientUnavailable(reason) => {
            Some(("transient_unavailable", reason))
        }
        ScheduledRefreshDisposition::InvalidDefinition(reason) => {
            Some(("invalid_definition", reason))
        }
        ScheduledRefreshDisposition::TerminalFailure(reason) => Some(("terminal_failure", reason)),
        ScheduledRefreshDisposition::Corruption(reason) => Some(("corruption", reason)),
        ScheduledRefreshDisposition::InvariantViolation(reason) => {
            Some(("invariant_violation", reason))
        }
        ScheduledRefreshDisposition::TargetGone => {
            Some(("target_gone", "MV target no longer exists"))
        }
        ScheduledRefreshDisposition::Completed
        | ScheduledRefreshDisposition::NoOp
        | ScheduledRefreshDisposition::AlreadyActive
        | ScheduledRefreshDisposition::ShutdownCancelled => None,
    }
}

fn execute_scheduled_refresh(
    dependencies: &RefreshWorkerDependencies,
    request: &ScheduledRefreshRequest,
    cancellation: novarocks_query_application::cancellation::QueryCancellationView,
) -> ScheduledRefreshDisposition {
    if scheduled_refresh_test_barrier(&request.target, &cancellation) {
        return ScheduledRefreshDisposition::ShutdownCancelled;
    }
    let topology = match dependencies.topology.snapshot() {
        Ok(snapshot) => snapshot,
        Err(error) => return ScheduledRefreshDisposition::TransientUnavailable(error.to_string()),
    };
    // FE restart begins before authenticated BE announces have rebuilt the
    // runtime topology. The MV definition remains valid, so retry after the
    // frontend observes at least one admitted backend instead of blocking it.
    if topology.targets().is_empty() {
        return ScheduledRefreshDisposition::TransientUnavailable(
            "scheduler refresh is waiting for a non-empty admitted backend topology".to_string(),
        );
    }
    let deadline = match Instant::now().checked_add(dependencies.attempt_timeout) {
        Some(deadline) => deadline,
        None => {
            return ScheduledRefreshDisposition::InvariantViolation(
                "MV worker deadline overflow".to_string(),
            );
        }
    };
    let context = RequestContext::admit(RequestAdmission::new(
        request.target.catalog.clone(),
        request.target.database.clone(),
        dependencies.role,
        topology,
        Some(deadline),
        cancellation.clone(),
        SessionOptimizerSettings {
            optimizer_query_mem_limit_bytes: Some(
                dependencies.optimizer_query_mem_limit_bytes as f64,
            ),
            ..SessionOptimizerSettings::default()
        },
    ));
    let connector_context = match crate::connector::connector_request_context_for_execution(
        None,
        context.execution(),
    ) {
        Ok(context) => context,
        Err(error) => return ScheduledRefreshDisposition::TransientUnavailable(error),
    };
    if cancellation.is_cancelled() {
        return ScheduledRefreshDisposition::ShutdownCancelled;
    }
    let steps = match dependencies
        .background_engine
        .resolve_refresh_steps(&request.target)
    {
        Ok(steps) => steps,
        Err(error) => return ScheduledRefreshDisposition::from_background_error(error),
    };
    for step in steps {
        if cancellation.is_cancelled() {
            return ScheduledRefreshDisposition::ShutdownCancelled;
        }
        let attempt = dependencies.product_service.reserve_refresh_attempt();
        let publication_id = attempt.publication_id.as_uuid();
        let _diagnostic_scope = crate::preparation_diagnostics::enter_product_work(
            format!("mv-publication:{publication_id}"),
            format!("mv-publication:{publication_id}"),
        );
        let prepared = match dependencies.background_engine.prepare_refresh_step(
            &step,
            attempt,
            &connector_context,
        ) {
            Ok(prepared) => prepared,
            Err(error) => return ScheduledRefreshDisposition::from_background_error(error),
        };
        let no_op = matches!(prepared.work, PreparedMvRefreshWork::NoOp);
        if let Err(error) = refresh::execute(
            dependencies.product_service.as_ref(),
            &dependencies.refresh,
            prepared,
            connector_context.clone(),
            context.execution(),
        ) {
            return application_disposition(error);
        }
        if no_op {
            return ScheduledRefreshDisposition::NoOp;
        }
    }
    ScheduledRefreshDisposition::Completed
}

/// Debug-only native-test seam for asserting that frontend scheduler permits
/// bound actual refresh execution, rather than just queue admission.  Normal
/// production builds do not inspect this environment variable.
#[cfg(debug_assertions)]
fn scheduled_refresh_test_barrier(
    target: &novarocks_sql::planning::mv::SqlMvTarget,
    cancellation: &novarocks_query_application::cancellation::QueryCancellationView,
) -> bool {
    let Some(directory) = std::env::var_os("NOVAROCKS_MVX4_SCHEDULER_TEST_DIR") else {
        return false;
    };
    let directory = std::path::PathBuf::from(directory);
    let marker = directory.join(format!("mvx4-scheduler-admitted-{}.marker", target.name));
    let _ = std::fs::write(marker, "admitted\n");
    let hold = directory.join("mvx4-scheduler-hold.trigger");
    while hold.exists() {
        if cancellation.is_cancelled() {
            return true;
        }
        std::thread::park_timeout(Duration::from_millis(10));
    }
    cancellation.is_cancelled()
}

#[cfg(not(debug_assertions))]
fn scheduled_refresh_test_barrier(
    _target: &novarocks_sql::planning::mv::SqlMvTarget,
    _cancellation: &novarocks_query_application::cancellation::QueryCancellationView,
) -> bool {
    false
}

fn repository_disposition(
    error: novarocks_mv_application::repository::MvRepositoryError,
) -> ScheduledRefreshDisposition {
    use novarocks_mv_application::repository::MvRepositoryErrorKind;
    match error.kind() {
        MvRepositoryErrorKind::Conflict => ScheduledRefreshDisposition::AlreadyActive,
        MvRepositoryErrorKind::NotFound => ScheduledRefreshDisposition::TargetGone,
        MvRepositoryErrorKind::Unavailable => {
            ScheduledRefreshDisposition::TransientUnavailable(error.to_string())
        }
        MvRepositoryErrorKind::Corruption => {
            ScheduledRefreshDisposition::Corruption(error.to_string())
        }
        MvRepositoryErrorKind::CommitUnknown => {
            ScheduledRefreshDisposition::TerminalFailure(error.to_string())
        }
        MvRepositoryErrorKind::InvalidRequest => {
            ScheduledRefreshDisposition::InvariantViolation(error.to_string())
        }
    }
}

fn application_disposition(error: MvApplicationError) -> ScheduledRefreshDisposition {
    use crate::mv::domain::application::MvApplicationErrorKind;
    match error.kind() {
        MvApplicationErrorKind::AlreadyActive => ScheduledRefreshDisposition::AlreadyActive,
        MvApplicationErrorKind::TargetGone => ScheduledRefreshDisposition::TargetGone,
        MvApplicationErrorKind::Unavailable | MvApplicationErrorKind::BindingInvalidated => {
            ScheduledRefreshDisposition::TransientUnavailable(error.message().to_owned())
        }
        MvApplicationErrorKind::InvalidRequest => {
            ScheduledRefreshDisposition::InvalidDefinition(error.message().to_owned())
        }
        MvApplicationErrorKind::Corruption => {
            ScheduledRefreshDisposition::Corruption(error.message().to_owned())
        }
        MvApplicationErrorKind::TerminalFailure
        | MvApplicationErrorKind::CommitUnknown
        | MvApplicationErrorKind::KnownCommittedFinalizeFailed => {
            ScheduledRefreshDisposition::TerminalFailure(error.message().to_owned())
        }
        MvApplicationErrorKind::ShutdownCancelled => ScheduledRefreshDisposition::ShutdownCancelled,
        MvApplicationErrorKind::Engine | MvApplicationErrorKind::Repository => {
            ScheduledRefreshDisposition::InvariantViolation(error.message().to_owned())
        }
    }
}

fn now_unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod shutdown_tests {
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    use super::{MvBackgroundRuntime, application_disposition};
    use crate::mv::domain::application::{MvApplicationError, MvApplicationErrorKind};
    use crate::mv::scheduler::ScheduledRefreshDisposition;

    #[test]
    fn pre_dispatch_binding_invalidation_is_reprepared_not_recorded_as_unknown() {
        let disposition = application_disposition(MvApplicationError::new(
            MvApplicationErrorKind::BindingInvalidated,
            "test target generation changed",
        ));

        assert!(matches!(
            disposition,
            ScheduledRefreshDisposition::TransientUnavailable(_)
        ));
    }

    #[tokio::test]
    async fn shared_deadline_retains_the_same_mv_worker_join_for_retry() {
        let (stop_tx, _stop_rx) = mpsc::channel();
        let (maintenance_stop_tx, _maintenance_stop_rx) = mpsc::channel();
        let (maintenance_wakeup_tx, _maintenance_wakeup_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::channel();
        let refresh_worker = thread::spawn(move || {
            let _ = release_rx.recv();
        });
        let maintenance_worker = thread::spawn(|| {});
        let mut runtime = MvBackgroundRuntime::new(
            stop_tx,
            refresh_worker,
            maintenance_stop_tx,
            maintenance_wakeup_tx,
            maintenance_worker,
        );

        let error = runtime
            .stop_and_join_until(Instant::now() + Duration::from_millis(10))
            .await
            .expect_err("blocked MV worker must respect the shared deadline");
        assert!(error.contains("shared shutdown deadline"));

        release_tx.send(()).unwrap();
        runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("the retained MV worker join remains retryable");
    }

    #[tokio::test]
    async fn retry_still_joins_mv_maintenance_after_refresh_already_stopped() {
        let (stop_tx, _stop_rx) = mpsc::channel();
        let (maintenance_stop_tx, _maintenance_stop_rx) = mpsc::channel();
        let (maintenance_wakeup_tx, _maintenance_wakeup_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::channel();
        let refresh_worker = thread::spawn(|| {});
        let maintenance_worker = thread::spawn(move || {
            let _ = release_rx.recv();
        });
        let mut runtime = MvBackgroundRuntime::new(
            stop_tx,
            refresh_worker,
            maintenance_stop_tx,
            maintenance_wakeup_tx,
            maintenance_worker,
        );

        let error = runtime
            .stop_and_join_until(Instant::now() + Duration::from_millis(10))
            .await
            .expect_err("blocked MV maintenance must respect the shared deadline");
        assert!(error.contains("shared shutdown deadline"));

        release_tx.send(()).unwrap();
        runtime
            .stop_and_join_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("retry must join the retained MV maintenance worker");
    }
}
