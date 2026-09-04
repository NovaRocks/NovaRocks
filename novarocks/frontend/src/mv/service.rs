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

use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::background::{
    MvBackgroundBindings, MvBackgroundEngine, MvBackgroundEngineError, MvBackgroundEngineErrorKind,
    MvBackgroundEngineSink,
};
use crate::common::admitted_query_context::{
    RequestAdmission, RequestContext, SessionOptimizerSettings,
};
use crate::common::backend_topology::BackendTopologyService;
use crate::mv::domain::application::{
    MvApplicationError, MvApplicationService, MvApplicationStatement, MvEngine, MvRequestContext,
    MvStatementResult,
};
use crate::mv::domain::readiness::MvReadinessPort;
use crate::mv::domain::repository::MvRepository;
use crate::mv::process_runtime::ProcessRuntime;
use crate::query_execution::maintenance::{TableMaintenanceEngine, TableMaintenanceService};
use crate::query_execution::mv_assembly::refresh_handoff::{
    MvRefreshAttemptIdentity, MvRefreshPreparationRequest, MvRefreshPreparationService,
    PreparedMvRefresh, PreparedMvRefreshWork,
};
use crate::query_execution::service::QueryExecutionService;
use crate::workload_lifecycle::{FrontendServingLifecycle, FrontendWorkloadKind};
use novarocks_spi::connector::{ConnectorControlRegistry, ConnectorRequestContext};

use super::{
    activity::{CanonicalMvTarget, MvActivityGate, MvActivityOwner},
    create,
    maintenance::MaintenanceCoordinatorConfig,
    maintenance_worker::{FrontendMaintenanceWorker, FrontendMaintenanceWorkerDependencies},
    refresh,
    scheduler::{
        FrontendMvScheduler, FrontendMvSchedulerConfig, ScheduledRefreshDisposition,
        ScheduledRefreshRequest,
    },
};

const MV_WORKER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const MV_WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Frontend-owned application service for materialized-view statements.
///
/// MVX-1 owns only Iceberg CREATE sequencing. Other MV statement classes
/// deliberately return `None` so their existing core routes remain active.
pub struct FrontendMvService {
    readiness: Arc<MvReadinessPort>,
    refresh: Option<refresh::FrontendMvRefreshDependencies>,
    activity_gate: MvActivityGate,
    background: Mutex<Option<FrontendMvBackgroundRuntime>>,
    scheduler_config: FrontendMvSchedulerConfig,
    maintenance_config: MaintenanceCoordinatorConfig,
    table_maintenance_service: Option<Arc<dyn TableMaintenanceService>>,
    execution_role: novarocks_types::ClusterRole,
    topology: Option<BackendTopologyService>,
    /// Cost budget frozen from `[runtime]`; the MV worker has no session, so it
    /// carries the value that statement admission would otherwise have to guess.
    optimizer_query_mem_limit_bytes: u64,
    attempt_timeout: Duration,
    workload_lifecycle: Option<FrontendServingLifecycle>,
}

impl FrontendMvService {
    pub fn new(repository: Arc<dyn MvRepository>) -> Self {
        let runtime = Arc::new(ProcessRuntime::default());
        Self {
            readiness: Arc::new(MvReadinessPort::new(Arc::clone(&repository), runtime)),
            refresh: None,
            activity_gate: MvActivityGate::new(),
            background: Mutex::new(None),
            scheduler_config: FrontendMvSchedulerConfig::default(),
            maintenance_config: MaintenanceCoordinatorConfig::default(),
            table_maintenance_service: None,
            execution_role: novarocks_types::ClusterRole::Fe,
            topology: None,
            optimizer_query_mem_limit_bytes: 2 * 1024 * 1024 * 1024,
            attempt_timeout: MV_WORKER_ATTEMPT_TIMEOUT,
            workload_lifecycle: None,
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Frontend MV composition keeps independently owned ports explicit at the application boundary."
    )]
    pub(crate) fn with_refresh_dependencies(
        repository: Arc<dyn MvRepository>,
        query_execution: QueryExecutionService,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        provider_activation: Arc<refresh::FrontendMvRefreshProviderActivationPort>,
        execution_role: novarocks_types::ClusterRole,
        topology: BackendTopologyService,
        scheduler_config: FrontendMvSchedulerConfig,
        maintenance_config: MaintenanceCoordinatorConfig,
        table_maintenance_service: Arc<dyn TableMaintenanceService>,
        optimizer_query_mem_limit_bytes: u64,
        attempt_timeout: Duration,
    ) -> Self {
        let runtime = Arc::new(ProcessRuntime::default());
        let readiness = Arc::new(MvReadinessPort::new(Arc::clone(&repository), runtime));
        Self {
            refresh: Some(refresh::FrontendMvRefreshDependencies {
                query_execution,
                connector_control: Arc::clone(&connector_control),
                provider_activation: Arc::clone(&provider_activation),
                readiness: Arc::clone(&readiness),
            }),
            readiness,
            activity_gate: MvActivityGate::new(),
            background: Mutex::new(None),
            scheduler_config,
            maintenance_config,
            table_maintenance_service: Some(table_maintenance_service),
            execution_role,
            topology: Some(topology),
            optimizer_query_mem_limit_bytes,
            attempt_timeout,
            workload_lifecycle: None,
        }
    }

    /// Installs the FE-local owner that admits every effect-capable MV
    /// background attempt. The application host must install the one shared
    /// lifecycle before binding the background engine.
    pub(crate) fn with_workload_lifecycle(mut self, lifecycle: FrontendServingLifecycle) -> Self {
        self.workload_lifecycle = Some(lifecycle);
        self
    }

    pub(crate) fn background_engine_sink(service: Arc<Self>) -> Arc<dyn MvBackgroundEngineSink> {
        Arc::new(FrontendMvBackgroundEngineSink { service })
    }

    pub(crate) fn readiness_port(&self) -> Arc<MvReadinessPort> {
        Arc::clone(&self.readiness)
    }

    pub(crate) fn shutdown_background_workers(&self) -> Result<(), String> {
        self.activity_gate.begin_stopping();
        let mut guard = self
            .background
            .lock()
            .map_err(|error| format!("lock frontend MV worker lifecycle: {error}"))?;
        let Some(runtime) = guard.as_mut() else {
            return Ok(());
        };
        runtime.stop_and_join(Instant::now() + MV_WORKER_SHUTDOWN_TIMEOUT)?;
        guard.take();
        Ok(())
    }

    fn bind_background_engine(
        &self,
        bindings: MvBackgroundBindings,
    ) -> Result<(), MvBackgroundEngineError> {
        let dependencies = self.refresh.clone().ok_or_else(|| {
            MvBackgroundEngineError::new(
                MvBackgroundEngineErrorKind::InvariantViolation,
                "frontend MV refresh dependencies are not installed",
            )
        })?;
        let topology = self.topology.clone().ok_or_else(|| {
            MvBackgroundEngineError::new(
                MvBackgroundEngineErrorKind::InvariantViolation,
                "frontend MV worker topology is not installed",
            )
        })?;
        let table_maintenance_service =
            self.table_maintenance_service.clone().ok_or_else(|| {
                MvBackgroundEngineError::new(
                    MvBackgroundEngineErrorKind::InvariantViolation,
                    "frontend table-maintenance service is not installed",
                )
            })?;
        let workload_lifecycle = self.workload_lifecycle.clone().ok_or_else(|| {
            MvBackgroundEngineError::new(
                MvBackgroundEngineErrorKind::InvariantViolation,
                "frontend MV background workers require the shared serving lifecycle",
            )
        })?;
        let mut guard = self.background.lock().map_err(|error| {
            MvBackgroundEngineError::new(
                MvBackgroundEngineErrorKind::InvariantViolation,
                format!("lock frontend MV worker lifecycle: {error}"),
            )
        })?;
        if guard.is_some() {
            return Err(MvBackgroundEngineError::new(
                MvBackgroundEngineErrorKind::InvariantViolation,
                "frontend MV background engine was bound more than once",
            ));
        }
        *guard = Some(FrontendMvBackgroundRuntime::start(
            RefreshWorkerDependencies {
                readiness: Arc::clone(&self.readiness),
                refresh: dependencies,
                background_engine: bindings.engine,
                topology,
                role: self.execution_role,
                scheduler_config: self.scheduler_config.clone(),
                maintenance_config: self.maintenance_config.clone(),
                table_maintenance_engine: bindings.table_maintenance_engine,
                table_maintenance_service,
                activity_gate: self.activity_gate.clone(),
                workload_lifecycle,
                maintenance_wakeup_tx: None,
                optimizer_query_mem_limit_bytes: self.optimizer_query_mem_limit_bytes,
                attempt_timeout: self.attempt_timeout,
            },
        )?);
        Ok(())
    }
}

impl MvApplicationService for FrontendMvService {
    fn try_handle_statement(
        &self,
        engine: &dyn MvEngine,
        statement: &MvApplicationStatement,
        context: MvRequestContext<'_>,
    ) -> Result<Option<MvStatementResult>, MvApplicationError> {
        match statement {
            MvApplicationStatement::Create(statement) => {
                create::handle_create(engine, statement, context).map(Some)
            }
            MvApplicationStatement::Unhandled => Ok(None),
        }
    }
}

impl FrontendMvService {
    pub fn execute_prepared_refresh(
        &self,
        refresh_plan: PreparedMvRefresh,
        connector_context: ConnectorRequestContext,
        execution: &crate::common::admitted_query_context::QueryExecutionContext,
    ) -> Result<MvStatementResult, MvApplicationError> {
        let dependencies = self.refresh.as_ref().ok_or_else(|| {
            MvApplicationError::new(
                crate::mv::domain::application::MvApplicationErrorKind::Unavailable,
                "frontend MV refresh dependencies are not installed",
            )
        })?;
        refresh::execute(dependencies, refresh_plan, connector_context, execution)
    }

    pub(crate) fn prepare_and_execute_refresh(
        &self,
        preparation: &dyn MvRefreshPreparationService,
        statement: novarocks_sql::planning::mv::MvRefreshStatement,
        target: crate::mv::domain::repository::MvTarget,
        owner: MvActivityOwner,
        connector_context: ConnectorRequestContext,
        execution: &crate::common::admitted_query_context::QueryExecutionContext,
    ) -> Result<MvStatementResult, MvApplicationError> {
        let _gate_lease = self.acquire_activity_lease(&target, owner, execution)?;
        let attempt = self.reserve_refresh_attempt();
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
        target: &crate::mv::domain::repository::MvTarget,
        owner: MvActivityOwner,
        execution: &crate::common::admitted_query_context::QueryExecutionContext,
        action: impl FnOnce() -> Result<T, String>,
    ) -> Result<T, String> {
        let _gate_lease = self
            .acquire_activity_lease(target, owner, execution)
            .map_err(|error| error.to_string())?;
        action()
    }

    fn acquire_activity_lease(
        &self,
        target: &crate::mv::domain::repository::MvTarget,
        owner: MvActivityOwner,
        execution: &crate::common::admitted_query_context::QueryExecutionContext,
    ) -> Result<crate::mv::activity::MvActivityLease, MvApplicationError> {
        let mut gate_ticket = self
            .activity_gate
            .request(CanonicalMvTarget::from_mv_target(target), owner)
            .map_err(|_| {
                MvApplicationError::new(
                    crate::mv::domain::application::MvApplicationErrorKind::ShutdownCancelled,
                    "frontend MV activity admission is closed",
                )
            })?;
        loop {
            if execution.cancellation().is_cancelled() {
                return Err(MvApplicationError::new(
                    crate::mv::domain::application::MvApplicationErrorKind::ShutdownCancelled,
                    "MV statement was cancelled while waiting for activity gate",
                ));
            }
            match gate_ticket.try_acquire() {
                Ok(Some(lease)) => return Ok(lease),
                Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                Err(_) => {
                    return Err(MvApplicationError::new(
                        crate::mv::domain::application::MvApplicationErrorKind::ShutdownCancelled,
                        "frontend MV activity admission is closed",
                    ));
                }
            }
        }
    }

    fn reserve_refresh_attempt(&self) -> MvRefreshAttemptIdentity {
        MvRefreshAttemptIdentity {
            publication_id: novarocks_spi::connector::LakePublicationId::new_v7(),
        }
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

struct FrontendMvBackgroundEngineSink {
    service: Arc<FrontendMvService>,
}

impl MvBackgroundEngineSink for FrontendMvBackgroundEngineSink {
    fn bind_mv_background_engine(
        &self,
        bindings: MvBackgroundBindings,
    ) -> Result<(), MvBackgroundEngineError> {
        self.service.bind_background_engine(bindings)
    }
}

#[derive(Clone)]
struct RefreshWorkerDependencies {
    readiness: Arc<MvReadinessPort>,
    refresh: refresh::FrontendMvRefreshDependencies,
    background_engine: Arc<dyn MvBackgroundEngine>,
    topology: BackendTopologyService,
    role: novarocks_types::ClusterRole,
    scheduler_config: FrontendMvSchedulerConfig,
    maintenance_config: MaintenanceCoordinatorConfig,
    table_maintenance_engine: Arc<dyn TableMaintenanceEngine>,
    table_maintenance_service: Arc<dyn TableMaintenanceService>,
    activity_gate: MvActivityGate,
    workload_lifecycle: FrontendServingLifecycle,
    maintenance_wakeup_tx: Option<mpsc::SyncSender<()>>,
    optimizer_query_mem_limit_bytes: u64,
    attempt_timeout: Duration,
}

struct FrontendMvBackgroundRuntime {
    stop_tx: mpsc::Sender<()>,
    refresh_worker: Option<thread::JoinHandle<()>>,
    maintenance_stop_tx: mpsc::Sender<()>,
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    maintenance_wakeup_tx: mpsc::SyncSender<()>,
    maintenance_worker: Option<thread::JoinHandle<()>>,
}

impl FrontendMvBackgroundRuntime {
    fn start(dependencies: RefreshWorkerDependencies) -> Result<Self, MvBackgroundEngineError> {
        let (stop_tx, stop_rx) = mpsc::channel();
        let (maintenance_stop_tx, maintenance_stop_rx) = mpsc::channel();
        let (maintenance_wakeup_tx, maintenance_wakeup_rx) = mpsc::sync_channel(1);
        let interval = Duration::from_millis(dependencies.scheduler_config.tick_interval_ms.max(1));
        let maintenance_interval =
            Duration::from_millis(dependencies.maintenance_config.tick_interval_ms.max(1));
        let maintenance = Arc::new(FrontendMaintenanceWorker::new(
            FrontendMaintenanceWorkerDependencies {
                readiness: Arc::clone(&dependencies.readiness),
                background_engine: Arc::clone(&dependencies.background_engine),
                table_maintenance_engine: Arc::clone(&dependencies.table_maintenance_engine),
                table_maintenance_service: Arc::clone(&dependencies.table_maintenance_service),
                activity_gate: dependencies.activity_gate.clone(),
                workload_lifecycle: dependencies.workload_lifecycle.clone(),
                coordinator_config: dependencies.maintenance_config.clone(),
                attempt_timeout: dependencies.attempt_timeout,
            },
        ));
        let mut refresh_dependencies = dependencies;
        refresh_dependencies.maintenance_wakeup_tx = Some(maintenance_wakeup_tx.clone());
        let refresh_worker = thread::Builder::new()
            .name("novarocks-frontend-mv-refresh".to_string())
            .spawn(move || run_refresh_worker(refresh_dependencies, stop_rx, interval))
            .map_err(|error| {
                MvBackgroundEngineError::new(
                    MvBackgroundEngineErrorKind::TransientUnavailable,
                    format!("start frontend MV refresh worker: {error}"),
                )
            })?;
        let maintenance_worker = match thread::Builder::new()
            .name("novarocks-frontend-mv-maintenance".to_string())
            .spawn(move || {
                maintenance.run_until_stopped(
                    &maintenance_stop_rx,
                    &maintenance_wakeup_rx,
                    maintenance_interval,
                )
            }) {
            Ok(worker) => worker,
            Err(error) => {
                let _ = stop_tx.send(());
                let _ = refresh_worker.join();
                return Err(MvBackgroundEngineError::new(
                    MvBackgroundEngineErrorKind::TransientUnavailable,
                    format!("start frontend MV maintenance worker: {error}"),
                ));
            }
        };
        Ok(Self {
            stop_tx,
            refresh_worker: Some(refresh_worker),
            maintenance_stop_tx,
            maintenance_wakeup_tx,
            maintenance_worker: Some(maintenance_worker),
        })
    }

    fn stop_and_join(&mut self, deadline: Instant) -> Result<(), String> {
        let _ = self.stop_tx.send(());
        let _ = self.maintenance_stop_tx.send(());
        let Some(worker) = self.refresh_worker.as_ref() else {
            return Ok(());
        };
        while !worker.is_finished() {
            if Instant::now() >= deadline {
                return Err("frontend MV refresh worker did not stop within 5 seconds".to_string());
            }
            thread::sleep(Duration::from_millis(10));
        }
        self.refresh_worker
            .take()
            .expect("finished frontend MV refresh worker is retained")
            .join()
            .map_err(|_| "frontend MV refresh worker panicked during shutdown".to_string())?;
        let Some(worker) = self.maintenance_worker.as_ref() else {
            return Ok(());
        };
        while !worker.is_finished() {
            if Instant::now() >= deadline {
                return Err(
                    "frontend MV maintenance worker did not stop within 5 seconds".to_string(),
                );
            }
            thread::sleep(Duration::from_millis(10));
        }
        self.maintenance_worker
            .take()
            .expect("finished frontend MV maintenance worker is retained")
            .join()
            .map_err(|_| "frontend MV maintenance worker panicked during shutdown".to_string())
    }
}

fn run_refresh_worker(
    dependencies: RefreshWorkerDependencies,
    stop_rx: mpsc::Receiver<()>,
    interval: Duration,
) {
    let mut scheduler = FrontendMvScheduler::new(dependencies.scheduler_config.clone());
    loop {
        let now_ms = now_unix_millis();
        match scheduler.poll(
            dependencies.readiness.as_ref(),
            dependencies.background_engine.as_ref(),
            now_ms,
        ) {
            Ok(requests) => {
                run_scheduled_refreshes(&dependencies, &mut scheduler, requests, &stop_rx);
            }
            Err(error) => tracing::warn!(error = %error, "frontend MV scheduler poll failed"),
        }
        match stop_rx.recv_timeout(interval) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
            Err(RecvTimeoutError::Timeout) => {}
        }
    }
}

fn run_scheduled_refreshes(
    dependencies: &RefreshWorkerDependencies,
    scheduler: &mut FrontendMvScheduler,
    requests: Vec<ScheduledRefreshRequest>,
    stop_rx: &mpsc::Receiver<()>,
) {
    let mut started = Vec::new();
    for request in requests {
        if stop_rx.try_recv().is_ok() {
            scheduler.requeue(request);
            continue;
        }
        let workload_lease = match dependencies
            .workload_lifecycle
            .try_admit(FrontendWorkloadKind::Background)
        {
            Ok(lease) => lease,
            Err(_) => {
                // Draining is terminal for this process runtime. Preserve the
                // coalesced request without creating a new refresh attempt.
                scheduler.requeue(request);
                break;
            }
        };
        let mut ticket = match dependencies.activity_gate.request(
            CanonicalMvTarget::from_mv_target(&request.target),
            MvActivityOwner::ScheduledRefresh,
        ) {
            Ok(ticket) => ticket,
            Err(_) => continue,
        };
        let lease = match ticket.try_acquire() {
            Ok(Some(lease)) => lease,
            Ok(None) => {
                scheduler.requeue(request);
                continue;
            }
            Err(_) => continue,
        };
        if scheduler.mark_started(request.definition.mv_id) {
            started.push((request, lease, workload_lease));
        } else {
            scheduler.requeue(request);
        }
    }
    std::thread::scope(|scope| {
        let (result_tx, result_rx) = mpsc::channel();
        for (request, lease, workload_lease) in started {
            let dependencies = dependencies.clone();
            let result_tx = result_tx.clone();
            scope.spawn(move || {
                let cancellation = workload_lease.cancellation_source().view();
                let disposition = execute_scheduled_refresh(&dependencies, &request, cancellation);
                // The receiver completes the scheduler's terminal transition
                // before it releases these attempt leases.
                let _ = result_tx.send((request, disposition, lease, workload_lease));
            });
        }
        drop(result_tx);
        for (request, disposition, _activity_lease, _workload_lease) in result_rx {
            let completed = matches!(disposition, ScheduledRefreshDisposition::Completed);
            if let Some((disposition_kind, reason)) = scheduler_outcome_log_fields(&disposition) {
                tracing::warn!(
                    mv_id = request.definition.mv_id,
                    target = %request.target.display_name(),
                    disposition_kind,
                    reason = %reason,
                    "frontend MV scheduler refresh did not complete"
                );
            }
            if let Err(error) = scheduler.complete(&request, disposition, now_unix_millis()) {
                tracing::warn!(mv_id = request.definition.mv_id, error = %error, "persist frontend MV scheduler outcome failed");
            } else if completed && let Some(wakeup_tx) = &dependencies.maintenance_wakeup_tx {
                let _ = wakeup_tx.try_send(());
            }
        }
    });
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
    cancellation: crate::common::query_cancellation::QueryCancellationView,
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
        let attempt = reserve_refresh_attempt();
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
    target: &crate::mv::domain::repository::MvTarget,
    cancellation: &crate::common::query_cancellation::QueryCancellationView,
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
        std::thread::sleep(Duration::from_millis(10));
    }
    cancellation.is_cancelled()
}

#[cfg(not(debug_assertions))]
fn scheduled_refresh_test_barrier(
    _target: &crate::mv::domain::repository::MvTarget,
    _cancellation: &crate::common::query_cancellation::QueryCancellationView,
) -> bool {
    false
}

fn reserve_refresh_attempt() -> MvRefreshAttemptIdentity {
    MvRefreshAttemptIdentity {
        publication_id: novarocks_spi::connector::LakePublicationId::new_v7(),
    }
}

fn repository_disposition(
    error: crate::mv::domain::repository::MvRepositoryError,
) -> ScheduledRefreshDisposition {
    use crate::mv::domain::repository::MvRepositoryErrorKind;
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
        MvApplicationErrorKind::Unavailable => {
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
