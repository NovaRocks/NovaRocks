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

use novarocks::mv::application::{
    MvApplicationError, MvApplicationService, MvApplicationStatement, MvEngine, MvRequestContext,
    MvStatementResult,
};
use novarocks::mv::background::{
    MvBackgroundBindings, MvBackgroundEngineError, MvBackgroundEngineErrorKind,
    MvBackgroundEngineSink,
};
use novarocks::mv::repository::MvRepository;
use novarocks::query_execution::backend::BackendTopologyService;
use novarocks::query_execution::cancellation::QueryCancellationSource;
use novarocks::query_execution::request_context::{
    RequestAdmission, RequestContext, SessionOptimizerSettings,
};
use novarocks::query_execution::service::QueryExecutionService;
use novarocks::sql::mv_refresh::PreparedMvRefresh;
use novarocks_spi::connector::{ConnectorControlRegistry, ConnectorRequestContext};

use super::{
    FrontendMvRecoverySummary,
    activity::{CanonicalMvTarget, MvActivityGate, MvActivityOwner},
    create,
    maintenance::MaintenanceCoordinatorConfig,
    maintenance_worker::{FrontendMaintenanceWorker, FrontendMaintenanceWorkerDependencies},
    recovery, refresh,
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
    repository: Arc<dyn MvRepository>,
    refresh: Option<refresh::FrontendMvRefreshDependencies>,
    recovery: Option<recovery::FrontendMvRecoveryDependencies>,
    activity_gate: MvActivityGate,
    background: Mutex<Option<FrontendMvBackgroundRuntime>>,
    scheduler_config: FrontendMvSchedulerConfig,
    maintenance_config: MaintenanceCoordinatorConfig,
    table_maintenance_service:
        Option<Arc<dyn novarocks::engine::table_maintenance::TableMaintenanceService>>,
    execution_role: novarocks::common::app_config::ClusterRole,
    topology: Option<BackendTopologyService>,
}

impl FrontendMvService {
    pub fn new(repository: Arc<dyn MvRepository>) -> Self {
        Self {
            repository,
            refresh: None,
            recovery: None,
            activity_gate: MvActivityGate::new(),
            background: Mutex::new(None),
            scheduler_config: FrontendMvSchedulerConfig::default(),
            maintenance_config: MaintenanceCoordinatorConfig::default(),
            table_maintenance_service: None,
            execution_role: novarocks::common::app_config::ClusterRole::AllInOne,
            topology: None,
        }
    }

    pub(crate) fn with_refresh_dependencies(
        repository: Arc<dyn MvRepository>,
        query_execution: QueryExecutionService,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        first_refresh_activator: Arc<refresh::FrontendMvFirstRefreshWriteActivatorPort>,
        execution_role: novarocks::common::app_config::ClusterRole,
        topology: BackendTopologyService,
        scheduler_config: FrontendMvSchedulerConfig,
        maintenance_config: MaintenanceCoordinatorConfig,
        table_maintenance_service: Arc<
            dyn novarocks::engine::table_maintenance::TableMaintenanceService,
        >,
    ) -> Self {
        Self {
            repository,
            refresh: Some(refresh::FrontendMvRefreshDependencies {
                query_execution,
                connector_control: Arc::clone(&connector_control),
                first_refresh_activator,
            }),
            recovery: Some(recovery::FrontendMvRecoveryDependencies { connector_control }),
            activity_gate: MvActivityGate::new(),
            background: Mutex::new(None),
            scheduler_config,
            maintenance_config,
            table_maintenance_service: Some(table_maintenance_service),
            execution_role,
            topology: Some(topology),
        }
    }

    /// Run one bounded startup recovery pass. Failures are retained as MV
    /// fences inside the repository and do not prevent unrelated SQL from
    /// becoming ready.
    pub fn recover_frontend_mv_refreshes(&self) -> FrontendMvRecoverySummary {
        self.recovery.as_ref().map_or_else(
            || FrontendMvRecoverySummary {
                unresolved: 1,
                ..Default::default()
            },
            |dependencies| recovery::recover_once(self.repository.as_ref(), dependencies),
        )
    }

    pub(crate) fn background_engine_sink(service: Arc<Self>) -> Arc<dyn MvBackgroundEngineSink> {
        Arc::new(FrontendMvBackgroundEngineSink { service })
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
                repository: Arc::clone(&self.repository),
                refresh: dependencies,
                background_engine: bindings.engine,
                topology,
                role: self.execution_role,
                scheduler_config: self.scheduler_config.clone(),
                maintenance_config: self.maintenance_config.clone(),
                table_maintenance_engine: bindings.table_maintenance_engine,
                table_maintenance_service,
                activity_gate: self.activity_gate.clone(),
                maintenance_wakeup_tx: None,
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
                create::handle_create(self.repository.as_ref(), engine, statement, context)
                    .map(Some)
            }
            // REFRESH needs the immutable admitted execution context, which
            // only the typed refresh entrypoint accepts.  Returning `None`
            // here would let a caller silently fall through to the retired
            // core lifecycle.
            MvApplicationStatement::Refresh(_) => Err(MvApplicationError::new(
                novarocks::mv::application::MvApplicationErrorKind::InvalidRequest,
                "REFRESH MATERIALIZED VIEW requires the frontend refresh entrypoint",
            )),
            MvApplicationStatement::Unhandled => Ok(None),
        }
    }

    fn execute_prepared_refresh(
        &self,
        refresh_plan: PreparedMvRefresh,
        connector_context: ConnectorRequestContext,
        execution: &novarocks::query_execution::request_context::QueryExecutionContext,
    ) -> Result<MvStatementResult, MvApplicationError> {
        let dependencies = self.refresh.as_ref().ok_or_else(|| {
            MvApplicationError::new(
                novarocks::mv::application::MvApplicationErrorKind::Unavailable,
                "frontend MV refresh dependencies are not installed",
            )
        })?;
        refresh::execute(
            self.repository.as_ref(),
            dependencies,
            refresh_plan,
            connector_context,
            execution,
        )
    }

    fn prepare_and_execute_refresh(
        &self,
        preparation: &dyn novarocks::sql::mv_refresh::MvRefreshPreparationService,
        statement: MvApplicationStatement,
        target: novarocks::mv::repository::MvTarget,
        connector_context: ConnectorRequestContext,
        execution: &novarocks::query_execution::request_context::QueryExecutionContext,
    ) -> Result<MvStatementResult, MvApplicationError> {
        let MvApplicationStatement::Refresh(statement) = statement else {
            return Err(MvApplicationError::new(
                novarocks::mv::application::MvApplicationErrorKind::InvalidRequest,
                "frontend refresh entrypoint requires REFRESH MATERIALIZED VIEW",
            ));
        };
        let mut gate_ticket = self
            .activity_gate
            .request(
                CanonicalMvTarget::from_mv_target(&target),
                MvActivityOwner::ManualRefresh,
            )
            .map_err(|_| {
                MvApplicationError::new(
                    novarocks::mv::application::MvApplicationErrorKind::ShutdownCancelled,
                    "frontend MV activity admission is closed",
                )
            })?;
        let _gate_lease = loop {
            if execution.cancellation().is_cancelled() {
                return Err(MvApplicationError::new(
                    novarocks::mv::application::MvApplicationErrorKind::ShutdownCancelled,
                    "manual MV refresh was cancelled while waiting for activity gate",
                ));
            }
            match gate_ticket.try_acquire() {
                Ok(Some(lease)) => break lease,
                Ok(None) => std::thread::sleep(Duration::from_millis(10)),
                Err(_) => {
                    return Err(MvApplicationError::new(
                        novarocks::mv::application::MvApplicationErrorKind::ShutdownCancelled,
                        "frontend MV activity admission is closed",
                    ));
                }
            }
        };
        let attempt = self.reserve_refresh_attempt()?;
        let prepared = preparation
            .prepare_step(novarocks::sql::mv_refresh::MvRefreshPreparationRequest {
                statement,
                target,
                attempt: attempt.clone(),
            })
            .map_err(|error| {
                MvApplicationError::new(
                    novarocks::mv::application::MvApplicationErrorKind::InvalidRequest,
                    error,
                )
            })?;
        if prepared.attempt != attempt {
            return Err(MvApplicationError::new(
                novarocks::mv::application::MvApplicationErrorKind::InvalidRequest,
                "MV refresh preparation changed the frontend-reserved attempt identity",
            ));
        }
        self.execute_prepared_refresh(prepared, connector_context, execution)
    }

    fn recover_startup_mv_refreshes(&self) -> Result<(), MvApplicationError> {
        let summary = self.recover_frontend_mv_refreshes();
        tracing::info!(
            candidates = summary.candidates,
            resolved = summary.resolved,
            unresolved = summary.unresolved,
            cleanup_backlog = summary.cleanup_backlog,
            "completed bounded frontend MV startup recovery pass"
        );
        Ok(())
    }
}

impl FrontendMvService {
    fn reserve_refresh_attempt(
        &self,
    ) -> Result<novarocks::sql::mv_refresh::MvRefreshAttemptIdentity, MvApplicationError> {
        let refresh_id = self
            .repository
            .reserve_frontend_refresh_id()
            .map_err(|error| {
                MvApplicationError::new(
                    novarocks::mv::application::MvApplicationErrorKind::Repository,
                    error.to_string(),
                )
            })?;
        let request_id = *uuid::Uuid::now_v7().as_bytes();
        Ok(novarocks::sql::mv_refresh::MvRefreshAttemptIdentity {
            refresh_id,
            request_id,
            staging_branch: format!("__novarocks_mv_refresh_{refresh_id}"),
            marker_token: uuid::Uuid::now_v7().to_string(),
            staging_create_operation_id: *uuid::Uuid::now_v7().as_bytes(),
            write_operation_id: novarocks_spi::connector::ConnectorWriteOperationId::from_bytes(
                *uuid::Uuid::now_v7().as_bytes(),
            ),
            publication_operation_id: *uuid::Uuid::now_v7().as_bytes(),
            staging_drop_operation_id: *uuid::Uuid::now_v7().as_bytes(),
        })
    }
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
    repository: Arc<dyn MvRepository>,
    refresh: refresh::FrontendMvRefreshDependencies,
    background_engine: Arc<dyn novarocks::mv::background::MvBackgroundEngine>,
    topology: BackendTopologyService,
    role: novarocks::common::app_config::ClusterRole,
    scheduler_config: FrontendMvSchedulerConfig,
    maintenance_config: MaintenanceCoordinatorConfig,
    table_maintenance_engine: Arc<dyn novarocks::engine::table_maintenance::TableMaintenanceEngine>,
    table_maintenance_service:
        Arc<dyn novarocks::engine::table_maintenance::TableMaintenanceService>,
    activity_gate: MvActivityGate,
    maintenance_wakeup_tx: Option<mpsc::SyncSender<()>>,
}

struct FrontendMvBackgroundRuntime {
    stop_tx: mpsc::Sender<()>,
    refresh_worker: Option<thread::JoinHandle<()>>,
    maintenance_stop_tx: mpsc::Sender<()>,
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
                repository: Arc::clone(&dependencies.repository),
                background_engine: Arc::clone(&dependencies.background_engine),
                table_maintenance_engine: Arc::clone(&dependencies.table_maintenance_engine),
                table_maintenance_service: Arc::clone(&dependencies.table_maintenance_service),
                activity_gate: dependencies.activity_gate.clone(),
                coordinator_config: dependencies.maintenance_config.clone(),
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
            dependencies.repository.as_ref(),
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
            started.push((request, lease));
        } else {
            scheduler.requeue(request);
        }
    }
    std::thread::scope(|scope| {
        let (result_tx, result_rx) = mpsc::channel();
        for (request, lease) in started {
            let dependencies = dependencies.clone();
            let result_tx = result_tx.clone();
            scope.spawn(move || {
                let disposition =
                    execute_scheduled_refresh(&dependencies, &request, lease.cancellation());
                let _ = result_tx.send((request, disposition));
            });
        }
        drop(result_tx);
        for (request, disposition) in result_rx {
            let completed = matches!(disposition, ScheduledRefreshDisposition::Completed);
            if let Err(error) = scheduler.complete(
                dependencies.repository.as_ref(),
                &request,
                disposition,
                now_unix_millis(),
            ) {
                tracing::warn!(mv_id = request.definition.mv_id, error = %error, "persist frontend MV scheduler outcome failed");
            } else if completed {
                if let Some(wakeup_tx) = &dependencies.maintenance_wakeup_tx {
                    let _ = wakeup_tx.try_send(());
                }
            }
        }
    });
}

fn execute_scheduled_refresh(
    dependencies: &RefreshWorkerDependencies,
    request: &ScheduledRefreshRequest,
    cancellation: Option<novarocks::query_execution::cancellation::QueryCancellationView>,
) -> ScheduledRefreshDisposition {
    let cancellation = cancellation.unwrap_or_else(|| QueryCancellationSource::new().view());
    if scheduled_refresh_test_barrier(&request.target, &cancellation) {
        return ScheduledRefreshDisposition::ShutdownCancelled;
    }
    let topology = match dependencies.topology.snapshot() {
        Ok(snapshot) => snapshot,
        Err(error) => return ScheduledRefreshDisposition::TransientUnavailable(error.to_string()),
    };
    let deadline = match Instant::now().checked_add(MV_WORKER_ATTEMPT_TIMEOUT) {
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
        SessionOptimizerSettings::default(),
    ));
    let connector_context = match novarocks::connector::connector_request_context_for_execution(
        None,
        context.execution(),
    ) {
        Ok(context) => context,
        Err(error) => return ScheduledRefreshDisposition::TransientUnavailable(error),
    };
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
        let attempt = match reserve_refresh_attempt(dependencies.repository.as_ref()) {
            Ok(attempt) => attempt,
            Err(error) => return repository_disposition(error),
        };
        let prepared = match dependencies.background_engine.prepare_refresh_step(
            &step,
            attempt,
            &connector_context,
        ) {
            Ok(prepared) => prepared,
            Err(error) => return ScheduledRefreshDisposition::from_background_error(error),
        };
        let no_op = matches!(
            prepared.work,
            novarocks::sql::mv_refresh::PreparedMvRefreshWork::NoOp
        );
        if let Err(error) = refresh::execute(
            dependencies.repository.as_ref(),
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
    target: &novarocks::mv::repository::MvTarget,
    cancellation: &novarocks::query_execution::cancellation::QueryCancellationView,
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
    _target: &novarocks::mv::repository::MvTarget,
    _cancellation: &novarocks::query_execution::cancellation::QueryCancellationView,
) -> bool {
    false
}

fn reserve_refresh_attempt(
    repository: &dyn MvRepository,
) -> Result<
    novarocks::sql::mv_refresh::MvRefreshAttemptIdentity,
    novarocks::mv::repository::MvRepositoryError,
> {
    let refresh_id = repository.reserve_frontend_refresh_id()?;
    Ok(novarocks::sql::mv_refresh::MvRefreshAttemptIdentity {
        refresh_id,
        request_id: *uuid::Uuid::now_v7().as_bytes(),
        staging_branch: format!("__novarocks_mv_refresh_{refresh_id}"),
        marker_token: uuid::Uuid::now_v7().to_string(),
        staging_create_operation_id: *uuid::Uuid::now_v7().as_bytes(),
        write_operation_id: novarocks_spi::connector::ConnectorWriteOperationId::from_bytes(
            *uuid::Uuid::now_v7().as_bytes(),
        ),
        publication_operation_id: *uuid::Uuid::now_v7().as_bytes(),
        staging_drop_operation_id: *uuid::Uuid::now_v7().as_bytes(),
    })
}

fn repository_disposition(
    error: novarocks::mv::repository::MvRepositoryError,
) -> ScheduledRefreshDisposition {
    use novarocks::mv::repository::MvRepositoryErrorKind;
    match error.kind() {
        MvRepositoryErrorKind::Conflict => ScheduledRefreshDisposition::AlreadyActive,
        MvRepositoryErrorKind::NotFound => ScheduledRefreshDisposition::TargetGone,
        MvRepositoryErrorKind::Unavailable => {
            ScheduledRefreshDisposition::TransientUnavailable(error.to_string())
        }
        MvRepositoryErrorKind::Corruption => {
            ScheduledRefreshDisposition::Corruption(error.to_string())
        }
        MvRepositoryErrorKind::CommitUnknown
        | MvRepositoryErrorKind::KnownCommittedFinalizeFailed => {
            ScheduledRefreshDisposition::RecoveryRequired(error.to_string())
        }
        MvRepositoryErrorKind::InvalidRequest => {
            ScheduledRefreshDisposition::InvariantViolation(error.to_string())
        }
    }
}

fn application_disposition(error: MvApplicationError) -> ScheduledRefreshDisposition {
    use novarocks::mv::application::MvApplicationErrorKind;
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
        MvApplicationErrorKind::RecoveryRequired
        | MvApplicationErrorKind::CommitUnknown
        | MvApplicationErrorKind::KnownCommittedFinalizeFailed => {
            ScheduledRefreshDisposition::RecoveryRequired(error.message().to_owned())
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
