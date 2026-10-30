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

//! Frontend-owned native execution adapter for current-process ANALYZE attempts.
//!
//! Core owns the provider-neutral statistics program and its one-shot
//! prepare/finish boundary.  This adapter owns the only native mapping step:
//! it encodes the sealed prepared view before returning the resulting request
//! to the carrier-neutral query-execution service.

use arrow::datatypes::FieldRef;
use std::sync::Arc;
use std::time::{Duration, Instant};

use novarocks_statistics_application::{
    StatisticsAttemptError as CoreStatisticsAttemptError,
    StatisticsAttemptExecutor as CoreStatisticsAttemptExecutor, StatisticsColumns,
    StatisticsFailure, StatisticsJob, StatisticsPublicationFact, StatisticsPublicationOutcome,
};

use crate::statistics_jobs::application::{
    StatisticsApplicationError, StatisticsAttemptRequest, StatisticsColumnIntent,
};
use novarocks_query_application::api::BackendTopologyService;
use novarocks_spi::connector::{
    ConnectorControlRegistry, ConnectorMutationOperationId, ConnectorRequestContext,
    ExternalMutationFinalization, ExternalMutationOutcome, StatisticsCollectionStartRequest,
    StatisticsColumnSelection,
};

/// Exact Frontend composition leaves retained by the process-owned ANALYZE worker.
/// Each collection takes a fresh live topology snapshot. The worker persists
/// only a logical target and physical object ID; a current binding, its data
/// version, and schema columns are attempt-local facts.
#[derive(Clone)]
pub struct StatisticsAttemptExecutorPorts {
    constant_policy: novarocks_functions::ConstantPolicy,
    static_plan_carrier: crate::query_execution::package_freeze::CompiledPackageCarrier,
    execution_role: novarocks_types::ClusterRole,
    connector_control: Arc<dyn ConnectorControlRegistry>,
    /// The composition root's single typed control registry. A collection is
    /// an ordinary typed read, so it resolves its relation through the same
    /// installed generation every statement does.
    #[expect(
        dead_code,
        reason = "Retains the original process Connector control host through ANALYZE execution and teardown."
    )]
    typed_connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
    backend_topology: BackendTopologyService,
    client: QueryExecutionClient,
    launcher: Arc<dyn LogicalExecutionLauncher>,
    completion: CompletionFactOwners,
    execution_runtime: (
        crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
        crate::native::data_runtime::FrontendDataRuntime,
    ),
    function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
    attempt_timeout: Duration,
    runtime: tokio::runtime::Handle,
}

impl StatisticsAttemptExecutorPorts {
    #[expect(
        clippy::too_many_arguments,
        reason = "Explicit role-owned ANALYZE capabilities at composition."
    )]
    pub(crate) fn new(
        execution_role: novarocks_types::ClusterRole,
        connector_control: Arc<dyn ConnectorControlRegistry>,
        typed_connector_control: Arc<novarocks_catalog_application::ConnectorControlHost>,
        backend_topology: BackendTopologyService,
        client: QueryExecutionClient,
        launcher: Arc<dyn LogicalExecutionLauncher>,
        completion: CompletionFactOwners,
        execution_runtime: (
            crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
            crate::native::data_runtime::FrontendDataRuntime,
        ),
        function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
        runtime_policy: novarocks_query_application::publication::LakePublicationRuntimePolicy,
        runtime: tokio::runtime::Handle,
        constant_policy: novarocks_functions::ConstantPolicy,
        static_plan_carrier: crate::query_execution::package_freeze::CompiledPackageCarrier,
    ) -> Self {
        Self {
            constant_policy,
            static_plan_carrier,
            execution_role,
            connector_control,
            typed_connector_control,
            backend_topology,
            client,
            launcher,
            completion,
            execution_runtime,
            function_catalog,
            attempt_timeout: runtime_policy.max_attempt_duration(),
            runtime,
        }
    }
}

fn resolve_columns(
    request: &StatisticsAttemptRequest,
    bound_columns: &[FieldRef],
) -> Result<Vec<FieldRef>, StatisticsApplicationError> {
    match &request.columns {
        StatisticsColumnIntent::AllColumns => Ok(bound_columns.to_vec()),
        StatisticsColumnIntent::Explicit(requested_columns) => {
            let mut resolved = Vec::with_capacity(requested_columns.len());
            for requested in requested_columns {
                let mut matches = bound_columns
                    .iter()
                    .filter(|bound| bound.name().eq_ignore_ascii_case(requested));
                let Some(column) = matches.next() else {
                    return Err(StatisticsApplicationError::new(format!(
                        "ANALYZE requested column '{requested}' does not exist on the rebound table"
                    )));
                };
                if matches.next().is_some() {
                    return Err(StatisticsApplicationError::new(format!(
                        "ANALYZE requested column '{requested}' matches multiple rebound table columns"
                    )));
                }
                if resolved
                    .iter()
                    .any(|existing: &FieldRef| existing.name().eq_ignore_ascii_case(column.name()))
                {
                    return Err(StatisticsApplicationError::new(format!(
                        "ANALYZE requested column '{requested}' is duplicated"
                    )));
                }
                resolved.push(column.clone());
            }
            Ok(resolved)
        }
    }
}

fn operation_id(request: &StatisticsAttemptRequest) -> ConnectorMutationOperationId {
    ConnectorMutationOperationId::from_bytes(request.operation_id.to_bytes())
}

use crate::query::statement_computation::{StatementComputation, StatementNativeTail};
use crate::query_execution::{
    completion_facts::{CompletionFactOwners, FrontendCompletionCustody},
    internal_consumer::decoded::ReadyStatisticsComputation,
    logical_execution::{
        InitialComputationCredentials, InitialComputationReservation, LogicalExecutionLauncher,
    },
    maintenance::io_admission::MetadataIoAdmission,
    statistics::preparation::{
        PreparedStatisticsComputationAssembly, StatisticsComputationPlanningServices,
        StatisticsPreparationCells, StatisticsPreparationFailure, StatisticsPreparationJob,
        StatisticsPreparationOutcome, prepare_statistics_computation_in,
    },
};
use novarocks_query_application::{
    admitted_query_context::QueryExecutionContext,
    api::{QueryExecutionClient, StatementNativeHealthFailure},
    cancellation::QueryCancellationView,
    session_control::{GovernedActionContext, GovernedBackgroundAction, GovernedStatementResidual},
    session_error::{QueryServiceError, QueryServiceErrorKind},
};
use novarocks_spi::connector::{
    AsyncStatisticsCollectionSession, ConnectorControlPlanningLease, ConnectorStatisticsLease,
    LakePublicationDisposition, StatisticsArtifactDraft, StatisticsCpuPort,
    StatisticsPublicationClose, StatisticsPublicationReady,
};
use novarocks_statistics_application::{StatisticsJobAdmission, StatisticsJobProgress};
use novarocks_workload_control::{WorkError, WorkScope};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

type PhaseFuture = Pin<Box<dyn Future<Output = Result<(), CoreStatisticsAttemptError>> + Send>>;
type CloseFuture = Pin<Box<dyn Future<Output = Result<(), StatementNativeHealthFailure>> + Send>>;
#[derive(Debug, Default)]
struct StatisticsWake(futures::task::AtomicWaker);
impl crate::task_execution::status_intake::StatusIntakeWake for StatisticsWake {
    fn wake(&self) {
        self.0.wake();
    }
}
#[derive(Default)]
struct SessionSlot {
    session: Option<Box<dyn AsyncStatisticsCollectionSession>>,
    program: Option<crate::query_execution::statistics::StatisticsCollectionProgram>,
    artifacts: Option<Vec<StatisticsArtifactDraft>>,
    // This is the actual decoder's statement retention, not a P1 alias.
    ready: Option<ReadyStatisticsComputation>,
    publication: Option<StatisticsPublicationReady>,
    close: Option<StatisticsPublicationClose>,
    close_failure: Option<novarocks_spi::connector::ConnectorError>,
    binding_failure: Option<StatisticsApplicationError>,
    provider_failure: Option<novarocks_spi::connector::ConnectorError>,
}
#[derive(Default)]
struct PlanSlot {
    custody: crate::query_execution::statistics::preparation::StatisticsPreparationCustody,
    job: Option<StatisticsPreparationJob>,
    outcome: Option<StatisticsPreparationOutcome>,
    failure: Option<StatisticsPreparationFailure>,
}

pub(crate) struct FrontendStatisticsAttemptExecutor {
    ports: StatisticsAttemptExecutorPorts,
}
impl FrontendStatisticsAttemptExecutor {
    pub(crate) fn new(ports: StatisticsAttemptExecutorPorts) -> Self {
        Self { ports }
    }
    async fn execute_original(
        &self,
        job: StatisticsJob,
        admission: StatisticsJobAdmission,
        progress: StatisticsJobProgress,
    ) -> Result<(), CoreStatisticsAttemptError> {
        let mut originals = admission.into_originals();
        let result = self
            .mount_original(&job, &mut originals, progress.clone())
            .await;
        if originals.owner.is_some() {
            // No Provider request is created in startup. Only the original
            // finite Root proposal is polled before the shared bank owns it.
            progress
                .retire_unmounted(originals, now_ms())
                .map_err(|e| Self::failure(e.to_string()))?;
        }
        result
    }
    async fn mount_original(
        &self,
        job: &StatisticsJob,
        originals: &mut novarocks_statistics_application::StatisticsJobOriginals,
        progress: StatisticsJobProgress,
    ) -> Result<(), CoreStatisticsAttemptError> {
        let scope = originals
            .owner
            .as_ref()
            .ok_or_else(|| Self::failure("statistics Root is absent"))?
            .scope();
        let (deadline, cancellation) = self.context(&scope)?;
        let instance = novarocks_spi::connector::ConnectorInstanceId::parse(&job.target.catalog)
            .map_err(|e| Self::failure(e.to_string()))?;
        let planning_lease = self
            .ports
            .connector_control
            .acquire_current(&instance)
            .map_err(|e| Self::failure(e.to_string()))?;
        let lease = planning_lease
            .derive_statistics_lease()
            .map_err(|e| Self::failure(e.to_string()))?;
        let profile = lease
            .collection_io_profile()
            .map_err(|e| Self::failure(e.to_string()))?;
        // The original action header remains a responsibility fact. Provider
        // profile/SDK layout declarations do not authorize actual Result bytes.
        let metadata = std::mem::size_of::<StatisticsAction>() as u64;
        let io = MetadataIoAdmission::new(scope.clone(), profile, metadata)
            .map_err(Self::scope_error)?;
        io.adopt_original_payload_in(&mut originals.root_payload)
            .map_err(Self::scope_error)?;
        // The former repository view was only a retention alias of this same
        // payload. Drop it after the original MIO field has accepted custody.
        drop(originals.result_capacity.take());
        std::future::poll_fn(
            |cx| match io.poll_progress(originals.owner.as_ref().unwrap(), cx) {
                Ok(true) => Poll::Ready(Ok(())),
                Ok(false) => Poll::Pending,
                Err(e) => Poll::Ready(Err(Self::scope_error(e))),
            },
        )
        .await?;
        let capacity = io.root_capacity().map_err(Self::scope_error)?;
        let topology = self
            .ports
            .backend_topology
            .snapshot()
            .map_err(|e| Self::failure(e.to_string()))?;
        let mut execution = QueryExecutionContext::new(
            self.ports.execution_role,
            topology,
            Some(deadline),
            cancellation.clone(),
            novarocks_sql::compiler::SessionOptimizerSettings::default(),
            novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        )
        .with_result_capacity(capacity.clone())
        .map_err(Self::scope_error)?;
        #[cfg(novarocks_dev_controls)]
        let observation = originals
            .test_statement_nomination
            .as_deref()
            .and_then(|nomination| {
                self.ports
                    .launcher
                    .resolve_statement_observation(nomination)
            });
        #[cfg(novarocks_dev_controls)]
        if let Some(nomination) = originals.test_statement_nomination.take() {
            execution = execution
                .with_test_statement_nomination(nomination)
                .map_err(Self::failure)?;
        }
        let context = self
            .collection_context(deadline, cancellation.clone())
            .map_err(Self::application_error)?
            .with_io_admission(
                Arc::clone(&io) as Arc<dyn novarocks_spi::connector::ConnectorIoAdmissionPort>
            );
        let wake = Arc::new(StatisticsWake::default());
        let cpu = self
            .ports
            .execution_runtime
            .0
            .provider_creator_cpu()
            .map_err(Self::scope_error)?;
        let cpu = cpu
            .statistics_cpu_port(
                &scope,
                capacity.window_alias(),
                &lease,
                cancellation,
                self.ports.runtime.clone(),
                wake.clone(),
            )
            .map_err(Self::scope_error)?;
        let initial = InitialComputationReservation::new(
            self.ports
                .launcher
                .reserve_identity()
                .map_err(|e| Self::failure(e.to_string()))?,
        )
        .map_err(|e| Self::failure(e.to_string()))?;
        let credentials = initial.credentials();
        let context = initial
            .connector_request_context(
                context,
                #[cfg(novarocks_dev_controls)]
                execution.test_statement_nomination(),
            )
            .map_err(|e| Self::failure(e.to_string()))?;
        let request = Self::request(job)?;
        let mut action: Option<Box<dyn GovernedBackgroundAction>> =
            Some(Box::new(StatisticsAction {
                state: Some(StatisticsState {
                    ports: self.ports.clone(),
                    root: scope.clone(),
                    execution: execution.clone(),
                    context,
                    credentials,
                    progress: progress.clone(),
                    relation:
                        crate::query_execution::statistics::StatisticsRelationIdentity::try_new(
                            job.target.catalog.as_ref(),
                            job.target.namespace.as_ref(),
                            job.target.table.as_ref(),
                        )
                        .map_err(|e| Self::failure(e.to_string()))?,
                    request: Some(request),
                    planning_lease: Some(planning_lease),
                    lease: Some(lease),
                    cpu,
                    io,
                    wake,
                    session: Arc::new(tokio::sync::Mutex::new(SessionSlot::default())),
                    plan: Arc::new(tokio::sync::Mutex::new(PlanSlot::default())),
                    provider: FrontendCompletionCustody::new(),
                    phase: ActionPhase::Begin,
                    pending: None,
                    close: None,
                    close_done: false,
                    cleanup_observed: false,
                    computation: StatementComputation::new(
                        scope,
                        initial,
                        self.ports.launcher.clone(),
                        #[cfg(novarocks_dev_controls)]
                        observation,
                    ),
                    native_tail: None,
                    selected: false,
                    disposition: None,
                    failure: None,
                    health: None,
                    io_close_failure: None,
                }),
            }));
        let mut input = None;
        let mut ready = None;
        let mut exit = None;
        let mut execution = Some(execution);
        self.ports
            .client
            .prepare_background_action_in(
                &originals.owner,
                &mut execution,
                &mut originals.query_concurrency,
                &mut originals.root_payload,
                &mut action,
                &mut input,
            )
            .map_err(|e| Self::failure(e.to_string()))?;
        let tail = self
            .ports
            .client
            .start_background_action_in(&mut input, &mut originals.owner, &mut ready, &mut exit)
            .map_err(|e| Self::failure(e.to_string()))?;
        // The repository's original record receives the observer before any
        // wait can disappear. This transfers observation, not physical custody.
        progress
            .install_exit_in(&mut exit)
            .map_err(|e| Self::failure(e.to_string()))?;
        tail.finish().map_err(|e| Self::failure(e.to_string()))?;
        let receipt = ready
            .as_mut()
            .ok_or_else(|| Self::failure("background start returned no Ready source"))?
            .await
            .map_err(|original| {
                // The already-installed exit observer retains the original
                // source and opaque health cause. Project only safe detail.
                Self::failure(
                    original
                        .health()
                        .map_or("statistics background action panicked", |health| {
                            health.detail()
                        }),
                )
            })?;
        let result = {
            let original = receipt.view();
            match original.result() {
                novarocks_query_application::session_control::BackgroundActionResultView::Succeeded => Ok(()),
                novarocks_query_application::session_control::BackgroundActionResultView::Failed(error) => Err(Self::failure(error.to_string())),
            }
        };
        receipt.finish();
        result
    }
}
impl CoreStatisticsAttemptExecutor for FrontendStatisticsAttemptExecutor {
    fn execute<'a>(
        &'a self,
        job: StatisticsJob,
        admission: StatisticsJobAdmission,
        progress: StatisticsJobProgress,
    ) -> Pin<Box<dyn Future<Output = Result<(), CoreStatisticsAttemptError>> + Send + 'a>> {
        Box::pin(execute_statistics_original(
            self.ports.clone(),
            job,
            admission,
            progress,
        ))
    }
}

// A concrete owned factory lets the original per-Root process-source quote
// measure the actual Starting F before any job or MIO allocation exists.
async fn execute_statistics_original(
    ports: StatisticsAttemptExecutorPorts,
    job: StatisticsJob,
    admission: StatisticsJobAdmission,
    progress: StatisticsJobProgress,
) -> Result<(), CoreStatisticsAttemptError> {
    FrontendStatisticsAttemptExecutor { ports }
        .execute_original(job, admission, progress)
        .await
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ActionPhase {
    Begin,
    AdmitCollection,
    PreparePlan,
    Collect,
    Publish,
    Finished,
}
struct StatisticsState {
    ports: StatisticsAttemptExecutorPorts,
    root: WorkScope,
    execution: QueryExecutionContext,
    context: ConnectorRequestContext,
    // The Native factory borrows only a Weak. This same Arc crosses Action ->
    // residual and remains live until the original Native tail actually exits.
    credentials: Arc<InitialComputationCredentials>,
    progress: StatisticsJobProgress,
    relation: crate::query_execution::statistics::StatisticsRelationIdentity,
    request: Option<StatisticsAttemptRequest>,
    planning_lease: Option<ConnectorControlPlanningLease>,
    lease: Option<ConnectorStatisticsLease>,
    cpu: Arc<dyn StatisticsCpuPort>,
    io: Arc<MetadataIoAdmission>,
    wake: Arc<StatisticsWake>,
    session: Arc<tokio::sync::Mutex<SessionSlot>>,
    plan: Arc<tokio::sync::Mutex<PlanSlot>>,
    provider: Arc<FrontendCompletionCustody>,
    phase: ActionPhase,
    pending: Option<PhaseFuture>,
    close: Option<CloseFuture>,
    close_done: bool,
    cleanup_observed: bool,
    computation: StatementComputation<PreparedStatisticsComputationAssembly>,
    native_tail: Option<StatementNativeTail>,
    selected: bool,
    disposition: Option<LakePublicationDisposition>,
    failure: Option<CoreStatisticsAttemptError>,
    health: Option<StatementNativeHealthFailure>,
    // Keep an exited ordinary I/O refusal without changing the selected job.
    io_close_failure: Option<WorkError>,
}
struct StatisticsAction {
    state: Option<StatisticsState>,
}
struct StatisticsResidual {
    state: StatisticsState,
}
fn service_error(error: impl ToString) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, error.to_string())
}
fn failure(error: impl ToString) -> CoreStatisticsAttemptError {
    FrontendStatisticsAttemptExecutor::failure(error.to_string())
}
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}
impl Drop for StatisticsState {
    fn drop(&mut self) {
        // A refused start or notification unwind still closes the same
        // collector before releasing this final stable domain custody.
        let _ = self.credentials.seal_for_terminal();
    }
}
impl StatisticsState {
    fn seal_credentials(&mut self) {
        if let Err(error) = self.credentials.seal_for_terminal() {
            // Keep this original ordinary failure without rewriting an already
            // selected publication or pretending that actual exit failed.
            self.failure.get_or_insert_with(|| failure(error));
        }
    }
    fn retain_health(&mut self, error: StatementNativeHealthFailure) -> QueryServiceError {
        let projection = service_error(error.detail());
        self.health.get_or_insert(error);
        projection
    }
    fn poll_business(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        self.wake.0.register(cx.waker());
        if let Err(error) = self.root.check() {
            self.failure
                .get_or_insert_with(|| FrontendStatisticsAttemptExecutor::scope_error(error));
        }
        if let Some(original) = owner.execution_owner_slot().as_ref()
            && let Err(error) = self.io.poll_progress(original, cx)
        {
            self.failure
                .get_or_insert_with(|| FrontendStatisticsAttemptExecutor::scope_error(error));
        }
        if let Some(future) = self.pending.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => {
                    self.failure.get_or_insert(e);
                }
                Poll::Ready(Ok(())) => {}
            }
            drop(self.pending.take());
            match self.phase {
                ActionPhase::Begin => self.phase = ActionPhase::AdmitCollection,
                ActionPhase::PreparePlan => self.phase = ActionPhase::Collect,
                ActionPhase::Publish => self.phase = ActionPhase::Finished,
                _ => {}
            }
        }
        let has_publication = self.phase == ActionPhase::Finished
            && self
                .session
                .try_lock()
                .map_err(service_error)?
                .publication
                .is_some();
        if !has_publication && let Some(error) = self.failure.take() {
            self.progress
                .finish(Err(error), now_ms())
                .map_err(service_error)?;
            self.selected = true;
            self.phase = ActionPhase::Finished;
            return Poll::Ready(Ok(()));
        }
        match self.phase {
            ActionPhase::Begin => {
                self.pending = Some(Box::pin(begin_session(
                    self.session.clone(),
                    self.request.take().unwrap(),
                    self.planning_lease.as_ref().unwrap().clone(),
                    self.lease.take().unwrap(),
                    self.context.clone(),
                    self.cpu.clone(),
                )));
            }
            ActionPhase::AdmitCollection => {
                let empty = {
                    self.session
                        .try_lock()
                        .map_err(service_error)?
                        .program
                        .is_none()
                };
                if empty {
                    self.progress.prepared(now_ms()).map_err(service_error)?;
                    self.progress.collected(now_ms()).map_err(service_error)?;
                    self.phase = ActionPhase::Publish;
                } else {
                    let work = owner
                        .execution_owner_slot()
                        .as_ref()
                        .ok_or_else(|| service_error("missing statistics Root"))?;
                    std::task::ready!(self.computation.poll_admit(&self.io, work, cx))?;
                    let capacity = self.computation.capacity().unwrap().clone();
                    let scope = capacity.scope().clone();
                    // The actual collection Stage, not its retired preparation parent.
                    let cancellation = QueryCancellationView::governed(
                        scope.cancellation().map_err(service_error)?,
                        None,
                    );
                    let mut execution = QueryExecutionContext::new(
                        self.execution.role(),
                        self.execution.topology().clone(),
                        self.execution.deadline(),
                        cancellation.clone(),
                        self.execution.optimizer_settings().clone(),
                        self.execution.sql_semantics().clone(),
                    )
                    .with_result_capacity(capacity)
                    .map_err(service_error)?;
                    #[cfg(novarocks_dev_controls)]
                    if let Some(nomination) = self.execution.test_statement_nomination() {
                        execution = execution
                            .with_test_statement_nomination(nomination.clone())
                            .map_err(service_error)?;
                    }
                    let context = crate::connector::query_connector_request_context_on_runtime(
                        &self.ports.runtime,
                        self.context.deadline(),
                        cancellation,
                    )
                    .map_err(service_error)?
                    .with_io_admission(self.io.clone());
                    let program = self
                        .session
                        .try_lock()
                        .map_err(service_error)?
                        .program
                        .take()
                        .unwrap();
                    let identity = self.relation.clone();
                    let context = self
                        .computation
                        .preparation_connector_context(
                            context,
                            #[cfg(novarocks_dev_controls)]
                            execution.test_statement_nomination(),
                        )
                        .map_err(service_error)?;
                    self.pending = Some(Box::pin(prepare_plan(
                        self.plan.clone(),
                        self.provider.clone(),
                        self.ports.clone(),
                        execution,
                        context,
                        identity,
                        program,
                        self.planning_lease.take().unwrap(),
                        scope,
                        self.wake.clone(),
                    )));
                    self.phase = ActionPhase::PreparePlan;
                }
            }
            ActionPhase::Collect => {
                let outcome = self.plan.try_lock().map_err(service_error)?.outcome.take();
                if let Some(outcome) = outcome {
                    let assembly = match outcome {
                        Ok(assembly) => assembly,
                        Err(original) => {
                            let projected = match &original {
                                StatisticsPreparationFailure::Domain(error) => {
                                    CoreStatisticsAttemptError::Failed(StatisticsFailure {
                                        compile_control: error.compile_control_error(),
                                        message: Arc::from(error.to_string()),
                                    })
                                }
                                StatisticsPreparationFailure::Cpu(error) => {
                                    failure(error.user_error())
                                }
                            };
                            self.plan.try_lock().map_err(service_error)?.failure = Some(original);
                            return self.fail(projected, cx);
                        }
                    };
                    self.computation.install_assembly(assembly)?;
                    self.progress.prepared(now_ms()).map_err(service_error)?;
                }
                std::task::ready!(self.computation.poll_ready(
                    &self.ports.execution_runtime,
                    &self.io,
                    owner,
                    cx
                ))?;
                std::task::ready!(self.computation.poll_ready_local_exit(cx))
                    .map_err(|error| self.retain_health(error))?;
                let ready = self
                    .computation
                    .take_ready()
                    .ok_or_else(|| service_error("statistics has no exact Ready"))?;
                {
                    let mut session = self.session.try_lock().map_err(service_error)?;
                    let mut ready = ready;
                    session.artifacts = Some(std::mem::take(&mut ready.artifacts));
                    session.ready = Some(ready);
                }
                self.computation
                    .release_result_aliases()
                    .map_err(|error| self.retain_health(error))?;
                self.native_tail = Some(
                    self.computation
                        .take_native_tail()
                        .map_err(|error| self.retain_health(error))?,
                );
                self.progress.collected(now_ms()).map_err(service_error)?;
                self.phase = ActionPhase::Publish;
            }
            ActionPhase::Publish => {
                self.pending = Some(Box::pin(publish_session(self.session.clone())));
            }
            ActionPhase::Finished => {
                if !self.selected {
                    let outcome = {
                        let original = self.session.try_lock().map_err(service_error)?;
                        let ready = original
                            .publication
                            .as_ref()
                            .ok_or_else(|| service_error("statistics NeededReady is absent"))?;
                        FrontendStatisticsAttemptExecutor::publication_ready_outcome(ready)
                    };
                    self.disposition = match outcome.fact() {
                        StatisticsPublicationFact::KnownCommitted => {
                            Some(LakePublicationDisposition::KnownCommitted)
                        }
                        StatisticsPublicationFact::KnownUncommitted => {
                            Some(LakePublicationDisposition::KnownUncommitted)
                        }
                        StatisticsPublicationFact::CommitUnknown => {
                            Some(LakePublicationDisposition::CommitUnknown)
                        }
                        _ => None,
                    };
                    self.progress
                        .finish(Ok(outcome), now_ms())
                        .map_err(service_error)?;
                    self.selected = true;
                }
                return Poll::Ready(Ok(()));
            }
            ActionPhase::PreparePlan => unreachable!("preparation retains its original future"),
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
    fn fail(
        &mut self,
        error: CoreStatisticsAttemptError,
        cx: &Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        self.failure = Some(error);
        cx.waker().wake_by_ref();
        Poll::Pending
    }
    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.wake.0.register(cx.waker());
        if let Some(health) = &self.health {
            return Poll::Ready(Err(health.clone()));
        }
        self.io
            .poll_continuing(cx)
            .map_err(|e| health("statistics original I/O failed", Arc::new(e)))?;
        if let Some(future) = self.pending.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    self.failure.get_or_insert(error);
                }
                Poll::Ready(Ok(())) => {}
            }
            drop(self.pending.take());
        }
        if self.close.is_none() && !self.close_done {
            self.seal_credentials();
            self.close = Some(Box::pin(close_session(
                self.session.clone(),
                self.plan.clone(),
                self.provider.clone(),
                self.context.clone(),
                self.cpu.clone(),
            )));
        }
        if !self.close_done {
            match self.close.as_mut().unwrap().as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    self.health = Some(error.clone());
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Ok(())) => {
                    self.close_done = true;
                    drop(self.close.take());
                }
            }
        }
        if let Some(tail) = self.native_tail.as_mut() {
            std::task::ready!(tail.poll_exit(cx))?;
        } else {
            std::task::ready!(self.computation.poll_actual_exit(cx))?;
        }
        let observed = std::task::ready!(self.io.poll_actual_quiescent_continuing(cx))?;
        if let Some(failure) = observed.into_ordinary_failure() {
            // Business was already selected. Its retained admission diagnostic
            // is independent of the positive physical-exit observation.
            self.io_close_failure.get_or_insert(failure);
        }
        let Ok(original) = self.session.try_lock() else {
            return Poll::Pending;
        };
        if !self.cleanup_observed {
            if let Some(StatisticsPublicationClose::Observed { outcome, .. }) = &original.close {
                let cleanup = match outcome {
                    ExternalMutationOutcome::KnownCommitted { cleanup, .. }
                    | ExternalMutationOutcome::KnownUncommitted { cleanup, .. } => Some(cleanup),
                    ExternalMutationOutcome::CommitUnknown { .. } => None,
                };
                if let Some(failure) =
                    cleanup.and_then(FrontendStatisticsAttemptExecutor::finalization_failure)
                {
                    self.progress
                        .record_publication_cleanup_failure(failure)
                        .map_err(|error| {
                            health(
                                "statistics cleanup observation was not retained",
                                Arc::new(error),
                            )
                        })?;
                }
            }
            self.cleanup_observed = true;
        }
        if original.session.is_some()
            && !matches!(
                original.close,
                Some(StatisticsPublicationClose::Observed {
                    owned_objects_disposed: true,
                    ..
                })
            )
        {
            return Poll::Pending;
        }
        Poll::Ready(Ok(()))
    }
}
fn health(
    message: &'static str,
    cause: Arc<dyn std::error::Error + Send + Sync>,
) -> StatementNativeHealthFailure {
    StatementNativeHealthFailure::actual_exit_unproven(message, cause)
}
impl GovernedBackgroundAction for StatisticsAction {
    fn poll_action(
        &mut self,
        context: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        let state = self.state.as_mut().unwrap();
        match state.poll_business(context, cx) {
            Poll::Ready(Err(error)) if !state.selected => {
                // Product state selects an ordinary failure independently of
                // the shared bank's process-health result.
                state
                    .progress
                    .finish(Err(failure(&error)), now_ms())
                    .map_err(service_error)?;
                state.selected = true;
                Poll::Ready(Err(error))
            }
            result => result,
        }
    }
    fn external_effect_disposition(
        &self,
    ) -> Option<novarocks_query_application::api::StatementExternalEffectDisposition> {
        self.state
            .as_ref()
            .and_then(|s| s.disposition)
            .map(crate::query::external_effect_from_lake)
    }
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.state.as_ref().and_then(|s| {
            s.health
                .clone()
                .or_else(|| s.io.health_failure())
                .or_else(|| s.computation.local_health_failure())
                .or_else(|| {
                    s.cpu.local_failure().map(|cause| {
                        health("statistics provider CPU actual exit is unproven", cause)
                    })
                })
        })
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if target.is_some() {
            return Err(health(
                "statistics residual receiver is occupied",
                Arc::new(WorkError::Conflict),
            ));
        }
        if let Some(state) = self.state.as_mut() {
            state.seal_credentials();
        }
        if let Some(state) = self.state.take() {
            *target = Some(Box::new(StatisticsResidual { state }));
        }
        Ok(())
    }
}
impl GovernedStatementResidual for StatisticsResidual {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.state.poll_close(cx)
    }
}

impl FrontendStatisticsAttemptExecutor {
    fn failure(message: impl Into<String>) -> CoreStatisticsAttemptError {
        CoreStatisticsAttemptError::Failed(StatisticsFailure {
            compile_control: None,
            message: Arc::from(message.into()),
        })
    }

    fn scope_error(error: novarocks_workload_control::WorkError) -> CoreStatisticsAttemptError {
        if matches!(error, novarocks_workload_control::WorkError::Cancelled(_)) {
            CoreStatisticsAttemptError::Cancelled(StatisticsFailure {
                compile_control: None,
                message: Arc::from(error.to_string()),
            })
        } else {
            Self::failure(error.to_string())
        }
    }

    fn application_error(error: StatisticsApplicationError) -> CoreStatisticsAttemptError {
        Self::application_error_ref(&error)
    }
    fn application_error_ref(error: &StatisticsApplicationError) -> CoreStatisticsAttemptError {
        if error.target_binding_failure().is_some() {
            CoreStatisticsAttemptError::Stale(StatisticsFailure {
                compile_control: None,
                message: Arc::from(error.to_string()),
            })
        } else {
            Self::failure(error.to_string())
        }
    }

    fn request(
        job: &StatisticsJob,
    ) -> Result<StatisticsAttemptRequest, CoreStatisticsAttemptError> {
        let operation_id = novarocks_spi::connector::LakePublicationId::try_from_uuid(
            job.publication_id.as_uuid(),
        )
        .map_err(|error| Self::failure(error.to_string()))?;
        Ok(StatisticsAttemptRequest {
            operation_id,
            connector_instance_id: job.target.catalog.to_string(),
            namespace: job.target.namespace.to_string(),
            table: job.target.table.to_string(),
            object_id: job.target.object_id.to_vec(),
            columns: match &job.columns {
                StatisticsColumns::All => StatisticsColumnIntent::AllColumns,
                StatisticsColumns::Explicit(columns) => StatisticsColumnIntent::Explicit(
                    columns.iter().map(ToString::to_string).collect(),
                ),
            },
        })
    }

    fn context(
        &self,
        scope: &novarocks_workload_control::WorkScope,
    ) -> Result<
        (
            Instant,
            novarocks_query_application::cancellation::QueryCancellationView,
        ),
        CoreStatisticsAttemptError,
    > {
        scope.check().map_err(Self::scope_error)?;
        let deadline = scope
            .cancellation()
            .map_err(Self::scope_error)?
            .deadline()
            .map(Into::into)
            .unwrap_or_else(|| Instant::now() + self.ports.attempt_timeout);
        let cancellation =
            novarocks_query_application::cancellation::QueryCancellationView::governed(
                scope.cancellation().map_err(Self::scope_error)?,
                None,
            );
        if cancellation.is_cancelled() {
            return Err(CoreStatisticsAttemptError::Cancelled(StatisticsFailure {
                compile_control: None,
                message: Arc::from("statistics attempt cancelled before phase start"),
            }));
        }
        Ok((deadline, cancellation))
    }

    fn collection_context(
        &self,
        deadline: Instant,
        cancellation: novarocks_query_application::cancellation::QueryCancellationView,
    ) -> Result<ConnectorRequestContext, StatisticsApplicationError> {
        crate::connector::query_connector_request_context_on_runtime(
            &self.ports.runtime,
            deadline,
            cancellation,
        )
        .map(|context| {
            context
                .with_initiation(novarocks_spi::connector::ConnectorRequestInitiation::JobAttempt)
        })
        .map_err(StatisticsApplicationError::new)
    }

    #[cfg(test)]
    fn publication_outcome(
        outcome: ExternalMutationOutcome<novarocks_spi::connector::StatisticsReceipt>,
    ) -> StatisticsPublicationOutcome {
        Self::publication_outcome_ref(&outcome)
    }
    fn publication_failure(
        error: &novarocks_spi::connector::ConnectorMutationFailure,
    ) -> novarocks_statistics_application::StatisticsPublicationFailure {
        use novarocks_failure_contract::FailureProjection;
        let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
        let mut compile_control = None;
        while let Some(cause) = current {
            if let Some(control) =
                cause.downcast_ref::<novarocks_type_contract::CompileControlError>()
            {
                compile_control = Some(control.provenance());
                break;
            }
            current = cause.source();
        }
        let observation = novarocks_statistics_application::StatisticsPublicationFailure::capture(
            error.failure_fact(),
            compile_control,
            error,
        );
        // Reuse the already captured bounded local detail. No second Provider
        // formatting or source traversal is performed for logging.
        tracing::debug!(
            fact = ?observation.fact(),
            compile_control = ?observation.compile_control(),
            diagnostic = observation.local_diagnostic(),
            "Statistics original publication failed",
        );
        observation
    }
    fn finalization_failure(
        value: &ExternalMutationFinalization,
    ) -> Option<novarocks_statistics_application::StatisticsPublicationFailure> {
        match value {
            ExternalMutationFinalization::Complete => None,
            ExternalMutationFinalization::Failed(error) => Some(Self::publication_failure(error)),
        }
    }

    fn publication_ready_outcome(
        ready: &StatisticsPublicationReady,
    ) -> StatisticsPublicationOutcome {
        match ready {
            StatisticsPublicationReady::Committed {
                effect,
                finalization,
                ..
            } => Self::completed_publication(*effect, finalization, None),
            StatisticsPublicationReady::Settled(outcome) => Self::publication_outcome_ref(outcome),
        }
    }
    fn completed_publication(
        effect: novarocks_spi::connector::ExternalMutationEffect,
        finalization: &ExternalMutationFinalization,
        cleanup: Option<&ExternalMutationFinalization>,
    ) -> StatisticsPublicationOutcome {
        let finalization_failure = Self::finalization_failure(finalization);
        let cleanup_failure = cleanup.and_then(Self::finalization_failure);
        match effect {
            novarocks_spi::connector::ExternalMutationEffect::Applied => {
                StatisticsPublicationOutcome::KnownCommitted {
                    finalization_failure,
                    cleanup_failure,
                }
            }
            novarocks_spi::connector::ExternalMutationEffect::NoOp => {
                StatisticsPublicationOutcome::NoOp {
                    finalization_failure,
                    cleanup_failure,
                }
            }
        }
    }
    fn publication_outcome_ref(
        outcome: &ExternalMutationOutcome<novarocks_spi::connector::StatisticsReceipt>,
    ) -> StatisticsPublicationOutcome {
        match outcome {
            ExternalMutationOutcome::KnownCommitted {
                effect,
                finalization,
                cleanup,
                ..
            } => Self::completed_publication(*effect, finalization, Some(cleanup)),
            ExternalMutationOutcome::KnownUncommitted { failure, cleanup } => {
                StatisticsPublicationOutcome::KnownUncommitted {
                    failure: Self::publication_failure(failure),
                    cleanup_failure: Self::finalization_failure(cleanup),
                }
            }
            ExternalMutationOutcome::CommitUnknown { failure, .. } => {
                StatisticsPublicationOutcome::CommitUnknown {
                    failure: Self::publication_failure(failure),
                }
            }
        }
    }
}

async fn begin_session(
    slot: Arc<tokio::sync::Mutex<SessionSlot>>,
    request: StatisticsAttemptRequest,
    planning: ConnectorControlPlanningLease,
    lease: ConnectorStatisticsLease,
    context: ConnectorRequestContext,
    cpu: Arc<dyn StatisticsCpuPort>,
) -> Result<(), CoreStatisticsAttemptError> {
    // This stable slot remains owned by Action/Residual if its waiter unwinds.
    let mut original = slot.lock().await;
    let binding = match super::application::rebind_table_object_async(
        &planning,
        context.clone(),
        &request,
        cpu.clone(),
    )
    .await
    {
        Ok(binding) => binding,
        Err(error) => {
            let projected = FrontendStatisticsAttemptExecutor::application_error_ref(&error);
            original.binding_failure = Some(error);
            return Err(projected);
        }
    };
    let selection = match &request.columns {
        StatisticsColumnIntent::AllColumns => StatisticsColumnSelection::Default,
        StatisticsColumnIntent::Explicit(_) => StatisticsColumnSelection::explicit(
            resolve_columns(&request, &binding.sql_columns)
                .map_err(FrontendStatisticsAttemptExecutor::application_error)?
                .into_iter()
                .map(|c| Arc::from(c.name().as_str()))
                .collect(),
        )
        .map_err(failure)?,
    };
    let start = match lease
        .begin_collection_async(
            StatisticsCollectionStartRequest {
                operation_id: operation_id(&request),
                table: binding.table,
                data_version: binding.data_version,
                selection,
                context,
            },
            cpu,
        )
        .await
    {
        Ok(start) => start,
        Err(error) => {
            let projected = failure(&error);
            original.provider_failure = Some(error);
            return Err(projected);
        }
    };
    let (table, _, ordinal, required, session) = start.into_parts();
    original.session = Some(session);
    if required.is_empty() {
        original.artifacts = Some(Vec::new());
    } else {
        original.program = Some(
            crate::query_execution::statistics::StatisticsCollectionProgram::try_new(
                table, ordinal, required,
            )
            .map_err(failure)?,
        );
    }
    Ok(())
}
async fn prepare_plan(
    slot: Arc<tokio::sync::Mutex<PlanSlot>>,
    provider: Arc<FrontendCompletionCustody>,
    ports: StatisticsAttemptExecutorPorts,
    execution: QueryExecutionContext,
    context: ConnectorRequestContext,
    identity: crate::query_execution::statistics::StatisticsRelationIdentity,
    program: crate::query_execution::statistics::StatisticsCollectionProgram,
    planning: ConnectorControlPlanningLease,
    stage: WorkScope,
    wake: Arc<StatisticsWake>,
) -> Result<(), CoreStatisticsAttemptError> {
    let mut original = slot.lock().await;
    let PlanSlot {
        custody,
        job,
        outcome,
        ..
    } = &mut *original;
    let result = prepare_statistics_computation_in(
        StatisticsComputationPlanningServices {
            completion: ports.completion,
            blocking: &ports.execution_runtime.0,
            functions: ports.function_catalog,
            constant_policy: ports.constant_policy,
            static_plan_carrier: ports.static_plan_carrier,
            wake,
        },
        &execution,
        context,
        &identity,
        program,
        planning,
        &stage,
        &provider,
        StatisticsPreparationCells {
            custody,
            job,
            outcome,
        },
    )
    .await;
    match result {
        Ok(()) => Ok(()),
        Err(error) => {
            let projected = CoreStatisticsAttemptError::Failed(StatisticsFailure {
                compile_control: error.compile_control_error(),
                message: Arc::from(error.to_string()),
            });
            original.failure = Some(StatisticsPreparationFailure::Domain(error));
            Err(projected)
        }
    }
}
async fn publish_session(
    slot: Arc<tokio::sync::Mutex<SessionSlot>>,
) -> Result<(), CoreStatisticsAttemptError> {
    let mut original = slot.lock().await;
    let SessionSlot {
        session,
        artifacts,
        publication,
        provider_failure,
        ..
    } = &mut *original;
    let session = session
        .as_mut()
        .ok_or_else(|| failure("statistics publication has no original session"))?;
    if let Err(error) = session.prepare_publication_async(artifacts).await {
        let projected = failure(&error);
        *provider_failure = Some(error);
        return Err(projected);
    }
    match session.finish_publication_async().await {
        Ok(ready) => *publication = Some(ready),
        Err(error) => {
            let projected = failure(&error);
            *provider_failure = Some(error);
            return Err(projected);
        }
    }
    Ok(())
}
async fn close_session(
    slot: Arc<tokio::sync::Mutex<SessionSlot>>,
    plan: Arc<tokio::sync::Mutex<PlanSlot>>,
    provider: Arc<FrontendCompletionCustody>,
    context: ConnectorRequestContext,
    cpu: Arc<dyn StatisticsCpuPort>,
) -> Result<(), StatementNativeHealthFailure> {
    {
        let mut original = slot.lock().await;
        if let Some(session) = original.session.as_mut() {
            // These are product facts. Unknown disposal keeps the SAME
            // session/retention in the residual; it is not a process failure.
            match session.close_publication_async(&context).await {
                Ok(observation) => original.close = Some(observation),
                Err(error) => original.close_failure = Some(error),
            }
        }
    }
    std::future::poll_fn(|cx| cpu.poll_closed(cx))
        .await
        .map_err(|e| health("statistics CPU close failed", Arc::new(e)))?;
    {
        let mut original = plan.lock().await;
        let PlanSlot { job, custody, .. } = &mut *original;
        if let Some(job) = job.as_mut() {
            job.close(custody)
                .await
                .map_err(|e| health("statistics plan CPU close failed", e))?;
        }
        custody.close(&provider).await?;
    }
    provider.close().await?;
    Ok(())
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorError, ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
        ExternalMutationEffect, ProviderBindingEpoch, StatisticsArtifactIdentity,
        StatisticsDataVersion, StatisticsEvidenceRevision, StatisticsReceipt,
        StatisticsSessionFuture,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn uncommitted_statistics_keeps_original_publication_and_cleanup_causes() {
        use novarocks_failure_contract::FailureProjection;
        use novarocks_spi::connector::{ConnectorMutationFailure, ConnectorMutationFailureKind};
        let cause = ConnectorMutationFailure::from_connector(ConnectorError::compile_interruption(
            novarocks_type_contract::CompileControlError::ResourceExhausted,
        ));
        let cleanup = ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Unavailable,
            "private Puffin cleanup location",
        );
        let outcome = FrontendStatisticsAttemptExecutor::publication_outcome(
            ExternalMutationOutcome::KnownUncommitted {
                failure: cause.clone(),
                cleanup: ExternalMutationFinalization::Failed(cleanup.clone()),
            },
        );
        let StatisticsPublicationOutcome::KnownUncommitted {
            failure,
            cleanup_failure,
        } = outcome
        else {
            panic!("exact uncommitted fact")
        };
        assert_eq!(
            failure.compile_control(),
            Some(novarocks_type_contract::CompileControlProvenance::ResourceExhausted)
        );
        assert_eq!(failure.fact(), cause.failure_fact());
        assert_eq!(failure.local_diagnostic(), cause.to_string());
        let cleanup_failure = cleanup_failure.unwrap();
        assert_eq!(cleanup_failure.fact(), cleanup.failure_fact());
        assert_eq!(cleanup_failure.local_diagnostic(), cleanup.to_string());
        assert!(!cleanup_failure.to_string().contains("private Puffin"));
    }

    #[test]
    fn unknown_statistics_preserves_original_cause_and_never_claims_finalization() {
        use novarocks_failure_contract::FailureProjection;
        use novarocks_spi::connector::{
            ConnectorMutationFailure, ConnectorMutationFailureKind, ExternalMutationEvidence,
        };
        let cause = ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Unavailable,
            "private catalog response",
        );
        let outcome = FrontendStatisticsAttemptExecutor::publication_outcome(
            ExternalMutationOutcome::CommitUnknown {
                failure: cause.clone(),
                evidence: ExternalMutationEvidence::try_new(
                    1,
                    ConnectorInstanceDescriptor {
                        provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                        instance_id: ConnectorInstanceId::parse("ice.main").unwrap(),
                    },
                    ProviderBindingEpoch::from_bytes([7; 16]),
                    ConnectorMutationOperationId::new(),
                    "statistics-publish",
                    Bytes::from_static(b"original evidence"),
                )
                .unwrap(),
            },
        );
        let StatisticsPublicationOutcome::CommitUnknown { failure } = outcome else {
            panic!("exact unknown fact")
        };
        assert_eq!(failure.fact(), cause.failure_fact());
        assert_eq!(failure.local_diagnostic(), cause.to_string());
        assert!(!failure.to_string().contains("private catalog"));
    }

    struct EmptyCollectionSession {
        descriptor: ConnectorInstanceDescriptor,
        finish_calls: Arc<AtomicUsize>,
        close_calls: Arc<AtomicUsize>,
        settled: bool,
        prepared: bool,
        published: bool,
    }
    impl EmptyCollectionSession {
        fn receipt(&self) -> StatisticsReceipt {
            StatisticsReceipt::try_new(
                self.descriptor.clone(),
                self.incarnation(),
                self.operation_id(),
                StatisticsDataVersion::try_new(Bytes::from_static(b"snapshot-42")).unwrap(),
                StatisticsEvidenceRevision::try_new(Bytes::from_static(b"empty")).unwrap(),
                Bytes::new(),
            )
            .unwrap()
        }
    }
    impl AsyncStatisticsCollectionSession for EmptyCollectionSession {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> ProviderBindingEpoch {
            ProviderBindingEpoch::from_bytes([7; 16])
        }
        fn operation_id(&self) -> ConnectorMutationOperationId {
            ConnectorMutationOperationId::from_bytes([8; 16])
        }
        fn expectations(&self) -> &[StatisticsArtifactIdentity] {
            &[]
        }
        fn prepare_publication_async<'a>(
            &'a mut self,
            artifacts: &'a mut Option<Vec<StatisticsArtifactDraft>>,
        ) -> StatisticsSessionFuture<'a, ()> {
            Box::pin(async move {
                assert!(artifacts.as_ref().unwrap().is_empty());
                assert!(!self.prepared);
                drop(artifacts.take());
                self.prepared = true;
                Ok(())
            })
        }
        fn finish_publication_async(
            &mut self,
        ) -> StatisticsSessionFuture<'_, StatisticsPublicationReady> {
            Box::pin(async move {
                assert!(self.prepared && !self.published);
                self.published = true;
                self.finish_calls.fetch_add(1, Ordering::SeqCst);
                Ok(if self.settled {
                    StatisticsPublicationReady::Settled(ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::NoOp,
                        receipt: self.receipt(),
                        finalization: ExternalMutationFinalization::Complete,
                        cleanup: ExternalMutationFinalization::Complete,
                    })
                } else {
                    StatisticsPublicationReady::Committed {
                        effect: ExternalMutationEffect::NoOp,
                        receipt: self.receipt(),
                        finalization: ExternalMutationFinalization::Complete,
                    }
                })
            })
        }
        fn close_publication_async<'a>(
            &'a mut self,
            _context: &'a ConnectorRequestContext,
        ) -> StatisticsSessionFuture<'a, StatisticsPublicationClose> {
            Box::pin(async move {
                assert_eq!(self.close_calls.fetch_add(1, Ordering::SeqCst), 0);
                let outcome = if self.published {
                    ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::NoOp,
                        receipt: self.receipt(),
                        finalization: ExternalMutationFinalization::Complete,
                        cleanup: ExternalMutationFinalization::Complete,
                    }
                } else {
                    ExternalMutationOutcome::KnownUncommitted {
                        failure: novarocks_spi::connector::ConnectorMutationFailure::new(
                            novarocks_spi::connector::ConnectorMutationFailureKind::Conflict,
                            "collection cancelled",
                        ),
                        cleanup: ExternalMutationFinalization::Complete,
                    }
                };
                Ok(StatisticsPublicationClose::Observed {
                    outcome,
                    owned_objects_disposed: true,
                })
            })
        }
    }
    fn empty_session(
        settled: bool,
    ) -> (
        Arc<tokio::sync::Mutex<SessionSlot>>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let finishes = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let original = SessionSlot {
            session: Some(Box::new(EmptyCollectionSession {
                descriptor: ConnectorInstanceDescriptor {
                    provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                    instance_id: ConnectorInstanceId::parse("ice.main").unwrap(),
                },
                finish_calls: finishes.clone(),
                close_calls: closes.clone(),
                settled,
                prepared: false,
                published: false,
            })),
            artifacts: Some(Vec::new()),
            ..SessionSlot::default()
        };
        (
            Arc::new(tokio::sync::Mutex::new(original)),
            finishes,
            closes,
        )
    }
    #[tokio::test]
    async fn empty_collection_is_retained_until_needed_publication_without_a_query_request() {
        let (slot, finishes, closes) = empty_session(false);
        assert!(slot.lock().await.program.is_none());
        assert_eq!(finishes.load(Ordering::SeqCst), 0);
        publish_session(slot.clone()).await.unwrap();
        let original = slot.lock().await;
        assert!(
            original.session.is_some(),
            "NeededReady retains the original session for actual close"
        );
        assert!(matches!(
            original.publication,
            Some(StatisticsPublicationReady::Committed { .. })
        ));
        assert_eq!(finishes.load(Ordering::SeqCst), 1);
        assert_eq!(
            closes.load(Ordering::SeqCst),
            0,
            "publication does not manufacture GC exit"
        );
    }
    // Reuse the actual async session/publish_session component in the Attached
    // waiter regression. Both SPI variants carry the same original NoOp fact.
    pub(in crate::statistics_jobs) async fn no_op_publication_for_test(
        settled: bool,
    ) -> StatisticsPublicationOutcome {
        let (slot, finishes, closes) = empty_session(settled);
        publish_session(slot.clone()).await.unwrap();
        let original = slot.lock().await;
        let outcome = FrontendStatisticsAttemptExecutor::publication_ready_outcome(
            original.publication.as_ref().unwrap(),
        );
        assert_eq!(outcome.fact(), StatisticsPublicationFact::NoOp);
        assert!(original.session.is_some());
        assert_eq!(finishes.load(Ordering::SeqCst), 1);
        assert_eq!(closes.load(Ordering::SeqCst), 0);
        outcome
    }
    #[test]
    fn explicit_statistics_columns_keep_missing_duplicate_and_ambiguous_rejections() {
        use arrow::datatypes::{DataType, Field};
        let request = |names: Vec<&str>| StatisticsAttemptRequest {
            operation_id: novarocks_spi::connector::LakePublicationId::try_from_uuid(
                uuid::Uuid::now_v7(),
            )
            .unwrap(),
            connector_instance_id: "ice".into(),
            namespace: "db".into(),
            table: "t".into(),
            object_id: vec![1],
            columns: StatisticsColumnIntent::Explicit(
                names.into_iter().map(str::to_string).collect(),
            ),
        };
        let a = Arc::new(Field::new("a", DataType::Int64, true));
        assert!(resolve_columns(&request(vec!["missing"]), std::slice::from_ref(&a)).is_err());
        assert!(resolve_columns(&request(vec!["a", "A"]), std::slice::from_ref(&a)).is_err());
        assert!(
            resolve_columns(
                &request(vec!["a"]),
                &[a, Arc::new(Field::new("A", DataType::Int64, true))]
            )
            .is_err()
        );
    }
}
