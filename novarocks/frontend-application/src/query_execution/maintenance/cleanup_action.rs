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

//! SQL cleanup borrows the original Root, local result ticket and Product.
use super::{
    RequestScopedMaintenanceEngine,
    io_admission::{MetadataIoAdmission, extend_original_dependencies},
};
use crate::query_execution::kernels::MaintenanceExecutionKernel;
use crate::state_store::execution::StateStoreOriginalOwner;
use novarocks_failure_contract::SharedFailureCause;
use novarocks_query_application::{
    admitted_query_context::QueryExecutionContext,
    api::{LocalResultProducer, StatementNativeHealthFailure},
    session_control::{
        GovernedActionContext, GovernedStatementAction, GovernedStatementResidual,
        StatementActionValue,
    },
    session_error::{QueryServiceError, QueryServiceErrorKind},
};
use novarocks_spi::connector::{
    ConnectorCleanupAsyncExecution, ConnectorCleanupAsyncObservation,
    ConnectorCleanupAsyncOutput as Output, ConnectorCleanupAsyncStart,
    ConnectorCleanupAsyncStep as Step, ConnectorCleanupCandidatePageRequest,
    ConnectorCleanupExecuteRequest, ConnectorCleanupFinalizeRequest,
    ConnectorCleanupMaintenanceLease, ConnectorCleanupOperationId,
    ConnectorCleanupOwnedRefSelection, ConnectorCleanupPrepareRequest,
    ConnectorControlPlanningLease, ConnectorError, ConnectorOriginalResultScope,
    ConnectorRequestContext, LakePublicationDisposition, ProviderMetadataCpuPort, SharedDependency,
};
use novarocks_state_store_api::StateStoreExecutionContext;
use novarocks_table_maintenance::gc_observation::{
    GcOwnedRefObservation, GcOwnedRefObservationAccelerator, GcOwnedRefObservationDecision,
    GcOwnedRefObservationInError,
};
use novarocks_table_maintenance::{
    MaintenanceActionOutcome, MaintenanceActionRequest, MaintenanceTarget,
    product::{
        CleanupCandidate, CleanupOwnedRefFact, CleanupSession, CleanupTerminal,
        DistributedRewriteSession, MaintenanceFuture, RewriteIntent, TableMaintenanceEffectPort,
        TableMaintenanceProduct,
    },
    runtime::TerminalError,
};
use novarocks_workload_control::{ResultWindowAlias, ResultWindowClass, WorkError};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
type ProductF =
    Pin<Box<dyn Future<Output = Result<MaintenanceActionOutcome, TerminalError>> + Send>>;
#[derive(Default)]
struct Wake {
    waker: futures::task::AtomicWaker,
    observation: std::sync::Mutex<Option<ConnectorCleanupAsyncObservation>>,
}
impl std::fmt::Debug for Wake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CleanupWake")
    }
}
impl crate::task_execution::status_intake::StatusIntakeWake for Wake {
    fn wake(&self) {
        self.waker.wake()
    }
}
struct Body {
    engine: RequestScopedMaintenanceEngine,
    product: Arc<TableMaintenanceProduct>,
    target: MaintenanceTarget,
    older_than: i64,
    lease: ConnectorCleanupMaintenanceLease,
    planning: ConnectorControlPlanningLease,
    context: ConnectorRequestContext,
    cpu: Option<Arc<dyn ProviderMetadataCpuPort>>,
    original: Option<ConnectorOriginalResultScope>,
    wake: Arc<Wake>,
    start: Option<ConnectorCleanupAsyncStart>,
    step: Option<Step>,
    session: Option<ConnectorCleanupAsyncExecution>,
    plan: Option<novarocks_spi::connector::ConnectorCleanupPlan>,
    page: Option<novarocks_spi::connector::CandidatePage>,
    candidates: Arc<[CleanupCandidate]>,
    output: Option<Result<Output, ConnectorError>>,
    prepared: Option<novarocks_spi::connector::PreparedBatch>,
    receipt: Option<novarocks_spi::connector::BatchReceipt>,
    dispatch_started: bool,
    first_error: Option<ConnectorError>,
    failure: Option<SharedFailureCause>,
    required_finalization_failed: bool,
    state_store_context: Option<StateStoreExecutionContext>,
    gc_failure: Option<GcOwnedRefObservationInError>,
}
struct State {
    family: novarocks_result_contract::ClientPresentationFamily,
    body: Arc<tokio::sync::Mutex<Body>>,
    dependencies: Vec<SharedDependency>,
    io: Option<Arc<MetadataIoAdmission>>,
    cpu: Option<Arc<dyn ProviderMetadataCpuPort>>,
    origin: Option<ResultWindowAlias>,
    spark_call: bool,
    local: Option<LocalResultProducer>,
    wake: Arc<Wake>,
    future: Option<ProductF>,
    result: Option<Result<MaintenanceActionOutcome, TerminalError>>,
    ordinary_io_failure: Option<WorkError>,
    state_store: Option<StateStoreOriginalOwner>,
    state_store_prepared: bool,
    state_store_close_requested: bool,
    state_store_progress_failure: Option<SharedFailureCause>,
    product_exited: bool,
    source_closed: bool,
    selected: bool,
}
struct Action {
    state: Option<State>,
}
struct Residual {
    state: State,
}
fn service(error: impl ToString) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, error.to_string())
}
fn original_service(error: WorkError) -> QueryServiceError {
    QueryServiceError::from_failure_cause(SharedFailureCause::from_static_detail(
        error,
        "cleanup original admission failed",
    ))
}
fn health(error: impl std::error::Error + Send + Sync + 'static) -> StatementNativeHealthFailure {
    StatementNativeHealthFailure::actual_exit_unproven(
        "cleanup original source exit is unproven",
        Arc::new(error),
    )
}
pub(crate) fn prepare(
    kernel: &MaintenanceExecutionKernel,
    target: MaintenanceTarget,
    older_than: i64,
    spark_call: bool,
    execution: &QueryExecutionContext,
    connector: &ConnectorRequestContext,
    family: novarocks_result_contract::ClientPresentationFamily,
) -> Result<Box<dyn GovernedStatementAction>, TerminalError> {
    let capacity = execution.result_capacity().ok_or_else(|| {
        TerminalError::pre_dispatch_failed("cleanup has no original result capacity")
    })?;
    let origin = capacity
        .try_window_alias()
        .map_err(TerminalError::from_original_cause)?;
    if origin.class() != ResultWindowClass::Local || !origin.is_for_scope(capacity.root_scope()) {
        return Err(TerminalError::pre_dispatch_failed(
            "cleanup requires its original SQL Local declaration",
        ));
    }
    let identity = RequestScopedMaintenanceEngine::target_identity(&target)
        .map_err(TerminalError::pre_dispatch_failed)?;
    let planning = kernel
        .connector_control()
        .acquire_current(&identity.instance_id)
        .map_err(TerminalError::from_original_cause)?;
    let lease = kernel
        .connector_control()
        .acquire_exact_cleanup_maintenance(planning.control_runtime_id())
        .map_err(TerminalError::from_original_cause)?;
    let dependencies = lease
        .initial_dependencies()
        .map_err(TerminalError::from_original_cause)?;
    let product = kernel
        .service()
        .original_optimize_host()
        .map_err(TerminalError::pre_dispatch_failed)?
        .product;
    let wake = Arc::new(Wake::default());
    let body = Arc::new(tokio::sync::Mutex::new(Body {
        engine: RequestScopedMaintenanceEngine::new(
            kernel.clone(),
            execution.clone(),
            connector.clone(),
        ),
        product,
        target,
        older_than,
        lease,
        planning,
        context: connector.clone(),
        cpu: None,
        original: None,
        wake: wake.clone(),
        start: None,
        step: None,
        session: None,
        plan: None,
        page: None,
        candidates: Arc::from([]),
        output: None,
        prepared: None,
        receipt: None,
        dispatch_started: false,
        first_error: None,
        failure: None,
        required_finalization_failed: false,
        state_store_context: None,
        gc_failure: None,
    }));
    Ok(Box::new(Action {
        state: Some(State {
            family,
            body,
            dependencies,
            io: None,
            cpu: None,
            origin: Some(origin),
            spark_call,
            local: None,
            wake,
            future: None,
            result: None,
            ordinary_io_failure: None,
            state_store: None,
            state_store_prepared: false,
            state_store_close_requested: false,
            state_store_progress_failure: None,
            product_exited: false,
            source_closed: false,
            selected: false,
        }),
    }))
}
impl State {
    fn poll_product(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.product_exited {
            return Poll::Ready(());
        }
        let Some(future) = self.future.as_mut() else {
            return Poll::Ready(());
        };
        let result = std::task::ready!(future.as_mut().poll(cx));
        self.result = Some(result);
        // Result is stable before foreign F Drop. If Drop panics the original
        // Statement Source keeps this WHOLE Action and does not repoll it.
        drop(self.future.take());
        self.product_exited = true;
        Poll::Ready(())
    }
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        self.wake.waker.register(cx.waker());
        if self.selected {
            return Poll::Ready(Err(service("cleanup result was already selected")));
        }
        let scope = owner.scope().clone();
        if !self
            .origin
            .as_ref()
            .is_some_and(|origin| origin.is_for_scope(&scope))
        {
            return Poll::Ready(Err(service("cleanup borrowed another original Root")));
        }
        // Freeze the original Local delivery allowance before the first
        // Provider effect; an observed successful terminal is not cancelled by
        // trying to acquire a fresh ticket after the commit.
        if self.spark_call && self.local.is_none() {
            let window = owner
                .result_window_alias()
                .ok_or_else(|| service("cleanup original Local result window is absent"))?;
            self.local = Some(
                LocalResultProducer::try_new(owner.scope(), window, self.family)
                    .map_err(service)?,
            );
        }
        let work = owner
            .execution_owner_slot()
            .as_ref()
            .ok_or_else(|| service("cleanup original WorkOwner is absent"))?;
        if !self.state_store_prepared {
            let Ok(mut body) = self.body.try_lock() else {
                return Poll::Pending;
            };
            if let Ok(binding) = body.engine.kernel.state_store_original() {
                extend_original_dependencies(
                    &mut self.dependencies,
                    binding.blocking_dependency_facts(),
                );
                let original = binding
                    .for_root(scope.clone())
                    .map_err(QueryServiceError::from_failure_cause)?;
                let context = original.context(body.context.deadline());
                // Strong custody precedes the weak borrower and any Product F.
                self.state_store = Some(original);
                body.state_store_context = Some(context);
            }
            // Absence is frozen, not a legacy capability. Only an actual
            // owned-ref observation needs this service; NoOp does not.
            self.state_store_prepared = true;
        }
        self.progress_state_store(cx);
        if self.io.is_none() {
            self.io = Some(
                MetadataIoAdmission::for_original_source(work, self.dependencies.clone())
                    .map_err(original_service)?,
            );
        }
        let io = self.io.as_ref().unwrap();
        let progress = io.poll_progress(work, cx);
        if self.future.is_none() && !self.product_exited {
            if !*progress
                .as_ref()
                .map_err(|error| original_service(error.clone()))?
            {
                return Poll::Pending;
            }
            let Ok(mut body) = self.body.try_lock() else {
                return Poll::Pending;
            };
            let runtime = body
                .engine
                .kernel
                .rewrite_runtime()
                .map_err(service)?
                .clone();
            let capacity = io.root_capacity().map_err(original_service)?;
            // Retain data under the SAME original Root budget. The SQL Local
            // ticket is only for delivery; residual cleanup never keeps it.
            let execution = body
                .engine
                .execution
                .for_statement_retention(capacity.clone())
                .map_err(original_service)?;
            body.engine = RequestScopedMaintenanceEngine::new(
                body.engine.kernel.clone(),
                execution,
                body.context.clone(),
            );
            let cpu = runtime
                .execution
                .0
                .provider_creator_cpu()
                .map_err(service)?
                .cleanup_metadata_cpu_port(
                    &scope,
                    capacity.try_window_alias().map_err(original_service)?,
                    &body.lease,
                    body.engine.execution.cancellation().clone(),
                    runtime.execution.0.runtime().clone(),
                    self.wake.clone(),
                )
                .map_err(original_service)?;
            body.context = body.context.clone().with_io_admission(io.clone());
            body.original = Some(
                super::rewrite_capture::borrow_positive_capture(io, &body.context).map_err(
                    |e| {
                        QueryServiceError::from_failure_cause(
                            SharedFailureCause::from_static_detail(
                                e,
                                "cleanup original result source refused",
                            ),
                        )
                    },
                )?,
            );
            body.cpu = Some(cpu.clone());
            self.cpu = Some(cpu);
            drop(body);
            self.future = Some(Box::pin(run_product(self.body.clone())));
        }
        if self.poll_product(cx).is_pending() {
            // Physical I/O is progressed even after a business stop. Its
            // ordinary refusal is not an actual-exit/health verdict.
            if let Err(error) = progress {
                self.ordinary_io_failure.get_or_insert(error);
            }
            return Poll::Pending;
        }
        let result = self
            .result
            .take()
            .ok_or_else(|| service("cleanup original Product result is absent"))?;
        self.selected = true;
        let disposition = self.disposition();
        let outcome = result.map_err(|error| {
            let error = super::rewrite_job::computation_service_error(error);
            match disposition {
                Some(d) => error
                    .attach_external_effect_disposition(crate::query::external_effect_from_lake(d)),
                None => error,
            }
        })?;
        if !self.spark_call {
            return Poll::Ready(Ok(StatementActionValue::Ok(
                novarocks_query_application::session_control::StatementCommandCompletion::new(0),
            )));
        }
        let producer = self
            .local
            .take()
            .ok_or_else(|| service("cleanup positive Local producer is absent"))?;
        Poll::Ready(
            produce_local(producer, outcome)
                .map(StatementActionValue::Local)
                .map_err(service),
        )
    }

    fn disposition(&self) -> Option<LakePublicationDisposition> {
        let observer = self
            .wake
            .observation
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        observer.and_then(|source| source.publication_disposition())
    }
    fn local_health(&self) -> Option<StatementNativeHealthFailure> {
        self.state_store
            .as_ref()
            .and_then(|original| original.local_health_failure())
            .map(health)
            .or_else(|| {
                self.io
                    .as_ref()
                    .and_then(|io| io.health_failure())
                    .or_else(|| {
                        self.cpu
                            .as_ref()
                            .and_then(|cpu| cpu.local_failure())
                            .map(|cause| {
                                StatementNativeHealthFailure::actual_exit_unproven(
                                    "cleanup original CPU exit is unproven",
                                    cause,
                                )
                            })
                    })
            })
    }
    fn progress_state_store(&mut self, cx: &mut Context<'_>) {
        if let Some(original) = &self.state_store
            && let Err(failure) = original.poll_progress(cx)
        {
            // Keep the port's original progress failure while the Product
            // receives its own result. Health remains independently owned
            // by the same port and is observed through local_health().
            self.state_store_progress_failure.get_or_insert(failure);
        }
    }
    fn poll_state_store_closed(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if let Some(original) = &mut self.state_store {
            std::task::ready!(original.poll_closed(cx)).map_err(health)?;
        }
        Poll::Ready(Ok(()))
    }
    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.wake.waker.register(cx.waker());
        if !self.state_store_close_requested {
            self.state_store_close_requested = true;
            if let Some(original) = &self.state_store {
                original.request_close();
            }
        }
        self.progress_state_store(cx);
        let Some(io) = self.io.as_ref() else {
            return self.poll_state_store_closed(cx);
        };
        if let Some(observed) = io.withdraw_unstarted_in().map_err(health)? {
            if let Some(failure) = observed.into_ordinary_failure() {
                self.ordinary_io_failure.get_or_insert(failure);
            }
            return self.poll_state_store_closed(cx);
        }
        if let Err(error) = io.poll_continuing(cx) {
            self.ordinary_io_failure.get_or_insert(error);
        }
        std::task::ready!(self.poll_product(cx));
        if !self.source_closed {
            let Ok(mut body) = self.body.try_lock() else {
                return Poll::Pending;
            };
            if let Some(source) = &body.session {
                std::task::ready!(source.poll_closed(cx)).map_err(health)?;
            }
            if let Some(cpu) = &body.cpu {
                std::task::ready!(cpu.poll_closed(cx)).map_err(health)?;
            }
            let retired = (
                body.session.take(),
                body.cpu.take(),
                body.start.take(),
                body.step.take(),
                body.original.take(),
            );
            drop(body);
            let observation = self
                .wake
                .observation
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take();
            // Clear only after actual source/CPU exit, with all foreign Drops
            // outside both receiving locks. Unknown journals keep us Pending.
            drop(observation);
            drop(retired);
            drop(self.cpu.take());
            self.source_closed = true;
        }
        let observed = std::task::ready!(
            self.io
                .as_ref()
                .unwrap()
                .poll_actual_quiescent_continuing(cx)
        )?;
        if let Some(failure) = observed.into_ordinary_failure() {
            self.ordinary_io_failure.get_or_insert(failure);
        }
        self.poll_state_store_closed(cx)
    }
}
impl GovernedStatementAction for Action {
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        self.state.as_mut().unwrap().poll_action(owner, cx)
    }
    fn external_effect_disposition(
        &self,
    ) -> Option<novarocks_query_application::api::StatementExternalEffectDisposition> {
        self.state
            .as_ref()
            .and_then(State::disposition)
            .map(crate::query::external_effect_from_lake)
    }
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.state.as_ref().and_then(State::local_health)
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if target.is_some() {
            return Err(health(WorkError::Conflict));
        }
        if let Some(mut state) = self.state.take() {
            let local_delivery = (state.origin.take(), state.local.take());
            *target = Some(Box::new(Residual { state }));
            // Stable receiver precedes notification/last delivery alias Drop.
            drop(local_delivery);
        }
        Ok(())
    }
}
impl GovernedStatementResidual for Residual {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.state.poll_close(cx)
    }
}
struct Effects {
    body: Arc<tokio::sync::Mutex<Body>>,
}
struct BorrowedSession {
    body: Arc<tokio::sync::Mutex<Body>>,
    candidates: Arc<[CleanupCandidate]>,
}
// The same Source receives the whole GC error, including an unresolved exact
// CommitObservation, before the Product can publish its business conclusion.
async fn observe_owned_ref_original(
    original: Arc<tokio::sync::Mutex<Body>>,
    observations: &GcOwnedRefObservationAccelerator,
    observation: GcOwnedRefObservation,
    now_ms: i64,
    safe_gc_age_ms: i64,
) -> Result<GcOwnedRefObservationDecision, TerminalError> {
    let context = original.lock().await.state_store_context.clone();
    let Some(context) = context else {
        let error =
            GcOwnedRefObservationInError::Run(novarocks_state_store_runtime::RunFailure::Begin(
                novarocks_state_store_api::StateStoreOriginalUnsupported.into_original_failure(),
            ));
        return Err(receive_gc_failure(&original, error).await);
    };
    match observations
        .observe_in(&context, observation, now_ms, safe_gc_age_ms)
        .await
    {
        Ok(decision) => Ok(decision),
        Err(error) => Err(receive_gc_failure(&original, error).await),
    }
}
async fn receive_gc_failure(
    original: &Arc<tokio::sync::Mutex<Body>>,
    error: GcOwnedRefObservationInError,
) -> TerminalError {
    let mut incoming = Some(error);
    let terminal = {
        let mut body = original.lock().await;
        if body.gc_failure.is_none() {
            body.gc_failure = incoming.take();
        }
        gc_observation_terminal(body.gc_failure.as_ref().unwrap())
    };
    // Any later original error is disposed outside the Body lock.
    drop(incoming);
    terminal
}
fn gc_observation_terminal(error: &GcOwnedRefObservationInError) -> TerminalError {
    if let Some(cause) = error.original_failure() {
        return TerminalError::pre_dispatch_failed("cleanup GC maturity observation failed")
            .with_failure_cause(Some(cause.clone()));
    }
    // A GC validation/runner deadline has no external cause to fabricate.
    TerminalError::pre_dispatch_failed(error.to_string())
}

async fn run_product(
    body: Arc<tokio::sync::Mutex<Body>>,
) -> Result<MaintenanceActionOutcome, TerminalError> {
    let (product, target, older) = {
        let b = body.lock().await;
        (b.product.clone(), b.target.clone(), b.older_than)
    };
    product
        .execute_user_action(
            &Effects { body },
            MaintenanceActionRequest::RemoveOrphanFiles {
                target,
                older_than_ms: older,
            },
        )
        .await
}
impl TableMaintenanceEffectPort for Effects {
    fn observe_cleanup_owned_ref<'a>(
        &'a self,
        observations: &'a GcOwnedRefObservationAccelerator,
        observation: GcOwnedRefObservation,
        now_ms: i64,
        safe_gc_age_ms: i64,
    ) -> MaintenanceFuture<'a, Result<GcOwnedRefObservationDecision, TerminalError>> {
        Box::pin(observe_owned_ref_original(
            self.body.clone(),
            observations,
            observation,
            now_ms,
            safe_gc_age_ms,
        ))
    }

    fn reject_user_action_on_mv<'a>(
        &'a self,
        target: &'a MaintenanceTarget,
    ) -> MaintenanceFuture<'a, Result<(), novarocks_table_maintenance::runtime::TerminalError>>
    {
        Box::pin(reject_mv(self.body.clone(), target))
    }
    fn execute_metadata<'a>(
        &'a self,
        _request: MaintenanceActionRequest,
    ) -> MaintenanceFuture<'a, Result<MaintenanceActionOutcome, String>> {
        Box::pin(async { Err("cleanup Source cannot dispatch a metadata command".into()) })
    }
    fn begin_rewrite<'a>(
        &'a self,
        _target: &'a MaintenanceTarget,
        _intent: RewriteIntent,
    ) -> MaintenanceFuture<'a, Result<Box<dyn DistributedRewriteSession + 'a>, String>> {
        Box::pin(async { Err("cleanup Source cannot dispatch a rewrite".into()) })
    }
    fn begin_cleanup<'a>(
        &'a self,
        _target: &'a MaintenanceTarget,
        _older: i64,
    ) -> MaintenanceFuture<'a, Result<Box<dyn CleanupSession + 'a>, TerminalError>> {
        Box::pin(async move {
            begin_cleanup(self.body.clone())
                .await
                .map(|session| session as Box<dyn CleanupSession + 'a>)
        })
    }
}
async fn receive(body: &mut Body) -> Result<(), String> {
    let source = body
        .session
        .as_ref()
        .ok_or("cleanup original provider Source is absent")?;
    let result = std::future::poll_fn(|cx| source.poll_output(cx)).await;
    if let Err(error) = &result
        && body.first_error.is_none()
    {
        body.first_error = Some(error.clone());
    }
    body.output = Some(result);
    receive_original_failure(body);
    Ok(())
}
async fn step(body: &mut Body, input: Step) -> Result<(), String> {
    if body.output.is_some() {
        return Err("cleanup stable output receiving is occupied".into());
    }
    if body.step.is_some() {
        return Err("cleanup stable step input is occupied".into());
    }
    body.step = Some(input);
    if let Err(error) = body
        .session
        .as_ref()
        .ok_or("cleanup original provider Source is absent")?
        .install_step_in(&mut body.step)
    {
        let message = error.to_string();
        if body.first_error.is_none() {
            body.first_error = Some(error);
        }
        return Err(message);
    }
    receive(body).await
}
async fn start_plan(
    body: &mut Body,
    selection: Option<ConnectorCleanupOwnedRefSelection>,
) -> Result<(), String> {
    if body.dispatch_started {
        return Err("cleanup cannot replace a dispatched original plan".into());
    }
    if let Some(source) = &body.session {
        std::future::poll_fn(|cx| source.poll_closed(cx))
            .await
            .map_err(|e| e.to_string())?;
    }
    // The old read-only source has actually exited; its old plan/page data
    // remains paid in body while the replacement receives its immutable plan.
    let retired = {
        body.wake
            .observation
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
    };
    drop(retired);
    drop(body.session.take());
    body.start = Some(ConnectorCleanupAsyncStart {
        table: RequestScopedMaintenanceEngine::target_identity(&body.target)?,
        operation_id: ConnectorCleanupOperationId::new(),
        older_than_ms: body.older_than,
        selection,
        context: body.context.clone(),
        original: body
            .original
            .as_ref()
            .ok_or("cleanup original paid scope is absent")?
            .clone(),
    });
    let installed = body.lease.begin_async_in(
        &mut body.start,
        body.cpu
            .as_ref()
            .ok_or("cleanup scoped metadata CPU port is absent")?
            .clone(),
        &mut body.session,
    );
    if let Err(error) = installed {
        let message = error.to_string();
        if body.first_error.is_none() {
            body.first_error = Some(error);
        }
        return Err(message);
    }
    *body
        .wake
        .observation
        .lock()
        .unwrap_or_else(|p| p.into_inner()) = Some(body.session.as_ref().unwrap().observation());
    receive(body).await?;
    let Output::Plan(plan) = body.output.take().unwrap().map_err(|e| e.to_string())? else {
        return Err("cleanup plan output differs".into());
    };
    body.plan = Some(plan);
    Ok(())
}
impl CleanupSession for BorrowedSession {
    fn candidates(&self) -> &[CleanupCandidate] {
        &self.candidates
    }
    fn require_scanned_object_gc(&self) -> Result<(), String> {
        Err("scanned orphan cleanup is unsupported without Provider writer-fencing, Unknown/unreceived and restart safety evidence".into())
    }
    fn select_owned_refs<'a>(
        &'a mut self,
        indexes: &'a [usize],
    ) -> MaintenanceFuture<'a, Result<(), TerminalError>> {
        Box::pin(select_owned(self.body.clone(), indexes))
    }
    fn execute<'a>(&'a mut self) -> MaintenanceFuture<'a, Result<CleanupTerminal, TerminalError>> {
        Box::pin(execute_cleanup(self.body.clone()))
    }
}
async fn reject_mv(
    original: Arc<tokio::sync::Mutex<Body>>,
    target: &MaintenanceTarget,
) -> Result<(), TerminalError> {
    let body = original.lock().await;
    crate::mv::domain::iceberg_guard::reject_if_iceberg_mv_table_with_governed_planning_lease_and_context_async(body.engine.kernel.mv_storage_observation().as_ref(),&body.planning,body.engine.kernel.mv_readiness().as_ref(),&crate::catalog_application::resolver::TargetBackend{provider_id:body.lease.descriptor().provider_id.clone(),catalog:target.catalog.clone(),namespace:target.namespace.clone(),table:target.table.clone()},crate::mv::domain::iceberg_guard::IcebergMvUserMutation::Maintenance,body.context.clone(),body.cpu.as_ref().ok_or_else(|| TerminalError::pre_dispatch_failed("cleanup original metadata CPU is absent"))?.clone()).await.map_err(super::rewrite_job::guard_terminal_error)
}

async fn begin_cleanup_original(
    original: Arc<tokio::sync::Mutex<Body>>,
) -> Result<Box<dyn CleanupSession>, String> {
    let mut body = original.lock().await;
    start_plan(&mut body, None).await?;
    let plan = body.plan.as_ref().unwrap().clone();
    let context = body.context.clone();
    step(
        &mut body,
        Step::Page(ConnectorCleanupCandidatePageRequest {
            plan,
            offset: 0,
            limit: novarocks_spi::connector::MAX_CONNECTOR_CLEANUP_CANDIDATE_PAGE_ITEMS as u32,
            context,
        }),
    )
    .await?;
    let Output::Page(page) = body.output.take().unwrap().map_err(|e| e.to_string())? else {
        return Err("cleanup candidate output differs".into());
    };
    // Same original maturity pass: select only this exact bounded page; later
    // calls re-observe remaining refs. A page bound is not a whole-plan limit.
    if page.candidates().is_empty() && !page.complete() {
        return Err("cleanup discovery returned a non-terminal empty candidate page".into());
    }
    let candidates = page
        .candidates()
        .iter()
        .map(|c| match c {
            novarocks_spi::connector::ConnectorCleanupCandidate::Object { .. } => {
                CleanupCandidate::Object
            }
            novarocks_spi::connector::ConnectorCleanupCandidate::OwnedRef {
                table_uuid,
                name,
                head_snapshot_id,
                provenance_version,
                provenance_digest,
                ..
            } => CleanupCandidate::OwnedRef(CleanupOwnedRefFact {
                table_uuid: *table_uuid,
                ref_name: name.to_string(),
                head_snapshot_id: *head_snapshot_id,
                provenance_version: *provenance_version,
                provenance_digest: *provenance_digest,
            }),
        })
        .collect::<Vec<_>>();
    body.page = Some(page);
    body.candidates = Arc::from(candidates);
    let candidates = body.candidates.clone();
    drop(body);
    Ok(Box::new(BorrowedSession {
        body: original.clone(),
        candidates,
    }) as Box<dyn CleanupSession>)
}
async fn select_owned_original(
    original: Arc<tokio::sync::Mutex<Body>>,
    indexes: &[usize],
) -> Result<(), String> {
    let mut body = original.lock().await;
    let page = body
        .page
        .as_ref()
        .ok_or("cleanup original candidate page is absent")?;
    let identities = indexes
        .iter()
        .map(|i| {
            page.candidates()
                .get(*i)
                .and_then(|c| c.owned_ref_identity())
                .ok_or_else(|| "owned-ref candidate has no valid exact identity".to_owned())
        })
        .collect::<Result<Vec<_>, String>>()?;
    let selection =
        ConnectorCleanupOwnedRefSelection::try_new(identities).map_err(|e| e.to_string())?;
    start_plan(&mut body, Some(selection)).await
}
async fn read_report_locations(body: &mut Body) -> Result<Vec<String>, String> {
    use novarocks_query_application::api::LocalResultBound;
    let bound = LocalResultBound::V1;
    let count = usize::try_from(
        body.plan
            .as_ref()
            .ok_or("cleanup plan is absent")?
            .summary()
            .candidate_count(),
    )
    .map_err(|_| "cleanup report candidate count cannot be represented".to_owned())?;
    bound
        .admit(count, 0)
        .map_err(|e| format!("orphan cleanup report: {e}"))?;
    let mut locations = Vec::with_capacity(count);
    let mut bytes = 0usize;
    let mut offset = 0u64;
    loop {
        let plan = body.plan.as_ref().unwrap().clone();
        let context = body.context.clone();
        step(
            body,
            Step::Page(ConnectorCleanupCandidatePageRequest {
                plan,
                offset,
                limit: novarocks_spi::connector::MAX_CONNECTOR_CLEANUP_CANDIDATE_PAGE_ITEMS as u32,
                context,
            }),
        )
        .await?;
        let Output::Page(page) = body.output.take().unwrap().map_err(|e| e.to_string())? else {
            return Err("cleanup report page differs".into());
        };
        if page.candidates().is_empty() && !page.complete() {
            return Err("cleanup report returned a non-terminal empty page".into());
        }
        for candidate in page.candidates() {
            let len = candidate.display_key_len().map_err(|e| e.to_string())?;
            bytes = bytes
                .checked_add(LocalResultBound::cell_bytes(len))
                .ok_or_else(|| "cleanup report byte count overflow".to_owned())?;
            let rows = locations
                .len()
                .checked_add(1)
                .ok_or_else(|| "cleanup report row count overflow".to_owned())?;
            bound
                .admit(rows, bytes)
                .map_err(|e| format!("orphan cleanup report: {e}"))?;
            locations.push(candidate.display_key_string());
        }
        let complete = page.complete();
        offset = offset
            .checked_add(page.candidates().len() as u64)
            .ok_or_else(|| "cleanup report page offset overflow".to_owned())?;
        body.page = Some(page);
        if complete {
            return Ok(locations);
        }
    }
}

async fn execute_cleanup_original(
    original: Arc<tokio::sync::Mutex<Body>>,
) -> Result<CleanupTerminal, String> {
    let mut body = original.lock().await;
    // The original Local report contract is checked before any deletion,
    // one actual Provider page at a time, rather than rejecting a full plan
    // merely because one page is incomplete.
    let locations = read_report_locations(&mut body).await?;
    let batches = body.plan.as_ref().unwrap().summary().batch_count();
    for ordinal in 0..batches {
        let plan = body.plan.as_ref().unwrap().clone();
        let context = body.context.clone();
        step(
            &mut body,
            Step::Prepare(ConnectorCleanupPrepareRequest {
                plan,
                batch_ordinal: ordinal,
                context,
            }),
        )
        .await?;
        let Output::Prepared(prepared) = body.output.take().unwrap().map_err(|e| e.to_string())?
        else {
            return Err("cleanup prepared output differs".into());
        };
        body.prepared = Some(prepared);
        let prepared = body.prepared.take().unwrap();
        let plan = body.plan.as_ref().unwrap().clone();
        let context = body.context.clone();
        body.dispatch_started = true;
        step(
            &mut body,
            Step::Execute(ConnectorCleanupExecuteRequest {
                plan,
                prepared,
                context,
            }),
        )
        .await?;
        let output = body.output.take().unwrap();
        let receipt = match output {
            Ok(Output::Receipt(receipt)) => receipt,
            Ok(_) => return Err("cleanup receipt output differs".into()),
            Err(error) => return Err(error.to_string()),
        };
        let failed = receipt.summary().failed() != 0;
        let unknown = receipt.summary().unknown() != 0;
        body.receipt = Some(receipt);
        if unknown {
            return Err("provider reported an unknown cleanup batch outcome".into());
        }
        if failed {
            return Err("cleanup batch retained an exact failed retirement".into());
        }
    }
    let plan = body.plan.as_ref().unwrap().clone();
    let context = body.context.clone();
    step(
        &mut body,
        Step::Finalize(ConnectorCleanupFinalizeRequest { plan, context }),
    )
    .await?;
    match body.output.take().unwrap() {
        Ok(Output::Finalized) => {
            let source = body.session.as_ref().unwrap();
            if source.has_applied() {
                Ok(CleanupTerminal::KnownCommitted { locations })
            } else {
                Ok(CleanupTerminal::NoOp { locations })
            }
        }
        Ok(_) => Err("cleanup finalization output differs".into()),
        Err(error) => Err(error.to_string()),
    }
}

fn receive_original_failure(body: &mut Body) {
    if body.failure.is_some() {
        return;
    }
    let observed = body
        .session
        .as_ref()
        .and_then(|source| source.observation().original_failure());
    if let Some((failure, required)) = observed {
        body.first_error = Some(failure.clone());
        body.required_finalization_failed = required;
        body.failure = Some(SharedFailureCause::from_static_detail(
            failure,
            "cleanup original publication failed",
        ));
    } else if let Some(error) = body.first_error.as_ref() {
        body.failure = Some(SharedFailureCause::from_static_detail(
            error.clone(),
            "cleanup original provider operation failed",
        ));
    }
}
async fn original_terminal(
    original: &Arc<tokio::sync::Mutex<Body>>,
    detail: String,
) -> TerminalError {
    let mut body = original.lock().await;
    receive_original_failure(&mut body);
    let disposition = body
        .session
        .as_ref()
        .and_then(ConnectorCleanupAsyncExecution::publication_disposition);
    let applied = body
        .session
        .as_ref()
        .is_some_and(ConnectorCleanupAsyncExecution::has_applied);
    let mut failure = if body.required_finalization_failed
        && disposition == Some(LakePublicationDisposition::KnownCommitted)
    {
        TerminalError::known_committed_finalization_failed(detail)
    } else {
        match disposition {
            Some(LakePublicationDisposition::CommitUnknown) => {
                TerminalError::commit_unknown(detail)
            }
            Some(LakePublicationDisposition::KnownCommitted) => TerminalError::failed(detail),
            Some(LakePublicationDisposition::KnownUncommitted) => {
                TerminalError::known_uncommitted(detail)
            }
            None => TerminalError::pre_dispatch_failed(detail),
        }
    }
    .with_failure_cause(body.failure.clone());
    // The immutable applied bit, not KnownCommitted or Terminal, proves effect.
    if applied {
        failure = failure.with_effect(novarocks_coordination_contract::ExecutionEffect::External);
    } else if disposition != Some(LakePublicationDisposition::CommitUnknown) {
        failure = failure.with_effect(novarocks_coordination_contract::ExecutionEffect::None);
    }
    failure
}
async fn begin_cleanup(
    original: Arc<tokio::sync::Mutex<Body>>,
) -> Result<Box<dyn CleanupSession>, TerminalError> {
    match begin_cleanup_original(original.clone()).await {
        Ok(session) => Ok(session),
        Err(detail) => Err(original_terminal(&original, detail).await),
    }
}
async fn select_owned(
    original: Arc<tokio::sync::Mutex<Body>>,
    indexes: &[usize],
) -> Result<(), TerminalError> {
    match select_owned_original(original.clone(), indexes).await {
        Ok(()) => Ok(()),
        Err(detail) => Err(original_terminal(&original, detail).await),
    }
}
async fn execute_cleanup(
    original: Arc<tokio::sync::Mutex<Body>>,
) -> Result<CleanupTerminal, TerminalError> {
    match execute_cleanup_original(original.clone()).await {
        Ok(result) => Ok(result),
        Err(detail) => Err(original_terminal(&original, detail).await),
    }
}

fn produce_local(
    producer: LocalResultProducer,
    outcome: MaintenanceActionOutcome,
) -> Result<novarocks_query_application::api::OwnedLocalResult, String> {
    producer.produce(
        || match crate::table_maintenance::result::action_result(outcome)? {
            super::MaintenanceStatementResult::Query(result) => Ok(result),
            _ => Err("cleanup result author returned no rows".into()),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
    use novarocks_workload_control::{
        ResultCapacityConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
        WorkloadShutdownError,
    };

    #[test]
    fn cleanup_local_delivery_retires_without_retiring_the_same_root_data() {
        let control = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .unwrap()
            .owner;
        control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let (root, local) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Local,
            )
            .unwrap();
        let occupied = control.snapshot().result_windows.held_positions;
        assert_eq!(occupied.iter().sum::<usize>(), 1);
        let retained = QueryResultCapacityBinding::from_root_owner(&root.owner).unwrap();
        assert!(retained.is_root_backed_internal());
        assert_eq!(
            control.snapshot().result_windows.held_positions,
            occupied,
            "root-only cleanup retention occupies no Internal computation position"
        );
        let allowance = retained.window_alias().reserve_payload_bytes(16).unwrap();
        let original_bytes = vec![0u8; 16];
        let producer = LocalResultProducer::try_new(
            &root.owner.scope(),
            local.retain_alias(),
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
        )
        .unwrap();
        let result = produce_local(
            producer,
            MaintenanceActionOutcome::RemoveOrphanFiles {
                orphan_file_locations: vec!["ref:original".to_owned()],
            },
        )
        .unwrap();
        assert_eq!(result.row_count(), 1);
        assert_eq!(result.columns().len(), 1);
        drop(local);
        drop(result);
        assert_eq!(
            control.snapshot().result_windows.held_positions,
            [0; 4],
            "cleanup residual retention cannot pin the Local delivery window"
        );
        assert_eq!(retained.window_alias().retained_payload_bytes(), 16);
        root.owner.complete();
        root.business.release();
        control.close_admission();
        let stopped = match control.shutdown() {
            Err(error) => error,
            Ok(_) => panic!("original retained data must prevent root drain"),
        };
        let (reason, control) = stopped.into_parts();
        assert_eq!(reason, WorkloadShutdownError::NotDrained);
        drop(original_bytes);
        drop(allowance);
        drop(retained);
        assert!(control.shutdown().is_ok());
    }
}

#[cfg(test)]
mod original_gc_failure_tests {
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    use novarocks_state_store_api::{StateStore, StateStoreError, StateStoreErrorKind};
    use novarocks_state_store_runtime::RunFailure;
    use novarocks_state_store_testkit::testing::InMemoryStateStore;
    #[test]
    fn missing_original_capability_is_typed_without_cleanup_dispatch() {
        let original = GcOwnedRefObservationInError::Run(RunFailure::Begin(
            novarocks_state_store_api::StateStoreOriginalUnsupported.into_original_failure(),
        ));
        let terminal = gc_observation_terminal(&original);
        let cause = original.original_failure().unwrap();
        assert!(
            cause
                .downcast_ref::<novarocks_state_store_api::StateStoreOriginalUnsupported>()
                .is_some()
        );
        assert_eq!(terminal.failure_cause.as_ref(), Some(cause));
        assert_eq!(terminal.selected_failure_fact(), Some(cause.failure_fact()));
        assert_eq!(
            terminal.effect,
            Some(novarocks_coordination_contract::ExecutionEffect::None)
        );
        let service = super::super::rewrite_job::computation_service_error(terminal);
        assert_eq!(service.failure_cause(), Some(cause));
        let input = service.execution_failure().unwrap();
        assert_eq!(input.fact(), cause.failure_fact());
        assert_eq!(
            input.descriptor().descriptor.code.as_str(),
            "query.execution.unsupported"
        );
        assert!(
            service
                .message()
                .starts_with("[query.execution.unsupported]")
        );
    }
    #[test]
    fn gc_unknown_keeps_exact_observation_and_original_failure_without_deletion_authority() {
        let store = InMemoryStateStore::new("cleanup-gc-unknown-cause");
        let (attempt, observation) = store.attempts().reserve().unwrap();
        let id = observation.id();
        let cause = StateStoreError::new(
            StateStoreErrorKind::Transient,
            "actual original commit observation unresolved",
        )
        .into_original_failure();
        let original = GcOwnedRefObservationInError::Run(RunFailure::CommitUnknown {
            observation,
            error: cause.clone(),
        });
        let terminal = gc_observation_terminal(&original);
        assert_eq!(terminal.failure_cause.as_ref(), Some(&cause));
        assert_eq!(terminal.selected_failure_fact(), Some(cause.failure_fact()));
        assert_eq!(
            terminal.state,
            novarocks_table_maintenance::runtime::MaintenanceJobState::PreDispatchFailed
        );
        assert_eq!(
            terminal.effect,
            Some(novarocks_coordination_contract::ExecutionEffect::None)
        );
        let RunFailure::CommitUnknown { observation, .. } = original.run_failure().unwrap() else {
            unreachable!()
        };
        assert_eq!(observation.id(), id);
        assert_eq!(observation.id(), attempt.id());
        assert!(
            observation.peek().unwrap().is_none(),
            "carrier/projection did not invent commitment or exit"
        );
        let service = super::super::rewrite_job::computation_service_error(terminal);
        assert_eq!(service.failure_cause(), Some(&cause));
        assert_eq!(
            service.execution_failure().unwrap().fact(),
            cause.failure_fact()
        );
    }
}
