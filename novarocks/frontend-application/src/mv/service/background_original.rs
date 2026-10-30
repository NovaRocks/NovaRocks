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

//! Original fixed background custody; observers borrow, never own the Root.
use super::*;
use crate::mv::domain::staged_create::{CurrentMetadataCapture, CurrentProjectionMetadataCpu};
use crate::query::mv_action::MvExecutionServices;
use crate::query_execution::maintenance::io_admission::MetadataIoAdmission;
use crate::state_store::execution::StateStoreOriginalOwner;
use novarocks_query_application::{
    admitted_query_context::{QueryExecutionContext, QueryResultCapacityBinding},
    api::{
        LogicalExecutionExitObserver, OriginalBackgroundActionInput, StatementNativeHealthFailure,
    },
    cancellation::QueryCancellationView,
    session_control::{
        BackgroundActionReady, BackgroundActionReceipt, BackgroundActionResultView,
        GovernedActionContext, GovernedBackgroundAction, GovernedStatementAction,
        GovernedStatementResidual, StatementActionFailure, StatementActionValue,
    },
    session_error::{QueryServiceError, QueryServiceErrorKind},
};
use std::{
    future::Future,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

/// Original process leaves. Operations own no Service or background Runtime.
#[derive(Clone)]
pub(super) struct OriginalWorkerDependencies {
    readiness: Arc<MvReadinessPort>,
    activation: Arc<dyn crate::query_execution::mv_native_write::MvRefreshProviderActivation>,
    ports: crate::mv::domain::iceberg_refresh::IcebergMvCorePorts,
    execution: MvExecutionServices,
    topology: BackendTopologyService,
    role: novarocks_types::ClusterRole,
    product_service: Arc<novarocks_mv_application::service::MvProductOperations>,
    root_admission: RootAdmissionHandle,
    optimizer_query_mem_limit_bytes: u64,
    attempt_timeout: Duration,
    runtime: tokio::runtime::Handle,
}
impl From<&RefreshWorkerDependencies> for OriginalWorkerDependencies {
    fn from(original: &RefreshWorkerDependencies) -> Self {
        Self {
            readiness: original.readiness.clone(),
            activation: original.provider_activation.clone(),
            ports: original.ports.clone(),
            execution: original.execution.clone(),
            topology: original.topology.clone(),
            role: original.role,
            product_service: original.product_service.original_operations(),
            root_admission: original.root_admission.clone(),
            optimizer_query_mem_limit_bytes: original.optimizer_query_mem_limit_bytes,
            attempt_timeout: original.attempt_timeout,
            runtime: original.runtime.clone(),
        }
    }
}

pub(super) fn work_error(error: novarocks_workload_control::WorkError) -> QueryServiceError {
    QueryServiceError::from_failure_cause(novarocks_failure_contract::SharedFailureCause::new(
        error,
    ))
}
pub(super) fn protocol(message: &'static str) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, message)
}
#[derive(Debug, Default)]
struct BackgroundWake(futures::task::AtomicWaker);
impl crate::task_execution::status_intake::StatusIntakeWake for BackgroundWake {
    fn wake(&self) {
        self.0.wake();
    }
}

/// An existing cadence is real management work, with no query permit or P.
/// This value is a named field of the original fixed worker before I/O.
struct BackgroundManagementPass {
    io: Option<Arc<MetadataIoAdmission>>,
    store: Option<StateStoreOriginalOwner>,
    requests: Option<Arc<dyn novarocks_state_store_api::StateStoreExecutionRequests>>,
    context: Option<ConnectorRequestContext>,
    metadata_cpu: Option<Arc<CurrentProjectionMetadataCpu>>,
    capture: Arc<CurrentMetadataCapture>,
    execution: Option<QueryExecutionContext>,
    wake: Arc<BackgroundWake>,
    stopped: std::sync::atomic::AtomicBool,
    initialized: bool,
    window: Option<novarocks_workload_control::ResultWindowAlias>,
    root: Option<novarocks_workload_control::RootWork>,
}
impl BackgroundManagementPass {
    fn new(admission: &RootAdmissionHandle) -> Result<Self, QueryServiceError> {
        // No child F or physical Source is constructed here. The caller puts
        // this Root in its original slot before polling initialization.
        let mut original = Self {
            io: None,
            store: None,
            requests: None,
            context: None,
            metadata_cpu: None,
            capture: CurrentMetadataCapture::new(),
            execution: None,
            wake: Arc::new(BackgroundWake::default()),
            stopped: std::sync::atomic::AtomicBool::new(false),
            initialized: false,
            window: None,
            root: None,
        };
        admission
            .try_begin_root_in(WorkRequest::new(WorkClass::Management), &mut original.root)
            .map_err(work_error)?;
        Ok(original)
    }
    fn poll_initialize(
        &mut self,
        dependencies: &OriginalWorkerDependencies,
        stop: &MvBackgroundStop,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        if !self.initialized {
            let binding = dependencies
                .execution
                .state_store
                .as_ref()
                .ok_or_else(|| protocol("MV cadence has no selected StateStore instance"))?;
            let owner = &self
                .root
                .as_ref()
                .expect("same original management Root")
                .owner;
            let capacity =
                QueryResultCapacityBinding::from_root_owner(owner).map_err(work_error)?;
            let scope = owner.scope();
            let cancel =
                QueryCancellationView::governed(scope.cancellation().map_err(work_error)?, None);
            let deadline = Instant::now()
                .checked_add(dependencies.attempt_timeout)
                .ok_or_else(|| {
                    work_error(novarocks_workload_control::WorkError::ArithmeticOverflow)
                })?;
            let topology = dependencies.topology.snapshot().map_err(|e| {
                QueryServiceError::new(QueryServiceErrorKind::Unavailable, e.to_string())
            })?;
            let request = RequestContext::admit(RequestAdmission::new(
                None,
                String::new(),
                dependencies.role,
                topology,
                Some(deadline),
                cancel.clone(),
                SessionOptimizerSettings {
                    optimizer_query_mem_limit_bytes: Some(
                        dependencies.optimizer_query_mem_limit_bytes as f64,
                    ),
                    ..Default::default()
                },
                Default::default(),
                novarocks_result_contract::ClientPresentationFamily::MysqlText,
            ))
            .with_result_capacity(capacity.clone())
            .map_err(|e| QueryServiceError::new(QueryServiceErrorKind::Internal, e.to_string()))?;
            self.execution = Some(request.execution().clone());
            self.io = Some(
                MetadataIoAdmission::for_original_source(
                    owner,
                    binding.blocking_dependency_facts().to_vec(),
                )
                .map_err(work_error)?,
            );
            self.store = Some(
                binding
                    .for_root(scope.clone())
                    .map_err(QueryServiceError::from_failure_cause)?,
            );
            self.requests = Some(Arc::new(
                self.store
                    .as_ref()
                    .unwrap()
                    .execution_requests(binding.transaction_deadline, Some(deadline))
                    .map_err(QueryServiceError::from_failure_cause)?,
            ));
            self.context = Some(
                crate::connector::connector_request_context_for_execution_on_runtime(
                    None,
                    self.execution.as_ref().unwrap(),
                    &dependencies.runtime,
                )
                .map_err(|e| QueryServiceError::new(QueryServiceErrorKind::Internal, e))?
                .with_initiation(novarocks_spi::connector::ConnectorRequestInitiation::Background)
                .with_io_admission(self.io.as_ref().unwrap().clone()),
            );
            self.window = Some(capacity.try_window_alias().map_err(work_error)?);
            let cpu = dependencies
                .execution
                .blocking
                .provider_creator_cpu()
                .map_err(work_error)?;
            self.metadata_cpu = Some(Arc::new(CurrentProjectionMetadataCpu::business(
                cpu,
                scope,
                capacity.try_window_alias().map_err(work_error)?,
                cancel,
                dependencies.runtime.clone(),
                self.wake.clone(),
            )));
            self.initialized = true;
        }
        match self.poll_progress(stop, cx) {
            Ok(true) => Poll::Ready(Ok(())),
            Ok(false) => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
        }
    }
    fn poll_progress(
        &self,
        stop: &MvBackgroundStop,
        cx: &mut Context<'_>,
    ) -> Result<bool, QueryServiceError> {
        self.wake.0.register(cx.waker());
        if stop.is_requested() && !self.stopped.swap(true, std::sync::atomic::Ordering::AcqRel) {
            self.root
                .as_ref()
                .unwrap()
                .owner
                .cancel(novarocks_workload_control::CancellationReason::ServerShutdown);
        }
        if let Some(store) = &self.store {
            store
                .poll_progress(cx)
                .map_err(QueryServiceError::from_failure_cause)?;
        }
        self.io
            .as_ref()
            .ok_or_else(|| protocol("MV cadence MIO has not initialized"))?
            .poll_progress(&self.root.as_ref().unwrap().owner, cx)
            .map_err(work_error)
    }
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.capture
            .local_health_failure()
            .or_else(|| self.io.as_ref().and_then(|io| io.health_failure()))
            .or_else(|| {
                self.store
                    .as_ref()
                    .and_then(|store| store.local_health_failure())
                    .map(|cause| {
                        StatementNativeHealthFailure::actual_exit_unproven(
                            "MV cadence original StateStore exit is unproven",
                            Arc::new(cause),
                        )
                    })
            })
    }
    async fn close(&mut self) -> Result<(), StatementNativeHealthFailure> {
        std::future::poll_fn(|cx| {
            if let Some(io) = &self.io {
                let _ = io.poll_continuing(cx);
            }
            if let Some(store) = &self.store
                && let Err(cause) = store.poll_progress(cx)
            {
                return Poll::Ready(Err(StatementNativeHealthFailure::actual_exit_unproven(
                    "MV cadence StateStore progress is unproven",
                    Arc::new(cause),
                )));
            }
            let capture = self.capture.poll_closed(cx);
            if let Some(health) = self.local_health_failure() {
                return Poll::Ready(Err(health));
            }
            if capture.is_pending() {
                return Poll::Pending;
            }
            if let Some(store) = self.store.as_mut() {
                std::task::ready!(store.poll_closed(cx)).map_err(|cause| {
                    StatementNativeHealthFailure::actual_exit_unproven(
                        "MV cadence StateStore exit is unproven",
                        Arc::new(cause),
                    )
                })?;
            }
            if let Some(io) = &self.io {
                match io.withdraw_unstarted_in() {
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => {
                        std::task::ready!(io.poll_actual_quiescent_continuing(cx))?;
                    }
                }
            }
            Poll::Ready(Ok(()))
        })
        .await?;
        let root = self
            .root
            .take()
            .expect("one original management Root completion");
        drop(root.business);
        root.owner.complete_after_terminal_cancel_settled();
        Ok(())
    }
}

/// Thin typed adapter to the existing QA host, with no independent poll/Drop
/// or residual kernel. MV commands have only the unit business result.
struct BackgroundMvAction(Box<dyn GovernedStatementAction>);
impl GovernedBackgroundAction for BackgroundMvAction {
    fn external_effect_disposition(
        &self,
    ) -> Option<novarocks_query_application::api::StatementExternalEffectDisposition> {
        self.0.external_effect_disposition()
    }
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.0.local_health_failure()
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        self.0.handoff_residual_in(target)
    }
    fn poll_action(
        &mut self,
        context: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        self.0.poll_action(context, cx).map(|result| {
            result.and_then(|value| match value {
                StatementActionValue::Ok(_) => Ok(()),
                _ => Err(protocol("MV background command returned a row consumer")),
            })
        })
    }
}

/// The original worker installs this before admission, action preparation,
/// start or notification. Business Ready and the actual exit observer differ.
pub(super) struct ScheduledCall {
    pub(super) request: ScheduledRefreshRequest,
    owner: Option<WorkOwner>,
    scope: novarocks_workload_control::WorkScope,
    cancel: novarocks_workload_control::WorkCancellationRequester,
    admission: Option<novarocks_workload_control::QueryAdmission>,
    permit: Option<QueryConcurrencyPermit>,
    context: Option<QueryExecutionContext>,
    connector: Option<ConnectorRequestContext>,
    action: Option<Box<dyn GovernedBackgroundAction>>,
    input: Option<OriginalBackgroundActionInput>,
    ready: Option<BackgroundActionReady>,
    receipt: Option<BackgroundActionReceipt>,
    pub(super) exit: Option<LogicalExecutionExitObserver>,
    completion: crate::query::mv_action::MvCommandCompletionReceiving,
    pub(super) result: Option<Result<bool, QueryServiceError>>,
    health: Option<Arc<StatementActionFailure>>,
    initialized: bool,
    started: bool,
    stopped: bool,
    policy_started: bool,
    #[cfg(novarocks_dev_controls)]
    outcome: Option<crate::mv::test_controls::SchedulerOutcomeRequest>,
}
impl ScheduledCall {
    pub(super) fn new(
        dependencies: &OriginalWorkerDependencies,
        request: ScheduledRefreshRequest,
    ) -> Result<Self, QueryServiceError> {
        let root = dependencies
            .root_admission
            .begin_warehouse_root(WorkRequest::new(WorkClass::MaterializedView))
            .map_err(work_error)?;
        let scope = root.owner.scope();
        let cancel = root.owner.cancellation_requester();
        Ok(Self {
            request,
            owner: Some(root.owner),
            scope,
            cancel,
            admission: None,
            permit: None,
            context: None,
            connector: None,
            action: None,
            input: None,
            ready: None,
            receipt: None,
            exit: None,
            completion: Default::default(),
            result: None,
            health: None,
            initialized: false,
            started: false,
            stopped: false,
            policy_started: false,
            #[cfg(novarocks_dev_controls)]
            outcome: None,
        })
    }
    pub(super) fn poll_business(
        &mut self,
        dependencies: &OriginalWorkerDependencies,
        product: &Arc<novarocks_mv_application::service::MvProductOperations>,
        stop: &MvBackgroundStop,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        if self.result.is_some() || self.health.is_some() {
            return Poll::Ready(());
        }
        if stop.is_requested() && !self.stopped {
            self.stopped = true;
            let _ = self
                .cancel
                .request(novarocks_workload_control::CancellationReason::ServerShutdown);
        }
        match self.poll_original(dependencies, product, cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(()),
            Poll::Ready(Err(error)) => {
                self.result = Some(Err(error));
                Poll::Ready(())
            }
        }
    }
    fn poll_original(
        &mut self,
        dependencies: &OriginalWorkerDependencies,
        product: &Arc<novarocks_mv_application::service::MvProductOperations>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        if self.permit.is_none() && !self.started {
            if self.admission.is_none() {
                self.admission = Some(self.scope.admit_query().map_err(work_error)?);
            }
            self.permit = Some(
                std::task::ready!(Pin::new(self.admission.as_mut().unwrap()).poll(cx))
                    .map_err(work_error)?,
            );
            drop(self.admission.take());
        }
        if !self.policy_started {
            if !scheduler::mark_started(product, self.request.projection().mv_id) {
                return Poll::Ready(Ok(()));
            }
            self.policy_started = true;
            #[cfg(novarocks_dev_controls)]
            if let Some(controls) = dependencies.readiness.test_controls() {
                self.outcome = match controls.scheduler_request_admitted(
                    &self.request.target,
                    self.request.projection().mv_id,
                    self.request.projection().facts.source_revision(),
                ) {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        tracing::warn!(%error,"finite MV scheduler admission observation failed");
                        None
                    }
                };
                if let Some(catalog) = self.request.target.catalog.as_deref()
                    && controls
                        .transient_preparation(
                            catalog,
                            &self.request.target.database,
                            &self.request.target.name,
                        )
                        .map_err(|error| {
                            QueryServiceError::new(QueryServiceErrorKind::Unavailable, error)
                        })?
                {
                    return Poll::Ready(Err(QueryServiceError::new(
                        QueryServiceErrorKind::Unavailable,
                        format!(
                            "test-injected temporary connector unavailability while preparing {}.{}.{}",
                            catalog, self.request.target.database, self.request.target.name
                        ),
                    )));
                }
            }
        }
        if !self.initialized {
            let topology = dependencies.topology.snapshot().map_err(|e| {
                QueryServiceError::new(QueryServiceErrorKind::Unavailable, e.to_string())
            })?;
            if topology.targets().is_empty() {
                return Poll::Ready(Err(QueryServiceError::new(
                    QueryServiceErrorKind::Unavailable,
                    "scheduler refresh is waiting for a non-empty admitted backend topology",
                )));
            }
            let deadline = Instant::now()
                .checked_add(dependencies.attempt_timeout)
                .ok_or_else(|| {
                    work_error(novarocks_workload_control::WorkError::ArithmeticOverflow)
                })?;
            let capacity =
                QueryResultCapacityBinding::from_root_owner(self.owner.as_ref().unwrap())
                    .map_err(work_error)?;
            let cancel = QueryCancellationView::governed(
                self.scope.cancellation().map_err(work_error)?,
                None,
            );
            let request = RequestContext::admit(RequestAdmission::new(
                self.request.target.catalog.clone(),
                self.request.target.database.clone(),
                dependencies.role,
                topology,
                Some(deadline),
                cancel,
                SessionOptimizerSettings {
                    optimizer_query_mem_limit_bytes: Some(
                        dependencies.optimizer_query_mem_limit_bytes as f64,
                    ),
                    ..Default::default()
                },
                Default::default(),
                novarocks_result_contract::ClientPresentationFamily::MysqlText,
            ))
            .with_result_capacity(capacity)
            .map_err(|e| QueryServiceError::new(QueryServiceErrorKind::Internal, e.to_string()))?;
            self.context = Some(request.execution().clone());
            self.connector = Some(
                crate::connector::connector_request_context_for_execution_on_runtime(
                    None,
                    self.context.as_ref().unwrap(),
                    &dependencies.runtime,
                )
                .map_err(|e| QueryServiceError::new(QueryServiceErrorKind::Internal, e))?
                .with_initiation(novarocks_spi::connector::ConnectorRequestInitiation::Background),
            );
            let statement = crate::mv::domain::application::MvRefreshRequest {
                name_parts: vec![self.request.target.name.clone()],
                full: false,
            };
            let action = crate::query::mv_action::prepare_in(
                self.request.target.catalog.clone(),
                self.request.target.database.clone(),
                dependencies.ports.clone(),
                dependencies.activation.clone(),
                product.clone(),
                dependencies.execution.clone(),
                crate::query::mv_action::MvCommandRequest::ScheduledRefresh {
                    statement,
                    requested: self.request.target.clone(),
                },
                self.context.as_ref().unwrap().clone(),
                self.connector.as_ref().unwrap().clone(),
                self.completion.clone(),
            )
            .map_err(crate::query::product_command_service_error)?;
            self.action = Some(Box::new(BackgroundMvAction(action)));
            self.initialized = true;
        }
        if !self.started {
            let mut payload = None;
            dependencies
                .execution
                .client
                .prepare_background_action_in(
                    &self.owner,
                    &mut self.context,
                    &mut self.permit,
                    &mut payload,
                    &mut self.action,
                    &mut self.input,
                )
                .map_err(start_error)?;
            let tail = dependencies
                .execution
                .client
                .start_background_action_in(
                    &mut self.input,
                    &mut self.owner,
                    &mut self.ready,
                    &mut self.exit,
                )
                .map_err(start_error)?;
            // The same fixed worker field receives exit before any wake.
            self.started = true;
            tail.finish().map_err(start_error)?;
        }
        if self.receipt.is_none() {
            match std::task::ready!(Pin::new(self.ready.as_mut().unwrap()).poll(cx)) {
                Ok(receipt) => self.receipt = Some(receipt),
                Err(failure) => {
                    self.health = Some(failure);
                    return Poll::Ready(Ok(()));
                }
            }
            drop(self.ready.take());
        }
        let receipt = self.receipt.as_ref().unwrap();
        let result = match receipt.view().result() {
            BackgroundActionResultView::Succeeded => self
                .completion
                .observed()
                .map(|c| Ok(c.performed_publication))
                .unwrap_or_else(|| {
                    Err(protocol(
                        "successful MV background Action omitted its completion fact",
                    ))
                }),
            BackgroundActionResultView::Failed(error) => Err(error.clone()),
        };
        self.result = Some(result);
        Poll::Ready(Ok(()))
    }
    pub(super) fn acknowledge(&mut self) {
        if let Some(receipt) = self.receipt.take() {
            receipt.finish();
        }
    }
    pub(super) fn finish_unstarted(&mut self) {
        assert!(!self.started, "actual Action Root belongs to QA");
        drop(self.admission.take());
        drop(self.input.take());
        drop(self.action.take());
        drop(self.context.take());
        drop(self.permit.take());
        self.owner
            .take()
            .expect("same unstarted background Root")
            .complete_after_terminal_cancel_settled();
    }
    pub(super) fn failure(&self) -> Option<&Arc<StatementActionFailure>> {
        self.health.as_ref()
    }
}

fn start_error(error: novarocks_query_application::api::QueryExecutionError) -> QueryServiceError {
    match error.execution_failure() {
        Some(input) => QueryServiceError::from_execution_failure(input.clone()),
        None => QueryServiceError::new(
            match error.kind() {
                novarocks_query_application::api::QueryExecutionErrorKind::Cancelled => {
                    QueryServiceErrorKind::Interrupted
                }
                novarocks_query_application::api::QueryExecutionErrorKind::DeadlineExceeded => {
                    QueryServiceErrorKind::Timeout
                }
                novarocks_query_application::api::QueryExecutionErrorKind::Rejected => {
                    QueryServiceErrorKind::Unavailable
                }
                _ => QueryServiceErrorKind::Internal,
            },
            error.message(),
        ),
    }
}

#[derive(Default)]
struct SchedulerBody {
    management: Option<BackgroundManagementPass>,
    discovered: Option<
        Result<
            Vec<ScheduledRefreshRequest>,
            novarocks_mv_application::repository::MvRepositoryError,
        >,
    >,
    queued: std::collections::VecDeque<ScheduledRefreshRequest>,
    call: Option<ScheduledCall>,
    tails: Vec<Option<LogicalExecutionExitObserver>>,
}
/// Runtime owns this same Source before any event F is constructed. The
/// operations edge cannot return to the Service or Runtime, even while a
/// command retains an Unknown/poisoned original Action.
pub(super) struct SchedulerOriginal {
    dependencies: OriginalWorkerDependencies,
    body: Mutex<Option<SchedulerBody>>,
}
struct SchedulerLoan<'a> {
    source: &'a SchedulerOriginal,
    body: Option<SchedulerBody>,
}
impl Drop for SchedulerLoan<'_> {
    fn drop(&mut self) {
        let mut target = self.source.body.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            target.is_none(),
            "same original scheduler body returns once"
        );
        *target = self.body.take();
    }
}
impl SchedulerOriginal {
    pub(super) fn new(dependencies: OriginalWorkerDependencies) -> Arc<Self> {
        Arc::new(Self {
            dependencies,
            body: Mutex::new(Some(SchedulerBody::default())),
        })
    }
    pub(super) async fn run(
        self: Arc<Self>,
        stop: MvBackgroundStop,
        wakeup: tokio::sync::mpsc::Sender<()>,
    ) {
        let body = self
            .body
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
            .expect("one serial scheduler event");
        let mut original = SchedulerLoan {
            source: &self,
            body: Some(body),
        };
        let body = original.body.as_mut().unwrap();
        let product = self.dependencies.product_service.clone();
        if !stop.is_requested() {
            assert!(
                body.management.is_none() && body.call.is_none(),
                "previous scheduler event actually closed"
            );
            match BackgroundManagementPass::new(&self.dependencies.root_admission) {
                Ok(pass) => body.management = Some(pass),
                Err(error) => {
                    tracing::warn!(%error,"MV scheduler management admission failed");
                    return;
                }
            }
            let ready = std::future::poll_fn(|cx| {
                body.management
                    .as_mut()
                    .unwrap()
                    .poll_initialize(&self.dependencies, &stop, cx)
            })
            .await;
            let pass = body.management.as_ref().unwrap();
            if ready.is_ok() {
                let readiness = self
                    .dependencies
                    .readiness
                    .in_requests(pass.requests.as_ref().unwrap().clone());
                let mut discovery = Box::pin(scheduler::poll_async(
                    &product,
                    &readiness,
                    &self.dependencies.ports,
                    pass.context.as_ref().unwrap(),
                    pass.metadata_cpu.as_ref().unwrap(),
                    &pass.capture,
                    now_unix_millis(),
                ));
                std::future::poll_fn(|cx| {
                    // Stop is observed through the original Root; it never
                    // destroys a transaction/capture receiving owner.
                    if let Err(error) = pass.poll_progress(&stop, cx) {
                        // The original request records its own typed failure.
                        // Continue its same F until it actually returns.
                        tracing::debug!(%error,"MV scheduler original cadence stopped");
                    }
                    if let Some(health) = pass.local_health_failure() {
                        std::panic::panic_any(health);
                    }
                    match discovery.as_mut().poll(cx) {
                        Poll::Pending => Poll::Pending,
                        Poll::Ready(raw) => {
                            body.discovered = Some(raw);
                            Poll::Ready(())
                        }
                    }
                })
                .await;
                drop(discovery);
            } else if let Err(error) = ready {
                tracing::warn!(%error,"MV scheduler management preparation failed");
            }
            if let Err(health) = body.management.as_mut().unwrap().close().await {
                std::panic::panic_any(health);
            }
            drop(body.management.take());
            match body.discovered.take() {
                Some(Ok(requests)) => body.queued.extend(requests),
                Some(Err(error)) => tracing::warn!(%error,"frontend MV scheduler inventory failed"),
                None => {}
            }
        }
        while let Some(request) = body.queued.pop_front() {
            if stop.is_requested() {
                scheduler::requeue(&product, request);
                continue;
            }
            match ScheduledCall::new(&self.dependencies, request.clone()) {
                Ok(call) => body.call = Some(call),
                Err(_) => {
                    scheduler::requeue(&product, request);
                    break;
                }
            }
            let call = body.call.as_mut().unwrap();
            std::future::poll_fn(|cx| {
                poll_tails(&mut body.tails, cx);
                call.poll_business(&self.dependencies, &product, &stop, cx)
            })
            .await;
            if let Some(health) = call.failure() {
                std::panic::panic_any(health.clone());
            }
            if !call.policy_started {
                call.finish_unstarted();
                scheduler::requeue(&product, request);
                drop(body.call.take());
                break;
            }
            let disposition = match call
                .result
                .as_ref()
                .expect("same original scheduler business result")
            {
                Ok(true) => ScheduledRefreshDisposition::Completed,
                Ok(false) => ScheduledRefreshDisposition::NoOp,
                Err(error) => match call.completion.observed_failure() {
                    Some(domain) => application_disposition(domain.clone()),
                    None => scheduler_service_disposition(error),
                },
            };
            let completed = matches!(disposition, ScheduledRefreshDisposition::Completed);
            if let Some((disposition_kind, reason)) = scheduler_outcome_log_fields(&disposition) {
                tracing::warn!(mv_id=request.projection().mv_id,target=%request.target.display_name(),disposition_kind,reason,"frontend MV scheduler refresh did not complete");
            }
            let decision = scheduler::complete(&product, &request, disposition, now_unix_millis());
            #[cfg(novarocks_dev_controls)]
            if let Some(admitted) = call.outcome.take()
                && let Some(controls) = self.dependencies.readiness.test_controls()
                && let Err(error) = controls.scheduler_request_completed(admitted, &decision)
            {
                tracing::warn!(%error,"finite MV scheduler terminal observation failed");
            }
            #[cfg(not(novarocks_dev_controls))]
            let _ = decision;
            call.acknowledge();
            if let Some(exit) = call.exit.take() {
                body.tails.push(Some(exit));
            }
            if !call.started {
                call.finish_unstarted();
            }
            drop(body.call.take());
            if completed {
                let _ = wakeup.try_send(());
            }
        }
        if stop.is_requested() {
            // Business completion did not retire old Native/Provider tails.
            std::future::poll_fn(|cx| {
                poll_tails(&mut body.tails, cx);
                if body.tails.iter().all(Option::is_none) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
        }
    }
}
fn poll_tails(tails: &mut Vec<Option<LogicalExecutionExitObserver>>, cx: &mut Context<'_>) {
    for slot in tails.iter_mut() {
        let Some(observer) = slot.as_mut() else {
            continue;
        };
        match observer.poll_exit(cx) {
            Poll::Pending => {}
            Poll::Ready(Ok(receipt)) => {
                drop(slot.take());
                drop(receipt);
            }
            Poll::Ready(Err(error)) => std::panic::panic_any(error),
        }
    } // Completed empty slots carry no original responsibility. Keep only
    // actual observer tails; do not accumulate one empty slot per job.
    tails.retain(Option::is_some);
}

fn scheduler_service_disposition(error: &QueryServiceError) -> ScheduledRefreshDisposition {
    let failure = novarocks_mv_application::scheduler_runtime::MvRefreshFailure::new(
        error.message(),
        error.compile_control_error(),
    )
    .with_execution_failure(error.execution_failure().cloned());
    if error.external_effect_disposition().is_some_and(|d|matches!(d,novarocks_query_application::api::StatementExternalEffectDisposition::KnownCommitted|novarocks_query_application::api::StatementExternalEffectDisposition::CommitUnknown)) {
        return ScheduledRefreshDisposition::TerminalFailure(failure);
    }
    match error.kind() {
        QueryServiceErrorKind::Interrupted => {
            ScheduledRefreshDisposition::ShutdownCancelled(failure)
        }
        QueryServiceErrorKind::Unavailable | QueryServiceErrorKind::FrontendDraining => {
            ScheduledRefreshDisposition::TransientUnavailable(failure)
        }
        _ => ScheduledRefreshDisposition::InvariantViolation(failure),
    }
}

#[derive(Default)]
struct MaintenanceBody {
    management: Option<BackgroundManagementPass>,
    definitions: Option<
        Result<
            Vec<novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection>,
            novarocks_mv_application::repository::MvRepositoryError,
        >,
    >,
    facts: Option<crate::mv::domain::maintenance::stats::OriginalMaintenanceFacts>,
    facts_result: Option<
        Result<crate::mv::domain::maintenance::stats::TableMaintenanceStats, MvApplicationError>,
    >,
    startup: Option<crate::mv::domain::startup_continuation::StartupContinuationReceiving>,
    startup_report: Option<crate::mv::domain::startup_continuation::StartupContinuationReport>,
    report: Option<super::super::maintenance_worker::FrontendMaintenancePassReport>,
}
pub(super) struct MaintenanceOriginal {
    dependencies: OriginalWorkerDependencies,
    admission: novarocks_mv_application::maintenance::MaintenanceAdmission,
    isolation: Option<crate::mv::startup_isolation_file::StartupIsolationSource>,
    body: Mutex<Option<MaintenanceBody>>,
}
struct MaintenanceLoan<'a> {
    source: &'a MaintenanceOriginal,
    body: Option<MaintenanceBody>,
}
impl Drop for MaintenanceLoan<'_> {
    fn drop(&mut self) {
        let mut target = self.source.body.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            target.is_none(),
            "same original maintenance body returns once"
        );
        *target = self.body.take();
    }
}
impl MaintenanceOriginal {
    pub(super) fn new(dependencies: &RefreshWorkerDependencies) -> Arc<Self> {
        Arc::new(Self {
            dependencies: OriginalWorkerDependencies::from(dependencies),
            admission:
                super::super::maintenance_worker::unavailable_automatic_maintenance_admission(
                    &dependencies.maintenance_config,
                ),
            isolation: dependencies.startup_isolation.clone(),
            body: Mutex::new(Some(MaintenanceBody::default())),
        })
    }
    pub(super) async fn run(self: Arc<Self>, stop: MvBackgroundStop) {
        let body = self
            .body
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
            .expect("one serial maintenance event");
        let mut original = MaintenanceLoan {
            source: &self,
            body: Some(body),
        };
        let body = original.body.as_mut().unwrap();
        let product = self.dependencies.product_service.clone();
        if stop.is_requested() {
            return;
        }
        assert!(
            body.management.is_none() && body.facts.is_none() && body.startup.is_none(),
            "previous maintenance event actually exited"
        );
        body.report = Some(Default::default());
        match BackgroundManagementPass::new(&self.dependencies.root_admission) {
            Ok(pass) => body.management = Some(pass),
            Err(error) => {
                tracing::warn!(%error, "MV maintenance management admission failed");
                return;
            }
        }
        let ready = std::future::poll_fn(|cx| {
            body.management
                .as_mut()
                .unwrap()
                .poll_initialize(&self.dependencies, &stop, cx)
        })
        .await;
        let pass = body.management.as_ref().unwrap();
        if ready.is_ok() {
            let readiness = self
                .dependencies
                .readiness
                .in_requests(pass.requests.as_ref().unwrap().clone());
            let mut inventory = Box::pin(readiness.list_ready_projections());
            std::future::poll_fn(|cx| {
                let _ = pass.poll_progress(&stop, cx);
                if let Some(health) = pass.local_health_failure() {
                    std::panic::panic_any(health);
                }
                match inventory.as_mut().poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(result) => {
                        body.definitions = Some(
                            result.map(|rows| rows.into_iter().map(|row| row.projection).collect()),
                        );
                        Poll::Ready(())
                    }
                }
            })
            .await;
            drop(inventory);
            let definitions = match body.definitions.as_ref().unwrap() {
                Ok(rows) => Some(rows),
                Err(error) => {
                    tracing::warn!(%error, "frontend MV maintenance inventory failed");
                    None
                }
            };
            if let Some(definitions) = definitions {
                for definition in definitions {
                    if stop.is_requested() {
                        break;
                    }
                    let canonical_target = definition.facts.target();
                    let Some(catalog) = canonical_target.catalog() else {
                        body.report.as_mut().unwrap().skipped.push(super::super::maintenance_worker::FrontendMaintenanceSkip::MissingCanonicalTarget { mv_id: definition.mv_id });
                        continue;
                    };
                    let target = novarocks_table_maintenance::MaintenanceTarget {
                        catalog: catalog.to_owned(),
                        namespace: definition.facts.target().namespace().to_owned(),
                        table: definition.facts.target().name().to_owned(),
                    };
                    match crate::mv::domain::maintenance::stats::OriginalMaintenanceFacts::try_new(
                        self.dependencies.ports.connector_control(),
                        &target,
                        pass.context.as_ref().unwrap().clone(),
                    ) {
                        Ok(facts) => body.facts = Some(facts),
                        Err(error) => {
                            tracing::warn!(%error, "frontend MV maintenance facts could not prepare its exact generation");
                            body.report.as_mut().unwrap().skipped.push(super::super::maintenance_worker::FrontendMaintenanceSkip::FactsFailed { mv_id: definition.mv_id, kind: MvBackgroundEngineErrorKind::TransientUnavailable });
                            continue;
                        }
                    }
                    let source = body.facts.as_mut().unwrap();
                    let capture_health = source.capture_observer();
                    let mut observation = Box::pin(source.observe_in(
                        self.dependencies.ports.connector_control(),
                        self.dependencies.ports.storage_observation(),
                        pass.metadata_cpu.as_ref().unwrap(),
                        definitions,
                    ));
                    std::future::poll_fn(|cx| {
                        let _ = pass.poll_progress(&stop, cx);
                        if let Some(health) = pass.local_health_failure() {
                            std::panic::panic_any(health);
                        }
                        if let Some(health) = capture_health.local_health_failure() {
                            std::panic::panic_any(health);
                        }
                        let polled = observation.as_mut().poll(cx);
                        if let Some(health) = capture_health.local_health_failure() {
                            std::panic::panic_any(health);
                        }
                        polled
                    })
                    .await;
                    drop(observation);
                    source.take_result_in(&mut body.facts_result);
                    std::future::poll_fn(|cx| {
                        let _ = pass.poll_progress(&stop, cx);
                        source.poll_closed(cx)
                    })
                    .await;
                    if let Some(health) = source.local_health_failure() {
                        std::panic::panic_any(health);
                    }
                    // Actual source and F exited before the next target's slot.
                    drop(body.facts.take());
                    match body.facts_result.take().unwrap() {
                        Ok(stats) => {
                            // Preserve all actual facts even though the
                            // current production effect adapter is unavailable.
                            let _facts =
                                super::super::background_engine::maintenance_facts_from_stats(
                                    stats,
                                );
                            body.report.as_mut().unwrap().skipped.push(super::super::maintenance_worker::FrontendMaintenanceSkip::Admission { mv_id: definition.mv_id, admission: self.admission.clone() });
                        }
                        Err(error) => {
                            tracing::warn!(%error, "frontend MV maintenance facts failed");
                            body.report.as_mut().unwrap().skipped.push(super::super::maintenance_worker::FrontendMaintenanceSkip::FactsFailed { mv_id: definition.mv_id, kind: MvBackgroundEngineErrorKind::TransientUnavailable });
                        }
                    }
                }
            }
            if !stop.is_requested()
                && let (Some(isolation), Some(entrance), Some(continuation)) = (
                    self.isolation.as_ref(),
                    product.management_entrance(),
                    product.management_continuation(),
                )
            {
                body.startup = Some(Default::default());
                let receiving = body.startup.as_mut().unwrap();
                match receiving.prepare_file_in(
                    isolation,
                    &self.dependencies.execution.blocking,
                    &pass.root.as_ref().unwrap().owner.scope(),
                    pass.window.as_ref().unwrap(),
                ) {
                    Err(error) => {
                        tracing::warn!(%error, "MV startup continuation could not prepare its original file source")
                    }
                    Ok(()) => {
                        let mut continuation_f = Box::pin(crate::mv::domain::startup_continuation::run_startup_continuation_pass_async_in(&entrance, &continuation, &readiness, self.dependencies.ports.connector_control(), isolation, pass.context.as_ref().unwrap(), pass.metadata_cpu.as_ref().unwrap(), receiving));
                        std::future::poll_fn(|cx| {
                            let _ = pass.poll_progress(&stop, cx);
                            if let Some(health) = pass.local_health_failure() {
                                std::panic::panic_any(health);
                            }
                            match continuation_f.as_mut().poll(cx) {
                                Poll::Pending => Poll::Pending,
                                Poll::Ready(report) => {
                                    body.startup_report = Some(report);
                                    Poll::Ready(())
                                }
                            }
                        })
                        .await;
                        drop(continuation_f);
                        std::future::poll_fn(|cx| {
                            let _ = pass.poll_progress(&stop, cx);
                            receiving.poll_closed(cx)
                        })
                        .await;
                        if let Some(health) = receiving.local_health_failure() {
                            std::panic::panic_any(health);
                        }
                    }
                }
            }
        } else if let Err(error) = ready {
            tracing::warn!(%error, "MV maintenance management preparation failed");
        }
        // File/Provider children and their actual receipts retire before the
        // StateStore and MIO parent, on both normal and stopped paths.
        drop(body.startup.take());
        if let Some(report) = body.startup_report.take()
            && !report.is_silent()
        {
            tracing::info!(
                readmitted = report.readmitted,
                waiting = report.waiting,
                operator_only = report.operator_only,
                not_covered = report.not_covered,
                failed = report.failed,
                "MV startup continuation pass"
            );
        }
        if let Err(health) = body.management.as_mut().unwrap().close().await {
            std::panic::panic_any(health);
        }
        drop(body.management.take());
        drop(body.definitions.take());
        drop(body.report.take());
    }
}

#[cfg(test)]
mod management_root_tests {
    use super::*;
    use novarocks_workload_control::{
        ControlStorageConfig, ResponsibilityConfig, ResultCapacityConfig, WorkloadConfig,
        WorkloadControl,
        joint_admission::{JointProgress, JointProgressDriver},
    };
    use std::{num::NonZeroUsize, task::Waker};

    fn original_management() -> (
        WorkloadControl,
        JointProgressDriver,
        BackgroundManagementPass,
    ) {
        let control = WorkloadControl::try_new_counted(WorkloadConfig {
            root_limit: 8,
            business_limit: 8,
            scope_records_limit: 8,
            query_concurrency_limit: 1,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        let (_, driver) = control
            .configure_responsibility(ResponsibilityConfig {
                work_per_turn: NonZeroUsize::new(7).unwrap(),
            })
            .unwrap();
        control
            .configure_control_storage(ControlStorageConfig::new(8, &[]))
            .unwrap();
        control.mark_ready().unwrap();
        let pass = BackgroundManagementPass::new(&control.root_admission()).unwrap();
        (control, driver, pass)
    }
    fn drive(driver: &mut JointProgressDriver) {
        for _ in 0..100000 {
            if matches!(driver.step(), JointProgress::Idle(_)) {
                return;
            }
        }
        panic!("same original management responsibility driver did not become idle");
    }

    #[test]
    fn management_pass_retains_same_root_and_business_through_original_mio_close() {
        let (control, mut driver, mut pass) = original_management();
        let scope = pass.root.as_ref().unwrap().owner.scope();
        let snapshot = control.snapshot();
        assert_eq!(
            (
                snapshot.root_responsibilities,
                snapshot.businesses,
                snapshot.admitted_queries
            ),
            (1, 1, 0)
        );
        assert_eq!(snapshot.result_windows.held_positions, [0; 4]);
        assert_eq!(
            snapshot
                .scopes
                .iter()
                .find(|s| s.id == scope.id())
                .unwrap()
                .class,
            WorkClass::Management
        );
        let capacity =
            QueryResultCapacityBinding::from_root_owner(&pass.root.as_ref().unwrap().owner)
                .unwrap();
        assert!(capacity.is_root_backed_internal());
        assert!(capacity.native_action_authority().is_err());
        pass.window = Some(capacity.try_window_alias().unwrap());
        drop(capacity);
        let io = MetadataIoAdmission::for_original_source(
            &pass.root.as_ref().unwrap().owner,
            Vec::new(),
        )
        .unwrap();
        pass.io = Some(io);
        let mut cx = Context::from_waker(Waker::noop());
        let mut ready = false;
        for _ in 0..256 {
            ready = pass
                .io
                .as_ref()
                .unwrap()
                .poll_progress(&pass.root.as_ref().unwrap().owner, &mut cx)
                .unwrap();
            if ready {
                break;
            }
            drive(&mut driver);
        }
        assert!(
            ready,
            "actual original Management MIO bootstrap was admitted"
        );
        assert_eq!(
            pass.io
                .as_ref()
                .unwrap()
                .root_capacity()
                .unwrap()
                .scope()
                .id(),
            scope.id()
        );
        assert_eq!(control.snapshot().result_windows.held_positions, [0; 4]);
        assert_eq!(control.snapshot().admitted_queries, 0);
        assert_eq!(
            control.snapshot().businesses,
            1,
            "management business remains in its original field"
        );
        {
            let close = pass.close();
            tokio::pin!(close);
            let mut closed = false;
            for _ in 0..256 {
                drive(&mut driver);
                if let Poll::Ready(result) = close.as_mut().poll(&mut cx) {
                    assert!(result.is_ok());
                    closed = true;
                    break;
                }
            }
            assert!(
                closed,
                "same original capture/MIO close reached actual quiescence"
            );
        }
        assert!(pass.root.is_none());
        assert_eq!(control.snapshot().businesses, 0);
        drop(pass);
        drive(&mut driver);
        let snapshot = control.snapshot();
        assert_eq!(
            (
                snapshot.root_responsibilities,
                snapshot.businesses,
                snapshot.admitted_queries
            ),
            (0, 0, 0)
        );
        assert_eq!(snapshot.result_windows.held_positions, [0; 4]);
        control.close_admission();
        drive(&mut driver);
        assert!(control.shutdown().is_ok());
    }

    #[test]
    fn stopped_uninitialized_management_pass_closes_same_root_without_query_or_result_position() {
        let (control, mut driver, mut pass) = original_management();
        pass.root
            .as_ref()
            .unwrap()
            .owner
            .cancel(novarocks_workload_control::CancellationReason::ServerShutdown);
        let mut cx = Context::from_waker(Waker::noop());
        {
            let close = pass.close();
            tokio::pin!(close);
            assert!(matches!(close.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
        }
        drop(pass);
        drive(&mut driver);
        let snapshot = control.snapshot();
        assert_eq!(
            (
                snapshot.root_responsibilities,
                snapshot.businesses,
                snapshot.admitted_queries
            ),
            (0, 0, 0)
        );
        assert_eq!(snapshot.result_windows.held_positions, [0; 4]);
        control.close_admission();
        drive(&mut driver);
        assert!(control.shutdown().is_ok());
    }
}

#[cfg(test)]
mod native_scheduler_failure_tests {
    use super::*;
    use novarocks_failure_contract::FailureProjection;
    use novarocks_mv_application::{
        scheduler::MvSchedulerConfig,
        scheduler_runtime::{MvRefreshRuntimeDecision, MvRefreshSchedulerRuntime},
    };
    use novarocks_query_application::execution_error::{ExecutionFailureInput, QueryFailureReason};

    #[test]
    fn original_native_failure_reaches_scheduler_without_becoming_compile_control() {
        let unsupported = novarocks_local_compiler::FragmentCompileError::Unsupported {
            node: Some(novarocks_physical_plan::NodeId::new(2)),
            feature: "private Native capability diagnostic",
        };
        let invalid = novarocks_local_compiler::FragmentCompileError::Invalid(
            "unsupported diagnostic cannot choose the category",
        );
        for fact in [
            unsupported.failure_fact(),
            invalid.failure_fact(),
            QueryFailureReason::Cancelled.failure_fact(),
            QueryFailureReason::PreparationFailed.failure_fact(),
            QueryFailureReason::BackendLost.failure_fact(),
            QueryFailureReason::TransportCapacity.failure_fact(),
        ] {
            for coordinator in [false, true] {
                let input = if coordinator {
                    // An independent originating coordinator failure exercises
                    // its own cancellation/resource policy. Do not relabel a
                    // selected Task failure as a coordinator observation.
                    ExecutionFailureInput::new(
                        fact,
                        novarocks_execution_contract::SafeDetail::truncating(
                            "private original coordinator failure diagnostic",
                        ),
                        novarocks_query_application::execution_error::ExecutionFailureSite::Coordinator,
                    )
                } else {
                    let identity = novarocks_execution_contract::TaskIdentity::new(
                        novarocks_types::QueryExecutionId::new(
                            novarocks_types::QueryId::new(61, 21),
                            novarocks_types::AttemptId::new(1).unwrap(),
                        )
                        .unwrap(),
                        novarocks_types::StageId::new(3).unwrap(),
                        novarocks_types::TaskId::new(5).unwrap(),
                        novarocks_types::BackendProcessId::new_v7(),
                    );
                    let selected = novarocks_coordination_contract::SelectedTaskTermination::new(
                        identity,
                        novarocks_execution_contract::TerminationDetail::Failed(
                            novarocks_execution_contract::TaskFailure::new_in_phase(
                                fact,
                                novarocks_execution_contract::SafeDetail::truncating(
                                    "private original Native failure diagnostic",
                                ),
                                novarocks_execution_contract::TaskFailurePhase::Preparation,
                            ),
                        ),
                    );
                    ExecutionFailureInput::from_selected_task(&selected).unwrap()
                };
                let error = QueryServiceError::from_execution_failure(input.clone());
                assert_eq!(error.compile_control_error(), None);
                let disposition = scheduler_service_disposition(&error);
                assert_eq!(disposition.execution_failure(), Some(&input));
                assert_eq!(disposition.compile_control_error(), None);
                let cancelled = matches!(error.kind(), QueryServiceErrorKind::Interrupted);
                let transient = matches!(error.kind(), QueryServiceErrorKind::Unavailable);
                assert!(if cancelled {
                    matches!(
                        &disposition,
                        ScheduledRefreshDisposition::ShutdownCancelled(_)
                    )
                } else if transient {
                    matches!(
                        &disposition,
                        ScheduledRefreshDisposition::TransientUnavailable(_)
                    )
                } else {
                    matches!(
                        &disposition,
                        ScheduledRefreshDisposition::InvariantViolation(_)
                    )
                });
                let mut runtime = MvRefreshSchedulerRuntime::<i64, ()>::new(
                    MvSchedulerConfig::new(true, 1, 1, 10, 40),
                );
                assert!(runtime.mark_started(&7));
                let decision = runtime.complete(&7, disposition, 100);
                assert_eq!(decision.execution_failure(), Some(&input));
                assert_eq!(decision.compile_control_error(), None);
                assert!(if cancelled {
                    matches!(decision, MvRefreshRuntimeDecision::NoChange { .. })
                } else if transient {
                    matches!(decision, MvRefreshRuntimeDecision::TransientBackoff { .. })
                } else {
                    matches!(decision, MvRefreshRuntimeDecision::Blocked { .. })
                });
                assert_eq!(runtime.is_suppressed(&7, 100), !cancelled);
                for effect in [
                novarocks_query_application::api::StatementExternalEffectDisposition::KnownCommitted,
                novarocks_query_application::api::StatementExternalEffectDisposition::CommitUnknown,
            ] {
                let effected = error.clone().attach_external_effect_disposition(effect);
                let disposition = scheduler_service_disposition(&effected);
                assert!(matches!(&disposition, ScheduledRefreshDisposition::TerminalFailure(_)));
                assert_eq!(disposition.execution_failure(), Some(&input));
                assert_eq!(disposition.compile_control_error(), None);
            }
            }
        }
        let diagnostic = QueryServiceError::new(
            QueryServiceErrorKind::Internal,
            "[UnsupportedCompiledShape] diagnostic text is not an identity",
        );
        assert!(
            scheduler_service_disposition(&diagnostic)
                .execution_failure()
                .is_none()
        );
    }
}
