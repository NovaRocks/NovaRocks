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

//! SHOW VIEWS retains the original Root across Provider I/O and pure CPU turns.
use crate::{
    query_execution::{
        internal_result_cpu::{CreatorCpuJob, InternalResultCpu},
        kernels::ViewExecutionKernel,
        maintenance::io_admission::MetadataIoAdmission,
    },
    task_execution::{blocking_io::ConnectorBlockingIoSupervisor, status_intake::StatusIntakeWake},
};
use novarocks_failure_contract::SharedFailureCause;
use novarocks_parser::ast::ShowViews;
use novarocks_query_application::{
    admitted_query_context::RequestContext,
    api::{
        CommandContext, CommandError, CommandErrorKind, LocalResultProducer, OwnedLocalResult,
        StatementNativeHealthFailure,
    },
    cancellation::QueryCancellationView,
    execution_error::ExecutionFailureInput,
    session_control::{
        GovernedActionContext, GovernedStatementAction, GovernedStatementResidual,
        StatementActionValue,
    },
    session_error::{QueryServiceError, QueryServiceErrorKind},
    view::{PreparedShowViews, ShowViewsSource, ViewRequestContext},
};
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorError, ConnectorErrorKind, ConnectorListViewsRequest,
    ConnectorListingBound, ConnectorNamespaceIdentity, ConnectorRequestContext,
    ConnectorViewIdentity, ProviderMetadataCpuPort,
};
use novarocks_workload_control::{ResultWindowAlias, ResultWindowClass, WorkError, WorkScope};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

type IoFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
#[derive(Clone, Copy, Eq, PartialEq)]
enum Turn {
    Prepare,
    List,
    Finish,
    Done,
}
struct Input {
    kernel: ViewExecutionKernel,
    statement: ShowViews,
    request: RequestContext,
    command: CommandContext,
}
struct Body {
    input: Option<Input>,
    context: Option<ConnectorRequestContext>,
    turn: Turn,
    prepared: Option<PreparedShowViews>,
    lease: Option<ConnectorControlPlanningLease>,
    names: Option<Vec<String>>,
    raw: Option<Result<Vec<ConnectorViewIdentity>, ConnectorError>>,
    producer: Option<LocalResultProducer>,
    result: Option<Result<OwnedLocalResult, QueryServiceError>>,
}
impl Body {
    fn refuse(&mut self, error: QueryServiceError) {
        if !matches!(self.result, Some(Err(_))) {
            let retired = self.result.replace(Err(error));
            drop(retired);
        }
        self.turn = Turn::Done;
    }
}
/// Stable input and result; neither the borrowed Future nor a CPU packet owns it.
pub(crate) struct ViewShowOriginal {
    body: Mutex<Option<Body>>,
    cpu_unreturned: AtomicBool,
}
struct BodyLoan {
    source: Arc<ViewShowOriginal>,
    body: Option<Body>,
}
impl BodyLoan {
    fn take(source: &Arc<ViewShowOriginal>) -> Self {
        let body = source.body.lock().unwrap_or_else(|p| p.into_inner()).take();
        assert!(body.is_some(), "one original SHOW phase owns its body");
        Self {
            source: source.clone(),
            body,
        }
    }
}
impl Drop for BodyLoan {
    fn drop(&mut self) {
        let mut destination = self.source.body.lock().unwrap_or_else(|p| p.into_inner());
        assert!(destination.is_none(), "same SHOW body returns once");
        *destination = self.body.take();
    }
}
impl ViewShowOriginal {
    fn turn(&self) -> Turn {
        self.body
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .expect("SHOW phase exited before observation")
            .turn
    }
    pub(crate) fn run_once(self: &Arc<Self>) {
        self.cpu_unreturned.store(true, Ordering::Release);
        let mut loan = BodyLoan::take(self);
        let body = loan.body.as_mut().unwrap();
        match body.turn {
            Turn::Prepare => {
                let input = body.input.as_ref().expect("same original SHOW admission");
                let prepared = input.kernel.view_service().prepare_show_views(
                    &input.statement,
                    ViewRequestContext {
                        current_catalog: input.request.session().current_catalog(),
                        current_database: input.request.session().current_database(),
                        connector_context: body.context.as_ref(),
                    },
                );
                match prepared {
                    Err(detail) => body.refuse(contract(detail)),
                    Ok(mut prepared) => {
                        match &mut prepared.source {
                            ShowViewsSource::Local(names) => {
                                body.names = Some(std::mem::take(names));
                                body.turn = Turn::Finish;
                            }
                            ShowViewsSource::External { catalog, .. } => {
                                match crate::connector::metadata_binding_typed(
                                    input.kernel.connector_control().as_ref(),
                                    catalog,
                                ) {
                                    Ok(lease) => {
                                        body.lease = Some(lease);
                                        body.turn = Turn::List;
                                    }
                                    Err(error) => body.refuse(connector(error)),
                                }
                            }
                        }
                        body.prepared = Some(prepared);
                    }
                }
            }
            Turn::Finish => {
                let names = match body.names.take() {
                    Some(names) => names,
                    None => match body.raw.take().expect("one original Provider response") {
                        Ok(views) => views
                            .into_iter()
                            .map(|view| view.view.to_string())
                            .collect(),
                        Err(error) => {
                            body.refuse(connector(error));
                            Vec::new()
                        }
                    },
                };
                if body.turn != Turn::Done {
                    let prepared = body.prepared.take().expect("one original SHOW declaration");
                    let result = body
                        .producer
                        .take()
                        .expect("positive original Local producer")
                        .produce(|| prepared.finish(names))
                        .map_err(contract);
                    body.result = Some(result);
                    body.turn = Turn::Done;
                }
            }
            _ => panic!("SHOW CPU cannot poll Provider I/O"),
        }
        drop(loan);
        self.cpu_unreturned.store(false, Ordering::Release);
    }
    fn reject_in(self: &Arc<Self>, error: QueryServiceError) {
        let mut loan = BodyLoan::take(self);
        loan.body.as_mut().unwrap().refuse(error);
    }
    fn listing_lease(self: &Arc<Self>) -> Result<ConnectorControlPlanningLease, QueryServiceError> {
        let loan = BodyLoan::take(self);
        let body = loan.body.as_ref().unwrap();
        let lease = body
            .lease
            .as_ref()
            .ok_or_else(|| contract("SHOW listing has no frozen planning lease"))?;
        // Preserve the View capability's own Unsupported before asking for a
        // metadata CPU author. No missing capability may be a sync fallback.
        if lease.binding().view_metadata().is_none() {
            return Err(connector(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "connector control generation does not declare view metadata",
            )));
        }
        Ok(lease.clone())
    }
    fn observe_root_stop(self: &Arc<Self>, cancellation: &QueryCancellationView) {
        let mut loan = BodyLoan::take(self);
        let body = loan.body.as_mut().unwrap();
        select_root_stop(
            &mut body.result,
            matches!(body.raw, Some(Err(_))),
            cancellation,
        );
        if matches!(body.result, Some(Err(_))) {
            body.turn = Turn::Done;
        }
    }
    fn take_result(self: &Arc<Self>) -> Result<OwnedLocalResult, QueryServiceError> {
        let mut loan = BodyLoan::take(self);
        let body = loan.body.as_mut().unwrap();
        // A selected Root verdict and the later original Provider Err are
        // distinct facts. Keep the raw Err in this same residual body. An
        // undelivered successful payload exits before its Local allowance.
        if !matches!(body.raw, Some(Err(_))) {
            drop(body.raw.take());
        }
        drop(body.names.take());
        drop(body.prepared.take());
        drop(body.producer.take());
        // Actual Provider/CPU quiescence precedes delivery. Retire only the
        // completed admission aliases here so the result graph alone retains
        // its Local position; residual health still owns this same body.
        drop(body.lease.take());
        drop(body.context.take());
        drop(body.input.take());
        body.result
            .take()
            .ok_or_else(|| contract("SHOW result was already transferred"))?
    }
}
/// The same child is retained through raw receipt and opaque Drop. A borrowed
/// poll's cancellation never removes this Future from its original Action.
async fn run_io(
    source: Arc<ViewShowOriginal>,
    cancellation: QueryCancellationView,
    cpu: Arc<dyn ProviderMetadataCpuPort>,
) {
    let mut loan = BodyLoan::take(&source);
    let body = loan.body.as_mut().unwrap();
    assert!(body.turn == Turn::List, "one original SHOW listing phase");
    let lease = body.lease.as_ref().expect("one frozen Provider generation");
    let ShowViewsSource::External { namespace, .. } = &body.prepared.as_ref().unwrap().source
    else {
        panic!("local SHOW cannot dispatch Provider I/O");
    };
    let mut child = open_views(lease, namespace, body.context.as_ref().unwrap(), cpu);
    // These are disjoint fields of the SAME loan. Selecting the Root verdict
    // never takes the loan from this child or substitutes an I/O response.
    let result = &mut body.result;
    let raw_error_received = matches!(body.raw, Some(Err(_)));
    let raw = std::future::poll_fn(|cx| {
        select_root_stop(result, raw_error_received, &cancellation);
        child.as_mut().poll(cx)
    })
    .await;
    body.raw = Some(raw);
    drop(child);
    // Keep the raw receipt while projecting its typed failure. Cancellation
    // cannot replace the original Provider error received by this same Source.
    if let Some(Err(error)) = &body.raw {
        body.refuse(connector(error.clone()));
    } else {
        body.turn = Turn::Finish;
    }
}
/// Observe only this original Root, before polling a still-unreceived child.
/// An actual selected error wins over a later stop. No Provider kind or text
/// grants query cancellation, and an interim Ok is not ActionReady delivery.
fn select_root_stop(
    result: &mut Option<Result<OwnedLocalResult, QueryServiceError>>,
    raw_error_received: bool,
    cancellation: &QueryCancellationView,
) {
    if raw_error_received || matches!(result, Some(Err(_))) {
        return;
    }
    if let Some(reason) = cancellation.reason() {
        let retired = result.replace(Err(super::cancellation_error(reason)));
        drop(retired);
    }
}
/// Borrow exactly the already-selected generation. No fallback or bridge is
/// allowed when its view capability has no asynchronous listing.
pub(crate) fn open_views<'a>(
    lease: &'a ConnectorControlPlanningLease,
    namespace: &str,
    context: &ConnectorRequestContext,
    cpu: Arc<dyn ProviderMetadataCpuPort>,
) -> futures::future::BoxFuture<'a, Result<Vec<ConnectorViewIdentity>, ConnectorError>> {
    let Some(capability) = lease.binding().view_metadata() else {
        return Box::pin(async {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "connector control generation does not declare view metadata",
            ))
        });
    };
    capability.list_views_async(
        ConnectorListViewsRequest {
            namespace: ConnectorNamespaceIdentity {
                instance_id: lease.binding().descriptor().instance_id.clone(),
                namespace: Arc::from(namespace),
            },
            bound: ConnectorListingBound::V1,
            context: context.clone(),
        },
        cpu,
    )
}
#[derive(Default)]
struct Wake(futures::task::AtomicWaker);
impl std::fmt::Debug for Wake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ViewShowWake")
    }
}
impl StatusIntakeWake for Wake {
    fn wake(&self) {
        self.0.wake();
    }
}
fn contract(detail: impl Into<String>) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, detail)
}
fn work(error: WorkError) -> QueryServiceError {
    QueryServiceError::from_failure_cause(SharedFailureCause::from_static_detail(
        error,
        "SHOW original workload operation refused",
    ))
}
fn connector(error: ConnectorError) -> QueryServiceError {
    QueryServiceError::from_failure_cause(SharedFailureCause::new(error))
}
fn health(cause: Arc<dyn std::error::Error + Send + Sync>) -> StatementNativeHealthFailure {
    StatementNativeHealthFailure::actual_exit_unproven(
        "SHOW original physical exit is unproven",
        cause,
    )
}
#[derive(Debug)]
struct Unreturned;
impl std::fmt::Display for Unreturned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SHOW original poll or Drop did not return normally")
    }
}
impl std::error::Error for Unreturned {}
struct State {
    runtime: tokio::runtime::Handle,
    cancellation: QueryCancellationView,
    scope: WorkScope,
    origin: Option<ResultWindowAlias>,
    producer: Option<LocalResultProducer>,
    input: Option<Input>,
    io: Option<Arc<MetadataIoAdmission>>,
    original: Option<Arc<ViewShowOriginal>>,
    future: Option<IoFuture>,
    job: Option<CreatorCpuJob>,
    provider_cpu: Option<Arc<dyn ProviderMetadataCpuPort>>,
    marker: Option<Result<(), ExecutionFailureInput>>,
    cpu: InternalResultCpu,
    raw_health: Option<StatementNativeHealthFailure>,
    ordinary_io: Option<WorkError>,
    wake: Arc<Wake>,
    in_foreign: bool,
    selected: bool,
    closed: bool,
}
struct Action {
    state: Option<State>,
}
struct Residual {
    state: State,
}
pub(crate) fn prepare(
    kernel: ViewExecutionKernel,
    blocking: &ConnectorBlockingIoSupervisor,
    statement: ShowViews,
    request: RequestContext,
    command: CommandContext,
) -> Result<Box<dyn GovernedStatementAction>, CommandError> {
    let execution = request.execution();
    let capacity = execution.result_capacity().ok_or_else(|| {
        CommandError::new(
            CommandErrorKind::Failed,
            "SHOW has no original Local capacity",
        )
    })?;
    let origin = capacity
        .try_window_alias()
        .map_err(|e| CommandError::from_failure_cause(SharedFailureCause::new(e)))?;
    if origin.class() != ResultWindowClass::Local
        || !origin.is_for_scope(capacity.root_scope())
        || command.scope().id() != capacity.root_scope().id()
    {
        return Err(CommandError::new(
            CommandErrorKind::Failed,
            "SHOW requires its original SQL Local declaration",
        ));
    }
    let scope = capacity.root_scope().clone();
    let cancellation = execution.cancellation().clone();
    let cpu = blocking
        .provider_creator_cpu()
        .map_err(|e| CommandError::from_failure_cause(SharedFailureCause::new(e)))?;
    Ok(Box::new(Action {
        state: Some(State {
            runtime: blocking.runtime().clone(),
            cancellation,
            scope,
            origin: Some(origin),
            producer: None,
            input: Some(Input {
                kernel,
                statement,
                request,
                command,
            }),
            io: None,
            original: None,
            future: None,
            job: None,
            provider_cpu: None,
            marker: None,
            cpu,
            raw_health: None,
            ordinary_io: None,
            wake: Arc::new(Wake::default()),
            in_foreign: false,
            selected: false,
            closed: false,
        }),
    }))
}
impl State {
    fn local_health(&self) -> Option<StatementNativeHealthFailure> {
        self.raw_health
            .clone()
            .or_else(|| {
                self.job
                    .as_ref()
                    .and_then(CreatorCpuJob::local_health_failure)
            })
            .or_else(|| {
                self.provider_cpu
                    .as_ref()
                    .and_then(|port| port.local_failure())
                    .map(health)
            })
            .or_else(|| self.io.as_ref().and_then(|io| io.health_failure()))
            .or_else(|| {
                (self.in_foreign
                    || (self.job.is_none()
                        && self
                            .original
                            .as_ref()
                            .is_some_and(|source| source.cpu_unreturned.load(Ordering::Acquire))))
                .then(|| health(Arc::new(Unreturned)))
            })
    }
    fn progress(&mut self, cx: &mut Context<'_>) {
        if let Some(io) = &self.io
            && let Err(error) = io.poll_continuing(cx)
        {
            self.ordinary_io.get_or_insert(error);
        }
    }
    fn listing_cpu(
        &mut self,
        original: &Arc<ViewShowOriginal>,
    ) -> Result<Arc<dyn ProviderMetadataCpuPort>, QueryServiceError> {
        if self.provider_cpu.is_none() {
            let lease = original.listing_lease()?;
            let capacity = self.io.as_ref().unwrap().root_capacity().map_err(work)?;
            let window = capacity.try_window_alias().map_err(work)?;
            self.provider_cpu = Some(
                self.cpu
                    .planning_metadata_cpu_port(
                        &self.scope,
                        window,
                        &lease,
                        self.cancellation.clone(),
                        self.runtime.clone(),
                        self.wake.clone(),
                    )
                    .map_err(work)?,
            );
        }
        Ok(self.provider_cpu.as_ref().unwrap().clone())
    }
    fn poll_provider_closed(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if let Some(original) = &self.raw_health {
            return Poll::Ready(Err(original.clone()));
        }
        let Some(port) = self.provider_cpu.as_ref() else {
            return Poll::Ready(Ok(()));
        };
        self.in_foreign = true;
        let observed = match port.poll_closed(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(error)) => {
                // poll_closed may only return its safe port_unproven
                // projection. Keep the SAME worker/retirement cause first.
                let failure = if let Some(cause) = port.local_failure() {
                    let retained = self.raw_health.get_or_insert_with(|| health(cause)).clone();
                    drop(error);
                    retained
                } else {
                    self.raw_health
                        .get_or_insert_with(|| health(Arc::new(error)))
                        .clone()
                };
                Poll::Ready(Err(failure))
            }
        };
        self.in_foreign = false;
        observed
    }
    fn poll_io(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let Some(future) = self.future.as_mut() else {
            return Poll::Ready(());
        };
        self.in_foreign = true;
        if future.as_mut().poll(cx).is_pending() {
            self.in_foreign = false;
            return Poll::Pending;
        }
        // The original raw value was received before this opaque child Drop.
        drop(self.future.take());
        self.in_foreign = false;
        Poll::Ready(())
    }
    fn poll_cpu(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let Some(job) = self.job.as_mut() else {
            return Poll::Ready(());
        };
        self.in_foreign = true;
        let polled = job.poll_finish_in(cx, &mut self.marker);
        self.in_foreign = false;
        match polled {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => {
                self.raw_health = Some(health(error));
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
        }
        if let Some(Err(error)) = self.marker.take() {
            self.original
                .as_ref()
                .unwrap()
                .reject_in(QueryServiceError::from_execution_failure(error));
        }
        self.in_foreign = true;
        drop(self.job.take());
        self.in_foreign = false;
        self.marker = None;
        Poll::Ready(())
    }
    fn initialize(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        if self.original.is_some() {
            return Poll::Ready(Ok(()));
        }
        if !self
            .origin
            .as_ref()
            .is_some_and(|origin| origin.is_for_scope(owner.scope()))
        {
            return Poll::Ready(Err(contract("SHOW borrowed a foreign original Root")));
        }
        if self.producer.is_none() {
            self.producer = Some(
                LocalResultProducer::try_new(
                    owner.scope(),
                    owner
                        .result_window_alias()
                        .ok_or_else(|| contract("SHOW Local delivery is absent"))?,
                    *self
                        .input
                        .as_ref()
                        .expect("same SHOW input")
                        .request
                        .session()
                        .presentation(),
                )
                .map_err(contract)?,
            );
        }
        let work_owner = owner
            .execution_owner_slot()
            .as_ref()
            .ok_or_else(|| work(WorkError::NotReady))?;
        if self.io.is_none() {
            // The first phase is only local SHOW preparation. A Provider adds
            // its own exact I/O request; no guessed external route is declared.
            self.io = Some(
                MetadataIoAdmission::for_original_source(work_owner, Vec::new()).map_err(work)?,
            );
        }
        if !self
            .io
            .as_ref()
            .unwrap()
            .poll_progress(work_owner, cx)
            .map_err(work)?
        {
            return Poll::Pending;
        }
        self.io.as_ref().unwrap().root_capacity().map_err(work)?;
        let input = self.input.take().expect("one original SHOW input");
        let context = input
            .command
            .connector_context()
            .clone()
            .with_io_admission(self.io.as_ref().unwrap().clone());
        self.original = Some(Arc::new(ViewShowOriginal {
            body: Mutex::new(Some(Body {
                input: Some(input),
                context: Some(context),
                turn: Turn::Prepare,
                prepared: None,
                lease: None,
                names: None,
                raw: None,
                producer: self.producer.take(),
                result: None,
            })),
            cpu_unreturned: AtomicBool::new(false),
        }));
        Poll::Ready(Ok(()))
    }
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        if self.selected {
            return Poll::Ready(Err(contract("SHOW result was already selected")));
        }
        self.wake.0.register(cx.waker());
        std::task::ready!(self.initialize(owner, cx))?;
        self.poll_prepared(cx)
    }
    fn poll_prepared(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        loop {
            self.progress(cx);
            let polled = if self.future.is_some() {
                self.poll_io(cx)
            } else {
                self.poll_cpu(cx)
            };
            self.progress(cx);
            if polled.is_pending() || self.local_health().is_some() {
                return Poll::Pending;
            }
            let original = self.original.as_ref().unwrap().clone();
            // IO and CPU have returned their original loans before this
            // observation. An ActionReady result is never revisited here.
            self.in_foreign = true;
            original.observe_root_stop(&self.cancellation);
            self.in_foreign = false;
            match original.turn() {
                Turn::Done => break,
                Turn::List => {
                    self.in_foreign = true;
                    let admitted = self.listing_cpu(&original);
                    self.in_foreign = false;
                    match admitted {
                        Ok(cpu) => {
                            self.future =
                                Some(Box::pin(run_io(original, self.cancellation.clone(), cpu)))
                        }
                        Err(error) => original.reject_in(error),
                    }
                }
                Turn::Prepare | Turn::Finish => {
                    let admission = (|| {
                        let capacity = self.io.as_ref().unwrap().root_capacity().map_err(work)?;
                        let source = self
                            .cpu
                            .provider_creator_source(&self.scope)
                            .map_err(work)?;
                        Ok::<_, QueryServiceError>((
                            capacity.try_window_alias().map_err(work)?,
                            source,
                        ))
                    })();
                    match admission {
                        Ok((window, source)) => {
                            self.job = Some(self.cpu.submit_view_show(
                                original,
                                window,
                                source,
                                self.cancellation.clone(),
                                &self.runtime,
                                self.wake.clone(),
                            ))
                        }
                        Err(error) => original.reject_in(error),
                    }
                }
            }
        }
        // A Provider child may have returned or dropped while its original
        // CPU packet still owns input. Close the exact port before waiting on
        // MIO, otherwise that input can retain the very payload MIO awaits.
        match std::task::ready!(self.poll_provider_closed(cx)) {
            Ok(()) => {}
            Err(error) => {
                self.raw_health.get_or_insert(error);
                return Poll::Pending;
            }
        }
        match std::task::ready!(
            self.io
                .as_ref()
                .unwrap()
                .poll_actual_quiescent_continuing(cx)
        ) {
            Ok(observed) => {
                if let Some(error) = observed.into_ordinary_failure() {
                    self.ordinary_io.get_or_insert(error);
                }
            }
            Err(error) => {
                self.raw_health = Some(error);
                return Poll::Pending;
            }
        }
        let original = self.original.as_ref().unwrap();
        if let Some(error) = self.ordinary_io.take() {
            original.reject_in(work(error));
        }
        self.selected = true;
        self.in_foreign = true;
        drop(self.provider_cpu.take());
        let result = original.take_result().map(StatementActionValue::Local);
        self.in_foreign = false;
        Poll::Ready(result)
    }
    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if self.closed {
            return Poll::Ready(Ok(()));
        }
        self.progress(cx);
        let io_done = self.poll_io(cx).is_ready();
        let cpu_done = self.poll_cpu(cx).is_ready();
        self.progress(cx);
        if let Some(error) = self.local_health() {
            return Poll::Ready(Err(error));
        }
        if !io_done || !cpu_done {
            return Poll::Pending;
        }
        std::task::ready!(self.poll_provider_closed(cx))?;
        if let Some(io) = &self.io
            && io
                .withdraw_unstarted_in()
                .map_err(|e| health(Arc::new(e)))?
                .is_none()
        {
            let _ = io.poll_continuing(cx);
            std::task::ready!(io.poll_actual_quiescent_continuing(cx))?;
        }
        self.closed = true;
        Poll::Ready(Ok(()))
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
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.state.as_ref().and_then(State::local_health)
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if target.is_some() {
            return Err(health(Arc::new(WorkError::Conflict)));
        }
        if let Some(mut state) = self.state.take() {
            let delivery = state.origin.take();
            let unused_producer = state.producer.take();
            *target = Some(Box::new(Residual { state }));
            drop(delivery);
            drop(unused_producer);
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

#[cfg(test)]
#[path = "view_read_action/tests.rs"]
pub(crate) mod tests;
