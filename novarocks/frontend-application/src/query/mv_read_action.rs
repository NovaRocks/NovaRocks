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

//! Read-only MV commands borrow their original Root and Local delivery.
use super::mv_action::MvExecutionServices;
use crate::mv::domain::{
    analysis_adapter::{MvShowResultRows, MvShowRowSelection},
    application::{MvApplicationError, MvShowStatement},
    iceberg_refresh::IcebergMvCorePorts,
    readiness::MvReadinessPort,
};
use crate::query_execution::{
    internal_result_cpu::{InternalResultCpu, MvCpuJob},
    maintenance::io_admission::MetadataIoAdmission,
    mv_assembly::{
        refresh_explain::MvExplainPreparationInput,
        refresh_preparation::{
            async_original::{MvRefreshPreparationOriginal, MvRefreshPreparationOutput},
            driver::{MvRefreshTurnCustody, MvTurnFailure},
            io_gate::{MvRefreshPreparationIo, MvRefreshPreparationServices},
        },
    },
};
use crate::state_store::execution::{StateStoreOriginalBinding, StateStoreOriginalOwner};
use crate::task_execution::status_intake::StatusIntakeWake;
use novarocks_failure_contract::SharedFailureCause;
use novarocks_mv_application::{
    management::ManagementEntrance,
    persistence::dependency::StoredMvDependency,
    readiness::{ListedKeyedMvProjection, MvBoundedKeyedProjectionInventory},
    repository::MvRepositoryError,
};
use novarocks_query_application::{
    admitted_query_context::QueryExecutionContext,
    api::{
        CommandError, CommandErrorKind, LocalResultProducer, OwnedLocalResult,
        StatementNativeHealthFailure, build_string_query_result,
    },
    execution_error::ExecutionFailureInput,
    session_control::{
        GovernedActionContext, GovernedStatementAction, GovernedStatementResidual,
        StatementActionValue,
    },
    session_error::{QueryServiceError, QueryServiceErrorKind},
};
use novarocks_state_store_api::StateStoreExecutionRequests;
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

/// Parser-lowered closed read requests, not refresh publication admission.
pub(crate) enum ReadRequest {
    Show {
        catalog: Option<String>,
        statement: MvShowStatement,
    },
    Explain(Box<MvExplainPreparationInput>),
}

type IoFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
#[derive(Clone, Copy, Eq, PartialEq)]
enum Turn {
    Inventory,
    Explain,
    ExplainFinish,
    Start,
    Next,
    Select,
    Dependencies,
    Append,
    Finish,
    Done,
}
impl Turn {
    fn is_io(self) -> bool {
        matches!(self, Self::Inventory | Self::Next | Self::Dependencies)
    }
}
struct Body {
    readiness: Arc<MvReadinessPort>,
    entrance: Arc<ManagementEntrance>,
    requests: Arc<dyn StateStoreExecutionRequests>,
    show: Option<(Option<String>, MvShowStatement)>,
    explain_lines: Option<Vec<String>>,
    turn: Turn,
    inventory: Option<MvBoundedKeyedProjectionInventory>,
    next: Option<Result<Option<ListedKeyedMvProjection>, MvRepositoryError>>,
    listed: Option<ListedKeyedMvProjection>,
    selection: Option<MvShowRowSelection>,
    dependencies: Option<Result<Vec<StoredMvDependency>, MvRepositoryError>>,
    rows: Option<MvShowResultRows>,
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
/// One stable input/result, independent of its borrowed F and worker packet.
/// The whole body returns on normal exit and unwind; no foreign Drop is run
/// under its receiving lock.
pub(crate) struct MvReadOriginal {
    body: Mutex<Option<Body>>,
    cpu_unreturned: AtomicBool,
}
struct BodyLoan {
    source: Arc<MvReadOriginal>,
    body: Option<Body>,
}
impl BodyLoan {
    fn take(source: &Arc<MvReadOriginal>) -> Self {
        let body = source.body.lock().unwrap_or_else(|p| p.into_inner()).take();
        assert!(body.is_some(), "one original MV read phase owns its body");
        Self {
            source: source.clone(),
            body,
        }
    }
}
impl Drop for BodyLoan {
    fn drop(&mut self) {
        let mut destination = self.source.body.lock().unwrap_or_else(|p| p.into_inner());
        assert!(destination.is_none(), "same MV read body returns once");
        *destination = self.body.take();
    }
}
impl MvReadOriginal {
    fn turn(&self) -> Turn {
        self.body
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .expect("MV read phase exited before observation")
            .turn
    }
    pub(crate) fn run_once(self: &Arc<Self>) {
        self.cpu_unreturned.store(true, Ordering::Release);
        let mut loan = BodyLoan::take(self);
        let body = loan.body.as_mut().unwrap();
        let result = match body.turn {
            Turn::Start => {
                body.inventory
                    .as_mut()
                    .expect("original inventory received")
                    .order_by_namespace_and_name();
                MvShowResultRows::new().map(|rows| {
                    body.rows = Some(rows);
                    body.turn = Turn::Next;
                })
            }
            Turn::Select => {
                if let Some(listed) = body.listed.as_ref() {
                    body.rows
                        .as_ref()
                        .unwrap()
                        .begin_row(
                            listed,
                            Some(body.entrance.as_ref()),
                            body.show.as_ref().unwrap().0.as_deref(),
                            &body.show.as_ref().unwrap().1,
                            None,
                        )
                        .map(|selection| match selection {
                            None => {
                                body.listed.take();
                                body.turn = Turn::Next;
                            }
                            Some(selection) => {
                                body.turn = if selection.read_dependencies {
                                    Turn::Dependencies
                                } else {
                                    Turn::Append
                                };
                                body.selection = Some(selection);
                            }
                        })
                } else {
                    body.turn = Turn::Finish;
                    Ok(())
                }
            }
            Turn::Append => {
                let dependencies = match body.dependencies.take() {
                    Some(Ok(value)) => value,
                    Some(Err(error)) => {
                        body.refuse(repository_error(error));
                        Vec::new()
                    }
                    None => Vec::new(),
                };
                if body.turn == Turn::Done {
                    Ok(())
                } else {
                    body.rows
                        .as_mut()
                        .unwrap()
                        .append(
                            &body.listed.as_ref().unwrap().loaded.projection,
                            body.selection.take().unwrap(),
                            &dependencies,
                        )
                        .map(|()| {
                            body.listed.take();
                            body.turn = Turn::Next;
                        })
                }
            }
            Turn::ExplainFinish => {
                let lines = body.explain_lines.take().expect("original Explain output");
                let result = body
                    .producer
                    .take()
                    .expect("positive original Local producer")
                    .produce(|| build_string_query_result("Explain String", lines))
                    .map_err(contract);
                body.result = Some(result);
                body.turn = Turn::Done;
                Ok(())
            }
            Turn::Finish => {
                let rows = body.rows.take().expect("SHOW's one row collector");
                let result = body
                    .producer
                    .take()
                    .expect("positive original Local producer")
                    .produce(|| rows.finish())
                    .map_err(contract);
                body.result = Some(result);
                body.turn = Turn::Done;
                Ok(())
            }
            _ => panic!("MV read CPU cannot poll repository I/O"),
        };
        if let Err(detail) = result {
            body.refuse(contract(detail));
        }
        drop(loan);
        self.cpu_unreturned.store(false, Ordering::Release);
    }
    fn receive_explain(self: &Arc<Self>, result: Result<Vec<String>, QueryServiceError>) {
        let mut loan = BodyLoan::take(self);
        let body = loan.body.as_mut().unwrap();
        assert!(body.turn == Turn::Explain, "one original Explain receipt");
        match result {
            Ok(lines) => {
                body.explain_lines = Some(lines);
                body.turn = Turn::ExplainFinish;
            }
            Err(error) => body.refuse(error),
        }
    }
    fn reject_in(self: &Arc<Self>, error: QueryServiceError) {
        let mut loan = BodyLoan::take(self);
        loan.body.as_mut().unwrap().refuse(error);
    }
    fn take_result(self: &Arc<Self>) -> Result<OwnedLocalResult, QueryServiceError> {
        let mut loan = BodyLoan::take(self);
        let body = loan.body.as_mut().unwrap();
        // An unsuccessful collector has no delivery graph. Retire it before
        // releasing its positive Local allowance, while the primary is stable.
        drop(body.rows.take());
        drop(body.producer.take());
        body.result
            .take()
            .ok_or_else(|| contract("MV read original result was already transferred"))?
    }
}
// Each concrete leaf receives raw output before destroying the SAME child.
// Cancellation of a borrowed poll never takes this future from its Action.
async fn run_io(source: Arc<MvReadOriginal>) {
    let mut loan = BodyLoan::take(&source);
    let body = loan.body.as_mut().unwrap();
    match body.turn {
        Turn::Inventory => {
            let execution = body.readiness.in_requests(body.requests.clone());
            let mut child = Box::pin(execution.local_projection_inventory());
            let raw = std::future::poll_fn(|cx| child.as_mut().poll(cx)).await;
            match raw {
                Ok(inventory) => {
                    body.inventory = Some(inventory);
                    body.turn = Turn::Start;
                }
                Err(error) => {
                    body.result = Some(Err(repository_error(error)));
                    body.turn = Turn::Done;
                }
            }
            drop(child);
        }
        Turn::Next => {
            let mut child = Box::pin(body.inventory.as_mut().unwrap().next_listable());
            let raw = std::future::poll_fn(|cx| child.as_mut().poll(cx)).await;
            body.next = Some(raw);
            drop(child);
            match body.next.take().unwrap() {
                Ok(listed) => {
                    body.listed = listed;
                    body.turn = Turn::Select;
                }
                Err(error) => body.refuse(repository_error(error)),
            }
        }
        Turn::Dependencies => {
            let execution = body.readiness.in_requests(body.requests.clone());
            let mut child = Box::pin(
                execution
                    .list_local_dependencies_by_downstream(&body.listed.as_ref().unwrap().loaded),
            );
            let raw = std::future::poll_fn(|cx| child.as_mut().poll(cx)).await;
            body.dependencies = Some(raw);
            drop(child);
            body.turn = Turn::Append;
        }
        _ => panic!("SHOW I/O cannot execute its pure row turn"),
    }
}
#[derive(Default)]
struct Wake(futures::task::AtomicWaker);
impl std::fmt::Debug for Wake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MvReadWake")
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
        "MV read original workload operation refused",
    ))
}
fn repository_error(error: MvRepositoryError) -> QueryServiceError {
    let original = error
        .original_failure()
        .cloned()
        .unwrap_or_else(|| SharedFailureCause::new(error));
    QueryServiceError::from_failure_cause(original)
}
fn domain_error(error: MvApplicationError) -> QueryServiceError {
    super::routed_execution_service_error(super::product_command_error(
        crate::mv::command::mv_application_command_error(error),
    ))
}
fn health(cause: Arc<dyn std::error::Error + Send + Sync>) -> StatementNativeHealthFailure {
    StatementNativeHealthFailure::actual_exit_unproven(
        "MV read original physical exit is unproven",
        cause,
    )
}
fn store_health(cause: SharedFailureCause) -> StatementNativeHealthFailure {
    health(Arc::new(cause))
}
#[derive(Debug)]
struct Unreturned;
impl std::fmt::Display for Unreturned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MV read original poll or Drop did not return normally")
    }
}
impl std::error::Error for Unreturned {}
struct Explain {
    reader: Arc<MvRefreshPreparationOriginal>,
    // Same gate and reader are retained after business-ready and through close.
    turn: MvRefreshTurnCustody,
    result: Option<Result<MvRefreshPreparationOutput, MvApplicationError>>,
    received: bool,
}
struct State {
    family: novarocks_result_contract::ClientPresentationFamily,
    readiness: Arc<MvReadinessPort>,
    entrance: Arc<ManagementEntrance>,
    binding: StateStoreOriginalBinding,
    runtime: tokio::runtime::Handle,
    cancellation: novarocks_query_application::cancellation::QueryCancellationView,
    deadline: Option<std::time::Instant>,
    scope: WorkScope,
    origin: Option<ResultWindowAlias>,
    producer: Option<LocalResultProducer>,
    input: Option<ReadRequest>,
    explain: Option<Explain>,
    io: Option<Arc<MetadataIoAdmission>>,
    store: Option<StateStoreOriginalOwner>,
    original: Option<Arc<MvReadOriginal>>,
    future: Option<IoFuture>,
    job: Option<MvCpuJob>,
    marker: Option<Result<(), ExecutionFailureInput>>,
    cpu: InternalResultCpu,
    refusal: Option<QueryServiceError>,
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
    ports: IcebergMvCorePorts,
    services: MvExecutionServices,
    request: ReadRequest,
    execution: QueryExecutionContext,
    family: novarocks_result_contract::ClientPresentationFamily,
) -> Result<Box<dyn GovernedStatementAction>, CommandError> {
    let capacity = execution.result_capacity().ok_or_else(|| {
        CommandError::new(
            CommandErrorKind::Failed,
            "MV read has no original Local capacity",
        )
    })?;
    let origin = capacity
        .try_window_alias()
        .map_err(|error| CommandError::from_failure_cause(SharedFailureCause::new(error)))?;
    if origin.class() != ResultWindowClass::Local || !origin.is_for_scope(capacity.root_scope()) {
        return Err(CommandError::new(
            CommandErrorKind::Failed,
            "MV read requires its original SQL Local declaration",
        ));
    }
    let binding = services.state_store.clone().ok_or_else(|| {
        CommandError::from_failure_cause(
            novarocks_state_store_api::StateStoreOriginalUnsupported.into_original_failure(),
        )
    })?;
    let cpu = services
        .blocking
        .provider_creator_cpu()
        .map_err(|error| CommandError::from_failure_cause(SharedFailureCause::new(error)))?;
    let entrance = ports
        .management_entrance()
        .map_err(|detail| CommandError::new(CommandErrorKind::Failed, detail))?
        .clone();
    let scope = capacity.root_scope().clone();
    Ok(Box::new(Action {
        state: Some(State {
            family,
            readiness: ports.readiness().clone(),
            entrance,
            binding,
            runtime: services.data.handle().clone(),
            cancellation: execution.cancellation().clone(),
            deadline: execution.deadline(),
            scope,
            origin: Some(origin),
            producer: None,
            input: Some(request),
            explain: None,
            io: None,
            store: None,
            original: None,
            future: None,
            job: None,
            marker: None,
            cpu,
            refusal: None,
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
            .or_else(|| self.job.as_ref().and_then(MvCpuJob::local_health_failure))
            .or_else(|| {
                self.store
                    .as_ref()
                    .and_then(StateStoreOriginalOwner::local_health_failure)
                    .map(store_health)
            })
            .or_else(|| {
                self.explain
                    .as_ref()
                    .and_then(|explain| explain.turn.local_health_failure())
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
        if let Some(store) = &self.store
            && let Err(error) = store.poll_progress(cx)
        {
            self.refusal
                .get_or_insert_with(|| QueryServiceError::from_failure_cause(error));
        }
        if let Some(io) = &self.io
            && let Err(error) = io.poll_continuing(cx)
        {
            self.ordinary_io.get_or_insert(error);
        }
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
    fn poll_explain(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let explain = self.explain.as_mut().unwrap();
        self.in_foreign = true;
        let polled = explain.turn.poll_ready(cx);
        self.in_foreign = false;
        let selected = match std::task::ready!(polled) {
            Ok(()) => {
                assert!(
                    explain
                        .turn
                        .preparation_original()
                        .take_result_in(&mut explain.result),
                    "same original Explain output"
                );
                match explain.result.as_mut().unwrap() {
                    Ok(MvRefreshPreparationOutput::Explain(lines)) => Ok(std::mem::take(lines)),
                    Err(error) => Err(domain_error(error.clone())),
                    Ok(_) => Err(contract(
                        "MV Explain reader returned another operation's output",
                    )),
                }
            }
            Err(MvTurnFailure::Admission(error)) => Err(work(error)),
            Err(MvTurnFailure::Cpu(error)) => {
                Err(QueryServiceError::from_execution_failure((*error).clone()))
            }
            Err(MvTurnFailure::Health(error)) => {
                self.raw_health = Some(error);
                return Poll::Pending;
            }
        };
        self.original.as_ref().unwrap().receive_explain(selected);
        explain.received = true;
        Poll::Ready(())
    }
    fn poll_explain_closed(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        let Some(explain) = self.explain.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        self.in_foreign = true;
        let closed = explain.turn.poll_closed(cx);
        self.in_foreign = false;
        closed
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
            return Poll::Ready(Err(contract("MV read borrowed a foreign original Root")));
        }
        if self.producer.is_none() {
            self.producer = Some(
                LocalResultProducer::try_new(
                    owner.scope(),
                    owner
                        .result_window_alias()
                        .ok_or_else(|| contract("MV read Local delivery is absent"))?,
                    self.family,
                )
                .map_err(contract)?,
            );
        }
        let work_owner = owner
            .execution_owner_slot()
            .as_ref()
            .ok_or_else(|| work(WorkError::NotReady))?;
        let binding = &self.binding;
        if self.io.is_none() {
            self.io = Some(
                MetadataIoAdmission::for_original_source(
                    work_owner,
                    binding.blocking_dependency_facts().to_vec(),
                )
                .map_err(work)?,
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
        self.install_original()?;
        Poll::Ready(Ok(()))
    }
    fn install_original(&mut self) -> Result<(), QueryServiceError> {
        self.io.as_ref().unwrap().root_capacity().map_err(work)?;
        if self.store.is_none() {
            self.store = Some(
                self.binding
                    .for_root(self.scope.clone())
                    .map_err(QueryServiceError::from_failure_cause)?,
            );
        }
        let binding = &self.binding;
        let requests: Arc<dyn StateStoreExecutionRequests> = Arc::new(
            self.store
                .as_ref()
                .unwrap()
                .execution_requests(binding.transaction_deadline, self.deadline)
                .map_err(QueryServiceError::from_failure_cause)?,
        );

        let request = self.input.take().expect("one original MV read request");
        let (show, turn) = match request {
            ReadRequest::Show { catalog, statement } => {
                (Some((catalog, statement)), Turn::Inventory)
            }
            ReadRequest::Explain(input) => {
                let mut input = *input;
                let io = self.io.as_ref().unwrap();
                let capacity = io.root_capacity().map_err(work)?;
                let window = capacity.try_window_alias().map_err(work)?;
                input.context = input.context.with_io_admission(io.clone());
                let original = io.original_result_scope(&input.context).map_err(|error| {
                    QueryServiceError::from_failure_cause(SharedFailureCause::new(error))
                })?;
                let gate = Arc::new(MvRefreshPreparationIo::new(
                    MvRefreshPreparationServices {
                        cpu: self.cpu.clone(),
                        scope: self.scope.clone(),
                        window: window.clone(),
                        cancellation: self.cancellation.clone(),
                        runtime: self.runtime.clone(),
                        wake: self.wake.clone(),
                        requests: requests.clone(),
                    },
                    original,
                ));
                let reader = MvRefreshPreparationOriginal::for_explain(input, gate.clone());
                let custody = MvRefreshTurnCustody::new(
                    gate,
                    self.cpu.clone(),
                    self.scope.clone(),
                    window,
                    self.cancellation.clone(),
                    self.runtime.clone(),
                    self.wake.clone(),
                );
                self.explain = Some(Explain {
                    reader: reader.clone(),
                    turn: custody,
                    result: None,
                    received: false,
                });
                self.explain
                    .as_mut()
                    .unwrap()
                    .turn
                    .install_preparation(reader)
                    .map_err(|returned| {
                        debug_assert!(Arc::ptr_eq(
                            &returned,
                            &self.explain.as_ref().unwrap().reader
                        ));
                        contract("MV Explain reader already installed")
                    })?;
                (None, Turn::Explain)
            }
        };
        self.original = Some(Arc::new(MvReadOriginal {
            body: Mutex::new(Some(Body {
                readiness: self.readiness.clone(),
                entrance: self.entrance.clone(),
                requests,
                show,
                explain_lines: None,
                turn,
                inventory: None,
                next: None,
                listed: None,
                selection: None,
                dependencies: None,
                rows: None,
                producer: self.producer.take(),
                result: None,
            })),
            cpu_unreturned: AtomicBool::new(false),
        }));
        Ok(())
    }

    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        if self.selected {
            return Poll::Ready(Err(contract("MV read result was already selected")));
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
            let polled = if self
                .explain
                .as_ref()
                .is_some_and(|explain| !explain.received)
            {
                self.poll_explain(cx)
            } else if self.future.is_some() {
                self.poll_io(cx)
            } else {
                self.poll_cpu(cx)
            };
            self.progress(cx);
            if polled.is_pending() {
                return Poll::Pending;
            }
            if self.local_health().is_some() {
                return Poll::Pending;
            }
            let original = self.original.as_ref().unwrap().clone();
            if original.turn() != Turn::Done {
                if let Some(error) = self.refusal.take() {
                    original.reject_in(error);
                } else if let Some(reason) = self.cancellation.reason() {
                    original.reject_in(super::cancellation_error(reason));
                }
            }
            let turn = original.turn();
            if turn == Turn::Done {
                break;
            }
            if turn.is_io() {
                self.future = Some(Box::pin(run_io(original)));
            } else {
                let admission = (|| {
                    let capacity = self.io.as_ref().unwrap().root_capacity().map_err(work)?;
                    let source = self
                        .cpu
                        .provider_creator_source(&self.scope)
                        .map_err(work)?;
                    Ok::<_, QueryServiceError>((capacity.try_window_alias().map_err(work)?, source))
                })();
                match admission {
                    Ok((window, source)) => {
                        self.job = Some(self.cpu.submit_mv_read(
                            original,
                            window,
                            source,
                            self.cancellation.clone(),
                            &self.runtime,
                            self.wake.clone(),
                        ));
                    }
                    Err(error) => original.reject_in(error),
                }
            }
        }
        match std::task::ready!(self.poll_explain_closed(cx)) {
            Ok(()) => {}
            Err(error) => {
                self.raw_health = Some(error);
                return Poll::Pending;
            }
        }
        match self.store.as_ref().unwrap().poll_actual_quiescent(cx) {
            Ok(true) => {}
            Ok(false) => return Poll::Pending,
            Err(error) => {
                self.raw_health = Some(store_health(error));
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
        if let Some(error) = self.refusal.take() {
            original.reject_in(error);
        }
        self.selected = true;
        Poll::Ready(original.take_result().map(StatementActionValue::Local))
    }
    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if self.closed {
            return Poll::Ready(Ok(()));
        }
        if let Some(store) = &self.store {
            store.request_close();
        }
        self.progress(cx);
        let io_done = self.poll_io(cx).is_ready();
        let cpu_done = self.poll_cpu(cx).is_ready();
        self.progress(cx);
        if !io_done || !cpu_done {
            return Poll::Pending;
        }
        if let Some(error) = self.local_health() {
            return Poll::Ready(Err(error));
        }
        std::task::ready!(self.poll_explain_closed(cx))?;
        if let Some(store) = self.store.as_mut() {
            std::task::ready!(store.poll_closed(cx)).map_err(store_health)?;
        }
        if let Some(io) = &self.io
            && io
                .withdraw_unstarted_in()
                .map_err(|error| health(Arc::new(error)))?
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
#[path = "mv_read_action/tests.rs"]
mod tests;
