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

//! Governed UPDATE/MERGE stages over the original signed mutation authors.

use super::statement_computation::{StatementComputation, StatementNativeTail};
use super::statement_publication::StatementPublication;
use super::*;
use crate::dml::{attempt::DmlPublicationAttempt, error::DmlExecutionError};
use crate::query_execution::{
    compiler::PreparedDmlWriteAssembly,
    completion_facts::FrontendCompletionCustody,
    dml::{
        mutation::{self, MutationStatementInput, OwnedMutationStatement},
        mutation_flow::{self, PreparedMorMutationWrite, PreparedMutationFacts},
    },
    internal_consumer::write::ReadyWriteComputation,
    logical_execution::InitialComputationReservation,
    maintenance::io_admission::MetadataIoAdmission,
    write_session::{ConnectorWriteSession, WriteBeginReceiving},
};
use novarocks_query_application::{
    admitted_query_context::QueryExecutionContext,
    api::StatementNativeHealthFailure,
    session_control::{
        GovernedActionContext, GovernedStatementAction, GovernedStatementResidual,
        StatementActionValue,
    },
};
use novarocks_spi::connector::{
    ConnectorControlPlanningPin, ConnectorMetadataCleanupResponsibility, ConnectorRequestContext,
    ConnectorWriteOperationId,
};
use novarocks_workload_control::{WorkError, WorkScope};
use std::task::{Context, Poll};

struct OriginalInput {
    kernel: crate::query_execution::kernels::DmlExecutionKernel,
    statement: OwnedMutationStatement,
    target: crate::catalog_application::resolver::TargetBackend,
    reference: String,
    planning_pin: ConnectorControlPlanningPin,
    operation: ConnectorWriteOperationId,
    request: novarocks_query_application::admitted_query_context::RequestContext,
    options: QueryOptions,
    sql: String,
    receiving: Arc<WriteBeginReceiving>,
}
enum PhaseRequest {
    Facts(Box<(QueryExecutionContext, ConnectorRequestContext)>),
    Mor(Arc<PreparedMutationFacts>),
    CompileMor(
        Arc<PreparedMorMutationWrite>,
        Arc<FrontendCompletionCustody>,
    ),
}
enum PhaseValue {
    Facts(Arc<PreparedMutationFacts>),
    Mor(Arc<PreparedMorMutationWrite>),
    Assembly(Box<PreparedDmlWriteAssembly>),
}
async fn prepare_phase(
    input: Arc<OriginalInput>,
    request: PhaseRequest,
) -> Result<PhaseValue, DmlExecutionError> {
    match request {
        PhaseRequest::Facts(request) => {
            let (execution, context) = *request;
            mutation_flow::prepare_mutation_facts_async(
                &input.kernel,
                &input.statement,
                &input.target,
                &input.reference,
                input.request.session().current_catalog(),
                input.planning_pin.clone(),
                execution,
                &context,
                input.operation,
            )
            .await
            .map(Arc::new)
            .map(PhaseValue::Facts)
        }
        PhaseRequest::Mor(facts) => facts
            .prepare_mor_write_async(&input.kernel, Arc::clone(&input.receiving))
            .await
            .map(Arc::new)
            .map(PhaseValue::Mor),

        PhaseRequest::CompileMor(prepared, custody) => prepared
            .compile_async(&custody, Some(input.options.clone()))
            .await
            .map(Box::new)
            .map(PhaseValue::Assembly),
    }
}
async fn close_provider(
    custody: Arc<FrontendCompletionCustody>,
) -> Result<(), StatementNativeHealthFailure> {
    custody.close().await
}
type PhaseFuture = Pin<Box<dyn Future<Output = Result<PhaseValue, DmlExecutionError>> + Send>>;
type CloseFuture = Pin<Box<dyn Future<Output = Result<(), StatementNativeHealthFailure>> + Send>>;

#[derive(Default)]
struct PhasePreparation {
    future: Option<PhaseFuture>,
    result: Option<Result<PhaseValue, DmlExecutionError>>,
    busy: bool,
}
impl PhasePreparation {
    fn poll_returned(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.busy {
            return Poll::Pending;
        }
        if self.result.is_some() {
            return Poll::Ready(());
        }
        self.busy = true;
        if let Some(future) = self.future.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Pending => {
                    self.busy = false;
                    return Poll::Pending;
                }
                Poll::Ready(result) => self.result = Some(result),
            }
            // A hostile child Drop cannot unwind the returned original cause.
            drop(self.future.take());
        }
        self.busy = false;
        Poll::Ready(())
    }
}

type Computation = StatementComputation<PreparedDmlWriteAssembly>;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Admit,
    Preparing,
    CloseProvider,
    CloseIo,
    Compute,
    LocalExit,
    Publication,
    Done,
}

/// The original mutation session owns the complete logical Root count.
#[derive(Default)]
enum MutationAffectedRows {
    #[default]
    Pending,
    MorSummary(u64),
}
impl MutationAffectedRows {
    fn receive_write(&mut self, session: &ConnectorWriteSession) -> Result<(), QueryServiceError> {
        match self {
            Self::Pending => {
                *self = Self::MorSummary(session.freeze_received_row_count().map_err(|error| {
                    QueryServiceError::from_failure_cause(
                        novarocks_failure_contract::SharedFailureCause::new(error),
                    )
                })?);
                Ok(())
            }
            Self::MorSummary(_) => Err(internal_error("mutation write count was already received")),
        }
    }
    fn get(&self) -> Option<u64> {
        match self {
            Self::Pending => None,
            Self::MorSummary(rows) => Some(*rows),
        }
    }
}

fn poll_mutation_publication(
    publication: &mut StatementPublication,
    session: Option<&Arc<ConnectorWriteSession>>,
    context: &ConnectorRequestContext,
    selected: &Option<Result<(), QueryServiceError>>,
    cx: &mut Context<'_>,
) -> Poll<()> {
    let abort = selected.is_some();
    publication.poll_settle(session, context, abort, cx)
}

struct MutationAction {
    original: Arc<OriginalInput>,
    root: WorkScope,
    io: Option<Arc<MetadataIoAdmission>>,
    session_receiving: Arc<Mutex<Option<Arc<ConnectorWriteSession>>>>,
    context: ConnectorRequestContext,
    computation: Option<Computation>,
    ready: Option<ReadyWriteComputation>,
    inline_tail: Option<StatementNativeTail>,
    facts: Option<Arc<PreparedMutationFacts>>,
    mor: Option<Arc<PreparedMorMutationWrite>>,
    preparation: PhasePreparation,
    assembly: Option<Box<PreparedDmlWriteAssembly>>,
    provider: Option<Arc<FrontendCompletionCustody>>,
    close: Option<CloseFuture>,
    close_result: Option<Result<(), StatementNativeHealthFailure>>,
    failure: Option<DmlExecutionError>,
    selected: Option<Result<(), QueryServiceError>>,
    affected_rows: MutationAffectedRows,
    publication: Option<StatementPublication>,
    phase: Phase,
    busy: bool,
    health: StatementNativeHealthFailure,
}
impl MutationAction {
    fn session(&self) -> Option<Arc<ConnectorWriteSession>> {
        self.session_receiving
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
    fn fail(&mut self, error: QueryServiceError) {
        if self.selected.is_none() {
            self.selected = Some(Err(error));
        }
    }
    fn start(&mut self, request: PhaseRequest) {
        self.preparation.future =
            Some(Box::pin(prepare_phase(Arc::clone(&self.original), request)));
        self.phase = Phase::Preparing;
    }
    fn compile(&mut self, request: impl FnOnce(Arc<FrontendCompletionCustody>) -> PhaseRequest) {
        self.provider = Some(FrontendCompletionCustody::new());
        self.start(request(Arc::clone(self.provider.as_ref().unwrap())));
    }
    fn poll_tails(&mut self, cx: &mut Context<'_>) {
        if let Some(tail) = self.inline_tail.as_mut() {
            let _ = tail.poll_exit(cx);
        }
    }
    fn store_tail(&mut self) -> Result<(), QueryServiceError> {
        self.computation
            .as_mut()
            .unwrap()
            .release_result_aliases()
            .map_err(|_| internal_error("mutation computation result aliases did not retire"))?;
        if self.inline_tail.is_some() {
            return Err(internal_error("mutation Native tail storage is occupied"));
        }
        let tail = self
            .computation
            .as_mut()
            .unwrap()
            .take_native_tail()
            .map_err(|_| internal_error("mutation computation Native tail handoff was refused"))?;
        self.inline_tail = Some(tail);
        drop(self.computation.take());
        Ok(())
    }
    fn poll_inner(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        let cancellation =
            QueryCancellationView::governed(owner.cancellation().clone(), owner.timeout_ms());
        let root_owner = owner
            .execution_owner_slot()
            .as_ref()
            .ok_or_else(|| internal_error("mutation lost its original Root owner"))?;
        if !root_owner.is_for_scope(&self.root) {
            return Poll::Ready(Err(internal_error("mutation has a foreign Root")));
        }
        match self.io.as_ref().unwrap().poll_progress(root_owner, cx) {
            Ok(false) => return Poll::Pending,
            Ok(true) => {}
            Err(e) => self.fail(internal_error(e.to_string())),
        }
        self.poll_tails(cx);
        // A pending original Provider F is polled to its actual result before
        // cancellation can start abort. Its Ready value is stored before Drop.
        if self.preparation.future.is_none()
            && self.close.is_none()
            && self.phase != Phase::Publication
            && self.phase != Phase::Done
            && let Some(reason) = cancellation.reason()
        {
            self.fail(cancellation_error(reason));
        }
        if self.selected.is_some()
            && self.preparation.future.is_none()
            && self.close.is_none()
            && self.phase != Phase::Publication
            && self.phase != Phase::LocalExit
        {
            if let Some(computation) = self.computation.as_mut() {
                match computation.poll_refused_exit(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(_)) => return Poll::Pending,
                    Poll::Ready(Ok(())) => {}
                }
            }
            self.phase = Phase::Publication;
        }
        loop {
            if self.preparation.future.is_none()
                && self.close.is_none()
                && matches!(self.phase, Phase::Admit | Phase::CloseIo)
                && let Some(reason) = cancellation.reason()
            {
                self.fail(cancellation_error(reason));
                self.phase = Phase::Publication;
            }
            match self.phase {
                Phase::Admit => {
                    let root_owner = owner.execution_owner_slot().as_ref().unwrap();
                    match self.computation.as_mut().unwrap().poll_admit(
                        self.io.as_ref().unwrap(),
                        root_owner,
                        cx,
                    ) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(r) => r?,
                    }
                    let execution = self
                        .computation
                        .as_ref()
                        .unwrap()
                        .computation_context(self.original.request.execution())
                        .map_err(|e| internal_error(e.to_string()))?;
                    self.start(PhaseRequest::Facts(Box::new((
                        execution,
                        self.context.clone(),
                    ))));
                }
                Phase::Preparing => {
                    if self.preparation.poll_returned(cx).is_pending() {
                        return Poll::Pending;
                    }
                    if self.preparation.result.as_ref().is_some_and(Result::is_err) {
                        self.failure = Some(self.preparation.result.take().unwrap().err().unwrap());
                        // The exact typed cause remains in this stable source;
                        // its borrowed client projection is made only at exit.
                        self.fail(internal_error("mutation phase failed"));
                        if self.provider.is_some() {
                            self.phase = Phase::CloseProvider;
                        } else {
                            self.phase = Phase::Publication;
                        }
                        continue;
                    }
                    match self.preparation.result.take().unwrap().unwrap() {
                        PhaseValue::Facts(facts) => {
                            self.facts = Some(facts);
                            if self.selected.is_some() || cancellation.reason().is_some() {
                                if let Some(reason) = cancellation.reason() {
                                    self.fail(cancellation_error(reason));
                                }
                                self.phase = Phase::Publication;
                                continue;
                            }
                            let facts = Arc::clone(self.facts.as_ref().unwrap());
                            self.start(PhaseRequest::Mor(facts));
                        }
                        PhaseValue::Mor(prepared) => {
                            self.mor = Some(prepared);
                            let p = Arc::clone(self.mor.as_ref().unwrap());
                            self.compile(|c| PhaseRequest::CompileMor(p, c));
                        }

                        PhaseValue::Assembly(assembly) => {
                            self.assembly = Some(assembly);
                            self.phase = Phase::CloseProvider;
                        }
                    }
                }
                Phase::CloseProvider => {
                    if self.close.is_none() {
                        self.close = Some(Box::pin(close_provider(Arc::clone(
                            self.provider.as_ref().unwrap(),
                        ))));
                    }
                    match self.close.as_mut().unwrap().as_mut().poll(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(result) => self.close_result = Some(result),
                    }
                    if self.close_result.as_ref().is_some_and(Result::is_err) {
                        return Poll::Pending;
                    }
                    drop(self.close.take());
                    drop(self.provider.take());
                    drop(self.close_result.take());
                    self.phase = Phase::CloseIo;
                }
                Phase::CloseIo => {
                    let root_owner = owner.execution_owner_slot().as_ref().unwrap();
                    match self.io.as_ref().unwrap().poll_quiescent(root_owner, cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(r) => r.map_err(|e| internal_error(e.to_string()))?,
                    }
                    if self.selected.is_some() || cancellation.reason().is_some() {
                        if let Some(reason) = cancellation.reason() {
                            self.fail(cancellation_error(reason));
                        }
                        self.phase = Phase::Publication;
                        continue;
                    }
                    self.computation
                        .as_mut()
                        .unwrap()
                        .install_assembly(*self.assembly.take().unwrap())?;
                    self.phase = Phase::Compute;
                }
                Phase::Compute => {
                    let computation = match self.original.kernel.write_runtime() {
                        Ok(runtime) => self.computation.as_mut().unwrap().poll_ready(
                            runtime,
                            self.io.as_ref().unwrap(),
                            owner,
                            cx,
                        ),
                        Err(error) => Poll::Ready(Err(internal_error(error))),
                    };
                    match computation {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => {
                            self.fail(error);
                            self.phase = Phase::Publication;
                            continue;
                        }
                        Poll::Ready(Ok(())) => {
                            self.ready = self.computation.as_mut().unwrap().take_ready()
                        }
                    }
                    if self.ready.is_none() {
                        return Poll::Ready(Err(internal_error(
                            "mutation has no actual Ready payload",
                        )));
                    }
                    self.phase = Phase::LocalExit;
                }
                Phase::LocalExit => {
                    match self.computation.as_mut().unwrap().poll_ready_local_exit(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(_)) => return Poll::Pending,
                        Poll::Ready(Ok(())) => {}
                    }
                    self.store_tail()?;
                    let ready = self.ready.take().unwrap();
                    let session = self
                        .session()
                        .ok_or_else(|| internal_error("mutation Ready has no original session"))?;
                    if !Arc::ptr_eq(&ready.session, &session)
                        || !ready.accepted.belongs_to(&session)
                    {
                        return Poll::Ready(Err(internal_error(
                            "mutation Ready replaced the original session",
                        )));
                    }
                    self.affected_rows.receive_write(&session)?;
                    self.phase = Phase::Publication;
                }

                Phase::Publication => {
                    if self.publication.as_ref().unwrap().call.is_none()
                        && let Some(error) = self
                            .inline_tail
                            .as_mut()
                            .and_then(|tail| tail.poll_supervision_failure(cx))
                    {
                        self.fail(governed_query_execution_error(error));
                    }
                    // No new effect begins after cancellation. Existing Calls
                    // settle once; an Applied verdict survives late cancellation.
                    if self.publication.as_ref().unwrap().call.is_none()
                        && let Some(reason) = cancellation.reason()
                    {
                        self.fail(cancellation_error(reason));
                    }
                    if let Some(computation) = self.computation.as_mut() {
                        match computation.poll_refused_exit(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(Err(_)) => return Poll::Pending,
                            Poll::Ready(Ok(())) => {}
                        }
                    }
                    if self.publication.as_ref().unwrap().call.is_none() {
                        let root_owner = owner.execution_owner_slot().as_ref().unwrap();
                        match self.io.as_ref().unwrap().poll_quiescent(root_owner, cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(Err(error)) => self.fail(internal_error(error.to_string())),
                            Poll::Ready(Ok(())) => {}
                        }
                    }
                    let session = self.session();
                    if poll_mutation_publication(
                        self.publication.as_mut().unwrap(),
                        session.as_ref(),
                        &self.context,
                        &self.selected,
                        cx,
                    )
                    .is_pending()
                    {
                        return Poll::Pending;
                    }
                    self.publication.as_mut().unwrap().freeze_terminal()?;
                    if self.failure.is_none() && self.selected.is_none() {
                        self.selected = Some(
                            self.publication
                                .as_ref()
                                .unwrap()
                                .select(cancellation.reason()),
                        );
                    }
                    self.phase = Phase::Done;
                }
                Phase::Done => {
                    let result = if let Some(error) = self.failure.as_ref() {
                        Err(project_failure(error, &self.original.sql))
                    } else {
                        self.selected.take().unwrap_or_else(|| {
                            Err(internal_error(
                                "mutation has no original business selection",
                            ))
                        })
                    };
                    return Poll::Ready(
                        super::write_action::selected_command_completion(
                            result,
                            self.affected_rows.get(),
                        )
                        .map_err(|error| {
                            match self.publication.as_ref().unwrap().attempt.terminal() {
                                Some(t) => error.attach_publication_terminal(t.clone()),
                                None => error,
                            }
                        }),
                    );
                }
            }
        }
    }
}

/// Borrow the existing error authors while the exact cause remains in its
/// original source through abort, client projection and residual handoff.
fn project_failure(error: &DmlExecutionError, sql: &str) -> QueryServiceError {
    match error {
        DmlExecutionError::Application(error) => error.clone(),
        DmlExecutionError::Execution(input) => {
            QueryServiceError::from_execution_failure(input.clone())
        }
        DmlExecutionError::Engine(error) => routed_execution_service_error(
            dml_result::<()>(Err(crate::dml::DmlError::executor(error))).unwrap_err(),
        ),
        DmlExecutionError::Compilation(source) => QueryServiceError::from_compile_control(
            novarocks_type_contract::CompileControlError::Source(source.clone()),
        ),
        DmlExecutionError::Analyze(error) => match error.control_error() {
            Some(control) => QueryServiceError::from_compile_control(control),
            None => QueryServiceError::from_user_error(error.to_user_error(Some(sql))),
        },
        DmlExecutionError::Control(error) => QueryServiceError::from_compile_control(error.clone()),
    }
}
#[derive(Debug)]
struct MutationExitUnproven;
impl std::fmt::Display for MutationExitUnproven {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("mutation source poll or Future Drop did not return normally")
    }
}
impl std::error::Error for MutationExitUnproven {}
fn health() -> StatementNativeHealthFailure {
    StatementNativeHealthFailure::actual_exit_unproven(
        "mutation source exit is unproven",
        Arc::new(MutationExitUnproven),
    )
}
impl GovernedStatementAction for MutationAction {
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        if self.busy {
            return Poll::Pending;
        }
        self.busy = true;
        let mut result = self.poll_inner(owner, cx);
        if self.phase != Phase::Done
            && let Poll::Ready(Err(error)) = result
        {
            self.fail(error);
            self.phase = Phase::Publication;
            result = self.poll_inner(owner, cx);
        }
        self.busy = false;
        result
    }
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.busy
            .then(|| self.health.clone())
            .or_else(|| self.preparation.busy.then(|| self.health.clone()))
            .or_else(|| self.original.receiving.local_health_failure())
            .or_else(|| {
                self.provider
                    .as_ref()
                    .and_then(|p| p.local_health_failure())
            })
            .or_else(|| {
                self.close_result
                    .as_ref()
                    .and_then(|r| r.as_ref().err())
                    .cloned()
            })
            .or_else(|| {
                self.computation
                    .as_ref()
                    .and_then(Computation::local_health_failure)
            })
            .or_else(|| {
                self.inline_tail
                    .as_ref()
                    .and_then(StatementNativeTail::local_health_failure)
            })
            .or_else(|| {
                self.publication
                    .as_ref()
                    .and_then(StatementPublication::local_health_failure)
            })
            .or_else(|| self.io.as_ref().and_then(|io| io.health_failure()))
    }
    fn external_effect_disposition(
        &self,
    ) -> Option<novarocks_query_application::api::StatementExternalEffectDisposition> {
        self.publication
            .as_ref()
            .and_then(StatementPublication::disposition)
            .map(crate::query::external_effect_from_lake)
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if target.is_some() || self.busy || self.phase != Phase::Done {
            return Err(self.health.clone());
        }
        self.busy = true;
        // Move original observations and typed causes into a concrete owner.
        // Installation precedes release of any original result/source aliases.
        let mut residual = Box::new(MutationResidual {
            inline_tail: None,
            session: self.session(),
            publication: None,
            io: None,
            continuation_error: None,
            failure: None,
            health: self.health.clone(),
            busy: false,
        });
        residual.inline_tail = self.inline_tail.take();
        residual.publication = self.publication.take();
        residual.io = self.io.take();
        residual.failure = self.failure.take();
        *target = Some(residual);
        drop(self.ready.take());
        drop(self.computation.take());
        drop(self.assembly.take());
        drop(self.mor.take());
        drop(self.facts.take());
        let session = self
            .session_receiving
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        drop(session);
        self.busy = false;
        Ok(())
    }
}
struct MutationResidual {
    inline_tail: Option<StatementNativeTail>,
    session: Option<Arc<ConnectorWriteSession>>,
    publication: Option<StatementPublication>,
    io: Option<Arc<MetadataIoAdmission>>,
    continuation_error: Option<WorkError>,
    failure: Option<DmlExecutionError>,
    health: StatementNativeHealthFailure,
    busy: bool,
}
impl GovernedStatementResidual for MutationResidual {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if self.busy {
            return Poll::Ready(Err(self.health.clone()));
        }
        self.busy = true;
        let mut pending = false;
        if let Some(io) = &self.io {
            if let Err(error) = io.poll_continuing(cx) {
                self.continuation_error = Some(error);
                self.busy = false;
                return Poll::Ready(Err(self.health.clone()));
            }
            if let Some(health) = io.health_failure() {
                self.busy = false;
                return Poll::Ready(Err(health));
            }
        }
        if let Some(publication) = self.publication.as_mut() {
            pending |= publication.poll_retirement(cx).is_pending();
            if let Some(health) = publication.local_health_failure() {
                self.busy = false;
                return Poll::Ready(Err(health));
            }
        }
        if let Some(tail) = self.inline_tail.as_mut() {
            pending |= tail.poll_exit(cx).is_pending();
            if let Some(health) = tail.local_health_failure() {
                self.busy = false;
                return Poll::Ready(Err(health));
            }
        }
        if pending {
            self.busy = false;
            return Poll::Pending;
        }
        if let Some(io) = &self.io {
            match io.poll_quiescent_continuing(cx) {
                Poll::Pending => {
                    self.busy = false;
                    return Poll::Pending;
                }
                Poll::Ready(Err(error)) => {
                    self.continuation_error = Some(error);
                    self.busy = false;
                    return Poll::Ready(Err(self.health.clone()));
                }
                Poll::Ready(Ok(())) => {}
            }
        }
        match self.session.as_ref().map(|s| s.cleanup_responsibility()) {
            Some(Ok(ConnectorMetadataCleanupResponsibility::Retained)) | Some(Err(_)) => {
                self.busy = false;
                return Poll::Pending;
            }
            _ => {}
        }
        drop(self.publication.take());
        drop(self.session.take());
        drop(self.inline_tail.take());
        drop(self.failure.take());
        drop(self.io.take());
        self.busy = false;
        Poll::Ready(Ok(()))
    }
}

pub(crate) fn prepare(
    kernel: crate::query_execution::kernels::DmlExecutionKernel,
    input: MutationStatementInput,
) -> Result<
    Box<dyn GovernedStatementAction>,
    crate::catalog_application::resolver::TargetResolutionError,
> {
    let root = input
        .context
        .execution()
        .result_capacity()
        .ok_or_else(|| "mutation has no original Root result capacity".to_owned())?
        .root_scope()
        .clone();
    let (target, reference) = mutation::resolve_target(
        &kernel,
        &input.statement,
        input.context.session().current_catalog(),
        input.context.session().current_database(),
    )?;
    let planning_pin = crate::connector::acquire_metadata_planning_pin(
        kernel.connector_control().as_ref(),
        &target.catalog,
    )?;
    let stack = kernel
        .typed_connector_control()
        .acquire_exact_write_stack(planning_pin.control_runtime_id())
        .map_err(|e| e.to_string())?;
    let profile = stack.session().io_profile().map_err(|e| e.to_string())?;
    drop(stack);
    let source_bytes = profile.geometry().metadata_bytes().get();
    let io =
        MetadataIoAdmission::new(root.clone(), profile, source_bytes).map_err(|e| e.to_string())?;
    let initial = InitialComputationReservation::new(
        input
            .launcher
            .reserve_identity()
            .map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    let context = initial
        .connector_request_context(
            kernel.connector_request_context_for_execution(
                Some(&input.query_options),
                input.context.execution(),
            )?,
            #[cfg(novarocks_dev_controls)]
            input.context.execution().test_statement_nomination(),
        )
        .map_err(|e| e.to_string())?
        .with_io_admission(io.clone());
    let session_receiving = Arc::new(Mutex::new(None));
    let receiving = WriteBeginReceiving::for_statement(
        Arc::clone(&session_receiving),
        Arc::clone(input.context.execution().session_property_inputs()),
    );
    let publication_id = novarocks_spi::connector::LakePublicationId::new_v7();
    let attempt = DmlPublicationAttempt::new(
        publication_id,
        novarocks_spi::connector::LakePublicationFamily::DataMutation,
        novarocks_spi::connector::LakePublicationTarget::try_new(
            target.catalog.clone(),
            target.namespace.clone(),
            Some(target.table.clone()),
            (reference != "main").then(|| reference.clone()),
        )
        .map_err(|e| e.to_string())?,
        None,
    );
    let computation = Computation::new(
        root.clone(),
        initial,
        Arc::clone(&input.launcher),
        #[cfg(novarocks_dev_controls)]
        input.observation.clone(),
    );
    let original = Arc::new(OriginalInput {
        kernel,
        statement: input.statement,
        target,
        reference,
        planning_pin,
        operation: publication_id.into(),
        request: input.context,
        options: input.query_options,
        sql: input.sql,
        receiving,
    });
    Ok(Box::new(MutationAction {
        original,
        root,
        io: Some(io),
        session_receiving,
        context,
        computation: Some(computation),
        ready: None,
        inline_tail: None,
        facts: None,
        mor: None,
        preparation: PhasePreparation::default(),
        assembly: None,
        provider: None,
        close: None,
        close_result: None,
        failure: None,
        selected: None,
        affected_rows: MutationAffectedRows::default(),
        publication: Some(StatementPublication::new(attempt)),
        phase: Phase::Admit,
        busy: false,
        health: health(),
    }))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod affected_count_tests {
    use super::*;
    use crate::query_execution::{
        write_result::DecodedPreparedWriteSet,
        write_session::tests::{fixture_with_outcome, known_committed},
    };

    #[test]
    fn mor_action_count_uses_complete_logical_root_and_cannot_be_reauthored() {
        let fixture = fixture_with_outcome(1, 16, known_committed());
        fixture
            .session
            .receive_for_test(DecodedPreparedWriteSet::for_test(19, Vec::new()))
            .unwrap();
        let mut rows = MutationAffectedRows::default();
        assert_eq!(rows.get(), None);
        rows.receive_write(&fixture.session).unwrap();
        assert_eq!(rows.get(), Some(19));
        assert!(rows.receive_write(&fixture.session).is_err());
        assert_eq!(rows.get(), Some(19));
        assert_eq!(fixture.session.finish_invocations(), 0);
    }
}
