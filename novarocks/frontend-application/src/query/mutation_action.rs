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

use super::statement_computation::{
    StatementComputation, StatementComputationAssembly, StatementComputationOutput,
    StatementNativeTail,
};
use super::statement_publication::StatementPublication;
use super::*;
use crate::dml::{attempt::DmlPublicationAttempt, error::DmlExecutionError};
use crate::query_execution::{
    compiler::PreparedDmlWriteAssembly,
    completion_facts::FrontendCompletionCustody,
    dml::{
        mutation::{self, MutationStatementInput, OwnedMutationStatement},
        mutation_flow::{
            self, PreparedCowMatchAssembly, PreparedCowMutationWrite, PreparedCowSelection,
            PreparedMorMutationWrite, PreparedMutationFacts,
        },
    },
    internal_consumer::{
        decoded::{CowComputationOutput, DecodedConsumerRuntime, ReadyCowComputation},
        domain_cpu::DomainCpuCustody,
        write::{ReadyWriteComputation, WriteComputationOutput},
    },
    logical_execution::{InitialComputationReservation, PreparedLogicalComputation},
    maintenance::io_admission::MetadataIoAdmission,
    write_session::{ConnectorWriteSession, WriteBeginReceiving},
};
use novarocks_query_application::{
    admitted_query_context::{QueryExecutionContext, QueryResultCapacityBinding},
    api::{ComputationReadyToken, ReservedExecutionIdentity, StatementNativeHealthFailure},
    session_control::{
        GovernedActionContext, GovernedStatementAction, GovernedStatementResidual,
        StatementActionValue,
    },
};
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorMetadataCleanupResponsibility, ConnectorRequestContext,
    ConnectorRowMutationSelection, ConnectorWriteOperationId,
};
use novarocks_workload_control::{WorkError, WorkScope};
use std::{
    alloc::Layout,
    task::{Context, Poll},
};

// This closed sum delegates actual assembly/consumer construction. No query is
// reconstructed and no selection is cloned to cross a phase boundary.
enum MutationAssembly {
    Selection(Box<MutationSelectionAssembly>),
    Write(Box<PreparedDmlWriteAssembly>),
}
struct MutationSelectionAssembly {
    assembly: PreparedCowMatchAssembly,
    options: QueryOptions,
}
enum MutationOutput {
    Selection(CowComputationOutput),
    Write(WriteComputationOutput),
}
enum MutationReady {
    Selection(Box<ReadyCowComputation>),
    Write(ReadyWriteComputation),
}
impl StatementComputationAssembly for MutationAssembly {
    type Output = MutationOutput;
    fn prepare(
        self,
        initial: InitialComputationReservation,
        runtime: DecodedConsumerRuntime,
    ) -> Result<
        (
            PreparedLogicalComputation,
            ReservedExecutionIdentity,
            MutationOutput,
        ),
        QueryExecutionError,
    > {
        match self {
            Self::Selection(selection) => {
                let MutationSelectionAssembly { assembly, options } = *selection;
                assembly
                    .prepare_computation(initial, runtime, Some(options))
                    .map(|(c, r, o)| (c, r, MutationOutput::Selection(o)))
            }
            Self::Write(assembly) => (*assembly)
                .prepare_computation(initial, runtime)
                .map(|(c, r, o)| (c, r, MutationOutput::Write(o))),
        }
    }
}
impl StatementComputationOutput for MutationOutput {
    type Ready = MutationReady;
    fn cpu_custody(&self) -> Arc<DomainCpuCustody> {
        match self {
            Self::Selection(o) => o.cpu_custody(),
            Self::Write(o) => o.cpu_custody(),
        }
    }
    fn take_ready(
        &mut self,
        token: ComputationReadyToken,
    ) -> Result<MutationReady, QueryExecutionError> {
        match self {
            Self::Selection(o) => o
                .take_ready(token)
                .map(Box::new)
                .map(MutationReady::Selection),
            Self::Write(o) => o.take_ready(token).map(MutationReady::Write),
        }
    }
}

struct OriginalInput {
    kernel: crate::query_execution::kernels::DmlExecutionKernel,
    statement: OwnedMutationStatement,
    target: crate::catalog_application::resolver::TargetBackend,
    reference: String,
    lease: ConnectorControlPlanningLease,
    operation: ConnectorWriteOperationId,
    request: novarocks_query_application::admitted_query_context::RequestContext,
    options: QueryOptions,
    sql: String,
    receiving: Arc<WriteBeginReceiving>,
}
enum PhaseRequest {
    Facts(QueryExecutionContext, ConnectorRequestContext),
    Mor(Arc<PreparedMutationFacts>),
    Selection(Arc<PreparedMutationFacts>),
    BeginCow(
        Arc<PreparedMutationFacts>,
        ConnectorRowMutationSelection,
        QueryExecutionContext,
    ),
    CompileMor(
        Arc<PreparedMorMutationWrite>,
        Arc<FrontendCompletionCustody>,
    ),
    CompileSelection(Arc<PreparedCowSelection>, Arc<FrontendCompletionCustody>),
    CompileTarget(
        Arc<PreparedCowMutationWrite>,
        usize,
        QueryExecutionContext,
        ConnectorRequestContext,
        Arc<FrontendCompletionCustody>,
    ),
}
enum PhaseValue {
    Facts(Arc<PreparedMutationFacts>),
    Mor(Arc<PreparedMorMutationWrite>),
    Selection(Arc<PreparedCowSelection>),
    Cow(Arc<PreparedCowMutationWrite>),
    Assembly(MutationAssembly),
}
async fn prepare_phase(
    input: Arc<OriginalInput>,
    request: PhaseRequest,
) -> Result<PhaseValue, DmlExecutionError> {
    match request {
        PhaseRequest::Facts(execution, context) => mutation_flow::prepare_mutation_facts_async(
            &input.kernel,
            &input.statement,
            &input.target,
            &input.reference,
            input.request.session().current_catalog(),
            input.lease.clone(),
            execution,
            &context,
            input.operation,
        )
        .await
        .map(Arc::new)
        .map(PhaseValue::Facts),
        PhaseRequest::Mor(facts) => facts
            .prepare_mor_write_async(&input.kernel, Arc::clone(&input.receiving))
            .await
            .map(Arc::new)
            .map(PhaseValue::Mor),
        PhaseRequest::Selection(facts) => facts
            .prepare_cow_selection(&input.kernel)
            .map(Arc::new)
            .map(PhaseValue::Selection),
        PhaseRequest::BeginCow(facts, selection, execution) => facts
            .prepare_cow_write_async(
                &input.kernel,
                selection,
                execution,
                Arc::clone(&input.receiving),
            )
            .await
            .map(Arc::new)
            .map(PhaseValue::Cow),
        PhaseRequest::CompileMor(prepared, custody) => prepared
            .compile_async(&custody, Some(input.options.clone()))
            .await
            .map(Box::new)
            .map(MutationAssembly::Write)
            .map(PhaseValue::Assembly),
        PhaseRequest::CompileSelection(prepared, custody) => prepared
            .compile_async(&custody, Some(input.options.clone()))
            .await
            .map(|assembly| {
                MutationAssembly::Selection(Box::new(MutationSelectionAssembly {
                    assembly,
                    options: input.options.clone(),
                }))
            })
            .map(PhaseValue::Assembly),
        PhaseRequest::CompileTarget(prepared, index, execution, context, custody) => prepared
            .compile_target_async(
                index,
                &execution,
                &context,
                &custody,
                Some(input.options.clone()),
            )
            .await
            .map(Box::new)
            .map(MutationAssembly::Write)
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

type Computation = StatementComputation<MutationAssembly>;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Admit,
    Preparing,
    CloseProvider,
    CloseIo,
    Compute,
    LocalExit,
    NextWriter,
    Publication,
    Done,
}

/// The logical count author is chosen by the original mutation path. A COW
/// rewrite result cannot replace its exact matched-selection count.
#[derive(Default)]
enum MutationAffectedRows {
    #[default]
    Pending,
    CowSelection(u64),
    MorSummary(u64),
}
impl MutationAffectedRows {
    fn receive_selection(
        &mut self,
        selection: &ConnectorRowMutationSelection,
    ) -> Result<(), QueryServiceError> {
        if !matches!(self, Self::Pending) {
            return Err(internal_error(
                "mutation selected-row count was already received",
            ));
        }
        *self = Self::CowSelection(selection.row_count());
        Ok(())
    }
    fn receive_write(&mut self, session: &ConnectorWriteSession) -> Result<(), QueryServiceError> {
        match self {
            Self::CowSelection(_) => Ok(()),
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
            Self::CowSelection(rows) | Self::MorSummary(rows) => Some(*rows),
        }
    }
}

struct MutationAction {
    original: Arc<OriginalInput>,
    root: WorkScope,
    io: Option<Arc<MetadataIoAdmission>>,
    launcher: Arc<dyn LogicalExecutionLauncher>,
    #[cfg(novarocks_dev_controls)]
    observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
    session_receiving: Arc<Mutex<Option<Arc<ConnectorWriteSession>>>>,
    context: ConnectorRequestContext,
    next_context: Option<ConnectorRequestContext>,
    selection_execution: Option<QueryExecutionContext>,
    computation: Option<Computation>,
    ready: Option<MutationReady>,
    inline_tail: Option<StatementNativeTail>,
    tails: Vec<StatementNativeTail>,
    tail_binding: Option<QueryResultCapacityBinding>,
    tail_capacity: usize,
    tail_bytes: u64,
    tails_reserved: bool,
    facts: Option<Arc<PreparedMutationFacts>>,
    mor: Option<Arc<PreparedMorMutationWrite>>,
    selection: Option<Arc<PreparedCowSelection>>,
    cow: Option<Arc<PreparedCowMutationWrite>>,
    preparation: PhasePreparation,
    assembly: Option<MutationAssembly>,
    provider: Option<Arc<FrontendCompletionCustody>>,
    close: Option<CloseFuture>,
    close_result: Option<Result<(), StatementNativeHealthFailure>>,
    failure: Option<DmlExecutionError>,
    selected: Option<Result<(), QueryServiceError>>,
    affected_rows: MutationAffectedRows,
    publication: Option<StatementPublication>,
    phase: Phase,
    index: usize,
    wrote_any: bool,
    no_op: bool,
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
        for tail in &mut self.tails {
            let _ = tail.poll_exit(cx);
        }
    }
    fn store_tail(&mut self) -> Result<(), QueryServiceError> {
        self.computation
            .as_mut()
            .unwrap()
            .release_result_aliases()
            .map_err(|_| internal_error("mutation computation result aliases did not retire"))?;
        if (self.tails_reserved && self.tails.len() == self.tails.capacity())
            || (!self.tails_reserved && self.inline_tail.is_some())
        {
            return Err(internal_error("mutation Native tail storage is occupied"));
        }
        let tail = self
            .computation
            .as_mut()
            .unwrap()
            .take_native_tail()
            .map_err(|_| internal_error("mutation computation Native tail handoff was refused"))?;
        if self.tails_reserved {
            self.tails.push(tail);
        } else if self.inline_tail.is_none() {
            self.inline_tail = Some(tail);
        }
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
                && matches!(
                    self.phase,
                    Phase::Admit | Phase::NextWriter | Phase::CloseIo
                )
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
                    self.start(PhaseRequest::Facts(execution, self.context.clone()));
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
                            if facts.is_copy_on_write() {
                                self.start(PhaseRequest::Selection(facts));
                            } else {
                                self.start(PhaseRequest::Mor(facts));
                            }
                        }
                        PhaseValue::Mor(prepared) => {
                            self.mor = Some(prepared);
                            let p = Arc::clone(self.mor.as_ref().unwrap());
                            self.compile(|c| PhaseRequest::CompileMor(p, c));
                        }
                        PhaseValue::Selection(prepared) => {
                            self.selection = Some(prepared);
                            let p = Arc::clone(self.selection.as_ref().unwrap());
                            self.compile(|c| PhaseRequest::CompileSelection(p, c));
                        }
                        PhaseValue::Cow(prepared) => {
                            self.cow = Some(prepared);
                            self.tail_capacity = self
                                .cow
                                .as_ref()
                                .unwrap()
                                .target_count()
                                .checked_add(1)
                                .ok_or_else(|| internal_error("mutation tail count overflow"))?;
                            if self.tail_capacity
                                > novarocks_spi::connector::write_stack::MAX_CONNECTOR_WRITE_TARGETS
                                    + 1
                            {
                                return Poll::Ready(Err(internal_error(
                                    "mutation exceeds the frozen target bound",
                                )));
                            }
                            self.tail_bytes = u64::try_from(
                                Layout::array::<StatementNativeTail>(self.tail_capacity)
                                    .map_err(|_| internal_error("mutation tail layout overflow"))?
                                    .size()
                                    .checked_add(
                                        StatementNativeTail::retained_heap_bytes()
                                            .map_err(|e| internal_error(e.to_string()))?
                                            .checked_mul(self.tail_capacity)
                                            .ok_or_else(|| {
                                                internal_error("mutation tail health overflow")
                                            })?,
                                    )
                                    .ok_or_else(|| {
                                        internal_error("mutation tail storage overflow")
                                    })?,
                            )
                            .map_err(|_| internal_error("mutation tail layout overflow"))?;
                            self.phase = Phase::NextWriter;
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
                        .install_assembly(self.assembly.take().unwrap())?;
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
                    if self.selection_execution.is_none()
                        && let Some(MutationReady::Selection(ready)) = self.ready.as_ref()
                    {
                        self.selection_execution = Some(
                            self.original
                                .request
                                .execution()
                                .for_statement_retention(ready.binding.clone())
                                .map_err(|e| internal_error(e.to_string()))?,
                        );
                    }
                    match self.computation.as_mut().unwrap().poll_ready_local_exit(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(_)) => return Poll::Pending,
                        Poll::Ready(Ok(())) => {}
                    }
                    self.store_tail()?;
                    match self.ready.as_ref().unwrap() {
                        MutationReady::Selection(ready) => {
                            // This exact selection includes only admitted actions. COW
                            // rewrite streams also contain unaffected retained rows.
                            self.affected_rows.receive_selection(&ready.selection)?;
                            self.no_op = ready.selection.row_count() == 0;
                            self.tail_binding = Some(ready.binding.clone());
                            if self.no_op || cancellation.reason().is_some() {
                                if let Some(reason) = cancellation.reason() {
                                    self.fail(cancellation_error(reason));
                                }
                                self.phase = Phase::Publication;
                                continue;
                            }
                            let execution = self
                                .selection_execution
                                .take()
                                .expect("original Ready selection execution");
                            // Moving selection into the original F preserves the
                            // signed scope and its complete first-party payload.
                            let MutationReady::Selection(ready) = self.ready.take().unwrap() else {
                                unreachable!()
                            };
                            let ready = *ready;
                            self.start(PhaseRequest::BeginCow(
                                Arc::clone(self.facts.as_ref().unwrap()),
                                ready.selection,
                                execution,
                            ));
                        }
                        MutationReady::Write(ready) => {
                            let session = self.session().ok_or_else(|| {
                                internal_error("mutation Ready has no original session")
                            })?;
                            if !Arc::ptr_eq(&ready.session, &session)
                                || !ready.accepted.belongs_to(&session)
                            {
                                return Poll::Ready(Err(internal_error(
                                    "mutation Ready replaced the original session",
                                )));
                            }
                            self.affected_rows.receive_write(&session)?;
                            self.wrote_any |= !ready.accepted.is_empty();
                            self.index = self
                                .index
                                .checked_add(1)
                                .ok_or_else(|| internal_error("mutation target index overflow"))?;
                            drop(self.ready.take());
                            if self
                                .cow
                                .as_ref()
                                .is_some_and(|p| self.index < p.target_count())
                            {
                                self.phase = Phase::NextWriter;
                            } else {
                                self.no_op = !self.wrote_any;
                                self.phase = Phase::Publication;
                            }
                        }
                    }
                }
                Phase::NextWriter => {
                    // The actual checked-begin/previous compile IO has exited
                    // before extending this Root's next dependency union.
                    let root_owner = owner.execution_owner_slot().as_ref().unwrap();
                    match self.io.as_ref().unwrap().poll_quiescent(root_owner, cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(r) => r.map_err(|e| internal_error(e.to_string()))?,
                    }
                    if self.computation.is_none() {
                        let initial = InitialComputationReservation::new(
                            self.launcher
                                .reserve_identity()
                                .map_err(governed_query_execution_error)?,
                        )
                        .map_err(|e| internal_error(e.to_string()))?;
                        self.next_context = Some(
                            initial
                                .connector_request_context(
                                    self.context.clone(),
                                    #[cfg(novarocks_dev_controls)]
                                    self.original
                                        .request
                                        .execution()
                                        .test_statement_nomination(),
                                )
                                .map_err(|e| internal_error(e.to_string()))?,
                        );
                        let peak = crate::query_execution::internal_result_cpu::INTERNAL_PEAK_BYTES
                            .checked_add(if self.tails_reserved {
                                0
                            } else {
                                self.tail_bytes
                            })
                            .ok_or_else(|| {
                                internal_error("mutation retained tail peak overflow")
                            })?;
                        self.computation = Some(Computation::new_with_peak(
                            self.root.clone(),
                            initial,
                            Arc::clone(&self.launcher),
                            peak,
                            #[cfg(novarocks_dev_controls)]
                            self.observation.clone(),
                        ));
                    }
                    let root_owner = owner.execution_owner_slot().as_ref().unwrap();
                    match self.computation.as_mut().unwrap().poll_admit(
                        self.io.as_ref().unwrap(),
                        root_owner,
                        cx,
                    ) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(r) => r?,
                    }
                    if !self.tails_reserved {
                        // The original positive joint stage has installed the
                        // retained increment before this actual allocation.
                        self.tails
                            .try_reserve_exact(self.tail_capacity)
                            .map_err(|_| {
                                QueryServiceError::from_compile_control(
                                    novarocks_type_contract::CompileControlError::ResourceExhausted,
                                )
                            })?;
                        self.tails_reserved = true;
                        if let Some(tail) = self.inline_tail.take() {
                            self.tails.push(tail);
                        }
                    }
                    let execution = self
                        .computation
                        .as_ref()
                        .unwrap()
                        .computation_context(self.original.request.execution())
                        .map_err(|e| internal_error(e.to_string()))?;
                    let context = self.next_context.take().unwrap();
                    let p = Arc::clone(self.cow.as_ref().unwrap());
                    let index = self.index;
                    self.compile(|c| PhaseRequest::CompileTarget(p, index, execution, context, c));
                }
                Phase::Publication => {
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
                    let abort = self.selected.is_some() || self.no_op;
                    if self
                        .publication
                        .as_mut()
                        .unwrap()
                        .poll_settle(session.as_ref(), &self.context, abort, cx)
                        .is_pending()
                    {
                        return Poll::Pending;
                    }
                    if self.publication.as_ref().unwrap().committed()
                        && self
                            .publication
                            .as_ref()
                            .unwrap()
                            .local_finalization
                            .is_none()
                    {
                        let result = self
                            .facts
                            .as_ref()
                            .ok_or_else(|| {
                                internal_error("committed mutation has no original facts")
                            })?
                            .finalize(&self.original.kernel);
                        self.publication.as_mut().unwrap().local_finalization = Some(result);
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
                        Err(project_failure(
                            error,
                            &self.original.sql,
                            self.publication.as_ref().unwrap().attempt.terminal(),
                        ))
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
fn project_failure(
    error: &DmlExecutionError,
    sql: &str,
    terminal: Option<&novarocks_spi::connector::LakePublicationTerminal>,
) -> QueryServiceError {
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
        DmlExecutionError::Cow(error) => internal_error(error.borrowed_presentation(terminal)),
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
                self.tails
                    .iter()
                    .find_map(StatementNativeTail::local_health_failure)
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
            tails: Vec::new(),
            inline_tail: None,
            tail_binding: None,
            session: self.session(),
            publication: None,
            io: None,
            continuation_error: None,
            failure: None,
            health: self.health.clone(),
            busy: false,
        });
        residual.tails = std::mem::take(&mut self.tails);
        residual.inline_tail = self.inline_tail.take();
        residual.tail_binding = self.tail_binding.take();
        residual.publication = self.publication.take();
        residual.io = self.io.take();
        residual.failure = self.failure.take();
        *target = Some(residual);
        drop(self.ready.take());
        drop(self.selection_execution.take());
        drop(self.next_context.take());
        drop(self.computation.take());
        drop(self.assembly.take());
        drop(self.cow.take());
        drop(self.mor.take());
        drop(self.selection.take());
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
    tails: Vec<StatementNativeTail>,
    inline_tail: Option<StatementNativeTail>,
    tail_binding: Option<QueryResultCapacityBinding>,
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
        for tail in &mut self.tails {
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
        drop(std::mem::take(&mut self.tails));
        drop(self.tail_binding.take());
        drop(self.failure.take());
        drop(self.io.take());
        self.busy = false;
        Poll::Ready(Ok(()))
    }
}

pub(crate) fn prepare(
    kernel: crate::query_execution::kernels::DmlExecutionKernel,
    input: MutationStatementInput,
) -> Result<Box<dyn GovernedStatementAction>, String> {
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
    let lease = crate::connector::acquire_metadata_planning_lease(
        kernel.connector_control().as_ref(),
        &target.catalog,
    )?;
    let stack = kernel
        .typed_connector_control()
        .acquire_exact_write_stack(lease.control_runtime_id())
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
    let receiving = WriteBeginReceiving::new(Arc::clone(&session_receiving));
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
        lease,
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
        launcher: input.launcher,
        #[cfg(novarocks_dev_controls)]
        observation: input.observation,
        session_receiving,
        context,
        next_context: None,
        selection_execution: None,
        computation: Some(computation),
        ready: None,
        inline_tail: None,
        tails: Vec::new(),
        tail_binding: None,
        tail_capacity: 0,
        tail_bytes: 0,
        tails_reserved: false,
        facts: None,
        mor: None,
        selection: None,
        cow: None,
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
        index: 0,
        wrote_any: false,
        no_op: false,
        busy: false,
        health: health(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_execution::dml::mutation_flow::CowFailure;
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
    use std::sync::atomic::{AtomicBool, Ordering};

    struct ReadyErrorThenPanicDrop(Option<DmlExecutionError>);
    impl Future for ReadyErrorThenPanicDrop {
        type Output = Result<PhaseValue, DmlExecutionError>;
        fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Err(self.0.take().unwrap()))
        }
    }
    impl Drop for ReadyErrorThenPanicDrop {
        fn drop(&mut self) {
            panic!("scripted original preparation Future Drop");
        }
    }
    #[test]
    fn actual_preparation_stores_typed_return_before_hostile_future_drop() {
        let exited = Arc::new(AtomicBool::new(false));
        let probe = Arc::clone(&exited);
        let failure = CowFailure::provider(ConnectorError::new(
            ConnectorErrorKind::PermissionDenied,
            "original provider denied",
        ))
        .with_exit_probe(move || probe.store(true, Ordering::SeqCst));
        let mut preparation = PhasePreparation {
            future: Some(Box::pin(ReadyErrorThenPanicDrop(Some(
                DmlExecutionError::Cow(failure),
            )))),
            result: None,
            busy: false,
        };
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || preparation.poll_returned(&mut cx)
            ))
            .is_err()
        );
        assert!(
            preparation.busy,
            "the actual preparation owner is poisoned by child Drop"
        );
        let Some(Err(DmlExecutionError::Cow(original))) = preparation.result.as_ref() else {
            panic!("original typed return must be stable");
        };
        assert!(
            original
                .borrowed_presentation(None)
                .contains("PermissionDenied: original provider denied")
        );
        assert!(
            !exited.load(Ordering::SeqCst),
            "client projection must not retire the original cause"
        );
        assert!(
            preparation.poll_returned(&mut cx).is_pending(),
            "a poisoned original F is never repolled"
        );
        drop(preparation);
        assert!(exited.load(Ordering::SeqCst));
    }
    #[test]
    fn actual_preparation_refusal_is_retained_through_borrowed_projection() {
        let exited = Arc::new(AtomicBool::new(false));
        let probe = Arc::clone(&exited);
        let failure = CowFailure::provider(ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            "same provider failure",
        ))
        .with_exit_probe(move || probe.store(true, Ordering::SeqCst));
        let mut preparation = PhasePreparation {
            future: Some(Box::pin(
                async move { Err(DmlExecutionError::Cow(failure)) },
            )),
            result: None,
            busy: false,
        };
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(preparation.poll_returned(&mut cx).is_ready());
        let original = preparation.result.as_ref().unwrap().as_ref().err().unwrap();
        let client = project_failure(original, "UPDATE exact_admitted_source SET x = 1", None);
        assert!(
            client
                .message()
                .contains("Unavailable: same provider failure")
        );
        assert!(!exited.load(Ordering::SeqCst));
        drop(preparation);
        assert!(exited.load(Ordering::SeqCst));
    }
}

#[cfg(test)]
mod affected_count_tests {
    use super::*;
    use crate::query_execution::{
        write_result::DecodedPreparedWriteSet,
        write_session::tests::{fixture_with_outcome, known_committed},
    };
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    fn selected_rows() -> ConnectorRowMutationSelection {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1_i64, 5, 9]))],
        )
        .unwrap();
        ConnectorRowMutationSelection::try_new(schema, vec![batch], 3, 4096).unwrap()
    }

    #[test]
    fn cow_action_count_keeps_selected_rows_across_larger_rewrite_output() {
        let selection = selected_rows();
        let fixture = fixture_with_outcome(1, 16, known_committed());
        fixture
            .session
            .receive_for_test(DecodedPreparedWriteSet::for_test(29, Vec::new()))
            .unwrap();
        let mut rows = MutationAffectedRows::default();
        rows.receive_selection(&selection).unwrap();
        rows.receive_write(&fixture.session).unwrap();
        let StatementActionValue::Ok(completion) =
            super::super::write_action::selected_command_completion(Ok(()), rows.get()).unwrap()
        else {
            panic!("COW success must preserve selected logical count")
        };
        assert_eq!(completion.affected_rows(), 3);
        assert_eq!(fixture.session.accumulated_row_count().unwrap(), 29);
        assert_eq!(fixture.session.finish_invocations(), 0);
    }

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
        assert!(rows.receive_selection(&selected_rows()).is_err());
        assert_eq!(rows.get(), Some(19));
        assert_eq!(fixture.session.finish_invocations(), 0);
    }
}
