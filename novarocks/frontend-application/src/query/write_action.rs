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

//! Governed write execution over the original statement-family authors.
//! Publication never runs inside a blocking worker.

use super::statement_computation::{StatementComputation, StatementNativeTail};
use super::statement_publication::StatementPublication;
use super::*;
use crate::dml::attempt::DmlPublicationAttempt;
use crate::query_execution::{
    compiler::PreparedDmlWriteAssembly,
    completion_facts::FrontendCompletionCustody,
    internal_consumer::write::ReadyWriteComputation,
    logical_execution::InitialComputationReservation,
    maintenance::io_admission::MetadataIoAdmission,
    write_session::{ConnectorWriteSession, WriteBeginReceiving},
};
use novarocks_query_application::{
    api::StatementNativeHealthFailure,
    session_control::{
        GovernedActionContext, GovernedStatementAction, GovernedStatementResidual,
        StatementActionValue,
    },
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMetadataCleanupResponsibility,
    ConnectorRequestContext, ExternalMutationEffect, ExternalMutationFinalization,
    LakePublicationDisposition,
};
use novarocks_workload_control::{WorkError, WorkScope};
use std::{
    sync::Mutex,
    task::{Context, Poll},
};

/// Internal statement-family authors provide preparation and compilation;
/// the shared action retains the original governed execution policy.
pub(crate) trait WriteStatementFamily: Send + Sync + 'static {
    type Input: Send + Sync + 'static;
    type Prepared: Send + Sync + 'static;

    fn prepare(
        input: Arc<Self::Input>,
    ) -> impl Future<Output = Result<Arc<Self::Prepared>, crate::dml::error::DmlExecutionError>> + Send;

    fn compile(
        prepared: Arc<Self::Prepared>,
        source: Arc<FrontendCompletionCustody>,
        options: QueryOptions,
    ) -> impl Future<Output = Result<PreparedDmlWriteAssembly, crate::dml::error::DmlExecutionError>>
    + Send;

    fn finalize(prepared: &Self::Prepared) -> Result<(), String>;
}

pub(crate) struct WriteStatementStart<P: WriteStatementFamily> {
    pub(crate) kernel: crate::query_execution::kernels::DmlExecutionKernel,
    pub(crate) root: WorkScope,
    pub(crate) io: Arc<MetadataIoAdmission>,
    pub(crate) initial: InitialComputationReservation,
    pub(crate) credentials:
        Arc<crate::query_execution::logical_execution::InitialComputationCredentials>,
    pub(crate) input: Arc<P::Input>,
    pub(crate) context: ConnectorRequestContext,
    pub(crate) session_receiving: Arc<Mutex<Option<Arc<ConnectorWriteSession>>>>,
    pub(crate) begin_receiving: Arc<WriteBeginReceiving>,
    pub(crate) sql: String,
    pub(crate) query_options: QueryOptions,
    pub(crate) attempt: DmlPublicationAttempt,
    pub(crate) launcher: Arc<dyn LogicalExecutionLauncher>,
    #[cfg(novarocks_dev_controls)]
    pub(crate) observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}

type Preparation<P> = Pin<
    Box<
        dyn Future<
                Output = Result<
                    Arc<<P as WriteStatementFamily>::Prepared>,
                    crate::dml::error::DmlExecutionError,
                >,
            > + Send,
    >,
>;
type Compilation = Pin<
    Box<
        dyn Future<Output = Result<PreparedDmlWriteAssembly, crate::dml::error::DmlExecutionError>>
            + Send,
    >,
>;
type ProviderClose = Pin<Box<dyn Future<Output = Result<(), StatementNativeHealthFailure>> + Send>>;
use novarocks_spi::connector::write_stack::{
    ConnectorWriteNeededReady, ConnectorWritePublicationCompletion,
};
pub(super) type WriteOutcome = Result<ConnectorWritePublicationCompletion, ConnectorError>;

async fn close_original(
    source: Arc<FrontendCompletionCustody>,
) -> Result<(), StatementNativeHealthFailure> {
    source.close().await
}

pub(crate) fn prepare<P: WriteStatementFamily>(
    start: WriteStatementStart<P>,
) -> Box<dyn GovernedStatementAction> {
    let WriteStatementStart {
        kernel,
        root,
        io,
        initial,
        credentials,
        input,
        context,
        session_receiving,
        begin_receiving,
        sql,
        query_options,
        attempt,
        launcher,
        #[cfg(novarocks_dev_controls)]
        observation,
    } = start;
    let computation = StatementComputation::new(
        root.clone(),
        initial,
        launcher,
        #[cfg(novarocks_dev_controls)]
        observation,
    );
    Box::new(WriteStatementAction::<P> {
        kernel,
        root,
        io: Some(io),
        _credentials: credentials,
        input: Some(input),
        context: Some(context),
        session_receiving,
        begin_receiving,
        preparation: None,
        preparation_result: None,
        prepared: None,
        compilation: None,
        assembly: None,
        compilation_error: None,
        provider: FrontendCompletionCustody::new(),
        provider_close: None,
        provider_closed: false,
        pre_stage_io_closed: false,
        computation,
        assembly_installed: false,
        native_tail: None,
        ready: None,
        publication: Some(StatementPublication::new(attempt)),
        sql,
        query_options,
        selected: None,
        affected_rows: None,
        close_failure: None,
    })
}

struct WriteStatementAction<P: WriteStatementFamily> {
    kernel: crate::query_execution::kernels::DmlExecutionKernel,
    root: WorkScope,
    io: Option<Arc<MetadataIoAdmission>>,
    _credentials: Arc<crate::query_execution::logical_execution::InitialComputationCredentials>,
    input: Option<Arc<P::Input>>,
    context: Option<ConnectorRequestContext>,
    session_receiving: Arc<Mutex<Option<Arc<ConnectorWriteSession>>>>,
    begin_receiving: Arc<WriteBeginReceiving>,
    preparation: Option<Preparation<P>>,
    preparation_result: Option<Result<Arc<P::Prepared>, crate::dml::error::DmlExecutionError>>,
    prepared: Option<Arc<P::Prepared>>,
    compilation: Option<Compilation>,
    assembly: Option<PreparedDmlWriteAssembly>,
    compilation_error: Option<crate::dml::error::DmlExecutionError>,
    provider: Arc<FrontendCompletionCustody>,
    provider_close: Option<ProviderClose>,
    provider_closed: bool,
    pre_stage_io_closed: bool,
    computation: StatementComputation<PreparedDmlWriteAssembly>,
    assembly_installed: bool,
    native_tail: Option<StatementNativeTail>,
    ready: Option<ReadyWriteComputation>,
    publication: Option<StatementPublication>,
    sql: String,
    query_options: QueryOptions,
    selected: Option<Result<(), QueryServiceError>>,
    affected_rows: Option<u64>,
    close_failure: Option<StatementNativeHealthFailure>,
}

impl<P: WriteStatementFamily> WriteStatementAction<P> {
    fn poll_computation(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        if self.native_tail.is_some() {
            return Poll::Ready(Ok(()));
        }
        if !self.assembly_installed {
            let root_owner = owner
                .execution_owner_slot()
                .as_ref()
                .ok_or_else(|| internal_error("write statement lost its original Root owner"))?;
            match self
                .computation
                .poll_admit(self.io.as_ref().unwrap(), root_owner, cx)
            {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result?,
            }
            self.computation
                .install_assembly(self.assembly.take().ok_or_else(|| {
                    internal_error("write statement has no original frozen assembly")
                })?)?;
            self.assembly_installed = true;
        }
        if self.ready.is_none() {
            match self.computation.poll_ready(
                self.kernel.write_runtime().map_err(internal_error)?,
                self.io.as_ref().unwrap(),
                owner,
                cx,
            ) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result?,
            }
            self.ready = self.computation.take_ready();
            if self.ready.is_none() {
                return Poll::Ready(Err(internal_error(
                    "write statement has no actual ready result",
                )));
            }
        }
        match self.computation.poll_ready_local_exit(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(health)) => {
                self.close_failure = Some(health);
                return Poll::Pending;
            }
            Poll::Ready(Ok(())) => {}
        }
        let session = self.session();
        let ready = self.ready.as_ref();
        match self
            .computation
            .transfer_native_tail_then(&mut self.native_tail, || {
                let session =
                    session.ok_or_else(|| internal_error("write Ready has no original session"))?;
                let ready = ready
                    .ok_or_else(|| internal_error("write statement has no actual Ready write"))?;
                if !Arc::ptr_eq(&ready.session, &session) || !ready.accepted.belongs_to(&session) {
                    return Err(internal_error("write Ready replaced the original session"));
                }
                session.freeze_received_row_count().map_err(|error| {
                    QueryServiceError::from_failure_cause(
                        novarocks_failure_contract::SharedFailureCause::new(error),
                    )
                })
            }) {
            Ok(rows) => self.affected_rows = Some(rows?),
            Err(health) => {
                self.close_failure = Some(health);
                return Poll::Pending;
            }
        }
        Poll::Ready(Ok(()))
    }
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
    fn poll_source(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        let cancellation =
            QueryCancellationView::governed(owner.cancellation().clone(), owner.timeout_ms());
        let root_owner = owner
            .execution_owner_slot()
            .as_ref()
            .ok_or_else(|| internal_error("write statement lost its original Root owner"))?;
        if !root_owner.is_for_scope(&self.root) {
            return Poll::Ready(Err(internal_error(
                "write statement has a foreign original Root",
            )));
        }
        let io = self.io.as_ref().expect("same original IO source");
        match io.poll_progress(root_owner, cx) {
            Ok(false) => return Poll::Pending,
            Ok(true) => {}
            Err(error) => self.fail(internal_error(error.to_string())),
        }
        if self.preparation.is_none()
            && self.preparation_result.is_none()
            && self.prepared.is_none()
            && self.selected.is_none()
        {
            if let Some(reason) = cancellation.reason() {
                self.fail(cancellation_error(reason));
            } else {
                self.preparation = Some(Box::pin(P::prepare(Arc::clone(
                    self.input.as_ref().expect("original preparation input"),
                ))));
            }
        }
        if let Some(future) = self.preparation.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => self.preparation_result = Some(result),
            }
            drop(self.preparation.take());
        }
        if let Some(result) = self.preparation_result.take() {
            match result {
                Ok(prepared) => self.prepared = Some(prepared),
                Err(error) => self.fail(routed_execution_service_error(
                    dml_result::<()>(Err(error.into_dml_error(Some(&self.sql)))).unwrap_err(),
                )),
            }
        }
        if let Some(prepared) = self.prepared.as_ref()
            && self.assembly.is_none()
            && self.compilation.is_none()
            && !self.provider_closed
            && self.selected.is_none()
        {
            self.compilation = Some(Box::pin(P::compile(
                Arc::clone(prepared),
                Arc::clone(&self.provider),
                self.query_options.clone(),
            )));
        }
        if let Some(future) = self.compilation.as_mut() {
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(value)) => self.assembly = Some(value),
                Poll::Ready(Err(error)) => self.compilation_error = Some(error),
            }
            if self.provider.local_health_failure().is_some() {
                return Poll::Pending;
            }
            drop(self.compilation.take());
        }
        if let Some(error) = self.compilation_error.take() {
            self.fail(routed_execution_service_error(
                dml_result::<()>(Err(error.into_dml_error(Some(&self.sql)))).unwrap_err(),
            ));
        }
        if self.preparation.is_none() && !self.provider_closed {
            if self.provider_close.is_none() {
                self.provider_close = Some(Box::pin(close_original(Arc::clone(&self.provider))));
            }
            let future = self.provider_close.as_mut().unwrap();
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(_)) => return Poll::Pending,
                Poll::Ready(Ok(())) => self.provider_closed = true,
            }
            drop(self.provider_close.take());
        }
        if !self.pre_stage_io_closed {
            // Metadata's old physical actions have actually exited before a
            // later Native manifest extends this Root's dependency union.
            let root_owner = owner.execution_owner_slot().as_ref().unwrap();
            match self.io.as_ref().unwrap().poll_quiescent(root_owner, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => self.pre_stage_io_closed = true,
                Poll::Ready(Err(error)) => {
                    self.pre_stage_io_closed = true;
                    self.fail(internal_error(error.to_string()));
                }
            }
        }
        if self.selected.is_none() {
            match self.poll_computation(owner, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => self.fail(error),
                Poll::Ready(Ok(())) => {}
            }
        }
        if self.selected.is_some() && self.ready.is_none() {
            // Refusal retains both actual CPU and Native observers until
            // their original exits; it cannot authorize an early abort.
            match self.computation.poll_refused_exit(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(health)) => {
                    self.close_failure = Some(health);
                    return Poll::Pending;
                }
                Poll::Ready(Ok(())) => {}
            }
        }
        // Only this original session's Call crosses the publication frontier.
        // Its business result and later physical retirement remain separate.
        if self.publication.as_ref().unwrap().call.is_none()
            && self.selected.is_none()
            && let Some(reason) = cancellation.reason()
        {
            self.fail(cancellation_error(reason));
        }
        let session = self.session();
        let abort = self.selected.is_some();
        let context = self.context.as_ref().expect("original write context");
        if self
            .publication
            .as_mut()
            .unwrap()
            .poll_settle(session.as_ref(), context, abort, cx)
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
            self.publication.as_mut().unwrap().local_finalization = Some(P::finalize(
                self.prepared.as_ref().expect("committed original write"),
            ));
        }
        if self.selected.is_none() {
            self.selected = Some(
                self.publication
                    .as_ref()
                    .unwrap()
                    .select(cancellation.reason()),
            );
        }
        self.publication.as_mut().unwrap().freeze_terminal()?;
        let root_owner = owner.execution_owner_slot().as_ref().unwrap();
        match self.io.as_ref().unwrap().poll_quiescent(root_owner, cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => self.fail(internal_error(e.to_string())),
            Poll::Ready(Ok(())) => {}
        }
        if self.selected.is_none() {
            self.selected = Some(Ok(()));
        }
        Poll::Ready(selected_command_completion(
            self.selected.take().unwrap(),
            self.affected_rows,
        ))
    }
}

/// Only an already selected business success may expose the original count.
/// A missing count is a contract refusal, never an implicit zero-row success.
pub(super) fn selected_command_completion(
    selected: Result<(), QueryServiceError>,
    affected_rows: Option<u64>,
) -> Result<StatementActionValue, QueryServiceError> {
    selected?;
    let rows = affected_rows
        .ok_or_else(|| internal_error("successful write has no original affected-row count"))?;
    Ok(StatementActionValue::Ok(
        novarocks_query_application::session_control::StatementCommandCompletion::new(rows),
    ))
}

pub(super) fn disposition(outcome: &WriteOutcome) -> LakePublicationDisposition {
    match outcome {
        Ok(ConnectorWritePublicationCompletion::KnownCommitted { .. }) => {
            LakePublicationDisposition::KnownCommitted
        }
        Ok(ConnectorWritePublicationCompletion::KnownUncommitted { .. }) => {
            LakePublicationDisposition::KnownUncommitted
        }
        Ok(ConnectorWritePublicationCompletion::CommitUnknown { .. }) | Err(_) => {
            LakePublicationDisposition::CommitUnknown
        }
    }
}
/// Keep the originating provider fact and same shared source. A Provider
/// refusal is not a compiler-control result, even when its cause is control.
pub(super) fn publication_failure<E>(error: &E) -> QueryServiceError
where
    E: novarocks_failure_contract::FailureProjection
        + std::error::Error
        + Clone
        + Send
        + Sync
        + 'static,
{
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(actual) = source {
        if let Some(cause) = actual.downcast_ref::<novarocks_failure_contract::SharedFailureCause>()
        {
            return QueryServiceError::from_failure_cause(cause.clone());
        }
        source = actual.source();
    }
    QueryServiceError::from_failure_cause(novarocks_failure_contract::SharedFailureCause::new(
        error.clone(),
    ))
}

pub(super) fn select_needed_publication(
    needed: &ConnectorWriteNeededReady,
    finalization: Option<&Result<(), String>>,
    cancellation: Option<QueryCancellationReason>,
) -> Result<(), QueryServiceError> {
    if let ExternalMutationFinalization::Failed(failure) = needed.finalization() {
        return Err(publication_failure(failure));
    }
    if let Some(Err(error)) = finalization {
        return Err(internal_error(error.clone()));
    }
    match (needed.effect(), cancellation) {
        (ExternalMutationEffect::NoOp, Some(reason)) => Err(cancellation_error(reason)),
        _ => Ok(()),
    }
}
pub(super) fn select_publication(
    outcome: &WriteOutcome,
    finalization: Option<&Result<(), String>>,
    cancellation: Option<QueryCancellationReason>,
) -> Result<(), QueryServiceError> {
    match outcome {
        Ok(ConnectorWritePublicationCompletion::KnownCommitted { needed, .. }) => {
            select_needed_publication(needed, finalization, cancellation)
        }
        Ok(ConnectorWritePublicationCompletion::KnownUncommitted { failure, .. })
        | Ok(ConnectorWritePublicationCompletion::CommitUnknown { failure, .. }) => {
            Err(publication_failure(failure))
        }
        Err(error) => Err(publication_failure(error)),
    }
}
impl<P: WriteStatementFamily> GovernedStatementAction for WriteStatementAction<P> {
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        self.poll_source(owner, cx).map(|result| {
            result.map_err(
                |error| match self.publication.as_ref().unwrap().attempt.terminal() {
                    Some(terminal) => error.attach_publication_terminal(terminal.clone()),
                    None => error,
                },
            )
        })
    }
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.close_failure
            .clone()
            .or_else(|| self.begin_receiving.local_health_failure())
            .or_else(|| self.provider.local_health_failure())
            .or_else(|| self.computation.local_health_failure())
            .or_else(|| {
                self.native_tail
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
        if target.is_some() {
            return Err(health("write statement residual target is occupied"));
        }
        let session = self.session();
        if session.is_none() && self.native_tail.is_none() {
            return Ok(());
        }
        // The original session checks accepted ownership, not a public error
        // classification. A later accepted/Unknown publication retains its
        // complete envelope. QA Drops this entire Action before polling residual.
        let formation = match session.as_ref() {
            Some(session) => {
                if session.has_no_accepted_formation().map_err(|error| {
                    StatementNativeHealthFailure::actual_exit_unproven(
                        "write formation ownership could not be established",
                        Arc::new(error),
                    )
                })? {
                    self.computation.refused_formation_bytes()
                } else {
                    None
                }
            }
            None => None,
        };
        let mut residual = Box::new(WriteCleanupResidual {
            formation,
            publication: None,
            native_tail: None,
            continuation_error: None,
            health: self.computation.retained_health(),
            session: None,
            io: None,
        });
        residual.publication = self.publication.take();
        residual.native_tail = self.native_tail.take();
        residual.session = session;
        residual.io = self.io.take();
        *target = Some(residual);
        // The stable residual is installed before releasing computation/result
        // aliases. It does not retain the statement's result window.
        drop(self.ready.take());
        drop(self.input.take());
        drop(self.prepared.take());
        drop(self.context.take());
        let retired = self
            .session_receiving
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        drop(retired);
        Ok(())
    }
}
fn health(detail: &'static str) -> StatementNativeHealthFailure {
    StatementNativeHealthFailure::actual_exit_unproven(
        detail,
        Arc::new(ConnectorError::new(ConnectorErrorKind::Internal, detail)),
    )
}
struct WriteCleanupResidual {
    // Original stage amount only; never a new grant. Consumed after actual
    // Action destruction and Provider/MIO retirement, before unknown custody.
    formation: Option<u64>,
    publication: Option<StatementPublication>,
    native_tail: Option<StatementNativeTail>,
    continuation_error: Option<WorkError>,
    health: StatementNativeHealthFailure,
    session: Option<Arc<ConnectorWriteSession>>,
    io: Option<Arc<MetadataIoAdmission>>,
}
impl GovernedStatementResidual for WriteCleanupResidual {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if let Some(io) = &self.io {
            if let Err(error) = io.poll_continuing(cx) {
                self.continuation_error = Some(error);
                return Poll::Ready(Err(self.health.clone()));
            }
            if let Some(health) = io.health_failure() {
                return Poll::Ready(Err(health));
            }
        }
        let mut pending = false;
        if let Some(publication) = self.publication.as_mut() {
            pending |= publication.poll_retirement(cx).is_pending();
            if let Some(health) = publication.local_health_failure() {
                return Poll::Ready(Err(health));
            }
        }
        if let Some(tail) = self.native_tail.as_mut()
            && tail.poll_exit(cx).is_pending()
        {
            if let Some(health) = tail.local_health_failure() {
                return Poll::Ready(Err(health));
            }
            pending = true;
        }
        if pending {
            return Poll::Pending;
        }
        if let Some(io) = &self.io {
            match io.poll_quiescent_continuing(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    self.continuation_error = Some(error);
                    return Poll::Ready(Err(self.health.clone()));
                }
                Poll::Ready(Ok(())) => {}
            }
        }
        if self.formation.is_some() {
            let Some(io) = self.io.as_ref() else {
                return Poll::Ready(Err(health("write formation lost its original IO owner")));
            };
            let notice = match io.retire_unaccepted_formation_in(&mut self.formation) {
                Ok(notice) => notice,
                Err(WorkError::NotReady) => return Poll::Pending,
                Err(error) => {
                    self.continuation_error = Some(error);
                    return Poll::Ready(Err(self.health.clone()));
                }
            };
            if let Some(notice) = notice {
                notice.notify();
            }
        }
        match self
            .session
            .as_ref()
            .map(|session| session.cleanup_responsibility())
        {
            Some(Ok(ConnectorMetadataCleanupResponsibility::Retained)) => return Poll::Pending,
            Some(Err(_)) => return Poll::Pending,
            _ => {}
        }
        drop(self.publication.take());
        drop(self.native_tail.take());
        drop(self.session.take());
        drop(self.io.take());
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use novarocks_spi::connector::{
        ConnectorMutationFailure, ConnectorMutationFailureKind, ExternalMutationOutcome,
    };

    // Test-only composition within the query module. The observer comes from
    // the original supervisor fixture; this constructor creates no observer.
    pub(in crate::query) fn residual_without_session(
        tail: StatementNativeTail,
    ) -> Box<dyn GovernedStatementResidual> {
        Box::new(WriteCleanupResidual {
            formation: None,
            publication: None,
            native_tail: Some(tail),
            continuation_error: None,
            health: health("original no-session residual health cause"),
            session: None,
            io: None,
        })
    }

    #[test]
    fn exact_commit_cleanup_and_business_finalization_are_independent() {
        let mut outcome = crate::query_execution::write_session::tests::known_committed();
        let ExternalMutationOutcome::KnownCommitted { cleanup, .. } = &mut outcome else {
            unreachable!()
        };
        *cleanup = ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Unavailable,
            "exact owned cleanup remains",
        ));
        assert!(
            select_publication(
                &Ok(ConnectorWritePublicationCompletion::from_outcome(outcome)),
                Some(&Ok(())),
                None
            )
            .is_ok()
        );
        let mut outcome = crate::query_execution::write_session::tests::known_committed();
        let ExternalMutationOutcome::KnownCommitted { finalization, .. } = &mut outcome else {
            unreachable!()
        };
        *finalization = ExternalMutationFinalization::Failed(ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Unavailable,
            "necessary cache barrier failed",
        ));
        let error = select_publication(
            &Ok(ConnectorWritePublicationCompletion::from_outcome(outcome)),
            Some(&Ok(())),
            Some(QueryCancellationReason::ClientDisconnected),
        )
        .unwrap_err();
        assert!(error.message().contains("necessary cache barrier failed"));
    }

    #[test]
    fn no_op_does_not_gain_applied_commit_late_cancel_immunity() {
        let applied = Ok(ConnectorWritePublicationCompletion::from_outcome(
            crate::query_execution::write_session::tests::known_committed(),
        ));
        assert!(
            select_publication(
                &applied,
                Some(&Ok(())),
                Some(QueryCancellationReason::ClientDisconnected)
            )
            .is_ok()
        );
        let mut noop = crate::query_execution::write_session::tests::known_committed();
        let ExternalMutationOutcome::KnownCommitted { effect, .. } = &mut noop else {
            unreachable!()
        };
        *effect = ExternalMutationEffect::NoOp;
        assert!(
            select_publication(
                &Ok(ConnectorWritePublicationCompletion::from_outcome(noop)),
                Some(&Ok(())),
                Some(QueryCancellationReason::ClientDisconnected)
            )
            .is_err()
        );
    }

    #[test]
    fn residual_keeps_the_actual_session_until_its_provider_disposal_observation() {
        let fixture = crate::query_execution::write_session::tests::fixture_with_outcome(
            1,
            16,
            crate::query_execution::write_session::tests::known_committed(),
        );
        let same = Arc::downgrade(&fixture.session);
        let mut residual = WriteCleanupResidual {
            formation: None,
            publication: None,
            native_tail: None,
            continuation_error: None,
            health: health("component residual original health cause"),
            session: Some(Arc::clone(&fixture.session)),
            io: None,
        };
        drop(fixture);
        assert!(
            same.upgrade().is_some(),
            "original session is owned after business observer exit"
        );
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(matches!(
            residual.poll_retirement(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert!(
            same.upgrade().is_none(),
            "actual same session exited after its author reported disposed"
        );
    }
}

#[cfg(test)]
mod original_publication_failure_tests {
    use super::*;
    use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
    use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorMutationFailure};

    #[test]
    fn selected_publication_refusal_preserves_the_actual_provider_source() {
        let error = ConnectorError::from_domain(
            ConnectorErrorKind::ResourceExhausted,
            novarocks_workload_control::WorkError::Capacity("original publication resource"),
        );
        let source = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap()
            .clone();
        let selected = select_publication(&Err(error.clone()), None, None).unwrap_err();
        assert_eq!(selected.failure_cause(), Some(&source));
        assert_eq!(
            selected.failure_cause().unwrap().failure_fact(),
            error.failure_fact()
        );
        assert!(selected.compile_control_error().is_none());
        let failure = ConnectorMutationFailure::from_connector(error);
        let selected = publication_failure(&failure);
        assert_eq!(selected.failure_cause(), Some(&source));
        assert!(selected.compile_control_error().is_none());
    }
}

#[cfg(test)]
mod command_completion_tests {
    use super::*;
    #[test]
    fn missing_count_is_not_zero_and_refusal_keeps_its_original_error() {
        assert!(selected_command_completion(Ok(()), None).is_err());
        let failure = internal_error("original unsuccessful publication");
        let Err(selected) = selected_command_completion(Err(failure), Some(17)) else {
            panic!("an unsuccessful publication cannot become command success")
        };
        assert!(
            selected
                .message()
                .contains("original unsuccessful publication")
        );
    }
    #[test]
    fn successful_empty_write_preserves_its_explicit_zero() {
        let StatementActionValue::Ok(rows) = selected_command_completion(Ok(()), Some(0)).unwrap()
        else {
            panic!("successful empty write must carry its explicit count")
        };
        assert_eq!(rows.affected_rows(), 0);
    }
}
