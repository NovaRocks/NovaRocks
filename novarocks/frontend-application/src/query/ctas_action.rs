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

//! The original governed CTAS statement and its distinct publication frontiers.

use super::statement_computation::{StatementComputation, StatementNativeTail};
use super::*;
use crate::dml::{
    attempt::{
        DmlPublicationAdjudication, DmlPublicationAdjudicationOutcome, DmlPublicationAttempt,
        DmlPublicationFinalization,
    },
    error::DmlError,
};
use crate::query_execution::{
    compiler::PreparedDmlWriteAssembly,
    completion_facts::FrontendCompletionCustody,
    dml::ctas::*,
    internal_consumer::write::ReadyWriteComputation,
    logical_execution::{InitialComputationCredentials, InitialComputationReservation},
    maintenance::io_admission::MetadataIoAdmission,
    write_session::{ConnectorWriteSession, WriteBeginReceiving, WritePublicationCall},
};
use novarocks_query_application::{
    api::StatementNativeHealthFailure,
    session_control::{
        GovernedActionContext, GovernedStatementAction, GovernedStatementResidual,
        StatementActionValue,
    },
    sql::dml_admission::DmlAdmissionError,
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorMetadataCleanupResponsibility, ConnectorRequestContext,
    ConnectorWriteAbortOutcome, CreatePolicy, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization, LakePublicationDisposition, LakePublicationFamily,
    LakePublicationId, LakePublicationStatementTag, LakePublicationTarget,
};
use novarocks_workload_control::{WorkError, WorkScope};
use std::{
    sync::Mutex,
    task::{Context, Poll},
};

use novarocks_spi::connector::write_stack::{
    ConnectorWriteNeededReady, ConnectorWritePublicationCompletion,
};
type WriteOutcome = Result<ConnectorWritePublicationCompletion, ConnectorError>;
/// Original receiving storage exists before its first foreign poll and Drop.
struct PhaseSlot<T> {
    future: Option<Pin<Box<dyn Future<Output = T> + Send>>>,
    result: Option<T>,
    started: bool,
    settled: bool,
    busy: bool,
}
impl<T> Default for PhaseSlot<T> {
    fn default() -> Self {
        Self {
            future: None,
            result: None,
            started: false,
            settled: false,
            busy: false,
        }
    }
}
impl<T> PhaseSlot<T> {
    fn start(&mut self, future: impl Future<Output = T> + Send + 'static) {
        assert!(!self.started, "one original CTAS phase");
        self.started = true;
        self.future = Some(Box::pin(future));
    }
    fn poll_settled(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.busy {
            return Poll::Pending;
        }
        if self.settled {
            return Poll::Ready(());
        }
        let Some(original) = self.future.as_mut() else {
            return Poll::Pending;
        };
        self.busy = true;
        match original.as_mut().poll(cx) {
            Poll::Pending => {
                self.busy = false;
                return Poll::Pending;
            }
            Poll::Ready(value) => self.result = Some(value),
        }
        drop(self.future.take());
        self.settled = true;
        self.busy = false;
        Poll::Ready(())
    }
    fn take(&mut self) -> T {
        self.result
            .take()
            .expect("settled original CTAS phase result")
    }
}
struct CatalogInput {
    kernel: crate::query_execution::kernels::DmlExecutionKernel,
    action: Arc<dyn CtasPreparedCatalogAction>,
    receiving: Arc<GovernedCtasControlReceiving>,
}
async fn stage_original(input: Arc<CatalogInput>) -> Result<StandardCtasStageOutcome, CtasFailure> {
    input
        .kernel
        .stage_governed_ctas_target_async(input.action.as_ref(), &input.receiving)
        .await
}
async fn publish_original(
    input: Arc<CatalogInput>,
) -> Result<StandardCtasPublishOutcome, CtasFailure> {
    input
        .kernel
        .publish_governed_ctas_async(input.action.as_ref(), &input.receiving)
        .await
}
struct AdjudicationInput {
    kernel: crate::query_execution::kernels::DmlExecutionKernel,
    target: Arc<dyn CtasPreparedTarget>,
    evidence: ExternalMutationEvidence,
    receiving: Arc<GovernedCtasControlReceiving>,
}
async fn adjudicate_original(
    input: Arc<AdjudicationInput>,
) -> Result<StandardCtasPublicationAdjudicationOutcome, CtasFailure> {
    input
        .kernel
        .adjudicate_governed_ctas_publication_async(
            input.target.as_ref(),
            input.evidence.clone(),
            &input.receiving,
        )
        .await
}
async fn close_original(
    source: Arc<FrontendCompletionCustody>,
) -> Result<(), StatementNativeHealthFailure> {
    source.close().await
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Source,
    Stage,
    PrepareWrite,
    Admit,
    Compile,
    Compute,
    Seal,
    Bind,
    Publish,
    Adjudicate,
    Close,
    Done,
}
struct CtasAction {
    source: Option<Box<CtasSource>>,
}
struct CtasSource {
    kernel: crate::query_execution::kernels::DmlExecutionKernel,
    root: WorkScope,
    io: Arc<MetadataIoAdmission>,
    _credentials: Arc<InitialComputationCredentials>,
    context: ConnectorRequestContext,
    sql: String,
    preflight: PreparedCtasTargetPreflight,
    source_input: Option<Arc<GovernedCtasSourceInput>>,
    controls: Arc<GovernedCtasControlReceiving>,
    provider: Arc<FrontendCompletionCustody>,
    begin: Arc<WriteBeginReceiving>,
    sessions: Arc<Mutex<Option<Arc<ConnectorWriteSession>>>>,
    publication_id: LakePublicationId,
    policy: CreatePolicy,
    attempt: DmlPublicationAttempt,
    phase: Phase,
    prepare_source: PhaseSlot<Result<Arc<PreparedCtasSource>, CtasFailure>>,
    prepared_source: Option<Arc<PreparedCtasSource>>,
    stage_input: Option<Arc<CatalogInput>>,
    stage: PhaseSlot<Result<StandardCtasStageOutcome, CtasFailure>>,
    stage_unresolved: bool,
    target_disposed: bool,
    target_disposal_observed: bool,
    native_launch_attempted: bool,
    target_abort:
        PhaseSlot<Result<novarocks_spi::connector::ConnectorStagedCreateAbortOutcome, CtasFailure>>,
    continuation_error: Option<WorkError>,
    cleanup_refusal: Option<CtasFailure>,
    target: Option<PreparedStandardCtasTarget>,
    stage_effect: Option<(
        novarocks_spi::connector::ConnectorStagedCreateReceipt,
        ExternalMutationFinalization,
    )>,
    write_input: Option<Arc<GovernedCtasWriteInput>>,
    prepare_write: PhaseSlot<Result<Arc<PreparedGovernedCtasWrite>, CtasFailure>>,
    prepared_write: Option<Arc<PreparedGovernedCtasWrite>>,
    compile: PhaseSlot<Result<PreparedDmlWriteAssembly, crate::dml::error::DmlExecutionError>>,
    assembly: Option<PreparedDmlWriteAssembly>,
    computation: StatementComputation<PreparedDmlWriteAssembly>,
    ready: Option<ReadyWriteComputation>,
    tail: Option<StatementNativeTail>,
    seal: Option<WritePublicationCall>,
    seal_refusal: Option<ConnectorError>,
    seal_outcome: Option<WriteOutcome>,
    seal_needed: Option<Arc<ConnectorWriteNeededReady>>,
    seal_abort: Option<Result<ConnectorWriteAbortOutcome, ConnectorError>>,
    seal_settled: bool,
    terminal_context: Option<ConnectorRequestContext>,
    seal_reconciled: bool,
    publish_input: Option<Arc<CatalogInput>>,
    publish: PhaseSlot<Result<StandardCtasPublishOutcome, CtasFailure>>,
    publication_boundary_received: bool,
    publication_failure: Option<CtasFailure>,
    adjudication_input: Option<Arc<AdjudicationInput>>,
    adjudication: PhaseSlot<Result<StandardCtasPublicationAdjudicationOutcome, CtasFailure>>,
    adjudication_token: Option<DmlPublicationAdjudication>,
    provider_close: PhaseSlot<Result<(), StatementNativeHealthFailure>>,
    provider_closed: bool,
    target_cleanup: PhaseSlot<Result<ConnectorMetadataCleanupResponsibility, CtasFailure>>,
    selected: Option<Result<(), QueryServiceError>>,
    affected_rows: Option<u64>,
    disposition: Option<LakePublicationDisposition>,
    busy: bool,
    completed: bool,
    health: StatementNativeHealthFailure,
    close_failure: Option<StatementNativeHealthFailure>,
}

pub(crate) fn prepare(
    kernel: crate::query_execution::kernels::DmlExecutionKernel,
    input: CtasStatementInput,
) -> Result<Box<dyn GovernedStatementAction>, DmlError> {
    let command = CtasCommand::from_typed(&input.statement, &input.sql).map_err(|error| {
        DmlError::admit(DmlAdmissionError::CreateTableUnsupportedForm.to_user_error(
            &input.sql,
            error.span,
            error.message,
        ))
    })?;
    let root = input
        .context
        .execution()
        .result_capacity()
        .ok_or_else(|| DmlError::executor("CTAS has no original Root result capacity"))?
        .root_scope()
        .clone();
    let current_catalog = input
        .context
        .session()
        .current_catalog()
        .map(ToOwned::to_owned);
    let current_database = input.context.session().current_database().to_string();
    let CtasTargetPreflightOutcome::Ready(preflight) = kernel
        .preflight_governed_ctas_target(&command, current_catalog.as_deref(), &current_database)
        .map_err(|error| error.into_governed_dml_error(&input.sql))?;
    let policy = if command.if_not_exists {
        CreatePolicy::NoOpIfExists
    } else {
        CreatePolicy::FailIfExists
    };
    let profile = preflight
        .governed_io_profile(&kernel)
        .map_err(|error| error.into_governed_dml_error(&input.sql))?;
    let source_bytes = profile.geometry().metadata_bytes().get();
    let io = MetadataIoAdmission::new(root.clone(), profile, source_bytes)
        .map_err(|error| DmlError::executor(error.to_string()))?;
    let initial = InitialComputationReservation::new(
        input
            .launcher
            .reserve_identity()
            .map_err(|error| DmlError::executor(error.to_string()))?,
    )
    .map_err(|error| DmlError::executor(error.to_string()))?;
    let credentials = initial.credentials();
    let context = initial
        .connector_request_context(
            kernel
                .connector_request_context_for_execution(
                    Some(&input.query_options),
                    input.context.execution(),
                )
                .map_err(DmlError::executor)?,
            #[cfg(novarocks_dev_controls)]
            input.context.execution().test_statement_nomination(),
        )
        .map_err(|error| DmlError::executor(error.to_string()))?
        .with_io_admission(io.clone());
    let provider = FrontendCompletionCustody::new();
    let controls = GovernedCtasControlReceiving::new();
    let sessions = Arc::new(Mutex::new(None));
    let begin = WriteBeginReceiving::new(sessions.clone());
    let publication_id = LakePublicationId::new_v7();
    let attempt = DmlPublicationAttempt::new(
        publication_id,
        LakePublicationFamily::Ctas,
        LakePublicationTarget::try_new(
            preflight.facts.instance_id.clone(),
            preflight.facts.target_namespace.clone(),
            Some(preflight.facts.target_table.clone()),
            None,
        )
        .map_err(DmlError::executor)?,
        Some(LakePublicationStatementTag::try_new("ctas".to_string()).map_err(DmlError::executor)?),
    );
    let source_input = Arc::new(GovernedCtasSourceInput {
        kernel: kernel.clone(),
        preflight: preflight.handle.clone(),
        request: PrepareCtasSourceRequest {
            command,
            current_catalog,
            current_database,
            query_options: Some(input.query_options),
            execution: input.context.execution().clone(),
        },
        context: context.clone(),
        provider: provider.clone(),
    });
    let computation = StatementComputation::new(
        root.clone(),
        initial,
        input.launcher,
        #[cfg(novarocks_dev_controls)]
        input.observation,
    );
    Ok(Box::new(CtasAction {
        source: Some(Box::new(CtasSource {
            kernel,
            root,
            io,
            _credentials: credentials,
            context,
            sql: input.sql,
            preflight,
            source_input: Some(source_input),
            controls,
            provider,
            begin,
            sessions,
            publication_id,
            policy,
            attempt,
            phase: Phase::Source,
            prepare_source: PhaseSlot::default(),
            prepared_source: None,
            stage_input: None,
            stage: PhaseSlot::default(),
            stage_unresolved: false,
            target_disposed: false,
            target_disposal_observed: false,
            native_launch_attempted: false,
            target_abort: PhaseSlot::default(),
            continuation_error: None,
            cleanup_refusal: None,
            target: None,
            stage_effect: None,
            write_input: None,
            prepare_write: PhaseSlot::default(),
            prepared_write: None,
            compile: PhaseSlot::default(),
            assembly: None,
            computation,
            ready: None,
            tail: None,
            seal: None,
            seal_refusal: None,
            seal_outcome: None,
            seal_needed: None,
            seal_abort: None,
            seal_settled: false,
            terminal_context: None,
            seal_reconciled: false,
            publish_input: None,
            publish: PhaseSlot::default(),
            publication_boundary_received: false,
            publication_failure: None,
            adjudication_input: None,
            adjudication: PhaseSlot::default(),
            adjudication_token: None,
            provider_close: PhaseSlot::default(),
            provider_closed: false,
            target_cleanup: PhaseSlot::default(),
            selected: None,
            affected_rows: None,
            disposition: None,
            busy: false,
            completed: false,
            health: health(),
            close_failure: None,
        })),
    }))
}

fn service(error: DmlError) -> QueryServiceError {
    routed_execution_service_error(dml_result::<()>(Err(error)).unwrap_err())
}
fn terminal_failure(
    failure: CtasFailure,
    terminal: novarocks_spi::connector::LakePublicationTerminal,
    sql: &str,
) -> QueryServiceError {
    service(failure.into_governed_dml_error(sql)).attach_publication_terminal(terminal)
}
impl CtasSource {
    fn fail(&mut self, error: QueryServiceError) {
        if self.selected.is_none() {
            self.selected = Some(Err(error));
        }
        self.phase = Phase::Close;
    }
    fn failure(&mut self, error: CtasFailure) {
        self.fail(service(error.into_governed_dml_error(&self.sql)));
    }
    fn session(&self) -> Option<Arc<ConnectorWriteSession>> {
        self.sessions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
    fn check_cancel(&mut self, cancellation: &QueryCancellationView) -> bool {
        if let Some(reason) = cancellation.reason() {
            self.fail(cancellation_error(reason));
            true
        } else {
            false
        }
    }
    fn pre_dispatch_terminal(&mut self) {
        if self.attempt.terminal().is_none() {
            if self.attempt.terminal_pre_dispatch_uncommitted().is_err() {
                self.close_failure = Some(self.health.clone());
            }
            self.disposition = Some(LakePublicationDisposition::KnownUncommitted);
        }
    }
    fn receive_publication_boundary(&mut self) -> bool {
        if !self.publication_boundary_received {
            if self.attempt.mark_dispatch_possible().is_err() {
                self.close_failure = Some(self.health.clone());
                return false;
            }
            self.publication_boundary_received = true;
        }
        true
    }
    fn terminal_committed(
        &mut self,
        finalization: ExternalMutationFinalization,
        applied: bool,
        cancellation: &QueryCancellationView,
    ) {
        let finalized = match &finalization {
            ExternalMutationFinalization::Complete => DmlPublicationFinalization::Succeeded,
            ExternalMutationFinalization::Failed(_) => DmlPublicationFinalization::Failed,
        };
        if self.attempt.terminal_known_committed(finalized).is_err() {
            self.close_failure = Some(self.health.clone());
            return;
        }
        self.disposition = Some(LakePublicationDisposition::KnownCommitted);
        if !applied {
            // The final CREATE author proved NoOp; staged/sealed rows did not
            // become table rows through this statement.
            self.affected_rows = Some(0);
        }
        self.selected = Some(match finalization {
            ExternalMutationFinalization::Complete => {
                if !applied {
                    cancellation
                        .reason()
                        .map(cancellation_error)
                        .map_or(Ok(()), Err)
                } else {
                    Ok(())
                }
            }
            ExternalMutationFinalization::Failed(failure) => Err(terminal_failure(
                mutation_failure(failure),
                self.attempt.terminal().unwrap().clone(),
                &self.sql,
            )),
        });
        self.phase = Phase::Close;
    }
    fn terminal_unknown(&mut self, failure: CtasFailure) {
        if self.attempt.terminal_commit_unknown().is_err() {
            self.close_failure = Some(self.health.clone());
            return;
        }
        self.disposition = Some(LakePublicationDisposition::CommitUnknown);
        self.selected = Some(Err(terminal_failure(
            failure,
            self.attempt.terminal().unwrap().clone(),
            &self.sql,
        )));
        self.phase = Phase::Close;
    }
    fn poll_provider_close(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.provider_closed {
            return Poll::Ready(());
        }
        if !self.provider_close.started {
            self.provider_close
                .start(close_original(self.provider.clone()));
        }
        if self.provider_close.poll_settled(cx).is_pending() {
            return Poll::Pending;
        }
        match self.provider_close.take() {
            Ok(()) => self.provider_closed = true,
            Err(health) => {
                self.close_failure = Some(health);
                return Poll::Pending;
            }
        }
        Poll::Ready(())
    }
    fn poll_seal(
        &mut self,
        cancellation: &QueryCancellationView,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        if self.seal_settled {
            return Poll::Ready(());
        }
        if self.seal.is_none() {
            let Some(session) = self.session() else {
                self.seal_settled = true;
                return Poll::Ready(());
            };
            let prepared = if self.selected.is_some() {
                session
                    .prepare_abort_call(self.context.clone())
                    .map(|call| self.seal = Some(call))
            } else {
                // Retain the same terminal resolver BEFORE the original Call
                // releases its alias. A context clone grants no seal proof.
                match session.following_terminal_context(self.context.clone()) {
                    Ok(context) => self.terminal_context = Some(context),
                    Err(error) => {
                        self.close_failure =
                            Some(StatementNativeHealthFailure::actual_exit_unproven(
                                "CTAS terminal context retention failed",
                                Arc::new(error),
                            ));
                        return Poll::Pending;
                    }
                }
                session.prepare_finish_or_abort_call_in(
                    self.context.clone(),
                    &mut self.seal,
                    &mut self.seal_refusal,
                )
            };
            if let Err(error) = prepared {
                self.close_failure = Some(StatementNativeHealthFailure::actual_exit_unproven(
                    "CTAS seal preparation did not complete",
                    Arc::new(error),
                ));
                return Poll::Pending;
            }
            if let Some(refusal) = &self.seal_refusal {
                let error = cancellation
                    .reason()
                    .map(cancellation_error)
                    .unwrap_or_else(|| internal_error(refusal.to_string()));
                self.fail(error);
            }
        }
        let call = self.seal.as_mut().unwrap();
        if call.poll_needed_ready(cx).is_pending() {
            return Poll::Pending;
        }
        if call.needed_ready().is_some() {
            if let Err(error) = call.take_needed_ready_in(&mut self.seal_needed) {
                self.close_failure = Some(StatementNativeHealthFailure::actual_exit_unproven(
                    "CTAS needed-ready seal transfer failed",
                    Arc::new(error),
                ));
                return Poll::Pending;
            }
            self.seal_settled = true;
            return Poll::Ready(());
        }
        if !self.seal_reconciled {
            let evidence = match (call.outcome(), call.abort_outcome()) {
                (
                    Some(Ok(ConnectorWritePublicationCompletion::CommitUnknown {
                        evidence, ..
                    })),
                    _,
                ) => Some(evidence.clone()),
                (_, Some(Ok(ConnectorWriteAbortOutcome::CommitUnknown { evidence, .. }))) => {
                    Some(evidence.clone())
                }
                _ => None,
            };
            if let Some(evidence) = evidence {
                if let Err(error) = call.start_reconciliation(evidence, self.context.clone()) {
                    self.close_failure = Some(StatementNativeHealthFailure::actual_exit_unproven(
                        "CTAS seal adjudication preparation failed",
                        Arc::new(error),
                    ));
                    return Poll::Pending;
                }
                self.seal_reconciled = true;
                if call.poll_needed_ready(cx).is_pending() {
                    return Poll::Pending;
                }
            }
        }
        if call.needed_ready().is_some() {
            if let Err(error) = call.take_needed_ready_in(&mut self.seal_needed) {
                self.close_failure = Some(StatementNativeHealthFailure::actual_exit_unproven(
                    "CTAS needed-ready seal transfer failed",
                    Arc::new(error),
                ));
                return Poll::Pending;
            }
            self.seal_settled = true;
            return Poll::Ready(());
        }
        let transferred = if call.outcome().is_some() {
            call.take_outcome_in(&mut self.seal_outcome)
        } else if call.abort_outcome().is_some() {
            call.take_abort_outcome_in(&mut self.seal_abort)
        } else {
            return Poll::Pending;
        };
        if let Err(error) = transferred {
            self.close_failure = Some(StatementNativeHealthFailure::actual_exit_unproven(
                "CTAS seal result transfer failed",
                Arc::new(error),
            ));
            return Poll::Pending;
        }
        self.seal_settled = true;
        Poll::Ready(())
    }
    fn poll_inner(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        let cancellation =
            QueryCancellationView::governed(owner.cancellation().clone(), owner.timeout_ms());
        let Some(root_owner) = owner.execution_owner_slot().as_ref() else {
            self.close_failure = Some(self.health.clone());
            return Poll::Pending;
        };
        if !root_owner.is_for_scope(&self.root) {
            self.close_failure = Some(self.health.clone());
            return Poll::Pending;
        }
        match self.io.poll_progress(root_owner, cx) {
            Ok(false) => return Poll::Pending,
            Ok(true) => {}
            Err(error) => self.fail(internal_error(error.to_string())),
        }
        loop {
            match self.phase {
                Phase::Source => {
                    if !self.prepare_source.started {
                        if self.check_cancel(&cancellation) {
                            continue;
                        }
                        self.prepare_source.start(prepare_governed_ctas_source(
                            self.source_input.as_ref().unwrap().clone(),
                        ));
                    }
                    if self.prepare_source.poll_settled(cx).is_pending() {
                        return Poll::Pending;
                    }
                    match self.prepare_source.take() {
                        Ok(source) => match self.preflight.validate_source(
                            &source,
                            self.source_input
                                .as_ref()
                                .unwrap()
                                .request
                                .current_catalog
                                .as_deref(),
                            &self.source_input.as_ref().unwrap().request.current_database,
                        ) {
                            Ok(()) => {
                                self.prepared_source = Some(source);
                                self.phase = Phase::Stage;
                            }
                            Err(error) => self.failure(error),
                        },
                        Err(error) => self.failure(error),
                    }
                }
                Phase::Stage => {
                    if !self.stage.started {
                        if self.check_cancel(&cancellation) {
                            continue;
                        }
                        let action = match self.kernel.prepare_standard_ctas_target(
                            self.prepared_source.as_ref().unwrap().handle.as_ref(),
                            self.publication_id,
                            self.policy,
                        ) {
                            Ok(action) => action,
                            Err(error) => {
                                self.failure(error);
                                continue;
                            }
                        };
                        let input = Arc::new(CatalogInput {
                            kernel: self.kernel.clone(),
                            action: action.handle,
                            receiving: self.controls.clone(),
                        });
                        self.stage_input = Some(input.clone());
                        self.stage_unresolved = true;
                        self.stage.start(stage_original(input));
                    }
                    if self.stage.poll_settled(cx).is_pending() {
                        return Poll::Pending;
                    }
                    match self.stage.take() {
                        Ok(StandardCtasStageOutcome::Prepared { target }) => {
                            match target.stage_effect() {
                                Ok(effect) => self.stage_effect = Some(effect),
                                Err(error) => {
                                    self.target = Some(target);
                                    self.failure(error);
                                    continue;
                                }
                            }
                            let valid =
                                self.preflight.validate_target(&target, self.publication_id);
                            self.target = Some(target);
                            match valid {
                                Ok(()) => self.phase = Phase::PrepareWrite,
                                Err(error) => self.failure(error),
                            }
                        }
                        Ok(StandardCtasStageOutcome::AppliedButUnusable {
                            receipt,
                            finalization,
                        }) => {
                            self.stage_effect = Some((receipt.clone(), finalization.clone()));
                            self.fail(service(DmlError::executor(format!("CTAS staging was applied but cannot continue: {finalization:?}; final table publication was not attempted")).with_unusable_stage(receipt,finalization)));
                        }
                        Ok(StandardCtasStageOutcome::KnownUncommitted { failure }) => {
                            self.stage_unresolved = false;
                            self.failure(failure);
                        }
                        Ok(StandardCtasStageOutcome::CommitUnknown { failure }) | Err(failure) => {
                            self.failure(failure)
                        }
                    }
                }
                Phase::PrepareWrite => {
                    if !self.prepare_write.started {
                        if self.check_cancel(&cancellation) {
                            continue;
                        }
                        let input = Arc::new(GovernedCtasWriteInput {
                            kernel: self.kernel.clone(),
                            source: self.prepared_source.as_ref().unwrap().handle.clone(),
                            target: self.target.as_ref().unwrap().handle.clone(),
                            begin_receiving: self.begin.clone(),
                        });
                        self.write_input = Some(input.clone());
                        self.prepare_write.start(prepare_governed_ctas_write(input));
                    }
                    if self.prepare_write.poll_settled(cx).is_pending() {
                        return Poll::Pending;
                    }
                    match self.prepare_write.take() {
                        Ok(write) => {
                            if write.execution_identity()
                                != self
                                    .prepared_source
                                    .as_ref()
                                    .unwrap()
                                    .facts
                                    .execution_identity
                                || write.target_facts() != self.target.as_ref().unwrap().facts
                            {
                                self.prepared_write = Some(write);
                                self.fail(internal_error("CTAS prepared write facts drifted from source or staged target"));
                            } else {
                                self.prepared_write = Some(write);
                                self.phase = Phase::Admit;
                            }
                        }
                        Err(error) => self.failure(error),
                    }
                }
                Phase::Admit => {
                    if self.check_cancel(&cancellation) {
                        continue;
                    }
                    let root_owner = owner.execution_owner_slot().as_ref().unwrap();
                    // Every previous metadata physical action has actually
                    // exited before the Native dependency union can extend.
                    match self.io.poll_quiescent(root_owner, cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => {
                            self.fail(internal_error(error.to_string()));
                            continue;
                        }
                        Poll::Ready(Ok(())) => {}
                    }
                    match self.computation.poll_admit(&self.io, root_owner, cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => self.fail(error),
                        Poll::Ready(Ok(())) => self.phase = Phase::Compile,
                    }
                }
                Phase::Compile => {
                    if !self.compile.started {
                        if self.check_cancel(&cancellation) {
                            continue;
                        }
                        self.compile.start(compile_governed_ctas_write(
                            self.prepared_write.as_ref().unwrap().clone(),
                            self.provider.clone(),
                            self.computation.capacity().unwrap().clone(),
                        ));
                    }
                    if self.compile.poll_settled(cx).is_pending() {
                        return Poll::Pending;
                    }
                    if self.compile.result.is_some() {
                        match self.compile.take() {
                            Ok(assembly) => self.assembly = Some(assembly),
                            Err(error) => self.fail(service(error.into_dml_error(Some(&self.sql)))),
                        }
                    }
                    if self.poll_provider_close(cx).is_pending() {
                        return Poll::Pending;
                    }
                    if self.phase == Phase::Close {
                        continue;
                    }
                    let root_owner = owner.execution_owner_slot().as_ref().unwrap();
                    match self.io.poll_quiescent(root_owner, cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => {
                            self.fail(internal_error(error.to_string()));
                            continue;
                        }
                        Poll::Ready(Ok(())) => {}
                    }
                    if let Err(error) = self.prepared_write.as_ref().unwrap().claim_execution() {
                        self.failure(error);
                        continue;
                    }
                    if let Err(error) = self
                        .computation
                        .install_assembly(self.assembly.take().unwrap())
                    {
                        self.fail(error);
                        continue;
                    }
                    self.phase = Phase::Compute;
                }
                Phase::Compute => {
                    if self.ready.is_none() {
                        // Install before the sole foreign launch call; unwind
                        // cannot turn a possibly started Native action inert.
                        self.native_launch_attempted = true;
                        let computation = match self.kernel.write_runtime() {
                            Ok(runtime) => {
                                self.computation.poll_ready(runtime, &self.io, owner, cx)
                            }
                            Err(error) => Poll::Ready(Err(internal_error(error))),
                        };
                        match computation {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(Err(error)) => {
                                self.fail(error);
                                continue;
                            }
                            Poll::Ready(Ok(())) => self.ready = self.computation.take_ready(),
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
                    let rows = self
                        .computation
                        .transfer_native_tail_then(&mut self.tail, || {
                            let session = session.ok_or_else(|| {
                                internal_error("CTAS Ready has no original session")
                            })?;
                            let ready = ready
                                .ok_or_else(|| internal_error("CTAS has no actual Ready write"))?;
                            if !Arc::ptr_eq(&ready.session, &session)
                                || !ready.accepted.belongs_to(&session)
                            {
                                return Err(internal_error(
                                    "CTAS Ready replaced the original session",
                                ));
                            }
                            session.freeze_received_row_count().map_err(|error| {
                                QueryServiceError::from_failure_cause(
                                    novarocks_failure_contract::SharedFailureCause::new(error),
                                )
                            })
                        });
                    match rows {
                        Ok(Ok(rows)) => self.affected_rows = Some(rows),
                        Ok(Err(error)) => {
                            self.fail(error);
                            continue;
                        }
                        Err(health) => {
                            self.close_failure = Some(health);
                            return Poll::Pending;
                        }
                    }
                    self.phase = Phase::Seal;
                }
                Phase::Seal => {
                    if self.seal.is_none() {
                        self.check_cancel(&cancellation);
                    }
                    if self.poll_seal(&cancellation, cx).is_pending() {
                        return Poll::Pending;
                    }
                    if self.phase != Phase::Close {
                        self.phase = Phase::Bind;
                    }
                }
                Phase::Bind => {
                    // Only the lease-validated ACTUAL seal can authorize this
                    // pure bind and consume the retained terminal resolver.
                    if self.seal_needed.is_none()
                        && let Some(Ok(ConnectorWritePublicationCompletion::KnownCommitted {
                            needed,
                            ..
                        })) = self.seal_outcome.as_ref()
                    {
                        self.seal_needed = Some(Arc::clone(needed));
                    }
                    let Some(needed) = self.seal_needed.as_ref().map(Arc::clone) else {
                        if matches!(
                            self.seal_outcome,
                            Some(Ok(
                                ConnectorWritePublicationCompletion::CommitUnknown { .. }
                            ))
                        ) && let Err(error) =
                            self.prepared_write.as_ref().unwrap().mark_write_unknown()
                        {
                            self.close_failure =
                                Some(StatementNativeHealthFailure::actual_exit_unproven(
                                    "CTAS unknown write responsibility transition failed",
                                    Arc::new(error),
                                ));
                            return Poll::Pending;
                        }
                        self.fail(internal_error(
                            "CTAS staged write sealing did not prove a publishable write",
                        ));
                        continue;
                    };
                    let write = self
                        .prepared_write
                        .as_ref()
                        .unwrap()
                        .bind_needed_sealed_write(
                            self.ready.as_ref().unwrap(),
                            needed,
                            self.terminal_context.take().unwrap(),
                        );
                    match write {
                        StandardCtasWriteOutcome::Completed {
                            write,
                            execution_identity,
                        } => {
                            if execution_identity
                                != self
                                    .prepared_source
                                    .as_ref()
                                    .unwrap()
                                    .facts
                                    .execution_identity
                            {
                                self.fail(internal_error(
                                    "CTAS sealed write changed its original execution identity",
                                ));
                                continue;
                            }
                            if self.check_cancel(&cancellation) {
                                continue;
                            }
                            match self.kernel.prepare_standard_publish_ctas(
                                self.target.as_ref().unwrap().handle.as_ref(),
                                self.publication_id,
                                write,
                            ) {
                                Ok(action) => {
                                    self.publish_input = Some(Arc::new(CatalogInput {
                                        kernel: self.kernel.clone(),
                                        action: action.handle,
                                        receiving: self.controls.clone(),
                                    }));
                                    self.phase = Phase::Publish;
                                }
                                Err(error) => self.failure(error),
                            }
                        }
                        StandardCtasWriteOutcome::KnownUncommitted { failure } => {
                            self.failure(*failure)
                        }
                    }
                }
                Phase::Publish => {
                    if !self.publish.started {
                        if self.check_cancel(&cancellation) {
                            continue;
                        }
                        self.publish.start(publish_original(
                            self.publish_input.as_ref().unwrap().clone(),
                        ));
                    }
                    let polled = self.publish.poll_settled(cx);
                    // Observe the original journal's actual dispatch cut even
                    // while its necessary finalization child remains Pending.
                    let observation = self.controls.publication_original.observation();
                    if observation.dispatch_started() {
                        if !self.receive_publication_boundary() {
                            return Poll::Pending;
                        }
                        self.disposition = Some(LakePublicationDisposition::CommitUnknown);
                    }
                    if polled.is_pending() {
                        return Poll::Pending;
                    }
                    let outcome = self.publish.take();
                    if matches!(
                        &outcome,
                        Ok(StandardCtasPublishOutcome::Applied { .. }
                            | StandardCtasPublishOutcome::NoOp { .. }
                            | StandardCtasPublishOutcome::CommitUnknown { .. })
                    ) || (outcome.is_err()
                        && !self.controls.publication_confirmed_not_dispatched())
                    {
                        // A typed positive/Unknown terminal is authoritative
                        // even for a no-RPC NoOp. Never infer it from absence.
                        if !self.receive_publication_boundary() {
                            return Poll::Pending;
                        }
                    }
                    match outcome {
                        Ok(StandardCtasPublishOutcome::Applied { finalization }) => {
                            self.terminal_committed(finalization, true, &cancellation)
                        }
                        Ok(StandardCtasPublishOutcome::NoOp { finalization }) => {
                            self.terminal_committed(finalization, false, &cancellation)
                        }
                        Ok(StandardCtasPublishOutcome::KnownUncommitted { failure, cleanup }) => {
                            if self.attempt.terminal_known_uncommitted().is_err() {
                                self.close_failure = Some(self.health.clone());
                                return Poll::Pending;
                            }
                            self.disposition = Some(LakePublicationDisposition::KnownUncommitted);
                            let mut failure = *failure;
                            if let ExternalMutationFinalization::Failed(cleanup) = cleanup {
                                failure.message.push_str(&format!("; cleanup: {cleanup}"));
                            }
                            self.failure(failure);
                        }
                        Ok(StandardCtasPublishOutcome::CommitUnknown { failure, evidence }) => {
                            // The one exact Unknown origin remains primary
                            // throughout its single later observation.
                            assert!(self.publication_failure.is_none());
                            self.publication_failure = Some(*failure);
                            match self.attempt.begin_adjudication() {
                                Ok(token) => self.adjudication_token = Some(token),
                                Err(_) => {
                                    self.close_failure = Some(self.health.clone());
                                    return Poll::Pending;
                                }
                            }
                            self.adjudication_input = Some(Arc::new(AdjudicationInput {
                                kernel: self.kernel.clone(),
                                target: self.target.as_ref().unwrap().handle.clone(),
                                evidence,
                                receiving: self.controls.clone(),
                            }));
                            self.phase = Phase::Adjudicate;
                        }
                        Err(error) if self.controls.publication_confirmed_not_dispatched() => {
                            if self.attempt.terminal_known_uncommitted().is_err() {
                                self.close_failure = Some(self.health.clone());
                                return Poll::Pending;
                            }
                            self.disposition = Some(LakePublicationDisposition::KnownUncommitted);
                            self.failure(error);
                        }
                        Err(error) => self.terminal_unknown(error),
                    }
                }
                Phase::Adjudicate => {
                    // The exact late-observation author reads the Provider's
                    // PublicationUnknown map row. Return that same original
                    // from its active F before consuming the one observation.
                    if self.controls.poll_publication_closed(cx).is_pending() {
                        return Poll::Pending;
                    }
                    if !self.adjudication.started {
                        self.adjudication.start(adjudicate_original(
                            self.adjudication_input.as_ref().unwrap().clone(),
                        ));
                    }
                    if self.adjudication.poll_settled(cx).is_pending() {
                        return Poll::Pending;
                    }
                    match self.adjudication.take() {
                        Ok(StandardCtasPublicationAdjudicationOutcome::Published {
                            finalization,
                        }) => {
                            // Consume the original single allowance. It cannot
                            // represent absence as permission to retry/delete.
                            let finalized = match &finalization {
                                ExternalMutationFinalization::Complete => {
                                    DmlPublicationFinalization::Succeeded
                                }
                                _ => DmlPublicationFinalization::Failed,
                            };
                            if self
                                .attempt
                                .finish_adjudication(
                                    self.adjudication_token.take().unwrap(),
                                    DmlPublicationAdjudicationOutcome::KnownCommitted,
                                    finalized,
                                )
                                .is_err()
                            {
                                self.close_failure = Some(self.health.clone());
                                return Poll::Pending;
                            }
                            self.disposition = Some(LakePublicationDisposition::KnownCommitted);
                            self.selected = Some(match finalization {
                                ExternalMutationFinalization::Complete => Ok(()),
                                ExternalMutationFinalization::Failed(failure) => {
                                    Err(terminal_failure(
                                        mutation_failure(failure),
                                        self.attempt.terminal().unwrap().clone(),
                                        &self.sql,
                                    ))
                                }
                            });
                            self.phase = Phase::Close;
                        }
                        Ok(StandardCtasPublicationAdjudicationOutcome::CommitUnknown {
                            failure,
                        }) => self.finish_unknown_adjudication(*failure),
                        Err(error) => self.finish_unknown_adjudication(error),
                    }
                }
                Phase::Close => {
                    if self.poll_provider_close(cx).is_pending() {
                        return Poll::Pending;
                    }
                    if self.ready.is_none() && self.tail.is_none() {
                        match self.computation.poll_refused_exit(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(Err(health)) => {
                                self.close_failure = Some(health);
                                return Poll::Pending;
                            }
                            Poll::Ready(Ok(())) => {}
                        }
                        if let Err(health) = self.computation.release_result_aliases() {
                            self.close_failure = Some(health);
                            return Poll::Pending;
                        }
                    }
                    if self.poll_seal(&cancellation, cx).is_pending() {
                        return Poll::Pending;
                    }
                    if !self.native_launch_attempted
                        && !self.publish.started
                        && let Some(target) = self.target.as_ref()
                        && self.unlaunched_session_disposed()
                    {
                        if let Some(health) = self.subordinate_health() {
                            self.close_failure = Some(health);
                            return Poll::Pending;
                        }
                        if !self.target_abort.started {
                            let root_owner = owner.execution_owner_slot().as_ref().unwrap();
                            match self.io.poll_quiescent(root_owner, cx) {
                                Poll::Pending => return Poll::Pending,
                                Poll::Ready(Err(error)) => {
                                    self.continuation_error = Some(error);
                                    self.close_failure = Some(self.health.clone());
                                    return Poll::Pending;
                                }
                                Poll::Ready(Ok(())) => {}
                            }
                            self.target_abort
                                .start(abort_governed_unlaunched_ctas_target(
                                    target.handle.clone(),
                                    self.controls.clone(),
                                ));
                        }
                        if self.target_abort.poll_settled(cx).is_pending() {
                            return Poll::Pending;
                        }
                        // The actual abort result stays in the original slot.
                        // Cleanup failures cannot replace the primary cancel,
                        // and only Provider disposal can release the target.
                    }
                    self.pre_dispatch_terminal();
                    if self.close_failure.is_some() {
                        return Poll::Pending;
                    }
                    // The same installed publication Source retains canonical
                    // GC. Its actual F/IO exit belongs to the original residual,
                    // not to the business Needed frontier.
                    if self
                        .controls
                        .publication_original
                        .original_owner()
                        .is_none()
                    {
                        let root_owner = owner.execution_owner_slot().as_ref().unwrap();
                        match self.io.poll_quiescent(root_owner, cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(Err(error)) => {
                                self.fail(internal_error(error.to_string()));
                                return Poll::Pending;
                            }
                            Poll::Ready(Ok(())) => {}
                        }
                    }
                    drop(self.ready.take());
                    drop(self.source_input.take());
                    drop(self.prepared_source.take());
                    drop(self.write_input.take());
                    drop(self.prepared_write.take());
                    drop(self.stage_input.take());
                    // Keep the exact target/action/lease until the residual
                    // observes both physical exit and inventory disposition.
                    drop(self.adjudication_input.take());
                    drop(self.assembly.take());
                    drop(self.terminal_context.take());
                    self.phase = Phase::Done;
                    self.completed = true;
                }
                Phase::Done => {
                    return Poll::Ready(super::write_action::selected_command_completion(
                        self.selected.take().unwrap_or_else(|| {
                            Err(internal_error("CTAS has no original business selection"))
                        }),
                        self.affected_rows,
                    ));
                }
            }
        }
    }
    fn finish_unknown_adjudication(&mut self, later: CtasFailure) {
        if self
            .attempt
            .finish_adjudication(
                self.adjudication_token.take().unwrap(),
                DmlPublicationAdjudicationOutcome::CommitUnknown,
                DmlPublicationFinalization::NotApplicable,
            )
            .is_err()
        {
            self.close_failure = Some(self.health.clone());
            return;
        }
        self.disposition = Some(LakePublicationDisposition::CommitUnknown);
        let original = self.publication_failure.take().unwrap_or(later);
        self.selected = Some(Err(terminal_failure(
            original,
            self.attempt.terminal().unwrap().clone(),
            &self.sql,
        )));
        self.phase = Phase::Close;
    }
    fn unlaunched_session_disposed(&self) -> bool {
        match self.session() {
            None => {
                self.seal_needed.is_none()
                    && self.seal_outcome.is_none()
                    && self.seal_abort.is_none()
            }
            Some(session) => {
                matches!(
                    self.seal_abort,
                    Some(Ok(ConnectorWriteAbortOutcome::KnownUncommitted { .. }))
                ) && matches!(
                    session.cleanup_responsibility(),
                    Ok(ConnectorMetadataCleanupResponsibility::Disposed)
                )
            }
        }
    }
    fn subordinate_health(&self) -> Option<StatementNativeHealthFailure> {
        self.begin
            .local_health_failure()
            .or_else(|| self.provider.local_health_failure())
            .or_else(|| self.computation.local_health_failure())
            .or_else(|| {
                self.seal
                    .as_ref()
                    .and_then(WritePublicationCall::local_health_failure)
            })
            .or_else(|| {
                self.tail
                    .as_ref()
                    .and_then(StatementNativeTail::local_health_failure)
            })
            .or_else(|| self.io.health_failure())
    }
    fn local_health(&self) -> Option<StatementNativeHealthFailure> {
        self.close_failure
            .clone()
            .or_else(|| self.busy.then(|| self.health.clone()))
            .or_else(|| self.subordinate_health())
    }
}
impl GovernedStatementAction for CtasAction {
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        let source = self.source.as_mut().expect("original CTAS source");
        if source.busy || source.close_failure.is_some() {
            return Poll::Pending;
        }
        source.busy = true;
        let result = source.poll_inner(owner, cx);
        source.busy = false;
        result.map(|result| {
            result.map_err(|error| match source.attempt.terminal() {
                Some(terminal) => error.attach_publication_terminal(terminal.clone()),
                None => error,
            })
        })
    }
    fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.source
            .as_ref()
            .and_then(|source| source.local_health())
    }
    fn external_effect_disposition(
        &self,
    ) -> Option<novarocks_query_application::api::StatementExternalEffectDisposition> {
        (|| {
            let source = self.source.as_ref()?;
            let observation = source.controls.publication_original.observation();
            if observation.applied_receipt().is_some() {
                return Some(LakePublicationDisposition::KnownCommitted);
            }
            // A signed NoOp is not an externally applied publication, even
            // though the original CTAS logical terminal uses KnownCommitted.
            if observation
                .needed_committed()
                .is_some_and(|needed| needed.effect == ExternalMutationEffect::NoOp)
                || matches!(
                    observation.raw_outcome(),
                    Some(Ok(
                        novarocks_spi::connector::ConnectorStagedCreatePublishOutcome::NoOp { .. }
                    ))
                )
            {
                return None;
            }
            if source.controls.publication_confirmed_not_dispatched() {
                return Some(LakePublicationDisposition::KnownUncommitted);
            }
            if observation.dispatch_started() && source.disposition.is_none() {
                return Some(LakePublicationDisposition::CommitUnknown);
            }
            if source.publish.started
                && source.disposition == Some(LakePublicationDisposition::CommitUnknown)
            {
                if source.adjudication.started
                    && let Some(raw) = source
                        .controls
                        .adjudication
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .as_ref()
                {
                    return Some(observed_governed_ctas_adjudication(
                        source.publish_input.as_ref()?.action.as_ref(),
                        &source.adjudication_input.as_ref()?.evidence,
                        raw,
                    ));
                }
                if let Some(raw) = source
                    .controls
                    .publication
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .as_ref()
                {
                    return Some(observed_governed_ctas_publication(
                        source.publish_input.as_ref()?.action.as_ref(),
                        raw,
                    ));
                }
            }
            source.disposition
        })()
        .map(crate::query::external_effect_from_lake)
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        let source = self.source.as_ref().ok_or_else(health)?;
        if target.is_some() || !source.completed || source.busy || source.local_health().is_some() {
            return Err(source.health.clone());
        }
        let mut residual = Box::new(CtasResidual {
            source: None,
            retiring: false,
            health: source.health.clone(),
        });
        residual.source = self.source.take();
        *target = Some(residual);
        Ok(())
    }
}
struct CtasResidual {
    source: Option<Box<CtasSource>>,
    retiring: bool,
    health: StatementNativeHealthFailure,
}
impl GovernedStatementResidual for CtasResidual {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if self.retiring {
            return Poll::Ready(Err(self.health.clone()));
        }
        let Some(source) = self.source.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        if source.busy || source.local_health().is_some() {
            return Poll::Pending;
        }
        source.busy = true;
        let mut pending = false;
        match source.io.poll_continuing(cx) {
            Ok(ready) => pending |= !ready,
            Err(error) => {
                // Ordinary admission/control refusal is not a physical exit
                // failure. Preserve it while the original Provider F joins.
                if source.continuation_error.is_none() {
                    source.continuation_error = Some(error);
                }
            }
        }
        if let Some(call) = source.seal.as_mut() {
            pending |= call.poll_retirement(cx).is_pending();
            if let Some(health) = call.local_health_failure() {
                source.close_failure = Some(health);
                source.busy = false;
                return Poll::Pending;
            }
        }
        if let Some(tail) = source.tail.as_mut()
            && tail.poll_exit(cx).is_pending()
        {
            pending = true;
        }
        // The canonical publisher may have delivered Needed while its GC
        // child remains active. Finish that exact Source before asking the
        // Provider Map to settle or release the original target inventory.
        if source.controls.poll_publication_closed(cx).is_pending() {
            pending = true;
        }
        let mut physical_pending = pending;
        if !pending && !source.target_disposed {
            if let Some(target) = &source.target {
                if !source.target_disposal_observed {
                    let may_settle =
                        match governed_ctas_target_cleanup_responsibility(target.handle.as_ref()) {
                            Ok(ConnectorMetadataCleanupResponsibility::Disposed) => {
                                source.target_disposal_observed = true;
                                false
                            }
                            Ok(ConnectorMetadataCleanupResponsibility::Retained) => true,
                            Err(error) => {
                                if source.cleanup_refusal.is_none() {
                                    source.cleanup_refusal = Some(error);
                                }
                                false
                            }
                        };
                    if !source.target_disposal_observed
                        && (may_settle || source.target_cleanup.started)
                    {
                        if !source.target_cleanup.started {
                            source
                                .target_cleanup
                                .start(settle_governed_ctas_target_cleanup(
                                    target.handle.clone(),
                                    source.controls.clone(),
                                ));
                        }
                        if source.target_cleanup.poll_settled(cx).is_ready() {
                            source.target_disposal_observed = matches!(
                                source.target_cleanup.result,
                                Some(Ok(ConnectorMetadataCleanupResponsibility::Disposed))
                            );
                        } else {
                            physical_pending = true;
                        }
                    }
                }
                if !source.target_disposal_observed {
                    pending = true;
                }
            } else if source.stage_unresolved || source.stage_effect.is_some() {
                pending = true;
            } else if let Some(session) = source.session() {
                if matches!(
                    session.cleanup_responsibility(),
                    Ok(ConnectorMetadataCleanupResponsibility::Disposed)
                ) {
                    source.target_disposal_observed = true;
                } else {
                    pending = true;
                }
            } else {
                source.target_disposal_observed = true;
            }
        }
        if !physical_pending {
            // Release actual IO slots even while an Unknown/cleanup inventory
            // remains retained by this same Root and exact Provider lease.
            match source.io.poll_actual_quiescent_continuing(cx) {
                Poll::Pending => {
                    physical_pending = true;
                    pending = true;
                }
                Poll::Ready(Err(health)) => {
                    source.close_failure = Some(health);
                    source.busy = false;
                    return Poll::Pending;
                }
                Poll::Ready(Ok(observed)) => {
                    if source.continuation_error.is_none() {
                        source.continuation_error = observed.into_ordinary_failure();
                    }
                }
            }
        }
        if source.target_disposal_observed && !source.target_disposed && !physical_pending {
            if let Some(target) = &source.target {
                match acknowledge_governed_ctas_target_cleanup(target.handle.as_ref()) {
                    Ok(()) => source.target_disposed = true,
                    Err(error) => {
                        if source.cleanup_refusal.is_none() {
                            source.cleanup_refusal = Some(error);
                        }
                        pending = true;
                    }
                }
            } else {
                source.target_disposed = true;
            }
        }
        if pending {
            source.busy = false;
            return Poll::Pending;
        }
        source.busy = false;
        self.retiring = true;
        drop(self.source.take());
        self.retiring = false;
        Poll::Ready(Ok(()))
    }
}
#[derive(Debug)]
struct CtasExitUnproven;
impl std::fmt::Display for CtasExitUnproven {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CTAS original poll or Future Drop did not return normally")
    }
}
impl std::error::Error for CtasExitUnproven {}
fn health() -> StatementNativeHealthFailure {
    StatementNativeHealthFailure::actual_exit_unproven(
        "CTAS original local exit is unproven",
        Arc::new(CtasExitUnproven),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::ExternalMutationOutcome;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    #[test]
    fn original_provider_cause_reaches_actual_statement_terminal_projection() {
        use novarocks_failure_contract::{FailureClass, FailureProjection, SharedFailureCause};
        use novarocks_type_contract::CompileControlError;
        // This is a real first-party source, not a synthetic class inferred
        // from a message or the broad Connector Interrupted kind.
        let compilation = SharedFailureCause::new(
            novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(
                "CTAS test source".into(),
            ),
        );
        let control = CompileControlError::Source(compilation.clone());
        let connector = ConnectorError::compile_interruption(control.clone());
        let original = std::error::Error::source(&connector)
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap()
            .clone();
        assert_eq!(original.failure_fact().class(), FailureClass::Unsupported);
        let failure = mutation_failure(
            novarocks_spi::connector::ConnectorMutationFailure::from_connector(connector),
        );
        for committed in [false, true] {
            let mut attempt = DmlPublicationAttempt::new(
                LakePublicationId::new_v7(),
                LakePublicationFamily::Ctas,
                LakePublicationTarget::try_new(
                    "actual-ctas".into(),
                    "db".into(),
                    Some("t".into()),
                    None,
                )
                .unwrap(),
                None,
            );
            attempt.mark_dispatch_possible().unwrap();
            if committed {
                attempt
                    .terminal_known_committed(DmlPublicationFinalization::Failed)
                    .unwrap();
            } else {
                let ticket = attempt.begin_adjudication().unwrap();
                attempt
                    .finish_adjudication(
                        ticket,
                        DmlPublicationAdjudicationOutcome::CommitUnknown,
                        DmlPublicationFinalization::NotApplicable,
                    )
                    .unwrap();
            }
            let terminal = attempt.terminal().unwrap().clone();
            let error = terminal_failure(
                failure.clone(),
                terminal.clone(),
                "CREATE TABLE t AS SELECT 1",
            );
            assert_eq!(error.failure_cause(), Some(&original));
            assert_eq!(
                error.execution_failure().unwrap().fact(),
                original.failure_fact()
            );
            assert_eq!(
                error
                    .failure_cause()
                    .unwrap()
                    .downcast_ref::<CompileControlError>(),
                Some(&control)
            );
            assert_eq!(error.publication_terminal(), Some(&terminal));
            assert!(error.message().contains("do_not_retry=true"));
            assert!(
                error.compile_control_error().is_none(),
                "a Connector source is not relabeled as a compile operation"
            );
            assert!(
                error.external_effect_disposition().is_none(),
                "a terminal projection cannot invent an Applied receipt"
            );
        }
    }
    struct OriginalCatalogFuture {
        polls: Arc<AtomicUsize>,
        drop_panics: bool,
    }
    impl Future for OriginalCatalogFuture {
        type Output = Result<StandardCtasPublishOutcome, CtasFailure>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Ok(StandardCtasPublishOutcome::Applied {
                finalization: ExternalMutationFinalization::Complete,
            }))
        }
    }
    impl Drop for OriginalCatalogFuture {
        fn drop(&mut self) {
            if self.drop_panics {
                panic!("actual original Provider Future Drop failed");
            }
        }
    }
    #[test]
    fn actual_catalog_effect_is_retained_before_its_original_future_drop() {
        let polls = Arc::new(AtomicUsize::new(0));
        let mut slot = PhaseSlot::default();
        slot.start(OriginalCatalogFuture {
            polls: polls.clone(),
            drop_panics: true,
        });
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| slot.poll_settled(&mut cx)))
                .is_err()
        );
        assert!(matches!(
            slot.result,
            Some(Ok(StandardCtasPublishOutcome::Applied {
                finalization: ExternalMutationFinalization::Complete
            }))
        ));
        assert!(slot.busy);
        assert!(!slot.settled);
        assert!(slot.poll_settled(&mut cx).is_pending());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
    }
    struct GatedCatalogFuture {
        ready: Arc<AtomicBool>,
        drops: Arc<AtomicUsize>,
    }
    impl Future for GatedCatalogFuture {
        type Output = Result<StandardCtasPublishOutcome, CtasFailure>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            if self.ready.load(Ordering::SeqCst) {
                Poll::Ready(Ok(StandardCtasPublishOutcome::NoOp {
                    finalization: ExternalMutationFinalization::Complete,
                }))
            } else {
                Poll::Pending
            }
        }
    }
    impl Drop for GatedCatalogFuture {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn pending_control_is_not_dropped_or_replaced_by_a_cancelled_waiter() {
        let ready = Arc::new(AtomicBool::new(false));
        let drops = Arc::new(AtomicUsize::new(0));
        let mut slot = PhaseSlot::default();
        slot.start(GatedCatalogFuture {
            ready: ready.clone(),
            drops: drops.clone(),
        });
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(slot.poll_settled(&mut cx).is_pending());
        assert!(slot.future.is_some());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        ready.store(true, Ordering::SeqCst);
        assert!(slot.poll_settled(&mut cx).is_ready());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(matches!(
            slot.take(),
            Ok(StandardCtasPublishOutcome::NoOp { .. })
        ));
    }
    #[test]
    fn terminal_context_retention_does_not_substitute_for_actual_seal_receipt() {
        use crate::query_execution::{
            write_result::DecodedPreparedWriteSet, write_session::tests as fixture,
        };
        use novarocks_spi::connector::write_stack::WriteTargetOrdinal;
        let mut actual = fixture::known_committed();
        if let ExternalMutationOutcome::KnownCommitted { effect, .. } = &mut actual {
            *effect = ExternalMutationEffect::NoOp;
        }
        let fixture = fixture::fixture_with_outcome(1, 16, actual);
        fixture
            .session
            .receive_for_test(DecodedPreparedWriteSet::for_test(
                7,
                vec![(
                    WriteTargetOrdinal::try_new(0).unwrap(),
                    fixture::commit_fragment_bytes(),
                )],
            ))
            .unwrap();
        let retained = fixture
            .session
            .following_terminal_context(fixture::request_context())
            .unwrap();
        assert_eq!(fixture.session.finish_invocations(), 0);
        let mut call = fixture
            .session
            .prepare_finish_call(fixture::request_context())
            .unwrap();
        let mut original_result = None;
        assert!(call.take_outcome_in(&mut original_result).is_err());
        assert!(original_result.is_none());
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(call.poll_completed(&mut cx).is_ready());
        call.take_outcome_in(&mut original_result).unwrap();
        assert!(matches!(
            original_result,
            Some(Ok(ConnectorWritePublicationCompletion::KnownCommitted { needed, .. })) if needed.effect() == ExternalMutationEffect::NoOp
        ));
        assert_eq!(fixture.session.finish_invocations(), 1);
        drop(retained);
    }
}
