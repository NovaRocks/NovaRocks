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

use super::publication::{
    StatisticsAttemptError as CoreStatisticsAttemptError, StatisticsFailure,
    StatisticsPublicationFact, StatisticsPublicationOutcome,
};

use crate::statistics::application::{
    StatisticsApplicationError, StatisticsAttemptRequest, StatisticsColumnIntent,
};
use novarocks_spi::connector::{
    ConnectorMutationOperationId, ConnectorOperationControl, ConnectorRequestContext,
    ExternalMutationFinalization, ExternalMutationOutcome, StatisticsCollectionStartRequest,
    StatisticsColumnSelection,
};

/// Exact Frontend composition leaves retained by the original ANALYZE action.
/// The same capture supplies the binding, data version and schema to collection.
#[derive(Clone)]
pub struct StatisticsAttemptExecutorPorts {
    constant_policy: novarocks_functions::ConstantPolicy,
    static_plan_carrier: crate::query_execution::package_freeze::CompiledPackageCarrier,
    launcher: Arc<dyn LogicalExecutionLauncher>,
    completion: CompletionFactOwners,
    execution_runtime: (
        crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
        crate::native::data_runtime::FrontendDataRuntime,
    ),
    function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
    runtime: tokio::runtime::Handle,
}

impl StatisticsAttemptExecutorPorts {
    pub(crate) fn new(
        launcher: Arc<dyn LogicalExecutionLauncher>,
        completion: CompletionFactOwners,
        execution_runtime: (
            crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor,
            crate::native::data_runtime::FrontendDataRuntime,
        ),
        function_catalog: Arc<novarocks_functions::EngineFunctionCatalog>,
        runtime: tokio::runtime::Handle,
        constant_policy: novarocks_functions::ConstantPolicy,
        static_plan_carrier: crate::query_execution::package_freeze::CompiledPackageCarrier,
    ) -> Self {
        Self {
            constant_policy,
            static_plan_carrier,
            launcher,
            completion,
            execution_runtime,
            function_catalog,
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
            if requested_columns.is_empty() {
                return Err(StatisticsApplicationError::invalid_analyze_property(
                    "ANALYZE columns must not be empty",
                ));
            }
            let mut resolved = Vec::with_capacity(requested_columns.len());
            for requested in requested_columns {
                let mut matches = bound_columns
                    .iter()
                    .filter(|bound| bound.name().eq_ignore_ascii_case(requested));
                let Some(column) = matches.next() else {
                    return Err(StatisticsApplicationError::invalid_analyze_property(
                        format!(
                            "ANALYZE requested column '{requested}' does not exist on the captured table"
                        ),
                    ));
                };
                if matches.next().is_some() {
                    return Err(StatisticsApplicationError::invalid_analyze_property(
                        format!(
                            "ANALYZE requested column '{requested}' matches multiple captured table columns"
                        ),
                    ));
                }
                if resolved
                    .iter()
                    .any(|existing: &FieldRef| existing.name().eq_ignore_ascii_case(column.name()))
                {
                    return Err(StatisticsApplicationError::invalid_analyze_property(
                        format!("ANALYZE requested column '{requested}' is duplicated"),
                    ));
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
    api::StatementNativeHealthFailure,
    cancellation::QueryCancellationView,
    session_control::GovernedActionContext,
    session_error::{QueryServiceError, QueryServiceErrorKind},
};
use novarocks_spi::connector::{
    AsyncStatisticsCollectionSession, ConnectorControlPlanningPin, ConnectorStatisticsSession,
    LakePublicationDisposition, StatisticsArtifactDraft, StatisticsCpuPort,
    StatisticsPublicationClose, StatisticsPublicationReady,
};
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
impl crate::wake::StatusIntakeWake for StatisticsWake {
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
    business_result: Option<
        Result<super::publication::StatisticsPublicationOutcome, CoreStatisticsAttemptError>,
    >,
    captured_binding: Option<super::application::StatisticsAttemptBinding>,
    cleanup_failure: Option<super::publication::StatisticsPublicationFailure>,
    relation: crate::query_execution::statistics::StatisticsRelationIdentity,
    request: Option<StatisticsAttemptRequest>,
    planning_pin: Option<ConnectorControlPlanningPin>,
    statistics_session: Option<ConnectorStatisticsSession>,
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
    // Keep an exited ordinary I/O refusal without changing the selected statement.
    io_close_failure: Option<WorkError>,
}
fn service_error(error: impl ToString) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, error.to_string())
}
fn failure(error: impl ToString) -> CoreStatisticsAttemptError {
    FrontendStatisticsAttemptExecutor::failure(error.to_string())
}

impl Drop for StatisticsState {
    fn drop(&mut self) {
        // A refused start or notification unwind still closes the same
        // collector before releasing this final stable domain custody.
        let _ = self.credentials.seal_for_terminal();
    }
}
impl StatisticsState {
    fn finish_business(
        &mut self,
        result: Result<
            super::publication::StatisticsPublicationOutcome,
            CoreStatisticsAttemptError,
        >,
    ) -> Result<(), QueryServiceError> {
        if self.business_result.is_none() {
            self.business_result = Some(result);
            Ok(())
        } else {
            Err(service_error(
                "statistics statement result was already selected",
            ))
        }
    }
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
            self.finish_business(Err(error))?;
            self.selected = true;
            self.phase = ActionPhase::Finished;
            return Poll::Ready(Ok(()));
        }
        match self.phase {
            ActionPhase::Begin => {
                self.pending = Some(Box::pin(begin_session(
                    self.session.clone(),
                    self.request.take().unwrap(),
                    self.statistics_session.take().unwrap(),
                    self.context.clone(),
                    self.cpu.clone(),
                    self.captured_binding.take().ok_or_else(|| {
                        service_error("ANALYZE lost its original captured binding")
                    })?,
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
                        self.planning_pin.take().unwrap(),
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
                            let projected = preparation_failure(&original);
                            self.plan.try_lock().map_err(service_error)?.failure = Some(original);
                            return self.fail(projected, cx);
                        }
                    };
                    self.computation.install_assembly(assembly)?;
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
                self.phase = ActionPhase::Publish;
            }
            ActionPhase::Publish => {
                self.pending = Some(Box::pin(publish_session(
                    self.session.clone(),
                    self.context.clone(),
                )));
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
                    self.finish_business(Ok(outcome))?;
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
        if let Err(error) = self.io.poll_continuing(cx) {
            self.io_close_failure.get_or_insert(error);
        }
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
                    self.cleanup_failure.get_or_insert(failure);
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
fn preparation_failure(original: &StatisticsPreparationFailure) -> CoreStatisticsAttemptError {
    let error = match original {
        StatisticsPreparationFailure::Cpu(input) => {
            QueryServiceError::from_execution_failure(input.as_ref().clone())
        }
        StatisticsPreparationFailure::Domain(error) => {
            QueryServiceError::from_execution_failure(error.clone().into_execution_failure_input())
                .with_compile_control(error.compile_control_error())
        }
    };
    CoreStatisticsAttemptError::Failed(StatisticsFailure { error })
}
fn health(
    message: &'static str,
    cause: Arc<dyn std::error::Error + Send + Sync>,
) -> StatementNativeHealthFailure {
    StatementNativeHealthFailure::actual_exit_unproven(message, cause)
}
impl FrontendStatisticsAttemptExecutor {
    fn failure(message: impl Into<String>) -> CoreStatisticsAttemptError {
        CoreStatisticsAttemptError::Failed(StatisticsFailure {
            error: service_error(Into::<String>::into(message)),
        })
    }

    fn scope_error(error: WorkError) -> CoreStatisticsAttemptError {
        let cancelled = matches!(error, WorkError::Cancelled(_));
        let failure = StatisticsFailure {
            error: QueryServiceError::from_compile_control(
                novarocks_type_contract::CompileControlError::Source(
                    novarocks_failure_contract::SharedFailureCause::from_static_detail(
                        error,
                        "statistics original workload refused progress",
                    ),
                ),
            ),
        };
        if cancelled {
            CoreStatisticsAttemptError::Cancelled(failure)
        } else {
            CoreStatisticsAttemptError::Failed(failure)
        }
    }
    fn application_error_ref(error: &StatisticsApplicationError) -> CoreStatisticsAttemptError {
        let failure = StatisticsFailure {
            error: error.to_query_service_error(),
        };
        if error.target_binding_failure().is_some() {
            CoreStatisticsAttemptError::Stale(failure)
        } else {
            CoreStatisticsAttemptError::Failed(failure)
        }
    }

    #[cfg(test)]
    fn publication_outcome(
        outcome: ExternalMutationOutcome<novarocks_spi::connector::StatisticsReceipt>,
    ) -> StatisticsPublicationOutcome {
        Self::publication_outcome_ref(&outcome)
    }
    fn publication_failure(
        error: &novarocks_spi::connector::ConnectorMutationFailure,
    ) -> super::publication::StatisticsPublicationFailure {
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
        let observation = super::publication::StatisticsPublicationFailure::capture(
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
    ) -> Option<super::publication::StatisticsPublicationFailure> {
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
    statistics_session: ConnectorStatisticsSession,
    context: ConnectorRequestContext,
    cpu: Arc<dyn StatisticsCpuPort>,
    binding: super::application::StatisticsAttemptBinding,
) -> Result<(), CoreStatisticsAttemptError> {
    // This stable slot remains owned by Action/Residual if its waiter unwinds.
    let mut original = slot.lock().await;
    let selection = match &request.columns {
        StatisticsColumnIntent::AllColumns => StatisticsColumnSelection::Default,
        StatisticsColumnIntent::Explicit(_) => {
            let columns = match resolve_columns(&request, &binding.sql_columns) {
                Ok(columns) => columns,
                Err(error) => {
                    let projected =
                        FrontendStatisticsAttemptExecutor::application_error_ref(&error);
                    original.binding_failure = Some(error);
                    return Err(projected);
                }
            };
            StatisticsColumnSelection::explicit(
                columns
                    .into_iter()
                    .map(|column| Arc::from(column.name().as_str()))
                    .collect(),
            )
            .map_err(failure)?
        }
    };
    let start = match statistics_session
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
    planning: ConnectorControlPlanningPin,
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
            let projected =
                preparation_failure(&StatisticsPreparationFailure::Domain(error.clone()));
            original.failure = Some(StatisticsPreparationFailure::Domain(error));
            Err(projected)
        }
    }
}
async fn publish_session(
    slot: Arc<tokio::sync::Mutex<SessionSlot>>,
    context: ConnectorRequestContext,
) -> Result<(), CoreStatisticsAttemptError> {
    let mut original = slot.lock().await;
    let SessionSlot {
        session,
        artifacts,
        publication,
        provider_failure,
        ..
    } = &mut *original;
    if let Err(error) = context.check_active() {
        let projected = failure(&error);
        *provider_failure = Some(error);
        return Err(projected);
    }
    let session = session
        .as_mut()
        .ok_or_else(|| failure("statistics publication has no original session"))?;
    if let Err(error) = session.prepare_publication_async(artifacts).await {
        let projected = failure(&error);
        *provider_failure = Some(error);
        return Err(projected);
    }
    if let Err(error) = context.check_active() {
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
    use novarocks_failure_contract::FailureProjection;
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
        stop_on_prepare: Option<novarocks_spi::connector::ConnectorStopOwner>,
        close_gate: Option<Arc<tokio::sync::Notify>>,
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
                if let Some(stop) = &self.stop_on_prepare {
                    stop.request_stop();
                }
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
                if let Some(gate) = &self.close_gate {
                    gate.notified().await;
                }
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
        close_gate: Option<Arc<tokio::sync::Notify>>,
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
                stop_on_prepare: None,
                close_gate,
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
        let (slot, finishes, closes) = empty_session(false, None);
        assert!(slot.lock().await.program.is_none());
        assert_eq!(finishes.load(Ordering::SeqCst), 0);
        publish_session(slot.clone(), crate::connector::test_request_context())
            .await
            .unwrap();
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
    #[tokio::test]
    async fn both_no_op_publication_shapes_keep_original_session_until_close() {
        for settled in [false, true] {
            let (slot, finishes, closes) = empty_session(settled, None);
            publish_session(slot.clone(), crate::connector::test_request_context())
                .await
                .unwrap();
            let original = slot.lock().await;
            let outcome = FrontendStatisticsAttemptExecutor::publication_ready_outcome(
                original.publication.as_ref().unwrap(),
            );
            assert_eq!(outcome.fact(), StatisticsPublicationFact::NoOp);
            assert!(original.session.is_some());
            assert_eq!(finishes.load(Ordering::SeqCst), 1);
            assert_eq!(closes.load(Ordering::SeqCst), 0);
        }
    }
    struct CloseOnlyCpu(Arc<AtomicUsize>);
    impl StatisticsCpuPort for CloseOnlyCpu {
        fn submit_in(
            &self,
            _: &mut Option<novarocks_spi::connector::StatisticsCpuInput>,
        ) -> Result<(), ConnectorError> {
            panic!("close does not dispatch new CPU work")
        }
        fn poll_finished(&self, _: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
            panic!("close does not wait for computation readiness")
        }
        fn poll_closed(&self, _: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
        fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
            None
        }
        fn metadata_bytes(&self) -> Result<std::num::NonZeroU64, ConnectorError> {
            Ok(std::num::NonZeroU64::new(1).unwrap())
        }
    }
    #[tokio::test]
    async fn publication_readiness_waits_for_real_provider_close_and_cpu_exit() {
        use futures::FutureExt;
        let gate = Arc::new(tokio::sync::Notify::new());
        let (slot, finishes, closes) = empty_session(false, Some(gate.clone()));
        let context = crate::connector::test_request_context();
        publish_session(slot.clone(), context.clone())
            .await
            .unwrap();
        assert_eq!(finishes.load(Ordering::SeqCst), 1);
        let cpu_closes = Arc::new(AtomicUsize::new(0));
        let mut closing = Box::pin(close_session(
            slot.clone(),
            Arc::new(tokio::sync::Mutex::new(PlanSlot::default())),
            FrontendCompletionCustody::new(),
            context,
            Arc::new(CloseOnlyCpu(cpu_closes.clone())),
        ));
        assert!(
            closing.as_mut().now_or_never().is_none(),
            "NeededReady cannot invent actual close"
        );
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        assert_eq!(
            cpu_closes.load(Ordering::SeqCst),
            0,
            "provider retains the original close before CPU exit"
        );
        gate.notify_one();
        closing.await.unwrap();
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        assert_eq!(cpu_closes.load(Ordering::SeqCst), 1);
        assert!(matches!(
            slot.lock().await.close,
            Some(StatisticsPublicationClose::Observed {
                owned_objects_disposed: true,
                ..
            })
        ));
    }
    #[test]
    fn preparation_cpu_and_domain_failures_keep_the_original_selected_input() {
        use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
        use novarocks_query_application::execution_error::{
            ExecutionFailureInput, ExecutionFailureSite,
        };
        let input = ExecutionFailureInput::new(
            WorkError::Capacity("original statistics capacity").failure_fact(),
            novarocks_execution_contract::SafeDetail::truncating(
                "original selected statistics failure",
            ),
            ExecutionFailureSite::Backend {
                process: novarocks_types::BackendProcessId::new_v7(),
            },
        );
        for original in [
            StatisticsPreparationFailure::Cpu(Box::new(input.clone())),
            StatisticsPreparationFailure::Domain(DistributedQueryError::from_execution(
                input.clone(),
            )),
        ] {
            let projected = preparation_failure(&original).failure().error;
            assert_eq!(projected.execution_failure(), Some(&input));
        }
        let cause = novarocks_type_contract::CompileControlError::Source(
            novarocks_failure_contract::SharedFailureCause::new(WorkError::Cancelled(
                novarocks_workload_control::CancellationReason::Requested,
            )),
        );
        let original = DistributedQueryError::from_encode(
            novarocks_plan_codec::PhysicalEncodeError::Control(cause.clone()),
        );
        let expected = original.clone().into_execution_failure_input();
        let projected = preparation_failure(&StatisticsPreparationFailure::Domain(original))
            .failure()
            .error;
        assert_eq!(projected.execution_failure(), Some(&expected));
        assert_eq!(projected.compile_control_error(), Some(cause));
        let selected = DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "opaque owner detail",
        );
        assert_eq!(
            preparation_failure(&StatisticsPreparationFailure::Domain(selected.clone()))
                .failure()
                .error
                .execution_failure(),
            Some(&selected.into_execution_failure_input())
        );
    }
    #[test]
    fn workload_stops_keep_the_original_cause_and_descriptor() {
        use novarocks_failure_contract::FailureProjection;
        for work in [
            WorkError::Cancelled(novarocks_workload_control::CancellationReason::Requested),
            WorkError::Cancelled(novarocks_workload_control::CancellationReason::DeadlineExceeded),
            WorkError::Capacity("test resource"),
        ] {
            let failure = FrontendStatisticsAttemptExecutor::scope_error(work.clone());
            let original = failure.failure();
            let control = original.error.compile_control_error().unwrap();
            let novarocks_type_contract::CompileControlError::Source(cause) = &control else {
                panic!("original scope source")
            };
            assert_eq!(cause.downcast_ref::<WorkError>(), Some(&work));
            let error = StatisticsApplicationError::from_statistics_attempt(
                novarocks_spi::connector::LakePublicationId::new_v7(),
                &super::super::application::StatisticsTableTarget {
                    catalog: "ice".into(),
                    namespace: "db".into(),
                    table: "t".into(),
                },
                &Err(failure),
            )
            .unwrap()
            .to_query_service_error();
            assert_eq!(
                error.execution_failure().unwrap().fact(),
                work.failure_fact()
            );
            assert_eq!(error.compile_control_error(), Some(control));
        }
    }
    #[tokio::test]
    async fn cancellation_before_finish_never_dispatches_publication() {
        for during_prepare in [false, true] {
            let finishes = Arc::new(AtomicUsize::new(0));
            let stop = novarocks_spi::connector::ConnectorStopOwner::new();
            let context = crate::connector::connector_request_context(None, stop.view()).unwrap();
            let slot = Arc::new(tokio::sync::Mutex::new(SessionSlot {
                session: Some(Box::new(EmptyCollectionSession {
                    descriptor: ConnectorInstanceDescriptor {
                        provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                        instance_id: ConnectorInstanceId::parse("ice").unwrap(),
                    },
                    finish_calls: finishes.clone(),
                    close_calls: Arc::new(AtomicUsize::new(0)),
                    settled: false,
                    prepared: false,
                    published: false,
                    stop_on_prepare: during_prepare.then(|| stop.clone()),
                    close_gate: None,
                })),
                artifacts: Some(Vec::new()),
                ..Default::default()
            }));
            if !during_prepare {
                stop.request_stop();
            }
            assert!(publish_session(slot.clone(), context).await.is_err());
            assert_eq!(finishes.load(Ordering::SeqCst), 0);
            let original = slot.lock().await;
            assert!(original.publication.is_none());
            let cause = original.provider_failure.as_ref().unwrap();
            assert_eq!(
                StatisticsApplicationError::from_connector_error(cause.clone())
                    .to_query_service_error()
                    .execution_failure()
                    .unwrap()
                    .fact(),
                cause.failure_fact()
            );
        }
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
        for names in [vec![], vec!["missing"], vec!["a", "A"]] {
            let error = resolve_columns(&request(names), std::slice::from_ref(&a))
                .unwrap_err()
                .to_query_service_error();
            assert_eq!(
                error.user_error().unwrap().code().as_str(),
                "sql.analyze.invalid_analyze_property"
            );
        }
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

mod statement;
pub(crate) use statement::StatisticsStatementExecutor;
