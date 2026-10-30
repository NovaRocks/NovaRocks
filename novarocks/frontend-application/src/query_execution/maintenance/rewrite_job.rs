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

//! Actual Optimize Root and Product borrow the common background Action host.
use super::*;
use super::{
    io_admission::{MetadataIoAdmission, extend_original_dependencies},
    rewrite_computation::{ReadyRewriteCohort, RewriteCohortComputation},
    rewrite_preparation::RewritePreparationCustody,
};
use crate::query::statement_computation::StatementNativeTail;
use crate::query::statement_publication::{CommittedWriteEffect, StatementPublication};
use crate::query_execution::{
    completion_facts::FrontendCompletionCustody, logical_execution::InitialComputationReservation,
};
use novarocks_query_application::{
    admitted_query_context::{QueryExecutionContext, QueryResultCapacityBinding},
    api::StatementNativeHealthFailure,
    session_control::{GovernedActionContext, GovernedBackgroundAction, GovernedStatementResidual},
    session_error::{QueryServiceError, QueryServiceErrorKind},
};
use novarocks_spi::connector::{
    ConnectorDistributedRewriteLease, ConnectorRequestContext, ConnectorRewritePlanSource,
    ConnectorRewritePrepared, ConnectorRewriteSelection, ConnectorRewriteSourceRequest,
    LakePublicationDisposition, ProviderMetadataCpuPort, RewriteCpuPort,
};
use novarocks_table_maintenance::worker::OptimizeJobOriginals;
use novarocks_table_maintenance::{OptimizeJob, OptimizeJobOutcome, OptimizeProcessRuntime};
use novarocks_table_maintenance::{product::RewriteFailure, runtime::TerminalError};
use novarocks_workload_control::{WorkError, WorkScope};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll},
};

enum OriginalProductOutcome {
    Job(OptimizeJobOutcome),
    Direct(MaintenanceActionOutcome),
}
type ProductFuture =
    Pin<Box<dyn Future<Output = Result<OriginalProductOutcome, TerminalError>> + Send>>;
type CloseFuture = Pin<Box<dyn Future<Output = Result<(), StatementNativeHealthFailure>> + Send>>;
#[derive(Debug, Default)]
struct RewriteWake(futures::task::AtomicWaker);
impl crate::task_execution::status_intake::StatusIntakeWake for RewriteWake {
    fn wake(&self) {
        self.0.wake();
    }
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum CohortPhase {
    Requested,
    Admitted,
    Prepared,
    Refused,
    Complete,
}
struct CohortCall {
    ordinal: usize,
    phase: CohortPhase,
    engine: Option<RequestScopedMaintenanceEngine>,
    prepared: Option<PreparedDistributedRewriteCohort>,
    result: Option<Result<(), TerminalError>>,
    ready: Option<ReadyRewriteCohort>,
}
/// One original Source. A Product/session facade only borrows an Arc of this
/// same owner. No session, operation, input or output belongs solely to its F.
pub(crate) struct RewriteDomain {
    engine: RequestScopedMaintenanceEngine,
    root: WorkScope,
    context: ConnectorRequestContext,
    lease: ConnectorDistributedRewriteLease,
    cpu: Arc<dyn RewriteCpuPort>,
    binding_cpu: Arc<dyn ProviderMetadataCpuPort>,
    io: Arc<MetadataIoAdmission>,
    wake: Arc<RewriteWake>,
    plan_source: Option<Box<dyn ConnectorRewritePlanSource>>,
    prepared: Option<ConnectorRewritePrepared>,
    session:
        Option<crate::query_execution::distributed_rewrite::ConnectorDistributedRewriteSession>,
    original_write: Arc<
        std::sync::Mutex<Option<Arc<crate::query_execution::write_session::ConnectorWriteSession>>>,
    >,
    begin: Arc<crate::query_execution::write_session::WriteBeginReceiving>,
    begin_source: Option<Arc<dyn novarocks_spi::connector::ConnectorRewriteWriteBeginSource>>,
    write_stack: Option<novarocks_catalog_application::ConnectorWriteStackLease>,
    seal: Option<Arc<super::rewrite_preparation::RewriteCohortCpuOriginal>>,
    provider: Option<Arc<FrontendCompletionCustody>>,
    preparation: RewritePreparationCustody,
    cohort: Option<CohortCall>,
    computation: Option<RewriteCohortComputation>,
    // Actual cohort count determines receiving slots. Old Native tails retain
    // their own Stage and actual-exit custody.
    tails: Vec<Option<StatementNativeTail>>,
    publication: Option<StatementPublication>,
    publication_failure: Option<novarocks_failure_contract::SharedFailureCause>,
    action_failure:
        Option<Arc<novarocks_query_application::session_control::StatementActionFailure>>,
    receipt: Option<novarocks_spi::connector::ConnectorDistributedRewriteReceipt>,
    result: Option<Result<OriginalProductOutcome, TerminalError>>,
    health: Option<StatementNativeHealthFailure>,
    stopping: bool,
    stop_reason: Option<TerminalError>,
    #[cfg(novarocks_dev_controls)]
    observation: Option<crate::preparation_diagnostics::StatementObservationHandle>,
}
pub(crate) type RewriteDomainAlias = Arc<tokio::sync::Mutex<RewriteDomain>>;
pub(crate) type RewriteDomainView = Weak<tokio::sync::Mutex<RewriteDomain>>;
struct RewriteAction {
    state: Option<RewriteActionState>,
}
struct RewriteResidual {
    state: RewriteActionState,
}
struct RewriteActionState {
    domain: RewriteDomainAlias,
    io: Arc<MetadataIoAdmission>,
    wake: Arc<RewriteWake>,
    product: Option<ProductFuture>,
    product_result: Option<Result<OriginalProductOutcome, TerminalError>>,
    refused_close: Option<CloseFuture>,
    refused_result: Option<Result<(), StatementNativeHealthFailure>>,
    close: Option<CloseFuture>,
    close_result: Option<Result<(), StatementNativeHealthFailure>>,
    selected: Option<TerminalError>,
    product_exited: bool,
    close_exited: bool,
    disposition: Option<LakePublicationDisposition>,
    health: Option<StatementNativeHealthFailure>,
}
fn select_product_terminal(
    result: Result<OriginalProductOutcome, TerminalError>,
    selected: &mut Option<TerminalError>,
    stop_reason: &mut Option<TerminalError>,
    effect: Option<novarocks_coordination_contract::ExecutionEffect>,
) -> Result<OriginalProductOutcome, TerminalError> {
    use novarocks_execution_contract::failure::FailureEvidence;
    use novarocks_table_maintenance::runtime::MaintenanceJobState;

    let Err(mut product) = result else {
        // Necessary finalization or a real NoOp has already succeeded.
        return result;
    };
    let evidence =
        |error: &TerminalError| error.selected_failure_fact().map(|fact| fact.evidence());
    let publication_terminal = matches!(
        product.state,
        MaintenanceJobState::KnownUncommitted
            | MaintenanceJobState::CommitUnknown
            | MaintenanceJobState::KnownCommittedFinalizationFailed
    );
    let source = selected.as_ref().or(stop_reason.as_ref());
    let refine_stop = source.is_some_and(|source| {
        evidence(&product) == Some(FailureEvidence::StopObservation)
            && evidence(source) == Some(FailureEvidence::Originating)
    });
    let preserve_origin = source.is_some_and(|source| {
        evidence(&product) == Some(FailureEvidence::Originating)
            && evidence(source) == Some(FailureEvidence::StopObservation)
    });
    if (publication_terminal && !refine_stop) || preserve_origin {
        // Product owns the publication frontier and its necessary-finalize
        // cause. Keep a later ordinary refusal in the original residual slot.
        return Err(product);
    }
    if let Some(mut cause) = selected.take().or_else(|| stop_reason.take()) {
        if publication_terminal {
            // Refine only a producer-authored stop observation. The original
            // publication state/effect remain authoritative, including NoOp.
            product.message = cause.message;
            product.compile_control = cause.compile_control;
            product.execution_failure = cause.execution_failure;
            product.failure_cause = cause.failure_cause;
            Err(product)
        } else {
            // Preserve the existing Source restoration/first-selected policy
            // for ordinary pre-dispatch or computation failures.
            if let Some(effect) = effect {
                cause = cause.with_effect(effect);
            }
            Err(cause)
        }
    } else {
        Err(product)
    }
}

fn trace_original_work_refusal(site: &'static str, error: &WorkError) {
    // WorkError has a closed first-party projection; never format the cause.
    let fact = novarocks_failure_contract::FailureProjection::failure_fact(error);
    tracing::debug!(
        target: "novarocks_frontend_application::maintenance_error",
        site,
        failure_domain = fact.reason().domain(),
        failure_reason = fact.reason().code(),
        "Original Rewrite work admission refused"
    );
}

fn service(error: impl ToString) -> QueryServiceError {
    QueryServiceError::new(QueryServiceErrorKind::Internal, error.to_string())
}
pub(crate) fn computation_service_error(error: TerminalError) -> QueryServiceError {
    match (
        error.failure_cause,
        error.execution_failure,
        error.compile_control,
    ) {
        (Some(cause), _, _) => QueryServiceError::from_failure_cause(cause),
        (_, Some(input), _) => QueryServiceError::from_execution_failure(input),
        (_, _, Some(control)) => QueryServiceError::from_compile_control(control),
        _ => service(error.message),
    }
}
// Only the Provider's explicit public projection may replace the neutral
// detail. Reading it invokes no foreign formatter or new failure selection.
fn from_connector_cause(error: novarocks_spi::connector::ConnectorError) -> TerminalError {
    let public_detail = error.public_failure_detail().map(str::to_owned);
    let mut terminal = TerminalError::from_original_cause(error);
    if let Some(detail) = public_detail {
        terminal.message = detail;
    }
    terminal
}
fn health(
    detail: &'static str,
    cause: impl std::error::Error + Send + Sync + 'static,
) -> StatementNativeHealthFailure {
    StatementNativeHealthFailure::actual_exit_unproven(detail, Arc::new(cause))
}
fn background_start_error(
    error: novarocks_query_application::api::QueryExecutionError,
) -> TerminalError {
    // Application refusals do not carry a domain fact. Preserve the original
    // execution input when present; never invent a fact from its public kind.
    TerminalError::pre_dispatch_failed(error.message())
        .with_execution_failure(error.execution_failure().cloned())
}

pub(crate) fn guard_terminal_error(error: QueryServiceError) -> TerminalError {
    let mut terminal = computation_error(error);
    terminal.state = novarocks_table_maintenance::runtime::MaintenanceJobState::PreDispatchFailed;
    terminal.effect = Some(novarocks_coordination_contract::ExecutionEffect::None);
    terminal
}

pub(crate) fn computation_error(error: QueryServiceError) -> TerminalError {
    if let Some(cause) = error.failure_cause() {
        return TerminalError::failed(error.to_string()).with_failure_cause(Some(cause.clone()));
    }
    if let Some(input) = error.execution_failure() {
        return TerminalError::failed(error.to_string())
            .with_execution_failure(Some(input.clone()));
    }
    if let Some(control) = error.compile_control_error() {
        return TerminalError::from_compile_control(control.clone());
    }
    TerminalError::failed(error.to_string())
}
impl RewriteDomain {
    fn poll_cohort(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        let Some(call) = self.cohort.as_mut() else {
            return Poll::Ready(Ok(()));
        };
        if self.stopping && call.phase != CohortPhase::Complete {
            call.result = Some(Err(self.stop_reason.clone().unwrap_or_else(|| {
                TerminalError::cancelled_before_dispatch("Rewrite original Source is closing")
            })));
            call.phase = CohortPhase::Refused;
        }
        if matches!(call.phase, CohortPhase::Complete | CohortPhase::Refused) {
            return Poll::Ready(Ok(()));
        }
        if call.phase == CohortPhase::Requested {
            if self.computation.is_none() {
                let original = self
                    .session
                    .as_ref()
                    .ok_or_else(|| service("Rewrite cohort has no original session"))?;
                let write = original
                    .write_session()
                    .ok_or_else(|| service("Rewrite empty plan cannot request a cohort"))?;
                let runtime = self.engine.kernel.rewrite_runtime().map_err(service)?;
                let initial = InitialComputationReservation::new(
                    runtime.launcher.reserve_identity().map_err(service)?,
                )
                .map_err(service)?;
                self.computation = Some(RewriteCohortComputation::empty(
                    self.root.clone(),
                    initial,
                    runtime.launcher.clone(),
                    write.clone(),
                    #[cfg(novarocks_dev_controls)]
                    self.observation.clone(),
                ));
            }
            let work = owner
                .execution_owner_slot()
                .as_ref()
                .ok_or_else(|| service("Rewrite cohort lost its original Root"))?;
            std::task::ready!(
                self.computation
                    .as_mut()
                    .unwrap()
                    .poll_admit(&self.io, work, cx)
            )?;
            let computation = self.computation.as_ref().unwrap();
            let execution = computation
                .preparation_execution(&self.engine.execution)
                .map_err(service)?;
            let cancellation =
                novarocks_query_application::cancellation::QueryCancellationView::governed(
                    execution
                        .result_capacity()
                        .unwrap()
                        .scope()
                        .cancellation()
                        .map_err(service)?,
                    None,
                );
            let runtime = self
                .engine
                .kernel
                .rewrite_runtime()
                .map_err(service)?
                .execution
                .0
                .runtime();
            let context = crate::connector::query_connector_request_context_on_runtime(
                runtime,
                self.context.deadline(),
                cancellation,
            )
            .map_err(service)?
            .with_io_admission(self.io.clone());
            let context = computation
                .preparation_context(
                    context,
                    #[cfg(novarocks_dev_controls)]
                    execution.test_statement_nomination(),
                )
                .map_err(service)?;
            call.engine = Some(RequestScopedMaintenanceEngine::new(
                self.engine.kernel.clone(),
                execution,
                context,
            ));
            call.phase = CohortPhase::Admitted;
            cx.waker().wake_by_ref();
            return Poll::Ready(Ok(()));
        }
        if call.phase == CohortPhase::Admitted {
            return Poll::Ready(Ok(()));
        }
        let computation = self.computation.as_mut().unwrap();
        if let Some(prepared) = call.prepared.take() {
            computation.install_prepared(prepared)?;
            drop(call.engine.take());
        }
        let runtime = self.engine.kernel.rewrite_runtime().map_err(service)?;
        match computation.poll_ready(&runtime.execution, &self.io, owner, cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => {
                call.result = Some(Err(computation_error(error)));
                call.phase = CohortPhase::Refused;
            }
            Poll::Ready(Ok(())) => {
                // The actual tail receiving backing was paid before plan transfer.
                let tail = self
                    .tails
                    .get_mut(call.ordinal)
                    .ok_or_else(|| service("Rewrite tail receiver was not paid"))?;
                computation
                    .handoff_ready_in(&mut call.ready, tail)
                    .map_err(|failure| {
                        self.health = Some(failure.clone());
                        service(failure.detail())
                    })?;
                self.computation.take();
                call.result = Some(Ok(()));
                call.phase = CohortPhase::Complete;
            }
        }
        cx.waker().wake_by_ref();
        Poll::Ready(Ok(()))
    }
}
impl RewriteActionState {
    fn new(
        domain: RewriteDomainAlias,
        io: Arc<MetadataIoAdmission>,
        wake: Arc<RewriteWake>,
        product: ProductFuture,
    ) -> Self {
        Self {
            domain,
            io,
            wake,
            product: Some(product),
            product_result: None,
            refused_close: None,
            refused_result: None,
            close: None,
            close_result: None,
            selected: None,
            product_exited: false,
            close_exited: false,
            disposition: None,
            health: None,
        }
    }
    fn poll_refused_close(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if self.refused_close.is_none() && self.refused_result.is_none() {
            let refused = self.domain.try_lock().ok().is_some_and(|source| {
                source
                    .cohort
                    .as_ref()
                    .is_some_and(|call| call.phase == CohortPhase::Refused)
            });
            if refused {
                self.refused_close = Some(Box::pin(close_refused_cohort(self.domain.clone())));
            }
        }
        if let Some(original) = self.refused_close.as_mut()
            && let Poll::Ready(result) = original.as_mut().poll(cx)
        {
            self.refused_result = Some(result);
            drop(self.refused_close.take());
        }
        if let Some(Err(failure)) = self.refused_result.as_ref() {
            self.health = Some(failure.clone());
            return Poll::Pending;
        }
        if self.refused_result.as_ref().is_some_and(Result::is_ok) {
            let Ok(mut source) = self.domain.try_lock() else {
                return Poll::Pending;
            };
            if let Some(call) = source.cohort.as_mut() {
                call.phase = CohortPhase::Complete;
            }
            self.refused_result.take();
            cx.waker().wake_by_ref();
        }
        Poll::Ready(Ok(()))
    }
    fn poll_product(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        self.wake.0.register(cx.waker());
        if let Some(original) = owner.execution_owner_slot().as_ref()
            && let Err(error) = self.io.poll_progress(original, cx)
            && self.selected.is_none()
        {
            self.selected = Some(TerminalError::from_original_cause(error));
        }
        if self.selected.is_some()
            && let Ok(mut source) = self.domain.try_lock()
        {
            source.stopping = true;
            if source.stop_reason.is_none() {
                source.stop_reason = self.selected.clone();
            }
        }
        if !self.product_exited
            && let Poll::Ready(result) = self
                .product
                .as_mut()
                .expect("same original Rewrite Product F")
                .as_mut()
                .poll(cx)
        {
            self.product_result = Some(result);
            // The actual result is stable before the original F Drop,
            // including when another negative observer borrows Domain.
            drop(self.product.take());
            self.product_exited = true;
        }
        if let Ok(mut source) = self.domain.try_lock() {
            if let Some(result) = self.product_result.take() {
                let effect = source.publication.as_ref().and_then(original_effect);
                source.result = Some(select_product_terminal(
                    result,
                    &mut self.selected,
                    &mut source.stop_reason,
                    effect,
                ));
            }
            if let Poll::Ready(Err(error)) = source.poll_cohort(owner, cx) {
                let cause = source
                    .stop_reason
                    .clone()
                    .unwrap_or_else(|| computation_error(error));
                if let Some(call) = source.cohort.as_mut() {
                    if call.result.is_none() {
                        call.result = Some(Err(cause));
                    }
                    call.phase = CohortPhase::Refused;
                }
            }
            self.disposition = source
                .publication
                .as_ref()
                .and_then(StatementPublication::disposition);
        }
        let _ = self.poll_refused_close(cx);
        if self.product_exited && self.product_result.is_none() {
            let Ok(source) = self.domain.try_lock() else {
                return Poll::Pending;
            };
            match source.result.as_ref() {
                Some(Ok(_)) => Poll::Ready(Ok(())),
                Some(Err(cause)) => Poll::Ready(Err(computation_service_error(cause.clone()))),
                None => Poll::Pending,
            }
        } else {
            Poll::Pending
        }
    }
    fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.wake.0.register(cx.waker());
        if let Ok(mut source) = self.domain.try_lock() {
            source.stopping = true;
            if let Some(call) = source.cohort.as_mut()
                && call.phase != CohortPhase::Complete
            {
                if call.result.is_none() {
                    call.result = Some(Err(TerminalError::cancelled_before_dispatch(
                        "Rewrite original Source is closing",
                    )));
                }
                call.phase = CohortPhase::Refused;
            }
        }
        if let Err(error) = self.io.poll_continuing(cx) {
            // Preserve the ordinary refusal separately; only the actual
            // quiescence author below can establish or reject local exit.
            self.selected
                .get_or_insert_with(|| TerminalError::from_original_cause(error));
        }
        if let Some(failure) = self.io.health_failure() {
            self.health.get_or_insert(failure);
        }
        let _ = self.poll_refused_close(cx);
        if self.health.is_some() {
            return Poll::Pending;
        }
        if !self.product_exited {
            match self
                .product
                .as_mut()
                .expect("same retained Rewrite Product F")
                .as_mut()
                .poll(cx)
            {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    self.product_result = Some(result);
                }
            }
            drop(self.product.take());
            self.product_exited = true;
        }
        if self.product_result.is_some() {
            let Ok(mut source) = self.domain.try_lock() else {
                return Poll::Pending;
            };
            source.result = self.product_result.take();
        }
        if !self.close_exited {
            if self.close.is_none() {
                self.close = Some(Box::pin(close_original(self.domain.clone())));
            }
            if let Poll::Ready(result) = self.close.as_mut().unwrap().as_mut().poll(cx) {
                self.close_result = Some(result);
                drop(self.close.take());
                self.close_exited = true;
            } else {
                return Poll::Pending;
            }
            if let Some(Err(failure)) = self.close_result.as_ref() {
                self.health = Some(failure.clone());
                return Poll::Pending;
            }
        }
        // Actual quiescence is separate from a business admission refusal.
        let _observed = std::task::ready!(self.io.poll_actual_quiescent_continuing(cx))?;
        Poll::Ready(Ok(()))
    }
    fn local_health(&self) -> Option<StatementNativeHealthFailure> {
        self.health
            .clone()
            .or_else(|| self.io.health_failure())
            .or_else(|| {
                self.domain.try_lock().ok().and_then(|source| {
                    source
                        .health
                        .clone()
                        .or_else(|| {
                            source.cpu.local_failure().map(|cause| {
                                StatementNativeHealthFailure::actual_exit_unproven(
                                    "Rewrite CPU exit is unproven",
                                    cause,
                                )
                            })
                        })
                        .or_else(|| {
                            source
                                .provider
                                .as_ref()
                                .and_then(|p| p.local_health_failure())
                        })
                        .or_else(|| {
                            source
                                .computation
                                .as_ref()
                                .and_then(RewriteCohortComputation::local_health_failure)
                        })
                        .or_else(|| {
                            source
                                .tails
                                .iter()
                                .flatten()
                                .find_map(StatementNativeTail::local_health_failure)
                        })
                })
            })
    }
}
impl GovernedBackgroundAction for RewriteAction {
    fn poll_action(
        &mut self,
        owner: &mut GovernedActionContext<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryServiceError>> {
        self.state.as_mut().unwrap().poll_product(owner, cx)
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
        self.state
            .as_ref()
            .and_then(RewriteActionState::local_health)
    }
    fn handoff_residual_in(
        &mut self,
        target: &mut Option<Box<dyn GovernedStatementResidual>>,
    ) -> Result<(), StatementNativeHealthFailure> {
        if target.is_some() {
            return Err(health(
                "Rewrite residual receiver is occupied",
                WorkError::Conflict,
            ));
        }
        if let Some(state) = self.state.take() {
            *target = Some(Box::new(RewriteResidual { state }));
        }
        Ok(())
    }
}
impl GovernedStatementResidual for RewriteResidual {
    fn poll_retirement(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.state.poll_close(cx)
    }
}

// Same pure forming author for the original Job and SQL Root. No work,
// permit or physical action is created here; all callers hold positive MIO.
fn form_original_domain(
    engine: RequestScopedMaintenanceEngine,
    root: WorkScope,
    lease: ConnectorDistributedRewriteLease,
    io: Arc<MetadataIoAdmission>,
    target: &MaintenanceTarget,
    #[cfg(novarocks_dev_controls)] observation: Option<
        crate::preparation_diagnostics::StatementObservationHandle,
    >,
) -> Result<RewriteDomainAlias, TerminalError> {
    let runtime = engine
        .kernel
        .rewrite_runtime()
        .map_err(|error| {
            trace_original_work_refusal("tm.domain.runtime", &error);
            TerminalError::from_original_cause(error)
        })?
        .clone();
    let capacity = io.root_capacity().map_err(|error| {
        trace_original_work_refusal("tm.domain.capacity", &error);
        TerminalError::from_original_cause(error)
    })?;
    if !capacity.window_alias().is_for_scope(&root) {
        return Err(TerminalError::pre_dispatch_failed(
            "Rewrite MIO belongs to another original Root",
        ));
    }
    let execution = if engine.execution.result_capacity().is_some() {
        engine.execution.for_statement_retention(capacity.clone())
    } else {
        engine
            .execution
            .clone()
            .with_result_capacity(capacity.clone())
    }
    .map_err(|error| {
        trace_original_work_refusal("tm.domain.retention", &error);
        TerminalError::from_original_cause(error)
    })?;
    let context = engine
        .connector_context
        .clone()
        .with_io_admission(io.clone());
    let engine = RequestScopedMaintenanceEngine::new(
        engine.kernel.clone(),
        execution.clone(),
        context.clone(),
    );
    let wake = Arc::new(RewriteWake::default());
    let cpu = runtime
        .execution
        .0
        .provider_creator_cpu()
        .map_err(|error| {
            trace_original_work_refusal("tm.domain.cpu_runtime", &error);
            TerminalError::from_original_cause(error)
        })?;
    let write_stack = engine
        .kernel
        .typed_connector_control()
        .acquire_exact_write_stack(lease.planning_lease().control_runtime_id())
        .map_err(TerminalError::from_original_cause)?;
    let (cpu, binding_cpu) = cpu
        .rewrite_cpu_ports(
            &root,
            capacity.window_alias(),
            &lease,
            &write_stack,
            execution.cancellation().clone(),
            runtime.execution.0.runtime().clone(),
            wake.clone(),
        )
        .map_err(|error| {
            trace_original_work_refusal("tm.domain.cpu_ports", &error);
            TerminalError::from_original_cause(error)
        })?;
    let original_write = Arc::new(std::sync::Mutex::new(None));
    let begin =
        crate::query_execution::write_session::WriteBeginReceiving::new(original_write.clone());
    let publication = StatementPublication::new(crate::dml::attempt::DmlPublicationAttempt::new(
        novarocks_spi::connector::LakePublicationId::new_v7(),
        novarocks_spi::connector::LakePublicationFamily::MetadataMaintenance,
        novarocks_spi::connector::LakePublicationTarget::try_new(
            target.catalog.clone(),
            target.namespace.clone(),
            Some(target.table.clone()),
            None,
        )
        .map_err(TerminalError::from_original_cause)?,
        None,
    ));
    let domain = Arc::new(tokio::sync::Mutex::new(RewriteDomain {
        engine,
        root: root.clone(),
        context,
        lease,
        cpu,
        binding_cpu,
        io: io.clone(),
        wake: wake.clone(),
        plan_source: None,
        prepared: None,
        session: None,
        original_write,
        begin,
        begin_source: None,
        write_stack: Some(write_stack),
        seal: None,
        provider: None,
        preparation: RewritePreparationCustody::default(),
        cohort: None,
        computation: None,
        tails: Vec::new(),
        publication: Some(publication),
        publication_failure: None,
        action_failure: None,
        receipt: None,
        result: None,
        health: None,
        stopping: false,
        stop_reason: None,
        #[cfg(novarocks_dev_controls)]
        observation,
    }));
    Ok(domain)
}

/// No external I/O happens before start_background_action_in transfers the
/// SAME original owner/context/permit to the existing background Source.
pub(crate) async fn mount_original(
    mut adapter: Box<crate::table_maintenance::worker::FrontendOptimizeJobExecution>,
    jobs: Arc<OptimizeProcessRuntime>,
    mut originals: OptimizeJobOriginals,
    job: OptimizeJob,
) -> Result<OptimizeJobOutcome, TerminalError> {
    let root = originals
        .owner
        .as_ref()
        .ok_or_else(|| TerminalError::pre_dispatch_failed("Optimize original Root is empty"))?
        .scope();
    let engine = adapter.engine.clone();
    let runtime = engine
        .kernel
        .rewrite_runtime()
        .map_err(TerminalError::from_original_cause)?
        .clone();
    let identity = RequestScopedMaintenanceEngine::target_identity(&job.target)
        .map_err(TerminalError::pre_dispatch_failed)?;
    let lease = engine
        .kernel
        .connector_control()
        .acquire_current_distributed_rewrite(&identity.instance_id)
        .map_err(TerminalError::from_original_cause)?;
    let mut dependencies = lease
        .planning_initial_dependencies()
        .map_err(TerminalError::from_original_cause)?;
    let write = engine
        .kernel
        .typed_connector_control()
        .acquire_exact_write_stack(lease.planning_lease().control_runtime_id())
        .map_err(TerminalError::from_original_cause)?;
    extend_original_dependencies(
        &mut dependencies,
        &write
            .session()
            .rewrite_begin_initial_dependencies()
            .map_err(TerminalError::from_original_cause)?,
    );
    let io =
        MetadataIoAdmission::for_original_source(originals.owner.as_ref().unwrap(), dependencies)
            .map_err(TerminalError::from_original_cause)?;
    std::future::poll_fn(
        |cx| match io.poll_progress(originals.owner.as_ref().unwrap(), cx) {
            Ok(true) => Poll::Ready(Ok(())),
            Ok(false) => Poll::Pending,
            Err(e) => Poll::Ready(Err(TerminalError::from_original_cause(e))),
        },
    )
    .await?;
    let domain = form_original_domain(
        engine,
        root.clone(),
        lease,
        io.clone(),
        &job.target,
        // Background admission has its own job controls, not a SQL observation.
        #[cfg(novarocks_dev_controls)]
        None,
    )?;
    let (capacity, wake, execution) = {
        let source = domain.try_lock().map_err(|_| {
            TerminalError::pre_dispatch_failed("new Rewrite Domain is already borrowed")
        })?;
        (
            source.engine.execution.result_capacity().unwrap().clone(),
            source.wake.clone(),
            source.engine.execution.clone(),
        )
    };
    adapter.engine = domain
        .try_lock()
        .map_err(|_| TerminalError::pre_dispatch_failed("new Rewrite Domain is already borrowed"))?
        .engine
        .clone();
    adapter.mounted = Some(Arc::downgrade(&domain));
    let adapter: Arc<crate::table_maintenance::worker::FrontendOptimizeJobExecution> =
        Arc::from(adapter);
    let job_id = job.job_id;
    let product: ProductFuture = Box::pin(run_original_product(
        adapter,
        jobs.clone(),
        root,
        capacity,
        job,
    ));
    let mut action: Option<Box<dyn GovernedBackgroundAction>> = Some(Box::new(RewriteAction {
        state: Some(RewriteActionState::new(domain.clone(), io, wake, product)),
    }));
    let mut context = Some(execution);
    // Actual data custody stays in MIO/Domain. The host receives that same
    // positive Root retention context, with no additional legacy payload.
    let mut additional_payload = None;
    let mut input = None;
    let mut ready = None;
    let mut exit = None;
    runtime
        .client
        .prepare_background_action_in(
            &originals.owner,
            &mut context,
            &mut originals.query_concurrency,
            &mut additional_payload,
            &mut action,
            &mut input,
        )
        .map_err(background_start_error)?;
    let tail = runtime
        .client
        .start_background_action_in(&mut input, &mut originals.owner, &mut ready, &mut exit)
        .map_err(background_start_error)?;
    // Same original TM record receives observation before any waiter or the
    // Starting F can disappear. Physical custody stays in the common Source.
    jobs.install_original_exit_in(job_id, &mut exit)
        .map_err(|e| TerminalError::failed(e.to_string()))?;
    tail.finish()
        .map_err(|e| TerminalError::failed(e.to_string()))?;
    let receipt = match ready
        .as_mut()
        .ok_or_else(|| TerminalError::failed("Optimize host returned no original Ready source"))?
        .await
    {
        Ok(receipt) => receipt,
        Err(failure) => {
            domain.lock().await.action_failure = Some(failure);
            return Err(TerminalError::failed(
                "Optimize original Action failed; original health retained",
            ));
        }
    };
    let result = domain
        .lock()
        .await
        .result
        .take()
        .ok_or_else(|| TerminalError::failed("Optimize original Product result is absent"))?;
    receipt.finish();
    match result {
        Ok(OriginalProductOutcome::Job(outcome)) => Ok(outcome),
        Ok(OriginalProductOutcome::Direct(_)) => Err(TerminalError::failed(
            "Job Source received a direct SQL product outcome",
        )),
        Err(cause) => Err(cause),
    }
}
async fn run_original_product(
    adapter: Arc<crate::table_maintenance::worker::FrontendOptimizeJobExecution>,
    jobs: Arc<OptimizeProcessRuntime>,
    root: WorkScope,
    capacity: QueryResultCapacityBinding,
    job: OptimizeJob,
) -> Result<OriginalProductOutcome, TerminalError> {
    novarocks_table_maintenance::worker::execute_claimed_job(
        jobs.as_ref(),
        adapter.as_ref(),
        &root,
        capacity,
        job,
    )
    .await
    .map(OriginalProductOutcome::Job)
}

#[path = "rewrite_job/direct.rs"]
pub(crate) mod direct;

fn upgrade_original(view: &RewriteDomainView) -> Result<RewriteDomainAlias, TerminalError> {
    view.upgrade()
        .ok_or_else(|| TerminalError::pre_dispatch_failed("Rewrite original Source has retired"))
}

pub(crate) async fn actual_original_effect(
    view: &RewriteDomainView,
) -> Option<novarocks_coordination_contract::ExecutionEffect> {
    let source = view.upgrade()?;
    let original = source.lock().await;
    if original.session.as_ref().is_some_and(|s| s.is_noop()) {
        return Some(novarocks_coordination_contract::ExecutionEffect::None);
    }
    original.publication.as_ref().and_then(original_effect)
}

pub(crate) async fn rebind_original(
    view: &RewriteDomainView,
    job: &OptimizeJob,
) -> Result<MaintenanceTargetRebind, String> {
    use novarocks_spi::connector::{
        ConnectorTableObjectId, ConnectorTableObjectRebindRequest, ConnectorTableObjectSelector,
        ConnectorTableResolution,
    };
    let source = upgrade_original(view).map_err(|e| e.to_string())?;
    let original = source.lock().await;
    if original.stopping {
        return Err("Rewrite original Source is closing".into());
    }
    let object = ConnectorTableObjectId::try_new(bytes::Bytes::copy_from_slice(&job.object_id))
        .map_err(|e| e.to_string())?;
    let identity = RequestScopedMaintenanceEngine::target_identity(&job.target)?;
    let binding = original
        .lease
        .metadata()
        .rebind_table_object_with_cpu(
            ConnectorTableObjectRebindRequest {
                table: identity,
                expected_object_id: object,
                resolution: ConnectorTableResolution::StrictBaseTable,
                selector: ConnectorTableObjectSelector::Current,
                context: original.context.clone(),
            },
            original.binding_cpu.clone(),
        )
        .await;
    maintenance_target_rebind_from_connector_result(binding.map(|_| ()))
}

pub(crate) struct GovernedRewriteEffectPort {
    original: RewriteDomainView,
}
impl GovernedRewriteEffectPort {
    pub(crate) fn new(original: RewriteDomainView) -> Self {
        Self { original }
    }
}
struct GovernedRewriteSession {
    original: RewriteDomainAlias,
    facts: novarocks_table_maintenance::product::RewritePlanFacts,
}

async fn begin_original(
    view: &RewriteDomainView,
    target: &MaintenanceTarget,
    intent: novarocks_table_maintenance::product::RewriteIntent,
    operation_id: novarocks_spi::connector::ConnectorWriteOperationId,
) -> Result<GovernedRewriteSession, TerminalError> {
    use crate::query_execution::write_session::ConnectorWriteSession;
    use novarocks_spi::connector::RewriteCpuInput;
    let source = upgrade_original(view)?;
    let mut original = source.lock().await;
    if original.stopping || original.plan_source.is_some() || original.session.is_some() {
        return Err(TerminalError::pre_dispatch_failed(
            "Rewrite Source already owns its original invocation",
        ));
    }
    let identity = RequestScopedMaintenanceEngine::target_identity(target)
        .map_err(TerminalError::pre_dispatch_failed)?;
    let selection = match intent {
        novarocks_table_maintenance::product::RewriteIntent::DataFiles { rewrite_all } => {
            ConnectorRewriteSelection::DataFiles { rewrite_all }
        }
        novarocks_table_maintenance::product::RewriteIntent::PositionDeletes {
            rewrite_all,
            min_input_files,
        } => ConnectorRewriteSelection::PositionDeletes {
            rewrite_all,
            min_input_files,
        },
    };
    let mut request = Some(ConnectorRewriteSourceRequest {
        operation_id,
        table: identity,
        selection,
        context: original.context.clone(),
    });
    let lease = original.lease.clone();
    // The entire request/source is in the original Action before its first poll.
    lease
        .prepare_plan_source_in(&mut request, &mut original.plan_source)
        .map_err(from_connector_cause)?;
    let capture = super::rewrite_capture::borrow_positive_capture(&original.io, &original.context)
        .map_err(TerminalError::from_original_cause)?;
    let cpu = original.cpu.clone();
    let metadata_cpu = original.binding_cpu.clone();
    original
        .plan_source
        .as_mut()
        .unwrap()
        .prepare(cpu.as_ref(), &metadata_cpu, &capture)
        .await
        .map_err(from_connector_cause)?;
    let RewriteDomain {
        plan_source,
        prepared,
        ..
    } = &mut *original;
    plan_source
        .as_mut()
        .unwrap()
        .take_preparation_in(prepared)
        .map_err(from_connector_cause)?;
    let prepared = original.prepared.as_ref().unwrap();
    let plan = prepared.plan.clone();
    let count = plan.cohorts().len();
    let mut tails = Vec::new();
    tails.try_reserve_exact(count).map_err(|_| {
        TerminalError::from_compile_control(
            novarocks_type_contract::CompileControlError::ResourceExhausted,
        )
    })?;
    tails.resize_with(count, || None);
    original.tails = tails;
    if count != 0 {
        let prepared = original.prepared.as_ref().unwrap();
        let request_source =
            super::rewrite_preparation::RewriteCohortCpuOriginal::begin_request_original(
                plan.clone(),
                lease.clone(),
                prepared.table.identity.clone(),
                original.context.clone(),
                prepared.capture.clone(),
            );
        original.seal = Some(request_source.clone());
        let mut input = Some(novarocks_spi::connector::RewriteCpuInput(
            request_source.clone(),
        ));
        cpu.submit_in(&mut input)
            .map_err(TerminalError::from_original_cause)?;
        std::future::poll_fn(|cx| cpu.poll_finished(cx))
            .await
            .map_err(TerminalError::from_original_cause)?;
        let mut formed = None;
        request_source
            .take_begin_request_in(&mut formed)
            .map_err(crate::table_maintenance::worker::rewrite_compile_terminal)?;
        let (request, properties) = formed.expect("same original CPU request receiving");
        // The actual CPU turn joined; the original request has moved into
        // its receiving path. No stale Source overlaps the later seal Source.
        drop(original.seal.take());
        drop(request_source);
        let stack = original
            .write_stack
            .take()
            .expect("same selected original write lease");
        let capture = original.prepared.as_ref().unwrap().capture.clone();
        let begin = original.begin.clone();
        let metadata_cpu = original.binding_cpu.clone();
        ConnectorWriteSession::begin_rewrite_scoped_in(
            stack,
            properties,
            request,
            &begin,
            &mut original.begin_source,
            cpu.as_ref(),
            &metadata_cpu,
            capture,
        )
        .await
        .map_err(TerminalError::from_original_cause)?;
    }
    let write = original
        .original_write
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    original.seal = Some(
        super::rewrite_preparation::RewriteCohortCpuOriginal::seal_original(plan, lease, write),
    );
    let mut input = Some(RewriteCpuInput(original.seal.as_ref().unwrap().clone()));
    cpu.submit_in(&mut input)
        .map_err(TerminalError::from_original_cause)?;
    std::future::poll_fn(|cx| cpu.poll_finished(cx))
        .await
        .map_err(TerminalError::from_original_cause)?;
    let RewriteDomain { seal, session, .. } = &mut *original;
    seal.as_ref()
        .unwrap()
        .take_sealed_in(session)
        .map_err(crate::table_maintenance::worker::rewrite_compile_terminal)?;
    drop(original.seal.take());
    let facts = novarocks_table_maintenance::product::RewritePlanFacts {
        noop: original.session.as_ref().unwrap().is_noop(),
        cohort_count: count,
        input_bytes: original
            .session
            .as_ref()
            .unwrap()
            .plan()
            .summary()
            .input_bytes,
    };
    drop(original);
    Ok(GovernedRewriteSession {
        original: source,
        facts,
    })
}

async fn execute_original_cohort(
    source: &RewriteDomainAlias,
    ordinal: usize,
) -> Result<(), TerminalError> {
    {
        let mut original = source.lock().await;
        if original.stopping || original.cohort.is_some() || ordinal >= original.tails.len() {
            return Err(original.stop_reason.clone().unwrap_or_else(|| {
                TerminalError::pre_dispatch_failed("Rewrite cohort Source is unavailable")
            }));
        }
        original.cohort = Some(CohortCall {
            ordinal,
            phase: CohortPhase::Requested,
            engine: None,
            prepared: None,
            result: None,
            ready: None,
        });
        original.wake.0.wake();
    }
    std::future::poll_fn(|cx| {
        let Ok(original) = source.try_lock() else {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        };
        let call = original.cohort.as_ref().unwrap();
        if call.phase == CohortPhase::Complete {
            return Poll::Ready(Ok(false));
        }
        if call.phase == CohortPhase::Admitted {
            Poll::Ready(Ok(true))
        } else {
            Poll::Pending
        }
    })
    .await
    .and_then(|admitted| {
        if admitted {
            Ok(())
        } else {
            Err(TerminalError::cancelled_before_dispatch(
                "Rewrite cohort was stopped before preparation",
            ))
        }
    })?;
    {
        let mut original = source.lock().await;
        let engine = original
            .cohort
            .as_ref()
            .unwrap()
            .engine
            .as_ref()
            .unwrap()
            .clone();
        let session = original.session.as_ref().unwrap().clone();
        let id = session.plan().cohorts()[ordinal].cohort_id();
        let lease = original.lease.clone();
        let cpu = original.cpu.clone();
        // Previous preparation's actual worker/forwarder and Provider already
        // exited before its Ready was transferred. Retire those original
        // aliases before constructing their one serial successor.
        original.preparation = RewritePreparationCustody::default();
        drop(original.provider.take());
        original.provider = Some(FrontendCompletionCustody::new());
        let provider = original.provider.as_ref().unwrap().clone();
        let completion = engine
            .kernel
            .rewrite_runtime()
            .map_err(|e| TerminalError::failed(e.to_string()))?
            .completion
            .clone();
        let capacity = engine.execution.result_capacity().unwrap().clone();
        let result = super::rewrite_preparation::prepare_original_cohort_in(
            &mut original.preparation,
            &engine,
            &session,
            id,
            &lease,
            cpu.as_ref(),
            &provider,
            completion,
            capacity,
        )
        .await;
        match result {
            Err(error) => {
                original.cohort.as_mut().unwrap().result =
                    Some(Err(TerminalError::pre_dispatch_failed(error)));
                original.cohort.as_mut().unwrap().phase = CohortPhase::Refused;
            }
            Ok(()) => {
                let mut receiving = None;
                original
                    .preparation
                    .take_prepared_in(&mut receiving)
                    .map_err(TerminalError::pre_dispatch_failed)?;
                match receiving.unwrap() {
                    Ok(prepared) => {
                        original
                            .preparation
                            .retire_transferred()
                            .map_err(TerminalError::pre_dispatch_failed)?;
                        // The same Provider close completed before transfer.
                        // Its binding/CPU aliases cannot retain the old P into
                        // the successor's admission.
                        drop(original.provider.take());
                        let call = original.cohort.as_mut().unwrap();
                        call.prepared = Some(prepared);
                        call.phase = CohortPhase::Prepared;
                    }
                    Err(error) => {
                        let call = original.cohort.as_mut().unwrap();
                        call.result = Some(Err(
                            crate::table_maintenance::worker::rewrite_compile_terminal(error),
                        ));
                        call.phase = CohortPhase::Refused;
                    }
                }
            }
        }
        original.wake.0.wake();
    }
    std::future::poll_fn(|cx| {
        let Ok(mut original) = source.try_lock() else {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        };
        let call = original.cohort.as_mut().unwrap();
        if call.phase != CohortPhase::Complete {
            return Poll::Pending;
        }
        let result = call.result.take().expect("same original cohort result");
        // Ready was already appended once by the common Write consumer.
        // There is deliberately no legacy accumulate call on this route.
        drop(original.cohort.take());
        Poll::Ready(result)
    })
    .await
}

fn original_effect(
    publication: &StatementPublication,
) -> Option<novarocks_coordination_contract::ExecutionEffect> {
    match publication.committed_effect()? {
        CommittedWriteEffect::Applied(_) => {
            Some(novarocks_coordination_contract::ExecutionEffect::External)
        }
        CommittedWriteEffect::NoOp => Some(novarocks_coordination_contract::ExecutionEffect::None),
    }
}
fn receive_mutation_failure(
    target: &mut Option<novarocks_failure_contract::SharedFailureCause>,
    failure: &novarocks_spi::connector::ConnectorMutationFailure,
) -> RewriteFailure {
    let cause = target.get_or_insert_with(|| {
        novarocks_failure_contract::SharedFailureCause::new(failure.clone())
    });
    RewriteFailure::from_cause(cause.clone())
}
fn receive_connector_failure(
    target: &mut Option<novarocks_failure_contract::SharedFailureCause>,
    failure: &novarocks_spi::connector::ConnectorError,
) -> RewriteFailure {
    let cause = target.get_or_insert_with(|| {
        novarocks_failure_contract::SharedFailureCause::new(failure.clone())
    });
    RewriteFailure::from_cause(cause.clone())
}
fn project_rewrite_publication(
    publication: &StatementPublication,
    failure_receiving: &mut Option<novarocks_failure_contract::SharedFailureCause>,
) -> Result<novarocks_table_maintenance::product::RewriteCommit, RewriteFailure> {
    use novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion;
    use novarocks_spi::connector::{ConnectorWriteAbortOutcome, ExternalMutationFinalization};
    use novarocks_table_maintenance::product::RewriteCommit;
    if let Some(effect) = original_effect(publication) {
        let needed = publication
            .committed_needed()
            .expect("actual committed fact");
        return Ok(RewriteCommit::KnownCommitted {
            effect,
            finalization_failed: match needed.finalization() {
                ExternalMutationFinalization::Complete => None,
                ExternalMutationFinalization::Failed(failure) => {
                    Some(receive_mutation_failure(failure_receiving, failure))
                }
            },
        });
    }
    // Read the actual Provider outcome, not only a pre-dispatch refusal.
    let failure = match (&publication.outcome, &publication.abort_outcome) {
        (
            Some(Ok(
                ConnectorWritePublicationCompletion::KnownUncommitted { failure, .. }
                | ConnectorWritePublicationCompletion::CommitUnknown { failure, .. },
            )),
            _,
        ) => Some(receive_mutation_failure(failure_receiving, failure)),
        (_, Some(Ok(ConnectorWriteAbortOutcome::CommitUnknown { failure, .. }))) => {
            Some(receive_mutation_failure(failure_receiving, failure))
        }
        (Some(Err(error)), _) | (_, Some(Err(error))) => {
            Some(receive_connector_failure(failure_receiving, error))
        }
        _ => publication
            .refusal
            .as_ref()
            .map(|error| receive_connector_failure(failure_receiving, error)),
    };
    let disposition = publication.disposition().or({
        match (&publication.outcome, &publication.abort_outcome) {
            (Some(Ok(ConnectorWritePublicationCompletion::KnownUncommitted { .. })), _) => {
                Some(LakePublicationDisposition::KnownUncommitted)
            }
            (Some(Ok(ConnectorWritePublicationCompletion::CommitUnknown { .. })), _)
            | (_, Some(Ok(ConnectorWriteAbortOutcome::CommitUnknown { .. }))) => {
                Some(LakePublicationDisposition::CommitUnknown)
            }
            _ => None,
        }
    });
    Ok(match disposition {
        Some(LakePublicationDisposition::KnownUncommitted) => RewriteCommit::KnownUncommitted {
            // An abort without a Provider failure is an actual local verdict.
            failure: failure
                .unwrap_or_else(|| "Rewrite original publication is known uncommitted".into()),
        },
        Some(LakePublicationDisposition::CommitUnknown) => RewriteCommit::CommitUnknown {
            failure: failure
                .unwrap_or_else(|| "Rewrite original publication remains unknown".into()),
        },
        _ => {
            return Err(
                failure.unwrap_or_else(|| "Rewrite publication has no exact terminal fact".into())
            );
        }
    })
}
async fn settle_original(
    source: &RewriteDomainAlias,
    abort: bool,
) -> Result<novarocks_table_maintenance::product::RewriteCommit, RewriteFailure> {
    std::future::poll_fn(|cx| {
        let Ok(mut original) = source.try_lock() else {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        };
        let context = original.context.clone();
        let session = original
            .original_write
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let publication = original
            .publication
            .as_mut()
            .ok_or_else(|| RewriteFailure::from("Rewrite original publication is empty"))?;
        std::task::ready!(publication.poll_settle(session.as_ref(), &context, abort, cx));
        let RewriteDomain {
            publication,
            publication_failure,
            ..
        } = &mut *original;
        Poll::Ready(project_rewrite_publication(
            publication.as_ref().unwrap(),
            publication_failure,
        ))
    })
    .await
}

impl novarocks_table_maintenance::product::DistributedRewriteSession for GovernedRewriteSession {
    fn plan_facts(&self) -> novarocks_table_maintenance::product::RewritePlanFacts {
        self.facts
    }
    fn execute_cohort<'a>(
        &'a mut self,
        ordinal: usize,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<'a, Result<(), TerminalError>>
    {
        Box::pin(product_session_execute_cohort(self, ordinal))
    }
    fn commit<'a>(
        &'a mut self,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<novarocks_table_maintenance::product::RewriteCommit, RewriteFailure>,
    > {
        Box::pin(product_session_commit(self))
    }
    fn abort<'a>(
        &'a mut self,
        _reason: String,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<novarocks_table_maintenance::product::RewriteAbort, RewriteFailure>,
    > {
        Box::pin(product_session_abort(self, _reason))
    }
    fn finalize_committed<'a>(
        &'a mut self,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<novarocks_table_maintenance::product::RewriteReceiptFacts, RewriteFailure>,
    > {
        Box::pin(product_session_finalize_committed(self))
    }
}

impl novarocks_table_maintenance::product::TableMaintenanceEffectPort
    for GovernedRewriteEffectPort
{
    fn observe_cleanup_owned_ref<'a>(
        &'a self,
        _: &'a novarocks_table_maintenance::gc_observation::GcOwnedRefObservationAccelerator,
        _: novarocks_table_maintenance::gc_observation::GcOwnedRefObservation,
        _: i64,
        _: i64,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<
            novarocks_table_maintenance::gc_observation::GcOwnedRefObservationDecision,
            novarocks_table_maintenance::runtime::TerminalError,
        >,
    > {
        Box::pin(async {
            Err(
                novarocks_table_maintenance::runtime::TerminalError::pre_dispatch_failed(
                    "this effect source cannot observe owned-ref cleanup",
                ),
            )
        })
    }
    fn reject_user_action_on_mv<'a>(
        &'a self,
        _target: &'a MaintenanceTarget,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<(), novarocks_table_maintenance::runtime::TerminalError>,
    > {
        Box::pin(product_port_reject_user_action_on_mv(self, _target))
    }
    fn execute_metadata<'a>(
        &'a self,
        _request: MaintenanceActionRequest,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<MaintenanceActionOutcome, String>,
    > {
        Box::pin(product_port_execute_metadata(self, _request))
    }
    fn begin_rewrite<'a>(
        &'a self,
        target: &'a MaintenanceTarget,
        intent: novarocks_table_maintenance::product::RewriteIntent,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<
            Box<dyn novarocks_table_maintenance::product::DistributedRewriteSession + 'a>,
            String,
        >,
    > {
        Box::pin(product_port_begin_rewrite(self, target, intent))
    }
    fn begin_rewrite_with_id<'a>(
        &'a self,
        target: &'a MaintenanceTarget,
        intent: novarocks_table_maintenance::product::RewriteIntent,
        effect: novarocks_table_maintenance::MaintenanceEffectId,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<
            Box<dyn novarocks_table_maintenance::product::DistributedRewriteSession + 'a>,
            TerminalError,
        >,
    > {
        Box::pin(product_port_begin_rewrite_with_id(
            self, target, intent, effect,
        ))
    }
    fn begin_cleanup<'a>(
        &'a self,
        _target: &'a MaintenanceTarget,
        _older: i64,
    ) -> novarocks_table_maintenance::product::MaintenanceFuture<
        'a,
        Result<Box<dyn novarocks_table_maintenance::product::CleanupSession + 'a>, TerminalError>,
    > {
        Box::pin(product_port_begin_cleanup(self, _target, _older))
    }
}

async fn product_session_execute_cohort(
    this: &mut GovernedRewriteSession,
    ordinal: usize,
) -> Result<(), TerminalError> {
    execute_original_cohort(&this.original, ordinal).await
}
async fn product_session_commit(
    this: &mut GovernedRewriteSession,
) -> Result<novarocks_table_maintenance::product::RewriteCommit, RewriteFailure> {
    settle_original(&this.original, false).await
}
async fn product_session_abort(
    this: &mut GovernedRewriteSession,
    _reason: String,
) -> Result<novarocks_table_maintenance::product::RewriteAbort, RewriteFailure> {
    use novarocks_table_maintenance::product::{RewriteAbort, RewriteCommit};
    Ok(match settle_original(&this.original, true).await? {
        RewriteCommit::KnownCommitted { effect, .. } => RewriteAbort::KnownCommitted { effect },
        RewriteCommit::KnownUncommitted { .. } => RewriteAbort::KnownUncommitted,
        RewriteCommit::CommitUnknown { failure } => RewriteAbort::CommitUnknown { failure },
    })
}
async fn product_session_finalize_committed(
    this: &mut GovernedRewriteSession,
) -> Result<novarocks_table_maintenance::product::RewriteReceiptFacts, RewriteFailure> {
    use novarocks_spi::connector::RewriteCpuInput;
    let mut original = this.original.lock().await;
    let receipt = match original
        .publication
        .as_ref()
        .and_then(StatementPublication::committed_effect)
    {
        Some(CommittedWriteEffect::Applied(receipt)) => receipt.clone(),
        Some(CommittedWriteEffect::NoOp) => return Ok(Default::default()),
        None => return Err("Rewrite has no original committed receipt".into()),
    };
    if original.seal.is_some() || original.receipt.is_some() {
        return Err("Rewrite finalization already has an original invocation".into());
    }
    original.seal = Some(
        super::rewrite_preparation::RewriteCohortCpuOriginal::finalize_original(
            original.session.as_ref().unwrap().clone(),
            receipt,
        ),
    );
    let cpu = original.cpu.clone();
    let mut input = Some(RewriteCpuInput(original.seal.as_ref().unwrap().clone()));
    cpu.submit_in(&mut input)
        .map_err(|error| receive_connector_failure(&mut original.publication_failure, &error))?;
    std::future::poll_fn(|cx| cpu.poll_finished(cx))
        .await
        .map_err(|error| receive_connector_failure(&mut original.publication_failure, &error))?;
    let RewriteDomain {
        seal,
        receipt,
        publication_failure,
        ..
    } = &mut *original;
    if let Err(error) = seal.as_ref().unwrap().take_receipt_in(receipt) {
        let terminal = crate::table_maintenance::worker::rewrite_compile_terminal(error);
        let cause = terminal.compile_control.map(|control| match control {
            novarocks_type_contract::CompileControlError::Source(cause) => cause,
            control => novarocks_failure_contract::SharedFailureCause::new(control),
        });
        if let Some(cause) = cause {
            let original = publication_failure.get_or_insert(cause);
            return Err(RewriteFailure::from_cause(original.clone()));
        }
        return Err(terminal.message.into());
    }
    drop(original.seal.take());
    let summary = original.receipt.as_ref().unwrap().summary();
    Ok(novarocks_table_maintenance::product::RewriteReceiptFacts {
        target_snapshot_id: summary.target_version,
        input_data_files: summary.input_data_files,
        input_delete_files: summary.input_delete_files,
        output_data_files: summary.output_data_files,
        output_delete_files: summary.output_delete_files,
        output_rows: summary.output_rows,
    })
}

async fn product_port_reject_user_action_on_mv<'a>(
    _this: &'a GovernedRewriteEffectPort,
    _target: &'a MaintenanceTarget,
) -> Result<(), TerminalError> {
    Err(TerminalError::pre_dispatch_failed(
        "mounted Optimize uses its already admitted maintenance activity",
    ))
}
async fn product_port_execute_metadata(
    _this: &GovernedRewriteEffectPort,
    _request: MaintenanceActionRequest,
) -> Result<MaintenanceActionOutcome, String> {
    Err("Rewrite Source does not dispatch metadata maintenance".into())
}
async fn product_port_begin_rewrite<'a>(
    this: &'a GovernedRewriteEffectPort,
    target: &'a MaintenanceTarget,
    intent: novarocks_table_maintenance::product::RewriteIntent,
) -> Result<Box<dyn novarocks_table_maintenance::product::DistributedRewriteSession + 'a>, String> {
    // The facade only holds a weak view. Borrow the same stable Domain so
    // a String-only Product error does not discard its original typed cause.
    let source = upgrade_original(&this.original).map_err(|cause| cause.to_string())?;
    let result = begin_original(
        &this.original,
        target,
        intent,
        novarocks_spi::connector::ConnectorWriteOperationId::new(),
    )
    .await;
    match result {
        Ok(session) => Ok(Box::new(session) as _),
        Err(cause) => {
            let message = cause.to_string();
            let mut original = source.lock().await;
            if original.stop_reason.is_none() {
                original.stop_reason = Some(cause);
            }
            Err(message)
        }
    }
}
async fn product_port_begin_rewrite_with_id<'a>(
    this: &'a GovernedRewriteEffectPort,
    target: &'a MaintenanceTarget,
    intent: novarocks_table_maintenance::product::RewriteIntent,
    effect: novarocks_table_maintenance::MaintenanceEffectId,
) -> Result<
    Box<dyn novarocks_table_maintenance::product::DistributedRewriteSession + 'a>,
    TerminalError,
> {
    begin_original(
        &this.original,
        target,
        intent,
        novarocks_spi::connector::ConnectorWriteOperationId::from_bytes(effect.to_bytes()),
    )
    .await
    .map(|s| Box::new(s) as _)
}
async fn product_port_begin_cleanup<'a>(
    _this: &'a GovernedRewriteEffectPort,
    _target: &'a MaintenanceTarget,
    _older: i64,
) -> Result<Box<dyn novarocks_table_maintenance::product::CleanupSession + 'a>, TerminalError> {
    Err(TerminalError::pre_dispatch_failed(
        "Rewrite Source does not dispatch cleanup maintenance",
    ))
}

async fn close_refused_cohort(
    source: RewriteDomainAlias,
) -> Result<(), StatementNativeHealthFailure> {
    let mut original = source.lock().await;
    let cpu = original.cpu.clone();
    if let Some(provider) = original.provider.clone() {
        original.preparation.close(cpu.as_ref(), &provider).await?;
    } else {
        std::future::poll_fn(|cx| cpu.poll_closed(cx))
            .await
            .map_err(|e| health("Rewrite refused CPU exit is unproven", e))?;
    }
    if let Some(computation) = original.computation.as_mut() {
        std::future::poll_fn(|cx| computation.poll_actual_exit(cx)).await?;
    }
    // No failed local producer or Native observer remains. Retire the exact
    // old stage aliases before the Root's publication/GC continuation.
    if let Some(call) = original.cohort.as_mut() {
        drop(call.engine.take());
    }
    drop(original.computation.take());
    original.preparation = RewritePreparationCustody::default();
    drop(original.provider.take());
    Ok(())
}

async fn close_original(source: RewriteDomainAlias) -> Result<(), StatementNativeHealthFailure> {
    let mut original = source.lock().await;
    let cpu = original.cpu.clone();
    if let Some(provider) = original.provider.clone() {
        original.preparation.close(cpu.as_ref(), &provider).await?;
    } else {
        std::future::poll_fn(|cx| cpu.poll_closed(cx))
            .await
            .map_err(|e| health("Rewrite CPU close is unproven", e))?;
    }
    if let Some(begin) = original.begin_source.as_ref() {
        begin
            .close(cpu.as_ref())
            .await
            .map_err(|e| health("Rewrite original WriteBegin close is unproven", e))?;
    }
    if let Some(plan) = original.plan_source.as_mut() {
        plan.close(cpu.as_ref())
            .await
            .map_err(|e| health("Rewrite Provider plan close is unproven", e))?;
    }
    if let Some(computation) = original.computation.as_mut() {
        std::future::poll_fn(|cx| computation.poll_actual_exit(cx)).await?;
    }
    for tail in original.tails.iter_mut().flatten() {
        std::future::poll_fn(|cx| tail.poll_exit(cx)).await?;
    }
    let session = original
        .original_write
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    let context = original.context.clone();
    if let Some(publication) = original.publication.as_mut() {
        std::future::poll_fn(|cx| publication.poll_settle(session.as_ref(), &context, true, cx))
            .await;
        std::future::poll_fn(|cx| publication.poll_retirement(cx)).await;
        if let Some(failure) = publication.local_health_failure() {
            return Err(failure);
        }
    }
    if let Some(failure) = original.begin.local_health_failure() {
        return Err(failure);
    }
    Ok(())
}

#[cfg(test)]
#[path = "rewrite_job/terminal_tests.rs"]
mod terminal_tests;
