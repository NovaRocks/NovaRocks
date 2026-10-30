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

//! Stable original custody for one MV step's concrete borrowed phases.

use super::super::{CommandInput, MvPhaseFailure, MvWake, StepRequest, original_error, service};
use crate::mv::domain::{
    application::{MvApplicationError, MvApplicationErrorKind},
    refresh::target_binding::MvTargetBinding,
    staged_create::{
        AdmittedMvDataPublication, CurrentProjectionMetadataCpu, CurrentProjectionRegistration,
        PublishedOutput, install_published_current_projection_async_in,
    },
};
use crate::query_execution::{
    internal_result_cpu::InternalResultCpu,
    logical_execution::InitialComputationReservation,
    maintenance::io_admission::MetadataIoAdmission,
    mv_assembly::{
        MvWritePreparationCustody, MvWritePreparationFailure, OriginalMvWritePreparationRequest,
        activation_cpu::{
            MvWriteActivationOriginal, MvWriteActivationOutput, MvWriteActivationRequest,
            MvWritePreparePublicationInput, MvWritePublicationInputKind,
        },
        iceberg_activation::begin_metadata_only_connector_write_session_async_in,
        refresh_artifact::PreparedMvMetadataOnlyWrite,
        refresh_handoff::{
            MvRefreshPreparationRequest, PreparedMvRefreshWork, PreparedMvRefreshWrite,
        },
        refresh_preparation::{
            async_original::{MvRefreshPreparationOriginal, MvRefreshPreparationOutput},
            driver::{MvRefreshTurnCustody, MvTurnFailure},
            io_gate::{MvRefreshPreparationIo, MvRefreshPreparationServices},
            pure_reader::MvRefreshPreparationInput,
        },
    },
    mv_native_write::PreparedMvNativeWriteAssembly,
    write_session::{ConnectorWriteSession, WriteBeginReceiving},
};
use novarocks_mv_application::{
    activity::{MvActivityAdmissionError, MvActivityLease},
    management::EffectDisposition,
    product::{MvRefreshAttemptIdentity, MvTarget as ProductMvTarget},
    publication::{MvRefreshPublicationFinalizationFacts, MvRefreshPublicationIntent},
};
use novarocks_query_application::{
    admitted_query_context::{QueryExecutionContext, QueryResultCapacityBinding},
    api::{StatementExternalEffectDisposition, StatementNativeHealthFailure},
    cancellation::QueryCancellationView,
    session_error::QueryServiceError,
};
use novarocks_spi::connector::write_stack::ConnectorWriteNeededReady;
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorError, ConnectorOriginalResultScope,
    ConnectorRequestContext, ConnectorWriteLease, ExternalMutationEffect, ExternalMutationOutcome,
};
use novarocks_state_store_api::StateStoreExecutionRequests;
use novarocks_workload_control::WorkScope;
use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

pub(super) type PhaseFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StepPreparedKind {
    NoOp,
    MetadataOnly,
    Data,
}
#[derive(Clone, Copy)]
enum Phase {
    Prepare,
    Activate,
    Precommit,
    Project,
    Close,
}

/// The Action owns this header before creating its first borrowed phase.
/// Observation never borrows the unique Body or grants work authority.
pub(super) struct StepSource {
    body: Mutex<Option<Box<Body>>>,
    response: Mutex<Option<Result<(), MvPhaseFailure>>>,
    active: AtomicBool,
    poisoned: AtomicBool,
    health: Mutex<Option<StatementNativeHealthFailure>>,
    exit_unproven: StatementNativeHealthFailure,
    io: Arc<MvRefreshPreparationIo>,
    begin: Arc<WriteBeginReceiving>,
    precommit_registration: CurrentProjectionRegistration,
    project_registration: CurrentProjectionRegistration,
    closed: AtomicBool,
}
struct Body {
    input: Arc<CommandInput>,
    context: ConnectorRequestContext,
    io: Arc<MetadataIoAdmission>,
    requests: Arc<dyn StateStoreExecutionRequests>,
    attempt: MvRefreshAttemptIdentity,
    root: WorkScope,
    capacity: QueryResultCapacityBinding,
    window: novarocks_workload_control::ResultWindowAlias,
    execution: QueryExecutionContext,
    cpu: InternalResultCpu,
    cancellation: QueryCancellationView,
    runtime: tokio::runtime::Handle,
    wake: Arc<MvWake>,
    preparation: MvRefreshTurnCustody,
    preparation_input: Arc<MvRefreshPreparationInput>,
    preparation_started: bool,
    activity: Option<Result<MvActivityLease, MvActivityAdmissionError>>,
    activation_started: bool,
    preparation_result: Option<Result<MvRefreshPreparationOutput, MvApplicationError>>,
    precommit: Option<MvRefreshTurnCustody>,
    precommit_result: Option<Result<MvWriteActivationOutput, MvApplicationError>>,
    kind: Option<StepPreparedKind>,
    target: Option<MvTargetBinding>,
    product_target: Option<ProductMvTarget>,
    planning: Option<ConnectorControlPlanningLease>,
    exact_write: Option<ConnectorWriteLease>,
    data: Option<PreparedMvRefreshWrite>,
    metadata: Option<PreparedMvMetadataOnlyWrite>,
    admitted: Option<Arc<AdmittedMvDataPublication>>,
    intent: Option<MvRefreshPublicationIntent>,
    // Keep the exact original preparation projection until this Source exits.
    _rewrite: Option<Arc<crate::mv::domain::rewrite::context::IcebergMvRewriteContext>>,
    initial: Option<InitialComputationReservation>,
    session: Option<Arc<ConnectorWriteSession>>,
    write: Option<MvWritePreparationCustody>,
    assembly: Option<PreparedMvNativeWriteAssembly>,
    needed: Option<Arc<ConnectorWriteNeededReady>>,
    published: Option<MvRefreshPublicationFinalizationFacts>,
    commit_result: Option<Result<(), MvPhaseFailure>>,
    project: Option<ProjectInput>,
    terminal_recorded: Option<(EffectDisposition, Result<(), QueryServiceError>)>,
}
struct ProjectInput {
    context: ConnectorRequestContext,
    requests: Arc<dyn StateStoreExecutionRequests>,
}
struct BodyLoan<'a> {
    original: &'a StepSource,
    body: Option<Box<Body>>,
}
impl Drop for BodyLoan<'_> {
    fn drop(&mut self) {
        let body = self.body.take().expect("MV original Body loan");
        let mut slot = self.original.body.lock().unwrap_or_else(|p| p.into_inner());
        assert!(slot.is_none(), "one original MV Body receiver");
        *slot = Some(body);
    }
}
#[derive(Debug)]
struct PhaseExitUnproven;
impl std::fmt::Display for PhaseExitUnproven {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original MV phase did not exit normally")
    }
}
impl std::error::Error for PhaseExitUnproven {}

impl StepSource {
    pub(super) fn new(
        input: Arc<CommandInput>,
        request: StepRequest,
        attempt: MvRefreshAttemptIdentity,
        context: ConnectorRequestContext,
        io: Arc<MetadataIoAdmission>,
        requests: Arc<dyn StateStoreExecutionRequests>,
        cancellation: QueryCancellationView,
        wake: Arc<MvWake>,
        original: ConnectorOriginalResultScope,
    ) -> Result<Arc<Self>, QueryServiceError> {
        let StepRequest::Iceberg {
            target,
            statement,
            repartition,
        } = request
        else {
            return Err(service(
                "MV original step requires an admitted Iceberg request",
            ));
        };
        let capacity = io.root_capacity().map_err(original_error)?;
        let root = capacity.root_scope().clone();
        let window = capacity.try_window_alias().map_err(original_error)?;
        let execution = input
            .execution
            .for_statement_retention(capacity.clone())
            .map_err(original_error)?;
        let cpu = input
            .services
            .blocking
            .provider_creator_cpu()
            .map_err(original_error)?;
        let runtime = input.services.blocking.runtime().clone();
        let context = context.with_io_admission(io.clone());
        let services = MvRefreshPreparationServices {
            cpu: cpu.clone(),
            scope: root.clone(),
            window: window.clone(),
            cancellation: cancellation.clone(),
            runtime: runtime.clone(),
            wake: wake.clone(),
            requests: requests.clone(),
        };
        let gate = MvRefreshPreparationIo::new(services, original);
        #[cfg(novarocks_dev_controls)]
        let gate =
            gate.with_source_window_control(input.services.controls.clone(), target.clone())?;
        let gate = Arc::new(gate);
        let preparation = MvRefreshTurnCustody::new(
            gate.clone(),
            cpu.clone(),
            root.clone(),
            window.clone(),
            cancellation.clone(),
            runtime.clone(),
            wake.clone(),
        );
        let request = MvRefreshPreparationRequest {
            statement: statement.sql_refresh_statement(),
            target,
            attempt: attempt.clone(),
        };
        let preparation_input = Arc::new(MvRefreshPreparationInput {
            source: input.ports.clone(),
            current_catalog: input.current_catalog.clone(),
            current_database: input.current_database.clone(),
            statement,
            connector_context: context.clone(),
            repartition_fields: repartition,
            request,
        });
        Ok(Arc::new(Self {
            body: Mutex::new(Some(Box::new(Body {
                input,
                context,
                io,
                requests,
                attempt,
                root,
                capacity,
                window,
                execution,
                cpu,
                cancellation,
                runtime,
                wake,
                preparation,
                preparation_input,
                preparation_started: false,
                activity: None,
                activation_started: false,
                preparation_result: None,
                precommit: None,
                precommit_result: None,
                kind: None,
                target: None,
                product_target: None,
                planning: None,
                exact_write: None,
                data: None,
                metadata: None,
                admitted: None,
                intent: None,
                _rewrite: None,
                initial: None,
                session: None,
                write: None,
                assembly: None,
                needed: None,
                published: None,
                commit_result: None,
                project: None,
                terminal_recorded: None,
            }))),
            response: Mutex::new(None),
            active: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            health: Mutex::new(None),
            exit_unproven: StatementNativeHealthFailure::actual_exit_unproven(
                "original MV step phase exit is unproven",
                Arc::new(PhaseExitUnproven),
            ),
            io: gate,
            begin: WriteBeginReceiving::new(Arc::new(Mutex::new(None))),
            precommit_registration: CurrentProjectionRegistration::new(),
            project_registration: CurrentProjectionRegistration::new(),
            closed: AtomicBool::new(false),
        }))
    }
    fn loan(&self) -> Result<BodyLoan<'_>, QueryServiceError> {
        let body = self
            .body
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
            .ok_or_else(|| service("MV original Body is already borrowed"))?;
        Ok(BodyLoan {
            original: self,
            body: Some(body),
        })
    }
    fn phase(self: &Arc<Self>, phase: Phase) -> PhaseFuture {
        assert!(
            !self.poisoned.load(Ordering::Acquire),
            "MV original phase is poisoned"
        );
        assert!(
            !self.active.swap(true, Ordering::AcqRel),
            "one original MV phase borrower"
        );
        assert!(
            self.response
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_none(),
            "receive the original MV phase result before another phase"
        );
        let source = self.clone();
        let future = Box::pin(async move {
            let result = source.run(phase).await;
            let mut receiver = source.response.lock().unwrap_or_else(|p| p.into_inner());
            assert!(receiver.is_none(), "one MV phase result receiver");
            *receiver = Some(result);
        });
        Box::pin(PhaseCall {
            original: self.clone(),
            future: Some(future),
            ready: false,
        })
    }
    pub(super) fn prepare(self: &Arc<Self>) -> PhaseFuture {
        self.phase(Phase::Prepare)
    }
    pub(super) fn activate(self: &Arc<Self>) -> PhaseFuture {
        self.phase(Phase::Activate)
    }
    pub(super) fn precommit(self: &Arc<Self>) -> PhaseFuture {
        self.phase(Phase::Precommit)
    }
    pub(super) fn close(self: &Arc<Self>) -> PhaseFuture {
        self.phase(Phase::Close)
    }
    pub(super) fn project(
        self: &Arc<Self>,
        needed: Arc<ConnectorWriteNeededReady>,
        context: ConnectorRequestContext,
        requests: Arc<dyn StateStoreExecutionRequests>,
    ) -> PhaseFuture {
        {
            let mut loan = self.loan().expect("inactive original MV project input");
            let body = loan.body.as_mut().unwrap();
            assert!(
                body.project.is_none(),
                "one original MV projection invocation"
            );
            assert!(
                body.needed
                    .as_ref()
                    .is_some_and(|original| Arc::ptr_eq(original, &needed)),
                "MV projection must borrow the same received NeededReady"
            );
            body.project = Some(ProjectInput { context, requests });
        }
        self.phase(Phase::Project)
    }
    pub(super) fn take_phase_result_in(
        &self,
        target: &mut Option<Result<(), MvPhaseFailure>>,
    ) -> Result<(), QueryServiceError> {
        if target.is_some() {
            return Err(service("MV phase result receiver is occupied"));
        }
        let mut slot = self.response.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_none() {
            return Err(service("MV original phase has not returned its result"));
        }
        *target = slot.take();
        Ok(())
    }
    pub(super) fn prepared_kind(&self) -> Result<Option<StepPreparedKind>, QueryServiceError> {
        Ok(self.loan()?.body.as_ref().unwrap().kind)
    }
    pub(super) fn release_business_activity(&self) -> Result<(), QueryServiceError> {
        if self.active.load(Ordering::Acquire) {
            return Err(service("MV business phase Future has not exited"));
        }
        let mut loan = self.loan()?;
        let body = loan.body.as_mut().unwrap();
        let retired = match body.activity.take() {
            Some(Ok(lease)) => Some(lease),
            original => {
                body.activity = original;
                None
            }
        };
        // Restore all original facts before releasing the gate and notifying
        // its next waiter; cleanup keeps this same Source independently.
        drop(loan);
        let mut guard = PollGuard {
            source: self,
            normal: false,
        };
        drop(retired);
        guard.normal = true;
        Ok(())
    }
    pub(super) fn refresh_identity(
        &self,
    ) -> Result<Option<(ProductMvTarget, MvRefreshAttemptIdentity)>, QueryServiceError> {
        let loan = self.loan()?;
        let body = loan.body.as_ref().unwrap();
        Ok(body
            .product_target
            .clone()
            .map(|target| (target, body.attempt.clone())))
    }
    /// Continue the actual begin/preparation request scope and its original
    /// credential/nomination capabilities, never the pre-reservation context.
    pub(super) fn publication_context(&self) -> Result<ConnectorRequestContext, QueryServiceError> {
        Ok(self.loan()?.body.as_ref().unwrap().context.clone())
    }
    pub(super) fn session(&self) -> Result<Option<Arc<ConnectorWriteSession>>, QueryServiceError> {
        // The original begin receiver also covers failures before normal begin return.
        Ok(self.begin.session())
    }
    pub(super) fn take_native_in(
        &self,
        target: &mut Option<(InitialComputationReservation, PreparedMvNativeWriteAssembly)>,
    ) -> Result<(), QueryServiceError> {
        if target.is_some() {
            return Err(service("MV Native receiver is occupied"));
        }
        if self.active.load(Ordering::Acquire) {
            return Err(service("MV activation Future has not exited"));
        }
        let mut loan = self.loan()?;
        let body = loan.body.as_mut().unwrap();
        if body.kind != Some(StepPreparedKind::Data)
            || body.initial.is_none()
            || body.assembly.is_none()
        {
            return Err(service(
                "MV activation has no complete original Native handoff",
            ));
        }
        *target = Some((body.initial.take().unwrap(), body.assembly.take().unwrap()));
        Ok(())
    }
    pub(super) fn mark_dispatched(&self) -> Result<(), QueryServiceError> {
        let mut loan = self.loan()?;
        let body = loan.body.as_mut().unwrap();
        let id = body
            .intent
            .as_ref()
            .ok_or_else(|| service("MV publication has no original intent"))?
            .publication_id();
        let admitted = Arc::get_mut(
            body.admitted
                .as_mut()
                .ok_or_else(|| service("MV publication has no original admission"))?,
        )
        .ok_or_else(|| service("MV publication CPU still borrows its original admission"))?;
        let mut guard = PollGuard {
            source: self,
            normal: false,
        };
        let result = admitted.mark_dispatched(id);
        guard.normal = true;
        result.map_err(service)
    }

    pub(super) fn receive_committed(
        &self,
        needed: &Arc<ConnectorWriteNeededReady>,
    ) -> Result<(), MvPhaseFailure> {
        let mut loan = self.loan()?;
        let body = loan.body.as_mut().unwrap();
        match body.needed.as_ref() {
            Some(original) if !Arc::ptr_eq(original, needed) => {
                return Err(phase_service("MV received a different publication receipt"));
            }
            Some(_) => {
                if let Some(result) = &body.commit_result {
                    return result.clone();
                }
            }
            None => body.needed = Some(needed.clone()),
        }
        let result = (|| {
            require_applied_receipt(needed)?;
            let intent = body
                .intent
                .as_ref()
                .ok_or_else(|| phase_service("MV committed publication lost its original intent"))?
                .clone();
            let mut guard = PollGuard {
                source: self,
                normal: false,
            };
            let interpreted = body
                .input
                .activation
                .interpret_write_commit(intent, needed.receipt());
            guard.normal = true;
            body.published = Some(publication_facts_from_interpreted(interpreted)?);
            Ok(())
        })();
        body.commit_result = Some(result.clone());
        result
    }

    pub(super) fn published_facts(
        &self,
    ) -> Result<Option<MvRefreshPublicationFinalizationFacts>, QueryServiceError> {
        Ok(self.loan()?.body.as_ref().unwrap().published.clone())
    }
    pub(super) fn record_terminal(
        &self,
        disposition: EffectDisposition,
    ) -> Result<(), QueryServiceError> {
        let mut loan = self.loan()?;
        let body = loan.body.as_mut().unwrap();
        if let Some((original, result)) = &body.terminal_recorded {
            return if *original == disposition {
                result.clone()
            } else {
                Err(service("MV admission terminal disposition changed"))
            };
        }
        let admitted = body
            .admitted
            .take()
            .ok_or_else(|| service("MV terminal has no original admission"))?;
        let admitted = match Arc::try_unwrap(admitted) {
            Ok(admitted) => admitted,
            Err(original) => {
                body.admitted = Some(original);
                return Err(service(
                    "MV original CPU still holds the publication admission",
                ));
            }
        };
        // The actual management author consumes its one lease, never a wrapper Future.
        let mut guard = PollGuard {
            source: self,
            normal: false,
        };
        let result = admitted.record_terminal(disposition);
        guard.normal = true;
        let result = result.map_err(service);
        body.terminal_recorded = Some((disposition, result.clone()));
        result
    }
    pub(super) fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        let mut result = None;
        for registration in [
            self.io.registration(),
            &self.precommit_registration,
            &self.project_registration,
        ] {
            let observation = registration.observation();
            let fact = if observation.applied_receipt().is_some() {
                Some(StatementExternalEffectDisposition::KnownCommitted)
            } else {
                match observation.validated_outcome() {
                    Some(Ok(ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::Applied,
                        ..
                    })) => Some(StatementExternalEffectDisposition::KnownCommitted),
                    Some(Ok(ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::NoOp,
                        ..
                    })) => None,
                    Some(Ok(ExternalMutationOutcome::KnownUncommitted { .. })) => {
                        Some(StatementExternalEffectDisposition::KnownUncommitted)
                    }
                    Some(Ok(ExternalMutationOutcome::CommitUnknown { .. })) => {
                        Some(StatementExternalEffectDisposition::CommitUnknown)
                    }
                    _ if observation.dispatched() => {
                        Some(StatementExternalEffectDisposition::CommitUnknown)
                    }
                    _ => None,
                }
            };
            result = super::super::fold_effect(result, fact);
        }
        result
    }
    fn retain_health(&self, health: StatementNativeHealthFailure) {
        let mut slot = self.health.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_none() {
            *slot = Some(health);
        }
    }
    pub(super) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        if self.poisoned.load(Ordering::Acquire) {
            return Some(self.exit_unproven.clone());
        }
        let health = self
            .health
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        health
            .or_else(|| self.begin.local_health_failure())
            .or_else(|| self.io.local_health_failure())
            .or_else(|| self.precommit_registration.local_health_failure())
            .or_else(|| self.project_registration.local_health_failure())
    }
    pub(super) fn poll_closed(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        if let Some(health) = self.local_health_failure() {
            return Poll::Ready(Err(health));
        }
        // Only the explicit close borrower may withdraw named I/O requests.
        // Polling retirement while a business phase is Pending must not close
        // its still-live reader or its requested Gate edge.
        if !self.closed.load(Ordering::Acquire) {
            return Poll::Pending;
        }
        let mut pending = false;
        pending |= self
            .precommit_registration
            .poll_registration_closed(cx)
            .is_pending();
        pending |= self
            .project_registration
            .poll_registration_closed(cx)
            .is_pending();
        match self.io.poll_closed(cx) {
            Poll::Pending => pending = true,
            Poll::Ready(Err(health)) => return Poll::Ready(Err(health)),
            Poll::Ready(Ok(())) => {}
        }
        if pending {
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }
    async fn run(&self, phase: Phase) -> Result<(), MvPhaseFailure> {
        let mut loan = self.loan()?;
        let body = loan.body.as_mut().unwrap();
        match phase {
            Phase::Prepare => body.prepare(self).await,
            Phase::Activate => body.activate(self).await,
            Phase::Precommit => body.precommit(self).await,
            Phase::Project => body.project(self).await,
            Phase::Close => body.close(self).await,
        }
    }
}

impl Body {
    fn turn(&self, gate: &Arc<MvRefreshPreparationIo>) -> MvRefreshTurnCustody {
        MvRefreshTurnCustody::new(
            gate.clone(),
            self.cpu.clone(),
            self.root.clone(),
            self.window.clone(),
            self.cancellation.clone(),
            self.runtime.clone(),
            self.wake.clone(),
        )
    }
    async fn prepare(&mut self, source: &StepSource) -> Result<(), MvPhaseFailure> {
        if self.preparation_started {
            return Err(phase_service("MV original preparation was already started"));
        }
        self.preparation_started = true;
        // Preserve the original activity-before-preparation order, including
        // NoOp. The supplied cancellation Future owns no timer or authority.
        let product = self.input.product.clone();
        let cancellation = self.cancellation.clone();
        let mut waiting = Box::pin(product.acquire_foreground_async(
            crate::mv::activity::canonical_mv_target(&self.preparation_input.request.target),
            self.input.request.owner(),
            async {
                let _ = cancellation.cancelled().await;
            },
        ));
        let raw = waiting.as_mut().await;
        self.activity = Some(raw);
        // Result/lease is in the original Body before the gate F's destructor.
        drop(waiting);
        if let Some(Err(error)) = self.activity.as_ref() {
            return Err(domain_error(MvApplicationError::new(
                MvApplicationErrorKind::ShutdownCancelled,
                match error {
                    MvActivityAdmissionError::Cancelled => {
                        "MV statement was cancelled while waiting for activity gate"
                    }
                    MvActivityAdmissionError::Stopping => {
                        "frontend MV activity admission is closed"
                    }
                },
            )));
        }
        #[cfg(novarocks_dev_controls)]
        if let super::super::MvCommandRequest::ScheduledRefresh { requested, .. } =
            &self.input.request
        {
            // Dependency steps must not consume the requested target's one
            // scheduler claim. The activity lease is already in this Body.
            if requested == &self.preparation_input.request.target
                && let Some(controls) = self.input.services.controls.as_ref()
            {
                match controls
                    .scheduler_async(requested, &self.cancellation)
                    .await
                {
                    Ok(true) => {
                        return Err(domain_error(MvApplicationError::new(
                            MvApplicationErrorKind::ShutdownCancelled,
                            "MV scheduler barrier was cancelled",
                        )));
                    }
                    Err(error) => {
                        return Err(domain_error(MvApplicationError::new(
                            MvApplicationErrorKind::Unavailable,
                            error,
                        )));
                    }
                    Ok(false) => {}
                }
            }
        }
        // The outer Step installed this Source before this first child exists.
        let original = MvRefreshPreparationOriginal::for_refresh(
            self.preparation_input.clone(),
            source.io.clone(),
        );
        self.preparation
            .install_preparation(original)
            .map_err(|_| phase_service("MV original preparation slot is occupied"))?;
        poll_fn(|cx| self.preparation.poll_ready(cx))
            .await
            .map_err(|e| turn_error(source, e))?;
        if !self
            .preparation
            .preparation_original()
            .take_result_in(&mut self.preparation_result)
        {
            return Err(phase_service(
                "MV original preparation returned no receivable result",
            ));
        }
        self.preparation.retire_transferred().map_err(service)?;
        // The complete original output is stable here before any domain getter.
        let output = self
            .preparation_result
            .as_mut()
            .unwrap()
            .as_mut()
            .map_err(|e| domain_error(e.clone()))?;
        let MvRefreshPreparationOutput::Refresh(prepared) = output else {
            return Err(phase_service(
                "MV refresh reader returned another operation's output",
            ));
        };
        if prepared.prepared.attempt != self.attempt {
            return Err(phase_service(
                "MV preparation replaced its reserved attempt",
            ));
        }
        if matches!(prepared.prepared.work, PreparedMvRefreshWork::NoOp) {
            self.kind = Some(StepPreparedKind::NoOp);
            return Ok(());
        }
        match &prepared.prepared.work {
            PreparedMvRefreshWork::DataProducing { write, .. } => {
                if write.operation_id() != self.attempt.write_operation_id()
                    || write.publication_intent().publication_id() != self.attempt.publication_id
                {
                    return Err(domain_error(MvApplicationError::new(
                        MvApplicationErrorKind::InvalidRequest,
                        "SQL-prepared MV write differs from its Lake publication identity",
                    )));
                }
            }
            PreparedMvRefreshWork::MetadataOnly { write, .. } => {
                if write.publication_intent().publication_id() != self.attempt.publication_id {
                    return Err(domain_error(MvApplicationError::new(
                        MvApplicationErrorKind::InvalidRequest,
                        "SQL-prepared metadata-only refresh changed its Lake publication identity",
                    )));
                }
            }
            PreparedMvRefreshWork::NoOp => {
                unreachable!("NoOp returned before publication validation")
            }
        }
        self.product_target = Some(
            ProductMvTarget::try_new(
                prepared.prepared.finalize.target.catalog.clone(),
                prepared.prepared.finalize.target.database.clone(),
                prepared.prepared.finalize.target.name.clone(),
            )
            .map_err(service)?,
        );
        let target = prepared
            .target_binding
            .as_ref()
            .ok_or_else(|| phase_service("MV preparation has no exact retained target"))?;
        let planning = target.lease().clone();
        let binding = planning.binding();
        if prepared.prepared.observed_binding
            != (novarocks_spi::connector::ConnectorProviderBindingKey {
                instance_id: binding.descriptor().instance_id.clone(),
                incarnation: binding.incarnation(),
            })
        {
            return Err(domain_error(MvApplicationError::new(
                MvApplicationErrorKind::BindingInvalidated,
                "MV preparation target differs from its observed connector generation",
            )));
        }
        let exact = planning.derive_write_lease().map_err(connector_error)?;
        self.planning = Some(planning);
        self.exact_write = Some(exact);
        self.target = prepared.target_binding.take();
        self._rewrite = prepared.rewrite.take();
        let work = std::mem::replace(&mut prepared.prepared.work, PreparedMvRefreshWork::NoOp);
        match work {
            PreparedMvRefreshWork::MetadataOnly { write, admitted } => {
                self.intent = Some(write.publication_intent().clone());
                self.metadata = Some(write);
                self.admitted = Some(Arc::new(admitted));
                self.kind = Some(StepPreparedKind::MetadataOnly);
            }
            PreparedMvRefreshWork::DataProducing { write, admitted } => {
                self.intent = Some(write.publication_intent().clone());
                self.data = Some(write);
                self.admitted = Some(Arc::new(admitted));
                self.kind = Some(StepPreparedKind::Data);
            }
            PreparedMvRefreshWork::NoOp => unreachable!("NoOp returned before data admission"),
        }
        Ok(())
    }
    async fn activate(&mut self, source: &StepSource) -> Result<(), MvPhaseFailure> {
        if self.activation_started {
            return Err(phase_service("MV original activation was already started"));
        }
        self.activation_started = true;
        let planning = self
            .planning
            .as_ref()
            .ok_or_else(|| phase_service("MV activation has no retained planning lease"))?;
        let exact = self
            .exact_write
            .as_ref()
            .ok_or_else(|| phase_service("MV activation has no exact write lease"))?;
        let current = self
            .input
            .ports
            .connector_control()
            .acquire_current(&planning.binding().descriptor().instance_id)
            .map_err(connector_error)?;
        if !Arc::ptr_eq(current.binding(), planning.binding()) {
            return Err(domain_error(MvApplicationError::new(
                MvApplicationErrorKind::BindingInvalidated,
                "MV refresh connector generation changed before provider dispatch",
            )));
        }
        match self.kind {
            Some(StepPreparedKind::Data) => {
                #[cfg(novarocks_dev_controls)]
                if let Some(controls) = &self.input.services.controls {
                    let target = self
                        .product_target
                        .as_ref()
                        .ok_or_else(|| phase_service("prepared MV has no target"))?;
                    controls
                        .recovery_async(
                            target.catalog().unwrap(),
                            target.namespace(),
                            target.name(),
                            "source-prepared",
                        )
                        .await
                        .map_err(|error| {
                            domain_error(MvApplicationError::new(
                                MvApplicationErrorKind::Engine,
                                error,
                            ))
                        })?;
                }

                self.initial = Some(InitialComputationReservation::new(self.input.services.launcher.reserve_identity()
                    .map_err(novarocks_query_application::session_outcome::governed_query_execution_error)?).map_err(distributed_error)?);
                self.context = self
                    .initial
                    .as_ref()
                    .unwrap()
                    .connector_request_context(
                        self.context.clone(),
                        #[cfg(novarocks_dev_controls)]
                        self.execution.test_statement_nomination(),
                    )
                    .map_err(distributed_error)?;
                let session = self
                    .data
                    .as_ref()
                    .ok_or_else(|| phase_service("MV data activation lost its original artifact"))?
                    .begin_session_in(
                        self.context.clone(),
                        exact,
                        planning,
                        &self.input.services.connector_control,
                        self.target.as_ref().unwrap(),
                        &source.begin,
                    )
                    .await
                    .map_err(connector_error)?;
                self.session = Some(session);
                let credentials = self.initial.as_ref().unwrap().credentials();
                credentials
                    .bind_write_production_session(self.session.as_ref().unwrap())
                    .map_err(service)?;
                self.session
                    .as_ref()
                    .unwrap()
                    .retain_computation_credentials(&credentials)
                    .map_err(connector_error)?;
                let turn = self.turn(&source.io);
                self.write = Some(self.input.activation.prepare_write_source(
                    OriginalMvWritePreparationRequest {
                        prepared: self.data.take().unwrap(),
                        target: self.target.take().unwrap(),
                        planning: planning.clone(),
                        execution: self.execution.clone(),
                        context: self.context.clone(),
                        session: self.session.as_ref().unwrap().clone(),
                        io: source.io.clone(),
                        turn,
                        owners: self.input.services.completion.clone(),
                        capacity: self.capacity.clone(),
                        requests: self.requests.clone(),
                    },
                ));
                self.write
                    .as_mut()
                    .unwrap()
                    .prepare()
                    .await
                    .map_err(|e| write_error(source, e))?;
                self.write
                    .as_mut()
                    .unwrap()
                    .take_assembly_in(&mut self.assembly)
                    .map_err(service)?;
            }
            Some(StepPreparedKind::MetadataOnly) => {
                self.session = Some(
                    begin_metadata_only_connector_write_session_async_in(
                        self.metadata.as_ref().unwrap(),
                        self.context.clone(),
                        exact,
                        planning,
                        &self.input.services.connector_control,
                        &source.begin,
                    )
                    .await
                    .map_err(connector_error)?,
                );
            }
            _ => {
                return Err(phase_service(
                    "MV activation requires a prepared publication",
                ));
            }
        }
        Ok(())
    }
    async fn precommit(&mut self, source: &StepSource) -> Result<(), MvPhaseFailure> {
        if self.precommit.is_some() {
            return Err(phase_service("MV original precommit was already started"));
        }
        #[cfg(novarocks_dev_controls)]
        if self.kind == Some(StepPreparedKind::Data)
            && let Some(controls) = &self.input.services.controls
        {
            let target = self.product_target.as_ref().unwrap();
            controls
                .recovery_async(
                    target.catalog().unwrap(),
                    target.namespace(),
                    target.name(),
                    "data-prepared",
                )
                .await
                .map_err(|error| {
                    domain_error(MvApplicationError::new(
                        MvApplicationErrorKind::Engine,
                        error,
                    ))
                })?;
        }
        // Install its original receiver before any dependency recheck can suspend.
        self.precommit = Some(self.turn(&source.io));
        let factory = CurrentProjectionMetadataCpu::business(
            self.cpu.clone(),
            self.root.clone(),
            self.window.clone(),
            self.cancellation.clone(),
            self.runtime.clone(),
            self.wake.clone(),
        );
        self.admitted
            .as_ref()
            .ok_or_else(|| phase_service("MV precommit lost its original admission"))?
            .recheck_current_dependencies_async(
                self.planning.as_ref().unwrap(),
                &self.context,
                &factory,
                &source.precommit_registration,
            )
            .await
            .map_err(domain_error)?;
        let original = MvWriteActivationOriginal::new(
            MvWriteActivationRequest::PreparePublication(MvWritePreparePublicationInput {
                session: self.session.as_ref().unwrap().clone(),
                intent: self.intent.as_ref().unwrap().clone(),
                admitted: self.admitted.as_ref().unwrap().clone(),
                context: self.context.clone(),
                kind: if self.kind == Some(StepPreparedKind::Data) {
                    MvWritePublicationInputKind::Data
                } else {
                    MvWritePublicationInputKind::MetadataOnly
                },
            }),
        );
        self.precommit
            .as_mut()
            .unwrap()
            .install_activation(original)
            .map_err(|_| phase_service("MV precommit original receiver is occupied"))?;
        let turn = self.precommit.as_mut().unwrap();
        poll_fn(|cx| turn.poll_ready(cx))
            .await
            .map_err(|e| turn_error(source, e))?;
        turn.activation_original()
            .take_output_in(&mut self.precommit_result)
            .map_err(service)?;
        turn.retire_transferred().map_err(service)?;
        match self.precommit_result.as_ref().unwrap() {
            Ok(MvWriteActivationOutput::PublicationPrepared) => Ok(()),
            Ok(_) => Err(phase_service(
                "MV precommit returned another operation's output",
            )),
            Err(error) => Err(domain_error(error.clone())),
        }
    }
    async fn project(&mut self, source: &StepSource) -> Result<(), MvPhaseFailure> {
        let project = self
            .project
            .as_ref()
            .ok_or_else(|| phase_service("MV projection has no original finite context"))?;
        self.io
            .check_existing_metadata_obligation_context(&self.root, &project.context)
            .map_err(original_error)?;
        let published = self
            .published
            .as_ref()
            .ok_or_else(|| phase_service("MV projection has no received committed facts"))?;
        let snapshot_id = publication_snapshot_id(published)?;
        #[cfg(novarocks_dev_controls)]
        if let Some(controls) = &self.input.services.controls {
            let target = self.product_target.as_ref().unwrap();
            // Both barriers still observe the actual interpreted Applied fact,
            // before any Current capture/registration/projection I/O is started.
            for phase in ["write-committed", "publication-committed"] {
                controls
                    .recovery_async(
                        target.catalog().unwrap(),
                        target.namespace(),
                        target.name(),
                        phase,
                    )
                    .await
                    .map_err(|error| {
                        domain_error(MvApplicationError::new(
                            MvApplicationErrorKind::KnownCommittedFinalizeFailed,
                            error,
                        ))
                    })?;
            }
            controls
                .known_committed_async(
                    target.catalog().unwrap(),
                    target.namespace(),
                    target.name(),
                    &self.attempt.publication_id,
                )
                .await
                .map_err(|error| {
                    domain_error(MvApplicationError::new(
                        MvApplicationErrorKind::KnownCommittedFinalizeFailed,
                        error,
                    ))
                })?;
        }
        let catalog = self
            .planning
            .as_ref()
            .unwrap()
            .binding()
            .catalog_handle()
            .map_err(connector_error)?
            .clone();
        let readiness = self
            .input
            .ports
            .readiness()
            .in_requests(project.requests.clone());
        let factory = CurrentProjectionMetadataCpu::existing_obligation(
            self.cpu.clone(),
            self.root.clone(),
            self.window.clone(),
            self.io.clone(),
            project.context.clone(),
            self.runtime.clone(),
            self.wake.clone(),
        );
        let entrance = self.input.ports.management_entrance().map_err(service)?;
        install_published_current_projection_async_in(
            entrance,
            &readiness,
            self.input.ports.connector_control(),
            catalog,
            self.product_target.as_ref().unwrap().clone(),
            *self.attempt.publication_id.as_uuid(),
            PublishedOutput { snapshot_id },
            project.context.clone(),
            &factory,
            &source.project_registration,
        )
        .await
        .map_err(|error| {
            let mut domain = MvApplicationError::new(
                MvApplicationErrorKind::KnownCommittedFinalizeFailed,
                error.to_string(),
            );
            if let Some(cause) = error.original_failure() {
                domain = domain.with_original_failure(cause.clone());
            }
            domain_error(domain)
        })
    }
    async fn close(&mut self, source: &StepSource) -> Result<(), MvPhaseFailure> {
        // The outer Step waits for Native and its publication Call first. These
        // are only this Source's concrete CPU, Provider and registration tails.
        if let Some(write) = self.write.as_mut() {
            write
                .close()
                .await
                .map_err(|health| health_error(source, health))?;
        }
        poll_fn(|cx| self.preparation.poll_closed(cx))
            .await
            .map_err(|health| health_error(source, health))?;
        if let Some(turn) = self.precommit.as_mut() {
            poll_fn(|cx| turn.poll_closed(cx))
                .await
                .map_err(|health| health_error(source, health))?;
        }
        // A failed precommit's original input still borrows the admission.
        // Retire that exact input only after the same worker/forwarder closed,
        // so the outer Step can now record its actual terminal disposition.
        let retired_precommit = self.precommit.take();
        drop(retired_precommit);
        poll_fn(|cx| source.precommit_registration.poll_registration_closed(cx)).await;
        poll_fn(|cx| source.project_registration.poll_registration_closed(cx)).await;
        poll_fn(|cx| source.io.poll_closed(cx))
            .await
            .map_err(|health| health_error(source, health))?;
        source.closed.store(true, Ordering::Release);
        Ok(())
    }
}

// No domain Future owns the only Source. A normal Pending clears no original
// receipt; a foreign unwind or an abandoned borrower is sticky health evidence.
struct PhaseCall {
    original: Arc<StepSource>,
    future: Option<PhaseFuture>,
    ready: bool,
}
struct PollGuard<'a> {
    source: &'a StepSource,
    normal: bool,
}
impl Drop for PollGuard<'_> {
    fn drop(&mut self) {
        if !self.normal {
            self.source.poisoned.store(true, Ordering::Release);
        }
    }
}
impl Future for PhaseCall {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.original.poisoned.load(Ordering::Acquire) {
            return Poll::Pending;
        }
        let source = self.original.clone();
        let mut guard = PollGuard {
            source: &source,
            normal: false,
        };
        let poll = self
            .future
            .as_mut()
            .expect("original MV phase Future")
            .as_mut()
            .poll(cx);
        if poll.is_ready() {
            self.ready = true;
        }
        guard.normal = true;
        poll
    }
}
impl Drop for PhaseCall {
    fn drop(&mut self) {
        if !self.ready {
            self.original.poisoned.store(true, Ordering::Release);
        }
        let mut guard = PollGuard {
            source: &self.original,
            normal: false,
        };
        drop(self.future.take());
        guard.normal = true;
        self.original.active.store(false, Ordering::Release);
    }
}

pub(super) fn domain_error(error: MvApplicationError) -> MvPhaseFailure {
    let service =
        crate::query::routed_execution_service_error(crate::query::product_command_error(
            crate::mv::command::mv_application_command_error(error.clone()),
        ));
    MvPhaseFailure {
        service,
        domain: Some(error),
    }
}
fn phase_service(message: impl ToString) -> MvPhaseFailure {
    service(message).into()
}
fn connector_error(error: ConnectorError) -> MvPhaseFailure {
    domain_error(MvApplicationError::from_connector(
        MvApplicationErrorKind::Engine,
        error,
    ))
}
fn turn_error(source: &StepSource, error: MvTurnFailure) -> MvPhaseFailure {
    match error {
        MvTurnFailure::Admission(error) => original_error(error).into(),
        MvTurnFailure::Cpu(error) => {
            QueryServiceError::from_execution_failure((*error).clone()).into()
        }
        MvTurnFailure::Health(health) => health_error(source, health).into(),
    }
}
fn health_error(source: &StepSource, health: StatementNativeHealthFailure) -> QueryServiceError {
    source.retain_health(health);
    service("MV original local exit is unproven")
}
fn write_error(source: &StepSource, error: MvWritePreparationFailure) -> MvPhaseFailure {
    match error {
        MvWritePreparationFailure::Domain(error) => domain_error(error),
        MvWritePreparationFailure::Turn(error) => turn_error(source, error),
        MvWritePreparationFailure::Provider(error) => match error.into_original_cause() {
            Ok(cause) => QueryServiceError::from_failure_cause(cause).into(),
            Err(crate::query_execution::provider_read_facts::ProviderReadError::ActualExit(
                health,
            )) => health_error(source, health).into(),
            Err(crate::query_execution::provider_read_facts::ProviderReadError::Contract(
                message,
            )) => phase_service(message),
            Err(_) => unreachable!("typed Provider source was received above"),
        },
        MvWritePreparationFailure::Statistics(error) => connector_error(error.error),
        MvWritePreparationFailure::Health(health) => health_error(source, health).into(),
    }
}

fn distributed_error(
    error: crate::query_execution::contract::DistributedQueryError,
) -> QueryServiceError {
    if let Some(cause) = std::error::Error::source(&error)
        .and_then(|source| source.downcast_ref::<novarocks_failure_contract::SharedFailureCause>())
    {
        return QueryServiceError::from_failure_cause(cause.clone())
            .with_compile_control(error.compile_control_error());
    }
    crate::query::routed_execution_service_error(crate::query::routed_distributed_query_failure(
        error,
    ))
}

fn require_applied_receipt(needed: &ConnectorWriteNeededReady) -> Result<(), MvPhaseFailure> {
    if needed.effect() != ExternalMutationEffect::Applied {
        return Err(phase_service(
            "MV publication did not apply its declared document set",
        ));
    }
    Ok(())
}

fn publication_facts_from_interpreted(
    interpreted: Result<novarocks_mv_application::publication::MvRefreshCommittedFacts, String>,
) -> Result<MvRefreshPublicationFinalizationFacts, MvPhaseFailure> {
    let committed = interpreted.map_err(|message| {
        domain_error(MvApplicationError::new(
            MvApplicationErrorKind::KnownCommittedFinalizeFailed,
            message,
        ))
    })?;
    MvRefreshPublicationFinalizationFacts::try_new(
        committed.intent().clone(),
        committed.committed_version().clone(),
    )
    .map_err(|message| {
        domain_error(MvApplicationError::new(
            MvApplicationErrorKind::KnownCommittedFinalizeFailed,
            message,
        ))
    })
}

fn publication_snapshot_id(
    published: &MvRefreshPublicationFinalizationFacts,
) -> Result<i64, MvPhaseFailure> {
    published
        .publication_version()
        .snapshot_id()
        .ok_or_else(|| phase_service("MV committed receipt has no exact snapshot"))
}

#[cfg(test)]
mod receipt_migration_tests {
    use super::super::publication_migration_tests::*;
    use super::*;
    use crate::query_execution::write_session::tests as write_session_tests;
    use novarocks_mv_application::publication::MvRefreshCommittedFacts;
    use novarocks_spi::connector::{ConnectorCommittedVersion, ConnectorWriteReceipt};

    #[test]
    fn a_first_refresh_commits_exactly_once_through_its_write_session() {
        let (fixture, publication) = publication_after_actual_call(
            published_outcome(7),
            1,
            7,
            vec![(sole_target(), write_session_tests::commit_fragment_bytes())],
        );
        publication.select(None).unwrap();
        let needed = publication.committed_needed().unwrap();
        assert_eq!(needed.effect(), ExternalMutationEffect::Applied);
        assert_eq!(needed.receipt().resulting_row_count(), Some(7));
        require_applied_receipt(needed).unwrap();
        let facts = publication_facts_from_interpreted(
            MvRefreshCommittedFacts::from_write_receipt(publication_intent(), needed.receipt()),
        )
        .unwrap();
        assert_eq!(facts.publication_version().snapshot_id(), Some(42));
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
    }
    #[test]
    fn an_empty_first_refresh_still_reaches_its_publication_commit() {
        let (fixture, publication) =
            publication_after_actual_call(published_outcome(0), 1, 0, Vec::new());
        let needed = publication.committed_needed().unwrap();
        assert_eq!(needed.effect(), ExternalMutationEffect::Applied);
        require_applied_receipt(needed).unwrap();
        publication_facts_from_interpreted(MvRefreshCommittedFacts::from_write_receipt(
            publication_intent(),
            needed.receipt(),
        ))
        .unwrap();
        assert_eq!(fixture.session.finish_invocations(), 1);
    }
    #[test]
    fn an_incremental_refresh_commits_every_branch_once_through_its_write_session() {
        let (fixture, publication) = publication_after_actual_call(
            published_outcome(7),
            2,
            7,
            vec![
                (ordinal(0), write_session_tests::commit_fragment_bytes()),
                (ordinal(1), write_session_tests::commit_fragment_bytes()),
            ],
        );
        let needed = publication.committed_needed().unwrap();
        assert_eq!(needed.effect(), ExternalMutationEffect::Applied);
        assert_eq!(needed.receipt().resulting_row_count(), Some(7));
        require_applied_receipt(needed).unwrap();
        publication_facts_from_interpreted(MvRefreshCommittedFacts::from_write_receipt(
            publication_intent(),
            needed.receipt(),
        ))
        .unwrap();
        assert_eq!(fixture.session.finish_invocations(), 1);
        assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
    }
    #[test]
    fn an_empty_append_refresh_retains_noop_but_cannot_cross_the_applied_barrier() {
        let (fixture, publication) =
            publication_after_actual_call(skipped_publication_outcome(), 1, 0, Vec::new());
        let needed = publication.committed_needed().unwrap();
        assert_eq!(needed.effect(), ExternalMutationEffect::NoOp);
        assert!(needed.receipt().resulting_row_count().is_none());
        let refusal = require_applied_receipt(needed).unwrap_err();
        assert!(refusal.domain.is_none());
        let error =
            MvRefreshCommittedFacts::from_write_receipt(publication_intent(), needed.receipt())
                .unwrap_err();
        assert!(
            error.contains("without resulting row-count fact"),
            "unexpected interpretation failure: {error}"
        );
        assert_eq!(
            publication.disposition(),
            Some(novarocks_spi::connector::LakePublicationDisposition::KnownCommitted)
        );
        assert_eq!(
            publication.received_committed_effect(),
            Some(ExternalMutationEffect::NoOp)
        );
        assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
    }
    #[test]
    fn actual_applied_receipt_validation_failure_keeps_committed_fact() {
        // The original two valid generic Provider receipts reach their real
        // MV-specific refusal cuts: interpretation and later projection gate.
        for (snapshot, rows) in [(Some(42), None), (None, Some(0))] {
            let receipt = ConnectorWriteReceipt::try_new_with_committed_facts(
                bytes::Bytes::from_static(b"mv-committed-receipt"),
                ConnectorCommittedVersion::try_new(
                    bytes::Bytes::from_static(b"mv-committed-version"),
                    snapshot,
                )
                .unwrap(),
                rows,
            )
            .unwrap();
            let outcome = ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt,
                finalization: novarocks_spi::connector::ExternalMutationFinalization::Complete,
                cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
            };
            let (fixture, publication) = publication_after_actual_call(outcome, 1, 0, Vec::new());
            let needed = publication.committed_needed().unwrap();
            require_applied_receipt(needed).unwrap();
            let interpreted =
                MvRefreshCommittedFacts::from_write_receipt(publication_intent(), needed.receipt());
            let failure = match publication_facts_from_interpreted(interpreted) {
                Err(failure) => failure,
                Ok(facts) => publication_snapshot_id(&facts)
                    .expect_err("the real projection cut requires the exact snapshot"),
            };
            let terminal = super::super::product_failure(
                super::super::provider_error(failure.service, Some(&publication))
                    .into_product_error(),
            )
            .domain
            .unwrap();
            assert_eq!(
                terminal.kind(),
                MvApplicationErrorKind::KnownCommittedFinalizeFailed
            );
            assert_eq!(
                publication.received_committed_effect(),
                Some(ExternalMutationEffect::Applied)
            );
            assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
        }
    }
    #[test]
    fn actual_noop_does_not_gain_a_committed_mv_fact() {
        let (fixture, publication) =
            publication_after_actual_call(skipped_publication_outcome(), 1, 0, Vec::new());
        let needed = publication.committed_needed().unwrap();
        let failure = require_applied_receipt(needed).unwrap_err();
        assert!(failure.domain.is_none());
        assert_eq!(
            super::super::provider_error(failure.service, Some(&publication)).kind(),
            novarocks_mv_application::ports::MvProviderFailureKind::KnownUncommitted
        );
        assert_eq!(
            publication.received_committed_effect(),
            Some(ExternalMutationEffect::NoOp)
        );
        assert_eq!(fixture.recorded.lock().unwrap().finish, 1);
    }
}
