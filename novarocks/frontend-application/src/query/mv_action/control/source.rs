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

//! Concrete control custody. Futures borrow the Body; receipts outlive every F.
pub(super) mod call;
mod prepare;
use super::{CommandInput, MvControlRequest, MvWake, requests::DropProjectionRequests};
use crate::mv::domain::{
    application::MvAlterAction,
    staged_create::{
        CurrentMetadataCapture, CurrentProjectionMetadataCpu, CurrentProjectionRegistration,
    },
};
use crate::query_execution::{
    internal_result_cpu::InternalResultCpu, maintenance::io_admission::MetadataIoAdmission,
};
use crate::state_store::execution::StateStoreOriginalOwner;
use call::CatalogCall;
use novarocks_failure_contract::SharedFailureCause;
use novarocks_mv_application::{
    activity::{MvActivityAdmissionError, MvActivityLease},
    management::{EffectDisposition, ManagementEntranceLease},
    ports::{MvProviderFailure, MvProviderFailureKind},
    product::MvTarget,
    readiness::{MvKeyedDropReadiness, MvKeyedProjectionDeleteGuard},
};
use novarocks_query_application::{
    api::{StatementExternalEffectDisposition, StatementNativeHealthFailure},
    cancellation::QueryCancellationView,
    session_error::QueryServiceError,
};
use novarocks_spi::connector::{
    ConnectorCatalogMutationLease, ConnectorCatalogMutationOperation,
    ConnectorCatalogMutationReceiving, ConnectorControlPlanningLease,
    ConnectorDocumentStorageLease, ConnectorError, ConnectorMutationFailure,
    ConnectorMutationOperationId, ConnectorRequestContext, ConnectorTableObjectBinding,
    ExternalMutationEffect, ExternalMutationFinalization, ExternalMutationOutcome,
    KeyedDocumentObservationRequest,
};
use novarocks_workload_control::{ResultWindowAlias, WorkScope};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

#[derive(Clone, Copy)]
pub(super) enum Phase {
    Prepare,
    DeleteProjection,
    Unregister,
    Project,
}
type OriginalFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
pub(super) struct ControlSource {
    body: Mutex<Option<Box<Body>>>,
    response: Mutex<Option<Result<(), MvProviderFailure>>>,
    active: AtomicBool,
    physical_closed: AtomicBool,
    poisoned: AtomicBool,
    health: StatementNativeHealthFailure,
    request: MvControlRequest,
    target: MvTarget,
    operation: uuid::Uuid,
    root: WorkScope,
    window: ResultWindowAlias,
    cpu: InternalResultCpu,
    runtime: tokio::runtime::Handle,
    wake: Arc<MvWake>,
    io: Arc<MetadataIoAdmission>,
    requests: Arc<DropProjectionRequests>,
    capture: Arc<CurrentMetadataCapture>,
    current: CurrentProjectionRegistration,
    call: CatalogCall,
    finite: OnceLock<ConnectorRequestContext>,
    business_refusal: OnceLock<novarocks_workload_control::WorkError>,
}
struct Body {
    input: Arc<CommandInput>,
    context: ConnectorRequestContext,
    cancel: QueryCancellationView,
    factory: CurrentProjectionMetadataCpu,
    activity: Option<Result<MvActivityLease, MvActivityAdmissionError>>,
    planning: Option<ConnectorControlPlanningLease>,
    binding: Option<ConnectorTableObjectBinding>,
    documents: Option<ConnectorDocumentStorageLease>,
    management: Option<ManagementEntranceLease>,
    // Keep a raw acquired lease/result before dropping its original wait F.
    management_failure: Option<MvProviderFailure>,
    exact_management_target: Option<novarocks_mv_application::management::ManagedMvTarget>,
    current_admission: Option<novarocks_mv_application::management::MvCurrentManagementAdmission>,
    drop_admission: Option<novarocks_spi::connector::ConnectorDocumentManagementAdmission>,
    management_marked: bool,
    management_terminal: Option<Result<(), MvProviderFailure>>,
    keyed_request: Option<KeyedDocumentObservationRequest>,
    operation: Option<ConnectorCatalogMutationOperation>,
    drop_readiness: Option<MvKeyedDropReadiness>,
    delete_guard: Option<MvKeyedProjectionDeleteGuard>,
    finalization_failure: Option<MvProviderFailure>,
    preparation_failure: Option<MvProviderFailure>,
}
struct BodyLoan<'a> {
    source: &'a ControlSource,
    body: Option<Box<Body>>,
}
impl std::ops::Deref for BodyLoan<'_> {
    type Target = Body;
    fn deref(&self) -> &Body {
        self.body.as_ref().unwrap()
    }
}
impl std::ops::DerefMut for BodyLoan<'_> {
    fn deref_mut(&mut self) -> &mut Body {
        self.body.as_mut().unwrap()
    }
}
impl Drop for BodyLoan<'_> {
    fn drop(&mut self) {
        let body = self.body.take().unwrap();
        let mut slot = self.source.body.lock().unwrap_or_else(|p| p.into_inner());
        assert!(slot.is_none(), "same original control Body receiver");
        *slot = Some(body);
    }
}
struct Foreign<'a> {
    source: &'a ControlSource,
    normal: bool,
}
impl Drop for Foreign<'_> {
    fn drop(&mut self) {
        if !self.normal {
            self.source.poisoned.store(true, Ordering::Release)
        }
    }
}
#[derive(Debug)]
struct ExitUnproven;
impl std::fmt::Display for ExitUnproven {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MV control original did not exit normally")
    }
}
impl std::error::Error for ExitUnproven {}

pub(super) struct PhaseTask {
    source: Arc<ControlSource>,
    future: Option<OriginalFuture>,
    ready: bool,
}
impl PhaseTask {
    pub(super) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.ready {
            return Poll::Ready(());
        }
        if self.source.poisoned.load(Ordering::Acquire) {
            return Poll::Pending;
        }
        let mut foreign = Foreign {
            source: &self.source,
            normal: false,
        };
        match self.future.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => {
                foreign.normal = true;
                Poll::Pending
            }
            Poll::Ready(()) => {
                // run stored response and restored Body before its Future Drop.
                drop(self.future.take());
                self.ready = true;
                self.source.active.store(false, Ordering::Release);
                foreign.normal = true;
                Poll::Ready(())
            }
        }
    }
}
impl Drop for PhaseTask {
    fn drop(&mut self) {
        if !self.ready {
            self.source.poisoned.store(true, Ordering::Release)
        }
    }
}
impl ControlSource {
    pub(super) fn new(
        input: Arc<CommandInput>,
        request: MvControlRequest,
        context: ConnectorRequestContext,
        io: Arc<MetadataIoAdmission>,
        root: WorkScope,
        window: ResultWindowAlias,
        requests: Arc<DropProjectionRequests>,
        cancel: QueryCancellationView,
        wake: Arc<MvWake>,
    ) -> Result<Arc<Self>, QueryServiceError> {
        let cpu = input
            .services
            .blocking
            .provider_creator_cpu()
            .map_err(super::super::original_error)?;
        let runtime = input.services.blocking.runtime().clone();
        let target = crate::mv::activity::canonical_mv_target(request.target());
        let factory = CurrentProjectionMetadataCpu::business(
            cpu.clone(),
            root.clone(),
            window.clone(),
            cancel.clone(),
            runtime.clone(),
            wake.clone(),
        );
        Ok(Arc::new(Self {
            body: Mutex::new(Some(Box::new(Body {
                input,
                context,
                cancel,
                factory,
                activity: None,
                planning: None,
                binding: None,
                documents: None,
                management: None,
                management_failure: None,
                exact_management_target: None,
                current_admission: None,
                drop_admission: None,
                management_marked: false,
                management_terminal: None,
                keyed_request: None,
                operation: None,
                drop_readiness: None,
                delete_guard: None,
                finalization_failure: None,
                preparation_failure: None,
            }))),
            response: Mutex::new(None),
            active: AtomicBool::new(false),
            physical_closed: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            health: StatementNativeHealthFailure::actual_exit_unproven(
                "MV control original exit is unproven",
                Arc::new(ExitUnproven),
            ),
            request,
            target,
            operation: uuid::Uuid::now_v7(),
            root,
            window,
            cpu,
            runtime,
            wake,
            io,
            requests,
            capture: CurrentMetadataCapture::new(),
            current: CurrentProjectionRegistration::new(),
            call: CatalogCall::new(),
            finite: OnceLock::new(),
            business_refusal: OnceLock::new(),
        }))
    }
    fn loan(&self) -> BodyLoan<'_> {
        let body = self
            .body
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
            .expect("one control Body borrower");
        BodyLoan {
            source: self,
            body: Some(body),
        }
    }
    pub(super) fn receive_business_refusal(&self, error: novarocks_workload_control::WorkError) {
        let _ = self.business_refusal.set(error);
    }
    fn check_business_refusal(&self) -> Result<(), MvProviderFailure> {
        match self.business_refusal.get() {
            Some(error) => Err(shared(SharedFailureCause::new(error.clone()))),
            None => self
                .root
                .check()
                .map_err(|error| shared(SharedFailureCause::new(error))),
        }
    }
    async fn cancelled_or_refused(&self, cancellation: QueryCancellationView) {
        let cancelled = cancellation.cancelled();
        tokio::pin!(cancelled);
        std::future::poll_fn(|cx| {
            if self.business_refusal.get().is_some() {
                Poll::Ready(())
            } else {
                cancelled.as_mut().poll(cx).map(|_| ())
            }
        })
        .await
    }
    pub(super) fn operation_id(&self) -> uuid::Uuid {
        self.operation
    }
    pub(super) fn product_target(&self) -> &MvTarget {
        &self.target
    }
    pub(super) fn phase(self: &Arc<Self>, phase: Phase) -> PhaseTask {
        assert!(
            !self.active.swap(true, Ordering::AcqRel),
            "one control original phase"
        );
        assert!(
            self.response
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_none(),
            "receive prior control response"
        );
        let source = self.clone();
        let future = Box::pin(async move {
            let mut operation = Box::pin(source.run(phase));
            let result = operation.as_mut().await;
            *source.response.lock().unwrap_or_else(|p| p.into_inner()) = Some(result);
            drop(operation);
        });
        PhaseTask {
            source: self.clone(),
            future: Some(future),
            ready: false,
        }
    }
    pub(super) fn take_response(&self) -> Result<(), MvProviderFailure> {
        self.response
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
            .expect("original phase response")
    }
    async fn run(&self, phase: Phase) -> Result<(), MvProviderFailure> {
        let mut body = self.loan();
        match phase {
            Phase::Prepare => self.prepare(&mut body).await,
            Phase::DeleteProjection => {
                let guard = body.delete_guard.take().expect("same non-Clone DROP guard");
                let input = body.input.clone();
                let view = input.ports.readiness().in_requests(self.requests.clone());
                let mut deletion = Box::pin(view.delete_after_provider_drop(self.operation, guard));
                let raw = deletion.as_mut().await;
                // Stable result before child Drop, including exact provider/domain cause.
                let result = raw
                    .map_err(projection_failure)
                    .and_then(crate::mv::domain::iceberg_refresh::validate_drop_installation);
                body.finalization_failure = body
                    .finalization_failure
                    .take()
                    .or_else(|| result.as_ref().err().cloned());
                drop(deletion);
                result
            }
            Phase::Unregister => {
                crate::catalog_application::query_catalog::drop_local_table_registration_if_exists(
                    &body.input.ports,
                    self.target.namespace(),
                    self.target.name(),
                )
                .map_err(invalid)
            }
            Phase::Project => {
                let context = self
                    .finite
                    .get()
                    .expect("same Applied finite context")
                    .clone();
                let factory = CurrentProjectionMetadataCpu::existing_obligation(
                    self.cpu.clone(),
                    self.root.clone(),
                    self.window.clone(),
                    self.io.clone(),
                    context.clone(),
                    self.runtime.clone(),
                    self.wake.clone(),
                );
                let input = body.input.clone();
                let view = input.ports.readiness().in_requests(self.requests.clone());
                let entrance = body
                    .input
                    .ports
                    .management_entrance()
                    .map_err(invalid)?
                    .clone();
                let catalog = body
                    .planning
                    .as_ref()
                    .unwrap()
                    .binding()
                    .catalog_handle()
                    .map_err(connector)?
                    .clone();
                let result = if matches!(&self.request, MvControlRequest::Alter { statement, .. }
                    if matches!(statement.action, MvAlterAction::SetProperties(_))) {
                    crate::mv::domain::staged_create::install_admitted_current_projection_async_in(
                        &entrance,&view,body.input.ports.connector_control(),catalog,self.target.clone(),self.operation,
                        context,&factory,&self.current).await
                } else {
                    crate::mv::domain::staged_create::install_configured_current_projection_async_in(
                        &entrance,&view,body.input.ports.connector_control(),catalog,self.target.clone(),self.operation,
                        context,&factory,&self.current).await
                }.map_err(projection_failure);
                result.map_err(necessary)
            }
        }
    }
    pub(super) fn take_drop_readiness(&self) -> Result<MvKeyedDropReadiness, MvProviderFailure> {
        self.loan()
            .drop_readiness
            .take()
            .ok_or_else(|| invalid("MV DROP preparation returned no original guard"))
    }
    pub(super) fn receive_delete_guard(
        &self,
        source: &mut Option<MvKeyedProjectionDeleteGuard>,
    ) -> Result<(), MvProviderFailure> {
        let mut body = self.loan();
        if body.delete_guard.is_some() || source.is_none() {
            return Err(invalid(
                "MV DROP original guard receiver is occupied or absent",
            ));
        }
        body.delete_guard = source.take();
        Ok(())
    }
    fn start_publication(&self) -> Result<(), MvProviderFailure> {
        if self.call.started() {
            return Ok(());
        }
        self.check_business_refusal()?;
        let mut foreign = Foreign {
            source: self,
            normal: false,
        };
        let result = (|| {
            let mut body = self.loan();
            self.mark_dispatched(&mut body)?;
            let lease = body
                .planning
                .as_ref()
                .unwrap()
                .derive_mutation_lease()
                .map_err(connector)?;
            let operation = body
                .operation
                .take()
                .ok_or_else(|| invalid("MV control mutation was not prepared"))?;
            self.call.start(
                lease,
                ConnectorMutationOperationId::from_bytes(*self.operation.as_bytes()),
                operation,
                body.context.clone(),
            );
            Ok(())
        })();
        foreign.normal = true;
        result
    }
    pub(super) fn poll_progress(&self, cx: &mut Context<'_>) {
        let _ = self.capture.poll_progress(cx);
        let _ = self.current.poll_progress(cx);
        self.call.poll_progress(cx);
    }
    pub(super) fn applied(&self) -> bool {
        effect(&self.call.receiving) == Some(StatementExternalEffectDisposition::KnownCommitted)
    }
    pub(super) fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        super::super::fold_effect(
            effect(&self.call.receiving),
            effect_of_current(&self.current),
        )
    }
    pub(super) fn poll_publication(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), MvProviderFailure>> {
        if let Err(error) = self.start_publication() {
            return Poll::Ready(Err(error));
        }
        self.poll_progress(cx);
        let (needed, validation, raw, dispatched) = {
            let o = self.call.receiving.observation();
            (
                o.needed_committed().cloned(),
                o.needed_validation().cloned(),
                o.validated_outcome()
                    .map(|r| r.cloned().map_err(Clone::clone)),
                o.dispatched(),
            )
        };
        if let Some(Err(error)) = validation {
            return Poll::Ready(Err(connector(error)));
        }
        if let Some(needed) = needed {
            if let ExternalMutationFinalization::Failed(error) = needed.finalization() {
                let mut body = self.loan();
                if body.finalization_failure.is_none() {
                    body.finalization_failure =
                        Some(finalization_failure(needed.effect(), error.clone()))
                }
            }
            // DROP still removes its stale local projection after actual Applied,
            // even when Provider necessary finalization reports failure.
            return Poll::Ready(Ok(()));
        }
        match raw {
            Some(Ok(ExternalMutationOutcome::KnownCommitted {
                effect,
                finalization,
                ..
            })) => {
                if let ExternalMutationFinalization::Failed(error) = finalization {
                    self.loan().finalization_failure = Some(finalization_failure(effect, error));
                }
                Poll::Ready(Ok(()))
            }
            Some(Ok(ExternalMutationOutcome::KnownUncommitted { failure, .. })) => {
                Poll::Ready(Err(rekind(
                    mutation(failure),
                    MvProviderFailureKind::KnownUncommitted,
                )))
            }
            Some(Ok(ExternalMutationOutcome::CommitUnknown { failure, .. })) => Poll::Ready(Err(
                rekind(mutation(failure), MvProviderFailureKind::CommitUnknown),
            )),
            Some(Err(error)) => Poll::Ready(Err(rekind(
                connector(error),
                if dispatched {
                    MvProviderFailureKind::CommitUnknown
                } else {
                    MvProviderFailureKind::KnownUncommitted
                },
            ))),
            None => {
                let error = self
                    .call
                    .state
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .result
                    .as_ref()
                    .and_then(|r| r.as_ref().err())
                    .cloned();
                error.map_or(Poll::Pending, |error| {
                    Poll::Ready(Err(rekind(
                        connector(error),
                        if dispatched {
                            MvProviderFailureKind::CommitUnknown
                        } else {
                            MvProviderFailureKind::KnownUncommitted
                        },
                    )))
                })
            }
        }
    }
    pub(super) fn select_existing_requests(
        &self,
        store: &StateStoreOriginalOwner,
        duration: std::time::Duration,
    ) -> Result<(), MvProviderFailure> {
        if self.requests.selected() {
            return Ok(());
        }
        let context = self
            .requests
            .select_applied(&self.call.receiving, &self.io, &self.root, store, duration)
            .map_err(|error| {
                let mut failure = invalid(error.to_string());
                if let Some(cause) = error.failure_cause() {
                    failure = failure.with_original_failure(cause.clone())
                }
                necessary(failure)
            })?;
        assert!(
            self.finite.set(context).is_ok(),
            "one original finite Context"
        );
        // Current requires management's committed frontier before observation.
        if matches!(self.request, MvControlRequest::Alter { .. }) {
            self.finish_management()?;
        }
        Ok(())
    }
    pub(super) fn finalization_failure(&self) -> Option<MvProviderFailure> {
        self.loan().finalization_failure.clone()
    }
    pub(super) fn release_activity(&self) {
        let mut foreign = Foreign {
            source: self,
            normal: false,
        };
        let activity = self.loan().activity.take();
        drop(activity);
        foreign.normal = true;
    }
    pub(super) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        let busy = self
            .call
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .busy;
        if self.poisoned.load(Ordering::Acquire)
            || busy
            || self.call.receiving.foreign_exit_unproven()
        {
            return Some(self.health.clone());
        }
        self.capture
            .local_health_failure()
            .or_else(|| self.current.local_health_failure())
    }
    pub(super) fn physical_exit_observed(&self) -> bool {
        self.physical_closed.load(Ordering::Acquire)
    }
    pub(super) fn poll_closed(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StatementNativeHealthFailure>> {
        self.poll_progress(cx);
        if let Some(error) = self.local_health_failure() {
            return Poll::Ready(Err(error));
        }
        if self.active.load(Ordering::Acquire) {
            return Poll::Pending;
        }
        if self.capture.poll_closed(cx).is_pending()
            || self
                .current
                .poll_registration_physical_closed(cx)
                .is_pending()
        {
            return Poll::Pending;
        }
        if self.call.started() {
            if !self
                .call
                .state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .exited
            {
                return Poll::Pending;
            }
            let observation = self.call.receiving.observation();
            if observation.dispatched() && !observation.provider_exited() {
                return Poll::Pending;
            }
        }
        // All physical children have exited. Logical inventory still needs
        // this Source/lease and is observed separately below.
        self.physical_closed.store(true, Ordering::Release);
        if self.current.poll_registration_closed(cx).is_pending() {
            return Poll::Pending;
        }
        let o = self.call.receiving.observation();
        if !self.call.started() || !o.dispatched() {
            return Poll::Ready(Ok(()));
        }
        match o.validated_outcome() {
            Some(Ok(
                ExternalMutationOutcome::KnownCommitted {
                    cleanup: ExternalMutationFinalization::Complete,
                    ..
                }
                | ExternalMutationOutcome::KnownUncommitted {
                    cleanup: ExternalMutationFinalization::Complete,
                    ..
                },
            )) => Poll::Ready(Ok(())),
            _ => Poll::Pending,
        }
    }
}
fn effect(
    receiving: &ConnectorCatalogMutationReceiving,
) -> Option<StatementExternalEffectDisposition> {
    let o = receiving.observation();
    observation_effect(&o)
}
fn effect_of_current(
    current: &CurrentProjectionRegistration,
) -> Option<StatementExternalEffectDisposition> {
    let o = current.observation();
    observation_effect(&o)
}
fn observation_effect(
    o: &novarocks_spi::connector::ConnectorCatalogMutationObservation<'_>,
) -> Option<StatementExternalEffectDisposition> {
    use StatementExternalEffectDisposition as E;
    if o.applied_receipt().is_some() {
        return Some(E::KnownCommitted);
    }
    if let Some(needed) = o.needed_committed() {
        return match needed.effect() {
            ExternalMutationEffect::Applied => Some(E::KnownCommitted),
            ExternalMutationEffect::NoOp => None,
        };
    }
    match o.validated_outcome() {
        Some(Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::NoOp,
            ..
        })) => None,
        Some(Ok(ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            ..
        })) => Some(E::KnownCommitted),
        Some(Ok(ExternalMutationOutcome::KnownUncommitted { .. })) => Some(E::KnownUncommitted),
        Some(Ok(ExternalMutationOutcome::CommitUnknown { .. })) => Some(E::CommitUnknown),
        _ if o.dispatched() => Some(E::CommitUnknown),
        _ => None,
    }
}
fn invalid(message: impl ToString) -> MvProviderFailure {
    MvProviderFailure::new(MvProviderFailureKind::InvalidRequest, message.to_string())
}
fn shared(cause: SharedFailureCause) -> MvProviderFailure {
    MvProviderFailure::new(MvProviderFailureKind::Unavailable, cause.detail())
        .with_original_failure(cause)
}
fn connector(error: ConnectorError) -> MvProviderFailure {
    let original = crate::mv::domain::application::MvApplicationError::from_connector(
        crate::mv::domain::application::MvApplicationErrorKind::Engine,
        error,
    );
    MvProviderFailure::new(MvProviderFailureKind::Unavailable, original.to_string())
        .with_original_failure(original.original_failure().unwrap().clone())
}
fn mutation(error: ConnectorMutationFailure) -> MvProviderFailure {
    match error.connector_source() {
        Some(source) => connector(source.clone()),
        None => shared(SharedFailureCause::new(error)),
    }
}
fn projection_failure(
    error: novarocks_mv_application::readiness::MvProjectionError,
) -> MvProviderFailure {
    use novarocks_mv_application::readiness::MvProjectionErrorKind as K;
    let kind = match error.kind() {
        K::SourceConflict | K::Unsupported => MvProviderFailureKind::InvalidRequest,
        K::CorruptDocument => MvProviderFailureKind::Corruption,
        _ => MvProviderFailureKind::Unavailable,
    };
    let mut result = MvProviderFailure::new(kind, error.to_string());
    if let Some(cause) = error.original_failure() {
        result = result.with_original_failure(cause.clone())
    }
    result
}
fn finalization_failure(
    effect: ExternalMutationEffect,
    error: ConnectorMutationFailure,
) -> MvProviderFailure {
    let error = mutation(error);
    if effect == ExternalMutationEffect::Applied {
        necessary(error)
    } else {
        error
    }
}
fn necessary(error: MvProviderFailure) -> MvProviderFailure {
    rekind(error, MvProviderFailureKind::KnownCommittedFinalizeFailed)
}
fn rekind(error: MvProviderFailure, kind: MvProviderFailureKind) -> MvProviderFailure {
    let mut result = MvProviderFailure::new(kind, error.to_string())
        .with_compile_control(error.compile_control_error());
    if let Some(cause) = error.original_failure() {
        result = result.with_original_failure(cause.clone())
    }
    result
}
