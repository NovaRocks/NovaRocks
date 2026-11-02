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

//! Original Provider custody borrowed by the existing CREATE product transition.

use super::{CurrentProjectionMetadataCpu, CurrentProjectionRegistration};
use crate::mv::domain::{
    application::{MvApplicationError, MvApplicationErrorKind, MvCreateProjectionSeed},
    iceberg_refresh::{IcebergMvCorePorts, IcebergMvCreatePlan},
};
use crate::query_execution::{
    maintenance::io_admission::MetadataIoAdmission,
    mv_assembly::{
        activation_cpu::{
            MvWriteActivationOriginal, MvWriteActivationOutput, MvWriteActivationRequest,
        },
        create_publication::{CreatePublicationInput, CreatePublicationOutput, EmptyCreateWrite},
        refresh_preparation::driver::{MvRefreshTurnCustody, MvTurnFailure},
    },
};
use novarocks_failure_contract::SharedFailureCause;
use novarocks_mv_application::{
    management::{
        CreateIntent, EffectDisposition, EffectIdentity, EffectScope, ManagedMvTarget,
        ManagementAdmissionError, ManagementEntrance, ManagementEntranceLease, ManagementRequest,
    },
    persistence::create_documents::MvCreateDocuments,
    product::{MvCreatePublicationOutcome, MvCreatedTarget, MvStagedTarget, MvTarget},
};
use novarocks_query_application::{
    api::{StatementExternalEffectDisposition, StatementNativeHealthFailure},
    cancellation::QueryCancellationView,
    session_error::{QueryServiceError, QueryServiceErrorKind},
};
use novarocks_spi::connector::document_storage::{
    ConnectorDocumentManagementAdmission, ConnectorDocumentManagementAdmissionRequest,
    ConnectorDocumentManagementOperation, ConnectorDocumentStorageLease, KeyedCreateIntent,
};
use novarocks_spi::connector::{
    CatalogHandle, ConnectorControlPlanningLease, ConnectorError, ConnectorMutationFailure,
    ConnectorMutationOperationId, ConnectorRequestContext, ConnectorStagedCreateAbortOutcome,
    ConnectorStagedCreateAbortRequest, ConnectorStagedCreateLease,
    ConnectorStagedCreateNeededCommitted, ConnectorStagedCreatePrepareOutcome,
    ConnectorStagedCreatePrepareRequest, ConnectorStagedCreatePublication,
    ConnectorStagedCreatePublicationPayload, ConnectorStagedCreatePublicationReceiving,
    ConnectorStagedCreatePublishOutcome, ConnectorStagedCreatePublishRequest,
    ConnectorStagedTableHandle, ConnectorTableIdentity, CreatePolicy, ExternalMutationEffect,
    ExternalMutationFinalization, LakePublicationId,
};
use novarocks_state_store_api::StateStoreExecutionRequests;
use std::{
    future::{Future, poll_fn},
    mem::ManuallyDrop,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

pub(crate) struct CreateOriginalInput {
    pub(crate) plan: Arc<IcebergMvCreatePlan>,
    pub(crate) seed: Arc<MvCreateProjectionSeed>,
    pub(crate) ports: IcebergMvCorePorts,
    pub(crate) planning: ConnectorControlPlanningLease,
    pub(crate) operation_id: uuid::Uuid,
    pub(crate) context: ConnectorRequestContext,
    pub(crate) cancellation: QueryCancellationView,
    pub(crate) io: Arc<MetadataIoAdmission>,
    pub(crate) cpu: MvRefreshTurnCustody,
}
#[derive(Clone, Debug)]
pub(crate) struct CreatePhaseFailure {
    pub(crate) service: QueryServiceError,
    pub(crate) domain: Option<MvApplicationError>,
}
impl From<MvApplicationError> for CreatePhaseFailure {
    fn from(domain: MvApplicationError) -> Self {
        let service = crate::query::product_command_service_error(
            crate::mv::command::mv_application_command_error(domain.clone()),
        );
        Self {
            service,
            domain: Some(domain),
        }
    }
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Prepare,
    Stage,
    Publish,
    Abort,
    Project,
    Register,
}
type OriginalFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
#[derive(Default)]
struct OriginalCall {
    future: Option<ManuallyDrop<OriginalFuture>>,
    phase: Option<Phase>,
    borrowed: bool,
    drop_started: bool,
    poisoned: bool,
}
/// This header is installed in the Action before its first phase borrower.
/// Its Future owns only an Arc of State, which has no back-reference to this
/// header or Future. Unknown custody therefore creates no strong cycle.
pub(crate) struct CreateOriginal {
    state: Arc<State>,
    call: Mutex<OriginalCall>,
    health: StatementNativeHealthFailure,
    poisoned: AtomicBool,
    physical_exit: AtomicBool,
    physical_failure: Mutex<Option<StatementNativeHealthFailure>>,
    close_refusal: Mutex<Option<ConnectorError>>,
}
struct State {
    body: Mutex<Option<Box<Body>>>,
    response: Mutex<Option<Result<(), CreatePhaseFailure>>>,
    publication: ConnectorStagedCreatePublicationReceiving,
    stage_raw: Mutex<Option<Result<ConnectorStagedCreatePrepareOutcome, ConnectorError>>>,
    abort_raw: Mutex<Option<Result<ConnectorStagedCreateAbortOutcome, ConnectorError>>>,
    current: CurrentProjectionRegistration,
}
struct Body {
    input: CreateOriginalInput,
    entrance: Arc<ManagementEntrance>,
    catalog: CatalogHandle,
    table: ConnectorTableIdentity,
    target: MvTarget,
    operation: ConnectorMutationOperationId,
    documents_lease: ConnectorDocumentStorageLease,
    staged_lease: ConnectorStagedCreateLease,
    intent: CreateIntent,
    management: Option<Result<ManagementEntranceLease, ManagementAdmissionError>>,
    admission: Option<ConnectorDocumentManagementAdmission>,
    request: Option<ConnectorStagedCreatePrepareRequest>,
    stage_result: Option<Result<ConnectorStagedCreatePrepareOutcome, ConnectorError>>,
    handle: Option<ConnectorStagedTableHandle>,
    cpu_result: Option<Result<MvWriteActivationOutput, MvApplicationError>>,
    empty: EmptyCreateWrite,
    keyed: Option<KeyedCreateIntent>,
    documents: Option<MvCreateDocuments>,
    publication: Option<ConnectorStagedCreatePublication>,
    publication_result: Option<Result<ConnectorStagedCreatePublishOutcome, ConnectorError>>,
    abort_result: Option<Result<ConnectorStagedCreateAbortOutcome, ConnectorError>>,
    terminal: Option<Result<(), CreatePhaseFailure>>,
    project: Option<ProjectInput>,
    projected: bool,
}
struct ProjectInput {
    requests: Arc<dyn StateStoreExecutionRequests>,
    metadata_cpu: Arc<CurrentProjectionMetadataCpu>,
}
struct BodyLoan<'a> {
    state: &'a State,
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
        let mut slot = self.state.body.lock().unwrap_or_else(|p| p.into_inner());
        assert!(slot.is_none(), "same original CREATE Body receiver");
        *slot = Some(body);
    }
}
impl State {
    fn try_loan(&self) -> Option<BodyLoan<'_>> {
        let body = self.body.lock().unwrap_or_else(|p| p.into_inner()).take()?;
        Some(BodyLoan {
            state: self,
            body: Some(body),
        })
    }
    fn loan(&self) -> BodyLoan<'_> {
        self.try_loan().expect("one original CREATE Body borrower")
    }
    async fn run(&self, phase: Phase) -> Result<(), CreatePhaseFailure> {
        let mut body = self.loan();
        match phase {
            Phase::Prepare => body.prepare().await,
            Phase::Stage => body.stage(self).await,
            Phase::Publish => body.publish(self).await,
            Phase::Abort => body.abort(self).await,
            Phase::Project => body.project(self).await,
            Phase::Register => body.register(),
        }
    }
}
/// No prepared handle, lease, session or business result travels in this Ready
/// value. Dropping a borrower leaves the same original Future in the header.
pub(crate) struct CreatePhaseFuture {
    source: Arc<CreateOriginal>,
}
impl Future for CreatePhaseFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.source.poll_progress(cx)
    }
}
struct CallLoan<'a> {
    source: &'a CreateOriginal,
    future: Option<ManuallyDrop<OriginalFuture>>,
    normal: bool,
}
impl Drop for CallLoan<'_> {
    fn drop(&mut self) {
        let mut call = self.source.call.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            call.future.is_none() && call.borrowed,
            "same original CREATE F receiver"
        );
        call.future = self.future.take();
        call.borrowed = false;
        if !self.normal {
            call.poisoned = true;
            self.source.poisoned.store(true, Ordering::Release)
        }
    }
}
struct CloseGuard<'a> {
    source: &'a CreateOriginal,
    normal: bool,
}
impl Drop for CloseGuard<'_> {
    fn drop(&mut self) {
        if !self.normal {
            self.source.poisoned.store(true, Ordering::Release)
        }
    }
}
#[derive(Debug)]
struct OriginalExitUnproven;
impl std::fmt::Display for OriginalExitUnproven {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original CREATE phase exit is unproven")
    }
}
impl std::error::Error for OriginalExitUnproven {}

impl CreateOriginal {
    pub(crate) fn new(mut input: CreateOriginalInput) -> Result<Arc<Self>, MvApplicationError> {
        let descriptor = input.planning.binding().descriptor();
        if descriptor.instance_id.as_str() != input.plan.target.catalog
            || descriptor.provider_id.as_str() != "iceberg"
        {
            return Err(invalid(
                "CREATE plan does not name the selected Iceberg generation",
            ));
        }
        let catalog = input
            .planning
            .binding()
            .catalog_handle()
            .map_err(provider)?
            .clone();
        let entrance = input.ports.management_entrance().map_err(invalid)?.clone();
        let table = ConnectorTableIdentity {
            instance_id: descriptor.instance_id.clone(),
            namespace: Arc::from(input.plan.target.namespace.as_str()),
            table: Arc::from(input.plan.target.table.as_str()),
        };
        let target = MvTarget::try_new(
            Some(input.plan.target.catalog.clone()),
            input.plan.target.namespace.clone(),
            input.plan.target.table.clone(),
        )
        .map_err(|error| invalid(error.to_string()))?;
        let operation = ConnectorMutationOperationId::from_bytes(*input.operation_id.as_bytes());
        let intent = CreateIntent::try_new(
            catalog.clone(),
            table.clone(),
            EffectIdentity::from_bytes(*input.operation_id.as_bytes()),
        )
        .map_err(|e| invalid(format!("freeze CREATE intent: {e:?}")))?;
        let documents_lease = input
            .planning
            .derive_document_storage_lease()
            .map_err(provider)?;
        let staged_lease = input
            .planning
            .derive_staged_create_lease()
            .map_err(provider)?;
        input.context = input.context.with_io_admission(input.io.clone());
        // All rejection above precedes installing any management or Provider work.
        Ok(Arc::new(Self {
            state: Arc::new(State {
                body: Mutex::new(Some(Box::new(Body {
                    input,
                    entrance,
                    catalog,
                    table,
                    target,
                    operation,
                    documents_lease,
                    staged_lease,
                    intent,
                    management: None,
                    admission: None,
                    request: None,
                    stage_result: None,
                    handle: None,
                    cpu_result: None,
                    empty: EmptyCreateWrite::new(),
                    keyed: None,
                    documents: None,
                    publication: None,
                    publication_result: None,
                    abort_result: None,
                    terminal: None,
                    project: None,
                    projected: false,
                }))),
                response: Mutex::new(None),
                publication: ConnectorStagedCreatePublicationReceiving::default(),
                stage_raw: Mutex::new(None),
                abort_raw: Mutex::new(None),
                current: CurrentProjectionRegistration::new(),
            }),
            call: Mutex::new(OriginalCall::default()),
            poisoned: AtomicBool::new(false),
            physical_exit: AtomicBool::new(false),
            physical_failure: Mutex::new(None),
            close_refusal: Mutex::new(None),
            health: StatementNativeHealthFailure::actual_exit_unproven(
                "CREATE phase actual exit is unproven",
                Arc::new(OriginalExitUnproven),
            ),
        }))
    }
    fn phase(self: &Arc<Self>, phase: Phase) -> CreatePhaseFuture {
        assert!(
            !self.poisoned.load(Ordering::Acquire),
            "poisoned CREATE Source cannot start another phase"
        );
        assert!(
            self.state
                .response
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_none(),
            "receive original CREATE response first"
        );
        let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            call.future.is_none() && !call.borrowed && call.phase.is_none(),
            "one CREATE phase borrower"
        );
        let state = self.state.clone();
        call.future = Some(ManuallyDrop::new(Box::pin(async move {
            let result = state.run(phase).await;
            *state.response.lock().unwrap_or_else(|p| p.into_inner()) = Some(result);
        })));
        call.phase = Some(phase);
        self.physical_exit.store(false, Ordering::Release);
        call.drop_started = false;
        CreatePhaseFuture {
            source: self.clone(),
        }
    }
    pub(crate) fn prepare(self: &Arc<Self>) -> CreatePhaseFuture {
        self.phase(Phase::Prepare)
    }
    pub(crate) fn stage(self: &Arc<Self>) -> CreatePhaseFuture {
        self.phase(Phase::Stage)
    }
    pub(crate) fn publish(self: &Arc<Self>) -> CreatePhaseFuture {
        self.phase(Phase::Publish)
    }
    pub(crate) fn abort(self: &Arc<Self>) -> CreatePhaseFuture {
        self.phase(Phase::Abort)
    }
    pub(crate) fn register(self: &Arc<Self>) -> CreatePhaseFuture {
        self.phase(Phase::Register)
    }
    pub(crate) fn project(
        self: &Arc<Self>,
        requests: Arc<dyn StateStoreExecutionRequests>,
        metadata_cpu: Arc<CurrentProjectionMetadataCpu>,
    ) -> CreatePhaseFuture {
        {
            let mut body = self.state.loan();
            assert!(body.project.is_none(), "one CREATE Current invocation");
            body.project = Some(ProjectInput {
                requests,
                metadata_cpu,
            });
        }
        self.phase(Phase::Project)
    }
    pub(crate) fn poll_progress(&self, cx: &mut Context<'_>) -> Poll<()> {
        let future = {
            let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
            if call.poisoned || call.borrowed || call.drop_started {
                return Poll::Pending;
            }
            let Some(future) = call.future.take() else {
                return Poll::Ready(());
            };
            call.borrowed = true;
            future
        };
        let mut loan = CallLoan {
            source: self,
            future: Some(future),
            normal: false,
        };
        match loan.future.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Pending => {
                loan.normal = true;
                Poll::Pending
            }
            Poll::Ready(()) => {
                // State.run restored the Body and received its result before
                // this potentially foreign destructor. Never drop it twice.
                self.call
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .drop_started = true;
                unsafe { ManuallyDrop::drop(loan.future.as_mut().unwrap()) };
                loan.future.take();
                {
                    let mut call = self.call.lock().unwrap_or_else(|p| p.into_inner());
                    call.drop_started = false;
                    call.phase = None;
                }
                loan.normal = true;
                Poll::Ready(())
            }
        }
    }
    pub(crate) fn take_phase_result_in(
        &self,
        target: &mut Option<Result<(), CreatePhaseFailure>>,
    ) -> Result<(), MvApplicationError> {
        if target.is_some() {
            return Err(invalid("CREATE phase result target is occupied"));
        }
        let call = self.call.lock().unwrap_or_else(|p| p.into_inner());
        if call.phase.is_some() || call.borrowed || call.poisoned {
            return Err(invalid("CREATE phase has not actually exited"));
        }
        drop(call);
        let mut slot = self
            .state
            .response
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if slot.is_none() {
            return Err(invalid("CREATE phase has no original response"));
        }
        *target = slot.take();
        Ok(())
    }
    pub(crate) fn staged_target(&self) -> Result<MvStagedTarget, MvApplicationError> {
        let body = self
            .state
            .try_loan()
            .ok_or_else(|| invalid("CREATE Body is still borrowed"))?;
        if body.handle.is_none() {
            return Err(invalid("CREATE has no exact staged target"));
        }
        Ok(MvStagedTarget {
            target: body.target.clone(),
            staged_operation_id: body.input.operation_id,
        })
    }
    pub(crate) fn created_target(&self) -> Result<Option<MvCreatedTarget>, MvApplicationError> {
        if self
            .state
            .publication
            .observation()
            .applied_receipt()
            .is_none()
        {
            return Ok(None);
        }
        let body = self
            .state
            .try_loan()
            .ok_or_else(|| invalid("CREATE Body is still borrowed"))?;
        let object = body
            .handle
            .as_ref()
            .and_then(|h| h.document_target())
            .ok_or_else(|| invalid("applied CREATE has no exact prepared target"))?
            .object_id()
            .clone();
        Ok(Some(MvCreatedTarget {
            target: body.target.clone(),
            object_id: object,
        }))
    }
    pub(crate) fn product_publication_outcome(
        &self,
    ) -> Result<Option<MvCreatePublicationOutcome>, MvApplicationError> {
        let observation = self.state.publication.observation();
        if observation.validation() != Some(Ok(())) {
            return Ok(None);
        }
        match observation.raw_outcome() {
            Some(Ok(ConnectorStagedCreatePublishOutcome::Applied { .. })) => self
                .created_target()
                .map(|created| created.map(MvCreatePublicationOutcome::Applied)),
            Some(Ok(ConnectorStagedCreatePublishOutcome::NoOp { .. })) => {
                Ok(Some(MvCreatePublicationOutcome::NoOp))
            }
            _ => Ok(None),
        }
    }
    pub(crate) fn needed_committed(&self) -> Option<ConnectorStagedCreateNeededCommitted> {
        self.state.publication.observation().needed_committed()
    }
    pub(crate) fn publish_outcome(
        &self,
    ) -> Option<Result<ConnectorStagedCreatePublishOutcome, ConnectorError>> {
        self.state.publication.observation().raw_outcome()
    }
    pub(crate) fn existing_finalization_context(
        &self,
    ) -> Option<Result<ConnectorRequestContext, ConnectorError>> {
        self.state.publication.observation().finalization_context()
    }
    pub(crate) fn external_effect_disposition(&self) -> Option<StatementExternalEffectDisposition> {
        if self
            .state
            .publication
            .observation()
            .applied_receipt()
            .is_some()
        {
            return Some(StatementExternalEffectDisposition::KnownCommitted);
        }
        if self
            .state
            .publication
            .observation()
            .confirmed_not_dispatched()
        {
            return Some(StatementExternalEffectDisposition::KnownUncommitted);
        }
        match self.publish_outcome() {
            Some(Ok(ConnectorStagedCreatePublishOutcome::CommitUnknown { .. })) => {
                Some(StatementExternalEffectDisposition::CommitUnknown)
            }
            Some(Ok(
                ConnectorStagedCreatePublishOutcome::Conflict { .. }
                | ConnectorStagedCreatePublishOutcome::KnownUncommitted { .. },
            )) => Some(StatementExternalEffectDisposition::KnownUncommitted),
            _ => None,
        }
    }
    pub(crate) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        if self.poisoned.load(Ordering::Acquire)
            || self.state.publication.observation().foreign_exit_unproven()
        {
            return Some(self.health.clone());
        }
        if let Some(health) = self
            .physical_failure
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .cloned()
        {
            return Some(health);
        }
        if let Some(health) = self.state.current.local_health_failure() {
            return Some(health);
        }
        let body = self.state.body.lock().unwrap_or_else(|p| p.into_inner());
        body.as_ref().and_then(|body| {
            body.empty
                .local_health_failure()
                .or_else(|| body.input.cpu.local_health_failure())
        })
    }
    pub(crate) fn physical_exit_observed(&self) -> bool {
        self.physical_exit.load(Ordering::Acquire)
    }
    pub(crate) fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut foreign = CloseGuard {
            source: self,
            normal: false,
        };
        let result = self.poll_closed_original(cx);
        foreign.normal = true;
        result
    }
    fn poll_closed_original(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.poisoned.load(Ordering::Acquire) {
            return Poll::Pending;
        }
        if self.poll_progress(cx).is_pending() {
            return Poll::Pending;
        }
        let Some(mut body) = self.state.try_loan() else {
            return Poll::Pending;
        };
        if body.empty.poll_closed(cx).is_pending() {
            return Poll::Pending;
        }
        if let Some(publication) = body.publication.as_mut()
            && publication.poll_closed(cx).is_pending()
        {
            return Poll::Pending;
        }
        if self
            .state
            .current
            .poll_registration_physical_closed(cx)
            .is_pending()
        {
            return Poll::Pending;
        }
        match body.input.cpu.poll_closed(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(health)) => {
                let rejected = {
                    let mut slot = self
                        .physical_failure
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    if slot.is_none() {
                        *slot = Some(health);
                        None
                    } else {
                        Some(health)
                    }
                };
                drop(rejected);
                return Poll::Pending;
            }
            Poll::Pending => return Poll::Pending,
        }
        // Release actual CPU/gate/MIO resources even if the original Provider
        // graph remains Unknown. Retained graph/payload is a separate lifetime.
        match body.input.io.poll_actual_quiescent_continuing(cx) {
            Poll::Ready(Ok(_)) => {}
            Poll::Ready(Err(health)) => {
                let rejected = {
                    let mut slot = self
                        .physical_failure
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    if slot.is_none() {
                        *slot = Some(health);
                        None
                    } else {
                        Some(health)
                    }
                };
                drop(rejected);
                return Poll::Pending;
            }
            Poll::Pending => return Poll::Pending,
        }
        self.physical_exit.store(true, Ordering::Release);
        // Current may retain its original catalog mutation inventory after its
        // physical children exit. It still belongs to this same Source.
        if self.state.current.poll_registration_closed(cx).is_pending() {
            return Poll::Pending;
        }
        // The exact same Map has received the aggregate before provider_exited,
        // but its generation can retire when the last lease disappears. Keep
        // THIS Source/lease in the existing Root residual until disposed; never
        // use current-generation presence as an independent owner or add a cycle.
        if let Some(handle) = body.handle.as_ref() {
            match body.staged_lease.cleanup_responsibility(handle) {
                Ok(novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Disposed) => {
                    Poll::Ready(())
                }
                Ok(novarocks_spi::connector::ConnectorMetadataCleanupResponsibility::Retained) => {
                    Poll::Pending
                }
                Err(error) => {
                    let rejected = {
                        let mut slot = self.close_refusal.lock().unwrap_or_else(|p| p.into_inner());
                        if slot.is_none() {
                            *slot = Some(error);
                            None
                        } else {
                            Some(error)
                        }
                    };
                    drop(rejected);
                    Poll::Pending
                }
            }
        } else if matches!(
            body.stage_result.as_ref(),
            Some(Err(_))
                | Some(Ok(
                    ConnectorStagedCreatePrepareOutcome::CommitUnknown { .. }
                        | ConnectorStagedCreatePrepareOutcome::AppliedButUnusable { .. }
                ))
        ) {
            // No exact handle exists yet. The actual stage author retained its
            // StageUnknown/StageAppliedUnusable row; absence grants no disposal.
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

impl Body {
    async fn prepare(&mut self) -> Result<(), CreatePhaseFailure> {
        if self.management.is_some() {
            return Err(invalid("CREATE management was already requested").into());
        }
        let entrance = self.entrance.clone();
        let cancellation = self.input.cancellation.clone();
        let mut wait = Box::pin(entrance.acquire_async(
            ManagementRequest::for_create_intent(
                self.intent.clone(),
                EffectScope::CATALOG_AND_OBJECT_DELETION,
            ),
            async move {
                let _ = cancellation.cancelled().await;
            },
        ));
        poll_fn(|cx| match wait.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.management = Some(result);
                Poll::Ready(())
            }
        })
        .await;
        drop(wait);
        self.management
            .as_ref()
            .unwrap()
            .as_ref()
            .map_err(|error| {
                MvApplicationError::new(
                    if matches!(
                        error,
                        ManagementAdmissionError::Cancelled | ManagementAdmissionError::Stopping
                    ) {
                        MvApplicationErrorKind::ShutdownCancelled
                    } else {
                        MvApplicationErrorKind::Engine
                    },
                    format!("admit CREATE: {error:?}"),
                )
            })?;
        self.admission = Some(
            self.documents_lease
                .admit_management(
                    ConnectorDocumentManagementAdmissionRequest::try_new(
                        self.documents_lease.owner().clone(),
                        self.catalog.clone(),
                        self.operation,
                        self.table.clone(),
                        None,
                        ConnectorDocumentManagementOperation::Create,
                        self.input.context.clone(),
                    )
                    .map_err(provider)?,
                )
                .map_err(provider)?,
        );
        Ok(())
    }
    async fn cpu(&mut self, input: CreatePublicationInput) -> Result<(), CreatePhaseFailure> {
        self.cpu_result = None;
        self.input
            .cpu
            .install_activation(MvWriteActivationOriginal::new(
                MvWriteActivationRequest::Create(input),
            ))
            .map_err(|_| invalid("CREATE CPU original receiving is occupied"))?;
        poll_fn(|cx| self.input.cpu.poll_ready(cx))
            .await
            .map_err(turn_failure)?;
        self.input
            .cpu
            .activation_original()
            .take_output_in(&mut self.cpu_result)
            .map_err(invalid)?;
        self.input.cpu.retire_transferred().map_err(invalid)?;
        match self.cpu_result.as_ref().unwrap() {
            Ok(_) => Ok(()),
            Err(error) => Err(error.clone().into()),
        }
    }
    async fn stage(&mut self, state: &State) -> Result<(), CreatePhaseFailure> {
        if self.request.is_some() || self.stage_result.is_some() || self.handle.is_some() {
            return Err(invalid("CREATE stage was already requested").into());
        }
        self.cpu(CreatePublicationInput::StageDeclarations {
            plan: self.input.plan.clone(),
        })
        .await?;
        if !matches!(
            self.cpu_result.as_ref(),
            Some(Ok(MvWriteActivationOutput::Create(
                CreatePublicationOutput::StageDeclarations { .. }
            )))
        ) {
            return Err(invalid("CREATE declaration turn returned another output").into());
        }
        let Some(Ok(MvWriteActivationOutput::Create(CreatePublicationOutput::StageDeclarations {
            columns,
            partitioning,
            properties,
        }))) = self.cpu_result.take()
        else {
            unreachable!()
        };
        self.request = Some(
            self.staged_lease
                .prepare_document_managed_request(
                    LakePublicationId::try_from_uuid(self.input.operation_id)
                        .map_err(|e| invalid(e.to_string()))?,
                    self.operation,
                    self.table.clone(),
                    columns,
                    partitioning,
                    properties,
                    CreatePolicy::FailIfExists,
                    self.admission
                        .as_ref()
                        .ok_or_else(|| invalid("CREATE management admission is absent"))?
                        .clone(),
                    self.input.context.clone(),
                )
                .map_err(provider)?,
        );
        self.management_mut()?
            .mark_create_intent_dispatched(super::now_management_timestamp().map_err(invalid)?)
            .map_err(|e| invalid(format!("record CREATE intent: {e:?}")))?;
        let result = self
            .staged_lease
            .prepare_async(self.request.as_ref().unwrap().clone(), &state.stage_raw)
            .await;
        self.stage_result = Some(result);
        match self.stage_result.as_ref().unwrap() {
            Ok(ConnectorStagedCreatePrepareOutcome::Prepared {handle,..})=>{self.handle=Some(handle.clone());Ok(())},
            Ok(ConnectorStagedCreatePrepareOutcome::Conflict {failure}|ConnectorStagedCreatePrepareOutcome::KnownUncommitted {failure})=>{
                let error=mutation(MvApplicationErrorKind::Engine,failure);self.record_terminal(EffectDisposition::KnownUncommitted)?;Err(error.into())
            }
            Ok(ConnectorStagedCreatePrepareOutcome::CommitUnknown {failure,..})=>Err(mutation(MvApplicationErrorKind::CommitUnknown,failure).into()),
            Ok(ConnectorStagedCreatePrepareOutcome::AppliedButUnusable {..})=>Err(MvApplicationError::new(
                MvApplicationErrorKind::CommitUnknown,"CREATE stage was applied but is unusable; its original responsibility is retained").into()),
            Err(error)=>Err(MvApplicationError::from_connector(MvApplicationErrorKind::CommitUnknown,error.clone()).into()),
        }
    }
    async fn publish(&mut self, state: &State) -> Result<(), CreatePhaseFailure> {
        if self.publication.is_some() {
            return Err(invalid("CREATE publication was already requested").into());
        }
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| invalid("CREATE has no staged target"))?
            .clone();
        self.cpu(CreatePublicationInput::Publication {
            plan: self.input.plan.clone(),
            seed: self.input.seed.clone(),
            staged: self.staged_lease.clone(),
            handle: handle.clone(),
            admission: self.admission.as_ref().unwrap().clone(),
            owner: self.entrance.owner().clone(),
            incarnation: self.entrance.incarnation().clone(),
            context: self.input.context.clone(),
        })
        .await?;
        if !matches!(
            self.cpu_result.as_ref(),
            Some(Ok(MvWriteActivationOutput::Create(
                CreatePublicationOutput::Publication { .. }
            )))
        ) {
            return Err(invalid("CREATE publication turn returned another output").into());
        }
        let Some(Ok(MvWriteActivationOutput::Create(CreatePublicationOutput::Publication {
            begin,
            intent,
            documents,
        }))) = self.cpu_result.take()
        else {
            unreachable!()
        };
        self.keyed = Some(intent);
        self.documents = Some(documents);
        let host = self.input.ports.typed_connector_control().clone();
        if let Err(error) = self
            .empty
            .seal(
                &host,
                &self.input.planning,
                &self.staged_lease,
                &handle,
                begin,
            )
            .await
        {
            let kind = if self.empty.allows_staged_abort() {
                MvApplicationErrorKind::Engine
            } else {
                MvApplicationErrorKind::CommitUnknown
            };
            return Err(MvApplicationError::from_connector(kind, error).into());
        }
        let proof = self
            .empty
            .proof()
            .ok_or_else(|| invalid("CREATE empty write has no original proof"))?
            .clone();
        // The caller-owned receiving installs the strong Provider Source
        // before any original publish Future can be polled.
        self.publication = Some(
            self.staged_lease
                .begin_publish_original(
                    ConnectorStagedCreatePublishRequest {
                        operation_id: self.operation,
                        handle,
                        write: proof,
                        payload: ConnectorStagedCreatePublicationPayload::KeyedDocuments(
                            self.keyed.as_ref().unwrap().clone(),
                        ),
                        context: self.input.context.clone(),
                    },
                    &state.publication,
                )
                .map_err(provider)?,
        );
        let result = poll_fn(|cx| self.publication.as_mut().unwrap().poll_result(cx)).await;
        self.publication_result = Some(result);
        // Only these small immutable receipts/failures are cloned. All unique
        // stage/session/input/Provider custody remains in its original fields.
        match self.publication_result.as_ref().unwrap().clone() {
            Ok(ConnectorStagedCreatePublishOutcome::Applied { finalization, .. }) => {
                self.record_applied_terminal().map_err(committed_failure)?;
                match finalization {
                    ExternalMutationFinalization::Complete => Ok(()),
                    ExternalMutationFinalization::Failed(failure) => Err(mutation(
                        MvApplicationErrorKind::KnownCommittedFinalizeFailed,
                        &failure,
                    )
                    .into()),
                }
            }
            // The product's explicit NoOp verdict decides Ack/FailIfExists.
            // It cannot borrow a created target, Current, or a delete right.
            Ok(ConnectorStagedCreatePublishOutcome::NoOp { finalization, .. }) => {
                self.record_terminal(EffectDisposition::KnownUncommitted)?;
                match finalization {
                    ExternalMutationFinalization::Complete => Ok(()),
                    ExternalMutationFinalization::Failed(failure) => {
                        Err(mutation(MvApplicationErrorKind::Engine, &failure).into())
                    }
                }
            }
            Ok(
                ConnectorStagedCreatePublishOutcome::Conflict { failure, .. }
                | ConnectorStagedCreatePublishOutcome::KnownUncommitted { failure, .. },
            ) => Err(mutation(MvApplicationErrorKind::Engine, &failure).into()),
            Ok(ConnectorStagedCreatePublishOutcome::CommitUnknown { failure, .. }) => {
                Err(mutation(MvApplicationErrorKind::CommitUnknown, &failure).into())
            }
            Err(error) => {
                let observation = state.publication.observation();
                let kind = if observation.applied_receipt().is_some() {
                    MvApplicationErrorKind::KnownCommittedFinalizeFailed
                } else if observation.confirmed_not_dispatched() {
                    MvApplicationErrorKind::Engine
                } else {
                    MvApplicationErrorKind::CommitUnknown
                };
                Err(MvApplicationError::from_connector(kind, error).into())
            }
        }
    }
    fn management_mut(&mut self) -> Result<&mut ManagementEntranceLease, MvApplicationError> {
        self.management
            .as_mut()
            .and_then(|r| r.as_mut().ok())
            .ok_or_else(|| invalid("CREATE has no original management lease"))
    }
    fn record_terminal(
        &mut self,
        disposition: EffectDisposition,
    ) -> Result<(), CreatePhaseFailure> {
        if let Some(result) = &self.terminal {
            return result.clone();
        }
        let result = self
            .management_mut()?
            .record_terminal_in_place(disposition)
            .map_err(|e| {
                CreatePhaseFailure::from(invalid(format!("record CREATE terminal: {e:?}")))
            });
        self.terminal = Some(result.clone());
        result
    }
    fn record_applied_terminal(&mut self) -> Result<(), CreatePhaseFailure> {
        if let Some(result) = &self.terminal {
            return result.clone();
        }
        let object = self
            .handle
            .as_ref()
            .and_then(|h| h.document_target())
            .ok_or_else(|| invalid("applied CREATE has no exact object"))?
            .object_id()
            .clone();
        let target = ManagedMvTarget::try_new(self.catalog.clone(), self.table.clone(), object)
            .map_err(|e| invalid(format!("bind CREATE actual target: {e:?}")))?;
        self.management_mut()?
            .late_bind_create_target(target)
            .map_err(|e| invalid(format!("late bind CREATE responsibility: {e:?}")))?;
        self.record_terminal(EffectDisposition::KnownCommitted)
    }
    async fn abort(&mut self, state: &State) -> Result<(), CreatePhaseFailure> {
        if state.publication.observation().applied_receipt().is_some() {
            return Err(invalid("an applied CREATE cannot abort its original stage").into());
        }
        if self.abort_result.is_some() {
            return Err(invalid("CREATE abort was already requested").into());
        }
        match self.publication_result.as_ref() {
            Some(Ok(
                ConnectorStagedCreatePublishOutcome::Conflict { .. }
                | ConnectorStagedCreatePublishOutcome::KnownUncommitted { .. },
            )) => {}
            Some(Err(_)) if state.publication.observation().confirmed_not_dispatched() => {}
            None if self.publication.is_none() && self.empty.allows_staged_abort() => {}
            _ => {
                return Err(
                    invalid("CREATE stage has no original KnownUncommitted abort proof").into(),
                );
            }
        }
        let handle = self
            .handle
            .as_ref()
            .ok_or_else(|| invalid("CREATE abort has no stage"))?
            .clone();
        self.abort_result = Some(
            self.staged_lease
                .abort_async(
                    ConnectorStagedCreateAbortRequest {
                        operation_id: self.operation,
                        handle,
                        write: self.empty.proof().cloned(),
                        context: self.input.context.clone(),
                    },
                    &state.abort_raw,
                )
                .await,
        );
        match self.abort_result.as_ref().unwrap().clone() {
            Ok(ConnectorStagedCreateAbortOutcome::Aborted { finalization, .. }) => {
                self.record_terminal(EffectDisposition::KnownUncommitted)?;
                match finalization {
                    ExternalMutationFinalization::Complete => Ok(()),
                    ExternalMutationFinalization::Failed(failure) => {
                        Err(mutation(MvApplicationErrorKind::Engine, &failure).into())
                    }
                }
            }
            Ok(ConnectorStagedCreateAbortOutcome::KnownUncommitted { failure }) => {
                Err(mutation(MvApplicationErrorKind::Engine, &failure).into())
            }
            Ok(ConnectorStagedCreateAbortOutcome::CommitUnknown { failure, .. }) => {
                Err(mutation(MvApplicationErrorKind::CommitUnknown, &failure).into())
            }
            Err(error) => Err(provider(error.clone()).into()),
        }
    }
    async fn project(&mut self, state: &State) -> Result<(), CreatePhaseFailure> {
        let needed = state
            .publication
            .observation()
            .needed_committed()
            .ok_or_else(|| invalid("CREATE Current requires actual Needed"))?;
        if needed.effect != ExternalMutationEffect::Applied
            || state.publication.observation().applied_receipt().is_none()
        {
            return Err(invalid("CREATE NoOp or Unknown cannot install Current").into());
        }
        // Management terminal precedes Current. A failed Current may not
        // leave a committed create masquerading as unsettled/uncommitted.
        self.record_applied_terminal().map_err(committed_failure)?;
        let context = state
            .publication
            .observation()
            .finalization_context()
            .ok_or_else(|| invalid("CREATE has no original finite finalization Context"))?
            .map_err(provider)?;
        let project = self
            .project
            .as_ref()
            .ok_or_else(|| invalid("CREATE Current has no existing-obligation borrower"))?;
        let view = self
            .input
            .ports
            .readiness()
            .in_requests(project.requests.clone());
        let result = super::install_created_current_projection_async_in(
            &self.entrance,
            &view,
            self.input.ports.connector_control(),
            self.catalog.clone(),
            self.target.clone(),
            self.input.operation_id,
            context,
            &project.metadata_cpu,
            &state.current,
        )
        .await;
        match result {
            Ok(()) => {
                self.projected = true;
                Ok(())
            }
            Err(error) => {
                let mut failure = MvApplicationError::new(
                    MvApplicationErrorKind::KnownCommittedFinalizeFailed,
                    error.to_string(),
                );
                if let Some(cause) = error.original_failure() {
                    failure = failure.with_original_failure(cause.clone())
                }
                Err(failure.into())
            }
        }
    }
    fn register(&self) -> Result<(), CreatePhaseFailure> {
        if !self.projected {
            return Err(
                invalid("CREATE registration precedes its actual Current installation").into(),
            );
        }
        // The existing registration author only confirms that the catalog's
        // current generation is published; it does not mutate a table cache.
        let instance =
            novarocks_spi::connector::ConnectorInstanceId::parse(&self.input.plan.target.catalog)
                .map_err(|e| invalid(e.to_string()))?;
        novarocks_spi::connector::ConnectorControlResolver::acquire_current(
            self.input.ports.connector_control(),
            &instance,
        )
        .map(|_| ())
        .map_err(|e| provider(e).into())
    }
}
fn invalid(message: impl Into<String>) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::InvalidRequest, message)
}
fn provider(error: ConnectorError) -> MvApplicationError {
    MvApplicationError::from_connector(MvApplicationErrorKind::Engine, error)
}
fn mutation(
    kind: MvApplicationErrorKind,
    failure: &ConnectorMutationFailure,
) -> MvApplicationError {
    if let Some(error) = failure.connector_source() {
        MvApplicationError::from_connector(kind, error.clone())
    } else {
        let cause = SharedFailureCause::new(failure.clone());
        MvApplicationError::new(kind, cause.detail()).with_original_failure(cause)
    }
}
fn turn_failure(error: MvTurnFailure) -> CreatePhaseFailure {
    match error {
        MvTurnFailure::Admission(error) => {
            let cause = SharedFailureCause::new(error);
            CreatePhaseFailure {
                service: QueryServiceError::from_failure_cause(cause),
                domain: None,
            }
        }
        MvTurnFailure::Cpu(error) => CreatePhaseFailure {
            service: QueryServiceError::from_execution_failure((*error).clone()),
            domain: None,
        },
        MvTurnFailure::Health(_) => CreatePhaseFailure {
            service: QueryServiceError::new(
                QueryServiceErrorKind::Internal,
                "CREATE CPU original exit is unproven",
            ),
            domain: None,
        },
    }
}

fn committed_failure(error: CreatePhaseFailure) -> CreatePhaseFailure {
    let Some(domain) = error.domain else {
        return error;
    };
    let mut known = MvApplicationError::new(
        MvApplicationErrorKind::KnownCommittedFinalizeFailed,
        domain.message(),
    )
    .with_compile_control(domain.compile_control_error())
    .with_unavailable_implementation(domain.unavailable_implementation().cloned());
    if let Some(cause) = domain.original_failure() {
        known = known.with_original_failure(cause.clone())
    }
    known.into()
}

#[cfg(test)]
mod original_call_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    // Exercise the actual header/loan/Drop author only. This fixture has no
    // Body and grants no Root, Provider, CPU, management or publication rights.
    struct Probe {
        state: Arc<State>,
        ready: Arc<AtomicBool>,
        dropped: Arc<AtomicUsize>,
        panic_on_drop: bool,
    }
    impl Future for Probe {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            if !self.ready.load(Ordering::Acquire) {
                return Poll::Pending;
            }
            *self.state.response.lock().unwrap() =
                Some(Err(invalid("original probe result").into()));
            Poll::Ready(())
        }
    }
    #[derive(Debug)]
    struct ActualDropPanic;
    impl Drop for Probe {
        fn drop(&mut self) {
            assert!(
                self.state.response.lock().unwrap().is_some(),
                "actual result precedes original Drop"
            );
            self.dropped.fetch_add(1, Ordering::SeqCst);
            if self.panic_on_drop {
                std::panic::panic_any(ActualDropPanic)
            }
        }
    }
    fn fixture(panic_on_drop: bool) -> (Arc<CreateOriginal>, Arc<AtomicBool>, Arc<AtomicUsize>) {
        let state = Arc::new(State {
            body: Mutex::new(None),
            response: Mutex::new(None),
            publication: ConnectorStagedCreatePublicationReceiving::default(),
            stage_raw: Mutex::new(None),
            abort_raw: Mutex::new(None),
            current: CurrentProjectionRegistration::new(),
        });
        let ready = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicUsize::new(0));
        let future: OriginalFuture = Box::pin(Probe {
            state: state.clone(),
            ready: ready.clone(),
            dropped: dropped.clone(),
            panic_on_drop,
        });
        (
            Arc::new(CreateOriginal {
                state,
                call: Mutex::new(OriginalCall {
                    future: Some(ManuallyDrop::new(future)),
                    phase: Some(Phase::Prepare),
                    ..Default::default()
                }),
                health: StatementNativeHealthFailure::actual_exit_unproven(
                    "probe original exit unproven",
                    Arc::new(OriginalExitUnproven),
                ),
                poisoned: AtomicBool::new(false),
                physical_exit: AtomicBool::new(false),
                physical_failure: Mutex::new(None),
                close_refusal: Mutex::new(None),
            }),
            ready,
            dropped,
        )
    }
    #[test]
    fn dropping_phase_borrower_keeps_original_input_until_normal_actual_drop() {
        let (source, ready, dropped) = fixture(false);
        let mut borrower = CreatePhaseFuture {
            source: source.clone(),
        };
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(Pin::new(&mut borrower).poll(&mut cx).is_pending());
        drop(borrower);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        assert!(source.state.response.lock().unwrap().is_none());
        ready.store(true, Ordering::Release);
        assert!(source.poll_progress(&mut cx).is_ready());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        let mut received = None;
        source.take_phase_result_in(&mut received).unwrap();
        assert!(received.unwrap().is_err());
        assert!(!source.poisoned.load(Ordering::Acquire));
        assert!(source.poll_progress(&mut cx).is_ready());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn original_drop_panic_keeps_received_cause_and_prevents_repoll_or_second_drop() {
        let (source, ready, dropped) = fixture(true);
        ready.store(true, Ordering::Release);
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            source.poll_progress(&mut cx)
        }))
        .unwrap_err();
        assert!(panic.is::<ActualDropPanic>());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert!(
            source
                .state
                .response
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .is_err()
        );
        assert!(source.local_health_failure().is_some());
        assert!(source.poll_closed(&mut cx).is_pending());
        assert!(source.poll_progress(&mut cx).is_pending());
        let mut received = None;
        assert!(source.take_phase_result_in(&mut received).is_err());
        assert!(received.is_none());
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }
}

#[cfg(test)]
mod original_stage_tests {
    use super::*;
    use crate::mv::domain::test_admission::{AdmittingManagement, catalog_properties};
    use crate::query_execution::{
        internal_consumer::domain_cpu::root_action_tests::{bind, root_control},
        internal_result_cpu::InternalResultCpuOwner,
        mv_assembly::refresh_preparation::io_gate::{
            MvRefreshPreparationIo, MvRefreshPreparationServices,
        },
    };
    use crate::task_execution::status_intake::StatusIntakeWake;
    use bytes::Bytes;
    use novarocks_mv_application::management::{
        DeploymentOwner, MvManagementPhase, ProcessIncarnation,
    };
    use novarocks_spi::connector::*;
    use novarocks_state_store_api::{
        StateStoreExecutionContext, StateStoreExecutionFailure, StateStoreReadExecution,
    };
    use novarocks_workload_control::{
        WorkClass, WorkRequest, WorkloadControl,
        joint_admission::{JointProgress, JointProgressDriver},
    };
    use std::{
        collections::BTreeMap,
        sync::atomic::AtomicUsize,
        time::{Duration, Instant},
    };

    struct NoRepositoryIo;
    impl StateStoreReadExecution for NoRepositoryIo {
        fn read_transaction_context(
            &self,
        ) -> Result<StateStoreExecutionContext, StateStoreExecutionFailure> {
            panic!("CREATE staging does not perform a repository read");
        }
    }
    impl StateStoreExecutionRequests for NoRepositoryIo {
        fn write_operation_context(
            &self,
        ) -> Result<StateStoreExecutionContext, StateStoreExecutionFailure> {
            panic!("CREATE staging does not perform a repository write");
        }
    }
    #[derive(Default)]
    struct Wake(futures::task::AtomicWaker);
    impl std::fmt::Debug for Wake {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("CreateStageWake")
        }
    }
    impl StatusIntakeWake for Wake {
        fn wake(&self) {
            self.0.wake();
        }
    }
    async fn with_driver<F: Future>(
        control: &WorkloadControl,
        driver: &mut JointProgressDriver,
        future: F,
    ) -> F::Output {
        tokio::pin!(future);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match driver.step() {
                    JointProgress::More => tokio::task::yield_now().await,
                    JointProgress::Idle(revision) => tokio::select! {
                        biased;
                        result = &mut future => return result,
                        _ = control.wait_progress(revision) => {},
                    },
                }
            }
        })
        .await
        .expect("original CREATE stage/component watchdog")
    }

    struct RefusingStage {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        entrance: Arc<ManagementEntrance>,
        mode: u8,
        prepares: Arc<AtomicUsize>,
    }
    impl ConnectorStagedCreate for RefusingStage {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> ProviderBindingEpoch {
            self.incarnation
        }
        fn prepare_async(
            &self,
            request: ConnectorStagedCreatePrepareRequest,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<ConnectorStagedCreatePrepareOutcome, ConnectorError>>
                    + Send
                    + '_,
            >,
        > {
            Box::pin(async move {
                self.prepares.fetch_add(1, Ordering::SeqCst);
                assert_eq!(
                    self.entrance.management_phase(&request.table),
                    MvManagementPhase::AwaitingCreateBinding
                );
                let failure = ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Internal,
                    "injected stage failure",
                );
                match self.mode {
                    0 => Ok(ConnectorStagedCreatePrepareOutcome::AppliedButUnusable {
                        receipt: ConnectorStagedCreateReceipt::try_new(
                            request.owner,
                            request.operation_id,
                            ConnectorStagedCreateReceiptPhase::Prepared,
                            ExternalMutationEffect::Applied,
                            Bytes::new(),
                        )
                        .unwrap(),
                        finalization: ExternalMutationFinalization::Failed(failure),
                    }),
                    1 => Err(ConnectorError::new(
                        ConnectorErrorKind::Unavailable,
                        "injected provider dispatch failure",
                    )),
                    2 => Ok(ConnectorStagedCreatePrepareOutcome::Conflict { failure }),
                    3 => Ok(ConnectorStagedCreatePrepareOutcome::KnownUncommitted { failure }),
                    _ => unreachable!(),
                }
            })
        }
        fn prepare(
            &self,
            _: ConnectorStagedCreatePrepareRequest,
        ) -> Result<ConnectorStagedCreatePrepareOutcome, ConnectorError> {
            panic!("original CREATE must use prepare_async");
        }
        fn plan_write(
            &self,
            _: ConnectorStagedWritePlanningRequest,
        ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorError> {
            panic!("failed stage cannot plan writes");
        }
        fn bind_write(
            &self,
            _: ConnectorStagedTableHandle,
            _: ConnectorStagedWriteProof,
        ) -> Result<(), ConnectorError> {
            panic!("failed stage cannot bind writes");
        }
        fn publish(
            &self,
            _: ConnectorStagedCreatePublishRequest,
        ) -> Result<ConnectorStagedCreatePublishOutcome, ConnectorError> {
            panic!("failed stage cannot publish");
        }
        fn abort(
            &self,
            _: ConnectorStagedCreateAbortRequest,
        ) -> Result<ConnectorStagedCreateAbortOutcome, ConnectorError> {
            panic!("failed stage cannot abort");
        }
        fn adjudicate_publication(
            &self,
            _: ConnectorStagedCreatePublicationAdjudicationRequest,
        ) -> Result<ConnectorStagedCreatePublicationAdjudicationOutcome, ConnectorError> {
            panic!("failed stage is not final publication");
        }
    }

    fn plan(
        context: &ConnectorRequestContext,
    ) -> (Arc<IcebergMvCreatePlan>, Arc<MvCreateProjectionSeed>) {
        use crate::mv::domain::{
            application::{MvCreateRefreshPolicy, MvCreateStatement},
            iceberg_refresh::{
                finish_mv_create_preparation, prepare_mv_create_properties,
                prepare_mv_create_shape, prepare_mv_create_target,
            },
        };
        use novarocks_sql::compiler::{
            CatalogRelationFact, SqlCapturedCatalogPreparation, SqlCompileControl,
        };
        use novarocks_sql::planning::catalog::{
            ConnectorReadTableFacts, materialize_connector_read_table,
        };
        let sql = "SELECT id, count(*) AS n FROM iceberg.db.orders GROUP BY id";
        let query = crate::mv::domain::refresh::definition::parse_mv_select_query(sql).unwrap();
        let statement = MvCreateStatement {
            name_parts: vec!["new_mv".into()],
            if_not_exists: false,
            partition_by: None,
            distribution: None,
            refresh_policy: MvCreateRefreshPolicy::Manual,
            select_sql: sql.into(),
            select_query: query.clone(),
            properties: Vec::new(),
            primary_key: None,
        };
        let control = SqlCompileControl::unbounded();
        let preparation = SqlCapturedCatalogPreparation::try_new(
            query.clone(),
            Some("iceberg"),
            "db",
            control.clone(),
        )
        .unwrap();
        let [need] = preparation.needs() else {
            panic!("one exact original relation");
        };
        let mut bindings = novarocks_sql::binding::SqlTableBindingAllocator::new_unique().unwrap();
        let table = materialize_connector_read_table(ConnectorReadTableFacts {
            catalog: "iceberg".into(),
            namespace: "db".into(),
            table: "orders".into(),
            columns: vec![
                novarocks_types::schema::ColumnDef::from_value_type(
                    "id".into(),
                    novarocks_type_contract::FunctionValueType::new(
                        arrow::datatypes::DataType::Int64,
                        true,
                    ),
                    None,
                )
                .unwrap(),
            ],
            iceberg_row_lineage_metadata_columns: Vec::new(),
            schema: Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, true),
            ])),
            binding: bindings.allocate().unwrap(),
            selector: ConnectorReadSelector::Current,
            planning_facts: ConnectorTablePlanningFacts::empty(),
        })
        .unwrap();
        let fact = CatalogRelationFact::resolved(need, table.into_resolved_table()).unwrap();
        let captured = preparation.capture(&[fact]).unwrap();
        let analysis =
            crate::mv::domain::analysis_adapter::analyze_mv_select_with_captured_catalog(
                Some("iceberg"),
                &captured,
                "db",
                &query,
                novarocks_sql::compiler::builtin_sql_function_catalog(),
                crate::application::test_constant_policy(),
                control.clone(),
            )
            .unwrap();
        let target = prepare_mv_create_target(Some("iceberg"), "db", &statement).unwrap();
        let shape = prepare_mv_create_shape(target, &statement, query, analysis, &control).unwrap();
        let (property, persistence) = prepare_mv_create_properties(&shape, &control).unwrap();
        let object_id =
            ConnectorTableObjectId::try_new(Bytes::from_static(b"orders-object")).unwrap();
        let observation = crate::mv::domain::storage_observation::source_schema_from_spi(
            novarocks_spi::connector::MvSourceSchemaObservation::try_new(
                ConnectorTableIdentity {
                    instance_id: ConnectorInstanceId::parse("iceberg").unwrap(),
                    namespace: Arc::from("db"),
                    table: Arc::from("orders"),
                },
                object_id,
                Bytes::from_static(b"schema-1"),
                true,
                true,
                vec![
                    MvObservedSourceField::try_new(
                        Bytes::from_static(b"id-1"),
                        "id".into(),
                        "long".into(),
                        true,
                    )
                    .unwrap(),
                ],
                context,
            )
            .unwrap(),
            context,
        )
        .unwrap();
        // These are pure captured declaration facts, not Provider authority.
        // The test stops at stage refusal and never manufactures a handle or a
        // document-management admission. Both admissions come from real leases.
        let plan = finish_mv_create_preparation(
            shape, &statement, property, persistence,
            vec![novarocks_mv_application::persistence::aggregate_bindings::MvCreateRelationObservation {
                occurrence_id: 0, provider_object_id: Bytes::from_static(b"orders-object"),
                provider_schema_version: Bytes::from_static(b"schema-1"), fields: Vec::new(),
            }],
            novarocks_mv_application::persistence::codec::keyed::BindingDocument {
                data_versions: vec![novarocks_mv_application::persistence::codec::keyed::DataVersionFormat {
                    provider: "iceberg".into(), name: "fixture/stage-data-version".into(),
                    version: 1, max_value_bytes: 8,
                }],
            },
            BTreeMap::from([("iceberg.db.orders".into(), observation)]),
        ).unwrap();
        let seed = Arc::new(
            plan.projection_seed(&statement, Some("iceberg"), "db")
                .unwrap(),
        );
        (plan, seed)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_non_negative_stage_failure_retains_the_unbound_create_responsibility() {
        for mode in 0..4 {
            let mut cpu = InternalResultCpuOwner::try_new_for_test().unwrap();
            let (control, _, mut driver) = root_control(&mut cpu);
            let root = control
                .try_begin_root(WorkRequest::new(WorkClass::Management))
                .unwrap();
            let scope = root.owner.scope();
            bind(&scope, &mut driver);
            let io = MetadataIoAdmission::for_original_source(&root.owner, Vec::new()).unwrap();
            with_driver(
                &control,
                &mut driver,
                poll_fn(|cx| {
                    if io.poll_progress(&root.owner, cx).unwrap() {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                }),
            )
            .await;
            let capacity = io.root_capacity().unwrap();
            let window = capacity.try_window_alias().unwrap();
            let context = ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(5),
                ConnectorStopOwner::new().view(),
                4096,
                65536,
            )
            .unwrap()
            .with_io_admission(io.clone());
            let (plan, seed) = plan(&context);
            let instance_id = ConnectorInstanceId::parse("iceberg").unwrap();
            let base = novarocks_catalog_application::test_support::test_control_binding_for(
                instance_id.clone(),
                8,
            );
            let descriptor = base.descriptor().clone();
            let incarnation = base.incarnation();
            let entrance = Arc::new(ManagementEntrance::new(
                DeploymentOwner::parse("deployment-test").unwrap(),
                ProcessIncarnation::parse("process-test").unwrap(),
            ));
            let prepares = Arc::new(AtomicUsize::new(0));
            let stage = Arc::new(RefusingStage {
                descriptor: descriptor.clone(),
                incarnation,
                entrance: entrance.clone(),
                mode,
                prepares: prepares.clone(),
            });
            let documents = ConnectorDocumentStorageBinding::try_new(
                descriptor.clone(),
                incarnation,
                None,
                Some(Arc::new(AdmittingManagement {
                    descriptor: descriptor.clone(),
                    incarnation,
                })),
            )
            .unwrap();
            let binding = ConnectorControlBinding::try_new_with_all_capabilities_and_staged_create(
                descriptor,
                incarnation,
                base.metadata().clone(),
                base.planning().clone(),
                base.execution_distribution().clone(),
                None,
                None,
                None,
                Some(stage),
                None,
                None,
            )
            .unwrap()
            .with_catalog_properties(catalog_properties(&instance_id))
            .unwrap()
            .try_with_document_storage(Some(documents))
            .unwrap();
            let planning = ConnectorControlPlanningLease::new(Arc::new(binding), || {});
            let catalog = planning.binding().catalog_handle().unwrap().clone();
            let table = ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from("db"),
                table: Arc::from("new_mv"),
            };
            let operation_id = uuid::Uuid::now_v7();
            let intent = CreateIntent::try_new(
                catalog.clone(),
                table.clone(),
                EffectIdentity::from_bytes(*operation_id.as_bytes()),
            )
            .unwrap();
            let (_, repository) = crate::mv::domain::test_admission::keyed_product().await;
            let readiness = Arc::new(crate::mv::domain::readiness::MvReadinessPort::from_product(
                novarocks_mv_application::readiness::MvKeyedReadinessService::new(
                    repository,
                    Arc::default(),
                ),
                tokio::runtime::Handle::current(),
                entrance.clone(),
            ));
            let host = Arc::new(novarocks_catalog_application::ConnectorControlHost::new());
            let ports = IcebergMvCorePorts::new_with_management_entrance(
                Arc::new(novarocks_sql::compiler::build_builtin_engine_function_catalog().unwrap()),
                Arc::new(crate::catalog_application::query_catalog::new_query_catalog_service()),
                None,
                host.clone(),
                host,
                readiness,
                Arc::new(UnavailableMvStorageObservationPort),
                entrance.clone(),
                crate::application::test_constant_policy(),
            );
            let wake = Arc::new(Wake::default());
            let cancellation = QueryCancellationView::governed(scope.cancellation().unwrap(), None);
            let requests = Arc::new(NoRepositoryIo);
            let gate = Arc::new(MvRefreshPreparationIo::new(
                MvRefreshPreparationServices {
                    cpu: cpu.runtime(),
                    scope: scope.clone(),
                    window: window.clone(),
                    cancellation: cancellation.clone(),
                    runtime: tokio::runtime::Handle::current(),
                    wake: wake.clone(),
                    requests: requests.clone(),
                },
                io.original_result_scope(&context).unwrap(),
            ));
            let turn = MvRefreshTurnCustody::new(
                gate.clone(),
                cpu.runtime(),
                scope.clone(),
                window.clone(),
                cancellation.clone(),
                tokio::runtime::Handle::current(),
                wake.clone(),
            );
            let source = CreateOriginal::new(CreateOriginalInput {
                plan,
                seed,
                ports,
                planning,
                operation_id,
                context,
                cancellation,
                io: io.clone(),
                cpu: turn,
            })
            .unwrap();
            source.prepare().await;
            let mut prepared = None;
            source.take_phase_result_in(&mut prepared).unwrap();
            assert!(prepared.unwrap().is_ok());
            let mut phase = source.stage();
            with_driver(
                &control,
                &mut driver,
                poll_fn(|cx| {
                    wake.0.register(cx.waker());
                    Pin::new(&mut phase).poll(cx)
                }),
            )
            .await;
            drop(phase);
            let mut result = None;
            source.take_phase_result_in(&mut result).unwrap();
            assert!(result.unwrap().is_err());
            assert_eq!(prepares.load(Ordering::SeqCst), 1);
            let replacement = CreateIntent::try_new(
                catalog,
                table.clone(),
                EffectIdentity::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
            )
            .unwrap();
            let next = || {
                entrance.acquire(
                    ManagementRequest::for_create_intent(
                        replacement.clone(),
                        EffectScope::CATALOG_COMMIT,
                    ),
                    || false,
                )
            };
            if mode < 2 {
                assert_eq!(
                    entrance.management_phase(&table),
                    MvManagementPhase::AwaitingCreateBinding
                );
                assert!(entrance.begin_unbound_create_observation(&intent).is_ok());
            } else {
                assert_eq!(
                    entrance.management_phase(&table),
                    MvManagementPhase::NotObserved
                );
                assert!(entrance.begin_unbound_create_observation(&intent).is_err());
            }
            // Actual CPU/MIO exit is observable independently of the retained
            // unbound responsibility. Unknown is not a full-close proof.
            with_driver(
                &control,
                &mut driver,
                poll_fn(|cx| {
                    wake.0.register(cx.waker());
                    let _ = source.poll_closed(cx);
                    if source.physical_exit_observed() {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                }),
            )
            .await;
            assert!(source.local_health_failure().is_none());
            drop(source);
            // Disposal releases the original activity lease after physical exit.
            // Unbound CREATE responsibility remains in the management entrance.
            if mode < 2 {
                assert_eq!(
                    entrance.management_phase(&table),
                    MvManagementPhase::AwaitingCreateBinding
                );
                assert_eq!(
                    next().err().unwrap(),
                    ManagementAdmissionError::EffectUnsettled
                );
            } else {
                assert_eq!(
                    entrance.management_phase(&table),
                    MvManagementPhase::NotObserved
                );
                assert!(next().is_ok());
            }
            drop(gate);
            drop(window);
            drop(capacity);
            drop(io);
            drop(scope);
            root.owner.complete();
            root.business.release();
            drop(driver);
            drop(control);
            cpu.shutdown_until(Instant::now() + Duration::from_secs(5))
                .await
                .unwrap();
        }
    }
}
