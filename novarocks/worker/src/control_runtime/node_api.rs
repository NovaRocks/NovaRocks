// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Public application port over the one original Worker node. Native owns
//! wire projection; this port retains the actual R delivery and Root holders.
//! Deadlines use the same process-clock origin as the node's actual pollers.

use super::{
    context_host::{NodeRootDelivery, NodeRootRead, RootReadError},
    control_request::ControlRequestDelivery,
    loading::TaskCreatePayload,
    node::{NodeCreateHandle, NodeHostRefusal, WorkerNodeHost},
    request_endpoint::RequestDelivery,
};
use crate::{
    TaskRootDrainPermit,
    root_result_channel::{
        RootCapacityRefusal, RootChannelError, RootDeliveryOwner, RootMetadataReservation,
    },
};
use novarocks_execution::runtime::fragment::io::ResultWriteAdmission;
use novarocks_execution_contract::{
    AbortQueryContext, AcquireQueryContextAdmissionTicket, AdmissionTicketId, CreateTask,
    EstablishQueryContext, QueryContextRef, QuiesceQueryContext, ReleaseQueryContext,
    RenewQueryExecutionLease, TaskCreationInput, TaskOperationId,
    root_result::{RootResultRead, RootResultReply},
};
use novarocks_worker_protocol::{context::ContextCommand, control::ControlInput};
use std::{fmt, mem::ManuallyDrop, sync::Arc};

pub use novarocks_worker_protocol::{
    context::{ContextReplyBody as WorkerContextReplyBody, RequestResult as WorkerRequestResult},
    control::ControlReplyBody as WorkerControlReplyBody,
    lease::MonotonicInstant as WorkerMonotonicInstant,
};

/// A clone shares the same original node, directory, identity issuer and
/// admitted owners. It creates neither a registry nor an alternate driver.
#[derive(Clone)]
pub struct WorkerTaskControl {
    node: Arc<WorkerNodeHost>,
    clock: Arc<dyn crate::WorkerMonotonicClock>,
}
impl crate::WorkerAdmissionEpochAuthority for WorkerTaskControl {
    fn admission_epoch_capability(&self) -> novarocks_execution_contract::AdmissionEpochCapability {
        self.node.admission_epoch_capability()
    }
}
/// An unaccepted handoff can still own original opaque sources or a partial
/// admission. It must move to its real recovery/disposal owner; ordinary Drop
/// cannot erase those originals or run their foreign destruction here.
#[must_use = "Retain the refused original custody until its actual supervised disposition"]
pub struct WorkerRequestRefusal {
    original: ManuallyDrop<NodeHostRefusal>,
}
impl WorkerRequestRefusal {
    pub fn stage(&self) -> &'static str {
        self.original.operation
    }
    pub(crate) fn into_original(self) -> NodeHostRefusal {
        ManuallyDrop::into_inner(self.original)
    }
}
impl fmt::Debug for WorkerRequestRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerRequestRefusal")
            .field("stage", &self.stage())
            .finish_non_exhaustive()
    }
}
#[derive(Debug)]
pub enum WorkerRequestError {
    HandoffRefused(WorkerRequestRefusal),
    /// The same original observer failed; this does not overwrite an already
    /// committed pure business verdict or report a physical author exit.
    OwnerFailed,
}
impl fmt::Display for WorkerRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HandoffRefused(refusal) => write!(
                f,
                "original Worker handoff was refused at {}",
                refusal.stage()
            ),
            Self::OwnerFailed => f.write_str("the original Worker reply observer failed"),
        }
    }
}
impl std::error::Error for WorkerRequestError {}
impl From<NodeHostRefusal> for WorkerRequestError {
    fn from(original: NodeHostRefusal) -> Self {
        Self::HandoffRefused(WorkerRequestRefusal {
            original: ManuallyDrop::new(original),
        })
    }
}
/// Borrow only the original complete Core decision. Keeping this value keeps
/// that same R's external delivery leg; no synthetic receipt is constructed.
#[must_use = "Hold the original delivery through its actual response lifetime"]
pub struct WorkerControlDelivery {
    original: ControlRequestDelivery,
}
impl WorkerControlDelivery {
    pub fn operation(&self) -> TaskOperationId {
        self.original.request().operation
    }
    pub fn body(&self) -> &WorkerControlReplyBody {
        self.original.body()
    }
}
#[must_use = "Hold the original delivery through its actual response lifetime"]
pub struct WorkerContextDelivery {
    // Original borrowed evidence exits before the R external-delivery leg.
    evidence: Option<crate::ReleasedContextEvidence>,
    original: RequestDelivery<WorkerContextReplyBody>,
}
/// A Create uses the Context R only after its actual address exists. Before
/// that, the same call receives the Root's original retry/closing decision.
pub enum WorkerCreateDelivery {
    Context(WorkerContextDelivery),
    BeforeContext(WorkerControlDelivery),
}
pub enum WorkerCreateReply<'a> {
    Context(&'a WorkerContextReplyBody),
    Root(&'a WorkerControlReplyBody),
}
impl WorkerCreateDelivery {
    pub fn operation(&self) -> TaskOperationId {
        match self {
            Self::Context(original) => original.operation(),
            Self::BeforeContext(original) => original.operation(),
        }
    }
    pub fn reply(&self) -> WorkerCreateReply<'_> {
        match self {
            Self::Context(original) => WorkerCreateReply::Context(original.body()),
            Self::BeforeContext(original) => WorkerCreateReply::Root(original.body()),
        }
    }
}
impl WorkerContextDelivery {
    pub fn operation(&self) -> TaskOperationId {
        self.original.request().operation
    }
    pub fn body(&self) -> &WorkerContextReplyBody {
        self.original.body()
    }
    pub fn released_evidence(&self) -> Option<&crate::ReleasedContextEvidence> {
        self.evidence.as_ref()
    }
}

#[must_use = "Hold the original Task-causal R through its actual response lifetime"]
pub struct WorkerTaskDelivery {
    original:
        RequestDelivery<WorkerContextReplyBody, crate::task_proposal_ingress::RequestMetadata>,
}
impl WorkerTaskDelivery {
    pub fn operation(&self) -> TaskOperationId {
        self.original.request().operation
    }
    pub fn body(&self) -> &WorkerContextReplyBody {
        self.original.body()
    }
}
impl WorkerTaskControl {
    /// Process composition is the only constructor. Numeric source/payload
    /// addresses below all come from this same original node's checked issuer.
    pub(crate) fn from_node(
        node: Arc<WorkerNodeHost>,
        clock: Arc<dyn crate::WorkerMonotonicClock>,
    ) -> Self {
        Self { node, clock }
    }

    pub fn now(&self) -> WorkerMonotonicInstant {
        self.clock.now()
    }

    pub async fn update_task(
        &self,
        context: QueryContextRef,
        original: novarocks_execution_contract::UpdateTask,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerTaskDelivery, WorkerRequestError> {
        let handle = self.node.submit_task_domain(context, original, deadline)?;
        let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
        Ok(WorkerTaskDelivery { original })
    }
    pub async fn advance_query_context_domain(
        &self,
        original: novarocks_execution_contract::AdvanceQueryContextDomain,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerContextDelivery, WorkerRequestError> {
        let handle = self.node.submit_context_domain(original, deadline)?;
        let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
        Ok(WorkerContextDelivery {
            evidence: None,
            original,
        })
    }
    pub async fn observe_admission_ticket(
        &self,
        operation: TaskOperationId,
        ticket: AdmissionTicketId,
        context: QueryContextRef,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerControlDelivery, WorkerRequestError> {
        let handle = self
            .node
            .observe_ticket(operation, deadline, ticket, context, false)?;
        let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
        Ok(WorkerControlDelivery { original })
    }
    pub async fn preflight_expired_establish_ticket(
        &self,
        operation: TaskOperationId,
        ticket: AdmissionTicketId,
        context: QueryContextRef,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerControlDelivery, WorkerRequestError> {
        let handle = self
            .node
            .observe_ticket(operation, deadline, ticket, context, true)?;
        let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
        Ok(WorkerControlDelivery { original })
    }

    pub fn context_for_task(
        &self,
        task: novarocks_execution_contract::TaskIdentity,
    ) -> Option<QueryContextRef> {
        self.node.task_context(task)
    }
    #[cfg(novarocks_dev_controls)]
    pub fn live_task_execution_control_for_test(
        &self,
        identity: novarocks_execution_contract::TaskIdentity,
    ) -> Option<Arc<novarocks_execution::runtime::task_control::TaskExecutionControl>> {
        self.node.live_task_execution_control_for_test(identity)
    }

    pub async fn cancel(
        &self,
        context: QueryContextRef,
        request: novarocks_execution_contract::CancelTask,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerTaskDelivery, WorkerRequestError> {
        let handle = self.node.submit_task_command(
            context,
            request.envelope().operation_id(),
            deadline,
            ContextCommand::Cancel {
                task: request.identity(),
                reason: request.reason(),
            },
        )?;
        let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
        Ok(WorkerTaskDelivery { original })
    }

    pub async fn acquire(
        &self,
        request: AcquireQueryContextAdmissionTicket,
        ticket: AdmissionTicketId,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerControlDelivery, WorkerRequestError> {
        let handle = self.node.submit_control(
            self.node.next_source(),
            request.envelope().operation_id(),
            deadline,
            ControlInput::Acquire { request, ticket },
        )?;
        let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
        Ok(WorkerControlDelivery { original })
    }
    pub async fn establish(
        &self,
        request: EstablishQueryContext,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerControlDelivery, WorkerRequestError> {
        let operation = request.envelope().operation_id();
        let handle = self.node.submit_establish(
            self.node.next_source(),
            operation,
            deadline,
            request,
            self.node.next_payload(),
        )?;
        let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
        Ok(WorkerControlDelivery { original })
    }
    pub async fn create(
        &self,
        request: Arc<CreateTask>,
        input: TaskCreationInput,
        ingress_retained_bytes: usize,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerCreateDelivery, WorkerRequestError> {
        let payload = TaskCreatePayload {
            id: self.node.next_payload(),
            request,
            input,
            ingress_retained_bytes,
        };
        // Node constructs the actual TaskHostBinding from its own held
        // Context, original runtime generation, ports and identity issuer.
        let handle = self
            .node
            .submit_task_create(self.node.next_source(), deadline, payload)?;
        match handle {
            NodeCreateHandle::Context(handle) => {
                let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
                Ok(WorkerCreateDelivery::Context(WorkerContextDelivery {
                    evidence: None,
                    original,
                }))
            }
            NodeCreateHandle::BeforeContext(handle) => {
                let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
                Ok(WorkerCreateDelivery::BeforeContext(WorkerControlDelivery {
                    original,
                }))
            }
        }
    }
    async fn context_request(
        &self,
        context: QueryContextRef,
        operation: TaskOperationId,
        deadline: WorkerMonotonicInstant,
        command: ContextCommand,
    ) -> Result<WorkerContextDelivery, WorkerRequestError> {
        let handle = self.node.submit_context(
            context,
            self.node.next_source(),
            operation,
            deadline,
            command,
        )?;
        let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
        Ok(WorkerContextDelivery {
            evidence: None,
            original,
        })
    }
    pub async fn quiesce(
        &self,
        request: QuiesceQueryContext,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerContextDelivery, WorkerRequestError> {
        self.context_request(
            request.context(),
            request.envelope().operation_id(),
            deadline,
            ContextCommand::Quiesce,
        )
        .await
    }
    pub async fn abort(
        &self,
        request: AbortQueryContext,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerContextDelivery, WorkerRequestError> {
        self.context_request(
            request.context(),
            request.envelope().operation_id(),
            deadline,
            ContextCommand::Abort {
                cause: request.cause(),
            },
        )
        .await
    }
    pub async fn release(
        &self,
        request: ReleaseQueryContext,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerContextDelivery, WorkerRequestError> {
        let mut delivery = self
            .context_request(
                request.context(),
                request.envelope().operation_id(),
                deadline,
                ContextCommand::Release,
            )
            .await?;
        if matches!(delivery.body(), WorkerContextReplyBody::Release { release, .. }
            if *release != novarocks_execution_contract::ReleaseOutcome::NotReady)
        {
            delivery.evidence = Some(self.node.released_evidence(request.context())?);
        }
        Ok(delivery)
    }
    pub async fn renew(
        &self,
        request: RenewQueryExecutionLease,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerContextDelivery, WorkerRequestError> {
        self.context_request(
            request.context(),
            request.envelope().operation_id(),
            deadline,
            ContextCommand::Renew {
                sequence: request.sequence(),
                valid_for: request.valid_for(),
            },
        )
        .await
    }
    /// A declared Native experimental action selects the same real Renew R.
    /// There is no extra fault request, input, Work or protocol command.
    #[cfg(novarocks_dev_controls)]
    pub async fn renew_with_context_owner_panic(
        &self,
        request: RenewQueryExecutionLease,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerContextDelivery, WorkerRequestError> {
        let handle = self.node.submit_context_owner_panic(
            request.context(),
            self.node.next_source(),
            request.envelope().operation_id(),
            deadline,
            ContextCommand::Renew {
                sequence: request.sequence(),
                valid_for: request.valid_for(),
            },
        )?;
        let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
        Ok(WorkerContextDelivery {
            evidence: None,
            original,
        })
    }
    /// This admission intentionally precedes metadata/ACK/payload pregrant.
    /// Only the returned admitted owner can perform the subsequent async read.
    pub fn begin_root_read(
        &self,
        context: QueryContextRef,
        operation: TaskOperationId,
        deadline: WorkerMonotonicInstant,
        request: &RootResultRead,
    ) -> Result<WorkerRootRead, WorkerRootReadError> {
        let original = self
            .node
            .begin_root_read(
                context,
                self.node.next_source(),
                operation,
                deadline,
                request,
            )
            .map_err(WorkerRootReadError::from_original)?;
        Ok(WorkerRootRead { original })
    }
    pub fn begin_task_root_read(
        &self,
        operation: TaskOperationId,
        deadline: WorkerMonotonicInstant,
        request: &RootResultRead,
    ) -> Result<WorkerRootRead, WorkerRootReadError> {
        let context = self
            .node
            .task_context(request.root_task())
            .ok_or_else(|| WorkerRootReadError::from_original(RootReadError::UnknownTask))?;
        self.begin_root_read(context, operation, deadline, request)
    }
}

/// These preserve the actual route/admission facts. Absence is not a Gone
/// receipt, and an admission refusal is not a fabricated channel verdict.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerRootReadErrorKind {
    UnknownContext,
    ContextRemoved,
    AdmissionRefused,
    TaskNotReady,
    NoRoot,
    Channel,
    UnknownTask,
    Capacity,
    WrongOwner,
    BodyFailed,
}
#[derive(Debug)]
pub struct WorkerRootReadError {
    kind: WorkerRootReadErrorKind,
    // Its partial addresses refer to actual originals retained by Node. This
    // error itself owns metadata only, never those Work or source originals.
    original: RootReadError,
}
impl WorkerRootReadError {
    fn from_original(original: RootReadError) -> Self {
        use super::task::TaskRootReadError;
        let kind = match &original {
            RootReadError::UnknownContext => WorkerRootReadErrorKind::UnknownContext,
            RootReadError::ContextRemoved => WorkerRootReadErrorKind::ContextRemoved,
            RootReadError::Request(_) => WorkerRootReadErrorKind::AdmissionRefused,
            RootReadError::Task(
                TaskRootReadError::NotInstalled | TaskRootReadError::OwnerNotReady,
            ) => WorkerRootReadErrorKind::TaskNotReady,
            RootReadError::Task(TaskRootReadError::NoRoot) => WorkerRootReadErrorKind::NoRoot,
            RootReadError::Task(TaskRootReadError::Channel(_)) | RootReadError::Channel(_) => {
                WorkerRootReadErrorKind::Channel
            }
            RootReadError::UnknownTask => WorkerRootReadErrorKind::UnknownTask,
            RootReadError::Capacity(_) => WorkerRootReadErrorKind::Capacity,
            RootReadError::WrongOwner => WorkerRootReadErrorKind::WrongOwner,
            RootReadError::BodyFailed => WorkerRootReadErrorKind::BodyFailed,
        };
        Self { kind, original }
    }
    pub fn kind(&self) -> WorkerRootReadErrorKind {
        self.kind
    }
    pub fn channel_error(&self) -> Option<RootChannelError> {
        match &self.original {
            RootReadError::Channel(error)
            | RootReadError::Task(super::task::TaskRootReadError::Channel(error)) => {
                Some(error.clone())
            }
            _ => None,
        }
    }
    pub fn deferral(&self) -> Option<novarocks_execution_contract::root_result::RootReadDeferral> {
        use super::task::TaskRootReadError;
        use novarocks_execution_contract::root_result::RootReadDeferral;
        match &self.original {
            RootReadError::Task(
                TaskRootReadError::NotInstalled | TaskRootReadError::OwnerNotReady,
            ) => Some(RootReadDeferral::Preparing),
            RootReadError::Capacity(facts) => Some(RootReadDeferral::ReadHoldersBusy(*facts)),
            RootReadError::Channel(RootChannelError::Capacity(cause))
            | RootReadError::Task(TaskRootReadError::Channel(RootChannelError::Capacity(cause))) => {
                Some(RootReadDeferral::ReadHoldersBusy(cause.resource_facts()))
            }
            _ => None,
        }
    }
}
impl fmt::Display for WorkerRootReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "original Worker Root read refused: {:?}", self.kind)
    }
}
impl std::error::Error for WorkerRootReadError {}
impl novarocks_execution_contract::failure::FailureProjection for WorkerRootReadError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use super::context_host::ContextRequestError;
        use super::task::TaskRootReadError;
        use crate::failure_reason::WorkerFailureReason as R;
        use novarocks_control_runtime::transaction::RuntimeError;
        use novarocks_control_runtime_core::capacity::CapacityError;
        use novarocks_execution_contract::failure::{FailureClass, FailureFact};
        let (class, reason) = match &self.original {
            RootReadError::Channel(error)
            | RootReadError::Task(TaskRootReadError::Channel(error)) => {
                return error.failure_fact();
            }
            RootReadError::UnknownContext | RootReadError::ContextRemoved => {
                (FailureClass::Data, R::QueryContextMissing)
            }
            RootReadError::UnknownTask | RootReadError::Task(TaskRootReadError::NoRoot) => {
                (FailureClass::Data, R::RootReadUnknown)
            }
            RootReadError::Task(
                TaskRootReadError::NotInstalled | TaskRootReadError::OwnerNotReady,
            ) => (FailureClass::Control, R::RootControlReadAdmission),
            RootReadError::Capacity(facts) => {
                return FailureFact::new(
                    FailureClass::Resource,
                    R::RootControlReadCapacity.reason(),
                )
                .with_resource(*facts);
            }
            RootReadError::Request(original) => match &original.error {
                ContextRequestError::Runtime(RuntimeError::Capacity(CapacityError::Exhausted(
                    _,
                )))
                | ContextRequestError::RootReadCapacity => {
                    (FailureClass::Resource, R::RootControlReadCapacity)
                }
                ContextRequestError::AdmissionClosed => {
                    (FailureClass::Control, R::RootControlReadAdmission)
                }
                ContextRequestError::UnknownTask => (FailureClass::Data, R::RootReadUnknown),
                ContextRequestError::TaskNotReady => {
                    (FailureClass::Control, R::RootControlReadAdmission)
                }
                _ => (FailureClass::Invariant, R::ProtocolOwnerFailed),
            },
            RootReadError::WrongOwner | RootReadError::BodyFailed => {
                (FailureClass::Invariant, R::ProtocolOwnerFailed)
            }
        };
        FailureFact::new(class, reason.reason())
    }
}
#[must_use = "The original admitted Root R owns this read and its actual data holder"]
pub struct WorkerRootRead {
    original: NodeRootRead,
}
impl WorkerRootRead {
    pub fn try_reserve_native_send_metadata(
        &self,
        bytes: usize,
    ) -> Result<RootMetadataReservation, RootChannelError> {
        self.original.try_reserve_native_send_metadata(bytes)
    }
    pub fn apply_consumed(&self) -> Result<u64, RootChannelError> {
        self.original.apply_consumed()
    }
    pub fn accepted_consumed(&self) -> u64 {
        self.original.accepted_consumed()
    }
    pub fn try_reserve_native_send_backing(
        &self,
    ) -> Result<ResultWriteAdmission, RootChannelError> {
        self.original.try_reserve_native_send_backing()
    }
    pub fn native_send_backing_refusal(&self) -> Result<RootCapacityRefusal, RootChannelError> {
        self.original.native_send_backing_refusal()
    }
    pub async fn read(self) -> Result<WorkerRootDelivery, WorkerRootReadError> {
        let original = self
            .original
            .read()
            .await
            .map_err(WorkerRootReadError::from_original)?;
        Ok(WorkerRootDelivery { original })
    }
}
#[must_use = "Keep the same Root delivery through the last physical send/backing alias"]
pub struct WorkerRootDelivery {
    original: NodeRootDelivery,
}
impl WorkerRootDelivery {
    pub fn reply(&self) -> &RootResultReply {
        self.original.reply()
    }
    pub fn take_drain_permit(&mut self) -> Option<TaskRootDrainPermit> {
        self.original.take_drain_permit()
    }
    pub fn into_parts(
        self,
    ) -> (
        RootResultReply,
        RootDeliveryOwner,
        Option<TaskRootDrainPermit>,
    ) {
        self.original.into_parts()
    }
}

/// The one admitted covered observation R. Frames borrow its original external
/// delivery; neither polling nor dropping a frame advances source coverage.
pub struct WorkerObservationStream {
    original: super::context_host::requests::observation::ContextObservationStream,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerObservationErrorKind {
    ContextRemoved,
    OwnerFailed,
    FrameOutstanding,
    IntakeRejected,
    SourceRejected,
}
#[derive(Debug)]
pub struct WorkerObservationError {
    original: super::context_host::requests::observation::ContextObservationStreamError,
}
impl WorkerObservationError {
    pub fn kind(&self) -> WorkerObservationErrorKind {
        use super::context_host::requests::observation::ContextObservationStreamError as E;
        match &self.original {
            E::ContextRemoved => WorkerObservationErrorKind::ContextRemoved,
            E::FrameOutstanding => WorkerObservationErrorKind::FrameOutstanding,
            E::IntakeRejected(_) => WorkerObservationErrorKind::IntakeRejected,
            E::Source(_) => WorkerObservationErrorKind::SourceRejected,
            E::UnknownRequest | E::OwnerFailed | E::Intake(_) => {
                WorkerObservationErrorKind::OwnerFailed
            }
        }
    }
    pub fn source_refusal(
        &self,
    ) -> Option<&novarocks_worker_protocol::observation::CoveredSubscriptionError> {
        use super::{
            context_host::observation::ContextObservationError as S,
            context_host::requests::observation::ContextObservationStreamError as E,
        };
        match &self.original {
            E::Source(error) => match error.as_ref() {
                S::Source(refusal) => Some(refusal),
                _ => None,
            },
            _ => None,
        }
    }
    pub fn intake_reply(&self) -> Option<&WorkerContextReplyBody> {
        match &self.original {
            super::context_host::requests::observation::ContextObservationStreamError::IntakeRejected(body) => Some(body),
            _ => None,
        }
    }
}
impl fmt::Display for WorkerObservationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "original covered observation {:?}", self.kind())
    }
}
impl std::error::Error for WorkerObservationError {}
#[must_use = "A selected frame is acknowledged only after actual ordered transport acceptance"]
pub struct WorkerObservationFrameDelivery {
    original: super::context_host::requests::observation::ObservationFrameDelivery,
}
impl WorkerObservationFrameDelivery {
    pub fn selected(&self) -> &novarocks_worker_protocol::observation::SelectedCoveredObservation {
        self.original.selected()
    }
    pub fn delivered(
        self,
    ) -> Result<
        novarocks_worker_protocol::observation::CoveredObservationBookmark,
        (WorkerObservationError, Self),
    > {
        match self.original.delivered() {
            Ok(bookmark) => Ok(bookmark),
            Err((original, frame)) => Err((
                WorkerObservationError { original },
                Self { original: frame },
            )),
        }
    }
}
impl WorkerObservationStream {
    pub fn poll_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<WorkerObservationFrameDelivery, WorkerObservationError>> {
        self.original.poll_next(cx).map(|result| {
            result
                .map(|original| WorkerObservationFrameDelivery { original })
                .map_err(|original| WorkerObservationError { original })
        })
    }
    pub async fn next(&mut self) -> Result<WorkerObservationFrameDelivery, WorkerObservationError> {
        std::future::poll_fn(|cx| self.poll_next(cx)).await
    }
    /// Issues a bookmark of this same source's real delivered prefix. This
    /// neither acknowledges a selected frame nor opens a new request.
    pub fn bookmark(
        &self,
    ) -> Result<
        novarocks_worker_protocol::observation::CoveredObservationBookmark,
        WorkerObservationError,
    > {
        self.original
            .bookmark()
            .map_err(|original| WorkerObservationError { original })
    }
}
impl WorkerTaskControl {
    pub fn begin_status_subscription(
        &self,
        context: QueryContextRef,
        operation: TaskOperationId,
        deadline: WorkerMonotonicInstant,
        generation: u64,
        position: novarocks_worker_protocol::observation::CoveredSubscriptionRequest,
    ) -> Result<WorkerObservationStream, WorkerRequestError> {
        let original = self.node.begin_status_subscription(
            context,
            self.node.next_source(),
            operation,
            deadline,
            generation,
            position,
        )?;
        Ok(WorkerObservationStream { original })
    }
}

/// A read keeps its exact original causal or positively sealed-source R until
/// the real external reply exits. Both variants carry the Core's original body.
#[must_use = "Retain the actual read delivery through response projection and send"]
pub struct WorkerTaskReadDelivery {
    // The original loan returns before its external endpoint is ended.
    payload: Option<super::task_read_payload::DynamicFilterReadDelivery>,
    original: TaskReadDelivery,
}
enum TaskReadDelivery {
    Live(RequestDelivery<WorkerContextReplyBody, crate::task_proposal_ingress::RequestMetadata>),
    Sealed(RequestDelivery<WorkerContextReplyBody>),
}
impl WorkerTaskReadDelivery {
    pub fn dynamic_filter_payload(
        &self,
    ) -> Option<(
        novarocks_execution_contract::DomainVersion,
        &dyn novarocks_execution_contract::task_execution::domain::CodecOwnedContent,
    )> {
        self.payload
            .as_ref()
            .map(|loan| (loan.version(), loan.payload()))
    }
    pub fn operation(&self) -> TaskOperationId {
        match &self.original {
            TaskReadDelivery::Live(original) => original.request().operation,
            TaskReadDelivery::Sealed(original) => original.request().operation,
        }
    }
    pub fn body(&self) -> &WorkerContextReplyBody {
        match &self.original {
            TaskReadDelivery::Live(original) => original.body(),
            TaskReadDelivery::Sealed(original) => original.body(),
        }
    }
}
impl WorkerTaskControl {
    async fn task_read(
        &self,
        context: QueryContextRef,
        operation: TaskOperationId,
        deadline: WorkerMonotonicInstant,
        command: ContextCommand,
    ) -> Result<WorkerTaskReadDelivery, WorkerRequestError> {
        use super::node::NodeTaskReadHandle;
        let (original, payload) = match self
            .node
            .submit_task_read(context, operation, deadline, command)?
        {
            NodeTaskReadHandle::Live(handle, shared) => {
                let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
                let payload = match &shared {
                    Some(shared) => shared
                        .take_ready()
                        .map_err(|_| WorkerRequestError::OwnerFailed)?,
                    None => None,
                };
                match (original.body(), &payload) {
                    (
                        WorkerContextReplyBody::ReadPayload {
                            version,
                            payload: id,
                            ..
                        },
                        Some(loan),
                    ) if *version == loan.version() && *id == loan.payload_id() => {}
                    (WorkerContextReplyBody::ReadPayload { .. }, _) | (_, Some(_)) => {
                        return Err(WorkerRequestError::OwnerFailed);
                    }
                    _ => {}
                }
                (TaskReadDelivery::Live(original), payload)
            }
            NodeTaskReadHandle::Sealed(handle) => (
                TaskReadDelivery::Sealed(
                    handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?,
                ),
                None,
            ),
        };
        Ok(WorkerTaskReadDelivery { payload, original })
    }
    pub async fn get_final_task_info(
        &self,
        context: QueryContextRef,
        request: novarocks_execution_contract::GetFinalTaskInfo,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerTaskReadDelivery, WorkerRequestError> {
        self.task_read(
            context,
            request.envelope().operation_id(),
            deadline,
            ContextCommand::GetFinalInfo {
                task: request.identity(),
            },
        )
        .await
    }
    pub async fn get_dynamic_filters(
        &self,
        context: QueryContextRef,
        request: novarocks_execution_contract::FetchTaskDynamicFilters,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerTaskReadDelivery, WorkerRequestError> {
        self.task_read(
            context,
            request.envelope().operation_id(),
            deadline,
            ContextCommand::GetDynamicFilters {
                task: request.identity(),
                acknowledged: request.acknowledged_version(),
            },
        )
        .await
    }
}

impl WorkerTaskControl {
    /// Actual charged-resource samples; not queue counts or convergence proof.
    pub fn metrics_snapshot(&self) -> super::node_metrics::WorkerNodeMetricsSnapshot {
        self.node.metrics_snapshot()
    }
}

impl WorkerTaskControl {
    /// Nonblocking reads of the actual charged-preparation owner and physical
    /// fair queue. Busy means unavailable, never zero or a stale replay.
    pub fn try_preparation_snapshot(
        &self,
    ) -> Result<
        Option<super::node_metrics::WorkerPreparationSnapshot>,
        super::node_metrics::WorkerPreparationObservationError,
    > {
        self.node.try_preparation_snapshot()
    }
}
