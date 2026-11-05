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
    control_request::ControlRequestDelivery,
    loading::TaskCreatePayload,
    node::{NodeCreateHandle, NodeHostRefusal, WorkerNodeHost},
    participation_host::{NodeRootDelivery, NodeRootRead, RootReadError},
    request_endpoint::RequestDelivery,
};
use crate::{
    TaskRootDrainPermit,
    root_result_channel::{
        RootCapacityRefusal, RootChannelError, RootDeliveryOwner, RootMetadataReservation,
    },
};
use novarocks_execution::runtime::task::io::ResultWriteAdmission;
use novarocks_execution_contract::{
    AbortParticipation, AcquireParticipationAdmissionTicket, AdmissionTicketId, CreateTask,
    EstablishParticipation, ParticipationRef, QuiesceParticipation, ReleaseParticipation,
    RenewParticipationLease, TaskCreationInput, TaskOperationId,
    root_result::{RootResultRead, RootResultReply},
};
use novarocks_worker_protocol::{control::ControlInput, participation::ParticipationCommand};
use std::{fmt, mem::ManuallyDrop, sync::Arc};

pub use novarocks_worker_protocol::{
    control::ControlReplyBody as WorkerControlReplyBody,
    lease::MonotonicInstant as WorkerMonotonicInstant,
    participation::{
        ParticipationReplyBody as WorkerParticipationReplyBody,
        RequestResult as WorkerRequestResult,
    },
};

/// The original admitted transport responsibility, not a new request permit.
/// Trusted role composition implements this only for admission bookkeeping:
/// Drop may return scheduler credits, but must perform no I/O, user callback,
/// or destruction of a rich input. Rich inputs have their separate host job.
pub trait WorkerIngressBacking: Send + Sync + 'static {}

/// One validated batch item's responsibility for its original ingress owner.
/// Sharing the underlying owner grants neither another Native permit nor
/// another item; only the bounded ingress projection constructs these values.
/// The wrapper is deliberately not Clone and has no empty/default form.
pub struct WorkerIngressResponsibility {
    _original: Arc<dyn WorkerIngressBacking>,
}
impl WorkerIngressResponsibility {
    pub fn from_admitted(original: Arc<dyn WorkerIngressBacking>) -> Self {
        Self {
            _original: original,
        }
    }
}
impl fmt::Debug for WorkerIngressResponsibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerIngressResponsibility")
            .finish_non_exhaustive()
    }
}

/// A clone shares the same original node, directory, identity issuer and
/// admitted owners. It creates neither a registry nor an alternate driver.
#[derive(Clone)]
pub struct WorkerTaskControl {
    node: Arc<WorkerNodeHost>,
    clock: Arc<dyn crate::WorkerMonotonicClock>,
    backend: novarocks_types::BackendProcessId,
}
impl crate::WorkerAdmissionEpochAuthority for WorkerTaskControl {
    fn admission_epoch_capability(&self) -> novarocks_execution_contract::AdmissionEpochCapability {
        self.node.admission_epoch_capability()
    }
}
enum WorkerRefusalCustody {
    Opaque(ManuallyDrop<NodeHostRefusal>),
    /// The existing paid Root cleanup owns the rich original and ingress.
    /// This original negative diagnostic carries no source or capacity.
    Neutral(super::node::NeutralNodeRequestRefusal),
}

/// An unaccepted handoff can still own original opaque sources or a partial
/// admission. It must move to its real recovery/disposal owner; ordinary Drop
/// cannot erase those originals or run their foreign destruction here.
/// A closed pre-admission input already accepted by the original Root's
/// physical cleanup owner retains only its same negative diagnostic here.
#[must_use = "Retain the refused original custody until its actual supervised disposition"]
pub struct WorkerRequestRefusal {
    original: WorkerRefusalCustody,
}
impl WorkerRequestRefusal {
    pub fn stage(&self) -> &'static str {
        match &self.original {
            WorkerRefusalCustody::Opaque(original) => original.operation,
            WorkerRefusalCustody::Neutral(original) => original.stage(),
        }
    }
    #[cfg(test)]
    pub(crate) fn into_original_for_test(self) -> NodeHostRefusal {
        match self.original {
            WorkerRefusalCustody::Opaque(original) => ManuallyDrop::into_inner(original),
            WorkerRefusalCustody::Neutral(original) => original.into_original_for_test(),
        }
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
    /// The actual request owner failed before admission or its same original
    /// reply observer failed. Existing commitments and physical exits remain
    /// independently owned facts.
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
            Self::OwnerFailed => f.write_str("the original Worker request owner failed"),
        }
    }
}
impl std::error::Error for WorkerRequestError {}
impl From<NodeHostRefusal> for WorkerRequestError {
    fn from(original: NodeHostRefusal) -> Self {
        Self::HandoffRefused(WorkerRequestRefusal {
            original: WorkerRefusalCustody::Opaque(ManuallyDrop::new(original)),
        })
    }
}
impl From<super::node::NodeRequestRefusal> for WorkerRequestError {
    fn from(original: super::node::NodeRequestRefusal) -> Self {
        match original {
            super::node::NodeRequestRefusal::OwnerFailed => Self::OwnerFailed,
            super::node::NodeRequestRefusal::Invariant(original) => original.into(),
            super::node::NodeRequestRefusal::Neutral(original) => {
                Self::HandoffRefused(WorkerRequestRefusal {
                    original: WorkerRefusalCustody::Neutral(original),
                })
            }
        }
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
pub struct WorkerParticipationDelivery {
    // Original borrowed evidence exits before the R external-delivery leg.
    evidence: Option<crate::ReleasedParticipationEvidence>,
    original: RequestDelivery<WorkerParticipationReplyBody>,
}
/// A Create uses the Participation R only after its actual address exists. Before
/// that, the same call receives the Root's original retry/closing decision.
pub enum WorkerCreateDelivery {
    Participation(WorkerParticipationDelivery),
    BeforeParticipation(WorkerControlDelivery),
}
pub enum WorkerCreateReply<'a> {
    Participation(&'a WorkerParticipationReplyBody),
    Root(&'a WorkerControlReplyBody),
}
impl WorkerCreateDelivery {
    pub fn operation(&self) -> TaskOperationId {
        match self {
            Self::Participation(original) => original.operation(),
            Self::BeforeParticipation(original) => original.operation(),
        }
    }
    pub fn reply(&self) -> WorkerCreateReply<'_> {
        match self {
            Self::Participation(original) => WorkerCreateReply::Participation(original.body()),
            Self::BeforeParticipation(original) => WorkerCreateReply::Root(original.body()),
        }
    }
}
impl WorkerParticipationDelivery {
    pub fn operation(&self) -> TaskOperationId {
        self.original.request().operation
    }
    pub fn body(&self) -> &WorkerParticipationReplyBody {
        self.original.body()
    }
    pub fn released_evidence(&self) -> Option<&crate::ReleasedParticipationEvidence> {
        self.evidence.as_ref()
    }
}

#[must_use = "Hold the original paid R through its actual response lifetime"]
pub struct WorkerTaskDelivery {
    original: RequestDelivery<
        WorkerParticipationReplyBody,
        crate::task_proposal_ingress::RequestMetadata,
    >,
}
impl WorkerTaskDelivery {
    pub fn operation(&self) -> TaskOperationId {
        self.original.request().operation
    }
    pub fn body(&self) -> &WorkerParticipationReplyBody {
        self.original.body()
    }
    #[cfg(test)]
    pub(crate) fn request_deadline_for_test(&self) -> WorkerMonotonicInstant {
        self.original.request().deadline
    }
}
/// An admitted cancellation retains its exact original R delivery. A lookup
/// refusal is only a neutral pre-admission fact and owns no replacement R.
#[must_use = "Project the original cancellation decision without discarding its delivery"]
pub enum WorkerCancelOutcome {
    Task(WorkerTaskDelivery),
    Participation(WorkerParticipationDelivery),
    Refused {
        operation: TaskOperationId,
        reason: novarocks_worker_protocol::participation::TaskControlRefusal,
    },
}

impl WorkerTaskControl {
    /// Process composition is the only constructor. Numeric source/payload
    /// addresses below all come from this same original node's checked issuer.
    pub(crate) fn from_node(
        node: Arc<WorkerNodeHost>,
        clock: Arc<dyn crate::WorkerMonotonicClock>,
        backend: novarocks_types::BackendProcessId,
    ) -> Self {
        Self {
            node,
            clock,
            backend,
        }
    }

    pub fn now(&self) -> WorkerMonotonicInstant {
        self.clock.now()
    }

    pub async fn update_task(
        &self,
        participation: ParticipationRef,
        original: novarocks_execution_contract::UpdateTask,
        deadline: WorkerMonotonicInstant,
        ingress: WorkerIngressResponsibility,
    ) -> Result<WorkerTaskDelivery, WorkerRequestError> {
        let handle = self
            .node
            .submit_task_domain(participation, original, deadline, ingress)?;
        let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
        Ok(WorkerTaskDelivery { original })
    }
    pub async fn advance_participation_domain(
        &self,
        original: novarocks_execution_contract::AdvanceParticipationDomain,
        deadline: WorkerMonotonicInstant,
        ingress: WorkerIngressResponsibility,
    ) -> Result<WorkerParticipationDelivery, WorkerRequestError> {
        let handle = self
            .node
            .submit_participation_domain(original, deadline, ingress)?;
        let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
        Ok(WorkerParticipationDelivery {
            evidence: None,
            original,
        })
    }
    pub async fn observe_admission_ticket(
        &self,
        operation: TaskOperationId,
        ticket: AdmissionTicketId,
        participation: ParticipationRef,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerControlDelivery, WorkerRequestError> {
        let handle = self
            .node
            .observe_ticket(operation, deadline, ticket, participation, false)?;
        let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
        Ok(WorkerControlDelivery { original })
    }
    pub async fn preflight_expired_establish_ticket(
        &self,
        operation: TaskOperationId,
        ticket: AdmissionTicketId,
        participation: ParticipationRef,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerControlDelivery, WorkerRequestError> {
        let handle = self
            .node
            .observe_ticket(operation, deadline, ticket, participation, true)?;
        let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
        Ok(WorkerControlDelivery { original })
    }

    pub fn participation_for_task(
        &self,
        task: novarocks_execution_contract::TaskIdentity,
    ) -> Option<ParticipationRef> {
        self.node.task_participation(task)
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
        request: novarocks_execution_contract::CancelTask,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerCancelOutcome, WorkerRequestError> {
        let operation = request.envelope().operation_id();
        match self.node.submit_cancel_task(request, deadline)? {
            super::node::NodeCancelHandle::Task(handle) => {
                let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
                Ok(WorkerCancelOutcome::Task(WorkerTaskDelivery { original }))
            }
            super::node::NodeCancelHandle::Participation(handle) => {
                let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
                Ok(WorkerCancelOutcome::Participation(
                    WorkerParticipationDelivery {
                        evidence: None,
                        original,
                    },
                ))
            }
            super::node::NodeCancelHandle::Refused(reason) => {
                Ok(WorkerCancelOutcome::Refused { operation, reason })
            }
        }
    }

    pub async fn acquire(
        &self,
        request: AcquireParticipationAdmissionTicket,
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
        request: EstablishParticipation,
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
        ingress: WorkerIngressResponsibility,
    ) -> Result<WorkerCreateDelivery, WorkerRequestError> {
        let payload = TaskCreatePayload {
            id: self.node.next_payload(),
            request,
            input,
            ingress_retained_bytes,
        };
        // Node constructs the actual TaskHostBinding from its own held
        // Participation, original runtime generation, ports and identity issuer.
        let handle =
            self.node
                .submit_task_create(self.node.next_source(), deadline, payload, ingress)?;
        match handle {
            NodeCreateHandle::Participation(handle) => {
                let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
                Ok(WorkerCreateDelivery::Participation(
                    WorkerParticipationDelivery {
                        evidence: None,
                        original,
                    },
                ))
            }
            NodeCreateHandle::BeforeParticipation(handle) => {
                let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
                Ok(WorkerCreateDelivery::BeforeParticipation(
                    WorkerControlDelivery { original },
                ))
            }
        }
    }
    async fn participation_request(
        &self,
        participation: ParticipationRef,
        operation: TaskOperationId,
        deadline: WorkerMonotonicInstant,
        command: ParticipationCommand,
    ) -> Result<WorkerParticipationDelivery, WorkerRequestError> {
        let handle = self.node.submit_participation(
            participation,
            self.node.next_source(),
            operation,
            deadline,
            command,
        )?;
        let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
        Ok(WorkerParticipationDelivery {
            evidence: None,
            original,
        })
    }
    pub async fn quiesce(
        &self,
        request: QuiesceParticipation,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerParticipationDelivery, WorkerRequestError> {
        self.participation_request(
            request.participation(),
            request.envelope().operation_id(),
            deadline,
            ParticipationCommand::Quiesce,
        )
        .await
    }
    pub async fn abort(
        &self,
        request: AbortParticipation,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerParticipationDelivery, WorkerRequestError> {
        self.participation_request(
            request.participation(),
            request.envelope().operation_id(),
            deadline,
            ParticipationCommand::Abort {
                cause: request.cause(),
            },
        )
        .await
    }
    pub async fn release(
        &self,
        request: ReleaseParticipation,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerParticipationDelivery, WorkerRequestError> {
        let mut delivery = self
            .participation_request(
                request.participation(),
                request.envelope().operation_id(),
                deadline,
                ParticipationCommand::Release,
            )
            .await?;
        if matches!(delivery.body(), WorkerParticipationReplyBody::Release { release, .. }
            if *release != novarocks_execution_contract::ReleaseOutcome::NotReady)
        {
            delivery.evidence = Some(self.node.released_evidence(request.participation())?);
        }
        Ok(delivery)
    }
    pub async fn renew(
        &self,
        request: RenewParticipationLease,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerParticipationDelivery, WorkerRequestError> {
        self.participation_request(
            request.participation(),
            request.envelope().operation_id(),
            deadline,
            ParticipationCommand::Renew {
                sequence: request.sequence(),
                valid_for: request.valid_for(),
            },
        )
        .await
    }
    /// A declared Native experimental action selects the same real Renew R.
    /// There is no extra fault request, input, Work or protocol command.
    #[cfg(novarocks_dev_controls)]
    pub async fn renew_with_participation_owner_panic(
        &self,
        request: RenewParticipationLease,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerParticipationDelivery, WorkerRequestError> {
        let handle = self.node.submit_participation_owner_panic(
            request.participation(),
            self.node.next_source(),
            request.envelope().operation_id(),
            deadline,
            ParticipationCommand::Renew {
                sequence: request.sequence(),
                valid_for: request.valid_for(),
            },
        )?;
        let original = handle.await.map_err(|_| WorkerRequestError::OwnerFailed)?;
        Ok(WorkerParticipationDelivery {
            evidence: None,
            original,
        })
    }
    /// This admission intentionally precedes metadata/ACK/payload pregrant.
    /// Only the returned admitted owner can perform the subsequent async read.
    pub fn begin_root_read(
        &self,
        participation: ParticipationRef,
        operation: TaskOperationId,
        deadline: WorkerMonotonicInstant,
        request: &RootResultRead,
    ) -> Result<WorkerRootRead, WorkerRootReadError> {
        if request.root_task().backend_process_id() != self.backend {
            return Err(WorkerRootReadError::from_original(
                RootReadError::ForeignProcess,
            ));
        }
        let original = self
            .node
            .begin_root_read(
                participation,
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
        if request.root_task().backend_process_id() != self.backend {
            return Err(WorkerRootReadError::from_original(
                RootReadError::ForeignProcess,
            ));
        }
        let participation = self
            .node
            .task_participation(request.root_task())
            .ok_or_else(|| WorkerRootReadError::from_original(RootReadError::UnknownTask))?;
        self.begin_root_read(participation, operation, deadline, request)
    }
}

/// These preserve the actual route/admission facts. Absence is not a Gone
/// receipt, and an admission refusal is not a fabricated channel verdict.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerRootReadErrorKind {
    ForeignProcess,
    UnknownParticipation,
    ParticipationRemoved,
    AdmissionRefused,
    TaskNotReady,
    NoRoot,
    Channel,
    UnknownTask,
    Capacity,
    WrongOwner,
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
            RootReadError::ForeignProcess => WorkerRootReadErrorKind::ForeignProcess,
            RootReadError::UnknownParticipation => WorkerRootReadErrorKind::UnknownParticipation,
            RootReadError::ParticipationRemoved => WorkerRootReadErrorKind::ParticipationRemoved,
            RootReadError::Request(_) => WorkerRootReadErrorKind::AdmissionRefused,
            RootReadError::Task(
                TaskRootReadError::NotInstalled | TaskRootReadError::OwnerNotReady,
            ) => WorkerRootReadErrorKind::TaskNotReady,
            RootReadError::Task(TaskRootReadError::NoRoot) => WorkerRootReadErrorKind::NoRoot,
            RootReadError::Task(TaskRootReadError::Channel(_))
            | RootReadError::Channel(_)
            | RootReadError::Closed { .. } => WorkerRootReadErrorKind::Channel,
            RootReadError::UnknownTask => WorkerRootReadErrorKind::UnknownTask,
            RootReadError::Capacity(_) => WorkerRootReadErrorKind::Capacity,
            RootReadError::WrongOwner => WorkerRootReadErrorKind::WrongOwner,
        };
        Self { kind, original }
    }
    pub fn kind(&self) -> WorkerRootReadErrorKind {
        self.kind
    }
    /// The original sealed channel's applied ACK frontier, without a new read.
    pub fn closed_frontier(&self) -> Option<u64> {
        match &self.original {
            RootReadError::Closed { accepted_consumed } => Some(*accepted_consumed),
            _ => None,
        }
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
        use super::participation_host::ParticipationRequestError;
        use super::task::TaskRootReadError;
        use crate::failure_reason::WorkerFailureReason as R;
        use novarocks_control_runtime::transaction::RuntimeError;
        use novarocks_control_runtime_core::capacity::CapacityError;
        use novarocks_execution_contract::failure::{FailureClass, FailureFact};
        let (class, reason) = match &self.original {
            RootReadError::ForeignProcess => (FailureClass::Data, R::RootReadIdentity),
            RootReadError::Closed { .. } => {
                return RootChannelError::Closed.failure_fact();
            }
            RootReadError::Channel(error)
            | RootReadError::Task(TaskRootReadError::Channel(error)) => {
                return error.failure_fact();
            }
            RootReadError::UnknownParticipation | RootReadError::ParticipationRemoved => {
                (FailureClass::Data, R::ParticipationMissing)
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
                ParticipationRequestError::Runtime(RuntimeError::Capacity(
                    CapacityError::Exhausted(_),
                )) => (FailureClass::Resource, R::RootControlReadCapacity),
                ParticipationRequestError::AdmissionClosed => {
                    (FailureClass::Control, R::RootControlReadAdmission)
                }
                ParticipationRequestError::UnknownTask => (FailureClass::Data, R::RootReadUnknown),
                ParticipationRequestError::TaskNotReady => {
                    (FailureClass::Control, R::RootControlReadAdmission)
                }
                _ => (FailureClass::Invariant, R::ProtocolOwnerFailed),
            },
            RootReadError::WrongOwner => (FailureClass::Invariant, R::ProtocolOwnerFailed),
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
    original: super::participation_host::requests::observation::ParticipationObservationStream,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerObservationErrorKind {
    ParticipationRemoved,
    OwnerFailed,
    FrameOutstanding,
    IntakeRejected,
    SourceRejected,
}
#[derive(Debug)]
pub struct WorkerObservationError {
    original: super::participation_host::requests::observation::ParticipationObservationStreamError,
}
impl WorkerObservationError {
    pub fn kind(&self) -> WorkerObservationErrorKind {
        use super::participation_host::requests::observation::ParticipationObservationStreamError as E;
        match &self.original {
            E::ParticipationRemoved => WorkerObservationErrorKind::ParticipationRemoved,
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
            participation_host::observation::ParticipationObservationError as S,
            participation_host::requests::observation::ParticipationObservationStreamError as E,
        };
        match &self.original {
            E::Source(error) => match error.as_ref() {
                S::Source(refusal) => Some(refusal),
                _ => None,
            },
            _ => None,
        }
    }
    pub fn intake_reply(&self) -> Option<&WorkerParticipationReplyBody> {
        match &self.original {
            super::participation_host::requests::observation::ParticipationObservationStreamError::IntakeRejected(body) => Some(body),
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
    original: super::participation_host::requests::observation::ObservationFrameDelivery,
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
        participation: ParticipationRef,
        operation: TaskOperationId,
        deadline: WorkerMonotonicInstant,
        generation: u64,
        position: novarocks_worker_protocol::observation::CoveredSubscriptionRequest,
    ) -> Result<WorkerObservationStream, WorkerRequestError> {
        let original = self.node.begin_status_subscription(
            participation,
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
    Live(
        RequestDelivery<
            WorkerParticipationReplyBody,
            crate::task_proposal_ingress::RequestMetadata,
        >,
    ),
    Sealed(RequestDelivery<WorkerParticipationReplyBody>),
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
    pub fn body(&self) -> &WorkerParticipationReplyBody {
        match &self.original {
            TaskReadDelivery::Live(original) => original.body(),
            TaskReadDelivery::Sealed(original) => original.body(),
        }
    }
}
impl WorkerTaskControl {
    async fn task_read(
        &self,
        participation: ParticipationRef,
        operation: TaskOperationId,
        deadline: WorkerMonotonicInstant,
        command: ParticipationCommand,
    ) -> Result<WorkerTaskReadDelivery, WorkerRequestError> {
        use super::node::NodeTaskReadHandle;
        let (original, payload) =
            match self
                .node
                .submit_task_read(participation, operation, deadline, command)?
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
                            WorkerParticipationReplyBody::ReadPayload {
                                version,
                                payload: id,
                                ..
                            },
                            Some(loan),
                        ) if *version == loan.version() && *id == loan.payload_id() => {}
                        (WorkerParticipationReplyBody::ReadPayload { .. }, _) | (_, Some(_)) => {
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
        participation: ParticipationRef,
        request: novarocks_execution_contract::GetFinalTaskInfo,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerTaskReadDelivery, WorkerRequestError> {
        self.task_read(
            participation,
            request.envelope().operation_id(),
            deadline,
            ParticipationCommand::GetFinalInfo {
                task: request.identity(),
            },
        )
        .await
    }
    pub async fn get_dynamic_filters(
        &self,
        participation: ParticipationRef,
        request: novarocks_execution_contract::FetchTaskDynamicFilters,
        deadline: WorkerMonotonicInstant,
    ) -> Result<WorkerTaskReadDelivery, WorkerRequestError> {
        self.task_read(
            participation,
            request.envelope().operation_id(),
            deadline,
            ParticipationCommand::GetDynamicFilters {
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

impl WorkerTaskControl {
    /// Nonblocking original registered Root census, not a settlement receipt.
    /// Busy, incomplete, poisoned or overflowing observations are unavailable.
    pub fn try_root_ownership_snapshot(
        &self,
    ) -> Option<crate::root_result_channel::RootOwnershipSnapshot> {
        self.node.try_root_ownership_snapshot()
    }
}

impl WorkerTaskControl {
    /// Independent raw samples of the original production poller slots. None
    /// means unavailable; no duration, phase or timestamp grants a health,
    /// scheduling, termination, physical-exit or resource-retirement verdict.
    pub fn try_control_plane_snapshot(
        &self,
    ) -> Option<super::node_metrics::WorkerControlPlaneSnapshot> {
        self.node.try_control_plane_snapshot()
    }
    /// Cumulative original-owner diagnostics. These independently sampled
    /// counters are neither a coherent instant nor a capacity/exit receipt.
    pub fn runtime_observation_snapshot(
        &self,
    ) -> Option<super::node_metrics::WorkerRuntimeObservationSnapshot> {
        self.node.runtime_observation_snapshot()
    }
    /// Advance at most `budget` original inventory records without blocking.
    /// Only a completing page carries a fresh interval; no cached inventory
    /// is supplied for busy or incomplete traversals.
    pub fn try_runtime_inventory_page(
        &self,
        budget: usize,
    ) -> Result<
        Option<super::node_metrics::WorkerRuntimeInventoryPage>,
        super::node_metrics::WorkerRuntimeObservationError,
    > {
        self.node.try_runtime_inventory_page(budget)
    }
}

impl WorkerTaskControl {
    /// Nonblocking original admission projection and actual ticket charges.
    /// Logical closure does not prove the physical ticket fee has returned.
    /// Missing, busy, poisoned or overflowing samples return None as a whole.
    pub fn try_admission_snapshot(&self) -> Option<super::node_metrics::WorkerAdmissionSnapshot> {
        self.node.try_admission_snapshot()
    }
}

impl WorkerTaskControl {
    /// Current original execution custody and physical Root borrows. Cold
    /// retention does not grant capacity reuse or prove final Scope removal.
    /// Busy, incomplete, poisoned or overflowing owners are unavailable.
    pub fn try_execution_quiescence_snapshot(
        &self,
    ) -> Option<super::node_metrics::WorkerExecutionQuiescenceSnapshot> {
        self.node.try_execution_quiescence_snapshot()
    }
}

impl WorkerTaskControl {
    /// Original retained owners, sampled without waiting for any owner lock.
    /// Missing/busy/poisoned census omits the sample, never substitutes zero.
    pub fn try_protocol_retention_snapshot(
        &self,
    ) -> Option<super::node_metrics::WorkerProtocolRetentionSnapshot> {
        self.node.try_protocol_retention_snapshot()
    }
}
