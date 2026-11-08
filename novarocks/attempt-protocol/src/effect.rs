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

//! Neutral effect values. Captures, permits, SDK carriers and their destructors
//! stay in the host. Completing an effect means handoff, never actual exit.

use crate::capacity::ReservationToken;
use crate::config::DispatchLane;
use crate::failure::AttemptFailure;
use crate::start::PayloadRef;
use novarocks_coordination_contract::{
    AttemptActionId, AttemptRequestToken, AttemptRootProjection, AttemptToLogical, MonotonicInstant,
};
use novarocks_execution_contract::{
    AbortParticipation, AcquireParticipationAdmissionTicket, AdmissionTicketId, CancelTask,
    ContentFingerprint, CredentialDomainId, CredentialEpoch, DomainVersion, EdgeOpenVersion,
    ExchangeEdgeId, LeaseSequence, LeaseValidFor, OperationEnvelope, ParticipationRef, PlanNodeId,
    QuiesceParticipation, ReleaseParticipation, RenewParticipationLease, SplitOffer, TaskIdentity,
    TaskOperationId, TaskStatus,
};
use novarocks_types::BackendProcessId;
use std::num::NonZeroU64;

pub type AttemptEffectId = AttemptActionId;

/// A host-computed fingerprint is passive data; the core never hashes a rich
/// carrier or confidential material during an actor turn.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FrozenContentRef {
    pub resource: PayloadRef,
    pub fingerprint: ContentFingerprint,
    pub queued_bytes: usize,
}

#[derive(Clone, Debug)]
pub enum TaskDomainIntent {
    SplitAssignment {
        node: PlanNodeId,
        offer: SplitOffer,
        payload: PayloadRef,
        queued_bytes: usize,
    },
    TaskDynamicFilter {
        version: DomainVersion,
        payload: FrozenContentRef,
    },
    OpenExchangeEdges {
        version: EdgeOpenVersion,
        edges: Vec<ExchangeEdgeId>,
    },
    CloseExchangeDestination {
        version: DomainVersion,
        edge: ExchangeEdgeId,
        destination: TaskIdentity,
    },
}

#[derive(Clone, Debug)]
pub enum ParticipationDomainIntent {
    CatalogBinding {
        version: DomainVersion,
        payload: FrozenContentRef,
    },
    SharedDynamicFilter {
        version: DomainVersion,
        payload: FrozenContentRef,
    },
    Credential {
        domain: CredentialDomainId,
        epoch: CredentialEpoch,
        material: PayloadRef,
        queued_bytes: usize,
    },
}

/// Typed request metadata is frozen once. Original rich material is resolved
/// only by the executor, which keeps the same allocation for exact replay.
#[derive(Clone, Debug)]
pub enum OperationIntent {
    AcquireParticipationAdmissionTicket(AcquireParticipationAdmissionTicket),
    EstablishParticipation {
        envelope: OperationEnvelope,
        participation: ParticipationRef,
        ticket: AdmissionTicketId,
        initial_lease_sequence: LeaseSequence,
        initial_lease_valid_for: LeaseValidFor,
        material: PayloadRef,
        queued_bytes: usize,
    },
    CreateTask {
        envelope: OperationEnvelope,
        identity: TaskIdentity,
        creation: PayloadRef,
        queued_bytes: usize,
    },
    UpdateTask {
        envelope: OperationEnvelope,
        identity: TaskIdentity,
        domains: std::sync::Arc<[TaskDomainIntent]>,
    },
    AdvanceParticipationDomain {
        envelope: OperationEnvelope,
        participation: ParticipationRef,
        domain: ParticipationDomainIntent,
    },
    RenewParticipationLease(RenewParticipationLease),
    CancelTask(CancelTask),
    QuiesceParticipation(QuiesceParticipation),
    AbortParticipation(AbortParticipation),
    ReleaseParticipation(ReleaseParticipation),
}

#[derive(Clone, Debug)]
pub struct DispatchBatch {
    pub backend: BackendProcessId,
    pub lane: DispatchLane,
    pub control_progress: bool,
    pub operations: Box<[OperationIntent]>,
    pub queued_bytes: usize,
    pub queued_at: MonotonicInstant,
    pub queued_inputs: Box<[crate::input::QueuedInputGrantRef]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectClass {
    Business,
    Publication,
    Reply,
    Cleanup,
    Settlement,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PumpKind {
    SplitAssignment,
    DynamicFilters,
}

#[derive(Clone, Debug)]
pub enum Publication {
    RootStatus(Box<AttemptRootProjection>),
    ProtocolBatch(crate::publication::ProtocolPublication),
    FinalTaskInfo(novarocks_execution_contract::FinalTaskInfo),
    Diagnostics(crate::publication::DiagnosticPublication),
    DevRootObservation { status: TaskStatus, installed: bool },
}

#[derive(Clone, Debug)]
pub enum EffectBody {
    SendBatch {
        batch: DispatchBatch,
        reservation: ReservationToken,
    },
    AdmitQueuedInput {
        operation: OperationIntent,
        lane: DispatchLane,
    },
    Withdraw {
        effect: AttemptEffectId,
    },
    OpenSubscription(crate::observation::SubscriptionRequest),
    /// Update the same generation's applied request record. This does not
    /// alter or restart the immutable RPC. Reopen uses explicit Close/Open.
    AdjustReadWindow(crate::observation::SubscriptionRequest),
    CloseSubscription {
        participation: ParticipationRef,
        generation: NonZeroU64,
    },
    ConfirmReadState {
        token: AttemptRequestToken,
        participation: ParticipationRef,
        generation: NonZeroU64,
        requested_at: MonotonicInstant,
    },
    /// Exact local lifecycle read on the original canonical Source. The
    /// original endpoint and access/capture credentials remain in that Source.
    ObservePreReadyProcess {
        request: AttemptRequestToken,
        original: BackendProcessId,
    },
    StartPump {
        pump: PayloadRef,
        kind: PumpKind,
    },
    StopPump {
        pump: PayloadRef,
    },
    ReadDynamicFilters {
        operation: TaskOperationId,
        request: AttemptRequestToken,
        task: TaskIdentity,
        acknowledged: Option<DomainVersion>,
        advertised: DomainVersion,
    },
    AdmitDynamicFilterFeedback {
        request: AttemptRequestToken,
        task: TaskIdentity,
        version: DomainVersion,
        feedback: PayloadRef,
    },
    CollectDiagnostics {
        request: AttemptRequestToken,
        task: TaskIdentity,
        deadline: MonotonicInstant,
    },
    /// Move the original late/rejected diagnostic object to credentialed
    /// disposal on its same Source. Pollers must never destroy it locally.
    DisposeDiagnosticInfo {
        request: AttemptRequestToken,
        task: TaskIdentity,
        information: Box<novarocks_execution_contract::FinalTaskInfo>,
    },
    ReleaseQueuedInput {
        grant: crate::input::QueuedInputGrantRef,
    },
    ReleaseCreationPayload {
        task: TaskIdentity,
        payload: PayloadRef,
    },
    /// First freeze the exact original Establish allocation in the canonical
    /// host record, then forward its matching authorization request. The same
    /// allocation is resolved by Native transport after the logical reply.
    EstablishAuthorization {
        operation: OperationIntent,
        request: AttemptToLogical<AttemptFailure>,
    },
    Port(AttemptToLogical<AttemptFailure>),
    Publish(Publication),
    /// Local admission is reported even when no remote operation was released.
    PumpUpdateAdmitted {
        pump: PayloadRef,
        delivery: crate::input::SplitDeliveryRef,
        settlement: Option<crate::split_delivery::SplitDeliverySettlement>,
    },
    /// The association begins only at the original operation release point.
    PumpOperationReleased {
        pump: PayloadRef,
        delivery: crate::input::SplitDeliveryRef,
        operation: TaskOperationId,
    },
    /// Preserve the exact receipt, including split watermarks and queue depth.
    PumpOperationSettled {
        pump: PayloadRef,
        delivery: crate::input::SplitDeliveryRef,
        settlement: crate::split_delivery::SplitDeliverySettlement,
    },
}

#[derive(Clone, Debug)]
pub struct AttemptEffect {
    pub id: AttemptEffectId,
    pub class: EffectClass,
    pub body: EffectBody,
}

impl TaskDomainIntent {
    pub const fn kind(&self) -> novarocks_execution_contract::TaskDomainKind {
        use novarocks_execution_contract::TaskDomainKind as K;
        match self {
            Self::SplitAssignment { .. } => K::SplitAssignment,
            Self::TaskDynamicFilter { .. } => K::TaskDynamicFilter,
            Self::OpenExchangeEdges { .. } => K::OpenExchangeEdges,
            Self::CloseExchangeDestination { .. } => K::CloseExchangeDestination,
        }
    }
}

impl ParticipationDomainIntent {
    pub const fn kind(&self) -> novarocks_execution_contract::ParticipationDomainKind {
        use novarocks_execution_contract::ParticipationDomainKind as K;
        match self {
            Self::CatalogBinding { .. } => K::CatalogBinding,
            Self::SharedDynamicFilter { .. } => K::SharedDynamicFilter,
            Self::Credential { .. } => K::Credential,
        }
    }
}

pub const OPERATION_FIXED_BYTES: usize = 256;

impl OperationIntent {
    pub fn envelope(&self) -> OperationEnvelope {
        match self {
            Self::AcquireParticipationAdmissionTicket(r) => r.envelope(),
            Self::EstablishParticipation { envelope, .. }
            | Self::CreateTask { envelope, .. }
            | Self::UpdateTask { envelope, .. }
            | Self::AdvanceParticipationDomain { envelope, .. } => *envelope,
            Self::RenewParticipationLease(r) => r.envelope(),
            Self::CancelTask(r) => r.envelope(),
            Self::QuiesceParticipation(r) => r.envelope(),
            Self::AbortParticipation(r) => r.envelope(),
            Self::ReleaseParticipation(r) => r.envelope(),
        }
    }
    pub fn operation_id(&self) -> TaskOperationId {
        self.envelope().operation_id()
    }
    pub fn kind(&self) -> novarocks_execution_contract::OperationKind {
        self.envelope().kind()
    }
    pub fn shape(&self) -> novarocks_execution_contract::OperationShape {
        use novarocks_execution_contract::OperationShape as S;
        match self {
            Self::AcquireParticipationAdmissionTicket(_) => S::AcquireParticipationAdmissionTicket,
            Self::EstablishParticipation { .. } => S::EstablishParticipation,
            Self::CreateTask { .. } => S::CreateTask,
            Self::UpdateTask { .. } => S::UpdateTask,
            Self::AdvanceParticipationDomain { .. } => S::AdvanceParticipationDomain,
            Self::RenewParticipationLease(_) => S::RenewParticipationLease,
            Self::CancelTask(_) => S::CancelTask,
            Self::QuiesceParticipation(_) => S::QuiesceParticipation,
            Self::AbortParticipation(_) => S::AbortParticipation,
            Self::ReleaseParticipation(_) => S::ReleaseParticipation,
        }
    }
    pub fn lane(&self) -> DispatchLane {
        crate::config::DispatchBudget::lane_of(self.shape())
    }
    pub fn backend_process_id(&self) -> BackendProcessId {
        match self {
            Self::AcquireParticipationAdmissionTicket(r) => r.participation().backend_process_id(),
            Self::EstablishParticipation { participation, .. }
            | Self::AdvanceParticipationDomain { participation, .. } => {
                participation.backend_process_id()
            }
            Self::CreateTask { identity, .. } | Self::UpdateTask { identity, .. } => {
                identity.backend_process_id()
            }
            Self::RenewParticipationLease(r) => r.participation().backend_process_id(),
            Self::CancelTask(r) => r.identity().backend_process_id(),
            Self::QuiesceParticipation(r) => r.participation().backend_process_id(),
            Self::AbortParticipation(r) => r.participation().backend_process_id(),
            Self::ReleaseParticipation(r) => r.participation().backend_process_id(),
        }
    }
    pub fn requires_control_progress(&self) -> bool {
        match self {
            Self::AcquireParticipationAdmissionTicket(_)
            | Self::EstablishParticipation { .. }
            | Self::CreateTask { .. }
            | Self::AdvanceParticipationDomain { .. } => false,
            Self::UpdateTask { domains, .. } => {
                !domains.is_empty()
                    && domains.iter().all(|domain| {
                        matches!(domain, TaskDomainIntent::CloseExchangeDestination { .. })
                    })
            }
            Self::RenewParticipationLease(_)
            | Self::CancelTask(_)
            | Self::QuiesceParticipation(_)
            | Self::AbortParticipation(_)
            | Self::ReleaseParticipation(_) => true,
        }
    }
    pub fn queued_bytes(&self) -> usize {
        let payload = match self {
            Self::CreateTask { queued_bytes, .. }
            | Self::EstablishParticipation { queued_bytes, .. } => return *queued_bytes,
            Self::UpdateTask { domains, .. } => {
                domains.iter().map(TaskDomainIntent::queued_bytes).sum()
            }
            Self::AdvanceParticipationDomain { domain, .. } => domain.queued_bytes(),
            _ => 0,
        };
        payload.saturating_add(OPERATION_FIXED_BYTES)
    }
}

impl TaskDomainIntent {
    /// Payload-side queue charge before the operation fixed allowance.
    pub fn queued_bytes(&self) -> usize {
        match self {
            Self::SplitAssignment { queued_bytes, .. } => *queued_bytes,
            Self::TaskDynamicFilter { payload, .. } => payload.queued_bytes,
            Self::OpenExchangeEdges { edges, .. } => edges.len() * size_of::<u32>(),
            Self::CloseExchangeDestination { .. } => size_of::<TaskIdentity>() + size_of::<u32>(),
        }
    }
}
impl ParticipationDomainIntent {
    pub const fn queued_bytes(&self) -> usize {
        match self {
            Self::CatalogBinding { payload, .. } | Self::SharedDynamicFilter { payload, .. } => {
                payload.queued_bytes
            }
            Self::Credential { queued_bytes, .. } => *queued_bytes,
        }
    }
}
