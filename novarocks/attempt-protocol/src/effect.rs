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
    QuiesceParticipation, ReleaseParticipation, SplitOffer, TaskIdentity, TaskOperationId,
    TaskStatus,
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
        domains: Vec<TaskDomainIntent>,
    },
    UpdateParticipation {
        envelope: OperationEnvelope,
        participation: ParticipationRef,
        domains: Vec<ParticipationDomainIntent>,
    },
    CancelTask(CancelTask),
    QuiesceParticipation(QuiesceParticipation),
    AbortParticipation(AbortParticipation),
    ReleaseParticipation(ReleaseParticipation),
}

#[derive(Clone, Debug)]
pub struct DispatchBatch {
    pub backend: BackendProcessId,
    pub lane: DispatchLane,
    pub operations: Box<[OperationIntent]>,
    pub queued_bytes: usize,
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
    CredentialRotation,
}

#[derive(Clone, Debug)]
pub enum Publication {
    RootStatus(Box<AttemptRootProjection>),
    ProtocolTask(TaskStatus),
    FinalTaskInfo(novarocks_execution_contract::FinalTaskInfo),
    DevRootObservation(TaskStatus),
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
    OpenSubscription {
        participation: ParticipationRef,
        generation: NonZeroU64,
    },
    AdjustReadWindow {
        participation: ParticipationRef,
        generation: NonZeroU64,
    },
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
    StartPump {
        pump: PayloadRef,
        kind: PumpKind,
    },
    StopPump {
        pump: PayloadRef,
    },
    ReadDynamicFilters {
        task: TaskIdentity,
    },
    CollectDiagnostics {
        task: TaskIdentity,
        deadline: MonotonicInstant,
    },
    ReleaseCreationPayload {
        task: TaskIdentity,
        payload: PayloadRef,
    },
    Port(AttemptToLogical<AttemptFailure>),
    Publish(Publication),
    /// The original producer learns the actual core-accepted operation result.
    PumpOperationSettled {
        pump: PayloadRef,
        operation: TaskOperationId,
        result: crate::operation::OperationDispatchResult,
    },
}

#[derive(Clone, Debug)]
pub struct AttemptEffect {
    pub id: AttemptEffectId,
    pub class: EffectClass,
    pub body: EffectBody,
}
