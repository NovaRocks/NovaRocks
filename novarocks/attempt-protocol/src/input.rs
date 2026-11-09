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

//! Passive executor observations. Every timestamp is on the process timeline.

use crate::failure::{AttemptFailure, HostFailureRef};
use crate::operation::IngressRejection;
use crate::start::PayloadRef;
use novarocks_coordination_contract::{
    AttemptActionId, AttemptRequestToken, LogicalToAttempt, MonotonicInstant,
};
use novarocks_execution_contract::{
    CreateTaskReceipt, OperationKind, ParticipationAdmissionTicketReceipt,
    ParticipationConvergenceReceipt, ParticipationReceipt, ParticipationRef,
    QuiesceParticipationReceipt, ReleaseOutcome, TaskConvergenceReceipt, TaskIdentity,
    TaskOperationId, TaskStatus, UpdateTaskReceipt,
};
use std::num::NonZeroU64;
use std::time::Duration;

#[derive(Clone, Debug)]
pub enum AckPayload {
    None,
    AdmissionTicket(ParticipationAdmissionTicketReceipt),
    Create(Box<CreateTaskReceipt>),
    Update(UpdateTaskReceipt),
    Participation(ParticipationReceipt),
    Quiesce(QuiesceParticipationReceipt),
    Release {
        receipt: ParticipationReceipt,
        outcome: ReleaseOutcome,
        runtime_filter: Option<PayloadRef>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalDispatchResult {
    IngressRejected(IngressRejection),
    NonWorkerRejected,
    TransportUnknown,
}

#[derive(Clone, Debug)]
pub enum OperationReplyResult {
    Worker(novarocks_execution_contract::operation::OperationResult<AckPayload>),
    Local {
        result: LocalDispatchResult,
        failure: Option<crate::failure::HostProjectedFailure>,
        protocol_failure: Option<crate::failure::ProtocolFailure>,
        detail: Option<novarocks_execution_contract::SafeDetail>,
    },
}

#[derive(Clone, Debug)]
pub struct OperationReply {
    pub operation_id: TaskOperationId,
    pub kind: OperationKind,
    pub result: OperationReplyResult,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationFailureKind {
    RpcRejected,
    ProtocolViolation,
    ProcessMismatch,
    QueryMismatch,
    RecoverableTransport { observed_connection_loss: bool },
}

/// Classification comes from the original typed adapter error, never text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObservationFailure {
    pub kind: ObservationFailureKind,
    pub cause: crate::failure::HostProjectedFailure,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "accepted Task statuses remain by-value neutral snapshots"
)]
pub enum ObservationFact {
    Status(TaskStatus),
    Gone(TaskIdentity),
    ParticipationConvergence(ParticipationConvergenceReceipt),
    TaskConvergence(TaskConvergenceReceipt),
    Quiesce(QuiesceParticipationReceipt),
    StatusUnchanged(TaskIdentity),
    TaskConvergenceUnchanged(TaskIdentity),
    Unknown(TaskIdentity),
    CatchUpComplete {
        generation: u64,
        initial_cut: u64,
    },
    Bookmark {
        generation: u64,
        sequence: u64,
        covered_prefix: u64,
        source_cut: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "observation frames retain the original by-value neutral status representation"
)]
pub enum ObservationEvent {
    Fact {
        source_revision: Option<u64>,
        fact: ObservationFact,
    },
    Closed,
    Failed(ObservationFailure),
}

/// Immutable actual opening facts from this same original generation. Each
/// frame carries them because independent coalescing phase delivery may be
/// late. The core checks the opening budget before accepting entity, cursor,
/// coverage or liveness contributions; a later confirmation cannot undo a seal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObservationOpening {
    pub started_at: MonotonicInstant,
    /// None is valid for an opening failure, never for a stream entity frame.
    pub opened_at: Option<MonotonicInstant>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceivedObservation {
    pub participation: ParticipationRef,
    pub generation: NonZeroU64,
    pub received_at: MonotonicInstant,
    pub opening: ObservationOpening,
    pub event: ObservationEvent,
}

/// One consecutive part of the common ingress. The host cuts a batch before
/// every root control and serializes actual filling across mailbox classes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservationBatch(Box<[ReceivedObservation]>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationBatchError {
    Empty,
    TooMany,
}

impl ObservationBatch {
    /// Original TaskRound per-turn observation budget.
    pub const MAX_ITEMS: usize = 256;
    pub fn new(items: Vec<ReceivedObservation>) -> Result<Self, ObservationBatchError> {
        if items.is_empty() {
            return Err(ObservationBatchError::Empty);
        }
        if items.len() > Self::MAX_ITEMS {
            return Err(ObservationBatchError::TooMany);
        }
        Ok(Self(items.into_boxed_slice()))
    }
    pub fn items(&self) -> &[ReceivedObservation] {
        &self.0
    }
    pub fn into_items(self) -> Box<[ReceivedObservation]> {
        self.0
    }
}

/// Exact normalized union of read pauses, clipped to these snapshot bounds.
/// This passive measure avoids retaining an unbounded history of pause edges.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadPauseCoverage {
    pub from: MonotonicInstant,
    pub through: MonotonicInstant,
    pub paused: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReceptionCoverage {
    Complete,
    Incomplete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadStateSnapshot {
    pub request: AttemptRequestToken,
    pub participation: ParticipationRef,
    pub generation: NonZeroU64,
    pub snapshot_at: MonotonicInstant,
    /// Actual first open-future poll and successful open, from the same
    /// original Source lock as this read snapshot. Independent phase delivery
    /// may be late and cannot prove that the RPC is still opening.
    pub opening_started_at: Option<MonotonicInstant>,
    pub opened_at: Option<MonotonicInstant>,
    /// A current-generation first catch-up/new bookmark was actually received.
    /// Equal clock ticks cannot recover this fact from timestamps alone.
    pub had_liveness: bool,
    pub last_liveness_at: MonotonicInstant,
    pub pauses: ReadPauseCoverage,
    /// Actual Source read-credit suspension at this same snapshot. Accumulated
    /// pause duration alone cannot determine whether the stream is still paused.
    pub reading_paused: bool,
    /// Per-stream resume version from the same Source lock. It only rejects
    /// stale pause observations; it never orders frames or root controls.
    pub resume_revision: u64,
    pub closed: bool,
    pub received_through_snapshot: ReceptionCoverage,
}

/// Actual execution times, reported through one prepaid per-generation phase
/// source. Same-generation merging keeps the first start and successful open.
/// Late phase delivery cannot reopen a generation whose stream already closed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscriptionPhaseSnapshot {
    pub participation: ParticipationRef,
    pub generation: NonZeroU64,
    pub opening_started_at: MonotonicInstant,
    pub opened_at: Option<MonotonicInstant>,
}

/// A hint from the same Source after actual read-credit suspension ends.
/// It changes no liveness or coverage fact and grants no capacity. A controller
/// waiting on a paused snapshot may restore its original measurement deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadProgressSnapshot {
    pub participation: ParticipationRef,
    pub generation: NonZeroU64,
    pub resumed_at: MonotonicInstant,
    pub resume_revision: NonZeroU64,
}

/// One physical-send generation, identified by its original AdmitQueuedInput
/// effect. Exact replay retains the operation identity and creates a new
/// generation. This is neither a Scope nor a subscription generation, and
/// grants no capacity or right to reuse a prior WC input.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SendGeneration(AttemptActionId);
impl SendGeneration {
    pub const fn new(admission_effect: AttemptActionId) -> Self {
        Self(admission_effect)
    }
    pub const fn admission_effect(self) -> AttemptActionId {
        self.0
    }
}

/// Original Source record correlation for exactly one physical-send input.
/// The record and its generation travel together; neither creates a grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueuedInputGrantRef {
    record: NonZeroU64,
    generation: SendGeneration,
}
impl QueuedInputGrantRef {
    pub const fn new(record: NonZeroU64, generation: SendGeneration) -> Self {
        Self { record, generation }
    }
    pub const fn record(self) -> NonZeroU64 {
        self.record
    }
    pub const fn generation(self) -> SendGeneration {
        self.generation
    }
}

/// The original physical send record supplies this correlation around the
/// pure decoded reply. Receipt projection itself does not invent a generation.
#[derive(Clone, Debug)]
pub struct OperationObservation {
    pub generation: SendGeneration,
    pub reply: OperationReply,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WithdrawalOutcome {
    DefinitelyUnsent,
    TooLate,
}

/// Actual result of the SAME original publication owner. It carries no
/// completion/visibility or work-exit authority beyond that specific apply.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationApplied {
    RootStatus,
    ProtocolBatch { published: bool },
    FinalTaskInfo,
    Diagnostics,
    DevRootObservation,
}

#[derive(Clone, Debug)]
pub enum ExecutorFact {
    Operation(OperationObservation),
    /// The selected original publisher has actually returned. Match both the
    /// effect and its original publication kind before releasing the phase.
    /// Handoff, result filling and actual job exit cannot supply this result.
    PublicationApplied {
        effect: AttemptActionId,
        result: Result<PublicationApplied, AttemptFailure>,
    },
    /// The executor job won its start arbitration. This grants no Create gate.
    SendStarted {
        effect: AttemptActionId,
        started_at: MonotonicInstant,
    },
    /// Produced only when this exact Establish unary future is first polled.
    EstablishRpcStarted {
        effect: AttemptActionId,
        operation: TaskOperationId,
        participation: ParticipationRef,
        started_at: MonotonicInstant,
    },
    Withdrawn {
        effect: AttemptActionId,
        outcome: WithdrawalOutcome,
    },
    /// Positive result from the same original AdmitQueuedInput job, after
    /// unused exit. This carries no grant and settles only this generation.
    QueuedInputWithdrawn {
        operation: TaskOperationId,
        generation: SendGeneration,
    },
    ActualExit {
        effect: AttemptActionId,
    },
    QueuedInputGranted {
        operation: TaskOperationId,
        grant: QueuedInputGrantRef,
        queued_bytes: usize,
    },
    QueuedInputFailed {
        operation: TaskOperationId,
        generation: SendGeneration,
        failure: AttemptFailure,
    },
    SubscriptionProgress(SubscriptionPhaseSnapshot),
    /// Authored by the original open job's actual exit, independently of
    /// stream closure and CloseSubscription's effect completion.
    SubscriptionExited {
        effect: AttemptActionId,
        participation: ParticipationRef,
        generation: NonZeroU64,
    },
    Observations(ObservationBatch),
    ReadState(ReadStateSnapshot),
    ReadProgress(ReadProgressSnapshot),
    /// An exact read requested AFTER the same original QA permission (when
    /// required), resolved against the host's frozen endpoint. Membership
    /// change notifications alone cannot stand in for this correlated sample.
    ProcessSnapshot {
        request: novarocks_coordination_contract::AttemptRequestToken,
        participation: ParticipationRef,
        sampled_at: MonotonicInstant,
        observation: MembershipObservation,
    },
    /// The original pump's first business failure, visible while its close
    /// work may still retain captures. This is neither completion nor exit.
    PumpFailed {
        pump: PayloadRef,
        failure: AttemptFailure,
    },
    PumpFinished {
        pump: PayloadRef,
        result: Result<(), AttemptFailure>,
    },
    /// Original profile availability, separate from protocol completion and
    /// actual pump/capture exit. The Source retains any selected rich record.
    SplitProfile {
        pump: PayloadRef,
        observation: crate::publication::SplitDiagnosticObservation,
    },
    PumpTaskUpdate {
        pump: PayloadRef,
        delivery: SplitDeliveryRef,
        task: TaskIdentity,
        domains: Box<[crate::effect::TaskDomainIntent]>,
    },
    DynamicFiltersRead {
        request: AttemptRequestToken,
        task: TaskIdentity,
        result: DynamicFilterRead,
    },
    DynamicFilterFeedbackAdmitted {
        request: AttemptRequestToken,
        task: TaskIdentity,
        result: Result<(), AttemptFailure>,
    },
    DiagnosticsFinished {
        request: AttemptRequestToken,
        task: TaskIdentity,
        result: DiagnosticRead,
    },
}

/// Only positive exact replacement evidence can decide an existing attempt.
/// Missing heartbeat/admission membership is not process-replacement evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MembershipObservation {
    ExactOriginal,
    ExactReplacement(novarocks_types::BackendProcessId),
    Unobserved,
}

#[derive(Clone, Debug)]
pub struct MembershipView {
    pub revision: u64,
    pub processes: std::collections::BTreeMap<ParticipationRef, MembershipObservation>,
}

/// Passive identities copied from the original mailbox claim and source cut.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptSourceId(pub NonZeroU64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptInputId(pub NonZeroU64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptSourceCut {
    pub source: AttemptSourceId,
    /// None means this source was empty at the original cut.
    pub tail: Option<AttemptInputId>,
}

/// Original mailbox identities and pre-pop facts for this same claim.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptInputClaim {
    pub source: AttemptSourceId,
    pub input: AttemptInputId,
    pub native_cut: Option<AttemptSourceCut>,
    pub selected_cut: Option<AttemptSourceCut>,
}

#[derive(Clone, Debug)]
pub enum AttemptInput {
    /// The original paid first step installs the initial Native preference.
    Bootstrap,
    Executor(ExecutorFact),
    Port(LogicalToAttempt<AttemptFailure>),
    Membership(MembershipView),
    CapacityReleased,
    Timer,
}

/// Identity of the original canonical split-delivery record, without its sender.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SplitDeliveryRef(pub NonZeroU64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateAdmission {
    Preparing,
    Queued,
    DiscardedNoLongerNeeded,
    DiscardedTerminal,
}

#[derive(Clone, Debug)]
pub enum DynamicFilterRead {
    Empty,
    Available {
        version: novarocks_execution_contract::DomainVersion,
        feedback: PayloadRef,
    },
    Unavailable(HostFailureRef),
    Refused(AttemptFailure),
}

/// Reference to the original Source's retained final-info allocation. It is
/// scoped to this activation's actual read; copying it grants no access,
/// capacity, disposal authority, or additional result/exit leg.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FinalTaskInfoRef {
    request: AttemptRequestToken,
    task: TaskIdentity,
}
impl FinalTaskInfoRef {
    pub const fn new(request: AttemptRequestToken, task: TaskIdentity) -> Self {
        Self { request, task }
    }
    pub const fn request(self) -> AttemptRequestToken {
        self.request
    }
    pub const fn task(self) -> TaskIdentity {
        self.task
    }
}
/// Only the bounded validation snapshot crosses into the pure Core. Operator
/// statistics and their actual destructor remain in the original Source.
#[derive(Clone, Debug)]
pub struct FinalTaskInfoProjection {
    pub reference: FinalTaskInfoRef,
    pub final_status: TaskStatus,
}

/// Diagnostics remain observations. Read failure never becomes query failure.
/// The core closes its diagnostic snapshot before applying a reply at or after
/// the original deadline, while the host continues to retain the actual job.
#[derive(Clone, Debug)]
pub enum DiagnosticRead {
    Available(FinalTaskInfoProjection),
    Unavailable(novarocks_execution_contract::OperationOutcome),
    Failed(HostFailureRef),
}

impl OperationReply {
    pub const fn operation_id(&self) -> TaskOperationId {
        self.operation_id
    }
    pub const fn kind(&self) -> OperationKind {
        self.kind
    }
    pub fn worker_outcome(&self) -> Option<novarocks_execution_contract::OperationOutcome> {
        match &self.result {
            OperationReplyResult::Worker(result) => Some(result.outcome()),
            OperationReplyResult::Local { .. } => None,
        }
    }
    pub fn dispatch_result(&self) -> crate::operation::OperationDispatchResult {
        use crate::operation::{OperationDispatchResult as D, WorkerReceiptOutcome};
        match &self.result {
            OperationReplyResult::Worker(result) => {
                D::WorkerReceipt(WorkerReceiptOutcome::from_contract(result.outcome()))
            }
            OperationReplyResult::Local { result, .. } => match result {
                LocalDispatchResult::IngressRejected(reason) => D::IngressRejected(*reason),
                LocalDispatchResult::NonWorkerRejected => D::NonWorkerRejected,
                LocalDispatchResult::TransportUnknown => D::TransportUnknown,
            },
        }
    }
    pub fn payload(&self) -> &AckPayload {
        use novarocks_execution_contract::operation::OperationResult;
        match &self.result {
            OperationReplyResult::Worker(OperationResult::Verdict {
                acknowledgement: Some(payload),
                ..
            }) => payload,
            _ => &AckPayload::None,
        }
    }
    pub fn detail(&self) -> Option<&novarocks_execution_contract::SafeDetail> {
        match &self.result {
            OperationReplyResult::Worker(result) => result.detail(),
            OperationReplyResult::Local { detail, .. } => detail.as_ref(),
        }
    }
    pub fn worker_header(
        &self,
    ) -> Option<novarocks_execution_contract::operation::OperationResult<std::convert::Infallible>>
    {
        use novarocks_execution_contract::operation::OperationResult;
        match &self.result {
            OperationReplyResult::Worker(OperationResult::Verdict {
                outcome, detail, ..
            }) => Some(OperationResult::Verdict {
                outcome: *outcome,
                detail: detail.clone(),
                acknowledgement: None,
            }),
            OperationReplyResult::Worker(OperationResult::DomainFailure { outcome, failure }) => {
                Some(OperationResult::DomainFailure {
                    outcome: *outcome,
                    failure: failure.clone(),
                })
            }
            OperationReplyResult::Local { .. } => None,
        }
    }
    pub fn is_applied(&self) -> bool {
        use novarocks_execution_contract::OperationOutcome as O;
        matches!(self.worker_outcome(), Some(O::Accepted | O::Idempotent))
    }
    pub fn local_failure(&self) -> Option<&crate::failure::ProtocolFailure> {
        match &self.result {
            OperationReplyResult::Local {
                protocol_failure, ..
            } => protocol_failure.as_ref(),
            OperationReplyResult::Worker(_) => None,
        }
    }
    pub fn host_failure(&self) -> Option<&crate::failure::HostProjectedFailure> {
        match &self.result {
            OperationReplyResult::Local { failure, .. } => failure.as_ref(),
            OperationReplyResult::Worker(_) => None,
        }
    }
}
