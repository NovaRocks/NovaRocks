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
        failure: Option<HostFailureRef>,
    },
}

#[derive(Clone, Debug)]
pub struct OperationReply {
    pub operation_id: TaskOperationId,
    pub kind: OperationKind,
    pub result: OperationReplyResult,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationFailure {
    Transport(&'static str),
    ConnectionLost,
    IdentityViolation(&'static str),
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReceivedObservation {
    pub participation: ParticipationRef,
    pub generation: NonZeroU64,
    pub received_at: MonotonicInstant,
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
    pub last_liveness_at: MonotonicInstant,
    pub pauses: ReadPauseCoverage,
    pub closed: bool,
    pub received_through_snapshot: ReceptionCoverage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueuedInputGrantRef(pub NonZeroU64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WithdrawalOutcome {
    DefinitelyUnsent,
    TooLate,
}

#[derive(Clone, Debug)]
pub enum ExecutorFact {
    Operation(OperationReply),
    SendStarted {
        effect: AttemptActionId,
        started_at: MonotonicInstant,
    },
    Withdrawn {
        effect: AttemptActionId,
        outcome: WithdrawalOutcome,
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
        failure: AttemptFailure,
    },
    SubscriptionOpened {
        participation: ParticipationRef,
        generation: NonZeroU64,
        opened_at: MonotonicInstant,
    },
    Observations(ObservationBatch),
    ReadState(ReadStateSnapshot),
    PumpFinished {
        pump: PayloadRef,
        result: Result<(), AttemptFailure>,
    },
    PumpTaskUpdate {
        pump: PayloadRef,
        task: TaskIdentity,
        domains: Box<[crate::effect::TaskDomainIntent]>,
    },
    DiagnosticsFinished {
        result: Result<(), AttemptFailure>,
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

#[derive(Clone, Debug)]
pub enum AttemptInput {
    Executor(ExecutorFact),
    Port(LogicalToAttempt<AttemptFailure>),
    Membership(MembershipView),
    CapacityReleased,
    Timer,
}
