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

//! Exact source, activation and action facts. Host resources never enter the core.

use crate::{
    LogicalActionId, LogicalRequestToken, LogicalSourceId,
    cancel::LogicalCancellationReason,
    start::{LogicalPlanIdentity, RootCarrier},
};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, ExecutionFailure, ExecutionFailureInput, LogicalExecutionActorId,
    PreReadyProcessReplacementWitness, QueryExecutionError, logical_port::AttemptToLogical,
};
use novarocks_execution_contract::{
    AcquireParticipationAdmissionTicket, ParticipationAdmissionTicketReceipt, ParticipationRef,
    TaskIdentity, root_result::RootResultEnd,
};
use novarocks_types::{BackendProcessId, QueryExecutionId};

/// Logical failure and the failed attempt's accepted original cause are distinct.
/// A successor preparation failure must not overwrite an earlier attempt cause.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailureReport {
    pub error: QueryExecutionError,
    pub original_attempt_failure: Option<Box<ExecutionFailureInput>>,
    pub pre_ready_replacement: Option<PreReadyProcessReplacementWitness>,
}

impl FailureReport {
    pub fn from_attempt(failure: &ExecutionFailure) -> Self {
        Self {
            error: failure.error().clone(),
            original_attempt_failure: Some(Box::new(failure.input().clone())),
            pre_ready_replacement: failure.pre_ready_replacement().cloned(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SuccessorTopology {
    LiveSnapshot,
    ExcludeProcess(BackendProcessId),
}

/// Identity of one qualification round, never a reservation capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QualificationIdentity {
    pub actor: LogicalExecutionActorId,
    pub failed: QueryExecutionId,
    pub replacement: QueryExecutionId,
    pub generation: u64,
}

/// Original immutable admission request and Worker grant, without its owner.
#[derive(Clone, Copy, Debug)]
pub struct QualifiedAdmission {
    pub request: AcquireParticipationAdmissionTicket,
    pub receipt: ParticipationAdmissionTicketReceipt,
}

#[derive(Clone, Debug)]
pub struct QualifiedReplacementFacts {
    pub identity: QualificationIdentity,
    pub operation_id: novarocks_execution_contract::TaskOperationId,
    pub topology_revision: u64,
    pub admissions: Box<[QualifiedAdmission]>,
}

#[derive(Clone, Debug)]
pub enum QualificationSettlement {
    Qualified(QualifiedReplacementFacts),
    Rejected(QueryExecutionError),
    OutcomeUnknown,
}

#[derive(Clone, Debug)]
pub enum NativeFact {
    AttemptActivated {
        activation: AttemptActivationIdentity,
        root: Option<TaskIdentity>,
    },
    /// The original run returned; protocol terminal decisions still come from AP.
    RunCompleted {
        activation: AttemptActivationIdentity,
    },
    SuccessorPrepared {
        execution: QueryExecutionId,
        participations: std::sync::Arc<[ParticipationRef]>,
    },
    QualificationSettled {
        identity: QualificationIdentity,
        operation_id: novarocks_execution_contract::TaskOperationId,
        /// The original handoff time, reported with this action's one result.
        /// Qualified/unknown outcomes require Some; an unissued rejection does not.
        dispatched_at: Option<novarocks_coordination_contract::MonotonicInstant>,
        settlement: QualificationSettlement,
    },
    ReservationActivated {
        identity: QualificationIdentity,
        operation_id: novarocks_execution_contract::TaskOperationId,
    },
    Failed {
        execution: QueryExecutionId,
        failure: FailureReport,
    },
    /// Independent recovery veto, not an attempt's ordinary terminal failure.
    NativeRunFailed {
        activation: AttemptActivationIdentity,
        failure: FailureReport,
    },
    DormantConverged {
        execution: QueryExecutionId,
    },
    Applied,
}

#[derive(Clone, Debug)]
pub enum RelayRequest {
    OpenDelivery {
        root: TaskIdentity,
        carrier: RootCarrier,
    },
    /// The caller already owns its original consumer queue position.
    RequestVisibility,
    DeliveryFailed(FailureReport),
    RootWithdrawn {
        root: TaskIdentity,
    },
    EndConsumed {
        root: TaskIdentity,
        /// Original wire sequence is nonzero; logical packets are sequence - 1.
        end: RootResultEnd,
        accepted_packets: u64,
        delivered_through: Option<u64>,
    },
    Stopped {
        all_deliveries_settled: bool,
    },
}

/// The original physical receipt remains in the consumer executor. This value
/// only correlates the registered source with that exact End.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InternalEndReceiptId(pub std::num::NonZeroU64);

#[derive(Clone, Debug)]
pub enum InternalRequest {
    RegisterSource {
        root: TaskIdentity,
        plan: LogicalPlanIdentity,
        supervised_exit: bool,
    },
    EndDecoded {
        receipt: InternalEndReceiptId,
        end: RootResultEnd,
    },
    AcceptReady {
        root: TaskIdentity,
        plan: LogicalPlanIdentity,
        receipt: InternalEndReceiptId,
        end: RootResultEnd,
    },
    Failed(FailureReport),
    /// Closing the source does not replace a reserved supervised exit verdict.
    SourceClosed,
    /// Authored by the original consumer supervisor after actual exit.
    ExitObserved {
        outcome: Option<Result<(), QueryExecutionError>>,
    },
}

#[derive(Clone, Debug)]
pub enum ConsumerFact {
    Applied,
    SchemaWritten,
    EndSlotReady,
    EndEnqueued,
    /// The original End receipt completed, failed or was dropped; actual job
    /// exit is still reported separately by the carrier.
    EndSettled,
    OutputExposed,
    ReadyHandedOff,
    DiagnosticsPublished,
    ConvergencePublished,
    Failed(QueryExecutionError),
}

#[derive(Clone, Debug)]
pub enum RelayFact {
    Applied,
    Stopped {
        activation: AttemptActivationIdentity,
        all_deliveries_settled: bool,
    },
    Failed {
        activation: AttemptActivationIdentity,
        failure: FailureReport,
        all_deliveries_settled: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObligationKind {
    RunningWork,
    RetiredAttempt,
}

#[derive(Clone, Debug)]
pub enum GovernanceFact {
    ObligationRegistered {
        activation: AttemptActivationIdentity,
        kind: ObligationKind,
    },
    ObligationResolved {
        activation: AttemptActivationIdentity,
        kind: ObligationKind,
    },
    RemoteTrackingEnded {
        activation: AttemptActivationIdentity,
        kind: ObligationKind,
    },
    StageReleased,
    CancellationRequested,
    WorkOwnerCompleted,
    Failed(QueryExecutionError),
}

#[derive(Clone, Debug)]
pub enum ActionResult {
    Native(NativeFact),
    Relay(RelayFact),
    Consumer(ConsumerFact),
    Governance(GovernanceFact),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistryClose {
    Retire,
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistryParticipationConvergence {
    NeverEstablished,
    WorkerStoppedAndParticipationFenced,
    WorkerProcessReplaced,
    RemoteTrackingEnded,
}

#[derive(Clone, Debug)]
pub enum LogicalInput {
    Started,
    AttemptPort(Box<AttemptToLogical>),
    ActionResult {
        action: LogicalActionId,
        result: ActionResult,
    },
    /// Authored only after the original work and captures actually exited.
    ActionExited {
        action: LogicalActionId,
    },
    Relay {
        source: LogicalSourceId,
        token: LogicalRequestToken,
        activation: AttemptActivationIdentity,
        request: RelayRequest,
    },
    Internal {
        source: LogicalSourceId,
        token: LogicalRequestToken,
        activation: AttemptActivationIdentity,
        request: InternalRequest,
    },
    RegistryParticipation {
        activation: AttemptActivationIdentity,
        participation: ParticipationRef,
        convergence: RegistryParticipationConvergence,
    },
    RegistryJoinReadiness {
        source: LogicalSourceId,
        token: LogicalRequestToken,
        close_ingress: bool,
    },
    Cancelled(LogicalCancellationReason),
    RegistryClose(RegistryClose),
    ConsumerClosed {
        source: LogicalSourceId,
    },
    Snapshot {
        source: LogicalSourceId,
        token: LogicalRequestToken,
    },
    Timer,
}
