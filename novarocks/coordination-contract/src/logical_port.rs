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

//! Passive messages exchanged by the logical execution and attempt cores.
//!
//! Request tokens correlate exactly one prepaid reply. Failure values are
//! concrete passive facts; host object references remain with their owner.

use crate::{
    AttemptActionId, AttemptActivationIdentity, AttemptAuthorizationDecision,
    AttemptAuthorizationRef, AttemptComputationCut, AttemptConvergenceClosure,
    AttemptConvergenceMode, AttemptIssueSettlement, AttemptRequestToken, AttemptRootProjection,
    AttemptStopReason, ExecutionFailure, MonotonicInstant, ResultParticipationWithdrawal,
    RuntimeFilterDiagnostic,
};
use novarocks_execution_contract::{
    AbortParticipation, AcquireParticipationAdmissionTicket, AdmissionTicketId,
    EstablishSemanticIdentity, OperationOutcome, ParticipationAdmissionTicketReceipt,
    ParticipationRef, ParticipationState, TaskIdentity, TaskOperationId,
};
use novarocks_types::NativeCompatibilityId;

/// Only Worker states proving admission closure can cross this settlement port.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbortWorkerClosure {
    Aborting,
    Releasing,
    TerminalRetained,
    Gone,
}

impl TryFrom<ParticipationState> for AbortWorkerClosure {
    type Error = ParticipationState;
    fn try_from(value: ParticipationState) -> Result<Self, Self::Error> {
        match value {
            ParticipationState::Aborting => Ok(Self::Aborting),
            ParticipationState::Releasing => Ok(Self::Releasing),
            ParticipationState::TerminalRetained => Ok(Self::TerminalRetained),
            ParticipationState::Gone => Ok(Self::Gone),
            other => Err(other),
        }
    }
}

/// The Worker closure is the passive fact selected from the original receipt.
/// Physical send generations remain private to the attempt's host.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbortIssueSettlement {
    DefinitelyUnsent,
    TransportUnknown,
    Worker {
        outcome: OperationOutcome,
        closure: AbortWorkerClosure,
    },
    FailedClosed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttemptTerminal {
    Completed,
    Failed(ExecutionFailure),
    Stopped {
        reason: AttemptStopReason,
        /// Original typed cancellation/logical failure; never reconstructed
        /// from the coarse control reason or its display text.
        failure: ExecutionFailure,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum AttemptAuthorization {
    BeginAdmission(AcquireParticipationAdmissionTicket),
    SettleAdmission {
        /// Correlate the same core-issued admission identity.
        issue: AttemptAuthorizationRef,
        participation: ParticipationRef,
        operation_id: TaskOperationId,
        outcome: Option<OperationOutcome>,
        receipt: Option<ParticipationAdmissionTicketReceipt>,
        prior_transport_unknown: bool,
    },
    /// The original request allocation is held by the host under this exact identity.
    Establish {
        participation: ParticipationRef,
        operation_id: TaskOperationId,
        ticket_id: AdmissionTicketId,
        semantics: EstablishSemanticIdentity,
        native_compatibility_id: NativeCompatibilityId,
    },
    ReauthorizeEstablish {
        participation: ParticipationRef,
        /// Expected original request, checked against the returned submission.
        operation_id: TaskOperationId,
    },
}

/// Independent typed ledger verdict. The attempt gate retains its own
/// exact-operation transport history when interpreting budget exhaustion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttemptAuthorizationFailure {
    Rejected(ExecutionFailure),
    /// Only QA's actual EstablishIssueError::AuthorizationBudgetExhausted.
    /// The pure gate still decides whether this same operation was unknown.
    EstablishBudgetExhausted(ExecutionFailure),
}

/// Each envelope is checked against the registered activation before application.
#[derive(Clone, Debug)]
pub struct LogicalToAttempt {
    pub activation: AttemptActivationIdentity,
    pub body: LogicalToAttemptBody,
}

#[derive(Clone, Debug)]
pub enum LogicalToAttemptBody {
    AuthorizationReply {
        token: AttemptRequestToken,
        decision: Result<AttemptAuthorizationDecision, AttemptAuthorizationFailure>,
    },
    /// The logical core applied or rejected the exact ledger settlement.
    /// Host handoff and actual exit cannot replace this reply.
    EstablishSettlementReply {
        token: AttemptRequestToken,
        authorization: AttemptAuthorizationRef,
        result: Result<(), ExecutionFailure>,
    },
    PreReadyReplacementReply {
        token: AttemptRequestToken,
        may_observe: bool,
    },
    /// Hint from the SAME original QA observation registered with this query.
    /// It never grants permission: ask the original policy owner again before
    /// continuing observation. Query/register itself must not emit this hint.
    PreReadyReplacementChanged { token: AttemptRequestToken },
    Abort {
        action: AttemptActionId,
        request: AbortParticipation,
    },
    Stop {
        reason: AttemptStopReason,
        failure: ExecutionFailure,
    },
    /// Must share the observation source FIFO, including across mailbox classes.
    SealRequest { token: AttemptRequestToken },
    TerminalControlDemand {
        token: AttemptRequestToken,
        root: TaskIdentity,
    },
    BeginConvergence {
        mode: AttemptConvergenceMode,
        remote_deadline: MonotonicInstant,
        computation_ready: Option<AttemptComputationCut>,
        retain_diagnostics: bool,
    },
}

#[derive(Clone, Debug)]
pub struct AttemptToLogical {
    pub activation: AttemptActivationIdentity,
    pub action: AttemptActionId,
    pub body: AttemptToLogicalBody,
}

#[derive(Clone, Debug)]
pub enum AttemptToLogicalBody {
    AuthorizationRequest {
        token: AttemptRequestToken,
        request: AttemptAuthorization,
    },
    PreReadyReplacementQuery {
        token: AttemptRequestToken,
        refusal: ExecutionFailure,
    },
    EstablishSettlement {
        token: AttemptRequestToken,
        /// AP selects current/oldest-late/all-late order, not the host lookup.
        authorization: AttemptAuthorizationRef,
        participation: ParticipationRef,
        operation_id: TaskOperationId,
        settlement: AttemptIssueSettlement,
    },
    AbortSettlement {
        /// The logical core's exact outstanding Abort action.
        abort_action: AttemptActionId,
        participation: ParticipationRef,
        operation_id: TaskOperationId,
        settlement: AbortIssueSettlement,
    },
    RootStatus(Box<AttemptRootProjection>),
    SealResult {
        token: AttemptRequestToken,
        result: Result<std::sync::Arc<[ResultParticipationWithdrawal]>, ExecutionFailure>,
    },
    TerminalControlResult {
        token: AttemptRequestToken,
        result: Result<(), ExecutionFailure>,
    },
    AttemptTerminal(AttemptTerminal),
    /// The failed core is never read to reconstruct this host-authored fact.
    AttemptLost {
        failure: ExecutionFailure,
        unknown_participations: Box<[ParticipationRef]>,
        unresolved_actions: Box<[AttemptActionId]>,
    },
    ConvergenceProgress {
        local_exited: bool,
        closure: Option<std::sync::Arc<AttemptConvergenceClosure>>,
        lifecycle_runtime_filters: Option<std::sync::Arc<RuntimeFilterDiagnostic>>,
    },
}
