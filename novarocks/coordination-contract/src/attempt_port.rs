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

//! Neutral messages between logical execution and one physical attempt.
//!
//! Tokens and grant references are correlation values, never authorization.
//! The issuing owner retains the actual permits and rich request allocations.
//! The bridge resolves references against that original owner; copying a message
//! cannot mint authority. Each request prepays exactly one reply position.
//! Failure type parameters carry passive attempt facts, never application owners.

use crate::{
    AttemptActivationIdentity, MonotonicInstant, ResultParticipationWithdrawal,
    SelectedTaskTermination,
};
use novarocks_execution_contract::root_result::RootResultEnd;
use novarocks_execution_contract::{
    AbortParticipation, AcquireParticipationAdmissionTicket, OperationOutcome,
    ParticipationAdmissionTicketReceipt, ParticipationRef, TaskIdentity, TaskOperationId,
    TaskStatus,
};
use std::num::NonZeroU64;

/// Process-local request identity in the original activation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AttemptRequestToken(NonZeroU64);
impl AttemptRequestToken {
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Identity of one fact or action. It does not identify or own executable work.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AttemptActionId(NonZeroU64);
impl AttemptActionId {
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Opaque reference to the bridge's original issued authorization.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct AttemptAuthorizationRef(NonZeroU64);
impl AttemptAuthorizationRef {
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Initialization-only evidence. Cleanup remains with the active isolation owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptInheritedAdmission {
    pub participation: ParticipationRef,
    pub receipt: ParticipationAdmissionTicketReceipt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptCompletionPolicy {
    AcceptedRootSuccessSeal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptLifecyclePolicy {
    pub completion: AttemptCompletionPolicy,
    pub retain_diagnostics: bool,
}

/// Passive projection of the original accepted computation cut. It does not
/// grant publication, cleanup, or a newly extended observation window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptComputationCut {
    pub activation: AttemptActivationIdentity,
    pub root: TaskIdentity,
    pub cut: MonotonicInstant,
    pub diagnostic_deadline: MonotonicInstant,
    pub convergence_deadline: MonotonicInstant,
    pub end: RootResultEnd,
}

/// The logical owner's frozen activation facts, separate from host plan projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttemptActivation {
    pub identity: AttemptActivationIdentity,
    pub policy: AttemptLifecyclePolicy,
    pub inherited_admissions: Box<[AttemptInheritedAdmission]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptStopReason {
    Client,
    Deadline,
    DrainDeadline,
    Shutdown,
    LogicalExecutionFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptConvergenceMode {
    Normal,
    Cleanup,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptParticipationConvergence {
    NeverEstablished,
    WorkerStoppedAndParticipationFenced,
    WorkerProcessReplaced,
}

/// Unknown means only that finite remote tracking ended, never actual Worker exit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttemptConvergenceClosure {
    Evidence(Box<[(ParticipationRef, AttemptParticipationConvergence)]>),
    RemoteTrackingEnded {
        exact_participations: Box<[ParticipationRef]>,
    },
}

/// Exact classification accepted alongside the root status in the same serial step.
#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "the accepted root projection retains the original selected Task termination by value"
)]
pub enum AttemptRootFailure {
    None,
    DerivedPending,
    Authoritative(SelectedTaskTermination),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttemptRootProjection {
    pub status: TaskStatus,
    pub failure: AttemptRootFailure,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttemptTerminal<F> {
    Completed,
    Failed(F),
    Stopped(AttemptStopReason),
}

#[derive(Clone, Copy, Debug)]
pub enum AttemptAuthorization {
    BeginAdmission(AcquireParticipationAdmissionTicket),
    SettleAdmission {
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
    },
    ReauthorizeEstablish {
        participation: ParticipationRef,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptAuthorizationDecision {
    AdmissionStarted(AttemptAuthorizationRef),
    AdmissionGranted,
    AdmissionRetryable,
    EstablishAuthorized(AttemptAuthorizationRef),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptIssueSettlement {
    TransportOwned,
    DefinitelyUnsent,
    TransportUnknown,
    PreWorkerRejected,
    Worker(OperationOutcome),
}

/// Each envelope is checked against the registered activation before application.
#[derive(Clone, Debug)]
pub struct LogicalToAttempt<F> {
    pub activation: AttemptActivationIdentity,
    pub body: LogicalToAttemptBody<F>,
}

#[derive(Clone, Debug)]
pub enum LogicalToAttemptBody<F> {
    AuthorizationReply {
        token: AttemptRequestToken,
        decision: Result<AttemptAuthorizationDecision, F>,
    },
    PreReadyReplacementReply {
        token: AttemptRequestToken,
        may_observe: bool,
    },
    Abort {
        action: AttemptActionId,
        request: AbortParticipation,
    },
    Stop(AttemptStopReason),
    /// Must share the observation source FIFO, including across mailbox classes.
    SealRequest {
        token: AttemptRequestToken,
    },
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
pub struct AttemptToLogical<F> {
    pub activation: AttemptActivationIdentity,
    pub action: AttemptActionId,
    pub body: AttemptToLogicalBody<F>,
}

#[derive(Clone, Debug)]
pub enum AttemptToLogicalBody<F> {
    AuthorizationRequest {
        token: AttemptRequestToken,
        request: AttemptAuthorization,
    },
    PreReadyReplacementQuery {
        token: AttemptRequestToken,
        refusal: F,
    },
    EstablishSettlement {
        participation: ParticipationRef,
        operation_id: TaskOperationId,
        settlement: AttemptIssueSettlement,
    },
    AbortSettlement {
        participation: ParticipationRef,
        operation_id: TaskOperationId,
        settlement: AttemptIssueSettlement,
    },
    RootStatus(Box<AttemptRootProjection>),
    SealResult {
        token: AttemptRequestToken,
        result: Result<std::sync::Arc<[ResultParticipationWithdrawal]>, F>,
    },
    TerminalControlResult {
        token: AttemptRequestToken,
        result: Result<(), F>,
    },
    AttemptTerminal(AttemptTerminal<F>),
    /// The failed core is never read to reconstruct this host-authored fact.
    AttemptLost {
        failure: F,
        unknown_participations: Box<[ParticipationRef]>,
        unresolved_actions: Box<[AttemptActionId]>,
    },
    ConvergenceProgress {
        local_exited: bool,
        closure: Option<AttemptConvergenceClosure>,
    },
}
