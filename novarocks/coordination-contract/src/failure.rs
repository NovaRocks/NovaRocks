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
//! Passive execution failures shared by the logical execution and attempt owners.

use crate::AttemptFailureClass;
use novarocks_execution_contract::{
    ParticipationRef, SafeDetail, TaskFailurePhase, TaskIdentity, TaskOperationId,
    failure::{FailureClass, FailureFact, FailureProjection, FailureReason, domain},
    operation::OperationKind,
};
use novarocks_types::BackendProcessId;
use std::{error::Error, fmt, num::NonZeroU16, sync::Arc};

/// Shared identity-refusal reason authored by the FE Task protocol projector.
/// It describes identity-refusal semantics, not remote provenance, a specific
/// identity field, pre-ready state, or independent process replacement.
pub const TASK_PROTOCOL_IDENTITY_MISMATCH_REASON: FailureReason =
    FailureReason::new(domain::QUERY, NonZeroU16::new(15).unwrap());

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionFailureSite {
    Task {
        identity: TaskIdentity,
        phase: TaskFailurePhase,
    },
    /// A control termination reports no fabricated preparation/execution phase.
    TaskControl {
        identity: TaskIdentity,
    },
    Operation {
        participation: ParticipationRef,
        operation_id: TaskOperationId,
        kind: OperationKind,
    },
    RootRead {
        root: TaskIdentity,
    },
    Backend {
        process: BackendProcessId,
    },
    /// A local coordinator failure with no Task/operation acceptance claim.
    Coordinator,
}

/// Facts and the bounded safe rendering cross the boundary together exactly
/// once. No caller can replace only the class or reconstruct facts from text.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionFailureInput {
    fact: FailureFact,
    detail: SafeDetail,
    site: ExecutionFailureSite,
}
impl ExecutionFailureInput {
    pub const fn new(fact: FailureFact, detail: SafeDetail, site: ExecutionFailureSite) -> Self {
        Self { fact, detail, site }
    }
    pub fn from_selected_task(selected: &crate::SelectedTaskTermination) -> Option<Self> {
        use novarocks_execution_contract::{AbortCause, TerminationDetail};
        let identity = selected.identity();
        let (fact, detail, site) = match selected.detail() {
            TerminationDetail::Failed(failure) => (
                failure.fact(),
                failure.detail().clone(),
                ExecutionFailureSite::Task {
                    identity,
                    phase: failure.phase(),
                },
            ),
            TerminationDetail::Aborted(cause) => {
                let reason = match cause {
                    AbortCause::PeerTaskFailed => return None,
                    AbortCause::LeaseExpired => QueryFailureReason::TaskLeaseExpired,
                    AbortCause::QueryFailed => QueryFailureReason::TaskAborted,
                };
                (
                    reason.failure_fact(),
                    SafeDetail::truncating(cause.as_str()),
                    ExecutionFailureSite::TaskControl { identity },
                )
            }
            TerminationDetail::Canceled(reason) => (
                QueryFailureReason::TaskCancelled.failure_fact(),
                SafeDetail::truncating(reason.as_str()),
                ExecutionFailureSite::TaskControl { identity },
            ),
        };
        Some(Self::new(fact, detail, site))
    }

    pub const fn fact(&self) -> FailureFact {
        self.fact
    }
    pub const fn detail(&self) -> &SafeDetail {
        &self.detail
    }
    pub const fn site(&self) -> ExecutionFailureSite {
        self.site
    }
    pub fn attempt_class(&self) -> AttemptFailureClass {
        if self.fact.class() == FailureClass::Dependency
            && self.fact.evidence()
                == novarocks_execution_contract::failure::FailureEvidence::Originating
            && self.fact.reason() == QueryFailureReason::BackendLost.failure_fact().reason()
            && matches!(self.site, ExecutionFailureSite::Backend { .. })
        {
            return AttemptFailureClass::RecoverableInfrastructure;
        }
        if self.fact.evidence()
            == novarocks_execution_contract::failure::FailureEvidence::Originating
            && matches!(self.site, ExecutionFailureSite::Coordinator)
            && self.fact.class() == QueryFailureReason::Cancelled.failure_fact().class()
            && self.fact.reason() == QueryFailureReason::Cancelled.failure_fact().reason()
        {
            return AttemptFailureClass::Cancelled;
        }
        if self.fact.evidence()
            == novarocks_execution_contract::failure::FailureEvidence::Originating
            && matches!(self.site, ExecutionFailureSite::Coordinator)
            && self.fact.class() == QueryFailureReason::DeadlineExceeded.failure_fact().class()
            && self.fact.reason() == QueryFailureReason::DeadlineExceeded.failure_fact().reason()
        {
            return AttemptFailureClass::DeadlineExceeded;
        }
        if self.fact.evidence()
            == novarocks_execution_contract::failure::FailureEvidence::Originating
            && matches!(self.site, ExecutionFailureSite::Coordinator)
            && self.fact.class() == QueryFailureReason::CommitUnknown.failure_fact().class()
            && self.fact.reason() == QueryFailureReason::CommitUnknown.failure_fact().reason()
        {
            return AttemptFailureClass::CommitUnknown;
        }
        match self.fact.class() {
            FailureClass::Unsupported => AttemptFailureClass::Unsupported,
            FailureClass::Resource => AttemptFailureClass::ResourceGovernance,
            FailureClass::Invariant => AttemptFailureClass::ContractViolation,
            FailureClass::Unexpected
            | FailureClass::Data
            | FailureClass::Dependency
            | FailureClass::Representation
            | FailureClass::Control => AttemptFailureClass::ExecutionFailure,
        }
    }
    pub fn error_kind(&self) -> QueryExecutionErrorKind {
        match self.attempt_class() {
            AttemptFailureClass::Cancelled => QueryExecutionErrorKind::Cancelled,
            AttemptFailureClass::DeadlineExceeded => QueryExecutionErrorKind::DeadlineExceeded,
            AttemptFailureClass::ResourceGovernance => QueryExecutionErrorKind::Rejected,
            _ => QueryExecutionErrorKind::Failed,
        }
    }
}
/// Reasons produced by query coordination itself. Domain errors arriving from
/// Task, operation or root owners retain their original fact without wrapping.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryFailureReason {
    Cancelled,
    DeadlineExceeded,
    /// An actual failed backend connection/process observation. Never a local
    /// read deadline, observation budget, Preparing or plain remote Unavailable.
    BackendLost,
    TransportDeadline,
    TransportRefused,
    TransportCapacity,
    ResultProtocol,
    ActorProtocol,
    ObservationLost,
    PreparationProtocol,
    CommitUnknown,
    TaskAborted,
    TaskLeaseExpired,
    TaskCancelled,
    NativeFuturePanicked,
    InternalConsumerPanicked,
    SplitAssignmentFailed,
    PreparationFailed,
    RecoveryPreparationBudgetExceeded,
    LogicalFailureUnspecified,
    LogicalOwnerStopped,
    BlockingWorkerPanicked,
    BlockingExecutorClosed,
    BlockingSourceContract,
    BlockingQueueFull,
}
impl FailureProjection for QueryFailureReason {
    fn failure_fact(&self) -> FailureFact {
        let (class, code) = match self {
            Self::Cancelled => (FailureClass::Control, 1),
            Self::DeadlineExceeded => (FailureClass::Control, 2),
            Self::BackendLost => (FailureClass::Dependency, 3),
            Self::TransportDeadline => (FailureClass::Dependency, 4),
            Self::TransportRefused => (FailureClass::Dependency, 5),
            Self::TransportCapacity => (FailureClass::Resource, 13),
            Self::ResultProtocol => (FailureClass::Invariant, 6),
            Self::ActorProtocol => (FailureClass::Invariant, 7),
            Self::ObservationLost => (FailureClass::Dependency, 8),
            Self::PreparationProtocol => (FailureClass::Invariant, 9),
            Self::CommitUnknown => (FailureClass::Unexpected, 10),
            Self::TaskAborted => (FailureClass::Control, 11),
            Self::TaskLeaseExpired => (FailureClass::Control, 12),
            Self::TaskCancelled => (FailureClass::Control, 27),
            Self::NativeFuturePanicked => (FailureClass::Unexpected, 33),
            Self::InternalConsumerPanicked => (FailureClass::Unexpected, 175),
            Self::SplitAssignmentFailed => (FailureClass::Unexpected, 176),
            Self::PreparationFailed => (FailureClass::Unexpected, 34),
            Self::RecoveryPreparationBudgetExceeded => (FailureClass::Dependency, 151),
            Self::LogicalFailureUnspecified => (FailureClass::Unexpected, 172),
            Self::LogicalOwnerStopped => (FailureClass::Control, 174),
            Self::BlockingWorkerPanicked => (FailureClass::Unexpected, 177),
            Self::BlockingExecutorClosed => (FailureClass::Dependency, 178),
            Self::BlockingSourceContract => (FailureClass::Invariant, 179),
            Self::BlockingQueueFull => (FailureClass::Resource, 180),
        };
        let reason = FailureReason::new(domain::QUERY, NonZeroU16::new(code).unwrap());
        if matches!(self, Self::LogicalOwnerStopped) {
            FailureFact::stop_observation(reason)
        } else {
            FailureFact::new(class, reason)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryExecutionErrorKind {
    InvalidRequest,
    Rejected,
    Cancelled,
    DeadlineExceeded,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum QueryExecutionErrorBody {
    Application {
        kind: QueryExecutionErrorKind,
        message: Arc<str>,
    },
    Execution(Box<crate::failure::ExecutionFailureInput>),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryExecutionError {
    body: QueryExecutionErrorBody,
}
impl QueryExecutionError {
    /// Application/control errors with no domain execution conclusion.
    pub fn new(kind: QueryExecutionErrorKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            body: QueryExecutionErrorBody::Application {
                kind,
                message: message.into(),
            },
        }
    }
    pub fn from_execution(input: crate::failure::ExecutionFailureInput) -> Self {
        Self {
            body: QueryExecutionErrorBody::Execution(Box::new(input)),
        }
    }
    pub fn execution_failure(&self) -> Option<&crate::failure::ExecutionFailureInput> {
        match &self.body {
            QueryExecutionErrorBody::Execution(input) => Some(input.as_ref()),
            QueryExecutionErrorBody::Application { .. } => None,
        }
    }
    pub fn kind(&self) -> QueryExecutionErrorKind {
        match &self.body {
            QueryExecutionErrorBody::Application { kind, .. } => *kind,
            QueryExecutionErrorBody::Execution(input) => input.error_kind(),
        }
    }
    pub fn message(&self) -> &str {
        match &self.body {
            QueryExecutionErrorBody::Application { message, .. } => message,
            QueryExecutionErrorBody::Execution(input) => input.detail().as_str(),
        }
    }
}
impl fmt::Display for QueryExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message())
    }
}

impl Error for QueryExecutionError {}

/// Independent exact process-replacement evidence retained beside an original
/// pre-ready Establish or Create refusal. Native must have correlated a real
/// Worker IdentityMismatch with its original operation and Task/participation; QA
/// retains the parent Establish and current recovery policy. The producer must
/// also observe a different process at its frozen endpoint within the ORIGINAL
/// observation horizon. This value does not claim
/// that the wire identified a particular IdentityField, grant retry, or prove exit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreReadyProcessReplacementWitness {
    // Rare, immutable diagnostic facts share one allocation across the original
    // failure carriers. This holds no permit, source, or resource authority.
    facts: Arc<PreReadyProcessReplacementFacts>,
}

#[derive(Debug, Eq, PartialEq)]
struct PreReadyProcessReplacementFacts {
    participation: novarocks_execution_contract::ParticipationRef,
    operation_id: novarocks_execution_contract::TaskOperationId,
    kind: novarocks_execution_contract::OperationKind,
    replacement: BackendProcessId,
    refusal_fact: novarocks_execution_contract::failure::FailureFact,
}
impl PreReadyProcessReplacementWitness {
    pub fn try_new(
        input: &crate::failure::ExecutionFailureInput,
        participation: novarocks_execution_contract::ParticipationRef,
        operation_id: novarocks_execution_contract::TaskOperationId,
        replacement: BackendProcessId,
    ) -> Result<Self, &'static str> {
        let Some((original_participation, original_operation, kind)) =
            Self::refusal_operation(input)
        else {
            return Err("pre-ready replacement evidence has no exact original refusal");
        };
        if original_participation != participation
            || original_operation != operation_id
            || replacement == participation.backend_process_id()
        {
            return Err("pre-ready replacement evidence does not match its original refusal");
        }
        Ok(Self {
            facts: Arc::new(PreReadyProcessReplacementFacts {
                participation,
                operation_id,
                kind,
                replacement,
                refusal_fact: input.fact(),
            }),
        })
    }
    pub fn refusal_operation(
        input: &crate::failure::ExecutionFailureInput,
    ) -> Option<(
        ParticipationRef,
        novarocks_execution_contract::TaskOperationId,
        novarocks_execution_contract::OperationKind,
    )> {
        use crate::failure::{ExecutionFailureSite, TASK_PROTOCOL_IDENTITY_MISMATCH_REASON};
        use novarocks_execution_contract::OperationKind;
        use novarocks_execution_contract::failure::{FailureClass, FailureEvidence};
        if input.fact().class() != FailureClass::Invariant
            || input.fact().evidence() != FailureEvidence::Originating
            || input.fact().reason() != TASK_PROTOCOL_IDENTITY_MISMATCH_REASON
        {
            return None;
        }
        match input.site() {
            ExecutionFailureSite::Operation {
                participation,
                operation_id,
                kind: kind @ (OperationKind::UpdateParticipation | OperationKind::CreateTask),
            } => Some((participation, operation_id, kind)),
            _ => None,
        }
    }
    pub fn participation(&self) -> novarocks_execution_contract::ParticipationRef {
        self.facts.participation
    }
    pub fn operation_id(&self) -> novarocks_execution_contract::TaskOperationId {
        self.facts.operation_id
    }
    pub fn replacement_process(&self) -> BackendProcessId {
        self.facts.replacement
    }
    pub fn matches_input(&self, input: &crate::failure::ExecutionFailureInput) -> bool {
        input.fact() == self.facts.refusal_fact
            && input.site()
                == (crate::failure::ExecutionFailureSite::Operation {
                    participation: self.facts.participation,
                    operation_id: self.facts.operation_id,
                    kind: self.facts.kind,
                })
    }
}

/// Runtime failure while preparing one physical attempt.
///
/// Coordination consumes the attempt failure class for recovery policy and
/// retains the query error for the caller-visible terminal result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionFailure {
    error: QueryExecutionError,
    pre_ready_replacement: Option<PreReadyProcessReplacementWitness>,
}
impl ExecutionFailure {
    pub fn from_input(input: crate::failure::ExecutionFailureInput) -> Self {
        Self {
            error: QueryExecutionError::from_execution(input),
            pre_ready_replacement: None,
        }
    }
    pub fn input(&self) -> &crate::failure::ExecutionFailureInput {
        self.error
            .execution_failure()
            .expect("typed constructor retains its original failure input")
    }
    pub fn attach_pre_ready_replacement(
        &mut self,
        witness: PreReadyProcessReplacementWitness,
    ) -> Result<(), &'static str> {
        if self.pre_ready_replacement.is_some() || !witness.matches_input(self.input()) {
            return Err("pre-ready replacement witness belongs to another original failure");
        }
        self.pre_ready_replacement = Some(witness);
        Ok(())
    }
    pub const fn pre_ready_replacement(&self) -> Option<&PreReadyProcessReplacementWitness> {
        self.pre_ready_replacement.as_ref()
    }
    pub fn class(&self) -> AttemptFailureClass {
        self.input().attempt_class()
    }
    pub const fn error(&self) -> &QueryExecutionError {
        &self.error
    }
}

impl fmt::Display for ExecutionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for ExecutionFailure {}
