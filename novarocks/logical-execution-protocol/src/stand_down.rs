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

//! Pure participation cleanup. Worker admission closure, physical resource
//! evidence and finite observation retirement remain separate facts.

use crate::{
    StepBudget, establish::EstablishStandDownFact, input::RegistryParticipationConvergence,
};
use novarocks_coordination_contract::{
    ABORT_RETRY_BACKOFF, AttemptActionId, AttemptActivationIdentity, MonotonicInstant,
    OperationIds,
    logical_port::{AbortIssueSettlement, AbortWorkerClosure},
};
use novarocks_execution_contract::{
    AbortCause, AbortParticipation, OperationOutcome, ParticipationRef, TaskOperationId,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::NonZeroUsize,
    ops::Bound::{Excluded, Unbounded},
    sync::Arc,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ParticipationStandDownCause {
    LogicalExecutionCancelled,
    LogicalExecutionFailed,
}

impl ParticipationStandDownCause {
    const fn abort_cause(self) -> AbortCause {
        match self {
            Self::LogicalExecutionCancelled | Self::LogicalExecutionFailed => {
                AbortCause::QueryFailed
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StandDownMode {
    SuccessfulCleanup,
    Abort(ParticipationStandDownCause),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParticipationClosureState {
    NoRemoteResponsibility,
    AbortPending,
    AbortIssueExhausted,
    Aborting,
    Releasing,
    TerminalRetained,
    Gone,
    WorkerStoppedAndParticipationFenced,
    WorkerProcessReplaced,
    RemoteTrackingEnded,
}

impl ParticipationClosureState {
    pub const fn stand_down_responsibility_settled(self) -> bool {
        !matches!(self, Self::AbortPending | Self::AbortIssueExhausted)
    }

    pub const fn residual_resource_settled(self) -> bool {
        matches!(
            self,
            Self::NoRemoteResponsibility
                | Self::WorkerStoppedAndParticipationFenced
                | Self::WorkerProcessReplaced
        )
    }

    const fn tracking_complete(self) -> bool {
        self.residual_resource_settled() || matches!(self, Self::RemoteTrackingEnded)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbortParticipationIssueState {
    AwaitingSettlement,
    DefinitelyUnsent,
    TransportUnknown,
    WorkerSettled,
    FailedClosed,
    AuthorizationExhausted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AbortParticipationIssueIdentity {
    pub activation: AttemptActivationIdentity,
    pub operation_id: TaskOperationId,
    pub participation: ParticipationRef,
    pub cause: ParticipationStandDownCause,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ParticipationStandDownSnapshot {
    pub closure: ParticipationClosureState,
    pub identity: Option<AbortParticipationIssueIdentity>,
    pub issue_state: Option<AbortParticipationIssueState>,
    pub authorizations: usize,
    pub current_action: Option<AttemptActionId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ParticipationStandDownError {
    DuplicateParticipation,
    ActivationExecutionMismatch,
    UnknownParticipation,
    StandDownAlreadyStarted,
    StandDownNotStarted,
    InitializationIncomplete,
    WrongState,
    IdentityMismatch,
    UnknownAction,
    ConflictingWorkerSettlement,
    InvalidWorkerSettlement,
    ClosureRegression,
}

impl fmt::Display for ParticipationStandDownError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::DuplicateParticipation => "stand-down contains a duplicate participation",
            Self::ActivationExecutionMismatch => {
                "stand-down activation and participation name different executions"
            }
            Self::UnknownParticipation => "stand-down names an unknown participation",
            Self::StandDownAlreadyStarted => "participation stand-down was already started",
            Self::StandDownNotStarted => "participation stand-down has not started",
            Self::InitializationIncomplete => {
                "stand-down requires every original participation fact"
            }
            Self::WrongState => "stand-down action is illegal in the current state",
            Self::IdentityMismatch => {
                "stand-down settlement does not match its original Abort issue"
            }
            Self::UnknownAction => "stand-down settlement names an unissued Abort action",
            Self::ConflictingWorkerSettlement => {
                "Abort issue received conflicting Worker closure branches"
            }
            Self::InvalidWorkerSettlement => {
                "Abort Worker outcome does not prove participation closure"
            }
            Self::ClosureRegression => "participation closure observation regressed",
        })
    }
}

impl std::error::Error for ParticipationStandDownError {}

impl novarocks_execution_contract::failure::FailureProjection for ParticipationStandDownError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureFact, FailureReason, domain,
        };
        let (class, code) = match self {
            Self::DuplicateParticipation
            | Self::ActivationExecutionMismatch
            | Self::UnknownParticipation
            | Self::IdentityMismatch => (FailureClass::Invariant, 84),
            Self::StandDownNotStarted | Self::InitializationIncomplete | Self::WrongState => {
                (FailureClass::Control, 85)
            }
            Self::StandDownAlreadyStarted => (FailureClass::Control, 86),
            Self::UnknownAction => (FailureClass::Invariant, 87),
            Self::ConflictingWorkerSettlement
            | Self::InvalidWorkerSettlement
            | Self::ClosureRegression => (FailureClass::Invariant, 88),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerBranch {
    Aborting,
    Releasing,
}

#[derive(Debug)]
struct Record {
    closure: ParticipationClosureState,
    identity: Option<AbortParticipationIssueIdentity>,
    issue_state: Option<AbortParticipationIssueState>,
    authorizations: usize,
    current_action: Option<AttemptActionId>,
    retry_at: Option<MonotonicInstant>,
    worker_branch: Option<WorkerBranch>,
    registry_fenced: bool,
}

#[derive(Clone, Copy, Debug)]
struct IssuedAction {
    participation: ParticipationRef,
    operation: TaskOperationId,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AbortDispatch {
    pub action: AttemptActionId,
    pub request: AbortParticipation,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum AbortPoll {
    Issued(AbortDispatch),
    Progressed,
    Idle,
    Yielded,
}

/// Only this ledger owns the per-participation remote truth. Aggregate counters
/// avoid rescanning N rows from the core's retirement/deadline accessors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RemoteProgress {
    pub initialized: bool,
    pub responsibility_settled: bool,
    pub positive_resource_closure: bool,
    pub tracking_complete: bool,
}

#[derive(Debug)]
pub(crate) struct ParticipationStandDownLedger {
    activation: AttemptActivationIdentity,
    required: Arc<BTreeSet<ParticipationRef>>,
    limit: NonZeroUsize,
    mode: Option<StandDownMode>,
    initialized: bool,
    remote_deadline: Option<MonotonicInstant>,
    records: BTreeMap<ParticipationRef, Record>,
    issued: BTreeMap<AttemptActionId, IssuedAction>,
    ready: BTreeSet<ParticipationRef>,
    retry: BTreeSet<(MonotonicInstant, ParticipationRef)>,
    last_issued: Option<ParticipationRef>,
    settled_count: usize,
    positive_count: usize,
    tracking_count: usize,
}

impl ParticipationStandDownLedger {
    /// The original frozen set is shared without traversing/copying it here.
    pub fn new(
        activation: AttemptActivationIdentity,
        required: Arc<BTreeSet<ParticipationRef>>,
        limit: NonZeroUsize,
    ) -> Self {
        Self {
            activation,
            required,
            limit,
            mode: None,
            initialized: false,
            remote_deadline: None,
            records: BTreeMap::new(),
            issued: BTreeMap::new(),
            ready: BTreeSet::new(),
            retry: BTreeSet::new(),
            last_issued: None,
            settled_count: 0,
            positive_count: 0,
            tracking_count: 0,
        }
    }

    pub fn begin(
        &mut self,
        mode: StandDownMode,
        remote_deadline: MonotonicInstant,
    ) -> Result<(), ParticipationStandDownError> {
        if self.mode.is_some() {
            return Err(ParticipationStandDownError::StandDownAlreadyStarted);
        }
        self.mode = Some(mode);
        self.remote_deadline = Some(remote_deadline);
        Ok(())
    }

    /// Call once per charged frozen row; duplicate detection precedes insertion.
    pub fn initialize_participation(
        &mut self,
        p: ParticipationRef,
        fact: EstablishStandDownFact,
    ) -> Result<(), ParticipationStandDownError> {
        self.require_started()?;
        if self.initialized {
            return Err(ParticipationStandDownError::WrongState);
        }
        self.check_participation(p)?;
        if self.records.contains_key(&p) {
            return Err(ParticipationStandDownError::DuplicateParticipation);
        }
        let closure = match fact {
            EstablishStandDownFact::NoGrant => ParticipationClosureState::NoRemoteResponsibility,
            EstablishStandDownFact::AbortRequired => ParticipationClosureState::AbortPending,
        };
        self.records.insert(
            p,
            Record {
                closure,
                identity: None,
                issue_state: None,
                authorizations: 0,
                current_action: None,
                retry_at: None,
                worker_branch: None,
                registry_fenced: false,
            },
        );
        self.reindex(p, None);
        Ok(())
    }

    pub fn finish_initialization(&mut self) -> Result<(), ParticipationStandDownError> {
        self.require_started()?;
        if self.records.len() != self.required.len() {
            return Err(ParticipationStandDownError::InitializationIncomplete);
        }
        self.initialized = true;
        Ok(())
    }

    /// NoGrant never erases prior possible Worker effects. Authoritative
    /// registry fencing remains stronger than a late Establish projection.
    pub fn refresh_establish_fact(
        &mut self,
        p: ParticipationRef,
        fact: EstablishStandDownFact,
    ) -> Result<(), ParticipationStandDownError> {
        self.require_initialized()?;
        let old = self.record_flags(p)?;
        let record = self.records.get_mut(&p).unwrap();
        if !record.registry_fenced
            && fact == EstablishStandDownFact::AbortRequired
            && record.closure == ParticipationClosureState::NoRemoteResponsibility
        {
            record.closure = ParticipationClosureState::AbortPending;
        }
        self.reindex(p, Some(old));
        Ok(())
    }

    /// One call examines at most one due retry or one ready participation.
    /// Promotion/exhaustion are explicit progress, never an uncharged N scan.
    pub fn poll_next_abort(
        &mut self,
        now: MonotonicInstant,
        action: AttemptActionId,
        ids: &mut OperationIds,
        budget: &mut dyn StepBudget,
    ) -> Result<AbortPoll, ParticipationStandDownError> {
        self.require_initialized()?;
        if self.mode == Some(StandDownMode::SuccessfulCleanup) {
            return Ok(AbortPoll::Idle);
        }
        if let Some(&(at, p)) = self.retry.first().filter(|(at, _)| *at <= now) {
            if !budget.charge(1) {
                return Ok(AbortPoll::Yielded);
            }
            let old = self.record_flags(p)?;
            self.retry.remove(&(at, p));
            self.records.get_mut(&p).unwrap().retry_at = None;
            self.reindex(p, Some(old));
            return Ok(AbortPoll::Progressed);
        }
        let candidate = self
            .last_issued
            .and_then(|p| self.ready.range((Excluded(p), Unbounded)).next().copied())
            .or_else(|| self.ready.first().copied());
        let Some(p) = candidate else {
            return Ok(AbortPoll::Idle);
        };
        if !budget.charge(1) {
            return Ok(AbortPoll::Yielded);
        }
        if self.issued.contains_key(&action) {
            return Err(ParticipationStandDownError::IdentityMismatch);
        }
        let old = self.record_flags(p)?;
        let record = self.records.get_mut(&p).unwrap();
        if record.authorizations == self.limit.get() {
            record.issue_state = Some(AbortParticipationIssueState::AuthorizationExhausted);
            record.closure = ParticipationClosureState::AbortIssueExhausted;
            self.reindex(p, Some(old));
            return Ok(AbortPoll::Progressed);
        }
        let Some(StandDownMode::Abort(cause)) = self.mode else {
            unreachable!("only Abort mode has ready work")
        };
        let identity = *record
            .identity
            .get_or_insert_with(|| AbortParticipationIssueIdentity {
                activation: self.activation,
                operation_id: ids.issue(),
                participation: p,
                cause,
            });
        record.authorizations += 1;
        record.current_action = Some(action);
        record.issue_state = Some(AbortParticipationIssueState::AwaitingSettlement);
        record.retry_at = None;
        self.issued.insert(
            action,
            IssuedAction {
                participation: p,
                operation: identity.operation_id,
            },
        );
        self.last_issued = Some(p);
        self.reindex(p, Some(old));
        Ok(AbortPoll::Issued(AbortDispatch {
            action,
            request: AbortParticipation::new(identity.operation_id, p, cause.abort_cause()),
        }))
    }

    pub fn settle(
        &mut self,
        now: MonotonicInstant,
        activation: AttemptActivationIdentity,
        action: AttemptActionId,
        p: ParticipationRef,
        operation: TaskOperationId,
        settlement: AbortIssueSettlement,
    ) -> Result<(), ParticipationStandDownError> {
        self.require_initialized()?;
        if activation != self.activation {
            return Err(ParticipationStandDownError::ActivationExecutionMismatch);
        }
        let issued = self
            .issued
            .get(&action)
            .ok_or(ParticipationStandDownError::UnknownAction)?;
        if (issued.participation, issued.operation) != (p, operation) {
            return Err(ParticipationStandDownError::IdentityMismatch);
        }
        let old = self.record_flags(p)?;
        let record = self.records.get_mut(&p).unwrap();
        if record.registry_fenced {
            return Ok(());
        }
        let current = record.current_action == Some(action);
        match settlement {
            AbortIssueSettlement::Worker { outcome, closure } => {
                if !matches!(
                    outcome,
                    OperationOutcome::Accepted
                        | OperationOutcome::Idempotent
                        | OperationOutcome::ParticipationTerminalReceipt
                        | OperationOutcome::LeaseExpired
                        | OperationOutcome::Gone
                ) {
                    return Err(ParticipationStandDownError::InvalidWorkerSettlement);
                }
                let (next, branch) = worker_transition(record, closure)?;
                record.closure = next;
                record.worker_branch = branch;
                record.issue_state = Some(AbortParticipationIssueState::WorkerSettled);
                record.retry_at = None;
            }
            AbortIssueSettlement::FailedClosed
                if record.issue_state != Some(AbortParticipationIssueState::WorkerSettled) =>
            {
                record.closure = ParticipationClosureState::AbortIssueExhausted;
                record.issue_state = Some(AbortParticipationIssueState::FailedClosed);
                record.retry_at = None;
            }
            AbortIssueSettlement::DefinitelyUnsent | AbortIssueSettlement::TransportUnknown
                if current
                    && record.issue_state != Some(AbortParticipationIssueState::WorkerSettled) =>
            {
                let next = if settlement == AbortIssueSettlement::DefinitelyUnsent {
                    AbortParticipationIssueState::DefinitelyUnsent
                } else {
                    AbortParticipationIssueState::TransportUnknown
                };
                match record.issue_state {
                    Some(AbortParticipationIssueState::AwaitingSettlement) => {
                        record.issue_state = Some(next);
                        record.retry_at = Some(now.saturating_add(ABORT_RETRY_BACKOFF));
                    }
                    Some(existing) if existing == next => {} // Replay cannot renew the original retry deadline.
                    Some(
                        AbortParticipationIssueState::FailedClosed
                        | AbortParticipationIssueState::AuthorizationExhausted,
                    ) => {}
                    _ => return Err(ParticipationStandDownError::WrongState),
                }
            }
            _ => {} // Old transport dispositions never retract a newer logical action.
        }
        self.reindex(p, Some(old));
        Ok(())
    }

    pub fn observe_registry(
        &mut self,
        now: MonotonicInstant,
        p: ParticipationRef,
        fact: RegistryParticipationConvergence,
    ) -> Result<(), ParticipationStandDownError> {
        self.require_initialized()?;
        if fact == RegistryParticipationConvergence::RemoteTrackingEnded
            && self.remote_deadline.is_some_and(|at| now < at)
        {
            return Err(ParticipationStandDownError::WrongState);
        }
        let old = self.record_flags(p)?;
        let record = self.records.get_mut(&p).unwrap();
        if fact == RegistryParticipationConvergence::RemoteTrackingEnded
            && record.closure.residual_resource_settled()
        {
            return Ok(());
        }
        record.closure = match fact {
            RegistryParticipationConvergence::NeverEstablished => {
                ParticipationClosureState::NoRemoteResponsibility
            }
            RegistryParticipationConvergence::WorkerStoppedAndParticipationFenced => {
                ParticipationClosureState::WorkerStoppedAndParticipationFenced
            }
            RegistryParticipationConvergence::WorkerProcessReplaced => {
                ParticipationClosureState::WorkerProcessReplaced
            }
            RegistryParticipationConvergence::RemoteTrackingEnded => {
                ParticipationClosureState::RemoteTrackingEnded
            }
        };
        record.registry_fenced = true;
        record.retry_at = None;
        self.reindex(p, Some(old));
        Ok(())
    }

    pub fn shorten_remote_deadline(&mut self, deadline: MonotonicInstant) {
        self.remote_deadline = self.remote_deadline.map(|old| old.min(deadline));
    }

    pub fn next_abort_at(&self, now: MonotonicInstant) -> Option<MonotonicInstant> {
        if !self.initialized || self.mode == Some(StandDownMode::SuccessfulCleanup) {
            return None;
        }
        if !self.ready.is_empty() {
            Some(now)
        } else {
            self.retry.first().map(|(at, _)| *at)
        }
    }

    pub fn snapshot(&self, p: ParticipationRef) -> Option<ParticipationStandDownSnapshot> {
        self.records
            .get(&p)
            .map(|r| ParticipationStandDownSnapshot {
                closure: r.closure,
                identity: r.identity,
                issue_state: r.issue_state,
                authorizations: r.authorizations,
                current_action: r.current_action,
            })
    }

    pub fn progress(&self) -> RemoteProgress {
        let n = self.required.len();
        RemoteProgress {
            initialized: self.initialized,
            responsibility_settled: self.initialized && self.settled_count == n,
            positive_resource_closure: self.initialized && self.positive_count == n,
            tracking_complete: self.initialized && self.tracking_count == n,
        }
    }

    #[cfg(test)]
    pub fn responsibility_settled(&self) -> bool {
        self.progress().responsibility_settled
    }
    pub fn residual_resource_settled(&self) -> bool {
        self.progress().positive_resource_closure
    }
    pub fn remote_tracking_ended(&self) -> bool {
        self.progress().tracking_complete
    }

    fn require_started(&self) -> Result<(), ParticipationStandDownError> {
        if self.mode.is_some() {
            Ok(())
        } else {
            Err(ParticipationStandDownError::StandDownNotStarted)
        }
    }
    fn require_initialized(&self) -> Result<(), ParticipationStandDownError> {
        self.require_started()?;
        if self.initialized {
            Ok(())
        } else {
            Err(ParticipationStandDownError::InitializationIncomplete)
        }
    }
    fn check_participation(&self, p: ParticipationRef) -> Result<(), ParticipationStandDownError> {
        if p.query_execution_id() != self.activation.execution() {
            return Err(ParticipationStandDownError::ActivationExecutionMismatch);
        }
        if !self.required.contains(&p) {
            return Err(ParticipationStandDownError::UnknownParticipation);
        }
        Ok(())
    }
    fn record_flags(
        &self,
        p: ParticipationRef,
    ) -> Result<(ParticipationClosureState, Option<MonotonicInstant>), ParticipationStandDownError>
    {
        self.records
            .get(&p)
            .map(|r| (r.closure, r.retry_at))
            .ok_or(ParticipationStandDownError::UnknownParticipation)
    }
    fn reindex(
        &mut self,
        p: ParticipationRef,
        old: Option<(ParticipationClosureState, Option<MonotonicInstant>)>,
    ) {
        if let Some((closure, retry)) = old {
            self.settled_count -= usize::from(closure.stand_down_responsibility_settled());
            self.positive_count -= usize::from(closure.residual_resource_settled());
            self.tracking_count -= usize::from(closure.tracking_complete());
            if let Some(at) = retry {
                self.retry.remove(&(at, p));
            }
        }
        self.ready.remove(&p);
        let record = &self.records[&p];
        self.settled_count += usize::from(record.closure.stand_down_responsibility_settled());
        self.positive_count += usize::from(record.closure.residual_resource_settled());
        self.tracking_count += usize::from(record.closure.tracking_complete());
        if self
            .mode
            .is_some_and(|m| matches!(m, StandDownMode::Abort(_)))
            && record.closure == ParticipationClosureState::AbortPending
            && matches!(
                record.issue_state,
                None | Some(
                    AbortParticipationIssueState::DefinitelyUnsent
                        | AbortParticipationIssueState::TransportUnknown
                )
            )
        {
            if let Some(at) = record.retry_at {
                self.retry.insert((at, p));
            } else {
                self.ready.insert(p);
            }
        }
    }
}

fn worker_transition(
    record: &Record,
    closure: AbortWorkerClosure,
) -> Result<(ParticipationClosureState, Option<WorkerBranch>), ParticipationStandDownError> {
    use ParticipationClosureState as C;
    let (next, observed) = match closure {
        AbortWorkerClosure::Aborting => (C::Aborting, Some(WorkerBranch::Aborting)),
        AbortWorkerClosure::Releasing => (C::Releasing, Some(WorkerBranch::Releasing)),
        AbortWorkerClosure::TerminalRetained => (C::TerminalRetained, None),
        AbortWorkerClosure::Gone => (C::Gone, None),
    };
    let branch = match (record.worker_branch, observed) {
        (Some(current), Some(observed)) if current != observed => {
            return Err(ParticipationStandDownError::ConflictingWorkerSettlement);
        }
        (current, observed) => current.or(observed),
    };
    let closure = match (record.closure, next) {
        (current, next) if current == next => current,
        (C::AbortPending | C::AbortIssueExhausted, next) => next,
        (C::Aborting | C::Releasing, C::TerminalRetained | C::Gone)
        | (C::TerminalRetained, C::Gone) => next,
        (C::TerminalRetained, C::Aborting | C::Releasing)
        | (C::Gone, C::Aborting | C::Releasing | C::TerminalRetained) => record.closure,
        _ => return Err(ParticipationStandDownError::ClosureRegression),
    };
    Ok((closure, branch))
}

#[cfg(test)]
#[path = "stand_down/tests.rs"]
mod tests;
