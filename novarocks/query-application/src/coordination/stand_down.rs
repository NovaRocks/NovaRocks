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

//! Actor-owned participation stand-down responsibility.
//!
//! The ledger owns application semantics only. Native operation carriers,
//! codecs, RPC retries, Worker task termination, and resource-release facts
//! remain outside this module. A participation closure state says how far the Worker
//! participation lifecycle has advanced; it is not proof that a process stopped or
//! that all physical resources were released.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use novarocks_execution_contract::{
    ParticipationReceipt, ParticipationRef, ParticipationState, TaskOperationId,
};
use tokio::sync::{mpsc, watch};

use novarocks_coordination_contract::{
    ABORT_RETRY_BACKOFF, AttemptActivationIdentity, MonotonicInstant,
};

/// Why the logical execution requires its remote participations to stand down.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ParticipationStandDownCause {
    LogicalExecutionCancelled,
    LogicalExecutionFailed,
}

/// What Establish/admission currently proves about a participation's remote effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EstablishStandDownFact {
    /// No admission grant was ever received, so no Worker capacity or participation
    /// can remain on behalf of this logical execution.
    NoGrant,
    /// An authorized Establish has not yet linearized to definitely-unsent or
    /// transport-owned. Cancellation must wait for that exact transition.
    IssueUnsettled,
    /// A Worker admission ticket was granted. Abort remains required even if
    /// Establish was definitely unsent: Worker Abort establishes the absent
    /// participation fence and returns the ticket's reserved capacity.
    AbortRequired,
}

/// Exact, secret-free identity of one actor-owned Abort effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AbortParticipationIssueIdentity {
    activation: AttemptActivationIdentity,
    operation_id: TaskOperationId,
    participation: ParticipationRef,
    cause: ParticipationStandDownCause,
}

impl AbortParticipationIssueIdentity {
    pub const fn activation(self) -> AttemptActivationIdentity {
        self.activation
    }

    pub const fn operation_id(self) -> TaskOperationId {
        self.operation_id
    }

    pub const fn participation(self) -> ParticipationRef {
        self.participation
    }

    pub const fn cause(self) -> ParticipationStandDownCause {
        self.cause
    }
}

/// Query-application request projected into the Abort effect port.
///
/// Frontend composition translates this value into its Native carrier. The
/// exact allocation is retained by the actor across transport-unknown replay.
#[derive(Debug)]
pub struct AbortParticipationEffectRequest {
    identity: AbortParticipationIssueIdentity,
}

impl AbortParticipationEffectRequest {
    pub const fn identity(&self) -> AbortParticipationIssueIdentity {
        self.identity
    }
}

/// Worker participation-lifecycle progress observed while standing down.
///
/// These values do not assert Worker process exit, task-thread exit, or
/// physical resource release. Those remain separately observed convergence
/// facts owned by coordination and workload governance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParticipationClosureState {
    /// The actor proved that no admission operation could have left Worker
    /// capacity or a participation behind. This is a local responsibility result,
    /// not an observation that the Worker reported `Gone`.
    NoRemoteResponsibility,
    AwaitingEstablishIssue,
    AbortPending,
    /// The actor exhausted this participation's bounded Abort issue budget. The
    /// residual supervisor must retain the exact participation responsibility and
    /// may still apply later Worker or process-fence evidence.
    AbortIssueExhausted,
    Aborting,
    Releasing,
    TerminalRetained,
    Gone,
    /// The Registry proved actual stop together with closed normal admission
    /// and invalidated old admission authority for this exact participation.
    WorkerStoppedAndParticipationFenced,
    /// The Registry observed replacement of the exact Worker process named by
    /// this participation. This fences the old process identity but does not claim
    /// that its resources were released cleanly.
    WorkerProcessReplaced,
    /// The coordinator stopped tracking an unreachable participation after its
    /// finite cleanup window. This is a local observation-retirement result,
    /// never a Worker stop or zero-resource fact.
    RemoteTrackingEnded,
}

impl ParticipationClosureState {
    pub const fn stand_down_responsibility_settled(self) -> bool {
        matches!(
            self,
            Self::NoRemoteResponsibility
                | Self::Aborting
                | Self::Releasing
                | Self::TerminalRetained
                | Self::Gone
                | Self::WorkerStoppedAndParticipationFenced
                | Self::WorkerProcessReplaced
                | Self::RemoteTrackingEnded
        )
    }

    pub const fn residual_resource_settled(self) -> bool {
        matches!(
            self,
            Self::NoRemoteResponsibility
                | Self::WorkerStoppedAndParticipationFenced
                | Self::WorkerProcessReplaced
        )
    }
}

/// Exact settlement produced by the Worker-facing Abort adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
struct AbortParticipationWorkerSettlement {
    identity: AbortParticipationIssueIdentity,
    receipt: ParticipationReceipt,
}

impl AbortParticipationWorkerSettlement {
    fn try_new(
        identity: AbortParticipationIssueIdentity,
        receipt: ParticipationReceipt,
    ) -> Result<Self, ParticipationStandDownError> {
        if receipt.participation() != identity.participation() {
            return Err(ParticipationStandDownError::IdentityMismatch);
        }
        if worker_closure(receipt.state()).is_none() {
            return Err(ParticipationStandDownError::InvalidWorkerSettlement);
        }
        Ok(Self { identity, receipt })
    }

    const fn identity(&self) -> AbortParticipationIssueIdentity {
        self.identity
    }

    fn closure(&self) -> ParticipationClosureState {
        worker_closure(self.receipt.state()).expect("validated Abort Worker settlement")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbortParticipationIssueState {
    IssueAuthorized,
    TransportOwned,
    DefinitelyUnsent,
    TransportUnknown,
    WorkerSettled,
    /// The effect owner observed a local protocol or dispatcher violation.
    /// Replaying cannot repair such a violation, so the participation remains a
    /// residual responsibility until Worker-stop or process-fence evidence.
    FailedClosed,
    AuthorizationExhausted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParticipationStandDownSnapshot {
    closure: ParticipationClosureState,
    identity: Option<AbortParticipationIssueIdentity>,
    issue_state: Option<AbortParticipationIssueState>,
}

impl ParticipationStandDownSnapshot {
    pub const fn closure(self) -> ParticipationClosureState {
        self.closure
    }

    pub const fn identity(self) -> Option<AbortParticipationIssueIdentity> {
        self.identity
    }

    pub const fn issue_state(self) -> Option<AbortParticipationIssueState> {
        self.issue_state
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParticipationStandDownError {
    DuplicateParticipation,
    ActivationExecutionMismatch,
    UnknownParticipation,
    StandDownAlreadyStarted,
    StandDownNotStarted,
    EstablishIssueStillUnsettled,
    WrongState,
    IdentityMismatch,
    AuthorizationGenerationExhausted,
    FutureAuthorization,
    ConflictingWorkerSettlement,
    InvalidWorkerSettlement,
    ClosureRegression,
    EventCapacityOverflow,
    EventChannelClosed,
    EffectCapacityClosed,
    EventBackpressureInvariant,
    ResponsibilitySettlementFailed,
}

impl fmt::Display for ParticipationStandDownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::DuplicateParticipation => {
                "stand-down participation set contains a duplicate identity"
            }
            Self::ActivationExecutionMismatch => {
                "stand-down activation and participation name different executions"
            }
            Self::UnknownParticipation => "stand-down event names an unknown participation",
            Self::StandDownAlreadyStarted => "participation stand-down was already started",
            Self::StandDownNotStarted => "participation stand-down has not started",
            Self::EstablishIssueStillUnsettled => {
                "Establish issue has not linearized before stand-down"
            }
            Self::WrongState => "participation stand-down action is illegal in the current state",
            Self::IdentityMismatch => "stand-down event does not match its exact Abort issue",
            Self::AuthorizationGenerationExhausted => {
                "Abort issue authorization generation is exhausted"
            }
            Self::FutureAuthorization => {
                "Abort event belongs to an authorization generation not yet issued"
            }
            Self::ConflictingWorkerSettlement => {
                "Abort issue received conflicting Worker settlements"
            }
            Self::InvalidWorkerSettlement => {
                "Abort Worker receipt does not prove a closing participation state"
            }
            Self::ClosureRegression => "participation closure observation regressed",
            Self::EventCapacityOverflow => "Abort event capacity calculation overflowed",
            Self::EventChannelClosed => "Abort issue actor event channel is closed",
            Self::EffectCapacityClosed => "Abort effect-port capacity observation is closed",
            Self::EventBackpressureInvariant => {
                "bounded Abort event capacity was exhausted before its issue budget"
            }
            Self::ResponsibilitySettlementFailed => {
                "stand-down residual responsibility could not be settled"
            }
        })
    }
}

impl std::error::Error for ParticipationStandDownError {}

impl novarocks_execution_contract::failure::FailureProjection for ParticipationStandDownError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureFact, FailureReason, domain,
        };
        let (class, code, observed_stop) = match self {
            Self::DuplicateParticipation => (FailureClass::Invariant, 84, false),
            Self::ActivationExecutionMismatch => (FailureClass::Invariant, 84, false),
            Self::UnknownParticipation => (FailureClass::Invariant, 84, false),
            Self::IdentityMismatch => (FailureClass::Invariant, 84, false),
            Self::StandDownNotStarted => (FailureClass::Control, 85, false),
            Self::EstablishIssueStillUnsettled => (FailureClass::Control, 85, false),
            Self::WrongState => (FailureClass::Control, 85, false),
            Self::StandDownAlreadyStarted => (FailureClass::Control, 86, true),
            Self::FutureAuthorization => (FailureClass::Invariant, 87, false),
            Self::ConflictingWorkerSettlement => (FailureClass::Invariant, 88, false),
            Self::InvalidWorkerSettlement => (FailureClass::Invariant, 88, false),
            Self::ClosureRegression => (FailureClass::Invariant, 88, false),
            Self::AuthorizationGenerationExhausted => (FailureClass::Invariant, 89, false),
            Self::EventCapacityOverflow => (FailureClass::Resource, 90, false),
            Self::EventBackpressureInvariant => (FailureClass::Invariant, 91, false),
            Self::EventChannelClosed => (FailureClass::Control, 92, true),
            Self::EffectCapacityClosed => (FailureClass::Control, 92, true),
            Self::ResponsibilitySettlementFailed => (FailureClass::Unexpected, 93, false),
        };
        let reason = FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(code).unwrap());
        if observed_stop {
            FailureFact::stop_observation(reason)
        } else {
            FailureFact::new(class, reason)
        }
    }
}

impl ParticipationStandDownError {
    /// Bounded diagnostic authored here; no foreign Display becomes safe detail.
    pub fn safe_detail(&self) -> novarocks_execution_contract::SafeDetail {
        let message = match self {
            Self::DuplicateParticipation => {
                "stand-down participation set contains a duplicate identity"
            }
            Self::ActivationExecutionMismatch => {
                "stand-down activation and participation name different executions"
            }
            Self::UnknownParticipation => "stand-down event names an unknown participation",
            Self::IdentityMismatch => "stand-down event does not match its exact Abort issue",
            Self::StandDownNotStarted => "participation stand-down has not started",
            Self::EstablishIssueStillUnsettled => {
                "Establish issue has not settled before stand-down"
            }
            Self::WrongState => {
                "participation stand-down action is not allowed in the current state"
            }
            Self::StandDownAlreadyStarted => "participation stand-down already started",
            Self::FutureAuthorization => {
                "Abort event names an authorization generation not yet issued"
            }
            Self::ConflictingWorkerSettlement => {
                "Abort issue received conflicting Worker settlements"
            }
            Self::InvalidWorkerSettlement => {
                "Abort Worker receipt does not prove a closing participation state"
            }
            Self::ClosureRegression => "participation closure observation regressed",
            Self::AuthorizationGenerationExhausted => {
                "bounded Abort authorization generation could not advance"
            }
            Self::EventCapacityOverflow => "Abort event capacity arithmetic overflowed",
            Self::EventBackpressureInvariant => {
                "bounded Abort event capacity exhausted before its issue budget"
            }
            Self::EventChannelClosed => "Abort issue actor event channel is closed",
            Self::EffectCapacityClosed => "Abort effect-port capacity observation is closed",
            Self::ResponsibilitySettlementFailed => {
                "stand-down residual responsibility could not be settled"
            }
        };
        novarocks_execution_contract::SafeDetail::truncating(message)
    }
}

#[derive(Debug)]
struct ParticipationStandDownRecord {
    activation: AttemptActivationIdentity,
    cause: Option<ParticipationStandDownCause>,
    closure: ParticipationClosureState,
    request: Option<Arc<AbortParticipationEffectRequest>>,
    identity: Option<AbortParticipationIssueIdentity>,
    issue_state: Option<AbortParticipationIssueState>,
    authorization_generation: u64,
    worker_settled: bool,
    worker_branch: Option<WorkerClosureBranch>,
    retry_not_before: Option<novarocks_coordination_contract::MonotonicInstant>,
    registry_fenced: bool,
}

impl ParticipationStandDownRecord {
    fn snapshot(&self) -> ParticipationStandDownSnapshot {
        ParticipationStandDownSnapshot {
            closure: self.closure,
            identity: self.identity,
            issue_state: self.issue_state,
        }
    }

    fn require_abort(&mut self, participation: ParticipationRef) {
        if self.identity.is_none() {
            let Some(cause) = self.cause else {
                if matches!(
                    self.closure,
                    ParticipationClosureState::AwaitingEstablishIssue
                ) {
                    self.closure = ParticipationClosureState::AbortPending;
                }
                return;
            };
            let identity = AbortParticipationIssueIdentity {
                activation: self.activation,
                operation_id: TaskOperationId::new_v7(),
                participation,
                cause,
            };
            self.identity = Some(identity);
            self.request = Some(Arc::new(AbortParticipationEffectRequest { identity }));
        }
        if matches!(
            self.closure,
            ParticipationClosureState::AwaitingEstablishIssue
        ) {
            self.closure = ParticipationClosureState::AbortPending;
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AbortIssueEventKind {
    TransportOwned,
    DefinitelyUnsent,
    TransportUnknown,
    WorkerSettled(Box<AbortParticipationWorkerSettlement>),
    FailedClosed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AbortIssueEvent {
    identity: AbortParticipationIssueIdentity,
    authorization_generation: u64,
    kind: AbortIssueEventKind,
}

/// Pure state owned by one logical-execution actor.
///
/// The event channel is physically bounded to three fixed-size events per
/// participation and authorization: transport ownership, one transport disposition,
/// and one possible late Worker settlement. Authorization budget exhaustion
/// therefore applies backpressure without an unbounded task or thread queue.
#[derive(Debug)]
pub(crate) struct ParticipationStandDownLedger {
    required_participations: BTreeSet<ParticipationRef>,
    max_authorizations_per_participation: NonZeroUsize,
    records: BTreeMap<ParticipationRef, ParticipationStandDownRecord>,
    events: mpsc::Sender<AbortIssueEvent>,
    event_rx: mpsc::Receiver<AbortIssueEvent>,
    started: bool,
    cause: Option<ParticipationStandDownCause>,
    authorization_cursor: Option<ParticipationRef>,
    retry_backoff: Duration,
}

impl ParticipationStandDownLedger {
    pub(crate) fn new(
        required_participations: BTreeSet<ParticipationRef>,
        max_authorizations_per_participation: NonZeroUsize,
    ) -> Result<Self, ParticipationStandDownError> {
        let event_capacity = required_participations
            .len()
            .max(1)
            .checked_mul(max_authorizations_per_participation.get())
            .and_then(|capacity| capacity.checked_mul(3))
            .ok_or(ParticipationStandDownError::EventCapacityOverflow)?;
        let (events, event_rx) = mpsc::channel(event_capacity);
        Ok(Self {
            required_participations,
            max_authorizations_per_participation,
            records: BTreeMap::new(),
            events,
            event_rx,
            started: false,
            cause: None,
            authorization_cursor: None,
            retry_backoff: ABORT_RETRY_BACKOFF,
        })
    }

    pub(crate) fn begin(
        &mut self,
        activation: AttemptActivationIdentity,
        cause: ParticipationStandDownCause,
        facts: impl IntoIterator<Item = (ParticipationRef, EstablishStandDownFact)>,
    ) -> Result<(), ParticipationStandDownError> {
        self.begin_with_cause(activation, Some(cause), facts)
    }

    pub(crate) fn begin_successful_cleanup(
        &mut self,
        activation: AttemptActivationIdentity,
        facts: impl IntoIterator<Item = (ParticipationRef, EstablishStandDownFact)>,
    ) -> Result<(), ParticipationStandDownError> {
        self.begin_with_cause(activation, None, facts)
    }

    fn begin_with_cause(
        &mut self,
        activation: AttemptActivationIdentity,
        cause: Option<ParticipationStandDownCause>,
        facts: impl IntoIterator<Item = (ParticipationRef, EstablishStandDownFact)>,
    ) -> Result<(), ParticipationStandDownError> {
        if self.started {
            return Err(ParticipationStandDownError::StandDownAlreadyStarted);
        }
        let mut collected = BTreeMap::new();
        for (participation, fact) in facts {
            if collected.insert(participation, fact).is_some() {
                return Err(ParticipationStandDownError::DuplicateParticipation);
            }
        }
        if collected
            .keys()
            .any(|participation| !self.required_participations.contains(participation))
        {
            return Err(ParticipationStandDownError::UnknownParticipation);
        }
        if collected.len() != self.required_participations.len() {
            return Err(ParticipationStandDownError::UnknownParticipation);
        }
        for participation in &self.required_participations {
            if participation.query_execution_id() != activation.execution() {
                return Err(ParticipationStandDownError::ActivationExecutionMismatch);
            }
            let fact = *collected
                .get(participation)
                .ok_or(ParticipationStandDownError::UnknownParticipation)?;
            let closure = match fact {
                EstablishStandDownFact::NoGrant => {
                    ParticipationClosureState::NoRemoteResponsibility
                }
                EstablishStandDownFact::IssueUnsettled => {
                    ParticipationClosureState::AwaitingEstablishIssue
                }
                EstablishStandDownFact::AbortRequired => ParticipationClosureState::AbortPending,
            };
            let mut record = ParticipationStandDownRecord {
                activation,
                cause,
                closure,
                request: None,
                identity: None,
                issue_state: None,
                authorization_generation: 0,
                worker_settled: false,
                worker_branch: None,
                retry_not_before: None,
                registry_fenced: false,
            };
            if fact == EstablishStandDownFact::AbortRequired {
                record.require_abort(*participation);
            }
            self.records.insert(*participation, record);
        }
        self.cause = cause;
        self.started = true;
        Ok(())
    }

    pub(crate) const fn started(&self) -> bool {
        self.started
    }

    pub(crate) const fn authorization_limit(&self) -> NonZeroUsize {
        self.max_authorizations_per_participation
    }

    pub(crate) fn is_successful_cleanup(&self) -> bool {
        self.started && self.cause.is_none()
    }

    pub(crate) fn refresh_establish_fact(
        &mut self,
        participation: ParticipationRef,
        fact: EstablishStandDownFact,
    ) -> Result<(), ParticipationStandDownError> {
        if !self.started {
            return Err(ParticipationStandDownError::StandDownNotStarted);
        }
        let record = self
            .records
            .get_mut(&participation)
            .ok_or(ParticipationStandDownError::UnknownParticipation)?;
        if record.closure != ParticipationClosureState::AwaitingEstablishIssue {
            return Ok(());
        }
        match fact {
            EstablishStandDownFact::NoGrant => {
                record.closure = ParticipationClosureState::NoRemoteResponsibility;
            }
            EstablishStandDownFact::IssueUnsettled => {}
            EstablishStandDownFact::AbortRequired => {
                record.require_abort(participation);
            }
        }
        Ok(())
    }

    pub(crate) fn authorize_next_at(
        &mut self,
        now: novarocks_coordination_contract::MonotonicInstant,
    ) -> Result<Option<AbortParticipationIssuePermit>, ParticipationStandDownError> {
        self.drain_events_at(now)?;
        let participations: Vec<_> = self
            .records
            .keys()
            .copied()
            .filter(|participation| {
                self.authorization_cursor
                    .is_none_or(|cursor| *participation > cursor)
            })
            .chain(self.records.keys().copied().filter(|participation| {
                self.authorization_cursor
                    .is_some_and(|cursor| *participation <= cursor)
            }))
            .collect();
        for participation in participations {
            let record = self
                .records
                .get_mut(&participation)
                .expect("Abort authorization participation came from the same ledger");
            if record.closure != ParticipationClosureState::AbortPending
                || !matches!(
                    record.issue_state,
                    None | Some(AbortParticipationIssueState::DefinitelyUnsent)
                        | Some(AbortParticipationIssueState::TransportUnknown)
                )
                || record
                    .retry_not_before
                    .is_some_and(|deadline| !now.has_reached(deadline))
            {
                continue;
            }
            if record.authorization_generation
                >= self.max_authorizations_per_participation.get() as u64
            {
                record.issue_state = Some(AbortParticipationIssueState::AuthorizationExhausted);
                record.closure = ParticipationClosureState::AbortIssueExhausted;
                continue;
            }
            record.authorization_generation = record
                .authorization_generation
                .checked_add(1)
                .ok_or(ParticipationStandDownError::AuthorizationGenerationExhausted)?;
            record.issue_state = Some(AbortParticipationIssueState::IssueAuthorized);
            record.retry_not_before = None;
            self.authorization_cursor = Some(participation);
            return Ok(Some(AbortParticipationIssuePermit {
                identity: record
                    .identity
                    .ok_or(ParticipationStandDownError::IdentityMismatch)?,
                authorization_generation: record.authorization_generation,
                request: Some(Arc::clone(
                    record
                        .request
                        .as_ref()
                        .ok_or(ParticipationStandDownError::IdentityMismatch)?,
                )),
                events: self.events.clone(),
            }));
        }
        Ok(None)
    }

    #[cfg(test)]
    pub(crate) fn authorize_next(
        &mut self,
    ) -> Result<Option<AbortParticipationIssuePermit>, ParticipationStandDownError> {
        self.authorize_next_at(
            novarocks_coordination_contract::MonotonicInstant::from_origin(Duration::MAX),
        )
    }

    pub(crate) fn snapshot(
        &self,
        participation: ParticipationRef,
    ) -> Option<ParticipationStandDownSnapshot> {
        self.records
            .get(&participation)
            .map(ParticipationStandDownRecord::snapshot)
    }

    pub(crate) fn responsibility_settled(&self) -> bool {
        self.started
            && self
                .records
                .values()
                .all(|record| record.closure.stand_down_responsibility_settled())
    }

    pub(crate) fn residual_resource_settled(&self) -> bool {
        self.started
            && self
                .records
                .values()
                .all(|record| record.closure.residual_resource_settled())
    }

    /// Every remaining participation either has positive resource-closure evidence
    /// or this coordinator has ended its finite remote observation. The latter
    /// permits FE retirement only; it deliberately does not make
    /// `residual_resource_settled` true.
    pub(crate) fn remote_tracking_ended(&self) -> bool {
        self.started
            && self.records.values().all(|record| {
                record.closure.residual_resource_settled()
                    || record.closure == ParticipationClosureState::RemoteTrackingEnded
            })
    }

    #[cfg(test)]
    pub(crate) fn has_unsettled_establish_issue(&self) -> bool {
        self.records
            .values()
            .any(|record| record.closure == ParticipationClosureState::AwaitingEstablishIssue)
    }

    pub(crate) fn observe_registry_convergence(
        &mut self,
        participation: ParticipationRef,
        convergence: RegistryParticipationConvergence,
    ) -> Result<(), ParticipationStandDownError> {
        if !self.started {
            return Err(ParticipationStandDownError::StandDownNotStarted);
        }
        let record = self
            .records
            .get_mut(&participation)
            .ok_or(ParticipationStandDownError::UnknownParticipation)?;
        if convergence == RegistryParticipationConvergence::RemoteTrackingEnded
            && record.closure.residual_resource_settled()
        {
            return Ok(());
        }
        record.registry_fenced = true;
        record.retry_not_before = None;
        record.request = None;
        record.closure = match convergence {
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
        Ok(())
    }

    pub(crate) fn next_retry_at(
        &self,
    ) -> Option<novarocks_coordination_contract::MonotonicInstant> {
        self.records
            .values()
            .filter(|record| record.closure == ParticipationClosureState::AbortPending)
            .filter_map(|record| record.retry_not_before)
            .min()
    }

    pub(crate) fn drain_events_at(
        &mut self,
        now: novarocks_coordination_contract::MonotonicInstant,
    ) -> Result<usize, ParticipationStandDownError> {
        let mut applied = 0;
        loop {
            match self.event_rx.try_recv() {
                Ok(event) => {
                    self.apply_event(event, now)?;
                    applied += 1;
                }
                Err(mpsc::error::TryRecvError::Empty) => return Ok(applied),
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return Err(ParticipationStandDownError::EventChannelClosed);
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn drain_events(&mut self) -> Result<usize, ParticipationStandDownError> {
        self.drain_events_at(novarocks_coordination_contract::MonotonicInstant::ORIGIN)
    }

    #[cfg(test)]
    pub(crate) async fn apply_next_event(
        &mut self,
        now: impl FnOnce() -> novarocks_coordination_contract::MonotonicInstant,
    ) -> Result<(), ParticipationStandDownError> {
        let event = self
            .event_rx
            .recv()
            .await
            .ok_or(ParticipationStandDownError::EventChannelClosed)?;
        self.apply_event(event, now())
    }

    pub(crate) fn poll_next_event(
        &mut self,
        context: &mut Context<'_>,
        now: impl FnOnce() -> MonotonicInstant,
    ) -> Poll<Result<(), ParticipationStandDownError>> {
        match self.event_rx.poll_recv(context) {
            Poll::Ready(Some(event)) => Poll::Ready(self.apply_event(event, now())),
            Poll::Ready(None) => Poll::Ready(Err(ParticipationStandDownError::EventChannelClosed)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn apply_event(
        &mut self,
        event: AbortIssueEvent,
        now: novarocks_coordination_contract::MonotonicInstant,
    ) -> Result<(), ParticipationStandDownError> {
        let record = self
            .records
            .get_mut(&event.identity.participation())
            .ok_or(ParticipationStandDownError::UnknownParticipation)?;
        if record.identity != Some(event.identity) {
            return Err(ParticipationStandDownError::IdentityMismatch);
        }
        if event.authorization_generation > record.authorization_generation {
            return Err(ParticipationStandDownError::FutureAuthorization);
        }
        if event.authorization_generation < record.authorization_generation {
            return match event.kind {
                AbortIssueEventKind::WorkerSettled(_) if record.registry_fenced => Ok(()),
                AbortIssueEventKind::WorkerSettled(settlement) => {
                    settle_worker(record, *settlement)
                }
                AbortIssueEventKind::FailedClosed => fail_closed(record),
                _ => Ok(()),
            };
        }
        if record.registry_fenced {
            return Ok(());
        }
        if record.worker_settled && !matches!(&event.kind, AbortIssueEventKind::WorkerSettled(_)) {
            return Ok(());
        }
        match event.kind {
            AbortIssueEventKind::WorkerSettled(settlement) => settle_worker(record, *settlement),
            AbortIssueEventKind::TransportOwned => match record.issue_state {
                Some(AbortParticipationIssueState::IssueAuthorized) => {
                    record.issue_state = Some(AbortParticipationIssueState::TransportOwned);
                    Ok(())
                }
                Some(AbortParticipationIssueState::TransportOwned) => Ok(()),
                _ => Err(ParticipationStandDownError::WrongState),
            },
            AbortIssueEventKind::DefinitelyUnsent => match record.issue_state {
                Some(AbortParticipationIssueState::IssueAuthorized)
                | Some(AbortParticipationIssueState::TransportOwned) => {
                    record.issue_state = Some(AbortParticipationIssueState::DefinitelyUnsent);
                    Ok(())
                }
                Some(AbortParticipationIssueState::DefinitelyUnsent)
                | Some(AbortParticipationIssueState::WorkerSettled) => Ok(()),
                _ => Err(ParticipationStandDownError::WrongState),
            },
            AbortIssueEventKind::TransportUnknown => match record.issue_state {
                Some(AbortParticipationIssueState::TransportOwned) => {
                    record.issue_state = Some(AbortParticipationIssueState::TransportUnknown);
                    record.retry_not_before = Some(now.saturating_add(self.retry_backoff));
                    Ok(())
                }
                Some(AbortParticipationIssueState::TransportUnknown) => Ok(()),
                _ => Err(ParticipationStandDownError::WrongState),
            },
            AbortIssueEventKind::FailedClosed => fail_closed(record),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegistryParticipationConvergence {
    NeverEstablished,
    WorkerStoppedAndParticipationFenced,
    WorkerProcessReplaced,
    RemoteTrackingEnded,
}

fn settle_worker(
    record: &mut ParticipationStandDownRecord,
    settlement: AbortParticipationWorkerSettlement,
) -> Result<(), ParticipationStandDownError> {
    if record.identity != Some(settlement.identity()) {
        return Err(ParticipationStandDownError::IdentityMismatch);
    }
    let closure = settlement.closure();
    reconcile_worker_branch(record, closure)?;
    advance_closure(record, closure)?;
    record.worker_settled = true;
    record.issue_state = Some(AbortParticipationIssueState::WorkerSettled);
    record.request = None;
    Ok(())
}

fn fail_closed(
    record: &mut ParticipationStandDownRecord,
) -> Result<(), ParticipationStandDownError> {
    if record.registry_fenced || record.worker_settled {
        return Ok(());
    }
    record.issue_state = Some(AbortParticipationIssueState::FailedClosed);
    record.closure = ParticipationClosureState::AbortIssueExhausted;
    record.retry_not_before = None;
    record.request = None;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerClosureBranch {
    Aborting,
    Releasing,
}

fn reconcile_worker_branch(
    record: &mut ParticipationStandDownRecord,
    closure: ParticipationClosureState,
) -> Result<(), ParticipationStandDownError> {
    let observed = match closure {
        ParticipationClosureState::Aborting => Some(WorkerClosureBranch::Aborting),
        ParticipationClosureState::Releasing => Some(WorkerClosureBranch::Releasing),
        _ => None,
    };
    match (record.worker_branch, observed) {
        (Some(current), Some(next)) if current != next => {
            Err(ParticipationStandDownError::ConflictingWorkerSettlement)
        }
        (None, Some(branch)) => {
            record.worker_branch = Some(branch);
            Ok(())
        }
        _ => Ok(()),
    }
}

fn advance_closure(
    record: &mut ParticipationStandDownRecord,
    closure: ParticipationClosureState,
) -> Result<(), ParticipationStandDownError> {
    use ParticipationClosureState::{
        AbortIssueExhausted, AbortPending, Aborting, AwaitingEstablishIssue, Gone, Releasing,
        TerminalRetained,
    };

    if record.closure == closure {
        return Ok(());
    }
    let next = match (record.closure, closure) {
        (
            AwaitingEstablishIssue | AbortPending | AbortIssueExhausted,
            Aborting | Releasing | TerminalRetained | Gone,
        ) => closure,
        (Aborting | Releasing, TerminalRetained | Gone) | (TerminalRetained, Gone) => closure,
        // Exact Abort replays may return an older response after a newer
        // response already observed the same branch at a terminal state.
        (TerminalRetained, Aborting | Releasing)
        | (Gone, Aborting | Releasing | TerminalRetained) => record.closure,
        (Aborting, Releasing) | (Releasing, Aborting) => {
            return Err(ParticipationStandDownError::ConflictingWorkerSettlement);
        }
        _ => return Err(ParticipationStandDownError::ClosureRegression),
    };
    record.closure = next;
    Ok(())
}

fn worker_closure(state: ParticipationState) -> Option<ParticipationClosureState> {
    match state {
        ParticipationState::Aborting => Some(ParticipationClosureState::Aborting),
        ParticipationState::Releasing => Some(ParticipationClosureState::Releasing),
        ParticipationState::TerminalRetained => Some(ParticipationClosureState::TerminalRetained),
        ParticipationState::Gone => Some(ParticipationClosureState::Gone),
        ParticipationState::Absent
        | ParticipationState::Establishing
        | ParticipationState::Active
        | ParticipationState::Quiescing => None,
    }
}

/// Result of the synchronous effect-port admission boundary.
#[derive(Debug)]
pub enum AbortParticipationIssueSubmit {
    Accepted,
    Backpressured(AbortParticipationIssuePermit),
}

/// Query application's only dependency on Abort transport.
pub trait AbortParticipationEffectPort: fmt::Debug + Send + Sync {
    /// Monotonic capacity epoch. A Backpressured answer must eventually
    /// publish a newer value when another reservation may be attempted.
    fn subscribe_capacity(&self) -> watch::Receiver<u64>;

    fn try_reserve(
        &self,
        identity: AbortParticipationIssueIdentity,
    ) -> AbortParticipationEffectAdmission;
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct PermanentlyBackpressuredAbortEffectPort {
    capacity: watch::Sender<u64>,
}

#[cfg(test)]
impl PermanentlyBackpressuredAbortEffectPort {
    pub(crate) fn shared() -> Arc<dyn AbortParticipationEffectPort> {
        let (capacity, _) = watch::channel(0);
        Arc::new(Self { capacity })
    }
}

#[cfg(test)]
impl AbortParticipationEffectPort for PermanentlyBackpressuredAbortEffectPort {
    fn subscribe_capacity(&self) -> watch::Receiver<u64> {
        self.capacity.subscribe()
    }

    fn try_reserve(
        &self,
        _identity: AbortParticipationIssueIdentity,
    ) -> AbortParticipationEffectAdmission {
        AbortParticipationEffectAdmission::Backpressured
    }
}

pub enum AbortParticipationEffectAdmission {
    Admitted(Box<dyn AbortParticipationEffectReservation>),
    Backpressured,
    /// The role-composed effect owner is gone, so no later capacity change
    /// can make this Abort issuable.
    Closed,
}

impl fmt::Debug for AbortParticipationEffectAdmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admitted(_) => formatter.write_str("Admitted(..)"),
            Self::Backpressured => formatter.write_str("Backpressured"),
            Self::Closed => formatter.write_str("Closed"),
        }
    }
}

/// Capacity is reserved before this object can receive or inspect a request.
pub trait AbortParticipationEffectReservation: fmt::Debug + Send {
    fn submit(self: Box<Self>, submission: AbortParticipationEffectSubmission);
}

#[derive(Debug)]
#[must_use = "an authorized Abort must enter effect-port ownership or settle definitely unsent"]
pub struct AbortParticipationIssuePermit {
    identity: AbortParticipationIssueIdentity,
    authorization_generation: u64,
    request: Option<Arc<AbortParticipationEffectRequest>>,
    events: mpsc::Sender<AbortIssueEvent>,
}

impl AbortParticipationIssuePermit {
    pub const fn identity(&self) -> AbortParticipationIssueIdentity {
        self.identity
    }

    pub fn try_submit(
        mut self,
        port: &dyn AbortParticipationEffectPort,
    ) -> Result<AbortParticipationIssueSubmit, ParticipationStandDownError> {
        let reservation = match port.try_reserve(self.identity) {
            AbortParticipationEffectAdmission::Admitted(reservation) => reservation,
            AbortParticipationEffectAdmission::Backpressured => {
                return Ok(AbortParticipationIssueSubmit::Backpressured(self));
            }
            AbortParticipationEffectAdmission::Closed => {
                return Err(ParticipationStandDownError::EffectCapacityClosed);
            }
        };
        self.publish(AbortIssueEventKind::TransportOwned)?;
        let submission = AbortParticipationEffectSubmission {
            identity: self.identity,
            authorization_generation: self.authorization_generation,
            request: self.request.take(),
            events: self.events.clone(),
        };
        reservation.submit(submission);
        Ok(AbortParticipationIssueSubmit::Accepted)
    }

    fn publish(&self, kind: AbortIssueEventKind) -> Result<(), ParticipationStandDownError> {
        publish_event(
            &self.events,
            AbortIssueEvent {
                identity: self.identity,
                authorization_generation: self.authorization_generation,
                kind,
            },
        )
    }

    pub(crate) fn settle_definitely_unsent(mut self) -> Result<(), ParticipationStandDownError> {
        self.publish(AbortIssueEventKind::DefinitelyUnsent)?;
        self.request.take();
        Ok(())
    }
}

impl Drop for AbortParticipationIssuePermit {
    fn drop(&mut self) {
        if self.request.is_some() {
            let _ = self.publish(AbortIssueEventKind::DefinitelyUnsent);
        }
    }
}

#[derive(Debug)]
#[must_use = "an effect-port-owned Abort must publish its exact settlement"]
pub struct AbortParticipationEffectSubmission {
    identity: AbortParticipationIssueIdentity,
    authorization_generation: u64,
    request: Option<Arc<AbortParticipationEffectRequest>>,
    events: mpsc::Sender<AbortIssueEvent>,
}

impl AbortParticipationEffectSubmission {
    pub const fn identity(&self) -> AbortParticipationIssueIdentity {
        self.identity
    }

    pub fn request(&self) -> &AbortParticipationEffectRequest {
        self.request
            .as_deref()
            .expect("live Abort effect submission retains its exact request")
    }

    pub fn transport_unknown(
        mut self,
    ) -> Result<LateAbortParticipationWorkerSettlement, ParticipationStandDownError> {
        self.publish(AbortIssueEventKind::TransportUnknown)?;
        self.request.take();
        Ok(LateAbortParticipationWorkerSettlement {
            identity: self.identity,
            authorization_generation: self.authorization_generation,
            events: self.events.clone(),
        })
    }

    /// The frontend dispatcher proved that this port-owned effect expired
    /// before it was released to process transport. This preserves the exact
    /// request in the actor ledger and permits the next bounded replay.
    pub fn definitely_unsent(mut self) -> Result<(), ParticipationStandDownError> {
        let result = self.publish(AbortIssueEventKind::DefinitelyUnsent);
        self.request.take();
        result
    }

    pub fn worker_settled(
        mut self,
        receipt: ParticipationReceipt,
    ) -> Result<(), ParticipationStandDownError> {
        let settlement = AbortParticipationWorkerSettlement::try_new(self.identity, receipt)?;
        let result = self.publish(AbortIssueEventKind::WorkerSettled(Box::new(settlement)));
        self.request.take();
        result
    }

    /// A local protocol or dispatcher violation makes replay unsafe.
    ///
    /// The actor stops authorizing this issue while its residual supervisor
    /// retains the participation responsibility until exact convergence evidence.
    pub fn fail_closed(mut self) -> Result<(), ParticipationStandDownError> {
        let result = self.publish(AbortIssueEventKind::FailedClosed);
        self.request.take();
        result
    }

    /// Another generation of this exact operation observed the definitive
    /// Worker receipt. The actor ledger has already consumed that fact, so
    /// this transport owner can relinquish its duplicate request silently.
    pub fn resolved_by_other_generation(mut self) {
        self.request.take();
    }

    fn publish(&self, kind: AbortIssueEventKind) -> Result<(), ParticipationStandDownError> {
        publish_event(
            &self.events,
            AbortIssueEvent {
                identity: self.identity,
                authorization_generation: self.authorization_generation,
                kind,
            },
        )
    }
}

impl Drop for AbortParticipationEffectSubmission {
    fn drop(&mut self) {
        if self.request.is_some() {
            let _ = self.publish(AbortIssueEventKind::TransportUnknown);
        }
    }
}

#[derive(Debug)]
pub struct LateAbortParticipationWorkerSettlement {
    identity: AbortParticipationIssueIdentity,
    authorization_generation: u64,
    events: mpsc::Sender<AbortIssueEvent>,
}

impl LateAbortParticipationWorkerSettlement {
    pub const fn identity(&self) -> AbortParticipationIssueIdentity {
        self.identity
    }

    pub fn worker_settled(
        self,
        receipt: ParticipationReceipt,
    ) -> Result<(), ParticipationStandDownError> {
        let settlement = AbortParticipationWorkerSettlement::try_new(self.identity, receipt)?;
        publish_event(
            &self.events,
            AbortIssueEvent {
                identity: self.identity,
                authorization_generation: self.authorization_generation,
                kind: AbortIssueEventKind::WorkerSettled(Box::new(settlement)),
            },
        )
    }

    /// A late receipt violated the exact operation/participation contract.
    /// Replaying the same request cannot repair that local protocol failure.
    pub fn fail_closed(self) -> Result<(), ParticipationStandDownError> {
        publish_event(
            &self.events,
            AbortIssueEvent {
                identity: self.identity,
                authorization_generation: self.authorization_generation,
                kind: AbortIssueEventKind::FailedClosed,
            },
        )
    }
}

fn publish_event(
    events: &mpsc::Sender<AbortIssueEvent>,
    event: AbortIssueEvent,
) -> Result<(), ParticipationStandDownError> {
    events.try_send(event).map_err(|error| match error {
        mpsc::error::TrySendError::Closed(_) => ParticipationStandDownError::EventChannelClosed,
        mpsc::error::TrySendError::Full(_) => {
            ParticipationStandDownError::EventBackpressureInvariant
        }
    })
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::sync::Mutex;

    use novarocks_execution_contract::{
        AcquireParticipationAdmissionTicket, AdmissionEpochCapability, AdmissionTicketId,
        LeaseValidFor, OperationOutcome, ParticipationAdmissionTicketReceipt,
    };
    use novarocks_types::NativeCompatibilityId;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
    };
    use novarocks_workload_control::{
        ResourceConfig, Stage, StagePermit, WorkClass, WorkOwner, WorkRequest, WorkloadConfig,
        WorkloadControl,
    };
    use std::time::Duration;
    use tokio::runtime::Handle;

    use super::*;

    fn execution() -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(71, 1), AttemptId::new(1).unwrap()).unwrap()
    }

    fn governed_work() -> (WorkOwner, StagePermit) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1 << 30,
                control_bytes: 1 << 20,
                per_scope_bytes: 1 << 28,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let stage = root.owner.scope().try_acquire(Stage::Execution).unwrap();
        (root.owner, stage)
    }

    fn activation() -> AttemptActivationIdentity {
        AttemptActivationIdentity::from_parts(11, execution(), 3)
    }

    fn participation(backend: BackendProcessId) -> ParticipationRef {
        ParticipationRef::new(execution(), FrontendProcessId::new_v7(), backend)
    }

    fn ledger(
        participations: impl IntoIterator<Item = ParticipationRef>,
    ) -> ParticipationStandDownLedger {
        ParticipationStandDownLedger::new(
            participations.into_iter().collect(),
            NonZeroUsize::new(3).unwrap(),
        )
        .unwrap()
    }

    #[derive(Debug)]
    struct RecordingPort {
        submissions: Arc<Mutex<Vec<AbortParticipationEffectSubmission>>>,
        backpressured: bool,
        capacity: watch::Sender<u64>,
    }

    impl Default for RecordingPort {
        fn default() -> Self {
            let (capacity, _) = watch::channel(0);
            Self {
                submissions: Arc::default(),
                backpressured: false,
                capacity,
            }
        }
    }

    #[derive(Debug)]
    struct RecordingReservation {
        submissions: Arc<Mutex<Vec<AbortParticipationEffectSubmission>>>,
    }

    impl AbortParticipationEffectReservation for RecordingReservation {
        fn submit(self: Box<Self>, submission: AbortParticipationEffectSubmission) {
            self.submissions.lock().unwrap().push(submission);
        }
    }

    impl AbortParticipationEffectPort for RecordingPort {
        fn subscribe_capacity(&self) -> watch::Receiver<u64> {
            self.capacity.subscribe()
        }

        fn try_reserve(
            &self,
            _identity: AbortParticipationIssueIdentity,
        ) -> AbortParticipationEffectAdmission {
            if self.backpressured {
                AbortParticipationEffectAdmission::Backpressured
            } else {
                AbortParticipationEffectAdmission::Admitted(Box::new(RecordingReservation {
                    submissions: Arc::clone(&self.submissions),
                }))
            }
        }
    }

    #[derive(Debug)]
    struct ClosedPort {
        capacity: watch::Sender<u64>,
    }

    impl Default for ClosedPort {
        fn default() -> Self {
            let (capacity, _) = watch::channel(0);
            Self { capacity }
        }
    }

    impl AbortParticipationEffectPort for ClosedPort {
        fn subscribe_capacity(&self) -> watch::Receiver<u64> {
            self.capacity.subscribe()
        }

        fn try_reserve(
            &self,
            _identity: AbortParticipationIssueIdentity,
        ) -> AbortParticipationEffectAdmission {
            AbortParticipationEffectAdmission::Closed
        }
    }

    #[test]
    fn no_grant_closes_without_abort_but_a_held_ticket_requires_abort() {
        let no_grant = participation(BackendProcessId::new_v7());
        let ticket_held = participation(BackendProcessId::new_v7());
        let mut ledger = ledger([no_grant, ticket_held]);
        ledger
            .begin(
                activation(),
                ParticipationStandDownCause::LogicalExecutionCancelled,
                [
                    (no_grant, EstablishStandDownFact::NoGrant),
                    (ticket_held, EstablishStandDownFact::AbortRequired),
                ],
            )
            .unwrap();

        assert_eq!(
            ledger.snapshot(no_grant).unwrap().closure(),
            ParticipationClosureState::NoRemoteResponsibility
        );
        assert_eq!(
            ledger.snapshot(ticket_held).unwrap().closure(),
            ParticipationClosureState::AbortPending
        );
        assert!(ledger.authorize_next().unwrap().is_some());
        assert!(!ledger.responsibility_settled());
    }

    #[test]
    fn duplicate_fact_rows_are_rejected_before_map_deduplication() {
        let first = participation(BackendProcessId::new_v7());
        let second = participation(BackendProcessId::new_v7());
        let mut ledger = ledger([first, second]);
        assert_eq!(
            ledger.begin(
                activation(),
                ParticipationStandDownCause::LogicalExecutionCancelled,
                [
                    (first, EstablishStandDownFact::NoGrant),
                    (first, EstablishStandDownFact::AbortRequired),
                    (second, EstablishStandDownFact::NoGrant),
                ],
            ),
            Err(ParticipationStandDownError::DuplicateParticipation)
        );
    }

    #[test]
    fn definitely_unsent_establish_still_requires_abort_for_its_ticket() {
        let participation = participation(BackendProcessId::new_v7());
        let mut ledger = ledger([participation]);
        ledger
            .begin(
                activation(),
                ParticipationStandDownCause::LogicalExecutionCancelled,
                [(participation, EstablishStandDownFact::AbortRequired)],
            )
            .unwrap();

        let permit = ledger.authorize_next().unwrap().unwrap();
        assert_eq!(permit.identity().participation(), participation);
        drop(permit);
        ledger.drain_events().unwrap();
        assert_eq!(
            ledger.snapshot(participation).unwrap().issue_state(),
            Some(AbortParticipationIssueState::DefinitelyUnsent)
        );
        assert!(ledger.authorize_next().unwrap().is_some());
    }

    #[test]
    fn abort_authorization_is_round_robin_across_participations() {
        let first = participation(BackendProcessId::new_v7());
        let second = participation(BackendProcessId::new_v7());
        let participations = BTreeSet::from([first, second]);
        let expected_first = *participations.first().unwrap();
        let expected_second = *participations.last().unwrap();
        let mut ledger =
            ParticipationStandDownLedger::new(participations, NonZeroUsize::new(2).unwrap())
                .unwrap();
        ledger
            .begin(
                activation(),
                ParticipationStandDownCause::LogicalExecutionCancelled,
                [
                    (first, EstablishStandDownFact::AbortRequired),
                    (second, EstablishStandDownFact::AbortRequired),
                ],
            )
            .unwrap();

        let first_issue = ledger.authorize_next().unwrap().unwrap();
        assert_eq!(first_issue.identity().participation(), expected_first);
        drop(first_issue);
        ledger.drain_events().unwrap();

        let next = ledger.authorize_next().unwrap().unwrap();
        assert_eq!(next.identity().participation(), expected_second);
    }

    #[test]
    fn transport_unknown_retries_wait_for_one_aggregated_deadline() {
        let first = participation(BackendProcessId::new_v7());
        let second = participation(BackendProcessId::new_v7());
        let participations = BTreeSet::from([first, second]);
        let expected_first = *participations.first().unwrap();
        let expected_second = *participations.last().unwrap();
        let mut ledger =
            ParticipationStandDownLedger::new(participations, NonZeroUsize::new(2).unwrap())
                .unwrap();
        ledger
            .begin(
                activation(),
                ParticipationStandDownCause::LogicalExecutionCancelled,
                [
                    (first, EstablishStandDownFact::AbortRequired),
                    (second, EstablishStandDownFact::AbortRequired),
                ],
            )
            .unwrap();
        let port = RecordingPort::default();
        let now = MonotonicInstant::from_origin(Duration::from_secs(1));

        ledger
            .authorize_next_at(now)
            .unwrap()
            .unwrap()
            .try_submit(&port)
            .unwrap();
        let first_submission = port.submissions.lock().unwrap().pop().unwrap();
        assert_eq!(first_submission.identity().participation(), expected_first);
        let _first_late = first_submission.transport_unknown().unwrap();
        ledger.drain_events_at(now).unwrap();

        ledger
            .authorize_next_at(now)
            .unwrap()
            .unwrap()
            .try_submit(&port)
            .unwrap();
        let second_submission = port.submissions.lock().unwrap().pop().unwrap();
        assert_eq!(
            second_submission.identity().participation(),
            expected_second
        );
        let _second_late = second_submission.transport_unknown().unwrap();
        ledger.drain_events_at(now).unwrap();

        assert!(ledger.authorize_next_at(now).unwrap().is_none());
        let retry_at = now.saturating_add(ABORT_RETRY_BACKOFF);
        assert_eq!(ledger.next_retry_at(), Some(retry_at));
        let before_retry = MonotonicInstant::from_origin(
            retry_at
                .since_origin()
                .saturating_sub(Duration::from_millis(1)),
        );
        assert!(ledger.authorize_next_at(before_retry).unwrap().is_none());
        assert_eq!(
            ledger
                .authorize_next_at(retry_at)
                .unwrap()
                .unwrap()
                .identity()
                .participation(),
            expected_first
        );
    }

    #[tokio::test]
    async fn retry_deadline_uses_the_clock_after_the_event_arrives() {
        let participation = participation(BackendProcessId::new_v7());
        let mut ledger = ledger([participation]);
        ledger
            .begin(
                activation(),
                ParticipationStandDownCause::LogicalExecutionCancelled,
                [(participation, EstablishStandDownFact::AbortRequired)],
            )
            .unwrap();
        let port = RecordingPort::default();
        ledger
            .authorize_next_at(MonotonicInstant::ORIGIN)
            .unwrap()
            .unwrap()
            .try_submit(&port)
            .unwrap();
        ledger.drain_events_at(MonotonicInstant::ORIGIN).unwrap();
        let submission = port.submissions.lock().unwrap().pop().unwrap();
        let clock_reads = Cell::new(0);
        let arrived_at = MonotonicInstant::from_origin(Duration::from_secs(9));
        let mut receive = Box::pin(ledger.apply_next_event(|| {
            clock_reads.set(clock_reads.get() + 1);
            arrived_at
        }));

        assert!(
            tokio::time::timeout(Duration::from_millis(1), &mut receive)
                .await
                .is_err()
        );
        assert_eq!(clock_reads.get(), 0);
        let _late = submission.transport_unknown().unwrap();
        receive.await.unwrap();

        assert_eq!(clock_reads.get(), 1);
        assert_eq!(
            ledger.next_retry_at(),
            Some(arrived_at.saturating_add(ABORT_RETRY_BACKOFF))
        );
    }

    #[test]
    fn one_participation_exhausting_abort_replay_does_not_block_the_next_participation() {
        let first = participation(BackendProcessId::new_v7());
        let second = participation(BackendProcessId::new_v7());
        let participations = BTreeSet::from([first, second]);
        let expected_first = *participations.first().unwrap();
        let expected_second = *participations.last().unwrap();
        let mut ledger =
            ParticipationStandDownLedger::new(participations, NonZeroUsize::new(1).unwrap())
                .unwrap();
        ledger
            .begin(
                activation(),
                ParticipationStandDownCause::LogicalExecutionCancelled,
                [
                    (first, EstablishStandDownFact::AbortRequired),
                    (second, EstablishStandDownFact::AbortRequired),
                ],
            )
            .unwrap();

        let exhausted = ledger.authorize_next().unwrap().unwrap();
        assert_eq!(exhausted.identity().participation(), expected_first);
        drop(exhausted);
        ledger.drain_events().unwrap();

        let next = ledger.authorize_next().unwrap().unwrap();
        assert_eq!(next.identity().participation(), expected_second);
        drop(next);
        ledger.drain_events().unwrap();
        assert!(ledger.authorize_next().unwrap().is_none());
        assert_eq!(
            ledger.snapshot(expected_first).unwrap().issue_state(),
            Some(AbortParticipationIssueState::AuthorizationExhausted)
        );
        assert_eq!(
            ledger.snapshot(expected_first).unwrap().closure(),
            ParticipationClosureState::AbortIssueExhausted
        );
    }

    #[test]
    fn exact_process_replacement_settles_exhausted_residual_responsibility() {
        let participation = participation(BackendProcessId::new_v7());
        let mut ledger = ParticipationStandDownLedger::new(
            [participation].into_iter().collect(),
            NonZeroUsize::MIN,
        )
        .unwrap();
        ledger
            .begin(
                activation(),
                ParticipationStandDownCause::LogicalExecutionCancelled,
                [(participation, EstablishStandDownFact::AbortRequired)],
            )
            .unwrap();
        drop(ledger.authorize_next().unwrap().unwrap());
        ledger.drain_events().unwrap();
        assert!(ledger.authorize_next().unwrap().is_none());
        assert_eq!(
            ledger.snapshot(participation).unwrap().closure(),
            ParticipationClosureState::AbortIssueExhausted
        );
        ledger
            .observe_registry_convergence(
                participation,
                RegistryParticipationConvergence::WorkerProcessReplaced,
            )
            .unwrap();
        assert_eq!(
            ledger.snapshot(participation).unwrap().closure(),
            ParticipationClosureState::WorkerProcessReplaced
        );
        assert!(ledger.responsibility_settled());
    }

    #[test]
    fn authorized_establish_must_linearize_before_abort_is_minted() {
        let participation = participation(BackendProcessId::new_v7());
        let mut ledger = ledger([participation]);
        ledger
            .begin(
                activation(),
                ParticipationStandDownCause::LogicalExecutionCancelled,
                [(participation, EstablishStandDownFact::IssueUnsettled)],
            )
            .unwrap();
        assert!(ledger.authorize_next().unwrap().is_none());
        assert!(ledger.has_unsettled_establish_issue());

        ledger
            .refresh_establish_fact(participation, EstablishStandDownFact::AbortRequired)
            .unwrap();
        assert!(ledger.authorize_next().unwrap().is_some());
    }

    #[test]
    fn effect_port_reserves_before_receiving_the_exact_replay_request() {
        let participation = participation(BackendProcessId::new_v7());
        let mut ledger = ledger([participation]);
        ledger
            .begin(
                activation(),
                ParticipationStandDownCause::LogicalExecutionFailed,
                [(participation, EstablishStandDownFact::AbortRequired)],
            )
            .unwrap();
        let mut port = RecordingPort {
            backpressured: true,
            ..RecordingPort::default()
        };
        let permit = ledger.authorize_next().unwrap().unwrap();
        let identity = permit.identity();
        let permit = match permit.try_submit(&port).unwrap() {
            AbortParticipationIssueSubmit::Backpressured(permit) => permit,
            AbortParticipationIssueSubmit::Accepted => panic!("backpressured port accepted Abort"),
        };
        assert!(port.submissions.lock().unwrap().is_empty());

        port.backpressured = false;
        assert!(matches!(
            permit.try_submit(&port).unwrap(),
            AbortParticipationIssueSubmit::Accepted
        ));
        let submission = port.submissions.lock().unwrap().pop().unwrap();
        assert_eq!(submission.identity(), identity);
        assert_eq!(submission.request().identity(), identity);
    }

    #[test]
    fn a_closed_effect_port_fails_instead_of_waiting_for_capacity() {
        let participation = participation(BackendProcessId::new_v7());
        let mut ledger = ledger([participation]);
        ledger
            .begin(
                activation(),
                ParticipationStandDownCause::LogicalExecutionCancelled,
                [(participation, EstablishStandDownFact::AbortRequired)],
            )
            .unwrap();
        let permit = ledger.authorize_next().unwrap().unwrap();
        let error = permit.try_submit(&ClosedPort::default()).unwrap_err();
        assert_eq!(error, ParticipationStandDownError::EffectCapacityClosed);
        use novarocks_execution_contract::failure::{FailureEvidence, FailureProjection};
        assert_eq!(
            error.failure_fact().evidence(),
            FailureEvidence::StopObservation
        );
        let actor_error = crate::coordination::LogicalExecutionActorError::StandDown(error);
        assert_eq!(actor_error.failure_fact(), error.failure_fact());
        assert_eq!(actor_error.safe_detail(), error.safe_detail());
    }

    #[test]
    fn transport_unknown_replays_the_same_operation_and_request_allocation() {
        let participation = participation(BackendProcessId::new_v7());
        let mut ledger = ledger([participation]);
        ledger
            .begin(
                activation(),
                ParticipationStandDownCause::LogicalExecutionCancelled,
                [(participation, EstablishStandDownFact::AbortRequired)],
            )
            .unwrap();
        let port = RecordingPort::default();
        let permit = ledger.authorize_next().unwrap().unwrap();
        permit.try_submit(&port).unwrap();
        let submission = port.submissions.lock().unwrap().pop().unwrap();
        let identity = submission.identity();
        let request = Arc::clone(submission.request.as_ref().unwrap());
        let _late = submission.transport_unknown().unwrap();
        ledger.drain_events().unwrap();

        let replay = ledger.authorize_next().unwrap().unwrap();
        replay.try_submit(&port).unwrap();
        let replayed = port.submissions.lock().unwrap().pop().unwrap();
        assert_eq!(replayed.identity(), identity);
        assert!(Arc::ptr_eq(replayed.request.as_ref().unwrap(), &request));
    }

    #[test]
    fn terminal_worker_settlement_finishes_abort_responsibility_only() {
        let participation = participation(BackendProcessId::new_v7());
        let mut ledger = ledger([participation]);
        ledger
            .begin(
                activation(),
                ParticipationStandDownCause::LogicalExecutionCancelled,
                [(participation, EstablishStandDownFact::AbortRequired)],
            )
            .unwrap();
        let port = RecordingPort::default();
        ledger
            .authorize_next()
            .unwrap()
            .unwrap()
            .try_submit(&port)
            .unwrap();
        let submission = port.submissions.lock().unwrap().pop().unwrap();
        submission
            .worker_settled(ParticipationReceipt::new(
                participation,
                ParticipationState::TerminalRetained,
            ))
            .unwrap();
        ledger.drain_events().unwrap();

        let snapshot = ledger.snapshot(participation).unwrap();
        assert_eq!(
            snapshot.closure(),
            ParticipationClosureState::TerminalRetained
        );
        assert!(ledger.responsibility_settled());
        // TerminalRetained is deliberately not an actual-stop or resource-
        // release fact; this ledger has no API that can manufacture either.
    }

    #[test]
    fn worker_receipt_closure_accepts_all_worker_closing_states() {
        for (state, expected) in [
            (
                ParticipationState::Aborting,
                ParticipationClosureState::Aborting,
            ),
            (
                ParticipationState::Releasing,
                ParticipationClosureState::Releasing,
            ),
            (
                ParticipationState::TerminalRetained,
                ParticipationClosureState::TerminalRetained,
            ),
            (ParticipationState::Gone, ParticipationClosureState::Gone),
        ] {
            let participation = participation(BackendProcessId::new_v7());
            let mut ledger = ledger([participation]);
            ledger
                .begin(
                    activation(),
                    ParticipationStandDownCause::LogicalExecutionCancelled,
                    [(participation, EstablishStandDownFact::AbortRequired)],
                )
                .unwrap();
            let port = RecordingPort::default();
            ledger
                .authorize_next()
                .unwrap()
                .unwrap()
                .try_submit(&port)
                .unwrap();
            port.submissions
                .lock()
                .unwrap()
                .pop()
                .unwrap()
                .worker_settled(ParticipationReceipt::new(participation, state))
                .unwrap();
            ledger.drain_events().unwrap();

            assert_eq!(ledger.snapshot(participation).unwrap().closure(), expected);
            assert!(ledger.responsibility_settled());
        }
    }

    #[test]
    fn late_stale_abort_reply_does_not_regress_terminal_closure() {
        let participation = participation(BackendProcessId::new_v7());
        let mut ledger = ledger([participation]);
        ledger
            .begin(
                activation(),
                ParticipationStandDownCause::LogicalExecutionCancelled,
                [(participation, EstablishStandDownFact::AbortRequired)],
            )
            .unwrap();
        let port = RecordingPort::default();
        ledger
            .authorize_next()
            .unwrap()
            .unwrap()
            .try_submit(&port)
            .unwrap();
        let first = port.submissions.lock().unwrap().pop().unwrap();
        let late = first.transport_unknown().unwrap();
        ledger.drain_events().unwrap();

        ledger
            .authorize_next()
            .unwrap()
            .unwrap()
            .try_submit(&port)
            .unwrap();
        port.submissions
            .lock()
            .unwrap()
            .pop()
            .unwrap()
            .worker_settled(ParticipationReceipt::new(
                participation,
                ParticipationState::TerminalRetained,
            ))
            .unwrap();
        ledger.drain_events().unwrap();

        late.worker_settled(ParticipationReceipt::new(
            participation,
            ParticipationState::Aborting,
        ))
        .unwrap();
        ledger.drain_events().unwrap();
        assert_eq!(
            ledger.snapshot(participation).unwrap().closure(),
            ParticipationClosureState::TerminalRetained
        );
    }

    #[test]
    fn late_settlement_allows_an_actor_held_replay_permit_to_close_unsent() {
        let participation = participation(BackendProcessId::new_v7());
        let mut ledger = ledger([participation]);
        ledger
            .begin(
                activation(),
                ParticipationStandDownCause::LogicalExecutionCancelled,
                [(participation, EstablishStandDownFact::AbortRequired)],
            )
            .unwrap();
        let port = RecordingPort::default();
        ledger
            .authorize_next()
            .unwrap()
            .unwrap()
            .try_submit(&port)
            .unwrap();
        let first = port.submissions.lock().unwrap().pop().unwrap();
        let late = first.transport_unknown().unwrap();
        ledger.drain_events().unwrap();
        let replay = ledger.authorize_next().unwrap().unwrap();

        late.worker_settled(ParticipationReceipt::new(
            participation,
            ParticipationState::TerminalRetained,
        ))
        .unwrap();
        ledger.drain_events().unwrap();
        replay.settle_definitely_unsent().unwrap();
        ledger.drain_events().unwrap();

        let snapshot = ledger.snapshot(participation).unwrap();
        assert_eq!(
            snapshot.closure(),
            ParticipationClosureState::TerminalRetained
        );
        assert_eq!(
            snapshot.issue_state(),
            Some(AbortParticipationIssueState::WorkerSettled)
        );
    }

    #[test]
    fn exact_abort_replies_cannot_switch_closure_branches() {
        let participation = participation(BackendProcessId::new_v7());
        let mut ledger = ledger([participation]);
        ledger
            .begin(
                activation(),
                ParticipationStandDownCause::LogicalExecutionCancelled,
                [(participation, EstablishStandDownFact::AbortRequired)],
            )
            .unwrap();
        let port = RecordingPort::default();
        ledger
            .authorize_next()
            .unwrap()
            .unwrap()
            .try_submit(&port)
            .unwrap();
        let first = port.submissions.lock().unwrap().pop().unwrap();
        let late = first.transport_unknown().unwrap();
        ledger.drain_events().unwrap();

        ledger
            .authorize_next()
            .unwrap()
            .unwrap()
            .try_submit(&port)
            .unwrap();
        port.submissions
            .lock()
            .unwrap()
            .pop()
            .unwrap()
            .worker_settled(ParticipationReceipt::new(
                participation,
                ParticipationState::Releasing,
            ))
            .unwrap();
        ledger.drain_events().unwrap();

        late.worker_settled(ParticipationReceipt::new(
            participation,
            ParticipationState::Aborting,
        ))
        .unwrap();
        assert_eq!(
            ledger.drain_events().unwrap_err(),
            ParticipationStandDownError::ConflictingWorkerSettlement
        );
    }

    #[test]
    fn terminal_closure_retains_the_first_observed_abort_branch() {
        let participation = participation(BackendProcessId::new_v7());
        let mut ledger = ledger([participation]);
        ledger
            .begin(
                activation(),
                ParticipationStandDownCause::LogicalExecutionCancelled,
                [(participation, EstablishStandDownFact::AbortRequired)],
            )
            .unwrap();
        let port = RecordingPort::default();

        ledger
            .authorize_next()
            .unwrap()
            .unwrap()
            .try_submit(&port)
            .unwrap();
        let first = port.submissions.lock().unwrap().pop().unwrap();
        let first_late = first.transport_unknown().unwrap();
        ledger.drain_events().unwrap();
        ledger
            .authorize_next()
            .unwrap()
            .unwrap()
            .try_submit(&port)
            .unwrap();
        let second = port.submissions.lock().unwrap().pop().unwrap();
        let second_late = second.transport_unknown().unwrap();
        ledger.drain_events().unwrap();
        ledger
            .authorize_next()
            .unwrap()
            .unwrap()
            .try_submit(&port)
            .unwrap();
        port.submissions
            .lock()
            .unwrap()
            .pop()
            .unwrap()
            .worker_settled(ParticipationReceipt::new(
                participation,
                ParticipationState::TerminalRetained,
            ))
            .unwrap();
        ledger.drain_events().unwrap();

        second_late
            .worker_settled(ParticipationReceipt::new(
                participation,
                ParticipationState::Aborting,
            ))
            .unwrap();
        ledger.drain_events().unwrap();
        assert_eq!(
            ledger.snapshot(participation).unwrap().closure(),
            ParticipationClosureState::TerminalRetained
        );

        first_late
            .worker_settled(ParticipationReceipt::new(
                participation,
                ParticipationState::Releasing,
            ))
            .unwrap();
        assert_eq!(
            ledger.drain_events().unwrap_err(),
            ParticipationStandDownError::ConflictingWorkerSettlement
        );
    }

    #[tokio::test]
    async fn actor_owner_waits_for_abort_settlement_after_logical_cancellation() {
        let execution = execution();
        let participation = participation(BackendProcessId::new_v7());
        let port = Arc::new(RecordingPort::default());
        let (work_owner, stage) = governed_work();
        let config = crate::coordination::LogicalExecutionActorConfig::single_attempt_completion(
            execution,
            novarocks_coordination_contract::ExecutionEffect::None,
            NonZeroUsize::new(2).unwrap(),
            vec![participation],
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            work_owner,
            stage,
        )
        .unwrap()
        .with_abort_participation_effect_port(
            Arc::clone(&port) as Arc<dyn AbortParticipationEffectPort>,
            NonZeroUsize::new(2).unwrap(),
        );
        let (owner, initial, _output) =
            crate::coordination::spawn_logical_execution_actor(&Handle::current(), config)
                .unwrap()
                .into_parts();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let admission = AcquireParticipationAdmissionTicket::new(
            TaskOperationId::new_v7(),
            participation,
            LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
            NativeCompatibilityId::new([7; 32]),
            AdmissionEpochCapability::try_from_bytes([7; 16]).unwrap(),
        );
        let activation = running.identity();
        let pending_issue = running.begin_admission_issue(admission).await.unwrap();
        drop(running);

        assert_eq!(
            actor
                .settle_late_admission_issue(
                    activation,
                    pending_issue,
                    crate::coordination::AdmissionIssueSettlement::applied(
                        pending_issue.operation_id(),
                        OperationOutcome::Accepted,
                        ParticipationAdmissionTicketReceipt::new(
                            AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
                            participation,
                            LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
                        ),
                    )
                    .unwrap(),
                )
                .await
                .unwrap(),
            crate::coordination::AdmissionIssueDisposition::Granted
        );

        let submission = loop {
            if let Some(submission) = port.submissions.lock().unwrap().pop() {
                break submission;
            }
            tokio::task::yield_now().await;
        };
        assert_eq!(
            actor.snapshot().await.unwrap().conclusion,
            Some(crate::coordination::LogicalConclusion::Cancelled)
        );
        assert_eq!(
            actor
                .stand_down_snapshot(participation)
                .await
                .unwrap()
                .unwrap()
                .closure(),
            ParticipationClosureState::AbortPending
        );

        submission
            .worker_settled(ParticipationReceipt::new(
                participation,
                ParticipationState::TerminalRetained,
            ))
            .unwrap();
        loop {
            if actor
                .stand_down_snapshot(participation)
                .await
                .unwrap()
                .is_some_and(|snapshot| {
                    snapshot.closure() == ParticipationClosureState::TerminalRetained
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        let supervisor = owner.into_residual_stand_down_supervisor();
        drop(actor);
        assert!(
            !supervisor.is_finished(),
            "a terminal record does not prove actual stop or process replacement"
        );
        supervisor
            .observe_worker_stopped_and_participation_fenced(participation)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), supervisor.join())
            .await
            .expect("positive Registry convergence lets the residual supervisor finish")
            .unwrap();
    }

    #[tokio::test]
    async fn actor_with_remote_participations_requires_an_abort_effect_port() {
        let execution = execution();
        let participation = participation(BackendProcessId::new_v7());
        let (work_owner, stage) = governed_work();
        let config = crate::coordination::LogicalExecutionActorConfig::single_attempt_completion(
            execution,
            novarocks_coordination_contract::ExecutionEffect::None,
            NonZeroUsize::new(1).unwrap(),
            vec![participation],
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            work_owner,
            stage,
        )
        .unwrap();
        assert!(matches!(
            crate::coordination::spawn_logical_execution_actor(&Handle::current(), config),
            Err(crate::coordination::LogicalExecutionActorError::InvariantViolation)
        ));
    }
}
