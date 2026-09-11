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

//! Actor-owned query-context stand-down responsibility.
//!
//! The ledger owns application semantics only. Native operation carriers,
//! codecs, RPC retries, Worker task termination, and resource-release facts
//! remain outside this module. A context closure state says how far the Worker
//! context lifecycle has advanced; it is not proof that a process stopped or
//! that all physical resources were released.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use novarocks_execution_contract::{
    QueryContextReceipt, QueryContextRef, QueryContextState, TaskOperationId,
};
use tokio::sync::mpsc;
use tokio::sync::watch;

use super::{AttemptActivationIdentity, MonotonicInstant};

const DEFAULT_ABORT_RETRY_BACKOFF: Duration = Duration::from_millis(50);

/// Why the logical execution requires its remote contexts to stand down.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ContextStandDownCause {
    LogicalExecutionCancelled,
    LogicalExecutionFailed,
}

/// What Establish/admission currently proves about a context's remote effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EstablishStandDownFact {
    /// No admission grant was ever received, so no Worker capacity or context
    /// can remain on behalf of this logical execution.
    NoGrant,
    /// An authorized Establish has not yet linearized to definitely-unsent or
    /// transport-owned. Cancellation must wait for that exact transition.
    IssueUnsettled,
    /// A Worker admission ticket was granted. Abort remains required even if
    /// Establish was definitely unsent: Worker Abort establishes the absent
    /// context fence and returns the ticket's reserved capacity.
    AbortRequired,
}

/// Exact, secret-free identity of one actor-owned Abort effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AbortQueryContextIssueIdentity {
    activation: AttemptActivationIdentity,
    operation_id: TaskOperationId,
    context: QueryContextRef,
    cause: ContextStandDownCause,
}

impl AbortQueryContextIssueIdentity {
    pub const fn activation(self) -> AttemptActivationIdentity {
        self.activation
    }

    pub const fn operation_id(self) -> TaskOperationId {
        self.operation_id
    }

    pub const fn context(self) -> QueryContextRef {
        self.context
    }

    pub const fn cause(self) -> ContextStandDownCause {
        self.cause
    }
}

/// Query-application request projected into the Abort effect port.
///
/// Frontend composition translates this value into its Native carrier. The
/// exact allocation is retained by the actor across transport-unknown replay.
#[derive(Debug)]
pub struct AbortQueryContextEffectRequest {
    identity: AbortQueryContextIssueIdentity,
}

impl AbortQueryContextEffectRequest {
    pub const fn identity(&self) -> AbortQueryContextIssueIdentity {
        self.identity
    }
}

/// Worker context-lifecycle progress observed while standing down.
///
/// These values do not assert Worker process exit, task-thread exit, or
/// physical resource release. Those remain separately observed convergence
/// facts owned by coordination and workload governance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContextClosureState {
    /// The actor proved that no admission operation could have left Worker
    /// capacity or a context behind. This is a local responsibility result,
    /// not an observation that the Worker reported `Gone`.
    NoRemoteResponsibility,
    AwaitingEstablishIssue,
    AbortPending,
    /// The actor exhausted this context's bounded Abort issue budget. The
    /// residual supervisor must retain the exact context responsibility and
    /// may still apply later Worker or process-fence evidence.
    AbortIssueExhausted,
    Aborting,
    Releasing,
    TerminalRetained,
    Gone,
    /// The Registry proved actual stop together with closed normal admission
    /// and invalidated old admission authority for this exact context.
    WorkerStoppedAndContextFenced,
    /// The Registry observed replacement of the exact Worker process named by
    /// this context. This fences the old process identity but does not claim
    /// that its resources were released cleanly.
    WorkerProcessReplaced,
}

impl ContextClosureState {
    pub const fn stand_down_responsibility_settled(self) -> bool {
        matches!(
            self,
            Self::NoRemoteResponsibility
                | Self::Aborting
                | Self::Releasing
                | Self::TerminalRetained
                | Self::Gone
                | Self::WorkerStoppedAndContextFenced
                | Self::WorkerProcessReplaced
        )
    }

    pub const fn residual_resource_settled(self) -> bool {
        matches!(
            self,
            Self::NoRemoteResponsibility
                | Self::WorkerStoppedAndContextFenced
                | Self::WorkerProcessReplaced
        )
    }
}

/// Exact settlement produced by the Worker-facing Abort adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
struct AbortQueryContextWorkerSettlement {
    identity: AbortQueryContextIssueIdentity,
    receipt: QueryContextReceipt,
}

impl AbortQueryContextWorkerSettlement {
    fn try_new(
        identity: AbortQueryContextIssueIdentity,
        receipt: QueryContextReceipt,
    ) -> Result<Self, ContextStandDownError> {
        if receipt.context() != identity.context() {
            return Err(ContextStandDownError::IdentityMismatch);
        }
        if worker_closure(receipt.state()).is_none() {
            return Err(ContextStandDownError::InvalidWorkerSettlement);
        }
        Ok(Self { identity, receipt })
    }

    const fn identity(&self) -> AbortQueryContextIssueIdentity {
        self.identity
    }

    fn closure(&self) -> ContextClosureState {
        worker_closure(self.receipt.state()).expect("validated Abort Worker settlement")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AbortQueryContextIssueState {
    IssueAuthorized,
    TransportOwned,
    DefinitelyUnsent,
    TransportUnknown,
    WorkerSettled,
    /// The effect owner observed a local protocol or dispatcher violation.
    /// Replaying cannot repair such a violation, so the context remains a
    /// residual responsibility until Worker-stop or process-fence evidence.
    FailedClosed,
    AuthorizationExhausted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContextStandDownSnapshot {
    closure: ContextClosureState,
    identity: Option<AbortQueryContextIssueIdentity>,
    issue_state: Option<AbortQueryContextIssueState>,
}

impl ContextStandDownSnapshot {
    pub const fn closure(self) -> ContextClosureState {
        self.closure
    }

    pub const fn identity(self) -> Option<AbortQueryContextIssueIdentity> {
        self.identity
    }

    pub const fn issue_state(self) -> Option<AbortQueryContextIssueState> {
        self.issue_state
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContextStandDownError {
    DuplicateContext,
    ActivationExecutionMismatch,
    UnknownContext,
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

impl fmt::Display for ContextStandDownError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::DuplicateContext => "stand-down context set contains a duplicate identity",
            Self::ActivationExecutionMismatch => {
                "stand-down activation and query context name different executions"
            }
            Self::UnknownContext => "stand-down event names an unknown query context",
            Self::StandDownAlreadyStarted => "query-context stand-down was already started",
            Self::StandDownNotStarted => "query-context stand-down has not started",
            Self::EstablishIssueStillUnsettled => {
                "Establish issue has not linearized before stand-down"
            }
            Self::WrongState => "query-context stand-down action is illegal in the current state",
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
                "Abort Worker receipt does not prove a closing context state"
            }
            Self::ClosureRegression => "query-context closure observation regressed",
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

impl std::error::Error for ContextStandDownError {}

#[derive(Debug)]
struct ContextStandDownRecord {
    activation: AttemptActivationIdentity,
    cause: Option<ContextStandDownCause>,
    closure: ContextClosureState,
    request: Option<Arc<AbortQueryContextEffectRequest>>,
    identity: Option<AbortQueryContextIssueIdentity>,
    issue_state: Option<AbortQueryContextIssueState>,
    authorization_generation: u64,
    worker_settled: bool,
    worker_branch: Option<WorkerClosureBranch>,
    retry_not_before: Option<super::MonotonicInstant>,
    registry_fenced: bool,
}

impl ContextStandDownRecord {
    fn snapshot(&self) -> ContextStandDownSnapshot {
        ContextStandDownSnapshot {
            closure: self.closure,
            identity: self.identity,
            issue_state: self.issue_state,
        }
    }

    fn require_abort(&mut self, context: QueryContextRef) {
        if self.identity.is_none() {
            let Some(cause) = self.cause else {
                if matches!(self.closure, ContextClosureState::AwaitingEstablishIssue) {
                    self.closure = ContextClosureState::AbortPending;
                }
                return;
            };
            let identity = AbortQueryContextIssueIdentity {
                activation: self.activation,
                operation_id: TaskOperationId::new_v7(),
                context,
                cause,
            };
            self.identity = Some(identity);
            self.request = Some(Arc::new(AbortQueryContextEffectRequest { identity }));
        }
        if matches!(self.closure, ContextClosureState::AwaitingEstablishIssue) {
            self.closure = ContextClosureState::AbortPending;
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AbortIssueEventKind {
    TransportOwned,
    DefinitelyUnsent,
    TransportUnknown,
    WorkerSettled(AbortQueryContextWorkerSettlement),
    FailedClosed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AbortIssueEvent {
    identity: AbortQueryContextIssueIdentity,
    authorization_generation: u64,
    kind: AbortIssueEventKind,
}

/// Pure state owned by one logical-execution actor.
///
/// The event channel is physically bounded to three fixed-size events per
/// context and authorization: transport ownership, one transport disposition,
/// and one possible late Worker settlement. Authorization budget exhaustion
/// therefore applies backpressure without an unbounded task or thread queue.
#[derive(Debug)]
pub(crate) struct ContextStandDownLedger {
    required_contexts: BTreeSet<QueryContextRef>,
    max_authorizations_per_context: NonZeroUsize,
    records: BTreeMap<QueryContextRef, ContextStandDownRecord>,
    events: mpsc::Sender<AbortIssueEvent>,
    event_rx: mpsc::Receiver<AbortIssueEvent>,
    started: bool,
    cause: Option<ContextStandDownCause>,
    authorization_cursor: Option<QueryContextRef>,
    retry_backoff: Duration,
}

impl ContextStandDownLedger {
    pub(crate) fn new(
        required_contexts: BTreeSet<QueryContextRef>,
        max_authorizations_per_context: NonZeroUsize,
    ) -> Result<Self, ContextStandDownError> {
        let event_capacity = required_contexts
            .len()
            .max(1)
            .checked_mul(max_authorizations_per_context.get())
            .and_then(|capacity| capacity.checked_mul(3))
            .ok_or(ContextStandDownError::EventCapacityOverflow)?;
        let (events, event_rx) = mpsc::channel(event_capacity);
        Ok(Self {
            required_contexts,
            max_authorizations_per_context,
            records: BTreeMap::new(),
            events,
            event_rx,
            started: false,
            cause: None,
            authorization_cursor: None,
            retry_backoff: DEFAULT_ABORT_RETRY_BACKOFF,
        })
    }

    pub(crate) fn begin(
        &mut self,
        activation: AttemptActivationIdentity,
        cause: ContextStandDownCause,
        facts: impl IntoIterator<Item = (QueryContextRef, EstablishStandDownFact)>,
    ) -> Result<(), ContextStandDownError> {
        self.begin_with_cause(activation, Some(cause), facts)
    }

    pub(crate) fn begin_successful_cleanup(
        &mut self,
        activation: AttemptActivationIdentity,
        facts: impl IntoIterator<Item = (QueryContextRef, EstablishStandDownFact)>,
    ) -> Result<(), ContextStandDownError> {
        self.begin_with_cause(activation, None, facts)
    }

    fn begin_with_cause(
        &mut self,
        activation: AttemptActivationIdentity,
        cause: Option<ContextStandDownCause>,
        facts: impl IntoIterator<Item = (QueryContextRef, EstablishStandDownFact)>,
    ) -> Result<(), ContextStandDownError> {
        if self.started {
            return Err(ContextStandDownError::StandDownAlreadyStarted);
        }
        let mut collected = BTreeMap::new();
        for (context, fact) in facts {
            if collected.insert(context, fact).is_some() {
                return Err(ContextStandDownError::DuplicateContext);
            }
        }
        if collected
            .keys()
            .any(|context| !self.required_contexts.contains(context))
        {
            return Err(ContextStandDownError::UnknownContext);
        }
        if collected.len() != self.required_contexts.len() {
            return Err(ContextStandDownError::UnknownContext);
        }
        for context in &self.required_contexts {
            if context.query_execution_id() != activation.execution() {
                return Err(ContextStandDownError::ActivationExecutionMismatch);
            }
            let fact = *collected
                .get(context)
                .ok_or(ContextStandDownError::UnknownContext)?;
            let closure = match fact {
                EstablishStandDownFact::NoGrant => ContextClosureState::NoRemoteResponsibility,
                EstablishStandDownFact::IssueUnsettled => {
                    ContextClosureState::AwaitingEstablishIssue
                }
                EstablishStandDownFact::AbortRequired => ContextClosureState::AbortPending,
            };
            let mut record = ContextStandDownRecord {
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
                record.require_abort(*context);
            }
            self.records.insert(*context, record);
        }
        self.cause = cause;
        self.started = true;
        Ok(())
    }

    pub(crate) const fn started(&self) -> bool {
        self.started
    }

    pub(crate) fn is_successful_cleanup(&self) -> bool {
        self.started && self.cause.is_none()
    }

    pub(crate) fn refresh_establish_fact(
        &mut self,
        context: QueryContextRef,
        fact: EstablishStandDownFact,
    ) -> Result<(), ContextStandDownError> {
        if !self.started {
            return Err(ContextStandDownError::StandDownNotStarted);
        }
        let record = self
            .records
            .get_mut(&context)
            .ok_or(ContextStandDownError::UnknownContext)?;
        if record.closure != ContextClosureState::AwaitingEstablishIssue {
            return Ok(());
        }
        match fact {
            EstablishStandDownFact::NoGrant => {
                record.closure = ContextClosureState::NoRemoteResponsibility;
            }
            EstablishStandDownFact::IssueUnsettled => {}
            EstablishStandDownFact::AbortRequired => {
                record.require_abort(context);
            }
        }
        Ok(())
    }

    pub(crate) fn authorize_next_at(
        &mut self,
        now: super::MonotonicInstant,
    ) -> Result<Option<AbortQueryContextIssuePermit>, ContextStandDownError> {
        self.drain_events_at(now)?;
        let contexts: Vec<_> = self
            .records
            .keys()
            .copied()
            .filter(|context| {
                self.authorization_cursor
                    .is_none_or(|cursor| *context > cursor)
            })
            .chain(self.records.keys().copied().filter(|context| {
                self.authorization_cursor
                    .is_some_and(|cursor| *context <= cursor)
            }))
            .collect();
        for context in contexts {
            let record = self
                .records
                .get_mut(&context)
                .expect("Abort authorization context came from the same ledger");
            if record.closure != ContextClosureState::AbortPending
                || !matches!(
                    record.issue_state,
                    None | Some(AbortQueryContextIssueState::DefinitelyUnsent)
                        | Some(AbortQueryContextIssueState::TransportUnknown)
                )
                || record
                    .retry_not_before
                    .is_some_and(|deadline| !now.has_reached(deadline))
            {
                continue;
            }
            if record.authorization_generation >= self.max_authorizations_per_context.get() as u64 {
                record.issue_state = Some(AbortQueryContextIssueState::AuthorizationExhausted);
                record.closure = ContextClosureState::AbortIssueExhausted;
                continue;
            }
            record.authorization_generation = record
                .authorization_generation
                .checked_add(1)
                .ok_or(ContextStandDownError::AuthorizationGenerationExhausted)?;
            record.issue_state = Some(AbortQueryContextIssueState::IssueAuthorized);
            record.retry_not_before = None;
            self.authorization_cursor = Some(context);
            return Ok(Some(AbortQueryContextIssuePermit {
                identity: record
                    .identity
                    .ok_or(ContextStandDownError::IdentityMismatch)?,
                authorization_generation: record.authorization_generation,
                request: Some(Arc::clone(
                    record
                        .request
                        .as_ref()
                        .ok_or(ContextStandDownError::IdentityMismatch)?,
                )),
                events: self.events.clone(),
            }));
        }
        Ok(None)
    }

    #[cfg(test)]
    pub(crate) fn authorize_next(
        &mut self,
    ) -> Result<Option<AbortQueryContextIssuePermit>, ContextStandDownError> {
        self.authorize_next_at(super::MonotonicInstant::from_origin(Duration::MAX))
    }

    pub(crate) fn snapshot(&self, context: QueryContextRef) -> Option<ContextStandDownSnapshot> {
        self.records
            .get(&context)
            .map(ContextStandDownRecord::snapshot)
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

    pub(crate) fn has_unsettled_establish_issue(&self) -> bool {
        self.records
            .values()
            .any(|record| record.closure == ContextClosureState::AwaitingEstablishIssue)
    }

    pub(crate) fn observe_registry_convergence(
        &mut self,
        context: QueryContextRef,
        convergence: RegistryContextConvergence,
    ) -> Result<(), ContextStandDownError> {
        if !self.started {
            return Err(ContextStandDownError::StandDownNotStarted);
        }
        let record = self
            .records
            .get_mut(&context)
            .ok_or(ContextStandDownError::UnknownContext)?;
        record.registry_fenced = true;
        record.retry_not_before = None;
        record.request = None;
        record.closure = match convergence {
            RegistryContextConvergence::WorkerStoppedAndContextFenced => {
                ContextClosureState::WorkerStoppedAndContextFenced
            }
            RegistryContextConvergence::WorkerProcessReplaced => {
                ContextClosureState::WorkerProcessReplaced
            }
        };
        Ok(())
    }

    pub(crate) fn next_retry_at(&self) -> Option<super::MonotonicInstant> {
        self.records
            .values()
            .filter(|record| record.closure == ContextClosureState::AbortPending)
            .filter_map(|record| record.retry_not_before)
            .min()
    }

    pub(crate) fn drain_events_at(
        &mut self,
        now: super::MonotonicInstant,
    ) -> Result<usize, ContextStandDownError> {
        let mut applied = 0;
        loop {
            match self.event_rx.try_recv() {
                Ok(event) => {
                    self.apply_event(event, now)?;
                    applied += 1;
                }
                Err(mpsc::error::TryRecvError::Empty) => return Ok(applied),
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return Err(ContextStandDownError::EventChannelClosed);
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn drain_events(&mut self) -> Result<usize, ContextStandDownError> {
        self.drain_events_at(super::MonotonicInstant::ORIGIN)
    }

    pub(crate) async fn apply_next_event(
        &mut self,
        now: impl FnOnce() -> super::MonotonicInstant,
    ) -> Result<(), ContextStandDownError> {
        let event = self
            .event_rx
            .recv()
            .await
            .ok_or(ContextStandDownError::EventChannelClosed)?;
        self.apply_event(event, now())
    }

    pub(crate) fn poll_next_event(
        &mut self,
        context: &mut Context<'_>,
        now: impl FnOnce() -> MonotonicInstant,
    ) -> Poll<Result<(), ContextStandDownError>> {
        match self.event_rx.poll_recv(context) {
            Poll::Ready(Some(event)) => Poll::Ready(self.apply_event(event, now())),
            Poll::Ready(None) => Poll::Ready(Err(ContextStandDownError::EventChannelClosed)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn apply_event(
        &mut self,
        event: AbortIssueEvent,
        now: super::MonotonicInstant,
    ) -> Result<(), ContextStandDownError> {
        let record = self
            .records
            .get_mut(&event.identity.context())
            .ok_or(ContextStandDownError::UnknownContext)?;
        if record.identity != Some(event.identity) {
            return Err(ContextStandDownError::IdentityMismatch);
        }
        if event.authorization_generation > record.authorization_generation {
            return Err(ContextStandDownError::FutureAuthorization);
        }
        if event.authorization_generation < record.authorization_generation {
            return match event.kind {
                AbortIssueEventKind::WorkerSettled(_) if record.registry_fenced => Ok(()),
                AbortIssueEventKind::WorkerSettled(settlement) => settle_worker(record, settlement),
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
            AbortIssueEventKind::WorkerSettled(settlement) => settle_worker(record, settlement),
            AbortIssueEventKind::TransportOwned => match record.issue_state {
                Some(AbortQueryContextIssueState::IssueAuthorized) => {
                    record.issue_state = Some(AbortQueryContextIssueState::TransportOwned);
                    Ok(())
                }
                Some(AbortQueryContextIssueState::TransportOwned) => Ok(()),
                _ => Err(ContextStandDownError::WrongState),
            },
            AbortIssueEventKind::DefinitelyUnsent => match record.issue_state {
                Some(AbortQueryContextIssueState::IssueAuthorized)
                | Some(AbortQueryContextIssueState::TransportOwned) => {
                    record.issue_state = Some(AbortQueryContextIssueState::DefinitelyUnsent);
                    Ok(())
                }
                Some(AbortQueryContextIssueState::DefinitelyUnsent)
                | Some(AbortQueryContextIssueState::WorkerSettled) => Ok(()),
                _ => Err(ContextStandDownError::WrongState),
            },
            AbortIssueEventKind::TransportUnknown => match record.issue_state {
                Some(AbortQueryContextIssueState::TransportOwned) => {
                    record.issue_state = Some(AbortQueryContextIssueState::TransportUnknown);
                    record.retry_not_before = Some(now.saturating_add(self.retry_backoff));
                    Ok(())
                }
                Some(AbortQueryContextIssueState::TransportUnknown) => Ok(()),
                _ => Err(ContextStandDownError::WrongState),
            },
            AbortIssueEventKind::FailedClosed => fail_closed(record),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegistryContextConvergence {
    WorkerStoppedAndContextFenced,
    WorkerProcessReplaced,
}

fn settle_worker(
    record: &mut ContextStandDownRecord,
    settlement: AbortQueryContextWorkerSettlement,
) -> Result<(), ContextStandDownError> {
    if record.identity != Some(settlement.identity()) {
        return Err(ContextStandDownError::IdentityMismatch);
    }
    let closure = settlement.closure();
    reconcile_worker_branch(record, closure)?;
    advance_closure(record, closure)?;
    record.worker_settled = true;
    record.issue_state = Some(AbortQueryContextIssueState::WorkerSettled);
    record.request = None;
    Ok(())
}

fn fail_closed(record: &mut ContextStandDownRecord) -> Result<(), ContextStandDownError> {
    if record.registry_fenced || record.worker_settled {
        return Ok(());
    }
    record.issue_state = Some(AbortQueryContextIssueState::FailedClosed);
    record.closure = ContextClosureState::AbortIssueExhausted;
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
    record: &mut ContextStandDownRecord,
    closure: ContextClosureState,
) -> Result<(), ContextStandDownError> {
    let observed = match closure {
        ContextClosureState::Aborting => Some(WorkerClosureBranch::Aborting),
        ContextClosureState::Releasing => Some(WorkerClosureBranch::Releasing),
        _ => None,
    };
    match (record.worker_branch, observed) {
        (Some(current), Some(next)) if current != next => {
            Err(ContextStandDownError::ConflictingWorkerSettlement)
        }
        (None, Some(branch)) => {
            record.worker_branch = Some(branch);
            Ok(())
        }
        _ => Ok(()),
    }
}

fn advance_closure(
    record: &mut ContextStandDownRecord,
    closure: ContextClosureState,
) -> Result<(), ContextStandDownError> {
    use ContextClosureState::{
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
            return Err(ContextStandDownError::ConflictingWorkerSettlement);
        }
        _ => return Err(ContextStandDownError::ClosureRegression),
    };
    record.closure = next;
    Ok(())
}

fn worker_closure(state: QueryContextState) -> Option<ContextClosureState> {
    match state {
        QueryContextState::Aborting => Some(ContextClosureState::Aborting),
        QueryContextState::Releasing => Some(ContextClosureState::Releasing),
        QueryContextState::TerminalRetained => Some(ContextClosureState::TerminalRetained),
        QueryContextState::Gone => Some(ContextClosureState::Gone),
        QueryContextState::Absent | QueryContextState::Establishing | QueryContextState::Active => {
            None
        }
    }
}

/// Result of the synchronous effect-port admission boundary.
#[derive(Debug)]
pub enum AbortQueryContextIssueSubmit {
    Accepted,
    Backpressured(AbortQueryContextIssuePermit),
}

/// Query application's only dependency on Abort transport.
pub trait AbortQueryContextEffectPort: fmt::Debug + Send + Sync {
    /// Monotonic capacity epoch. A Backpressured answer must eventually
    /// publish a newer value when another reservation may be attempted.
    fn subscribe_capacity(&self) -> watch::Receiver<u64>;

    fn try_reserve(
        &self,
        identity: AbortQueryContextIssueIdentity,
    ) -> AbortQueryContextEffectAdmission;
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct PermanentlyBackpressuredAbortEffectPort {
    capacity: watch::Sender<u64>,
}

#[cfg(test)]
impl PermanentlyBackpressuredAbortEffectPort {
    pub(crate) fn shared() -> Arc<dyn AbortQueryContextEffectPort> {
        let (capacity, _) = watch::channel(0);
        Arc::new(Self { capacity })
    }
}

#[cfg(test)]
impl AbortQueryContextEffectPort for PermanentlyBackpressuredAbortEffectPort {
    fn subscribe_capacity(&self) -> watch::Receiver<u64> {
        self.capacity.subscribe()
    }

    fn try_reserve(
        &self,
        _identity: AbortQueryContextIssueIdentity,
    ) -> AbortQueryContextEffectAdmission {
        AbortQueryContextEffectAdmission::Backpressured
    }
}

pub enum AbortQueryContextEffectAdmission {
    Admitted(Box<dyn AbortQueryContextEffectReservation>),
    Backpressured,
    /// The role-composed effect owner is gone, so no later capacity change
    /// can make this Abort issuable.
    Closed,
}

impl fmt::Debug for AbortQueryContextEffectAdmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admitted(_) => formatter.write_str("Admitted(..)"),
            Self::Backpressured => formatter.write_str("Backpressured"),
            Self::Closed => formatter.write_str("Closed"),
        }
    }
}

/// Capacity is reserved before this object can receive or inspect a request.
pub trait AbortQueryContextEffectReservation: fmt::Debug + Send {
    fn submit(self: Box<Self>, submission: AbortQueryContextEffectSubmission);
}

#[derive(Debug)]
#[must_use = "an authorized Abort must enter effect-port ownership or settle definitely unsent"]
pub struct AbortQueryContextIssuePermit {
    identity: AbortQueryContextIssueIdentity,
    authorization_generation: u64,
    request: Option<Arc<AbortQueryContextEffectRequest>>,
    events: mpsc::Sender<AbortIssueEvent>,
}

impl AbortQueryContextIssuePermit {
    pub const fn identity(&self) -> AbortQueryContextIssueIdentity {
        self.identity
    }

    pub fn try_submit(
        mut self,
        port: &dyn AbortQueryContextEffectPort,
    ) -> Result<AbortQueryContextIssueSubmit, ContextStandDownError> {
        let reservation = match port.try_reserve(self.identity) {
            AbortQueryContextEffectAdmission::Admitted(reservation) => reservation,
            AbortQueryContextEffectAdmission::Backpressured => {
                return Ok(AbortQueryContextIssueSubmit::Backpressured(self));
            }
            AbortQueryContextEffectAdmission::Closed => {
                return Err(ContextStandDownError::EffectCapacityClosed);
            }
        };
        self.publish(AbortIssueEventKind::TransportOwned)?;
        let submission = AbortQueryContextEffectSubmission {
            identity: self.identity,
            authorization_generation: self.authorization_generation,
            request: self.request.take(),
            events: self.events.clone(),
        };
        reservation.submit(submission);
        Ok(AbortQueryContextIssueSubmit::Accepted)
    }

    fn publish(&self, kind: AbortIssueEventKind) -> Result<(), ContextStandDownError> {
        publish_event(
            &self.events,
            AbortIssueEvent {
                identity: self.identity,
                authorization_generation: self.authorization_generation,
                kind,
            },
        )
    }

    pub(crate) fn settle_definitely_unsent(mut self) -> Result<(), ContextStandDownError> {
        self.publish(AbortIssueEventKind::DefinitelyUnsent)?;
        self.request.take();
        Ok(())
    }
}

impl Drop for AbortQueryContextIssuePermit {
    fn drop(&mut self) {
        if self.request.is_some() {
            let _ = self.publish(AbortIssueEventKind::DefinitelyUnsent);
        }
    }
}

#[derive(Debug)]
#[must_use = "an effect-port-owned Abort must publish its exact settlement"]
pub struct AbortQueryContextEffectSubmission {
    identity: AbortQueryContextIssueIdentity,
    authorization_generation: u64,
    request: Option<Arc<AbortQueryContextEffectRequest>>,
    events: mpsc::Sender<AbortIssueEvent>,
}

impl AbortQueryContextEffectSubmission {
    pub const fn identity(&self) -> AbortQueryContextIssueIdentity {
        self.identity
    }

    pub fn request(&self) -> &AbortQueryContextEffectRequest {
        self.request
            .as_deref()
            .expect("live Abort effect submission retains its exact request")
    }

    pub fn transport_unknown(
        mut self,
    ) -> Result<LateAbortQueryContextWorkerSettlement, ContextStandDownError> {
        self.publish(AbortIssueEventKind::TransportUnknown)?;
        self.request.take();
        Ok(LateAbortQueryContextWorkerSettlement {
            identity: self.identity,
            authorization_generation: self.authorization_generation,
            events: self.events.clone(),
        })
    }

    /// The frontend dispatcher proved that this port-owned effect expired
    /// before it was released to process transport. This preserves the exact
    /// request in the actor ledger and permits the next bounded replay.
    pub fn definitely_unsent(mut self) -> Result<(), ContextStandDownError> {
        let result = self.publish(AbortIssueEventKind::DefinitelyUnsent);
        self.request.take();
        result
    }

    pub fn worker_settled(
        mut self,
        receipt: QueryContextReceipt,
    ) -> Result<(), ContextStandDownError> {
        let settlement = AbortQueryContextWorkerSettlement::try_new(self.identity, receipt)?;
        let result = self.publish(AbortIssueEventKind::WorkerSettled(settlement));
        self.request.take();
        result
    }

    /// A local protocol or dispatcher violation makes replay unsafe.
    ///
    /// The actor stops authorizing this issue while its residual supervisor
    /// retains the context responsibility until exact convergence evidence.
    pub fn fail_closed(mut self) -> Result<(), ContextStandDownError> {
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

    fn publish(&self, kind: AbortIssueEventKind) -> Result<(), ContextStandDownError> {
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

impl Drop for AbortQueryContextEffectSubmission {
    fn drop(&mut self) {
        if self.request.is_some() {
            let _ = self.publish(AbortIssueEventKind::TransportUnknown);
        }
    }
}

#[derive(Debug)]
pub struct LateAbortQueryContextWorkerSettlement {
    identity: AbortQueryContextIssueIdentity,
    authorization_generation: u64,
    events: mpsc::Sender<AbortIssueEvent>,
}

impl LateAbortQueryContextWorkerSettlement {
    pub const fn identity(&self) -> AbortQueryContextIssueIdentity {
        self.identity
    }

    pub fn worker_settled(self, receipt: QueryContextReceipt) -> Result<(), ContextStandDownError> {
        let settlement = AbortQueryContextWorkerSettlement::try_new(self.identity, receipt)?;
        publish_event(
            &self.events,
            AbortIssueEvent {
                identity: self.identity,
                authorization_generation: self.authorization_generation,
                kind: AbortIssueEventKind::WorkerSettled(settlement),
            },
        )
    }

    /// A late receipt violated the exact operation/context contract.
    /// Replaying the same request cannot repair that local protocol failure.
    pub fn fail_closed(self) -> Result<(), ContextStandDownError> {
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
) -> Result<(), ContextStandDownError> {
    events.try_send(event).map_err(|error| match error {
        mpsc::error::TrySendError::Closed(_) => ContextStandDownError::EventChannelClosed,
        mpsc::error::TrySendError::Full(_) => ContextStandDownError::EventBackpressureInvariant,
    })
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::sync::Mutex;

    use novarocks_execution_contract::{
        AcquireQueryContextAdmissionTicket, AdmissionEpochCapability, AdmissionTicketId,
        LeaseValidFor, OperationOutcome, QueryContextAdmissionTicketReceipt,
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

    fn context(backend: BackendProcessId) -> QueryContextRef {
        QueryContextRef::new(execution(), FrontendProcessId::new_v7(), backend)
    }

    fn ledger(contexts: impl IntoIterator<Item = QueryContextRef>) -> ContextStandDownLedger {
        ContextStandDownLedger::new(
            contexts.into_iter().collect(),
            NonZeroUsize::new(3).unwrap(),
        )
        .unwrap()
    }

    #[derive(Debug)]
    struct RecordingPort {
        submissions: Arc<Mutex<Vec<AbortQueryContextEffectSubmission>>>,
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
        submissions: Arc<Mutex<Vec<AbortQueryContextEffectSubmission>>>,
    }

    impl AbortQueryContextEffectReservation for RecordingReservation {
        fn submit(self: Box<Self>, submission: AbortQueryContextEffectSubmission) {
            self.submissions.lock().unwrap().push(submission);
        }
    }

    impl AbortQueryContextEffectPort for RecordingPort {
        fn subscribe_capacity(&self) -> watch::Receiver<u64> {
            self.capacity.subscribe()
        }

        fn try_reserve(
            &self,
            _identity: AbortQueryContextIssueIdentity,
        ) -> AbortQueryContextEffectAdmission {
            if self.backpressured {
                AbortQueryContextEffectAdmission::Backpressured
            } else {
                AbortQueryContextEffectAdmission::Admitted(Box::new(RecordingReservation {
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

    impl AbortQueryContextEffectPort for ClosedPort {
        fn subscribe_capacity(&self) -> watch::Receiver<u64> {
            self.capacity.subscribe()
        }

        fn try_reserve(
            &self,
            _identity: AbortQueryContextIssueIdentity,
        ) -> AbortQueryContextEffectAdmission {
            AbortQueryContextEffectAdmission::Closed
        }
    }

    #[test]
    fn no_grant_closes_without_abort_but_a_held_ticket_requires_abort() {
        let no_grant = context(BackendProcessId::new_v7());
        let ticket_held = context(BackendProcessId::new_v7());
        let mut ledger = ledger([no_grant, ticket_held]);
        ledger
            .begin(
                activation(),
                ContextStandDownCause::LogicalExecutionCancelled,
                [
                    (no_grant, EstablishStandDownFact::NoGrant),
                    (ticket_held, EstablishStandDownFact::AbortRequired),
                ],
            )
            .unwrap();

        assert_eq!(
            ledger.snapshot(no_grant).unwrap().closure(),
            ContextClosureState::NoRemoteResponsibility
        );
        assert_eq!(
            ledger.snapshot(ticket_held).unwrap().closure(),
            ContextClosureState::AbortPending
        );
        assert!(ledger.authorize_next().unwrap().is_some());
        assert!(!ledger.responsibility_settled());
    }

    #[test]
    fn duplicate_fact_rows_are_rejected_before_map_deduplication() {
        let first = context(BackendProcessId::new_v7());
        let second = context(BackendProcessId::new_v7());
        let mut ledger = ledger([first, second]);
        assert_eq!(
            ledger.begin(
                activation(),
                ContextStandDownCause::LogicalExecutionCancelled,
                [
                    (first, EstablishStandDownFact::NoGrant),
                    (first, EstablishStandDownFact::AbortRequired),
                    (second, EstablishStandDownFact::NoGrant),
                ],
            ),
            Err(ContextStandDownError::DuplicateContext)
        );
    }

    #[test]
    fn definitely_unsent_establish_still_requires_abort_for_its_ticket() {
        let context = context(BackendProcessId::new_v7());
        let mut ledger = ledger([context]);
        ledger
            .begin(
                activation(),
                ContextStandDownCause::LogicalExecutionCancelled,
                [(context, EstablishStandDownFact::AbortRequired)],
            )
            .unwrap();

        let permit = ledger.authorize_next().unwrap().unwrap();
        assert_eq!(permit.identity().context(), context);
        drop(permit);
        ledger.drain_events().unwrap();
        assert_eq!(
            ledger.snapshot(context).unwrap().issue_state(),
            Some(AbortQueryContextIssueState::DefinitelyUnsent)
        );
        assert!(ledger.authorize_next().unwrap().is_some());
    }

    #[test]
    fn abort_authorization_is_round_robin_across_contexts() {
        let first = context(BackendProcessId::new_v7());
        let second = context(BackendProcessId::new_v7());
        let contexts = BTreeSet::from([first, second]);
        let expected_first = *contexts.first().unwrap();
        let expected_second = *contexts.last().unwrap();
        let mut ledger =
            ContextStandDownLedger::new(contexts, NonZeroUsize::new(2).unwrap()).unwrap();
        ledger
            .begin(
                activation(),
                ContextStandDownCause::LogicalExecutionCancelled,
                [
                    (first, EstablishStandDownFact::AbortRequired),
                    (second, EstablishStandDownFact::AbortRequired),
                ],
            )
            .unwrap();

        let first_issue = ledger.authorize_next().unwrap().unwrap();
        assert_eq!(first_issue.identity().context(), expected_first);
        drop(first_issue);
        ledger.drain_events().unwrap();

        let next = ledger.authorize_next().unwrap().unwrap();
        assert_eq!(next.identity().context(), expected_second);
    }

    #[test]
    fn transport_unknown_retries_wait_for_one_aggregated_deadline() {
        let first = context(BackendProcessId::new_v7());
        let second = context(BackendProcessId::new_v7());
        let contexts = BTreeSet::from([first, second]);
        let expected_first = *contexts.first().unwrap();
        let expected_second = *contexts.last().unwrap();
        let mut ledger =
            ContextStandDownLedger::new(contexts, NonZeroUsize::new(2).unwrap()).unwrap();
        ledger
            .begin(
                activation(),
                ContextStandDownCause::LogicalExecutionCancelled,
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
        assert_eq!(first_submission.identity().context(), expected_first);
        let _first_late = first_submission.transport_unknown().unwrap();
        ledger.drain_events_at(now).unwrap();

        ledger
            .authorize_next_at(now)
            .unwrap()
            .unwrap()
            .try_submit(&port)
            .unwrap();
        let second_submission = port.submissions.lock().unwrap().pop().unwrap();
        assert_eq!(second_submission.identity().context(), expected_second);
        let _second_late = second_submission.transport_unknown().unwrap();
        ledger.drain_events_at(now).unwrap();

        assert!(ledger.authorize_next_at(now).unwrap().is_none());
        let retry_at = now.saturating_add(DEFAULT_ABORT_RETRY_BACKOFF);
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
                .context(),
            expected_first
        );
    }

    #[tokio::test]
    async fn retry_deadline_uses_the_clock_after_the_event_arrives() {
        let context = context(BackendProcessId::new_v7());
        let mut ledger = ledger([context]);
        ledger
            .begin(
                activation(),
                ContextStandDownCause::LogicalExecutionCancelled,
                [(context, EstablishStandDownFact::AbortRequired)],
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
            Some(arrived_at.saturating_add(DEFAULT_ABORT_RETRY_BACKOFF))
        );
    }

    #[test]
    fn one_context_exhausting_abort_replay_does_not_block_the_next_context() {
        let first = context(BackendProcessId::new_v7());
        let second = context(BackendProcessId::new_v7());
        let contexts = BTreeSet::from([first, second]);
        let expected_first = *contexts.first().unwrap();
        let expected_second = *contexts.last().unwrap();
        let mut ledger =
            ContextStandDownLedger::new(contexts, NonZeroUsize::new(1).unwrap()).unwrap();
        ledger
            .begin(
                activation(),
                ContextStandDownCause::LogicalExecutionCancelled,
                [
                    (first, EstablishStandDownFact::AbortRequired),
                    (second, EstablishStandDownFact::AbortRequired),
                ],
            )
            .unwrap();

        let exhausted = ledger.authorize_next().unwrap().unwrap();
        assert_eq!(exhausted.identity().context(), expected_first);
        drop(exhausted);
        ledger.drain_events().unwrap();

        let next = ledger.authorize_next().unwrap().unwrap();
        assert_eq!(next.identity().context(), expected_second);
        drop(next);
        ledger.drain_events().unwrap();
        assert!(ledger.authorize_next().unwrap().is_none());
        assert_eq!(
            ledger.snapshot(expected_first).unwrap().issue_state(),
            Some(AbortQueryContextIssueState::AuthorizationExhausted)
        );
        assert_eq!(
            ledger.snapshot(expected_first).unwrap().closure(),
            ContextClosureState::AbortIssueExhausted
        );
    }

    #[test]
    fn exact_process_replacement_settles_exhausted_residual_responsibility() {
        let context = context(BackendProcessId::new_v7());
        let mut ledger =
            ContextStandDownLedger::new([context].into_iter().collect(), NonZeroUsize::MIN)
                .unwrap();
        ledger
            .begin(
                activation(),
                ContextStandDownCause::LogicalExecutionCancelled,
                [(context, EstablishStandDownFact::AbortRequired)],
            )
            .unwrap();
        drop(ledger.authorize_next().unwrap().unwrap());
        ledger.drain_events().unwrap();
        assert!(ledger.authorize_next().unwrap().is_none());
        assert_eq!(
            ledger.snapshot(context).unwrap().closure(),
            ContextClosureState::AbortIssueExhausted
        );
        ledger
            .observe_registry_convergence(
                context,
                RegistryContextConvergence::WorkerProcessReplaced,
            )
            .unwrap();
        assert_eq!(
            ledger.snapshot(context).unwrap().closure(),
            ContextClosureState::WorkerProcessReplaced
        );
        assert!(ledger.responsibility_settled());
    }

    #[test]
    fn authorized_establish_must_linearize_before_abort_is_minted() {
        let context = context(BackendProcessId::new_v7());
        let mut ledger = ledger([context]);
        ledger
            .begin(
                activation(),
                ContextStandDownCause::LogicalExecutionCancelled,
                [(context, EstablishStandDownFact::IssueUnsettled)],
            )
            .unwrap();
        assert!(ledger.authorize_next().unwrap().is_none());
        assert!(ledger.has_unsettled_establish_issue());

        ledger
            .refresh_establish_fact(context, EstablishStandDownFact::AbortRequired)
            .unwrap();
        assert!(ledger.authorize_next().unwrap().is_some());
    }

    #[test]
    fn effect_port_reserves_before_receiving_the_exact_replay_request() {
        let context = context(BackendProcessId::new_v7());
        let mut ledger = ledger([context]);
        ledger
            .begin(
                activation(),
                ContextStandDownCause::LogicalExecutionFailed,
                [(context, EstablishStandDownFact::AbortRequired)],
            )
            .unwrap();
        let mut port = RecordingPort {
            backpressured: true,
            ..RecordingPort::default()
        };
        let permit = ledger.authorize_next().unwrap().unwrap();
        let identity = permit.identity();
        let permit = match permit.try_submit(&port).unwrap() {
            AbortQueryContextIssueSubmit::Backpressured(permit) => permit,
            AbortQueryContextIssueSubmit::Accepted => panic!("backpressured port accepted Abort"),
        };
        assert!(port.submissions.lock().unwrap().is_empty());

        port.backpressured = false;
        assert!(matches!(
            permit.try_submit(&port).unwrap(),
            AbortQueryContextIssueSubmit::Accepted
        ));
        let submission = port.submissions.lock().unwrap().pop().unwrap();
        assert_eq!(submission.identity(), identity);
        assert_eq!(submission.request().identity(), identity);
    }

    #[test]
    fn a_closed_effect_port_fails_instead_of_waiting_for_capacity() {
        let context = context(BackendProcessId::new_v7());
        let mut ledger = ledger([context]);
        ledger
            .begin(
                activation(),
                ContextStandDownCause::LogicalExecutionCancelled,
                [(context, EstablishStandDownFact::AbortRequired)],
            )
            .unwrap();
        let permit = ledger.authorize_next().unwrap().unwrap();
        assert_eq!(
            permit.try_submit(&ClosedPort::default()).unwrap_err(),
            ContextStandDownError::EffectCapacityClosed
        );
    }

    #[test]
    fn transport_unknown_replays_the_same_operation_and_request_allocation() {
        let context = context(BackendProcessId::new_v7());
        let mut ledger = ledger([context]);
        ledger
            .begin(
                activation(),
                ContextStandDownCause::LogicalExecutionCancelled,
                [(context, EstablishStandDownFact::AbortRequired)],
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
        let context = context(BackendProcessId::new_v7());
        let mut ledger = ledger([context]);
        ledger
            .begin(
                activation(),
                ContextStandDownCause::LogicalExecutionCancelled,
                [(context, EstablishStandDownFact::AbortRequired)],
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
            .worker_settled(QueryContextReceipt::new(
                context,
                QueryContextState::TerminalRetained,
            ))
            .unwrap();
        ledger.drain_events().unwrap();

        let snapshot = ledger.snapshot(context).unwrap();
        assert_eq!(snapshot.closure(), ContextClosureState::TerminalRetained);
        assert!(ledger.responsibility_settled());
        // TerminalRetained is deliberately not an actual-stop or resource-
        // release fact; this ledger has no API that can manufacture either.
    }

    #[test]
    fn worker_receipt_closure_accepts_all_worker_closing_states() {
        for (state, expected) in [
            (QueryContextState::Aborting, ContextClosureState::Aborting),
            (QueryContextState::Releasing, ContextClosureState::Releasing),
            (
                QueryContextState::TerminalRetained,
                ContextClosureState::TerminalRetained,
            ),
            (QueryContextState::Gone, ContextClosureState::Gone),
        ] {
            let context = context(BackendProcessId::new_v7());
            let mut ledger = ledger([context]);
            ledger
                .begin(
                    activation(),
                    ContextStandDownCause::LogicalExecutionCancelled,
                    [(context, EstablishStandDownFact::AbortRequired)],
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
                .worker_settled(QueryContextReceipt::new(context, state))
                .unwrap();
            ledger.drain_events().unwrap();

            assert_eq!(ledger.snapshot(context).unwrap().closure(), expected);
            assert!(ledger.responsibility_settled());
        }
    }

    #[test]
    fn late_stale_abort_reply_does_not_regress_terminal_closure() {
        let context = context(BackendProcessId::new_v7());
        let mut ledger = ledger([context]);
        ledger
            .begin(
                activation(),
                ContextStandDownCause::LogicalExecutionCancelled,
                [(context, EstablishStandDownFact::AbortRequired)],
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
            .worker_settled(QueryContextReceipt::new(
                context,
                QueryContextState::TerminalRetained,
            ))
            .unwrap();
        ledger.drain_events().unwrap();

        late.worker_settled(QueryContextReceipt::new(
            context,
            QueryContextState::Aborting,
        ))
        .unwrap();
        ledger.drain_events().unwrap();
        assert_eq!(
            ledger.snapshot(context).unwrap().closure(),
            ContextClosureState::TerminalRetained
        );
    }

    #[test]
    fn late_settlement_allows_an_actor_held_replay_permit_to_close_unsent() {
        let context = context(BackendProcessId::new_v7());
        let mut ledger = ledger([context]);
        ledger
            .begin(
                activation(),
                ContextStandDownCause::LogicalExecutionCancelled,
                [(context, EstablishStandDownFact::AbortRequired)],
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

        late.worker_settled(QueryContextReceipt::new(
            context,
            QueryContextState::TerminalRetained,
        ))
        .unwrap();
        ledger.drain_events().unwrap();
        replay.settle_definitely_unsent().unwrap();
        ledger.drain_events().unwrap();

        let snapshot = ledger.snapshot(context).unwrap();
        assert_eq!(snapshot.closure(), ContextClosureState::TerminalRetained);
        assert_eq!(
            snapshot.issue_state(),
            Some(AbortQueryContextIssueState::WorkerSettled)
        );
    }

    #[test]
    fn exact_abort_replies_cannot_switch_closure_branches() {
        let context = context(BackendProcessId::new_v7());
        let mut ledger = ledger([context]);
        ledger
            .begin(
                activation(),
                ContextStandDownCause::LogicalExecutionCancelled,
                [(context, EstablishStandDownFact::AbortRequired)],
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
            .worker_settled(QueryContextReceipt::new(
                context,
                QueryContextState::Releasing,
            ))
            .unwrap();
        ledger.drain_events().unwrap();

        late.worker_settled(QueryContextReceipt::new(
            context,
            QueryContextState::Aborting,
        ))
        .unwrap();
        assert_eq!(
            ledger.drain_events().unwrap_err(),
            ContextStandDownError::ConflictingWorkerSettlement
        );
    }

    #[test]
    fn terminal_closure_retains_the_first_observed_abort_branch() {
        let context = context(BackendProcessId::new_v7());
        let mut ledger = ledger([context]);
        ledger
            .begin(
                activation(),
                ContextStandDownCause::LogicalExecutionCancelled,
                [(context, EstablishStandDownFact::AbortRequired)],
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
            .worker_settled(QueryContextReceipt::new(
                context,
                QueryContextState::TerminalRetained,
            ))
            .unwrap();
        ledger.drain_events().unwrap();

        second_late
            .worker_settled(QueryContextReceipt::new(
                context,
                QueryContextState::Aborting,
            ))
            .unwrap();
        ledger.drain_events().unwrap();
        assert_eq!(
            ledger.snapshot(context).unwrap().closure(),
            ContextClosureState::TerminalRetained
        );

        first_late
            .worker_settled(QueryContextReceipt::new(
                context,
                QueryContextState::Releasing,
            ))
            .unwrap();
        assert_eq!(
            ledger.drain_events().unwrap_err(),
            ContextStandDownError::ConflictingWorkerSettlement
        );
    }

    #[tokio::test]
    async fn actor_owner_waits_for_abort_settlement_after_logical_cancellation() {
        let execution = execution();
        let context = context(BackendProcessId::new_v7());
        let port = Arc::new(RecordingPort::default());
        let (work_owner, stage) = governed_work();
        let config = crate::coordination::LogicalExecutionActorConfig::single_attempt_completion(
            execution,
            crate::coordination::ExecutionEffect::None,
            NonZeroUsize::new(2).unwrap(),
            vec![context],
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            work_owner,
            stage,
        )
        .unwrap()
        .with_abort_query_context_effect_port(
            Arc::clone(&port) as Arc<dyn AbortQueryContextEffectPort>,
            NonZeroUsize::new(2).unwrap(),
        );
        let (owner, initial) =
            crate::coordination::spawn_logical_execution_actor(&Handle::current(), config).unwrap();
        let actor = owner.actor().clone();
        let running = actor.activate(initial.ready()).await.unwrap();
        let admission = AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            context,
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
                        QueryContextAdmissionTicketReceipt::new(
                            AdmissionTicketId::try_from_bytes([9; 16]).unwrap(),
                            context,
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
                .stand_down_snapshot(context)
                .await
                .unwrap()
                .unwrap()
                .closure(),
            ContextClosureState::AbortPending
        );

        submission
            .worker_settled(QueryContextReceipt::new(
                context,
                QueryContextState::TerminalRetained,
            ))
            .unwrap();
        loop {
            if actor
                .stand_down_snapshot(context)
                .await
                .unwrap()
                .is_some_and(|snapshot| snapshot.closure() == ContextClosureState::TerminalRetained)
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
            .observe_worker_stopped_and_context_fenced(context)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), supervisor.join())
            .await
            .expect("positive Registry convergence lets the residual supervisor finish")
            .unwrap();
    }

    #[tokio::test]
    async fn actor_with_remote_contexts_requires_an_abort_effect_port() {
        let execution = execution();
        let context = context(BackendProcessId::new_v7());
        let (work_owner, stage) = governed_work();
        let config = crate::coordination::LogicalExecutionActorConfig::single_attempt_completion(
            execution,
            crate::coordination::ExecutionEffect::None,
            NonZeroUsize::new(1).unwrap(),
            vec![context],
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
