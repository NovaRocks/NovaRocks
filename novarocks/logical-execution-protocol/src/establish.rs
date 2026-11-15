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
//! Pure admission and Establish authorization, replay and settlement records.
//!
//! A ledger is bound to one exact activation. Copying an authorization reference
//! cannot authorize another operation, and closing authority does not discard
//! outstanding settlements. An authorized issue already counts as a possible
//! remote effect; no transport guard, destructor or cross-thread fence is needed.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

use novarocks_coordination_contract::{
    AttemptActivationIdentity, AttemptAuthorizationDecision, AttemptAuthorizationRef,
    AttemptIssueSettlement, ExecutionFailure, ExecutionFailureInput, ExecutionFailureSite,
    MonotonicInstant, PreReadyProcessReplacementWitness, logical_port::AttemptAuthorization,
};
use novarocks_execution_contract::{
    AcquireParticipationAdmissionTicket, AdmissionEpochCapability, AdmissionTicketId,
    EstablishSemanticIdentity, LeaseValidFor, OperationKind, OperationOutcome,
    ParticipationAdmissionTicketReceipt, ParticipationRef, SafeDetail, TaskOperationId,
    failure::{FailureClass, FailureFact, FailureProjection, FailureReason, domain},
};
use novarocks_types::NativeCompatibilityId;

/// Conservative remote responsibility, independent of result-only withdrawal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EstablishStandDownFact {
    NoGrant,
    AbortRequired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AdmissionIssueDisposition {
    Granted,
    Retryable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AdmissionIssueReceipt {
    activation: AttemptActivationIdentity,
    operation_id: TaskOperationId,
    participation: ParticipationRef,
    valid_for: LeaseValidFor,
    native_compatibility_id: NativeCompatibilityId,
    admission_epoch_capability: AdmissionEpochCapability,
    first_sent_at: MonotonicInstant,
}

impl AdmissionIssueReceipt {
    fn from_request(
        activation: AttemptActivationIdentity,
        request: AcquireParticipationAdmissionTicket,
        first_sent_at: MonotonicInstant,
    ) -> Result<Self, EstablishIssueError> {
        if activation.execution() != request.participation().query_execution_id() {
            return Err(EstablishIssueError::ActivationExecutionMismatch);
        }
        Ok(Self {
            activation,
            operation_id: request.envelope().operation_id(),
            participation: request.participation(),
            valid_for: request.valid_for(),
            native_compatibility_id: request.native_compatibility_id(),
            admission_epoch_capability: request.admission_epoch_capability(),
            first_sent_at,
        })
    }

    fn authorizes_same_request(self, other: Self) -> bool {
        self.activation == other.activation
            && self.operation_id == other.operation_id
            && self.participation == other.participation
            && self.valid_for == other.valid_for
            && self.native_compatibility_id == other.native_compatibility_id
            && self.admission_epoch_capability == other.admission_epoch_capability
    }

    #[cfg(test)]
    pub(crate) const fn first_sent_at(self) -> MonotonicInstant {
        self.first_sent_at
    }
}

/// Original immutable fields, not an address of a transport request allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EstablishIssueIdentity {
    pub(crate) activation: AttemptActivationIdentity,
    pub(crate) participation: ParticipationRef,
    pub(crate) operation_id: TaskOperationId,
    pub(crate) ticket_id: AdmissionTicketId,
    pub(crate) semantics: EstablishSemanticIdentity,
    pub(crate) native_compatibility_id: NativeCompatibilityId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EstablishWorkerSettlement {
    Applied,
    Rejected(OperationOutcome),
}

impl EstablishWorkerSettlement {
    const fn from_outcome(outcome: OperationOutcome) -> Self {
        match outcome {
            OperationOutcome::Accepted | OperationOutcome::Idempotent => Self::Applied,
            rejected => Self::Rejected(rejected),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EstablishIssueState {
    TicketHeld,
    IssueAuthorized,
    TransportOwned,
    DefinitelyUnsent,
    TransportUnknown,
    PreWorkerRejected,
    WorkerSettled(EstablishWorkerSettlement),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EstablishIssueSnapshot {
    pub(crate) ticket: ParticipationAdmissionTicketReceipt,
    pub(crate) conservative_expiry: MonotonicInstant,
    pub(crate) identity: Option<EstablishIssueIdentity>,
    /// Includes uncertainty from an older authorization of the same operation.
    pub(crate) state: EstablishIssueState,
    pub(crate) latest_authorization: (u64, EstablishIssueState),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EstablishIssueError {
    ActivationExecutionMismatch,
    UnknownParticipation,
    ConflictingAdmissionIssue,
    AdmissionReplayClosed,
    AdmissionIssueBudgetExhausted,
    InvalidAdmissionSettlement,
    AdmissionRejected,
    AdmissionTimeInFuture,
    TicketExpired,
    TicketDoesNotAuthorizeRequest,
    WrongState,
    IdentityMismatch,
    IssueAuthorityRevoked,
    AuthorizationGenerationExhausted,
    AuthorizationBudgetExhausted,
    FutureAuthorization,
    ConflictingWorkerSettlement,
    EstablishNotSettled,
    EstablishRejected,
}

impl fmt::Display for EstablishIssueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ActivationExecutionMismatch => {
                "attempt activation and participation name different executions"
            }
            Self::UnknownParticipation => {
                "Establish issue names a participation with no held ticket"
            }
            Self::ConflictingAdmissionIssue => {
                "participation admission was already issued with different immutable facts"
            }
            Self::AdmissionReplayClosed => "settled admission operation cannot be issued again",
            Self::AdmissionIssueBudgetExhausted => {
                "participation admission issue budget is exhausted"
            }
            Self::InvalidAdmissionSettlement => {
                "admission settlement does not match its exact request or outcome shape"
            }
            Self::AdmissionRejected => "Worker rejected participation admission",
            Self::AdmissionTimeInFuture => {
                "admission issue time is later than the actor monotonic time"
            }
            Self::TicketExpired => "admission ticket expired before Establish issue authorization",
            Self::TicketDoesNotAuthorizeRequest => {
                "held admission ticket does not authorize the Establish request"
            }
            Self::WrongState => "Establish issue is not legal in the current ledger state",
            Self::IdentityMismatch => "Establish event does not match its authorized request",
            Self::IssueAuthorityRevoked => {
                "Establish issue authority was revoked before transport ownership"
            }
            Self::AuthorizationGenerationExhausted => {
                "Establish issue authorization generation is exhausted"
            }
            Self::AuthorizationBudgetExhausted => {
                "Establish issue authorization budget is exhausted"
            }
            Self::FutureAuthorization => {
                "Establish event belongs to an authorization generation not yet issued"
            }
            Self::ConflictingWorkerSettlement => {
                "Establish issue received conflicting Worker settlement outcomes"
            }
            Self::EstablishNotSettled => {
                "logical execution completed before every Establish was settled by its Worker"
            }
            Self::EstablishRejected => {
                "logical execution completed after a Worker rejected Establish"
            }
        })
    }
}

impl std::error::Error for EstablishIssueError {}

impl FailureProjection for EstablishIssueError {
    fn failure_fact(&self) -> FailureFact {
        let (class, code, observed_stop) = match self {
            Self::ActivationExecutionMismatch
            | Self::IdentityMismatch
            | Self::TicketDoesNotAuthorizeRequest => (FailureClass::Invariant, 72, false),
            Self::UnknownParticipation => (FailureClass::Invariant, 73, false),
            Self::ConflictingAdmissionIssue | Self::ConflictingWorkerSettlement => {
                (FailureClass::Invariant, 74, false)
            }
            Self::InvalidAdmissionSettlement => (FailureClass::Invariant, 75, false),
            Self::AdmissionTimeInFuture | Self::FutureAuthorization => {
                (FailureClass::Invariant, 76, false)
            }
            Self::WrongState | Self::EstablishNotSettled => (FailureClass::Control, 78, false),
            Self::AdmissionReplayClosed | Self::IssueAuthorityRevoked => {
                (FailureClass::Control, 79, true)
            }
            Self::AdmissionIssueBudgetExhausted | Self::AuthorizationBudgetExhausted => {
                (FailureClass::Resource, 80, false)
            }
            Self::AuthorizationGenerationExhausted => (FailureClass::Invariant, 81, false),
            Self::TicketExpired => (FailureClass::Control, 82, false),
            Self::AdmissionRejected | Self::EstablishRejected => (FailureClass::Control, 83, false),
        };
        let reason = FailureReason::new(domain::QUERY, std::num::NonZeroU16::new(code).unwrap());
        if observed_stop {
            FailureFact::stop_observation(reason)
        } else {
            FailureFact::new(class, reason)
        }
    }
}

impl EstablishIssueError {
    pub(crate) fn failure(self) -> ExecutionFailure {
        ExecutionFailure::from_input(ExecutionFailureInput::new(
            self.failure_fact(),
            self.safe_detail(),
            ExecutionFailureSite::Coordinator,
        ))
    }

    pub(crate) fn safe_detail(self) -> SafeDetail {
        let message = match self {
            Self::ActivationExecutionMismatch => {
                "attempt activation and participation name different executions"
            }
            Self::IdentityMismatch => "Establish event does not match its authorized request",
            Self::TicketDoesNotAuthorizeRequest => {
                "held admission ticket does not authorize the Establish request"
            }
            Self::UnknownParticipation => {
                "Establish issue names a participation with no held ticket"
            }
            Self::ConflictingAdmissionIssue => {
                "participation admission was issued with conflicting immutable facts"
            }
            Self::ConflictingWorkerSettlement => {
                "Establish issue received conflicting Worker settlements"
            }
            Self::InvalidAdmissionSettlement => {
                "admission settlement does not match its exact request or outcome shape"
            }
            Self::AdmissionTimeInFuture => {
                "admission issue time is later than the actor monotonic time"
            }
            Self::FutureAuthorization => {
                "Establish event names an authorization generation not yet issued"
            }
            Self::WrongState => "Establish issue is not allowed in the current ledger state",
            Self::EstablishNotSettled => "required Establish operations are not all settled",
            Self::AdmissionReplayClosed => "settled admission operation is closed to reissue",
            Self::IssueAuthorityRevoked => {
                "Establish issue authority was revoked before transport ownership"
            }
            Self::AdmissionIssueBudgetExhausted => {
                "participation admission issue budget is exhausted"
            }
            Self::AuthorizationBudgetExhausted => {
                "Establish issue authorization budget is exhausted"
            }
            Self::AuthorizationGenerationExhausted => {
                "bounded Establish authorization generation could not advance"
            }
            Self::TicketExpired => "admission ticket expired before Establish issue authorization",
            Self::AdmissionRejected => "Worker rejected participation admission",
            Self::EstablishRejected => "Worker rejected a required Establish operation",
        };
        SafeDetail::truncating(message)
    }
}

#[derive(Clone, Copy, Debug)]
struct EstablishIssueRecord {
    activation: AttemptActivationIdentity,
    ticket: ParticipationAdmissionTicketReceipt,
    conservative_expiry: MonotonicInstant,
    identity: Option<EstablishIssueIdentity>,
    authorization_generation: u64,
    prior_transport_unknown: bool,
    state: EstablishIssueState,
}

impl EstablishIssueRecord {
    fn operation_state(self) -> EstablishIssueState {
        if self.prior_transport_unknown && self.state == EstablishIssueState::DefinitelyUnsent {
            EstablishIssueState::TransportUnknown
        } else {
            self.state
        }
    }

    fn snapshot(self) -> EstablishIssueSnapshot {
        EstablishIssueSnapshot {
            ticket: self.ticket,
            conservative_expiry: self.conservative_expiry,
            identity: self.identity,
            state: self.operation_state(),
            latest_authorization: (self.authorization_generation, self.state),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdmissionIssueState {
    Pending,
    RetryableNoGrant(OperationOutcome),
    Granted {
        ticket: ParticipationAdmissionTicketReceipt,
        conservative_expiry: MonotonicInstant,
    },
    ExpiredGrant(ParticipationAdmissionTicketReceipt),
    Rejected(OperationOutcome),
    PreWorkerRejected,
    PreWorkerRejectedAfterUnknown,
}

impl AdmissionIssueState {
    fn may_hold_capacity(self) -> bool {
        matches!(
            self,
            Self::Pending
                | Self::Granted { .. }
                | Self::ExpiredGrant(_)
                | Self::PreWorkerRejectedAfterUnknown
                | Self::Rejected(OperationOutcome::OwnerFailed)
                | Self::RetryableNoGrant(OperationOutcome::AdmissionTicketStillActive)
        )
    }
}

#[derive(Clone, Copy, Debug)]
struct AdmissionIssueRecord {
    receipt: AdmissionIssueReceipt,
    generation: usize,
    authorization: AttemptAuthorizationRef,
    state: AdmissionIssueState,
}

#[derive(Clone, Copy, Debug)]
enum IssuedAuthorization {
    Admission(AdmissionIssueReceipt),
    Establish {
        identity: EstablishIssueIdentity,
        generation: u64,
    },
}

/// Each update touches one participation and O(log N) deterministic indexes.
/// Aggregate readiness and conservative stand-down do not scan other records.
#[derive(Debug)]
pub(crate) struct EstablishIssueLedger {
    activation: AttemptActivationIdentity,
    required: Arc<BTreeSet<ParticipationRef>>,
    withdrawn: BTreeSet<ParticipationRef>,
    authority_open: bool,
    admission_limit: NonZeroUsize,
    establish_limit: NonZeroUsize,
    admissions: BTreeMap<ParticipationRef, AdmissionIssueRecord>,
    retired_admissions: BTreeMap<(ParticipationRef, TaskOperationId), AdmissionIssueRecord>,
    admission_operations: BTreeMap<TaskOperationId, AttemptAuthorizationRef>,
    records: BTreeMap<ParticipationRef, EstablishIssueRecord>,
    authorizations: BTreeMap<AttemptAuthorizationRef, IssuedAuthorization>,
    next_authorization: u64,
    remote_admissions: BTreeMap<ParticipationRef, usize>,
    pending: BTreeSet<ParticipationRef>,
    refusals: BTreeMap<ParticipationRef, EstablishIssueError>,
    establish_rejections: BTreeSet<ParticipationRef>,
}

/// Constructed one charged row at a time for a successor whose topology was
/// unknown at installation. The two indexes move into the ledger without N-copy.
pub(crate) struct EstablishLedgerBuilder {
    execution: novarocks_types::QueryExecutionId,
    required: BTreeSet<ParticipationRef>,
    pending: BTreeSet<ParticipationRef>,
}
impl EstablishLedgerBuilder {
    pub(crate) fn new(execution: novarocks_types::QueryExecutionId) -> Self {
        Self {
            execution,
            required: BTreeSet::new(),
            pending: BTreeSet::new(),
        }
    }
    pub(crate) fn push(
        &mut self,
        participation: ParticipationRef,
    ) -> Result<(), EstablishIssueError> {
        if participation.query_execution_id() != self.execution {
            return Err(EstablishIssueError::ActivationExecutionMismatch);
        }
        if !self.required.insert(participation) {
            return Err(EstablishIssueError::IdentityMismatch);
        }
        self.pending.insert(participation);
        Ok(())
    }
    pub(crate) fn finish(
        self,
        activation: AttemptActivationIdentity,
        admission_limit: NonZeroUsize,
        establish_limit: NonZeroUsize,
    ) -> EstablishIssueLedger {
        assert_eq!(
            self.execution,
            activation.execution(),
            "validated roster belongs to the exact successor activation"
        );
        EstablishIssueLedger {
            activation,
            pending: self.pending,
            required: Arc::new(self.required),
            withdrawn: BTreeSet::new(),
            authority_open: true,
            admission_limit,
            establish_limit,
            admissions: BTreeMap::new(),
            retired_admissions: BTreeMap::new(),
            admission_operations: BTreeMap::new(),
            records: BTreeMap::new(),
            authorizations: BTreeMap::new(),
            next_authorization: 1,
            remote_admissions: BTreeMap::new(),
            refusals: BTreeMap::new(),
            establish_rejections: BTreeSet::new(),
        }
    }
}

impl EstablishIssueLedger {
    pub(crate) fn new(
        activation: AttemptActivationIdentity,
        required: BTreeSet<ParticipationRef>,
        admission_limit: NonZeroUsize,
        establish_limit: NonZeroUsize,
    ) -> Result<Self, EstablishIssueError> {
        let mut builder = EstablishLedgerBuilder::new(activation.execution());
        for participation in required {
            builder.push(participation)?;
        }
        Ok(builder.finish(activation, admission_limit, establish_limit))
    }

    pub(crate) fn shared_required(&self) -> Arc<BTreeSet<ParticipationRef>> {
        Arc::clone(&self.required)
    }

    pub(crate) fn authority_open(&self) -> bool {
        self.authority_open
    }
    pub(crate) fn close_authority(&mut self) {
        self.authority_open = false;
    }
    #[cfg(test)]
    pub(crate) fn authorization_limit(&self) -> NonZeroUsize {
        self.establish_limit
    }
    pub(crate) fn required_participations(&self) -> &BTreeSet<ParticipationRef> {
        &self.required
    }

    pub(crate) fn authorize(
        &mut self,
        now: MonotonicInstant,
        request: AttemptAuthorization,
    ) -> Result<AttemptAuthorizationDecision, EstablishIssueError> {
        match request {
            AttemptAuthorization::BeginAdmission(request) => self
                .begin_admission(request, now)
                .map(AttemptAuthorizationDecision::AdmissionStarted),
            AttemptAuthorization::SettleAdmission {
                issue,
                participation,
                operation_id,
                outcome,
                receipt,
                prior_transport_unknown,
            } => {
                let disposition = self.settle_admission(
                    issue,
                    participation,
                    operation_id,
                    outcome,
                    receipt,
                    prior_transport_unknown,
                    now,
                )?;
                Ok(match disposition {
                    AdmissionIssueDisposition::Granted => {
                        AttemptAuthorizationDecision::AdmissionGranted
                    }
                    AdmissionIssueDisposition::Retryable => {
                        AttemptAuthorizationDecision::AdmissionRetryable
                    }
                })
            }
            AttemptAuthorization::Establish {
                participation,
                operation_id,
                ticket_id,
                semantics,
                native_compatibility_id,
            } => self
                .authorize_establish(
                    EstablishIssueIdentity {
                        activation: self.activation,
                        participation,
                        operation_id,
                        ticket_id,
                        semantics,
                        native_compatibility_id,
                    },
                    now,
                )
                .map(AttemptAuthorizationDecision::EstablishAuthorized),
            AttemptAuthorization::ReauthorizeEstablish {
                participation,
                operation_id,
            } => self
                .reauthorize_establish(participation, operation_id, now)
                .map(AttemptAuthorizationDecision::EstablishAuthorized),
        }
    }

    fn check_participation(
        &self,
        participation: ParticipationRef,
    ) -> Result<(), EstablishIssueError> {
        if participation.query_execution_id() != self.activation.execution() {
            Err(EstablishIssueError::ActivationExecutionMismatch)
        } else if !self.required.contains(&participation) {
            Err(EstablishIssueError::UnknownParticipation)
        } else {
            Ok(())
        }
    }

    fn check_authority(&self) -> Result<(), EstablishIssueError> {
        if self.authority_open {
            Ok(())
        } else {
            Err(EstablishIssueError::IssueAuthorityRevoked)
        }
    }

    fn allocate_authorization(
        &mut self,
        issued: IssuedAuthorization,
    ) -> Result<AttemptAuthorizationRef, EstablishIssueError> {
        let next = self
            .next_authorization
            .checked_add(1)
            .ok_or(EstablishIssueError::AuthorizationGenerationExhausted)?;
        let id = AttemptAuthorizationRef::new(
            NonZeroU64::new(self.next_authorization).expect("authorization sequence is nonzero"),
        );
        assert!(
            self.authorizations.insert(id, issued).is_none(),
            "authorization identity was reused"
        );
        self.next_authorization = next;
        Ok(id)
    }

    fn begin_admission(
        &mut self,
        request: AcquireParticipationAdmissionTicket,
        now: MonotonicInstant,
    ) -> Result<AttemptAuthorizationRef, EstablishIssueError> {
        let receipt = AdmissionIssueReceipt::from_request(self.activation, request, now)?;
        let participation = receipt.participation;
        self.check_participation(participation)?;
        self.check_authority()?;
        if let Some(reference) = self.admission_operations.get(&receipt.operation_id) {
            let IssuedAuthorization::Admission(original) = self
                .authorizations
                .get(reference)
                .expect("operation index names an issued admission")
            else {
                panic!("admission operation index names an Establish authorization");
            };
            if !original.authorizes_same_request(receipt) {
                return Err(EstablishIssueError::ConflictingAdmissionIssue);
            }
            return if self.admissions.get(&participation).is_some_and(|current| {
                current.authorization == *reference && current.state == AdmissionIssueState::Pending
            }) {
                Ok(*reference)
            } else {
                Err(EstablishIssueError::AdmissionReplayClosed)
            };
        }
        let generation = if let Some(existing) = self.admissions.get(&participation) {
            let replaceable = match existing.state {
                AdmissionIssueState::RetryableNoGrant(_) | AdmissionIssueState::ExpiredGrant(_) => {
                    true
                }
                AdmissionIssueState::Granted {
                    conservative_expiry,
                    ..
                } if now.has_reached(conservative_expiry) => self
                    .records
                    .get(&participation)
                    .is_some_and(|record| record.state == EstablishIssueState::TicketHeld),
                _ => false,
            };
            if !replaceable {
                return Err(EstablishIssueError::ConflictingAdmissionIssue);
            }
            if existing.generation >= self.admission_limit.get() {
                return Err(EstablishIssueError::AdmissionIssueBudgetExhausted);
            }
            existing
                .generation
                .checked_add(1)
                .ok_or(EstablishIssueError::AuthorizationGenerationExhausted)?
        } else {
            1
        };
        let authorization = self.allocate_authorization(IssuedAuthorization::Admission(receipt))?;
        if let Some(existing) = self.admissions.remove(&participation) {
            assert!(
                self.retired_admissions
                    .insert((participation, existing.receipt.operation_id), existing)
                    .is_none(),
                "admission operation was retired twice"
            );
            self.records.remove(&participation);
        }
        self.admission_operations
            .insert(receipt.operation_id, authorization);
        self.admissions.insert(
            participation,
            AdmissionIssueRecord {
                receipt,
                generation,
                authorization,
                state: AdmissionIssueState::Pending,
            },
        );
        self.add_remote_admission(participation);
        self.refresh_readiness(participation);
        Ok(authorization)
    }

    fn settle_admission(
        &mut self,
        issue: AttemptAuthorizationRef,
        participation: ParticipationRef,
        operation_id: TaskOperationId,
        outcome: Option<OperationOutcome>,
        ticket: Option<ParticipationAdmissionTicketReceipt>,
        prior_unknown: bool,
        now: MonotonicInstant,
    ) -> Result<AdmissionIssueDisposition, EstablishIssueError> {
        self.check_participation(participation)?;
        let Some(IssuedAuthorization::Admission(receipt)) =
            self.authorizations.get(&issue).copied()
        else {
            return Err(EstablishIssueError::InvalidAdmissionSettlement);
        };
        if receipt.participation != participation
            || receipt.operation_id != operation_id
            || receipt.activation != self.activation
        {
            return Err(EstablishIssueError::ConflictingAdmissionIssue);
        }
        let current = self
            .admissions
            .get(&participation)
            .ok_or(EstablishIssueError::InvalidAdmissionSettlement)?;
        if current.authorization != issue {
            let retired = self
                .retired_admissions
                .get(&(participation, operation_id))
                .ok_or(EstablishIssueError::ConflictingAdmissionIssue)?;
            if retired.receipt != receipt {
                return Err(EstablishIssueError::ConflictingAdmissionIssue);
            }
            return replay_retired_admission(retired.state, outcome, ticket, now);
        }
        if receipt.first_sent_at > now {
            return Err(EstablishIssueError::AdmissionTimeInFuture);
        }
        let old = current.state;
        let (next, result) = match (outcome, ticket) {
            (Some(OperationOutcome::Accepted | OperationOutcome::Idempotent), Some(ticket)) => {
                if ticket.participation() != participation
                    || ticket.valid_for() != receipt.valid_for
                {
                    return Err(EstablishIssueError::InvalidAdmissionSettlement);
                }
                let expiry = receipt
                    .first_sent_at
                    .saturating_add(ticket.valid_for().get());
                match old {
                    AdmissionIssueState::Pending => {
                        if now.has_reached(expiry) {
                            (
                                AdmissionIssueState::ExpiredGrant(ticket),
                                Ok(AdmissionIssueDisposition::Retryable),
                            )
                        } else {
                            (
                                AdmissionIssueState::Granted {
                                    ticket,
                                    conservative_expiry: expiry,
                                },
                                Ok(AdmissionIssueDisposition::Granted),
                            )
                        }
                    }
                    AdmissionIssueState::Granted {
                        ticket: original,
                        conservative_expiry,
                    } if original == ticket && conservative_expiry == expiry => {
                        return Ok(if now.has_reached(expiry) {
                            AdmissionIssueDisposition::Retryable
                        } else {
                            AdmissionIssueDisposition::Granted
                        });
                    }
                    AdmissionIssueState::ExpiredGrant(original) if original == ticket => {
                        return Ok(AdmissionIssueDisposition::Retryable);
                    }
                    _ => return Err(EstablishIssueError::InvalidAdmissionSettlement),
                }
            }
            (
                Some(
                    outcome @ (OperationOutcome::OperationTimedOut
                    | OperationOutcome::ResourceExhausted
                    | OperationOutcome::AdmissionTicketStillActive),
                ),
                None,
            ) => match old {
                AdmissionIssueState::Pending => (
                    AdmissionIssueState::RetryableNoGrant(outcome),
                    Ok(AdmissionIssueDisposition::Retryable),
                ),
                AdmissionIssueState::RetryableNoGrant(original) if original == outcome => {
                    return Ok(AdmissionIssueDisposition::Retryable);
                }
                _ => return Err(EstablishIssueError::InvalidAdmissionSettlement),
            },
            (None, None) => match old {
                AdmissionIssueState::Pending
                | AdmissionIssueState::PreWorkerRejected
                | AdmissionIssueState::PreWorkerRejectedAfterUnknown => (
                    if prior_unknown {
                        AdmissionIssueState::PreWorkerRejectedAfterUnknown
                    } else {
                        AdmissionIssueState::PreWorkerRejected
                    },
                    Err(EstablishIssueError::AdmissionRejected),
                ),
                _ => return Err(EstablishIssueError::InvalidAdmissionSettlement),
            },
            (Some(outcome), None) => match old {
                AdmissionIssueState::Pending => (
                    AdmissionIssueState::Rejected(outcome),
                    Err(EstablishIssueError::AdmissionRejected),
                ),
                AdmissionIssueState::Rejected(original) if original == outcome => {
                    return Err(EstablishIssueError::AdmissionRejected);
                }
                _ => return Err(EstablishIssueError::InvalidAdmissionSettlement),
            },
            (_, Some(_)) => return Err(EstablishIssueError::InvalidAdmissionSettlement),
        };
        self.replace_remote_admission(participation, old, next);
        self.admissions
            .get_mut(&participation)
            .expect("current admission remains owned")
            .state = next;
        if let AdmissionIssueState::Granted {
            ticket,
            conservative_expiry,
        } = next
        {
            self.records.insert(
                participation,
                EstablishIssueRecord {
                    activation: self.activation,
                    ticket,
                    conservative_expiry,
                    identity: None,
                    authorization_generation: 0,
                    prior_transport_unknown: false,
                    state: EstablishIssueState::TicketHeld,
                },
            );
        }
        self.refresh_readiness(participation);
        result
    }

    pub(crate) fn import_admission(
        &mut self,
        request: AcquireParticipationAdmissionTicket,
        ticket: ParticipationAdmissionTicketReceipt,
        first_sent_at: MonotonicInstant,
        now: MonotonicInstant,
    ) -> Result<AdmissionIssueDisposition, EstablishIssueError> {
        let authorization = self.begin_admission(request, first_sent_at)?;
        self.settle_admission(
            authorization,
            request.participation(),
            request.envelope().operation_id(),
            Some(OperationOutcome::Accepted),
            Some(ticket),
            false,
            now,
        )
    }

    fn authorize_establish(
        &mut self,
        identity: EstablishIssueIdentity,
        now: MonotonicInstant,
    ) -> Result<AttemptAuthorizationRef, EstablishIssueError> {
        self.check_participation(identity.participation)?;
        self.check_authority()?;
        let record = self
            .records
            .get(&identity.participation)
            .ok_or(EstablishIssueError::UnknownParticipation)?;
        if record.activation != identity.activation {
            return Err(EstablishIssueError::IdentityMismatch);
        }
        if record.ticket.participation() != identity.participation
            || record.ticket.ticket_id() != identity.ticket_id
        {
            return Err(EstablishIssueError::TicketDoesNotAuthorizeRequest);
        }
        if record.state != EstablishIssueState::TicketHeld {
            return Err(EstablishIssueError::WrongState);
        }
        if now.has_reached(record.conservative_expiry) {
            return Err(EstablishIssueError::TicketExpired);
        }
        self.mint_establish(identity)
    }

    fn reauthorize_establish(
        &mut self,
        participation: ParticipationRef,
        operation_id: TaskOperationId,
        now: MonotonicInstant,
    ) -> Result<AttemptAuthorizationRef, EstablishIssueError> {
        self.check_participation(participation)?;
        self.check_authority()?;
        let record = self
            .records
            .get(&participation)
            .ok_or(EstablishIssueError::UnknownParticipation)?;
        let identity = record
            .identity
            .ok_or(EstablishIssueError::IdentityMismatch)?;
        if identity.operation_id != operation_id || identity.activation != self.activation {
            return Err(EstablishIssueError::IdentityMismatch);
        }
        match record.operation_state() {
            EstablishIssueState::DefinitelyUnsent
                if now.has_reached(record.conservative_expiry) =>
            {
                return Err(EstablishIssueError::TicketExpired);
            }
            EstablishIssueState::DefinitelyUnsent | EstablishIssueState::TransportUnknown => {}
            _ => return Err(EstablishIssueError::WrongState),
        }
        self.mint_establish(identity)
    }

    fn mint_establish(
        &mut self,
        identity: EstablishIssueIdentity,
    ) -> Result<AttemptAuthorizationRef, EstablishIssueError> {
        let record = self
            .records
            .get(&identity.participation)
            .ok_or(EstablishIssueError::UnknownParticipation)?;
        if record.authorization_generation >= self.establish_limit.get() as u64 {
            return Err(EstablishIssueError::AuthorizationBudgetExhausted);
        }
        let generation = record
            .authorization_generation
            .checked_add(1)
            .ok_or(EstablishIssueError::AuthorizationGenerationExhausted)?;
        let reference = self.allocate_authorization(IssuedAuthorization::Establish {
            identity,
            generation,
        })?;
        let record = self
            .records
            .get_mut(&identity.participation)
            .expect("held ticket remains owned");
        record.identity = Some(identity);
        record.authorization_generation = generation;
        record.prior_transport_unknown |= record.state == EstablishIssueState::TransportUnknown;
        record.state = EstablishIssueState::IssueAuthorized;
        self.refresh_readiness(identity.participation);
        Ok(reference)
    }

    pub(crate) fn settle_establish(
        &mut self,
        authorization: AttemptAuthorizationRef,
        participation: ParticipationRef,
        operation_id: TaskOperationId,
        settlement: AttemptIssueSettlement,
    ) -> Result<bool, EstablishIssueError> {
        self.check_participation(participation)?;
        let Some(issued) = self.authorizations.get(&authorization).copied() else {
            return Err(if authorization.get() >= self.next_authorization {
                EstablishIssueError::FutureAuthorization
            } else {
                EstablishIssueError::IdentityMismatch
            });
        };
        let IssuedAuthorization::Establish {
            identity,
            generation,
        } = issued
        else {
            return Err(EstablishIssueError::IdentityMismatch);
        };
        if identity.participation != participation || identity.operation_id != operation_id {
            return Err(EstablishIssueError::IdentityMismatch);
        }
        let record = self
            .records
            .get_mut(&participation)
            .ok_or(EstablishIssueError::UnknownParticipation)?;
        if record.identity != Some(identity) {
            return Err(EstablishIssueError::IdentityMismatch);
        }
        if generation > record.authorization_generation {
            return Err(EstablishIssueError::FutureAuthorization);
        }
        let changed = if generation < record.authorization_generation {
            match settlement {
                AttemptIssueSettlement::Worker(outcome) => {
                    settle_worker(record, EstablishWorkerSettlement::from_outcome(outcome))?
                }
                _ => false,
            }
        } else if let AttemptIssueSettlement::Worker(outcome) = settlement {
            match record.state {
                EstablishIssueState::TransportOwned
                | EstablishIssueState::TransportUnknown
                | EstablishIssueState::WorkerSettled(_) => {
                    settle_worker(record, EstablishWorkerSettlement::from_outcome(outcome))?
                }
                _ => return Err(EstablishIssueError::WrongState),
            }
        } else if matches!(
            record.state,
            EstablishIssueState::WorkerSettled(_) | EstablishIssueState::PreWorkerRejected
        ) {
            false
        } else {
            let next = match (record.state, settlement) {
                (EstablishIssueState::IssueAuthorized, AttemptIssueSettlement::TransportOwned) => {
                    EstablishIssueState::TransportOwned
                }
                (
                    EstablishIssueState::IssueAuthorized | EstablishIssueState::TransportOwned,
                    AttemptIssueSettlement::DefinitelyUnsent,
                ) => EstablishIssueState::DefinitelyUnsent,
                (EstablishIssueState::TransportOwned, AttemptIssueSettlement::TransportUnknown) => {
                    EstablishIssueState::TransportUnknown
                }
                (
                    EstablishIssueState::TransportOwned | EstablishIssueState::TransportUnknown,
                    AttemptIssueSettlement::PreWorkerRejected,
                ) => EstablishIssueState::PreWorkerRejected,
                (EstablishIssueState::TransportOwned, AttemptIssueSettlement::TransportOwned)
                | (
                    EstablishIssueState::DefinitelyUnsent,
                    AttemptIssueSettlement::DefinitelyUnsent,
                )
                | (
                    EstablishIssueState::TransportUnknown,
                    AttemptIssueSettlement::TransportUnknown,
                ) => return Ok(false),
                _ => return Err(EstablishIssueError::WrongState),
            };
            record.state = next;
            true
        };
        self.refresh_readiness(participation);
        Ok(changed)
    }

    pub(crate) fn snapshot(
        &self,
        participation: ParticipationRef,
    ) -> Option<EstablishIssueSnapshot> {
        self.records
            .get(&participation)
            .copied()
            .map(EstablishIssueRecord::snapshot)
    }
    #[cfg(test)]
    pub(crate) fn has_establish_rejection(&self) -> bool {
        !self.establish_rejections.is_empty()
    }

    pub(crate) fn stand_down_fact(
        &self,
        participation: ParticipationRef,
    ) -> Result<EstablishStandDownFact, EstablishIssueError> {
        self.check_participation(participation)?;
        Ok(
            if self.records.contains_key(&participation)
                || self.remote_admissions.contains_key(&participation)
            {
                EstablishStandDownFact::AbortRequired
            } else {
                EstablishStandDownFact::NoGrant
            },
        )
    }

    pub(crate) fn record_result_withdrawal(
        &mut self,
        participation: ParticipationRef,
    ) -> Result<(), EstablishIssueError> {
        self.check_participation(participation)?;
        self.withdrawn.insert(participation);
        self.refresh_readiness(participation);
        Ok(())
    }

    pub(crate) fn ensure_success_ready(&self) -> Result<(), EstablishIssueError> {
        if let Some((_, error)) = self.refusals.first_key_value() {
            Err(*error)
        } else if !self.pending.is_empty() {
            Err(EstablishIssueError::EstablishNotSettled)
        } else {
            Ok(())
        }
    }

    /// Correlation is independent of policy. Create refusal cannot borrow an
    /// untransported parent, even though authorization already needs stand-down.
    pub(crate) fn correlates(&self, refusal: &ExecutionFailureInput) -> bool {
        let Some((participation, operation_id, kind)) =
            PreReadyProcessReplacementWitness::refusal_operation(refusal)
        else {
            return false;
        };
        if participation.query_execution_id() != self.activation.execution() {
            return false;
        }
        self.snapshot(participation).is_some_and(|snapshot| {
            snapshot.identity.is_some_and(|identity| {
                identity.activation == self.activation
                    && identity.participation == participation
                    && match kind {
                        OperationKind::UpdateParticipation => identity.operation_id == operation_id,
                        OperationKind::CreateTask => matches!(
                            snapshot.state,
                            EstablishIssueState::TransportOwned
                                | EstablishIssueState::TransportUnknown
                                | EstablishIssueState::WorkerSettled(
                                    EstablishWorkerSettlement::Applied
                                        | EstablishWorkerSettlement::Rejected(
                                            OperationOutcome::IdentityMismatch
                                        )
                                )
                        ),
                        _ => false,
                    }
            })
        })
    }

    fn add_remote_admission(&mut self, participation: ParticipationRef) {
        let count = self.remote_admissions.entry(participation).or_default();
        *count = count
            .checked_add(1)
            .expect("bounded admission count overflowed");
    }

    fn replace_remote_admission(
        &mut self,
        participation: ParticipationRef,
        old: AdmissionIssueState,
        next: AdmissionIssueState,
    ) {
        match (old.may_hold_capacity(), next.may_hold_capacity()) {
            (false, true) => self.add_remote_admission(participation),
            (true, false) => {
                let count = self
                    .remote_admissions
                    .get_mut(&participation)
                    .expect("held admission has its conservative index");
                *count = count
                    .checked_sub(1)
                    .expect("held admission count underflowed");
                if *count == 0 {
                    self.remote_admissions.remove(&participation);
                }
            }
            _ => {}
        }
    }

    fn refresh_readiness(&mut self, participation: ParticipationRef) {
        self.pending.remove(&participation);
        self.refusals.remove(&participation);
        let establish_state = self.records.get(&participation).map(|record| record.state);
        if matches!(
            establish_state,
            Some(
                EstablishIssueState::WorkerSettled(EstablishWorkerSettlement::Rejected(_))
                    | EstablishIssueState::PreWorkerRejected
            )
        ) {
            self.establish_rejections.insert(participation);
        } else {
            self.establish_rejections.remove(&participation);
        }
        if self.withdrawn.contains(&participation) {
            return;
        }
        if self.admissions.get(&participation).is_some_and(|record| {
            matches!(
                record.state,
                AdmissionIssueState::Rejected(_)
                    | AdmissionIssueState::PreWorkerRejected
                    | AdmissionIssueState::PreWorkerRejectedAfterUnknown
            )
        }) {
            self.refusals
                .insert(participation, EstablishIssueError::AdmissionRejected);
        } else {
            match establish_state {
                Some(EstablishIssueState::WorkerSettled(EstablishWorkerSettlement::Applied)) => {}
                Some(
                    EstablishIssueState::WorkerSettled(EstablishWorkerSettlement::Rejected(_))
                    | EstablishIssueState::PreWorkerRejected,
                ) => {
                    self.refusals
                        .insert(participation, EstablishIssueError::EstablishRejected);
                }
                _ => {
                    self.pending.insert(participation);
                }
            }
        }
    }
}

fn settle_worker(
    record: &mut EstablishIssueRecord,
    settlement: EstablishWorkerSettlement,
) -> Result<bool, EstablishIssueError> {
    match record.state {
        EstablishIssueState::WorkerSettled(existing) if existing == settlement => Ok(false),
        EstablishIssueState::WorkerSettled(_) => {
            Err(EstablishIssueError::ConflictingWorkerSettlement)
        }
        _ => {
            record.state = EstablishIssueState::WorkerSettled(settlement);
            record.prior_transport_unknown = false;
            Ok(true)
        }
    }
}

fn replay_retired_admission(
    state: AdmissionIssueState,
    outcome: Option<OperationOutcome>,
    ticket: Option<ParticipationAdmissionTicketReceipt>,
    now: MonotonicInstant,
) -> Result<AdmissionIssueDisposition, EstablishIssueError> {
    match (state, outcome, ticket) {
        (
            AdmissionIssueState::Granted {
                ticket: original,
                conservative_expiry,
            },
            Some(OperationOutcome::Accepted | OperationOutcome::Idempotent),
            Some(ticket),
        ) if original == ticket => Ok(if now.has_reached(conservative_expiry) {
            AdmissionIssueDisposition::Retryable
        } else {
            AdmissionIssueDisposition::Granted
        }),
        (
            AdmissionIssueState::ExpiredGrant(original),
            Some(OperationOutcome::Accepted | OperationOutcome::Idempotent),
            Some(ticket),
        ) if original == ticket => Ok(AdmissionIssueDisposition::Retryable),
        (AdmissionIssueState::RetryableNoGrant(original), Some(outcome), None)
            if original == outcome =>
        {
            Ok(AdmissionIssueDisposition::Retryable)
        }
        _ => Err(EstablishIssueError::InvalidAdmissionSettlement),
    }
}

#[cfg(test)]
mod tests;
