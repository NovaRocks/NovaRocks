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

//! Actor-owned authorization for issuing one exact query-context establish.
//!
//! This module owns no transport. It treats confidential request content as an
//! opaque value and retains the exact request `Arc` while issue responsibility
//! remains unsettled. Worker settlement releases that request and keeps only
//! its typed, secret-free identity. Move-only permits close the cancellation
//! window between actor authorization and Native transport ownership.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;

use novarocks_execution_contract::{
    AcquireQueryContextAdmissionTicket, AdmissionEpochCapability, AdmissionTicketId,
    CodecOwnedContent, ConfidentialContent, CredentialEpoch, CredentialLeaseId,
    EstablishQueryContext, EstablishSemanticIdentity, LeaseSequence, LeaseValidFor,
    OperationOutcome, QueryContextAdmissionTicketReceipt, QueryContextRef, TaskOperationId,
};
use novarocks_types::NativeCompatibilityId;
use tokio::sync::mpsc;

use super::{AttemptActivationIdentity, EstablishStandDownFact, MonotonicInstant};

/// Unforgeable actor receipt for the first issue of one exact admission request.
///
/// Replays retain both the operation identity and the first send time. A caller
/// therefore cannot extend a Worker's admission grant by reminting local time
/// for the same context.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionIssueReceipt {
    activation: AttemptActivationIdentity,
    operation_id: TaskOperationId,
    context: QueryContextRef,
    valid_for: LeaseValidFor,
    native_compatibility_id: NativeCompatibilityId,
    admission_epoch_capability: AdmissionEpochCapability,
    first_sent_at: MonotonicInstant,
}

/// Exact Worker settlement for one admission operation.
///
/// The type itself fixes the operation kind. Applied outcomes must carry the
/// grant they acknowledge, while every rejection must carry no grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionIssueSettlement {
    operation_id: TaskOperationId,
    outcome: OperationOutcome,
    ticket: Option<QueryContextAdmissionTicketReceipt>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionIssueDisposition {
    Granted,
    Retryable,
}

impl AdmissionIssueSettlement {
    pub fn applied(
        operation_id: TaskOperationId,
        outcome: OperationOutcome,
        ticket: QueryContextAdmissionTicketReceipt,
    ) -> Result<Self, EstablishIssueError> {
        if !matches!(
            outcome,
            OperationOutcome::Accepted | OperationOutcome::Idempotent
        ) {
            return Err(EstablishIssueError::InvalidAdmissionSettlement);
        }
        Ok(Self {
            operation_id,
            outcome,
            ticket: Some(ticket),
        })
    }

    pub fn rejected(
        operation_id: TaskOperationId,
        outcome: OperationOutcome,
    ) -> Result<Self, EstablishIssueError> {
        if matches!(
            outcome,
            OperationOutcome::Accepted | OperationOutcome::Idempotent
        ) {
            return Err(EstablishIssueError::InvalidAdmissionSettlement);
        }
        Ok(Self {
            operation_id,
            outcome,
            ticket: None,
        })
    }

    pub const fn operation_id(self) -> TaskOperationId {
        self.operation_id
    }

    pub const fn outcome(self) -> OperationOutcome {
        self.outcome
    }

    pub const fn ticket(self) -> Option<QueryContextAdmissionTicketReceipt> {
        self.ticket
    }
}

impl AdmissionIssueReceipt {
    fn from_request(
        activation: AttemptActivationIdentity,
        request: AcquireQueryContextAdmissionTicket,
        first_sent_at: MonotonicInstant,
    ) -> Result<Self, EstablishIssueError> {
        if activation.execution() != request.context().query_execution_id() {
            return Err(EstablishIssueError::ActivationExecutionMismatch);
        }
        Ok(Self {
            activation,
            operation_id: request.envelope().operation_id(),
            context: request.context(),
            valid_for: request.valid_for(),
            native_compatibility_id: request.native_compatibility_id(),
            admission_epoch_capability: request.admission_epoch_capability(),
            first_sent_at,
        })
    }

    pub const fn operation_id(self) -> TaskOperationId {
        self.operation_id
    }

    pub const fn context(self) -> QueryContextRef {
        self.context
    }

    fn authorizes_same_request(self, other: Self) -> bool {
        self.activation == other.activation
            && self.operation_id == other.operation_id
            && self.context == other.context
            && self.valid_for == other.valid_for
            && self.native_compatibility_id == other.native_compatibility_id
            && self.admission_epoch_capability == other.admission_epoch_capability
    }
}

/// The exact, typed identity of one authorized Establish issue.
///
/// The request's semantic projection remains typed rather than being folded
/// into another digest. Routing, replay, admission, and wire compatibility are
/// separate fields because the actor needs to validate each of them directly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EstablishIssueIdentity {
    activation: AttemptActivationIdentity,
    context: QueryContextRef,
    operation_id: TaskOperationId,
    ticket_id: AdmissionTicketId,
    semantics: EstablishSemanticIdentity,
    native_compatibility_id: NativeCompatibilityId,
}

impl EstablishIssueIdentity {
    /// Constructs the identity from the exact immutable request.
    ///
    /// Kept crate-private so an adapter cannot present independently authored
    /// fields as proof of actor authorization.
    pub(crate) fn from_request(
        activation: AttemptActivationIdentity,
        request: &EstablishQueryContext,
        native_compatibility_id: NativeCompatibilityId,
    ) -> Result<Self, EstablishIssueError> {
        let context = request.context();
        if activation.execution() != context.query_execution_id() {
            return Err(EstablishIssueError::ActivationExecutionMismatch);
        }
        Ok(Self {
            activation,
            context,
            operation_id: request.envelope().operation_id(),
            ticket_id: request.admission_ticket_id(),
            semantics: request.semantic_identity(),
            native_compatibility_id,
        })
    }

    pub const fn activation(self) -> AttemptActivationIdentity {
        self.activation
    }

    pub const fn context(self) -> QueryContextRef {
        self.context
    }

    pub const fn operation_id(self) -> TaskOperationId {
        self.operation_id
    }

    pub const fn ticket_id(self) -> AdmissionTicketId {
        self.ticket_id
    }

    pub const fn semantics(self) -> EstablishSemanticIdentity {
        self.semantics
    }

    pub const fn native_compatibility_id(self) -> NativeCompatibilityId {
        self.native_compatibility_id
    }
}

/// The normalized terminal fact published by the Worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EstablishWorkerSettlement {
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

/// The monotonic issue state retained by the logical execution actor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EstablishIssueState {
    TicketHeld,
    IssueAuthorized,
    TransportOwned,
    DefinitelyUnsent,
    TransportUnknown,
    WorkerSettled(EstablishWorkerSettlement),
}

/// Immutable observation of one context's Establish issue ledger.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EstablishIssueSnapshot {
    ticket: QueryContextAdmissionTicketReceipt,
    conservative_expiry: MonotonicInstant,
    identity: Option<EstablishIssueIdentity>,
    state: EstablishIssueState,
}

/// Borrowed, non-cloneable projection used by the transport encoder after it
/// owns the Establish guard. It exposes every field needed for encoding while
/// keeping the actor-retained request allocation private.
#[derive(Clone, Copy)]
pub struct EstablishTransportRequest<'a> {
    request: &'a EstablishQueryContext,
}

impl<'a> EstablishTransportRequest<'a> {
    pub const fn operation_id(self) -> TaskOperationId {
        self.request.envelope().operation_id()
    }

    pub const fn context(self) -> QueryContextRef {
        self.request.context()
    }

    pub const fn admission_ticket_id(self) -> AdmissionTicketId {
        self.request.admission_ticket_id()
    }

    pub fn catalog_binding(self) -> &'a dyn CodecOwnedContent {
        self.request.catalog_binding().as_ref()
    }

    pub fn initial_runtime_filter(self) -> &'a dyn CodecOwnedContent {
        self.request.initial_runtime_filter().as_ref()
    }

    pub fn query_options(self) -> &'a dyn CodecOwnedContent {
        self.request.query_options().as_ref()
    }

    pub const fn credential_lease_id(self) -> CredentialLeaseId {
        self.request.initial_credential().lease_id()
    }

    pub const fn credential_epoch(self) -> CredentialEpoch {
        self.request.initial_credential().epoch()
    }

    pub fn credential_material(self) -> &'a dyn ConfidentialContent {
        self.request.initial_credential().material().as_ref()
    }

    pub const fn initial_lease_sequence(self) -> LeaseSequence {
        self.request.initial_lease_sequence()
    }

    pub const fn initial_lease_valid_for(self) -> LeaseValidFor {
        self.request.initial_lease_valid_for()
    }
}

impl fmt::Debug for EstablishTransportRequest<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EstablishTransportRequest")
            .field("operation_id", &self.operation_id())
            .field("context", &self.context())
            .field("admission_ticket_id", &self.admission_ticket_id())
            .field("credential_lease_id", &self.credential_lease_id())
            .field("credential_epoch", &self.credential_epoch())
            .field("initial_lease_sequence", &self.initial_lease_sequence())
            .field("initial_lease_valid_for", &self.initial_lease_valid_for())
            .finish_non_exhaustive()
    }
}

impl EstablishIssueSnapshot {
    pub const fn ticket(self) -> QueryContextAdmissionTicketReceipt {
        self.ticket
    }

    pub const fn conservative_expiry(self) -> MonotonicInstant {
        self.conservative_expiry
    }

    pub const fn identity(self) -> Option<EstablishIssueIdentity> {
        self.identity
    }

    pub const fn state(self) -> EstablishIssueState {
        self.state
    }
}

/// Fail-closed violations of the Establish issue protocol.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EstablishIssueError {
    ActivationExecutionMismatch,
    UnknownContext,
    ConflictingAdmissionIssue,
    AdmissionReplayClosed,
    AdmissionIssueBudgetExhausted,
    InvalidAdmissionSettlement,
    AdmissionRejected,
    ConflictingTicket,
    AdmissionTimeInFuture,
    TicketExpired,
    TicketDoesNotAuthorizeRequest,
    WrongState,
    IdentityMismatch,
    RetainedRequestMissing,
    IssueAuthorityRevoked,
    AuthorizationGenerationExhausted,
    AuthorizationBudgetExhausted,
    FutureAuthorization,
    ConflictingWorkerSettlement,
    EstablishNotSettled,
    EstablishRejected,
    EventChannelClosed,
}

impl fmt::Display for EstablishIssueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ActivationExecutionMismatch => {
                "attempt activation and query context name different executions"
            }
            Self::UnknownContext => "Establish issue names a context with no held ticket",
            Self::ConflictingAdmissionIssue => {
                "query context admission was already issued with different immutable facts"
            }
            Self::AdmissionReplayClosed => "settled admission operation cannot be issued again",
            Self::AdmissionIssueBudgetExhausted => {
                "query context admission issue budget is exhausted"
            }
            Self::InvalidAdmissionSettlement => {
                "admission settlement does not match its exact request or outcome shape"
            }
            Self::AdmissionRejected => "Worker rejected query context admission",
            Self::ConflictingTicket => "query context already holds a different admission ticket",
            Self::AdmissionTimeInFuture => {
                "admission issue time is later than the actor's current monotonic time"
            }
            Self::TicketExpired => "admission ticket expired before Establish issue authorization",
            Self::TicketDoesNotAuthorizeRequest => {
                "held admission ticket does not authorize the Establish request"
            }
            Self::WrongState => "Establish issue is not legal in the current ledger state",
            Self::IdentityMismatch => "Establish event does not match its authorized request",
            Self::RetainedRequestMissing => {
                "Establish replay authority lost its exact immutable request instance"
            }
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
            Self::EventChannelClosed => "Establish issue actor event channel is closed",
        })
    }
}

impl std::error::Error for EstablishIssueError {}

#[derive(Debug)]
struct EstablishIssueRecord {
    activation: AttemptActivationIdentity,
    ticket: QueryContextAdmissionTicketReceipt,
    conservative_expiry: MonotonicInstant,
    identity: Option<EstablishIssueIdentity>,
    request: Option<Arc<EstablishQueryContext>>,
    authorization_generation: u64,
    state: EstablishIssueState,
}

impl EstablishIssueRecord {
    fn snapshot(&self) -> EstablishIssueSnapshot {
        EstablishIssueSnapshot {
            ticket: self.ticket,
            conservative_expiry: self.conservative_expiry,
            identity: self.identity,
            state: self.state,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EstablishIssueEventKind {
    TransportOwned,
    DefinitelyUnsent,
    TransportUnknown,
    WorkerSettled(EstablishWorkerSettlement),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct EstablishIssueEvent {
    identity: EstablishIssueIdentity,
    authorization_generation: u64,
    kind: EstablishIssueEventKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdmissionIssueState {
    Pending,
    RetryableNoGrant(OperationOutcome),
    Granted {
        ticket: QueryContextAdmissionTicketReceipt,
        conservative_expiry: MonotonicInstant,
    },
    ExpiredGrant(QueryContextAdmissionTicketReceipt),
    Rejected(OperationOutcome),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AdmissionIssueRecord {
    receipt: AdmissionIssueReceipt,
    generation: usize,
    state: AdmissionIssueState,
}

/// Pure actor-owned state for Establish issue authorization and settlement.
///
/// One unbounded sender is created per ledger. The ledger state permits at most
/// one live permit or submission per context, and both context count and
/// authorization count are bounded. Each authorization publishes at most two
/// fixed-size events: transport ownership plus one settlement. Late Worker
/// carriers contain no request and cannot exceed the authorization budget, so
/// the actor-owned queue remains bounded by three times the product of both
/// limits (owned, unknown, and a possible late Worker settlement).
/// A closed receiver fails the caller instead of silently losing authority.
#[derive(Debug)]
pub(crate) struct EstablishIssueLedger {
    required_contexts: BTreeSet<QueryContextRef>,
    max_admission_issues_per_context: NonZeroUsize,
    max_authorizations_per_context: NonZeroUsize,
    admission_issues: BTreeMap<QueryContextRef, AdmissionIssueRecord>,
    retired_admission_issues: BTreeMap<(QueryContextRef, TaskOperationId), AdmissionIssueRecord>,
    records: BTreeMap<QueryContextRef, EstablishIssueRecord>,
    events: mpsc::UnboundedSender<EstablishIssueEvent>,
    event_rx: mpsc::UnboundedReceiver<EstablishIssueEvent>,
    fence: Arc<EstablishIssueFence>,
}

#[derive(Debug)]
struct EstablishIssueFence {
    open: Mutex<bool>,
}

impl EstablishIssueFence {
    fn new() -> Self {
        Self {
            open: Mutex::new(true),
        }
    }

    fn revoke(&self) {
        *self
            .open
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = false;
    }

    fn publish_transport_owned<T>(
        &self,
        publish: impl FnOnce() -> Result<T, EstablishIssueError>,
    ) -> Result<T, EstablishIssueError> {
        let open = self
            .open
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if !*open {
            return Err(EstablishIssueError::IssueAuthorityRevoked);
        }
        publish()
    }

    fn is_open(&self) -> bool {
        *self
            .open
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}

impl EstablishIssueLedger {
    pub(crate) fn new(
        required_contexts: BTreeSet<QueryContextRef>,
        max_admission_issues_per_context: NonZeroUsize,
        max_authorizations_per_context: NonZeroUsize,
    ) -> Self {
        let (events, event_rx) = mpsc::unbounded_channel();
        Self {
            required_contexts,
            max_admission_issues_per_context,
            max_authorizations_per_context,
            admission_issues: BTreeMap::new(),
            retired_admission_issues: BTreeMap::new(),
            records: BTreeMap::new(),
            events,
            event_rx,
            fence: Arc::new(EstablishIssueFence::new()),
        }
    }

    /// Freezes the first local send time for one exact admission operation.
    /// Exact replays return the original receipt. A new operation is legal
    /// only after the prior operation proved that no usable grant remains.
    pub(crate) fn begin_admission_issue(
        &mut self,
        activation: AttemptActivationIdentity,
        request: AcquireQueryContextAdmissionTicket,
        now: MonotonicInstant,
    ) -> Result<AdmissionIssueReceipt, EstablishIssueError> {
        let receipt = AdmissionIssueReceipt::from_request(activation, request, now)?;
        if !self.required_contexts.contains(&receipt.context) {
            return Err(EstablishIssueError::UnknownContext);
        }
        if !self.fence.is_open() {
            return Err(EstablishIssueError::IssueAuthorityRevoked);
        }
        let context = receipt.context;
        if let Some(existing) = self
            .admission_issues
            .values()
            .find(|record| record.receipt.operation_id == receipt.operation_id)
            .copied()
        {
            if !existing.receipt.authorizes_same_request(receipt) {
                return Err(EstablishIssueError::ConflictingAdmissionIssue);
            }
            return if existing.state == AdmissionIssueState::Pending {
                Ok(existing.receipt)
            } else {
                Err(EstablishIssueError::AdmissionReplayClosed)
            };
        }
        if let Some(existing) = self
            .retired_admission_issues
            .values()
            .find(|record| record.receipt.operation_id == receipt.operation_id)
        {
            return if existing.receipt.authorizes_same_request(receipt) {
                Err(EstablishIssueError::AdmissionReplayClosed)
            } else {
                Err(EstablishIssueError::ConflictingAdmissionIssue)
            };
        }
        let Some(existing) = self.admission_issues.get(&context).copied() else {
            self.admission_issues.insert(
                context,
                AdmissionIssueRecord {
                    receipt,
                    generation: 1,
                    state: AdmissionIssueState::Pending,
                },
            );
            return Ok(receipt);
        };
        let replaceable = match existing.state {
            AdmissionIssueState::RetryableNoGrant(_) | AdmissionIssueState::ExpiredGrant(_) => true,
            AdmissionIssueState::Granted {
                conservative_expiry,
                ..
            } if now.has_reached(conservative_expiry) => self
                .records
                .get(&context)
                .is_some_and(|record| record.state == EstablishIssueState::TicketHeld),
            AdmissionIssueState::Pending
            | AdmissionIssueState::Granted { .. }
            | AdmissionIssueState::Rejected(_) => false,
        };
        if !replaceable {
            return Err(EstablishIssueError::ConflictingAdmissionIssue);
        }
        if existing.generation >= self.max_admission_issues_per_context.get() {
            return Err(EstablishIssueError::AdmissionIssueBudgetExhausted);
        }
        self.records.remove(&context);
        self.retired_admission_issues
            .insert((context, existing.receipt.operation_id), existing);
        self.admission_issues.insert(
            context,
            AdmissionIssueRecord {
                receipt,
                generation: existing.generation + 1,
                state: AdmissionIssueState::Pending,
            },
        );
        Ok(receipt)
    }

    /// Closes the attempt's Establish issue authority. This linearizes with
    /// every pre-transport permit transition, so a permit cannot become
    /// transport-owned after logical execution cancellation or conclusion.
    pub(crate) fn revoke_issue_authority(&mut self) {
        self.fence.revoke();
        for record in self.records.values_mut() {
            if record.state == EstablishIssueState::DefinitelyUnsent {
                record.request = None;
            }
        }
    }

    /// Applies one exact Worker admission settlement.
    pub(crate) fn settle_admission_issue(
        &mut self,
        activation: AttemptActivationIdentity,
        admission_issue: AdmissionIssueReceipt,
        settlement: AdmissionIssueSettlement,
        now: MonotonicInstant,
    ) -> Result<AdmissionIssueDisposition, EstablishIssueError> {
        let context = admission_issue.context;
        if admission_issue.activation != activation
            || settlement.operation_id != admission_issue.operation_id
        {
            return Err(EstablishIssueError::ConflictingAdmissionIssue);
        }
        if self
            .admission_issues
            .get(&context)
            .is_none_or(|record| record.receipt != admission_issue)
        {
            let retired = self
                .retired_admission_issues
                .get(&(context, admission_issue.operation_id))
                .ok_or(EstablishIssueError::ConflictingAdmissionIssue)?;
            if retired.receipt != admission_issue {
                return Err(EstablishIssueError::ConflictingAdmissionIssue);
            }
            return replay_retired_admission_settlement(retired, settlement, now);
        }
        let record = self
            .admission_issues
            .get_mut(&context)
            .ok_or(EstablishIssueError::ConflictingAdmissionIssue)?;
        if admission_issue.first_sent_at > now {
            return Err(EstablishIssueError::AdmissionTimeInFuture);
        }
        match (settlement.outcome, settlement.ticket) {
            (OperationOutcome::Accepted | OperationOutcome::Idempotent, Some(ticket)) => {
                if ticket.context() != context || ticket.valid_for() != admission_issue.valid_for {
                    return Err(EstablishIssueError::InvalidAdmissionSettlement);
                }
                let conservative_expiry = admission_issue
                    .first_sent_at
                    .saturating_add(ticket.valid_for().get());
                match record.state {
                    AdmissionIssueState::Pending => {}
                    AdmissionIssueState::Granted {
                        ticket: existing,
                        conservative_expiry: existing_expiry,
                    } if existing == ticket && existing_expiry == conservative_expiry => {
                        return Ok(if now.has_reached(conservative_expiry) {
                            AdmissionIssueDisposition::Retryable
                        } else {
                            AdmissionIssueDisposition::Granted
                        });
                    }
                    AdmissionIssueState::ExpiredGrant(existing) if existing == ticket => {
                        return Ok(AdmissionIssueDisposition::Retryable);
                    }
                    _ => return Err(EstablishIssueError::InvalidAdmissionSettlement),
                }
                if now.has_reached(conservative_expiry) {
                    record.state = AdmissionIssueState::ExpiredGrant(ticket);
                    return Ok(AdmissionIssueDisposition::Retryable);
                }
                record.state = AdmissionIssueState::Granted {
                    ticket,
                    conservative_expiry,
                };
                self.records.insert(
                    context,
                    EstablishIssueRecord {
                        activation,
                        ticket,
                        conservative_expiry,
                        identity: None,
                        request: None,
                        authorization_generation: 0,
                        state: EstablishIssueState::TicketHeld,
                    },
                );
                Ok(AdmissionIssueDisposition::Granted)
            }
            (
                outcome @ (OperationOutcome::OperationTimedOut
                | OperationOutcome::ResourceExhausted
                | OperationOutcome::AdmissionTicketStillActive),
                None,
            ) => match record.state {
                AdmissionIssueState::Pending => {
                    record.state = AdmissionIssueState::RetryableNoGrant(outcome);
                    Ok(AdmissionIssueDisposition::Retryable)
                }
                AdmissionIssueState::RetryableNoGrant(existing) if existing == outcome => {
                    Ok(AdmissionIssueDisposition::Retryable)
                }
                _ => Err(EstablishIssueError::InvalidAdmissionSettlement),
            },
            (outcome, None) => match record.state {
                AdmissionIssueState::Pending => {
                    record.state = AdmissionIssueState::Rejected(outcome);
                    Err(EstablishIssueError::AdmissionRejected)
                }
                AdmissionIssueState::Rejected(existing) if existing == outcome => {
                    Err(EstablishIssueError::AdmissionRejected)
                }
                _ => Err(EstablishIssueError::InvalidAdmissionSettlement),
            },
            (_, Some(_)) => Err(EstablishIssueError::InvalidAdmissionSettlement),
        }
    }

    /// Authorizes the first issue from a held ticket.
    pub(crate) fn authorize_issue(
        &mut self,
        activation: AttemptActivationIdentity,
        request: Arc<EstablishQueryContext>,
        native_compatibility_id: NativeCompatibilityId,
        now: MonotonicInstant,
    ) -> Result<EstablishIssuePermit, EstablishIssueError> {
        self.drain_events()?;
        let identity = EstablishIssueIdentity::from_request(
            activation,
            request.as_ref(),
            native_compatibility_id,
        )?;
        let record = self
            .records
            .get_mut(&identity.context())
            .ok_or(EstablishIssueError::UnknownContext)?;
        if record.activation != activation {
            return Err(EstablishIssueError::IdentityMismatch);
        }
        if record.ticket.context() != identity.context()
            || record.ticket.ticket_id() != identity.ticket_id()
        {
            return Err(EstablishIssueError::TicketDoesNotAuthorizeRequest);
        }

        if record.state != EstablishIssueState::TicketHeld {
            return Err(EstablishIssueError::WrongState);
        }
        if now.has_reached(record.conservative_expiry) {
            return Err(EstablishIssueError::TicketExpired);
        }
        record.identity = Some(identity);
        record.request = Some(Arc::clone(&request));

        mint_issue_permit(
            record,
            identity,
            request,
            &self.events,
            &self.fence,
            self.max_authorizations_per_context,
        )
    }

    /// Re-signs only the actor-retained request after a definitely-unsent or
    /// transport-unknown settlement. The caller names the context but cannot
    /// substitute another payload, ticket, operation, or compatibility id.
    pub(crate) fn reauthorize_issue(
        &mut self,
        activation: AttemptActivationIdentity,
        context: QueryContextRef,
        now: MonotonicInstant,
    ) -> Result<EstablishIssuePermit, EstablishIssueError> {
        self.drain_events()?;
        let record = self
            .records
            .get_mut(&context)
            .ok_or(EstablishIssueError::UnknownContext)?;
        if record.activation != activation {
            return Err(EstablishIssueError::IdentityMismatch);
        }
        match record.state {
            EstablishIssueState::DefinitelyUnsent => {
                if now.has_reached(record.conservative_expiry) {
                    return Err(EstablishIssueError::TicketExpired);
                }
            }
            // An unknown issue may already have redeemed the ticket. Its exact
            // replay remains legal after the original ticket validity elapsed.
            EstablishIssueState::TransportUnknown => {}
            _ => return Err(EstablishIssueError::WrongState),
        }
        let identity = record
            .identity
            .ok_or(EstablishIssueError::IdentityMismatch)?;
        let request = Arc::clone(
            record
                .request
                .as_ref()
                .ok_or(EstablishIssueError::RetainedRequestMissing)?,
        );
        mint_issue_permit(
            record,
            identity,
            request,
            &self.events,
            &self.fence,
            self.max_authorizations_per_context,
        )
    }

    /// Applies every fixed-size event currently published by issue permits.
    pub(crate) fn drain_events(&mut self) -> Result<usize, EstablishIssueError> {
        let mut applied = 0;
        loop {
            match self.event_rx.try_recv() {
                Ok(event) => {
                    applied += usize::from(self.apply_event(event)?);
                }
                Err(mpsc::error::TryRecvError::Empty) => return Ok(applied),
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return Err(EstablishIssueError::EventChannelClosed);
                }
            }
        }
    }

    /// Waits for and applies one permit or transport settlement event.
    ///
    /// The logical execution actor selects this future alongside its bounded
    /// command mailbox, so issue settlement never depends on polling.
    pub(crate) async fn apply_next_event(&mut self) -> Result<(), EstablishIssueError> {
        let event = self
            .event_rx
            .recv()
            .await
            .ok_or(EstablishIssueError::EventChannelClosed)?;
        self.apply_event(event).map(|_| ())
    }

    pub(crate) fn snapshot(&self, context: QueryContextRef) -> Option<EstablishIssueSnapshot> {
        self.records
            .get(&context)
            .map(EstablishIssueRecord::snapshot)
    }

    /// Projects only the facts needed by actor-owned context stand-down.
    ///
    /// A granted admission ticket remains an Abort responsibility even when
    /// Establish was definitely unsent. Worker Abort owns both the absent-
    /// context fence and prompt return of that ticket's reserved capacity.
    pub(crate) fn stand_down_facts(
        &mut self,
    ) -> Result<Vec<(QueryContextRef, EstablishStandDownFact)>, EstablishIssueError> {
        self.drain_events()?;
        Ok(self
            .required_contexts
            .iter()
            .map(|context| {
                let fact = if self
                    .records
                    .get(context)
                    .is_some_and(|record| record.state == EstablishIssueState::IssueAuthorized)
                {
                    EstablishStandDownFact::IssueUnsettled
                } else if self.records.contains_key(context)
                    || self
                        .admission_issues
                        .get(context)
                        .is_some_and(|record| admission_may_hold_worker_capacity(record.state))
                    || self
                        .retired_admission_issues
                        .iter()
                        .any(|((retired, _), record)| {
                            retired == context && admission_may_hold_worker_capacity(record.state)
                        })
                {
                    EstablishStandDownFact::AbortRequired
                } else {
                    EstablishStandDownFact::NoGrant
                };
                (*context, fact)
            })
            .collect())
    }

    pub(crate) fn has_worker_rejection(&self) -> bool {
        self.records.values().any(|record| {
            matches!(
                record.state,
                EstablishIssueState::WorkerSettled(EstablishWorkerSettlement::Rejected(_))
            )
        })
    }

    pub(crate) fn ensure_success_ready(&mut self) -> Result<(), EstablishIssueError> {
        self.drain_events()?;
        for context in &self.required_contexts {
            match self.records.get(context).map(|record| record.state) {
                None => return Err(EstablishIssueError::EstablishNotSettled),
                Some(EstablishIssueState::WorkerSettled(EstablishWorkerSettlement::Applied)) => {}
                Some(EstablishIssueState::WorkerSettled(EstablishWorkerSettlement::Rejected(
                    _,
                ))) => {
                    return Err(EstablishIssueError::EstablishRejected);
                }
                Some(_) => return Err(EstablishIssueError::EstablishNotSettled),
            }
        }
        Ok(())
    }

    fn apply_event(&mut self, event: EstablishIssueEvent) -> Result<bool, EstablishIssueError> {
        let issue_authority_open = self.fence.is_open();
        let record = self
            .records
            .get_mut(&event.identity.context())
            .ok_or(EstablishIssueError::UnknownContext)?;
        if record.identity != Some(event.identity) {
            return Err(EstablishIssueError::IdentityMismatch);
        }

        if event.authorization_generation > record.authorization_generation {
            return Err(EstablishIssueError::FutureAuthorization);
        }
        if event.authorization_generation < record.authorization_generation {
            return match event.kind {
                EstablishIssueEventKind::WorkerSettled(outcome) => settle_worker(record, outcome),
                EstablishIssueEventKind::TransportOwned
                | EstablishIssueEventKind::DefinitelyUnsent
                | EstablishIssueEventKind::TransportUnknown => Ok(false),
            };
        }

        if let EstablishIssueEventKind::WorkerSettled(outcome) = event.kind {
            return match record.state {
                EstablishIssueState::TransportOwned | EstablishIssueState::WorkerSettled(_) => {
                    settle_worker(record, outcome)
                }
                _ => Err(EstablishIssueError::WrongState),
            };
        }
        if matches!(record.state, EstablishIssueState::WorkerSettled(_)) {
            return Ok(false);
        }

        let next = match (record.state, event.kind) {
            (EstablishIssueState::IssueAuthorized, EstablishIssueEventKind::TransportOwned) => {
                EstablishIssueState::TransportOwned
            }
            (EstablishIssueState::IssueAuthorized, EstablishIssueEventKind::DefinitelyUnsent)
            | (EstablishIssueState::TransportOwned, EstablishIssueEventKind::DefinitelyUnsent) => {
                EstablishIssueState::DefinitelyUnsent
            }
            (EstablishIssueState::TransportOwned, EstablishIssueEventKind::TransportUnknown) => {
                EstablishIssueState::TransportUnknown
            }
            (state, kind) if event_matches_state(state, kind) => return Ok(false),
            _ => return Err(EstablishIssueError::WrongState),
        };
        record.state = next;
        if next == EstablishIssueState::DefinitelyUnsent && !issue_authority_open {
            record.request = None;
        }
        Ok(true)
    }
}

fn admission_may_hold_worker_capacity(state: AdmissionIssueState) -> bool {
    matches!(
        state,
        AdmissionIssueState::Pending
            | AdmissionIssueState::Granted { .. }
            | AdmissionIssueState::ExpiredGrant(_)
            | AdmissionIssueState::RetryableNoGrant(OperationOutcome::AdmissionTicketStillActive)
    )
}

fn replay_retired_admission_settlement(
    retired: &AdmissionIssueRecord,
    settlement: AdmissionIssueSettlement,
    now: MonotonicInstant,
) -> Result<AdmissionIssueDisposition, EstablishIssueError> {
    match (retired.state, settlement.outcome, settlement.ticket) {
        (
            AdmissionIssueState::Granted {
                ticket,
                conservative_expiry,
            },
            OperationOutcome::Accepted | OperationOutcome::Idempotent,
            Some(replayed_ticket),
        ) if replayed_ticket == ticket => Ok(if now.has_reached(conservative_expiry) {
            AdmissionIssueDisposition::Retryable
        } else {
            AdmissionIssueDisposition::Granted
        }),
        (
            AdmissionIssueState::ExpiredGrant(ticket),
            OperationOutcome::Accepted | OperationOutcome::Idempotent,
            Some(replayed_ticket),
        ) if replayed_ticket == ticket => Ok(AdmissionIssueDisposition::Retryable),
        (AdmissionIssueState::RetryableNoGrant(outcome), replayed_outcome, None)
            if replayed_outcome == outcome =>
        {
            Ok(AdmissionIssueDisposition::Retryable)
        }
        _ => Err(EstablishIssueError::InvalidAdmissionSettlement),
    }
}

fn mint_issue_permit(
    record: &mut EstablishIssueRecord,
    identity: EstablishIssueIdentity,
    request: Arc<EstablishQueryContext>,
    events: &mpsc::UnboundedSender<EstablishIssueEvent>,
    fence: &Arc<EstablishIssueFence>,
    max_authorizations_per_context: NonZeroUsize,
) -> Result<EstablishIssuePermit, EstablishIssueError> {
    if !fence.is_open() {
        return Err(EstablishIssueError::IssueAuthorityRevoked);
    }
    if record.authorization_generation >= max_authorizations_per_context.get() as u64 {
        return Err(EstablishIssueError::AuthorizationBudgetExhausted);
    }
    record.authorization_generation = record
        .authorization_generation
        .checked_add(1)
        .ok_or(EstablishIssueError::AuthorizationGenerationExhausted)?;
    record.state = EstablishIssueState::IssueAuthorized;
    Ok(EstablishIssuePermit {
        identity,
        authorization_generation: record.authorization_generation,
        request: Some(request),
        events: events.clone(),
        fence: Arc::clone(fence),
    })
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
            record.request = None;
            Ok(true)
        }
    }
}

fn event_matches_state(state: EstablishIssueState, event: EstablishIssueEventKind) -> bool {
    matches!(
        (state, event),
        (
            EstablishIssueState::TransportOwned,
            EstablishIssueEventKind::TransportOwned
        ) | (
            EstablishIssueState::DefinitelyUnsent,
            EstablishIssueEventKind::DefinitelyUnsent
        ) | (
            EstablishIssueState::TransportUnknown,
            EstablishIssueEventKind::TransportUnknown
        )
    )
}

/// Move-only authorization that has not crossed into transport ownership.
#[derive(Debug)]
#[must_use = "an authorized Establish must enter transport ownership or settle definitely unsent"]
pub struct EstablishIssuePermit {
    identity: EstablishIssueIdentity,
    authorization_generation: u64,
    request: Option<Arc<EstablishQueryContext>>,
    events: mpsc::UnboundedSender<EstablishIssueEvent>,
    fence: Arc<EstablishIssueFence>,
}

impl EstablishIssuePermit {
    pub const fn identity(&self) -> EstablishIssueIdentity {
        self.identity
    }

    /// Hands this exact request to a non-blocking transport reservation.
    /// Capacity is reserved before the sink can inspect the request. The short
    /// issue-fence section then linearizes transport ownership against actor
    /// revocation, and the already-reserved handoff runs after that lock is
    /// released.
    pub fn try_submit(
        mut self,
        sink: &dyn EstablishTransportSink,
    ) -> Result<EstablishIssueSubmit, EstablishIssueError> {
        let reservation = match sink.try_reserve(self.identity) {
            EstablishTransportAdmission::Admitted(reservation) => reservation,
            EstablishTransportAdmission::Backpressured => {
                return Ok(EstablishIssueSubmit::Backpressured);
            }
        };
        let fence = Arc::clone(&self.fence);
        let submission = fence.publish_transport_owned(|| {
            self.publish(EstablishIssueEventKind::TransportOwned)?;
            let request = self
                .request
                .take()
                .expect("live Establish issue permit retains its exact request");
            Ok(EstablishTransportSubmission {
                identity: self.identity,
                authorization_generation: self.authorization_generation,
                request: Some(request),
                events: self.events.clone(),
            })
        })?;
        reservation.submit(submission);
        Ok(EstablishIssueSubmit::Accepted)
    }

    fn publish(&self, kind: EstablishIssueEventKind) -> Result<(), EstablishIssueError> {
        self.events
            .send(EstablishIssueEvent {
                identity: self.identity,
                authorization_generation: self.authorization_generation,
                kind,
            })
            .map_err(|_| EstablishIssueError::EventChannelClosed)
    }
}

impl Drop for EstablishIssuePermit {
    fn drop(&mut self) {
        if self.request.is_some() {
            let _ = self.publish(EstablishIssueEventKind::DefinitelyUnsent);
        }
    }
}

/// Result of the synchronous transport admission boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EstablishIssueSubmit {
    Accepted,
    Backpressured,
}

/// The only transport seam allowed to receive an Establish request.
pub trait EstablishTransportSink: fmt::Debug + Send + Sync {
    fn try_reserve(&self, identity: EstablishIssueIdentity) -> EstablishTransportAdmission;
}

/// Capacity is decided from secret-free identity before transport may inspect
/// or encode the request.
pub enum EstablishTransportAdmission {
    Admitted(Box<dyn EstablishTransportReservation>),
    Backpressured,
}

impl fmt::Debug for EstablishTransportAdmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admitted(_) => formatter.write_str("Admitted(..)"),
            Self::Backpressured => formatter.write_str("Backpressured"),
        }
    }
}

/// A pre-reserved, non-blocking handoff owned by the transport. Its `submit`
/// method receives the request only after actor revocation loses the race.
pub trait EstablishTransportReservation: fmt::Debug + Send {
    fn submit(self: Box<Self>, submission: EstablishTransportSubmission);
}

/// Move-only ownership of an Establish accepted by transport.
#[derive(Debug)]
#[must_use = "a transport-owned Establish must publish its exact settlement"]
pub struct EstablishTransportSubmission {
    identity: EstablishIssueIdentity,
    authorization_generation: u64,
    request: Option<Arc<EstablishQueryContext>>,
    events: mpsc::UnboundedSender<EstablishIssueEvent>,
}

impl EstablishTransportSubmission {
    pub const fn identity(&self) -> EstablishIssueIdentity {
        self.identity
    }

    pub fn request(&self) -> EstablishTransportRequest<'_> {
        EstablishTransportRequest {
            request: self
                .request
                .as_deref()
                .expect("live transport-owned Establish guard retains its exact request"),
        }
    }

    pub fn transport_unknown(
        mut self,
    ) -> Result<LateEstablishWorkerSettlement, EstablishIssueError> {
        self.settle(EstablishIssueEventKind::TransportUnknown)?;
        Ok(LateEstablishWorkerSettlement {
            identity: self.identity,
            authorization_generation: self.authorization_generation,
            events: self.events.clone(),
        })
    }

    pub fn worker_settled(mut self, outcome: OperationOutcome) -> Result<(), EstablishIssueError> {
        self.settle(EstablishIssueEventKind::WorkerSettled(
            EstablishWorkerSettlement::from_outcome(outcome),
        ))
    }

    fn settle(&mut self, kind: EstablishIssueEventKind) -> Result<(), EstablishIssueError> {
        self.publish(kind)?;
        self.request.take();
        Ok(())
    }

    fn publish(&self, kind: EstablishIssueEventKind) -> Result<(), EstablishIssueError> {
        self.events
            .send(EstablishIssueEvent {
                identity: self.identity,
                authorization_generation: self.authorization_generation,
                kind,
            })
            .map_err(|_| EstablishIssueError::EventChannelClosed)
    }
}

impl Drop for EstablishTransportSubmission {
    fn drop(&mut self) {
        if self.request.is_some() {
            let _ = self.publish(EstablishIssueEventKind::TransportUnknown);
        }
    }
}

/// Bounded carrier that lets reconciliation publish a Worker receipt after
/// the original transport attempt became unknown and a replay was issued.
#[derive(Debug)]
pub struct LateEstablishWorkerSettlement {
    identity: EstablishIssueIdentity,
    authorization_generation: u64,
    events: mpsc::UnboundedSender<EstablishIssueEvent>,
}

impl LateEstablishWorkerSettlement {
    pub const fn identity(&self) -> EstablishIssueIdentity {
        self.identity
    }

    pub fn worker_settled(self, outcome: OperationOutcome) -> Result<(), EstablishIssueError> {
        self.events
            .send(EstablishIssueEvent {
                identity: self.identity,
                authorization_generation: self.authorization_generation,
                kind: EstablishIssueEventKind::WorkerSettled(
                    EstablishWorkerSettlement::from_outcome(outcome),
                ),
            })
            .map_err(|_| EstablishIssueError::EventChannelClosed)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::Duration;

    use novarocks_execution_contract::{
        CodecOwnedContent, ConfidentialContent, ContentFingerprint, CredentialEpoch,
        CredentialLeaseId, CredentialUpdate, LeaseValidFor,
    };
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
    };
    use tokio::runtime::Handle;

    use super::*;
    use crate::coordination::{
        ExecutionEffect, LogicalExecutionActorConfig, LogicalExecutionActorError,
        LogicalExecutionClock, spawn_logical_execution_actor,
    };

    #[derive(Debug)]
    struct FakeContent(ContentFingerprint);

    impl CodecOwnedContent for FakeContent {
        fn fingerprint(&self) -> ContentFingerprint {
            self.0
        }

        fn encoded_len(&self) -> usize {
            1
        }
    }

    struct FakeSecret;

    impl ConfidentialContent for FakeSecret {
        fn encoded_len(&self) -> usize {
            1
        }

        fn matches(&self, other: &dyn ConfidentialContent) -> bool {
            other.encoded_len() == 1
        }
    }

    #[derive(Debug, Default)]
    struct AcceptingTransport {
        accepted: Arc<Mutex<Option<EstablishTransportSubmission>>>,
    }

    impl EstablishTransportSink for AcceptingTransport {
        fn try_reserve(&self, _identity: EstablishIssueIdentity) -> EstablishTransportAdmission {
            EstablishTransportAdmission::Admitted(Box::new(AcceptingReservation {
                accepted: Arc::clone(&self.accepted),
            }))
        }
    }

    #[derive(Debug)]
    struct AcceptingReservation {
        accepted: Arc<Mutex<Option<EstablishTransportSubmission>>>,
    }

    impl EstablishTransportReservation for AcceptingReservation {
        fn submit(self: Box<Self>, submission: EstablishTransportSubmission) {
            let mut accepted = self.accepted.lock().expect("accepting transport");
            assert!(accepted.is_none(), "test transport accepts one submission");
            *accepted = Some(submission);
        }
    }

    impl AcceptingTransport {
        fn take(&self) -> EstablishTransportSubmission {
            self.accepted
                .lock()
                .expect("accepting transport")
                .take()
                .expect("transport accepted an Establish submission")
        }
    }

    #[derive(Debug)]
    struct ManualActorClock {
        now: Mutex<MonotonicInstant>,
    }

    impl ManualActorClock {
        fn new(now: MonotonicInstant) -> Self {
            Self {
                now: Mutex::new(now),
            }
        }

        fn set(&self, now: MonotonicInstant) {
            *self.now.lock().expect("manual actor clock") = now;
        }
    }

    impl LogicalExecutionClock for ManualActorClock {
        fn now(&self) -> MonotonicInstant {
            *self.now.lock().expect("manual actor clock")
        }
    }

    fn submit(permit: EstablishIssuePermit) -> EstablishTransportSubmission {
        let transport = AcceptingTransport::default();
        assert_eq!(
            permit.try_submit(&transport).unwrap(),
            EstablishIssueSubmit::Accepted
        );
        transport.take()
    }

    #[derive(Debug)]
    struct BackpressuredTransport;

    impl EstablishTransportSink for BackpressuredTransport {
        fn try_reserve(&self, _identity: EstablishIssueIdentity) -> EstablishTransportAdmission {
            EstablishTransportAdmission::Backpressured
        }
    }

    fn execution(query: i64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(43, query), AttemptId::new(1).unwrap()).unwrap()
    }

    fn activation(execution: QueryExecutionId) -> AttemptActivationIdentity {
        let config = LogicalExecutionActorConfig::single_attempt_completion(
            execution,
            ExecutionEffect::None,
            NonZeroUsize::new(1).unwrap(),
            Vec::new(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
        );
        let (_owner, permit) = spawn_logical_execution_actor(&Handle::current(), config).unwrap();
        permit.identity()
    }

    fn actor_config(
        execution: QueryExecutionId,
        contexts: Vec<QueryContextRef>,
    ) -> LogicalExecutionActorConfig {
        LogicalExecutionActorConfig::single_attempt_completion(
            execution,
            ExecutionEffect::None,
            NonZeroUsize::new(1).unwrap(),
            contexts,
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
        )
        .with_abort_query_context_effect_port(
            crate::coordination::PermanentlyBackpressuredAbortEffectPort::shared(),
            NonZeroUsize::new(2).unwrap(),
        )
    }

    fn context(execution: QueryExecutionId) -> QueryContextRef {
        QueryContextRef::new(
            execution,
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        )
    }

    fn ticket(context: QueryContextRef, tag: u8) -> QueryContextAdmissionTicketReceipt {
        QueryContextAdmissionTicketReceipt::new(
            AdmissionTicketId::try_from_bytes([tag; 16]).unwrap(),
            context,
            LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
        )
    }

    fn admission_request(context: QueryContextRef, tag: u8) -> AcquireQueryContextAdmissionTicket {
        AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            context,
            LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
            NativeCompatibilityId::new([tag; 32]),
            AdmissionEpochCapability::try_from_bytes([tag; 16]).unwrap(),
        )
    }

    fn hold_ticket(
        ledger: &mut EstablishIssueLedger,
        activation: AttemptActivationIdentity,
        ticket: QueryContextAdmissionTicketReceipt,
        tag: u8,
        first_sent_at: MonotonicInstant,
        observed_at: MonotonicInstant,
    ) -> AdmissionIssueReceipt {
        let admission = ledger
            .begin_admission_issue(
                activation,
                admission_request(ticket.context(), tag),
                first_sent_at,
            )
            .unwrap();
        ledger
            .settle_admission_issue(
                activation,
                admission,
                AdmissionIssueSettlement::applied(
                    admission.operation_id(),
                    OperationOutcome::Accepted,
                    ticket,
                )
                .unwrap(),
                observed_at,
            )
            .unwrap();
        admission
    }

    async fn settle_actor_ticket(
        running: &crate::coordination::RunningAttemptPermit,
        admission: AdmissionIssueReceipt,
        ticket: QueryContextAdmissionTicketReceipt,
    ) {
        assert_eq!(
            running
                .settle_admission_issue(
                    admission,
                    AdmissionIssueSettlement::applied(
                        admission.operation_id(),
                        OperationOutcome::Accepted,
                        ticket,
                    )
                    .unwrap(),
                )
                .await
                .unwrap(),
            AdmissionIssueDisposition::Granted
        );
    }

    fn establish_request(
        context: QueryContextRef,
        ticket: QueryContextAdmissionTicketReceipt,
        tag: u8,
    ) -> Arc<EstablishQueryContext> {
        let content = |offset| {
            Arc::new(FakeContent(ContentFingerprint::from_bytes(
                [tag + offset; 16],
            ))) as Arc<dyn CodecOwnedContent>
        };
        Arc::new(EstablishQueryContext::new(
            TaskOperationId::new_v7(),
            context,
            ticket.ticket_id(),
            content(0),
            content(1),
            content(2),
            CredentialUpdate::new(
                CredentialLeaseId::new(u64::from(tag)),
                CredentialEpoch::new(1).unwrap(),
                Arc::new(FakeSecret),
            ),
            LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
        ))
    }

    fn times() -> (MonotonicInstant, MonotonicInstant) {
        (
            MonotonicInstant::from_origin(Duration::from_secs(1)),
            MonotonicInstant::from_origin(Duration::from_secs(11)),
        )
    }

    fn issue_ledger(context: QueryContextRef, max_authorizations: usize) -> EstablishIssueLedger {
        EstablishIssueLedger::new(
            BTreeSet::from([context]),
            NonZeroUsize::new(3).unwrap(),
            NonZeroUsize::new(max_authorizations).unwrap(),
        )
    }

    #[tokio::test]
    async fn identity_is_typed_and_rejects_a_foreign_activation() {
        let first = execution(1);
        let second = execution(2);
        let context = context(first);
        let ticket = ticket(context, 1);
        let request = establish_request(context, ticket, 1);
        assert_eq!(
            EstablishIssueIdentity::from_request(
                activation(second),
                &request,
                NativeCompatibilityId::new([1; 32]),
            ),
            Err(EstablishIssueError::ActivationExecutionMismatch)
        );

        let activation = activation(first);
        let identity = EstablishIssueIdentity::from_request(
            activation,
            &request,
            NativeCompatibilityId::new([2; 32]),
        )
        .unwrap();
        assert_eq!(identity.activation(), activation);
        assert_eq!(identity.context(), context);
        assert_eq!(identity.operation_id(), request.envelope().operation_id());
        assert_eq!(identity.ticket_id(), ticket.ticket_id());
        assert_eq!(identity.semantics(), request.semantic_identity());
        assert_eq!(
            identity.native_compatibility_id(),
            NativeCompatibilityId::new([2; 32])
        );
    }

    #[tokio::test]
    async fn ticket_receipt_and_expiry_are_retained_exactly() {
        let execution = execution(3);
        let activation = activation(execution);
        let context = context(execution);
        let receipt = ticket(context, 3);
        let (now, expiry) = times();
        let mut ledger = issue_ledger(context, 2);
        let admission = hold_ticket(&mut ledger, activation, receipt, 3, now, now);
        let snapshot = ledger.snapshot(context).unwrap();
        assert_eq!(snapshot.ticket(), receipt);
        assert_eq!(snapshot.conservative_expiry(), expiry);
        assert_eq!(snapshot.identity(), None);
        assert_eq!(snapshot.state(), EstablishIssueState::TicketHeld);

        ledger
            .settle_admission_issue(
                activation,
                admission,
                AdmissionIssueSettlement::applied(
                    admission.operation_id(),
                    OperationOutcome::Idempotent,
                    receipt,
                )
                .unwrap(),
                expiry,
            )
            .unwrap();
        let other_context = self::context(execution);
        assert_eq!(
            ledger.begin_admission_issue(activation, admission_request(other_context, 13), now,),
            Err(EstablishIssueError::UnknownContext)
        );
    }

    #[tokio::test]
    async fn admission_replay_retains_the_first_issue_time_and_operation() {
        let execution = execution(3);
        let activation = activation(execution);
        let context = context(execution);
        let request = admission_request(context, 3);
        let first_sent_at = MonotonicInstant::from_origin(Duration::from_secs(2));
        let replayed_at = MonotonicInstant::from_origin(Duration::from_secs(8));
        let mut ledger = issue_ledger(context, 2);

        let first = ledger
            .begin_admission_issue(activation, request, first_sent_at)
            .unwrap();
        let replay = ledger
            .begin_admission_issue(activation, request, replayed_at)
            .unwrap();
        assert_eq!(replay, first);
        assert_eq!(replay.operation_id(), request.envelope().operation_id());
        assert_eq!(
            ledger.begin_admission_issue(activation, admission_request(context, 4), replayed_at,),
            Err(EstablishIssueError::ConflictingAdmissionIssue)
        );
    }

    #[tokio::test]
    async fn expired_ticket_can_be_replaced_by_one_bounded_new_admission_operation() {
        let execution = execution(3);
        let activation = activation(execution);
        let context = context(execution);
        let (now, expiry) = times();
        let mut ledger = EstablishIssueLedger::new(
            BTreeSet::from([context]),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
        );
        let first_ticket = ticket(context, 3);
        let first_request = admission_request(context, 3);
        let first_admission = ledger
            .begin_admission_issue(activation, first_request, now)
            .unwrap();
        assert_eq!(
            ledger
                .settle_admission_issue(
                    activation,
                    first_admission,
                    AdmissionIssueSettlement::applied(
                        first_admission.operation_id(),
                        OperationOutcome::Accepted,
                        first_ticket,
                    )
                    .unwrap(),
                    now,
                )
                .unwrap(),
            AdmissionIssueDisposition::Granted
        );
        assert_eq!(
            ledger.begin_admission_issue(activation, first_request, now),
            Err(EstablishIssueError::AdmissionReplayClosed)
        );

        let replacement_request = admission_request(context, 4);
        let replacement = ledger
            .begin_admission_issue(activation, replacement_request, expiry)
            .unwrap();
        assert_eq!(
            replacement.operation_id(),
            replacement_request.envelope().operation_id()
        );
        assert_eq!(ledger.snapshot(context), None);
        assert_eq!(
            ledger
                .settle_admission_issue(
                    activation,
                    first_admission,
                    AdmissionIssueSettlement::applied(
                        first_admission.operation_id(),
                        OperationOutcome::Idempotent,
                        first_ticket,
                    )
                    .unwrap(),
                    expiry,
                )
                .unwrap(),
            AdmissionIssueDisposition::Retryable
        );
        assert_eq!(
            ledger
                .settle_admission_issue(
                    activation,
                    replacement,
                    AdmissionIssueSettlement::rejected(
                        replacement.operation_id(),
                        OperationOutcome::AdmissionTicketStillActive,
                    )
                    .unwrap(),
                    expiry,
                )
                .unwrap(),
            AdmissionIssueDisposition::Retryable
        );
        assert_eq!(
            ledger.stand_down_facts().unwrap(),
            vec![(context, EstablishStandDownFact::AbortRequired)],
            "a locally expired retired grant and AdmissionTicketStillActive still own Worker capacity"
        );
        assert_eq!(
            ledger.begin_admission_issue(activation, first_request, expiry),
            Err(EstablishIssueError::AdmissionReplayClosed)
        );
        let reused_operation = AcquireQueryContextAdmissionTicket::new(
            first_request.envelope().operation_id(),
            context,
            LeaseValidFor::new(Duration::from_secs(10)).unwrap(),
            NativeCompatibilityId::new([5; 32]),
            AdmissionEpochCapability::try_from_bytes([5; 16]).unwrap(),
        );
        assert_eq!(
            ledger.begin_admission_issue(activation, reused_operation, expiry),
            Err(EstablishIssueError::ConflictingAdmissionIssue)
        );
        assert_eq!(
            ledger.begin_admission_issue(activation, admission_request(context, 5), expiry),
            Err(EstablishIssueError::AdmissionIssueBudgetExhausted)
        );
    }

    #[tokio::test]
    async fn admission_settlement_must_name_the_exact_operation_and_ticket_shape() {
        let execution = execution(3);
        let activation = activation(execution);
        let context = context(execution);
        let (now, _expiry) = times();
        let mut ledger = issue_ledger(context, 2);
        let admission = ledger
            .begin_admission_issue(activation, admission_request(context, 3), now)
            .unwrap();
        let wrong_operation = TaskOperationId::new_v7();

        assert_eq!(
            ledger.settle_admission_issue(
                activation,
                admission,
                AdmissionIssueSettlement::applied(
                    wrong_operation,
                    OperationOutcome::Accepted,
                    ticket(context, 3),
                )
                .unwrap(),
                now,
            ),
            Err(EstablishIssueError::ConflictingAdmissionIssue)
        );
        assert_eq!(
            AdmissionIssueSettlement::rejected(
                admission.operation_id(),
                OperationOutcome::Accepted,
            ),
            Err(EstablishIssueError::InvalidAdmissionSettlement)
        );
    }

    #[tokio::test]
    async fn dropping_a_permit_is_definitely_unsent_and_allows_exact_resign() {
        let execution = execution(4);
        let activation = activation(execution);
        let context = context(execution);
        let ticket = ticket(context, 4);
        let request = establish_request(context, ticket, 4);
        let (now, expiry) = times();
        let mut ledger = issue_ledger(context, 2);
        let admission = hold_ticket(&mut ledger, activation, ticket, 4, now, now);
        let permit = ledger
            .authorize_issue(
                activation,
                Arc::clone(&request),
                NativeCompatibilityId::new([4; 32]),
                now,
            )
            .unwrap();
        drop(permit);
        assert_eq!(ledger.drain_events().unwrap(), 1);
        assert_eq!(
            ledger.snapshot(context).unwrap().state(),
            EstablishIssueState::DefinitelyUnsent
        );
        ledger
            .settle_admission_issue(
                activation,
                admission,
                AdmissionIssueSettlement::applied(
                    admission.operation_id(),
                    OperationOutcome::Idempotent,
                    ticket,
                )
                .unwrap(),
                expiry,
            )
            .unwrap();

        let request_weak = Arc::downgrade(&request);
        drop(request);
        let resigned = ledger.reauthorize_issue(activation, context, now).unwrap();
        assert!(Arc::ptr_eq(
            resigned
                .request
                .as_ref()
                .expect("re-signed permit retains the exact request"),
            request_weak
                .upgrade()
                .as_ref()
                .expect("actor ledger retains the exact request")
        ));
        drop(resigned);
        ledger.drain_events().unwrap();
    }

    #[tokio::test]
    async fn revocation_releases_a_retained_definitely_unsent_request() {
        let execution = execution(4);
        let activation = activation(execution);
        let context = context(execution);
        let ticket = ticket(context, 4);
        let request = establish_request(context, ticket, 4);
        let request_weak = Arc::downgrade(&request);
        let (now, _expiry) = times();
        let mut ledger = issue_ledger(context, 2);
        hold_ticket(&mut ledger, activation, ticket, 4, now, now);
        let permit = ledger
            .authorize_issue(
                activation,
                Arc::clone(&request),
                NativeCompatibilityId::new([4; 32]),
                now,
            )
            .unwrap();
        drop(request);
        drop(permit);
        ledger.drain_events().unwrap();
        assert!(request_weak.upgrade().is_some());

        ledger.revoke_issue_authority();
        assert!(request_weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn transport_backpressure_is_the_only_external_definitely_unsent_proof() {
        let execution = execution(5);
        let activation = activation(execution);
        let context = context(execution);
        let ticket = ticket(context, 5);
        let request = establish_request(context, ticket, 5);
        let (now, _expiry) = times();
        let mut ledger = issue_ledger(context, 2);
        hold_ticket(&mut ledger, activation, ticket, 5, now, now);
        let permit = ledger
            .authorize_issue(
                activation,
                request,
                NativeCompatibilityId::new([5; 32]),
                now,
            )
            .unwrap();
        assert_eq!(
            permit.try_submit(&BackpressuredTransport).unwrap(),
            EstablishIssueSubmit::Backpressured
        );
        assert_eq!(ledger.drain_events().unwrap(), 1);
        assert_eq!(
            ledger.snapshot(context).unwrap().state(),
            EstablishIssueState::DefinitelyUnsent
        );
    }

    #[tokio::test]
    async fn transport_guard_drop_is_unknown_and_exact_replay_survives_expiry() {
        let execution = execution(5);
        let activation = activation(execution);
        let context = context(execution);
        let ticket = ticket(context, 5);
        let request = establish_request(context, ticket, 5);
        let (now, expiry) = times();
        let mut ledger = issue_ledger(context, 2);
        hold_ticket(&mut ledger, activation, ticket, 5, now, now);
        let permit = ledger
            .authorize_issue(
                activation,
                Arc::clone(&request),
                NativeCompatibilityId::new([5; 32]),
                now,
            )
            .unwrap();
        let guard = submit(permit);
        drop(guard);
        assert_eq!(ledger.drain_events().unwrap(), 2);
        assert_eq!(
            ledger.snapshot(context).unwrap().state(),
            EstablishIssueState::TransportUnknown
        );

        let after_expiry = expiry.saturating_add(Duration::from_secs(1));
        drop(request);
        let replay = ledger
            .reauthorize_issue(activation, context, after_expiry)
            .unwrap();
        drop(replay);
    }

    #[tokio::test]
    async fn exact_replay_cannot_exceed_the_context_authorization_budget() {
        let execution = execution(6);
        let activation = activation(execution);
        let context = context(execution);
        let ticket = ticket(context, 6);
        let request = establish_request(context, ticket, 6);
        let (now, _expiry) = times();
        let mut ledger = issue_ledger(context, 1);
        hold_ticket(&mut ledger, activation, ticket, 6, now, now);
        let submission = submit(
            ledger
                .authorize_issue(
                    activation,
                    request,
                    NativeCompatibilityId::new([6; 32]),
                    now,
                )
                .unwrap(),
        );
        let _late = submission.transport_unknown().unwrap();
        ledger.drain_events().unwrap();
        assert!(matches!(
            ledger.reauthorize_issue(activation, context, now),
            Err(EstablishIssueError::AuthorizationBudgetExhausted)
        ));
    }

    #[tokio::test]
    async fn worker_settlement_is_terminal_and_idempotent() {
        let execution = execution(6);
        let activation = activation(execution);
        let context = context(execution);
        let ticket = ticket(context, 6);
        let request = establish_request(context, ticket, 6);
        let request_weak = Arc::downgrade(&request);
        let (now, _expiry) = times();
        let mut ledger = issue_ledger(context, 2);
        hold_ticket(&mut ledger, activation, ticket, 6, now, now);
        let guard = submit(
            ledger
                .authorize_issue(
                    activation,
                    request,
                    NativeCompatibilityId::new([6; 32]),
                    now,
                )
                .unwrap(),
        );
        let identity = guard.identity();
        let generation = guard.authorization_generation;
        guard.worker_settled(OperationOutcome::Accepted).unwrap();
        assert_eq!(ledger.drain_events().unwrap(), 2);
        assert_eq!(
            ledger.snapshot(context).unwrap().state(),
            EstablishIssueState::WorkerSettled(EstablishWorkerSettlement::Applied)
        );
        assert!(request_weak.upgrade().is_none());
        assert!(
            !ledger
                .apply_event(EstablishIssueEvent {
                    identity,
                    authorization_generation: generation,
                    kind: EstablishIssueEventKind::WorkerSettled(
                        EstablishWorkerSettlement::Applied,
                    ),
                })
                .unwrap()
        );
    }

    #[tokio::test]
    async fn late_worker_settlement_wins_over_a_reissue_but_old_carrier_events_do_not() {
        let execution = execution(7);
        let activation = activation(execution);
        let context = context(execution);
        let ticket = ticket(context, 7);
        let request = establish_request(context, ticket, 7);
        let (now, _expiry) = times();
        let mut ledger = issue_ledger(context, 2);
        hold_ticket(&mut ledger, activation, ticket, 7, now, now);
        let first_guard = submit(
            ledger
                .authorize_issue(
                    activation,
                    Arc::clone(&request),
                    NativeCompatibilityId::new([7; 32]),
                    now,
                )
                .unwrap(),
        );
        let identity = first_guard.identity();
        let first_generation = first_guard.authorization_generation;
        let late_worker = first_guard.transport_unknown().unwrap();
        assert_eq!(ledger.drain_events().unwrap(), 2);

        drop(request);
        let second_guard = submit(ledger.reauthorize_issue(activation, context, now).unwrap());
        let second_generation = second_guard.authorization_generation;
        assert!(second_generation > first_generation);

        for kind in [
            EstablishIssueEventKind::DefinitelyUnsent,
            EstablishIssueEventKind::TransportOwned,
            EstablishIssueEventKind::TransportUnknown,
        ] {
            assert!(
                !ledger
                    .apply_event(EstablishIssueEvent {
                        identity,
                        authorization_generation: first_generation,
                        kind,
                    })
                    .unwrap()
            );
        }
        assert_eq!(
            ledger.snapshot(context).unwrap().state(),
            EstablishIssueState::IssueAuthorized
        );

        late_worker
            .worker_settled(OperationOutcome::Accepted)
            .unwrap();
        assert_eq!(ledger.drain_events().unwrap(), 2);
        assert_eq!(
            ledger.snapshot(context).unwrap().state(),
            EstablishIssueState::WorkerSettled(EstablishWorkerSettlement::Applied)
        );
        second_guard
            .worker_settled(OperationOutcome::Idempotent)
            .unwrap();
        assert_eq!(ledger.drain_events().unwrap(), 0);
        assert_eq!(
            ledger.snapshot(context).unwrap().state(),
            EstablishIssueState::WorkerSettled(EstablishWorkerSettlement::Applied)
        );
        assert_eq!(
            ledger.apply_event(EstablishIssueEvent {
                identity,
                authorization_generation: second_generation,
                kind: EstablishIssueEventKind::WorkerSettled(
                    EstablishWorkerSettlement::from_outcome(OperationOutcome::ContextConflict),
                ),
            }),
            Err(EstablishIssueError::ConflictingWorkerSettlement)
        );

        let foreign = EstablishIssueIdentity {
            native_compatibility_id: NativeCompatibilityId::new([8; 32]),
            ..identity
        };
        assert_eq!(
            ledger.apply_event(EstablishIssueEvent {
                identity: foreign,
                authorization_generation: second_generation,
                kind: EstablishIssueEventKind::DefinitelyUnsent,
            }),
            Err(EstablishIssueError::IdentityMismatch)
        );
        assert_eq!(
            ledger.apply_event(EstablishIssueEvent {
                identity,
                authorization_generation: second_generation + 1,
                kind: EstablishIssueEventKind::DefinitelyUnsent,
            }),
            Err(EstablishIssueError::FutureAuthorization)
        );
    }

    #[tokio::test]
    async fn definitely_unsent_replay_still_obeys_ticket_expiry() {
        let execution = execution(8);
        let activation = activation(execution);
        let context = context(execution);
        let ticket = ticket(context, 8);
        let request = establish_request(context, ticket, 8);
        let (now, expiry) = times();
        let mut ledger = issue_ledger(context, 2);
        hold_ticket(&mut ledger, activation, ticket, 8, now, now);
        let permit = ledger
            .authorize_issue(
                activation,
                Arc::clone(&request),
                NativeCompatibilityId::new([8; 32]),
                now,
            )
            .unwrap();
        drop(permit);
        ledger.drain_events().unwrap();
        assert!(matches!(
            ledger.reauthorize_issue(activation, context, expiry),
            Err(EstablishIssueError::TicketExpired)
        ));
    }

    #[tokio::test]
    async fn actor_derives_ticket_expiry_from_its_admission_issue_clock() {
        let execution = execution(9);
        let context = context(execution);
        let sent_at = MonotonicInstant::from_origin(Duration::from_secs(3));
        let observed_at = MonotonicInstant::from_origin(Duration::from_secs(5));
        let expected_expiry = MonotonicInstant::from_origin(Duration::from_secs(13));
        let clock = Arc::new(ManualActorClock::new(sent_at));
        let config = actor_config(execution, vec![context]).with_clock(clock.clone());
        let (owner, initial) = spawn_logical_execution_actor(&Handle::current(), config).unwrap();
        let actor = owner.actor();
        let running = actor.activate(initial.ready()).await.unwrap();
        let issue = running
            .begin_admission_issue(admission_request(context, 9))
            .await
            .unwrap();
        clock.set(observed_at);
        settle_actor_ticket(&running, issue, ticket(context, 9)).await;
        assert_eq!(
            actor
                .establish_snapshot(context)
                .await
                .unwrap()
                .unwrap()
                .conservative_expiry(),
            expected_expiry
        );
    }

    #[tokio::test]
    async fn actor_refuses_success_when_a_required_context_was_never_issued() {
        let execution = execution(10);
        let context = context(execution);
        let config = actor_config(execution, vec![context]);
        let (owner, initial) = spawn_logical_execution_actor(&Handle::current(), config).unwrap();
        let actor = owner.actor();
        let running = actor.activate(initial.ready()).await.unwrap();
        assert_eq!(
            actor.complete_attempt(running).await,
            Err(LogicalExecutionActorError::Establish(
                EstablishIssueError::EstablishNotSettled
            ))
        );
        assert_eq!(
            actor.snapshot().await.unwrap().conclusion,
            Some(crate::coordination::LogicalConclusion::Failed)
        );
    }

    #[tokio::test]
    async fn actor_owns_exact_establish_replay_until_worker_settlement() {
        let execution = execution(11);
        let context = context(execution);
        let config = actor_config(execution, vec![context]);
        let (owner, initial) = spawn_logical_execution_actor(&Handle::current(), config).unwrap();
        let actor = owner.actor();
        let running = actor.activate(initial.ready()).await.unwrap();
        let ticket = ticket(context, 11);
        let request = establish_request(context, ticket, 11);
        let request_weak = Arc::downgrade(&request);
        let admission = running
            .begin_admission_issue(admission_request(context, 11))
            .await
            .unwrap();

        settle_actor_ticket(&running, admission, ticket).await;
        let permit = running
            .authorize_establish(Arc::clone(&request), NativeCompatibilityId::new([11; 32]))
            .await
            .unwrap();
        drop(request);
        drop(permit);

        let replay = running.reauthorize_establish(context).await.unwrap();
        assert!(Arc::ptr_eq(
            replay
                .request
                .as_ref()
                .expect("replay permit retains the exact request"),
            request_weak
                .upgrade()
                .as_ref()
                .expect("actor retains the exact request across replay")
        ));
        submit(replay)
            .worker_settled(OperationOutcome::Accepted)
            .unwrap();

        assert_eq!(
            actor
                .establish_snapshot(context)
                .await
                .unwrap()
                .unwrap()
                .state(),
            EstablishIssueState::WorkerSettled(EstablishWorkerSettlement::Applied)
        );
        assert!(request_weak.upgrade().is_none());
        assert_eq!(
            actor.complete_attempt(running).await.unwrap(),
            crate::coordination::LogicalConclusion::Succeeded
        );
    }

    #[tokio::test]
    async fn actor_cancellation_revokes_an_authorized_pre_transport_permit() {
        let execution = execution(12);
        let context = context(execution);
        let config = actor_config(execution, vec![context]);
        let (owner, initial) = spawn_logical_execution_actor(&Handle::current(), config).unwrap();
        let actor = owner.actor();
        let running = actor.activate(initial.ready()).await.unwrap();
        let ticket = ticket(context, 12);
        let request = establish_request(context, ticket, 12);
        let admission = running
            .begin_admission_issue(admission_request(context, 12))
            .await
            .unwrap();

        settle_actor_ticket(&running, admission, ticket).await;
        let permit = running
            .authorize_establish(request, NativeCompatibilityId::new([12; 32]))
            .await
            .unwrap();
        drop(running);

        for _ in 0..16 {
            if actor.snapshot().await.unwrap().conclusion
                == Some(crate::coordination::LogicalConclusion::Cancelled)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        let transport = AcceptingTransport::default();
        assert!(matches!(
            permit.try_submit(&transport),
            Err(EstablishIssueError::IssueAuthorityRevoked)
        ));
        for _ in 0..16 {
            if actor
                .establish_snapshot(context)
                .await
                .unwrap()
                .is_some_and(|snapshot| snapshot.state() == EstablishIssueState::DefinitelyUnsent)
            {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("revoked Establish permit did not settle definitely unsent");
    }

    #[tokio::test]
    async fn actor_keeps_consuming_settlements_after_a_protocol_failure() {
        let execution = execution(13);
        let first_context = context(execution);
        let second_context = context(execution);
        let config = actor_config(execution, vec![first_context, second_context]);
        let (owner, initial) = spawn_logical_execution_actor(&Handle::current(), config).unwrap();
        let actor = owner.actor();
        let running = actor.activate(initial.ready()).await.unwrap();
        let first_ticket = ticket(first_context, 13);
        let second_ticket = ticket(second_context, 14);
        let first_request = establish_request(first_context, first_ticket, 13);
        let second_request = establish_request(second_context, second_ticket, 14);
        let second_request_weak = Arc::downgrade(&second_request);

        let first_issue = running
            .begin_admission_issue(admission_request(first_context, 13))
            .await
            .unwrap();
        settle_actor_ticket(&running, first_issue, first_ticket).await;
        let first_submission = submit(
            running
                .authorize_establish(first_request, NativeCompatibilityId::new([13; 32]))
                .await
                .unwrap(),
        );
        let late_first = first_submission.transport_unknown().unwrap();

        let second_issue = running
            .begin_admission_issue(admission_request(second_context, 14))
            .await
            .unwrap();
        settle_actor_ticket(&running, second_issue, second_ticket).await;
        let second_submission = submit(
            running
                .authorize_establish(second_request, NativeCompatibilityId::new([14; 32]))
                .await
                .unwrap(),
        );

        submit(running.reauthorize_establish(first_context).await.unwrap())
            .worker_settled(OperationOutcome::Accepted)
            .unwrap();
        for _ in 0..16 {
            if actor
                .establish_snapshot(first_context)
                .await
                .unwrap()
                .is_some_and(|snapshot| {
                    snapshot.state()
                        == EstablishIssueState::WorkerSettled(EstablishWorkerSettlement::Applied)
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }

        late_first
            .worker_settled(OperationOutcome::ContextConflict)
            .unwrap();
        for _ in 0..16 {
            if actor.snapshot().await.unwrap().establish_error
                == Some(EstablishIssueError::ConflictingWorkerSettlement)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        second_submission
            .worker_settled(OperationOutcome::Accepted)
            .unwrap();
        for _ in 0..16 {
            if second_request_weak.upgrade().is_none() {
                let snapshot = actor.snapshot().await.unwrap();
                assert_eq!(
                    snapshot.conclusion,
                    Some(crate::coordination::LogicalConclusion::Failed)
                );
                assert_eq!(
                    snapshot.establish_error,
                    Some(EstablishIssueError::ConflictingWorkerSettlement)
                );
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("actor stopped consuming Establish settlements after protocol failure");
    }
}
