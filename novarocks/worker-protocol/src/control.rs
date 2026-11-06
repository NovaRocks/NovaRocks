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

//! Process control actor: low-frequency directory and ticket metadata.
//! Task creation/preparation capacity never traverses this mailbox.
use crate::{
    admission::{
        AcquisitionIdentity, AdmissionCore, AdmissionTicketConfig, AdmissionTicketRedemption,
    },
    creation::{AdmissionNeed, AdmissionPort, AdmissionRefusal, PayloadId, RequestReplyTag},
    lease::MonotonicInstant,
    participation::{
        CoreError, DomainRefusal, EstablishReplayRequest, ParticipationAcknowledgement,
        ParticipationEstablishment, ParticipationEstablishmentDecision, ParticipationReplyBody,
        RequestResult, RequestSettlement,
    },
    retention::ParticipationRetirementIdentity,
};
use novarocks_control_runtime_core::identity::{Generation, InputId, ReservationId};
use novarocks_execution_contract::{
    AcquireParticipationAdmissionTicket, AdmissionTicketId, EstablishSemanticIdentity,
    LeaseValidFor, OperationOutcome, ParticipationAdmissionTicketReceipt, ParticipationRef,
    ParticipationState, TaskIdentity, TaskOperationId,
};
use novarocks_execution_contract::{ContentFingerprint, CredentialDomainId, CredentialEpoch};
use novarocks_types::identity::{BackendProcessId, FrontendProcessId, QueryExecutionId};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Clone, Copy, Debug)]
pub struct ControlConfig {
    /// Derived from the original admitted ingress R inventory. Exceeding this
    /// count is an owner invariant violation, never protocol resource refusal.
    pub max_pending_requests: usize,
    pub step_budget: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EstablishSemanticFacts {
    pub catalog: ContentFingerprint,
    pub shared_filter: ContentFingerprint,
    pub query_options: ContentFingerprint,
    pub credential_domain_id: CredentialDomainId,
    pub credential_epoch: CredentialEpoch,
    pub initial_lease_valid_for: LeaseValidFor,
}
impl From<EstablishSemanticIdentity> for EstablishSemanticFacts {
    fn from(identity: EstablishSemanticIdentity) -> Self {
        Self {
            catalog: identity.catalog_binding(),
            shared_filter: identity.initial_runtime_filter(),
            query_options: identity.query_options(),
            credential_domain_id: identity.credential_domain_id(),
            credential_epoch: identity.credential_epoch(),
            initial_lease_valid_for: identity.initial_lease_valid_for(),
        }
    }
}
#[derive(Debug)]
enum ActiveControlRequest {
    Acquire(AcquisitionIdentity),
    Establish {
        participation: ParticipationRef,
        ticket: AdmissionTicketId,
        semantics: EstablishSemanticFacts,
    },
}
impl ActiveControlRequest {
    fn conflict(&self, input: &ControlInput) -> Option<OperationOutcome> {
        match (self, input) {
            (Self::Acquire(original), ControlInput::Acquire { request, .. }) => (*original
                != AcquisitionIdentity::from(*request))
            .then_some(OperationOutcome::DomainConflict),
            (
                Self::Establish {
                    participation,
                    ticket,
                    semantics,
                },
                ControlInput::Establish {
                    participation: candidate_participation,
                    ticket: candidate_ticket,
                    semantics: candidate_semantics,
                    ..
                },
            ) => (!(*participation == *candidate_participation
                && *ticket == *candidate_ticket
                && *semantics == *candidate_semantics))
                .then_some(OperationOutcome::ParticipationConflict),
            _ => Some(OperationOutcome::DomainConflict),
        }
    }
}
#[derive(Debug)]
struct ActiveOperation {
    // This shape exists exactly while the original finite token set is live.
    // Deadlines, capabilities and driver-minted IDs are not operation bodies.
    shape: ActiveControlRequest,
    tokens: BTreeSet<InputId>,
}
#[derive(Debug)]
struct PendingControlRequest {
    request: RequestReplyTag,
    // Existing-entity reads belong to their exact original RPC, independently
    // of another RPC with the same operation awaiting its creation result.
    route: Option<(ParticipationRef, MonotonicInstant)>,
}
#[derive(Debug)]
struct DirectoryRecord {
    state: ParticipationState,
    generation: Generation,
    waiters: BTreeSet<TaskOperationId>,
    reservation: Option<ReservationId>,
    reply: Option<(ControlReplyBody, MonotonicInstant)>,
    // Retention may end before the original bounded reply fanout. Keep that
    // obligation in this same paid row until its last operation moves to R.
    removal_pending: bool,
    removal_requested: bool,
    // The original protocol close or actual owner failure has its own identity.
    // A terminal phase or missing address never establishes execution exit.
    retirement: Option<ParticipationRetirementIdentity>,
    execution_retired: bool,
    participation_expired: bool,
}
#[derive(Debug)]
pub enum ControlInput {
    ContinueRequestReplies {
        operation: TaskOperationId,
    },
    Acquire {
        request: AcquireParticipationAdmissionTicket,
        ticket: AdmissionTicketId,
    },
    Establish {
        request: TaskOperationId,
        participation: ParticipationRef,
        ticket: AdmissionTicketId,
        payload: PayloadId,
        semantics: EstablishSemanticFacts,
        generation: Generation,
    },
    /// The original admitted Root R observed no actual Participation address.
    /// This is readiness observation, never Task admission or a new Participation.
    CreateWithoutParticipationAddress {
        participation: ParticipationRef,
        task: TaskIdentity,
    },
    /// The actual ingress getter failed before this RPC submitted its command.
    /// Only handle_request may settle the same original admitted token.
    IngressOwnerFailed {
        request: TaskOperationId,
    },
    /// An actual routed comparison or the target owner failed. The Root
    /// observes its own current time; a host cannot backdate this verdict.
    RoutedEstablishOwnerFailed {
        participation: ParticipationRef,
        request: RequestReplyTag,
    },
    RoutedEstablishFinished {
        participation: ParticipationRef,
        request: RequestReplyTag,
        result: Result<OperationOutcome, DomainRefusal>,
        acknowledgement: Option<ParticipationAcknowledgement>,
        decided_at: MonotonicInstant,
    },
    EstablishmentDecided {
        participation: ParticipationRef,
        decision: ParticipationEstablishmentDecision,
    },
    ContinueReplies {
        participation: ParticipationRef,
    },
    /// The original P06 host proved execution custody actually retired. This
    /// releases bootstrap execution Work while preserving its metadata route.
    ExecutionRetired {
        participation: ParticipationRef,
        retirement: ParticipationRetirementIdentity,
    },
    /// The original Participation lifecycle emitted expiry at its own H deadline.
    /// Root neither starts another timer nor infers this fact from phase.
    ParticipationExpired {
        participation: ParticipationRef,
        retirement: ParticipationRetirementIdentity,
    },
    HostRemoved {
        participation: ParticipationRef,
    },
    RevokeTicket {
        participation: ParticipationRef,
    },
    FenceFrontend {
        frontend: FrontendProcessId,
    },
    ContinueFence,
    /// Same original process owner closes new elections and drains actual
    /// Participation/ticket owners; this is not a frontend process fence.
    ProcessDrain,
    /// The real external listeners have stopped and their bodies have left.
    /// Only process-local neutral replay retention may now end early.
    ProcessIngressClosed,
    ContinueProcessDrain,
    /// Advance one currently actionable internal class. Addresses and decisions
    /// remain in their original bounded Core inventories.
    NextInternal,
    Tick,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlEffect {
    DomainFailureReply {
        request: TaskOperationId,
        refusal: DomainRefusal,
    },
    ContinueRequestReplies {
        operation: TaskOperationId,
    },
    AdmissionReply {
        request: TaskOperationId,
        result: Result<ParticipationAdmissionTicketReceipt, OperationOutcome>,
    },
    Reply {
        request: TaskOperationId,
        outcome: OperationOutcome,
    },
    Materialize {
        participation: ParticipationRef,
        establishment: ParticipationEstablishment,
        reservation: ReservationId,
        generation: Generation,
    },
    RouteEstablish {
        participation: ParticipationRef,
        request: EstablishReplayRequest,
    },
    /// Retire future read-only probe issuance after the original R deadline.
    /// Started calls and captures still need their actual exit observations.
    CancelEstablishRoute {
        participation: ParticipationRef,
        request: RequestReplyTag,
    },
    Route {
        participation: ParticipationRef,
        state: ParticipationState,
        generation: Generation,
    },
    AbortParticipation {
        participation: ParticipationRef,
        generation: Generation,
    },
    RetireReservation(ReservationId),
    RemoveHost {
        participation: ParticipationRef,
    },
    ContinueReplies {
        participation: ParticipationRef,
    },
    ContinueMaintenance,
    ContinueFence,
    /// Coalescible scheduling notice used by production actor projection. The
    /// Core's legacy addressed continuations remain available to pure callers.
    WakeInternal,
}
#[derive(Debug, Default)]
pub struct ControlStep {
    pub effects: Vec<ControlEffect>,
    pub requests: Vec<ControlRequestSettlement>,
    pub records_visited: usize,
}
/// The immutable neutral decision travels with its exact request token. A
/// host must not reconstruct a receipt or a typed failure from an operation
/// cache after another request with that operation has already been decided.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlReplyBody {
    CreateParticipation(Arc<ParticipationReplyBody>),
    Participation(ParticipationAcknowledgement),
    TicketObservation(crate::admission::AdmissionTicketObservation),
    PreflightExpired(Option<OperationOutcome>),
    Admission(Result<ParticipationAdmissionTicketReceipt, OperationOutcome>),
    Domain(DomainRefusal),
    Verdict(RequestResult),
}
impl ControlReplyBody {
    pub fn matches(&self, result: RequestResult) -> bool {
        match self {
            Self::CreateParticipation(body) => body.matches(result),
            Self::Participation(ack) => result == RequestResult::Outcome(ack.outcome),
            Self::TicketObservation(_) | Self::PreflightExpired(_) => {
                result == RequestResult::Outcome(OperationOutcome::Accepted)
            }
            Self::Admission(Ok(_)) => result == RequestResult::Outcome(OperationOutcome::Accepted),
            Self::Admission(Err(outcome)) => result == RequestResult::Outcome(*outcome),
            Self::Domain(refusal) => result == RequestResult::Outcome(refusal.outcome.outcome()),
            Self::Verdict(value) => *value == result,
        }
    }
    fn result(&self) -> RequestResult {
        match self {
            Self::CreateParticipation(body) => body.result(),
            Self::Participation(ack) => RequestResult::Outcome(ack.outcome),
            Self::TicketObservation(_) | Self::PreflightExpired(_) => {
                RequestResult::Outcome(OperationOutcome::Accepted)
            }
            Self::Admission(result) => RequestResult::Outcome(
                result
                    .as_ref()
                    .map_or_else(|outcome| *outcome, |_| OperationOutcome::Accepted),
            ),
            Self::Domain(refusal) => RequestResult::Outcome(refusal.outcome.outcome()),
            Self::Verdict(result) => *result,
        }
    }
    fn from_effect(effect: &ControlEffect) -> Option<(TaskOperationId, Self)> {
        match effect {
            ControlEffect::AdmissionReply { request, result } => {
                Some((*request, Self::Admission(*result)))
            }
            ControlEffect::DomainFailureReply { request, refusal } => {
                Some((*request, Self::Domain(refusal.clone())))
            }
            ControlEffect::Reply { request, outcome } => {
                Some((*request, Self::Verdict(RequestResult::Outcome(*outcome))))
            }
            _ => None,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlRequestSettlement {
    pub settlement: RequestSettlement,
    pub body: ControlReplyBody,
}
impl std::ops::Deref for ControlRequestSettlement {
    type Target = RequestSettlement;
    fn deref(&self) -> &Self::Target {
        &self.settlement
    }
}
impl ControlRequestSettlement {
    fn new(request: RequestReplyTag, body: ControlReplyBody) -> Self {
        Self {
            settlement: RequestSettlement {
                request,
                result: body.result(),
            },
            body,
        }
    }
    fn verdict(request: RequestReplyTag, result: RequestResult) -> Self {
        Self::new(request, ControlReplyBody::Verdict(result))
    }
}
impl ControlStep {
    /// Total cost includes outputs. Charge `records_visited` only when the
    /// runtime's per-effect emission already debits each effect/reply.
    pub fn charged_work(&self) -> usize {
        self.effects.len() + self.requests.len() + self.records_visited
    }
}
#[derive(Clone, Copy, Debug)]
enum InternalClass {
    RequestReplies,
    ParticipationReplies,
    Fence,
    Maintenance,
}
impl InternalClass {
    const ALL: [Self; 4] = [
        Self::RequestReplies,
        Self::ParticipationReplies,
        Self::Fence,
        Self::Maintenance,
    ];
    fn index(self) -> usize {
        self as usize
    }
}

/// Inline Control metadata estimate. Nested waiters, shared reply bodies,
/// tree nodes and allocator overhead remain unmeasured, not defaulted to zero.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ControlRetentionInventory {
    pub directory_records: usize,
    pub frontend_fences: usize,
    pub metadata_bytes_estimate: Option<usize>,
}
#[derive(Debug)]
pub struct ControlCore {
    // Bound once by this Worker process's original composition. A ticket from
    // another process cannot turn an address mismatch into ordinary absence.
    backend: BackendProcessId,
    config: ControlConfig,
    now: MonotonicInstant,
    admission: AdmissionCore,
    directory: BTreeMap<ParticipationRef, DirectoryRecord>,
    owners: BTreeMap<QueryExecutionId, FrontendProcessId>,
    frontend_members: BTreeMap<FrontendProcessId, BTreeSet<ParticipationRef>>,
    fenced: BTreeSet<FrontendProcessId>,
    requests: BTreeMap<InputId, PendingControlRequest>,
    request_operations: BTreeMap<TaskOperationId, ActiveOperation>,
    establish_reply_owners: BTreeMap<TaskOperationId, ParticipationRef>,
    request_deadlines: BTreeSet<(MonotonicInstant, InputId)>,
    ready: BTreeMap<TaskOperationId, (ControlReplyBody, MonotonicInstant)>,
    fencing: Option<(FrontendProcessId, Option<ParticipationRef>)>,
    // Addresses only: bodies and waiter ownership stay in directory/ready.
    reply_participations: BTreeSet<ParticipationRef>,
    pending_fences: BTreeSet<FrontendProcessId>,
    internal_cursor: usize,
    request_cursor: Option<TaskOperationId>,
    participation_cursor: Option<ParticipationRef>,
    process_draining: bool,
    process_ingress_closed: bool,
    process_drain_cursor: Option<ParticipationRef>,
    process_drain_scanned: bool,
    process_drain_ticket_next: bool,
    maintenance_drain_next: bool,
}
impl ControlCore {
    pub const MAX_STEP_WORK: usize = 15;
    pub fn max_step_work(&self) -> usize {
        Self::MAX_STEP_WORK
    }

    pub fn new(
        backend: BackendProcessId,
        config: ControlConfig,
        admission: AdmissionTicketConfig,
        issuer: [u8; 8],
        now: MonotonicInstant,
    ) -> Result<Self, CoreError> {
        if config.step_budget != Self::MAX_STEP_WORK || config.max_pending_requests == 0 {
            return Err(CoreError::InvalidConfiguration);
        }
        Ok(Self {
            backend,
            config,
            now,
            admission: AdmissionCore::new(admission, issuer),
            directory: BTreeMap::new(),
            owners: BTreeMap::new(),
            frontend_members: BTreeMap::new(),
            fenced: BTreeSet::new(),
            requests: BTreeMap::new(),
            request_operations: BTreeMap::new(),
            establish_reply_owners: BTreeMap::new(),
            request_deadlines: BTreeSet::new(),
            ready: BTreeMap::new(),
            fencing: None,
            reply_participations: BTreeSet::new(),
            pending_fences: BTreeSet::new(),
            internal_cursor: 0,
            request_cursor: None,
            participation_cursor: None,
            process_draining: false,
            process_ingress_closed: false,
            process_drain_cursor: None,
            process_drain_scanned: true,
            process_drain_ticket_next: true,
            maintenance_drain_next: true,
        })
    }
    pub fn retention_inventory(&self) -> ControlRetentionInventory {
        use std::mem::size_of;
        // Self includes the AdmissionCore header; exclude it because admission
        // publishes that same header in its own non-overlapping estimate.
        let metadata_bytes_estimate = size_of::<Self>()
            .checked_sub(size_of::<AdmissionCore>())
            .and_then(|n| {
                n.checked_add(
                    self.directory
                        .len()
                        .checked_mul(size_of::<(ParticipationRef, DirectoryRecord)>())?,
                )
            })
            .and_then(|n| {
                n.checked_add(
                    self.owners
                        .len()
                        .checked_mul(size_of::<(QueryExecutionId, FrontendProcessId)>())?,
                )
            })
            .and_then(|n| {
                n.checked_add(
                    self.frontend_members
                        .len()
                        .checked_mul(size_of::<(FrontendProcessId, BTreeSet<ParticipationRef>)>())?,
                )
            })
            .and_then(|n| {
                n.checked_add(
                    self.fenced
                        .len()
                        .checked_mul(size_of::<FrontendProcessId>())?,
                )
            });
        ControlRetentionInventory {
            directory_records: self.directory.len(),
            frontend_fences: self.fenced.len(),
            metadata_bytes_estimate,
        }
    }
    pub fn admission(&self) -> &AdmissionCore {
        &self.admission
    }
    pub fn route(&self, participation: ParticipationRef) -> ParticipationState {
        self.directory
            .get(&participation)
            .map_or(ParticipationState::Absent, |r| r.state)
    }
    pub fn next_deadline(&self) -> Option<MonotonicInstant> {
        self.admission
            .next_deadline()
            .into_iter()
            .chain(self.request_deadlines.first().map(|(at, _)| *at))
            .min()
    }
    /// Whether an internal step can act now. Future deadlines do not create a
    /// wake. This observation neither advances fairness nor transfers authority.
    pub fn has_internal_work(&self) -> bool {
        !self.ready.is_empty()
            || !self.reply_participations.is_empty()
            || self.fencing.is_some()
            || !self.pending_fences.is_empty()
            || self.has_process_drain_work()
            || self.next_deadline().is_some_and(|at| at <= self.now)
    }
    fn has_process_drain_work(&self) -> bool {
        self.process_draining && (!self.process_drain_scanned || self.admission.has_unredeemed())
    }
    fn advance_process_drain(&mut self, step: &mut ControlStep) -> Result<(), CoreError> {
        use std::ops::Bound::{Excluded, Unbounded};
        if !self.process_draining {
            return Err(CoreError::IllegalFact);
        }
        // Alternate the finite Participation pass with one original unused ticket.
        // The frozen election gate means no new Participation can appear behind it.
        if self.admission.has_unredeemed()
            && (self.process_drain_ticket_next || self.process_drain_scanned)
        {
            assert!(self.admission.revoke_next_unredeemed(self.now));
            self.process_drain_ticket_next = false;
            step.records_visited += 1;
            return Ok(());
        }
        if self.process_drain_scanned {
            return Ok(());
        }
        let next = match self.process_drain_cursor {
            Some(cursor) => self.directory.range((Excluded(cursor), Unbounded)).next(),
            None => self.directory.first_key_value(),
        }
        .map(|(&participation, _)| participation);
        let Some(participation) = next else {
            self.process_drain_scanned = true;
            return Ok(());
        };
        self.process_drain_cursor = Some(participation);
        self.process_drain_ticket_next = true;
        step.records_visited += 1;
        let record = self
            .directory
            .get_mut(&participation)
            .expect("original drain Participation");
        if record.state == ParticipationState::TerminalRetained {
            if self.process_ingress_closed && record.execution_retired {
                self.request_host_removal(participation, step)?;
            }
        } else if !matches!(
            record.state,
            ParticipationState::Aborting | ParticipationState::Releasing
        ) {
            record.state = ParticipationState::Aborting;
            record.generation = record
                .generation
                .next()
                .expect("process close generation exhausted");
            step.effects.push(ControlEffect::Route {
                participation,
                state: record.state,
                generation: record.generation,
            });
            step.effects.push(ControlEffect::AbortParticipation {
                participation,
                generation: record.generation,
            });
        }
        Ok(())
    }

    fn next_internal(&self) -> Option<(InternalClass, ControlInput, usize)> {
        use std::ops::Bound::{Excluded, Unbounded};
        for offset in 0..InternalClass::ALL.len() {
            let class = InternalClass::ALL[(self.internal_cursor + offset) % 4];
            let input = match class {
                InternalClass::RequestReplies => {
                    let next = self
                        .request_cursor
                        .and_then(|cursor| self.ready.range((Excluded(cursor), Unbounded)).next())
                        .or_else(|| self.ready.first_key_value());
                    next.map(|(&operation, _)| ControlInput::ContinueRequestReplies { operation })
                }
                InternalClass::ParticipationReplies => {
                    let next = self
                        .participation_cursor
                        .and_then(|cursor| {
                            self.reply_participations
                                .range((Excluded(cursor), Unbounded))
                                .next()
                        })
                        .or_else(|| self.reply_participations.first());
                    next.map(|&participation| ControlInput::ContinueReplies { participation })
                }
                InternalClass::Fence => (self.fencing.is_some() || !self.pending_fences.is_empty())
                    .then_some(ControlInput::ContinueFence),
                InternalClass::Maintenance => {
                    let due = self.next_deadline().is_some_and(|at| at <= self.now);
                    if self.has_process_drain_work() && (self.maintenance_drain_next || !due) {
                        Some(ControlInput::ContinueProcessDrain)
                    } else {
                        due.then_some(ControlInput::Tick)
                    }
                }
            };
            if let Some(input) = input {
                return Some((class, input, offset + 1));
            }
        }
        None
    }
    fn finish_routed_establish(
        &mut self,
        now: MonotonicInstant,
        participation: ParticipationRef,
        request: RequestReplyTag,
        result: Result<OperationOutcome, DomainRefusal>,
        acknowledgement: Option<ParticipationAcknowledgement>,
        decided_at: MonotonicInstant,
        step: &mut ControlStep,
    ) -> Result<(), CoreError> {
        if let Some(ack) = &acknowledgement
            && (result != Ok(ack.outcome)
                || ack.receipt.participation() != participation
                || ack.receipt.lease().is_none())
        {
            return Err(CoreError::IllegalFact);
        }
        if decided_at > now || result == Ok(OperationOutcome::Accepted) {
            return Err(CoreError::IllegalFact);
        }
        if let Some(original) = self.requests.get(&request.token) {
            let Some((target, issued_at)) = original.route else {
                return Err(CoreError::WrongIdentity);
            };
            if target != participation || original.request != request {
                return Err(CoreError::WrongIdentity);
            }
            if decided_at < issued_at {
                return Err(CoreError::IllegalFact);
            }
            let original = self.remove_request(request.token);
            let body = if decided_at.has_reached(original.deadline) {
                ControlReplyBody::Verdict(RequestResult::TimeoutWhileAccepted)
            } else {
                match result {
                    Ok(outcome) => acknowledgement.map_or_else(
                        || ControlReplyBody::Verdict(RequestResult::Outcome(outcome)),
                        ControlReplyBody::Participation,
                    ),
                    Err(refusal) => ControlReplyBody::Domain(refusal),
                }
            };
            step.requests
                .push(ControlRequestSettlement::new(original, body));
        }
        // A real late completion still retires its original physical
        // route, but cannot reopen an endpoint or settle a newer R.
        Ok(())
    }
    pub fn handle_request(
        &mut self,
        now: MonotonicInstant,
        request: RequestReplyTag,
        input: ControlInput,
        capacity: &mut impl AdmissionPort,
    ) -> Result<ControlStep, CoreError> {
        if now < self.now {
            return Err(CoreError::TimeRegressed);
        }
        let operation = match &input {
            ControlInput::Acquire { request, .. } => request.envelope().operation_id(),
            ControlInput::CreateWithoutParticipationAddress { .. } => request.operation,
            ControlInput::Establish { request, .. }
            | ControlInput::IngressOwnerFailed { request } => *request,
            _ => return Err(CoreError::WrongIdentity),
        };
        if operation != request.operation || self.requests.contains_key(&request.token) {
            return Err(CoreError::WrongIdentity);
        }
        self.now = now;
        if let ControlInput::CreateWithoutParticipationAddress {
            participation,
            task,
        } = &input
        {
            // A first Create can overtake the physical Establish handoff.
            // Numeric absence grants neither Task ownership nor a permanent
            // failure. The Root's original identity/fence facts still win.
            let record = self.directory.get(participation);
            let outcome = if participation.backend_process_id() != self.backend {
                OperationOutcome::IdentityMismatch
            } else if self.fenced.contains(&participation.frontend_process_id()) {
                OperationOutcome::Gone
            } else if self
                .owners
                .get(&participation.query_execution_id())
                .is_some_and(|owner| *owner != participation.frontend_process_id())
                || task.verify_participation(*participation).is_err()
            {
                OperationOutcome::IdentityMismatch
            } else if record.is_some_and(|record| record.participation_expired) {
                OperationOutcome::Gone
            } else if record
                .and_then(|record| record.reply.as_ref())
                .is_some_and(|(body, _)| {
                    body.result() == RequestResult::Outcome(OperationOutcome::OwnerFailed)
                })
            {
                OperationOutcome::OwnerFailed
            } else {
                match record.map(|record| record.state) {
                    Some(ParticipationState::Gone) => OperationOutcome::Gone,
                    Some(
                        ParticipationState::Quiescing
                        | ParticipationState::Releasing
                        | ParticipationState::Aborting
                        | ParticipationState::TerminalRetained,
                    ) => OperationOutcome::ParticipationTerminalReceipt,
                    None
                    | Some(
                        ParticipationState::Absent
                        | ParticipationState::Establishing
                        | ParticipationState::Active,
                    ) => {
                        if now.has_reached(request.deadline) {
                            OperationOutcome::OperationTimedOut
                        } else {
                            OperationOutcome::NotReady
                        }
                    }
                }
            };
            return Ok(ControlStep {
                records_visited: 1,
                requests: vec![ControlRequestSettlement::new(
                    request,
                    ControlReplyBody::CreateParticipation(Arc::new(
                        ParticipationReplyBody::Verdict(RequestResult::Outcome(outcome)),
                    )),
                )],
                ..ControlStep::default()
            });
        }
        if matches!(input, ControlInput::IngressOwnerFailed { .. }) {
            // No business command was submitted for this token. Its failure
            // cannot join, flush or replace another token's operation latch.
            return Ok(ControlStep {
                records_visited: 1,
                requests: vec![ControlRequestSettlement::verdict(
                    request,
                    RequestResult::Outcome(OperationOutcome::OwnerFailed),
                )],
                ..ControlStep::default()
            });
        }
        if let ControlInput::Acquire {
            request: candidate, ..
        } = &input
            && candidate.participation().backend_process_id() != self.backend
        {
            return Ok(ControlStep {
                records_visited: 1,
                requests: vec![ControlRequestSettlement::verdict(
                    request,
                    RequestResult::Outcome(OperationOutcome::IdentityMismatch),
                )],
                ..ControlStep::default()
            });
        }
        if let ControlInput::Establish {
            participation,
            ticket,
            payload,
            semantics,
            ..
        } = &input
        {
            let refusal = if participation.backend_process_id() != self.backend {
                Some(OperationOutcome::IdentityMismatch)
            } else if self.fenced.contains(&participation.frontend_process_id()) {
                Some(OperationOutcome::Gone)
            } else if self
                .owners
                .get(&participation.query_execution_id())
                .is_some_and(|owner| *owner != participation.frontend_process_id())
            {
                Some(OperationOutcome::IdentityMismatch)
            } else if self
                .directory
                .get(participation)
                .is_some_and(|record| record.participation_expired)
            {
                Some(OperationOutcome::Gone)
            } else {
                None
            };
            if let Some(outcome) = refusal {
                return Ok(ControlStep {
                    records_visited: 1,
                    requests: vec![ControlRequestSettlement::verdict(
                        request,
                        RequestResult::Outcome(outcome),
                    )],
                    ..ControlStep::default()
                });
            }
            if self.directory.contains_key(participation) {
                // No Root operation cache proves the current credential or
                // closing state. Each new RPC must ask the actual entity owner.
                if !now.has_reached(request.deadline)
                    && self.requests.len() >= self.config.max_pending_requests
                {
                    return Err(CoreError::IllegalFact);
                }
                let refusal = now
                    .has_reached(request.deadline)
                    .then_some(RequestResult::TimeoutWhileAccepted);
                if let Some(result) = refusal {
                    return Ok(ControlStep {
                        records_visited: 1,
                        requests: vec![ControlRequestSettlement::verdict(request, result)],
                        ..ControlStep::default()
                    });
                }
                self.requests.insert(
                    request.token,
                    PendingControlRequest {
                        request,
                        route: Some((*participation, now)),
                    },
                );
                self.request_deadlines
                    .insert((request.deadline, request.token));
                return Ok(ControlStep {
                    records_visited: 1,
                    effects: vec![ControlEffect::RouteEstablish {
                        participation: *participation,
                        request: EstablishReplayRequest {
                            request,
                            ticket: *ticket,
                            payload: *payload,
                            semantics: *semantics,
                        },
                    }],
                    ..ControlStep::default()
                });
            }
        }
        // An existing operation owns its original body, not merely its numeric
        // ID. Check before both the join and owned-deadline shortcuts; a bad
        // new waiter must never consume or poison the original operation.
        if let Some(outcome) = self
            .request_operations
            .get(&operation)
            .and_then(|active| active.shape.conflict(&input))
        {
            return Ok(ControlStep {
                effects: Vec::new(),
                records_visited: 1,
                requests: vec![ControlRequestSettlement::verdict(
                    request,
                    RequestResult::Outcome(outcome),
                )],
            });
        }
        if now.has_reached(request.deadline) {
            let owned = self.request_operations.contains_key(&operation)
                || match &input {
                    ControlInput::Acquire { .. } => self.admission.has_decision(operation),
                    _ => false,
                };
            if owned {
                if self.request_operations.contains_key(&operation) {
                    // An expired probe observes this original operation. It
                    // neither replays the current entity state nor consumes
                    // another token's already-owned reply position.
                    let reply = self.active_request_body(operation).map_or_else(
                        || {
                            ControlRequestSettlement::verdict(
                                request,
                                RequestResult::TimeoutWhileAccepted,
                            )
                        },
                        |body| ControlRequestSettlement::new(request, body),
                    );
                    return Ok(ControlStep {
                        records_visited: 1,
                        requests: vec![reply],
                        ..ControlStep::default()
                    });
                }
                let can_replay = match &input {
                    ControlInput::Acquire { .. } => self.admission.has_decision(operation),
                    _ => false,
                };
                if can_replay {
                    let mut step = self.handle(now, input, capacity)?;
                    let body = step
                        .effects
                        .iter()
                        .filter_map(ControlReplyBody::from_effect)
                        .find_map(|(id, body)| (id == operation).then_some(body))
                        .ok_or(CoreError::IllegalFact)?;
                    step.requests
                        .push(ControlRequestSettlement::new(request, body));
                    return Ok(step);
                }
                return Ok(ControlStep {
                    effects: Vec::new(),
                    records_visited: 1,
                    requests: vec![ControlRequestSettlement::verdict(
                        request,
                        RequestResult::TimeoutWhileAccepted,
                    )],
                });
            }
        }
        let immediate = if now.has_reached(request.deadline) {
            Some(OperationOutcome::OperationTimedOut)
        } else {
            None
        };
        if let Some(outcome) = immediate {
            return Ok(ControlStep {
                effects: Vec::new(),
                records_visited: 1,
                requests: vec![ControlRequestSettlement::verdict(
                    request,
                    RequestResult::Outcome(outcome),
                )],
            });
        }
        if self.requests.len() >= self.config.max_pending_requests {
            return Err(CoreError::IllegalFact);
        }
        let joined = self.request_operations.contains_key(&operation);
        let shape = match &input {
            ControlInput::Acquire { request, .. } => {
                ActiveControlRequest::Acquire(AcquisitionIdentity::from(*request))
            }
            ControlInput::Establish {
                participation,
                ticket,
                semantics,
                ..
            } => ActiveControlRequest::Establish {
                participation: *participation,
                ticket: *ticket,
                semantics: *semantics,
            },
            _ => unreachable!("validated Control request kind"),
        };
        if !joined && let ControlInput::Establish { participation, .. } = &input {
            self.establish_reply_owners
                .insert(operation, *participation);
        }
        self.requests.insert(
            request.token,
            PendingControlRequest {
                request,
                route: None,
            },
        );
        self.request_operations
            .entry(operation)
            .or_insert_with(|| ActiveOperation {
                shape,
                tokens: BTreeSet::new(),
            })
            .tokens
            .insert(request.token);
        self.request_deadlines
            .insert((request.deadline, request.token));
        if joined {
            let mut step = ControlStep::default();
            self.flush_request(operation, &mut step);
            return Ok(step);
        }
        self.handle(now, input, capacity)
    }
    pub fn handle(
        &mut self,
        now: MonotonicInstant,
        input: ControlInput,
        capacity: &mut impl AdmissionPort,
    ) -> Result<ControlStep, CoreError> {
        if now < self.now {
            return Err(CoreError::TimeRegressed);
        }
        self.now = now;
        if matches!(input, ControlInput::NextInternal) {
            let Some((class, input, probes)) = self.next_internal() else {
                return Ok(ControlStep {
                    records_visited: InternalClass::ALL.len(),
                    ..ControlStep::default()
                });
            };
            self.internal_cursor = (class.index() + 1) % InternalClass::ALL.len();
            match &input {
                ControlInput::ContinueRequestReplies { operation } => {
                    self.request_cursor = Some(*operation)
                }
                ControlInput::ContinueReplies { participation } => {
                    self.participation_cursor = Some(*participation)
                }
                ControlInput::ContinueProcessDrain => self.maintenance_drain_next = false,
                ControlInput::Tick => self.maintenance_drain_next = true,
                _ => {}
            }
            let mut step = self.handle(now, input, capacity)?;
            step.records_visited += probes;
            return Ok(step);
        }
        let mut step = ControlStep {
            records_visited: match &input {
                ControlInput::Tick => 2,
                ControlInput::HostRemoved { .. } => 2,
                ControlInput::ExecutionRetired { .. }
                | ControlInput::ParticipationExpired { .. } => 2,
                ControlInput::EstablishmentDecided { decision, .. }
                    if decision.state == ParticipationState::TerminalRetained =>
                {
                    2
                }
                _ => 1,
            },
            ..ControlStep::default()
        };
        match input {
            ControlInput::IngressOwnerFailed { .. }
            | ControlInput::CreateWithoutParticipationAddress { .. } => {
                return Err(CoreError::WrongIdentity);
            }
            ControlInput::RoutedEstablishOwnerFailed {
                participation,
                request,
            } => {
                self.finish_routed_establish(
                    now,
                    participation,
                    request,
                    Ok(OperationOutcome::OwnerFailed),
                    None,
                    now,
                    &mut step,
                )?;
            }
            ControlInput::RoutedEstablishFinished {
                participation,
                request,
                result,
                acknowledgement,
                decided_at,
            } => {
                self.finish_routed_establish(
                    now,
                    participation,
                    request,
                    result,
                    acknowledgement,
                    decided_at,
                    &mut step,
                )?;
            }
            ControlInput::ContinueRequestReplies { operation } => {
                self.flush_request(operation, &mut step)
            }
            ControlInput::Acquire { request, ticket } => {
                let operation = request.envelope().operation_id();
                let result = if request.participation().backend_process_id() != self.backend {
                    Err(AdmissionRefusal::Verdict(
                        OperationOutcome::IdentityMismatch,
                    ))
                } else if self
                    .fenced
                    .contains(&request.participation().frontend_process_id())
                {
                    Err(AdmissionRefusal::Verdict(OperationOutcome::Gone))
                } else {
                    self.admission
                        .acquire(request, ticket, now)
                        .map(|grant| grant.receipt())
                        .map_err(|error| {
                            use crate::admission::AdmissionTicketAcquisitionRejection as R;
                            match error {
                                R::ParticipationAlreadyGranted => AdmissionRefusal::Verdict(
                                    OperationOutcome::AdmissionTicketStillActive,
                                ),
                                R::OperationReplayConflict => {
                                    AdmissionRefusal::Verdict(OperationOutcome::DomainConflict)
                                }
                                R::ValidityExceedsWorkerLimit => AdmissionRefusal::Verdict(
                                    OperationOutcome::InvalidStateOrRequest,
                                ),
                                R::Inactive(_) | R::SealedEpoch => {
                                    AdmissionRefusal::Verdict(OperationOutcome::Gone)
                                }
                            }
                        })
                };
                match result {
                    Ok(receipt) => step.effects.push(ControlEffect::AdmissionReply {
                        request: operation,
                        result: Ok(receipt),
                    }),
                    Err(AdmissionRefusal::Verdict(outcome)) => {
                        step.effects.push(ControlEffect::AdmissionReply {
                            request: operation,
                            result: Err(outcome),
                        })
                    }
                    Err(AdmissionRefusal::Domain(outcome, failure)) => {
                        step.effects.push(ControlEffect::DomainFailureReply {
                            request: operation,
                            refusal: DomainRefusal { outcome, failure },
                        })
                    }
                }
            }
            ControlInput::Establish {
                request,
                participation,
                ticket,
                payload,
                semantics,
                generation,
            } => {
                if participation.backend_process_id() != self.backend {
                    step.effects.push(ControlEffect::Reply {
                        request,
                        outcome: OperationOutcome::IdentityMismatch,
                    });
                } else if self.fenced.contains(&participation.frontend_process_id()) {
                    step.effects.push(ControlEffect::Reply {
                        request,
                        outcome: OperationOutcome::Gone,
                    });
                } else if self
                    .owners
                    .get(&participation.query_execution_id())
                    .is_some_and(|owner| *owner != participation.frontend_process_id())
                {
                    step.effects.push(ControlEffect::Reply {
                        request,
                        outcome: OperationOutcome::IdentityMismatch,
                    });
                } else if self
                    .directory
                    .get(&participation)
                    .is_some_and(|record| record.participation_expired)
                {
                    step.effects.push(ControlEffect::Reply {
                        request,
                        outcome: OperationOutcome::Gone,
                    });
                } else if self.directory.contains_key(&participation) {
                    // An existing entity requires an exact RPC route. Bare
                    // administrative input cannot adjudicate its business state.
                    return Err(CoreError::WrongIdentity);
                } else if self.process_draining {
                    step.effects.push(ControlEffect::Reply {
                        request,
                        outcome: OperationOutcome::Gone,
                    });
                } else {
                    let redeemed = self.admission.redeem(ticket, participation, now);
                    match redeemed {
                        Err(_) => step.effects.push(ControlEffect::Reply {
                            request,
                            outcome: OperationOutcome::Gone,
                        }),
                        Ok(
                            AdmissionTicketRedemption::Redeemed(_)
                            | AdmissionTicketRedemption::Replayed(_),
                        ) => {
                            // The original materialization work is separate from
                            // the ticket's capacity-free protocol metadata.
                            match capacity.try_reserve(AdmissionNeed::Participation {
                                participation,
                                generation,
                            }) {
                                Err(AdmissionRefusal::Verdict(outcome)) => {
                                    step.effects.push(ControlEffect::Reply { request, outcome })
                                }
                                Err(AdmissionRefusal::Domain(outcome, failure)) => {
                                    step.effects.push(ControlEffect::DomainFailureReply {
                                        request,
                                        refusal: DomainRefusal { outcome, failure },
                                    })
                                }
                                Ok(reservation) => {
                                    self.directory.insert(
                                        participation,
                                        DirectoryRecord {
                                            state: ParticipationState::Establishing,
                                            generation,
                                            waiters: [request].into_iter().collect(),
                                            reservation: Some(reservation),
                                            reply: None,
                                            removal_pending: false,
                                            removal_requested: false,
                                            retirement: None,
                                            execution_retired: false,
                                            participation_expired: false,
                                        },
                                    );
                                    self.owners.insert(
                                        participation.query_execution_id(),
                                        participation.frontend_process_id(),
                                    );
                                    self.frontend_members
                                        .entry(participation.frontend_process_id())
                                        .or_default()
                                        .insert(participation);
                                    step.effects.push(ControlEffect::Materialize {
                                        participation,
                                        establishment: ParticipationEstablishment {
                                            operation: request,
                                            ticket,
                                            payload,
                                            semantics,
                                            elected_at: now,
                                        },
                                        reservation,
                                        generation,
                                    });
                                }
                            }
                        }
                    }
                }
            }
            ControlInput::EstablishmentDecided {
                participation,
                decision,
            } => {
                if !self.directory.contains_key(&participation) {
                    return Err(CoreError::WrongIdentity);
                }
                if matches!(
                    decision.state,
                    ParticipationState::Absent
                        | ParticipationState::Establishing
                        | ParticipationState::Gone
                ) || (decision.state == ParticipationState::Active)
                    != (decision.outcome == Ok(OperationOutcome::Accepted))
                    || decision.outcome == Ok(OperationOutcome::Idempotent)
                {
                    return Err(CoreError::IllegalFact);
                }
                self.establishment_decided(participation, decision, &mut step)?;
            }
            ControlInput::ContinueReplies { participation } => {
                self.replies(participation, &mut step)?
            }
            ControlInput::ExecutionRetired {
                participation,
                retirement,
            } => {
                self.note_retirement_identity(participation, retirement)?;
                self.retain_participation(participation, &mut step)?;
                let record = self
                    .directory
                    .get_mut(&participation)
                    .expect("original Participation");
                record.execution_retired = true;
                if let Some(reservation) = record.reservation.take() {
                    step.effects
                        .push(ControlEffect::RetireReservation(reservation));
                }
                if record.participation_expired
                    || (self.process_draining && self.process_ingress_closed)
                {
                    self.request_host_removal(participation, &mut step)?;
                }
            }
            ControlInput::ParticipationExpired {
                participation,
                retirement,
            } => {
                self.note_retirement_identity(participation, retirement)?;
                let record = self
                    .directory
                    .get_mut(&participation)
                    .expect("original Participation");
                record.participation_expired = true;
                if record.execution_retired {
                    self.request_host_removal(participation, &mut step)?;
                }
            }
            ControlInput::HostRemoved { participation } => {
                let record = self
                    .directory
                    .get(&participation)
                    .ok_or(CoreError::WrongIdentity)?;
                if record.state != ParticipationState::TerminalRetained
                    || !record.waiters.is_empty()
                    || !record.removal_requested
                    || !record.execution_retired
                    || record.reservation.is_some()
                {
                    return Err(CoreError::IllegalFact);
                }
                let record = self.directory.remove(&participation).unwrap();
                self.reply_participations.remove(&participation);
                self.owners.remove(&participation.query_execution_id());
                let frontend = participation.frontend_process_id();
                if let Some(members) = self.frontend_members.get_mut(&frontend) {
                    members.remove(&participation);
                    if members.is_empty() {
                        self.frontend_members.remove(&frontend);
                    }
                }
                // The live route reader must observe the same Gone decision
                // before the original directory payer leaves the host. Only
                // this core authors the state; the driver does not infer it
                // from a missing host address or a retired reservation.
                step.effects.push(ControlEffect::Route {
                    participation,
                    state: ParticipationState::Gone,
                    generation: record.generation,
                });
            }
            ControlInput::RevokeTicket { participation } => {
                self.admission.revoke_unredeemed(participation, now);
            }
            ControlInput::FenceFrontend { frontend } => {
                // The permanent fence is also the deduplication authority.
                // A duplicate cannot rewind an in-progress member cursor.
                if !self.fenced.contains(&frontend) {
                    self.fenced.insert(frontend);
                    self.pending_fences.insert(frontend);
                    step.effects.push(ControlEffect::ContinueFence);
                }
            }
            ControlInput::ContinueFence => self.fence(&mut step),
            ControlInput::ProcessDrain => {
                if !self.process_draining {
                    self.process_draining = true;
                    self.process_drain_cursor = None;
                    self.process_drain_scanned = false;
                    self.admission.begin_process_drain();
                }
            }
            ControlInput::ProcessIngressClosed => {
                if !self.process_draining {
                    return Err(CoreError::IllegalFact);
                }
                if !self.process_ingress_closed {
                    self.process_ingress_closed = true;
                    self.process_drain_cursor = None;
                    self.process_drain_scanned = false;
                }
            }
            ControlInput::ContinueProcessDrain => self.advance_process_drain(&mut step)?,
            ControlInput::NextInternal => unreachable!("selected before ordinary dispatch"),
            ControlInput::Tick => {
                self.expire_request(&mut step);
                step.records_visited += self.admission.advance(now, 1);
                if self.next_deadline().is_some_and(|at| at <= now) {
                    step.effects.push(ControlEffect::ContinueMaintenance);
                }
            }
        }
        let ready: Vec<_> = step
            .effects
            .iter()
            .filter_map(ControlReplyBody::from_effect)
            .collect();
        for (operation, body) in ready {
            if self.request_operations.contains_key(&operation) {
                let decided_at = self.request_ready_at(operation).unwrap_or(now);
                let body = self.active_request_body(operation).unwrap_or(body);
                self.ready.entry(operation).or_insert((body, decided_at));
                self.flush_request(operation, &mut step);
            }
        }
        Ok(step)
    }
    /// Read-only preflight only where actual ticket expiry will be the next
    /// Root decision. Existing entity, retention, owner and fence priorities
    /// remain authoritative; absence alone proves no terminal verdict.
    pub fn preflight_expired_establish(
        &self,
        participation: ParticipationRef,
        ticket: AdmissionTicketId,
        now: MonotonicInstant,
    ) -> Option<OperationOutcome> {
        if self.directory.contains_key(&participation)
            || self.fenced.contains(&participation.frontend_process_id())
            || self
                .owners
                .get(&participation.query_execution_id())
                .is_some_and(|owner| *owner != participation.frontend_process_id())
        {
            return None;
        }
        matches!(
            self.admission.observe(ticket, participation, now),
            crate::admission::AdmissionTicketObservation::Expired
        )
        .then_some(OperationOutcome::InvalidStateOrRequest)
    }
    fn remove_request(&mut self, token: InputId) -> RequestReplyTag {
        let original = self.requests.remove(&token).expect("indexed request");
        let request = original.request;
        self.request_deadlines.remove(&(request.deadline, token));
        if original.route.is_some() {
            return request;
        }
        let active = self
            .request_operations
            .get_mut(&request.operation)
            .expect("request operation");
        active.tokens.remove(&token);
        if active.tokens.is_empty() {
            self.request_operations.remove(&request.operation);
            self.ready.remove(&request.operation);
            if let Some(participation) = self.establish_reply_owners.remove(&request.operation) {
                if let Some(record) = self.directory.get_mut(&participation) {
                    // A dead reply position does not revoke the ticket or the
                    // already-owned participation materialization action.
                    record.waiters.remove(&request.operation);
                }
                self.refresh_reply_participation(participation);
            }
        }
        request
    }
    fn flush_request(&mut self, operation: TaskOperationId, step: &mut ControlStep) {
        let Some((body, at)) = self.ready.get(&operation).cloned() else {
            return;
        };
        if step.requests.is_empty() {
            let token = self.request_operations[&operation]
                .tokens
                .first()
                .copied()
                .expect("operation waiter");
            let request = self.remove_request(token);
            step.requests.push(if at.has_reached(request.deadline) {
                ControlRequestSettlement::verdict(request, RequestResult::TimeoutWhileAccepted)
            } else {
                ControlRequestSettlement::new(request, body)
            });
        }
        if self.request_operations.contains_key(&operation) {
            step.effects
                .push(ControlEffect::ContinueRequestReplies { operation });
        }
    }
    fn active_request_body(&self, operation: TaskOperationId) -> Option<ControlReplyBody> {
        self.ready
            .get(&operation)
            .map(|(body, _)| body.clone())
            .or_else(|| {
                let participation = self.establish_reply_owners.get(&operation)?;
                let record = self.directory.get(participation)?;
                let (body, _) = record.reply.as_ref()?;
                Some(body.clone())
            })
    }
    fn request_ready_at(&self, operation: TaskOperationId) -> Option<MonotonicInstant> {
        self.ready.get(&operation).map(|(_, at)| *at).or_else(|| {
            self.establish_reply_owners
                .get(&operation)
                .and_then(|participation| self.directory.get(participation))
                .and_then(|record| record.reply.as_ref().map(|(_, at)| *at))
        })
    }
    fn expire_request(&mut self, step: &mut ControlStep) {
        let Some((at, token)) = self.request_deadlines.first().copied() else {
            return;
        };
        if self.now < at {
            return;
        }
        self.request_deadlines.pop_first();
        let pending = &self.requests[&token];
        let operation = pending.request.operation;
        if let Some((participation, _)) = pending.route {
            let request = self.remove_request(token);
            step.effects.push(ControlEffect::CancelEstablishRoute {
                participation,
                request,
            });
            step.requests.push(ControlRequestSettlement::verdict(
                request,
                RequestResult::TimeoutWhileAccepted,
            ));
            return;
        }
        if self
            .request_ready_at(operation)
            .is_some_and(|ready| ready < at)
        {
            return;
        }
        let request = self.remove_request(token);
        step.requests.push(ControlRequestSettlement::verdict(
            request,
            RequestResult::TimeoutWhileAccepted,
        ));
    }
    fn replies(
        &mut self,
        participation: ParticipationRef,
        step: &mut ControlStep,
    ) -> Result<(), CoreError> {
        let r = self
            .directory
            .get_mut(&participation)
            .ok_or(CoreError::WrongIdentity)?;
        if r.reply.is_none() {
            return Err(CoreError::IllegalFact);
        }
        if let Some(request) = r.waiters.pop_first() {
            let (body, _) = r.reply.as_ref().expect("original directory decision");
            step.effects.push(match body {
                ControlReplyBody::Domain(refusal) => ControlEffect::DomainFailureReply {
                    request,
                    refusal: refusal.clone(),
                },
                _ => {
                    let RequestResult::Outcome(outcome) = body.result() else {
                        return Err(CoreError::IllegalFact);
                    };
                    ControlEffect::Reply { request, outcome }
                }
            });
        }
        if !r.waiters.is_empty() {
            step.effects
                .push(ControlEffect::ContinueReplies { participation });
        } else if r.removal_pending {
            r.removal_pending = false;
            if !r.removal_requested {
                r.removal_requested = true;
                step.effects
                    .push(ControlEffect::RemoveHost { participation });
            }
        }
        self.refresh_reply_participation(participation);
        Ok(())
    }
    fn request_host_removal(
        &mut self,
        participation: ParticipationRef,
        step: &mut ControlStep,
    ) -> Result<(), CoreError> {
        step.records_visited += 1;
        let record = self
            .directory
            .get_mut(&participation)
            .ok_or(CoreError::WrongIdentity)?;
        if record.removal_requested {
            return Ok(());
        }
        if record.reply.is_some() && record.waiters.is_empty() {
            record.removal_requested = true;
            step.effects
                .push(ControlEffect::RemoveHost { participation });
        } else {
            record.removal_pending = true;
            self.refresh_reply_participation(participation);
        }
        Ok(())
    }
    fn note_retirement_identity(
        &mut self,
        participation: ParticipationRef,
        retirement: ParticipationRetirementIdentity,
    ) -> Result<(), CoreError> {
        if !retirement.is_valid() {
            return Err(CoreError::IllegalFact);
        }
        let record = self
            .directory
            .get_mut(&participation)
            .ok_or(CoreError::WrongIdentity)?;
        if record
            .retirement
            .is_some_and(|original| original != retirement)
        {
            return Err(CoreError::IllegalFact);
        }
        record.retirement = Some(retirement);
        Ok(())
    }
    fn retain_participation(
        &mut self,
        participation: ParticipationRef,
        step: &mut ControlStep,
    ) -> Result<(), CoreError> {
        self.admission.close_participation(participation, self.now);
        let record = self
            .directory
            .get_mut(&participation)
            .ok_or(CoreError::WrongIdentity)?;
        if record.state != ParticipationState::TerminalRetained {
            record.state = ParticipationState::TerminalRetained;
            step.effects.push(ControlEffect::Route {
                participation,
                state: record.state,
                generation: record.generation,
            });
        }
        Ok(())
    }
    fn establishment_decided(
        &mut self,
        participation: ParticipationRef,
        decision: ParticipationEstablishmentDecision,
        step: &mut ControlStep,
    ) -> Result<(), CoreError> {
        if self
            .directory
            .get(&participation)
            .ok_or(CoreError::WrongIdentity)?
            .reply
            .is_some()
        {
            // A duplicate neutral report cannot replace the original decision
            // or its ready time. The real bridge still owns one-shot authorship.
            return self.replies(participation, step);
        }
        if decision.state == ParticipationState::TerminalRetained {
            self.retain_participation(participation, step)?;
        }
        let record = self
            .directory
            .get_mut(&participation)
            .expect("known participation");
        let ParticipationEstablishmentDecision {
            state,
            mut outcome,
            mut acknowledgement,
            mut decided_at,
        } = decision;
        if decided_at > self.now {
            return Err(CoreError::IllegalFact);
        }
        if let Some(ack) = &acknowledgement {
            if outcome != Ok(ack.outcome)
                || ack.receipt.participation() != participation
                || ack.receipt.state() != state
                || ack.receipt.lease().is_none()
            {
                return Err(CoreError::IllegalFact);
            }
        } else if outcome == Ok(OperationOutcome::Accepted) {
            return Err(CoreError::IllegalFact);
        }
        // Root fencing can outrun the Participation's success return. It blocks only
        // reopening; it cannot erase a typed failure or a lease verdict.
        let fenced_active = state == ParticipationState::Active
            && self.fenced.contains(&participation.frontend_process_id());
        if state == ParticipationState::Active
            && (record.state != ParticipationState::Establishing || fenced_active)
        {
            outcome = Ok(OperationOutcome::ParticipationTerminalReceipt);
            // This is the Root's later fence refusal, not the Participation's old
            // successful receipt. Never rewrite that receipt's state/lease.
            acknowledgement = None;
            decided_at = self.now;
        }
        record.state = match (record.state, state) {
            (ParticipationState::TerminalRetained, _)
            | (_, ParticipationState::TerminalRetained) => ParticipationState::TerminalRetained,
            (ParticipationState::Releasing, _) => ParticipationState::Releasing,
            (ParticipationState::Aborting, _) => ParticipationState::Aborting,
            _ if fenced_active => ParticipationState::Aborting,
            _ => state,
        };
        let body = match outcome {
            Ok(outcome) => acknowledgement.map_or_else(
                || ControlReplyBody::Verdict(RequestResult::Outcome(outcome)),
                ControlReplyBody::Participation,
            ),
            Err(refusal) => ControlReplyBody::Domain(refusal),
        };
        record.reply = Some((body, decided_at));
        if record.state != ParticipationState::TerminalRetained {
            step.effects.push(ControlEffect::Route {
                participation,
                state: record.state,
                generation: record.generation,
            });
        }
        self.replies(participation, step)
    }
    fn refresh_reply_participation(&mut self, participation: ParticipationRef) {
        if self.directory.get(&participation).is_some_and(|record| {
            record.reply.is_some() && (!record.waiters.is_empty() || record.removal_pending)
        }) {
            self.reply_participations.insert(participation);
        } else {
            self.reply_participations.remove(&participation);
        }
    }
    fn fence(&mut self, step: &mut ControlStep) {
        use std::ops::Bound::{Excluded, Unbounded};
        if self.fencing.is_none() {
            let Some(frontend) = self.pending_fences.pop_first() else {
                return;
            };
            step.records_visited += 1;
            self.fencing = Some((frontend, None));
        }
        let (frontend, mut cursor) = self.fencing.expect("selected frontend fence");
        if let Some(members) = self.frontend_members.get(&frontend) {
            let keys: Vec<_> = match cursor {
                Some(cursor) => members.range((Excluded(cursor), Unbounded)),
                None => members.range(..),
            }
            .take(1)
            .copied()
            .collect();
            for participation in keys {
                step.records_visited += 1;
                cursor = Some(participation);
                if let Some(record) = self.directory.get_mut(&participation)
                    && record.state != ParticipationState::TerminalRetained
                {
                    record.state = ParticipationState::Aborting;
                    record.generation = record
                        .generation
                        .next()
                        .expect("participation fence generation exhausted");
                    step.effects.push(ControlEffect::Route {
                        participation,
                        state: record.state,
                        generation: record.generation,
                    });
                    step.effects.push(ControlEffect::AbortParticipation {
                        participation,
                        generation: record.generation,
                    });
                }
            }
            if cursor.is_some_and(|cursor| {
                members
                    .range((Excluded(cursor), Unbounded))
                    .next()
                    .is_some()
            }) {
                self.fencing = Some((frontend, cursor));
                step.effects.push(ControlEffect::ContinueFence);
                return;
            }
        }
        self.fencing = None;
        if !self.pending_fences.is_empty() {
            step.effects.push(ControlEffect::ContinueFence);
        }
    }
}

#[cfg(test)]
#[path = "control/frontend_members_tests.rs"]
mod frontend_members_tests;
