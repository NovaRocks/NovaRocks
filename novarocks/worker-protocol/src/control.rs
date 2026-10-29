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

//! Process control actor: low-frequency directory, ticket and retention policy.
//! Task creation/preparation capacity never traverses this mailbox.
use crate::{
    admission::{
        AcquisitionIdentity, AdmissionCore, AdmissionTicketConfig, AdmissionTicketRedemption,
    },
    context::{
        ContextAcknowledgement, ContextEstablishment, ContextEstablishmentDecision,
        ContextReplyBody, CoreError, DomainRefusal, EstablishReplayRequest, RequestResult,
        RequestSettlement,
    },
    creation::{AdmissionNeed, AdmissionPort, AdmissionRefusal, PayloadId, RequestContext},
    lease::MonotonicInstant,
    retention::RetentionIndex,
};
use novarocks_control_runtime_core::identity::{Generation, InputId, ReservationId};
use novarocks_execution_contract::{
    AcquireQueryContextAdmissionTicket, AdmissionTicketId, EstablishSemanticIdentity,
    LeaseValidFor, OperationOutcome, QueryContextAdmissionTicketReceipt, QueryContextRef,
    QueryContextState, TaskIdentity, TaskOperationId,
};
use novarocks_execution_contract::{ContentFingerprint, CredentialEpoch, CredentialLeaseId};
use novarocks_types::identity::{FrontendProcessId, QueryExecutionId};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Clone, Copy, Debug)]
pub struct ControlConfig {
    /// Total directory records, including retained and physically removing
    /// contexts. Composition pays for these independently of active tickets.
    pub max_contexts: usize,
    pub max_waiters: usize,
    pub max_pending_requests: usize,
    pub step_budget: usize,
    pub retained_contexts: usize,
    pub gone_fences: usize,
    pub retention: std::time::Duration,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EstablishSemanticFacts {
    pub catalog: ContentFingerprint,
    pub shared_filter: ContentFingerprint,
    pub query_options: ContentFingerprint,
    pub credential_lease: CredentialLeaseId,
    pub credential_epoch: CredentialEpoch,
    pub initial_lease_valid_for: LeaseValidFor,
}
impl From<EstablishSemanticIdentity> for EstablishSemanticFacts {
    fn from(identity: EstablishSemanticIdentity) -> Self {
        Self {
            catalog: identity.catalog_binding(),
            shared_filter: identity.initial_runtime_filter(),
            query_options: identity.query_options(),
            credential_lease: identity.credential_lease_id(),
            credential_epoch: identity.credential_epoch(),
            initial_lease_valid_for: identity.initial_lease_valid_for(),
        }
    }
}
#[derive(Debug)]
enum ActiveControlRequest {
    Acquire(AcquisitionIdentity),
    Establish {
        context: QueryContextRef,
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
                    context,
                    ticket,
                    semantics,
                },
                ControlInput::Establish {
                    context: candidate_context,
                    ticket: candidate_ticket,
                    semantics: candidate_semantics,
                    ..
                },
            ) => (!(*context == *candidate_context
                && *ticket == *candidate_ticket
                && *semantics == *candidate_semantics))
                .then_some(OperationOutcome::ContextConflict),
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
    request: RequestContext,
    // Existing-entity reads belong to their exact original RPC, independently
    // of another RPC with the same operation awaiting its creation result.
    route: Option<(QueryContextRef, MonotonicInstant)>,
}
#[derive(Debug)]
struct DirectoryRecord {
    state: QueryContextState,
    generation: Generation,
    waiters: BTreeSet<TaskOperationId>,
    reservation: ReservationId,
    reply: Option<(ControlReplyBody, MonotonicInstant)>,
    // Retention may end before the original bounded reply fanout. Keep that
    // obligation in this same paid row until its last operation moves to R.
    removal_pending: bool,
    removal_requested: bool,
    // The first actual retained transition fixes the horizon. A late or repeated
    // host observation never extends it; unknown is not evidence of no record.
    first_retained_at: Option<MonotonicInstant>,
    normal_close: Option<bool>,
}
#[derive(Debug)]
pub enum ControlInput {
    ContinueRequestReplies {
        operation: TaskOperationId,
    },
    Acquire {
        request: AcquireQueryContextAdmissionTicket,
        ticket: AdmissionTicketId,
    },
    Establish {
        request: TaskOperationId,
        context: QueryContextRef,
        ticket: AdmissionTicketId,
        payload: PayloadId,
        semantics: EstablishSemanticFacts,
        generation: Generation,
    },
    /// The original admitted Root R observed no actual Context address.
    /// This is readiness observation, never Task admission or a new Context.
    CreateWithoutContextAddress {
        context: QueryContextRef,
        task: TaskIdentity,
    },
    /// The actual ingress getter failed before this RPC submitted its command.
    /// Only handle_request may settle the same original admitted token.
    IngressOwnerFailed {
        request: TaskOperationId,
    },
    RoutedEstablishFinished {
        context: QueryContextRef,
        request: RequestContext,
        result: Result<OperationOutcome, DomainRefusal>,
        acknowledgement: Option<ContextAcknowledgement>,
        decided_at: MonotonicInstant,
    },
    EstablishmentDecided {
        context: QueryContextRef,
        decision: ContextEstablishmentDecision,
    },
    /// Bootstrap could not deliver a usable Context decision. Any original
    /// partial owner still requires its real host-side teardown.
    ContextBootstrapFailed {
        context: QueryContextRef,
        refusal: AdmissionRefusal,
    },
    ContinueReplies {
        context: QueryContextRef,
    },
    Retained {
        context: QueryContextRef,
        /// Observed on the original physical Release call after Task retirement.
        normal_close: bool,
    },
    HostRemoved {
        context: QueryContextRef,
    },
    RevokeTicket {
        context: QueryContextRef,
    },
    FenceFrontend {
        frontend: FrontendProcessId,
    },
    ContinueFence,
    /// Same original process owner closes new elections and drains actual
    /// Context/ticket owners; this is not a frontend process fence.
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
        result: Result<QueryContextAdmissionTicketReceipt, OperationOutcome>,
    },
    Reply {
        request: TaskOperationId,
        outcome: OperationOutcome,
    },
    Materialize {
        context: QueryContextRef,
        establishment: ContextEstablishment,
        reservation: ReservationId,
        generation: Generation,
    },
    RouteEstablish {
        context: QueryContextRef,
        request: EstablishReplayRequest,
    },
    /// Retire future read-only probe issuance after the original R deadline.
    /// Started calls and captures still need their actual exit observations.
    CancelEstablishRoute {
        context: QueryContextRef,
        request: RequestContext,
    },
    Route {
        context: QueryContextRef,
        state: QueryContextState,
        generation: Generation,
    },
    AbortContext {
        context: QueryContextRef,
        generation: Generation,
    },
    RetireReservation(ReservationId),
    RemoveHost {
        context: QueryContextRef,
    },
    ContinueReplies {
        context: QueryContextRef,
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
    CreateContext(Arc<ContextReplyBody>),
    Context(ContextAcknowledgement),
    TicketObservation(crate::admission::AdmissionTicketObservation),
    PreflightExpired(Option<OperationOutcome>),
    Admission(Result<QueryContextAdmissionTicketReceipt, OperationOutcome>),
    Domain(DomainRefusal),
    Verdict(RequestResult),
}
impl ControlReplyBody {
    pub fn matches(&self, result: RequestResult) -> bool {
        match self {
            Self::CreateContext(body) => body.matches(result),
            Self::Context(ack) => result == RequestResult::Outcome(ack.outcome),
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
            Self::CreateContext(body) => body.result(),
            Self::Context(ack) => RequestResult::Outcome(ack.outcome),
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
    fn new(request: RequestContext, body: ControlReplyBody) -> Self {
        Self {
            settlement: RequestSettlement {
                request,
                result: body.result(),
            },
            body,
        }
    }
    fn verdict(request: RequestContext, result: RequestResult) -> Self {
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
    ContextReplies,
    Fence,
    Maintenance,
}
impl InternalClass {
    const ALL: [Self; 4] = [
        Self::RequestReplies,
        Self::ContextReplies,
        Self::Fence,
        Self::Maintenance,
    ];
    fn index(self) -> usize {
        self as usize
    }
}

#[derive(Debug)]
pub struct ControlCore {
    config: ControlConfig,
    now: MonotonicInstant,
    admission: AdmissionCore,
    directory: BTreeMap<QueryContextRef, DirectoryRecord>,
    owners: BTreeMap<QueryExecutionId, FrontendProcessId>,
    // Paid directory geometry: at most C retained record/deadline pairs and
    // at most min(C, K) eligible address nodes, all referring to these same
    // C directory rows. Each row adds one first-retained instant and one
    // optional physical observation; neither index stores another host owner.
    retained: RetentionIndex<QueryContextRef>,
    gone: RetentionIndex<QueryContextRef>,
    frontend_members: BTreeMap<FrontendProcessId, BTreeSet<QueryContextRef>>,
    fenced: BTreeSet<FrontendProcessId>,
    requests: BTreeMap<InputId, PendingControlRequest>,
    request_operations: BTreeMap<TaskOperationId, ActiveOperation>,
    establish_reply_owners: BTreeMap<TaskOperationId, QueryContextRef>,
    request_deadlines: BTreeSet<(MonotonicInstant, InputId)>,
    ready: BTreeMap<TaskOperationId, (ControlReplyBody, MonotonicInstant)>,
    fencing: Option<(FrontendProcessId, Option<QueryContextRef>)>,
    // Addresses only: bodies and waiter ownership stay in directory/ready.
    reply_contexts: BTreeSet<QueryContextRef>,
    pending_fences: BTreeSet<FrontendProcessId>,
    internal_cursor: usize,
    request_cursor: Option<TaskOperationId>,
    context_cursor: Option<QueryContextRef>,
    process_draining: bool,
    process_ingress_closed: bool,
    process_drain_cursor: Option<QueryContextRef>,
    process_drain_scanned: bool,
    process_drain_ticket_next: bool,
    maintenance_drain_next: bool,
}
impl ControlCore {
    pub const MAX_STEP_WORK: usize = 15;
    /// The actual directory geometry, including retained/removing records.
    /// Reading it grants no Context admission or host ownership authority.
    pub const fn directory_capacity(&self) -> usize {
        self.config.max_contexts
    }
    pub fn max_step_work(&self) -> usize {
        Self::MAX_STEP_WORK
    }

    pub fn new(
        config: ControlConfig,
        admission: AdmissionTicketConfig,
        issuer: [u8; 8],
        now: MonotonicInstant,
    ) -> Result<Self, CoreError> {
        if config.max_contexts == 0
            || config.max_waiters == 0
            || config.step_budget != Self::MAX_STEP_WORK
            || config.max_pending_requests == 0
            || config.retained_contexts == 0
            || config.gone_fences == 0
        {
            return Err(CoreError::InvalidConfiguration);
        }
        Ok(Self {
            config,
            now,
            admission: AdmissionCore::new(admission, issuer),
            directory: BTreeMap::new(),
            owners: BTreeMap::new(),
            // Every retained row already occupies one of the original C directory
            // positions. K limits only early-evictable rows; protected rows cannot
            // consume a new directory position or enlarge admission capacity.
            retained: RetentionIndex::new(config.max_contexts, u64::MAX),
            gone: RetentionIndex::new(config.gone_fences, u64::MAX),
            frontend_members: BTreeMap::new(),
            fenced: BTreeSet::new(),
            requests: BTreeMap::new(),
            request_operations: BTreeMap::new(),
            establish_reply_owners: BTreeMap::new(),
            request_deadlines: BTreeSet::new(),
            ready: BTreeMap::new(),
            fencing: None,
            reply_contexts: BTreeSet::new(),
            pending_fences: BTreeSet::new(),
            internal_cursor: 0,
            request_cursor: None,
            context_cursor: None,
            process_draining: false,
            process_ingress_closed: false,
            process_drain_cursor: None,
            process_drain_scanned: true,
            process_drain_ticket_next: true,
            maintenance_drain_next: true,
        })
    }
    pub fn admission(&self) -> &AdmissionCore {
        &self.admission
    }
    pub fn route(&self, context: QueryContextRef) -> QueryContextState {
        self.directory.get(&context).map_or_else(
            || {
                if self.gone.contains(context) {
                    QueryContextState::Gone
                } else {
                    QueryContextState::Absent
                }
            },
            |r| r.state,
        )
    }
    pub fn next_deadline(&self) -> Option<MonotonicInstant> {
        self.admission
            .next_deadline()
            .into_iter()
            .chain(self.retained.next_deadline())
            .chain(self.gone.next_deadline())
            .chain(self.request_deadlines.first().map(|(at, _)| *at))
            .min()
    }
    /// Whether an internal step can act now. Future deadlines do not create a
    /// wake. This observation neither advances fairness nor transfers authority.
    pub fn has_internal_work(&self) -> bool {
        !self.ready.is_empty()
            || !self.reply_contexts.is_empty()
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
        // Alternate the finite Context pass with one original unused ticket.
        // The frozen election gate means no new Context can appear behind it.
        if self.admission.has_unredeemed()
            && (self.process_drain_ticket_next || self.process_drain_scanned)
        {
            let reservation = self
                .admission
                .revoke_next_unredeemed(self.now)
                .expect("actual issued ticket frontier");
            self.process_drain_ticket_next = false;
            step.records_visited += 1;
            step.effects
                .push(ControlEffect::RetireReservation(reservation));
            return Ok(());
        }
        if self.process_drain_scanned {
            return Ok(());
        }
        let next = match self.process_drain_cursor {
            Some(cursor) => self.directory.range((Excluded(cursor), Unbounded)).next(),
            None => self.directory.first_key_value(),
        }
        .map(|(&context, _)| context);
        let Some(context) = next else {
            self.process_drain_scanned = true;
            return Ok(());
        };
        self.process_drain_cursor = Some(context);
        self.process_drain_ticket_next = true;
        step.records_visited += 1;
        let record = self
            .directory
            .get_mut(&context)
            .expect("original drain Context");
        if record.state == QueryContextState::TerminalRetained {
            if self.process_ingress_closed {
                if self.retained.contains(context) {
                    self.retained
                        .remove(context)
                        .expect("same retained Context");
                }
                self.request_host_removal(context, step)?;
            }
        } else if !matches!(
            record.state,
            QueryContextState::Aborting | QueryContextState::Releasing
        ) {
            record.state = QueryContextState::Aborting;
            record.generation = record
                .generation
                .next()
                .expect("process close generation exhausted");
            step.effects.push(ControlEffect::Route {
                context,
                state: record.state,
                generation: record.generation,
            });
            step.effects.push(ControlEffect::AbortContext {
                context,
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
                InternalClass::ContextReplies => {
                    let next = self
                        .context_cursor
                        .and_then(|cursor| {
                            self.reply_contexts
                                .range((Excluded(cursor), Unbounded))
                                .next()
                        })
                        .or_else(|| self.reply_contexts.first());
                    next.map(|&context| ControlInput::ContinueReplies { context })
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
    pub fn handle_request(
        &mut self,
        now: MonotonicInstant,
        request: RequestContext,
        input: ControlInput,
        capacity: &mut impl AdmissionPort,
    ) -> Result<ControlStep, CoreError> {
        if now < self.now {
            return Err(CoreError::TimeRegressed);
        }
        let operation = match &input {
            ControlInput::Acquire { request, .. } => request.envelope().operation_id(),
            ControlInput::CreateWithoutContextAddress { .. } => request.operation,
            ControlInput::Establish { request, .. }
            | ControlInput::IngressOwnerFailed { request } => *request,
            _ => return Err(CoreError::WrongIdentity),
        };
        if operation != request.operation || self.requests.contains_key(&request.token) {
            return Err(CoreError::WrongIdentity);
        }
        self.now = now;
        if let ControlInput::CreateWithoutContextAddress { context, task } = &input {
            // A first Create can overtake the physical Establish handoff.
            // Numeric absence grants neither Task ownership nor a permanent
            // failure. The Root's original identity/fence facts still win.
            let record = self.directory.get(context);
            let outcome = if self.fenced.contains(&context.frontend_process_id())
                || self.gone.contains(*context)
            {
                OperationOutcome::Gone
            } else if self
                .owners
                .get(&context.query_execution_id())
                .is_some_and(|owner| *owner != context.frontend_process_id())
                || task.verify_query_context(*context).is_err()
            {
                OperationOutcome::IdentityMismatch
            } else if record
                .and_then(|record| record.reply.as_ref())
                .is_some_and(|(body, _)| {
                    body.result() == RequestResult::Outcome(OperationOutcome::OwnerFailed)
                })
            {
                OperationOutcome::OwnerFailed
            } else {
                match record.map(|record| record.state) {
                    Some(QueryContextState::Gone) => OperationOutcome::Gone,
                    Some(
                        QueryContextState::Quiescing
                        | QueryContextState::Releasing
                        | QueryContextState::Aborting
                        | QueryContextState::TerminalRetained,
                    ) => OperationOutcome::ContextTerminalReceipt,
                    None
                    | Some(
                        QueryContextState::Absent
                        | QueryContextState::Establishing
                        | QueryContextState::Active,
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
                    ControlReplyBody::CreateContext(Arc::new(ContextReplyBody::Verdict(
                        RequestResult::Outcome(outcome),
                    ))),
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
        if let ControlInput::Establish {
            context,
            ticket,
            payload,
            semantics,
            ..
        } = &input
        {
            let refusal = if self.fenced.contains(&context.frontend_process_id())
                || self.gone.contains(*context)
            {
                Some(OperationOutcome::Gone)
            } else if self
                .owners
                .get(&context.query_execution_id())
                .is_some_and(|owner| *owner != context.frontend_process_id())
            {
                Some(OperationOutcome::IdentityMismatch)
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
            if self.directory.contains_key(context) {
                // No Root operation cache proves the current credential or
                // closing state. Each new RPC must ask the actual entity owner.
                let refusal = if now.has_reached(request.deadline) {
                    Some(RequestResult::TimeoutWhileAccepted)
                } else if self.requests.len() >= self.config.max_pending_requests {
                    Some(RequestResult::Outcome(OperationOutcome::ResourceExhausted))
                } else {
                    None
                };
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
                        route: Some((*context, now)),
                    },
                );
                self.request_deadlines
                    .insert((request.deadline, request.token));
                return Ok(ControlStep {
                    records_visited: 1,
                    effects: vec![ControlEffect::RouteEstablish {
                        context: *context,
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
        } else if self.requests.len() >= self.config.max_pending_requests {
            Some(OperationOutcome::ResourceExhausted)
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
        let joined = self.request_operations.contains_key(&operation);
        let shape = match &input {
            ControlInput::Acquire { request, .. } => {
                ActiveControlRequest::Acquire(AcquisitionIdentity::from(*request))
            }
            ControlInput::Establish {
                context,
                ticket,
                semantics,
                ..
            } => ActiveControlRequest::Establish {
                context: *context,
                ticket: *ticket,
                semantics: *semantics,
            },
            _ => unreachable!("validated Control request kind"),
        };
        if !joined && let ControlInput::Establish { context, .. } = &input {
            self.establish_reply_owners.insert(operation, *context);
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
                ControlInput::ContinueReplies { context } => self.context_cursor = Some(*context),
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
                ControlInput::Tick => 4,
                ControlInput::HostRemoved { .. } => 3,
                ControlInput::Retained { .. } => 2,
                ControlInput::EstablishmentDecided { decision, .. }
                    if decision.state == QueryContextState::TerminalRetained =>
                {
                    2
                }
                _ => 1,
            },
            ..ControlStep::default()
        };
        match input {
            ControlInput::IngressOwnerFailed { .. }
            | ControlInput::CreateWithoutContextAddress { .. } => {
                return Err(CoreError::WrongIdentity);
            }
            ControlInput::RoutedEstablishFinished {
                context,
                request,
                result,
                acknowledgement,
                decided_at,
            } => {
                if let Some(ack) = &acknowledgement {
                    if result != Ok(ack.outcome)
                        || ack.receipt.context() != context
                        || ack.receipt.lease().is_none()
                    {
                        return Err(CoreError::IllegalFact);
                    }
                }
                if decided_at > now || result == Ok(OperationOutcome::Accepted) {
                    return Err(CoreError::IllegalFact);
                }
                if let Some(original) = self.requests.get(&request.token) {
                    let Some((target, issued_at)) = original.route else {
                        return Err(CoreError::WrongIdentity);
                    };
                    if target != context
                        || original.request.operation != request.operation
                        || original.request.deadline != request.deadline
                    {
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
                                ControlReplyBody::Context,
                            ),
                            Err(refusal) => ControlReplyBody::Domain(refusal),
                        }
                    };
                    step.requests
                        .push(ControlRequestSettlement::new(original, body));
                }
                // A real late completion still retires its original physical
                // route, but cannot reopen an endpoint or settle a newer R.
            }
            ControlInput::ContinueRequestReplies { operation } => {
                self.flush_request(operation, &mut step)
            }
            ControlInput::Acquire { request, ticket } => {
                let operation = request.envelope().operation_id();
                let result = if self
                    .fenced
                    .contains(&request.context().frontend_process_id())
                {
                    Err(AdmissionRefusal::Verdict(OperationOutcome::Gone))
                } else {
                    self.admission
                        .acquire(request, ticket, now, capacity)
                        .map(|grant| grant.receipt())
                        .map_err(|error| {
                            use crate::admission::AdmissionTicketAcquisitionRejection as R;
                            match error {
                                R::ReservationRefused(refusal) => refusal,
                                R::ContextAlreadyGranted => AdmissionRefusal::Verdict(
                                    OperationOutcome::AdmissionTicketStillActive,
                                ),
                                R::OperationReplayConflict => {
                                    AdmissionRefusal::Verdict(OperationOutcome::DomainConflict)
                                }
                                R::ReservationCapacityExhausted | R::ReplayCapacityExhausted => {
                                    AdmissionRefusal::Verdict(OperationOutcome::ResourceExhausted)
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
                context,
                ticket,
                payload,
                semantics,
                generation,
            } => {
                if self.fenced.contains(&context.frontend_process_id())
                    || self.gone.contains(context)
                {
                    step.effects.push(ControlEffect::Reply {
                        request,
                        outcome: OperationOutcome::Gone,
                    });
                } else if self
                    .owners
                    .get(&context.query_execution_id())
                    .is_some_and(|owner| *owner != context.frontend_process_id())
                {
                    step.effects.push(ControlEffect::Reply {
                        request,
                        outcome: OperationOutcome::IdentityMismatch,
                    });
                } else if self.directory.contains_key(&context) {
                    // An existing entity requires an exact RPC route. Bare
                    // administrative input cannot adjudicate its business state.
                    return Err(CoreError::WrongIdentity);
                } else if self.process_draining {
                    step.effects.push(ControlEffect::Reply {
                        request,
                        outcome: OperationOutcome::Gone,
                    });
                } else if self.directory.len() >= self.config.max_contexts {
                    step.effects.push(ControlEffect::Reply {
                        request,
                        outcome: OperationOutcome::ResourceExhausted,
                    });
                } else {
                    let redeemed = self.admission.redeem(ticket, context, now);
                    match redeemed {
                        Err(_) => step.effects.push(ControlEffect::Reply {
                            request,
                            outcome: OperationOutcome::Gone,
                        }),
                        Ok(
                            AdmissionTicketRedemption::Redeemed(_)
                            | AdmissionTicketRedemption::Replayed(_),
                        ) => {
                            // Context adds materialization/reply storage; the existing ticket's
                            // context-position reservation is moved by the runtime, never charged twice.
                            match capacity.try_reserve(AdmissionNeed::Context {
                                context,
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
                                        context,
                                        DirectoryRecord {
                                            state: QueryContextState::Establishing,
                                            generation,
                                            waiters: [request].into_iter().collect(),
                                            reservation,
                                            reply: None,
                                            removal_pending: false,
                                            removal_requested: false,
                                            first_retained_at: None,
                                            normal_close: None,
                                        },
                                    );
                                    self.owners.insert(
                                        context.query_execution_id(),
                                        context.frontend_process_id(),
                                    );
                                    self.frontend_members
                                        .entry(context.frontend_process_id())
                                        .or_default()
                                        .insert(context);
                                    step.effects.push(ControlEffect::Materialize {
                                        context,
                                        establishment: ContextEstablishment {
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
            ControlInput::EstablishmentDecided { context, decision } => {
                if !self.directory.contains_key(&context) {
                    return Err(CoreError::WrongIdentity);
                }
                if matches!(
                    decision.state,
                    QueryContextState::Absent
                        | QueryContextState::Establishing
                        | QueryContextState::Gone
                ) || (decision.state == QueryContextState::Active)
                    != (decision.outcome == Ok(OperationOutcome::Accepted))
                    || decision.outcome == Ok(OperationOutcome::Idempotent)
                {
                    return Err(CoreError::IllegalFact);
                }
                self.establishment_decided(context, decision, &mut step)?;
            }
            ControlInput::ContextBootstrapFailed { context, refusal } => {
                if matches!(
                    refusal.outcome(),
                    OperationOutcome::Accepted | OperationOutcome::Idempotent
                ) {
                    return Err(CoreError::IllegalFact);
                }
                let record = self
                    .directory
                    .get(&context)
                    .ok_or(CoreError::WrongIdentity)?;
                if record.reply.is_none()
                    && !matches!(
                        record.state,
                        QueryContextState::Establishing | QueryContextState::Aborting
                    )
                {
                    return Err(CoreError::IllegalFact);
                }
                let outcome = match refusal {
                    AdmissionRefusal::Verdict(outcome) => Ok(outcome),
                    AdmissionRefusal::Domain(outcome, failure) => {
                        Err(DomainRefusal { outcome, failure })
                    }
                };
                self.establishment_decided(
                    context,
                    ContextEstablishmentDecision {
                        state: QueryContextState::Aborting,
                        outcome,
                        acknowledgement: None,
                        decided_at: self.now,
                    },
                    &mut step,
                )?;
            }
            ControlInput::ContinueReplies { context } => self.replies(context, &mut step)?,
            ControlInput::Retained {
                context,
                normal_close,
            } => self.retain_context(context, Some(normal_close), &mut step)?,
            ControlInput::HostRemoved { context } => {
                let record = self
                    .directory
                    .get(&context)
                    .ok_or(CoreError::WrongIdentity)?;
                if record.state != QueryContextState::TerminalRetained || !record.waiters.is_empty()
                {
                    return Err(CoreError::IllegalFact);
                }
                let record = self.directory.remove(&context).unwrap();
                self.reply_contexts.remove(&context);
                if self.retained.contains(context) {
                    self.retained
                        .remove(context)
                        .expect("retained context exists");
                }
                if self.gone.len() == self.config.gone_fences {
                    let key = self.gone.oldest().unwrap();
                    self.gone.remove(key).unwrap();
                }
                self.gone
                    .insert(context, now.saturating_add(self.config.retention), 0)
                    .map_err(|_| CoreError::IllegalFact)?;
                self.owners.remove(&context.query_execution_id());
                let frontend = context.frontend_process_id();
                if let Some(members) = self.frontend_members.get_mut(&frontend) {
                    members.remove(&context);
                    if members.is_empty() {
                        self.frontend_members.remove(&frontend);
                    }
                }
                // The live route reader must observe the same Gone decision
                // before the original directory payer leaves the host. Only
                // this core authors the state; the driver does not infer it
                // from a missing host address or a retired reservation.
                step.effects.push(ControlEffect::Route {
                    context,
                    state: QueryContextState::Gone,
                    generation: record.generation,
                });
                step.effects
                    .push(ControlEffect::RetireReservation(record.reservation));
            }
            ControlInput::RevokeTicket { context } => {
                if let Some(r) = self.admission.revoke_unredeemed(context, now) {
                    step.effects.push(ControlEffect::RetireReservation(r));
                }
            }
            ControlInput::FenceFrontend { frontend } => {
                // The permanent fence is also the deduplication authority.
                // A duplicate cannot rewind an in-progress member cursor.
                if !self.fenced.contains(&frontend) {
                    if self.fenced.len() >= self.config.gone_fences {
                        return Err(CoreError::InvalidConfiguration);
                    }
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
                for r in self.admission.advance(now, 1) {
                    step.effects.push(ControlEffect::RetireReservation(r));
                }
                for _ in 0..1 {
                    if let Some(context) = self.retained.take_due(now) {
                        self.request_host_removal(context, &mut step)?;
                    } else {
                        break;
                    }
                }
                for _ in 0..1 {
                    if self.gone.take_due(now).is_none() {
                        break;
                    }
                }
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
        context: QueryContextRef,
        ticket: AdmissionTicketId,
        now: MonotonicInstant,
    ) -> Option<OperationOutcome> {
        if self.directory.contains_key(&context)
            || self.directory.len() >= self.config.max_contexts
            || self.gone.contains(context)
            || self.fenced.contains(&context.frontend_process_id())
            || self
                .owners
                .get(&context.query_execution_id())
                .is_some_and(|owner| *owner != context.frontend_process_id())
        {
            return None;
        }
        matches!(
            self.admission.observe(ticket, context, now),
            crate::admission::AdmissionTicketObservation::Expired
        )
        .then_some(OperationOutcome::InvalidStateOrRequest)
    }
    fn remove_request(&mut self, token: InputId) -> RequestContext {
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
            if let Some(context) = self.establish_reply_owners.remove(&request.operation) {
                if let Some(record) = self.directory.get_mut(&context) {
                    // A dead reply position does not revoke the ticket or the
                    // already-owned context materialization action.
                    record.waiters.remove(&request.operation);
                }
                self.refresh_reply_context(context);
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
                let context = self.establish_reply_owners.get(&operation)?;
                let record = self.directory.get(context)?;
                let (body, _) = record.reply.as_ref()?;
                Some(body.clone())
            })
    }
    fn request_ready_at(&self, operation: TaskOperationId) -> Option<MonotonicInstant> {
        self.ready.get(&operation).map(|(_, at)| *at).or_else(|| {
            self.establish_reply_owners
                .get(&operation)
                .and_then(|context| self.directory.get(context))
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
        if let Some((context, _)) = pending.route {
            let request = self.remove_request(token);
            step.effects
                .push(ControlEffect::CancelEstablishRoute { context, request });
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
        context: QueryContextRef,
        step: &mut ControlStep,
    ) -> Result<(), CoreError> {
        let r = self
            .directory
            .get_mut(&context)
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
                .push(ControlEffect::ContinueReplies { context });
        } else if r.removal_pending {
            r.removal_pending = false;
            if !r.removal_requested {
                r.removal_requested = true;
                step.effects.push(ControlEffect::RemoveHost { context });
            }
        }
        self.refresh_reply_context(context);
        Ok(())
    }
    fn request_host_removal(
        &mut self,
        context: QueryContextRef,
        step: &mut ControlStep,
    ) -> Result<(), CoreError> {
        step.records_visited += 1;
        let record = self
            .directory
            .get_mut(&context)
            .ok_or(CoreError::WrongIdentity)?;
        if record.removal_requested {
            return Ok(());
        }
        if record.reply.is_some() && record.waiters.is_empty() {
            record.removal_requested = true;
            step.effects.push(ControlEffect::RemoveHost { context });
        } else {
            record.removal_pending = true;
            self.refresh_reply_context(context);
        }
        Ok(())
    }
    fn retain_context(
        &mut self,
        context: QueryContextRef,
        normal_close: Option<bool>,
        step: &mut ControlStep,
    ) -> Result<(), CoreError> {
        let record = self
            .directory
            .get(&context)
            .ok_or(CoreError::WrongIdentity)?;
        if let (Some(original), Some(observed)) = (record.normal_close, normal_close)
            && original != observed
        {
            return Err(CoreError::IllegalFact);
        }
        // Consume the original ticket once, independently of a late physical
        // retention observation. Establishment may announce TerminalRetained
        // before that same R's Retained input reaches Root.
        let ticket = self.admission.close_context(context, self.now);
        let record = self.directory.get_mut(&context).expect("known context");
        let first = *record.first_retained_at.get_or_insert(self.now);
        if let Some(observed) = normal_close {
            record.normal_close = Some(observed);
        }
        record.state = QueryContextState::TerminalRetained;
        if let Some(ticket) = ticket {
            step.effects.push(ControlEffect::RetireReservation(ticket));
            step.effects.push(ControlEffect::Route {
                context,
                state: record.state,
                generation: record.generation,
            });
        }
        if record.removal_requested || record.removal_pending || self.retained.contains(context) {
            return Ok(());
        }
        if self.process_draining && self.process_ingress_closed {
            // Real listener/body exit ends external replay obligations. Original
            // Task, R, H and Scope exit gates remain independently mandatory.
            return self.request_host_removal(context, step);
        }
        let Some(protected) = record.normal_close else {
            // Wait for the original R fact, not H: H requires RemoveHost, whose
            // normal-horizon decision in turn needs this observation.
            return Ok(());
        };
        if !protected && self.retained.eligible_len() == self.config.retained_contexts {
            let oldest = self
                .retained
                .oldest_eligible()
                .expect("full eligible set has an oldest");
            self.retained
                .remove(oldest)
                .expect("eligible retained row exists");
            self.request_host_removal(oldest, step)?;
        }
        self.retained
            .insert_with_protection(
                context,
                first.saturating_add(self.config.retention),
                0,
                protected,
            )
            .map_err(|_| CoreError::IllegalFact)?;
        Ok(())
    }
    fn establishment_decided(
        &mut self,
        context: QueryContextRef,
        decision: ContextEstablishmentDecision,
        step: &mut ControlStep,
    ) -> Result<(), CoreError> {
        if self
            .directory
            .get(&context)
            .ok_or(CoreError::WrongIdentity)?
            .reply
            .is_some()
        {
            // A duplicate neutral report cannot replace the original decision
            // or its ready time. The real bridge still owns one-shot authorship.
            return self.replies(context, step);
        }
        if decision.state == QueryContextState::TerminalRetained {
            self.retain_context(context, None, step)?;
        }
        let record = self.directory.get_mut(&context).expect("known context");
        let ContextEstablishmentDecision {
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
                || ack.receipt.context() != context
                || ack.receipt.state() != state
                || ack.receipt.lease().is_none()
            {
                return Err(CoreError::IllegalFact);
            }
        } else if outcome == Ok(OperationOutcome::Accepted) {
            return Err(CoreError::IllegalFact);
        }
        // Root fencing can outrun the Context's success return. It blocks only
        // reopening; it cannot erase a typed failure or a lease verdict.
        let fenced_active = state == QueryContextState::Active
            && self.fenced.contains(&context.frontend_process_id());
        if state == QueryContextState::Active
            && (record.state != QueryContextState::Establishing || fenced_active)
        {
            outcome = Ok(OperationOutcome::ContextTerminalReceipt);
            // This is the Root's later fence refusal, not the Context's old
            // successful receipt. Never rewrite that receipt's state/lease.
            acknowledgement = None;
            decided_at = self.now;
        }
        record.state = match (record.state, state) {
            (QueryContextState::TerminalRetained, _) | (_, QueryContextState::TerminalRetained) => {
                QueryContextState::TerminalRetained
            }
            (QueryContextState::Releasing, _) => QueryContextState::Releasing,
            (QueryContextState::Aborting, _) => QueryContextState::Aborting,
            _ if fenced_active => QueryContextState::Aborting,
            _ => state,
        };
        let body = match outcome {
            Ok(outcome) => acknowledgement.map_or_else(
                || ControlReplyBody::Verdict(RequestResult::Outcome(outcome)),
                ControlReplyBody::Context,
            ),
            Err(refusal) => ControlReplyBody::Domain(refusal),
        };
        record.reply = Some((body, decided_at));
        if record.state != QueryContextState::TerminalRetained {
            step.effects.push(ControlEffect::Route {
                context,
                state: record.state,
                generation: record.generation,
            });
        }
        self.replies(context, step)
    }
    fn refresh_reply_context(&mut self, context: QueryContextRef) {
        if self.directory.get(&context).is_some_and(|record| {
            record.reply.is_some() && (!record.waiters.is_empty() || record.removal_pending)
        }) {
            self.reply_contexts.insert(context);
        } else {
            self.reply_contexts.remove(&context);
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
            for context in keys {
                step.records_visited += 1;
                cursor = Some(context);
                if let Some(record) = self.directory.get_mut(&context)
                    && record.state != QueryContextState::TerminalRetained
                {
                    record.state = QueryContextState::Aborting;
                    record.generation = record
                        .generation
                        .next()
                        .expect("context fence generation exhausted");
                    step.effects.push(ControlEffect::Route {
                        context,
                        state: record.state,
                        generation: record.generation,
                    });
                    step.effects.push(ControlEffect::AbortContext {
                        context,
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
