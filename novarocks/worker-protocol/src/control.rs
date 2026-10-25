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
    admission::{AdmissionCore, AdmissionTicketConfig, AdmissionTicketRedemption},
    context::{CoreError, RequestResult, RequestSettlement},
    creation::{AdmissionNeed, AdmissionPort, PayloadId, RequestContext},
    lease::MonotonicInstant,
    retention::RetentionIndex,
};
use novarocks_control_runtime_core::identity::{Generation, InputId, ReservationId};
use novarocks_execution_contract::{
    AcquireQueryContextAdmissionTicket, AdmissionTicketId, OperationOutcome,
    QueryContextAdmissionTicketReceipt, QueryContextRef, QueryContextState, TaskOperationId,
};
use novarocks_execution_contract::{ContentFingerprint, CredentialEpoch, CredentialLeaseId};
use novarocks_types::identity::{FrontendProcessId, QueryExecutionId};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug)]
pub struct ControlConfig {
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
}
#[derive(Debug)]
struct DirectoryRecord {
    state: QueryContextState,
    generation: Generation,
    payload: PayloadId,
    semantics: EstablishSemanticFacts,
    waiters: BTreeSet<TaskOperationId>,
    reservation: ReservationId,
    reply: Option<OperationOutcome>,
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
        compared_payload: Option<PayloadId>,
        same_credential: bool,
        generation: Generation,
    },
    Materialized {
        context: QueryContextRef,
        result: Result<(), OperationOutcome>,
    },
    ContinueReplies {
        context: QueryContextRef,
    },
    Retained {
        context: QueryContextRef,
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
    Tick,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlEffect {
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
        payload: PayloadId,
        reservation: ReservationId,
        generation: Generation,
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
}
#[derive(Debug, Default)]
pub struct ControlStep {
    pub effects: Vec<ControlEffect>,
    pub requests: Vec<RequestSettlement>,
    pub records_visited: usize,
}
impl ControlStep {
    /// Total cost includes outputs. Charge `records_visited` only when the
    /// runtime's per-effect emission already debits each effect/reply.
    pub fn charged_work(&self) -> usize {
        self.effects.len() + self.requests.len() + self.records_visited
    }
}
#[derive(Debug)]
pub struct ControlCore {
    config: ControlConfig,
    now: MonotonicInstant,
    admission: AdmissionCore,
    directory: BTreeMap<QueryContextRef, DirectoryRecord>,
    owners: BTreeMap<QueryExecutionId, FrontendProcessId>,
    retained: RetentionIndex<QueryContextRef>,
    gone: RetentionIndex<QueryContextRef>,
    frontend_members: BTreeMap<FrontendProcessId, BTreeSet<QueryContextRef>>,
    fenced: BTreeSet<FrontendProcessId>,
    requests: BTreeMap<InputId, RequestContext>,
    request_operations: BTreeMap<TaskOperationId, BTreeSet<InputId>>,
    establish_reply_owners: BTreeMap<TaskOperationId, QueryContextRef>,
    request_deadlines: BTreeSet<(MonotonicInstant, InputId)>,
    ready: BTreeMap<TaskOperationId, (OperationOutcome, MonotonicInstant)>,
    last_request_token: u64,
    fencing: Option<(FrontendProcessId, Option<QueryContextRef>)>,
}
impl ControlCore {
    pub const MAX_STEP_WORK: usize = 15;
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
            retained: RetentionIndex::new(config.retained_contexts, u64::MAX),
            gone: RetentionIndex::new(config.gone_fences, u64::MAX),
            frontend_members: BTreeMap::new(),
            fenced: BTreeSet::new(),
            requests: BTreeMap::new(),
            request_operations: BTreeMap::new(),
            establish_reply_owners: BTreeMap::new(),
            request_deadlines: BTreeSet::new(),
            ready: BTreeMap::new(),
            last_request_token: 0,
            fencing: None,
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
            ControlInput::Establish { request, .. } => *request,
            _ => return Err(CoreError::WrongIdentity),
        };
        if operation != request.operation || request.token.get() <= self.last_request_token {
            return Err(CoreError::WrongIdentity);
        }
        self.last_request_token = request.token.get();
        self.now = now;
        if now.has_reached(request.deadline) {
            let owned = self.request_operations.contains_key(&operation)
                || match &input {
                    ControlInput::Establish { context, .. } => self.directory.contains_key(context),
                    ControlInput::Acquire { .. } => self.admission.has_decision(operation),
                    _ => false,
                };
            if owned {
                let can_replay = match &input {
                    ControlInput::Acquire { .. } => self.admission.has_decision(operation),
                    ControlInput::Establish { context, .. } => self
                        .directory
                        .get(context)
                        .is_some_and(|record| record.reply.is_some()),
                    _ => false,
                };
                if can_replay {
                    let mut step = self.handle(now, input, capacity)?;
                    let outcome =
                        step.effects
                            .iter()
                            .find_map(|effect| match effect {
                                ControlEffect::Reply {
                                    request: id,
                                    outcome,
                                } if *id == operation => Some(*outcome),
                                ControlEffect::AdmissionReply {
                                    request: id,
                                    result,
                                } if *id == operation => Some(result.as_ref().map_or_else(
                                    |outcome| *outcome,
                                    |_| OperationOutcome::Accepted,
                                )),
                                _ => None,
                            })
                            .ok_or(CoreError::IllegalFact)?;
                    step.requests.push(RequestSettlement {
                        request,
                        result: RequestResult::Outcome(outcome),
                    });
                    return Ok(step);
                }
                return Ok(ControlStep {
                    effects: Vec::new(),
                    records_visited: 1,
                    requests: vec![RequestSettlement {
                        request,
                        result: RequestResult::TimeoutWhileAccepted,
                    }],
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
                requests: vec![RequestSettlement {
                    request,
                    result: RequestResult::Outcome(outcome),
                }],
            });
        }
        let joined = self.request_operations.contains_key(&operation);
        if !joined && let ControlInput::Establish { context, .. } = &input {
            self.establish_reply_owners.insert(operation, *context);
        }
        self.requests.insert(request.token, request);
        self.request_operations
            .entry(operation)
            .or_default()
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
        let mut step = ControlStep {
            records_visited: match &input {
                ControlInput::Tick => 4,
                ControlInput::HostRemoved { .. } => 3,
                ControlInput::Retained { .. } => 2,
                _ => 1,
            },
            ..ControlStep::default()
        };
        match input {
            ControlInput::ContinueRequestReplies { operation } => {
                self.flush_request(operation, &mut step)
            }
            ControlInput::Acquire { request, ticket } => {
                let operation = request.envelope().operation_id();
                let result = if self
                    .fenced
                    .contains(&request.context().frontend_process_id())
                {
                    Err(OperationOutcome::Gone)
                } else {
                    self.admission
                        .acquire(request, ticket, now, capacity)
                        .map(|g| g.receipt())
                        .map_err(|e| {
                            use crate::admission::AdmissionTicketAcquisitionRejection as R;
                            match e {
                                R::ContextAlreadyGranted => {
                                    OperationOutcome::AdmissionTicketStillActive
                                }
                                R::OperationReplayConflict => OperationOutcome::DomainConflict,
                                R::ReservationCapacityExhausted | R::ReplayCapacityExhausted => {
                                    OperationOutcome::ResourceExhausted
                                }
                                R::ValidityExceedsWorkerLimit => {
                                    OperationOutcome::InvalidStateOrRequest
                                }
                                _ => OperationOutcome::Gone,
                            }
                        })
                };
                step.effects.push(ControlEffect::AdmissionReply {
                    request: operation,
                    result,
                });
            }
            ControlInput::Establish {
                request,
                context,
                ticket,
                payload,
                semantics,
                compared_payload,
                same_credential,
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
                } else if let Some(record) = self.directory.get_mut(&context) {
                    if record.semantics != semantics
                        || compared_payload != Some(record.payload)
                        || !same_credential
                    {
                        step.effects.push(ControlEffect::Reply {
                            request,
                            outcome: OperationOutcome::ContextConflict,
                        });
                    } else if record.state == QueryContextState::Establishing {
                        if record.waiters.contains(&request) {
                        } else if record.waiters.len() == self.config.max_waiters {
                            step.effects.push(ControlEffect::Reply {
                                request,
                                outcome: OperationOutcome::ResourceExhausted,
                            });
                        } else {
                            record.waiters.insert(request);
                        }
                    } else {
                        step.effects.push(ControlEffect::Reply {
                            request,
                            outcome: if record.state == QueryContextState::Active {
                                OperationOutcome::Idempotent
                            } else {
                                OperationOutcome::ContextTerminalReceipt
                            },
                        });
                    }
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
                                Err(outcome) => {
                                    step.effects.push(ControlEffect::Reply { request, outcome })
                                }
                                Ok(reservation) => {
                                    self.directory.insert(
                                        context,
                                        DirectoryRecord {
                                            state: QueryContextState::Establishing,
                                            generation,
                                            payload,
                                            semantics,
                                            waiters: [request].into_iter().collect(),
                                            reservation,
                                            reply: None,
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
                                        payload,
                                        reservation,
                                        generation,
                                    });
                                }
                            }
                        }
                    }
                }
            }
            ControlInput::Materialized { context, result } => {
                let record = self
                    .directory
                    .get_mut(&context)
                    .ok_or(CoreError::WrongIdentity)?;
                if !matches!(
                    record.state,
                    QueryContextState::Establishing | QueryContextState::Aborting
                ) {
                    return Err(CoreError::IllegalFact);
                }
                if record.state == QueryContextState::Aborting {
                    record.reply = Some(OperationOutcome::ContextTerminalReceipt);
                } else {
                    record.state = if result.is_ok() {
                        QueryContextState::Active
                    } else {
                        QueryContextState::Aborting
                    };
                    record.reply = Some(result.err().unwrap_or(OperationOutcome::Accepted));
                }
                step.effects.push(ControlEffect::Route {
                    context,
                    state: record.state,
                    generation: record.generation,
                });
                self.replies(context, &mut step)?;
            }
            ControlInput::ContinueReplies { context } => self.replies(context, &mut step)?,
            ControlInput::Retained { context } => {
                let record = self
                    .directory
                    .get_mut(&context)
                    .ok_or(CoreError::WrongIdentity)?;
                record.state = QueryContextState::TerminalRetained;
                step.effects.push(ControlEffect::Route {
                    context,
                    state: record.state,
                    generation: record.generation,
                });
                if self.retained.len() == self.config.retained_contexts {
                    let oldest = self
                        .retained
                        .oldest()
                        .expect("full retained set has an oldest");
                    self.retained.remove(oldest).expect("oldest exists");
                    step.effects
                        .push(ControlEffect::RemoveHost { context: oldest });
                }
                self.retained
                    .insert(context, now.saturating_add(self.config.retention), 0)
                    .map_err(|_| CoreError::IllegalFact)?;
            }
            ControlInput::HostRemoved { context } => {
                let record = self
                    .directory
                    .get(&context)
                    .ok_or(CoreError::WrongIdentity)?;
                if record.state != QueryContextState::TerminalRetained {
                    return Err(CoreError::IllegalFact);
                }
                let record = self.directory.remove(&context).unwrap();
                let _ = record.payload;
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
                if let Some(members) = self
                    .frontend_members
                    .get_mut(&context.frontend_process_id())
                {
                    members.remove(&context);
                }
                step.effects
                    .push(ControlEffect::RetireReservation(record.reservation));
                if let Some(ticket) = self.admission.close_context(context, now) {
                    step.effects.push(ControlEffect::RetireReservation(ticket));
                }
            }
            ControlInput::RevokeTicket { context } => {
                if let Some(r) = self.admission.close_context(context, now) {
                    step.effects.push(ControlEffect::RetireReservation(r));
                }
            }
            ControlInput::FenceFrontend { frontend } => {
                if self.fencing.is_some() {
                    return Err(CoreError::IllegalFact);
                }
                if !self.fenced.contains(&frontend) && self.fenced.len() >= self.config.gone_fences
                {
                    return Err(CoreError::InvalidConfiguration);
                }
                self.fenced.insert(frontend);
                self.fencing = Some((frontend, None));
                step.effects.push(ControlEffect::ContinueFence);
            }
            ControlInput::ContinueFence => self.fence(&mut step),
            ControlInput::Tick => {
                self.expire_request(&mut step);
                for r in self.admission.advance(now, 1) {
                    step.effects.push(ControlEffect::RetireReservation(r));
                }
                for _ in 0..1 {
                    if let Some(context) = self.retained.take_due(now) {
                        step.effects.push(ControlEffect::RemoveHost { context });
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
            .filter_map(|effect| match effect {
                ControlEffect::Reply { request, outcome } => Some((*request, *outcome)),
                ControlEffect::AdmissionReply { request, result } => Some((
                    *request,
                    result
                        .as_ref()
                        .map_or_else(|outcome| *outcome, |_| OperationOutcome::Accepted),
                )),
                _ => None,
            })
            .collect();
        for (operation, outcome) in ready {
            if self.request_operations.contains_key(&operation) {
                self.ready.entry(operation).or_insert((outcome, now));
                self.flush_request(operation, &mut step);
            }
        }
        Ok(step)
    }
    fn remove_request(&mut self, token: InputId) -> RequestContext {
        let request = self.requests.remove(&token).expect("indexed request");
        self.request_deadlines.remove(&(request.deadline, token));
        let tokens = self
            .request_operations
            .get_mut(&request.operation)
            .expect("request operation");
        tokens.remove(&token);
        if tokens.is_empty() {
            self.request_operations.remove(&request.operation);
            self.ready.remove(&request.operation);
            if let Some(context) = self.establish_reply_owners.remove(&request.operation)
                && let Some(record) = self.directory.get_mut(&context)
            {
                // A dead reply position does not revoke the ticket or the
                // already-owned context materialization action.
                record.waiters.remove(&request.operation);
            }
        }
        request
    }
    fn flush_request(&mut self, operation: TaskOperationId, step: &mut ControlStep) {
        let Some((outcome, at)) = self.ready.get(&operation).copied() else {
            return;
        };
        if step.requests.is_empty() {
            let token = self.request_operations[&operation]
                .first()
                .copied()
                .expect("operation waiter");
            let request = self.remove_request(token);
            step.requests.push(RequestSettlement {
                request,
                result: if at.has_reached(request.deadline) {
                    RequestResult::TimeoutWhileAccepted
                } else {
                    RequestResult::Outcome(outcome)
                },
            });
        }
        if self.request_operations.contains_key(&operation) {
            step.effects
                .push(ControlEffect::ContinueRequestReplies { operation });
        }
    }
    fn expire_request(&mut self, step: &mut ControlStep) {
        let Some((at, token)) = self.request_deadlines.first().copied() else {
            return;
        };
        if self.now < at {
            return;
        }
        self.request_deadlines.pop_first();
        let operation = self.requests[&token].operation;
        if self
            .ready
            .get(&operation)
            .is_some_and(|(_, ready)| *ready < at)
        {
            return;
        }
        let request = self.remove_request(token);
        step.requests.push(RequestSettlement {
            request,
            result: RequestResult::TimeoutWhileAccepted,
        });
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
        let outcome = r.reply.ok_or(CoreError::IllegalFact)?;
        if let Some(request) = r.waiters.pop_first() {
            step.effects.push(ControlEffect::Reply { request, outcome });
        }
        if !r.waiters.is_empty() {
            step.effects
                .push(ControlEffect::ContinueReplies { context });
        }
        Ok(())
    }
    fn fence(&mut self, step: &mut ControlStep) {
        use std::ops::Bound::{Excluded, Unbounded};
        let Some((frontend, mut cursor)) = self.fencing else {
            return;
        };
        if let Some(members) = self.frontend_members.get(&frontend) {
            let keys: Vec<_> = match cursor {
                Some(cursor) => members.range((Excluded(cursor), Unbounded)),
                None => members.range(..),
            }
            .take(1)
            .copied()
            .collect();
            for context in keys {
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
    }
}
