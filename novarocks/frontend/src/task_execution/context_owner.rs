// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The single owner of one query context on one backend.
//!
//! Exactly one of these exists per [`QueryContextRef`], and it is the only
//! thing in the frontend that mints a lease sequence, sends a renewal, or
//! sends a release. Stages and remote tasks publish facts to it — a create
//! was acknowledged, a task drained, an output responsibility closed — and
//! never touch the lease themselves. That is what keeps a stalled domain
//! update, or one whose outcome is unknown, from silently stopping the
//! liveness contract of a healthy backend.

use std::sync::Arc;
use std::time::Duration;

use novarocks_execution::task_execution::{
    AbortCause, AbortQueryContext, AcquireQueryContextAdmissionTicket, AdmissionEpochCapability,
    CodecOwnedContent, CredentialUpdate, EstablishQueryContext, LeaseSequence, LeaseValidFor,
    OperationKind, OperationOutcome, QueryContextAdmissionTicketReceipt, QueryContextRef,
    QueryContextState, ReleaseOutcome, ReleaseQueryContext, RenewQueryExecutionLease,
    TaskOperationId, UpdateQueryContext,
};
use novarocks_query_application::coordination::{
    ContextTransition, FrontendAction, MonotonicInstant, QueryContextEvent, RenewSchedule,
    RequestedLeaseDurations, classify_context_transition, context_state_is_closed, frontend_action,
};

use novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry;
use novarocks_types::NativeCompatibilityId;

use super::error::TaskExecutionError;
use super::intent::{AckPayload, OperationAcknowledgement, OperationIntent};

/// Backoff for a release that reached a backend before its local in-flight
/// operations finished settling.
///
/// `NotReady` is an observed outcome, so retrying the exact immutable request
/// is safe. The lower bound prevents the coordinator's 5 ms turn cadence from
/// becoming an RPC loop; the cap keeps a context converging well inside its
/// lease and the attempt's drain budget.
const RELEASE_RETRY_INITIAL: Duration = Duration::from_millis(50);
const RELEASE_RETRY_MAX: Duration = Duration::from_secs(1);

/// How long query coordination asks a Worker to hold an unconsumed ticket.
///
/// This is a query-side policy. Worker retention limits remain Worker-owned
/// and are not imported into the frontend.
const ADMISSION_TICKET_VALID_FOR: Duration = Duration::from_secs(10);

/// The shared facts an establish installs atomically.
///
/// All four are codec-owned content: the frontend can size and pass them
/// along, and cannot walk them. The credential never reaches a comparable or
/// printable form at all.
#[derive(Clone)]
pub struct ContextEstablishFacts {
    pub catalog_binding: Arc<dyn CodecOwnedContent>,
    pub initial_runtime_filter: Arc<dyn CodecOwnedContent>,
    pub query_options: Arc<dyn CodecOwnedContent>,
    pub initial_credential: CredentialUpdate,
}

impl std::fmt::Debug for ContextEstablishFacts {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ContextEstablishFacts")
            .field("catalog_binding", &self.catalog_binding.fingerprint())
            .field(
                "initial_runtime_filter",
                &self.initial_runtime_filter.fingerprint(),
            )
            .field("query_options", &self.query_options.fingerprint())
            .field("initial_credential", &self.initial_credential)
            .finish()
    }
}

/// Where the establish facts of one context come from.
pub trait ContextEstablishSource {
    fn facts_for(
        &self,
        context: QueryContextRef,
    ) -> Result<ContextEstablishFacts, TaskExecutionError>;
}

/// One released lifecycle request this owner must be able to replay verbatim.
#[derive(Clone, Debug)]
struct ReleasedLease {
    request: Arc<UpdateQueryContext>,
    sequence: LeaseSequence,
    /// The earliest frontend-local send time of this immutable request.
    ///
    /// A retry keeps the first send time on purpose. The backend's own timer
    /// starts when it applies the request, which can be as early as the first
    /// send, so scheduling from a later retry's send time would credit the
    /// lease with time it does not have.
    first_sent_at: MonotonicInstant,
    /// Whether this request is released and its outcome not yet known.
    ///
    /// A retained request is handed out again only after a genuinely unknown
    /// outcome. While it is merely in flight it must not be queued a second
    /// time.
    awaiting_outcome: bool,
}

/// One immutable capacity request retained for exact replay.
#[derive(Copy, Clone, Debug)]
struct ReleasedAdmission {
    request: AcquireQueryContextAdmissionTicket,
    first_sent_at: MonotonicInstant,
    awaiting_outcome: bool,
}

/// A validated grant held until the first establish is minted.
#[derive(Copy, Clone, Debug)]
struct GrantedAdmission {
    receipt: QueryContextAdmissionTicketReceipt,
    conservative_expiry: MonotonicInstant,
}

/// What a release answer did to this owner.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ReleaseSettlement {
    /// Shared resources are gone and this context is retained terminal.
    Released,
    /// Something is still draining. The owner keeps renewing and resends the
    /// identical request after local progress or a bounded backoff.
    NotReadyKeepRenewing,
    /// The outcome was genuinely unknown; the identical request must be resent.
    RetryExactRequest,
    /// The release failed closed.
    FailedClosed(OperationOutcome),
}

/// The frontend's owner of one query context.
#[derive(Debug)]
pub struct QueryContextOwner {
    context: QueryContextRef,
    native_compatibility_id: NativeCompatibilityId,
    admission_epoch_capability: AdmissionEpochCapability,
    state: QueryContextState,
    admission: Option<ReleasedAdmission>,
    admission_ticket: Option<GrantedAdmission>,
    admission_qualified: bool,
    lease_sequence: LeaseSequence,
    renew_schedule: Option<RenewSchedule>,
    establish: Option<ReleasedLease>,
    establish_acknowledged: bool,
    renewal: Option<ReleasedLease>,
    expected_creates: usize,
    acknowledged_creates: usize,
    expected_tasks: usize,
    drained_tasks: usize,
    released_outputs: usize,
    release: Option<ReleaseQueryContext>,
    release_in_flight: bool,
    release_blocked_at: Option<u64>,
    release_retry_at: Option<MonotonicInstant>,
    release_retry_delay: Duration,
    released: bool,
    abort: Option<AbortQueryContext>,
    progress: u64,
    /// The sealed runtime-filter observation this backend reported on the
    /// release that took its shared facts down.
    ///
    /// This owner is the only frontend holder of it: the release
    /// acknowledgement is the only message that carries it, and it is settled
    /// here. `None` after a release means the backend installed no
    /// participant for this query.
    runtime_filter_contribution: Option<QueryTerminalProfileContributionTelemetry>,
}

impl QueryContextOwner {
    /// Creates the owner of one context, over the frozen task set this
    /// attempt places on that backend.
    pub const fn new(
        context: QueryContextRef,
        tasks: usize,
        native_compatibility_id: NativeCompatibilityId,
        admission_epoch_capability: AdmissionEpochCapability,
    ) -> Self {
        Self {
            context,
            native_compatibility_id,
            admission_epoch_capability,
            state: QueryContextState::Absent,
            admission: None,
            admission_ticket: None,
            admission_qualified: true,
            lease_sequence: LeaseSequence::INITIAL,
            renew_schedule: None,
            establish: None,
            establish_acknowledged: false,
            renewal: None,
            expected_creates: tasks,
            acknowledged_creates: 0,
            expected_tasks: tasks,
            drained_tasks: 0,
            released_outputs: 0,
            release: None,
            release_in_flight: false,
            release_blocked_at: None,
            release_retry_at: None,
            release_retry_delay: RELEASE_RETRY_INITIAL,
            released: false,
            abort: None,
            progress: 0,
            runtime_filter_contribution: None,
        }
    }

    pub const fn context(&self) -> QueryContextRef {
        self.context
    }

    pub const fn state(&self) -> QueryContextState {
        self.state
    }

    pub const fn lease_sequence(&self) -> LeaseSequence {
        self.lease_sequence
    }

    pub const fn renew_schedule(&self) -> Option<RenewSchedule> {
        self.renew_schedule
    }

    /// The sealed runtime-filter observation this backend reported, once its
    /// release settled.
    ///
    /// Read by the attempt after the drain: it is the only production reader
    /// of the release acknowledgement's contribution, and a query whose
    /// backends never released has nothing here rather than an empty
    /// contribution.
    pub const fn runtime_filter_contribution(
        &self,
    ) -> Option<&QueryTerminalProfileContributionTelemetry> {
        self.runtime_filter_contribution.as_ref()
    }

    pub const fn is_released(&self) -> bool {
        self.released
    }

    /// Whether this owner must keep renewing.
    ///
    /// Renewal outlasts the client's end-of-stream: the read completes as soon
    /// as the root task's output is complete, while this context still holds
    /// resources until the release is answered.
    pub const fn must_keep_renewing(&self) -> bool {
        !self.released && !matches!(self.state, QueryContextState::Gone)
    }

    /// Whether this owner still has to establish its context.
    pub const fn needs_establish(&self) -> bool {
        !self.establish_acknowledged
    }

    /// Whether this context still needs a Worker capacity grant.
    pub const fn needs_admission_ticket(&self) -> bool {
        self.admission_qualified
            && self.admission_ticket.is_none()
            && self.establish.is_none()
            && !self.establish_acknowledged
    }

    /// Acquires Worker-local capacity before the first establish.
    ///
    /// A transport-unknown settlement only releases the in-flight marker. The
    /// exact request, including its operation id and compatibility identity,
    /// remains here and is handed out verbatim on the next turn.
    pub fn admission_intent(
        &mut self,
        now: MonotonicInstant,
    ) -> Result<Option<OperationIntent>, TaskExecutionError> {
        if !self.needs_admission_ticket() {
            return Ok(None);
        }
        if let Some(released) = &mut self.admission {
            if released.awaiting_outcome {
                return Ok(None);
            }
            released.awaiting_outcome = true;
            return Ok(Some(OperationIntent::AcquireQueryContextAdmissionTicket(
                released.request,
            )));
        }
        if !matches!(self.state, QueryContextState::Absent) {
            return Ok(None);
        }
        let valid_for = LeaseValidFor::new(ADMISSION_TICKET_VALID_FOR)
            .map_err(|error| TaskExecutionError::Schedule(error.to_string()))?;
        let request = AcquireQueryContextAdmissionTicket::new(
            TaskOperationId::new_v7(),
            self.context,
            valid_for,
            self.native_compatibility_id,
            self.admission_epoch_capability,
        );
        self.admission = Some(ReleasedAdmission {
            request,
            first_sent_at: now,
            awaiting_outcome: true,
        });
        Ok(Some(OperationIntent::AcquireQueryContextAdmissionTicket(
            request,
        )))
    }

    /// The establish request, once.
    pub fn establish_intent(
        &mut self,
        facts: ContextEstablishFacts,
        now: MonotonicInstant,
    ) -> Result<Option<OperationIntent>, TaskExecutionError> {
        if self.establish_acknowledged {
            return Ok(None);
        }
        if let Some(released) = &mut self.establish {
            if released.awaiting_outcome {
                return Ok(None);
            }
            released.awaiting_outcome = true;
            return Ok(Some(OperationIntent::UpdateQueryContext(Arc::clone(
                &released.request,
            ))));
        }
        if !matches!(
            classify_context_transition(self.state, QueryContextEvent::Establish),
            ContextTransition::Apply(_)
        ) {
            return Ok(None);
        }
        let Some(granted) = self.admission_ticket else {
            return Ok(None);
        };
        if now.has_reached(granted.conservative_expiry) {
            self.admission_ticket = None;
            self.admission_qualified = false;
            self.state = QueryContextState::TerminalRetained;
            self.released = true;
            return Err(TaskExecutionError::OperationFailed {
                kind: OperationKind::AcquireQueryContextAdmissionTicket,
                outcome: OperationOutcome::LeaseExpired,
                detail: Some("the admission ticket expired before establish was sent".to_owned()),
            });
        }
        let valid_for = LeaseValidFor::new(RequestedLeaseDurations::DEFAULT.initial())
            .map_err(|error| TaskExecutionError::Schedule(error.to_string()))?;
        let request = Arc::new(UpdateQueryContext::Establish(EstablishQueryContext::new(
            TaskOperationId::new_v7(),
            self.context,
            granted.receipt.ticket_id(),
            facts.catalog_binding,
            facts.initial_runtime_filter,
            facts.query_options,
            facts.initial_credential,
            valid_for,
        )));
        self.state = QueryContextState::Establishing;
        self.admission_ticket = None;
        self.admission_qualified = false;
        self.establish = Some(ReleasedLease {
            request: Arc::clone(&request),
            sequence: LeaseSequence::INITIAL,
            first_sent_at: now,
            awaiting_outcome: true,
        });
        Ok(Some(OperationIntent::UpdateQueryContext(request)))
    }

    /// The next renewal, if one is due.
    ///
    /// This deliberately consults nothing but the lease schedule. It never
    /// reads a task state, a queued domain fact, or an unsettled update, so a
    /// domain update whose outcome is unknown cannot block liveness.
    pub fn renew_intent(
        &mut self,
        now: MonotonicInstant,
    ) -> Result<Option<OperationIntent>, TaskExecutionError> {
        if !self.must_keep_renewing() {
            return Ok(None);
        }
        if let Some(released) = &mut self.renewal {
            if released.awaiting_outcome {
                return Ok(None);
            }
            released.awaiting_outcome = true;
            return Ok(Some(OperationIntent::UpdateQueryContext(Arc::clone(
                &released.request,
            ))));
        }
        let Some(schedule) = self.renew_schedule else {
            return Ok(None);
        };
        if !schedule.must_renew_at(now) {
            return Ok(None);
        }
        let sequence = self.lease_sequence.next().ok_or_else(|| {
            TaskExecutionError::Schedule("lease sequence space exhausted".to_owned())
        })?;
        let valid_for = LeaseValidFor::new(RequestedLeaseDurations::DEFAULT.steady())
            .map_err(|error| TaskExecutionError::Schedule(error.to_string()))?;
        let request = Arc::new(UpdateQueryContext::RenewLease(
            RenewQueryExecutionLease::new(
                TaskOperationId::new_v7(),
                self.context,
                sequence,
                valid_for,
            ),
        ));
        self.renewal = Some(ReleasedLease {
            request: Arc::clone(&request),
            sequence,
            first_sent_at: now,
            awaiting_outcome: true,
        });
        Ok(Some(OperationIntent::UpdateQueryContext(request)))
    }

    /// The release request, once every local obligation has closed.
    pub fn release_intent(&mut self, now: MonotonicInstant) -> Option<OperationIntent> {
        if self.released || self.release_in_flight {
            return None;
        }
        if !matches!(self.state, QueryContextState::Active) {
            return None;
        }
        if !self.creates_closed() || !self.locally_drained() {
            return None;
        }
        if self.release_blocked_at == Some(self.progress) {
            if self.release_retry_at.is_none_or(|retry_at| now < retry_at) {
                return None;
            }
        } else {
            // Frontend-visible progress is stronger than the timer: retry
            // immediately, and begin again at the initial delay if the
            // backend still has a local operation settling.
            self.release_retry_at = None;
            self.release_retry_delay = RELEASE_RETRY_INITIAL;
        }
        let request = *self.release.get_or_insert_with(|| {
            ReleaseQueryContext::new(TaskOperationId::new_v7(), self.context)
        });
        self.release_in_flight = true;
        Some(OperationIntent::ReleaseQueryContext(request))
    }

    /// The forced stand-down of this context, once.
    pub fn abort_intent(&mut self, cause: AbortCause) -> Option<OperationIntent> {
        if self.released || self.abort.is_some() {
            return None;
        }
        let request = *self.abort.get_or_insert_with(|| {
            AbortQueryContext::new(TaskOperationId::new_v7(), self.context, cause)
        });
        self.admission_qualified = false;
        self.admission_ticket = None;
        Some(OperationIntent::AbortQueryContext(request))
    }

    /// Whether no legal create can follow.
    pub const fn creates_closed(&self) -> bool {
        self.acknowledged_creates >= self.expected_creates
    }

    /// Whether every local task and output responsibility has drained.
    ///
    /// This is per-context on purpose. The attempt-level drain, which also
    /// waits for every context to be released, is derived separately from the
    /// neutral `AttemptDrainFacts`.
    pub const fn locally_drained(&self) -> bool {
        self.drained_tasks >= self.expected_tasks && self.released_outputs >= self.expected_tasks
    }

    pub fn note_create_acknowledged(&mut self) {
        self.acknowledged_creates += 1;
        self.advance();
    }

    pub fn note_task_drained(&mut self) {
        self.drained_tasks += 1;
        self.advance();
    }

    pub fn note_output_released(&mut self) {
        self.released_outputs += 1;
        self.advance();
    }

    fn advance(&mut self) {
        self.progress = self.progress.saturating_add(1);
    }

    /// Settles one Worker admission answer without starting an establish.
    ///
    /// The next runner turn is the only place that may consume a held ticket.
    /// This keeps a late receipt from advancing a context after its owner lost
    /// coordination qualification.
    pub fn on_admission_ack(
        &mut self,
        ack: &OperationAcknowledgement,
        now: MonotonicInstant,
    ) -> Result<(), TaskExecutionError> {
        let released = self
            .admission
            .as_mut()
            .filter(|released| {
                released.awaiting_outcome
                    && released.request.envelope().operation_id() == ack.operation_id()
            })
            .ok_or(TaskExecutionError::UnknownOperation)?;
        released.awaiting_outcome = false;
        let released = *released;

        if !self.admission_qualified {
            self.admission = None;
            return Ok(());
        }
        if matches!(
            frontend_action(ack.dispatch_result()),
            FrontendAction::RetryExactRequest
        ) {
            return Ok(());
        }
        if !ack.is_applied() {
            self.fail_admission();
            return Err(TaskExecutionError::OperationFailed {
                kind: OperationKind::AcquireQueryContextAdmissionTicket,
                outcome: ack
                    .worker_outcome()
                    .expect("a non-transport acknowledgement has a Worker outcome"),
                detail: ack.detail().map(|detail| detail.as_str().to_owned()),
            });
        }
        let AckPayload::AdmissionTicket(receipt) = ack.payload() else {
            self.fail_admission();
            return Err(TaskExecutionError::MissingReceipt(
                OperationKind::AcquireQueryContextAdmissionTicket,
            ));
        };
        self.context.verify_matches(receipt.context())?;
        if receipt.valid_for() != released.request.valid_for() {
            self.fail_admission();
            return Err(TaskExecutionError::OperationFailed {
                kind: OperationKind::AcquireQueryContextAdmissionTicket,
                outcome: OperationOutcome::DomainConflict,
                detail: Some(
                    "the admission ticket validity differs from the exact request".to_owned(),
                ),
            });
        }
        let conservative_expiry = released
            .first_sent_at
            .saturating_add(receipt.valid_for().get());
        if now.has_reached(conservative_expiry) {
            self.fail_admission();
            return Err(TaskExecutionError::OperationFailed {
                kind: OperationKind::AcquireQueryContextAdmissionTicket,
                outcome: OperationOutcome::LeaseExpired,
                detail: Some("the admission ticket expired before its receipt arrived".to_owned()),
            });
        }
        self.admission = None;
        self.admission_ticket = Some(GrantedAdmission {
            receipt: *receipt,
            conservative_expiry,
        });
        Ok(())
    }

    fn fail_admission(&mut self) {
        self.admission = None;
        self.admission_ticket = None;
        self.admission_qualified = false;
        self.state = QueryContextState::TerminalRetained;
        self.released = true;
    }

    /// Settles one query-context acknowledgement.
    pub fn on_context_ack(
        &mut self,
        ack: &OperationAcknowledgement,
    ) -> Result<(), TaskExecutionError> {
        let released = match (&mut self.establish, &mut self.renewal) {
            (Some(establish), _)
                if establish.awaiting_outcome
                    && Self::request_id(&establish.request) == ack.operation_id() =>
            {
                establish.awaiting_outcome = false;
                establish.clone()
            }
            (_, Some(renewal))
                if renewal.awaiting_outcome
                    && Self::request_id(&renewal.request) == ack.operation_id() =>
            {
                renewal.awaiting_outcome = false;
                renewal.clone()
            }
            _ => return Err(TaskExecutionError::UnknownOperation),
        };
        let is_establish = matches!(released.request.as_ref(), UpdateQueryContext::Establish(_));

        if ack.is_applied() {
            let AckPayload::Context(receipt) = ack.payload() else {
                return Err(TaskExecutionError::MissingReceipt(
                    OperationKind::UpdateQueryContext,
                ));
            };
            self.context.verify_matches(receipt.context())?;
            let lease = receipt.lease().ok_or(TaskExecutionError::MissingReceipt(
                OperationKind::UpdateQueryContext,
            ))?;
            if lease.sequence() != released.sequence {
                return Err(TaskExecutionError::OperationFailed {
                    kind: OperationKind::UpdateQueryContext,
                    outcome: OperationOutcome::DomainConflict,
                    detail: ack.detail().map(|d| d.as_str().to_owned()),
                });
            }
            // The schedule comes from the frontend-local send time plus the
            // duration the backend made effective. The requested value never
            // takes part: a clamped lease is shorter than what was asked for,
            // and scheduling from the request would renew too late.
            self.lease_sequence = lease.sequence();
            self.renew_schedule = Some(RenewSchedule::after(
                released.first_sent_at,
                lease.effective_valid_for(),
            ));
            if is_establish {
                self.establish_acknowledged = true;
                self.establish = None;
                match classify_context_transition(self.state, QueryContextEvent::EstablishCompleted)
                {
                    ContextTransition::Apply(state) => self.state = state,
                    ContextTransition::Idempotent => {}
                    _ => {
                        return Err(TaskExecutionError::OperationFailed {
                            kind: OperationKind::UpdateQueryContext,
                            outcome: OperationOutcome::InvalidStateOrRequest,
                            detail: ack.detail().map(|d| d.as_str().to_owned()),
                        });
                    }
                }
                // A receipt may still report that the context closed while the
                // establish was in flight; the backend's answer wins.
                if context_state_is_closed(receipt.state()) {
                    self.state = receipt.state();
                    self.released = true;
                }
            } else {
                self.renewal = None;
            }
            return Ok(());
        }
        if matches!(
            frontend_action(ack.dispatch_result()),
            FrontendAction::RetryExactRequest
        ) {
            // The retained request keeps its first send time, so the identical
            // retry cannot inflate the lease it schedules from.
            return Ok(());
        }
        if is_establish {
            self.establish = None;
        } else {
            self.renewal = None;
        }
        self.state = QueryContextState::TerminalRetained;
        self.released = true;
        Err(TaskExecutionError::OperationFailed {
            kind: OperationKind::UpdateQueryContext,
            outcome: ack
                .worker_outcome()
                .expect("a non-transport acknowledgement has a Worker outcome"),
            detail: ack.detail().map(|d| d.as_str().to_owned()),
        })
    }

    /// Settles one release acknowledgement.
    pub fn on_release_ack(
        &mut self,
        ack: &OperationAcknowledgement,
        now: MonotonicInstant,
    ) -> Result<ReleaseSettlement, TaskExecutionError> {
        if self
            .release
            .is_none_or(|request| request.envelope().operation_id() != ack.operation_id())
        {
            return Err(TaskExecutionError::UnknownOperation);
        }
        self.release_in_flight = false;
        if ack.is_applied() {
            let (outcome, runtime_filter) = match ack.payload() {
                AckPayload::Release {
                    receipt,
                    outcome,
                    runtime_filter,
                } => {
                    self.context.verify_matches(receipt.context())?;
                    (*outcome, runtime_filter.clone())
                }
                _ => {
                    return Err(TaskExecutionError::MissingReceipt(
                        OperationKind::ReleaseQueryContext,
                    ));
                }
            };
            return Ok(match outcome {
                ReleaseOutcome::Released | ReleaseOutcome::AlreadyTerminal => {
                    self.state = QueryContextState::TerminalRetained;
                    self.released = true;
                    // Kept only on the answer that actually released the
                    // shared facts. A `NOT_READY` answer sealed nothing, and
                    // the identical request is resent.
                    self.runtime_filter_contribution = runtime_filter;
                    ReleaseSettlement::Released
                }
                ReleaseOutcome::NotReady => {
                    // Not applied and not first-wins, so the identical request
                    // is resent after local progress or a bounded backoff. A
                    // backend-local operation can finish without producing a
                    // new frontend progress fact, so progress alone cannot be
                    // the retry trigger. The lease keeps being renewed in the
                    // meantime.
                    self.defer_release_retry(now);
                    ReleaseSettlement::NotReadyKeepRenewing
                }
            });
        }
        let Some(outcome) = ack.worker_outcome() else {
            // The backend outcome is unknown, so preserve the existing
            // exact-request replay behavior without adding a NotReady delay
            // that was never observed.
            self.release_blocked_at = None;
            self.release_retry_at = None;
            return Ok(ReleaseSettlement::RetryExactRequest);
        };
        match outcome {
            OperationOutcome::ReleaseNotReady => {
                self.defer_release_retry(now);
                Ok(ReleaseSettlement::NotReadyKeepRenewing)
            }
            OperationOutcome::ContextTerminalReceipt | OperationOutcome::Gone => {
                self.state = QueryContextState::TerminalRetained;
                self.released = true;
                Ok(ReleaseSettlement::Released)
            }
            outcome => {
                self.state = QueryContextState::TerminalRetained;
                self.released = true;
                Ok(ReleaseSettlement::FailedClosed(outcome))
            }
        }
    }

    /// Settles one abort acknowledgement.
    pub fn on_abort_ack(
        &mut self,
        ack: &OperationAcknowledgement,
    ) -> Result<(), TaskExecutionError> {
        if self
            .abort
            .is_none_or(|request| request.envelope().operation_id() != ack.operation_id())
        {
            return Err(TaskExecutionError::UnknownOperation);
        }
        if ack.is_applied()
            || matches!(
                ack.worker_outcome(),
                Some(OperationOutcome::ContextTerminalReceipt | OperationOutcome::Gone)
            )
        {
            self.state = QueryContextState::TerminalRetained;
            self.released = true;
        }
        Ok(())
    }

    /// How long this owner may sleep before its next renewal is due.
    pub fn renew_delay(&self, now: MonotonicInstant) -> Option<Duration> {
        self.renew_schedule.map(|schedule| schedule.delay_from(now))
    }

    fn defer_release_retry(&mut self, now: MonotonicInstant) {
        self.release_blocked_at = Some(self.progress);
        self.release_retry_at = Some(now.saturating_add(self.release_retry_delay));
        self.release_retry_delay = self
            .release_retry_delay
            .saturating_mul(2)
            .min(RELEASE_RETRY_MAX);
    }

    fn request_id(request: &Arc<UpdateQueryContext>) -> TaskOperationId {
        request.envelope().operation_id()
    }
}
