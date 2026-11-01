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

use crate::task_execution::context_state::{
    ContextTransition, QueryContextEvent, classify_context_transition, context_state_is_closed,
};
use crate::task_execution::lease::{RenewSchedule, RequestedLeaseDurations};
use crate::task_execution::operation::{FrontendAction, OperationDispatchResult, frontend_action};
use novarocks_coordination_contract::MonotonicInstant;
use novarocks_execution::task_execution::{
    AcquireQueryContextAdmissionTicket, AdmissionEpochCapability, CodecOwnedContent,
    CredentialUpdate, EstablishQueryContext, LeaseSequence, LeaseValidFor, OperationKind,
    OperationOutcome, QueryContextAdmissionTicketReceipt, QueryContextRef, QueryContextState,
    QuiesceQueryContext, QuiesceQueryContextReceipt, ReleaseOutcome, ReleaseQueryContext,
    RenewQueryExecutionLease, TaskOperationId, UpdateQueryContext,
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
pub(crate) const ADMISSION_TICKET_VALID_FOR: Duration = Duration::from_secs(10);

/// The original finite horizon that may outlive a rejected Create.
///
/// Neither source grants Worker ownership or a new observation interval.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreateRefusalObservationHorizon {
    AppliedLease(MonotonicInstant),
    IssuedEstablish(MonotonicInstant),
}

impl CreateRefusalObservationHorizon {
    pub(crate) const fn expiry(self) -> MonotonicInstant {
        match self {
            Self::AppliedLease(expiry) | Self::IssuedEstablish(expiry) => expiry,
        }
    }

    pub(crate) const fn matches_parent_acknowledgement(self, acknowledged: bool) -> bool {
        matches!(
            (self, acknowledged),
            (Self::AppliedLease(_), true) | (Self::IssuedEstablish(_), false)
        )
    }
}

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
pub trait ContextEstablishSource: Send {
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

/// One exact Establish allocation retained for actor authorization, Native
/// transport, and replay after an unknown outcome.
#[derive(Clone, Debug)]
struct ReleasedEstablish {
    request: Arc<EstablishQueryContext>,
    first_sent_at: MonotonicInstant,
    /// Original admission receipt horizon, copied before its ticket is redeemed.
    /// This only bounds process-evidence observation after an unknown issue;
    /// it is neither a Worker stop fact nor replay or lease authorization.
    observation_expiry: Option<MonotonicInstant>,
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
    /// Present only when this Task owner captured the actual admission
    /// request's first send. An adopted ticket has no same-clock send fact.
    observation_expiry: Option<MonotonicInstant>,
    /// A frontend-local upper bound on the Worker's ticket expiry.
    ///
    /// The Worker issued the ticket no later than the receipt reached this
    /// owner. Waiting one full validity window from that observation therefore
    /// prevents a replacement request from racing a still-active old ticket.
    reissue_not_before: MonotonicInstant,
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
    admission_queue_preparation: super::intent::TaskOperationQueuePreparation,
    establish_queue_preparation: super::intent::TaskOperationQueuePreparation,
    renewal_queue_preparation: super::intent::TaskOperationQueuePreparation,
    quiesce_queue_preparation: super::intent::TaskOperationQueuePreparation,
    release_queue_preparation: super::intent::TaskOperationQueuePreparation,
    context: QueryContextRef,
    native_compatibility_id: NativeCompatibilityId,
    admission_epoch_capability: AdmissionEpochCapability,
    state: QueryContextState,
    admission: Option<ReleasedAdmission>,
    admission_remote_unknown: bool,
    admission_ticket: Option<GrantedAdmission>,
    admission_reissue_at: Option<MonotonicInstant>,
    admission_qualified: bool,
    /// Once terminal cleanup starts, this owner may settle requests already
    /// handed to transport, but it must not mint or replay normal lifecycle
    /// work. In particular, a remote cleanup wait must never keep extending a
    /// Worker lease after the FE has fixed the query outcome.
    terminal_cleanup_started: bool,
    lease_sequence: LeaseSequence,
    renew_schedule: Option<RenewSchedule>,
    establish: Option<ReleasedEstablish>,
    /// The original rejected Establish may only observe replacement until its
    /// already accepted ticket horizon. It grants no replay or Worker lease.
    rejected_establish_observation: Option<(TaskOperationId, MonotonicInstant)>,
    establish_send_started: bool,
    establish_acknowledged: bool,
    renewal: Option<ReleasedLease>,
    expected_creates: usize,
    owned_creates: usize,
    fenced_out_creates: usize,
    expected_tasks: usize,
    drained_tasks: usize,
    released_outputs: usize,
    quiesce: Option<QuiesceQueryContext>,
    quiesce_in_flight: bool,
    quiesce_receipt: Option<QuiesceQueryContextReceipt>,
    release: Option<ReleaseQueryContext>,
    release_in_flight: bool,
    release_blocked_at: Option<u64>,
    release_retry_at: Option<MonotonicInstant>,
    release_retry_delay: Duration,
    released: bool,
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
    pub(super) fn queue_preparation(
        &mut self,
        intent: &OperationIntent,
    ) -> &mut super::intent::TaskOperationQueuePreparation {
        match intent {
            OperationIntent::AcquireQueryContextAdmissionTicket(_) => {
                &mut self.admission_queue_preparation
            }
            OperationIntent::EstablishQueryContext(_) => &mut self.establish_queue_preparation,
            OperationIntent::UpdateQueryContext(_) => &mut self.renewal_queue_preparation,
            OperationIntent::QuiesceQueryContext(_) => &mut self.quiesce_queue_preparation,
            OperationIntent::ReleaseQueryContext(_) => &mut self.release_queue_preparation,
            _ => panic!("Task input cannot borrow a Context preparation"),
        }
    }

    /// Creates the owner of one context, over the frozen task set this
    /// attempt places on that backend.
    pub const fn new(
        context: QueryContextRef,
        tasks: usize,
        native_compatibility_id: NativeCompatibilityId,
        admission_epoch_capability: AdmissionEpochCapability,
    ) -> Self {
        Self {
            admission_queue_preparation: super::intent::TaskOperationQueuePreparation::new(),
            establish_queue_preparation: super::intent::TaskOperationQueuePreparation::new(),
            renewal_queue_preparation: super::intent::TaskOperationQueuePreparation::new(),
            quiesce_queue_preparation: super::intent::TaskOperationQueuePreparation::new(),
            release_queue_preparation: super::intent::TaskOperationQueuePreparation::new(),
            context,
            native_compatibility_id,
            admission_epoch_capability,
            state: QueryContextState::Absent,
            admission: None,
            admission_remote_unknown: false,
            admission_ticket: None,
            admission_reissue_at: None,
            admission_qualified: true,
            terminal_cleanup_started: false,
            lease_sequence: LeaseSequence::INITIAL,
            renew_schedule: None,
            establish: None,
            rejected_establish_observation: None,
            establish_send_started: false,
            establish_acknowledged: false,
            renewal: None,
            expected_creates: tasks,
            owned_creates: 0,
            fenced_out_creates: 0,
            expected_tasks: tasks,
            drained_tasks: 0,
            released_outputs: 0,
            quiesce: None,
            quiesce_in_flight: false,
            quiesce_receipt: None,
            release: None,
            release_in_flight: false,
            release_blocked_at: None,
            release_retry_at: None,
            release_retry_delay: RELEASE_RETRY_INITIAL,
            released: false,
            progress: 0,
            runtime_filter_contribution: None,
        }
    }

    /// Adopts the exact Worker grant already acquired by the logical
    /// replacement owner. The Task protocol must consume this receipt before
    /// its first turn so it cannot issue a second admission request for the
    /// same successor context.
    pub(crate) fn adopt_replacement_admission(
        &mut self,
        receipt: QueryContextAdmissionTicketReceipt,
        now: MonotonicInstant,
    ) -> Result<(), TaskExecutionError> {
        if receipt.context() != self.context
            || self.admission.is_some()
            || self.admission_ticket.is_some()
            || self.establish.is_some()
            || self.establish_acknowledged
            || !matches!(self.state, QueryContextState::Absent)
        {
            return Err(TaskExecutionError::Schedule(format!(
                "replacement admission does not belong to pristine context {}",
                self.context
            )));
        }
        self.admission_ticket = Some(GrantedAdmission {
            receipt,
            conservative_expiry: now.saturating_add(receipt.valid_for().get()),
            observation_expiry: None,
            reissue_not_before: now.saturating_add(receipt.valid_for().get()),
        });
        // The replacement remains authorized. Holding a ticket already makes
        // `needs_admission_ticket` false; preserving qualification lets an
        // unconsumed expired ticket be replaced instead of failing the query.
        self.admission_qualified = true;
        Ok(())
    }

    #[cfg(test)]
    pub const fn state(&self) -> QueryContextState {
        self.state
    }

    #[cfg(test)]
    pub const fn lease_sequence(&self) -> LeaseSequence {
        self.lease_sequence
    }

    #[cfg(test)]
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

    /// No Establish request has ever been minted for this exact context.
    /// An outstanding granted ticket remains the actor's Abort responsibility,
    /// so only a context without one may take this local closure branch.
    pub(crate) fn never_attempted_establish(&self) -> bool {
        matches!(self.state, QueryContextState::Absent)
            && self.establish.is_none()
            && !self.establish_acknowledged
            && self.admission_ticket.is_none()
    }

    /// Whether this owner must keep renewing.
    ///
    /// Renewal outlasts the client's end-of-stream: the read completes as soon
    /// as the root task's output is complete, while this context still holds
    /// resources until the release is answered.
    pub const fn must_keep_renewing(&self) -> bool {
        !self.terminal_cleanup_started
            && !self.released
            && !matches!(self.state, QueryContextState::Gone)
    }

    /// Whether this owner still has to establish its context.
    pub const fn needs_establish(&self) -> bool {
        !self.terminal_cleanup_started && !self.establish_acknowledged
    }

    pub const fn establish_acknowledged(&self) -> bool {
        self.establish_acknowledged
    }

    /// A first Create wave can follow only the exact Establish RPC start or
    /// its acknowledgement, whichever the serial owner observes first.
    pub const fn may_pipeline_first_create(&self) -> bool {
        self.establish_send_started || self.establish_acknowledged
    }

    pub fn on_establish_send_started(
        &mut self,
        operation_id: TaskOperationId,
    ) -> Result<(), TaskExecutionError> {
        if self.establish_acknowledged {
            return Ok(());
        }
        let Some(establish) = &self.establish else {
            return Err(TaskExecutionError::UnknownOperation);
        };
        if establish.request.envelope().operation_id() != operation_id {
            return Err(TaskExecutionError::UnknownOperation);
        }
        self.establish_send_started = true;
        Ok(())
    }

    /// Whether this context still needs a Worker capacity grant.
    pub const fn needs_admission_ticket(&self) -> bool {
        !self.terminal_cleanup_started
            && self.admission_qualified
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
        if self
            .admission_reissue_at
            .is_some_and(|reissue_at| !now.has_reached(reissue_at))
        {
            return Ok(None);
        }
        self.admission_reissue_at = None;
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
            return Ok(Some(OperationIntent::EstablishQueryContext(Arc::clone(
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
            self.defer_admission_reissue(granted.reissue_not_before);
            return Ok(None);
        }
        let valid_for = LeaseValidFor::new(RequestedLeaseDurations::DEFAULT.initial())
            .map_err(|error| TaskExecutionError::Schedule(error.to_string()))?;
        let request = Arc::new(EstablishQueryContext::new(
            TaskOperationId::new_v7(),
            self.context,
            granted.receipt.ticket_id(),
            facts.catalog_binding,
            facts.initial_runtime_filter,
            facts.query_options,
            facts.initial_credential,
            valid_for,
        ));
        self.state = QueryContextState::Establishing;
        self.admission_ticket = None;
        self.admission_reissue_at = None;
        self.admission_qualified = false;
        self.establish = Some(ReleasedEstablish {
            request: Arc::clone(&request),
            first_sent_at: now,
            observation_expiry: granted.observation_expiry,
            awaiting_outcome: true,
        });
        Ok(Some(OperationIntent::EstablishQueryContext(request)))
    }

    /// Remaining original ticket horizon on this owner's own clock. A replay
    /// never renews it; consuming this observation bound grants no operation.
    pub(crate) fn pre_ready_observation_remaining(
        &self,
        now: MonotonicInstant,
    ) -> Option<Duration> {
        if self.establish_acknowledged {
            return None;
        }
        self.establish
            .as_ref()
            .and_then(|released| released.observation_expiry)
            .map(|expiry| expiry.saturating_duration_since(now))
    }

    pub(crate) fn rejected_establish_observation_remaining(
        &self,
        operation_id: TaskOperationId,
        now: MonotonicInstant,
    ) -> Option<Duration> {
        let (original_operation, expiry) = self.rejected_establish_observation?;
        (original_operation == operation_id && !self.establish_acknowledged)
            .then(|| expiry.saturating_duration_since(now))
    }

    /// Original Establish ticket or effective Worker lease at a Create refusal.
    /// A pipelined Create does not imply that the Establish ACK arrived. Keep
    /// those sources distinct, and snapshot once before any later ACK/renewal.
    pub(crate) fn create_refusal_observation_horizon(
        &self,
    ) -> Option<CreateRefusalObservationHorizon> {
        if self.terminal_cleanup_started || self.released || self.quiesce_receipt.is_some() {
            return None;
        }
        if self.establish_acknowledged {
            return self.renew_schedule.map(|schedule| {
                CreateRefusalObservationHorizon::AppliedLease(schedule.conservative_expiry())
            });
        }
        if !self.establish_send_started {
            return None;
        }
        self.establish
            .as_ref()?
            .observation_expiry
            .map(CreateRefusalObservationHorizon::IssuedEstablish)
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

    /// Installs one immutable normal fence after the attempt stops producing
    /// work. A transport-unknown answer reissues this exact operation.
    pub fn quiesce_intent(&mut self) -> Option<OperationIntent> {
        if self.terminal_cleanup_started
            || self.released
            || self.quiesce_receipt.is_some()
            || self.quiesce_in_flight
        {
            return None;
        }
        let request = *self.quiesce.get_or_insert_with(|| {
            QuiesceQueryContext::new(TaskOperationId::new_v7(), self.context)
        });
        self.quiesce_in_flight = true;
        Some(OperationIntent::QuiesceQueryContext(request))
    }

    /// Settles the exact normal fence, retaining its complete cumulative
    /// membership until Release. No task is inferred absent from silence.
    pub fn on_quiesce_ack(
        &mut self,
        ack: &OperationAcknowledgement,
    ) -> Result<Option<QuiesceQueryContextReceipt>, TaskExecutionError> {
        if !self.quiesce_in_flight
            || self
                .quiesce
                .is_none_or(|request| request.envelope().operation_id() != ack.operation_id())
        {
            return Err(TaskExecutionError::UnknownOperation);
        }
        self.quiesce_in_flight = false;
        if ack.is_applied() {
            let AckPayload::Quiesce(receipt) = ack.payload() else {
                return Err(TaskExecutionError::MissingReceipt(
                    OperationKind::QuiesceQueryContext,
                ));
            };
            self.context.verify_matches(receipt.context())?;
            if let Some(previous) = &self.quiesce_receipt
                && (previous.fence_version() != receipt.fence_version()
                    || previous.accepted_tasks() != receipt.accepted_tasks())
            {
                return Err(TaskExecutionError::DomainReceipt(
                    "a replayed Quiesce changed its cumulative accepted set".to_owned(),
                ));
            }
            self.state = receipt.state();
            self.quiesce_receipt = Some(receipt.clone());
            return Ok(Some(receipt.clone()));
        }
        if let Some(receipt) = &self.quiesce_receipt {
            // A positive observation of this exact Worker fence is stronger
            // than a delayed negative result from an earlier send attempt.
            return Ok(Some(receipt.clone()));
        }
        if ack.dispatch_result() == OperationDispatchResult::TransportUnknown {
            return Ok(None);
        }
        if let Some(_outcome) = ack.worker_outcome() {
            return Err(TaskExecutionError::OperationFailed {
                context: self.context,
                operation_id: ack.operation_id(),
                kind: OperationKind::QuiesceQueryContext,
                rejection: ack
                    .worker_header()
                    .expect("a checked Worker verdict has its original branch"),
            });
        }
        Err(crate::task_execution::error::dispatch_rejection(
            ack,
            OperationKind::QuiesceQueryContext,
        ))
    }

    #[cfg(test)]
    pub const fn quiesce_acknowledged(&self) -> bool {
        self.quiesce_receipt.is_some()
    }

    /// Accepts the same complete Worker fence through the ordered observation
    /// stream when the operation acknowledgement was lost. It does not retire
    /// the transport's exact operation; a late acknowledgement still settles
    /// that reservation against this immutable receipt.
    pub(crate) fn observe_quiesce(
        &mut self,
        receipt: &QuiesceQueryContextReceipt,
    ) -> Result<(), TaskExecutionError> {
        self.context.verify_matches(receipt.context())?;
        if self.quiesce.is_none() {
            return Err(TaskExecutionError::DomainReceipt(
                "observed a Quiesce fence before this Context issued one".to_owned(),
            ));
        }
        if let Some(previous) = &self.quiesce_receipt
            && (previous.fence_version() != receipt.fence_version()
                || previous.accepted_tasks() != receipt.accepted_tasks())
        {
            return Err(TaskExecutionError::DomainReceipt(
                "observed Quiesce conflicts with the settled fence".to_owned(),
            ));
        }
        self.state = receipt.state();
        self.quiesce_receipt = Some(receipt.clone());
        Ok(())
    }

    /// The release request, once every local obligation has closed.
    pub fn release_intent(&mut self, now: MonotonicInstant) -> Option<OperationIntent> {
        if self.released || self.release_in_flight {
            return None;
        }
        if !matches!(self.state, QueryContextState::Quiescing) || self.quiesce_receipt.is_none() {
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

    /// Closes future normal lifecycle issuance while retaining any immutable
    /// request that already crossed transport. A late Worker settlement may
    /// still apply that request once, but the FE never uses the settlement to
    /// mint a successor admission, establish, or renewal.
    pub(crate) fn begin_terminal_cleanup(&mut self) {
        self.terminal_cleanup_started = true;
        self.admission_qualified = false;
        self.admission_reissue_at = None;
    }

    /// Rolls back a lifecycle request that never crossed process queue
    /// admission.
    ///
    /// Replayable requests keep their immutable payload and first-send time;
    /// only the in-flight marker is released.
    pub(crate) fn rollback_unsent(&mut self, operation_id: TaskOperationId) {
        if let Some(released) = &mut self.admission
            && released.request.envelope().operation_id() == operation_id
        {
            released.awaiting_outcome = false;
            return;
        }
        if let Some(released) = &mut self.establish
            && released.request.envelope().operation_id() == operation_id
        {
            released.awaiting_outcome = false;
            return;
        }
        if let Some(released) = &mut self.renewal
            && Self::request_id(&released.request) == operation_id
        {
            released.awaiting_outcome = false;
            return;
        }
        if self
            .quiesce
            .is_some_and(|request| request.envelope().operation_id() == operation_id)
        {
            self.quiesce_in_flight = false;
            return;
        }
        if self
            .release
            .is_some_and(|request| request.envelope().operation_id() == operation_id)
        {
            self.release_in_flight = false;
        }
    }

    /// Whether no legal create can follow.
    pub const fn creates_closed(&self) -> bool {
        self.owned_creates.saturating_add(self.fenced_out_creates) >= self.expected_creates
    }

    /// Whether every local task and output responsibility has drained.
    ///
    /// This is per-context on purpose. The original Release author uses these
    /// Task/output observations; physical convergence still needs the Worker's
    /// actual-exit receipt or exact process replacement.
    pub const fn locally_drained(&self) -> bool {
        self.drained_tasks >= self.expected_tasks && self.released_outputs >= self.expected_tasks
    }

    pub fn note_create_owned(&mut self) {
        self.owned_creates += 1;
        self.advance();
    }

    pub fn note_create_fenced_out(&mut self) {
        self.fenced_out_creates += 1;
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
            self.admission_remote_unknown = true;
            return Ok(());
        }
        if !ack.is_applied() {
            self.fail_admission();
            let Some(_outcome) = ack.worker_outcome() else {
                return Err(crate::task_execution::error::dispatch_rejection(
                    ack,
                    OperationKind::AcquireQueryContextAdmissionTicket,
                ));
            };
            return Err(TaskExecutionError::OperationFailed {
                context: self.context,
                operation_id: ack.operation_id(),
                kind: OperationKind::AcquireQueryContextAdmissionTicket,
                rejection: ack
                    .worker_header()
                    .expect("a checked Worker verdict has its original branch"),
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
                context: self.context,
                operation_id: ack.operation_id(),
                kind: OperationKind::AcquireQueryContextAdmissionTicket,
                rejection: novarocks_execution_contract::operation::OperationResult::Verdict {
                    outcome: OperationOutcome::DomainConflict,
                    detail: Some(novarocks_execution_contract::SafeDetail::truncating(
                        "the admission ticket validity differs from the exact request",
                    )),
                    acknowledgement: None,
                },
            });
        }
        let conservative_expiry = released
            .first_sent_at
            .saturating_add(receipt.valid_for().get());
        if now.has_reached(conservative_expiry) {
            self.defer_admission_reissue(now.saturating_add(receipt.valid_for().get()));
            return Ok(());
        }
        self.admission = None;
        self.admission_reissue_at = None;
        self.admission_ticket = Some(GrantedAdmission {
            receipt: *receipt,
            conservative_expiry,
            observation_expiry: Some(conservative_expiry),
            reissue_not_before: now.saturating_add(receipt.valid_for().get()),
        });
        Ok(())
    }

    fn defer_admission_reissue(&mut self, reissue_not_before: MonotonicInstant) {
        self.admission = None;
        self.admission_ticket = None;
        self.admission_reissue_at = Some(reissue_not_before);
    }

    fn fail_admission(&mut self) {
        self.admission = None;
        self.admission_ticket = None;
        self.admission_reissue_at = None;
        self.admission_qualified = false;
        if !self.admission_remote_unknown {
            self.state = QueryContextState::TerminalRetained;
            self.released = true;
        }
    }

    /// Settles one query-context acknowledgement.
    pub fn on_context_ack(
        &mut self,
        ack: &OperationAcknowledgement,
    ) -> Result<(), TaskExecutionError> {
        let (released_sequence, first_sent_at, is_establish) =
            match (&mut self.establish, &mut self.renewal) {
                (Some(establish), _)
                    if establish.awaiting_outcome
                        && establish.request.envelope().operation_id() == ack.operation_id() =>
                {
                    establish.awaiting_outcome = false;
                    (LeaseSequence::INITIAL, establish.first_sent_at, true)
                }
                (_, Some(renewal))
                    if renewal.awaiting_outcome
                        && Self::request_id(&renewal.request) == ack.operation_id() =>
                {
                    renewal.awaiting_outcome = false;
                    (renewal.sequence, renewal.first_sent_at, false)
                }
                _ => return Err(TaskExecutionError::UnknownOperation),
            };

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
            if lease.sequence() != released_sequence {
                return Err(TaskExecutionError::OperationFailed {
                    context: self.context,
                    operation_id: ack.operation_id(),
                    kind: OperationKind::UpdateQueryContext,
                    rejection: novarocks_execution_contract::operation::OperationResult::Verdict {
                        outcome: OperationOutcome::DomainConflict,
                        detail: ack.detail().cloned(),
                        acknowledgement: None,
                    },
                });
            }
            // The schedule comes from the frontend-local send time plus the
            // duration the backend made effective. The requested value never
            // takes part: a clamped lease is shorter than what was asked for,
            // and scheduling from the request would renew too late.
            self.lease_sequence = lease.sequence();
            self.renew_schedule = Some(RenewSchedule::after(
                first_sent_at,
                lease.effective_valid_for(),
            ));
            if is_establish {
                self.establish_acknowledged = true;
                self.establish = None;
                // An older Establish answer may arrive after Quiesce. It
                // proves historical lease installation but cannot reopen the
                // admission fence or change its accepted membership.
                if self.quiesce_receipt.is_none() {
                    match classify_context_transition(
                        self.state,
                        QueryContextEvent::EstablishCompleted,
                    ) {
                        ContextTransition::Apply(state) => self.state = state,
                        ContextTransition::Idempotent => {}
                        _ => {
                            return Err(TaskExecutionError::OperationFailed{context:self.context,operation_id:ack.operation_id(),kind:OperationKind::UpdateQueryContext,rejection:novarocks_execution_contract::operation::OperationResult::Verdict{outcome:OperationOutcome::InvalidStateOrRequest,detail:ack.detail().cloned(),acknowledgement:None}});
                        }
                    }
                }
                // A receipt may still report that the context closed while the
                // establish was in flight; the backend's answer wins.
                if self.quiesce_receipt.is_none() && context_state_is_closed(receipt.state()) {
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
            // Preserve this exact request's observation horizon before retiring
            // the dispatched request. A local refusal or a foreign ACK cannot
            // create this candidate; neither can another Worker verdict.
            if self.quiesce_receipt.is_none()
                && matches!(
                    ack.worker_header(),
                    Some(
                        novarocks_execution_contract::operation::OperationResult::Verdict {
                            outcome: OperationOutcome::IdentityMismatch,
                            ..
                        }
                    )
                )
            {
                self.rejected_establish_observation = self
                    .establish
                    .as_ref()
                    .and_then(|released| released.observation_expiry)
                    .map(|expiry| (ack.operation_id(), expiry));
            }
            self.establish = None;
            if self.quiesce_receipt.is_some() {
                // The normal fence has already settled whether this Context
                // owns any Task; an older negative Establish cannot undo it.
                return Ok(());
            }
        } else {
            self.renewal = None;
        }
        let Some(_outcome) = ack.worker_outcome() else {
            // The admission grant may still occupy Worker capacity. Keep the
            // context open so failure cleanup issues its exact Abort.
            return Err(crate::task_execution::error::dispatch_rejection(
                ack,
                OperationKind::UpdateQueryContext,
            ));
        };
        self.state = QueryContextState::TerminalRetained;
        self.released = true;
        if is_establish {
            return Err(TaskExecutionError::PreReadyEstablishRejected {
                context: self.context,
                operation_id: ack.operation_id(),
                rejection: ack
                    .worker_header()
                    .expect("the original Worker verdict was checked"),
            });
        }
        Err(TaskExecutionError::OperationFailed {
            context: self.context,
            operation_id: ack.operation_id(),
            kind: OperationKind::UpdateQueryContext,
            rejection: ack
                .worker_header()
                .expect("a checked Worker verdict has its original branch"),
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
            self.release_blocked_at = None;
            self.release_retry_at = None;
            return if ack.dispatch_result() == OperationDispatchResult::TransportUnknown {
                Ok(ReleaseSettlement::RetryExactRequest)
            } else {
                Err(crate::task_execution::error::dispatch_rejection(
                    ack,
                    OperationKind::ReleaseQueryContext,
                ))
            };
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

    /// Records the definitive closure receipt for an actor-owned Abort.
    ///
    /// Actor Abort operations are settled by their own application authority,
    /// rather than by this context owner's normal lifecycle request path. A
    /// validated Worker receipt is nevertheless the same positive context
    /// closure fact: retaining this owner afterwards would make attempt
    /// convergence wait for a Release that the terminal context no longer
    /// needs or accepts.
    pub(crate) fn observe_actor_abort_closure(&mut self) {
        self.state = QueryContextState::TerminalRetained;
        self.released = true;
    }

    /// Delays another release attempt while local work remains unsettled.
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
