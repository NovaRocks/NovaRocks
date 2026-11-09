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

//! The frontend's owner of one remote task, with exactly three states.
//!
//! `Creating` accepts domain facts but sends none of them. `Created` is
//! reached by an exact Accepted acknowledgement or a stronger Installed
//! observation; accumulated facts drain only after Installed. `Terminal`
//! discards what was never sent and lets what is already in flight converge
//! on its own retained receipt. An update rejected because the backend task
//! already terminated stops further sends while the owner waits for the
//! terminal status; the rejection itself is not a second terminal authority.
//!
//! The create payload has its own lifecycle, separate from the task's. Until
//! the create is first admitted to a send queue it is a move-only seed whose
//! encoded size is learned at most once; admission freezes it exactly once;
//! every resend reuses those frozen parts; an exact Accepted acknowledgement
//! or Installed observation releases them for good. Status and control never
//! decode the create payload.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use crate::domain_tracker::{
    TaskDomainIntentTracker, TaskDomainReceiptExpectation, verify_task_domain_receipt,
};
use crate::operation::{FrontendAction, OperationDispatchResult, frontend_action};
use crate::status_validation::{GoneObservation, classify_gone};
use novarocks_coordination_contract::{
    MonotonicInstant, ObservedTaskTransition, StatusObservation, classify_observation,
    classify_observed_task_transition,
};
use novarocks_execution_contract::{
    CancelReason, CancelTask, DomainConflict, DomainProgression, ExchangeEdgeId, OperationEnvelope,
    OperationKind, OperationOutcome, ParticipationRef, TaskIdentity, TaskOperationId, TaskState,
    TaskStatus, TaskStatusCursor, TerminationDetail,
};

use super::error::{CapacityBound, TaskExecutionError};
use super::participation_owner::CreateRefusalObservationHorizon;
use crate::effect::{OperationIntent, TaskDomainIntent as TaskDomainUpdate};
use crate::ids::OperationIds;
use crate::input::{
    AckPayload, OperationReply as OperationAcknowledgement, QueuedInputGrantRef, UpdateAdmission,
};
use crate::intent::{TaskOperationQueueRequest, TaskUpdateRequest as UpdateTask};
use crate::start::{FrozenCreation, PayloadRef};

/// The three states of one remote task.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RemoteTaskState {
    /// No positive Worker ownership fact has arrived. Domain facts accumulate locally.
    Creating,
    /// Accepted or Installed proved ownership. Domain facts drain after Installed.
    Created,
    /// This task reached a terminal outcome, or an operation for it failed
    /// closed.
    Terminal,
}

/// What a create acknowledgement did to this task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateSettlement {
    /// Worker accepted this task; its installation may still be pending.
    Created,
    /// Normal closure retains identity cleanup but never replays this body.
    Closing,
    /// The outcome was genuinely unknown; the identical request must be resent.
    RetryExactRequest,
    /// Worker did not take ownership; the exact body remains available after
    /// the named prerequisite advances.
    RetryAfterProgress(OperationOutcome),
    /// The operation failed closed and this task is terminal.
    FailedClosed(OperationOutcome),
}

/// What an update acknowledgement did to this task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UpdateSettlement {
    /// The domain advanced, or the identical request was already applied.
    Applied,
    /// The outcome was genuinely unknown; the identical request must be resent.
    RetryExactRequest,
    /// An ordinary input request became obsolete after accurate local normal
    /// stand-down; its unknown result cannot block a later exact close.
    DiscardedAfterNormalStandDown,
    /// The task went terminal while this request was in flight, so its
    /// outcome no longer changes anything.
    ConvergedAfterTerminal,
    /// The backend says the task is terminal, but the frontend has not yet
    /// observed the authoritative terminal status.
    AwaitingTerminalStatus,
    /// The operation failed closed and this task is terminal.
    FailedClosed(OperationOutcome),
}

/// The terminal facts this task's owner needs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskTerminalReport {
    pub identity: TaskIdentity,
    pub state: TaskState,
    pub termination: Option<TerminationDetail>,
    /// Domain facts that were queued but never sent.
    pub discarded_updates: usize,
}

/// One update this owner has released and must be able to replay verbatim.
#[derive(Clone, Debug)]
struct ReleasedUpdate {
    operation_id: TaskOperationId,
    request: Arc<UpdateTask>,
    expected_receipt: TaskDomainReceiptExpectation,
    /// Whether this request is released and its outcome not yet known.
    ///
    /// A retained request is handed out again only after a genuinely unknown
    /// outcome. While it is merely in flight it must not be queued a second
    /// time: two copies of one immutable request would each expect the
    /// receipt the other consumed.
    awaiting_outcome: bool,
}

#[derive(Debug)]
struct PendingUpdateInput {
    operation: TaskOperationId,
    update: TaskDomainUpdate,
    discarded: Option<UpdateAdmission>,
}

#[derive(Debug)]
struct PendingUpdate {
    operation: TaskOperationId,
    ordinal: u64,
    update: TaskDomainUpdate,
    expected_receipt: TaskDomainReceiptExpectation,
    queue_permit: QueuedInputGrantRef,
}

fn is_destination_close(update: &TaskDomainUpdate) -> bool {
    matches!(update, TaskDomainUpdate::CloseExchangeDestination { .. })
}

#[derive(Debug)]
enum CreationReplay {
    Unfrozen(FrozenCreation),
    Frozen(FrozenCreation),
    Settled,
    Closed,
}

/// One create's admission price, before the create is allowed to exist.
#[derive(Copy, Clone, Debug)]
pub(crate) struct CreateCandidate {
    operation_id: TaskOperationId,
    request: TaskOperationQueueRequest,
    plan_carrier_bytes: usize,
}

impl CreateCandidate {
    pub(crate) const fn operation_id(self) -> TaskOperationId {
        self.operation_id
    }

    pub(crate) const fn request(self) -> TaskOperationQueueRequest {
        self.request
    }

    /// The static plan and task assignment bytes the backend bounds together.
    pub(crate) const fn plan_carrier_bytes(self) -> usize {
        self.plan_carrier_bytes
    }
}

/// What the next update send would cost, read without taking it.
#[derive(Copy, Clone, Debug)]
pub(crate) struct UpdateCandidate {
    request: TaskOperationQueueRequest,
    holds_queue_permit: bool,
}

impl UpdateCandidate {
    pub(crate) const fn request(self) -> TaskOperationQueueRequest {
        self.request
    }

    /// Whether the update already carries the process reservation it was
    /// admitted with, so taking it must not reserve a second one.
    pub(crate) const fn holds_queue_permit(self) -> bool {
        self.holds_queue_permit
    }
}

/// The frontend's owner of one remote task.
#[derive(Debug)]
pub struct RemoteTask {
    pending_input: Option<PendingUpdateInput>,
    released_payloads: Vec<PayloadRef>,
    released_grants: Vec<QueuedInputGrantRef>,
    withdrawn_preparations: Vec<TaskOperationId>,
    identity: TaskIdentity,
    participation: ParticipationRef,
    create_envelope: OperationEnvelope,
    creation: CreationReplay,
    state: RemoteTaskState,
    create_in_flight: Option<TaskOperationId>,
    create_remote_unknown: bool,
    // The exact rejected Create and the already-effective participation lease cut.
    // This is immutable observation evidence, never authority for new work.
    rejected_create_observation: Option<(TaskOperationId, MonotonicInstant)>,
    create_ownership_proven: bool,
    normal_stand_down: bool,
    fenced_out: bool,
    create_waiting_for_establish: bool,
    create_busy_until: Option<MonotonicInstant>,
    create_busy_rejections: u32,
    pending_ordinary: VecDeque<PendingUpdate>,
    pending_control: VecDeque<PendingUpdate>,
    next_pending_ordinal: u64,
    /// Logically discarded in constant time. Each original grant is returned
    /// separately by charged cleanup, without delaying the accepted terminal.
    discarded_pending: VecDeque<VecDeque<PendingUpdate>>,
    released_update: Option<ReleasedUpdate>,
    cancel_in_flight: Option<TaskOperationId>,
    cancel_requested: bool,
    status: Option<TaskStatus>,
    cursor: TaskStatusCursor,
    domains: TaskDomainIntentTracker,
    awaiting_terminal_status: bool,
    discarded_updates: usize,
    #[cfg(test)]
    converged_after_terminal: usize,
}

/// An edge-open refusal that names the version and edge set it refused.
fn edge_regression(
    version: novarocks_execution_contract::EdgeOpenVersion,
    edges: &[novarocks_execution_contract::ExchangeEdgeId],
    conflict: DomainConflict,
) -> TaskExecutionError {
    TaskExecutionError::DomainRegression {
        domain: "open_exchange_edges",
        token: format!("version={} edges={edges:?}", version.get()),
        conflict,
    }
}

/// A split-domain refusal that names the offer it refused.
fn split_regression(
    node: novarocks_execution_contract::PlanNodeId,
    offer: novarocks_execution_contract::SplitOffer,
    watermark: crate::domain_tracker::SentSplitWatermark,
    conflict: DomainConflict,
) -> TaskExecutionError {
    TaskExecutionError::DomainRegression {
        domain: "split_assignment",
        // Both halves. A conflict is a relation between the offer and the
        // state it lost against, and reporting only the offer leaves the
        // reader to obtain the other half from a cluster run.
        token: format!(
            "plan_node={} {} against accepted_through={:?} sealed={}",
            node,
            offer,
            watermark.accepted_through().map(|s| s.get()),
            watermark.no_more_splits()
        ),
        conflict,
    }
}

pub(crate) type NextTaskUpdateIntent = Option<(OperationIntent, Option<QueuedInputGrantRef>)>;

impl RemoteTask {
    #[cfg(test)]
    fn record_terminal_convergence(&mut self) {
        self.converged_after_terminal += 1;
    }

    #[cfg(not(test))]
    fn record_terminal_convergence(&mut self) {}

    /// Takes ownership of one task's creation seed.
    ///
    /// The create operation id is minted here, once: every send of this
    /// create carries it, so the acknowledgement of any send correlates.
    pub(crate) fn new(
        seed: &crate::graph::TaskNode,
        creation: FrozenCreation,
        outbound_edges: Vec<ExchangeEdgeId>,
        ids: &mut OperationIds,
        outbound_destinations: Vec<(ExchangeEdgeId, TaskIdentity)>,
    ) -> Result<Self, TaskExecutionError> {
        let identity = seed.identity();
        let participation = identity
            .verify_participation(seed.participation())
            .map(|()| seed.participation())
            .map_err(TaskExecutionError::Identity)?;
        let domains =
            TaskDomainIntentTracker::new(seed.split_plan_nodes().iter().copied(), outbound_edges)
                .with_exchange_destinations(outbound_destinations);
        Ok(Self {
            pending_input: None,
            released_payloads: Vec::new(),
            released_grants: Vec::new(),
            withdrawn_preparations: Vec::new(),
            identity,
            participation,
            create_envelope: OperationEnvelope::with_default_wait(
                ids.issue(),
                OperationKind::CreateTask,
            ),
            creation: CreationReplay::Unfrozen(creation),
            state: RemoteTaskState::Creating,
            create_in_flight: None,
            create_remote_unknown: false,
            rejected_create_observation: None,
            create_ownership_proven: false,
            normal_stand_down: false,
            fenced_out: false,
            create_waiting_for_establish: false,
            create_busy_until: None,
            create_busy_rejections: 0,
            pending_ordinary: VecDeque::new(),
            pending_control: VecDeque::new(),
            next_pending_ordinal: 0,
            discarded_pending: VecDeque::new(),
            released_update: None,
            cancel_in_flight: None,
            cancel_requested: false,
            status: None,
            cursor: TaskStatusCursor::unobserved(identity),
            domains,
            awaiting_terminal_status: false,
            discarded_updates: 0,
            #[cfg(test)]
            converged_after_terminal: 0,
        })
    }

    pub fn identity(&self) -> TaskIdentity {
        self.identity
    }

    /// Where this task's status observation has reached.
    ///
    /// A resubscription replays from here rather than from nothing, so a
    /// transport that dropped does not re-deliver every version this task ever
    /// published.
    pub const fn cursor(&self) -> TaskStatusCursor {
        self.cursor
    }

    pub fn participation(&self) -> ParticipationRef {
        self.participation
    }

    /// Whether this task still holds a create payload: a seed it has not
    /// frozen, or frozen parts a resend may still need.
    #[cfg(test)]
    pub(crate) const fn holds_create_payload(&self) -> bool {
        matches!(
            self.creation,
            CreationReplay::Unfrozen(_) | CreationReplay::Frozen(_)
        )
    }

    /// Whether this task's create was frozen, settled or closed, i.e. has
    /// left its seed.
    #[cfg(test)]
    pub(crate) const fn create_frozen(&self) -> bool {
        !matches!(self.creation, CreationReplay::Unfrozen(_))
    }

    pub(crate) const fn create_operation(&self) -> TaskOperationId {
        self.create_envelope.operation_id()
    }

    pub(crate) const fn create_result_is_unresolved(&self) -> bool {
        self.create_remote_unknown && !self.create_ownership_proven
    }

    pub const fn state(&self) -> RemoteTaskState {
        self.state
    }

    /// An exact positive Worker fact proves this Task exists even if its
    /// Create acknowledgement was lost or is still in flight.
    pub const fn create_ownership_proven(&self) -> bool {
        self.create_ownership_proven
    }

    pub(crate) fn rejected_create_observation_remaining(
        &self,
        operation_id: TaskOperationId,
        now: MonotonicInstant,
    ) -> Option<Duration> {
        let (original, expiry) = self.rejected_create_observation?;
        (original == operation_id).then(|| expiry.saturating_duration_since(now))
    }

    /// A deployment position survives the Accepted RPC and unknown outcomes.
    /// Runtime installation, terminal evidence or permanent stand-down returns it.
    pub(crate) fn needs_deployment_window(&self) -> bool {
        !self.normal_stand_down
            && !self.fenced_out
            && !self.installed()
            && !self.is_terminal()
            && (self.create_in_flight.is_some()
                || self.create_remote_unknown
                || self.create_ownership_proven)
    }

    pub const fn fenced_out(&self) -> bool {
        self.fenced_out
    }

    pub(crate) const fn normally_stood_down(&self) -> bool {
        self.normal_stand_down
    }
    /// This owner has exactly one original Create identity and at most one
    /// released Update. Normal withdrawal inspects these fixed positions,
    /// never searches every other Task's dispatcher entries.
    pub(crate) fn withdrawal_operation_ids(&self) -> [Option<TaskOperationId>; 2] {
        [
            Some(self.create_envelope.operation_id()),
            self.released_update
                .as_ref()
                .map(|update| update.operation_id),
        ]
    }

    /// Stops new Create and update work while preserving a possible in-flight
    /// Create receipt and the identity that the Participation fence must classify.
    pub(crate) fn begin_normal_stand_down(&mut self) {
        self.normal_stand_down = true;
        self.create_waiting_for_establish = false;
        self.create_busy_until = None;
        self.discard_pending_input(false);
        self.discard_pending(false);
        if self.released_update.as_ref().is_some_and(|released| {
            !released.awaiting_outcome
                && !released.request.domains().iter().all(is_destination_close)
        }) {
            self.released_update = None;
        }
        self.close_creation();
    }

    /// Returns frontend replay backing after the attempt has failed. A sent
    /// transport retains its own carrier until that physical operation exits.
    pub(crate) fn discard_creation_after_attempt_failure(&mut self) {
        self.close_creation();
        self.create_waiting_for_establish = false;
        self.create_busy_until = None;
        self.discard_pending(true);
        self.discard_pending_input(true);
        self.withdraw_unsent_preparations();
    }

    fn pending_front(&self) -> Option<&PendingUpdate> {
        match (self.pending_ordinary.front(), self.pending_control.front()) {
            (Some(ordinary), Some(control)) => Some(if ordinary.ordinal < control.ordinal {
                ordinary
            } else {
                control
            }),
            (Some(ordinary), None) => Some(ordinary),
            (None, control) => control,
        }
    }

    fn pop_pending(&mut self) -> Option<PendingUpdate> {
        let control_first = match (self.pending_ordinary.front(), self.pending_control.front()) {
            (Some(ordinary), Some(control)) => control.ordinal < ordinary.ordinal,
            (None, Some(_)) => true,
            _ => false,
        };
        if control_first {
            self.pending_control.pop_front()
        } else {
            self.pending_ordinary.pop_front()
        }
    }

    fn discard_pending(&mut self, include_destination_close: bool) {
        if !self.pending_ordinary.is_empty() {
            self.discarded_updates += self.pending_ordinary.len();
            self.discarded_pending
                .push_back(std::mem::take(&mut self.pending_ordinary));
        }
        if include_destination_close && !self.pending_control.is_empty() {
            self.discarded_updates += self.pending_control.len();
            self.discarded_pending
                .push_back(std::mem::take(&mut self.pending_control));
        }
    }

    fn discard_pending_input(&mut self, include_destination_close: bool) {
        if self
            .pending_input
            .as_ref()
            .is_some_and(|input| include_destination_close || !is_destination_close(&input.update))
        {
            let disposition = self
                .pending_input
                .as_ref()
                .and_then(|input| self.update_disposition(&input.update))
                .unwrap_or(UpdateAdmission::DiscardedTerminal);
            let input = self.pending_input.as_mut().expect("checked pending input");
            if input.discarded.is_none() {
                self.discarded_updates += 1;
                input.discarded = Some(disposition);
                self.withdrawn_preparations.push(input.operation);
            }
        }
    }

    fn withdraw_unsent_preparations(&mut self) {
        self.discard_pending_input(true);
    }

    /// Applies the Worker's complete cumulative fence membership.
    pub(crate) fn on_quiesce_membership(
        &mut self,
        accepted: bool,
    ) -> Result<bool, TaskExecutionError> {
        self.begin_normal_stand_down();
        if accepted {
            if self.fenced_out {
                return Err(TaskExecutionError::DomainReceipt(format!(
                    "quiesce membership reclaims fenced-out task {}",
                    self.identity
                )));
            }
            let became_owned = !self.create_ownership_proven;
            self.create_ownership_proven = true;
            if self
                .status
                .as_ref()
                .is_none_or(|status| !status.is_terminal())
            {
                self.state = RemoteTaskState::Created;
            }
            return Ok(became_owned);
        }
        if self.create_ownership_proven {
            return Err(TaskExecutionError::DomainReceipt(format!(
                "quiesce omitted previously accepted task {}",
                self.identity
            )));
        }
        self.fenced_out = true;
        self.discard_pending(true);
        self.enter_terminal();
        Ok(false)
    }

    /// The Worker has published the installation fact for this task. An
    /// Accepted Create receipt alone does not permit domain updates or input.
    pub fn installed(&self) -> bool {
        self.status.as_ref().is_some_and(TaskStatus::installed)
    }

    /// This task's last observed lifecycle state.
    ///
    /// A task whose status has not been published yet is `PLANNED`: that is
    /// what the create acknowledgement's own first snapshot carries, and it is
    /// the only state a stage may assume for a task it has not observed.
    pub fn task_state(&self) -> TaskState {
        if self.fenced_out {
            return TaskState::Canceled;
        }
        self.status
            .as_ref()
            .map_or(TaskState::Planned, TaskStatus::state)
    }

    pub fn status(&self) -> Option<&TaskStatus> {
        self.status.as_ref()
    }

    pub const fn is_terminal(&self) -> bool {
        matches!(self.state, RemoteTaskState::Terminal)
    }

    #[cfg(test)]
    pub fn pending_updates(&self) -> usize {
        self.pending_ordinary.len() + self.pending_control.len()
    }

    #[cfg(test)]
    pub(super) fn preparing_update(&self) -> Option<&TaskDomainUpdate> {
        self.pending_input
            .as_ref()
            .and_then(|input| Some(&input.update))
    }

    #[cfg(test)]
    pub(super) fn first_pending_update(&self) -> Option<&TaskDomainUpdate> {
        self.pending_front().map(|input| &input.update)
    }

    #[cfg(test)]
    pub const fn discarded_updates(&self) -> usize {
        self.discarded_updates
    }

    #[cfg(test)]
    pub const fn converged_after_terminal(&self) -> usize {
        self.converged_after_terminal
    }

    /// Whether an operation for this task is currently released.
    #[cfg(test)]
    pub const fn has_released_operation(&self) -> bool {
        self.create_in_flight.is_some()
            || self.cancel_in_flight.is_some()
            || matches!(
                &self.released_update,
                Some(released) if released.awaiting_outcome
            )
    }

    /// Whether this task still owes a create send.
    pub(crate) fn create_pending(&self) -> bool {
        !self.create_ownership_proven
            && !matches!(self.state, RemoteTaskState::Terminal)
            && matches!(
                self.creation,
                CreationReplay::Unfrozen(_) | CreationReplay::Frozen(_)
            )
    }

    fn create_sendable(&self, now: MonotonicInstant) -> bool {
        !self.create_ownership_proven
            && self.create_in_flight.is_none()
            && !self.create_waiting_for_establish
            && self
                .create_busy_until
                .is_none_or(|deadline| now.has_reached(deadline))
            && !matches!(self.state, RemoteTaskState::Terminal)
            && matches!(
                self.creation,
                CreationReplay::Unfrozen(_) | CreationReplay::Frozen(_)
            )
    }

    /// What sending this task's create would cost, while it still needs to
    /// be sent, without releasing or freezing anything.
    ///
    /// An unfrozen create is measured the first time it is asked for, and the
    /// measurement is kept: a create that waits behind backpressure for many
    /// turns is measured once and never encoded until it is admitted.
    pub(crate) fn create_candidate(
        &mut self,
        now: MonotonicInstant,
    ) -> Result<Option<CreateCandidate>, TaskExecutionError> {
        if !self.create_sendable(now) {
            return Ok(None);
        }
        let creation = match self.creation {
            CreationReplay::Unfrozen(creation) | CreationReplay::Frozen(creation) => creation,
            CreationReplay::Settled | CreationReplay::Closed => return Ok(None),
        };
        Ok(Some(CreateCandidate {
            operation_id: self.create_envelope.operation_id(),
            request: TaskOperationQueueRequest::create_task(
                self.identity.backend_process_id(),
                creation.queued_bytes,
            ),
            plan_carrier_bytes: creation.plan_carrier_bytes,
        }))
    }

    pub(super) fn begin_input_preparation(
        &mut self,
        update: TaskDomainUpdate,
        ids: &mut OperationIds,
    ) -> Result<(UpdateAdmission, Option<OperationIntent>), TaskExecutionError> {
        if let Some(disposition) = self.update_disposition(&update) {
            return Ok((disposition, None));
        }
        if self.pending_input.is_some() {
            return Err(TaskExecutionError::Schedule(
                "Task Native input preparation already exists".into(),
            ));
        }
        let operation = ids.issue();
        let request = UpdateTask::try_new(operation, self.identity, vec![update.clone()])?;
        let intent = request.intent();
        self.pending_input = Some(PendingUpdateInput {
            operation,
            update,
            discarded: None,
        });
        Ok((UpdateAdmission::Preparing, Some(intent)))
    }

    pub(super) fn complete_input_preparation(
        &mut self,
        operation: TaskOperationId,
        grant: QueuedInputGrantRef,
    ) -> Result<UpdateAdmission, TaskExecutionError> {
        if self
            .pending_input
            .as_ref()
            .is_none_or(|input| input.operation != operation)
        {
            self.released_grants.push(grant);
            return Err(TaskExecutionError::UnknownOperation);
        }
        let input = self.pending_input.take().expect("checked original input");
        if let Some(disposition) = input.discarded {
            self.released_grants.push(grant);
            return Ok(disposition);
        }
        self.enqueue_update(operation, input.update, grant)
    }

    /// A definitely-unused withdrawal consumes only its original preparation.
    pub(crate) fn finish_unused_preparation(
        &mut self,
        operation: TaskOperationId,
    ) -> Option<UpdateAdmission> {
        if self
            .pending_input
            .as_ref()
            .is_none_or(|input| input.operation != operation || input.discarded.is_none())
        {
            return None;
        }
        self.pending_input.take().and_then(|input| input.discarded)
    }

    /// Passive Create preview. Only a matching Source grant can confirm that
    /// the original payload has actually entered retained custody.
    pub(crate) fn create_admission_intent(
        &self,
    ) -> Option<(OperationIntent, crate::effect::QueuedInputPayload)> {
        use crate::effect::QueuedInputPayload;
        let (creation, payload) = match self.creation {
            CreationReplay::Unfrozen(c) => (c, QueuedInputPayload::NewCreation(c.resource)),
            CreationReplay::Frozen(c) => (c, QueuedInputPayload::AlreadyRetained),
            CreationReplay::Settled | CreationReplay::Closed => return None,
        };
        Some((
            OperationIntent::CreateTask {
                envelope: self.create_envelope,
                identity: self.identity,
                creation: creation.resource,
                queued_bytes: creation.queued_bytes,
            },
            payload,
        ))
    }

    pub(crate) fn note_creation_retained(&mut self) {
        if let CreationReplay::Unfrozen(creation) = self.creation {
            self.creation = CreationReplay::Frozen(creation);
        }
    }

    /// Release the already-retained passive Create request into the lane.
    pub(crate) fn release_retained_create(
        &mut self,
        now: MonotonicInstant,
    ) -> Result<OperationIntent, TaskExecutionError> {
        if !self.create_sendable(now) || !matches!(self.creation, CreationReplay::Frozen(_)) {
            return Err(TaskExecutionError::Schedule(format!(
                "task {} has no create to release",
                self.identity
            )));
        }
        let (intent, _) = self
            .create_admission_intent()
            .expect("checked sendable Create");
        self.create_in_flight = Some(self.create_envelope.operation_id());
        Ok(intent)
    }

    #[cfg(test)]
    pub(crate) fn release_create(
        &mut self,
        now: MonotonicInstant,
    ) -> Result<OperationIntent, TaskExecutionError> {
        self.note_creation_retained();
        self.release_retained_create(now)
    }

    /// Records one domain fact for this task.
    ///
    /// While the task is `Creating` the fact only enters the local queue. It
    /// is refused outright when it would move its own domain backwards, so an
    /// out-of-order producer fails where it is, not on the wire.
    pub fn enqueue_update(
        &mut self,
        operation: TaskOperationId,
        update: TaskDomainUpdate,
        queue_permit: QueuedInputGrantRef,
    ) -> Result<UpdateAdmission, TaskExecutionError> {
        if let Some(disposition) = self.update_disposition(&update) {
            self.released_grants.push(queue_permit);
            return Ok(disposition);
        }
        let expected_receipt = match self.admit_domain(&update) {
            Ok(expected) => expected,
            Err(error) => {
                self.released_grants.push(queue_permit);
                return Err(error);
            }
        };
        let ordinal = self.next_pending_ordinal;
        self.next_pending_ordinal = ordinal
            .checked_add(1)
            .expect("Task pending ordinal exhausted");
        let queue = if is_destination_close(&update) {
            &mut self.pending_control
        } else {
            &mut self.pending_ordinary
        };
        queue.push_back(PendingUpdate {
            operation,
            ordinal,
            update,
            expected_receipt,
            queue_permit,
        });
        Ok(UpdateAdmission::Queued)
    }

    fn update_disposition(&self, update: &TaskDomainUpdate) -> Option<UpdateAdmission> {
        if (self.normal_stand_down && !is_destination_close(update))
            || matches!(self.state, RemoteTaskState::Terminal)
            || self.awaiting_terminal_status
        {
            // Only this original Task owner can distinguish withdrawn demand
            // from an observed failure or a terminal outcome still awaiting its
            // status. Normal input withdrawal never replaces a contrary cause.
            let normal_withdrawal = self.normal_stand_down
                && self
                    .status
                    .as_ref()
                    .and_then(TaskStatus::termination)
                    .is_none_or(TerminationDetail::is_success_compatible);
            let normal_terminal = self
                .status
                .as_ref()
                .is_some_and(TaskStatus::is_success_compatible_terminal);
            return Some(if normal_withdrawal || normal_terminal {
                UpdateAdmission::DiscardedNoLongerNeeded
            } else {
                UpdateAdmission::DiscardedTerminal
            });
        }
        None
    }

    /// Records the decision to open one of this producer's frozen edges.
    ///
    /// The version is minted here because it is a token of *this task's*
    /// edge-open domain, and only one version may ever name one edge set. A
    /// producer feeding several exchange nodes -- which is what a multi-cast
    /// CTE, and any plan that consumes one fragment twice, produces -- has its
    /// edges decided one at a time as each edge's destinations acknowledge
    /// creation. Giving every one of those decisions the first version would
    /// replay that version with a different edge set, which is
    /// `SameTokenDifferentContent` and fails the whole attempt.
    pub fn prepare_edge_open(
        &self,
        edge: ExchangeEdgeId,
    ) -> Result<TaskDomainUpdate, TaskExecutionError> {
        let version = self
            .domains
            .next_edge_open_version()
            .ok_or(TaskExecutionError::Capacity(
                CapacityBound::EdgeOpenVersions { limit: u32::MAX },
            ))?;
        Ok(TaskDomainUpdate::OpenExchangeEdges {
            version,
            edges: vec![edge],
        })
    }

    /// Mints the next exact per-destination close token for this producer.
    ///
    /// The caller admits the result through `enqueue_update`, which checks
    /// membership against this task's frozen outbound destinations and holds
    /// the immutable intent for an exact resend after an unknown outcome.
    /// Closing one member never changes the eligibility of its siblings.
    pub fn prepare_destination_close(
        &self,
        edge: ExchangeEdgeId,
        destination: TaskIdentity,
    ) -> Result<TaskDomainUpdate, TaskExecutionError> {
        let version = self
            .domains
            .next_destination_close_version()
            .ok_or_else(|| {
                TaskExecutionError::Schedule(
                    "the task destination-close version space is exhausted".to_owned(),
                )
            })?;
        Ok(TaskDomainUpdate::CloseExchangeDestination {
            version,
            edge,
            destination,
        })
    }

    fn admit_domain(
        &mut self,
        update: &TaskDomainUpdate,
    ) -> Result<TaskDomainReceiptExpectation, TaskExecutionError> {
        let split_watermark = match update {
            TaskDomainUpdate::SplitAssignment { node, .. } => {
                Some(self.domains.split_watermark(*node))
            }
            _ => None,
        };
        let progression = self.domains.record(update);
        let admitted = match update {
            TaskDomainUpdate::SplitAssignment { node, offer, .. } => {
                let watermark = split_watermark.expect("a split update has a prior watermark");
                // The same rule the backend applies. Judging by the range
                // alone rejects the terminal marker every task of a plan node
                // receives once its splits already arrived, and that task's
                // scan then waits forever for a seal it was already told
                // about.
                match progression {
                    DomainProgression::Apply => Ok(()),
                    // An offer this owner already applied. ADR-0123 makes the
                    // identical request the recovery for an unknown outcome,
                    // and by then this side's watermark has advanced, so the
                    // resend can only classify as idempotent -- refusing it
                    // made the frontend reject its own recovery. Observed as a
                    // seal offered twice: `offered=1..=1 no_more=true` against
                    // a watermark already sealed there.
                    //
                    // The producer-bug this used to catch -- a sequence reused
                    // for different content -- is not catchable here either:
                    // ADR-0123 records that only the watermark is kept, so an
                    // exact replay and a reused sequence are indistinguishable
                    // by construction. Nothing advances; the watermark is
                    // already where this update would put it.
                    DomainProgression::Idempotent => Ok(()),
                    DomainProgression::Older => Err(split_regression(
                        *node,
                        *offer,
                        watermark,
                        DomainConflict::NotMonotonic,
                    )),
                    DomainProgression::Conflict(conflict) => {
                        Err(split_regression(*node, *offer, watermark, conflict))
                    }
                }
            }
            TaskDomainUpdate::TaskDynamicFilter { version, .. } => {
                // A same-version re-offer is a replay only when the immutable
                // payload fingerprint is also identical. A producer cannot
                // reuse the token for different content.
                match progression {
                    DomainProgression::Apply | DomainProgression::Idempotent => Ok(()),
                    DomainProgression::Older => Err(TaskExecutionError::DomainRegression {
                        domain: "task_dynamic_filter",
                        token: format!("version={}", version.get()),
                        conflict: DomainConflict::NotMonotonic,
                    }),
                    DomainProgression::Conflict(conflict) => {
                        Err(TaskExecutionError::DomainRegression {
                            domain: "task_dynamic_filter",
                            token: format!("version={}", version.get()),
                            conflict,
                        })
                    }
                }
            }
            TaskDomainUpdate::OpenExchangeEdges { version, edges } => match progression {
                DomainProgression::Apply => Ok(()),
                DomainProgression::Idempotent => Err(TaskExecutionError::EdgeAlreadyOpened(
                    *edges.first().expect("a validated edge set is nonempty"),
                )),
                DomainProgression::Older => Err(edge_regression(
                    *version,
                    edges,
                    DomainConflict::NotMonotonic,
                )),
                DomainProgression::Conflict(conflict) => {
                    Err(edge_regression(*version, edges, conflict))
                }
            },
            TaskDomainUpdate::CloseExchangeDestination { version, .. } => match progression {
                DomainProgression::Apply | DomainProgression::Idempotent => Ok(()),
                DomainProgression::Older => Err(TaskExecutionError::DomainRegression {
                    domain: "close_exchange_destination",
                    token: format!("version={}", version.get()),
                    conflict: DomainConflict::NotMonotonic,
                }),
                DomainProgression::Conflict(conflict) => {
                    Err(TaskExecutionError::DomainRegression {
                        domain: "close_exchange_destination",
                        token: format!("version={}", version.get()),
                        conflict,
                    })
                }
            },
        };
        admitted.map(|()| self.domains.receipt_expectation(update))
    }

    /// What the next update send would cost, read without taking it.
    ///
    /// Admission asks this before it may take anything: an update skipped
    /// because its target is full must stay exactly where it is, with the
    /// process reservation it already holds.
    pub(crate) fn update_candidate(&self) -> Option<UpdateCandidate> {
        if !matches!(self.state, RemoteTaskState::Created)
            || !self.installed()
            || self.awaiting_terminal_status
        {
            return None;
        }
        let backend = self.identity.backend_process_id();
        if let Some(released) = &self.released_update {
            if self.normal_stand_down
                && !released.request.domains().iter().all(is_destination_close)
            {
                return None;
            }
            if released.awaiting_outcome {
                return None;
            }
            return Some(UpdateCandidate {
                request: released.request.queue_request(),
                holds_queue_permit: false,
            });
        }
        let pending = self.pending_front()?;
        Some(UpdateCandidate {
            request: TaskOperationQueueRequest::task_update(backend, &pending.update),
            holds_queue_permit: true,
        })
    }

    /// The next update to send, if any may be sent right now.
    ///
    /// One domain change per request and at most one request in flight. That
    /// keeps a receipt attributable to exactly one domain and keeps a slow
    /// task from accumulating unacknowledged work.
    pub fn next_update_intent(&mut self) -> Result<NextTaskUpdateIntent, TaskExecutionError> {
        if !matches!(self.state, RemoteTaskState::Created)
            || !self.installed()
            || self.awaiting_terminal_status
        {
            return Ok(None);
        }
        if let Some(released) = &mut self.released_update {
            if self.normal_stand_down
                && !released.request.domains().iter().all(is_destination_close)
            {
                return Ok(None);
            }
            if released.awaiting_outcome {
                return Ok(None);
            }
            released.awaiting_outcome = true;
            return Ok(Some((released.request.intent(), None)));
        }
        let Some(pending) = self.pop_pending() else {
            return Ok(None);
        };
        let operation_id = pending.operation;
        let request = Arc::new(UpdateTask::try_new(
            operation_id,
            self.identity(),
            vec![pending.update],
        )?);
        self.released_update = Some(ReleasedUpdate {
            operation_id,
            request: Arc::clone(&request),
            expected_receipt: pending.expected_receipt,
            awaiting_outcome: true,
        });
        Ok(Some((request.intent(), Some(pending.queue_permit))))
    }

    /// Rolls back a request that never crossed process queue admission.
    ///
    /// The immutable request remains retained for exact re-release. The only
    /// exception is cancellation: an unsent cancellation has no remote effect,
    /// so clearing its local marker lets the stage mint a fresh request later.
    pub(crate) fn rollback_unsent(&mut self, operation_id: TaskOperationId) {
        if self.create_in_flight == Some(operation_id) {
            self.create_in_flight = None;
            return;
        }
        if let Some(released) = &mut self.released_update
            && released.operation_id == operation_id
        {
            released.awaiting_outcome = false;
            if self.normal_stand_down
                && !released.request.domains().iter().all(is_destination_close)
            {
                self.released_update = None;
            }
            return;
        }
        if self.cancel_in_flight == Some(operation_id) {
            self.cancel_in_flight = None;
            self.cancel_requested = false;
        }
    }

    /// Stands this task down normally, once.
    pub fn cancel_intent(
        &mut self,
        reason: CancelReason,
        ids: &mut OperationIds,
    ) -> Option<OperationIntent> {
        if self.cancel_requested
            || (self.normal_stand_down && !self.create_ownership_proven)
            || matches!(self.state, RemoteTaskState::Terminal)
            || self.awaiting_terminal_status
        {
            return None;
        }
        let operation_id = ids.issue();
        self.cancel_requested = true;
        self.cancel_in_flight = Some(operation_id);
        Some(OperationIntent::CancelTask(CancelTask::new(
            operation_id,
            self.identity(),
            reason,
        )))
    }

    #[cfg(test)]
    pub const fn cancel_requested(&self) -> bool {
        self.cancel_requested
    }

    /// Settles the create acknowledgement.
    ///
    /// Only an exactly correlated success -- this send's operation, an applied
    /// outcome, a receipt for this very task, and a status this owner can
    /// adopt -- settles the create and releases its payload for good. An
    /// unknown outcome keeps the frozen parts for the identical resend; a
    /// definitive refusal closes the create.
    pub fn on_create_ack(
        &mut self,
        ack: &OperationAcknowledgement,
        now: MonotonicInstant,
        participation_established: bool,
        observation_horizon: Option<CreateRefusalObservationHorizon>,
    ) -> Result<CreateSettlement, TaskExecutionError> {
        if ack.kind() != OperationKind::CreateTask
            || self.create_in_flight != Some(ack.operation_id())
        {
            return Err(TaskExecutionError::UnknownOperation);
        }
        self.apply_create_ack(ack, now, participation_established, observation_horizon)
    }

    /// Fold a Source-correlated definitive reply while the newer replay is
    /// still waiting for input admission. The original remote-unknown marker
    /// belongs to the operation and survives the gap between sends.
    pub(crate) fn on_prior_create_ack(
        &mut self,
        ack: &OperationAcknowledgement,
        now: MonotonicInstant,
        participation_established: bool,
        observation_horizon: Option<CreateRefusalObservationHorizon>,
    ) -> Result<CreateSettlement, TaskExecutionError> {
        if ack.kind() != OperationKind::CreateTask
            || ack.operation_id() != self.create_envelope.operation_id()
            || ack.dispatch_result() == OperationDispatchResult::TransportUnknown
            || !self.create_remote_unknown
            || self.create_in_flight.is_some()
        {
            return Err(TaskExecutionError::UnknownOperation);
        }
        self.apply_create_ack(ack, now, participation_established, observation_horizon)
    }

    fn apply_create_ack(
        &mut self,
        ack: &OperationAcknowledgement,
        now: MonotonicInstant,
        participation_established: bool,
        observation_horizon: Option<CreateRefusalObservationHorizon>,
    ) -> Result<CreateSettlement, TaskExecutionError> {
        self.create_in_flight = None;
        if ack.worker_outcome() == Some(OperationOutcome::OwnerFailed) {
            self.stand_down_after_owner_failure();
            return Ok(CreateSettlement::FailedClosed(
                OperationOutcome::OwnerFailed,
            ));
        }
        if self.fenced_out {
            if ack.is_applied() {
                return Err(TaskExecutionError::DomainReceipt(format!(
                    "accepted Create contradicts the exact Quiesce fence for task {}",
                    self.identity
                )));
            }
            return Ok(CreateSettlement::Closing);
        }
        if ack.is_applied() {
            let AckPayload::Create(receipt) = ack.payload() else {
                return Err(TaskExecutionError::MissingReceipt(
                    OperationKind::CreateTask,
                ));
            };
            self.identity().verify_matches(receipt.identity())?;
            // Settled before the status is adopted: the task exists on the
            // backend from here on whatever the snapshot says, and no create
            // may follow an applied one.
            self.settle_creation();
            self.create_ownership_proven = true;
            if !matches!(self.state, RemoteTaskState::Terminal) {
                self.state = RemoteTaskState::Created;
            }
            // The acknowledgement carries the task's own first snapshot, which
            // closes the window between creating a task and observing it.
            //
            // It is classified exactly like a subscribed snapshot rather than
            // adopted outright. The create response and the status stream are
            // two independent transports, so the stream can already have
            // delivered a newer version by the time this response is settled;
            // adopting version 1 over a held version 2 regressed a running
            // task back to PLANNED and failed the attempt on an illegal
            // transition. One classification owns whether a snapshot is
            // adoptable, whichever channel carried it.
            self.observe_status(receipt.current_status())?;
            return Ok(CreateSettlement::Created);
        }
        if self.normal_stand_down {
            // An older refusal cannot restore replay after normal closure;
            // Quiesce decides whether a prior unknown send was accepted.
            return Ok(CreateSettlement::Closing);
        }
        match frontend_action(ack.dispatch_result()) {
            FrontendAction::RetryExactRequest => {
                if self.create_ownership_proven {
                    return Ok(CreateSettlement::Created);
                }
                self.create_remote_unknown = true;
                Ok(CreateSettlement::RetryExactRequest)
            }
            FrontendAction::RetryAfterProgress => {
                let outcome = ack
                    .worker_outcome()
                    .ok_or(crate::error::dispatch_rejection(
                        ack,
                        OperationKind::CreateTask,
                    ))?;
                if self.create_ownership_proven
                    && matches!(
                        outcome,
                        OperationOutcome::NotReady | OperationOutcome::PreparationBusy
                    )
                {
                    return Ok(CreateSettlement::Created);
                }
                match outcome {
                    OperationOutcome::NotReady => {
                        self.create_waiting_for_establish = !participation_established;
                    }
                    OperationOutcome::PreparationBusy => {
                        self.create_busy_rejections = self.create_busy_rejections.saturating_add(1);
                        let shift = self.create_busy_rejections.saturating_sub(1).min(5);
                        let delay_ms = 10_u64.saturating_mul(1_u64 << shift);
                        self.create_busy_until =
                            Some(now.saturating_add(Duration::from_millis(delay_ms)));
                    }
                    _ => {
                        self.enter_terminal();
                        return Ok(CreateSettlement::FailedClosed(outcome));
                    }
                }
                Ok(CreateSettlement::RetryAfterProgress(outcome))
            }
            _ => {
                let Some(outcome) = ack.worker_outcome() else {
                    // An earlier unknown generation may already have created
                    // this Task. Preserve status and Abort responsibility in
                    // that case; this ingress refusal proves only this send
                    // did not reach Worker.
                    if !self.create_remote_unknown {
                        self.enter_terminal();
                    }
                    return Err(crate::error::dispatch_rejection(
                        ack,
                        OperationKind::CreateTask,
                    ));
                };
                if !self.create_ownership_proven
                    && outcome == OperationOutcome::IdentityMismatch
                    && matches!(
                        ack.worker_header(),
                        Some(
                            novarocks_execution_contract::operation::OperationResult::Verdict {
                                outcome: OperationOutcome::IdentityMismatch,
                                ..
                            }
                        )
                    )
                    && let Some(horizon) = observation_horizon
                    && horizon.matches_parent_acknowledgement(participation_established)
                    && !now.has_reached(horizon.expiry())
                {
                    // on_create_ack has matched the original in-flight Task.
                    // Preserve the issued Establish ticket or Applied lease;
                    // a later ACK/renewal never replaces this original cut.
                    self.rejected_create_observation
                        .get_or_insert((ack.operation_id(), horizon.expiry()));
                }
                self.enter_terminal();
                Ok(CreateSettlement::FailedClosed(outcome))
            }
        }
    }

    /// The exact Establish acknowledgement satisfies a prior NotReady reply.
    pub(crate) fn on_participation_established(&mut self) {
        self.create_waiting_for_establish = false;
    }

    /// Settles one update acknowledgement.
    pub fn on_update_ack(
        &mut self,
        ack: &OperationAcknowledgement,
    ) -> Result<UpdateSettlement, TaskExecutionError> {
        let released = self
            .released_update
            .as_mut()
            .filter(|released| {
                released.awaiting_outcome && released.operation_id == ack.operation_id()
            })
            .ok_or(TaskExecutionError::UnknownOperation)?;
        released.awaiting_outcome = false;
        if ack.worker_outcome() == Some(OperationOutcome::OwnerFailed) {
            self.released_update = None;
            self.stand_down_after_owner_failure();
            return Ok(UpdateSettlement::FailedClosed(
                OperationOutcome::OwnerFailed,
            ));
        }
        let identity = released.request.identity();
        let obsolete_input =
            self.normal_stand_down && !released.request.domains().iter().all(is_destination_close);
        if ack.is_applied() {
            let AckPayload::Update(receipt) = ack.payload() else {
                return Err(TaskExecutionError::MissingReceipt(
                    OperationKind::UpdateTask,
                ));
            };
            identity.verify_matches(receipt.identity())?;
            let [intent] = released.request.domains() else {
                return Err(TaskExecutionError::DomainReceipt(format!(
                    "one update operation carried {} domains",
                    released.request.domains().len()
                )));
            };
            verify_task_domain_receipt(intent, &released.expected_receipt, receipt.domains())
                .map_err(|error| TaskExecutionError::DomainReceipt(error.to_string()))?;
            self.released_update = None;
            return Ok(UpdateSettlement::Applied);
        }
        match frontend_action(ack.dispatch_result()) {
            FrontendAction::RetryExactRequest => {
                if obsolete_input {
                    self.released_update = None;
                    return Ok(UpdateSettlement::DiscardedAfterNormalStandDown);
                }
                if matches!(self.state, RemoteTaskState::Terminal) {
                    // A terminal task owes nothing further, so an unknown
                    // outcome is abandoned rather than replayed at a task that
                    // can no longer apply it.
                    self.released_update = None;
                    self.record_terminal_convergence();
                    return Ok(UpdateSettlement::ConvergedAfterTerminal);
                }
                return Ok(UpdateSettlement::RetryExactRequest);
            }
            FrontendAction::StopSendingAndReconcile | FrontendAction::Settled => {
                self.released_update = None;
                self.record_terminal_convergence();
                if obsolete_input {
                    return Ok(UpdateSettlement::DiscardedAfterNormalStandDown);
                }
                if !matches!(self.state, RemoteTaskState::Terminal) {
                    self.await_terminal_status();
                    return Ok(UpdateSettlement::AwaitingTerminalStatus);
                }
                return Ok(UpdateSettlement::ConvergedAfterTerminal);
            }
            FrontendAction::FailOperationClosed
            | FrontendAction::FailAttempt
            | FrontendAction::RetryAfterProgress => {}
        }
        self.released_update = None;
        let Some(outcome) = ack.worker_outcome() else {
            // An ingress refusal says nothing about the already accepted
            // task's terminal state. Failure cleanup must still stop it.
            return Err(crate::error::dispatch_rejection(
                ack,
                OperationKind::UpdateTask,
            ));
        };
        self.enter_terminal();
        Ok(UpdateSettlement::FailedClosed(outcome))
    }

    /// Settles one cancel acknowledgement.
    ///
    /// A cancel is a request to stand down, not a terminal fact: the terminal
    /// state still arrives as a published status.
    pub fn on_cancel_ack(
        &mut self,
        ack: &OperationAcknowledgement,
    ) -> Result<(), TaskExecutionError> {
        if self.cancel_in_flight != Some(ack.operation_id()) {
            return Err(TaskExecutionError::UnknownOperation);
        }
        self.cancel_in_flight = None;
        if ack.worker_outcome() == Some(OperationOutcome::OwnerFailed) {
            self.stand_down_after_owner_failure();
            return Err(TaskExecutionError::OperationFailed {
                task: Some(self.identity),
                participation: self.participation,
                operation_id: ack.operation_id(),
                kind: OperationKind::CancelTask,
                rejection: ack
                    .worker_header()
                    .expect("owner failure is a checked Worker verdict"),
            });
        }
        if ack.worker_outcome().is_none()
            && ack.dispatch_result() != OperationDispatchResult::TransportUnknown
        {
            return Err(crate::error::dispatch_rejection(
                ack,
                OperationKind::CancelTask,
            ));
        }
        Ok(())
    }

    /// Applies one published status snapshot.
    pub fn observe_status(
        &mut self,
        observed: &TaskStatus,
    ) -> Result<StatusObservation, TaskExecutionError> {
        if self.fenced_out {
            return Err(TaskExecutionError::DomainReceipt(format!(
                "status contradicts the exact Quiesce fence for task {}",
                self.identity
            )));
        }
        let observation = classify_observation(self.cursor, self.status.as_ref(), observed);
        match observation {
            StatusObservation::Accept => {
                self.adopt_status(observed)?;
                Ok(StatusObservation::Accept)
            }
            StatusObservation::Idempotent | StatusObservation::Ignore => Ok(observation),
            fatal => Err(TaskExecutionError::Observation(fatal)),
        }
    }

    /// Classifies a `task_gone` event against what this owner holds.
    pub fn observe_gone(&self) -> GoneObservation {
        classify_gone(self.status.as_ref())
    }

    fn adopt_status(&mut self, observed: &TaskStatus) -> Result<(), TaskExecutionError> {
        // Transition legality is a statement about consecutive versions, so it
        // may only be asked of consecutive versions. A status is an immutable
        // versioned snapshot rather than an event: a subscription's catch-up
        // replays only the latest version per cursor, so any resubscription --
        // which is what an observation loss produces -- can legitimately hand
        // this owner a version several ahead of the one it holds. The states in
        // between existed and were passed through; they were simply not seen.
        //
        // Judging such a jump by the adjacent-transition table refuses it:
        // PLANNED to FINISHED is illegal between neighbours and unremarkable
        // across a gap. That refusal failed the whole query, so a dropped
        // status stream -- a recoverable observation problem by construction --
        // became a lost query.
        //
        // What is deliberately still enforced across a gap: a terminal state
        // never becomes anything else, which the observed-transition
        // classifier reports separately from `Illegal`, and the terminal
        // content agreement the final-info acceptance checks. Neither depends
        // on adjacency.
        let adjacent = self
            .status
            .as_ref()
            .and_then(|held| held.version().next())
            .is_some_and(|next| next == observed.version());
        if adjacent
            && let Some(held) = &self.status
            && matches!(
                classify_observed_task_transition(held.state(), observed.state()),
                ObservedTaskTransition::Illegal
            )
        {
            return Err(TaskExecutionError::IllegalTaskTransition {
                from: held.state(),
                to: observed.state(),
            });
        }
        self.cursor = self.cursor.advanced_to(observed.version());
        let terminal = observed.is_terminal();
        self.status = Some(observed.clone());
        if observed.installed() || terminal {
            self.create_ownership_proven = true;
            if !terminal && matches!(self.state, RemoteTaskState::Creating) {
                self.state = RemoteTaskState::Created;
            }
            self.settle_creation();
        }
        if terminal {
            self.enter_terminal();
        }
        Ok(())
    }

    /// Stops business sends after owner failure without inventing terminal
    /// evidence. Existing published status and physical convergence remain
    /// owned by the status/participation channels.
    fn stand_down_after_owner_failure(&mut self) {
        self.discard_creation_after_attempt_failure();
        if !self.is_terminal() {
            self.await_terminal_status();
        }
    }

    fn enter_terminal(&mut self) {
        self.state = RemoteTaskState::Terminal;
        self.awaiting_terminal_status = false;
        self.discard_pending(true);
        self.discard_pending_input(true);
        // A terminal task is never created again, so neither its seed nor its
        // frozen parts can be needed. A send already in flight keeps its own
        // handle until it settles.
        if !matches!(self.creation, CreationReplay::Settled) {
            self.close_creation();
        }
        self.withdraw_unsent_preparations();
    }

    fn await_terminal_status(&mut self) {
        self.awaiting_terminal_status = true;
        self.discard_pending(true);
        self.discard_pending_input(true);
        self.withdraw_unsent_preparations();
    }

    fn close_creation(&mut self) {
        self.retire_creation(CreationReplay::Closed);
    }
    fn settle_creation(&mut self) {
        self.retire_creation(CreationReplay::Settled);
    }
    fn retire_creation(&mut self, next: CreationReplay) {
        if let CreationReplay::Unfrozen(c) | CreationReplay::Frozen(c) =
            std::mem::replace(&mut self.creation, next)
        {
            self.released_payloads.push(c.resource);
        }
    }
    pub(crate) fn take_released_payload(&mut self) -> Option<PayloadRef> {
        self.released_payloads.pop()
    }
    /// The caller charges one cleanup item before consuming this original ref.
    pub(crate) fn take_released_grant(&mut self) -> Option<QueuedInputGrantRef> {
        if let Some(grant) = self.released_grants.pop() {
            return Some(grant);
        }
        let batch = self.discarded_pending.front_mut()?;
        let grant = batch
            .pop_front()
            .expect("discarded batch is nonempty")
            .queue_permit;
        if batch.is_empty() {
            self.discarded_pending.pop_front();
        }
        Some(grant)
    }

    pub(crate) fn has_released_grant(&self) -> bool {
        !self.released_grants.is_empty() || !self.discarded_pending.is_empty()
    }
    pub(crate) fn take_withdrawn_preparation(&mut self) -> Option<TaskOperationId> {
        self.withdrawn_preparations.pop()
    }

    /// The terminal facts, once this task has them.
    pub fn terminal_report(&self) -> Option<TaskTerminalReport> {
        if self.fenced_out || !matches!(self.state, RemoteTaskState::Terminal) {
            return None;
        }
        let status = self.status.as_ref();
        Some(TaskTerminalReport {
            identity: self.identity(),
            state: status.map_or(TaskState::Planned, TaskStatus::state),
            termination: status.and_then(|status| status.termination().cloned()),
            discarded_updates: self.discarded_updates,
        })
    }

    /// Whether this task's output responsibility is complete.
    pub fn output_released(&self) -> bool {
        self.status
            .as_ref()
            .is_some_and(|status| status.output().responsibility_complete())
    }
}

#[cfg(test)]
mod bounded_discard_tests {
    use super::*;
    use crate::effect::FrozenContentRef;
    use novarocks_execution_contract::{ContentFingerprint, DomainVersion};
    use std::num::NonZeroU64;

    fn grant(value: u64) -> QueuedInputGrantRef {
        QueuedInputGrantRef::new(
            NonZeroU64::new(value).unwrap(),
            crate::input::SendGeneration::new(
                novarocks_coordination_contract::AttemptActionId::new(
                    NonZeroU64::new(value).unwrap(),
                ),
            ),
        )
    }
    fn operation(value: u64) -> TaskOperationId {
        OperationIds::new(crate::ids::IdSeed::new(u128::from(value), 1)).issue()
    }
    fn filter(version: u64) -> TaskDomainUpdate {
        TaskDomainUpdate::TaskDynamicFilter {
            version: DomainVersion::new(version).unwrap(),
            payload: FrozenContentRef {
                resource: PayloadRef(NonZeroU64::new(version).unwrap()),
                fingerprint: ContentFingerprint::from_bytes([7; 16]),
                queued_bytes: 16,
            },
        }
    }
    #[test]
    fn normal_stand_down_preserves_exact_close_and_refunds_original_discarded_grants_once() {
        let mut task = crate::test_support::remote(1);
        let destination = crate::test_support::remote(2).identity();
        let edge = ExchangeEdgeId::new(1).unwrap();
        task.domains = TaskDomainIntentTracker::new([], [edge])
            .with_exchange_destinations([(edge, destination)]);
        assert_eq!(
            task.enqueue_update(operation(1), filter(1), grant(1))
                .unwrap(),
            UpdateAdmission::Queued
        );
        let close = TaskDomainUpdate::CloseExchangeDestination {
            version: DomainVersion::FIRST,
            edge,
            destination,
        };
        assert_eq!(
            task.enqueue_update(operation(2), close, grant(2)).unwrap(),
            UpdateAdmission::Queued
        );
        assert_eq!(
            task.enqueue_update(operation(3), filter(2), grant(3))
                .unwrap(),
            UpdateAdmission::Queued
        );
        task.begin_normal_stand_down();
        assert_eq!(task.pending_updates(), 1);
        assert_eq!(task.discarded_updates(), 2);
        assert!(is_destination_close(task.first_pending_update().unwrap()));
        assert_eq!(task.take_released_grant(), Some(grant(1)));
        assert_eq!(task.take_released_grant(), Some(grant(3)));
        assert_eq!(task.take_released_grant(), None);
        task.enter_terminal();
        assert_eq!(task.discarded_updates(), 3);
        assert_eq!(task.take_released_grant(), Some(grant(2)));
        task.enter_terminal();
        assert_eq!(task.discarded_updates(), 3);
        assert_eq!(task.take_released_grant(), None);
    }

    #[test]
    fn terminal_acceptance_records_all_discards_before_separate_grant_cleanup() {
        let mut task = crate::test_support::remote(1);
        for version in 1..=10_000 {
            task.enqueue_update(operation(version), filter(version), grant(version))
                .unwrap();
        }
        task.enter_terminal();
        assert_eq!(task.pending_updates(), 0);
        assert_eq!(task.terminal_report().unwrap().discarded_updates, 10_000);
        assert!(task.update_candidate().is_none());
        for value in 1..=10_000 {
            assert!(task.has_released_grant());
            assert_eq!(task.take_released_grant(), Some(grant(value)));
        }
        assert!(!task.has_released_grant());
        assert_eq!(task.take_released_grant(), None);
    }

    #[test]
    fn native_input_preparation_and_exact_replay_keep_the_same_update_operation() {
        use crate::input::{LocalDispatchResult, OperationReplyResult};
        use novarocks_execution_contract::{TaskOutputFacts, TaskStatusVersion};
        let mut task = crate::test_support::remote(1);
        let status = TaskStatus::try_new(
            task.identity(),
            TaskStatusVersion::new(2).unwrap(),
            TaskState::Running,
            None,
            TaskOutputFacts::default(),
        )
        .unwrap();
        task.observe_status(&status).unwrap();
        let mut ids = OperationIds::new(crate::ids::IdSeed::new(100, 1));
        let (_, original) = task.begin_input_preparation(filter(1), &mut ids).unwrap();
        let original = original.unwrap();
        assert_eq!(
            task.complete_input_preparation(original.operation_id(), grant(1))
                .unwrap(),
            UpdateAdmission::Queued
        );
        let (first, held) = task.next_update_intent().unwrap().unwrap();
        assert_eq!(first.operation_id(), original.operation_id());
        assert_eq!(held, Some(grant(1)));
        let reply = OperationAcknowledgement {
            operation_id: first.operation_id(),
            kind: OperationKind::UpdateTask,
            result: OperationReplyResult::Local {
                result: LocalDispatchResult::TransportUnknown,
                failure: None,
                protocol_failure: None,
                detail: None,
            },
        };
        assert_eq!(
            task.on_update_ack(&reply).unwrap(),
            UpdateSettlement::RetryExactRequest
        );
        let (replay, held) = task.next_update_intent().unwrap().unwrap();
        assert_eq!(replay.operation_id(), original.operation_id());
        assert!(held.is_none());
        let (
            OperationIntent::UpdateTask {
                domains: original, ..
            },
            OperationIntent::UpdateTask {
                domains: replay, ..
            },
        ) = (first, replay)
        else {
            panic!("exact Update intent");
        };
        assert!(Arc::ptr_eq(&original, &replay));
    }

    #[test]
    fn late_preparation_grant_retains_the_original_cut_disposition() {
        let mut task = crate::test_support::remote(1);
        let mut ids = OperationIds::new(crate::ids::IdSeed::new(100, 1));
        let (admission, intent) = task.begin_input_preparation(filter(1), &mut ids).unwrap();
        assert_eq!(admission, UpdateAdmission::Preparing);
        let operation = intent.unwrap().operation_id();
        task.begin_normal_stand_down();
        assert_eq!(task.discarded_updates(), 1);
        assert_eq!(task.take_withdrawn_preparation(), Some(operation));
        assert_eq!(
            task.complete_input_preparation(operation, grant(1))
                .unwrap(),
            UpdateAdmission::DiscardedNoLongerNeeded
        );
        assert_eq!(task.discarded_updates(), 1);
        assert_eq!(task.take_released_grant(), Some(grant(1)));
        assert_eq!(task.take_released_grant(), None);
    }
}

#[cfg(test)]
mod destination_close_tests;
