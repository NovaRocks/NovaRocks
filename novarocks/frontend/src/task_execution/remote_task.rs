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
//! `Creating` accepts domain facts but sends none of them: only the exact
//! `CreateTask` may be in flight, and while its outcome is unknown nothing
//! else may go out concurrently, because an update that raced a create could
//! be applied to a task the backend has not admitted. `Created` is reached
//! only by an exact create acknowledgement, and the accumulated facts then
//! drain one at a time in the order their domains progressed. `Terminal`
//! discards what was never sent and lets what is already in flight converge
//! on its own retained receipt. An update rejected because the backend task
//! already terminated stops further sends while the owner waits for the
//! terminal status; the rejection itself is not a second terminal authority.

use std::collections::VecDeque;
use std::sync::Arc;

use novarocks_execution::task_execution::{
    CancelReason, CancelTask, CreateTask, DomainConflict, DomainProgression, ExchangeEdgeId,
    OperationKind, OperationOutcome, QueryContextRef, TaskDescriptor, TaskDomainKind,
    TaskDomainUpdate, TaskIdentity, TaskOperationId, TaskState, TaskStatus, TaskStatusCursor,
    TerminationDetail, UpdateTask,
};
use novarocks_query_application::coordination::{
    FrontendAction, GoneObservation, ObservedTaskTransition, StatusObservation,
    TaskDomainIntentTracker, TaskDomainReceiptExpectation, classify_gone, classify_observation,
    classify_observed_task_transition, frontend_action, verify_task_domain_receipt,
};

use super::error::{CapacityBound, TaskExecutionError};
use super::intent::{AckPayload, OperationAcknowledgement, OperationIntent};

/// The three states of one remote task.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RemoteTaskState {
    /// The create has not been acknowledged. Domain facts accumulate locally.
    Creating,
    /// An exact create acknowledgement arrived. Accumulated facts drain.
    Created,
    /// This task reached a terminal outcome, or an operation for it failed
    /// closed.
    Terminal,
}

/// What a create acknowledgement did to this task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CreateSettlement {
    /// The task is created and its accumulated facts may now drain.
    Created,
    /// The outcome was genuinely unknown; the identical request must be resent.
    RetryExactRequest,
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

#[derive(Clone, Debug)]
struct PendingUpdate {
    update: TaskDomainUpdate,
    expected_receipt: TaskDomainReceiptExpectation,
}

/// The frontend's owner of one remote task.
#[derive(Debug)]
pub struct RemoteTask {
    /// The single owner of this task's descriptor. Every consumer of the
    /// create request, including the dispatcher, holds a handle to this value
    /// rather than a copy of the plan it carries.
    create: Arc<CreateTask>,
    state: RemoteTaskState,
    create_in_flight: Option<TaskOperationId>,
    create_acknowledged: bool,
    pending: VecDeque<PendingUpdate>,
    released_update: Option<ReleasedUpdate>,
    cancel_in_flight: Option<TaskOperationId>,
    cancel_requested: bool,
    status: Option<TaskStatus>,
    cursor: TaskStatusCursor,
    domains: TaskDomainIntentTracker,
    awaiting_terminal_status: bool,
    discarded_updates: usize,
    converged_after_terminal: usize,
}

/// An edge-open refusal that names the version and edge set it refused.
fn edge_regression(
    version: novarocks_execution::task_execution::EdgeOpenVersion,
    edges: &[novarocks_execution::task_execution::ExchangeEdgeId],
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
    intent: &novarocks_execution::task_execution::SplitAssignmentIntent,
    watermark: novarocks_query_application::coordination::SentSplitWatermark,
    conflict: DomainConflict,
) -> TaskExecutionError {
    TaskExecutionError::DomainRegression {
        domain: "split_assignment",
        // Both halves. A conflict is a relation between the offer and the
        // state it lost against, and reporting only the offer leaves the
        // reader to obtain the other half from a cluster run.
        token: format!(
            "plan_node={} {} against accepted_through={:?} sealed={}",
            intent.node(),
            intent.offer(),
            watermark.accepted_through().map(|s| s.get()),
            watermark.no_more_splits()
        ),
        conflict,
    }
}

impl RemoteTask {
    /// Freezes one task's create request from its descriptor.
    pub fn new(
        descriptor: TaskDescriptor,
        context: QueryContextRef,
    ) -> Result<Self, TaskExecutionError> {
        let identity = descriptor.identity();
        let domains = TaskDomainIntentTracker::new(
            descriptor.split_plan_nodes().iter().copied(),
            descriptor.topology().edge_ids(),
        );
        let create =
            CreateTask::try_new(TaskOperationId::new_v7(), context, descriptor, Vec::new())?;
        Ok(Self {
            create: Arc::new(create),
            state: RemoteTaskState::Creating,
            create_in_flight: None,
            create_acknowledged: false,
            pending: VecDeque::new(),
            released_update: None,
            cancel_in_flight: None,
            cancel_requested: false,
            status: None,
            cursor: TaskStatusCursor::unobserved(identity),
            domains,
            awaiting_terminal_status: false,
            discarded_updates: 0,
            converged_after_terminal: 0,
        })
    }

    pub fn identity(&self) -> TaskIdentity {
        self.create.identity()
    }

    /// Where this task's status observation has reached.
    ///
    /// A resubscription replays from here rather than from nothing, so a
    /// transport that dropped does not re-deliver every version this task ever
    /// published.
    pub const fn cursor(&self) -> TaskStatusCursor {
        self.cursor
    }

    pub fn context(&self) -> QueryContextRef {
        self.create.context()
    }

    pub fn descriptor(&self) -> &TaskDescriptor {
        self.create.descriptor()
    }

    pub const fn state(&self) -> RemoteTaskState {
        self.state
    }

    /// This task's last observed lifecycle state.
    ///
    /// A task whose status has not been published yet is `PLANNED`: that is
    /// what the create acknowledgement's own first snapshot carries, and it is
    /// the only state a stage may assume for a task it has not observed.
    pub fn task_state(&self) -> TaskState {
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

    pub fn pending_updates(&self) -> usize {
        self.pending.len()
    }

    pub const fn discarded_updates(&self) -> usize {
        self.discarded_updates
    }

    pub const fn converged_after_terminal(&self) -> usize {
        self.converged_after_terminal
    }

    /// Whether an operation for this task is currently released.
    pub const fn has_released_operation(&self) -> bool {
        self.create_in_flight.is_some()
            || self.cancel_in_flight.is_some()
            || matches!(
                &self.released_update,
                Some(released) if released.awaiting_outcome
            )
    }

    /// The exact create request, while it still needs to be sent.
    ///
    /// The same immutable value is returned for every attempt, so an unknown
    /// outcome is retried as the identical request by construction.
    pub fn create_intent(&mut self) -> Option<OperationIntent> {
        if self.create_acknowledged || self.create_in_flight.is_some() {
            return None;
        }
        if matches!(self.state, RemoteTaskState::Terminal) {
            return None;
        }
        self.create_in_flight = Some(self.create.envelope().operation_id());
        Some(OperationIntent::CreateTask(Arc::clone(&self.create)))
    }

    /// Records one domain fact for this task.
    ///
    /// While the task is `Creating` the fact only enters the local queue. It
    /// is refused outright when it would move its own domain backwards, so an
    /// out-of-order producer fails where it is, not on the wire.
    pub fn enqueue_update(
        &mut self,
        update: TaskDomainUpdate,
    ) -> Result<UpdateAdmission, TaskExecutionError> {
        if matches!(self.state, RemoteTaskState::Terminal) || self.awaiting_terminal_status {
            return Ok(UpdateAdmission::DiscardedTerminal);
        }
        let expected_receipt = self.admit_domain(&update)?;
        self.pending.push_back(PendingUpdate {
            update,
            expected_receipt,
        });
        Ok(UpdateAdmission::Queued)
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
    pub fn enqueue_edge_open(
        &mut self,
        edge: ExchangeEdgeId,
    ) -> Result<UpdateAdmission, TaskExecutionError> {
        if matches!(self.state, RemoteTaskState::Terminal) || self.awaiting_terminal_status {
            return Ok(UpdateAdmission::DiscardedTerminal);
        }
        let version = self
            .domains
            .next_edge_open_version()
            .ok_or(TaskExecutionError::Capacity(
                CapacityBound::EdgeOpenVersions { limit: u32::MAX },
            ))?;
        self.enqueue_update(TaskDomainUpdate::OpenExchangeEdges {
            version,
            edges: vec![edge],
        })
    }

    fn admit_domain(
        &mut self,
        update: &TaskDomainUpdate,
    ) -> Result<TaskDomainReceiptExpectation, TaskExecutionError> {
        let split_watermark = match update {
            TaskDomainUpdate::SplitAssignment(intent) => {
                Some(self.domains.split_watermark(intent.node()))
            }
            _ => None,
        };
        let progression = self.domains.record(update);
        let admitted = match update {
            TaskDomainUpdate::SplitAssignment(intent) => {
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
                        intent,
                        watermark,
                        DomainConflict::NotMonotonic,
                    )),
                    DomainProgression::Conflict(conflict) => {
                        Err(split_regression(intent, watermark, conflict))
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
        };
        admitted.map(|()| self.domains.receipt_expectation(update))
    }

    /// The next update to send, if any may be sent right now.
    ///
    /// One domain change per request and at most one request in flight. That
    /// keeps a receipt attributable to exactly one domain and keeps a slow
    /// task from accumulating unacknowledged work.
    pub fn next_update_intent(&mut self) -> Result<Option<OperationIntent>, TaskExecutionError> {
        if !matches!(self.state, RemoteTaskState::Created) || self.awaiting_terminal_status {
            return Ok(None);
        }
        if let Some(released) = &mut self.released_update {
            if released.awaiting_outcome {
                return Ok(None);
            }
            released.awaiting_outcome = true;
            return Ok(Some(OperationIntent::UpdateTask(Arc::clone(
                &released.request,
            ))));
        }
        let Some(pending) = self.pending.pop_front() else {
            return Ok(None);
        };
        let operation_id = TaskOperationId::new_v7();
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
        Ok(Some(OperationIntent::UpdateTask(request)))
    }

    /// Stands this task down normally, once.
    pub fn cancel_intent(&mut self, reason: CancelReason) -> Option<OperationIntent> {
        if self.cancel_requested
            || matches!(self.state, RemoteTaskState::Terminal)
            || self.awaiting_terminal_status
        {
            return None;
        }
        let operation_id = TaskOperationId::new_v7();
        self.cancel_requested = true;
        self.cancel_in_flight = Some(operation_id);
        Some(OperationIntent::CancelTask(CancelTask::new(
            operation_id,
            self.identity(),
            reason,
        )))
    }

    pub const fn cancel_requested(&self) -> bool {
        self.cancel_requested
    }

    /// Settles the create acknowledgement.
    pub fn on_create_ack(
        &mut self,
        ack: &OperationAcknowledgement,
    ) -> Result<CreateSettlement, TaskExecutionError> {
        if self.create_in_flight != Some(ack.operation_id()) {
            return Err(TaskExecutionError::UnknownOperation);
        }
        self.create_in_flight = None;
        if ack.is_applied() {
            let AckPayload::Create(receipt) = ack.payload() else {
                return Err(TaskExecutionError::MissingReceipt(
                    OperationKind::CreateTask,
                ));
            };
            self.identity().verify_matches(receipt.identity())?;
            self.create_acknowledged = true;
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
        match frontend_action(ack.dispatch_result()) {
            FrontendAction::RetryExactRequest => Ok(CreateSettlement::RetryExactRequest),
            _ => {
                self.enter_terminal();
                Ok(CreateSettlement::FailedClosed(
                    ack.worker_outcome()
                        .expect("a failed Worker receipt has an outcome"),
                ))
            }
        }
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
        let identity = released.request.identity();
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
                if matches!(self.state, RemoteTaskState::Terminal) {
                    // A terminal task owes nothing further, so an unknown
                    // outcome is abandoned rather than replayed at a task that
                    // can no longer apply it.
                    self.released_update = None;
                    self.converged_after_terminal += 1;
                    return Ok(UpdateSettlement::ConvergedAfterTerminal);
                }
                return Ok(UpdateSettlement::RetryExactRequest);
            }
            FrontendAction::StopSendingAndReconcile | FrontendAction::Settled => {
                self.released_update = None;
                self.converged_after_terminal += 1;
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
        self.enter_terminal();
        Ok(UpdateSettlement::FailedClosed(
            ack.worker_outcome()
                .expect("a failed Worker receipt has an outcome"),
        ))
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
        Ok(())
    }

    /// Applies one published status snapshot.
    pub fn observe_status(
        &mut self,
        observed: &TaskStatus,
    ) -> Result<StatusObservation, TaskExecutionError> {
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
        if terminal {
            self.enter_terminal();
        }
        Ok(())
    }

    fn enter_terminal(&mut self) {
        self.state = RemoteTaskState::Terminal;
        self.awaiting_terminal_status = false;
        self.discarded_updates += self.pending.len();
        self.pending.clear();
    }

    fn await_terminal_status(&mut self) {
        self.awaiting_terminal_status = true;
        self.discarded_updates += self.pending.len();
        self.pending.clear();
    }

    /// The terminal facts, once this task has them.
    pub fn terminal_report(&self) -> Option<TaskTerminalReport> {
        if !matches!(self.state, RemoteTaskState::Terminal) {
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

    /// Which domains still hold unsent facts, for diagnostics.
    pub fn pending_domains(&self) -> Vec<TaskDomainKind> {
        self.pending
            .iter()
            .map(|pending| pending.update.kind())
            .collect()
    }
}

/// What happened to one recorded domain fact.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum UpdateAdmission {
    /// The fact is queued and will be sent when this task may send.
    Queued,
    /// The task is terminal, so the fact was discarded.
    DiscardedTerminal,
}
