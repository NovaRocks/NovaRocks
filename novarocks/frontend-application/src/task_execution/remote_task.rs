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

use novarocks_execution::task_execution::{
    CancelReason, CancelTask, DomainConflict, DomainProgression, ExchangeEdgeId, OperationEnvelope,
    OperationKind, OperationOutcome, QueryContextRef, TaskDomainKind, TaskDomainUpdate,
    TaskIdentity, TaskOperationId, TaskState, TaskStatus, TaskStatusCursor, TerminationDetail,
    UpdateTask,
};
use novarocks_query_application::coordination::{
    FrontendAction, GoneObservation, MonotonicInstant, ObservedTaskTransition,
    OperationDispatchResult, StatusObservation, TaskDomainIntentTracker,
    TaskDomainReceiptExpectation, classify_gone, classify_observation,
    classify_observed_task_transition, frontend_action, verify_task_domain_receipt,
};

use super::creation::{
    CreateTaskIntent, CreationLengths, TaskCreationSeed, create_queued_bytes, plan_carrier_bytes,
};
use super::error::{CapacityBound, TaskExecutionError};
use super::intent::{
    AckPayload, OperationAcknowledgement, OperationIntent, TaskOperationQueuePermit,
    TaskOperationQueueRequest,
};

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
struct PendingUpdate {
    update: TaskDomainUpdate,
    expected_receipt: TaskDomainReceiptExpectation,
    queue_permit: Box<dyn TaskOperationQueuePermit>,
}

fn is_destination_close(update: &TaskDomainUpdate) -> bool {
    matches!(update, TaskDomainUpdate::CloseExchangeDestination { .. })
}

/// Where one task's create payload is.
#[derive(Debug)]
enum CreationReplay {
    /// The create was never admitted to a send queue. The seed is still
    /// move-only input; its encoded size is learned at most once and kept, so
    /// a create that waits behind backpressure is not measured again.
    Unfrozen {
        seed: Box<TaskCreationSeed>,
        priced: Option<CreationLengths>,
    },
    /// Frozen once. Every send of this create, including an exact resend after
    /// an unknown outcome, hands out this same intent.
    Frozen(Arc<CreateTaskIntent>),
    /// An exactly correlated success settled the create, so nothing may ever
    /// resend it and the payload was released.
    Settled,
    /// No create will be sent again: the task went terminal, or its create
    /// failed closed.
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
    identity: TaskIdentity,
    context: QueryContextRef,
    create_envelope: OperationEnvelope,
    creation: CreationReplay,
    state: RemoteTaskState,
    create_in_flight: Option<TaskOperationId>,
    create_remote_unknown: bool,
    create_acknowledged: bool,
    create_ownership_proven: bool,
    normal_stand_down: bool,
    fenced_out: bool,
    create_waiting_for_establish: bool,
    create_busy_until: Option<MonotonicInstant>,
    create_busy_rejections: u32,
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
    /// Takes ownership of one task's creation seed.
    ///
    /// The create operation id is minted here, once: every send of this
    /// create carries it, so the acknowledgement of any send correlates.
    pub(crate) fn new(
        seed: TaskCreationSeed,
        outbound_destinations: Vec<(ExchangeEdgeId, TaskIdentity)>,
    ) -> Result<Self, TaskExecutionError> {
        let identity = seed.identity();
        let context = identity
            .verify_query_context(seed.context())
            .map(|()| seed.context())
            .map_err(TaskExecutionError::Identity)?;
        let domains = TaskDomainIntentTracker::new(
            seed.split_plan_nodes().iter().copied(),
            seed.outbound_edges().iter().copied(),
        )
        .with_exchange_destinations(outbound_destinations);
        Ok(Self {
            identity,
            context,
            create_envelope: OperationEnvelope::with_default_wait(
                TaskOperationId::new_v7(),
                OperationKind::CreateTask,
            ),
            creation: CreationReplay::Unfrozen {
                seed: Box::new(seed),
                priced: None,
            },
            state: RemoteTaskState::Creating,
            create_in_flight: None,
            create_remote_unknown: false,
            create_acknowledged: false,
            create_ownership_proven: false,
            normal_stand_down: false,
            fenced_out: false,
            create_waiting_for_establish: false,
            create_busy_until: None,
            create_busy_rejections: 0,
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

    pub fn context(&self) -> QueryContextRef {
        self.context
    }

    /// Whether this task still holds a create payload: a seed it has not
    /// frozen, or frozen parts a resend may still need.
    #[cfg(test)]
    pub(crate) const fn holds_create_payload(&self) -> bool {
        matches!(
            self.creation,
            CreationReplay::Unfrozen { .. } | CreationReplay::Frozen(_)
        )
    }

    /// Whether this task's create was frozen, settled or closed, i.e. has
    /// left its seed.
    #[cfg(test)]
    pub(crate) const fn create_frozen(&self) -> bool {
        !matches!(self.creation, CreationReplay::Unfrozen { .. })
    }

    pub const fn state(&self) -> RemoteTaskState {
        self.state
    }

    /// Whether this frontend has historically accepted the exact CreateTask
    /// receipt for this task.
    ///
    /// Lifecycle state is intentionally not used as a substitute: a task may
    /// move directly from Creating to Terminal when status observation races
    /// ahead of its create acknowledgement, while the later exact receipt
    /// still proves that the task was admitted.
    pub const fn create_acknowledged(&self) -> bool {
        self.create_acknowledged
    }

    /// An exact positive Worker fact proves this Task exists even if its
    /// Create acknowledgement was lost or is still in flight.
    pub const fn create_ownership_proven(&self) -> bool {
        self.create_ownership_proven
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

    /// Stops new Create and update work while preserving a possible in-flight
    /// Create receipt and the identity that the Context fence must classify.
    pub(crate) fn begin_normal_stand_down(&mut self) {
        self.normal_stand_down = true;
        self.create_waiting_for_establish = false;
        self.create_busy_until = None;
        let before = self.pending.len();
        self.pending
            .retain(|pending| is_destination_close(&pending.update));
        self.discarded_updates += before - self.pending.len();
        if self.released_update.as_ref().is_some_and(|released| {
            !released.awaiting_outcome
                && !released.request.domains().iter().all(is_destination_close)
        }) {
            self.released_update = None;
        }
        self.creation = CreationReplay::Closed;
    }

    /// Returns frontend replay backing after the attempt has failed. A sent
    /// transport retains its own carrier until that physical operation exits.
    pub(crate) fn discard_creation_after_attempt_failure(&mut self) {
        self.creation = CreationReplay::Closed;
        self.create_waiting_for_establish = false;
        self.create_busy_until = None;
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
        self.discarded_updates += self.pending.len();
        self.pending.clear();
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

    /// Whether this task still owes a create send.
    pub(crate) fn create_pending(&self) -> bool {
        !self.create_ownership_proven
            && !matches!(self.state, RemoteTaskState::Terminal)
            && matches!(
                self.creation,
                CreationReplay::Unfrozen { .. } | CreationReplay::Frozen(_)
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
                CreationReplay::Unfrozen { .. } | CreationReplay::Frozen(_)
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
        let backend = self.identity.backend_process_id();
        let (fragment, lengths) = match &mut self.creation {
            CreationReplay::Unfrozen { seed, priced } => {
                let lengths = match priced {
                    Some(lengths) => *lengths,
                    None => {
                        let lengths = seed.lengths()?;
                        *priced = Some(lengths);
                        lengths
                    }
                };
                (Arc::clone(seed.fragment()), lengths)
            }
            CreationReplay::Frozen(intent) => (
                Arc::clone(intent.parts().fragment()),
                intent.parts().lengths(),
            ),
            CreationReplay::Settled | CreationReplay::Closed => return Ok(None),
        };
        Ok(Some(CreateCandidate {
            operation_id: self.create_envelope.operation_id(),
            request: TaskOperationQueueRequest::create_task(
                backend,
                create_queued_bytes(self.create_envelope, &fragment, lengths),
            ),
            plan_carrier_bytes: plan_carrier_bytes(&fragment, lengths),
        }))
    }

    /// Releases this task's create for sending, after admission reserved its
    /// capacity.
    ///
    /// The first release freezes the seed exactly once, at the length it was
    /// priced at; every later release hands out the same frozen intent, so an
    /// unknown outcome is retried as the identical request by construction.
    pub(crate) fn release_create(
        &mut self,
        now: MonotonicInstant,
    ) -> Result<OperationIntent, TaskExecutionError> {
        if !self.create_sendable(now) {
            return Err(TaskExecutionError::Schedule(format!(
                "task {} has no create to release",
                self.identity
            )));
        }
        if let CreationReplay::Unfrozen { priced, .. } = &self.creation
            && priced.is_none()
        {
            return Err(TaskExecutionError::Schedule(format!(
                "task {} create was released before it was priced",
                self.identity
            )));
        }
        if matches!(self.creation, CreationReplay::Unfrozen { .. }) {
            let CreationReplay::Unfrozen { seed, priced } =
                std::mem::replace(&mut self.creation, CreationReplay::Closed)
            else {
                unreachable!("checked above");
            };
            let parts = seed.freeze(priced.expect("checked above"))?;
            self.creation = CreationReplay::Frozen(Arc::new(CreateTaskIntent::new(
                self.create_envelope,
                self.identity,
                parts,
            )));
        }
        let CreationReplay::Frozen(intent) = &self.creation else {
            unreachable!("a sendable create is frozen by now");
        };
        self.create_in_flight = Some(self.create_envelope.operation_id());
        Ok(OperationIntent::CreateTask(Arc::clone(intent)))
    }

    /// Records one domain fact for this task.
    ///
    /// While the task is `Creating` the fact only enters the local queue. It
    /// is refused outright when it would move its own domain backwards, so an
    /// out-of-order producer fails where it is, not on the wire.
    pub fn enqueue_update(
        &mut self,
        update: TaskDomainUpdate,
        queue_permit: Box<dyn TaskOperationQueuePermit>,
    ) -> Result<UpdateAdmission, TaskExecutionError> {
        if self.normal_stand_down && !is_destination_close(&update) {
            return Ok(UpdateAdmission::DiscardedTerminal);
        }
        if matches!(self.state, RemoteTaskState::Terminal) || self.awaiting_terminal_status {
            return Ok(UpdateAdmission::DiscardedTerminal);
        }
        let expected_receipt = self.admit_domain(&update)?;
        self.pending.push_back(PendingUpdate {
            update,
            expected_receipt,
            queue_permit,
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
                request: OperationIntent::UpdateTask(Arc::clone(&released.request)).queue_request(),
                holds_queue_permit: false,
            });
        }
        let pending = self.pending.front()?;
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
    pub fn next_update_intent(
        &mut self,
    ) -> Result<
        Option<(OperationIntent, Option<Box<dyn TaskOperationQueuePermit>>)>,
        TaskExecutionError,
    > {
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
            return Ok(Some((
                OperationIntent::UpdateTask(Arc::clone(&released.request)),
                None,
            )));
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
        Ok(Some((
            OperationIntent::UpdateTask(request),
            Some(pending.queue_permit),
        )))
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
    pub fn cancel_intent(&mut self, reason: CancelReason) -> Option<OperationIntent> {
        if self.cancel_requested
            || (self.normal_stand_down && !self.create_ownership_proven)
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
        context_established: bool,
    ) -> Result<CreateSettlement, TaskExecutionError> {
        if self.create_in_flight != Some(ack.operation_id()) {
            return Err(TaskExecutionError::UnknownOperation);
        }
        self.create_in_flight = None;
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
            self.creation = CreationReplay::Settled;
            self.create_acknowledged = true;
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
                    .ok_or(TaskExecutionError::DispatchRejected {
                        kind: OperationKind::CreateTask,
                        result: ack.dispatch_result(),
                    })?;
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
                        self.create_waiting_for_establish = !context_established;
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
                    return Err(TaskExecutionError::DispatchRejected {
                        kind: OperationKind::CreateTask,
                        result: ack.dispatch_result(),
                    });
                };
                self.enter_terminal();
                Ok(CreateSettlement::FailedClosed(outcome))
            }
        }
    }

    /// The exact Establish acknowledgement satisfies a prior NotReady reply.
    pub(crate) fn on_context_established(&mut self) {
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
                    self.converged_after_terminal += 1;
                    return Ok(UpdateSettlement::ConvergedAfterTerminal);
                }
                return Ok(UpdateSettlement::RetryExactRequest);
            }
            FrontendAction::StopSendingAndReconcile | FrontendAction::Settled => {
                self.released_update = None;
                self.converged_after_terminal += 1;
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
            return Err(TaskExecutionError::DispatchRejected {
                kind: OperationKind::UpdateTask,
                result: ack.dispatch_result(),
            });
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
        if ack.worker_outcome().is_none()
            && ack.dispatch_result() != OperationDispatchResult::TransportUnknown
        {
            return Err(TaskExecutionError::DispatchRejected {
                kind: OperationKind::CancelTask,
                result: ack.dispatch_result(),
            });
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
            self.creation = CreationReplay::Settled;
        }
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
        // A terminal task is never created again, so neither its seed nor its
        // frozen parts can be needed. A send already in flight keeps its own
        // handle until it settles.
        if !matches!(self.creation, CreationReplay::Settled) {
            self.creation = CreationReplay::Closed;
        }
    }

    fn await_terminal_status(&mut self) {
        self.awaiting_terminal_status = true;
        self.discarded_updates += self.pending.len();
        self.pending.clear();
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

    /// Which domains still hold unsent facts, for diagnostics.
    pub fn pending_domains(&self) -> Vec<TaskDomainKind> {
        self.pending
            .iter()
            .map(|pending| pending.update.kind())
            .collect()
    }
}

#[cfg(test)]
mod destination_close_tests {
    use super::*;
    use novarocks_execution::task_execution::DomainVersion;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    fn identity(task: u32, backend: BackendProcessId) -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(task).unwrap(),
            backend,
        )
    }

    fn producer_with_two_destinations() -> (RemoteTask, ExchangeEdgeId, TaskIdentity, TaskIdentity)
    {
        let backend = BackendProcessId::new_v7();
        let producer = identity(1, backend);
        let target = identity(2, backend);
        let sibling = identity(3, backend);
        let edge = ExchangeEdgeId::new(1).unwrap();
        let context = QueryContextRef::new(
            producer.query_execution_id(),
            FrontendProcessId::new_v7(),
            backend,
        );
        let domains = TaskDomainIntentTracker::new([], [edge])
            .with_exchange_destinations([(edge, target), (edge, sibling)]);
        (
            RemoteTask {
                identity: producer,
                context,
                create_envelope: OperationEnvelope::with_default_wait(
                    TaskOperationId::new_v7(),
                    OperationKind::CreateTask,
                ),
                creation: CreationReplay::Settled,
                state: RemoteTaskState::Created,
                create_in_flight: None,
                create_remote_unknown: false,
                create_acknowledged: true,
                create_ownership_proven: true,
                normal_stand_down: false,
                fenced_out: false,
                create_waiting_for_establish: false,
                create_busy_until: None,
                create_busy_rejections: 0,
                pending: VecDeque::new(),
                released_update: None,
                cancel_in_flight: None,
                cancel_requested: false,
                status: Some(TaskStatus::created(producer).with_installed()),
                cursor: TaskStatusCursor::unobserved(producer),
                domains,
                awaiting_terminal_status: false,
                discarded_updates: 0,
                converged_after_terminal: 0,
            },
            edge,
            target,
            sibling,
        )
    }

    #[test]
    fn destination_close_rejects_nonmembers_and_versions_each_exact_member() {
        let (mut producer, edge, target, sibling) = producer_with_two_destinations();
        let outsider = identity(4, target.backend_process_id());
        let invalid = producer.prepare_destination_close(edge, outsider).unwrap();
        assert!(matches!(
            producer.enqueue_update(invalid, super::super::intent::test_queue_permit()),
            Err(TaskExecutionError::DomainRegression {
                conflict: DomainConflict::UnknownMember,
                ..
            })
        ));

        let first = producer.prepare_destination_close(edge, target).unwrap();
        assert!(matches!(
            first,
            TaskDomainUpdate::CloseExchangeDestination {
                version: DomainVersion::FIRST,
                destination,
                ..
            } if destination == target
        ));
        assert!(matches!(
            producer.enqueue_update(first, super::super::intent::test_queue_permit()),
            Ok(UpdateAdmission::Queued)
        ));
        let second = producer.prepare_destination_close(edge, sibling).unwrap();
        assert!(matches!(
            second,
            TaskDomainUpdate::CloseExchangeDestination { version, destination, .. }
                if version.get() == 2 && destination == sibling
        ));
        assert!(matches!(
            producer.enqueue_update(second, super::super::intent::test_queue_permit()),
            Ok(UpdateAdmission::Queued)
        ));
    }

    #[test]
    fn destination_close_transport_unknown_replays_the_same_update_arc() {
        let (mut producer, edge, target, _) = producer_with_two_destinations();
        let close = producer.prepare_destination_close(edge, target).unwrap();
        producer
            .enqueue_update(close, super::super::intent::test_queue_permit())
            .unwrap();
        let (first, first_permit) = producer.next_update_intent().unwrap().unwrap();
        assert!(first_permit.is_some());
        let OperationIntent::UpdateTask(first_request) = first else {
            panic!("destination close is a task update");
        };
        assert_eq!(
            producer
                .on_update_ack(&OperationAcknowledgement::transport_unknown(
                    first_request.envelope().operation_id(),
                    OperationKind::UpdateTask,
                ))
                .unwrap(),
            UpdateSettlement::RetryExactRequest
        );
        let (replay, replay_permit) = producer.next_update_intent().unwrap().unwrap();
        assert!(replay_permit.is_none());
        let OperationIntent::UpdateTask(replay_request) = replay else {
            panic!("replay remains a task update");
        };
        assert!(Arc::ptr_eq(&first_request, &replay_request));
        assert_eq!(
            first_request.envelope().operation_id(),
            replay_request.envelope().operation_id()
        );
    }

    #[test]
    fn normal_stand_down_discards_unknown_ordinary_update_before_exact_close() {
        let (mut producer, edge, target, _) = producer_with_two_destinations();
        let open = producer.prepare_edge_open(edge).unwrap();
        producer
            .enqueue_update(open, super::super::intent::test_queue_permit())
            .unwrap();
        let (ordinary, _) = producer.next_update_intent().unwrap().unwrap();
        producer.begin_normal_stand_down();
        let close = producer.prepare_destination_close(edge, target).unwrap();
        producer
            .enqueue_update(close, super::super::intent::test_queue_permit())
            .unwrap();
        assert_eq!(
            producer
                .on_update_ack(&OperationAcknowledgement::transport_unknown(
                    ordinary.operation_id(),
                    OperationKind::UpdateTask,
                ))
                .unwrap(),
            UpdateSettlement::DiscardedAfterNormalStandDown
        );
        let (next, _) = producer.next_update_intent().unwrap().unwrap();
        let OperationIntent::UpdateTask(next) = next else {
            panic!("the exact close follows the obsolete ordinary update");
        };
        assert!(matches!(
            next.domains(),
            [TaskDomainUpdate::CloseExchangeDestination { destination, .. }] if *destination == target
        ));
    }

    #[test]
    fn destination_close_reservation_matches_frozen_control_class() {
        let (mut producer, edge, target, _) = producer_with_two_destinations();
        let close = producer.prepare_destination_close(edge, target).unwrap();
        let preview = TaskOperationQueueRequest::task_update(
            producer.identity().backend_process_id(),
            &close,
        );
        assert!(preview.requires_control_progress());
        assert_eq!(
            preview.lane(),
            novarocks_query_application::coordination::DispatchLane::Update
        );
        producer
            .enqueue_update(close, super::super::intent::test_queue_permit())
            .unwrap();
        let (frozen, _) = producer.next_update_intent().unwrap().unwrap();
        assert_eq!(preview, frozen.queue_request());
        assert!(
            producer.next_update_intent().unwrap().is_none(),
            "control reserve does not bypass the Task's single in-flight Update"
        );

        let (mut ordinary_producer, edge, _, _) = producer_with_two_destinations();
        let open = ordinary_producer.prepare_edge_open(edge).unwrap();
        let ordinary_preview = TaskOperationQueueRequest::task_update(
            ordinary_producer.identity().backend_process_id(),
            &open,
        );
        assert!(!ordinary_preview.requires_control_progress());
        ordinary_producer
            .enqueue_update(open, super::super::intent::test_queue_permit())
            .unwrap();
        let (ordinary_frozen, _) = ordinary_producer.next_update_intent().unwrap().unwrap();
        assert_eq!(ordinary_preview, ordinary_frozen.queue_request());
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
