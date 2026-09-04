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

//! The split-assignment delivery bridge.
//!
//! `SplitAssignmentDriver` delivers splits through a synchronous port and runs
//! the ADR-0123 retry machinery around it: it holds the immutable request
//! until a strict Accepted acknowledgement, resends only that identical
//! request after a genuinely unknown outcome, bounds each attempt and the
//! total error duration by frozen config, and interrupts its backoff on round
//! stop. The task protocol is the opposite shape: `QueryTaskExecution` is a
//! single-owner state machine that batches operations and settles them later
//! from acknowledgements.
//!
//! This module bridges the two without reimplementing either. Nothing here
//! decides whether a request may be resent, how long to wait, or when a round
//! ends -- the driver already owns all of that, and
//! [`super::split_domain`] owns the address and verdict vocabulary the two
//! sides share.
//!
//! It is two halves over one shared state, because `QueryTaskExecution` takes
//! `&mut self` and belongs to the round runner while the driver runs on its
//! own blocking worker thread:
//!
//! - the transport half ([`TaskUpdateTransport`]) translates one request into
//!   task-domain updates, hands them to the shared state, and blocks its
//!   caller until that submission settles, its timeout elapses, or the round
//!   is abandoned;
//! - the owner half ([`SplitDeliveryBridge::take_pending`],
//!   [`SplitDeliveryBridge::admit`], [`SplitDeliveryBridge::settle`]) is what
//!   the round runner calls on its own turn.
//!
//! The binding from a substrate operation to a blocked sender is established
//! by [`SplitDeliverySink`], a pass-through observer over the operation sink.
//! A released `UpdateTask` is the only place where an operation id and the
//! split domain it carries are both visible, and a failed-closed
//! acknowledgement carries no receipt, so routing by the acknowledgement's own
//! identity would strand exactly the senders that must not keep waiting.
//!
//! The acknowledgement itself reaches the owner half through
//! [`super::round::AcknowledgementObserver`]: the round runner is the only
//! thing that drains acknowledgements, so it is the only place a verdict for a
//! blocked sender can be learned.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use novarocks_execution::task_execution::{
    ContentFingerprint, DomainProgression, FrontendAction, OperationKind, PlanNodeId,
    SplitAssignmentIntent, SplitSequence, TaskDomainReceipt, TaskDomainUpdate, TaskOperationId,
    UpdateTaskReceipt,
};
use novarocks_proto_codec::lifecycle::QueryExecutionId;
use novarocks_proto_codec::task_execution::domain::wire_split_assignment;
use novarocks_types::UniqueId;
use novarocks_types::identity::TaskId;

use super::error::TaskExecutionError;
use super::graph::TaskGraph;
use super::intent::{
    AckPayload, DispatchBatch, OperationAcknowledgement, OperationIntent, TaskOperationSink,
};
use super::remote_task::UpdateAdmission;
use crate::query_execution::connector_domain::{SplitAssignment, TaskUpdateRequest};
use crate::query_execution::split_assignment::{
    AcceptedPlanNode, AssignmentTarget, SplitAssignmentStop, TaskUpdateOutcome,
    TaskUpdateTransport, TaskUpdateTransportError,
};

/// One submission a blocked sender produced, as this bridge addresses it.
///
/// It is local to the bridge: the substrate's own operation id is minted later,
/// when the task is allowed to send.
#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct DeliveryId(u64);

/// Why the owner half refused a call.
///
/// Every variant is a frontend bug rather than a query outcome, which is why
/// none of them is reported to the driver as a delivery result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SplitDeliveryError {
    /// The named submission is not live, so nothing can be recorded about it.
    UnknownDelivery(DeliveryId),
    /// An acknowledgement bound to a split submission is not a task update.
    NotATaskUpdate(OperationKind),
}

impl fmt::Display for SplitDeliveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownDelivery(delivery) => {
                write!(formatter, "split delivery {} is not live", delivery.0)
            }
            Self::NotATaskUpdate(kind) => write!(
                formatter,
                "a split delivery was acknowledged as {kind:?} rather than a task update"
            ),
        }
    }
}

impl std::error::Error for SplitDeliveryError {}

/// One submission the owner half has to hand to the substrate.
#[derive(Clone, Debug)]
pub(crate) struct PendingSplitDelivery {
    delivery: DeliveryId,
    task: TaskId,
    update: TaskDomainUpdate,
}

impl PendingSplitDelivery {
    pub(crate) const fn delivery(&self) -> DeliveryId {
        self.delivery
    }

    pub(crate) const fn task(&self) -> TaskId {
        self.task
    }

    /// The exact domain update to record with
    /// `QueryTaskExecution::enqueue_task_update`.
    pub(crate) fn into_update(self) -> TaskDomainUpdate {
        self.update
    }
}

/// What one [`SplitDeliveryBridge::settle`] call did.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum SettleVerdict {
    /// The operation belongs to another owner. Split delivery is one of
    /// several task-update producers, so this is normal, not an error.
    Ignored,
    /// The submission is finished; its sender has its result.
    Settled,
    /// The outcome is unknown, so the submission stays live and the substrate
    /// keeps the identical request to release again.
    Retained,
}

/// What one submission is waiting for, or what it got.
#[derive(Clone, Debug)]
enum DeliveryOutcome {
    Accepted(Vec<AcceptedPlanNode>),
    /// The backend decided about this attempt. Never resent.
    Rejected {
        reason: String,
        detail: String,
    },
    /// No backend outcome was established, and resending would not establish
    /// one either.
    Local(String),
    /// The destination finished before this update reached it.
    DestinationFinished(String),
    /// The outcome is genuinely unknown, so the identical request may be
    /// resent.
    Unknown(String),
}

/// One live submission.
#[derive(Debug)]
struct Delivery {
    task: TaskId,
    /// Kept after the owner takes it, so a resent `send` can be proved to be
    /// the identical request rather than a second one wearing the same task.
    update: TaskDomainUpdate,
    outcome: Option<DeliveryOutcome>,
}

#[derive(Debug, Default)]
struct DeliveryState {
    next_delivery: u64,
    live: BTreeMap<DeliveryId, Delivery>,
    /// Submissions the owner half has not taken yet, in production order.
    queued: VecDeque<DeliveryId>,
    /// At most one live submission per task. The driver already keeps one
    /// update in flight per task; recording it here turns a violation of that
    /// rule into a refusal instead of two updates racing for one sequence
    /// space.
    by_task: BTreeMap<TaskId, DeliveryId>,
    /// Substrate operations that carry a live submission.
    by_operation: BTreeMap<TaskOperationId, DeliveryId>,
    /// Set once the round is abandoned. A submission produced afterwards is
    /// refused rather than queued for an owner that will never take it.
    abandoned: Option<String>,
}

/// The shared state both halves own.
#[derive(Debug)]
pub(crate) struct SplitDeliveryBridge {
    /// Frozen with the graph, before the substrate takes ownership of it.
    tasks: BTreeMap<UniqueId, TaskId>,
    state: Mutex<DeliveryState>,
    /// Woken by [`SplitDeliveryBridge::settle`],
    /// [`SplitDeliveryBridge::admit`] and
    /// [`SplitDeliveryBridge::abandon`]. A sender never sleeps on a poll
    /// interval: it waits once for the whole remaining timeout and is woken
    /// early by whichever of those calls resolves it.
    settled: Condvar,
}

impl SplitDeliveryBridge {
    /// Builds the bridge over one attempt's frozen graph.
    ///
    /// The kernel-key index is taken here, before the substrate takes the
    /// graph, and it comes from [`super::split_domain::task_kernel_index`] so
    /// the driver's addresses are resolved by the one module that owns that
    /// translation.
    pub(crate) fn for_graph(graph: &TaskGraph) -> Arc<Self> {
        Self::with_tasks(super::split_domain::task_kernel_index(graph))
    }

    fn with_tasks(tasks: BTreeMap<UniqueId, TaskId>) -> Arc<Self> {
        Arc::new(Self {
            tasks,
            state: Mutex::new(DeliveryState::default()),
            settled: Condvar::new(),
        })
    }

    /// Wraps the substrate's operation sink so released operations bind to the
    /// submissions they carry.
    pub(crate) fn sink(
        self: &Arc<Self>,
        inner: Arc<dyn TaskOperationSink>,
    ) -> Arc<dyn TaskOperationSink> {
        Arc::new(SplitDeliverySink {
            bridge: Arc::clone(self),
            inner,
        })
    }

    /// Every submission produced since the last call, in production order.
    ///
    /// The owner records each one with
    /// `QueryTaskExecution::enqueue_task_update` and then reports what the
    /// substrate did through [`Self::admit`].
    pub(crate) fn take_pending(&self) -> Vec<PendingSplitDelivery> {
        let mut state = self.lock();
        let queued = std::mem::take(&mut state.queued);
        queued
            .into_iter()
            .map(|delivery| {
                // Queued ids are retired together with their submission, so a
                // missing one would mean a blocked sender whose submission
                // this call has just dropped on the floor.
                let live = state
                    .live
                    .get(&delivery)
                    .expect("a queued submission is live");
                PendingSplitDelivery {
                    delivery,
                    task: live.task,
                    update: live.update.clone(),
                }
            })
            .collect()
    }

    /// Records what the substrate did with a taken submission.
    ///
    /// This must be called for every submission [`Self::take_pending`] handed
    /// out. A submission the substrate refused would otherwise keep its sender
    /// blocked until the driver's per-attempt timeout and then be resent,
    /// which is exactly the "retry a rejection" the delivery contract forbids.
    pub(crate) fn admit(
        &self,
        delivery: DeliveryId,
        admitted: Result<UpdateAdmission, &TaskExecutionError>,
    ) -> Result<(), SplitDeliveryError> {
        let outcome = match admitted {
            Ok(UpdateAdmission::Queued) => return self.expect_live(delivery),
            // The destination is terminal, so the fact was dropped before it
            // could reach a backend. Nothing was established remotely and
            // resending cannot change that.
            Ok(UpdateAdmission::DiscardedTerminal) => DeliveryOutcome::Local(format!(
                "task update discarded: task {} is terminal",
                self.task_of(delivery)?
            )),
            Err(error) => {
                DeliveryOutcome::Local(format!("task update refused by the frontend: {error}"))
            }
        };
        self.resolve(delivery, outcome)
    }

    /// Settles the submission that `operation_id` carried.
    ///
    /// The verdict comes from the acknowledgement itself: `is_applied` decides
    /// whether the backend applied the operation and `frontend_action` decides
    /// whether an unapplied one may be resent. That is the same order
    /// `RemoteTask::on_update_ack` uses, so the two owners cannot drift on
    /// which failures are replayable.
    pub(crate) fn settle(
        &self,
        operation_id: TaskOperationId,
        ack: &OperationAcknowledgement,
    ) -> Result<SettleVerdict, SplitDeliveryError> {
        let mut state = self.lock();
        let Some(&delivery) = state.by_operation.get(&operation_id) else {
            return Ok(SettleVerdict::Ignored);
        };
        if ack.kind() != OperationKind::UpdateTask {
            return Err(SplitDeliveryError::NotATaskUpdate(ack.kind()));
        }
        let outcome = if ack.is_applied() {
            match applied_nodes(ack.payload()) {
                Ok(nodes) => DeliveryOutcome::Accepted(nodes),
                Err(detail) => DeliveryOutcome::Local(detail),
            }
        } else if matches!(
            ack.outcome().frontend_action(),
            FrontendAction::RetryExactRequest
        ) {
            DeliveryOutcome::Unknown(format!("task update outcome unknown: {:?}", ack.outcome()))
        } else if matches!(
            ack.outcome().frontend_action(),
            FrontendAction::StopSendingAndReconcile | FrontendAction::Settled
        ) {
            // The destination is gone or already finished. That is not this
            // producer's failure: it held a status older than the terminal by
            // construction. Delivery stops for that one destination and the
            // round keeps serving the rest.
            DeliveryOutcome::DestinationFinished(format!(
                "the task update was answered with {:?}",
                ack.outcome()
            ))
        } else {
            // Every remaining action -- fail closed, fail the attempt, or stop
            // sending and reconcile -- is a decision about this attempt that
            // must not be resent. The exact outcome travels in `reason`, so
            // none of them is folded into another.
            DeliveryOutcome::Rejected {
                reason: format!("{:?}", ack.outcome()),
                detail: format!("the task update was answered with {:?}", ack.outcome()),
            }
        };
        let retained = matches!(outcome, DeliveryOutcome::Unknown(_));
        let Some(live) = state.live.get_mut(&delivery) else {
            return Err(SplitDeliveryError::UnknownDelivery(delivery));
        };
        live.outcome = Some(outcome);
        if !retained {
            // The substrate cleared its own released request, so this
            // operation id can never come back for this submission.
            state.by_operation.remove(&operation_id);
        }
        drop(state);
        self.settled.notify_all();
        Ok(if retained {
            SettleVerdict::Retained
        } else {
            SettleVerdict::Settled
        })
    }

    /// Abandons the round, waking every blocked sender with an unknown
    /// outcome.
    ///
    /// The round owner calls this where it stops the round. A stopped round is
    /// the driver's own verdict to reach -- it re-reads its stop handle before
    /// it resends -- so this deliberately reports an unknown outcome rather
    /// than a local close it is not entitled to claim.
    pub(crate) fn abandon(&self, reason: impl Into<String>) {
        let reason = reason.into();
        {
            let mut state = self.lock();
            if state.abandoned.is_none() {
                state.abandoned = Some(reason.clone());
            }
            state.queued.clear();
            state.by_operation.clear();
            for live in state.live.values_mut() {
                live.outcome = Some(DeliveryOutcome::Unknown(reason.clone()));
            }
        }
        self.settled.notify_all();
    }

    fn note_released(&self, batch: &DispatchBatch) {
        let mut state = self.lock();
        for operation in batch.operations() {
            let OperationIntent::UpdateTask(request) = operation else {
                continue;
            };
            if !request
                .domains()
                .iter()
                .any(|domain| matches!(domain, TaskDomainUpdate::SplitAssignment(_)))
            {
                continue;
            }
            let task = request.identity().task_id();
            let operation_id = request.envelope().operation_id();
            match state.by_task.get(&task).copied() {
                Some(delivery) => {
                    // A replayed release repeats its own operation id, so this
                    // is idempotent by construction.
                    state.by_operation.insert(operation_id, delivery);
                }
                None => tracing::warn!(
                    task = %task,
                    "a split assignment was released for a task this bridge holds no delivery for"
                ),
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, DeliveryState> {
        self.state.lock().expect("split delivery state lock")
    }

    fn task_of(&self, delivery: DeliveryId) -> Result<TaskId, SplitDeliveryError> {
        self.lock()
            .live
            .get(&delivery)
            .map(|live| live.task)
            .ok_or(SplitDeliveryError::UnknownDelivery(delivery))
    }

    fn expect_live(&self, delivery: DeliveryId) -> Result<(), SplitDeliveryError> {
        if self.lock().live.contains_key(&delivery) {
            return Ok(());
        }
        Err(SplitDeliveryError::UnknownDelivery(delivery))
    }

    fn resolve(
        &self,
        delivery: DeliveryId,
        outcome: DeliveryOutcome,
    ) -> Result<(), SplitDeliveryError> {
        {
            let mut state = self.lock();
            let Some(live) = state.live.get_mut(&delivery) else {
                return Err(SplitDeliveryError::UnknownDelivery(delivery));
            };
            live.outcome = Some(outcome);
        }
        self.settled.notify_all();
        Ok(())
    }
}

impl DeliveryState {
    /// Attaches to the task's live submission, or opens a new one.
    ///
    /// A resent request must reach the submission the substrate already holds:
    /// building a second update for the same splits would offer sequences the
    /// task already accepted, which its own domain admission refuses as a
    /// regression.
    fn attach_or_open(
        &mut self,
        task: TaskId,
        update: TaskDomainUpdate,
    ) -> Result<DeliveryId, String> {
        if let Some(reason) = &self.abandoned {
            return Err(format!("split assignment round abandoned: {reason}"));
        }
        if let Some(&delivery) = self.by_task.get(&task) {
            let live = self
                .live
                .get(&delivery)
                .expect("a task index entry names a live delivery");
            if !same_split_update(&live.update, &update) {
                return Err(format!(
                    "task {task} already has a different split assignment in flight"
                ));
            }
            return Ok(delivery);
        }
        self.next_delivery += 1;
        let delivery = DeliveryId(self.next_delivery);
        self.live.insert(
            delivery,
            Delivery {
                task,
                update,
                outcome: None,
            },
        );
        self.by_task.insert(task, delivery);
        self.queued.push_back(delivery);
        Ok(delivery)
    }

    /// Takes a resolved outcome, retiring the submission unless the outcome
    /// leaves the identical request live.
    fn take_outcome(&mut self, delivery: DeliveryId) -> Option<DeliveryOutcome> {
        let live = self.live.get_mut(&delivery)?;
        let outcome = live.outcome.take()?;
        if matches!(outcome, DeliveryOutcome::Unknown(_)) {
            return Some(outcome);
        }
        let task = live.task;
        self.live.remove(&delivery);
        self.by_task.remove(&task);
        self.by_operation.retain(|_, bound| *bound != delivery);
        Some(outcome)
    }
}

impl TaskUpdateTransport for SplitDeliveryBridge {
    fn send(
        &self,
        _execution_id: QueryExecutionId,
        target: &AssignmentTarget,
        request: &TaskUpdateRequest,
        timeout: Duration,
        stop: &SplitAssignmentStop,
    ) -> Result<TaskUpdateOutcome, TaskUpdateTransportError> {
        let task = *self
            .tasks
            .get(&target.fragment_instance_id)
            .ok_or_else(|| {
                TaskUpdateTransportError::fatal(format!(
                    "fragment instance {:x}:{:x} is not a task of this attempt",
                    target.fragment_instance_id.high(),
                    target.fragment_instance_id.low(),
                ))
            })?;
        let update = task_update(request).map_err(TaskUpdateTransportError::fatal)?;

        let deadline = Instant::now() + timeout;
        let mut state = self.lock();
        let delivery = state
            .attach_or_open(task, update)
            .map_err(TaskUpdateTransportError::fatal)?;
        loop {
            if let Some(outcome) = state.take_outcome(delivery) {
                return delivered(outcome);
            }
            // The round's stop handle has its own condition variable, which
            // this bridge cannot be woken by. `abandon` is the prompt path;
            // this check keeps a stop that only reached the driver correct,
            // and the driver bounds the wait by the same per-attempt timeout
            // it bounds every delivery attempt with.
            if stop.is_stopped() {
                return Err(TaskUpdateTransportError::retryable_network(
                    "split assignment round stopped while a task update was outstanding",
                ));
            }
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return Err(TaskUpdateTransportError::retryable_network(format!(
                    "task update to backend {} was not acknowledged within {} ms",
                    target.backend_idx,
                    timeout.as_millis(),
                )));
            };
            let (guard, _) = self
                .settled
                .wait_timeout(state, remaining)
                .expect("split delivery condvar");
            state = guard;
        }
    }
}

fn delivered(outcome: DeliveryOutcome) -> Result<TaskUpdateOutcome, TaskUpdateTransportError> {
    match outcome {
        DeliveryOutcome::Accepted(nodes) => Ok(TaskUpdateOutcome::Accepted(nodes)),
        DeliveryOutcome::Rejected { reason, detail } => {
            Ok(TaskUpdateOutcome::Rejected { reason, detail })
        }
        DeliveryOutcome::DestinationFinished(detail) => {
            Ok(TaskUpdateOutcome::DestinationFinished { detail })
        }
        DeliveryOutcome::Local(detail) => Err(TaskUpdateTransportError::fatal(detail)),
        DeliveryOutcome::Unknown(detail) => {
            Err(TaskUpdateTransportError::retryable_network(detail))
        }
    }
}

impl super::round::AcknowledgementObserver for SplitDeliveryBridge {
    /// Settles whatever submission this acknowledgement carried.
    ///
    /// Most acknowledgements carry none -- creates, lifecycle operations and
    /// task updates from other producers -- and [`Self::settle`] reports that
    /// as `Ignored` rather than as an error, because split delivery is one of
    /// several task-update producers.
    fn observe_acknowledgement(&self, ack: &OperationAcknowledgement) -> Result<(), String> {
        self.settle(ack.operation_id(), ack)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

/// The pass-through observer that binds released operations to submissions.
#[derive(Debug)]
struct SplitDeliverySink {
    bridge: Arc<SplitDeliveryBridge>,
    inner: Arc<dyn TaskOperationSink>,
}

impl TaskOperationSink for SplitDeliverySink {
    fn submit(&self, batch: &DispatchBatch) {
        // Bind before forwarding: the acknowledgement of a request this batch
        // puts on the wire may reach `settle` on another thread before
        // `submit` returns.
        self.bridge.note_released(batch);
        self.inner.submit(batch);
    }
}

/// Translates one immutable request into the task-domain update it carries.
///
/// The driver builds exactly one assignment per request and its own Accepted
/// validation refuses an acknowledgement for any other shape, so anything else
/// is refused here rather than split across operations whose acknowledgements
/// could not be attributed to one delivery.
fn task_update(request: &TaskUpdateRequest) -> Result<TaskDomainUpdate, String> {
    let [assignment] = request.assignments() else {
        return Err(format!(
            "a task update must carry exactly one assignment, not {}",
            request.assignments().len()
        ));
    };
    let node = PlanNodeId::new(assignment.plan_node_id())
        .map_err(|error| format!("plan node {}: {error}", assignment.plan_node_id()))?;
    let (first, last) = split_range(assignment)?;
    let proto = request
        .to_proto_assignments()?
        .into_iter()
        .next()
        .ok_or_else(|| "a validated assignment produced no wire assignment".to_owned())?;
    let intent = SplitAssignmentIntent::new(
        node,
        first,
        last,
        assignment.no_more_splits(),
        wire_split_assignment(proto),
    )
    .ok_or_else(|| format!("split batch sequences {first}..{last} of plan node {node} descend"))?;
    Ok(TaskDomainUpdate::SplitAssignment(intent))
}

/// The batch's sequence range in the protocol's own sequence space.
///
/// This reproduces `decode_task_domain` exactly, including its rule that an
/// assignment with no splits is the standalone terminal marker. Deriving the
/// range differently here would let the frontend and the backend classify the
/// same wire assignment against different watermarks.
fn split_range(assignment: &SplitAssignment) -> Result<(SplitSequence, SplitSequence), String> {
    let splits = assignment.splits();
    match (splits.first(), splits.last()) {
        (Some(first), Some(last)) => {
            let sequence = |raw: u64| {
                SplitSequence::new(raw).map_err(|error| {
                    format!(
                        "plan node {} split sequence {raw}: {error}",
                        assignment.plan_node_id()
                    )
                })
            };
            Ok((
                sequence(first.sequence_id())?,
                sequence(last.sequence_id())?,
            ))
        }
        _ => {
            if !assignment.no_more_splits() {
                return Err(format!(
                    "plan node {} assignment carries no splits and no terminal marker",
                    assignment.plan_node_id()
                ));
            }
            Ok((SplitSequence::FIRST, SplitSequence::FIRST))
        }
    }
}

/// The watermarks an applied acknowledgement reports, as the driver reads
/// them.
fn applied_nodes(payload: &AckPayload) -> Result<Vec<AcceptedPlanNode>, String> {
    let AckPayload::Update(receipt) = payload else {
        return Err("an applied task update carried no update receipt".to_owned());
    };
    let nodes = split_receipt_nodes(receipt)?;
    if nodes.is_empty() {
        return Err("an applied split assignment carried no split receipt".to_owned());
    }
    Ok(nodes)
}

fn split_receipt_nodes(receipt: &UpdateTaskReceipt) -> Result<Vec<AcceptedPlanNode>, String> {
    let mut accepted = Vec::new();
    for domain in receipt.domains() {
        let TaskDomainReceipt::SplitAssignment { nodes, progression } = domain else {
            continue;
        };
        if !matches!(
            progression,
            DomainProgression::Apply | DomainProgression::Idempotent
        ) {
            return Err(format!(
                "an applied split assignment reported progression {progression:?}"
            ));
        }
        for node in nodes {
            // A watermark this bridge admitted is nonzero, so zero is the
            // exact encoding of "accepted nothing" in the driver's own field
            // and can never be mistaken for a sequence it issued.
            let accepted_through_sequence = node
                .watermark()
                .accepted_through()
                .map_or(0, SplitSequence::get);
            // A queue depth the backend did not report is not a queue depth of
            // zero: reporting zero would tell the driver this task has nothing
            // outstanding and disable the backpressure it uses to stop
            // pulling.
            let queued_splits = node.queued_splits().ok_or_else(|| {
                format!(
                    "the split receipt of plan node {} reports no queue depth",
                    node.node()
                )
            })?;
            accepted.push(AcceptedPlanNode {
                plan_node_id: node.node().get(),
                accepted_through_sequence,
                no_more_splits: node.watermark().no_more_splits(),
                queued_splits,
            });
        }
    }
    Ok(accepted)
}

/// Whether two updates are the same immutable split assignment.
fn same_split_update(held: &TaskDomainUpdate, offered: &TaskDomainUpdate) -> bool {
    let (TaskDomainUpdate::SplitAssignment(held), TaskDomainUpdate::SplitAssignment(offered)) =
        (held, offered)
    else {
        return false;
    };
    held.node() == offered.node()
        && held.first() == offered.first()
        && held.last() == offered.last()
        && held.no_more_splits() == offered.no_more_splits()
        && fingerprint(held) == fingerprint(offered)
}

fn fingerprint(intent: &SplitAssignmentIntent) -> ContentFingerprint {
    intent.payload().fingerprint()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use novarocks_execution::task_execution::{
        DispatchLane, DomainProgression, OperationOutcome, PlanNodeSplitReceipt, SplitWatermark,
        TaskIdentity, UpdateTask,
    };
    use novarocks_proto_codec::connector_read::{ConnectorReadCodecError, ConnectorReadEncoder};
    use novarocks_types::identity::{BackendProcessId, StageId};
    use novarocks_types::{AttemptId, QueryId};

    use crate::query_execution::connector_domain::PlanNodeAssignmentState;

    use super::*;

    const SCAN_NODE: i32 = 5;
    const OTHER_SCAN_NODE: i32 = 6;
    const FINST: UniqueId = UniqueId::new(0x11, 0x22);

    /// A split source is never enumerated by these tests, so only the encoder
    /// methods a terminal marker reaches may be reachable.
    struct TerminalOnlyEncoder;

    impl ConnectorReadEncoder for TerminalOnlyEncoder {
        fn owner(&self) -> &str {
            "split-delivery-test"
        }

        fn encode_relation(
            &self,
            _relation: &novarocks_spi::connector::read_stack::ConnectorReadRelation,
        ) -> Result<
            novarocks_proto_models::connector_read::CatalogTableHandle,
            ConnectorReadCodecError,
        > {
            unreachable!("split delivery tests never encode a relation")
        }

        fn encode_column(
            &self,
            _column: &novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
        ) -> Result<novarocks_proto_models::connector_read::ColumnHandle, ConnectorReadCodecError>
        {
            unreachable!("split delivery tests never encode a column")
        }

        fn encode_transaction(
            &self,
            _transaction: &novarocks_spi::connector::read_stack::ConnectorReadTransactionHandle,
        ) -> Result<
            novarocks_proto_models::connector_read::ConnectorTransactionHandle,
            ConnectorReadCodecError,
        > {
            unreachable!("split delivery tests never encode a transaction")
        }

        fn encode_split(
            &self,
            _split: &novarocks_spi::connector::read_stack::ConnectorReadSplit,
        ) -> Result<novarocks_proto_models::connector_read::ConnectorSplit, ConnectorReadCodecError>
        {
            unreachable!("split delivery tests never enumerate a split")
        }
    }

    #[derive(Debug, Default)]
    struct CountingSink {
        batches: AtomicUsize,
    }

    impl TaskOperationSink for CountingSink {
        fn submit(&self, _batch: &DispatchBatch) {
            self.batches.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn task_id() -> TaskId {
        TaskId::new(1).expect("a nonzero task id")
    }

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(3, 4),
            AttemptId::new(1).expect("a nonzero attempt id"),
        )
        .expect("a valid execution id")
    }

    fn identity() -> TaskIdentity {
        TaskIdentity::new(
            execution_id(),
            StageId::new(1).expect("a nonzero stage id"),
            task_id(),
            BackendProcessId::new_v7(),
        )
    }

    fn bridge() -> Arc<SplitDeliveryBridge> {
        SplitDeliveryBridge::with_tasks(BTreeMap::from([(FINST, task_id())]))
    }

    fn target() -> AssignmentTarget {
        AssignmentTarget {
            backend_idx: 2,
            fragment_instance_id: FINST,
        }
    }

    /// One terminal-only request, which is the shape a task that received no
    /// work in the final batch is told to finish with.
    fn terminal_request(no_more_splits: bool) -> TaskUpdateRequest {
        terminal_request_for(SCAN_NODE, no_more_splits)
    }

    fn terminal_request_for(plan_node_id: i32, no_more_splits: bool) -> TaskUpdateRequest {
        let assignment = PlanNodeAssignmentState::new()
            .assign(
                plan_node_id,
                Vec::new(),
                no_more_splits,
                Arc::new(TerminalOnlyEncoder),
            )
            .expect("an empty assignment is legal");
        TaskUpdateRequest::new(FINST, vec![assignment])
    }

    fn accepted_ack(
        operation_id: TaskOperationId,
        receipt: PlanNodeSplitReceipt,
        progression: DomainProgression,
    ) -> OperationAcknowledgement {
        OperationAcknowledgement::new(
            operation_id,
            OperationKind::UpdateTask,
            OperationOutcome::Accepted,
            AckPayload::Update(UpdateTaskReceipt::new(
                identity(),
                vec![TaskDomainReceipt::SplitAssignment {
                    nodes: vec![receipt],
                    progression,
                }],
            )),
        )
    }

    fn failed_ack(
        operation_id: TaskOperationId,
        outcome: OperationOutcome,
    ) -> OperationAcknowledgement {
        OperationAcknowledgement::new(
            operation_id,
            OperationKind::UpdateTask,
            outcome,
            AckPayload::None,
        )
    }

    /// Runs `send` on another thread so the owner half can act while it
    /// blocks, and returns the join handle.
    fn spawn_send(
        bridge: &Arc<SplitDeliveryBridge>,
        request: TaskUpdateRequest,
        timeout: Duration,
        stop: SplitAssignmentStop,
    ) -> std::thread::JoinHandle<Result<TaskUpdateOutcome, TaskUpdateTransportError>> {
        let bridge = Arc::clone(bridge);
        std::thread::spawn(move || bridge.send(execution_id(), &target(), &request, timeout, &stop))
    }

    /// Waits until the sender thread has produced its submission.
    ///
    /// Bounded rather than unbounded so a bridge that never queues fails the
    /// test instead of hanging the suite.
    fn await_pending(bridge: &Arc<SplitDeliveryBridge>) -> Vec<PendingSplitDelivery> {
        for _ in 0..100_000 {
            let pending = bridge.take_pending();
            if !pending.is_empty() {
                return pending;
            }
            std::thread::yield_now();
        }
        panic!("the sender never produced a submission")
    }

    /// Takes the pending submission and releases it the way the owner half
    /// and the substrate would, so its operation id binds to it.
    fn release_pending(bridge: &Arc<SplitDeliveryBridge>) -> TaskOperationId {
        let pending = await_pending(bridge);
        let [delivery] = pending.as_slice() else {
            panic!("exactly one submission is pending, found {}", pending.len());
        };
        assert_eq!(
            delivery.task(),
            task_id(),
            "the kernel key must resolve to the task the owner enqueues against"
        );
        bridge
            .admit(delivery.delivery(), Ok(UpdateAdmission::Queued))
            .expect("a queued admission");
        let operation_id = TaskOperationId::new_v7();
        let request = UpdateTask::try_new(
            operation_id,
            identity(),
            vec![delivery.clone().into_update()],
        )
        .expect("a legal task update");
        let sink = bridge.sink(Arc::new(CountingSink::default()));
        sink.submit(&DispatchBatch::new(
            identity().backend_process_id(),
            DispatchLane::Update,
            vec![OperationIntent::UpdateTask(Arc::new(request))],
            0,
        ));
        operation_id
    }

    #[test]
    fn an_unacknowledged_task_update_times_out_as_an_unknown_remote_outcome() {
        // A timeout must be retryable: the submission may already have reached
        // the backend, so failing it closed would drop the splits it carries.
        let bridge = bridge();
        let error = bridge
            .send(
                execution_id(),
                &target(),
                &terminal_request(true),
                Duration::from_millis(1),
                &SplitAssignmentStop::default(),
            )
            .expect_err("nothing settled this submission");
        assert_eq!(
            error.kind(),
            crate::query_execution::split_assignment::TaskUpdateTransportErrorKind::RetryableNetwork
        );
    }

    #[test]
    fn an_abandoned_round_reports_an_unknown_outcome_rather_than_a_local_close() {
        // Only the driver may conclude that its round is closed; it re-reads
        // its own stop handle before it resends. A transport that claimed
        // `Closed` here would also forbid the identical resend the delivery
        // contract requires.
        let bridge = bridge();
        let handle = spawn_send(
            &bridge,
            terminal_request(true),
            Duration::from_secs(30),
            SplitAssignmentStop::default(),
        );
        await_pending(&bridge);
        bridge.abandon("round replaced");
        let error = handle
            .join()
            .expect("the sender thread joins")
            .expect_err("an abandoned round settles nothing");
        assert_eq!(
            error.kind(),
            crate::query_execution::split_assignment::TaskUpdateTransportErrorKind::RetryableNetwork
        );
        assert_eq!(error.detail(), "round replaced");
    }

    #[test]
    fn a_stopped_round_does_not_leave_a_sender_waiting_for_a_settlement() {
        // The round stop reaches the driver, not this bridge's condition
        // variable. A sender must still observe it rather than wait out the
        // whole error budget.
        let bridge = bridge();
        let stop = SplitAssignmentStop::default();
        stop.stop();
        let error = bridge
            .send(
                execution_id(),
                &target(),
                &terminal_request(true),
                Duration::from_secs(30),
                &stop,
            )
            .expect_err("a stopped round settles nothing");
        assert_eq!(
            error.kind(),
            crate::query_execution::split_assignment::TaskUpdateTransportErrorKind::RetryableNetwork
        );
    }

    #[test]
    fn a_resent_request_attaches_to_the_submission_the_substrate_already_holds() {
        // The driver resends the identical request after an unknown outcome.
        // Opening a second submission would offer sequences the task already
        // accepted, which its own domain admission refuses as a regression.
        let bridge = bridge();
        let request = terminal_request(true);
        for _ in 0..2 {
            let _ = bridge.send(
                execution_id(),
                &target(),
                &request,
                Duration::from_millis(1),
                &SplitAssignmentStop::default(),
            );
        }
        assert_eq!(bridge.take_pending().len(), 1);
    }

    #[test]
    fn a_second_distinct_request_for_one_task_is_refused() {
        // One update in flight per task is the driver's rule. Two live
        // submissions for one task would race for one sequence space and their
        // acknowledgements could not be attributed.
        let bridge = bridge();
        let _ = bridge.send(
            execution_id(),
            &target(),
            &terminal_request(true),
            Duration::from_millis(1),
            &SplitAssignmentStop::default(),
        );
        let error = bridge
            .send(
                execution_id(),
                &target(),
                &terminal_request_for(OTHER_SCAN_NODE, true),
                Duration::from_millis(1),
                &SplitAssignmentStop::default(),
            )
            .expect_err("a different request must not join a live submission");
        assert_eq!(
            error.kind(),
            crate::query_execution::split_assignment::TaskUpdateTransportErrorKind::Fatal
        );
        assert!(error.detail().contains("already has a different"));
    }

    #[test]
    fn an_applied_acknowledgement_becomes_the_drivers_accepted_watermark() {
        // The driver validates the watermark against its own request, so every
        // field has to arrive unchanged.
        let bridge = bridge();
        let handle = spawn_send(
            &bridge,
            terminal_request(true),
            Duration::from_secs(30),
            SplitAssignmentStop::default(),
        );
        let operation_id = release_pending(&bridge);
        let watermark =
            SplitWatermark::empty().apply_batch(SplitSequence::new(7).expect("nonzero"), true);
        bridge
            .settle(
                operation_id,
                &accepted_ack(
                    operation_id,
                    PlanNodeSplitReceipt::new(
                        PlanNodeId::new(SCAN_NODE).expect("a nonnegative plan node"),
                        watermark,
                    )
                    .with_queued_splits(3),
                    DomainProgression::Apply,
                ),
            )
            .expect("a bound operation settles");
        let outcome = handle
            .join()
            .expect("the sender thread joins")
            .expect("an applied acknowledgement");
        assert_eq!(
            outcome,
            TaskUpdateOutcome::Accepted(vec![AcceptedPlanNode {
                plan_node_id: SCAN_NODE,
                accepted_through_sequence: 7,
                no_more_splits: true,
                queued_splits: 3,
            }])
        );
    }

    #[test]
    fn an_accepted_receipt_without_a_queue_depth_is_refused() {
        // Reporting an unreported queue depth as zero would tell the driver the
        // task has nothing outstanding and switch its backpressure off.
        let bridge = bridge();
        let handle = spawn_send(
            &bridge,
            terminal_request(true),
            Duration::from_secs(30),
            SplitAssignmentStop::default(),
        );
        let operation_id = release_pending(&bridge);
        bridge
            .settle(
                operation_id,
                &accepted_ack(
                    operation_id,
                    PlanNodeSplitReceipt::new(
                        PlanNodeId::new(SCAN_NODE).expect("a nonnegative plan node"),
                        SplitWatermark::empty().apply_no_more(),
                    ),
                    DomainProgression::Apply,
                ),
            )
            .expect("a bound operation settles");
        let error = handle
            .join()
            .expect("the sender thread joins")
            .expect_err("a receipt without a queue depth is not an accepted watermark");
        assert_eq!(
            error.kind(),
            crate::query_execution::split_assignment::TaskUpdateTransportErrorKind::Fatal
        );
        assert!(error.detail().contains("no queue depth"));
    }

    #[test]
    fn a_retryable_outcome_keeps_the_submission_the_substrate_will_replay() {
        // `RemoteTask` keeps its released request after an unknown outcome and
        // hands out the same operation id again. Retiring the submission here
        // would make the driver's resend mint a second, non-monotonic update.
        let bridge = bridge();
        let request = terminal_request(true);
        let handle = spawn_send(
            &bridge,
            request,
            Duration::from_secs(30),
            SplitAssignmentStop::default(),
        );
        let operation_id = release_pending(&bridge);
        assert_eq!(
            bridge
                .settle(
                    operation_id,
                    &failed_ack(operation_id, OperationOutcome::RetryableTransportUnknown)
                )
                .expect("a bound operation settles"),
            SettleVerdict::Retained
        );
        let error = handle
            .join()
            .expect("the sender thread joins")
            .expect_err("an unknown outcome is not a delivery");
        assert_eq!(
            error.kind(),
            crate::query_execution::split_assignment::TaskUpdateTransportErrorKind::RetryableNetwork
        );
        // Still live and still bound, so the replayed operation lands on it.
        let state = bridge.state.lock().expect("split delivery state lock");
        assert_eq!(state.live.len(), 1);
        assert_eq!(
            state.by_operation.get(&operation_id).copied(),
            state.by_task.get(&task_id()).copied()
        );
    }

    #[test]
    fn a_failed_closed_outcome_is_a_rejection_the_driver_must_not_resend() {
        // A typed rejection is a decision about this attempt. Reporting it as
        // a transport failure would make the driver resend it for the whole
        // error budget.
        let bridge = bridge();
        let handle = spawn_send(
            &bridge,
            terminal_request(true),
            Duration::from_secs(30),
            SplitAssignmentStop::default(),
        );
        let operation_id = release_pending(&bridge);
        bridge
            .settle(
                operation_id,
                &failed_ack(operation_id, OperationOutcome::InvalidStateOrRequest),
            )
            .expect("a bound operation settles");
        let outcome = handle
            .join()
            .expect("the sender thread joins")
            .expect("a rejection is a delivery outcome, not a transport failure");
        assert!(matches!(outcome, TaskUpdateOutcome::Rejected { .. }));
        let TaskUpdateOutcome::Rejected { reason, .. } = outcome else {
            unreachable!("asserted above")
        };
        assert_eq!(reason, "InvalidStateOrRequest");
    }

    #[test]
    fn a_progression_conflict_in_an_applied_receipt_is_refused() {
        // An applied operation cannot also report a domain conflict. Reading
        // its watermark as accepted would confirm splits the task refused.
        let bridge = bridge();
        let handle = spawn_send(
            &bridge,
            terminal_request(true),
            Duration::from_secs(30),
            SplitAssignmentStop::default(),
        );
        let operation_id = release_pending(&bridge);
        bridge
            .settle(
                operation_id,
                &accepted_ack(
                    operation_id,
                    PlanNodeSplitReceipt::new(
                        PlanNodeId::new(SCAN_NODE).expect("a nonnegative plan node"),
                        SplitWatermark::empty().apply_no_more(),
                    )
                    .with_queued_splits(0),
                    DomainProgression::Conflict(
                        novarocks_execution::task_execution::DomainConflict::AfterSeal,
                    ),
                ),
            )
            .expect("a bound operation settles");
        let error = handle
            .join()
            .expect("the sender thread joins")
            .expect_err("a conflicting progression is not an accepted watermark");
        assert!(error.detail().contains("progression"));
    }

    #[test]
    fn a_discarded_update_fails_its_sender_at_once_instead_of_waiting_out_the_timeout() {
        // A terminal task drops the fact. Leaving the sender blocked would
        // spend the driver's whole error budget resending a fact that can
        // never be applied.
        let bridge = bridge();
        let handle = spawn_send(
            &bridge,
            terminal_request(true),
            Duration::from_secs(30),
            SplitAssignmentStop::default(),
        );
        let pending = await_pending(&bridge);
        bridge
            .admit(
                pending[0].delivery(),
                Ok(UpdateAdmission::DiscardedTerminal),
            )
            .expect("a taken submission");
        let error = handle
            .join()
            .expect("the sender thread joins")
            .expect_err("a discarded update establishes no backend outcome");
        assert_eq!(
            error.kind(),
            crate::query_execution::split_assignment::TaskUpdateTransportErrorKind::Fatal
        );
        assert!(error.detail().contains("is terminal"));
    }

    #[test]
    fn a_refused_update_fails_its_sender_locally() {
        // A domain regression is refused by the frontend itself. Nothing
        // reached a backend, so resending would establish nothing.
        let bridge = bridge();
        let handle = spawn_send(
            &bridge,
            terminal_request(true),
            Duration::from_secs(30),
            SplitAssignmentStop::default(),
        );
        let pending = await_pending(&bridge);
        bridge
            .admit(
                pending[0].delivery(),
                Err(&TaskExecutionError::UnknownOperation),
            )
            .expect("a taken submission");
        let error = handle
            .join()
            .expect("the sender thread joins")
            .expect_err("a refused update establishes no backend outcome");
        assert_eq!(
            error.kind(),
            crate::query_execution::split_assignment::TaskUpdateTransportErrorKind::Fatal
        );
    }

    #[test]
    fn a_released_operation_binds_so_a_receiptless_acknowledgement_still_reaches_its_sender() {
        // A failed-closed acknowledgement carries no receipt and therefore no
        // task identity. Binding at release is what lets it be routed at all.
        let bridge = bridge();
        let handle = spawn_send(
            &bridge,
            terminal_request(true),
            Duration::from_secs(30),
            SplitAssignmentStop::default(),
        );
        let operation_id = release_pending(&bridge);
        assert_eq!(
            bridge
                .settle(
                    operation_id,
                    &failed_ack(operation_id, OperationOutcome::DestinationFailure)
                )
                .expect("the released operation is bound"),
            SettleVerdict::Settled
        );
        let outcome = handle
            .join()
            .expect("the sender thread joins")
            .expect("a rejection is a delivery outcome");
        assert!(matches!(outcome, TaskUpdateOutcome::Rejected { .. }));
    }

    #[test]
    fn an_acknowledgement_of_another_owners_operation_is_ignored() {
        // Split delivery is one of several task-update producers. Claiming an
        // operation it never produced would settle a submission with a foreign
        // receipt.
        let bridge = bridge();
        let operation_id = TaskOperationId::new_v7();
        assert_eq!(
            bridge
                .settle(
                    operation_id,
                    &failed_ack(operation_id, OperationOutcome::Gone)
                )
                .expect("an unbound operation is not an error"),
            SettleVerdict::Ignored
        );
    }

    #[test]
    fn an_unknown_fragment_instance_is_refused_rather_than_delivered_elsewhere() {
        // The fragment instance id is the kernel key. Guessing a task for an
        // unknown one would send a scan's splits to a different scan.
        let bridge = SplitDeliveryBridge::with_tasks(BTreeMap::new());
        let error = bridge
            .send(
                execution_id(),
                &target(),
                &terminal_request(true),
                Duration::from_millis(1),
                &SplitAssignmentStop::default(),
            )
            .expect_err("an unknown kernel key has no task");
        assert_eq!(
            error.kind(),
            crate::query_execution::split_assignment::TaskUpdateTransportErrorKind::Fatal
        );
        assert!(error.detail().contains("is not a task of this attempt"));
    }

    #[test]
    fn a_request_that_does_not_carry_exactly_one_assignment_is_refused() {
        // The driver builds one assignment per request and its own Accepted
        // validation refuses any other shape. Splitting a request over several
        // operations would make one acknowledgement unattributable.
        let bridge = bridge();
        let error = bridge
            .send(
                execution_id(),
                &target(),
                &TaskUpdateRequest::new(FINST, Vec::new()),
                Duration::from_millis(1),
                &SplitAssignmentStop::default(),
            )
            .expect_err("an assignment-free request delivers nothing");
        assert_eq!(
            error.kind(),
            crate::query_execution::split_assignment::TaskUpdateTransportErrorKind::Fatal
        );
        assert!(error.detail().contains("exactly one assignment"));
    }

    #[test]
    fn an_empty_terminal_assignment_translates_to_the_codecs_own_terminal_intent() {
        // The backend derives the same range from the same wire assignment. A
        // different range here would classify one payload against two
        // watermarks.
        let update = task_update(&terminal_request(true)).expect("a terminal marker translates");
        let TaskDomainUpdate::SplitAssignment(intent) = update else {
            panic!("a split assignment translates to its own domain")
        };
        assert_eq!(intent.first(), SplitSequence::FIRST);
        assert_eq!(intent.last(), SplitSequence::FIRST);
        assert!(intent.no_more_splits());
        assert_eq!(intent.node().get(), SCAN_NODE);
    }

    #[test]
    fn an_assignment_with_no_splits_and_no_terminal_marker_is_refused() {
        // Such an update enqueues nothing and seals nothing, so no
        // acknowledgement of it could ever cover a request.
        let detail = task_update(&terminal_request(false))
            .expect_err("an empty non-terminal assignment carries nothing");
        assert!(detail.contains("no splits and no terminal marker"));
    }
}
