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

//! The per-round split-assignment driver.
//!
//! It pulls bounded batches from one split source at a time, spreads them over
//! already admitted tasks by split weight, and keeps at most one update in
//! flight per task. Every admitted task — including one that receives no work
//! at all — is told no-more-splits so it can finish.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use novarocks_execution::task_execution::OperationOutcome;
use novarocks_proto_codec::lifecycle::QueryExecutionId;
use novarocks_types::UniqueId;

use novarocks_spi::connector::ConnectorReadWireEncoder;
use novarocks_spi::connector::read_stack::{
    ConnectorReadDynamicFilterSnapshot, ConnectorReadSplitSource,
};

use super::super::connector_domain::{PlanNodeAssignmentState, Split, SplitAssignmentError};
use super::transport::{
    TaskUpdateOutcome, TaskUpdateTransport, TaskUpdateTransportError, TaskUpdateTransportErrorKind,
};

// Design: ADR-0123 (docs/adr/ADR-0123-task-update-watermark-retry-delivery.md)
/// Largest number of splits one update may carry, matching the wire bound.
pub(crate) const MAX_SPLITS_PER_UPDATE: usize = 4096;

/// Server-frozen retry policy for one TaskUpdate request. The driver receives
/// this value through the coordinator and never consults process-global config.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaskUpdateRetryPolicy {
    pub(crate) rpc_timeout: Duration,
    pub(crate) error_duration: Duration,
    pub(crate) initial_backoff: Duration,
    pub(crate) max_backoff: Duration,
}

impl TaskUpdateRetryPolicy {
    pub fn try_new(
        rpc_timeout: Duration,
        error_duration: Duration,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Result<Self, String> {
        if rpc_timeout.is_zero()
            || error_duration.is_zero()
            || initial_backoff.is_zero()
            || max_backoff.is_zero()
        {
            return Err("task update retry durations must be greater than zero".to_owned());
        }
        if initial_backoff > max_backoff {
            return Err("task update retry initial backoff must not exceed max backoff".to_owned());
        }
        if rpc_timeout > error_duration {
            return Err("task update rpc timeout must not exceed error duration".to_owned());
        }
        if max_backoff > error_duration {
            return Err("task update retry max backoff must not exceed error duration".to_owned());
        }
        Ok(Self {
            rpc_timeout,
            error_duration,
            initial_backoff,
            max_backoff,
        })
    }
}

impl Default for TaskUpdateRetryPolicy {
    fn default() -> Self {
        Self::try_new(
            Duration::from_secs(5),
            Duration::from_secs(30),
            Duration::from_millis(100),
            Duration::from_secs(1),
        )
        .expect("default task update retry policy is valid")
    }
}

/// The one round-owned stop authority. Its atomic makes hot checks cheap and
/// its condition variable wakes a synchronous retry backoff immediately.
#[derive(Clone)]
pub(crate) struct SplitAssignmentStop {
    stopped: Arc<AtomicBool>,
    wait: Arc<(Mutex<()>, Condvar)>,
    cancel: tokio::sync::watch::Sender<bool>,
}

impl Default for SplitAssignmentStop {
    fn default() -> Self {
        let (cancel, _) = tokio::sync::watch::channel(false);
        Self {
            stopped: Arc::new(AtomicBool::new(false)),
            wait: Arc::new((Mutex::new(()), Condvar::new())),
            cancel,
        }
    }
}

impl SplitAssignmentStop {
    pub(crate) fn stop(&self) {
        if !self.stopped.swap(true, Ordering::AcqRel) {
            let _ = self.cancel.send(true);
            self.wait.1.notify_all();
        }
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    pub(crate) fn wait_backoff(&self, duration: Duration) -> bool {
        if self.is_stopped() {
            return true;
        }
        let guard = self.wait.0.lock().expect("split assignment stop lock");
        let _ = self
            .wait
            .1
            .wait_timeout(guard, duration)
            .expect("split assignment stop condvar");
        self.is_stopped()
    }

    pub(crate) fn subscribe(&self) -> tokio::sync::watch::Receiver<bool> {
        self.cancel.subscribe()
    }
}

/// One admitted task this driver may assign work to.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct AssignmentTarget {
    pub(crate) backend_idx: usize,
    pub(crate) fragment_instance_id: UniqueId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum SplitAssignmentDriverError {
    /// The driver was closed; a caller must not keep assigning.
    Closed,
    /// No admitted task exists for this plan node, so a split has nowhere to
    /// go. Failing closed is required: silently dropping it would produce a
    /// query that returns fewer rows than it should.
    NoAdmittedTask {
        plan_node_id: i32,
    },
    Assignment(SplitAssignmentError),
    Transport {
        target: AssignmentTarget,
        detail: String,
    },
    Rejected {
        target: AssignmentTarget,
        reason: String,
        detail: String,
    },
    /// The connector's own enumeration failed. It is never retried blindly:
    /// re-enumerating could produce a different split set for the same pinned
    /// snapshot only if something is wrong, and silently continuing would drop
    /// work.
    SplitSource {
        plan_node_id: i32,
        detail: String,
    },
}

impl fmt::Display for SplitAssignmentDriverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => formatter.write_str("split assignment driver is closed"),
            Self::NoAdmittedTask { plan_node_id } => write!(
                formatter,
                "plan node {plan_node_id} has no admitted task to receive splits"
            ),
            Self::Assignment(error) => write!(formatter, "{error}"),
            Self::Transport { target, detail } => write!(
                formatter,
                "task update to backend {} failed: {detail}",
                target.backend_idx
            ),
            Self::Rejected {
                target,
                reason,
                detail,
            } => write!(
                formatter,
                "backend {} rejected a task update ({reason}): {detail}",
                target.backend_idx
            ),
            Self::SplitSource {
                plan_node_id,
                detail,
            } => write!(
                formatter,
                "split enumeration for plan node {plan_node_id} failed: {detail}"
            ),
        }
    }
}

impl std::error::Error for SplitAssignmentDriverError {}

impl SplitAssignmentDriverError {
    /// This delivery failure in the neutral task-protocol vocabulary.
    ///
    /// The task-protocol owners consume this rather than restating which
    /// delivery failures may be replayed, so the two owners cannot drift on
    /// the one rule that matters: only a genuinely unknown transport outcome
    /// is retryable, and every typed rejection fails closed however transient
    /// its wording looks.
    pub(crate) const fn as_operation_outcome(&self) -> OperationOutcome {
        match self {
            // The driver already exhausted its retry budget on an unknown
            // outcome before surfacing this, and the identical immutable
            // request is still the only legal resend.
            Self::Transport { .. } => OperationOutcome::RetryableTransportUnknown,
            Self::Closed => OperationOutcome::ContextTerminalReceipt,
            Self::Rejected { .. } | Self::Assignment(_) | Self::NoAdmittedTask { .. } => {
                OperationOutcome::InvalidStateOrRequest
            }
            Self::SplitSource { .. } => OperationOutcome::ResourceExhausted,
        }
    }
}

impl From<SplitAssignmentError> for SplitAssignmentDriverError {
    fn from(error: SplitAssignmentError) -> Self {
        Self::Assignment(error)
    }
}

/// A driver-owned split source, closed exactly once when the round ends.
pub(crate) struct SplitSourceHandle {
    plan_node_id: i32,
    finished: bool,
    closed: bool,
}

impl SplitSourceHandle {
    pub(crate) const fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }

    pub(crate) const fn is_finished(&self) -> bool {
        self.finished
    }

    pub(crate) const fn is_closed(&self) -> bool {
        self.closed
    }
}

/// Per-task send state.
#[derive(Debug, Default)]
struct TaskState {
    /// Assigned weight so far, used to spread work rather than to bound it.
    assigned_weight: u64,
    /// Splits the backend still had queued at the last acknowledgement.
    queued_splits: u64,
    /// One update at a time: a driver waits for the acknowledgement before it
    /// sends the next, so a slow task cannot accumulate unbounded work.
    in_flight: bool,
    /// This destination finished before a delivery reached it, so it needs
    /// nothing more. Kept per task rather than per round: the other
    /// destinations of the same round are still consuming.
    finished: bool,
}

/// The coordinator-side driver for one execution round.
pub(crate) struct SplitAssignmentDriver {
    execution_id: QueryExecutionId,
    transport: std::sync::Arc<dyn TaskUpdateTransport>,
    /// Admitted tasks per plan node, frozen before the Init barrier.
    ///
    /// Keyed by plan node alone rather than by (fragment, plan node): the
    /// distributed planner allocates node ids from one counter spanning every
    /// fragment, so two fragments cannot share one. If that ever changes, this
    /// map would silently merge two scans' task sets.
    tasks: BTreeMap<i32, Vec<AssignmentTarget>>,
    sequences: BTreeMap<AssignmentTarget, PlanNodeAssignmentState>,
    task_state: BTreeMap<AssignmentTarget, TaskState>,
    sources: Vec<SplitSourceHandle>,
    closed: bool,
    /// Splits the backends reported as still queued, above which the driver
    /// stops pulling new batches.
    max_queued_splits_per_task: u64,
    encoders: BTreeMap<i32, std::sync::Arc<dyn ConnectorReadWireEncoder>>,
    retry_policy: TaskUpdateRetryPolicy,
    stop: SplitAssignmentStop,
}

impl SplitAssignmentDriver {
    pub(crate) fn new(
        execution_id: QueryExecutionId,
        transport: std::sync::Arc<dyn TaskUpdateTransport>,
        tasks: BTreeMap<i32, Vec<AssignmentTarget>>,
        max_queued_splits_per_task: u64,
        encoders: BTreeMap<i32, std::sync::Arc<dyn ConnectorReadWireEncoder>>,
        retry_policy: TaskUpdateRetryPolicy,
        stop: SplitAssignmentStop,
    ) -> Self {
        let mut sequences = BTreeMap::new();
        let mut task_state = BTreeMap::new();
        for targets in tasks.values() {
            for target in targets {
                sequences
                    .entry(target.clone())
                    .or_insert_with(PlanNodeAssignmentState::new);
                task_state.entry(target.clone()).or_default();
            }
        }
        let sources = tasks
            .keys()
            .map(|plan_node_id| SplitSourceHandle {
                plan_node_id: *plan_node_id,
                finished: false,
                closed: false,
            })
            .collect();
        Self {
            execution_id,
            transport,
            tasks,
            sequences,
            task_state,
            sources,
            closed: false,
            max_queued_splits_per_task,
            encoders,
            retry_policy,
            stop,
        }
    }

    pub(crate) const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub(crate) const fn is_closed(&self) -> bool {
        self.closed
    }

    pub(crate) fn sources(&self) -> &[SplitSourceHandle] {
        &self.sources
    }

    /// Whether this plan node has already sent its terminal marker to every
    /// admitted task, so there is nothing left to pump.
    pub(crate) fn is_terminal_for(&self, plan_node_id: i32) -> bool {
        self.sources
            .iter()
            .find(|source| source.plan_node_id() == plan_node_id)
            .is_some_and(SplitSourceHandle::is_finished)
    }

    /// Whether every task is currently at or above its queue ceiling, so the
    /// driver should not pull another batch from a split source yet.
    pub(crate) fn is_backpressured(&self, plan_node_id: i32) -> bool {
        let Some(targets) = self.tasks.get(&plan_node_id) else {
            return false;
        };
        targets.iter().all(|target| {
            self.task_state.get(target).is_some_and(|state| {
                state.in_flight || state.queued_splits >= self.max_queued_splits_per_task
            })
        })
    }

    /// Distribute one batch over the admitted tasks of a plan node.
    ///
    /// Placement is by accumulated weight so a heavy split does not crowd a
    /// task the way an equal split count would.
    pub(crate) fn distribute(
        &mut self,
        plan_node_id: i32,
        splits: Vec<Split>,
    ) -> Result<BTreeMap<AssignmentTarget, Vec<Split>>, SplitAssignmentDriverError> {
        if self.closed {
            return Err(SplitAssignmentDriverError::Closed);
        }
        let all_targets = self
            .tasks
            .get(&plan_node_id)
            .filter(|targets| !targets.is_empty())
            .ok_or(SplitAssignmentDriverError::NoAdmittedTask { plan_node_id })?
            .clone();
        // Prefer tasks that are not already at their queue ceiling. Weight
        // balancing alone would keep feeding a saturated task simply because it
        // has carried less weight so far, which is exactly the case where its
        // queue depth, not its history, is what matters. If every task is
        // saturated, fall back to all of them: the caller only reaches here
        // when it has splits in hand, and holding them back here would strand
        // work that `is_backpressured` already declined to pull.
        let ceiling = self.max_queued_splits_per_task;
        let targets = {
            let available = all_targets
                .iter()
                .filter(|target| {
                    self.task_state
                        .get(*target)
                        .is_none_or(|state| state.queued_splits < ceiling)
                })
                .cloned()
                .collect::<Vec<_>>();
            if available.is_empty() {
                all_targets
            } else {
                available
            }
        };

        let mut placement: BTreeMap<AssignmentTarget, Vec<Split>> = BTreeMap::new();
        for split in splits {
            // Heaviest-first would need the whole batch sorted; assigning each
            // split to the currently lightest task keeps the pass streaming and
            // still balances weight across a batch.
            let target = targets
                .iter()
                .min_by_key(|target| {
                    (
                        self.task_state
                            .get(*target)
                            .map(|state| state.assigned_weight)
                            .unwrap_or_default(),
                        (*target).clone(),
                    )
                })
                .expect("targets is non-empty")
                .clone();
            self.task_state
                .entry(target.clone())
                .or_default()
                .assigned_weight += split.weight_raw();
            placement.entry(target).or_default().push(split);
        }
        Ok(placement)
    }

    /// Send one plan node's batch, then wait for each acknowledgement.
    pub(crate) fn send(
        &mut self,
        plan_node_id: i32,
        placement: BTreeMap<AssignmentTarget, Vec<Split>>,
        no_more_splits: bool,
    ) -> Result<(), SplitAssignmentDriverError> {
        if self.closed {
            return Err(SplitAssignmentDriverError::Closed);
        }
        // A task that received nothing still has to hear the terminal marker,
        // otherwise its scan would block forever waiting for work that will
        // never arrive.
        let mut recipients: BTreeMap<AssignmentTarget, Vec<Split>> = placement;
        if no_more_splits {
            for target in self.tasks.get(&plan_node_id).into_iter().flatten() {
                recipients.entry(target.clone()).or_default();
            }
        }

        for (target, splits) in recipients {
            if splits.len() > MAX_SPLITS_PER_UPDATE {
                return Err(SplitAssignmentDriverError::Assignment(
                    SplitAssignmentError::BatchTooLarge {
                        plan_node_id,
                        splits: splits.len(),
                    },
                ));
            }
            // A destination that already finished needs nothing more, and
            // enumerating splits for it would allocate sequences no one will
            // ever accept.
            if self
                .task_state
                .get(&target)
                .is_some_and(|state| state.finished)
            {
                continue;
            }
            // Sequences are allocated once and the resulting immutable request
            // stays alive until a strict acknowledgement confirms it. A retry
            // never enumerates splits or allocates a replacement sequence.
            let assignment = self
                .sequences
                .entry(target.clone())
                .or_insert_with(PlanNodeAssignmentState::new)
                .assign(
                    plan_node_id,
                    splits,
                    no_more_splits,
                    self.encoders.get(&plan_node_id).cloned().ok_or_else(|| {
                        SplitAssignmentDriverError::SplitSource {
                            plan_node_id,
                            detail: "missing exact connector read encoder".to_owned(),
                        }
                    })?,
                )?;
            let request = super::super::connector_domain::TaskUpdateRequest::new(
                target.fragment_instance_id,
                vec![assignment],
            );

            let state = self.task_state.entry(target.clone()).or_default();
            state.in_flight = true;
            let outcome = self.send_until_confirmed(&target, &request);
            let state = self.task_state.entry(target.clone()).or_default();
            state.in_flight = false;

            match outcome {
                Ok(TaskUpdateOutcome::Accepted(nodes)) => {
                    let node = validate_accepted_ack(&request, &nodes).map_err(|detail| {
                        SplitAssignmentDriverError::Transport {
                            target: target.clone(),
                            detail,
                        }
                    })?;
                    state.queued_splits = node.queued_splits;
                }
                Ok(TaskUpdateOutcome::Rejected { reason, detail }) => {
                    return Err(SplitAssignmentDriverError::Rejected {
                        target,
                        reason,
                        detail,
                    });
                }
                Ok(TaskUpdateOutcome::DestinationFinished { detail }) => {
                    // Not an error: the consumer stopped before this delivery
                    // arrived, which is ordinary for an early-terminating
                    // branch. Its queue depth is left as it was -- nothing was
                    // enqueued -- and the loop moves on to the destinations
                    // that are still consuming.
                    tracing::debug!(
                        target = ?target,
                        detail = %detail,
                        "split delivery stopped for a destination that already finished"
                    );
                    state.finished = true;
                }
                Err(error) => {
                    return Err(SplitAssignmentDriverError::Transport {
                        target,
                        detail: error.detail().to_owned(),
                    });
                }
            }
        }

        if no_more_splits
            && let Some(source) = self
                .sources
                .iter_mut()
                .find(|source| source.plan_node_id == plan_node_id)
        {
            source.finished = true;
        }
        Ok(())
    }

    /// Pull one batch from a split source and deliver it.
    ///
    /// Exactly one batch per call, so a caller driving several sources can give
    /// each a turn: draining one source to exhaustion here would let a slow
    /// enumeration starve every other scan in the round.
    ///
    /// Returns `Ok(false)` when nothing was delivered — the tasks are
    /// saturated, or the source had nothing right now. An empty batch means
    /// "nothing right now", never the end of enumeration.
    pub(crate) fn pump(
        &mut self,
        plan_node_id: i32,
        source: &mut dyn ConnectorReadSplitSource,
        batch_size: usize,
        dynamic_filter: &ConnectorReadDynamicFilterSnapshot,
    ) -> Result<bool, SplitAssignmentDriverError> {
        if self.closed {
            return Err(SplitAssignmentDriverError::Closed);
        }
        if self.is_backpressured(plan_node_id) {
            return Ok(false);
        }
        let batch = source
            .next_batch(batch_size.clamp(1, MAX_SPLITS_PER_UPDATE), dynamic_filter)
            .map_err(|error| SplitAssignmentDriverError::SplitSource {
                plan_node_id,
                detail: error.to_string(),
            })?;
        let no_more_splits = batch.no_more_splits();
        let splits = batch
            .into_splits()
            .into_iter()
            .map(Split::new)
            .collect::<Vec<_>>();
        let has_work = !splits.is_empty();
        if !has_work && !no_more_splits {
            return Ok(false);
        }
        let placement = self.distribute(plan_node_id, splits)?;
        self.send(plan_node_id, placement, no_more_splits)?;
        Ok(true)
    }

    /// Close the round.
    ///
    /// Idempotent, and after it no assignment may be produced or sent. A
    /// replacement round builds a new driver rather than reviving this one.
    pub(crate) fn close(&mut self) {
        self.closed = true;
        self.stop.stop();
        for source in &mut self.sources {
            source.closed = true;
        }
    }

    fn send_until_confirmed(
        &self,
        target: &AssignmentTarget,
        request: &super::super::connector_domain::TaskUpdateRequest,
    ) -> Result<TaskUpdateOutcome, TaskUpdateTransportError> {
        let mut first_retryable_error = None;
        let mut last_retryable_error = None;
        let mut retryable_attempts = 0_u64;
        let mut backoff = self.retry_policy.initial_backoff;
        loop {
            if self.stop.is_stopped() || self.closed {
                return Err(TaskUpdateTransportError::closed(
                    "split assignment round stopped",
                ));
            }
            let remaining = first_retryable_error.map(|started: Instant| {
                self.retry_policy
                    .error_duration
                    .saturating_sub(started.elapsed())
            });
            if remaining.is_some_and(|remaining| remaining.is_zero()) {
                let started = first_retryable_error.expect("retry budget has a first failure");
                let last_error = last_retryable_error
                    .as_ref()
                    .expect("retry budget has a latest failure");
                return Err(retry_budget_exhausted(
                    target,
                    retryable_attempts,
                    started.elapsed(),
                    last_error,
                ));
            }
            let rpc_timeout = remaining
                .map(|remaining| remaining.min(self.retry_policy.rpc_timeout))
                .unwrap_or(self.retry_policy.rpc_timeout);
            match self
                .transport
                .send(self.execution_id, target, request, rpc_timeout, &self.stop)
            {
                Ok(outcome) => return Ok(outcome),
                Err(error) if error.kind() == TaskUpdateTransportErrorKind::RetryableNetwork => {
                    let started = *first_retryable_error.get_or_insert_with(Instant::now);
                    retryable_attempts = retryable_attempts.saturating_add(1);
                    last_retryable_error = Some(error.clone());
                    let remaining = self
                        .retry_policy
                        .error_duration
                        .saturating_sub(started.elapsed());
                    if remaining.is_zero() {
                        return Err(retry_budget_exhausted(
                            target,
                            retryable_attempts,
                            started.elapsed(),
                            &error,
                        ));
                    }
                    let wait = backoff.min(remaining);
                    tracing::debug!(
                        backend_idx = target.backend_idx,
                        attempt_hi = request.fragment_instance_id().high(),
                        attempt_lo = request.fragment_instance_id().low(),
                        elapsed_ms = started.elapsed().as_millis(),
                        error_kind = "retryable_network",
                        "retrying task update after unknown outcome"
                    );
                    if cfg!(debug_assertions)
                        && std::env::var_os("NOVAROCKS_SQL_TEST_EMIT_CONNECTOR_READER_MARKER")
                            .is_some()
                    {
                        eprintln!(
                            "NOVAROCKS_TASK_UPDATE_RETRY backend={} finst={:x}:{:x} attempt={} elapsed_ms={}",
                            target.backend_idx,
                            request.fragment_instance_id().high(),
                            request.fragment_instance_id().low(),
                            retryable_attempts,
                            started.elapsed().as_millis(),
                        );
                    }
                    if self.stop.wait_backoff(wait) {
                        return Err(TaskUpdateTransportError::closed(
                            "split assignment round stopped during task update retry",
                        ));
                    }
                    backoff = backoff.saturating_mul(2).min(self.retry_policy.max_backoff);
                }
                Err(error) => return Err(error),
            }
        }
    }
}

fn retry_budget_exhausted(
    target: &AssignmentTarget,
    retryable_attempts: u64,
    elapsed: Duration,
    last_error: &TaskUpdateTransportError,
) -> TaskUpdateTransportError {
    TaskUpdateTransportError::retryable_network(format!(
        "task update retry error duration exhausted backend={} attempts={} elapsed_ms={} last_error={}",
        target.backend_idx,
        retryable_attempts,
        elapsed.as_millis(),
        last_error.detail(),
    ))
}

/// Validate that an Accepted acknowledgement covers exactly the immutable
/// request that produced it. This remains pure so mocked transport tests cover
/// malformed and stale acknowledgements without a live backend.
fn validate_accepted_ack<'a>(
    request: &super::super::connector_domain::TaskUpdateRequest,
    accepted: &'a [super::transport::AcceptedPlanNode],
) -> Result<&'a super::transport::AcceptedPlanNode, String> {
    if request.assignments().len() != 1 || accepted.len() != request.assignments().len() {
        return Err(
            "task update accepted acknowledgement does not match request node count".to_owned(),
        );
    }
    let assignment = &request.assignments()[0];
    let node = accepted
        .iter()
        .find(|node| node.plan_node_id == assignment.plan_node_id())
        .ok_or_else(|| {
            "task update accepted acknowledgement misses request plan node".to_owned()
        })?;
    let max_sequence = assignment.splits().last().map(|split| split.sequence_id());
    if max_sequence.is_some_and(|sequence| node.accepted_through_sequence < sequence) {
        return Err(
            "task update accepted acknowledgement watermark does not cover request".to_owned(),
        );
    }
    if assignment.no_more_splits() && !node.no_more_splits {
        return Err(
            "task update accepted acknowledgement does not confirm no-more-splits".to_owned(),
        );
    }
    Ok(node)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use novarocks_types::{AttemptId, QueryId};

    use crate::query_execution::connector_domain::TaskUpdateRequest;

    use super::*;

    struct ScriptedTransport {
        outcomes: Mutex<VecDeque<Result<TaskUpdateOutcome, TaskUpdateTransportError>>>,
        requests: Mutex<Vec<usize>>,
        stop_after_first_send: bool,
    }

    impl TaskUpdateTransport for ScriptedTransport {
        fn send(
            &self,
            _execution_id: QueryExecutionId,
            _target: &AssignmentTarget,
            request: &TaskUpdateRequest,
            _timeout: Duration,
            stop: &SplitAssignmentStop,
        ) -> Result<TaskUpdateOutcome, TaskUpdateTransportError> {
            self.requests
                .lock()
                .expect("scripted transport requests")
                .push(std::ptr::from_ref(request).addr());
            if self.stop_after_first_send {
                stop.stop();
            }
            self.outcomes
                .lock()
                .expect("scripted transport outcomes")
                .pop_front()
                .expect("scripted transport has an outcome")
        }
    }

    fn target() -> AssignmentTarget {
        AssignmentTarget {
            backend_idx: 7,
            fragment_instance_id: UniqueId::new(8, 9),
        }
    }

    fn retry_policy(error_duration: Duration, initial_backoff: Duration) -> TaskUpdateRetryPolicy {
        TaskUpdateRetryPolicy::try_new(
            error_duration,
            error_duration,
            initial_backoff,
            initial_backoff,
        )
        .expect("valid retry policy")
    }

    fn driver(
        transport: Arc<dyn TaskUpdateTransport>,
        retry_policy: TaskUpdateRetryPolicy,
        stop: SplitAssignmentStop,
    ) -> SplitAssignmentDriver {
        let execution_id =
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).expect("attempt id"))
                .expect("execution id");
        SplitAssignmentDriver::new(
            execution_id,
            transport,
            BTreeMap::new(),
            1,
            BTreeMap::new(),
            retry_policy,
            stop,
        )
    }

    #[test]
    fn retryable_unknown_outcome_reuses_the_same_immutable_request() {
        let transport = Arc::new(ScriptedTransport {
            outcomes: Mutex::new(VecDeque::from([
                Err(TaskUpdateTransportError::retryable_network("ack dropped")),
                Ok(TaskUpdateOutcome::Accepted(Vec::new())),
            ])),
            requests: Mutex::new(Vec::new()),
            stop_after_first_send: false,
        });
        let stop = SplitAssignmentStop::default();
        let driver = driver(
            Arc::clone(&transport) as Arc<dyn TaskUpdateTransport>,
            retry_policy(Duration::from_millis(50), Duration::from_millis(1)),
            stop,
        );
        let target = target();
        let request = TaskUpdateRequest::new(target.fragment_instance_id, Vec::new());

        assert!(matches!(
            driver.send_until_confirmed(&target, &request),
            Ok(TaskUpdateOutcome::Accepted(_))
        ));
        assert_eq!(
            *transport
                .requests
                .lock()
                .expect("scripted transport requests"),
            vec![std::ptr::from_ref(&request).addr(); 2]
        );
    }

    #[test]
    fn fatal_transport_error_is_not_retried() {
        let transport = Arc::new(ScriptedTransport {
            outcomes: Mutex::new(VecDeque::from([Err(TaskUpdateTransportError::fatal(
                "request encoding failed",
            ))])),
            requests: Mutex::new(Vec::new()),
            stop_after_first_send: false,
        });
        let stop = SplitAssignmentStop::default();
        let driver = driver(
            Arc::clone(&transport) as Arc<dyn TaskUpdateTransport>,
            retry_policy(Duration::from_millis(50), Duration::from_millis(1)),
            stop,
        );
        let target = target();
        let request = TaskUpdateRequest::new(target.fragment_instance_id, Vec::new());

        let error = driver
            .send_until_confirmed(&target, &request)
            .expect_err("fatal transport failure must stop the send");
        assert_eq!(error.kind(), TaskUpdateTransportErrorKind::Fatal);
        assert_eq!(
            transport
                .requests
                .lock()
                .expect("scripted transport requests")
                .len(),
            1
        );
    }

    #[test]
    fn exhausted_retry_budget_keeps_the_last_network_error_and_context() {
        let transport = Arc::new(ScriptedTransport {
            outcomes: Mutex::new(VecDeque::from([Err(
                TaskUpdateTransportError::retryable_network("last response lost"),
            )])),
            requests: Mutex::new(Vec::new()),
            stop_after_first_send: false,
        });
        let stop = SplitAssignmentStop::default();
        let driver = driver(
            Arc::clone(&transport) as Arc<dyn TaskUpdateTransport>,
            retry_policy(Duration::from_millis(2), Duration::from_millis(2)),
            stop,
        );
        let target = target();
        let request = TaskUpdateRequest::new(target.fragment_instance_id, Vec::new());

        let error = driver
            .send_until_confirmed(&target, &request)
            .expect_err("retry budget must eventually expire");
        assert_eq!(error.kind(), TaskUpdateTransportErrorKind::RetryableNetwork);
        assert!(error.detail().contains("backend=7"));
        assert!(error.detail().contains("attempts=1"));
        assert!(error.detail().contains("elapsed_ms="));
        assert!(error.detail().contains("last response lost"));
    }

    #[test]
    fn stop_interrupts_retry_backoff_without_waiting_for_the_budget() {
        let transport = Arc::new(ScriptedTransport {
            outcomes: Mutex::new(VecDeque::from([Err(
                TaskUpdateTransportError::retryable_network("connection reset"),
            )])),
            requests: Mutex::new(Vec::new()),
            stop_after_first_send: true,
        });
        let stop = SplitAssignmentStop::default();
        let driver = driver(
            Arc::clone(&transport) as Arc<dyn TaskUpdateTransport>,
            retry_policy(Duration::from_secs(2), Duration::from_secs(1)),
            stop,
        );
        let target = target();
        let request = TaskUpdateRequest::new(target.fragment_instance_id, Vec::new());
        let started = Instant::now();

        let error = driver
            .send_until_confirmed(&target, &request)
            .expect_err("stop must interrupt retry backoff");
        assert_eq!(error.kind(), TaskUpdateTransportErrorKind::Closed);
        assert!(started.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn malformed_ack_shape_is_a_fatal_contract_error() {
        let request = TaskUpdateRequest::new(UniqueId::new(8, 9), Vec::new());
        let error = validate_accepted_ack(&request, &[])
            .expect_err("an empty request may not accept a malformed empty acknowledgement");
        assert!(error.contains("node count"));
    }

    #[test]
    fn split_source_error_names_its_plan_node() {
        assert!(
            SplitAssignmentDriverError::SplitSource {
                plan_node_id: 7,
                detail: "closed".to_owned()
            }
            .to_string()
            .contains("plan node 7")
        );
    }
}
