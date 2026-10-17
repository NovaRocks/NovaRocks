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

//! The per-(task attempt, plan node) dynamic split queue.
//!
//! Responsibilities:
//! - Owns the runtime stream of splits a scan receives after its plan was
//!   frozen, one queue per plan node inside one task attempt.
//! - Enforces the wire contract locally: monotonic sequence watermarks,
//!   idempotent duplicate suppression, an idempotent terminal marker, and a
//!   retained-byte ceiling that a sender can use for backpressure.
//!
//! Key exported interfaces:
//! - Types: `SplitQueueRegistry`, `TaskAttemptSplitQueues`, `SplitQueue`,
//!   `TaskAttemptKey`, `SplitQueueConfig`, `SplitPoll`, `SplitOfferOutcome`,
//!   `SplitQueueStats`, `SplitQueueError`, `SplitQueueErrorKind`,
//!   `ScheduledSplitFacts`.
//!
//! Current limitations:
//! - This is queue ownership only. It creates no lifecycle entry, extends no
//!   retention, and performs no cross-attempt recovery: an attempt's queues are
//!   born with the attempt and die with it.
//!
//! Provider neutrality: the queue moves role-owned received splits and reads
//! only their sequence, plan node, and retained size. It
//! imports neither a wire model nor a provider implementation.
// Design: ADR-0123 (docs/adr/ADR-0123-task-update-watermark-retry-delivery.md)

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use novarocks_types::{QueryExecutionId, UniqueId};

use crate::runtime::observable::Observable;

/// The facts this queue needs from one scheduled split.
///
/// The queue is generic over this narrow view rather than over a wire type.
/// Roles recover their own received split before entering the queue, so
/// ordering, duplicate suppression, and budget rules cannot reach a provider
/// variant.
pub trait ScheduledSplitFacts: Send + Sync + 'static {
    /// Monotonic within one (task attempt, plan node).
    fn sequence_id(&self) -> u64;

    /// The plan node whose queue this split belongs to.
    fn plan_node_id(&self) -> i32;

    /// Retained-byte estimate charged against the queue's ceiling.
    fn retained_size_in_bytes(&self) -> u64;
}

/// The protocol evidence required to classify a retransmission before a role
/// attempts provider-specific recovery. It intentionally carries no payload:
/// a sequence at or below the accepted watermark is a duplicate regardless of
/// its payload, so it must not be decoded merely to compare content.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SplitSequenceEvidence {
    sequence_id: u64,
    plan_node_id: i32,
}

impl SplitSequenceEvidence {
    pub const fn new(sequence_id: u64, plan_node_id: i32) -> Self {
        Self {
            sequence_id,
            plan_node_id,
        }
    }

    pub const fn sequence_id(&self) -> u64 {
        self.sequence_id
    }

    pub const fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }
}

/// Why one split offer was rejected.
///
/// Every variant is terminal for that offer: the queue is never partially
/// updated, and a rejected sequence is never silently overwritten.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SplitQueueErrorKind {
    /// The assignment names a plan node other than the queue it reached.
    PlanNodeMismatch,
    /// The batch did not strictly increase.
    SequenceConflict,
    /// A previously unseen sequence arrived after this plan node's terminal
    /// marker.
    AfterNoMoreSplits,
    /// The queue was closed; it accepts nothing further.
    Closed,
    /// The offer would push retained bytes past the configured ceiling.
    ResourceExhausted,
}

impl fmt::Display for SplitQueueErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::PlanNodeMismatch => "plan node mismatch",
            Self::SequenceConflict => "sequence conflict",
            Self::AfterNoMoreSplits => "split after no-more-splits",
            Self::Closed => "queue closed",
            Self::ResourceExhausted => "resource exhausted",
        })
    }
}

/// A typed rejection of one split offer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SplitQueueError {
    kind: SplitQueueErrorKind,
    plan_node_id: i32,
    sequence_id: Option<u64>,
    detail: String,
}

impl SplitQueueError {
    pub fn new(
        kind: SplitQueueErrorKind,
        plan_node_id: i32,
        sequence_id: Option<u64>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            plan_node_id,
            sequence_id,
            detail: detail.into(),
        }
    }

    pub const fn kind(&self) -> SplitQueueErrorKind {
        self.kind
    }

    pub const fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }

    pub const fn sequence_id(&self) -> Option<u64> {
        self.sequence_id
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for SplitQueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} for plan node {}",
            self.kind, self.plan_node_id
        )?;
        if let Some(sequence_id) = self.sequence_id {
            write!(formatter, " sequence {sequence_id}")?;
        }
        write!(formatter, ": {}", self.detail)
    }
}

impl std::error::Error for SplitQueueError {}

/// Budgets one plan-node queue enforces.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SplitQueueConfig {
    /// Ceiling on the retained bytes of splits waiting to be polled. This is
    /// the backpressure signal: a sender that ignores the reported queue depth
    /// is rejected rather than allowed to grow the queue without bound.
    pub max_queued_bytes: u64,
}

impl Default for SplitQueueConfig {
    fn default() -> Self {
        Self {
            // Roles select their protocol budget explicitly. Execution has no
            // dependency on the central wire contract.
            max_queued_bytes: 64 * 1024 * 1024,
        }
    }
}

/// What one accepted offer changed.
///
/// `max_accepted_sequence` and `no_more_splits` are exactly the per-plan-node
/// acknowledgement the sender needs to decide what to retransmit.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SplitOfferOutcome {
    /// Sequences newly enqueued by this offer, in offer order.
    pub enqueued: Vec<u64>,
    /// Sequences at or below the accepted watermark; they enqueued nothing.
    pub duplicate_sequences: Vec<u64>,
    /// The highest sequence this plan node has accepted.
    pub max_accepted_sequence: Option<u64>,
    /// Whether this plan node has seen its terminal marker.
    pub no_more_splits: bool,
}

/// The result of a sequence-only preflight. Roles use this to avoid decoding
/// duplicates and to preserve queue-state rejection priority.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SplitSequencePreflight {
    duplicate_sequences: Vec<u64>,
    new_sequences: Vec<u64>,
}

impl SplitSequencePreflight {
    pub fn duplicate_sequences(&self) -> &[u64] {
        &self.duplicate_sequences
    }

    pub fn new_sequences(&self) -> &[u64] {
        &self.new_sequences
    }

    pub fn is_duplicate(&self, sequence_id: u64) -> bool {
        self.duplicate_sequences.binary_search(&sequence_id).is_ok()
    }
}

/// Observable depth of one plan-node queue.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SplitQueueStats {
    pub queued_splits: usize,
    pub queued_bytes: u64,
    /// Number of splits skipped because their sequence was at or below the
    /// accepted watermark. This is observability only: it never retains or
    /// compares split payloads.
    pub duplicate_splits_skipped: u64,
    pub max_accepted_sequence: Option<u64>,
    pub no_more_splits: bool,
    pub closed: bool,
}

/// The result of asking a queue for its next split.
///
/// Three states, because an empty queue is not end of stream: only a drained
/// terminal marker or a close is.
#[derive(Debug)]
pub enum SplitPoll<S> {
    /// One split is ready to run.
    Ready(S),
    /// Nothing right now. The caller parks and is woken through
    /// [`SplitQueue::observable`].
    Blocked,
    /// No split will ever be produced again: drained after the terminal marker,
    /// or closed.
    Exhausted,
}

struct SplitQueueState<S> {
    pending: VecDeque<S>,
    max_accepted_sequence: Option<u64>,
    queued_bytes: u64,
    duplicate_splits_skipped: u64,
    no_more_splits: bool,
    closed: bool,
    #[cfg(novarocks_dev_controls)]
    consumption_gate: Option<Arc<TestSplitConsumptionGate>>,
}

/// What the classification pass decided about one offered split.
enum SplitDecision {
    Enqueue { retained: u64 },
    Duplicate,
}

/// A finite test owner may pause consumption without stopping split delivery.
/// Releasing it wakes the same queue observation used by ordinary empty input.
#[cfg(novarocks_dev_controls)]
pub struct TestSplitConsumptionGate {
    held: AtomicBool,
    observable: Arc<Observable>,
}
#[cfg(novarocks_dev_controls)]
impl TestSplitConsumptionGate {
    pub fn release(&self) {
        let notify = self.observable.defer_notify();
        if self.held.swap(false, Ordering::AcqRel) {
            notify.arm();
        }
    }
    pub fn is_held(&self) -> bool {
        self.held.load(Ordering::Acquire)
    }
}

/// One plan node's runtime split queue inside one task attempt.
///
/// Created empty: a task scan may legally start with zero splits and receive
/// all of them later, or receive none at all.
pub struct SplitQueue<S> {
    plan_node_id: i32,
    config: SplitQueueConfig,
    state: Mutex<SplitQueueState<S>>,
    observable: Arc<Observable>,
}

impl<S: ScheduledSplitFacts> SplitQueue<S> {
    /// An empty queue for one plan node.
    pub fn new(plan_node_id: i32, config: SplitQueueConfig) -> Arc<Self> {
        Arc::new(Self {
            plan_node_id,
            config,
            state: Mutex::new(SplitQueueState {
                pending: VecDeque::new(),
                max_accepted_sequence: None,
                queued_bytes: 0,
                duplicate_splits_skipped: 0,
                no_more_splits: false,
                closed: false,
                #[cfg(novarocks_dev_controls)]
                consumption_gate: None,
            }),
            observable: Arc::new(Observable::new()),
        })
    }

    pub const fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }

    pub const fn config(&self) -> SplitQueueConfig {
        self.config
    }

    /// Waiters register here. Armed on a new split, on the terminal marker, on
    /// the transition to exhausted, and on close.
    pub fn observable(&self) -> Arc<Observable> {
        Arc::clone(&self.observable)
    }

    /// Classify sequence evidence without recovering provider data or mutating
    /// the queue. The caller must still submit every newly decoded split
    /// through [`Self::offer_splits`], which performs the atomic commit and
    /// budget checks.
    pub fn preflight_sequences(
        &self,
        plan_node_id: i32,
        splits: &[SplitSequenceEvidence],
    ) -> Result<SplitSequencePreflight, SplitQueueError> {
        if plan_node_id != self.plan_node_id {
            return Err(self.error(
                SplitQueueErrorKind::PlanNodeMismatch,
                None,
                format!(
                    "assignment for plan node {plan_node_id} reached the queue of plan node {}",
                    self.plan_node_id
                ),
            ));
        }
        let state = self.state.lock().expect("split queue lock");
        if state.closed {
            return Err(self.error(
                SplitQueueErrorKind::Closed,
                None,
                "the queue is closed and accepts no further split",
            ));
        }
        let mut previous = None;
        let mut outcome = SplitSequencePreflight::default();
        for split in splits {
            let sequence_id = split.sequence_id();
            if split.plan_node_id() != self.plan_node_id {
                return Err(self.error(
                    SplitQueueErrorKind::PlanNodeMismatch,
                    Some(sequence_id),
                    "scheduled split belongs to another plan node",
                ));
            }
            if let Some(previous) = previous
                && sequence_id <= previous
            {
                return Err(self.error(
                    SplitQueueErrorKind::SequenceConflict,
                    Some(sequence_id),
                    "offered sequences must strictly increase within one batch",
                ));
            }
            previous = Some(sequence_id);
            if state
                .max_accepted_sequence
                .is_some_and(|watermark| sequence_id <= watermark)
            {
                outcome.duplicate_sequences.push(sequence_id);
            } else {
                if state.no_more_splits {
                    return Err(self.error(
                        SplitQueueErrorKind::AfterNoMoreSplits,
                        Some(sequence_id),
                        "plan node already saw its terminal marker",
                    ));
                }
                outcome.new_sequences.push(sequence_id);
            }
        }
        Ok(outcome)
    }

    /// Accept one plan node's batch.
    ///
    /// The whole batch is classified before anything is committed, so a
    /// rejected offer leaves the queue byte-for-byte as it was.
    pub fn offer_splits(
        &self,
        plan_node_id: i32,
        splits: Vec<S>,
        no_more_splits: bool,
    ) -> Result<SplitOfferOutcome, SplitQueueError> {
        if plan_node_id != self.plan_node_id {
            return Err(self.error(
                SplitQueueErrorKind::PlanNodeMismatch,
                None,
                format!(
                    "assignment for plan node {plan_node_id} reached the queue of plan node {}",
                    self.plan_node_id
                ),
            ));
        }

        let notify = self.observable.defer_notify();
        let mut state = self.state.lock().expect("split queue lock");
        if state.closed {
            return Err(self.error(
                SplitQueueErrorKind::Closed,
                None,
                "the queue is closed and accepts no further split",
            ));
        }

        let mut decisions = Vec::with_capacity(splits.len());
        let mut added_queued_bytes: u64 = 0;
        let mut previous: Option<u64> = None;

        for split in &splits {
            let sequence_id = split.sequence_id();
            if split.plan_node_id() != self.plan_node_id {
                return Err(self.error(
                    SplitQueueErrorKind::PlanNodeMismatch,
                    Some(sequence_id),
                    "scheduled split belongs to another plan node",
                ));
            }
            // The wire contract already requires strictly increasing sequences
            // inside one assignment; re-checking keeps that guarantee for every
            // caller of this generic entry point.
            if let Some(previous) = previous
                && sequence_id <= previous
            {
                return Err(self.error(
                    SplitQueueErrorKind::SequenceConflict,
                    Some(sequence_id),
                    "offered sequences must strictly increase within one batch",
                ));
            }
            previous = Some(sequence_id);

            if state
                .max_accepted_sequence
                .is_some_and(|watermark| sequence_id <= watermark)
            {
                // Duplicate classification intentionally ignores payload. The
                // receiver records only a monotonically advancing watermark,
                // so retransmission never retains or compares provider bytes.
                decisions.push(SplitDecision::Duplicate);
            } else {
                if state.no_more_splits {
                    return Err(self.error(
                        SplitQueueErrorKind::AfterNoMoreSplits,
                        Some(sequence_id),
                        "plan node already saw its terminal marker",
                    ));
                }
                let retained = split.retained_size_in_bytes();
                added_queued_bytes = added_queued_bytes.saturating_add(retained);
                decisions.push(SplitDecision::Enqueue { retained });
            }
        }

        let projected_queued = state.queued_bytes.saturating_add(added_queued_bytes);
        if projected_queued > self.config.max_queued_bytes {
            return Err(self.error(
                SplitQueueErrorKind::ResourceExhausted,
                None,
                format!(
                    "offer would retain {projected_queued} queued bytes, above the ceiling of {}",
                    self.config.max_queued_bytes
                ),
            ));
        }
        let mut outcome = SplitOfferOutcome::default();
        for (split, decision) in splits.into_iter().zip(decisions) {
            let sequence_id = split.sequence_id();
            match decision {
                SplitDecision::Enqueue { retained } => {
                    state.max_accepted_sequence = Some(sequence_id);
                    state.queued_bytes = state.queued_bytes.saturating_add(retained);
                    state.pending.push_back(split);
                    outcome.enqueued.push(sequence_id);
                }
                SplitDecision::Duplicate => outcome.duplicate_sequences.push(sequence_id),
            }
        }
        state.duplicate_splits_skipped = state
            .duplicate_splits_skipped
            .saturating_add(outcome.duplicate_sequences.len() as u64);

        // The terminal marker is idempotent: only the first transition wakes.
        let marked_terminal = no_more_splits && !state.no_more_splits;
        if marked_terminal {
            state.no_more_splits = true;
        }
        outcome.max_accepted_sequence = state.max_accepted_sequence;
        outcome.no_more_splits = state.no_more_splits;

        if !outcome.enqueued.is_empty() || marked_terminal {
            notify.arm();
        }
        Ok(outcome)
    }

    /// Reserves a real input-consumption pause before a test-faulted enqueue.
    /// A closed queue can never be reopened, including by a controlled test.
    #[cfg(novarocks_dev_controls)]
    pub fn reserve_test_consumption_gate(&self) -> Result<Arc<TestSplitConsumptionGate>, String> {
        let mut state = self.state.lock().expect("split queue lock");
        if state.closed {
            return Err("cannot gate a closed split queue".into());
        }
        if state
            .consumption_gate
            .as_ref()
            .is_some_and(|gate| gate.is_held())
        {
            return Err("split queue already has a held consumption gate".into());
        }
        let gate = Arc::new(TestSplitConsumptionGate {
            held: AtomicBool::new(true),
            observable: Arc::clone(&self.observable),
        });
        state.consumption_gate = Some(Arc::clone(&gate));
        Ok(gate)
    }

    /// Take the next split, or report why there is none.
    pub fn poll(&self) -> SplitPoll<S> {
        let notify = self.observable.defer_notify();
        let mut state = self.state.lock().expect("split queue lock");
        if state.closed {
            return SplitPoll::Exhausted;
        }
        #[cfg(novarocks_dev_controls)]
        if let Some(gate) = state.consumption_gate.as_ref() {
            if gate.is_held() {
                return SplitPoll::Blocked;
            }
            state.consumption_gate = None;
        }
        if let Some(split) = state.pending.pop_front() {
            state.queued_bytes = state
                .queued_bytes
                .saturating_sub(split.retained_size_in_bytes());
            // Popping the last split of a terminated plan node is itself a
            // state change siblings must observe, otherwise a driver parked on
            // "blocked" would never learn that the queue is now exhausted.
            if state.pending.is_empty() && state.no_more_splits {
                notify.arm();
            }
            return SplitPoll::Ready(split);
        }
        if state.no_more_splits {
            return SplitPoll::Exhausted;
        }
        SplitPoll::Blocked
    }

    /// Close the queue and wake every waiter exactly once.
    ///
    /// This is also the cancellation path. It is idempotent, drops any split
    /// still queued (nothing will run it), and leaves no waiter parked.
    pub fn close(&self) {
        let notify = self.observable.defer_notify();
        let mut state = self.state.lock().expect("split queue lock");
        if state.closed {
            return;
        }
        state.closed = true;
        #[cfg(novarocks_dev_controls)]
        let gate = state.consumption_gate.take();
        state.pending.clear();
        state.queued_bytes = 0;
        drop(state);
        #[cfg(novarocks_dev_controls)]
        if let Some(gate) = gate {
            gate.release();
        }
        notify.arm();
    }

    /// Whether more splits may still arrive. Mirrors the scan dispatch latch so
    /// a source operator can combine both the same way.
    pub fn has_more(&self) -> bool {
        let state = self.state.lock().expect("split queue lock");
        !state.closed && !state.no_more_splits
    }

    pub fn queue_empty(&self) -> bool {
        let state = self.state.lock().expect("split queue lock");
        state.pending.is_empty()
    }

    /// Whether the queue can never produce another split.
    pub fn is_exhausted(&self) -> bool {
        let state = self.state.lock().expect("split queue lock");
        state.closed || (state.no_more_splits && state.pending.is_empty())
    }

    pub fn no_more_splits(&self) -> bool {
        let state = self.state.lock().expect("split queue lock");
        state.no_more_splits
    }

    pub fn is_closed(&self) -> bool {
        let state = self.state.lock().expect("split queue lock");
        state.closed
    }

    /// Record duplicates classified by a role before provider decoding.
    ///
    /// `preflight_sequences` intentionally leaves queue state unchanged, so a
    /// role calls this only after the matching offer has committed. Duplicates
    /// discovered again during that commit are accounted for by
    /// [`Self::offer_splits`].
    pub fn record_preflight_duplicate_splits(&self, count: usize) {
        if count == 0 {
            return;
        }
        let mut state = self.state.lock().expect("split queue lock");
        state.duplicate_splits_skipped =
            state.duplicate_splits_skipped.saturating_add(count as u64);
    }

    pub fn stats(&self) -> SplitQueueStats {
        let state = self.state.lock().expect("split queue lock");
        SplitQueueStats {
            queued_splits: state.pending.len(),
            queued_bytes: state.queued_bytes,
            duplicate_splits_skipped: state.duplicate_splits_skipped,
            max_accepted_sequence: state.max_accepted_sequence,
            no_more_splits: state.no_more_splits,
            closed: state.closed,
        }
    }

    fn error(
        &self,
        kind: SplitQueueErrorKind,
        sequence_id: Option<u64>,
        detail: impl Into<String>,
    ) -> SplitQueueError {
        SplitQueueError::new(kind, self.plan_node_id, sequence_id, detail)
    }
}

/// The task attempt one set of queues belongs to.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskAttemptKey {
    execution_id: QueryExecutionId,
    fragment_instance_id: UniqueId,
}

impl TaskAttemptKey {
    pub const fn new(execution_id: QueryExecutionId, fragment_instance_id: UniqueId) -> Self {
        Self {
            execution_id,
            fragment_instance_id,
        }
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }
}

impl fmt::Display for TaskAttemptKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "query={} attempt={} finst={}",
            self.execution_id.query_id(),
            self.execution_id.attempt_id().get(),
            self.fragment_instance_id
        )
    }
}

/// Every plan-node queue of exactly one task attempt.
///
/// The attempt id is part of the key, so a retry never reaches these queues: it
/// keys a different entry, which is allocated fresh with an empty sequence
/// space. There is no reopen, so a handle to a closed
/// attempt can never be revived into a live one.
pub struct TaskAttemptSplitQueues<S> {
    key: TaskAttemptKey,
    config: SplitQueueConfig,
    nodes: Mutex<HashMap<i32, Arc<SplitQueue<S>>>>,
    closed: AtomicBool,
}

impl<S: ScheduledSplitFacts> TaskAttemptSplitQueues<S> {
    fn new(key: TaskAttemptKey, config: SplitQueueConfig) -> Arc<Self> {
        Arc::new(Self {
            key,
            config,
            nodes: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
        })
    }

    pub const fn key(&self) -> TaskAttemptKey {
        self.key
    }

    pub const fn config(&self) -> SplitQueueConfig {
        self.config
    }

    /// The queue for one plan node, created empty on first use.
    ///
    /// Both sides call this: the scan operator when it starts, possibly before
    /// any split has arrived, and the split receiver when the first assignment
    /// lands. After the attempt is closed this hands back a closed queue rather
    /// than growing the map, so a late caller observes termination instead of a
    /// queue that can never be served.
    pub fn queue(&self, plan_node_id: i32) -> Arc<SplitQueue<S>> {
        if self.closed.load(Ordering::Acquire) {
            let queue = SplitQueue::new(plan_node_id, self.config);
            queue.close();
            return queue;
        }
        let mut nodes = self.nodes.lock().expect("task attempt split queues lock");
        if self.closed.load(Ordering::Acquire) {
            let queue = SplitQueue::new(plan_node_id, self.config);
            queue.close();
            return queue;
        }
        Arc::clone(
            nodes
                .entry(plan_node_id)
                .or_insert_with(|| SplitQueue::new(plan_node_id, self.config)),
        )
    }

    /// The queue for one plan node, without creating it.
    pub fn existing_queue(&self, plan_node_id: i32) -> Option<Arc<SplitQueue<S>>> {
        let nodes = self.nodes.lock().expect("task attempt split queues lock");
        nodes.get(&plan_node_id).map(Arc::clone)
    }

    pub fn plan_node_ids(&self) -> Vec<i32> {
        let nodes = self.nodes.lock().expect("task attempt split queues lock");
        let mut ids: Vec<i32> = nodes.keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    pub fn stats(&self) -> Vec<(i32, SplitQueueStats)> {
        let queues: Vec<(i32, Arc<SplitQueue<S>>)> = {
            let nodes = self.nodes.lock().expect("task attempt split queues lock");
            nodes
                .iter()
                .map(|(plan_node_id, queue)| (*plan_node_id, Arc::clone(queue)))
                .collect()
        };
        let mut stats: Vec<(i32, SplitQueueStats)> = queues
            .into_iter()
            .map(|(plan_node_id, queue)| (plan_node_id, queue.stats()))
            .collect();
        stats.sort_unstable_by_key(|(plan_node_id, _)| *plan_node_id);
        stats
    }

    /// Close every queue of this attempt. Idempotent; each queue wakes once.
    pub fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let queues: Vec<Arc<SplitQueue<S>>> = {
            let mut nodes = self.nodes.lock().expect("task attempt split queues lock");
            nodes.drain().map(|(_, queue)| queue).collect()
        };
        for queue in queues {
            queue.close();
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

/// The owner of every live task attempt's split queues in one process role.
///
/// This is a plain owned object, deliberately not a process global: it is
/// created by whatever already owns query-scoped runtime state and dies with
/// it.
pub struct SplitQueueRegistry<S> {
    attempts: Mutex<HashMap<TaskAttemptKey, Arc<TaskAttemptSplitQueues<S>>>>,
}

impl<S: ScheduledSplitFacts> SplitQueueRegistry<S> {
    pub fn new() -> Self {
        Self {
            attempts: Mutex::new(HashMap::new()),
        }
    }

    /// The queues of one task attempt, created on first use.
    ///
    /// Idempotent for one key: the same attempt always sees the same queues.
    /// A different attempt is a different key and therefore a different, empty
    /// set of queues; no state from an earlier attempt can reach it.
    pub fn open_attempt(
        &self,
        key: TaskAttemptKey,
        config: SplitQueueConfig,
    ) -> Arc<TaskAttemptSplitQueues<S>> {
        let mut attempts = self.attempts.lock().expect("split queue registry lock");
        Arc::clone(
            attempts
                .entry(key)
                .or_insert_with(|| TaskAttemptSplitQueues::new(key, config)),
        )
    }

    pub fn attempt(&self, key: TaskAttemptKey) -> Option<Arc<TaskAttemptSplitQueues<S>>> {
        let attempts = self.attempts.lock().expect("split queue registry lock");
        attempts.get(&key).map(Arc::clone)
    }

    /// Drop one attempt's queues and close them. Idempotent; returns whether
    /// this call was the one that removed the entry.
    pub fn close_attempt(&self, key: TaskAttemptKey) -> bool {
        let attempt = {
            let mut attempts = self.attempts.lock().expect("split queue registry lock");
            attempts.remove(&key)
        };
        match attempt {
            Some(attempt) => {
                attempt.close();
                true
            }
            None => false,
        }
    }

    /// Drop every queue owned by one query execution. The lifecycle owner uses
    /// this only after the attempt reaches its bounded terminal tombstone, so
    /// a late TaskUpdate can still confirm an already accepted watermark.
    pub fn close_execution(&self, execution_id: QueryExecutionId) -> usize {
        let attempts = {
            let mut attempts = self.attempts.lock().expect("split queue registry lock");
            let keys = attempts
                .keys()
                .filter(|key| key.execution_id() == execution_id)
                .copied()
                .collect::<Vec<_>>();
            keys.into_iter()
                .filter_map(|key| attempts.remove(&key))
                .collect::<Vec<_>>()
        };
        let count = attempts.len();
        for attempt in attempts {
            attempt.close();
        }
        count
    }

    pub fn len(&self) -> usize {
        let attempts = self.attempts.lock().expect("split queue registry lock");
        attempts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<S: ScheduledSplitFacts> Default for SplitQueueRegistry<S> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use novarocks_types::{AttemptId, QueryId};

    use super::*;

    /// A neutral stand-in for one scheduled split.
    ///
    /// The wire type requires a present provider variant, so building it here
    /// would put a provider name into a file that must stay provider-free. The
    /// queue only ever sees [`ScheduledSplitFacts`], so exercising it through
    /// this view tests exactly the production code path.
    #[derive(Clone, Debug)]
    struct TestSplit {
        sequence_id: u64,
        plan_node_id: i32,
        retained: u64,
    }

    impl TestSplit {
        fn new(sequence_id: u64, plan_node_id: i32, payload: &[u8]) -> Self {
            Self {
                sequence_id,
                plan_node_id,
                retained: payload.len() as u64,
            }
        }

        fn with_retained(mut self, retained: u64) -> Self {
            self.retained = retained;
            self
        }
    }

    impl ScheduledSplitFacts for TestSplit {
        fn sequence_id(&self) -> u64 {
            self.sequence_id
        }

        fn plan_node_id(&self) -> i32 {
            self.plan_node_id
        }

        fn retained_size_in_bytes(&self) -> u64 {
            self.retained
        }
    }

    const NODE: i32 = 7;

    fn queue() -> Arc<SplitQueue<TestSplit>> {
        SplitQueue::new(NODE, SplitQueueConfig::default())
    }

    fn attempt_key(attempt: u64) -> TaskAttemptKey {
        TaskAttemptKey::new(
            QueryExecutionId::new(
                QueryId::new(1, 2),
                AttemptId::new(attempt).expect("attempt"),
            )
            .expect("execution id"),
            UniqueId::new(3, 4),
        )
    }

    fn counting_observer(queue: &SplitQueue<TestSplit>) -> Arc<AtomicUsize> {
        let counter = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&counter);
        queue.observable().add_observer(Arc::new(move || {
            observed.fetch_add(1, Ordering::AcqRel);
        }));
        counter
    }

    #[test]
    fn zero_splits_then_no_more_splits_is_a_clean_end_of_stream() {
        let queue = queue();
        assert!(matches!(queue.poll(), SplitPoll::Blocked));

        let outcome = queue
            .offer_splits(NODE, Vec::new(), true)
            .expect("terminal marker only");
        assert!(outcome.enqueued.is_empty());
        assert!(outcome.duplicate_sequences.is_empty());
        assert_eq!(outcome.max_accepted_sequence, None);
        assert!(outcome.no_more_splits);

        // No synthetic empty split is manufactured to carry the marker.
        assert_eq!(queue.stats().queued_splits, 0);
        assert!(matches!(queue.poll(), SplitPoll::Exhausted));
        assert!(queue.is_exhausted());
    }

    #[test]
    fn an_empty_queue_is_blocked_and_a_later_offer_wakes_it() {
        let queue = queue();
        let woken = counting_observer(&queue);
        assert!(matches!(queue.poll(), SplitPoll::Blocked));
        assert_eq!(woken.load(Ordering::Acquire), 0);

        queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], false)
            .expect("first split");
        assert_eq!(woken.load(Ordering::Acquire), 1);

        match queue.poll() {
            SplitPoll::Ready(split) => assert_eq!(split.sequence_id(), 1),
            SplitPoll::Blocked | SplitPoll::Exhausted => panic!("the offered split must be ready"),
        }
        // Still not end of stream: no terminal marker has arrived.
        assert!(matches!(queue.poll(), SplitPoll::Blocked));
    }

    #[test]
    fn a_duplicate_offer_does_not_wake_waiters_or_consume_bytes() {
        let queue = queue();
        queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], false)
            .expect("first split");
        let before = queue.stats();
        let woken = counting_observer(&queue);
        queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"different")], false)
            .expect("duplicate with rewritten payload");
        assert_eq!(woken.load(Ordering::Acquire), 0);
        let after = queue.stats();
        assert_eq!(after.queued_splits, before.queued_splits);
        assert_eq!(after.queued_bytes, before.queued_bytes);
        assert_eq!(after.duplicate_splits_skipped, 1);
    }

    #[test]
    fn duplicate_classification_uses_the_sequence_watermark_not_payload_bytes() {
        let queue = queue();
        let first = queue
            .offer_splits(
                NODE,
                vec![TestSplit::new(1, NODE, b"a"), TestSplit::new(2, NODE, b"b")],
                false,
            )
            .expect("first offer");
        assert_eq!(first.enqueued, vec![1, 2]);

        let duplicate = queue
            .offer_splits(
                NODE,
                vec![
                    TestSplit::new(1, NODE, b"rewritten-one"),
                    TestSplit::new(2, NODE, b"rewritten-two"),
                ],
                false,
            )
            .expect("duplicate batch");
        assert!(duplicate.enqueued.is_empty());
        assert_eq!(duplicate.duplicate_sequences, vec![1, 2]);
        assert_eq!(duplicate.max_accepted_sequence, Some(2));
        assert_eq!(queue.stats().queued_splits, 2);
        assert_eq!(queue.stats().duplicate_splits_skipped, 2);
    }

    #[test]
    fn sequence_preflight_classifies_duplicates_without_mutating_the_queue() {
        let queue = queue();
        queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"one")], false)
            .expect("first offer");

        let preflight = queue
            .preflight_sequences(
                NODE,
                &[
                    SplitSequenceEvidence::new(1, NODE),
                    SplitSequenceEvidence::new(2, NODE),
                ],
            )
            .expect("duplicate and new tail are admissible");
        assert_eq!(preflight.duplicate_sequences(), &[1]);
        assert_eq!(preflight.new_sequences(), &[2]);
        assert_eq!(queue.stats().queued_splits, 1);
        assert_eq!(queue.stats().duplicate_splits_skipped, 0);
        queue.record_preflight_duplicate_splits(preflight.duplicate_sequences().len());
        assert_eq!(queue.stats().duplicate_splits_skipped, 1);
    }

    #[test]
    fn sequence_preflight_treats_a_watermark_hole_as_a_duplicate() {
        let queue = queue();
        queue
            .offer_splits(NODE, vec![TestSplit::new(5, NODE, b"five")], false)
            .expect("first offer");

        let preflight = queue
            .preflight_sequences(
                NODE,
                &[
                    SplitSequenceEvidence::new(3, NODE),
                    SplitSequenceEvidence::new(6, NODE),
                ],
            )
            .expect("watermark hole is duplicate and tail is new");
        assert_eq!(preflight.duplicate_sequences(), &[3]);
        assert_eq!(preflight.new_sequences(), &[6]);
    }

    #[test]
    fn a_mixed_duplicate_and_new_batch_enqueues_only_the_new_tail() {
        let queue = queue();
        queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], false)
            .expect("first offer");
        let outcome = queue
            .offer_splits(
                NODE,
                vec![TestSplit::new(1, NODE, b"a"), TestSplit::new(2, NODE, b"b")],
                false,
            )
            .expect("retransmission with a new tail");
        assert_eq!(outcome.duplicate_sequences, vec![1]);
        assert_eq!(outcome.enqueued, vec![2]);
    }

    #[test]
    fn a_sequence_below_the_watermark_is_a_duplicate_even_when_never_queued() {
        let queue = queue();
        queue
            .offer_splits(NODE, vec![TestSplit::new(5, NODE, b"a")], false)
            .expect("first offer");
        let outcome = queue
            .offer_splits(NODE, vec![TestSplit::new(3, NODE, b"hole")], false)
            .expect("sequence below the watermark is duplicate");
        assert_eq!(outcome.duplicate_sequences, vec![3]);
        assert!(outcome.enqueued.is_empty());
        assert_eq!(queue.stats().queued_splits, 1);
    }

    #[test]
    fn a_batch_whose_sequences_do_not_increase_is_a_conflict() {
        let queue = queue();
        let error = queue
            .offer_splits(
                NODE,
                vec![TestSplit::new(2, NODE, b"a"), TestSplit::new(2, NODE, b"b")],
                false,
            )
            .expect_err("a repeated sequence inside one batch is a conflict");
        assert_eq!(error.kind(), SplitQueueErrorKind::SequenceConflict);
        assert_eq!(queue.stats().max_accepted_sequence, None);
    }

    #[test]
    fn a_new_split_after_the_terminal_marker_is_rejected() {
        let queue = queue();
        queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], true)
            .expect("terminal batch");
        let error = queue
            .offer_splits(NODE, vec![TestSplit::new(2, NODE, b"b")], false)
            .expect_err("a split after the terminal marker must be rejected");
        assert_eq!(error.kind(), SplitQueueErrorKind::AfterNoMoreSplits);
        assert_eq!(error.sequence_id(), Some(2));
        assert_eq!(queue.stats().queued_splits, 1);
    }

    #[cfg(novarocks_dev_controls)]
    #[test]
    fn reserved_consumption_keeps_original_input_for_live_duplicate_then_wakes_once() {
        let queue = queue();
        let gate = queue.reserve_test_consumption_gate().unwrap();
        queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"abc")], true)
            .unwrap();
        let wake = counting_observer(&queue);
        let before = queue.stats();
        // Both ordinary and speculative callers use this same poll seam.
        assert!(matches!(queue.poll(), SplitPoll::Blocked));
        assert!(matches!(queue.poll(), SplitPoll::Blocked));
        assert_eq!(queue.stats().queued_splits, 1);
        assert_eq!(queue.stats().queued_bytes, before.queued_bytes);
        let duplicate = queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"ignored")], true)
            .unwrap();
        assert_eq!(duplicate.duplicate_sequences, vec![1]);
        assert!(duplicate.enqueued.is_empty());
        assert_eq!(queue.stats().duplicate_splits_skipped, 1);
        assert!(matches!(queue.poll(), SplitPoll::Blocked));
        gate.release();
        gate.release();
        assert_eq!(wake.load(Ordering::Acquire), 1);
        let SplitPoll::Ready(original) = queue.poll() else {
            panic!("original input is released once");
        };
        assert_eq!(original.retained_size_in_bytes(), 3);
        assert!(matches!(queue.poll(), SplitPoll::Exhausted));
        assert!(!queue.is_closed());
    }

    #[cfg(novarocks_dev_controls)]
    #[test]
    fn consumption_gate_cannot_delay_close_or_reopen_closed_input() {
        let queue = queue();
        let gate = queue.reserve_test_consumption_gate().unwrap();
        queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], true)
            .unwrap();
        queue.close();
        assert!(!gate.is_held());
        assert!(matches!(queue.poll(), SplitPoll::Exhausted));
        assert!(queue.reserve_test_consumption_gate().is_err());
        assert_eq!(queue.stats().queued_splits, 0);
        gate.release();
        assert!(matches!(queue.poll(), SplitPoll::Exhausted));
    }

    #[cfg(novarocks_dev_controls)]
    #[test]
    fn a_reservation_precedes_enqueue_even_when_the_consumer_is_already_polling() {
        let queue = queue();
        let gate = queue.reserve_test_consumption_gate().unwrap();
        let ready = Arc::new(std::sync::Barrier::new(2));
        let consumer = {
            let queue = Arc::clone(&queue);
            let ready = Arc::clone(&ready);
            std::thread::spawn(move || {
                ready.wait();
                for _ in 0..100 {
                    assert!(matches!(queue.poll(), SplitPoll::Blocked));
                }
            })
        };
        ready.wait();
        queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], true)
            .unwrap();
        consumer.join().unwrap();
        assert_eq!(queue.stats().queued_splits, 1);
        gate.release();
        assert!(matches!(queue.poll(), SplitPoll::Ready(_)));
        assert!(matches!(queue.poll(), SplitPoll::Exhausted));
    }

    #[test]
    fn no_more_splits_is_idempotent_and_terminal_retransmission_is_a_duplicate() {
        let queue = queue();
        let first = queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], true)
            .expect("terminal batch");
        assert!(first.no_more_splits);

        let woken = counting_observer(&queue);
        // Retransmitting the terminal batch stays legal and changes nothing,
        // even if its payload differs: the watermark is the acknowledgement.
        let again = queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"rewritten")], true)
            .expect("terminal batch retransmission");
        assert!(again.no_more_splits);
        assert!(again.enqueued.is_empty());
        assert_eq!(again.duplicate_sequences, vec![1]);
        assert_eq!(woken.load(Ordering::Acquire), 0);

        // A bare repeated marker is equally idempotent.
        let bare = queue
            .offer_splits(NODE, Vec::new(), true)
            .expect("bare terminal marker retransmission");
        assert!(bare.no_more_splits);
        assert_eq!(woken.load(Ordering::Acquire), 0);
    }

    #[test]
    fn draining_the_last_split_of_a_terminated_node_is_exhaustion() {
        let queue = queue();
        queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], true)
            .expect("terminal batch");
        let woken = counting_observer(&queue);
        assert!(matches!(queue.poll(), SplitPoll::Ready(_)));
        // Siblings parked on "blocked" must learn that the queue is now done.
        assert_eq!(woken.load(Ordering::Acquire), 1);
        assert!(matches!(queue.poll(), SplitPoll::Exhausted));
    }

    #[test]
    fn close_is_idempotent_and_wakes_every_waiter_exactly_once() {
        let queue = queue();
        let first = counting_observer(&queue);
        let second = counting_observer(&queue);
        queue
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], false)
            .expect("one queued split");
        assert_eq!(first.load(Ordering::Acquire), 1);
        assert_eq!(second.load(Ordering::Acquire), 1);

        queue.close();
        queue.close();
        queue.close();
        assert_eq!(first.load(Ordering::Acquire), 2);
        assert_eq!(second.load(Ordering::Acquire), 2);

        assert!(queue.is_closed());
        assert!(queue.is_exhausted());
        assert_eq!(queue.stats().queued_bytes, 0);
        assert!(matches!(queue.poll(), SplitPoll::Exhausted));

        let error = queue
            .offer_splits(NODE, vec![TestSplit::new(2, NODE, b"b")], false)
            .expect_err("a closed queue accepts nothing");
        assert_eq!(error.kind(), SplitQueueErrorKind::Closed);
    }

    #[test]
    fn an_assignment_for_another_plan_node_is_rejected() {
        let queue = queue();
        let error = queue
            .offer_splits(NODE + 1, Vec::new(), false)
            .expect_err("plan node mismatch");
        assert_eq!(error.kind(), SplitQueueErrorKind::PlanNodeMismatch);
    }

    #[test]
    fn the_retained_bytes_ceiling_rejects_an_oversized_offer() {
        let queue: Arc<SplitQueue<TestSplit>> = SplitQueue::new(
            NODE,
            SplitQueueConfig {
                max_queued_bytes: 64,
            },
        );
        queue
            .offer_splits(
                NODE,
                vec![TestSplit::new(1, NODE, b"a").with_retained(48)],
                false,
            )
            .expect("within the ceiling");
        assert_eq!(queue.stats().queued_bytes, 48);

        let error = queue
            .offer_splits(
                NODE,
                vec![TestSplit::new(2, NODE, b"b").with_retained(32)],
                false,
            )
            .expect_err("the ceiling must reject the offer");
        assert_eq!(error.kind(), SplitQueueErrorKind::ResourceExhausted);
        // Rejected in full: the queue and watermark do not move.
        assert_eq!(queue.stats().queued_splits, 1);

        // Draining frees the budget again, which is what makes this a
        // backpressure signal rather than a permanent cap.
        assert!(matches!(queue.poll(), SplitPoll::Ready(_)));
        assert_eq!(queue.stats().queued_bytes, 0);
        queue
            .offer_splits(
                NODE,
                vec![TestSplit::new(2, NODE, b"b").with_retained(32)],
                false,
            )
            .expect("accepted once the queue drained");
    }

    #[test]
    fn two_attempts_are_isolated() {
        let registry: SplitQueueRegistry<TestSplit> = SplitQueueRegistry::new();
        let first = registry.open_attempt(attempt_key(1), SplitQueueConfig::default());
        let second = registry.open_attempt(attempt_key(2), SplitQueueConfig::default());
        assert!(!Arc::ptr_eq(&first, &second));
        assert_eq!(registry.len(), 2);

        first
            .queue(NODE)
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"a")], true)
            .expect("first attempt");
        // Sequence watermarks are per attempt, so each attempt starts with a
        // fresh sequence space.
        second
            .queue(NODE)
            .offer_splits(NODE, vec![TestSplit::new(1, NODE, b"different")], false)
            .expect("second attempt has a fresh sequence space");
        assert!(first.queue(NODE).no_more_splits());
        assert!(!second.queue(NODE).no_more_splits());

        // Closing one attempt leaves the other untouched, and the closed handle
        // can never be revived.
        assert!(registry.close_attempt(attempt_key(1)));
        assert!(!registry.close_attempt(attempt_key(1)));
        assert!(first.is_closed());
        assert!(first.queue(NODE).is_closed());
        assert!(!second.is_closed());
        assert!(!second.queue(NODE).is_closed());
        assert_eq!(registry.len(), 1);
        assert!(registry.attempt(attempt_key(1)).is_none());

        // Reopening the key allocates a fresh, empty attempt rather than
        // resurrecting the closed one.
        let reopened = registry.open_attempt(attempt_key(1), SplitQueueConfig::default());
        assert!(!Arc::ptr_eq(&reopened, &first));
        assert!(!reopened.is_closed());
        assert_eq!(reopened.queue(NODE).stats(), SplitQueueStats::default());
    }

    #[test]
    fn closing_an_execution_releases_all_of_its_attempts_only() {
        let registry: SplitQueueRegistry<TestSplit> = SplitQueueRegistry::new();
        let execution_id = attempt_key(1).execution_id();
        let first = registry.open_attempt(attempt_key(1), SplitQueueConfig::default());
        let sibling = registry.open_attempt(
            TaskAttemptKey::new(execution_id, UniqueId::new(3, 5)),
            SplitQueueConfig::default(),
        );
        let other = registry.open_attempt(attempt_key(2), SplitQueueConfig::default());

        assert_eq!(registry.close_execution(execution_id), 2);
        assert!(first.is_closed());
        assert!(sibling.is_closed());
        assert!(!other.is_closed());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn one_attempt_keeps_one_queue_per_plan_node() {
        let registry: SplitQueueRegistry<TestSplit> = SplitQueueRegistry::new();
        let attempt = registry.open_attempt(attempt_key(1), SplitQueueConfig::default());
        assert!(Arc::ptr_eq(&attempt.queue(NODE), &attempt.queue(NODE)));
        assert!(attempt.existing_queue(NODE + 1).is_none());
        let other = attempt.queue(NODE + 1);
        assert_eq!(other.plan_node_id(), NODE + 1);
        assert_eq!(attempt.plan_node_ids(), vec![NODE, NODE + 1]);

        // Reopening a live attempt is idempotent: the same key keeps the same
        // queues, so a second registration never resets a running scan.
        let again = registry.open_attempt(attempt_key(1), SplitQueueConfig::default());
        assert!(Arc::ptr_eq(&attempt, &again));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn closing_an_attempt_closes_every_plan_node_queue_once() {
        let registry: SplitQueueRegistry<TestSplit> = SplitQueueRegistry::new();
        let attempt = registry.open_attempt(attempt_key(9), SplitQueueConfig::default());
        let first = attempt.queue(1);
        let second = attempt.queue(2);
        let first_woken = counting_observer(&first);
        let second_woken = counting_observer(&second);

        attempt.close();
        attempt.close();
        assert_eq!(first_woken.load(Ordering::Acquire), 1);
        assert_eq!(second_woken.load(Ordering::Acquire), 1);
        assert!(first.is_closed());
        assert!(second.is_closed());

        // A queue requested after the close is born closed rather than parked.
        let late = attempt.queue(3);
        assert!(late.is_closed());
        assert!(matches!(late.poll(), SplitPoll::Exhausted));
    }
}
