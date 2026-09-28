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

//! The status subscription source of one query context.
//!
//! It holds the current snapshot of every task it knows, the retained gone
//! tombstones, and one coalesced diagnostic queue. There is no history buffer,
//! so a cursor that has fallen behind is answered with the current latest
//! rather than a replay — a frontend that missed versions has missed nothing
//! it can act on, because every snapshot is complete rather than a delta.
//!
//! Every RPC subscription owns its own cursor position and reads these retained
//! facts without consuming them. That property is load-bearing during
//! reconnect: an old HTTP/2 stream may outlive the frontend's local handle for
//! a short time, and it must not be able to take a terminal frame away from the
//! replacement stream. Selection rotates after the last delivered task, so a
//! noisy task cannot starve another task in the same subscription.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroU64;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::{Arc, Mutex};

use novarocks_execution_contract::task_execution::context_convergence::{
    QueryContextConvergenceCursor, QueryContextConvergenceReceipt,
};
use novarocks_execution_contract::task_execution::identity::{QueryContextRef, TaskIdentity};
use novarocks_execution_contract::task_execution::operation::QuiesceQueryContextReceipt;
use novarocks_execution_contract::task_execution::status::{
    TaskStatus, TaskStatusCursor, TaskStatusVersion,
};
use novarocks_execution_contract::task_execution::task_convergence::{
    TaskConvergenceCursor, TaskConvergenceReceipt,
};

/// The facts share one source revision, independent of their protocol versions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoveredObservationFact {
    Status(TaskStatus),
    StatusUnchanged(TaskIdentity),
    Unknown(TaskIdentity),
    Gone(TaskIdentity),
    TaskConvergence(TaskConvergenceReceipt),
    TaskConvergenceUnchanged(TaskIdentity),
    ContextConvergence(QueryContextConvergenceReceipt),
    Quiesce(QuiesceQueryContextReceipt),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoveredObservationFrame {
    CatchUp(CoveredObservationFact),
    CatchUpComplete {
        initial_cut: u64,
    },
    Live {
        revision: u64,
        fact: CoveredObservationFact,
    },
}

/// Selection is not delivery. The caller must acknowledge the exact selected
/// frame only after its ordered transport has accepted the frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectedCoveredObservation {
    pub delivery_id: u64,
    pub frame: CoveredObservationFrame,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct CoveredObservationBookmark {
    pub generation: u64,
    pub sequence: u64,
    pub covered_prefix: u64,
    pub source_cut: u64,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct QuiesceObservationCursor {
    pub context: QueryContextRef,
    pub fence_version: Option<NonZeroU64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CoveredSubscriptionError {
    InvalidGeneration,
    MismatchedContext,
    FutureTaskCursor(TaskIdentity),
    FutureTaskConvergenceCursor(TaskIdentity),
    FutureContextCursor,
    FutureQuiesceCursor,
    TargetLimitExceeded,
    InitialSnapshotTooLarge { bytes: usize, limit: usize },
    FrameTooLarge { bytes: usize, limit: usize },
    WrongDelivery,
}

impl std::fmt::Display for CoveredSubscriptionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "covered observation subscription error: {self:?}"
        )
    }
}

impl std::error::Error for CoveredSubscriptionError {}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum ObservationKey {
    Status(TaskIdentity),
    Gone(TaskIdentity),
    TaskConvergence(TaskIdentity),
    ContextConvergence,
    Quiesce,
}

#[derive(Clone, Debug)]
struct OwedObservation {
    first_uncovered: u64,
    latest_revision: u64,
    fact: CoveredObservationFact,
}

#[derive(Clone, Debug)]
struct SelectedInternal {
    delivery_id: u64,
    frame: CoveredObservationFrame,
    key: Option<ObservationKey>,
    first_uncovered: Option<u64>,
}

#[derive(Debug)]
struct CoveredSubscriberState {
    generation: u64,
    bookmark_sequence: u64,
    initial_cut: u64,
    initial: VecDeque<CoveredObservationFact>,
    initial_complete: bool,
    selected: Option<SelectedInternal>,
    pending: BTreeMap<ObservationKey, OwedObservation>,
    order: BTreeSet<(u64, ObservationKey)>,
    tracked: BTreeSet<TaskIdentity>,
    max_targets: usize,
    overflow_at: Option<u64>,
    next_delivery_id: u64,
}

impl CoveredSubscriberState {
    fn note_change(&mut self, key: ObservationKey, revision: u64, fact: CoveredObservationFact) {
        let identity = match key {
            ObservationKey::Status(identity)
            | ObservationKey::Gone(identity)
            | ObservationKey::TaskConvergence(identity) => Some(identity),
            ObservationKey::ContextConvergence | ObservationKey::Quiesce => None,
        };
        if let Some(identity) = identity
            && !self.tracked.contains(&identity)
        {
            if self.tracked.len() >= self.max_targets {
                self.overflow_at.get_or_insert(revision);
                return;
            }
            self.tracked.insert(identity);
        }
        if let Some(owed) = self.pending.get_mut(&key) {
            owed.latest_revision = revision;
            owed.fact = fact;
        } else {
            self.pending.insert(
                key,
                OwedObservation {
                    first_uncovered: revision,
                    latest_revision: revision,
                    fact,
                },
            );
            self.order.insert((revision, key));
        }
    }

    fn covered_prefix(&self, source_cut: u64) -> u64 {
        if !self.initial_complete {
            return 0;
        }
        let first = self
            .order
            .first()
            .map(|(revision, _)| *revision)
            .into_iter()
            .chain(
                self.selected
                    .as_ref()
                    .and_then(|selected| selected.first_uncovered),
            )
            .chain(self.overflow_at)
            .min();
        first.map_or(source_cut, |revision| revision - 1)
    }
}

/// One private reconnect position. Dropping it removes its bounded pending set.
#[derive(Debug)]
pub struct CoveredSubscription {
    source: Arc<TaskStatusSource>,
    id: u64,
}

impl Drop for CoveredSubscription {
    fn drop(&mut self) {
        self.source
            .state
            .lock()
            .expect("task status source lock")
            .covered_subscribers
            .remove(&self.id);
    }
}

impl CoveredSubscription {
    /// Waits for a publication after `observed_cut`. Registering the Notify
    /// waiter before rechecking the cut prevents a publication from being lost
    /// between the read and park.
    pub async fn wait_for_change(&self, observed_cut: u64) -> u64 {
        loop {
            let woken = self.source.wake.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            let current = self.source_cut();
            if current != observed_cut {
                return current;
            }
            woken.await;
        }
    }

    pub fn source_cut(&self) -> u64 {
        self.source
            .state
            .lock()
            .expect("task status source lock")
            .control_revision
    }

    /// `measure` must return the encoded size of this frame on the selected
    /// transport. A too-small transport budget is an explicit error.
    pub fn select_next(
        &self,
        max_bytes: usize,
        measure: impl FnOnce(&CoveredObservationFrame) -> usize,
    ) -> Result<Option<SelectedCoveredObservation>, CoveredSubscriptionError> {
        let mut source = self.source.state.lock().expect("task status source lock");
        let subscriber = source
            .covered_subscribers
            .get_mut(&self.id)
            .expect("live covered subscription");
        if subscriber.overflow_at.is_some() {
            return Err(CoveredSubscriptionError::TargetLimitExceeded);
        }
        if let Some(selected) = &subscriber.selected {
            let bytes = measure(&selected.frame);
            if bytes > max_bytes {
                return Err(CoveredSubscriptionError::FrameTooLarge {
                    bytes,
                    limit: max_bytes,
                });
            }
            return Ok(Some(SelectedCoveredObservation {
                delivery_id: selected.delivery_id,
                frame: selected.frame.clone(),
            }));
        }
        let (frame, key, first_uncovered) = if let Some(fact) = subscriber.initial.front() {
            (CoveredObservationFrame::CatchUp(fact.clone()), None, None)
        } else if !subscriber.initial_complete {
            (
                CoveredObservationFrame::CatchUpComplete {
                    initial_cut: subscriber.initial_cut,
                },
                None,
                None,
            )
        } else if let Some(&(first, key)) = subscriber.order.first() {
            let owed = subscriber.pending.get(&key).expect("ordered pending fact");
            debug_assert_eq!(owed.first_uncovered, first);
            (
                CoveredObservationFrame::Live {
                    revision: owed.latest_revision,
                    fact: owed.fact.clone(),
                },
                Some(key),
                Some(first),
            )
        } else {
            return Ok(None);
        };
        let bytes = measure(&frame);
        if bytes > max_bytes {
            return Err(CoveredSubscriptionError::FrameTooLarge {
                bytes,
                limit: max_bytes,
            });
        }
        if let Some(key) = key {
            let first = first_uncovered.expect("live selection has first revision");
            subscriber.order.remove(&(first, key));
            subscriber.pending.remove(&key);
        }
        let delivery_id = subscriber
            .next_delivery_id
            .checked_add(1)
            .expect("covered delivery id exhausted");
        subscriber.next_delivery_id = delivery_id;
        subscriber.selected = Some(SelectedInternal {
            delivery_id,
            frame: frame.clone(),
            key,
            first_uncovered,
        });
        Ok(Some(SelectedCoveredObservation { delivery_id, frame }))
    }

    /// Acknowledges an ordered-stream handoff, not merely selection.
    pub fn note_delivered(&self, delivery_id: u64) -> Result<(), CoveredSubscriptionError> {
        let mut source = self.source.state.lock().expect("task status source lock");
        let subscriber = source
            .covered_subscribers
            .get_mut(&self.id)
            .expect("live covered subscription");
        if subscriber
            .selected
            .as_ref()
            .map(|selected| selected.delivery_id)
            != Some(delivery_id)
        {
            return Err(CoveredSubscriptionError::WrongDelivery);
        }
        let selected = subscriber.selected.take().expect("verified selected frame");
        match selected.frame {
            CoveredObservationFrame::CatchUp(_) => {
                subscriber.initial.pop_front();
            }
            CoveredObservationFrame::CatchUpComplete { .. } => {
                subscriber.initial_complete = true;
            }
            CoveredObservationFrame::Live { .. } => {
                debug_assert!(selected.key.is_some());
            }
        }
        source.stats.delivered = source.stats.delivered.saturating_add(1);
        Ok(())
    }

    /// A cut and its honest covered prefix are read under the same source lock.
    pub fn bookmark(&self) -> CoveredObservationBookmark {
        let mut source = self.source.state.lock().expect("task status source lock");
        let source_cut = source.control_revision;
        let subscriber = source
            .covered_subscribers
            .get_mut(&self.id)
            .expect("live covered subscription");
        subscriber.bookmark_sequence = subscriber
            .bookmark_sequence
            .checked_add(1)
            .expect("covered bookmark sequence exhausted");
        CoveredObservationBookmark {
            generation: subscriber.generation,
            sequence: subscriber.bookmark_sequence,
            covered_prefix: subscriber.covered_prefix(source_cut),
            source_cut,
        }
    }
}

/// One immutable observation frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskStatusEvent {
    /// The worker has closed every responsibility of this exact context.
    ContextConvergence(QueryContextConvergenceReceipt),
    /// A full immutable snapshot, never a delta over a previous frame.
    Status(TaskStatus),
    /// Retained state for this task was reclaimed.
    Gone(TaskIdentity),
}

/// One server stream's private observation position.
///
/// It is never stored in [`TaskStatusSource`], so overlapping reconnect streams
/// cannot advance or consume each other's task or context observations.
#[derive(Debug)]
pub struct TaskStatusSubscriptionPosition {
    context_cursor: Option<QueryContextConvergenceCursor>,
    task_versions: BTreeMap<TaskIdentity, Option<TaskStatusVersion>>,
    gone: BTreeSet<TaskIdentity>,
    task_change_revision: u64,
    gone_after_terminal: Option<TaskIdentity>,
}

impl TaskStatusSubscriptionPosition {
    pub fn new(
        task_cursors: &[TaskStatusCursor],
        context_cursor: Option<QueryContextConvergenceCursor>,
    ) -> Self {
        Self {
            context_cursor,
            task_versions: task_cursors
                .iter()
                .map(|cursor| (cursor.identity(), cursor.current_version()))
                .collect(),
            gone: BTreeSet::new(),
            task_change_revision: 0,
            gone_after_terminal: None,
        }
    }

    /// Advances only the stream that actually handed this frame to its caller.
    pub fn note_delivered(&mut self, event: &TaskStatusEvent) {
        match event {
            TaskStatusEvent::ContextConvergence(receipt) => {
                self.context_cursor = Some(QueryContextConvergenceCursor::at(
                    receipt.context(),
                    receipt.version(),
                ));
            }
            TaskStatusEvent::Status(status) => {
                let identity = status.identity();
                self.task_versions.insert(identity, Some(status.version()));
                self.gone.remove(&identity);
            }
            TaskStatusEvent::Gone(identity) => {
                self.gone.insert(*identity);
            }
        }
    }
}

/// Why a context convergence cursor cannot open a subscription.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ContextConvergenceCursorError {
    /// The cursor names another exact worker context.
    MismatchedContext,
    /// The cursor claims a version this worker has not published.
    FutureVersion,
}

impl std::fmt::Display for ContextConvergenceCursorError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MismatchedContext => formatter.write_str(
                "context convergence cursor names a different context than the subscription",
            ),
            Self::FutureVersion => formatter.write_str(
                "context convergence cursor is ahead of the worker's latest observation",
            ),
        }
    }
}

impl std::error::Error for ContextConvergenceCursorError {}

/// How a cursor read was answered.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CursorObservation {
    /// The cursor was behind. This is the current latest snapshot; the
    /// versions in between are deliberately not replayed.
    Current(Box<TaskStatus>),
    /// The cursor already holds the current version.
    UpToDate,
    /// Retained state for this task was reclaimed.
    Gone,
    /// This source has never known the task.
    Unknown,
}

/// Delivery and coalescing counters of one source.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct TaskStatusSourceStats {
    pub published: u64,
    pub delivered: u64,
    /// Non-current snapshots dropped because a newer one superseded them
    /// before any observer took delivery.
    pub coalesced: u64,
}

#[derive(Clone, Debug, Default)]
struct PendingSlot {
    status: Option<TaskStatus>,
    gone: bool,
}

impl PendingSlot {
    fn is_empty(&self) -> bool {
        self.status.is_none() && !self.gone
    }
}

#[derive(Debug, Default)]
struct SourceState {
    latest_context_convergence: Option<QueryContextConvergenceReceipt>,
    latest_quiesce: Option<QuiesceQueryContextReceipt>,
    /// Shared revision of control facts, including status, actual stop,
    /// context convergence, quiesce, and retention gaps.
    control_revision: u64,
    next_covered_subscriber_id: u64,
    covered_subscribers: BTreeMap<u64, CoveredSubscriberState>,
    /// Retained independently of terminal status and Gone. The cumulative
    /// per-context task admission bound keeps this set finite.
    latest_task_convergence: BTreeMap<TaskIdentity, TaskConvergenceReceipt>,
    latest: BTreeMap<TaskIdentity, TaskStatus>,
    /// The last complete terminal remains observable for the same bounded
    /// lifetime as the task's gone fence. A reconnect that was behind must see
    /// this fact before it sees `Gone`.
    reclaimed: BTreeMap<TaskIdentity, TaskStatus>,
    /// Identities whose detailed gone fence expired while this context source
    /// is still reachable. The owning context's cumulative task bound makes
    /// this set finite. It preserves an explicit observation gap (`Gone`) for
    /// a lagging stream after the heavier terminal snapshot is released.
    forgotten: BTreeSet<TaskIdentity>,
    /// Tasks whose creation lost to this context closing underneath it. They
    /// exist only until their physical stop converges and were never
    /// released to observers, so this source answers them as `Unknown` for
    /// its whole lifetime: reclaiming one fences nothing and expires nothing.
    /// The owning context's cumulative task bound makes this set finite.
    withheld: BTreeSet<TaskIdentity>,
    /// One coalesced change-index entry per retained task. Subscriptions walk
    /// this index with private revision cursors instead of rescanning all tasks
    /// for every delivered frame.
    task_change_order: BTreeMap<u64, TaskIdentity>,
    task_change_revision: BTreeMap<TaskIdentity, u64>,
    next_task_change_revision: u64,
    pending: BTreeMap<TaskIdentity, PendingSlot>,
    order: VecDeque<TaskIdentity>,
    stats: TaskStatusSourceStats,
    /// Bumped by anything that can change a task's retirement eligibility.
    /// The owner uses it to skip a context whose tasks cannot have moved.
    revision: u64,
    /// Sticky: set once any task of this context published a failure state.
    failure_seen: bool,
}

/// The observation channel of one query context.
///
/// Taking a frame is a poll, but waiting for one is not. Terminal delivery is
/// on the critical path of every query's completion, so an observer parks on
/// `wake` instead of sampling on a timer, and any state change wakes it.
#[derive(Debug, Default)]
pub struct TaskStatusSource {
    state: Mutex<SourceState>,
    wake: tokio::sync::Notify,
}

impl TaskStatusSource {
    /// Upper bound for the debug-encoded representation of a reconnect cut.
    /// Exact wire frame size is separately checked by `select_next`.
    pub const MAX_INITIAL_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;

    pub fn new() -> Self {
        Self::default()
    }

    fn publish_control_locked(
        state: &mut SourceState,
        key: ObservationKey,
        fact: CoveredObservationFact,
    ) {
        state.control_revision = state
            .control_revision
            .checked_add(1)
            .expect("control observation revision exhausted");
        let revision = state.control_revision;
        for subscriber in state.covered_subscribers.values_mut() {
            subscriber.note_change(key, revision, fact.clone());
        }
    }

    /// Creates the reconnect cut and registers later changes under one lock.
    /// The initial target set is bounded by `max_targets`; catch-up values are
    /// immutable snapshots at `initial_cut`, not a second read after unlock.
    pub fn begin_covered_subscription(
        self: &Arc<Self>,
        context: QueryContextRef,
        generation: u64,
        status_cursors: &[TaskStatusCursor],
        task_convergence_cursors: &[TaskConvergenceCursor],
        context_cursor: Option<QueryContextConvergenceCursor>,
        required_identities: &[TaskIdentity],
        max_targets: usize,
    ) -> Result<CoveredSubscription, CoveredSubscriptionError> {
        self.begin_covered_subscription_with_quiesce(
            context,
            generation,
            status_cursors,
            task_convergence_cursors,
            context_cursor,
            None,
            required_identities,
            max_targets,
        )
    }

    pub fn begin_covered_subscription_with_quiesce(
        self: &Arc<Self>,
        context: QueryContextRef,
        generation: u64,
        status_cursors: &[TaskStatusCursor],
        task_convergence_cursors: &[TaskConvergenceCursor],
        context_cursor: Option<QueryContextConvergenceCursor>,
        quiesce_cursor: Option<QuiesceObservationCursor>,
        required_identities: &[TaskIdentity],
        max_targets: usize,
    ) -> Result<CoveredSubscription, CoveredSubscriptionError> {
        if generation == 0 {
            return Err(CoveredSubscriptionError::InvalidGeneration);
        }
        let mut state = self.state.lock().expect("task status source lock");
        let mut targets = BTreeSet::new();
        for identity in state
            .latest
            .keys()
            .chain(state.reclaimed.keys())
            .chain(state.forgotten.iter())
            .chain(state.latest_task_convergence.keys())
            .copied()
            .chain(
                state
                    .latest_quiesce
                    .iter()
                    .flat_map(|receipt| receipt.accepted_tasks().iter().copied()),
            )
            .chain(status_cursors.iter().map(|cursor| cursor.identity()))
            .chain(
                task_convergence_cursors
                    .iter()
                    .map(|cursor| cursor.identity()),
            )
            .chain(required_identities.iter().copied())
        {
            if identity.verify_query_context(context).is_err() {
                return Err(CoveredSubscriptionError::MismatchedContext);
            }
            targets.insert(identity);
            if targets.len() > max_targets {
                return Err(CoveredSubscriptionError::TargetLimitExceeded);
            }
        }
        let mut status_positions = BTreeMap::new();
        for cursor in status_cursors {
            let identity = cursor.identity();
            let known = state
                .latest
                .get(&identity)
                .or_else(|| state.reclaimed.get(&identity));
            if let Some(version) = cursor.current_version()
                && known.is_none_or(|status| version > status.version())
                && !state.forgotten.contains(&identity)
            {
                return Err(CoveredSubscriptionError::FutureTaskCursor(identity));
            }
            status_positions.insert(identity, cursor.current_version());
        }
        let mut convergence_positions = BTreeMap::new();
        for cursor in task_convergence_cursors {
            let identity = cursor.identity();
            let known = state.latest_task_convergence.get(&identity);
            if let Some(version) = cursor.current_version()
                && known.is_none_or(|receipt| version > receipt.version())
            {
                return Err(CoveredSubscriptionError::FutureTaskConvergenceCursor(
                    identity,
                ));
            }
            convergence_positions.insert(identity, cursor.current_version());
        }
        if let Some(cursor) = context_cursor {
            if cursor.context() != context {
                return Err(CoveredSubscriptionError::MismatchedContext);
            }
            if let Some(version) = cursor.current_version()
                && state
                    .latest_context_convergence
                    .is_none_or(|receipt| version > receipt.version())
            {
                return Err(CoveredSubscriptionError::FutureContextCursor);
            }
        }
        if let Some(cursor) = quiesce_cursor {
            if cursor.context != context {
                return Err(CoveredSubscriptionError::MismatchedContext);
            }
            if let Some(version) = cursor.fence_version
                && state
                    .latest_quiesce
                    .as_ref()
                    .is_none_or(|receipt| version.get() > receipt.fence_version())
            {
                return Err(CoveredSubscriptionError::FutureQuiesceCursor);
            }
        }
        let mut initial = VecDeque::new();
        for identity in &targets {
            let observed = status_positions.get(identity).copied().flatten();
            if let Some(status) = state
                .latest
                .get(identity)
                .or_else(|| state.reclaimed.get(identity))
            {
                if observed == Some(status.version()) {
                    initial.push_back(CoveredObservationFact::StatusUnchanged(*identity));
                } else {
                    initial.push_back(CoveredObservationFact::Status(status.clone()));
                }
            } else if state.forgotten.contains(identity) {
                initial.push_back(CoveredObservationFact::Gone(*identity));
            } else {
                initial.push_back(CoveredObservationFact::Unknown(*identity));
            }
            if state.reclaimed.contains_key(identity) {
                initial.push_back(CoveredObservationFact::Gone(*identity));
            }
            if let Some(receipt) = state.latest_task_convergence.get(identity) {
                if convergence_positions.get(identity).copied().flatten() == Some(receipt.version())
                {
                    initial.push_back(CoveredObservationFact::TaskConvergenceUnchanged(*identity));
                } else {
                    initial.push_back(CoveredObservationFact::TaskConvergence(*receipt));
                }
            }
        }
        if let Some(receipt) = state.latest_context_convergence
            && context_cursor
                .is_none_or(|cursor| cursor.current_version() != Some(receipt.version()))
        {
            initial.push_back(CoveredObservationFact::ContextConvergence(receipt));
        }
        // The fence version names the immutable accepted set, while the
        // closing state may still advance. Replay the current full receipt
        // even when the caller already observed this fence version.
        if let Some(receipt) = &state.latest_quiesce {
            initial.push_back(CoveredObservationFact::Quiesce(receipt.clone()));
        }
        let mut initial_bytes = 0usize;
        for fact in &initial {
            initial_bytes = initial_bytes.checked_add(format!("{fact:?}").len()).ok_or(
                CoveredSubscriptionError::InitialSnapshotTooLarge {
                    bytes: usize::MAX,
                    limit: Self::MAX_INITIAL_SNAPSHOT_BYTES,
                },
            )?;
            if initial_bytes > Self::MAX_INITIAL_SNAPSHOT_BYTES {
                return Err(CoveredSubscriptionError::InitialSnapshotTooLarge {
                    bytes: initial_bytes,
                    limit: Self::MAX_INITIAL_SNAPSHOT_BYTES,
                });
            }
        }
        let id = state
            .next_covered_subscriber_id
            .checked_add(1)
            .expect("covered subscriber id exhausted");
        state.next_covered_subscriber_id = id;
        let subscriber = CoveredSubscriberState {
            generation,
            bookmark_sequence: 0,
            initial_cut: state.control_revision,
            initial,
            initial_complete: false,
            selected: None,
            pending: BTreeMap::new(),
            order: BTreeSet::new(),
            tracked: targets,
            max_targets,
            overflow_at: None,
            next_delivery_id: 0,
        };
        state.covered_subscribers.insert(id, subscriber);
        Ok(CoveredSubscription {
            source: Arc::clone(self),
            id,
        })
    }

    /// Publishes the cumulative normal close cut and subsequent closing state.
    pub fn publish_quiesce(&self, receipt: QuiesceQueryContextReceipt) -> bool {
        let mut state = self.state.lock().expect("task status source lock");
        if let Some(current) = &state.latest_quiesce {
            assert_eq!(current.context(), receipt.context());
            assert_eq!(current.fence_version(), receipt.fence_version());
            assert_eq!(current.accepted_tasks(), receipt.accepted_tasks());
            if current == &receipt {
                return false;
            }
        }
        state.latest_quiesce = Some(receipt.clone());
        Self::publish_control_locked(
            &mut state,
            ObservationKey::Quiesce,
            CoveredObservationFact::Quiesce(receipt),
        );
        state.stats.published = state.stats.published.saturating_add(1);
        drop(state);
        self.wake.notify_waiters();
        true
    }

    /// Publishes one immutable snapshot.
    ///
    /// A snapshot already queued for this task is replaced, which is the one
    /// drop this contract allows: it was not current and no observer had taken
    /// it. A queued terminal snapshot is never replaced.
    pub fn publish(&self, status: TaskStatus) {
        let identity = status.identity();
        let status_state = status.state();
        let mut state = self.state.lock().expect("task status source lock");
        debug_assert!(
            !state.withheld.contains(&identity),
            "a task withheld from observers is never published"
        );
        state.latest.insert(identity, status.clone());
        Self::publish_control_locked(
            &mut state,
            ObservationKey::Status(identity),
            CoveredObservationFact::Status(status.clone()),
        );
        Self::note_task_change_locked(&mut state, identity);
        let slot = state.pending.entry(identity).or_default();
        let was_empty = slot.is_empty();
        let queued_terminal = slot.status.as_ref().is_some_and(TaskStatus::is_terminal);
        let superseded = slot.status.is_some() && !queued_terminal;
        if !queued_terminal {
            slot.status = Some(status);
        }
        if was_empty {
            state.order.push_back(identity);
        }
        state.stats.published = state.stats.published.saturating_add(1);
        if superseded {
            state.stats.coalesced = state.stats.coalesced.saturating_add(1);
        }
        state.revision = state.revision.saturating_add(1);
        if status_state.is_failure() {
            state.failure_seen = true;
        }
        drop(state);
        self.wake.notify_waiters();
    }

    /// Publishes the single closed convergence fact of this exact context.
    ///
    /// Re-entering settlement with the same receipt changes nothing. A
    /// different receipt would mean the worker tried to revise an immutable
    /// convergence fact, which is an owner invariant violation.
    pub fn publish_context_convergence(&self, receipt: QueryContextConvergenceReceipt) -> bool {
        let mut state = self.state.lock().expect("task status source lock");
        if let Some(current) = state.latest_context_convergence {
            assert_eq!(
                current, receipt,
                "query context convergence cannot be revised after publication"
            );
            return false;
        }
        state.latest_context_convergence = Some(receipt);
        Self::publish_control_locked(
            &mut state,
            ObservationKey::ContextConvergence,
            CoveredObservationFact::ContextConvergence(receipt),
        );
        state.stats.published = state.stats.published.saturating_add(1);
        drop(state);
        self.wake.notify_waiters();
        true
    }

    /// Publishes a task's immutable actual-stop fact for reconnect replay.
    /// Status retirement does not remove this fact from the context source.
    pub fn publish_task_convergence(&self, receipt: TaskConvergenceReceipt) -> bool {
        let mut state = self.state.lock().expect("task status source lock");
        if let Some(current) = state.latest_task_convergence.get(&receipt.identity()) {
            assert_eq!(
                *current, receipt,
                "task convergence cannot be revised after publication"
            );
            return false;
        }
        state
            .latest_task_convergence
            .insert(receipt.identity(), receipt);
        Self::publish_control_locked(
            &mut state,
            ObservationKey::TaskConvergence(receipt.identity()),
            CoveredObservationFact::TaskConvergence(receipt),
        );
        state.stats.published = state.stats.published.saturating_add(1);
        state.revision = state.revision.saturating_add(1);
        drop(state);
        self.wake.notify_waiters();
        true
    }

    pub fn latest_task_convergence(
        &self,
        identity: TaskIdentity,
    ) -> Option<TaskConvergenceReceipt> {
        self.state
            .lock()
            .expect("task status source lock")
            .latest_task_convergence
            .get(&identity)
            .copied()
    }

    /// Records that a task lost its creation to this context closing and is
    /// never released to observers.
    ///
    /// Called at the creation's commit, before the task can retire, so its
    /// later reclamation is known to have no observation to fence.
    pub fn withhold(&self, identity: TaskIdentity) {
        let mut state = self.state.lock().expect("task status source lock");
        assert!(
            !state.latest.contains_key(&identity)
                && !state.reclaimed.contains_key(&identity)
                && !state.forgotten.contains(&identity),
            "only a task no observer has seen can be withheld"
        );
        state.withheld.insert(identity);
    }

    /// Records that a task's retained state was reclaimed.
    pub fn mark_gone(&self, identity: TaskIdentity) {
        let mut state = self.state.lock().expect("task status source lock");
        let Some(terminal) = state.latest.remove(&identity) else {
            assert!(
                state.reclaimed.contains_key(&identity)
                    || state.forgotten.contains(&identity)
                    || state.withheld.contains(&identity),
                "a reclaimed task must retain its final observation"
            );
            return;
        };
        assert!(
            terminal.is_terminal(),
            "only a terminal task observation can become a gone fence"
        );
        state.reclaimed.insert(identity, terminal);
        Self::publish_control_locked(
            &mut state,
            ObservationKey::Gone(identity),
            CoveredObservationFact::Gone(identity),
        );
        Self::note_task_change_locked(&mut state, identity);
        let slot = state.pending.entry(identity).or_default();
        let was_empty = slot.is_empty();
        slot.gone = true;
        if was_empty {
            state.order.push_back(identity);
        }
        state.revision = state.revision.saturating_add(1);
        drop(state);
        self.wake.notify_waiters();
    }

    /// Releases detailed observation data when the Registry evicts the gone
    /// fence. A minimal `Gone` remains until the context source itself is
    /// dropped, so an already-open or reconnecting stream observes an explicit
    /// retention gap instead of parking forever.
    pub fn forget_gone(&self, identity: TaskIdentity) {
        let mut state = self.state.lock().expect("task status source lock");
        if state.reclaimed.remove(&identity).is_none() {
            assert!(
                state.forgotten.contains(&identity) || state.withheld.contains(&identity),
                "only a reclaimed observation can expire"
            );
            return;
        }
        state.forgotten.insert(identity);
        Self::publish_control_locked(
            &mut state,
            ObservationKey::Gone(identity),
            CoveredObservationFact::Gone(identity),
        );
        Self::note_task_change_locked(&mut state, identity);
        let slot = state.pending.entry(identity).or_default();
        let was_empty = slot.is_empty();
        slot.status = None;
        slot.gone = true;
        if was_empty {
            state.order.push_back(identity);
        }
        drop(state);
        self.wake.notify_waiters();
    }

    fn note_task_change_locked(state: &mut SourceState, identity: TaskIdentity) {
        let revision = state
            .next_task_change_revision
            .checked_add(1)
            .expect("task observation change revision exhausted");
        state.next_task_change_revision = revision;
        if let Some(previous) = state.task_change_revision.insert(identity, revision) {
            state.task_change_order.remove(&previous);
        }
        assert!(
            state.task_change_order.insert(revision, identity).is_none(),
            "task observation change revisions are unique"
        );
    }

    /// Waits for the next per-task frame without consuming a context frame.
    ///
    /// This preserves the legacy task-only subscription while a
    /// convergence-aware subscriber reconnects with its context cursor.
    pub async fn next_task_event_owned(&self) -> Option<TaskStatusEvent> {
        loop {
            let woken = self.wake.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            if let Some(event) = self.next_task_event() {
                return Some(event);
            }
            woken.await;
            if let Some(event) = self.next_task_event() {
                return Some(event);
            }
        }
    }

    /// Waits for the next frame owed to one subscription's own cursor.
    pub async fn next_subscription_event_owned(
        &self,
        context: QueryContextRef,
        position: &Mutex<TaskStatusSubscriptionPosition>,
    ) -> Result<Option<TaskStatusEvent>, ContextConvergenceCursorError> {
        loop {
            let woken = self.wake.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            if let Some(event) = self.next_subscription_event_at(context, position)? {
                return Ok(Some(event));
            }
            woken.await;
            if let Some(event) = self.next_subscription_event_at(context, position)? {
                return Ok(Some(event));
            }
        }
    }

    /// Reads the next fact owed to one stream without consuming shared state.
    pub fn next_subscription_event_at(
        &self,
        context: QueryContextRef,
        position: &Mutex<TaskStatusSubscriptionPosition>,
    ) -> Result<Option<TaskStatusEvent>, ContextConvergenceCursorError> {
        let mut state = self.state.lock().expect("task status source lock");
        let mut position = position.lock().expect("task status subscription position");
        if let Some(cursor) = position.context_cursor
            && let Some(receipt) = Self::context_event_locked(&state, context, cursor)?
        {
            state.stats.delivered = state.stats.delivered.saturating_add(1);
            return Ok(Some(TaskStatusEvent::ContextConvergence(receipt)));
        }
        let event = Self::task_event_at_locked(&state, &mut position);
        if event.is_some() {
            state.stats.delivered = state.stats.delivered.saturating_add(1);
        }
        Ok(event)
    }

    fn task_event_at_locked(
        state: &SourceState,
        position: &mut TaskStatusSubscriptionPosition,
    ) -> Option<TaskStatusEvent> {
        if let Some(identity) = position.gone_after_terminal.take()
            && !position.gone.contains(&identity)
        {
            return Some(TaskStatusEvent::Gone(identity));
        }

        loop {
            let (&revision, &identity) = state
                .task_change_order
                .range((Excluded(position.task_change_revision), Unbounded))
                .next()?;
            position.task_change_revision = revision;

            if let Some(status) = state.latest.get(&identity) {
                let observed = position.task_versions.get(&identity).copied().flatten();
                if observed.is_none_or(|version| version < status.version()) {
                    return Some(TaskStatusEvent::Status(status.clone()));
                }
                continue;
            }

            let Some(terminal) = state.reclaimed.get(&identity) else {
                if state.forgotten.contains(&identity) && !position.gone.contains(&identity) {
                    return Some(TaskStatusEvent::Gone(identity));
                }
                continue;
            };
            if position.gone.contains(&identity) {
                continue;
            }
            let observed = position.task_versions.get(&identity).copied().flatten();
            if observed.is_none_or(|version| version < terminal.version()) {
                position.gone_after_terminal = Some(identity);
                return Some(TaskStatusEvent::Status(terminal.clone()));
            }
            return Some(TaskStatusEvent::Gone(identity));
        }
    }

    /// Takes one task frame or reads the context fact owed to this cursor.
    pub fn next_subscription_event(
        &self,
        context: QueryContextRef,
        context_cursor: Option<QueryContextConvergenceCursor>,
    ) -> Result<Option<TaskStatusEvent>, ContextConvergenceCursorError> {
        let mut state = self.state.lock().expect("task status source lock");
        if let Some(cursor) = context_cursor
            && let Some(receipt) = Self::context_event_locked(&state, context, cursor)?
        {
            state.stats.delivered = state.stats.delivered.saturating_add(1);
            return Ok(Some(TaskStatusEvent::ContextConvergence(receipt)));
        }
        Ok(Self::next_task_event_locked(&mut state))
    }

    /// Takes the next frame owed to an observer, round-robin across tasks.
    pub fn next_event(&self) -> Option<TaskStatusEvent> {
        let mut state = self.state.lock().expect("task status source lock");
        Self::next_task_event_locked(&mut state)
    }

    /// Takes the next per-task frame and leaves context convergence pending.
    pub fn next_task_event(&self) -> Option<TaskStatusEvent> {
        let mut state = self.state.lock().expect("task status source lock");
        Self::next_task_event_locked(&mut state)
    }

    fn next_task_event_locked(state: &mut SourceState) -> Option<TaskStatusEvent> {
        while let Some(identity) = state.order.pop_front() {
            let Some(mut slot) = state.pending.remove(&identity) else {
                continue;
            };
            if let Some(status) = slot.status.take() {
                if slot.gone {
                    // The reclamation frame keeps its own queue position, so
                    // it cannot displace the terminal snapshot before it.
                    state.pending.insert(identity, slot);
                    state.order.push_back(identity);
                }
                state.stats.delivered = state.stats.delivered.saturating_add(1);
                return Some(TaskStatusEvent::Status(status));
            }
            if slot.gone {
                state.stats.delivered = state.stats.delivered.saturating_add(1);
                return Some(TaskStatusEvent::Gone(identity));
            }
        }
        None
    }

    /// Records a change that no snapshot carries — an output release — so the
    /// owner knows to look at this context again.
    pub fn note_progress(&self) {
        let mut state = self.state.lock().expect("task status source lock");
        state.revision = state.revision.saturating_add(1);
    }

    /// A counter that changes whenever a task of this context might have
    /// become retirable.
    pub fn revision(&self) -> u64 {
        self.state.lock().expect("task status source lock").revision
    }

    /// Whether any task of this context has ever published a failure state.
    pub fn failure_seen(&self) -> bool {
        self.state
            .lock()
            .expect("task status source lock")
            .failure_seen
    }

    /// Answers one per-task cursor.
    pub fn observe(&self, cursor: TaskStatusCursor) -> CursorObservation {
        let state = self.state.lock().expect("task status source lock");
        let identity = cursor.identity();
        if let Some(status) = state.latest.get(&identity) {
            return match cursor.current_version() {
                Some(version) if version >= status.version() => CursorObservation::UpToDate,
                _ => CursorObservation::Current(Box::new(status.clone())),
            };
        }
        if let Some(terminal) = state.reclaimed.get(&identity) {
            return match cursor.current_version() {
                Some(version) if version >= terminal.version() => CursorObservation::Gone,
                _ => CursorObservation::Current(Box::new(terminal.clone())),
            };
        }
        if state.forgotten.contains(&identity) {
            return CursorObservation::Gone;
        }
        CursorObservation::Unknown
    }

    /// The catch-up frames one subscription owes for its cursors.
    pub fn subscribe(&self, cursors: &[TaskStatusCursor]) -> Vec<TaskStatusEvent> {
        cursors
            .iter()
            .filter_map(|cursor| match self.observe(*cursor) {
                CursorObservation::Current(status) => Some(TaskStatusEvent::Status(*status)),
                CursorObservation::Gone => Some(TaskStatusEvent::Gone(cursor.identity())),
                CursorObservation::UpToDate | CursorObservation::Unknown => None,
            })
            .collect()
    }

    /// The context-first catch-up frames one subscription owes.
    ///
    /// A future context cursor fails closed. Treating it as current would let
    /// the caller claim evidence this worker has never published.
    pub fn subscribe_context_aware(
        &self,
        context: QueryContextRef,
        cursors: &[TaskStatusCursor],
        context_cursor: Option<QueryContextConvergenceCursor>,
    ) -> Result<Vec<TaskStatusEvent>, ContextConvergenceCursorError> {
        let context_event = match context_cursor {
            Some(cursor) => {
                let state = self.state.lock().expect("task status source lock");
                Self::context_event_locked(&state, context, cursor)?
                    .map(TaskStatusEvent::ContextConvergence)
            }
            None => None,
        };
        let mut catch_up = Vec::new();
        if let Some(event) = context_event {
            catch_up.push(event);
        }
        catch_up.extend(self.subscribe(cursors));
        Ok(catch_up)
    }

    fn context_event_locked(
        state: &SourceState,
        context: QueryContextRef,
        cursor: QueryContextConvergenceCursor,
    ) -> Result<Option<QueryContextConvergenceReceipt>, ContextConvergenceCursorError> {
        if cursor.context() != context {
            return Err(ContextConvergenceCursorError::MismatchedContext);
        }
        match (state.latest_context_convergence, cursor.current_version()) {
            (None, None) => Ok(None),
            (None, Some(_)) => Err(ContextConvergenceCursorError::FutureVersion),
            (Some(receipt), None) => Ok(Some(receipt)),
            (Some(receipt), Some(version)) if version < receipt.version() => Ok(Some(receipt)),
            (Some(receipt), Some(version)) if version == receipt.version() => Ok(None),
            (Some(_), Some(_)) => Err(ContextConvergenceCursorError::FutureVersion),
        }
    }

    /// The retained convergence observation, if it has been published.
    pub fn latest_context_convergence(&self) -> Option<QueryContextConvergenceReceipt> {
        self.state
            .lock()
            .expect("task status source lock")
            .latest_context_convergence
    }

    /// The current snapshot of one task, if this source still holds it.
    pub fn latest(&self, identity: TaskIdentity) -> Option<TaskStatus> {
        self.state
            .lock()
            .expect("task status source lock")
            .latest
            .get(&identity)
            .cloned()
    }

    pub fn stats(&self) -> TaskStatusSourceStats {
        self.state.lock().expect("task status source lock").stats
    }

    /// How many frames are waiting for an observer.
    pub fn queued_frames(&self) -> usize {
        self.state
            .lock()
            .expect("task status source lock")
            .order
            .len()
    }
}

#[cfg(test)]
mod task_convergence_tests {
    use super::*;
    use novarocks_execution_contract::task_execution::task_convergence::TaskConvergenceVersion;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    #[test]
    fn actual_stop_is_replayable_independently_of_status() {
        let execution = QueryExecutionId::new(
            QueryId::new(11, 12),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("nonzero query id");
        let identity = TaskIdentity::new(
            execution,
            StageId::new(1).expect("nonzero stage"),
            TaskId::new(2).expect("nonzero task"),
            BackendProcessId::new_v7(),
        );
        let source = TaskStatusSource::new();
        let receipt =
            TaskConvergenceReceipt::actual_stopped(identity, TaskConvergenceVersion::FIRST);
        assert_eq!(source.latest_task_convergence(identity), None);
        assert!(source.publish_task_convergence(receipt));
        assert!(!source.publish_task_convergence(receipt));
        assert_eq!(source.latest_task_convergence(identity), Some(receipt));
    }
}

#[cfg(test)]
mod covered_subscription_tests {
    use super::*;
    use novarocks_execution_contract::task_execution::context_convergence::{
        QueryContextConvergenceState, QueryContextConvergenceVersion,
    };
    use novarocks_execution_contract::task_execution::status::{TaskOutputFacts, TaskState};
    use novarocks_execution_contract::task_execution::task_convergence::TaskConvergenceVersion;
    use novarocks_execution_contract::task_execution::transition::QueryContextState;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    fn identities() -> (QueryContextRef, TaskIdentity, TaskIdentity) {
        let execution = QueryExecutionId::new(
            QueryId::new(71, 72),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("nonzero query id");
        let backend = BackendProcessId::new_v7();
        let context = QueryContextRef::new(execution, FrontendProcessId::new_v7(), backend);
        let a = TaskIdentity::new(
            execution,
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            backend,
        );
        let b = TaskIdentity::new(
            execution,
            StageId::new(1).unwrap(),
            TaskId::new(2).unwrap(),
            backend,
        );
        (context, a, b)
    }

    fn subscribe(
        source: &Arc<TaskStatusSource>,
        context: QueryContextRef,
        required: &[TaskIdentity],
    ) -> CoveredSubscription {
        source
            .begin_covered_subscription(context, 7, &[], &[], None, required, 8)
            .unwrap()
    }

    fn take(subscription: &CoveredSubscription) -> CoveredObservationFrame {
        let selected = subscription.select_next(1024, |_| 1).unwrap().unwrap();
        let frame = selected.frame;
        subscription.note_delivered(selected.delivery_id).unwrap();
        frame
    }

    fn drain_initial(subscription: &CoveredSubscription) -> Vec<CoveredObservationFrame> {
        let mut frames = Vec::new();
        loop {
            let frame = take(subscription);
            let complete = matches!(frame, CoveredObservationFrame::CatchUpComplete { .. });
            frames.push(frame);
            if complete {
                return frames;
            }
        }
    }

    #[test]
    fn covered_a1_atomic_cut_and_bounded_catch_up() {
        let (context, a, b) = identities();
        let source = Arc::new(TaskStatusSource::new());
        source.publish(TaskStatus::created(a));
        source.publish_task_convergence(TaskConvergenceReceipt::actual_stopped(
            a,
            TaskConvergenceVersion::FIRST,
        ));
        source.publish_context_convergence(QueryContextConvergenceReceipt::new(
            context,
            QueryContextConvergenceVersion::FIRST,
            QueryContextConvergenceState::WorkerStoppedAndContextFenced,
        ));
        source.publish_quiesce(QuiesceQueryContextReceipt::new(
            context,
            1,
            vec![a],
            QueryContextState::Quiescing,
        ));
        let subscriber = subscribe(&source, context, &[a, b]);
        let cut = subscriber.bookmark().source_cut;
        assert_eq!(cut, 4);
        // A publication after registration belongs to live catch-up, even
        // while the initial cut has not been handed to the transport.
        source.publish(TaskStatus::created(b));
        assert_eq!(subscriber.bookmark().covered_prefix, 0);
        let initial = drain_initial(&subscriber);
        assert!(initial.iter().any(|frame| matches!(
            frame,
            CoveredObservationFrame::CatchUp(CoveredObservationFact::Unknown(identity)) if *identity == b
        )));
        assert!(initial.iter().any(|frame| matches!(
            frame,
            CoveredObservationFrame::CatchUp(CoveredObservationFact::TaskConvergence(receipt)) if receipt.identity() == a
        )));
        assert!(initial.iter().any(|frame| matches!(
            frame,
            CoveredObservationFrame::CatchUp(CoveredObservationFact::Quiesce(_))
        )));
        assert_eq!(subscriber.bookmark().covered_prefix, cut);
        assert!(
            matches!(take(&subscriber), CoveredObservationFrame::Live { revision: 5, fact: CoveredObservationFact::Status(status) } if status.identity() == b)
        );
        assert_eq!(subscriber.bookmark().covered_prefix, 5);
    }

    #[test]
    fn covered_b2_hot_task_cannot_starve_cold_task() {
        let (context, hot, cold) = identities();
        let source = Arc::new(TaskStatusSource::new());
        let subscriber = subscribe(&source, context, &[hot, cold]);
        drain_initial(&subscriber);
        source.publish(TaskStatus::created(hot));
        source.publish(TaskStatus::created(cold));
        for _ in 0..10 {
            source.publish(TaskStatus::created(hot));
        }
        assert!(
            matches!(take(&subscriber), CoveredObservationFrame::Live { fact: CoveredObservationFact::Status(status), .. } if status.identity() == hot)
        );
        assert!(
            matches!(take(&subscriber), CoveredObservationFrame::Live { fact: CoveredObservationFact::Status(status), .. } if status.identity() == cold)
        );
        assert_eq!(
            subscriber.bookmark().covered_prefix,
            subscriber.bookmark().source_cut
        );
    }

    #[test]
    fn covered_a3_selected_frame_and_successor_remain_uncovered() {
        let (context, a, _) = identities();
        let source = Arc::new(TaskStatusSource::new());
        let subscriber = subscribe(&source, context, &[a]);
        drain_initial(&subscriber);
        source.publish(TaskStatus::created(a));
        let selected = subscriber.select_next(1024, |_| 1).unwrap().unwrap();
        assert_eq!(subscriber.bookmark().covered_prefix, 0);
        source.publish(TaskStatus::created(a));
        assert_eq!(subscriber.bookmark().source_cut, 2);
        assert_eq!(subscriber.bookmark().covered_prefix, 0);
        assert_eq!(
            subscriber.select_next(1024, |_| 1).unwrap().unwrap(),
            selected
        );
        subscriber.note_delivered(selected.delivery_id).unwrap();
        assert_eq!(subscriber.bookmark().covered_prefix, 1);
        assert!(matches!(
            take(&subscriber),
            CoveredObservationFrame::Live { revision: 2, .. }
        ));
        assert_eq!(subscriber.bookmark().covered_prefix, 2);
        assert!(matches!(
            subscriber.note_delivered(selected.delivery_id),
            Err(CoveredSubscriptionError::WrongDelivery)
        ));
    }

    #[test]
    fn covered_future_cursors_and_frame_budget_fail_closed() {
        let (context, a, _) = identities();
        let source = Arc::new(TaskStatusSource::new());
        source.publish(TaskStatus::created(a));
        assert!(matches!(
            source.begin_covered_subscription(
                context,
                1,
                &[TaskStatusCursor::at(a, TaskStatusVersion::new(2).unwrap())],
                &[],
                None,
                &[],
                8,
            ),
            Err(CoveredSubscriptionError::FutureTaskCursor(identity)) if identity == a
        ));
        assert!(matches!(
            source.begin_covered_subscription(
                context,
                1,
                &[],
                &[],
                Some(QueryContextConvergenceCursor::at(
                    context,
                    QueryContextConvergenceVersion::FIRST,
                )),
                &[],
                8,
            ),
            Err(CoveredSubscriptionError::FutureContextCursor)
        ));
        let subscriber = subscribe(&source, context, &[]);
        assert!(matches!(
            subscriber.select_next(1, |_| 2),
            Err(CoveredSubscriptionError::FrameTooLarge { bytes: 2, limit: 1 })
        ));
        assert_eq!(subscriber.bookmark().covered_prefix, 0);
    }

    #[test]
    fn covered_live_control_facts_share_revision_and_selected_size_is_checked() {
        let (context, a, _) = identities();
        let source = Arc::new(TaskStatusSource::new());
        let subscriber = subscribe(&source, context, &[a]);
        drain_initial(&subscriber);
        source.publish_task_convergence(TaskConvergenceReceipt::actual_stopped(
            a,
            TaskConvergenceVersion::FIRST,
        ));
        source.publish_quiesce(QuiesceQueryContextReceipt::new(
            context,
            1,
            vec![a],
            QueryContextState::Quiescing,
        ));
        source.publish_context_convergence(QueryContextConvergenceReceipt::new(
            context,
            QueryContextConvergenceVersion::FIRST,
            QueryContextConvergenceState::WorkerStoppedAndContextFenced,
        ));
        let selected = subscriber.select_next(10, |_| 2).unwrap().unwrap();
        assert!(matches!(
            subscriber.select_next(1, |_| 2),
            Err(CoveredSubscriptionError::FrameTooLarge { bytes: 2, limit: 1 })
        ));
        assert_eq!(subscriber.bookmark().covered_prefix, 0);
        subscriber.note_delivered(selected.delivery_id).unwrap();
        assert!(matches!(
            selected.frame,
            CoveredObservationFrame::Live {
                revision: 1,
                fact: CoveredObservationFact::TaskConvergence(_),
            }
        ));
        assert!(matches!(
            take(&subscriber),
            CoveredObservationFrame::Live {
                revision: 2,
                fact: CoveredObservationFact::Quiesce(_),
            }
        ));
        assert!(matches!(
            take(&subscriber),
            CoveredObservationFrame::Live {
                revision: 3,
                fact: CoveredObservationFact::ContextConvergence(_),
            }
        ));
        assert_eq!(subscriber.bookmark().covered_prefix, 3);
    }

    #[test]
    fn covered_new_target_overflow_does_not_claim_coverage() {
        let (context, a, b) = identities();
        let source = Arc::new(TaskStatusSource::new());
        let subscriber = source
            .begin_covered_subscription(context, 1, &[], &[], None, &[a], 1)
            .unwrap();
        drain_initial(&subscriber);
        source.publish(TaskStatus::created(b));
        assert_eq!(subscriber.bookmark().source_cut, 1);
        assert_eq!(subscriber.bookmark().covered_prefix, 0);
        assert!(matches!(
            subscriber.select_next(1024, |_| 1),
            Err(CoveredSubscriptionError::TargetLimitExceeded)
        ));
    }

    #[test]
    fn covered_bookmarks_advance_during_silence_and_generation_is_nonzero() {
        let (context, a, _) = identities();
        let source = Arc::new(TaskStatusSource::new());
        assert!(matches!(
            source.begin_covered_subscription(context, 0, &[], &[], None, &[a], 1),
            Err(CoveredSubscriptionError::InvalidGeneration)
        ));
        let subscriber = subscribe(&source, context, &[a]);
        drain_initial(&subscriber);
        let first = subscriber.bookmark();
        let second = subscriber.bookmark();
        assert_eq!(first.generation, 7);
        assert_eq!(second.sequence, first.sequence + 1);
        assert_eq!(second.covered_prefix, first.covered_prefix);
        assert_eq!(second.source_cut, first.source_cut);
    }

    #[test]
    fn covered_initial_snapshot_accepts_full_task_bound_with_explicit_byte_cap() {
        let (context, a, _) = identities();
        let source = Arc::new(TaskStatusSource::new());
        let accepted: Vec<_> = (1..=4096)
            .map(|id| {
                TaskIdentity::new(
                    a.query_execution_id(),
                    a.stage_id(),
                    TaskId::new(id).unwrap(),
                    a.backend_process_id(),
                )
            })
            .collect();
        source.publish_quiesce(QuiesceQueryContextReceipt::new(
            context,
            1,
            accepted,
            QueryContextState::Quiescing,
        ));
        let subscriber = source
            .begin_covered_subscription(context, 1, &[], &[], None, &[], 4096)
            .expect("legal maximum task count fits source-side byte cap");
        let state = source.state.lock().expect("task status source lock");
        let initial = &state.covered_subscribers[&subscriber.id].initial;
        assert_eq!(initial.len(), 4097);
        let measured_bytes: usize = initial.iter().map(|fact| format!("{fact:?}").len()).sum();
        assert!(measured_bytes < TaskStatusSource::MAX_INITIAL_SNAPSHOT_BYTES);
    }

    #[tokio::test]
    async fn covered_wait_observes_change_after_registered_cut() {
        let (context, a, _) = identities();
        let source = Arc::new(TaskStatusSource::new());
        let subscriber = subscribe(&source, context, &[a]);
        let cut = subscriber.source_cut();
        source.publish(TaskStatus::created(a));
        assert_eq!(subscriber.wait_for_change(cut).await, cut + 1);
    }

    #[test]
    fn covered_forgotten_status_cursor_returns_gone() {
        let (context, a, _) = identities();
        let source = Arc::new(TaskStatusSource::new());
        source.publish(
            TaskStatus::try_new(
                a,
                TaskStatusVersion::FIRST,
                TaskState::Finished,
                None,
                TaskOutputFacts::new(true),
            )
            .unwrap(),
        );
        source.mark_gone(a);
        source.forget_gone(a);
        let subscriber = source
            .begin_covered_subscription(
                context,
                1,
                &[TaskStatusCursor::at(a, TaskStatusVersion::FIRST)],
                &[],
                None,
                &[],
                1,
            )
            .unwrap();
        assert!(matches!(
            take(&subscriber),
            CoveredObservationFrame::CatchUp(CoveredObservationFact::Gone(identity)) if identity == a
        ));
        assert!(matches!(
            source.begin_covered_subscription(
                context,
                1,
                &[],
                &[TaskConvergenceCursor::at(a, TaskConvergenceVersion::FIRST)],
                None,
                &[],
                1,
            ),
            Err(CoveredSubscriptionError::FutureTaskConvergenceCursor(identity)) if identity == a
        ));
    }

    #[test]
    fn covered_quiesce_cursor_is_exact_and_future_closed() {
        let (context, a, _) = identities();
        let source = Arc::new(TaskStatusSource::new());
        let first = NonZeroU64::new(1).unwrap();
        let cursor = QuiesceObservationCursor {
            context,
            fence_version: Some(first),
        };
        assert!(matches!(
            source.begin_covered_subscription_with_quiesce(
                context,
                1,
                &[],
                &[],
                None,
                Some(cursor),
                &[a],
                1,
            ),
            Err(CoveredSubscriptionError::FutureQuiesceCursor)
        ));
        source.publish_quiesce(QuiesceQueryContextReceipt::new(
            context,
            1,
            vec![a],
            QueryContextState::Quiescing,
        ));
        let subscriber = source
            .begin_covered_subscription_with_quiesce(
                context,
                1,
                &[],
                &[],
                None,
                Some(cursor),
                &[a],
                1,
            )
            .unwrap();
        let initial = drain_initial(&subscriber);
        assert!(initial.iter().any(|frame| matches!(
            frame,
            CoveredObservationFrame::CatchUp(CoveredObservationFact::Quiesce(_))
        )));
        let unobserved = QuiesceObservationCursor {
            context,
            fence_version: None,
        };
        let replay = source
            .begin_covered_subscription_with_quiesce(
                context,
                2,
                &[],
                &[],
                None,
                Some(unobserved),
                &[a],
                1,
            )
            .unwrap();
        assert!(drain_initial(&replay).iter().any(|frame| matches!(
            frame,
            CoveredObservationFrame::CatchUp(CoveredObservationFact::Quiesce(_))
        )));
    }
}
