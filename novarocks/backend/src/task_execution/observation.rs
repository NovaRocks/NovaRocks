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
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Mutex;

use novarocks_execution_contract::task_execution::context_convergence::{
    QueryContextConvergenceCursor, QueryContextConvergenceReceipt,
};
use novarocks_execution_contract::task_execution::identity::{QueryContextRef, TaskIdentity};
use novarocks_execution_contract::task_execution::status::{
    TaskStatus, TaskStatusCursor, TaskStatusVersion,
};

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
pub(crate) struct TaskStatusSubscriptionPosition {
    context_cursor: Option<QueryContextConvergenceCursor>,
    task_versions: BTreeMap<TaskIdentity, Option<TaskStatusVersion>>,
    gone: BTreeSet<TaskIdentity>,
    task_change_revision: u64,
    gone_after_terminal: Option<TaskIdentity>,
}

impl TaskStatusSubscriptionPosition {
    pub(crate) fn new(
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
    pub(crate) fn note_delivered(&mut self, event: &TaskStatusEvent) {
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
    pub fn new() -> Self {
        Self::default()
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
        state.latest.insert(identity, status.clone());
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
        state.stats.published = state.stats.published.saturating_add(1);
        drop(state);
        self.wake.notify_waiters();
        true
    }

    /// Records that a task's retained state was reclaimed.
    pub fn mark_gone(&self, identity: TaskIdentity) {
        let mut state = self.state.lock().expect("task status source lock");
        let Some(terminal) = state.latest.remove(&identity) else {
            assert!(
                state.reclaimed.contains_key(&identity) || state.forgotten.contains(&identity),
                "a reclaimed task must retain its final observation"
            );
            return;
        };
        assert!(
            terminal.is_terminal(),
            "only a terminal task observation can become a gone fence"
        );
        state.reclaimed.insert(identity, terminal);
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
                state.forgotten.contains(&identity),
                "only a reclaimed observation can expire"
            );
            return;
        }
        state.forgotten.insert(identity);
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
    pub(crate) async fn next_subscription_event_owned(
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
    pub(crate) fn next_subscription_event_at(
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
