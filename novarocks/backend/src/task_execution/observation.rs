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
//! It holds three things and nothing else: the current snapshot of every task
//! it knows, the at-most-one snapshot queued for delivery per task, and the
//! terminal snapshot. There is no history buffer, so a cursor that has fallen
//! behind is answered with the current latest rather than a replay — a
//! frontend that missed versions has missed nothing it can act on, because
//! every snapshot is complete rather than a delta.
//!
//! Delivery is per-task fair by construction: each task owns exactly one
//! queue position while it has anything pending, so a task publishing
//! thousands of metric versions coalesces into its own slot instead of
//! pushing another task's snapshot back.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Mutex;

use novarocks_execution::task_execution::identity::TaskIdentity;
use novarocks_execution::task_execution::status::{TaskStatus, TaskStatusCursor};

/// One immutable observation frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskStatusEvent {
    /// A full immutable snapshot, never a delta over a previous frame.
    Status(TaskStatus),
    /// Retained state for this task was reclaimed.
    Gone(TaskIdentity),
}

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
    latest: BTreeMap<TaskIdentity, TaskStatus>,
    reclaimed: BTreeSet<TaskIdentity>,
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
#[derive(Debug, Default)]
pub struct TaskStatusSource {
    state: Mutex<SourceState>,
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
    }

    /// Records that a task's retained state was reclaimed.
    pub fn mark_gone(&self, identity: TaskIdentity) {
        let mut state = self.state.lock().expect("task status source lock");
        state.latest.remove(&identity);
        state.reclaimed.insert(identity);
        let slot = state.pending.entry(identity).or_default();
        let was_empty = slot.is_empty();
        slot.gone = true;
        if was_empty {
            state.order.push_back(identity);
        }
        state.revision = state.revision.saturating_add(1);
    }

    /// Takes the next frame owed to an observer, round-robin across tasks.
    pub fn next_event(&self) -> Option<TaskStatusEvent> {
        let mut state = self.state.lock().expect("task status source lock");
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
        if state.reclaimed.contains(&identity) {
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
