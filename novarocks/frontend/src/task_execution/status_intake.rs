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

//! Status intake: a bounded queue and one serial runner.
//!
//! A status callback holds a [`StatusIntakeHandle`] and nothing else. It can
//! enqueue one immutable snapshot and wake the runner; it has no path to a
//! task, a stage, or a lease, so it cannot create a task or complete a stage
//! on the transport's thread.
//!
//! Overflow is not a silent drop of a snapshot. It is reported as observation
//! loss, which is exactly the protocol's own answer: resubscribe with the
//! per-task cursors, whose backend tasks are untouched.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use novarocks_execution::task_execution::{TaskIdentity, TaskStatus};
use novarocks_query_application::coordination::{
    AcceptedRootSuccessSealPort, AcceptedRootSuccessSealRequest,
};

/// One immutable observation the transport published.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatusEvent {
    /// A complete status snapshot of one task.
    Published(TaskStatus),
    /// One task's retained record was reaped.
    Gone(TaskIdentity),
}

/// How the runner is woken.
pub trait StatusIntakeWake: std::fmt::Debug + Send + Sync {
    fn wake(&self);
}

/// Wakes a runner parked on a condition variable.
///
/// The coordinator's statement thread is blocking rather than async, so it
/// parks here instead of on a `Notify` it would have to enter a runtime to
/// await. It is a pacing aid and never a correctness condition: every wait is
/// bounded, so a wake that is missed costs latency and nothing else.
#[derive(Debug, Default)]
pub struct CondvarWake {
    woken: Mutex<bool>,
    signal: Condvar,
}

impl CondvarWake {
    /// Blocks until something wakes this, or `timeout` elapses.
    ///
    /// A wake that arrived while the caller was working is consumed here
    /// rather than lost: without the flag, a publish between two waits would
    /// be slept through, and on a control-plane-only turn nothing else would
    /// arrive to end that sleep.
    pub fn wait(&self, timeout: Duration) {
        let mut woken = self.woken.lock().expect("task round wake lock");
        if std::mem::take(&mut *woken) {
            return;
        }
        let (mut guard, _) = self
            .signal
            .wait_timeout(woken, timeout)
            .expect("task round wake condvar");
        *guard = false;
    }
}

impl StatusIntakeWake for CondvarWake {
    fn wake(&self) {
        let mut woken = self.woken.lock().expect("task round wake lock");
        *woken = true;
        drop(woken);
        self.signal.notify_all();
    }
}

/// Wakes a runner parked on a `Notify`.
#[derive(Debug)]
pub struct NotifyWake(Arc<tokio::sync::Notify>);

impl NotifyWake {
    pub fn new(notify: Arc<tokio::sync::Notify>) -> Self {
        Self(notify)
    }
}

impl StatusIntakeWake for NotifyWake {
    fn wake(&self) {
        self.0.notify_one();
    }
}

/// Counts wakes instead of parking anything.
///
/// It exists so a test can assert that a callback woke the runner exactly
/// once per publish without waiting on a task scheduler.
#[derive(Debug, Default)]
pub struct CountingWake(AtomicUsize);

impl CountingWake {
    pub fn count(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
}

impl StatusIntakeWake for CountingWake {
    fn wake(&self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}

#[derive(Debug)]
struct StatusIntakeInner {
    queue: Mutex<StatusIntakeQueue>,
    capacity: usize,
    runner_busy: AtomicBool,
    wake: Arc<dyn StatusIntakeWake>,
}

#[derive(Debug, Default)]
struct StatusIntakeQueue {
    entries: VecDeque<StatusIntakeEntry>,
    status_count: usize,
    observation_loss_queued: bool,
    success_seal_queued: bool,
}

#[derive(Debug)]
pub(super) enum StatusIntakeEntry {
    Status(StatusEvent),
    ObservationLoss,
    SuccessSeal(AcceptedRootSuccessSealRequest),
}

impl StatusIntakeQueue {
    fn enqueue_observation_loss(&mut self) {
        if !self.observation_loss_queued {
            self.entries.push_back(StatusIntakeEntry::ObservationLoss);
            self.observation_loss_queued = true;
        }
    }
}

/// Whether a published snapshot was admitted.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum StatusIntakeAdmission {
    Enqueued,
    /// The bounded queue was full. The runner will be told to resubscribe
    /// rather than to trust a gap it cannot see.
    Overflowed,
}

/// The only handle a transport callback holds.
#[derive(Clone, Debug)]
pub struct StatusIntakeHandle {
    inner: Arc<StatusIntakeInner>,
}

impl StatusIntakeHandle {
    /// Enqueues one immutable snapshot and wakes the runner.
    ///
    /// This is everything a callback may do. It never classifies the
    /// snapshot, advances a cursor, or touches a stage.
    pub fn publish(&self, event: StatusEvent) -> StatusIntakeAdmission {
        let admission = {
            let mut queue = self.inner.queue.lock().expect("status intake queue");
            if queue.status_count >= self.inner.capacity {
                queue.enqueue_observation_loss();
                StatusIntakeAdmission::Overflowed
            } else {
                queue.entries.push_back(StatusIntakeEntry::Status(event));
                queue.status_count += 1;
                StatusIntakeAdmission::Enqueued
            }
        };
        self.inner.wake.wake();
        admission
    }

    /// Reports that the status transport dropped while the backend process is
    /// intact.
    pub fn note_observation_loss(&self) {
        self.inner
            .queue
            .lock()
            .expect("status intake queue")
            .enqueue_observation_loss();
        self.inner.wake.wake();
    }
}

impl AcceptedRootSuccessSealPort for StatusIntakeHandle {
    fn enqueue_success_seal(
        &self,
        request: AcceptedRootSuccessSealRequest,
    ) -> Result<(), AcceptedRootSuccessSealRequest> {
        {
            let mut queue = self.inner.queue.lock().expect("status intake queue");
            if queue.success_seal_queued {
                return Err(request);
            }
            // One result pump owns one move-only request. It is the queue's
            // reserved control slot, so a full status burst cannot lose the
            // success decision or make it wait for new capacity.
            queue
                .entries
                .push_back(StatusIntakeEntry::SuccessSeal(request));
            queue.success_seal_queued = true;
        }
        self.inner.wake.wake();
        Ok(())
    }
}

/// The runner side of the intake.
#[derive(Debug)]
pub struct StatusIntake {
    inner: Arc<StatusIntakeInner>,
}

impl StatusIntake {
    pub fn new(capacity: usize, wake: Arc<dyn StatusIntakeWake>) -> Self {
        Self {
            inner: Arc::new(StatusIntakeInner {
                queue: Mutex::new(StatusIntakeQueue::default()),
                capacity: capacity.max(1),
                runner_busy: AtomicBool::new(false),
                wake,
            }),
        }
    }

    pub fn handle(&self) -> StatusIntakeHandle {
        StatusIntakeHandle {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn queued(&self) -> usize {
        self.inner
            .queue
            .lock()
            .expect("status intake queue")
            .status_count
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// Claims the single serial runner slot.
    ///
    /// A second concurrent claim returns `None`, so intake can never be
    /// applied from two threads at once.
    pub fn try_enter(&self) -> Option<StatusIntakeRunner<'_>> {
        if self
            .inner
            .runner_busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return None;
        }
        Some(StatusIntakeRunner { intake: self })
    }
}

/// The exclusive right to apply intake.
#[derive(Debug)]
pub struct StatusIntakeRunner<'a> {
    intake: &'a StatusIntake,
}

impl StatusIntakeRunner<'_> {
    /// Takes at most `max` queued snapshots and reports any observation-loss
    /// marker ordered before the next success-seal request.
    pub fn drain_statuses(&mut self, max: usize) -> (bool, Vec<StatusEvent>) {
        let mut queue = self.intake.inner.queue.lock().expect("status intake queue");
        let mut statuses = Vec::new();
        let mut observation_loss = false;
        while statuses.len() < max {
            match queue.entries.front() {
                Some(StatusIntakeEntry::Status(_)) => {
                    let Some(StatusIntakeEntry::Status(status)) = queue.entries.pop_front() else {
                        unreachable!("the queue front was a status")
                    };
                    queue.status_count -= 1;
                    statuses.push(status);
                }
                Some(StatusIntakeEntry::ObservationLoss) => {
                    queue.entries.pop_front();
                    queue.observation_loss_queued = false;
                    observation_loss = true;
                }
                Some(StatusIntakeEntry::SuccessSeal(_)) | None => break,
            }
        }
        (observation_loss, statuses)
    }

    /// Drains in publication order and stops at the first success-seal
    /// request. Entries behind that request remain residual work.
    pub(super) fn drain_ordered(&mut self, max: usize) -> Vec<StatusIntakeEntry> {
        let mut queue = self.intake.inner.queue.lock().expect("status intake queue");
        let mut entries = Vec::new();
        while entries.len() < max {
            let Some(entry) = queue.entries.pop_front() else {
                break;
            };
            let is_seal = matches!(entry, StatusIntakeEntry::SuccessSeal(_));
            match &entry {
                StatusIntakeEntry::Status(_) => queue.status_count -= 1,
                StatusIntakeEntry::ObservationLoss => queue.observation_loss_queued = false,
                StatusIntakeEntry::SuccessSeal(_) => queue.success_seal_queued = false,
            }
            entries.push(entry);
            if is_seal {
                break;
            }
        }
        entries
    }
}

impl Drop for StatusIntakeRunner<'_> {
    fn drop(&mut self) {
        self.intake
            .inner
            .runner_busy
            .store(false, Ordering::Release);
    }
}
