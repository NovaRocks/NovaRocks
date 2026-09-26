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

//! Status and observation intake: bounded queues and serial runners.
//!
//! A status callback holds a [`StatusIntakeHandle`] and nothing else. It can
//! enqueue one immutable snapshot and wake the runner; it has no path to a
//! task, a stage, or a lease, so it cannot create a task or complete a stage
//! on the transport's thread.
//!
//! The legacy status queue reports overflow as observation loss. The unified
//! observation queue below reserves capacity before reading a frame and
//! applies bounded backpressure instead; ordinary queue fullness cannot
//! counterfeit a gap in the accepted observation stream.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use novarocks_execution::task_execution::{
    QueryContextConvergenceReceipt, QueryContextRef, QuiesceQueryContextReceipt,
    TaskConvergenceReceipt, TaskIdentity, TaskStatus,
};
use novarocks_query_application::coordination::{
    AcceptedRootSuccessSealPort, AcceptedRootSuccessSealRequest, MonotonicInstant,
};

use super::clock::{ProcessMonotonicClock, TaskProtocolClock};

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
        self.note_observation_incomplete();
    }

    /// Reports an observation boundary that prevents a pending success seal
    /// from treating task status as complete. This includes transport loss and
    /// an identity-invalid status frame; the serial runner makes the final
    /// fail-closed classification from its subscription state.
    pub fn note_observation_incomplete(&self) {
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

/// One complete frame of the single Context observation stream. The
/// generation and watermark fields are source claims; only the serial owner
/// may validate and apply them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ObservationFrame {
    Covered {
        context: QueryContextRef,
        generation: u64,
        event: novarocks_task_codec::operation::CoveredStatusStreamEvent,
    },
    Status(TaskStatus),
    TaskConvergence(TaskConvergenceReceipt),
    ContextConvergence(QueryContextConvergenceReceipt),
    Quiesce(QuiesceQueryContextReceipt),
    Gone(TaskIdentity),
    CatchUpComplete {
        context: QueryContextRef,
        generation: u64,
        source_cut: u64,
    },
    Bookmark {
        context: QueryContextRef,
        generation: u64,
        sequence: u64,
        covered_prefix: u64,
        source_cut: u64,
    },
}

/// A frame that cannot fit in one pre-read slot is a protocol/budget error,
/// not an observation gap or a reason to silently truncate the frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OversizedObservationFrame {
    pub charged_bytes: usize,
    pub max_frame_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ObservationIntakeConfigError {
    NoQueueCapacity,
    NoPendingOrControlCapacity,
    NoFrameCapacity,
    NoControlByteCapacity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ObservationSubscriptionLimit;

#[derive(Debug)]
pub(crate) enum ObservationIntakeEntry {
    Frame(ObservationFrame),
    SuccessSeal(AcceptedRootSuccessSealRequest),
    #[cfg(test)]
    TestSeal,
}

#[derive(Debug, Default)]
struct ObservationSubscriptionState {
    reserved: bool,
    pending: bool,
    closed: bool,
}

#[derive(Debug)]
struct RegisteredObservation {
    registered_at: MonotonicInstant,
    entry: ObservationIntakeEntry,
    subscription: Option<u64>,
    pending: bool,
    charged_bytes: usize,
}

#[derive(Debug, Default)]
struct ObservationQueue {
    batch_pending: bool,
    applying_since: Option<MonotonicInstant>,
    applying_count: usize,
    applying_bytes: usize,
    entries: VecDeque<RegisteredObservation>,
    subscriptions: HashMap<u64, ObservationSubscriptionState>,
    next_subscription: u64,
    queued_frames: usize,
    data_count: usize,
    data_bytes: usize,
    success_seal_queued: bool,
}

#[derive(Debug)]
struct ObservationIntakeInner {
    clock: Arc<dyn TaskProtocolClock>,
    queue: Mutex<ObservationQueue>,
    queue_capacity: usize,
    data_capacity: usize,
    subscription_capacity: usize,
    data_bytes_capacity: usize,
    max_frame_bytes: usize,
    capacity_wake: tokio::sync::Notify,
    runner_busy: AtomicBool,
    wake: Arc<dyn StatusIntakeWake>,
}

/// Bounded admission for one Context observation stream and its serial owner.
///
/// The total budget reserves one control entry for a success seal. Each
/// subscription may hold one pre-read reservation or one queue-full pending
/// frame. All reservations and pending frames count against the data budget.
#[derive(Debug)]
pub(crate) struct ObservationIntake {
    inner: Arc<ObservationIntakeInner>,
}

impl ObservationIntake {
    pub(crate) fn for_task_attempt(
        wake: Arc<dyn StatusIntakeWake>,
        clock: Arc<dyn TaskProtocolClock>,
    ) -> Result<Self, ObservationIntakeConfigError> {
        let max_frame =
            novarocks_task_codec::operation::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES + 4096;
        Self::new_with_clock(
            4096,
            4098,
            max_frame * 4 + std::mem::size_of::<AcceptedRootSuccessSealRequest>(),
            max_frame,
            wake,
            clock,
        )
    }

    pub(crate) fn new(
        queue_capacity: usize,
        total_capacity: usize,
        total_bytes: usize,
        max_frame_bytes: usize,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Result<Self, ObservationIntakeConfigError> {
        Self::new_with_clock(
            queue_capacity,
            total_capacity,
            total_bytes,
            max_frame_bytes,
            wake,
            Arc::new(ProcessMonotonicClock::new()),
        )
    }

    pub(crate) fn new_with_clock(
        queue_capacity: usize,
        total_capacity: usize,
        total_bytes: usize,
        max_frame_bytes: usize,
        wake: Arc<dyn StatusIntakeWake>,
        clock: Arc<dyn TaskProtocolClock>,
    ) -> Result<Self, ObservationIntakeConfigError> {
        if queue_capacity == 0 {
            return Err(ObservationIntakeConfigError::NoQueueCapacity);
        }
        if total_capacity < queue_capacity.saturating_add(2) {
            return Err(ObservationIntakeConfigError::NoPendingOrControlCapacity);
        }
        if max_frame_bytes == 0 {
            return Err(ObservationIntakeConfigError::NoFrameCapacity);
        }
        let control_bytes = std::mem::size_of::<AcceptedRootSuccessSealRequest>().max(1);
        if total_bytes < max_frame_bytes.saturating_add(control_bytes) {
            return Err(ObservationIntakeConfigError::NoControlByteCapacity);
        }
        Ok(Self {
            inner: Arc::new(ObservationIntakeInner {
                clock,
                queue: Mutex::new(ObservationQueue::default()),
                queue_capacity,
                data_capacity: total_capacity - 1,
                subscription_capacity: total_capacity,
                data_bytes_capacity: total_bytes - control_bytes,
                max_frame_bytes,
                capacity_wake: tokio::sync::Notify::new(),
                runner_busy: AtomicBool::new(false),
                wake,
            }),
        })
    }

    /// Age of the oldest registered frame the serial owner has not applied.
    /// Pre-read reservations and a pending result seal are not stream facts.
    pub(crate) fn oldest_unapplied_age(&self) -> Option<Duration> {
        let queue = self.inner.queue.lock().expect("observation intake queue");
        let queued_since = queue.entries.iter().find_map(|registered| {
            matches!(registered.entry, ObservationIntakeEntry::Frame(_))
                .then_some(registered.registered_at)
        });
        let oldest = queue.applying_since.into_iter().chain(queued_since).min()?;
        Some(self.inner.clock.now().saturating_duration_since(oldest))
    }

    /// Local application stalls have their own budget. They never authorize
    /// replacing a healthy subscription or inventing a source observation gap.
    pub(crate) fn local_progress_expired(&self) -> bool {
        self.oldest_unapplied_age()
            .is_some_and(|age| age >= Duration::from_secs(30))
    }

    /// Called only after the serial reducer successfully applies its drained
    /// batch. Taking frames out of the queue alone does not apply their facts.
    pub(crate) fn acknowledge_applied(&self) {
        let mut queue = self.inner.queue.lock().expect("observation intake queue");
        queue.data_count -= queue.applying_count;
        queue.data_bytes -= queue.applying_bytes;
        queue.applying_count = 0;
        queue.applying_bytes = 0;
        queue.applying_since = None;
        queue.batch_pending = false;
        drop(queue);
        self.inner.capacity_wake.notify_waiters();
    }

    pub(crate) fn subscribe(&self) -> Result<ObservationPublisher, ObservationSubscriptionLimit> {
        let mut queue = self.inner.queue.lock().expect("observation intake queue");
        if queue.subscriptions.len() >= self.inner.subscription_capacity {
            return Err(ObservationSubscriptionLimit);
        }
        let id = queue.next_subscription;
        queue.next_subscription = queue
            .next_subscription
            .checked_add(1)
            .expect("observation subscription identifiers exhausted");
        queue
            .subscriptions
            .insert(id, ObservationSubscriptionState::default());
        Ok(ObservationPublisher {
            inner: Arc::clone(&self.inner),
            id,
        })
    }

    /// The seal uses its reserved control entry. Its registration shares the
    /// same mutex as frames, so an already accepted pending frame stays first.
    pub(crate) fn enqueue_success_seal(
        &self,
        request: AcceptedRootSuccessSealRequest,
    ) -> Result<(), AcceptedRootSuccessSealRequest> {
        let mut queue = self.inner.queue.lock().expect("observation intake queue");
        if queue.success_seal_queued {
            return Err(request);
        }
        queue.entries.push_back(RegisteredObservation {
            registered_at: self.inner.clock.now(),
            entry: ObservationIntakeEntry::SuccessSeal(request),
            subscription: None,
            pending: false,
            charged_bytes: 0,
        });
        queue.success_seal_queued = true;
        drop(queue);
        self.inner.wake.wake();
        Ok(())
    }

    pub(crate) fn try_enter(&self) -> Option<ObservationIntakeRunner<'_>> {
        self.inner
            .runner_busy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| ObservationIntakeRunner { intake: self })
    }

    #[cfg(test)]
    fn enqueue_test_seal(&self) {
        let mut queue = self.inner.queue.lock().expect("observation intake queue");
        assert!(!queue.success_seal_queued);
        queue.entries.push_back(RegisteredObservation {
            registered_at: self.inner.clock.now(),
            entry: ObservationIntakeEntry::TestSeal,
            subscription: None,
            pending: false,
            charged_bytes: 0,
        });
        queue.success_seal_queued = true;
        drop(queue);
        self.inner.wake.wake();
    }
}

impl AcceptedRootSuccessSealPort for ObservationIntake {
    fn enqueue_success_seal(
        &self,
        request: AcceptedRootSuccessSealRequest,
    ) -> Result<(), AcceptedRootSuccessSealRequest> {
        ObservationIntake::enqueue_success_seal(self, request)
    }
}

/// A subscription must obtain this permit before reading the next stream
/// frame. The permit reserves a bounded slot, so publication never waits
/// after the transport has validated and accepted the frame.
#[derive(Debug)]
pub(crate) struct ObservationPublisher {
    inner: Arc<ObservationIntakeInner>,
    id: u64,
}

impl ObservationPublisher {
    pub(crate) async fn reserve_read(&self) -> ObservationReadPermit {
        loop {
            // Register the waiter before inspecting capacity, avoiding a
            // lost wake between checking the budget and parking.
            let notified = self.inner.capacity_wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut queue = self.inner.queue.lock().expect("observation intake queue");
                let subscription = queue
                    .subscriptions
                    .get(&self.id)
                    .expect("registered observation subscription");
                if !subscription.reserved
                    && !subscription.pending
                    && queue.data_count < self.inner.data_capacity
                    && queue.data_bytes
                        <= self
                            .inner
                            .data_bytes_capacity
                            .saturating_sub(self.inner.max_frame_bytes)
                {
                    queue.subscriptions.get_mut(&self.id).unwrap().reserved = true;
                    queue.data_count += 1;
                    queue.data_bytes += self.inner.max_frame_bytes;
                    return ObservationReadPermit {
                        inner: Arc::clone(&self.inner),
                        subscription: self.id,
                        active: true,
                    };
                }
            }
            notified.await;
        }
    }
}

impl Drop for ObservationPublisher {
    fn drop(&mut self) {
        let mut queue = self.inner.queue.lock().expect("observation intake queue");
        let subscription = queue
            .subscriptions
            .get_mut(&self.id)
            .expect("registered observation subscription");
        if subscription.reserved || subscription.pending {
            subscription.closed = true;
        } else {
            queue.subscriptions.remove(&self.id);
        }
    }
}

#[derive(Debug)]
pub(crate) struct ObservationReadPermit {
    inner: Arc<ObservationIntakeInner>,
    subscription: u64,
    active: bool,
}

impl ObservationReadPermit {
    pub(crate) fn publish(
        mut self,
        frame: ObservationFrame,
        encoded_bytes: usize,
    ) -> Result<(), OversizedObservationFrame> {
        // Include the locally owned variant and the larger of the encoded
        // frame or its dynamic decoded allocations. Neither an accidentally
        // short wire-size report nor a large Quiesce membership can evade the
        // intake's byte budget.
        let dynamic_bytes = match &frame {
            ObservationFrame::Covered { event, .. } => match &event.fact {
                novarocks_task_codec::operation::CoveredStatusStreamFact::Status(status) => {
                    match status.termination() {
                        Some(novarocks_execution::task_execution::TerminationDetail::Failed(
                            failure,
                        )) => failure.detail().as_str().len(),
                        _ => 0,
                    }
                }
                novarocks_task_codec::operation::CoveredStatusStreamFact::Quiesce(receipt) => {
                    receipt
                        .accepted_tasks()
                        .len()
                        .saturating_mul(std::mem::size_of::<TaskIdentity>())
                }
                _ => 0,
            },
            ObservationFrame::Status(status) => match status.termination() {
                Some(novarocks_execution::task_execution::TerminationDetail::Failed(failure)) => {
                    failure.detail().as_str().len()
                }
                _ => 0,
            },
            ObservationFrame::Quiesce(receipt) => receipt
                .accepted_tasks()
                .len()
                .saturating_mul(std::mem::size_of::<TaskIdentity>()),
            _ => 0,
        };
        let charged_bytes = std::mem::size_of::<ObservationFrame>()
            .checked_add(encoded_bytes.max(dynamic_bytes))
            .unwrap_or(usize::MAX);
        if charged_bytes > self.inner.max_frame_bytes {
            return Err(OversizedObservationFrame {
                charged_bytes,
                max_frame_bytes: self.inner.max_frame_bytes,
            });
        }
        let mut queue = self.inner.queue.lock().expect("observation intake queue");
        let pending = queue.queued_frames >= self.inner.queue_capacity;
        let subscription = queue
            .subscriptions
            .get_mut(&self.subscription)
            .expect("registered observation subscription");
        assert!(subscription.reserved && !subscription.pending);
        subscription.reserved = false;
        subscription.pending = pending;
        if !pending && subscription.closed {
            queue.subscriptions.remove(&self.subscription);
        }
        if !pending {
            queue.queued_frames += 1;
        }
        queue.data_bytes -= self.inner.max_frame_bytes - charged_bytes;
        queue.entries.push_back(RegisteredObservation {
            registered_at: self.inner.clock.now(),
            entry: ObservationIntakeEntry::Frame(frame),
            subscription: Some(self.subscription),
            pending,
            charged_bytes,
        });
        self.active = false;
        drop(queue);
        self.inner.capacity_wake.notify_waiters();
        self.inner.wake.wake();
        Ok(())
    }
}

impl Drop for ObservationReadPermit {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut queue = self.inner.queue.lock().expect("observation intake queue");
        queue
            .subscriptions
            .get_mut(&self.subscription)
            .expect("registered observation subscription")
            .reserved = false;
        if queue.subscriptions[&self.subscription].closed {
            queue.subscriptions.remove(&self.subscription);
        }
        queue.data_count -= 1;
        queue.data_bytes -= self.inner.max_frame_bytes;
        drop(queue);
        self.inner.capacity_wake.notify_waiters();
    }
}

#[derive(Debug)]
pub(crate) struct ObservationIntakeRunner<'a> {
    intake: &'a ObservationIntake,
}

impl ObservationIntakeRunner<'_> {
    /// Drain in one registration order; stop at a seal so the owner decides
    /// it against all earlier accepted observation facts.
    pub(crate) fn drain_ordered(&mut self, max: usize) -> Vec<ObservationIntakeEntry> {
        let mut queue = self
            .intake
            .inner
            .queue
            .lock()
            .expect("observation intake queue");
        // A dequeue batch remains unapplied until the serial owner explicitly
        // acknowledges it. Do not let a later batch overwrite that obligation.
        if queue.batch_pending {
            return Vec::new();
        }
        let mut drained = Vec::new();
        while drained.len() < max {
            let Some(registered) = queue.entries.pop_front() else {
                break;
            };
            let seal = match &registered.entry {
                ObservationIntakeEntry::SuccessSeal(_) => true,
                ObservationIntakeEntry::Frame(_) => false,
                #[cfg(test)]
                ObservationIntakeEntry::TestSeal => true,
            };
            if let Some(subscription) = registered.subscription {
                queue.applying_since = Some(
                    queue
                        .applying_since
                        .map_or(registered.registered_at, |oldest| {
                            oldest.min(registered.registered_at)
                        }),
                );
                queue.applying_count += 1;
                queue.applying_bytes += registered.charged_bytes;
                if registered.pending {
                    let state = queue.subscriptions.get_mut(&subscription).unwrap();
                    state.pending = false;
                    if state.closed {
                        queue.subscriptions.remove(&subscription);
                    }
                } else {
                    queue.queued_frames -= 1;
                }
            } else {
                queue.success_seal_queued = false;
            }
            drained.push(registered.entry);
            if seal {
                break;
            }
        }
        queue.batch_pending = !drained.is_empty();
        drop(queue);
        if !drained.is_empty() {
            self.intake.inner.capacity_wake.notify_waiters();
        }
        drained
    }
}

impl Drop for ObservationIntakeRunner<'_> {
    fn drop(&mut self) {
        self.intake
            .inner
            .runner_busy
            .store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod observation_tests {
    use super::*;
    use novarocks_execution::task_execution::{
        SafeDetail, TaskFailure, TaskFailureCategory, TaskOutputFacts, TaskState,
        TaskStatusVersion, TerminationDetail,
    };
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    fn identity() -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(1, 1), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        )
    }

    fn intake_with_manual_clock() -> (ObservationIntake, Arc<super::super::clock::ManualClock>) {
        let clock = Arc::new(super::super::clock::ManualClock::new());
        let intake = ObservationIntake::new_with_clock(
            1,
            4,
            4096,
            1024,
            Arc::new(CountingWake::default()),
            clock.clone(),
        )
        .unwrap();
        (intake, clock)
    }

    #[tokio::test]
    async fn unapplied_frames_keep_their_age_through_drain_until_application() {
        let (intake, clock) = intake_with_manual_clock();
        let publisher = intake.subscribe().unwrap();
        publisher
            .reserve_read()
            .await
            .publish(ObservationFrame::Gone(identity()), 64)
            .unwrap();
        clock.advance(Duration::from_secs(29));
        assert!(!intake.local_progress_expired());
        let mut runner = intake.try_enter().unwrap();
        assert_eq!(runner.drain_ordered(1).len(), 1);
        drop(runner);
        clock.advance(Duration::from_secs(1));
        assert!(
            intake.local_progress_expired(),
            "dequeue is not application"
        );
        intake.acknowledge_applied();
        assert_eq!(intake.oldest_unapplied_age(), None);
        assert!(!intake.local_progress_expired());
    }

    #[tokio::test]
    async fn newer_pending_frames_cannot_reset_the_oldest_application_budget() {
        let (intake, clock) = intake_with_manual_clock();
        let publisher = intake.subscribe().unwrap();
        publisher
            .reserve_read()
            .await
            .publish(ObservationFrame::Gone(identity()), 64)
            .unwrap();
        clock.advance(Duration::from_secs(20));
        publisher
            .reserve_read()
            .await
            .publish(ObservationFrame::Gone(identity()), 64)
            .unwrap();
        intake.enqueue_test_seal();
        clock.advance(Duration::from_secs(10));
        assert!(intake.local_progress_expired());
        let mut runner = intake.try_enter().unwrap();
        assert_eq!(runner.drain_ordered(1).len(), 1);
        intake.acknowledge_applied();
        assert_eq!(intake.oldest_unapplied_age(), Some(Duration::from_secs(10)));
        clock.advance(Duration::from_secs(20));
        assert!(
            intake.local_progress_expired(),
            "pending frame has its original age"
        );
        assert_eq!(runner.drain_ordered(2).len(), 2);
        intake.acknowledge_applied();
        assert_eq!(intake.oldest_unapplied_age(), None);
    }

    #[tokio::test]
    async fn an_unapplied_batch_blocks_later_drain_until_its_exact_ack() {
        let (intake, clock) = intake_with_manual_clock();
        let publisher = intake.subscribe().unwrap();
        publisher
            .reserve_read()
            .await
            .publish(ObservationFrame::Gone(identity()), 64)
            .unwrap();
        clock.advance(Duration::from_secs(10));
        publisher
            .reserve_read()
            .await
            .publish(ObservationFrame::Gone(identity()), 64)
            .unwrap();
        let mut runner = intake.try_enter().unwrap();
        assert_eq!(runner.drain_ordered(1).len(), 1);
        assert!(runner.drain_ordered(1).is_empty());
        assert_eq!(intake.oldest_unapplied_age(), Some(Duration::from_secs(10)));
        intake.acknowledge_applied();
        assert_eq!(intake.oldest_unapplied_age(), Some(Duration::ZERO));
        assert_eq!(runner.drain_ordered(1).len(), 1);
        clock.advance(Duration::from_secs(30));
        assert!(intake.local_progress_expired());
        intake.acknowledge_applied();
        assert_eq!(intake.oldest_unapplied_age(), None);
    }

    #[tokio::test]
    async fn pre_read_reservations_and_success_seals_do_not_age_as_stream_facts() {
        let (intake, clock) = intake_with_manual_clock();
        let publisher = intake.subscribe().unwrap();
        let permit = publisher.reserve_read().await;
        intake.enqueue_test_seal();
        clock.advance(Duration::from_secs(60));
        assert_eq!(intake.oldest_unapplied_age(), None);
        assert!(!intake.local_progress_expired());
        permit
            .publish(ObservationFrame::Gone(identity()), 64)
            .unwrap();
        assert_eq!(intake.oldest_unapplied_age(), Some(Duration::ZERO));
        clock.advance(Duration::from_secs(30));
        assert!(intake.local_progress_expired());
    }

    #[tokio::test]
    async fn drained_frames_retain_count_and_bytes_until_the_reducer_applies_them() {
        let control_bytes = std::mem::size_of::<AcceptedRootSuccessSealRequest>().max(1);
        for (total_count, total_bytes, charged_bytes) in [
            (3, 4096, 64),
            (
                10,
                2048 + control_bytes,
                1024 - std::mem::size_of::<ObservationFrame>(),
            ),
        ] {
            let intake = ObservationIntake::new(
                1,
                total_count,
                total_bytes,
                1024,
                Arc::new(CountingWake::default()),
            )
            .unwrap();
            let publisher = intake.subscribe().unwrap();
            for _ in 0..2 {
                publisher
                    .reserve_read()
                    .await
                    .publish(ObservationFrame::Gone(identity()), charged_bytes)
                    .unwrap();
            }
            {
                let mut runner = intake.try_enter().unwrap();
                assert_eq!(runner.drain_ordered(1).len(), 1);
            }
            let independent_publisher = intake.subscribe().unwrap();
            let next_read = independent_publisher.reserve_read();
            tokio::pin!(next_read);
            assert!(
                tokio::time::timeout(Duration::from_millis(10), &mut next_read)
                    .await
                    .is_err(),
                "taking a frame into the reducer cannot return its still-retained budget: count={total_count}, bytes={total_bytes}"
            );
            intake.acknowledge_applied();
            let permit = tokio::time::timeout(Duration::from_secs(1), &mut next_read)
                .await
                .expect("applied facts return their exact retained budget");
            drop(permit);
        }
    }

    #[tokio::test]
    async fn pending_failure_precedes_a_later_success_seal() {
        let intake = ObservationIntake::new(1, 3, 4096, 1024, Arc::new(CountingWake::default()))
            .expect("valid budget");
        let publisher = intake.subscribe().unwrap();
        let task = identity();
        publisher
            .reserve_read()
            .await
            .publish(ObservationFrame::Status(TaskStatus::created(task)), 64)
            .expect("first frame");
        let failed = TaskStatus::try_new(
            task,
            TaskStatusVersion::new(2).unwrap(),
            TaskState::Failed,
            Some(TerminationDetail::Failed(TaskFailure::new(
                TaskFailureCategory::Execution,
                SafeDetail::truncating("pending failure"),
            ))),
            TaskOutputFacts::default(),
        )
        .unwrap();
        publisher
            .reserve_read()
            .await
            .publish(ObservationFrame::Status(failed), 64)
            .expect("queue-full frame is retained in the pending slot");
        intake.enqueue_test_seal();
        let mut runner = intake.try_enter().expect("one serial owner");
        let entries = runner.drain_ordered(3);
        assert!(matches!(
            entries[0],
            ObservationIntakeEntry::Frame(ObservationFrame::Status(_))
        ));
        assert!(matches!(
            entries[1],
            ObservationIntakeEntry::Frame(ObservationFrame::Status(ref status))
                if status.state() == TaskState::Failed
        ));
        assert!(matches!(entries[2], ObservationIntakeEntry::TestSeal));
    }

    #[tokio::test]
    async fn full_budget_waits_for_capacity_without_reporting_observation_loss() {
        let intake = ObservationIntake::new(1, 3, 4096, 1024, Arc::new(CountingWake::default()))
            .expect("valid budget");
        let first = intake.subscribe().unwrap();
        let second = intake.subscribe().unwrap();
        let third = intake.subscribe().unwrap();
        first
            .reserve_read()
            .await
            .publish(ObservationFrame::Gone(identity()), 64)
            .unwrap();
        second
            .reserve_read()
            .await
            .publish(ObservationFrame::Gone(identity()), 64)
            .unwrap();
        let wait = third.reserve_read();
        tokio::pin!(wait);
        assert!(matches!(
            futures::poll!(&mut wait),
            std::task::Poll::Pending
        ));
        let mut runner = intake.try_enter().unwrap();
        assert_eq!(runner.drain_ordered(1).len(), 1);
        intake.acknowledge_applied();
        drop(runner);
        wait.await
            .publish(ObservationFrame::Gone(identity()), 64)
            .unwrap();
        let mut runner = intake.try_enter().unwrap();
        assert_eq!(runner.drain_ordered(2).len(), 2);
    }

    #[tokio::test]
    async fn oversized_frame_is_rejected_and_its_read_reservation_is_released() {
        let intake = ObservationIntake::new(1, 3, 4096, 1024, Arc::new(CountingWake::default()))
            .expect("valid budget");
        let publisher = intake.subscribe().unwrap();
        let error = publisher
            .reserve_read()
            .await
            .publish(ObservationFrame::Gone(identity()), 1025)
            .expect_err("the frame exceeds its reserved maximum");
        assert_eq!(error.max_frame_bytes, 1024);
        publisher
            .reserve_read()
            .await
            .publish(ObservationFrame::Gone(identity()), 64)
            .expect("the rejected frame released its slot");
        let mut runner = intake.try_enter().unwrap();
        assert_eq!(runner.drain_ordered(1).len(), 1);
    }
}
