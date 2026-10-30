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

//! Process-local serialization for activity against one materialized view.
//!
//! The gate deliberately does not model scheduler or maintenance capacity.
//! Callers first enqueue a ticket and acquire their own capacity only after a
//! ticket obtains its lease, so waiting does not consume either worker's
//! independent concurrency budget.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Duration;
use tokio::sync::Notify;

use novarocks_query_application::cancellation::{
    QueryCancellationReason, QueryCancellationSource, QueryCancellationView,
};

pub use crate::product::MvTarget as CanonicalMvTarget;

/// The application path currently holding, or waiting to hold, an MV gate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvActivityOwner {
    Create,
    Alter,
    Drop,
    ManualRefresh,
    ScheduledRefresh,
    Repartition,
    AutomaticMaintenance,
}

impl MvActivityOwner {
    fn is_worker_owned(self) -> bool {
        matches!(self, Self::ScheduledRefresh | Self::AutomaticMaintenance)
    }
}

/// Admission can no longer be granted because process shutdown has started.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvActivityGateError {
    Stopping,
}

/// Terminal result of a foreground transition's product-owned FIFO admission.
/// The caller remains responsible only for mapping its own cancellation/error
/// vocabulary and executing the provider/native effect after the lease exists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvActivityAdmissionError {
    Stopping,
    Cancelled,
}

/// A diagnostic snapshot of one target's place in the gate. It is never an
/// admission decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MvActivityObservation {
    pub stopping: bool,
    pub active: bool,
    pub waiting: usize,
}

/// A process-local FIFO gate shared by DDL, foreground refresh, the scheduler,
/// and automatic maintenance.
#[derive(Clone, Default)]
pub struct MvActivityGate {
    inner: Arc<GateInner>,
}

struct GateInner {
    state: Mutex<GateState>,
    // Temporary legacy wait support. P06 removes this with the sync entrypoints.
    changed: Condvar,
    changed_async: Notify,
}

impl Default for GateInner {
    fn default() -> Self {
        Self {
            state: Mutex::new(GateState::default()),
            changed: Condvar::new(),
            changed_async: Notify::new(),
        }
    }
}

impl GateInner {
    // Every release/cancel/stop publishes the same level to both temporary
    // caller forms. Foreign async wakes must run after the state lock exits.
    fn notify_waiters(&self) {
        self.changed.notify_all();
        self.changed_async.notify_waiters();
    }
}

impl MvActivityGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one attempt without taking an execution permit.
    pub fn request(
        &self,
        target: CanonicalMvTarget,
        owner: MvActivityOwner,
    ) -> Result<MvActivityTicket, MvActivityGateError> {
        let mut state = lock(&self.inner.state);
        if state.stopping {
            return Err(MvActivityGateError::Stopping);
        }
        let ticket_id = state.next_ticket_id;
        state.next_ticket_id = state
            .next_ticket_id
            .checked_add(1)
            .unwrap_or_else(|| panic!("MV activity ticket ID overflow"));
        state
            .entries
            .entry(target.clone())
            .or_default()
            .waiters
            .push_back(Waiter { ticket_id, owner });
        self.inner.changed.notify_all();
        Ok(MvActivityTicket {
            inner: Arc::downgrade(&self.inner),
            target,
            ticket_id,
            claimed: false,
        })
    }

    /// What this gate can currently say about one target, for diagnosis only.
    ///
    /// It is a snapshot of a concurrent structure, so it may be stale the
    /// moment it returns. Nothing may admit, refuse, or settle work from it;
    /// `request` and the lease transitions remain the only decisions.
    pub fn observe(&self, target: &CanonicalMvTarget) -> MvActivityObservation {
        let state = lock(&self.inner.state);
        let entry = state.entries.get(target);
        MvActivityObservation {
            stopping: state.stopping,
            active: entry.is_some_and(|entry| entry.active.is_some()),
            waiting: entry.map_or(0, |entry| entry.waiters.len()),
        }
    }

    /// Stops new admission and asks only worker-owned attempts to cancel.
    /// Foreground work retains its statement-owned cancellation lifecycle.
    pub fn begin_stopping(&self) {
        let cancellations = {
            let mut state = lock(&self.inner.state);
            state.stopping = true;
            state
                .entries
                .values()
                .filter_map(|entry| entry.active.as_ref()?.cancellation.clone())
                .collect::<Vec<_>>()
        };
        // Stop and wake callbacks are foreign work. Never invoke them under
        // the FIFO mutex, including during shutdown.
        // Publish the gate's terminal level before invoking independent
        // worker cancellation callbacks. Their failure cannot withhold the
        // close notification from already queued foreground callers.
        self.inner.notify_waiters();
        for cancellation in cancellations {
            let _ = cancellation.request(QueryCancellationReason::ServerShutdown);
        }
    }

    /// Perform the complete foreground transition into an activity lease.
    /// Hosts cannot retain a ticket or independently implement a wait loop;
    /// they receive exactly one lease or an explicit terminal admission fact.
    pub fn acquire_foreground(
        &self,
        target: CanonicalMvTarget,
        owner: MvActivityOwner,
        cancelled: impl Fn() -> bool,
    ) -> Result<MvActivityLease, MvActivityAdmissionError> {
        let mut ticket = self
            .request(target, owner)
            .map_err(|_| MvActivityAdmissionError::Stopping)?;
        match ticket.acquire_waiting(cancelled) {
            Ok(Some(lease)) => Ok(lease),
            Ok(None) => Err(MvActivityAdmissionError::Cancelled),
            Err(_) => Err(MvActivityAdmissionError::Stopping),
        }
    }

    /// Async migration entry on the same FIFO. The legacy sync entry above
    /// remains only until its actual callers migrate and P06 removes it.
    pub async fn acquire_foreground_async(
        &self,
        target: CanonicalMvTarget,
        owner: MvActivityOwner,
        cancelled: impl Future<Output = ()>,
    ) -> Result<MvActivityLease, MvActivityAdmissionError> {
        let mut ticket = self
            .request(target, owner)
            .map_err(|_| MvActivityAdmissionError::Stopping)?;
        match ticket.acquire_waiting_async(cancelled).await {
            Ok(Some(lease)) => Ok(lease),
            Ok(None) => Err(MvActivityAdmissionError::Cancelled),
            Err(_) => Err(MvActivityAdmissionError::Stopping),
        }
    }

    #[cfg(test)]
    fn tracked_target_count(&self) -> usize {
        lock(&self.inner.state).entries.len()
    }
}

/// A queued request. Dropping an unclaimed ticket removes it from the FIFO
/// queue, preventing cancelled pre-dispatch work from stranding later work.
pub struct MvActivityTicket {
    inner: Weak<GateInner>,
    target: CanonicalMvTarget,
    ticket_id: u64,
    claimed: bool,
}

impl MvActivityTicket {
    /// Acquires only when this ticket is the head of its target's FIFO queue.
    pub fn try_acquire(&mut self) -> Result<Option<MvActivityLease>, MvActivityGateError> {
        if self.claimed {
            return Ok(None);
        }
        let Some(inner) = self.inner.upgrade() else {
            return Err(MvActivityGateError::Stopping);
        };
        let mut state = lock(&inner.state);
        if state.stopping {
            remove_waiter(&mut state, &self.target, self.ticket_id);
            drop(state);
            inner.notify_waiters();
            return Err(MvActivityGateError::Stopping);
        }
        let Some(entry) = state.entries.get_mut(&self.target) else {
            return Ok(None);
        };
        if entry.active.is_some()
            || entry
                .waiters
                .front()
                .is_none_or(|waiter| waiter.ticket_id != self.ticket_id)
        {
            return Ok(None);
        }
        let waiter = entry
            .waiters
            .pop_front()
            .expect("front waiter exists after FIFO check");
        debug_assert_eq!(waiter.ticket_id, self.ticket_id);
        let cancellation = waiter
            .owner
            .is_worker_owned()
            .then(QueryCancellationSource::new);
        entry.active = Some(ActiveAttempt {
            ticket_id: self.ticket_id,
            cancellation: cancellation.clone(),
        });
        self.claimed = true;
        inner.changed.notify_all();
        Ok(Some(MvActivityLease {
            inner: Arc::downgrade(&inner),
            target: self.target.clone(),
            ticket_id: self.ticket_id,
            cancellation: cancellation.map(|source| source.view()),
        }))
    }

    /// Wait for the FIFO head without spin sleeping.  The cancellation probe
    /// is intentionally owned by the statement/work-scope adapter; the gate
    /// only owns queue ordering and wakes immediately for every state change.
    pub fn acquire_waiting(
        &mut self,
        cancelled: impl Fn() -> bool,
    ) -> Result<Option<MvActivityLease>, MvActivityGateError> {
        if self.claimed {
            return Ok(None);
        }
        let Some(inner) = self.inner.upgrade() else {
            return Err(MvActivityGateError::Stopping);
        };
        let mut state = lock(&inner.state);
        loop {
            if cancelled() {
                remove_waiter(&mut state, &self.target, self.ticket_id);
                drop(state);
                inner.notify_waiters();
                return Ok(None);
            }
            if state.stopping {
                remove_waiter(&mut state, &self.target, self.ticket_id);
                drop(state);
                inner.notify_waiters();
                return Err(MvActivityGateError::Stopping);
            }
            let Some(entry) = state.entries.get_mut(&self.target) else {
                return Ok(None);
            };
            if entry.active.is_none()
                && entry
                    .waiters
                    .front()
                    .is_some_and(|waiter| waiter.ticket_id == self.ticket_id)
            {
                let waiter = entry
                    .waiters
                    .pop_front()
                    .expect("front waiter exists after FIFO check");
                let cancellation = waiter
                    .owner
                    .is_worker_owned()
                    .then(QueryCancellationSource::new);
                entry.active = Some(ActiveAttempt {
                    ticket_id: self.ticket_id,
                    cancellation: cancellation.clone(),
                });
                self.claimed = true;
                inner.changed.notify_all();
                return Ok(Some(MvActivityLease {
                    inner: Arc::downgrade(&inner),
                    target: self.target.clone(),
                    ticket_id: self.ticket_id,
                    cancellation: cancellation.map(|source| source.view()),
                }));
            }
            let (next, _) = inner
                .changed
                .wait_timeout(state, Duration::from_millis(50))
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next;
        }
    }

    /// Await the FIFO head and the original caller's cancellation/deadline.
    /// Cancellation is a wakeable future, not a probe requiring a poll timer.
    /// Dropping this borrow leaves the same ticket queued until its owner
    /// retries or drops it; it never creates another waiter or grants a lease.
    pub async fn acquire_waiting_async(
        &mut self,
        cancelled: impl Future<Output = ()>,
    ) -> Result<Option<MvActivityLease>, MvActivityGateError> {
        if self.claimed {
            return Ok(None);
        }
        let Some(inner) = self.inner.upgrade() else {
            return Err(MvActivityGateError::Stopping);
        };
        tokio::pin!(cancelled);
        tokio::select! {
            biased;
            () = &mut cancelled => {
                {
                    let mut state = lock(&inner.state);
                    remove_waiter(&mut state, &self.target, self.ticket_id);
                }
                inner.notify_waiters();
                Ok(None)
            }
            result = async {
                loop {
                    let changed = inner.changed_async.notified();
                    tokio::pin!(changed);
                    // Register before checking the FIFO level. A release
                    // between the check and await cannot strand this ticket.
                    changed.as_mut().enable();
                    if let Some(lease) = self.try_acquire()? {
                        return Ok(Some(lease));
                    }
                    changed.await;
                }
            } => result,
        }
    }
}

impl Drop for MvActivityTicket {
    fn drop(&mut self) {
        if self.claimed {
            return;
        }
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        remove_waiter(&mut lock(&inner.state), &self.target, self.ticket_id);
        inner.notify_waiters();
    }
}

/// Exclusive ownership of one MV activity slot. Dropping it releases the slot.
pub struct MvActivityLease {
    inner: Weak<GateInner>,
    target: CanonicalMvTarget,
    ticket_id: u64,
    cancellation: Option<QueryCancellationView>,
}

impl MvActivityLease {
    pub fn cancellation(&self) -> Option<QueryCancellationView> {
        self.cancellation.clone()
    }
}

impl Drop for MvActivityLease {
    fn drop(&mut self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut state = lock(&inner.state);
        let mut remove_entry = false;
        if let Some(entry) = state.entries.get_mut(&self.target) {
            if entry
                .active
                .as_ref()
                .is_some_and(|active| active.ticket_id == self.ticket_id)
            {
                entry.active = None;
            }
            remove_entry = entry.active.is_none() && entry.waiters.is_empty();
        }
        if remove_entry {
            state.entries.remove(&self.target);
        }
        drop(state);
        inner.notify_waiters();
    }
}

#[derive(Default)]
struct GateState {
    stopping: bool,
    next_ticket_id: u64,
    entries: BTreeMap<CanonicalMvTarget, TargetEntry>,
}

#[derive(Default)]
struct TargetEntry {
    waiters: VecDeque<Waiter>,
    active: Option<ActiveAttempt>,
}

struct Waiter {
    ticket_id: u64,
    owner: MvActivityOwner,
}

struct ActiveAttempt {
    ticket_id: u64,
    cancellation: Option<QueryCancellationSource>,
}

fn remove_waiter(state: &mut GateState, target: &CanonicalMvTarget, ticket_id: u64) {
    let mut remove_entry = false;
    if let Some(entry) = state.entries.get_mut(target) {
        if let Some(position) = entry
            .waiters
            .iter()
            .position(|waiter| waiter.ticket_id == ticket_id)
        {
            entry.waiters.remove(position);
        }
        remove_entry = entry.active.is_none() && entry.waiters.is_empty();
    }
    if remove_entry {
        state.entries.remove(target);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(name: &str) -> CanonicalMvTarget {
        CanonicalMvTarget::from_parts(Some("iceberg"), "db", name)
    }

    #[test]
    fn shared_target_is_fifo_across_foreground_and_worker_owners() {
        let gate = MvActivityGate::new();
        let mut manual = gate
            .request(target("mv"), MvActivityOwner::ManualRefresh)
            .unwrap();
        let mut worker = gate
            .request(target("mv"), MvActivityOwner::AutomaticMaintenance)
            .unwrap();
        let lease = manual.try_acquire().unwrap().unwrap();
        assert!(worker.try_acquire().unwrap().is_none());
        drop(lease);
        assert!(worker.try_acquire().unwrap().is_some());
    }

    #[test]
    fn foreground_transition_returns_a_terminal_cancellation_without_a_host_wait_loop() {
        let gate = MvActivityGate::new();
        let first = gate
            .acquire_foreground(target("mv"), MvActivityOwner::ManualRefresh, || false)
            .expect("first foreground transition acquires its lease");
        let cancelled =
            gate.acquire_foreground(target("mv"), MvActivityOwner::ManualRefresh, || true);
        assert!(matches!(
            cancelled,
            Err(MvActivityAdmissionError::Cancelled)
        ));
        drop(first);
    }

    #[test]
    fn foreground_waiter_is_woken_by_lease_release() {
        let gate = MvActivityGate::new();
        let first = gate
            .acquire_foreground(target("mv"), MvActivityOwner::ManualRefresh, || false)
            .expect("first foreground transition acquires its lease");
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        let waiting_gate = gate.clone();
        let waiter = std::thread::spawn(move || {
            started_tx.send(()).expect("test waiter starts");
            let lease = waiting_gate
                .acquire_foreground(target("mv"), MvActivityOwner::ManualRefresh, || false)
                .expect("released lease wakes the next foreground transition");
            finished_tx.send(()).expect("test waiter completes");
            drop(lease);
        });
        started_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("waiter starts before releasing active lease");
        drop(first);
        finished_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("lease release wakes FIFO waiter without host sleeping");
        waiter.join().expect("waiter does not panic");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn incremental_async_foreground_transition_returns_a_terminal_cancellation_without_a_host_wait_loop()
     {
        let gate = MvActivityGate::new();
        let first = gate
            .acquire_foreground_async(
                target("mv"),
                MvActivityOwner::ManualRefresh,
                std::future::pending(),
            )
            .await
            .expect("first foreground transition acquires its lease");
        let cancelled = gate
            .acquire_foreground_async(
                target("mv"),
                MvActivityOwner::ManualRefresh,
                std::future::ready(()),
            )
            .await;
        assert!(matches!(
            cancelled,
            Err(MvActivityAdmissionError::Cancelled)
        ));
        assert_eq!(gate.observe(&target("mv")).waiting, 0);
        drop(first);
        assert_eq!(gate.tracked_target_count(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn incremental_async_foreground_waiter_is_woken_by_lease_release() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Context, Poll, Wake, Waker};
        struct Count(AtomicUsize);
        impl Wake for Count {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let gate = MvActivityGate::new();
        let first = gate
            .acquire_foreground(target("mv"), MvActivityOwner::Create, || false)
            .expect("legacy admission owns the same target");
        let wake = Arc::new(Count(AtomicUsize::new(0)));
        let waker = Waker::from(wake.clone());
        let mut cx = Context::from_waker(&waker);
        let mut second = Box::pin(gate.acquire_foreground_async(
            target("mv"),
            MvActivityOwner::ManualRefresh,
            std::future::pending(),
        ));
        assert!(second.as_mut().poll(&mut cx).is_pending());
        let before = wake.0.load(Ordering::SeqCst);
        drop(first);
        assert!(wake.0.load(Ordering::SeqCst) > before);
        let Poll::Ready(Ok(lease)) = second.as_mut().poll(&mut cx) else {
            panic!("actual lease release must wake and admit the original FIFO waiter");
        };
        drop(second);
        drop(lease);
        assert_eq!(gate.tracked_target_count(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn incremental_async_cancellation_wakes_and_removes_the_original_waiter_without_a_gate_timer()
     {
        let gate = MvActivityGate::new();
        let first = gate
            .acquire_foreground_async(
                target("mv"),
                MvActivityOwner::ManualRefresh,
                std::future::pending(),
            )
            .await
            .unwrap();
        let cancellation = QueryCancellationSource::new();
        let view = cancellation.view();
        let mut second = Box::pin(gate.acquire_foreground_async(
            target("mv"),
            MvActivityOwner::ManualRefresh,
            async {
                let _ = view.cancelled().await;
            },
        ));
        struct Count(std::sync::atomic::AtomicUsize);
        impl std::task::Wake for Count {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let wake = Arc::new(Count(std::sync::atomic::AtomicUsize::new(0)));
        let waker = std::task::Waker::from(wake.clone());
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(second.as_mut().poll(&mut cx).is_pending());
        assert_eq!(gate.observe(&target("mv")).waiting, 1);
        cancellation.request(QueryCancellationReason::ClientDisconnected);
        assert!(
            wake.0.load(std::sync::atomic::Ordering::SeqCst) > 0,
            "same cancellation must wake without a gate timer"
        );
        assert!(matches!(
            second.await,
            Err(MvActivityAdmissionError::Cancelled)
        ));
        assert_eq!(gate.observe(&target("mv")).waiting, 0);
        drop(first);
        assert_eq!(gate.tracked_target_count(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn incremental_async_worker_cancellation_panic_does_not_withhold_gate_closure() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Context, Poll, Wake, Waker};
        struct PanicWake(MvActivityGate);
        impl Wake for PanicWake {
            fn wake(self: Arc<Self>) {
                assert!(
                    self.0.inner.state.try_lock().is_ok(),
                    "worker cancellation runs outside FIFO lock"
                );
                panic!("actual worker cancellation wake failed");
            }
        }
        struct Count(AtomicUsize);
        impl Wake for Count {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let gate = MvActivityGate::new();
        let mut worker = gate
            .request(target("mv"), MvActivityOwner::ScheduledRefresh)
            .unwrap();
        let lease = worker.try_acquire().unwrap().unwrap();
        let cancellation = lease.cancellation().unwrap();
        let mut worker_wait = Box::pin(cancellation.cancelled());
        let worker_waker = Waker::from(Arc::new(PanicWake(gate.clone())));
        assert!(
            worker_wait
                .as_mut()
                .poll(&mut Context::from_waker(&worker_waker))
                .is_pending()
        );
        let wake = Arc::new(Count(AtomicUsize::new(0)));
        let waker = Waker::from(wake.clone());
        let mut cx = Context::from_waker(&waker);
        let mut queued = Box::pin(gate.acquire_foreground_async(
            target("mv"),
            MvActivityOwner::ManualRefresh,
            std::future::pending(),
        ));
        assert!(queued.as_mut().poll(&mut cx).is_pending());
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| gate.begin_stopping()));
        assert!(
            result.is_err(),
            "the original wake panic must remain observable"
        );
        assert!(
            wake.0.load(Ordering::SeqCst) > 0,
            "gate closure has its own actual notification"
        );
        assert!(matches!(
            queued.as_mut().poll(&mut cx),
            Poll::Ready(Err(MvActivityAdmissionError::Stopping))
        ));
        drop(queued);
        drop(worker_wait);
        drop(lease);
        assert_eq!(gate.tracked_target_count(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn incremental_async_borrow_drop_keeps_ticket_and_enqueue_does_not_wake_waiters() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Context, Wake, Waker};
        struct Count(AtomicUsize);
        impl Wake for Count {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let gate = MvActivityGate::new();
        let first = gate
            .acquire_foreground(target("mv"), MvActivityOwner::Create, || false)
            .unwrap();
        let mut original = gate
            .request(target("mv"), MvActivityOwner::Repartition)
            .unwrap();
        let wake = Arc::new(Count(AtomicUsize::new(0)));
        let waker = Waker::from(wake.clone());
        let mut cx = Context::from_waker(&waker);
        {
            let mut borrow = Box::pin(original.acquire_waiting_async(std::future::pending()));
            assert!(borrow.as_mut().poll(&mut cx).is_pending());
        }
        assert_eq!(gate.observe(&target("mv")).waiting, 1);
        let before = wake.0.load(Ordering::SeqCst);
        let mut later = gate.request(target("mv"), MvActivityOwner::Drop).unwrap();
        assert_eq!(
            wake.0.load(Ordering::SeqCst),
            before,
            "enqueue cannot enable an existing waiter"
        );
        drop(first);
        assert!(
            later.try_acquire().unwrap().is_none(),
            "borrow drop cannot surrender the original FIFO position"
        );
        let lease = original
            .acquire_waiting_async(std::future::pending())
            .await
            .unwrap()
            .unwrap();
        assert!(later.try_acquire().unwrap().is_none());
        drop(lease);
        drop(later.try_acquire().unwrap().unwrap());
        assert_eq!(gate.tracked_target_count(), 0);
    }

    #[test]
    fn shutdown_cancels_worker_but_not_manual_activity() {
        let gate = MvActivityGate::new();
        let mut worker = gate
            .request(target("worker"), MvActivityOwner::ScheduledRefresh)
            .unwrap();
        let worker_lease = worker.try_acquire().unwrap().unwrap();
        let worker_cancellation = worker_lease.cancellation().unwrap();
        let mut manual = gate
            .request(target("manual"), MvActivityOwner::ManualRefresh)
            .unwrap();
        let manual_lease = manual.try_acquire().unwrap().unwrap();
        assert!(manual_lease.cancellation().is_none());
        gate.begin_stopping();
        assert_eq!(
            worker_cancellation.reason(),
            Some(QueryCancellationReason::ServerShutdown)
        );
    }

    #[test]
    fn cancelled_and_finished_activity_reaps_target() {
        let gate = MvActivityGate::new();
        let ticket = gate
            .request(target("cancelled"), MvActivityOwner::ScheduledRefresh)
            .unwrap();
        assert_eq!(gate.tracked_target_count(), 1);
        drop(ticket);
        assert_eq!(gate.tracked_target_count(), 0);

        let mut terminal = gate
            .request(target("terminal"), MvActivityOwner::ScheduledRefresh)
            .unwrap();
        let lease = terminal.try_acquire().unwrap().unwrap();
        assert_eq!(gate.tracked_target_count(), 1);
        drop(lease);
        assert_eq!(gate.tracked_target_count(), 0);
    }

    #[test]
    fn ddl_refresh_and_repartition_share_one_fifo_target_queue() {
        let gate = MvActivityGate::new();
        let mut create = gate.request(target("mv"), MvActivityOwner::Create).unwrap();
        let mut refresh = gate
            .request(target("mv"), MvActivityOwner::ManualRefresh)
            .unwrap();
        let mut repartition = gate
            .request(target("mv"), MvActivityOwner::Repartition)
            .unwrap();
        let mut drop_ticket = gate.request(target("mv"), MvActivityOwner::Drop).unwrap();
        let create_lease = create.try_acquire().unwrap().unwrap();
        assert!(refresh.try_acquire().unwrap().is_none());
        assert!(repartition.try_acquire().unwrap().is_none());
        assert!(drop_ticket.try_acquire().unwrap().is_none());
        drop(create_lease);
        let refresh_lease = refresh.try_acquire().unwrap().unwrap();
        assert!(repartition.try_acquire().unwrap().is_none());
        drop(refresh_lease);
        let repartition_lease = repartition.try_acquire().unwrap().unwrap();
        assert!(drop_ticket.try_acquire().unwrap().is_none());
        drop(repartition_lease);
        assert!(drop_ticket.try_acquire().unwrap().is_some());
    }

    /// The process-local gate is a fairness and shutdown mechanism, not a
    /// durable MV ownership fence.
    #[test]
    fn separate_process_runtimes_do_not_arbitrate_publications() {
        let target = CanonicalMvTarget::from_parts(Some("ice"), "sales", "daily");
        let first = MvActivityGate::new();
        let mut first_ticket = first
            .request(target.clone(), MvActivityOwner::ManualRefresh)
            .unwrap();
        let first_lease = first_ticket.try_acquire().unwrap().unwrap();
        let mut same_process = first
            .request(target.clone(), MvActivityOwner::ScheduledRefresh)
            .unwrap();
        assert!(same_process.try_acquire().unwrap().is_none());

        let second = MvActivityGate::new();
        let mut other_process = second
            .request(target, MvActivityOwner::ScheduledRefresh)
            .unwrap();
        assert!(other_process.try_acquire().unwrap().is_some());
        drop(first_lease);
    }
}
