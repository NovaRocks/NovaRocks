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

//! Attempt-local stop signals shared across connector and file operations.
//!
//! The owner can stop its own subtree. A view can only observe that decision;
//! neither Tokio's token nor a task registry becomes part of the connector
//! contract. Cancellation is a wakeup, not a typed failure cause or evidence
//! that started work has drained.

use std::any::Any;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

// Registrations belong only to polled, live StopWait futures. There is no
// child registry, detached relay task, or history of dropped observers.
#[derive(Default)]
struct StopSignal {
    stopped: AtomicBool,
    waiters: Mutex<BTreeMap<usize, Waker>>,
}

// A live allocation identifies one observer registration. It is process-local
// bookkeeping only; neither addresses nor IDs cross a Connector boundary.
struct WaiterIdentity;

struct StopWait {
    signal: Arc<StopSignal>,
    identity: Arc<WaiterIdentity>,
}

impl Future for StopWait {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.signal.stopped.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        // Cloning and dropping an arbitrary Waker may invoke user callbacks.
        // Neither action, nor waking, is permitted under the signal mutex.
        let mut next_waker = Some(cx.waker().clone());
        let key = Arc::as_ptr(&self.identity) as usize;
        let (stopped, previous) = {
            let mut waiters = self
                .signal
                .waiters
                .lock()
                .expect("stop signal mutex poisoned");
            if self.signal.stopped.load(Ordering::Acquire) {
                (true, None)
            } else {
                (
                    false,
                    waiters.insert(key, next_waker.take().expect("registered waker exists")),
                )
            }
        };
        drop(previous);
        drop(next_waker);
        if stopped {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for StopWait {
    fn drop(&mut self) {
        let key = Arc::as_ptr(&self.identity) as usize;
        let previous = {
            self.signal
                .waiters
                .lock()
                .expect("stop signal mutex poisoned")
                .remove(&key)
        };
        drop(previous);
    }
}

/// The only capability allowed to request a stop for one operation subtree.
#[derive(Clone, Default)]
pub struct ConnectorStopOwner {
    signal: Arc<StopSignal>,
    parent: Option<ConnectorStopView>,
}

/// A read-only view of one operation's stop state.
#[derive(Clone)]
pub struct ConnectorStopView {
    inner: StopViewInner,
    /// Keeps a host-owned signal relay alive even when a file operation
    /// outlives the Connector request object that admitted it.
    lifetime: Option<Arc<dyn Any + Send + Sync>>,
}

#[derive(Clone)]
enum StopViewInner {
    Signal(Arc<StopSignal>),
    Any(Arc<[ConnectorStopView]>),
}

impl ConnectorStopOwner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn view(&self) -> ConnectorStopView {
        let own = ConnectorStopView {
            inner: StopViewInner::Signal(self.signal.clone()),
            lifetime: None,
        };
        match &self.parent {
            Some(parent) => ConnectorStopView::any_of(parent.clone(), [own]),
            None => own,
        }
    }

    /// Derive a child that the caller may stop without stopping this owner.
    pub fn child(&self) -> Self {
        Self {
            signal: Arc::new(StopSignal::default()),
            parent: Some(self.view()),
        }
    }

    pub fn request_stop(&self) {
        let waiters = {
            let mut waiters = self
                .signal
                .waiters
                .lock()
                .expect("stop signal mutex poisoned");
            if self.signal.stopped.swap(true, Ordering::AcqRel) {
                return;
            }
            std::mem::take(&mut *waiters)
        };
        for waker in waiters.into_values() {
            waker.wake();
        }
    }

    pub fn is_stopped(&self) -> bool {
        self.signal.stopped.load(Ordering::Acquire)
            || self
                .parent
                .as_ref()
                .is_some_and(ConnectorStopView::is_stopped)
    }
}

impl ConnectorStopView {
    /// Combine independent, already-admitted stop authorities. Constructing a
    /// view installs no task or subscription; awaiting it registers with each
    /// source and observes a stop that happened before the await as well.
    pub fn any_of(first: Self, others: impl IntoIterator<Item = Self>) -> Self {
        let mut views = vec![first];
        views.extend(others);
        Self {
            inner: StopViewInner::Any(Arc::from(views)),
            lifetime: None,
        }
    }

    pub(crate) fn with_lifetime<T: Any + Send + Sync>(mut self, lifetime: Arc<T>) -> Self {
        self.lifetime = Some(lifetime);
        self
    }

    pub fn is_stopped(&self) -> bool {
        match &self.inner {
            StopViewInner::Signal(signal) => signal.stopped.load(Ordering::Acquire),
            StopViewInner::Any(views) => views.iter().any(Self::is_stopped),
        }
    }

    /// This future is safe to create before or after a stop request and does
    /// not need a running Tokio executor merely to observe the flag.
    pub fn stopped(&self) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        let keepalive = self.clone();
        let wait: Pin<Box<dyn Future<Output = ()> + Send + 'static>> = match &self.inner {
            StopViewInner::Signal(signal) => Box::pin(StopWait {
                signal: signal.clone(),
                identity: Arc::new(WaiterIdentity),
            }),
            StopViewInner::Any(views) => {
                let mut waits: Vec<_> = views.iter().map(Self::stopped).collect();
                Box::pin(async move {
                    std::future::poll_fn(move |context| {
                        for wait in &mut waits {
                            if wait.as_mut().poll(context).is_ready() {
                                return Poll::Ready(());
                            }
                        }
                        Poll::Pending
                    })
                    .await
                })
            }
        };
        Box::pin(async move {
            let _keepalive = keepalive;
            wait.await;
        })
    }
}

impl std::fmt::Debug for ConnectorStopOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorStopOwner")
            .field("stopped", &self.is_stopped())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ConnectorStopView {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorStopView")
            .field("stopped", &self.is_stopped())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::task::{ArcWake, noop_waker, waker};
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Barrier, Weak};

    type Wait = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

    #[derive(Default)]
    struct CountWake(AtomicUsize);

    impl ArcWake for CountWake {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn poll(wait: &mut Wait, wake: &Waker) -> Poll<()> {
        wait.as_mut().poll(&mut Context::from_waker(wake))
    }

    fn registrations(owner: &ConnectorStopOwner) -> usize {
        owner.signal.waiters.lock().unwrap().len()
    }

    #[test]
    fn manual_poll_wakes_all_live_waiters_and_keeps_stop_sticky() {
        let owner = ConnectorStopOwner::new();
        let mut waits: Vec<_> = (0..8).map(|_| owner.view().stopped()).collect();
        let counters: Vec<_> = (0..8).map(|_| Arc::new(CountWake::default())).collect();
        for (wait, counter) in waits.iter_mut().zip(&counters) {
            assert!(poll(wait, &waker(counter.clone())).is_pending());
        }
        assert_eq!(registrations(&owner), 8);
        owner.request_stop();
        owner.request_stop();
        assert_eq!(registrations(&owner), 0);
        for (wait, counter) in waits.iter_mut().zip(&counters) {
            assert_eq!(counter.0.load(Ordering::Relaxed), 1);
            assert!(poll(wait, &noop_waker()).is_ready());
        }
        assert!(poll(&mut owner.view().stopped(), &noop_waker()).is_ready());
    }

    #[test]
    fn unpolled_future_never_registers_and_prior_stop_completes_it() {
        let owner = ConnectorStopOwner::new();
        let mut wait = owner.view().stopped();
        assert_eq!(registrations(&owner), 0);
        owner.request_stop();
        assert!(poll(&mut wait, &noop_waker()).is_ready());
        assert_eq!(registrations(&owner), 0);
    }

    #[test]
    fn repeat_poll_updates_one_registration_and_drop_releases_waker() {
        let owner = ConnectorStopOwner::new();
        let mut wait = owner.view().stopped();
        let first = Arc::new(CountWake::default());
        let first_weak = Arc::downgrade(&first);
        let first_waker = waker(first.clone());
        assert!(poll(&mut wait, &first_waker).is_pending());
        drop(first_waker);
        drop(first);
        assert!(first_weak.upgrade().is_some());
        let second = Arc::new(CountWake::default());
        let second_weak = Arc::downgrade(&second);
        let second_waker = waker(second.clone());
        for _ in 0..16 {
            assert!(poll(&mut wait, &second_waker).is_pending());
            assert_eq!(registrations(&owner), 1);
        }
        assert!(first_weak.upgrade().is_none());
        drop(second_waker);
        drop(second);
        assert!(second_weak.upgrade().is_some());
        drop(wait);
        assert_eq!(registrations(&owner), 0);
        assert!(second_weak.upgrade().is_none());
    }

    #[test]
    fn owner_drop_does_not_manufacture_stop_or_retain_child_history() {
        let owner = ConnectorStopOwner::new();
        let parent_signal = Arc::downgrade(&owner.signal);
        let child = owner.child();
        let child_signal = Arc::downgrade(&child.signal);
        let mut wait = child.view().stopped();
        assert!(poll(&mut wait, &noop_waker()).is_pending());
        drop(owner);
        drop(child);
        assert!(parent_signal.upgrade().is_some());
        assert!(child_signal.upgrade().is_some());
        assert!(poll(&mut wait, &noop_waker()).is_pending());
        drop(wait);
        assert!(parent_signal.upgrade().is_none());
        assert!(child_signal.upgrade().is_none());
    }

    #[test]
    fn arbitrary_parent_depth_stops_only_descendants() {
        let root = ConnectorStopOwner::new();
        let sibling = root.child();
        let mut owners = vec![root.clone()];
        for _ in 0..64 {
            owners.push(owners.last().unwrap().child());
        }
        let mut waits: Vec<_> = owners.iter().map(|owner| owner.view().stopped()).collect();
        let counters: Vec<_> = owners
            .iter()
            .map(|_| Arc::new(CountWake::default()))
            .collect();
        for (wait, counter) in waits.iter_mut().zip(&counters) {
            assert!(poll(wait, &waker(counter.clone())).is_pending());
        }
        owners[32].request_stop();
        for (index, counter) in counters.iter().enumerate() {
            assert_eq!(counter.0.load(Ordering::Relaxed), usize::from(index >= 32));
        }
        for (index, ((owner, wait), counter)) in
            owners.iter().zip(&mut waits).zip(&counters).enumerate()
        {
            assert_eq!(owner.is_stopped(), index >= 32);
            // Keep the original observer registered for the later root stop.
            assert_eq!(poll(wait, &waker(counter.clone())).is_ready(), index >= 32);
        }
        assert!(!sibling.is_stopped());
        root.request_stop();
        assert!(sibling.is_stopped());
        for wait in waits.iter_mut().take(32) {
            assert!(poll(wait, &noop_waker()).is_ready());
        }
        for counter in &counters {
            assert_eq!(counter.0.load(Ordering::Relaxed), 1);
        }
    }

    #[test]
    fn any_of_subscribes_to_both_and_unregisters_other_on_completion() {
        for stop_first in [true, false] {
            let first = ConnectorStopOwner::new();
            let second = ConnectorStopOwner::new();
            let view = ConnectorStopView::any_of(first.view(), [second.view()]);
            let mut wait = view.stopped();
            let counter = Arc::new(CountWake::default());
            assert!(poll(&mut wait, &waker(counter.clone())).is_pending());
            assert_eq!(registrations(&first), 1);
            assert_eq!(registrations(&second), 1);
            let (stopped, untouched) = if stop_first {
                (&first, &second)
            } else {
                (&second, &first)
            };
            stopped.request_stop();
            assert_eq!(counter.0.load(Ordering::Relaxed), 1);
            assert!(poll(&mut wait, &noop_waker()).is_ready());
            drop(wait);
            assert_eq!(registrations(untouched), 0);
            assert!(!untouched.is_stopped());
        }
    }

    #[test]
    fn returned_future_retains_host_relay_until_completion_or_drop() {
        for complete in [true, false] {
            let owner = ConnectorStopOwner::new();
            let relay = Arc::new(());
            let relay_weak = Arc::downgrade(&relay);
            let view = owner.view().with_lifetime(relay.clone());
            let mut wait = view.stopped();
            drop(view);
            drop(relay);
            assert!(relay_weak.upgrade().is_some());
            assert!(poll(&mut wait, &noop_waker()).is_pending());
            if complete {
                owner.request_stop();
                assert!(poll(&mut wait, &noop_waker()).is_ready());
                assert!(relay_weak.upgrade().is_none());
            }
            drop(wait);
            assert!(relay_weak.upgrade().is_none());
        }
    }

    struct ReentrantWake {
        owner: ConnectorStopOwner,
        other_wait: Mutex<Option<Wait>>,
        calls: AtomicUsize,
    }

    impl ArcWake for ReentrantWake {
        fn wake_by_ref(this: &Arc<Self>) {
            // Fail immediately instead of hanging if a future implementation
            // accidentally invokes arbitrary callbacks under its mutex.
            assert!(this.owner.signal.waiters.try_lock().is_ok());
            assert!(this.owner.is_stopped());
            this.owner.request_stop();
            assert!(poll(&mut this.owner.view().stopped(), &noop_waker()).is_ready());
            drop(this.other_wait.lock().unwrap().take());
            this.calls.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn wake_can_reenter_stop_poll_and_unregister_without_signal_lock() {
        let owner = ConnectorStopOwner::new();
        let other_owner = ConnectorStopOwner::new();
        let mut other = other_owner.view().stopped();
        assert!(poll(&mut other, &noop_waker()).is_pending());
        let callback = Arc::new(ReentrantWake {
            owner: owner.clone(),
            other_wait: Mutex::new(Some(other)),
            calls: AtomicUsize::new(0),
        });
        let mut wait = owner.view().stopped();
        assert!(poll(&mut wait, &waker(callback.clone())).is_pending());
        owner.request_stop();
        assert_eq!(callback.calls.load(Ordering::Relaxed), 1);
        assert_eq!(registrations(&other_owner), 0);
        assert!(poll(&mut wait, &noop_waker()).is_ready());
    }

    struct DropChecksLock(Weak<StopSignal>);

    impl ArcWake for DropChecksLock {
        fn wake_by_ref(_: &Arc<Self>) {}
    }

    impl Drop for DropChecksLock {
        fn drop(&mut self) {
            if let Some(signal) = self.0.upgrade() {
                assert!(signal.waiters.try_lock().is_ok());
            }
        }
    }

    #[test]
    fn replacing_and_unregistering_wakers_drops_payload_outside_lock() {
        let owner = ConnectorStopOwner::new();
        let mut wait = owner.view().stopped();
        for _ in 0..2 {
            let tracked = waker(Arc::new(DropChecksLock(Arc::downgrade(&owner.signal))));
            assert!(poll(&mut wait, &tracked).is_pending());
            drop(tracked);
        }
        drop(wait);
        assert_eq!(registrations(&owner), 0);
    }

    #[test]
    fn racing_registration_and_stop_has_no_lost_wakeup() {
        for _ in 0..128 {
            let owner = ConnectorStopOwner::new();
            let view = owner.view();
            let barrier = Arc::new(Barrier::new(2));
            let thread_barrier = barrier.clone();
            let counter = Arc::new(CountWake::default());
            let thread_counter = counter.clone();
            let thread = std::thread::spawn(move || {
                let mut wait = view.stopped();
                thread_barrier.wait();
                let first = poll(&mut wait, &waker(thread_counter));
                (wait, first)
            });
            barrier.wait();
            owner.request_stop();
            let (mut wait, first) = thread.join().unwrap();
            if first.is_pending() {
                assert_eq!(counter.0.load(Ordering::Relaxed), 1);
                assert!(poll(&mut wait, &noop_waker()).is_ready());
            }
            // A completed Future must not be polled again. A fresh observer
            // proves that the stop remains visible after either race outcome.
            assert!(poll(&mut owner.view().stopped(), &noop_waker()).is_ready());
            assert_eq!(registrations(&owner), 0);
        }
    }

    #[tokio::test]
    async fn parent_stops_children_and_prior_stop_wakes_new_waiters() {
        let task = ConnectorStopOwner::new();
        let source = task.child();
        let operation = source.child();
        let first = operation.view().stopped();
        task.request_stop();
        first.await;
        operation.view().stopped().await;
        assert!(operation.is_stopped());
        assert!(source.is_stopped());
    }

    #[tokio::test]
    async fn child_stop_does_not_stop_parent_or_sibling() {
        let task = ConnectorStopOwner::new();
        let source = task.child();
        let sibling = task.child();
        let operation = source.child();
        let waiter_a = operation.view().stopped();
        let waiter_b = operation.view().stopped();
        source.request_stop();
        waiter_a.await;
        waiter_b.await;
        assert!(!task.is_stopped());
        assert!(!sibling.is_stopped());
    }

    #[tokio::test]
    async fn combined_view_wakes_from_either_authority_even_before_subscription() {
        let request = ConnectorStopOwner::new();
        let fence = ConnectorStopOwner::new();
        let combined = ConnectorStopView::any_of(request.view(), [fence.view()]);
        assert!(!combined.is_stopped());
        fence.request_stop();
        combined.stopped().await;
        assert!(combined.is_stopped());
        assert!(!request.is_stopped());

        let later = ConnectorStopView::any_of(request.view(), [ConnectorStopOwner::new().view()]);
        let waiter = later.stopped();
        request.request_stop();
        waiter.await;
    }
}
