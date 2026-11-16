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

//! Ordinary test-harness threads only. No custom stack, runtime or unsafe waker.

use super::GuardedFuture;
use std::cell::Cell;
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};

const DEPTH: usize = 20_000;
type BoxedFuture<'a> = Pin<Box<dyn Future<Output = usize> + 'a>>;

#[derive(Default)]
struct Counts {
    created: AtomicUsize,
    dropped: AtomicUsize,
    live: AtomicIsize,
    polled: AtomicUsize,
    user_panics: AtomicUsize,
}

struct Probe(Arc<Counts>);

impl Probe {
    fn new(counts: &Arc<Counts>) -> Self {
        counts.created.fetch_add(1, Ordering::SeqCst);
        counts.live.fetch_add(1, Ordering::SeqCst);
        Self(counts.clone())
    }

    fn polled(&self) {
        self.0.polled.fetch_add(1, Ordering::SeqCst);
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        self.0.live.fetch_sub(1, Ordering::SeqCst);
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

fn assert_released(counts: &Counts, nodes: usize) {
    assert_eq!(counts.created.load(Ordering::SeqCst), nodes);
    assert_eq!(counts.dropped.load(Ordering::SeqCst), nodes);
    assert_eq!(counts.live.load(Ordering::SeqCst), 0);
}

struct Leaf {
    ready: bool,
    panic_on_drop: bool,
    probe: Probe,
}

impl Future for Leaf {
    type Output = usize;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<usize> {
        self.probe.polled();
        if self.ready {
            Poll::Ready(0)
        } else {
            Poll::Pending
        }
    }
}

impl Drop for Leaf {
    fn drop(&mut self) {
        if self.panic_on_drop {
            self.probe.0.user_panics.fetch_add(1, Ordering::SeqCst);
            panic!("intentional guarded future user Drop panic");
        }
    }
}

struct Step<'a> {
    child: BoxedFuture<'a>,
    probe: Probe,
}

impl Future for Step<'_> {
    type Output = usize;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<usize> {
        let this = self.get_mut();
        this.probe.polled();
        this.child.as_mut().poll(cx).map(|value| value + 1)
    }
}

fn chain<'a>(
    depth: usize,
    leaf: impl Future<Output = usize> + 'a,
    counts: &Arc<Counts>,
) -> BoxedFuture<'a> {
    let mut child: BoxedFuture<'a> = Box::pin(GuardedFuture::new(leaf));
    for _ in 0..depth {
        child = Box::pin(GuardedFuture::new(Step {
            child,
            probe: Probe::new(counts),
        }));
    }
    child
}

fn leaf(counts: &Arc<Counts>, ready: bool, panic_on_drop: bool) -> Leaf {
    Leaf {
        ready,
        panic_on_drop,
        probe: Probe::new(counts),
    }
}

fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn twenty_thousand_dyn_edges_poll_ready_and_release_only_on_actual_drop() {
    let counts = Arc::new(Counts::default());
    let mut future = chain(DEPTH, leaf(&counts, true, false), &counts);
    assert_eq!(counts.live.load(Ordering::SeqCst), (DEPTH + 1) as isize);
    assert_eq!(poll_once(future.as_mut()), Poll::Ready(DEPTH));
    assert_eq!(counts.polled.load(Ordering::SeqCst), DEPTH + 1);
    // A completion value is not evidence that retained future resources exited.
    assert_eq!(counts.live.load(Ordering::SeqCst), (DEPTH + 1) as isize);
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 0);
    drop(future);
    assert_released(&counts, DEPTH + 1);
}

#[test]
fn never_polled_twenty_thousand_dyn_edges_release_every_payload() {
    let counts = Arc::new(Counts::default());
    let future = chain(DEPTH, leaf(&counts, false, false), &counts);
    assert_eq!(counts.live.load(Ordering::SeqCst), (DEPTH + 1) as isize);
    drop(future);
    assert_eq!(counts.polled.load(Ordering::SeqCst), 0);
    assert_released(&counts, DEPTH + 1);
}

#[derive(Default)]
struct BorrowState {
    polls: usize,
    drops: usize,
}

struct BorrowedLeaf<'a> {
    state: &'a mut BorrowState,
    probe: Probe,
}

impl Future for BorrowedLeaf<'_> {
    type Output = usize;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<usize> {
        let this = self.get_mut();
        this.state.polls += 1;
        this.probe.polled();
        Poll::Pending
    }
}

impl Drop for BorrowedLeaf<'_> {
    fn drop(&mut self) {
        self.state.drops += 1;
    }
}

#[test]
fn dropping_a_pending_deep_borrowed_future_ends_the_real_borrow_and_payloads() {
    let counts = Arc::new(Counts::default());
    let mut state = BorrowState::default();
    let mut future = chain(
        DEPTH,
        BorrowedLeaf {
            state: &mut state,
            probe: Probe::new(&counts),
        },
        &counts,
    );
    assert_eq!(poll_once(future.as_mut()), Poll::Pending);
    assert_eq!(counts.polled.load(Ordering::SeqCst), DEPTH + 1);
    assert_eq!(counts.live.load(Ordering::SeqCst), (DEPTH + 1) as isize);
    drop(future);
    // Reading state here requires the original mutable borrow to have ended.
    assert_eq!(state.polls, 1);
    assert_eq!(state.drops, 1);
    assert_released(&counts, DEPTH + 1);
}

struct Forest {
    first: BoxedFuture<'static>,
    second: BoxedFuture<'static>,
    probe: Probe,
}

impl Future for Forest {
    type Output = usize;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<usize> {
        let this = self.get_mut();
        this.probe.polled();
        assert_eq!(this.first.as_mut().poll(cx), Poll::Pending);
        assert_eq!(this.second.as_mut().poll(cx), Poll::Pending);
        Poll::Pending
    }
}

#[test]
fn a_single_deep_user_drop_panic_unwinds_guarded_stacks_and_releases_siblings() {
    let counts = Arc::new(Counts::default());
    let mut future = GuardedFuture::new(Forest {
        first: chain(DEPTH, leaf(&counts, false, true), &counts),
        second: chain(DEPTH, leaf(&counts, false, false), &counts),
        probe: Probe::new(&counts),
    });
    let nodes = 2 * (DEPTH + 1) + 1;
    assert_eq!(poll_once(Pin::new(&mut future)), Poll::Pending);
    assert_eq!(counts.polled.load(Ordering::SeqCst), nodes);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(future)));
    let payload = result.expect_err("the user's one Drop panic must propagate");
    assert_eq!(
        payload.downcast_ref::<&str>().copied(),
        Some("intentional guarded future user Drop panic")
    );
    assert_eq!(counts.user_panics.load(Ordering::SeqCst), 1);
    assert_released(&counts, nodes);
}

struct AddressPinnedFuture {
    address: Cell<Option<usize>>,
    probe: Probe,
    _pin: PhantomPinned,
}

impl Future for AddressPinnedFuture {
    type Output = usize;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<usize> {
        let this = self.as_ref().get_ref();
        let address = this as *const Self as usize;
        if let Some(first) = this.address.get() {
            assert_eq!(first, address, "the pinned F allocation moved");
        } else {
            this.address.set(Some(address));
        }
        this.probe.polled();
        Poll::Pending
    }
}

impl Drop for AddressPinnedFuture {
    fn drop(&mut self) {
        if let Some(first) = self.address.get() {
            assert_eq!(first, self as *const Self as usize, "Drop moved pinned F");
        }
    }
}

#[test]
fn moving_the_guard_preserves_a_not_unpin_future_and_borrowed_async_output() {
    let counts = Arc::new(Counts::default());
    let mut future = GuardedFuture::new(AddressPinnedFuture {
        address: Cell::new(None),
        probe: Probe::new(&counts),
        _pin: PhantomPinned,
    });
    assert_eq!(poll_once(Pin::new(&mut future)), Poll::Pending);
    let mut moved = future;
    assert_eq!(poll_once(Pin::new(&mut moved)), Poll::Pending);
    drop(moved);
    assert_eq!(counts.polled.load(Ordering::SeqCst), 2);
    assert_released(&counts, 1);

    // The real compiler-generated async F borrows a caller-owned string. No
    // 'static lifetime or Send promise is added by GuardedFuture::new.
    let text = String::from("borrowed output");
    let mut borrowing_async = GuardedFuture::new(async { text.as_str() });
    assert_eq!(
        poll_once(Pin::new(&mut borrowing_async)),
        Poll::Ready(text.as_str())
    );
    drop(borrowing_async);
    assert_eq!(text, "borrowed output");
}
