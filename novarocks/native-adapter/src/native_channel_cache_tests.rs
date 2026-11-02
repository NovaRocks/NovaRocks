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

//! Fixed cache rows, single-flight election and notification tests.
//! Lazy Channels are publication values only: their tasks and connections are
//! outside this fixture.

use super::*;
use novarocks_proto_codec::native_rpc::NativeRpcMethod;
use novarocks_types::{BackendProcessId, NativeEndpoint};
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Weak;
use std::sync::atomic::Ordering;
use std::task::{RawWaker, RawWakerVTable, Wake};
use tonic::transport::Channel;

// Election mechanics do not depend on the published value; these fixtures
// publish bare lazy Channels.
type NativeChannelCache = super::NativeChannelCache<Channel>;
type Acquire = super::Acquire<Channel>;
type Election = super::Election<Channel>;
type Leader = super::Leader<Channel>;
type Core = super::Core<Channel>;

fn fixture() -> (NativeChannelCache, (), (), ()) {
    (NativeChannelCache::bounded().unwrap(), (), (), ())
}

fn identity(suffix: u8, port: u16, method: NativeRpcMethod) -> InlineNativeChannelIdentity {
    let mut bytes = [0; 16];
    bytes[6] = 0x70;
    bytes[8] = 0x80;
    bytes[15] = suffix;
    let peer = BackendProcessId::try_from_bytes(bytes).unwrap();
    let endpoint = NativeEndpoint::from_host_port("cache.example.com", port).unwrap();
    InlineNativeChannelIdentity::from_parts(Some(peer), &endpoint, method).unwrap()
}

fn poll(acquire: &mut Acquire, waker: &Waker) -> Poll<io::Result<Election>> {
    Pin::new(acquire).poll(&mut Context::from_waker(waker))
}

fn elect(cache: &NativeChannelCache, key: InlineNativeChannelIdentity) -> Leader {
    match poll(&mut cache.acquire(key), Waker::noop()) {
        Poll::Ready(Ok(Election::Leader(leader))) => leader,
        _ => panic!("a fresh key must elect exactly one leader"),
    }
}

fn assert_error(outcome: Poll<io::Result<Election>>, expected: io::ErrorKind) {
    match outcome {
        Poll::Ready(Err(error)) => assert_eq!(error.kind(), expected),
        _ => panic!("cache acquisition must return the expected explicit refusal"),
    }
}

struct WakeCount {
    calls: AtomicUsize,
    panic: bool,
}

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.panic {
            std::panic::panic_any("first cache notification panic");
        }
    }
}

fn counting_waker(panic: bool) -> (Waker, Arc<WakeCount>) {
    let count = Arc::new(WakeCount {
        calls: AtomicUsize::new(0),
        panic,
    });
    (Waker::from(count.clone()), count)
}

#[tokio::test]
async fn one_leader_eight_waiters_and_ninth_refusal_publish_one_result() {
    let (cache, _factory, _budget, _) = fixture();
    let key = identity(1, 9000, NativeRpcMethod::DeliverExchangeUnit);
    let leader = elect(&cache, key);
    let (waker, count) = counting_waker(false);
    let mut waiting: Vec<_> = (0..8).map(|_| cache.acquire(key)).collect();
    for waiter in &mut waiting {
        assert!(poll(waiter, &waker).is_pending());
    }
    assert_error(
        poll(&mut cache.acquire(key), &waker),
        io::ErrorKind::WouldBlock,
    );
    assert_eq!(count.calls.load(Ordering::SeqCst), 0);
    let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:9").connect_lazy();
    leader.publish(channel).unwrap();
    assert_eq!(count.calls.load(Ordering::SeqCst), 8);
    for waiter in &mut waiting {
        assert!(matches!(
            poll(waiter, &waker),
            Poll::Ready(Ok(Election::Ready(_)))
        ));
    }
    assert!(matches!(
        poll(&mut cache.acquire(key), &waker),
        Poll::Ready(Ok(Election::Ready(_)))
    ));
}

#[test]
fn distinct_frozen_peers_endpoints_and_lanes_elect_independently() {
    let (cache, _factory, _budget, _) = fixture();
    let leaders: Vec<_> = [
        identity(1, 9000, NativeRpcMethod::DeliverExchangeUnit),
        identity(2, 9000, NativeRpcMethod::DeliverExchangeUnit),
        identity(1, 9001, NativeRpcMethod::DeliverExchangeUnit),
        identity(1, 9000, NativeRpcMethod::TransmitRuntimeFilterEnvelope),
    ]
    .into_iter()
    .map(|key| elect(&cache, key))
    .collect();
    for (index, first) in leaders.iter().enumerate() {
        for second in &leaders[index + 1..] {
            assert_ne!(first.entry, second.entry);
        }
    }
    drop(leaders);
}

#[test]
fn cancelled_waiter_returns_its_position_and_cancelled_leader_notifies_failure() {
    let (cache, _factory, _budget, _) = fixture();
    let key = identity(1, 9000, NativeRpcMethod::DeliverExchangeUnit);
    let leader = elect(&cache, key);
    let (waker, count) = counting_waker(false);
    let mut waiting: Vec<_> = (0..8).map(|_| cache.acquire(key)).collect();
    for waiter in &mut waiting {
        assert!(poll(waiter, &waker).is_pending());
    }
    drop(waiting.pop());
    let mut replacement = cache.acquire(key);
    assert!(poll(&mut replacement, &waker).is_pending());
    drop(leader);
    assert_eq!(count.calls.load(Ordering::SeqCst), 8);
    for waiter in &mut waiting {
        assert_error(poll(waiter, &waker), io::ErrorKind::ConnectionAborted);
    }
    assert_error(
        poll(&mut replacement, &waker),
        io::ErrorKind::ConnectionAborted,
    );
    drop(elect(&cache, key));
}

#[tokio::test]
async fn stale_election_cannot_publish_or_cancel_the_replacement_generation() {
    let (cache, _factory, _budget, _) = fixture();
    let key = identity(1, 9000, NativeRpcMethod::DeliverExchangeUnit);
    let old = elect(&cache, key);
    let entry = old.entry;
    let generation = old.generation;
    drop(old);
    let current = elect(&cache, key);
    assert_eq!(current.entry, entry);
    assert!(current.generation > generation);
    // Recreate an old callback token without changing current cache state.
    let stale = Leader {
        cache: cache.clone(),
        entry,
        generation,
        completed: false,
    };
    let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:9").connect_lazy();
    assert_eq!(
        stale.publish(channel.clone()).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    let mut waiter = cache.acquire(key);
    assert!(poll(&mut waiter, Waker::noop()).is_pending());
    current.publish(channel).unwrap();
    assert!(matches!(
        poll(&mut waiter, Waker::noop()),
        Poll::Ready(Ok(Election::Ready(_)))
    ));
}

#[test]
fn exhausted_entry_and_waiter_generations_refuse_without_wrapping() {
    let (cache, _factory, _budget, _) = fixture();
    let key = identity(1, 9000, NativeRpcMethod::DeliverExchangeUnit);
    let leader = elect(&cache, key);
    {
        let mut state = cache.core().state.lock().unwrap();
        let entries = &mut *state;
        for waiter in &mut entries[leader.entry].waiters {
            waiter.generation = u64::MAX;
        }
    }
    assert_error(
        poll(&mut cache.acquire(key), Waker::noop()),
        io::ErrorKind::WouldBlock,
    );
    drop(leader);
    {
        let mut state = cache.core().state.lock().unwrap();
        let entries = &mut *state;
        for entry in entries.iter_mut() {
            entry.generation = u64::MAX;
        }
    }
    assert_error(
        poll(&mut cache.acquire(key), Waker::noop()),
        io::ErrorKind::WouldBlock,
    );
    let state = cache.core().state.lock().unwrap();
    let entries = &*state;
    assert!(entries.iter().all(|entry| entry.generation == u64::MAX));
}

struct ReentrantWake {
    core: Weak<Core>,
    clones: AtomicUsize,
    drops: AtomicUsize,
    wakes: AtomicUsize,
}

impl ReentrantWake {
    fn check_unlocked(&self) {
        let core = self
            .core
            .upgrade()
            .expect("cache still owned by acquisition");
        assert!(
            core.state.try_lock().is_ok(),
            "arbitrary waker callback ran under cache mutex"
        );
    }
}

unsafe fn reentrant_clone(data: *const ()) -> RawWaker {
    // The vtable owns one Arc count; borrowing it must not consume that count.
    let owner = ManuallyDrop::new(unsafe { Arc::from_raw(data.cast::<ReentrantWake>()) });
    owner.check_unlocked();
    owner.clones.fetch_add(1, Ordering::SeqCst);
    RawWaker::new(Arc::into_raw(Arc::clone(&owner)).cast(), &REENTRANT_VTABLE)
}
unsafe fn reentrant_wake(data: *const ()) {
    // Consuming wake owns and retires exactly the count represented by data.
    let owner = unsafe { Arc::from_raw(data.cast::<ReentrantWake>()) };
    owner.check_unlocked();
    owner.wakes.fetch_add(1, Ordering::SeqCst);
}
unsafe fn reentrant_wake_by_ref(data: *const ()) {
    // Borrowed wake leaves the represented Arc count intact.
    let owner = ManuallyDrop::new(unsafe { Arc::from_raw(data.cast::<ReentrantWake>()) });
    owner.check_unlocked();
    owner.wakes.fetch_add(1, Ordering::SeqCst);
}
unsafe fn reentrant_drop(data: *const ()) {
    // Drop consumes exactly the count represented by this RawWaker.
    let owner = unsafe { Arc::from_raw(data.cast::<ReentrantWake>()) };
    owner.check_unlocked();
    owner.drops.fetch_add(1, Ordering::SeqCst);
}
static REENTRANT_VTABLE: RawWakerVTable = RawWakerVTable::new(
    reentrant_clone,
    reentrant_wake,
    reentrant_wake_by_ref,
    reentrant_drop,
);

#[test]
fn arbitrary_waker_clone_drop_and_wake_can_reenter_the_unlocked_cache() {
    let (cache, _factory, _budget, _) = fixture();
    let key = identity(1, 9000, NativeRpcMethod::DeliverExchangeUnit);
    let leader = elect(&cache, key);
    let owner = Arc::new(ReentrantWake {
        core: Arc::downgrade(cache.core()),
        clones: AtomicUsize::new(0),
        drops: AtomicUsize::new(0),
        wakes: AtomicUsize::new(0),
    });
    let raw = RawWaker::new(Arc::into_raw(owner.clone()).cast(), &REENTRANT_VTABLE);
    // Every callback uses the same Arc allocation and the matching vtable.
    let waker = unsafe { Waker::from_raw(raw) };
    let mut waiter = cache.acquire(key);
    assert!(poll(&mut waiter, &waker).is_pending());
    assert!(poll(&mut waiter, &waker).is_pending());
    assert!(owner.clones.load(Ordering::SeqCst) >= 2);
    assert!(owner.drops.load(Ordering::SeqCst) >= 1);
    drop(leader);
    assert_eq!(owner.wakes.load(Ordering::SeqCst), 1);
    assert_error(poll(&mut waiter, &waker), io::ErrorKind::ConnectionAborted);
    drop(waiter);
    drop(waker);
}

#[tokio::test]
async fn first_notification_panic_does_not_skip_second_or_poison_published_result() {
    let (cache, _factory, _budget, _) = fixture();
    let key = identity(1, 9000, NativeRpcMethod::DeliverExchangeUnit);
    let leader = elect(&cache, key);
    let (first_waker, first_count) = counting_waker(true);
    let (second_waker, second_count) = counting_waker(false);
    let mut first = cache.acquire(key);
    let mut second = cache.acquire(key);
    assert!(poll(&mut first, &first_waker).is_pending());
    assert!(poll(&mut second, &second_waker).is_pending());
    let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:9").connect_lazy();
    let panic = catch_unwind(AssertUnwindSafe(|| leader.publish(channel))).unwrap_err();
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"first cache notification panic")
    );
    assert_eq!(first_count.calls.load(Ordering::SeqCst), 1);
    assert_eq!(second_count.calls.load(Ordering::SeqCst), 1);
    for waiter in [&mut first, &mut second] {
        assert!(matches!(
            poll(waiter, Waker::noop()),
            Poll::Ready(Ok(Election::Ready(_)))
        ));
    }
}

#[test]
fn leader_cancellation_during_outer_unwind_notifies_all_without_double_panic() {
    let (cache, _factory, _budget, _) = fixture();
    let key = identity(1, 9000, NativeRpcMethod::DeliverExchangeUnit);
    let leader = elect(&cache, key);
    let (first_waker, first_count) = counting_waker(true);
    let (second_waker, second_count) = counting_waker(false);
    let mut first = cache.acquire(key);
    let mut second = cache.acquire(key);
    assert!(poll(&mut first, &first_waker).is_pending());
    assert!(poll(&mut second, &second_waker).is_pending());
    let panic = catch_unwind(AssertUnwindSafe(move || {
        let _leader = leader;
        std::panic::panic_any("outer cache owner unwind");
    }))
    .unwrap_err();
    assert_eq!(
        panic.downcast_ref::<&str>(),
        Some(&"outer cache owner unwind")
    );
    assert_eq!(first_count.calls.load(Ordering::SeqCst), 1);
    assert_eq!(second_count.calls.load(Ordering::SeqCst), 1);
    assert_error(
        poll(&mut first, Waker::noop()),
        io::ErrorKind::ConnectionAborted,
    );
    assert_error(
        poll(&mut second, Waker::noop()),
        io::ErrorKind::ConnectionAborted,
    );
    drop(elect(&cache, key));
}

#[tokio::test]
async fn full_ready_cache_recycles_cold_entries_with_fresh_generations_and_rotation() {
    let (cache, _factory, _budget, _) = fixture();
    let count = entry_positions().unwrap();
    assert_eq!(count, 230);
    let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:9").connect_lazy();
    let keys: Vec<_> = (1..=count)
        .map(|suffix| {
            identity(
                u8::try_from(suffix).unwrap(),
                9000,
                NativeRpcMethod::DeliverExchangeUnit,
            )
        })
        .collect();
    let mut previous_generations = vec![0; count];
    for &key in &keys {
        let leader = elect(&cache, key);
        previous_generations[leader.entry] = leader.generation;
        leader.publish(channel.clone()).unwrap();
    }
    let first_key = identity(231, 9000, NativeRpcMethod::DeliverExchangeUnit);
    let first = elect(&cache, first_key);
    let first_entry = first.entry;
    assert_eq!(first.generation, previous_generations[first_entry] + 1);
    first.publish(channel.clone()).unwrap();
    // A warm hit remains usable rather than causing another cold-key eviction.
    assert!(matches!(
        poll(&mut cache.acquire(first_key), Waker::noop()),
        Poll::Ready(Ok(Election::Ready(_)))
    ));
    let second = elect(
        &cache,
        identity(232, 9000, NativeRpcMethod::DeliverExchangeUnit),
    );
    assert_ne!(
        second.entry, first_entry,
        "eviction must rotate instead of replacing the newest row repeatedly"
    );
    assert_eq!(second.generation, previous_generations[second.entry] + 1);
    second.publish(channel.clone()).unwrap();
    // The genuinely evicted key is a miss again, with another exact election.
    let old_key = keys[first_entry];
    let replacement = elect(&cache, old_key);
    replacement.publish(channel).unwrap();
    assert!(matches!(
        poll(&mut cache.acquire(old_key), Waker::noop()),
        Poll::Ready(Ok(Election::Ready(_)))
    ));
}

#[tokio::test]
async fn full_connecting_occupied_ready_or_exhausted_rows_refuse_cold_eviction() {
    let (cache, _factory, _budget, _) = fixture();
    let count = entry_positions().unwrap();
    let keys: Vec<_> = (1..=count)
        .map(|suffix| {
            identity(
                u8::try_from(suffix).unwrap(),
                9000,
                NativeRpcMethod::DeliverExchangeUnit,
            )
        })
        .collect();
    let cold_key = identity(231, 9000, NativeRpcMethod::DeliverExchangeUnit);
    let leaders: Vec<_> = keys.iter().map(|&key| elect(&cache, key)).collect();
    assert_error(
        poll(&mut cache.acquire(cold_key), Waker::noop()),
        io::ErrorKind::WouldBlock,
    );
    drop(leaders);

    let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:9").connect_lazy();
    let mut waiting = Vec::with_capacity(count);
    for key in keys {
        let leader = elect(&cache, key);
        let mut waiter = cache.acquire(key);
        assert!(poll(&mut waiter, Waker::noop()).is_pending());
        leader.publish(channel.clone()).unwrap();
        // Notification is not waiter exit: these Ready rows still have users
        // that must be able to observe the original published generation.
        waiting.push(waiter);
    }
    assert_error(
        poll(&mut cache.acquire(cold_key), Waker::noop()),
        io::ErrorKind::WouldBlock,
    );
    drop(waiting);

    {
        let mut state = cache.core().state.lock().unwrap();
        let entries = &mut *state;
        for entry in entries.iter_mut() {
            entry.generation = u64::MAX;
        }
    }
    assert_error(
        poll(&mut cache.acquire(cold_key), Waker::noop()),
        io::ErrorKind::WouldBlock,
    );
    let state = cache.core().state.lock().unwrap();
    let entries = &*state;
    assert!(
        entries
            .iter()
            .all(|entry| matches!(entry.phase, Phase::Ready(_)) && entry.generation == u64::MAX)
    );
}
