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

#![expect(
    clippy::disallowed_methods,
    reason = "Integration tests own and join the threads that exercise memory races; see ADR-0218."
)]
#![cfg(not(loom))]
mod common;
use common::*;
use novarocks_memory::{
    LaneShards,
    attribution::{AttributingAllocator, binding, future::AttributedFuture},
    lane::RecordRef,
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    future::Future,
    marker::PhantomPinned,
    pin::Pin,
    ptr::NonNull,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};
const BYTES: usize = 600;
fn layout() -> Layout {
    Layout::from_size_align(BYTES, 8).unwrap()
}
struct Block(NonNull<u8>);
// SAFETY: every block has one owner; moving it does not access its contents.
unsafe impl Send for Block {}
impl Block {
    fn new() -> Self {
        Self(NonNull::new(unsafe { AttributingAllocator::new(System).alloc(layout()) }).unwrap())
    }
    fn source(&self) -> RecordRef {
        unsafe { RecordRef::read(self.0.as_ptr().add(BYTES)) }
    }
}
impl Drop for Block {
    fn drop(&mut self) {
        unsafe { AttributingAllocator::new(System).dealloc(self.0.as_ptr(), layout()) };
    }
}
struct Probe {
    owner: LaneShards,
    events: Arc<Mutex<Vec<(&'static str, RecordRef)>>>,
    held: Vec<Block>,
    polls: usize,
    address: usize,
    panic_poll: bool,
    panic_drop: bool,
    _pin: PhantomPinned,
}
impl Probe {
    fn new(
        owner: LaneShards,
        events: Arc<Mutex<Vec<(&'static str, RecordRef)>>>,
        panic_poll: bool,
        panic_drop: bool,
    ) -> Self {
        let block = Block::new();
        assert_eq!(block.source(), reference(&owner));
        events.lock().unwrap().push(("construct", block.source()));
        Self {
            owner,
            events,
            held: vec![block],
            polls: 0,
            address: 0,
            panic_poll,
            panic_drop,
            _pin: PhantomPinned,
        }
    }
}
impl Future for Probe {
    type Output = Block;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Block> {
        let address = std::ptr::from_ref(&*self).addr();
        // SAFETY: fields are mutated in place; the !Unpin field is never moved.
        let this = unsafe { self.get_unchecked_mut() };
        if this.address == 0 {
            this.address = address;
        }
        assert_eq!(this.address, address);
        let block = Block::new();
        assert_eq!(block.source(), reference(&this.owner));
        this.events.lock().unwrap().push(("poll", block.source()));
        this.polls += 1;
        if this.panic_poll {
            panic!("intentional poll unwind");
        }
        if this.polls == 1 {
            this.held.push(block);
            Poll::Pending
        } else {
            Poll::Ready(block)
        }
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        if self.address != 0 {
            assert_eq!(self.address, std::ptr::from_ref(self).addr());
        }
        let block = Block::new();
        assert_eq!(block.source(), reference(&self.owner));
        self.events.lock().unwrap().push(("drop", block.source()));
        if self.panic_drop {
            panic!("intentional destructor unwind");
        }
    }
}
fn poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
#[test]
fn construction_poll_migration_output_and_in_place_drop_keep_their_thread_shards() {
    let a = authority(65_536);
    let q = work(&a);
    let owner = q.create_lane_shards();
    let events = Arc::new(Mutex::new(Vec::new()));
    let future = Box::pin(AttributedFuture::new(owner.clone(), || {
        Probe::new(owner.clone(), events.clone(), false, false)
    }));
    let future = std::thread::spawn(move || {
        let mut f = future;
        assert!(poll(f.as_mut()).is_pending());
        assert_eq!(binding::pending_bytes(), 0);
        f
    })
    .join()
    .unwrap();
    let (future, output) = std::thread::spawn(move || {
        let mut f = future;
        let Poll::Ready(output) = poll(f.as_mut()) else {
            panic!("second poll must complete")
        };
        assert_eq!(binding::pending_bytes(), 0);
        (f, output)
    })
    .join()
    .unwrap();
    let output_source = output.source();
    assert!(contains(&owner, output_source));
    assert_eq!(owner.snapshot().tagged_bytes, 3 * 608);
    std::thread::spawn(move || {
        drop(future);
        assert_eq!(binding::pending_bytes(), 0);
    })
    .join()
    .unwrap();
    assert_eq!(output.source(), output_source);
    assert_eq!(owner.snapshot().tagged_bytes, 608);
    drop(output);
    assert_eq!(owner.snapshot().tagged_bytes, 0);
    assert_eq!(owner.snapshot().outstanding, 0);
    assert_eq!(owner.snapshot().slot_pins, 0);
    let events = events.lock().unwrap();
    assert_eq!(
        events.iter().map(|e| e.0).collect::<Vec<_>>(),
        ["construct", "poll", "poll", "drop"]
    );
    assert_ne!(events[0].1, events[1].1);
    assert_ne!(events[1].1, events[2].1);
    assert_ne!(events[0].1, events[3].1);
    assert_ne!(events[1].1, events[3].1);
    assert_ne!(events[2].1, events[3].1);
}
#[test]
fn unpolled_cancelled_completed_and_unwinding_destructors_restore_outer() {
    let a = authority(65_536);
    let q = work(&a);
    let outer = q.create_lane_shards();
    let owner = q.create_lane_shards();
    for (polls, panic_poll, panic_drop) in [
        (0, false, false),
        (1, false, false),
        (2, false, false),
        (1, true, false),
        (0, false, true),
    ] {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut future = Box::pin(AttributedFuture::new(owner.clone(), || {
            Probe::new(owner.clone(), events.clone(), panic_poll, panic_drop)
        }));
        outer.run(|| {
            for _ in 0..polls {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    poll(future.as_mut())
                }));
                assert_eq!(result.is_err(), panic_poll);
            }
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(future)));
            assert_eq!(result.is_err(), panic_drop);
            let block = Block::new();
            assert_eq!(block.source(), reference(&outer));
        });
        assert_eq!(events.lock().unwrap().last().unwrap().0, "drop");
        assert_eq!(
            events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| event.0 == "drop")
                .count(),
            1
        );
        assert_eq!(owner.snapshot().tagged_bytes, 0);
        assert_eq!(owner.snapshot().slot_pins, 0);
    }
    outer.run(|| {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            AttributedFuture::<std::future::Ready<()>>::new(owner.clone(), || {
                let block = Block::new();
                assert_eq!(block.source(), reference(&owner));
                panic!("construction unwind")
            })
        }));
        assert!(result.is_err());
        let block = Block::new();
        assert_eq!(block.source(), reference(&outer));
    });
    assert_eq!(owner.snapshot().tagged_bytes, 0);
    assert_eq!(binding::pending_bytes(), 0);
}
