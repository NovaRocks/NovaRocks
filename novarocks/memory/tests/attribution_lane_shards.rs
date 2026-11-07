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
    reason = "Integration tests own and join the threads that exercise memory attribution; see ADR-0218."
)]
#![cfg(not(loom))]
mod common;
use common::*;
use novarocks_memory::{
    attribution::{AttributingAllocator, binding},
    lane::{CoverageError, LANE_SHARDS, ProductionState},
    *,
};
use std::alloc::{GlobalAlloc, Layout, System};
struct Block(std::ptr::NonNull<u8>);
// SAFETY: the unique allocation moves without concurrent access.
unsafe impl Send for Block {}
impl Block {
    fn source(&self) -> lane::RecordRef {
        unsafe { lane::RecordRef::read(self.0.as_ptr().add(600)) }
    }
}
impl Drop for Block {
    fn drop(&mut self) {
        unsafe {
            AttributingAllocator::new(System)
                .dealloc(self.0.as_ptr(), Layout::from_size_align(600, 8).unwrap())
        };
    }
}
#[test]
fn thread_shards_are_stable_distinct_members_and_stop_as_one_group() {
    let a = authority(65_536);
    let q = work(&a);
    let before = a.pressure_projection();
    let owner = q.create_lane_shards();
    assert_eq!(owner.coverage(), Ok(()));
    assert_eq!(a.pressure_projection(), before);
    let results: Vec<_> = (0..LANE_SHARDS)
        .map(|_| {
            let owner = owner.clone();
            std::thread::spawn(move || {
                let expected = reference(&owner);
                for _ in 0..3 {
                    owner.run(|| {
                        let allocator = AttributingAllocator::new(System);
                        let layout = Layout::from_size_align(600, 8).unwrap();
                        unsafe {
                            let pointer = allocator.alloc(layout);
                            assert!(!pointer.is_null());
                            assert_eq!(lane::RecordRef::read(pointer.add(600)), expected);
                            allocator.dealloc(pointer, layout);
                        }
                    });
                    assert_eq!(reference(&owner), expected);
                }
                let block = owner.run(|| {
                    Block(
                        std::ptr::NonNull::new(unsafe {
                            AttributingAllocator::new(System)
                                .alloc(Layout::from_size_align(600, 8).unwrap())
                        })
                        .unwrap(),
                    )
                });
                assert_eq!(block.source(), expected);
                block
            })
        })
        .collect();
    let blocks: Vec<_> = results.into_iter().map(|t| t.join().unwrap()).collect();
    let mut references: Vec<_> = blocks.iter().map(Block::source).collect();
    let held = owner.snapshot();
    assert_eq!(held.tagged_bytes, (608 * LANE_SHARDS) as i128);
    assert_eq!(held.outstanding, LANE_SHARDS as i128);
    assert_eq!(held.slot_pins, 0);
    references.sort_by_key(|r| r.index);
    references.dedup();
    assert_eq!(references.len(), LANE_SHARDS);
    assert_eq!(
        owner.snapshot().records.iter().flatten().count(),
        LANE_SHARDS
    );
    assert!(
        owner
            .snapshot()
            .records
            .iter()
            .flatten()
            .all(|s| s.record.origin == q.id().get())
    );
    // Sealing an active group must seal every shard before returning refusal.
    owner.run(|| assert_eq!(owner.stop_producing(), Err(CoverageError::ScopeRefused)));
    assert!(
        owner
            .snapshot()
            .records
            .iter()
            .flatten()
            .all(|s| s.production == ProductionState::Sealed)
    );
    owner.stop_producing().unwrap();
    drop(blocks);
    let snapshot = owner.snapshot();
    assert_eq!(
        (
            snapshot.tagged_bytes,
            snapshot.r1_small_bytes,
            snapshot.outstanding,
            snapshot.slot_pins
        ),
        (0, 0, 0, 0)
    );
    assert!(
        snapshot
            .records
            .iter()
            .flatten()
            .all(|s| s.production == ProductionState::Stopped)
    );
    assert_eq!(q.retire(&exited()).unwrap().visited_members, LANE_SHARDS);
    assert_eq!(binding::pending_bytes(), 0);
    let registry = a.observation_metadata_bytes();
    let unavailable = q.create_lane_shards();
    assert_eq!(unavailable.coverage(), Err(CoverageGap::AccountClosed));
    assert_eq!(a.observation_metadata_bytes(), registry);
    assert!(unavailable.snapshot().records.iter().all(Option::is_none));
    unavailable.stop_producing().unwrap();
}
