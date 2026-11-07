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
    reason = "Integration tests own and join their threads and runtime tasks; see ADR-0218."
)]

//! Real production capacity. Kept out of Miri because this deliberately holds
//! every observation record and drives sixteen persistent allocation threads.
#![cfg(not(loom))]
mod common;
use common::*;
use novarocks_memory::{
    attribution::{AttributingAllocator, binding},
    lane::{
        LANE_SHARDS, RecordRef, RecordRegion, global_store, shards::OBSERVATION_CATCH_UP_BUDGET,
    },
    *,
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    ptr::NonNull,
    sync::mpsc,
};
struct Block {
    pointer: NonNull<u8>,
    source: RecordRef,
    small: bool,
}
// SAFETY: a block transfers one unique allocation and no concurrent access.
unsafe impl Send for Block {}
impl Drop for Block {
    fn drop(&mut self) {
        let bytes = if self.small { 1 } else { 600 };
        // SAFETY: the exact allocation is still outstanding. Release the real
        // block first; for R1 small facts this saved identity is protected by
        // that allocation, not by an owner handle or copied token alone.
        unsafe {
            AttributingAllocator::new(System).dealloc(
                self.pointer.as_ptr(),
                Layout::from_size_align(bytes, 8).unwrap(),
            );
            if self.small {
                binding::publish_small(self.source, -1, -1);
            }
        }
    }
}
struct Command {
    owner: LaneShards,
    small: bool,
}
#[test]
fn full_observation_region_isolates_funding_and_recovers_after_all_retention_kinds_exit() {
    let a = authority(1 << 20);
    let store = global_store();
    let initial_funding = store.region_snapshot(RecordRegion::Funding);
    let capacity = store.region_snapshot(RecordRegion::Observation).capacity as usize;
    assert_eq!(capacity % LANE_SHARDS, 0);
    let (responses, results) = mpsc::channel();
    let mut inputs = Vec::new();
    let mut workers = Vec::new();
    for _ in 0..LANE_SHARDS {
        let (input, requests) = mpsc::channel::<Command>();
        let responses = responses.clone();
        inputs.push(input);
        workers.push(std::thread::spawn(move || {
            while let Ok(Command { owner, small }) = requests.recv() {
                let reference = reference(&owner);
                let bytes = if small { 1 } else { 600 };
                let layout = Layout::from_size_align(bytes, 8).unwrap();
                let allocator = AttributingAllocator::new(System);
                let pointer = if small {
                    owner
                        .explicit_owner()
                        .allocate_with(layout, || {
                            NonNull::new(unsafe { allocator.alloc(layout) }).ok_or(())
                        })
                        .unwrap()
                } else {
                    owner.run(|| NonNull::new(unsafe { allocator.alloc(layout) }).unwrap())
                };
                if !small {
                    assert_eq!(
                        unsafe { RecordRef::read(pointer.as_ptr().add(bytes)) },
                        reference
                    );
                }
                binding::flush_current();
                // No owner handle remains on this worker when its block is
                // received. Small-only retention therefore needs no Arc pin.
                drop(owner);
                responses
                    .send(Block {
                        pointer,
                        source: reference,
                        small,
                    })
                    .unwrap();
            }
        }));
    }
    drop(responses);
    let mut blocks = Vec::new();
    let mut handles = Vec::new();
    let mut tagged_count = 0;
    let mut small_count = 0;
    let mut handle_count = 0;
    for group in 0..capacity / LANE_SHARDS {
        let account = work(&a);
        let owner = account.create_lane_shards();
        assert_eq!(owner.coverage(), Ok(()));
        if group % 3 == 2 {
            handles.push(owner.clone());
            handle_count += LANE_SHARDS;
        } else {
            let small = group % 3 == 1;
            for input in &inputs {
                input
                    .send(Command {
                        owner: owner.clone(),
                        small,
                    })
                    .unwrap();
            }
            let mut group_references = Vec::new();
            for _ in 0..LANE_SHARDS {
                let block = results.recv().unwrap();
                assert!(contains(&owner, block.source));
                group_references.push(block.source);
                blocks.push(block);
            }
            group_references.sort_by_key(|r| r.index);
            group_references.dedup();
            assert_eq!(group_references.len(), LANE_SHARDS);
            if small {
                small_count += LANE_SHARDS;
            } else {
                tagged_count += LANE_SHARDS;
            }
        }
        assert_eq!(
            account.retire(&exited()).unwrap().visited_members,
            LANE_SHARDS
        );
        drop(owner);
        drop(account);
    }
    drop(inputs);
    for worker in workers {
        worker.join().unwrap();
    }
    let occupied = store.region_snapshot(RecordRegion::Observation);
    assert_eq!(occupied.active, 0);
    assert_eq!(occupied.residual_retained, capacity as u64);
    assert_eq!(occupied.residual_tagged_bytes, (tagged_count * 608) as i128);
    assert_eq!(occupied.residual_r1_small_bytes, small_count as i128);
    assert_eq!(tagged_count + small_count + handle_count, capacity);
    assert_eq!(
        store.region_snapshot(RecordRegion::Funding),
        initial_funding
    );
    let account = work(&a);
    let before_exhaustion = store.faults.snapshot().record_exhaustions;
    for _ in 0..3 {
        assert_eq!(
            account.create_lane_shards().coverage(),
            Err(CoverageGap::ObservationExhausted)
        );
    }
    assert!(store.faults.snapshot().record_exhaustions > before_exhaustion);
    // Observation pressure neither consumes a funding record nor changes its
    // availability. Exercise both direct and maintenance-mediated admission.
    let domain = account.create_domain(1).unwrap();
    assert_eq!(
        domain.lane().responsibility_class(),
        lane::ResponsibilityClass::Query
    );
    let RequestOutcome::Granted(granted) = a.request_domain(&account, 1, 1) else {
        panic!("observation exhaustion must not refuse funding")
    };
    assert_eq!(
        store.region_snapshot(RecordRegion::Funding).active,
        initial_funding.active + 2
    );
    granted.stop_producing().unwrap();
    domain.stop_producing().unwrap();
    drop(granted);
    drop(domain);
    drop(handles);
    drop(blocks);
    binding::flush_current();
    // Each failed creation advances at least this fixed catch-up budget. One
    // complete registry pass, plus its retry, is an explicit progress bound.
    let bound = capacity.div_ceil(OBSERVATION_CATCH_UP_BUDGET) + 1;
    let mut recovered = None;
    for _ in 0..bound {
        let owner = account.create_lane_shards();
        if owner.coverage().is_ok() {
            recovered = Some(owner);
            break;
        }
    }
    let recovered = recovered.expect("record release must recover coverage within the scan bound");
    assert_eq!(
        recovered.snapshot().records.iter().flatten().count(),
        LANE_SHARDS
    );
    assert_eq!(recovered.snapshot().outstanding, 0);
    recovered.stop_producing().unwrap();
}
