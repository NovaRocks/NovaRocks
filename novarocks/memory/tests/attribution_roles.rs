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
#![cfg(not(loom))]
mod common;
use novarocks_memory::attribution::{
    AttributingAllocator, ThreadRole, ThreadRoleError, binding, freeze_thread_roles,
    readout::AttributionSnapshot,
    register_thread_role,
    scope::{OwnerEntryObservation, OwnerStepObservation},
    set_current_thread_role, thread_role_labels,
};
use novarocks_memory::lane::{RecordRef, UNATTRIBUTED_RECORDS, UNATTRIBUTED_SHARDS, global_store};
use std::alloc::{GlobalAlloc, Layout, System};

static ALLOCATOR: AttributingAllocator<System> = AttributingAllocator::new(System);

struct Block(*mut u8, Layout);
// SAFETY: ownership of the live allocation moves with this wrapper; its token
// identifies a process-lifetime record independently of the freeing thread.
unsafe impl Send for Block {}
impl Block {
    fn allocate(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 8).unwrap();
        // SAFETY: valid nonzero layout; the matching deallocation is in Drop.
        let ptr = unsafe { ALLOCATOR.alloc(layout) };
        assert!(!ptr.is_null());
        Self(ptr, layout)
    }
    fn reference(&self) -> RecordRef {
        // SAFETY: this live tagged allocation owns the initialized tail token.
        unsafe { RecordRef::read(self.0.add(self.1.size())) }
    }
    fn release(self) {
        drop(self);
    }
}
impl Drop for Block {
    fn drop(&mut self) {
        // SAFETY: exact live allocation and original user layout, freed once.
        unsafe { ALLOCATOR.dealloc(self.0, self.1) };
    }
}
fn role_of(reference: RecordRef) -> usize {
    assert_eq!(reference.generation, 0);
    assert!(reference.index < UNATTRIBUTED_RECORDS);
    reference.index as usize / UNATTRIBUTED_SHARDS as usize
}
fn sample() -> AttributionSnapshot {
    binding::flush_current();
    AttributionSnapshot::sample(global_store(), ALLOCATOR.band_snapshot())
}

// One test owns this process's registration/freeze lifecycle, avoiding a
// process-global reset API and test-order dependence. The destructor-free,
// const-initialized TLS cannot be made unavailable through the public API on
// this platform; no synthetic TLS-unavailable result is claimed here.
#[test]
fn registered_roles_route_unbound_and_shadowed_allocations_and_release_to_the_original_role() {
    assert_eq!(thread_role_labels()[0], Some("unknown"));
    assert_eq!(register_thread_role(""), Err(ThreadRoleError::EmptyName));
    assert_eq!(
        register_thread_role("unknown"),
        Err(ThreadRoleError::DuplicateName)
    );
    let driver = register_thread_role("driver").unwrap();
    assert_eq!(
        register_thread_role("driver"),
        Err(ThreadRoleError::DuplicateName)
    );
    let scan = register_thread_role("scan").unwrap();
    freeze_thread_roles();
    freeze_thread_roles();
    let labels = thread_role_labels();
    assert_eq!(labels[driver.index()], Some("driver"));
    assert_eq!(labels[scan.index()], Some("scan"));
    assert_eq!(register_thread_role("late"), Err(ThreadRoleError::Frozen));

    // A new, unmarked thread remains unknown even after other roles register.
    let unknown = std::thread::spawn(|| Block::allocate(512)).join().unwrap();
    assert_eq!(role_of(unknown.reference()), ThreadRole::UNKNOWN.index());
    set_current_thread_role(driver).unwrap();
    let unbound = Block::allocate(1024);
    assert_eq!(role_of(unbound.reference()), driver.index());

    let authority = common::authority(64 * 1024);
    let outer_account = common::work(&authority);
    let closed_account = common::work(&authority);
    closed_account.retire(&common::exited()).unwrap();
    let outer = outer_account.create_lane_shards();
    assert_eq!(outer.coverage(), Ok(()));
    let uncovered = closed_account.create_lane_shards();
    assert!(uncovered.coverage().is_err());
    let mut observation = OwnerStepObservation::default();
    let shadowed = outer.run(|| uncovered.run_observed(&mut observation, || Block::allocate(2048)));
    assert_eq!(observation.entry(), OwnerEntryObservation::UncoveredShadow);
    assert_eq!(role_of(shadowed.reference()), driver.index());
    let explicit_owner = uncovered.explicit_owner();
    let explicitly_shadowed = outer.run(|| {
        // A covered outer ambient must not take ownership from an uncovered
        // explicit owner. Its stable explicit shadow is installed by the helper.
        explicit_owner
            .allocate_with(Layout::from_size_align(512, 8).unwrap(), || {
                Ok::<_, ()>(Block::allocate(512))
            })
            .unwrap()
    });
    assert_eq!(role_of(explicitly_shadowed.reference()), driver.index());

    let before_release = sample();
    assert_eq!(before_release.unattributed_by_role[0], 520);
    assert_eq!(
        before_release.unattributed_by_role[driver.index()],
        1032 + 2056 + 520
    );
    assert_eq!(before_release.unattributed_by_role[scan.index()], 0);
    assert_eq!(
        before_release.unattributed_tagged_bytes,
        520 + 1032 + 2056 + 520
    );
    assert_eq!(
        before_release.unattributed_by_role.iter().sum::<i128>(),
        before_release.unattributed_tagged_bytes
    );
    assert_eq!(before_release.thread_roles, labels);
    assert_eq!(
        before_release.records[3].iter().sum::<u64>(),
        u64::from(UNATTRIBUTED_RECORDS)
    );

    // Both a new role and a different thread must leave each original role's
    // debit/credit pair symmetric; release follows the stored record token.
    set_current_thread_role(scan).unwrap();
    drop(unknown);
    drop(shadowed);
    std::thread::spawn(move || {
        set_current_thread_role(scan).unwrap();
        unbound.release();
    })
    .join()
    .unwrap();
    drop(explicitly_shadowed);
    let after_release = sample();
    assert_eq!(
        after_release.unattributed_by_role,
        [0; novarocks_memory::lane::THREAD_ROLE_CAPACITY as usize]
    );
    assert_eq!(after_release.unattributed_tagged_bytes, 0);
    assert_eq!(after_release.tagged_reconciliation_bytes, 0);
    assert_eq!(after_release.faults.orphan_events, 0);
    assert_eq!(after_release.faults.pinned_slots, 0);
    assert_eq!(ALLOCATOR.snapshot().live_bytes, 0);
    set_current_thread_role(ThreadRole::UNKNOWN).unwrap();
}
