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

mod common;
use common::*;
use novarocks_memory::*;

fn sweep(a: &MemoryAuthority) {
    a.request_maintenance(MaintenanceReason::ExplicitLocalReclaim);
    while !a.maintain(64).complete {}
}

#[test]
fn origin_survives_origin_thread_and_execution_account_exit() {
    let a = authority(65_536);
    let baseline = a.root().committed_bytes();
    let q = work(&a);
    let old_id = q.id();
    let producer = q.clone();
    let origin = std::thread::spawn(move || {
        let domain = producer.create_domain(512).unwrap();
        let mut scope = domain.activate(512, 0).unwrap();
        let origin = scope.record_allocation(512);
        scope.finish();
        // Domain, account and thread-local scope handles all leave this thread.
        origin
    })
    .join()
    .unwrap();
    q.retire(&exited()).unwrap();
    drop(q);
    sweep(&a);
    assert_eq!(a.live_accounts(), 1);
    assert_eq!(
        a.pressure_projection().residual_committed,
        512 + OWNER_METADATA_BYTES
    );

    let replacement = work(&a);
    let replacement_domain = replacement.create_domain(256).unwrap();
    let mut scope = replacement_domain.activate(256, 0).unwrap();
    let replacement_origin = scope.record_allocation(256);
    scope.finish();
    assert_ne!(replacement.id(), old_id);
    // SAFETY: both allocations are outstanding; immutable origin identity is
    // diagnostic only and is never read after its matching free.
    assert_eq!(unsafe { origin.account_id() }, old_id);
    assert_eq!(unsafe { replacement_origin.account_id() }, replacement.id());
    std::thread::spawn(move || free(origin, 512))
        .join()
        .unwrap();
    sweep(&a);
    assert_eq!(a.pressure_projection().residual_committed, 0);
    assert_eq!(replacement_domain.snapshot().live, 256);

    free(replacement_origin, 256);
    replacement.retire(&exited()).unwrap();
    drop(replacement_domain);
    drop(replacement);
    sweep(&a);
    assert_eq!(a.root().committed_bytes(), baseline);
}

#[test]
fn zero_byte_allocation_keeps_origin_record_until_its_matching_free() {
    let a = authority(16_384);
    let q = work(&a);
    let domain = q.create_domain(0).unwrap();
    let mut scope = domain.activate(0, 0).unwrap();
    let origin = scope.record_allocation(0);
    scope.finish();
    q.retire(&exited()).unwrap();
    drop(domain);
    drop(q);
    sweep(&a);
    assert_eq!(
        a.pressure_projection().residual_metadata,
        OWNER_METADATA_BYTES
    );
    free(origin, 0);
    sweep(&a);
    assert_eq!(a.pressure_projection().residual_metadata, 0);
}

#[test]
fn final_free_does_not_reclaim_record_while_an_external_domain_handle_exists() {
    let a = authority(16_384);
    let q = work(&a);
    let domain = q.create_domain(64).unwrap();
    let mut scope = domain.activate(64, 0).unwrap();
    let origin = scope.record_allocation(64);
    scope.finish();
    q.retire(&exited()).unwrap();
    drop(q);
    free(origin, 64);
    sweep(&a);
    assert_eq!(domain.snapshot().live, 0);
    assert_eq!(
        a.pressure_projection().residual_metadata,
        OWNER_METADATA_BYTES
    );
    drop(domain);
    sweep(&a);
    assert_eq!(a.pressure_projection().residual_metadata, 0);
}

#[test]
fn authority_shutdown_preserves_tokens_until_real_last_release() {
    for bytes in [0, 600] {
        let a = authority(16_384);
        let account = work(&a);
        let lane = account.create_domain(bytes).unwrap();
        let mut scope = lane.activate(bytes, 0).unwrap();
        let token = scope.record_allocation(bytes);
        scope.finish();
        let reference = token.reference();
        drop(lane);
        drop(account);
        drop(a);
        let store = novarocks_memory::lane::global_store();
        let snapshot = store
            .snapshot_ref(reference)
            .expect("late allocation stays accessible");
        assert!(snapshot.is_draining());
        assert_eq!(snapshot.outstanding, 1);
        store.reclaim(usize::MAX);
        assert!(store.snapshot_ref(reference).is_some());
        free(token, bytes);
        store.reclaim(usize::MAX);
        assert!(store.snapshot_ref(reference).is_none());
    }
}

#[test]
fn retired_funding_domain_releases_its_record_after_last_free() {
    let a = authority(16_384);
    let account = work(&a);
    let domain = account.create_domain(600).unwrap();
    let mut scope = domain.activate(600, 0).unwrap();
    let token = scope.record_allocation(600);
    scope.finish();
    let reference = token.reference();
    domain.stop_producing().unwrap();
    drop(domain);
    a.maintain(usize::MAX);
    assert!(
        novarocks_memory::lane::global_store()
            .snapshot_ref(reference)
            .is_some()
    );
    free(token, 600);
    a.maintain(usize::MAX);
    assert!(
        novarocks_memory::lane::global_store()
            .snapshot_ref(reference)
            .is_none()
    );
    // The old identity no longer resolves after reuse, even with a live successor.
    let successor = account.create_domain(1).unwrap();
    assert_ne!(successor.lane().reference(), reference);
    assert!(
        novarocks_memory::lane::global_store()
            .snapshot_ref(reference)
            .is_none()
    );
}
