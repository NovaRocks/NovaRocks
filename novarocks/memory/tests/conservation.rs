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

//! Capacity conservation (MEM-1 acceptance A01).
//!
//! Concurrent works and sub-grants stay inside the local authorisation bound,
//! parents and projections do not charge the same bytes twice, and capacity
//! handed down and not returned is never issued to anyone else.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use novarocks_memory::account::TopUpPolicy;
use novarocks_memory::authority::{AuthorityConfig, MemoryAuthority};
use novarocks_memory::ids::{AccountKind, ExternalRef};

fn authority(capacity: u64, step: u64) -> MemoryAuthority {
    let mut config = AuthorityConfig::new(capacity * 4, capacity, capacity);
    config.top_up = TopUpPolicy::uniform(step);
    MemoryAuthority::new(config).expect("authority configuration is valid")
}

#[test]
fn the_worked_example_from_the_specification_holds() {
    // A process hands 100 units down; the child allocates 60. The process must
    // report L=60, F=40, C=100 -- not L=0 just because the process itself
    // allocated nothing.
    let authority = authority(1000, 100);
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work account");

    let grant = work.request_grant(100).expect("grant 100");
    assert_eq!(work.committed_bytes(), 100);

    let charge = grant.fulfil(60).expect("fulfil 60");
    let root = authority.snapshot().root;
    assert_eq!(root.live_bytes, 60, "{root:?}");
    assert_eq!(root.granted_bytes, 40, "{root:?}");
    assert_eq!(root.committed_bytes, 100, "{root:?}");
    assert!(root.is_internally_consistent(), "{root:?}");

    // Releasing the backing while keeping the local quota leaves the
    // commitment at 100: the child still holds the capacity.
    let released = charge.release();
    assert_eq!(released, 60);
    let root = authority.snapshot().root;
    assert_eq!(root.live_bytes, 0, "{root:?}");
    assert_eq!(root.granted_bytes, 100, "{root:?}");
    assert_eq!(root.committed_bytes, 100, "{root:?}");

    // Only once the child gives the quota back can the process re-issue it.
    drop(grant);
    assert_eq!(work.committed_bytes(), 100, "the account still holds it");
    let reclaimed = work.shrink_idle(100);
    assert_eq!(reclaimed.reclaimed_bytes, 100);
    assert_eq!(authority.snapshot().root.committed_bytes, 0);
}

#[test]
fn a_parent_never_charges_a_child_s_bytes_twice() {
    let authority = authority(1000, 10);
    let group = authority
        .create_account(AccountKind::ResourceGroup, ExternalRef::from_u128(1))
        .expect("group");
    let work = group
        .create_child(AccountKind::Work, ExternalRef::from_u128(2))
        .expect("work");
    let task = work
        .create_child(AccountKind::Task, ExternalRef::from_u128(3))
        .expect("task");

    let grant = task.request_grant(50).expect("grant");
    let _charge = grant.fulfil(50).expect("fulfil");

    // Every level reports the same 50 bytes exactly once.
    for handle in [&group, &work, &task] {
        let snapshot = handle.snapshot();
        assert_eq!(snapshot.live_bytes, 50, "{snapshot:?}");
        assert_eq!(snapshot.committed_bytes, 50, "{snapshot:?}");
    }
    let root = authority.snapshot().root;
    assert_eq!(root.live_bytes, 50, "{root:?}");
    assert_eq!(root.committed_bytes, 50, "{root:?}");
}

#[test]
fn the_capacity_bound_holds_and_refusals_name_the_constraint() {
    let authority = authority(100, 10);
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work");

    let held = work.request_grant(100).expect("the whole capacity");
    assert_eq!(authority.snapshot().root.committed_bytes, 100);
    assert!(authority.snapshot().honours_capacity_bound());

    let refused = work.request_grant(1).expect_err("nothing is left to grant");
    match refused {
        novarocks_memory::CapacityError::Denied {
            constraint,
            requested,
            ..
        } => {
            assert_eq!(
                constraint,
                novarocks_memory::ConstraintKind::ProcessCapacity
            );
            assert_eq!(requested, 1);
        }
        other => panic!("expected a process-capacity denial, got {other:?}"),
    }
    drop(held);
}

#[test]
fn concurrent_works_never_oversubscribe_the_capacity_bound() {
    const THREADS: usize = 16;
    const ROUNDS: usize = 2_000;
    const CHUNK: u64 = 64;
    // Deliberately tight: one quantised top-up is 256 bytes and the whole
    // authority holds 1024, so at most four accounts can hold slack at once
    // and the other twelve must be refused. Workers return their idle slack
    // periodically, so capacity really circulates through the root instead of
    // being parked by whoever asked first.
    let authority = Arc::new(authority(1024, 256));
    let sampling = Arc::new(AtomicBool::new(true));
    let barrier = Arc::new(Barrier::new(THREADS + 1));

    let sampler = {
        let authority = Arc::clone(&authority);
        let sampling = Arc::clone(&sampling);
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            let mut samples = 0u64;
            while sampling.load(Ordering::Acquire) {
                let snapshot = authority.snapshot();
                assert!(
                    snapshot.honours_capacity_bound(),
                    "capacity bound broken: {snapshot:?}"
                );
                samples += 1;
            }
            samples
        })
    };

    // A second barrier makes the contention structural instead of dependent
    // on timing: every worker holds its first reservation until all of them
    // have tried, so 16 accounts each needing a 256-byte top-up cannot all fit
    // in 1024 bytes and some must be refused on every run.
    let holding = Arc::new(Barrier::new(THREADS));
    let first_round_refusals = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let workers: Vec<_> = (0..THREADS)
        .map(|index| {
            let authority = Arc::clone(&authority);
            let barrier = Arc::clone(&barrier);
            let holding = Arc::clone(&holding);
            let first_round_refusals = Arc::clone(&first_round_refusals);
            thread::spawn(move || {
                let work = authority
                    .create_account(AccountKind::Work, ExternalRef::from_u128(index as u128 + 1))
                    .expect("work account");
                barrier.wait();
                let mut granted = 0u64;
                let mut refused = 0u64;

                // Round one: everyone attempts and keeps what it got.
                let first = work.request_grant(CHUNK);
                match &first {
                    Ok(_) => granted += 1,
                    Err(_) => {
                        refused += 1;
                        first_round_refusals.fetch_add(1, Ordering::Relaxed);
                    }
                }
                holding.wait();
                drop(first);

                for round in 0..ROUNDS {
                    match work.request_grant(CHUNK) {
                        Ok(grant) => {
                            granted += 1;
                            if round % 3 == 0 {
                                let charge = grant.fulfil(CHUNK).expect("fulfil within remainder");
                                charge.release();
                            }
                        }
                        Err(_) => refused += 1,
                    }
                    if round % 8 == 7 {
                        // Give idle slack back so the root is genuinely
                        // contended rather than parked by the first arrivals.
                        work.shrink_idle(CHUNK * 8);
                    }
                }
                work.shrink_idle(u64::MAX / 2);
                (granted, refused)
            })
        })
        .collect();

    let mut total_granted = 0u64;
    let mut total_refused = 0u64;
    for worker in workers {
        let (granted, refused) = worker.join().expect("worker");
        total_granted += granted;
        total_refused += refused;
    }
    sampling.store(false, Ordering::Release);
    let samples = sampler.join().expect("sampler");

    assert!(samples > 0, "the sampler observed nothing");
    assert_eq!(
        total_granted + total_refused,
        (THREADS * ROUNDS) as u64 + THREADS as u64
    );
    assert!(
        first_round_refusals.load(Ordering::Relaxed) > 0,
        "the bound is tight by construction: 16 accounts cannot all hold a          256-byte top-up inside 1024 bytes"
    );
    assert!(total_refused >= first_round_refusals.load(Ordering::Relaxed));

    // Every grant was dropped, so the whole capacity is returnable again.
    let snapshot = authority.snapshot();
    assert!(snapshot.honours_capacity_bound(), "{snapshot:?}");
}

#[test]
fn sub_grants_do_not_widen_the_total_issued_capacity() {
    let authority = authority(1000, 10);
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work");

    let parent_grant = work.request_grant(100).expect("grant");
    let committed_before = authority.snapshot().root.committed_bytes;

    let child_grant = parent_grant.split(40).expect("split");
    assert_eq!(parent_grant.remaining_bytes(), 60);
    assert_eq!(child_grant.remaining_bytes(), 40);
    assert_eq!(
        authority.snapshot().root.committed_bytes,
        committed_before,
        "splitting a grant does not create capacity"
    );

    let charge = child_grant.fulfil(40).expect("fulfil the sub-grant");
    let root = authority.snapshot().root;
    assert_eq!(root.live_bytes, 40, "{root:?}");
    assert_eq!(root.granted_bytes, 60, "{root:?}");
    assert_eq!(root.committed_bytes, 100, "{root:?}");
    charge.release();
}

#[test]
fn a_returned_grant_frees_capacity_for_a_different_account() {
    let authority = authority(100, 100);
    let first = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("first");
    let second = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(2))
        .expect("second");

    let grant = first.request_grant(100).expect("the whole capacity");
    assert!(second.request_grant(1).is_err(), "nothing is left");

    // Returning the grant is not enough: the account still holds the quota.
    drop(grant);
    assert!(
        second.request_grant(1).is_err(),
        "the first account still holds the capacity it was handed"
    );

    // Only a confirmed return puts it back in circulation.
    let outcome = first.shrink_idle(100);
    assert_eq!(outcome.reclaimed_bytes, 100);
    let grant = second.request_grant(100).expect("now it is available");
    assert_eq!(grant.remaining_bytes(), 100);
}

#[test]
fn local_slack_keeps_ordinary_requests_off_the_root() {
    // One top-up covers many small requests: the root's commitment moves once,
    // not once per request. This is the property that keeps the root off the
    // hot path.
    let authority = authority(1_000_000, 4096);
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work");

    let first = work.request_grant(8).expect("first request");
    let after_first = authority.snapshot().root.committed_bytes;
    assert_eq!(after_first, 4096, "the top-up is quantised");

    let mut grants = vec![first];
    for _ in 0..500 {
        grants.push(work.request_grant(8).expect("served from local slack"));
    }
    assert_eq!(
        authority.snapshot().root.committed_bytes,
        4096,
        "504 requests of 8 bytes fit in one 4096-byte top-up"
    );
    drop(grants);
    assert_eq!(
        work.local_free_bytes(),
        4096,
        "returned capacity stays as local slack"
    );
}
