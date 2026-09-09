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

//! The floor, idle reclamation and policy updates
//! (MEM-1 acceptance A13, and the A09 competition rule for idle capacity).
//!
//! Capacity comes in three kinds of revocability, and the difference decides
//! who may take it back: a floor that ordinary competition may not touch, an
//! amount an owner already holds and must cooperate to give up, and idle
//! account slack an arbitrator may simply take. Lowering a policy erases none
//! of them; it reports the excess and stops growth.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use novarocks_memory::account::TopUpPolicy;
use novarocks_memory::authority::{AuthorityConfig, MemoryAuthority};
use novarocks_memory::ids::{AccountKind, ExternalRef};
use novarocks_memory::policy::LimitDimension;

fn authority(capacity: u64) -> MemoryAuthority {
    let mut config = AuthorityConfig::new(capacity * 4, capacity, capacity);
    config.top_up = TopUpPolicy::uniform(1);
    MemoryAuthority::new(config).expect("authority configuration is valid")
}

#[test]
fn idle_slack_is_reclaimable_but_issued_capacity_is_not() {
    let authority = authority(1000);
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work");

    let grant = work.request_grant(300).expect("grant");
    let charge = grant.fulfil(100).expect("fulfil part");
    // 100 live, 200 still granted, no idle slack at all.
    let outcome = work.shrink_idle(1000);
    assert_eq!(outcome.reclaimed_bytes, 0, "nothing is idle: {outcome:?}");
    assert_eq!(outcome.kept_for_grants_bytes, 300);

    // Releasing the live bytes turns them into idle slack, which is now
    // reclaimable without the account's cooperation.
    charge.release();
    let outcome = work.shrink_idle(1000);
    assert_eq!(outcome.reclaimed_bytes, 100, "{outcome:?}");
    assert_eq!(
        outcome.kept_for_grants_bytes, 200,
        "the outstanding grant still holds its share"
    );
    drop(grant);
}

#[test]
fn the_floor_survives_ordinary_competition() {
    let authority = authority(1000);
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work");

    let grant = work.request_grant(300).expect("grant");
    drop(grant);
    assert_eq!(work.local_free_bytes(), 300, "all of it is idle now");

    work.set_floor(200);
    let outcome = work.shrink_idle(1000);
    assert_eq!(
        outcome.reclaimed_bytes, 100,
        "only what is above the floor may go: {outcome:?}"
    );
    assert_eq!(outcome.kept_for_floor_bytes, 200, "{outcome:?}");
    assert_eq!(work.committed_bytes(), 200, "the floor is still held");

    // The floor still serves the work it protects: a request inside it is met
    // from the retained slack.
    let grant = work
        .request_grant(200)
        .expect("the floor is usable capacity");
    assert_eq!(grant.remaining_bytes(), 200);
}

#[test]
fn the_floor_is_not_a_fourth_quantity() {
    // The bytes the floor protects are the same bytes counted in L, F and O.
    // Setting a floor must not add anything to the commitment.
    let authority = authority(1000);
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work");
    let grant = work.request_grant(300).expect("grant");
    let charge = grant.fulfil(300).expect("fulfil");

    let before = work.snapshot();
    work.set_floor(300);
    let after = work.snapshot();

    assert_eq!(after.committed_bytes, before.committed_bytes);
    assert_eq!(after.live_bytes, before.live_bytes);
    assert_eq!(after.granted_bytes, before.granted_bytes);
    assert_eq!(after.floor_bytes, 300);
    assert_eq!(
        after.live_bytes + after.granted_bytes + after.bounded_bytes,
        after.committed_bytes,
        "the floor is a retention bound, not an extra charge: {after:?}"
    );
    charge.release();
}

#[test]
fn idle_reclamation_and_a_new_request_have_exactly_one_winner() {
    const THREADS: usize = 8;
    const ROUNDS: usize = 500;
    let authority = Arc::new(authority(64 * 1024));
    let work = Arc::new(
        authority
            .create_account(AccountKind::Work, ExternalRef::from_u128(1))
            .expect("work"),
    );
    let barrier = Arc::new(Barrier::new(THREADS * 2));
    let granted_total = Arc::new(AtomicU64::new(0));
    let reclaimed_total = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for _ in 0..THREADS {
        // Requesters.
        let work = Arc::clone(&work);
        let barrier = Arc::clone(&barrier);
        let granted_total = Arc::clone(&granted_total);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for _ in 0..ROUNDS {
                if let Ok(grant) = work.request_grant(64) {
                    granted_total.fetch_add(64, Ordering::Relaxed);
                    drop(grant);
                }
            }
        }));
    }
    for _ in 0..THREADS {
        // Arbitrators taking idle slack back.
        let work = Arc::clone(&work);
        let barrier = Arc::clone(&barrier);
        let reclaimed_total = Arc::clone(&reclaimed_total);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for _ in 0..ROUNDS {
                let outcome = work.shrink_idle(64);
                reclaimed_total.fetch_add(outcome.reclaimed_bytes, Ordering::Relaxed);
            }
        }));
    }
    for handle in handles {
        handle.join().expect("thread");
    }

    // Both sides made progress, and the account's own decomposition adds up
    // once everything is quiet: no bytes were created or lost by the race.
    assert!(granted_total.load(Ordering::Relaxed) > 0);
    assert!(reclaimed_total.load(Ordering::Relaxed) > 0);
    let snapshot = work.snapshot();
    assert!(snapshot.is_internally_consistent(), "{snapshot:?}");
    assert_eq!(snapshot.live_bytes, 0, "{snapshot:?}");
    assert!(authority.snapshot().honours_capacity_bound());

    // Everything is returnable: nothing leaked into a state no one can free.
    let remaining = work.committed_bytes();
    let outcome = work.shrink_idle(remaining);
    assert_eq!(outcome.reclaimed_bytes, remaining, "{outcome:?}");
    assert_eq!(authority.snapshot().root.committed_bytes, 0);
}

#[test]
fn lowering_a_policy_keeps_commitments_and_reports_the_excess() {
    let authority = authority(1000);
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work");
    let grant = work.request_grant(300).expect("grant");
    let charge = grant.fulfil(300).expect("fulfil");

    let outcome = work.install_policy(100, LimitDimension::Work);
    assert!(outcome.is_over_policy(), "{outcome:?}");
    assert_eq!(outcome.committed_bytes, 300);
    assert_eq!(outcome.excess_bytes, 200);
    assert!(outcome.growth_frozen);

    let snapshot = work.snapshot();
    assert_eq!(
        snapshot.live_bytes, 300,
        "a lowered policy erases nothing: {snapshot:?}"
    );
    assert_eq!(snapshot.excess_bytes, 200, "{snapshot:?}");
    assert_eq!(snapshot.policy_limit_bytes, Some(100));
    assert_eq!(
        snapshot.policy_remaining_bytes(),
        Some(0),
        "the remainder saturates rather than going negative"
    );

    // Growth stops while the account is over policy.
    let refused = work.request_grant(1).expect_err("growth is frozen");
    assert!(
        matches!(
            refused,
            novarocks_memory::CapacityError::FrozenByExcess { .. }
        ),
        "{refused:?}"
    );

    // Releasing back inside the policy reopens growth.
    charge.release();
    let outcome = work.shrink_idle(300);
    assert!(outcome.reclaimed_bytes > 0, "{outcome:?}");
    assert_eq!(work.snapshot().excess_bytes, 0);
    assert!(!work.is_closed_to_growth());
    work.request_grant(1).expect("growth resumed");
}

#[test]
fn a_policy_version_advances_and_is_reported_with_every_reading() {
    let authority = authority(1000);
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work");
    assert_eq!(work.snapshot().policy_version.get(), 0);

    let first = work.install_policy(500, LimitDimension::Work);
    assert_eq!(first.version.get(), 1);
    assert_eq!(work.snapshot().policy_version.get(), 1);

    let second = work.install_policy(400, LimitDimension::ResourceGroup);
    assert_eq!(second.version.get(), 2);
    assert!(second.version > first.version);

    // A refusal reports the version it was judged under, so a consumer can
    // tell a stale decision from a policy change.
    let refused = work.request_grant(500).expect_err("over the new limit");
    match refused {
        novarocks_memory::CapacityError::Denied { version, .. } => {
            assert_eq!(version.get(), 2);
        }
        other => panic!("expected a denial carrying its version, got {other:?}"),
    }
}

#[test]
fn an_ancestor_policy_bounds_a_child_that_has_none() {
    let authority = authority(10_000);
    let group = authority
        .create_account(AccountKind::ResourceGroup, ExternalRef::from_u128(1))
        .expect("group");
    group.install_policy(500, LimitDimension::ResourceGroup);
    let work = group
        .create_child(AccountKind::Work, ExternalRef::from_u128(2))
        .expect("work");

    let grant = work.request_grant(500).expect("up to the group limit");
    assert_eq!(grant.remaining_bytes(), 500);

    let refused = work
        .request_grant(1)
        .expect_err("the group limit bounds the child");
    match refused {
        novarocks_memory::CapacityError::Denied {
            scope, constraint, ..
        } => {
            assert_eq!(
                scope,
                group.id(),
                "the refusal names the group that refused"
            );
            assert_eq!(constraint, novarocks_memory::ConstraintKind::AccountPolicy);
        }
        other => panic!("expected an account-policy denial, got {other:?}"),
    }
}

#[test]
fn unbudgeted_live_allocation_is_absorbed_rather_than_hidden() {
    // A library that has already allocated cannot be asked to stop. The
    // account takes the bytes, reports the excess and freezes growth.
    let authority = authority(1000);
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work");
    work.install_policy(100, LimitDimension::Work);
    let grant = work.request_grant(100).expect("grant the policy limit");
    let charge = grant.fulfil(100).expect("fulfil");

    let unbudgeted = work.absorb_unbudgeted_live(40);
    let snapshot = work.snapshot();
    assert_eq!(
        snapshot.live_bytes, 140,
        "the bytes exist, so they are charged: {snapshot:?}"
    );
    assert_eq!(snapshot.excess_bytes, 40, "{snapshot:?}");
    assert!(snapshot.growth_frozen, "{snapshot:?}");
    assert!(
        work.request_grant(1).is_err(),
        "growth stops until an arbitrator resolves the excess"
    );

    // The root sees the same bytes as a commitment it never authorised.
    assert_eq!(authority.snapshot().root.committed_bytes, 140);

    unbudgeted.release();
    charge.release();
    assert_eq!(work.snapshot().live_bytes, 0);
}
