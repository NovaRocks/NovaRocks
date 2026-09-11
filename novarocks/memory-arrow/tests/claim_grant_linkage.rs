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

//! The link between a claim and the capacity behind it (MEM-1 spec §8.1).
//!
//! Arrow's `reserve` cannot fail, so it cannot be where capacity is decided.
//! A pool is built from a grant the core already issued, and claiming turns
//! that grant's unfulfilled `F` into known live `L`. When a buffer is larger
//! than the remainder the bytes still exist, so they are charged as excess and
//! the account stops growing — the one behaviour that keeps a claim honest
//! without either failing or hiding memory.

use novarocks_memory::account::{AccountHandle, TopUpPolicy};
use novarocks_memory::authority::{AuthorityConfig, MemoryAuthority};
use novarocks_memory::ids::{AccountKind, ExternalRef};
use novarocks_memory::{CapacityError, MemoryEventKind};
use novarocks_memory_arrow::{FulfilmentPool, claim_buffer};

use arrow_buffer::{Buffer, MemoryPool};

fn fixture(capacity: u64) -> (MemoryAuthority, AccountHandle) {
    let mut config = AuthorityConfig::new(capacity * 4, capacity, capacity);
    config.top_up = TopUpPolicy::uniform(1);
    let authority = MemoryAuthority::new(config).expect("valid configuration");
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work account");
    (authority, work)
}

#[test]
fn claiming_converts_granted_capacity_into_live_allocation() {
    let (_authority, work) = fixture(1024 * 1024);
    let pool = FulfilmentPool::new(work.request_grant(4096).expect("grant"));

    let before = work.snapshot();
    assert_eq!(before.granted_bytes, 4096, "{before:?}");
    assert_eq!(before.live_bytes, 0, "{before:?}");
    assert_eq!(before.committed_bytes, 4096, "{before:?}");

    let buffer = Buffer::from_vec(vec![0u8; 1024]);
    let capacity = buffer.capacity() as u64;
    claim_buffer(&buffer, &pool);

    let after = work.snapshot();
    assert_eq!(after.live_bytes, capacity, "{after:?}");
    assert_eq!(after.granted_bytes, 4096 - capacity, "{after:?}");
    assert_eq!(
        after.committed_bytes, 4096,
        "converting F into L does not change the commitment: {after:?}"
    );
    assert_eq!(pool.excess_bytes(), 0);
}

#[test]
fn the_pool_reports_its_grant_as_arrow_expects() {
    let (_authority, work) = fixture(1024 * 1024);
    let pool = FulfilmentPool::new(work.request_grant(8192).expect("grant"));
    assert_eq!(pool.capacity(), 8192);
    assert_eq!(pool.used(), 0);
    assert_eq!(pool.available(), 8192);

    let buffer = Buffer::from_vec(vec![0u8; 2048]);
    let charged = buffer.capacity();
    claim_buffer(&buffer, &pool);

    assert_eq!(pool.capacity(), 8192);
    assert_eq!(pool.used(), charged);
    assert_eq!(pool.available(), 8192 - charged as isize);
}

#[test]
fn a_buffer_larger_than_the_remainder_is_charged_as_excess_and_freezes_growth() {
    let (authority, work) = fixture(1024 * 1024);
    let pool = FulfilmentPool::new(
        work.request_grant(1024)
            .expect("a deliberately small grant"),
    );

    // The allocation already happened; the pool cannot refuse it.
    let buffer = Buffer::from_vec(vec![0u8; 8192]);
    let capacity = buffer.capacity() as u64;
    assert!(capacity > 1024);
    let receipt = claim_buffer(&buffer, &pool);

    assert_eq!(
        receipt.charged_bytes(),
        capacity,
        "the bytes exist, so they are charged in full"
    );
    assert_eq!(pool.excess_bytes(), capacity, "the overshoot is reported");
    assert_eq!(pool.excess_events(), 1);

    let snapshot = work.snapshot();
    assert_eq!(snapshot.live_bytes, capacity, "{snapshot:?}");
    assert_eq!(
        snapshot.unbudgeted_bytes, capacity,
        "the whole allocation was unbudgeted: {snapshot:?}"
    );
    assert!(
        snapshot.frozen_by_unbudgeted,
        "the account stops growing until an arbitrator resolves it: {snapshot:?}"
    );
    assert!(snapshot.growth_frozen, "{snapshot:?}");
    // No bound was exceeded: the process had plenty of capacity, and the
    // account has no policy of its own. Unbudgeted absorption and over-bound
    // excess are separate facts, and conflating them would either freeze
    // healthy work for no reason or let a broken estimate run unchecked.
    assert_eq!(
        snapshot.excess_bytes, 0,
        "nothing exceeded a bound: {snapshot:?}"
    );

    let refused = work.request_grant(1).expect_err("growth is frozen");
    assert!(
        matches!(refused, CapacityError::FrozenByExcess { .. }),
        "{refused:?}"
    );

    // Releasing the bytes does not by itself make the sizing correct, so the
    // freeze needs an arbitrator to lift it.
    let absorbed = work.resume_growth_after_arbitration();
    assert_eq!(absorbed, capacity);
    assert!(!work.is_closed_to_growth());
    work.request_grant(1)
        .expect("growth resumed after arbitration");
    assert_eq!(
        work.snapshot().unbudgeted_bytes,
        capacity,
        "the record of what was absorbed is kept"
    );

    // The overshoot is visible as an event, not only as a counter.
    let events = authority.events().read_from(0);
    assert!(
        events
            .events
            .iter()
            .any(|event| matches!(event.kind, MemoryEventKind::ExcessRecorded { .. })),
        "an excess event is recorded: {events:?}"
    );
    assert!(
        events
            .events
            .iter()
            .any(|event| matches!(event.kind, MemoryEventKind::GrowthFrozen { .. })),
        "a growth-frozen event is recorded: {events:?}"
    );

    // The pool reports that it owes capacity rather than pretending it has
    // some left.
    assert!(pool.available() < 0, "available = {}", pool.available());

    drop(buffer);
    assert_eq!(work.snapshot().live_bytes, 0);
}

#[test]
fn a_bare_account_is_not_a_pool() {
    // There is deliberately no way to build a pool from an account alone:
    // without a grant in hand, every claim would be an unbudgeted allocation.
    // This test states the contract; it is enforced by the type signature of
    // `FulfilmentPool::new`, which accepts only issued capacity.
    let (_authority, work) = fixture(1024 * 1024);
    let grant = work.request_grant(4096).expect("grant");
    let pool = FulfilmentPool::new(grant);
    assert_eq!(pool.account().id(), work.id());
    assert_eq!(pool.issued_bytes(), 4096);
}

#[test]
fn a_growing_reservation_draws_on_the_grant_before_recording_excess() {
    let (_authority, work) = fixture(1024 * 1024);
    let pool = FulfilmentPool::new(work.request_grant(64 * 1024).expect("grant"));

    // Arrow's own resize path: reserve a small amount, then grow it. The
    // growth is settled from the grant while the remainder covers it.
    let mut reservation = pool.reserve(1024);
    assert_eq!(reservation.size(), 1024);
    reservation.resize(4096);
    assert_eq!(reservation.size(), 4096);
    assert_eq!(work.snapshot().live_bytes, 4096);
    assert_eq!(pool.excess_bytes(), 0, "the grant covered the growth");

    // A shrink releases the difference.
    reservation.resize(2048);
    assert_eq!(work.snapshot().live_bytes, 2048);

    drop(reservation);
    assert_eq!(work.snapshot().live_bytes, 0);
}

#[test]
fn growth_beyond_the_grant_is_absorbed_rather_than_lost() {
    let (_authority, work) = fixture(1024 * 1024);
    let pool = FulfilmentPool::new(work.request_grant(2048).expect("a small grant"));

    let mut reservation = pool.reserve(1024);
    // Arrow reallocated to more than the grant can cover. The bytes exist.
    reservation.resize(16 * 1024);
    assert_eq!(reservation.size(), 16 * 1024);
    assert_eq!(
        work.snapshot().live_bytes,
        16 * 1024,
        "the grown allocation is charged in full"
    );
    let snapshot = work.snapshot();
    assert!(
        snapshot.unbudgeted_bytes > 0,
        "the growth beyond the grant is recorded: {snapshot:?}"
    );
    assert!(snapshot.frozen_by_unbudgeted, "{snapshot:?}");

    drop(reservation);
    assert_eq!(work.snapshot().live_bytes, 0);
}
