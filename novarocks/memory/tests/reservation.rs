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

use std::sync::{Arc, Barrier};

use novarocks_memory::account::TopUpPolicy;
use novarocks_memory::authority::{AuthorityConfig, MemoryAuthority};
use novarocks_memory::ids::{AccountKind, ExternalRef};
use novarocks_memory::{CapacityError, Reservation};

fn authority(capacity: u64, quantum: u64) -> MemoryAuthority {
    let mut config = AuthorityConfig::new(capacity * 2, capacity, capacity);
    config.top_up = TopUpPolicy::uniform(quantum);
    MemoryAuthority::new(config).unwrap()
}

#[test]
fn leaf_is_a_real_account_with_live_and_free_capacity() {
    let authority = authority(64, 8);
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let leaf = Reservation::new(&sponsor, ExternalRef::from_u128(2)).unwrap();
    let lease = leaf.try_grow(3).unwrap();
    let snapshot = leaf.snapshot();
    assert_eq!(snapshot.live_bytes, 3);
    assert_eq!(snapshot.free_bytes, 5);
    assert_eq!(snapshot.committed_bytes, 8);
    assert_eq!(sponsor.snapshot().live_bytes, 3);
    assert_eq!(sponsor.snapshot().committed_bytes, 8);
    drop(lease);
    assert_eq!(leaf.snapshot().live_bytes, 0);
    assert!(authority.snapshot().honours_capacity_bound());
}

#[test]
fn slow_growth_commits_its_own_delta_before_exposing_surplus() {
    let authority = authority(16, 8);
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let leaf = Reservation::new(&sponsor, ExternalRef::from_u128(2)).unwrap();
    let lease = leaf.try_grow(9).unwrap();
    assert_eq!(leaf.snapshot().live_bytes, 9);
    assert_eq!(leaf.snapshot().committed_bytes, 16);
    assert!(leaf.try_grow(8).is_err());
    assert_eq!(leaf.snapshot().live_bytes, 9);
    drop(lease);
}

#[test]
fn exact_fallback_reaches_parent_for_leaf_growth() {
    let authority = authority(14, 8);
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let leaf = Reservation::new(&sponsor, ExternalRef::from_u128(2)).unwrap();
    let lease = leaf.try_grow(9).unwrap();
    assert_eq!(leaf.snapshot().committed_bytes, 9);
    drop(lease);
}

#[test]
fn close_refuses_new_live_bytes_but_settles_existing_ones() {
    let authority = authority(64, 8);
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let leaf = Reservation::new(&sponsor, ExternalRef::from_u128(2)).unwrap();
    let lease = leaf.try_grow(3).unwrap();
    let outcome = leaf.close();
    assert_eq!(outcome.reclaimed_bytes, 5);
    assert_eq!(leaf.snapshot().committed_bytes, 3);
    assert!(matches!(
        leaf.try_grow(1),
        Err(CapacityError::Cancelled { .. })
    ));
    drop(lease);
    assert_eq!(leaf.snapshot().committed_bytes, 0);
    assert_eq!(sponsor.snapshot().live_bytes, 0);
}

#[test]
fn lease_outlives_the_reservation_handle_and_releases_each_split_once() {
    let authority = authority(64, 8);
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let leaf = Reservation::new(&sponsor, ExternalRef::from_u128(2)).unwrap();
    let mut first = leaf.try_grow(9).unwrap();
    let second = first.split_off(4);
    drop(leaf);
    assert_eq!(sponsor.snapshot().live_bytes, 9);

    drop(first);
    assert_eq!(sponsor.snapshot().live_bytes, 4);
    drop(second);
    assert_eq!(sponsor.snapshot().live_bytes, 0);
    assert_eq!(sponsor.local_free_bytes(), sponsor.committed_bytes());
    assert!(authority.snapshot().honours_capacity_bound());
}

#[test]
fn same_leaf_leases_merge_without_changing_live_bytes() {
    let authority = authority(64, 8);
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let first_leaf = Reservation::new(&sponsor, ExternalRef::from_u128(2)).unwrap();
    let second_leaf = Reservation::new(&sponsor, ExternalRef::from_u128(3)).unwrap();
    let mut first = first_leaf.try_grow(3).unwrap();
    let second = first_leaf.try_grow(4).unwrap();
    assert_eq!(first.leaf_key(), second.leaf_key());
    assert!(first.merge(second).is_ok());
    assert_eq!(first.bytes(), 7);
    assert_eq!(first_leaf.snapshot().live_bytes, 7);

    let different = second_leaf.try_grow(2).unwrap();
    assert_ne!(first.leaf_key(), different.leaf_key());
    let different = first.merge(different).unwrap_err();
    assert_eq!(first.bytes(), 7);
    assert_eq!(second_leaf.snapshot().live_bytes, 2);
    drop(different);
    drop(first);
    assert_eq!(sponsor.snapshot().live_bytes, 0);
}

#[test]
fn reservation_metrics_count_parent_refill_and_return() {
    let authority = authority(64, 8);
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let leaf = Reservation::new(&sponsor, ExternalRef::from_u128(2)).unwrap();
    let lease = leaf.try_grow(9).unwrap();
    assert_eq!(leaf.metrics().parent_top_up_calls, 1);
    assert_eq!(leaf.metrics().free_cas_retries, 0);
    drop(lease);
    assert_eq!(leaf.metrics().parent_return_calls, 0);
    leaf.trim();
    assert_eq!(leaf.metrics().parent_return_calls, 1);
}

#[test]
fn excess_idle_returns_before_the_last_lease_drop_completes() {
    let authority = authority(64, 8);
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let leaf = Reservation::new(&sponsor, ExternalRef::from_u128(2)).unwrap();
    let lease = leaf.try_grow(32).unwrap();
    assert_eq!(leaf.snapshot().committed_bytes, 32);

    drop(lease);
    assert_eq!(leaf.snapshot().live_bytes, 0);
    assert_eq!(leaf.snapshot().committed_bytes, 8);
    assert!(authority.snapshot().honours_capacity_bound());
}

#[test]
fn concurrent_growth_and_close_keep_the_leaf_within_its_commitment() {
    for _ in 0..100 {
        let authority = authority(16, 8);
        let sponsor = authority
            .create_account(AccountKind::Work, ExternalRef::from_u128(1))
            .unwrap();
        let leaf = Arc::new(Reservation::new(&sponsor, ExternalRef::from_u128(2)).unwrap());
        let lease = leaf.try_grow(8).unwrap();
        let start = Arc::new(Barrier::new(2));
        let left = Arc::clone(&leaf);
        let ready = Arc::clone(&start);
        let grow = std::thread::spawn(move || {
            ready.wait();
            left.try_grow(8).map(drop)
        });
        start.wait();
        leaf.close();
        let _ = grow.join().unwrap();
        let snapshot = leaf.snapshot();
        assert!(snapshot.live_bytes <= snapshot.committed_bytes);
        assert_eq!(snapshot.live_bytes, 8);
        drop(lease);
        assert_eq!(leaf.snapshot().committed_bytes, 0);
        assert!(authority.snapshot().honours_capacity_bound());
    }
}

#[test]
fn dropping_each_leaf_releases_its_account_slot() {
    let mut config = AuthorityConfig::new(128, 64, 64);
    config.max_accounts = 3;
    let authority = MemoryAuthority::new(config).unwrap();
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    for _ in 0..100 {
        let leaf = Reservation::new(&sponsor, ExternalRef::from_u128(2)).unwrap();
        drop(leaf.try_grow(1).unwrap());
        drop(leaf);
    }
}
