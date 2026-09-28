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

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_memory::account::TopUpPolicy;
use novarocks_memory::authority::{AuthorityConfig, MemoryAuthority};
use novarocks_memory::ids::{AccountKind, ExternalRef};
use novarocks_memory_arrow::{BackingCollector, BackingProvenance, RetentionDomain};

fn authority(capacity: u64) -> MemoryAuthority {
    let mut config = AuthorityConfig::new(capacity * 2, capacity, capacity);
    config.top_up = TopUpPolicy::uniform(1);
    MemoryAuthority::new(config).unwrap()
}

fn batch(rows: usize) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(Int32Array::from_iter_values((0..rows).map(|i| i as i32))) as ArrayRef],
    )
    .unwrap()
}

fn trusted() -> BackingProvenance {
    // Every test input is built by standard Arrow constructors.
    unsafe { BackingProvenance::trusted_standard_arrow() }
}

/// This independent oracle walks the actual Arrow payloads. It never asks a
/// candidate Entry or lineage set which address is paid.
fn actual_unique_backings(values: &[&RecordBatch]) -> (usize, u64) {
    let mut collector = BackingCollector::new();
    for value in values {
        collector.collect_batch(value, &trusted()).unwrap();
    }
    let collection = collector.finish();
    (collection.backings().len(), collection.total_capacity())
}

#[test]
fn independent_census_finds_one_backing_across_fork_and_derivation() {
    let authority = authority(16 * 1024);
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let domain = RetentionDomain::new(&sponsor, ExternalRef::from_u128(2)).unwrap();
    let source = domain.retain(batch(256), &trusted()).unwrap();
    let fork = source.fork();
    let derived = domain
        .derive(&[&source], source.payload().slice(64, 64), &trusted())
        .unwrap();
    let (count, capacity) =
        actual_unique_backings(&[source.payload(), fork.payload(), derived.payload()]);
    assert_eq!(count, 1);
    assert_eq!(source.data_bytes(), capacity);
    assert_eq!(derived.data_bytes(), capacity);
    assert!(domain.snapshot().live_bytes > capacity);
    drop(source);
    drop(fork);
    drop(derived);
    assert_eq!(domain.snapshot().live_bytes, 0);
}

#[test]
fn independent_census_detects_conservative_bare_reimport() {
    let authority = authority(16 * 1024);
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let domain = RetentionDomain::new(&sponsor, ExternalRef::from_u128(2)).unwrap();
    let first = domain.retain(batch(256), &trusted()).unwrap();
    let single_live = domain.snapshot().live_bytes;
    let second = domain.retain(first.payload().clone(), &trusted()).unwrap();
    let (count, capacity) = actual_unique_backings(&[first.payload(), second.payload()]);
    assert_eq!(count, 1);
    assert_eq!(capacity, first.data_bytes());
    assert_eq!(domain.snapshot().live_bytes, single_live * 2);
    drop(first);
    drop(second);
    assert_eq!(domain.snapshot().live_bytes, 0);
}

#[test]
fn stream_exceeding_capacity_many_times_does_not_accumulate_entries() {
    let authority = authority(8192);
    let baseline_accounts = authority.live_accounts();
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let domain = RetentionDomain::new(&sponsor, ExternalRef::from_u128(2)).unwrap();
    let mut charged_total = 0u64;
    let mut peak = 0u64;
    for _ in 0..256 {
        let retained = domain.retain(batch(1024), &trusted()).unwrap();
        let (_, capacity) = actual_unique_backings(&[retained.payload()]);
        charged_total += capacity;
        peak = peak.max(domain.snapshot().live_bytes);
        drop(retained);
        assert_eq!(domain.snapshot().live_bytes, 0);
        assert_eq!(domain.census().entries, 0);
        assert_eq!(domain.census().sets, 0);
    }
    assert!(charged_total >= authority.capacity_bytes() * 100);
    assert!(peak < authority.capacity_bytes());
    drop(domain);
    drop(sponsor);
    assert_eq!(authority.live_accounts(), baseline_accounts);
}

#[test]
fn repeated_domain_creation_reuses_a_tight_account_slot_budget() {
    let mut config = AuthorityConfig::new(16 * 1024, 8192, 8192);
    config.top_up = TopUpPolicy::uniform(1);
    config.max_accounts = 3; // process, sponsor, and exactly one leaf
    let authority = MemoryAuthority::new(config).unwrap();
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    for generation in 0..128 {
        let domain =
            RetentionDomain::new(&sponsor, ExternalRef::from_u128(generation + 2)).unwrap();
        let retained = domain.retain(batch(16), &trusted()).unwrap();
        drop(domain);
        assert_eq!(authority.live_accounts(), 3);
        drop(retained);
        assert_eq!(authority.live_accounts(), 2);
    }
}
