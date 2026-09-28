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
use novarocks_memory_arrow::{BackingProvenance, RetentionDomain};

fn authority(capacity: u64) -> MemoryAuthority {
    let mut config = AuthorityConfig::new(capacity * 2, capacity, capacity);
    config.top_up = TopUpPolicy::uniform(1);
    MemoryAuthority::new(config).unwrap()
}

fn batch(values: Vec<i32>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(Int32Array::from(values)) as ArrayRef],
    )
    .unwrap()
}

fn provenance() -> BackingProvenance {
    // These tests create every buffer through standard Arrow constructors.
    unsafe { BackingProvenance::trusted_standard_arrow() }
}

#[test]
fn grouped_import_and_fork_keep_one_data_debit_until_final_exit() {
    let authority = authority(16 * 1024);
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let domain = RetentionDomain::new(&sponsor, ExternalRef::from_u128(2)).unwrap();
    let first = batch((0..64).collect());
    let second = first.slice(8, 16);
    let mut group = domain
        .retain_many(vec![first, second], &provenance())
        .unwrap();
    let data = group[0].data_bytes();
    assert!(data >= 256);
    assert_eq!(group[1].data_bytes(), data);
    let live = domain.snapshot().live_bytes;
    assert!(live > data);

    let fork = group[0].fork();
    assert_eq!(domain.snapshot().live_bytes, live);
    drop(group.remove(0));
    drop(group);
    assert!(domain.snapshot().live_bytes < live);
    assert!(domain.snapshot().live_bytes >= data);
    drop(fork);
    assert_eq!(domain.snapshot().live_bytes, 0);
    assert!(authority.snapshot().honours_capacity_bound());
}

#[test]
fn derive_reuses_source_data_and_settles_new_metadata_separately() {
    let authority = authority(16 * 1024);
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let domain = RetentionDomain::new(&sponsor, ExternalRef::from_u128(2)).unwrap();
    let source = domain
        .retain(batch((0..64).collect()), &provenance())
        .unwrap();
    let source_live = domain.snapshot().live_bytes;
    assert_eq!(source_live, source.data_bytes() + source.metadata_bytes());
    let census = domain.census();
    assert_eq!(census.entries, 1);
    assert_eq!(census.sets, 1);
    assert_eq!(census.data_bytes, source.data_bytes());
    assert_eq!(census.data_bytes + census.metadata_bytes, source_live);
    let derived = domain
        .derive(&[&source], source.payload().slice(4, 12), &provenance())
        .unwrap();
    assert_eq!(derived.data_bytes(), source.data_bytes());
    let with_derived = domain.snapshot().live_bytes;
    assert!(with_derived > source_live);
    assert!(with_derived - source_live < source.data_bytes());

    drop(source);
    assert!(domain.snapshot().live_bytes >= derived.data_bytes());
    drop(derived);
    assert_eq!(domain.snapshot().live_bytes, 0);
    assert_eq!(domain.census().entries, 0);
    assert_eq!(domain.census().sets, 0);
}

#[test]
fn original_domain_handle_can_exit_before_retained_value() {
    let authority = authority(16 * 1024);
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let domain = RetentionDomain::new(&sponsor, ExternalRef::from_u128(2)).unwrap();
    let retained = domain.retain(batch(vec![1, 2, 3]), &provenance()).unwrap();
    let live = domain.snapshot().live_bytes;
    drop(domain);
    assert_eq!(sponsor.snapshot().live_bytes, live);
    drop(retained);
    assert_eq!(sponsor.snapshot().live_bytes, 0);
}
