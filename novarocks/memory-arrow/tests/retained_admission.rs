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
use novarocks_memory_arrow::{BackingProvenance, RetainError, RetentionDomain};

fn authority(capacity: u64) -> MemoryAuthority {
    let mut config = AuthorityConfig::new(capacity * 2, capacity, capacity);
    config.top_up = TopUpPolicy::uniform(1);
    MemoryAuthority::new(config).unwrap()
}

fn batch(count: usize) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(Int32Array::from_iter_values((0..count).map(|i| i as i32))) as ArrayRef],
    )
    .unwrap()
}

fn trusted() -> BackingProvenance {
    // Tests construct their inputs using standard Arrow arrays only.
    unsafe { BackingProvenance::trusted_standard_arrow() }
}

#[test]
fn failing_mixed_derivation_keeps_all_sources_and_releases_no_partial_entry() {
    let authority = authority(1024);
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let domain = RetentionDomain::new(&sponsor, ExternalRef::from_u128(2)).unwrap();
    let source = domain.retain(batch(16), &trusted()).unwrap();
    let before = domain.snapshot().live_bytes;

    let output = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("old", DataType::Int32, false),
            Field::new("new", DataType::Int32, false),
        ])),
        vec![
            source.payload().column(0).clone(),
            Arc::new(Int32Array::from_iter_values(0..300).slice(0, 16)) as ArrayRef,
        ],
    )
    .unwrap();
    assert!(matches!(
        domain.derive(&[&source], output, &trusted()),
        Err(RetainError::Capacity(_))
    ));
    assert_eq!(domain.snapshot().live_bytes, before);
    assert_eq!(source.payload().num_rows(), 16);
    drop(source);
    assert_eq!(domain.snapshot().live_bytes, 0);
}

#[test]
fn bare_reimport_of_same_backing_pays_full_capacity_again() {
    let authority = authority(4096);
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let domain = RetentionDomain::new(&sponsor, ExternalRef::from_u128(2)).unwrap();
    let first = domain.retain(batch(32), &trusted()).unwrap();
    let first_live = domain.snapshot().live_bytes;
    let second = domain.retain(first.payload().clone(), &trusted()).unwrap();
    assert_eq!(second.data_bytes(), first.data_bytes());
    assert_eq!(domain.snapshot().live_bytes, first_live * 2);
    drop(first);
    assert_eq!(domain.snapshot().live_bytes, first_live);
    drop(second);
    assert_eq!(domain.snapshot().live_bytes, 0);
}

#[test]
fn unknown_provenance_and_foreign_authority_are_rejected_before_admission() {
    let first_authority = authority(4096);
    let second_authority = authority(4096);
    let first_sponsor = first_authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    let second_sponsor = second_authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(2))
        .unwrap();
    let first = RetentionDomain::new(&first_sponsor, ExternalRef::from_u128(3)).unwrap();
    let second = RetentionDomain::new(&second_sponsor, ExternalRef::from_u128(4)).unwrap();
    assert!(matches!(
        first.retain(batch(4), &BackingProvenance::unknown()),
        Err(RetainError::Backing(_))
    ));
    assert_eq!(first.snapshot().live_bytes, 0);

    let source = first.retain(batch(4), &trusted()).unwrap();
    assert!(matches!(
        second.derive(&[&source], source.payload().clone(), &trusted()),
        Err(RetainError::DifferentAuthority)
    ));
    assert_eq!(second.snapshot().live_bytes, 0);
    drop(source);
    assert_eq!(first.snapshot().live_bytes, 0);
}
