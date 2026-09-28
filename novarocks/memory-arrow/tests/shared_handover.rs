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
use novarocks_memory::budget::MetadataBudget;
use novarocks_memory::holder::HolderRegistry;
use novarocks_memory::ids::{AccountKind, ExternalRef};
use novarocks_memory_arrow::{BackingProvenance, RetentionDomain, SharedError, SharedRetention};

fn authority() -> MemoryAuthority {
    let mut config = AuthorityConfig::new(32 * 1024, 16 * 1024, 16 * 1024);
    config.top_up = TopUpPolicy::uniform(1);
    MemoryAuthority::new(config).unwrap()
}

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(Int32Array::from_iter_values(0..64)) as ArrayRef],
    )
    .unwrap()
}

fn provenance() -> BackingProvenance {
    // The test batch uses a standard Arrow allocation.
    unsafe { BackingProvenance::trusted_standard_arrow() }
}

#[test]
fn removing_service_registration_keeps_borrowed_data_paid_by_service() {
    let authority = authority();
    let service_account = authority
        .create_account(AccountKind::Service, ExternalRef::from_u128(1))
        .unwrap();
    let query_account = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(2))
        .unwrap();
    let service = RetentionDomain::new(&service_account, ExternalRef::from_u128(3)).unwrap();
    let query = RetentionDomain::new(&query_account, ExternalRef::from_u128(4)).unwrap();
    let retained = service.retain(batch(), &provenance()).unwrap();
    let registration =
        SharedRetention::register(&service, ExternalRef::from_u128(5), retained).unwrap();
    let service_live = service.snapshot().live_bytes;
    let borrowed = registration.borrow_for(&query).unwrap();
    assert_eq!(service.snapshot().live_bytes, service_live);
    assert_eq!(query.snapshot().live_bytes, 0);
    assert_eq!(service.census().entries, 1);
    assert_eq!(query.census().entries, 0);

    drop(registration);
    service.close();
    drop(service);
    assert_eq!(service_account.snapshot().live_bytes, service_live);
    assert_eq!(query.snapshot().live_bytes, 0);
    drop(borrowed);
    assert_eq!(service_account.snapshot().live_bytes, 0);
    assert_eq!(query.census().data_bytes, 0);
}

#[test]
fn foreign_registration_and_unsupported_publication_preserve_source() {
    let authority = authority();
    let service_account = authority
        .create_account(AccountKind::Service, ExternalRef::from_u128(1))
        .unwrap();
    let query_account = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(2))
        .unwrap();
    let service = RetentionDomain::new(&service_account, ExternalRef::from_u128(3)).unwrap();
    let query = RetentionDomain::new(&query_account, ExternalRef::from_u128(4)).unwrap();
    let query_retained = query.retain(batch(), &provenance()).unwrap();
    let before = query.snapshot().live_bytes;
    let (error, query_retained) =
        SharedRetention::register(&service, ExternalRef::from_u128(5), query_retained).unwrap_err();
    assert_eq!(error, SharedError::ForeignSponsor);
    assert_eq!(query.snapshot().live_bytes, before);
    drop(query_retained);
    assert_eq!(query.snapshot().live_bytes, 0);

    let service_retained = service.retain(batch(), &provenance()).unwrap();
    let registration =
        SharedRetention::register(&service, ExternalRef::from_u128(5), service_retained).unwrap();
    let before = service.snapshot().live_bytes;
    let registration = registration.try_publish_to(&query).unwrap_err();
    assert_eq!(service.snapshot().live_bytes, before);
    assert_eq!(query.snapshot().live_bytes, 0);
    drop(registration);
    assert_eq!(service.snapshot().live_bytes, 0);
}

#[test]
fn pin_generation_is_separate_from_borrowed_capacity() {
    let authority = authority();
    let service_account = authority
        .create_account(AccountKind::Service, ExternalRef::from_u128(1))
        .unwrap();
    let query_account = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(2))
        .unwrap();
    let service = RetentionDomain::new(&service_account, ExternalRef::from_u128(3)).unwrap();
    let query = RetentionDomain::new(&query_account, ExternalRef::from_u128(4)).unwrap();
    let retained = service.retain(batch(), &provenance()).unwrap();
    let registration =
        SharedRetention::register(&service, ExternalRef::from_u128(5), retained).unwrap();
    let borrowed = registration.borrow_for(&query).unwrap();
    let before = service.snapshot().live_bytes;

    let holders = HolderRegistry::new(&MetadataBudget::with_defaults());
    let lease = holders
        .acquire_lease(
            query_account.id(),
            registration.resource(),
            borrowed.data_bytes(),
        )
        .unwrap();
    let pin = lease.try_pin(0).unwrap();
    assert_eq!(pin.generation(), 0);
    assert_eq!(holders.invalidate(registration.resource()), Some(1));
    assert!(lease.try_pin(0).is_err());
    drop(pin);
    drop(lease);
    assert_eq!(service.snapshot().live_bytes, before);
    drop(registration);
    drop(borrowed);
    assert_eq!(service.snapshot().live_bytes, 0);
}

#[test]
fn mixed_derivation_keeps_each_existing_backing_on_its_original_sponsor() {
    let authority = authority();
    let service_account = authority
        .create_account(AccountKind::Service, ExternalRef::from_u128(1))
        .unwrap();
    let query_account = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(2))
        .unwrap();
    let service = RetentionDomain::new(&service_account, ExternalRef::from_u128(3)).unwrap();
    let query = RetentionDomain::new(&query_account, ExternalRef::from_u128(4)).unwrap();
    let service_source = service.retain(batch(), &provenance()).unwrap();
    let query_source = query.retain(batch(), &provenance()).unwrap();
    let service_live = service.snapshot().live_bytes;
    let query_live = query.snapshot().live_bytes;
    let output = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("service", DataType::Int32, false),
            Field::new("query", DataType::Int32, false),
        ])),
        vec![
            service_source.payload().column(0).clone(),
            query_source.payload().column(0).clone(),
        ],
    )
    .unwrap();
    let mixed = query
        .derive(&[&service_source, &query_source], output, &provenance())
        .unwrap();
    assert_eq!(service.snapshot().live_bytes, service_live);
    assert!(query.snapshot().live_bytes > query_live);
    drop(service_source);
    drop(query_source);
    assert!(service.snapshot().live_bytes > 0);
    assert!(query.snapshot().live_bytes > 0);
    drop(mixed);
    assert_eq!(service.snapshot().live_bytes, 0);
    assert_eq!(query.snapshot().live_bytes, 0);
}

#[test]
fn closed_query_cannot_begin_a_new_service_borrow() {
    let authority = authority();
    let service_account = authority
        .create_account(AccountKind::Service, ExternalRef::from_u128(1))
        .unwrap();
    let query_account = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(2))
        .unwrap();
    let service = RetentionDomain::new(&service_account, ExternalRef::from_u128(3)).unwrap();
    let query = RetentionDomain::new(&query_account, ExternalRef::from_u128(4)).unwrap();
    let retained = service.retain(batch(), &provenance()).unwrap();
    let registration =
        SharedRetention::register(&service, ExternalRef::from_u128(5), retained).unwrap();
    query.close();
    assert_eq!(
        registration.borrow_for(&query).unwrap_err(),
        SharedError::ClosedBorrower
    );
    assert!(service.snapshot().live_bytes > 0);
}

#[test]
fn closed_service_cannot_publish_a_previously_retained_resource() {
    let authority = authority();
    let service_account = authority
        .create_account(AccountKind::Service, ExternalRef::from_u128(1))
        .unwrap();
    let service = RetentionDomain::new(&service_account, ExternalRef::from_u128(3)).unwrap();
    let retained = service.retain(batch(), &provenance()).unwrap();
    let before = service.snapshot().live_bytes;
    service.close();
    let (error, retained) =
        SharedRetention::register(&service, ExternalRef::from_u128(5), retained).unwrap_err();
    assert_eq!(error, SharedError::ClosedService);
    assert_eq!(service.snapshot().live_bytes, before);
    drop(retained);
    assert_eq!(service.snapshot().live_bytes, 0);
}
