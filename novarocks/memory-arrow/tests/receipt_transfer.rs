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

//! Moving a claimed charge (MEM-1 acceptance A06, transfer half).
//!
//! A handover between scopes moves one debt. Claiming the backing again would
//! replace its reservation, creating the new charge before dropping the old
//! one, so a sampler watching the common ancestor would see a spike that never
//! happened. Moving through the core does not, and a refusal leaves the source
//! exactly as it was.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use arrow_buffer::Buffer;
use novarocks_memory::account::{AccountHandle, TopUpPolicy};
use novarocks_memory::authority::{AuthorityConfig, MemoryAuthority};
use novarocks_memory::error::TransferError;
use novarocks_memory::ids::{AccountKind, ExternalRef};
use novarocks_memory::policy::LimitDimension;
use novarocks_memory_arrow::{ClaimReceipt, FulfilmentPool, claim_buffer};

const MIB: u64 = 1024 * 1024;

struct Fixture {
    authority: MemoryAuthority,
    group: AccountHandle,
    source: AccountHandle,
    destination: AccountHandle,
}

fn fixture(capacity: u64) -> Fixture {
    let mut config = AuthorityConfig::new(capacity * 4, capacity, capacity);
    config.top_up = TopUpPolicy::uniform(1);
    let authority = MemoryAuthority::new(config).expect("valid configuration");
    let group = authority
        .create_account(AccountKind::ResourceGroup, ExternalRef::from_u128(1))
        .expect("group");
    let source = group
        .create_child(AccountKind::Work, ExternalRef::from_u128(2))
        .expect("source");
    let destination = group
        .create_child(AccountKind::Work, ExternalRef::from_u128(3))
        .expect("destination");
    Fixture {
        authority,
        group,
        source,
        destination,
    }
}

#[test]
fn a_receipt_moves_the_charge_without_re_claiming() {
    let fixture = fixture(64 * MIB);
    let pool = FulfilmentPool::new(fixture.source.request_grant(8 * MIB).expect("grant"));

    let buffer = Buffer::from_vec(vec![0u8; 64 * 1024]);
    let capacity = buffer.capacity() as u64;
    let receipt = claim_buffer(&buffer, &pool);
    assert_eq!(fixture.source.snapshot().live_bytes, capacity);
    assert_eq!(receipt.sponsors(), vec![fixture.source.id()]);

    receipt
        .transfer_to(&fixture.destination)
        .expect("the handover is accepted");

    assert_eq!(receipt.sponsors(), vec![fixture.destination.id()]);
    assert_eq!(fixture.destination.snapshot().live_bytes, capacity);
    assert_eq!(fixture.source.snapshot().live_bytes, 0);
    assert_eq!(
        receipt.charged_bytes(),
        capacity,
        "the debt moved; it did not double or disappear"
    );

    // The buffer still owns the release obligation, wherever the charge lives.
    drop(buffer);
    assert_eq!(fixture.destination.snapshot().live_bytes, 0);
}

#[test]
fn the_common_ancestor_never_moves_while_a_charge_is_handed_over() {
    // A sampler watching the group must never see its commitment change: the
    // capacity does not leave the group, it only changes which child holds it.
    const HANDOVERS: u64 = 500;
    let fixture = Arc::new(fixture(64 * MIB));
    let pool = FulfilmentPool::new(fixture.source.request_grant(8 * MIB).expect("grant"));

    let buffer = Buffer::from_vec(vec![0u8; 32 * 1024]);
    let receipt = claim_buffer(&buffer, &pool);
    let expected = fixture.group.committed_bytes();

    let sampling = Arc::new(AtomicBool::new(true));
    let samples = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(2));

    let sampler = {
        let fixture = Arc::clone(&fixture);
        let sampling = Arc::clone(&sampling);
        let samples = Arc::clone(&samples);
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            while sampling.load(Ordering::Acquire) {
                let observed = fixture.group.committed_bytes();
                assert_eq!(
                    observed, expected,
                    "the common ancestor's commitment moved during a handover"
                );
                samples.fetch_add(1, Ordering::Relaxed);
            }
        })
    };

    barrier.wait();
    let mut transfers = 0u64;
    for _ in 0..HANDOVERS {
        receipt
            .transfer_to(&fixture.destination)
            .expect("handover to the destination");
        receipt
            .transfer_to(&fixture.source)
            .expect("handover back to the source");
        transfers += 2;
    }
    sampling.store(false, Ordering::Release);
    sampler.join().expect("sampler");

    assert_eq!(transfers, HANDOVERS * 2);
    assert!(
        samples.load(Ordering::Relaxed) > transfers,
        "the sampler must observe the ancestor more often than it changes hands: {} samples for \
         {transfers} handovers",
        samples.load(Ordering::Relaxed)
    );
    assert_eq!(fixture.group.committed_bytes(), expected);
    drop(buffer);
}

#[test]
fn a_refused_handover_leaves_every_charge_where_it_was() {
    let fixture = fixture(64 * MIB);
    // Sized to admit the first backing and refuse a later one, so the rollback
    // has something real to undo. A policy that refused the very first charge
    // would prove nothing about reverting.
    fixture
        .destination
        .install_policy(20 * 1024, LimitDimension::Work);
    let pool = FulfilmentPool::new(fixture.source.request_grant(8 * MIB).expect("grant"));

    // Three separate backings, so the rollback has something to undo.
    let buffers: Vec<Buffer> = (0..3)
        .map(|index| Buffer::from_vec(vec![index as u8; 16 * 1024]))
        .collect();
    let charged_before_claim = fixture.source.snapshot().live_bytes;
    assert_eq!(charged_before_claim, 0);

    // Claimed separately, handed over together: merging is what makes the move
    // all-or-nothing across backings a caller took at different moments.
    let combined = ClaimReceipt::merge(
        buffers
            .iter()
            .map(|buffer| claim_buffer(buffer, &pool))
            .collect::<Vec<_>>(),
    );
    let charged = fixture.source.snapshot().live_bytes;
    assert!(
        charged > 20 * 1024,
        "three backings exceed the destination policy"
    );
    assert_eq!(combined.len(), 3);

    let error = combined
        .transfer_to(&fixture.destination)
        .expect_err("the destination policy is too small");
    assert!(
        matches!(error.cause, TransferError::Denied { .. }),
        "{error:?}"
    );
    assert_eq!(
        error.reverted, 1,
        "the first backing moved and was put back: {error:?}"
    );
    assert_eq!(error.total, 3);

    assert_eq!(
        fixture.source.snapshot().live_bytes,
        charged,
        "every charge is back with the source"
    );
    assert_eq!(fixture.destination.snapshot().live_bytes, 0);
    for sponsor in combined.sponsors() {
        assert_eq!(sponsor, fixture.source.id());
    }
    assert!(fixture.authority.snapshot().honours_capacity_bound());
    drop(buffers);
}

#[test]
fn a_handover_of_an_already_released_charge_is_not_an_error() {
    let fixture = fixture(64 * MIB);
    let pool = FulfilmentPool::new(fixture.source.request_grant(8 * MIB).expect("grant"));

    let buffer = Buffer::from_vec(vec![0u8; 8192]);
    let receipt = claim_buffer(&buffer, &pool);
    // The backing goes away before the handover runs, which is a normal race
    // between a producer finishing and a consumer taking ownership.
    drop(buffer);

    receipt
        .transfer_to(&fixture.destination)
        .expect("there is no debt left to move");
    assert_eq!(receipt.charged_bytes(), 0);
    assert_eq!(fixture.destination.snapshot().live_bytes, 0);
    assert_eq!(fixture.source.snapshot().live_bytes, 0);
}

#[test]
fn a_handover_to_the_same_account_is_a_no_op() {
    let fixture = fixture(64 * MIB);
    let pool = FulfilmentPool::new(fixture.source.request_grant(8 * MIB).expect("grant"));
    let buffer = Buffer::from_vec(vec![0u8; 8192]);
    let capacity = buffer.capacity() as u64;
    let receipt = claim_buffer(&buffer, &pool);

    receipt
        .transfer_to(&fixture.source)
        .expect("moving to the current sponsor changes nothing");
    assert_eq!(fixture.source.snapshot().live_bytes, capacity);
    assert_eq!(receipt.sponsors(), vec![fixture.source.id()]);
    drop(buffer);
}
