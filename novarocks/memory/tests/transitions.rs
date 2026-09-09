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

//! Transition failures and the actual-versus-committed distinction
//! (MEM-1 acceptance A02 and A03).
//!
//! A failed allocation, a failed grow, a failed transfer, a repeated
//! completion and a rollback all leave the original accounting untouched, and
//! settlement after a successful physical action has no window in which it can
//! fail on capacity. The conservative commitment peak and the real allocation
//! peak are reported separately, and neither is fabricated from the other.

use novarocks_memory::account::TopUpPolicy;
use novarocks_memory::authority::{AuthorityConfig, MemoryAuthority};
use novarocks_memory::bound::SetRelation;
use novarocks_memory::error::{FulfilError, TransferError};
use novarocks_memory::ids::{AccountKind, ExternalRef};
use novarocks_memory::policy::LimitDimension;

const MIB: u64 = 1024 * 1024;

fn authority(capacity: u64) -> MemoryAuthority {
    let mut config = AuthorityConfig::new(capacity * 4, capacity, capacity);
    config.top_up = TopUpPolicy::uniform(1);
    MemoryAuthority::new(config).expect("authority configuration is valid")
}

#[test]
fn settlement_within_a_grant_cannot_fail_on_capacity() {
    // This is the property the whole grant model exists for: a caller that
    // reserved first, then allocated, is never left holding memory it cannot
    // account for.
    let authority = authority(1024);
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work");
    let grant = work.request_grant(512).expect("grant");

    // Lowering the policy below what is already granted must not break the
    // settlement guarantee of capacity already issued.
    work.install_policy(1, LimitDimension::Work);
    assert!(work.is_closed_to_growth(), "growth freezes over policy");

    let charge = grant
        .fulfil(512)
        .expect("already-granted capacity still settles");
    assert_eq!(charge.bytes(), 512);
}

#[test]
fn cancelling_a_scope_stops_new_capacity_but_not_settlement() {
    let authority = authority(1024);
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work");
    let grant = work.request_grant(256).expect("grant");

    work.close_to_growth();
    assert!(
        work.request_grant(1).is_err(),
        "a cancelled scope issues nothing new"
    );

    let charge = grant.fulfil(256).expect("granted capacity still settles");
    assert_eq!(charge.bytes(), 256);
    assert_eq!(
        work.snapshot().live_bytes,
        256,
        "a cancelled scope keeps accounting for what it holds"
    );
}

#[test]
fn a_fulfilment_beyond_the_remainder_is_refused_and_changes_nothing() {
    let authority = authority(1024);
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work");
    let grant = work.request_grant(100).expect("grant");
    let before = work.snapshot();

    let error = grant.fulfil(101).expect_err("beyond the remainder");
    match error {
        FulfilError::ExceedsRemainder {
            requested,
            remainder,
            ..
        } => {
            assert_eq!(requested, 101);
            assert_eq!(remainder, 100);
        }
        other => panic!("expected an exceeded remainder, got {other:?}"),
    }
    let after = work.snapshot();
    assert_eq!(after.live_bytes, before.live_bytes);
    assert_eq!(after.granted_bytes, before.granted_bytes);
    assert_eq!(after.committed_bytes, before.committed_bytes);
    assert_eq!(grant.remaining_bytes(), 100, "the grant is intact");
}

#[test]
fn a_revoked_remainder_is_confirmed_by_what_it_actually_took() {
    let authority = authority(1024);
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work");
    let grant = work.request_grant(100).expect("grant");
    let charge = grant.fulfil(30).expect("fulfil part of it");

    let taken = grant.revoke_remainder();
    assert_eq!(taken, 70, "only the unfulfilled part can be taken");
    assert_eq!(grant.remaining_bytes(), 0);
    assert!(grant.is_revoked());

    let error = grant.fulfil(1).expect_err("the right is gone");
    assert!(matches!(error, FulfilError::Cancelled { .. }), "{error:?}");
    assert_eq!(
        charge.bytes(),
        30,
        "revocation does not touch what was already settled"
    );
    assert_eq!(work.snapshot().live_bytes, 30);
}

#[test]
fn a_conservative_grow_reports_the_commitment_peak_and_the_real_peak_apart() {
    // 64 MiB grows to 96 MiB. A relocating grow must cover the full
    // replacement, so the commitment peak reaches 160 MiB while the real
    // allocation never exceeds 96 MiB -- and if the grow fails, live
    // allocation is still only the original 64 MiB.
    let authority = authority(1024 * MIB);
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work");

    let first = work.request_grant(64 * MIB).expect("grant the original");
    let charge = first.fulfil(64 * MIB).expect("settle the original");

    let replacement = work
        .request_grant(96 * MIB)
        .expect("grant the full replacement");
    assert_eq!(
        work.committed_bytes(),
        160 * MIB,
        "the old block and its replacement are both covered"
    );
    let committed_peak = work.snapshot().peak_committed_bytes;
    assert_eq!(committed_peak, 160 * MIB);

    // The underlying allocation fails, so the replacement is never settled.
    drop(replacement);
    let snapshot = work.snapshot();
    assert_eq!(
        snapshot.live_bytes,
        64 * MIB,
        "known live allocation is still only the original block"
    );
    assert_eq!(
        snapshot.peak_live_bytes,
        64 * MIB,
        "the real allocation peak is not inflated by the conservative cover"
    );
    assert_eq!(
        snapshot.peak_committed_bytes,
        160 * MIB,
        "the commitment peak is kept as the separate fact it is"
    );
    charge.release();
}

#[test]
fn peaks_of_different_accounts_are_never_added_together() {
    let authority = authority(1024);
    let first = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("first");
    let second = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(2))
        .expect("second");

    // Two accounts peak at 300 each, at different times.
    let grant = first.request_grant(300).expect("first grant");
    let charge = grant.fulfil(300).expect("first fulfil");
    assert_eq!(first.snapshot().peak_live_bytes, 300);
    charge.release();
    drop(grant);
    first.shrink_idle(300);

    let grant = second.request_grant(300).expect("second grant");
    let charge = grant.fulfil(300).expect("second fulfil");
    assert_eq!(second.snapshot().peak_live_bytes, 300);

    // The root's own peak is its own, not the sum of the two.
    let root = authority.snapshot().root;
    assert_eq!(
        root.peak_committed_bytes, 300,
        "the root peaked at 300, not at 600: {root:?}"
    );
    charge.release();
}

#[test]
fn releasing_twice_settles_once() {
    let authority = authority(1024);
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work");
    let grant = work.request_grant(64).expect("grant");
    let charge = grant.fulfil(64).expect("fulfil");
    let state = charge.state().clone();

    assert_eq!(charge.release(), 64, "the first release settles");
    assert_eq!(state.release(), 0, "a repeated release settles nothing");
    assert!(state.is_released());
    assert_eq!(
        work.snapshot().live_bytes,
        0,
        "the account is not credited twice"
    );
}

#[test]
fn a_refused_transfer_leaves_the_source_exactly_as_it_was() {
    let authority = authority(1024);
    let source = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("source");
    let destination = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(2))
        .expect("destination");
    destination.install_policy(10, LimitDimension::Work);

    let grant = source.request_grant(100).expect("grant");
    let charge = grant.fulfil(100).expect("fulfil");
    let before_source = source.snapshot();
    let before_root = authority.snapshot().root;

    let error = charge
        .transfer_to(&destination)
        .expect_err("the destination policy refuses");
    assert!(matches!(error, TransferError::Denied { .. }), "{error:?}");

    let after_source = source.snapshot();
    assert_eq!(after_source.live_bytes, before_source.live_bytes);
    assert_eq!(after_source.committed_bytes, before_source.committed_bytes);
    assert_eq!(
        authority.snapshot().root.committed_bytes,
        before_root.committed_bytes,
        "the common ancestor is untouched by a refused transfer"
    );
    assert_eq!(charge.sponsor_id(), source.id(), "the sponsor did not move");
    charge.release();
}

#[test]
fn a_successful_transfer_moves_the_debt_without_moving_the_ancestor() {
    let authority = authority(1024);
    let group = authority
        .create_account(AccountKind::ResourceGroup, ExternalRef::from_u128(1))
        .expect("group");
    let source = group
        .create_child(AccountKind::Work, ExternalRef::from_u128(2))
        .expect("source");
    let destination = group
        .create_child(AccountKind::Work, ExternalRef::from_u128(3))
        .expect("destination");

    let grant = source.request_grant(100).expect("grant");
    let charge = grant.fulfil(100).expect("fulfil");
    let ancestor_before = group.committed_bytes();

    charge.transfer_to(&destination).expect("transfer");

    assert_eq!(
        group.committed_bytes(),
        ancestor_before,
        "the common ancestor's commitment does not move"
    );
    assert_eq!(charge.sponsor_id(), destination.id());
    assert_eq!(destination.snapshot().live_bytes, 100);
    assert_eq!(source.snapshot().live_bytes, 0);
    assert_eq!(source.committed_bytes(), 0, "the source gave the debt up");
    charge.release();
}

#[test]
fn a_bound_converts_only_what_it_actually_covered() {
    let authority = authority(1024);
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work");

    let bound = work.begin_bounded_step(100).expect("open a bounded step");
    assert_eq!(work.snapshot().bounded_bytes, 100);
    assert_eq!(work.snapshot().live_bytes, 0, "a bound is not live memory");

    // A separate copy cannot come out of this bound: both allocations are
    // real at the same time.
    let refused = bound
        .convert(40, SetRelation::SeparatelyCopied)
        .expect_err("a separate copy needs its own capacity");
    assert!(refused.to_string().contains("separate copy"), "{refused}");

    // Output that is part of the covered allocation converts, and the total
    // commitment does not change.
    let committed_before = work.committed_bytes();
    let charge = bound
        .convert(40, SetRelation::WithinAuthorisedAllocation)
        .expect("convert measured output");
    assert_eq!(work.committed_bytes(), committed_before);
    let snapshot = work.snapshot();
    assert_eq!(snapshot.live_bytes, 40);
    assert_eq!(snapshot.bounded_bytes, 60);

    // Measuring more than the bound covered means the bound was not an upper
    // bound; it is refused rather than quietly widened.
    let refused = bound
        .convert(61, SetRelation::WithinAuthorisedAllocation)
        .expect_err("the bound did not cover this much");
    assert!(refused.to_string().contains("exceed"), "{refused}");

    charge.release();
    assert_eq!(bound.settle(), 60, "settling returns what it still covered");
}

#[test]
fn an_overlapping_copy_is_charged_twice_on_purpose() {
    let authority = authority(1024);
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work");

    let bound = work.begin_bounded_step(100).expect("bound");
    // The copy needs its own capacity while the external memory is alive.
    let copy_grant = work.request_grant(40).expect("capacity for the copy");
    let copy = copy_grant.fulfil(40).expect("settle the copy");

    let snapshot = work.snapshot();
    assert_eq!(
        snapshot.bounded_bytes, 100,
        "the external memory is covered"
    );
    assert_eq!(snapshot.live_bytes, 40, "the copy is charged as well");
    assert_eq!(
        snapshot.committed_bytes, 140,
        "the overlap is charged twice while both are alive: {snapshot:?}"
    );

    // Once the external memory is really gone, its bound settles.
    assert_eq!(bound.settle(), 100);
    assert_eq!(work.snapshot().committed_bytes, 140);
    assert_eq!(work.snapshot().live_bytes, 40);
    copy.release();
}
