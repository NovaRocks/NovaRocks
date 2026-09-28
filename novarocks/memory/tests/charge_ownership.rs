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

//! Ownership of charge metadata after handover and growth consolidation.

use std::sync::Arc;

use novarocks_memory::account::TopUpPolicy;
use novarocks_memory::authority::{AuthorityConfig, MemoryAuthority};
use novarocks_memory::ids::{AccountKind, ExternalRef};

fn authority() -> MemoryAuthority {
    let mut config = AuthorityConfig::new(4096, 2048, 2048);
    config.top_up = TopUpPolicy::uniform(1);
    MemoryAuthority::new(config).expect("valid authority")
}

#[test]
fn handover_moves_the_only_arc_and_the_last_owner_releases_once() {
    let authority = authority();
    let baseline = authority.live_accounts();
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .expect("work account");
    let grant = work.request_grant(64).expect("grant");
    let charge = grant.fulfil(64).expect("charge");

    let state = charge.into_state();
    assert_eq!(Arc::strong_count(&state), 1, "handover must move the Arc");
    assert_eq!(work.snapshot().live_bytes, 64);
    let weak = Arc::downgrade(&state);
    assert_eq!(state.release(), 64);
    assert_eq!(state.release(), 0);
    drop(state);
    assert!(weak.upgrade().is_none(), "the state must be destroyed");

    drop(grant);
    drop(work);
    assert_eq!(authority.live_accounts(), baseline);
    assert_eq!(authority.snapshot().root.live_bytes, 0);
}

#[test]
fn growth_consolidation_drops_the_temporary_state_and_account_reference() {
    let authority = authority();
    let baseline = authority.live_accounts();
    let work = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(2))
        .expect("work account");
    let original = work.request_grant(40).expect("original grant");
    let growth = work.request_grant(24).expect("growth grant");
    let charge = original.fulfil(40).expect("original charge");

    charge.grow_within(&growth, 24).expect("growth settles");
    assert_eq!(charge.bytes(), 64);
    assert_eq!(work.snapshot().live_bytes, 64);
    assert_eq!(charge.release(), 64);
    assert_eq!(work.snapshot().live_bytes, 0, "the total settles once");

    drop(growth);
    drop(original);
    drop(work);
    assert_eq!(authority.live_accounts(), baseline);
    assert_eq!(authority.snapshot().root.live_bytes, 0);
}

#[test]
fn handed_over_state_transfers_and_releases_from_its_new_sponsor() {
    let authority = authority();
    let baseline = authority.live_accounts();
    let source = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(3))
        .expect("source");
    let destination = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(4))
        .expect("destination");
    let grant = source.request_grant(32).expect("grant");
    let state = grant.fulfil(32).expect("charge").into_state();

    state.transfer_to(&destination).expect("transfer");
    assert_eq!(source.snapshot().live_bytes, 0);
    assert_eq!(destination.snapshot().live_bytes, 32);
    assert_eq!(state.release(), 32);
    assert_eq!(state.release(), 0);
    drop(state);
    drop(grant);
    drop(source);
    drop(destination);
    assert_eq!(authority.live_accounts(), baseline);
    assert_eq!(authority.snapshot().root.live_bytes, 0);
}
