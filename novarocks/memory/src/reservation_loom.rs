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

//! Compiled into the memory crate's unit-test target under `cfg(loom)` so
//! the actual Account and Reservation atomics and the leaf slow mutex use
//! loom's synchronization types from the crate's dev-only dependency.

use crate::Reservation;
use crate::account::TopUpPolicy;
use crate::authority::{AuthorityConfig, MemoryAuthority};
use crate::error::CapacityError;
use crate::ids::{AccountKind, ExternalRef};
use crate::reservation_protocol;
use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};

fn setup() -> Reservation {
    let mut config = AuthorityConfig::new(8, 4, 4);
    config.top_up = TopUpPolicy::uniform(2);
    let authority = MemoryAuthority::new(config).unwrap();
    let sponsor = authority
        .create_account(AccountKind::Work, ExternalRef::from_u128(1))
        .unwrap();
    Reservation::new(&sponsor, ExternalRef::from_u128(2)).unwrap()
}

#[test]
fn grow_and_close_have_one_winner_for_idle_capacity() {
    loom::model(|| {
        let leaf = setup();
        let original = leaf.try_grow(1).unwrap();
        assert_eq!(leaf.snapshot().free_bytes, 1);
        let contender = leaf.clone();
        let grow = loom::thread::spawn(move || contender.try_grow(1).ok());
        let closer = leaf.clone();
        let close = loom::thread::spawn(move || {
            closer.close();
        });
        let grown = grow.join().unwrap();
        let grew = grown.is_some();
        close.join().unwrap();
        let snapshot = leaf.snapshot();
        assert!(snapshot.live_bytes <= snapshot.committed_bytes);
        assert_eq!(snapshot.live_bytes, 1 + u64::from(grew));
        assert!(snapshot.closed);
        drop(grown);
        drop(original);
        assert_eq!(leaf.snapshot().committed_bytes, 0);
    });
}

#[test]
fn shrink_and_growth_preserve_live_within_committed_capacity() {
    loom::model(|| {
        let leaf = setup();
        let original = leaf.try_grow(1).unwrap();
        let shrink = loom::thread::spawn(move || drop(original));
        let growing = leaf.clone();
        let grow = loom::thread::spawn(move || growing.try_grow(1).ok());
        shrink.join().unwrap();
        let grown = grow.join().unwrap();
        let grew = grown.is_some();
        let snapshot = leaf.snapshot();
        assert!(snapshot.live_bytes <= snapshot.committed_bytes);
        assert_eq!(snapshot.live_bytes, u64::from(grew));
        drop(grown);
        leaf.trim();
        assert_eq!(leaf.snapshot().committed_bytes, 0);
    });
}

#[test]
fn concurrent_snapshot_never_reports_more_live_than_commitment() {
    loom::model(|| {
        let leaf = setup();
        let growing = leaf.clone();
        let grow = loom::thread::spawn(move || {
            drop(growing.try_grow(1).unwrap());
        });
        let observing = leaf.clone();
        let snapshot = loom::thread::spawn(move || {
            let snapshot = observing.snapshot();
            assert!(snapshot.live_bytes <= snapshot.committed_bytes);
            assert_eq!(
                snapshot.free_bytes + snapshot.live_bytes,
                snapshot.committed_bytes
            );
        });
        grow.join().unwrap();
        snapshot.join().unwrap();
        leaf.trim();
    });
}

#[test]
fn competing_growth_keeps_each_slow_request_inside_its_own_commitment() {
    loom::model(|| {
        let mut config = AuthorityConfig::new(4, 2, 2);
        config.top_up = TopUpPolicy::uniform(2);
        let authority = MemoryAuthority::new(config).unwrap();
        let sponsor = authority
            .create_account(AccountKind::Work, ExternalRef::from_u128(1))
            .unwrap();
        let leaf = Reservation::new(&sponsor, ExternalRef::from_u128(2)).unwrap();

        let first = leaf.clone();
        let first_grow = loom::thread::spawn(move || first.try_grow(1).unwrap());
        let second = leaf.clone();
        let second_grow = loom::thread::spawn(move || second.try_grow(1).unwrap());
        let first_lease = first_grow.join().unwrap();
        let second_lease = second_grow.join().unwrap();
        let snapshot = leaf.snapshot();
        assert_eq!(snapshot.committed_bytes, 2);
        assert_eq!(snapshot.live_bytes, 2);
        assert_eq!(snapshot.free_bytes, 0);
        drop(first_lease);
        drop(second_lease);
        leaf.trim();
        assert_eq!(leaf.snapshot().committed_bytes, 0);
    });
}

#[test]
fn trim_and_snapshot_share_one_commitment_version() {
    loom::model(|| {
        let leaf = setup();
        let lease = leaf.try_grow(1).unwrap();
        let trimming = leaf.clone();
        let trim = loom::thread::spawn(move || trimming.trim());
        let observing = leaf.clone();
        let snapshot = loom::thread::spawn(move || {
            let snapshot = observing.snapshot();
            assert!(snapshot.live_bytes <= snapshot.committed_bytes);
            assert_eq!(
                snapshot.free_bytes + snapshot.live_bytes,
                snapshot.committed_bytes
            );
        });
        trim.join().unwrap();
        snapshot.join().unwrap();
        assert_eq!(leaf.snapshot().committed_bytes, 1);
        drop(lease);
        leaf.trim();
        assert_eq!(leaf.snapshot().committed_bytes, 0);
    });
}

/// I2/I5: begin with no idle F so close's sweep can race the final release's
/// first F publication. Both operations must complete with no retained C.
#[test]
fn close_and_final_release_return_every_idle_byte() {
    loom::model(|| {
        let leaf = setup();
        let lease = leaf.try_grow(2).unwrap();
        let closing = leaf.clone();
        let close = loom::thread::spawn(move || closing.close());
        let release = loom::thread::spawn(move || drop(lease));
        close.join().unwrap();
        release.join().unwrap();
        let snapshot = leaf.snapshot();
        assert!(snapshot.closed);
        assert_eq!(snapshot.live_bytes, 0);
        assert_eq!(snapshot.free_bytes, 0);
        assert_eq!(snapshot.committed_bytes, 0);
    });
}

/// The controlled parent starts with two granted bytes. The model calls the
/// production F helpers while deriving L and C from admitted and returned
/// outcomes. Atomic take-all stands in for the serialized return, while the
/// close sweep uses the production first-F RMW.
#[test]
fn grow_close_and_final_release_conserve_capacity() {
    loom::model(|| {
        struct Leaf {
            free: AtomicU64,
            closed: AtomicBool,
        }

        impl Leaf {
            fn sweep(&self, close_sweep: bool) -> u64 {
                if close_sweep {
                    reservation_protocol::observe_free_for_close(&self.free);
                }
                self.free.swap(0, Ordering::AcqRel)
            }

            fn release(&self) -> u64 {
                reservation_protocol::publish_free(&self.free, 1);
                if self.closed.load(Ordering::Acquire) {
                    self.sweep(false)
                } else {
                    0
                }
            }
        }

        let leaf = Arc::new(Leaf {
            free: AtomicU64::new(1),
            closed: AtomicBool::new(false),
        });
        let growing = Arc::clone(&leaf);
        let grow = loom::thread::spawn(move || {
            if growing.closed.load(Ordering::Acquire)
                || !reservation_protocol::take_free_with_retries(&growing.free, 1).0
            {
                return false;
            }
            true
        });
        let closing = Arc::clone(&leaf);
        let close = loom::thread::spawn(move || {
            closing.closed.swap(true, Ordering::AcqRel);
            closing.sweep(true)
        });
        let release_returned = leaf.release();

        let grown = grow.join().unwrap();
        let close_returned = close.join().unwrap();
        let live = u64::from(grown);
        let parent_returned = close_returned + release_returned;
        let committed = 2 - parent_returned;
        assert!(leaf.closed.load(Ordering::Acquire));
        assert_eq!(committed, live);
        assert_eq!(leaf.free.load(Ordering::Acquire), 0);
        if grown {
            assert_eq!(leaf.release(), 1);
        }
        assert_eq!(leaf.free.load(Ordering::Acquire), 0);
        assert_eq!(parent_returned + live, 2);
    });
}

/// Close can publish CLOSED before acquiring the slow lock. A final release
/// must finish after that lock is handed on, with no nested-lock dependency.
#[test]
fn final_release_completes_behind_slow_close() {
    loom::model(|| {
        let leaf = setup();
        let lease = leaf.try_grow(2).unwrap();
        let slow = leaf.lock_slow_for_test();
        let closing = leaf.clone();
        let close = loom::thread::spawn(move || closing.close());
        while !leaf.snapshot().closed {
            loom::thread::yield_now();
        }
        let release = loom::thread::spawn(move || drop(lease));
        drop(slow);
        close.join().unwrap();
        release.join().unwrap();
        let snapshot = leaf.snapshot();
        assert_eq!(snapshot.live_bytes, 0);
        assert_eq!(snapshot.free_bytes, 0);
        assert_eq!(snapshot.committed_bytes, 0);
    });
}

#[test]
fn close_after_final_release_returns_every_idle_byte() {
    loom::model(|| {
        let leaf = setup();
        drop(leaf.try_grow(2).unwrap());
        assert_eq!(leaf.snapshot().free_bytes, 2);
        let outcome = leaf.close();
        assert_eq!(outcome.reclaimed_bytes, 2);
        assert_eq!(leaf.snapshot().committed_bytes, 0);
    });
}

#[test]
fn concurrent_idle_returns_cannot_spend_the_same_above_floor_commitment() {
    loom::model(|| {
        let mut config = AuthorityConfig::new(8, 4, 4);
        config.top_up = TopUpPolicy::uniform(4);
        let authority = MemoryAuthority::new(config).unwrap();
        let sponsor = authority
            .create_account(AccountKind::Work, ExternalRef::from_u128(1))
            .unwrap();
        let grant = sponsor.request_grant(4).unwrap();
        sponsor.set_floor(2);
        drop(grant);
        assert_eq!(sponsor.committed_bytes(), 4);

        let first = sponsor.clone();
        let first_return = loom::thread::spawn(move || first.shrink_idle(2).reclaimed_bytes);
        let second = sponsor.clone();
        let second_return = loom::thread::spawn(move || second.shrink_idle(2).reclaimed_bytes);
        let reclaimed = first_return.join().unwrap() + second_return.join().unwrap();

        assert_eq!(reclaimed, 2);
        assert_eq!(sponsor.committed_bytes(), 2);
        assert_eq!(sponsor.local_free_bytes(), 2);
        assert!(authority.snapshot().honours_capacity_bound());
    });
}

#[test]
fn revoke_competes_with_growth_for_the_same_free_byte() {
    loom::model(|| {
        let leaf = setup();
        let original = leaf.try_grow(1).unwrap();
        let growing = leaf.clone();
        let grow = loom::thread::spawn(move || growing.try_grow(1).ok());
        let revoking = leaf.clone();
        let revoke = loom::thread::spawn(move || revoking.revoke());
        let grown = grow.join().unwrap();
        revoke.join().unwrap();
        let snapshot = leaf.snapshot();
        assert!(snapshot.closed);
        assert_eq!(snapshot.live_bytes, 1 + u64::from(grown.is_some()));
        assert_eq!(snapshot.committed_bytes, snapshot.live_bytes);
        drop(original);
        drop(grown);
        assert_eq!(leaf.snapshot().committed_bytes, 0);
    });
}

#[test]
fn unwind_of_the_final_debit_keeps_leaf_available_for_cleanup() {
    loom::model(|| {
        let leaf = setup();
        let retained = leaf.try_grow(1).unwrap();
        let unwinding = loom::thread::spawn(move || {
            let result = std::panic::catch_unwind(|| {
                let _retained = retained;
                panic!("modeled retention unwind");
            });
            assert!(result.is_err());
        });
        let trimming = leaf.clone();
        let trim = loom::thread::spawn(move || trimming.trim());
        unwinding.join().unwrap();
        trim.join().unwrap();
        assert_eq!(leaf.snapshot().live_bytes, 0);
        leaf.trim();
        assert_eq!(leaf.snapshot().committed_bytes, 0);
    });
}

/// I1/I3: the parent can admit only one operation. The winning slow grow
/// must own its complete debit before its quantum surplus becomes available.
#[test]
fn competing_slow_growth_cannot_spend_another_requests_delta() {
    loom::model(|| {
        let mut config = AuthorityConfig::new(4, 2, 2);
        config.top_up = TopUpPolicy::uniform(2);
        let authority = MemoryAuthority::new(config).unwrap();
        let sponsor = authority
            .create_account(AccountKind::Work, ExternalRef::from_u128(1))
            .unwrap();
        let leaf = Reservation::new(&sponsor, ExternalRef::from_u128(2)).unwrap();

        let large = leaf.clone();
        let large_grow = loom::thread::spawn(move || large.try_grow(2).ok());
        let small = leaf.clone();
        let small_grow = loom::thread::spawn(move || small.try_grow(1).ok());
        let large_lease = large_grow.join().unwrap();
        let small_lease = small_grow.join().unwrap();
        assert!(large_lease.is_some() ^ small_lease.is_some());
        let snapshot = leaf.snapshot();
        assert_eq!(snapshot.committed_bytes, 2);
        assert_eq!(
            snapshot.live_bytes,
            if large_lease.is_some() { 2 } else { 1 }
        );
        assert!(authority.snapshot().honours_capacity_bound());
        drop(large_lease);
        drop(small_lease);
        leaf.trim();
        assert_eq!(leaf.snapshot().committed_bytes, 0);
    });
}

/// I1/I2/I5: a refused parent top-up must restore F that the slow grow took
/// before asking the parent, even while a trim competes for that same F.
#[test]
fn parent_refusal_and_trim_preserve_the_original_lease() {
    loom::model(|| {
        let mut config = AuthorityConfig::new(4, 2, 2);
        config.top_up = TopUpPolicy::uniform(2);
        let authority = MemoryAuthority::new(config).unwrap();
        let sponsor = authority
            .create_account(AccountKind::Work, ExternalRef::from_u128(1))
            .unwrap();
        let leaf = Reservation::new(&sponsor, ExternalRef::from_u128(2)).unwrap();
        let original = leaf.try_grow(1).unwrap();

        let growing = leaf.clone();
        let refused = loom::thread::spawn(move || growing.try_grow(2));
        let trimming = leaf.clone();
        let trim = loom::thread::spawn(move || trimming.trim());
        assert!(matches!(
            refused.join().unwrap(),
            Err(CapacityError::Denied { .. })
        ));
        trim.join().unwrap();
        let snapshot = leaf.snapshot();
        assert_eq!(snapshot.committed_bytes, 1);
        assert_eq!(snapshot.live_bytes, 1);
        assert_eq!(snapshot.free_bytes, 0);
        assert!(authority.snapshot().honours_capacity_bound());
        drop(original);
        leaf.trim();
        assert_eq!(leaf.snapshot().committed_bytes, 0);
    });
}
