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

#![cfg(not(loom))]
use novarocks_memory::RequestOutcome;
use novarocks_memory::attribution::scope::{
    AmbientEntryObservation, AmbientExitObservation, AmbientStepObservation,
};
use novarocks_memory::attribution::{AttributingAllocator, binding};
use novarocks_memory::lane::{RecordRef, global_store};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::ptr::NonNull;

// Explicit test authority configuration, matching Memory's existing scope
// fixtures. This is not a native default policy or a production wallet.
fn authority(bytes: u64) -> novarocks_memory::MemoryAuthority {
    let mut config = novarocks_memory::AuthorityConfig::new(bytes * 2, bytes, bytes);
    config.max_accounts = 32;
    config.metadata_budget_bytes = 16 * 1024;
    config.max_active_owners = 4;
    config.top_up = novarocks_memory::TopUpPolicy::uniform(1024);
    novarocks_memory::MemoryAuthority::new(config).unwrap()
}
fn work(authority: &novarocks_memory::MemoryAuthority) -> novarocks_memory::Account {
    authority
        .create_account(
            novarocks_memory::AccountKind::Work,
            novarocks_memory::ExternalRef::NONE,
        )
        .unwrap()
}

fn layout() -> Layout {
    Layout::from_size_align(600, 8).unwrap()
}
fn allocate(a: &AttributingAllocator<System>) -> NonNull<u8> {
    // SAFETY: the genuine test allocator receives a valid nonzero layout.
    NonNull::new(unsafe { a.alloc(layout()) }).unwrap()
}
fn origin(p: NonNull<u8>) -> RecordRef {
    // SAFETY: the wrapped allocator initialized the real 8-byte token tail.
    unsafe { RecordRef::read(p.as_ptr().add(600)) }
}
fn release(a: &AttributingAllocator<System>, p: NonNull<u8>) {
    // SAFETY: one real block returned by this allocator with this exact layout.
    unsafe { a.dealloc(p.as_ptr(), layout()) }
}

#[test]
fn observed_ready_domain_has_one_real_tagged_fact_and_preserves_return() {
    let authority = authority(65_536);
    let account = work(&authority);
    // This is the actual layout-derived fact size of this one test block,
    // not an envelope for an Arrow operation or a kernel's hidden scratch.
    let RequestOutcome::Granted(domain) = authority.request_domain(&account, 608, 128) else {
        panic!("the real known block request must be granted");
    };
    let lease = domain.activate(608, 0).unwrap();
    let allocator = AttributingAllocator::new(System);
    let mut facts = AmbientStepObservation::default();
    let (block, value) = domain.lane().run_observed(&mut facts, || {
        let block = allocate(&allocator);
        assert_eq!(origin(block), domain.lane().reference());
        (block, 41)
    });
    assert_eq!(value, 41);
    assert_eq!(facts.entry(), AmbientEntryObservation::Bound);
    assert_eq!(facts.exit(), AmbientExitObservation::Restored);
    assert_eq!(binding::pending_bytes(), 0);
    let lane = global_store()
        .snapshot_ref(domain.lane().reference())
        .unwrap();
    assert_eq!((lane.tagged_bytes, lane.outstanding), (608, 1));
    let receipt = lease.finish();
    assert_eq!(receipt.accepted_live, 608);
    assert!(receipt.next_step.is_ok());
    release(&allocator, block);
    assert_eq!(domain.snapshot().live, 0);
    domain.stop_producing().unwrap();
}

#[test]
fn observed_sealed_entry_still_runs_body_under_actual_outer_origin() {
    let authority = authority(65_536);
    let account = work(&authority);
    let outer = account.create_lane_shards();
    let sealed = account.create_domain(1).unwrap();
    sealed.seal();
    let allocator = AttributingAllocator::new(System);
    let mut facts = AmbientStepObservation::default();
    let calls = Cell::new(0);
    let value = outer.run(|| {
        sealed.lane().run_observed(&mut facts, || {
            calls.set(calls.get() + 1);
            let block = allocate(&allocator);
            assert_eq!(origin(block), outer.explicit_owner().reference().unwrap());
            release(&allocator, block);
            73
        })
    });
    assert_eq!(value, 73);
    assert_eq!(calls.get(), 1);
    assert_eq!(facts.entry(), AmbientEntryObservation::LaneEntryRefused);
    assert_eq!(facts.exit(), AmbientExitObservation::EntryWasUnbound);
    assert_eq!(binding::pending_bytes(), 0);
}

#[test]
fn observed_body_panic_retains_facts_and_restores_actual_outer_origin() {
    let authority = authority(65_536);
    let account = work(&authority);
    let outer = account.create_lane_shards();
    let inner = account.create_lane_shards();
    let allocator = AttributingAllocator::new(System);
    let mut facts = novarocks_memory::attribution::scope::OwnerStepObservation::default();
    outer.run(|| {
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            inner.run_observed(&mut facts, || {
                let block = allocate(&allocator);
                assert_eq!(origin(block), inner.explicit_owner().reference().unwrap());
                release(&allocator, block);
                panic!("original synchronous body panic");
            });
        }));
        let payload = panic.expect_err("the original panic must propagate");
        assert_eq!(
            payload.downcast_ref::<&str>(),
            Some(&"original synchronous body panic")
        );
        assert_eq!(
            facts.entry(),
            novarocks_memory::attribution::scope::OwnerEntryObservation::Bound
        );
        assert_eq!(facts.exit(), AmbientExitObservation::Restored);
        let block = allocate(&allocator);
        assert_eq!(origin(block), outer.explicit_owner().reference().unwrap());
        release(&allocator, block);
    });
    assert_eq!(binding::pending_bytes(), 0);
    inner.stop_producing().unwrap();
}

#[test]
fn observed_reuse_resets_prior_unbound_fact_without_process_sampling() {
    let authority = authority(65_536);
    let account = work(&authority);
    let sealed = account.create_domain(1).unwrap();
    let ready = account.create_domain(1).unwrap();
    sealed.seal();
    let mut facts = AmbientStepObservation::default();
    sealed.lane().run_observed(&mut facts, || ());
    let first = facts;
    ready.lane().run_observed(&mut facts, || ());
    assert_eq!(first.entry(), AmbientEntryObservation::LaneEntryRefused);
    assert_eq!(first.exit(), AmbientExitObservation::EntryWasUnbound);
    assert_eq!(facts.entry(), AmbientEntryObservation::Bound);
    assert_eq!(facts.exit(), AmbientExitObservation::Restored);
}

#[test]
fn observed_granted_scope_does_not_mint_a_second_writer_for_domain_clone() {
    let authority = authority(65_536);
    let account = work(&authority);
    let RequestOutcome::Granted(domain) = authority.request_domain(&account, 608, 128) else {
        panic!("the real request must be granted");
    };
    let alias = domain.clone();
    let lease = domain.activate(608, 0).unwrap();
    let refusal = alias
        .activate(608, 0)
        .expect_err("one real domain has one writer");
    assert!(matches!(
        refusal,
        novarocks_memory::CapacityError::Invalid {
            detail: "domain already has an active allocation writer"
        }
    ));
    let mut facts = AmbientStepObservation::default();
    assert_eq!(domain.lane().run_observed(&mut facts, || 19), 19);
    assert_eq!(facts.exit(), AmbientExitObservation::Restored);
    assert!(lease.finish().next_step.is_ok());
    domain.stop_producing().unwrap();
}

// Test-only proof of the host's required guard ordering. This does not
// install a driver runner or supply an allocation envelope for a kernel.
#[derive(Default)]
struct SettlementObservation {
    receipt: Option<novarocks_memory::StepReceipt>,
    stopped: Option<Result<(), novarocks_memory::TeardownError>>,
}
struct SettleOnDrop<'a> {
    funding: Option<novarocks_memory::FundingScope>,
    domain: &'a novarocks_memory::FundingDomain,
    journal: &'a mut SettlementObservation,
}
impl Drop for SettleOnDrop<'_> {
    fn drop(&mut self) {
        if let Some(lease) = self.funding.take() {
            self.journal.receipt = Some(lease.finish());
            self.journal.stopped = Some(self.domain.stop_producing());
        }
    }
}

#[test]
fn observed_ready_unwind_settles_after_restore_without_replaying_body() {
    let authority = authority(65_536);
    let account = work(&authority);
    let RequestOutcome::Granted(domain) = authority.request_domain(&account, 608, 128) else {
        panic!("the real request must be granted");
    };
    let lease = domain.activate(608, 0).unwrap();
    let allocator = AttributingAllocator::new(System);
    let mut coverage = AmbientStepObservation::default();
    let mut settlement = SettlementObservation::default();
    let calls = Cell::new(0);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _settler = SettleOnDrop {
            funding: Some(lease),
            domain: &domain,
            journal: &mut settlement,
        };
        domain.lane().run_observed(&mut coverage, || {
            calls.set(calls.get() + 1);
            let block = allocate(&allocator);
            release(&allocator, block);
            panic!("original granted body panic");
        });
    }));
    assert!(panic.is_err());
    assert_eq!(calls.get(), 1);
    assert_eq!(coverage.exit(), AmbientExitObservation::Restored);
    let receipt = settlement.receipt.as_ref().unwrap();
    assert_eq!(receipt.accepted_live, 0);
    assert!(receipt.next_step.is_ok());
    assert_eq!(settlement.stopped, Some(Ok(())));
    assert!(!domain.snapshot().active);
    assert!(domain.snapshot().sealed);
}

#[test]
fn observed_ready_body_error_and_real_closed_settlement_remain_distinct() {
    let authority = authority(65_536);
    let account = work(&authority);
    let RequestOutcome::Granted(domain) = authority.request_domain(&account, 608, 128) else {
        panic!("the real request must be granted");
    };
    let lease = domain.activate(608, 0).unwrap();
    let mut coverage = AmbientStepObservation::default();
    let mut settlement = SettlementObservation::default();
    let result = {
        let _settler = SettleOnDrop {
            funding: Some(lease),
            domain: &domain,
            journal: &mut settlement,
        };
        domain.lane().run_observed(&mut coverage, || {
            domain.seal();
            Err::<(), _>("original body error")
        })
    };
    assert_eq!(result, Err("original body error"));
    assert_eq!(coverage.exit(), AmbientExitObservation::Restored);
    let receipt = settlement.receipt.as_ref().unwrap();
    assert_eq!(
        receipt.next_step,
        Err(novarocks_memory::CapacityError::Closed {
            account: account.id()
        })
    );
    assert_eq!(settlement.stopped, Some(Ok(())));
}
