// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Addresses observe original physical owners; they authorize no Work exit.

use super::*;
use novarocks_control_runtime::{
    lease::{CapacityBundle, WorkLease},
    transaction::RuntimeLedger,
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    identity::{DimensionId, Generation},
    scope::{WorkAdmission, WorkKind},
};
use novarocks_types::{AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId};
use std::thread;

fn context(seed: i64) -> QueryContextRef {
    QueryContextRef::new(
        QueryExecutionId::new(QueryId::new(seed, 71), AttemptId::new(1).unwrap()).unwrap(),
        FrontendProcessId::new_v7(),
        BackendProcessId::new_v7(),
    )
}
fn dimension() -> DimensionId {
    DimensionId::new(1).unwrap()
}
fn resources(count: u64) -> ResourceVector {
    ResourceVector::new([(dimension(), count)]).unwrap()
}
struct ScopeOwner {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    work: WorkLease,
    capacity: CapacityBundle,
}
impl ScopeOwner {
    fn new(ledger: &RuntimeLedger) -> Self {
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation, 1, 1).unwrap();
        let (mut work, capacity) = scope
            .reserve_next(
                generation,
                resources(1),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        work.start().unwrap();
        Self {
            ledger: ledger.clone(),
            scope,
            work,
            capacity,
        }
    }
    fn finish(mut self) {
        self.scope.close_external().unwrap();
        // This test's actual body and capture holder end here. Readiness,
        // logical close and failure never supply either physical exit.
        self.work.body_exited().unwrap();
        self.work.captures_exited().unwrap();
        self.ledger.reap_scope_ready(self.scope.id(), 1).unwrap();
        self.capacity.release(&resources(1)).unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
    }
}
fn fixture(
    count: usize,
    positions: usize,
) -> (ActorRuntime<WorkerActor>, RuntimeLedger, ContextReady) {
    let runtime = ActorRuntime::<WorkerActor>::new(1);
    let ledger = RuntimeLedger::new(resources(u64::try_from(count).unwrap()), count, count);
    let index = ContextReady::new(runtime.clone(), NonZeroUsize::new(positions).unwrap());
    (runtime, ledger, index)
}

#[test]
fn a_real_thread_wakes_the_sampled_runtime_and_coalesces_the_original_address() {
    let (runtime, ledger, index) = fixture(1, 1);
    let owner = ScopeOwner::new(&ledger);
    let identity = context(101);
    let notice = index.register(identity, owner.scope.clone()).unwrap();
    let sampled = runtime.change_version();
    let original = notice.clone();
    thread::spawn(move || {
        for _ in 0..16 {
            original.notify();
        }
    })
    .join()
    .unwrap();
    let woke = runtime.change_version() > sampled;
    let selected = index.next_ready().unwrap().unwrap();
    let same_owner = selected.context() == identity && selected.scope() == owner.scope.id();
    let coalesced = index.next_ready().unwrap().is_none();
    selected.notify();
    let remarked = index.next_ready().unwrap().unwrap();
    let mark_after_take_kept = remarked.scope() == owner.scope.id();
    let one_remark = index.next_ready().unwrap().is_none();
    owner.finish();
    notice.unregister_removed().unwrap();
    drop((selected, remarked, notice));
    assert!(ledger.is_empty());
    assert!(runtime.is_empty());
    assert!(woke);
    assert!(same_owner);
    assert!(coalesced);
    assert!(mark_after_take_kept);
    assert!(one_remark);
}

#[test]
fn continuously_remarking_the_smaller_scope_cannot_starve_the_larger_scope() {
    let (runtime, ledger, index) = fixture(2, 2);
    let smaller = ScopeOwner::new(&ledger);
    let larger = ScopeOwner::new(&ledger);
    let first = index.register(context(102), smaller.scope.clone()).unwrap();
    let second = index.register(context(103), larger.scope.clone()).unwrap();
    let ascending = first.scope() < second.scope();
    first.notify();
    let initial = index.next_ready().unwrap().unwrap();
    let initial_is_first = initial.scope() == first.scope();
    first.notify();
    second.notify();
    let mut both_progress = true;
    for _ in 0..16 {
        let selected = index.next_ready().unwrap().unwrap();
        both_progress &= selected.same_origin(&first);
        first.notify(); // Requeue behind the already waiting second owner.
        let selected = index.next_ready().unwrap().unwrap();
        both_progress &= selected.same_origin(&second);
        second.notify();
    }
    let tail_first = index.next_ready().unwrap().unwrap();
    let tail_second = index.next_ready().unwrap().unwrap();
    let tail_order = tail_first.same_origin(&first) && tail_second.same_origin(&second);
    let drained = index.next_ready().unwrap().is_none();
    smaller.finish();
    larger.finish();
    first.unregister_removed().unwrap();
    second.unregister_removed().unwrap();
    drop((initial, tail_first, tail_second, first, second));
    assert!(ledger.is_empty());
    assert!(runtime.is_empty());
    assert!(ascending);
    assert!(initial_is_first);
    assert!(both_progress && tail_order);
    assert!(drained);
}

#[test]
fn stop_or_failure_does_not_free_a_live_slot_and_old_aliases_cannot_mark_its_successor() {
    let (runtime, ledger, index) = fixture(2, 1);
    let original = ScopeOwner::new(&ledger);
    let successor = ScopeOwner::new(&ledger);
    let identity = context(104);
    let notice = index.register(identity, original.scope.clone()).unwrap();
    let live_refused = matches!(
        notice.unregister_removed(),
        Err(ContextReadyError::Runtime(RuntimeError::ScopeNotRetired))
    );
    original.scope.request_stop().unwrap();
    let stopped_refused = matches!(
        notice.unregister_removed(),
        Err(ContextReadyError::Runtime(RuntimeError::ScopeNotRetired))
    );
    original.scope.fail().unwrap();
    let failed_refused = matches!(
        notice.unregister_removed(),
        Err(ContextReadyError::Runtime(RuntimeError::ScopeNotRetired))
    );
    let work_still_live = !original.work.actual_exit_ready().unwrap();
    let original_fee_held = ledger.used(dimension()) == 2;
    let full = matches!(
        index.register(identity, successor.scope.clone()),
        Err(ContextReadyError::Full)
    );
    notice.notify();
    let failed_owner_ready = index.next_ready().unwrap().unwrap();
    let same_failed_owner = failed_owner_ready.scope() == original.scope.id();
    original.finish();
    notice.unregister_removed().unwrap();
    let current = index.register(identity, successor.scope.clone()).unwrap();
    let before_old_mark = runtime.change_version();
    notice.notify();
    let old_did_not_wake = runtime.change_version() == before_old_mark;
    let old_did_not_mark = index.next_ready().unwrap().is_none();
    // Repeated cleanup from the old actual Scope cannot erase a new owner.
    notice.unregister_removed().unwrap();
    current.notify();
    let selected = index.next_ready().unwrap().unwrap();
    let current_selected =
        selected.context() == identity && selected.scope() == successor.scope.id();
    successor.finish();
    current.unregister_removed().unwrap();
    drop((notice, failed_owner_ready, current, selected));
    assert!(ledger.is_empty());
    assert!(runtime.is_empty());
    assert!(live_refused);
    assert!(stopped_refused);
    assert!(failed_refused);
    assert!(work_still_live);
    assert!(original_fee_held);
    assert!(full);
    assert!(same_failed_owner);
    assert!(old_did_not_wake);
    assert!(old_did_not_mark);
    assert!(current_selected);
}

#[test]
fn the_last_metadata_notice_releases_its_address_only_after_actual_scope_removal() {
    let (runtime, ledger, index) = fixture(2, 1);
    let original = ScopeOwner::new(&ledger);
    let successor = ScopeOwner::new(&ledger);
    let identity = context(105);
    let notice = index.register(identity, original.scope.clone()).unwrap();
    notice.notify();
    original.finish();
    drop(notice);
    let old_ready_removed = index.next_ready().unwrap().is_none();
    let current = index.register(identity, successor.scope.clone()).unwrap();
    current.notify();
    let selected = index.next_ready().unwrap().unwrap();
    let new_owner_ready = selected.scope() == successor.scope.id();
    successor.finish();
    drop((selected, current));
    let final_empty = index.next_ready().unwrap().is_none();
    assert!(ledger.is_empty());
    assert!(runtime.is_empty());
    assert!(old_ready_removed);
    assert!(new_owner_ready);
    assert!(final_empty);
}

#[test]
fn losing_a_live_notice_keeps_the_bounded_address_instead_of_reclassifying_its_work() {
    let (runtime, ledger, index) = fixture(2, 1);
    let original = ScopeOwner::new(&ledger);
    let contender = ScopeOwner::new(&ledger);
    let identity = context(106);
    let notice = index.register(identity, original.scope.clone()).unwrap();
    notice.notify();
    drop(notice);
    let owner_lost = matches!(index.next_ready(), Err(ContextReadyError::OwnerLost));
    let no_replacement = matches!(
        index.register(identity, contender.scope.clone()),
        Err(ContextReadyError::Full)
    );
    let still_funded = ledger.used(dimension()) == 2;
    let still_live = !original.work.actual_exit_ready().unwrap();
    // The actual Work was never lost: this test still owns its body/captures.
    // End them for real without presenting the lost index as recovered/S8-clean.
    original.finish();
    contender.finish();
    let no_implicit_recovery = matches!(index.next_ready(), Err(ContextReadyError::OwnerLost));
    assert!(ledger.is_empty());
    assert!(runtime.is_empty());
    assert!(owner_lost);
    assert!(no_replacement);
    assert!(still_funded);
    assert!(still_live);
    assert!(no_implicit_recovery);
}

#[test]
fn an_older_scope_becoming_ready_late_cannot_be_starved_by_real_fresh_scope_churn() {
    let (runtime, ledger, index) = fixture(2, 2);
    let older = ScopeOwner::new(&ledger);
    let old_notice = index.register(context(201), older.scope.clone()).unwrap();
    let first_fresh = ScopeOwner::new(&ledger);
    let fresh_notice = index
        .register(context(202), first_fresh.scope.clone())
        .unwrap();
    fresh_notice.notify();
    let passed_old_address = index.next_ready().unwrap().unwrap();
    let first_fresh_served = passed_old_address.same_origin(&fresh_notice);
    old_notice.notify();
    first_fresh.finish();
    fresh_notice.unregister_removed().unwrap();
    drop((passed_old_address, fresh_notice));

    let mut old_always_first = true;
    let mut fresh_always_second = true;
    let mut increasing_scopes = true;
    let mut previous = older.scope.id();
    for seed in 203..211 {
        // The shared index has only two actual positions. Every new scope
        // is genuinely admitted, exited, removed and unregistered before
        // another fresh owner is constructed; no fake retirement is used.
        let fresh = ScopeOwner::new(&ledger);
        let notice = index.register(context(seed), fresh.scope.clone()).unwrap();
        increasing_scopes &= notice.scope() > previous;
        previous = notice.scope();
        notice.notify();
        let selected_old = index.next_ready().unwrap().unwrap();
        old_always_first &= selected_old.same_origin(&old_notice);
        old_notice.notify();
        let selected_fresh = index.next_ready().unwrap().unwrap();
        fresh_always_second &= selected_fresh.same_origin(&notice);
        fresh.finish();
        notice.unregister_removed().unwrap();
        drop((selected_old, selected_fresh, notice));
    }
    let final_old = index.next_ready().unwrap();
    let old_remark_retained = final_old
        .as_ref()
        .is_some_and(|notice| notice.same_origin(&old_notice));
    older.finish();
    old_notice.unregister_removed().unwrap();
    drop((final_old, old_notice));
    let drained = index.next_ready().unwrap().is_none();
    assert!(ledger.is_empty());
    assert!(runtime.is_empty());
    assert!(first_fresh_served && increasing_scopes);
    assert!(old_always_first && fresh_always_second && old_remark_retained);
    assert!(drained);
}
