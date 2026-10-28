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

use novarocks_control_runtime::{
    lease::{CapacityBundle, WorkLease},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{CapacityError, ResourceVector},
    identity::{DimensionId, Generation},
    scope::{ScopeError, WorkAdmission, WorkKind},
};

fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn dimension() -> DimensionId {
    DimensionId::new(1).unwrap()
}
fn resources() -> ResourceVector {
    ResourceVector::new([(dimension(), 1)]).unwrap()
}
fn parent(
    reservations: usize,
    scopes: usize,
    parent_work: usize,
) -> (RuntimeLedger, ScopeHandle, WorkLease, CapacityBundle) {
    let ledger = RuntimeLedger::new(resources(), reservations, scopes);
    let scope = ledger
        .open_scope_next(None, generation(), parent_work, 4)
        .unwrap();
    let (mut work, capacity) = scope
        .reserve_next(
            generation(),
            resources(),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    work.start().unwrap();
    (ledger, scope, work, capacity)
}
fn release(mut capacity: CapacityBundle) {
    capacity.release(&capacity.resources().unwrap()).unwrap();
}
fn exit(work: &mut WorkLease) {
    work.body_exited().unwrap();
    work.captures_exited().unwrap();
}
fn retire_child(
    ledger: &RuntimeLedger,
    scope: ScopeHandle,
    mut work: WorkLease,
    capacity: CapacityBundle,
) {
    scope.close_external().unwrap();
    exit(&mut work);
    ledger.reap_scope_ready(scope.id(), 8).unwrap();
    release(capacity);
    assert!(scope.snapshot().unwrap().retirement_ready);
    ledger.remove_scope(scope.id()).unwrap();
    // Removing the complete child is the actual parent-link exit. Reaping
    // that link is separate from the original parent's body/capture exit.
    ledger.reap_ready(8).unwrap();
}
fn retire_parent(
    ledger: &RuntimeLedger,
    scope: ScopeHandle,
    mut work: WorkLease,
    capacity: CapacityBundle,
) {
    scope.close_external().unwrap();
    exit(&mut work);
    ledger.reap_scope_ready(scope.id(), 8).unwrap();
    release(capacity);
    ledger.remove_scope(scope.id()).unwrap();
    assert!(ledger.is_empty());
}

#[test]
fn original_parent_bootstraps_a_real_root_after_external_close_without_recharging_capacity() {
    let (ledger, scope, parent, funding) = parent(8, 2, 4);
    ledger.close_admission();
    scope.close_external().unwrap();
    let (child, guard) = ledger
        .open_child_scope_with_root(&parent, generation(), 4, 4, WorkKind::Business)
        .unwrap();
    assert_ne!(child.id(), scope.id());
    assert!(!child.gates().unwrap().external_open);
    assert_eq!(ledger.used(dimension()), 1);
    assert_eq!(scope.snapshot().unwrap().work, 2);
    let (mut root, root_capacity) = guard.bind(WorkKind::Business).unwrap();
    assert!(root_capacity.resources().unwrap().is_empty());
    root.start().unwrap();
    let (mut timer, timer_capacity) = child
        .reserve_child_next(
            generation(),
            root_capacity.id(),
            ResourceVector::default(),
            WorkKind::Step,
        )
        .unwrap()
        .bind(WorkKind::Step)
        .unwrap();
    timer.start().unwrap();
    assert_eq!(ledger.used(dimension()), 1);
    assert!(matches!(
        child.reserve_next(
            generation(),
            ResourceVector::default(),
            WorkKind::Business,
            WorkAdmission::External,
        ),
        Err(RuntimeError::Scope(ScopeError::Closed))
    ));
    exit(&mut timer);
    ledger.reap_scope_ready(child.id(), 8).unwrap();
    release(timer_capacity);
    retire_child(&ledger, child, root, root_capacity);
    assert_eq!(ledger.used(dimension()), 1);
    retire_parent(&ledger, scope, parent, funding);
}

#[test]
fn child_scope_and_real_link_keep_an_exited_original_parent_live_until_actual_child_removal() {
    let (ledger, scope, mut parent, funding) = parent(8, 2, 4);
    let (child, guard) = ledger
        .open_child_scope_with_root(&parent, generation(), 2, 2, WorkKind::Business)
        .unwrap();
    let (mut root, root_capacity) = guard.bind(WorkKind::Business).unwrap();
    root.start().unwrap();
    scope.close_external().unwrap();
    exit(&mut parent);
    assert!(!parent.actual_exit_ready().unwrap());
    assert_eq!(ledger.reap_scope_ready(scope.id(), 8).unwrap(), 0);
    let retired = parent.retirement_observer();
    retire_child(&ledger, child, root, root_capacity);
    assert!(retired.is_retired());
    assert!(parent.actual_exit_ready().unwrap());
    release(funding);
    ledger.remove_scope(scope.id()).unwrap();
    assert!(ledger.is_empty());
}

#[test]
fn stopped_parent_only_bootstraps_cleanup_and_child_cannot_derive_business() {
    let (ledger, scope, parent, funding) = parent(8, 2, 4);
    scope.request_stop().unwrap();
    assert!(matches!(
        ledger.open_child_scope_with_root(&parent, generation(), 2, 2, WorkKind::Business),
        Err(RuntimeError::Scope(ScopeError::Stopped))
    ));
    assert_eq!(scope.snapshot().unwrap().work, 1);
    let (child, guard) = ledger
        .open_child_scope_with_root(&parent, generation(), 2, 2, WorkKind::Cleanup)
        .unwrap();
    let gates = child.gates().unwrap();
    assert!(!gates.external_open);
    assert!(gates.stopped);
    assert!(!gates.failed);
    let (mut root, root_capacity) = guard.bind(WorkKind::Cleanup).unwrap();
    root.start().unwrap();
    assert!(matches!(
        child.reserve_child_next(
            gates.generation,
            root_capacity.id(),
            ResourceVector::default(),
            WorkKind::Business,
        ),
        Err(RuntimeError::Capacity(CapacityError::Scope(
            ScopeError::Stopped
        )))
    ));
    retire_child(&ledger, child, root, root_capacity);
    retire_parent(&ledger, scope, parent, funding);
}

#[test]
fn failed_parent_fences_child_generation_and_still_allows_original_cleanup_root() {
    let (ledger, scope, parent, funding) = parent(8, 2, 4);
    scope.fail().unwrap();
    assert!(matches!(
        ledger.open_child_scope_with_root(&parent, generation(), 2, 2, WorkKind::Business),
        Err(RuntimeError::Scope(ScopeError::Stopped))
    ));
    let initial = Generation::new(7).unwrap();
    let (child, guard) = ledger
        .open_child_scope_with_root(&parent, initial, 2, 2, WorkKind::Cleanup)
        .unwrap();
    let gates = child.gates().unwrap();
    assert_eq!(gates.generation, initial.next().unwrap());
    assert!(!gates.external_open);
    assert!(gates.stopped && gates.failed);
    let (mut root, root_capacity) = guard.bind(WorkKind::Cleanup).unwrap();
    root.start_in_generation(gates.generation).unwrap();
    assert!(matches!(
        child.reserve_child_next(
            gates.generation,
            root_capacity.id(),
            ResourceVector::default(),
            WorkKind::Business,
        ),
        Err(RuntimeError::Capacity(CapacityError::Scope(
            ScopeError::Stopped
        )))
    ));
    retire_child(&ledger, child, root, root_capacity);
    retire_parent(&ledger, scope, parent, funding);
}

#[test]
fn root_reservation_refusal_leaves_no_child_or_parent_link_and_spends_no_new_identity() {
    let (ledger, scope, parent, funding) = parent(2, 2, 4);
    let filler = scope
        .reserve_next(
            generation(),
            ResourceVector::default(),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap();
    let previous = filler.reservation_id();
    let original = scope.snapshot().unwrap();
    assert!(matches!(
        ledger.open_child_scope_with_root(&parent, generation(), 2, 2, WorkKind::Business),
        Err(RuntimeError::Capacity(CapacityError::Scope(
            ScopeError::SlotCapacity
        )))
    ));
    assert_eq!(scope.snapshot().unwrap(), original);
    assert_eq!(ledger.used(dimension()), 1);
    assert!(ledger.abandoned_bundles().is_empty());
    // This is a real unused guard rollback, never release of a running permit.
    drop(filler);
    let (child, guard) = ledger
        .open_child_scope_with_root(&parent, generation(), 2, 2, WorkKind::Business)
        .unwrap();
    assert_eq!(child.id().get(), scope.id().get() + 1);
    assert_eq!(guard.reservation_id().get(), previous.get() + 1);
    let (root, root_capacity) = guard.bind(WorkKind::Business).unwrap();
    retire_child(&ledger, child, root, root_capacity);
    retire_parent(&ledger, scope, parent, funding);
}

#[test]
fn child_root_work_capacity_refusal_is_atomic_and_can_retry_with_the_same_next_identities() {
    let (ledger, scope, parent, funding) = parent(4, 2, 4);
    let original = scope.snapshot().unwrap();
    assert!(matches!(
        ledger.open_child_scope_with_root(&parent, generation(), 0, 2, WorkKind::Business),
        Err(RuntimeError::Capacity(CapacityError::Scope(
            ScopeError::WorkCapacity
        )))
    ));
    assert_eq!(scope.snapshot().unwrap(), original);
    let (child, guard) = ledger
        .open_child_scope_with_root(&parent, generation(), 2, 2, WorkKind::Business)
        .unwrap();
    assert_eq!(child.id().get(), scope.id().get() + 1);
    assert_eq!(guard.reservation_id().get(), funding.id().get() + 1);
    let (root, root_capacity) = guard.bind(WorkKind::Business).unwrap();
    retire_child(&ledger, child, root, root_capacity);
    retire_parent(&ledger, scope, parent, funding);
}

#[test]
fn parent_link_work_capacity_refusal_does_not_publish_a_child() {
    let (ledger, scope, parent, funding) = parent(4, 2, 1);
    let original = scope.snapshot().unwrap();
    assert!(matches!(
        ledger.open_child_scope_with_root(&parent, generation(), 2, 2, WorkKind::Business),
        Err(RuntimeError::Scope(ScopeError::WorkCapacity))
    ));
    assert_eq!(scope.snapshot().unwrap(), original);
    assert_eq!(ledger.used(dimension()), 1);
    assert!(ledger.abandoned_bundles().is_empty());
    retire_parent(&ledger, scope, parent, funding);
}

#[test]
fn scope_capacity_refusal_preserves_original_parent_and_funding() {
    let (ledger, scope, parent, funding) = parent(4, 1, 4);
    let original = scope.snapshot().unwrap();
    assert!(matches!(
        ledger.open_child_scope_with_root(&parent, generation(), 2, 2, WorkKind::Business),
        Err(RuntimeError::ScopeCapacity)
    ));
    assert_eq!(scope.snapshot().unwrap(), original);
    assert_eq!(ledger.used(dimension()), 1);
    retire_parent(&ledger, scope, parent, funding);
}

#[test]
fn foreign_or_exited_parent_has_no_child_admission_authority() {
    let (ledger, scope, mut parent, funding) = parent(4, 2, 4);
    let foreign = RuntimeLedger::new(resources(), 4, 2);
    assert!(matches!(
        foreign.open_child_scope_with_root(&parent, generation(), 2, 2, WorkKind::Business),
        Err(RuntimeError::Capacity(CapacityError::WrongOwner))
    ));
    assert!(foreign.is_empty());
    parent.body_exited().unwrap();
    assert!(matches!(
        ledger.open_child_scope_with_root(&parent, generation(), 2, 2, WorkKind::Cleanup),
        Err(RuntimeError::Scope(ScopeError::InvalidTransition))
    ));
    assert_eq!(scope.snapshot().unwrap().work, 1);
    parent.captures_exited().unwrap();
    scope.close_external().unwrap();
    ledger.reap_scope_ready(scope.id(), 8).unwrap();
    release(funding);
    ledger.remove_scope(scope.id()).unwrap();
    assert!(ledger.is_empty());
}

#[test]
fn inherited_failure_generation_exhaustion_cannot_leave_a_root_reservation() {
    let (ledger, scope, parent, funding) = parent(4, 2, 4);
    scope.fail().unwrap();
    let original = scope.snapshot().unwrap();
    assert!(matches!(
        ledger.open_child_scope_with_root(
            &parent,
            Generation::new(u64::MAX).unwrap(),
            2,
            2,
            WorkKind::Cleanup,
        ),
        Err(RuntimeError::Scope(ScopeError::IdentityExhausted))
    ));
    assert_eq!(scope.snapshot().unwrap(), original);
    let (child, guard) = ledger
        .open_child_scope_with_root(&parent, generation(), 2, 2, WorkKind::Cleanup)
        .unwrap();
    assert_eq!(child.id().get(), scope.id().get() + 1);
    assert_eq!(guard.reservation_id().get(), funding.id().get() + 1);
    let (root, root_capacity) = guard.bind(WorkKind::Cleanup).unwrap();
    retire_child(&ledger, child, root, root_capacity);
    retire_parent(&ledger, scope, parent, funding);
}

#[test]
fn unused_bootstrap_root_rolls_back_but_scope_link_waits_for_real_scope_removal() {
    let (ledger, scope, parent, funding) = parent(4, 2, 4);
    let (child, guard) = ledger
        .open_child_scope_with_root(&parent, generation(), 2, 2, WorkKind::Business)
        .unwrap();
    drop(guard);
    assert_eq!(child.snapshot().unwrap().work, 0);
    assert_eq!(scope.snapshot().unwrap().work, 2);
    assert_eq!(ledger.used(dimension()), 1);
    child.close_external().unwrap();
    ledger.remove_scope(child.id()).unwrap();
    assert_eq!(ledger.reap_scope_ready(scope.id(), 8).unwrap(), 1);
    assert_eq!(scope.snapshot().unwrap().work, 1);
    retire_parent(&ledger, scope, parent, funding);
}

#[test]
fn inherited_stop_fence_rejects_real_descendant_business_admission() {
    let (ledger, scope, parent, funding) = parent(8, 2, 4);
    scope.request_stop().unwrap();
    let (child, guard) = ledger
        .open_child_scope_with_root(&parent, generation(), 2, 2, WorkKind::Cleanup)
        .unwrap();
    let (mut root, root_capacity) = guard.bind(WorkKind::Cleanup).unwrap();
    root.start().unwrap();
    let result = child.reserve_child_next(
        child.gates().unwrap().generation,
        root_capacity.id(),
        ResourceVector::default(),
        WorkKind::Business,
    );
    let refused = matches!(
        result,
        Err(RuntimeError::Capacity(CapacityError::Scope(
            ScopeError::Stopped
        )))
    );
    // Even a broken fence's unexpected pure guard is disposed before the
    // final assertion, so the negative control observes real admission.
    drop(result);
    retire_child(&ledger, child, root, root_capacity);
    retire_parent(&ledger, scope, parent, funding);
    assert!(
        refused,
        "a cleanup bootstrap cannot reopen Business admission"
    );
}

#[test]
fn concurrent_bootstraps_and_external_close_publish_complete_distinct_children() {
    let (ledger, scope, parent, funding) = parent(3, 3, 4);
    let gate = std::sync::Barrier::new(3);
    let children = std::thread::scope(|threads| {
        let first = threads.spawn(|| {
            gate.wait();
            ledger.open_child_scope_with_root(&parent, generation(), 2, 2, WorkKind::Business)
        });
        let second = threads.spawn(|| {
            gate.wait();
            ledger.open_child_scope_with_root(&parent, generation(), 2, 2, WorkKind::Business)
        });
        gate.wait();
        ledger.close_admission();
        [
            first.join().unwrap().unwrap(),
            second.join().unwrap().unwrap(),
        ]
    });
    assert_ne!(children[0].0.id(), children[1].0.id());
    assert_ne!(
        children[0].1.reservation_id(),
        children[1].1.reservation_id()
    );
    assert_eq!(scope.snapshot().unwrap().work, 3);
    assert_eq!(ledger.used(dimension()), 1);
    for (child, guard) in children {
        assert_eq!(child.snapshot().unwrap().work, 1);
        assert!(!child.gates().unwrap().external_open);
        let (root, root_capacity) = guard.bind(WorkKind::Business).unwrap();
        retire_child(&ledger, child, root, root_capacity);
    }
    retire_parent(&ledger, scope, parent, funding);
}
