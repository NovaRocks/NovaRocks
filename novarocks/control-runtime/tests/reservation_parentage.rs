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
    lease::{ReservationBundle, WorkLease},
    transaction::{RuntimeError, RuntimeLedger},
};
use novarocks_control_runtime_core::{
    capacity::{CapacityError, ReservationResources, ResourceVector},
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
fn release(mut capacity: ReservationBundle) {
    let resources = capacity.resources().unwrap();
    capacity.release_quota(&resources.quota).unwrap();
    capacity.finish().unwrap();
}
fn exit(work: &mut WorkLease) {
    work.body_exited().unwrap();
    work.captures_exited().unwrap();
}

#[test]
fn bound_reservation_derives_from_the_actual_parent_after_external_close_but_not_after_parent_exit()
{
    let ledger = RuntimeLedger::new(ResourceVector::new([(dimension(), 2)]).unwrap());
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let (mut parent, parent_capacity) = scope
        .reserve_next(
            generation(),
            ReservationResources::for_work(resources()),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    parent.start().unwrap();
    ledger.close_admission();
    scope.close_external().unwrap();
    let (mut child, child_capacity) = scope
        .reserve_child_next(
            generation(),
            parent_capacity.id(),
            ReservationResources::for_work(resources()),
            WorkKind::Business,
        )
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    child.start().unwrap();
    exit(&mut parent);
    assert!(!parent.actual_exit_ready().unwrap());
    assert!(matches!(
        scope.reserve_child_next(
            generation(),
            parent_capacity.id(),
            ReservationResources::work_only(),
            WorkKind::Cleanup
        ),
        Err(RuntimeError::Capacity(CapacityError::Scope(
            ScopeError::InvalidTransition
        )))
    ));
    exit(&mut child);
    assert_eq!(ledger.reap_scope_ready(scope.id(), 2).unwrap(), 2);
    assert!(parent.actual_exit_ready().unwrap());
    release(child_capacity);
    release(parent_capacity);
    ledger.remove_scope(scope.id()).unwrap();
    assert!(ledger.is_empty());
}

#[test]
fn child_resolution_requires_a_bound_exact_owner_and_follows_actual_bundle_transfer() {
    let ledger = RuntimeLedger::new(ResourceVector::new([(dimension(), 2)]).unwrap());
    let a = ledger.open_scope_next(None, generation()).unwrap();
    let b = ledger.open_scope_next(None, generation()).unwrap();
    let guard = a
        .reserve_next(
            generation(),
            ReservationResources::for_work(resources()),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap();
    let parent_id = guard.reservation_id();
    assert!(matches!(
        a.reserve_child_next(
            generation(),
            parent_id,
            ReservationResources::for_work(resources()),
            WorkKind::Business
        ),
        Err(RuntimeError::Capacity(CapacityError::UnboundReservation))
    ));
    assert_eq!(ledger.used(dimension()), 1);
    let (mut aw, mut parent_capacity) = guard.bind(WorkKind::Business).unwrap();
    // Only this actual unused quota parcel moves. The original bound Work
    // header/primary carrier stay with aw and retire through its real exit.
    let mut capacity = parent_capacity
        .split_next(&ReservationResources::new(resources(), Default::default()))
        .unwrap();
    let parent_id = capacity.id();
    let (mut bw, bc) = b
        .reserve_next(
            generation(),
            ReservationResources::work_only(),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    assert!(matches!(
        b.reserve_child_next(
            generation(),
            parent_id,
            ReservationResources::for_work(resources()),
            WorkKind::Business
        ),
        Err(RuntimeError::Capacity(CapacityError::WrongOwner))
    ));
    capacity.transfer(&bw).unwrap();
    assert!(matches!(
        a.reserve_child_next(
            generation(),
            parent_id,
            ReservationResources::for_work(resources()),
            WorkKind::Business
        ),
        Err(RuntimeError::Capacity(CapacityError::WrongOwner))
    ));
    let generation = b.fail().unwrap();
    assert!(matches!(
        b.reserve_child_next(
            generation,
            parent_id,
            ReservationResources::for_work(resources()),
            WorkKind::Business
        ),
        Err(RuntimeError::Capacity(CapacityError::Scope(
            ScopeError::Stopped
        )))
    ));
    let (mut child, cc) = b
        .reserve_child_next(
            generation,
            parent_id,
            ReservationResources::for_work(resources()),
            WorkKind::Cleanup,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    child.start().unwrap();
    exit(&mut aw);
    exit(&mut bw);
    assert!(!bw.actual_exit_ready().unwrap());
    exit(&mut child);
    ledger.reap_scope_ready(a.id(), 4).unwrap();
    ledger.reap_scope_ready(b.id(), 4).unwrap();
    assert!(bw.actual_exit_ready().unwrap());
    release(parent_capacity);
    release(capacity);
    release(bc);
    release(cc);
    a.close_external().unwrap();
    b.close_external().unwrap();
    ledger.remove_scope(a.id()).unwrap();
    ledger.remove_scope(b.id()).unwrap();
    assert!(ledger.is_empty());
}

#[test]
fn rollback_of_the_last_unbound_child_requeues_its_already_exited_parent() {
    let ledger = RuntimeLedger::new(ResourceVector::new([(dimension(), 2)]).unwrap());
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let (mut parent, capacity) = scope
        .reserve_next(
            generation(),
            ReservationResources::for_work(resources()),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    let child = scope
        .reserve_child_next(
            generation(),
            capacity.id(),
            ReservationResources::for_work(resources()),
            WorkKind::Business,
        )
        .unwrap();
    exit(&mut parent);
    assert_eq!(ledger.reap_scope_ready(scope.id(), 1).unwrap(), 0);
    drop(child); // A pure step unwound before any payload/capture bound.
    assert_eq!(ledger.used(dimension()), 1);
    assert_eq!(ledger.reap_scope_ready(scope.id(), 1).unwrap(), 1);
    release(capacity);
    scope.close_external().unwrap();
    ledger.remove_scope(scope.id()).unwrap();
    assert!(ledger.is_empty());
}
