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

#![cfg(not(loom))]

use novarocks_control_runtime::{
    capacity_notification::CapacityRegistrant,
    history::HistoryCapture,
    observation::{CapacityWaiterSnapshot, RuntimeObservation},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{KeyedQuota, ReservationResources, ResourceVector},
    identity::{CapacityKey, DimensionId, FamilyId, Generation},
    scope::{WorkAdmission, WorkKind},
};
use std::task::Waker;
fn dimension(n: u64) -> DimensionId {
    DimensionId::new(n).unwrap()
}
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn vector(amount: u64) -> ResourceVector {
    ResourceVector::new([(dimension(1), amount)]).unwrap()
}
fn keyed() -> KeyedQuota {
    KeyedQuota {
        family: FamilyId::new(1).unwrap(),
        key: CapacityKey(9),
        quota: ResourceVector::new([(dimension(2), 1)]).unwrap(),
    }
}
fn denied(scope: &ScopeHandle, resources: ReservationResources, registrant: &CapacityRegistrant) {
    assert!(matches!(
        scope.reserve_next_or_wait(
            generation(),
            resources,
            WorkKind::Business,
            WorkAdmission::External,
            registrant
        ),
        Err(RuntimeError::CapacityDenied(_))
    ));
}
#[test]
fn original_wait_counters_distinguish_logical_domains_deliveries_and_history_overflow() {
    let observation = RuntimeObservation::new(None);
    let history = HistoryCapture::new(1);
    let ledger = RuntimeLedger::with_keyed_families_and_observation(
        vector(1),
        [(
            FamilyId::new(1).unwrap(),
            ResourceVector::new([(dimension(2), 1)]).unwrap(),
        )],
        [],
        Some(history.clone()),
        observation.clone(),
    )
    .unwrap();
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let process = scope
        .reserve_next(
            generation(),
            ReservationResources::for_work(vector(1)),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap();
    let target = scope
        .reserve_next(
            generation(),
            ReservationResources::work_only().with_keyed(keyed()),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap();
    let registrant = ledger.capacity_registrant(Waker::noop().clone()).unwrap();
    denied(
        &scope,
        ReservationResources::for_work(vector(1)),
        &registrant,
    );
    denied(
        &scope,
        ReservationResources::for_work(vector(1)),
        &registrant,
    );
    denied(
        &scope,
        ReservationResources::work_only().with_keyed(keyed()),
        &registrant,
    );
    denied(
        &scope,
        ReservationResources::work_only().with_keyed(keyed()),
        &registrant,
    );
    assert_eq!(
        ledger.try_capacity_waiter_snapshot().unwrap(),
        Some(CapacityWaiterSnapshot {
            logical_waiters: 1,
            domain_registrations: 2,
            distinct_domains: 2
        })
    );
    let registered = observation.snapshot().unwrap();
    assert_eq!(registered.capacity_wait_registrations, 1);
    assert_eq!(registered.capacity_wait_domains_registered, 2);
    assert_eq!(registered.capacity_wait_deliveries, 0);
    drop(process);
    drop(target);
    let delivered = observation.snapshot().unwrap();
    assert_eq!(delivered.capacity_wait_deliveries, 1);
    assert_eq!(
        ledger
            .try_capacity_waiter_snapshot()
            .unwrap()
            .unwrap()
            .logical_waiters,
        0
    );
    let target = scope
        .reserve_next(
            generation(),
            ReservationResources::work_only().with_keyed(keyed()),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap();
    denied(
        &scope,
        ReservationResources::work_only().with_keyed(keyed()),
        &registrant,
    );
    drop(registrant);
    drop(target);
    let canceled = observation.snapshot().unwrap();
    assert_eq!(canceled.capacity_wait_registrations, 2);
    assert_eq!(canceled.capacity_wait_domains_registered, 3);
    assert_eq!(canceled.capacity_wait_deliveries, 1);
    assert_eq!(
        ledger.try_capacity_waiter_snapshot().unwrap(),
        Some(CapacityWaiterSnapshot {
            logical_waiters: 0,
            domain_registrations: 0,
            distinct_domains: 0
        })
    );
    assert!(
        !history.is_complete(),
        "bounded history overflow cannot disable or reconstruct the original counters"
    );
    scope.close_external().unwrap();
    ledger.remove_scope(scope.id()).unwrap();
}
