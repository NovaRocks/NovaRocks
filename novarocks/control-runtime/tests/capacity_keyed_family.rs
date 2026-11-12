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
    obligation::{ReservationObligation, WorkObligation},
    transaction::{CapacityDenial, RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{CapacityError, KeyedQuota, ReservationResources, ResourceVector},
    identity::{CapacityKey, DimensionId, FamilyId, Generation},
    scope::{WorkAdmission, WorkKind},
};
fn dim(n: u64) -> DimensionId {
    DimensionId::new(n).unwrap()
}
fn family() -> FamilyId {
    FamilyId::new(1).unwrap()
}
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn vector(n: u64) -> ResourceVector {
    ResourceVector::new([(dim(1), n)]).unwrap()
}
fn keyed(key: u128, n: u64) -> KeyedQuota {
    KeyedQuota {
        family: family(),
        key: CapacityKey(key),
        quota: ResourceVector::new([(dim(2), n)]).unwrap(),
    }
}
fn request(key: u128, process: u64, target: u64) -> ReservationResources {
    ReservationResources::for_work(vector(process)).with_keyed(keyed(key, target))
}
fn ledger() -> RuntimeLedger {
    RuntimeLedger::with_keyed_families(
        vector(5),
        [(family(), ResourceVector::new([(dim(2), 3)]).unwrap())],
    )
    .unwrap()
}
fn reserve(
    scope: &ScopeHandle,
    resources: ReservationResources,
) -> (WorkObligation, ReservationObligation) {
    scope
        .reserve_next(
            generation(),
            resources,
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap()
}
fn exit(work: &mut WorkObligation, quota: &mut ReservationObligation) {
    work.body_exited().unwrap();
    work.captures_exited().unwrap();
    let remaining = quota.resources().unwrap();
    quota
        .release_quota_with_keyed(&remaining.quota, remaining.keyed.as_ref())
        .unwrap();
    quota.finish().unwrap();
}
#[test]
fn process_is_reported_before_key_and_denials_are_atomic() {
    let ledger = ledger();
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let (mut work, mut quota) = reserve(&scope, request(7, 4, 2));
    let denied = scope
        .reserve_next(
            generation(),
            request(7, 2, 2),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .err();
    assert_eq!(
        denied,
        Some(RuntimeError::CapacityDenied(CapacityDenial {
            dimension: dim(1),
            keyed: None,
            requested: 2,
            committed: 4,
            limit: 5
        }))
    );
    let denied = scope
        .reserve_next(
            generation(),
            request(7, 1, 2),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .err();
    assert_eq!(
        denied,
        Some(RuntimeError::CapacityDenied(CapacityDenial {
            dimension: dim(2),
            keyed: Some((family(), CapacityKey(7))),
            requested: 2,
            committed: 2,
            limit: 3
        }))
    );
    assert_eq!(ledger.used(dim(1)), 4);
    assert_eq!(scope.snapshot().unwrap().work, 1);
    let other = scope
        .reserve_next(
            generation(),
            request(8, 1, 3),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap();
    assert_eq!(ledger.try_keyed_usage_snapshot().unwrap().len(), 2);
    drop(other);
    assert_eq!(ledger.try_keyed_usage_snapshot().unwrap().len(), 1);
    quota
        .release_quota_with_keyed(&vector(0), Some(&keyed(7, 1)))
        .unwrap();
    assert_eq!(
        ledger.try_keyed_usage_snapshot().unwrap()[0]
            .used
            .get(dim(2)),
        1
    );
    assert_eq!(
        quota.release_quota_with_keyed(&vector(1), Some(&keyed(8, 1))),
        Err(RuntimeError::Capacity(CapacityError::WrongKey))
    );
    assert_eq!(ledger.used(dim(1)), 4);
    exit(&mut work, &mut quota);
    assert!(ledger.try_keyed_usage_snapshot().unwrap().is_empty());
    ledger.reap_ready(2).unwrap();
}
#[test]
fn split_transfer_and_parent_reservation_share_the_same_keyed_charge() {
    let ledger = ledger();
    let source = ledger.open_scope_next(None, generation()).unwrap();
    let target = ledger.open_scope_next(None, generation()).unwrap();
    let (mut parent_work, mut parent) = reserve(&source, request(7, 2, 2));
    let (mut target_work, mut target_quota) = reserve(&target, ReservationResources::work_only());
    let partition =
        ReservationResources::new(vector(1), Default::default()).with_keyed(keyed(7, 1));
    let mut split = parent.split_next(&partition).unwrap();
    split.transfer(&target_work).unwrap();
    assert_eq!(split.resources().unwrap().keyed, partition.keyed);
    assert_eq!(
        ledger.try_keyed_usage_snapshot().unwrap()[0]
            .used
            .get(dim(2)),
        2
    );
    let child = source
        .reserve_child_next(
            generation(),
            parent.id(),
            request(7, 1, 1),
            WorkKind::Business,
        )
        .unwrap();
    assert_eq!(
        ledger.try_keyed_usage_snapshot().unwrap()[0]
            .used
            .get(dim(2)),
        3
    );
    drop(child);
    assert_eq!(
        ledger.try_keyed_usage_snapshot().unwrap()[0]
            .used
            .get(dim(2)),
        2
    );
    split
        .release_quota_with_keyed(&partition.quota, partition.keyed.as_ref())
        .unwrap();
    split.finish().unwrap();
    exit(&mut parent_work, &mut parent);
    exit(&mut target_work, &mut target_quota);
    assert!(ledger.try_keyed_usage_snapshot().unwrap().is_empty());
    ledger.reap_ready(3).unwrap();
}

#[test]
fn keyed_release_and_prepaid_report_fill_share_the_original_transaction() {
    use novarocks_control_runtime::completion::ReportSlot;
    use novarocks_control_runtime_core::{
        completion::ConsumerKind, identity::ConsumerId, owned_storage::OwnedSlotNeed,
    };
    let ledger = ledger();
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let resources = ReservationResources::new(
        vector(1),
        OwnedSlotNeed {
            completions: 1,
            ..OwnedSlotNeed::work_only()
        },
    )
    .with_keyed(keyed(7, 2));
    let (mut work, mut quota) = reserve(&scope, resources);
    let consumer = ConsumerId::new(1).unwrap();
    let report = ReportSlot::reserve_next(
        &mut quota,
        generation(),
        1,
        [(consumer, ConsumerKind::Observer)],
    )
    .unwrap();
    quota
        .release_quota_with_keyed_and_report(&vector(1), Some(&keyed(7, 2)), &report)
        .unwrap();
    report.consume(consumer).unwrap();
    assert_eq!(ledger.used(dim(1)), 0);
    assert!(ledger.try_keyed_usage_snapshot().unwrap().is_empty());
    work.body_exited().unwrap();
    work.captures_exited().unwrap();
    report.body_exited().unwrap();
    report.captures_exited().unwrap();
    assert!(report.try_retire().unwrap());
    quota.finish().unwrap();
    ledger.reap_ready(1).unwrap();
}
