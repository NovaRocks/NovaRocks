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
    completion::ReportSlot,
    history::HistoryCapture,
    obligation::{ReservationObligation, WorkObligation},
    reported_work::ReportedWork,
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{CapacityError, ReservationResources, ResourceVector},
    history::HistoryFact,
    identity::{DimensionId, Generation, ReservationId},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use std::{
    collections::BTreeSet,
    sync::{Arc, Barrier},
    thread,
};
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn dimension(n: u64) -> DimensionId {
    DimensionId::new(n).unwrap()
}
fn resources(first: u64, second: u64) -> ResourceVector {
    ResourceVector::new([(dimension(1), first), (dimension(2), second)]).unwrap()
}
fn quota_only(quota: ResourceVector) -> ReservationResources {
    ReservationResources::new(quota, OwnedSlotNeed::default())
}
fn used(ledger: &RuntimeLedger) -> (u64, u64) {
    (ledger.used(dimension(1)), ledger.used(dimension(2)))
}
fn reserve(
    scope: &ScopeHandle,
    resources: ResourceVector,
) -> (WorkObligation, ReservationObligation) {
    scope
        .reserve_next(
            generation(),
            ReservationResources::for_work(resources),
            WorkKind::Step,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Step)
        .unwrap()
}
fn release(mut capacity: ReservationObligation) {
    let resources = capacity.resources().unwrap();
    capacity.release_quota(&resources.quota).unwrap();
    capacity.finish().unwrap();
}
fn exit(work: &mut WorkObligation) {
    work.body_exited().unwrap();
    work.captures_exited().unwrap();
}
fn exit_with_report(work: &mut WorkObligation, report: &ReportSlot) {
    work.body_exited().unwrap();
    work.captures_exited_with_report(report).unwrap();
    report.body_exited().unwrap();
    report.captures_exited().unwrap();
    assert!(report.try_retire().unwrap());
}
fn finish_scope(ledger: &RuntimeLedger, scope: &ScopeHandle, work: usize) {
    scope.close_external().unwrap();
    assert_eq!(ledger.reap_scope_ready(scope.id(), work).unwrap(), work);
    ledger.remove_scope(scope.id()).unwrap();
}

#[test]
fn paid_split_keeps_its_real_owner_then_transfers_to_an_existing_other_scope_work() {
    let history = HistoryCapture::new(64);
    let ledger = RuntimeLedger::with_history(resources(10, 20), history.clone());
    let source = ledger.open_scope_next(None, generation()).unwrap();
    let target = ledger.open_scope_next(None, generation()).unwrap();
    let reserve_reported = |scope: &ScopeHandle, quota: ResourceVector| {
        let (work, mut capacity) = scope
            .reserve_next(
                generation(),
                ReservationResources::new(
                    quota,
                    OwnedSlotNeed {
                        work_headers: 1,
                        completions: 1,
                        ..OwnedSlotNeed::default()
                    },
                ),
                WorkKind::Step,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Step)
            .unwrap();
        let report = ReportSlot::reserve_next(&mut capacity, generation(), 0, []).unwrap();
        (work, capacity, report)
    };
    let (source_work, mut remainder, source_report) = reserve_reported(&source, resources(10, 20));
    let (target_work, target_base, target_report) =
        reserve_reported(&target, ResourceVector::default());
    let original = remainder.id();
    let work_before = source.snapshot().unwrap().work + target.snapshot().unwrap().work;
    let route = remainder.split_next(&quota_only(resources(3, 8))).unwrap();
    let split_id = route.id();
    // The real binding check proves the split still belongs to the original
    // source Work, rather than to an invented new lease or copied scope ID.
    let (mut source_work, mut route, source_report) =
        ReportedWork::try_new(source_work, route, source_report)
            .unwrap()
            .into_parts();
    route.transfer(&target_work).unwrap();
    let (mut target_work, route, target_report) =
        ReportedWork::try_new(target_work, route, target_report)
            .unwrap()
            .into_parts();
    let work_after = source.snapshot().unwrap().work + target.snapshot().unwrap().work;
    let conserved = used(&ledger);
    let source_remaining = remainder.resources().unwrap().quota;
    source_work.start().unwrap();
    target_work.start().unwrap();
    exit_with_report(&mut source_work, &source_report);
    remainder.release_quota(&resources(2, 4)).unwrap();
    let after_partial_release = used(&ledger);
    let after_partial_remainder = remainder.resources().unwrap().quota;
    release(remainder);
    let target_still_held = used(&ledger);
    exit_with_report(&mut target_work, &target_report);
    release(route);
    release(target_base);
    finish_scope(&ledger, &source, 1);
    finish_scope(&ledger, &target, 1);
    let facts = history.drain(64);
    let splits = facts
        .entries
        .iter()
        .filter_map(|entry| match entry.fact {
            HistoryFact::CapacitySplit { from, to } => Some((from, to)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(ledger.is_empty());
    assert_eq!(used(&ledger), (0, 0));
    assert_eq!(work_before, 2);
    assert_eq!(work_after, work_before);
    assert_ne!(split_id, original);
    assert_eq!(source_remaining, resources(7, 12));
    assert_eq!(conserved, (10, 20));
    assert_eq!(after_partial_release, (8, 16));
    assert_eq!(after_partial_remainder, resources(5, 8));
    assert_eq!(target_still_held, (3, 8));
    assert!(facts.complete);
    assert_eq!(splits, [(original, split_id)]);
}

#[test]
fn refused_splits_preserve_the_original_vector_work_and_identity_watermark() {
    let ledger = RuntimeLedger::new(resources(4, 8));
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let (mut work, mut capacity) = reserve(&scope, resources(4, 8));
    work.start().unwrap();
    let original = capacity.id();
    let before = scope.snapshot().unwrap();
    let mut errors = Vec::new();
    for requested in [
        ResourceVector::default(),
        resources(5, 9),
        ResourceVector::new([(dimension(3), 1)]).unwrap(),
    ] {
        errors.push(capacity.split_next(&quota_only(requested)).err());
    }
    let explicit_duplicate = capacity
        .split(original, &ReservationResources::default())
        .err();
    let explicit_failed_high_id = capacity
        .split(
            ReservationId::new(1000).unwrap(),
            &quota_only(resources(5, 8)),
        )
        .err();
    let unchanged = capacity.resources().unwrap().quota;
    let committed = used(&ledger);
    let after = scope.snapshot().unwrap();
    let split = capacity.split_next(&quota_only(resources(1, 2))).unwrap();
    let next = split.id();
    // All unused units may move; the original primary still owns the real
    // bound Work header. A fully moved non-primary parcel has no remaining owner.
    let mut whole = capacity.split_next(&quota_only(resources(3, 6))).unwrap();
    assert!(!capacity.is_released());
    assert!(capacity.resources().unwrap().quota.is_empty());
    let moved = whole.split_next(&quota_only(resources(3, 6))).unwrap();
    assert!(whole.is_released());
    assert_eq!(whole.resources().unwrap(), ReservationResources::default());
    assert_eq!(scope.snapshot().unwrap().work, 1);
    assert_eq!(used(&ledger), (4, 8));
    exit(&mut work);
    release(moved);
    release(split);
    release(capacity);
    finish_scope(&ledger, &scope, 1);
    assert!(ledger.is_empty());
    assert_eq!(
        errors,
        [
            Some(RuntimeError::Capacity(CapacityError::EmptyReservation)),
            Some(RuntimeError::Capacity(CapacityError::InvalidSplit)),
            Some(RuntimeError::Capacity(CapacityError::InvalidSplit)),
        ]
    );
    assert_eq!(
        explicit_duplicate,
        Some(RuntimeError::Capacity(CapacityError::DuplicateReservation))
    );
    assert_eq!(
        explicit_failed_high_id,
        Some(RuntimeError::Capacity(CapacityError::InvalidSplit))
    );
    assert_eq!(unchanged, resources(4, 8));
    assert_eq!(committed, (4, 8));
    assert_eq!(after, before);
    assert_eq!(next.get(), original.get() + 1);
}

#[test]
fn dynamic_split_records_preserve_unknown_owner_and_exhausted_identity_error_order() {
    let ledger = RuntimeLedger::new(resources(4, 8));
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let (mut work, mut capacity) = reserve(&scope, resources(4, 8));
    work.start().unwrap();
    let empty = capacity.split_next(&ReservationResources::default()).err();
    let unchanged = capacity.resources().unwrap().quota;
    let first = capacity.split_next(&quota_only(resources(1, 2))).unwrap();
    let second = capacity.split_next(&quota_only(resources(1, 2))).unwrap();
    assert_eq!(scope.snapshot().unwrap().work, 1);
    assert_eq!(used(&ledger), (4, 8));
    exit(&mut work);
    release(first);
    release(second);
    capacity.release_quota(&resources(2, 4)).unwrap();
    capacity.finish().unwrap();
    let unknown = capacity.split_next(&quota_only(resources(1, 2))).err();
    finish_scope(&ledger, &scope, 1);
    assert!(ledger.is_empty());
    assert_eq!(
        empty,
        Some(RuntimeError::Capacity(CapacityError::EmptyReservation))
    );
    assert_eq!(unchanged, resources(4, 8));
    assert_eq!(
        unknown,
        Some(RuntimeError::Capacity(CapacityError::UnknownReservation))
    );

    let ledger = RuntimeLedger::new(resources(4, 8));
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let (mut work, mut capacity) = reserve(&scope, resources(4, 8));
    work.start().unwrap();
    let high = capacity
        .split(
            ReservationId::new(u64::MAX).unwrap(),
            &quota_only(resources(1, 2)),
        )
        .unwrap();
    let before = capacity.resources().unwrap();
    let exhausted = capacity.split_next(&quota_only(resources(1, 2))).err();
    let explicit = capacity
        .split(
            ReservationId::new(u64::MAX).unwrap(),
            &quota_only(resources(1, 2)),
        )
        .err();
    let after = capacity.resources().unwrap();
    let committed = used(&ledger);
    exit(&mut work);
    release(high);
    release(capacity);
    finish_scope(&ledger, &scope, 1);
    assert!(ledger.is_empty());
    assert_eq!(
        exhausted,
        Some(RuntimeError::Capacity(CapacityError::Overflow))
    );
    assert_eq!(
        explicit,
        Some(RuntimeError::Capacity(CapacityError::DuplicateReservation))
    );
    assert_eq!(before, after);
    assert_eq!(committed, (4, 8));
}

#[test]
fn two_original_bundles_split_next_while_another_scope_reserves_without_identity_races() {
    let ledger = RuntimeLedger::new(resources(9, 18));
    let scopes = (0..3)
        .map(|_| ledger.open_scope_next(None, generation()).unwrap())
        .collect::<Vec<_>>();
    let (mut first_work, first_capacity) = reserve(&scopes[0], resources(4, 8));
    let (mut second_work, second_capacity) = reserve(&scopes[1], resources(4, 8));
    first_work.start().unwrap();
    second_work.start().unwrap();
    let initial_max = second_capacity.id();
    let barrier = Arc::new(Barrier::new(4));
    let splitting = |mut capacity: ReservationObligation, barrier: Arc<Barrier>| {
        thread::spawn(move || {
            barrier.wait();
            let result = capacity.split_next(&quota_only(resources(1, 2)));
            (capacity, result)
        })
    };
    let first = splitting(first_capacity, barrier.clone());
    let second = splitting(second_capacity, barrier.clone());
    let reserving = scopes[2].clone();
    let gate = barrier.clone();
    let third = thread::spawn(move || {
        gate.wait();
        reserving
            .reserve_next(
                generation(),
                ReservationResources::for_work(resources(1, 2)),
                WorkKind::Step,
                WorkAdmission::External,
            )
            .and_then(|guard| guard.bind(WorkKind::Step))
    });
    barrier.wait();
    let (first_capacity, first_split) = first.join().unwrap();
    let (second_capacity, second_split) = second.join().unwrap();
    let reserved = third.join().unwrap();
    let errors = [
        first_split.as_ref().err().copied(),
        second_split.as_ref().err().copied(),
        reserved.as_ref().err().copied(),
    ];
    let mut ids = Vec::new();
    for capacity in [&first_split, &second_split].into_iter().flatten() {
        ids.push(capacity.id());
    }
    if let Ok((_, capacity)) = &reserved {
        ids.push(capacity.id());
    }
    let committed = used(&ledger);
    let work_count = scopes
        .iter()
        .map(|scope| scope.snapshot().unwrap().work)
        .sum::<usize>();
    exit(&mut first_work);
    exit(&mut second_work);
    release(first_capacity);
    release(second_capacity);
    if let Ok(capacity) = first_split {
        release(capacity);
    }
    if let Ok(capacity) = second_split {
        release(capacity);
    }
    let mut third_work = 0;
    if let Ok((mut work, capacity)) = reserved {
        work.start().unwrap();
        exit(&mut work);
        release(capacity);
        third_work = 1;
    }
    for (scope, count) in scopes.iter().zip([1, 1, third_work]) {
        finish_scope(&ledger, scope, count);
    }
    assert!(ledger.is_empty());
    assert_eq!(errors, [None; 3]);
    assert_eq!(committed, (9, 18));
    assert_eq!(work_count, 3);
    assert_eq!(ids.len(), 3);
    assert_eq!(ids.iter().copied().collect::<BTreeSet<_>>().len(), 3);
    assert!(ids.iter().all(|id| id.get() > initial_max.get()));
}
