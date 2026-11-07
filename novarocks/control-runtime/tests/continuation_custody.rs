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

#![expect(
    clippy::disallowed_methods,
    reason = "Integration tests own and join their threads and runtime tasks; see ADR-0218."
)]
#![cfg(not(loom))]

use novarocks_control_runtime::{
    completion::ReportSlot,
    continuation::{
        ContinuationPhase as Phase, ContinuationSlot, InvocationResult, RegistrationToken,
    },
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::ConsumerKind,
    identity::{ConsumerId, DimensionId, Generation, WorkId},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering},
};

struct Fixture<T: Send + 'static> {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    report: ReportSlot,
    slot: ContinuationSlot<T>,
    token: Option<RegistrationToken<T>>,
    owner: ConsumerId,
    observer: ConsumerId,
}
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn dimension() -> DimensionId {
    DimensionId::new(1).unwrap()
}
fn work_resources(quota: ResourceVector) -> ReservationResources {
    ReservationResources {
        quota,
        owned_slots: OwnedSlotNeed {
            work_headers: 1,
            ..OwnedSlotNeed::default()
        },
    }
}
fn role_resources(quota: ResourceVector) -> ReservationResources {
    ReservationResources {
        quota,
        owned_slots: OwnedSlotNeed {
            work_headers: 1,
            completions: 1,
            captures: 1,
            ..OwnedSlotNeed::default()
        },
    }
}
fn fixture<T: Send + 'static>() -> Fixture<T> {
    let resources = ResourceVector::new([(dimension(), 1)]).unwrap();
    let ledger = RuntimeLedger::new(resources.clone());
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let (work, mut capacity) = scope
        .reserve_next(
            generation(),
            role_resources(resources),
            WorkKind::Settlement,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Settlement)
        .unwrap();
    let owner = ConsumerId::new(1).unwrap();
    let observer = ConsumerId::new(2).unwrap();
    let report = ReportSlot::reserve_next(
        &mut capacity,
        generation(),
        2,
        [
            (
                owner,
                ConsumerKind::Owner {
                    scope: scope.id(),
                    generation: generation(),
                },
            ),
            (observer, ConsumerKind::Observer),
        ],
    )
    .unwrap();
    let (slot, token) = ContinuationSlot::from_admitted(
        novarocks_control_runtime::reported_work::ReportedWork::try_new(
            work,
            capacity,
            report.clone(),
        )
        .unwrap(),
    );
    Fixture {
        ledger,
        scope,
        report,
        slot,
        token: Some(token),
        owner,
        observer,
    }
}
fn drain<T: Send + 'static>(f: &Fixture<T>) {
    f.report.consume(f.owner).unwrap();
    assert!(!f.slot.try_retire().unwrap());
    assert_eq!(
        f.ledger.used(dimension()),
        1,
        "every consumer leg retains custody"
    );
    f.report.consume(f.observer).unwrap();
    assert!(f.slot.try_retire().unwrap());
    assert_eq!(f.slot.phase(), Phase::Retired);
    assert_eq!(f.ledger.reap_ready(1).unwrap(), 1);
    f.scope.close_external().unwrap();
    assert!(f.scope.snapshot().unwrap().retirement_ready);
    f.ledger.remove_scope(f.scope.id()).unwrap();
}

#[test]
fn unique_registration_waits_for_the_actual_barrier_and_all_report_consumers() {
    let mut f = fixture::<u64>();
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let observer = f.slot.clone();
    assert_eq!(f.slot.publish_barrier(9).unwrap_err().1, 9);
    assert!(f.slot.invoke().is_err());
    f.token
        .take()
        .unwrap()
        .register(Box::new(move |value| {
            assert_eq!(value, 42);
            assert_eq!(
                observer.phase(),
                Phase::Running,
                "callback holds no custody lock"
            );
            counter.fetch_add(1, Ordering::SeqCst);
        }))
        .unwrap_or_else(|_| panic!("one prepaid registration"));
    assert_eq!(f.slot.phase(), Phase::Registered);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        f.slot.invoke().is_err(),
        "registration alone cannot start work"
    );
    f.slot.publish_barrier(42).unwrap();
    assert_eq!(f.slot.publish_barrier(11).unwrap_err().1, 11);
    assert_eq!(f.slot.invoke().unwrap(), InvocationResult::Exited);
    assert!(f.slot.invoke().is_err(), "FnOnce is never invoked again");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(f.slot.body_exited());
    assert!(f.slot.captures_exited());
    assert!(!f.slot.has_panic_payload());
    assert!(!f.slot.try_retire().unwrap());
    assert!(
        f.ledger.unresolved_work().is_empty(),
        "a pending report consumer is not unknown exit"
    );
    drain(&f);
}

#[test]
fn failure_before_delayed_start_keeps_original_callback_ready_for_the_new_generation() {
    use novarocks_control_runtime::{continuation::ContinuationError, transaction::RuntimeError};
    use novarocks_control_runtime_core::scope::ScopeError;
    let mut f = fixture::<u64>();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    f.token
        .take()
        .unwrap()
        .register(Box::new(move |value| {
            assert_eq!(
                value, 42,
                "the failure owner supplies its actual published value"
            );
            seen.fetch_add(1, Ordering::SeqCst);
        }))
        .unwrap_or_else(|_| panic!("one original registration"));
    f.slot.publish_barrier(0).unwrap();
    let next = f.scope.fail().unwrap();
    assert_eq!(
        f.slot.invoke_in_generation(generation()),
        Err(ContinuationError::Runtime(RuntimeError::Scope(
            ScopeError::GenerationMismatch
        )))
    );
    assert_eq!(f.slot.phase(), Phase::Ready);
    assert!(!f.slot.body_exited());
    assert!(!f.slot.captures_exited());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(f.ledger.unresolved_work().is_empty());
    let mut dispatch = f.slot.try_claim_dispatch().unwrap();
    assert_eq!(dispatch.refresh_ready_value(42).unwrap(), 0);
    assert_eq!(
        dispatch.invoke_in_generation(next).unwrap(),
        InvocationResult::Exited
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(dispatch);
    f.report
        .abandon_failed_owner(f.owner, &f.scope, generation())
        .unwrap();
    assert!(!f.slot.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 1);
    f.report.consume(f.observer).unwrap();
    assert!(f.slot.try_retire().unwrap());
    f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap();
    f.ledger.remove_scope(f.scope.id()).unwrap();
    assert!(f.ledger.is_empty());
}

#[test]
fn dropping_unused_authority_waits_for_the_original_holders_real_exit() {
    let mut f = fixture::<()>();
    drop(f.token.take().unwrap());
    assert_eq!(f.slot.phase(), Phase::Unused);
    f.scope.close_external().unwrap();
    assert!(!f.slot.body_exited());
    assert!(!f.slot.captures_exited());
    assert!(!f.scope.snapshot().unwrap().join_ready);
    assert!(!f.slot.try_retire().unwrap());
    assert!(
        f.report.consume(f.owner).is_err(),
        "authority revocation is not an exit report"
    );
    assert_eq!(
        f.slot.unused_holder_exited().unwrap(),
        InvocationResult::Exited
    );
    drain(&f);
}

struct SlowCapture {
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
}
impl Drop for SlowCapture {
    fn drop(&mut self) {
        self.entered.wait();
        self.release.wait();
    }
}
#[test]
fn failed_owner_cannot_release_slow_captures_or_abandon_another_consumers_leg() {
    let mut f = fixture::<()>();
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let capture = SlowCapture {
        entered: entered.clone(),
        release: release.clone(),
    };
    f.token
        .take()
        .unwrap()
        .register(Box::new(move |_| {
            let _ = &capture;
        }))
        .unwrap_or_else(|_| panic!("registered capture"));
    f.slot.publish_barrier(()).unwrap();
    let host_slot = f.slot.clone();
    let host = std::thread::spawn(move || host_slot.invoke().unwrap());
    entered.wait();
    assert_eq!(
        f.slot.phase(),
        Phase::Running,
        "slow foreign Drop holds no custody lock"
    );
    assert!(!f.slot.body_exited());
    assert!(!f.slot.captures_exited());
    assert!(f.report.consume(f.observer).is_err());
    f.scope.fail().unwrap();
    f.report
        .abandon_failed_owner(f.owner, &f.scope, generation())
        .unwrap();
    assert!(
        f.report
            .abandon_failed_owner(f.observer, &f.scope, generation())
            .is_err()
    );
    assert!(!f.slot.try_retire().unwrap());
    assert_eq!(f.ledger.used(dimension()), 1);
    release.wait();
    assert_eq!(host.join().unwrap(), InvocationResult::Exited);
    assert!(
        !f.slot.try_retire().unwrap(),
        "observer still owes its own consumption"
    );
    f.report.consume(f.observer).unwrap();
    assert!(f.slot.try_retire().unwrap());
    f.ledger.reap_ready(1).unwrap();
    assert!(f.scope.snapshot().unwrap().retirement_ready);
    f.ledger.remove_scope(f.scope.id()).unwrap();
}

struct PanicValue(Arc<AtomicUsize>);
impl Drop for PanicValue {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn callback_panic_keeps_original_payload_work_and_capacity_after_external_handles_drop() {
    let mut f = fixture::<()>();
    let disposed = Arc::new(AtomicUsize::new(0));
    let payload = PanicValue(disposed.clone());
    f.token
        .take()
        .unwrap()
        .register(Box::new(move |_| std::panic::panic_any(payload)))
        .unwrap_or_else(|_| panic!("registered panic"));
    f.slot.publish_barrier(()).unwrap();
    assert_eq!(f.slot.invoke().unwrap(), InvocationResult::Unresolved);
    assert_eq!(f.slot.phase(), Phase::Unresolved);
    let unknown = (f.scope.id(), WorkId::new(1).unwrap());
    assert_eq!(f.ledger.unresolved_work(), vec![unknown]);
    assert_eq!(f.scope.snapshot().unwrap().unresolved, 1);
    assert_eq!(f.scope.snapshot().unwrap().work, 1);
    assert_eq!(f.ledger.used(dimension()), 1);
    assert!(f.slot.has_panic_payload());
    assert!(!f.slot.body_exited());
    assert!(!f.slot.captures_exited());
    assert!(!f.slot.try_retire().unwrap());
    assert!(f.report.consume(f.owner).is_err());
    let ledger = f.ledger.clone();
    let scope = f.scope.clone();
    drop(f);
    assert_eq!(
        disposed.load(Ordering::SeqCst),
        0,
        "original panic remains in retained custody"
    );
    assert_eq!(
        ledger.unresolved_work(),
        vec![unknown],
        "unknown exit is diagnosed while its actual WorkObligation stays in retained custody"
    );
    assert_eq!(ledger.used(dimension()), 1);
    assert!(!scope.snapshot().unwrap().join_ready);
}

struct ReentrantValue(Box<dyn FnOnce() + Send>);
impl Drop for ReentrantValue {
    fn drop(&mut self) {
        // Replacement disposes only an empty closure after the callback returns.
        let callback = std::mem::replace(&mut self.0, Box::new(|| {}));
        callback();
    }
}
#[test]
fn rejected_barrier_input_is_returned_and_disposed_without_a_custody_lock() {
    let mut f = fixture::<ReentrantValue>();
    let slot = f.slot.clone();
    let disposed = Arc::new(AtomicUsize::new(0));
    let counter = disposed.clone();
    let value = ReentrantValue(Box::new(move || {
        assert_eq!(slot.phase(), Phase::AwaitingRegistration);
        counter.fetch_add(1, Ordering::SeqCst);
    }));
    let (_, original) = f.slot.publish_barrier(value).err().unwrap();
    assert_eq!(disposed.load(Ordering::SeqCst), 0);
    drop(original);
    assert_eq!(disposed.load(Ordering::SeqCst), 1);
    drop(f.token.take().unwrap());
    f.slot.unused_holder_exited().unwrap();
    drain(&f);
}

#[test]
fn abandoning_all_unused_handles_does_not_drop_the_original_work_lease() {
    let f = fixture::<()>();
    let ledger = f.ledger.clone();
    let scope = f.scope.clone();
    drop(f);
    assert!(
        ledger.unresolved_work().is_empty(),
        "self retention keeps actual role ownership"
    );
    assert!(!scope.snapshot().unwrap().join_ready);
    assert_eq!(ledger.used(dimension()), 1);
}

#[test]
fn exited_continuation_keeps_its_capacity_until_the_admitted_child_tree_retires() {
    use novarocks_control_runtime_core::{
        capacity::ReservationRequest,
        identity::{ReservationId, WorkId},
    };
    // One budget retains the parent record; two budgets reap it before the
    // retained real WorkObligation asks whether its actual family has exited.
    for reaping_budget in [1, 2] {
        let resources = ResourceVector::new([(dimension(), 1)]).unwrap();
        let ledger = RuntimeLedger::new(ResourceVector::new([(dimension(), 2)]).unwrap());
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let contender = ledger.open_scope_next(None, generation()).unwrap();
        let (work, mut capacity) = scope
            .reserve_next(
                generation(),
                role_resources(resources.clone()),
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        assert!(!work.actual_exit_ready().unwrap());
        let (mut child, mut child_capacity) = work
            .derive(ReservationRequest {
                id: ReservationId::new(2).unwrap(),
                generation: generation(),
                guard: WorkId::new(2).unwrap(),
                resources: work_resources(resources.clone()),
                admission: WorkAdmission::External, // derive replaces this with the original parent.
                kind: WorkKind::Settlement,
            })
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        child.start().unwrap();
        let consumer = ConsumerId::new(1).unwrap();
        let report = ReportSlot::reserve_next(
            &mut capacity,
            generation(),
            1,
            [(consumer, ConsumerKind::Observer)],
        )
        .unwrap();
        let (slot, token) = ContinuationSlot::<()>::from_admitted(
            novarocks_control_runtime::reported_work::ReportedWork::try_new(
                work,
                capacity,
                report.clone(),
            )
            .unwrap(),
        );
        token
            .register(Box::new(|_| {}))
            .unwrap_or_else(|_| panic!("one admitted callback"));
        slot.publish_barrier(()).unwrap();
        assert_eq!(slot.invoke().unwrap(), InvocationResult::Exited);
        assert!(slot.body_exited());
        assert!(slot.captures_exited());
        report.consume(consumer).unwrap();
        assert!(
            !slot.try_retire().unwrap(),
            "callback/report exit cannot retire a live admitted child"
        );
        assert_eq!(ledger.used(dimension()), 2);
        assert!(
            ledger.unresolved_work().is_empty(),
            "an ordinary live child is not unknown exit"
        );
        assert!(
            contender
                .reserve_next(
                    generation(),
                    work_resources(resources.clone()),
                    WorkKind::Business,
                    WorkAdmission::External
                )
                .is_err(),
            "parent capacity cannot be reused while its child is running"
        );
        let version = ledger.change_version();
        child.body_exited().unwrap();
        assert!(
            ledger.change_version() > version,
            "child progress still wakes its host finisher"
        );
        child.captures_exited().unwrap();
        assert!(child.actual_exit_ready().unwrap());
        child_capacity.release_quota(&resources).unwrap();
        child_capacity.finish().unwrap();
        assert_eq!(
            ledger.used(dimension()),
            1,
            "the continuation still owns its own capacity"
        );
        assert!(
            !slot.try_retire().unwrap(),
            "the actual child record still retains its parent's child count"
        );
        assert_eq!(
            ledger.reap_scope_ready(scope.id(), reaping_budget).unwrap(),
            reaping_budget
        );
        assert!(
            child.actual_exit_ready().unwrap(),
            "a known reaped lease remains positively exited"
        );
        assert!(slot.try_retire().unwrap());
        assert_eq!(ledger.used(dimension()), 0);
        assert_eq!(
            ledger.reap_scope_ready(scope.id(), 1).unwrap(),
            2 - reaping_budget
        );
        let next = contender
            .reserve_next(
                generation(),
                work_resources(resources.clone()),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap();
        drop(next);
        scope.close_external().unwrap();
        contender.close_external().unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(
            child.actual_exit_ready().unwrap(),
            "full scope removal cannot invalidate known actual exit"
        );
        ledger.remove_scope(contender.id()).unwrap();
        assert!(ledger.is_empty());
    }
}
