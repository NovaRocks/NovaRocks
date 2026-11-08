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
    actor::{Actor, ActorConfig, ActorStep},
    actor_turn::ActorRuntime,
    history::HistoryCapture,
    obligation::{ReservationObligation, WorkObligation},
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{CapacityWaitTarget, KeyedQuota, ReservationResources, ResourceVector},
    history::HistoryFact,
    identity::*,
    mailbox::{InputClass, InputEnvelope},
    scope::{WorkAdmission, WorkKind},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Wake, Waker},
};
fn dim(n: u64) -> DimensionId {
    DimensionId::new(n).unwrap()
}
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn vector(n: u64) -> ResourceVector {
    ResourceVector::new([(dim(1), n)]).unwrap()
}
fn key(n: u64) -> KeyedQuota {
    KeyedQuota {
        family: FamilyId::new(1).unwrap(),
        key: CapacityKey(7),
        quota: ResourceVector::new([(dim(2), n)]).unwrap(),
    }
}
fn request(process: u64, keyed: u64) -> ReservationResources {
    ReservationResources::for_work(vector(process)).with_keyed(key(keyed))
}
#[derive(Default)]
struct Counter(AtomicUsize);
impl Wake for Counter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn counter() -> (Arc<Counter>, Waker) {
    let value = Arc::new(Counter::default());
    (value.clone(), Waker::from(value))
}
fn denied(
    scope: &ScopeHandle,
    r: ReservationResources,
    registrant: &novarocks_control_runtime::capacity_notification::CapacityRegistrant,
) {
    assert!(matches!(
        scope.reserve_next_or_wait(
            generation(),
            r,
            WorkKind::Business,
            WorkAdmission::External,
            registrant
        ),
        Err(RuntimeError::CapacityDenied(_))
    ));
}
fn reserve(
    scope: &ScopeHandle,
    r: ReservationResources,
) -> (WorkObligation, ReservationObligation) {
    scope
        .reserve_next(generation(), r, WorkKind::Business, WorkAdmission::External)
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap()
}
fn exit(work: &mut WorkObligation, quota: &mut ReservationObligation) {
    work.body_exited().unwrap();
    work.captures_exited().unwrap();
    let r = quota.resources().unwrap();
    quota
        .release_quota_with_keyed(&r.quota, r.keyed.as_ref())
        .unwrap();
    quota.finish().unwrap();
}
#[test]
fn duplicate_waits_merge_release_is_once_and_drop_unregisters() {
    let history = HistoryCapture::new(64);
    let ledger = RuntimeLedger::with_history(vector(2), history.clone());
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let (mut work, mut quota) = reserve(&scope, request(2, 0));
    let (count, waker) = counter();
    let registrant = ledger.capacity_registrant(waker).unwrap();
    denied(&scope, request(1, 0), &registrant);
    denied(&scope, request(1, 0), &registrant);
    assert_eq!(ledger.capacity_waiter_count(), 1);
    quota.release_quota(&vector(1)).unwrap();
    assert_eq!(count.0.load(Ordering::SeqCst), 1);
    assert_eq!(ledger.capacity_waiter_count(), 0);
    quota.release_quota(&vector(1)).unwrap();
    assert_eq!(count.0.load(Ordering::SeqCst), 1);
    let (mut other_work, mut other_quota) = reserve(&scope, request(2, 0));
    denied(&scope, request(1, 0), &registrant);
    drop(registrant);
    assert_eq!(ledger.capacity_waiter_count(), 0);
    exit(&mut other_work, &mut other_quota);
    assert_eq!(count.0.load(Ordering::SeqCst), 1);
    exit(&mut work, &mut quota);
    ledger.reap_ready(2).unwrap();
    let facts = history.drain(64).entries;
    assert_eq!(
        facts
            .iter()
            .filter(|entry| matches!(entry.fact, HistoryFact::CapacityWaitRegistered { .. }))
            .count(),
        2
    );
    assert_eq!(
        facts
            .iter()
            .filter(|entry| matches!(entry.fact, HistoryFact::CapacityWaitNotified { .. }))
            .count(),
        1
    );
}
#[test]
fn process_and_keyed_domains_merge_and_either_first_release_consumes_all() {
    for process_first in [true, false] {
        let ledger = RuntimeLedger::with_keyed_families(
            vector(1),
            [(
                FamilyId::new(1).unwrap(),
                ResourceVector::new([(dim(2), 1)]).unwrap(),
            )],
        )
        .unwrap();
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let (mut process_work, mut process_quota) = reserve(&scope, request(1, 0));
        let (mut keyed_work, mut keyed_quota) = reserve(&scope, request(0, 1));
        let (count, waker) = counter();
        let registrant = ledger.capacity_registrant(waker).unwrap();
        denied(&scope, request(1, 0), &registrant);
        denied(&scope, request(0, 1), &registrant);
        assert_eq!(ledger.capacity_waiter_count(), 1);
        if process_first {
            process_quota.release_quota(&vector(1)).unwrap();
            assert_eq!(count.0.load(Ordering::SeqCst), 1);
            assert_eq!(ledger.capacity_waiter_count(), 0);
            keyed_quota
                .release_quota_with_keyed(&vector(0), Some(&key(1)))
                .unwrap();
        } else {
            keyed_quota
                .release_quota_with_keyed(&vector(0), Some(&key(1)))
                .unwrap();
            assert_eq!(count.0.load(Ordering::SeqCst), 1);
            assert_eq!(ledger.capacity_waiter_count(), 0);
            process_quota.release_quota(&vector(1)).unwrap();
        }
        assert_eq!(
            count.0.load(Ordering::SeqCst),
            1,
            "the first matching release consumes every domain of one logical wait"
        );
        exit(&mut process_work, &mut process_quota);
        exit(&mut keyed_work, &mut keyed_quota);
        ledger.reap_ready(2).unwrap();
    }
}
#[test]
fn foreign_ledger_registration_is_rejected_and_parent_wait_is_atomic() {
    let ledger = RuntimeLedger::new(vector(1));
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let foreign = RuntimeLedger::new(vector(1));
    let (_, waker) = counter();
    let registrant = foreign.capacity_registrant(waker).unwrap();
    assert!(matches!(
        scope.reserve_next_or_wait(
            generation(),
            request(1, 0),
            WorkKind::Business,
            WorkAdmission::External,
            &registrant
        ),
        Err(RuntimeError::Capacity(_))
    ));
    let (mut work, mut quota) = reserve(&scope, request(1, 0));
    let (count, waker) = counter();
    let local = ledger.capacity_registrant(waker).unwrap();
    assert!(matches!(
        scope.reserve_child_next_or_wait(
            generation(),
            quota.id(),
            request(1, 0),
            WorkKind::Business,
            &local
        ),
        Err(RuntimeError::CapacityDenied(_))
    ));
    quota.release_quota(&vector(1)).unwrap();
    assert_eq!(count.0.load(Ordering::SeqCst), 1);
    exit(&mut work, &mut quota);
    ledger.reap_ready(1).unwrap();
}
#[derive(Debug)]
struct Idle;
impl Actor for Idle {
    type Input = ();
    type Effect = ();
    fn timer_input(_: Tick) {}
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut ActorStep<'_, ()>) {}
    fn handle(&mut self, _: Tick, _: InputEnvelope, _: (), _: &mut ActorStep<'_, ()>) {}
}
fn config() -> ActorConfig {
    ActorConfig {
        mailbox: [4; 4],
        max_batches: 4,
        max_effects_per_step: 2,
        max_dependencies_per_effect: 2,
        turn_budget: 16,
        max_step_work: 2,
    }
}
#[test]
fn multiple_runtime_attachments_and_closed_idle_runtime_do_not_delay_external_wake() {
    let ledger = RuntimeLedger::new(vector(1));
    let first = ledger.open_scope_next(None, generation()).unwrap();
    let second = ledger.open_scope_next(None, generation()).unwrap();
    let one = ActorRuntime::new();
    let two = ActorRuntime::new();
    one.register_next(ActorId::new(1).unwrap(), first.clone(), Idle, config())
        .unwrap();
    two.register_next(ActorId::new(1).unwrap(), second.clone(), Idle, config())
        .unwrap();
    let (mut work, mut quota) = reserve(&first, request(1, 0));
    let (count, waker) = counter();
    let registrant = ledger.capacity_registrant(waker).unwrap();
    denied(&first, request(1, 0), &registrant);
    one.close_admission();
    two.close_admission();
    quota.release_quota(&vector(1)).unwrap();
    assert_eq!(
        count.0.load(Ordering::SeqCst),
        1,
        "ordinary external release must not depend on another poll"
    );
    exit(&mut work, &mut quota);
    ledger.reap_ready(1).unwrap();
    first.close_external().unwrap();
    second.close_external().unwrap();
    one.remove_actor(ActorId::new(1).unwrap()).unwrap();
    two.remove_actor(ActorId::new(1).unwrap()).unwrap();
}

struct Rollback {
    scope: ScopeHandle,
    registrant: novarocks_control_runtime::capacity_notification::CapacityRegistrant,
}
impl Actor for Rollback {
    type Input = ();
    type Effect = ();
    fn timer_input(_: Tick) {}
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut ActorStep<'_, ()>) {}
    fn handle(&mut self, _: Tick, _: InputEnvelope, _: (), step: &mut ActorStep<'_, ()>) {
        let guard = self
            .scope
            .reserve_next(
                generation(),
                request(1, 0),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap();
        denied(&self.scope, request(1, 0), &self.registrant);
        assert!(step.reservation(guard).is_ok());
        panic!("intentional actor failure after staging an unbound reservation");
    }
}
struct SnapshotWake {
    runtime: std::sync::Weak<std::sync::Mutex<Option<ActorRuntime<Rollback>>>>,
    count: Arc<Counter>,
}
impl Wake for SnapshotWake {
    fn wake(self: Arc<Self>) {
        let owner = self.runtime.upgrade().unwrap();
        let runtime = owner.lock().unwrap().as_ref().unwrap().clone();
        assert!(
            runtime.snapshot(ActorId::new(1).unwrap()).unwrap().failed,
            "wake must follow failed-turn processing outside the inbox lock"
        );
        self.count.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn failed_turn_rollback_wakes_after_failure_processing_and_without_runtime_locks() {
    let ledger = RuntimeLedger::new(vector(1));
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let runtime = ActorRuntime::new();
    let owner = Arc::new(std::sync::Mutex::new(Some(runtime.clone())));
    let count = Arc::new(Counter::default());
    let waker = Waker::from(Arc::new(SnapshotWake {
        runtime: Arc::downgrade(&owner),
        count: count.clone(),
    }));
    let registrant = ledger.capacity_registrant(waker).unwrap();
    assert!(
        runtime
            .register_next(
                ActorId::new(1).unwrap(),
                scope.clone(),
                Rollback {
                    scope: scope.clone(),
                    registrant
                },
                config()
            )
            .is_ok()
    );
    let input = runtime
        .reserve_input(
            ActorId::new(1).unwrap(),
            ActorId::new(2).unwrap(),
            InputClass::Ordinary,
        )
        .unwrap()
        .fill(())
        .unwrap();
    assert!(runtime.poll_one(Tick(1)).unwrap());
    assert_eq!(count.0.load(Ordering::SeqCst), 1);
    assert_eq!(ledger.used(dim(1)), 0);
    assert_eq!(ledger.capacity_waiter_count(), 0);
    let novarocks_control_runtime::actor_turn::FailureSourceState::ClosedPending(supervisor) =
        runtime
            .failure_source_state(ActorId::new(1).unwrap())
            .unwrap()
    else {
        panic!("expected original failure input");
    };
    runtime
        .retire_input(ActorId::new(1).unwrap(), input)
        .unwrap();
    runtime
        .retire_input(ActorId::new(1).unwrap(), supervisor)
        .unwrap();
    drop(
        runtime
            .take_failure_payload(ActorId::new(1).unwrap())
            .unwrap(),
    );
    while let Some(turn) = runtime.claim_host_actor() {
        turn.release(false).unwrap();
    }
    scope.close_external().unwrap();
    assert!(runtime.remove_actor(ActorId::new(1).unwrap()).is_ok());
}

#[test]
fn a_successful_different_target_preserves_the_original_release_subscription() {
    let history = HistoryCapture::new(64);
    let ledger = RuntimeLedger::with_keyed_families_and_observation(
        vector(2),
        [(
            FamilyId::new(1).unwrap(),
            ResourceVector::new([(dim(2), 1)]).unwrap(),
        )],
        [],
        Some(history.clone()),
        Default::default(),
    )
    .unwrap();
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let (mut slow_work, mut slow_quota) = reserve(&scope, request(1, 1));
    let (count, waker) = counter();
    let registrant = ledger.capacity_registrant(waker).unwrap();
    denied(&scope, request(1, 1), &registrant);
    let different = ReservationResources::for_work(vector(1)).with_keyed(KeyedQuota {
        key: CapacityKey(8),
        ..key(1)
    });
    let healthy = scope
        .reserve_next_or_wait(
            generation(),
            different,
            WorkKind::Business,
            WorkAdmission::External,
            &registrant,
        )
        .unwrap();
    assert_eq!(
        ledger.capacity_waiter_count(),
        1,
        "another successful target cannot erase the earlier wait"
    );
    drop(healthy);
    assert_eq!(
        count.0.load(Ordering::SeqCst),
        1,
        "another key's release may send one non-authorizing family-domain hint"
    );
    slow_quota
        .release_quota_with_keyed(&vector(0), Some(&key(1)))
        .unwrap();
    assert_eq!(count.0.load(Ordering::SeqCst), 1);
    exit(&mut slow_work, &mut slow_quota);
    ledger.reap_ready(1).unwrap();
    let facts = history.drain(64).entries;
    assert!(
        facts.iter().any(|entry| matches!(
            entry.fact,
            HistoryFact::CapacityWaitNotified {
                target: CapacityWaitTarget::Keyed {
                    key: CapacityKey(8),
                    ..
                },
                ..
            }
        )),
        "notification history names the actual released key, not the originally refused key"
    );
}

#[test]
fn one_logical_wait_covers_all_blocked_targets_and_first_release_consumes_it() {
    let ledger = RuntimeLedger::with_keyed_families(
        vector(2),
        [(
            FamilyId::new(1).unwrap(),
            ResourceVector::new([(dim(2), 1)]).unwrap(),
        )],
    )
    .unwrap();
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let (mut first_work, mut first_quota) = reserve(&scope, request(0, 1));
    let second_key = KeyedQuota {
        key: CapacityKey(8),
        ..key(1)
    };
    let second_resources = ReservationResources::for_work(vector(0)).with_keyed(second_key.clone());
    let (mut second_work, mut second_quota) = reserve(&scope, second_resources.clone());
    let (count, waker) = counter();
    let registrant = ledger.capacity_registrant(waker).unwrap();
    denied(&scope, request(0, 1), &registrant);
    denied(&scope, second_resources, &registrant);
    assert_eq!(
        ledger.capacity_waiter_count(),
        1,
        "one actor has one logical wait covering both refused targets"
    );
    first_quota
        .release_quota_with_keyed(&vector(0), Some(&key(1)))
        .unwrap();
    assert_eq!(
        count.0.load(Ordering::SeqCst),
        1,
        "the first target can progress even while the later refused target remains blocked"
    );
    assert_eq!(ledger.capacity_waiter_count(), 0);
    second_quota
        .release_quota_with_keyed(&vector(0), Some(&second_key))
        .unwrap();
    assert_eq!(
        count.0.load(Ordering::SeqCst),
        1,
        "the first related release consumes the entire logical registration"
    );
    exit(&mut first_work, &mut first_quota);
    exit(&mut second_work, &mut second_quota);
    ledger.reap_ready(2).unwrap();
}
