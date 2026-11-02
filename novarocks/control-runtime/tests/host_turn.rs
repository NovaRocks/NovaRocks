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
    actor::{Actor, ActorConfig, StepContext},
    driver::{ActorHostTurn, ActorRegistration, ActorRuntime},
    lease::{ReservationBundle, WorkLease},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    identity::{ActorId, DimensionId, Generation, ScopeId, Tick},
    mailbox::{InputClass, InputEnvelope},
    scope::{WorkAdmission, WorkKind},
};
use std::sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug)]
struct OriginalActor {
    calls: Arc<AtomicUsize>,
}
impl Actor for OriginalActor {
    type Input = usize;
    type Effect = ();
    fn timer_input(_: Tick) -> usize {
        unreachable!("the original actor has no deadline")
    }
    fn handle(&mut self, _: Tick, _: InputEnvelope, value: usize, _: &mut StepContext<'_, ()>) {
        self.calls.fetch_add(value, Ordering::SeqCst);
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut StepContext<'_, ()>) {
        unreachable!("the original actor does not panic")
    }
}
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn dimension() -> DimensionId {
    DimensionId::new(1).unwrap()
}
fn paid(count: u64) -> ResourceVector {
    ResourceVector::new([(dimension(), count)]).unwrap()
}
fn release(turn: ActorHostTurn<OriginalActor>) {
    if let Err((error, original)) = turn.release(false) {
        drop(original);
        panic!("the original host turn could not release: {error}");
    }
}
fn retire(turn: ActorHostTurn<OriginalActor>) -> OriginalActor {
    match turn.retire_actor() {
        Ok(original) => original,
        Err((error, original)) => {
            drop(original);
            panic!("the physically drained original actor could not retire: {error}");
        }
    }
}

struct OriginalActorFixture {
    ledger: RuntimeLedger,
    runtime: ActorRuntime<OriginalActor>,
    scope: ScopeHandle,
    registration: ActorRegistration,
    work: WorkLease,
    capacity: ReservationBundle,
    calls: Arc<AtomicUsize>,
}
impl OriginalActorFixture {
    fn new(ledger: RuntimeLedger, runtime: ActorRuntime<OriginalActor>, actor: ActorId) -> Self {
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let (mut work, capacity) = scope
            .reserve_next(
                generation(),
                ReservationResources::for_work(paid(1)),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        work.start().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let registration = runtime
            .register_supervised_bound(
                actor,
                scope.clone(),
                OriginalActor {
                    calls: calls.clone(),
                },
                ActorConfig {
                    mailbox: [2; 4],
                    max_batches: 2,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
                &work,
            )
            .unwrap();
        Self {
            ledger,
            runtime,
            scope,
            registration,
            work,
            capacity,
            calls,
        }
    }
    fn independent() -> Self {
        Self::new(
            RuntimeLedger::new(paid(1)),
            ActorRuntime::new(),
            ActorId::new(1).unwrap(),
        )
    }
    fn exit_work(&mut self) {
        self.scope.close_external().unwrap();
        self.work.body_exited().unwrap();
        self.work.captures_exited().unwrap();
        self.ledger.reap_scope_ready(self.scope.id(), 1).unwrap();
    }
    fn finish_after_actor_retired(mut self) {
        self.capacity
            .release_quota(&self.capacity.resources().unwrap().quota)
            .unwrap();
        self.capacity.finish().unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
    }
    fn finish(mut self) {
        self.exit_work();
        self.runtime.notify_host_actor(&self.registration).unwrap();
        let turn = self.runtime.claim_host_actor().unwrap();
        assert!(turn.matches_registration(&self.registration));
        drop(retire(turn));
        self.finish_after_actor_retired();
    }
}

#[test]
fn a_host_turn_excludes_same_actor_but_allows_core_and_other_actor_progress() {
    let ledger = RuntimeLedger::new(paid(2));
    let runtime = ActorRuntime::new();
    let first =
        OriginalActorFixture::new(ledger.clone(), runtime.clone(), ActorId::new(1).unwrap());
    let second =
        OriginalActorFixture::new(ledger.clone(), runtime.clone(), ActorId::new(2).unwrap());
    runtime.notify_host_actor(&first.registration).unwrap();
    runtime.notify_host_actor(&second.registration).unwrap();
    let turn = runtime.claim_host_actor().unwrap();
    let turn_registration = turn.registration();
    let first_claim_is_original = turn.matches_registration(&first.registration)
        && runtime.validate_registration(&turn_registration).is_ok()
        && turn_registration.matches_scope(&first.scope)
        && turn_registration.attachment() == first.registration.attachment();
    let input = runtime
        .reserve_input(
            first.registration.actor(),
            first.registration.actor(),
            InputClass::Ordinary,
        )
        .unwrap()
        .fill(7)
        .unwrap();
    let worker_runtime = runtime.clone();
    let first_registration = first.registration.clone();
    let second_registration = second.registration.clone();
    let observed = Arc::new(Barrier::new(2));
    let race = Arc::new(Barrier::new(2));
    let worker_observed = observed.clone();
    let worker_race = race.clone();
    let worker = std::thread::spawn(move || {
        let notified_while_claimed = worker_runtime
            .notify_host_actor(&first_registration)
            .is_ok();
        let (other_is_original, other_released) = match worker_runtime.claim_host_actor() {
            Some(other) => {
                let original = other.matches_registration(&second_registration);
                let released = match other.release(false) {
                    Ok(()) => true,
                    Err((_error, original)) => {
                        drop(original);
                        false
                    }
                };
                (original, released)
            }
            None => (false, false),
        };
        let no_second_first_claim = match worker_runtime.claim_host_actor() {
            Some(unexpected) => {
                release(unexpected);
                false
            }
            None => true,
        };
        let core_progressed = matches!(worker_runtime.poll_one(Tick(1)), Ok(true));
        worker_observed.wait();
        worker_race.wait();
        let racing_notification = worker_runtime
            .notify_host_actor(&first_registration)
            .is_ok();
        (
            notified_while_claimed,
            other_is_original,
            other_released,
            no_second_first_claim,
            core_progressed,
            racing_notification,
        )
    });
    observed.wait();
    // Both real threads leave the same barrier before notify/release. Either
    // scheduler-lock order must leave the original actor ready afterwards.
    race.wait();
    release(turn);
    let observed = worker.join().expect("the original notifier thread exits");
    runtime
        .retire_input(first.registration.actor(), input)
        .unwrap();
    let followup = runtime.claim_host_actor().unwrap();
    let racing_mark_retained = followup.matches_registration(&first.registration);
    release(followup);
    let core_value = first.calls.load(Ordering::SeqCst);
    first.finish();
    second.finish();
    let fully_clean = runtime.is_empty() && ledger.is_empty();
    assert!(first_claim_is_original);
    assert!(observed.0 && observed.1 && observed.2 && observed.3 && observed.4 && observed.5);
    assert!(racing_mark_retained);
    assert_eq!(core_value, 7);
    assert!(fully_clean);
}

#[test]
fn dropping_an_unreleased_turn_requeues_only_metadata_and_keeps_original_work_funded() {
    let fixture = OriginalActorFixture::independent();
    fixture
        .runtime
        .notify_host_actor(&fixture.registration)
        .unwrap();
    let turn = fixture.runtime.claim_host_actor().unwrap();
    let before = fixture.scope.snapshot().unwrap().work;
    drop(turn);
    let returned = fixture.runtime.claim_host_actor().unwrap();
    let same_original = returned.matches_registration(&fixture.registration);
    let unchanged_work = fixture.scope.snapshot().unwrap().work == before && before == 1;
    let unchanged_funding = fixture.ledger.used(dimension()) == 1;
    let no_core_call = fixture.calls.load(Ordering::SeqCst) == 0;
    release(returned);
    let ledger = fixture.ledger.clone();
    let runtime = fixture.runtime.clone();
    fixture.finish();
    assert!(same_original && unchanged_work && unchanged_funding && no_core_call);
    assert!(runtime.is_empty() && ledger.is_empty());
}

#[test]
fn retirement_preserves_refused_turn_until_real_input_and_work_exit_then_rejects_old_registration()
{
    let mut fixture = OriginalActorFixture::independent();
    let ledger = fixture.ledger.clone();
    let runtime = fixture.runtime.clone();
    let old = fixture.registration.clone();
    runtime.notify_host_actor(&old).unwrap();
    let turn = runtime.claim_host_actor().unwrap();
    let (live_work_refused, turn) = match turn.retire_actor() {
        Err((_error, original)) => (original.matches_registration(&old), original),
        Ok(_) => panic!("live original work cannot authorize actor retirement"),
    };
    let input = runtime
        .reserve_input(old.actor(), old.actor(), InputClass::Ordinary)
        .unwrap()
        .fill(11)
        .unwrap();
    fixture.exit_work();
    let (queued_input_refused, turn) = match turn.retire_actor() {
        Err((_error, original)) => (original.matches_registration(&old), original),
        Ok(_) => panic!("an original queued input cannot be discarded by actor retirement"),
    };
    let core_progressed = runtime.poll_one(Tick(1)).unwrap();
    let (committed_input_refused, turn) = match turn.retire_actor() {
        Err((_error, original)) => (original.matches_registration(&old), original),
        Ok(_) => panic!("an original unretired input batch cannot be discarded"),
    };
    runtime.retire_input(old.actor(), input).unwrap();
    runtime.notify_host_actor(&old).unwrap();
    let returned = retire(turn);
    let retired_value = returned.calls.load(Ordering::SeqCst);
    drop(returned);
    let pending_mark_removed = runtime.claim_host_actor().is_none();
    let original_fee_still_held = ledger.used(dimension()) == 1;
    fixture.finish_after_actor_retired();
    let successor = OriginalActorFixture::new(ledger.clone(), runtime.clone(), old.actor());
    let own_valid = runtime
        .validate_registration(&successor.registration)
        .is_ok();
    let old_rejected =
        runtime.validate_registration(&old).is_err() && runtime.notify_host_actor(&old).is_err();
    let stale_mark_absent = match runtime.claim_host_actor() {
        Some(unexpected) => {
            release(unexpected);
            false
        }
        None => true,
    };
    let old_scope_not_successor = !old.matches_scope(&successor.scope);
    successor.finish();
    assert!(live_work_refused && queued_input_refused && committed_input_refused);
    assert!(core_progressed);
    assert_eq!(retired_value, 11);
    assert!(pending_mark_removed && original_fee_still_held);
    assert!(own_valid && old_rejected && stale_mark_absent && old_scope_not_successor);
    assert!(runtime.is_empty() && ledger.is_empty());
}

#[test]
fn foreign_equal_numeric_registration_cannot_wake_and_original_cleanup_can_wake_after_close() {
    let first = OriginalActorFixture::independent();
    let second = OriginalActorFixture::independent();
    let numeric_collision = first.registration.actor() == second.registration.actor()
        && first.registration.scope_id() == second.registration.scope_id()
        && first.registration.attachment() == second.registration.attachment()
        && first.work.id() == second.work.id();
    let foreign_scope_rejected = !first.registration.matches_scope(&second.scope)
        && first.registration.matches_scope(&first.scope);
    let foreign_registration_rejected = first
        .runtime
        .validate_registration(&second.registration)
        .is_err();
    let foreign_wake_rejected = first
        .runtime
        .notify_host_actor(&second.registration)
        .is_err();
    let no_foreign_wake = match first.runtime.claim_host_actor() {
        Some(unexpected) => {
            release(unexpected);
            false
        }
        None => true,
    };
    // Intake closure and failed scope do not revoke the registered original
    // cleanup owner's metadata wake or discharge any of its actual work.
    first.runtime.close_admission();
    first.scope.fail().unwrap();
    let cleanup_registration_valid = first
        .runtime
        .validate_registration(&first.registration)
        .is_ok();
    let cleanup_wake_valid = first.runtime.notify_host_actor(&first.registration).is_ok();
    let cleanup_turn = first.runtime.claim_host_actor().unwrap();
    let original_cleanup_turn = cleanup_turn.matches_registration(&first.registration)
        && !cleanup_turn.matches_registration(&second.registration);
    let original_funding_held = first.ledger.used(dimension()) == 1;
    release(cleanup_turn);
    let first_ledger = first.ledger.clone();
    let first_runtime = first.runtime.clone();
    let second_ledger = second.ledger.clone();
    let second_runtime = second.runtime.clone();
    first.finish();
    second.finish();
    assert!(numeric_collision && foreign_scope_rejected);
    assert!(foreign_registration_rejected && foreign_wake_rejected && no_foreign_wake);
    assert!(cleanup_registration_valid && cleanup_wake_valid && original_cleanup_turn);
    assert!(original_funding_held);
    assert!(first_runtime.is_empty() && first_ledger.is_empty());
    assert!(second_runtime.is_empty() && second_ledger.is_empty());
}
