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
    actor_turn::{ActorRuntime, ActorTurnError, EffectTicket, FailureSourceState, InputPhase},
    history::HistoryCapture,
    obligation::{ReservationObligation, WorkObligation},
    publication::{PublishedReply, PublishedSnapshot},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    effect::{EffectDescriptor, EffectKind},
    history::HistoryFact,
    identity::*,
    mailbox::{InputClass, InputEnvelope},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
fn aid() -> ActorId {
    ActorId::new(1).unwrap()
}
fn action(n: u64) -> ActionId {
    ActionId::new(n).unwrap()
}
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn vector(n: u64) -> ResourceVector {
    ResourceVector::new([(DimensionId::new(1).unwrap(), n)]).unwrap()
}
fn work_resources(quota: ResourceVector) -> ReservationResources {
    ReservationResources {
        keyed: None,
        quota,
        owned_slots: OwnedSlotNeed {
            work_headers: 1,
            ..OwnedSlotNeed::default()
        },
    }
}
fn supervisor_input(runtime: &ActorRuntime<FailingActor>) -> InputId {
    let FailureSourceState::ClosedPending(input) = runtime.failure_source_state(aid()).unwrap()
    else {
        panic!("the original failure supervisor is pending");
    };
    input
}
#[derive(Debug)]
struct FailingActor(ScopedWorkRef);
impl Actor for FailingActor {
    type Input = u64;
    type Effect = u64;
    fn timer_input(now: Tick) -> u64 {
        now.0
    }
    fn handle(
        &mut self,
        _now: Tick,
        _input: InputEnvelope,
        value: u64,
        step: &mut ActorStep<'_, u64>,
    ) {
        assert_ne!(value, 99, "controlled owner failure");
        step.effect(
            EffectDescriptor {
                id: action(1),
                work: step.input_work().unwrap_or(self.0),
                kind: EffectKind::Business,
                dependencies: vec![],
            },
            value,
        )
        .unwrap();
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(
        _scope: ScopeId,
        _generation: Generation,
        _input: InputEnvelope,
        step: &mut ActorStep<'_, u64>,
    ) {
        // Deliberately sparse: history publications must follow the actual
        // failure batch action watermark, never batch length or a guessed 1.
        step.effect(
            EffectDescriptor {
                id: action(17),
                work: step.failure_work().expect("original Cleanup supervisor"),
                kind: EffectKind::Cleanup,
                dependencies: vec![],
            },
            99,
        )
        .unwrap();
    }
}
struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    runtime: ActorRuntime<FailingActor>,
    history: HistoryCapture,
    committed: PublishedReply<&'static str>,
    business: WorkObligation,
    business_capacity: ReservationObligation,
    cleanup: WorkObligation,
    cleanup_capacity: ReservationObligation,
    first: InputId,
    failed: InputId,
    reserved: InputId,
    queued: InputId,
    old: Option<EffectTicket<u64>>,
    initial: Option<EffectTicket<u64>>,
}
fn fixture(start_cleanup: bool) -> Fixture {
    fixture_with_before_failure(start_cleanup, |_, _| ()).0
}

fn fixture_with_before_failure<T>(
    start_cleanup: bool,
    before_failure: impl FnOnce(&ActorRuntime<FailingActor>, &WorkObligation) -> T,
) -> (Fixture, T) {
    let history = HistoryCapture::new(256);
    let ledger = RuntimeLedger::with_history(vector(2), history.clone());
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let (mut business, business_capacity) = scope
        .reserve_next(
            generation(),
            work_resources(vector(1)),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    let (mut cleanup, cleanup_capacity) = scope
        .reserve_next(
            generation(),
            work_resources(vector(1)),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    business.start().unwrap();
    if start_cleanup {
        cleanup.start().unwrap();
    }
    let runtime = ActorRuntime::with_history(history.clone());
    runtime
        .register_supervised_bound(
            aid(),
            scope.clone(),
            FailingActor(business.reference()),
            ActorConfig {
                mailbox: [8; 4],
                max_batches: 8,
                max_effects_per_step: 1,
                max_dependencies_per_effect: 0,
                turn_budget: 16,
                max_step_work: 1,
            },
            &cleanup,
        )
        .unwrap();
    let first = runtime
        .reserve_input(aid(), aid(), InputClass::Ordinary)
        .unwrap()
        .fill(10)
        .unwrap();
    runtime.poll_one(Tick(1)).unwrap();
    let old = runtime.take_effect(aid(), first, action(1)).unwrap();
    assert!(
        runtime.claim_failure_publication(&old).is_err(),
        "ordinary business cannot claim failure authority"
    );
    let committed = PublishedReply::new(&scope);
    runtime
        .commit_reply(&old, &committed, Arc::new("success"))
        .unwrap();
    let reserved = runtime
        .reserve_input(aid(), aid(), InputClass::Ordinary)
        .unwrap()
        .id();
    let failed = runtime
        .reserve_input(aid(), aid(), InputClass::Ordinary)
        .unwrap()
        .fill(99)
        .unwrap();
    let queued = runtime
        .reserve_input(aid(), aid(), InputClass::Ordinary)
        .unwrap()
        .fill(77)
        .unwrap();
    let original = before_failure(&runtime, &business);
    runtime.poll_one(Tick(2)).unwrap();
    let initial = runtime
        .take_effect(aid(), supervisor_input(&runtime), action(17))
        .unwrap();
    (
        Fixture {
            ledger,
            scope,
            runtime,
            history,
            committed,
            business,
            business_capacity,
            cleanup,
            cleanup_capacity,
            first,
            failed,
            reserved,
            queued,
            old: Some(old),
            initial: Some(initial),
        },
        original,
    )
}
fn drain_neutral_inputs(f: &Fixture) {
    f.runtime.abandon_failed_input(aid(), f.reserved).unwrap();
    f.runtime.retire_input(aid(), f.reserved).unwrap();
    if let Some((queued, value)) = f
        .runtime
        .take_failed_input(aid(), InputClass::Ordinary)
        .unwrap()
    {
        assert_eq!(queued.input, f.queued);
        assert_eq!(value, 77);
    } else {
        assert!(
            f.runtime
                .input_inventory(aid(), None, 8)
                .unwrap()
                .iter()
                .any(|input| input.input == f.queued
                    && input.phase == InputPhase::AbandonedByOwnerFailure)
        );
    }
    f.runtime.retire_input(aid(), f.queued).unwrap();
}
fn drain(mut f: Fixture) {
    drain_neutral_inputs(&f);
    f.cleanup.body_exited().unwrap();
    f.cleanup.captures_exited().unwrap();
    f.cleanup_capacity.release_quota(&vector(1)).unwrap();
    f.cleanup_capacity.finish().unwrap();
    let initial = f.initial.take().unwrap();
    let supervisor = initial.input;
    f.runtime.effect_completed(initial).unwrap();
    f.runtime.retire_input(aid(), supervisor).unwrap();
    f.runtime.retire_input(aid(), f.failed).unwrap();
    f.business.body_exited().unwrap();
    f.business.captures_exited().unwrap();
    f.business_capacity.release_quota(&vector(1)).unwrap();
    f.business_capacity.finish().unwrap();
    f.runtime.effect_completed(f.old.take().unwrap()).unwrap();
    f.runtime.retire_input(aid(), f.first).unwrap();
    f.ledger.reap_ready(2).unwrap();
    drop(f.runtime.take_failure_payload(aid()).unwrap());
    while let Some(original_host_turn) = f.runtime.claim_host_actor() {
        original_host_turn.release(false).unwrap();
    }
    f.runtime.remove_actor(aid()).unwrap();
    f.ledger.remove_scope(f.scope.id()).unwrap();
    assert!(f.ledger.is_empty());
}

struct Incarnation {
    scope: ScopeHandle,
    work: [(WorkObligation, ReservationObligation); 2],
    input: InputId,
    cleanup: EffectTicket<u64>,
}
fn failed_incarnation(runtime: &ActorRuntime<FailingActor>, ledger: &RuntimeLedger) -> Incarnation {
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let admit = |kind| {
        let (mut work, capacity) = scope
            .reserve_next(
                generation(),
                work_resources(vector(1)),
                kind,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(kind)
            .unwrap();
        work.start().unwrap();
        (work, capacity)
    };
    let work = [admit(WorkKind::Business), admit(WorkKind::Cleanup)];
    runtime
        .register_supervised_bound(
            aid(),
            scope.clone(),
            FailingActor(work[0].0.reference()),
            ActorConfig {
                mailbox: [1; 4],
                max_batches: 1,
                max_effects_per_step: 1,
                max_dependencies_per_effect: 0,
                turn_budget: 8,
                max_step_work: 1,
            },
            &work[1].0,
        )
        .unwrap();
    let input = runtime
        .reserve_input(aid(), aid(), InputClass::Ordinary)
        .unwrap()
        .fill(99)
        .unwrap();
    runtime.poll_one(Tick(1)).unwrap();
    let cleanup = runtime
        .take_effect(aid(), supervisor_input(runtime), action(17))
        .unwrap();
    Incarnation {
        scope,
        work,
        input,
        cleanup,
    }
}
fn retire_incarnation(
    runtime: &ActorRuntime<FailingActor>,
    ledger: &RuntimeLedger,
    incarnation: Incarnation,
) {
    for (mut work, mut capacity) in incarnation.work {
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        capacity.release_quota(&vector(1)).unwrap();
        capacity.finish().unwrap();
    }
    let supervisor = incarnation.cleanup.input;
    runtime.effect_completed(incarnation.cleanup).unwrap();
    runtime.retire_input(aid(), supervisor).unwrap();
    runtime.retire_input(aid(), incarnation.input).unwrap();
    ledger.reap_scope_ready(incarnation.scope.id(), 2).unwrap();
    drop(runtime.take_failure_payload(aid()).unwrap());
    while let Some(original_host_turn) = runtime.claim_host_actor() {
        original_host_turn.release(false).unwrap();
    }
    runtime.remove_actor(aid()).unwrap();
    ledger.remove_scope(incarnation.scope.id()).unwrap();
    assert!(ledger.is_empty());
}

fn reject_reused_actor_session(different_ledger: bool) {
    let ledger = RuntimeLedger::new(vector(2));
    let runtime = ActorRuntime::new();
    let first = failed_incarnation(&runtime, &ledger);
    let mut stale = runtime.claim_failure_publication(&first.cleanup).unwrap();
    let original_scope = first.scope.id();
    let original_input = first.input;
    let original_work = first.cleanup.descriptor.work;
    let original_generation = first.cleanup.generation;
    retire_incarnation(&runtime, &ledger, first);

    let next_ledger = if different_ledger {
        RuntimeLedger::new(vector(2))
    } else {
        ledger.clone()
    };
    let second = failed_incarnation(&runtime, &next_ledger);
    if different_ledger {
        assert_eq!(second.scope.id(), original_scope);
    } else {
        assert_ne!(second.scope.id(), original_scope);
    }
    assert_eq!(second.input, original_input);
    assert_eq!(second.cleanup.descriptor.work.work, original_work.work);
    assert_eq!(second.cleanup.descriptor.work.scope, second.scope.id());
    assert_eq!(original_work.scope, original_scope);
    assert_eq!(second.cleanup.generation, original_generation);
    let mut current = runtime.claim_failure_publication(&second.cleanup).unwrap();
    let stale_matches = stale.matches_actor_runtime(&runtime, aid());
    let current_matches = current.matches_actor_runtime(&runtime, aid());
    let target = PublishedSnapshot::new(&second.scope, Arc::new("new bootstrap"));
    let rejected = stale.commit_snapshot(&target, Arc::new("stale owner"));
    let after_stale = *target.read();
    let replacement = current.commit_snapshot(&target, Arc::new("current owner"));
    let after_current = *target.read();
    // Always retire actual work before asserting the intended regression, so
    // an old implementation fails once rather than unwinding live actor custody.
    retire_incarnation(&runtime, &next_ledger, second);
    assert_eq!(
        stale_matches,
        Err(ActorTurnError::EffectTicketOriginMismatch)
    );
    assert_eq!(current_matches, Ok(true));
    assert!(
        matches!(
            rejected,
            Err((ActorTurnError::EffectTicketOriginMismatch, _))
        ),
        "a retired scope's session must not acquire the new actor incarnation: {rejected:?}"
    );
    assert_eq!(after_stale, "new bootstrap");
    assert!(replacement.is_ok());
    assert_eq!(after_current, "current owner");
}

#[test]
fn stale_session_cannot_publish_into_a_reused_actor_with_matching_local_identifiers() {
    reject_reused_actor_session(false);
}

#[test]
fn stale_session_cannot_follow_reused_scope_numbers_in_another_ledger() {
    reject_reused_actor_session(true);
}

#[test]
fn one_prepaid_failure_session_publishes_each_actual_target_without_republishing_its_ticket() {
    let f = fixture(true);
    let initial = f.initial.as_ref().unwrap();
    assert!(
        f.runtime
            .claim_failure_publication(f.old.as_ref().unwrap())
            .is_err(),
        "old generation cannot claim"
    );
    let mut session = f.runtime.claim_failure_publication(initial).unwrap();
    assert!(session.matches_actor_runtime(&f.runtime, aid()).unwrap());
    assert!(
        session
            .matches_actor_runtime(&f.runtime.clone(), aid())
            .unwrap()
    );
    assert!(
        !session
            .matches_actor_runtime(&f.runtime, ActorId::new(2).unwrap())
            .unwrap()
    );
    let unrelated = ActorRuntime::<FailingActor>::new();
    assert!(!session.matches_actor_runtime(&unrelated, aid()).unwrap());
    assert_eq!(
        f.runtime.claim_failure_publication(initial).unwrap_err(),
        ActorTurnError::FailurePublicationAlreadyClaimed
    );
    let first = PublishedSnapshot::new(&f.scope, Arc::new("old"));
    let second = PublishedSnapshot::new(&f.scope, Arc::new("old"));
    let reply = PublishedReply::new(&f.scope);
    assert_eq!(
        *session
            .commit_snapshot(&first, Arc::new("OwnerFailed"))
            .unwrap(),
        "old"
    );
    assert_eq!(
        *session
            .commit_snapshot(&second, Arc::new("OwnerFailed"))
            .unwrap(),
        "old"
    );
    session
        .commit_reply(&reply, Arc::new("OwnerFailed"))
        .unwrap();
    assert_eq!(*reply.take_once().unwrap(), "OwnerFailed");
    assert_eq!(
        f.runtime
            .commit_snapshot(initial, &first, Arc::new("bypass"))
            .unwrap_err()
            .0,
        ActorTurnError::FailurePublicationRequired
    );
    let actions: Vec<_> = f
        .history
        .drain(256)
        .entries
        .into_iter()
        .filter_map(|entry| match entry.fact {
            HistoryFact::FailurePublication {
                actor_generation,
                action,
                ..
            } if actor_generation == session.generation() => Some(action.get()),
            _ => None,
        })
        .collect();
    assert_eq!(
        actions,
        vec![18, 19, 20],
        "each actual commit has its own action after sparse failure action 17"
    );
    assert_eq!(
        f.runtime
            .retire_input(aid(), f.initial.as_ref().unwrap().input),
        Err(ActorTurnError::EffectsRemain),
        "initial cleanup stays Started throughout publication slices"
    );
    drop(session);
    assert_eq!(
        f.runtime.claim_failure_publication(initial).unwrap_err(),
        ActorTurnError::FailurePublicationAlreadyClaimed,
        "Drop cannot mint another authority or fake exit"
    );
    drain(f);
}

#[test]
fn previously_committed_reply_survives_failure_and_is_never_overwritten() {
    let f = fixture(true);
    let reply = &f.committed;
    let mut session = f
        .runtime
        .claim_failure_publication(f.initial.as_ref().unwrap())
        .unwrap();
    let (error, rejected) = session
        .commit_reply(reply, Arc::new("OwnerFailed"))
        .unwrap_err();
    assert_eq!(error, ActorTurnError::ReplyAlreadyCommitted);
    assert_eq!(*rejected, "OwnerFailed");
    assert_eq!(*reply.take_once().unwrap(), "success");
    assert_eq!(
        session
            .commit_reply(reply, Arc::new("OwnerFailed"))
            .unwrap_err()
            .0,
        ActorTurnError::ReplyAlreadyCommitted
    );
    drain(f);
}

#[test]
fn cleanup_work_must_be_started_and_still_have_a_live_body_for_each_commit() {
    let mut f = fixture(false);
    assert_eq!(
        f.runtime
            .claim_failure_publication(f.initial.as_ref().unwrap())
            .unwrap_err(),
        ActorTurnError::FailureWorkInactive
    );
    f.cleanup.start().unwrap();
    let mut session = f
        .runtime
        .claim_failure_publication(f.initial.as_ref().unwrap())
        .unwrap();
    let target = PublishedSnapshot::new(&f.scope, Arc::new(0));
    assert!(session.matches_actor_runtime(&f.runtime, aid()).unwrap());
    f.cleanup.body_exited().unwrap();
    assert_eq!(
        session.matches_actor_runtime(&f.runtime, aid()),
        Err(ActorTurnError::FailureWorkInactive)
    );
    assert_eq!(
        session.commit_snapshot(&target, Arc::new(1)).unwrap_err().0,
        ActorTurnError::FailureWorkInactive
    );
    assert_eq!(*target.read(), 0);
    f.cleanup.captures_exited().unwrap();
    f.cleanup_capacity.release_quota(&vector(1)).unwrap();
    f.cleanup_capacity.finish().unwrap();
    drain_neutral_inputs(&f);
    let initial = f.initial.take().unwrap();
    let supervisor = initial.input;
    f.runtime.effect_completed(initial).unwrap();
    f.runtime.retire_input(aid(), supervisor).unwrap();
    f.runtime.retire_input(aid(), f.failed).unwrap();
    f.business.body_exited().unwrap();
    f.business.captures_exited().unwrap();
    f.business_capacity.release_quota(&vector(1)).unwrap();
    f.business_capacity.finish().unwrap();
    f.runtime.effect_completed(f.old.take().unwrap()).unwrap();
    f.runtime.retire_input(aid(), f.first).unwrap();
    f.ledger.reap_ready(2).unwrap();
    drop(f.runtime.take_failure_payload(aid()).unwrap());
    while let Some(original_host_turn) = f.runtime.claim_host_actor() {
        original_host_turn.release(false).unwrap();
    }
    f.runtime.remove_actor(aid()).unwrap();
    f.ledger.remove_scope(f.scope.id()).unwrap();
}

#[test]
fn a_cleanup_descriptor_cannot_borrow_business_work_as_failure_publication_authority() {
    let ledger = RuntimeLedger::new(vector(1));
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let (mut business, mut capacity) = scope
        .reserve_next(
            generation(),
            work_resources(vector(1)),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    business.start().unwrap();
    let runtime = ActorRuntime::new();
    let refusal = runtime
        .register_supervised_bound(
            aid(),
            scope.clone(),
            FailingActor(business.reference()),
            ActorConfig {
                mailbox: [1; 4],
                max_batches: 1,
                max_effects_per_step: 1,
                max_dependencies_per_effect: 0,
                turn_budget: 8,
                max_step_work: 1,
            },
            &business,
        )
        .unwrap_err();
    assert_eq!(
        refusal.0,
        ActorTurnError::FailureWorkInactive,
        "the actual registration gate refuses a Business role as failure authority"
    );
    business.body_exited().unwrap();
    business.captures_exited().unwrap();
    capacity.release_quota(&vector(1)).unwrap();
    capacity.finish().unwrap();
    ledger.reap_scope_ready(scope.id(), 1).unwrap();
    scope.close_external().unwrap();
    ledger.remove_scope(scope.id()).unwrap();
    assert!(runtime.is_empty());
    assert!(ledger.is_empty());
}

struct DropProbe {
    value: u64,
    dropped: Option<Box<dyn FnOnce() + Send + Sync>>,
}
impl Drop for DropProbe {
    fn drop(&mut self) {
        if let Some(callback) = self.dropped.take() {
            callback();
        }
    }
}
fn probe(f: &Fixture, value: u64, counter: &Arc<AtomicUsize>) -> Arc<DropProbe> {
    let runtime = f.runtime.clone();
    let scope = f.scope.clone();
    let counter = counter.clone();
    Arc::new(DropProbe {
        value,
        dropped: Some(Box::new(move || {
            let _ = runtime.snapshot(aid());
            let _ = scope.snapshot();
            counter.fetch_add(1, Ordering::SeqCst);
        })),
    })
}
#[test]
fn replaced_and_rejected_source_arcs_are_disposed_by_the_host_outside_every_gate() {
    let f = fixture(true);
    let mut session = f
        .runtime
        .claim_failure_publication(f.initial.as_ref().unwrap())
        .unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let target = PublishedSnapshot::new(&f.scope, probe(&f, 0, &drops));
    let old = session
        .commit_snapshot(&target, probe(&f, 1, &drops))
        .unwrap_or_else(|_| panic!("actual snapshot commit"));
    assert_eq!(old.value, 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(old);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let reply = PublishedReply::new(&f.scope);
    session
        .commit_reply(&reply, probe(&f, 2, &drops))
        .unwrap_or_else(|_| panic!("first reply"));
    let (_, rejected) = session
        .commit_reply(&reply, probe(&f, 3, &drops))
        .err()
        .unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(rejected);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    drop(reply.take_once());
    drop(target);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
    drain(f);
}

#[test]
fn failed_input_is_abandoned_once_and_waits_for_its_failure_batch_to_retire() {
    let f = fixture(true);
    let input = f
        .runtime
        .input_inventory(aid(), None, 8)
        .unwrap()
        .into_iter()
        .find(|input| input.input == f.failed)
        .unwrap();
    assert_eq!(input.phase, InputPhase::AbandonedByOwnerFailure);
    f.runtime.abandon_failed_input(aid(), f.failed).unwrap();
    assert_eq!(
        f.runtime
            .retire_input(aid(), f.initial.as_ref().unwrap().input),
        Err(ActorTurnError::EffectsRemain)
    );
    f.runtime.abandon_failed_input(aid(), f.reserved).unwrap();
    f.runtime.abandon_failed_input(aid(), f.reserved).unwrap();
    let (queued, value) = f
        .runtime
        .take_failed_input(aid(), InputClass::Ordinary)
        .unwrap()
        .unwrap();
    assert_eq!((queued.input, value), (f.queued, 77));
    f.runtime.abandon_failed_input(aid(), f.queued).unwrap();
    let entries = f.history.drain(256).entries;
    for abandoned in [f.failed, f.reserved, f.queued] {
        assert_eq!(entries.iter().filter(|entry| matches!(entry.fact, HistoryFact::InputAbandoned { input, .. } if input == abandoned)).count(), 1);
        assert!(!entries.iter().any(|entry| matches!(entry.fact, HistoryFact::InputConsumed { input, .. } if input == abandoned)));
    }
    assert_eq!(entries.iter().filter(|entry| matches!(entry.fact, HistoryFact::InputAbandoned { input, .. } if input == f.failed)).count(), 1);
    assert!(!entries.iter().any(
        |entry| matches!(entry.fact, HistoryFact::InputConsumed { input, .. } if input == f.failed)
    ));
    assert!(entries.iter().any(
        |entry| matches!(entry.fact, HistoryFact::InputConsumed { input, .. } if input == f.first)
    ));
    drain(f);
}

#[test]
fn failed_source_moves_only_its_exact_fifo_head_and_pins_it_through_disposal() {
    let (fixture, (mut first, first_id, mut second, second_id)) =
        fixture_with_before_failure(true, |runtime, work| {
            let source = ActorId::new(2).unwrap();
            let standing = || {
                runtime
                    .reserve_admitted_input(aid(), source, InputClass::Ordinary, work)
                    .unwrap()
                    .into_standing_input()
                    .unwrap()
            };
            let first = standing();
            let second = standing();
            let first_id = first.fill(201).unwrap();
            let second_id = second.fill(202).unwrap();
            (first, first_id, second, second_id)
        });
    let runtime = &fixture.runtime;
    assert!(matches!(
        second.take_failed_source_payload(&[second.id()].into()),
        Err(ActorTurnError::InputAuthorMismatch)
    ));
    assert_eq!(
        runtime
            .input_custody(aid(), first_id)
            .unwrap()
            .unwrap()
            .phase,
        InputPhase::Filled
    );
    let roles = [first.id(), second.id()].into();
    for (expected, role) in [(first_id, first.id()), (second_id, second.id())] {
        let original = first.take_failed_source_payload(&roles).unwrap().unwrap();
        assert_eq!(original.standing_id(), role);
        assert_eq!(
            runtime
                .input_custody(aid(), expected)
                .unwrap()
                .unwrap()
                .phase,
            InputPhase::DisposingByOwnerFailure
        );
        assert_eq!(
            runtime.retire_input(aid(), expected),
            Err(ActorTurnError::InputStillQueued)
        );
        assert_eq!(
            runtime.abandon_failed_input(aid(), expected),
            Err(ActorTurnError::InputStillQueued)
        );
        assert_eq!(original.dispose().unwrap(), expected);
        runtime.retire_input(aid(), expected).unwrap();
    }
    assert!(first.take_failed_source_payload(&roles).unwrap().is_none());
    assert_eq!(
        runtime
            .input_custody(aid(), fixture.queued)
            .unwrap()
            .unwrap()
            .phase,
        InputPhase::Filled
    );
    first.close_after_exit().unwrap();
    second.close_after_exit().unwrap();
    drain(fixture);
}
