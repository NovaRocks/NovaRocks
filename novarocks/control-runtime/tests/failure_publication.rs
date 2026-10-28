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
    driver::{ActorRuntime, DriverError, EffectTicket, InputPhase},
    history::HistoryCapture,
    lease::{CapacityBundle, WorkLease},
    publication::{PublishedReply, PublishedSnapshot},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    effect::{EffectDescriptor, EffectKind},
    history::HistoryFact,
    identity::*,
    mailbox::{InputClass, InputEnvelope},
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
fn work_id(n: u64) -> WorkId {
    WorkId::new(n).unwrap()
}
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn vector(n: u64) -> ResourceVector {
    ResourceVector::new([(DimensionId::new(1).unwrap(), n)]).unwrap()
}
#[derive(Debug)]
struct FailingActor;
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
        step: &mut StepContext<'_, u64>,
    ) {
        assert_ne!(value, 99, "controlled owner failure");
        step.effect(
            EffectDescriptor {
                id: action(1),
                work: work_id(1),
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
        step: &mut StepContext<'_, u64>,
    ) {
        // Deliberately sparse: history publications must follow the actual
        // failure batch action watermark, never batch length or a guessed 1.
        step.effect(
            EffectDescriptor {
                id: action(17),
                work: work_id(2),
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
    business: WorkLease,
    business_capacity: CapacityBundle,
    cleanup: WorkLease,
    cleanup_capacity: CapacityBundle,
    first: InputId,
    failed: InputId,
    reserved: InputId,
    queued: InputId,
    old: Option<EffectTicket<u64>>,
    initial: Option<EffectTicket<u64>>,
}
fn fixture(cleanup_kind: WorkKind, start_cleanup: bool) -> Fixture {
    let history = HistoryCapture::new(256);
    let ledger = RuntimeLedger::with_history(vector(2), 2, 1, history.clone());
    let scope = ledger.open_scope_next(None, generation(), 2, 1).unwrap();
    let (mut business, business_capacity) = scope
        .reserve_next(
            generation(),
            vector(1),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    let (mut cleanup, cleanup_capacity) = scope
        .reserve_next(
            generation(),
            vector(1),
            cleanup_kind,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(cleanup_kind)
        .unwrap();
    business.start().unwrap();
    if start_cleanup {
        cleanup.start().unwrap();
    }
    let runtime = ActorRuntime::with_history(1, history.clone());
    runtime
        .register_next(
            aid(),
            scope.clone(),
            FailingActor,
            ActorConfig {
                mailbox: [8; 4],
                max_batches: 8,
                max_effects_per_step: 1,
                max_dependencies_per_effect: 0,
                turn_budget: 16,
                max_step_work: 1,
            },
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
    runtime.poll_one(Tick(2)).unwrap();
    let initial = runtime.take_effect(aid(), failed, action(17)).unwrap();
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
    }
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
    f.cleanup_capacity.release(&vector(1)).unwrap();
    f.runtime
        .effect_completed(f.initial.take().unwrap())
        .unwrap();
    f.runtime.retire_input(aid(), f.failed).unwrap();
    f.business.body_exited().unwrap();
    f.business.captures_exited().unwrap();
    f.business_capacity.release(&vector(1)).unwrap();
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
    work: [(WorkLease, CapacityBundle); 2],
    input: InputId,
    cleanup: EffectTicket<u64>,
}
fn failed_incarnation(runtime: &ActorRuntime<FailingActor>, ledger: &RuntimeLedger) -> Incarnation {
    let scope = ledger.open_scope_next(None, generation(), 2, 1).unwrap();
    let admit = |kind| {
        let (mut work, capacity) = scope
            .reserve_next(generation(), vector(1), kind, WorkAdmission::External)
            .unwrap()
            .bind(kind)
            .unwrap();
        work.start().unwrap();
        (work, capacity)
    };
    let work = [admit(WorkKind::Business), admit(WorkKind::Cleanup)];
    runtime
        .register_next(
            aid(),
            scope.clone(),
            FailingActor,
            ActorConfig {
                mailbox: [1; 4],
                max_batches: 1,
                max_effects_per_step: 1,
                max_dependencies_per_effect: 0,
                turn_budget: 8,
                max_step_work: 1,
            },
        )
        .unwrap();
    let input = runtime
        .reserve_input(aid(), aid(), InputClass::Ordinary)
        .unwrap()
        .fill(99)
        .unwrap();
    runtime.poll_one(Tick(1)).unwrap();
    let cleanup = runtime.take_effect(aid(), input, action(17)).unwrap();
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
        capacity.release(&vector(1)).unwrap();
    }
    runtime.effect_completed(incarnation.cleanup).unwrap();
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
    let ledger = RuntimeLedger::new(vector(2), 2, 1);
    let runtime = ActorRuntime::new(1);
    let first = failed_incarnation(&runtime, &ledger);
    let mut stale = runtime.claim_failure_publication(&first.cleanup).unwrap();
    let original_scope = first.scope.id();
    let original_input = first.input;
    let original_work = first.cleanup.descriptor.work;
    let original_generation = first.cleanup.generation;
    retire_incarnation(&runtime, &ledger, first);

    let next_ledger = if different_ledger {
        RuntimeLedger::new(vector(2), 2, 1)
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
    assert_eq!(second.cleanup.descriptor.work, original_work);
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
    assert_eq!(stale_matches, Err(DriverError::PublicationTargetMismatch));
    assert_eq!(current_matches, Ok(true));
    assert!(
        matches!(rejected, Err((DriverError::PublicationTargetMismatch, _))),
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
    let f = fixture(WorkKind::Cleanup, true);
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
    let unrelated = ActorRuntime::<FailingActor>::new(1);
    assert!(!session.matches_actor_runtime(&unrelated, aid()).unwrap());
    assert_eq!(
        f.runtime.claim_failure_publication(initial).unwrap_err(),
        DriverError::FailurePublicationAlreadyClaimed
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
        DriverError::FailurePublicationRequired
    );
    let actions: Vec<_> = f
        .history
        .drain(256)
        .entries
        .into_iter()
        .filter_map(|entry| match entry.fact {
            HistoryFact::Published {
                generation, action, ..
            } if generation == session.generation() => Some(action.get()),
            _ => None,
        })
        .collect();
    assert_eq!(
        actions,
        vec![18, 19, 20],
        "each actual commit has its own action after sparse failure action 17"
    );
    assert_eq!(
        f.runtime.retire_input(aid(), f.failed),
        Err(DriverError::EffectsRemain),
        "initial cleanup stays Started throughout publication slices"
    );
    drop(session);
    assert_eq!(
        f.runtime.claim_failure_publication(initial).unwrap_err(),
        DriverError::FailurePublicationAlreadyClaimed,
        "Drop cannot mint another authority or fake exit"
    );
    drain(f);
}

#[test]
fn previously_committed_reply_survives_failure_and_is_never_overwritten() {
    let f = fixture(WorkKind::Cleanup, true);
    let reply = &f.committed;
    let mut session = f
        .runtime
        .claim_failure_publication(f.initial.as_ref().unwrap())
        .unwrap();
    let (error, rejected) = session
        .commit_reply(reply, Arc::new("OwnerFailed"))
        .unwrap_err();
    assert_eq!(error, DriverError::ReplyAlreadyCommitted);
    assert_eq!(*rejected, "OwnerFailed");
    assert_eq!(*reply.take_once().unwrap(), "success");
    assert_eq!(
        session
            .commit_reply(reply, Arc::new("OwnerFailed"))
            .unwrap_err()
            .0,
        DriverError::ReplyAlreadyCommitted
    );
    drain(f);
}

#[test]
fn cleanup_work_must_be_started_and_still_have_a_live_body_for_each_commit() {
    let mut f = fixture(WorkKind::Cleanup, false);
    assert_eq!(
        f.runtime
            .claim_failure_publication(f.initial.as_ref().unwrap())
            .unwrap_err(),
        DriverError::FailureWorkInactive
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
        Err(DriverError::FailureWorkInactive)
    );
    assert_eq!(
        session.commit_snapshot(&target, Arc::new(1)).unwrap_err().0,
        DriverError::FailureWorkInactive
    );
    assert_eq!(*target.read(), 0);
    f.cleanup.captures_exited().unwrap();
    f.cleanup_capacity.release(&vector(1)).unwrap();
    drain_neutral_inputs(&f);
    f.runtime
        .effect_completed(f.initial.take().unwrap())
        .unwrap();
    f.runtime.retire_input(aid(), f.failed).unwrap();
    f.business.body_exited().unwrap();
    f.business.captures_exited().unwrap();
    f.business_capacity.release(&vector(1)).unwrap();
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
    let f = fixture(WorkKind::Business, true);
    assert_eq!(
        f.runtime
            .claim_failure_publication(f.initial.as_ref().unwrap())
            .unwrap_err(),
        DriverError::FailureWorkInactive
    );
    drain(f);
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
    let f = fixture(WorkKind::Cleanup, true);
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
    let f = fixture(WorkKind::Cleanup, true);
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
        f.runtime.retire_input(aid(), f.failed),
        Err(DriverError::EffectsRemain)
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
