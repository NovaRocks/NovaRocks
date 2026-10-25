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
    completion::ReportSlot,
    driver::{ActorRuntime, DriverError},
    resource::{ResourceError, ResourceStore},
    transaction::{RuntimeLedger, ScopeParent},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationRequest, ResourceVector},
    completion::ConsumerKind,
    effect::{EffectDescriptor, EffectKind},
    identity::*,
    mailbox::{InputClass, InputEnvelope},
    scope::{ScopeError, WorkAdmission, WorkKind},
};
fn sid(n: u64) -> ScopeId {
    ScopeId::new(n).unwrap()
}
fn wid(n: u64) -> WorkId {
    WorkId::new(n).unwrap()
}
fn slot(n: u64) -> SlotId {
    SlotId::new(n).unwrap()
}
fn aid(n: u64) -> ActorId {
    ActorId::new(n).unwrap()
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
fn ledger() -> RuntimeLedger {
    RuntimeLedger::new(vector(8), 16, 8)
}
fn request(n: u64, amount: u64) -> ReservationRequest {
    ReservationRequest {
        id: ReservationId::new(n).unwrap(),
        generation: generation(),
        guard: wid(n),
        resources: vector(amount),
        admission: WorkAdmission::External,
        kind: WorkKind::Business,
    }
}
fn config() -> ActorConfig {
    ActorConfig {
        mailbox: [8; 4],
        max_batches: 8,
        max_effects_per_step: 2,
        max_dependencies_per_effect: 2,
        turn_budget: 16,
        max_step_work: 2,
    }
}
#[derive(Debug)]
struct Recorder {
    seen: Vec<(u64, u64)>,
    effects: bool,
    panic_on: Option<u64>,
    deadline: Option<Tick>,
}
impl Actor for Recorder {
    type Input = u64;
    type Effect = u64;
    fn timer_input(now: Tick) -> u64 {
        now.0
    }
    fn handle(
        &mut self,
        _now: Tick,
        input: InputEnvelope,
        value: u64,
        step: &mut StepContext<'_, u64>,
    ) {
        if self.panic_on == Some(value) {
            panic!("controlled protocol owner failure");
        }
        self.seen.push((input.source.get(), value));
        self.deadline = None;
        if self.effects {
            step.effect(
                EffectDescriptor {
                    id: action(1),
                    work: wid(input.input.get()),
                    kind: EffectKind::Publication,
                    dependencies: vec![],
                },
                value,
            )
            .unwrap();
            step.effect(
                EffectDescriptor {
                    id: action(2),
                    work: wid(input.input.get()),
                    kind: EffectKind::Business,
                    dependencies: vec![action(1)],
                },
                value + 100,
            )
            .unwrap();
        }
    }
    fn next_deadline(&self) -> Option<Tick> {
        self.deadline
    }
    fn owner_failed(
        _scope: ScopeId,
        _generation: Generation,
        input: InputEnvelope,
        step: &mut StepContext<'_, u64>,
    ) {
        step.effect(
            EffectDescriptor {
                id: action(1),
                work: wid(input.input.get()),
                kind: EffectKind::Cleanup,
                dependencies: vec![],
            },
            999,
        )
        .unwrap();
    }
}
fn quiet<A: Actor>(runtime: &ActorRuntime<A>) {
    while let Some(actor) = runtime.claim_effect_actor() {
        runtime.release_effect_actor(actor, false).unwrap();
    }
}
fn remove_actor<A: Actor>(runtime: &ActorRuntime<A>, actor: ActorId) -> Result<A, DriverError> {
    quiet(runtime);
    runtime.remove_actor(actor)
}
fn recorder(effects: bool) -> Recorder {
    Recorder {
        seen: vec![],
        effects,
        panic_on: None,
        deadline: None,
    }
}

#[test]
fn reserve_guard_is_scope_visible_and_only_unbound_guard_rolls_back() {
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let guard = scope.reserve(request(1, 3)).unwrap();
    scope.close_external().unwrap();
    assert_eq!(scope.snapshot().unwrap().work, 1);
    assert!(!scope.snapshot().unwrap().join_ready);
    assert_eq!(ledger.used(DimensionId::new(1).unwrap()), 3);
    drop(guard);
    assert_eq!(ledger.used(DimensionId::new(1).unwrap()), 0);
    assert!(scope.snapshot().unwrap().retirement_ready);
    ledger.remove_scope(sid(1)).unwrap();
}
#[test]
fn dropping_a_bound_lease_or_capacity_never_invents_exit() {
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let (work, bundle) = scope
        .reserve(request(1, 3))
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    drop(work);
    drop(bundle);
    scope.close_external().unwrap();
    assert_eq!(ledger.unresolved_work(), vec![(sid(1), wid(1))]);
    assert_eq!(
        ledger.abandoned_bundles(),
        vec![ReservationId::new(1).unwrap()]
    );
    assert_eq!(ledger.used(DimensionId::new(1).unwrap()), 3);
    assert!(!scope.snapshot().unwrap().retirement_ready);
}
#[test]
fn work_join_and_storage_or_capacity_retirement_are_distinct() {
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let (mut work, mut bundle) = scope
        .reserve(request(1, 3))
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    let consumer = ConsumerId::new(1).unwrap();
    let report = ReportSlot::reserve(
        &scope,
        slot(1),
        generation(),
        wid(1),
        2,
        [(
            consumer,
            ConsumerKind::Owner {
                scope: sid(1),
                generation: generation(),
            },
        )],
    )
    .unwrap();
    work.start().unwrap();
    work.body_exited().unwrap();
    work.captures_exited().unwrap();
    scope.close_external().unwrap();
    ledger.reap_ready(1).unwrap();
    assert!(scope.snapshot().unwrap().join_ready);
    assert!(!scope.snapshot().unwrap().retirement_ready);
    report.fill().unwrap();
    report.body_exited().unwrap();
    report.captures_exited().unwrap();
    assert!(!report.try_retire().unwrap());
    report.consume(consumer).unwrap();
    assert!(report.try_retire().unwrap());
    assert!(!scope.snapshot().unwrap().retirement_ready);
    bundle.release(&vector(3)).unwrap();
    assert!(scope.snapshot().unwrap().retirement_ready);
    ledger.remove_scope(sid(1)).unwrap();
}
#[test]
fn parent_scope_counts_actual_child_scope_until_complete_retirement() {
    let ledger = ledger();
    let parent = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let child = ledger
        .open_scope(
            sid(2),
            Some(ScopeParent {
                scope: sid(1),
                generation: generation(),
                work: wid(1),
                kind: WorkKind::Business,
                admission: WorkAdmission::External,
            }),
            generation(),
            8,
            8,
        )
        .unwrap();
    parent.close_external().unwrap();
    assert!(!parent.snapshot().unwrap().join_ready);
    child.close_external().unwrap();
    assert!(!parent.snapshot().unwrap().join_ready);
    ledger.remove_scope(sid(2)).unwrap();
    ledger.reap_ready(1).unwrap();
    assert!(parent.snapshot().unwrap().join_ready);
    ledger.remove_scope(sid(1)).unwrap();
}
#[test]
fn close_allows_existing_parent_drain_and_stop_allows_only_cleanup() {
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let (mut parent, mut bundle) = scope
        .reserve(request(1, 1))
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    parent.start().unwrap();
    scope.close_external().unwrap();
    let mut child_request = request(2, 1);
    child_request.admission = WorkAdmission::Derived(wid(1));
    let (mut child, mut child_bundle) = parent
        .derive(child_request)
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    scope.request_stop().unwrap();
    assert_eq!(
        child.start().unwrap_err(),
        novarocks_control_runtime::transaction::RuntimeError::Scope(ScopeError::Stopped)
    );
    let mut cleanup_request = request(3, 1);
    cleanup_request.kind = WorkKind::Cleanup;
    let (mut cleanup, mut cleanup_bundle) = parent
        .derive(cleanup_request)
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    cleanup.start().unwrap();
    cleanup.body_exited().unwrap();
    cleanup.captures_exited().unwrap();
    cleanup_bundle.release(&vector(1)).unwrap();
    child.body_exited().unwrap();
    child.captures_exited().unwrap();
    child_bundle.release(&vector(1)).unwrap();
    parent.body_exited().unwrap();
    parent.captures_exited().unwrap();
    bundle.release(&vector(1)).unwrap();
    ledger.reap_ready(8).unwrap();
    assert!(scope.snapshot().unwrap().retirement_ready);
}
#[test]
fn failed_owner_abandons_only_its_leg_and_still_waits_for_captures() {
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let mut work = scope
        .admit_work(
            generation(),
            wid(1),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap();
    let owner = ConsumerId::new(1).unwrap();
    let observer = ConsumerId::new(2).unwrap();
    let report = ReportSlot::reserve(
        &scope,
        slot(1),
        generation(),
        wid(1),
        2,
        [
            (
                owner,
                ConsumerKind::Owner {
                    scope: sid(1),
                    generation: generation(),
                },
            ),
            (observer, ConsumerKind::Observer),
        ],
    )
    .unwrap();
    scope.fail().unwrap();
    report.fill().unwrap();
    report
        .abandon_failed_owner(owner, &scope, generation())
        .unwrap();
    report.body_exited().unwrap();
    report.consume(observer).unwrap();
    assert!(!report.try_retire().unwrap());
    report.captures_exited().unwrap();
    assert!(report.try_retire().unwrap());
    work.body_exited().unwrap();
    work.captures_exited().unwrap();
    ledger.reap_ready(1).unwrap();
    assert!(scope.snapshot().unwrap().retirement_ready);
}
#[test]
fn reserved_input_remains_bounded_until_effects_and_explicit_retirement() {
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let runtime = ActorRuntime::new(1);
    let mut c = config();
    c.mailbox[2] = 1;
    runtime
        .register(aid(1), scope.clone(), slot(1), recorder(true), c)
        .unwrap();
    let permit = runtime
        .reserve_input(aid(1), aid(2), InputClass::Ordinary)
        .unwrap();
    let input = permit.id();
    permit.fill(7).unwrap();
    runtime.poll_one(Tick(1)).unwrap();
    assert_eq!(
        runtime
            .reserve_input(aid(1), aid(2), InputClass::Ordinary)
            .err(),
        Some(DriverError::InputCapacity)
    );
    assert_eq!(
        runtime.ready_effects(aid(1), input, 4).unwrap(),
        vec![action(1)]
    );
    assert_eq!(
        runtime.retire_input(aid(1), input),
        Err(DriverError::EffectsRemain)
    );
    let first = runtime.take_effect(aid(1), input, action(1)).unwrap();
    assert_eq!(first.effect, 7);
    runtime.observe_external_publication(&first).unwrap();
    // A publication alone does not silently satisfy a completion dependency.
    assert!(runtime.ready_effects(aid(1), input, 4).unwrap().is_empty());
    runtime.effect_completed(first).unwrap();
    runtime
        .acknowledge_dependency(aid(1), input, action(2), action(1))
        .unwrap();
    let second = runtime.take_effect(aid(1), input, action(2)).unwrap();
    assert_eq!(second.effect, 107);
    runtime.effect_completed(second).unwrap();
    runtime.retire_input(aid(1), input).unwrap();
    let unused = runtime
        .reserve_input(aid(1), aid(2), InputClass::Ordinary)
        .unwrap();
    unused.revoke_unused().unwrap();
    scope.close_external().unwrap();
    assert!(scope.snapshot().unwrap().join_ready);
    assert!(!scope.snapshot().unwrap().retirement_ready);
    let actor = remove_actor(&runtime, aid(1)).unwrap();
    assert_eq!(actor.seen, vec![(2, 7)]);
    ledger.remove_scope(sid(1)).unwrap();
}
#[test]
fn exact_source_pair_fifo_is_preserved_across_control_priority() {
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let runtime = ActorRuntime::new(1);
    runtime
        .register(aid(1), scope.clone(), slot(1), recorder(false), config())
        .unwrap();
    let first = runtime
        .reserve_input(aid(1), aid(2), InputClass::Ordinary)
        .unwrap()
        .fill(1)
        .unwrap();
    let second = runtime
        .reserve_input(aid(1), aid(2), InputClass::Control)
        .unwrap()
        .fill(2)
        .unwrap();
    runtime.poll_one(Tick(1)).unwrap();
    runtime.retire_input(aid(1), first).unwrap();
    runtime.retire_input(aid(1), second).unwrap();
    scope.close_external().unwrap();
    assert_eq!(
        remove_actor(&runtime, aid(1)).unwrap().seen,
        vec![(2, 1), (2, 2)]
    );
}
#[test]
fn existing_input_permits_fill_after_external_close_but_new_reservations_fail() {
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let runtime = ActorRuntime::new(1);
    runtime
        .register(aid(1), scope.clone(), slot(1), recorder(false), config())
        .unwrap();
    let permit = runtime
        .reserve_input(aid(1), aid(2), InputClass::Lifecycle)
        .unwrap();
    runtime.close_admission();
    assert_eq!(
        runtime
            .reserve_input(aid(1), aid(2), InputClass::Control)
            .err(),
        Some(DriverError::Closed)
    );
    let input = permit.fill(1).unwrap();
    runtime.poll_one(Tick(1)).unwrap();
    runtime.retire_input(aid(1), input).unwrap();
    scope.close_external().unwrap();
    remove_actor(&runtime, aid(1)).unwrap();
}
#[test]
fn initial_timer_uses_same_actor_turn_and_its_own_bounded_cell() {
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let runtime = ActorRuntime::new(1);
    let mut actor = recorder(false);
    actor.deadline = Some(Tick(5));
    runtime
        .register(aid(1), scope.clone(), slot(1), actor, config())
        .unwrap();
    assert_eq!(runtime.advance_time(Tick(4), 8).unwrap(), 0);
    assert_eq!(runtime.advance_time(Tick(5), 8).unwrap(), 1);
    assert_eq!(runtime.advance_time(Tick(6), 8).unwrap(), 0);
    runtime.poll_one(Tick(6)).unwrap();
    runtime
        .retire_input(aid(1), InputId::new(1).unwrap())
        .unwrap();
    scope.close_external().unwrap();
    assert_eq!(remove_actor(&runtime, aid(1)).unwrap().seen, vec![(1, 5)]);
}
#[test]
fn owner_failure_fences_old_business_but_keeps_published_commitments() {
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let runtime = ActorRuntime::new(1);
    let mut actor = recorder(true);
    actor.panic_on = Some(2);
    runtime
        .register(aid(1), scope.clone(), slot(1), actor, config())
        .unwrap();
    let first = runtime
        .reserve_input(aid(1), aid(2), InputClass::Ordinary)
        .unwrap()
        .fill(1)
        .unwrap();
    runtime.poll_one(Tick(1)).unwrap();
    let published = runtime.take_effect(aid(1), first, action(1)).unwrap();
    runtime.observe_external_publication(&published).unwrap();
    let failed = runtime
        .reserve_input(aid(1), aid(2), InputClass::Ordinary)
        .unwrap()
        .fill(2)
        .unwrap();
    runtime.poll_one(Tick(2)).unwrap();
    assert!(runtime.snapshot(aid(1)).unwrap().failed);
    assert!(runtime.take_effect(aid(1), first, action(2)).is_err());
    runtime.effect_completed(published).unwrap();
    runtime
        .abandon_unstarted_effect(aid(1), first, action(2))
        .unwrap();
    runtime.retire_input(aid(1), first).unwrap();
    let cleanup = runtime.take_effect(aid(1), failed, action(1)).unwrap();
    assert_eq!(cleanup.effect, 999);
    runtime.effect_completed(cleanup).unwrap();
    runtime.retire_input(aid(1), failed).unwrap();
    drop(runtime.take_failure_payload(aid(1)).unwrap().unwrap());
    remove_actor(&runtime, aid(1)).unwrap();
}
#[test]
fn resource_recovery_uses_actual_entry_and_never_double_takes_or_scans_unboundedly() {
    let resources = ResourceStore::new(3);
    let first = resources
        .insert(slot(1), sid(2), generation(), 11u64)
        .unwrap();
    let second = resources
        .insert(slot(2), sid(1), generation(), 22u64)
        .unwrap();
    let inventory = resources.inventory(sid(1), generation(), None, 1);
    assert_eq!(inventory.scanned, 1);
    assert!(inventory.ids.is_empty());
    assert_eq!(inventory.next_after, Some(slot(1)));
    let inventory = resources.inventory(sid(1), generation(), inventory.next_after, 1);
    assert_eq!(inventory.ids, vec![slot(2)]);
    assert_eq!(
        resources.take_owned(slot(2), sid(1), generation()).unwrap(),
        22
    );
    assert_eq!(second.take().unwrap_err(), ResourceError::Missing);
    assert_eq!(first.take().unwrap(), 11);
    assert!(resources.is_empty());
}

#[test]
fn global_external_close_keeps_healthy_parent_drain_and_cleanup_capacity() {
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let (mut parent, mut bundle) = scope
        .reserve(request(1, 1))
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    parent.start().unwrap();
    ledger.close_admission();
    assert_eq!(
        ledger.open_scope(sid(2), None, generation(), 8, 8).err(),
        Some(novarocks_control_runtime::transaction::RuntimeError::Scope(
            ScopeError::Closed
        ))
    );
    assert_eq!(
        scope.reserve(request(2, 1)).err(),
        Some(novarocks_control_runtime::transaction::RuntimeError::Scope(
            ScopeError::Closed
        ))
    );
    let (mut child, mut child_bundle) = parent
        .derive(request(2, 1))
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    child.start().unwrap();
    let closed = ledger.close_scopes(None, 1);
    assert_eq!(closed, vec![sid(1)]);
    scope.request_stop().unwrap();
    let mut cleanup_request = request(3, 1);
    cleanup_request.kind = WorkKind::Cleanup;
    let (mut cleanup, mut cleanup_bundle) = parent
        .derive(cleanup_request)
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    cleanup.start().unwrap();
    for work in [&mut child, &mut cleanup, &mut parent] {
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
    }
    for charge in [&mut child_bundle, &mut cleanup_bundle, &mut bundle] {
        charge.release(&vector(1)).unwrap();
    }
    ledger.reap_ready(8).unwrap();
    assert!(scope.snapshot().unwrap().retirement_ready);
}

#[test]
fn shared_history_observes_real_handoffs_without_becoming_a_policy_input() {
    use novarocks_control_runtime::{history::HistoryCapture, transaction::RuntimeLedger};
    use novarocks_control_runtime_core::history::HistoryFact;
    let history = HistoryCapture::new(64);
    let ledger = RuntimeLedger::with_history(vector(8), 16, 8, history.clone());
    let runtime = ActorRuntime::with_history(1, history.clone());
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let mut work = scope
        .admit_work(
            generation(),
            wid(1),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap();
    runtime
        .register(aid(1), scope.clone(), slot(1), recorder(false), config())
        .unwrap();
    let input = runtime
        .reserve_input(aid(1), aid(2), InputClass::Ordinary)
        .unwrap()
        .fill(7)
        .unwrap();
    runtime.poll_one(Tick(10)).unwrap();
    work.start().unwrap();
    work.body_exited().unwrap();
    work.captures_exited().unwrap();
    ledger.reap_ready(1).unwrap();
    runtime.retire_input(aid(1), input).unwrap();
    scope.close_external().unwrap();
    remove_actor(&runtime, aid(1)).unwrap();
    let slice = history.drain(64);
    assert!(slice.complete);
    assert!(
        slice
            .entries
            .windows(2)
            .all(|pair| pair[0].ordinal + 1 == pair[1].ordinal)
    );
    assert!(slice.entries.iter().any(|entry| entry.fact
        == HistoryFact::BatchCommitted {
            scope: sid(1),
            input
        }));
    let exits: Vec<_> = slice
        .entries
        .iter()
        .filter(|entry| {
            matches!(
                entry.fact,
                HistoryFact::BodyExited { .. } | HistoryFact::CapturesExited { .. }
            )
        })
        .collect();
    assert_eq!(exits.len(), 2);
    assert!(exits.iter().all(|entry| entry.now == Tick(10)));
}

#[test]
fn history_pressure_invalidates_history_but_does_not_reject_work() {
    use novarocks_control_runtime::history::HistoryCapture;
    use novarocks_control_runtime_core::history::HistoryError;
    let history = HistoryCapture::new(1);
    let ledger = RuntimeLedger::with_history(vector(8), 16, 8, history.clone());
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let mut work = scope
        .admit_work(
            generation(),
            wid(1),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap();
    work.start().unwrap();
    work.body_exited().unwrap();
    work.captures_exited().unwrap();
    scope.close_external().unwrap();
    ledger.reap_ready(1).unwrap();
    assert!(scope.snapshot().unwrap().retirement_ready);
    let slice = history.drain(8);
    assert_eq!(slice.entries.len(), 1);
    assert!(!slice.complete);
    assert_eq!(slice.error, Some(HistoryError::Full));
}

#[test]
fn complete_step_budget_is_reserved_before_core_mutation_for_all_four_classes() {
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let runtime = ActorRuntime::new(1);
    let mut actor = recorder(true);
    actor.deadline = Some(Tick(1));
    let mut policy = config();
    policy.turn_budget = 12;
    policy.max_step_work = 2;
    runtime
        .register(aid(1), scope.clone(), slot(1), actor, policy)
        .unwrap();
    for (source, class, value) in [
        (2, InputClass::Lifecycle, 10),
        (3, InputClass::Control, 20),
        (4, InputClass::Ordinary, 30),
    ] {
        runtime
            .reserve_input(aid(1), aid(source), class)
            .unwrap()
            .fill(value)
            .unwrap();
    }
    runtime.advance_time(Tick(1), 1).unwrap();
    runtime.poll_one(Tick(1)).unwrap();
    assert_eq!(runtime.snapshot(aid(1)).unwrap().batches, 4);
    assert!(!runtime.snapshot(aid(1)).unwrap().failed);
    for input in runtime.pending_batches(aid(1), None, 8).unwrap() {
        let first = runtime.take_effect(aid(1), input, action(1)).unwrap();
        runtime.observe_external_publication(&first).unwrap();
        runtime.effect_completed(first).unwrap();
        runtime
            .acknowledge_dependency(aid(1), input, action(2), action(1))
            .unwrap();
        let second = runtime.take_effect(aid(1), input, action(2)).unwrap();
        runtime.effect_completed(second).unwrap();
        runtime.retire_input(aid(1), input).unwrap();
    }
    scope.close_external().unwrap();
    let actor = remove_actor(&runtime, aid(1)).unwrap();
    assert_eq!(actor.seen, vec![(2, 10), (3, 20), (4, 30), (1, 1)]);
}

#[test]
fn impossible_whole_step_quota_is_rejected_at_configuration_time() {
    let mut policy = config();
    policy.max_step_work = 16;
    policy.turn_budget = 64;
    assert!(policy.validate().is_err());
    policy.max_step_work = usize::MAX;
    assert!(policy.validate().is_err());
}

#[test]
fn unused_whole_step_reservation_returns_to_the_same_fair_turn() {
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let runtime = ActorRuntime::new(1);
    let mut policy = config();
    policy.turn_budget = 64;
    policy.max_step_work = 15;
    policy.mailbox[2] = 64;
    policy.max_batches = 64;
    runtime
        .register(aid(1), scope.clone(), slot(1), recorder(false), policy)
        .unwrap();
    let inputs: Vec<_> = (0..40)
        .map(|value| {
            runtime
                .reserve_input(aid(1), aid(2), InputClass::Ordinary)
                .unwrap()
                .fill(value)
                .unwrap()
        })
        .collect();
    runtime.poll_one(Tick(1)).unwrap();
    assert_eq!(runtime.snapshot(aid(1)).unwrap().batches, 40);
    for input in inputs {
        runtime.retire_input(aid(1), input).unwrap();
    }
    // A coalesced readiness notification may still require one empty bounded turn.
    runtime.poll_one(Tick(1)).unwrap();
    scope.close_external().unwrap();
    remove_actor(&runtime, aid(1)).unwrap();
}

#[test]
fn reliable_exit_report_validation_precedes_capacity_or_work_mutation() {
    use novarocks_control_runtime::transaction::RuntimeError;
    use novarocks_control_runtime_core::completion::CompletionError;
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let (mut work, mut bundle) = scope
        .reserve(request(1, 3))
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    let consumer = ConsumerId::new(1).unwrap();
    let body = ReportSlot::reserve(
        &scope,
        slot(1),
        generation(),
        wid(1),
        1,
        [(consumer, ConsumerKind::Observer)],
    )
    .unwrap();
    let released = ReportSlot::reserve(
        &scope,
        slot(2),
        generation(),
        wid(1),
        1,
        [(consumer, ConsumerKind::Observer)],
    )
    .unwrap();
    let captures = ReportSlot::reserve(
        &scope,
        slot(3),
        generation(),
        wid(1),
        1,
        [(consumer, ConsumerKind::Observer)],
    )
    .unwrap();
    work.body_exited_with_report(&body).unwrap();
    assert_eq!(
        bundle.release_with_report(&vector(3), &body),
        Err(RuntimeError::Completion(CompletionError::AlreadyFilled))
    );
    assert_eq!(ledger.used(DimensionId::new(1).unwrap()), 3);
    bundle.release_with_report(&vector(3), &released).unwrap();
    assert_eq!(ledger.used(DimensionId::new(1).unwrap()), 0);
    work.captures_exited_with_report(&captures).unwrap();
    for report in [&body, &released, &captures] {
        report.consume(consumer).unwrap();
        report.body_exited().unwrap();
        report.captures_exited().unwrap();
        assert!(report.try_retire().unwrap());
    }
    scope.close_external().unwrap();
    ledger.reap_ready(1).unwrap();
    assert!(scope.snapshot().unwrap().retirement_ready);
}

#[test]
fn bound_unfilled_role_can_withdraw_after_its_holder_exit_without_owner_failure() {
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let runtime = ActorRuntime::new(1);
    let mut policy = config();
    policy.mailbox[2] = 1;
    runtime
        .register(aid(1), scope.clone(), slot(1), recorder(false), policy)
        .unwrap();
    let mut role = scope
        .admit_work(
            generation(),
            wid(1),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap();
    let permit = runtime
        .reserve_input(aid(1), aid(2), InputClass::Ordinary)
        .unwrap();
    permit.bind().unwrap();
    assert_eq!(
        runtime
            .reserve_input(aid(1), aid(2), InputClass::Ordinary)
            .err(),
        Some(DriverError::InputCapacity)
    );
    // The host observes canceled body/captures; no command was ever enqueued.
    role.body_exited().unwrap();
    role.captures_exited().unwrap();
    ledger.reap_ready(1).unwrap();
    permit.revoke_bound_after_exit().unwrap();
    runtime
        .reserve_input(aid(1), aid(2), InputClass::Ordinary)
        .unwrap()
        .revoke_unused()
        .unwrap();
    assert!(!runtime.snapshot(aid(1)).unwrap().failed);
    scope.close_external().unwrap();
    remove_actor(&runtime, aid(1)).unwrap();
}

fn finish_failed_publication_test(
    runtime: &ActorRuntime<Recorder>,
    old_input: InputId,
    failed_input: InputId,
    ticket: novarocks_control_runtime::driver::EffectTicket<u64>,
) {
    runtime.effect_completed(ticket).unwrap();
    runtime
        .abandon_unstarted_effect(aid(1), old_input, action(2))
        .unwrap();
    runtime.retire_input(aid(1), old_input).unwrap();
    let cleanup = runtime
        .take_effect(aid(1), failed_input, action(1))
        .unwrap();
    runtime.effect_completed(cleanup).unwrap();
    runtime.retire_input(aid(1), failed_input).unwrap();
    drop(runtime.take_failure_payload(aid(1)).unwrap().unwrap());
    remove_actor(runtime, aid(1)).unwrap();
}

#[test]
fn taken_old_ticket_cannot_replace_snapshot_after_owner_failure() {
    use novarocks_control_runtime::publication::PublishedSnapshot;
    use novarocks_control_runtime_core::effect::EffectError;
    use std::sync::Arc;
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let runtime = ActorRuntime::new(1);
    let mut actor = recorder(true);
    actor.panic_on = Some(2);
    runtime
        .register(aid(1), scope.clone(), slot(1), actor, config())
        .unwrap();
    let snapshot = PublishedSnapshot::new(&scope, Arc::new(0u64));
    let input = runtime
        .reserve_input(aid(1), aid(2), InputClass::Ordinary)
        .unwrap()
        .fill(1)
        .unwrap();
    runtime.poll_one(Tick(1)).unwrap();
    let ticket = runtime.take_effect(aid(1), input, action(1)).unwrap();
    let failed = runtime
        .reserve_input(aid(1), aid(2), InputClass::Ordinary)
        .unwrap()
        .fill(2)
        .unwrap();
    runtime.poll_one(Tick(2)).unwrap();
    let (error, rejected) = runtime
        .commit_snapshot(&ticket, &snapshot, Arc::new(7))
        .unwrap_err();
    assert_eq!(error, DriverError::Effect(EffectError::StaleGeneration));
    assert_eq!(*rejected, 7);
    assert_eq!(*snapshot.read(), 0);
    finish_failed_publication_test(&runtime, input, failed, ticket);
}

#[test]
fn committed_snapshot_survives_later_owner_failure_and_old_arc_is_returned() {
    use novarocks_control_runtime::publication::PublishedSnapshot;
    use std::sync::Arc;
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let runtime = ActorRuntime::new(1);
    let mut actor = recorder(true);
    actor.panic_on = Some(2);
    runtime
        .register(aid(1), scope.clone(), slot(1), actor, config())
        .unwrap();
    let initial = Arc::new(0u64);
    let snapshot = PublishedSnapshot::new(&scope, initial.clone());
    let input = runtime
        .reserve_input(aid(1), aid(2), InputClass::Ordinary)
        .unwrap()
        .fill(1)
        .unwrap();
    runtime.poll_one(Tick(1)).unwrap();
    let ticket = runtime.take_effect(aid(1), input, action(1)).unwrap();
    let old = runtime
        .commit_snapshot(&ticket, &snapshot, Arc::new(7))
        .unwrap();
    assert!(Arc::ptr_eq(&old, &initial));
    drop(old);
    let failed = runtime
        .reserve_input(aid(1), aid(2), InputClass::Ordinary)
        .unwrap()
        .fill(2)
        .unwrap();
    runtime.poll_one(Tick(2)).unwrap();
    assert_eq!(*snapshot.read(), 7);
    finish_failed_publication_test(&runtime, input, failed, ticket);
}

#[test]
fn publication_ticket_cannot_target_another_scope_snapshot() {
    use novarocks_control_runtime::publication::PublishedSnapshot;
    use std::sync::Arc;
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let other = ledger.open_scope(sid(2), None, generation(), 8, 8).unwrap();
    let runtime = ActorRuntime::new(1);
    runtime
        .register(aid(1), scope.clone(), slot(1), recorder(true), config())
        .unwrap();
    let snapshot = PublishedSnapshot::new(&other, Arc::new(0u64));
    let input = runtime
        .reserve_input(aid(1), aid(2), InputClass::Ordinary)
        .unwrap()
        .fill(1)
        .unwrap();
    runtime.poll_one(Tick(1)).unwrap();
    let first = runtime.take_effect(aid(1), input, action(1)).unwrap();
    assert_eq!(
        runtime
            .commit_snapshot(&first, &snapshot, Arc::new(7))
            .unwrap_err()
            .0,
        DriverError::PublicationTargetMismatch
    );
    assert_eq!(*snapshot.read(), 0);
    runtime.effect_completed(first).unwrap();
    runtime
        .acknowledge_dependency(aid(1), input, action(2), action(1))
        .unwrap();
    let second = runtime.take_effect(aid(1), input, action(2)).unwrap();
    runtime.effect_completed(second).unwrap();
    runtime.retire_input(aid(1), input).unwrap();
    scope.close_external().unwrap();
    remove_actor(&runtime, aid(1)).unwrap();
    other.close_external().unwrap();
}

#[test]
fn committed_reply_remains_once_ever_after_failure_and_take() {
    use novarocks_control_runtime::publication::PublishedReply;
    use std::sync::Arc;
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let runtime = ActorRuntime::new(1);
    let mut actor = recorder(true);
    actor.panic_on = Some(2);
    runtime
        .register(aid(1), scope.clone(), slot(1), actor, config())
        .unwrap();
    let reply = PublishedReply::new(&scope);
    let input = runtime
        .reserve_input(aid(1), aid(2), InputClass::Ordinary)
        .unwrap()
        .fill(1)
        .unwrap();
    runtime.poll_one(Tick(1)).unwrap();
    let first = runtime.take_effect(aid(1), input, action(1)).unwrap();
    runtime.commit_reply(&first, &reply, Arc::new(7)).unwrap();
    let failed = runtime
        .reserve_input(aid(1), aid(2), InputClass::Ordinary)
        .unwrap()
        .fill(2)
        .unwrap();
    runtime.poll_one(Tick(2)).unwrap();
    assert_eq!(*reply.take_once().unwrap(), 7);
    assert!(reply.take_once().is_none());
    assert!(reply.is_committed());
    let cleanup = runtime.take_effect(aid(1), failed, action(1)).unwrap();
    assert_eq!(
        runtime
            .commit_reply(&cleanup, &reply, Arc::new(999))
            .unwrap_err()
            .0,
        DriverError::ReplyAlreadyCommitted
    );
    runtime.effect_completed(cleanup).unwrap();
    runtime.retire_input(aid(1), failed).unwrap();
    runtime.effect_completed(first).unwrap();
    runtime
        .abandon_unstarted_effect(aid(1), input, action(2))
        .unwrap();
    runtime.retire_input(aid(1), input).unwrap();
    drop(runtime.take_failure_payload(aid(1)).unwrap().unwrap());
    remove_actor(&runtime, aid(1)).unwrap();
}

#[test]
fn prepaid_role_advances_cursor_after_global_external_close_without_new_credit() {
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let runtime = ActorRuntime::new(1);
    let mut policy = config();
    policy.mailbox[0] = 1;
    runtime
        .register(aid(1), scope.clone(), slot(1), recorder(false), policy)
        .unwrap();
    let permit = runtime
        .reserve_input(aid(1), aid(2), InputClass::Lifecycle)
        .unwrap();
    let original = permit.id();
    let mut role = permit.into_role().unwrap();
    runtime.close_admission();
    ledger.close_admission();
    assert_eq!(
        runtime
            .reserve_input(aid(1), aid(2), InputClass::Control)
            .err(),
        Some(DriverError::Closed)
    );
    let mut previous = original;
    for cursor in 10..13 {
        let input = role.fill(cursor).unwrap();
        assert!(input > previous);
        previous = input;
        runtime.poll_one(Tick(cursor)).unwrap();
        runtime.retire_input(aid(1), input).unwrap();
        assert_eq!(role.in_flight().unwrap(), None);
        assert_eq!(runtime.snapshot(aid(1)).unwrap().roles, 1);
    }
    scope.close_external().unwrap();
    assert!(scope.join_ready().unwrap());
    assert_eq!(
        runtime.remove_actor(aid(1)).err(),
        Some(DriverError::ActorNotRetired)
    );
    role.close_after_exit().unwrap();
    assert_eq!(
        remove_actor(&runtime, aid(1)).unwrap().seen,
        vec![(2, 10), (2, 11), (2, 12)]
    );
}

#[test]
fn role_inflight_credit_waits_for_effect_retirement_and_never_reuses_input_id() {
    use novarocks_control_runtime::{driver::EffectTicket, publication::PublishedSnapshot};
    use std::sync::Arc;
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let runtime = ActorRuntime::new(1);
    let mut policy = config();
    policy.mailbox[2] = 1;
    runtime
        .register(aid(1), scope.clone(), slot(1), recorder(true), policy)
        .unwrap();
    let mut role = runtime
        .reserve_input(aid(1), aid(2), InputClass::Ordinary)
        .unwrap()
        .into_role()
        .unwrap();
    let first = role.fill(7).unwrap();
    assert_eq!(role.fill(8).unwrap_err(), (DriverError::RoleBusy, 8));
    runtime.poll_one(Tick(1)).unwrap();
    assert_eq!(role.fill(8).unwrap_err(), (DriverError::RoleBusy, 8));
    let old = runtime.take_effect(aid(1), first, action(1)).unwrap();
    let obsolete = EffectTicket {
        actor: old.actor,
        input: old.input,
        generation: old.generation,
        descriptor: old.descriptor.clone(),
        effect: old.effect,
    };
    runtime.observe_external_publication(&old).unwrap();
    runtime.effect_completed(old).unwrap();
    assert_eq!(role.close_after_exit(), Err(DriverError::RoleBusy));
    runtime
        .acknowledge_dependency(aid(1), first, action(2), action(1))
        .unwrap();
    let last = runtime.take_effect(aid(1), first, action(2)).unwrap();
    runtime.effect_completed(last).unwrap();
    assert_eq!(role.fill(8).unwrap_err(), (DriverError::RoleBusy, 8));
    runtime.retire_input(aid(1), first).unwrap();
    // Retirement returns the role to idle, but does not give its credit to new admissions.
    assert_eq!(
        runtime
            .reserve_input(aid(1), aid(3), InputClass::Ordinary)
            .err(),
        Some(DriverError::InputCapacity)
    );
    let second = role.fill(9).unwrap();
    assert!(second > first);
    runtime.poll_one(Tick(2)).unwrap();
    let snapshot = PublishedSnapshot::new(&scope, Arc::new(0u64));
    assert_eq!(
        runtime
            .commit_snapshot(&obsolete, &snapshot, Arc::new(999))
            .unwrap_err()
            .0,
        DriverError::UnknownInput
    );
    assert_eq!(*snapshot.read(), 0);
    let fresh = runtime.take_effect(aid(1), second, action(1)).unwrap();
    runtime
        .commit_snapshot(&fresh, &snapshot, Arc::new(9))
        .unwrap();
    runtime.effect_completed(fresh).unwrap();
    runtime
        .acknowledge_dependency(aid(1), second, action(2), action(1))
        .unwrap();
    let last = runtime.take_effect(aid(1), second, action(2)).unwrap();
    runtime.effect_completed(last).unwrap();
    runtime.retire_input(aid(1), second).unwrap();
    role.close_after_exit().unwrap();
    scope.close_external().unwrap();
    remove_actor(&runtime, aid(1)).unwrap();
}

#[test]
fn dropped_idle_role_stays_charged_and_is_visible_to_failed_scope_teardown() {
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let runtime = ActorRuntime::new(1);
    let mut policy = config();
    policy.mailbox[2] = 1;
    runtime
        .register(aid(1), scope.clone(), slot(1), recorder(false), policy)
        .unwrap();
    let role = runtime
        .reserve_input(aid(1), aid(2), InputClass::Ordinary)
        .unwrap()
        .into_role()
        .unwrap();
    let id = role.id();
    drop(role);
    assert_eq!(runtime.snapshot(aid(1)).unwrap().roles, 1);
    assert_eq!(
        runtime
            .reserve_input(aid(1), aid(2), InputClass::Ordinary)
            .err(),
        Some(DriverError::InputCapacity)
    );
    let inventory = runtime.role_inventory(aid(1), None, 1).unwrap();
    assert_eq!(inventory.len(), 1);
    assert_eq!(inventory[0].role, id);
    assert_eq!(inventory[0].in_flight, None);
    assert!(
        runtime
            .role_inventory(aid(1), Some(id), 1)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        runtime.close_failed_role_after_exit(aid(1), id),
        Err(DriverError::ActorNotRetired)
    );
    scope.fail().unwrap();
    runtime.close_failed_role_after_exit(aid(1), id).unwrap();
    assert_eq!(runtime.snapshot(aid(1)).unwrap().roles, 0);
    remove_actor(&runtime, aid(1)).unwrap();
}

#[test]
fn failed_role_still_waits_for_its_started_failure_effect_and_input_retirement() {
    let ledger = ledger();
    let scope = ledger.open_scope(sid(1), None, generation(), 8, 8).unwrap();
    let runtime = ActorRuntime::new(1);
    let mut actor = recorder(false);
    actor.panic_on = Some(7);
    runtime
        .register(aid(1), scope.clone(), slot(1), actor, config())
        .unwrap();
    let role = runtime
        .reserve_input(aid(1), aid(2), InputClass::Lifecycle)
        .unwrap()
        .into_role()
        .unwrap();
    let id = role.id();
    let input = role.fill(7).unwrap();
    runtime.poll_one(Tick(1)).unwrap();
    assert_eq!(role.fill(8).unwrap_err(), (DriverError::ActorFailed, 8));
    assert_eq!(
        runtime.close_failed_role_after_exit(aid(1), id),
        Err(DriverError::RoleBusy)
    );
    let cleanup = runtime.take_effect(aid(1), input, action(1)).unwrap();
    runtime.effect_completed(cleanup).unwrap();
    runtime.retire_input(aid(1), input).unwrap();
    runtime.close_failed_role_after_exit(aid(1), id).unwrap();
    drop(role);
    drop(runtime.take_failure_payload(aid(1)).unwrap().unwrap());
    remove_actor(&runtime, aid(1)).unwrap();
}
