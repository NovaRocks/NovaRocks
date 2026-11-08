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

use super::*;
use crate::{
    actor::{ActorConfig, ActorStep},
    obligation::{ReservationObligation, WorkObligation},
    publication::PublishedSnapshot,
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    effect::{EffectDescriptor, EffectKind},
    identity::{Generation, ScopeId, ScopedWorkRef, Tick},
    mailbox::{InputClass, InputEnvelope},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use std::sync::Arc;

#[derive(Clone, Debug)]
struct TestEffect {
    lane: u8,
    release: EffectRelease,
    value: u64,
    dependencies: Vec<ActionId>,
}
#[derive(Debug)]
enum TestInput {
    Effects(Vec<TestEffect>),
    Fail,
}
#[derive(Debug)]
struct TestActor(ScopedWorkRef);
impl Actor for TestActor {
    type Input = TestInput;
    type Effect = TestEffect;
    fn timer_input(_: Tick) -> Self::Input {
        unreachable!("no timer")
    }
    fn handle(
        &mut self,
        _: Tick,
        _: InputEnvelope,
        input: Self::Input,
        step: &mut ActorStep<'_, TestEffect>,
    ) {
        let TestInput::Effects(effects) = input else {
            panic!("the original test actor fails for controlled cleanup");
        };
        for (ordinal, effect) in effects.into_iter().enumerate() {
            step.effect(
                EffectDescriptor {
                    id: ActionId::new(ordinal as u64 + 1).unwrap(),
                    work: self.0,
                    kind: if effect.release == EffectRelease::Published {
                        EffectKind::Publication
                    } else {
                        EffectKind::Business
                    },
                    dependencies: effect.dependencies.clone(),
                },
                effect,
            )
            .unwrap();
        }
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(
        _: ScopeId,
        _: Generation,
        _: InputEnvelope,
        _: &mut ActorStep<'_, TestEffect>,
    ) {
    }
}
struct Policy;
impl EffectLanePolicy<TestActor> for Policy {
    type LaneKey = u8;
    fn classify(&self, effect: &TestEffect) -> Option<(u8, EffectRelease)> {
        Some((effect.lane, effect.release))
    }
    fn effect_inventory_limit(&self) -> usize {
        2
    }
}
fn actor() -> ActorId {
    ActorId::new(1).unwrap()
}
fn effect(lane: u8, release: EffectRelease, value: u64) -> TestEffect {
    TestEffect {
        lane,
        release,
        value,
        dependencies: vec![],
    }
}
struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    runtime: ActorRuntime<TestActor>,
    registration: ActorRegistration,
    work: WorkObligation,
    capacity: ReservationObligation,
    inputs: Vec<InputId>,
}
impl Fixture {
    fn new() -> Self {
        let ledger = RuntimeLedger::new(ResourceVector::new([]).unwrap());
        let scope = ledger
            .open_scope_next(None, Generation::new(1).unwrap())
            .unwrap();
        let (mut work, capacity) = scope
            .reserve_next(
                Generation::new(1).unwrap(),
                ReservationResources::new(
                    ResourceVector::new([]).unwrap(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        work.start().unwrap();
        let runtime = ActorRuntime::new();
        let registration = runtime
            .register_bound(
                actor(),
                scope.clone(),
                TestActor(work.reference()),
                ActorConfig {
                    mailbox: [8; 4],
                    max_batches: 8,
                    max_effects_per_step: 2,
                    max_dependencies_per_effect: 1,
                    turn_budget: 32,
                    max_step_work: 2,
                },
            )
            .unwrap();
        Self {
            ledger,
            scope,
            runtime,
            registration,
            work,
            capacity,
            inputs: vec![],
        }
    }
    fn send(&mut self, effects: Vec<TestEffect>) -> InputId {
        let input = self
            .runtime
            .reserve_input(actor(), actor(), InputClass::Ordinary)
            .unwrap()
            .fill(TestInput::Effects(effects))
            .unwrap();
        self.runtime.poll_one(Tick(1)).unwrap();
        self.inputs.push(input);
        input
    }
    fn fail_owner(&mut self) {
        let input = self
            .runtime
            .reserve_input(actor(), actor(), InputClass::Control)
            .unwrap()
            .fill(TestInput::Fail)
            .unwrap();
        self.inputs.push(input);
        self.runtime.poll_one(Tick(2)).unwrap();
        for original in self.runtime.input_inventory(actor(), None, 16).unwrap() {
            if !self.inputs.contains(&original.input) {
                self.inputs.push(original.input);
            }
            let effects = match self
                .runtime
                .effect_inventory(actor(), original.input, None, 2)
            {
                Ok(effects) => effects,
                Err(ActorTurnError::UnknownInput)
                    if original.phase == crate::actor_turn::InputPhase::AbandonedByOwnerFailure =>
                {
                    // The panicking step was never committed. Its original
                    // input still retires, but it owns no effect batch.
                    Vec::new()
                }
                Err(error) => panic!("original failure cleanup inventory: {error:?}"),
            };
            for (descriptor, state) in effects {
                if state == EffectState::Pending && !descriptor.kind.allowed_after_failure() {
                    self.runtime
                        .abandon_unstarted_effect(actor(), original.input, descriptor.id)
                        .unwrap();
                }
            }
        }
        drop(self.runtime.take_failure_payload(actor()).unwrap());
    }
    fn finish(mut self) {
        for input in self.inputs {
            self.runtime.retire_input(actor(), input).unwrap();
        }
        self.scope.close_external().unwrap();
        self.work.body_exited().unwrap();
        self.work.captures_exited().unwrap();
        self.ledger.reap_scope_ready(self.scope.id(), 1).unwrap();
        while let Some(turn) = self.runtime.claim_host_actor() {
            turn.release(false).unwrap();
        }
        self.runtime.remove_actor(actor()).unwrap();
        self.capacity.finish().unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
    }
}
#[test]
fn publication_releases_fifo_and_dependencies_before_actual_ticket_exit() {
    let mut f = Fixture::new();
    let mut dependent = effect(2, EffectRelease::Started, 20);
    dependent.dependencies.push(ActionId::new(1).unwrap());
    let first_input = f.send(vec![effect(1, EffectRelease::Published, 10), dependent]);
    let second_input = f.send(vec![effect(1, EffectRelease::Started, 30)]);
    let mut dispatch =
        EffectDispatch::with_completion_runtime(f.runtime.clone(), actor(), 3, Policy);
    let indexed = dispatch.index_committed(&f.runtime, 8).unwrap();
    let first = dispatch.take_ready(&f.runtime, 8).unwrap().unwrap();
    let premature = dispatch.publication_committed(&f.runtime, &first).is_err();
    let blocked = dispatch.take_ready(&f.runtime, 8).unwrap().is_none();
    let snapshot = PublishedSnapshot::new(&f.scope, Arc::new(0_u64));
    drop(
        f.runtime
            .commit_snapshot(&first, &snapshot, Arc::new(first.effect.value))
            .unwrap(),
    );
    dispatch.publication_committed(&f.runtime, &first).unwrap();
    let mut values = vec![];
    while let Some(ticket) = dispatch.take_ready(&f.runtime, 8).unwrap() {
        values.push(ticket.effect.value);
        f.runtime.effect_completed(ticket).unwrap();
    }
    let empty_before_original_exit = dispatch.is_empty();
    let original_still_published = f
        .runtime
        .effect_inventory(actor(), first.input, None, 2)
        .unwrap()
        .iter()
        .any(|(descriptor, state)| {
            descriptor.id == first.descriptor.id && *state == EffectState::Published
        });
    f.runtime.effect_completed(first).unwrap();
    drop(snapshot);
    f.finish();
    assert_eq!(indexed.inputs, vec![first_input, second_input]);
    assert!(premature && blocked && empty_before_original_exit && original_still_published);
    assert_eq!(values, vec![20, 30]);
}
#[test]
fn completion_requires_the_exact_completed_lane_head() {
    let mut f = Fixture::new();
    f.send(vec![effect(3, EffectRelease::Completed, 10)]);
    f.send(vec![effect(3, EffectRelease::Completed, 20)]);
    let mut dispatch =
        EffectDispatch::with_completion_runtime(f.runtime.clone(), actor(), 2, Policy);
    dispatch.index_committed(&f.runtime, 8).unwrap();
    let first = dispatch.take_ready(&f.runtime, 8).unwrap().unwrap();
    let reference = EffectRef {
        input: first.input,
        action: first.descriptor.id,
    };
    let requires = dispatch.requires_completion(&first).unwrap();
    let premature = dispatch.completion_observed(3, reference).is_err();
    let wrong_lane = dispatch.completion_observed(4, reference).is_err();
    let blocked = dispatch.take_ready(&f.runtime, 8).unwrap().is_none();
    f.runtime.effect_completed(first).unwrap();
    dispatch.completion_observed(3, reference).unwrap();
    let stale = dispatch.completion_observed(3, reference).is_err();
    let second = dispatch.take_ready(&f.runtime, 8).unwrap().unwrap();
    let second_value = second.effect.value;
    let reference = EffectRef {
        input: second.input,
        action: second.descriptor.id,
    };
    f.runtime.effect_completed(second).unwrap();
    dispatch.completion_observed(3, reference).unwrap();
    let empty = dispatch.is_empty();
    f.finish();
    assert!(requires && premature && wrong_lane && blocked && stale && empty);
    assert_eq!(second_value, 20);
}
#[test]
fn indexing_preserves_atomic_batch_budget_and_prepaid_inventory() {
    let mut f = Fixture::new();
    let input = f.send(vec![
        effect(1, EffectRelease::Started, 10),
        effect(2, EffectRelease::Started, 20),
    ]);
    let mut small = EffectDispatch::new(actor(), 1, Policy);
    let budget = small.index_committed(&f.runtime, 2).unwrap();
    let capacity = small.index_committed(&f.runtime, 3);
    let small_empty = small.is_empty();
    let mut dispatch = EffectDispatch::new(actor(), 2, Policy);
    let indexed = dispatch.index_committed(&f.runtime, 3).unwrap();
    let mut values = vec![];
    while let Some(ticket) = dispatch.take_ready(&f.runtime, 8).unwrap() {
        values.push(ticket.effect.value);
        f.runtime.effect_completed(ticket).unwrap();
    }
    f.finish();
    assert!(budget.inputs.is_empty() && budget.units == 0 && small_empty);
    assert!(matches!(capacity, Err(DispatchError::Capacity)));
    assert_eq!(indexed.inputs, vec![input]);
    assert_eq!(indexed.units, 3);
    assert_eq!(values, vec![10, 20]);
}

#[test]
fn pending_observation_includes_same_lane_tails_and_unindexed_batches_without_advancing() {
    let mut f = Fixture::new();
    f.send(vec![
        effect(1, EffectRelease::Completed, 10),
        effect(1, EffectRelease::Completed, 11),
    ]);
    f.send(vec![effect(2, EffectRelease::Completed, 20)]);
    let mut dispatch =
        EffectDispatch::with_completion_runtime(f.runtime.clone(), actor(), 3, Policy);
    let before = dispatch
        .try_observation(&f.runtime, &f.registration)
        .unwrap()
        .unwrap();
    assert_eq!(before.runtime.states.pending, 3);
    assert_eq!(before.runtime.unindexed_states.pending, 3);
    assert_eq!(before.runtime.unindexed_batches, 2);
    assert_eq!(before.indexed_routing_positions, 0);
    assert_eq!(before.ready_markers, 0);
    assert_eq!(
        dispatch
            .index_committed(&f.runtime, 3)
            .unwrap()
            .inputs
            .len(),
        1
    );
    let indexed = dispatch
        .try_observation(&f.runtime, &f.registration)
        .unwrap()
        .unwrap();
    assert_eq!(indexed.runtime.states.pending, 3);
    assert_eq!(indexed.runtime.unindexed_states.pending, 1);
    assert_eq!(indexed.runtime.unindexed_batches, 1);
    assert_eq!(indexed.indexed_routing_positions, 2);
    assert_eq!(
        indexed.ready_markers, 1,
        "one lane marker still has two pending records"
    );
    let first = dispatch.take_ready(&f.runtime, 3).unwrap().unwrap();
    let started = dispatch
        .try_observation(&f.runtime, &f.registration)
        .unwrap()
        .unwrap();
    assert_eq!(started.runtime.states.pending, 2);
    assert_eq!(started.runtime.states.started, 1);
    assert_eq!(
        started.ready_markers, 0,
        "an in-flight head cannot hide the tail from runtime pending inventory"
    );
    let completion = f.runtime.effect_completed_with_receipt(first).unwrap();
    dispatch
        .completion_observed_with_receipt(1, &completion)
        .unwrap();
    dispatch.index_committed(&f.runtime, 3).unwrap();
    while let Some(ticket) = dispatch.take_ready(&f.runtime, 3).unwrap() {
        let lane = ticket.effect.lane;
        let completion = f.runtime.effect_completed_with_receipt(ticket).unwrap();
        dispatch
            .completion_observed_with_receipt(lane, &completion)
            .unwrap();
    }
    let complete = dispatch
        .try_observation(&f.runtime, &f.registration)
        .unwrap()
        .unwrap();
    assert_eq!(complete.runtime.states.pending, 0);
    assert_eq!(complete.runtime.states.completed, 3);
    assert_eq!(complete.runtime.unindexed_batches, 0);
    assert_eq!(complete.indexed_routing_positions, 0);
    f.finish();
}
#[test]
fn pending_observation_rejects_another_runtime_registration_instead_of_zero() {
    let f = Fixture::new();
    let other = Fixture::new();
    let dispatch = EffectDispatch::with_completion_runtime(f.runtime.clone(), actor(), 1, Policy);
    assert!(matches!(
        dispatch.try_observation(&f.runtime, &other.registration),
        Err(crate::observation::EffectObservationError::Runtime(
            ActorTurnError::EffectTicketOriginMismatch
        ))
    ));
    other.finish();
    f.finish();
}

#[test]
fn candidate_peek_is_neutral_and_exact_claim_rejects_foreign_and_stale_heads() {
    let mut f = Fixture::new();
    let input = f.send(vec![
        effect(1, EffectRelease::Started, 10),
        effect(2, EffectRelease::Started, 20),
    ]);
    let mut dispatch =
        EffectDispatch::with_completion_runtime(f.runtime.clone(), actor(), 2, Policy);
    dispatch.index_committed(&f.runtime, 8).unwrap();
    let host = f.runtime.claim_host_actor().unwrap();
    let candidate = dispatch.peek_candidate(&f.runtime, &host).unwrap().unwrap();
    let reference = candidate.effect_ref();
    let untouched = f
        .runtime
        .effect_inventory(actor(), input, None, 2)
        .unwrap()
        .iter()
        .all(|(_, state)| *state == EffectState::Pending);
    let mut foreign =
        EffectDispatch::with_completion_runtime(f.runtime.clone(), actor(), 2, Policy);
    foreign.index_committed(&f.runtime, 8).unwrap();
    let foreign_rejected = matches!(
        foreign.claim_candidate(&f.runtime, &candidate),
        Err(DispatchError::InvalidCandidate)
    );
    let foreign_defer_rejected = matches!(
        foreign.defer_candidate(&candidate),
        Err(DispatchError::InvalidCandidate)
    );
    let other = Fixture::new();
    let wrong_runtime_rejected = matches!(
        dispatch.claim_candidate(&other.runtime, &candidate),
        Err(DispatchError::InvalidCandidate)
    );
    let first = dispatch
        .claim_candidate(&f.runtime, &candidate)
        .unwrap()
        .unwrap();
    let stale_rejected = matches!(
        dispatch.claim_candidate(&f.runtime, &candidate),
        Err(DispatchError::InvalidCandidate)
    );
    let second_pending =
        f.runtime.effect_inventory(actor(), input, None, 2).unwrap()[1].1 == EffectState::Pending;
    drop(candidate);
    let next = dispatch.peek_candidate(&f.runtime, &host).unwrap().unwrap();
    let second = dispatch
        .claim_candidate(&f.runtime, &next)
        .unwrap()
        .unwrap();
    let values = (first.effect.value, second.effect.value);
    drop(next);
    f.runtime.effect_completed(first).unwrap();
    f.runtime.effect_completed(second).unwrap();
    host.release(false).unwrap();
    other.finish();
    f.finish();
    assert!(
        untouched
            && foreign_rejected
            && foreign_defer_rejected
            && wrong_runtime_rejected
            && stale_rejected
            && second_pending
    );
    assert_eq!(
        reference,
        EffectRef {
            input,
            action: ActionId::new(1).unwrap()
        }
    );
    assert_eq!(values, (10, 20));
}

#[test]
fn candidate_dependency_gate_does_not_borrow_a_loan_or_claim_another_head() {
    let mut f = Fixture::new();
    let mut dependent = effect(1, EffectRelease::Started, 10);
    dependent.dependencies.push(ActionId::new(2).unwrap());
    let input = f.send(vec![dependent, effect(2, EffectRelease::Started, 20)]);
    let mut dispatch =
        EffectDispatch::with_completion_runtime(f.runtime.clone(), actor(), 2, Policy);
    dispatch.index_committed(&f.runtime, 8).unwrap();
    let host = f.runtime.claim_host_actor().unwrap();
    let blocked = dispatch.peek_candidate(&f.runtime, &host).unwrap().unwrap();
    let no_metadata = blocked.metadata().is_none();
    let no_ticket = dispatch
        .claim_candidate(&f.runtime, &blocked)
        .unwrap()
        .is_none();
    let other_pending =
        f.runtime.effect_inventory(actor(), input, None, 2).unwrap()[1].1 == EffectState::Pending;
    dispatch.defer_candidate(&blocked).unwrap();
    drop(blocked);
    let ready = dispatch.peek_candidate(&f.runtime, &host).unwrap().unwrap();
    let predecessor = dispatch
        .claim_candidate(&f.runtime, &ready)
        .unwrap()
        .unwrap();
    drop(ready);
    let completion = f
        .runtime
        .effect_completed_with_receipt(predecessor)
        .unwrap();
    f.runtime.acknowledge_completed_effect(&completion).unwrap();
    let ready = dispatch.peek_candidate(&f.runtime, &host).unwrap().unwrap();
    let metadata_ready = ready
        .metadata()
        .is_some_and(|metadata| metadata.value == 10);
    let ticket = dispatch
        .claim_candidate(&f.runtime, &ready)
        .unwrap()
        .unwrap();
    drop(ready);
    f.runtime.effect_completed(ticket).unwrap();
    host.release(false).unwrap();
    f.finish();
    assert!(no_metadata && no_ticket && other_pending && metadata_ready);
}

#[test]
fn deferred_full_head_keeps_its_lane_tail_behind_independent_continuation() {
    let mut f = Fixture::new();
    f.send(vec![
        effect(1, EffectRelease::Completed, 10),
        effect(2, EffectRelease::Started, 20),
    ]);
    f.send(vec![effect(1, EffectRelease::Started, 30)]);
    let mut dispatch =
        EffectDispatch::with_completion_runtime(f.runtime.clone(), actor(), 3, Policy);
    dispatch.index_committed(&f.runtime, 8).unwrap();
    let host = f.runtime.claim_host_actor().unwrap();
    for _ in 0..2048 {
        let head = dispatch.peek_candidate(&f.runtime, &host).unwrap().unwrap();
        dispatch.defer_candidate(&head).unwrap();
        drop(head);
        let other = dispatch.peek_candidate(&f.runtime, &host).unwrap().unwrap();
        dispatch.defer_candidate(&other).unwrap();
        drop(other);
    }
    let before = (dispatch.ready_positions(), dispatch.indexed);
    let full = dispatch.peek_candidate(&f.runtime, &host).unwrap().unwrap();
    let full_value = full.metadata().unwrap().value;
    dispatch.defer_candidate(&full).unwrap();
    drop(full);
    let available = dispatch.peek_candidate(&f.runtime, &host).unwrap().unwrap();
    let continuation = dispatch
        .claim_candidate(&f.runtime, &available)
        .unwrap()
        .unwrap();
    let continuation_value = continuation.effect.value;
    drop(available);
    f.runtime.effect_completed(continuation).unwrap();
    let full = dispatch.peek_candidate(&f.runtime, &host).unwrap().unwrap();
    let first = dispatch
        .claim_candidate(&f.runtime, &full)
        .unwrap()
        .unwrap();
    drop(full);
    let hidden_tail = dispatch
        .peek_candidate(&f.runtime, &host)
        .unwrap()
        .is_none();
    let completion = f.runtime.effect_completed_with_receipt(first).unwrap();
    dispatch
        .completion_observed_with_receipt(1, &completion)
        .unwrap();
    let tail = dispatch.peek_candidate(&f.runtime, &host).unwrap().unwrap();
    let last = dispatch
        .claim_candidate(&f.runtime, &tail)
        .unwrap()
        .unwrap();
    let tail_value = last.effect.value;
    drop(tail);
    f.runtime.effect_completed(last).unwrap();
    host.release(false).unwrap();
    f.finish();
    assert_eq!(before, (2, 3));
    assert_eq!((full_value, continuation_value, tail_value), (10, 20, 30));
    assert!(hidden_tail);
}

#[test]
fn stopped_business_candidates_need_no_source_borrow_return_or_self_wake() {
    use crate::causal_input::CausalInputObligation;
    let mut f = Fixture::new();
    f.send(vec![
        effect(1, EffectRelease::Started, 10),
        effect(2, EffectRelease::Started, 20),
    ]);
    let mut source_loan = Some(
        CausalInputObligation::reserve_admitted(
            &f.runtime,
            &f.registration,
            ActorId::new(3).unwrap(),
            InputClass::Lifecycle,
            &f.work,
        )
        .unwrap(),
    );
    let mut dispatch =
        EffectDispatch::with_completion_runtime(f.runtime.clone(), actor(), 2, Policy);
    dispatch.index_committed(&f.runtime, 8).unwrap();
    let host = f.runtime.claim_host_actor().unwrap();
    f.scope.request_stop().unwrap();
    let mut remaining = dispatch.ready_positions();
    let mut visits = 0;
    let mut borrows = 0;
    let mut returns = 0;
    // A one-candidate turn budget retains the original pass boundary. Returning
    // a Source loan would actually notify its host, so blocked qualification
    // must occur BEFORE a borrow rather than relying on a finite scan alone.
    while remaining != 0 {
        let candidate = dispatch.peek_candidate(&f.runtime, &host).unwrap().unwrap();
        if candidate.metadata().is_some() {
            let loan = source_loan.take().unwrap();
            borrows += 1;
            source_loan = Some(loan);
            returns += 1;
            f.runtime.notify_host_actor(&f.registration).unwrap();
        }
        dispatch.defer_candidate(&candidate).unwrap();
        visits += 1;
        remaining -= 1;
    }
    source_loan.as_mut().unwrap().close_after_exit().unwrap();
    drop(source_loan);
    host.release(false).unwrap();
    let no_self_wake = f.runtime.claim_host_actor().is_none();
    f.fail_owner();
    f.finish();
    assert_eq!((visits, borrows, returns), (2, 0, 0));
    assert!(no_self_wake);
}

#[test]
fn exact_readiness_rechecks_stop_and_scope_failure_before_ticket_claim() {
    let mut f = Fixture::new();
    let input = f.send(vec![effect(1, EffectRelease::Started, 10)]);
    let mut dispatch =
        EffectDispatch::with_completion_runtime(f.runtime.clone(), actor(), 1, Policy);
    dispatch.index_committed(&f.runtime, 8).unwrap();
    let host = f.runtime.claim_host_actor().unwrap();
    let candidate = dispatch.peek_candidate(&f.runtime, &host).unwrap().unwrap();
    let initially_ready = candidate.metadata().is_some();
    f.scope.request_stop().unwrap();
    let stopped = dispatch
        .claim_candidate(&f.runtime, &candidate)
        .unwrap()
        .is_none();
    drop(candidate);
    let blocked = dispatch.peek_candidate(&f.runtime, &host).unwrap().unwrap();
    let no_new_borrow = blocked.metadata().is_none();
    f.scope.fail().unwrap();
    let failed = matches!(
        f.runtime
            .effect_ready_for(&host, input, ActionId::new(1).unwrap()),
        Err(ActorTurnError::InputAuthorInactive)
    );
    let original_failure = matches!(
        f.runtime
            .take_effect(actor(), input, ActionId::new(1).unwrap()),
        Err(ActorTurnError::InputAuthorInactive)
    );
    let failed_claim = matches!(
        dispatch.claim_candidate(&f.runtime, &blocked),
        Err(DispatchError::Runtime(ActorTurnError::InputAuthorInactive))
    );
    drop(blocked);
    host.release(false).unwrap();
    f.fail_owner();
    f.finish();
    assert!(
        initially_ready && stopped && no_new_borrow && failed && original_failure && failed_claim
    );
}
