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
struct TestActor(ScopedWorkRef);
impl Actor for TestActor {
    type Input = Vec<TestEffect>;
    type Effect = TestEffect;
    fn timer_input(_: Tick) -> Self::Input {
        unreachable!("no timer")
    }
    fn handle(
        &mut self,
        _: Tick,
        _: InputEnvelope,
        effects: Self::Input,
        step: &mut ActorStep<'_, TestEffect>,
    ) {
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
        runtime
            .register_next(
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
            .fill(effects)
            .unwrap();
        self.runtime.poll_one(Tick(1)).unwrap();
        self.inputs.push(input);
        input
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
