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
    driver::{ActorRuntime, DriverError, EffectTicket, InputPhase, InputRole},
    lease::{CapacityBundle, WorkLease},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    effect::{EffectDescriptor, EffectError, EffectKind},
    identity::{ActionId, ActorId, DimensionId, Generation, InputId, ScopeId, Tick, WorkId},
    mailbox::{InputClass, InputEnvelope},
    scope::{WorkAdmission, WorkKind},
};
use std::sync::{Arc, Barrier};

#[derive(Debug)]
enum Input {
    Emit,
    Value(Arc<u64>),
}
#[derive(Debug)]
struct Neutral {
    work: WorkId,
    received: Vec<u64>,
}
impl Actor for Neutral {
    type Input = Input;
    type Effect = ();
    fn timer_input(_: Tick) -> Input {
        unreachable!("no timer in this fixture")
    }
    fn handle(&mut self, _: Tick, _: InputEnvelope, input: Input, step: &mut StepContext<'_, ()>) {
        match input {
            Input::Emit => step
                .effect(
                    EffectDescriptor {
                        id: ActionId::new(1).unwrap(),
                        work: self.work,
                        kind: EffectKind::Publication,
                        dependencies: vec![],
                    },
                    (),
                )
                .unwrap(),
            Input::Value(value) => self.received.push(*value),
        }
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut StepContext<'_, ()>) {}
}
fn id(value: u64) -> ActorId {
    ActorId::new(value).unwrap()
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
struct Owner {
    actor: ActorId,
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    work: WorkLease,
    capacity: CapacityBundle,
}
impl Owner {
    fn register(runtime: &ActorRuntime<Neutral>, actor: ActorId, ledger: RuntimeLedger) -> Self {
        let scope = ledger.open_scope_next(None, generation(), 4, 4).unwrap();
        let (mut work, capacity) = scope
            .reserve_next(
                generation(),
                paid(1),
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        work.start().unwrap();
        runtime
            .register_next(
                actor,
                scope.clone(),
                Neutral {
                    work: work.id(),
                    received: vec![],
                },
                ActorConfig {
                    mailbox: [4; 4],
                    max_batches: 4,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
            )
            .unwrap();
        Self {
            actor,
            ledger,
            scope,
            work,
            capacity,
        }
    }
}
struct Fixture {
    runtime: ActorRuntime<Neutral>,
    owners: Vec<Owner>,
    source: ActorId,
    target: ActorId,
    source_input: InputId,
    role: Option<InputRole<Neutral>>,
}
impl Fixture {
    fn new(
        source: u64,
        target: u64,
        separate_ledger: bool,
        role_source: Option<u64>,
    ) -> (Self, EffectTicket<()>) {
        let runtime = ActorRuntime::new(2);
        let ledger = RuntimeLedger::new(paid(2), 8, 2);
        let source = id(source);
        let target = id(target);
        let mut owners = vec![Owner::register(&runtime, source, ledger.clone())];
        if target != source {
            let target_ledger = if separate_ledger {
                RuntimeLedger::new(paid(2), 8, 2)
            } else {
                ledger
            };
            owners.push(Owner::register(&runtime, target, target_ledger));
        }
        let role = runtime
            .reserve_input(
                target,
                role_source.map(id).unwrap_or(source),
                InputClass::Control,
            )
            .unwrap()
            .into_role()
            .unwrap();
        let source_input = runtime
            .reserve_input(source, source, InputClass::Ordinary)
            .unwrap()
            .fill(Input::Emit)
            .unwrap();
        runtime.poll_one(Tick(1)).unwrap();
        let ticket = runtime
            .take_effect(source, source_input, ActionId::new(1).unwrap())
            .unwrap();
        (
            Self {
                runtime,
                owners,
                source,
                target,
                source_input,
                role: Some(role),
            },
            ticket,
        )
    }
    fn role(&self) -> &InputRole<Neutral> {
        self.role.as_ref().unwrap()
    }
    fn target_scope(&self) -> &ScopeHandle {
        &self
            .owners
            .iter()
            .find(|owner| owner.actor == self.target)
            .unwrap()
            .scope
    }
    fn finish(mut self, ticket: EffectTicket<()>) -> Vec<Vec<u64>> {
        self.runtime.effect_completed(ticket).unwrap();
        self.runtime
            .retire_input(self.source, self.source_input)
            .unwrap();
        if let Some(mut role) = self.role.take() {
            if let Some(input) = role.in_flight().unwrap() {
                self.runtime.poll_one(Tick(2)).unwrap();
                self.runtime.retire_input(self.target, input).unwrap();
            }
            role.close_after_exit().unwrap();
        }
        for owner in &mut self.owners {
            self.runtime.close_actor_admission(owner.actor).unwrap();
            owner.scope.close_external().unwrap();
            owner.work.body_exited().unwrap();
            owner.work.captures_exited().unwrap();
            owner.ledger.reap_scope_ready(owner.scope.id(), 8).unwrap();
        }
        while let Some(original_host_turn) = self.runtime.claim_host_actor() {
            original_host_turn.release(false).unwrap();
        }
        let mut received = vec![];
        for owner in &mut self.owners {
            self.runtime.seal_actor(owner.actor).unwrap();
            received.push(self.runtime.remove_actor(owner.actor).unwrap().received);
            owner
                .capacity
                .release(&owner.capacity.resources().unwrap())
                .unwrap();
            owner.ledger.remove_scope(owner.scope.id()).unwrap();
        }
        assert!(self.runtime.is_empty());
        for owner in &self.owners {
            assert!(owner.ledger.is_empty());
        }
        received
    }
}
fn returned_same(
    result: Result<InputId, (DriverError, Input)>,
    expected: &Arc<u64>,
) -> DriverError {
    let (error, returned) = result.unwrap_err();
    match returned {
        Input::Value(actual) => assert!(Arc::ptr_eq(&actual, expected)),
        Input::Emit => panic!("the original neutral value must be returned"),
    }
    error
}

#[test]
fn forwarding_uses_paid_roles_during_drain_in_both_lock_orders_and_the_same_actor() {
    for (source, target) in [(1, 2), (2, 1), (1, 1)] {
        let (f, ticket) = Fixture::new(source, target, false, None);
        f.runtime.close_admission();
        f.runtime.close_actor_admission(f.target).unwrap();
        f.target_scope().close_external().unwrap();
        f.target_scope().request_stop().unwrap();
        let before = f.owners[0].ledger.used(dimension());
        let value = Arc::new(41);
        let forwarded = f
            .runtime
            .fill_from_effect(&ticket, f.role(), Input::Value(value.clone()))
            .unwrap();
        let custody = f
            .runtime
            .input_custody(f.target, forwarded)
            .unwrap()
            .unwrap();
        let ticket_unchanged = f.runtime.validate_effect_ticket(&ticket);
        let busy = f
            .runtime
            .fill_from_effect(&ticket, f.role(), Input::Value(value.clone()));
        let busy_error = returned_same(busy, &value);
        let after = f.owners[0].ledger.used(dimension());
        let received = f.finish(ticket);
        assert_eq!(before, after);
        assert_eq!(ticket_unchanged, Ok(()));
        assert_eq!(custody.source, id(source));
        assert_eq!(custody.phase, InputPhase::Filled);
        assert_eq!(busy_error, DriverError::RoleBusy);
        assert_eq!(
            received.iter().flatten().copied().collect::<Vec<_>>(),
            vec![41]
        );
    }
}

#[test]
fn foreign_runtime_and_same_number_foreign_ticket_keep_the_original_value() {
    let (left, left_ticket) = Fixture::new(1, 2, false, None);
    let (right, right_ticket) = Fixture::new(1, 2, false, None);
    let value = Arc::new(42);
    let target_error = returned_same(
        left.runtime
            .fill_from_effect(&left_ticket, right.role(), Input::Value(value.clone())),
        &value,
    );
    let ticket_error = returned_same(
        right
            .runtime
            .fill_from_effect(&left_ticket, right.role(), Input::Value(value.clone())),
        &value,
    );
    let unused =
        left.role().in_flight().unwrap().is_none() && right.role().in_flight().unwrap().is_none();
    left.finish(left_ticket);
    right.finish(right_ticket);
    assert_eq!(target_error, DriverError::PublicationTargetMismatch);
    assert_eq!(ticket_error, DriverError::EffectTicketOriginMismatch);
    assert!(unused);
}

#[test]
fn one_runtime_cannot_forward_between_independent_ledgers_or_a_different_declared_source() {
    for (separate, source, expected) in [
        (true, None, DriverError::PublicationTargetMismatch),
        (false, Some(9), DriverError::InputAuthorMismatch),
    ] {
        let (f, ticket) = Fixture::new(1, 2, separate, source);
        let value = Arc::new(43);
        let error = returned_same(
            f.runtime
                .fill_from_effect(&ticket, f.role(), Input::Value(value.clone())),
            &value,
        );
        let unused = f.role().in_flight().unwrap().is_none();
        f.finish(ticket);
        assert_eq!(error, expected);
        assert!(unused);
    }
}

#[test]
fn prior_source_failure_rejects_the_original_generation_without_consuming_its_ticket() {
    let (f, ticket) = Fixture::new(1, 2, false, None);
    f.owners[0].scope.fail().unwrap();
    let value = Arc::new(44);
    let error = returned_same(
        f.runtime
            .fill_from_effect(&ticket, f.role(), Input::Value(value.clone())),
        &value,
    );
    let unused = f.role().in_flight().unwrap().is_none();
    f.finish(ticket);
    assert_eq!(error, DriverError::Effect(EffectError::StaleGeneration));
    assert!(unused);
}

#[test]
fn target_failure_and_a_closed_original_role_preserve_the_value() {
    for failed in [true, false] {
        let (mut f, ticket) = Fixture::new(1, 2, false, None);
        let mut closed_role = None;
        if failed {
            f.target_scope().fail().unwrap();
        } else {
            let mut role = f.role.take().unwrap();
            role.close_after_exit().unwrap();
            closed_role = Some(role);
        }
        let target = closed_role.as_ref().unwrap_or_else(|| f.role());
        let value = Arc::new(45);
        let error = returned_same(
            f.runtime
                .fill_from_effect(&ticket, target, Input::Value(value.clone())),
            &value,
        );
        f.finish(ticket);
        assert_eq!(
            error,
            if failed {
                DriverError::ActorFailed
            } else {
                DriverError::UnknownRole
            }
        );
    }
}

#[test]
fn forwarding_and_source_failure_share_one_actual_ledger_order() {
    let (f, ticket) = Fixture::new(1, 2, false, None);
    let value = Arc::new(46);
    let barrier = Barrier::new(2);
    let result = std::thread::scope(|threads| {
        let forward = threads.spawn(|| {
            barrier.wait();
            f.runtime
                .fill_from_effect(&ticket, f.role(), Input::Value(value.clone()))
        });
        let failure = threads.spawn(|| {
            barrier.wait();
            f.owners[0].scope.fail().unwrap()
        });
        let result = forward.join().unwrap();
        failure.join().unwrap();
        result
    });
    let accepted = match result {
        Ok(input) => {
            let custody = f.runtime.input_custody(f.target, input).unwrap().unwrap();
            assert_eq!(custody.phase, InputPhase::Filled);
            true
        }
        Err((error, Input::Value(original))) => {
            assert!(Arc::ptr_eq(&original, &value));
            assert_eq!(error, DriverError::Effect(EffectError::StaleGeneration));
            false
        }
        Err((_, Input::Emit)) => panic!("the original value cannot change shape"),
    };
    let receipt = f.role().in_flight().unwrap();
    let received = f.finish(ticket);
    assert_eq!(receipt.is_some(), accepted);
    assert_eq!(received.iter().flatten().count(), usize::from(accepted));
}

#[test]
fn opposite_actor_forwards_use_one_lock_order_and_both_threads_join() {
    let (f, first_ticket) = Fixture::new(1, 2, false, None);
    let mut reverse = f
        .runtime
        .reserve_input(f.source, f.target, InputClass::Control)
        .unwrap()
        .into_role()
        .unwrap();
    let other_input = f
        .runtime
        .reserve_input(f.target, f.target, InputClass::Ordinary)
        .unwrap()
        .fill(Input::Emit)
        .unwrap();
    f.runtime.poll_one(Tick(1)).unwrap();
    let other_ticket = f
        .runtime
        .take_effect(f.target, other_input, ActionId::new(1).unwrap())
        .unwrap();
    let barrier = Barrier::new(2);
    let (first, second) = std::thread::scope(|threads| {
        let first = threads.spawn(|| {
            barrier.wait();
            f.runtime
                .fill_from_effect(&first_ticket, f.role(), Input::Value(Arc::new(47)))
        });
        let second = threads.spawn(|| {
            barrier.wait();
            f.runtime
                .fill_from_effect(&other_ticket, &reverse, Input::Value(Arc::new(48)))
        });
        (first.join().unwrap(), second.join().unwrap())
    });
    let first = first.unwrap();
    let second = second.unwrap();
    f.runtime.poll_one(Tick(2)).unwrap();
    f.runtime.poll_one(Tick(2)).unwrap();
    f.runtime.retire_input(f.source, second).unwrap();
    reverse.close_after_exit().unwrap();
    f.runtime.effect_completed(other_ticket).unwrap();
    f.runtime.retire_input(f.target, other_input).unwrap();
    let actual = f.runtime.input_custody(f.target, first).unwrap().unwrap();
    let received = f.finish(first_ticket);
    assert_eq!(actual.phase, InputPhase::Committed);
    assert_eq!(received, vec![vec![48], vec![47]]);
}
