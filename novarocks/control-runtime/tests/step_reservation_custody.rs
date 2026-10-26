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
    driver::{ActorRuntime, DriverError},
    lease::{CapacityBundle, WorkLease},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    effect::{EffectDescriptor, EffectKind},
    identity::*,
    mailbox::{InputClass, InputEnvelope},
    scope::{WorkAdmission, WorkKind},
};

fn vector(n: u64) -> ResourceVector {
    ResourceVector::new([(DimensionId::new(1).unwrap(), n)]).unwrap()
}
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn actor() -> ActorId {
    ActorId::new(1).unwrap()
}
fn action() -> ActionId {
    ActionId::new(1).unwrap()
}
struct Reserving {
    scope: ScopeHandle,
}
impl Actor for Reserving {
    type Input = u8;
    type Effect = Option<ReservationId>;
    fn timer_input(_: Tick) -> u8 {
        unreachable!()
    }
    fn handle(
        &mut self,
        _: Tick,
        _: InputEnvelope,
        command: u8,
        step: &mut StepContext<'_, Self::Effect>,
    ) {
        if command == 2 {
            panic!("controlled later owner failure");
        }
        let guard = self
            .scope
            .reserve_next(
                generation(),
                vector(1),
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap();
        let reservation = guard.reservation_id();
        let work = guard.work_id();
        step.reservation(guard)
            .unwrap_or_else(|(e, _)| panic!("handoff: {e:?}"));
        step.effect(
            EffectDescriptor {
                id: action(),
                work,
                kind: EffectKind::Business,
                dependencies: Vec::new(),
            },
            Some(reservation),
        )
        .unwrap();
        if command == 1 {
            panic!("controlled failure before atomic batch commitment");
        }
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(
        _: ScopeId,
        _: Generation,
        _: InputEnvelope,
        step: &mut StepContext<'_, Self::Effect>,
    ) {
        step.effect(
            EffectDescriptor {
                id: action(),
                work: step.failure_work().unwrap(),
                kind: EffectKind::Cleanup,
                dependencies: Vec::new(),
            },
            None,
        )
        .unwrap();
    }
}
struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    runtime: ActorRuntime<Reserving>,
    failure: WorkLease,
    capacity: CapacityBundle,
}
impl Fixture {
    fn new() -> Self {
        let ledger = RuntimeLedger::new(vector(1), 8, 1);
        let scope = ledger.open_scope_next(None, generation(), 8, 4).unwrap();
        let (failure, capacity) = scope
            .reserve_next(
                generation(),
                vector(0),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        let runtime = ActorRuntime::new(1);
        runtime
            .register_supervised_next(
                actor(),
                scope.clone(),
                Reserving {
                    scope: scope.clone(),
                },
                ActorConfig {
                    mailbox: [4; 4],
                    max_batches: 4,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
                &failure,
            )
            .unwrap_or_else(|(e, _)| panic!("registration: {e:?}"));
        Self {
            ledger,
            scope,
            runtime,
            failure,
            capacity,
        }
    }
    fn command(&self, n: u8) -> InputId {
        let input = self
            .runtime
            .reserve_input(actor(), ActorId::new(2).unwrap(), InputClass::Ordinary)
            .unwrap()
            .fill(n)
            .unwrap();
        self.runtime.poll_one(Tick(1)).unwrap();
        input
    }
    fn finish(mut self) {
        if self.runtime.snapshot(actor()).unwrap().failed {
            drop(self.runtime.take_failure_payload(actor()).unwrap());
        }
        while let Some(id) = self.runtime.claim_effect_actor() {
            self.runtime.release_effect_actor(id, false).unwrap();
        }
        self.failure.body_exited().unwrap();
        self.failure.captures_exited().unwrap();
        self.ledger.reap_scope_ready(self.scope.id(), 8).unwrap();
        self.capacity
            .release(&self.capacity.resources().unwrap())
            .unwrap();
        self.scope.close_external().unwrap();
        self.runtime
            .remove_actor(actor())
            .unwrap_or_else(|e| panic!("remove: {e:?}"));
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
    }
    fn complete_failure(&mut self, input: InputId) {
        let ticket = self.runtime.take_effect(actor(), input, action()).unwrap();
        assert_eq!(ticket.descriptor.work, self.failure.id());
        assert!(ticket.effect.is_none());
        self.failure.start().unwrap();
        self.runtime.effect_completed(ticket).unwrap();
        self.runtime.retire_input(actor(), input).unwrap();
    }
}
#[test]
fn a_committed_guard_cannot_disappear_when_its_input_effect_completes() {
    let f = Fixture::new();
    let input = f.command(0);
    let ticket = f.runtime.take_effect(actor(), input, action()).unwrap();
    let reservation = ticket.effect.unwrap();
    let guard = f
        .runtime
        .take_step_reservation(&ticket, reservation)
        .unwrap();
    assert_eq!(guard.work_id(), ticket.descriptor.work);
    assert!(matches!(
        f.runtime.take_step_reservation(&ticket, reservation),
        Err(DriverError::UnknownReservation)
    ));
    let (mut work, mut capacity) = guard.bind(WorkKind::Business).unwrap();
    work.start().unwrap();
    f.runtime.effect_completed(ticket).unwrap();
    f.runtime.retire_input(actor(), input).unwrap();
    assert_eq!(f.ledger.used(DimensionId::new(1).unwrap()), 1);
    work.body_exited().unwrap();
    assert_eq!(f.ledger.used(DimensionId::new(1).unwrap()), 1);
    work.captures_exited().unwrap();
    f.ledger.reap_scope_ready(f.scope.id(), 8).unwrap();
    capacity.release(&capacity.resources().unwrap()).unwrap();
    f.finish();
}
#[test]
fn an_uncommitted_step_unwinds_its_original_unbound_guards() {
    let mut f = Fixture::new();
    let input = f.command(1);
    assert!(f.runtime.snapshot(actor()).unwrap().failed);
    assert_eq!(f.ledger.used(DimensionId::new(1).unwrap()), 0);
    assert!(
        f.runtime
            .step_reservations(actor(), input, None, 8)
            .unwrap()
            .is_empty()
    );
    f.complete_failure(input);
    f.finish();
}
#[test]
fn failure_teardown_uses_the_committed_guard_without_reading_the_failed_core() {
    let mut f = Fixture::new();
    let earlier = f.command(0);
    let reservation = f
        .runtime
        .step_reservations(actor(), earlier, None, 1)
        .unwrap()[0]
        .0;
    assert_eq!(
        f.runtime.retire_input(actor(), earlier),
        Err(DriverError::EffectsRemain)
    );
    let failed = f.command(2);
    f.runtime
        .abandon_unstarted_effect(actor(), earlier, action())
        .unwrap();
    assert_eq!(
        f.runtime.retire_input(actor(), earlier),
        Err(DriverError::ReservationsRemain)
    );
    assert_eq!(f.ledger.used(DimensionId::new(1).unwrap()), 1);
    let guard = f
        .runtime
        .take_failed_reservation(actor(), earlier, reservation)
        .unwrap();
    drop(guard);
    assert_eq!(f.ledger.used(DimensionId::new(1).unwrap()), 0);
    f.runtime.retire_input(actor(), earlier).unwrap();
    f.complete_failure(failed);
    f.finish();
}
