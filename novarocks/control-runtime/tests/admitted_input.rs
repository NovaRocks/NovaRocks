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

//! An admitted holder may finish its bounded neutral handoff after external
//! closure. Scope identity and actual holder exits remain authoritative.
use novarocks_control_runtime::{
    actor::{Actor, ActorConfig, StepContext},
    driver::{ActorRuntime, DriverError, InputPermit},
    lease::{ReservationBundle, WorkLease},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    identity::{ActorId, DimensionId, Generation, ScopeId, Tick},
    mailbox::{InputClass, InputEnvelope},
    scope::{WorkAdmission, WorkKind},
};

#[derive(Debug)]
struct Neutral;
impl Actor for Neutral {
    type Input = ();
    type Effect = ();
    fn timer_input(_: Tick) {
        unreachable!("no timer in this fixture")
    }
    fn handle(&mut self, _: Tick, _: InputEnvelope, _: (), _: &mut StepContext<'_, ()>) {}
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut StepContext<'_, ()>) {
        unreachable!("no failing pure step in this fixture")
    }
}
fn actor() -> ActorId {
    ActorId::new(1).unwrap()
}
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    runtime: ActorRuntime<Neutral>,
    work: WorkLease,
    capacity: ReservationBundle,
    body_exited: bool,
    captures_exited: bool,
}
impl Fixture {
    fn new() -> Self {
        let paid = ResourceVector::new([(DimensionId::new(1).unwrap(), 2)]).unwrap();
        let ledger = RuntimeLedger::new(paid.clone());
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let (mut work, capacity) = scope
            .reserve_next(
                generation(),
                ReservationResources::for_work(paid),
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        work.start().unwrap();
        let runtime = ActorRuntime::new();
        runtime
            .register_next(
                actor(),
                scope.clone(),
                Neutral,
                ActorConfig {
                    mailbox: [2; 4],
                    max_batches: 4,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
            )
            .unwrap();
        Self {
            ledger,
            scope,
            runtime,
            work,
            capacity,
            body_exited: false,
            captures_exited: false,
        }
    }
    fn admitted(&self) -> Result<InputPermit<Neutral>, DriverError> {
        self.runtime
            .reserve_admitted_input(actor(), actor(), InputClass::Control, &self.work)
    }
    fn finish(mut self) {
        assert_eq!(self.runtime.snapshot(actor()).unwrap().inputs, 0);
        assert_eq!(self.runtime.snapshot(actor()).unwrap().roles, 0);
        self.runtime.close_actor_admission(actor()).unwrap();
        // A committed empty batch still delivered a real dispatcher wake.
        // Consume it after the original inputs retired, before sealing.
        while let Some(original_host_turn) = self.runtime.claim_host_actor() {
            original_host_turn.release(false).unwrap();
        }
        self.runtime.seal_actor(actor()).unwrap();
        self.scope.close_external().unwrap();
        if !self.body_exited {
            self.work.body_exited().unwrap();
        }
        if !self.captures_exited {
            self.work.captures_exited().unwrap();
        }
        self.ledger.reap_scope_ready(self.scope.id(), 16).unwrap();
        self.runtime.remove_actor(actor()).unwrap();
        self.capacity
            .release_quota(&self.capacity.resources().unwrap().quota)
            .unwrap();
        self.capacity.finish().unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
        assert!(self.runtime.is_empty());
    }
}

#[test]
fn an_existing_holder_can_finish_after_all_external_gates_close_without_new_work_or_capacity() {
    let mut f = Fixture::new();
    f.ledger.close_admission();
    f.scope.close_external().unwrap();
    f.scope.request_stop().unwrap();
    f.runtime.close_actor_admission(actor()).unwrap();
    f.runtime.close_admission();
    let before = f.scope.snapshot().unwrap();
    let ordinary_closed = matches!(
        f.runtime
            .reserve_input(actor(), actor(), InputClass::Control),
        Err(DriverError::Closed)
    );
    let mut role = f.admitted().unwrap().into_role().unwrap();
    let second = f.admitted().unwrap();
    let full = matches!(f.admitted(), Err(DriverError::InputCapacity));
    // The producer really ends before its already-admitted completion is
    // dispatched. No child Work can accidentally keep this header resident.
    f.work.body_exited().unwrap();
    f.body_exited = true;
    f.work.captures_exited().unwrap();
    f.captures_exited = true;
    assert!(f.work.actual_exit_ready().unwrap());
    let original_exit = f.scope.work_lifecycle(f.work.id()).unwrap().unwrap();
    assert!(original_exit.1 && original_exit.2);
    assert_eq!(original_exit.3, 0);
    assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap(), 0);
    assert!(f.scope.work_lifecycle(f.work.id()).unwrap().is_some());
    assert!(matches!(
        f.admitted(),
        Err(DriverError::InputAuthorInactive)
    ));
    let first = role.fill(()).unwrap();
    f.runtime.poll_one(Tick(1)).unwrap();
    f.runtime.retire_input(actor(), first).unwrap();
    assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap(), 0);
    let next = role.fill(()).unwrap();
    f.runtime.poll_one(Tick(2)).unwrap();
    f.runtime.retire_input(actor(), next).unwrap();
    second.revoke_unused().unwrap();
    role.close_after_exit().unwrap();
    let same_scope = f.scope.snapshot().unwrap() == before;
    let same_charge = f.ledger.used(DimensionId::new(1).unwrap()) == 2;
    assert_eq!(f.ledger.reap_scope_ready(f.scope.id(), 1).unwrap(), 1);
    assert!(f.scope.work_lifecycle(f.work.id()).unwrap().is_none());
    f.finish();
    assert!(ordinary_closed);
    assert!(full);
    assert_ne!(first, next);
    assert!(same_scope);
    assert!(same_charge);
}

#[test]
fn matching_local_work_numbers_from_another_scope_or_ledger_cannot_author_an_input() {
    let f = Fixture::new();
    let foreign = Fixture::new();
    assert_eq!(f.scope.id(), foreign.scope.id());
    assert_eq!(f.work.id(), foreign.work.id());
    let before = f.runtime.snapshot(actor()).unwrap();
    let other_ledger = matches!(
        f.runtime
            .reserve_admitted_input(actor(), actor(), InputClass::Control, &foreign.work,),
        Err(DriverError::InputAuthorMismatch)
    );
    let other_scope = f.ledger.open_scope_next(None, generation()).unwrap();
    let (mut other_work, mut other_capacity) = other_scope
        .reserve_next(
            generation(),
            ReservationResources::work_only(),
            WorkKind::Settlement,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Settlement)
        .unwrap();
    assert_eq!(f.work.id(), other_work.id());
    let other_owner = matches!(
        f.runtime
            .reserve_admitted_input(actor(), actor(), InputClass::Control, &other_work,),
        Err(DriverError::InputAuthorMismatch)
    );
    let after = f.runtime.snapshot(actor()).unwrap();
    other_work.body_exited().unwrap();
    other_work.captures_exited().unwrap();
    other_scope.close_external().unwrap();
    f.ledger.reap_scope_ready(other_scope.id(), 4).unwrap();
    other_capacity
        .release_quota(&other_capacity.resources().unwrap().quota)
        .unwrap();
    other_capacity.finish().unwrap();
    f.ledger.remove_scope(other_scope.id()).unwrap();
    foreign.finish();
    f.finish();
    assert!(other_ledger);
    assert!(other_owner);
    assert_eq!(before.inputs, after.inputs);
    assert_eq!(before.roles, after.roles);
}

#[test]
fn exited_unknown_failed_and_sealed_authors_do_not_gain_positions() {
    for mode in 0..6 {
        let mut f = Fixture::new();
        let expected = match mode {
            0 => {
                f.work.body_exited().unwrap();
                f.body_exited = true;
                DriverError::InputAuthorInactive
            }
            1 => {
                f.work.captures_exited().unwrap();
                f.captures_exited = true;
                DriverError::InputAuthorInactive
            }
            2 => {
                f.work.mark_unresolved();
                DriverError::InputAuthorInactive
            }
            3 => {
                f.scope.fail().unwrap();
                DriverError::ActorFailed
            }
            4 => {
                f.runtime.close_actor_admission(actor()).unwrap();
                f.runtime.seal_actor(actor()).unwrap();
                DriverError::Closed
            }
            _ => DriverError::InvalidConfig,
        };
        let class = if mode == 5 {
            InputClass::Timer
        } else {
            InputClass::Control
        };
        let before = f.scope.snapshot().unwrap();
        let refused = matches!(f.runtime.reserve_admitted_input(actor(), actor(), class, &f.work), Err(error) if error == expected);
        let unchanged = f.scope.snapshot().unwrap() == before;
        let no_position = f.runtime.snapshot(actor()).unwrap().inputs == 0;
        f.finish();
        assert!(refused, "author case {mode}");
        assert!(unchanged);
        assert!(no_position);
    }
}
