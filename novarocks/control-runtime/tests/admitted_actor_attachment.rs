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
    transaction::{RuntimeError, RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    identity::{ActorId, DimensionId, Generation, ScopeId, Tick},
    mailbox::InputEnvelope,
    scope::{ScopeError, WorkAdmission, WorkKind},
};
use std::sync::{Arc, Condvar, Mutex};

fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn actor() -> ActorId {
    ActorId::new(1).unwrap()
}
fn config() -> ActorConfig {
    ActorConfig {
        mailbox: [1; 4],
        max_batches: 4,
        max_effects_per_step: 1,
        max_dependencies_per_effect: 0,
        turn_budget: 8,
        max_step_work: 1,
    }
}
// (The deadline preflight was reached, registration may continue.)
type RegistrationPauseState = (bool, bool);
type RegistrationPause = Arc<(Mutex<RegistrationPauseState>, Condvar)>;

#[derive(Debug, Default)]
struct TestActor {
    pause: Option<RegistrationPause>,
}
impl Actor for TestActor {
    type Input = ();
    type Effect = ();
    fn timer_input(_: Tick) {
        unreachable!("fixture has no deadline")
    }
    fn handle(&mut self, _: Tick, _: InputEnvelope, _: (), _: &mut StepContext<'_, ()>) {}
    fn next_deadline(&self) -> Option<Tick> {
        // Controlled test scheduling between the real driver preflight and
        // its same-ledger attachment revalidation. No host callback is run.
        if let Some(pause) = &self.pause {
            let mut state = pause.0.lock().unwrap();
            state.0 = true;
            pause.1.notify_all();
            while !state.1 {
                state = pause.1.wait(state).unwrap();
            }
        }
        None
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut StepContext<'_, ()>) {
        unreachable!("fixture does not run a failing Core")
    }
}
struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    runtime: ActorRuntime<TestActor>,
    work: WorkLease,
    capacity: CapacityBundle,
    body_exited: bool,
    captures_exited: bool,
}
impl Fixture {
    fn new(kind: WorkKind, slots: usize) -> Self {
        // The original owner pays its supervisor plus one possible derived
        // metadata holder; attaching an actor must not acquire this again.
        let resources = ResourceVector::new([(DimensionId::new(1).unwrap(), 2)]).unwrap();
        let ledger = RuntimeLedger::new(resources.clone(), 4, 1);
        let scope = ledger
            .open_scope_next(None, generation(), 4, slots)
            .unwrap();
        let (mut work, capacity) = scope
            .reserve_next(generation(), resources, kind, WorkAdmission::External)
            .unwrap()
            .bind(kind)
            .unwrap();
        work.start().unwrap();
        Self {
            ledger,
            scope,
            runtime: ActorRuntime::new(2),
            work,
            capacity,
            body_exited: false,
            captures_exited: false,
        }
    }
    fn body_exited(&mut self) {
        self.work.body_exited().unwrap();
        self.body_exited = true;
    }
    fn captures_exited(&mut self) {
        self.work.captures_exited().unwrap();
        self.captures_exited = true;
    }
    fn finish(mut self, registered: bool) {
        if registered {
            self.runtime.close_actor_admission(actor()).unwrap();
            self.runtime.seal_actor(actor()).unwrap();
        }
        self.scope.close_external().unwrap();
        if !self.body_exited {
            self.body_exited();
        }
        if !self.captures_exited {
            self.captures_exited();
        }
        self.ledger.reap_scope_ready(self.scope.id(), 8).unwrap();
        if registered {
            drop(self.runtime.remove_actor(actor()).unwrap());
        }
        self.capacity
            .release(&self.capacity.resources().unwrap())
            .unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.runtime.is_empty());
        assert!(self.ledger.is_empty());
    }
}

#[test]
fn admitted_supervisor_attaches_after_external_close_while_ordinary_registration_stays_closed() {
    let f = Fixture::new(WorkKind::Cleanup, 1);
    f.ledger.close_admission();
    f.scope.close_external().unwrap();
    let before = f.scope.snapshot().unwrap();
    assert!(matches!(
        f.runtime
            .register_next(actor(), f.scope.clone(), TestActor::default(), config()),
        Err((
            DriverError::Runtime(RuntimeError::Scope(ScopeError::Closed)),
            _
        ))
    ));
    assert_eq!(f.scope.snapshot().unwrap(), before);
    f.runtime
        .register_supervised_next(
            actor(),
            f.scope.clone(),
            TestActor::default(),
            config(),
            &f.work,
        )
        .unwrap();
    assert_eq!(f.scope.snapshot().unwrap().storage, 1);
    assert_eq!(f.scope.snapshot().unwrap().work, before.work);
    assert_eq!(f.ledger.used(DimensionId::new(1).unwrap()), 2);
    assert!(!f.scope.gates().unwrap().external_open);
    assert!(matches!(
        f.scope.reserve_next(
            generation(),
            ResourceVector::default(),
            WorkKind::Business,
            WorkAdmission::External
        ),
        Err(RuntimeError::Scope(ScopeError::Closed))
    ));
    f.finish(true);
}

#[test]
fn stopped_scope_can_attach_original_cleanup_core_without_reopening_business() {
    let f = Fixture::new(WorkKind::Cleanup, 1);
    f.scope.request_stop().unwrap();
    f.runtime
        .register_supervised_next(
            actor(),
            f.scope.clone(),
            TestActor::default(),
            config(),
            &f.work,
        )
        .unwrap();
    let refused = matches!(
        f.scope.reserve_child_next(
            generation(),
            f.capacity.id(),
            ResourceVector::default(),
            WorkKind::Business
        ),
        Err(RuntimeError::Capacity(
            novarocks_control_runtime_core::capacity::CapacityError::Scope(ScopeError::Stopped)
        ))
    );
    f.finish(true);
    assert!(refused);
}

#[test]
fn failed_scope_or_exited_supervisor_never_revives_a_core_or_reserves_an_attachment() {
    for mode in 0..4 {
        let mut f = Fixture::new(
            if mode == 3 {
                WorkKind::Business
            } else {
                WorkKind::Cleanup
            },
            1,
        );
        match mode {
            0 => {
                f.scope.fail().unwrap();
            }
            1 => f.body_exited(),
            2 => f.captures_exited(),
            3 => {}
            _ => unreachable!(),
        }
        let result = f.runtime.register_supervised_next(
            actor(),
            f.scope.clone(),
            TestActor::default(),
            config(),
            &f.work,
        );
        assert!(result.is_err());
        assert_eq!(f.scope.snapshot().unwrap().storage, 0);
        assert!(f.runtime.is_empty());
        f.finish(false);
    }
}

#[test]
fn admitted_attachment_does_not_bypass_slot_capacity_or_leave_failed_scheduler_registration_slots()
{
    let f = Fixture::new(WorkKind::Cleanup, 1);
    f.scope.close_external().unwrap();
    f.runtime
        .register_supervised_next(
            actor(),
            f.scope.clone(),
            TestActor::default(),
            config(),
            &f.work,
        )
        .unwrap();
    assert!(matches!(
        f.runtime.register_supervised_next(
            ActorId::new(2).unwrap(),
            f.scope.clone(),
            TestActor::default(),
            config(),
            &f.work
        ),
        Err((
            DriverError::Runtime(RuntimeError::Scope(ScopeError::SlotCapacity)),
            _
        ))
    ));
    assert_eq!(f.scope.snapshot().unwrap().storage, 1);
    f.finish(true);

    let f = Fixture::new(WorkKind::Cleanup, 1);
    f.runtime.close_admission();
    assert!(matches!(
        f.runtime.register_supervised_next(
            actor(),
            f.scope.clone(),
            TestActor::default(),
            config(),
            &f.work
        ),
        Err((DriverError::Closed, _))
    ));
    assert_eq!(f.scope.snapshot().unwrap().storage, 0);
    f.finish(false);
}

#[test]
fn scope_failure_between_driver_preflight_and_attachment_is_rechecked_atomically() {
    let f = Fixture::new(WorkKind::Cleanup, 1);
    let pause = Arc::new((Mutex::new((false, false)), Condvar::new()));
    let result = std::thread::scope(|threads| {
        let pause_ref = pause.clone();
        let registration = threads.spawn(|| {
            f.runtime.register_supervised_next(
                actor(),
                f.scope.clone(),
                TestActor {
                    pause: Some(pause_ref),
                },
                config(),
                &f.work,
            )
        });
        let mut state = pause.0.lock().unwrap();
        while !state.0 {
            state = pause.1.wait(state).unwrap();
        }
        f.scope.fail().unwrap();
        state.1 = true;
        pause.1.notify_all();
        drop(state);
        registration.join().unwrap()
    });
    let refused = matches!(
        result,
        Err((
            DriverError::Runtime(RuntimeError::Scope(ScopeError::Failed)),
            _
        ))
    );
    let registered = result.is_ok();
    let no_attachment = f.scope.snapshot().unwrap().storage == 0;
    drop(result);
    f.finish(registered);
    assert!(
        refused,
        "a failed owner cannot attach a new Core after preflight"
    );
    assert!(no_attachment);
}

#[test]
fn foreign_supervisor_cannot_authorize_attachment_in_another_ledger() {
    let a = Fixture::new(WorkKind::Cleanup, 1);
    let b = Fixture::new(WorkKind::Cleanup, 1);
    assert!(matches!(
        a.runtime.register_supervised_next(
            actor(),
            a.scope.clone(),
            TestActor::default(),
            config(),
            &b.work
        ),
        Err((DriverError::PublicationTargetMismatch, _))
    ));
    assert_eq!(a.scope.snapshot().unwrap().storage, 0);
    assert_eq!(b.scope.snapshot().unwrap().storage, 0);
    a.finish(false);
    b.finish(false);
}
