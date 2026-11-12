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
    actor_turn::{ActorRuntime, InputPhase},
    causal_input::CausalInputObligation,
    obligation::{ReservationObligation, WorkObligation},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    identity::{ActorId, DimensionId, Generation, InputId, ScopeId, Tick},
    mailbox::{InputClass, InputEnvelope},
    scope::{WorkAdmission, WorkKind},
};
use std::sync::{Arc, Mutex};

fn actor() -> ActorId {
    ActorId::new(1).unwrap()
}
fn preferred() -> ActorId {
    ActorId::new(21).unwrap()
}
fn other() -> ActorId {
    ActorId::new(31).unwrap()
}
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn quota(value: u64) -> ResourceVector {
    ResourceVector::new([(DimensionId::new(1).unwrap(), value)]).unwrap()
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Event {
    Bootstrap,
    Data(u64),
    Own,
    Stop,
    Tick,
    Reassert,
    Only,
    All,
}
struct Recorder {
    seen: Arc<Mutex<Vec<(InputEnvelope, Event)>>>,
    timer_after_bootstrap: bool,
    deadline: Option<Tick>,
}
impl Actor for Recorder {
    type Input = Event;
    type Effect = ();
    fn timer_input(_: Tick) -> Event {
        Event::Tick
    }
    fn handle(
        &mut self,
        _: Tick,
        input: InputEnvelope,
        event: Event,
        step: &mut ActorStep<'_, ()>,
    ) {
        self.seen.lock().unwrap().push((input, event));
        match event {
            Event::Bootstrap => {
                step.prefer_input_source(preferred());
                self.deadline = self.timer_after_bootstrap.then_some(Tick(5));
            }
            Event::Reassert => step.prefer_input_source(preferred()),
            Event::Only => step.select_input_source(Some(preferred())),
            Event::All => step.select_input_source(None),
            Event::Tick => self.deadline = None,
            Event::Stop => step.stop_producing(),
            Event::Data(_) | Event::Own => {}
        }
    }
    fn next_deadline(&self) -> Option<Tick> {
        self.deadline
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut ActorStep<'_, ()>) {
        panic!("source preference fixture has no failing pure state");
    }
}
struct Original {
    work: WorkObligation,
    capacity: ReservationObligation,
}
struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    runtime: ActorRuntime<Recorder>,
    registration: novarocks_control_runtime::actor_turn::ActorRegistration,
    sources: Vec<Original>,
    roles: Vec<CausalInputObligation<Recorder>>,
    seen: Arc<Mutex<Vec<(InputEnvelope, Event)>>>,
}
impl Fixture {
    fn new(timer: bool) -> Self {
        Self::with_batches(timer, 12)
    }
    fn with_batches(timer: bool, max_batches: usize) -> Self {
        let ledger = RuntimeLedger::new(quota(2));
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let mut sources = Vec::new();
        for _ in 0..2 {
            let (mut work, capacity) = scope
                .reserve_next(
                    generation(),
                    ReservationResources::for_work(quota(1)),
                    WorkKind::Cleanup,
                    WorkAdmission::External,
                )
                .unwrap()
                .bind(WorkKind::Cleanup)
                .unwrap();
            work.start().unwrap();
            sources.push(Original { work, capacity });
        }
        let seen = Arc::new(Mutex::new(Vec::new()));
        let runtime = ActorRuntime::new();
        let registration = runtime
            .register_supervised_bound(
                actor(),
                scope.clone(),
                Recorder {
                    seen: seen.clone(),
                    timer_after_bootstrap: timer,
                    deadline: None,
                },
                ActorConfig {
                    mailbox: [16, 8, 8, 1],
                    max_batches,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 16,
                    max_step_work: 1,
                },
                &sources[0].work,
            )
            .unwrap_or_else(|(error, _)| panic!("original preference registration: {error:?}"));
        let mut roles = Vec::new();
        for (source, author, class) in [
            (preferred(), 0, InputClass::Lifecycle),
            (preferred(), 0, InputClass::Control),
            (other(), 1, InputClass::Lifecycle),
            (other(), 1, InputClass::Control),
            (actor(), 0, InputClass::Control),
            (actor(), 0, InputClass::Lifecycle),
        ] {
            roles.push(
                CausalInputObligation::reserve_admitted(
                    &runtime,
                    &registration,
                    source,
                    class,
                    &sources[author].work,
                )
                .unwrap(),
            );
        }
        Self {
            ledger,
            scope,
            runtime,
            registration,
            sources,
            roles,
            seen,
        }
    }
    fn fill(&mut self, role: usize, event: Event) -> InputId {
        self.roles[role]
            .fill_with_receipt(None, event)
            .unwrap_or_else(|failure| panic!("same paid Source input: {:?}", failure.error))
            .1
    }
    fn bootstrap(&mut self) {
        let input = self.fill(5, Event::Bootstrap);
        self.runtime.poll_one(Tick(0)).unwrap();
        self.runtime.retire_input(actor(), input).unwrap();
    }
    fn records(&self) -> Vec<(InputEnvelope, Event)> {
        self.seen.lock().unwrap().clone()
    }
    fn finish(mut self) {
        // A genuine own-source input restores All AFTER each measured oracle.
        // It cannot alter the saved trace and lets a future failure retire all
        // original paid inputs rather than aborting in obligation Drop.
        self.fill(5, Event::All);
        for _ in 0..24 {
            for input in self.runtime.input_inventory(actor(), None, 32).unwrap() {
                if input.phase == InputPhase::Committed {
                    self.runtime.retire_input(actor(), input.input).unwrap();
                }
            }
            self.runtime.poll_one(Tick(5)).unwrap();
        }
        self.runtime.close_actor_admission(actor()).unwrap();
        self.scope.close_external().unwrap();
        for input in self.runtime.input_inventory(actor(), None, 32).unwrap() {
            assert_eq!(input.phase, InputPhase::Committed);
            self.runtime.retire_input(actor(), input.input).unwrap();
        }
        for source in &mut self.sources {
            source.work.body_exited().unwrap();
            source.work.captures_exited().unwrap();
        }
        for role in &mut self.roles {
            role.close_after_exit().unwrap();
        }
        self.ledger.reap_scope_ready(self.scope.id(), 8).unwrap();
        while let Some(host) = self.runtime.claim_host_actor() {
            host.release(false).unwrap();
        }
        self.runtime.seal_actor(actor()).unwrap();
        self.runtime.remove_actor(actor()).unwrap();
        for mut source in self.sources {
            source.capacity.release_quota(&quota(1)).unwrap();
            source.capacity.finish().unwrap();
        }
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
        assert!(self.runtime.is_empty());
    }
}

#[test]
fn default_all_keeps_original_order_and_real_bootstrap_precedes_competing_inputs() {
    for bootstrap in [false, true] {
        let mut f = Fixture::new(false);
        if bootstrap {
            f.bootstrap();
        }
        f.fill(2, Event::Data(2));
        f.fill(0, Event::Data(1));
        f.runtime.poll_one(Tick(1)).unwrap();
        let actual: Vec<_> = f.records().into_iter().map(|(_, event)| event).collect();
        f.finish();
        let expected = if bootstrap {
            vec![Event::Bootstrap, Event::Data(1), Event::Data(2)]
        } else {
            vec![Event::Data(2), Event::Data(1)]
        };
        assert_eq!(actual, expected);
    }
}

#[test]
fn preferred_source_cross_class_fifo_blocks_neither_its_head_nor_empty_fallback() {
    let mut f = Fixture::new(false);
    f.bootstrap();
    f.fill(2, Event::Data(3));
    let head = f
        .runtime
        .reserve_admitted_input_for(
            &f.registration,
            preferred(),
            InputClass::Ordinary,
            &f.sources[0].work,
        )
        .unwrap();
    head.fill(Event::Data(1)).unwrap();
    f.fill(1, Event::Data(2));
    f.runtime.poll_one(Tick(1)).unwrap();
    let records = f.records();
    f.finish();
    assert_eq!(
        records.iter().map(|(_, event)| *event).collect::<Vec<_>>(),
        [
            Event::Bootstrap,
            Event::Data(1),
            Event::Data(2),
            Event::Data(3)
        ]
    );
    assert_eq!(records[1].0.class, InputClass::Ordinary);
    assert_eq!(records[2].0.class, InputClass::Control);
}

#[test]
fn empty_fallback_and_later_preferred_fill_use_real_wakes_even_after_external_close() {
    let mut f = Fixture::new(false);
    f.bootstrap();
    let first = f.fill(2, Event::Data(1));
    let other_woke = f.runtime.poll_one(Tick(1)).unwrap();
    f.runtime.retire_input(actor(), first).unwrap();
    let idle_without_spin = !f.runtime.poll_one(Tick(1)).unwrap();
    let later = f.fill(0, Event::Data(2));
    let preferred_woke = f.runtime.poll_one(Tick(2)).unwrap();
    f.runtime.retire_input(actor(), later).unwrap();
    f.runtime.close_actor_admission(actor()).unwrap();
    f.scope.close_external().unwrap();
    f.fill(3, Event::Data(4));
    f.fill(1, Event::Data(3));
    f.runtime.poll_one(Tick(3)).unwrap();
    let actual: Vec<_> = f.records().into_iter().map(|(_, event)| event).collect();
    let work_still_live = !f.sources[0].work.actual_exit_ready().unwrap();
    f.finish();
    assert!(other_woke && idle_without_spin && preferred_woke && work_still_live);
    assert_eq!(
        actual,
        [
            Event::Bootstrap,
            Event::Data(1),
            Event::Data(2),
            Event::Data(3),
            Event::Data(4)
        ]
    );
}

#[test]
fn repeated_preference_and_same_pair_mode_changes_preserve_own_source_fairness() {
    for first in [Event::Reassert, Event::Only] {
        let mut f = Fixture::new(false);
        f.bootstrap();
        for event in [first, Event::Reassert] {
            f.runtime
                .reserve_admitted_input_for(
                    &f.registration,
                    preferred(),
                    InputClass::Control,
                    &f.sources[0].work,
                )
                .unwrap()
                .fill(event)
                .unwrap();
        }
        f.fill(4, Event::Own);
        f.runtime.poll_one(Tick(1)).unwrap();
        let actual: Vec<_> = f.records().into_iter().map(|(_, event)| event).collect();
        f.finish();
        assert_eq!(
            actual,
            [Event::Bootstrap, first, Event::Own, Event::Reassert]
        );
    }
}

#[test]
fn own_timer_and_stop_are_consumed_in_original_source_fifo_without_skipping() {
    let mut f = Fixture::new(true);
    f.bootstrap();
    f.runtime.advance_time(Tick(5), 1).unwrap();
    f.fill(4, Event::Stop);
    f.fill(2, Event::Data(2));
    f.fill(0, Event::Data(1));
    f.runtime.poll_one(Tick(5)).unwrap();
    let records = f.records();
    let stopped = f.scope.gates().unwrap().stopped;
    f.finish();
    let timer = records
        .iter()
        .position(|(_, event)| *event == Event::Tick)
        .unwrap();
    let stop = records
        .iter()
        .position(|(_, event)| *event == Event::Stop)
        .unwrap();
    assert!(timer < stop && stopped);
    assert_eq!(records[timer].0.source, actor());
    assert_eq!(records[timer].0.class, InputClass::Timer);
    assert_eq!(records[stop].0.source, actor());
    assert_eq!(records[stop].0.class, InputClass::Control);
    assert_eq!(
        records
            .iter()
            .filter(|(_, event)| matches!(event, Event::Data(_)))
            .count(),
        2
    );
}

#[test]
fn empty_preference_retains_all_source_round_robin_and_cross_class_heads() {
    let run = |bootstrap| {
        let mut f = Fixture::new(false);
        if bootstrap {
            f.bootstrap();
        }
        f.fill(2, Event::Data(1));
        f.runtime
            .reserve_admitted_input_for(
                &f.registration,
                ActorId::new(41).unwrap(),
                InputClass::Lifecycle,
                &f.sources[1].work,
            )
            .unwrap()
            .fill(Event::Data(2))
            .unwrap();
        f.fill(3, Event::Data(3));
        f.runtime.poll_one(Tick(1)).unwrap();
        let records: Vec<_> = f
            .records()
            .into_iter()
            .filter(|(_, event)| *event != Event::Bootstrap)
            .map(|(input, event)| (input.source, input.class, event))
            .collect();
        f.finish();
        records
    };
    // Compare against the original All scheduler rather than assuming class
    // priority: the original TurnBudget also rotates between ready classes.
    let original = run(false);
    let empty_preference = run(true);
    assert_eq!(empty_preference, original);
    assert_eq!(original.len(), 3);
    let earlier = original
        .iter()
        .position(|(_, _, event)| *event == Event::Data(1))
        .unwrap();
    let later = original
        .iter()
        .position(|(_, _, event)| *event == Event::Data(3))
        .unwrap();
    assert!(earlier < later);
    assert_eq!(original[earlier].0, other());
    assert_eq!(original[later].0, other());
}

#[test]
fn real_batch_retirement_reawakens_empty_preference_fallback() {
    let mut f = Fixture::with_batches(false, 1);
    f.bootstrap();
    f.fill(2, Event::Data(2));
    let preferred_input = f.fill(0, Event::Data(1));
    f.runtime.poll_one(Tick(1)).unwrap();
    let first = f.records();
    let full_does_not_spin = !f.runtime.poll_one(Tick(1)).unwrap();
    f.runtime.retire_input(actor(), preferred_input).unwrap();
    let fallback_woke = f.runtime.poll_one(Tick(2)).unwrap();
    let final_records = f.records();
    f.finish();
    assert_eq!(
        first.iter().map(|(_, event)| *event).collect::<Vec<_>>(),
        [Event::Bootstrap, Event::Data(1)]
    );
    assert!(full_does_not_spin && fallback_woke);
    assert_eq!(
        final_records
            .iter()
            .map(|(_, event)| *event)
            .collect::<Vec<_>>(),
        [Event::Bootstrap, Event::Data(1), Event::Data(2)]
    );
}
