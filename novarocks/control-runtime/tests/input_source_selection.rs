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
    actor_turn::{ActorRuntime, ActorTurnError, InputPhase},
    causal_input::CausalInputObligation,
    obligation::{ReservationObligation, WorkObligation},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    effect::{EffectDescriptor, EffectKind},
    identity::*,
    mailbox::{InputClass, InputEnvelope},
    scope::{WorkAdmission, WorkKind},
};
use std::sync::{Arc, Mutex};
fn actor() -> ActorId {
    ActorId::new(1).unwrap()
}
fn continuation() -> ActorId {
    ActorId::new(21).unwrap()
}
fn protocol() -> ActorId {
    ActorId::new(31).unwrap()
}
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn vector(n: u64) -> ResourceVector {
    ResourceVector::new([(DimensionId::new(1).unwrap(), n)]).unwrap()
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Event {
    Begin,
    Protocol(u64),
    Tick,
    Reply,
    AskNext(u64),
    Reassert,
    Own,
}
struct Controller {
    seen: Arc<Mutex<Vec<(ActorId, Event)>>>,
    deadline: Option<Tick>,
}
impl Actor for Controller {
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
        self.seen.lock().unwrap().push((input.source, event));
        match event {
            Event::Begin => {
                step.select_input_source(Some(continuation()));
                self.deadline = Some(Tick(5));
            }
            Event::Reply => {
                step.select_input_source(None);
                self.deadline = None;
            }
            Event::Tick => self.deadline = None,
            Event::Reassert => step.select_input_source(Some(continuation())),
            Event::AskNext(action) => {
                step.effect(
                    EffectDescriptor {
                        id: ActionId::new(action).unwrap(),
                        work: step
                            .input_work()
                            .expect("original admitted continuation Source"),
                        kind: EffectKind::Publication,
                        dependencies: vec![],
                    },
                    (),
                )
                .unwrap();
            }
            Event::Protocol(_) | Event::Own => {}
        }
    }
    fn next_deadline(&self) -> Option<Tick> {
        self.deadline
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut ActorStep<'_, ()>) {
        panic!("unexpected fixture owner failure");
    }
}
struct Original {
    work: WorkObligation,
    capacity: ReservationObligation,
}
struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    runtime: ActorRuntime<Controller>,
    sources: Vec<Original>,
    roles: Vec<CausalInputObligation<Controller>>,
    seen: Arc<Mutex<Vec<(ActorId, Event)>>>,
}
impl Fixture {
    fn new() -> Self {
        let ledger = RuntimeLedger::new(vector(2));
        let scope = ledger.open_scope_next(None, generation()).unwrap();
        let mut sources = vec![];
        for _ in 0..2 {
            let (mut work, capacity) = scope
                .reserve_next(
                    generation(),
                    ReservationResources::for_work(vector(1)),
                    WorkKind::Settlement,
                    WorkAdmission::External,
                )
                .unwrap()
                .bind(WorkKind::Settlement)
                .unwrap();
            work.start().unwrap();
            sources.push(Original { work, capacity });
        }
        let seen = Arc::new(Mutex::new(vec![]));
        let runtime = ActorRuntime::new();
        let registration = runtime
            .register_bound(
                actor(),
                scope.clone(),
                Controller {
                    seen: seen.clone(),
                    deadline: None,
                },
                ActorConfig {
                    mailbox: [12, 8, 8, 1],
                    max_batches: 12,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 16,
                    max_step_work: 1,
                },
            )
            .unwrap_or_else(|(error, _)| panic!("actual fixture registration: {error:?}"));
        let mut roles = vec![];
        for (source, author, class) in [
            (continuation(), 0, InputClass::Lifecycle),
            (continuation(), 0, InputClass::Lifecycle),
            (protocol(), 1, InputClass::Lifecycle),
            (protocol(), 1, InputClass::Control),
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
            sources,
            roles,
            seen,
        }
    }
    fn fill(&mut self, role: usize, event: Event) -> InputId {
        self.roles[role]
            .fill_with_receipt(None, event)
            .unwrap_or_else(|failure| panic!("same admitted Source input: {:?}", failure.error))
            .1
    }
    fn retire(&self, input: InputId) {
        self.runtime.retire_input(actor(), input).unwrap();
    }
    fn finish(mut self) {
        self.runtime.close_actor_admission(actor()).unwrap();
        self.scope.close_external().unwrap();
        for input in self.runtime.input_inventory(actor(), None, 32).unwrap() {
            assert_eq!(
                input.phase,
                InputPhase::Committed,
                "filled inputs must really reach their actor"
            );
            self.runtime.retire_input(actor(), input.input).unwrap();
        }
        for source in &mut self.sources {
            source.work.body_exited().unwrap();
            source.work.captures_exited().unwrap();
        }
        for role in &mut self.roles {
            role.close_after_exit().unwrap();
        }
        self.ledger.reap_scope_ready(self.scope.id(), 32).unwrap();
        while let Some(turn) = self.runtime.claim_host_actor() {
            turn.release(false).unwrap();
        }
        self.runtime.seal_actor(actor()).unwrap();
        self.runtime.remove_actor(actor()).unwrap();
        for mut source in self.sources {
            source.capacity.release_quota(&vector(1)).unwrap();
            source.capacity.finish().unwrap();
        }
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.ledger.is_empty());
        assert!(self.runtime.is_empty());
    }
}
#[test]
fn actual_selection_keeps_paid_protocol_fifo_and_reply_progress_after_external_close_without_idle_spin()
 {
    let mut f = Fixture::new();
    let begin = f.fill(0, Event::Begin);
    assert!(f.runtime.poll_one(Tick(0)).unwrap());
    f.retire(begin);
    let first = f.fill(2, Event::Protocol(1));
    let later = f.fill(3, Event::Protocol(2));
    // A real readiness notification may cause one empty claim, then the paid
    // blocked heads must stay queued without re-enqueuing an idle actor.
    f.runtime.poll_one(Tick(0)).unwrap();
    assert!(!f.runtime.poll_one(Tick(0)).unwrap());
    assert_eq!(
        f.runtime
            .input_custody(actor(), first)
            .unwrap()
            .unwrap()
            .phase,
        InputPhase::Filled
    );
    assert_eq!(
        f.runtime
            .input_custody(actor(), later)
            .unwrap()
            .unwrap()
            .phase,
        InputPhase::Filled
    );
    assert_eq!(
        f.seen.lock().unwrap().as_slice(),
        &[(continuation(), Event::Begin)]
    );
    f.scope.close_external().unwrap();
    let reply = f.fill(0, Event::Reply);
    assert!(f.runtime.poll_one(Tick(1)).unwrap());
    assert_eq!(
        f.seen.lock().unwrap().as_slice(),
        &[
            (continuation(), Event::Begin),
            (continuation(), Event::Reply),
            (protocol(), Event::Protocol(1)),
            (protocol(), Event::Protocol(2))
        ]
    );
    for input in [reply, first, later] {
        f.retire(input);
    }
    assert!(!f.runtime.poll_one(Tick(1)).unwrap());
    f.finish();
}
#[test]
fn original_timer_and_actor_source_still_progress_while_other_source_heads_are_paid_and_waiting() {
    let mut f = Fixture::new();
    let begin = f.fill(0, Event::Begin);
    f.runtime.poll_one(Tick(0)).unwrap();
    f.retire(begin);
    let protocol = f.fill(2, Event::Protocol(1));
    let own = f.fill(4, Event::Own);
    f.runtime.poll_one(Tick(1)).unwrap();
    f.retire(own);
    assert!(!f.runtime.poll_one(Tick(1)).unwrap());
    assert_eq!(f.runtime.advance_time(Tick(5), 1).unwrap(), 1);
    f.runtime.poll_one(Tick(5)).unwrap();
    assert!(f.seen.lock().unwrap().contains(&(actor(), Event::Tick)));
    assert!(
        !f.seen
            .lock()
            .unwrap()
            .iter()
            .any(|(_, event)| matches!(event, Event::Protocol(_)))
    );
    let timer = f
        .runtime
        .input_inventory(actor(), None, 8)
        .unwrap()
        .into_iter()
        .filter(|input| input.class == InputClass::Timer)
        .collect::<Vec<_>>();
    assert_eq!(timer.len(), 1);
    f.retire(timer[0].input);
    assert!(!f.runtime.poll_one(Tick(5)).unwrap());
    let reply = f.fill(0, Event::Reply);
    f.runtime.poll_one(Tick(6)).unwrap();
    f.retire(reply);
    f.retire(protocol);
    f.finish();
}
#[test]
fn alternating_original_causal_loans_break_reply_to_successor_handoff_self_wait_without_faking_exit()
 {
    let mut f = Fixture::new();
    let begin = f.fill(0, Event::Begin);
    f.runtime.poll_one(Tick(0)).unwrap();
    f.retire(begin);
    let original = f.fill(0, Event::AskNext(1));
    f.runtime.poll_one(Tick(1)).unwrap();
    assert_eq!(
        f.runtime.retire_input(actor(), original),
        Err(ActorTurnError::EffectsRemain),
        "the original reply still owns its actual successor effect"
    );
    let refused = f.roles[0]
        .fill_with_receipt(None, Event::Reply)
        .unwrap_err();
    assert_eq!(refused.event, Event::Reply);
    assert_eq!(refused.error, ActorTurnError::StandingInputBusy);
    // The separately prepaid loan belongs to the SAME Source/work, with no
    // new grant, Source identity, quota, or fabricated consumed acknowledgement.
    let next = f.fill(1, Event::AskNext(2));
    let first_ticket = f
        .runtime
        .take_effect(actor(), original, ActionId::new(1).unwrap())
        .unwrap();
    f.runtime.effect_completed(first_ticket).unwrap();
    f.retire(original);
    f.runtime.poll_one(Tick(2)).unwrap();
    let final_reply = f.fill(0, Event::Reply);
    let second_ticket = f
        .runtime
        .take_effect(actor(), next, ActionId::new(2).unwrap())
        .unwrap();
    f.runtime.effect_completed(second_ticket).unwrap();
    f.retire(next);
    f.runtime.poll_one(Tick(3)).unwrap();
    f.retire(final_reply);
    assert_eq!(
        f.seen.lock().unwrap().as_slice(),
        &[
            (continuation(), Event::Begin),
            (continuation(), Event::AskNext(1)),
            (continuation(), Event::AskNext(2)),
            (continuation(), Event::Reply)
        ]
    );
    // Loan reuse proves runtime retirement only. The actual original Source
    // Work is still live, and finish supplies its real body/capture exit.
    assert!(!f.ledger.is_empty());
    f.finish();
}

#[test]
fn repeating_the_same_source_selection_preserves_fair_progress_of_the_actor_source() {
    let mut f = Fixture::new();
    let begin = f.fill(0, Event::Begin);
    f.runtime.poll_one(Tick(0)).unwrap();
    f.retire(begin);
    let first = f.fill(0, Event::Reassert);
    let second = f.fill(1, Event::Reassert);
    let own = f.fill(4, Event::Own);
    f.runtime.poll_one(Tick(1)).unwrap();
    let observed = f.seen.lock().unwrap().clone();
    for input in [first, second, own] {
        f.retire(input);
    }
    let reply = f.fill(0, Event::Reply);
    f.runtime.poll_one(Tick(2)).unwrap();
    f.retire(reply);
    f.finish();
    assert_eq!(
        observed,
        vec![
            (continuation(), Event::Begin),
            (continuation(), Event::Reassert),
            (actor(), Event::Own),
            (continuation(), Event::Reassert)
        ]
    );
}
