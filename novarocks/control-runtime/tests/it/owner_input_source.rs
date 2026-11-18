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

use novarocks_control_runtime::{
    actor::{Actor, ActorConfig, ActorStep, InputSourceCut},
    actor_turn::{ActorRuntime, TimerSourceState},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    identity::{ActorId, Generation, InputId, ScopeId, Tick},
    mailbox::{InputClass, InputEnvelope},
};
use std::sync::{Arc, Mutex};
type Observations = Arc<Mutex<Vec<(u8, InputEnvelope, Option<InputSourceCut>)>>>;
fn id(n: u64) -> ActorId {
    ActorId::new(n).unwrap()
}
#[derive(Debug)]
enum Input {
    Select(bool),
    Value(u8),
    Open,
}
#[derive(Debug)]
struct Receiver {
    primary: ActorId,
    owner: ActorId,
    seen: Observations,
}
impl Actor for Receiver {
    type Input = Input;
    type Effect = ();
    fn timer_input(_: Tick) -> Input {
        panic!("this fixture has no timer")
    }
    fn handle(
        &mut self,
        _: Tick,
        envelope: InputEnvelope,
        input: Input,
        step: &mut ActorStep<'_, ()>,
    ) {
        assert!(step.charge(1));
        match input {
            Input::Select(prefer) => {
                if prefer {
                    step.prefer_input_source_with_owner(self.primary, self.owner)
                } else {
                    step.select_input_source_with_owner(self.primary, self.owner)
                }
            }
            Input::Value(value) => {
                self.seen
                    .lock()
                    .unwrap()
                    .push((value, envelope, step.selected_source_cut()))
            }
            Input::Open => step.select_input_source(None),
        }
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut ActorStep<'_, ()>) {}
}
struct Fixture {
    runtime: ActorRuntime<Receiver>,
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    seen: Observations,
    inputs: Vec<InputId>,
}
impl Fixture {
    fn new(prefer: bool) -> Self {
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let scope = ledger
            .open_scope_next(None, Generation::new(1).unwrap())
            .unwrap();
        let runtime = ActorRuntime::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        runtime
            .register_bound_with_source_cut(
                id(1),
                scope.clone(),
                Receiver {
                    primary: id(2),
                    owner: id(3),
                    seen: seen.clone(),
                },
                ActorConfig {
                    mailbox: [16; 4],
                    max_batches: 32,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    max_step_work: 1,
                    turn_budget: 128,
                },
                id(2),
            )
            .unwrap();
        let mut f = Self {
            runtime,
            ledger,
            scope,
            seen,
            inputs: Vec::new(),
        };
        f.fill(id(1), InputClass::Lifecycle, Input::Select(prefer));
        f.poll();
        f
    }
    fn fill(&mut self, source: ActorId, class: InputClass, value: Input) {
        self.inputs.push(
            self.runtime
                .reserve_input(id(1), source, class)
                .unwrap()
                .fill(value)
                .unwrap(),
        );
    }
    fn poll(&self) {
        self.runtime.poll_one(Tick(1)).unwrap();
    }
    fn values(&self) -> Vec<u8> {
        self.seen.lock().unwrap().iter().map(|row| row.0).collect()
    }
    fn finish(mut self) {
        self.fill(id(1), InputClass::Lifecycle, Input::Open);
        self.poll();
        for input in self.inputs {
            self.runtime.retire_input(id(1), input).unwrap();
        }
        self.runtime.close_actor_admission(id(1)).unwrap();
        assert_eq!(
            self.runtime.close_timer_source(id(1)).unwrap(),
            TimerSourceState::ClosedDrained
        );
        while let Some(turn) = self.runtime.claim_host_actor() {
            turn.release(false).unwrap();
        }
        self.runtime.seal_actor(id(1)).unwrap();
        self.scope.close_external().unwrap();
        drop(self.runtime.remove_actor(id(1)).unwrap());
        self.ledger.remove_scope(self.scope.id()).unwrap();
        assert!(self.runtime.is_empty() && self.ledger.is_empty());
    }
}
#[test]
fn owner_head_reaches_a_parked_continuation_without_changing_the_primary_cut_or_fifo() {
    let mut f = Fixture::new(false);
    f.fill(id(4), InputClass::Control, Input::Value(99));
    f.fill(id(3), InputClass::Lifecycle, Input::Value(11));
    f.fill(id(3), InputClass::Control, Input::Value(12));
    f.poll();
    assert_eq!(f.values(), vec![11, 12]);
    for (_, envelope, cut) in f.seen.lock().unwrap().iter() {
        assert_eq!(envelope.source, id(3));
        assert_eq!(
            *cut,
            Some(InputSourceCut {
                source: id(2),
                tail: None
            })
        );
    }
    f.finish();
}
#[test]
fn owner_primary_and_actor_heads_all_progress_while_the_preferred_source_is_busy() {
    for prefer in [false, true] {
        let mut f = Fixture::new(prefer);
        for n in 0..3 {
            f.fill(id(2), InputClass::Ordinary, Input::Value(10 + n));
            f.fill(id(1), InputClass::Ordinary, Input::Value(20 + n));
            f.fill(id(3), InputClass::Ordinary, Input::Value(30 + n));
        }
        f.poll();
        let seen = f.values();
        assert_eq!(seen.len(), 9);
        for band in [10, 20, 30] {
            assert_eq!(
                seen.iter()
                    .copied()
                    .filter(|n| *n >= band && *n < band + 3)
                    .collect::<Vec<_>>(),
                vec![band, band + 1, band + 2]
            );
        }
        assert!(seen[..3].contains(&10) && seen[..3].contains(&20) && seen[..3].contains(&30));
        f.finish();
    }
}
