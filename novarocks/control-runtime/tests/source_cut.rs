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
    actor::{Actor, ActorConfig, ActorStep, InputSourceCut},
    actor_turn::{ActorRegistration, ActorRuntime, InputPhase},
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
fn watched() -> ActorId {
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
#[derive(Clone, Copy, Debug)]
struct Record {
    input: InputEnvelope,
    cut: Option<InputSourceCut>,
    selected: Option<InputSourceCut>,
}
struct Recorder {
    seen: Arc<Mutex<Vec<Record>>>,
    deadline: Option<Tick>,
    phase: Option<SourcePhases>,
}
struct SourcePhases {
    frozen_tail: Option<InputId>,
    other_finished: bool,
}
impl Actor for Recorder {
    type Input = u8;
    type Effect = ();
    fn timer_input(_: Tick) -> u8 {
        0
    }
    fn handle(&mut self, _: Tick, input: InputEnvelope, value: u8, step: &mut ActorStep<'_, ()>) {
        self.seen.lock().unwrap().push(Record {
            input,
            cut: step.input_source_cut(),
            selected: step.selected_source_cut(),
        });
        if input.class == InputClass::Timer {
            self.deadline = None;
        }
        if value == u8::MAX {
            // A real paid fixture input restores admission only AFTER the
            // measured sequence, so a failing oracle can still retire every
            // original input instead of aborting during obligation Drop.
            step.select_input_source(None);
            self.phase = None;
        } else if let Some(phase) = self.phase.as_mut() {
            if input.source == watched() {
                if phase.other_finished {
                    step.select_input_source(None);
                } else {
                    if phase.frozen_tail.is_none() {
                        phase.frozen_tail = step.input_source_cut().and_then(|cut| cut.tail);
                    }
                    let next = if Some(input.input) == phase.frozen_tail {
                        other()
                    } else {
                        watched()
                    };
                    step.select_input_source(Some(next));
                }
            } else if input.source == other() {
                phase.other_finished = true;
                step.select_input_source(Some(watched()));
            }
        }
    }
    fn next_deadline(&self) -> Option<Tick> {
        self.deadline
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut ActorStep<'_, ()>) {
        panic!("source cut fixture has no failing pure state");
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
    registration: ActorRegistration,
    sources: Vec<Original>,
    roles: Vec<CausalInputObligation<Recorder>>,
    seen: Arc<Mutex<Vec<Record>>>,
}
impl Fixture {
    fn new(observe: bool, supervised: bool, timer: bool) -> Self {
        Self::with_source_phases(observe, supervised, timer, false)
    }
    fn with_source_phases(observe: bool, supervised: bool, timer: bool, phases: bool) -> Self {
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
        let runtime = ActorRuntime::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorder = Recorder {
            seen: seen.clone(),
            deadline: timer.then_some(Tick(1)),
            phase: phases.then_some(SourcePhases {
                frozen_tail: None,
                other_finished: false,
            }),
        };
        let config = ActorConfig {
            mailbox: [8, 8, 8, 1],
            max_batches: if phases { 1 } else { 8 },
            max_effects_per_step: 1,
            max_dependencies_per_effect: 0,
            turn_budget: 16,
            max_step_work: 1,
        };
        let registration = match (observe, supervised) {
            (true, true) => runtime.register_supervised_bound_with_source_cut(
                actor(),
                scope.clone(),
                recorder,
                config,
                &sources[0].work,
                watched(),
            ),
            (true, false) => runtime.register_bound_with_source_cut(
                actor(),
                scope.clone(),
                recorder,
                config,
                watched(),
            ),
            (false, _) => runtime.register_bound(actor(), scope.clone(), recorder, config),
        }
        .unwrap_or_else(|(error, _)| panic!("original source-cut registration: {error:?}"));
        let mut roles = Vec::new();
        for (source, author, class) in [
            (watched(), 0, InputClass::Lifecycle),
            (watched(), 0, InputClass::Control),
            (other(), 1, InputClass::Control),
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
    fn fill(&mut self, role: usize) -> InputId {
        self.roles[role]
            .fill_with_receipt(None, 1)
            .unwrap_or_else(|failure| panic!("same paid Source input: {:?}", failure.error))
            .1
    }
    fn records(&self) -> Vec<Record> {
        self.seen.lock().unwrap().clone()
    }
    fn finish(mut self) {
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
fn source_cut_includes_the_claimed_input_and_default_registration_remains_unobserved() {
    for observe in [false, true] {
        let mut f = Fixture::new(observe, false, false);
        let input = f.fill(0);
        f.runtime.poll_one(Tick(0)).unwrap();
        let records = f.records();
        f.finish();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].input.input, input);
        assert_eq!(
            records[0].cut,
            observe.then_some(InputSourceCut {
                source: watched(),
                tail: Some(input)
            })
        );
    }
}

#[test]
fn cross_class_source_tail_is_fifo_identity_not_the_largest_reserved_input_id() {
    let f = Fixture::new(true, true, false);
    let tail = f
        .runtime
        .reserve_admitted_input_for(
            &f.registration,
            watched(),
            InputClass::Control,
            &f.sources[0].work,
        )
        .unwrap();
    let tail_id = tail.id();
    let head = f
        .runtime
        .reserve_admitted_input_for(
            &f.registration,
            watched(),
            InputClass::Lifecycle,
            &f.sources[0].work,
        )
        .unwrap();
    let head_id = head.id();
    head.fill(1).unwrap();
    tail.fill(2).unwrap();
    f.runtime.poll_one(Tick(0)).unwrap();
    let records = f.records();
    f.finish();
    assert!(
        head_id > tail_id,
        "reservation order differs from actual FIFO fill order"
    );
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].input.input, head_id);
    assert_eq!(records[1].input.input, tail_id);
    assert_eq!(records[0].input.class, InputClass::Lifecycle);
    assert_eq!(records[1].input.class, InputClass::Control);
    for record in records {
        assert_eq!(
            record.cut,
            Some(InputSourceCut {
                source: watched(),
                tail: Some(tail_id)
            })
        );
    }
}

#[test]
fn later_paid_fill_does_not_extend_the_copied_cut_and_next_claim_observes_it() {
    let mut f = Fixture::new(true, false, false);
    let first = f.fill(0);
    f.runtime.poll_one(Tick(0)).unwrap();
    let original_cut = f.records()[0].cut;
    f.runtime.close_actor_admission(actor()).unwrap();
    f.scope.close_external().unwrap();
    let late = f.fill(1);
    let preserved_cut = f.records()[0].cut;
    f.runtime.poll_one(Tick(1)).unwrap();
    let next_cut = f.records()[1].cut;
    f.finish();
    assert_eq!(
        original_cut,
        Some(InputSourceCut {
            source: watched(),
            tail: Some(first)
        })
    );
    assert_eq!(preserved_cut, original_cut);
    assert_eq!(
        next_cut,
        Some(InputSourceCut {
            source: watched(),
            tail: Some(late)
        })
    );
}

#[test]
fn empty_source_cut_excludes_other_sources_and_timer_without_claiming_source_exit() {
    let mut f = Fixture::new(true, true, true);
    let other_input = f.fill(2);
    f.runtime.advance_time(Tick(1), 1).unwrap();
    f.runtime.poll_one(Tick(1)).unwrap();
    let records = f.records();
    let source_still_live = !f.sources[0].work.actual_exit_ready().unwrap();
    let scope_still_live = !f.scope.join_ready().unwrap();
    f.finish();
    assert_eq!(records.len(), 2);
    assert!(
        records
            .iter()
            .any(|record| record.input.input == other_input)
    );
    assert!(records.iter().any(|record| record.input.source == actor() && record.input.class == InputClass::Timer));
    assert!(records.iter().all(|record| record.cut
        == Some(InputSourceCut {
            source: watched(),
            tail: None
        })));
    assert!(source_still_live && scope_still_live);
}

#[test]
fn frozen_tail_and_source_selection_complete_the_original_prefix_before_a_late_input() {
    let mut f = Fixture::with_source_phases(true, true, false, true);
    let phase_input = f.fill(2);
    // Reserve the actual tail first, then fill it second. Only identity
    // equality with the frozen FIFO tail may finish this prefix.
    let tail = f
        .runtime
        .reserve_admitted_input_for(
            &f.registration,
            watched(),
            InputClass::Control,
            &f.sources[0].work,
        )
        .unwrap();
    let tail_id = tail.id();
    let head = f
        .runtime
        .reserve_admitted_input_for(
            &f.registration,
            watched(),
            InputClass::Lifecycle,
            &f.sources[0].work,
        )
        .unwrap();
    let head_id = head.id();
    head.fill(1).unwrap();
    tail.fill(1).unwrap();
    f.runtime.poll_one(Tick(0)).unwrap();
    let first_claim = f.records();
    // The first committed input owns the sole batch. This gives the real
    // producer a later fill before the original tail can be claimed.
    let late = f.fill(0);
    let retire_if_committed = |input| {
        if f.runtime
            .input_custody(actor(), input)
            .unwrap()
            .is_some_and(|record| record.phase == InputPhase::Committed)
        {
            f.runtime.retire_input(actor(), input).unwrap();
        }
    };
    retire_if_committed(head_id);
    f.runtime.poll_one(Tick(1)).unwrap();
    let after_tail = f.records();
    let late_waiting_after_tail = f
        .runtime
        .input_custody(actor(), late)
        .unwrap()
        .is_some_and(|record| record.phase == InputPhase::Filled);
    retire_if_committed(tail_id);
    f.runtime.poll_one(Tick(2)).unwrap();
    let after_phase = f.records();
    let late_waiting_after_phase = f
        .runtime
        .input_custody(actor(), late)
        .unwrap()
        .is_some_and(|record| record.phase == InputPhase::Filled);
    retire_if_committed(phase_input);
    f.runtime.poll_one(Tick(3)).unwrap();
    let measured = f.records();

    // Capture every oracle before cleanup. This unused, already paid Source
    // role provides a genuine restore message if a future regression left a
    // source selected; no setter, skipped input or fictitious consumption is
    // used to unwind the fixture.
    f.roles[1]
        .fill_with_receipt(None, u8::MAX)
        .unwrap_or_else(|failure| panic!("original paid cleanup input: {:?}", failure.error));
    for _ in 0..8 {
        for input in f.runtime.input_inventory(actor(), None, 32).unwrap() {
            if input.phase == InputPhase::Committed {
                f.runtime.retire_input(actor(), input.input).unwrap();
            }
        }
        f.runtime.poll_one(Tick(4)).unwrap();
    }
    f.finish();

    assert!(head_id > tail_id);
    assert_eq!(first_claim.len(), 1);
    assert_eq!(first_claim[0].input.input, head_id);
    assert_eq!(
        first_claim[0].cut,
        Some(InputSourceCut {
            source: watched(),
            tail: Some(tail_id)
        })
    );
    assert_eq!(
        after_tail
            .iter()
            .map(|record| record.input.input)
            .collect::<Vec<_>>(),
        [head_id, tail_id]
    );
    assert_eq!(after_tail[1].input.class, InputClass::Control);
    assert_eq!(
        after_tail[1].cut,
        Some(InputSourceCut {
            source: watched(),
            tail: Some(late)
        })
    );
    assert_eq!(first_claim[0].selected, None);
    assert_eq!(after_tail[1].selected, after_tail[1].cut);
    assert_eq!(
        after_phase[2].selected,
        Some(InputSourceCut {
            source: other(),
            tail: Some(phase_input)
        })
    );
    assert_eq!(
        after_phase[2].cut,
        Some(InputSourceCut {
            source: watched(),
            tail: Some(late)
        })
    );
    assert!(late_waiting_after_tail && late_waiting_after_phase);
    assert_eq!(
        after_phase
            .iter()
            .map(|record| record.input.input)
            .collect::<Vec<_>>(),
        [head_id, tail_id, phase_input]
    );
    assert_eq!(
        measured
            .iter()
            .map(|record| record.input.input)
            .collect::<Vec<_>>(),
        [head_id, tail_id, phase_input, late]
    );
}
