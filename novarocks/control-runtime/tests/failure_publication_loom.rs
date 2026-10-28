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

#![cfg(loom)]

//! A two-preemption model of the actual cleanup-body/publication gate.
use loom::{sync::Arc, thread};
use novarocks_control_runtime::{
    actor::{Actor, ActorConfig, StepContext},
    driver::{ActorRuntime, DriverError},
    publication::PublishedSnapshot,
    transaction::RuntimeLedger,
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    effect::{EffectDescriptor, EffectKind},
    identity::*,
    mailbox::{InputClass, InputEnvelope},
    scope::{WorkAdmission, WorkKind},
};
#[derive(Debug)]
struct Failing;
impl Actor for Failing {
    type Input = ();
    type Effect = ();
    fn timer_input(_now: Tick) {}
    fn handle(
        &mut self,
        _now: Tick,
        _input: InputEnvelope,
        _value: (),
        _step: &mut StepContext<'_, ()>,
    ) {
        panic!("controlled failure");
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(
        _scope: ScopeId,
        _generation: Generation,
        _input: InputEnvelope,
        step: &mut StepContext<'_, ()>,
    ) {
        step.effect(
            EffectDescriptor {
                id: ActionId::new(7).unwrap(),
                work: WorkId::new(1).unwrap(),
                kind: EffectKind::Cleanup,
                dependencies: vec![],
            },
            (),
        )
        .unwrap();
    }
}
#[test]
fn actual_cleanup_body_exit_and_actual_snapshot_swap_have_one_gate_order() {
    let mut model = loom::model::Builder::new();
    model.preemption_bound = Some(2);
    model.max_branches = 20_000;
    model.check(|| {
        let generation = Generation::new(1).unwrap();
        let actor = ActorId::new(1).unwrap();
        let resources = ResourceVector::new([(DimensionId::new(1).unwrap(), 1)]).unwrap();
        let ledger = RuntimeLedger::new(resources.clone(), 1, 1);
        let scope = ledger.open_scope_next(None, generation, 1, 1).unwrap();
        let (mut work, mut capacity) = scope
            .reserve_next(
                generation,
                resources.clone(),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        work.start().unwrap();
        let runtime = ActorRuntime::new(1);
        runtime
            .register_next(
                actor,
                scope.clone(),
                Failing,
                ActorConfig {
                    mailbox: [1; 4],
                    max_batches: 1,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
            )
            .unwrap();
        let input = runtime
            .reserve_input(actor, actor, InputClass::Ordinary)
            .unwrap()
            .fill(())
            .unwrap();
        runtime.poll_one(Tick(1)).unwrap();
        let ticket = runtime
            .take_effect(actor, input, ActionId::new(7).unwrap())
            .unwrap();
        let mut session = runtime.claim_failure_publication(&ticket).unwrap();
        let target = PublishedSnapshot::new(&scope, Arc::new(0));
        let published = target.clone();
        let publish = thread::spawn(move || session.commit_snapshot(&published, Arc::new(1)));
        let exit = thread::spawn(move || {
            work.body_exited().unwrap();
            work.captures_exited().unwrap();
            capacity.release(&resources).unwrap();
        });
        let publication = publish.join().unwrap();
        exit.join().unwrap();
        match publication {
            Ok(old) => {
                assert_eq!(*old, 0);
                assert_eq!(*target.read(), 1);
            }
            Err((DriverError::FailureWorkInactive, rejected)) => {
                assert_eq!(*rejected, 1);
                assert_eq!(*target.read(), 0);
            }
            other => panic!("unexpected gate result: {other:?}"),
        }
        runtime.effect_completed(ticket).unwrap();
        runtime.retire_input(actor, input).unwrap();
        ledger.reap_ready(1).unwrap();
        drop(runtime.take_failure_payload(actor).unwrap());
        while let Some(original_host_turn) = runtime.claim_host_actor() {
            original_host_turn.release(false).unwrap();
        }
        runtime.remove_actor(actor).unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    });
}
