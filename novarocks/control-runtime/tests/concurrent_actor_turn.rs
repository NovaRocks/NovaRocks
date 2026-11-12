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

#![expect(
    clippy::disallowed_methods,
    reason = "Integration tests own and join their threads and runtime tasks; see ADR-0218."
)]
#![cfg(not(loom))]

use novarocks_control_runtime::{
    actor::{Actor, ActorConfig, ActorStep},
    actor_turn::{ActorRuntime, ActorTurnError, PollerPool, ProcessClock},
    resource::ResourceStore,
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{ReservationRequest, ReservationResources, ResourceVector},
    effect::{EffectDescriptor, EffectKind},
    identity::*,
    mailbox::{InputClass, InputEnvelope},
    owned_storage::OwnedSlotNeed,
    scheduler::SchedulerError,
    scope::{WorkAdmission, WorkKind},
};
use std::sync::{Arc, Condvar, Mutex, mpsc};
fn aid(n: u64) -> ActorId {
    ActorId::new(n).unwrap()
}
fn sid(n: u64) -> ScopeId {
    ScopeId::new(n).unwrap()
}
fn slot(n: u64) -> SlotId {
    SlotId::new(n).unwrap()
}
fn generation() -> Generation {
    Generation::new(1).unwrap()
}
fn vector(n: u64) -> ResourceVector {
    ResourceVector::new([(DimensionId::new(1).unwrap(), n)]).unwrap()
}
fn config() -> ActorConfig {
    ActorConfig {
        mailbox: [64; 4],
        max_batches: 64,
        max_effects_per_step: 2,
        max_dependencies_per_effect: 0,
        turn_budget: 16,
        max_step_work: 2,
    }
}
#[derive(Debug)]
struct Counter {
    seen: Vec<u64>,
    effect: bool,
    author: Option<ScopedWorkRef>,
}
impl Actor for Counter {
    type Input = u64;
    type Effect = u64;
    fn timer_input(now: Tick) -> u64 {
        now.0
    }
    fn handle(
        &mut self,
        _now: Tick,
        _input: InputEnvelope,
        value: u64,
        step: &mut ActorStep<'_, u64>,
    ) {
        self.seen.push(value);
        if self.effect {
            step.effect(
                EffectDescriptor {
                    id: ActionId::new(1).unwrap(),
                    work: step
                        .input_work()
                        .or(self.author)
                        .expect("original admitted Counter Business author"),
                    kind: EffectKind::Business,
                    dependencies: vec![],
                },
                value,
            )
            .unwrap();
        }
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(
        _scope: ScopeId,
        _generation: Generation,
        _input: InputEnvelope,
        _step: &mut ActorStep<'_, u64>,
    ) {
    }
}
fn wait_batches(runtime: &ActorRuntime<Counter>, actor: ActorId, count: usize) {
    loop {
        let version = runtime.change_version();
        if runtime.snapshot(actor).unwrap().batches == count {
            return;
        }
        runtime.wait_after(version);
    }
}
fn remove_after_turn(
    runtime: &ActorRuntime<Counter>,
    scope: &ScopeHandle,
    actor: ActorId,
) -> Counter {
    scope.close_external().unwrap();
    loop {
        let version = runtime.change_version();
        while let Some(original_host_turn) = runtime.claim_host_actor() {
            original_host_turn.release(false).unwrap();
        }
        match runtime.remove_actor(actor) {
            Ok(core) => return core,
            Err(ActorTurnError::Scheduler(SchedulerError::Busy)) => runtime.wait_after(version),
            Err(error) => panic!("cannot retire actor: {error}"),
        }
    }
}
fn pool_case(workers: usize) {
    let ledger = RuntimeLedger::new(vector(8));
    let runtime = ActorRuntime::new();
    let mut scopes = vec![];
    for id in 1..=4 {
        let scope = ledger.open_scope(sid(id), None, generation()).unwrap();
        runtime
            .register(
                aid(id),
                scope.clone(),
                slot(id),
                Counter {
                    seen: vec![],
                    effect: false,
                    author: None,
                },
                config(),
            )
            .unwrap();
        scopes.push(scope);
    }
    let pool =
        PollerPool::start(runtime.clone(), Arc::new(ProcessClock::default()), workers).unwrap();
    let mut writers = vec![];
    for actor in 1..=4 {
        let runtime = runtime.clone();
        writers.push(std::thread::spawn(move || {
            let mut inputs = vec![];
            for value in 0..40 {
                inputs.push(
                    runtime
                        .reserve_input(aid(actor), aid(actor + 100), InputClass::Ordinary)
                        .unwrap()
                        .fill(value)
                        .unwrap(),
                );
            }
            inputs
        }));
    }
    for (index, writer) in writers.into_iter().enumerate() {
        let actor = aid(index as u64 + 1);
        let inputs = writer.join().unwrap();
        wait_batches(&runtime, actor, 40);
        for input in inputs {
            runtime.retire_input(actor, input).unwrap();
        }
        let core = remove_after_turn(&runtime, &scopes[index], actor);
        assert_eq!(core.seen, (0..40).collect::<Vec<_>>());
        ledger.remove_scope(sid(index as u64 + 1)).unwrap();
    }
    pool.shutdown()
        .unwrap_or_else(|_| panic!("all poller actors must drain"));
    assert!(ledger.is_empty());
}
#[test]
fn two_pollers_preserve_exact_actor_and_pair_fifo() {
    pool_case(2);
}
#[test]
fn four_pollers_preserve_exact_actor_and_pair_fifo() {
    pool_case(4);
}

struct SlowDrop {
    entered: mpsc::Sender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}
impl Drop for SlowDrop {
    fn drop(&mut self) {
        self.entered.send(()).unwrap();
        let (lock, gate) = &*self.release;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = gate.wait(released).unwrap();
        }
    }
}
#[test]
fn slow_host_destructor_keeps_its_work_but_does_not_occupy_the_only_poller() {
    let ledger = RuntimeLedger::new(vector(8));
    let runtime = ActorRuntime::new();
    let first = ledger.open_scope(sid(1), None, generation()).unwrap();
    let second = ledger.open_scope(sid(2), None, generation()).unwrap();
    runtime
        .register_next(
            aid(2),
            second.clone(),
            Counter {
                seen: vec![],
                effect: false,
                author: None,
            },
            config(),
        )
        .unwrap();
    let (mut work, mut bundle) = first
        .reserve(ReservationRequest {
            id: ReservationId::new(1).unwrap(),
            generation: generation(),
            guard: WorkId::new(1).unwrap(),
            resources: ReservationResources {
                keyed: None,
                quota: vector(1),
                owned_slots: OwnedSlotNeed {
                    work_headers: 1,
                    ..OwnedSlotNeed::default()
                },
            },
            admission: WorkAdmission::External,
            kind: WorkKind::Business,
        })
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    work.start().unwrap();
    runtime
        .register_next(
            aid(1),
            first.clone(),
            Counter {
                seen: vec![],
                effect: true,
                author: Some(work.reference()),
            },
            config(),
        )
        .unwrap();

    let (entered_tx, entered_rx) = mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let resources = ResourceStore::new(1);
    let resource = resources
        .insert(
            slot(99),
            sid(1),
            generation(),
            SlowDrop {
                entered: entered_tx,
                release: release.clone(),
            },
        )
        .unwrap_or_else(|_| panic!("reserved host resource"));
    let pool = PollerPool::start(runtime.clone(), Arc::new(ProcessClock::default()), 1).unwrap();
    let input1 = runtime
        .reserve_input(aid(1), aid(100), InputClass::Ordinary)
        .unwrap()
        .fill(99)
        .unwrap();
    wait_batches(&runtime, aid(1), 1);
    let ticket = runtime
        .take_effect(aid(1), input1, ActionId::new(1).unwrap())
        .unwrap();
    let host_runtime = runtime.clone();
    let host = std::thread::spawn(move || {
        drop(resource.take().unwrap());
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        bundle.release_quota(&vector(1)).unwrap();
        bundle.finish().unwrap();
        host_runtime.effect_completed(ticket).unwrap();
    });
    entered_rx.recv().unwrap();
    assert_eq!(first.snapshot().unwrap().work, 1);
    assert_eq!(ledger.used(DimensionId::new(1).unwrap()), 1);
    let input2 = runtime
        .reserve_input(aid(2), aid(100), InputClass::Ordinary)
        .unwrap()
        .fill(42)
        .unwrap();
    wait_batches(&runtime, aid(2), 1);
    {
        let (lock, gate) = &*release;
        *lock.lock().unwrap() = true;
        gate.notify_all();
    }
    host.join().unwrap();
    ledger.reap_ready(8).unwrap();
    runtime.retire_input(aid(1), input1).unwrap();
    runtime.retire_input(aid(2), input2).unwrap();
    assert_eq!(remove_after_turn(&runtime, &first, aid(1)).seen, vec![99]);
    assert_eq!(remove_after_turn(&runtime, &second, aid(2)).seen, vec![42]);
    ledger.remove_scope(sid(1)).unwrap();
    ledger.remove_scope(sid(2)).unwrap();
    pool.shutdown()
        .unwrap_or_else(|_| panic!("all actors drain"));
    assert!(resources.is_empty());
}

#[test]
fn concurrent_production_admission_mints_and_binds_ids_in_one_transaction() {
    use novarocks_control_runtime::completion::ReportSlot;
    let ledger = RuntimeLedger::new(vector(32));
    let scope = ledger.open_scope_next(None, generation()).unwrap();
    let runtime = ActorRuntime::new();
    let barrier = Arc::new(std::sync::Barrier::new(4));
    let mut threads = vec![];
    for thread_index in 0..4 {
        let scope = scope.clone();
        let runtime = runtime.clone();
        let barrier = barrier.clone();
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            let mut admitted = vec![];
            for local in 0..8 {
                let (work, mut bundle) = scope
                    .reserve_next(
                        generation(),
                        ReservationResources {
                            keyed: None,
                            quota: vector(1),
                            owned_slots: OwnedSlotNeed {
                                work_headers: 1,
                                completions: 1,
                                ..OwnedSlotNeed::default()
                            },
                        },
                        WorkKind::Business,
                        WorkAdmission::External,
                    )
                    .unwrap()
                    .bind(WorkKind::Business)
                    .unwrap();
                let report = ReportSlot::reserve_next(&mut bundle, generation(), 0, []).unwrap();
                let actor = aid(thread_index * 8 + local + 1);
                let attachment = runtime
                    .register_next(
                        actor,
                        scope.clone(),
                        Counter {
                            seen: vec![],
                            effect: false,
                            author: None,
                        },
                        config(),
                    )
                    .unwrap();
                admitted.push((work, bundle, report, actor, attachment));
            }
            admitted
        }));
    }
    let admitted: Vec<_> = threads
        .into_iter()
        .flat_map(|t| t.join().unwrap())
        .collect();
    let work_ids: std::collections::BTreeSet<_> = admitted.iter().map(|v| v.0.id()).collect();
    let reservation_ids: std::collections::BTreeSet<_> =
        admitted.iter().map(|v| v.1.id()).collect();
    let slot_ids: std::collections::BTreeSet<_> =
        admitted.iter().flat_map(|v| [v.2.id(), v.4]).collect();
    assert_eq!(work_ids.len(), 32);
    assert_eq!(reservation_ids.len(), 32);
    assert_eq!(slot_ids.len(), 64);
    assert_eq!(scope.snapshot().unwrap().storage, 64);
    assert_eq!(ledger.used(DimensionId::new(1).unwrap()), 32);
    let mut actors = vec![];
    for (mut work, mut bundle, report, actor, _) in admitted {
        report.revoke_unused().unwrap();
        report.body_exited().unwrap();
        report.captures_exited().unwrap();
        assert!(report.try_retire().unwrap());
        work.body_exited().unwrap();
        work.captures_exited().unwrap();
        bundle.release_quota(&vector(1)).unwrap();
        bundle.finish().unwrap();
        actors.push(actor);
    }
    assert_eq!(ledger.reap_ready(32).unwrap(), 32);
    scope.close_external().unwrap();
    for actor in actors {
        runtime.remove_actor(actor).unwrap();
    }
    assert!(scope.snapshot().unwrap().retirement_ready);
    ledger.remove_scope(scope.id()).unwrap();
    assert!(ledger.is_empty());
}

#[test]
fn concurrent_child_scope_creation_mints_parent_work_inside_the_scope_transaction() {
    use novarocks_control_runtime::transaction::ScopeParentAdmission;
    let ledger = RuntimeLedger::new(vector(1));
    let parent = ledger.open_scope_next(None, generation()).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(4));
    let mut threads = vec![];
    for _ in 0..4 {
        let ledger = ledger.clone();
        let parent_id = parent.id();
        let barrier = barrier.clone();
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            (0..4)
                .map(|_| {
                    ledger
                        .open_scope_next(
                            Some(ScopeParentAdmission {
                                scope: parent_id,
                                generation: generation(),
                                kind: WorkKind::Business,
                                admission: WorkAdmission::External,
                            }),
                            generation(),
                        )
                        .unwrap()
                })
                .collect::<Vec<_>>()
        }));
    }
    let children: Vec<_> = threads
        .into_iter()
        .flat_map(|t| t.join().unwrap())
        .collect();
    assert_eq!(
        children
            .iter()
            .map(ScopeHandle::id)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        16
    );
    assert_eq!(parent.snapshot().unwrap().work, 16);
    for (removed, child) in children.into_iter().enumerate() {
        child.close_external().unwrap();
        ledger.remove_scope(child.id()).unwrap();
        assert_eq!(parent.snapshot().unwrap().work, 16 - removed - 1);
    }
    assert_eq!(ledger.reap_ready(16).unwrap(), 0);
    parent.close_external().unwrap();
    ledger.remove_scope(parent.id()).unwrap();
    assert!(ledger.is_empty());
}
