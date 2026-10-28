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
    driver::{ActorHostTurn, ActorRegistration, ActorRuntime, Clock, DriverError, PollerPool},
    lease::{CapacityBundle, WorkLease},
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    identity::{ActorId, DimensionId, Generation, InputId, ScopeId, Tick},
    mailbox::{InputClass, InputEnvelope},
    scope::{WorkAdmission, WorkKind},
};
use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender},
    },
    time::{Duration, Instant},
};

const WAIT: Duration = Duration::from_secs(5);
const INPUTS: usize = 128;

#[derive(Debug)]
struct OriginalActor {
    calls: Arc<AtomicUsize>,
    target: usize,
    completed: Option<Sender<()>>,
}
impl Actor for OriginalActor {
    type Input = usize;
    type Effect = ();
    fn timer_input(_: Tick) -> usize {
        unreachable!("these actors have no deadline")
    }
    fn handle(&mut self, _: Tick, _: InputEnvelope, value: usize, _: &mut StepContext<'_, ()>) {
        let completed = self.calls.fetch_add(value, Ordering::SeqCst) + value;
        if completed == self.target
            && let Some(notice) = self.completed.take()
        {
            let _ = notice.send(());
        }
    }
    fn next_deadline(&self) -> Option<Tick> {
        None
    }
    fn owner_failed(_: ScopeId, _: Generation, _: InputEnvelope, _: &mut StepContext<'_, ()>) {
        unreachable!("the original actor does not panic")
    }
}

struct ProbeClock {
    observe: AtomicBool,
    samples: SyncSender<()>,
}
impl ProbeClock {
    fn new() -> (Arc<Self>, Receiver<()>) {
        let (samples, receiver) = mpsc::sync_channel(1);
        (
            Arc::new(Self {
                observe: AtomicBool::new(false),
                samples,
            }),
            receiver,
        )
    }
}
impl Clock for ProbeClock {
    fn now(&self) -> Tick {
        if self.observe.load(Ordering::SeqCst) {
            // One bounded I/O probe, never a blocking callback or an
            // accumulating notification queue inside the poller.
            let _ = self.samples.try_send(());
        }
        Tick(1)
    }
}
fn dimension() -> DimensionId {
    DimensionId::new(1).unwrap()
}
fn paid(amount: u64) -> ResourceVector {
    ResourceVector::new([(dimension(), amount)]).unwrap()
}
fn release(turn: ActorHostTurn<OriginalActor>, failed: &AtomicBool) {
    if let Err((_error, original)) = turn.release(false) {
        failed.store(true, Ordering::SeqCst);
        // The original guard's Drop only requeues metadata. No Work exit is
        // inferred if an unexpected release refusal reaches this test probe.
        drop(original);
    }
}
struct Fixture {
    ledger: RuntimeLedger,
    runtime: ActorRuntime<OriginalActor>,
    scope: ScopeHandle,
    registration: ActorRegistration,
    work: WorkLease,
    capacity: CapacityBundle,
    inputs: Vec<InputId>,
    calls: Arc<AtomicUsize>,
    retired: bool,
}
impl Fixture {
    fn new(
        ledger: RuntimeLedger,
        runtime: ActorRuntime<OriginalActor>,
        actor: ActorId,
        target: usize,
        completed: Option<Sender<()>>,
    ) -> Self {
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation, 4, 4).unwrap();
        let (mut work, capacity) = scope
            .reserve_next(
                generation,
                paid(1),
                WorkKind::Cleanup,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Cleanup)
            .unwrap();
        work.start().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let registration = runtime
            .register_supervised_bound(
                actor,
                scope.clone(),
                OriginalActor {
                    calls: calls.clone(),
                    target,
                    completed,
                },
                ActorConfig {
                    mailbox: [INPUTS; 4],
                    max_batches: INPUTS,
                    max_effects_per_step: 1,
                    max_dependencies_per_effect: 0,
                    turn_budget: 8,
                    max_step_work: 1,
                },
                &work,
            )
            .unwrap();
        Self {
            ledger,
            runtime,
            scope,
            registration,
            work,
            capacity,
            inputs: Vec::new(),
            calls,
            retired: false,
        }
    }
    fn submit_finite_chain(&mut self) {
        for _ in 0..INPUTS {
            self.inputs.push(
                self.runtime
                    .reserve_input(
                        self.registration.actor(),
                        self.registration.actor(),
                        InputClass::Ordinary,
                    )
                    .unwrap()
                    .fill(1)
                    .unwrap(),
            );
        }
    }
    fn retire_committed_inputs(&mut self) -> bool {
        let mut valid = true;
        self.inputs.retain(|input| {
            match self.runtime.retire_input(self.registration.actor(), *input) {
                Ok(()) => false,
                Err(DriverError::InputStillQueued) => true,
                Err(_) => {
                    valid = false;
                    true
                }
            }
        });
        valid
    }
}

/// The candidate and the starvation negative both have finite real inputs.
/// Release every external gate before calling this cleanup; advance the same
/// original Core/input/Work custody, retire actors, then join the actual pool.
fn finish(pool: PollerPool<OriginalActor>, mut fixtures: Vec<Fixture>) {
    let runtime = fixtures[0].runtime.clone();
    for fixture in &mut fixtures {
        fixture.scope.close_external().unwrap();
        fixture.work.body_exited().unwrap();
        fixture.work.captures_exited().unwrap();
        fixture
            .ledger
            .reap_scope_ready(fixture.scope.id(), 1)
            .unwrap();
    }
    let deadline = Instant::now() + WAIT * 2;
    let mut healthy_cleanup = true;
    while fixtures.iter().any(|fixture| !fixture.retired) && Instant::now() < deadline {
        let version = runtime.change_version();
        healthy_cleanup &= runtime.poll_one(Tick(1)).is_ok();
        for fixture in fixtures.iter_mut().filter(|fixture| !fixture.retired) {
            healthy_cleanup &= fixture.retire_committed_inputs();
            healthy_cleanup &= runtime.notify_host_actor(&fixture.registration).is_ok();
        }
        if let Some(turn) = runtime.claim_host_actor() {
            let index = fixtures
                .iter()
                .position(|fixture| turn.matches_registration(&fixture.registration));
            if let Some(index) = index {
                match turn.retire_actor() {
                    Ok(actor) => {
                        drop(actor);
                        fixtures[index].retired = true;
                    }
                    Err((_error, original)) => {
                        let failed = AtomicBool::new(false);
                        release(original, &failed);
                        healthy_cleanup &= !failed.load(Ordering::SeqCst);
                    }
                }
            } else {
                let failed = AtomicBool::new(false);
                release(turn, &failed);
                healthy_cleanup = false;
            }
        }
        if fixtures.iter().any(|fixture| !fixture.retired) {
            runtime.wait_after_timeout(version, Duration::from_millis(1));
            std::thread::yield_now();
        }
    }
    if fixtures.iter().any(|fixture| !fixture.retired) {
        // A cleanup regression must report its first failure, not unwind into
        // PollerPool::drop's second panic. Preserve this finite original pool
        // and custody only on that failing path; this is never successful exit.
        std::mem::forget((pool, fixtures));
        panic!("the original poller/input/actor inventory did not drain before cleanup deadline");
    }
    let joined = pool.shutdown();
    if let Err(original) = joined {
        std::mem::forget((original, fixtures));
        panic!("the original actor runtime was not empty at poller shutdown");
    }
    for mut fixture in fixtures {
        fixture
            .capacity
            .release(&fixture.capacity.resources().unwrap())
            .unwrap();
        fixture.ledger.remove_scope(fixture.scope.id()).unwrap();
    }
    assert!(healthy_cleanup);
}

#[test]
fn one_poller_services_host_during_a_finite_core_chain_and_wakes_again_from_idle() {
    let ledger = RuntimeLedger::new(paid(1), 4, 4);
    let runtime = ActorRuntime::new(1);
    let (all_core, all_core_rx) = mpsc::channel();
    let mut fixture = Fixture::new(
        ledger.clone(),
        runtime.clone(),
        ActorId::new(1).unwrap(),
        INPUTS,
        Some(all_core),
    );
    fixture.submit_finite_chain();
    runtime.notify_host_actor(&fixture.registration).unwrap();
    let (clock, clock_rx) = ProbeClock::new();
    let (first_host, first_host_rx) = mpsc::channel();
    let (later_host, later_host_rx) = mpsc::channel();
    let first = Arc::new(AtomicBool::new(true));
    let request_later_host = Arc::new(AtomicBool::new(false));
    let release_failed = Arc::new(AtomicBool::new(false));
    let calls = fixture.calls.clone();
    let worker_first = first.clone();
    let worker_later = request_later_host.clone();
    let worker_failed = release_failed.clone();
    let pool = PollerPool::start_with_host(runtime.clone(), clock.clone(), 1, move |_, turn| {
        if worker_first.swap(false, Ordering::SeqCst) {
            let _ = first_host.send(calls.load(Ordering::SeqCst));
        }
        if worker_later.swap(false, Ordering::SeqCst) {
            let _ = later_host.send(());
        }
        release(turn, &worker_failed);
    })
    .unwrap();
    let first_count = first_host_rx.recv_timeout(WAIT).ok();
    let core_chain_finished = all_core_rx.recv_timeout(WAIT).is_ok();
    let deadline = Instant::now() + WAIT;
    let mut input_cleanup_valid = true;
    while !fixture.inputs.is_empty() && Instant::now() < deadline {
        input_cleanup_valid &= fixture.retire_committed_inputs();
        if !fixture.inputs.is_empty() {
            let version = runtime.change_version();
            runtime.wait_after_timeout(version, Duration::from_millis(1));
        }
    }
    let original_inputs_retired = fixture.inputs.is_empty();
    // Observe a real loop boundary and then a quiet interval with no clock
    // samples before notifying. This checks the idle/wait wake path, not a
    // physical Task exit inferred from elapsed time.
    clock.observe.store(true, Ordering::SeqCst);
    runtime.notify_pollers();
    let idle_boundary = clock_rx.recv_timeout(WAIT).is_ok();
    let deadline = Instant::now() + WAIT;
    let mut idle_quiet = false;
    while idle_boundary && Instant::now() < deadline {
        match clock_rx.recv_timeout(Duration::from_millis(20)) {
            Err(RecvTimeoutError::Timeout) => {
                idle_quiet = true;
                break;
            }
            Ok(()) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    clock.observe.store(false, Ordering::SeqCst);
    request_later_host.store(true, Ordering::SeqCst);
    let external_mark = runtime.notify_host_actor(&fixture.registration).is_ok();
    let external_callback = later_host_rx.recv_timeout(WAIT).is_ok();
    let final_count = fixture.calls.load(Ordering::SeqCst);
    finish(pool, vec![fixture]);
    assert!(first_count.is_some_and(|count| count < INPUTS));
    assert!(core_chain_finished && input_cleanup_valid && original_inputs_retired);
    assert!(idle_boundary && idle_quiet && external_mark && external_callback);
    assert_eq!(final_count, INPUTS);
    assert!(!release_failed.load(Ordering::SeqCst));
    assert!(runtime.is_empty() && ledger.is_empty());
}

#[derive(Default)]
struct ReleaseGate {
    open: Mutex<bool>,
    changed: Condvar,
}
impl ReleaseGate {
    fn wait(&self) -> bool {
        let deadline = Instant::now() + WAIT * 2;
        let mut open = self.open.lock().unwrap();
        while !*open {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            open = self.changed.wait_timeout(open, remaining).unwrap().0;
        }
        true
    }
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

#[test]
fn two_pollers_keep_one_original_host_turn_exclusive_while_another_actor_progresses() {
    let ledger = RuntimeLedger::new(paid(2), 8, 8);
    let runtime = ActorRuntime::new(2);
    let first = Fixture::new(
        ledger.clone(),
        runtime.clone(),
        ActorId::new(1).unwrap(),
        0,
        None,
    );
    let second = Fixture::new(
        ledger.clone(),
        runtime.clone(),
        ActorId::new(2).unwrap(),
        0,
        None,
    );
    runtime.notify_host_actor(&first.registration).unwrap();
    let (entered, entered_rx) = mpsc::channel();
    let (other, other_rx) = mpsc::channel();
    let (returned, returned_rx) = mpsc::channel();
    let gate = Arc::new(ReleaseGate::default());
    let entries = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let gate_expired = Arc::new(AtomicBool::new(false));
    let release_failed = Arc::new(AtomicBool::new(false));
    let first_registration = first.registration.clone();
    let other_sent = AtomicBool::new(false);
    let worker_gate = gate.clone();
    let worker_entries = entries.clone();
    let worker_active = active.clone();
    let worker_maximum = maximum.clone();
    let worker_expired = gate_expired.clone();
    let worker_failed = release_failed.clone();
    let (clock, _clock_rx) = ProbeClock::new();
    let pool = PollerPool::start_with_host(runtime.clone(), clock, 2, move |_, turn| {
        if turn.matches_registration(&first_registration) {
            let sequence = worker_entries.fetch_add(1, Ordering::SeqCst);
            let simultaneous = worker_active.fetch_add(1, Ordering::SeqCst) + 1;
            worker_maximum.fetch_max(simultaneous, Ordering::SeqCst);
            if sequence == 0 {
                let _ = entered.send(());
                if !worker_gate.wait() {
                    worker_expired.store(true, Ordering::SeqCst);
                }
            }
            worker_active.fetch_sub(1, Ordering::SeqCst);
            release(turn, &worker_failed);
            if sequence == 0 {
                let _ = returned.send(());
            }
        } else {
            release(turn, &worker_failed);
            if !other_sent.swap(true, Ordering::SeqCst) {
                let _ = other.send(());
            }
        }
    })
    .unwrap();
    let first_entered = entered_rx.recv_timeout(WAIT).is_ok();
    let mut marks_valid = true;
    for _ in 0..8 {
        marks_valid &= runtime.notify_host_actor(&first.registration).is_ok();
    }
    marks_valid &= runtime.notify_host_actor(&second.registration).is_ok();
    let second_progressed = other_rx.recv_timeout(WAIT).is_ok();
    let first_still_exclusive = entries.load(Ordering::SeqCst) == 1
        && active.load(Ordering::SeqCst) == 1
        && maximum.load(Ordering::SeqCst) == 1;
    // Always open the gate before checking any channel or concurrency oracle;
    // it remains releasable even if the first callback never reached it.
    gate.release();
    let first_returned = returned_rx.recv_timeout(WAIT).is_ok();
    finish(pool, vec![first, second]);
    assert!(first_entered && marks_valid && second_progressed);
    assert!(first_still_exclusive && first_returned);
    assert_eq!(maximum.load(Ordering::SeqCst), 1);
    assert!(!gate_expired.load(Ordering::SeqCst));
    assert!(!release_failed.load(Ordering::SeqCst));
    assert!(runtime.is_empty() && ledger.is_empty());
}
