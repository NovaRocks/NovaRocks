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

//! Purpose-loan routing through the actual host and runtime. Captured oracles
//! are asserted only after genuine metadata/causal/report/header teardown.
use super::*;
use crate::control_runtime::actor::{FrontendInput, TestActor};
use novarocks_control_runtime::{
    actor::ActorStep,
    actor_turn::Clock,
    completion::ReportSlot,
    exit_custody::ExitCustody,
    obligation::{ReservationObligation, WorkObligation},
    reported_work::ReportedWork,
};
use novarocks_control_runtime_core::{
    capacity::{ReservationResources, ResourceVector},
    completion::ConsumerKind,
    effect::{EffectDescriptor, EffectKind, EffectState},
    identity::{ActionId, ActorId, ConsumerId, Generation, ScopedWorkRef},
    mailbox::{InputClass, InputEnvelope},
    owned_storage::OwnedSlotNeed,
    scope::{WorkAdmission, WorkKind},
};
use std::collections::BTreeSet;

const CLEANUP_FENCE: u64 = u64::MAX;
#[derive(Clone)]
struct Planned {
    effect: FrontendEffect,
    dependencies: Vec<u64>,
    work: ScopedWorkRef,
}
struct Emitter {
    batches: Vec<Vec<Planned>>,
}
impl TestActor for Emitter {
    fn handle(
        &mut self,
        _: Tick,
        _: InputEnvelope,
        input: FrontendInput,
        step: &mut ActorStep<'_, FrontendEffect>,
    ) {
        let FrontendInput::Test(batch) = input else {
            return;
        };
        if batch == CLEANUP_FENCE {
            panic!("purpose fixture owner-failure cleanup fence");
        }
        for (index, planned) in self.batches[batch as usize].iter().enumerate() {
            step.effect(
                EffectDescriptor {
                    id: ActionId::new(index as u64 + 1).unwrap(),
                    work: planned.work,
                    kind: match &planned.effect {
                        FrontendEffect::Attempt(effect)
                            if matches!(
                                effect.protocol.body,
                                novarocks_attempt_protocol::effect::EffectBody::Withdraw { .. }
                            ) =>
                        {
                            EffectKind::Cleanup
                        }
                        _ => EffectKind::Business,
                    },
                    dependencies: planned
                        .dependencies
                        .iter()
                        .map(|id| ActionId::new(*id).unwrap())
                        .collect(),
                },
                planned.effect.clone(),
            )
            .unwrap();
        }
    }
}
struct FixedClock;
impl Clock for FixedClock {
    fn now(&self) -> Tick {
        Tick(0)
    }
}
struct Capture(Arc<std::sync::atomic::AtomicUsize>);
impl Drop for Capture {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct Original {
    custody: ExitCustody,
    exit: Option<ExitObligation>,
    role: Option<CausalInputObligation<FrontendActor>>,
    capture: Option<Box<dyn Any + Send>>,
    reference: ScopedWorkRef,
}
#[derive(Clone, Default, Debug)]
struct Observed {
    visits: Vec<u64>,
    borrowed: Vec<u64>,
    returned: usize,
    bound: Vec<u64>,
    origins: Vec<bool>,
    availability_changes: usize,
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum FailurePoint {
    Retirement,
    CreditProbe,
}
struct FailureHook {
    point: FailurePoint,
    runtime: ActorRuntime<FrontendActor>,
    registration: ActorRegistration,
}
impl FailureHook {
    fn fire(self) {
        self.runtime
            .reserve_input(
                self.registration.actor(),
                self.registration.actor(),
                InputClass::Lifecycle,
            )
            .unwrap()
            .fill(FrontendInput::Test(CLEANUP_FENCE))
            .unwrap();
        self.runtime.poll_one(Tick(2)).unwrap();
        assert!(
            self.runtime
                .snapshot(self.registration.actor())
                .unwrap()
                .failed
        );
    }
}
struct Bank {
    originals: BTreeMap<u64, Original>,
    held: BTreeMap<u64, CausalInputObligation<FrontendActor>>,
    observed: Observed,
    changed: bool,
    closed: bool,
    failed: Option<PendingHandoff>,
    failure_hook: Option<FailureHook>,
}
fn take_failure_hook(bank: &mut Bank, point: FailurePoint) -> Option<FailureHook> {
    if bank
        .failure_hook
        .as_ref()
        .is_some_and(|hook| hook.point == point)
    {
        bank.failure_hook.take()
    } else {
        None
    }
}
struct PurposeSource(Arc<Mutex<Bank>>);
fn value(effect: &FrontendEffect) -> u64 {
    match effect {
        FrontendEffect::Test { value } => *value,
        FrontendEffect::Attempt(effect) => effect.protocol.id.get(),
        _ => panic!("unexpected effect entered the test Source"),
    }
}
impl EffectSource for PurposeSource {
    fn prepay_result(
        &mut self,
        effect: &FrontendEffect,
    ) -> Option<CausalInputObligation<FrontendActor>> {
        let mut bank = self.0.lock().unwrap();
        let value = value(effect);
        bank.observed.visits.push(value);
        let role = bank.originals.get_mut(&value).unwrap().role.take();
        if role.is_some() {
            bank.observed.borrowed.push(value);
        }
        let hook = take_failure_hook(&mut bank, FailurePoint::CreditProbe);
        drop(bank);
        if let Some(hook) = hook {
            hook.fire();
        }
        role
    }
    fn bind_job(
        &mut self,
        _: &ActorRuntime<FrontendActor>,
        _: &ActorHostTurn<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        result: CausalInputObligation<FrontendActor>,
    ) -> PreparedEffectJob {
        let mut bank = self.0.lock().unwrap();
        let value = value(&ticket.effect);
        bank.observed.bound.push(value);
        let original = bank.originals.get(&value).unwrap().reference;
        bank.observed.origins.push(
            result.event_author().unwrap().work() == original && ticket.descriptor.work == original,
        );
        let original = bank.originals.get_mut(&value).unwrap();
        PreparedEffectJob {
            effect: ticket.effect.clone(),
            exit: original.exit.take().unwrap(),
            result,
            captures: original.capture.take().unwrap(),
        }
    }
    fn return_unclaimed_result(&mut self, result: CausalInputObligation<FrontendActor>) {
        let mut bank = self.0.lock().unwrap();
        bank.observed.returned += 1;
        let original = result.event_author().unwrap().work();
        let row = bank
            .originals
            .values_mut()
            .find(|row| row.reference == original)
            .unwrap();
        assert!(
            row.role.replace(result).is_none(),
            "return the same original loan, never manufacture another"
        );
    }
    fn core_consumed(&mut self, _: &[InputId]) {}
    fn owner_failed_pending(&mut self, pending: PendingHandoff) {
        self.0.lock().unwrap().failed = Some(pending);
    }
    fn close_external(&mut self) {
        self.0.lock().unwrap().closed = true;
    }
    fn result_admission_closed(&self) -> bool {
        self.0.lock().unwrap().closed
    }
    fn progress_retirement(
        &mut self,
        _: &ActorRuntime<FrontendActor>,
        _: &ActorHostTurn<FrontendActor>,
        _: Tick,
        budget: usize,
    ) -> SourceProgress {
        let mut bank = self.0.lock().unwrap();
        let changed = budget != 0 && std::mem::take(&mut bank.changed);
        if changed {
            bank.observed.availability_changes += 1;
        }
        // This fixture Source retains all actual resources for explicit cleanup.
        // Only a physically moved original purpose loan is progress; a check,
        // registration wake, or Full visit never manufactures another pass.
        let progress = SourceProgress {
            units: usize::from(changed),
            progressed: changed,
            more: false,
            retired: false,
        };
        let hook = take_failure_hook(&mut bank, FailurePoint::Retirement);
        drop(bank);
        if let Some(hook) = hook {
            hook.fire();
        }
        progress
    }
}
#[derive(Default)]
struct HoldingExecutor {
    refusing: AtomicBool,
    jobs: Mutex<Vec<PreparedEffectJob>>,
    values: Mutex<Vec<u64>>,
}
impl EffectExecutor for HoldingExecutor {
    fn handoff(&self, job: PreparedEffectJob) -> Result<(), PreparedEffectJob> {
        if self.refusing.load(Ordering::Acquire) {
            return Err(job);
        }
        self.values.lock().unwrap().push(value(&job.effect));
        self.jobs.lock().unwrap().push(job);
        Ok(())
    }
}
struct Fixture {
    ledger: RuntimeLedger,
    scope: ScopeHandle,
    runtime: ActorRuntime<FrontendActor>,
    registration: ActorRegistration,
    host: HostRegistry,
    bank: Arc<Mutex<Bank>>,
    executor: Arc<HoldingExecutor>,
    captures: Arc<std::sync::atomic::AtomicUsize>,
    count: usize,
    max_effects: usize,
}
impl Fixture {
    // Each tuple is value/lane/dependencies; identities are correlations only.
    // Every author/loan comes from the same actual reserve/bind/registration.
    fn new(groups: Vec<Vec<(u64, u64, Vec<u64>)>>, full: &[u64]) -> Self {
        Self::with_effects(groups, full, |value| FrontendEffect::Test { value })
    }
    fn with_effects(
        groups: Vec<Vec<(u64, u64, Vec<u64>)>>,
        full: &[u64],
        make_effect: impl Fn(u64) -> FrontendEffect,
    ) -> Self {
        let ledger = RuntimeLedger::new(ResourceVector::default());
        let scope = ledger
            .open_scope_next(None, Generation::new(1).unwrap())
            .unwrap();
        let count = groups.iter().map(Vec::len).sum();
        let max_effects = groups.iter().map(Vec::len).max().unwrap();
        let mut reserved: Vec<(u64, WorkObligation, ReservationObligation)> = Vec::new();
        let mut batches = Vec::new();
        let mut lanes = BTreeMap::new();
        for group in groups {
            let mut batch = Vec::new();
            for (value, lane, dependencies) in group {
                let (work, capacity) = scope
                    .reserve_next(
                        Generation::new(1).unwrap(),
                        ReservationResources::new(
                            ResourceVector::default(),
                            OwnedSlotNeed {
                                work_headers: 1,
                                completions: 1,
                                captures: 1,
                                ..Default::default()
                            },
                        ),
                        WorkKind::Business,
                        WorkAdmission::External,
                    )
                    .unwrap()
                    .bind(WorkKind::Business)
                    .unwrap();
                batch.push(Planned {
                    effect: make_effect(value),
                    dependencies,
                    work: work.reference(),
                });
                lanes.insert(value, lane);
                reserved.push((value, work, capacity));
            }
            batches.push(batch);
        }
        let attempts = batches
            .iter()
            .flatten()
            .any(|entry| matches!(entry.effect, FrontendEffect::Attempt(_)));
        let runtime = ActorRuntime::new();
        let config = ActorConfig {
            mailbox: [32; 4],
            max_batches: 32,
            max_effects_per_step: max_effects,
            max_dependencies_per_effect: 1,
            turn_budget: (max_effects + 1) * 4,
            max_step_work: max_effects,
        };
        let actor = ActorId::new(1).unwrap();
        let registration = runtime
            .register_bound(
                actor,
                scope.clone(),
                FrontendActor::Test(Box::new(Emitter { batches })),
                config,
            )
            .unwrap_or_else(|_| panic!("actual purpose actor registration"));
        let captures = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut bank = Bank {
            originals: BTreeMap::new(),
            held: BTreeMap::new(),
            observed: Observed::default(),
            changed: false,
            closed: false,
            failed: None,
            failure_hook: None,
        };
        for (value, work, mut capacity) in reserved {
            let role = CausalInputObligation::reserve_admitted(
                &runtime,
                &registration,
                registration.actor(),
                InputClass::Ordinary,
                &work,
            )
            .unwrap();
            let reference = work.reference();
            let report = ReportSlot::reserve_next(
                &mut capacity,
                Generation::new(1).unwrap(),
                1,
                [(ConsumerId::new(1).unwrap(), ConsumerKind::Observer)],
            )
            .unwrap();
            let (custody, exit) =
                ExitCustody::from_admitted(ReportedWork::try_new(work, capacity, report).unwrap());
            let role = if full.contains(&value) {
                bank.held.insert(value, role);
                None
            } else {
                Some(role)
            };
            bank.originals.insert(
                value,
                Original {
                    custody,
                    exit: Some(exit),
                    role,
                    capture: Some(Box::new(Capture(captures.clone()))),
                    reference,
                },
            );
        }
        let bank = Arc::new(Mutex::new(bank));
        let executor = Arc::new(HoldingExecutor::default());
        let host = HostRegistry::new(
            runtime.clone(),
            ledger.clone(),
            Arc::new(FrontendHostObservations::new(Arc::new(FixedClock))),
        );
        // Static lanes encode the test value directly, except values 1/3 share
        // the original lane in the dedicated FIFO case below. No allocation or
        // authorization is created by this routing classifier.
        let common_lane = lanes.get(&1) == lanes.get(&3) && lanes.contains_key(&3);
        let lane: fn(&FrontendEffect) -> Option<FrontendLaneKey> = if common_lane {
            |effect| {
                Some(FrontendLaneKey::Test(if value(effect) == 3 {
                    1
                } else {
                    value(effect)
                }))
            }
        } else {
            |effect| Some(FrontendLaneKey::Test(value(effect)))
        };
        host.register(HostBinding {
            scope: scope.clone(),
            registration: registration.clone(),
            kind: FrontendActorKind::Test,
            config,
            policy: if attempts {
                FrontendLanePolicy::attempt(max_effects)
            } else {
                FrontendLanePolicy {
                    lane,
                    per_step_inventory: max_effects,
                }
            },
            source: Box::new(PurposeSource(bank.clone())),
            executor: executor.clone(),
        })
        .unwrap_or_else(|_| panic!("actual purpose Source registration"));
        Self {
            ledger,
            scope,
            runtime,
            registration,
            host,
            bank,
            executor,
            captures,
            count,
            max_effects,
        }
    }
    fn send(&self, batch: u64) {
        self.runtime
            .reserve_input(
                self.registration.actor(),
                self.registration.actor(),
                InputClass::Ordinary,
            )
            .unwrap()
            .fill(FrontendInput::Test(batch))
            .unwrap();
        self.runtime.poll_one(Tick(1)).unwrap();
    }
    fn drive_one(&self) -> bool {
        if let Some(turn) = self.runtime.claim_host_actor() {
            self.host.drive(Tick(1), turn);
            true
        } else {
            false
        }
    }
    fn install_failure_hook(&self, point: FailurePoint) {
        self.bank.lock().unwrap().failure_hook = Some(FailureHook {
            point,
            runtime: self.runtime.clone(),
            registration: self.registration.clone(),
        });
    }
    /// Capture the real host result before its public fail-stop wrapper. A
    /// regression therefore reports Err, while originals can still be cleaned.
    fn probe_host_result(&self) -> Result<HostProgress, HostError> {
        let entry = self.host.state.lock().unwrap().entries[&self.scope.id()].clone();
        let turn = self.runtime.claim_host_actor().unwrap();
        let mut machine = entry.machine.lock().unwrap().take().unwrap();
        let result = self.host.drive_inner(&entry, &mut machine, &turn, Tick(1));
        *entry.machine.lock().unwrap() = Some(machine);
        turn.release(false).unwrap();
        result
    }
    fn to_idle(&self) -> (usize, bool) {
        let mut turns = 0;
        while turns < 128 && self.drive_one() {
            turns += 1;
        }
        let extra = self.runtime.claim_host_actor();
        let idle = extra.is_none();
        if let Some(turn) = extra {
            turn.release(false).unwrap();
        }
        (turns, idle)
    }
    fn active_scan(&self) -> bool {
        let state = self.host.state.lock().unwrap();
        let entry = state.entries.get(&self.scope.id()).unwrap();
        let machine = entry.machine.lock().unwrap();
        machine
            .as_ref()
            .unwrap()
            .scan_remaining
            .is_some_and(|remaining| remaining != 0)
    }
    fn release_credit(&self, value: u64) {
        let mut bank = self.bank.lock().unwrap();
        let original = bank.held.remove(&value).unwrap();
        bank.originals.get_mut(&value).unwrap().role = Some(original);
        bank.changed = true;
        drop(bank);
        self.runtime.notify_host_actor(&self.registration).unwrap();
    }
    fn captured(&self) -> (Observed, Vec<u64>) {
        (
            self.bank.lock().unwrap().observed.clone(),
            self.executor.values.lock().unwrap().clone(),
        )
    }
    fn finish(self, stopped: bool) -> (usize, bool) {
        // Observations have already been copied. Taking the host machine only
        // stops the fixture driver; it does not claim an original runtime exit.
        let entry = self
            .host
            .state
            .lock()
            .unwrap()
            .entries
            .remove(&self.scope.id())
            .unwrap();
        let mut machine = entry.machine.lock().unwrap().take().unwrap();
        let mut jobs = std::mem::take(&mut *self.executor.jobs.lock().unwrap());
        if let Some(pending) = machine.pending.take() {
            self.runtime
                .effect_completed(pending.ticket)
                .unwrap_or_else(|_| panic!("complete same claimed cleanup metadata"));
            jobs.push(pending.job);
        }
        if let Some(pending) = self.bank.lock().unwrap().failed.take() {
            self.runtime
                .effect_completed(pending.ticket)
                .unwrap_or_else(|_| panic!("complete original failure custodian ticket"));
            jobs.push(pending.job);
        }
        if stopped
            && !self
                .runtime
                .snapshot(self.registration.actor())
                .unwrap()
                .failed
        {
            // Stop is irreversible. After capturing the oracle, a real isolated
            // actor panic fences its old generation solely for cleanup. Runtime
            // owns/catches that panic; the test thread does not unwind or abort.
            self.runtime
                .reserve_input(
                    self.registration.actor(),
                    self.registration.actor(),
                    InputClass::Lifecycle,
                )
                .unwrap()
                .fill(FrontendInput::Test(CLEANUP_FENCE))
                .unwrap();
            self.runtime.poll_one(Tick(2)).unwrap();
        }
        let actor = self.registration.actor();
        let failed = self.runtime.snapshot(actor).unwrap().failed;
        for (_, input) in self
            .runtime
            .committed_batches(actor, None, usize::MAX)
            .unwrap()
        {
            for (descriptor, state) in self
                .runtime
                .effect_inventory(actor, input, None, self.max_effects)
                .unwrap()
            {
                if state == EffectState::Pending {
                    if failed {
                        self.runtime
                            .abandon_unstarted_effect(actor, input, descriptor.id)
                            .unwrap();
                    } else {
                        let ticket = self
                            .runtime
                            .take_effect(actor, input, descriptor.id)
                            .unwrap();
                        let completion = self
                            .runtime
                            .effect_completed_with_receipt(ticket)
                            .unwrap_or_else(|_| panic!("consume original cleanup effect"));
                        self.runtime
                            .acknowledge_completed_effect(&completion)
                            .unwrap();
                    }
                }
            }
        }
        for input in self
            .runtime
            .input_inventory(actor, None, usize::MAX)
            .unwrap()
        {
            self.runtime.retire_input(actor, input.input).unwrap();
        }
        if failed {
            drop(self.runtime.take_failure_payload(actor).unwrap());
        }
        drop(machine);
        let mut bank = self.bank.lock().unwrap();
        let mut roles = std::mem::take(&mut bank.held)
            .into_values()
            .collect::<Vec<_>>();
        let mut exits = Vec::new();
        for original in bank.originals.values_mut() {
            if let Some(role) = original.role.take() {
                roles.push(role);
            }
            drop(original.capture.take());
            if let Some(exit) = original.exit.take() {
                exits.push(exit);
            }
        }
        for PreparedEffectJob {
            exit,
            result,
            captures,
            ..
        } in jobs
        {
            drop(captures);
            roles.push(result);
            exits.push(exit);
        }
        // Actual capture destruction above precedes the same never-started
        // job's exit. Its still-held causal roles are not an exit substitute.
        for exit in exits {
            exit.revoke_unused_and_exit().unwrap();
        }
        for role in &mut roles {
            role.close_after_exit().unwrap();
        }
        drop(roles);
        for original in bank.originals.values() {
            assert!(
                original.custody.try_retire().unwrap(),
                "retire only genuinely exited original job"
            );
        }
        drop(bank);
        self.ledger
            .reap_scope_ready(self.scope.id(), self.count * 8 + 32)
            .unwrap();
        self.runtime.close_actor_admission(actor).unwrap();
        while let Some(turn) = self.runtime.claim_host_actor() {
            turn.release(false).unwrap();
        }
        self.runtime.seal_actor(actor).unwrap();
        self.scope.close_external().unwrap();
        self.runtime.remove_actor(actor).unwrap();
        self.ledger.remove_scope(self.scope.id()).unwrap();
        let dropped = self.captures.load(Ordering::SeqCst);
        let empty = self.ledger.is_empty() && self.runtime.is_empty() && self.host.is_empty();
        (dropped, empty)
    }
}

#[test]
fn full_purpose_does_not_block_an_independent_purpose_or_let_same_lane_tail_overtake() {
    let f = Fixture::new(
        vec![
            vec![(1, 7, vec![])],
            vec![(2, 8, vec![])],
            vec![(3, 7, vec![])],
        ],
        &[1],
    );
    for input in 0..3 {
        f.send(input);
    }
    let (_, idle) = f.to_idle();
    let (observed, accepted) = f.captured();
    let (dropped, empty) = f.finish(false);
    assert!(idle && empty);
    assert_eq!(dropped, 3);
    assert_eq!(accepted, vec![2]);
    assert_eq!(observed.borrowed, vec![2]);
    assert_eq!(observed.bound, vec![2]);
    assert_eq!(observed.returned, 0);
    assert!(observed.origins.iter().all(|same| *same));
    assert!(observed.visits.contains(&1));
    assert!(
        !observed.visits.contains(&3),
        "same original lane head protects its independently funded tail"
    );
}

#[test]
fn all_full_markers_exceeding_host_budget_finish_a_finite_pass_and_stay_idle() {
    let groups = (1..=16).map(|value| vec![(value, value, vec![])]).collect();
    let full = (1..=16).collect::<Vec<_>>();
    let f = Fixture::new(groups, &full);
    for input in 0..16 {
        f.send(input);
    }
    let budget = f.host.state.lock().unwrap().entries[&f.scope.id()].budget;
    let (turns, idle) = f.to_idle();
    let (observed, accepted) = f.captured();
    let no_idle_claims = (0..8).all(|_| {
        let turn = f.runtime.claim_host_actor();
        let idle = turn.is_none();
        if let Some(turn) = turn {
            turn.release(false).unwrap();
        }
        idle
    });
    let visited = observed.visits.iter().copied().collect::<BTreeSet<_>>();
    let (dropped, empty) = f.finish(false);
    assert!(16 > budget);
    assert!(turns < 128 && idle && no_idle_claims && empty);
    assert_eq!(visited.len(), 16);
    assert!(accepted.is_empty() && observed.borrowed.is_empty() && observed.bound.is_empty());
    assert_eq!(observed.returned, 0);
    assert_eq!(dropped, 16);
}

#[test]
fn exact_not_ready_and_stopped_candidates_never_borrow_or_refund_a_purpose_loan() {
    let f = Fixture::new(vec![vec![(1, 1, vec![]), (2, 2, vec![1])]], &[1]);
    f.send(0);
    let (_, dependency_idle) = f.to_idle();
    let (dependency, accepted) = f.captured();
    let dep_cleanup = f.finish(false);
    let f = Fixture::new(vec![vec![(1, 1, vec![])]], &[]);
    f.send(0);
    f.scope.request_stop().unwrap();
    let (_, stop_idle) = f.to_idle();
    let (stopped, stop_accepted) = f.captured();
    let stop_cleanup = f.finish(true);
    assert!(dependency_idle && stop_idle);
    assert_eq!(dep_cleanup, (2, true));
    assert_eq!(stop_cleanup, (1, true));
    assert!(accepted.is_empty() && stop_accepted.is_empty());
    assert!(!dependency.visits.contains(&2));
    assert!(stopped.visits.is_empty());
    assert!(dependency.borrowed.is_empty() && stopped.borrowed.is_empty());
    assert_eq!(dependency.returned + stopped.returned, 0);
}

#[test]
fn original_credit_change_during_budget_yield_requests_one_rescan_after_the_old_pass() {
    let groups = (1..=16).map(|value| vec![(value, value, vec![])]).collect();
    let full = (1..=16).collect::<Vec<_>>();
    let f = Fixture::new(groups, &full);
    for input in 0..16 {
        f.send(input);
    }
    let mut reached = false;
    for _ in 0..64 {
        if !f.drive_one() {
            break;
        }
        if f.active_scan() && f.bank.lock().unwrap().observed.visits.contains(&1) {
            reached = true;
            break;
        }
    }
    let visits_before = f
        .bank
        .lock()
        .unwrap()
        .observed
        .visits
        .iter()
        .filter(|&&value| value == 1)
        .count();
    f.release_credit(1);
    let (_, idle) = f.to_idle();
    let (observed, accepted) = f.captured();
    let visits_after = observed.visits.iter().filter(|&&value| value == 1).count();
    let (dropped, empty) = f.finish(false);
    assert!(reached && idle && empty);
    assert!(visits_after > visits_before);
    assert_eq!(accepted, vec![1]);
    assert_eq!(observed.borrowed, vec![1]);
    assert_eq!(observed.bound, vec![1]);
    assert_eq!(observed.availability_changes, 1);
    assert_eq!(observed.returned, 0);
    assert!(observed.origins.iter().all(|same| *same));
    assert_eq!(dropped, 16);
}

#[test]
fn actual_completion_during_active_pass_rescans_the_new_same_lane_head() {
    let groups = (1..=16)
        .map(|value| vec![(value, if value == 3 { 1 } else { value }, vec![])])
        .collect();
    let full = (1..=16)
        .filter(|value| *value != 1 && *value != 3)
        .collect::<Vec<_>>();
    let f = Fixture::new(groups, &full);
    for input in 0..16 {
        f.send(input);
    }
    let mut first_handoff_yielded = false;
    for _ in 0..64 {
        if !f.drive_one() {
            break;
        }
        if f.executor.values.lock().unwrap().as_slice() == [1] && f.active_scan() {
            first_handoff_yielded = true;
            break;
        }
    }
    // No loan credit changes or artificial availability notifications occur.
    // The next turn consumes A1's real completion wake while finishing the old
    // pass over Full heads. Only retained completion progress can revisit A2.
    let (_, idle) = f.to_idle();
    let (observed, accepted) = f.captured();
    let (dropped, empty) = f.finish(false);
    assert!(first_handoff_yielded && idle && empty);
    assert_eq!(accepted, vec![1, 3]);
    assert_eq!(observed.borrowed, vec![1, 3]);
    assert_eq!(observed.bound, vec![1, 3]);
    assert_eq!(observed.availability_changes, 0);
    assert_eq!(observed.returned, 0);
    assert!(observed.origins.iter().all(|same| *same));
    assert_eq!(dropped, 16);
}

#[test]
fn owner_failure_after_initial_snapshot_reaches_the_original_custodian_before_indexing() {
    let f = Fixture::new(vec![vec![(1, 1, vec![])]], &[1]);
    f.send(0);
    f.install_failure_hook(FailurePoint::Retirement);
    let result = f.probe_host_result();
    let core_failed = f.runtime.snapshot(f.registration.actor()).unwrap().failed;
    let (observed, accepted) = f.captured();
    let (dropped, empty) = f.finish(false);
    assert!(matches!(result, Ok(progress) if progress.more && !progress.retired));
    assert!(core_failed && empty);
    assert!(observed.visits.is_empty() && observed.borrowed.is_empty() && accepted.is_empty());
    assert_eq!(observed.returned, 0);
    assert_eq!(dropped, 1);
}

#[test]
fn owner_failure_during_full_credit_probe_reaches_the_original_custodian_before_defer() {
    let f = Fixture::new(vec![vec![(1, 1, vec![])]], &[1]);
    f.send(0);
    f.install_failure_hook(FailurePoint::CreditProbe);
    let result = f.probe_host_result();
    let core_failed = f.runtime.snapshot(f.registration.actor()).unwrap().failed;
    let (observed, accepted) = f.captured();
    let (dropped, empty) = f.finish(false);
    assert!(matches!(result, Ok(progress) if progress.more && !progress.retired));
    assert!(core_failed && empty);
    assert_eq!(observed.visits, [1]);
    assert!(observed.borrowed.is_empty() && accepted.is_empty());
    assert_eq!(observed.returned, 0);
    assert_eq!(dropped, 1);
}

#[test]
fn inactive_financial_author_without_core_failure_preserves_the_original_typed_error() {
    let f = Fixture::new(vec![vec![(1, 1, vec![])]], &[]);
    f.send(0);
    let (capture, exit) = {
        let mut bank = f.bank.lock().unwrap();
        let original = bank.originals.get_mut(&1).unwrap();
        (original.capture.take(), original.exit.take().unwrap())
    };
    drop(capture);
    exit.revoke_unused_and_exit().unwrap();
    let result = f.probe_host_result();
    let core_failed = f.runtime.snapshot(f.registration.actor()).unwrap().failed;
    let (observed, accepted) = f.captured();
    // The author really exited before its pending metadata was claimed. Fence
    // that invalid old generation only after capturing its original error.
    let (dropped, empty) = f.finish(true);
    assert!(matches!(
        result,
        Err(HostError::Dispatch(DispatchError::Runtime(
            ActorTurnError::InputAuthorInactive
        )))
    ));
    assert!(!core_failed && empty);
    assert!(observed.visits.is_empty() && observed.borrowed.is_empty() && accepted.is_empty());
    assert_eq!(dropped, 1);
}

#[test]
fn committed_withdrawal_reaches_a_full_send_before_it_can_bind() {
    assert_withdrawal_reaches_the_original_send(false);
}

#[test]
fn committed_withdrawal_reaches_the_send_held_by_executor_refusal() {
    assert_withdrawal_reaches_the_original_send(true);
}

fn assert_withdrawal_reaches_the_original_send(executor_refuses: bool) {
    use crate::control_runtime::transport_withdrawal::AttemptEffectEnvelope;
    use novarocks_attempt_protocol::{
        capacity::ReservationToken,
        config::DispatchLane,
        effect::{AttemptEffect, DispatchBatch, EffectBody, EffectClass},
    };
    use novarocks_control_runtime::exit_work::HandoffState;
    use novarocks_coordination_contract::{AttemptActionId, MonotonicInstant};
    use novarocks_types::BackendProcessId;
    use std::{num::NonZeroU64, time::Duration};

    let send_id = AttemptActionId::new(NonZeroU64::MIN);
    let send = AttemptEffectEnvelope::new(AttemptEffect {
        id: send_id,
        class: EffectClass::Business,
        body: EffectBody::SendBatch {
            batch: DispatchBatch {
                backend: BackendProcessId::new_v7(),
                lane: DispatchLane::Create,
                control_progress: false,
                operations: Box::new([]),
                queued_bytes: 0,
                queued_at: MonotonicInstant::from_origin(Duration::ZERO),
                queued_inputs: Box::new([]),
            },
            reservation: ReservationToken(NonZeroU64::MIN),
        },
    });
    let withdraw = AttemptEffectEnvelope::new(AttemptEffect {
        id: AttemptActionId::new(NonZeroU64::new(2).unwrap()),
        class: EffectClass::Cleanup,
        body: EffectBody::Withdraw { effect: send_id },
    });
    let unavailable = if executor_refuses {
        [].as_slice()
    } else {
        [1].as_slice()
    };
    let f = Fixture::with_effects(
        vec![vec![(1, 1, vec![])], vec![(2, 2, vec![])]],
        unavailable,
        |value| {
            FrontendEffect::Attempt(if value == 1 {
                send.clone()
            } else {
                withdraw.clone()
            })
        },
    );
    f.executor
        .refusing
        .store(executor_refuses, Ordering::Release);
    f.send(0);
    let (_, idle_before) = f.to_idle();
    let pending_before = send.handoff().unwrap().state();
    f.send(1);
    f.scope.request_stop().unwrap();
    let (_, idle_after) = f.to_idle();
    let (blocked, handed_off) = f.captured();
    let withdrawn = send.handoff().unwrap().state();
    if executor_refuses {
        f.executor.refusing.store(false, Ordering::Release);
        f.runtime.notify_host_actor(&f.registration).unwrap();
    } else {
        f.release_credit(1);
    }
    let (_, idle_after_credit) = f.to_idle();
    let (released, handed_off_after_credit) = f.captured();
    let same_cell = f.executor.jobs.lock().unwrap().iter().any(|job| {
        let FrontendEffect::Attempt(effect) = &job.effect else {
            return false;
        };
        Arc::ptr_eq(effect.handoff().unwrap(), send.handoff().unwrap())
    });
    let (dropped, empty) = f.finish(false);
    assert!(idle_before && idle_after && idle_after_credit && empty);
    assert_eq!(pending_before, HandoffState::Pending);
    assert_eq!(withdrawn, HandoffState::Withdrawn);
    assert_eq!(blocked.bound.len(), usize::from(executor_refuses));
    assert!(handed_off.is_empty());
    assert_eq!(released.bound, [1]);
    assert_eq!(handed_off_after_credit, [1]);
    assert!(same_cell);
    assert_eq!(dropped, 2);
}
