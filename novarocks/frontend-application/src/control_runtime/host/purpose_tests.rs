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
#[path = "purpose_tests/credit_wake.rs"]
mod credit_wake;

use super::*;
use crate::control_runtime::actor::{FrontendInput, TestActor};
use novarocks_control_runtime::{
    actor::ActorStep,
    actor_turn::Clock,
    completion::ReportSlot,
    exit_custody::ExitCustody,
    obligation::{ReservationObligation, WorkObligation},
    publication::PublishedSnapshot,
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
    retirement_checks: usize,
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
    inspect_pending_retirement: bool,
    retain_binding: bool,
    retained_jobs: Vec<PreparedEffectJob>,
    publication: Arc<PublishedSnapshot<u64>>,
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
            transport: None,
            effect: ticket.effect.clone(),
            exit: original.exit.take().unwrap(),
            result,
            captures: original.capture.take().unwrap(),
        }
    }
    fn try_bind_effect(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        turn: &ActorHostTurn<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        result: CausalInputObligation<FrontendActor>,
    ) -> Result<PreparedEffectBinding, EffectBindRefusal> {
        let job = self.bind_job(runtime, turn, ticket, result);
        let publication = {
            let mut bank = self.0.lock().unwrap();
            if !bank.retain_binding {
                return Ok(PreparedEffectBinding::Job(job));
            }
            // Store the exact complete original before its current-ticket
            // publication; metadata acceptance never consumes its Exit.
            bank.retained_jobs.push(job);
            bank.publication.clone()
        };
        runtime
            .commit_snapshot(ticket, &publication, Arc::new(value(&ticket.effect)))
            .unwrap_or_else(|(error, _)| panic!("original retained publication: {error:?}"));
        Ok(PreparedEffectBinding::Retained { transport: None })
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
        if bank.inspect_pending_retirement {
            let mut units = 0;
            // Every unit pays one real original custody readiness inspection.
            // None of these never-started jobs has actually exited; observing
            // Pending neither returns its loan nor manufactures progress.
            for original in bank.originals.values().take(budget) {
                assert!(!original.custody.actual_exit_ready().unwrap());
                units += 1;
            }
            bank.observed.retirement_checks += units;
            return SourceProgress {
                units,
                progressed: false,
                more: false,
                retired: false,
            };
        }
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
            inspect_pending_retirement: false,
            retain_binding: false,
            retained_jobs: Vec::new(),
            publication: Arc::new(PublishedSnapshot::new_for_actor(&registration, Arc::new(0))),
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
            transport: None,
            startup: None,
            capacity: None,
            membership: None,

            local_exit: None,
            ordered_observations: None,
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
        jobs.append(&mut self.bank.lock().unwrap().retained_jobs);
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
fn an_exact_budget_full_scan_reaches_idle_without_restarting_itself() {
    // The same original fixture derives ten Host units. After original
    // indexing, exactly ten Full heads consume one complete finite pass.
    // Ending that pass must not look like a fresh scan that never started.
    let full = (1..=10).collect::<Vec<_>>();
    let f = Fixture::new(
        full.iter()
            .map(|&value| vec![(value, value, vec![])])
            .collect(),
        &full,
    );
    for batch in 0..10 {
        f.send(batch);
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
    let cleanup = f.finish(false);
    // Both behavior and the bounded no-spin oracle follow actual cleanup.
    assert_eq!(budget, 10);
    assert_eq!(cleanup, (10, true));
    assert!(turns < 128 && idle && no_idle_claims);
    assert_eq!(visited.len(), 10);
    assert!(accepted.is_empty() && observed.borrowed.is_empty() && observed.bound.is_empty());
    assert_eq!(observed.returned, 0);
}

#[test]
fn an_original_credit_return_survives_exact_index_budget_before_a_new_scan() {
    // The maximum batch has two effects, so this existing fixture derives a
    // fifteen-unit Host allowance. Do not alter the actual registered budget.
    let groups = vec![
        vec![(1, 1, vec![]), (2, 2, vec![])],
        vec![(3, 3, vec![]), (4, 4, vec![])],
        vec![(5, 5, vec![]), (6, 6, vec![])],
        vec![(7, 7, vec![]), (8, 8, vec![])],
        vec![(9, 9, vec![]), (10, 10, vec![])],
        vec![(11, 11, vec![])],
    ];
    let f = Fixture::new(groups, &(1..=11).collect::<Vec<_>>());
    f.send(0);
    let (_, first_idle) = f.to_idle();
    let first_visits = f.captured().0.visits;
    let no_partial_scan = !f.active_scan();
    let budget = f.host.state.lock().unwrap().entries[&f.scope.id()].budget;
    // This moves the same original paid loan back into its bank. Its one real
    // availability change costs one unit; the next five original batches cost
    // 4*(1+2)+(1+1)=14 index units. The old scan has already ended.
    f.release_credit(1);
    for batch in 1..6 {
        f.send(batch);
    }
    let (_, idle) = f.to_idle();
    let (observed, accepted) = f.captured();
    let cleanup = f.finish(false);
    assert!(first_idle && no_partial_scan && idle);
    assert!(first_visits.contains(&1) && first_visits.contains(&2));
    assert_eq!(budget, 15);
    assert_eq!(cleanup, (11, true));
    assert_eq!(observed.availability_changes, 1);
    assert_eq!(accepted, vec![1]);
    assert_eq!(observed.borrowed, vec![1]);
    assert_eq!(observed.bound, vec![1]);
    assert_eq!(observed.returned, 0);
    assert!(observed.origins.iter().all(|same| *same));
}

#[test]
fn pending_retirement_checks_preserve_one_notified_full_pass_then_return_idle() {
    let full = (1..=10).collect::<Vec<_>>();
    let f = Fixture::new(
        full.iter()
            .map(|&value| vec![(value, value, vec![])])
            .collect(),
        &full,
    );
    for batch in 0..10 {
        f.send(batch);
    }
    let (_, first_idle) = f.to_idle();
    let before = f.captured().0;
    let scan_finished = {
        let entry = f.host.state.lock().unwrap().entries[&f.scope.id()].clone();
        entry
            .machine
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .scan_remaining
            .is_none()
    };
    let budget = f.host.state.lock().unwrap().entries[&f.scope.id()].budget;
    f.bank.lock().unwrap().inspect_pending_retirement = true;
    // One controlled original Host notification starts the readiness check.
    // No new input, credit return, timer, or subsequent wake is submitted.
    f.runtime.notify_host_actor(&f.registration).unwrap();
    let checked = f.drive_one();
    let after_check = f.captured().0;
    // The actual Pending inventory has received its one finite pass. A
    // continuation must not start that same metadata inspection again.
    f.bank.lock().unwrap().inspect_pending_retirement = false;
    let (turns, idle) = f.to_idle();
    let (observed, accepted) = f.captured();
    let quiet = (0..8).all(|_| !f.drive_one());
    let cleanup = f.finish(false);
    // Inspect every outcome only after all original jobs/captures/roles exit.
    assert!(first_idle && scan_finished);
    assert_eq!(budget, 10);
    assert_eq!(cleanup, (10, true));
    assert!(checked);
    assert_eq!(after_check.retirement_checks, budget);
    assert_eq!(after_check.visits, before.visits);
    assert_eq!(turns, 1);
    assert!(idle && quiet);
    assert_eq!(observed.retirement_checks, budget);
    assert_eq!(observed.visits.len(), before.visits.len() + full.len());
    assert_eq!(
        observed.visits[before.visits.len()..]
            .iter()
            .copied()
            .collect::<BTreeSet<_>>(),
        full.iter().copied().collect::<BTreeSet<_>>()
    );
    assert_eq!(observed.availability_changes, 0);
    assert!(accepted.is_empty() && observed.borrowed.is_empty() && observed.bound.is_empty());
    // The notification owes exactly one finite retry; unchanged Pending
    // checks are not progress and cannot keep Full heads polling themselves.
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

use crate::task_execution::host::failure_supervisor::{
    FailureCleanupPort, FailureCleanupRequest, FailureSupervisor, RunningFailureSupervisor,
};

struct SupervisorSource {
    setup: Option<(
        FailureSupervisor,
        FailureContext,
        Box<dyn FailureCleanupPort>,
    )>,
    running: Option<RunningFailureSupervisor>,
    cleanup: Arc<Mutex<Option<(InputId, InputId)>>>,
    unresolved: novarocks_coordination_contract::AttemptActionId,
}

impl EffectSource for SupervisorSource {
    fn prepay_result(
        &mut self,
        _: &FrontendEffect,
    ) -> Option<CausalInputObligation<FrontendActor>> {
        panic!("supervisor cleanup does not borrow a business input position")
    }

    fn bind_job(
        &mut self,
        _: &ActorRuntime<FrontendActor>,
        _: &ActorHostTurn<FrontendActor>,
        _: &EffectTicket<FrontendEffect>,
        _: CausalInputObligation<FrontendActor>,
    ) -> PreparedEffectJob {
        panic!("supervisor cleanup uses its original Work")
    }

    fn return_unclaimed_result(&mut self, _: CausalInputObligation<FrontendActor>) {
        unreachable!();
    }

    fn core_consumed(&mut self, _: &[InputId]) {}

    fn owner_failed_pending(&mut self, _: PendingHandoff) {
        unreachable!();
    }

    fn owner_failed_cleanup(
        &mut self,
        ticket: EffectTicket<FrontendEffect>,
        context: Option<FailureContext>,
        observations: Option<OrderedObservationFailure>,
    ) {
        assert!(context.is_none());
        let FrontendEffect::OwnerFailed { failed_input, .. } = &ticket.effect else {
            unreachable!()
        };
        assert!(
            self.cleanup
                .lock()
                .unwrap()
                .replace((ticket.input, failed_input.input))
                .is_none()
        );
        let (supervisor, context, port) = self.setup.take().unwrap();
        self.running = Some(supervisor.start(
            ticket,
            context,
            [self.unresolved].into(),
            port,
            observations,
        ));
    }

    fn close_external(&mut self) {}

    fn result_admission_closed(&self) -> bool {
        true
    }

    fn progress_retirement(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        turn: &ActorHostTurn<FrontendActor>,
        _: Tick,
        budget: usize,
    ) -> SourceProgress {
        let mut progress = SourceProgress {
            units: 0,
            progressed: false,
            more: false,
            retired: false,
        };
        if budget != 0
            && let Some(running) = self.running.as_mut()
        {
            progress.units = 1;
            if let std::task::Poll::Ready(result) = running.progress() {
                result.unwrap();
                let (_, failed) = self.cleanup.lock().unwrap().unwrap();
                runtime
                    .retire_input(turn.registration().actor(), failed)
                    .unwrap();
                self.running.take();
                progress.progressed = true;
                progress.retired = true;
            }
        }
        progress
    }
}

struct HoldCleanup {
    reached: tokio::sync::oneshot::Sender<(
        FailureCleanupRequest,
        Vec<novarocks_execution_contract::AbortParticipation>,
    )>,
    release: tokio::sync::oneshot::Receiver<()>,
    aborts: Vec<novarocks_execution_contract::AbortParticipation>,
}
impl FailureCleanupPort for HoldCleanup {
    fn abort_once(
        &mut self,
        request: novarocks_execution_contract::AbortParticipation,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.aborts.push(request);
        })
    }

    fn finish(
        self: Box<Self>,
        original: FailureCleanupRequest,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        Box::pin(async move {
            self.reached
                .send((original, self.aborts))
                .unwrap_or_else(|_| panic!("original cleanup observer"));
            self.release.await.unwrap();
        })
    }
}

#[tokio::test]
async fn supervisor_cleanup_uses_its_own_ticket_and_keeps_the_host_until_exit() {
    run_failure_publication(false, false).await;
    run_failure_publication(true, false).await;
    run_failure_publication(true, true).await;
}

async fn run_failure_publication(bind_reply: bool, already_published: bool) {
    use crate::task_execution::host::port::{
        AttemptPortEndpoints,
        abort::{AbortIngressRetirementProgress, AttemptAbortIngress},
        control::{ControlBinding, ControlPositions, SourcePublishedSlot},
        facts::{AttemptFactBank, FactRole},
        requests::AttemptRequests,
        root_control::RootControlRequests,
    };
    use crate::task_execution::host::startup::AttemptStartupSource;
    use crate::task_execution::host::{
        failure_lifecycle::FailureLifecycle,
        failure_publication::FailurePublications,
        observations::{
            MetadataRetirementRefusal, ObservationBudget, ObservationError,
            ObservationMetadataLayout, ObservationMetadataObligation, OrderedObservationInput,
            RootControlBinding, StreamKey,
            credits::ObservationCredits,
            read_progress::ReadProgressInput,
            source::{BoundObservationSource, ObservationMetadataAuthor},
        },
    };
    use novarocks_attempt_protocol::input::{
        AttemptInput, ExecutorFact, ObservationEvent, ObservationFact, ObservationFailure,
    };
    use novarocks_attempt_protocol::port_output as ap;
    use novarocks_control_runtime::obligation::OwnedPositionObligation;
    use novarocks_control_runtime_core::owned_storage::OwnedSlotKind;
    use novarocks_coordination_contract::logical_port::{AttemptToLogical, AttemptToLogicalBody};
    use novarocks_coordination_contract::{AttemptActionId, AttemptActivationIdentity};
    use novarocks_execution_contract::ParticipationRef;
    use novarocks_execution_contract::TaskIdentity;
    use novarocks_logical_execution_protocol::input::LogicalInput;
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::{
        future::ready,
        num::NonZeroU64,
        pin::Pin,
        task::{Context, Poll, Waker},
    };

    #[derive(Debug)]
    struct ObservationClock;
    impl novarocks_coordination_contract::MonotonicClock for ObservationClock {
        fn now(&self) -> novarocks_coordination_contract::MonotonicInstant {
            novarocks_coordination_contract::MonotonicInstant::from_origin(
                std::time::Duration::ZERO,
            )
        }
    }

    struct Metadata(Mutex<Option<OwnedPositionObligation>>);
    struct Backing(Option<OwnedPositionObligation>);
    impl ObservationMetadataAuthor for Metadata {
        fn retain(
            &self,
            _: ObservationMetadataLayout,
            _: usize,
        ) -> Result<Box<dyn ObservationMetadataObligation>, ObservationError> {
            Ok(Box::new(Backing(self.0.lock().unwrap().take())))
        }
    }
    impl ObservationMetadataObligation for Backing {
        fn retire_after_exit(mut self: Box<Self>) -> Result<(), MetadataRetirementRefusal> {
            self.0.take().unwrap().retire_after_exit().unwrap();
            Ok(())
        }
    }

    struct ReplyEmitter {
        activation: AttemptActivationIdentity,
        prior: Arc<novarocks_coordination_contract::RuntimeFilterDiagnostic>,
    }
    struct LogicalReceiver(Arc<Mutex<Vec<FrontendInput>>>);
    impl TestActor for LogicalReceiver {
        fn handle(
            &mut self,
            _: Tick,
            _: InputEnvelope,
            input: FrontendInput,
            step: &mut ActorStep<'_, FrontendEffect>,
        ) {
            assert!(
                step.charge(1),
                "the original LP input consumes bounded step work"
            );
            self.0.lock().unwrap().push(input);
        }
    }
    // This is the receiving actor's actual registered host Source. The bank
    // alone owns its sender credits; indexing here invents no credit or ACK.
    struct LogicalReceivingSource {
        scope: ScopeHandle,
        indexed: Arc<Mutex<Vec<InputId>>>,
        closing: bool,
    }
    impl EffectSource for LogicalReceivingSource {
        fn prepay_result(
            &mut self,
            _: &FrontendEffect,
        ) -> Option<CausalInputObligation<FrontendActor>> {
            panic!("passive receiving actor emits no effects")
        }
        fn bind_job(
            &mut self,
            _: &ActorRuntime<FrontendActor>,
            _: &ActorHostTurn<FrontendActor>,
            _: &EffectTicket<FrontendEffect>,
            _: CausalInputObligation<FrontendActor>,
        ) -> PreparedEffectJob {
            panic!("passive receiving actor has no effect job")
        }
        fn return_unclaimed_result(&mut self, _: CausalInputObligation<FrontendActor>) {
            unreachable!("passive receiving actor borrows no result position")
        }
        fn core_consumed(&mut self, inputs: &[InputId]) {
            self.indexed.lock().unwrap().extend_from_slice(inputs);
        }
        fn owner_failed_pending(&mut self, _: PendingHandoff) {
            unreachable!("the passive receiving actor does not fail")
        }
        fn close_external(&mut self) {
            self.closing = true;
            self.scope.close_external().unwrap();
        }
        fn result_admission_closed(&self) -> bool {
            self.closing
        }
        fn progress_retirement(
            &mut self,
            runtime: &ActorRuntime<FrontendActor>,
            turn: &ActorHostTurn<FrontendActor>,
            _: Tick,
            budget: usize,
        ) -> SourceProgress {
            // All actual bank positions and the original bridge Work/header
            // are retired by their owners before the original close request.
            SourceProgress {
                units: usize::from(budget != 0),
                progressed: false,
                more: budget == 0 && self.closing,
                retired: budget != 0
                    && self.closing
                    && runtime
                        .snapshot(turn.registration().actor())
                        .unwrap()
                        .inputs
                        == 0
                    && self.scope.join_ready().unwrap(),
            }
        }
    }
    impl TestActor for ReplyEmitter {
        fn handle(
            &mut self,
            _: Tick,
            _: InputEnvelope,
            input: FrontendInput,
            step: &mut ActorStep<'_, FrontendEffect>,
        ) {
            assert!(
                !matches!(input, FrontendInput::Test(CLEANUP_FENCE)),
                "fixture failure after original reply admission"
            );
            if let FrontendInput::Attempt(original) = input {
                assert!(matches!(
                    *original,
                    AttemptInput::Executor(
                        ExecutorFact::Observations(_) | ExecutorFact::ReadProgress(_)
                    )
                ));
                return;
            }
            let work = step.input_author().unwrap().work();
            step.effect(
                EffectDescriptor {
                    id: ActionId::new(4).unwrap(),
                    work,
                    kind: EffectKind::Settlement,
                    dependencies: vec![],
                },
                FrontendEffect::Attempt(
                    crate::control_runtime::transport_withdrawal::AttemptEffectEnvelope::new(
                        novarocks_attempt_protocol::effect::AttemptEffect {
                            id: AttemptActionId::new(NonZeroU64::new(4).unwrap()),
                            class: novarocks_attempt_protocol::effect::EffectClass::Settlement,
                            body: novarocks_attempt_protocol::effect::EffectBody::Port(
                                ap::AttemptToLogical {
                                    activation: self.activation,
                                    action: AttemptActionId::new(NonZeroU64::new(4).unwrap()),
                                    body: ap::AttemptToLogicalBody::ConvergenceProgress {
                                        local_exited: false,
                                        closure: None,
                                        lifecycle_runtime_filters: Some(self.prior.clone()),
                                    },
                                },
                            ),
                        },
                    ),
                ),
            )
            .unwrap();
        }
    }

    let execution = QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap();
    let activation = AttemptActivationIdentity::from_parts(1, execution, 1);
    let prior_filters = Arc::new(novarocks_coordination_contract::RuntimeFilterDiagnostic {
        contributions: Vec::new(),
        complete: false,
    });
    let budget = ObservationBudget {
        queued_frames: 1,
        total_positions: 3,
        max_frame_bytes: 2048,
        control_bytes: 512,
        total_bytes: 4608,
    };
    let ledger = RuntimeLedger::new(budget.byte_window().unwrap());
    let initial = Generation::new(1).unwrap();
    let scope = ledger.open_scope_next(None, initial).unwrap();
    let (work, mut capacity) = scope
        .reserve_next(
            initial,
            ReservationResources::new(
                ResourceVector::default(),
                OwnedSlotNeed {
                    work_headers: 1,
                    completions: 1,
                    captures: 1,
                    ..Default::default()
                },
            ),
            WorkKind::Cleanup,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Cleanup)
        .unwrap();
    let runtime = ActorRuntime::new();
    let config = ActorConfig {
        mailbox: [4, 6, 4, 4],
        max_batches: 4,
        max_effects_per_step: 1,
        max_dependencies_per_effect: 0,
        turn_budget: 8,
        max_step_work: 1,
    };
    let actor = ActorId::new(1).unwrap();
    let registration = runtime
        .register_supervised_bound(
            actor,
            scope.clone(),
            FrontendActor::Test(Box::new(ReplyEmitter {
                activation,
                prior: prior_filters.clone(),
            })),
            config,
            &work,
        )
        .unwrap_or_else(|(error, _)| panic!("supervised registration: {error:?}"));
    let bridge_scope = ledger.open_scope_next(None, initial).unwrap();
    let (mut bridge_work, mut bridge_capacity) = bridge_scope
        .reserve_next(
            initial,
            ReservationResources::work_only(),
            WorkKind::Business,
            WorkAdmission::External,
        )
        .unwrap()
        .bind(WorkKind::Business)
        .unwrap();
    bridge_work.start().unwrap();
    let logical_seen = Arc::new(Mutex::new(Vec::new()));
    let logical = runtime
        .register_bound(
            ActorId::new(2).unwrap(),
            bridge_scope.clone(),
            FrontendActor::Test(Box::new(LogicalReceiver(logical_seen.clone()))),
            config,
        )
        .unwrap_or_else(|_| panic!("same paid LP fact receiver"));
    let participation = ParticipationRef::new(
        execution,
        FrontendProcessId::new_v7(),
        BackendProcessId::new_v7(),
    );
    let late_participation = ParticipationRef::new(
        execution,
        participation.frontend_process_id(),
        BackendProcessId::new_v7(),
    );
    let lifecycle = FailureLifecycle::freeze([participation, late_participation].into_iter());
    lifecycle.starts().record(participation);
    let late_start = lifecycle.starts();
    let publications = Arc::new(FailurePublications::new(
        registration.clone(),
        activation,
        &scope,
    ));
    let context = FailureContext {
        lifecycle,
        publications: publications.clone(),
    };
    let root = TaskIdentity::new(
        execution,
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        participation.backend_process_id(),
    );
    let root_binding =
        RootControlBinding::for_registered_source(registration.clone(), activation, root).unwrap();
    let roots = RootControlRequests::new(root_binding);
    let requests = AttemptRequests::new(
        activation,
        registration.clone(),
        logical.clone(),
        std::num::NonZeroUsize::new(1).unwrap(),
        Waker::noop().clone(),
    );
    let facts = AttemptFactBank::reserve(
        &runtime,
        &logical,
        &registration,
        activation,
        &bridge_work,
        Waker::noop().clone(),
        Waker::noop().clone(),
    )
    .unwrap();
    let standing = |class| {
        runtime
            .reserve_admitted_input_for(&registration, logical.actor(), class, &work)
            .unwrap()
            .into_standing_input()
            .unwrap_or_else(|_| panic!("same paid AP role"))
    };
    let abort = Arc::new(AttemptAbortIngress::new(
        logical.clone(),
        registration.clone(),
        activation,
        vec![
            (participation, standing(InputClass::Control)),
            (late_participation, standing(InputClass::Control)),
        ],
    ));
    let (storage_work, mut storage_payer) = bridge_scope
        .reserve_child_next(
            initial,
            bridge_capacity.id(),
            ReservationResources::new(
                ResourceVector::default(),
                OwnedSlotNeed {
                    work_headers: 1,
                    completions: 1,
                    captures: 1,
                    ..Default::default()
                },
            ),
            WorkKind::Settlement,
        )
        .unwrap()
        .bind(WorkKind::Settlement)
        .unwrap();
    let control_observer = ConsumerId::new(2).unwrap();
    let storage_report = ReportSlot::reserve_next(
        &mut storage_payer,
        initial,
        1,
        [(control_observer, ConsumerKind::Observer)],
    )
    .unwrap();
    let (control_slot, mut reader) = SourcePublishedSlot::bind(
        &runtime,
        ControlBinding {
            lp: logical.clone(),
            ap: registration.clone(),
            activation,
            positions: ControlPositions {
                stop: standing(InputClass::Control),
                convergence: standing(InputClass::Control),
                changed: standing(InputClass::Ordinary),
            },
            storage: ReportedWork::try_new(storage_work, storage_payer, storage_report).unwrap(),
            observer: control_observer,
            roots: roots.clone(),
        },
    )
    .unwrap_or_else(|_| panic!("same original control storage"));
    let (mut startup_source, startup) =
        AttemptStartupSource::new(registration.clone(), standing(InputClass::Lifecycle));
    let endpoints = AttemptPortEndpoints::bind(
        logical.clone(),
        registration.clone(),
        activation,
        startup,
        requests.clone(),
        facts.clone(),
        control_slot,
        abort,
        roots.clone(),
    )
    .unwrap_or_else(|_| panic!("same actual endpoint pair"));
    publications.bind_ports(endpoints.clone());
    assert!(publications.matches(&registration, activation));
    assert!(!publications.matches(&logical, activation));
    let mut role = bind_reply.then(|| {
        CausalInputObligation::reserve_admitted(
            &runtime,
            &registration,
            actor,
            InputClass::Control,
            &bridge_work,
        )
        .unwrap()
    });
    let original_prior = already_published.then(|| {
        let (_, input) = role
            .as_mut()
            .unwrap()
            .fill_with_receipt(None, FrontendInput::Test(1))
            .unwrap_or_else(|_| panic!("same admitted original source"));
        assert!(runtime.poll_one(Tick(0)).unwrap());
        let ticket = runtime
            .take_effect(actor, input, ActionId::new(4).unwrap())
            .unwrap();
        let value = AttemptToLogical {
            activation,
            action: AttemptActionId::new(NonZeroU64::new(4).unwrap()),
            body: AttemptToLogicalBody::ConvergenceProgress {
                local_exited: false,
                closure: None,
                lifecycle_runtime_filters: Some(prior_filters.clone()),
            },
        };
        let mut loan = facts.try_loan(FactRole::Lifecycle).unwrap();
        let published = loan.publish_now(&runtime, &ticket, value).unwrap();
        drop(loan);
        runtime.effect_completed(ticket).unwrap();
        runtime.retire_input(actor, input).unwrap();
        // Consume the original LP input but keep its original role in_flight
        // until the bank's real credit-retirement pass. Seen keeps the same Arc.
        assert!(runtime.poll_one(Tick(0)).unwrap());
        assert!(facts.try_loan(FactRole::Lifecycle).is_none());
        published
    });
    let mut observation = None;
    let mut ordered = None;
    if already_published {
        let (mut owner, mut positions) = scope
            .reserve_next(
                initial,
                ReservationResources::new(
                    budget.byte_window().unwrap(),
                    OwnedSlotNeed {
                        work_headers: 1,
                        inputs: budget.total_positions,
                        captures: 1,
                        ..Default::default()
                    },
                ),
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        owner.start().unwrap();
        let credits =
            ObservationCredits::bind(budget, [participation], &owner, &mut positions, None)
                .unwrap();
        let mut backing = positions.take_position(OwnedSlotKind::Capture).unwrap();
        backing.bind().unwrap();
        let binding = roots.binding().clone();
        let requests = roots.clone();
        let observation_source = ActorId::new(9).unwrap();
        let standing = |class, author| {
            runtime
                .reserve_admitted_input_for(&registration, observation_source, class, author)
                .unwrap()
                .into_standing_input()
                .unwrap()
        };
        let ordinary = standing(InputClass::Ordinary, &owner);
        let ordinary_id = ordinary.id();
        let progress = ReadProgressInput::new(&registration, participation, ordinary);
        let control = standing(InputClass::Control, &bridge_work);
        binding.bind_reply_author(control.author().unwrap());
        let ingress = OrderedObservationInput::new(
            standing(InputClass::Lifecycle, &owner),
            control,
            BoundObservationSource::new(
                binding.clone(),
                requests.clone(),
                credits,
                [(participation, progress.clone())].into(),
                Arc::new(Metadata(Mutex::new(Some(backing)))),
            ),
            binding,
            Arc::new(ObservationClock),
            |input| FrontendInput::Attempt(Box::new(input)),
            Waker::noop().clone(),
        )
        .unwrap();
        requests.bind_ingress(&ingress).unwrap();
        let key = StreamKey {
            participation,
            generation: NonZeroU64::MIN,
        };
        ingress.register_stream(key).unwrap();
        let mut credit = Some(ingress.reserve_read(key).await.unwrap());
        assert!(runtime.poll_one(Tick(0)).unwrap());
        assert!(progress.retire_ready().unwrap());
        let mut opened = ready(Ok::<(), ObservationFailure>(()));
        assert!(matches!(
            ingress
                .poll_open(
                    key,
                    &mut credit,
                    Pin::new(&mut opened),
                    &mut Context::from_waker(Waker::noop()),
                )
                .unwrap()
                .result,
            Poll::Ready(Ok(()))
        ));
        ingress
            .accept(
                key,
                ObservationEvent::Fact {
                    source_revision: None,
                    fact: ObservationFact::Bookmark {
                        generation: 1,
                        sequence: 1,
                        covered_prefix: 1,
                        source_cut: 1,
                    },
                },
                32,
                credit.take().unwrap(),
            )
            .unwrap();
        assert!(runtime.poll_one(Tick(0)).unwrap());
        let committed = runtime.committed_batches(actor, None, 4).unwrap()[0].1;
        drop(ingress.reserve_read(key).await.unwrap());
        ingress
            .accept(
                key,
                ObservationEvent::Fact {
                    source_revision: None,
                    fact: ObservationFact::Bookmark {
                        generation: 1,
                        sequence: 2,
                        covered_prefix: 2,
                        source_cut: 2,
                    },
                },
                32,
                ingress.reserve_read(key).await.unwrap(),
            )
            .unwrap();
        let mut after = None;
        let queued = loop {
            let originals = runtime.standing_input_inventory(actor, after, 8).unwrap();
            if let Some(original) = originals
                .iter()
                .find(|original| original.standing_input_id == ordinary_id)
            {
                break original
                    .in_flight
                    .expect("same queued original read progress");
            }
            after = Some(
                originals
                    .last()
                    .expect("the exact original Ordinary role remains registered")
                    .standing_input_id,
            );
        };
        observation = Some((ingress.clone(), owner, positions, key, committed, queued));
        ordered = Some(OrderedObservationHost::new(ingress).with_read_progress([progress]));
    }
    let consumer = ConsumerId::new(1).unwrap();
    let report = ReportSlot::reserve_next(
        &mut capacity,
        initial,
        1,
        [(consumer, ConsumerKind::Observer)],
    )
    .unwrap();
    let supervisor = FailureSupervisor::new(
        runtime.clone(),
        registration.clone(),
        ReportedWork::try_new(work, capacity, report).unwrap(),
        consumer,
        tokio::runtime::Handle::current(),
    );
    let cleanup = Arc::new(Mutex::new(None));
    let unresolved = AttemptActionId::new(NonZeroU64::new(7).unwrap());
    let (release, wait) = tokio::sync::oneshot::channel::<()>();
    let (reached, mut reached_cleanup) = tokio::sync::oneshot::channel();
    let executor = Arc::new(HoldingExecutor::default());
    let host = HostRegistry::new(
        runtime.clone(),
        ledger.clone(),
        Arc::new(FrontendHostObservations::new(Arc::new(FixedClock))),
    );
    let ap_exit = host
        .register(HostBinding {
            scope: scope.clone(),
            registration: registration.clone(),
            kind: FrontendActorKind::Test,
            config,
            policy: FrontendLanePolicy::attempt(1),
            source: Box::new(SupervisorSource {
                cleanup: cleanup.clone(),
                setup: Some((
                    supervisor,
                    context,
                    Box::new(HoldCleanup {
                        reached,
                        release: wait,
                        aborts: Vec::new(),
                    }),
                )),
                running: None,
                unresolved,
            }),
            executor: executor.clone(),
            transport: None,
            startup: None,
            capacity: None,
            membership: None,

            local_exit: None,
            ordered_observations: ordered,
        })
        .unwrap_or_else(|_| panic!("original AP host registration"));
    let actual_ap_exit = ap_exit.observe_actual_exit();
    let logical_indexed = Arc::new(Mutex::new(Vec::new()));
    let logical_exit = host
        .register(HostBinding {
            scope: bridge_scope.clone(),
            registration: logical.clone(),
            kind: FrontendActorKind::Test,
            config,
            policy: FrontendLanePolicy::logical(1),
            source: Box::new(LogicalReceivingSource {
                scope: bridge_scope.clone(),
                indexed: logical_indexed.clone(),
                closing: false,
            }),
            executor: executor.clone(),
            transport: None,
            ordered_observations: None,
            startup: None,
            capacity: None,
            membership: None,
            local_exit: None,
        })
        .unwrap_or_else(|_| panic!("same LP receiving host registration"));
    // Drain the original LP fact receipt while retaining the SAME AP Host
    // claim: its committed/queued observation originals belong to failure
    // cleanup, and must not be normally retired before the Core fault.
    let mut held_ap_turn = None;
    while let Some(turn) = runtime.claim_host_actor() {
        if already_published && turn.registration().same_registration(&registration) {
            assert!(held_ap_turn.replace(turn).is_none());
        } else {
            host.drive(Tick(0), turn);
        }
    }
    assert_eq!(held_ap_turn.is_some(), already_published);
    assert_eq!(
        logical_indexed.lock().unwrap().len(),
        usize::from(already_published)
    );
    assert!(!*actual_ap_exit.borrow());
    let failed = runtime
        .reserve_input(actor, actor, InputClass::Lifecycle)
        .unwrap()
        .fill(FrontendInput::Test(CLEANUP_FENCE))
        .unwrap();
    assert!(runtime.poll_one(Tick(1)).unwrap());
    if let Some((_, _, _, _, committed, queued)) = &observation {
        use novarocks_control_runtime::actor_turn::InputPhase;
        assert_eq!(
            runtime
                .input_custody(actor, *committed)
                .unwrap()
                .unwrap()
                .phase,
            InputPhase::Committed
        );
        assert_eq!(
            runtime
                .input_custody(actor, *queued)
                .unwrap()
                .unwrap()
                .phase,
            InputPhase::Filled
        );
    }
    let failed_turn = held_ap_turn
        .take()
        .unwrap_or_else(|| runtime.claim_host_actor().unwrap());
    assert!(failed_turn.registration().same_registration(&registration));
    host.drive(Tick(1), failed_turn);
    let (supervisor, triggering) = cleanup.lock().unwrap().unwrap();
    assert_eq!(triggering, failed);
    let lost = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            facts.progress(3, false, false).unwrap();
            while runtime.poll_one(Tick(2)).unwrap() {}
            while let Some(turn) = runtime.claim_host_actor() {
                host.drive(Tick(2), turn);
            }
            let selected = logical_seen
                .lock()
                .unwrap()
                .iter()
                .find_map(|input| match input {
                    FrontendInput::Logical(input) => match &**input {
                        LogicalInput::AttemptPort(message)
                            if matches!(message.body, AttemptToLogicalBody::AttemptLost { .. }) =>
                        {
                            Some(message.clone())
                        }
                        _ => None,
                    },
                    _ => None,
                });
            if let Some(original) = selected {
                break original;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(lost.activation, activation);
    assert!(matches!(&lost.body, AttemptToLogicalBody::AttemptLost {
        failure, unknown_participations, unresolved_actions,
    } if failure.input() == &novarocks_attempt_protocol::port_projection::owner_failure()
        && **unknown_participations == [participation] && **unresolved_actions == [unresolved]));
    if original_prior.is_some() {
        let seen = logical_seen.lock().unwrap();
        let originals = seen
            .iter()
            .filter_map(|input| match input {
                FrontendInput::Logical(input) => match &**input {
                    LogicalInput::AttemptPort(message) => Some(message),
                    _ => None,
                },
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            originals.len(),
            2,
            "prior fact and failure are distinct original inputs"
        );
        assert!(
            matches!(&originals[0].body, AttemptToLogicalBody::ConvergenceProgress {
            local_exited: false, closure: None, lifecycle_runtime_filters: Some(original),
        } if Arc::ptr_eq(original, &prior_filters))
        );
        assert!(matches!(
            originals[1].body,
            AttemptToLogicalBody::AttemptLost { .. }
        ));
    }
    assert!(matches!(
        runtime.retire_input(actor, supervisor),
        Err(ActorTurnError::EffectsRemain)
    ));
    runtime.notify_host_actor(&registration).unwrap();
    while let Some(turn) = runtime.claim_host_actor() {
        host.drive(Tick(2), turn);
    }
    assert_eq!(
        logical_indexed.lock().unwrap().len(),
        1 + usize::from(already_published)
    );
    assert!(executor.jobs.lock().unwrap().is_empty());
    if observation.is_none() {
        assert!(runtime.claim_host_actor().is_none());
    }
    assert!(!host.is_empty());
    assert!(matches!(
        reached_cleanup.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    late_start.record(late_participation);
    drop(late_start);
    let (original, aborts) =
        tokio::time::timeout(std::time::Duration::from_secs(1), reached_cleanup)
            .await
            .unwrap()
            .unwrap();
    assert!(
        original.root_reply.is_none(),
        "failure publication invents no Root acknowledgement"
    );
    original.publication.unwrap();
    original.observation_drain.unwrap();
    if let Some((ingress, mut owner, mut positions, key, committed, queued)) = observation {
        while let Some(turn) = runtime.claim_host_actor() {
            host.drive(Tick(2), turn);
        }
        assert!(runtime.input_custody(actor, committed).unwrap().is_none());
        assert!(runtime.input_custody(actor, queued).unwrap().is_none());
        ingress.job_closed(key).unwrap();
        ingress.retire_stream(key).unwrap();
        ingress.close_after_exit().unwrap();
        owner.body_exited().unwrap();
        owner.captures_exited().unwrap();
        positions.finish().unwrap();
        scope.reap_ready(8).unwrap();
    }
    assert_eq!(
        original.core_panic.downcast_ref::<&str>().copied(),
        Some("fixture failure after original reply admission")
    );
    let mut expected = [participation, late_participation];
    expected.sort();
    assert_eq!(&*original.unknown_participations, &expected);
    assert!(
        aborts
            .iter()
            .map(|request| request.participation())
            .eq(expected)
    );
    assert!(matches!(
        runtime.retire_input(actor, supervisor),
        Err(ActorTurnError::EffectsRemain)
    ));
    if let Some(role) = role.as_mut() {
        role.close_after_exit().unwrap();
    }
    let proof = runtime.failure_proof_for(&registration).unwrap();
    assert!(reader.close_after_failure(&runtime, &proof).unwrap());
    assert!(
        roots.close_after_exit().is_ok(),
        "no pending Root request is synthesized"
    );
    let mut abort_roles = endpoints.abort_retirement();
    let mut abort_closed = false;
    for _ in 0..8 {
        if matches!(
            abort_roles.progress().unwrap(),
            AbortIngressRetirementProgress::Complete
        ) {
            abort_closed = true;
            break;
        }
    }
    assert!(abort_closed);
    startup_source.progress(true, true).unwrap();
    assert!(startup_source.is_closed());
    release.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !*actual_ap_exit.borrow() {
            if let Some(turn) = runtime.claim_host_actor() {
                host.drive(Tick(3), turn);
            } else {
                tokio::task::yield_now().await;
            }
        }
    })
    .await
    .unwrap();
    assert!(
        *actual_ap_exit.borrow(),
        "actual failure supervisor and AP Host exited before producer proof"
    );
    assert!(
        !host.is_empty(),
        "the original LP receiving Source remains alive until bank and Work retirement"
    );
    endpoints.original_producer_exited();
    requests.close_admission();
    facts.close_admission();
    let mut bank_closed = false;
    for _ in 0..8 {
        while runtime.poll_one(Tick(3)).unwrap() {}
        bank_closed = facts.progress(3, true, false).unwrap().complete;
        if bank_closed {
            break;
        }
    }
    assert!(bank_closed && facts.is_closed());
    drop((endpoints, reader, startup_source, roots, requests, facts));
    bridge_work.body_exited().unwrap();
    bridge_work.captures_exited().unwrap();
    bridge_capacity.finish().unwrap();
    bridge_scope.close_external().unwrap();
    ledger.reap_scope_ready(bridge_scope.id(), 8).unwrap();
    logical_exit.request_close().unwrap();
    while let Some(turn) = runtime.claim_host_actor() {
        host.drive(Tick(3), turn);
    }
    assert!(
        *logical_exit.observe_actual_exit().borrow(),
        "same receiving Actor and Scope actually removed"
    );
    assert!(host.is_empty() && runtime.is_empty() && ledger.is_empty());
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

#[test]
fn original_source_retained_binding_publishes_and_completes_only_metadata_without_dispatching_or_exiting_work()
 {
    let f = Fixture::new(vec![vec![(1, 7, vec![]), (3, 7, vec![1])]], &[]);
    f.bank.lock().unwrap().retain_binding = true;
    f.executor.refusing.store(true, Ordering::Release);
    f.send(0);
    let (_, idle) = f.to_idle();
    let (observed, dispatched) = f.captured();
    let (not_exited, published, captures_before_cleanup, retained) = {
        let bank = f.bank.lock().unwrap();
        (
            bank.originals
                .values()
                .all(|original| !original.custody.actual_exit_ready().unwrap()),
            *bank.publication.read(),
            f.captures.load(Ordering::SeqCst),
            bank.retained_jobs.len(),
        )
    };
    let states: Vec<_> = f
        .runtime
        .committed_batches(f.registration.actor(), None, usize::MAX)
        .unwrap()
        .into_iter()
        .flat_map(|(_, input)| {
            f.runtime
                .effect_inventory(f.registration.actor(), input, None, f.max_effects)
                .unwrap()
        })
        .map(|(_, state)| state)
        .collect();
    let actual_cleanup = f.finish(false);
    assert!(idle && dispatched.is_empty());
    assert!(
        not_exited,
        "metadata completion is not original physical exit"
    );
    assert_eq!(
        published, 3,
        "the real second dependent publication committed"
    );
    assert_eq!(
        captures_before_cleanup, 0,
        "publication is not capture destruction"
    );
    assert_eq!(
        observed.bound,
        vec![1, 3],
        "same-lane dependent ticket becomes eligible after actual metadata completion"
    );
    assert!(observed.origins.iter().all(|same| *same));
    assert_eq!(retained, 2);
    assert!(states.iter().all(|state| *state == EffectState::Completed));
    assert_eq!(
        actual_cleanup,
        (2, true),
        "only explicit original capture/unused-exit/role/header cleanup retires the owners"
    );
}
