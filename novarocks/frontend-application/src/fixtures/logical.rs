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

//! Shared original assembly. Source unit tests and the two-owner fixture use
//! this single construction path; backends supply work, never LP decisions.
#[path = "attempt.rs"]
mod attempt;
#[path = "process.rs"]
mod process;
#[path = "scripts.rs"]
mod scripts;
use super::{
    adapter::{LogicalActor, LogicalHostEffect},
    application::LogicalHostReplies,
    failure::{LogicalFailureOriginals, LogicalInfrastructureFailure},
    geometry::LogicalGeometry,
    input_bank::LogicalInputBank,
    inputs::LogicalBootstrapPublisher,
    installed::LogicalInstalledState,
    links::LogicalAttemptLinks,
    work::LogicalJobExecutor,
};
use crate::control_runtime::{
    actor::{FrontendActor, FrontendActorKind, FrontendEffect, FrontendInput},
    host::{FrontendLanePolicy, HostBinding, HostRegistry},
};
pub(crate) use attempt::{AttemptCase, AttemptScript};
use novarocks_control_runtime::{
    actor_turn::{ActorRegistration, ActorRuntime, Clock},
    history::HistoryCapture,
    transaction::{RuntimeLedger, ScopeHandle},
};
use novarocks_control_runtime_core::{
    identity::{ScopeId, Tick},
    mailbox::InputClass,
    owned_storage::OwnedSlotNeed,
    scope::WorkKind,
};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, LogicalExecutionActorId, QueryExecutionError,
};
use novarocks_logical_execution_protocol::{effect::LogicalEffectBody, start::LogicalStart};
use novarocks_query_application::coordination::executor::{
    GovernanceExecutor, GovernanceObligationSnapshot,
};
pub(crate) use scripts::{LogicalScript, SuccessorScript, participations};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Wake, Waker},
    time::Duration,
};

pub(in crate::control_runtime::logical) struct LogicalParts {
    pub(in crate::control_runtime::logical) registration: ActorRegistration,
    pub(in crate::control_runtime::logical) geometry: LogicalGeometry,
    pub(in crate::control_runtime::logical) state: Arc<LogicalInstalledState>,
    pub(in crate::control_runtime::logical) failure: Arc<LogicalInfrastructureFailure>,
    pub(in crate::control_runtime::logical) inputs: Arc<LogicalInputBank>,
    pub(in crate::control_runtime::logical) links: Arc<LogicalAttemptLinks>,
    pub(in crate::control_runtime::logical) bootstrap: Arc<LogicalBootstrapPublisher>,
    pub(in crate::control_runtime::logical) wake: Waker,
    pub(in crate::control_runtime::logical) scope: ScopeHandle,
}

#[derive(Clone)]
pub(crate) struct LogicalCase {
    pub(crate) start: LogicalStart,
    pub(crate) script: LogicalScript,
    pub(crate) registration: ActorRegistration,
    parts: Arc<LogicalParts>,
    governor: std::sync::Weak<GovernanceExecutor>,
    gate: Arc<process::TurnGate>,
}
#[derive(Clone, Copy)]
pub(crate) enum ProgressSelection {
    All,
    ExceptLogical(LogicalExecutionActorId),
}
#[derive(Clone, Copy)]
pub(crate) struct ProgressBudget {
    pub(crate) actors: usize,
    pub(crate) hosts: usize,
    pub(crate) timers: usize,
}
impl Default for ProgressBudget {
    fn default() -> Self {
        Self {
            actors: 64,
            hosts: 64,
            timers: 64,
        }
    }
}
#[derive(Default, Debug)]
pub(crate) struct ProgressObservation {
    pub(crate) actors: usize,
    pub(crate) hosts: usize,
    pub(crate) held_logical: bool,
}
pub(crate) struct LogicalObservation {
    pub(crate) effects: Vec<LogicalEffectBody>,
    pub(crate) received: Vec<novarocks_coordination_contract::logical_port::LogicalToAttempt>,
    pub(crate) governor: Vec<GovernanceObligationSnapshot>,
    pub(crate) failed: bool,
    pub(crate) exited: bool,
    pub(crate) geometry_mailbox: [usize; 4],
    pub(crate) error: Option<QueryExecutionError>,
    pub(crate) registration: ActorRegistration,
    pub(crate) scope: ScopeId,
    pub(crate) request_occupied: bool,
    pub(crate) fact_occupied: bool,
    pub(crate) native_cleanup: Vec<novarocks_logical_execution_protocol::effect::NativeEffect>,
}
struct FixtureState {
    process: process::Process,
    logical: Vec<LogicalCase>,
    attempts: Vec<AttemptCase>,
    worker: Option<
        std::thread::JoinHandle<
            Result<bool, novarocks_control_runtime::actor_turn::ActorTurnError>,
        >,
    >,
    worker_failures: Vec<Box<dyn std::any::Any + Send>>,
}
pub(crate) struct FrontendFixture {
    original: Option<Box<FixtureState>>,
    finished: bool,
}
impl FrontendFixture {
    pub(crate) fn new() -> Self {
        Self {
            original: Some(Box::new(FixtureState {
                process: process::Process::new(),
                logical: Vec::new(),
                attempts: Vec::new(),
                worker: None,
                worker_failures: Vec::new(),
            })),
            finished: false,
        }
    }
    fn state(&self) -> &FixtureState {
        self.original.as_ref().unwrap()
    }
    fn state_mut(&mut self) -> &mut FixtureState {
        self.original.as_mut().unwrap()
    }
    pub(crate) fn runtime(&self) -> &ActorRuntime<FrontendActor> {
        &self.state().process.original.runtime
    }
    pub(crate) fn ledger(&self) -> &RuntimeLedger {
        &self.state().process.original.ledger
    }
    pub(crate) fn history(&self) -> HistoryCapture {
        self.state().process.history.clone()
    }
    pub(crate) fn advance_time(&self, delta: Duration) {
        self.state().process.clock.advance(delta);
    }
    pub(crate) fn install_logical(
        &mut self,
        start: LogicalStart,
        script: LogicalScript,
    ) -> LogicalCase {
        let gate = Arc::new(process::TurnGate::default());
        let governor = scripts::governor(&start, self.state().process.workload.as_ref().unwrap());
        let p = &self.state().process;
        let parts = super::source::assemble_logical(
            &p.original.runtime,
            &p.original.ledger,
            &p.original.host,
            &start,
            {
                let gate = gate.clone();
                let panic = script.core_panic.clone();
                move |original| {
                    FrontendActor::Test(Box::new(process::PausedLogicalActor {
                        original,
                        gate,
                        panic,
                    }))
                }
            },
            |parts| {
                Arc::new(scripts::ScriptExecutor::new(
                    parts,
                    script.clone(),
                    governor.clone(),
                    start.clone(),
                    p.clock.clone(),
                ))
            },
        );
        let case = LogicalCase {
            registration: parts.registration.clone(),
            start,
            script,
            parts: Arc::new(parts),
            governor: Arc::downgrade(&governor),
            gate,
        };
        self.state_mut().logical.push(case.clone());
        case
    }
    pub(crate) async fn activate_attempt(
        &mut self,
        logical: &LogicalCase,
        activation: AttemptActivationIdentity,
        script: AttemptScript,
    ) -> AttemptCase {
        let mut original = None;
        for _ in 0..1024 {
            if let Some(loan) = logical.parts.links.borrow(activation) {
                original = Some(loan);
                break;
            }
            self.progress(ProgressSelection::All, ProgressBudget::default())
                .await;
        }
        let loan =
            original.expect("actual ActivateAttempt must issue its original link before binding");
        let participations = if activation == logical.start.first_attempt.activation {
            Arc::from(logical.start.first_attempt.participations.as_ref())
        } else {
            logical
                .script
                .prepared_roster(activation.execution())
                .expect("same original prepared successor roster")
        };
        let input_loan = logical
            .parts
            .inputs
            .loan()
            .await
            .expect("same original input inventory is live for issued activation");
        let p = &self.state().process.original;
        let case = attempt::AttemptBackend::activate(
            attempt::AttemptContext {
                runtime: p.runtime.clone(),
                ledger: p.ledger.clone(),
                host: p.host.clone(),
                logical: logical.registration.clone(),
                inputs: logical.parts.inputs.clone(),
                links: logical.parts.links.clone(),
                loan,
                activation,
                logical_wake: logical.parts.wake.clone(),
                participations,
                input_loan,
            },
            script,
        );
        self.state_mut().attempts.push(case.clone());
        // Startup-ready is not yet LP Running. Wait for the actual Core-issued
        // Run action after SAME activation result consumption; otherwise an
        // early Completed fact is legally ignored while activation is pending.
        for _ in 0..1024 {
            self.progress(ProgressSelection::All, ProgressBudget::default())
                .await;
            if logical.script.effects().iter().any(|effect|matches!(effect,
                LogicalEffectBody::Native(novarocks_logical_execution_protocol::effect::NativeEffect::RunAttempt { activation:actual }) if *actual==activation)) {
                return case;
            }
        }
        panic!(
            "same original activation did not reach its actual Core-issued Run action: effects={:?}; failure={:?}; lp={:?}; ap={:?}; received={:?}",
            logical.script.effects(),
            self.observe(logical).error,
            self.runtime().snapshot(logical.registration.actor()),
            self.runtime().snapshot(case.registration.actor()),
            case.script.received(),
        );
    }
    fn release_worker(&mut self) {
        for case in &self.state().logical {
            case.gate.release();
        }
        if let Some(worker) = self.state_mut().worker.take() {
            match worker.join() {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => self.state_mut().worker_failures.push(Box::new(error)),
                Err(original) => self.state_mut().worker_failures.push(original),
            }
        }
    }
    pub(crate) async fn progress(
        &mut self,
        selection: ProgressSelection,
        budget: ProgressBudget,
    ) -> ProgressObservation {
        let mut result = ProgressObservation::default();
        let now = self.state().process.now();
        self.state().process.history.observe_time(now);
        self.runtime().advance_time(now, budget.timers).unwrap();
        match selection {
            ProgressSelection::All => self.release_worker(),
            ProgressSelection::ExceptLogical(actor) => {
                let target = self
                    .state()
                    .logical
                    .iter()
                    .find(|l| l.start.actor_instance == actor)
                    .expect("original installed logical")
                    .gate
                    .clone();
                if self
                    .state()
                    .logical
                    .iter()
                    .any(|l| l.start.actor_instance != actor && l.gate.entered())
                {
                    self.release_worker();
                }
                target.pause();
                // Each worker performs exactly one original poll. A held Join
                // is retained only after the target's actual handle entered.
                for _ in 0..budget.actors {
                    if target.entered() {
                        break;
                    }
                    if let Some(worker) = self.state().worker.as_ref()
                        && !worker.is_finished()
                    {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    if let Some(worker) = self.state_mut().worker.take() {
                        worker
                            .join()
                            .expect("actual selector Join")
                            .expect("original selected poll");
                    }
                    if result.hosts < budget.hosts
                        && let Some(turn) = self.runtime().claim_host_actor()
                    {
                        self.state().process.original.host.drive(now, turn);
                        result.hosts += 1;
                    }
                    let runtime = self.runtime().clone();
                    let now = self.state().process.now();
                    self.state_mut().worker =
                        Some(std::thread::spawn(move || runtime.poll_one(now)));
                    result.actors += 1;
                    tokio::task::yield_now().await;
                }
                result.held_logical = target.entered();
                if !result.held_logical {
                    return result;
                }
            }
        }
        let p = &self.state().process;
        assert!(
            self.state().worker_failures.is_empty(),
            "actual selective-poll worker failed"
        );
        for _ in result.actors..budget.actors {
            if !p.original.runtime.poll_one(now).unwrap() {
                break;
            }
            result.actors += 1;
        }
        for _ in result.hosts..budget.hosts {
            let Some(turn) = p.original.runtime.claim_host_actor() else {
                break;
            };
            p.original.host.drive(now, turn);
            result.hosts += 1;
        }
        tokio::task::yield_now().await;
        result
    }
    pub(crate) fn observe(&self, logical: &LogicalCase) -> LogicalObservation {
        let script = &logical.script;
        let mut received = Vec::new();
        let mut request_occupied = false;
        let mut fact_occupied = false;
        for a in self
            .state()
            .attempts
            .iter()
            .filter(|a| a.activation.actor() == logical.start.actor_instance)
        {
            received.extend(a.script.received());
            let occupied = a.occupied();
            request_occupied |= occupied.0;
            fact_occupied |= occupied.1;
        }
        let failed = logical.parts.failure.failed();
        LogicalObservation {
            effects: script.effects(),
            received,
            governor: logical
                .governor
                .upgrade()
                .map_or_else(|| script.governor_snapshot(), |g| g.cleanup_inventory()),
            failed,
            exited: self
                .runtime()
                .snapshot(logical.registration.actor())
                .is_err(),
            geometry_mailbox: logical.parts.geometry.config.mailbox,
            error: failed.then(|| logical.parts.failure.error()),
            registration: logical.registration.clone(),
            scope: logical.parts.scope.id(),
            request_occupied,
            fact_occupied,
            native_cleanup: script.native_cleanup(),
        }
    }
    pub(crate) async fn finish(&mut self) {
        self.release_worker();
        use novarocks_logical_execution_protocol::input::RegistryClose;
        use novarocks_query_application::api::logical_runtime::{
            InstalledLogicalExecution, LogicalJoinReadiness,
        };
        use std::{
            future::Future,
            task::{Context, Poll},
        };
        // Business completion leaves the original installed owner alive. The
        // external Registry caller must request and poll its actual close
        // protocol; absence of a client is not a retirement receipt.
        let mut closes = self
            .state()
            .logical
            .iter()
            .filter(|case| !case.parts.failure.failed())
            .map(|case| {
                let state = case.parts.state.clone();
                state.request_close(RegistryClose::Retire);
                (
                    case.start.actor_instance,
                    Some(Box::pin(async move {
                        loop {
                            match state.join_readiness(true).await {
                                Ok(LogicalJoinReadiness::WaitingForConvergence) => {}
                                result => return result,
                            }
                        }
                    })),
                )
            })
            .collect::<Vec<_>>();
        for attempt in &self.state().attempts {
            if self.state().logical.iter().any(|case| {
                case.start.actor_instance == attempt.activation.actor()
                    && case.parts.failure.failed()
            }) {
                attempt.close();
            }
        }
        for _ in 0..1024 {
            for (actor, close) in &mut closes {
                if let Some(original) = close
                    && let Poll::Ready(result) = original
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                {
                    result.expect("same healthy original Registry close reply");
                    // The real close reply certifies the Core's remaining
                    // port deliveries have settled. Only now may this test
                    // Native backend stop its original AP producer.
                    for attempt in &self.state().attempts {
                        if attempt.activation.actor() == *actor {
                            attempt.close();
                        }
                    }
                    *close = None;
                }
            }
            self.progress(ProgressSelection::All, ProgressBudget::default())
                .await;
            if self.runtime().is_empty()
                && self.ledger().is_empty()
                && self.state().process.original.host.is_empty()
                && closes.iter().all(|(_, original)| original.is_none())
            {
                let workload = self
                    .state_mut()
                    .process
                    .workload
                    .take()
                    .expect("same original process Workload owner");
                workload.close_admission();
                workload.shutdown().unwrap_or_else(|failure| {
                    panic!("actual workload owner did not retire: {}", failure.error())
                });
                self.finished = true;
                return;
            }
        }
        panic!(
            "original fixture did not retire: runtime_empty={}, ledger_empty={}, host_empty={}; logical_snapshot_total_and_last_effects={:?}; attempts={:?}",
            self.runtime().is_empty(),
            self.ledger().is_empty(),
            self.state().process.original.host.is_empty(),
            self.state()
                .logical
                .iter()
                .map(|original| {
                    let mut effects = original.script.effects();
                    let total = effects.len();
                    let tail = effects.split_off(total.saturating_sub(32));
                    (
                        self.runtime().snapshot(original.registration.actor()),
                        total,
                        tail,
                    )
                })
                .collect::<Vec<_>>(),
            self.state()
                .attempts
                .iter()
                .map(|original| (
                    self.runtime().snapshot(original.registration.actor()),
                    original.script.received()
                ))
                .collect::<Vec<_>>()
        );
    }
}
impl Drop for FrontendFixture {
    fn drop(&mut self) {
        self.release_worker();
        if !self.finished {
            // An assertion is not asynchronous exit. Retain the entire same
            // unresolved owner in stable custody; never Drop fragments or turn
            // UNKNOWN into a completed Scope merely to make teardown pass.
            static UNRESOLVED: std::sync::OnceLock<Mutex<Vec<Box<dyn std::any::Any + Send>>>> =
                std::sync::OnceLock::new();
            if let Some(original) = self.original.take() {
                UNRESOLVED
                    .get_or_init(Default::default)
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .push(original);
            }
        }
    }
}

#[tokio::test]
async fn shared_original_two_owner_smoke_retires_actual_host_runtime_and_ledger() {
    use novarocks_coordination_contract::{
        AttemptConvergenceClosure,
        logical_port::{AttemptTerminal, AttemptToLogicalBody},
    };
    use novarocks_logical_execution_protocol::effect::ConsumerEffect;
    let mut fixture = FrontendFixture::new();
    let logical =
        fixture.install_logical(LogicalScript::start(9001, 0, 1), LogicalScript::default());
    let attempt = fixture
        .activate_attempt(
            &logical,
            logical.start.first_attempt.activation,
            AttemptScript::default(),
        )
        .await;
    attempt
        .script
        .enqueue(AttemptToLogicalBody::AttemptTerminal(
            AttemptTerminal::Completed,
        ));
    attempt
        .script
        .enqueue(AttemptToLogicalBody::ConvergenceProgress {
            local_exited: true,
            closure: Some(Arc::new(AttemptConvergenceClosure::Evidence(Vec::new()))),
            lifecycle_runtime_filters: None,
        });
    logical.script.run.open();
    let mut completed = false;
    for _ in 0..1024 {
        fixture
            .progress(ProgressSelection::All, ProgressBudget::default())
            .await;
        if fixture.observe(&logical).effects.iter().any(|e| {
            matches!(
                e,
                LogicalEffectBody::Consumer(ConsumerEffect::PublishCompletion)
            )
        }) {
            completed = true;
            break;
        }
    }
    assert!(
        completed,
        "real Core accepted only the explicitly supplied original terminal and closure"
    );
    fixture.finish().await;
    let history = crate::fixtures::history::CapturedHistory::drain(&fixture.history(), usize::MAX);
    history
        .check_completed_runtime(fixture.runtime(), fixture.ledger())
        .unwrap();
}
