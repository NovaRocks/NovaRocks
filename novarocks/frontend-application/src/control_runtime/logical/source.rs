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

//! Original fixed purpose roles. The host never issues work from an identity.
use super::{
    adapter::{LogicalEffectEnvelope, LogicalHostEffect},
    auxiliary_work::{AuxiliaryHeaderOwner, AuxiliaryWorkObligation, AuxiliaryWorkStart},
    failure::{
        LogicalFailureCut, LogicalFailureProgressObligation, LogicalFailureSupervisor,
        LogicalInfrastructureFailure,
    },
    funding::{LogicalFundingObligation, SourceFundingObligation, reported_step},
    geometry::{ATTEMPT_PURPOSES, LogicalGeometry},
    input_bank::LogicalInputBank,
    installed::LogicalInstalledState,
    links::LogicalAttemptLinks,
    purpose::LogicalPurposes,
    tails::LogicalTailSource,
    work::{LogicalJobExecutor, LogicalWorkObligation},
};
use crate::control_runtime::{
    actor::{FrontendActor, FrontendEffect, FrontendInput},
    host::{
        EffectBindRefusal, EffectExecutor, EffectSource, PendingHandoff, PreparedEffectJob,
        SourceProgress,
    },
};
use crate::task_execution::host::custody::HostDisposal;
use novarocks_control_runtime::{
    actor_turn::{
        ActorHostTurn, ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket,
        TimerSourceState,
    },
    causal_input::{CAUSAL_INPUT_POSITIONS, CausalInputObligation},
};
use novarocks_control_runtime::{
    exit_custody::ExitCustody, obligation::WorkRetirementObserver, reported_work::ReportedWork,
};
use novarocks_control_runtime_core::{
    identity::{InputId, Tick},
    mailbox::InputClass,
    owned_storage::OwnedSlotNeed,
    scope::WorkKind,
};
use novarocks_logical_execution_protocol::{
    effect::LogicalWorkKind, input::LogicalInput, start::LogicalStart,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use std::{
    collections::VecDeque,
    sync::Arc,
    task::{Context, Waker},
};
use tokio::runtime::Handle;

type Position = CausalInputObligation<FrontendActor>;
struct Purpose {
    author: Option<SourceFundingObligation>,
    result: Option<Position>,
    exit: Option<Position>,
    job: Option<Arc<LogicalWorkObligation>>,
    exit_filled: bool,
    positions_closed: bool,
    relay_start: Option<novarocks_coordination_contract::AttemptActivationIdentity>,
    cut_checked: bool,
    activation_link: Option<usize>,
}
impl Purpose {
    fn idle(&self) -> bool {
        self.job.is_none() && self.result.is_some()
    }
}

pub(super) struct LogicalEffectSource {
    registration: ActorRegistration,
    progress_requested: Arc<AtomicBool>,
    purposes: Vec<Purpose>,
    routing: LogicalPurposes,
    pending: Option<usize>,
    cursor: usize,
    remaining: usize,
    pass_active: bool,
    input_pass_done: bool,
    tail_pass_done: bool,
    tails_retired: bool,
    pending_headers: VecDeque<Arc<LogicalWorkObligation>>,
    header_remaining: usize,
    header_limit: usize,
    rescan_requested: bool,
    first_family: usize,
    live_authors: usize,
    live_jobs: usize,
    marker_closed: bool,
    inputs_retired: bool,
    closed: bool,
    failure: Arc<LogicalInfrastructureFailure>,
    executor: Option<Arc<dyn LogicalJobExecutor>>,
    tails: Option<LogicalTailSource>,
    runtime: Handle,
    inputs: Arc<LogicalInputBank>,
    state: Arc<LogicalInstalledState>,
    links: Arc<LogicalAttemptLinks>,
    funding: LogicalFundingObligation,
    failure_work: Option<ReportedWork>,
    failure_ticket: Option<Arc<EffectTicket<FrontendEffect>>>,
    failure_cut: Option<LogicalFailureCut>,
    failure_job: Option<Arc<AuxiliaryWorkObligation>>,
    failure_progress: Option<Arc<LogicalFailureProgressObligation>>,
    owner_failure_observer:
        Option<novarocks_query_application::coordination::executor::LogicalOwnerFailureObserver>,
    failure_ticket_completed: bool,
    failure_budget: std::time::Duration,
    disposal_work: Option<ReportedWork>,
    unused_failure: Option<(ExitCustody, WorkRetirementObserver)>,
    failure_retired: bool,
    marker_author: Option<SourceFundingObligation>,
    disposal: Option<(HostDisposal<LogicalFinalCaptures>, WorkRetirementObserver)>,
    originals_disposed: bool,
    startup_cursor: u8,
    observe_client: bool,
    bootstrap_purpose: usize,
    client_purpose: usize,
    wake: Waker,
}
impl LogicalEffectSource {
    pub(super) fn reserve(build: LogicalSourceAssembly<'_>) -> Self {
        let LogicalSourceAssembly {
            runtime,
            registration,
            funding,
            geometry,
            start,
            failure,
            executor,
            wake,
            data_runtime,
            inputs,
            state,
            links,
            failure_work,
            disposal_work,
            marker_author,
            progress_requested,
        } = build;
        let purposes = (0..geometry.purposes)
            .map(|index| {
                let guard = funding
                    .reserve_step(
                        WorkKind::Settlement,
                        OwnedSlotNeed {
                            inputs: 2 * CAUSAL_INPUT_POSITIONS,
                            work_headers: 1,
                            ..Default::default()
                        },
                    )
                    .expect("original logical purpose inventory");
                let author = SourceFundingObligation::start(guard);
                let source = geometry
                    .source(index)
                    .expect("checked original logical source geometry");
                let reserve = || {
                    CausalInputObligation::reserve_admitted(
                        runtime,
                        &registration,
                        source,
                        InputClass::Lifecycle,
                        author.work(),
                    )
                    .expect("prepaid logical purpose position")
                };
                Purpose {
                    result: Some(reserve()),
                    exit: Some(reserve()),
                    author: Some(author),
                    job: None,
                    exit_filled: false,
                    positions_closed: false,
                    relay_start: None,
                    cut_checked: false,
                    activation_link: None,
                }
            })
            .collect();
        let tails = LogicalTailSource::reserve(
            executor.native_executor(),
            &funding,
            geometry.attempts,
            runtime.clone(),
            registration.clone(),
            data_runtime.clone(),
            wake.clone(),
            failure.clone(),
        );
        let header_limit = geometry
            .config
            .max_batches
            .checked_mul(geometry.config.max_effects_per_step)
            .and_then(|n| n.checked_add(geometry.purposes))
            .expect("original logical header inventory bound");
        let owner_failure_observer = executor
            .native_executor()
            .map(|native| native.owner_failure_observer());
        Self {
            registration,
            progress_requested,
            purposes,
            pending_headers: VecDeque::new(),
            header_remaining: 0,
            header_limit,
            tails: Some(tails),
            executor: Some(executor),
            runtime: data_runtime,
            inputs,
            state,
            links,
            bootstrap_purpose: geometry.attempts * ATTEMPT_PURPOSES + 14,
            client_purpose: geometry.attempts * ATTEMPT_PURPOSES + 16,
            observe_client: start.policy.output
                == novarocks_logical_execution_protocol::start::LogicalOutputMode::ResultStream,
            startup_cursor: 0,
            funding,
            failure_work: Some(failure_work),
            failure_ticket: None,
            failure_cut: None,
            failure_job: None,
            failure_progress: None,
            owner_failure_observer,
            failure_ticket_completed: false,
            failure_budget: start.budgets.remote_cleanup_timeout,
            disposal_work: Some(disposal_work),
            unused_failure: None,
            failure_retired: false,
            marker_author: Some(marker_author),
            disposal: None,
            originals_disposed: false,
            routing: LogicalPurposes::new(start),
            pending: None,
            cursor: 0,
            remaining: 0,
            pass_active: false,
            input_pass_done: false,
            tail_pass_done: false,
            tails_retired: false,
            rescan_requested: false,
            first_family: 0,
            live_authors: geometry.purposes,
            live_jobs: 0,
            marker_closed: false,
            inputs_retired: false,
            closed: false,
            failure,
            wake,
        }
    }
    fn launch_original(&mut self, key: usize, body: LogicalHostEffect) {
        let purpose = &mut self.purposes[key];
        assert!(purpose.idle(), "same original logical service purpose");
        let original = self
            .funding
            .reserve_reported(WorkKind::Settlement, 0)
            .expect("original logical service work");
        let reservation = original.reservation_id();
        let (owner, exit) =
            LogicalWorkObligation::prepare(original, self.wake.clone(), self.failure.reporter());
        purpose.job = Some(owner.clone());
        self.live_jobs += 1;
        let job = PreparedEffectJob {
            effect: FrontendEffect::Logical(Arc::new(LogicalEffectEnvelope { body, reservation })),
            exit,
            result: purpose.result.take().unwrap(),
            captures: Box::new(owner.clone()),
            transport: None,
        };
        owner.launch(
            &self.runtime,
            job,
            self.executor
                .as_ref()
                .expect("live original logical executor")
                .clone(),
        );
        self.rescan_requested = true;
    }
    fn retain(&self, original: Box<dyn std::any::Any + Send>) {
        self.failure.retain(original);
    }
}

struct LogicalFinalCaptures {
    _executor: Arc<dyn LogicalJobExecutor>,
    _tails: LogicalTailSource,
}

pub(super) struct LogicalSourceAssembly<'a> {
    pub(super) runtime: &'a ActorRuntime<FrontendActor>,
    pub(super) registration: ActorRegistration,
    pub(super) progress_requested: Arc<AtomicBool>,
    pub(super) funding: LogicalFundingObligation,
    pub(super) geometry: &'a LogicalGeometry,
    pub(super) start: &'a LogicalStart,
    pub(super) failure: Arc<LogicalInfrastructureFailure>,
    pub(super) executor: Arc<dyn LogicalJobExecutor>,
    pub(super) wake: Waker,
    pub(super) data_runtime: Handle,
    pub(super) inputs: Arc<LogicalInputBank>,
    pub(super) state: Arc<LogicalInstalledState>,
    pub(super) links: Arc<LogicalAttemptLinks>,
    pub(super) failure_work: ReportedWork,
    pub(super) disposal_work: ReportedWork,
    pub(super) marker_author: SourceFundingObligation,
}

pub(super) struct LogicalEffectExecutor {
    pub(super) runtime: Handle,
    pub(super) original: std::sync::Weak<dyn LogicalJobExecutor>,
}
impl EffectExecutor for LogicalEffectExecutor {
    fn handoff(&self, job: PreparedEffectJob) -> Result<(), PreparedEffectJob> {
        let Some(owner) = job
            .captures
            .downcast_ref::<Arc<LogicalWorkObligation>>()
            .cloned()
        else {
            return Err(job);
        };
        let Some(original) = self.original.upgrade() else {
            return Err(job);
        };
        owner.launch(&self.runtime, job, original);
        Ok(())
    }
}
impl EffectSource for LogicalEffectSource {
    fn prepay_result(&mut self, effect: &FrontendEffect) -> Option<Position> {
        let FrontendEffect::Logical(effect) = effect else {
            self.retain(Box::new(effect.clone()));
            return None;
        };
        assert!(self.pending.is_none(), "one original logical prepayment");
        // A result role can be reused after its real job/report retires while
        // an earlier input batch still pins that job's metadata header. Keep
        // that exact header separately; its bounded credit precedes handoff.
        if self.pending_headers.len() + self.live_jobs >= self.header_limit {
            return None;
        }
        if let LogicalHostEffect::Protocol(effect) = &effect.body
            && let novarocks_logical_execution_protocol::effect::LogicalEffectBody::Native(
                novarocks_logical_execution_protocol::effect::NativeEffect::ActivateAttempt {
                    activation,
                    ..
                },
            ) = &effect.body
        {
            let mut inputs = self.inputs.try_loan()?;
            if let Err(original) = inputs.bind_data_activation(*activation) {
                self.retain(Box::new(original));
                return None;
            }
        }
        let Some(key) = self.routing.effect(&effect.body) else {
            self.retain(Box::new(effect.clone()));
            return None;
        };
        let purpose = &mut self.purposes[key];
        if !purpose.idle() {
            return None;
        }
        self.pending = Some(key);
        purpose.result.take()
    }
    fn bind_job(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        turn: &ActorHostTurn<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        result: Position,
    ) -> PreparedEffectJob {
        self.try_bind_job(runtime, turn, ticket, result)
            .unwrap_or_else(|original| std::panic::resume_unwind(Box::new(original)))
    }
    fn try_bind_job(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        _: &ActorHostTurn<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
        result: Position,
    ) -> Result<PreparedEffectJob, EffectBindRefusal> {
        let key = self
            .pending
            .take()
            .expect("same original logical prepayment");
        let FrontendEffect::Logical(effect) = &ticket.effect else {
            unreachable!("preflighted logical effect");
        };
        let guard = match runtime.take_step_reservation(ticket, effect.reservation) {
            Ok(guard) => guard,
            Err(error) => {
                return Err(EffectBindRefusal {
                    error,
                    original: Box::new((key, result)),
                });
            }
        };
        let kind = match &effect.body {
            LogicalHostEffect::JoinReadiness(_)
            | LogicalHostEffect::Bootstrap
            | LogicalHostEffect::ClientCloseObservation
            | LogicalHostEffect::PortDelivered(_) => WorkKind::Settlement,
            LogicalHostEffect::Protocol(effect) => match effect.work_kind() {
                LogicalWorkKind::Business => WorkKind::Business,
                LogicalWorkKind::Settlement => WorkKind::Settlement,
                LogicalWorkKind::Cleanup => WorkKind::Cleanup,
            },
        };
        let body = if let LogicalHostEffect::Protocol(effect) = &effect.body
            && let novarocks_logical_execution_protocol::effect::LogicalEffectBody::AttemptPort(
                message,
            ) = &effect.body
        {
            match self
                .executor
                .as_ref()
                .expect("live original logical executor")
                .deliver_port_now(runtime, ticket, (**message).clone())
            {
                Some(Ok(())) => {}
                Some(Err(original)) => self.failure.retain_typed(original),
                None => self
                    .failure
                    .retain(Box::new("logical port has no original registered receiver")),
            }
            LogicalHostEffect::PortDelivered(message.activation)
        } else {
            effect.body.clone()
        };
        let original = reported_step(guard, kind);
        if let LogicalHostEffect::Protocol(effect) = &effect.body
            && let novarocks_logical_execution_protocol::effect::LogicalEffectBody::Native(
                novarocks_logical_execution_protocol::effect::NativeEffect::ActivateAttempt {
                    activation,
                    ..
                },
            ) = &effect.body
        {
            let index = self
                .routing
                .activation(*activation)
                .expect("preflighted activation row");
            if let Err(original) = self.links.issue(index, *activation, &original) {
                self.failure.retain(Box::new(original));
            }
            self.purposes[key].activation_link = Some(index);
        }
        let (owner, exit) =
            LogicalWorkObligation::prepare(original, self.wake.clone(), self.failure.reporter());
        assert!(self.purposes[key].job.replace(owner.clone()).is_none());
        self.live_jobs += 1;
        self.purposes[key].relay_start = match &effect.body {
            LogicalHostEffect::Protocol(effect) => match effect.body {
                novarocks_logical_execution_protocol::effect::LogicalEffectBody::Relay(
                    novarocks_logical_execution_protocol::effect::RelayEffect::Start { activation },
                ) => Some(activation),
                _ => None,
            },
            _ => None,
        };
        self.purposes[key].cut_checked = false;
        self.rescan_requested = true;
        Ok(PreparedEffectJob {
            effect: FrontendEffect::Logical(Arc::new(LogicalEffectEnvelope {
                body,
                reservation: effect.reservation,
            })),
            exit,
            result,
            captures: Box::new(owner),
            transport: None,
        })
    }
    fn return_unclaimed_result(&mut self, result: Position) {
        let key = self
            .pending
            .take()
            .expect("same original unclaimed logical prepayment");
        assert!(self.purposes[key].result.replace(result).is_none());
    }
    fn core_consumed(&mut self, inputs: &[InputId]) {
        if !inputs.is_empty() {
            self.rescan_requested = true;
        }
    }
    fn owner_failed_binding(
        &mut self,
        ticket: EffectTicket<FrontendEffect>,
        refused: EffectBindRefusal,
    ) {
        self.retain(Box::new((ticket, refused)));
    }
    fn owner_failed_pending(&mut self, original: PendingHandoff) {
        self.retain(Box::new(original));
    }
    fn owner_failed_cleanup(
        &mut self,
        ticket: EffectTicket<FrontendEffect>,
        context: Option<crate::task_execution::host::failure_publication::FailureContext>,
        observations: Option<
            crate::task_execution::host::observations::host_turn::OrderedObservationFailure,
        >,
    ) {
        if ticket.actor != self.registration.actor()
            || self.failure_ticket.is_some()
            || context.is_some()
            || observations.is_some()
        {
            // A mismatched shared-host binding is a carrier invariant. Keep
            // every original intact rather than unwinding foreign custody.
            std::mem::forget((ticket, context, observations));
            std::process::abort();
        }
        self.failure_ticket = Some(Arc::new(ticket));
        self.failure.observe_owner_failed();
        self.state.stop_observation_after_owner_failure();
        // No new observers may start after the fence. Any already launched
        // observer remains counted by live_jobs until its actual exit.
        self.startup_cursor = 2;
        self.close_external();
    }
    fn retain_transport_failure(&mut self, original: Box<dyn std::any::Any + Send>) {
        self.retain(original);
    }
    fn retain_transport_release(&mut self, original: Box<dyn std::any::Any + Send>) {
        self.retain(original);
    }
    fn close_external(&mut self) {
        self.closed = true;
        self.inputs.close_admission_gate();
        if let Some(mut inputs) = self.inputs.try_loan() {
            inputs.close();
        }
        self.rescan_requested = true;
    }
    fn result_admission_closed(&self) -> bool {
        self.closed
    }
    fn progress_retirement(
        &mut self,
        runtime: &ActorRuntime<FrontendActor>,
        turn: &ActorHostTurn<FrontendActor>,
        _: Tick,
        budget: usize,
    ) -> SourceProgress {
        let mut units = 0;
        let mut progressed = false;
        let failed = runtime
            .snapshot(self.registration.actor())
            .expect("same original logical actor")
            .failed;
        if failed && self.failure_cut.is_none() {
            let proof = runtime
                .failure_proof_for(&self.registration)
                .expect("same original logical failure proof");
            self.failure_cut = Some(LogicalFailureCut::from_original(
                &proof,
                self.failure_budget,
            ));
        }
        if failed
            && self.failure_job.is_none()
            && !self.failure_retired
            && units < budget
            && let Some(ticket) = &self.failure_ticket
        {
            let original = self
                .failure_work
                .take()
                .expect("same original prepaid logical failure work");
            assert_eq!(
                original.work().reference(),
                ticket.descriptor.work,
                "same original logical failure credential"
            );
            runtime
                .validate_failure_effect_ticket(ticket)
                .expect("original logical failure ticket");
            let originals = self
                .executor
                .as_ref()
                .and_then(|executor| executor.failure_executors());
            let progress = LogicalFailureProgressObligation::new(
                originals.as_ref().map(|originals| originals.governance()),
                self.wake.clone(),
            );
            let supervisor = LogicalFailureSupervisor {
                runtime: runtime.clone(),
                registration: self.registration.clone(),
                ticket: ticket.clone(),
                cut: self
                    .failure_cut
                    .expect("original fixed logical failure cut"),
                originals,
                links: self.links.clone(),
                progress: progress.clone(),
                failure: self.failure.clone(),
            };
            if let Some(observer) = &self.owner_failure_observer {
                observer.report(self.failure.error());
            }
            let mut supervisor = Some(supervisor);
            let job = AuxiliaryWorkObligation::start(AuxiliaryWorkStart {
                handle: self.runtime.clone(),
                original,
                make: Box::new(move || {
                    Box::pin(
                        supervisor
                            .take()
                            .expect("same original logical failure supervisor")
                            .run(),
                    )
                }),
                wake: self.wake.clone(),
                failure: self.failure.reporter(),
                retirement: AuxiliaryHeaderOwner {
                    runtime: runtime.clone(),
                    actor: self.registration.clone(),
                },
            });
            self.failure_progress = Some(progress);
            self.failure_job = Some(job);
            units += 1;
            progressed = true;
        }
        let notified = self.progress_requested.swap(false, Ordering::AcqRel);
        if self.pass_active && notified {
            self.rescan_requested = true;
        }
        if budget != 0 && !turn.take_header_readiness(1).is_empty() {
            units += 1;
            self.rescan_requested = true;
        }
        if !failed && self.startup_cursor == 0 && units < budget {
            self.launch_original(self.bootstrap_purpose, LogicalHostEffect::Bootstrap);
            self.startup_cursor = 1;
            units += 1;
            progressed = true;
        }
        if !failed && self.startup_cursor == 1 && units < budget {
            if self.observe_client {
                self.launch_original(
                    self.client_purpose,
                    LogicalHostEffect::ClientCloseObservation,
                );
                units += 1;
                progressed = true;
            }
            self.startup_cursor = 2;
        }
        if self.state.retirable.load(Ordering::Acquire) && !self.closed {
            // Core retirement closes new external submission immediately. Its
            // Registry reply and other already-committed neutral effects may
            // still be awaiting their original purpose. Keep those original
            // result/exit positions and dispatch admission until the actual
            // batches and jobs have drained; Retirable is not their receipt.
            self.inputs.close_admission_gate();
            if let Some(mut inputs) = self.inputs.try_loan() {
                inputs.close();
            }
            let inventory = runtime
                .snapshot(self.registration.actor())
                .expect("same original logical retirement inventory");
            if self.live_jobs == 0
                && self.inputs_retired
                && self.pending.is_none()
                && inventory.inputs == 0
                && inventory.batches == 0
            {
                self.close_external();
                self.funding
                    .scope()
                    .close_external()
                    .expect("close original logical external admission");
                progressed = true;
            }
        }
        let mut cx = Context::from_waker(&self.wake);
        // One finite pass per real change. Pending Join/header/input readiness
        // owns the next wake; a cursor inspection alone never self-schedules.
        if !self.pass_active {
            self.pass_active = true;
            self.remaining = if self.live_authors == 0 {
                0
            } else {
                self.purposes.len()
            };
            self.input_pass_done = false;
            self.tail_pass_done = self.tails_retired;
            self.header_remaining = self.pending_headers.len();
            if !self.tail_pass_done {
                self.tails
                    .as_mut()
                    .expect("original Native tail Source")
                    .begin_pass();
            }
            self.rescan_requested = false;
        }
        // Completed families remain done through this composite pass. Each
        // real progress event owes at most one following pass; family cursors
        // never restart each other merely because another family yielded.
        let available = budget.saturating_sub(units);
        let mut family_budget = [available / 4; 4];
        for n in 0..available % 4 {
            family_budget[(self.first_family + n) % 4] += 1;
        }
        if available != 0 {
            self.first_family = (self.first_family + 1) % 4;
        }
        let visits = self.remaining.min(family_budget[0]);
        for _ in 0..visits {
            let key = self.cursor;
            self.cursor = (self.cursor + 1) % self.purposes.len();
            self.remaining -= 1;
            units += 1;
            let purpose = &mut self.purposes[key];
            // The original child/port may outlive its activation job. Keep
            // progressing that same row until actual child removal and author
            // closure, even after the job's own custody has retired.
            if let Some(index) = purpose.activation_link {
                let failed = runtime
                    .snapshot(self.registration.actor())
                    .expect("original logical port actor")
                    .failed;
                match self.links.endpoint_progress(index, failed) {
                    Ok(Some(port)) => {
                        progressed |= port.progressed;
                        self.rescan_requested |= port.more;
                        if port.complete
                            && let Some(mut inputs) = self.inputs.try_loan()
                        {
                            match inputs.ap_inputs_closed_after_producer_exit(index) {
                                Ok(()) => self.links.input_author_closed(index),
                                Err(original) => self.failure.retain(Box::new(original)),
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(original) => self.failure.retain(Box::new(original)),
                }
                if purpose.job.is_none() {
                    match self.links.progress(index, runtime, &self.registration) {
                        Ok(true) => purpose.activation_link = None,
                        Ok(false) => {}
                        Err(original) => self.failure.retain(Box::new(original)),
                    }
                }
            }
            if let Some(job) = purpose.job.as_ref() {
                if !job.poll_join(&mut cx) {
                    continue;
                }
                if !purpose.cut_checked {
                    purpose.cut_checked = true;
                    if let Some(activation) = purpose.relay_start
                        && let Some(cut) = self
                            .executor
                            .as_ref()
                            .expect("live original executor")
                            .take_joined_relay_cut(activation)
                    {
                        if let Some(index) = self.routing.activation(activation) {
                            self.tails
                                .as_mut()
                                .expect("original tail Source")
                                .stage_cut(index, cut);
                        } else {
                            self.failure.retain(Box::new(cut));
                        }
                    }
                }
                if purpose.result.is_none() {
                    purpose.result = job.take_result();
                }
                if !job.actual_exit() {
                    continue;
                }
                if let Some(index) = purpose.activation_link {
                    self.links.activation_exited(index);
                    match self.links.progress(index, runtime, &self.registration) {
                        Ok(true) => purpose.activation_link = None,
                        Ok(false) => {}
                        Err(original) => self.failure.retain(Box::new(original)),
                    }
                }
                if failed {
                    // Both roles carry only the frozen passive FrontendInput
                    // vocabulary. Do not create an ActionExited occurrence in
                    // an already failed Core; retire actual queued originals.
                    for position in [purpose.result.as_ref(), purpose.exit.as_ref()]
                        .into_iter()
                        .flatten()
                    {
                        match position.take_failed_payload() {
                            Ok(Some(payload)) => {
                                payload
                                    .dispose()
                                    .expect("actual passive logical input disposal");
                                progressed = true;
                            }
                            // The paired result/exit share this exact Source;
                            // only its current original head may be taken.
                            Ok(None) | Err(ActorTurnError::InputAuthorMismatch) => {}
                            Err(error) => std::panic::resume_unwind(Box::new(error)),
                        }
                    }
                }
                if !failed && !purpose.exit_filled {
                    let input = match job.result_action() {
                        Some(action) => {
                            FrontendInput::Logical(Box::new(LogicalInput::ActionExited { action }))
                        }
                        None => FrontendInput::LogicalHostSettled,
                    };
                    match purpose
                        .exit
                        .as_mut()
                        .unwrap()
                        .fill_with_receipt(None, input)
                    {
                        Ok(_) => {
                            purpose.exit_filled = true;
                            progressed = true;
                        }
                        Err(original) => {
                            self.failure.retain(Box::new(original));
                            continue;
                        }
                    }
                }
                let Some(result) = purpose.result.as_ref() else {
                    continue;
                };
                let retired = result.retire_ready_after_exit().and_then(|a| {
                    purpose
                        .exit
                        .as_ref()
                        .unwrap()
                        .retire_ready_after_exit()
                        .map(|b| a && b)
                });
                match retired {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(original) => {
                        self.failure.retain(Box::new(original));
                        continue;
                    }
                }
                let reaped = match job.retire(runtime, &self.registration) {
                    Ok(reaped) => reaped,
                    Err(original) => {
                        self.failure.retain(Box::new(original));
                        continue;
                    }
                };
                if !reaped {
                    if !job.custody_retired() {
                        continue;
                    }
                    assert!(
                        self.pending_headers.len() < self.header_limit,
                        "same prepaid logical header retirement credit"
                    );
                    self.pending_headers.push_back(job.clone());
                }
                purpose.job = None;
                self.live_jobs -= 1;
                purpose.exit_filled = false;
                progressed = true;
            }
            if self.closed
                && purpose.job.is_none()
                && purpose.activation_link.is_none()
                && purpose.author.is_some()
            {
                if !purpose.positions_closed {
                    let closed = purpose
                        .result
                        .as_mut()
                        .unwrap()
                        .close_after_exit()
                        .and_then(|()| purpose.exit.as_mut().unwrap().close_after_exit());
                    if let Err(original) = closed {
                        self.failure.retain(Box::new(original));
                        continue;
                    }
                    purpose.positions_closed = true;
                    purpose.result = None;
                    purpose.exit = None;
                }
                match purpose
                    .author
                    .as_mut()
                    .unwrap()
                    .progress(runtime, &self.registration)
                {
                    Ok(true) => {
                        purpose.author = None;
                        self.live_authors -= 1;
                        progressed = true;
                    }
                    Ok(false) => {}
                    Err(original) => self.failure.retain(Box::new(original)),
                }
            }
        }
        if self.live_authors == 0 {
            self.remaining = 0;
        }
        let observers_exited = self.startup_cursor == 2 && self.live_jobs == 0;
        let failed = runtime
            .snapshot(self.registration.actor())
            .expect("original logical actor snapshot")
            .failed;
        let mut inputs_complete = self.inputs_retired;
        let mut inputs_more = false;
        let mut inputs_waiting = false;
        if !self.input_pass_done && family_budget[1] != 0 && units < budget {
            if let Some(mut inputs) = self.inputs.try_loan() {
                if self.closed || self.state.retirable.load(Ordering::Acquire) {
                    inputs.close();
                }
                let input_progress = inputs
                    .progress(
                        runtime,
                        observers_exited,
                        failed,
                        family_budget[1].min(budget.saturating_sub(units)),
                    )
                    .expect("original logical input custody");
                units += input_progress.units;
                progressed |= input_progress.progressed;
                inputs_complete = input_progress.complete;
                inputs_more = input_progress.more;
                self.input_pass_done = !inputs_more;
                if inputs_complete {
                    inputs.mark_retired();
                    self.inputs_retired = true;
                }
            } else {
                inputs_waiting = true;
            }
        }
        if self.closed
            && self.live_jobs == 0
            && let Some(tails) = self.tails.as_mut()
        {
            tails.close();
        }
        if !self.tail_pass_done && family_budget[2] != 0 && units < budget {
            let routing = &mut self.routing;
            let progress = self.tails.as_mut().expect("original tail Source").progress(
                &mut cx,
                family_budget[2].min(budget.saturating_sub(units)),
                |activation| routing.activation(activation),
            );
            units += progress.units;
            progressed |= progress.progressed;
            self.tail_pass_done = !progress.more;
            self.tails_retired = progress.retired;
        }
        let header_visits = self
            .header_remaining
            .min(family_budget[3].min(budget.saturating_sub(units)));
        for _ in 0..header_visits {
            units += 1;
            self.header_remaining -= 1;
            let original = self
                .pending_headers
                .pop_front()
                .expect("same pending logical header");
            match original.retire(runtime, &self.registration) {
                Ok(true) => progressed = true,
                Ok(false) => self.pending_headers.push_back(original),
                Err(error) => {
                    self.failure.retain(Box::new(error));
                    self.pending_headers.push_back(original);
                }
            }
        }
        self.rescan_requested |= progressed;
        let mut more = self.remaining != 0
            || (!self.input_pass_done && !inputs_waiting)
            || inputs_more
            || !self.tail_pass_done
            || self.header_remaining != 0
            || self.startup_cursor < 2;
        if self.remaining == 0
            && self.input_pass_done
            && self.tail_pass_done
            && self.header_remaining == 0
        {
            self.pass_active = false;
            more |= self.rescan_requested;
        }
        let drained = self.closed
            && self.live_authors == 0
            && inputs_complete
            && self.tails_retired
            && self.pending_headers.is_empty();
        let mut retired = false;
        if drained && let Some(progress) = &self.failure_progress {
            progress.observe_other_work_exited();
        }
        let failure_disposal = self
            .failure_progress
            .as_ref()
            .is_some_and(|progress| progress.disposal_requested());
        if drained && units < budget && (!failed || failure_disposal) {
            units += 1;
            if self.disposal.is_none() && !self.originals_disposed {
                let original = self
                    .disposal_work
                    .take()
                    .expect("original prepaid executor disposal work");
                let header = original.work().retirement_observer();
                let capture = LogicalFinalCaptures {
                    _executor: self.executor.take().expect("same original executor set"),
                    _tails: self.tails.take().expect("same original Native tail Source"),
                };
                let disposal = HostDisposal::start(
                    &self.runtime,
                    original,
                    super::work::OBSERVER,
                    capture,
                    drop,
                )
                .unwrap_or_else(|original| std::panic::resume_unwind(Box::new(original)));
                self.disposal = Some((disposal, header));
                progressed = true;
            }
            if let Some((disposal, header)) = &self.disposal {
                match disposal.poll_complete(&mut cx) {
                    Poll::Pending => {}
                    Poll::Ready(Ok(())) => {
                        if self
                            .funding
                            .scope()
                            .reap_observed_header(header)
                            .expect("original logical disposal header")
                        {
                            self.disposal = None;
                            self.originals_disposed = true;
                            if let Some(progress) = &self.failure_progress {
                                progress.observe_originals_disposed();
                            }
                            progressed = true;
                        }
                    }
                    Poll::Ready(Err(original)) => self.failure.retain(Box::new(original)),
                }
            }
            if self.originals_disposed && !failed {
                if !self.marker_closed {
                    self.marker_closed = self
                        .failure
                        .close_marker_after_exit(false)
                        .expect("original logical marker");
                }
                if self.marker_closed {
                    if let Some(author) = &mut self.marker_author
                        && author
                            .progress(runtime, &self.registration)
                            .expect("original logical marker author")
                    {
                        self.marker_author = None;
                    }
                    if self.marker_author.is_none()
                        && matches!(
                            runtime.timer_source_state(self.registration.actor()),
                            Ok(TimerSourceState::ClosedDrained)
                        )
                    {
                        let sealed = match turn.seal_drained() {
                            Ok(()) => true,
                            Err(
                                ActorTurnError::ActorNotRetired
                                | ActorTurnError::Scheduler(
                                    novarocks_control_runtime_core::scheduler::SchedulerError::Busy,
                                ),
                            ) => false,
                            Err(original) => std::panic::resume_unwind(Box::new(original)),
                        };
                        if sealed {
                            if !self.failure_retired && self.unused_failure.is_none() {
                                // The same Core and all ingress are now sealed.
                                // Its prepaid supervisor was never handed out.
                                let original = self
                                    .failure_work
                                    .take()
                                    .expect("original unused logical supervisor");
                                let header = original.work().retirement_observer();
                                let (custody, exit) = ExitCustody::from_admitted(original);
                                exit.revoke_unused_and_exit()
                                    .expect("actual unused sealed supervisor");
                                self.unused_failure = Some((custody, header));
                            }
                            if let Some((custody, header)) = &self.unused_failure
                                && custody
                                    .try_retire()
                                    .expect("original unused supervisor custody")
                                && self
                                    .funding
                                    .scope()
                                    .reap_observed_header(header)
                                    .expect("unused original supervisor header")
                            {
                                self.failure_retired = true;
                                self.unused_failure = None;
                            }
                            if self.failure_retired {
                                retired = self
                                    .funding
                                    .progress(runtime, &self.registration)
                                    .expect("original logical root custody");
                            }
                        } else {
                            more = true;
                        }
                    }
                }
            }
            more |= progressed && !retired;
        }
        if failed
            && self.originals_disposed
            && units < budget
            && self
                .failure_progress
                .as_ref()
                .is_some_and(|progress| progress.governance_completed())
        {
            units += 1;
            if let Some(job) = &self.failure_job {
                match job.poll_work_retirement(&mut cx) {
                    Poll::Ready(Ok(true)) => {
                        if !self.failure_ticket_completed {
                            let ticket = Arc::try_unwrap(self.failure_ticket.take().expect("original failure ticket"))
                                .unwrap_or_else(|_| panic!("original logical failure ticket capture remains live after actual exit"));
                            let input = ticket.input;
                            runtime
                                .effect_completed_for(&self.registration, ticket)
                                .expect("original failure effect completed");
                            runtime
                                .retire_input(self.registration.actor(), input)
                                .expect("original failure input retired");
                            self.failure_ticket_completed = true;
                            progressed = true;
                        }
                        if matches!(job.poll_complete(&mut cx), Poll::Ready(Ok(()))) {
                            self.failure_retired = true;
                            self.failure_job = None;
                            progressed = true;
                        }
                    }
                    Poll::Ready(Err(_)) | Poll::Pending | Poll::Ready(Ok(false)) => {}
                }
            }
            if self.failure_retired {
                if !self.marker_closed {
                    self.marker_closed = self
                        .failure
                        .close_marker_after_exit(true)
                        .expect("original failed logical marker");
                }
                if self.marker_closed {
                    if let Some(author) = &mut self.marker_author
                        && author
                            .progress(runtime, &self.registration)
                            .expect("original failed marker author")
                    {
                        self.marker_author = None;
                    }
                    if self.marker_author.is_none()
                        && matches!(
                            runtime.timer_source_state(self.registration.actor()),
                            Ok(TimerSourceState::ClosedDrained)
                        )
                    {
                        retired = self
                            .funding
                            .progress(runtime, &self.registration)
                            .expect("original failed logical root custody");
                    }
                }
            }
            more |= progressed && !retired;
        }
        // Crossing the last row on an exact budget boundary leaves a real
        // finalization step owed. Once its Join/header wait is armed, Pending
        // waits for that actual notification and does not self-schedule.
        more |= drained && units == budget && !retired && (!failed || failure_disposal);
        if progressed && let Some(progress) = &self.failure_progress {
            progress.observe_other_work_progress();
        }
        SourceProgress {
            units,
            progressed,
            more,
            retired,
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(super) use tests::fixture::assemble_logical;
