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

//! Select and consume the original mailbox claim at the protocol boundary.

use super::{
    Core, RootControlApplication, SealedRootCut, round_tail::RoundTailStep, schedule,
    stop::StopProgress,
};
use crate::{
    AttemptCore,
    authorization::gate::{CloseProgress, QaReply},
    capacity::StepContext,
    convergence::RemoteConvergenceDirective,
    diagnostics::sources::DiagnosticSourceAssembly,
    effect::{AttemptEffect, EffectBody, EffectClass, Publication},
    error::TaskExecutionError,
    execution::CreateGrantProgress,
    failure::AttemptFailure,
    input::{
        AttemptInput, AttemptInputClaim, AttemptInputId, AttemptSourceId, ExecutorFact,
        PublicationApplied,
    },
    root_control::{RootSealRefusal, SealSettlement},
    start::AttemptStart,
    terminal::{OriginalHorizon, TerminalDirective, TerminalFacts, TerminalWake},
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptConvergenceMode, AttemptRequestToken, AttemptRootProjection,
    AttemptToLogical, AttemptToLogicalBody, LogicalToAttemptBody, MonotonicInstant,
};
use novarocks_execution_contract::{TaskOperationId, TaskStatus};
use std::{num::NonZeroU64, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InputSelection {
    Prefer(AttemptSourceId),
    Only(AttemptSourceId),
}

#[derive(Debug)]
pub struct AttemptStep {
    pub selection: InputSelection,
    pub stop_producing: bool,
    pub effect: Option<AttemptEffect>,
    pub abort_settlement: Option<AttemptEffect>,
    /// Reserved on the same action sequence for the host's passive turn report.
    pub notice_action: Option<AttemptActionId>,
    pub root_control_processed: Option<AttemptInputId>,
    pub definitely_unsent_abort: Option<TaskOperationId>,
    pub retired_authorization: Option<TaskOperationId>,
    /// Original diagnostic protocol or publisher failure, separate from query settlement.
    pub diagnostic_failure: Option<AttemptFailure>,
    /// The original round result goes to the terminal controller.
    pub round_result: Option<Result<(), AttemptFailure>>,
}

#[derive(Debug, Default, PartialEq, Eq)]
enum Phase {
    #[default]
    Bootstrap,
    Idle,
    Native,
    NativeQuiesce,
    Observations,
    Common {
        source_ready: bool,
    },
    Root,
    AwaitRoot(AttemptActionId, PublicationApplied),
    AwaitSeal {
        effect: AttemptActionId,
        success: bool,
    },
    Terminal(TerminalWake),
    PublishTerminal,
    Tail,
    Converge,
    ClosingGate,
    Candidates,
}

#[derive(Debug)]
struct RootWork {
    dev_observation: Option<(TaskStatus, bool)>,
    projection: Option<AttemptRootProjection>,
    seal: Option<SealSettlement>,
    publication_failure: Option<AttemptFailure>,
}

#[derive(Debug, Default)]
pub(super) struct Turn {
    phase: Phase,
    observe_dev_root: bool,
    observe_lifecycle_diagnostics: bool,
    lifecycle_diagnostics: Option<DiagnosticSourceAssembly>,
    next: u64,
    root: Option<RootWork>,
    observation: Option<super::ObservationPass>,
    latest_input_time: Option<MonotonicInstant>,
    local_close_requested: bool,
    local_exited: bool,
    reported_local_exit: bool,
    reported_remote_closure: bool,
    membership_revision: Option<u64>,
    convergence_changed: bool,
    seal_delivered: bool,
    terminal_after_close: Option<TerminalWake>,
    cancellation_due: bool,
    split_failed: bool,
    observation_recovery_failed: bool,
}

impl Turn {
    pub(super) fn new(observe_dev_root: bool, observe_lifecycle_diagnostics: bool) -> Self {
        Self {
            observe_dev_root,
            observe_lifecycle_diagnostics,
            ..Self::default()
        }
    }

    fn issue(&mut self) -> Result<(AttemptActionId, AttemptRequestToken), TaskExecutionError> {
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| schedule("attempt step identities exhausted"))?;
        let id = NonZeroU64::new(self.next).expect("positive attempt identity");
        Ok((AttemptActionId::new(id), AttemptRequestToken::new(id)))
    }
}

impl AttemptCore for Core {
    fn new(start: AttemptStart) -> Result<Self, AttemptFailure> {
        Core::new(start)
    }

    fn handle(
        &mut self,
        now: MonotonicInstant,
        claim: AttemptInputClaim,
        input: AttemptInput,
        context: &mut dyn StepContext,
    ) -> Result<AttemptStep, TaskExecutionError> {
        if now
            < self
                .turn
                .latest_input_time
                .unwrap_or_else(|| self.execution.monotonic_now())
        {
            return Err(schedule("attempt time regressed"));
        }
        self.turn.latest_input_time = Some(now);
        // A derived continuation keeps the time of its original primary
        // input, including the following prepaid frames in the same batch.
        let now = if matches!(self.turn.phase, Phase::NativeQuiesce | Phase::Observations) {
            self.execution.monotonic_now()
        } else {
            self.execution.set_now(now)?;
            now
        };
        let units = match &input {
            AttemptInput::Executor(ExecutorFact::Withdrawn { .. }) => self
                .maximum_send_members
                .checked_mul(2)
                .ok_or_else(|| schedule("withdrawal input work quote overflow"))?,
            AttemptInput::Executor(ExecutorFact::Operation(observation))
                if observation.reply.kind()
                    == novarocks_execution_contract::OperationKind::AbortParticipation =>
            {
                3
            }
            AttemptInput::Membership(view) => view.processes.len().max(1),
            _ => 1,
        };
        if !context.charge(units) {
            return Err(schedule(
                "original step cannot apply its complete primary input",
            ));
        }
        let mut result = AttemptStep {
            selection: self.input_selection(),
            stop_producing: false,
            effect: None,
            abort_settlement: None,
            notice_action: None,
            root_control_processed: None,
            definitely_unsent_abort: None,
            retired_authorization: None,
            diagnostic_failure: None,
            round_result: None,
        };
        let dynamic_progress = matches!(
            &input,
            AttemptInput::Membership(_)
                | AttemptInput::CapacityReleased
                | AttemptInput::Executor(
                    ExecutorFact::Operation(_)
                        | ExecutorFact::Observations(_)
                        | ExecutorFact::ReadProgress(_)
                        | ExecutorFact::SubscriptionProgress(_)
                )
        );
        let cancelling = self.terminal.stop_reason().is_some()
            || (claim.source == self.sources.actor
                && matches!(
                    &input,
                    AttemptInput::Port(port) if port.activation == self.activation
                        && matches!(port.body, LogicalToAttemptBody::Stop { .. })
                ));
        self.diagnostic_before_input(now, cancelling);
        self.apply_claim(now, claim, input, context, &mut result)?;
        if self.execution.failure_cause().is_some() && self.convergence.upgrade_to_cleanup() {
            self.turn.convergence_changed = true;
            self.request_stop(&mut result);
        }
        if dynamic_progress {
            self.execution.signal_dynamic_feedback_progress();
        }
        self.diagnostic_after_input(now);
        if result.root_control_processed.is_some()
            || result.definitely_unsent_abort.is_some()
            || result.retired_authorization.is_some()
            || result.diagnostic_failure.is_some()
            || result.round_result.as_ref().is_some_and(Result::is_err)
        {
            result.notice_action = Some(self.turn.issue()?.0);
        }
        result.selection = self.input_selection();
        Ok(result)
    }

    fn next_deadline(&self) -> Option<MonotonicInstant> {
        if self.diagnostics_wait_for_publication() {
            return None;
        }
        match self.turn.phase {
            Phase::Bootstrap
            | Phase::Native
            | Phase::AwaitRoot(..)
            | Phase::AwaitSeal { .. }
            | Phase::Common { source_ready: true } => None,
            Phase::Terminal(_)
                if self.terminal.waiting_for_permission()
                    || self.terminal.waiting_for_process() =>
            {
                None
            }
            Phase::Terminal(_)
            | Phase::PublishTerminal
            | Phase::NativeQuiesce
            | Phase::Observations => Some(self.execution.monotonic_now()),
            Phase::Idle if self.stop.pending() && !self.authorizations.waiting_for_reply() => {
                Some(self.execution.monotonic_now())
            }
            Phase::Idle if self.authorizations.waiting_for_reply() => None,
            Phase::Idle if self.authorizations.close_needs_drive() => {
                Some(self.execution.monotonic_now())
            }
            Phase::ClosingGate if self.authorizations.waiting_for_reply() => None,
            Phase::ClosingGate => Some(self.execution.monotonic_now()),
            Phase::Idle if self.turn.cancellation_due && self.terminal_needs_drive() => {
                Some(self.execution.monotonic_now())
            }
            Phase::Idle
                if self.execution.has_pending_terminal_cleanup()
                    || self.execution.has_task_input_cleanup()
                    || self.execution.has_released_native_input()
                    || (self.stop.business_closed()
                        && self.candidates.enabled
                        && (self.execution.split_pump_lifecycle_due(false)
                            || self.execution.split_task_after(None).is_some()))
                    || self.execution.has_split_notices() =>
            {
                Some(self.execution.monotonic_now())
            }
            Phase::Candidates if self.authorizations.waiting_for_reply() => None,
            Phase::Candidates => Some(self.execution.monotonic_now()),
            Phase::Idle if self.candidates.wake && !self.stop.business_closed() => {
                Some(self.execution.monotonic_now())
            }
            Phase::Idle
                if self.execution.has_dynamic_feedback_work(
                    self.candidates.enabled
                        && !self.stop.business_closed()
                        && !self.execution.normal_drain_started(),
                ) =>
            {
                Some(self.execution.monotonic_now())
            }
            Phase::Idle if self.aborts.has_pending() => Some(self.execution.monotonic_now()),
            Phase::Idle
                if self.diagnostic_work_due()
                    || self.convergence_due(self.execution.monotonic_now()) =>
            {
                Some(self.execution.monotonic_now())
            }
            Phase::Idle => [
                self.next_round_deadline(),
                self.next_observation_recovery_deadline(),
                self.aborts
                    .stand_down_deadline(self.execution.monotonic_now()),
                self.candidates
                    .enabled
                    .then(|| self.execution.next_lifecycle_deadline())
                    .flatten(),
                self.candidates
                    .enabled
                    .then(|| self.execution.next_create_retry_deadline())
                    .flatten(),
                self.convergence.next_deadline(),
                self.terminal.next_deadline(),
                self.execution.diagnostics().deadline(),
            ]
            .into_iter()
            .flatten()
            .min(),
            Phase::Tail => self.next_round_deadline(),
            Phase::Converge
                if self.convergence.waiting_for_process()
                    && !self.convergence_progress_ready()
                    && !self.diagnostic_work_due() =>
            {
                self.execution.diagnostics().deadline()
            }
            Phase::Converge => Some(self.execution.monotonic_now()),
            Phase::Common {
                source_ready: false,
            }
            | Phase::Root => Some(self.execution.monotonic_now()),
        }
    }
}

impl Core {
    fn input_selection(&self) -> InputSelection {
        use InputSelection::{Only, Prefer};
        if self.diagnostics_wait_for_publication() {
            return Only(self.sources.publication);
        }
        match self.turn.phase {
            Phase::Bootstrap | Phase::Root | Phase::NativeQuiesce | Phase::Observations => {
                Only(self.sources.actor)
            }
            Phase::Native => Only(self.sources.native),
            Phase::Common { .. } => Only(self.sources.observation),
            Phase::AwaitRoot(..) | Phase::AwaitSeal { .. } => Only(self.sources.publication),
            Phase::Terminal(_) if self.terminal.waiting_for_permission() => Only(self.sources.qa),
            Phase::Terminal(_) if self.terminal.waiting_for_process() => Only(self.sources.process),
            Phase::Terminal(_) | Phase::PublishTerminal => Only(self.sources.actor),
            Phase::Tail
                if self.round_tail.waiting_for_publication()
                    && self.publication.has_pending_apply() =>
            {
                Only(self.sources.publication)
            }
            Phase::Tail | Phase::Idle | Phase::ClosingGate | Phase::Candidates
                if self.authorizations.waiting_for_reply() =>
            {
                Only(self.sources.qa)
            }
            Phase::Tail | Phase::ClosingGate | Phase::Candidates => Only(self.sources.actor),
            Phase::Converge if self.convergence.waiting_for_process() => Only(self.sources.process),
            Phase::Converge => Only(self.sources.actor),
            Phase::Idle if self.stop.pending() => Only(self.sources.actor),
            Phase::Idle => Prefer(self.sources.native),
        }
    }

    fn apply_claim(
        &mut self,
        now: MonotonicInstant,
        claim: AttemptInputClaim,
        input: AttemptInput,
        context: &mut dyn StepContext,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        if self.turn.phase != Phase::Bootstrap
            && self.apply_owner_control(now, claim, &input, context, result)?
        {
            return Ok(());
        }
        let own = claim.source == self.sources.actor;
        let timer = own && matches!(input, AttemptInput::Timer);
        if self.diagnostics_wait_for_publication() {
            return if timer {
                Ok(())
            } else {
                Err(schedule(
                    "input crossed the original diagnostic publication",
                ))
            };
        }

        match self.turn.phase {
            Phase::Bootstrap => {
                if !own || !matches!(input, AttemptInput::Bootstrap) {
                    return Err(schedule("attempt requires its original bootstrap input"));
                }
                self.turn.phase = Phase::Idle;
            }
            Phase::Idle => {
                if claim.source == self.sources.qa
                    && let AttemptInput::Port(port) = &input
                    && self.authorizations.waiting_for_reply()
                {
                    return self.accept_qa(port.clone(), result);
                }
                if self.stop.pending() {
                    if !timer {
                        return Err(schedule(
                            "new primary input crossed the original stop cleanup",
                        ));
                    }
                    let (action, _) = self.turn.issue()?;
                    match self.stop.advance(&mut self.execution, action, context) {
                        StopProgress::Yield => return Ok(()),
                        StopProgress::Effect(effect) => {
                            result.effect = Some(effect);
                            return Ok(());
                        }
                        StopProgress::Done => {}
                    }
                }
                if timer
                    && !self.aborts.has_pending()
                    && !self.stand_down_due(now)
                    && !self.authorizations.close_needs_drive()
                    && self.convergence_due(now)
                    && (self.start_convergence_pass()
                        || self.convergence_progress_ready()
                        || self.local_close_due())
                {
                    self.turn.phase = Phase::Converge;
                    return self.drive_convergence(now, true, context, result);
                }
                let cut = claim
                    .native_cut
                    .filter(|cut| cut.source == self.sources.native)
                    .ok_or_else(|| schedule("claim lacks its original Native pre-pop cut"))?;
                if cut.tail.is_some() {
                    if !timer && claim.source != self.sources.native {
                        return Err(schedule("primary input crossed the original Native prefix"));
                    }
                    self.begin_native_prefix(now, cut)?;
                    self.turn.phase = Phase::Native;
                    if !timer {
                        self.consume_native(now, claim, input, context, result)?;
                    }
                    return Ok(());
                }
                if timer && self.authorizations.close_needs_drive() {
                    self.turn.phase = Phase::ClosingGate;
                    return self.drive_gate_close(context, result);
                }
                if timer
                    && self
                        .terminal
                        .next_deadline()
                        .is_some_and(|deadline| now.has_reached(deadline))
                {
                    self.turn.phase = Phase::Terminal(TerminalWake::Continuation);
                    return self.drive_terminal(
                        now,
                        TerminalWake::Continuation,
                        false,
                        context,
                        result,
                    );
                }
                if timer && self.turn.cancellation_due && self.terminal_needs_drive() {
                    self.turn.cancellation_due = false;
                    self.turn.phase = Phase::Terminal(TerminalWake::Cancellation);
                    return self.drive_terminal(
                        now,
                        TerminalWake::Cancellation,
                        false,
                        context,
                        result,
                    );
                }
                self.begin_native_prefix(now, cut)?;
                self.enter_common_or_skip()?;
                if self.turn.phase == Phase::Tail {
                    return Ok(());
                }
                if claim.source == self.sources.observation {
                    self.consume_common(now, claim, input, context, result)?;
                } else {
                    self.consume_independent(now, claim, input, result)?;
                }
            }
            Phase::NativeQuiesce if timer => self.drive_native_quiesce(context)?,
            Phase::Observations if timer => {
                let pass = self
                    .turn
                    .observation
                    .take()
                    .expect("selected observation continuation");
                self.drive_observation_pass(now, pass, context)?;
            }
            Phase::NativeQuiesce | Phase::Observations => {
                return Err(schedule("input crossed its original Quiesce continuation"));
            }
            Phase::Native if timer => {}
            Phase::Native => self.consume_native(now, claim, input, context, result)?,
            Phase::Common { .. } if timer => {
                let cut = claim
                    .selected_cut
                    .filter(|cut| cut.source == self.sources.observation)
                    .ok_or_else(|| schedule("timer lacks its selected observation Source cut"))?;
                if cut.tail.is_some() {
                    self.turn.phase = Phase::Common { source_ready: true };
                } else {
                    let seal = self.evaluate_pending_seal();
                    self.select_root(seal)?;
                }
            }
            Phase::Common { .. } => self.consume_common(now, claim, input, context, result)?,
            Phase::Root if timer => self.drive_root(context, result)?,
            Phase::AwaitRoot(..) if timer => {}
            Phase::AwaitRoot(expected, kind) => {
                let AttemptInput::Executor(ExecutorFact::PublicationApplied {
                    effect,
                    result: applied,
                }) = input
                else {
                    return Err(schedule("root publication awaited its actual result"));
                };
                self.verify_source(claim.source, self.sources.publication)?;
                if effect == expected && (applied.is_err() || applied == Ok(kind)) {
                    if let Err(failure) = applied {
                        if kind == PublicationApplied::DevRootObservation {
                            result.diagnostic_failure = Some(failure);
                        } else {
                            let root = self.turn.root.as_mut().expect("original root publication");
                            root.publication_failure = Some(failure.clone());
                            self.round_failure.get_or_insert(failure);
                        }
                    }
                    self.turn.phase = Phase::Root;
                }
            }
            Phase::AwaitSeal { .. } if timer => {}
            Phase::AwaitSeal {
                effect: expected,
                success,
            } => {
                self.verify_source(claim.source, self.sources.publication)?;
                let AttemptInput::Executor(ExecutorFact::PublicationApplied {
                    effect,
                    result: applied,
                }) = input
                else {
                    return Err(schedule("seal awaited its original delivery result"));
                };
                if effect == expected
                    && (applied.is_err() || matches!(applied, Ok(PublicationApplied::SealResult)))
                {
                    match applied {
                        Ok(_) => self.turn.seal_delivered |= success,
                        Err(failure) => {
                            self.round_failure.get_or_insert(failure);
                        }
                    }
                    self.begin_round_tail(true)?;
                    self.turn.phase = Phase::Tail;
                }
            }
            Phase::Terminal(wake) => {
                self.apply_terminal(now, claim, input, wake, context, result)?
            }
            Phase::PublishTerminal if timer => {
                if context.charge(1) {
                    let terminal = self
                        .terminal
                        .terminal()
                        .expect("selected terminal publication")
                        .clone();
                    self.publish_terminal(terminal, result)?;
                }
            }
            Phase::Tail => self.apply_tail(now, claim, input, context, result)?,
            Phase::Candidates if timer => self.drive_candidates(context, result)?,
            Phase::Candidates => {
                self.verify_source(claim.source, self.sources.qa)?;
                let AttemptInput::Port(port) = input else {
                    return Err(schedule("candidate awaited its original QA reply"));
                };
                self.accept_qa(port, result)?;
                if self.turn.phase == Phase::Candidates {
                    self.drive_candidates(context, result)?;
                }
            }
            Phase::ClosingGate if timer => self.drive_gate_close(context, result)?,
            Phase::ClosingGate => {
                self.verify_source(claim.source, self.sources.qa)?;
                let AttemptInput::Port(port) = input else {
                    return Err(schedule(
                        "authorization close awaited its original QA reply",
                    ));
                };
                // The original QA result input must retire before the next
                // gate settlement can borrow that same result position.
                self.accept_qa(port, result)?;
            }
            Phase::Converge if timer => {
                if self.diagnostic_work_due() {
                    let (action, request) = self.turn.issue()?;
                    if let super::diagnostic_drive::DiagnosticStep::Effect(effect) =
                        self.advance_diagnostics(action, request, context)
                    {
                        result.effect = Some(effect);
                    }
                } else {
                    self.drive_convergence(now, true, context, result)?;
                }
            }
            Phase::Converge => {
                self.verify_source(claim.source, self.sources.process)?;
                let AttemptInput::Executor(ExecutorFact::ProcessSnapshot {
                    request,
                    participation,
                    sampled_at,
                    observation,
                }) = input
                else {
                    return Err(schedule(
                        "convergence awaited its original process snapshot",
                    ));
                };
                self.convergence.process_reply(
                    now,
                    request,
                    participation,
                    sampled_at,
                    observation,
                )?;
                // The reply still owns the Process result position. Park an
                // exact next read for the Timer while allowing independent progress.
                self.drive_convergence(now, false, context, result)?;
            }
            _ => return Err(schedule("input crossed an unfinished Root phase")),
        }
        Ok(())
    }

    fn apply_owner_control(
        &mut self,
        now: MonotonicInstant,
        claim: AttemptInputClaim,
        input: &AttemptInput,
        context: &mut dyn StepContext,
        result: &mut AttemptStep,
    ) -> Result<bool, TaskExecutionError> {
        match input {
            AttemptInput::StartProtocol => {
                self.verify_source(claim.source, self.sources.actor)?;
                if self.candidates.enabled
                    || self.turn.phase != Phase::Idle
                    || self.stop.business_closed()
                {
                    return Err(schedule(
                        "attempt protocol drive was already started or closed",
                    ));
                }
                self.candidates.enabled = true;
                self.candidates.wake = true;
            }

            AttemptInput::Port(port) => match &port.body {
                LogicalToAttemptBody::PreReadyReplacementChanged { token } => {
                    self.verify_source(claim.source, self.sources.qa)?;
                    if port.activation == self.activation
                        && self.terminal.permission_changed(*token)
                            == crate::terminal::ReplyDisposition::Applied
                        && self.turn.phase == Phase::Idle
                        && !self.authorizations.waiting_for_reply()
                    {
                        self.turn.phase = Phase::Terminal(TerminalWake::Continuation);
                    }
                }
                LogicalToAttemptBody::Abort { action, request } => {
                    self.verify_owner_port(claim.source, port.activation)?;
                    let (admission, _) = self.turn.issue()?;
                    if self.aborts.taking_over() || self.aborts.retired_request(*request) {
                        result.abort_settlement = Some(self.abort_effect(
                            AttemptToLogicalBody::AbortSettlement {
                                abort_action: *action,
                                generation: admission,
                                receipt_generation: None,
                                participation: request.participation(),
                                operation_id: request.envelope().operation_id(),
                                settlement: novarocks_coordination_contract::AbortIssueSettlement::DefinitelyUnsent,
                            },
                        )?);
                    } else {
                        self.aborts.begin(
                            &self.execution,
                            *action,
                            *request,
                            crate::input::SendGeneration::new(admission),
                        )?;
                    }
                }
                LogicalToAttemptBody::Stop { reason, failure } => {
                    self.verify_owner_port(claim.source, port.activation)?;
                    let first = self.terminal.stop_reason().is_none();
                    self.terminal.observe_stop(*reason, failure.clone());
                    if self.terminal.requires_logical_failure_stand_down() {
                        self.aborts.start_stand_down();
                        self.turn.convergence_changed |= self.convergence.upgrade_to_cleanup();
                    }
                    self.turn.cancellation_due |= first;
                    self.execution
                        .with_diagnostics(|collector, _| collector.stop_starting());
                    self.request_stop(result);
                }
                LogicalToAttemptBody::BeginConvergence {
                    mode,
                    remote_deadline,
                    computation_ready,
                    retain_diagnostics,
                } => {
                    self.verify_owner_port(claim.source, port.activation)?;
                    if let Some(ready) = computation_ready {
                        if ready.activation != self.activation
                            || ready.root != self.execution.graph().root_identity()
                        {
                            return Err(schedule(
                                "convergence cut differs from its original attempt",
                            ));
                        }
                        if *retain_diagnostics {
                            self.execution
                                .with_diagnostics(|collector, _| collector.begin(*ready))?;
                            self.diagnostic_before_input(
                                now,
                                self.terminal.stop_reason().is_some(),
                            );
                            self.execution
                                .retain_profile_diagnostics_until(ready.diagnostic_deadline);
                        }
                    } else {
                        self.execution
                            .with_diagnostics(|collector, _| collector.stop_starting());
                    }
                    let mode = if self.turn.split_failed
                        || self.aborts.taking_over()
                        || self.execution.failure_cause().is_some()
                    {
                        AttemptConvergenceMode::Cleanup
                    } else {
                        *mode
                    };
                    self.convergence.begin(mode, *remote_deadline);
                    self.turn.convergence_changed = true;
                    if mode == AttemptConvergenceMode::Cleanup {
                        self.request_stop(result);
                    }
                    if self.turn.phase == Phase::Idle
                        && !self.stop.pending()
                        && !self.stand_down_due(now)
                        && !self.authorizations.waiting_for_reply()
                        && (self.start_convergence_pass() || self.convergence_progress_ready())
                    {
                        self.turn.phase = Phase::Converge;
                    }
                }
                _ => return Ok(false),
            },
            AttemptInput::Executor(ExecutorFact::PublicationApplied {
                effect,
                result: applied,
            }) if self.diagnostics_wait_for_publication() => {
                self.verify_source(claim.source, self.sources.publication)?;
                self.observe_diagnostic_publication(*effect, applied, result);
            }
            AttemptInput::Executor(ExecutorFact::ActualExit { effect })
                if self.observe_diagnostic_exit(*effect, result) => {}
            AttemptInput::Executor(ExecutorFact::LocalConvergenceExited) => {
                self.verify_source(claim.source, self.sources.actor)?;
                self.turn.local_exited = true;
                if self.turn.phase == Phase::Idle
                    && !self.authorizations.waiting_for_reply()
                    && self.convergence_progress_ready()
                {
                    self.turn.phase = Phase::Converge;
                }
                if self.turn.phase == Phase::Converge {
                    self.drive_convergence(now, true, context, result)?;
                }
            }
            AttemptInput::Membership(view) => {
                self.verify_source(claim.source, self.sources.membership)?;
                if self
                    .turn
                    .membership_revision
                    .is_some_and(|old| view.revision <= old)
                {
                    return Ok(true);
                }
                self.turn.membership_revision = Some(view.revision);
                self.turn.convergence_changed = true;
                return Ok(self.turn.phase != Phase::Idle);
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn verify_owner_port(
        &self,
        source: AttemptSourceId,
        activation: novarocks_coordination_contract::AttemptActivationIdentity,
    ) -> Result<(), TaskExecutionError> {
        self.verify_source(source, self.sources.actor)?;
        if activation != self.activation {
            return Err(schedule(
                "owner control differs from its original activation",
            ));
        }
        Ok(())
    }

    fn request_stop(&mut self, result: &mut AttemptStep) {
        self.authorizations.request_close();
        if self.stop.request() {
            self.round_tail.suppress_dispatch();
            result.stop_producing = true;
        }
    }

    fn drive_gate_close(
        &mut self,
        context: &mut dyn StepContext,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        if !self.acknowledgements.is_idle() {
            return Err(schedule(
                "authorization close overtook observed acknowledgements",
            ));
        }
        let (action, token) = self.turn.issue()?;
        match self.authorizations.advance_close(action, token, context)? {
            CloseProgress::Yield | CloseProgress::AwaitReply => {}
            CloseProgress::Request(effect) => result.effect = Some(effect),
            CloseProgress::WaitingTransport | CloseProgress::Ready => {
                self.turn.phase = self
                    .turn
                    .terminal_after_close
                    .take()
                    .map_or(Phase::Idle, Phase::Terminal);
            }
        }
        Ok(())
    }

    fn convergence_due(&self, now: MonotonicInstant) -> bool {
        self.convergence_progress_ready()
            || self.local_close_due()
            || (self.convergence.mode().is_some()
                && self.convergence.closure().is_none()
                && (self.turn.convergence_changed
                    || self
                        .convergence
                        .next_deadline()
                        .is_some_and(|deadline| now.has_reached(deadline))))
    }

    fn stand_down_due(&self, now: MonotonicInstant) -> bool {
        self.aborts
            .stand_down_deadline(now)
            .is_some_and(|deadline| now.has_reached(deadline))
    }

    fn start_convergence_pass(&mut self) -> bool {
        if self.convergence.start_pass() {
            self.turn.convergence_changed = false;
            true
        } else {
            false
        }
    }

    fn convergence_progress_ready(&self) -> bool {
        self.convergence.mode().is_some()
            && ((self.turn.local_exited
                && self.diagnostic_local_ready()
                && !self.turn.reported_local_exit)
                || (self.convergence.closure().is_some() && !self.turn.reported_remote_closure))
    }

    pub(super) fn local_sources_closing(&self) -> bool {
        self.turn.local_close_requested
    }

    fn local_close_due(&self) -> bool {
        self.convergence.closure().is_some()
            && self.diagnostic_local_ready()
            && !self.turn.local_close_requested
            && !self.turn.local_exited
            && (!self.stop.business_closed()
                || (!self.stop.pending()
                    && !self.authorizations.close_needs_drive()
                    && !self.execution.has_pending_terminal_cleanup()
                    && !self.execution.has_task_input_cleanup()
                    && !self.execution.has_released_native_input()
                    && self.execution.split_local_close_ready()
                    && !self.execution.has_dynamic_feedback_work(false)))
    }

    fn drive_convergence(
        &mut self,
        now: MonotonicInstant,
        may_observe_process: bool,
        context: &mut dyn StepContext,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        if self.convergence_progress_ready() {
            if !context.charge(1) {
                return Ok(());
            }
            let closure = (!self.turn.reported_remote_closure)
                .then(|| self.convergence.closure().cloned())
                .flatten();
            let local_exited = self.turn.local_exited && self.diagnostic_local_ready();
            let final_report = (self.turn.reported_remote_closure || closure.is_some())
                && (self.turn.reported_local_exit || local_exited);
            let lifecycle_runtime_filters =
                if final_report && self.turn.observe_lifecycle_diagnostics {
                    let sources = self.execution.diagnostic_sources();
                    let assembly = self
                        .turn
                        .lifecycle_diagnostics
                        .get_or_insert_with(|| DiagnosticSourceAssembly::new(sources.freeze(now)));
                    while !assembly.complete() {
                        if !assembly.advance(sources, context) {
                            return Ok(());
                        }
                    }
                    let selected = self
                        .turn
                        .lifecycle_diagnostics
                        .take()
                        .expect("original lifecycle diagnostics")
                        .finish()
                        .expect("completed lifecycle diagnostics");
                    Some(Arc::new(selected.runtime_filters))
                } else {
                    None
                };
            self.turn.reported_remote_closure |= closure.is_some();
            self.turn.reported_local_exit |= local_exited;
            result.effect = Some(self.port_effect(AttemptToLogicalBody::ConvergenceProgress {
                local_exited,
                closure,
                lifecycle_runtime_filters,
            })?);
            return Ok(());
        }
        if self.local_close_due() {
            if !context.charge(1) {
                return Ok(());
            }
            self.turn.phase = Phase::Idle;
            if !self.stop.business_closed() {
                self.request_stop(result);
                return Ok(());
            }
            let (id, _) = self.turn.issue()?;
            self.execution.close_local_split_notices();
            self.turn.local_close_requested = true;
            result.effect = Some(AttemptEffect {
                id,
                class: EffectClass::Cleanup,
                body: EffectBody::PrepareLocalConvergence,
            });
            return Ok(());
        }
        if self.convergence.waiting_for_process() || !context.charge(1) {
            return Ok(());
        }
        let process_read = may_observe_process.then(|| self.turn.issue()).transpose()?;
        match self.convergence.advance(
            now,
            &self.execution,
            context,
            process_read.map(|(_, token)| token),
        )? {
            RemoteConvergenceDirective::ObserveProcess {
                request,
                participation,
            } => {
                let (id, _) = process_read.expect("original process read credit");
                result.effect = Some(AttemptEffect {
                    id,
                    class: EffectClass::Settlement,
                    body: EffectBody::ObserveProcess {
                        request,
                        participation,
                    },
                });
            }
            RemoteConvergenceDirective::ClosureReady if self.convergence_progress_ready() => {}
            RemoteConvergenceDirective::ClosureReady | RemoteConvergenceDirective::Inactive => {
                self.turn.phase = Phase::Idle;
            }
            RemoteConvergenceDirective::Yield
            | RemoteConvergenceDirective::Waiting
            | RemoteConvergenceDirective::NeedRequestCredit => {}
        }
        Ok(())
    }

    fn consume_native(
        &mut self,
        now: MonotonicInstant,
        claim: AttemptInputClaim,
        input: AttemptInput,
        context: &mut dyn StepContext,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        let AttemptInput::Executor(fact) = input else {
            return Err(schedule("Native Source supplied an unrelated input"));
        };
        self.verify_source(claim.source, self.sources.native)?;
        let action = if matches!(&fact, ExecutorFact::Operation(observation)
            if observation.reply.kind() == novarocks_execution_contract::OperationKind::AbortParticipation)
        {
            let mut settlement = None;
            let mut cleanup = None;
            let mut fail_closed = false;
            let aborts = &mut self.aborts;
            let dispatch_action = self
                .acknowledgements
                .apply(
                    claim.source,
                    claim.input,
                    &fact,
                    &mut self.execution,
                    |execution, observation| {
                        let resolved = aborts.observe(execution, observation);
                        settlement = resolved.settlement;
                        cleanup = resolved.cleanup;
                        fail_closed = resolved.domain.is_err();
                        resolved.domain
                    },
                )
                .map_err(|_| schedule("invalid actor Abort fact in Native prefix"))?;
            if let Some(body) = settlement {
                result.abort_settlement = Some(self.abort_effect(body)?);
            }
            if fail_closed {
                self.request_stop(result);
            }
            debug_assert!(cleanup.is_none() || dispatch_action.is_none());
            cleanup.or(dispatch_action)
        } else {
            self.apply_native(now, claim.source, claim.input, &fact, |_, _| {
                unreachable!("Abort is handled by its original Core owner")
            })?
        };
        if let Some(body) = action {
            let (id, _) = self.turn.issue()?;
            result.effect = Some(AttemptEffect {
                id,
                class: EffectClass::Cleanup,
                body,
            });
        }
        if self.execution.has_pending_quiesce() {
            self.turn.phase = Phase::NativeQuiesce;
            self.drive_native_quiesce(context)?;
        } else if self.acknowledgements.selected_source().is_none() {
            self.enter_common_or_skip()?;
        }
        Ok(())
    }

    fn drive_native_quiesce(
        &mut self,
        context: &mut dyn StepContext,
    ) -> Result<(), TaskExecutionError> {
        if let Some(result) = self.execution.advance_quiesce(context) {
            self.acknowledgements
                .complete_deferred_ack(result)
                .map_err(|_| schedule("Quiesce lost its original Native prefix"))?;
            if self.acknowledgements.selected_source().is_some() {
                self.turn.phase = Phase::Native;
            } else {
                self.enter_common_or_skip()?;
            }
        }
        Ok(())
    }

    fn enter_common_or_skip(&mut self) -> Result<(), TaskExecutionError> {
        if self.acknowledgements.run_round_tail() == Ok(true) {
            self.turn.phase = Phase::Common {
                source_ready: false,
            };
        } else {
            self.begin_round_tail(true)?;
            self.turn.phase = Phase::Tail;
        }
        Ok(())
    }

    fn consume_common(
        &mut self,
        now: MonotonicInstant,
        claim: AttemptInputClaim,
        input: AttemptInput,
        context: &mut dyn StepContext,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        let seal = match input {
            AttemptInput::Executor(ExecutorFact::Observations(batch)) => {
                let pass = self.begin_observations(now, claim.source, batch)?;
                return self.drive_observation_pass(now, pass, context);
            }
            AttemptInput::Port(port) => {
                let RootControlApplication {
                    seal,
                    terminal_control,
                } = self.apply_root_control(now, claim.source, port)?;
                result.root_control_processed = Some(claim.input);
                if let Some((token, settled)) = terminal_control {
                    result.effect = Some(self.port_effect(
                        AttemptToLogicalBody::TerminalControlResult {
                            token,
                            result: settled.map_err(protocol_failure),
                        },
                    )?);
                }
                seal
            }
            _ => return Err(schedule("observation Source supplied an unrelated input")),
        };
        self.finish_common(seal)
    }

    fn drive_observation_pass(
        &mut self,
        now: MonotonicInstant,
        mut pass: super::ObservationPass,
        context: &mut dyn StepContext,
    ) -> Result<(), TaskExecutionError> {
        if let Some(seal) = self.advance_observations(now, &mut pass, context) {
            self.finish_common(seal)
        } else {
            self.turn.observation = Some(pass);
            self.turn.phase = Phase::Observations;
            Ok(())
        }
    }

    fn finish_common(
        &mut self,
        seal: Option<super::SealedRootCut>,
    ) -> Result<(), TaskExecutionError> {
        if self.acknowledgements.run_round_tail() == Ok(true) || seal.is_some() {
            self.select_root(seal)?;
        } else {
            self.begin_round_tail(true)?;
            self.turn.phase = Phase::Tail;
        }
        Ok(())
    }

    fn consume_independent(
        &mut self,
        now: MonotonicInstant,
        claim: AttemptInputClaim,
        input: AttemptInput,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        match input {
            AttemptInput::Timer => self.verify_source(claim.source, self.sources.actor)?,
            AttemptInput::CapacityReleased => {
                self.verify_source(claim.source, self.sources.capacity)?
            }
            AttemptInput::Membership(_) => {}
            AttemptInput::Executor(fact) => match fact {
                ExecutorFact::SubscriptionProgress(_)
                | ExecutorFact::ReadState(_)
                | ExecutorFact::ReadProgress(_)
                | ExecutorFact::SubscriptionExited { .. } => {
                    self.verify_source(claim.source, self.sources.subscription)?;
                    self.apply_subscription_input(now, &fact)?
                }
                ExecutorFact::Withdrawn { .. } | ExecutorFact::QueuedInputWithdrawn { .. } => {
                    self.verify_source(claim.source, self.sources.transport)?;
                    let mut abort = None;
                    self.apply_transport_withdrawal(now, &fact, |operation| {
                        // An emitted batch targets one frozen backend, which
                        // has at most one logical actor Abort owner.
                        if abort.replace(operation).is_some() {
                            return Err(schedule(
                                "one Native batch contained multiple actor Aborts",
                            ));
                        }
                        Ok(())
                    })?;
                    if let Some(operation) = abort
                        && let Some(body) = self.aborts.unsent(operation, now)?
                    {
                        result.abort_settlement = Some(self.abort_effect(body)?);
                    }
                }
                ExecutorFact::QueuedInputGranted {
                    operation, grant, ..
                } => {
                    self.verify_source(claim.source, self.sources.transport)?;
                    let cleanup = if self.execution.tracks_pending_update_grant(grant) {
                        None
                    } else if self.aborts.owns(operation) {
                        self.aborts.grant(&mut self.execution, operation, grant)?
                    } else {
                        let progress = if self.execution.has_lifecycle_admission(operation) {
                            self.execution.observe_lifecycle_grant(operation, grant)?
                        } else {
                            self.execution.observe_create_grant(operation, grant)?
                        };
                        match progress {
                            CreateGrantProgress::Release(grant) => {
                                Some(EffectBody::ReleaseQueuedInput { grant })
                            }
                            _ => None,
                        }
                    };
                    if let Some(body) = cleanup {
                        let (id, _) = self.turn.issue()?;
                        result.effect = Some(AttemptEffect {
                            id,
                            class: EffectClass::Cleanup,
                            body,
                        });
                    }
                }
                ExecutorFact::QueuedInputFailed {
                    operation,
                    generation,
                    failure,
                } => {
                    self.verify_source(claim.source, self.sources.transport)?;
                    if let Some(settlement) = self.aborts.admission_failed(operation, generation) {
                        if let Some(body) = settlement {
                            result.abort_settlement = Some(self.abort_effect(body)?);
                        }
                        self.request_stop(result);
                        self.round_failure.get_or_insert(failure);
                        self.begin_round_tail(true)?;
                        self.turn.phase = Phase::Tail;
                    } else if self.execution.complete_lifecycle_admission(
                        operation,
                        generation,
                        Some(&failure),
                    ) || self.execution.close_create_admission(operation, generation)
                    {
                        self.round_failure.get_or_insert(failure);
                        self.begin_round_tail(true)?;
                        self.turn.phase = Phase::Tail;
                    }
                }
                ExecutorFact::DiagnosticsFinished {
                    request,
                    task,
                    result: read,
                } => {
                    let (action, _) = self.turn.issue()?;
                    self.observe_diagnostic_result(action, request, task, read, result);
                }
                ExecutorFact::DynamicFiltersRead {
                    request,
                    task,
                    result,
                } => {
                    if let Some(failure) =
                        self.execution.observe_dynamic_read(request, task, result)
                    {
                        self.round_failure.get_or_insert(failure);
                        self.begin_round_tail(true)?;
                        self.turn.phase = Phase::Tail;
                    }
                }
                ExecutorFact::DynamicFilterFeedbackAdmitted {
                    request,
                    task,
                    result,
                } => {
                    if let Some(failure) = self
                        .execution
                        .observe_dynamic_admission(request, task, result)
                    {
                        self.round_failure.get_or_insert(failure);
                        self.begin_round_tail(true)?;
                        self.turn.phase = Phase::Tail;
                    }
                }
                ExecutorFact::PumpTaskUpdate {
                    pump,
                    delivery,
                    task,
                    domains,
                } => {
                    self.execution
                        .accept_split_delivery(pump, delivery, task, domains)?;
                }
                ExecutorFact::PumpFailed { pump, failure }
                    if self.execution.is_split_pump(pump) =>
                {
                    self.record_split_failure(failure, result)?;
                }
                ExecutorFact::PumpFinished {
                    pump,
                    result: finished,
                } if self.execution.is_split_pump(pump) => {
                    if self.execution.finish_split_pump(pump)
                        && let Err(failure) = finished
                    {
                        self.record_split_failure(failure, result)?;
                    }
                }
                ExecutorFact::SplitProfile { pump, observation }
                    if self.execution.is_split_pump(pump) =>
                {
                    self.execution.observe_split_diagnostic(pump, observation)?;
                }
                _ => return Err(schedule("executor input has no active protocol owner")),
            },
            _ => return Err(schedule("input has no active protocol owner")),
        }
        Ok(())
    }

    fn record_split_failure(
        &mut self,
        failure: AttemptFailure,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        if !self.turn.split_failed {
            self.turn.split_failed = true;
            self.turn.convergence_changed |= self.convergence.upgrade_to_cleanup();
            self.request_stop(result);
            self.round_failure.get_or_insert(failure);
            self.begin_round_tail(true)?;
            self.turn.phase = Phase::Tail;
        }
        Ok(())
    }

    fn next_observation_recovery_deadline(&self) -> Option<MonotonicInstant> {
        if self.turn.observation_recovery_failed
            || self.stop.business_closed()
            || self.terminal.terminal().is_some()
        {
            return None;
        }
        self.execution
            .covered_recovery_deadline()
            .into_iter()
            .chain(self.execution.result_terminal_control_recovery_deadline())
            .min()
    }

    fn observation_recovery_failure(&self) -> Option<TaskExecutionError> {
        let now = self.execution.monotonic_now();
        self.next_observation_recovery_deadline()
            .filter(|&deadline| now.has_reached(deadline))?;
        if !self.root.pending() && self.execution.covered_recovery_expired() {
            Some(schedule(
                "required covered observation evidence could not be recovered within its budget",
            ))
        } else if self
            .execution
            .result_terminal_control_recovery_deadline()
            .is_some_and(|deadline| now.has_reached(deadline))
        {
            let root = self.execution.graph().root_identity();
            Some(TaskExecutionError::Schedule(format!(
                "required terminal control for revoked root {root:?} did not arrive within its recovery budget"
            )))
        } else {
            None
        }
    }

    fn select_root(&mut self, cut: Option<SealedRootCut>) -> Result<(), TaskExecutionError> {
        // Decide after the selected Native and ordered observation inputs.
        // If they selected a seal reply, deliver it at its original position.
        if cut.is_none()
            && let Some(failure) = self.observation_recovery_failure()
        {
            self.turn.observation_recovery_failed = true;
            self.round_failure
                .get_or_insert_with(|| protocol_failure(failure));
            self.begin_round_tail(true)?;
            self.turn.phase = Phase::Tail;
            return Ok(());
        }
        let cut = cut.or_else(|| {
            if !self.stop.business_closed() {
                return None;
            }
            self.root
                .refuse(RootSealRefusal::AttemptCleanup)
                .map(|settlement| SealedRootCut {
                    root_before_seal: None,
                    settlement,
                })
        });
        let (projection, seal) = cut.map_or_else(
            || {
                (
                    (!self.root.sealed() && !self.stop.business_closed())
                        .then(|| self.root_projection())
                        .flatten(),
                    None,
                )
            },
            |cut| {
                let projection = cut.root_before_seal.or_else(|| {
                    (self.acknowledgements.run_round_tail() == Ok(true)
                        && !self.stop.business_closed())
                    .then(|| self.root_projection())
                    .flatten()
                });
                (projection, Some(cut.settlement))
            },
        );
        // The development observer follows the accepted Task even after the
        // product result seal has consumed its root publisher.
        let dev_observation = if self.turn.observe_dev_root {
            let root = self.execution.graph().root_identity();
            self.execution
                .task(root.task_id())
                .and_then(|task| task.status().map(|status| (status, task.installed())))
                .filter(|(status, _)| status.identity() == root)
                .map(|(status, installed)| (status.clone(), installed))
        } else {
            None
        };
        self.turn.root = Some(RootWork {
            dev_observation,
            projection,
            seal,
            publication_failure: None,
        });
        self.turn.phase = Phase::Root;
        Ok(())
    }

    fn drive_root(
        &mut self,
        context: &mut dyn StepContext,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        let root = self.turn.root.as_mut().expect("selected root work");
        if root.dev_observation.is_some() {
            if !context.charge(1) {
                return Ok(());
            }
            let (status, installed) = root.dev_observation.take().expect("selected observation");
            let (id, _) = self.turn.issue()?;
            result.effect = Some(AttemptEffect {
                id,
                class: EffectClass::Publication,
                body: EffectBody::Publish(Publication::DevRootObservation { status, installed }),
            });
            self.turn.phase = Phase::AwaitRoot(id, PublicationApplied::DevRootObservation);
            return Ok(());
        }
        if root.projection.is_some() {
            if !context.charge(1) {
                return Ok(());
            }
            let projection = root.projection.take().expect("selected projection");
            let (id, _) = self.turn.issue()?;
            result.effect = Some(AttemptEffect {
                id,
                class: EffectClass::Publication,
                body: EffectBody::Publish(Publication::RootStatus(Box::new(projection))),
            });
            self.turn.phase = Phase::AwaitRoot(id, PublicationApplied::RootStatus);
            return Ok(());
        }
        if root.seal.is_some() {
            if !context.charge(1) {
                return Ok(());
            }
            let seal = root.seal.take().expect("selected seal result");
            let settled = if let Some(failure) = root.publication_failure.take() {
                Err(failure)
            } else {
                seal.result
                    .map_err(|refusal| protocol_failure(schedule(refusal.message())))
            };
            let success = settled.is_ok();
            let effect = self.port_effect(AttemptToLogicalBody::SealResult {
                token: seal.token,
                result: settled,
            })?;
            self.turn.phase = Phase::AwaitSeal {
                effect: effect.id,
                success,
            };
            self.turn.root = None;
            result.effect = Some(effect);
            return Ok(());
        }
        self.turn.root = None;
        self.begin_round_tail(true)?;
        self.turn.phase = Phase::Tail;
        Ok(())
    }

    fn apply_tail(
        &mut self,
        now: MonotonicInstant,
        claim: AttemptInputClaim,
        input: AttemptInput,
        context: &mut dyn StepContext,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        match input {
            AttemptInput::Timer => self.verify_source(claim.source, self.sources.actor)?,
            AttemptInput::Executor(ExecutorFact::PublicationApplied { effect, result }) => {
                self.verify_source(claim.source, self.sources.publication)?;
                self.protocol_publication_applied(effect, result);
            }
            AttemptInput::Port(port) => {
                self.verify_source(claim.source, self.sources.qa)?;
                self.accept_qa(port, result)?;
                // The QA result position cannot retire while this input has
                // effects. Drive the next settlement from the original timer
                // so it can borrow the same position after actual retirement.
                return Ok(());
            }
            _ => return Err(schedule("primary input crossed the original round tail")),
        }
        let (action, token) = self.turn.issue()?;
        match self.advance_round_tail(now, action, token, context)? {
            RoundTailStep::Yield | RoundTailStep::AwaitPublication | RoundTailStep::AwaitQa => {}
            RoundTailStep::Effect(effect) => result.effect = Some(effect),
            RoundTailStep::ExpiredAbort { release, operation } => {
                result.effect = Some(release);
                if let Some(body) = self.aborts.unsent(operation, now)? {
                    result.abort_settlement = Some(self.abort_effect(body)?);
                }
                result.definitely_unsent_abort = Some(operation);
            }
            RoundTailStep::RetiredAuthorization(operation) => {
                result.retired_authorization = Some(operation)
            }
            RoundTailStep::Complete(round) => {
                if let Err(failure) = &round {
                    self.record_terminal_failure(failure.clone())?;
                }
                result.round_result = Some(round);
                if self.terminal_needs_drive() {
                    self.turn.cancellation_due = false;
                    self.turn.phase = Phase::Terminal(TerminalWake::Round);
                    self.drive_terminal(now, TerminalWake::Round, false, context, result)?;
                } else {
                    self.begin_candidates_or_resume();
                }
            }
        }
        Ok(())
    }

    fn begin_candidates_or_resume(&mut self) {
        if self.candidates.enabled && !self.stop.business_closed() {
            self.candidates.begin_round();
            self.turn.phase = Phase::Candidates;
        } else {
            self.resume_after_terminal();
        }
    }

    fn drive_candidates(
        &mut self,
        context: &mut dyn StepContext,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        use super::AuthorizationStep;
        use super::candidates::{Authorization, Maintenance, TaskCandidate};
        use crate::execution::{EdgeAdmissionProgress, ReleaseProgress};
        if self.authorizations.waiting_for_reply() {
            return Ok(());
        }
        if self.stop.business_closed() {
            self.candidates.authorization = None;
            self.candidates.wake = false;
            self.resume_after_terminal();
            return Ok(());
        }
        if !self.execution.consume_create_retry_wakes(context) {
            return Ok(());
        }
        if !self.candidates.stage_cancels_done {
            while let Some((operation, request)) = self
                .execution
                .next_stage_cancel_admission(self.candidates.stage_cancel_after)
            {
                if !context.charge(1) {
                    return Ok(());
                }
                self.candidates.stage_cancel_after = Some(operation);
                let (id, _) = self.turn.issue()?;
                if let Some(effect) = self.execution.begin_stage_cancel_admission(request, id)? {
                    result.effect = Some(effect);
                    return Ok(());
                }
            }
            self.candidates.stage_cancels_done = true;
        }
        if !self.candidates.splits_done {
            while let Some(task) = self.execution.split_task_after(self.candidates.split_after) {
                if !context.charge(1) {
                    return Ok(());
                }
                self.candidates.split_after = Some(task);
                let (id, _) = self.turn.issue()?;
                if let Some(effect) = self.execution.prepare_split_delivery(task, id)? {
                    result.effect = Some(effect);
                    return Ok(());
                }
            }
            self.candidates.splits_done = true;
        }
        if !self.candidates.edges_done {
            while let Some(producer) = self
                .execution
                .edge_producer_after(self.candidates.edge_after)
            {
                if !context.charge(1) {
                    return Ok(());
                }
                let (id, _) = self.turn.issue()?;
                match self.execution.advance_edge_admission(producer, id)? {
                    EdgeAdmissionProgress::Admitting(effect) => {
                        self.candidates.edge_after = Some(producer);
                        result.effect = Some(effect);
                        return Ok(());
                    }
                    EdgeAdmissionProgress::Blocked => self.candidates.edge_after = Some(producer),
                    EdgeAdmissionProgress::Discarded => {}
                }
            }
            self.candidates.edges_done = true;
        }
        if self.candidates.authorization.is_some() {
            if !context.charge(1) {
                return Ok(());
            }
            let Some(Authorization::Ready(intent)) = self.candidates.authorization.take() else {
                return Err(schedule("candidate lost its original authorization result"));
            };
            let (id, _) = self.turn.issue()?;
            result.effect = Some(self.execution.begin_lifecycle_admission(intent, id)?);
            self.candidates.creates = true;
            self.candidates.wake = self
                .execution
                .participation_after(self.candidates.participant_after)
                .is_some();
            return Ok(());
        }
        while !self.candidates.creates {
            if let Some(phase) = &mut self.candidates.maintaining {
                let participation = self
                    .candidates
                    .participant_after
                    .expect("active lifecycle candidate");
                let intent = match phase {
                    Maintenance::Quiesce => {
                        if !context.charge(1) {
                            return Ok(());
                        }
                        *phase = Maintenance::Renew;
                        self.execution.next_quiesce_intent(participation)
                    }
                    Maintenance::Renew => {
                        if !context.charge(1) {
                            return Ok(());
                        }
                        *phase = Maintenance::Release(Default::default());
                        self.execution.next_renewal_intent(participation)?
                    }
                    Maintenance::Release(proof) => {
                        let ReleaseProgress::Done(intent) = self
                            .execution
                            .advance_release_candidate(participation, proof, context)?
                        else {
                            return Ok(());
                        };
                        self.candidates.maintaining = None;
                        intent
                    }
                };
                if let Some(intent) = intent {
                    let (id, _) = self.turn.issue()?;
                    result.effect = Some(self.execution.begin_lifecycle_admission(intent, id)?);
                    return Ok(());
                }
                continue;
            }
            let Some(participation) = self
                .execution
                .participation_after(self.candidates.participant_after)
            else {
                self.candidates.creates = true;
                break;
            };
            if !context.charge(1) {
                return Ok(());
            }
            self.candidates.participant_after = Some(participation);
            let Some(intent) = self.execution.next_authorization_intent(
                participation,
                self.establish_material[&participation],
            )?
            else {
                self.candidates.maintaining = Some(Maintenance::Quiesce);
                continue;
            };
            let (id, token) = self.turn.issue()?;
            match self.authorize_candidate(&intent, token, id)? {
                AuthorizationStep::Request(effect) => {
                    self.candidates.authorization = Some(Authorization::Waiting(intent));
                    result.effect = Some(effect);
                    return Ok(());
                }
                AuthorizationStep::Ready => {
                    result.effect = Some(self.execution.begin_lifecycle_admission(intent, id)?);
                    self.candidates.creates = true;
                    self.candidates.wake = self
                        .execution
                        .participation_after(self.candidates.participant_after)
                        .is_some();
                    return Ok(());
                }
                AuthorizationStep::Waiting | AuthorizationStep::Retired => {
                    return Err(schedule(
                        "candidate crossed its original QA authorization phase",
                    ));
                }
            }
        }
        while let Some(task) = self
            .execution
            .graph()
            .task_after(self.candidates.task_after)
            .map(|task| task.identity())
        {
            if !context.charge(1) {
                return Ok(());
            }
            let (id, _) = self.turn.issue()?;
            let effect = match self.candidates.task_candidate {
                TaskCandidate::Cancel => {
                    self.candidates.task_candidate = TaskCandidate::Create;
                    self.execution
                        .next_task_cancel_intent(task)?
                        .map(|intent| self.execution.begin_lifecycle_admission(intent, id))
                        .transpose()?
                }
                TaskCandidate::Create => {
                    self.candidates.task_candidate = TaskCandidate::Update;
                    self.execution.begin_create_admission(task, id)?
                }
                TaskCandidate::Update => {
                    self.candidates.task_candidate = TaskCandidate::Cancel;
                    self.candidates.task_after = Some(task.task_id());
                    let (queued, effect) = self.execution.advance_update_send(task, id)?;
                    self.candidates.wake |= queued;
                    effect
                }
            };
            if let Some(effect) = effect {
                result.effect = Some(effect);
                return Ok(());
            }
        }
        self.resume_after_terminal();
        Ok(())
    }

    fn record_terminal_failure(
        &mut self,
        failure: AttemptFailure,
    ) -> Result<(), TaskExecutionError> {
        match failure {
            AttemptFailure::TaskProtocol(error) => {
                self.terminal
                    .record_execution_failure(*error, &self.execution)?;
            }
            failure => {
                self.terminal.record_original_failure(failure)?;
            }
        }
        Ok(())
    }

    fn terminal_needs_drive(&self) -> bool {
        self.terminal.terminal().is_none()
            && (self.turn.seal_delivered && self.execution.normal_drain_started()
                || self.terminal.has_pending_decision()
                || self.execution.selected_task_failure().is_some())
    }

    fn resume_after_terminal(&mut self) {
        self.turn.phase = if !self.stop.pending() && self.authorizations.close_needs_drive() {
            Phase::ClosingGate
        } else if !self.stop.pending()
            && !self.stand_down_due(self.execution.monotonic_now())
            && (self.start_convergence_pass() || self.convergence_progress_ready())
        {
            Phase::Converge
        } else {
            Phase::Idle
        };
    }

    fn apply_terminal(
        &mut self,
        now: MonotonicInstant,
        claim: AttemptInputClaim,
        input: AttemptInput,
        wake: TerminalWake,
        context: &mut dyn StepContext,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        let defer_publication = matches!(&input, AttemptInput::Port(_));
        let wake = match input {
            AttemptInput::Timer => {
                self.verify_source(claim.source, self.sources.actor)?;
                wake
            }
            AttemptInput::Port(port) => {
                self.verify_source(claim.source, self.sources.qa)?;
                let LogicalToAttemptBody::PreReadyReplacementReply { token, may_observe } =
                    port.body
                else {
                    return Err(schedule("terminal decision awaited original permission"));
                };
                if port.activation == self.activation {
                    self.terminal.permission_reply(token, may_observe);
                }
                TerminalWake::Continuation
            }
            AttemptInput::Executor(ExecutorFact::ProcessSnapshot {
                request,
                participation,
                sampled_at,
                observation,
            }) => {
                self.verify_source(claim.source, self.sources.process)?;
                if self
                    .execution
                    .graph()
                    .participation_on_backend(participation.backend_process_id())
                    != Some(participation)
                {
                    return Err(schedule(
                        "terminal process sample belongs to a foreign participation",
                    ));
                }
                if let Some(original) = self.terminal.observation() {
                    let horizon = OriginalHorizon::from_execution(&self.execution, original);
                    self.terminal.process_reply(
                        now,
                        request,
                        participation.backend_process_id(),
                        sampled_at,
                        observation,
                        horizon,
                    )?;
                }
                TerminalWake::Continuation
            }
            _ => return Err(schedule("input crossed the original terminal decision")),
        };
        self.drive_terminal(now, wake, defer_publication, context, result)
    }

    fn drive_terminal(
        &mut self,
        now: MonotonicInstant,
        wake: TerminalWake,
        defer_publication: bool,
        context: &mut dyn StepContext,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        if self.terminal.waiting_for_permission() || self.terminal.waiting_for_process() {
            return Ok(());
        }
        if !context.charge(1) {
            self.turn.phase = Phase::Terminal(wake);
            return Ok(());
        }
        let (_, token) = self.turn.issue()?;
        let horizon = self
            .terminal
            .observation()
            .map(|observation| OriginalHorizon::from_execution(&self.execution, observation));
        let facts = TerminalFacts {
            success_ready: self.turn.seal_delivered && self.execution.normal_drain_started(),
            selected_task: self.execution.selected_task_failure(),
            gate: self.authorizations.preparation(),
            horizon,
            wake,
        };
        match self.terminal.poll(now, facts, Some(token))? {
            TerminalDirective::PrepareConvergence => {
                self.authorizations.request_close();
                self.turn.terminal_after_close = Some(wake);
                self.turn.phase = Phase::ClosingGate;
            }
            TerminalDirective::QueryPermission { request, refusal } => {
                let mut effect =
                    self.port_effect(AttemptToLogicalBody::PreReadyReplacementQuery {
                        token: request,
                        refusal,
                    })?;
                effect.class = EffectClass::Settlement;
                result.effect = Some(effect);
                self.turn.phase = Phase::Terminal(TerminalWake::Continuation);
            }
            TerminalDirective::ObserveProcess { request, original } => {
                let participation = self
                    .execution
                    .graph()
                    .participation_on_backend(original)
                    .ok_or_else(|| {
                        schedule("terminal observation has no original participation")
                    })?;
                let (id, _) = self.turn.issue()?;
                result.effect = Some(AttemptEffect {
                    id,
                    class: EffectClass::Settlement,
                    body: EffectBody::ObserveProcess {
                        request,
                        participation,
                    },
                });
                self.turn.phase = Phase::Terminal(TerminalWake::Continuation);
            }
            TerminalDirective::Selected(terminal) => {
                if matches!(
                    terminal,
                    novarocks_coordination_contract::AttemptTerminal::Failed(_)
                ) {
                    self.request_stop(result);
                }
                if defer_publication {
                    // Keep the decision at its permission-reply cut, but let
                    // that input retire before publishing through the same QA lane.
                    self.turn.phase = Phase::PublishTerminal;
                } else {
                    self.publish_terminal(terminal, result)?;
                }
            }
            TerminalDirective::Wait => self.resume_after_terminal(),
            TerminalDirective::NeedRequestCredit => {
                return Err(schedule("charged terminal request lost its identity"));
            }
        }
        Ok(())
    }

    fn publish_terminal(
        &mut self,
        terminal: novarocks_coordination_contract::AttemptTerminal<AttemptFailure>,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        let normal = matches!(
            terminal,
            novarocks_coordination_contract::AttemptTerminal::Completed
        );
        result.effect = Some(self.port_effect(AttemptToLogicalBody::AttemptTerminal(terminal))?);
        if normal {
            self.begin_candidates_or_resume();
        } else {
            self.resume_after_terminal();
        }
        Ok(())
    }

    fn accept_qa(
        &mut self,
        port: novarocks_coordination_contract::LogicalToAttempt<AttemptFailure>,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        if !matches!(
            &port.body,
            LogicalToAttemptBody::AuthorizationReply { .. }
                | LogicalToAttemptBody::EstablishSettlementReply { .. }
                | LogicalToAttemptBody::EstablishDetachReply { .. }
        ) {
            return Err(schedule("QA continuation received an unrelated port reply"));
        }
        match self.apply_qa_reply(port)? {
            QaReply::ObserverComplete { retire } => result.retired_authorization = retire,
            QaReply::Authorized(operation) if self.turn.phase == Phase::Candidates => {
                use super::candidates::Authorization;
                let Some(Authorization::Waiting(intent)) = self.candidates.authorization.take()
                else {
                    return Err(schedule("QA reply lacks its original candidate"));
                };
                if intent.operation_id() != operation {
                    return Err(schedule(
                        "QA reply authorized a different original candidate",
                    ));
                }
                self.candidates.authorization = Some(Authorization::Ready(intent));
            }
            QaReply::ClosingFailed(failure) => {
                self.record_terminal_failure(failure)?;
                self.turn.phase = Phase::Terminal(
                    self.turn
                        .terminal_after_close
                        .take()
                        .unwrap_or(TerminalWake::Round),
                );
            }
            QaReply::AuthorizationFailed { failure, .. } => {
                self.record_terminal_failure(failure)?;
                if matches!(self.turn.phase, Phase::Idle | Phase::Candidates) {
                    self.candidates.authorization = None;
                    self.turn.phase = Phase::Terminal(TerminalWake::Round);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn abort_effect(
        &mut self,
        body: AttemptToLogicalBody<AttemptFailure>,
    ) -> Result<AttemptEffect, TaskExecutionError> {
        let mut effect = self.port_effect(body)?;
        effect.class = EffectClass::Settlement;
        Ok(effect)
    }

    fn port_effect(
        &mut self,
        body: AttemptToLogicalBody<AttemptFailure>,
    ) -> Result<AttemptEffect, TaskExecutionError> {
        let (id, _) = self.turn.issue()?;
        Ok(AttemptEffect {
            id,
            class: EffectClass::Reply,
            body: EffectBody::Port(AttemptToLogical {
                activation: self.activation,
                action: id,
                body,
            }),
        })
    }

    fn verify_source(
        &self,
        actual: AttemptSourceId,
        expected: AttemptSourceId,
    ) -> Result<(), TaskExecutionError> {
        if actual != expected {
            return Err(schedule("continuation used a foreign Source"));
        }
        Ok(())
    }
}

fn protocol_failure(error: TaskExecutionError) -> AttemptFailure {
    AttemptFailure::TaskProtocol(Box::new(error))
}
