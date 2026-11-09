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
    authorization::gate::QaReply,
    capacity::StepContext,
    convergence::RemoteConvergenceDirective,
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
};
use novarocks_coordination_contract::{
    AttemptActionId, AttemptConvergenceMode, AttemptRequestToken, AttemptRootProjection,
    AttemptToLogical, AttemptToLogicalBody, LogicalToAttemptBody, MonotonicInstant,
};
use novarocks_execution_contract::TaskOperationId;
use std::num::NonZeroU64;

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
    pub root_control_processed: Option<AttemptInputId>,
    pub definitely_unsent_abort: Option<TaskOperationId>,
    pub retired_authorization: Option<TaskOperationId>,
    /// The original round result goes to the terminal controller.
    pub round_result: Option<Result<(), AttemptFailure>>,
}

#[derive(Debug, Default, PartialEq, Eq)]
enum Phase {
    #[default]
    Bootstrap,
    Idle,
    Native,
    Common {
        source_ready: bool,
    },
    Root,
    AwaitRoot(AttemptActionId),
    Tail,
    Converge,
}

#[derive(Debug)]
struct RootWork {
    projection: Option<AttemptRootProjection>,
    seal: Option<SealSettlement>,
    publication_failure: Option<AttemptFailure>,
}

#[derive(Debug, Default)]
pub(super) struct Turn {
    phase: Phase,
    next: u64,
    root: Option<RootWork>,
    local_exited: bool,
    reported_local_exit: bool,
    reported_remote_closure: bool,
    membership_revision: Option<u64>,
    convergence_changed: bool,
}

impl Turn {
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
        self.execution.set_now(now)?;
        let units = match &input {
            AttemptInput::Executor(ExecutorFact::Observations(batch)) => batch.items().len(),
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
            root_control_processed: None,
            definitely_unsent_abort: None,
            retired_authorization: None,
            round_result: None,
        };
        self.apply_claim(now, claim, input, context, &mut result)?;
        result.selection = self.input_selection();
        Ok(result)
    }

    fn next_deadline(&self) -> Option<MonotonicInstant> {
        match self.turn.phase {
            Phase::Bootstrap
            | Phase::Native
            | Phase::AwaitRoot(_)
            | Phase::Common { source_ready: true } => None,
            Phase::Idle if self.stop.pending() && !self.authorizations.waiting_for_reply() => {
                Some(self.execution.monotonic_now())
            }
            Phase::Idle if self.authorizations.waiting_for_reply() => None,
            Phase::Idle if self.aborts.has_pending() => Some(self.execution.monotonic_now()),
            Phase::Idle if self.convergence_due(self.execution.monotonic_now()) => {
                Some(self.execution.monotonic_now())
            }
            Phase::Idle => [self.next_round_deadline(), self.convergence.next_deadline()]
                .into_iter()
                .flatten()
                .min(),
            Phase::Tail => self.next_round_deadline(),
            Phase::Converge
                if self.convergence.waiting_for_process() && !self.convergence_progress_ready() =>
            {
                None
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
        match self.turn.phase {
            Phase::Bootstrap | Phase::Root => Only(self.sources.actor),
            Phase::Native => Only(self.sources.native),
            Phase::Common { .. } => Only(self.sources.observation),
            Phase::AwaitRoot(_) => Only(self.sources.publication),
            Phase::Tail
                if self.round_tail.waiting_for_publication()
                    && self.publication.has_pending_apply() =>
            {
                Only(self.sources.publication)
            }
            Phase::Tail | Phase::Idle if self.authorizations.waiting_for_reply() => {
                Only(self.sources.qa)
            }
            Phase::Tail => Only(self.sources.actor),
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
                    && self.convergence_due(now)
                    && (self.start_convergence_pass() || self.convergence_progress_ready())
                {
                    self.turn.phase = Phase::Converge;
                    return self.drive_convergence(now, context, result);
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
                        self.consume_native(now, claim, input, result)?;
                    }
                    return Ok(());
                }
                self.begin_native_prefix(now, cut)?;
                self.enter_common_or_skip()?;
                if self.turn.phase == Phase::Tail {
                    return Ok(());
                }
                if claim.source == self.sources.observation {
                    self.consume_common(now, claim, input, result)?;
                } else {
                    self.consume_independent(now, claim, input, result)?;
                }
            }
            Phase::Native if timer => {}
            Phase::Native => self.consume_native(now, claim, input, result)?,
            Phase::Common { .. } if timer => {
                let cut = claim
                    .selected_cut
                    .filter(|cut| cut.source == self.sources.observation)
                    .ok_or_else(|| schedule("timer lacks its selected observation Source cut"))?;
                if cut.tail.is_some() {
                    self.turn.phase = Phase::Common { source_ready: true };
                } else {
                    let seal = self.evaluate_pending_seal();
                    self.select_root(seal);
                }
            }
            Phase::Common { .. } => self.consume_common(now, claim, input, result)?,
            Phase::Root if timer => self.drive_root(context, result)?,
            Phase::AwaitRoot(_) if timer => {}
            Phase::AwaitRoot(expected) => {
                let AttemptInput::Executor(ExecutorFact::PublicationApplied {
                    effect,
                    result: applied,
                }) = input
                else {
                    return Err(schedule("root publication awaited its actual result"));
                };
                self.verify_source(claim.source, self.sources.publication)?;
                if effect == expected
                    && (applied.is_err() || matches!(applied, Ok(PublicationApplied::RootStatus)))
                {
                    let root = self.turn.root.as_mut().expect("original root publication");
                    if let Err(failure) = applied {
                        root.publication_failure = Some(failure.clone());
                        self.round_failure.get_or_insert(failure);
                    }
                    self.turn.phase = Phase::Root;
                }
            }
            Phase::Tail => self.apply_tail(now, claim, input, context, result)?,
            Phase::Converge if timer => self.drive_convergence(now, context, result)?,
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
                self.drive_convergence(now, context, result)?;
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
            AttemptInput::Port(port) => match &port.body {
                LogicalToAttemptBody::Abort { action, request } => {
                    self.verify_owner_port(claim.source, port.activation)?;
                    let (admission, _) = self.turn.issue()?;
                    self.aborts.begin(
                        &self.execution,
                        *action,
                        *request,
                        crate::input::SendGeneration::new(admission),
                    )?;
                }
                LogicalToAttemptBody::Stop { reason, failure } => {
                    self.verify_owner_port(claim.source, port.activation)?;
                    self.terminal.observe_stop(*reason, failure.clone());
                    self.diagnostics.stop_starting();
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
                            self.diagnostics.begin(*ready)?;
                            self.execution
                                .retain_profile_diagnostics_until(ready.diagnostic_deadline);
                        }
                    } else {
                        self.diagnostics.stop_starting();
                    }
                    self.convergence.begin(*mode, *remote_deadline);
                    self.turn.convergence_changed = true;
                    if *mode == AttemptConvergenceMode::Cleanup {
                        self.request_stop(result);
                    }
                    if self.turn.phase == Phase::Idle
                        && !self.stop.pending()
                        && !self.authorizations.waiting_for_reply()
                        && (self.start_convergence_pass() || self.convergence_progress_ready())
                    {
                        self.turn.phase = Phase::Converge;
                    }
                }
                _ => return Ok(false),
            },
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
                    self.drive_convergence(now, context, result)?;
                }
            }
            AttemptInput::Membership(view) => {
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
        if self.stop.request() {
            self.round_tail.suppress_dispatch();
            result.stop_producing = true;
        }
    }

    fn convergence_due(&self, now: MonotonicInstant) -> bool {
        self.convergence_progress_ready()
            || (self.convergence.mode().is_some()
                && self.convergence.closure().is_none()
                && (self.turn.convergence_changed
                    || self
                        .convergence
                        .next_deadline()
                        .is_some_and(|deadline| now.has_reached(deadline))))
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
            && ((self.turn.local_exited && !self.turn.reported_local_exit)
                || (self.convergence.closure().is_some() && !self.turn.reported_remote_closure))
    }

    fn drive_convergence(
        &mut self,
        now: MonotonicInstant,
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
            self.turn.reported_remote_closure |= closure.is_some();
            self.turn.reported_local_exit |= self.turn.local_exited;
            result.effect = Some(self.port_effect(AttemptToLogicalBody::ConvergenceProgress {
                local_exited: self.turn.local_exited,
                closure,
            })?);
            return Ok(());
        }
        if self.convergence.waiting_for_process() || !context.charge(1) {
            return Ok(());
        }
        let (id, token) = self.turn.issue()?;
        match self
            .convergence
            .advance(now, &self.execution, context, Some(token))?
        {
            RemoteConvergenceDirective::ObserveProcess {
                request,
                participation,
            } => {
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
        if self.acknowledgements.selected_source().is_none() {
            self.enter_common_or_skip()?;
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
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        let seal = match input {
            AttemptInput::Executor(ExecutorFact::Observations(batch)) => {
                self.apply_observations(now, claim.source, batch)?
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
        if self.acknowledgements.run_round_tail() == Ok(true) || seal.is_some() {
            self.select_root(seal);
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
            AttemptInput::CapacityReleased => {}
            AttemptInput::Membership(_) => {}
            AttemptInput::Executor(fact) => match fact {
                ExecutorFact::SubscriptionProgress(_)
                | ExecutorFact::ReadState(_)
                | ExecutorFact::ReadProgress(_)
                | ExecutorFact::SubscriptionExited { .. } => {
                    self.apply_subscription_input(now, &fact)?
                }
                ExecutorFact::Withdrawn { .. } | ExecutorFact::QueuedInputWithdrawn { .. } => {
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
                    if let Some(operation) = abort {
                        let body = self.aborts.unsent(operation)?;
                        result.abort_settlement = Some(self.abort_effect(body)?);
                    }
                }
                ExecutorFact::QueuedInputGranted {
                    operation, grant, ..
                } => {
                    let cleanup = if self.aborts.owns(operation) {
                        self.aborts.grant(&mut self.execution, operation, grant)?
                    } else {
                        match self.execution.observe_create_grant(operation, grant)? {
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
                    if let Some(body) = self.aborts.admission_failed(operation, generation) {
                        result.abort_settlement = Some(self.abort_effect(body)?);
                        self.request_stop(result);
                        self.round_failure.get_or_insert(failure);
                        self.begin_round_tail(true)?;
                        self.turn.phase = Phase::Tail;
                    } else if self.execution.close_create_admission(operation, generation) {
                        self.round_failure.get_or_insert(failure);
                        self.begin_round_tail(true)?;
                        self.turn.phase = Phase::Tail;
                    }
                }
                _ => return Err(schedule("executor input has no active protocol owner")),
            },
            _ => return Err(schedule("input has no active protocol owner")),
        }
        Ok(())
    }

    fn select_root(&mut self, cut: Option<SealedRootCut>) {
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
        self.turn.root = Some(RootWork {
            projection,
            seal,
            publication_failure: None,
        });
        self.turn.phase = Phase::Root;
    }

    fn drive_root(
        &mut self,
        context: &mut dyn StepContext,
        result: &mut AttemptStep,
    ) -> Result<(), TaskExecutionError> {
        let root = self.turn.root.as_mut().expect("selected root work");
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
            self.turn.phase = Phase::AwaitRoot(id);
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
            result.effect = Some(self.port_effect(AttemptToLogicalBody::SealResult {
                token: seal.token,
                result: settled,
            })?);
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
            }
            _ => return Err(schedule("primary input crossed the original round tail")),
        }
        let (action, token) = self.turn.issue()?;
        match self.advance_round_tail(now, action, token, context)? {
            RoundTailStep::Yield | RoundTailStep::AwaitPublication | RoundTailStep::AwaitQa => {}
            RoundTailStep::Effect(effect) => result.effect = Some(effect),
            RoundTailStep::ExpiredAbort { release, operation } => {
                result.effect = Some(release);
                let body = self.aborts.unsent(operation)?;
                result.abort_settlement = Some(self.abort_effect(body)?);
                result.definitely_unsent_abort = Some(operation);
            }
            RoundTailStep::RetiredAuthorization(operation) => {
                result.retired_authorization = Some(operation)
            }
            RoundTailStep::Complete(round) => {
                self.turn.phase = if !self.stop.pending()
                    && (self.start_convergence_pass() || self.convergence_progress_ready())
                {
                    Phase::Converge
                } else {
                    Phase::Idle
                };
                result.round_result = Some(round);
            }
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
        ) {
            return Err(schedule("QA continuation received an unrelated port reply"));
        }
        match self.apply_qa_reply(port)? {
            QaReply::ObserverComplete { retire } => result.retired_authorization = retire,
            QaReply::AuthorizationFailed { failure, .. } => {
                self.terminal.record_original_failure(failure)?;
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
