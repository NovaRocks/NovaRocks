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

//! The Participation's one coalesced, prepaid self-source. It owns no callback,
//! foreign capture, Work or reply token. The original TIMER author remains
//! live until the actual role and every input/batch have drained.

use super::{ParticipationHostRecord, failure::ScopePublicationsDrained};
use crate::control_runtime::actors::{
    ParticipationActor, ParticipationMessage, WorkerAction, WorkerActor, WorkerMessage,
};
use crate::control_runtime_policy::StorageNeed;
use novarocks_control_runtime::{
    actor_turn::{
        ActorRegistration, ActorRuntime, ActorTurnError, EffectCompletionRefusal, EffectTicket,
        InputPhase, ReservedInputObligation, StandingInputObligation, TimerSourceState,
    },
    obligation::WorkObligation,
};
use novarocks_control_runtime_core::{
    completion::Consumption,
    identity::{ActorId, Generation, InputId, ScopedWorkRef},
    mailbox::InputClass,
};
use novarocks_execution_contract::ParticipationRef;
use novarocks_worker_protocol::{
    control::ControlEffect,
    creation::PayloadId,
    participation::{ParticipationEffect, ParticipationInput},
};

pub(super) const PARTICIPATION_INTERNAL_STORAGE: StorageNeed = StorageNeed {
    inputs: 1,
    batches: 1,
    effects: ParticipationActor::MAX_STEP_ACTIONS,
    requests: 0,
    cuts: 0,
    replies: 0,
    completions: 0,
    captures: 0,
    work: 0,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ParticipationInternalAdvance {
    Idle,
    Waiting(InputId),
    Submitted(InputId),
    AwaitingFailurePublication,
    Closed,
}

#[derive(Default)]
pub(in crate::control_runtime) struct ParticipationInternal {
    binding: Option<Binding>,
    permit: Option<ReservedInputObligation<WorkerActor>>,
    input_obligation: Option<StandingInputObligation<WorkerActor>>,
    pending: bool,
    // The same single input is borrowed by its physical-disposal owner until
    // that owner has saved the exact Core receipt. Wake hints cannot reap it.
    continuation: Option<InputId>,
    source_ended: bool,
    failed: bool,
    closed: bool,
}
struct Binding {
    runtime: ActorRuntime<WorkerActor>,
    registration: ActorRegistration,
    root: ActorId,
    participation: ParticipationRef,
    author: ScopedWorkRef,
}
impl ParticipationInternal {
    pub(super) fn detachment_source(
        &mut self,
    ) -> Result<Option<&mut StandingInputObligation<WorkerActor>>, ActorTurnError> {
        if self.pending || self.continuation.is_some() || self.failed || self.permit.is_some() {
            return Err(ActorTurnError::ActorNotRetired);
        }
        if self
            .input_obligation
            .as_ref()
            .map(StandingInputObligation::in_flight)
            .transpose()?
            .flatten()
            .is_some()
        {
            return Err(ActorTurnError::ActorNotRetired);
        }
        Ok(self.input_obligation.as_mut())
    }
    pub(super) fn execution_detached(&mut self) {
        // The same transaction has already removed the actual idle role.
        self.input_obligation = None;
        self.closed = true;
        self.source_ended = true;
    }
    pub(super) fn admit(
        &mut self,
        runtime: &ActorRuntime<WorkerActor>,
        registration: &ActorRegistration,
        root: ActorId,
        participation: ParticipationRef,
        author: &WorkObligation,
    ) -> Result<(), ActorTurnError> {
        if !registration.matches_scope(author.scope()) {
            return Err(ActorTurnError::InputAuthorMismatch);
        }
        runtime.validate_registration(registration)?;
        self.binding = Some(Binding {
            runtime: runtime.clone(),
            registration: registration.clone(),
            root,
            participation,
            author: author.reference(),
        });
        self.permit = Some(runtime.reserve_admitted_input(
            registration.actor(),
            registration.actor(),
            InputClass::Control,
            author,
        )?);
        match self
            .permit
            .take()
            .expect("original Participation self-source input")
            .into_standing_input()
        {
            Ok(standing_input) => self.input_obligation = Some(standing_input),
            Err((error, original)) => {
                self.permit = Some(original);
                return Err(error);
            }
        }
        Ok(())
    }
    fn binding(&self) -> Result<&Binding, ActorTurnError> {
        self.binding
            .as_ref()
            .ok_or(ActorTurnError::UnknownStandingInput)
    }
    fn accept_wake(&mut self, ticket: &EffectTicket<WorkerAction>) -> Result<(), ActorTurnError> {
        let original = self.binding()?;
        original
            .runtime
            .validate_registration(&original.registration)?;
        original.runtime.validate_effect_ticket(ticket)?;
        if ticket.actor != original.registration.actor()
            || !matches!(
                &ticket.effect,
                WorkerAction::Participation(ParticipationEffect::WakeInternal)
            )
        {
            return Err(ActorTurnError::PublicationTargetMismatch);
        }
        if self.closed || self.failed {
            return Err(ActorTurnError::Closed);
        }
        // Closing future sources does not discard a previously committed hint.
        // The actual batch is completed immediately, before reusing this role.
        self.pending = true;
        let original = self.binding()?;
        original.runtime.notify_host_actor(&original.registration)
    }
    pub(super) fn advance(&mut self) -> Result<ParticipationInternalAdvance, ActorTurnError> {
        if self.closed {
            return Ok(ParticipationInternalAdvance::Closed);
        }
        let original = self.binding()?;
        let standing_input = self
            .input_obligation
            .as_ref()
            .ok_or(ActorTurnError::UnknownStandingInput)?;
        if let Some(input) = standing_input.in_flight()? {
            if self.continuation == Some(input) {
                return Ok(ParticipationInternalAdvance::Waiting(input));
            }
            match original
                .runtime
                .retire_input(original.registration.actor(), input)
            {
                Ok(()) => {}
                Err(
                    ActorTurnError::InputStillQueued
                    | ActorTurnError::EffectsRemain
                    | ActorTurnError::ReservationsRemain,
                ) => return Ok(ParticipationInternalAdvance::Waiting(input)),
                Err(error) => return Err(error),
            }
        }
        if !self.pending {
            return Ok(ParticipationInternalAdvance::Idle);
        }
        if original
            .runtime
            .snapshot(original.registration.actor())?
            .failed
        {
            // A scheduling bool is not an input, decision or Work exit. Only
            // the real failed-owner stage below may withdraw this pure hint.
            return Ok(ParticipationInternalAdvance::AwaitingFailurePublication);
        }
        let input = standing_input
            .fill(WorkerMessage::Participation(
                ParticipationMessage::Command {
                    work: original.author,
                    request: None,
                    command: ParticipationInput::NextInternal,
                },
            ))
            .map_err(|(error, _neutral)| error)?;
        self.pending = false;
        Ok(ParticipationInternalAdvance::Submitted(input))
    }
    /// This bit is set only by discard_failed_hint after validating the
    /// original ScopePublicationsDrained stage; it is not a scope-state guess.
    pub(super) fn failed_publications_drained(&self) -> bool {
        self.failed
    }
    /// The only remaining input is the builtin supervisor, independent of
    /// this original self-source. No producer can create another callback.
    pub(super) fn ready_for_failure_tail(
        &self,
        supervisor: InputId,
    ) -> Result<bool, ActorTurnError> {
        if !self.failed || !self.source_ended || self.pending || self.continuation.is_some() {
            return Ok(false);
        }
        let original = self.binding()?;
        if self
            .input_obligation
            .as_ref()
            .map(StandingInputObligation::in_flight)
            .transpose()?
            .flatten()
            .is_some()
        {
            return Ok(false);
        }
        let snapshot = original.runtime.snapshot(original.registration.actor())?;
        Ok(!snapshot.external_open
            && snapshot.inputs == 1
            && snapshot.standing_inputs == usize::from(self.input_obligation.is_some())
            && original
                .runtime
                .pending_batches(original.registration.actor(), None, 2)?
                == [supervisor]
            && original
                .runtime
                .pending_timer_input(original.registration.actor())?
                .is_none())
    }
    /// A rejected creation keeps its original identity until this same
    /// completed-disposal fact is consumed. Its identity may then be retried.
    pub(super) fn try_submit_rejected_creation(
        &mut self,
        task: novarocks_execution_contract::TaskIdentity,
        payload: PayloadId,
    ) -> Result<Option<InputId>, ActorTurnError> {
        self.try_submit_continuation(ParticipationInput::RejectedCreationRetired { task, payload })
    }
    /// Accepted custody is the original RetireTask's identity and close epoch,
    /// carried only after its actual disposal and all independent tails exit.
    pub(super) fn try_submit_task_custody_retired(
        &mut self,
        task: novarocks_execution_contract::TaskIdentity,
        payload: PayloadId,
        close_epoch: Generation,
    ) -> Result<Option<InputId>, ActorTurnError> {
        self.try_submit_continuation(ParticipationInput::TaskExecutionCustodyRetired {
            task,
            payload,
            close_epoch,
        })
    }
    /// A real read-seal transfer releases an already registered original R.
    /// This uses the same self-source position, never another request author.
    pub(super) fn try_submit_task_read_resume(
        &mut self,
        request: InputId,
        task: novarocks_execution_contract::TaskIdentity,
    ) -> Result<Option<InputId>, ActorTurnError> {
        self.try_submit_continuation(ParticipationInput::ResumeTaskRead { request, task })
    }
    /// These continuations borrow the original single self-source position, in order
    /// with an existing hint. Busy retains the caller's same disposal owner.
    fn try_submit_continuation(
        &mut self,
        command: ParticipationInput,
    ) -> Result<Option<InputId>, ActorTurnError> {
        if self.closed || self.failed {
            return Err(ActorTurnError::Closed);
        }
        if self.continuation.is_some() {
            return Ok(None);
        }
        let original = self.binding()?;
        let standing_input = self
            .input_obligation
            .as_ref()
            .ok_or(ActorTurnError::UnknownStandingInput)?;
        if let Some(input) = standing_input.in_flight()? {
            match original
                .runtime
                .retire_input(original.registration.actor(), input)
            {
                Ok(()) => {}
                Err(
                    ActorTurnError::InputStillQueued
                    | ActorTurnError::EffectsRemain
                    | ActorTurnError::ReservationsRemain,
                ) => return Ok(None),
                Err(error) => return Err(error),
            }
        }
        let input = standing_input
            .fill(WorkerMessage::Participation(
                ParticipationMessage::Command {
                    work: original.author,
                    request: None,
                    command,
                },
            ))
            .map_err(|(error, _neutral)| error)?;
        self.continuation = Some(input);
        Ok(Some(input))
    }
    /// Save this same input's positive consumption before actually retiring
    /// its header. Failure abandonment remains distinct from Core consumption.
    pub(super) fn finish_continuation(
        &mut self,
        input: InputId,
    ) -> Result<Consumption, ActorTurnError> {
        let original = self.binding()?;
        let standing_input = self
            .input_obligation
            .as_ref()
            .ok_or(ActorTurnError::UnknownStandingInput)?;
        if self.continuation != Some(input) || standing_input.in_flight()? != Some(input) {
            return Err(ActorTurnError::InputAuthorMismatch);
        }
        let receipt = match original
            .runtime
            .input_custody(original.registration.actor(), input)?
        {
            Some(custody) if custody.phase == InputPhase::Committed => Consumption::Consumed,
            Some(custody) if custody.phase == InputPhase::AbandonedByOwnerFailure => {
                Consumption::AbandonedByOwnerFailure
            }
            Some(_) => return Ok(Consumption::Pending),
            None => return Err(ActorTurnError::UnknownInput),
        };
        match original
            .runtime
            .retire_input(original.registration.actor(), input)
        {
            Ok(()) => {}
            Err(
                ActorTurnError::InputStillQueued
                | ActorTurnError::EffectsRemain
                | ActorTurnError::ReservationsRemain,
            ) => return Ok(Consumption::Pending),
            Err(error) => return Err(error),
        }
        self.continuation = None;
        Ok(receipt)
    }
    pub(super) fn authorize_removal(
        &mut self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<(), ActorTurnError> {
        let original = self.binding()?;
        if ticket.actor != original.root
            || !matches!(&ticket.effect, WorkerAction::Control {effect: ControlEffect::RemoveHost {participation}, ..} if *participation == original.participation)
        {
            return Err(ActorTurnError::PublicationTargetMismatch);
        }
        original.runtime.validate_effect_ticket(ticket)?;
        original
            .runtime
            .close_actor_admission(original.registration.actor())?;
        original
            .runtime
            .close_timer_source(original.registration.actor())?;
        self.source_ended = true;
        Ok(())
    }
    pub(super) fn discard_failed_hint(
        &mut self,
        stage: &ScopePublicationsDrained<'_>,
    ) -> Result<(), ActorTurnError> {
        let original = self.binding()?;
        stage.validate(&original.runtime, original.registration.actor())?;
        original
            .runtime
            .close_actor_admission(original.registration.actor())?;
        // Failure fences Core timer production. Keep the same TimerIndex
        // position available for the retained custodian's original horizon.
        // An already filled Tick still needs real abandonment and retirement.
        // The original Core was discarded. There is no computation to resume.
        // A filled input still needs its actual failed-input abandonment and
        // batch retirement; neither is acknowledged by clearing this bool.
        self.pending = false;
        self.failed = true;
        self.source_ended = true;
        Ok(())
    }
    /// The host holds this original Participation cell across source teardown. Every
    /// other producer must first close its actual role/permit. A Root decision
    /// or empty bool alone cannot establish that no later batch can emit Wake.
    /// A failed owner's supervisor batch is an actual other batch too: finish
    /// all borrowers of its publication stage, then complete/retire that same
    /// batch before this final close. Keep TIMER/FAILURE Work live throughout;
    /// waiting for this role to close before finishing that batch would cycle.
    pub(super) fn close(&mut self) -> Result<bool, ActorTurnError> {
        if self.closed {
            return Ok(true);
        }
        if !self.source_ended || self.pending || self.continuation.is_some() {
            return Ok(false);
        }
        let original = self.binding()?;
        if let Some(input) = self
            .input_obligation
            .as_ref()
            .map(StandingInputObligation::in_flight)
            .transpose()?
            .flatten()
        {
            match original
                .runtime
                .retire_input(original.registration.actor(), input)
            {
                Ok(()) => {}
                Err(
                    ActorTurnError::InputStillQueued
                    | ActorTurnError::EffectsRemain
                    | ActorTurnError::ReservationsRemain,
                ) => return Ok(false),
                Err(error) => return Err(error),
            }
        }
        let snapshot = original.runtime.snapshot(original.registration.actor())?;
        if if self.failed {
            original
                .runtime
                .pending_timer_input(original.registration.actor())?
                .is_some()
        } else {
            original
                .runtime
                .timer_source_state(original.registration.actor())?
                != TimerSourceState::ClosedDrained
        } {
            return Ok(false);
        }
        let own_roles = usize::from(self.input_obligation.is_some());
        let own_permits = usize::from(self.permit.is_some());
        // Unfilled original permits are included in inputs. This also waits
        // for already committed Wake effects outside our own in-flight slot.
        if snapshot.external_open
            || snapshot.standing_inputs != own_roles
            || snapshot.inputs != own_permits
            || snapshot.batches != 0
        {
            return Ok(false);
        }
        if let Some(permit) = self.permit.take() {
            permit.revoke_unused()?;
        }
        if let Some(standing_input) = self.input_obligation.as_mut() {
            standing_input.close_after_exit()?;
        }
        self.input_obligation = None;
        self.closed = true;
        Ok(true)
    }
    #[cfg(test)]
    pub(in crate::control_runtime) fn close_for_teardown(
        &mut self,
    ) -> Result<bool, ActorTurnError> {
        if self.binding.is_none() {
            return Ok(true);
        }
        let original = self.binding()?;
        original
            .runtime
            .close_actor_admission(original.registration.actor())?;
        original
            .runtime
            .close_timer_source(original.registration.actor())?;
        self.source_ended = true;
        self.close()
    }
}
impl ParticipationHostRecord {
    /// Transfer the actual scheduling hint and immediately finish its original
    /// effect, so a continuation can retire its own old batch before refilling.
    pub(crate) fn handoff_internal_wake(
        &mut self,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<(), EffectCompletionRefusal<WorkerAction>> {
        if let Err(error) = self.resources.internal.accept_wake(&ticket) {
            return Err(EffectCompletionRefusal { error, ticket });
        }
        self.complete_host_effect(ticket)
    }
    #[cfg(test)]
    pub(crate) fn advance_internal(
        &mut self,
    ) -> Result<ParticipationInternalAdvance, ActorTurnError> {
        self.resources.internal.advance()
    }
    pub(crate) fn retire_internal_source(&mut self) -> Result<bool, ActorTurnError> {
        // Empty runtime positions alone cannot revoke a retained Task/R/route
        // owner's authority to issue its next already-admitted callback.
        if !self.tasks.is_empty()
            || !self.resources.requests.is_empty()
            || !self.resources.request_partials.is_empty()
            || !self.resources.routes.is_empty()
        {
            return Ok(false);
        }
        self.resources.internal.close()
    }
}
