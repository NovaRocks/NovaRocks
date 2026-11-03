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

//! One original internal input serializes coalesced control hints and original
//! failed-Participation lifecycle facts. No Participation prepays Work for this source.

mod lifecycle;
pub(crate) use lifecycle::{
    ControlLifecycleConsumption, ControlLifecycleFact, ControlLifecycleReceipt,
};
use lifecycle::{InFlightLifecycle, PendingLifecycle};

use super::{
    actors::{ControlMessage, WorkerAction, WorkerActor, WorkerMessage},
    participation_ready::ParticipationReadyNotice,
    process_control::ProcessControlOwner,
};
use crate::control_runtime_policy::StorageNeed;
use novarocks_control_runtime::{
    actor_turn::{
        ActorFailureProof, ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket,
        InputPhase, ReservedInputObligation, StandingInputObligation, TimerSourceState,
    },
    obligation::{ReservationObligation, WorkObligation},
    transaction::{RuntimeError, RuntimeLedger},
};
use novarocks_control_runtime_core::{
    identity::{ActorId, InputId},
    mailbox::InputClass,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_worker_protocol::control::{ControlCore, ControlEffect, ControlInput};
use novarocks_worker_protocol::retention::ParticipationRetirementIdentity;
use std::collections::VecDeque;

pub(crate) const CONTROL_INTERNAL_STORAGE: StorageNeed = StorageNeed {
    requests: 0,
    inputs: 1,
    batches: 1,
    effects: ControlCore::MAX_STEP_WORK,
    captures: 1,
    work: 1,
    cuts: 0,
    replies: 0,
    completions: 0,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ControlInternalError {
    Runtime(RuntimeError),
    ActorTurn(ActorTurnError),
    WrongEffect,
    WrongOwner,
    NotEnabled,
    AlreadyAdmitted,
    #[cfg(test)]
    TimerStillOpen,
    #[cfg(test)]
    Pending,
    Closed,
}
impl std::fmt::Display for ControlInternalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Runtime(error) => write!(f, "Control internal role operation failed: {error}"),
            Self::ActorTurn(error) => write!(
                f,
                "Control internal input or effect operation failed: {error}"
            ),
            Self::WrongEffect => f.write_str("Control internal source received the wrong effect"),
            Self::WrongOwner => {
                f.write_str("Control internal source does not belong to the original owner")
            }
            Self::NotEnabled => f.write_str("Control internal source is not enabled"),
            Self::AlreadyAdmitted => f.write_str("Control internal source was already admitted"),
            #[cfg(test)]
            Self::TimerStillOpen => f.write_str("Control internal timer source has not closed"),
            #[cfg(test)]
            Self::Pending => f.write_str("Control internal source still owns a pending input"),
            Self::Closed => f.write_str("Control internal source is closed"),
        }
    }
}
impl std::error::Error for ControlInternalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::ActorTurn(error) => Some(error),
            Self::WrongEffect
            | Self::WrongOwner
            | Self::NotEnabled
            | Self::AlreadyAdmitted
            | Self::Closed => None,
            #[cfg(test)]
            Self::TimerStillOpen | Self::Pending => None,
        }
    }
}

/// Only these same never-filled originals can take unused teardown. A bound
/// Work is not rolled back or recreated when the input position is refused.
#[must_use = "Retain the original partially admitted internal source"]
pub(crate) struct ControlInternalPartial {
    pub work: WorkObligation,
    pub capacity: ReservationObligation,
    pub permit: Option<ReservedInputObligation<WorkerActor>>,
    pub input_obligation: Option<StandingInputObligation<WorkerActor>>,
}
pub(crate) struct ControlInternalRefusal {
    pub error: ControlInternalError,
    pub partial: Option<ControlInternalPartial>,
}
impl std::fmt::Debug for ControlInternalRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlInternalRefusal")
            .field("error", &self.error)
            .field("retains_original_work", &self.partial.is_some())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InternalAdvance {
    Idle,
    Waiting(InputId),
    Submitted(InputId),
}

#[must_use = "Drain the internal source before sealing its actor"]
pub(crate) struct ControlInternal {
    ledger: RuntimeLedger,
    runtime: ActorRuntime<WorkerActor>,
    registration: ActorRegistration,
    actor: ActorId,
    work: WorkObligation,
    capacity: ReservationObligation,
    input_obligation: StandingInputObligation<WorkerActor>,
    pending: bool,
    process_drain_pending: bool,
    ingress_closed_pending: bool,
    lifecycle_pending: VecDeque<PendingLifecycle>,
    lifecycle_in_flight: Option<InFlightLifecycle>,
    lifecycle_turn: bool,
    body_exited: bool,
    captures_exited: bool,
    input_closed: bool,
}
impl ControlInternal {
    pub(crate) fn admit(root: &ProcessControlOwner) -> Result<Self, ControlInternalRefusal> {
        let scope = &root.resources().scope;
        let generation = scope
            .gates()
            .map_err(|error| ControlInternalRefusal {
                error: ControlInternalError::Runtime(error),
                partial: None,
            })?
            .generation;
        let need = CONTROL_INTERNAL_STORAGE
            .resources()
            .expect("fixed internal source vector");
        let guard = scope
            .reserve_next(
                generation,
                need,
                WorkKind::Step,
                WorkAdmission::Derived(root.lifetime_work()),
            )
            .map_err(|error| ControlInternalRefusal {
                error: ControlInternalError::Runtime(error),
                partial: None,
            })?;
        let (work, capacity) = match guard.bind(WorkKind::Step) {
            Ok(bound) => bound,
            Err(_) => std::process::abort(),
        };
        let mut partial = ControlInternalPartial {
            work,
            capacity,
            permit: None,
            input_obligation: None,
        };
        let permit =
            match root
                .runtime()
                .reserve_input(root.actor(), root.actor(), InputClass::Control)
            {
                Ok(permit) => permit,
                Err(error) => {
                    return Err(ControlInternalRefusal {
                        error: ControlInternalError::ActorTurn(error),
                        partial: Some(partial),
                    });
                }
            };
        match permit.into_standing_input() {
            Ok(role) => partial.input_obligation = Some(role),
            Err((error, permit)) => {
                partial.permit = Some(permit);
                return Err(ControlInternalRefusal {
                    error: ControlInternalError::ActorTurn(error),
                    partial: Some(partial),
                });
            }
        }
        if let Err(error) = partial.work.start() {
            return Err(ControlInternalRefusal {
                error: ControlInternalError::Runtime(error),
                partial: Some(partial),
            });
        }
        Ok(Self {
            ledger: root.ledger().clone(),
            runtime: root.runtime().clone(),
            registration: root.registration().clone(),
            actor: root.actor(),
            work: partial.work,
            capacity: partial.capacity,
            input_obligation: partial
                .input_obligation
                .take()
                .expect("actual input role converted"),
            pending: false,
            process_drain_pending: false,
            ingress_closed_pending: false,
            lifecycle_pending: VecDeque::new(),
            lifecycle_in_flight: None,
            lifecycle_turn: true,
            body_exited: false,
            captures_exited: false,
            input_closed: false,
        })
    }

    /// Called only after this same Root's external Work gate has closed.
    /// The original paid role serializes this with its in-flight continuation.
    pub(crate) fn request_process_drain(&mut self) -> Result<(), ControlInternalError> {
        if self.input_closed || self.body_exited {
            return Err(ControlInternalError::Closed);
        }
        self.process_drain_pending = true;
        self.runtime
            .notify_host_actor(&self.registration)
            .map_err(ControlInternalError::ActorTurn)
    }
    /// An explicit caller acknowledgement of actual listener/body exit, not a
    /// flag inferred from a timer or a request to begin shutting down.
    pub(crate) fn confirm_ingress_closed(&mut self) -> Result<(), ControlInternalError> {
        if self.input_closed || self.body_exited {
            return Err(ControlInternalError::Closed);
        }
        self.ingress_closed_pending = true;
        self.runtime
            .notify_host_actor(&self.registration)
            .map_err(ControlInternalError::ActorTurn)
    }

    /// Transfer one original failed owner's neutral fact to this process's
    /// existing input source. Failure proof and actual bindings qualify the
    /// sender; the receipt does not sign execution exit or another deadline.
    pub(crate) fn enqueue_failed_lifecycle(
        &mut self,
        proof: &ActorFailureProof,
        fact: ControlLifecycleFact,
        notice: ParticipationReadyNotice,
    ) -> Result<ControlLifecycleReceipt, ControlInternalError> {
        if self.input_closed || self.body_exited {
            return Err(ControlInternalError::Closed);
        }
        self.runtime
            .validate_registration(&self.registration)
            .map_err(ControlInternalError::ActorTurn)?;
        let retirement = fact.retirement().ok_or(ControlInternalError::WrongOwner)?;
        let actual = self
            .runtime
            .failure_proof_for(proof.registration())
            .map_err(ControlInternalError::ActorTurn)?;
        if actual.initial_epoch() != proof.initial_epoch()
            || actual.failed_epoch() != proof.failed_epoch()
            || !actual
                .registration()
                .same_registration(proof.registration())
            || proof.registration().actor() == self.actor
            || !proof.registration().retained_in_process(self.work.scope())
            || !notice.matches_registration(&self.runtime, proof.registration())
            || notice.participation() != fact.participation()
            || !retirement.is_valid()
        {
            return Err(ControlInternalError::WrongOwner);
        }
        if let ParticipationRetirementIdentity::OwnerFailure {
            actor,
            initial_epoch,
            failed_epoch,
        } = retirement
            && (actor != proof.registration().actor()
                || initial_epoch != proof.initial_epoch()
                || failed_epoch != proof.failed_epoch())
        {
            return Err(ControlInternalError::WrongOwner);
        }
        // Root's owner lock serializes this insertion with its real host
        // turn. Mark before the infallible move so a refused wake retains no
        // hidden accepted fact and a released caller cannot lose the notice.
        self.runtime
            .notify_host_actor(&self.registration)
            .map_err(ControlInternalError::ActorTurn)?;
        let (original, receipt) = PendingLifecycle::new(fact, notice, &self.registration);
        self.lifecycle_pending.push_back(original);
        Ok(receipt)
    }

    /// Called only by the original removal owner after its forget child and
    /// directory/actor removal. The subject keeps that owner alive through the
    /// receipt, while this existing Source alone pays the actual Root input.
    pub(crate) fn enqueue_host_removed(
        &mut self,
        original: &WorkObligation,
        participation: novarocks_execution_contract::ParticipationRef,
    ) -> Result<ControlLifecycleReceipt, ControlInternalError> {
        if self.input_closed || self.body_exited {
            return Err(ControlInternalError::Closed);
        }
        self.runtime
            .validate_registration(&self.registration)
            .map_err(ControlInternalError::ActorTurn)?;
        if !self.registration.matches_scope(original.scope()) {
            return Err(ControlInternalError::WrongOwner);
        }
        self.runtime
            .validate_work_author(&self.registration, original)
            .map_err(ControlInternalError::ActorTurn)?;
        let fact = ControlLifecycleFact::HostRemoved {
            participation,
            subject: original.reference(),
        };
        // Notify before transferring the single neutral fact. A refused wake
        // leaves the original caller's owner and its unregistered fact intact.
        self.runtime
            .notify_host_actor(&self.registration)
            .map_err(ControlInternalError::ActorTurn)?;
        let (pending, receipt) =
            PendingLifecycle::for_root(fact, &self.runtime, &self.registration);
        self.lifecycle_pending.push_back(pending);
        Ok(receipt)
    }

    /// Acceptance moves only a known scheduling hint into its paid bool cell.
    /// The caller then completes that actual scheduling effect. Holding it
    /// until a later fill would pin the very input role needed by that fill.
    pub(crate) fn accept_wake(
        &mut self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<(), ControlInternalError> {
        self.runtime
            .validate_effect_ticket(ticket)
            .map_err(ControlInternalError::ActorTurn)?;
        if self.input_closed || self.body_exited {
            return Err(ControlInternalError::Closed);
        }
        if ticket.actor != self.actor
            || !matches!(
                &ticket.effect,
                WorkerAction::Control {
                    effect: ControlEffect::WakeInternal,
                    admitted: None,
                }
            )
        {
            return Err(ControlInternalError::WrongEffect);
        }
        self.pending = true;
        self.runtime
            .notify_host_actor(&self.registration)
            .map_err(ControlInternalError::ActorTurn)?;
        Ok(())
    }

    /// Called from the root's actual progress wake, never by a request scan.
    /// Busy retirement keeps the old input and the pending bool unchanged.
    pub(crate) fn advance(&mut self) -> Result<InternalAdvance, ControlInternalError> {
        if self.input_closed || self.body_exited {
            return Err(ControlInternalError::Closed);
        }
        if let Some(input) = self
            .input_obligation
            .in_flight()
            .map_err(ControlInternalError::ActorTurn)?
        {
            if let Some(original) = &self.lifecycle_in_flight {
                if original.input != input {
                    return Err(ControlInternalError::WrongOwner);
                }
                let custody = self
                    .runtime
                    .input_custody(self.actor, input)
                    .map_err(ControlInternalError::ActorTurn)?
                    .ok_or(ControlInternalError::WrongOwner)?;
                if custody.standing_input_id != Some(self.input_obligation.id())
                    || custody.source != self.actor
                    || custody.class != InputClass::Control
                    || custody.runtime_metadata
                {
                    return Err(ControlInternalError::WrongOwner);
                }
                match custody.phase {
                    InputPhase::Committed => original.original.consumed(input)?,
                    InputPhase::AbandonedByOwnerFailure => {
                        return Err(ControlInternalError::ActorTurn(ActorTurnError::ActorFailed));
                    }
                    _ => {}
                }
            }
            match self.runtime.retire_input(self.actor, input) {
                Ok(()) => {
                    if let Some(original) = &self.lifecycle_in_flight {
                        original.original.retired(input)?;
                        self.lifecycle_in_flight = None;
                    }
                }
                Err(
                    ActorTurnError::InputStillQueued
                    | ActorTurnError::EffectsRemain
                    | ActorTurnError::ReservationsRemain
                    | ActorTurnError::MetadataStillPending,
                ) => return Ok(InternalAdvance::Waiting(input)),
                Err(error) => return Err(ControlInternalError::ActorTurn(error)),
            }
        } else if self.lifecycle_in_flight.is_some() {
            return Err(ControlInternalError::WrongOwner);
        }
        let lifecycle = !self.lifecycle_pending.is_empty()
            && (!self.pending || self.lifecycle_turn)
            && !self.process_drain_pending
            && !self.ingress_closed_pending;
        let mut selected = if lifecycle {
            self.lifecycle_pending.pop_front()
        } else {
            None
        };
        let command = if self.process_drain_pending {
            ControlInput::ProcessDrain
        } else if self.ingress_closed_pending {
            ControlInput::ProcessIngressClosed
        } else if let Some(original) = &selected {
            original.fact.into_command()
        } else if self.pending {
            ControlInput::NextInternal
        } else {
            return Ok(InternalAdvance::Idle);
        };
        let process_drain = matches!(&command, ControlInput::ProcessDrain);
        let ingress_closed = matches!(&command, ControlInput::ProcessIngressClosed);
        let message = WorkerMessage::Control(ControlMessage::Command {
            work: self.work.reference(),
            request: None,
            command,
        });
        let input = match self.input_obligation.fill(message) {
            Ok(input) => input,
            Err((error, _same_neutral_command)) => {
                if let Some(original) = selected.take() {
                    self.lifecycle_pending.push_front(original);
                }
                return Err(ControlInternalError::ActorTurn(error));
            }
        };
        if let Some(original) = selected {
            self.lifecycle_in_flight = Some(InFlightLifecycle { input, original });
            self.lifecycle_in_flight
                .as_ref()
                .expect("same original lifecycle source")
                .original
                .submitted(input)?;
            self.lifecycle_turn = false;
        } else if process_drain {
            self.process_drain_pending = false;
        } else if ingress_closed {
            self.ingress_closed_pending = false;
        } else {
            self.pending = false;
            self.lifecycle_turn = true;
        }
        Ok(InternalAdvance::Submitted(input))
    }

    /// Component callers retain the original owner when a bounded closing
    /// step still has ready metadata to reap.
    #[cfg(test)]
    pub(crate) fn close_after_drain(&mut self) -> Result<(), ControlInternalError> {
        if self
            .runtime
            .timer_source_state(self.actor)
            .map_err(ControlInternalError::ActorTurn)?
            != TimerSourceState::ClosedDrained
        {
            return Err(ControlInternalError::TimerStillOpen);
        }
        if self.pending
            || self.process_drain_pending
            || self.ingress_closed_pending
            || !self.lifecycle_pending.is_empty()
            || self.lifecycle_in_flight.is_some()
        {
            return Err(ControlInternalError::Pending);
        }
        if self.advance_close_after_drain()? {
            Ok(())
        } else {
            Err(ControlInternalError::Pending)
        }
    }

    /// Only the original stopped source can close its unused metadata body.
    /// Pending hints and real in-flight batches keep their original role. The
    /// paid Work units remain held until that exact header is actually reaped.
    pub(crate) fn advance_close_after_drain(&mut self) -> Result<bool, ControlInternalError> {
        if self
            .runtime
            .timer_source_state(self.actor)
            .map_err(ControlInternalError::ActorTurn)?
            != TimerSourceState::ClosedDrained
            || self.pending
            || self.process_drain_pending
            || self.ingress_closed_pending
            || !self.lifecycle_pending.is_empty()
            || self.lifecycle_in_flight.is_some()
        {
            return Ok(false);
        }
        if !self.input_closed {
            if let Some(input) = self
                .input_obligation
                .in_flight()
                .map_err(ControlInternalError::ActorTurn)?
            {
                match self.runtime.retire_input(self.actor, input) {
                    Ok(()) => {}
                    Err(
                        ActorTurnError::InputStillQueued
                        | ActorTurnError::EffectsRemain
                        | ActorTurnError::ReservationsRemain,
                    ) => return Ok(false),
                    Err(error) => return Err(ControlInternalError::ActorTurn(error)),
                }
            }
            if !self.body_exited {
                self.work
                    .body_exited()
                    .map_err(ControlInternalError::Runtime)?;
                self.body_exited = true;
            }
            if !self.captures_exited {
                self.work
                    .captures_exited()
                    .map_err(ControlInternalError::Runtime)?;
                self.captures_exited = true;
            }
            self.input_obligation
                .close_after_exit()
                .map_err(ControlInternalError::ActorTurn)?;
            self.input_closed = true;
        }
        if !self.work.retirement_observer().is_retired() {
            self.ledger
                .reap_scope_ready(self.work.scope().id(), 1)
                .map_err(ControlInternalError::Runtime)?;
            if !self.work.retirement_observer().is_retired() {
                // A finite ready-header tail, never an I/O poll or a false exit.
                self.runtime
                    .notify_host_actor(&self.registration)
                    .map_err(ControlInternalError::ActorTurn)?;
                return Ok(false);
            }
        }
        if !self.capacity.is_released() {
            self.capacity
                .finish()
                .map_err(ControlInternalError::Runtime)?;
        }
        Ok(true)
    }
}
