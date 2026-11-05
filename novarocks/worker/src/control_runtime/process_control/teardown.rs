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

//! The original process Root's final drain. External intake closes before
//! waiting; live requests and Context execution retain their actual custody.

use super::{ControlInternalError, KINDS, ProcessControlOwner, RuntimeError, WorkerActor};
use novarocks_control_runtime::actor_turn::{ActorHostTurn, ActorTurnError, TimerSourceState};

#[derive(Default)]
pub(super) struct ProcessControlTeardown {
    draining: bool,
    ingress_closed: bool,
    pub(super) internal_closed: bool,
    local_exit_observed: [bool; KINDS.len()],
    scope_removed: bool,
}
#[derive(Debug)]
pub(crate) enum RootDrainError {
    NotDraining,
    InventoryPending,
    RootFailed,
    WrongOwner,
    Runtime(RuntimeError),
    ActorTurn(ActorTurnError),
    Internal(ControlInternalError),
}
impl std::fmt::Display for RootDrainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotDraining => f.write_str("Process control has not begun draining"),
            Self::InventoryPending => {
                f.write_str("Process control still owns pending drain inventory")
            }
            Self::RootFailed => f.write_str("Process control Root actor failed"),
            Self::WrongOwner => {
                f.write_str("Process control drain does not belong to the original owner")
            }
            Self::Runtime(error) => {
                write!(f, "Process control drain runtime operation failed: {error}")
            }
            Self::ActorTurn(error) => {
                write!(f, "Process control drain actor operation failed: {error}")
            }
            Self::Internal(error) => {
                write!(f, "Process control internal source drain failed: {error}")
            }
        }
    }
}
impl std::error::Error for RootDrainError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::ActorTurn(error) => Some(error),
            Self::Internal(error) => Some(error),
            Self::NotDraining | Self::InventoryPending | Self::RootFailed | Self::WrongOwner => {
                None
            }
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct RootDrainProgress {
    pub ready: bool,
    /// Only finite ready-header work. A delivery, Source or future
    /// lease keeps the owner parked until its actual timer/exit notification.
    pub runnable: bool,
}
#[must_use = "Retain the original Root turn or the actual removed actor"]
pub(crate) enum RootDrainRefusal {
    BeforeActor {
        error: RootDrainError,
        turn: ActorHostTurn<WorkerActor>,
    },
    AfterActor {
        error: RootDrainError,
        _actor: WorkerActor,
    },
}
impl std::fmt::Debug for RootDrainRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BeforeActor { error, turn } => f
                .debug_struct("BeforeActor")
                .field("error", error)
                .field("turn", turn)
                .finish(),
            Self::AfterActor { error, .. } => f
                .debug_struct("AfterActor")
                .field("error", error)
                .finish_non_exhaustive(),
        }
    }
}

impl ProcessControlOwner {
    /// Close the actual external Work gate and mailbox intake. Already
    /// admitted RPCs and cleanup producers retain all their original rights.
    /// The timer continues while existing requests and live Context work may
    /// still need it. Pure ticket history does not delay process shutdown.
    pub(crate) fn begin_drain(&mut self) -> Result<(), RootDrainError> {
        if self.resources.teardown.scope_removed || self.resources.teardown.draining {
            return Ok(());
        }
        self.resources
            .scope
            .close_external()
            .map_err(RootDrainError::Runtime)?;
        self.runtime()
            .close_actor_admission(self.actor)
            .map_err(RootDrainError::ActorTurn)?;
        self.resources
            .internal
            .as_mut()
            .ok_or(RootDrainError::Internal(ControlInternalError::NotEnabled))?
            .request_process_drain()
            .map_err(RootDrainError::Internal)?;
        self.resources.teardown.draining = true;
        self.runtime()
            .notify_host_actor(&self.registration)
            .map_err(RootDrainError::ActorTurn)?;
        Ok(())
    }

    /// Application composition invokes this only after every Native listener
    /// actually joined and disposed its bodies. Closing intake alone is not
    /// enough to end process-local semantic retention.
    pub(crate) fn confirm_ingress_closed(&mut self) -> Result<(), RootDrainError> {
        if !self.resources.teardown.draining {
            return Err(RootDrainError::NotDraining);
        }
        if self.resources.teardown.ingress_closed {
            return Ok(());
        }
        self.resources
            .internal
            .as_mut()
            .ok_or(RootDrainError::Internal(ControlInternalError::NotEnabled))?
            .confirm_ingress_closed()
            .map_err(RootDrainError::Internal)?;
        self.resources.teardown.ingress_closed = true;
        self.runtime()
            .notify_host_actor(&self.registration)
            .map_err(RootDrainError::ActorTurn)
    }

    /// The Node must first empty its original removed-Context parent rows.
    /// This method separately verifies Root's own actual owners, and never
    /// manufactures any Context/Source exit. Once ingress has actually closed,
    /// process-local ticket history imposes no further shutdown obligation.
    pub(crate) fn advance_drain(&mut self) -> Result<RootDrainProgress, RootDrainError> {
        let mut progress = RootDrainProgress::default();
        if self.resources.teardown.scope_removed {
            progress.ready = true;
            return Ok(progress);
        }
        if !self.resources.teardown.draining || !self.resources.teardown.ingress_closed {
            return Ok(progress);
        }
        let snapshot = self
            .runtime()
            .snapshot(self.actor)
            .map_err(RootDrainError::ActorTurn)?;
        if snapshot.failed {
            return Err(RootDrainError::RootFailed);
        }
        if self
            .resources
            .rejected_ingress
            .as_ref()
            .is_some_and(|original| !original.is_empty())
            || !self.resources.request_partials.is_empty()
            || !self.resources.requests.is_empty()
            || !self.resources.request_header_routes.is_empty()
            || self.resources.request_ready.registered() != 0
            || !self.service.directory().is_empty()
            || !self.resources.route_waiters.is_empty()
            || !self.resources.ready_routes.is_empty()
            || !self.resources.return_routes.is_empty()
            || self.resources.internal_partial.is_some()
            || self
                .resources
                .task_capacity
                .as_ref()
                .is_some_and(|signal| !signal.is_empty())
        {
            return Ok(progress);
        }
        // No live Context callback or source owner can require a
        // deadline now. An already filled Tick still has its real batch.
        self.runtime()
            .close_timer_source(self.actor)
            .map_err(RootDrainError::ActorTurn)?;
        if let Some(input) = self
            .runtime()
            .pending_timer_input(self.actor)
            .map_err(RootDrainError::ActorTurn)?
        {
            match self.runtime().retire_input(self.actor, input) {
                Ok(()) => {}
                Err(
                    ActorTurnError::InputStillQueued
                    | ActorTurnError::EffectsRemain
                    | ActorTurnError::ReservationsRemain,
                ) => return Ok(progress),
                Err(error) => return Err(RootDrainError::ActorTurn(error)),
            }
        }
        if self
            .runtime()
            .timer_source_state(self.actor)
            .map_err(RootDrainError::ActorTurn)?
            != TimerSourceState::ClosedDrained
        {
            return Ok(progress);
        }
        if !self.resources.teardown.internal_closed {
            if let Some(internal) = self.resources.internal.as_mut()
                && !internal
                    .advance_close_after_drain()
                    .map_err(RootDrainError::Internal)?
            {
                return Ok(progress);
            }
            self.resources.teardown.internal_closed = true;
        }
        let snapshot = self
            .runtime()
            .snapshot(self.actor)
            .map_err(RootDrainError::ActorTurn)?;
        if snapshot.external_open
            || snapshot.inputs != 0
            || snapshot.standing_inputs != 0
            || snapshot.batches != 0
            || !self.resources.dispatch.is_empty()
        {
            return Ok(progress);
        }
        // These three permanent metadata authors have no opaque captures.
        // Every producer which could borrow them has actually closed above.
        let ledger = self.ledger().clone();
        let scope = self.resources.scope.clone();
        for index in (0..self.resources.metadata_resources.len()).rev() {
            let metadata_resource = &mut self.resources.metadata_resources[index];
            if metadata_resource.capacity.is_released() {
                continue;
            }
            if !self.resources.teardown.local_exit_observed[index] {
                metadata_resource
                    .work
                    .body_exited()
                    .map_err(RootDrainError::Runtime)?;
                metadata_resource
                    .work
                    .captures_exited()
                    .map_err(RootDrainError::Runtime)?;
                self.resources.teardown.local_exit_observed[index] = true;
            }
            ledger
                .reap_scope_ready(scope.id(), 1)
                .map_err(RootDrainError::Runtime)?;
            if !metadata_resource.work.retirement_observer().is_retired() {
                progress.runnable = true;
                return Ok(progress);
            }
            metadata_resource
                .capacity
                .finish()
                .map_err(RootDrainError::Runtime)?;
        }
        // Child-scope links can be the last genuine ready headers after their
        // actual parent owners moved out. Do not confuse work>0 with readiness.
        let reaped = ledger
            .reap_scope_ready(scope.id(), 1)
            .map_err(RootDrainError::Runtime)?;
        progress.ready = scope.join_ready().map_err(RootDrainError::Runtime)?;
        progress.runnable = !progress.ready && reaped != 0;
        Ok(progress)
    }

    /// Retire only through the original claimed Common Root turn. Return the
    /// pure actor for Drop outside Root's mutex, preserving it on every refusal.
    pub(crate) fn finish_drain(
        &mut self,
        turn: ActorHostTurn<WorkerActor>,
    ) -> Result<WorkerActor, RootDrainRefusal> {
        if !turn.matches_registration(&self.registration) {
            return Err(RootDrainRefusal::BeforeActor {
                error: RootDrainError::WrongOwner,
                turn,
            });
        }
        if !self.resources.teardown.draining {
            return Err(RootDrainRefusal::BeforeActor {
                error: RootDrainError::NotDraining,
                turn,
            });
        }
        match self.advance_drain() {
            Ok(progress) if progress.ready => {}
            Ok(_) => {
                return Err(RootDrainRefusal::BeforeActor {
                    error: RootDrainError::InventoryPending,
                    turn,
                });
            }
            Err(error) => return Err(RootDrainRefusal::BeforeActor { error, turn }),
        }
        let actor = turn
            .retire_actor()
            .map_err(|(error, turn)| RootDrainRefusal::BeforeActor {
                error: RootDrainError::ActorTurn(error),
                turn,
            })?;
        self.resources.registered = false;
        if let Err(error) = self.ledger().remove_scope(self.resources.scope.id()) {
            return Err(RootDrainRefusal::AfterActor {
                error: RootDrainError::Runtime(error),
                _actor: actor,
            });
        }
        self.resources.teardown.scope_removed = true;
        Ok(actor)
    }
}
