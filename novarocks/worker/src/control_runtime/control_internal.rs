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

//! One prepaid, coalesced internal control wake. The Core chooses its next
//! bounded continuation from its own state; the host owns no operation queue.

use super::{
    actors::{ControlMessage, WorkerAction, WorkerActor, WorkerMessage},
    process_control::ProcessControlOwner,
};
use crate::control_runtime_policy::StorageNeed;
use novarocks_control_runtime::{
    driver::{ActorRuntime, DriverError, EffectTicket, InputPermit, InputRole, TimerSourceState},
    lease::{CapacityBundle, WorkLease},
    transaction::RuntimeError,
};
use novarocks_control_runtime_core::{
    identity::{ActorId, InputId},
    mailbox::InputClass,
    scope::{WorkAdmission, WorkKind},
};
use novarocks_worker_protocol::control::{ControlCore, ControlEffect, ControlInput};

pub(crate) const CONTROL_INTERNAL_STORAGE: StorageNeed = StorageNeed {
    inputs: 1,
    batches: 1,
    effects: ControlCore::MAX_STEP_WORK,
    captures: 1,
    work: 1,
    cuts: 0,
    replies: 0,
    completions: 0,
};

#[derive(Debug)]
pub(crate) enum ControlInternalError {
    Runtime(RuntimeError),
    Driver(DriverError),
    WrongEffect,
    NotEnabled,
    AlreadyAdmitted,
    TimerStillOpen,
    Pending,
    Closed,
}

/// Only these same never-filled originals can take unused teardown. A bound
/// Work is not rolled back or recreated when the input position is refused.
#[must_use = "Retain the original partially admitted internal source"]
pub(crate) struct ControlInternalPartial {
    pub work: WorkLease,
    pub capacity: CapacityBundle,
    pub permit: Option<InputPermit<WorkerActor>>,
    pub role: Option<InputRole<WorkerActor>>,
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
    runtime: ActorRuntime<WorkerActor>,
    actor: ActorId,
    work: WorkLease,
    capacity: CapacityBundle,
    role: InputRole<WorkerActor>,
    pending: bool,
    body_exited: bool,
    captures_exited: bool,
    role_closed: bool,
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
            role: None,
        };
        let permit =
            match root
                .runtime()
                .reserve_input(root.actor(), root.actor(), InputClass::Control)
            {
                Ok(permit) => permit,
                Err(error) => {
                    return Err(ControlInternalRefusal {
                        error: ControlInternalError::Driver(error),
                        partial: Some(partial),
                    });
                }
            };
        match permit.into_role() {
            Ok(role) => partial.role = Some(role),
            Err((error, permit)) => {
                partial.permit = Some(permit);
                return Err(ControlInternalRefusal {
                    error: ControlInternalError::Driver(error),
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
            runtime: root.runtime().clone(),
            actor: root.actor(),
            work: partial.work,
            capacity: partial.capacity,
            role: partial.role.take().expect("actual input role converted"),
            pending: false,
            body_exited: false,
            captures_exited: false,
            role_closed: false,
        })
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
            .map_err(ControlInternalError::Driver)?;
        if self.role_closed || self.body_exited {
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
        self.runtime.notify_pollers();
        Ok(())
    }

    /// Called from the root's actual progress wake, never by a request scan.
    /// Busy retirement keeps the old input and the pending bool unchanged.
    pub(crate) fn advance(&mut self) -> Result<InternalAdvance, ControlInternalError> {
        if self.role_closed || self.body_exited {
            return Err(ControlInternalError::Closed);
        }
        if let Some(input) = self
            .role
            .in_flight()
            .map_err(ControlInternalError::Driver)?
        {
            match self.runtime.retire_input(self.actor, input) {
                Ok(()) => {}
                Err(
                    DriverError::InputStillQueued
                    | DriverError::EffectsRemain
                    | DriverError::ReservationsRemain,
                ) => return Ok(InternalAdvance::Waiting(input)),
                Err(error) => return Err(ControlInternalError::Driver(error)),
            }
        }
        if !self.pending {
            return Ok(InternalAdvance::Idle);
        }
        let message = WorkerMessage::Control(ControlMessage::Command {
            work: self.work.id(),
            request: None,
            command: ControlInput::NextInternal,
        });
        let input = match self.role.fill(message) {
            Ok(input) => input,
            Err((error, _same_neutral_hint)) => return Err(ControlInternalError::Driver(error)),
        };
        self.pending = false;
        Ok(InternalAdvance::Submitted(input))
    }

    /// The caller has stopped source production. Pending work and every
    /// in-flight batch must really drain; close never substitutes for them.
    pub(crate) fn close_after_drain(&mut self) -> Result<(), ControlInternalError> {
        if self
            .runtime
            .timer_source_state(self.actor)
            .map_err(ControlInternalError::Driver)?
            != TimerSourceState::ClosedDrained
        {
            return Err(ControlInternalError::TimerStillOpen);
        }
        if self.pending {
            return Err(ControlInternalError::Pending);
        }
        if !self.role_closed {
            if let Some(input) = self
                .role
                .in_flight()
                .map_err(ControlInternalError::Driver)?
            {
                self.runtime
                    .retire_input(self.actor, input)
                    .map_err(ControlInternalError::Driver)?;
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
            self.role
                .close_after_exit()
                .map_err(ControlInternalError::Driver)?;
            self.role_closed = true;
        }
        if !self.capacity.is_released() {
            let resources = self
                .capacity
                .resources()
                .map_err(ControlInternalError::Runtime)?;
            self.capacity
                .release(&resources)
                .map_err(ControlInternalError::Runtime)?;
        }
        Ok(())
    }
}
