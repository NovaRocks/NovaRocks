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

//! Final finite metadata teardown after the original Context H has really
//! disposed every opaque capture. The exclusive Common host turn performs
//! actor removal separately; a quiet directory or logical state is no proof.

use super::{ContextHostError, ContextHostRecord, LIFETIME};
use crate::control_runtime::{actors::WorkerActor, context_host_jobs::ContextHostJobError};
use novarocks_control_runtime::driver::{ActorHostTurn, TimerSourceState};

/// Local observation progress only. These bits follow successful observations
/// on the original WorkLease; they grant no work, exit or capacity by themselves.
#[derive(Default)]
pub(super) struct ContextTeardownState {
    local_exit_observed: [bool; 3],
    pub(super) parent_retire_authorized: bool,
    parent_exit_observed: bool,
}

/// Removal failure never reconstructs an already-consumed host claim. After
/// actor removal, the same pure actor travels back with the actual error.
#[must_use = "Retain the original host turn or the removed pure actor"]
pub(crate) enum ContextTeardownRefusal {
    BeforeActor {
        error: ContextHostError,
        turn: ActorHostTurn<WorkerActor>,
    },
    AfterActor {
        error: ContextHostError,
        actor: WorkerActor,
    },
}
impl std::fmt::Debug for ContextTeardownRefusal {
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

impl ContextHostRecord {
    /// Close the last empty metadata owners after actual M/R/H retirement.
    /// No callbacks, destructors, Task/source exits or failure publications are
    /// inferred here. Task disposal must also have really finished; a running
    /// failure publication owner keeps its own supervisor inventory live.
    pub(crate) fn advance_metadata_teardown(&mut self) -> Result<bool, ContextHostError> {
        if !self.tasks.is_empty()
            || !self.task_driver.is_idle()
            || !self.resources.requests.is_empty()
            || !self.resources.request_partials.is_empty()
            || !self.resources.routes.is_empty()
            || self.resources.failure.is_some()
        {
            return Ok(false);
        }
        let Some(scope) = self.resources.scope.as_ref().cloned() else {
            return Ok(!self.resources.registered);
        };
        let Some(jobs) = self.resources.host_jobs.as_ref().cloned() else {
            return Ok(false);
        };
        if !jobs.retire_callbacks().map_err(ContextHostError::HostJob)?
            || !jobs.retire_holder().map_err(ContextHostError::HostJob)?
            || !jobs
                .holder_disposed(&self.control_runtime, self.actor, self.identity, &scope)
                .map_err(ContextHostError::HostJob)?
        {
            return Ok(false);
        }
        if !self.retire_owner_control()?
            || !self
                .retire_internal_source()
                .map_err(ContextHostError::Driver)?
        {
            return Ok(false);
        }
        if self
            .control_runtime
            .timer_source_state(self.actor)
            .map_err(ContextHostError::Driver)?
            != TimerSourceState::ClosedDrained
            || !self.dispatch.is_empty()
        {
            return Ok(false);
        }
        let snapshot = self
            .control_runtime
            .snapshot(self.actor)
            .map_err(ContextHostError::Driver)?;
        if snapshot.external_open
            || snapshot.inputs != 0
            || snapshot.roles != 0
            || snapshot.batches != 0
        {
            return Ok(false);
        }
        scope.close_external().map_err(ContextHostError::Runtime)?;

        // These three permanent authors have no opaque captures. Every source
        // which could invoke them has really closed above. Retire children
        // before their lifetime parent, spending one actual reap per attempt.
        for index in (0..self.resources.roles.len()).rev() {
            let role = &mut self.resources.roles[index];
            if role.capacity.is_released() {
                continue;
            }
            if !self.resources.teardown.local_exit_observed[index] {
                role.work.body_exited().map_err(ContextHostError::Runtime)?;
                role.work
                    .captures_exited()
                    .map_err(ContextHostError::Runtime)?;
                self.resources.teardown.local_exit_observed[index] = true;
            }
            self.ledger
                .reap_scope_ready(scope.id(), 1)
                .map_err(ContextHostError::Runtime)?;
            if !role.work.retirement_observer().is_retired() {
                // This is finite ready-header progress, not a host/exit wait.
                self.notify_host_progress();
                return Ok(false);
            }
            let resources = role
                .capacity
                .resources()
                .map_err(ContextHostError::Runtime)?;
            role.capacity
                .release(&resources)
                .map_err(ContextHostError::Runtime)?;
        }
        scope.join_ready().map_err(ContextHostError::Runtime)
    }

    /// The caller holds this Context cell and its same exclusive host turn.
    /// Return the removed pure actor so the Node can drop it outside the cell.
    /// The parent admission and the two Root callback inputs remain in this
    /// actual Context owner until Root really consumes HostRemoved.
    pub(crate) fn finish_metadata_teardown(
        &mut self,
        turn: ActorHostTurn<WorkerActor>,
    ) -> Result<WorkerActor, ContextTeardownRefusal> {
        if !turn.matches_registration(self.registration()) {
            return Err(ContextTeardownRefusal::BeforeActor {
                error: ContextHostError::WrongRuntime,
                turn,
            });
        }
        match self.advance_metadata_teardown() {
            Ok(true) => {}
            Ok(false) => {
                return Err(ContextTeardownRefusal::BeforeActor {
                    error: ContextHostError::TaskStillOwned,
                    turn,
                });
            }
            Err(error) => return Err(ContextTeardownRefusal::BeforeActor { error, turn }),
        }
        let actor =
            turn.retire_actor()
                .map_err(|(error, turn)| ContextTeardownRefusal::BeforeActor {
                    error: ContextHostError::Driver(error),
                    turn,
                })?;
        self.resources.registered = false;
        let scope = self
            .resources
            .scope
            .as_ref()
            .expect("the original retired actor still owns its child Scope");
        if let Err(error) = self.ledger.remove_scope(scope.id()) {
            return Err(ContextTeardownRefusal::AfterActor {
                error: ContextHostError::Runtime(error),
                actor,
            });
        }
        self.resources.scope = None;
        match self.publish_host_removed() {
            Ok(true) => Ok(actor),
            Ok(false) => Err(ContextTeardownRefusal::AfterActor {
                error: ContextHostError::HostJob(ContextHostJobError::WrongPhase),
                actor,
            }),
            Err(error) => Err(ContextTeardownRefusal::AfterActor {
                error: ContextHostError::HostJob(error),
                actor,
            }),
        }
    }

    /// After real child removal, retire the original parent-side authors.
    /// Root's HostRemoved input may still emit RetireReservation, so both
    /// lifecycle batches must truly retire before this payer's own exit.
    pub(crate) fn advance_parent_teardown(&mut self) -> Result<bool, ContextHostError> {
        if self.resources.scope.is_some() || self.resources.registered {
            return Ok(false);
        }
        let Some(jobs) = self.resources.host_jobs.as_ref() else {
            return Ok(false);
        };
        let child = self.resources.roles[LIFETIME].work.scope();
        if !jobs
            .holder_disposed(&self.control_runtime, self.actor, self.identity, child)
            .map_err(ContextHostError::HostJob)?
            || !self.retire_lifecycle().map_err(ContextHostError::HostJob)?
        {
            return Ok(false);
        }
        if self.resources.admission.capacity.is_released() {
            return Ok(true);
        }
        // The original Root effect authorizes retirement. Its Context slot
        // remains charged while this same parent-side record still exists.
        if !self.resources.teardown.parent_retire_authorized {
            return Ok(false);
        }
        if !self.resources.teardown.parent_exit_observed {
            self.resources
                .admission
                .work
                .body_exited()
                .map_err(ContextHostError::Runtime)?;
            self.resources
                .admission
                .work
                .captures_exited()
                .map_err(ContextHostError::Runtime)?;
            self.resources.teardown.parent_exit_observed = true;
        }
        let parent = self.resources.admission.work.scope().id();
        self.ledger
            .reap_scope_ready(parent, 1)
            .map_err(ContextHostError::Runtime)?;
        if !self
            .resources
            .admission
            .work
            .retirement_observer()
            .is_retired()
        {
            return Ok(false);
        }
        let remaining = self
            .resources
            .admission
            .capacity
            .resources()
            .map_err(ContextHostError::Runtime)?;
        self.resources
            .admission
            .capacity
            .release(&remaining)
            .map_err(ContextHostError::Runtime)?;
        Ok(true)
    }
}
