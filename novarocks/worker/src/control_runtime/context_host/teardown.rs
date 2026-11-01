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

//! Execution custody exits before protocol retention. The original actor,
//! observation journal and timer survive without a live execution Scope; final
//! removal separately waits for the real expiry cleanup and remaining inputs.

use super::{ContextHostError, ContextHostRecord};
use crate::control_runtime::{actors::WorkerActor, context_host_jobs::ContextHostJobError};
use novarocks_control_runtime::driver::{ActorHostTurn, Clock, DriverError, TimerSourceState};

/// Local observation progress only. These bits follow successful observations
/// on the original WorkLease; they grant no work, exit or capacity by themselves.
#[derive(Default)]
pub(super) struct ContextTeardownState {
    pub(super) parent_retire_authorized: bool,
    pub(super) failed_owner_completed: bool,
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
        _actor: WorkerActor,
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
    pub(in crate::control_runtime) fn retention_snapshot(
        &self,
    ) -> Option<super::super::node_metrics::WorkerContextRetentionSnapshot> {
        match self.resources.lifecycle.as_ref() {
            Some(lifecycle) => Some(lifecycle.retention_snapshot()),
            // Bootstrap has not materialized retention. After physical detach,
            // a moved lifecycle owner makes this address unavailable, not empty.
            None if self.resources.scope.is_some() => {
                Some(super::super::node_metrics::WorkerContextRetentionSnapshot::empty())
            }
            None => None,
        }
    }
    /// End only actual execution custody. The same actor, journal and terminal
    /// protocol records remain; its builtin timer keeps the original horizon.
    pub(crate) fn try_detach_execution(
        &mut self,
        turn: &mut ActorHostTurn<WorkerActor>,
        clock: &dyn Clock,
    ) -> Result<bool, ContextHostError> {
        if !turn.matches_registration(self.registration()) {
            return Err(ContextHostError::WrongRuntime);
        }
        let Some(scope) = self.resources.scope.as_ref().cloned() else {
            return Ok(false);
        };
        if self
            .control_runtime
            .snapshot(self.actor)
            .map_err(ContextHostError::Driver)?
            .failed
        {
            return self.try_detach_failed_execution(turn, clock);
        }
        let Some(close_epoch) = self.resources.execution_close_epoch else {
            return Ok(false);
        };
        if !self.tasks.is_empty()
            || !self.task_driver.is_idle()
            || !self.resources.routes.is_empty()
            || self.resources.failure.is_some()
            || self.resources.pending_host_jobs.driver_effect.is_some()
            || self
                .resources
                .pending_host_jobs
                .close_fanouts
                .iter()
                .any(|f| f.ticket.is_some())
            || self.resources.pending_host_jobs.driver_seal.is_some()
        {
            return Ok(false);
        }
        let Some(jobs) = self.resources.host_jobs.as_ref() else {
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
        scope.close_external().map_err(ContextHostError::Runtime)?;
        let internal = match self.resources.internal.detachment_source() {
            Ok(role) => role,
            Err(DriverError::ActorNotRetired) => return Ok(false),
            Err(error) => return Err(ContextHostError::Driver(error)),
        };
        let control = match self.resources.owner_control.detachment_source() {
            Ok(role) => role,
            Err(DriverError::ActorNotRetired) => return Ok(false),
            Err(error) => return Err(ContextHostError::Driver(error)),
        };
        let mut sources: Vec<_> = internal.into_iter().chain(control).collect();
        let mut owners: Vec<_> = self
            .resources
            .roles
            .iter_mut()
            .map(|role| (&mut role.work, &mut role.capacity))
            .collect();
        match turn.detach_execution_with_metadata_sources(
            &self.request_scope,
            &mut owners,
            &mut sources,
        ) {
            Ok(()) => {}
            Err(DriverError::ActorNotRetired | DriverError::RoleBusy) => return Ok(false),
            Err(error) => return Err(ContextHostError::Driver(error)),
        }
        self.resources.internal.execution_detached();
        self.resources.owner_control.execution_detached();
        self.ledger
            .remove_scope(scope.id())
            .map_err(ContextHostError::Runtime)?;
        tracing::debug!(query_high=self.identity.query_execution_id().query_id().high(),
            query_low=self.identity.query_execution_id().query_id().low(),
            attempt=self.identity.query_execution_id().attempt_id().get(),
            execution_scope=?scope.id(), "Worker Context execution custody retired");
        self.resources.scope = None;
        self.resources.roles.clear();
        // Sample after actual Scope AND its intrinsic parent-link removal.
        let retired_at = novarocks_worker_protocol::lease::MonotonicInstant::from_origin(
            std::time::Duration::from_nanos(clock.now().0),
        );
        self.resources
            .lifecycle
            .as_mut()
            .ok_or(ContextHostError::TaskStillOwned)?
            .publish_execution_retired(close_epoch, retired_at)
            .map_err(ContextHostError::HostJob)?;
        self.notify_host_progress();
        Ok(true)
    }
    /// Failure closes only the original source consumers. The actual failed
    /// actor and its protocol retention survive removal of execution custody.
    fn try_detach_failed_execution(
        &mut self,
        turn: &mut ActorHostTurn<WorkerActor>,
        clock: &dyn Clock,
    ) -> Result<bool, ContextHostError> {
        if !self.failed_owner_completed()
            || self.resources.failure.is_some()
            || !self.tasks.is_empty()
            || !self.task_driver.is_idle()
            || !self.resources.requests.is_empty()
            || !self.resources.request_partials.is_empty()
            || !self.resources.routes.is_empty()
        {
            return Ok(false);
        }
        let proof = self
            .control_runtime
            .failure_proof_for(self.registration())
            .map_err(ContextHostError::Driver)?;
        if !self
            .resources
            .lifecycle
            .as_mut()
            .ok_or(ContextHostError::TaskStillOwned)?
            .withdraw_failed_callback(&proof)
            .map_err(ContextHostError::HostJob)?
            || !self.retire_owner_control()?
            || !self
                .retire_internal_source()
                .map_err(ContextHostError::Driver)?
        {
            return Ok(false);
        }
        let scope = self
            .resources
            .scope
            .as_ref()
            .ok_or(ContextHostError::TaskStillOwned)?
            .clone();
        let jobs = self
            .resources
            .host_jobs
            .as_ref()
            .ok_or(ContextHostError::TaskStillOwned)?;
        if !jobs.retire_callbacks().map_err(ContextHostError::HostJob)?
            || !jobs.retire_holder().map_err(ContextHostError::HostJob)?
            || !jobs
                .holder_disposed(&self.control_runtime, self.actor, self.identity, &scope)
                .map_err(ContextHostError::HostJob)?
        {
            return Ok(false);
        }
        scope.close_external().map_err(ContextHostError::Runtime)?;
        let mut owners: Vec<_> = self
            .resources
            .roles
            .iter_mut()
            .map(|role| (&mut role.work, &mut role.capacity))
            .collect();
        match turn.detach_failed_execution_for_custodian(&proof, &self.request_scope, &mut owners) {
            Ok(()) => {}
            Err(DriverError::ActorNotRetired | DriverError::RoleBusy) => return Ok(false),
            Err(error) => return Err(ContextHostError::Driver(error)),
        }
        self.ledger
            .remove_scope(scope.id())
            .map_err(ContextHostError::Runtime)?;
        tracing::debug!(query_high=self.identity.query_execution_id().query_id().high(),
            query_low=self.identity.query_execution_id().query_id().low(),
            attempt=self.identity.query_execution_id().attempt_id().get(),
            execution_scope=?scope.id(), "Worker Context execution custody retired");
        self.resources.scope = None;
        self.resources.roles.clear();
        let retired_at = novarocks_worker_protocol::lease::MonotonicInstant::from_origin(
            std::time::Duration::from_nanos(clock.now().0),
        );
        self.resources
            .lifecycle
            .as_mut()
            .ok_or(ContextHostError::TaskStillOwned)?
            .record_failed_execution_retired(
                &proof,
                self.resources.execution_close_epoch,
                retired_at,
            )
            .map_err(ContextHostError::HostJob)?;
        self.notify_host_progress();
        Ok(true)
    }
    pub(crate) fn failed_lifecycle_intent(
        &mut self,
        proof: &novarocks_control_runtime::driver::ActorFailureProof,
        now: novarocks_worker_protocol::lease::MonotonicInstant,
    ) -> Result<
        Option<crate::control_runtime::control_internal::RootLifecycleFact>,
        ContextHostJobError,
    > {
        // Root's actual RemoveHost is also the process-close authority: its
        // original forget job ends retention without waiting for the horizon.
        if self.resources.scope.is_some() || self.resources.removal.is_some() {
            return Ok(None);
        }
        self.resources
            .lifecycle
            .as_mut()
            .ok_or(ContextHostJobError::WrongPhase)?
            .failed_lifecycle_intent(proof, now)
    }
    pub(crate) fn install_failed_lifecycle_receipt(
        &mut self,
        proof: &novarocks_control_runtime::driver::ActorFailureProof,
        fact: crate::control_runtime::control_internal::RootLifecycleFact,
        receipt: crate::control_runtime::control_internal::RootLifecycleReceipt,
    ) -> Result<
        (),
        (
            ContextHostJobError,
            crate::control_runtime::control_internal::RootLifecycleReceipt,
        ),
    > {
        let Some(lifecycle) = self.resources.lifecycle.as_mut() else {
            return Err((ContextHostJobError::WrongPhase, receipt));
        };
        lifecycle.install_failed_lifecycle_receipt(proof, fact, receipt)
    }
    pub(crate) fn advance_lifecycle_metadata(
        &mut self,
        turn: &mut ActorHostTurn<WorkerActor>,
        budget: usize,
    ) -> Result<bool, ContextHostJobError> {
        match self.resources.lifecycle.as_mut() {
            Some(original) => original.advance_metadata(turn, budget),
            None => Ok(false),
        }
    }

    pub(crate) fn stage_removal(
        &mut self,
        owner: crate::control_runtime::context_removal::ContextRemovalOwner,
        job: super::task_driver::TaskDriverHostJob,
    ) -> Result<
        (),
        (
            crate::control_runtime::context_removal::ContextRemovalOwner,
            super::task_driver::TaskDriverHostJob,
        ),
    > {
        if owner.context() != self.identity
            || self.resources.removal.is_some()
            || self.resources.scope.is_some()
        {
            return Err((owner, job));
        }
        self.resources.removal = Some(owner);
        self.resources.removal_job = Some(job);
        self.notify_host_progress();
        Ok(())
    }
    pub(crate) fn advance_removal(
        &mut self,
        ports: &super::TaskDriverPorts,
    ) -> Result<bool, ContextHostError> {
        if let Some(job) = self.resources.removal_job.take() {
            let Some(executor) = ports.lifecycle.as_ref() else {
                self.resources.removal_job = Some(job);
                return Err(ContextHostError::MissingLifecycleExecutor);
            };
            let revision = ports.capacity.as_ref().map(|signal| signal.revision());
            if let Err((error, original)) = executor.try_submit_for_group(
                job,
                crate::control_runtime::preparation::HostFairnessGroup::Context(self.identity),
            ) {
                self.resources.removal_job = Some(original);
                if !matches!(error, crate::control_runtime::host_executor::HostSubmitError::Queue(
                    novarocks_control_runtime_core::fair_queue::FairQueueError::Full
                    | novarocks_control_runtime_core::fair_queue::FairQueueError::ContextFull))
                {
                    return Err(ContextHostError::RemovalSubmit(error));
                }
                if let (Some(signal), Some(revision)) = (&ports.capacity, revision) {
                    signal
                        .register_waiter(self.registration(), revision)
                        .map_err(ContextHostError::Capacity)?;
                } else {
                    return Err(ContextHostError::MissingCapacitySignal);
                }
                return Ok(false);
            }
        }
        let Some(owner) = self.resources.removal.as_ref() else {
            return Ok(false);
        };
        if owner
            .forget_retired()
            .map_err(|e| ContextHostError::HostJob(ContextHostJobError::Removal(e)))?
        {
            return Ok(false);
        }
        let reaped = self
            .ledger
            .reap_scope_ready(self.request_scope.id(), 1)
            .map_err(ContextHostError::Runtime)?;
        Ok(reaped != 0)
    }
    pub(crate) fn advance_removed_root_tail(
        &mut self,
        root: &mut crate::control_runtime::process_control::ProcessControlOwner,
    ) -> Result<bool, ContextHostError> {
        let Some(removal) = self.resources.removal.as_mut() else {
            return Ok(false);
        };
        if !removal
            .publish_host_removed(root)
            .map_err(|e| ContextHostError::HostJob(ContextHostJobError::Removal(e)))?
        {
            return Ok(false);
        }
        let done = removal
            .retire_after_host_removed()
            .map_err(|e| ContextHostError::HostJob(ContextHostJobError::Removal(e)))?;
        if !done {
            self.ledger
                .reap_scope_ready(self.request_scope.id(), 1)
                .map_err(ContextHostError::Runtime)?;
        }
        Ok(done)
    }
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
        if self.resources.scope.is_some() {
            return Ok(false);
        }
        if !self.resources.registered {
            return Ok(true);
        }
        if !self.resources.control_admission_closed
            || self.resources.removal_job.is_some()
            || !self.resources.admission.capacity.is_released()
        {
            return Ok(false);
        }
        let Some(removal) = self.resources.removal.as_ref() else {
            return Ok(false);
        };
        if !removal
            .forget_retired()
            .map_err(|e| ContextHostError::HostJob(ContextHostJobError::Removal(e)))?
        {
            return Ok(false);
        }
        let snapshot = self
            .control_runtime
            .snapshot(self.actor)
            .map_err(ContextHostError::Driver)?;
        Ok(!snapshot.external_open
            && snapshot.inputs == 0
            && snapshot.roles == 0
            && snapshot.batches == 0
            && self.dispatch.is_empty()
            && self
                .control_runtime
                .timer_source_state(self.actor)
                .map_err(ContextHostError::Driver)?
                == TimerSourceState::ClosedDrained)
    }

    /// The caller holds this Context cell and its same exclusive host turn.
    /// Actual expiry cleanup and all remaining original inputs have drained.
    /// Return the removed pure actor for Drop outside the cell; Node removes
    /// the same directory cell before the short Root owner sends HostRemoved.
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
        if let Some(scope) = self.resources.scope.take()
            && let Err(error) = self.ledger.remove_scope(scope.id())
        {
            self.resources.scope = Some(scope);
            return Err(ContextTeardownRefusal::AfterActor {
                error: ContextHostError::Runtime(error),
                _actor: actor,
            });
        }
        // Node removes the exact directory cell before publishing HostRemoved.
        Ok(actor)
    }

    pub(crate) fn parent_retirement_ready(&self) -> bool {
        self.resources.scope.is_none()
            && self.resources.teardown.parent_retire_authorized
            && !self.resources.admission.capacity.is_released()
    }
    /// Retire the original admission after actual execution removal and the
    /// original retirement callback. The retained actor and its own horizon
    /// impose no further Work or admission obligation on this parent.
    pub(crate) fn advance_parent_teardown(&mut self) -> Result<bool, ContextHostError> {
        if self.resources.scope.is_some() {
            return Ok(false);
        }
        if !self.retire_lifecycle().map_err(ContextHostError::HostJob)? {
            return Ok(false);
        }
        if self.resources.admission.capacity.is_released() {
            return Ok(true);
        }
        // The actual Root effect, not a local phase guess, authorizes this
        // remaining parent's own body/capture exit.
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
        self.resources
            .admission
            .capacity
            .finish()
            .map_err(ContextHostError::Runtime)?;
        Ok(true)
    }
}
