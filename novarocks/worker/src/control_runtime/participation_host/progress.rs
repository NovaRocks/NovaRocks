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

//! One bounded turn over the original Context's M/R/H owners. Pending slots
//! hold the actual admitted jobs, including every capture and return sender;
//! they are neither new work admission nor a second obligation ledger.

use super::ParticipationHostRecord;
use crate::control_runtime::{
    actors::WorkerAction,
    host_executor::HostSubmitError,
    participation_executors::{
        ParticipationCancelError, ParticipationExecutorHandles, ParticipationSubmitError,
    },
    participation_host_jobs::{
        ParticipationHostJob, ParticipationHostJobError, ParticipationHostReturned,
    },
    task_capacity::TaskCapacityError,
};
use novarocks_control_runtime::actor_turn::{
    ActorTurnError, EffectCompletionRefusal, EffectTicket,
};
use novarocks_control_runtime_core::fair_queue::FairQueueError;

#[derive(Default)]
pub(super) struct PendingHostJobs {
    // The bootstrap admits exactly one of each original role before capture.
    jobs: [Option<ParticipationHostJob>; 3],
    materialization_submitted: bool,
    materialization_cancel_checked: bool,
    // The same original claimed ticket stays in its owner across refusal.
    // This is not a copy of runtime obligations or a second work queue.
    pub(super) actor_effect: Option<EffectTicket<WorkerAction>>,
    // Exactly the two original close actions can await their real host jobs
    // without blocking unrelated stop/release effects in the same batch.
    pub(super) close_fanouts:
        Vec<super::task_io::participation_admission::ParticipationCloseFanout>,
    pub(super) actor_seal: Option<EffectTicket<WorkerAction>>,
    pub(super) root_release_after: Option<novarocks_execution_contract::TaskIdentity>,
    pub(super) root_release_finished: bool,
    pub(super) effect_scan_remaining: usize,
    pub(super) effect_scan_version: Option<u64>,
    pub(super) failure_after: Option<novarocks_control_runtime_core::identity::BatchId>,
    pub(super) route_sweep_version: Option<u64>,
    pub(super) route_sweep_complete: bool,
    pub(super) route_after: Option<novarocks_control_runtime_core::identity::InputId>,
    pub(super) route_until: Option<novarocks_control_runtime_core::identity::InputId>,
}

#[derive(Debug)]
pub(crate) enum ParticipationHostProgressError {
    Host(ParticipationHostJobError),
    Submit(ParticipationSubmitError),
    Capacity(TaskCapacityError),
    Cancel(ParticipationCancelError),
    Fault(ParticipationHostReturned),
}
impl std::fmt::Display for ParticipationHostProgressError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Host(error) => write!(f, "Participation host progress failed: {error}"),
            Self::Submit(error) => {
                write!(f, "Participation original job submission failed: {error:?}")
            }
            Self::Capacity(error) => {
                write!(f, "Participation capacity notification failed: {error:?}")
            }
            Self::Cancel(error) => write!(
                f,
                "Participation original queued job cancellation failed: {error:?}"
            ),
            Self::Fault(outcome) => {
                write!(f, "Participation original host reported a fault: {outcome}")
            }
        }
    }
}
impl std::error::Error for ParticipationHostProgressError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Host(error) => Some(error),
            Self::Submit(ParticipationSubmitError::Runtime(error)) => Some(error),
            Self::Capacity(TaskCapacityError::ActorTurn(error)) => Some(error),
            Self::Cancel(ParticipationCancelError::Runtime(error)) => Some(error),
            Self::Fault(ParticipationHostReturned::StartRefused(error)) => Some(error),
            Self::Submit(
                ParticipationSubmitError::WrongOwner
                | ParticipationSubmitError::StopRequired
                | ParticipationSubmitError::Host(_),
            )
            | Self::Capacity(
                TaskCapacityError::WrongOwner
                | TaskCapacityError::ForeignRegistration
                | TaskCapacityError::FutureRevision
                | TaskCapacityError::Queue(_),
            )
            | Self::Cancel(
                ParticipationCancelError::WrongOwner
                | ParticipationCancelError::StopRequired
                | ParticipationCancelError::Queue(_),
            )
            | Self::Fault(
                ParticipationHostReturned::Materialized(_)
                | ParticipationHostReturned::MaterializationWithdrawn
                | ParticipationHostReturned::Released { .. }
                | ParticipationHostReturned::Disposed
                | ParticipationHostReturned::Panicked,
            ) => None,
        }
    }
}
impl From<ParticipationHostJobError> for ParticipationHostProgressError {
    fn from(error: ParticipationHostJobError) -> Self {
        Self::Host(error)
    }
}
#[derive(Debug, Default)]
pub(crate) struct ParticipationHostProgress {
    pub returned: usize,
    pub submitted: usize,
    pub callbacks_retired: bool,
    pub holder_retired: bool,
    pub owner_control_retired: bool,
    pub internal: Option<super::ParticipationInternalAdvance>,
    pub internal_source_retired: bool,
}

impl ParticipationHostRecord {
    /// The real action owner calls this after completion, including Direct
    /// and Reply actions at the end of an M/R callback batch. Wake the same
    /// Context inventory so its original input/report/header can retire.
    /// Publication and metadata FIFO handoffs remain their separate facts.
    pub(crate) fn complete_host_effect(
        &self,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<(), EffectCompletionRefusal<WorkerAction>> {
        let input = ticket.input;
        let task = match self.effect_producer_task(&ticket) {
            Ok(task) => task,
            Err(error) => return Err(EffectCompletionRefusal { error, ticket }),
        };
        let registration = self
            .resources
            .registration
            .as_ref()
            .expect("the original completion observer validated this registration");
        self.control_runtime
            .effect_completed_for(registration, ticket)?;
        self.notify_effect_producer_task(task);
        // A late close/seal body can outlive its already delivered reply.
        // Wake that exact original R when its own batch changes; waking only
        // the Context would leave its drained ready slot parked indefinitely.
        if let Some(request) = self
            .request_inputs
            .get(&input)
            .and_then(|work| self.resources.requests.get(work))
        {
            request.note_input_progress();
        }
        self.notify_host_progress();
        Ok(())
    }

    /// Save the exact original input's Task address before its last physical
    /// completion may retire the batch. This is observation, not publication
    /// authority, and never derives Task ownership from an R financial Scope.
    pub(crate) fn effect_producer_task(
        &self,
        ticket: &EffectTicket<WorkerAction>,
    ) -> Result<Option<novarocks_execution_contract::TaskIdentity>, ActorTurnError> {
        let registration = self
            .resources
            .registration
            .as_ref()
            .ok_or(ActorTurnError::PublicationTargetMismatch)?;
        self.control_runtime.validate_registration(registration)?;
        if registration.actor() != self.actor || ticket.actor != self.actor {
            return Err(ActorTurnError::PublicationTargetMismatch);
        }
        self.control_runtime.effect_completion_observer(ticket)?;
        Ok(self
            .control_runtime
            .input_custody(self.actor, ticket.input)?
            .and_then(|custody| self.task_io.sources.get(&custody.source).copied()))
    }

    /// Call only after the same original completion succeeds. Deferred Reply
    /// owners use the saved address; this does not complete an effect twice.
    pub(crate) fn notify_effect_producer_task(
        &self,
        task: Option<novarocks_execution_contract::TaskIdentity>,
    ) {
        if let Some(task) = task {
            self.notify_task_io_progress(task);
        }
    }

    /// Record an actual Task producer/Core/state transition, including a
    /// notification coalesced while its finite retirement round is running.
    pub(super) fn notify_task_io_progress(&self, task: novarocks_execution_contract::TaskIdentity) {
        if let Some(
            super::TaskHostOwner::Ready(owner) | super::TaskHostOwner::Initializing(owner),
        ) = self.tasks.get(&task)
        {
            owner.pending_notice().pending();
        }
    }

    /// Move the same captured M into this original owner before submitting to
    /// physical workers. Refusal returns it untouched to the caller. Recovery
    /// through into_resources preserves this slot along with the other roles.
    pub(crate) fn stage_materialization(
        &mut self,
        job: ParticipationHostJob,
    ) -> Result<(), (ParticipationHostJobError, ParticipationHostJob)> {
        let Some(jobs) = &self.resources.host_jobs else {
            return Err((ParticipationHostJobError::WrongPhase, job));
        };
        if !job.is_materialization() || !job.belongs_to_jobs(jobs) {
            return Err((ParticipationHostJobError::WrongOwner, job));
        }
        let pending = &mut self.resources.pending_host_jobs;
        if pending.jobs[0].is_some() || pending.materialization_submitted {
            return Err((ParticipationHostJobError::WrongPhase, job));
        }
        pending.jobs[0] = Some(job);
        self.notify_host_progress();
        Ok(())
    }

    /// Permission to execute the already instructed R, not a new authority.
    /// Every condition is read from this same Context's original inventory;
    /// advancing one positive Root-idle cursor is the only self-notification.
    fn advance_release_execution_gate(&mut self, budget: usize) -> bool {
        if budget == 0
            || !self
                .resources
                .host_jobs
                .as_ref()
                .is_some_and(|jobs| jobs.release_pending())
            || !self.task_io_roots_sealed()
            || self.resources.pending_host_jobs.actor_seal.is_some()
        {
            return false;
        }
        if !self.resources.pending_host_jobs.root_release_finished {
            match self
                .resources
                .root_reads
                .next_release_root(self.resources.pending_host_jobs.root_release_after)
            {
                Some((task, true)) => {
                    self.resources.pending_host_jobs.root_release_after = Some(task);
                    self.notify_host_progress();
                    return false;
                }
                Some((_, false)) => return false,
                None => self.resources.pending_host_jobs.root_release_finished = true,
            }
        }
        // R snapshots normal-close retention only after Finalize/Retire and
        // actual Task disposal, never from Core intent or a Task's terminal.
        self.tasks.is_empty() && self.task_io.is_idle()
    }

    pub(super) fn notify_host_progress(&self) {
        if let Some(notice) = &self.resources.notice {
            notice.notify();
        }
    }

    /// Called by this Context's host owner after an original ready notice or
    /// Context effect completion. No foreign getter, destructor or host call
    /// runs here. A failure retains all original pending jobs in this owner.
    /// This turn does not remove an actor/Scope or assert whole Context exit.
    /// Physical Full retains the same job and waits for the original capacity
    /// revision. Closure, failure and invariant refusals remain typed errors.
    pub(crate) fn advance_host_io(
        &mut self,
        handles: &ParticipationExecutorHandles,
    ) -> Result<ParticipationHostProgress, ParticipationHostProgressError> {
        self.advance_host_io_for_owner(handles, None, 3)
    }

    pub(super) fn advance_host_io_for_owner(
        &mut self,
        handles: &ParticipationExecutorHandles,
        failed: Option<&super::failure::ScopePublicationsDrained<'_>>,
        budget: usize,
    ) -> Result<ParticipationHostProgress, ParticipationHostProgressError> {
        {
            let jobs = self
                .resources
                .host_jobs
                .as_ref()
                .ok_or(ParticipationHostJobError::WrongPhase)?;
            if !handles.accepts_owner(jobs) {
                return Err(ParticipationHostJobError::WrongOwner.into());
            }
        }
        let release_ready = self.advance_release_execution_gate(budget);
        let jobs = self
            .resources
            .host_jobs
            .as_ref()
            .expect("same original Participation jobs after metadata progress");
        let mut progress = ParticipationHostProgress::default();
        if let Some(stage) = failed {
            let returns = jobs.poll_failed_returns(stage, budget)?;
            progress.returned = returns.observed;
        } else {
            progress.returned = jobs.poll_returns()?;
        }
        if let Some(fault) = jobs.fault() {
            return Err(ParticipationHostProgressError::Fault(fault));
        }
        progress.callbacks_retired = match failed {
            Some(stage) => jobs.retire_failed_callbacks(stage, budget)?,
            None => jobs.retire_callbacks()?,
        };
        progress.holder_retired = jobs.retire_holder()?;
        progress.owner_control_retired = self
            .resources
            .owner_control
            .close()
            .map_err(ParticipationHostJobError::ActorTurn)?;
        progress.internal = Some(
            self.resources
                .internal
                .advance()
                .map_err(ParticipationHostJobError::ActorTurn)?,
        );

        let stopped = jobs
            .scope_stopped()
            .map_err(ParticipationHostJobError::Runtime)?;
        if let Some(stage) = failed {
            // The actual drained failure stage authorizes only R's original
            // instruction. Its body still crosses the same physical gate.
            jobs.request_failed_release(stage)?;
        }
        let registration = self.registration().clone();
        let capacity = handles.capacity_signal();
        let pending = &mut self.resources.pending_host_jobs;
        if stopped && pending.materialization_submitted && !pending.materialization_cancel_checked {
            // Cancel and physical pop arbitrate in the original queue. A pop
            // win supplies no exit fact and requires no replacement job.
            match handles.cancel_materialization(jobs) {
                Ok(original) => {
                    assert!(pending.jobs[0].is_none());
                    pending.jobs[0] = Some(original);
                    pending.materialization_submitted = false;
                }
                Err(ParticipationCancelError::Queue(FairQueueError::UnknownWork)) => {}
                Err(error) => return Err(ParticipationHostProgressError::Cancel(error)),
            }
            pending.materialization_cancel_checked = true;
        }
        if release_ready && pending.jobs[1].is_none() {
            pending.jobs[1] = jobs.take_release_job()?;
        }
        if pending.jobs[2].is_none()
            && pending.actor_seal.is_none()
            && self.tasks.is_empty()
            && self.task_io.is_idle()
        {
            // Process-owned R metadata can remain live. M/R retirement and
            // the original Root/capture gates, rather than an R map census,
            // decide whether this hot holder can actually be destroyed.
            pending.jobs[2] =
                jobs.take_holder_disposal_with_roots(&mut self.resources.root_reads)?;
        }
        for (index, slot) in pending.jobs.iter_mut().enumerate() {
            let Some(original) = slot.take() else {
                continue;
            };
            // Observe before submission: a physical tail racing the returned
            // Full is retained by register_waiter's post-insertion check.
            let before = capacity.revision();
            let submitted = if index == 0 && stopped {
                handles.try_submit_stopped(original)
            } else {
                handles.try_submit(original)
            };
            if let Err((error, original)) = submitted {
                *slot = Some(original);
                if matches!(
                    error,
                    ParticipationSubmitError::Host(HostSubmitError::Queue(
                        FairQueueError::Full | FairQueueError::GroupFull
                    ))
                ) {
                    capacity
                        .register_waiter(&registration, before)
                        .map_err(ParticipationHostProgressError::Capacity)?;
                    // Independent cleanup jobs may still enter their pool.
                    continue;
                }
                return Err(ParticipationHostProgressError::Submit(error));
            }
            if index == 0 && !stopped {
                pending.materialization_submitted = true;
            }
            progress.submitted += 1;
        }
        progress.internal_source_retired = self
            .retire_internal_source()
            .map_err(ParticipationHostJobError::ActorTurn)?;
        Ok(progress)
    }
}
