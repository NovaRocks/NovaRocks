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

use super::ContextHostRecord;
use crate::control_runtime::{
    actors::WorkerAction,
    context_executors::{ContextCancelError, ContextExecutorHandles, ContextSubmitError},
    context_host_jobs::{ContextHostJob, ContextHostJobError, ContextHostReturned},
};
use novarocks_control_runtime::driver::{DriverError, EffectCompletionRefusal, EffectTicket};
use novarocks_control_runtime_core::fair_queue::FairQueueError;

#[derive(Default)]
pub(super) struct PendingHostJobs {
    // The bootstrap admits exactly one of each original role before capture.
    jobs: [Option<ContextHostJob>; 3],
    materialization_submitted: bool,
    materialization_cancel_checked: bool,
    // The same original claimed ticket stays in its owner across refusal.
    // This is not a copy of runtime obligations or a second work queue.
    pub(super) driver_effect: Option<EffectTicket<WorkerAction>>,
    // Exactly the two original close actions can await their real host jobs
    // without blocking unrelated stop/release effects in the same batch.
    pub(super) driver_close: Option<EffectTicket<WorkerAction>>,
    pub(super) driver_seal: Option<EffectTicket<WorkerAction>>,
    // The one original Release effect waits on Context-owned Root aliases.
    pub(super) driver_release: Option<EffectTicket<WorkerAction>>,
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
pub(crate) enum ContextHostProgressError {
    Host(ContextHostJobError),
    Submit(ContextSubmitError),
    Cancel(ContextCancelError),
    Fault(ContextHostReturned),
}
impl From<ContextHostJobError> for ContextHostProgressError {
    fn from(error: ContextHostJobError) -> Self {
        Self::Host(error)
    }
}
#[derive(Debug, Default)]
pub(crate) struct ContextHostProgress {
    pub returned: usize,
    pub submitted: usize,
    pub callbacks_retired: bool,
    pub holder_retired: bool,
    pub owner_control_retired: bool,
    pub internal: Option<super::ContextInternalAdvance>,
    pub internal_source_retired: bool,
}

impl ContextHostRecord {
    /// The real action owner calls this after completion, including Direct
    /// and Reply actions at the end of an M/R callback batch. Wake the same
    /// Context inventory so its original input/report/header can retire.
    /// Publication and metadata FIFO handoffs remain their separate facts.
    pub(crate) fn complete_host_effect(
        &self,
        ticket: EffectTicket<WorkerAction>,
    ) -> Result<(), EffectCompletionRefusal<WorkerAction>> {
        let Some(registration) = &self.resources.registration else {
            return Err(EffectCompletionRefusal {
                error: DriverError::PublicationTargetMismatch,
                ticket,
            });
        };
        let input = ticket.input;
        self.control_runtime
            .effect_completed_for(registration, ticket)?;
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

    /// Move the same captured M into this original owner before submitting to
    /// physical workers. Refusal returns it untouched to the caller. Recovery
    /// through into_resources preserves this slot along with the other roles.
    pub(crate) fn stage_materialization(
        &mut self,
        job: ContextHostJob,
    ) -> Result<(), (ContextHostJobError, ContextHostJob)> {
        let Some(jobs) = &self.resources.host_jobs else {
            return Err((ContextHostJobError::WrongPhase, job));
        };
        if !job.is_materialization() || !job.belongs_to_jobs(jobs) {
            return Err((ContextHostJobError::WrongOwner, job));
        }
        let pending = &mut self.resources.pending_host_jobs;
        if pending.jobs[0].is_some() || pending.materialization_submitted {
            return Err((ContextHostJobError::WrongPhase, job));
        }
        pending.jobs[0] = Some(job);
        self.notify_host_progress();
        Ok(())
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
    /// Submission refusal is an explicit owner-driver error, not an ordinary
    /// parked retry: the pools cover all C original jobs plus N physical tails.
    /// No pool-capacity wake is promised by this operation after such an error.
    pub(crate) fn advance_host_io(
        &mut self,
        handles: &ContextExecutorHandles,
    ) -> Result<ContextHostProgress, ContextHostProgressError> {
        let jobs = self
            .resources
            .host_jobs
            .as_ref()
            .ok_or(ContextHostJobError::WrongPhase)?;
        if !handles.accepts_owner(jobs) {
            return Err(ContextHostJobError::WrongOwner.into());
        }
        let mut progress = ContextHostProgress {
            returned: jobs.poll_returns()?,
            ..Default::default()
        };
        if let Some(fault) = jobs.fault() {
            return Err(ContextHostProgressError::Fault(fault));
        }
        progress.callbacks_retired = jobs.retire_callbacks()?;
        progress.holder_retired = jobs.retire_holder()?;
        progress.owner_control_retired = self
            .resources
            .owner_control
            .close()
            .map_err(ContextHostJobError::Driver)?;
        progress.internal = Some(
            self.resources
                .internal
                .advance()
                .map_err(ContextHostJobError::Driver)?,
        );

        let stopped = jobs.scope_stopped().map_err(ContextHostJobError::Runtime)?;
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
                Err(ContextCancelError::Queue(FairQueueError::UnknownWork)) => {}
                Err(error) => return Err(ContextHostProgressError::Cancel(error)),
            }
            pending.materialization_cancel_checked = true;
        }
        if pending.jobs[1].is_none() {
            pending.jobs[1] = jobs.take_release_job()?;
        }
        if pending.jobs[2].is_none()
            && pending.driver_seal.is_none()
            && pending.driver_release.is_none()
            && self.tasks.is_empty()
            && self.task_driver.is_idle()
            && self.resources.requests.is_empty()
            && self.resources.request_partials.is_empty()
        {
            pending.jobs[2] =
                jobs.take_holder_disposal_with_roots(&mut self.resources.root_reads)?;
        }
        for (index, slot) in pending.jobs.iter_mut().enumerate() {
            let Some(original) = slot.take() else {
                continue;
            };
            let submitted = if index == 0 && stopped {
                handles.try_submit_stopped(original)
            } else {
                handles.try_submit(original)
            };
            if let Err((error, original)) = submitted {
                *slot = Some(original);
                return Err(ContextHostProgressError::Submit(error));
            }
            if index == 0 && !stopped {
                pending.materialization_submitted = true;
            }
            progress.submitted += 1;
        }
        progress.internal_source_retired = self
            .retire_internal_source()
            .map_err(ContextHostJobError::Driver)?;
        Ok(progress)
    }
}
