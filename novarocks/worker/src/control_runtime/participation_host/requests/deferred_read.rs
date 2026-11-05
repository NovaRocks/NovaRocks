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

//! Stable reads cross hot admission closure only through the original R and
//! the actual Task read seal. Its index owns no Work, request or source;
//! shared neutral seal facts grant no execution or capacity authority.

use super::*;
use novarocks_control_runtime_core::completion::Consumption;
use novarocks_worker_protocol::participation::TaskReadCommand;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

type SealedRead = Arc<super::super::super::lifecycle_read::TaskReadSnapshot>;

#[derive(Default)]
pub(in crate::control_runtime::participation_host) struct TaskReadWaiters {
    waiting: BTreeMap<TaskIdentity, ReadWaiterRows>,
    // Each value is the actual seal transfer, retained only while its already
    // paid R waiters still need the original self-source handoff.
    ready: BTreeMap<TaskIdentity, SealedRead>,
}
#[derive(Default)]
struct ReadWaiterRows {
    pending: BTreeSet<ScopedWorkRef>,
    // Keep the same nonowning route until the original R actually retires.
    ended: BTreeSet<ScopedWorkRef>,
}
impl TaskReadWaiters {
    #[cfg(test)]
    pub(in crate::control_runtime::participation_host) fn is_empty_for_test(&self) -> bool {
        self.waiting.is_empty() && self.ready.is_empty()
    }
    fn insert(&mut self, task: TaskIdentity, work: ScopedWorkRef) {
        assert!(self.waiting.entry(task).or_default().pending.insert(work));
    }
    fn suppress(&mut self, task: TaskIdentity, work: ScopedWorkRef) {
        let row = self
            .waiting
            .get_mut(&task)
            .expect("same original read route");
        assert!(row.pending.remove(&work));
        assert!(row.ended.insert(work));
        if row.pending.is_empty() {
            self.ready.remove(&task);
        }
    }

    pub(in crate::control_runtime::participation_host) fn sealed(
        &mut self,
        task: TaskIdentity,
        proof: SealedRead,
    ) {
        if self
            .waiting
            .get(&task)
            .is_some_and(|row| !row.pending.is_empty())
        {
            assert_eq!(proof.status.identity(), task);
            if let Some(previous) = self.ready.insert(task, proof.clone()) {
                assert!(
                    Arc::ptr_eq(&previous, &proof),
                    "one original Task read seal"
                );
            }
        }
    }
    fn remove(&mut self, task: TaskIdentity, work: ScopedWorkRef) {
        if let Some(waiting) = self.waiting.get_mut(&task) {
            assert!(waiting.pending.remove(&work) || waiting.ended.remove(&work));
            if waiting.pending.is_empty() {
                self.ready.remove(&task);
            }
            if waiting.pending.is_empty() && waiting.ended.is_empty() {
                self.waiting.remove(&task);
                self.ready.remove(&task);
            }
        }
    }
    fn next_ready(&self) -> Option<ScopedWorkRef> {
        let task = self.ready.first_key_value()?.0;
        self.waiting.get(task)?.pending.first().copied()
    }
}

pub(super) struct DeferredTaskRead {
    task: TaskIdentity,
    // The original internal input still owes its actual receipt and batch
    // retirement. A reply, timeout or external Drop cannot acknowledge it.
    pub(super) resume: Option<InputId>,
    inactive: bool,
}

impl ParticipationHostRecord {
    pub(crate) fn task_read_awaits_seal(&self, task: TaskIdentity) -> bool {
        !self.has_retired_task_read(task)
            && matches!(self.tasks.get(&task),
                Some(super::super::TaskHostOwner::Ready(owner))
                    if !owner.request_admission_open())
    }
    pub(crate) fn submit_deferred_task_read(
        &mut self,
        work: ScopedWorkRef,
        command: ParticipationCommand,
    ) -> Result<(), ParticipationRequestSubmitRefusal> {
        let (task, read) = match command {
            ParticipationCommand::GetFinalInfo { task } => (task, TaskReadCommand::FinalInfo),
            ParticipationCommand::GetDynamicFilters { task, acknowledged } => {
                (task, TaskReadCommand::DynamicFilters { acknowledged })
            }
            other => {
                return Err(ParticipationRequestSubmitRefusal {
                    error: ParticipationRequestError::InvalidClass,
                    command: Some(other),
                });
            }
        };
        let reject = |error| ParticipationRequestSubmitRefusal {
            error,
            command: Some(read.into_command(task)),
        };
        if !self.task_read_awaits_seal(task) {
            return Err(reject(ParticipationRequestError::WrongPhase));
        }
        let original = self
            .resources
            .requests
            .get_mut(&work)
            .ok_or_else(|| reject(ParticipationRequestError::UnknownRequest))?;
        if original.submitted
            || original.task_route.is_some()
            || original.deferred_task_read.is_some()
            || original.retired_task_read.is_some()
        {
            return Err(reject(ParticipationRequestError::WrongPhase));
        }
        original.deferred_task_read = Some(DeferredTaskRead {
            task,
            resume: None,
            inactive: false,
        });
        self.task_read_waiters.insert(task, work);
        // Core owns the same R's deadline from this first input, before the
        // read seal arrives. The original command is frozen there once.
        original.submit(ParticipationCommand::AwaitTaskReadSeal {
            task,
            command: read,
        })
    }
    pub(super) fn remove_retired_task_read_waiter(&mut self, work: ScopedWorkRef) {
        let original = self
            .resources
            .requests
            .get_mut(&work)
            .expect("same retired original R");
        if let Some(waiter) = original.deferred_task_read.take() {
            assert!(
                waiter.resume.is_none(),
                "the original continuation actually retired first"
            );
            self.task_read_waiters.remove(waiter.task, work);
        }
    }
    pub(in crate::control_runtime::participation_host) fn advance_sealed_task_reads(
        &mut self,
        budget: usize,
    ) -> Result<bool, ParticipationRequestError> {
        let mut progressed = false;
        for _ in 0..budget {
            let Some(work) = self.task_read_waiters.next_ready() else {
                break;
            };
            if !self.advance_deferred_task_read(work)? {
                break;
            }
            progressed = true;
        }
        Ok(progressed && self.task_read_waiters.next_ready().is_some())
    }
    pub(super) fn advance_deferred_task_read(
        &mut self,
        work: ScopedWorkRef,
    ) -> Result<bool, ParticipationRequestError> {
        let Some(original) = self.resources.requests.get_mut(&work) else {
            return Ok(false);
        };
        let Some(waiter) = original.deferred_task_read.as_ref() else {
            return Ok(false);
        };
        let task = waiter.task;
        if waiter.inactive {
            return Ok(false);
        }
        if let Some(input) = waiter.resume {
            match self
                .resources
                .internal
                .finish_continuation(input)
                .map_err(ParticipationRequestError::ActorTurn)?
            {
                Consumption::Pending => return Ok(false),
                Consumption::Consumed | Consumption::AbandonedByOwnerFailure => {}
            }
            let waiter = original.deferred_task_read.as_mut().unwrap();
            waiter.resume = None;
            waiter.inactive = true;
            self.task_read_waiters.suppress(task, work);
            original.endpoint.external().notify();
            return Ok(true);
        }
        let failed = self
            .control_runtime
            .snapshot(self.actor)
            .map_err(ParticipationRequestError::ActorTurn)?
            .failed;
        if original.reply_completed || original.endpoint.external_ended() || failed {
            // An ended external wait never authorizes the business read.
            // Core still owns its original timeout/failure settlement; the R
            // retires only through that existing receipt and actual exits.
            original.deferred_task_read.as_mut().unwrap().inactive = true;
            self.task_read_waiters.suppress(task, work);
            return Ok(true);
        }
        let Some(proof) = self.task_read_waiters.ready.get(&task).cloned() else {
            return Ok(false);
        };
        if proof.status.identity() != task || original.expected_task != Some(task) {
            return Err(ParticipationRequestError::WrongRequest);
        }
        // The actual Await input must have been consumed. A faster seal
        // cannot release a request which Core has not yet registered.
        if !original.observe_core()? {
            return Ok(false);
        }
        let Some(input) = self
            .resources
            .internal
            .try_submit_task_read_resume(original.request().token, task)
            .map_err(ParticipationRequestError::ActorTurn)?
        else {
            return Ok(false);
        };
        original.retired_task_read = Some(proof);
        original.deferred_task_read.as_mut().unwrap().resume = Some(input);
        Ok(true)
    }
}

impl ParticipationRequestCustody {
    pub(super) fn is_refused_deferred_task_read(
        &self,
        error: ActorTurnError,
        message: &WorkerMessage,
    ) -> bool {
        let Some(task_identity) = self.expected_task else {
            return false;
        };
        let Some(original) = self.request else {
            return false;
        };
        matches!(
            error,
            ActorTurnError::ActorFailed
                | ActorTurnError::Closed
                | ActorTurnError::FailurePublicationRequired
        ) && !self.submitted
            && self.started
            && self.task_route.is_none()
            && matches!(message, WorkerMessage::Participation(ParticipationMessage::Command {
                work, request: Some(metadata), command: ParticipationInput::Request(
                    ParticipationCommand::AwaitTaskReadSeal { task, .. }),
            }) if *work == self.work && *task == task_identity
                && metadata.operation == original.operation && metadata.deadline == original.deadline)
    }
}
