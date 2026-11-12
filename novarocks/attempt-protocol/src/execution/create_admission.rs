// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The original Create candidate waits for its own WC generation before
//! entering the dispatcher. The grant and queue position have separate lives.

use super::{OperationTarget, QueryTaskExecution};
use crate::{
    capacity::StepContext,
    dispatch::{DispatchOperationState, LocalQueueCapacity},
    effect::{AttemptEffect, AttemptEffectId, EffectBody, EffectClass},
    error::TaskExecutionError,
    input::{OperationObservation, QueuedInputGrantRef, SendGeneration},
    operation::OperationDispatchResult,
};
use novarocks_coordination_contract::MonotonicInstant;
use novarocks_execution_contract::{OperationKind, TaskIdentity, TaskOperationId};
use std::ops::Bound::{Excluded, Unbounded};

/// One pre-registered row per original Create operation. Its passive result
/// identity remains reachable after dispatcher credit retires and before a
/// later generation obtains admission. It retains no physical history.
#[derive(Debug)]
pub(super) struct CreateReceiptRoute {
    pub(super) task: TaskIdentity,
    pub(super) unknown: Option<SendGeneration>,
}

#[derive(Debug)]
pub(super) struct CreateAdmission {
    task: TaskIdentity,
    generation: SendGeneration,
    phase: Phase,
    prior_settled: bool,
}

#[derive(Clone, Copy, Debug)]
enum Phase {
    Admitting,
    Withdrawing,
    Granted(QueuedInputGrantRef),
}

#[derive(Debug)]
pub(crate) enum CreateGrantProgress {
    Waiting,
    Queued,
    /// This exact grant did not enter the dispatcher. Source still owns it.
    Release(QueuedInputGrantRef),
}

impl QueryTaskExecution {
    pub(crate) fn next_create_retry_deadline(&self) -> Option<MonotonicInstant> {
        if self.terminal_cleanup_started || self.normal_drain_started {
            return None;
        }
        self.create_retry_wakes
            .first()
            .map(|&(deadline, _)| deadline)
    }

    /// A due wake requests one candidate pass. If that pass is blocked on
    /// capacity, the original capacity Source is responsible for the next wake.
    pub(crate) fn consume_create_retry_wakes(&mut self, context: &mut dyn StepContext) -> bool {
        while self
            .next_create_retry_deadline()
            .is_some_and(|deadline| self.now.has_reached(deadline))
        {
            if !context.charge(1) {
                return false;
            }
            self.create_retry_wakes.pop_first();
        }
        true
    }

    fn create_window_available(&self, task: TaskIdentity) -> bool {
        let backend = task.backend_process_id();
        self.deployment_window
            .get(&backend)
            .is_none_or(|positions| {
                positions.contains(&task)
                    || positions.len() < self.deployment_window_limits[&backend]
            })
    }

    /// Called after charging this exact candidate and effect. The local owner
    /// remains unmodified until the original Source reports retained custody.
    pub(crate) fn begin_create_admission(
        &mut self,
        task: TaskIdentity,
        effect: AttemptEffectId,
    ) -> Result<Option<AttemptEffect>, TaskExecutionError> {
        let Some(candidate) = self.create_candidate_for_task(task)? else {
            return Ok(None);
        };
        let operation = candidate.operation_id();
        if self.create_admissions.contains_key(&operation)
            || self.dispatcher.operation_state(operation)? != DispatchOperationState::Absent
            || !self.create_window_available(task)
        {
            return Ok(None);
        }
        self.dispatcher
            .validate_queue_request(candidate.request())?;
        if self.dispatcher.local_capacity(candidate.request()) != LocalQueueCapacity::Fits {
            return Ok(None);
        }
        let (intent, payload) = self
            .task_by_identity_ref(task)
            .and_then(crate::remote_task::RemoteTask::create_admission_intent)
            .expect("same original sendable Create");
        let generation = SendGeneration::new(effect);
        self.create_admissions.insert(
            operation,
            CreateAdmission {
                task,
                generation,
                phase: Phase::Admitting,
                prior_settled: false,
            },
        );
        Ok(Some(AttemptEffect {
            id: effect,
            class: EffectClass::Business,
            body: EffectBody::AdmitQueuedInput {
                operation: intent,
                generation,
                payload,
                lane: candidate.request().lane(),
            },
        }))
    }

    /// An installed Source grant for this exact Create guarantees the original
    /// retained payload. This input cannot spend any physical window credit.
    pub(crate) fn observe_create_grant(
        &mut self,
        operation: TaskOperationId,
        grant: QueuedInputGrantRef,
    ) -> Result<CreateGrantProgress, TaskExecutionError> {
        if self.dispatcher.tracks_input(operation, grant) {
            return Ok(CreateGrantProgress::Waiting);
        }
        let Some(row) = self.create_admissions.get_mut(&operation) else {
            return Ok(CreateGrantProgress::Release(grant));
        };
        if row.generation != grant.generation() {
            return Ok(CreateGrantProgress::Release(grant));
        }
        let withdrawing = match row.phase {
            Phase::Withdrawing => true,
            Phase::Granted(original) if original == grant => {
                return Ok(CreateGrantProgress::Waiting);
            }
            Phase::Granted(_) => return Ok(CreateGrantProgress::Release(grant)),
            Phase::Admitting => {
                row.phase = Phase::Granted(grant);
                false
            }
        };
        let task = row.task;
        self.task_by_identity(task)
            .ok_or(TaskExecutionError::UnknownOperation)?
            .note_creation_retained();
        if withdrawing {
            self.create_admissions.remove(&operation);
            return Ok(CreateGrantProgress::Release(grant));
        }
        self.create_granted.insert(operation);
        self.advance_create_grant(operation)
    }

    pub(crate) fn next_create_grant(
        &self,
        after: Option<TaskOperationId>,
    ) -> Option<TaskOperationId> {
        match after {
            Some(after) => self
                .create_granted
                .range((Excluded(after), Unbounded))
                .next()
                .copied(),
            None => self.create_granted.first().copied(),
        }
    }

    /// Called after charging this exact row. A full local queue or deployment
    /// window holds the same grant until progress. Queue residence starts at
    /// actual enqueue, after external WC admission and the local gate.
    pub(crate) fn advance_create_grant(
        &mut self,
        operation: TaskOperationId,
    ) -> Result<CreateGrantProgress, TaskExecutionError> {
        let Some(row) = self.create_admissions.get(&operation) else {
            return Ok(CreateGrantProgress::Waiting);
        };
        let Phase::Granted(grant) = row.phase else {
            return Ok(CreateGrantProgress::Waiting);
        };
        let task = row.task;
        let Some(candidate) = self.create_candidate_for_task(task)? else {
            self.create_granted.remove(&operation);
            self.create_admissions.remove(&operation);
            return Ok(CreateGrantProgress::Release(grant));
        };
        if !self.create_window_available(task)
            || self.dispatcher.local_capacity(candidate.request()) != LocalQueueCapacity::Fits
        {
            return Ok(CreateGrantProgress::Waiting);
        }
        let intent = self.release_create_for_task(task)?;
        self.dispatcher.enqueue_reserved(intent, self.now, grant)?;
        self.operation_targets.insert(
            operation,
            OperationTarget::Task {
                stage: task.stage_id(),
                task: task.task_id(),
            },
        );
        self.deployment_window
            .entry(task.backend_process_id())
            .or_default()
            .insert(task);
        self.create_granted.remove(&operation);
        self.create_admissions.remove(&operation);
        Ok(CreateGrantProgress::Queued)
    }

    pub(super) fn record_create_result(
        &mut self,
        reply: &crate::input::OperationReply,
        generation: SendGeneration,
    ) {
        let Some(row) = self.create_receipts.get(&reply.operation_id()) else {
            return;
        };
        let still_unknown = reply.dispatch_result() == OperationDispatchResult::TransportUnknown
            && self
                .task_by_identity_ref(row.task)
                .is_some_and(crate::remote_task::RemoteTask::create_result_is_unresolved);
        self.create_receipts
            .get_mut(&reply.operation_id())
            .expect("pre-registered Create")
            .unknown = still_unknown.then_some(generation);
    }

    pub(super) fn observe_create_reply_between_sends(
        &mut self,
        observation: &OperationObservation,
    ) -> Option<Result<(), TaskExecutionError>> {
        let reply = &observation.reply;
        let operation = reply.operation_id();
        if reply.kind() != OperationKind::CreateTask
            || self.dispatcher.operation_state(operation).ok()? != DispatchOperationState::Absent
        {
            return None;
        }
        let row = self.create_receipts.get_mut(&operation)?;
        if observation.generation > row.unknown? {
            return None;
        }
        if !matches!(
            reply.dispatch_result(),
            OperationDispatchResult::WorkerReceipt(_)
        ) {
            return Some(Ok(()));
        }
        row.unknown = None;
        let task = row.task;
        Some(self.acknowledge_task_reply(task.stage_id(), task.task_id(), reply, true))
    }

    /// The Source only forwards exact operation/generation correlations from
    /// its original records. This handler covers a replay still awaiting a
    /// grant or dispatcher placement. A late definitive reply closes that
    /// admission even when the original domain verdict reports a failure.
    pub(crate) fn observe_prior_create_reply_during_admission(
        &mut self,
        observation: &OperationObservation,
    ) -> Option<(Option<EffectBody>, Result<(), TaskExecutionError>)> {
        let reply = &observation.reply;
        let operation = reply.operation_id();
        let row = self.create_admissions.get_mut(&operation)?;
        if reply.kind() != OperationKind::CreateTask || observation.generation >= row.generation {
            return None;
        }
        if row.prior_settled {
            return Some((None, Ok(())));
        }
        if observation.generation > self.create_receipts.get(&operation)?.unknown? {
            return None;
        }
        if !matches!(
            reply.dispatch_result(),
            OperationDispatchResult::WorkerReceipt(_)
        ) {
            return Some((None, Ok(())));
        }
        let task = row.task;
        row.prior_settled = true;
        let withdrawal = self.withdraw_create_admission(operation);
        self.record_create_result(reply, observation.generation);
        let result = self.acknowledge_task_reply(task.stage_id(), task.task_id(), reply, true);
        Some((withdrawal, result))
    }

    pub(crate) fn next_stop_create_admission(
        &mut self,
        after: Option<TaskOperationId>,
    ) -> Option<(TaskOperationId, Option<EffectBody>)> {
        let (&operation, _) = match after {
            Some(after) => self
                .create_admissions
                .range((Excluded(after), Unbounded))
                .next(),
            None => self.create_admissions.first_key_value(),
        }?;
        Some((operation, self.withdraw_create_admission(operation)))
    }

    /// Withdraw only this generation. The Source must still report either the
    /// original admitted grant or a definite unsent result.
    pub(crate) fn withdraw_create_admission(
        &mut self,
        operation: TaskOperationId,
    ) -> Option<EffectBody> {
        let row = self.create_admissions.get_mut(&operation)?;
        match row.phase {
            Phase::Admitting => {
                row.phase = Phase::Withdrawing;
                Some(EffectBody::Withdraw {
                    effect: row.generation.admission_effect(),
                })
            }
            Phase::Granted(grant) => {
                self.create_granted.remove(&operation);
                self.create_admissions.remove(&operation);
                Some(EffectBody::ReleaseQueuedInput { grant })
            }
            Phase::Withdrawing => None,
        }
    }

    pub(crate) fn close_create_admission(
        &mut self,
        operation: TaskOperationId,
        generation: SendGeneration,
    ) -> bool {
        if self.create_admissions.get(&operation).is_none_or(|row| {
            row.generation != generation || matches!(row.phase, Phase::Granted(_))
        }) {
            return false;
        }
        self.create_admissions.remove(&operation);
        true
    }
}
