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
    dispatch::LocalQueueCapacity,
    effect::{AttemptEffect, AttemptEffectId, EffectBody, EffectClass},
    error::TaskExecutionError,
    input::{OperationObservation, QueuedInputGrantRef, SendGeneration},
    operation::OperationDispatchResult,
};
use novarocks_execution_contract::{OperationKind, TaskIdentity, TaskOperationId};

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
        if self.create_admissions.contains_key(&operation) || !self.create_window_available(task) {
            return Ok(None);
        }
        self.dispatcher
            .validate_queue_request(candidate.request())?;
        self.dispatcher
            .validate_plan_carrier_bytes(candidate.plan_carrier_bytes())?;
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
        self.advance_create_grant(operation)
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
        self.create_admissions.remove(&operation);
        Ok(CreateGrantProgress::Queued)
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
        if reply.dispatch_result() == OperationDispatchResult::TransportUnknown || row.prior_settled
        {
            return Some((None, Ok(())));
        }
        let task = row.task;
        row.prior_settled = true;
        let withdrawal = self.withdraw_create_admission(operation);
        let result = self.acknowledge_task_reply(task.stage_id(), task.task_id(), reply, true);
        Some((withdrawal, result))
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
