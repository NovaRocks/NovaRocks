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

//! Move the original Task domain input through admission and into dispatch.

use super::{CreateGrantProgress, EdgeControlEffect, OperationTarget, QueryTaskExecution};
use crate::{
    dispatch::LocalQueueCapacity,
    effect::{AttemptEffect, AttemptEffectId, EffectBody, QueuedInputPayload},
    error::TaskExecutionError,
    input::{QueuedInputGrantRef, UpdateAdmission},
    intent::TaskOperationQueueRequest,
    remote_task::RemoteTask,
};
use novarocks_execution_contract::{TaskIdentity, TaskOperationId};
use std::ops::Bound::{Excluded, Unbounded};

#[expect(
    clippy::large_enum_variant,
    reason = "a charged edge step moves its queued-input request directly into the actor output"
)]
pub(crate) enum EdgeAdmissionProgress {
    Blocked,
    Discarded,
    Admitting(AttemptEffect),
}

impl QueryTaskExecution {
    pub(crate) fn edge_producer_after(&self, after: Option<TaskIdentity>) -> Option<TaskIdentity> {
        match after {
            Some(after) => self
                .edge_control_effects
                .range((Excluded(after), Unbounded))
                .next(),
            None => self.edge_control_effects.first_key_value(),
        }
        .map(|(&identity, _)| identity)
    }

    pub(crate) fn advance_edge_admission(
        &mut self,
        producer: TaskIdentity,
        action: AttemptEffectId,
    ) -> Result<EdgeAdmissionProgress, TaskExecutionError> {
        let effect = *self.edge_control_effects[&producer]
            .front()
            .expect("original edge row");
        let task = self
            .task_by_identity_ref(producer)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        if self.terminal_cleanup_started
            || (self.normal_drain_started && matches!(effect, EdgeControlEffect::Open { .. }))
            || task.fenced_out()
            || self.task_actual_stopped(producer)
        {
            self.pop_edge_input(producer);
            return Ok(EdgeAdmissionProgress::Discarded);
        }
        if task.input_preparation_pending()
            || (task.is_terminal() && self.covered_observation_active)
        {
            return Ok(EdgeAdmissionProgress::Blocked);
        }
        self.apply_prior_withdrawal(producer)?;
        let task = self
            .task_by_identity_ref(producer)
            .expect("frozen producer");
        let input = match effect {
            EdgeControlEffect::Close {
                edge, destination, ..
            } => task.prepare_destination_close(edge, destination)?,
            EdgeControlEffect::Open { edge, .. } => task.prepare_edge_open(edge)?,
        };
        self.dispatcher
            .validate_queue_request(TaskOperationQueueRequest::task_update(
                producer.backend_process_id(),
                &input,
            ))?;
        let stage = self
            .stages
            .get_mut(&producer.stage_id())
            .expect("frozen producer stage");
        let (_, intent) = stage
            .task_mut_observed(producer.task_id(), &self.stage_demand)
            .expect("frozen producer")
            .begin_input_preparation(input, &mut self.ids)?;
        self.pop_edge_input(producer);
        let Some(intent) = intent else {
            return Ok(EdgeAdmissionProgress::Discarded);
        };
        self.begin_initial_update_admission(intent, action, QueuedInputPayload::AlreadyRetained)
            .map(EdgeAdmissionProgress::Admitting)
    }

    fn pop_edge_input(&mut self, producer: TaskIdentity) {
        let queue = self
            .edge_control_effects
            .get_mut(&producer)
            .expect("original producer edge queue");
        queue.pop_front().expect("original edge input");
        if queue.is_empty() {
            self.edge_control_effects.remove(&producer);
        }
    }

    /// The first grant moves to the original pending domain position. A replay
    /// asks for a new grant after the immutable update has already been released.
    pub(crate) fn advance_update_send(
        &mut self,
        identity: TaskIdentity,
        action: AttemptEffectId,
    ) -> Result<(bool, Option<AttemptEffect>), TaskExecutionError> {
        if self.terminal_cleanup_started {
            return Ok((false, None));
        }
        self.apply_prior_withdrawal(identity)?;
        let Some(candidate) = self
            .task_by_identity_ref(identity)
            .and_then(RemoteTask::update_candidate)
        else {
            return Ok((false, None));
        };
        if self.dispatcher.local_capacity(candidate.request()) != LocalQueueCapacity::Fits {
            return Ok((false, None));
        }
        let (intent, grant) = self
            .task_by_identity(identity)
            .expect("frozen Task")
            .next_update_intent()?
            .expect("same original candidate");
        let operation = intent.operation_id();
        if !self.task_operation_allowed(&intent)? {
            let target = OperationTarget::Task {
                stage: identity.stage_id(),
                task: identity.task_id(),
            };
            self.rollback_unsent(target, operation);
            if let Some(grant) = grant {
                self.pending_update_grants.remove(&grant.generation());
                self.note_split_admission(operation, Ok(UpdateAdmission::DiscardedNoLongerNeeded));
                return Ok((
                    false,
                    Some(AttemptEffect {
                        id: action,
                        class: crate::effect::EffectClass::Cleanup,
                        body: EffectBody::ReleaseQueuedInput { grant },
                    }),
                ));
            }
            return Ok((false, None));
        }
        let Some(grant) = grant else {
            return self
                .begin_lifecycle_admission(intent, action)
                .map(|effect| (false, Some(effect)));
        };
        self.dispatcher.enqueue_reserved(intent, self.now, grant)?;
        self.pending_update_grants.remove(&grant.generation());
        self.operation_targets.insert(
            operation,
            OperationTarget::Task {
                stage: identity.stage_id(),
                task: identity.task_id(),
            },
        );
        Ok((true, None))
    }

    pub(super) fn accept_initial_update_grant(
        &mut self,
        identity: TaskIdentity,
        operation: TaskOperationId,
        grant: QueuedInputGrantRef,
    ) -> Result<CreateGrantProgress, TaskExecutionError> {
        let result = self.complete_task_input_preparation(identity, operation, grant);
        self.mark_task_input_cleanup(identity);
        let split = self.note_split_admission(operation, result.as_ref().copied());
        if split && matches!(result, Err(TaskExecutionError::DomainRegression { .. })) {
            // The split producer receives its original typed local rejection
            // and owns the resulting PumpFailed, after its own processing.
            return Ok(CreateGrantProgress::Waiting);
        }
        if result? == UpdateAdmission::Queued {
            self.pending_update_grants
                .insert(grant.generation(), (grant, operation));
        }
        Ok(CreateGrantProgress::Waiting)
    }

    pub(crate) fn tracks_pending_update_grant(&self, grant: QueuedInputGrantRef) -> bool {
        self.pending_update_grants
            .get(&grant.generation())
            .is_some_and(|(held, _)| *held == grant)
    }

    pub(super) fn mark_task_input_cleanup(&mut self, identity: TaskIdentity) {
        if self
            .task_by_identity_ref(identity)
            .is_some_and(RemoteTask::has_input_cleanup)
        {
            self.task_input_cleanup.insert(identity);
        }
    }

    pub(crate) fn has_task_input_cleanup(&self) -> bool {
        !self.task_input_cleanup.is_empty()
    }

    pub(crate) fn advance_task_input_cleanup(&mut self) -> Option<EffectBody> {
        let identity = self.task_input_cleanup.pop_first()?;
        let mut task = self.task_by_identity(identity).expect("original Task");
        let normal = task.normally_stood_down()
            || task.status().is_some_and(
                novarocks_execution_contract::TaskStatus::is_success_compatible_terminal,
            );
        let preparation = task.take_withdrawn_preparation();
        let body = if preparation.is_some() {
            None
        } else if let Some(grant) = task.take_released_grant() {
            Some(EffectBody::ReleaseQueuedInput { grant })
        } else {
            task.take_released_payload()
                .map(|payload| EffectBody::ReleaseCreationPayload {
                    task: identity,
                    payload,
                })
        };
        drop(task);
        self.mark_task_input_cleanup(identity);
        if let Some(operation) = preparation {
            return self.withdraw_lifecycle_admission(operation);
        }
        if let Some(EffectBody::ReleaseQueuedInput { grant }) = body
            && let Some((_, operation)) = self.pending_update_grants.remove(&grant.generation())
        {
            if normal {
                self.note_split_admission(operation, Ok(UpdateAdmission::DiscardedNoLongerNeeded));
            } else {
                self.discard_unsent_split(
                    operation,
                    crate::split_delivery::SplitDeliverySettlement::Unknown(
                        "split assignment stopped before Native dispatch".to_owned(),
                    ),
                );
            }
        }
        body
    }
}
