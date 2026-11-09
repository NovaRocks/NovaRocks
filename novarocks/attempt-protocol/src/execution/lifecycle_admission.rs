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

//! One fresh original queued-input admission per lifecycle send generation.

use super::{CreateGrantProgress, OperationTarget, QueryTaskExecution};
use crate::{
    dispatch::DispatchOperationState,
    effect::{AttemptEffect, EffectBody, EffectClass, OperationIntent, QueuedInputPayload},
    error::TaskExecutionError,
    input::{OperationObservation, OperationReply, QueuedInputGrantRef, SendGeneration},
    operation::OperationDispatchResult,
    participation_owner::ParticipationEstablishFacts,
    start::FrozenEstablish,
};
use novarocks_coordination_contract::AttemptActionId;
use novarocks_execution_contract::{OperationKind, ParticipationRef, TaskOperationId};
use std::ops::Bound::{Excluded, Unbounded};

#[derive(Debug)]
pub(super) struct LifecycleAdmission {
    intent: OperationIntent,
    participation: ParticipationRef,
    generation: SendGeneration,
    grant: Option<QueuedInputGrantRef>,
    withdrawing: bool,
}

/// Passive identity for the original operation between physical sends.
#[derive(Debug)]
pub(super) struct UnknownLifecycleSend {
    participation: ParticipationRef,
    kind: OperationKind,
    generation: SendGeneration,
}

impl QueryTaskExecution {
    pub(crate) fn participation_after(
        &self,
        after: Option<ParticipationRef>,
    ) -> Option<ParticipationRef> {
        match after {
            Some(after) => self
                .owners
                .range((Excluded(after), Unbounded))
                .next()
                .map(|(&key, _)| key),
            None => self.owners.first_key_value().map(|(&key, _)| key),
        }
    }

    pub(crate) fn next_authorization_intent(
        &mut self,
        participation: ParticipationRef,
        material: FrozenEstablish,
    ) -> Result<Option<OperationIntent>, TaskExecutionError> {
        if self.normal_drain_started || self.terminal_cleanup_started {
            return Ok(None);
        }
        let owner = self
            .owners
            .get_mut(&participation)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        if let Some(admission) = owner.admission_intent(self.now, &mut self.ids)? {
            return Ok(Some(admission));
        }
        if owner.needs_establish() {
            return owner.establish_intent(
                ParticipationEstablishFacts {
                    material: material.material,
                    queued_bytes: material.queued_bytes,
                },
                self.now,
                &mut self.ids,
            );
        }
        Ok(None)
    }

    pub(crate) fn begin_lifecycle_admission(
        &mut self,
        intent: OperationIntent,
        effect: AttemptActionId,
    ) -> Result<AttemptEffect, TaskExecutionError> {
        let participation = match &intent {
            OperationIntent::AcquireParticipationAdmissionTicket(request) => {
                request.participation()
            }
            OperationIntent::EstablishParticipation { participation, .. } => *participation,
            _ => return Err(TaskExecutionError::UnknownOperation),
        };
        self.dispatcher.validate_operation_carrier(&intent)?;
        let operation = intent.operation_id();
        if self.lifecycle_admissions.contains_key(&operation)
            || self.operation_targets.contains_key(&operation)
        {
            return Err(TaskExecutionError::UnknownOperation);
        }
        let generation = SendGeneration::new(effect);
        let lane = intent.lane();
        self.lifecycle_admissions.insert(
            operation,
            LifecycleAdmission {
                intent: intent.clone(),
                participation,
                generation,
                grant: None,
                withdrawing: false,
            },
        );
        Ok(AttemptEffect {
            id: effect,
            class: EffectClass::Business,
            body: EffectBody::AdmitQueuedInput {
                operation: intent,
                generation,
                payload: QueuedInputPayload::AlreadyRetained,
                lane,
            },
        })
    }

    pub(crate) fn has_lifecycle_admission(&self, operation: TaskOperationId) -> bool {
        self.lifecycle_admissions.contains_key(&operation)
    }

    pub(crate) fn observe_lifecycle_grant(
        &mut self,
        operation: TaskOperationId,
        grant: QueuedInputGrantRef,
    ) -> Result<CreateGrantProgress, TaskExecutionError> {
        if self.dispatcher.tracks_input(operation, grant) {
            return Ok(CreateGrantProgress::Waiting);
        }
        let Some(row) = self.lifecycle_admissions.get_mut(&operation) else {
            return Ok(CreateGrantProgress::Release(grant));
        };
        if row.generation != grant.generation() {
            return Ok(CreateGrantProgress::Release(grant));
        }
        if row.withdrawing || self.terminal_cleanup_started {
            self.finish_lifecycle_admission(operation);
            return Ok(CreateGrantProgress::Release(grant));
        }
        if let Some(original) = row.grant {
            return Ok(if original == grant {
                CreateGrantProgress::Waiting
            } else {
                CreateGrantProgress::Release(grant)
            });
        }
        row.grant = Some(grant);
        self.lifecycle_granted.insert(operation);
        self.advance_lifecycle_grant(operation)
    }

    pub(crate) fn next_lifecycle_grant(
        &self,
        after: Option<TaskOperationId>,
    ) -> Option<TaskOperationId> {
        match after {
            Some(after) => self
                .lifecycle_granted
                .range((Excluded(after), Unbounded))
                .next()
                .copied(),
            None => self.lifecycle_granted.first().copied(),
        }
    }

    pub(crate) fn advance_lifecycle_grant(
        &mut self,
        operation: TaskOperationId,
    ) -> Result<CreateGrantProgress, TaskExecutionError> {
        let row = self
            .lifecycle_admissions
            .get(&operation)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        let grant = row.grant.ok_or(TaskExecutionError::UnknownOperation)?;
        if row.withdrawing || self.terminal_cleanup_started {
            self.finish_lifecycle_admission(operation);
            return Ok(CreateGrantProgress::Release(grant));
        }
        if self.dispatcher.local_capacity(row.intent.queue_request())
            != crate::dispatch::LocalQueueCapacity::Fits
        {
            return Ok(CreateGrantProgress::Waiting);
        }
        let participation = row.participation;
        self.dispatcher
            .enqueue_reserved(row.intent.clone(), self.now, grant)?;
        self.lifecycle_admissions.remove(&operation);
        self.lifecycle_granted.remove(&operation);
        self.operation_targets
            .insert(operation, OperationTarget::Participation(participation));
        Ok(CreateGrantProgress::Queued)
    }

    pub(crate) fn close_lifecycle_admission(
        &mut self,
        operation: TaskOperationId,
        generation: SendGeneration,
    ) -> bool {
        if !self
            .lifecycle_admissions
            .get(&operation)
            .is_some_and(|row| row.generation == generation && row.grant.is_none())
        {
            return false;
        }
        self.finish_lifecycle_admission(operation);
        true
    }

    fn finish_lifecycle_admission(&mut self, operation: TaskOperationId) {
        if let Some(row) = self.lifecycle_admissions.remove(&operation) {
            self.lifecycle_granted.remove(&operation);
            self.rollback_unsent(OperationTarget::Participation(row.participation), operation);
        }
    }

    pub(super) fn record_lifecycle_result(
        &mut self,
        participation: ParticipationRef,
        reply: &OperationReply,
        generation: SendGeneration,
        applied: bool,
    ) {
        let operation = reply.operation_id();
        let kind = reply.kind();
        if applied
            && reply.dispatch_result() == OperationDispatchResult::TransportUnknown
            && self.owners[&participation].tracks_lifecycle_request(kind, operation)
        {
            self.lifecycle_unknown.insert(
                operation,
                UnknownLifecycleSend {
                    participation,
                    kind,
                    generation,
                },
            );
        } else {
            self.lifecycle_unknown.remove(&operation);
        }
    }

    pub(crate) fn prior_lifecycle_receipt(
        &self,
        observation: &OperationObservation,
    ) -> Option<ParticipationRef> {
        let reply = &observation.reply;
        let operation = reply.operation_id();
        let row = self.lifecycle_unknown.get(&operation)?;
        (row.kind == reply.kind()
            && observation.generation <= row.generation
            && matches!(
                reply.dispatch_result(),
                OperationDispatchResult::WorkerReceipt(_)
            )
            && self.dispatcher.operation_state(operation).ok()? == DispatchOperationState::Absent
            && self.owners[&row.participation].tracks_lifecycle_request(row.kind, operation))
        .then_some(row.participation)
    }

    pub(super) fn settle_prior_lifecycle_receipt(
        &mut self,
        participation: ParticipationRef,
        reply: &OperationReply,
    ) -> (Option<EffectBody>, Result<(), TaskExecutionError>) {
        let operation = reply.operation_id();
        self.lifecycle_unknown.remove(&operation);
        let withdraw = self.withdraw_lifecycle_admission(operation);
        let resumed = self
            .owners
            .get_mut(&participation)
            .expect("original participation")
            .resume_lifecycle_receipt(reply.kind(), operation);
        debug_assert!(resumed);
        (
            withdraw,
            self.acknowledge_participation(participation, reply),
        )
    }

    pub(crate) fn next_stop_lifecycle_admission(
        &mut self,
        after: Option<TaskOperationId>,
    ) -> Option<(TaskOperationId, Option<EffectBody>)> {
        let (&operation, _) = match after {
            Some(after) => self
                .lifecycle_admissions
                .range((Excluded(after), Unbounded))
                .next(),
            None => self.lifecycle_admissions.first_key_value(),
        }?;
        Some((operation, self.withdraw_lifecycle_admission(operation)))
    }

    fn withdraw_lifecycle_admission(&mut self, operation: TaskOperationId) -> Option<EffectBody> {
        let row = self.lifecycle_admissions.get_mut(&operation)?;
        if let Some(grant) = row.grant {
            self.finish_lifecycle_admission(operation);
            Some(EffectBody::ReleaseQueuedInput { grant })
        } else if row.withdrawing {
            None
        } else {
            row.withdrawing = true;
            Some(EffectBody::Withdraw {
                effect: row.generation.admission_effect(),
            })
        }
    }
}
