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

//! The logical result and the physical generation's credit settle separately.

use super::{OperationTarget, QueryTaskExecution};
use crate::{
    dispatch::{EmittedMember, PriorReplyRoute},
    effect::{AttemptEffectId, EffectBody},
    error::TaskExecutionError,
    input::{AckPayload, OperationObservation, OperationReply, WithdrawalOutcome},
    operation::OperationDispatchResult,
};
use novarocks_execution_contract::{
    OperationKind, OperationOutcome, OperationShape, ParticipationRef, ReleaseOutcome,
};

#[derive(Clone, Copy, Debug)]
pub(crate) enum UnsentOwner {
    /// Pass to the original actor Abort owner.
    ActorAbort,
    /// Close this generation's actual authorization leg. The authorization
    /// owner still holds the history of any prior unknown send.
    Establish(ParticipationRef),
    Local,
}

fn prior_result_resolves(route: PriorReplyRoute, reply: &OperationReply) -> bool {
    if !matches!(
        reply.dispatch_result(),
        OperationDispatchResult::WorkerReceipt(_)
    ) {
        return false;
    }
    match route {
        PriorReplyRoute::AlreadySettled => false,
        PriorReplyRoute::Queued(_) => true,
        // The handoff may already have started even if its progress fact is
        // still on its way. A retryable earlier refusal cannot mask a later
        // successful dispatch. The current physical generation decides.
        PriorReplyRoute::Emitted => match reply.kind() {
            OperationKind::CreateTask => !matches!(
                reply.worker_outcome(),
                Some(OperationOutcome::NotReady | OperationOutcome::PreparationBusy)
            ),
            OperationKind::ReleaseParticipation => {
                reply.worker_outcome() != Some(OperationOutcome::ReleaseNotReady)
                    && !(reply.is_applied()
                        && matches!(
                            reply.payload(),
                            AckPayload::Release {
                                outcome: ReleaseOutcome::NotReady,
                                ..
                            }
                        ))
            }
            _ => true,
        },
    }
}

impl QueryTaskExecution {
    /// Filter an older generation-only result or a newer result for an already-resolved
    /// logical operation before recording the original QA observer debt.
    pub(crate) fn dispatched_reply_needs_observer(
        &self,
        observation: &OperationObservation,
    ) -> bool {
        let reply = &observation.reply;
        match self.dispatcher.prior_reply_route(
            reply.operation_id(),
            reply.kind(),
            observation.generation,
        ) {
            Some(route) => prior_result_resolves(route, reply),
            None => self.dispatcher.reply_has_pending_owner(
                reply.operation_id(),
                reply.kind(),
                observation.generation,
            ),
        }
    }

    /// The Source validates correlation against the original physical record.
    /// A stale result must never consume a newer send's dispatcher credit.
    /// If its verdict closes the logical operation, retain any independent
    /// withdrawal action even when applying the verdict fails the attempt.
    pub(crate) fn apply_dispatched_reply(
        &mut self,
        observation: &OperationObservation,
    ) -> (Option<EffectBody>, Result<(), TaskExecutionError>) {
        if let Some(result) = self.observe_prior_create_reply_during_admission(observation) {
            return result;
        }
        if let Some(result) = self.observe_create_reply_between_sends(observation) {
            return (None, result);
        }
        let reply = &observation.reply;
        let operation = reply.operation_id();
        let generation = observation.generation;
        let Some(route) = self
            .dispatcher
            .prior_reply_route(operation, reply.kind(), generation)
        else {
            return (
                None,
                self.acknowledge_current_send(generation, reply).map(|_| ()),
            );
        };
        if !prior_result_resolves(route, reply) {
            return (None, Ok(()));
        }
        let Some(target) = self.operation_targets.get(&operation).copied() else {
            return (None, Err(TaskExecutionError::UnknownOperation));
        };
        let action = match route {
            PriorReplyRoute::Queued(grant) => {
                if let Err(error) = self.dispatcher.remove_queued_replay(operation, grant) {
                    return (None, Err(error));
                }
                Some(EffectBody::ReleaseQueuedInput { grant })
            }
            PriorReplyRoute::Emitted => self
                .dispatcher
                .settle_emitted_logically(operation)
                .map(|effect| EffectBody::Withdraw { effect }),
            PriorReplyRoute::AlreadySettled => unreachable!("ignored settled logical operation"),
        };
        self.operation_targets.remove(&operation);
        if reply.kind() == OperationKind::CreateTask {
            self.record_create_result(reply, generation);
        }
        (action, self.apply_operation_reply(target, reply))
    }

    pub(crate) fn next_stop_send_withdrawal(
        &mut self,
        after: Option<AttemptEffectId>,
    ) -> Option<(AttemptEffectId, bool)> {
        self.dispatcher.next_stop_withdrawal(after)
    }

    pub(crate) fn dispatched_participation(
        &self,
        operation: novarocks_execution_contract::TaskOperationId,
    ) -> Option<ParticipationRef> {
        match self.operation_targets.get(&operation) {
            Some(OperationTarget::Participation(participation)) => Some(*participation),
            _ => None,
        }
    }

    pub(crate) fn advance_send_batch(
        &mut self,
        pass: &mut crate::dispatch::BatchDispatchPass,
        effect: AttemptEffectId,
        context: &mut dyn crate::capacity::StepContext,
        validate_send: impl FnMut(&crate::effect::OperationIntent) -> Result<(), TaskExecutionError>,
    ) -> Result<crate::dispatch::BatchDispatch, TaskExecutionError> {
        pass.advance(&mut self.dispatcher, effect, context, validate_send)
    }

    pub(crate) fn observe_send_started(&mut self, effect: AttemptEffectId) {
        self.dispatcher.observe_send_started(effect);
    }

    /// The original bounded batch inventory is reused when applying its
    /// result. The caller charges one complete primary input before taking
    /// the inventory and folds every member before accepting another input.
    pub(crate) fn begin_send_withdrawal_result(
        &mut self,
        effect: AttemptEffectId,
        outcome: WithdrawalOutcome,
    ) -> Result<Option<Box<[EmittedMember]>>, TaskExecutionError> {
        self.dispatcher
            .take_definitely_unsent_members(effect, outcome)
    }

    /// Unused generation proof releases its own credit and local markers.
    /// There is no implied WC release, remote settlement or actual exit here.
    pub(crate) fn settle_unsent_member(
        &mut self,
        member: EmittedMember,
    ) -> Result<UnsentOwner, TaskExecutionError> {
        let pending = self
            .dispatcher
            .retire_generation(
                member.operation,
                member.generation,
                Some(member.shape.kind()),
            )?
            .ok_or(TaskExecutionError::UnknownOperation)?;
        if !pending {
            return Ok(UnsentOwner::Local);
        }
        let target = self
            .operation_targets
            .remove(&member.operation)
            .ok_or(TaskExecutionError::UnknownOperation)?;
        self.rollback_unsent(target, member.operation);
        if let OperationTarget::Task { stage, task } = target {
            self.settle_task_drain(stage, task);
        }
        Ok(match (target, member.shape) {
            (OperationTarget::ActorAbort, _) => UnsentOwner::ActorAbort,
            (
                OperationTarget::Participation(participation),
                OperationShape::EstablishParticipation,
            ) => UnsentOwner::Establish(participation),
            _ => UnsentOwner::Local,
        })
    }
}
