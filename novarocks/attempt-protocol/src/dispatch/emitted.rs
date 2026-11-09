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

//! Exact membership and outcome arbitration for an emitted Native batch.

use super::OperationDispatcher;
use crate::{
    effect::{AttemptEffectId, DispatchBatch, OperationIntent},
    error::TaskExecutionError,
    input::{QueuedInputGrantRef, SendGeneration, WithdrawalOutcome},
};
use novarocks_execution_contract::{OperationKind, OperationShape, TaskOperationId};

#[derive(Clone, Copy, Debug)]
pub(crate) struct EmittedMember {
    pub(crate) operation: TaskOperationId,
    pub(crate) generation: SendGeneration,
    pub(crate) shape: OperationShape,
}

#[derive(Debug)]
pub(super) struct EmittedBatch {
    members: Box<[EmittedMember]>,
    remaining: usize,
    started: bool,
    withdrawal_requested: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum PriorReplyRoute {
    Queued(QueuedInputGrantRef),
    Emitted,
    AlreadySettled,
}

impl OperationDispatcher {
    pub(crate) fn accept_emitted(
        &mut self,
        batch: &DispatchBatch,
        effect: AttemptEffectId,
    ) -> Result<(), TaskExecutionError> {
        if self.emitted.contains_key(&effect) {
            return Err(TaskExecutionError::Schedule(
                "dispatch effect already owns an emitted batch".into(),
            ));
        }
        self.accept_batch(batch, Some(effect))?;
        let members = batch
            .operations
            .iter()
            .zip(&batch.queued_inputs)
            .map(|(intent, grant)| EmittedMember {
                operation: intent.operation_id(),
                generation: grant.generation(),
                shape: intent.shape(),
            })
            .collect::<Box<[_]>>();
        self.emitted.insert(
            effect,
            EmittedBatch {
                remaining: members.len(),
                members,
                started: false,
                withdrawal_requested: false,
            },
        );
        Ok(())
    }

    pub(super) fn retire_emitted_member(&mut self, effect: AttemptEffectId) {
        let Some(batch) = self.emitted.get_mut(&effect) else {
            // The definitive batch withdrawal already took the exact members.
            return;
        };
        batch.remaining -= 1;
        if batch.remaining == 0 {
            self.emitted.remove(&effect);
        }
    }

    pub(crate) fn observe_send_started(&mut self, effect: AttemptEffectId) {
        if let Some(batch) = self.emitted.get_mut(&effect) {
            batch.started = true;
        }
    }

    pub(crate) fn prior_reply_route(
        &self,
        operation: TaskOperationId,
        kind: OperationKind,
        generation: SendGeneration,
    ) -> Option<PriorReplyRoute> {
        if let Some(row) = self.queued_entry(operation) {
            return (row.intent.kind() == kind && generation < row.queue_permit.generation())
                .then_some(PriorReplyRoute::Queued(row.queue_permit));
        }
        let row = self.in_flight.get(&operation)?;
        if row.kind != kind || generation >= row.input.generation() {
            return None;
        }
        Some(if row.logical_pending {
            PriorReplyRoute::Emitted
        } else {
            PriorReplyRoute::AlreadySettled
        })
    }

    pub(crate) fn reply_has_pending_owner(
        &self,
        operation: TaskOperationId,
        kind: OperationKind,
        generation: SendGeneration,
    ) -> bool {
        self.in_flight.get(&operation).is_some_and(|row| {
            row.logical_pending && row.kind == kind && row.input.generation() == generation
        })
    }

    /// The exact original owner has resolved the logical operation. Keep the
    /// emitted credit live until its own result or real withdrawal arrives.
    pub(crate) fn settle_emitted_logically(
        &mut self,
        operation: TaskOperationId,
    ) -> Option<AttemptEffectId> {
        let row = self
            .in_flight
            .get_mut(&operation)
            .expect("classified emitted replay");
        row.logical_pending = false;
        let effect = row.batch?;
        let batch = self.emitted.get_mut(&effect).expect("original batch");
        if batch.started || batch.withdrawal_requested {
            return None;
        }
        batch.withdrawal_requested = true;
        Some(effect)
    }

    pub(crate) fn remove_queued_replay(
        &mut self,
        operation: TaskOperationId,
        original: QueuedInputGrantRef,
    ) -> Result<OperationIntent, TaskExecutionError> {
        if !self
            .queued_entry(operation)
            .is_some_and(|row| row.queue_permit == original)
        {
            return Err(TaskExecutionError::UnknownOperation);
        }
        Ok(self.remove_queued(operation)?.intent)
    }

    /// Take the exact paid Source's batch outcome. Only a positive unused
    /// result authorizes processing these original members as unsent.
    pub(crate) fn take_definitely_unsent_members(
        &mut self,
        effect: AttemptEffectId,
        outcome: WithdrawalOutcome,
    ) -> Result<Option<Box<[EmittedMember]>>, TaskExecutionError> {
        if outcome != WithdrawalOutcome::DefinitelyUnsent {
            return Ok(None);
        }
        let Some(batch) = self.emitted.get(&effect) else {
            return Ok(None);
        };
        if batch.started || batch.remaining != batch.members.len() {
            return Err(TaskExecutionError::Schedule(
                "definitely-unsent batch already has a start or member reply".into(),
            ));
        }
        Ok(self.emitted.remove(&effect).map(|batch| batch.members))
    }
}
