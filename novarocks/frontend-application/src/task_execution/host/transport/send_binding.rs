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

use super::{GrantedInputs, QueueCustody, QueueWindow, SendInputLoan, SendKey};
use crate::{
    control_runtime::transport_withdrawal::AttemptEffectEnvelope,
    task_execution::{host::custody::TransportJoinRegistration, intent::OperationIntent},
};
use novarocks_attempt_protocol::effect::EffectBody;
use novarocks_execution_contract::TaskOperationId;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Holds each frozen rich request once for its logical operation. Physical
/// replays borrow the same backing by cloning the original intent handle.
#[derive(Default)]
pub(crate) struct FrozenSendRequests {
    originals: BTreeMap<TaskOperationId, OperationIntent>,
}
impl FrozenSendRequests {
    pub(crate) fn install(&mut self, original: OperationIntent) -> Result<(), OperationIntent> {
        let std::collections::btree_map::Entry::Vacant(record) =
            self.originals.entry(original.operation_id())
        else {
            return Err(original);
        };
        record.insert(original);
        Ok(())
    }

    pub(crate) fn retire(&mut self, operation: TaskOperationId) -> Option<OperationIntent> {
        self.originals.remove(&operation)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum SendBindingError {
    WrongBatch,
    MissingRequest,
    WrongRequest,
    WrongWindow,
    WrongOwner,
    MissingGrant,
}

/// The same committed batch and exact rich request handles, before any
/// grant or queue partition changes custody. The Source uses these handles
/// to quote the actual carrier and retries binding with this same object.
pub(crate) struct PreparedSendBatch {
    effect: Arc<AttemptEffectEnvelope>,
    operations: Vec<OperationIntent>,
    keys: Arc<[SendKey]>,
}
impl PreparedSendBatch {
    pub(crate) fn resolve(
        effect: Arc<AttemptEffectEnvelope>,
        requests: &FrozenSendRequests,
    ) -> Result<Self, SendBindingError> {
        let EffectBody::SendBatch { batch, .. } = &effect.protocol.body else {
            return Err(SendBindingError::WrongBatch);
        };
        if batch.operations.is_empty() || batch.operations.len() != batch.queued_inputs.len() {
            return Err(SendBindingError::WrongBatch);
        }
        let mut seen = BTreeSet::new();
        let mut operations = Vec::with_capacity(batch.operations.len());
        let mut keys = Vec::with_capacity(batch.operations.len());
        for (passive, grant) in batch.operations.iter().zip(&batch.queued_inputs) {
            if !seen.insert(grant.record())
                || passive.backend_process_id() != batch.backend
                || passive.lane() != batch.lane
                || passive.requires_control_progress() != batch.control_progress
            {
                return Err(SendBindingError::WrongBatch);
            }
            let operation = passive.operation_id();
            let original = requests
                .originals
                .get(&operation)
                .ok_or(SendBindingError::MissingRequest)?;
            let run = original.native_run_key();
            if original.shape() != passive.shape()
                || original.backend_process_id() != batch.backend
                || original.requires_control_progress() != batch.control_progress
                || run.1 != passive.envelope().max_wait().get()
                || operations
                    .first()
                    .is_some_and(|first: &OperationIntent| first.native_run_key() != run)
            {
                return Err(SendBindingError::WrongRequest);
            }
            operations.push(original.clone());
            keys.push(SendKey {
                operation,
                generation: grant.generation(),
            });
        }
        Ok(Self {
            effect,
            operations,
            keys: keys.into(),
        })
    }

    pub(crate) fn operations(&self) -> &[OperationIntent] {
        &self.operations
    }

    pub(super) fn keys(&self) -> &[SendKey] {
        &self.keys
    }

    pub(super) fn check_grants(&self, inputs: &GrantedInputs) -> Result<(), SendBindingError> {
        let EffectBody::SendBatch { batch, .. } = &self.effect.protocol.body else {
            unreachable!("prepared original send");
        };
        if batch
            .queued_inputs
            .iter()
            .zip(self.keys.iter())
            .any(|(grant, key)| !inputs.matches(*grant, *key))
        {
            return Err(SendBindingError::MissingGrant);
        }
        Ok(())
    }

    /// All exact original inputs must be present before any row moves. This
    /// exclusive Source borrow also prevents a concurrent ReleaseQueuedInput
    /// from consuming a checked row in the middle of the transfer.
    pub(crate) fn bind(
        self,
        inputs: &mut GrantedInputs,
        window: QueueWindow,
        registration: TransportJoinRegistration,
    ) -> Result<(QueueCustody, ResolvedBatch), SendBindingRefusal> {
        let error = self.validate_inputs(inputs, &window);
        if let Err(error) = error {
            return Err(SendBindingRefusal {
                error,
                batch: self,
                window,
            });
        }
        let EffectBody::SendBatch { batch, .. } = &self.effect.protocol.body else {
            unreachable!("prepared original send");
        };
        let queue = match QueueCustody::try_new(window, registration.clone(), self.keys.clone()) {
            Ok(queue) => queue,
            Err(window) => {
                return Err(SendBindingRefusal {
                    error: SendBindingError::WrongOwner,
                    batch: self,
                    window,
                });
            }
        };
        let mut queued_inputs = Vec::with_capacity(self.keys.len());
        for (index, (grant, key)) in batch.queued_inputs.iter().zip(self.keys.iter()).enumerate() {
            let input = inputs
                .take(*grant, *key)
                .expect("preflighted original input");
            queued_inputs.push(
                input
                    .bind_send(registration.clone(), index)
                    .unwrap_or_else(|_| {
                        panic!("new original return position must accept its exact input")
                    }),
            );
        }
        Ok((
            queue,
            ResolvedBatch {
                effect: self.effect,
                operations: self.operations,
                queued_inputs,
                keys: self.keys,
            },
        ))
    }

    fn validate_inputs(
        &self,
        inputs: &GrantedInputs,
        window: &QueueWindow,
    ) -> Result<(), SendBindingError> {
        let EffectBody::SendBatch { batch, reservation } = &self.effect.protocol.body else {
            unreachable!("prepared original send");
        };
        if window.parent_reservation().get() != reservation.0.get() || !window.matches_batch(batch)
        {
            return Err(SendBindingError::WrongWindow);
        }
        self.check_grants(inputs)
    }
}

#[must_use = "retain the original prepared batch and queue window on refusal"]
pub(crate) struct SendBindingRefusal {
    pub(crate) error: SendBindingError,
    pub(crate) batch: PreparedSendBatch,
    pub(crate) window: QueueWindow,
}

/// The same committed batch after all original grants have become loans.
pub(crate) struct ResolvedBatch {
    effect: Arc<AttemptEffectEnvelope>,
    pub(super) operations: Vec<OperationIntent>,
    pub(super) queued_inputs: Vec<SendInputLoan>,
    pub(super) keys: Arc<[SendKey]>,
}
impl ResolvedBatch {
    pub(super) fn matches_effect(&self, effect: &Arc<AttemptEffectEnvelope>) -> bool {
        Arc::ptr_eq(&self.effect, effect)
    }

    #[cfg(test)]
    pub(crate) fn into_parts(self) -> (Arc<[SendKey]>, Vec<SendInputLoan>) {
        (self.keys, self.queued_inputs)
    }
}
