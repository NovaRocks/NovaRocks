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

use super::{
    AttemptEstablishMaterial, GrantedInputs, QueueCustody, QueueWindow, SendInputLoan, SendKey,
};
use crate::{
    control_runtime::transport_withdrawal::AttemptEffectEnvelope,
    task_execution::{
        creation::CreateTaskIntent, host::custody::TransportJoinRegistration,
        intent::OperationIntent,
    },
};
use novarocks_attempt_protocol::{
    effect::{EffectBody, OperationIntent as ProtocolIntent},
    start::PayloadRef,
};
use novarocks_execution_contract::{TaskIdentity, TaskOperationId};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Holds each frozen rich request once for its logical operation. Physical
/// replays borrow the same backing by cloning the original intent handle.
#[derive(Default)]
pub(crate) struct FrozenSendRequests {
    originals: BTreeMap<TaskOperationId, OperationIntent>,
    creation_payloads: BTreeMap<PayloadRef, TaskOperationId>,
    establish: Option<Arc<AttemptEstablishMaterial>>,
}
impl FrozenSendRequests {
    pub(crate) fn with_establish(establish: Arc<AttemptEstablishMaterial>) -> Self {
        Self {
            establish: Some(establish),
            ..Self::default()
        }
    }

    pub(crate) fn install(&mut self, original: OperationIntent) -> Result<(), OperationIntent> {
        let std::collections::btree_map::Entry::Vacant(record) =
            self.originals.entry(original.operation_id())
        else {
            return Err(original);
        };
        record.insert(original);
        Ok(())
    }

    pub(crate) fn into_originals(self) -> BTreeMap<TaskOperationId, OperationIntent> {
        self.originals
    }

    #[cfg(test)]
    pub(crate) fn creation_for_test(&self, operation: TaskOperationId) -> Option<CreateTaskIntent> {
        match self.originals.get(&operation)? {
            OperationIntent::CreateTask(original) => Some(original.clone()),
            _ => None,
        }
    }

    pub(super) fn install_creation(
        &mut self,
        payload: PayloadRef,
        operation: TaskOperationId,
        creation: &mut Option<CreateTaskIntent>,
    ) -> bool {
        if self.originals.contains_key(&operation) || self.creation_payloads.contains_key(&payload)
        {
            return false;
        }
        let original = creation.take().expect("original admitted creation");
        assert_eq!(original.envelope().operation_id(), operation);
        self.originals
            .insert(operation, OperationIntent::CreateTask(original));
        self.creation_payloads.insert(payload, operation);
        true
    }

    pub(crate) fn retire_creation(
        &mut self,
        task: TaskIdentity,
        payload: PayloadRef,
    ) -> Option<OperationIntent> {
        let operation = *self.creation_payloads.get(&payload)?;
        if !matches!(self.originals.get(&operation), Some(OperationIntent::CreateTask(original)) if original.identity() == task)
        {
            return None;
        }
        self.creation_payloads.remove(&payload);
        self.originals.remove(&operation)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.originals.is_empty() && self.establish.is_none()
    }

    /// These lifecycle commands already carry the complete immutable
    /// request in Core. The host copies the value into its paid batch and
    /// borrows rich backing only for the payload-bearing commands.
    fn resolve(&self, passive: &ProtocolIntent) -> Result<OperationIntent, SendBindingError> {
        let original = match passive {
            ProtocolIntent::AcquireParticipationAdmissionTicket(request) => {
                OperationIntent::AcquireParticipationAdmissionTicket(*request)
            }
            ProtocolIntent::RenewParticipationLease(request) => {
                OperationIntent::RenewParticipationLease(request.clone())
            }
            ProtocolIntent::CancelTask(request) => OperationIntent::CancelTask(*request),
            ProtocolIntent::QuiesceParticipation(request) => {
                OperationIntent::QuiesceParticipation(*request)
            }
            ProtocolIntent::AbortParticipation(request) => {
                OperationIntent::AbortParticipation(*request)
            }
            ProtocolIntent::ReleaseParticipation(request) => {
                OperationIntent::ReleaseParticipation(*request)
            }
            ProtocolIntent::EstablishParticipation { .. } => {
                OperationIntent::EstablishParticipation(
                    self.establish
                        .as_ref()
                        .ok_or(SendBindingError::MissingRequest)?
                        .resolve(passive)?,
                )
            }
            _ => {
                let operation = passive.operation_id();
                let original = self
                    .originals
                    .get(&operation)
                    .ok_or(SendBindingError::MissingRequest)?;
                let same = match (passive, original) {
                    (
                        ProtocolIntent::CreateTask {
                            envelope,
                            identity,
                            creation,
                            queued_bytes,
                        },
                        OperationIntent::CreateTask(request),
                    ) => {
                        self.creation_payloads.get(creation) == Some(&operation)
                            && request.envelope() == *envelope
                            && request.identity() == *identity
                            && request.queued_bytes() == *queued_bytes
                    }
                    (
                        ProtocolIntent::UpdateTask { identity, .. },
                        OperationIntent::UpdateTask(request),
                    ) => request.identity() == *identity,
                    (
                        ProtocolIntent::AdvanceParticipationDomain { participation, .. },
                        OperationIntent::UpdateParticipation(request),
                    ) => request.participation() == *participation,
                    _ => false,
                };
                if !same {
                    return Err(SendBindingError::WrongRequest);
                }
                original.clone()
            }
        };
        Ok(original)
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
            let original = requests.resolve(passive)?;
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
            operations.push(original);
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
                effect: Some(self.effect),
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
    effect: Option<Arc<AttemptEffectEnvelope>>,
    pub(super) operations: Vec<OperationIntent>,
    pub(super) queued_inputs: Vec<SendInputLoan>,
    pub(super) keys: Arc<[SendKey]>,
}
impl ResolvedBatch {
    pub(super) fn cleanup_abort(
        request: novarocks_execution_contract::AbortParticipation,
        input: SendInputLoan,
        keys: Arc<[SendKey]>,
    ) -> Self {
        assert_eq!(&*keys, &[input.key()]);
        assert_eq!(input.key().operation, request.envelope().operation_id());
        Self {
            effect: None,
            operations: vec![OperationIntent::AbortParticipation(request)],
            queued_inputs: vec![input],
            keys,
        }
    }

    pub(super) fn matches_effect(&self, effect: &Arc<AttemptEffectEnvelope>) -> bool {
        self.effect
            .as_ref()
            .is_some_and(|original| Arc::ptr_eq(original, effect))
    }

    #[cfg(test)]
    pub(crate) fn into_parts(self) -> (Arc<[SendKey]>, Vec<SendInputLoan>) {
        (self.keys, self.queued_inputs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::task_transport::wire::{
        AttemptWireFacts, encode_operation, is_lease_renewal,
    };
    use crate::{
        query_execution::lifecycle_plan::QueryCredentialLeases,
        task_execution::sources::AttemptEstablishFacts,
    };
    use novarocks_coordination_contract::AttemptAuthorization;
    use novarocks_execution_contract::{
        AdmissionTicketId, LeaseSequence, LeaseValidFor, OperationEnvelope, OperationKind,
        ParticipationRef, RenewParticipationLease, UpdateParticipation,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, FrontendProcessId, NativeCompatibilityId, QueryExecutionId,
        QueryId,
    };
    use std::time::Duration;

    #[test]
    fn establish_authorization_and_send_share_one_frozen_request() {
        use novarocks_proto_models::novarocks::{
            RuntimeFilterContribution, task_operation::Operation,
            update_participation_request::Command,
        };
        let execution =
            QueryExecutionId::new(QueryId::new(0x7a5c, 6), AttemptId::new(1).unwrap()).unwrap();
        let frontend = FrontendProcessId::new_v7();
        let first = ParticipationRef::new(execution, frontend, BackendProcessId::new_v7());
        let second = ParticipationRef::new(execution, frontend, BackendProcessId::new_v7());
        let original_facts = AttemptEstablishFacts::freeze(
            Default::default(),
            [(first, 11), (second, 22)].map(|(participation, participant_id)| {
                (
                    participation.backend_process_id(),
                    RuntimeFilterContribution {
                        participant_id,
                        ..Default::default()
                    },
                )
            }),
            Default::default(),
            &QueryCredentialLeases::empty(),
        )
        .unwrap();
        let mut next_payload = std::num::NonZeroU64::new(12).unwrap();
        let (quotes, material) =
            AttemptEstablishMaterial::project(&original_facts, [first, second], &mut next_payload)
                .unwrap();
        assert_eq!(next_payload.get(), 14);
        let material = Arc::new(material);
        let requests = FrozenSendRequests::with_establish(material.clone());
        let operation_id = TaskOperationId::new_v7();
        let intent = |payload: PayloadRef, ticket: u8| ProtocolIntent::EstablishParticipation {
            envelope: OperationEnvelope::with_default_wait(
                operation_id,
                OperationKind::UpdateParticipation,
            ),
            participation: first,
            ticket: AdmissionTicketId::try_from_bytes([ticket; 16]).unwrap(),
            initial_lease_sequence: LeaseSequence::INITIAL,
            initial_lease_valid_for: LeaseValidFor::new(Duration::from_secs(30)).unwrap(),
            material: payload,
            queued_bytes: quotes[&first].queued_bytes,
        };
        let initial = AttemptAuthorization::Establish {
            participation: first,
            operation_id,
        };
        let replay = AttemptAuthorization::ReauthorizeEstablish {
            participation: first,
            operation_id,
        };
        let passive = intent(quotes[&first].material, 41);
        assert!(matches!(
            requests.resolve(&passive),
            Err(SendBindingError::MissingRequest)
        ));
        assert!(matches!(
            material.for_authorization(&passive, replay),
            Err(SendBindingError::MissingRequest)
        ));
        assert!(matches!(
            material.for_authorization(&intent(quotes[&second].material, 41), initial),
            Err(SendBindingError::WrongRequest)
        ));
        let authorized = material.for_authorization(&passive, initial).unwrap();
        let replayed = material.for_authorization(&passive, replay).unwrap();
        assert!(Arc::ptr_eq(&authorized, &replayed));
        assert!(matches!(
            requests.resolve(&intent(quotes[&first].material, 42)),
            Err(SendBindingError::WrongRequest)
        ));
        let projected = requests.resolve(&passive).unwrap();
        let OperationIntent::EstablishParticipation(ref sent) = projected else {
            panic!("the exact original Establish");
        };
        assert!(Arc::ptr_eq(sent, &authorized));
        assert_eq!(projected.queued_bytes(), quotes[&first].queued_bytes);
        let encoded = encode_operation(
            &projected,
            &AttemptWireFacts {
                native_compatibility_id: NativeCompatibilityId::new([1; 32]),
            },
        )
        .unwrap();
        let Some(Operation::UpdateParticipation(request)) = encoded.operation else {
            panic!("the original participation request");
        };
        let Some(Command::Establish(request)) = request.command else {
            panic!("the original Establish command");
        };
        assert_eq!(request.initial_runtime_filter.unwrap().participant_id, 11);
    }

    #[test]
    fn core_renewal_projects_inline_with_its_exact_wire_identity() {
        let participation = ParticipationRef::new(
            QueryExecutionId::new(QueryId::new(0x7a5c, 6), AttemptId::new(1).unwrap()).unwrap(),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        );
        let original = RenewParticipationLease::new(
            TaskOperationId::new_v7(),
            participation,
            LeaseSequence::INITIAL.next().unwrap(),
            LeaseValidFor::new(Duration::from_secs(5)).unwrap(),
        );
        let passive = ProtocolIntent::RenewParticipationLease(original.clone());
        let projected = FrozenSendRequests::default().resolve(&passive).unwrap();
        let existing = OperationIntent::UpdateParticipation(Arc::new(
            UpdateParticipation::RenewLease(original),
        ));
        let facts = AttemptWireFacts {
            native_compatibility_id: NativeCompatibilityId::new([1; 32]),
        };
        assert!(matches!(
            projected,
            OperationIntent::RenewParticipationLease(_)
        ));
        assert_eq!(projected.operation_id(), passive.operation_id());
        assert_eq!(projected.queued_bytes(), passive.queued_bytes());
        assert_eq!(projected.native_run_key(), existing.native_run_key());
        assert!(is_lease_renewal(&projected));
        assert_eq!(
            encode_operation(&projected, &facts).unwrap(),
            encode_operation(&existing, &facts).unwrap()
        );
    }
}
