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
    AdmissionSource, AttemptEstablishMaterial, GrantedInputs, QueueCustody, QueueWindow,
    SendInputLoan, SendKey,
};
use crate::{
    control_runtime::transport_withdrawal::AttemptEffectEnvelope,
    task_execution::{
        creation::CreateTaskIntent,
        host::{
            custody::TransportJoinRegistration,
            pumps::{
                split::SplitPayloadRegistration,
                split_source::{SplitPayloadError, SplitPayloadStore},
            },
        },
        intent::OperationIntent,
    },
};
use novarocks_attempt_protocol::{
    effect::{EffectBody, OperationIntent as ProtocolIntent, TaskDomainIntent},
    start::PayloadRef,
};
use novarocks_execution_contract::{
    OperationEnvelope, SplitAssignmentIntent, TaskDomainUpdate, TaskIdentity, TaskOperationId,
    UpdateTask,
};
use novarocks_resource_contract::ExistingOwnerRetentionPin;
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU64,
    sync::{Arc, OnceLock},
};

/// Holds each frozen rich request once for its logical operation. Physical
/// replays borrow the same backing by cloning the original intent handle.
#[derive(Default)]
pub(crate) struct FrozenSendRequests {
    originals: BTreeMap<TaskOperationId, OperationIntent>,
    creation_payloads: BTreeMap<PayloadRef, TaskOperationId>,
    split_payloads: BTreeMap<PayloadRef, FrozenSplitPayload>,
    establish: Option<Arc<AttemptEstablishMaterial>>,
}

struct FrozenSplitPayload {
    task: TaskIdentity,
    assignment: SplitAssignmentIntent,
    request: OnceLock<Arc<UpdateTask>>,
}

pub(super) struct NativeSplitPayloadStore<'a> {
    pub(super) requests: &'a mut FrozenSendRequests,
    pub(super) admission: &'a mut AdmissionSource,
    pub(super) next: &'a mut NonZeroU64,
}

struct DetachedSplitPayload {
    _request: FrozenSplitPayload,
    _pin: Option<ExistingOwnerRetentionPin>,
}

impl SplitPayloadStore for NativeSplitPayloadStore<'_> {
    fn install(
        &mut self,
        original: &SplitPayloadRegistration,
    ) -> Result<Option<PayloadRef>, SplitPayloadError> {
        let reference = PayloadRef(*self.next);
        let next = self
            .next
            .checked_add(1)
            .ok_or(SplitPayloadError::ReferenceExhausted)?;
        if !self.requests.payload_ref_is_unused(reference)
            || !self
                .admission
                .install_payload(reference, original.backing_bytes)
        {
            return Err(SplitPayloadError::DuplicateReference);
        }
        self.requests.install_split_payload(reference, original);
        *self.next = next;
        Ok(Some(reference))
    }

    fn detach(
        &mut self,
        reference: PayloadRef,
    ) -> Result<Option<Box<dyn Any + Send>>, SplitPayloadError> {
        let unaliased = self
            .requests
            .split_payload_unaliased(reference)
            .ok_or(SplitPayloadError::MissingReference)?;
        let claimed = self
            .admission
            .split_payload_claimed(reference)
            .ok_or(SplitPayloadError::MissingReference)?;
        if !unaliased || claimed {
            return Ok(None);
        }
        Ok(Some(Box::new(DetachedSplitPayload {
            _request: self.requests.take_split_payload(reference),
            _pin: self.admission.take_split_payload_pin(reference),
        })))
    }
}

impl FrozenSendRequests {
    pub(crate) fn with_establish(establish: Arc<AttemptEstablishMaterial>) -> Self {
        Self {
            establish: Some(establish),
            ..Self::default()
        }
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
        if self.originals.contains_key(&operation) || !self.payload_ref_is_unused(payload) {
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

    fn payload_ref_is_unused(&self, reference: PayloadRef) -> bool {
        !self.creation_payloads.contains_key(&reference)
            && !self.split_payloads.contains_key(&reference)
    }

    fn install_split_payload(
        &mut self,
        reference: PayloadRef,
        original: &SplitPayloadRegistration,
    ) {
        assert!(self.payload_ref_is_unused(reference));
        self.split_payloads.insert(
            reference,
            FrozenSplitPayload {
                task: original.task,
                assignment: original.assignment.clone(),
                request: OnceLock::new(),
            },
        );
    }

    fn split_payload_unaliased(&self, reference: PayloadRef) -> Option<bool> {
        let payload = self.split_payloads.get(&reference)?;
        // Only this host can clone the canonical request once the executor
        // has returned its final alias. Existing send owners keep it live.
        Some(
            payload
                .request
                .get()
                .is_none_or(|request| Arc::strong_count(request) == 1),
        )
    }

    fn take_split_payload(&mut self, reference: PayloadRef) -> FrozenSplitPayload {
        assert_eq!(self.split_payload_unaliased(reference), Some(true));
        self.split_payloads
            .remove(&reference)
            .expect("original split payload")
    }

    fn resolve_split_payload(
        &self,
        envelope: OperationEnvelope,
        identity: TaskIdentity,
        domain: &TaskDomainIntent,
    ) -> Result<OperationIntent, SendBindingError> {
        let TaskDomainIntent::SplitAssignment {
            node,
            offer,
            payload,
            queued_bytes,
        } = domain
        else {
            unreachable!("original split domain");
        };
        let original = self
            .split_payloads
            .get(payload)
            .ok_or(SendBindingError::MissingRequest)?;
        if original.task != identity
            || original.assignment.node() != *node
            || original.assignment.offer() != *offer
            || original.assignment.payload().encoded_len() != *queued_bytes
        {
            return Err(SendBindingError::WrongRequest);
        }
        if original.request.get().is_none() {
            let request = UpdateTask::try_new(
                envelope.operation_id(),
                identity,
                vec![TaskDomainUpdate::SplitAssignment(
                    original.assignment.clone(),
                )],
            )
            .map_err(|_| SendBindingError::WrongRequest)?;
            if request.envelope() != envelope {
                return Err(SendBindingError::WrongRequest);
            }
            assert!(original.request.set(Arc::new(request)).is_ok());
        }
        let request = original
            .request
            .get()
            .expect("original frozen split request");
        if request.envelope() != envelope {
            return Err(SendBindingError::WrongRequest);
        }
        Ok(OperationIntent::UpdateTask(request.clone()))
    }

    pub(super) fn is_empty(&self) -> bool {
        self.originals.is_empty() && self.split_payloads.is_empty() && self.establish.is_none()
    }

    /// Quote the same immutable request before queue admission without forming
    /// an UpdateTask allocation or claiming the original split request.
    pub(crate) fn encoded_protobuf_bytes(
        &self,
        passive: &ProtocolIntent,
    ) -> novarocks_task_codec::operation::wire_size::SizeResult {
        use novarocks_task_codec::operation::wire_size as size;
        let wrong = || size::WireSizeError::NotCodecProduced;
        if let ProtocolIntent::UpdateTask {
            envelope,
            identity,
            domains,
        } = passive
        {
            if domains.is_empty() {
                return Err(wrong());
            }
            if domains
                .iter()
                .any(|domain| matches!(domain, TaskDomainIntent::TaskDynamicFilter { .. }))
            {
                return self
                    .resolve(passive)
                    .map_err(|_| wrong())?
                    .encoded_protobuf_bytes();
            }
            if domains.len() != 1
                && domains
                    .iter()
                    .any(|domain| matches!(domain, TaskDomainIntent::SplitAssignment { .. }))
            {
                return Err(wrong());
            }
            let control = passive.requires_control_progress();
            let items = domains.iter().map(|domain| match domain {
                TaskDomainIntent::SplitAssignment {
                    node,
                    offer,
                    payload,
                    queued_bytes,
                } => {
                    let original = self.split_payloads.get(payload).ok_or_else(wrong)?;
                    if original.task != *identity
                        || original.assignment.node() != *node
                        || original.assignment.offer() != *offer
                        || original.assignment.payload().encoded_len() != *queued_bytes
                        || original
                            .request
                            .get()
                            .is_some_and(|request| request.envelope() != *envelope)
                    {
                        return Err(wrong());
                    }
                    size::split_assignment(&original.assignment)
                }
                TaskDomainIntent::TaskDynamicFilter { .. } => Err(wrong()),
                TaskDomainIntent::OpenExchangeEdges { version, edges } => {
                    size::open_exchange_edges(*version, edges)
                }
                TaskDomainIntent::CloseExchangeDestination {
                    version,
                    edge,
                    destination,
                } => {
                    let bytes = size::close_exchange_destination(*version, *edge, *destination)?;
                    // The ordinary grammar wraps the close domain, while the
                    // closed control grammar carries its body directly.
                    if control {
                        Ok(bytes)
                    } else {
                        size::close_domain(bytes)
                    }
                }
            });
            return size::batch_item(size::update_parts(*envelope, *identity, items, control)?);
        }
        self.resolve(passive)
            .map_err(|_| wrong())?
            .encoded_protobuf_bytes()
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
            ProtocolIntent::UpdateTask {
                envelope,
                identity,
                domains,
            } if matches!(domains.as_ref(), [TaskDomainIntent::SplitAssignment { .. }]) => {
                self.resolve_split_payload(*envelope, *identity, &domains[0])?
            }
            ProtocolIntent::UpdateTask {
                envelope,
                identity,
                domains,
            } if domains.iter().all(|domain| {
                matches!(
                    domain,
                    TaskDomainIntent::OpenExchangeEdges { .. }
                        | TaskDomainIntent::CloseExchangeDestination { .. }
                )
            }) =>
            {
                // Exchange updates have no external payload to recover. The
                // frozen Core values are the complete immutable request.
                let domains = domains
                    .iter()
                    .map(|domain| match domain {
                        TaskDomainIntent::OpenExchangeEdges { version, edges } => {
                            TaskDomainUpdate::OpenExchangeEdges {
                                version: *version,
                                edges: edges.clone(),
                            }
                        }
                        TaskDomainIntent::CloseExchangeDestination {
                            version,
                            edge,
                            destination,
                        } => TaskDomainUpdate::CloseExchangeDestination {
                            version: *version,
                            edge: *edge,
                            destination: *destination,
                        },
                        _ => unreachable!("original inline exchange domains"),
                    })
                    .collect();
                let request = UpdateTask::try_new(envelope.operation_id(), *identity, domains)
                    .map_err(|_| SendBindingError::WrongRequest)?;
                if request.envelope() != *envelope {
                    return Err(SendBindingError::WrongRequest);
                }
                OperationIntent::UpdateTask(Arc::new(request))
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
#[allow(
    dead_code,
    reason = "A refused binding returns the original batch and queue window with the cause."
)]
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
    pub(super) fn single(
        operation: OperationIntent,
        input: SendInputLoan,
        keys: Arc<[SendKey]>,
    ) -> Self {
        assert_eq!(&*keys, &[input.key()]);
        assert_eq!(input.key().operation, operation.operation_id());
        Self {
            effect: None,
            operations: vec![operation],
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

    #[test]
    fn core_exchange_updates_preserve_original_wire_request_on_replay() {
        use novarocks_execution_contract::{DomainVersion, EdgeOpenVersion, ExchangeEdgeId};
        use novarocks_types::{StageId, TaskId};

        let execution =
            QueryExecutionId::new(QueryId::new(0x7a5c, 7), AttemptId::new(1).unwrap()).unwrap();
        let task = |ordinal| {
            TaskIdentity::new(
                execution,
                StageId::new(ordinal).unwrap(),
                TaskId::new(ordinal).unwrap(),
                BackendProcessId::new_v7(),
            )
        };
        let (producer, destination) = (task(1), task(2));
        let edge = ExchangeEdgeId::new(3).unwrap();
        let wire = AttemptWireFacts {
            native_compatibility_id: NativeCompatibilityId::new([1; 32]),
        };
        let requests = FrozenSendRequests::default();
        for (passive, original) in [
            (
                TaskDomainIntent::OpenExchangeEdges {
                    version: EdgeOpenVersion::FIRST,
                    edges: vec![edge],
                },
                TaskDomainUpdate::OpenExchangeEdges {
                    version: EdgeOpenVersion::FIRST,
                    edges: vec![edge],
                },
            ),
            (
                TaskDomainIntent::CloseExchangeDestination {
                    version: DomainVersion::FIRST,
                    edge,
                    destination,
                },
                TaskDomainUpdate::CloseExchangeDestination {
                    version: DomainVersion::FIRST,
                    edge,
                    destination,
                },
            ),
        ] {
            let operation = TaskOperationId::new_v7();
            let original = UpdateTask::try_new(operation, producer, vec![original]).unwrap();
            let passive = ProtocolIntent::UpdateTask {
                envelope: original.envelope(),
                identity: producer,
                domains: Arc::from([passive]),
            };
            let original =
                encode_operation(&OperationIntent::UpdateTask(Arc::new(original)), &wire).unwrap();
            for _ in 0..2 {
                let projected = requests.resolve(&passive).unwrap();
                assert_eq!(projected.operation_id(), operation);
                assert_eq!(projected.queued_bytes(), passive.queued_bytes());
                assert_eq!(
                    requests.encoded_protobuf_bytes(&passive).unwrap(),
                    novarocks_task_codec::operation::wire_size::batch_item(
                        prost::Message::encoded_len(&original)
                    )
                    .unwrap()
                );
                assert_eq!(encode_operation(&projected, &wire).unwrap(), original);
            }
        }
    }

    #[test]
    fn split_native_request_keeps_its_original_binding_through_replay_and_alias_exit() {
        use super::super::queue_input::tests::Governed;
        use novarocks_attempt_protocol::input::SplitDeliveryRef;
        use novarocks_control_runtime::transaction::RuntimeLedger;
        use novarocks_control_runtime_core::{
            capacity::{ReservationResources, ResourceVector},
            identity::{ConsumerId, Generation},
            scope::{WorkAdmission, WorkKind},
        };
        use novarocks_execution_contract::{PlanNodeId, SplitOffer};
        use novarocks_proto_models::connector_read::SplitAssignment;
        use novarocks_task_codec::domain::wire_split_assignment;
        use novarocks_types::{StageId, TaskId};

        let ledger = RuntimeLedger::new(ResourceVector::default());
        let generation = Generation::new(1).unwrap();
        let scope = ledger.open_scope_next(None, generation).unwrap();
        let (mut parent, mut payer) = scope
            .reserve_next(
                generation,
                ReservationResources::work_only(),
                WorkKind::Settlement,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Settlement)
            .unwrap();
        parent.start().unwrap();
        let wc = Governed::new();
        let mut admission = AdmissionSource::new(
            &parent,
            &payer,
            generation,
            ConsumerId::new(1).unwrap(),
            wc.authority(),
            Default::default(),
        )
        .unwrap_or_else(|_| panic!("original split payer"));
        let mut requests = FrozenSendRequests::default();
        let mut next = NonZeroU64::MIN;
        let execution =
            QueryExecutionId::new(QueryId::new(7, 8), AttemptId::new(1).unwrap()).unwrap();
        let task = TaskIdentity::new(
            execution,
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        let node = PlanNodeId::new(7).unwrap();
        let original = wire_split_assignment(SplitAssignment {
            plan_node_id: 7,
            no_more_splits: true,
            splits: Vec::new(),
        });
        let row = SplitPayloadRegistration {
            delivery: SplitDeliveryRef(NonZeroU64::MIN),
            task,
            assignment: SplitAssignmentIntent::new(node, SplitOffer::Seal, original.clone()),
            backing_bytes: 256,
        };
        let mut store = NativeSplitPayloadStore {
            requests: &mut requests,
            admission: &mut admission,
            next: &mut next,
        };
        let payload = store.install(&row).unwrap().unwrap();
        let envelope = OperationEnvelope::with_default_wait(
            TaskOperationId::new_v7(),
            OperationKind::UpdateTask,
        );
        let passive = ProtocolIntent::UpdateTask {
            envelope,
            identity: task,
            domains: Arc::from([TaskDomainIntent::SplitAssignment {
                node,
                offer: SplitOffer::Seal,
                payload,
                queued_bytes: original.encoded_len(),
            }]),
        };
        let quoted = store.requests.encoded_protobuf_bytes(&passive).unwrap();
        assert!(
            store.requests.split_payloads[&payload]
                .request
                .get()
                .is_none(),
            "queue preflight must not allocate or claim a split request"
        );
        let OperationIntent::UpdateTask(first) = store.requests.resolve(&passive).unwrap() else {
            panic!("split update")
        };
        let OperationIntent::UpdateTask(replay) = store.requests.resolve(&passive).unwrap() else {
            panic!("split replay")
        };
        let [TaskDomainUpdate::SplitAssignment(assignment)] = first.domains() else {
            panic!("original split assignment")
        };
        assert!(Arc::ptr_eq(assignment.payload(), &original));
        assert!(Arc::ptr_eq(&first, &replay));
        assert_eq!(
            quoted,
            OperationIntent::UpdateTask(first.clone())
                .encoded_protobuf_bytes()
                .unwrap()
        );
        assert!(store.detach(payload).unwrap().is_none());
        drop((first, replay));
        drop(store.detach(payload).unwrap().unwrap());
        assert!(requests.is_empty() && admission.is_drained());
        drop(admission);
        assert!(wc.finish());
        parent.body_exited().unwrap();
        parent.captures_exited().unwrap();
        payer.finish().unwrap();
        assert!(
            scope
                .reap_observed_header(&parent.retirement_observer())
                .unwrap()
        );
        scope.close_external().unwrap();
        ledger.remove_scope(scope.id()).unwrap();
        assert!(ledger.is_empty());
    }
}
