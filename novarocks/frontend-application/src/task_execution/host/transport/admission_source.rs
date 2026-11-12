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

//! Attempt-owned queued input preparation, publication and paid result roles.

use super::queued_job::{CreationAdmission, CreationSeed};
use super::{
    ActorTransportWork, BodyOutcome, BoundQueue, FrozenSendRequests, GrantedInputs,
    OriginalQueuedInput, SendKey, SourceLayouts, actor_transport_layouts, prepare_actor_transport,
    queued_input_quote,
};
use crate::{
    control_runtime::{
        actor::{FrontendActor, FrontendEffect, FrontendInput},
        host::PreparedEffectJob,
        transport_withdrawal::AttemptEffectEnvelope,
    },
    task_execution::{
        creation::{PricedTaskCreation, creation_owner_header_bytes},
        error::TaskExecutionError,
        host::custody::ReturnedActorTransportResult,
    },
};
use novarocks_attempt_protocol::{
    effect::{EffectBody, OperationIntent, QueuedInputPayload},
    failure::{
        AttemptFailure, AttemptFailureSite, HostFailureKind, HostFailureRef, HostProjectedFailure,
    },
    input::{AttemptInput, ExecutorFact, QueuedInputGrantRef},
    start::PayloadRef,
};
use novarocks_control_runtime::{
    actor_turn::{ActorTurnError, EffectTicket},
    causal_input::{CausalFillFailure, CausalInputObligation},
    completion::ReportSlot,
    exit_custody::ExitCustody,
    exit_work::HandoffState,
    obligation::{ReservationObligation, WorkObligation, WorkRetirementObserver},
    reported_work::ReportedWork,
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    capacity::{CapacityError, ReservationResources, ResourceVector},
    completion::ConsumerKind,
    identity::{ConsumerId, Generation, InputId, ReservationId},
    owned_storage::OwnedSlotNeed,
    scope::{ScopeError, WorkKind},
};
use novarocks_coordination_contract::AttemptActionId;
use novarocks_execution_contract::TaskIdentity;
use novarocks_resource_contract::{ExistingOwnerRetentionPin, RetentionPinError};
use novarocks_workload_control::joint_admission::NativeAsyncActionAuthority;
use std::{
    collections::{BTreeMap, VecDeque},
    num::NonZeroU64,
    sync::{Arc, Mutex},
};

type Position = CausalInputObligation<FrontendActor>;
pub(super) type FailureRecords = Arc<Mutex<BTreeMap<HostFailureRef, TaskExecutionError>>>;

struct PayloadCharge {
    backing_bytes: usize,
    pin: Option<ExistingOwnerRetentionPin>,
    claimed: Option<AttemptActionId>,
    creation: Option<(TaskIdentity, CreationSeed)>,
    price_reported: bool,
}

struct AdmissionHeader {
    effect: Arc<AttemptEffectEnvelope>,
    original: WorkRetirementObserver,
    transport_retired: bool,
    encoded_bytes: Option<usize>,
}

struct Pending {
    effect: Arc<AttemptEffectEnvelope>,
    lane: usize,
    work: ReportedWork,
    bound: BoundQueue,
    encoded_bytes: Option<usize>,
}

#[must_use = "the original result and any refused input remain in custody"]
#[allow(
    dead_code,
    reason = "Admission refusal transfers the original result position and typed cause to the failure owner."
)]
pub(crate) struct AdmissionResultRefusal {
    pub(crate) result: ReturnedActorTransportResult,
    pub(crate) cause: AdmissionResultError,
}

#[allow(
    dead_code,
    reason = "Admission failure keeps the original refused input and payload custody."
)]
pub(crate) enum AdmissionResultError {
    WrongEffect,
    MissingOutcome,
    MissingPayload,
    DuplicateGrant,
    DuplicateCreation,
    PayloadRetention(RetentionPinError),
    Fill(CausalFillFailure<FrontendInput>),
}

#[cfg(test)]
type CreateGrantObserver =
    Box<dyn FnMut(SendKey, u64, &crate::task_execution::creation::CreateTaskIntent) + Send>;

/// Uses pre-paid result positions per purpose. Each physical generation
/// derives its own Work from the original attempt payer and submits to WC
/// once. Only a completed occurrence returns a result position to the pool.
pub(crate) struct AdmissionSource {
    scope: ScopeHandle,
    parent: ReservationId,
    generation: Generation,
    observer: ConsumerId,
    authority: NativeAsyncActionAuthority,
    idle: [VecDeque<Position>; 2],
    results: VecDeque<(usize, ReturnedActorTransportResult)>,
    pending: Option<Pending>,
    headers: BTreeMap<AttemptActionId, AdmissionHeader>,
    payloads: BTreeMap<PayloadRef, PayloadCharge>,
    grants: GrantedInputs,
    failures: FailureRecords,
    #[cfg(test)]
    create_grant_observer: Option<CreateGrantObserver>,
}

impl AdmissionSource {
    pub(crate) fn new(
        original_parent: &WorkObligation,
        payer: &ReservationObligation,
        generation: Generation,
        observer: ConsumerId,
        authority: NativeAsyncActionAuthority,
        idle: [VecDeque<Position>; 2],
    ) -> Result<Self, [VecDeque<Position>; 2]> {
        let scope = original_parent.scope();
        if !payer.belongs_to_bound_work(scope, original_parent.id()) {
            return Err(idle);
        }
        let positions = idle.iter().map(VecDeque::len).sum();
        Ok(Self {
            scope: scope.clone(),
            parent: payer.id(),
            generation,
            observer,
            authority,
            idle,
            results: VecDeque::with_capacity(positions),
            pending: None,
            headers: BTreeMap::new(),
            payloads: BTreeMap::new(),
            grants: GrantedInputs::default(),
            failures: FailureRecords::default(),
            #[cfg(test)]
            create_grant_observer: None,
        })
    }

    #[cfg(test)]
    pub(crate) fn observe_create_grants_for_test(
        &mut self,
        observer: impl FnMut(SendKey, u64, &crate::task_execution::creation::CreateTaskIntent)
        + Send
        + 'static,
    ) {
        assert!(
            self.create_grant_observer
                .replace(Box::new(observer))
                .is_none()
        );
    }

    /// Supplied by the same owner that retains the frozen request. The
    /// allocation invoice cannot be reconstructed from its protocol size.
    pub(crate) fn install_payload(&mut self, payload: PayloadRef, backing_bytes: usize) -> bool {
        use std::collections::btree_map::Entry;
        let Entry::Vacant(record) = self.payloads.entry(payload) else {
            return false;
        };
        record.insert(PayloadCharge {
            backing_bytes,
            pin: None,
            claimed: None,
            creation: None,
            price_reported: false,
        });
        true
    }

    pub(crate) fn install_creation(
        &mut self,
        payload: PayloadRef,
        original: PricedTaskCreation,
    ) -> Result<(), PricedTaskCreation> {
        let Some(backing_bytes) = creation_owner_header_bytes()
            .ok()
            .and_then(|headers| headers.checked_add(original.sizing.new_backing_bytes))
        else {
            return Err(original);
        };
        if !self.install_payload(payload, backing_bytes) {
            return Err(original);
        }
        let task = original.seed.identity();
        self.payloads.get_mut(&payload).unwrap().creation =
            Some((task, Arc::new(Mutex::new(Some(original)))));
        Ok(())
    }

    pub(crate) fn can_release_creation(&self, payload: PayloadRef) -> bool {
        self.payloads
            .get(&payload)
            .is_none_or(|row| row.claimed.is_none())
    }

    pub(super) fn split_payload_claimed(&self, payload: PayloadRef) -> Option<bool> {
        self.payloads
            .get(&payload)
            .filter(|row| row.creation.is_none())
            .map(|row| row.claimed.is_some())
    }

    pub(super) fn take_split_payload_pin(
        &mut self,
        payload: PayloadRef,
    ) -> Option<ExistingOwnerRetentionPin> {
        assert_eq!(self.split_payload_claimed(payload), Some(false));
        self.payloads
            .remove(&payload)
            .expect("original split admission payload")
            .pin
    }

    pub(crate) fn take_creation(
        &mut self,
        task: TaskIdentity,
        payload: PayloadRef,
    ) -> Option<CreationSeed> {
        let row = self.payloads.get(&payload)?;
        let (original_task, _) = row.creation.as_ref()?;
        assert_eq!(*original_task, task);
        assert!(row.claimed.is_none());
        self.payloads
            .remove(&payload)?
            .creation
            .map(|(_, seed)| seed)
    }

    #[cfg(test)]
    pub(crate) fn take_grant(
        &mut self,
        grant: QueuedInputGrantRef,
        key: SendKey,
    ) -> Option<OriginalQueuedInput> {
        self.grants.take(grant, key)
    }

    pub(super) fn grants(&self) -> &GrantedInputs {
        &self.grants
    }

    pub(super) fn release_grant(
        &mut self,
        grant: QueuedInputGrantRef,
    ) -> Option<OriginalQueuedInput> {
        self.grants.take_for_release(grant)
    }

    pub(super) fn is_quiescent(&self) -> bool {
        self.pending.is_none() && self.headers.is_empty() && self.results.is_empty()
    }

    pub(crate) fn take_unclaimed_grant(&mut self) -> Option<OriginalQueuedInput> {
        if !self.is_quiescent() {
            return None;
        }
        self.grants.take_unclaimed()
    }

    /// Split owns its own detachment. The closed Source calls this after all
    /// split payloads and unbound queue grants have left their original stores.
    pub(crate) fn take_unclaimed_creation(
        &mut self,
    ) -> Option<(TaskIdentity, PayloadRef, CreationSeed)> {
        if !self.is_quiescent() || !self.grants.is_empty() {
            return None;
        }
        let (&payload, row) = self.payloads.first_key_value()?;
        let &(task, _) = row.creation.as_ref().expect("original creation row");
        let seed = self
            .take_creation(task, payload)
            .expect("same original creation");
        Some((task, payload, seed))
    }

    pub(super) fn is_drained(&self) -> bool {
        self.is_quiescent()
            && self.grants.is_empty()
            && self.payloads.is_empty()
            && self.failures.lock().unwrap().is_empty()
            && self.idle.iter().all(VecDeque::is_empty)
    }

    pub(crate) fn idle_positions(&self) -> usize {
        self.idle.iter().map(VecDeque::len).sum()
    }

    pub(super) fn matches_scope(&self, scope: &ScopeHandle) -> bool {
        self.scope.same_scope(scope)
    }

    pub(super) fn bind_send(
        &mut self,
        batch: super::PreparedSendBatch,
        window: super::QueueWindow,
        registration: crate::task_execution::host::custody::TransportJoinRegistration,
    ) -> Result<(super::QueueCustody, super::ResolvedBatch), super::SendBindingRefusal> {
        batch.bind(&mut self.grants, window, registration)
    }

    pub(super) fn failure_records(&self) -> FailureRecords {
        self.failures.clone()
    }

    pub(super) fn take_unselected_failure(
        &mut self,
    ) -> Option<(HostFailureRef, TaskExecutionError)> {
        self.failures.lock().unwrap().pop_first()
    }

    pub(super) fn project_failure(
        &self,
        reference: HostFailureRef,
    ) -> Option<novarocks_query_application::execution_error::ExecutionFailureInput> {
        self.failures
            .lock()
            .unwrap()
            .get(&reference)
            .map(TaskExecutionError::preparation_input)
    }

    pub(crate) fn take_failure(&mut self, failure: HostFailureRef) -> Option<TaskExecutionError> {
        self.failures.lock().unwrap().remove(&failure)
    }

    fn payload_ref(payload: QueuedInputPayload) -> Option<PayloadRef> {
        match payload {
            QueuedInputPayload::NewCreation(payload)
            | QueuedInputPayload::NewTaskUpdate(payload) => Some(payload),
            QueuedInputPayload::AlreadyRetained => None,
        }
    }

    fn validate_request(
        operation: &OperationIntent,
        encoded: novarocks_task_codec::operation::wire_size::SizeResult,
        transport: novarocks_task_codec::TransportBudget,
    ) -> Result<usize, TaskExecutionError> {
        use crate::task_execution::{error::CapacityBound, intent::NativeRequestWindows};
        use novarocks_task_codec::operation::wire_size::WireSizeError;
        use novarocks_workload_control::WorkError;
        let encoded = encoded.map_err(|error| {
            TaskExecutionError::Workload(match error {
                WireSizeError::Overflow => WorkError::ArithmeticOverflow,
                WireSizeError::NotCodecProduced => WorkError::Conflict,
            })
        })?;
        let control = operation.requires_control_progress();
        let limit = transport.message_limits().request_max_bytes(control);
        if encoded > limit {
            return Err(CapacityBound::OperationBytes {
                limit,
                actual: encoded,
            }
            .into());
        }
        let windows = NativeRequestWindows::for_transport(transport).map_err(|_| {
            TaskExecutionError::Workload(WorkError::InvalidConfig("Native request windows"))
        })?;
        let limit = windows.empty_queue_capacity(control);
        let actual = operation.queued_bytes();
        if actual > limit {
            return Err(CapacityBound::OperationBytes { limit, actual }.into());
        }
        Ok(encoded)
    }

    /// Select and price the same pending metadata before the host claims its
    /// ticket. Full is the absence of the original purpose's result credit.
    pub(crate) fn prepay(
        &mut self,
        effect: &Arc<AttemptEffectEnvelope>,
        requests: &FrozenSendRequests,
        transport: novarocks_task_codec::TransportBudget,
    ) -> Result<Option<Position>, RuntimeError> {
        assert!(self.pending.is_none());
        let EffectBody::AdmitQueuedInput {
            operation,
            generation,
            payload,
            lane: effect_lane,
        } = &effect.protocol.body
        else {
            unreachable!("only the original queued-input purpose");
        };
        assert_eq!(generation.admission_effect(), effect.protocol.id);
        assert_eq!(*effect_lane, operation.lane());
        let control = operation.requires_control_progress();
        let lane = usize::from(control);
        if self.idle[lane].is_empty() {
            return Ok(None);
        }
        let withdrawn = effect.handoff().unwrap().state() == HandoffState::Withdrawn;
        let payload_ref = (!withdrawn).then(|| Self::payload_ref(*payload)).flatten();
        let mut creation = None;
        let mut payload_bytes = 0;
        if let Some(payload_ref) = payload_ref {
            let Some(original) = self.payloads.get(&payload_ref) else {
                // Cleanup can overtake an unclaimed Create admission. The
                // same host will rescan when its Withdraw is processed.
                assert!(matches!(payload, QueuedInputPayload::NewCreation(_)));
                return Ok(None);
            };
            assert!(
                original.pin.is_none() && original.claimed.is_none(),
                "one original physical generation may price new payload backing"
            );
            if let OperationIntent::CreateTask {
                envelope,
                identity,
                creation: reference,
                ..
            } = operation
            {
                let (task, seed) = original.creation.as_ref().expect("original creation seed");
                assert_eq!((*identity, *reference), (*task, payload_ref));
                creation = Some(CreationAdmission {
                    envelope: *envelope,
                    seed: seed.clone(),
                });
            }
            payload_bytes = original.backing_bytes;
        }
        let preflight = if withdrawn {
            Ok(0)
        } else {
            let encoded = match creation.as_ref() {
                Some(CreationAdmission { envelope, seed }) => {
                    let original = seed.lock().unwrap();
                    let original = original
                        .as_ref()
                        .expect("original unconsumed creation seed");
                    novarocks_task_codec::operation::wire_size::create(
                        *envelope,
                        original.seed.fragment().content().len(),
                        original.sizing.lengths.metadata_bytes(),
                    )
                    .and_then(novarocks_task_codec::operation::wire_size::batch_item)
                }
                None => requests.encoded_protobuf_bytes(operation),
            };
            Self::validate_request(operation, encoded, transport)
        };
        let encoded_bytes = preflight.as_ref().ok().copied();
        let [effect_layout, gate, bound, result, retirement, queue] =
            actor_transport_layouts(false).expect("original admission layout");
        let custody = crate::task_execution::host::custody::storage_layouts()
            .expect("original admission custody layout");
        let layouts = [
            effect_layout,
            gate,
            bound,
            result,
            retirement,
            queue,
            custody.transport_record,
            custody.transport_registration,
            custody.exit_custody,
            custody.panic_sink,
        ];
        let quote = queued_input_quote(payload_bytes, SourceLayouts { original: &layouts })
            .expect("original queued input has a valid invoice");
        let kind = if effect.handoff().unwrap().state() == HandoffState::Withdrawn {
            WorkKind::Cleanup
        } else if control {
            WorkKind::Settlement
        } else {
            WorkKind::Business
        };
        let resources = ReservationResources::new(
            ResourceVector::default(),
            OwnedSlotNeed {
                work_headers: 1,
                completions: 1,
                captures: 1,
                ..Default::default()
            },
        );
        let guard =
            match self
                .scope
                .reserve_child_next(self.generation, self.parent, resources, kind)
            {
                Ok(guard) => guard,
                Err(
                    RuntimeError::Scope(error)
                    | RuntimeError::Capacity(CapacityError::Scope(error)),
                ) if matches!(
                    error,
                    ScopeError::Failed | ScopeError::GenerationMismatch | ScopeError::Stopped
                ) =>
                {
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
        let (work, mut capacity) = guard
            .bind(kind)
            .expect("preflighted original admission guard");
        let report = ReportSlot::reserve_next(
            &mut capacity,
            self.generation,
            1,
            [(self.observer, ConsumerKind::Observer)],
        )
        .expect("preflighted original admission report");
        let work = ReportedWork::try_new(work, capacity, report)
            .expect("same original report and admission Work");
        let bound = BoundQueue {
            key: SendKey {
                operation: operation.operation_id(),
                generation: *generation,
            },
            scope_generation: self.generation,
            control,
            quote,
            preflight,
            authority: self.authority.clone(),
            creation,
        };
        self.pending = Some(Pending {
            effect: effect.clone(),
            lane,
            work,
            bound,
            encoded_bytes,
        });
        if let Some(payload) = payload_ref {
            let original = self.payloads.get_mut(&payload).unwrap();
            original.claimed = Some(effect.protocol.id);
            if original.creation.is_some() && !original.price_reported {
                crate::metrics::task_creation::create_priced();
                original.price_reported = true;
            }
        }
        Ok(self.idle[lane].pop_front())
    }

    pub(crate) fn bind(
        &mut self,
        ticket: &EffectTicket<FrontendEffect>,
        result: Position,
    ) -> PreparedEffectJob {
        let pending = self.pending.take().expect("same original prepayment");
        assert!(
            matches!(&ticket.effect, FrontendEffect::Attempt(effect) if Arc::ptr_eq(effect, &pending.effect))
        );
        let header = AdmissionHeader {
            effect: pending.effect.clone(),
            original: pending.work.work().retirement_observer(),
            transport_retired: false,
            encoded_bytes: pending.encoded_bytes,
        };
        assert!(
            self.headers
                .insert(pending.effect.protocol.id, header)
                .is_none()
        );
        prepare_actor_transport(
            pending.effect,
            result,
            pending.work,
            VecDeque::new(),
            self.observer,
            |_| ActorTransportWork::Admit(Box::new(pending.bound)),
        )
        .unwrap_or_else(|refused| std::panic::resume_unwind(Box::new(refused)))
    }

    pub(crate) fn return_unclaimed(&mut self, result: Position) {
        let pending = self.pending.take().expect("same original prepayment");
        self.return_payload_claim(&pending.effect);
        let header = pending.work.work().retirement_observer();
        let (custody, exit) = ExitCustody::from_admitted(pending.work);
        exit.revoke_unused_and_exit().expect("same unused work");
        assert!(
            custody
                .try_retire()
                .expect("unused role has no live child or producer")
        );
        assert!(
            self.scope
                .reap_observed_header(&header)
                .expect("reap original unused role")
        );
        self.idle[pending.lane].push_back(result);
    }

    pub(crate) fn accept(
        &mut self,
        mut result: ReturnedActorTransportResult,
        requests: &mut FrozenSendRequests,
    ) -> Result<Option<InputId>, AdmissionResultRefusal> {
        if !self
            .headers
            .get(&result.effect.protocol.id)
            .is_some_and(|header| Arc::ptr_eq(&header.effect, &result.effect))
        {
            return Err(AdmissionResultRefusal {
                result,
                cause: AdmissionResultError::WrongEffect,
            });
        }
        let EffectBody::AdmitQueuedInput {
            operation,
            generation,
            ..
        } = &result.effect.protocol.body
        else {
            return Err(AdmissionResultRefusal {
                result,
                cause: AdmissionResultError::WrongEffect,
            });
        };
        let lane = usize::from(operation.requires_control_progress());
        let operation = operation.operation_id();
        let generation = *generation;
        let publication = match result.outcome.as_ref() {
            Some(BodyOutcome::InputAdmitted { .. }) => {
                match self.publish_grant(&mut result, requests) {
                    Ok(published) => published,
                    Err(cause) => return Err(AdmissionResultRefusal { result, cause }),
                }
            }
            Some(BodyOutcome::Unused(_)) => result
                .withdrawal
                .take()
                .expect("original withdrawal delivery"),
            Some(BodyOutcome::LocalFailure(_) | BodyOutcome::CreationFailed(_)) => {
                let (error, kind) = match result.outcome.take().unwrap() {
                    BodyOutcome::LocalFailure(error) => (
                        TaskExecutionError::Workload(error),
                        HostFailureKind::Workload,
                    ),
                    BodyOutcome::CreationFailed(error) => (error, HostFailureKind::Execution),
                    _ => unreachable!(),
                };
                let original =
                    HostFailureRef(NonZeroU64::new(result.effect.protocol.id.get()).unwrap());
                let input = error.preparation_input();
                let failure = AttemptFailure::Host(HostProjectedFailure {
                    original,
                    kind,
                    class: input.attempt_class(),
                    site: AttemptFailureSite::Attempt,
                });
                let fact = ExecutorFact::QueuedInputFailed {
                    operation,
                    generation,
                    failure,
                };
                assert!(
                    self.failures
                        .lock()
                        .unwrap()
                        .insert(original, error)
                        .is_none()
                );
                result
                    .original
                    .fill_with_receipt(
                        None,
                        FrontendInput::Attempt(Box::new(AttemptInput::Executor(fact))),
                    )
                    .map(|(_, input)| input)
            }
            _ => {
                return Err(AdmissionResultRefusal {
                    result,
                    cause: AdmissionResultError::MissingOutcome,
                });
            }
        };
        let input = match publication {
            Ok(input) => Some(input),
            Err(cause)
                if matches!(
                    cause.error,
                    ActorTurnError::ActorFailed | ActorTurnError::Closed
                ) =>
            {
                // The original admission body has joined. Its rejected event
                // is passive, and the grant/payload remain with this Source.
                assert!(cause.predecessor.is_none() && cause.predecessor_input.is_none());
                drop(cause);
                None
            }
            Err(cause) => {
                return Err(AdmissionResultRefusal {
                    result,
                    cause: AdmissionResultError::Fill(cause),
                });
            }
        };
        self.return_payload_claim(&result.effect);
        self.results.push_back((lane, result));
        Ok(input)
    }

    fn publish_grant(
        &mut self,
        result: &mut ReturnedActorTransportResult,
        requests: &mut FrozenSendRequests,
    ) -> Result<Result<InputId, CausalFillFailure<FrontendInput>>, AdmissionResultError> {
        let EffectBody::AdmitQueuedInput {
            operation,
            generation,
            payload,
            ..
        } = &result.effect.protocol.body
        else {
            unreachable!("same original admission effect");
        };
        let Some(BodyOutcome::InputAdmitted { creation, input }) = result.outcome.as_mut() else {
            unreachable!("same original admission outcome");
        };
        let key = SendKey {
            operation: operation.operation_id(),
            generation: *generation,
        };
        if input.key() != key || generation.admission_effect() != result.effect.protocol.id {
            return Err(AdmissionResultError::WrongEffect);
        }
        let grant = QueuedInputGrantRef::new(
            NonZeroU64::new(result.effect.protocol.id.get()).unwrap(),
            *generation,
            self.headers[&result.effect.protocol.id]
                .encoded_bytes
                .expect("only the exact preflighted input can publish a grant"),
        );
        if self.grants.contains(grant) {
            return Err(AdmissionResultError::DuplicateGrant);
        }
        match *payload {
            QueuedInputPayload::NewCreation(reference) => {
                let Some(original) = creation.as_ref() else {
                    return Err(AdmissionResultError::MissingPayload);
                };
                if !matches!(operation, OperationIntent::CreateTask { envelope, identity, creation, .. }
                    if *envelope == original.envelope() && *identity == original.identity()
                        && *creation == reference && operation.queued_bytes() == original.queued_bytes())
                {
                    return Err(AdmissionResultError::WrongEffect);
                }
                if !requests.install_creation(reference, key.operation, creation) {
                    return Err(AdmissionResultError::DuplicateCreation);
                }
            }
            QueuedInputPayload::NewTaskUpdate(reference) => {
                let row = self
                    .payloads
                    .get_mut(&reference)
                    .filter(|row| row.pin.is_none())
                    .ok_or(AdmissionResultError::MissingPayload)?;
                row.pin = Some(
                    input
                        .retain_payload()
                        .map_err(AdmissionResultError::PayloadRetention)?,
                );
            }
            QueuedInputPayload::AlreadyRetained => {}
        }
        #[cfg(test)]
        if matches!(operation, OperationIntent::CreateTask { .. })
            && let Some(observer) = self.create_grant_observer.as_mut()
        {
            let original = requests
                .creation_for_test(key.operation)
                .expect("the paid original is installed before publication");
            let bytes = input.retain_payload().unwrap().capacity_bytes().unwrap();
            observer(key, bytes, &original);
        }
        let Some(BodyOutcome::InputAdmitted {
            input,
            creation: None,
        }) = result.outcome.take()
        else {
            unreachable!("same original grant after transferring its payload");
        };
        self.grants.install(grant, input);
        let event = FrontendInput::Attempt(Box::new(AttemptInput::Executor(
            ExecutorFact::QueuedInputGranted {
                operation: key.operation,
                grant,
                queued_bytes: operation.queued_bytes(),
            },
        )));
        Ok(result
            .original
            .fill_with_receipt(None, event)
            .map(|(_, input)| input))
    }

    fn return_payload_claim(&mut self, effect: &AttemptEffectEnvelope) {
        let EffectBody::AdmitQueuedInput { payload, .. } = effect.protocol.body else {
            unreachable!("same queued input effect");
        };
        if let Some(payload) = Self::payload_ref(payload)
            && let Some(original) = self.payloads.get_mut(&payload)
            && original.claimed == Some(effect.protocol.id)
        {
            original.claimed = None;
        }
    }

    pub(crate) fn awaiting_result_retirement(&self) -> usize {
        self.results.len()
    }

    /// Acknowledges actual transport retirement for this exact job.
    pub(crate) fn transport_retired(&mut self, effect: AttemptActionId) -> bool {
        let Some(header) = self.headers.get_mut(&effect) else {
            return false;
        };
        assert!(!header.transport_retired);
        header.transport_retired = true;
        true
    }

    /// Inspect one original result. A busy input does not block the next
    /// result, and a scheduler commit is never counted as producer exit.
    pub(crate) fn retire_result(&mut self) -> Result<bool, ActorTurnError> {
        let Some((lane, result)) = self.results.pop_front() else {
            return Ok(false);
        };
        let effect = result.effect.protocol.id;
        let header = self.headers.get(&effect).expect("same original job header");
        let mut disposed = false;
        let ready = (|| {
            if !header.transport_retired {
                return Ok(false);
            }
            if !result.original.retire_ready_after_exit()? {
                match result.original.take_failed_payload() {
                    Ok(Some(original)) => {
                        original.dispose()?;
                        disposed = true;
                    }
                    Ok(None)
                    | Err(ActorTurnError::ActorNotRetired | ActorTurnError::InputAuthorMismatch) => {
                    }
                    Err(error) => return Err(error),
                }
                return Ok(false);
            }
            self.scope
                .reap_observed_header(&header.original)
                .map_err(ActorTurnError::from)
        })();
        match ready {
            Ok(true) => {
                self.headers.remove(&effect);
                self.idle[lane].push_back(result.original);
                Ok(true)
            }
            pending => {
                self.results.push_back((lane, result));
                pending.map(|_| disposed)
            }
        }
    }

    pub(crate) fn close_one_idle(&mut self) -> Result<bool, ActorTurnError> {
        let Some(roles) = self.idle.iter_mut().find(|roles| !roles.is_empty()) else {
            return Ok(false);
        };
        roles.front_mut().unwrap().close_after_exit()?;
        roles.pop_front();
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task_execution::error::CapacityBound;
    use novarocks_execution_contract::{OperationEnvelope, OperationKind, TaskOperationId};
    use novarocks_task_codec::{TransportBudget, TransportMessageLimits};

    fn operation(queued_bytes: usize) -> OperationIntent {
        let fixture = crate::task_execution::tests::manifest_unstarted_regression_fixture();
        OperationIntent::CreateTask {
            envelope: OperationEnvelope::with_default_wait(
                TaskOperationId::new_v7(),
                OperationKind::CreateTask,
            ),
            identity: fixture.execution().graph().root_identity(),
            creation: PayloadRef(NonZeroU64::MIN),
            queued_bytes,
        }
    }

    fn transport() -> TransportBudget {
        TransportBudget::new(
            1,
            255,
            TransportMessageLimits {
                ordinary_request_max_bytes: 1024,
                control_request_max_bytes: 1024,
                ..TransportMessageLimits::DEFAULT
            },
            1,
            256 * 1024 * 1024,
            2,
            512 * 1024 * 1024,
            1,
            2,
            std::time::Duration::from_secs(1),
        )
        .unwrap()
    }

    #[test]
    fn queued_input_preflight_checks_method_bytes_separately_from_queue_shape() {
        let transport = transport();
        let request = operation(512);
        assert_eq!(
            AdmissionSource::validate_request(&request, Ok(1024), transport),
            Ok(1024)
        );
        assert_eq!(
            AdmissionSource::validate_request(&request, Ok(1025), transport),
            Err(TaskExecutionError::Capacity(
                CapacityBound::OperationBytes {
                    limit: 1024,
                    actual: 1025,
                }
            ))
        );
        let limit = crate::task_execution::intent::NativeRequestWindows::for_transport(transport)
            .unwrap()
            .empty_queue_capacity(false);
        assert_eq!(
            AdmissionSource::validate_request(&operation(limit + 1), Ok(1024), transport),
            Err(TaskExecutionError::Capacity(
                CapacityBound::OperationBytes {
                    limit,
                    actual: limit + 1,
                }
            ))
        );
    }
}
