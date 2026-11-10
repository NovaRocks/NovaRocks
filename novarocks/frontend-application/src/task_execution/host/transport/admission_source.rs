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

use super::{
    ActorTransportWork, BodyOutcome, BoundQueue, GrantedInputs, OriginalQueuedInput, SendKey,
    SourceLayouts, actor_transport_layouts, prepare_actor_transport, queued_input_quote,
};
use crate::{
    control_runtime::{
        actor::{FrontendActor, FrontendEffect, FrontendInput},
        host::PreparedEffectJob,
        transport_withdrawal::AttemptEffectEnvelope,
    },
    task_execution::{error::TaskExecutionError, host::custody::ReturnedActorTransportResult},
};
use novarocks_attempt_protocol::{
    effect::{EffectBody, QueuedInputPayload},
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
use novarocks_resource_contract::ExistingOwnerRetentionPin;
use novarocks_workload_control::{WorkError, joint_admission::NativeAsyncActionAuthority};
use std::{
    collections::{BTreeMap, VecDeque},
    num::NonZeroU64,
    sync::Arc,
};

type Position = CausalInputObligation<FrontendActor>;

struct PayloadCharge {
    backing_bytes: usize,
    pin: Option<ExistingOwnerRetentionPin>,
    claimed: Option<AttemptActionId>,
}

struct AdmissionHeader {
    effect: Arc<AttemptEffectEnvelope>,
    original: WorkRetirementObserver,
    transport_retired: bool,
}

struct Pending {
    effect: Arc<AttemptEffectEnvelope>,
    lane: usize,
    work: ReportedWork,
    bound: BoundQueue,
}

#[must_use = "the original result and any refused input remain in custody"]
pub(crate) struct AdmissionResultRefusal {
    pub(crate) result: ReturnedActorTransportResult,
    pub(crate) cause: AdmissionResultError,
}

pub(crate) enum AdmissionResultError {
    WrongEffect,
    MissingOutcome,
    MissingPayload,
    Publication(crate::task_execution::host::custody::AdmissionPublicationError),
    Fill(CausalFillFailure<FrontendInput>),
}

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
    failures: BTreeMap<HostFailureRef, WorkError>,
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
            failures: BTreeMap::new(),
        })
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
        });
        true
    }

    pub(crate) fn take_payload(
        &mut self,
        payload: PayloadRef,
    ) -> Option<ExistingOwnerRetentionPin> {
        self.payloads.remove(&payload)?.pin
    }

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

    pub(crate) fn take_failure(&mut self, failure: HostFailureRef) -> Option<WorkError> {
        self.failures.remove(&failure)
    }

    fn payload_ref(payload: QueuedInputPayload) -> Option<PayloadRef> {
        match payload {
            QueuedInputPayload::NewCreation(payload)
            | QueuedInputPayload::NewTaskUpdate(payload) => Some(payload),
            QueuedInputPayload::AlreadyRetained => None,
        }
    }

    /// Select and price the same pending metadata before the host claims its
    /// ticket. Full is the absence of the original purpose's result credit.
    pub(crate) fn prepay(
        &mut self,
        effect: &Arc<AttemptEffectEnvelope>,
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
        let payload_ref = Self::payload_ref(*payload);
        let payload_bytes = payload_ref.map_or(0, |payload| {
            let original = self
                .payloads
                .get(&payload)
                .expect("original frozen payload was installed");
            assert!(
                original.pin.is_none() && original.claimed.is_none(),
                "one original physical generation may price new payload backing"
            );
            original.backing_bytes
        });
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
            authority: self.authority.clone(),
            original_captures: Box::new(()),
        };
        self.pending = Some(Pending {
            effect: effect.clone(),
            lane,
            work,
            bound,
        });
        if let Some(payload) = payload_ref {
            self.payloads.get_mut(&payload).unwrap().claimed = Some(effect.protocol.id);
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
    ) -> Result<InputId, AdmissionResultRefusal> {
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
            payload,
            ..
        } = &result.effect.protocol.body
        else {
            return Err(AdmissionResultRefusal {
                result,
                cause: AdmissionResultError::WrongEffect,
            });
        };
        let lane = usize::from(operation.requires_control_progress());
        let publication = match result.outcome.as_ref() {
            Some(BodyOutcome::InputAdmitted(_)) => {
                let original = match Self::payload_ref(*payload) {
                    Some(payload) => {
                        let Some(original) = self.payloads.get_mut(&payload) else {
                            return Err(AdmissionResultRefusal {
                                result,
                                cause: AdmissionResultError::MissingPayload,
                            });
                        };
                        Some(&mut original.pin)
                    }
                    None => None,
                };
                match result.publish_admission(&mut self.grants, original) {
                    Ok(Some(published)) => published.delivery,
                    Ok(None) => unreachable!("same original admitted result"),
                    Err(cause) => {
                        return Err(AdmissionResultRefusal {
                            result,
                            cause: AdmissionResultError::Publication(cause),
                        });
                    }
                }
            }
            Some(BodyOutcome::Unused(_)) => result
                .withdrawal
                .take()
                .expect("original withdrawal delivery"),
            Some(BodyOutcome::LocalFailure(error)) => {
                let original =
                    HostFailureRef(NonZeroU64::new(result.effect.protocol.id.get()).unwrap());
                let input = TaskExecutionError::Workload(error.clone()).preparation_input();
                let failure = AttemptFailure::Host(HostProjectedFailure {
                    original,
                    kind: HostFailureKind::Workload,
                    class: input.attempt_class(),
                    site: AttemptFailureSite::Attempt,
                });
                let fact = ExecutorFact::QueuedInputFailed {
                    operation: operation.operation_id(),
                    generation: *generation,
                    failure,
                };
                let Some(BodyOutcome::LocalFailure(error)) = result.outcome.take() else {
                    unreachable!()
                };
                assert!(self.failures.insert(original, error).is_none());
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
            Ok(input) => input,
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

    fn return_payload_claim(&mut self, effect: &AttemptEffectEnvelope) {
        let EffectBody::AdmitQueuedInput { payload, .. } = effect.protocol.body else {
            unreachable!("same queued input effect");
        };
        if let Some(payload) = Self::payload_ref(payload)
            && let Some(original) = self.payloads.get_mut(&payload)
        {
            assert_eq!(original.claimed.take(), Some(effect.protocol.id));
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
        let ready = result
            .original
            .retire_ready_after_exit()
            .and_then(|input_idle| {
                if !input_idle || !header.transport_retired {
                    return Ok(false);
                }
                self.scope
                    .reap_observed_header(&header.original)
                    .map_err(ActorTurnError::from)
            });
        match ready {
            Ok(true) => {
                self.headers.remove(&effect);
                self.idle[lane].push_back(result.original);
                Ok(true)
            }
            pending => {
                self.results.push_back((lane, result));
                pending
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
