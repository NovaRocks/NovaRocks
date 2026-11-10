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

//! One original Control position and submission holder per frozen participation.

use crate::{
    control_runtime::actor::{FrontendActor, FrontendInput},
    task_execution::{
        abort_effect::{
            NativeAbortEffect, NativeAbortEffectAckError, NativeLateAbortEffect, closing_receipt,
        },
        host::transport::SendKey,
        intent::{OperationAcknowledgement, OperationIntent},
    },
};
use novarocks_attempt_protocol::{
    failure::AttemptFailure,
    input::{AttemptInput, SendGeneration},
};
use novarocks_control_runtime::actor_turn::{
    ActorRegistration, ActorTurnError, StandingInputObligation,
};
use novarocks_control_runtime_core::identity::InputId;
use novarocks_coordination_contract::{
    AbortIssueSettlement, AttemptActionId, AttemptActivationIdentity, AttemptToLogical,
    AttemptToLogicalBody, LogicalToAttempt, LogicalToAttemptBody,
};
use novarocks_execution_contract::{OperationKind, ParticipationRef, TaskOperationId};
use novarocks_query_application::coordination::{
    AbortParticipationEffectAdmission, AbortParticipationEffectPort,
    AbortParticipationEffectReservation, AbortParticipationEffectSubmission,
    AbortParticipationIssueIdentity, ParticipationStandDownError,
};
use std::{
    collections::BTreeMap,
    fmt,
    num::NonZeroU64,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::sync::watch;

type Position = StandingInputObligation<FrontendActor>;

struct Submitted {
    action: AttemptActionId,
    input: InputId,
    original: NativeAbortEffect,
}

struct Slot {
    position: Position,
    booked: AtomicBool,
    submitted: Mutex<Option<Submitted>>,
}

struct Ingress {
    registration: ActorRegistration,
    activation: AttemptActivationIdentity,
    slots: BTreeMap<ParticipationRef, Slot>,
    next_action: AtomicU64,
    closed: AtomicBool,
    capacity: watch::Sender<u64>,
    worker_closed: Option<Arc<dyn Fn(ParticipationRef) + Send + Sync>>,
}

/// The original QA submission remains here even after the Core has processed
/// its passive input. Only its exact settlement may take that submission.
#[must_use]
pub(crate) struct AttemptAbortIngress {
    original: Arc<Ingress>,
}

impl AttemptAbortIngress {
    pub(crate) fn new(
        registration: ActorRegistration,
        activation: AttemptActivationIdentity,
        positions: impl IntoIterator<Item = (ParticipationRef, Position)>,
        capacity: watch::Sender<u64>,
        worker_closed: Option<Arc<dyn Fn(ParticipationRef) + Send + Sync>>,
    ) -> Self {
        let mut slots = BTreeMap::new();
        for (participation, position) in positions {
            assert_eq!(participation.query_execution_id(), activation.execution());
            assert!(
                position
                    .author()
                    .is_ok_and(|author| registration.matches_input_author(&author))
            );
            assert!(
                slots
                    .insert(
                        participation,
                        Slot {
                            position,
                            booked: AtomicBool::new(false),
                            submitted: Mutex::new(None),
                        }
                    )
                    .is_none()
            );
        }
        Self {
            original: Arc::new(Ingress {
                registration,
                activation,
                slots,
                next_action: AtomicU64::new(1),
                closed: AtomicBool::new(false),
                capacity,
                worker_closed,
            }),
        }
    }

    /// The exact Core-selected logical action is the authority to take this
    /// QA submission. A mismatching operation or action leaves the slot intact.
    pub(crate) fn take_submission(
        &self,
        action: AttemptActionId,
        participation: ParticipationRef,
        operation: TaskOperationId,
    ) -> Option<(NativeAbortEffect, AbortInputRetirement)> {
        self.take_matching(participation, operation, |held| held == action)
    }

    fn take_matching(
        &self,
        participation: ParticipationRef,
        operation: TaskOperationId,
        matches_action: impl FnOnce(AttemptActionId) -> bool,
    ) -> Option<(NativeAbortEffect, AbortInputRetirement)> {
        let slot = self.original.slots.get(&participation)?;
        let mut current = slot.submitted.lock().expect("original Abort submission");
        if !current.as_ref().is_some_and(|row| {
            matches_action(row.action) && row.original.intent().operation_id() == operation
        }) {
            return None;
        }
        let submitted = current.take().unwrap();
        Some((
            submitted.original,
            AbortInputRetirement {
                original: self.original.clone(),
                participation,
                input: Some(submitted.input),
                action: submitted.action,
            },
        ))
    }

    pub(crate) fn close_external(&self) {
        if !self.original.closed.swap(true, Ordering::AcqRel) {
            publish_capacity(&self.original.capacity);
        }
    }

    /// Unregister the route and join its original callers before consuming
    /// this owner. Refusal keeps every still-live position.
    pub(crate) fn close_after_exit(self) -> Result<(), Self> {
        self.close_external();
        if self
            .original
            .slots
            .values()
            .any(|slot| slot.booked.load(Ordering::Acquire))
        {
            return Err(self);
        }
        let original = Arc::try_unwrap(self.original).map_err(|original| Self { original })?;
        for mut slot in original.slots.into_values() {
            slot.position
                .close_after_exit()
                .expect("exited original Abort Control position");
        }
        Ok(())
    }
}

impl fmt::Debug for AttemptAbortIngress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AttemptAbortIngress")
            .field("activation", &self.original.activation)
            .field("participations", &self.original.slots.len())
            .finish_non_exhaustive()
    }
}

impl AbortParticipationEffectPort for AttemptAbortIngress {
    fn subscribe_capacity(&self) -> watch::Receiver<u64> {
        self.original.capacity.subscribe()
    }

    fn try_reserve(
        &self,
        identity: AbortParticipationIssueIdentity,
    ) -> AbortParticipationEffectAdmission {
        if identity.activation() != self.original.activation
            || self.original.closed.load(Ordering::Acquire)
        {
            return AbortParticipationEffectAdmission::Closed;
        }
        let Some(slot) = self.original.slots.get(&identity.participation()) else {
            return AbortParticipationEffectAdmission::Closed;
        };
        if slot
            .booked
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return AbortParticipationEffectAdmission::Backpressured;
        }
        if self.original.closed.load(Ordering::Acquire) {
            slot.booked.store(false, Ordering::Release);
            return AbortParticipationEffectAdmission::Closed;
        }
        let action =
            self.original
                .next_action
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                    next.checked_add(1)
                });
        let Ok(action) = action else {
            self.close_external();
            slot.booked.store(false, Ordering::Release);
            return AbortParticipationEffectAdmission::Closed;
        };
        AbortParticipationEffectAdmission::Admitted(Box::new(AbortReservation {
            original: self.original.clone(),
            identity,
            action: AttemptActionId::new(NonZeroU64::new(action).unwrap()),
            owns_position: true,
        }))
    }
}

struct AbortReservation {
    original: Arc<Ingress>,
    identity: AbortParticipationIssueIdentity,
    action: AttemptActionId,
    owns_position: bool,
}

impl fmt::Debug for AbortReservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AbortReservation")
            .field("identity", &self.identity)
            .field("action", &self.action)
            .finish_non_exhaustive()
    }
}

impl AbortParticipationEffectReservation for AbortReservation {
    fn submit(mut self: Box<Self>, submission: AbortParticipationEffectSubmission) {
        assert_eq!(submission.identity(), self.identity);
        let slot = &self.original.slots[&self.identity.participation()];
        let original =
            NativeAbortEffect::from_submission(submission, self.original.worker_closed.clone());
        let OperationIntent::AbortParticipation(request) = original.intent() else {
            unreachable!("original Abort intent");
        };
        let input = FrontendInput::Attempt(Box::new(AttemptInput::Port(LogicalToAttempt {
            activation: self.original.activation,
            body: LogicalToAttemptBody::Abort {
                action: self.action,
                request: *request,
            },
        })));
        // Install the original submission under the same lock its consumer
        // uses. An immediately woken Core cannot settle an unbound action.
        let mut current = slot.submitted.lock().expect("original Abort submission");
        assert!(current.is_none());
        match slot.position.fill(input) {
            Ok(input) => {
                *current = Some(Submitted {
                    action: self.action,
                    input,
                    original,
                });
                self.owns_position = false;
            }
            Err(_) => {
                drop(current);
                // The original submission's Drop sends FailedClosed to QA.
                drop(original);
            }
        }
    }
}

impl Drop for AbortReservation {
    fn drop(&mut self) {
        if self.owns_position {
            let slot = &self.original.slots[&self.identity.participation()];
            assert!(slot.booked.swap(false, Ordering::AcqRel));
            publish_capacity(&self.original.capacity);
        }
    }
}

/// Credit is returned only after this exact runtime occurrence and its
/// effects are retired. Settling the QA submission does not supply that fact.
#[must_use]
pub(crate) struct AbortInputRetirement {
    original: Arc<Ingress>,
    participation: ParticipationRef,
    input: Option<InputId>,
    action: AttemptActionId,
}

impl AbortInputRetirement {
    pub(crate) fn retire_ready(&mut self) -> Result<bool, ActorTurnError> {
        let Some(expected) = self.input else {
            return Ok(true);
        };
        let slot = &self.original.slots[&self.participation];
        if slot
            .position
            .in_flight()?
            .is_some_and(|input| input != expected)
        {
            return Err(ActorTurnError::InputAuthorMismatch);
        }
        if !slot.position.retire_ready_after_exit()? {
            return Ok(false);
        }
        self.input = None;
        assert!(slot.booked.swap(false, Ordering::AcqRel));
        publish_capacity(&self.original.capacity);
        Ok(true)
    }
}

/// Each slot belongs to the report of one physical Abort send. The sender
/// aliases its canonical acknowledgement before publishing the passive input.
pub(crate) struct AbortReceiptSlot {
    key: SendKey,
    participation: ParticipationRef,
    original: OnceLock<OperationAcknowledgement>,
}

impl AbortReceiptSlot {
    pub(crate) fn new(key: SendKey, participation: ParticipationRef) -> Self {
        Self {
            key,
            participation,
            original: OnceLock::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn original_for_test(&self) -> Option<&OperationAcknowledgement> {
        self.original.get()
    }

    pub(crate) fn record(&self, acknowledgement: &OperationAcknowledgement) {
        assert_eq!(acknowledgement.operation_id(), self.key.operation);
        assert_eq!(acknowledgement.kind(), OperationKind::AbortParticipation);
        if acknowledgement.worker_outcome().is_some() {
            assert!(self.original.set(acknowledgement.clone()).is_ok());
        }
    }
}

struct LateSubmission {
    action: AttemptActionId,
    generation: AttemptActionId,
    original: NativeLateAbortEffect,
}

#[derive(Default)]
struct SettlementRow {
    late: Option<LateSubmission>,
    released_replay: Option<(AttemptActionId, TaskOperationId)>,
}

/// Runs in the original credentialed QA settlement work, outside the poller.
/// The pending Abort and the latest late authority are bounded per participant.
pub(crate) struct AbortSettlementOwner {
    ingress: Arc<AttemptAbortIngress>,
    rows: Mutex<BTreeMap<ParticipationRef, SettlementRow>>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum AbortSettlementRefusal {
    ForeignPort,
    MissingOriginalSubmission,
    WrongReceiptGeneration,
    InvalidReceipt(NativeAbortEffectAckError),
}

#[must_use]
pub(crate) struct AppliedAbortSettlement {
    pub(crate) result: Result<(), ParticipationStandDownError>,
    pub(crate) retirement: Option<AbortInputRetirement>,
}

impl AbortSettlementOwner {
    pub(crate) fn new(ingress: Arc<AttemptAbortIngress>) -> Self {
        let rows = ingress
            .original
            .slots
            .keys()
            .map(|part| (*part, SettlementRow::default()))
            .collect();
        Self {
            ingress,
            rows: Mutex::new(rows),
        }
    }

    pub(crate) fn matches(
        &self,
        registration: &ActorRegistration,
        activation: AttemptActivationIdentity,
    ) -> bool {
        self.ingress
            .original
            .registration
            .same_registration(registration)
            && self.ingress.original.activation == activation
    }

    pub(crate) fn apply(
        &self,
        port: &AttemptToLogical<AttemptFailure>,
        receipt: Option<&AbortReceiptSlot>,
    ) -> Result<AppliedAbortSettlement, AbortSettlementRefusal> {
        use AbortIssueSettlement as S;
        let AttemptToLogicalBody::AbortSettlement {
            abort_action,
            generation,
            receipt_generation,
            participation,
            operation_id,
            settlement,
        } = port.body
        else {
            return Err(AbortSettlementRefusal::ForeignPort);
        };
        if port.activation != self.ingress.original.activation {
            return Err(AbortSettlementRefusal::ForeignPort);
        }
        let receipt = match (settlement, receipt_generation, receipt) {
            (S::Worker(outcome), Some(original_generation), Some(receipt))
                if receipt.key
                    == (SendKey {
                        operation: operation_id,
                        generation: SendGeneration::new(original_generation),
                    })
                    && receipt.participation == participation =>
            {
                let original = receipt
                    .original
                    .get()
                    .filter(|original| original.worker_outcome() == Some(outcome))
                    .ok_or(AbortSettlementRefusal::WrongReceiptGeneration)?;
                Some(
                    closing_receipt(participation, original)
                        .map_err(AbortSettlementRefusal::InvalidReceipt)?,
                )
            }
            (S::Worker(_), _, _) => return Err(AbortSettlementRefusal::WrongReceiptGeneration),
            (_, None, None) => None,
            _ => return Err(AbortSettlementRefusal::WrongReceiptGeneration),
        };
        let mut rows = self.rows.lock().expect("original Abort settlement owner");
        let row = rows
            .get_mut(&participation)
            .ok_or(AbortSettlementRefusal::ForeignPort)?;
        if let Some((original, retirement)) =
            self.ingress
                .take_submission(abort_action, participation, operation_id)
        {
            let result = match settlement {
                S::DefinitelyUnsent => original.definitely_unsent(),
                S::FailedClosed => original.fail_closed(),
                S::TransportUnknown => original.transport_unknown().map(|original| {
                    if let Some(old) = row.late.replace(LateSubmission {
                        action: abort_action,
                        generation,
                        original,
                    }) {
                        old.original.superseded_by_newer_unknown();
                    }
                }),
                S::Worker(_) => {
                    let result =
                        original.worker_settled(receipt.expect("validated original Abort receipt"));
                    if result.is_ok()
                        && let Some(old) = row.late.take()
                    {
                        old.original.resolved_by_other_generation();
                    }
                    result
                }
            };
            return Ok(AppliedAbortSettlement {
                result,
                retirement: Some(retirement),
            });
        }
        if settlement == S::DefinitelyUnsent
            && row.released_replay == Some((abort_action, operation_id))
        {
            row.released_replay = None;
            return Ok(AppliedAbortSettlement {
                result: Ok(()),
                retirement: None,
            });
        }
        if !row.late.as_ref().is_some_and(|late| {
            late.action == abort_action
                && late.generation == generation
                && late.original.identity().operation_id() == operation_id
        }) {
            return Err(AbortSettlementRefusal::MissingOriginalSubmission);
        }
        let result = match settlement {
            S::FailedClosed => row.late.take().unwrap().original.fail_closed(),
            S::Worker(_) => row
                .late
                .take()
                .unwrap()
                .original
                .worker_settled(receipt.expect("validated original Abort receipt")),
            _ => return Err(AbortSettlementRefusal::MissingOriginalSubmission),
        };
        let (result, retirement) = if result.is_ok()
            && let Some((replay, retirement)) =
                self.ingress
                    .take_matching(participation, operation_id, |action| action > abort_action)
        {
            row.released_replay = Some((retirement.action, operation_id));
            let result = if matches!(settlement, S::Worker(_)) {
                replay.resolved_by_other_generation();
                Ok(())
            } else {
                replay.fail_closed()
            };
            (result, Some(retirement))
        } else {
            (result, None)
        };
        Ok(AppliedAbortSettlement { result, retirement })
    }
}

fn publish_capacity(capacity: &watch::Sender<u64>) {
    capacity.send_modify(|epoch| {
        *epoch = epoch
            .checked_add(1)
            .expect("Abort capacity epoch exhausted");
    });
}
