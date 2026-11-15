// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Original per-participation Control positions and physical Abort receipts.
//! The logical Core owns issue policy. This endpoint never creates a QA call,
//! logical action, transport generation, Worker closure or new Work authority.
use crate::{
    control_runtime::{
        actor::{FrontendActor, FrontendEffect, FrontendInput},
        logical::adapter::LogicalHostEffect,
    },
    task_execution::{
        host::transport::SendKey,
        intent::{AckPayload, OperationAcknowledgement},
    },
};
use novarocks_attempt_protocol::{
    input::AttemptInput,
    port_projection::{AbortReceiptFact, abort_worker_closure, project_logical_message},
};
use novarocks_control_runtime::actor_turn::{
    ActorRegistration, ActorRuntime, ActorTurnError, EffectTicket, StandingInputObligation,
};
use novarocks_control_runtime_core::identity::InputId;
use novarocks_coordination_contract::{
    AttemptActionId, AttemptActivationIdentity, logical_port::LogicalToAttemptBody,
};
use novarocks_execution_contract::{OperationKind, ParticipationRef, TaskOperationId};
use novarocks_logical_execution_protocol::effect::LogicalEffectBody;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    ops::Bound::{Excluded, Unbounded},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::watch;

/// Same original table and epoch read by failed-participation isolation.
/// Membership loss and RPC delivery never write this positive Worker fact.
#[derive(Clone)]
pub(crate) struct ClosedParticipations {
    rows: Arc<Mutex<BTreeSet<ParticipationRef>>>,
    epoch: watch::Sender<u64>,
}
impl ClosedParticipations {
    pub(crate) fn new() -> Self {
        let (epoch, _) = watch::channel(0);
        Self::from_original(Arc::new(Mutex::new(BTreeSet::new())), epoch)
    }
    pub(crate) fn from_original(
        rows: Arc<Mutex<BTreeSet<ParticipationRef>>>,
        epoch: watch::Sender<u64>,
    ) -> Self {
        Self { rows, epoch }
    }
    pub(crate) fn rows(&self) -> Arc<Mutex<BTreeSet<ParticipationRef>>> {
        self.rows.clone()
    }
    pub(crate) fn epoch(&self) -> watch::Sender<u64> {
        self.epoch.clone()
    }
    pub(crate) fn contains(&self, participation: ParticipationRef) -> bool {
        self.rows.lock().unwrap().contains(&participation)
    }
    fn record_worker_closed(&self, participation: ParticipationRef) {
        if self.rows.lock().unwrap().insert(participation) {
            self.epoch.send_modify(|epoch| {
                *epoch = epoch
                    .checked_add(1)
                    .expect("original closed-participation epoch exhausted")
            });
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AbortPortError {
    Closed,
    WrongOwner,
    WrongMessage,
    WrongActivation,
    UnknownParticipation,
    PositionLoaned,
    Runtime(ActorTurnError),
}
impl fmt::Display for AbortPortError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "original Abort port rejected delivery: {self:?}")
    }
}
impl std::error::Error for AbortPortError {}
type Position = StandingInputObligation<FrontendActor>;
struct Slot {
    position: Mutex<Option<Position>>,
}
#[must_use = "Retire the original Control positions only after their actual input and effects exit"]
pub(crate) struct AttemptAbortIngress {
    owner: ActorRegistration,
    registration: ActorRegistration,
    activation: AttemptActivationIdentity,
    slots: BTreeMap<ParticipationRef, Slot>,
    closed: AtomicBool,
}
impl AttemptAbortIngress {
    pub(crate) fn new(
        owner: ActorRegistration,
        registration: ActorRegistration,
        activation: AttemptActivationIdentity,
        positions: impl IntoIterator<Item = (ParticipationRef, Position)>,
    ) -> Self {
        let mut slots = BTreeMap::new();
        for (participation, position) in positions {
            assert_eq!(
                participation.query_execution_id(),
                activation.execution(),
                "original Abort participation activation"
            );
            assert!(
                position
                    .author()
                    .is_ok_and(|author| registration.matches_input_author(&author)),
                "original AP Control position author"
            );
            assert!(
                slots
                    .insert(
                        participation,
                        Slot {
                            position: Mutex::new(Some(position))
                        }
                    )
                    .is_none(),
                "one original Abort position per participation"
            );
        }
        Self {
            owner,
            registration,
            activation,
            slots,
            closed: AtomicBool::new(false),
        }
    }
    pub(crate) fn matches_pair(
        &self,
        owner: &ActorRegistration,
        registration: &ActorRegistration,
        activation: AttemptActivationIdentity,
    ) -> bool {
        self.activation == activation
            && self.owner.same_registration(owner)
            && self.registration.same_registration(registration)
    }
    /// Synchronous current-ticket handoff. Success is real queue insertion,
    /// never a wait for AP to consume the original Control input.
    pub(crate) fn deliver(
        &self,
        runtime: &ActorRuntime<FrontendActor>,
        ticket: &EffectTicket<FrontendEffect>,
    ) -> Result<InputId, AbortPortError> {
        if ticket.actor != self.owner.actor() {
            return Err(AbortPortError::WrongOwner);
        }
        let source = runtime
            .registration_from_effect(ticket)
            .map_err(AbortPortError::Runtime)?;
        if !self.owner.same_registration(&source) {
            return Err(AbortPortError::WrongOwner);
        }
        runtime
            .validate_registration(&self.registration)
            .map_err(AbortPortError::Runtime)?;
        if self.closed.load(Ordering::Acquire) {
            return Err(AbortPortError::Closed);
        }
        let FrontendEffect::Logical(envelope) = &ticket.effect else {
            return Err(AbortPortError::WrongMessage);
        };
        let LogicalHostEffect::Protocol(effect) = &envelope.body else {
            return Err(AbortPortError::WrongMessage);
        };
        let LogicalEffectBody::AttemptPort(message) = &effect.body else {
            return Err(AbortPortError::WrongMessage);
        };
        if message.activation != self.activation {
            return Err(AbortPortError::WrongActivation);
        }
        let LogicalToAttemptBody::Abort { request, .. } = &message.body else {
            return Err(AbortPortError::WrongMessage);
        };
        let slot = self
            .slots
            .get(&request.participation())
            .ok_or(AbortPortError::UnknownParticipation)?;
        let position = slot
            .position
            .lock()
            .unwrap()
            .take()
            .ok_or(AbortPortError::PositionLoaned)?;
        let loan = PositionLoan {
            slot,
            position: Some(position),
        };
        let position = loan.position.as_ref().unwrap();
        // The same previous occurrence may already be terminal; this probe
        // never waits, acknowledges an unconsumed input or invents Work exit.
        position
            .retire_ready_after_exit()
            .map_err(AbortPortError::Runtime)?;
        let input = FrontendInput::Attempt(Box::new(AttemptInput::Port(project_logical_message(
            message,
        ))));
        runtime
            .fill_from_effect(ticket, position, input)
            .map_err(|(error, _)| AbortPortError::Runtime(error))
    }
    pub(crate) fn close_external(&self) {
        self.closed.store(true, Ordering::Release);
    }
    pub(crate) fn retirement(self: &Arc<Self>) -> AbortIngressRetirement {
        self.close_external();
        AbortIngressRetirement {
            ingress: self.clone(),
            after: None,
        }
    }
    fn close_slot(slot: &Slot) -> Result<bool, ActorTurnError> {
        let Some(position) = slot.position.lock().unwrap().take() else {
            return Ok(false);
        };
        let mut loan = PositionLoan {
            slot,
            position: Some(position),
        };
        let original = loan.position.as_mut().unwrap();
        if !original.retire_ready_after_exit()? {
            return Ok(false);
        }
        original.close_after_exit()?;
        drop(loan.position.take());
        Ok(true)
    }
}
struct PositionLoan<'a> {
    slot: &'a Slot,
    position: Option<Position>,
}
impl Drop for PositionLoan<'_> {
    fn drop(&mut self) {
        if let Some(position) = self.position.take() {
            assert!(
                self.slot
                    .position
                    .lock()
                    .unwrap()
                    .replace(position)
                    .is_none(),
                "same original Abort Control loan returns once"
            );
        }
    }
}
pub(crate) struct AbortIngressRetirement {
    ingress: Arc<AttemptAbortIngress>,
    after: Option<ParticipationRef>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AbortIngressRetirementProgress {
    Blocked,
    Advanced,
    Complete,
}
impl AbortIngressRetirement {
    pub(crate) fn progress(&mut self) -> Result<AbortIngressRetirementProgress, ActorTurnError> {
        let next = match self.after {
            Some(after) => self
                .ingress
                .slots
                .range((Excluded(after), Unbounded))
                .next(),
            None => self.ingress.slots.first_key_value(),
        };
        let Some((&participation, slot)) = next else {
            return Ok(AbortIngressRetirementProgress::Complete);
        };
        if !AttemptAbortIngress::close_slot(slot)? {
            return Ok(AbortIngressRetirementProgress::Blocked);
        }
        self.after = Some(participation);
        Ok(AbortIngressRetirementProgress::Advanced)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AbortReceiptError {
    WrongOperation,
    WrongKind,
    DuplicateReceipt,
    ForeignActivation,
    DuplicatePhysicalGeneration,
}
/// Owned by one physical send report. The canonical acknowledgement stays in
/// this same slot through its report/input/effect retirement, including lateness.
pub(crate) struct AbortReceiptSlot {
    key: SendKey,
    participation: ParticipationRef,
    original: OnceLock<OperationAcknowledgement>,
    closed: ClosedParticipations,
}
impl AbortReceiptSlot {
    pub(crate) fn new(
        key: SendKey,
        participation: ParticipationRef,
        closed: ClosedParticipations,
    ) -> Self {
        Self {
            key,
            participation,
            original: OnceLock::new(),
            closed,
        }
    }
    pub(crate) fn record(
        &self,
        original: &OperationAcknowledgement,
    ) -> Result<(), AbortReceiptError> {
        if original.operation_id() != self.key.operation {
            return Err(AbortReceiptError::WrongOperation);
        }
        if original.kind() != OperationKind::AbortParticipation {
            return Err(AbortReceiptError::WrongKind);
        }
        if let Some(outcome) = original.worker_outcome() {
            self.original
                .set(original.clone())
                .map_err(|_| AbortReceiptError::DuplicateReceipt)?;
            if let AckPayload::Participation(receipt) = original.payload()
                && abort_worker_closure(self.participation, outcome, receipt).is_ok()
            {
                self.closed.record_worker_closed(self.participation);
            }
        }
        Ok(())
    }
    fn passive_fact(&self) -> Option<AbortReceiptFact> {
        let original = self.original.get()?;
        Some(AbortReceiptFact {
            generation: self.key.generation.admission_effect(),
            operation_id: original.operation_id(),
            kind: original.kind(),
            outcome: original.worker_outcome()?,
            receipt: match original.payload() {
                AckPayload::Participation(receipt) => Some(receipt.clone()),
                _ => None,
            },
        })
    }
}
/// Weak lookup only; canonical receipt lifetime remains with its original send
/// report. AP selects the physical generation; this table never chooses newer.
pub(crate) struct AbortReceiptTable {
    activation: AttemptActivationIdentity,
    slots: Mutex<BTreeMap<(TaskOperationId, AttemptActionId), Weak<AbortReceiptSlot>>>,
}
impl AbortReceiptTable {
    pub(crate) fn new(activation: AttemptActivationIdentity) -> Self {
        Self {
            activation,
            slots: Mutex::new(BTreeMap::new()),
        }
    }
    pub(crate) fn register(&self, slot: &Arc<AbortReceiptSlot>) -> Result<(), AbortReceiptError> {
        if slot.participation.query_execution_id() != self.activation.execution() {
            return Err(AbortReceiptError::ForeignActivation);
        }
        let mut slots = self.slots.lock().unwrap();
        let key = (slot.key.operation, slot.key.generation.admission_effect());
        if let Some(original) = slots.get(&key) {
            if original
                .upgrade()
                .is_some_and(|original| Arc::ptr_eq(&original, slot))
            {
                return Ok(());
            }
            return Err(AbortReceiptError::DuplicatePhysicalGeneration);
        }
        slots.insert(key, Arc::downgrade(slot));
        Ok(())
    }
    pub(crate) fn resolve(
        &self,
        participation: ParticipationRef,
        operation_id: TaskOperationId,
        generation: AttemptActionId,
    ) -> Option<AbortReceiptFact> {
        let key = (operation_id, generation);
        let slot = self.slots.lock().unwrap().get(&key)?.upgrade()?;
        if slot.participation != participation {
            return None;
        }
        slot.passive_fact()
    }
    pub(crate) fn retire(&self, key: SendKey) -> bool {
        let mut slots = self.slots.lock().unwrap();
        let key = (key.operation, key.generation.admission_effect());
        if slots.get(&key).is_some_and(|slot| slot.strong_count() != 0) {
            return false;
        }
        slots.remove(&key);
        true
    }
}

#[cfg(test)]
mod tests;
