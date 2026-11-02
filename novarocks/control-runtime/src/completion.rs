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

//! Completion storage has explicit production and consumption legs.

mod actor_consumer;
pub use actor_consumer::ActorReportConsumer;

use crate::transaction::{LedgerState, RuntimeError, RuntimeLedger, ScopeHandle};
use novarocks_control_runtime_core::{
    completion::{CompletionError, CompletionSlot, ConsumerKind, ReturnState},
    history::HistoryFact,
    identity::{ConsumerId, Generation, ReservationId, SlotId, WorkId},
    owned_storage::{OwnedPosition, OwnedSlotKind, OwnedStorageError},
};

pub(crate) mod sealed {
    pub trait Sealed {}
}
/// Only an original unbound guard or unique reservation bundle can supply the
/// actual completion position. Raw Scope/Work numbers never implement this.
pub trait ReportStorageOwner: sealed::Sealed {
    #[doc(hidden)]
    fn report_storage(&self) -> (&ScopeHandle, ReservationId);
}
pub(crate) struct ReportRecord {
    value: CompletionSlot,
    storage: OwnedPosition,
}
impl ReportRecord {
    pub(crate) fn new(value: CompletionSlot, storage: OwnedPosition) -> Self {
        assert_eq!(storage.kind(), OwnedSlotKind::Completion);
        Self { value, storage }
    }
    pub(crate) fn storage_reservation(&self) -> Option<ReservationId> {
        self.storage.reservation()
    }
}
impl std::ops::Deref for ReportRecord {
    type Target = CompletionSlot;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
impl std::ops::DerefMut for ReportRecord {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
#[derive(Clone)]
pub struct ReportSlot {
    pub(crate) runtime: RuntimeLedger,
    pub(crate) id: SlotId,
}
impl ReportSlot {
    /// Numeric slot identities from another ledger grant no authority here.
    pub fn same_slot(&self, other: &Self) -> bool {
        self.id == other.id && crate::sync::Arc::ptr_eq(&self.runtime.shared, &other.runtime.shared)
    }
    /// Observe the original pure payload state without granting either end
    /// another claim, take, producer exit or consumer acknowledgement.
    pub fn owned_return_state(&self) -> Result<ReturnState, RuntimeError> {
        Ok(self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock")
            .slots
            .get(&self.id)
            .ok_or(RuntimeError::UnknownSlot)?
            .return_state())
    }
    /// Claim this original paid report's payload exactly once, before its
    /// bound producer starts or captures exit. Cloning the report grants no
    /// second claim. Scope close/failure does not revoke original ownership.
    pub fn claim_owned_return<T: Send + 'static>(
        &self,
    ) -> Result<
        (
            crate::owned_return::OwnedReturnSender<T>,
            crate::owned_return::OwnedReturnReader<T>,
        ),
        RuntimeError,
    > {
        crate::owned_return::claim(self.clone())
    }
    pub fn reserve(
        owner: &mut impl ReportStorageOwner,
        id: SlotId,
        generation: Generation,
        max_consumers: usize,
        consumers: impl IntoIterator<Item = (ConsumerId, ConsumerKind)>,
    ) -> Result<Self, RuntimeError> {
        Self::reserve_inner(owner, Some(id), generation, max_consumers, consumers)
    }
    /// Claims one original completion position and its actual report under the
    /// same ledger transaction. Iterator execution stays outside every lock.
    pub fn reserve_next(
        owner: &mut impl ReportStorageOwner,
        generation: Generation,
        max_consumers: usize,
        consumers: impl IntoIterator<Item = (ConsumerId, ConsumerKind)>,
    ) -> Result<Self, RuntimeError> {
        Self::reserve_inner(owner, None, generation, max_consumers, consumers)
    }
    fn reserve_inner(
        owner: &mut impl ReportStorageOwner,
        explicit: Option<SlotId>,
        generation: Generation,
        max_consumers: usize,
        consumers: impl IntoIterator<Item = (ConsumerId, ConsumerKind)>,
    ) -> Result<Self, RuntimeError> {
        let read_limit = max_consumers
            .checked_add(1)
            .ok_or(RuntimeError::IdentityExhausted)?;
        let consumers: Vec<_> = consumers.into_iter().take(read_limit).collect();
        let (scope, reservation) = owner.report_storage();
        let mut state = scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let funding = state
            .ledger
            .reservation(reservation)
            .ok_or(novarocks_control_runtime_core::capacity::CapacityError::UnknownReservation)?;
        if funding.scope != scope.id {
            return Err(OwnedStorageError::WrongOwner.into());
        }
        if funding.owned_slots().completions == 0 {
            return Err(OwnedStorageError::MissingPosition(OwnedSlotKind::Completion).into());
        }
        let producer = funding.work;
        let scope_state = state
            .scopes
            .get(&scope.id)
            .ok_or(RuntimeError::UnknownScope)?;
        if scope_state.generation() != generation {
            return Err(
                novarocks_control_runtime_core::scope::ScopeError::GenerationMismatch.into(),
            );
        }
        if scope_state.work(producer).is_none() {
            return Err(novarocks_control_runtime_core::scope::ScopeError::UnknownWork.into());
        }
        let id = match explicit {
            Some(id) => {
                if state.slots.contains_key(&id) {
                    return Err(RuntimeError::DuplicateSlot);
                }
                if scope_state
                    .next_slot_id()
                    .map_or(true, |next| id.get() < next.get())
                {
                    return Err(
                        novarocks_control_runtime_core::scope::ScopeError::DuplicateSlot.into(),
                    );
                }
                id
            }
            None => state.next_slot_id(scope.id)?,
        };
        let value =
            CompletionSlot::new(id, scope.id, generation, producer, max_consumers, consumers)?;
        state.reserve_slot(scope.id, id)?;
        let position = state
            .ledger
            .take_owned(reservation, OwnedSlotKind::Completion)
            .expect("preflighted original completion position");
        let previous = state.slots.insert(id, ReportRecord::new(value, position));
        assert!(
            previous.is_none(),
            "actual original report position is unique"
        );
        scope.runtime.shared.record(|| HistoryFact::SlotReserved {
            scope: scope.id,
            slot: id,
        });
        drop(state);
        scope.runtime.shared.changed.notify();
        Ok(Self {
            runtime: scope.runtime.clone(),
            id,
        })
    }
    pub(crate) fn validate_fill_for(
        &self,
        runtime: &RuntimeLedger,
        state: &LedgerState,
        scope: novarocks_control_runtime_core::identity::ScopeId,
        work: WorkId,
    ) -> Result<(), RuntimeError> {
        if !crate::sync::Arc::ptr_eq(&self.runtime.shared, &runtime.shared) {
            return Err(RuntimeError::UnknownSlot);
        }
        let slot = state.slots.get(&self.id).ok_or(RuntimeError::UnknownSlot)?;
        if slot.scope() != scope || slot.producer() != work {
            return Err(RuntimeError::UnknownSlot);
        }
        if slot.state() != novarocks_control_runtime_core::completion::SlotState::Reserved {
            return Err(
                novarocks_control_runtime_core::completion::CompletionError::AlreadyFilled.into(),
            );
        }
        Ok(())
    }
    pub(crate) fn fill_validated(&self, state: &mut LedgerState) {
        state
            .slots
            .get_mut(&self.id)
            .expect("validated report storage")
            .fill()
            .expect("validated report is filled exactly once");
        self.runtime
            .shared
            .record(|| HistoryFact::SlotFilled { slot: self.id });
    }
    pub const fn id(&self) -> SlotId {
        self.id
    }
    /// The original declared consumer kind, without granting consumption.
    pub fn consumer_kind(&self, consumer: ConsumerId) -> Result<ConsumerKind, RuntimeError> {
        let state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        state
            .slots
            .get(&self.id)
            .ok_or(RuntimeError::UnknownSlot)?
            .consumer_kind(consumer)
            .ok_or(RuntimeError::Completion(CompletionError::UnknownConsumer))
    }
    /// Observe this original report's consumer leg. Reading metadata grants
    /// no publication, consumption, producer exit or retirement authority.
    pub fn consumption(
        &self,
        consumer: novarocks_control_runtime_core::identity::ConsumerId,
    ) -> Result<novarocks_control_runtime_core::completion::Consumption, RuntimeError> {
        self.runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock")
            .slots
            .get(&self.id)
            .ok_or(RuntimeError::UnknownSlot)?
            .consumption(consumer)
            .ok_or(RuntimeError::Completion(
                novarocks_control_runtime_core::completion::CompletionError::UnknownConsumer,
            ))
    }
    pub fn fill(&self) -> Result<(), RuntimeError> {
        let mut state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        state
            .slots
            .get_mut(&self.id)
            .ok_or(RuntimeError::UnknownSlot)?
            .fill()?;
        self.runtime
            .shared
            .record(|| HistoryFact::SlotFilled { slot: self.id });
        drop(state);
        self.runtime.shared.changed.notify();
        Ok(())
    }
    pub fn consume(&self, consumer: ConsumerId) -> Result<(), RuntimeError> {
        let mut state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        state
            .slots
            .get_mut(&self.id)
            .ok_or(RuntimeError::UnknownSlot)?
            .consume(consumer)?;
        self.runtime
            .shared
            .record(|| HistoryFact::CompletionConsumed {
                slot: self.id,
                consumer,
            });
        drop(state);
        self.runtime.shared.changed.notify();
        Ok(())
    }
    pub fn abandon_failed_owner(
        &self,
        consumer: ConsumerId,
        owner: &ScopeHandle,
        failed_generation: Generation,
    ) -> Result<(), RuntimeError> {
        if !crate::sync::Arc::ptr_eq(&self.runtime.shared, &owner.runtime.shared) {
            return Err(RuntimeError::UnknownScope);
        }
        let mut state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let scope = state
            .scopes
            .get(&owner.id)
            .ok_or(RuntimeError::UnknownScope)?;
        if !scope.failed() || scope.generation() <= failed_generation {
            return Err(RuntimeError::Scope(
                novarocks_control_runtime_core::scope::ScopeError::GenerationMismatch,
            ));
        }
        state
            .slots
            .get_mut(&self.id)
            .ok_or(RuntimeError::UnknownSlot)?
            .abandon_owner(consumer, owner.id, failed_generation)?;
        self.runtime.shared.record(|| HistoryFact::SlotAbandoned {
            slot: self.id,
            consumer,
        });
        drop(state);
        self.runtime.shared.changed.notify();
        Ok(())
    }
    pub fn body_exited(&self) -> Result<(), RuntimeError> {
        self.runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock")
            .slots
            .get_mut(&self.id)
            .ok_or(RuntimeError::UnknownSlot)?
            .observe_body_exit();
        self.runtime.shared.changed.notify();
        Ok(())
    }
    pub fn captures_exited(&self) -> Result<(), RuntimeError> {
        self.runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock")
            .slots
            .get_mut(&self.id)
            .ok_or(RuntimeError::UnknownSlot)?
            .observe_captures_exit();
        self.runtime.shared.changed.notify();
        Ok(())
    }
    pub fn revoke_unused(&self) -> Result<(), RuntimeError> {
        self.runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock")
            .slots
            .get_mut(&self.id)
            .ok_or(RuntimeError::UnknownSlot)?
            .revoke_unused()?;
        self.runtime.shared.changed.notify();
        Ok(())
    }
    /// Observe the original report's real producer and consumer exits. This
    /// does not retire its slot, Work header or any actual child Work.
    pub fn retirement_ready(&self) -> Result<bool, RuntimeError> {
        let state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        Ok(state
            .slots
            .get(&self.id)
            .ok_or(RuntimeError::UnknownSlot)?
            .retirable())
    }
    pub fn try_retire(&self) -> Result<bool, RuntimeError> {
        let mut state = self
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let slot = state.slots.get(&self.id).ok_or(RuntimeError::UnknownSlot)?;
        if !slot.retirable() {
            return Ok(false);
        }
        let scope = slot.scope();
        state
            .scopes
            .get_mut(&scope)
            .ok_or(RuntimeError::UnknownScope)?
            .retire_slot(self.id)?;
        state.slots.remove(&self.id);
        self.runtime
            .shared
            .record(|| HistoryFact::SlotRetired { slot: self.id });
        drop(state);
        self.runtime.shared.changed.notify();
        Ok(true)
    }
}
