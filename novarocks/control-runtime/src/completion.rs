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

use crate::transaction::{LedgerState, RuntimeError, RuntimeLedger, ScopeHandle};
use novarocks_control_runtime_core::{
    completion::{CompletionSlot, ConsumerKind},
    history::HistoryFact,
    identity::{ConsumerId, Generation, SlotId, WorkId},
};

#[derive(Clone)]
pub struct ReportSlot {
    runtime: RuntimeLedger,
    id: SlotId,
}
impl ReportSlot {
    pub fn reserve(
        scope: &ScopeHandle,
        id: SlotId,
        generation: Generation,
        producer: WorkId,
        max_consumers: usize,
        consumers: impl IntoIterator<Item = (ConsumerId, ConsumerKind)>,
    ) -> Result<Self, RuntimeError> {
        let slot =
            CompletionSlot::new(id, scope.id, generation, producer, max_consumers, consumers)?;
        let mut state = scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        if state.slots.contains_key(&id) {
            return Err(RuntimeError::DuplicateSlot);
        }
        let owner = state
            .scopes
            .get_mut(&scope.id)
            .ok_or(RuntimeError::UnknownScope)?;
        if owner.generation() != generation {
            return Err(
                novarocks_control_runtime_core::scope::ScopeError::GenerationMismatch.into(),
            );
        }
        if owner.work(producer).is_none() {
            return Err(novarocks_control_runtime_core::scope::ScopeError::UnknownWork.into());
        }
        state.reserve_slot(scope.id, id)?;
        state.slots.insert(id, slot);
        scope.runtime.shared.record(HistoryFact::SlotReserved {
            scope: scope.id,
            slot: id,
        });
        Ok(Self {
            runtime: scope.runtime.clone(),
            id,
        })
    }
    /// Chooses and reserves the globally unique storage identity while holding
    /// the same ledger transaction as the owner gate and actual slot binding.
    pub fn reserve_next(
        scope: &ScopeHandle,
        generation: Generation,
        producer: WorkId,
        max_consumers: usize,
        consumers: impl IntoIterator<Item = (ConsumerId, ConsumerKind)>,
    ) -> Result<Self, RuntimeError> {
        let read_limit = max_consumers
            .checked_add(1)
            .ok_or(crate::transaction::RuntimeError::IdentityExhausted)?;
        // User iterator execution is outside every ledger lock. Only this owned,
        // bounded vector of neutral identities enters the actual transaction.
        let consumers: Vec<_> = consumers.into_iter().take(read_limit).collect();
        let mut state = scope
            .runtime
            .shared
            .state
            .lock()
            .expect("runtime ledger lock");
        let owner = state
            .scopes
            .get(&scope.id)
            .ok_or(RuntimeError::UnknownScope)?;
        if owner.generation() != generation {
            return Err(
                novarocks_control_runtime_core::scope::ScopeError::GenerationMismatch.into(),
            );
        }
        if owner.work(producer).is_none() {
            return Err(novarocks_control_runtime_core::scope::ScopeError::UnknownWork.into());
        }
        let id = state.next_slot_id(scope.id)?;
        let slot =
            CompletionSlot::new(id, scope.id, generation, producer, max_consumers, consumers)?;
        state.reserve_slot(scope.id, id)?;
        let previous = state.slots.insert(id, slot);
        assert!(
            previous.is_none(),
            "monotonic process slot identity is unique"
        );
        scope.runtime.shared.record(HistoryFact::SlotReserved {
            scope: scope.id,
            slot: id,
        });
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
            .record(HistoryFact::SlotFilled { slot: self.id });
    }
    pub const fn id(&self) -> SlotId {
        self.id
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
            .record(HistoryFact::SlotFilled { slot: self.id });
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
            .record(HistoryFact::CompletionConsumed { slot: self.id });
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
        self.runtime
            .shared
            .record(HistoryFact::SlotAbandoned { slot: self.id });
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
            .record(HistoryFact::SlotRetired { slot: self.id });
        drop(state);
        self.runtime.shared.changed.notify();
        Ok(true)
    }
}
