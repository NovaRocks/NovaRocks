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

//! Bounded weak request addresses and coalesced readiness. This index holds
//! neither request bodies nor work, reply or resource-exit authority.

mod capacity;

use super::{actors::WorkerActor, control_request::ControlRequestError};
use novarocks_control_runtime::{
    driver::{ActorRegistration, ActorRuntime, DriverError},
    notify::Notify,
    publication::PublishedReply,
    transaction::ScopeHandle,
};
use novarocks_control_runtime_core::identity::{ActorId, InputId, ScopedWorkRef};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex, Weak, atomic::AtomicU64},
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Address {
    Control(InputId),
    // A Context's actual registration owns its scope-local Work addresses.
    // The registered actor ID selects its FIFO; the stored original witness
    // qualifies every fill/pop. Neither numeric field grants Work or access.
    Context { actor: ActorId, work: ScopedWorkRef },
}
struct RegisteredSlot {
    original: Weak<RequestReadySlot>,
    owner: ActorRegistration,
    queued: Option<u64>,
}
struct ReadyState {
    slots: BTreeMap<Address, RegisteredSlot>,
    control_ready: BTreeMap<u64, InputId>,
    context_ready: BTreeMap<ActorId, BTreeMap<u64, ScopedWorkRef>>,
    next_ready: u64,
    capacity: BTreeMap<ActorId, capacity::CapacityWaiters>,
}
struct ReadyShared {
    limit: NonZeroUsize,
    state: Mutex<ReadyState>,
    changed: Notify,
    runtime: ActorRuntime<WorkerActor>,
    source_capacity_revision: AtomicU64,
    source_capacity_owner: Mutex<Option<ActorRegistration>>,
}
/// One actual R limit covers both address domains. Registration grants no
/// funds, authorizes no work and records no second lifecycle decision.
#[derive(Clone)]
pub(crate) struct ControlRequestReady {
    shared: Arc<ReadyShared>,
}
impl ControlRequestReady {
    pub(super) fn same_runtime(&self, runtime: &ActorRuntime<WorkerActor>) -> bool {
        self.shared.runtime.same_runtime(runtime)
    }
    pub(crate) fn new(limit: NonZeroUsize, runtime: ActorRuntime<WorkerActor>) -> Self {
        Self {
            shared: Arc::new(ReadyShared {
                limit,
                state: Mutex::new(ReadyState {
                    slots: BTreeMap::new(),
                    control_ready: BTreeMap::new(),
                    context_ready: BTreeMap::new(),
                    next_ready: 0,
                    capacity: BTreeMap::new(),
                }),
                changed: Notify::default(),
                runtime,
                source_capacity_revision: AtomicU64::new(0),
                source_capacity_owner: Mutex::new(None),
            }),
        }
    }
    pub(super) fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
    pub(super) fn has_control_ready(&self) -> bool {
        !self
            .shared
            .state
            .lock()
            .expect("request ready index")
            .control_ready
            .is_empty()
    }
    pub(super) fn limit(&self) -> NonZeroUsize {
        self.shared.limit
    }
    pub(crate) fn register_control(
        &self,
        input: InputId,
        owner: &ActorRegistration,
    ) -> Result<Arc<RequestReadySlot>, ControlRequestError> {
        self.bind_source_capacity_owner(owner)?;
        self.register(Address::Control(input), owner, None)
    }
    pub(crate) fn register_context(
        &self,
        author: &novarocks_control_runtime::lease::WorkLease,
        owner: &ActorRegistration,
    ) -> Result<Arc<RequestReadySlot>, ControlRequestError> {
        self.shared
            .runtime
            .validate_work_author(owner, author)
            .map_err(ControlRequestError::Driver)?;
        let work = author.reference();
        self.register(
            Address::Context {
                actor: owner.actor(),
                work,
            },
            owner,
            Some(author.scope().clone()),
        )
    }
    fn register(
        &self,
        address: Address,
        owner: &ActorRegistration,
        financial_scope: Option<ScopeHandle>,
    ) -> Result<Arc<RequestReadySlot>, ControlRequestError> {
        // The real request's admitted Work/input keeps this actor resident.
        // This validation grants no additional Work or lifecycle position.
        self.shared
            .runtime
            .validate_registration(owner)
            .map_err(ControlRequestError::Driver)?;
        let mut state = self.shared.state.lock().expect("request ready index");
        if state.slots.contains_key(&address) {
            return Err(ControlRequestError::DuplicateReadySlot);
        }
        if state.slots.len() >= self.shared.limit.get() {
            return Err(ControlRequestError::ReadyFull);
        }
        let slot = Arc::new(RequestReadySlot {
            shared: Arc::downgrade(&self.shared),
            address,
            financial_scope,
        });
        state.slots.insert(
            address,
            RegisteredSlot {
                original: Arc::downgrade(&slot),
                owner: owner.clone(),
                queued: None,
            },
        );
        Ok(slot)
    }
    pub(crate) fn unregister(
        &self,
        slot: &Arc<RequestReadySlot>,
    ) -> Result<(), ControlRequestError> {
        let mut state = self.shared.state.lock().expect("request ready index");
        if !state
            .slots
            .get(&slot.address)
            .is_some_and(|registered| Weak::ptr_eq(&registered.original, &Arc::downgrade(slot)))
        {
            return Err(ControlRequestError::UnknownReadySlot);
        }
        let registered = state
            .slots
            .remove(&slot.address)
            .expect("original registered slot");
        capacity::remove_capacity_locked(&mut state, registered.owner.actor(), slot.address);
        if let Some(ordinal) = registered.queued {
            match slot.address {
                Address::Control(_) => {
                    state.control_ready.remove(&ordinal);
                }
                Address::Context { actor, .. } => {
                    let entries = state
                        .context_ready
                        .get_mut(&actor)
                        .expect("original Context ready FIFO");
                    entries.remove(&ordinal);
                    if entries.is_empty() {
                        state.context_ready.remove(&actor);
                    }
                }
            }
        }
        Ok(())
    }
    /// A wake proves only that this actual index should be rechecked.
    #[cfg(test)]
    pub(crate) fn change_version(&self) -> u64 {
        self.shared.changed.version()
    }
    /// Consume at most one Control address in actual ready-enqueue order.
    pub(crate) fn next_ready(&self) -> Option<InputId> {
        let mut state = self.shared.state.lock().expect("request ready index");
        let (ordinal, input) = state.control_ready.pop_first()?;
        let registered = state
            .slots
            .get_mut(&Address::Control(input))
            .expect("original queued control slot");
        assert_eq!(
            registered.queued,
            Some(ordinal),
            "one original queued ready mark"
        );
        registered.queued = None;
        Some(input)
    }
    /// Consume only this original Context's next Work address. Actor fairness
    /// belongs to the Common host queue; one Context cannot steal a sibling's
    /// pending return or mistake an old registration for its successor.
    pub(crate) fn next_context_ready(&self, owner: &ActorRegistration) -> Option<ScopedWorkRef> {
        let mut state = self.shared.state.lock().expect("request ready index");
        let (&ordinal, &work) = state.context_ready.get(&owner.actor())?.first_key_value()?;
        let address = Address::Context {
            actor: owner.actor(),
            work,
        };
        let registered = state
            .slots
            .get_mut(&address)
            .expect("original queued Context slot");
        if !registered.owner.same_registration(owner) {
            return None;
        }
        assert_eq!(
            registered.queued,
            Some(ordinal),
            "one original queued ready mark"
        );
        registered.queued = None;
        let entries = state
            .context_ready
            .get_mut(&owner.actor())
            .expect("original Context ready FIFO");
        entries.remove(&ordinal);
        if entries.is_empty() {
            state.context_ready.remove(&owner.actor());
        }
        Some(work)
    }
    pub(crate) fn registered(&self) -> usize {
        self.shared
            .state
            .lock()
            .expect("request ready index")
            .slots
            .len()
    }
}

/// An exact weak address, shared only as concrete notification metadata.
/// Keeping it alive does not retain its index, request, payload or Work.
pub(crate) struct RequestReadySlot {
    shared: Weak<ReadyShared>,
    address: Address,
    // Same original financial identity, without a Work/header pin or credit.
    financial_scope: Option<ScopeHandle>,
}
impl RequestReadySlot {
    /// The registered original consumer, never a reconstructed actor author.
    pub(crate) fn context_owner(self: &Arc<Self>) -> Option<ActorRegistration> {
        self.context_author()?;
        let shared = self.shared.upgrade()?;
        let state = shared.state.lock().expect("original request ready owner");
        let registered = state.slots.get(&self.address)?;
        if !Weak::ptr_eq(&registered.original, &Arc::downgrade(self)) {
            return None;
        }
        Some(registered.owner.clone())
    }
    pub(crate) fn belongs_to_financial_scope(&self, scope: &ScopeHandle) -> bool {
        self.financial_scope
            .as_ref()
            .is_some_and(|original| original.same_scope(scope))
    }
    /// Immutable original address only; it grants no Work or input authority.
    pub(crate) fn context_author(&self) -> Option<(ActorId, ScopedWorkRef)> {
        match self.address {
            Address::Context { actor, work } => Some((actor, work)),
            Address::Control(_) => None,
        }
    }
    pub(crate) fn belongs_to(&self, index: &ControlRequestReady) -> bool {
        Weak::ptr_eq(&self.shared, &Arc::downgrade(&index.shared))
    }
    pub(crate) fn notify(self: &Arc<Self>) {
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        let mut state = shared.state.lock().expect("request ready index");
        let Some(registered) = state.slots.get(&self.address) else {
            return;
        };
        if !Weak::ptr_eq(&registered.original, &Arc::downgrade(self)) {
            return;
        }
        let owner = registered.owner.clone();
        if registered.queued.is_none() {
            // This is only bounded ready ordering, never a business identity,
            // Work author or resource credit. Exhaustion cannot wrap silently.
            let ordinal = state
                .next_ready
                .checked_add(1)
                .expect("request ready ordering exhausted");
            state.next_ready = ordinal;
            state
                .slots
                .get_mut(&self.address)
                .expect("original registered slot")
                .queued = Some(ordinal);
            match self.address {
                Address::Control(input) => {
                    state.control_ready.insert(ordinal, input);
                }
                Address::Context { actor, work } => {
                    state
                        .context_ready
                        .entry(actor)
                        .or_default()
                        .insert(ordinal, work);
                }
            }
        }
        drop(state);
        // No application callback or asynchronous Waker runs under the index.
        shared.changed.notify();
        match shared.runtime.notify_host_actor(&owner) {
            Ok(()) | Err(DriverError::UnknownActor | DriverError::EffectTicketOriginMismatch) => {}
            Err(_) => std::process::abort(),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ExternalPhase {
    Waiting,
    Delivering,
    Ended,
}
/// Unique state for the external wait/delivery leg. The outer delivery owns
/// the actual body and must destroy it before calling end; this metadata does
/// not manufacture body, capture, report-consumption or Work-exit facts.
pub(crate) struct RequestExternalLeg {
    slot: Arc<RequestReadySlot>,
    phase: Mutex<ExternalPhase>,
}
impl RequestExternalLeg {
    pub(crate) fn new(slot: Arc<RequestReadySlot>) -> Self {
        Self {
            slot,
            phase: Mutex::new(ExternalPhase::Waiting),
        }
    }
    pub(crate) fn try_take<T>(&self, reply: &PublishedReply<T>) -> Option<Arc<T>> {
        let mut phase = self.phase.lock().expect("request external custody");
        if *phase != ExternalPhase::Waiting {
            return None;
        }
        let body = reply.take_once()?;
        *phase = ExternalPhase::Delivering;
        // Moving the actual Arc out neither clones nor destroys its T.
        Some(body)
    }
    pub(crate) fn end(&self) {
        let mut phase = self.phase.lock().expect("request external custody");
        let changed = *phase != ExternalPhase::Ended;
        *phase = ExternalPhase::Ended;
        drop(phase);
        if changed {
            self.slot.notify();
        }
    }
    pub(crate) fn is_ended(&self) -> bool {
        *self.phase.lock().expect("request external custody") == ExternalPhase::Ended
    }
    pub(crate) fn notify(&self) {
        self.slot.notify();
    }
    pub(crate) fn slot(&self) -> &Arc<RequestReadySlot> {
        &self.slot
    }
}

#[cfg(test)]
mod tests;
