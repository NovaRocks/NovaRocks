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

use super::{actors::WorkerActor, control_request::ControlRequestError};
use novarocks_control_runtime::{
    driver::ActorRuntime, notify::Notify, publication::PublishedReply,
};
use novarocks_control_runtime_core::identity::{InputId, WorkId};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
    ops::Bound::{Excluded, Unbounded},
    sync::{Arc, Mutex, Weak},
};

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum Address {
    Control(InputId),
    // Context inputs have actor-local IDs. Only the original Root request
    // Work identifies their actual process-wide ingress position here.
    Context(WorkId),
}
struct ReadyState {
    slots: BTreeMap<Address, Weak<RequestReadySlot>>,
    control_ready: BTreeSet<InputId>,
    context_ready: BTreeSet<WorkId>,
    control_cursor: Option<InputId>,
    context_cursor: Option<WorkId>,
}
struct ReadyShared {
    limit: NonZeroUsize,
    state: Mutex<ReadyState>,
    changed: Notify,
    runtime: ActorRuntime<WorkerActor>,
}
/// One actual R limit covers both address domains. Registration grants no
/// funds, authorizes no work and records no second lifecycle decision.
#[derive(Clone)]
pub(crate) struct ControlRequestReady {
    shared: Arc<ReadyShared>,
}
impl ControlRequestReady {
    pub(crate) fn new(limit: NonZeroUsize, runtime: ActorRuntime<WorkerActor>) -> Self {
        Self {
            shared: Arc::new(ReadyShared {
                limit,
                state: Mutex::new(ReadyState {
                    slots: BTreeMap::new(),
                    control_ready: BTreeSet::new(),
                    context_ready: BTreeSet::new(),
                    control_cursor: None,
                    context_cursor: None,
                }),
                changed: Notify::default(),
                runtime,
            }),
        }
    }
    pub(super) fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }
    pub(super) fn limit(&self) -> NonZeroUsize {
        self.shared.limit
    }
    pub(crate) fn register_control(
        &self,
        input: InputId,
    ) -> Result<Arc<RequestReadySlot>, ControlRequestError> {
        self.register(Address::Control(input))
    }
    pub(crate) fn register_context(
        &self,
        work: WorkId,
    ) -> Result<Arc<RequestReadySlot>, ControlRequestError> {
        self.register(Address::Context(work))
    }
    fn register(&self, address: Address) -> Result<Arc<RequestReadySlot>, ControlRequestError> {
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
        });
        state.slots.insert(address, Arc::downgrade(&slot));
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
            .is_some_and(|original| Weak::ptr_eq(original, &Arc::downgrade(slot)))
        {
            return Err(ControlRequestError::UnknownReadySlot);
        }
        state.slots.remove(&slot.address);
        match slot.address {
            Address::Control(input) => {
                state.control_ready.remove(&input);
            }
            Address::Context(work) => {
                state.context_ready.remove(&work);
            }
        }
        Ok(())
    }
    /// A wake proves only that this actual index should be rechecked.
    pub(crate) fn change_version(&self) -> u64 {
        self.shared.changed.version()
    }
    /// Consume at most one Control address, without visiting Context entries.
    pub(crate) fn next_ready(&self) -> Option<InputId> {
        let mut state = self.shared.state.lock().expect("request ready index");
        let next = state
            .control_cursor
            .and_then(|cursor| {
                state
                    .control_ready
                    .range((Excluded(cursor), Unbounded))
                    .next()
                    .copied()
            })
            .or_else(|| state.control_ready.first().copied())?;
        state.control_ready.remove(&next);
        state.control_cursor = Some(next);
        Some(next)
    }
    /// Consume at most one original Root Work address, independently of the
    /// other domain's ready population and wrap cursor.
    pub(crate) fn next_context_ready(&self) -> Option<WorkId> {
        let mut state = self.shared.state.lock().expect("request ready index");
        let next = state
            .context_cursor
            .and_then(|cursor| {
                state
                    .context_ready
                    .range((Excluded(cursor), Unbounded))
                    .next()
                    .copied()
            })
            .or_else(|| state.context_ready.first().copied())?;
        state.context_ready.remove(&next);
        state.context_cursor = Some(next);
        Some(next)
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
/// Keeping it alive does not retain its index, request, payload or runtime.
pub(crate) struct RequestReadySlot {
    shared: Weak<ReadyShared>,
    address: Address,
}
impl RequestReadySlot {
    pub(crate) fn belongs_to(&self, index: &ControlRequestReady) -> bool {
        Weak::ptr_eq(&self.shared, &Arc::downgrade(&index.shared))
    }
    pub(crate) fn notify(self: &Arc<Self>) {
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        let mut state = shared.state.lock().expect("request ready index");
        if !state
            .slots
            .get(&self.address)
            .is_some_and(|original| Weak::ptr_eq(original, &Arc::downgrade(self)))
        {
            return;
        }
        match self.address {
            Address::Control(input) => {
                state.control_ready.insert(input);
            }
            Address::Context(work) => {
                state.context_ready.insert(work);
            }
        }
        drop(state);
        // No application callback or asynchronous Waker runs under the index.
        shared.changed.notify();
        shared.runtime.notify_pollers();
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
