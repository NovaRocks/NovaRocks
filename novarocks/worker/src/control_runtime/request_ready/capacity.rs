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

//! Capacity-tail ordering over already registered original R positions.
//! These are neutral ready addresses, never a second job or lifecycle ledger.

use super::*;
use novarocks_control_runtime_core::fair_queue::{FairQueueError, FairWorkQueue};
use std::{
    sync::atomic::Ordering,
    task::{Wake, Waker},
};

pub(super) struct CapacityWaiters {
    original: ActorRegistration,
    fifo: FairWorkQueue<Address, ()>,
    before: BTreeMap<Address, u64>,
    revision: u64,
    remaining: usize,
    rescan: bool,
}
pub(super) fn remove_capacity_locked(state: &mut ReadyState, owner: ActorId, address: Address) {
    let Some(row) = state.capacity.get_mut(&owner) else {
        return;
    };
    row.before.remove(&address);
    match row.fifo.remove_queued(address, ()) {
        Ok(()) | Err(FairQueueError::UnknownWork) => {}
        Err(_) => std::process::abort(),
    }
    if row.before.is_empty() {
        state.capacity.remove(&owner);
    }
}
struct SourceCapacityWake(Weak<ReadyShared>);
impl Wake for SourceCapacityWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let Some(shared) = self.0.upgrade() else {
            return;
        };
        shared
            .source_capacity_revision
            .fetch_update(Ordering::Release, Ordering::Relaxed, |n| n.checked_add(1))
            .unwrap_or_else(|_| std::process::abort());
        let owner = shared
            .source_capacity_owner
            .lock()
            .expect("original source capacity owner")
            .clone();
        if let Some(owner) = owner {
            match shared.runtime.notify_host_actor(&owner) {
                Ok(())
                | Err(DriverError::UnknownActor | DriverError::EffectTicketOriginMismatch) => {}
                Err(_) => std::process::abort(),
            }
        }
    }
}
impl ControlRequestReady {
    pub(super) fn bind_source_capacity_owner(
        &self,
        owner: &ActorRegistration,
    ) -> Result<(), ControlRequestError> {
        self.shared
            .runtime
            .validate_registration(owner)
            .map_err(ControlRequestError::Driver)?;
        let mut original = self
            .shared
            .source_capacity_owner
            .lock()
            .expect("original source capacity owner");
        if let Some(existing) = original.as_ref() {
            if !existing.same_registration(owner) {
                return Err(ControlRequestError::WrongRoot);
            }
        } else {
            *original = Some(owner.clone());
        }
        Ok(())
    }
    pub(crate) fn source_capacity_waker(&self) -> Waker {
        Waker::from(Arc::new(SourceCapacityWake(Arc::downgrade(&self.shared))))
    }
    pub(crate) fn source_capacity_revision(&self) -> u64 {
        self.shared.source_capacity_revision.load(Ordering::Acquire)
    }
    pub(crate) fn park_capacity(
        &self,
        slot: &Arc<RequestReadySlot>,
        before: u64,
    ) -> Result<(), ControlRequestError> {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("original R capacity addresses");
        let registered = state
            .slots
            .get(&slot.address)
            .ok_or(ControlRequestError::UnknownReadySlot)?;
        if !Weak::ptr_eq(&registered.original, &Arc::downgrade(slot)) {
            return Err(ControlRequestError::UnknownReadySlot);
        }
        let owner = registered.owner.clone();
        let limit = self.shared.limit.get();
        let row = state
            .capacity
            .entry(owner.actor())
            .or_insert_with(|| CapacityWaiters {
                original: owner.clone(),
                fifo: FairWorkQueue::new(limit, limit, 1),
                before: BTreeMap::new(),
                revision: before,
                remaining: 0,
                rescan: false,
            });
        if !row.original.same_registration(&owner) {
            return Err(ControlRequestError::WrongRoot);
        }
        if row.before.insert(slot.address, before).is_none() {
            row.fifo
                .enqueue(slot.address, ())
                .expect("original registered capacity position");
        }
        drop(state);
        if matches!(slot.address, Address::Control(_)) && self.source_capacity_revision() != before
        {
            slot.notify();
        }
        Ok(())
    }
    pub(crate) fn clear_capacity_wait(
        &self,
        slot: &Arc<RequestReadySlot>,
    ) -> Result<(), ControlRequestError> {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("original R capacity addresses");
        let registered = state
            .slots
            .get(&slot.address)
            .ok_or(ControlRequestError::UnknownReadySlot)?;
        if !Weak::ptr_eq(&registered.original, &Arc::downgrade(slot)) {
            return Err(ControlRequestError::UnknownReadySlot);
        }
        let actor = registered.owner.actor();
        remove_capacity_locked(&mut state, actor, slot.address);
        Ok(())
    }
    /// Retry at most one frozen FIFO round per actual physical-tail revision.
    /// Busy by itself never requeues a runnable R or resets the current pass.
    pub(crate) fn retry_capacity(
        &self,
        owner: &ActorRegistration,
        revision: u64,
        budget: usize,
    ) -> Result<bool, ControlRequestError> {
        self.shared
            .runtime
            .validate_registration(owner)
            .map_err(ControlRequestError::Driver)?;
        let mut state = self.shared.state.lock().expect("original R capacity retry");
        let Some(row) = state.capacity.get_mut(&owner.actor()) else {
            return Ok(false);
        };
        if !row.original.same_registration(owner) {
            return Err(ControlRequestError::WrongRoot);
        }
        if revision != row.revision {
            if row.remaining == 0 {
                row.revision = revision;
                row.remaining = row.fifo.len();
                row.rescan = false;
            } else {
                row.rescan = true;
            }
        }
        let mut addresses = Vec::new();
        for _ in 0..budget {
            if row.remaining == 0 {
                break;
            }
            let Some((address, ())) = row.fifo.pop() else {
                row.remaining = 0;
                break;
            };
            row.fifo
                .finish(address, ())
                .expect("same original R capacity position");
            row.remaining -= 1;
            if row.before[&address] < revision {
                addresses.push(address);
            }
            row.fifo
                .enqueue(address, ())
                .expect("same original parked R");
        }
        if row.remaining == 0 && row.rescan {
            row.revision = revision;
            row.remaining = row.fifo.len();
            row.rescan = false;
        }
        let remaining = row.remaining > 0;
        let slots: Vec<_> = addresses
            .into_iter()
            .filter_map(|address| state.slots.get(&address).and_then(|r| r.original.upgrade()))
            .collect();
        drop(state);
        for slot in slots {
            slot.notify();
        }
        if remaining {
            self.shared
                .runtime
                .notify_host_actor(owner)
                .map_err(ControlRequestError::Driver)?;
        }
        Ok(remaining)
    }
}
