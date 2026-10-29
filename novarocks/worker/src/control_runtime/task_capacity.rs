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

//! Capacity-tail notifications for the original Task pools. The FIFO contains
//! only Context addresses; the same Context continues to own its pending job.
//! Production and simulation use this same metadata transition and replace
//! only the actual physical-tail event which invokes `capacity_returned`.

use super::actors::WorkerActor;
use novarocks_control_runtime::driver::{ActorRegistration, ActorRuntime, DriverError};
use novarocks_control_runtime_core::{
    fair_queue::{FairQueueError, FairWorkQueue},
    identity::ActorId,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Wake, Waker},
};

#[derive(Debug)]
pub(crate) enum TaskCapacityError {
    InvalidGeometry,
    WrongOwner,
    ForeignRegistration,
    FutureRevision,
    Full,
    Driver(DriverError),
    Queue(FairQueueError),
}
struct Waiter {
    registration: ActorRegistration,
    before: u64,
}
struct Waiters {
    original: BTreeMap<ActorId, Waiter>,
    fifo: FairWorkQueue<ActorId, ()>,
    sweep_revision: u64,
    sweep_remaining: usize,
    rescan: bool,
}
struct Shared {
    runtime: ActorRuntime<WorkerActor>,
    root: ActorRegistration,
    limit: usize,
    revision: AtomicU64,
    waiters: Mutex<Waiters>,
}
/// Cloning shares only the same bounded metadata index and tail event source.
/// It grants no Work, input, original job, publication or resource capacity.
#[derive(Clone)]
pub(crate) struct TaskCapacitySignal(Arc<Shared>);

impl TaskCapacitySignal {
    /// Process composition supplies the original Root's actual directory C.
    /// `ProcessControlOwner::task_capacity_signal` derives that value directly.
    pub(crate) fn new(
        runtime: ActorRuntime<WorkerActor>,
        root: ActorRegistration,
        max_contexts: usize,
    ) -> Result<Self, TaskCapacityError> {
        if max_contexts == 0 {
            return Err(TaskCapacityError::InvalidGeometry);
        }
        runtime
            .validate_registration(&root)
            .map_err(TaskCapacityError::Driver)?;
        Ok(Self(Arc::new(Shared {
            runtime,
            root,
            limit: max_contexts,
            revision: AtomicU64::new(0),
            waiters: Mutex::new(Waiters {
                original: BTreeMap::new(),
                fifo: FairWorkQueue::new(max_contexts, max_contexts, 1),
                sweep_revision: 0,
                sweep_remaining: 0,
                rescan: false,
            }),
        })))
    }
    pub(crate) fn same_signal(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub(crate) fn belongs_to(
        &self,
        runtime: &ActorRuntime<WorkerActor>,
        root: &ActorRegistration,
    ) -> bool {
        self.0.runtime.same_runtime(runtime) && self.0.root.same_registration(root)
    }
    pub(crate) fn is_empty(&self) -> bool {
        let state = self
            .0
            .waiters
            .lock()
            .expect("original Task capacity addresses");
        state.original.is_empty() && state.fifo.is_empty()
    }
    pub(crate) fn revision(&self) -> u64 {
        self.0.revision.load(Ordering::Acquire)
    }
    /// Observe revision before trying the real pool. Register only after the
    /// same original job is returned Busy. A racing tail either marks Root
    /// itself, or this post-insertion check observes it and repeats that mark.
    pub(crate) fn register_waiter(
        &self,
        registration: &ActorRegistration,
        before: u64,
    ) -> Result<(), TaskCapacityError> {
        self.0
            .runtime
            .validate_registration(&self.0.root)
            .map_err(TaskCapacityError::Driver)?;
        self.0
            .runtime
            .validate_registration(registration)
            .map_err(TaskCapacityError::Driver)?;
        if registration.same_registration(&self.0.root) {
            return Err(TaskCapacityError::WrongOwner);
        }
        let mut state = self
            .0
            .waiters
            .lock()
            .expect("original Task capacity addresses");
        let current = self.revision();
        if before > current {
            return Err(TaskCapacityError::FutureRevision);
        }
        let actor = registration.actor();
        if let Some(original) = state.original.get_mut(&actor) {
            if !original.registration.same_registration(registration) {
                return Err(TaskCapacityError::ForeignRegistration);
            }
            // Preserve the earliest outstanding observation and the same FIFO
            // position. A duplicate registration cannot jump a waiting sibling.
            original.before = original.before.min(before);
        } else {
            if state.original.len() >= self.0.limit {
                return Err(TaskCapacityError::Full);
            }
            state
                .fifo
                .enqueue(actor, ())
                .map_err(TaskCapacityError::Queue)?;
            state.original.insert(
                actor,
                Waiter {
                    registration: registration.clone(),
                    before,
                },
            );
        }
        // Reload after insertion: a physical tail never takes this state lock.
        // Root marking is a fixed metadata operation, not a foreign callback.
        if self.revision() != before {
            state.rescan = true;
            self.0
                .runtime
                .notify_host_actor(&self.0.root)
                .map_err(TaskCapacityError::Driver)?;
        }
        Ok(())
    }
    /// Actual Context teardown may remove only its identical queued address.
    /// This supplies no job/body/Scope exit fact; those gates precede the call.
    /// Removing it before real directory reuse keeps the index bounded by C.
    pub(crate) fn unregister_waiter(
        &self,
        registration: &ActorRegistration,
    ) -> Result<(), TaskCapacityError> {
        let mut state = self
            .0
            .waiters
            .lock()
            .expect("original Task capacity addresses");
        let actor = registration.actor();
        let Some(original) = state.original.get(&actor) else {
            return Ok(());
        };
        if !original.registration.same_registration(registration) {
            return Err(TaskCapacityError::ForeignRegistration);
        }
        state
            .fifo
            .remove_queued(actor, ())
            .map_err(TaskCapacityError::Queue)?;
        state.original.remove(&actor);
        state.sweep_remaining = state.sweep_remaining.min(state.fifo.len());
        Ok(())
    }
    /// The actual physical key/tail has been released before this event. This
    /// source does not inspect Contexts or acquire an owner inventory mutex.
    pub(crate) fn capacity_returned(&self) {
        self.0
            .revision
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .unwrap_or_else(|_| std::process::abort());
        match self.0.runtime.notify_host_actor(&self.0.root) {
            Ok(()) | Err(DriverError::UnknownActor | DriverError::EffectTicketOriginMismatch) => {}
            Err(_) => std::process::abort(),
        }
    }
    pub(crate) fn waker(&self) -> Waker {
        Waker::from(Arc::new(CapacityWake(self.clone())))
    }
    /// Root spends at most `budget` original address visits. A fresh capacity
    /// revision starts one finite FIFO sweep; unchanged Busy waiters rotate
    /// only within that sweep, then park until another real physical event.
    /// No caller job, permit or resource changes hands through this index.
    pub(crate) fn drain_pending(&self, budget: usize) -> Result<bool, TaskCapacityError> {
        if budget == 0 {
            return Ok(false);
        }
        self.0
            .runtime
            .validate_registration(&self.0.root)
            .map_err(TaskCapacityError::Driver)?;
        let mut state = self
            .0
            .waiters
            .lock()
            .expect("original Task capacity addresses");
        let revision = self.revision();
        if state.sweep_revision != revision || state.rescan {
            state.sweep_revision = revision;
            state.sweep_remaining = state.fifo.len();
            state.rescan = false;
        }
        state.sweep_remaining = state.sweep_remaining.min(state.fifo.len());
        for _ in 0..budget {
            if state.sweep_remaining == 0 {
                break;
            }
            let Some((actor, ())) = state.fifo.pop() else {
                state.sweep_remaining = 0;
                break;
            };
            state
                .fifo
                .finish(actor, ())
                .map_err(TaskCapacityError::Queue)?;
            state.sweep_remaining -= 1;
            let original = state
                .original
                .get(&actor)
                .expect("queued original Context registration");
            if original.before < revision {
                // Keep the metadata lock through this fixed Common mark. A
                // concurrent Busy registration cannot coalesce into the old
                // row after the mark and then be accidentally removed with it.
                match self.0.runtime.notify_host_actor(&original.registration) {
                    Ok(())
                    | Err(DriverError::UnknownActor | DriverError::EffectTicketOriginMismatch) => {
                        state.original.remove(&actor);
                    }
                    Err(error) => {
                        // The original metadata remains registered on refusal.
                        state
                            .fifo
                            .enqueue(actor, ())
                            .map_err(TaskCapacityError::Queue)?;
                        state.rescan = true;
                        return Err(TaskCapacityError::Driver(error));
                    }
                }
            } else {
                state
                    .fifo
                    .enqueue(actor, ())
                    .map_err(TaskCapacityError::Queue)?;
            }
        }
        Ok(state.sweep_remaining != 0 || state.rescan || self.revision() != revision)
    }
}
struct CapacityWake(TaskCapacitySignal);
impl Wake for CapacityWake {
    fn wake(self: Arc<Self>) {
        self.0.capacity_returned();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.capacity_returned();
    }
}
