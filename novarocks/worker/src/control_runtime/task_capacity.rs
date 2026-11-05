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
use novarocks_control_runtime::actor_turn::{ActorRegistration, ActorRuntime, ActorTurnError};
use novarocks_control_runtime_core::{
    identity::ActorId,
    scheduler::{ReadyQueue, SchedulerError},
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
    WrongOwner,
    ForeignRegistration,
    FutureRevision,
    ActorTurn(ActorTurnError),
    Queue(SchedulerError),
}
impl std::fmt::Display for TaskCapacityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WrongOwner => f.write_str("Task capacity signal belongs to another runtime"),
            Self::ForeignRegistration => {
                f.write_str("Task capacity waiter has a foreign registration")
            }
            Self::FutureRevision => f.write_str("Task capacity waiter refers to a future revision"),
            Self::ActorTurn(error) => write!(f, "Task capacity notification failed: {error}"),
            Self::Queue(error) => write!(f, "Task capacity waiter queue failed: {error:?}"),
        }
    }
}
impl std::error::Error for TaskCapacityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ActorTurn(error) => Some(error),
            // SchedulerError is a closed neutral leaf without an Error impl.
            Self::Queue(_)
            | Self::WrongOwner
            | Self::ForeignRegistration
            | Self::FutureRevision => None,
        }
    }
}
struct Waiter {
    registration: ActorRegistration,
    before: u64,
}
struct Waiters {
    original: BTreeMap<ActorId, Waiter>,
    fifo: ReadyQueue,
    sweep_revision: u64,
    sweep_remaining: usize,
    rescan: bool,
}
struct Shared {
    runtime: ActorRuntime<WorkerActor>,
    root: ActorRegistration,
    revision: AtomicU64,
    waiters: Mutex<Waiters>,
}
/// Cloning shares only the same per-actor metadata index and tail event source.
/// It grants no Work, input, original job, publication or resource capacity.
#[derive(Clone)]
pub(crate) struct TaskCapacitySignal(Arc<Shared>);

impl TaskCapacitySignal {
    /// The original Root authenticates this process-local notification index.
    /// Registration allocates one address row for that actor, with no C limit.
    pub(crate) fn new(
        runtime: ActorRuntime<WorkerActor>,
        root: ActorRegistration,
    ) -> Result<Self, TaskCapacityError> {
        runtime
            .validate_registration(&root)
            .map_err(TaskCapacityError::ActorTurn)?;
        Ok(Self(Arc::new(Shared {
            runtime,
            root,
            revision: AtomicU64::new(0),
            waiters: Mutex::new(Waiters {
                original: BTreeMap::new(),
                fifo: ReadyQueue::new(),
                sweep_revision: 0,
                sweep_remaining: 0,
                rescan: false,
            }),
        })))
    }
    pub(crate) fn same_signal(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub(crate) fn is_empty(&self) -> bool {
        let state = self
            .0
            .waiters
            .lock()
            .expect("original Task capacity addresses");
        state.original.is_empty() && state.fifo.actor_count() == 0
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
            .map_err(TaskCapacityError::ActorTurn)?;
        self.0
            .runtime
            .validate_registration(registration)
            .map_err(TaskCapacityError::ActorTurn)?;
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
            state
                .fifo
                .register(actor)
                .map_err(TaskCapacityError::Queue)?;
            state.fifo.notify(actor).map_err(TaskCapacityError::Queue)?;
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
                .map_err(TaskCapacityError::ActorTurn)?;
        }
        Ok(())
    }
    /// Actual Context teardown may remove only its identical queued address.
    /// This supplies no job/body/Scope exit fact; those gates precede the call.
    /// Each live registered actor owns at most one row; no record count grants
    /// or refuses Context admission.
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
            .cancel_pending(actor)
            .map_err(TaskCapacityError::Queue)?;
        state
            .fifo
            .unregister(actor)
            .map_err(TaskCapacityError::Queue)?;
        state.original.remove(&actor);
        state.sweep_remaining = state.sweep_remaining.min(state.fifo.ready_count());
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
            Ok(())
            | Err(ActorTurnError::UnknownActor | ActorTurnError::EffectTicketOriginMismatch) => {}
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
            .map_err(TaskCapacityError::ActorTurn)?;
        let mut state = self
            .0
            .waiters
            .lock()
            .expect("original Task capacity addresses");
        let revision = self.revision();
        if state.sweep_revision != revision || state.rescan {
            state.sweep_revision = revision;
            state.sweep_remaining = state.fifo.ready_count();
            state.rescan = false;
        }
        state.sweep_remaining = state.sweep_remaining.min(state.fifo.ready_count());
        for _ in 0..budget {
            if state.sweep_remaining == 0 {
                break;
            }
            let Some(actor) = state.fifo.claim() else {
                state.sweep_remaining = 0;
                break;
            };
            state.sweep_remaining -= 1;
            let original = state
                .original
                .get(&actor)
                .expect("queued original Participation registration");
            if original.before < revision {
                // Keep the metadata lock through this fixed Common mark. A
                // concurrent Busy registration cannot coalesce into the old
                // row after the mark and then be accidentally removed with it.
                match self.0.runtime.notify_host_actor(&original.registration) {
                    Ok(())
                    | Err(
                        ActorTurnError::UnknownActor | ActorTurnError::EffectTicketOriginMismatch,
                    ) => {
                        state
                            .fifo
                            .unregister_claimed(actor)
                            .map_err(TaskCapacityError::Queue)?;
                        state.original.remove(&actor);
                    }
                    Err(error) => {
                        // The original metadata remains registered on refusal.
                        state
                            .fifo
                            .release(actor, true)
                            .map_err(TaskCapacityError::Queue)?;
                        state.rescan = true;
                        return Err(TaskCapacityError::ActorTurn(error));
                    }
                }
            } else {
                state
                    .fifo
                    .release(actor, true)
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
