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

//! Synchronous handoff into the original fixed role cells.
//! Both outcomes retain their real sender tail until the caller unlocks.

use super::*;
use std::sync::Mutex;

pub(super) struct MountedInput<I, C = ()> {
    pub(super) owner: WorkOwner,
    pub(super) input: I,
    // Actual input/owner destruction precedes context, then the same footer.
    pub(super) context: C,
    pub(super) footer: Footer,
}

pub(super) struct SharedIngress<I, C = ()> {
    pub(super) inner: Arc<Inner>,
    pub(super) role: usize,
    pub(super) generation: u64,
    pub(super) ready: Arc<ReadyQueue>,
    pub(super) inputs: Mutex<Vec<Option<MountedInput<I, C>>>>,
    // Every accepted input and original sender exits before late registration.
    pub(super) _registration: BankRegistration,
}

pub struct RoleIngress<I, C = ()> {
    shared: Weak<SharedIngress<I, C>>,
}
impl<I, C> Clone for RoleIngress<I, C> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

/// Contains the actual original sender on refusal too. Its destruction may
/// execute registration/input destructors: carry BOTH outcomes out of every
/// receiving/source lock before finish or Drop. Drop supplies no rollback,
/// completion or settlement; the original committed level facts remain.
#[must_use = "Finish or drop the original sender only after all caller locks exit"]
pub struct MountAttempt<I, C = ()> {
    result: Result<(), WorkError>,
    shared: Option<Arc<SharedIngress<I, C>>>,
    notification_pending: bool,
}
impl<I, C> MountAttempt<I, C> {
    pub fn result(&self) -> &Result<(), WorkError> {
        &self.result
    }

    fn notify(&mut self) {
        if self.notification_pending {
            // A callback may unwind. Do not invoke it again from Drop.
            self.notification_pending = false;
            let shared = self
                .shared
                .as_ref()
                .expect("committed mount retains its sender");
            shared.ready.notify();
            shared.inner.changed.notify_waiters();
        }
    }

    pub fn finish(mut self) -> Result<(), WorkError> {
        self.notify();
        // Refusal can be the last strong sender after bank teardown. Its
        // implicit callbacks run here, outside the caller's receiving locks.
        drop(self.shared.take());
        std::mem::replace(&mut self.result, Ok(()))
    }
}
impl<I, C> Drop for MountAttempt<I, C> {
    fn drop(&mut self) {
        // Accepted input also progresses when its publisher unwinds before
        // finish. Both paths must run after every caller lock has exited.
        self.notify();
    }
}

impl<I, C> RoleIngress<I, C> {
    /// The original caller must keep all three source fields outside destructible
    /// publication futures. Refusal preserves every source even if its sender
    /// tail panics. Success moves all three into the original shared cell before
    /// notification, including when a notification callback panics.
    pub fn try_mount_in(
        &self,
        owner: &mut Option<WorkOwner>,
        input: &mut Option<I>,
        context: &mut Option<C>,
    ) -> MountAttempt<I, C> {
        let Some(shared) = self.shared.upgrade() else {
            return MountAttempt {
                result: Err(WorkError::Closed),
                shared: None,
                notification_pending: false,
            };
        };
        let result = shared.commit(owner, input, context);
        MountAttempt {
            notification_pending: result.is_ok(),
            result,
            shared: Some(shared),
        }
    }
}

impl<I, C> SharedIngress<I, C> {
    pub(super) fn export(self: &Arc<Self>) -> Result<RoleIngress<I, C>, WorkError> {
        let mut state = self.inner.state.lock().unwrap();
        for slot in &state.cancellations.slots {
            let role = &slot.polls[self.role];
            if role.registration != RegistrationPhase::Installed
                || role.registration_generation != self.generation
            {
                return Err(WorkError::NotReady);
            }
        }
        if state
            .cancellations
            .slots
            .iter()
            .any(|slot| !slot.polls[self.role].ingress_exported)
        {
            for slot in &mut state.cancellations.slots {
                slot.polls[self.role].ingress_exported = true;
            }
            state.advance_progress_revision();
        }
        Ok(RoleIngress {
            shared: Arc::downgrade(self),
        })
    }

    #[cfg(test)]
    pub(super) fn attempt(
        self: &Arc<Self>,
        owner: &mut Option<WorkOwner>,
        input: &mut Option<I>,
        context: &mut Option<C>,
    ) -> MountAttempt<I, C> {
        let result = self.commit(owner, input, context);
        MountAttempt {
            notification_pending: result.is_ok(),
            result,
            shared: Some(Arc::clone(self)),
        }
    }

    fn commit(
        &self,
        owner: &mut Option<WorkOwner>,
        input: &mut Option<I>,
        context: &mut Option<C>,
    ) -> Result<(), WorkError> {
        let scope = owner.as_ref().ok_or(WorkError::Released)?.scope();
        if input.is_none() || context.is_none() {
            return Err(WorkError::Released);
        }
        if !Arc::ptr_eq(&scope.inner, &self.inner) {
            return Err(WorkError::Conflict);
        }
        let mut state = self.inner.state.lock().unwrap();
        if state.closed {
            return Err(WorkError::Closed);
        }
        if !state.ready {
            return Err(WorkError::NotReady);
        }
        let node = state.nodes.get(&scope.id).ok_or(WorkError::Released)?;
        let slot_id = node.cancellation;
        let slot = &mut state.cancellations.slots[slot_id.0];
        let generation = slot.generation;
        if slot.phase != SlotPhase::Node(scope.id) {
            return Err(WorkError::Conflict);
        }
        let backing = &mut slot.polls[self.role];
        if backing.registration != RegistrationPhase::Installed
            || backing.registration_generation != self.generation
        {
            return Err(WorkError::NotReady);
        }
        if backing.phase != PollPhase::Free || Arc::get_mut(&mut backing.wake).is_none() {
            return Err(WorkError::Capacity("role backing has not actually exited"));
        }
        // This is the sole lock order: original State -> input -> ready ring.
        let mut inputs = self.inputs.lock().unwrap();
        let target = &mut inputs[slot_id.0];
        if target.is_some() {
            return Err(WorkError::Conflict);
        }
        let footer = Footer {
            wake: Arc::clone(&backing.wake),
            _pin: MountPin {
                inner: Arc::clone(&self.inner),
                slot: slot_id,
                generation,
                work: scope.id,
                role: self.role,
            },
        };
        *target = Some(MountedInput {
            owner: owner.take().unwrap(),
            input: input.take().unwrap(),
            context: context.take().unwrap(),
            footer,
        });
        backing.phase = PollPhase::Mounted;
        assert!(
            self.ready.publish(&backing.wake),
            "unique mount has no old ready hint"
        );
        state.advance_progress_revision();
        Ok(())
    }

    pub(super) fn take_input(&self, slot: SlotId) -> Option<MountedInput<I, C>> {
        // Move the SAME footer with its input. No business destructor or frame
        // construction runs under either lock, including during stopping drain.
        let _state = self.inner.state.lock().unwrap();
        self.inputs.lock().unwrap()[slot.0].take()
    }

    pub(super) fn stop(&self) {
        let mut state = self.inner.state.lock().unwrap();
        let mut changed = false;
        for slot in &mut state.cancellations.slots {
            let role = &mut slot.polls[self.role];
            assert_eq!(role.registration_generation, self.generation);
            match role.registration {
                RegistrationPhase::Installed => {
                    role.registration = RegistrationPhase::Stopping;
                    changed = true;
                }
                RegistrationPhase::Stopping => {}
                RegistrationPhase::Absent => unreachable!("live ingress retains registration"),
            }
        }
        if state.ready {
            state.ready = false;
            state.closed = true;
            changed = true;
        }
        if changed {
            state.advance_progress_revision();
        }
    }
}
