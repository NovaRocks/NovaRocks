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

//! One paid standing input coalesces notifications while its occurrence is live.
//! The host must call `after_retire` only after retiring that occurrence. These
//! values are neutral notifications; ordered protocol facts belong elsewhere.

use crate::actor_turn::StandingInputId;
use crate::{
    actor::Actor,
    actor_turn::{ActorTurnError, StandingInputObligation},
    sync::{Arc, Mutex},
};
use novarocks_control_runtime_core::identity::InputId;
use std::task::{Wake, Waker};

/// Combines notifications without introducing another queue. `unit` is the
/// notification represented by a wakeup, not an allocation or resource grant.
pub trait Merge: Send + 'static {
    fn merge(&mut self, newer: Self);
    fn unit() -> Self;
}

struct Update<T> {
    value: T,
    revision: u64,
}

impl<T: Merge> Update<T> {
    fn merge(&mut self, newer: Self) {
        self.value.merge(newer.value);
        self.revision = newer.revision;
    }
}

struct State<T> {
    latest: Option<Update<T>>,
    in_flight: Option<u64>,
    issued: u64,
    retired: u64,
    abandoned_through: u64,
    closed: bool,
}
struct Inner<A: Actor, T> {
    standing: Mutex<StandingInputObligation<A>>,
    state: Mutex<State<T>>,
    project: fn(&T) -> A::Input,
}

/// Clones share one standing obligation and one outstanding occurrence. Closing
/// notification admission does not prove holder/capture exit or retire inputs.
/// Drop retains the original obligation; its owner must call `close_after_exit`.
pub struct CoalescingInput<A: Actor, T: Merge>(Arc<Inner<A, T>>);
impl<A: Actor, T: Merge> Clone for CoalescingInput<A, T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
/// A passive receipt for one notification on its original role. It becomes
/// retired when the occurrence covering it actually retires, including when
/// the notification was merged into a newer value. It grants no input credit.
pub struct CoalescedReceipt<A: Actor, T: Merge> {
    original: Arc<Inner<A, T>>,
    revision: u64,
}

impl<A: Actor, T: Merge> CoalescedReceipt<A, T> {
    pub fn retired(&self) -> bool {
        self.original
            .state
            .lock()
            .expect("coalescing input state lock")
            .retired
            >= self.revision
    }

    /// Reconcile completion of concurrent notifications on the same role.
    pub fn retain_later(&mut self, other: Self) {
        assert!(Arc::ptr_eq(&self.original, &other.original));
        self.revision = self.revision.max(other.revision);
    }
}

impl<A: Actor, T: Merge> CoalescingInput<A, T> {
    /// Payload geometry of the actual shared role. Its original payer must
    /// include the Arc allocation overhead for the runtime's allocator.
    pub fn backing_payload_layout() -> std::alloc::Layout {
        std::alloc::Layout::new::<Inner<A, T>>()
    }

    /// `standing` must be an idle, prepaid input position. Its original Source
    /// chooses the input class; constructing this wrapper grants no new credit.
    pub fn new(standing: StandingInputObligation<A>, project: fn(&T) -> A::Input) -> Self {
        Self(Arc::new(Inner {
            standing: Mutex::new(standing),
            state: Mutex::new(State {
                latest: None,
                in_flight: None,
                issued: 0,
                retired: 0,
                abandoned_through: 0,
                closed: false,
            }),
            project,
        }))
    }
    pub fn id(&self) -> StandingInputId {
        self.0
            .standing
            .lock()
            .expect("coalescing standing input lock")
            .id()
    }
    pub fn in_flight(&self) -> Result<Option<InputId>, ActorTurnError> {
        self.0
            .standing
            .lock()
            .expect("coalescing standing input lock")
            .in_flight()
    }
    /// Returns the new occurrence when this call acquires the sole fill right.
    /// Closed/ActorFailed closes notification admission. Other fill errors retain
    /// the merged value for the original host to retry; they never discard it.
    pub fn notify(&self, value: T) -> Result<Option<InputId>, ActorTurnError> {
        let (_, next) = self.enqueue(value)?;
        self.fill(next)
    }

    /// Retain a receipt even if this notification waits behind an existing
    /// occurrence. A refused fill never reports retirement.
    pub fn notify_tracked(&self, value: T) -> Result<CoalescedReceipt<A, T>, ActorTurnError> {
        let (revision, next) = self.enqueue(value)?;
        self.fill(next)?;
        Ok(CoalescedReceipt {
            original: self.0.clone(),
            revision,
        })
    }

    fn enqueue(&self, value: T) -> Result<(u64, Option<Update<T>>), ActorTurnError> {
        let mut state = self.0.state.lock().expect("coalescing input state lock");
        if state.closed {
            return Err(ActorTurnError::Closed);
        }
        let revision = state
            .issued
            .checked_add(1)
            .ok_or(ActorTurnError::IdentityExhausted)?;
        state.issued = revision;
        let update = Update { value, revision };
        if let Some(latest) = state.latest.as_mut() {
            latest.merge(update);
        } else {
            state.latest = Some(update);
        }
        Ok((revision, Self::claim_fill(&mut state)))
    }
    /// Must follow actual `retire_input`, once per occurrence. A closed input
    /// does not refill. Concurrent
    /// notifications either join the pending value or obtain the next fill.
    pub fn after_retire(&self) -> Result<Option<InputId>, ActorTurnError> {
        let next = {
            let mut state = self.0.state.lock().expect("coalescing input state lock");
            if let Some(revision) = state.in_flight.take() {
                state.retired = state.retired.max(revision);
            }
            if state.closed {
                state.retired = state.retired.max(state.abandoned_through);
                return Ok(None);
            }
            Self::claim_fill(&mut state)
        };
        self.fill(next)
    }
    fn claim_fill(state: &mut State<T>) -> Option<Update<T>> {
        if state.in_flight.is_some() || state.closed {
            return None;
        }
        let update = state.latest.take()?;
        state.in_flight = Some(update.revision);
        Some(update)
    }
    /// Retries a retained notification after a nonterminal fill error. The
    /// original error is returned to the host; no new capacity is manufactured.
    pub fn retry(&self) -> Result<Option<InputId>, ActorTurnError> {
        let next = {
            let mut state = self.0.state.lock().expect("coalescing input state lock");
            if state.closed {
                return Err(ActorTurnError::Closed);
            }
            Self::claim_fill(&mut state)
        };
        self.fill(next)
    }
    fn fill(&self, update: Option<Update<T>>) -> Result<Option<InputId>, ActorTurnError> {
        let Some(update) = update else {
            return Ok(None);
        };
        // Actor notification can invoke an external waker. Never retain the
        // coalescing state lock across the public standing-input operation.
        let result = self
            .0
            .standing
            .lock()
            .expect("coalescing standing input lock")
            .fill((self.0.project)(&update.value));
        match result {
            Ok(input) => Ok(Some(input)),
            Err((error, input)) => {
                drop(input);
                if matches!(error, ActorTurnError::Closed | ActorTurnError::ActorFailed) {
                    let latest = {
                        let mut state = self.0.state.lock().expect("coalescing input state lock");
                        state.closed = true;
                        state.latest.take()
                    };
                    let abandoned = latest
                        .as_ref()
                        .map_or(update.revision, |latest| latest.revision);
                    drop((update, latest));
                    let mut state = self.0.state.lock().expect("coalescing input state lock");
                    state.in_flight = None;
                    state.abandoned_through = state.abandoned_through.max(abandoned);
                    state.retired = state.retired.max(state.abandoned_through);
                } else {
                    let mut state = self.0.state.lock().expect("coalescing input state lock");
                    // Preserve chronological merge order when a concurrent
                    // notifier updated latest while the fill ran unlocked.
                    let mut original = update;
                    if let Some(newer) = state.latest.take() {
                        original.merge(newer);
                    }
                    if !state.closed {
                        state.latest = Some(original);
                    }
                    // Busy still denotes a real outstanding occurrence. Only
                    // its actual retirement may authorize another fill.
                    state.in_flight = (error == ActorTurnError::StandingInputBusy).then_some(0);
                }
                Err(error)
            }
        }
    }
    /// Retire at most one actual occurrence on this original standing role.
    /// The owner has indexed the runtime input and released its captures.
    pub fn retire_ready_after_exit(&self) -> Result<bool, ActorTurnError> {
        self.retire_current(None)
    }

    /// The original host has indexed this exact occurrence. An old receipt
    /// cannot retire a later fill of the same reusable standing role.
    pub fn retire_indexed_after_exit(&self, input: InputId) -> Result<bool, ActorTurnError> {
        self.retire_current(Some(input))
    }

    fn retire_current(&self, expected: Option<InputId>) -> Result<bool, ActorTurnError> {
        {
            let standing = self
                .0
                .standing
                .lock()
                .expect("coalescing standing input lock");
            let current = standing.in_flight()?;
            if expected.is_some() && expected != current {
                return Err(ActorTurnError::InputAuthorMismatch);
            }
            if current.is_none() || !standing.retire_ready_after_exit()? {
                return Ok(false);
            }
        }
        self.after_retire()?;
        Ok(true)
    }

    /// The original failed Source disposes an accepted, unpublished update.
    /// Advance the physical receipt frontier only after actual destruction,
    /// and never across a still-live original occurrence.
    pub fn abandon_pending_after_failure(&self) {
        let latest = {
            let mut state = self.0.state.lock().expect("coalescing input state lock");
            state.closed = true;
            state.latest.take()
        };
        if let Some(latest) = latest {
            let revision = latest.revision;
            drop(latest);
            let mut state = self.0.state.lock().expect("coalescing input state lock");
            state.abandoned_through = state.abandoned_through.max(revision);
            if state.in_flight.is_none() {
                state.retired = state.retired.max(state.abandoned_through);
            }
        }
    }

    /// The failed owner has closed admission, and the runtime verifies the
    /// original occurrence has no remaining payload, effect, or reservation.
    /// No discarded notification is refilled.
    pub fn retire_failed_after_exit(&self) -> Result<(), ActorTurnError> {
        if !self
            .0
            .state
            .lock()
            .expect("coalescing input state lock")
            .closed
        {
            return Ok(());
        }
        let standing = self
            .0
            .standing
            .lock()
            .expect("coalescing standing input lock");
        if standing.is_active()
            && standing.in_flight()?.is_some()
            && standing.retire_ready_after_exit()?
        {
            drop(standing);
            self.after_retire()?;
        }
        Ok(())
    }

    /// Stops new notifications and discards pending updates. An occurrence
    /// whose fill right was already acquired still belongs to its original host.
    pub fn close(&self) {
        let latest = {
            let mut state = self.0.state.lock().expect("coalescing input state lock");
            state.closed = true;
            state.latest.take()
        };
        drop(latest);
    }
    /// The caller has observed all original holder/capture and report exits.
    /// Busy preserves the original standing obligation for a later retry.
    pub fn close_after_exit(&self) -> Result<(), ActorTurnError> {
        self.close();
        self.0
            .standing
            .lock()
            .expect("coalescing standing input lock")
            .close_after_exit()
    }
    /// Capacity owners can retain this waker as one deduplicated registration.
    /// Unregister and drop original holders before claiming `close_after_exit`.
    pub fn waker(&self) -> Waker {
        Waker::from(std::sync::Arc::new(NotificationWake(self.clone())))
    }
}
struct NotificationWake<A: Actor, T: Merge>(CoalescingInput<A, T>);
impl<A: Actor, T: Merge> Wake for NotificationWake<A, T> {
    fn wake(self: std::sync::Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        let _ = self.0.notify(T::unit());
    }
}
