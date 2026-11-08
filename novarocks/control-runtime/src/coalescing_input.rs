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

struct State<T> {
    latest: Option<T>,
    in_flight: bool,
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
impl<A: Actor, T: Merge> CoalescingInput<A, T> {
    /// `standing` must be an idle, prepaid input position. Its original Source
    /// chooses the input class; constructing this wrapper grants no new credit.
    pub fn new(standing: StandingInputObligation<A>, project: fn(&T) -> A::Input) -> Self {
        Self(Arc::new(Inner {
            standing: Mutex::new(standing),
            state: Mutex::new(State {
                latest: None,
                in_flight: false,
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
        let next = {
            let mut state = self.0.state.lock().expect("coalescing input state lock");
            if state.closed {
                return Err(ActorTurnError::Closed);
            }
            if let Some(latest) = state.latest.as_mut() {
                latest.merge(value);
            } else {
                state.latest = Some(value);
            }
            Self::claim_fill(&mut state)
        };
        self.fill(next)
    }
    /// Must follow actual `retire_input`, once per occurrence. A closed input
    /// does not refill. Concurrent
    /// notifications either join the pending value or obtain the next fill.
    pub fn after_retire(&self) -> Result<Option<InputId>, ActorTurnError> {
        let next = {
            let mut state = self.0.state.lock().expect("coalescing input state lock");
            state.in_flight = false;
            if state.closed {
                return Ok(None);
            }
            Self::claim_fill(&mut state)
        };
        self.fill(next)
    }
    fn claim_fill(state: &mut State<T>) -> Option<T> {
        if state.in_flight || state.closed {
            return None;
        }
        let value = state.latest.take()?;
        state.in_flight = true;
        Some(value)
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
    fn fill(&self, value: Option<T>) -> Result<Option<InputId>, ActorTurnError> {
        let Some(value) = value else {
            return Ok(None);
        };
        // Actor notification can invoke an external waker. Never retain the
        // coalescing state lock across the public standing-input operation.
        let result = self
            .0
            .standing
            .lock()
            .expect("coalescing standing input lock")
            .fill((self.0.project)(&value));
        match result {
            Ok(input) => Ok(Some(input)),
            Err((error, input)) => {
                if matches!(error, ActorTurnError::Closed | ActorTurnError::ActorFailed) {
                    self.close();
                } else {
                    let mut state = self.0.state.lock().expect("coalescing input state lock");
                    // Preserve chronological merge order when a concurrent
                    // notifier updated latest while the fill ran unlocked.
                    let mut original = value;
                    if let Some(newer) = state.latest.take() {
                        original.merge(newer);
                    }
                    if !state.closed {
                        state.latest = Some(original);
                    }
                    // Busy still denotes a real outstanding occurrence. Only
                    // its actual retirement may authorize another fill.
                    state.in_flight = error == ActorTurnError::StandingInputBusy;
                }
                drop(input);
                Err(error)
            }
        }
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
