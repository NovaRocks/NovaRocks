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

//! One external reply observer and its original request's bounded physical
//! wake/disposal capture. This mechanism creates no Work, report or capacity.
//! The original owner pays its actual storage before construction, retains
//! every returned job on Busy, and gates retirement on `is_idle()`.

use crate::{
    actor::Actor,
    driver::{ActorRegistration, ActorRuntime, DriverError},
    lease::WorkLease,
    sync::{Arc, Mutex},
};
use novarocks_control_runtime_core::{
    identity::{ScopeId, WorkId},
    scope::WorkPhase,
};
use std::{
    any::Any,
    mem::{ManuallyDrop, size_of},
    panic::{AssertUnwindSafe, catch_unwind},
    task::Waker,
};

type PanicPayload = ManuallyDrop<Box<dyn Any + Send>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplyNotificationError {
    Driver(DriverError),
    WrongWorkScope,
    WorkNotQueued,
    ObserverEnded,
    Failed,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplyNotificationFailure {
    Panicked,
    ExternalUnwind,
    AbandonedJob,
    OwnerLost,
    Driver(DriverError),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplyNotificationSnapshot {
    pub published: bool,
    pub external_ended: bool,
    pub waiter_held: bool,
    pub job_in_flight: bool,
    pub abandoned_waker_held: bool,
    pub panic_payloads: usize,
    pub failure: Option<ReplyNotificationFailure>,
}
impl ReplyNotificationSnapshot {
    /// Healthy physical quiescence of this notification capture only. This
    /// does not acknowledge a reply, report, input or any original Work exit.
    pub fn is_idle(self) -> bool {
        self.external_ended
            && !self.waiter_held
            && !self.job_in_flight
            && !self.abandoned_waker_held
            && self.panic_payloads == 0
            && self.failure.is_none()
    }
}

struct State {
    published: bool,
    external_ended: bool,
    waiter: Option<ManuallyDrop<Waker>>,
    job_in_flight: bool,
    abandoned: Option<ManuallyDrop<Waker>>,
    // One physical callback, one current waiter and one external replacement
    // Drop can overlap. The first failure rejects further arm operations.
    panics: [Option<PanicPayload>; 3],
    failure: Option<ReplyNotificationFailure>,
}
impl State {
    fn fail(&mut self, failure: ReplyNotificationFailure) {
        if self.failure.is_none() {
            self.failure = Some(failure);
        }
    }
    fn snapshot(&self) -> ReplyNotificationSnapshot {
        ReplyNotificationSnapshot {
            published: self.published,
            external_ended: self.external_ended,
            waiter_held: self.waiter.is_some(),
            job_in_flight: self.job_in_flight,
            abandoned_waker_held: self.abandoned.is_some(),
            panic_payloads: self.panics.iter().filter(|p| p.is_some()).count(),
            failure: self.failure,
        }
    }
}
struct Shared<A: Actor> {
    runtime: ActorRuntime<A>,
    registration: ActorRegistration,
    key: (ScopeId, WorkId),
    state: Mutex<State>,
}
impl<A: Actor> Shared<A> {
    fn notify_owner(&self) {
        if let Err(error) = self.runtime.notify_host_actor(&self.registration) {
            self.state
                .lock()
                .expect("original reply notification state")
                .fail(ReplyNotificationFailure::Driver(error));
        }
    }
    fn retain_panic(&self, payload: Box<dyn Any + Send>) {
        let mut state = self
            .state
            .lock()
            .expect("original reply notification panic");
        state.fail(ReplyNotificationFailure::Panicked);
        let Some(slot) = state.panics.iter_mut().find(|slot| slot.is_none()) else {
            std::process::abort();
        };
        *slot = Some(ManuallyDrop::new(payload));
    }
    fn end_external(&self) {
        let mut state = self
            .state
            .lock()
            .expect("original reply notification state");
        if state.external_ended {
            return;
        }
        state.external_ended = true;
        if std::thread::panicking() {
            state.fail(ReplyNotificationFailure::ExternalUnwind);
        }
        drop(state);
        self.notify_owner();
    }
}

/// The original request keeps this unique metadata owner. It never invokes
/// a foreign Waker, destroys it, acknowledges the reply or exits an author.
#[must_use = "Gate the original request's retirement on actual notification quiescence"]
pub struct ReplyNotification<A: Actor> {
    shared: Arc<Shared<A>>,
}
/// Exactly one external observer may arm this endpoint. It is not Clone.
/// Cancellation ends this wait only; existing wake jobs remain supervised.
#[must_use = "Retain the single external observer or end only this wait"]
pub struct ReplyObserver<A: Actor> {
    shared: Arc<Shared<A>>,
}
/// Returned only to the external polling thread. These flags only tell it
/// to recheck the same original reply/failure; they grant no business fact.
pub struct ReplyArm {
    pub ready: bool,
    pub failed: bool,
}
#[must_use = "The external caller still owns this refused Waker"]
pub struct ReplyArmRefusal {
    pub error: ReplyNotificationError,
    pub waker: Waker,
}
/// The same admitted request's physical notification capture. Queue refusal
/// must return this whole value. Metadata Drop retains an abandoned Waker;
/// only `run()` is allowed to invoke or destroy foreign callback code.
#[must_use = "Retain the same original notification job on physical queue refusal"]
pub struct ReplyNotificationJob<A: Actor> {
    shared: Arc<Shared<A>>,
    waker: Option<ManuallyDrop<Waker>>,
    wake: bool,
    finished: bool,
}

impl<A: Actor> ReplyNotification<A> {
    /// Construct once for the caller's already-paid original request before
    /// its first callback capture. Matching public IDs is insufficient: both
    /// the actual registration/runtime and the original Scope must match.
    /// This check does not prove that the caller paid notification geometry.
    pub fn for_admitted_work(
        runtime: &ActorRuntime<A>,
        registration: &ActorRegistration,
        work: &WorkLease,
    ) -> Result<(Self, ReplyObserver<A>), ReplyNotificationError> {
        runtime
            .validate_registration(registration)
            .map_err(ReplyNotificationError::Driver)?;
        if !registration.matches_scope(work.scope()) {
            return Err(ReplyNotificationError::WrongWorkScope);
        }
        {
            let state = work
                .scope()
                .runtime
                .shared
                .state
                .lock()
                .expect("original reply notification admission");
            let original = state
                .scopes
                .get(&work.scope().id())
                .and_then(|scope| scope.work(work.id()))
                .ok_or(ReplyNotificationError::WorkNotQueued)?;
            if original.unbound_guard
                || original.phase != WorkPhase::Queued
                || original.body_exited
                || original.captures_exited
            {
                return Err(ReplyNotificationError::WorkNotQueued);
            }
        }
        let shared = Arc::new(Shared {
            runtime: runtime.clone(),
            registration: registration.clone(),
            key: (work.scope().id(), work.id()),
            state: Mutex::new(State {
                published: false,
                external_ended: false,
                waiter: None,
                job_in_flight: false,
                abandoned: None,
                panics: Default::default(),
                failure: None,
            }),
        });
        Ok((
            Self {
                shared: shared.clone(),
            },
            ReplyObserver { shared },
        ))
    }
    /// The original publication owner has committed its real reply. This
    /// flag neither publishes another body nor invokes the external observer.
    pub fn published(&mut self) {
        self.shared
            .state
            .lock()
            .expect("original reply notification state")
            .published = true;
        self.shared.notify_owner();
    }
    /// Re-arm the same paid notification after its owner consumed a selected
    /// stream delivery. The caller must serialize this with its own source
    /// publication and recheck that source before parking. Common neither
    /// acknowledges the delivery nor discards an in-flight physical callback;
    /// a previously captured callback may still produce a harmless extra wake.
    pub fn clear_published(&mut self) -> Result<(), ReplyNotificationError> {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("original stream notification acknowledgement");
        if state.external_ended {
            return Err(ReplyNotificationError::ObserverEnded);
        }
        if state.failure.is_some() {
            return Err(ReplyNotificationError::Failed);
        }
        state.published = false;
        Ok(())
    }
    pub fn snapshot(&self) -> ReplyNotificationSnapshot {
        self.shared
            .state
            .lock()
            .expect("original reply notification state")
            .snapshot()
    }
    pub fn is_idle(&self) -> bool {
        self.snapshot().is_idle()
    }
    /// At most one physical job is outstanding. A later arm can occupy only
    /// the one waiting cell while that same job is queued, running or Busy.
    pub fn take_job(&mut self) -> Option<ReplyNotificationJob<A>> {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("original reply notification state");
        if state.job_in_flight
            || !(state.published || state.external_ended || state.failure.is_some())
        {
            return None;
        }
        let waker = state.abandoned.take().or_else(|| state.waiter.take())?;
        state.job_in_flight = true;
        let wake = state.published && !state.external_ended && state.failure.is_none();
        Some(ReplyNotificationJob {
            shared: self.shared.clone(),
            waker: Some(waker),
            wake,
            finished: false,
        })
    }
}
impl<A: Actor> Drop for ReplyNotification<A> {
    fn drop(&mut self) {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("original reply notification state");
        if state.snapshot().is_idle() {
            return;
        }
        state.fail(ReplyNotificationFailure::OwnerLost);
        drop(state);
        self.shared.notify_owner();
    }
}
impl<A: Actor> ReplyObserver<A> {
    /// Call only from the external polling thread. It clones the Waker
    /// before this call; replacement Drop runs here after the lock is released.
    /// No replaced Waker can escape that original observer's capture lifetime.
    /// Register, then recheck the actual reply to close publication races.
    pub fn arm(&mut self, waker: Waker) -> Result<ReplyArm, ReplyArmRefusal> {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("original reply notification state");
        let error = if state.external_ended {
            Some(ReplyNotificationError::ObserverEnded)
        } else if state.failure.is_some() {
            Some(ReplyNotificationError::Failed)
        } else {
            None
        };
        if let Some(error) = error {
            return Err(ReplyArmRefusal { error, waker });
        }
        let replaced = state.waiter.replace(ManuallyDrop::new(waker));
        drop(state);
        if let Some(replaced) = replaced {
            let result = catch_unwind(AssertUnwindSafe(|| {
                drop(ManuallyDrop::into_inner(replaced));
            }));
            if let Err(payload) = result {
                self.shared.retain_panic(payload);
            }
        }
        self.shared.notify_owner();
        let state = self
            .shared
            .state
            .lock()
            .expect("original reply notification state");
        Ok(ReplyArm {
            ready: state.published,
            failed: state.failure.is_some(),
        })
    }
    pub fn end(self) {
        // Drop performs the same idempotent metadata-only cancellation.
    }
    pub fn snapshot(&self) -> ReplyNotificationSnapshot {
        self.shared
            .state
            .lock()
            .expect("original reply notification state")
            .snapshot()
    }
}
impl<A: Actor> Drop for ReplyObserver<A> {
    fn drop(&mut self) {
        self.shared.end_external();
    }
}
impl<A: Actor> ReplyNotificationJob<A> {
    pub fn queue_key(&self) -> (ScopeId, WorkId) {
        self.shared.key
    }
    /// Run only on the actual physical host worker. A callback panic remains
    /// an unresolved original capture, with its payload retained in bounded
    /// storage. It never becomes a healthy idle or author-exit observation.
    pub fn run(mut self) {
        let waker = self
            .waker
            .take()
            .expect("same original reply notification Waker");
        let result = catch_unwind(AssertUnwindSafe(|| {
            let waker = ManuallyDrop::into_inner(waker);
            if self.wake {
                waker.wake();
            } else {
                drop(waker);
            }
        }));
        if let Err(payload) = result {
            self.shared.retain_panic(payload);
        }
        // Drop records physical completion after wake/Drop and panic capture.
        self.finished = true;
    }
}
impl<A: Actor> Drop for ReplyNotificationJob<A> {
    fn drop(&mut self) {
        let mut state = self
            .shared
            .state
            .lock()
            .expect("original reply notification tail");
        assert!(
            state.job_in_flight,
            "one original physical reply notification"
        );
        state.job_in_flight = false;
        if !self.finished {
            state.fail(ReplyNotificationFailure::AbandonedJob);
            if let Some(waker) = self.waker.take() {
                assert!(state.abandoned.is_none(), "one abandoned original Waker");
                state.abandoned = Some(waker);
            }
        }
        drop(state);
        self.shared.notify_owner();
    }
}

/// Payload/handle layouts, not a claim that an old request's fee covers them.
/// The caller must also price its pinned Arc allocation header, actual mutex
/// allocation layout and executor queue position in the original R geometry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplyNotificationGeometry {
    pub shared_payload_bytes: usize,
    pub owner_handle_bytes: usize,
    pub observer_handle_bytes: usize,
    pub physical_job_bytes: usize,
    pub maximum_retained_wakers: usize,
    pub maximum_transition_wakers: usize,
    pub panic_payload_slots: usize,
}
impl ReplyNotificationGeometry {
    pub fn for_actor<A: Actor>() -> Self {
        Self {
            shared_payload_bytes: size_of::<Shared<A>>(),
            owner_handle_bytes: size_of::<ReplyNotification<A>>(),
            observer_handle_bytes: size_of::<ReplyObserver<A>>(),
            physical_job_bytes: size_of::<ReplyNotificationJob<A>>(),
            maximum_retained_wakers: 2,
            maximum_transition_wakers: 3,
            panic_payload_slots: 3,
        }
    }
}
