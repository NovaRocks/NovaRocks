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

//! Custody for one already admitted continuation role. Publication grants
//! permission; a host explicitly invokes it and owns every foreign destructor.

use crate::{
    completion::ReportSlot,
    lease::{CapacityBundle, WorkLease},
    sync::{Arc, AtomicBool, Mutex, Ordering},
    transaction::RuntimeError,
};
use novarocks_control_runtime_core::{identity::Generation, scope::ScopeError};
use std::{
    any::Any,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
};

pub type ContinuationCallback<T> = Box<dyn FnOnce(T) + Send + 'static>;
type PanicPayload = ManuallyDrop<Box<dyn Any + Send + 'static>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContinuationPhase {
    AwaitingRegistration,
    Registered,
    Ready,
    Running,
    Unused,
    Exited,
    Unresolved,
    Retired,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContinuationError {
    InvalidPhase(ContinuationPhase),
    DispatchBusy,
    Runtime(RuntimeError),
}
impl From<RuntimeError> for ContinuationError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}
impl std::fmt::Display for ContinuationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "continuation custody failure: {self:?}")
    }
}
impl std::error::Error for ContinuationError {}

/// The runtime returns this neutral observation; application failure policy
/// and escalation remain with the host. A panic never proves actual exit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvocationResult {
    Exited,
    Unresolved,
}

struct State<T: Send + 'static> {
    phase: ContinuationPhase,
    callback: Option<ManuallyDrop<ContinuationCallback<T>>>,
    value: Option<ManuallyDrop<T>>,
    work: Option<WorkLease>,
    capacity: Option<CapacityBundle>,
    panic: Option<PanicPayload>,
    body_exited: bool,
    captures_exited: bool,
}
struct Custody<T: Send + 'static> {
    state: Mutex<State<T>>,
    report: ReportSlot,
    // The original work, captures and panic remain owned if external handles
    // disappear. Only successful explicit retirement breaks this custody cycle.
    retained: Mutex<Option<Arc<Self>>>,
    dispatching: AtomicBool,
}

/// One nonblocking host execution claim over preparation, optional input
/// refresh, invocation and capture disposal. No mutex spans foreign work.
/// Mutable access prevents two executors from refreshing one ready call with
/// independently read snapshots. The claim grants no work/registration credit.
pub struct ContinuationDispatch<T: Send + 'static> {
    slot: ContinuationSlot<T>,
}
impl<T: Send + 'static> Drop for ContinuationDispatch<T> {
    fn drop(&mut self) {
        self.slot.0.dispatching.store(false, Ordering::Release);
    }
}
impl<T: Send + 'static> ContinuationDispatch<T> {
    pub fn refresh_ready_value(&mut self, value: T) -> Result<T, (ContinuationError, T)> {
        self.slot.refresh_ready_value(value)
    }
    pub fn invoke(&mut self) -> Result<InvocationResult, ContinuationError> {
        self.slot.invoke_guarded(None)
    }
    pub fn invoke_in_generation(
        &mut self,
        generation: Generation,
    ) -> Result<InvocationResult, ContinuationError> {
        self.slot.invoke_guarded(Some(generation))
    }
}

/// Clones address the same prepaid role; they cannot issue another token.
#[must_use = "An admitted continuation must retain custody until actual retirement"]
pub struct ContinuationSlot<T: Send + 'static>(Arc<Custody<T>>);
impl<T: Send + 'static> Clone for ContinuationSlot<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<T: Send + 'static> std::fmt::Debug for ContinuationSlot<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContinuationSlot")
            .field("phase", &self.phase())
            .finish()
    }
}

/// The unique registration authority. Dropping it revokes unused authority;
/// the original holder still owes its independent actual-exit observation.
#[must_use = "Dropping authority alone does not retire its admitted work"]
pub struct RegistrationToken<T: Send + 'static> {
    slot: ContinuationSlot<T>,
    active: bool,
}
impl<T: Send + 'static> std::fmt::Debug for RegistrationToken<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegistrationToken")
            .field("active", &self.active)
            .finish()
    }
}
impl<T: Send + 'static> RegistrationToken<T> {
    /// Consumes the only authority and stores captures without invoking or
    /// disposing them. Even an unexpected rejection returns the original box.
    pub fn register(
        mut self,
        callback: ContinuationCallback<T>,
    ) -> Result<(), (ContinuationError, ContinuationCallback<T>)> {
        let callback = ManuallyDrop::new(callback);
        let mut state = self.slot.0.state.lock().expect("continuation custody lock");
        if state.phase != ContinuationPhase::AwaitingRegistration {
            let error = ContinuationError::InvalidPhase(state.phase);
            drop(state);
            return Err((error, ManuallyDrop::into_inner(callback)));
        }
        state.callback = Some(callback);
        state.phase = ContinuationPhase::Registered;
        self.active = false;
        Ok(())
    }
}
impl<T: Send + 'static> Drop for RegistrationToken<T> {
    fn drop(&mut self) {
        if self.active {
            let mut state = self.slot.0.state.lock().expect("continuation custody lock");
            if state.phase == ContinuationPhase::AwaitingRegistration {
                state.phase = ContinuationPhase::Unused;
            }
        }
    }
}

impl<T: Send + 'static> ContinuationSlot<T> {
    /// Takes the original admitted work, matching capacity bundle and report.
    /// It never signs work, allocates report credit or admits a replacement role.
    /// The caller must supply all three artifacts from that same admitted role.
    pub fn from_admitted(
        work: WorkLease,
        capacity: CapacityBundle,
        report: ReportSlot,
    ) -> (Self, RegistrationToken<T>) {
        let custody = Arc::new(Custody {
            state: Mutex::new(State {
                phase: ContinuationPhase::AwaitingRegistration,
                callback: None,
                value: None,
                work: Some(work),
                capacity: Some(capacity),
                panic: None,
                body_exited: false,
                captures_exited: false,
            }),
            report,
            retained: Mutex::new(None),
            dispatching: AtomicBool::new(false),
        });
        *custody
            .retained
            .lock()
            .expect("continuation retention lock") = Some(custody.clone());
        let slot = Self(custody);
        let token = RegistrationToken {
            slot: slot.clone(),
            active: true,
        };
        (slot, token)
    }
    pub fn phase(&self) -> ContinuationPhase {
        self.0
            .state
            .lock()
            .expect("continuation custody lock")
            .phase
    }
    pub fn try_claim_dispatch(&self) -> Option<ContinuationDispatch<T>> {
        self.0
            .dispatching
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| ContinuationDispatch { slot: self.clone() })
    }
    pub fn body_exited(&self) -> bool {
        self.0
            .state
            .lock()
            .expect("continuation custody lock")
            .body_exited
    }
    pub fn captures_exited(&self) -> bool {
        self.0
            .state
            .lock()
            .expect("continuation custody lock")
            .captures_exited
    }
    /// Read-only custody evidence. The original panic payload is never cloned,
    /// formatted or dropped here, and it does not establish capture disposal.
    pub fn has_panic_payload(&self) -> bool {
        self.0
            .state
            .lock()
            .expect("continuation custody lock")
            .panic
            .is_some()
    }
    /// The protocol's ordered barrier commits its actual input value. Only a
    /// registered role becomes ready, and rejection returns T after unlocking.
    pub fn publish_barrier(&self, value: T) -> Result<(), (ContinuationError, T)> {
        let value = ManuallyDrop::new(value);
        let mut state = self.0.state.lock().expect("continuation custody lock");
        if state.phase != ContinuationPhase::Registered {
            let error = ContinuationError::InvalidPhase(state.phase);
            drop(state);
            return Err((error, ManuallyDrop::into_inner(value)));
        }
        state.value = Some(value);
        state.phase = ContinuationPhase::Ready;
        Ok(())
    }
    /// A previously committed barrier remains valid after owner failure, but
    /// its input is not a frozen result. Before a delayed start, the host may
    /// supply its latest actually published projection. Both old and rejected
    /// values return to the host; no foreign destructor runs under this lock.
    fn refresh_ready_value(&self, value: T) -> Result<T, (ContinuationError, T)> {
        let value = ManuallyDrop::new(value);
        let mut state = self.0.state.lock().expect("continuation custody lock");
        if state.phase != ContinuationPhase::Ready {
            let error = ContinuationError::InvalidPhase(state.phase);
            drop(state);
            return Err((error, ManuallyDrop::into_inner(value)));
        }
        let old = state.value.replace(value).expect("published barrier input");
        drop(state);
        Ok(ManuallyDrop::into_inner(old))
    }
    /// Runs only on a host executor. No custody, inbox or ledger lock spans the
    /// callback call, argument disposal, capture disposal or panic unwinding.
    pub fn invoke(&self) -> Result<InvocationResult, ContinuationError> {
        self.try_claim_dispatch()
            .ok_or(ContinuationError::DispatchBusy)?
            .invoke()
    }
    /// Retryable generation rejection keeps the complete registered callback,
    /// published input and original work ready for the failure owner. It is
    /// neither a callback panic nor evidence of unresolved physical exit.
    pub fn invoke_in_generation(
        &self,
        generation: Generation,
    ) -> Result<InvocationResult, ContinuationError> {
        self.try_claim_dispatch()
            .ok_or(ContinuationError::DispatchBusy)?
            .invoke_in_generation(generation)
    }
    fn invoke_guarded(
        &self,
        generation: Option<Generation>,
    ) -> Result<InvocationResult, ContinuationError> {
        let (callback, value, mut work) = {
            let mut state = self.0.state.lock().expect("continuation custody lock");
            if state.phase != ContinuationPhase::Ready {
                return Err(ContinuationError::InvalidPhase(state.phase));
            }
            state.phase = ContinuationPhase::Running;
            (
                state.callback.take().expect("registered continuation"),
                state.value.take().expect("published continuation input"),
                state.work.take().expect("original continuation work"),
            )
        };
        let start = match generation {
            Some(generation) => work.start_in_generation(generation),
            None => work.start(),
        };
        if let Err(error) = start {
            let mut state = self.0.state.lock().expect("continuation custody lock");
            state.callback = Some(callback);
            state.value = Some(value);
            state.work = Some(work);
            if error == RuntimeError::Scope(ScopeError::GenerationMismatch) {
                state.phase = ContinuationPhase::Ready;
                return Err(error.into());
            }
            state.phase = ContinuationPhase::Unresolved;
            state
                .work
                .as_ref()
                .expect("original continuation work")
                .mark_unresolved();
            return Err(error.into());
        }
        let invocation = catch_unwind(AssertUnwindSafe(|| {
            ManuallyDrop::into_inner(callback)(ManuallyDrop::into_inner(value));
        }));
        match invocation {
            Ok(()) => self.observe_actual_exit(work),
            Err(payload) => {
                let payload = ManuallyDrop::new(payload);
                let mut state = self.0.state.lock().expect("continuation custody lock");
                state.panic = Some(payload);
                state.work = Some(work);
                state.phase = ContinuationPhase::Unresolved;
                state
                    .work
                    .as_ref()
                    .expect("original continuation work")
                    .mark_unresolved();
                Ok(InvocationResult::Unresolved)
            }
        }
    }
    /// Positive evidence supplied by the original unused holder, after its body
    /// and captures really exited. Token Drop deliberately cannot call this.
    pub fn unused_role_exited(&self) -> Result<InvocationResult, ContinuationError> {
        let work = {
            let mut state = self.0.state.lock().expect("continuation custody lock");
            if state.phase != ContinuationPhase::Unused {
                return Err(ContinuationError::InvalidPhase(state.phase));
            }
            state.phase = ContinuationPhase::Running;
            state.work.take().expect("original unused role work")
        };
        self.observe_actual_exit(work)
    }
    fn observe_actual_exit(
        &self,
        mut work: WorkLease,
    ) -> Result<InvocationResult, ContinuationError> {
        let mut body_exited = false;
        let mut captures_exited = false;
        let observation = (|| -> Result<(), RuntimeError> {
            work.body_exited()?;
            body_exited = true;
            work.captures_exited_with_report(&self.0.report)?;
            captures_exited = true;
            self.0.report.body_exited()?;
            self.0.report.captures_exited()?;
            Ok(())
        })();
        let mut state = self.0.state.lock().expect("continuation custody lock");
        state.work = Some(work);
        state.body_exited = body_exited;
        state.captures_exited = captures_exited;
        state.phase = if observation.is_ok() {
            ContinuationPhase::Exited
        } else {
            ContinuationPhase::Unresolved
        };
        if observation.is_err() {
            state
                .work
                .as_ref()
                .expect("original continuation work")
                .mark_unresolved();
        }
        observation
            .map(|()| InvocationResult::Exited)
            .map_err(Into::into)
    }
    /// The reliable report's consumers independently consume or abandon their
    /// own legs. Capacity is released only after actual exit of this work and
    /// every admitted child, plus every report leg's retirement. Timeout,
    /// authority Drop and owner failure cannot replace those observations.
    pub fn try_retire(&self) -> Result<bool, ContinuationError> {
        let mut state = self.0.state.lock().expect("continuation custody lock");
        if state.phase != ContinuationPhase::Exited {
            return Ok(false);
        }
        if !state
            .work
            .as_ref()
            .expect("original continuation work")
            .actual_exit_ready()?
        {
            return Ok(false);
        }
        if !self.0.report.try_retire()? {
            return Ok(false);
        }
        let capacity = state
            .capacity
            .as_mut()
            .expect("original continuation capacity");
        let resources = capacity.resources()?;
        if let Err(error) = capacity.release(&resources) {
            state.phase = ContinuationPhase::Unresolved;
            state
                .work
                .as_ref()
                .expect("original continuation work")
                .mark_unresolved();
            return Err(error.into());
        }
        let capacity = state.capacity.take();
        let work = state.work.take();
        state.phase = ContinuationPhase::Retired;
        drop(state);
        drop(work);
        drop(capacity);
        let retained = self
            .0
            .retained
            .lock()
            .expect("continuation retention lock")
            .take();
        drop(retained);
        Ok(true)
    }
}
