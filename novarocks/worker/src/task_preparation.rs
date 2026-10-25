// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Reservation-owned preparation exit, preceding optional Task acceptance.
//! No observer here owns a session, Task control, reporter, or execution work.

use crate::failure_reason::WorkerFailureReason as R;
use novarocks_execution::runtime::execution_health::{ExecutionExitOrigin, ExecutionHealth};
use novarocks_execution_contract::failure::{FailureClass, FailureFact, FailureProjection};
use novarocks_execution_contract::{TaskFailurePhase, TaskIdentity};
use std::{
    any::Any,
    fmt,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskPreparationChildExit {
    Pending,
    Exited,
    Unresolved,
}

/// A provider's one whole child observation. Methods only manipulate its
/// original cancellation/exit metadata; they must never run producer work.
pub trait TaskPreparationChild: Send + Sync {
    fn request_rollback(&self);
    fn poll_exit(&self) -> TaskPreparationChildExit;
    fn adopt_runtime(&self);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskPreparationChildRegistrationFailure {
    AlreadyRegistered,
    ScopeClosed,
}
impl fmt::Display for TaskPreparationChildRegistrationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AlreadyRegistered => "preparation already owns its whole child scope",
            Self::ScopeClosed => {
                "preparation child registration requires the original active work scope"
            }
        })
    }
}
impl std::error::Error for TaskPreparationChildRegistrationFailure {}
impl FailureProjection for TaskPreparationChildRegistrationFailure {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Invariant,
            R::PreparationChildRegistration.reason(),
        )
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Mode {
    Preparing,
    Rollback,
    Adopting,
    Adopted,
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    NotStarted,
    Started,
    Unresolved,
    Exited,
}
#[derive(Clone, Copy)]
#[repr(usize)]
enum Step {
    Cancel,
    Inbound,
    Receiver,
    Poll,
    Adopt,
    ChildDrop,
    Publication,
    StandDown,
    BodyPanic,
}
pub(crate) const STEPS: usize = Step::BodyPanic as usize + 1;
// Each role belongs to one lexically bounded, once-only body boundary. A
// retained-bytes unwind and its independent input destructor may both fail.
#[derive(Clone, Copy)]
#[repr(usize)]
pub(crate) enum PreparationUnwindRole {
    Admission,
    Preparation,
    Input,
    Tail,
}
pub(crate) const BODY_ROLES: usize = PreparationUnwindRole::Tail as usize + 1;
type Payload = Box<dyn Any + Send>;
type PanicCustody = [Option<ManuallyDrop<Payload>>; STEPS];

struct State {
    mode: Mode,
    body_exited: bool,
    registered: bool,
    settled: bool,
    handed_off: bool,
    admission_exited: bool,
    body_roles: [Phase; BODY_ROLES],
    body_payloads: [Option<ManuallyDrop<Payload>>; BODY_ROLES],
    body_secondary: [Option<ManuallyDrop<Payload>>; BODY_ROLES],
    body_publication: [Option<ManuallyDrop<Payload>>; BODY_ROLES],
    child: Option<Arc<dyn TaskPreparationChild>>,
    phases: [Phase; STEPS],
    // Cold, bounded process custody. Never place opaque panic payloads in a
    // Task's originating failure graph or invoke their destructor on exit.
    panics: Option<Box<PanicCustody>>,
}

pub(crate) struct TaskPreparationScope {
    identity: TaskIdentity,
    health: Arc<ExecutionHealth>,
    // A reserved input remains owned even before there is Accepted Task status.
    // This separate, private loan lock never spans foreign destruction.
    input: Mutex<Option<ManuallyDrop<novarocks_execution_contract::TaskCreationInput>>>,
    state: Mutex<State>,
}

#[derive(Clone, Copy)]
pub(crate) enum PreparationCleanup {
    Inbound,
    Receiver,
}

impl TaskPreparationScope {
    pub(crate) fn new(identity: TaskIdentity, health: Arc<ExecutionHealth>) -> Self {
        Self {
            identity,
            health,
            input: Mutex::new(None),
            state: Mutex::new(State {
                mode: Mode::Preparing,
                body_exited: false,
                registered: false,
                settled: false,
                handed_off: false,
                admission_exited: true,
                body_roles: [Phase::NotStarted; BODY_ROLES],
                body_payloads: std::array::from_fn(|_| None),
                body_secondary: std::array::from_fn(|_| None),
                body_publication: std::array::from_fn(|_| None),
                child: None,
                phases: [Phase::NotStarted; STEPS],
                panics: None,
            }),
        }
    }
    pub(crate) fn identity(&self) -> TaskIdentity {
        self.identity
    }
    pub(crate) fn install_input(&self, input: novarocks_execution_contract::TaskCreationInput) {
        let mut slot = self.input.lock().expect("preparation input loan lock");
        assert!(slot.is_none(), "one original reserved input");
        *slot = Some(ManuallyDrop::new(input));
    }
    pub(crate) fn input_retained_bytes(&self) -> usize {
        let input = self.input.lock().expect("preparation input loan lock");
        // Catch while borrowing the holder, then release the loan normally.
        // A foreign size method cannot poison the original holder's mutex.
        let measured = catch_unwind(AssertUnwindSafe(|| {
            input
                .as_ref()
                .expect("original reserved input")
                .retained_bytes()
        }));
        drop(input);
        match measured {
            Ok(bytes) => bytes,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }
    pub(crate) fn take_input(&self) -> novarocks_execution_contract::TaskCreationInput {
        let input = self
            .input
            .lock()
            .expect("preparation input loan lock")
            .take()
            .expect("one original reserved input transfer");
        ManuallyDrop::into_inner(input)
    }
    pub(crate) fn dispose_input(&self) -> Result<(), Payload> {
        let input = self
            .input
            .lock()
            .expect("preparation input loan lock")
            .take();
        catch_unwind(AssertUnwindSafe(|| {
            drop(input.map(ManuallyDrop::into_inner))
        }))
    }
    pub(crate) fn begin_admission(&self) {
        self.state
            .lock()
            .expect("preparation child scope lock")
            .admission_exited = false;
    }
    pub(crate) fn admission_exited(&self) {
        self.state
            .lock()
            .expect("preparation child scope lock")
            .admission_exited = true;
    }
    pub(crate) fn hand_off_body(&self) {
        self.state
            .lock()
            .expect("preparation child scope lock")
            .handed_off = true;
    }
    pub(crate) fn body_handed_off(&self) -> bool {
        self.state
            .lock()
            .expect("preparation child scope lock")
            .handed_off
    }
    fn body_obligations_exited(state: &State) -> bool {
        state.admission_exited
            && state
                .body_roles
                .iter()
                .all(|phase| matches!(phase, Phase::NotStarted | Phase::Exited))
            && state.body_publication.iter().all(Option::is_none)
    }
    pub(crate) fn allows_runtime_exit_publication(&self) -> bool {
        let state = self.state.lock().expect("preparation child scope lock");
        // Preserve the established normal no-child publication tail. Only an
        // actual body-unwind obligation closes this gate, before its author
        // can wake any runtime cleanup through the Task conclusion port.
        state
            .body_roles
            .iter()
            .all(|phase| matches!(phase, Phase::NotStarted | Phase::Exited))
            && state.body_publication.iter().all(Option::is_none)
    }
    fn report_body_fault(&self, reason: R) {
        // Identity here is neutral location metadata, including before Accepted.
        self.health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: self.identity,
                phase: TaskFailurePhase::Preparation,
            },
            FailureFact::new(FailureClass::Unexpected, reason.reason()),
        );
    }
    pub(crate) fn retain_body_panic(&self, role: PreparationUnwindRole, payload: Payload) {
        let payload = ManuallyDrop::new(payload);
        let mut state = self.state.lock().expect("preparation child scope lock");
        let role = role as usize;
        assert!(
            state.body_roles[role] == Phase::NotStarted,
            "one unwind per original body boundary"
        );
        state.body_roles[role] = Phase::Started;
        state.phases[Step::BodyPanic as usize] = Phase::Started;
        state.body_payloads[role] = Some(payload);
    }
    pub(crate) fn body_failure_publication_panicked(
        &self,
        role: PreparationUnwindRole,
        payload: Payload,
    ) {
        let payload = ManuallyDrop::new(payload);
        {
            let mut state = self.state.lock().expect("preparation child scope lock");
            let slot = &mut state.body_publication[role as usize];
            assert!(slot.is_none(), "one original failure publication attempt");
            *slot = Some(payload);
            state.phases[Step::BodyPanic as usize] = Phase::Unresolved;
        }
        self.report_body_fault(R::PreparationExitPublication);
    }
    pub(crate) fn dispose_body_panic(&self, role: PreparationUnwindRole) {
        let role = role as usize;
        let payload = {
            let mut state = self.state.lock().expect("preparation child scope lock");
            assert!(
                state.body_roles[role] == Phase::Started,
                "once-only opaque disposal"
            );
            state.body_payloads[role]
                .take()
                .expect("original opaque payload custody")
        };
        let dropped = catch_unwind(AssertUnwindSafe(|| drop(ManuallyDrop::into_inner(payload))));
        let mut state = self.state.lock().expect("preparation child scope lock");
        match dropped {
            Ok(()) => state.body_roles[role] = Phase::Exited,
            Err(payload) => {
                state.body_roles[role] = Phase::Unresolved;
                state.body_secondary[role] = Some(ManuallyDrop::new(payload));
                state.phases[Step::BodyPanic as usize] = Phase::Unresolved;
                drop(state);
                self.report_body_fault(R::PreparationPanicPayloadDrop);
                return;
            }
        }
        let opaque_exited = state
            .body_roles
            .iter()
            .all(|phase| matches!(phase, Phase::NotStarted | Phase::Exited))
            && state.body_publication.iter().all(Option::is_none);
        state.phases[Step::BodyPanic as usize] = if opaque_exited {
            Phase::Exited
        } else {
            Phase::Unresolved
        };
    }

    pub(crate) fn register(
        &self,
        child: Arc<dyn TaskPreparationChild>,
    ) -> Result<(), TaskPreparationChildRegistrationFailure> {
        let mut state = self.state.lock().expect("preparation child scope lock");
        if state.mode != Mode::Preparing || state.body_exited {
            drop(state);
            return Err(TaskPreparationChildRegistrationFailure::ScopeClosed);
        }
        if state.registered {
            drop(state);
            return Err(TaskPreparationChildRegistrationFailure::AlreadyRegistered);
        }
        state.registered = true;
        state.child = Some(child);
        Ok(())
    }
    fn fault(&self, reason: R, step: Step, payload: Option<Payload>) {
        // Wrap custody before acquiring a lock. No return/unwind path here
        // implicitly invokes an unknown payload's destructor.
        let payload = payload.map(ManuallyDrop::new);
        {
            let mut state = self.state.lock().expect("preparation child scope lock");
            state.phases[step as usize] = Phase::Unresolved;
            if let Some(payload) = payload {
                let custody = state
                    .panics
                    .get_or_insert_with(|| Box::new(std::array::from_fn(|_| None)));
                if custody[step as usize].is_none() {
                    custody[step as usize] = Some(payload);
                }
            }
        }
        self.health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: self.identity,
                phase: TaskFailurePhase::Preparation,
            },
            FailureFact::new(FailureClass::Unexpected, reason.reason()),
        );
    }
    fn is_unresolved(&self, step: Step) -> bool {
        self.state
            .lock()
            .expect("preparation child scope lock")
            .phases[step as usize]
            == Phase::Unresolved
    }
    fn once(&self, step: Step, reason: R, action: impl FnOnce()) -> bool {
        {
            let mut state = self.state.lock().expect("preparation child scope lock");
            match state.phases[step as usize] {
                Phase::Exited => return true,
                Phase::Started | Phase::Unresolved => return false,
                Phase::NotStarted => state.phases[step as usize] = Phase::Started,
            }
        }
        match catch_unwind(AssertUnwindSafe(action)) {
            Ok(()) => {
                self.state
                    .lock()
                    .expect("preparation child scope lock")
                    .phases[step as usize] = Phase::Exited;
                true
            }
            Err(payload) => {
                self.fault(reason, step, Some(payload));
                false
            }
        }
    }
    pub(crate) fn request_rollback(&self) {
        let child = {
            let mut state = self.state.lock().expect("preparation child scope lock");
            state.mode = Mode::Rollback;
            state.child.clone()
        };
        // Independent from every cleanup/poll/adoption phase. A failed hook
        // must never suppress the original Root cancellation request.
        self.once(Step::Cancel, R::PreparationChildRollback, || {
            if let Some(child) = &child {
                child.request_rollback();
            }
        });
        drop(child);
    }
    pub(crate) fn cleanup(&self, kind: PreparationCleanup, action: impl FnOnce()) {
        let (step, reason) = match kind {
            PreparationCleanup::Inbound => (Step::Inbound, R::PreparationInboundCleanup),
            PreparationCleanup::Receiver => (Step::Receiver, R::PreparationReceiverCleanup),
        };
        self.once(step, reason, action);
    }
    pub(crate) fn physical_bodies_exited(&self) -> bool {
        let state = self.state.lock().expect("preparation child scope lock");
        state.body_exited && state.admission_exited
    }
    pub(crate) fn body_exited(&self) {
        self.state
            .lock()
            .expect("preparation child scope lock")
            .body_exited = true;
    }
    fn clear_child(&self) -> bool {
        let child = {
            let mut state = self.state.lock().expect("preparation child scope lock");
            match state.phases[Step::ChildDrop as usize] {
                Phase::Exited => return true,
                Phase::Started | Phase::Unresolved => return false,
                Phase::NotStarted => state.phases[Step::ChildDrop as usize] = Phase::Started,
            }
            state.child.take()
        };
        match catch_unwind(AssertUnwindSafe(|| drop(child))) {
            Ok(()) => {
                self.state
                    .lock()
                    .expect("preparation child scope lock")
                    .phases[Step::ChildDrop as usize] = Phase::Exited;
                true
            }
            Err(payload) => {
                self.fault(R::PreparationChildDrop, Step::ChildDrop, Some(payload));
                false
            }
        }
    }
    pub(crate) fn rollback_exited(&self) -> bool {
        let child = {
            let mut state = self.state.lock().expect("preparation child scope lock");
            if !state.body_exited
                || state.mode != Mode::Rollback
                || [Step::Cancel, Step::Inbound, Step::Receiver]
                    .iter()
                    .any(|step| state.phases[*step as usize] != Phase::Exited)
            {
                return false;
            }
            match state.phases[Step::Poll as usize] {
                Phase::Started | Phase::Unresolved => return false,
                Phase::Exited => {
                    drop(state);
                    let child_exited = self.clear_child();
                    let state = self.state.lock().expect("preparation child scope lock");
                    return child_exited && Self::body_obligations_exited(&state);
                }
                Phase::NotStarted => state.phases[Step::Poll as usize] = Phase::Started,
            }
            state.child.clone()
        };
        let result = catch_unwind(AssertUnwindSafe(|| {
            child
                .as_ref()
                .map_or(TaskPreparationChildExit::Exited, |child| child.poll_exit())
        }));
        drop(child); // The scope still owns the original Arc; this is metadata only.
        match result {
            Ok(TaskPreparationChildExit::Pending) => {
                self.state
                    .lock()
                    .expect("preparation child scope lock")
                    .phases[Step::Poll as usize] = Phase::NotStarted;
                false
            }
            Ok(TaskPreparationChildExit::Exited) => {
                self.state
                    .lock()
                    .expect("preparation child scope lock")
                    .phases[Step::Poll as usize] = Phase::Exited;
                let child_exited = self.clear_child();
                let state = self.state.lock().expect("preparation child scope lock");
                child_exited && Self::body_obligations_exited(&state)
            }
            Ok(TaskPreparationChildExit::Unresolved) => {
                self.fault(R::PreparationChildUnresolved, Step::Poll, None);
                false
            }
            Err(payload) => {
                self.fault(R::PreparationChildPoll, Step::Poll, Some(payload));
                false
            }
        }
    }
    pub(crate) fn adopt_runtime(&self) -> bool {
        let child = {
            let mut state = self.state.lock().expect("preparation child scope lock");
            if !state.body_exited || !state.admission_exited {
                return false;
            }
            match state.mode {
                Mode::Rollback => return false,
                Mode::Adopted => return true,
                Mode::Preparing => state.mode = Mode::Adopting,
                Mode::Adopting => {}
            }
            state.child.clone()
        };
        let adopted = self.once(Step::Adopt, R::PreparationChildAdopt, || {
            if let Some(child) = &child {
                child.adopt_runtime();
            }
        });
        drop(child);
        if !adopted {
            if self.is_unresolved(Step::Adopt) {
                self.request_rollback();
            }
            return false;
        }
        // adopt_runtime clears the provider's Preparation observation role.
        // Retain only its metadata cancellation handle until commit returns,
        // so an unknown commit cannot suppress independent Root cancellation.
        self.state
            .lock()
            .expect("preparation child scope lock")
            .mode = Mode::Adopted;
        true
    }
    pub(crate) fn stand_down(&self, action: impl FnOnce()) -> bool {
        let done = self.once(Step::StandDown, R::PreparationRuntimeStandDown, action);
        if !done && self.is_unresolved(Step::StandDown) {
            self.request_rollback();
        }
        done
    }
    /// Runtime activation is independent owned cleanup work. Unknown opaque
    /// body custody keeps the original P, but cannot strand installed operators
    /// after their original Task cause has already stood them down.
    pub(crate) fn start_runtime(&self, action: impl FnOnce()) -> bool {
        if !self.once(Step::Publication, R::PreparationExitPublication, action) {
            if self.is_unresolved(Step::Publication) {
                self.request_rollback();
            }
            return false;
        }
        let child_retired = self.clear_child();
        let state = self.state.lock().expect("preparation child scope lock");
        child_retired && Self::body_obligations_exited(&state)
    }
    pub(crate) fn finish(&self, action: impl FnOnce()) -> bool {
        if !Self::body_obligations_exited(&self.state.lock().expect("preparation child scope lock"))
        {
            return false;
        }
        // Claim before any observable publication. A panic is never retried.
        if !self.once(Step::Publication, R::PreparationExitPublication, action) {
            if self.is_unresolved(Step::Publication) {
                self.request_rollback();
            }
            return false;
        }
        self.clear_child()
    }
    /// A legacy caller may supply genuine runtime actual-stop before its
    /// preparation's final metadata tail returned. Only an originally empty
    /// child scope can complete that way; a retired record is not evidence.
    pub(crate) fn complete_empty_after_actual_runtime_exit(
        &self,
        actual_stopped: bool,
        publish: impl FnOnce(),
    ) -> bool {
        let empty_body_exited = {
            let state = self.state.lock().expect("preparation child scope lock");
            state.body_exited && !state.registered
        };
        actual_stopped && empty_body_exited && self.adopt_runtime() && self.finish(publish)
    }
    pub(crate) fn claim_settlement(&self) -> bool {
        let mut state = self.state.lock().expect("preparation child scope lock");
        if state.settled {
            return false;
        }
        state.settled = true;
        true
    }
}
