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
    Exited { output: TaskPreparationOutputExit },
    Unresolved,
}

/// Neutral output evidence from the same original physical child owner.
/// Child exit alone does not establish that no output exists or has exited.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskPreparationOutputExit {
    NoOutputCreated,
    ActualOutputExited,
    Unknown,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
    output_exit: Option<TaskPreparationOutputExit>,
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

/// Positive evidence from the same original scope: its physical preparation
/// returned, every rollback callback/child exited, and runtime adoption never
/// began. This is neither a whole-preparation report nor source/Work exit.
/// The borrow preserves the exact original scope without another owner or grant.
pub(crate) struct NeverAdoptedPreparationExit<'a> {
    scope: &'a TaskPreparationScope,
}
impl NeverAdoptedPreparationExit<'_> {
    pub(crate) fn identity(&self) -> TaskIdentity {
        self.scope.identity
    }
    pub(crate) fn same_scope(&self, scope: &TaskPreparationScope) -> bool {
        std::ptr::eq(self.scope, scope)
    }
    /// Expose only the fact cached from this scope's original physical Poll.
    /// Construction already proved ChildDrop and all never-adopted exits.
    pub(crate) fn output_exit(&self) -> TaskPreparationOutputExit {
        self.scope
            .state
            .lock()
            .expect("original preparation output exit")
            .output_exit
            .expect("the original physical Poll supplied output evidence")
    }
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
                output_exit: None,
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
        self.run_claimed(step, reason, action)
    }
    /// The original phase was claimed in the same transaction as its mode
    /// gate. Its callback and capture disposal are both inside this boundary.
    fn run_claimed(&self, step: Step, reason: R, action: impl FnOnce()) -> bool {
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
    /// Read only the original provider-child cleanup observations. Adoption
    /// transfers runtime ownership elsewhere; this proves only that these
    /// preparation callbacks/metadata captures have actually exited.
    pub(crate) fn host_captures_exited(&self) -> bool {
        let state = self.state.lock().expect("preparation child scope lock");
        state.body_exited
            && Self::body_obligations_exited(&state)
            && Self::started_actions_exited(&state)
            && state.phases[Step::ChildDrop as usize] == Phase::Exited
            && match state.mode {
                Mode::Adopted => state.phases[Step::Publication as usize] == Phase::Exited,
                Mode::Rollback => {
                    state.phases[Step::Poll as usize] == Phase::Exited
                        && [Step::Cancel, Step::Inbound, Step::Receiver]
                            .iter()
                            .all(|step| state.phases[*step as usize] == Phase::Exited)
                }
                Mode::Preparing | Mode::Adopting => false,
            }
    }
    /// Never-started optional cleanup roles impose no exit obligation. Once
    /// a real call starts, however, adoption cannot hide its live captures or
    /// unresolved exit by switching to another lifecycle branch.
    fn started_actions_exited(state: &State) -> bool {
        state
            .phases
            .iter()
            .all(|phase| matches!(phase, Phase::NotStarted | Phase::Exited))
    }
    fn clear_child(&self) -> bool {
        let child = {
            let mut state = self.state.lock().expect("preparation child scope lock");
            match state.phases[Step::ChildDrop as usize] {
                Phase::Exited => return true,
                Phase::Started | Phase::Unresolved => return false,
                Phase::NotStarted => {}
            }
            if !Self::started_actions_exited(&state)
                || (state.mode == Mode::Rollback
                    && [Step::Cancel, Step::Inbound, Step::Receiver, Step::Poll]
                        .iter()
                        .any(|step| state.phases[*step as usize] != Phase::Exited))
            {
                return false;
            }
            state.phases[Step::ChildDrop as usize] = Phase::Started;
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
        let (child, registered) = {
            let mut state = self.state.lock().expect("preparation child scope lock");
            if !state.body_exited
                || state.mode != Mode::Rollback
                || !Self::started_actions_exited(&state)
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
            (state.child.clone(), state.registered)
        };
        let result = catch_unwind(AssertUnwindSafe(|| {
            match child.as_ref() {
                Some(child) => child.poll_exit(),
                // The original registration gate is permanently sealed by
                // physical rollback; no registered child ever created output.
                None if !registered => TaskPreparationChildExit::Exited {
                    output: TaskPreparationOutputExit::NoOutputCreated,
                },
                None => TaskPreparationChildExit::Exited {
                    output: TaskPreparationOutputExit::Unknown,
                },
            }
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
            Ok(TaskPreparationChildExit::Exited { output }) => {
                {
                    let mut state = self.state.lock().expect("preparation child scope lock");
                    state.output_exit = Some(output);
                    state.phases[Step::Poll as usize] = Phase::Exited;
                }
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
    /// Read only positive observations already completed by the original
    /// physical rollback owner. In particular this never calls poll_exit or
    /// drops a child on a metadata/poller thread. Rollback permanently rejects
    /// subsequent adoption/registration; the original Adopt phase distinguishes
    /// never-adopted from an adopted runtime that later entered rollback.
    pub(crate) fn never_adopted_rollback_exit(&self) -> Option<NeverAdoptedPreparationExit<'_>> {
        let state = self.state.lock().expect("preparation child scope lock");
        if state.mode != Mode::Rollback
            || state.phases[Step::Adopt as usize] != Phase::NotStarted
            || !state.body_exited
            || !Self::body_obligations_exited(&state)
            || !Self::started_actions_exited(&state)
            || state.child.is_some()
            || state.output_exit.is_none()
            || [
                Step::Cancel,
                Step::Inbound,
                Step::Receiver,
                Step::Poll,
                Step::ChildDrop,
            ]
            .iter()
            .any(|step| state.phases[*step as usize] != Phase::Exited)
        {
            return None;
        }
        Some(NeverAdoptedPreparationExit { scope: self })
    }
    pub(crate) fn output_exit_unresolved(&self) {
        if !self.is_unresolved(Step::Poll) {
            self.fault(R::PreparationChildUnresolved, Step::Poll, None);
        }
    }
    pub(crate) fn adopt_runtime(&self) -> bool {
        let child = {
            let mut state = self.state.lock().expect("preparation child scope lock");
            tracing::debug!(pid = std::process::id(), task = ?self.identity,
                mode = ?state.mode, body_exited = state.body_exited,
                admission_exited = state.admission_exited,
                child_present = state.child.is_some(),
                "Original preparation received its physical activation attempt");
            if !state.body_exited || !state.admission_exited {
                return false;
            }
            match state.mode {
                Mode::Rollback => return false,
                Mode::Adopted => return true,
                Mode::Preparing | Mode::Adopting => {}
            }
            // Claim in this same mode transaction. An earlier rollback must
            // not be followed by a newly started provider adoption callback.
            if state.phases[Step::Adopt as usize] != Phase::NotStarted {
                return false;
            }
            state.mode = Mode::Adopting;
            state.phases[Step::Adopt as usize] = Phase::Started;
            state.child.clone()
        };
        let adopted = self.run_claimed(Step::Adopt, R::PreparationChildAdopt, move || {
            if let Some(child) = &child {
                child.adopt_runtime();
            }
            drop(child);
        });
        if !adopted {
            if self.is_unresolved(Step::Adopt) {
                self.request_rollback();
            }
            return false;
        }
        // adopt_runtime clears the provider's Preparation observation role.
        // Retain only its metadata cancellation handle until commit returns,
        // so an unknown commit cannot suppress independent Root cancellation.
        let mut state = self.state.lock().expect("preparation child scope lock");
        if state.mode != Mode::Adopting {
            return false;
        }
        state.mode = Mode::Adopted;
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
    pub(crate) fn start_runtime<F: FnOnce()>(&self, action: F) -> Result<bool, F> {
        {
            let mut state = self.state.lock().expect("preparation child scope lock");
            if state.mode != Mode::Adopted
                || state.phases[Step::Publication as usize] != Phase::NotStarted
            {
                // The original host still owns a rejected action, including
                // its dormant runtime captures. Refusal never destroys it or
                // treats an earlier call as execution of this new closure.
                return Err(action);
            }
            state.phases[Step::Publication as usize] = Phase::Started;
        }
        if !self.run_claimed(Step::Publication, R::PreparationExitPublication, action) {
            if self.is_unresolved(Step::Publication) {
                self.request_rollback();
            }
            return Ok(false);
        }
        if self
            .state
            .lock()
            .expect("preparation child scope lock")
            .mode
            != Mode::Adopted
        {
            // Rollback may have won after the call really started. Its own
            // cleanup owner must observe the child and every started callback
            // before retiring the original preparation holder.
            return Ok(false);
        }
        let child_retired = self.clear_child();
        let state = self.state.lock().expect("preparation child scope lock");
        Ok(child_retired
            && state.mode == Mode::Adopted
            && Self::body_obligations_exited(&state)
            && Self::started_actions_exited(&state))
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

#[cfg(test)]
mod host_handoff_tests {
    use super::*;
    use std::sync::{
        Condvar,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };
    use std::time::Duration;

    #[derive(Default)]
    struct Gate {
        open: Mutex<bool>,
        changed: Condvar,
    }
    impl Gate {
        fn wait(&self) {
            let mut open = self.open.lock().unwrap();
            while !*open {
                open = self.changed.wait(open).unwrap();
            }
        }
        fn release(&self) {
            *self.open.lock().unwrap() = true;
            self.changed.notify_all();
        }
    }
    struct Threads {
        gates: Vec<Arc<Gate>>,
        joins: Vec<std::thread::JoinHandle<()>>,
    }
    impl Drop for Threads {
        fn drop(&mut self) {
            for gate in &self.gates {
                gate.release();
            }
            for join in self.joins.drain(..) {
                let _ = join.join();
            }
        }
    }
    struct Child {
        adoption: Option<(mpsc::Sender<()>, Arc<Gate>)>,
    }
    impl TaskPreparationChild for Child {
        fn request_rollback(&self) {}
        fn poll_exit(&self) -> TaskPreparationChildExit {
            TaskPreparationChildExit::Exited {
                output: TaskPreparationOutputExit::NoOutputCreated,
            }
        }
        fn adopt_runtime(&self) {
            if let Some((entered, gate)) = &self.adoption {
                let _ = entered.send(());
                gate.wait();
            }
        }
    }
    fn scope() -> Arc<TaskPreparationScope> {
        use novarocks_types::{
            AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
        };
        let identity = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(9301, 9302), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        Arc::new(TaskPreparationScope::new(
            identity,
            Arc::new(ExecutionHealth::default()),
        ))
    }
    fn entered(receiver: &mpsc::Receiver<()>) {
        receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("original callback reaches its controlled handoff");
    }
    #[test]
    fn late_adoption_cannot_overwrite_rollback_or_retire_a_running_receiver_cleanup() {
        let scope = scope();
        let adopt_gate = Arc::new(Gate::default());
        let receiver_gate = Arc::new(Gate::default());
        let mut threads = Threads {
            gates: vec![adopt_gate.clone(), receiver_gate.clone()],
            joins: vec![],
        };
        let (adopt_entered, adopt_rx) = mpsc::channel();
        scope
            .register(Arc::new(Child {
                adoption: Some((adopt_entered, adopt_gate.clone())),
            }))
            .unwrap();
        scope.body_exited();
        let commits = Arc::new(AtomicUsize::new(0));
        let (adopted, adopted_rx) = mpsc::channel();
        let owner = scope.clone();
        let committed = commits.clone();
        threads.joins.push(std::thread::spawn(move || {
            let accepted = owner.adopt_runtime();
            if accepted {
                let _ = owner.start_runtime(|| {
                    committed.fetch_add(1, Ordering::SeqCst);
                });
            }
            let _ = adopted.send(accepted);
        }));
        entered(&adopt_rx);
        scope.request_rollback();
        scope.cleanup(PreparationCleanup::Inbound, || {});
        let (receiver_entered, receiver_rx) = mpsc::channel();
        let owner = scope.clone();
        let gate = receiver_gate.clone();
        threads.joins.push(std::thread::spawn(move || {
            owner.cleanup(PreparationCleanup::Receiver, || {
                let _ = receiver_entered.send(());
                gate.wait();
            });
        }));
        entered(&receiver_rx);
        adopt_gate.release();
        let adopted = adopted_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("original adoption returns");
        assert!(
            !scope.host_captures_exited(),
            "running receiver cleanup keeps the original preparation captures live"
        );
        assert!(
            !adopted,
            "rollback is irreversible even if an earlier adoption callback succeeds later"
        );
        assert_eq!(commits.load(Ordering::SeqCst), 0);
        assert!(!scope.rollback_exited());
        receiver_gate.release();
        drop(threads);
        assert!(scope.rollback_exited());
        assert!(scope.host_captures_exited());
    }
    #[test]
    fn rollback_waits_for_the_original_runtime_start_call_before_dropping_child_custody() {
        let scope = scope();
        scope.register(Arc::new(Child { adoption: None })).unwrap();
        scope.body_exited();
        assert!(scope.adopt_runtime());
        let start_gate = Arc::new(Gate::default());
        let mut threads = Threads {
            gates: vec![start_gate.clone()],
            joins: vec![],
        };
        let (started, started_rx) = mpsc::channel();
        let owner = scope.clone();
        let gate = start_gate.clone();
        threads.joins.push(std::thread::spawn(move || {
            let _ = owner.start_runtime(|| {
                let _ = started.send(());
                gate.wait();
            });
        }));
        entered(&started_rx);
        let drops = Arc::new(AtomicUsize::new(0));
        let rejected = scope.start_runtime(captured(&drops));
        assert!(
            rejected.is_err(),
            "the original runtime start is already in flight"
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(rejected);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        scope.request_rollback();
        scope.cleanup(PreparationCleanup::Inbound, || {});
        scope.cleanup(PreparationCleanup::Receiver, || {});
        assert!(
            !scope.rollback_exited(),
            "an in-flight original publication still owns its captures"
        );
        assert!(!scope.host_captures_exited());
        start_gate.release();
        drop(threads);
        assert!(scope.rollback_exited());
        assert!(scope.host_captures_exited());
    }
    struct Capture(Arc<AtomicUsize>);
    impl Drop for Capture {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn captured(drops: &Arc<AtomicUsize>) -> impl FnOnce() + Send + 'static {
        let original = Capture(drops.clone());
        move || {
            std::hint::black_box(&original);
        }
    }
    #[test]
    fn refused_runtime_start_returns_its_original_captures_without_running_or_dropping_them() {
        let scope = scope();
        scope.body_exited();
        let drops = Arc::new(AtomicUsize::new(0));
        let rejected = scope.start_runtime(captured(&drops));
        assert!(rejected.is_err(), "preparation has not adopted its runtime");
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(rejected);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(scope.adopt_runtime());
        assert!(
            scope
                .start_runtime(|| {})
                .unwrap_or_else(|_| panic!("one original admitted start"))
        );
        assert!(
            scope.host_captures_exited(),
            "never-started cleanup does not block normal exit"
        );
        let rejected = scope.start_runtime(captured(&drops));
        assert!(
            rejected.is_err(),
            "a prior start cannot consume a second action"
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        drop(rejected);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        scope.request_rollback();
        let rejected = scope.start_runtime(captured(&drops));
        assert!(
            rejected.is_err(),
            "rollback cannot start a later runtime action"
        );
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        drop(rejected);
        assert_eq!(drops.load(Ordering::SeqCst), 3);
        scope.cleanup(PreparationCleanup::Inbound, || {});
        scope.cleanup(PreparationCleanup::Receiver, || {});
        assert!(scope.rollback_exited());
        assert!(scope.host_captures_exited());
    }
    #[test]
    fn a_runtime_start_panic_keeps_its_original_phase_unresolved_through_rollback() {
        let scope = scope();
        scope.body_exited();
        assert!(scope.adopt_runtime());
        assert!(
            !scope
                .start_runtime(|| panic!("original runtime start failed"))
                .unwrap_or_else(|_| panic!("the first action is admitted"))
        );
        assert!(scope.health.failure().is_some());
        scope.cleanup(PreparationCleanup::Inbound, || {});
        scope.cleanup(PreparationCleanup::Receiver, || {});
        assert!(!scope.rollback_exited());
        assert!(!scope.host_captures_exited());
        let drops = Arc::new(AtomicUsize::new(0));
        let rejected = scope.start_runtime(captured(&drops));
        assert!(rejected.is_err());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(rejected);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
