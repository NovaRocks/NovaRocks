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

//! Provider-child rollback and adoption under the original preparation holder.
//! Input, body unwind and whole-report custody belong to control_runtime::preparation.

use crate::failure_reason::WorkerFailureReason as R;
use crate::{control_runtime::preparation::PreparationParent, task_port::TaskPendingNotice};
use novarocks_control_runtime::{
    completion::ReportSlot,
    exit_custody::{ExitCustody, ExitCustodyError, ExitCustodyPhase, ExitObligation},
    obligation::WorkRetirementObserver,
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    identity::ConsumerId,
    scope::{ScopeError, WorkKind},
};
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
#[repr(usize)]
enum Step {
    Cancel,
    Inbound,
    Receiver,
    Child,
    Adopt,
    Publication,
}
const STEPS: usize = Step::Publication as usize + 1;
const CALLBACKS: [Step; 5] = [
    Step::Cancel,
    Step::Inbound,
    Step::Receiver,
    Step::Adopt,
    Step::Publication,
];
const OBSERVER: ConsumerId = ConsumerId::new(2).unwrap();
const HOST_WORK_STORAGE: crate::control_runtime_policy::StorageNeed =
    crate::control_runtime_policy::StorageNeed {
        requests: 0,
        inputs: 0,
        batches: 0,
        effects: 0,
        cuts: 0,
        replies: 0,
        work: 1,
        completions: 1,
        captures: 1,
    };
type Payload = Box<dyn Any + Send>;

/// The phase and physical-exit authority belong to the original runtime work.
/// `claimed` and `invoked` only serialize foreign calls and record whether an
/// adoption actually ran; neither can establish exit or release capacity.
struct HostWork {
    custody: ExitCustody,
    obligation: Option<ExitObligation>,
    report: ReportSlot,
    retirement: WorkRetirementObserver,
    claimed: bool,
    invoked: bool,
    panic: Option<ManuallyDrop<Payload>>,
}
impl HostWork {
    fn reserve(parent: &PreparationParent, step: Step) -> Result<Self, RuntimeError> {
        let kind = match step {
            Step::Adopt | Step::Publication => WorkKind::Business,
            _ => WorkKind::Cleanup,
        };
        let original = parent.reserve_host_work(
            kind,
            HOST_WORK_STORAGE
                .resources()
                .expect("fixed host work storage"),
        )?;
        original.work().set_diagnostic_label(match step {
            Step::Cancel => crate::control_runtime::work_labels::PREPARATION_CANCEL,
            Step::Inbound => crate::control_runtime::work_labels::PREPARATION_INBOUND,
            Step::Receiver => crate::control_runtime::work_labels::PREPARATION_RECEIVER,
            Step::Child => crate::control_runtime::work_labels::PREPARATION_CHILD,
            Step::Adopt => crate::control_runtime::work_labels::PREPARATION_ADOPT,
            Step::Publication => crate::control_runtime::work_labels::PREPARATION_PUBLICATION,
        })?;
        let retirement = original.work().retirement_observer();
        let report = original.report().clone();
        let (custody, obligation) = ExitCustody::from_admitted(original);
        Ok(Self {
            custody,
            obligation: Some(obligation),
            report,
            retirement,
            claimed: false,
            invoked: false,
            panic: None,
        })
    }
    fn exited(&self) -> bool {
        matches!(
            self.custody.phase(),
            ExitCustodyPhase::Exited | ExitCustodyPhase::Retired
        )
    }
    fn idle(&self) -> bool {
        !self.claimed || self.exited()
    }
    fn retire(&self) -> Result<(), RuntimeError> {
        if self.custody.phase() == ExitCustodyPhase::Exited {
            // A never-started holder revoked the report. A real invocation
            // filled it, and this actual host observer consumes its one leg.
            if !self.report.retirement_ready()? {
                self.report.consume(OBSERVER)?;
            }
            self.custody.try_retire().map_err(runtime_error)?;
        }
        Ok(())
    }
}
fn runtime_error(error: ExitCustodyError) -> RuntimeError {
    match error {
        ExitCustodyError::Runtime(error) => error,
        ExitCustodyError::InvalidPhase(_) => ScopeError::InvalidTransition.into(),
    }
}

struct State {
    mode: Mode,
    body_exited: bool,
    never_invoked: bool,
    registered: bool,
    child: Option<Arc<dyn TaskPreparationChild>>,
    output_exit: Option<TaskPreparationOutputExit>,
    parent: Option<PreparationParent>,
    financial_scope: Option<ScopeHandle>,
    pending: Option<Arc<dyn TaskPendingNotice>>,
    work: [Option<HostWork>; STEPS],
    polling: bool,
    callbacks_closed: bool,
}

pub(crate) struct TaskPreparationScope {
    identity: TaskIdentity,
    health: Arc<ExecutionHealth>,
    state: Mutex<State>,
    #[cfg(test)]
    before_body_exit: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

/// Positive domain evidence from the same original scope. Runtime retirement
/// supplies work exit; the original child additionally supplies output custody.
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
    pub(crate) fn output_exit(&self) -> TaskPreparationOutputExit {
        self.scope
            .state
            .lock()
            .expect("original preparation output exit")
            .output_exit
            .expect("the original child supplied output evidence")
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
            #[cfg(test)]
            before_body_exit: Mutex::new(None),
            state: Mutex::new(State {
                mode: Mode::Preparing,
                body_exited: false,
                never_invoked: false,
                registered: false,
                child: None,
                output_exit: None,
                parent: None,
                financial_scope: None,
                pending: None,
                work: std::array::from_fn(|_| None),
                polling: false,
                callbacks_closed: false,
            }),
        }
    }
    pub(crate) fn identity(&self) -> TaskIdentity {
        self.identity
    }

    /// Called within the original preparation body, before any foreign host
    /// call. Later callbacks move these already-admitted original obligations.
    pub(crate) fn bind_runtime(
        &self,
        parent: PreparationParent,
        pending: Arc<dyn TaskPendingNotice>,
    ) -> Result<(), RuntimeError> {
        let mut state = self.state.lock().expect("preparation child scope lock");
        if state.mode == Mode::Rollback {
            return Err(ScopeError::Stopped.into());
        }
        if state.parent.is_some() || state.body_exited || state.callbacks_closed {
            return Err(ScopeError::InvalidTransition.into());
        }
        state.pending = Some(pending);
        state.financial_scope = Some(parent.scope().clone());
        state.parent = Some(parent.clone());
        for step in CALLBACKS {
            state.work[step as usize] = Some(HostWork::reserve(&parent, step)?);
        }
        Ok(())
    }
    pub(crate) fn register(
        &self,
        child: Arc<dyn TaskPreparationChild>,
    ) -> Result<(), TaskPreparationChildRegistrationFailure> {
        let mut state = self.state.lock().expect("preparation child scope lock");
        if state.mode != Mode::Preparing || state.body_exited || state.callbacks_closed {
            return Err(TaskPreparationChildRegistrationFailure::ScopeClosed);
        }
        if state.registered {
            return Err(TaskPreparationChildRegistrationFailure::AlreadyRegistered);
        }
        let parent = state
            .parent
            .as_ref()
            .ok_or(TaskPreparationChildRegistrationFailure::ScopeClosed)?;
        let mut work = HostWork::reserve(parent, Step::Child)
            .map_err(|_| TaskPreparationChildRegistrationFailure::ScopeClosed)?;
        // Cleanup admission and its start remain valid after a stop race.
        if work
            .obligation
            .as_mut()
            .expect("new child obligation")
            .start()
            .is_err()
        {
            let obligation = work.obligation.take().expect("unused child obligation");
            obligation
                .revoke_unused_and_exit()
                .expect("unused child exit");
            state.work[Step::Child as usize] = Some(work);
            return Err(TaskPreparationChildRegistrationFailure::ScopeClosed);
        }
        work.claimed = true;
        work.invoked = true;
        state.work[Step::Child as usize] = Some(work);
        state.registered = true;
        state.child = Some(child);
        Ok(())
    }
    fn notify(&self) {
        let pending = self
            .state
            .lock()
            .expect("preparation child scope lock")
            .pending
            .clone();
        if let Some(pending) = pending {
            pending.pending();
        }
    }
    fn fault(
        &self,
        reason: R,
        step: Step,
        payload: Option<Payload>,
        original: Option<ExitObligation>,
    ) {
        let payload = payload.map(ManuallyDrop::new);
        let remaining = {
            let mut state = self.state.lock().expect("preparation child scope lock");
            let work = state.work[step as usize]
                .as_mut()
                .expect("admitted failing host work");
            if work.panic.is_none() {
                work.panic = payload;
            }
            work.obligation.take()
        };
        // No unknown callback or payload destructor runs under either lock.
        // Ordinary authority Drop records the original work as unresolved.
        drop(original);
        drop(remaining);
        self.health.report_unresolved(
            ExecutionExitOrigin::Task {
                identity: self.identity,
                phase: TaskFailurePhase::Preparation,
            },
            FailureFact::new(FailureClass::Unexpected, reason.reason()),
        );
        self.notify();
    }
    fn unresolved(&self, step: Step) -> bool {
        self.state
            .lock()
            .expect("preparation child scope lock")
            .work[step as usize]
            .as_ref()
            .is_some_and(|work| work.custody.phase() == ExitCustodyPhase::Unresolved)
    }
    fn callbacks_idle(state: &State) -> bool {
        CALLBACKS.iter().all(|step| {
            state.work[*step as usize]
                .as_ref()
                .is_none_or(HostWork::idle)
        })
    }
    fn cleanup_exited(state: &State) -> bool {
        state.never_invoked
            || [Step::Cancel, Step::Inbound, Step::Receiver]
                .iter()
                .all(|step| {
                    state.work[*step as usize]
                        .as_ref()
                        .is_some_and(HostWork::exited)
                })
    }
    fn claim(state: &mut State, step: Step) -> Option<ExitObligation> {
        let work = state.work[step as usize].as_mut()?;
        if work.claimed || work.exited() {
            return None;
        }
        work.claimed = true;
        work.obligation.take()
    }
    fn invoke_claimed(
        &self,
        step: Step,
        reason: R,
        mut original: ExitObligation,
        action: impl FnOnce(),
    ) -> bool {
        if original.start().is_err() {
            // The caller retains ownership of refused publication closures by
            // checking its start before handing them to this helper.
            let disposed = catch_unwind(AssertUnwindSafe(|| drop(action)));
            return match disposed {
                Ok(()) => {
                    original
                        .revoke_unused_and_exit()
                        .expect("stopped callback remains unused");
                    self.notify();
                    false
                }
                Err(payload) => {
                    self.fault(reason, step, Some(payload), Some(original));
                    false
                }
            };
        }
        self.invoke_started(step, reason, original, action)
    }
    fn invoke_started(
        &self,
        step: Step,
        reason: R,
        original: ExitObligation,
        action: impl FnOnce(),
    ) -> bool {
        self.state
            .lock()
            .expect("preparation child scope lock")
            .work[step as usize]
            .as_mut()
            .expect("claimed callback work")
            .invoked = true;
        match catch_unwind(AssertUnwindSafe(action)) {
            Ok(()) => {
                if original.exited().is_err() {
                    self.fault(reason, step, None, None);
                    return false;
                }
                self.notify();
                true
            }
            Err(payload) => {
                self.fault(reason, step, Some(payload), Some(original));
                false
            }
        }
    }
    fn once(&self, step: Step, reason: R, action: impl FnOnce()) -> bool {
        let original = {
            let mut state = self.state.lock().expect("preparation child scope lock");
            if state.never_invoked
                || state.work[step as usize]
                    .as_ref()
                    .is_some_and(HostWork::exited)
            {
                drop(state);
                drop(action);
                return true;
            }
            Self::claim(&mut state, step)
        };
        let Some(original) = original else {
            drop(action);
            return false;
        };
        self.invoke_claimed(step, reason, original, action)
    }
    pub(crate) fn request_rollback(&self) {
        let child = {
            let mut state = self.state.lock().expect("preparation child scope lock");
            state.mode = Mode::Rollback;
            state.child.clone()
        };
        self.once(Step::Cancel, R::PreparationChildRollback, move || {
            if let Some(child) = &child {
                child.request_rollback();
            }
            drop(child);
        });
    }
    pub(crate) fn cleanup(&self, kind: PreparationCleanup, action: impl FnOnce()) {
        let (step, reason) = match kind {
            PreparationCleanup::Inbound => (Step::Inbound, R::PreparationInboundCleanup),
            PreparationCleanup::Receiver => (Step::Receiver, R::PreparationReceiverCleanup),
        };
        self.once(step, reason, action);
    }
    /// The actual runner disposed its never-invoked host closure and input.
    /// There was no provider installation and no registered child to clean up.
    pub(crate) fn body_never_invoked(&self) {
        {
            let mut state = self.state.lock().expect("preparation child scope lock");
            assert!(
                !state.registered,
                "an uninvoked host cannot register a child"
            );
            state.never_invoked = true;
            state.mode = Mode::Rollback;
            state.output_exit = Some(TaskPreparationOutputExit::NoOutputCreated);
        }
        self.close_unused();
    }
    #[cfg(test)]
    pub(crate) fn physical_bodies_exited(&self) -> bool {
        self.state
            .lock()
            .expect("preparation child scope lock")
            .body_exited
    }
    pub(crate) fn body_exited(&self) {
        #[cfg(test)]
        {
            let hook = self.before_body_exit.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        self.state
            .lock()
            .expect("preparation child scope lock")
            .body_exited = true;
    }
    #[cfg(test)]
    pub(crate) fn before_original_body_exit_for_test(&self, hook: Box<dyn FnOnce() + Send>) {
        assert!(
            self.before_body_exit
                .lock()
                .unwrap()
                .replace(hook)
                .is_none()
        );
    }
    pub(crate) fn body_has_exited(&self) -> bool {
        self.state
            .lock()
            .expect("preparation child scope lock")
            .body_exited
    }
    #[cfg(test)]
    pub(crate) fn original_adoption_unstarted_for_test(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.mode == Mode::Preparing
            && state.work[Step::Adopt as usize]
                .as_ref()
                .is_none_or(|work| !work.invoked)
    }
    /// Retire only actual runtime receipts. No provider hook or foreign Drop
    /// runs here, so metadata callers can drive the same finite reaper path.
    pub(crate) fn retire_ready(&self) -> Result<bool, RuntimeError> {
        let state = self.state.lock().expect("preparation child scope lock");
        for work in state.work.iter().flatten() {
            work.retire()?;
        }
        let reaped = match &state.financial_scope {
            Some(scope) => scope.reap_ready(STEPS)?,
            None => 0,
        };
        let retired = state
            .work
            .iter()
            .flatten()
            .all(|work| work.retirement.is_retired());
        let pending = state.pending.clone();
        drop(state);
        // Another original ready header can consume this finite reaper turn.
        // Keep the same owner runnable after real progress, without polling a
        // pending callback or manufacturing its physical exit.
        if reaped != 0
            && let Some(pending) = pending
        {
            pending.pending();
        }
        Ok(retired)
    }
    #[cfg(test)]
    pub(crate) fn host_captures_exited(&self) -> bool {
        self.retire_ready().unwrap();
        let state = self.state.lock().unwrap();
        state.body_exited
            && state
                .work
                .iter()
                .flatten()
                .all(|work| work.retirement.is_retired())
    }
    fn close_unused(&self) {
        let unused = {
            let mut state = self.state.lock().expect("preparation child scope lock");
            state.callbacks_closed = true;
            CALLBACKS
                .iter()
                .filter_map(|step| {
                    let work = state.work[*step as usize].as_mut()?;
                    if work.claimed {
                        None
                    } else {
                        work.obligation.take()
                    }
                })
                .collect::<Vec<_>>()
        };
        for original in unused {
            original
                .revoke_unused_and_exit()
                .expect("unused preparation callback exit");
        }
        self.notify();
    }
    fn clear_child(&self) -> bool {
        let (child, original) = {
            let mut state = self.state.lock().expect("preparation child scope lock");
            if state.polling
                || !Self::callbacks_idle(&state)
                || (state.mode == Mode::Rollback && !Self::cleanup_exited(&state))
            {
                return false;
            }
            if !state.registered {
                return true;
            }
            let work = state.work[Step::Child as usize]
                .as_ref()
                .expect("registered child work");
            if work.exited() {
                return true;
            }
            if work.custody.phase() == ExitCustodyPhase::Unresolved {
                return false;
            }
            let Some(child) = state.child.take() else {
                return false;
            };
            let original = state.work[Step::Child as usize]
                .as_mut()
                .unwrap()
                .obligation
                .take()
                .expect("same running child obligation");
            (child, original)
        };
        match catch_unwind(AssertUnwindSafe(|| drop(child))) {
            Ok(()) => {
                let unknown_output = {
                    let state = self.state.lock().expect("preparation child scope lock");
                    state.output_exit == Some(TaskPreparationOutputExit::Unknown)
                        && state.work[Step::Adopt as usize]
                            .as_ref()
                            .is_none_or(|work| !work.invoked)
                };
                if unknown_output {
                    self.fault(
                        R::PreparationChildUnresolved,
                        Step::Child,
                        None,
                        Some(original),
                    );
                    return false;
                }
                if original.exited().is_err() {
                    self.fault(R::PreparationChildDrop, Step::Child, None, None);
                    false
                } else {
                    self.notify();
                    true
                }
            }
            Err(payload) => {
                self.fault(
                    R::PreparationChildDrop,
                    Step::Child,
                    Some(payload),
                    Some(original),
                );
                false
            }
        }
    }
    pub(crate) fn rollback_exited(&self) -> bool {
        let (child, registered, already_observed) = {
            let mut state = self.state.lock().expect("preparation child scope lock");
            if !state.body_exited
                || state.mode != Mode::Rollback
                || state.polling
                || !Self::callbacks_idle(&state)
                || !Self::cleanup_exited(&state)
            {
                return false;
            }
            if state.work[Step::Child as usize]
                .as_ref()
                .is_some_and(|work| work.custody.phase() == ExitCustodyPhase::Unresolved)
            {
                return false;
            }
            let observed = state.output_exit.is_some();
            state.polling = !observed;
            (state.child.clone(), state.registered, observed)
        };
        if !already_observed {
            let result = catch_unwind(AssertUnwindSafe(move || match child.as_ref() {
                Some(child) => child.poll_exit(),
                None if !registered => TaskPreparationChildExit::Exited {
                    output: TaskPreparationOutputExit::NoOutputCreated,
                },
                None => TaskPreparationChildExit::Exited {
                    output: TaskPreparationOutputExit::Unknown,
                },
            }));
            match result {
                Ok(TaskPreparationChildExit::Pending) => {
                    self.state
                        .lock()
                        .expect("preparation child scope lock")
                        .polling = false;
                    return false;
                }
                Ok(TaskPreparationChildExit::Exited { output }) => {
                    let mut state = self.state.lock().expect("preparation child scope lock");
                    state.output_exit = Some(output);
                    state.polling = false;
                }
                Ok(TaskPreparationChildExit::Unresolved) => {
                    self.fault(R::PreparationChildUnresolved, Step::Child, None, None);
                    return false;
                }
                Err(payload) => {
                    self.fault(R::PreparationChildPoll, Step::Child, Some(payload), None);
                    return false;
                }
            }
        }
        if !self.clear_child() {
            return false;
        }
        self.close_unused();
        true
    }
    pub(crate) fn never_adopted_rollback_exit(&self) -> Option<NeverAdoptedPreparationExit<'_>> {
        let state = self.state.lock().expect("preparation child scope lock");
        if state.mode != Mode::Rollback
            || !state.body_exited
            || state.child.is_some()
            || state.polling
            || state.output_exit.is_none()
            || !Self::cleanup_exited(&state)
            || !Self::callbacks_idle(&state)
            || state.work[Step::Adopt as usize]
                .as_ref()
                .is_some_and(|work| work.invoked)
            || state.work[Step::Child as usize]
                .as_ref()
                .is_some_and(|work| !work.exited())
        {
            return None;
        }
        Some(NeverAdoptedPreparationExit { scope: self })
    }
    pub(crate) fn output_exit_unresolved(&self) {
        if !self.unresolved(Step::Child) {
            self.fault(R::PreparationChildUnresolved, Step::Child, None, None);
        }
    }
    pub(crate) fn adopt_runtime(&self) -> bool {
        let (child, mut original) = {
            let mut state = self.state.lock().expect("preparation child scope lock");
            if !state.body_exited || state.callbacks_closed {
                return state.mode == Mode::Adopted;
            }
            match state.mode {
                Mode::Rollback => return false,
                Mode::Adopted => return true,
                Mode::Preparing | Mode::Adopting => {}
            }
            let Some(original) = Self::claim(&mut state, Step::Adopt) else {
                return false;
            };
            state.mode = Mode::Adopting;
            (state.child.clone(), original)
        };
        if original.start().is_err() {
            drop(child);
            original
                .revoke_unused_and_exit()
                .expect("stopped adoption never ran");
            self.request_rollback();
            return false;
        }
        let accepted = {
            let state = self.state.lock().expect("preparation child scope lock");
            state.mode == Mode::Adopting
        };
        if !accepted {
            drop(child);
            original
                .withdraw_unpublished_and_exit()
                .expect("rollback withdrew uninvoked adoption");
            self.notify();
            return false;
        }
        if !self.invoke_started(Step::Adopt, R::PreparationChildAdopt, original, move || {
            if let Some(child) = &child {
                child.adopt_runtime();
            }
            drop(child);
        }) {
            self.request_rollback();
            return false;
        }
        let mut state = self.state.lock().expect("preparation child scope lock");
        if state.mode != Mode::Adopting {
            return false;
        }
        state.mode = Mode::Adopted;
        true
    }
    pub(crate) fn start_runtime<F: FnOnce()>(&self, action: F) -> Result<bool, F> {
        let mut original = {
            let mut state = self.state.lock().expect("preparation child scope lock");
            if state.mode != Mode::Adopted || state.callbacks_closed {
                return Err(action);
            }
            let Some(original) = Self::claim(&mut state, Step::Publication) else {
                return Err(action);
            };
            original
        };
        if original.start().is_err() {
            original
                .revoke_unused_and_exit()
                .expect("stopped publication never ran");
            self.request_rollback();
            return Err(action);
        }
        if !self.invoke_started(
            Step::Publication,
            R::PreparationExitPublication,
            original,
            action,
        ) {
            self.request_rollback();
            return Ok(false);
        }
        if self
            .state
            .lock()
            .expect("preparation child scope lock")
            .mode
            != Mode::Adopted
        {
            return Ok(false);
        }
        if !self.clear_child() {
            return Ok(false);
        }
        self.close_unused();
        Ok(self
            .state
            .lock()
            .expect("preparation child scope lock")
            .mode
            == Mode::Adopted)
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
    struct TestScope {
        original: Arc<TaskPreparationScope>,
        custody: crate::control_runtime::preparation::PreparationCustody,
        body: Mutex<Option<crate::control_runtime::preparation::PreparationJob>>,
        ledger: novarocks_control_runtime::transaction::RuntimeLedger,
        financial_scope: ScopeHandle,
    }
    impl std::ops::Deref for TestScope {
        type Target = Arc<TaskPreparationScope>;
        fn deref(&self) -> &Self::Target {
            &self.original
        }
    }
    impl TestScope {
        fn body_exited(&self) {
            self.original.body_exited();
            self.body
                .lock()
                .unwrap()
                .take()
                .expect("one original test body")
                .finish_body_for_test();
        }
    }
    impl Drop for TestScope {
        fn drop(&mut self) {
            if let Some(body) = self.body.lock().unwrap().take() {
                self.original.body_never_invoked();
                self.original.body_exited();
                body.finish_body_for_test();
            }
            self.original.retire_ready().unwrap();
            if !self.custody.publish_whole_exit().unwrap() {
                // The panic tests intentionally keep the original unknown
                // work, capacity and payload in their real runtime custody.
                // Every normal test must fully retire and reap instead.
                assert!(
                    !self.ledger.unresolved_work().is_empty(),
                    "a normal host test left live work"
                );
                return;
            }
            for report in [self.custody.body_report(), self.custody.exit_report()] {
                for consumer in [ConsumerId::new(1).unwrap(), OBSERVER] {
                    report.consume(consumer).unwrap();
                }
            }
            let mut remainder = self
                .custody
                .take_after_exit()
                .unwrap()
                .expect("original whole exit");
            remainder.finish().unwrap();
            drop(remainder);
            self.financial_scope.reap_ready(STEPS + 1).unwrap();
            self.financial_scope.close_external().unwrap();
            self.ledger.remove_scope(self.financial_scope.id()).unwrap();
            assert!(
                self.ledger.is_empty(),
                "normal host test retired its actual runtime inventory"
            );
        }
    }
    struct Pending;
    impl TaskPendingNotice for Pending {
        fn pending(&self) {}
    }
    #[derive(Debug)]
    struct EmptyInput;
    impl novarocks_execution_contract::CreationContent for EmptyInput {
        fn encoded_len(&self) -> usize {
            0
        }
        fn retained_bytes(&self) -> usize {
            0
        }
        fn into_stored(self: Box<Self>) -> Box<dyn Any + Send> {
            self
        }
    }
    fn scope() -> TestScope {
        use crate::control_admission::Dimension;
        use crate::control_runtime::preparation::{
            PREPARATION_STORAGE, PreparationBinding, PreparationCustody,
        };
        use novarocks_control_runtime::{completion::ReportSlot, transaction::RuntimeLedger};
        use novarocks_control_runtime_core::{
            capacity::ResourceVector, completion::ConsumerKind, identity::Generation,
            scope::WorkAdmission,
        };
        use novarocks_types::{
            AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
        };
        let identity = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(9301, 9302), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        let quota = ResourceVector::new([(Dimension::PreparingTasks.id(), 1)]).unwrap();
        let ledger = RuntimeLedger::new(quota.clone());
        let generation = Generation::new(1).unwrap();
        let financial_scope = ledger.open_scope_next(None, generation).unwrap();
        let mut resources = PREPARATION_STORAGE.resources().unwrap();
        resources.quota = quota;
        let (work, mut capacity) = financial_scope
            .reserve_next(
                generation,
                resources,
                WorkKind::Business,
                WorkAdmission::External,
            )
            .unwrap()
            .bind(WorkKind::Business)
            .unwrap();
        let mut report = || {
            ReportSlot::reserve_next(
                &mut capacity,
                generation,
                2,
                [
                    (
                        ConsumerId::new(1).unwrap(),
                        ConsumerKind::Owner {
                            scope: financial_scope.id(),
                            generation,
                        },
                    ),
                    (OBSERVER, ConsumerKind::Observer),
                ],
            )
            .unwrap()
        };
        let reports = [report(), report()];
        let health = Arc::new(ExecutionHealth::default());
        let pending: Arc<dyn TaskPendingNotice> = Arc::new(Pending);
        let (custody, mut body) = PreparationCustody::from_admitted(
            PreparationBinding {
                stop: crate::task_port::TaskStopSignal::initial(identity),
                pending: pending.clone(),
                health: health.clone(),
            },
            work,
            capacity,
            reports,
            novarocks_execution_contract::TaskCreationInput::new(
                novarocks_execution_contract::FrozenBytes::freeze(bytes::Bytes::new()),
                Box::new(EmptyInput),
            ),
        );
        let parent = body.start_for_test();
        let original = Arc::new(TaskPreparationScope::new(identity, health));
        custody.bind_host_children(original.clone());
        original.bind_runtime(parent, pending).unwrap();
        TestScope {
            original,
            custody,
            body: Mutex::new(Some(body)),
            ledger,
            financial_scope,
        }
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
    #[test]
    fn host_callbacks_start_original_work_and_drop_captures_before_exit() {
        struct CaptureExit {
            work: ExitCustody,
            dropped: Arc<AtomicUsize>,
        }
        impl Drop for CaptureExit {
            fn drop(&mut self) {
                assert_eq!(self.work.phase(), ExitCustodyPhase::Started);
                self.dropped.fetch_add(1, Ordering::SeqCst);
            }
        }
        let scope = scope();
        scope.body_exited();
        scope.request_rollback();
        for (kind, step) in [
            (PreparationCleanup::Inbound, Step::Inbound),
            (PreparationCleanup::Receiver, Step::Receiver),
        ] {
            let work = scope.state.lock().unwrap().work[step as usize]
                .as_ref()
                .unwrap()
                .custody
                .clone();
            let dropped = Arc::new(AtomicUsize::new(0));
            let capture = CaptureExit {
                work: work.clone(),
                dropped: dropped.clone(),
            };
            scope.cleanup(kind, move || {
                assert_eq!(work.phase(), ExitCustodyPhase::Started);
                std::hint::black_box(&capture);
            });
            assert_eq!(dropped.load(Ordering::SeqCst), 1);
        }
        assert!(scope.rollback_exited());
        assert!(scope.host_captures_exited());
        let state = scope.state.lock().unwrap();
        assert!(!state.work[Step::Adopt as usize].as_ref().unwrap().invoked);
        assert!(
            !state.work[Step::Publication as usize]
                .as_ref()
                .unwrap()
                .invoked
        );
        assert!(
            state
                .work
                .iter()
                .flatten()
                .all(|work| work.retirement.is_retired())
        );
    }
    #[test]
    fn provider_cancel_adopt_poll_drop_and_publication_use_their_original_started_work() {
        struct CheckedChild {
            scope: std::sync::Weak<TaskPreparationScope>,
            calls: Arc<AtomicUsize>,
        }
        impl CheckedChild {
            fn observe(&self, step: Step) {
                let scope = self.scope.upgrade().expect("original child scope");
                let state = scope.state.lock().unwrap();
                assert_eq!(
                    state.work[step as usize].as_ref().unwrap().custody.phase(),
                    ExitCustodyPhase::Started,
                );
                self.calls.fetch_add(1, Ordering::SeqCst);
            }
        }
        impl TaskPreparationChild for CheckedChild {
            fn request_rollback(&self) {
                self.observe(Step::Cancel);
            }
            fn poll_exit(&self) -> TaskPreparationChildExit {
                self.observe(Step::Child);
                TaskPreparationChildExit::Exited {
                    output: TaskPreparationOutputExit::NoOutputCreated,
                }
            }
            fn adopt_runtime(&self) {
                self.observe(Step::Adopt);
            }
        }
        impl Drop for CheckedChild {
            fn drop(&mut self) {
                self.observe(Step::Child);
            }
        }
        for adopt in [false, true] {
            let scope = scope();
            let calls = Arc::new(AtomicUsize::new(0));
            scope
                .register(Arc::new(CheckedChild {
                    scope: Arc::downgrade(&scope.original),
                    calls: calls.clone(),
                }))
                .unwrap();
            scope.body_exited();
            if adopt {
                assert!(scope.adopt_runtime());
                let publication = scope.state.lock().unwrap().work[Step::Publication as usize]
                    .as_ref()
                    .unwrap()
                    .custody
                    .clone();
                let drops = Arc::new(AtomicUsize::new(0));
                struct PublicationCapture(ExitCustody, Arc<AtomicUsize>);
                impl Drop for PublicationCapture {
                    fn drop(&mut self) {
                        assert_eq!(self.0.phase(), ExitCustodyPhase::Started);
                        self.1.fetch_add(1, Ordering::SeqCst);
                    }
                }
                let capture = PublicationCapture(publication, drops.clone());
                assert!(
                    scope
                        .start_runtime(move || {
                            std::hint::black_box(&capture);
                        })
                        .unwrap_or_else(|_| panic!("original publication admitted"))
                );
                assert_eq!(drops.load(Ordering::SeqCst), 1);
                assert_eq!(calls.load(Ordering::SeqCst), 2);
            } else {
                scope.request_rollback();
                scope.cleanup(PreparationCleanup::Inbound, || {});
                scope.cleanup(PreparationCleanup::Receiver, || {});
                assert!(scope.rollback_exited());
                assert_eq!(calls.load(Ordering::SeqCst), 3);
            }
            assert!(scope.host_captures_exited());
        }
    }
    #[test]
    fn stopped_scope_refuses_adoption_and_publication_but_runs_original_cleanup() {
        for before_adoption in [true, false] {
            let scope = scope();
            scope.register(Arc::new(Child { adoption: None })).unwrap();
            scope.body_exited();
            if !before_adoption {
                assert!(scope.adopt_runtime());
            }
            scope.financial_scope.request_stop().unwrap();
            if before_adoption {
                assert!(!scope.adopt_runtime());
            } else {
                let drops = Arc::new(AtomicUsize::new(0));
                let refused = scope.start_runtime(captured(&drops));
                assert!(refused.is_err());
                assert_eq!(drops.load(Ordering::SeqCst), 0);
                drop(refused);
                assert_eq!(drops.load(Ordering::SeqCst), 1);
            }
            let calls = AtomicUsize::new(0);
            scope.cleanup(PreparationCleanup::Inbound, || {
                calls.fetch_add(1, Ordering::SeqCst);
            });
            scope.cleanup(PreparationCleanup::Receiver, || {
                calls.fetch_add(1, Ordering::SeqCst);
            });
            assert_eq!(calls.load(Ordering::SeqCst), 2);
            assert!(scope.rollback_exited());
            assert!(scope.host_captures_exited());
            assert!(scope.health.is_healthy());
        }
    }
    #[test]
    fn a_registered_pending_child_keeps_the_original_preparation_work_live() {
        use std::sync::atomic::AtomicBool;
        struct PendingChild {
            ready: Arc<AtomicBool>,
            drops: Arc<AtomicUsize>,
        }
        impl TaskPreparationChild for PendingChild {
            fn request_rollback(&self) {}
            fn poll_exit(&self) -> TaskPreparationChildExit {
                if self.ready.load(Ordering::SeqCst) {
                    TaskPreparationChildExit::Exited {
                        output: TaskPreparationOutputExit::NoOutputCreated,
                    }
                } else {
                    TaskPreparationChildExit::Pending
                }
            }
            fn adopt_runtime(&self) {}
        }
        impl Drop for PendingChild {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        let scope = scope();
        let ready = Arc::new(AtomicBool::new(false));
        let drops = Arc::new(AtomicUsize::new(0));
        scope
            .register(Arc::new(PendingChild {
                ready: ready.clone(),
                drops: drops.clone(),
            }))
            .unwrap();
        scope.body_exited();
        scope.request_rollback();
        scope.cleanup(PreparationCleanup::Inbound, || {});
        scope.cleanup(PreparationCleanup::Receiver, || {});
        assert!(!scope.rollback_exited());
        assert!(!scope.custody.publish_whole_exit().unwrap());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        ready.store(true, Ordering::SeqCst);
        assert!(scope.rollback_exited());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(scope.custody.publish_whole_exit().unwrap());
    }
    #[derive(Debug)]
    struct OriginalPanic(u16);
    struct PanickingChild(u16);
    impl TaskPreparationChild for PanickingChild {
        fn request_rollback(&self) {
            if self.0 == 152 {
                std::panic::panic_any(OriginalPanic(self.0));
            }
        }
        fn poll_exit(&self) -> TaskPreparationChildExit {
            if self.0 == 155 {
                std::panic::panic_any(OriginalPanic(self.0));
            }
            TaskPreparationChildExit::Exited {
                output: TaskPreparationOutputExit::NoOutputCreated,
            }
        }
        fn adopt_runtime(&self) {
            if self.0 == 156 {
                std::panic::panic_any(OriginalPanic(self.0));
            }
        }
    }
    impl Drop for PanickingChild {
        fn drop(&mut self) {
            if self.0 == 157 {
                std::panic::panic_any(OriginalPanic(self.0));
            }
        }
    }
    #[test]
    fn each_panicking_host_boundary_keeps_its_work_payload_and_original_health_reason() {
        for code in 152..=158 {
            let scope = scope();
            scope.register(Arc::new(PanickingChild(code))).unwrap();
            scope.body_exited();
            let step = match code {
                156 => {
                    assert!(!scope.adopt_runtime());
                    Step::Adopt
                }
                158 => {
                    assert!(scope.adopt_runtime());
                    assert!(
                        !scope
                            .start_runtime(|| std::panic::panic_any(OriginalPanic(code)))
                            .unwrap_or(false)
                    );
                    Step::Publication
                }
                _ => {
                    scope.request_rollback();
                    scope.cleanup(PreparationCleanup::Inbound, || {
                        if code == 153 {
                            std::panic::panic_any(OriginalPanic(code));
                        }
                    });
                    scope.cleanup(PreparationCleanup::Receiver, || {
                        if code == 154 {
                            std::panic::panic_any(OriginalPanic(code));
                        }
                    });
                    assert!(!scope.rollback_exited());
                    match code {
                        152 => Step::Cancel,
                        153 => Step::Inbound,
                        154 => Step::Receiver,
                        _ => Step::Child,
                    }
                }
            };
            let fault = scope.health.failure().expect("the original callback fault");
            assert_eq!(fault.fact().class(), FailureClass::Unexpected);
            assert_eq!(fault.fact().reason().code(), code);
            assert!(!scope.custody.publish_whole_exit().unwrap());
            let state = scope.state.lock().unwrap();
            let original = state.work[step as usize].as_ref().unwrap();
            assert_eq!(original.custody.phase(), ExitCustodyPhase::Unresolved);
            assert_eq!(
                original
                    .panic
                    .as_ref()
                    .unwrap()
                    .downcast_ref::<OriginalPanic>()
                    .unwrap()
                    .0,
                code
            );
            assert!(!original.retirement.is_retired());
        }
    }
}
