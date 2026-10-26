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
//! Bounded, original-owner custody for the three named completion roles.
//!
//! This is not a destructor safety boundary: panic=abort and a destructor's
//! hidden double panic cannot be recovered by catch_unwind. An opaque payload
//! is never inspected or destroyed. Its original role is quarantined instead.

use std::any::Any;
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use crate::runtime::fragment::{ExecutionFailure, ExecutionReason, ExecutionResult};
use crate::runtime::runtime_state::RuntimeErrorState;
use novarocks_execution_contract::CompletionWork;
use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection};

pub(crate) type Payload = ManuallyDrop<Box<dyn Any + Send>>;
pub(crate) type Callback<F> = Box<dyn FnMut(&F) -> bool + Send + 'static>;
pub(crate) type Notification<F> = Box<dyn FnMut(&F) + Send + 'static>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Role {
    PipelineFinisher,
    WallTime,
    TerminalPublisher,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Phase {
    Call,
    CaptureDrop,
    ResourceHook,
    ResourceHolderDrop,
    CleanupNotification,
}

#[derive(Debug)]
pub(crate) struct CompletionFailure {
    pub(crate) role: Role,
    pub(crate) phase: Phase,
}
impl std::fmt::Display for CompletionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?} {:?} panicked; its original exit scope is unresolved",
            self.role, self.phase
        )
    }
}
impl std::error::Error for CompletionFailure {}
impl FailureProjection for CompletionFailure {
    fn failure_fact(&self) -> FailureFact {
        let reason = match (self.role, self.phase) {
            (_, Phase::CleanupNotification) => ExecutionReason::PipelineFinisherCallPanicked,
            (_, Phase::ResourceHook) => ExecutionReason::FragmentResourceHookPanicked,
            (_, Phase::ResourceHolderDrop) => ExecutionReason::FragmentResourceHolderDropPanicked,
            (Role::PipelineFinisher, Phase::Call) => ExecutionReason::PipelineFinisherCallPanicked,
            (Role::PipelineFinisher, Phase::CaptureDrop) => {
                ExecutionReason::PipelineFinisherCaptureDropPanicked
            }
            (Role::WallTime, Phase::Call) => ExecutionReason::WallTimeCallPanicked,
            (Role::WallTime, Phase::CaptureDrop) => ExecutionReason::WallTimeCaptureDropPanicked,
            (Role::TerminalPublisher, Phase::Call) => {
                ExecutionReason::TerminalPublisherCallPanicked
            }
            (Role::TerminalPublisher, Phase::CaptureDrop) => {
                ExecutionReason::TerminalPublisherCaptureDropPanicked
            }
        };
        FailureFact::new(FailureClass::Unexpected, reason.reason())
    }
}

#[derive(Debug)]
struct BindingConflict(Role, bool);
impl std::fmt::Display for BindingConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?} completion role {}",
            self.0,
            if self.1 {
                "requires its original callback"
            } else {
                "is already bound"
            }
        )
    }
}
impl std::error::Error for BindingConflict {}
impl FailureProjection for BindingConflict {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Invariant,
            ExecutionReason::CompletionRoleBindingConflict.reason(),
        )
    }
}

struct State<F> {
    work: Option<CompletionWork>,
    callback: Option<ManuallyDrop<Callback<F>>>,
    bound: bool,
    call_started: bool,
    disposed: bool,
    call_returned: bool,
    dispose_requested: bool,
    call_payload: Option<Payload>,
    dispose_payload: Option<Payload>,
    // Reporting is metadata too. A broken reporter must not recursively report
    // itself or destroy a second opaque payload during unwind.
    report_payloads: [[Option<Payload>; 2]; 2],
}

pub(crate) struct OnceRole<F> {
    state: Mutex<State<F>>,
    role: Role,
    quarantine: Mutex<Option<Arc<Self>>>,
    #[cfg(test)]
    after_call_published: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl<F: Send + 'static> OnceRole<F> {
    pub(crate) fn new(role: Role) -> Arc<Self> {
        Self::with_work(role, None)
    }

    pub(crate) fn from_admitted(role: Role, work: CompletionWork) -> Arc<Self> {
        Self::with_work(role, Some(work))
    }

    fn with_work(role: Role, work: Option<CompletionWork>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                work,
                callback: None,
                bound: false,
                call_started: false,
                disposed: false,
                call_returned: false,
                dispose_requested: false,
                call_payload: None,
                dispose_payload: None,
                report_payloads: [[None, None], [None, None]],
            }),
            role,
            quarantine: Mutex::new(None),
            #[cfg(test)]
            after_call_published: Mutex::new(None),
        })
    }

    /// Before a dormant execution is handed to any runner, its preparation
    /// owner transfers the one already admitted role into this original cell.
    /// A rejected transfer preserves the caller's unique work authority.
    pub(crate) fn admit_work(&self, slot: &mut Option<CompletionWork>) -> ExecutionResult<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.bound || state.work.is_some() || slot.is_none() {
            return Err(ExecutionFailure::from_domain(BindingConflict(
                self.role, false,
            )));
        }
        state.work = slot.take();
        Ok(())
    }

    /// A failed binding leaves the original callback in the caller's slot.
    /// No rejected foreign capture is destroyed by this API.
    pub(crate) fn bind(self: &Arc<Self>, slot: &mut Option<Callback<F>>) -> ExecutionResult<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.bound {
            return Err(ExecutionFailure::from_domain(BindingConflict(
                self.role, false,
            )));
        }
        if slot.is_none() {
            return Err(ExecutionFailure::from_domain(BindingConflict(
                self.role, true,
            )));
        }
        if let Some(work) = state.work.as_mut() {
            work.bind();
        }
        state.callback = slot.take().map(ManuallyDrop::new);
        state.bound = true;
        self.retain();
        drop(state);
        Ok(())
    }

    pub(crate) fn bind_notification(
        self: &Arc<Self>,
        slot: &mut Option<Notification<F>>,
    ) -> ExecutionResult<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.bound {
            return Err(ExecutionFailure::from_domain(BindingConflict(
                self.role, false,
            )));
        }
        if slot.is_none() {
            return Err(ExecutionFailure::from_domain(BindingConflict(
                self.role, true,
            )));
        }
        if let Some(work) = state.work.as_mut() {
            work.bind();
        }
        state.callback = slot.take().map(|mut callback| {
            ManuallyDrop::new(Box::new(move |fact: &F| {
                callback(fact);
                true
            }) as Callback<F>)
        });
        state.bound = true;
        self.retain();
        drop(state);
        Ok(())
    }

    /// A concurrent resource interruption can finish after the one logical
    /// stopped call. Its original owner supplies that independent exit proof.
    pub(crate) fn complete_deferred(self: &Arc<Self>, control: &RuntimeErrorState) {
        let returned = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.dispose_requested = true;
            state.call_returned
        };
        if returned {
            self.dispose(control);
        }
    }

    pub(crate) fn invoke(self: &Arc<Self>, fact: &F, control: &RuntimeErrorState) {
        let (mut callback, mut work) = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.call_started || !state.bound {
                return;
            }
            state.call_started = true;
            (state.callback.take(), state.work.take())
        };
        let Some(mut callback_value) = callback.take() else {
            return;
        };
        // The work owner is a metadata port, but it may take its own ledger
        // lock. Never call even that port while holding callback custody.
        if let Some(work) = work.as_mut() {
            work.started();
        }
        let result = catch_unwind(AssertUnwindSafe(|| callback_value(fact)));
        // Restore the original capture before diagnostics can invoke any host.
        let call_completed = matches!(result, Ok(true));
        let call_panicked = result.is_err();
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.callback = Some(callback_value);
            state.work = work;
            // Establish deferred retention before completion can observe the
            // returned call and dispose its capture.
            if !call_completed {
                self.retain();
            }
            state.call_returned = true;
            if let Err(payload) = result {
                state.call_payload = Some(ManuallyDrop::new(payload));
            }
        }
        #[cfg(test)]
        {
            let hook = self.after_call_published.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        if call_panicked {
            self.report(control, Phase::Call);
        }
        // An unresolved resource finisher still owns its real holders. A pure
        // metadata role can retire its capture independently of notification.
        let dispose_requested = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .dispose_requested;
        if call_completed
            || dispose_requested
            || (call_panicked && self.role != Role::PipelineFinisher)
        {
            self.dispose(control);
        }
    }

    fn dispose(self: &Arc<Self>, control: &RuntimeErrorState) {
        let callback = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.disposed {
                return;
            }
            state.disposed = true;
            state.callback.take()
        };
        if let Some(callback) = callback {
            let result = catch_unwind(AssertUnwindSafe(|| {
                drop(ManuallyDrop::into_inner(callback))
            }));
            if let Err(payload) = result {
                self.state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .dispose_payload = Some(ManuallyDrop::new(payload));
                self.retain();
                self.report(control, Phase::CaptureDrop);
            } else {
                let (no_payload, work) = {
                    let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                    let no_payload = state.call_payload.is_none()
                        && state
                            .report_payloads
                            .iter()
                            .all(|phase| phase.iter().all(Option::is_none));
                    let work = no_payload.then(|| state.work.take()).flatten();
                    (no_payload, work)
                };
                if no_payload {
                    // `disposed` only claims disposal. Positive exit belongs
                    // here, after the actual capture destructor returned and
                    // after any deferred original resource work finished.
                    if let Some(work) = work {
                        work.completed();
                    }
                    let pin = self
                        .quarantine
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .take();
                    drop(pin);
                }
            }
        }
    }

    fn retain(self: &Arc<Self>) {
        let mut pin = self.quarantine.lock().unwrap_or_else(|e| e.into_inner());
        if pin.is_none() {
            *pin = Some(Arc::clone(self));
        }
    }

    fn report(self: &Arc<Self>, control: &RuntimeErrorState, phase: Phase) {
        let cause = ExecutionFailure::from_domain(CompletionFailure {
            role: self.role,
            phase,
        });
        let report_phase = match phase {
            Phase::Call => 0,
            Phase::CaptureDrop => 1,
            _ => unreachable!("named role reporting phase"),
        };
        for (ordinal, report) in [
            RuntimeErrorState::report_cleanup as fn(&RuntimeErrorState, &ExecutionFailure),
            RuntimeErrorState::report_unresolved_exit,
        ]
        .into_iter()
        .enumerate()
        {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| report(control, &cause))) {
                self.state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .report_payloads[report_phase][ordinal] = Some(ManuallyDrop::new(payload));
                self.retain();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct WorkEvents(Arc<Mutex<Vec<&'static str>>>);
    impl novarocks_execution_contract::CompletionWorkCapability for WorkEvents {
        fn started(&mut self) {
            self.0.lock().unwrap().push("started");
        }
        fn completed(self: Box<Self>) {
            self.0.lock().unwrap().push("completed");
        }
        fn unused_exited(self: Box<Self>) {
            self.0.lock().unwrap().push("unused");
        }
        fn unresolved(self: Box<Self>) {
            self.0.lock().unwrap().push("unresolved");
        }
    }
    fn admitted(role: Role, events: &Arc<Mutex<Vec<&'static str>>>) -> Arc<OnceRole<()>> {
        OnceRole::from_admitted(
            role,
            CompletionWork::from_admitted_role(None, Box::new(WorkEvents(events.clone()))),
        )
    }

    #[test]
    fn an_unused_role_exits_only_when_its_original_holder_is_dropped() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let role = admitted(Role::TerminalPublisher, &events);
        assert!(events.lock().unwrap().is_empty());
        drop(role);
        assert_eq!(*events.lock().unwrap(), ["unused"]);
    }

    #[test]
    fn admitted_work_spans_the_original_deferred_finisher() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let drops = Arc::new(AtomicUsize::new(0));
        let capture = Capture(drops.clone());
        let role = admitted(Role::PipelineFinisher, &events);
        let mut callback: Option<Callback<()>> = Some(Box::new(move |_| {
            std::hint::black_box(&capture);
            false
        }));
        let control = RuntimeErrorState::default();
        role.bind(&mut callback).unwrap();
        role.invoke(&(), &control);
        assert_eq!(*events.lock().unwrap(), ["started"]);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        role.complete_deferred(&control);
        role.complete_deferred(&control);
        assert_eq!(*events.lock().unwrap(), ["started", "completed"]);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn claiming_disposal_does_not_report_capture_exit_before_its_destructor_returns() {
        struct BlockingCapture {
            entered: std::sync::mpsc::Sender<()>,
            release: std::sync::mpsc::Receiver<()>,
        }
        impl Drop for BlockingCapture {
            fn drop(&mut self) {
                self.entered.send(()).unwrap();
                self.release.recv().unwrap();
            }
        }
        let events = Arc::new(Mutex::new(Vec::new()));
        let role = admitted(Role::WallTime, &events);
        let (entered, on_entered) = std::sync::mpsc::channel();
        let (release, on_release) = std::sync::mpsc::channel();
        let capture = BlockingCapture {
            entered,
            release: on_release,
        };
        let mut callback: Option<Callback<()>> = Some(Box::new(move |_| {
            std::hint::black_box(&capture);
            true
        }));
        role.bind(&mut callback).unwrap();
        let original = role.clone();
        let thread = std::thread::spawn(move || {
            original.invoke(&(), &RuntimeErrorState::default());
        });
        on_entered.recv().unwrap();
        assert!(role.state.lock().unwrap().disposed);
        assert_eq!(*events.lock().unwrap(), ["started"]);
        release.send(()).unwrap();
        thread.join().unwrap();
        assert_eq!(*events.lock().unwrap(), ["started", "completed"]);
    }

    #[test]
    fn callback_or_capture_panic_keeps_the_original_work_unresolved() {
        struct PanicCapture;
        impl Drop for PanicCapture {
            fn drop(&mut self) {
                panic!("capture exit is unknown");
            }
        }
        for capture_panics in [false, true] {
            let events = Arc::new(Mutex::new(Vec::new()));
            let role = admitted(Role::TerminalPublisher, &events);
            let mut callback: Option<Callback<()>> = if capture_panics {
                let capture = PanicCapture;
                Some(Box::new(move |_| {
                    std::hint::black_box(&capture);
                    true
                }))
            } else {
                Some(Box::new(|_| panic!("call exit is unknown")))
            };
            role.bind(&mut callback).unwrap();
            let control = RuntimeErrorState::default();
            role.invoke(&(), &control);
            assert_eq!(*events.lock().unwrap(), ["started"]);
            assert!(role.state.lock().unwrap().work.is_some());
            assert!(control.local_exit_fault().is_some());
        }
    }

    struct Capture(Arc<AtomicUsize>);
    impl Drop for Capture {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    // The original deferred owner can complete immediately after call_returned
    // is published. It must release both the capture and the role's own pin.
    // This owned, per-role hook fixes that real scheduling point; the helper
    // thread is joined and the oracle is actual capture/allocation destruction.
    #[test]
    fn deferred_completion_between_call_publication_and_retention_releases_the_role() {
        let role = OnceRole::<()>::new(Role::PipelineFinisher);
        let control = Arc::new(RuntimeErrorState::default());
        let drops = Arc::new(AtomicUsize::new(0));
        let capture = Capture(drops.clone());
        let mut callback: Option<Callback<()>> = Some(Box::new(move |_| {
            std::hint::black_box(&capture);
            false
        }));
        role.bind(&mut callback).unwrap();
        let weak = Arc::downgrade(&role);
        let completion = weak.clone();
        let completed_control = control.clone();
        *role.after_call_published.lock().unwrap() = Some(Box::new(move || {
            let role = completion.upgrade().unwrap();
            std::thread::spawn(move || role.complete_deferred(&completed_control))
                .join()
                .expect("the original deferred owner actually exits");
        }));
        role.invoke(&(), &control);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(
            role.quarantine.lock().unwrap().is_none(),
            "a disposed role cannot be pinned again after deferred completion"
        );
        drop(role);
        assert!(
            weak.upgrade().is_none(),
            "the role's allocation is reclaimed"
        );
        assert!(control.exit_health_reporter().failure().is_none());
    }
}
