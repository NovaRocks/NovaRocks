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

//! Once cleanup calls and opaque-panic custody for one original operator owner.
//!
//! The enclosing Driver instance, ChildStep or Fanout branch must retain its
//! entire original owner while this scope is unresolved. This helper owns only
//! the fixed control metadata and any secondary panic payload, not that owner.
//! It neither accepts a Task conclusion nor substitutes for physical exit.

use super::operator::{ExitObservation, ExitProgress, FinishWatch};
use crate::runtime::{
    observable::Observable,
    runtime_state::RuntimeErrorState,
    task::{ExecutionFailure, ExecutionResult},
};
use std::{
    any::Any,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};

type PanicPayload = Box<dyn Any + Send + 'static>;

/// One original operator Box. Metadata remains readable after explicit disposal.
/// Installed owners dispose through their own ExitAction::Dispose; before
/// installation the original synchronous preparation body retains its Drop.
pub(crate) struct OwnedOperator {
    value: Option<ManuallyDrop<Box<dyn super::operator::Operator>>>,
    name: String,
    control: Option<Arc<Observable>>,
    control_installed: bool,
}
impl OwnedOperator {
    pub(crate) fn new(value: Box<dyn super::operator::Operator>) -> Self {
        let name = value.name().to_owned();
        Self {
            value: Some(ManuallyDrop::new(value)),
            name,
            control: None,
            control_installed: false,
        }
    }
    /// Snapshot the endpoint only after the author's preparation installed it.
    /// Later retirement cannot replace its identity or call through a dead Box.
    pub(crate) fn install_control(&mut self) {
        if !self.control_installed {
            self.control = self.as_ref().control_observable();
            self.control_installed = true;
        }
    }
    pub(crate) fn name(&self) -> &str {
        &self.name
    }
    pub(crate) fn control_observable(&self) -> Option<Arc<Observable>> {
        self.control.clone()
    }
    pub(crate) fn is_live(&self) -> bool {
        self.value.is_some()
    }
    /// Taking does not run foreign code. Only the enclosing original exit
    /// scope may attempt the final destructor and retain an unknown payload.
    pub(crate) fn take(&mut self) -> Option<ManuallyDrop<Box<dyn super::operator::Operator>>> {
        self.value.take()
    }
    pub(crate) fn dispose(&mut self) {
        if let Some(mut value) = self.take() {
            // The Box is attempted once synchronously. Its dead ManuallyDrop
            // wrapper is never moved back into the original owner.
            unsafe {
                ManuallyDrop::drop(&mut value);
            }
        }
    }
}
impl AsRef<dyn super::operator::Operator> for OwnedOperator {
    fn as_ref(&self) -> &(dyn super::operator::Operator + 'static) {
        self.value
            .as_ref()
            .expect("live installed operator")
            .as_ref()
    }
}
impl AsMut<dyn super::operator::Operator> for OwnedOperator {
    fn as_mut(&mut self) -> &mut (dyn super::operator::Operator + 'static) {
        self.value
            .as_mut()
            .expect("live installed operator")
            .as_mut()
    }
}
impl std::ops::Deref for OwnedOperator {
    type Target = dyn super::operator::Operator;
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}
impl std::ops::DerefMut for OwnedOperator {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut()
    }
}
impl Drop for OwnedOperator {
    fn drop(&mut self) {
        // Dormant preparation retains its existing synchronous body boundary.
        // An installed unknown composite is retained by the original Driver;
        // it never reaches this fallback with a still-live foreign Box.
        self.dispose();
    }
}

/// A closed author phase list; indices select fixed original-owner custody.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub(crate) enum ExitAction {
    Begin,
    Close,
    Cancel,
    FailureStop,
    Observe,
    ReleaseInput,
    Dispose,
}
const EXIT_ACTIONS: usize = 7;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ActionState {
    NotStarted,
    Started,
    Returned,
    AuthorHandled,
    Unknown,
}

/// Returned errors preserve the exact original cause for one caller consumer.
/// Panics are cleanup/health incidents already reported to that same binding.
pub(crate) enum HookOutcome {
    Returned(ExecutionResult<()>),
    AlreadyStarted,
    AuthorHandled,
    Unresolved,
}

impl HookOutcome {
    /// Only an ordinary returned error needs the caller's cleanup consumer.
    /// Ok here is not a physical-exit receipt: always observe the scope too.
    pub(crate) fn into_cleanup_result(self) -> ExecutionResult<()> {
        match self {
            Self::Returned(result) => result,
            Self::AlreadyStarted | Self::AuthorHandled | Self::Unresolved => Ok(()),
        }
    }
}

/// The ordinary observation is inline. Additional authored events and waits
/// use a cold tail; healthy data turns do not allocate a Vec for every operator.
/// Repeated pure Exited elements aggregate only this visit's actual proofs.
/// No result is cached for a future claim, body or exit obligation.
pub(crate) struct ExitObservations {
    first: Option<ExitObservation>,
    tail: Vec<ExitObservation>,
}
impl ExitObservations {
    pub(crate) fn new() -> Self {
        Self {
            first: None,
            tail: Vec::new(),
        }
    }
    fn pure_exit(event: &ExitObservation) -> bool {
        matches!(&event.progress, ExitProgress::Exited)
            && event.failure.is_none()
            && event.unresolved_exit.is_none()
    }
    fn push(&mut self, event: ExitObservation) {
        if let Some(first) = &self.first {
            if self.tail.is_empty() && Self::pure_exit(first) && Self::pure_exit(&event) {
                return;
            }
            self.tail.push(event);
        } else {
            self.first = Some(event);
        }
    }
    pub(crate) fn extend(&mut self, observations: Self) {
        for event in observations {
            self.push(event);
        }
    }
    #[cfg(test)]
    fn pop(&mut self) -> Option<ExitObservation> {
        self.tail.pop().or_else(|| self.first.take())
    }
    #[cfg(test)]
    fn len(&self) -> usize {
        usize::from(self.first.is_some()) + self.tail.len()
    }
}
impl IntoIterator for ExitObservations {
    type Item = ExitObservation;
    type IntoIter = std::iter::Chain<
        std::option::IntoIter<ExitObservation>,
        std::vec::IntoIter<ExitObservation>,
    >;
    fn into_iter(self) -> Self::IntoIter {
        self.first.into_iter().chain(self.tail)
    }
}

pub(crate) struct OperatorExitScope {
    states: [ActionState; EXIT_ACTIONS],
    unresolved: bool,
    unknown_watch: Arc<Observable>,
    // The original opaque payload is disposed once outside all author locks.
    // A second opaque destructor panic cannot be safely disposed recursively.
    // ManuallyDrop deliberately preserves that payload through scope Drop.
    primary: [Option<ManuallyDrop<PanicPayload>>; EXIT_ACTIONS],
    secondary: [Option<ManuallyDrop<PanicPayload>>; EXIT_ACTIONS],
}

impl OperatorExitScope {
    pub(crate) fn new() -> Self {
        Self {
            states: [ActionState::NotStarted; EXIT_ACTIONS],
            unresolved: false,
            unknown_watch: Arc::new(Observable::new()),
            primary: std::array::from_fn(|_| None),
            secondary: std::array::from_fn(|_| None),
        }
    }

    /// `fault` is the caller's bounded, typed domain projection. Normal calls
    /// do not construct an error. The exact runtime binding is borrowed anew
    /// for every invocation; this scope stores no local or Task authority.
    pub(crate) fn run(
        &mut self,
        action: ExitAction,
        failures: &RuntimeErrorState,
        fault: impl FnOnce() -> ExecutionFailure,
        hook: impl FnOnce() -> ExecutionResult<()>,
    ) -> HookOutcome {
        assert_ne!(
            action,
            ExitAction::Observe,
            "use observe for exit observation"
        );
        let index = action as usize;
        if self.states[index] != ActionState::NotStarted {
            return if self.states[index] == ActionState::Unknown {
                HookOutcome::Unresolved
            } else {
                HookOutcome::AlreadyStarted
            };
        }
        self.states[index] = ActionState::Started;
        match catch_unwind(AssertUnwindSafe(hook)) {
            Ok(result) => {
                self.states[index] = ActionState::Returned;
                HookOutcome::Returned(result)
            }
            Err(payload) if crate::exec::operators::scan::is_scan_exit_unwind(payload.as_ref()) => {
                // Scan already saved the real phase, original payload custody
                // and health report. This closed, fieldless marker is inert.
                self.states[index] = ActionState::AuthorHandled;
                drop(payload);
                HookOutcome::AuthorHandled
            }
            Err(payload) => {
                self.record_unknown(action, failures, fault, payload);
                HookOutcome::Unresolved
            }
        }
    }

    /// Collect first, return, and only then let the caller consume Task events.
    /// No external event consumer runs inside the catch or the author's visit.
    /// One scope wraps one original owner: composites visit child-owned scopes
    /// independently, rather than replaying a consumed sibling prefix.
    pub(crate) fn observe(
        &mut self,
        failures: &RuntimeErrorState,
        fault: impl FnOnce() -> ExecutionFailure,
        mut visit: impl FnMut(&mut dyn FnMut(ExitObservation)),
    ) -> ExitObservations {
        // A cleanup hook and its independent exit observer are separate
        // actions. A hook unwind cannot stop known observer progress; only an
        // unwind of that observer itself forbids its foreign re-entry.
        if self.states[ExitAction::Observe as usize] == ActionState::Unknown {
            let mut observations = ExitObservations::new();
            observations.push(self.unresolved_observation());
            return observations;
        }
        let mut observations = ExitObservations::new();
        let mut fault = Some(fault);
        loop {
            self.states[ExitAction::Observe as usize] = ActionState::Started;
            match catch_unwind(AssertUnwindSafe(|| {
                visit(&mut |event| observations.push(event))
            })) {
                Ok(()) => {
                    self.states[ExitAction::Observe as usize] = ActionState::Returned;
                    if self.unresolved {
                        observations.push(self.unresolved_observation());
                    }
                    return observations;
                }
                Err(payload)
                    if crate::exec::operators::scan::is_scan_exit_unwind(payload.as_ref()) =>
                {
                    // Only this exact marker permits re-entry. Scan's closed
                    // phase machine never retries an unwound foreign action;
                    // re-entry reads the phase it saved before propagating.
                    self.states[ExitAction::Observe as usize] = ActionState::AuthorHandled;
                    drop(payload);
                }
                Err(payload) => {
                    self.record_unknown(
                        ExitAction::Observe,
                        failures,
                        fault.take().expect("one unknown incident per scope"),
                        payload,
                    );
                    observations.push(self.unresolved_observation());
                    return observations;
                }
            }
        }
    }

    /// Root's healthy quantum released all of its original temporary holders.
    /// Reuse only fully returned actions with no opaque custody. The stable
    /// unknown watch is retained; no per-batch Observable or phase grows here.
    pub(crate) fn reset_healthy_root_quantum(&mut self) -> bool {
        if self.unresolved
            || self.states[ExitAction::Begin as usize] != ActionState::Returned
            || self
                .states
                .iter()
                .any(|state| !matches!(state, ActionState::NotStarted | ActionState::Returned))
            || self.primary.iter().any(Option::is_some)
            || self.secondary.iter().any(Option::is_some)
        {
            return false;
        }
        self.states.fill(ActionState::NotStarted);
        true
    }

    pub(crate) fn is_unresolved(&self) -> bool {
        self.unresolved
    }

    pub(crate) fn unresolved_observation(&self) -> ExitObservation {
        // This distinct stable watch intentionally never self-wakes: unknown
        // work cannot be made runnable by a periodic recheck or stop intent.
        // Health was already reported once through the original runtime.
        ExitObservation::unresolved(FinishWatch::Notify(Arc::clone(&self.unknown_watch)))
    }

    fn record_unknown(
        &mut self,
        action: ExitAction,
        failures: &RuntimeErrorState,
        fault: impl FnOnce() -> ExecutionFailure,
        payload: PanicPayload,
    ) {
        let index = action as usize;
        self.states[index] = ActionState::Unknown;
        self.unresolved = true;
        // Save original opaque custody before even the bounded projection or
        // runtime reporting. If one of those unexpectedly unwinds, the exact
        // original payload remains addressable in this owner, never a lost
        // stack-local ManuallyDrop allocation.
        self.primary[index] = Some(ManuallyDrop::new(payload));
        let cause = fault();
        failures.report_unresolved_exit(&cause);
        failures.report_cleanup(&cause);
        // There is no scope lock and no foreign hook is active here. The
        // original payload is attempted exactly once, after neutral reporting.
        let mut payload = self.primary[index].take().expect("saved original primary");
        if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| unsafe {
            ManuallyDrop::drop(&mut payload)
        })) {
            self.secondary[index] = Some(ManuallyDrop::new(secondary));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::pipeline::operator::{ExitFailure, ExitProgress};
    use crate::runtime::{
        execution_health::{ExecutionExitOrigin, ExecutionHealth},
        task_control::{TaskExecutionControl, TaskExecutionPort, test_support::Owner},
    };
    use novarocks_execution_contract::{
        AbortCause, AcceptedTaskConclusion, TaskConclusionPort, TaskConclusionProposal,
        TaskFailurePhase, TaskIdentity,
    };
    use novarocks_failure_contract::{
        FailureClass, FailureFact, FailureProjection, FailureReason, domain,
    };
    use std::{
        error::Error,
        fmt,
        num::NonZeroU16,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    #[derive(Debug)]
    struct Fault;
    impl fmt::Display for Fault {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("operator exit scope test fault")
        }
    }
    impl Error for Fault {}
    impl FailureProjection for Fault {
        fn failure_fact(&self) -> FailureFact {
            FailureFact::new(
                FailureClass::Unexpected,
                FailureReason::new(domain::EXECUTION, NonZeroU16::new(65010).unwrap()),
            )
        }
    }
    fn fault() -> ExecutionFailure {
        ExecutionFailure::from_domain(Fault)
    }

    // Reuse the actual Task-author acceptance fixture, not a second latch.
    // This forwarding port exposes its exact shared Health for the oracle.
    struct Port {
        task: Arc<Owner>,
        health: Arc<ExecutionHealth>,
        cleanup: Mutex<Option<ExecutionFailure>>,
        cleanups: AtomicUsize,
    }
    impl TaskConclusionPort<ExecutionFailure> for Port {
        fn identity(&self) -> TaskIdentity {
            self.task.identity()
        }
        fn accepted(&self) -> Option<Arc<AcceptedTaskConclusion<ExecutionFailure>>> {
            self.task.accepted()
        }
        fn submit(&self, proposed: TaskConclusionProposal<ExecutionFailure>) {
            self.task.submit(proposed)
        }
    }
    impl TaskExecutionPort for Port {
        fn execution_health(&self) -> Arc<crate::runtime::execution_health::ExecutionHealth> {
            self.health.clone()
        }
        fn report_cleanup(&self, cause: &ExecutionFailure, _: TaskFailurePhase) {
            self.cleanups.fetch_add(1, Ordering::SeqCst);
            *self.cleanup.lock().unwrap() = Some(cause.clone());
        }
        fn report_unresolved_exit(&self, fact: FailureFact, phase: TaskFailurePhase) {
            self.health.report_unresolved(
                ExecutionExitOrigin::Task {
                    identity: self.identity(),
                    phase,
                },
                fact,
            );
        }
        fn is_stopped(&self) -> bool {
            self.task.is_stopped()
        }
        fn stop_observable(&self) -> Arc<Observable> {
            self.task.stop_observable()
        }
        fn wait_until_stopped(&self, duration: Duration) -> bool {
            self.task.wait_until_stopped(duration)
        }
    }
    fn binding() -> (Arc<Port>, RuntimeErrorState) {
        let port = Arc::new(Port {
            task: Owner::new(),
            health: Arc::new(ExecutionHealth::default()),
            cleanup: Mutex::new(None),
            cleanups: AtomicUsize::new(0),
        });
        let control = TaskExecutionControl::for_test(
            port.identity(),
            port.clone(),
            TaskFailurePhase::Execution,
        )
        .unwrap();
        (port, RuntimeErrorState::for_task(Arc::new(control)))
    }
    fn watch(observation: ExitObservation) -> Arc<Observable> {
        assert!(observation.failure.is_none());
        assert!(
            observation.unresolved_exit.is_none(),
            "original runtime already consumed this incident"
        );
        let ExitProgress::Unresolved(FinishWatch::Notify(watch)) = observation.progress else {
            panic!("unknown is neither physical exit nor timer recheck");
        };
        watch
    }

    #[test]
    fn healthy_root_quanta_reset_once_actions_without_replacing_original_watch() {
        let (port, failures) = binding();
        let mut scope = OperatorExitScope::new();
        let original_watch = Arc::clone(&scope.unknown_watch);
        let mut calls = 0;
        for _ in 0..2 {
            assert!(matches!(
                scope.run(
                    ExitAction::Begin,
                    &failures,
                    || panic!("healthy root release has no fault"),
                    || {
                        calls += 1;
                        Ok(())
                    }
                ),
                HookOutcome::Returned(Ok(()))
            ));
            assert!(scope.reset_healthy_root_quantum());
            assert!(Arc::ptr_eq(&original_watch, &scope.unknown_watch));
        }
        assert_eq!(calls, 2);
        assert!(port.health.is_healthy());
        assert_eq!(port.cleanups.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn unknown_root_quantum_never_resets_or_repeats_its_foreign_action() {
        let (port, failures) = binding();
        let mut scope = OperatorExitScope::new();
        let original_watch = Arc::clone(&scope.unknown_watch);
        let mut calls = 0;
        assert!(matches!(
            scope.run(ExitAction::Begin, &failures, fault, || {
                calls += 1;
                panic!("real root workspace unwind")
            }),
            HookOutcome::Unresolved
        ));
        assert!(!scope.reset_healthy_root_quantum());
        assert!(matches!(
            scope.run(
                ExitAction::Begin,
                &failures,
                || panic!("unknown must not reconstruct failure"),
                || {
                    calls += 1;
                    Ok(())
                }
            ),
            HookOutcome::Unresolved
        ));
        assert_eq!(calls, 1);
        assert!(Arc::ptr_eq(&original_watch, &scope.unknown_watch));
        assert!(!port.health.is_healthy());
        assert!(port.task.accepted().is_none());
    }

    #[test]
    fn returned_error_is_original_once_and_normal_hooks_do_not_construct_faults() {
        let (port, failures) = binding();
        let original = fault();
        let mut scope = OperatorExitScope::new();
        let mut calls = 0;
        let result = scope.run(
            ExitAction::Close,
            &failures,
            || panic!("normal hook must not build panic failure"),
            || {
                calls += 1;
                Err(original.clone())
            },
        );
        let HookOutcome::Returned(Err(actual)) = result else {
            panic!("return original error");
        };
        assert!(actual.same_occurrence(&original));
        assert!(matches!(
            scope.run(
                ExitAction::Close,
                &failures,
                || panic!("not called"),
                || {
                    calls += 1;
                    Ok(())
                }
            ),
            HookOutcome::AlreadyStarted
        ));
        assert_eq!(calls, 1);
        assert!(!scope.is_unresolved());
        assert!(port.health.is_healthy());
        assert_eq!(
            port.cleanups.load(Ordering::SeqCst),
            0,
            "caller alone consumes ordinary returned cleanup"
        );
        failures.report_cleanup(&actual);
        assert!(
            port.cleanup
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .same_occurrence(&original)
        );
        let mut events = scope.observe(
            &failures,
            || panic!("normal observation must not build fault"),
            |visit| visit(ExitObservation::exited()),
        );
        assert!(matches!(
            events.pop().unwrap().progress,
            ExitProgress::Exited
        ));
    }

    #[test]
    fn primary_drop_follows_shared_health_and_cleanup_without_refining_peer() {
        struct Capture {
            drops: Arc<AtomicUsize>,
        }
        impl Drop for Capture {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct Primary {
            capture: Arc<Capture>,
            port: Arc<Port>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for Primary {
            fn drop(&mut self) {
                assert!(
                    !self.port.health.is_healthy(),
                    "health committed before foreign payload destructor"
                );
                assert_eq!(self.port.cleanups.load(Ordering::SeqCst), 1);
                assert_eq!(Arc::strong_count(&self.capture), 1);
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        let (port, failures) = binding();
        port.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        let accepted = port.accepted().unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let capture_drops = Arc::new(AtomicUsize::new(0));
        let capture = Arc::new(Capture {
            drops: capture_drops.clone(),
        });
        let weak = Arc::downgrade(&capture);
        let primary = Primary {
            capture,
            port: port.clone(),
            drops: drops.clone(),
        };
        let original = fault();
        let mut scope = OperatorExitScope::new();
        let mut calls = 0;
        assert!(matches!(
            scope.run(
                ExitAction::Begin,
                &failures,
                || original.clone(),
                || {
                    calls += 1;
                    std::panic::panic_any(primary)
                }
            ),
            HookOutcome::Unresolved
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(capture_drops.load(Ordering::SeqCst), 1);
        assert!(weak.upgrade().is_none());
        assert!(Arc::ptr_eq(&accepted, &port.accepted().unwrap()));
        assert!(
            port.cleanup
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .same_occurrence(&original)
        );
        for _ in 0..3 {
            assert!(matches!(
                scope.run(
                    ExitAction::Begin,
                    &failures,
                    || panic!("no new fault"),
                    || {
                        calls += 1;
                        Ok(())
                    }
                ),
                HookOutcome::Unresolved
            ));
        }
        assert_eq!(calls, 1);
        assert_eq!(port.cleanups.load(Ordering::SeqCst), 1);
        let first = watch(
            scope
                .observe(
                    &failures,
                    || panic!("no new fault"),
                    |visit| visit(ExitObservation::exited()),
                )
                .pop()
                .unwrap(),
        );
        for _ in 0..3 {
            let next = watch(
                scope
                    .observe(
                        &failures,
                        || panic!("no new fault"),
                        |visit| visit(ExitObservation::exited()),
                    )
                    .pop()
                    .unwrap(),
            );
            assert!(Arc::ptr_eq(&first, &next));
            assert_eq!(next.generation(), 0);
        }
    }

    #[test]
    fn secondary_preserves_actual_capture_and_other_phases_and_sibling_continue() {
        struct Capture {
            drops: Arc<AtomicUsize>,
            data: Vec<u64>,
        }
        impl Drop for Capture {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct Secondary {
            _capture: Arc<Capture>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for Secondary {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct Primary {
            capture: ManuallyDrop<Arc<Capture>>,
            primary_drops: Arc<AtomicUsize>,
            secondary_drops: Arc<AtomicUsize>,
        }
        impl Drop for Primary {
            fn drop(&mut self) {
                self.primary_drops.fetch_add(1, Ordering::SeqCst);
                let capture = unsafe { ManuallyDrop::take(&mut self.capture) };
                std::panic::panic_any(Secondary {
                    _capture: capture,
                    drops: self.secondary_drops.clone(),
                });
            }
        }
        let (port, failures) = binding();
        let mut scope = OperatorExitScope::new();
        let mut sibling = OperatorExitScope::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let primary_drops = Arc::new(AtomicUsize::new(0));
        let secondary_drops = Arc::new(AtomicUsize::new(0));
        let capture = Arc::new(Capture {
            drops: drops.clone(),
            data: vec![7, 11, 19],
        });
        let weak = Arc::downgrade(&capture);
        let primary = Primary {
            capture: ManuallyDrop::new(capture),
            primary_drops: primary_drops.clone(),
            secondary_drops: secondary_drops.clone(),
        };
        assert!(matches!(
            scope.run(ExitAction::Cancel, &failures, fault, || {
                std::panic::panic_any(primary)
            }),
            HookOutcome::Unresolved
        ));
        let mut completed = 0;
        for action in [
            ExitAction::Begin,
            ExitAction::Close,
            ExitAction::FailureStop,
            ExitAction::ReleaseInput,
        ] {
            assert!(matches!(
                scope.run(
                    action,
                    &failures,
                    || panic!("normal independent phase"),
                    || {
                        completed += 1;
                        Ok(())
                    }
                ),
                HookOutcome::Returned(Ok(()))
            ));
            assert!(matches!(
                scope.run(action, &failures, || panic!("once"), || panic!("no repeat")),
                HookOutcome::AlreadyStarted
            ));
        }
        assert_eq!(completed, 4);
        let unknown = watch(
            scope
                .observe(&failures, fault, |visit| visit(ExitObservation::exited()))
                .pop()
                .unwrap(),
        );
        let mut events = sibling.observe(
            &failures,
            || panic!("sibling successful"),
            |visit| visit(ExitObservation::exited()),
        );
        assert!(matches!(
            events.pop().unwrap().progress,
            ExitProgress::Exited
        ));
        assert!(!sibling.is_unresolved());
        assert_eq!(unknown.generation(), 0);
        assert_eq!(primary_drops.load(Ordering::SeqCst), 1);
        assert_eq!(secondary_drops.load(Ordering::SeqCst), 0);
        assert_eq!(weak.upgrade().unwrap().data, [7, 11, 19]);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(!port.health.is_healthy());
        drop(scope);
        drop(sibling);
        drop(failures);
        drop(port);
        assert_eq!(weak.upgrade().unwrap().data, [7, 11, 19]);
        assert_eq!(secondary_drops.load(Ordering::SeqCst), 0);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            0,
            "opaque custody is never implicitly retried during scope Drop"
        );
    }

    #[test]
    fn observe_preserves_original_collected_prefix_then_never_reenters_after_panic() {
        let (_port, failures) = binding();
        let mut first = OperatorExitScope::new();
        let mut second = OperatorExitScope::new();
        let original = fault();
        let mut calls = 0;
        let mut events = first.observe(&failures, fault, |visit| {
            calls += 1;
            visit(ExitObservation::exited().with_failure(ExitFailure::Required(original.clone())));
            panic!("foreign observe after owned prefix");
        });
        assert_eq!(events.len(), 2);
        let unknown = watch(events.pop().unwrap());
        let ExitFailure::Required(actual) = events.pop().unwrap().failure.unwrap() else {
            panic!("original role");
        };
        assert!(actual.same_occurrence(&original));
        let again = watch(
            first
                .observe(
                    &failures,
                    || panic!("fault once"),
                    |_| {
                        calls += 1;
                        panic!("no reentry")
                    },
                )
                .pop()
                .unwrap(),
        );
        assert!(Arc::ptr_eq(&unknown, &again));
        assert_eq!(calls, 1);
        let other = watch(
            second
                .observe(&failures, fault, |_| panic!("different original scope"))
                .pop()
                .unwrap(),
        );
        assert!(!Arc::ptr_eq(&unknown, &other));
    }
    #[test]
    fn begin_panic_does_not_stop_independent_actual_exit_but_whole_stays_unknown() {
        use std::sync::atomic::AtomicBool;
        struct Resource {
            drops: Arc<AtomicUsize>,
        }
        impl Drop for Resource {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        let (_port, failures) = binding();
        let mut scope = OperatorExitScope::new();
        assert!(matches!(
            scope.run(ExitAction::Begin, &failures, fault, || panic!(
                "actual begin hook unwind"
            )),
            HookOutcome::Unresolved
        ));
        let gate = Arc::new(AtomicBool::new(false));
        let ready = Arc::new(Observable::new());
        let drops = Arc::new(AtomicUsize::new(0));
        let resource = Arc::new(Resource {
            drops: drops.clone(),
        });
        let weak = Arc::downgrade(&resource);
        let mut held = Some(resource);
        let mut polls = 0;
        let mut observe = |visit: &mut dyn FnMut(ExitObservation)| {
            polls += 1;
            if gate.load(Ordering::Acquire) {
                drop(held.take());
                visit(ExitObservation::exited());
            } else {
                visit(ExitObservation::pending(FinishWatch::Notify(ready.clone())));
            }
        };
        let mut initial = scope.observe(
            &failures,
            || panic!("independent observer is normal"),
            &mut observe,
        );
        assert_eq!(initial.len(), 2);
        let unknown = watch(initial.pop().unwrap());
        assert!(matches!(
            initial.pop().unwrap().progress,
            ExitProgress::Pending(_)
        ));
        assert!(weak.upgrade().is_some());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        gate.store(true, Ordering::Release);
        ready.notify_observers();
        let mut final_events = scope.observe(
            &failures,
            || panic!("independent observer still normal"),
            &mut observe,
        );
        assert_eq!(final_events.len(), 2);
        let still_unknown = watch(final_events.pop().unwrap());
        assert!(Arc::ptr_eq(&unknown, &still_unknown));
        assert!(matches!(
            final_events.pop().unwrap().progress,
            ExitProgress::Exited
        ));
        assert_eq!(polls, 2);
        assert!(weak.upgrade().is_none());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(
            scope.is_unresolved(),
            "independent actual exit does not prove the unwound Begin hook exited"
        );
    }
    #[test]
    fn projection_unwind_leaves_primary_addressable_in_its_original_fixed_slot() {
        struct Capture {
            data: Vec<u64>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for Capture {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        let (_port, failures) = binding();
        let mut scope = OperatorExitScope::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let capture = Arc::new(Capture {
            data: vec![23, 29],
            drops: drops.clone(),
        });
        let weak = Arc::downgrade(&capture);
        let outer = catch_unwind(AssertUnwindSafe(|| {
            scope.run(
                ExitAction::ReleaseInput,
                &failures,
                || panic!("projection unexpectedly unwound"),
                || std::panic::panic_any(capture),
            )
        }));
        assert!(outer.is_err());
        drop(outer);
        let saved = scope.primary[ExitAction::ReleaseInput as usize]
            .as_ref()
            .expect("original payload is addressable, not a lost local ManuallyDrop");
        let capture = saved.downcast_ref::<Arc<Capture>>().unwrap();
        assert_eq!(capture.data, [23, 29]);
        assert!(scope.is_unresolved());
        assert!(weak.upgrade().is_some());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(scope);
        assert_eq!(weak.upgrade().unwrap().data, [23, 29]);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn repeated_healthy_and_composite_exit_observations_keep_the_cold_tail_unallocated() {
        let failures = RuntimeErrorState::default();
        let mut scope = OperatorExitScope::new();
        for _ in 0..1024 {
            let events = scope.observe(
                &failures,
                || panic!("normal observation"),
                |visit| {
                    for _ in 0..64 {
                        visit(ExitObservation::exited());
                    }
                },
            );
            assert_eq!(
                events.len(),
                1,
                "only this visit's neutral exit proofs aggregate"
            );
            assert_eq!(
                events.tail.capacity(),
                0,
                "normal driver turns allocate no cold tail"
            );
            assert!(matches!(
                events.first.unwrap().progress,
                ExitProgress::Exited
            ));
        }
        assert!(!scope.is_unresolved());
    }
}

#[cfg(test)]
mod owned_operator_tests {
    use super::*;
    use crate::exec::pipeline::{
        driver_step::DriverStep,
        instance::OperatorInstance,
        operator::{InputEncodingPolicy, OperatorLifecycle},
        shape::StepOperator,
        step::{NextStep, StepResult},
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct MetadataOnly {
        event: Arc<Observable>,
        queries: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for MetadataOnly {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl OperatorLifecycle for MetadataOnly {
        fn name(&self) -> &str {
            "original cached endpoint"
        }
    }
    impl InputEncodingPolicy for MetadataOnly {}
    impl StepOperator for MetadataOnly {
        fn initial_step(&self) -> NextStep {
            NextStep::Finished
        }
        fn control_observable(&self) -> Option<Arc<Observable>> {
            self.queries.fetch_add(1, Ordering::SeqCst);
            Some(self.event.clone())
        }
        fn try_push(
            &mut self,
            _: &mut DriverStep<'_>,
            _: super::super::input::RawInput,
        ) -> super::super::step::InputResult {
            unreachable!("retired metadata owner accepts no data")
        }
        fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<()> {
            Ok(())
        }
        fn drive(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
            unreachable!("metadata-only retired owner has no data work")
        }
    }
    #[test]
    fn disposed_instance_keeps_its_installed_control_identity_without_calling_the_dead_box() {
        let event = Arc::new(Observable::new());
        let queries = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let failures = Arc::new(RuntimeErrorState::default());
        let mut instance = OperatorInstance::new(
            0,
            Box::new(MetadataOnly {
                event: event.clone(),
                queries: queries.clone(),
                drops: drops.clone(),
            }),
            None,
            failures.clone(),
        );
        let before = StepOperator::control_observable(&instance).unwrap();
        instance.begin_exit().unwrap();
        instance.visit_exits(&mut |event| {
            assert!(matches!(event.report_to(&failures), ExitProgress::Exited));
        });
        instance.close().unwrap();
        instance.visit_exits(&mut |event| {
            assert!(matches!(event.report_to(&failures), ExitProgress::Exited));
        });
        instance.dispose_after_observed_exit();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        for _ in 0..3 {
            let after = StepOperator::control_observable(&instance).unwrap();
            assert!(Arc::ptr_eq(&before, &after));
            assert!(Arc::ptr_eq(&after, &event));
            assert_eq!(instance.name(), "original cached endpoint");
            instance.dispose_after_observed_exit();
            instance.visit_exits(&mut |event| {
                assert!(matches!(event.report_to(&failures), ExitProgress::Exited));
            });
        }
        assert_eq!(
            queries.load(Ordering::SeqCst),
            1,
            "retirement metadata queries never re-enter the foreign owner"
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        drop(instance);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(failures.error().is_none());
        assert!(failures.local_exit_fault().is_none());
    }
}
