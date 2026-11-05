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

//! A composite's one child continuation, separate from its parent's input slot.
//!
//! A refused derived input stays here unchanged; an accepted input is never
//! offered again. Data completion does not retire the child's lifecycle owner.

use super::ControlStep;
use crate::exec::chunk::Chunk;
use crate::exec::pipeline::{
    driver_step::DriverStep,
    input::{RawInput, derived_raw_input},
    operator::{ExitFailure, ExitObservation, ExitProgress, FinishWatch, Operator},
    operator_exit::{ExitAction, ExitObservations, OperatorExitScope, OwnedOperator},
    step::{AdmissionNext, InputResult, NextStep, StepResult, WaitFact},
};
use crate::runtime::{
    observable::{Observable, ObserverSubscription},
    runtime_state::{RuntimeErrorState, RuntimeState},
    task::{ExecutionFailure, ExecutionResult, PipelineOperation},
};
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

#[derive(Debug)]
enum ChildContractFailure {
    InputSlotOccupied,
    UnexpectedInputRequest,
    InvalidExitWatch,
    CleanupPanic(&'static str),
    MissingFailurePort,
    FailurePortChanged,
}
impl std::fmt::Display for ChildContractFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InputSlotOccupied => "composite child input slot is occupied or closed",
            Self::UnexpectedInputRequest => "composite child requested input after EOS",
            Self::InvalidExitWatch => "composite child has an invalid exit recheck interval",
            Self::CleanupPanic(hook) => return write!(f, "composite child {hook} hook panicked"),
            Self::MissingFailurePort => "composite child has no original failure binding",
            Self::FailurePortChanged => {
                "composite child failure binding differs from its original owner"
            }
        })
    }
}
impl std::error::Error for ChildContractFailure {}
impl novarocks_failure_contract::FailureProjection for ChildContractFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let code = match self {
            Self::InputSlotOccupied => ExecutionReason::ChildInputSlotOccupied,
            Self::UnexpectedInputRequest => ExecutionReason::ChildUnexpectedInputRequest,
            Self::InvalidExitWatch => ExecutionReason::ChildInvalidExitWatch,
            Self::CleanupPanic(_) => ExecutionReason::ChildCleanupPanicked,
            Self::MissingFailurePort => ExecutionReason::ChildMissingFailurePort,
            Self::FailurePortChanged => ExecutionReason::ChildFailurePortChanged,
        };
        FailureFact::new(
            if matches!(self, Self::CleanupPanic(_)) {
                FailureClass::Unexpected
            } else {
                FailureClass::Invariant
            },
            code.reason(),
        )
    }
}
fn fault(cause: ChildContractFailure) -> ExecutionFailure {
    ExecutionFailure::from_domain(cause)
}
fn subscribe(source: &Arc<Observable>, target: &Arc<Observable>) -> ObserverSubscription {
    let target = Arc::downgrade(target);
    source.subscribe(Arc::new(move || {
        if let Some(target) = target.upgrade() {
            target.notify_observers();
        }
    }))
}

pub(crate) struct ChildStep {
    pending: Option<RawInput>,
    operator: OwnedOperator,
    next: NextStep,
    admission: Option<AdmissionNext>,
    finishing: bool,
    finish_signalled: bool,
    target: Arc<Observable>,
    control: Option<(Arc<Observable>, ObserverSubscription)>,
    wait: Option<(Arc<Observable>, ObserverSubscription)>,
    exit_sources: Vec<(Arc<Observable>, ObserverSubscription)>,
    exit_recheck: Option<(
        std::time::Duration,
        crate::exec::pipeline::operator::DriverBlockDeadline,
    )>,
    exit_scope: OperatorExitScope,
    failures: Option<Arc<RuntimeErrorState>>,
    close_started: bool,
    exit_started: bool,
}
impl ChildStep {
    pub(crate) fn new(operator: Box<dyn Operator>, target: Arc<Observable>) -> Self {
        Self {
            pending: None,
            operator: OwnedOperator::new(operator),
            next: NextStep::Runnable,
            admission: None,
            finishing: false,
            finish_signalled: false,
            target,
            control: None,
            wait: None,
            exit_sources: Vec::new(),
            exit_recheck: None,
            exit_scope: OperatorExitScope::new(),
            failures: None,
            close_started: false,
            exit_started: false,
        }
    }
    pub(crate) fn prepare(&mut self) -> ExecutionResult<()> {
        self.operator.prepare()?;
        self.operator.install_control();
        let step = self.operator.as_ref();
        self.next = step.initial_step();
        self.control = self.operator.control_observable().map(|source| {
            let subscription = subscribe(&source, &self.target);
            (source, subscription)
        });
        self.record_wait();
        Ok(())
    }
    fn record_wait(&mut self) {
        let wait = match (&self.next, &self.admission) {
            (NextStep::Wait(wait), _) => Some(wait),
            (_, Some(AdmissionNext::Wait(wait))) => Some(wait),
            _ => None,
        };
        match wait {
            Some(wait)
                if self
                    .control
                    .as_ref()
                    .is_none_or(|(event, _)| !Arc::ptr_eq(event, &wait.observable)) =>
            {
                if self
                    .wait
                    .as_ref()
                    .is_none_or(|(old, _)| !Arc::ptr_eq(old, &wait.observable))
                {
                    self.wait = Some((
                        wait.observable.clone(),
                        subscribe(&wait.observable, &self.target),
                    ));
                }
            }
            _ => {
                self.wait = None;
            }
        }
    }
    pub(crate) fn can_accept(&self) -> bool {
        !self.finishing && self.pending.is_none() && matches!(self.next, NextStep::NeedInput)
    }
    pub(crate) fn input_pending(&self) -> bool {
        self.pending.is_some()
    }
    pub(crate) fn is_complete(&self) -> bool {
        matches!(self.next, NextStep::Finished) && self.pending.is_none()
    }
    pub(crate) fn wait(&self) -> Option<&WaitFact> {
        match (&self.next, &self.admission) {
            (NextStep::Wait(wait), _) => Some(wait),
            (_, Some(AdmissionNext::Wait(wait))) => Some(wait),
            _ => None,
        }
    }
    pub(crate) fn work_next(&self) -> NextStep {
        if self.finishing && !self.finish_signalled && self.pending.is_none() {
            return NextStep::Runnable;
        }
        if matches!(self.next, NextStep::NeedInput) && self.pending.is_some() {
            return match &self.admission {
                Some(AdmissionNext::Wait(wait)) => NextStep::Wait(wait.clone()),
                Some(AdmissionNext::Yield) => NextStep::Yield,
                _ => NextStep::Runnable,
            };
        }
        self.next.clone()
    }
    pub(crate) fn runnable(&self) -> bool {
        if self.finishing && !self.finish_signalled && self.pending.is_none() {
            return true;
        }
        match &self.next {
            NextStep::Runnable | NextStep::Yield => true,
            NextStep::Wait(wait) => wait.is_ready(),
            NextStep::NeedInput if self.pending.is_some() => {
                !matches!(&self.admission, Some(AdmissionNext::Wait(wait)) if !wait.is_ready())
            }
            _ => false,
        }
    }
    pub(crate) fn offer(&mut self, input: Chunk) -> ExecutionResult<()> {
        if !self.can_accept() {
            return Err(fault(ChildContractFailure::InputSlotOccupied));
        }
        self.pending = Some(derived_raw_input(input));
        Ok(())
    }
    pub(crate) fn finish_input(&mut self) {
        self.finishing = true;
    }
    pub(crate) fn poll_control(
        &mut self,
        ctx: &mut DriverStep<'_>,
    ) -> ExecutionResult<ControlStep> {
        if !self.operator.is_live() {
            return Ok(ControlStep::Active);
        }
        let result = self.operator.as_mut().poll_control(ctx)?;
        match result {
            ControlStep::Withdrawn => {
                self.pending = None;
                self.next = NextStep::Finished;
                self.admission = None;
            }
            ControlStep::Runnable => {
                self.next = NextStep::Runnable;
            }
            ControlStep::Active => {}
        }
        self.record_wait();
        Ok(result)
    }
    pub(crate) fn advance(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<Option<Chunk>> {
        if self.finishing && !self.finish_signalled && self.pending.is_none() {
            self.finish_signalled = true;
            if !self.is_complete() {
                ctx.perform(PipelineOperation::Finishing, |ctx| {
                    self.operator.as_mut().finish(ctx)
                })?;
                if matches!(self.next, NextStep::NeedInput) {
                    self.next = NextStep::Runnable;
                }
            }
            return Ok(None);
        }
        if !self.runnable() {
            return Ok(None);
        }
        let result = if matches!(self.next, NextStep::NeedInput) {
            let input = self.pending.take().expect("selected pending child input");
            self.admission = None;
            let result = ctx.perform(PipelineOperation::Admission, |ctx| {
                Ok(match self.operator.as_mut().try_push(ctx, input) {
                    InputResult::Rejected { input, failure } => {
                        let failure = ctx.locate(failure);
                        ctx.report_failure(failure.clone());
                        InputResult::Rejected { input, failure }
                    }
                    InputResult::Accepted(Err(failure)) => {
                        let failure = ctx.locate(failure);
                        ctx.report_failure(failure.clone());
                        InputResult::Accepted(Err(failure))
                    }
                    result => result,
                })
            })?;
            match result {
                InputResult::Accepted(result) => result,
                InputResult::Rejected { input, failure } => {
                    let failure = ctx.locate(failure);
                    ctx.report_failure(failure.clone());
                    self.pending = Some(input);
                    return Err(failure);
                }
                InputResult::Deferred { input, next } => {
                    self.pending = Some(input);
                    if matches!(next, AdmissionNext::ResumeExisting)
                        && !matches!(self.next, NextStep::Wait(_))
                    {
                        self.next = NextStep::Runnable;
                    }
                    self.admission = Some(next);
                    self.record_wait();
                    return Ok(None);
                }
            }
        } else {
            ctx.perform(PipelineOperation::Pull, |ctx| {
                self.operator.as_mut().drive(ctx)
            })
        };
        let StepResult { output, next, .. } = result.map_err(|failure| {
            let failure = ctx.locate(failure);
            ctx.report_failure(failure.clone());
            failure
        })?;
        if self.finish_signalled && matches!(next, NextStep::NeedInput) {
            return Err(fault(ChildContractFailure::UnexpectedInputRequest));
        }
        self.next = next;
        self.record_wait();
        Ok(output)
    }
    pub(crate) fn bind_failure_port(
        &mut self,
        failures: Arc<RuntimeErrorState>,
    ) -> ExecutionResult<()> {
        if let Some(original) = &self.failures {
            if !Arc::ptr_eq(original, &failures) {
                return Err(fault(ChildContractFailure::FailurePortChanged));
            }
        } else {
            self.failures = Some(failures);
        }
        Ok(())
    }
    pub(crate) fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.bind_failure_port(state.error_state())?;
        self.operator.bind_runtime_state(state)
    }
    fn bound_failures(&self) -> ExecutionResult<Arc<RuntimeErrorState>> {
        self.failures
            .clone()
            .ok_or_else(|| fault(ChildContractFailure::MissingFailurePort))
    }
    fn discard(&mut self, failures: &RuntimeErrorState) {
        self.finishing = true;
        let pending = &mut self.pending;
        let admission = &mut self.admission;
        let next = &mut self.next;
        let wait = &mut self.wait;
        let result = self
            .exit_scope
            .run(
                ExitAction::ReleaseInput,
                failures,
                || fault(ChildContractFailure::CleanupPanic("release_input")),
                || {
                    drop(pending.take());
                    *admission = None;
                    *next = NextStep::Finished;
                    *wait = None;
                    Ok(())
                },
            )
            .into_cleanup_result();
        if let Err(error) = result {
            failures.report_cleanup(&error);
        }
    }
    fn run_hook(&mut self, action: ExitAction, failures: &RuntimeErrorState) {
        self.exit_started = true;
        self.discard(failures);
        if !self.operator.is_live() {
            return;
        }
        let operator = &mut self.operator;
        let result = self
            .exit_scope
            .run(
                action,
                failures,
                || {
                    fault(ChildContractFailure::CleanupPanic(match action {
                        ExitAction::Begin => "begin_exit",
                        ExitAction::Close => "close",
                        ExitAction::Cancel => "cancel",
                        ExitAction::FailureStop => "on_driver_failure",
                        _ => unreachable!("closed lifecycle hook set"),
                    }))
                },
                || match action {
                    ExitAction::Begin => operator.begin_exit(),
                    ExitAction::Close => operator.close(),
                    ExitAction::Cancel => {
                        operator.cancel();
                        Ok(())
                    }
                    ExitAction::FailureStop => {
                        operator.on_driver_failure();
                        Ok(())
                    }
                    _ => unreachable!("closed lifecycle hook set"),
                },
            )
            .into_cleanup_result();
        if let Err(error) = result {
            failures.report_cleanup(&error);
        }
        if action == ExitAction::Close {
            self.close_started = true;
        }
    }
    pub(crate) fn begin_exit(&mut self) -> ExecutionResult<()> {
        let failures = self.bound_failures()?;
        self.run_hook(ExitAction::Begin, &failures);
        Ok(())
    }
    pub(crate) fn close(&mut self) -> ExecutionResult<()> {
        let failures = self.bound_failures()?;
        self.run_hook(ExitAction::Close, &failures);
        Ok(())
    }
    fn observe_owner(&mut self, failures: &RuntimeErrorState) -> (ExitObservations, bool, bool) {
        let operator = &mut self.operator;
        let mut returned = false;
        let mut waiting = false;
        let observations = self.exit_scope.observe(
            failures,
            || fault(ChildContractFailure::CleanupPanic("observe_exit")),
            |collect| {
                if operator.is_live() {
                    operator.visit_exits(&mut |event| {
                        waiting |= !matches!(&event.progress, ExitProgress::Exited);
                        collect(event);
                    });
                } else {
                    collect(ExitObservation::exited());
                }
                returned = true;
            },
        );
        (observations, returned, waiting)
    }
    fn advance_exit(&mut self, failures: &RuntimeErrorState) -> (ExitObservations, bool, bool) {
        let (mut events, mut returned, mut waiting) = self.observe_owner(failures);
        if self.exit_started && returned && !waiting && !self.close_started {
            self.run_hook(ExitAction::Close, failures);
            let (next, done, pending) = self.observe_owner(failures);
            events.extend(next);
            returned = done;
            waiting = pending;
        }
        if self.exit_started
            && self.close_started
            && returned
            && !waiting
            && !self.exit_scope.is_unresolved()
            && self.operator.is_live()
        {
            let operator = &mut self.operator;
            let result = self
                .exit_scope
                .run(
                    ExitAction::Dispose,
                    failures,
                    || fault(ChildContractFailure::CleanupPanic("dispose")),
                    || {
                        operator.dispose();
                        Ok(())
                    },
                )
                .into_cleanup_result();
            if let Err(cause) = result {
                failures.report_cleanup(&cause);
            }
            let (next, done, pending) = self.observe_owner(failures);
            events.extend(next);
            returned = done;
            waiting = pending;
        }
        (events, returned, waiting)
    }
    fn collect_retirement(
        &mut self,
        failures: &RuntimeErrorState,
    ) -> (Vec<FinishWatch>, Option<ExecutionFailure>, bool, bool) {
        let (observations, author_returned, author_wait) = self.advance_exit(failures);
        let mut watches = Vec::new();
        let mut required = None;
        for observation in observations {
            if let Some(ExitFailure::Required(error)) = &observation.failure {
                required.get_or_insert_with(|| error.clone());
            }
            match observation.report_to(failures) {
                ExitProgress::Pending(watch) | ExitProgress::Unresolved(watch) => {
                    watches.push(watch)
                }
                ExitProgress::Exited => {}
            }
        }
        (watches, required, author_returned, author_wait)
    }
    /// Observe every accepted child exit before returning an originating error.
    /// Cleanup cannot hide an independent wait or become a data-step failure.
    pub(crate) fn retire(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<NextStep> {
        let failures = ctx.failure_port();
        if self
            .failures
            .as_ref()
            .is_some_and(|original| !std::ptr::eq(original.as_ref(), failures))
        {
            return Err(fault(ChildContractFailure::FailurePortChanged));
        }
        self.run_hook(ExitAction::Begin, failures);
        let generation = self.target.generation();
        let observations = self
            .control
            .iter()
            .chain(self.exit_sources.iter())
            .map(|(event, _)| (event.clone(), event.generation()))
            .collect::<Vec<_>>();
        let (watches, required, _, _) = self.collect_retirement(failures);
        if watches.is_empty() {
            self.exit_sources.clear();
            self.control = None;
            self.exit_recheck = None;
            return required.map_or(Ok(NextStep::Finished), Err);
        }
        self.exit_sources.retain(|(source, _)| {
            watches.iter().any(
                |watch| matches!(watch,FinishWatch::Notify(event) if Arc::ptr_eq(source,event)),
            )
        });
        let mut recheck = None;
        let mut installed = false;
        let mut notified = false;
        let mut invalid_watch = None;
        for watch in watches {
            match watch {
                FinishWatch::Notify(event) => {
                    if let Some((_, observed)) = observations
                        .iter()
                        .find(|(source, _)| Arc::ptr_eq(source, &event))
                    {
                        notified |= event.generation() != *observed;
                    } else {
                        if self
                            .exit_sources
                            .iter()
                            .all(|(source, _)| !Arc::ptr_eq(source, &event))
                        {
                            self.exit_sources
                                .push((event.clone(), subscribe(&event, &self.target)));
                        }
                        installed = true;
                    }
                }
                FinishWatch::RecheckAfter(duration) => {
                    if duration.is_zero() {
                        invalid_watch
                            .get_or_insert_with(|| fault(ChildContractFailure::InvalidExitWatch));
                    } else {
                        recheck = Some(
                            recheck.map_or(duration, |old: std::time::Duration| old.min(duration)),
                        );
                    }
                }
            }
        }
        // All valid sibling watches have been retained before either error.
        if let Some(error) = required {
            return Err(error);
        }
        if let Some(error) = invalid_watch {
            return Err(error);
        }
        if installed || notified || self.target.generation() != generation {
            return Ok(NextStep::Runnable);
        }
        self.exit_recheck = match (recheck, self.exit_recheck) {
            (None, _) => None,
            (Some(duration), Some((old, deadline)))
                if duration == old && std::time::Instant::now() < deadline.at() =>
            {
                Some((old, deadline))
            }
            (Some(duration), _) => Some((
                duration,
                crate::exec::pipeline::operator::DriverBlockDeadline::new(
                    std::time::Instant::now()
                        .checked_add(duration)
                        .ok_or_else(|| fault(ChildContractFailure::InvalidExitWatch))?,
                    ctx.work().generation().get(),
                ),
            )),
        };
        Ok(NextStep::Wait(crate::exec::pipeline::step::WaitFact {
            reason: crate::exec::pipeline::step::WaitReason::Finish,
            observable: self.target.clone(),
            observed_generation: generation,
            deadline: self.exit_recheck.map(|(_, deadline)| deadline),
        }))
    }
    pub(crate) fn cancel(&mut self) {
        let failures = self
            .bound_failures()
            .unwrap_or_else(|_| std::panic::panic_any(ChildContractFailure::MissingFailurePort));
        self.run_hook(ExitAction::Cancel, &failures);
    }
    pub(crate) fn on_driver_failure(&mut self) {
        let failures = self
            .bound_failures()
            .unwrap_or_else(|_| std::panic::panic_any(ChildContractFailure::MissingFailurePort));
        self.run_hook(ExitAction::FailureStop, &failures);
    }
    pub(crate) fn visit_exits(&mut self, visit: &mut dyn FnMut(ExitObservation)) {
        let failures = self
            .bound_failures()
            .unwrap_or_else(|_| std::panic::panic_any(ChildContractFailure::MissingFailurePort));
        let (observations, _, _) = self.advance_exit(&failures);
        // Forward original roles/sources. Only the outer worker consumes Task
        // events, after its entire foreign visit/catch has returned.
        for observation in observations {
            visit(observation);
        }
    }
}
impl Deref for ChildStep {
    type Target = dyn Operator;
    fn deref(&self) -> &Self::Target {
        self.operator.as_ref()
    }
}
impl DerefMut for ChildStep {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.operator.as_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::pipeline::operator::OperatorLifecycle;
    use crate::exec::pipeline::shape::fanout::test_support::{Probe, context, sink, typed_i32};
    use std::sync::atomic::Ordering;

    #[test]
    fn admission_wait_keeps_the_same_raw_and_eos_waits_for_acceptance() {
        let parent = Arc::new(Observable::new());
        let probe = Probe::new(false);
        let mut child = ChildStep::new(sink(probe.clone()), parent.clone());
        child.prepare().unwrap();
        child.offer(typed_i32(vec![4, 9])).unwrap();
        child.finish_input();
        assert!(context(|ctx| child.advance(ctx)).unwrap().is_none());
        assert_eq!(probe.tries.load(Ordering::Acquire), 1);
        assert_eq!(probe.drives.load(Ordering::Acquire), 0);
        assert_eq!(probe.finishes.load(Ordering::Acquire), 0);
        assert!(!child.runnable());
        assert!(context(|ctx| child.advance(ctx)).unwrap().is_none());
        assert_eq!(
            probe.tries.load(Ordering::Acquire),
            1,
            "no polling while the exact admission wait is unchanged"
        );
        let observed = parent.generation();
        probe.ready.store(true, Ordering::Release);
        probe.event.notify_observers();
        assert_ne!(parent.generation(), observed);
        assert!(child.runnable());
        context(|ctx| child.advance(ctx)).unwrap();
        assert!(
            !child.input_pending(),
            "accepted input now belongs to the child"
        );
        assert_eq!(
            probe.tries.load(Ordering::Acquire),
            2,
            "the shared probe verifies raw identity"
        );
        assert_eq!(probe.drives.load(Ordering::Acquire), 0);
        context(|ctx| child.advance(ctx)).unwrap();
        assert_eq!(probe.finishes.load(Ordering::Acquire), 1);
        for _ in 0..4 {
            context(|ctx| child.advance(ctx)).unwrap();
        }
        assert!(child.is_complete());
        assert_eq!(*probe.rows.lock().unwrap(), [4, 9]);
        assert_eq!(
            probe.tries.load(Ordering::Acquire),
            2,
            "never offer accepted input again"
        );
        assert_eq!(probe.finishes.load(Ordering::Acquire), 1);
        assert_eq!(probe.seals.load(Ordering::Acquire), 1);
        drop(child);
        let token = probe.event.subscribe(Arc::new(|| {}));
        drop(token);
    }
    struct ExitWake(Arc<Observable>, Arc<std::sync::atomic::AtomicBool>);
    impl futures::task::ArcWake for ExitWake {
        fn wake_by_ref(this: &Arc<Self>) {
            this.0.notify_observers();
            this.1.store(true, Ordering::Release);
        }
    }
    struct ExitingKernel {
        join: Option<tokio::task::JoinHandle<()>>,
        event: Arc<Observable>,
        begins: Arc<std::sync::atomic::AtomicUsize>,
        closes: Arc<std::sync::atomic::AtomicUsize>,
        joined: Arc<std::sync::atomic::AtomicBool>,
        close_panics: bool,
        wake_complete: Arc<std::sync::atomic::AtomicBool>,
    }
    impl OperatorLifecycle for ExitingKernel {
        fn name(&self) -> &str {
            "EXITING_CHILD"
        }
        fn begin_exit(&mut self) -> ExecutionResult<()> {
            self.begins.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
        fn close(&mut self) -> ExecutionResult<()> {
            assert!(
                self.joined.load(Ordering::Acquire),
                "close requires the actual joined owner"
            );
            self.closes.fetch_add(1, Ordering::AcqRel);
            assert!(!self.close_panics, "injected close panic");
            Ok(())
        }
        fn observe_exit(&mut self) -> ExitObservation {
            use std::{
                future::Future,
                task::{Context, Poll},
            };
            let Some(join) = self.join.as_mut() else {
                return ExitObservation::exited();
            };
            let wake = Arc::new(ExitWake(self.event.clone(), self.wake_complete.clone()));
            let waker = futures::task::waker_ref(&wake);
            let mut context = Context::from_waker(&waker);
            match std::pin::Pin::new(join).poll(&mut context) {
                Poll::Pending => ExitObservation::pending(
                    crate::exec::pipeline::operator::FinishWatch::Notify(self.event.clone()),
                ),
                Poll::Ready(result) => {
                    result.expect("owned exit task");
                    self.join = None;
                    self.joined.store(true, Ordering::Release);
                    ExitObservation::exited()
                }
            }
        }
        fn visit_exits(&mut self, visit: &mut dyn FnMut(ExitObservation)) {
            let observation = self.observe_exit();
            if let ExitProgress::Pending(watch) = &observation.progress {
                visit(ExitObservation::pending(watch.clone()));
            }
            visit(observation);
        }
    }
    impl crate::exec::pipeline::operator::InputEncodingPolicy for ExitingKernel {}
    impl super::super::TransformKernel for ExitingKernel {
        fn apply(
            &mut self,
            _: &mut DriverStep<'_>,
            _: crate::exec::pipeline::input::PreparedInput,
        ) -> ExecutionResult<StepResult> {
            unreachable!("exit fixture has no data work")
        }
    }
    #[test]
    fn retirement_subscribes_then_rechecks_and_requires_join_and_successful_close() {
        use crate::exec::pipeline::driver_step::TurnWork;
        use crate::runtime::runtime_state::RuntimeErrorState;
        use std::{
            sync::atomic::{AtomicBool, AtomicUsize},
            time::{Duration, Instant},
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        for close_panics in [false, true] {
            let event = Arc::new(Observable::try_bounded(1).unwrap());
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            let task_gate = gate.clone();
            let begins = Arc::new(AtomicUsize::new(0));
            let closes = Arc::new(AtomicUsize::new(0));
            let joined = Arc::new(AtomicBool::new(false));
            let wake_complete = Arc::new(AtomicBool::new(false));
            let operator = super::super::Transform::hydrating(ExitingKernel {
                join: Some(runtime.spawn(async move {
                    task_gate.acquire().await.unwrap().forget();
                })),
                event: event.clone(),
                begins: begins.clone(),
                closes: closes.clone(),
                joined: joined.clone(),
                close_panics,
                wake_complete: wake_complete.clone(),
            });
            let mut child = ChildStep::new(Box::new(operator), Arc::new(Observable::new()));
            child.prepare().unwrap();
            let control = RuntimeErrorState::default();
            let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 0);
            let mut value_conversions =
                novarocks_value_arrays::decode::ValueConversionContext::default();
            let mut ctx = DriverStep::new(&control, &mut work, &mut value_conversions);
            assert!(
                matches!(child.retire(&mut ctx).unwrap(), NextStep::Runnable),
                "a newly installed exit watch requires a worker recheck"
            );
            let NextStep::Wait(wait) = child.retire(&mut ctx).unwrap() else {
                panic!("owned task is still gated");
            };
            assert!(!wait.is_ready());
            assert!(wait.deadline.is_none());
            assert_eq!(begins.load(Ordering::Acquire), 1);
            assert_eq!(closes.load(Ordering::Acquire), 0);
            assert!(!joined.load(Ordering::Acquire));
            gate.add_permits(1);
            let deadline = Instant::now() + Duration::from_secs(5);
            while !wait.is_ready() {
                assert!(Instant::now() < deadline);
                std::thread::yield_now();
            }
            let result = child.retire(&mut ctx);
            assert!(
                joined.load(Ordering::Acquire),
                "the join is actually consumed before close"
            );
            if close_panics {
                assert!(
                    control.error().is_none(),
                    "close panic is cleanup/health, never Task origin"
                );
                assert!(control.local_exit_fault().is_some());
                assert!(
                    matches!(result.unwrap(), NextStep::Runnable),
                    "install original unknown watch"
                );
                assert!(
                    matches!(child.retire(&mut ctx).unwrap(), NextStep::Wait(_)),
                    "failed close never certifies successful EOS"
                );
            } else {
                assert!(matches!(result.unwrap(), NextStep::Finished));
                assert!(matches!(
                    child.retire(&mut ctx).unwrap(),
                    NextStep::Finished
                ));
            }
            // Join readiness can precede its waker's notification snapshot
            // exit. The bounded slot remains correctly covered until both end.
            while !wake_complete.load(Ordering::Acquire) {
                assert!(Instant::now() < deadline, "completion waker exits");
                std::thread::yield_now();
            }
            assert_eq!(closes.load(Ordering::Acquire), 1);
            drop(
                event
                    .try_subscribe(Arc::new(|| {}))
                    .expect("retirement returned the sole watch slot"),
            );
        }
    }
    struct HookEvents {
        inner: ExitingKernel,
        begin_error: Option<ExecutionFailure>,
        close_error: Option<ExecutionFailure>,
        event: Option<ExitFailure>,
        begin_panic: Option<Box<dyn std::any::Any + Send>>,
        cancels: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl OperatorLifecycle for HookEvents {
        fn name(&self) -> &str {
            "ACTUAL_EXIT_EVENTS"
        }
        fn begin_exit(&mut self) -> ExecutionResult<()> {
            self.inner.begin_exit()?;
            if let Some(payload) = self.begin_panic.take() {
                std::panic::resume_unwind(payload);
            }
            self.begin_error.take().map_or(Ok(()), Err)
        }
        fn close(&mut self) -> ExecutionResult<()> {
            self.inner.close()?;
            self.close_error.take().map_or(Ok(()), Err)
        }
        fn cancel(&mut self) {
            self.cancels.fetch_add(1, Ordering::SeqCst);
        }
        fn observe_exit(&mut self) -> ExitObservation {
            self.inner.observe_exit()
        }
        fn visit_exits(&mut self, visit: &mut dyn FnMut(ExitObservation)) {
            if let Some(failure) = self.event.take() {
                visit(ExitObservation::exited().with_failure(failure));
            }
            self.inner.visit_exits(visit);
        }
    }
    impl crate::exec::pipeline::operator::InputEncodingPolicy for HookEvents {}
    impl super::super::TransformKernel for HookEvents {
        fn apply(
            &mut self,
            _: &mut DriverStep<'_>,
            _: crate::exec::pipeline::input::PreparedInput,
        ) -> ExecutionResult<StepResult> {
            unreachable!("exit-only real task fixture")
        }
    }
    struct ValueDropCount {
        values: Vec<u64>,
        drops: Arc<std::sync::atomic::AtomicUsize>,
    }
    impl Drop for ValueDropCount {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct ReleaseGate(Arc<tokio::sync::Semaphore>);
    impl Drop for ReleaseGate {
        fn drop(&mut self) {
            self.0.add_permits(1);
        }
    }
    struct ExitFixture {
        gate: ReleaseGate,
        resource: std::sync::Weak<ValueDropCount>,
        resource_drops: Arc<std::sync::atomic::AtomicUsize>,
        begins: Arc<std::sync::atomic::AtomicUsize>,
        closes: Arc<std::sync::atomic::AtomicUsize>,
        cancels: Arc<std::sync::atomic::AtomicUsize>,
        joined: Arc<std::sync::atomic::AtomicBool>,
        wake_complete: Arc<std::sync::atomic::AtomicBool>,
        event: Arc<Observable>,
    }
    fn exit_child(
        runtime: &tokio::runtime::Runtime,
        failures: Arc<RuntimeErrorState>,
        event: Option<ExitFailure>,
        begin_error: Option<ExecutionFailure>,
        close_error: Option<ExecutionFailure>,
        begin_panic: Option<Box<dyn std::any::Any + Send>>,
    ) -> (ChildStep, ExitFixture) {
        use std::sync::atomic::{AtomicBool, AtomicUsize};
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let task_gate = gate.clone();
        let drops = Arc::new(AtomicUsize::new(0));
        let resource = Arc::new(ValueDropCount {
            values: vec![7, 11, 19],
            drops: drops.clone(),
        });
        let weak = Arc::downgrade(&resource);
        let begins = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let cancels = Arc::new(AtomicUsize::new(0));
        let joined = Arc::new(AtomicBool::new(false));
        let wake_complete = Arc::new(AtomicBool::new(false));
        let observable = Arc::new(Observable::try_bounded(1).unwrap());
        let kernel = HookEvents {
            inner: ExitingKernel {
                join: Some(runtime.spawn(async move {
                    let original = resource;
                    task_gate.acquire().await.unwrap().forget();
                    assert_eq!(original.values, [7, 11, 19]);
                })),
                event: observable.clone(),
                begins: begins.clone(),
                closes: closes.clone(),
                joined: joined.clone(),
                close_panics: false,
                wake_complete: wake_complete.clone(),
            },
            begin_error,
            close_error,
            event,
            begin_panic,
            cancels: cancels.clone(),
        };
        let mut child = ChildStep::new(
            Box::new(super::super::Transform::hydrating(kernel)),
            Arc::new(Observable::new()),
        );
        child.bind_failure_port(failures).unwrap();
        child.prepare().unwrap();
        (
            child,
            ExitFixture {
                gate: ReleaseGate(gate),
                resource: weak,
                resource_drops: drops,
                begins,
                closes,
                cancels,
                joined,
                wake_complete,
                event: observable,
            },
        )
    }
    fn wait_until_exit_ready(mut ready: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !ready() {
            assert!(
                std::time::Instant::now() < deadline,
                "actual task/waker gate must progress"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn returned_cleanup_keeps_peer_and_real_pending_sibling_while_required_refines_once() {
        use crate::runtime::task_control::test_support::Owner;
        use novarocks_execution_contract::{
            AbortCause, TaskConclusionPort, TaskConclusionProposal, TaskFailurePhase,
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        for required in [false, true] {
            let task = Owner::new();
            task.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
            let accepted = task.accepted().unwrap();
            let failures = Arc::new(RuntimeErrorState::for_task(Arc::new(
                task.view(TaskFailurePhase::Execution),
            )));
            let cleanup = fault(ChildContractFailure::CleanupPanic("returned_begin_error"));
            let original: ExecutionFailure = novarocks_functions::KernelFailure::InvalidProgram(
                novarocks_functions::KernelDiagnostic::new("original child output failure"),
            )
            .into();
            let event = if required {
                ExitFailure::Required(original.clone())
            } else {
                ExitFailure::Cleanup(original.clone())
            };
            let (mut child, fixture) = exit_child(
                &runtime,
                failures.clone(),
                Some(event),
                Some(cleanup),
                Some(fault(ChildContractFailure::CleanupPanic(
                    "returned_close_error",
                ))),
                None,
            );
            let mut work =
                crate::exec::pipeline::driver_step::TurnWork::new(std::num::NonZeroU64::MIN, 0);
            let mut value_conversions =
                novarocks_value_arrays::decode::ValueConversionContext::default();
            let mut ctx = DriverStep::new(&failures, &mut work, &mut value_conversions);
            let first = child.retire(&mut ctx);
            if required {
                assert!(first.unwrap_err().same_occurrence(&original));
                assert!(
                    task.accepted()
                        .unwrap()
                        .originating_failure()
                        .unwrap()
                        .same_occurrence(&original)
                );
            } else {
                assert!(matches!(first.unwrap(), NextStep::Runnable));
                assert!(
                    Arc::ptr_eq(&accepted, &task.accepted().unwrap()),
                    "neither returned hook Err nor Cleanup event refines Peer"
                );
            }
            assert_eq!(
                child.exit_sources.len(),
                1,
                "all duplicate sibling waits retained even after Required Err"
            );
            assert_eq!(fixture.resource.upgrade().unwrap().values, [7, 11, 19]);
            assert_eq!(fixture.resource_drops.load(Ordering::SeqCst), 0);
            assert!(!fixture.joined.load(Ordering::Acquire));
            let NextStep::Wait(wait) = child.retire(&mut ctx).unwrap() else {
                panic!("independent task remains gated");
            };
            assert!(!wait.is_ready());
            assert!(wait.deadline.is_none());
            fixture.gate.0.add_permits(1);
            wait_until_exit_ready(|| wait.is_ready());
            assert!(matches!(
                child.retire(&mut ctx).unwrap(),
                NextStep::Finished
            ));
            assert!(fixture.joined.load(Ordering::Acquire));
            assert!(fixture.resource.upgrade().is_none());
            assert_eq!(fixture.resource_drops.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.begins.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.closes.load(Ordering::SeqCst), 1);
            for _ in 0..3 {
                assert!(matches!(
                    child.retire(&mut ctx).unwrap(),
                    NextStep::Finished
                ));
            }
            assert_eq!(fixture.closes.load(Ordering::SeqCst), 1);
            wait_until_exit_ready(|| fixture.wake_complete.load(Ordering::Acquire));
            drop(
                fixture
                    .event
                    .try_subscribe(Arc::new(|| {}))
                    .expect("real join retired actual bounded watch slot"),
            );
            if required {
                assert!(
                    task.accepted()
                        .unwrap()
                        .originating_failure()
                        .unwrap()
                        .same_occurrence(&original)
                );
            } else {
                assert!(Arc::ptr_eq(&accepted, &task.accepted().unwrap()));
            }
        }
    }

    #[test]
    fn ordinary_and_opaque_begin_panic_never_repeat_and_safe_actual_exit_still_closes() {
        use std::sync::atomic::AtomicUsize;
        struct Data {
            values: Vec<u64>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for Data {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct Secondary {
            _data: Arc<Data>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for Secondary {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        struct Primary {
            data: std::mem::ManuallyDrop<Arc<Data>>,
            drops: Arc<AtomicUsize>,
            secondary: Arc<AtomicUsize>,
            opaque: bool,
        }
        impl Drop for Primary {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
                let data = unsafe { std::mem::ManuallyDrop::take(&mut self.data) };
                if self.opaque {
                    std::panic::panic_any(Secondary {
                        _data: data,
                        drops: self.secondary.clone(),
                    });
                }
                drop(data);
            }
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        for opaque in [false, true] {
            let failures = Arc::new(RuntimeErrorState::default());
            let data_drops = Arc::new(AtomicUsize::new(0));
            let primary = Arc::new(AtomicUsize::new(0));
            let secondary = Arc::new(AtomicUsize::new(0));
            let data = Arc::new(Data {
                values: vec![31, 37],
                drops: data_drops.clone(),
            });
            let weak = Arc::downgrade(&data);
            let payload = Box::new(Primary {
                data: std::mem::ManuallyDrop::new(data),
                drops: primary.clone(),
                secondary: secondary.clone(),
                opaque,
            });
            let (mut child, fixture) =
                exit_child(&runtime, failures.clone(), None, None, None, Some(payload));
            let mut work =
                crate::exec::pipeline::driver_step::TurnWork::new(std::num::NonZeroU64::MIN, 0);
            let mut value_conversions =
                novarocks_value_arrays::decode::ValueConversionContext::default();
            let mut ctx = DriverStep::new(&failures, &mut work, &mut value_conversions);
            assert!(matches!(
                child.retire(&mut ctx).unwrap(),
                NextStep::Runnable
            ));
            assert_eq!(
                child.exit_sources.len(),
                2,
                "safe join plus independent stable unknown"
            );
            assert_eq!(primary.load(Ordering::SeqCst), 1);
            assert_eq!(secondary.load(Ordering::SeqCst), 0);
            assert!(failures.error().is_none());
            assert!(failures.local_exit_fault().is_some());
            let health = failures.local_exit_fault();
            let NextStep::Wait(wait) = child.retire(&mut ctx).unwrap() else {
                panic!("safe owner still gated");
            };
            assert!(!wait.is_ready());
            assert!(!fixture.joined.load(Ordering::Acquire));
            assert_eq!(fixture.closes.load(Ordering::SeqCst), 0);
            fixture.gate.0.add_permits(1);
            wait_until_exit_ready(|| wait.is_ready());
            let next = child.retire(&mut ctx).unwrap();
            assert!(
                !matches!(next, NextStep::Finished),
                "safe sibling exit cannot certify unwound Begin"
            );
            assert!(fixture.joined.load(Ordering::Acquire));
            assert_eq!(fixture.resource_drops.load(Ordering::SeqCst), 1);
            assert!(fixture.resource.upgrade().is_none());
            assert_eq!(
                fixture.closes.load(Ordering::SeqCst),
                1,
                "close advances only after actual independent join"
            );
            for _ in 0..3 {
                assert!(!matches!(
                    child.retire(&mut ctx).unwrap(),
                    NextStep::Finished
                ));
            }
            child.cancel();
            child.cancel();
            assert_eq!(fixture.cancels.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.begins.load(Ordering::SeqCst), 1);
            assert_eq!(fixture.closes.load(Ordering::SeqCst), 1);
            assert_eq!(primary.load(Ordering::SeqCst), 1);
            assert_eq!(failures.local_exit_fault(), health);
            assert!(failures.error().is_none());
            wait_until_exit_ready(|| fixture.wake_complete.load(Ordering::Acquire));
            drop(
                fixture
                    .event
                    .try_subscribe(Arc::new(|| {}))
                    .expect("safe actual join released its watch despite unknown peer hook"),
            );
            if opaque {
                assert_eq!(weak.upgrade().unwrap().values, [31, 37]);
                assert_eq!(data_drops.load(Ordering::SeqCst), 0);
            } else {
                assert!(weak.upgrade().is_none());
                assert_eq!(data_drops.load(Ordering::SeqCst), 1);
            }
            drop(child);
            assert_eq!(secondary.load(Ordering::SeqCst), 0);
            if opaque {
                assert_eq!(weak.upgrade().unwrap().values, [31, 37]);
            }
        }
    }

    #[test]
    fn public_visit_keeps_original_event_unconsumed_until_outer_worker_and_rebinding_is_rejected() {
        use crate::runtime::task_control::test_support::Owner;
        use novarocks_execution_contract::{
            AbortCause, TaskConclusionPort, TaskConclusionProposal, TaskFailurePhase,
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let task = Owner::new();
        task.submit(TaskConclusionProposal::Aborted(AbortCause::PeerTaskFailed));
        let accepted = task.accepted().unwrap();
        let failures = Arc::new(RuntimeErrorState::for_task(Arc::new(
            task.view(TaskFailurePhase::Execution),
        )));
        let original = fault(ChildContractFailure::CleanupPanic("actual_required_event"));
        let (mut child, fixture) = exit_child(
            &runtime,
            failures.clone(),
            Some(ExitFailure::Required(original.clone())),
            None,
            None,
            None,
        );
        child.bind_failure_port(failures.clone()).unwrap();
        assert!(
            child
                .bind_failure_port(Arc::new(RuntimeErrorState::default()))
                .is_err()
        );
        assert!(Arc::ptr_eq(child.failures.as_ref().unwrap(), &failures));
        let mut events = Vec::new();
        child.visit_exits(&mut |event| events.push(event));
        assert!(
            Arc::ptr_eq(&accepted, &task.accepted().unwrap()),
            "foreign visit only collects; it never consumes Task events"
        );
        for event in events {
            event.report_to(&failures);
        }
        let origin = task.accepted().unwrap();
        assert!(
            origin
                .originating_failure()
                .unwrap()
                .same_occurrence(&original)
        );
        let mut repeated = Vec::new();
        child.visit_exits(&mut |event| repeated.push(event));
        assert!(repeated.iter().all(|event| event.failure.is_none()));
        fixture.gate.0.add_permits(1);
        let mut work =
            crate::exec::pipeline::driver_step::TurnWork::new(std::num::NonZeroU64::MIN, 0);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = DriverStep::new(&failures, &mut work, &mut value_conversions);
        wait_until_exit_ready(|| matches!(child.retire(&mut ctx).unwrap(), NextStep::Finished));
        assert!(fixture.joined.load(Ordering::Acquire));
        assert_eq!(fixture.resource_drops.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.closes.load(Ordering::SeqCst), 1);
        assert!(Arc::ptr_eq(&origin, &task.accepted().unwrap()));
    }
}
