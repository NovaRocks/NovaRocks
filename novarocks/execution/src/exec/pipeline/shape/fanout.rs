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

//! Common transformation state machine over the low-level step contract.

//! A fanout owns one accepted parent input and one raw slot per branch.
//! A blocked branch never hides another branch's runnable work or actual exit.

use super::{
    ControlStep, HydrateInput, InputPreparation, PreparationStep, ReceiverPreparation,
    ReceiverSlot, StepOperator,
};
use crate::exec::pipeline::{
    operator::{ExitObservation, OperatorLifecycle},
    operator_exit::{ExitAction, ExitObservations, OperatorExitScope, OwnedOperator},
};
use crate::exec::{
    chunk::Chunk,
    pipeline::{
        driver_step::DriverStep,
        input::{AcceptedInput, InputPreparationFailure, PreparedInput, RawInput},
        operator::{ConstructionFailure, InputConstructionFactory, InputEncodingPolicy, Operator},
        step::{AdmissionNext, InputResult, NextStep, StepResult, WaitFact, WaitReason},
    },
};
use crate::runtime::{
    mem_tracker::MemTracker,
    observable::{Observable, ObserverSubscription},
    profile::OperatorProfiles,
    runtime_state::{RuntimeErrorState, RuntimeState},
    task::{ExecutionFailure, ExecutionResult, io::TaskEventSink},
};
use std::sync::Arc;

/// A router owns domain ordering and its continuation. Each accepted parent
/// input visits a branch at most once. It creates one raw branch value at a
/// time through its installed creator, never an eager pile of expanded input.
pub(crate) trait FanoutRouter: InputEncodingPolicy {
    type Cursor: Send;
    fn begin(
        &mut self,
        input: PreparedInput,
        factory: &dyn InputConstructionFactory,
        ctx: &mut DriverStep<'_>,
    ) -> ExecutionResult<Self::Cursor>;
    fn advance(
        &mut self,
        cursor: &mut Self::Cursor,
        factory: &dyn InputConstructionFactory,
        ctx: &mut DriverStep<'_>,
    ) -> ExecutionResult<RouteStep>;
    fn accepts_branch(&self, branch: usize) -> bool;
    fn requires_branch_drain(&self, _branch: usize) -> bool {
        false
    }
    fn input_exhausted(&mut self, _ctx: &mut DriverStep<'_>) -> ExecutionResult<()> {
        Ok(())
    }
    fn visit_demand_events(&self, _visit: &mut dyn FnMut(Arc<Observable>)) {}
}
pub(crate) enum RouteStep {
    Input { branch: usize, chunk: Chunk },
    Finished,
    Skip,
}
#[derive(Default)]
struct FanoutPreparation(HydrateInput);
impl<R: FanoutRouter> InputPreparation<R> for FanoutPreparation {
    fn admission(
        &mut self,
        _: &mut DriverStep<'_>,
        input: &RawInput,
        _: &R,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        HydrateInput::validate_uncovered(input)?;
        Ok(None)
    }
    fn accept(&mut self, input: RawInput) -> AcceptedInput {
        self.0.accept_raw(input)
    }
    fn prepare(
        &mut self,
        ctx: &mut DriverStep<'_>,
        input: AcceptedInput,
        router: &R,
    ) -> Result<PreparationStep, InputPreparationFailure> {
        self.0.prepare_accepted(ctx, input, router)
    }
}
struct Branch {
    pending: Option<RawInput>,
    operator: OwnedOperator,
    next: NextStep,
    input_closed: bool,
    admission: Option<AdmissionNext>,
    control_event: Option<Arc<Observable>>,
    exit_scope: OperatorExitScope,
    close_started: bool,
    wait: Option<(Arc<Observable>, ObserverSubscription)>,
}
impl Branch {
    fn observe_owner(&mut self, failures: &RuntimeErrorState) -> (ExitObservations, bool) {
        let mut returned = false;
        let mut waiting = false;
        let operator = &mut self.operator;
        let events = self.exit_scope.observe(
            failures,
            || fault(FanoutFailure::CleanupPanic("observe-exit")),
            |collect| {
                if operator.is_live() {
                    operator.visit_exits(&mut |event| {
                        waiting |= !matches!(
                            &event.progress,
                            crate::exec::pipeline::operator::ExitProgress::Exited
                        );
                        collect(event);
                    });
                } else {
                    collect(ExitObservation::exited());
                }
                returned = true;
            },
        );
        (events, returned && !waiting)
    }
    fn advance_exit(&mut self, exiting: bool, failures: &RuntimeErrorState) -> ExitObservations {
        let (mut events, mut owner_exited) = self.observe_owner(failures);
        if exiting && owner_exited && !self.close_started {
            self.close_started = true;
            let operator = &mut self.operator;
            let result = self
                .exit_scope
                .run(
                    ExitAction::Close,
                    failures,
                    || fault(FanoutFailure::CleanupPanic("close")),
                    || operator.close(),
                )
                .into_cleanup_result();
            if let Err(cause) = result {
                failures.report_cleanup(&cause);
            }
            let (next, done) = self.observe_owner(failures);
            events.extend(next);
            owner_exited = done;
        }
        if exiting
            && owner_exited
            && self.close_started
            && !self.exit_scope.is_unresolved()
            && self.operator.is_live()
        {
            let operator = &mut self.operator;
            let result = self
                .exit_scope
                .run(
                    ExitAction::Dispose,
                    failures,
                    || fault(FanoutFailure::CleanupPanic("dispose")),
                    || {
                        operator.dispose();
                        Ok(())
                    },
                )
                .into_cleanup_result();
            if let Err(cause) = result {
                failures.report_cleanup(&cause);
            }
            let (next, _) = self.observe_owner(failures);
            events.extend(next);
        }
        events
    }
}
#[derive(Debug)]
enum FanoutFailure {
    EmptyBranches,
    NotPrepared,
    InputClosed,
    BranchOutput,
    RequiredBranchWithdrawn,
    InvalidRoute,
    CleanupPanic(&'static str),
    MissingFailurePort,
    FailurePortChanged,
}
impl std::fmt::Display for FanoutFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyBranches => f.write_str("fanout requires at least one sink branch"),
            Self::NotPrepared => f.write_str("fanout work started before preparation"),
            Self::InputClosed => f.write_str("fanout received input after its demand closed"),
            Self::BranchOutput => f.write_str("fanout sink branch produced an output payload"),
            Self::RequiredBranchWithdrawn => {
                f.write_str("required fanout branch withdrew before its input exhausted")
            }
            Self::InvalidRoute => {
                f.write_str("fanout router reused or exceeded its branch input slot")
            }
            Self::CleanupPanic(hook) => write!(f, "fanout branch {hook} hook panicked"),
            Self::MissingFailurePort => f.write_str("fanout has no bound failure port"),
            Self::FailurePortChanged => f.write_str("fanout failure port identity changed"),
        }
    }
}
impl std::error::Error for FanoutFailure {}
impl novarocks_failure_contract::FailureProjection for FanoutFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let (class, code) = match self {
            Self::EmptyBranches => (
                FailureClass::Invariant,
                ExecutionReason::FanoutEmptyBranches,
            ),
            Self::NotPrepared => (FailureClass::Invariant, ExecutionReason::FanoutNotPrepared),
            Self::InputClosed => (FailureClass::Invariant, ExecutionReason::FanoutInputClosed),
            Self::BranchOutput => (FailureClass::Invariant, ExecutionReason::FanoutBranchOutput),
            Self::InvalidRoute => (FailureClass::Invariant, ExecutionReason::FanoutInvalidRoute),
            Self::RequiredBranchWithdrawn => (
                FailureClass::Invariant,
                ExecutionReason::FanoutRequiredBranchWithdrawn,
            ),
            Self::MissingFailurePort => (
                FailureClass::Invariant,
                ExecutionReason::FanoutMissingFailurePort,
            ),
            Self::FailurePortChanged => (
                FailureClass::Invariant,
                ExecutionReason::FanoutFailurePortChanged,
            ),
            Self::CleanupPanic(_) => (
                FailureClass::Unexpected,
                ExecutionReason::FanoutCleanupPanicked,
            ),
        };
        FailureFact::new(class, code.reason())
    }
}
fn fault(reason: FanoutFailure) -> ExecutionFailure {
    ExecutionFailure::from_domain(reason)
}
fn subscribe(source: &Arc<Observable>, target: &Arc<Observable>) -> ObserverSubscription {
    let target = Arc::downgrade(target);
    source.subscribe(Arc::new(move || {
        if let Some(target) = target.upgrade() {
            target.notify_observers();
        }
    }))
}

pub(crate) struct Fanout<R: FanoutRouter> {
    receiver: ReceiverSlot<FanoutPreparation>,
    cursor: Option<R::Cursor>,
    branches: Vec<Branch>,
    router: R,
    name: String,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    control: Option<Arc<RuntimeErrorState>>,
    observable: Arc<Observable>,
    controls: Vec<(Arc<Observable>, ObserverSubscription)>,
    next: NextStep,
    failure: Option<ExecutionFailure>,
    prepared: bool,
    finishing: bool,
    exit_started: bool,
    round_robin: usize,
    pending_rows: Option<u64>,
    receiver_exit: OperatorExitScope,
    cursor_exit: OperatorExitScope,
}
impl<R: FanoutRouter> Fanout<R> {
    pub(crate) fn new(
        name: String,
        branches: Vec<Box<dyn Operator>>,
        router: R,
        failure: Option<ExecutionFailure>,
    ) -> Self {
        let failure = failure.or_else(|| {
            branches
                .is_empty()
                .then(|| fault(FanoutFailure::EmptyBranches))
        });
        Self {
            receiver: ReceiverSlot::new(FanoutPreparation::default()),
            cursor: None,
            branches: branches
                .into_iter()
                .map(|operator| {
                    let next = operator.initial_step();
                    Branch {
                        pending: None,
                        operator: OwnedOperator::new(operator),
                        next,
                        input_closed: false,
                        admission: None,
                        control_event: None,
                        exit_scope: OperatorExitScope::new(),
                        close_started: false,
                        wait: None,
                    }
                })
                .collect(),
            router,
            name,
            construction: None,
            control: None,
            observable: Arc::new(Observable::new()),
            controls: Vec::new(),
            next: NextStep::Runnable,
            failure,
            prepared: false,
            finishing: false,
            exit_started: false,
            round_robin: 0,
            pending_rows: None,
            receiver_exit: OperatorExitScope::new(),
            cursor_exit: OperatorExitScope::new(),
        }
    }
    fn fail(&mut self, ctx: &DriverStep<'_>, failure: ExecutionFailure) -> ExecutionFailure {
        ctx.report_failure(failure.clone());
        self.failure.get_or_insert(failure).clone()
    }
    fn discard_parent(&mut self) {
        let Some(control) = self.control.clone() else {
            // The bound outer scope keeps this original owner unresolved.
            // No foreign cleanup is attempted without its original authority.
            std::panic::panic_any(FanoutFailure::MissingFailurePort);
        };
        let result = self
            .receiver_exit
            .run(
                ExitAction::ReleaseInput,
                &control,
                || fault(FanoutFailure::CleanupPanic("release-parent-input")),
                || {
                    self.receiver.discard();
                    Ok(())
                },
            )
            .into_cleanup_result();
        if let Err(error) = result {
            control.report_cleanup(&error);
        }
        let result = self
            .cursor_exit
            .run(
                ExitAction::ReleaseInput,
                &control,
                || fault(FanoutFailure::CleanupPanic("release-router-cursor")),
                || {
                    drop(self.cursor.take());
                    Ok(())
                },
            )
            .into_cleanup_result();
        if let Err(error) = result {
            control.report_cleanup(&error);
        }
        for branch in &mut self.branches {
            let result = branch
                .exit_scope
                .run(
                    ExitAction::ReleaseInput,
                    &control,
                    || fault(FanoutFailure::CleanupPanic("release-branch-input")),
                    || {
                        drop(branch.pending.take());
                        Ok(())
                    },
                )
                .into_cleanup_result();
            if let Err(error) = result {
                control.report_cleanup(&error);
            }
        }
        self.pending_rows = None;
    }
    fn record_branch(
        &mut self,
        index: usize,
        result: StepResult,
        ctx: &DriverStep<'_>,
    ) -> ExecutionResult<()> {
        if result.output.is_some() {
            let failure = fault(FanoutFailure::BranchOutput);
            ctx.report_failure(failure.clone());
            return Err(failure);
        }
        let branch = &mut self.branches[index];
        match &result.next {
            NextStep::Wait(wait) => {
                if branch
                    .control_event
                    .as_ref()
                    .is_some_and(|event| Arc::ptr_eq(event, &wait.observable))
                {
                    drop(branch.wait.take());
                } else if branch
                    .wait
                    .as_ref()
                    .is_none_or(|(old, _)| !Arc::ptr_eq(old, &wait.observable))
                {
                    drop(branch.wait.take());
                    branch.wait = Some((
                        wait.observable.clone(),
                        subscribe(&wait.observable, &self.observable),
                    ));
                }
            }
            _ => {
                drop(branch.wait.take());
            }
        }
        if matches!(result.next, NextStep::Finished)
            && !branch.input_closed
            && self.router.requires_branch_drain(index)
        {
            return Err(fault(FanoutFailure::RequiredBranchWithdrawn));
        }
        branch.next = result.next;
        Ok(())
    }
    fn runnable_child(&self) -> Option<usize> {
        (0..self.branches.len())
            .map(|offset| (self.round_robin + offset) % self.branches.len())
            .find(|index| {
                let branch = &self.branches[*index];
                match &branch.next {
                    NextStep::Runnable | NextStep::Yield => true,
                    NextStep::Wait(wait) => wait.is_ready(),
                    NextStep::NeedInput => {
                        branch.pending.is_some()
                            && match &branch.admission {
                                Some(AdmissionNext::Wait(wait)) => wait.is_ready(),
                                _ => true,
                            }
                    }
                    NextStep::Finished => false,
                }
            })
    }
    fn close_disabled_inputs(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<bool> {
        // A shared zero cannot overtake the last prefix claimed by this driver.
        if self.receiver.is_occupied()
            || self.cursor.is_some()
            || self.branches.iter().any(|branch| branch.pending.is_some())
        {
            return Ok(false);
        }
        if self.finishing {
            self.router.input_exhausted(ctx)?;
        }
        let mut changed = false;
        for (index, branch) in self.branches.iter_mut().enumerate() {
            if !branch.input_closed && (self.finishing || !self.router.accepts_branch(index)) {
                branch.input_closed = true;
                if !matches!(branch.next, NextStep::Finished) {
                    ctx.perform(crate::runtime::task::PipelineOperation::Finishing, |ctx| {
                        branch.operator.as_mut().finish(ctx)
                    })?;
                    branch.next = NextStep::Runnable;
                    changed = true;
                }
            }
        }
        Ok(changed)
    }
    fn result(&mut self, observed_generation: u64) -> StepResult {
        let routing = self.receiver.is_occupied() || self.cursor.is_some();
        let pending = self.branches.iter().any(|branch| branch.pending.is_some());
        let next = if routing || self.runnable_child().is_some() {
            NextStep::Runnable
        } else if self
            .branches
            .iter()
            .all(|branch| matches!(branch.next, NextStep::Finished))
        {
            NextStep::Finished
        } else if !self.finishing
            && !pending
            && self.branches.iter().enumerate().all(|(index, branch)| {
                !self.router.accepts_branch(index)
                    || matches!(branch.next, NextStep::NeedInput | NextStep::Finished)
            })
        {
            NextStep::NeedInput
        } else {
            let deadline = self
                .branches
                .iter()
                .filter_map(|branch| match &branch.next {
                    NextStep::Wait(wait) => wait.deadline,
                    _ => None,
                })
                .chain(
                    self.branches
                        .iter()
                        .filter_map(|branch| match &branch.admission {
                            Some(AdmissionNext::Wait(wait)) => wait.deadline,
                            _ => None,
                        }),
                )
                .min_by_key(|deadline| deadline.at());
            NextStep::Wait(WaitFact {
                reason: if self.finishing {
                    WaitReason::Finish
                } else {
                    WaitReason::Output
                },
                observable: self.observable.clone(),
                observed_generation,
                deadline,
            })
        };
        let rows = if !routing && !pending {
            self.pending_rows.take().unwrap_or(0)
        } else {
            0
        };
        self.next = next.clone();
        StepResult::new(None, next, rows)
    }
    fn drive_once(
        &mut self,
        ctx: &mut DriverStep<'_>,
        generation: u64,
    ) -> ExecutionResult<StepResult> {
        if ctx.work().remaining() == 0 {
            self.next = NextStep::Yield;
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        if self.close_disabled_inputs(ctx)? {
            return Ok(self.result(generation));
        }
        if let Some(index) = self.runnable_child() {
            let before = ctx.work().remaining();
            self.round_robin = (index + 1) % self.branches.len();
            let branch = &mut self.branches[index];
            if matches!(branch.next, NextStep::NeedInput) {
                let raw = branch.pending.take().expect("pending route selected");
                branch.admission = None;
                let admission =
                    ctx.perform(crate::runtime::task::PipelineOperation::Admission, |ctx| {
                        Ok(match branch.operator.as_mut().try_push(ctx, raw) {
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
                match admission {
                    InputResult::Accepted(result) => self.record_branch(index, result?, ctx)?,
                    InputResult::Deferred { input, next } => {
                        branch.pending = Some(input);
                        match next {
                            AdmissionNext::ResumeExisting => {
                                branch.next = NextStep::Runnable;
                                branch.admission = Some(AdmissionNext::ResumeExisting);
                            }
                            AdmissionNext::Yield => {
                                branch.admission = Some(AdmissionNext::Yield);
                                self.next = NextStep::Yield;
                                return Ok(StepResult::new(None, NextStep::Yield, 0));
                            }
                            AdmissionNext::Wait(wait) => {
                                if branch
                                    .control_event
                                    .as_ref()
                                    .is_none_or(|event| !Arc::ptr_eq(event, &wait.observable))
                                    && branch
                                        .wait
                                        .as_ref()
                                        .is_none_or(|(old, _)| !Arc::ptr_eq(old, &wait.observable))
                                {
                                    drop(branch.wait.take());
                                    branch.wait = Some((
                                        wait.observable.clone(),
                                        subscribe(&wait.observable, &self.observable),
                                    ));
                                }
                                branch.admission = Some(AdmissionNext::Wait(wait));
                            }
                        }
                    }
                    InputResult::Rejected { input, failure } => {
                        ctx.report_failure(failure.clone());
                        branch.pending = Some(input);
                        return Err(failure);
                    }
                }
            } else {
                let result = branch.operator.as_mut().drive(ctx)?;
                self.record_branch(index, result, ctx)?;
            }
            if ctx.work().remaining() == before {
                ctx.work().try_consume(1);
            }
            return Ok(self.result(generation));
        }
        if !ctx.work().try_consume(1) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        if self.receiver.is_occupied() {
            let preparation = ctx.perform(
                crate::runtime::task::PipelineOperation::Preparation,
                |ctx| self.receiver.prepare(ctx, &self.router),
            )?;
            match preparation {
                ReceiverPreparation::Ready(input) => {
                    self.cursor = Some(
                        self.router.begin(
                            input,
                            self.construction
                                .as_deref()
                                .ok_or(ConstructionFailure::MissingOwner)?,
                            ctx,
                        )?,
                    );
                }
                ReceiverPreparation::Pending(next) => {
                    self.next = next.clone();
                    return Ok(StepResult::new(None, next, 0));
                }
                ReceiverPreparation::Empty => {}
            }
            return Ok(self.result(generation));
        }
        if let Some(cursor) = &mut self.cursor {
            let route = self.router.advance(
                cursor,
                self.construction
                    .as_deref()
                    .ok_or(ConstructionFailure::MissingOwner)?,
                ctx,
            )?;
            match route {
                RouteStep::Input { branch, chunk } => {
                    let Some(child) = self.branches.get_mut(branch) else {
                        let error = fault(FanoutFailure::InvalidRoute);
                        ctx.report_failure(error.clone());
                        return Err(error);
                    };
                    if child.pending.is_some() {
                        let error = fault(FanoutFailure::InvalidRoute);
                        ctx.report_failure(error.clone());
                        return Err(error);
                    }
                    if matches!(child.next, NextStep::Finished)
                        && self.router.requires_branch_drain(branch)
                    {
                        return Err(fault(FanoutFailure::RequiredBranchWithdrawn));
                    }
                    if !matches!(child.next, NextStep::Finished) {
                        child.pending = Some(super::super::input::derived_raw_input(chunk));
                    }
                }
                RouteStep::Finished => {
                    drop(self.cursor.take());
                }
                RouteStep::Skip => {}
            }
            return Ok(self.result(generation));
        }
        Ok(self.result(generation))
    }
}
impl<R: FanoutRouter> StepOperator for Fanout<R> {
    fn initial_step(&self) -> NextStep {
        self.next.clone()
    }
    fn control_observable(&self) -> Option<Arc<Observable>> {
        Some(self.observable.clone())
    }
    fn poll_control(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<ControlStep> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        if !self.prepared {
            return Err(self.fail(ctx, fault(FanoutFailure::NotPrepared)));
        }
        let result = (|| {
            let mut changed = false;
            for (index, branch) in self.branches.iter_mut().enumerate() {
                if matches!(branch.next, NextStep::Finished) {
                    continue;
                }
                match branch.operator.as_mut().poll_control(ctx)? {
                    ControlStep::Active => {}
                    ControlStep::Runnable => {
                        branch.next = NextStep::Runnable;
                        changed = true;
                    }
                    ControlStep::Withdrawn => {
                        if !branch.input_closed && self.router.requires_branch_drain(index) {
                            return Err(fault(FanoutFailure::RequiredBranchWithdrawn));
                        }
                        branch.next = NextStep::Finished;
                        drop(branch.pending.take());
                        changed = true;
                    }
                }
            }
            changed |= self.close_disabled_inputs(ctx)?;
            if self
                .branches
                .iter()
                .all(|branch| matches!(branch.next, NextStep::Finished))
            {
                self.discard_parent();
                self.next = NextStep::Finished;
                return Ok(ControlStep::Withdrawn);
            }
            if changed {
                self.next = NextStep::Runnable;
                Ok(ControlStep::Runnable)
            } else {
                Ok(ControlStep::Active)
            }
        })();
        result.map_err(|failure| self.fail(ctx, failure))
    }
    fn try_push(&mut self, ctx: &mut DriverStep<'_>, input: RawInput) -> InputResult {
        if let Some(failure) = self.failure.clone().or_else(|| ctx.checkpoint().err()) {
            return InputResult::Rejected { input, failure };
        }
        if !self.prepared {
            let failure = self.fail(ctx, fault(FanoutFailure::NotPrepared));
            return InputResult::Rejected { input, failure };
        }
        if self.finishing || matches!(self.next, NextStep::Finished) {
            return InputResult::Rejected {
                input,
                failure: fault(FanoutFailure::InputClosed),
            };
        }
        if let NextStep::Wait(wait) = &self.next {
            return InputResult::Deferred {
                input,
                next: AdmissionNext::Wait(wait.clone()),
            };
        }
        if matches!(self.next, NextStep::Yield) {
            return InputResult::Deferred {
                input,
                next: AdmissionNext::Yield,
            };
        }
        if !matches!(self.next, NextStep::NeedInput) {
            return InputResult::Deferred {
                input,
                next: AdmissionNext::ResumeExisting,
            };
        }
        if self.branches.iter().enumerate().all(|(index, branch)| {
            !self.router.accepts_branch(index)
                || branch.input_closed
                || matches!(branch.next, NextStep::Finished)
        }) {
            self.next = NextStep::Runnable;
            return InputResult::Deferred {
                input,
                next: AdmissionNext::ResumeExisting,
            };
        }
        let generation = self.observable.generation();
        if self.branches.iter().enumerate().any(|(index, branch)| {
            self.router.accepts_branch(index)
                && !matches!(branch.next, NextStep::NeedInput | NextStep::Finished)
        }) {
            return InputResult::Deferred {
                input,
                next: AdmissionNext::Wait(WaitFact {
                    reason: WaitReason::Output,
                    observable: self.observable.clone(),
                    observed_generation: generation,
                    deadline: None,
                }),
            };
        }
        let rows = input.rows() as u64;
        let result = self.receiver.try_accept(ctx, input, &self.router);
        if matches!(&result, InputResult::Accepted(_)) {
            self.pending_rows = Some(rows);
            self.next = NextStep::Runnable;
        }
        result
    }
    fn drive(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        ctx.checkpoint()?;
        if !self.prepared {
            return Err(self.fail(ctx, fault(FanoutFailure::NotPrepared)));
        }
        if matches!(self.next, NextStep::Finished) {
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        }
        let generation = self.observable.generation();
        self.drive_once(ctx, generation)
            .map_err(|failure| self.fail(ctx, failure))
    }
    fn finish(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<()> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        ctx.checkpoint()?;
        if !self.prepared {
            return Err(self.fail(ctx, fault(FanoutFailure::NotPrepared)));
        }
        self.finishing = true;
        self.next = NextStep::Runnable;
        Ok(())
    }
}
impl<R: FanoutRouter> OperatorLifecycle for Fanout<R> {
    fn name(&self) -> &str {
        &self.name
    }

    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.receiver
            .preparation_mut()
            .0
            .install_tracker(tracker.clone());
        for branch in &mut self.branches {
            branch.operator.set_mem_tracker(tracker.clone());
        }
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.receiver
            .preparation_mut()
            .0
            .install_factory(factory.clone());
        for branch in &mut self.branches {
            branch.operator.set_construction_factory(factory.clone());
        }
        self.construction = Some(factory);
    }
    fn set_profiles(&mut self, profiles: OperatorProfiles) {
        for (index, branch) in self.branches.iter_mut().enumerate() {
            branch.operator.set_profiles(OperatorProfiles::new(
                profiles.operator.child(format!("Branch{index}")),
            ));
        }
    }
    fn set_task_event_sink(&mut self, sink: Arc<dyn TaskEventSink>) {
        for branch in &mut self.branches {
            branch.operator.set_task_event_sink(sink.clone());
        }
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        let incoming = state.error_state();
        if self
            .control
            .as_ref()
            .is_some_and(|old| !Arc::ptr_eq(old, &incoming))
        {
            return Err(fault(FanoutFailure::FailurePortChanged));
        }
        self.control = Some(incoming);
        for branch in &mut self.branches {
            branch.operator.bind_runtime_state(state)?;
        }
        Ok(())
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if self.prepared {
            return Ok(());
        }
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        self.construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        for (index, branch) in self.branches.iter_mut().enumerate() {
            branch.operator.prepare()?;
            branch.operator.install_control();
            branch.next = branch.operator.initial_step();
            if matches!(branch.next, NextStep::Finished) && self.router.requires_branch_drain(index)
            {
                return Err(fault(FanoutFailure::RequiredBranchWithdrawn));
            }
            if let Some(event) = branch.operator.control_observable() {
                if !self
                    .controls
                    .iter()
                    .any(|(old, _)| Arc::ptr_eq(old, &event))
                {
                    let subscription = subscribe(&event, &self.observable);
                    self.controls.push((event.clone(), subscription));
                }
                branch.control_event = Some(event);
            }
        }
        self.router.visit_demand_events(&mut |event| {
            if !self
                .controls
                .iter()
                .any(|(old, _)| Arc::ptr_eq(old, &event))
            {
                let subscription = subscribe(&event, &self.observable);
                self.controls.push((event, subscription));
            }
        });
        self.prepared = true;
        Ok(())
    }
    fn activate(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        if !self.prepared {
            return Err(fault(FanoutFailure::NotPrepared));
        }
        for branch in &mut self.branches {
            branch.operator.activate(state)?;
        }
        Ok(())
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        let control = self
            .control
            .clone()
            .ok_or_else(|| fault(FanoutFailure::MissingFailurePort))?;
        self.next = NextStep::Finished;
        self.finishing = true;
        self.exit_started = true;
        self.discard_parent();
        for branch in &mut self.branches {
            branch.next = NextStep::Finished;
            if !branch.operator.is_live() {
                continue;
            }
            let result = branch
                .exit_scope
                .run(
                    ExitAction::Begin,
                    &control,
                    || fault(FanoutFailure::CleanupPanic("begin-exit")),
                    || branch.operator.begin_exit(),
                )
                .into_cleanup_result();
            if let Err(error) = result {
                control.report_cleanup(&error);
            }
        }
        Ok(())
    }
    fn visit_exits(&mut self, visit: &mut dyn FnMut(ExitObservation)) {
        let Some(control) = self.control.clone() else {
            std::panic::panic_any(FanoutFailure::MissingFailurePort);
        };
        for scope in [&mut self.receiver_exit, &mut self.cursor_exit] {
            let observations = scope.observe(
                &control,
                || fault(FanoutFailure::CleanupPanic("observe-parent-input")),
                |collect| collect(ExitObservation::exited()),
            );
            for observation in observations {
                visit(observation);
            }
        }
        for branch in &mut self.branches {
            for observation in branch.advance_exit(self.exit_started, &control) {
                visit(observation);
            }
        }
    }

    fn close(&mut self) -> ExecutionResult<()> {
        let control = self
            .control
            .clone()
            .ok_or_else(|| fault(FanoutFailure::MissingFailurePort))?;
        self.discard_parent();
        for branch in &mut self.branches {
            if branch.close_started || !branch.operator.is_live() {
                continue;
            }
            branch.close_started = true;
            let result = branch
                .exit_scope
                .run(
                    ExitAction::Close,
                    &control,
                    || fault(FanoutFailure::CleanupPanic("close")),
                    || branch.operator.close(),
                )
                .into_cleanup_result();
            if let Err(error) = result {
                control.report_cleanup(&error);
            }
        }
        Ok(())
    }
    fn cancel(&mut self) {
        let Some(control) = self.control.clone() else {
            std::panic::panic_any(FanoutFailure::MissingFailurePort);
        };
        self.next = NextStep::Finished;
        self.finishing = true;
        self.exit_started = true;
        self.discard_parent();
        for branch in &mut self.branches {
            branch.next = NextStep::Finished;
            if !branch.operator.is_live() {
                continue;
            }
            let result = branch
                .exit_scope
                .run(
                    ExitAction::Cancel,
                    &control,
                    || fault(FanoutFailure::CleanupPanic("cancel")),
                    || {
                        branch.operator.cancel();
                        Ok(())
                    },
                )
                .into_cleanup_result();
            if let Err(error) = result {
                control.report_cleanup(&error);
            }
        }
    }
    fn on_driver_failure(&mut self) {
        let Some(control) = self.control.clone() else {
            std::panic::panic_any(FanoutFailure::MissingFailurePort);
        };
        self.next = NextStep::Finished;
        self.finishing = true;
        self.exit_started = true;
        self.discard_parent();
        for branch in &mut self.branches {
            branch.next = NextStep::Finished;
            if !branch.operator.is_live() {
                continue;
            }
            let result = branch
                .exit_scope
                .run(
                    ExitAction::FailureStop,
                    &control,
                    || fault(FanoutFailure::CleanupPanic("failure-stop")),
                    || {
                        branch.operator.on_driver_failure();
                        Ok(())
                    },
                )
                .into_cleanup_result();
            if let Err(error) = result {
                control.report_cleanup(&error);
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};

    use arrow::{
        array::{ArrayRef, Int32Array},
        datatypes::{DataType, Field},
    };
    use novarocks_types::SlotId;
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    struct CopyRouter(usize);
    impl InputEncodingPolicy for CopyRouter {}
    impl FanoutRouter for CopyRouter {
        type Cursor = (Chunk, usize);
        fn begin(
            &mut self,
            input: PreparedInput,
            _: &dyn InputConstructionFactory,
            _: &mut DriverStep<'_>,
        ) -> ExecutionResult<Self::Cursor> {
            Ok((input.into_uncovered_chunk()?, 0))
        }
        fn advance(
            &mut self,
            cursor: &mut Self::Cursor,
            _: &dyn InputConstructionFactory,
            _: &mut DriverStep<'_>,
        ) -> ExecutionResult<RouteStep> {
            if cursor.1 == self.0 {
                return Ok(RouteStep::Finished);
            }
            let branch = cursor.1;
            cursor.1 += 1;
            Ok(RouteStep::Input {
                branch,
                chunk: cursor.0.clone(),
            })
        }
        fn accepts_branch(&self, _: usize) -> bool {
            true
        }
    }
    pub(crate) struct Probe {
        pub(crate) ready: AtomicBool,
        pub(crate) event: Arc<Observable>,
        pub(crate) tries: AtomicUsize,
        pub(crate) drives: AtomicUsize,
        pub(crate) finishes: AtomicUsize,
        pub(crate) rows: Mutex<Vec<i32>>,
        pub(crate) finish_ready: AtomicBool,
        pub(crate) seals: AtomicUsize,
        pub(crate) binds: AtomicUsize,
        value_slot: novarocks_types::SlotId,
        first_raw: Mutex<Option<std::sync::Weak<dyn arrow::array::Array>>>,
    }
    impl Probe {
        pub(crate) fn new(ready: bool) -> Arc<Self> {
            Self::with_slot(ready, SlotId::new(1))
        }
        pub(crate) fn with_slot(ready: bool, value_slot: SlotId) -> Arc<Self> {
            Arc::new(Self {
                ready: AtomicBool::new(ready),
                event: Arc::new(Observable::try_bounded(1).unwrap()),
                tries: AtomicUsize::new(0),
                drives: AtomicUsize::new(0),
                finishes: AtomicUsize::new(0),
                rows: Mutex::new(vec![]),
                first_raw: Mutex::new(None),
                finish_ready: AtomicBool::new(true),
                seals: AtomicUsize::new(0),
                binds: AtomicUsize::new(0),
                value_slot,
            })
        }
    }
    pub(crate) struct Sink {
        receiver: ReceiverSlot<FanoutPreparation>,
        probe: Arc<Probe>,
        finishing: bool,
    }
    pub(crate) fn sink(probe: Arc<Probe>) -> Box<dyn Operator> {
        Box::new(Sink {
            receiver: ReceiverSlot::new(FanoutPreparation::default()),
            probe,
            finishing: false,
        })
    }
    impl OperatorLifecycle for Sink {
        fn bind_runtime_state(&mut self, _: &RuntimeState) -> ExecutionResult<()> {
            self.probe.binds.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
        fn name(&self) -> &str {
            "FANOUT_PROBE"
        }

        fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
            self.receiver.preparation_mut().0.install_tracker(tracker);
        }
        fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
            self.receiver.preparation_mut().0.install_factory(factory);
        }
    }
    impl StepOperator for Sink {
        fn initial_step(&self) -> NextStep {
            NextStep::NeedInput
        }
        fn control_observable(&self) -> Option<Arc<Observable>> {
            Some(self.probe.event.clone())
        }
        fn try_push(&mut self, ctx: &mut DriverStep<'_>, input: RawInput) -> InputResult {
            let attempt = self.probe.tries.fetch_add(1, Ordering::AcqRel);
            let mut first = self.probe.first_raw.lock().unwrap();
            if attempt == 0 {
                *first = Some(Arc::downgrade(&input.original_for_test().columns()[0]));
            } else {
                assert!(
                    Arc::ptr_eq(
                        &first
                            .as_ref()
                            .unwrap()
                            .upgrade()
                            .expect("refused raw remains owned until acceptance"),
                        &input.original_for_test().columns()[0]
                    ),
                    "retry keeps exactly the first branch raw value"
                );
            }
            drop(first);
            let generation = self.probe.event.generation();
            if !self.probe.ready.load(Ordering::Acquire) {
                return InputResult::Deferred {
                    input,
                    next: AdmissionNext::Wait(WaitFact {
                        reason: WaitReason::Output,
                        observable: self.probe.event.clone(),
                        observed_generation: generation,
                        deadline: None,
                    }),
                };
            }
            self.receiver.try_accept(ctx, input, &CopyRouter(0))
        }
        fn drive(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
            self.probe.drives.fetch_add(1, Ordering::AcqRel);
            assert!(
                self.receiver.is_occupied() || self.finishing,
                "admission wake must retry raw input, never invent accepted child work"
            );
            if let ReceiverPreparation::Ready(input) = self.receiver.prepare(ctx, &CopyRouter(0))? {
                let index = *input
                    .chunk()
                    .slot_id_to_index()
                    .get(&self.probe.value_slot)
                    .unwrap();
                let array = input.chunk().columns()[index]
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                self.probe
                    .rows
                    .lock()
                    .unwrap()
                    .extend(array.values().iter().copied());
                return Ok(StepResult::new(
                    None,
                    if self.finishing {
                        NextStep::Runnable
                    } else {
                        NextStep::NeedInput
                    },
                    input.chunk().len() as u64,
                ));
            }
            if !self.probe.finish_ready.load(Ordering::Acquire) {
                return Ok(StepResult::new(
                    None,
                    NextStep::Wait(WaitFact {
                        reason: WaitReason::Finish,
                        observable: self.probe.event.clone(),
                        observed_generation: self.probe.event.generation(),
                        deadline: None,
                    }),
                    0,
                ));
            }
            assert_eq!(
                self.probe.seals.fetch_add(1, Ordering::AcqRel),
                0,
                "child seals once"
            );
            Ok(StepResult::new(None, NextStep::Finished, 0))
        }
        fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<()> {
            assert!(!self.finishing);
            self.finishing = true;
            self.probe.finishes.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }
    pub(crate) fn context<T>(action: impl FnOnce(&mut DriverStep<'_>) -> T) -> T {
        let control = RuntimeErrorState::default();
        let mut turn =
            crate::exec::pipeline::driver_step::TurnWork::new(std::num::NonZeroU64::MIN, 64);
        action(&mut DriverStep::new(
            &control,
            &mut turn,
            &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
        ))
    }
    pub(crate) fn prepare<R: FanoutRouter>(fanout: &mut Fanout<R>) -> Arc<MemTracker> {
        prepare_on(fanout, &RuntimeState::default())
    }
    pub(crate) fn prepare_on<R: FanoutRouter>(
        fanout: &mut Fanout<R>,
        runtime: &RuntimeState,
    ) -> Arc<MemTracker> {
        let tracker = MemTracker::new_root("fanout route fixture");
        let factory=crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),tracker.clone(),runtime.error_state()).unwrap();
        fanout.set_mem_tracker(tracker.clone());
        fanout.set_construction_factory(factory);
        fanout.bind_runtime_state(runtime).unwrap();
        fanout.prepare().unwrap();
        tracker
    }
    pub(crate) fn typed_chunk(columns: Vec<(SlotId, &str, DataType, bool, ArrayRef)>) -> Chunk {
        use novarocks_type_contract::{
            TypedValueBinding, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
        };
        let storage = ValueTypeStorageLifetime::pure();
        let mut slots = Vec::new();
        let mut arrays = Vec::new();
        for (slot, name, declared, nullable, array) in columns {
            // The fixture author supplies every logical declaration explicitly.
            let value = ValueTypeRef::try_new(
                ValueKind::Scalar(declared.clone()),
                nullable,
                storage.clone(),
            )
            .unwrap();
            let binding = TypedValueBinding::try_new(
                value,
                ValueLayout::try_new(&declared, storage.clone()).unwrap(),
            )
            .unwrap();
            slots.push(
                ChunkSlotSchema::try_new_typed(
                    slot,
                    Arc::new(Field::new(name, declared, nullable)),
                    binding,
                    None,
                    storage.clone(),
                )
                .unwrap(),
            );
            arrays.push(array);
        }
        let rows = arrays.first().unwrap().len();
        let schema = Arc::new(ChunkSchema::try_new_typed(slots, storage).unwrap());
        Chunk::try_new_typed_with_columns_observed::<crate::runtime::task::ExecutionFailure, _>(
            schema,
            arrays,
            rows,
            |_| Ok(()),
        )
        .unwrap()
    }
    pub(crate) fn typed_i32(rows: Vec<i32>) -> Chunk {
        typed_chunk(vec![(
            SlotId::new(1),
            "value",
            DataType::Int32,
            false,
            Arc::new(Int32Array::from(rows)),
        )])
    }
    pub(crate) fn until_wait_or_input<R: FanoutRouter>(fanout: &mut Fanout<R>) -> NextStep {
        for _ in 0..64 {
            let next = context(|ctx| fanout.drive(ctx)).unwrap().next;
            match next {
                NextStep::Runnable | NextStep::Yield => {}
                next => return next,
            }
        }
        panic!("fanout did not reach its next dependency")
    }
    #[test]
    fn finishing_branches_keep_one_wake_identity_and_advance_independently() {
        let first = Probe::new(true);
        let second = Probe::new(true);
        second.finish_ready.store(false, Ordering::Release);
        let mut fanout = Fanout::new(
            "finish fixture".into(),
            vec![sink(first.clone()), sink(second.clone())],
            CopyRouter(2),
            None,
        );
        prepare(&mut fanout);
        assert_eq!(first.binds.load(Ordering::Acquire), 1);
        assert_eq!(second.binds.load(Ordering::Acquire), 1);
        let event = fanout.control_observable().unwrap();
        context(|ctx| fanout.finish(ctx)).unwrap();
        let next = until_wait_or_input(&mut fanout);
        assert!(matches!(next, NextStep::Wait(_)));
        assert_eq!(first.seals.load(Ordering::Acquire), 1);
        assert_eq!(second.seals.load(Ordering::Acquire), 0);
        assert_eq!(first.finishes.load(Ordering::Acquire), 1);
        assert_eq!(second.finishes.load(Ordering::Acquire), 1);
        let generation = event.generation();
        second.finish_ready.store(true, Ordering::Release);
        second.event.notify_observers();
        assert!(Arc::ptr_eq(&event, &fanout.control_observable().unwrap()));
        assert!(event.generation() > generation);
        assert!(matches!(
            until_wait_or_input(&mut fanout),
            NextStep::Finished
        ));
        assert_eq!(first.seals.load(Ordering::Acquire), 1);
        assert_eq!(second.seals.load(Ordering::Acquire), 1);
    }
    #[test]
    fn pending_admission_keeps_its_raw_while_an_independent_branch_drains() {
        let first = Probe::new(false);
        let second = Probe::new(true);
        let branches = [first.clone(), second.clone()]
            .into_iter()
            .map(|probe| {
                Box::new(Sink {
                    receiver: ReceiverSlot::new(FanoutPreparation::default()),
                    probe,
                    finishing: false,
                }) as Box<dyn Operator>
            })
            .collect();
        let mut fanout = Fanout::new("fanout fixture".into(), branches, CopyRouter(2), None);
        let runtime = RuntimeState::default();
        let tracker = MemTracker::new_root("fanout fixture");
        let factory=crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),tracker.clone(),runtime.error_state()).unwrap();
        fanout.set_mem_tracker(tracker.clone());
        fanout.set_construction_factory(factory);
        fanout.bind_runtime_state(&runtime).unwrap();
        fanout.prepare().unwrap();
        assert!(matches!(
            context(|ctx| fanout.drive(ctx)).unwrap().next,
            NextStep::NeedInput
        ));
        let array: ArrayRef = Arc::new(Int32Array::from(vec![7, 8]));
        let schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("value", DataType::Int32, false),
                None,
                None,
            )])
            .unwrap(),
        );
        let chunk = Chunk::try_new_with_columns(schema, vec![array.clone()]).unwrap();
        assert!(matches!(
            context(|ctx| fanout.try_push(ctx, RawInput::for_test(chunk))),
            InputResult::Accepted(Ok(_))
        ));
        let mut parked = None;
        for _ in 0..16 {
            let result = context(|ctx| fanout.drive(ctx)).unwrap();
            if let NextStep::Wait(wait) = result.next {
                parked = Some(wait);
                break;
            }
        }
        let wait = parked.expect("only the first branch remains blocked");
        assert_eq!(second.rows.lock().unwrap().as_slice(), [7, 8]);
        assert!(first.rows.lock().unwrap().is_empty());
        assert_eq!(first.tries.load(Ordering::Acquire), 1);
        assert_eq!(first.drives.load(Ordering::Acquire), 0);
        let raw = fanout.branches[0].pending.as_ref().unwrap();
        assert!(Arc::ptr_eq(
            &first
                .first_raw
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .upgrade()
                .unwrap(),
            &raw.original_for_test().columns()[0]
        ));
        assert_eq!(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ptr(),
            raw.original_for_test().columns()[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ptr()
        );
        assert!(matches!(
            context(|ctx| fanout.drive(ctx)).unwrap().next,
            NextStep::Wait(_)
        ));
        assert_eq!(first.tries.load(Ordering::Acquire), 1);
        first.ready.store(true, Ordering::Release);
        first.event.notify_observers();
        assert!(wait.is_ready());
        for _ in 0..8 {
            if matches!(
                context(|ctx| fanout.drive(ctx)).unwrap().next,
                NextStep::NeedInput
            ) {
                break;
            }
        }
        assert_eq!(first.rows.lock().unwrap().as_slice(), [7, 8]);
        assert_eq!(second.rows.lock().unwrap().as_slice(), [7, 8]);
        assert_eq!(first.tries.load(Ordering::Acquire), 2);
        assert_eq!(first.drives.load(Ordering::Acquire), 1);
        context(|ctx| fanout.finish(ctx)).unwrap();
        let mut finished = false;
        for _ in 0..8 {
            if matches!(
                context(|ctx| fanout.drive(ctx)).unwrap().next,
                NextStep::Finished
            ) {
                finished = true;
                break;
            }
        }
        assert!(finished);
        assert_eq!(first.finishes.load(Ordering::Acquire), 1);
        assert_eq!(second.finishes.load(Ordering::Acquire), 1);
        drop(fanout);
        assert!(first.event.try_subscribe(Arc::new(|| {})).is_ok());
        assert!(second.event.try_subscribe(Arc::new(|| {})).is_ok());
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn branch_boxes_retire_independently_after_actual_join_and_unknown_keeps_the_parent_owner() {
        use crate::exec::pipeline::{
            driver::{DriverState, PipelineDriver},
            operator::FinishWatch,
            shape::{Source, SourceKernel, SourceStart},
        };
        use crate::runtime::mem_tracker::TrackedBytes;
        use std::{
            mem::ManuallyDrop,
            task::{Context, Poll},
            time::{Duration, Instant},
        };
        struct Data {
            bytes: Vec<u8>,
            _funding: TrackedBytes,
        }
        struct Secondary {
            data: ManuallyDrop<Arc<Data>>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for Secondary {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
                unsafe {
                    ManuallyDrop::drop(&mut self.data);
                }
            }
        }
        struct Primary {
            data: ManuallyDrop<Arc<Data>>,
            primary: Arc<AtomicUsize>,
            secondary: Arc<AtomicUsize>,
        }
        impl Drop for Primary {
            fn drop(&mut self) {
                self.primary.fetch_add(1, Ordering::SeqCst);
                let data = unsafe { ManuallyDrop::take(&mut self.data) };
                std::panic::panic_any(Secondary {
                    data: ManuallyDrop::new(data),
                    drops: self.secondary.clone(),
                });
            }
        }
        struct ExitWake(Arc<Observable>);
        impl std::task::Wake for ExitWake {
            fn wake(self: Arc<Self>) {
                self.0.notify_observers();
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.notify_observers();
            }
        }
        struct Kernel {
            data: ManuallyDrop<Arc<Data>>,
            event: Arc<Observable>,
            runtime: tokio::runtime::Handle,
            gate: Option<tokio::sync::oneshot::Receiver<()>>,
            join: Option<tokio::task::JoinHandle<()>>,
            joined: Arc<AtomicBool>,
            closed: Arc<AtomicUsize>,
            dropped: Arc<AtomicUsize>,
            primary: Arc<AtomicUsize>,
            secondary: Arc<AtomicUsize>,
            nested_unwind: Arc<AtomicBool>,
            fail_drop: bool,
            activated: bool,
            finishing: bool,
        }
        impl Drop for Kernel {
            fn drop(&mut self) {
                self.dropped.fetch_add(1, Ordering::SeqCst);
                if !self.activated {
                    // No job was installed. Preserve the ordinary preparation
                    // body's synchronous teardown if fixture setup itself fails.
                    let data = unsafe { ManuallyDrop::take(&mut self.data) };
                    drop(data);
                    return;
                }
                // Obtain a useful test failure instead of aborting the whole
                // libtest binary if a reverted Vec Drop calls this destructor
                // while a sibling's foreign unwind is still active.
                if std::thread::panicking() {
                    self.nested_unwind.store(true, Ordering::Release);
                    let data = unsafe { ManuallyDrop::take(&mut self.data) };
                    drop(data);
                    return;
                }
                assert!(
                    self.joined.load(Ordering::Acquire),
                    "the real job must be joined before the operator Box is destroyed"
                );
                assert_eq!(self.closed.load(Ordering::SeqCst), 1);
                let data = unsafe { ManuallyDrop::take(&mut self.data) };
                if self.fail_drop {
                    std::panic::panic_any(Primary {
                        data: ManuallyDrop::new(data),
                        primary: self.primary.clone(),
                        secondary: self.secondary.clone(),
                    });
                }
                drop(data);
            }
        }
        impl InputEncodingPolicy for Kernel {}
        impl OperatorLifecycle for Kernel {
            fn name(&self) -> &str {
                "actual independently owned branch"
            }
            fn activate(&mut self, _: &RuntimeState) -> ExecutionResult<()> {
                self.activated = true;
                if let Some(gate) = self.gate.take() {
                    let data = Arc::clone(&self.data);
                    let event = self.event.clone();
                    self.join = Some(self.runtime.spawn(async move {
                        let _ = gate.await;
                        assert_eq!(data.bytes, [5, 6, 7]);
                        drop(data);
                        event.notify_observers();
                    }));
                } else {
                    self.joined.store(true, Ordering::Release);
                }
                Ok(())
            }
            fn close(&mut self) -> ExecutionResult<()> {
                assert!(self.joined.load(Ordering::Acquire));
                assert_eq!(self.closed.fetch_add(1, Ordering::SeqCst), 0);
                Ok(())
            }
            fn observe_exit(&mut self) -> ExitObservation {
                use std::future::Future;
                let Some(join) = self.join.as_mut() else {
                    return ExitObservation::exited();
                };
                let waker = std::task::Waker::from(Arc::new(ExitWake(self.event.clone())));
                let mut cx = Context::from_waker(&waker);
                match std::pin::Pin::new(join).poll(&mut cx) {
                    Poll::Pending => {
                        ExitObservation::pending(FinishWatch::Notify(self.event.clone()))
                    }
                    Poll::Ready(result) => {
                        result.expect("the real branch job returned");
                        self.join = None;
                        self.joined.store(true, Ordering::Release);
                        ExitObservation::exited()
                    }
                }
            }
        }
        impl StepOperator for Kernel {
            fn initial_step(&self) -> NextStep {
                NextStep::NeedInput
            }
            fn control_observable(&self) -> Option<Arc<Observable>> {
                Some(self.event.clone())
            }
            fn try_push(&mut self, _: &mut DriverStep<'_>, _: RawInput) -> InputResult {
                unreachable!("this EOS fixture has no input data")
            }
            fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<()> {
                self.finishing = true;
                Ok(())
            }
            fn drive(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
                assert!(self.finishing);
                Ok(StepResult::new(None, NextStep::Finished, 0))
            }
        }
        struct EmptySource {
            event: Arc<Observable>,
            queries: Arc<AtomicUsize>,
            drops: Arc<AtomicUsize>,
        }
        impl Drop for EmptySource {
            fn drop(&mut self) {
                self.drops.fetch_add(1, Ordering::SeqCst);
            }
        }
        impl OperatorLifecycle for EmptySource {
            fn name(&self) -> &str {
                "disposed prefix with surviving suffix"
            }
        }
        impl SourceKernel for EmptySource {
            fn start(&self) -> SourceStart {
                SourceStart::Finished
            }
            fn control_observable(&self) -> Option<Arc<Observable>> {
                self.queries.fetch_add(1, Ordering::SeqCst);
                Some(self.event.clone())
            }
            fn poll_output(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
                unreachable!("the source has no work")
            }
        }
        struct ParentRouter {
            copy: CopyRouter,
            _original: Arc<Data>,
        }
        impl InputEncodingPolicy for ParentRouter {}
        impl FanoutRouter for ParentRouter {
            type Cursor = (Chunk, usize);
            fn begin(
                &mut self,
                input: PreparedInput,
                factory: &dyn InputConstructionFactory,
                ctx: &mut DriverStep<'_>,
            ) -> ExecutionResult<Self::Cursor> {
                self.copy.begin(input, factory, ctx)
            }
            fn advance(
                &mut self,
                cursor: &mut Self::Cursor,
                factory: &dyn InputConstructionFactory,
                ctx: &mut DriverStep<'_>,
            ) -> ExecutionResult<RouteStep> {
                self.copy.advance(cursor, factory, ctx)
            }
            fn accepts_branch(&self, index: usize) -> bool {
                self.copy.accepts_branch(index)
            }
        }
        for fail in [false, true] {
            let io = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap();
            let state = Arc::new(RuntimeState::default());
            let trackers = (0..3)
                .map(|i| MemTracker::new_root(format!("actual branch {i} owned payload")))
                .collect::<Vec<_>>();
            let mut weak = Vec::new();
            let mut drops = Vec::new();
            let mut closes = Vec::new();
            let mut primary = Vec::new();
            let mut secondary = Vec::new();
            let mut nested = Vec::new();
            let mut joined = Vec::new();
            let mut branches = Vec::new();
            let (release, gate) = tokio::sync::oneshot::channel();
            let mut gate = Some(gate);
            for (i, tracker) in trackers.iter().enumerate() {
                let bytes = if i == 2 {
                    vec![5, 6, 7]
                } else {
                    vec![u8::try_from(i + 1).unwrap(); 3]
                };
                let capacity = bytes.capacity();
                let data = Arc::new(Data {
                    bytes,
                    _funding: TrackedBytes::try_new(capacity, tracker.clone()).unwrap(),
                });
                weak.push(Arc::downgrade(&data));
                let d = Arc::new(AtomicUsize::new(0));
                let c = Arc::new(AtomicUsize::new(0));
                let p = Arc::new(AtomicUsize::new(0));
                let s = Arc::new(AtomicUsize::new(0));
                let n = Arc::new(AtomicBool::new(false));
                let j = Arc::new(AtomicBool::new(false));
                drops.push(d.clone());
                closes.push(c.clone());
                primary.push(p.clone());
                secondary.push(s.clone());
                nested.push(n.clone());
                joined.push(j.clone());
                branches.push(Box::new(Kernel {
                    data: ManuallyDrop::new(data),
                    event: Arc::new(Observable::new()),
                    runtime: io.handle().clone(),
                    gate: if i == 2 { gate.take() } else { None },
                    join: None,
                    joined: j,
                    closed: c,
                    dropped: d,
                    primary: p,
                    secondary: s,
                    nested_unwind: n,
                    fail_drop: fail && i < 2,
                    activated: false,
                    finishing: false,
                }) as Box<dyn Operator>);
            }
            let parent_tracker = MemTracker::new_root("original retained fanout parent");
            let bytes = vec![31, 37];
            let capacity = bytes.capacity();
            let parent = Arc::new(Data {
                bytes,
                _funding: TrackedBytes::try_new(capacity, parent_tracker.clone()).unwrap(),
            });
            let parent_weak = Arc::downgrade(&parent);
            let mut fanout = Fanout::new(
                "independent final Box retirement".into(),
                branches,
                ParentRouter {
                    copy: CopyRouter(3),
                    _original: parent,
                },
                None,
            );
            prepare_on(&mut fanout, &state);
            let source_queries = Arc::new(AtomicUsize::new(0));
            let source_drops = Arc::new(AtomicUsize::new(0));
            let mut driver = PipelineDriver::new(
                1,
                vec![
                    Box::new(Source::new(EmptySource {
                        event: Arc::new(Observable::new()),
                        queries: source_queries.clone(),
                        drops: source_drops.clone(),
                    })),
                    Box::new(fanout),
                ],
                None,
                Vec::new(),
                state.clone(),
                None,
            );
            let first = driver.process(Duration::from_secs(1));
            // Revisit the healthy data-finished prefix with its original stable
            // metadata while the third actual owner is still gated.
            let second = driver.process(Duration::from_secs(1));
            let dropped_before = drops
                .iter()
                .map(|n| n.load(Ordering::SeqCst))
                .collect::<Vec<_>>();
            let closed_before = closes
                .iter()
                .map(|n| n.load(Ordering::SeqCst))
                .collect::<Vec<_>>();
            let healthy_funding_before = trackers[2].current();
            let healthy_joined_before = joined[2].load(Ordering::Acquire);
            let sent = release.send(());
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut last = driver.process(Duration::from_millis(10));
            while drops[2].load(Ordering::SeqCst) == 0 && Instant::now() < deadline {
                std::thread::yield_now();
                last = driver.process(Duration::from_millis(10));
            }
            // Only after release and the actual JoinHandle is consumed do we
            // assert. No fake completion is issued for any pending branch.
            assert!(sent.is_ok());
            assert!(joined[2].load(Ordering::Acquire));
            assert!(matches!(first, DriverState::PendingFinish));
            assert!(matches!(second, DriverState::PendingFinish));
            assert_eq!(dropped_before, [1, 1, 0]);
            assert_eq!(closed_before, [1, 1, 0]);
            assert_eq!(healthy_funding_before, 3);
            assert!(!healthy_joined_before);
            assert_eq!(source_drops.load(Ordering::SeqCst), 1);
            assert_eq!(source_queries.load(Ordering::SeqCst), 1);
            assert_eq!(
                drops
                    .iter()
                    .map(|n| n.load(Ordering::SeqCst))
                    .collect::<Vec<_>>(),
                [1, 1, 1]
            );
            assert_eq!(
                closes
                    .iter()
                    .map(|n| n.load(Ordering::SeqCst))
                    .collect::<Vec<_>>(),
                [1, 1, 1]
            );
            assert!(
                nested.iter().all(|n| !n.load(Ordering::Acquire)),
                "no sibling destructor runs in another foreign unwind"
            );
            assert_eq!(trackers[2].current(), 0);
            assert!(weak[2].upgrade().is_none());
            assert!(
                state.error().is_none(),
                "Box cleanup cannot become an originating Task failure"
            );
            if fail {
                assert!(matches!(last, DriverState::PendingFinish));
                assert!(driver.has_pending_finish());
                assert!(state.error_state().local_exit_fault().is_some());
                for i in 0..2 {
                    assert_eq!(primary[i].load(Ordering::SeqCst), 1);
                    assert_eq!(secondary[i].load(Ordering::SeqCst), 0);
                    assert_eq!(trackers[i].current(), 3);
                    assert_eq!(
                        weak[i].upgrade().unwrap().bytes,
                        vec![u8::try_from(i + 1).unwrap(); 3]
                    );
                }
            } else {
                assert!(matches!(last, DriverState::Finished));
                assert!(!driver.has_pending_finish());
                assert!(state.error_state().local_exit_fault().is_none());
                for i in 0..2 {
                    assert_eq!(trackers[i].current(), 0);
                    assert!(weak[i].upgrade().is_none());
                }
            }
            let health = state.error_state().local_exit_fault();
            drop(driver);
            drop(io);
            if fail {
                assert_eq!(
                    parent_weak.upgrade().unwrap().bytes,
                    [31, 37],
                    "the original parent remains addressable in Driver custody, not a lost local ManuallyDrop"
                );
                assert_eq!(parent_tracker.current(), 2);
                assert_eq!(state.error_state().local_exit_fault(), health);
                for i in 0..2 {
                    assert_eq!(trackers[i].current(), 3);
                    assert_eq!(secondary[i].load(Ordering::SeqCst), 0);
                }
            } else {
                assert!(parent_weak.upgrade().is_none());
                assert_eq!(parent_tracker.current(), 0);
            }
        }
    }
    #[test]
    fn strict_initial_finished_branch_fails_preparation_while_ordinary_withdrawal_remains_legal() {
        struct Closed;
        impl OperatorLifecycle for Closed {
            fn name(&self) -> &str {
                "initially closed branch"
            }
        }
        impl InputEncodingPolicy for Closed {}
        impl super::super::SinkKernel for Closed {
            fn start(&self) -> super::super::TransformStart {
                super::super::TransformStart::Finished
            }
            fn consume(
                &mut self,
                _: &mut DriverStep<'_>,
                _: PreparedInput,
            ) -> ExecutionResult<super::super::SinkStep> {
                unreachable!("closed fixture cannot receive input")
            }
        }
        struct Router(bool);
        impl InputEncodingPolicy for Router {}
        impl FanoutRouter for Router {
            type Cursor = ();
            fn begin(
                &mut self,
                _: PreparedInput,
                _: &dyn InputConstructionFactory,
                _: &mut DriverStep<'_>,
            ) -> ExecutionResult<()> {
                unreachable!("empty fixture has no input")
            }
            fn advance(
                &mut self,
                _: &mut (),
                _: &dyn InputConstructionFactory,
                _: &mut DriverStep<'_>,
            ) -> ExecutionResult<RouteStep> {
                Ok(RouteStep::Finished)
            }
            fn accepts_branch(&self, _: usize) -> bool {
                true
            }
            fn requires_branch_drain(&self, _: usize) -> bool {
                self.0
            }
        }
        for strict in [false, true] {
            let state = RuntimeState::default();
            let mut fanout = Fanout::new(
                "closed branch admission".into(),
                vec![Box::new(super::super::Transform::hydrating(Closed))],
                Router(strict),
                None,
            );
            fanout.set_mem_tracker(MemTracker::new_root("closed branch fixture"));
            fanout.set_construction_factory(
                crate::exec::operators::logical_change_summary::tests::creator(&state),
            );
            fanout.bind_runtime_state(&state).unwrap();
            let result = fanout.prepare();
            if strict {
                assert!(
                    result
                        .unwrap_err()
                        .detail()
                        .contains("required fanout branch withdrew")
                );
            } else {
                result.unwrap();
                assert!(matches!(
                    fanout.drive_for_test(&state).unwrap().next,
                    NextStep::Finished
                ));
            }
        }
    }
}
