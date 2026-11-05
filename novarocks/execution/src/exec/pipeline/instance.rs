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

//! One installed operator and its last explicit scheduling facts. Preparation,
//! factory, profiles, accepted input and work cursors belong to the receiver.

use super::{
    driver_failure::{DriverFailure, ExitHook},
    driver_step::DriverStep,
    input::RawInput,
    operator::{ExitObservation, Operator},
    operator_exit::{ExitAction, OperatorExitScope, OwnedOperator},
    shape::StepOperator,
    step::{AdmissionNext, InputResult, NextStep, RootPending, RootStep, StepResult, UpstreamPull},
};
use crate::runtime::{
    mem_tracker::MemTracker,
    task::{ExecutionResult, PipelineOperation},
};
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

pub(crate) struct OperatorInstance {
    ordinal: usize,
    inner: OwnedOperator,
    next: NextStep,
    admission_next: Option<AdmissionNext>,
    finishing: bool,
    exit_started: bool,
    close_started: bool,
    observed_owner_exited: bool,
    tracker: Option<Arc<MemTracker>>,
    exit_scope: OperatorExitScope,
    runtime_error: Arc<crate::runtime::runtime_state::RuntimeErrorState>,
}
impl OperatorInstance {
    pub(crate) fn new(
        ordinal: usize,
        inner: Box<dyn Operator>,
        tracker: Option<Arc<MemTracker>>,
        runtime_error: Arc<crate::runtime::runtime_state::RuntimeErrorState>,
    ) -> Self {
        let next = inner.initial_step();
        let mut inner = OwnedOperator::new(inner);
        inner.install_control();
        Self {
            ordinal,
            inner,
            next,
            admission_next: None,
            finishing: false,
            exit_started: false,
            close_started: false,
            observed_owner_exited: false,
            tracker,
            exit_scope: OperatorExitScope::new(),
            runtime_error,
        }
    }
    pub(crate) fn name(&self) -> &str {
        self.inner.name()
    }
    pub(crate) fn tracker(&self) -> Option<&Arc<MemTracker>> {
        self.tracker.as_ref()
    }
    pub(crate) fn poll_control(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<()> {
        if !self.inner.is_live() {
            return Ok(());
        }
        let mut ctx = ctx.operator_step(self.ordinal, PipelineOperation::Control);
        let control = self
            .inner
            .poll_control(&mut ctx)
            .map_err(|failure| ctx.locate(failure))
            .inspect_err(|failure| ctx.report_failure(failure.clone()))?;
        // An exiting owner can still report a failure, but stale readiness
        // cannot restore the data responsibility already retired by demand.
        if self.exit_started {
            return Ok(());
        }
        match control {
            super::shape::ControlStep::Active => {}
            super::shape::ControlStep::Runnable => self.next = NextStep::Runnable,
            super::shape::ControlStep::Withdrawn => self.next = NextStep::Finished,
        }
        Ok(())
    }
    pub(crate) fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.exit_started = true;
        if !self.inner.is_live() {
            return Ok(());
        }
        self.exit_scope
            .run(
                ExitAction::Begin,
                &self.runtime_error,
                || DriverFailure::ExitHookPanicked(ExitHook::BeginExit).into(),
                || self.inner.begin_exit(),
            )
            .into_cleanup_result()
    }
    /// Data retirement cannot discharge the same operator's physical scope.
    pub(crate) fn retire_data(&mut self) -> ExecutionResult<()> {
        self.next = NextStep::Finished;
        self.admission_next = None;
        self.finishing = true;
        self.begin_exit()
    }
    pub(crate) fn visit_exits(&mut self, visit: &mut dyn FnMut(ExitObservation)) {
        let mut returned = false;
        let mut owner_pending = false;
        let observations = self.exit_scope.observe(
            &self.runtime_error,
            || DriverFailure::ExitHookPanicked(ExitHook::Observe).into(),
            |collect| {
                if self.inner.is_live() {
                    self.inner.visit_exits(&mut |observation| {
                        owner_pending |=
                            !matches!(&observation.progress, super::operator::ExitProgress::Exited);
                        collect(observation);
                    });
                } else {
                    collect(ExitObservation::exited());
                }
                returned = true;
            },
        );
        // An unknown independent cleanup phase cannot cancel an actual join
        // proof. Conversely, an observer unwind supplies no safe close proof.
        self.observed_owner_exited = returned && !owner_pending;
        for observation in observations {
            visit(observation);
        }
    }
    /// Close and its asynchronous work must already have actually returned.
    /// Final Box/foreign capture Drop is itself part of the physical proof.
    pub(crate) fn dispose_after_observed_exit(&mut self) {
        if !self.close_started || !self.observed_owner_exited || self.exit_scope.is_unresolved() {
            return;
        }
        let result = self
            .exit_scope
            .run(
                ExitAction::Dispose,
                &self.runtime_error,
                || DriverFailure::ExitHookPanicked(ExitHook::Dispose).into(),
                || {
                    self.inner.dispose();
                    Ok(())
                },
            )
            .into_cleanup_result();
        if let Err(cause) = result {
            self.runtime_error.report_cleanup(&cause);
        }
    }
    pub(crate) fn close_was_started(&self) -> bool {
        self.close_started
    }
    pub(crate) fn exit_has_started(&self) -> bool {
        self.exit_started
    }
    pub(crate) fn on_downstream_backpressure(&mut self, paused: bool) {
        if self.inner.is_live() {
            self.inner.on_downstream_backpressure(paused);
        }
    }
    pub(crate) fn owner_exit_observed(&self) -> bool {
        self.observed_owner_exited
    }
    pub(crate) fn close(&mut self) -> ExecutionResult<()> {
        self.close_started = true;
        if !self.inner.is_live() {
            return Ok(());
        }
        self.exit_scope
            .run(
                ExitAction::Close,
                &self.runtime_error,
                || DriverFailure::ExitHookPanicked(ExitHook::Close).into(),
                || self.inner.close(),
            )
            .into_cleanup_result()
    }
    pub(crate) fn cancel(&mut self) {
        if !self.inner.is_live() {
            return;
        }
        let result = self
            .exit_scope
            .run(
                ExitAction::Cancel,
                &self.runtime_error,
                || DriverFailure::ExitHookPanicked(ExitHook::Cancel).into(),
                || {
                    self.inner.cancel();
                    Ok(())
                },
            )
            .into_cleanup_result();
        if let Err(failure) = result {
            self.runtime_error.report_cleanup(&failure);
        }
    }
    pub(crate) fn on_driver_failure(&mut self) {
        if !self.inner.is_live() {
            return;
        }
        let result = self
            .exit_scope
            .run(
                ExitAction::FailureStop,
                &self.runtime_error,
                || DriverFailure::ExitHookPanicked(ExitHook::FailureStop).into(),
                || {
                    self.inner.on_driver_failure();
                    Ok(())
                },
            )
            .into_cleanup_result();
        if let Err(failure) = result {
            self.runtime_error.report_cleanup(&failure);
        }
    }
    pub(crate) fn is_finished(&self) -> bool {
        matches!(self.next, NextStep::Finished)
    }
    pub(crate) fn need_input(&self) -> bool {
        !self.finishing && matches!(self.next, NextStep::NeedInput)
    }
    pub(crate) fn reserve_upstream(
        &mut self,
        ctx: &mut DriverStep<'_>,
    ) -> ExecutionResult<UpstreamPull> {
        if !self.need_input() {
            return Ok(UpstreamPull::Busy);
        }
        if let Some(AdmissionNext::Wait(wait)) = &self.admission_next
            && !wait.is_ready()
        {
            return Ok(UpstreamPull::Wait(wait.clone()));
        }
        let mut ctx = ctx.operator_step(self.ordinal, PipelineOperation::Admission);
        let result = self.inner.reserve_upstream(&mut ctx)?;
        self.admission_next = match &result {
            UpstreamPull::Wait(wait) => Some(AdmissionNext::Wait(wait.clone())),
            _ => None,
        };
        Ok(result)
    }
    pub(crate) fn work_next(&self) -> Option<&NextStep> {
        Some(&self.next)
    }
    pub(crate) fn admission_next(&self) -> Option<&AdmissionNext> {
        self.admission_next.as_ref()
    }
    pub(crate) fn has_continuation(&self) -> bool {
        match &self.next {
            NextStep::Runnable | NextStep::Yield => true,
            NextStep::Wait(wait) => wait.is_ready(),
            _ => false,
        }
    }
    pub(crate) fn release_root_workspace(&mut self) {
        if self.inner.is_live() {
            self.inner.release_root_workspace();
        }
    }
    pub(crate) fn drive_precovered(
        &mut self,
        ctx: &mut DriverStep<'_>,
        input: &crate::runtime::task::io::RootInputPermit,
    ) -> ExecutionResult<RootStep> {
        let mut ctx = ctx.operator_step(self.ordinal, PipelineOperation::Pull);
        ctx.checkpoint()?;
        self.admission_next = None;
        if let NextStep::Wait(wait) = &self.next
            && !wait.is_ready()
        {
            return Ok(RootStep::Pending(RootPending::Wait(wait.clone())));
        }
        let result = self
            .inner
            .drive_with_root_input(&mut ctx, input)
            .inspect_err(|error| ctx.report_failure(error.clone()))?;
        self.next = match &result {
            RootStep::Complete(step) => step.next.clone(),
            RootStep::Pending(pending) => pending.next(),
        };
        Ok(result)
    }
}
impl Deref for OperatorInstance {
    type Target = dyn Operator;
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}
impl DerefMut for OperatorInstance {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut()
    }
}
impl StepOperator for OperatorInstance {
    #[cfg(novarocks_dev_controls)]
    fn is_mark_join_probe(&self) -> bool {
        self.inner.is_live() && self.inner.is_mark_join_probe()
    }

    fn control_observable(&self) -> Option<Arc<super::schedule::observer::Observable>> {
        self.inner.control_observable()
    }
    fn initial_step(&self) -> NextStep {
        self.next.clone()
    }
    fn try_push(&mut self, ctx: &mut DriverStep<'_>, input: RawInput) -> InputResult {
        let mut ctx = ctx.operator_step(self.ordinal, PipelineOperation::Admission);
        let result = self.inner.try_push(&mut ctx, input);
        match &result {
            InputResult::Accepted(Ok(step)) => {
                self.next = step.next.clone();
                self.admission_next = None;
            }
            InputResult::Deferred { next, .. } => self.admission_next = Some(next.clone()),
            _ => {}
        }
        result
    }
    fn drive(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        let mut ctx = ctx.operator_step(self.ordinal, PipelineOperation::Pull);
        ctx.checkpoint()?;
        self.admission_next = None;
        let step = self
            .inner
            .drive(&mut ctx)
            .inspect_err(|error| ctx.report_failure(error.clone()))?;
        self.next = step.next.clone();
        Ok(step)
    }
    fn finish(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<()> {
        if self.finishing {
            return Ok(());
        }
        let mut ctx = ctx.operator_step(self.ordinal, PipelineOperation::Finishing);
        ctx.checkpoint()?;
        self.inner.finish(&mut ctx)?;
        self.finishing = true;
        if matches!(self.next, NextStep::NeedInput) {
            self.next = NextStep::Runnable;
        }
        Ok(())
    }
}
