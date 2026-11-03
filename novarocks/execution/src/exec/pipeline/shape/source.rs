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

//! Source protocol: one reported output and next action per actual poll.

use super::{ControlStep, DriverStep, InputResult, NextStep, RawInput, StepOperator, StepResult};
#[cfg(test)]
use crate::exec::pipeline::operator::ExitProgress;
use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::exec::pipeline::operator::{ExitObservation, FinishWatch, InputConstructionFactory};
use crate::runtime::{
    mem_tracker::MemTracker,
    profile::OperatorProfiles,
    runtime_state::RuntimeState,
    task::{ExecutionFailure, ExecutionResult, io::TaskEventSink},
};
use std::sync::Arc;

pub trait SourceKernel: Send {
    fn poll_control(&mut self, _ctx: &mut DriverStep<'_>) -> ExecutionResult<ControlStep> {
        Ok(ControlStep::Active)
    }
    fn control_observable(&self) -> Option<Arc<super::super::schedule::observer::Observable>> {
        None
    }
    fn start(&self) -> SourceStart {
        SourceStart::Ready
    }
    fn poll_output(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult>;
    fn on_downstream_backpressure(&mut self, _paused: bool) {}
}

#[derive(Clone, Copy)]
pub enum SourceStart {
    Ready,
    Finished,
}

/// An atomic dequeue observation. Empty and closed must be decided under
/// the same queue lock; querying them separately can skip a producer's last
/// publication between those queries.
pub(crate) enum SourceRead {
    Chunk(crate::exec::chunk::Chunk),
    Pending,
    Finished,
}

pub struct Source<K> {
    kernel: K,
    next: NextStep,
    failure: Option<ExecutionFailure>,
}
impl<K: SourceKernel> Source<K> {
    pub fn new(kernel: K) -> Self {
        let next = match kernel.start() {
            SourceStart::Ready => NextStep::Runnable,
            SourceStart::Finished => NextStep::Finished,
        };
        Self {
            kernel,
            next,
            failure: None,
        }
    }
    /// Domain-specific immutable declarations remain available on a typed
    /// source wrapper without exposing its work protocol or private bindings.
    pub(crate) fn kernel(&self) -> &K {
        &self.kernel
    }
    fn stop_work(&mut self, failure: ExecutionFailure) -> ExecutionFailure {
        let first = self.failure.get_or_insert(failure).clone();
        self.next = NextStep::Finished;
        first
    }
}
#[derive(Debug)]
pub(crate) enum SourceContractFailure {
    InputEdge,
    InputEos,
    RequestedInput,
    MissingRuntimeBinding,
    ColumnCount { expected: usize, actual: usize },
}
impl std::fmt::Display for SourceContractFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InputEdge => f.write_str("source does not accept an input edge"),
            Self::InputEos => f.write_str("source received input EOS from a nonexistent edge"),
            Self::RequestedInput => f.write_str("source requested input from a nonexistent edge"),
            Self::MissingRuntimeBinding => f.write_str("source runtime binding is missing"),
            Self::ColumnCount { expected, actual } => write!(
                f,
                "source output binding has {expected} slots but the produced frame has {actual} columns"
            ),
        }
    }
}
impl std::error::Error for SourceContractFailure {}
impl novarocks_failure_contract::FailureProjection for SourceContractFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let reason = match self {
            Self::InputEdge => ExecutionReason::SourceInputEdge,
            Self::InputEos => ExecutionReason::SourceInputEos,
            Self::RequestedInput => ExecutionReason::SourceRequestedInput,
            Self::MissingRuntimeBinding => ExecutionReason::SourceMissingRuntimeBinding,
            Self::ColumnCount { .. } => ExecutionReason::SourceColumnCount,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
pub(crate) fn source_contract_failure(
    cause: SourceContractFailure,
) -> crate::runtime::task::ExecutionFailure {
    crate::runtime::task::ExecutionFailure::from_domain(cause)
}

impl<K: SourceKernel> StepOperator for Source<K> {
    fn poll_control(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<ControlStep> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if matches!(self.next, NextStep::Finished) {
            return Ok(ControlStep::Active);
        }
        match self.kernel.poll_control(ctx) {
            Ok(control) => {
                match control {
                    ControlStep::Runnable => self.next = NextStep::Runnable,
                    ControlStep::Withdrawn => self.next = NextStep::Finished,
                    ControlStep::Active => {}
                }
                Ok(control)
            }
            Err(error) => {
                ctx.report_failure(error.clone());
                Err(self.stop_work(error))
            }
        }
    }
    fn control_observable(&self) -> Option<Arc<super::super::schedule::observer::Observable>> {
        self.kernel.control_observable()
    }
    fn initial_step(&self) -> NextStep {
        self.next.clone()
    }
    fn try_push(&mut self, ctx: &mut DriverStep<'_>, input: RawInput) -> InputResult {
        InputResult::Rejected {
            input,
            failure: ctx
                .checkpoint()
                .err()
                .unwrap_or_else(|| source_contract_failure(SourceContractFailure::InputEdge)),
        }
    }
    fn drive(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        ctx.checkpoint()?;
        if matches!(self.next, NextStep::Finished) {
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        }
        if let NextStep::Wait(wait) = &self.next
            && !wait.is_ready()
        {
            return Ok(StepResult::new(None, self.next.clone(), 0));
        }
        let step = match self.kernel.poll_output(ctx) {
            Ok(step) => step,
            Err(failure) => return Err(self.stop_work(failure)),
        };
        if matches!(step.next, NextStep::NeedInput) {
            return Err(self.stop_work(source_contract_failure(
                SourceContractFailure::RequestedInput,
            )));
        }
        self.next = step.next.clone();
        Ok(step)
    }
    fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<()> {
        Err(source_contract_failure(SourceContractFailure::InputEos))
    }
}

// Lifecycle forwarding keeps each kernel's private bindings on the same owner.
impl<K: SourceKernel + OperatorLifecycle> OperatorLifecycle for Source<K> {
    fn name(&self) -> &str {
        self.kernel.name()
    }

    fn on_downstream_backpressure(&mut self, paused: bool) {
        SourceKernel::on_downstream_backpressure(&mut self.kernel, paused);
    }
    fn bind_poll_budget(
        &mut self,
        budget: novarocks_spi::connector::read_stack::ConnectorPollBudget,
    ) {
        self.kernel.bind_poll_budget(budget);
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.kernel.set_mem_tracker(tracker);
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.kernel.set_construction_factory(factory);
    }
    fn creates_typed_storage(&self) -> bool {
        self.kernel.creates_typed_storage()
    }
    fn set_profiles(&mut self, profiles: OperatorProfiles) {
        self.kernel.set_profiles(profiles);
    }
    fn compiled_schema_preparation_site(
        &self,
    ) -> Option<(
        &Arc<novarocks_local_program::LocalProgram>,
        crate::runtime::preparation_metadata::CompiledSchemaSite,
    )> {
        self.kernel.compiled_schema_preparation_site()
    }
    #[cfg(any(test, novarocks_dev_controls))]
    fn prepared_schema_for_test(&self) -> Option<crate::exec::chunk::ChunkSchemaRef> {
        self.kernel.prepared_schema_for_test()
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        self.kernel
            .prepare()
            .map_err(|failure| self.stop_work(failure))
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.kernel.bind_runtime_state(state)
    }
    fn activate(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.kernel.activate(state)
    }
    fn set_task_event_sink(&mut self, sink: Arc<dyn TaskEventSink>) {
        self.kernel.set_task_event_sink(sink);
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.next = NextStep::Finished;
        self.kernel.begin_exit()
    }
    fn observe_exit(&mut self) -> ExitObservation {
        self.kernel.observe_exit()
    }
    fn visit_exits(&mut self, visit: &mut dyn FnMut(ExitObservation)) {
        self.kernel.visit_exits(visit);
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.next = NextStep::Finished;
        self.kernel.close()
    }
    fn cancel(&mut self) {
        self.next = NextStep::Finished;
        self.kernel.cancel();
    }
    fn on_driver_failure(&mut self) {
        self.next = NextStep::Finished;
        self.kernel.on_driver_failure();
    }

    fn pending_finish(&self) -> Option<FinishWatch> {
        self.kernel.pending_finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::pipeline::{
        driver_step::TurnWork,
        schedule::observer::Observable,
        step::{WaitFact, WaitReason},
    };
    use crate::runtime::runtime_state::RuntimeErrorState;
    use std::{
        collections::VecDeque,
        num::NonZeroU64,
        sync::atomic::{AtomicUsize, Ordering},
    };

    struct Script {
        initial: SourceStart,
        steps: VecDeque<StepResult>,
        polls: Arc<AtomicUsize>,
    }
    impl SourceKernel for Script {
        fn start(&self) -> SourceStart {
            self.initial
        }
        fn poll_output(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
            self.polls.fetch_add(1, Ordering::Relaxed);
            Ok(self
                .steps
                .pop_front()
                .expect("source work is never replayed"))
        }
    }
    fn script(
        initial: SourceStart,
        steps: impl IntoIterator<Item = StepResult>,
    ) -> (Source<Script>, Arc<AtomicUsize>) {
        let polls = Arc::new(AtomicUsize::new(0));
        (
            Source::new(Script {
                initial,
                steps: steps.into_iter().collect(),
                polls: polls.clone(),
            }),
            polls,
        )
    }
    fn chunk() -> crate::exec::chunk::Chunk {
        super::super::tests::chunk()
    }

    #[test]
    fn output_and_wait_then_output_and_finished_do_not_replay_or_hide_output() {
        let event = Arc::new(Observable::new());
        let wait = WaitFact {
            observed_generation: event.generation(),
            observable: event.clone(),
            reason: WaitReason::Input,
            deadline: None,
        };
        let (mut source, polls) = script(
            SourceStart::Ready,
            [
                StepResult::new(Some(chunk()), NextStep::Wait(wait), 0),
                StepResult::new(Some(chunk()), NextStep::Finished, 0),
            ],
        );
        let control = RuntimeErrorState::default();
        let mut work = TurnWork::new(NonZeroU64::new(1).unwrap(), 2);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut context = DriverStep::new(&control, &mut work, &mut value_conversions);
        let first = source.drive(&mut context).unwrap();
        assert_eq!(first.output.unwrap().len(), 2);
        assert!(matches!(first.next, NextStep::Wait(_)));
        let waiting = source.drive(&mut context).unwrap();
        assert!(waiting.output.is_none());
        assert!(matches!(waiting.next, NextStep::Wait(_)));
        assert_eq!(polls.load(Ordering::Relaxed), 1);
        event.notify_observers();
        let last = source.drive(&mut context).unwrap();
        assert_eq!(last.output.unwrap().len(), 2);
        assert!(matches!(last.next, NextStep::Finished));
        let ended = source.drive(&mut context).unwrap();
        assert!(ended.output.is_none());
        assert!(matches!(ended.next, NextStep::Finished));
        assert_eq!(polls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn initial_finish_and_refused_input_never_enter_source_work() {
        let (mut source, polls) = script(SourceStart::Finished, []);
        let control = RuntimeErrorState::default();
        let mut work = TurnWork::new(NonZeroU64::new(1).unwrap(), 2);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut context = DriverStep::new(&control, &mut work, &mut value_conversions);
        let input = RawInput::new(chunk(), None);
        let InputResult::Rejected { input, .. } = source.try_push(&mut context, input) else {
            panic!("a source must return the exact unaccepted input");
        };
        assert_eq!(input.rows(), 2);
        let ended = source.drive(&mut context).unwrap();
        assert!(ended.output.is_none());
        assert!(matches!(ended.next, NextStep::Finished));
        assert_eq!(polls.load(Ordering::Relaxed), 0);
    }
}

#[cfg(test)]
mod exit_tests {
    use super::*;
    use crate::exec::pipeline::{driver_step::TurnWork, schedule::observer::Observable};
    use crate::runtime::runtime_state::RuntimeErrorState;
    use std::{
        num::NonZeroU64,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    struct Exiting {
        exited: Arc<AtomicBool>,
        event: Arc<Observable>,
        payload_calls: Arc<AtomicUsize>,
        failure: Option<ExecutionFailure>,
    }
    impl OperatorLifecycle for Exiting {
        fn name(&self) -> &str {
            "EXITING_SOURCE"
        }
        fn observe_exit(&mut self) -> ExitObservation {
            if self.exited.load(Ordering::Acquire) {
                ExitObservation::exited()
            } else {
                ExitObservation::pending(FinishWatch::Notify(self.event.clone()))
            }
        }
    }
    impl SourceKernel for Exiting {
        fn poll_output(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
            self.payload_calls.fetch_add(1, Ordering::Relaxed);
            if let Some(failure) = &self.failure {
                return Err(failure.clone());
            }
            panic!("an exiting source cannot do payload work");
        }
    }
    #[test]
    fn ending_source_dataflow_does_not_certify_its_real_owner_exit() {
        let exited = Arc::new(AtomicBool::new(false));
        let payload_calls = Arc::new(AtomicUsize::new(0));
        let mut source = Source::new(Exiting {
            exited: exited.clone(),
            event: Arc::new(Observable::new()),
            payload_calls: payload_calls.clone(),
            failure: None,
        });
        source.begin_exit().unwrap();
        assert!(matches!(source.next, NextStep::Finished));
        assert!(matches!(
            source.observe_exit().progress,
            ExitProgress::Pending(_)
        ));
        let control = RuntimeErrorState::default();
        let mut work = TurnWork::new(NonZeroU64::new(1).unwrap(), 8);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut context = DriverStep::new(&control, &mut work, &mut value_conversions);
        let step = source.drive(&mut context).unwrap();
        assert!(step.output.is_none());
        assert!(matches!(step.next, NextStep::Finished));
        assert_eq!(payload_calls.load(Ordering::Relaxed), 0);
        assert!(matches!(
            source.observe_exit().progress,
            ExitProgress::Pending(_)
        ));
        exited.store(true, Ordering::Release);
        assert!(matches!(
            source.observe_exit().progress,
            ExitProgress::Exited
        ));
    }
    #[test]
    fn a_source_failure_stops_payload_reentry_without_certifying_actual_exit_or_deciding_task() {
        let exited = Arc::new(AtomicBool::new(false));
        let polls = Arc::new(AtomicUsize::new(0));
        let first = ExecutionFailure::from(novarocks_functions::KernelFailure::ResourceExhausted);
        let mut source = Source::new(Exiting {
            exited: exited.clone(),
            event: Arc::new(Observable::new()),
            payload_calls: polls.clone(),
            failure: Some(first.clone()),
        });
        let control = RuntimeErrorState::default();
        let mut work = TurnWork::new(NonZeroU64::MIN, 8);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = DriverStep::new(&control, &mut work, &mut value_conversions);
        assert_eq!(source.drive(&mut ctx).unwrap_err(), first);
        assert_eq!(source.drive(&mut ctx).unwrap_err(), first);
        assert_eq!(polls.load(Ordering::Relaxed), 1);
        assert!(matches!(source.next, NextStep::Finished));
        assert!(matches!(
            source.observe_exit().progress,
            ExitProgress::Pending(_)
        ));
        assert!(control.error().is_none());
        source.begin_exit().unwrap();
        assert!(matches!(
            source.observe_exit().progress,
            ExitProgress::Pending(_)
        ));
        exited.store(true, Ordering::Release);
        assert!(matches!(
            source.observe_exit().progress,
            ExitProgress::Exited
        ));
        assert_eq!(polls.load(Ordering::Relaxed), 1);
    }
}
