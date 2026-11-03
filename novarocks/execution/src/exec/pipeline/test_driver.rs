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

//! Native caller for finite CPU component fixtures. It retains one real turn,
//! returns complete step facts, and never buffers an unpublished output. Tests
//! with external waits, root grants, physical exits or exact per-step quotas
//! drive those owners explicitly instead. Execution's affected component tests
//! own this helper; it uses no external process, fixture or production hook.

use super::{
    driver_step::{DEFAULT_TURN_WORK_UNITS, DriverStep, TurnWork},
    input::RawInput,
    operator::Operator,
    step::{AdmissionNext, InputResult, NextStep, StepResult},
};
use crate::{
    exec::chunk::Chunk,
    runtime::{
        runtime_state::{RuntimeErrorState, RuntimeState},
        task::{ExecutionFailure, ExecutionResult},
    },
};
use std::sync::Arc;

pub(crate) struct TestDriver<'a, O: Operator + ?Sized> {
    operator: &'a mut O,
    control: Arc<RuntimeErrorState>,
    turn: TurnWork,
    next: NextStep,
    turn_pending: bool,
    failure: Option<ExecutionFailure>,
    value_conversions: novarocks_value_arrays::decode::ValueConversionContext,
}
impl<'a, O: Operator + ?Sized> TestDriver<'a, O> {
    pub(crate) fn new(operator: &'a mut O, state: &RuntimeState) -> Self {
        Self::with_control(operator, state.error_state())
    }
    pub(crate) fn with_control(operator: &'a mut O, control: Arc<RuntimeErrorState>) -> Self {
        let next = operator.initial_step();
        Self {
            operator,
            control,
            turn: TurnWork::new(std::num::NonZeroU64::MIN, DEFAULT_TURN_WORK_UNITS),
            turn_pending: matches!(next, NextStep::Yield),
            next,
            failure: None,
            value_conversions: novarocks_value_arrays::decode::ValueConversionContext::default(),
        }
    }
    /// Match the real driver's local failure notification. This stops owned
    /// operator work once; it neither accepts a Task conclusion nor claims
    /// that asynchronous owners have physically exited.
    fn stop_failed(&mut self, failure: ExecutionFailure) -> ExecutionFailure {
        if let Some(original) = &self.failure {
            return original.clone();
        }
        DriverStep::new(&self.control, &mut self.turn, &mut self.value_conversions)
            .report_failure(failure.clone());
        self.value_conversions.clear();
        self.failure = Some(failure.clone());
        self.operator.on_driver_failure();
        failure
    }
    pub(crate) fn operator(&self) -> &O {
        self.operator
    }
    pub(crate) fn operator_mut(&mut self) -> &mut O {
        self.operator
    }
    pub(crate) fn next(&self) -> &NextStep {
        &self.next
    }
    fn next_turn(&mut self) {
        self.turn
            .begin_turn(DEFAULT_TURN_WORK_UNITS)
            .expect("finite fixture turn generation");
        self.turn_pending = false;
    }
    pub(crate) fn try_push(&mut self, input: RawInput) -> InputResult {
        if self.turn_pending {
            self.next_turn();
        }
        let result = self.operator.try_push(
            &mut DriverStep::new(&self.control, &mut self.turn, &mut self.value_conversions),
            input,
        );
        match &result {
            InputResult::Accepted(Ok(step)) => {
                self.turn_pending = matches!(step.next, NextStep::Yield);
                self.next = step.next.clone();
            }
            InputResult::Deferred {
                next: AdmissionNext::Yield,
                ..
            } => self.turn_pending = true,
            _ => {}
        }
        result
    }
    pub(crate) fn drive(&mut self) -> ExecutionResult<StepResult> {
        if let Some(original) = &self.failure {
            return Err(original.clone());
        }
        if self.turn_pending {
            self.next_turn();
        }
        let result = match self.operator.drive(&mut DriverStep::new(
            &self.control,
            &mut self.turn,
            &mut self.value_conversions,
        )) {
            Ok(result) => result,
            Err(failure) => return Err(self.stop_failed(failure)),
        };
        self.turn_pending = matches!(result.next, NextStep::Yield);
        self.next = result.next.clone();
        if matches!(self.next, NextStep::Finished) {
            self.value_conversions.clear();
        }
        Ok(result)
    }
    pub(crate) fn signal_eos(&mut self) -> ExecutionResult<()> {
        if let Some(original) = &self.failure {
            return Err(original.clone());
        }
        if self.turn_pending {
            self.next_turn();
        }
        if let Err(failure) = self.operator.finish(&mut DriverStep::new(
            &self.control,
            &mut self.turn,
            &mut self.value_conversions,
        )) {
            return Err(self.stop_failed(failure));
        }
        if matches!(self.next, NextStep::NeedInput) {
            self.next = NextStep::Runnable;
        }
        Ok(())
    }
    /// Return the actual call which produces a payload or reaches a data/wait
    /// boundary. Intermediate CPU-only work is driven under the same allowance.
    /// Per-step counter/quantum tests use drive, observing every call separately.
    pub(crate) fn advance_to_boundary(&mut self) -> ExecutionResult<StepResult> {
        for _ in 0..100_000 {
            let step = self.drive()?;
            if step.output.is_some() || !matches!(step.next, NextStep::Runnable | NextStep::Yield) {
                return Ok(step);
            }
        }
        panic!("finite CPU fixture exceeded its step bound");
    }
    fn accept_admissible(&mut self, chunk: Chunk) -> ExecutionResult<StepResult> {
        if let Some(original) = &self.failure {
            return Err(original.clone());
        }
        let mut input = RawInput::for_test(chunk);
        let mut accepted = None;
        for _ in 0..100_000 {
            match self.try_push(input) {
                InputResult::Accepted(result) => {
                    accepted = Some(match result {
                        Ok(step) => step,
                        Err(failure) => return Err(self.stop_failed(failure)),
                    });
                    break;
                }
                InputResult::Rejected { input, failure } => {
                    let failure = self.stop_failed(failure);
                    drop(input);
                    return Err(failure);
                }
                InputResult::Deferred {
                    input: raw,
                    next: AdmissionNext::Yield,
                } => {
                    input = raw;
                    self.next_turn();
                }
                InputResult::Deferred { next, .. } => {
                    panic!("CPU fixture input is not admissible: {next:?}")
                }
            }
        }
        Ok(accepted.expect("finite CPU input admission"))
    }
    /// Admit once and expose all future work through drive. Cursor and memory
    /// fixtures use this when the first continuation must remain unexecuted.
    pub(crate) fn accept_without_output(&mut self, chunk: Chunk) -> ExecutionResult<()> {
        let step = self.accept_admissible(chunk)?;
        assert!(
            step.output.is_none(),
            "fixture expects deferred computation after acceptance"
        );
        Ok(())
    }
    /// Complete an admissible blocking-operator input without publishing a
    /// prefix. An accepted input is never pushed again, even when work yields.
    pub(crate) fn input_without_output(&mut self, chunk: Chunk) -> ExecutionResult<()> {
        let mut step = self.accept_admissible(chunk)?;

        loop {
            assert!(
                step.output.is_none(),
                "blocking input published a premature output"
            );
            if matches!(step.next, NextStep::NeedInput) {
                return Ok(());
            }
            assert!(
                matches!(step.next, NextStep::Runnable | NextStep::Yield),
                "blocking input must complete before EOS: {:?}",
                step.next
            );
            step = self.advance_to_boundary()?;
        }
    }
    /// Collect the published values of one finite synchronous input. Sources,
    /// async admission and root-covered inputs use their explicit native calls.
    pub(crate) fn input_and_collect(&mut self, chunk: Chunk) -> ExecutionResult<Vec<Chunk>> {
        let mut step = self.accept_admissible(chunk)?;
        let mut output = Vec::new();
        loop {
            if let Some(chunk) = step.output {
                output.push(chunk);
            }
            match step.next {
                NextStep::NeedInput | NextStep::Finished => return Ok(output),
                NextStep::Runnable | NextStep::Yield => step = self.advance_to_boundary()?,
                other => panic!("finite CPU input needs an external event: {other:?}"),
            }
        }
    }
    /// The caller already signaled EOS, or owns a finite source with no input.
    /// Collected chunks are published caller values, not operator-side buffering.
    pub(crate) fn collect_until_finished(&mut self) -> ExecutionResult<Vec<Chunk>> {
        let mut output = Vec::new();
        loop {
            let step = self.advance_to_boundary()?;
            if let Some(chunk) = step.output {
                output.push(chunk);
            }
            match step.next {
                NextStep::Finished => return Ok(output),
                NextStep::Runnable | NextStep::Yield => {}
                other => panic!("finite CPU finalization/source did not complete: {other:?}"),
            }
        }
    }
}
