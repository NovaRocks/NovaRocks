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
//!
//! The adapter owns accepted input and preparation, not an upstream edge. A
//! custom credited/asynchronous operator may implement StepOperator directly.

use super::{
    driver_step::DriverStep,
    input::{AcceptedInput, InputPreparationFailure, PreparedInput, RawInput},
    step::{AdmissionNext, InputResult, NextStep, StepResult},
};
use crate::runtime::task::ExecutionResult;

/// Normal downstream demand withdrawal can stop data work independently of
/// blocked input/output or the turn's row allowance. It is not a Task verdict
/// and does not prove physical exit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlStep {
    /// The existing work state remains authoritative.
    Active,
    /// Control made receiver-owned continuation/finalization work runnable.
    /// No payload is produced and no input is accepted by this observation.
    Runnable,
    /// No more data work is owed. Physical exits remain independently owned.
    Withdrawn,
}

pub trait StepOperator: Send {
    #[cfg(novarocks_dev_controls)]
    fn is_mark_join_probe(&self) -> bool {
        false
    }

    #[cfg(test)]
    fn try_push_for_test(
        &mut self,
        state: &crate::runtime::runtime_state::RuntimeState,
        input: RawInput,
    ) -> InputResult {
        super::driver_step::with_test_step(state, |ctx| self.try_push(ctx, input))
    }
    #[cfg(test)]
    fn drive_for_test(
        &mut self,
        state: &crate::runtime::runtime_state::RuntimeState,
    ) -> ExecutionResult<StepResult> {
        super::driver_step::with_test_step(state, |ctx| self.drive(ctx))
    }
    #[cfg(test)]
    fn finish_for_test(
        &mut self,
        state: &crate::runtime::runtime_state::RuntimeState,
    ) -> ExecutionResult<()> {
        super::driver_step::with_test_step(state, |ctx| self.finish(ctx))
    }
    fn poll_control(&mut self, _ctx: &mut DriverStep<'_>) -> ExecutionResult<ControlStep> {
        Ok(ControlStep::Active)
    }
    /// Installed control endpoints keep their identity through retirement.
    /// Per-operation dependencies instead travel in their reported WaitFact.
    fn control_observable(&self) -> Option<std::sync::Arc<super::schedule::observer::Observable>> {
        None
    }
    /// Ordinary outputs have their own allocation owner. A root receiver
    /// overrides this to acquire its finite input overlap before production.
    fn reserve_upstream(
        &mut self,
        _ctx: &mut DriverStep<'_>,
    ) -> ExecutionResult<super::step::UpstreamPull> {
        Ok(super::step::UpstreamPull::Uncovered)
    }
    /// Only an exact root materializer uses this borrowed grant for allocation.
    /// Ordinary producers retain their own funding and finish in one call.
    fn drive_with_root_input(
        &mut self,
        ctx: &mut DriverStep<'_>,
        _input: &crate::runtime::task::io::RootInputPermit,
    ) -> ExecutionResult<super::step::RootStep> {
        self.drive(ctx).map(super::step::RootStep::Complete)
    }
    /// This destroys only unpublished root-covered scratch, synchronously.
    fn release_root_workspace(&mut self) {}
    /// Explicit initial dataflow state, before the first work call.
    fn initial_step(&self) -> NextStep {
        NextStep::Runnable
    }
    fn try_push(&mut self, ctx: &mut DriverStep<'_>, input: RawInput) -> InputResult;
    fn drive(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult>;
    /// Signal input EOS once. Payload-producing finalization advances only
    /// through admitted drive. An explicit no-payload EOS control may finish
    /// data work without another output/root grant; physical exit is separate.
    fn finish(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<()>;
}

/// Preparation state, including any scratch/plan, belongs to this receiver.
/// An operation that waits returns its original accepted responsibility.
pub enum PreparationNext {
    Runnable,
    Yield,
    Wait(super::step::WaitFact),
}
impl From<PreparationNext> for NextStep {
    fn from(next: PreparationNext) -> Self {
        match next {
            PreparationNext::Runnable => Self::Runnable,
            PreparationNext::Yield => Self::Yield,
            PreparationNext::Wait(wait) => Self::Wait(wait),
        }
    }
}

pub enum PreparationStep {
    Ready(PreparedInput),
    Pending {
        input: AcceptedInput,
        next: PreparationNext,
    },
}

// Design: ADR-0187 (docs/adr/ADR-0187-receiver-owned-steps-and-single-task-conclusion.md)
pub trait InputPreparation<K>: Send {
    /// None means the exact raw input may be accepted. This does not authorize
    /// every later allocation performed by preparation or calculation.
    fn admission(
        &mut self,
        ctx: &mut DriverStep<'_>,
        input: &RawInput,
        kernel: &K,
    ) -> ExecutionResult<Option<AdmissionNext>>;
    fn accept(&mut self, input: RawInput) -> AcceptedInput;
    fn prepare(
        &mut self,
        ctx: &mut DriverStep<'_>,
        input: AcceptedInput,
        kernel: &K,
    ) -> Result<PreparationStep, InputPreparationFailure>;
    fn creates_typed_storage(&self) -> bool {
        false
    }
    fn set_profiles(&mut self, _profiles: crate::runtime::profile::OperatorProfiles) {}
    fn set_mem_tracker(
        &mut self,
        _tracker: std::sync::Arc<crate::runtime::mem_tracker::MemTracker>,
    ) {
    }
    fn set_construction_factory(
        &mut self,
        _factory: std::sync::Arc<dyn super::operator::InputConstructionFactory>,
    ) {
    }
}

/// The accepted raw input has exactly one owner until preparation transfers
/// it to computation. Stateful and asynchronous shapes reuse this slot while
/// retaining their own computation cursor and private preparation capability.
pub struct ReceiverSlot<P> {
    // Responsibility drops before the capability that can prepare it.
    accepted: Option<AcceptedInput>,
    admission_next: Option<AdmissionNext>,
    preparation: P,
}

pub enum ReceiverPreparation {
    Empty,
    Ready(PreparedInput),
    Pending(NextStep),
}

impl<P> ReceiverSlot<P> {
    pub fn new(preparation: P) -> Self {
        Self {
            accepted: None,
            admission_next: None,
            preparation,
        }
    }

    /// The public step owner performs the checkpoint and determines whether
    /// its current computation can accept another input before entering here.
    pub fn try_accept<K>(
        &mut self,
        ctx: &mut DriverStep<'_>,
        input: RawInput,
        kernel: &K,
    ) -> InputResult
    where
        P: InputPreparation<K>,
    {
        if self.accepted.is_some() {
            return InputResult::Deferred {
                input,
                next: AdmissionNext::ResumeExisting,
            };
        }
        if let Some(AdmissionNext::Wait(wait)) = &self.admission_next
            && !wait.is_ready()
        {
            return InputResult::Deferred {
                input,
                next: AdmissionNext::Wait(wait.clone()),
            };
        }
        self.admission_next = None;
        match self.preparation.admission(ctx, &input, kernel) {
            Err(failure) => InputResult::Rejected { input, failure },
            Ok(Some(next)) => {
                self.admission_next = Some(next.clone());
                InputResult::Deferred { input, next }
            }
            Ok(None) => {
                self.accepted = Some(self.preparation.accept(input));
                InputResult::Accepted(Ok(StepResult::new(None, NextStep::Runnable, 0)))
            }
        }
    }

    pub fn prepare<K>(
        &mut self,
        ctx: &mut DriverStep<'_>,
        kernel: &K,
    ) -> ExecutionResult<ReceiverPreparation>
    where
        P: InputPreparation<K>,
    {
        let Some(input) = self.accepted.take() else {
            return Ok(ReceiverPreparation::Empty);
        };
        match self.preparation.prepare(ctx, input, kernel) {
            Ok(PreparationStep::Ready(input)) => Ok(ReceiverPreparation::Ready(input)),
            Ok(PreparationStep::Pending { input, next }) => {
                self.accepted = Some(input);
                Ok(ReceiverPreparation::Pending(next.into()))
            }
            Err(InputPreparationFailure { input, failure }) => {
                self.accepted = Some(input);
                Err(failure)
            }
        }
    }

    pub(crate) fn preparation_mut(&mut self) -> &mut P {
        &mut self.preparation
    }

    pub fn is_occupied(&self) -> bool {
        self.accepted.is_some()
    }
    /// Transfer the original accepted carrier into its owner's exit
    /// continuation without invoking a Chunk or permit destructor here.
    pub(crate) fn take_accepted_for_exit(&mut self) -> Option<AcceptedInput> {
        self.admission_next = None;
        self.accepted.take()
    }
    pub fn discard(&mut self) {
        drop(self.take_accepted_for_exit());
    }
}

impl ReceiverSlot<RootInputPreparation> {
    /// Root preparation is a pure ownership move, with no hydration or foreign
    /// destructor. Exit must retain its accepted original in the Root Sink's
    /// explicit input continuation, not discard RawInput and its permit together.
    pub(crate) fn take_original_for_exit(
        &mut self,
    ) -> Option<crate::runtime::task::io::RootResultInput> {
        self.take_accepted_for_exit().map(|input| {
            let (chunk, permit) = input.into_exit_parts();
            crate::runtime::task::io::RootResultInput::new(
                chunk,
                permit.expect("accepted Root input retains its original permit"),
            )
        })
    }
}

#[derive(Clone, Copy)]
pub enum TransformStart {
    NeedInput,
    Ready,
    Finished,
}

/// EOS control after every accepted input and computation continuation drained.
/// This contains no payload: data-producing finalization still needs an admitted
/// output step. Neither variant proves physical owner exit or a Task conclusion.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransformInputEnd {
    NeedsOutputStep,
    Finished,
}

pub trait TransformKernel: Send {
    #[cfg(novarocks_dev_controls)]
    fn is_mark_join_probe(&self) -> bool {
        false
    }

    /// Read-only receiver state from this exact wrapper. A kernel can stop
    /// future demand only after accepted preparation has drained; an empty
    /// kernel cursor alone is not such a proof.
    fn poll_receiver_control(
        &mut self,
        ctx: &mut DriverStep<'_>,
        _receiver_empty: bool,
    ) -> ExecutionResult<ControlStep> {
        self.poll_control(ctx)
    }
    fn poll_control(&mut self, _ctx: &mut DriverStep<'_>) -> ExecutionResult<ControlStep> {
        Ok(ControlStep::Active)
    }
    fn control_observable(&self) -> Option<std::sync::Arc<super::schedule::observer::Observable>> {
        None
    }
    fn start(&self) -> TransformStart {
        TransformStart::NeedInput
    }
    /// Inspect readiness with the raw envelope still owned by the edge. Only
    /// metadata is public here; prepared columns are available in apply.
    fn admission(
        &self,
        _ctx: &mut DriverStep<'_>,
        _input: &RawInput,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        Ok(None)
    }
    fn apply(
        &mut self,
        ctx: &mut DriverStep<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult>;
    fn drive(&mut self, _ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        Ok(StepResult::new(None, NextStep::NeedInput, 0))
    }
    /// Once-only EOS metadata. No input is accepted, columns read, storage
    /// materialized, or output produced here. A kernel with final output must
    /// retain the conservative result and use finish/drive after admission.
    fn end_input(&mut self, _ctx: &mut DriverStep<'_>) -> ExecutionResult<TransformInputEnd> {
        Ok(TransformInputEnd::NeedsOutputStep)
    }
    fn finish(&mut self, _ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        Ok(StepResult::new(None, NextStep::Finished, 0))
    }
}

/// One receiver slot. Multi-prefix kernels retain their own row continuation;
/// they signal NeedInput only after that responsibility has completed.
pub struct Transform<K, P> {
    receiver: ReceiverSlot<P>,
    kernel: K,
    needs_input: bool,
    finishing: bool,
    finish_started: bool,
    end_input_started: bool,
    next: NextStep,
    failure: Option<crate::runtime::task::ExecutionFailure>,
}
impl<K: TransformKernel, P: InputPreparation<K>> Transform<K, P> {
    pub fn new(kernel: K, preparation: P) -> Self {
        let next = match kernel.start() {
            TransformStart::NeedInput => NextStep::NeedInput,
            TransformStart::Ready => NextStep::Runnable,
            TransformStart::Finished => NextStep::Finished,
        };
        let needs_input = matches!(next, NextStep::NeedInput);
        Self {
            receiver: ReceiverSlot::new(preparation),
            kernel,
            needs_input,
            finishing: false,
            finish_started: false,
            end_input_started: false,
            next,
            failure: None,
        }
    }
    #[cfg(test)]
    pub(crate) fn kernel_for_test(&self) -> &K {
        &self.kernel
    }
    #[cfg(test)]
    pub(crate) fn kernel_for_test_mut(&mut self) -> &mut K {
        &mut self.kernel
    }
    fn stop_work(
        &mut self,
        failure: crate::runtime::task::ExecutionFailure,
    ) -> crate::runtime::task::ExecutionFailure {
        let failure = self.failure.get_or_insert(failure).clone();
        self.next = NextStep::Finished;
        self.needs_input = false;
        failure
    }
    fn end_input_control(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<ControlStep> {
        if !self.finishing
            || !self.needs_input
            || self.receiver.is_occupied()
            || self.finish_started
            || self.end_input_started
        {
            return Ok(ControlStep::Active);
        }
        // Save the once-only phase before invoking the actual kernel author.
        self.end_input_started = true;
        let end = ctx.perform(crate::runtime::task::PipelineOperation::Finishing, |ctx| {
            self.kernel.end_input(ctx)
        })?;
        match end {
            TransformInputEnd::NeedsOutputStep => Ok(ControlStep::Active),
            TransformInputEnd::Finished => {
                self.finish_started = true;
                self.needs_input = false;
                self.next = NextStep::Finished;
                Ok(ControlStep::Withdrawn)
            }
        }
    }
    fn record(&mut self, mut step: StepResult) -> StepResult {
        self.needs_input = matches!(step.next, NextStep::NeedInput);
        if self.finishing && self.needs_input {
            step.next = NextStep::Runnable;
        }
        self.next = step.next.clone();
        step
    }
}
impl<K: TransformKernel, P: InputPreparation<K>> StepOperator for Transform<K, P> {
    #[cfg(novarocks_dev_controls)]
    fn is_mark_join_probe(&self) -> bool {
        self.kernel.is_mark_join_probe()
    }

    fn poll_control(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<ControlStep> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        if matches!(self.next, NextStep::Finished) {
            return Ok(ControlStep::Active);
        }
        let result = self
            .kernel
            .poll_receiver_control(ctx, !self.receiver.is_occupied())
            .and_then(|control| match control {
                ControlStep::Active => self.end_input_control(ctx),
                other => Ok(other),
            });
        match result {
            Ok(ControlStep::Withdrawn) => {
                self.receiver.discard();
                self.needs_input = false;
                self.next = NextStep::Finished;
                Ok(ControlStep::Withdrawn)
            }
            Ok(ControlStep::Runnable) => {
                self.needs_input = false;
                self.next = NextStep::Runnable;
                Ok(ControlStep::Runnable)
            }
            Ok(ControlStep::Active) => Ok(ControlStep::Active),
            Err(error) => {
                ctx.report_failure(error.clone());
                Err(self.stop_work(error))
            }
        }
    }
    fn control_observable(&self) -> Option<std::sync::Arc<super::schedule::observer::Observable>> {
        self.kernel.control_observable()
    }
    fn initial_step(&self) -> NextStep {
        self.next.clone()
    }
    fn try_push(&mut self, ctx: &mut DriverStep<'_>, input: RawInput) -> InputResult {
        if let Some(failure) = &self.failure {
            return InputResult::Rejected {
                input,
                failure: failure.clone(),
            };
        }
        if let Err(failure) = ctx.checkpoint() {
            return InputResult::Rejected { input, failure };
        }
        if self.finishing || matches!(self.next, NextStep::Finished) {
            return InputResult::Rejected {
                input,
                failure: crate::runtime::task::ExecutionFailure::from_domain(
                    hydration::ReceiverContractFailure::InputAfterFinish,
                ),
            };
        }
        if !self.needs_input {
            return InputResult::Deferred {
                input,
                next: AdmissionNext::ResumeExisting,
            };
        }
        let result = self.receiver.try_accept(ctx, input, &self.kernel);
        if matches!(result, InputResult::Accepted(_)) {
            self.needs_input = false;
            self.next = NextStep::Runnable;
        }
        result
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
        use crate::runtime::task::PipelineOperation;
        let result = (|| -> ExecutionResult<StepResult> {
            let prepared = ctx.perform(PipelineOperation::Preparation, |ctx| {
                self.receiver.prepare(ctx, &self.kernel)
            })?;
            match prepared {
                ReceiverPreparation::Ready(input) => {
                    ctx.perform(PipelineOperation::Push, |ctx| self.kernel.apply(ctx, input))
                }
                ReceiverPreparation::Pending(next) => Ok(StepResult::new(None, next, 0)),
                ReceiverPreparation::Empty
                    if self.finishing && self.needs_input && !self.finish_started =>
                {
                    if matches!(self.end_input_control(ctx)?, ControlStep::Withdrawn) {
                        return Ok(StepResult::new(None, NextStep::Finished, 0));
                    }
                    self.finish_started = true;
                    ctx.perform(PipelineOperation::Finishing, |ctx| self.kernel.finish(ctx))
                }
                ReceiverPreparation::Empty => {
                    ctx.perform(PipelineOperation::Pull, |ctx| self.kernel.drive(ctx))
                }
            }
        })();
        match result {
            Ok(step) => Ok(self.record(step)),
            Err(failure) => Err(self.stop_work(failure)),
        }
    }
    fn finish(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<()> {
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        ctx.checkpoint()?;
        self.finishing = true;
        if self.needs_input {
            self.next = NextStep::Runnable;
        }
        Ok(())
    }
}

#[path = "shape/child.rs"]
pub(crate) mod child;

#[path = "shape/fanout.rs"]
pub(crate) mod fanout;

#[path = "shape/hydration.rs"]
mod hydration;
pub use hydration::HydrateInput;
pub(crate) use hydration::{RootInputPreparation, RootInputTarget};
#[path = "shape/transform.rs"]
mod transform;

#[path = "shape/sink.rs"]
mod sink;
pub use sink::{SinkKernel, SinkStep};

#[path = "shape/source.rs"]
mod source;
pub use source::{Source, SourceKernel, SourceStart};
pub(crate) use source::{SourceContractFailure, SourceRead, source_contract_failure};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema},
        exec::pipeline::driver_step::TurnWork,
        runtime::runtime_state::RuntimeErrorState,
    };
    use arrow::{
        array::{ArrayRef, Int32Array},
        datatypes::{DataType, Field},
    };
    use novarocks_types::SlotId;
    use std::{
        num::NonZeroU64,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    pub(super) fn chunk() -> Chunk {
        let schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("value", DataType::Int32, false),
                None,
                None,
            )])
            .unwrap(),
        );
        Chunk::try_new_with_columns(
            schema,
            vec![Arc::new(Int32Array::from(vec![7, 11])) as ArrayRef],
        )
        .unwrap()
    }
    #[derive(Default)]
    struct Counts {
        accepted: AtomicUsize,
        prepared: AtomicUsize,
        applied: AtomicUsize,
        finished: AtomicUsize,
    }
    struct Prepare {
        counts: Arc<Counts>,
        refuse: bool,
        pause: bool,
        fail: bool,
    }
    impl<K: Send> InputPreparation<K> for Prepare {
        fn admission(
            &mut self,
            _: &mut DriverStep<'_>,
            _: &RawInput,
            _: &K,
        ) -> ExecutionResult<Option<AdmissionNext>> {
            Ok(self.refuse.then_some(AdmissionNext::Yield))
        }
        fn accept(&mut self, input: RawInput) -> AcceptedInput {
            self.counts.accepted.fetch_add(1, Ordering::SeqCst);
            input.accept(None)
        }
        fn prepare(
            &mut self,
            _: &mut DriverStep<'_>,
            input: AcceptedInput,
            _: &K,
        ) -> Result<PreparationStep, InputPreparationFailure> {
            if std::mem::take(&mut self.pause) {
                return Ok(PreparationStep::Pending {
                    input,
                    next: PreparationNext::Yield,
                });
            }
            self.counts.prepared.fetch_add(1, Ordering::SeqCst);
            input
                .prepare(|chunk| {
                    if self.fail {
                        Err(novarocks_functions::KernelFailure::ResourceExhausted.into())
                    } else {
                        Ok(chunk.clone())
                    }
                })
                .map(PreparationStep::Ready)
        }
    }
    struct Kernel {
        counts: Arc<Counts>,
    }
    impl TransformKernel for Kernel {
        fn apply(
            &mut self,
            _: &mut DriverStep<'_>,
            input: PreparedInput,
        ) -> ExecutionResult<StepResult> {
            self.counts.applied.fetch_add(1, Ordering::SeqCst);
            let (chunk, permit) = input.into_parts();
            assert!(permit.is_none());
            Ok(StepResult::new(Some(chunk), NextStep::NeedInput, 2))
        }
        fn drive(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
            Ok(StepResult::new(None, NextStep::Finished, 0))
        }
        fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
            self.counts.finished.fetch_add(1, Ordering::SeqCst);
            Ok(StepResult::new(None, NextStep::Runnable, 0))
        }
    }
    fn transform(
        counts: &Arc<Counts>,
        refuse: bool,
        pause: bool,
        fail: bool,
    ) -> Transform<Kernel, Prepare> {
        Transform::new(
            Kernel {
                counts: counts.clone(),
            },
            Prepare {
                counts: counts.clone(),
                refuse,
                pause,
                fail,
            },
        )
    }
    #[test]
    fn refusal_returns_the_original_without_preparation_or_acceptance() {
        let counts = Arc::new(Counts::default());
        let mut operator = transform(&counts, true, false, false);
        let control = RuntimeErrorState::default();
        let mut work = TurnWork::new(NonZeroU64::MIN, 10);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = DriverStep::new(&control, &mut work, &mut value_conversions);
        let original = chunk();
        let address = original.columns()[0].as_ref() as *const _;
        let mut input = RawInput::new(original, None);
        for _ in 0..3 {
            let InputResult::Deferred {
                input: returned, ..
            } = operator.try_push(&mut ctx, input)
            else {
                panic!("input was accepted")
            };
            assert!(std::ptr::addr_eq(
                returned.original().columns()[0].as_ref() as *const _,
                address
            ));
            input = returned;
        }
        assert_eq!(counts.accepted.load(Ordering::SeqCst), 0);
        assert_eq!(counts.prepared.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn preparation_yield_resumes_one_accepted_input_and_finishes_once() {
        let counts = Arc::new(Counts::default());
        let mut operator = transform(&counts, false, true, false);
        let control = RuntimeErrorState::default();
        let mut work = TurnWork::new(NonZeroU64::MIN, 10);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = DriverStep::new(&control, &mut work, &mut value_conversions);
        assert!(matches!(
            operator.try_push(&mut ctx, RawInput::new(chunk(), None)),
            InputResult::Accepted(Ok(_))
        ));
        assert!(matches!(
            operator.drive(&mut ctx).unwrap().next,
            NextStep::Yield
        ));
        assert!(matches!(
            operator.try_push(&mut ctx, RawInput::new(chunk(), None)),
            InputResult::Deferred {
                next: AdmissionNext::ResumeExisting,
                ..
            }
        ));
        operator.finish(&mut ctx).unwrap();
        let output = operator.drive(&mut ctx).unwrap();
        assert!(matches!(output.next, NextStep::Runnable));
        let values = output
            .output
            .unwrap()
            .column_by_slot_id(SlotId::new(1))
            .unwrap();
        assert_eq!(
            values
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[7, 11]
        );
        operator.drive(&mut ctx).unwrap();
        operator.finish(&mut ctx).unwrap();
        operator.drive(&mut ctx).unwrap();
        assert_eq!(counts.accepted.load(Ordering::SeqCst), 1);
        assert_eq!(counts.prepared.load(Ordering::SeqCst), 1);
        assert_eq!(counts.applied.load(Ordering::SeqCst), 1);
        assert_eq!(counts.finished.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn acceptance_and_preparation_keep_one_accounting_responsibility() {
        let tracker = crate::runtime::mem_tracker::MemTracker::new_root("receiver");
        let raw = RawInput::new(chunk(), None);
        assert_eq!(tracker.current(), 0);
        let accepted = raw.accept(Some(&tracker));
        let held = tracker.current();
        assert!(held > 0);
        let prepared = match accepted.prepare(|original| Ok(original.clone())) {
            Ok(prepared) => prepared,
            Err(_) => panic!("preparation failed"),
        };
        assert_eq!(tracker.current(), held);
        drop(prepared);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn preparation_failure_keeps_accepted_responsibility_in_the_receiver() {
        let counts = Arc::new(Counts::default());
        let mut operator = transform(&counts, false, false, true);
        let control = RuntimeErrorState::default();
        let mut work = TurnWork::new(NonZeroU64::MIN, 10);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = DriverStep::new(&control, &mut work, &mut value_conversions);
        assert!(matches!(
            operator.try_push(&mut ctx, RawInput::new(chunk(), None)),
            InputResult::Accepted(_)
        ));
        let first = operator.drive(&mut ctx).unwrap_err();
        assert!(operator.receiver.is_occupied());
        assert_eq!(operator.drive(&mut ctx).unwrap_err(), first);
        assert_eq!(operator.finish(&mut ctx).unwrap_err(), first);
        let second = chunk();
        let original = second.columns()[0].clone();
        let InputResult::Rejected {
            input: returned,
            failure,
        } = operator.try_push(&mut ctx, RawInput::new(second, None))
        else {
            panic!("a failed receiver cannot accept or defer more input");
        };
        assert_eq!(failure, first);
        assert!(Arc::ptr_eq(&original, &returned.original().columns()[0]));
        assert_eq!(counts.prepared.load(Ordering::SeqCst), 1);
        assert!(
            control.error().is_none(),
            "a work result cache does not accept a Task conclusion"
        );
        assert_eq!(counts.accepted.load(Ordering::SeqCst), 1);
        assert_eq!(counts.applied.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn work_failure_stops_reentry_and_rejects_the_same_unaccepted_raw_without_a_task_decision() {
        struct FailingKernel {
            calls: Arc<AtomicUsize>,
            original: crate::runtime::task::ExecutionFailure,
        }
        impl TransformKernel for FailingKernel {
            fn apply(
                &mut self,
                _: &mut DriverStep<'_>,
                _: PreparedInput,
            ) -> ExecutionResult<StepResult> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Err(self.original.clone())
            }
            fn drive(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
                panic!("a failed input must never enter a continuation");
            }
            fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
                panic!("a failed input must never finalize");
            }
        }
        let counts = Arc::new(Counts::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let original = crate::runtime::task::ExecutionFailure::from(
            novarocks_functions::KernelFailure::ResourceExhausted,
        );
        let mut operator = Transform::new(
            FailingKernel {
                calls: calls.clone(),
                original: original.clone(),
            },
            Prepare {
                counts: counts.clone(),
                refuse: false,
                pause: false,
                fail: false,
            },
        );
        let control = RuntimeErrorState::default();
        let mut work = TurnWork::new(NonZeroU64::MIN, 10);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = DriverStep::new(&control, &mut work, &mut value_conversions);
        assert!(matches!(
            operator.try_push(&mut ctx, RawInput::new(chunk(), None)),
            InputResult::Accepted(Ok(_))
        ));
        assert_eq!(operator.drive(&mut ctx).unwrap_err(), original);
        assert_eq!(operator.drive(&mut ctx).unwrap_err(), original);
        assert_eq!(operator.finish(&mut ctx).unwrap_err(), original);
        let input = chunk();
        let source = input.columns()[0].clone();
        let InputResult::Rejected {
            input: returned,
            failure,
        } = operator.try_push(&mut ctx, RawInput::new(input, None))
        else {
            panic!("the failed work scope must refuse another raw input");
        };
        assert_eq!(failure, original);
        assert!(Arc::ptr_eq(&source, &returned.original().columns()[0]));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(counts.accepted.load(Ordering::SeqCst), 1);
        assert_eq!(counts.prepared.load(Ordering::SeqCst), 1);
        assert!(control.error().is_none());
    }
    struct WaitingKernel {
        event: Arc<super::super::schedule::observer::Observable>,
        counts: Arc<Counts>,
    }
    impl TransformKernel for WaitingKernel {
        fn apply(
            &mut self,
            _: &mut DriverStep<'_>,
            input: PreparedInput,
        ) -> ExecutionResult<StepResult> {
            self.counts.applied.fetch_add(1, Ordering::SeqCst);
            Ok(StepResult::new(
                Some(input.into_uncovered_chunk()?),
                NextStep::Wait(super::super::step::WaitFact {
                    reason: super::super::step::WaitReason::Dependency,
                    observed_generation: self.event.generation(),
                    observable: self.event.clone(),
                    deadline: None,
                }),
                2,
            ))
        }
        fn drive(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
            self.counts.applied.fetch_add(1, Ordering::SeqCst);
            Ok(StepResult::new(Some(chunk()), NextStep::NeedInput, 0))
        }
        fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
            self.counts.finished.fetch_add(1, Ordering::SeqCst);
            Ok(StepResult::new(None, NextStep::Finished, 0))
        }
    }
    #[test]
    fn eos_preserves_an_existing_wait_and_finishes_only_after_the_continuation_drains() {
        let event = Arc::new(super::super::schedule::observer::Observable::new());
        let counts = Arc::new(Counts::default());
        let mut operator = Transform::new(
            WaitingKernel {
                event: event.clone(),
                counts: counts.clone(),
            },
            Prepare {
                counts: counts.clone(),
                refuse: false,
                pause: false,
                fail: false,
            },
        );
        let control = RuntimeErrorState::default();
        let mut turn = TurnWork::new(NonZeroU64::MIN, 10);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = DriverStep::new(&control, &mut turn, &mut value_conversions);
        assert!(matches!(
            operator.try_push(&mut ctx, RawInput::new(chunk(), None)),
            InputResult::Accepted(Ok(_))
        ));
        let first = operator.drive(&mut ctx).unwrap();
        assert!(first.output.is_some());
        assert!(matches!(first.next, NextStep::Wait(_)));
        operator.finish(&mut ctx).unwrap();
        for _ in 0..3 {
            let waiting = operator.drive(&mut ctx).unwrap();
            assert!(waiting.output.is_none());
            assert!(matches!(waiting.next, NextStep::Wait(_)));
        }
        assert_eq!(counts.applied.load(Ordering::SeqCst), 1);
        assert_eq!(counts.finished.load(Ordering::SeqCst), 0);
        event.notify_observers();
        let last_output = operator.drive(&mut ctx).unwrap();
        assert!(last_output.output.is_some());
        assert!(matches!(last_output.next, NextStep::Runnable));
        let ended = operator.drive(&mut ctx).unwrap();
        assert!(ended.output.is_none());
        assert!(matches!(ended.next, NextStep::Finished));
        operator.finish(&mut ctx).unwrap();
        assert!(matches!(
            operator.drive(&mut ctx).unwrap().next,
            NextStep::Finished
        ));
        assert_eq!(counts.applied.load(Ordering::SeqCst), 2);
        assert_eq!(counts.finished.load(Ordering::SeqCst), 1);
        assert_eq!(counts.prepared.load(Ordering::SeqCst), 1);
    }
    struct NoOutputEnd {
        counts: Arc<Counts>,
        ends: Arc<AtomicUsize>,
    }
    impl TransformKernel for NoOutputEnd {
        fn apply(
            &mut self,
            _: &mut DriverStep<'_>,
            input: PreparedInput,
        ) -> ExecutionResult<StepResult> {
            self.counts.applied.fetch_add(1, Ordering::SeqCst);
            Ok(StepResult::new(
                Some(input.into_uncovered_chunk()?),
                NextStep::NeedInput,
                2,
            ))
        }
        fn end_input(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<TransformInputEnd> {
            self.ends.fetch_add(1, Ordering::SeqCst);
            Ok(TransformInputEnd::Finished)
        }
        fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
            panic!("no-output EOS must not invoke payload finalization")
        }
    }
    #[test]
    fn no_output_eos_waits_for_accepted_preparation_then_publishes_once_without_output_work() {
        let counts = Arc::new(Counts::default());
        let ends = Arc::new(AtomicUsize::new(0));
        let mut operator = Transform::new(
            NoOutputEnd {
                counts: counts.clone(),
                ends: ends.clone(),
            },
            Prepare {
                counts: counts.clone(),
                refuse: false,
                pause: true,
                fail: false,
            },
        );
        let control = RuntimeErrorState::default();
        let mut turn = TurnWork::new(NonZeroU64::MIN, 10);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = DriverStep::new(&control, &mut turn, &mut value_conversions);
        assert!(matches!(
            operator.try_push(&mut ctx, RawInput::new(chunk(), None)),
            InputResult::Accepted(Ok(_))
        ));
        operator.finish(&mut ctx).unwrap();
        assert!(matches!(
            operator.poll_control(&mut ctx).unwrap(),
            ControlStep::Active
        ));
        assert_eq!(ends.load(Ordering::SeqCst), 0);
        assert!(matches!(
            operator.drive(&mut ctx).unwrap().next,
            NextStep::Yield
        ));
        assert!(matches!(
            operator.poll_control(&mut ctx).unwrap(),
            ControlStep::Active
        ));
        assert_eq!(ends.load(Ordering::SeqCst), 0);
        let last = operator.drive(&mut ctx).unwrap();
        assert!(last.output.is_some());
        assert_eq!(counts.accepted.load(Ordering::SeqCst), 1);
        assert_eq!(counts.prepared.load(Ordering::SeqCst), 1);
        assert_eq!(counts.applied.load(Ordering::SeqCst), 1);
        assert!(matches!(
            operator.poll_control(&mut ctx).unwrap(),
            ControlStep::Withdrawn
        ));
        assert!(matches!(
            operator.poll_control(&mut ctx).unwrap(),
            ControlStep::Active
        ));
        assert_eq!(ends.load(Ordering::SeqCst), 1);
        assert!(operator.drive(&mut ctx).unwrap().output.is_none());
        assert_eq!(ends.load(Ordering::SeqCst), 1);
    }

    struct ProducingEnd(Arc<AtomicUsize>);
    impl TransformKernel for ProducingEnd {
        fn apply(
            &mut self,
            _: &mut DriverStep<'_>,
            _: PreparedInput,
        ) -> ExecutionResult<StepResult> {
            panic!("this finalizer has no input")
        }
        fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(StepResult::new(Some(chunk()), NextStep::Finished, 2))
        }
    }
    #[test]
    fn conservative_eos_control_does_not_run_a_payload_producing_finalizer() {
        let counts = Arc::new(Counts::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let mut operator = Transform::new(
            ProducingEnd(calls.clone()),
            Prepare {
                counts,
                refuse: false,
                pause: false,
                fail: false,
            },
        );
        let control = RuntimeErrorState::default();
        let mut turn = TurnWork::new(NonZeroU64::MIN, 10);
        let mut value_conversions =
            novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = DriverStep::new(&control, &mut turn, &mut value_conversions);
        operator.finish(&mut ctx).unwrap();
        for _ in 0..3 {
            assert!(matches!(
                operator.poll_control(&mut ctx).unwrap(),
                ControlStep::Active
            ));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let tail = operator.drive(&mut ctx).unwrap();
        assert_eq!(tail.output.unwrap().len(), 2);
        assert!(matches!(tail.next, NextStep::Finished));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(operator.drive(&mut ctx).unwrap().output.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn receiver_control_observes_original_accepted_preparation_until_it_drains() {
        struct Boundary {
            observations: Arc<std::sync::Mutex<Vec<bool>>>,
        }
        impl TransformKernel for Boundary {
            fn poll_receiver_control(
                &mut self,
                _: &mut DriverStep<'_>,
                empty: bool,
            ) -> ExecutionResult<ControlStep> {
                self.observations.lock().unwrap().push(empty);
                Ok(ControlStep::Active)
            }
            fn apply(
                &mut self,
                _: &mut DriverStep<'_>,
                _: PreparedInput,
            ) -> ExecutionResult<StepResult> {
                Ok(StepResult::new(None, NextStep::NeedInput, 0))
            }
        }
        let observations = Arc::new(std::sync::Mutex::new(Vec::new()));
        let counts = Arc::new(Counts::default());
        let mut operator = Transform::new(
            Boundary {
                observations: observations.clone(),
            },
            Prepare {
                counts: counts.clone(),
                refuse: false,
                pause: true,
                fail: false,
            },
        );
        let control = RuntimeErrorState::default();
        let mut turn = TurnWork::new(NonZeroU64::MIN, 10);
        let mut conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
        let mut ctx = DriverStep::new(&control, &mut turn, &mut conversions);
        operator.poll_control(&mut ctx).unwrap();
        assert!(matches!(
            operator.try_push(&mut ctx, RawInput::new(chunk(), None)),
            InputResult::Accepted(_)
        ));
        operator.poll_control(&mut ctx).unwrap();
        assert!(matches!(
            operator.drive(&mut ctx).unwrap().next,
            NextStep::Yield
        ));
        operator.poll_control(&mut ctx).unwrap();
        operator.drive(&mut ctx).unwrap();
        operator.poll_control(&mut ctx).unwrap();
        assert_eq!(*observations.lock().unwrap(), [true, false, false, true]);
        assert_eq!(counts.accepted.load(Ordering::SeqCst), 1);
        assert_eq!(counts.prepared.load(Ordering::SeqCst), 1);
    }
}
