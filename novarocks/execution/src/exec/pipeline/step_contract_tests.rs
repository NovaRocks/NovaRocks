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

//! A real Driver turn lends one allowance across operators and nested calls.

use super::{
    driver::{DriverState, PipelineDriver},
    driver_step::{DEFAULT_TURN_WORK_UNITS, DriverStep},
    input::{AcceptedInput, InputPreparationFailure, PreparedInput, RawInput},
    operator::{InputEncodingPolicy, Operator, OperatorLifecycle},
    shape::{
        InputPreparation, PreparationStep, SinkKernel, SinkStep, Source, SourceKernel, Transform,
        TransformKernel,
    },
    step::{AdmissionNext, NextStep, StepResult},
};
use crate::{
    exec::chunk::{Chunk, ChunkSchema, ChunkSlotSchema},
    runtime::{
        runtime_state::RuntimeState,
        task::{ExecutionResult, PipelineOperation},
    },
};
use arrow::{
    array::{Array, ArrayRef, Int32Array},
    datatypes::{DataType, Field},
};
use novarocks_types::SlotId;
use std::{
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Default)]
struct ReceiverCounts {
    accepted: AtomicUsize,
    prepared: AtomicUsize,
    apply: AtomicUsize,
    drive: AtomicUsize,
    finish: AtomicUsize,
}
#[derive(Debug, Eq, PartialEq)]
struct Observation {
    site: &'static str,
    generation: u64,
    remaining: u32,
}
#[derive(Default)]
struct Observed {
    source_calls: AtomicUsize,
    receivers: [ReceiverCounts; 3],
    kernel_drops: [AtomicUsize; 4],
    preparation_drops: [AtomicUsize; 3],
    trace: Mutex<Vec<Observation>>,
    output: Mutex<Vec<i32>>,
    source_array: Mutex<Option<Weak<dyn Array>>>,
}
fn observe(ctx: &mut DriverStep<'_>, observed: &Observed, site: &'static str) {
    let work = ctx.work();
    observed.trace.lock().unwrap().push(Observation {
        site,
        generation: work.generation().get(),
        remaining: work.remaining(),
    });
}

struct IdentityPreparation {
    index: usize,
    observed: Arc<Observed>,
}
impl<K: Send> InputPreparation<K> for IdentityPreparation {
    fn admission(
        &mut self,
        _: &mut DriverStep<'_>,
        _: &RawInput,
        _: &K,
    ) -> ExecutionResult<Option<AdmissionNext>> {
        Ok(None)
    }
    fn accept(&mut self, input: RawInput) -> AcceptedInput {
        self.observed.receivers[self.index]
            .accepted
            .fetch_add(1, Ordering::SeqCst);
        input.accept(None)
    }
    fn prepare(
        &mut self,
        _: &mut DriverStep<'_>,
        input: AcceptedInput,
        _: &K,
    ) -> Result<PreparationStep, InputPreparationFailure> {
        self.observed.receivers[self.index]
            .prepared
            .fetch_add(1, Ordering::SeqCst);
        input
            .prepare(|original| Ok(original.clone()))
            .map(PreparationStep::Ready)
    }
}
impl Drop for IdentityPreparation {
    fn drop(&mut self) {
        self.observed.preparation_drops[self.index].fetch_add(1, Ordering::SeqCst);
    }
}

struct OneBatch {
    values: Option<Vec<i32>>,
    observed: Arc<Observed>,
}
impl OperatorLifecycle for OneBatch {
    fn name(&self) -> &str {
        "TURN_SOURCE"
    }
}
impl SourceKernel for OneBatch {
    fn poll_output(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        self.observed.source_calls.fetch_add(1, Ordering::SeqCst);
        observe(ctx, &self.observed, "source.before");
        let values = self
            .values
            .as_ref()
            .expect("the finished source must not be repolled");
        // Charge only the actual production call, once for each source value.
        if !ctx.work().try_consume(u32::try_from(values.len()).unwrap()) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        observe(ctx, &self.observed, "source.after");
        let values = self.values.take().unwrap();
        let rows = values.len() as u64;
        let array = Arc::new(Int32Array::from(values));
        let schema = Arc::new(
            ChunkSchema::try_new(vec![ChunkSlotSchema::new_with_field(
                SlotId::new(1),
                Field::new("value", DataType::Int32, false),
                None,
                None,
            )])
            .unwrap(),
        );
        let chunk = Chunk::try_new_with_columns(schema, vec![array as ArrayRef]).unwrap();
        // Observe the actual produced carrier after its constructor's retagging.
        *self.observed.source_array.lock().unwrap() = Some(Arc::downgrade(&chunk.columns()[0]));
        Ok(StepResult::new(Some(chunk), NextStep::Finished, rows))
    }
}
impl Drop for OneBatch {
    fn drop(&mut self) {
        self.observed.kernel_drops[0].fetch_add(1, Ordering::SeqCst);
    }
}

struct NestedPass {
    observed: Arc<Observed>,
}
impl InputEncodingPolicy for NestedPass {}
impl OperatorLifecycle for NestedPass {
    fn name(&self) -> &str {
        "NESTED_PASS"
    }
}
impl TransformKernel for NestedPass {
    fn apply(
        &mut self,
        ctx: &mut DriverStep<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        self.observed.receivers[0]
            .apply
            .fetch_add(1, Ordering::SeqCst);
        observe(ctx, &self.observed, "first.before");
        {
            let mut outer = ctx.operator_step(1, PipelineOperation::Push);
            assert!(outer.work().try_consume(1));
            observe(&mut outer, &self.observed, "outer.after");
            {
                let mut inner = outer.operator_step(1, PipelineOperation::Preparation);
                observe(&mut inner, &self.observed, "inner.before");
                assert!(inner.work().try_consume(1));
                observe(&mut inner, &self.observed, "inner.after");
            }
            observe(&mut outer, &self.observed, "outer.reborrow_returned");
        }
        observe(ctx, &self.observed, "first.after");
        let chunk = input.into_uncovered_chunk()?;
        let rows = chunk.len() as u64;
        Ok(StepResult::new(Some(chunk), NextStep::NeedInput, rows))
    }
    fn drive(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        self.observed.receivers[0]
            .drive
            .fetch_add(1, Ordering::SeqCst);
        panic!("the first transform has no unfinished payload work")
    }
    fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        self.observed.receivers[0]
            .finish
            .fetch_add(1, Ordering::SeqCst);
        Ok(StepResult::new(None, NextStep::Finished, 0))
    }
}
impl Drop for NestedPass {
    fn drop(&mut self) {
        self.observed.kernel_drops[1].fetch_add(1, Ordering::SeqCst);
    }
}

struct DeferredPass {
    input: Option<PreparedInput>,
    observed: Arc<Observed>,
}
impl DeferredPass {
    fn advance(
        &mut self,
        ctx: &mut DriverStep<'_>,
        site: &'static str,
    ) -> ExecutionResult<StepResult> {
        observe(ctx, &self.observed, site);
        let mut nested = ctx.operator_step(2, PipelineOperation::Pull);
        if !nested.work().try_consume(1) {
            return Ok(StepResult::new(None, NextStep::Yield, 0));
        }
        observe(&mut nested, &self.observed, "second.completed");
        let chunk = self
            .input
            .take()
            .expect("continue the original prepared input")
            .into_uncovered_chunk()?;
        let rows = chunk.len() as u64;
        Ok(StepResult::new(Some(chunk), NextStep::NeedInput, rows))
    }
}
impl InputEncodingPolicy for DeferredPass {}
impl OperatorLifecycle for DeferredPass {
    fn name(&self) -> &str {
        "DEFERRED_PASS"
    }
}
impl TransformKernel for DeferredPass {
    fn apply(
        &mut self,
        ctx: &mut DriverStep<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        self.observed.receivers[1]
            .apply
            .fetch_add(1, Ordering::SeqCst);
        assert!(
            self.input.is_none(),
            "accept once before resuming the original work"
        );
        self.input = Some(input);
        self.advance(ctx, "second.first_turn")
    }
    fn drive(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        self.observed.receivers[1]
            .drive
            .fetch_add(1, Ordering::SeqCst);
        self.advance(ctx, "second.next_turn")
    }
    fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<StepResult> {
        self.observed.receivers[1]
            .finish
            .fetch_add(1, Ordering::SeqCst);
        assert!(
            self.input.is_none(),
            "EOS must follow the original continuation"
        );
        Ok(StepResult::new(None, NextStep::Finished, 0))
    }
}
impl Drop for DeferredPass {
    fn drop(&mut self) {
        self.observed.kernel_drops[2].fetch_add(1, Ordering::SeqCst);
    }
}

struct Collect {
    observed: Arc<Observed>,
}
impl InputEncodingPolicy for Collect {}
impl OperatorLifecycle for Collect {
    fn name(&self) -> &str {
        "TURN_COLLECT"
    }
}
impl SinkKernel for Collect {
    fn consume(
        &mut self,
        _: &mut DriverStep<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<SinkStep> {
        self.observed.receivers[2]
            .apply
            .fetch_add(1, Ordering::SeqCst);
        let chunk = input.into_uncovered_chunk()?;
        let values = chunk.column_by_slot_id(SlotId::new(1)).unwrap();
        self.observed.output.lock().unwrap().extend_from_slice(
            values
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
        );
        Ok(SinkStep::new(NextStep::NeedInput, chunk.len() as u64))
    }
    fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<SinkStep> {
        self.observed.receivers[2]
            .finish
            .fetch_add(1, Ordering::SeqCst);
        Ok(SinkStep::new(NextStep::Finished, 0))
    }
}
impl Drop for Collect {
    fn drop(&mut self) {
        self.observed.kernel_drops[3].fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn actual_driver_turn_shares_allowance_across_operators_and_nested_reborrows_without_repreparing() {
    let expected = (0..DEFAULT_TURN_WORK_UNITS - 2)
        .map(|i| i as i32 * 3 + 7)
        .collect::<Vec<_>>();
    let observed = Arc::new(Observed::default());
    let preparation = |index| IdentityPreparation {
        index,
        observed: observed.clone(),
    };
    let operators: Vec<Box<dyn Operator>> = vec![
        Box::new(Source::new(OneBatch {
            values: Some(expected.clone()),
            observed: observed.clone(),
        })),
        Box::new(Transform::new(
            NestedPass {
                observed: observed.clone(),
            },
            preparation(0),
        )),
        Box::new(Transform::new(
            DeferredPass {
                input: None,
                observed: observed.clone(),
            },
            preparation(1),
        )),
        Box::new(Transform::new(
            Collect {
                observed: observed.clone(),
            },
            preparation(2),
        )),
    ];
    let state = Arc::new(RuntimeState::default());
    let mut driver = PipelineDriver::new(71, operators, None, vec![], state.clone(), None);

    assert_eq!(driver.process(Duration::from_secs(1)), DriverState::Ready);
    assert!(state.error().is_none());
    assert!(observed.output.lock().unwrap().is_empty());
    assert_eq!(observed.source_calls.load(Ordering::SeqCst), 1);
    for receiver in &observed.receivers[..2] {
        assert_eq!(receiver.accepted.load(Ordering::SeqCst), 1);
        assert_eq!(receiver.prepared.load(Ordering::SeqCst), 1);
        assert_eq!(receiver.apply.load(Ordering::SeqCst), 1);
        assert_eq!(receiver.drive.load(Ordering::SeqCst), 0);
    }
    assert_eq!(observed.receivers[2].accepted.load(Ordering::SeqCst), 0);
    assert_eq!(observed.receivers[2].prepared.load(Ordering::SeqCst), 0);
    assert!(
        observed
            .source_array
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .is_some()
    );
    let generation = observed.trace.lock().unwrap()[0].generation;
    let expected_first = [
        ("source.before", DEFAULT_TURN_WORK_UNITS),
        ("source.after", 2),
        ("first.before", 2),
        ("outer.after", 1),
        ("inner.before", 1),
        ("inner.after", 0),
        ("outer.reborrow_returned", 0),
        ("first.after", 0),
        ("second.first_turn", 0),
    ]
    .into_iter()
    .map(|(site, remaining)| Observation {
        site,
        generation,
        remaining,
    })
    .collect::<Vec<_>>();
    assert_eq!(*observed.trace.lock().unwrap(), expected_first);

    assert_eq!(
        driver.process(Duration::from_secs(1)),
        DriverState::Finished
    );
    assert!(state.error().is_none());
    assert_eq!(*observed.output.lock().unwrap(), expected);
    assert_eq!(observed.source_calls.load(Ordering::SeqCst), 1);
    for receiver in &observed.receivers {
        assert_eq!(receiver.accepted.load(Ordering::SeqCst), 1);
        assert_eq!(receiver.prepared.load(Ordering::SeqCst), 1);
        assert_eq!(receiver.apply.load(Ordering::SeqCst), 1);
        assert_eq!(receiver.finish.load(Ordering::SeqCst), 1);
    }
    assert_eq!(observed.receivers[0].drive.load(Ordering::SeqCst), 0);
    assert_eq!(observed.receivers[1].drive.load(Ordering::SeqCst), 1);
    assert!(
        observed
            .source_array
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .is_none()
    );
    let trace = observed.trace.lock().unwrap();
    assert_eq!(
        &trace[expected_first.len()..],
        &[
            Observation {
                site: "second.next_turn",
                generation: generation + 1,
                remaining: DEFAULT_TURN_WORK_UNITS
            },
            Observation {
                site: "second.completed",
                generation: generation + 1,
                remaining: DEFAULT_TURN_WORK_UNITS - 1
            },
        ]
    );
    drop(trace);
    drop(driver);
    for drops in &observed.kernel_drops {
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    for drops in &observed.preparation_drops {
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
