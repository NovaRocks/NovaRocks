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
//! Processor lifecycle probes, independent of compile/source transport tests.
use super::*;
use crate::exec::chunk::ChunkSchema;
use crate::runtime::task::ExecutionFailureCause;
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_failure_contract::FailureProjection;
use novarocks_functions::{KernelDiagnostic, generate_series_core::SeriesError};
use novarocks_types::SlotId;
use std::sync::Mutex;

fn assert_failed_instance(error: ExecutionFailure) {
    assert!(matches!(error.cause(), ExecutionFailureCause::Domain(cause)
        if matches!(cause.downcast_ref::<CompiledSeriesFailure>(), Some(CompiledSeriesFailure::AlreadyFailed))));
}
fn assert_zero_step(error: &ExecutionFailure) {
    assert!(matches!(error.cause(), ExecutionFailureCause::Domain(cause)
        if matches!(cause.downcast_ref::<SeriesError>(), Some(SeriesError::ZeroStep))));
}
struct Control {
    trace: Mutex<Vec<u32>>,
    fail_at: Option<usize>,
    cause: KernelFailure,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        trace.push(units);
        if self.fail_at == Some(at) {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("integer series has no wait")
    }
}
fn instance(values: [i64; 3], control: Arc<Control>) -> (CompiledGenerateSeriesProcessor, Chunk) {
    let input = Arc::new(Schema::new(
        (0..3)
            .map(|ordinal| Field::new(format!("original-{ordinal}"), DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let input_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(
        input.as_ref(),
        &[SlotId::new(81), SlotId::new(6), SlotId::new(900)],
    )
    .unwrap();
    let output = ChunkSchema::try_ref_from_schema_and_slot_ids(
        &Schema::new(vec![Field::new("public-series", DataType::Int64, false)]),
        &[SlotId::new(71)],
    )
    .unwrap();
    let arrays = values
        .into_iter()
        .map(|value| Arc::new(Int64Array::from(vec![value])) as arrow::array::ArrayRef)
        .collect::<Vec<_>>();
    let chunk = Chunk::try_new_with_chunk_schema(
        RecordBatch::try_new(Arc::clone(&input), arrays).unwrap(),
        input_schema,
    )
    .unwrap();
    let processor = CompiledGenerateSeriesProcessor {
        name: "control-fixture".to_owned(),
        input,
        parameters: Arc::from([0, 1, 2]),
        output_layout: novarocks_local_program::StaticLayout::try_new(
            output.arrow_schema_ref(),
            Arc::from([SlotId::new(71)]),
        )
        .unwrap(),
        // This fixture isolates the kernel control journal; typed construction
        // ownership is verified through the production factory tests.
        output: Some(output),
        construction: None,
        control,
        pending: None,
        offset: 0,
        accepted: false,
        batch_rows: Some(std::num::NonZeroUsize::new(4096).unwrap()),
        failed: false,
    };
    (processor, chunk)
}
fn control(fail_at: Option<usize>, cause: KernelFailure) -> Arc<Control> {
    Arc::new(Control {
        trace: Mutex::new(vec![]),
        fail_at,
        cause,
    })
}
fn with_context<T>(state: &RuntimeState, action: impl FnOnce(&mut DriverStep<'_>) -> T) -> T {
    let error = state.error_state();
    let mut work = crate::exec::pipeline::driver_step::TurnWork::new(std::num::NonZeroU64::MIN, 64);
    action(&mut DriverStep::new(
        &error,
        &mut work,
        &mut novarocks_value_arrays::decode::ValueConversionContext::default(),
    ))
}
fn apply(
    processor: &mut CompiledGenerateSeriesProcessor,
    state: &RuntimeState,
    input: Chunk,
) -> ExecutionResult<StepResult> {
    with_context(state, |ctx| {
        processor.apply(ctx, PreparedInput::for_test(input))
    })
}
fn drive(
    processor: &mut CompiledGenerateSeriesProcessor,
    state: &RuntimeState,
) -> ExecutionResult<StepResult> {
    with_context(state, |ctx| processor.drive(ctx))
}
fn finish(
    processor: &mut CompiledGenerateSeriesProcessor,
    state: &RuntimeState,
) -> ExecutionResult<StepResult> {
    with_context(state, |ctx| processor.finish(ctx))
}
#[test]
fn compiled_generate_series_control_seven_causes_exact_prefix_no_tail_and_failed_latch() {
    let state = RuntimeState::default();
    let values = [-600, 600, 1];
    let baseline = control(None, KernelFailure::Cancelled);
    let (mut original, chunk) = instance(values, Arc::clone(&baseline));
    apply(&mut original, &state, chunk).unwrap();
    let trace = baseline.trace.lock().unwrap().clone();
    assert!(trace.contains(&MAX_UNOBSERVED_KERNEL_WORK));
    assert!(
        trace
            .iter()
            .all(|units| *units <= MAX_UNOBSERVED_KERNEL_WORK)
    );
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("injected")),
        KernelFailure::Internal(KernelDiagnostic::new("injected")),
        KernelFailure::Operational(KernelDiagnostic::new("injected")),
        KernelFailure::InstanceFailed,
    ] {
        for at in 0..trace.len() {
            let injection = control(Some(at), cause.clone());
            let (mut processor, chunk) = instance(values, Arc::clone(&injection));
            let error = apply(&mut processor, &state, chunk.clone()).unwrap_err();
            assert_eq!(error.cause(), &ExecutionFailureCause::Kernel(cause.clone()));
            assert_eq!(*injection.trace.lock().unwrap(), trace[..=at]);
            assert!(processor.pending.is_none());
            assert!(processor.failed);
            let before = injection.trace.lock().unwrap().len();
            for result in [
                apply(&mut processor, &state, chunk),
                finish(&mut processor, &state),
            ] {
                assert_failed_instance(result.unwrap_err());
            }
            assert_failed_instance(drive(&mut processor, &state).unwrap_err());
            assert_eq!(injection.trace.lock().unwrap().len(), before);
        }
    }
}
#[test]
fn compiled_generate_series_control_original_whole_data_has_no_late_footer_or_row_relabel() {
    let state = RuntimeState::default();
    let baseline = control(None, KernelFailure::Cancelled);
    let (mut processor, chunk) = instance([1, 3, 0], Arc::clone(&baseline));
    let original = apply(&mut processor, &state, chunk).unwrap_err();
    assert_zero_step(&original);
    let trace = baseline.trace.lock().unwrap().clone();
    let next = control(Some(trace.len()), KernelFailure::DeadlineExceeded);
    let (mut processor, chunk) = instance([1, 3, 0], Arc::clone(&next));
    let next_error = apply(&mut processor, &state, chunk).unwrap_err();
    assert_zero_step(&next_error);
    assert_eq!(next_error.failure_fact(), original.failure_fact());
    assert!(!next_error.same_occurrence(&original));
    assert_eq!(*next.trace.lock().unwrap(), trace);
    assert_failed_instance(drive(&mut processor, &state).unwrap_err());
    assert_eq!(*next.trace.lock().unwrap(), trace);
}
#[test]
fn compiled_generate_series_control_finishing_owes_exact_output_then_latches_finished() {
    use crate::exec::pipeline::{
        driver_step::TurnWork, input::RawInput, shape::StepOperator, step::InputResult,
    };
    let state = RuntimeState::default();
    let error = state.error_state();
    let control = control(None, KernelFailure::Cancelled);
    let (kernel, chunk) = instance([1, 9, 2], control);
    let mut processor = Transform::hydrating(kernel);
    let mut work = TurnWork::new(std::num::NonZeroU64::MIN, 64);
    let mut value_conversions = novarocks_value_arrays::decode::ValueConversionContext::default();
    let mut ctx = DriverStep::new(&error, &mut work, &mut value_conversions);
    assert!(matches!(processor.initial_step(), NextStep::NeedInput));
    assert!(matches!(
        processor.try_push(&mut ctx, RawInput::for_test(chunk)),
        InputResult::Accepted(Ok(_))
    ));
    processor.finish(&mut ctx).unwrap();
    let installed = processor.drive(&mut ctx).unwrap();
    assert!(installed.output.is_none());
    assert!(matches!(installed.next, NextStep::Runnable));
    assert!(matches!(installed.next, NextStep::Runnable));
    let result = processor.drive(&mut ctx).unwrap();
    assert!(matches!(result.next, NextStep::Finished));
    let out = result.output.expect("the bounds port's final output");
    assert_eq!(
        out.column_by_slot_id(SlotId::new(71))
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[1, 3, 5, 7, 9]
    );
    assert!(!out.schema().field(0).is_nullable());
    assert_eq!(out.schema().field(0).name(), "public-series");
    assert!(matches!(result.next, NextStep::Finished));
    let ended = processor.drive(&mut ctx).unwrap();
    assert!(ended.output.is_none());
    assert!(matches!(ended.next, NextStep::Finished));
}
