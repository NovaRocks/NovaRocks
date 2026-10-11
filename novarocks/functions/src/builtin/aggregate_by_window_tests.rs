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

//! Actual tracked window instances through the already-resolved BY owner.
use super::*;
use crate::builtin::aggregate_by_window;
use crate::opaque_memory::OpaqueAllocationHost;
use arrow_array::{Array, Float64Array, Int32Array, NullArray};
use std::sync::Mutex;

#[derive(Default)]
struct Gate {
    events: Vec<(bool, usize)>,
    refusal: Option<(usize, KernelFailure)>,
    refused: bool,
    opaque: usize,
    peak: usize,
}
#[derive(Default)]
struct WindowHost {
    state: Host,
    gate: Mutex<Gate>,
}
impl WindowHost {
    fn admit(&self, opaque: bool, bytes: usize) -> Result<(), KernelFailure> {
        let mut gate = self.gate.lock().unwrap();
        assert!(!gate.refused, "no host request after first refusal");
        let at = gate.events.len();
        gate.events.push((opaque, bytes));
        if let Some((stop, cause)) = &gate.refusal {
            if *stop == at {
                let cause = cause.clone();
                gate.refused = true;
                return Err(cause);
            }
        }
        if opaque {
            gate.opaque += bytes;
            gate.peak = gate.peak.max(gate.opaque);
        }
        Ok(())
    }
    fn zero(&self) {
        assert_eq!(self.gate.lock().unwrap().opaque, 0);
        let state = self.state.ledger.lock().unwrap();
        assert_eq!(state.bytes, 0);
        assert!(state.live.is_empty());
    }
}
impl AggregateStateAllocator for WindowHost {
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        self.admit(false, layout.size())?;
        self.state.allocate(layout)
    }
    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        unsafe {
            self.state.release(pointer, layout);
        }
    }
    fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
        Some(self)
    }
}
impl OpaqueAllocationHost for WindowHost {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        self.admit(true, bytes)
    }
    fn release_opaque(&self, bytes: usize) {
        let mut gate = self.gate.lock().unwrap();
        gate.opaque = gate
            .opaque
            .checked_sub(bytes)
            .expect("release exact live admission once");
    }
}
fn prepared(
    name: &str,
    value: FunctionValueType,
    key: FunctionValueType,
) -> Arc<dyn PreparedWindowKernel> {
    let aggregate = Arc::new(kernel(name, value, key, AggregateKernelPhase::Single));
    let options = WindowCallOptions::try_new(None, false, &Compile).unwrap();
    let window = Arc::new(
        WindowCallContract::try_aggregate(Arc::clone(aggregate.contract()), options, &Compile)
            .unwrap(),
    );
    aggregate_by_window::prepare(aggregate, window, &Compile).unwrap()
}
fn input<'a>(
    call: &'a WindowCallContract,
    args: &'a [EvaluatedArgument<'a>],
    peers: &'a [WindowRowRange],
    frames: &'a [WindowRowRange],
) -> WindowPartitionInput<'a> {
    let control = Control::default();
    let full = FullPartitionWindowInput::try_new(call, frames.len(), args, &[], &control).unwrap();
    WindowPartitionInput::try_new(full, peers, frames, &control).unwrap()
}
fn begin<'a>(
    prepared: Arc<dyn PreparedWindowKernel>,
    input: WindowPartitionInput<'a>,
    host: Arc<WindowHost>,
    control: &dyn KernelEvaluationControl,
) -> Result<WindowEvaluationPartition<'a>, WindowEvaluationFailure> {
    WindowEvaluationPartition::begin_evaluated(
        prepared,
        input,
        WindowInvocationContext::partition(0, 0),
        Some(host),
        control,
    )
}
#[test]
fn by_window_tracked_actual_last_buffer_loan_keeps_admission_after_finish_and_partition_drop() {
    let prepared = prepared("max_by", text_type(), text_type());
    let values: ArrayRef = Arc::new(StringArray::from(vec!["first", "second"]));
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["a", "z"]));
    let args = [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::Column(&keys),
    ];
    let peers = [WindowRowRange { start: 0, end: 2 }];
    let frames = [
        WindowRowRange { start: 0, end: 1 },
        WindowRowRange { start: 0, end: 2 },
    ];
    let input = input(prepared.contract(), &args, &peers, &frames);
    let host = Arc::new(WindowHost::default());
    let control = Control::default();
    let mut partition = begin(Arc::clone(&prepared), input, Arc::clone(&host), &control).unwrap();
    assert_eq!(
        partition.retention_policy(),
        WindowPartitionRetention::InputBackedFrozen
    );
    let output = partition
        .evaluate_evaluated(Selection::all(2), 2, &control)
        .unwrap();
    let (_, array, errors) = output.into_parts();
    assert!(errors.is_empty());
    assert_eq!(
        array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(1),
        "second"
    );
    let data = array.to_data();
    let payload = data.buffers()[1].clone();
    partition.finish_evaluated(&control).unwrap();
    drop(partition);
    drop(array);
    drop(data);
    assert!(
        host.gate.lock().unwrap().opaque > 0,
        "real payload loan remains charged"
    );
    assert!(
        !host.state.ledger.lock().unwrap().live.is_empty(),
        "real custody metadata remains owned"
    );
    drop(payload);
    host.zero();
}
#[test]
fn by_window_tracked_actual_null_winner_sparse_slice_empty_and_both_directions() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(99), Some(4), None, Some(9)]));
    let values = values.slice(1, 3);
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["prefix", "a", "z", "b"]));
    let keys = keys.slice(1, 3);
    for name in ["max_by", "min_by"] {
        let prepared = prepared(
            name,
            FunctionValueType::new(DataType::Int32, true),
            text_type(),
        );
        let args = [
            EvaluatedArgument::Column(&values),
            EvaluatedArgument::Column(&keys),
        ];
        let peers = [WindowRowRange { start: 0, end: 3 }];
        let frames = [
            WindowRowRange { start: 0, end: 1 },
            WindowRowRange { start: 0, end: 2 },
            WindowRowRange { start: 0, end: 3 },
        ];
        let input = input(prepared.contract(), &args, &peers, &frames);
        let host = Arc::new(WindowHost::default());
        let control = Control::default();
        let mut partition =
            begin(Arc::clone(&prepared), input, Arc::clone(&host), &control).unwrap();
        let sparse_rows = [0, 2];
        let selected = Selection::try_sparse(3, &sparse_rows).unwrap();
        let output = partition.evaluate_evaluated(selected, 2, &control).unwrap();
        let (_, array, errors) = output.into_parts();
        assert!(errors.is_empty());
        let typed = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(typed.value(0), 4);
        assert_eq!(typed.is_null(1), name == "max_by");
        let empty_rows = [];
        drop(
            partition
                .evaluate_evaluated(Selection::try_sparse(3, &empty_rows).unwrap(), 0, &control)
                .unwrap(),
        );
        partition.finish_evaluated(&control).unwrap();
        drop(partition);
        drop(array);
        host.zero();
    }
}
#[test]
fn by_window_tracked_actual_nan_whole_data_full_text_call_phase_no_footer_and_drop() {
    for name in ["min_by", "max_by"] {
        let prepared = prepared(
            name,
            FunctionValueType::new(DataType::Int32, false),
            FunctionValueType::new(DataType::Float64, false),
        );
        let values: ArrayRef = Arc::new(Int32Array::from(vec![7, 8]));
        let keys: ArrayRef = Arc::new(Float64Array::from(vec![1.0, f64::NAN]));
        let args = [
            EvaluatedArgument::Column(&values),
            EvaluatedArgument::Column(&keys),
        ];
        let peers = [WindowRowRange { start: 0, end: 2 }];
        let frames = [
            WindowRowRange { start: 0, end: 1 },
            WindowRowRange { start: 0, end: 2 },
        ];
        let input = input(prepared.contract(), &args, &peers, &frames);
        let host = Arc::new(WindowHost::default());
        let control = Control::default();
        let error = match begin(Arc::clone(&prepared), input, Arc::clone(&host), &control) {
            Ok(_) => panic!("original NaN update must fail before output demand"),
            Err(error) => error,
        };
        let WindowEvaluationFailure::InvocationData(data) = &error else {
            panic!("lossless original whole-window Data");
        };
        assert_eq!(
            data.message(),
            "window function #0: update aggregate state: float comparison is not ordered"
        );
        assert_eq!(data.partition_rows(), Some(2));
        assert_eq!(data.frame_ordinal(), Some(1));
        assert_eq!(data.input_row(), Some(1));
        assert_eq!(data.phase(), WindowInvocationPhase::FrameUpdate);
        assert_eq!(data.call_ordinal(), 0);
        let clone = error.clone();
        drop(error);
        assert!(
            host.gate.lock().unwrap().opaque > 0,
            "original diagnostic lives in a real pre-granted scope"
        );
        drop(clone);
        host.zero();
    }
}
#[test]
fn by_window_tracked_actual_every_callback_seven_causes_no_footer_and_drop() {
    let prepared = prepared("min_by", text_type(), text_type());
    let values: ArrayRef = Arc::new(StringArray::from(vec!["first", "second"]));
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["z", "a"]));
    let args = [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::Column(&keys),
    ];
    let peers = [WindowRowRange { start: 0, end: 2 }];
    let frames = [
        WindowRowRange { start: 0, end: 1 },
        WindowRowRange { start: 0, end: 2 },
    ];
    let input = input(prepared.contract(), &args, &peers, &frames);
    let host = Arc::new(WindowHost::default());
    let success = Control::default();
    drop(begin(Arc::clone(&prepared), input, Arc::clone(&host), &success).unwrap());
    host.zero();
    let callbacks = success.trace.lock().unwrap().len();
    assert!(callbacks > 0);
    for cause in causes() {
        for at in 0..callbacks {
            let host = Arc::new(WindowHost::default());
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((at, cause.clone())),
            };
            let result = begin(Arc::clone(&prepared), input, Arc::clone(&host), &control);
            assert!(
                matches!(result, Err(WindowEvaluationFailure::Kernel(actual)) if actual == cause)
            );
            assert_eq!(control.trace.lock().unwrap().len(), at + 1);
            host.zero();
        }
    }
}
#[test]
fn by_window_tracked_actual_real_state_and_opaque_refusals_all_seven_causes_rollback() {
    let prepared = prepared("max_by", text_type(), text_type());
    let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "larger"]));
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["a", "z"]));
    let args = [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::Column(&keys),
    ];
    let peers = [WindowRowRange { start: 0, end: 2 }];
    let frames = [
        WindowRowRange { start: 0, end: 1 },
        WindowRowRange { start: 0, end: 2 },
    ];
    let input = input(prepared.contract(), &args, &peers, &frames);
    let host = Arc::new(WindowHost::default());
    drop(
        begin(
            Arc::clone(&prepared),
            input,
            Arc::clone(&host),
            &Control::default(),
        )
        .unwrap(),
    );
    host.zero();
    let attempts = host.gate.lock().unwrap().events.len();
    assert!(
        host.gate
            .lock()
            .unwrap()
            .events
            .iter()
            .any(|(opaque, _)| *opaque)
    );
    for cause in causes() {
        for at in 0..attempts {
            let host = Arc::new(WindowHost::default());
            host.gate.lock().unwrap().refusal = Some((at, cause.clone()));
            let result = begin(
                Arc::clone(&prepared),
                input,
                Arc::clone(&host),
                &Control::default(),
            );
            assert!(
                matches!(result, Err(WindowEvaluationFailure::Kernel(actual)) if actual == cause)
            );
            assert_eq!(host.gate.lock().unwrap().events.len(), at + 1);
            host.zero();
        }
    }
}
#[test]
fn by_window_tracked_actual_missing_capability_refuses_even_empty_complete_input() {
    let prepared = prepared(
        "min_by",
        FunctionValueType::new(DataType::Null, true),
        text_type(),
    );
    let values: ArrayRef = Arc::new(NullArray::new(0));
    let keys: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
    let args = [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::Column(&keys),
    ];
    let peers = [];
    let frames = [];
    let input = input(prepared.contract(), &args, &peers, &frames);
    assert!(matches!(
        WindowEvaluationPartition::begin_evaluated(
            Arc::clone(&prepared),
            input,
            WindowInvocationContext::partition(0, 0),
            None,
            &Control::default()
        ),
        Err(WindowEvaluationFailure::Kernel(
            KernelFailure::InvalidProgram(_)
        ))
    ));
    let host = Arc::new(WindowHost::default());
    let mut partition = begin(
        Arc::clone(&prepared),
        input,
        Arc::clone(&host),
        &Control::default(),
    )
    .unwrap();
    let output = partition
        .evaluate_evaluated(Selection::all(0), 0, &Control::default())
        .unwrap();
    partition.finish_evaluated(&Control::default()).unwrap();
    drop(partition);
    drop(output);
    host.zero();
}

#[test]
fn by_window_tracked_actual_emission_every_callback_seven_causes_latches_without_replay() {
    let prepared = prepared("max_by", text_type(), text_type());
    let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
    let args = [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::Column(&keys),
    ];
    let peers = [WindowRowRange { start: 0, end: 3 }];
    let frames = [
        WindowRowRange { start: 0, end: 1 },
        WindowRowRange { start: 0, end: 2 },
        WindowRowRange { start: 0, end: 3 },
    ];
    let input = input(prepared.contract(), &args, &peers, &frames);
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let host = Arc::new(WindowHost::default());
    let control = Control::default();
    let mut partition = begin(
        Arc::clone(&prepared),
        input,
        Arc::clone(&host),
        &Control::default(),
    )
    .unwrap();
    drop(
        partition
            .evaluate_evaluated(selection, 2, &control)
            .unwrap(),
    );
    let callbacks = control.trace.lock().unwrap().len();
    drop(partition);
    host.zero();
    for cause in causes() {
        for at in 0..callbacks {
            let host = Arc::new(WindowHost::default());
            let mut partition = begin(
                Arc::clone(&prepared),
                input,
                Arc::clone(&host),
                &Control::default(),
            )
            .unwrap();
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(partition.evaluate_evaluated(selection,2,&control), Err(WindowEvaluationFailure::Kernel(actual)) if actual==cause)
            );
            let completed = control.trace.lock().unwrap().len();
            assert_eq!(completed, at + 1);
            assert!(
                matches!(partition.evaluate_evaluated(selection,2,&control), Err(WindowEvaluationFailure::Kernel(actual)) if actual==cause)
            );
            assert!(
                matches!(partition.finish_evaluated(&control), Err(WindowEvaluationFailure::Kernel(actual)) if actual==cause)
            );
            assert_eq!(control.trace.lock().unwrap().len(), completed);
            drop(partition);
            host.zero();
        }
    }
}
#[test]
fn by_window_tracked_actual_full_generic_scalar_builder_and_nested_metadata() {
    use crate::aggregate_scalar::{self as scalar, AggScalarValue as V, ScalarWork};
    use arrow_buffer::i256;
    use arrow_schema::Field;
    use std::collections::HashMap;
    let nested = DataType::List(Arc::new(
        Field::new("original_element", DataType::Utf8, true)
            .with_metadata(HashMap::from([("PARQUET:field_id".into(), "7".into())])),
    ));
    let cases = vec![
        (
            DataType::Boolean,
            vec![Some(V::Bool(false)), Some(V::Bool(true))],
        ),
        (DataType::Int8, vec![Some(V::Int64(-128)), None]),
        (DataType::Int16, vec![Some(V::Int64(-32768)), None]),
        (DataType::Int32, vec![Some(V::Int64(i32::MIN.into())), None]),
        (DataType::Int64, vec![Some(V::Int64(i64::MIN)), None]),
        (
            DataType::Float64,
            vec![Some(V::Float64(-0.0)), Some(V::Float64(f64::INFINITY))],
        ),
        (DataType::Utf8, vec![Some(V::Utf8("前\0z".into())), None]),
        (
            DataType::Timestamp(arrow_schema::TimeUnit::Second, None),
            vec![Some(V::Timestamp(-1)), None],
        ),
        (
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, Some("+08:00".into())),
            vec![Some(V::Timestamp(-1)), None],
        ),
        (
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("+08:00".into())),
            vec![Some(V::Timestamp(-1)), None],
        ),
        (
            DataType::Float32,
            vec![Some(V::Float64(-0.0)), Some(V::Float64(1.5))],
        ),
        (DataType::Date32, vec![Some(V::Date32(-1)), None]),
        (
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("+08:00".into())),
            vec![Some(V::Timestamp(-1)), None],
        ),
        (
            DataType::Decimal128(20, -2),
            vec![Some(V::Decimal128(-128)), None],
        ),
        (
            DataType::Decimal256(60, 4),
            vec![Some(V::Decimal256(i256::from_i128(-128))), None],
        ),
        (
            DataType::FixedSizeBinary(16),
            vec![Some(V::Decimal128(i128::MIN)), None],
        ),
        (
            DataType::Binary,
            vec![Some(V::Binary(vec![0, 255, 0])), None],
        ),
        (
            DataType::LargeBinary,
            vec![Some(V::Binary(vec![0, 255, 0])), None],
        ),
        (
            nested,
            vec![
                Some(V::List(vec![Some(V::Utf8("前\0z".into())), None])),
                None,
            ],
        ),
        (
            DataType::Struct(
                vec![Arc::new(Field::new(
                    "original_field",
                    DataType::Int32,
                    true,
                ))]
                .into(),
            ),
            vec![Some(V::Struct(vec![Some(V::Int64(7))])), None],
        ),
        (
            DataType::Map(
                Arc::new(Field::new(
                    "original_entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", DataType::Utf8, false)),
                            Arc::new(Field::new("value", DataType::Int32, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                true,
            ),
            vec![
                Some(V::Map(vec![(Some(V::Utf8("a".into())), Some(V::Int64(7)))])),
                None,
            ],
        ),
        (DataType::Null, vec![None, None]),
    ];
    for (ty, original) in cases {
        let values = scalar::build_scalar_array(&ty, original, &mut ScalarWork::new(None)).unwrap();
        let keys: ArrayRef = Arc::new(StringArray::from(vec!["a", "z"]));
        let args = [
            EvaluatedArgument::Column(&values),
            EvaluatedArgument::Column(&keys),
        ];
        let peers = [WindowRowRange { start: 0, end: 2 }];
        let frames = [
            WindowRowRange { start: 0, end: 1 },
            WindowRowRange { start: 0, end: 2 },
        ];
        for name in ["max_by", "min_by"] {
            let prepared = prepared(name, FunctionValueType::new(ty.clone(), true), text_type());
            let input = input(prepared.contract(), &args, &peers, &frames);
            let host = Arc::new(WindowHost::default());
            let mut partition = begin(
                Arc::clone(&prepared),
                input,
                Arc::clone(&host),
                &Control::default(),
            )
            .unwrap();
            let output = partition
                .evaluate_evaluated(Selection::all(2), 2, &Control::default())
                .unwrap();
            let (_, output, errors) = output.into_parts();
            assert!(errors.is_empty());
            assert_eq!(output.data_type(), &ty);
            assert_eq!(
                scalar::scalar_from_array(&output, 0, &mut ScalarWork::new(None))
                    .unwrap()
                    .map(|v| format!("{v:?}")),
                scalar::scalar_from_array(&values, 0, &mut ScalarWork::new(None))
                    .unwrap()
                    .map(|v| format!("{v:?}"))
            );
            let expected = if name == "max_by" { 1 } else { 0 };
            assert_eq!(
                scalar::scalar_from_array(&output, 1, &mut ScalarWork::new(None))
                    .unwrap()
                    .map(|v| format!("{v:?}")),
                scalar::scalar_from_array(&values, expected, &mut ScalarWork::new(None))
                    .unwrap()
                    .map(|v| format!("{v:?}"))
            );
            partition.finish_evaluated(&Control::default()).unwrap();
            drop(partition);
            drop(output);
            host.zero();
        }
    }
}

#[test]
fn by_window_tracked_actual_empty_struct_unwinds_original_builder_and_releases_real_admission() {
    use std::panic::{AssertUnwindSafe, catch_unwind};
    const ORIGINAL_PANIC: &str = "called `Result::unwrap()` on an `Err` value: InvalidArgumentError(\"use StructArray::try_new_with_length or StructArray::new_empty_fields to create a struct array with no fields so that the length can be set correctly\")";
    for name in ["max_by", "min_by"] {
        for null_winner in [false, true] {
            let ty = DataType::Struct(arrow_schema::Fields::empty());
            let prepared = prepared(
                name,
                FunctionValueType::new(ty, true),
                FunctionValueType::new(DataType::Int32, true),
            );
            let nulls = null_winner.then(|| arrow_buffer::NullBuffer::new_null(2));
            let values: ArrayRef = Arc::new(arrow_array::StructArray::new_empty_fields(2, nulls));
            let keys: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
            let args = [
                EvaluatedArgument::Column(&values),
                EvaluatedArgument::Column(&keys),
            ];
            let peers = [WindowRowRange { start: 0, end: 2 }];
            let frames = [WindowRowRange { start: 0, end: 2 }; 2];
            let input = input(prepared.contract(), &args, &peers, &frames);
            let host = Arc::new(WindowHost::default());
            let control = Control::default();
            let result = catch_unwind(AssertUnwindSafe(|| {
                begin(Arc::clone(&prepared), input, Arc::clone(&host), &control)
            }));
            let panic = result
                .err()
                .expect("ONE original builder still panics for empty Struct");
            let text = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .expect("original textual panic");
            assert_eq!(text, ORIGINAL_PANIC);
            host.zero();
        }
    }
}
#[test]
fn by_window_tracked_actual_complete_carrier_and_nominal_final_validation_ordinals() {
    let prepared = prepared(
        "max_by",
        FunctionValueType::new(DataType::Int32, true),
        FunctionValueType::new(DataType::Int32, true),
    );
    let values: ArrayRef = Arc::new(Int32Array::from(vec![4, 9]));
    let keys: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let args = [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::Column(&keys),
    ];
    let peers = [WindowRowRange { start: 0, end: 2 }];
    let frames = [WindowRowRange { start: 0, end: 2 }; 2];
    let input = input(prepared.contract(), &args, &peers, &frames);
    let host = Arc::new(WindowHost::default());
    let control = Control::default();
    let mut partition = begin(Arc::clone(&prepared), input, Arc::clone(&host), &control).unwrap();
    let carrier = partition
        .complete_carrier_evaluated(&control)
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(carrier.contract(), prepared.contract()));
    assert_eq!(carrier.values().len(), 2);
    assert_eq!(carrier.values().data_type(), &DataType::Int32);
    partition.finish_evaluated(&control).unwrap();
    drop(partition);
    assert!(
        host.gate.lock().unwrap().opaque > 0,
        "actual completed output still owns grant"
    );
    // Projection contract only: the Execution test invokes the ONE actual
    // validator. This names a real output column separately from a frame.
    let message =
        "analytic output length mismatch at column 3: expected_rows=2 actual=1".to_owned();
    let charge = crate::opaque_memory::OpaqueRetainedCharge::try_new(host.clone()).unwrap();
    let reservation = charge.reserve_operation(message.capacity()).unwrap();
    let data = carrier.original_output_validation_data(3, message, reservation);
    assert_eq!(data.phase(), WindowInvocationPhase::OutputValidation);
    assert_eq!(data.call_ordinal(), 0);
    assert_eq!(data.output_ordinal(), Some(3));
    assert_eq!(data.frame_ordinal(), None);
    assert_eq!(data.input_row(), None);
    assert_eq!(
        data.message(),
        "analytic output length mismatch at column 3: expected_rows=2 actual=1"
    );
    drop(carrier);
    assert!(
        host.gate.lock().unwrap().opaque > 0,
        "nominal complete Data owns its full string admission"
    );
    drop(data);
    drop(charge);
    host.zero();
}

fn invocation_input<'a>(
    contract: &'a WindowCallContract,
    arguments: &'a [EvaluatedArgument<'a>],
    partitions: &'a [WindowRowRange],
    peers: &'a [WindowRowRange],
    frames: &'a [WindowRowRange],
) -> crate::WindowInvocationInput<'a> {
    let control = Control::default();
    let full =
        crate::FullWindowInvocationInput::try_new(contract, frames.len(), arguments, &[], &control)
            .unwrap();
    crate::WindowInvocationInput::try_new(full, partitions, peers, frames, &control).unwrap()
}
#[test]
fn by_window_whole_invocation_nan_preserves_actual_partition_frame_and_global_row_source() {
    let prepared = prepared(
        "max_by",
        FunctionValueType::new(DataType::Int32, true),
        FunctionValueType::new(DataType::Float64, true),
    );
    let values: ArrayRef = Arc::new(Int32Array::from(vec![10, 11, 20, 21]));
    let keys: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 1.0, f64::NAN]));
    let args = [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::Column(&keys),
    ];
    let parts = [
        WindowRowRange { start: 0, end: 2 },
        WindowRowRange { start: 2, end: 4 },
    ];
    let peers = [
        WindowRowRange { start: 0, end: 1 },
        WindowRowRange { start: 1, end: 2 },
        WindowRowRange { start: 2, end: 3 },
        WindowRowRange { start: 3, end: 4 },
    ];
    let frames = [
        WindowRowRange { start: 0, end: 1 },
        WindowRowRange { start: 0, end: 2 },
        WindowRowRange { start: 2, end: 3 },
        WindowRowRange { start: 2, end: 4 },
    ];
    let source = invocation_input(prepared.contract(), &args, &parts, &peers, &frames);
    let host = Arc::new(WindowHost::default());
    let control = Control::default();
    let cause = match crate::WindowEvaluationInvocation::begin(
        Arc::clone(&prepared),
        source,
        WindowInvocationContext::complete_invocation(0),
        Some(host.clone()),
        &control,
    ) {
        Err(WindowEvaluationFailure::InvocationData(data)) => data,
        _ => panic!("actual original NaN whole invocation Data"),
    };
    assert_eq!(
        cause.message(),
        "window function #0: update aggregate state: float comparison is not ordered"
    );
    assert_eq!(
        cause.scope(),
        crate::WindowInvocationScope::CompleteInvocation
    );
    assert_eq!(cause.invocation_rows(), 4);
    assert_eq!(cause.partition_ordinal(), Some(1));
    assert_eq!(cause.partition_rows(), Some(2));
    assert_eq!(cause.frame_ordinal(), Some(3));
    assert_eq!(cause.partition_frame_ordinal(), Some(1));
    assert_eq!(cause.input_row(), Some(3));
    assert_eq!(cause.phase(), WindowInvocationPhase::FrameUpdate);
    assert!(
        host.gate.lock().unwrap().opaque > 0,
        "original diagnostic admission survives its returned Data"
    );
    drop(cause);
    host.zero();
}
#[test]
fn by_window_whole_invocation_one_global_builder_keeps_last_buffer_charge_until_drop() {
    let prepared = prepared("min_by", text_type(), text_type());
    let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["z", "a", "z", "a"]));
    let args = [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::Column(&keys),
    ];
    let parts = [
        WindowRowRange { start: 0, end: 2 },
        WindowRowRange { start: 2, end: 4 },
    ];
    let peers = [
        WindowRowRange { start: 0, end: 1 },
        WindowRowRange { start: 1, end: 2 },
        WindowRowRange { start: 2, end: 3 },
        WindowRowRange { start: 3, end: 4 },
    ];
    let frames = [
        WindowRowRange { start: 0, end: 1 },
        WindowRowRange { start: 0, end: 2 },
        WindowRowRange { start: 2, end: 3 },
        WindowRowRange { start: 2, end: 4 },
    ];
    let source = invocation_input(prepared.contract(), &args, &parts, &peers, &frames);
    let host = Arc::new(WindowHost::default());
    let control = Control::default();
    let mut invocation = crate::WindowEvaluationInvocation::begin(
        Arc::clone(&prepared),
        source,
        WindowInvocationContext::complete_invocation(0),
        Some(host.clone()),
        &control,
    )
    .unwrap();
    let carrier = invocation.complete_carrier(&control).unwrap();
    assert_eq!(carrier.invocation_rows(), 4);
    assert_eq!(
        carrier.source_context(),
        WindowInvocationContext::complete_invocation(0)
    );
    let array = carrier.values().clone();
    let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        strings.iter().collect::<Vec<_>>(),
        vec![Some("a"), Some("b"), Some("c"), Some("d")]
    );
    let data = array.to_data();
    let buffer = data.buffers()[1].clone();
    invocation.finish(&control).unwrap();
    drop(invocation);
    drop(carrier);
    drop(array);
    drop(data);
    assert!(host.gate.lock().unwrap().opaque > 0);
    drop(buffer);
    host.zero();
}
#[test]
fn by_window_whole_invocation_every_real_callback_seven_causes_and_no_footer() {
    let prepared = prepared("max_by", text_type(), text_type());
    let values: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["z", "a", "z", "a"]));
    let args = [
        EvaluatedArgument::Column(&values),
        EvaluatedArgument::Column(&keys),
    ];
    let parts = [
        WindowRowRange { start: 0, end: 2 },
        WindowRowRange { start: 2, end: 4 },
    ];
    let peers = parts;
    let frames = [parts[0], parts[0], parts[1], parts[1]];
    let source = invocation_input(prepared.contract(), &args, &parts, &peers, &frames);
    let host = Arc::new(WindowHost::default());
    let success = Control::default();
    drop(
        crate::WindowEvaluationInvocation::begin(
            Arc::clone(&prepared),
            source,
            WindowInvocationContext::complete_invocation(0),
            Some(host.clone()),
            &success,
        )
        .unwrap(),
    );
    host.zero();
    let callbacks = success.trace.lock().unwrap().len();
    assert!(callbacks > 0);
    for cause in causes() {
        for stop in 0..callbacks {
            let host = Arc::new(WindowHost::default());
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((stop, cause.clone())),
            };
            let result = crate::WindowEvaluationInvocation::begin(
                Arc::clone(&prepared),
                source,
                WindowInvocationContext::complete_invocation(0),
                Some(host.clone()),
                &control,
            );
            assert!(
                matches!(result, Err(WindowEvaluationFailure::Kernel(actual)) if actual == cause)
            );
            assert_eq!(control.trace.lock().unwrap().len(), stop + 1);
            host.zero();
        }
    }
}

#[test]
fn by_window_structural_scratch_real_layout_refusals_keep_all_seven_causes_and_drop() {
    let run = |host: Arc<WindowHost>, control: &Control| -> Result<(), KernelFailure> {
        let mut scratch = crate::WindowInvocationScratch::try_new(host, control)?;
        for row in 0..257 {
            scratch.try_push(
                WindowRowRange {
                    start: row,
                    end: row + 1,
                },
                control,
            )?;
        }
        assert_eq!(scratch.len(), 257);
        assert_eq!(
            scratch[256],
            WindowRowRange {
                start: 256,
                end: 257
            }
        );
        Ok(())
    };
    let host = Arc::new(WindowHost::default());
    let control = Control::default();
    run(host.clone(), &control).unwrap();
    host.zero();
    let attempts = host.gate.lock().unwrap().events.len();
    assert!(attempts > 2);
    assert!(
        host.gate
            .lock()
            .unwrap()
            .events
            .iter()
            .all(|(opaque, bytes)| !opaque && *bytes > 0)
    );
    let callbacks = control.trace.lock().unwrap().len();
    assert!(
        control
            .trace
            .lock()
            .unwrap()
            .iter()
            .all(|work| *work <= 256)
    );
    for cause in causes() {
        for stop in 0..attempts {
            let host = Arc::new(WindowHost::default());
            host.gate.lock().unwrap().refusal = Some((stop, cause.clone()));
            assert_eq!(run(host.clone(), &Control::default()), Err(cause.clone()));
            assert_eq!(host.gate.lock().unwrap().events.len(), stop + 1);
            host.zero();
        }
        for stop in 0..callbacks {
            let host = Arc::new(WindowHost::default());
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert_eq!(run(host.clone(), &control), Err(cause.clone()));
            assert_eq!(control.trace.lock().unwrap().len(), stop + 1);
            host.zero();
        }
    }
}

#[test]
fn by_window_tracked_actual_original_large_utf8_data_is_lossless() {
    use crate::aggregate_scalar::{self as scalar, ScalarWork};
    let values: ArrayRef = Arc::new(arrow_array::LargeStringArray::from(vec!["a", "b"]));
    let keys: ArrayRef = Arc::new(StringArray::from(vec!["a", "z"]));
    // Read the SAME original update reader directly, not a replacement
    // decoder or an inferred normalization from the declared wide type.
    let raw_host = Arc::new(WindowHost::default());
    let allocator =
        crate::aggregate_host_allocator::HostAggregateAllocator::try_new(raw_host.clone()).unwrap();
    let raw = scalar::tracked_scalar_from_array(&values, 0, &allocator, &mut ScalarWork::new(None))
        .unwrap_err();
    assert_eq!(
        raw.to_string(),
        "unsupported tracked scalar type: LargeUtf8"
    );
    drop(allocator);
    raw_host.zero();
    let raw = scalar::build_scalar_array(
        &DataType::LargeUtf8,
        vec![None, None],
        &mut ScalarWork::new(None),
    )
    .unwrap_err();
    assert_eq!(raw.to_string(), "unsupported scalar output type: LargeUtf8");
    for name in ["max_by", "min_by"] {
        let catalog = super::super::super::catalogue::by_window_private_test_catalog();
        let prepared = window_kernel_with_catalog(
            name,
            FunctionValueType::new(DataType::LargeUtf8, true),
            text_type(),
            &catalog,
        );
        let args = [
            EvaluatedArgument::Column(&values),
            EvaluatedArgument::Column(&keys),
        ];
        let peers = [WindowRowRange { start: 0, end: 2 }];
        let frames = [
            WindowRowRange { start: 0, end: 1 },
            WindowRowRange { start: 0, end: 2 },
        ];
        let input = input(prepared.contract(), &args, &peers, &frames);
        let host = Arc::new(WindowHost::default());
        let control = Control::default();
        let first = match begin(Arc::clone(&prepared), input, Arc::clone(&host), &control) {
            Ok(_) => panic!("the original wide scalar reader must fail"),
            Err(first) => first,
        };
        let WindowEvaluationFailure::InvocationData(data) = &first else {
            panic!("original scalar Data must not be Operational or Internal");
        };
        assert_eq!(
            data.message(),
            "window function #0: update aggregate state: unsupported tracked scalar type: LargeUtf8"
        );
        assert_eq!(data.phase(), WindowInvocationPhase::FrameUpdate);
        assert_eq!(data.frame_ordinal(), Some(0));
        assert_eq!(data.input_row(), Some(0));
        assert!(
            host.gate.lock().unwrap().opaque > 0,
            "actual diagnostic backing retains its original granted reservation"
        );
        drop(first);
        host.zero();
    }
}

#[path = "array_source_custody_tests.rs"]
mod source_custody_tests;
