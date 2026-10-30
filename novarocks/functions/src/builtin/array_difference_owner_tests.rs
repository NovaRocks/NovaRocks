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

use super::*;
use crate::kernel_control::internal;
use crate::{ConstantPolicy, ConstantPool, EvaluatedArgument, ScalarEvaluationInstance, Selection};
use arrow_array::{
    Array, ArrayRef, Decimal128Array, Float64Array, Int64Array, ListArray, new_empty_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, FunctionValueType,
};
use std::{sync::Mutex, time::Duration};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after first cause");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("array difference never waits")
    }
}
#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "compile callback after first cause");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}

fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original refusal"),
        internal("original refusal"),
        KernelFailure::Operational(crate::KernelDiagnostic::new("original refusal")),
        KernelFailure::InstanceFailed,
    ]
}

fn policy() -> ConstantPolicy {
    // Finite fixture admission of already allocated real Arrow arrays, not a MEM grant.
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 64,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 65536,
        max_library_validation_work: 4 * 1024 * 1024,
        max_library_validation_bytes: 4 * 1024 * 1024,
    }
}

fn instance(a: &ArrayRef) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_control(
            "array_difference",
            &[FunctionValueType::from_legacy_declared_projection(
                a.data_type().clone(),
                true,
            )],
            DecimalOverflowPolicy::OutputNull,
            crate::binding_test_control(),
        )
        .unwrap(),
    )
    .unwrap()
}
fn list(values: ArrayRef, offsets: Vec<i32>, valid: Option<Vec<bool>>) -> ArrayRef {
    let raw: ArrayRef = Arc::new(ListArray::new(
        Arc::new(
            Field::new("source-child", values.data_type().clone(), true).with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    "nested-proof".into(),
                    "retained".into(),
                )]),
            ),
        ),
        OffsetBuffer::new(offsets.into()),
        values,
        valid.map(NullBuffer::from),
    ));
    let binding = FunctionValueType::new(raw.data_type().clone(), true);
    crate::test_value_ingress::array_for_binding(&raw, &binding).unwrap()
}
fn ints(a: &ArrayRef) -> Vec<Option<i64>> {
    a.as_any()
        .downcast_ref::<ListArray>()
        .unwrap()
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
#[test]
fn array_difference_selected_origin_null_slice_empty_nonzero_pool_and_full_target_metadata() {
    let a = list(
        Arc::new(Int64Array::from(vec![
            Some(99),
            Some(2),
            None,
            Some(4),
            Some(88),
            Some(-7),
            Some(5),
        ])),
        vec![0, 1, 4, 5, 7],
        Some(vec![true, true, false, true]),
    );
    let rows = [1usize, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let args = [EvaluatedArgument::Column(&a)];
    let mut k = instance(&a);
    let expected_type = k.contract().result_type().data_type().clone();
    let result = k.evaluate(selection, &args, &Control::default()).unwrap();
    assert!(result.errors().is_empty());
    assert_eq!(result.values().data_type(), &expected_type);
    assert_eq!(
        ints(result.values()),
        vec![Some(0), None, None, Some(0), Some(12)]
    );
    assert!(result.values().is_null(1));
    assert_eq!(
        result
            .values()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .value_offsets(),
        &[0, 3, 3, 5]
    );
    let ty = FunctionValueType::from_legacy_declared_projection(a.data_type().clone(), true);
    let prepared = prepared_for_test_with_policy(
        "array_difference",
        std::slice::from_ref(&ty),
        DecimalOverflowPolicy::ReportError,
    )
    .unwrap();
    assert_eq!(
        prepared.contract().selected().argument_types[0],
        FunctionArgumentType::Value(ty.clone())
    );
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("pool-source").unwrap()),
        ty,
        a.to_data(),
        policy(),
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap();
    let constant = pool.value(1).unwrap();
    let args = [EvaluatedArgument::Constant(&constant)];
    let result = instance(&a)
        .evaluate(Selection::all(2), &args, &Control::default())
        .unwrap();
    assert_eq!(
        ints(result.values()),
        vec![Some(0), None, None, Some(0), None, None]
    );
    let sliced = a.slice(1, 3);
    let args = [EvaluatedArgument::Column(&sliced)];
    assert_eq!(
        ints(
            instance(&sliced)
                .evaluate(Selection::all(3), &args, &Control::default())
                .unwrap()
                .values()
        ),
        vec![Some(0), None, None, Some(0), Some(12)]
    );
    let empty = a.slice(0, 0);
    let args = [EvaluatedArgument::Column(&empty)];
    assert_eq!(
        instance(&empty)
            .evaluate(Selection::all(0), &args, &Control::default())
            .unwrap()
            .values()
            .len(),
        0
    );
}
#[test]
fn array_difference_selected_original_decimal_float_cast_and_no_admission_widening() {
    let values: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(125), Some(150), None, Some(-25)])
            .with_precision_and_scale(10, 2)
            .unwrap(),
    );
    let a = list(values, vec![0, 2, 4], None);
    let args = [EvaluatedArgument::Column(&a)];
    let result = instance(&a)
        .evaluate(Selection::all(2), &args, &Control::default())
        .unwrap();
    let output = result
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert_eq!(output.value_type(), DataType::Float64);
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(0.0), Some(0.25), None, None]
    );
    // These are actual resolver refusals, not new preparation-time restrictions.
    for item in [
        DataType::UInt64,
        DataType::Utf8,
        DataType::Date32,
        DataType::Decimal256(76, 0),
        DataType::FixedSizeBinary(16),
        DataType::Null,
    ] {
        let source = FunctionValueType::from_legacy_declared_projection(
            DataType::List(Arc::new(Field::new("item", item, true))),
            true,
        );
        assert!(
            prepared_for_test_with_policy(
                "array_difference",
                &[source],
                DecimalOverflowPolicy::OutputNull
            )
            .is_err()
        );
    }
}
#[test]
fn array_difference_selected_numeric_overflow_is_an_explicit_parent_row_outcome() {
    use super::super::array_difference_core;
    let a = list(
        Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX)])),
        vec![0, 2],
        None,
    );
    let args = [EvaluatedArgument::Column(&a)];
    let mut k = instance(&a);
    let target = k.contract().result_type().data_type().clone();
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let error = array_difference_core::difference(&a, Some(&target), 1).unwrap_err();
    assert_eq!(error.failure_fact().class(), FailureClass::Data);
    assert_eq!(
        error.failure_fact(),
        crate::DataFailureReason::NumericOutOfRange.failure_fact()
    );
    let result = k
        .evaluate(Selection::all(1), &args, &Control::default())
        .unwrap();
    assert_eq!(result.errors().len(), 1);
    assert_eq!(result.errors()[0].selected_ordinal(), 0);
    assert_eq!(
        result.errors()[0].reason(),
        crate::DataFailureReason::NumericOutOfRange
    );
    assert!(result.values().is_null(0));
    assert!(ints(result.values()).is_empty());
    // A hidden parent must retain the original skip-before-subtraction behavior.
    let a = list(
        Arc::new(Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX)])),
        vec![0, 2],
        Some(vec![false]),
    );
    let args = [EvaluatedArgument::Column(&a)];
    let result = instance(&a)
        .evaluate(Selection::all(1), &args, &Control::default())
        .unwrap();
    assert!(result.values().is_null(0));
}
#[test]
fn array_difference_selected_all_seven_causes_each_observation_failed_latch_and_real_quanta() {
    let a = list(
        Arc::new(Int64Array::from(vec![Some(1); 700])),
        vec![0, 700],
        None,
    );
    let args = [EvaluatedArgument::Column(&a)];
    let good = Control::default();
    instance(&a)
        .evaluate(Selection::all(1), &args, &good)
        .unwrap();
    let trace = good.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    for cause in causes() {
        for stop in 0..trace.len() {
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((stop, cause.clone())),
            };
            let mut k = instance(&a);
            assert_eq!(
                k.evaluate(Selection::all(1), &args, &control).unwrap_err(),
                cause
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
            let after = Control::default();
            assert_eq!(
                k.evaluate(Selection::all(1), &args, &after).unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
    let a = list(new_empty_array(&DataType::Int64), vec![0; 322], None);
    let args = [EvaluatedArgument::Column(&a)];
    let control = Control::default();
    instance(&a)
        .evaluate(Selection::all(321), &args, &control)
        .unwrap();
    assert!(control.trace.lock().unwrap().contains(&256));
}

#[test]
fn array_difference_compile_callbacks_preserve_primary_causes_on_success_and_rejection() {
    for name in ["array_difference"] {
        for (source, succeeds) in [
            (
                FunctionValueType::from_legacy_declared_projection(
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    true,
                ),
                true,
            ),
            (FunctionValueType::new(DataType::Binary, true), false),
        ] {
            let good = CompileControl::default();
            assert_eq!(
                prepared_for_test_with_control(
                    name,
                    std::slice::from_ref(&source),
                    DecimalOverflowPolicy::OutputNull,
                    &good,
                )
                .is_ok(),
                succeeds,
            );
            let trace = good.trace.lock().unwrap().clone();
            assert!(!trace.is_empty());
            for stop in 0..trace.len() {
                for cause in [
                    CompileControlError::Cancelled,
                    CompileControlError::DeadlineExceeded,
                    CompileControlError::ResourceExhausted,
                ] {
                    let control = CompileControl {
                        trace: Mutex::new(vec![]),
                        refusal: Some((stop, cause.clone())),
                    };
                    let error = prepared_for_test_with_control(
                        name,
                        std::slice::from_ref(&source),
                        DecimalOverflowPolicy::OutputNull,
                        &control,
                    )
                    .expect_err("refused compilation must not publish a prepared kernel");
                    let actual = match error {
                        crate::FunctionSpecializationFailure::Control(actual) => Some(actual),
                        crate::FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled) => {
                            Some(CompileControlError::Cancelled)
                        }
                        crate::FunctionSpecializationFailure::Kernel(
                            KernelFailure::DeadlineExceeded,
                        ) => Some(CompileControlError::DeadlineExceeded),
                        crate::FunctionSpecializationFailure::Kernel(
                            KernelFailure::ResourceExhausted,
                        ) => Some(CompileControlError::ResourceExhausted),
                        _ => None,
                    };
                    assert_eq!(actual, Some(cause));
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
                }
            }
        }
    }
}

#[test]
fn array_difference_overflow_nulls_only_the_demanded_parent_and_keeps_selection_coordinates() {
    let a = list(
        Arc::new(Int64Array::from(vec![i64::MIN, i64::MAX, 2, 8, 9, 5])),
        vec![0, 2, 4, 6],
        None,
    );
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let args = [EvaluatedArgument::Column(&a)];
    let mut k = instance(&a);
    let result = k.evaluate(selection, &args, &Control::default()).unwrap();
    assert_eq!(result.selection().iter().collect::<Vec<_>>(), vec![0, 2]);
    assert_eq!(result.errors().len(), 1);
    assert_eq!(result.errors()[0].selected_ordinal(), 0);
    assert_eq!(
        result.errors()[0].reason(),
        crate::DataFailureReason::NumericOutOfRange
    );
    let out = result
        .values()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    assert!(out.is_null(0));
    assert!(out.is_valid(1));
    assert_eq!(out.value_offsets(), &[0, 0, 2]);
    assert_eq!(ints(result.values()), vec![Some(0), Some(-4)]);
    // A value outcome is not a failed invocation latch: another real evaluation works.
    let result = k
        .evaluate(
            Selection::try_sparse(3, &[1]).unwrap(),
            &args,
            &Control::default(),
        )
        .unwrap();
    assert!(result.errors().is_empty());
    assert_eq!(ints(result.values()), vec![Some(0), Some(6)]);
}
#[test]
fn array_difference_sdk_refusal_never_enters_the_row_journal_and_releases_inputs() {
    use super::super::array_difference_core::{self, DifferenceFailure};
    use arrow_array::StructArray;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let values: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(Field::new("x", DataType::Int64, false))].into(),
        vec![Arc::new(Int64Array::from(vec![7]))],
        None,
    ));
    let expected = arrow_cast::cast(&values, &DataType::Int64)
        .unwrap_err()
        .to_string();
    let a: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new(vec![0, 1].into()),
        values,
        None,
    ));
    let weak = Arc::downgrade(&a);
    let target = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
    let mut row_callbacks = 0;
    let error = array_difference_core::difference_observed(
        &a,
        Some(&target),
        1,
        &mut |_| Ok::<(), KernelFailure>(()),
        &mut |_| {
            row_callbacks += 1;
            Ok(())
        },
    )
    .unwrap_err();
    assert_eq!(row_callbacks, 0);
    let DifferenceFailure::Operation(error) = error else {
        panic!("SDK operation author")
    };
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    let mut source: &(dyn std::error::Error + 'static) = &error;
    loop {
        if let Some(arrow) = source.downcast_ref::<arrow_schema::ArrowError>() {
            assert_eq!(arrow.to_string(), expected);
            break;
        }
        source = source.source().expect("actual SDK error retained");
    }
    drop(a);
    assert!(weak.upgrade().is_none());
}
#[test]
fn array_difference_required_decimal_overflow_and_shape_have_distinct_facts() {
    use super::super::array_difference_core;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let edge = 10_i128.pow(38) - 1;
    let values: ArrayRef = Arc::new(
        Decimal128Array::from(vec![-edge, edge])
            .with_precision_and_scale(38, 0)
            .unwrap(),
    );
    let a: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new(vec![0, 2].into()),
        values,
        None,
    ));
    assert_eq!(
        array_difference_core::difference(&a, None, 1)
            .unwrap_err()
            .failure_fact(),
        crate::DataFailureReason::NumericOutOfRange.failure_fact()
    );
    assert_eq!(
        array_difference_core::difference(&a, Some(&DataType::Int64), 1)
            .unwrap_err()
            .failure_fact()
            .class(),
        FailureClass::Invariant
    );
}

#[test]
fn array_difference_decimal_child_retains_original_float_bits_on_slices_and_nulls() {
    let values: ArrayRef = Arc::new(
        Decimal128Array::new(
            vec![
                0,
                12_345_678_901_234_567_890_i128,
                77,
                0,
                12_345_678_901_234_567_890,
            ]
            .into(),
            Some(NullBuffer::from(vec![true, true, false, true, true])),
        )
        .with_precision_and_scale(38, 2)
        .unwrap(),
    );
    let full = list(values, vec![0, 2, 3, 5], Some(vec![true, false, true]));
    for source in [full.clone(), full.slice(1, 2)] {
        let args = [EvaluatedArgument::Column(&source)];
        let actual = instance(&source)
            .evaluate(Selection::all(source.len()), &args, &Control::default())
            .unwrap();
        let output = actual
            .values()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(output.len(), source.len());
        let final_row = output.value(output.len() - 1);
        let values = final_row.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(values.value(0).to_bits(), 0);
        assert_eq!(values.value(1).to_bits(), 0x437b_69b4_ba63_0f34);
        assert!(output.is_null(output.len() - 2));
    }
}
