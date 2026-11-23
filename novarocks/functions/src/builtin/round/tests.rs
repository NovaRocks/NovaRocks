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
use crate::{FunctionValueType, ScalarEvaluationInstance, Selection};
use arrow_array::{ArrayRef, Decimal128Array, Float32Array, Float64Array, Int8Array, Int32Array};
use novarocks_type_contract::DecimalOverflowPolicy;
use std::time::Duration;

struct Control;
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("rounding must not wait");
    }
}

pub(in crate::builtin) fn evaluate(
    round: bool,
    arrays: &[ArrayRef],
    selection: Selection<'_>,
) -> (ArrayRef, Box<[RowDataError]>) {
    let types = arrays
        .iter()
        .map(|array| FunctionValueType::new(array.data_type().clone(), true))
        .collect::<Vec<_>>();
    let literals = vec![None; types.len()];
    let prepared = if round {
        super::super::round_owner::prepared_for_test(
            &types,
            &literals,
            DecimalOverflowPolicy::ReportError,
        )
    } else {
        super::super::truncate_owner::prepared_for_test(
            &types,
            &literals,
            DecimalOverflowPolicy::ReportError,
        )
    }
    .unwrap();
    let args = arrays
        .iter()
        .map(EvaluatedArgument::Column)
        .collect::<Vec<_>>();
    let (_, values, errors) = ScalarEvaluationInstance::instantiate(prepared)
        .unwrap()
        .evaluate(selection, &args, &Control)
        .unwrap()
        .into_parts();
    (values, errors)
}

#[test]
fn round_real_and_double_preserve_ties_nonfinite_values_and_signed_zero() {
    let real: ArrayRef = Arc::new(Float32Array::from(vec![1.5, -1.5, f32::INFINITY]));
    let real_output = evaluate(true, &[real], Selection::all(3)).0;
    assert_eq!(
        real_output
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values(),
        &[2.0, -2.0, f32::INFINITY]
    );

    let double: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(f64::NAN),
        Some(-1.0),
        Some(-0.0),
        None,
    ]));
    let digits: ArrayRef = Arc::new(Int32Array::from(vec![0, -500, 0, 0]));
    let output = evaluate(true, &[double, digits], Selection::all(4)).0;
    let values = output.as_any().downcast_ref::<Float64Array>().unwrap();
    assert!(values.value(0).is_nan());
    assert_eq!(values.value(1).to_bits(), (-0.0f64).to_bits());
    assert_eq!(values.value(2).to_bits(), 0.0f64.to_bits());
    assert!(values.is_null(3));
}

#[test]
fn round_decimal_keeps_declared_scale_while_reading_selected_runtime_digits() {
    let raw: ArrayRef = Arc::new(
        Decimal128Array::from(vec![
            Some(2550),
            Some(2550),
            Some(2550),
            Some(-2550),
            Some(50),
            None,
        ])
        .with_precision_and_scale(4, 2)
        .unwrap(),
    );
    let digits: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, -1, -1, i32::MIN, 0]));
    let output = evaluate(true, &[raw, digits], Selection::all(6)).0;
    assert_eq!(output.data_type(), &DataType::Decimal128(5, 2));
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [
            Some(2600),
            Some(2550),
            Some(3000),
            Some(-3000),
            Some(0),
            None
        ],
    );
}

#[test]
fn integer_round_masks_only_the_selected_overflow_row_in_its_original_width() {
    let input: ArrayRef = Arc::new(Int8Array::from(vec![127, 12, -125]));
    let digits: ArrayRef = Arc::new(Int32Array::from(vec![-1, -1, -1]));
    let arrays = [input, digits];
    let rows = [1, 2];
    let (output, errors) = evaluate(true, &arrays, Selection::try_sparse(3, &rows).unwrap());
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].selected_ordinal(), 1);
    assert_eq!(errors[0].reason(), DataFailureReason::NumericOutOfRange);
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some(10), None],
    );
}

#[test]
fn round_long_decimal_rejects_factors_outside_its_integer_rescale_domain() {
    let input: ArrayRef = Arc::new(
        Decimal128Array::from(vec![12_345_678_912_345_678_912_325_i128, 2550])
            .with_precision_and_scale(23, 2)
            .unwrap(),
    );
    let digits: ArrayRef = Arc::new(Int32Array::from(vec![i32::MIN, i32::MAX]));
    let (output, errors) = evaluate(true, &[input, digits], Selection::all(2));
    assert_eq!(output.data_type(), &DataType::Decimal128(24, 2));
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].selected_ordinal(), 0);
    assert_eq!(errors[0].reason(), DataFailureReason::NumericOutOfRange);
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [None, Some(2550)],
    );
}

#[test]
fn selected_rounding_body_observes_cancellation_before_publication() {
    struct CancelAtBatchWork;
    impl KernelEvaluationControl for CancelAtBatchWork {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            if units == crate::MAX_UNOBSERVED_KERNEL_WORK {
                return Err(KernelFailure::Cancelled);
            }
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("rounding must not wait");
        }
    }
    let ty = FunctionValueType::new(DataType::Float64, true);
    let value: ArrayRef = Arc::new(Float64Array::from(vec![3.5; 300]));
    let args = [EvaluatedArgument::Column(&value)];
    for prepared in [
        super::super::round_owner::prepared_for_test(
            &[ty.clone()],
            &[None],
            DecimalOverflowPolicy::ReportError,
        ),
        super::super::truncate_owner::prepared_for_test(
            &[ty.clone()],
            &[None],
            DecimalOverflowPolicy::ReportError,
        ),
    ] {
        let mut call = ScalarEvaluationInstance::instantiate(prepared.unwrap()).unwrap();
        assert!(matches!(
            call.evaluate(Selection::all(300), &args, &CancelAtBatchWork),
            Err(KernelFailure::Cancelled)
        ));
        assert!(matches!(
            call.evaluate(Selection::all(300), &args, &Control),
            Err(KernelFailure::InstanceFailed)
        ));
    }
}
