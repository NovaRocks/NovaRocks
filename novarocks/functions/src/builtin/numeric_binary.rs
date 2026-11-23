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
//! Selected binary f64 contracts and addresses delegate to the ONE original core.
//! Arrow builder allocation still requires formal host memory admission.
pub(super) use super::numeric_binary_core::NumericBinaryOp;
use super::numeric_binary_core::evaluate_binary_rows;
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
    math_numeric::NumericArrayView,
};
#[cfg(test)]
use arrow_array::Float64Array;
use arrow_array::{Array, ArrayRef, builder::Float64Builder};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::sync::Arc;
fn checked_view<'a>(
    array: &'a ArrayRef,
    data_type: &DataType,
) -> Result<NumericArrayView<'a>, KernelFailure> {
    if array.data_type() != data_type {
        return Err(internal(
            "binary numeric carrier differs from its checked argument",
        ));
    }
    if !matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
    ) {
        return Err(invalid(
            "binary numeric input is not an installed numeric profile",
        ));
    }
    NumericArrayView::new(array).map_err(KernelFailure::from_domain)
}
pub(super) fn evaluate_numeric_binary<'a>(
    op: NumericBinaryOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let [
        FunctionArgumentType::Value(left_type),
        FunctionArgumentType::Value(right_type),
    ] = input.contract().selected().argument_types.as_ref()
    else {
        return Err(invalid("binary numeric requires two exact value arguments"));
    };
    let [left, right] = input.arguments() else {
        return Err(invalid("binary numeric requires two evaluated arguments"));
    };
    let target = input.contract().result_type();
    if left_type.logical_type() != ValueLogicalType::Physical
        || right_type.logical_type() != ValueLogicalType::Physical
        || target.logical_type() != ValueLogicalType::Physical
        || target.data_type() != &DataType::Float64
        || target.nullable() != (left_type.nullable() || right_type.nullable())
    {
        return Err(invalid(
            "binary numeric logical or result type differs from its exact profile",
        ));
    }
    let left_array = left.array();
    let right_array = right.array();
    let left_view = checked_view(left_array, left_type.data_type())?;
    let right_view = checked_view(right_array, right_type.data_type())?;
    let selection = input.selection();
    output_capacity(selection.len())?;
    let mut builder = Float64Builder::with_capacity(selection.len());
    let mut work = EvaluationCheckpoints::new(control);
    evaluate_binary_rows(
        op,
        &left_view,
        &right_view,
        selection.iter(),
        |ordinal, batch_row| {
            let left_row = left.value_row(ordinal, batch_row);
            let right_row = right.value_row(ordinal, batch_row);
            if left_row >= left_array.len() || right_row >= right_array.len() {
                return Err(internal(
                    "binary numeric selected row is outside its checked carrier",
                ));
            }
            if (left_array.is_null(left_row) && !left_type.nullable())
                || (right_array.is_null(right_row) && !right_type.nullable())
            {
                return Err(internal(
                    "binary numeric non-null input contains selected SQL NULL",
                ));
            }
            Ok((left_row, right_row))
        },
        |value| {
            match value {
                Some(value) => builder.append_value(value),
                None => builder.append_null(),
            };
            Ok(())
        },
        || work.step(),
    )?;
    let values = Arc::new(builder.finish()) as ArrayRef;
    work.finish()?;
    SelectedValues::try_new(selection, target.data_type(), values, Box::default())
        .map_err(|_| internal("binary numeric compact output violates its selected contract"))
}
/// Rust allocation representability only; this does not authorize memory.
fn output_capacity(rows: usize) -> Result<(), KernelFailure> {
    let values = rows
        .checked_mul(8)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let bitmap = rows
        .checked_add(7)
        .map(|bits| bits / 8)
        .ok_or(KernelFailure::ResourceExhausted)?;
    isize::try_from(values).map_err(|_| KernelFailure::ResourceExhausted)?;
    isize::try_from(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    values
        .checked_add(bitmap)
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EvaluatedArgument, FunctionValueType, ScalarEvaluationInstance, Selection};
    use std::{sync::Mutex, time::Duration};

    #[derive(Default)]
    struct Control {
        calls: Mutex<Vec<u32>>,
        refusal: Option<(usize, KernelFailure)>,
    }
    impl Control {
        fn refusing(index: usize, error: KernelFailure) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                refusal: Some((index, error)),
            }
        }
        fn calls(&self) -> Vec<u32> {
            self.calls.lock().unwrap().clone()
        }
    }
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
            let mut calls = self.calls.lock().unwrap();
            let index = calls.len();
            calls.push(units);
            if let Some((at, error)) = &self.refusal
                && *at == index
            {
                return Err(error.clone());
            }
            Ok(())
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("binary numeric kernels must not wait");
        }
    }
    fn instance(name: &str, types: &[FunctionValueType; 2]) -> ScalarEvaluationInstance {
        let prepared = super::super::numeric_binary_owner::prepared_for_test(name, types).unwrap();
        ScalarEvaluationInstance::instantiate(prepared).unwrap()
    }
    fn dense(name: &str, left: ArrayRef, right: ArrayRef) -> ArrayRef {
        let types = [
            FunctionValueType::from_legacy_declared_projection(left.data_type().clone(), true),
            FunctionValueType::from_legacy_declared_projection(right.data_type().clone(), true),
        ];
        let mut instance = instance(name, &types);
        let arguments = [
            EvaluatedArgument::Column(&left),
            EvaluatedArgument::Column(&right),
        ];
        let output = instance
            .evaluate(Selection::all(left.len()), &arguments, &Control::default())
            .unwrap();
        assert!(output.errors().is_empty());
        output.into_parts().1
    }
    fn floats(name: &str, left: Vec<Option<f64>>, right: Vec<Option<f64>>) -> ArrayRef {
        dense(
            name,
            Arc::new(Float64Array::from(left)),
            Arc::new(Float64Array::from(right)),
        )
    }
    fn assert_values(array: &ArrayRef, expected: &[Option<f64>]) {
        let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(array.len(), expected.len());
        for (row, expected) in expected.iter().enumerate() {
            match expected {
                Some(value) => {
                    assert!(!array.is_null(row));
                    let actual = array.value(row);
                    if value.is_nan() {
                        assert!(actual.is_nan());
                    } else if *value == 0.0 || value.is_infinite() {
                        assert_eq!(actual.to_bits(), value.to_bits(), "signed zero");
                    } else {
                        assert!(
                            (actual - value).abs() <= value.abs().max(1.0) * 1e-14,
                            "actual={actual:?} expected={value:?}"
                        );
                    }
                }
                None => assert!(array.is_null(row)),
            }
        }
    }

    #[test]
    fn pow_preserves_raw_nan_and_zero_results() {
        let result = floats(
            "pow",
            vec![
                Some(f64::NAN),
                Some(1.0),
                Some(f64::INFINITY),
                Some(-1.0),
                Some(-0.0),
                Some(-0.0),
                Some(-0.0),
                Some(-2.0),
                Some(1e308),
                None,
            ],
            vec![
                Some(0.0),
                Some(f64::NAN),
                Some(0.0),
                Some(f64::INFINITY),
                Some(3.0),
                Some(2.0),
                Some(-1.0),
                Some(0.5),
                Some(2.0),
                Some(0.0),
            ],
        );
        assert_values(
            &result,
            &[
                Some(1.0),
                Some(f64::NAN),
                Some(1.0),
                Some(f64::NAN),
                Some(-0.0),
                Some(0.0),
                Some(f64::NEG_INFINITY),
                Some(f64::NAN),
                Some(f64::INFINITY),
                None,
            ],
        );
    }

    #[test]
    fn each_installed_alias_preserves_the_pow_body() {
        for name in ["pow", "power"] {
            let output = floats(
                name,
                vec![Some(2.0), Some(f64::NAN), None],
                vec![Some(3.0), Some(0.0), Some(0.0)],
            );
            assert_values(&output, &[Some(8.0), Some(1.0), None]);
        }
    }

    #[test]
    fn empty_selection_preserves_exact_result_and_skips_binary_body() {
        let left: ArrayRef = Arc::new(Float64Array::from(vec![None]));
        let right: ArrayRef = Arc::new(Float64Array::from(vec![0.0]));
        let arguments = [
            EvaluatedArgument::Scalar(&left),
            EvaluatedArgument::Scalar(&right),
        ];
        let source = FunctionValueType::new(DataType::Float64, true);
        let mut instance = instance("pow", &[source.clone(), source]);
        let control = Control::default();
        let result = instance
            .evaluate(Selection::all(0), &arguments, &control)
            .unwrap();
        assert!(result.values().is_empty());
        assert_eq!(result.values().data_type(), &DataType::Float64);
        assert!(result.errors().is_empty());
        assert_eq!(
            control.calls().iter().filter(|units| **units == 0).count(),
            3
        );
    }

    #[test]
    fn every_body_keeps_entry_256_tail_and_publication_errors_typed_and_latched() {
        let left: ArrayRef = Arc::new(Float64Array::from(vec![2.0]));
        let right: ArrayRef = Arc::new(Float64Array::from(vec![3.0]));
        let arguments = [
            EvaluatedArgument::Scalar(&left),
            EvaluatedArgument::Scalar(&right),
        ];
        let source = FunctionValueType::new(DataType::Float64, true);
        let types = [source.clone(), source];
        let selection = Selection::all(320);
        for name in ["atan2", "power", "pow"] {
            let mut baseline = instance(name, &types);
            let control = Control::default();
            baseline.evaluate(selection, &arguments, &control).unwrap();
            let calls = control.calls();
            let body_entry = calls
                .iter()
                .enumerate()
                .filter(|(_, units)| **units == 0)
                .nth(3)
                .unwrap()
                .0;
            let interior = calls.iter().position(|units| *units == 256).unwrap();
            assert!(body_entry < interior);
            assert_eq!(calls[interior + 1], 64);
            for error in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
            ] {
                for at in [0, body_entry, interior, interior + 1, calls.len() - 1] {
                    let mut instance = instance(name, &types);
                    let control = Control::refusing(at, error.clone());
                    assert_eq!(
                        instance
                            .evaluate(selection, &arguments, &control)
                            .unwrap_err(),
                        error
                    );
                    assert_eq!(control.calls().len(), at + 1);
                    assert_eq!(
                        instance
                            .evaluate(selection, &arguments, &Control::default())
                            .unwrap_err(),
                        KernelFailure::InstanceFailed
                    );
                }
            }
        }
    }

    #[test]
    fn capacity_overflow_fails_before_builder_allocation_or_row_formula() {
        let left: ArrayRef = Arc::new(Float64Array::from(vec![2.0]));
        let right: ArrayRef = Arc::new(Float64Array::from(vec![0.0]));
        let arguments = [
            EvaluatedArgument::Scalar(&left),
            EvaluatedArgument::Scalar(&right),
        ];
        let source = FunctionValueType::new(DataType::Float64, true);
        for name in ["atan2", "power", "pow"] {
            let mut instance = instance(name, &[source.clone(), source.clone()]);
            let control = Control::default();
            assert_eq!(
                instance
                    .evaluate(Selection::all(usize::MAX), &arguments, &control)
                    .unwrap_err(),
                KernelFailure::ResourceExhausted
            );
            assert!(control.calls().iter().all(|units| *units < 256));
            assert_eq!(
                control.calls().iter().filter(|units| **units == 0).count(),
                4
            );
        }
    }

    #[test]
    fn shared_binary_every_actual_callback_preserves_all_seven_causes_and_failed_latch() {
        let left: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN]));
        let right: ArrayRef = Arc::new(Float64Array::from(vec![0.0]));
        let arguments = [
            EvaluatedArgument::Scalar(&left),
            EvaluatedArgument::Scalar(&right),
        ];
        let types = [
            FunctionValueType::new(DataType::Float64, false),
            FunctionValueType::new(DataType::Float64, false),
        ];
        let selection = Selection::all(513);
        for name in ["atan2", "pow", "power"] {
            let mut baseline = instance(name, &types);
            let control = Control::default();
            baseline.evaluate(selection, &arguments, &control).unwrap();
            for cause in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
                crate::kernel_control::invalid("original invalid"),
                crate::kernel_control::internal("original internal"),
                KernelFailure::Operational(crate::kernel_control::KernelDiagnostic::new(
                    "original operational",
                )),
                KernelFailure::InstanceFailed,
            ] {
                for at in 0..control.calls().len() {
                    let mut kernel = instance(name, &types);
                    let refusal = Control::refusing(at, cause.clone());
                    assert_eq!(
                        kernel
                            .evaluate(selection, &arguments, &refusal)
                            .unwrap_err(),
                        cause
                    );
                    assert_eq!(refusal.calls().len(), at + 1);
                    assert_eq!(
                        kernel
                            .evaluate(selection, &arguments, &Control::default())
                            .unwrap_err(),
                        KernelFailure::InstanceFailed
                    );
                }
            }
        }
    }
}
