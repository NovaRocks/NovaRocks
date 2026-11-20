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
//! Selected MOD/PMOD contracts and addresses delegate to the ONE original core.
//! Arrow builder allocation still requires formal host memory admission.
pub(super) use super::numeric_mod_core::NumericModOp;
use super::numeric_mod_core::{NumericModError, evaluate_mod_rows};
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
    math_numeric::NumericArrayView,
};
use arrow_array::{Array, ArrayRef, builder::Float64Builder};
#[cfg(test)]
use arrow_array::{Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::sync::Arc;
fn checked_view<'a>(
    array: &'a ArrayRef,
    data_type: &DataType,
) -> Result<NumericArrayView<'a>, KernelFailure> {
    if array.data_type() != data_type {
        return Err(internal(
            "integer remainder carrier differs from its checked argument",
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
            "integer remainder input is not an installed numeric profile",
        ));
    }
    NumericArrayView::new(array).map_err(KernelFailure::from_domain)
}
pub(super) fn evaluate_numeric_mod<'a>(
    op: NumericModOp,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let [
        FunctionArgumentType::Value(left_type),
        FunctionArgumentType::Value(right_type),
    ] = input.contract().selected().argument_types.as_ref()
    else {
        return Err(invalid(
            "integer remainder requires two exact value arguments",
        ));
    };
    let [left, right] = input.arguments() else {
        return Err(invalid(
            "integer remainder requires two evaluated arguments",
        ));
    };
    let target = input.contract().result_type();
    if left_type.logical_type() != ValueLogicalType::Physical
        || right_type.logical_type() != ValueLogicalType::Physical
        || target.logical_type() != ValueLogicalType::Physical
        || target.data_type() != &DataType::Float64
        || !target.nullable()
    {
        return Err(invalid(
            "integer remainder logical or result type differs from its exact profile",
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
    evaluate_mod_rows(
        op,
        &left_view,
        &right_view,
        selection.iter(),
        |ordinal, batch_row| {
            let left_row = left.value_row(ordinal, batch_row);
            let right_row = right.value_row(ordinal, batch_row);
            if left_row >= left_array.len() || right_row >= right_array.len() {
                return Err(internal(
                    "integer remainder selected row is outside its checked carrier",
                ));
            }
            if (left_array.is_null(left_row) && !left_type.nullable())
                || (right_array.is_null(right_row) && !right_type.nullable())
            {
                return Err(internal(
                    "integer remainder non-null input contains selected SQL NULL",
                ));
            }
            Ok((left_row, right_row))
        },
        |value| {
            match value {
                Some(value) => builder.append_value(value as f64),
                None => builder.append_null(),
            };
            Ok(())
        },
        || work.step(),
    )
    .map_err(NumericModError::into_kernel)?;
    let values = Arc::new(builder.finish()) as ArrayRef;
    work.finish()?;
    SelectedValues::try_new(selection, target.data_type(), values, Box::default())
        .map_err(|_| internal("integer remainder compact output violates its selected contract"))
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
    use crate::{
        ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionValueType,
        ScalarEvaluationInstance, Selection,
    };
    use novarocks_type_contract::CompilePhase;
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
        let prepared = super::super::numeric_mod_owner::prepared_for_test(name, types).unwrap();
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
                    if *value == 0.0 {
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
    fn signed_minimum_maximum_and_negative_divisors_have_independent_oracles() {
        let left = [
            i64::MIN,
            -1,
            i64::MIN,
            i64::MAX,
            i64::MIN,
            i64::MAX,
            -7,
            -7,
            7,
            7,
            3,
        ];
        let right = [
            -1,
            i64::MIN,
            i64::MIN,
            i64::MIN,
            i64::MAX,
            2,
            3,
            -3,
            3,
            -3,
            0,
        ];
        let expected_mod = [
            Some(0.0),
            Some(-1.0),
            Some(0.0),
            Some(9223372036854775808.0),
            Some(-1.0),
            Some(1.0),
            Some(-1.0),
            Some(-1.0),
            Some(1.0),
            Some(1.0),
            None,
        ];
        for (name, expected) in [("mod", expected_mod)] {
            let result = dense(
                name,
                Arc::new(Int64Array::from(left.to_vec())),
                Arc::new(Int64Array::from(right.to_vec())),
            );
            assert_values(&result, &expected);
        }
    }

    #[test]
    fn signed_sources_keep_full_integer_precision_until_final_float_output() {
        for name in ["mod"] {
            let result = dense(
                name,
                Arc::new(Int64Array::from(vec![9007199254740993, i64::MAX, i64::MIN])),
                Arc::new(Int64Array::from(vec![2, 2, -3])),
            );
            assert_values(
                &result,
                &[
                    Some(1.0),
                    Some(1.0),
                    Some(if name == "mod" { -2.0 } else { 1.0 }),
                ],
            );
        }
    }

    #[test]
    fn finite_float_inputs_keep_rust_truncating_saturation_not_arrow_safe_cast() {
        let left = vec![
            Some(-7.9),
            Some(f64::MAX),
            Some(-f64::MAX),
            Some(-1.0),
            Some(-1.0),
            Some(7.9),
        ];
        let right = vec![
            Some(3.9),
            Some(2.0),
            Some(-1.0),
            Some(f64::MAX),
            Some(-f64::MAX),
            Some(0.9),
        ];
        for (name, expected) in [(
            "mod",
            [
                Some(-1.0),
                Some(1.0),
                Some(0.0),
                Some(-1.0),
                Some(-1.0),
                None,
            ],
        )] {
            assert_values(&floats(name, left.clone(), right.clone()), &expected);
            let result = dense(
                name,
                Arc::new(Float32Array::from(vec![f32::MAX])),
                Arc::new(Int8Array::from(vec![2])),
            );
            assert_values(&result, &[Some(1.0)]);
        }
    }

    #[test]
    fn nonfinite_zero_divisor_and_null_are_successful_null_without_row_errors() {
        for name in ["mod"] {
            let result = floats(
                name,
                vec![
                    Some(f64::NAN),
                    Some(f64::INFINITY),
                    Some(3.0),
                    Some(3.0),
                    Some(3.0),
                    None,
                    Some(3.0),
                    Some(-0.0),
                ],
                vec![
                    Some(1.0),
                    Some(1.0),
                    Some(f64::NAN),
                    Some(f64::NEG_INFINITY),
                    Some(0.0),
                    Some(2.0),
                    None,
                    Some(3.0),
                ],
            );
            assert_values(
                &result,
                &[None, None, None, None, None, None, None, Some(0.0)],
            );
        }
    }

    #[test]
    fn all_36_profiles_for_each_real_owner_use_the_original_integer_reader() {
        fn profiles(value: i8) -> [ArrayRef; 6] {
            [
                Arc::new(Int8Array::from(vec![value])),
                Arc::new(Int16Array::from(vec![i16::from(value)])),
                Arc::new(Int32Array::from(vec![i32::from(value)])),
                Arc::new(Int64Array::from(vec![i64::from(value)])),
                Arc::new(Float32Array::from(vec![f32::from(value)])),
                Arc::new(Float64Array::from(vec![f64::from(value)])),
            ]
        }
        for (name, expected) in [("mod", -1.0)] {
            let mut pairs = 0;
            for left in profiles(-7) {
                for right in profiles(3) {
                    assert_values(&dense(name, left.clone(), right), &[Some(expected)]);
                    pairs += 1;
                }
            }
            assert_eq!(pairs, 36);
        }
    }

    #[test]
    fn sparse_original_and_compact_arguments_keep_independent_row_maps() {
        let rows = [1, 4];
        let selection = Selection::try_sparse(6, &rows).unwrap();
        let left: ArrayRef = Arc::new(Int64Array::from(vec![999, -7, 999, 999, -8, 999]));
        let right: ArrayRef = Arc::new(Int16Array::from(vec![3, -3]));
        let compact =
            SelectedValues::try_new(selection, &DataType::Int16, right, Box::default()).unwrap();
        let arguments = [
            EvaluatedArgument::Column(&left),
            EvaluatedArgument::SelectedColumn(&compact),
        ];
        let types = [
            FunctionValueType::new(DataType::Int64, false),
            FunctionValueType::new(DataType::Int16, false),
        ];
        let mut instance = instance("mod", &types);
        let result = instance
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(result.selection(), selection);
        assert_values(result.values(), &[Some(-1.0), Some(-2.0)]);
    }

    #[test]
    fn each_side_scalar_broadcast_is_explicit_and_selected_null_is_strict() {
        let rows = [1, 4];
        let selection = Selection::try_sparse(6, &rows).unwrap();
        let left: ArrayRef = Arc::new(Int8Array::from(vec![-7]));
        let right: ArrayRef = Arc::new(Int8Array::from(vec![99, 3, 99, 99, -3, 99]));
        let arguments = [
            EvaluatedArgument::Scalar(&left),
            EvaluatedArgument::Column(&right),
        ];
        let types = [
            FunctionValueType::new(DataType::Int8, false),
            FunctionValueType::new(DataType::Int8, false),
        ];
        let mut broadcast = instance("mod", &types);
        assert_values(
            broadcast
                .evaluate(selection, &arguments, &Control::default())
                .unwrap()
                .values(),
            &[Some(-1.0), Some(-1.0)],
        );

        let left: ArrayRef = Arc::new(Float64Array::from(vec![Some(-8.9), None]));
        let compact =
            SelectedValues::try_new(selection, &DataType::Float64, left, Box::default()).unwrap();
        let right: ArrayRef = Arc::new(Int32Array::from(vec![3]));
        let arguments = [
            EvaluatedArgument::SelectedColumn(&compact),
            EvaluatedArgument::Scalar(&right),
        ];
        let types = [
            FunctionValueType::new(DataType::Float64, true),
            FunctionValueType::new(DataType::Int32, false),
        ];
        let mut reverse = instance("mod", &types);
        assert_values(
            reverse
                .evaluate(selection, &arguments, &Control::default())
                .unwrap()
                .values(),
            &[Some(-2.0), None],
        );
        let wrong = [
            EvaluatedArgument::SelectedColumn(&compact),
            EvaluatedArgument::Column(&right),
        ];
        let mut wrong_shape = instance("mod", &types);
        assert!(matches!(
            wrong_shape.evaluate(selection, &wrong, &Control::default()),
            Err(ref cause) if crate::kernel_control::test_contract::input(cause)
        ));
    }

    #[test]
    fn constant_values_keep_independent_nonzero_ordinals_in_one_shared_pool() {
        let ty = FunctionValueType::new(DataType::Int64, true);
        let array: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(-3), Some(-7)]));
        // Explicit finite test policy; the 8192-byte validation bound includes
        // the real owner's scratch model and does not grant runtime memory.
        let policy = ConstantPolicy {
            max_rows: 3,
            max_array_nodes: 1,
            max_logical_elements: 3,
            max_retained_buffer_bytes: 1024,
            max_type_depth: 1,
            max_type_nodes: 1,
            max_dictionary_depth: 0,
            max_metadata_bytes: 1024,
            max_library_validation_work: 4096,
            max_library_validation_bytes: 8192,
        };
        let pool = ConstantPool::try_new(
            Arc::new(ty.try_to_field("mod-input").unwrap()),
            ty.clone(),
            array.to_data(),
            policy,
            CompilePhase::Validate,
            crate::binding_test_control(),
        )
        .unwrap();
        let left = pool.value(2).unwrap();
        let right = pool.value(1).unwrap();
        let arguments = [
            EvaluatedArgument::Constant(&left),
            EvaluatedArgument::Constant(&right),
        ];
        let mut instance = instance("mod", &[ty.clone(), ty]);
        assert_values(
            instance
                .evaluate(Selection::all(3), &arguments, &Control::default())
                .unwrap()
                .values(),
            &[Some(-1.0); 3],
        );
        assert!(Arc::ptr_eq(left.pool().array(), right.pool().array()));
    }

    #[test]
    fn empty_selection_keeps_exact_result_and_skips_private_remainder_body() {
        let left: ArrayRef = Arc::new(Float64Array::from(vec![None]));
        let right: ArrayRef = Arc::new(Float64Array::from(vec![0.0]));
        let arguments = [
            EvaluatedArgument::Scalar(&left),
            EvaluatedArgument::Scalar(&right),
        ];
        let source = FunctionValueType::new(DataType::Float64, true);
        for name in ["mod"] {
            let mut instance = instance(name, &[source.clone(), source.clone()]);
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
    }

    #[test]
    fn both_owners_keep_entry_256_tail_and_publication_failures_typed_and_latched() {
        let left: ArrayRef = Arc::new(Float64Array::from(vec![-7.0]));
        let right: ArrayRef = Arc::new(Float64Array::from(vec![3.0]));
        let arguments = [
            EvaluatedArgument::Scalar(&left),
            EvaluatedArgument::Scalar(&right),
        ];
        let source = FunctionValueType::new(DataType::Float64, true);
        let types = [source.clone(), source];
        let selection = Selection::all(320);
        for name in ["mod"] {
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
    fn capacity_overflow_fails_before_builder_allocation_or_remainder_work() {
        let left: ArrayRef = Arc::new(Float64Array::from(vec![2.0]));
        let right: ArrayRef = Arc::new(Float64Array::from(vec![0.0]));
        let arguments = [
            EvaluatedArgument::Scalar(&left),
            EvaluatedArgument::Scalar(&right),
        ];
        let source = FunctionValueType::new(DataType::Float64, true);
        for name in ["mod"] {
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
    fn shared_mod_every_actual_callback_preserves_all_seven_causes_and_failed_latch() {
        let left: ArrayRef = Arc::new(Float64Array::from(vec![-7.9]));
        let right: ArrayRef = Arc::new(Int64Array::from(vec![i64::MIN]));
        let arguments = [
            EvaluatedArgument::Scalar(&left),
            EvaluatedArgument::Scalar(&right),
        ];
        let types = [
            FunctionValueType::new(DataType::Float64, false),
            FunctionValueType::new(DataType::Int64, false),
        ];
        let selection = Selection::all(513);
        for name in ["mod"] {
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
