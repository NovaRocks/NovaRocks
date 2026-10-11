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
use crate::{ConstantPolicy, ConstantPool, ScalarEvaluationInstance, Selection};
use arrow_array::{Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int64Array};
use arrow_cast::cast::DecimalCast;
use novarocks_type_contract::CompilePhase;
use novarocks_type_contract::DecimalOverflowPolicy;
use std::sync::Arc;
use std::{sync::Mutex, time::Duration};

#[derive(Default)]
struct Control {
    calls: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl Control {
    fn refusing(at: usize, error: KernelFailure) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            refusal: Some((at, error)),
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
        let at = calls.len();
        calls.push(units);
        if let Some((refuse_at, error)) = &self.refusal
            && at == *refuse_at
        {
            return Err(error.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("truncate must not wait");
    }
}
fn instance(
    types: &[FunctionValueType],
    policy: DecimalOverflowPolicy,
) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        super::super::truncate_owner::prepared_for_test(types, &vec![None; types.len()], policy)
            .unwrap(),
    )
    .unwrap()
}
fn dense(arrays: Vec<ArrayRef>) -> ArrayRef {
    let rows = arrays[0].len();
    let types = arrays
        .iter()
        .map(|a| FunctionValueType::new(a.data_type().clone(), true))
        .collect::<Vec<_>>();
    let mut instance = instance(&types, DecimalOverflowPolicy::OutputNull);
    let arguments = arrays
        .iter()
        .map(EvaluatedArgument::Column)
        .collect::<Vec<_>>();
    let output = instance
        .evaluate(Selection::all(rows), &arguments, &Control::default())
        .unwrap();
    assert!(output.errors().is_empty());
    output.into_parts().1
}
fn floats(values: Vec<Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}
fn signed(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}
fn decimals(values: Vec<Option<i128>>, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(38, scale)
            .unwrap(),
    )
}
fn assert_floats(array: &ArrayRef, expected: &[Option<f64>]) {
    assert_eq!(array.data_type(), &DataType::Float64);
    let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(array.len(), expected.len());
    for (row, value) in expected.iter().enumerate() {
        match value {
            Some(value) => {
                assert!(!array.is_null(row));
                let actual = array.value(row);
                if *value == 0.0 {
                    assert_eq!(actual.to_bits(), value.to_bits(), "signed zero row {row}");
                } else {
                    assert!(
                        (actual - value).abs() <= value.abs().max(1.0) * 1e-14,
                        "row {row}: actual {actual} expected {value}"
                    );
                }
            }
            None => assert!(array.is_null(row), "row {row}"),
        }
    }
}
fn assert_integers(array: &ArrayRef, expected: &[Option<i64>]) {
    assert_eq!(array.data_type(), &DataType::Int64);
    let actual = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}
fn assert_decimals(array: &ArrayRef, scale: i8, expected: &[Option<i128>]) {
    assert_eq!(array.data_type(), &DataType::Decimal128(38, scale));
    let actual = array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap()
        .iter()
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

#[test]
fn unary_checked_integer_boundaries_preserve_original_float_read_precision() {
    let min = f64::from_bits(0xc3e0000000000000);
    let below_min = f64::from_bits(0xc3e0000000000001);
    let below_upper = f64::from_bits(0x43dfffffffffffff);
    let upper = f64::from_bits(0x43e0000000000000);
    assert_eq!(<i64 as DecimalCast>::from_f64(min), Some(i64::MIN));
    assert_eq!(<i64 as DecimalCast>::from_f64(upper), None);
    assert_integers(
        &dense(vec![floats(vec![
            Some(min),
            Some(below_min),
            Some(below_upper),
            Some(upper),
            Some(-2.9),
            Some(f64::NAN),
            Some(f64::INFINITY),
            None,
        ])]),
        &[
            Some(i64::MIN),
            None,
            Some(9_223_372_036_854_774_784),
            None,
            Some(-2),
            None,
            None,
            None,
        ],
    );
    assert_integers(
        &dense(vec![signed(vec![
            Some(i64::MIN),
            Some(i64::MAX),
            Some(i64::MAX - 1023),
            Some(9_007_199_254_740_993),
        ])]),
        &[
            Some(i64::MIN),
            None,
            Some(9_223_372_036_854_774_784),
            Some(9_007_199_254_740_992),
        ],
    );
}

#[test]
fn binary_float_truncation_preserves_sign_and_successful_nulls() {
    assert_floats(
        &dense(vec![
            floats(vec![
                Some(1.99),
                Some(-1.99),
                Some(199.9),
                Some(-199.9),
                Some(-0.0),
                Some(-0.1),
                Some(f64::NAN),
                Some(f64::INFINITY),
                None,
                Some(1.0),
            ]),
            signed(vec![
                Some(1),
                Some(1),
                Some(-2),
                Some(-2),
                Some(2),
                Some(0),
                Some(0),
                Some(0),
                Some(0),
                None,
            ]),
        ]),
        &[
            Some(1.9),
            Some(-1.9),
            Some(100.0),
            Some(-100.0),
            Some(-0.0),
            Some(-0.0),
            None,
            None,
            None,
            None,
        ],
    );
}

#[test]
fn signed_digits_preserve_original_wide_exponent_projection() {
    assert_floats(
        &dense(vec![
            floats(vec![Some(12.99); 5]),
            signed(vec![
                Some(i64::MAX),
                Some(4_294_967_296),
                Some(4_294_967_297),
                Some(9_007_199_254_740_992),
                Some(9_007_199_254_740_993),
            ]),
        ]),
        &[Some(10.0), Some(12.0), Some(12.9), Some(12.0), Some(12.9)],
    );
}

#[test]
fn float_digits_truncate_saturate_and_refuse_nonfinite_values() {
    assert_floats(
        &dense(vec![
            floats(vec![Some(123.99); 7]),
            floats(vec![
                Some(1.9),
                Some(-1.9),
                Some(f64::MAX),
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                None,
            ]),
        ]),
        &[
            Some(123.9),
            Some(120.0),
            Some(120.0),
            None,
            None,
            None,
            None,
        ],
    );
}

#[test]
fn decimal_digits_use_lossy_float_reader_then_integer_saturation() {
    assert_floats(
        &dense(vec![
            floats(vec![Some(12.99)]),
            decimals(vec![Some(9_007_199_254_740_993)], 0),
        ]),
        &[Some(12.0)],
    );
    assert_floats(
        &dense(vec![
            floats(vec![Some(123.99), Some(123.99), Some(123.99)]),
            decimals(vec![Some(199), Some(-199), None], 2),
        ]),
        &[Some(123.9), Some(120.0), None],
    );
    // With negative scale the raw value is multiplied before the digits cast.
    assert_floats(
        &dense(vec![
            floats(vec![Some(123.99)]),
            decimals(vec![Some(1)], -1),
        ]),
        &[Some(123.99)],
    );
}

#[test]
fn decimal_unary_output_preserves_floating_conversion_and_extreme_scale() {
    assert_decimals(
        &dense(vec![decimals(
            vec![
                Some(9_007_199_254_740_993),
                Some(-9_007_199_254_740_993),
                None,
            ],
            0,
        )]),
        0,
        &[
            Some(9_007_199_254_740_992),
            Some(-9_007_199_254_740_992),
            None,
        ],
    );
    assert_decimals(
        &dense(vec![decimals(vec![Some(123), Some(-123), None], -2)]),
        -2,
        &[Some(123), Some(-123), None],
    );
    assert_decimals(
        &dense(vec![decimals(vec![Some(1), Some(-1), None], 38)]),
        38,
        &[Some(0), Some(0), None],
    );
    assert_decimals(
        &dense(vec![decimals(vec![Some(1), Some(-1), None], i8::MIN)]),
        i8::MIN,
        &[Some(1), Some(-1), None],
    );
}

#[test]
fn legal_decimal_precision_overflow_is_successful_null_under_both_policies() {
    const MAX38: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;
    // Independent IEEE arithmetic: MAX38 rounds to binary64 bits
    // 0x47d2ced32a16a1b1. At scale -46, decode then re-encode rounds
    // to 100000000000000016638275754934614884352, beyond precision 38.
    assert_eq!((MAX38 as f64).to_bits(), 0x47d2ced32a16a1b1);
    let ty = FunctionValueType::new(DataType::Decimal128(38, -46), false);
    let array = decimals(
        vec![
            Some(0),
            Some(MAX38),
            Some(0),
            Some(0),
            Some(MAX38),
            Some(MAX38),
        ],
        -46,
    );
    let rows = [2, 5];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let prepared = super::super::truncate_owner::prepared_for_test(
            std::slice::from_ref(&ty),
            &[None],
            policy,
        )
        .unwrap();
        assert_eq!(
            prepared.contract().result_type().data_type,
            DataType::Decimal128(38, -46)
        );
        let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
        let arguments = [EvaluatedArgument::Column(&array)];
        let output = instance
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(output.selection().iter().collect::<Vec<_>>(), rows);
        assert_decimals(output.values(), -46, &[Some(0), None]);
        assert!(output.errors().is_empty());
    }
}

#[test]
fn sparse_column_scalar_and_compact_arguments_keep_each_own_address_mapping() {
    let value_ty = FunctionValueType::new(DataType::Float64, true);
    let digit_ty = FunctionValueType::new(DataType::Int64, true);
    let rows = [1, 4];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    let column = floats(vec![
        Some(f64::NAN),
        Some(1.99),
        None,
        Some(f64::INFINITY),
        Some(-199.9),
        None,
    ]);
    let compact_values = signed(vec![Some(1), Some(-2)]);
    let compact =
        SelectedValues::try_new(selection, &DataType::Int64, compact_values, Box::default())
            .unwrap();
    let types = [value_ty, digit_ty];
    let mut call = instance(&types, DecimalOverflowPolicy::OutputNull);
    let arguments = [
        EvaluatedArgument::Column(&column),
        EvaluatedArgument::SelectedColumn(&compact),
    ];
    let output = call
        .evaluate(selection, &arguments, &Control::default())
        .unwrap();
    assert_floats(output.values(), &[Some(1.9), Some(-100.0)]);
    assert_eq!(output.selection().iter().collect::<Vec<_>>(), rows);
    let scalar = floats(vec![Some(123.99)]);
    let mut call = instance(&types, DecimalOverflowPolicy::OutputNull);
    assert_floats(
        call.evaluate(
            selection,
            &[
                EvaluatedArgument::Scalar(&scalar),
                EvaluatedArgument::SelectedColumn(&compact),
            ],
            &Control::default(),
        )
        .unwrap()
        .values(),
        &[Some(123.9), Some(100.0)],
    );
}

fn pool(ty: FunctionValueType, array: ArrayRef) -> ConstantPool {
    ConstantPool::try_new(
        Arc::new(ty.try_to_field("truncate-input").unwrap()),
        ty,
        array.to_data(),
        ConstantPolicy {
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
        },
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap()
}
#[test]
fn constant_pool_selected_ordinals_broadcast_without_reading_unused_rows() {
    let value_ty = FunctionValueType::new(DataType::Float64, true);
    let digit_ty = FunctionValueType::new(DataType::Int64, true);
    let values = pool(
        value_ty.clone(),
        floats(vec![None, Some(12.99), Some(-12.99)]),
    );
    let digits = pool(digit_ty.clone(), signed(vec![None, Some(2), Some(1)]));
    let value = values.value(1).unwrap();
    let digit = digits.value(2).unwrap();
    assert!(Arc::ptr_eq(value.pool().array(), values.array()));
    let mut call = instance(
        &[value_ty.clone(), digit_ty],
        DecimalOverflowPolicy::OutputNull,
    );
    assert_floats(
        call.evaluate(
            Selection::all(3),
            &[
                EvaluatedArgument::Constant(&value),
                EvaluatedArgument::Constant(&digit),
            ],
            &Control::default(),
        )
        .unwrap()
        .values(),
        &[Some(12.9); 3],
    );
    let null = values.value(0).unwrap();
    let mut call = instance(&[value_ty], DecimalOverflowPolicy::OutputNull);
    let arguments = [EvaluatedArgument::Constant(&null)];
    let output = call
        .evaluate(Selection::all(2), &arguments, &Control::default())
        .unwrap();
    assert_integers(output.values(), &[None; 2]);
    assert!(output.errors().is_empty());
}

fn control_fixture(arity: usize) -> (Vec<FunctionValueType>, Vec<ArrayRef>) {
    let mut arrays = vec![floats(vec![Some(1.99)])];
    if arity == 2 {
        arrays.push(signed(vec![Some(1)]));
    }
    let types = arrays
        .iter()
        .map(|array| FunctionValueType::new(array.data_type().clone(), true))
        .collect();
    (types, arrays)
}
#[test]
fn original_core_control_refusals_preserve_every_prefix_and_poison_instance() {
    let profiles = [
        vec![floats(vec![Some(1.99)])],
        vec![floats(vec![Some(1.99)]), signed(vec![Some(1)])],
        vec![decimals(vec![Some(199)], 2), signed(vec![Some(1)])],
    ];
    for arrays in profiles {
        let types = arrays
            .iter()
            .map(|a| FunctionValueType::new(a.data_type().clone(), true))
            .collect::<Vec<_>>();
        let args = arrays
            .iter()
            .map(EvaluatedArgument::Scalar)
            .collect::<Vec<_>>();
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for rows in [320, 1025] {
                let selection = Selection::all(rows);
                let mut baseline = instance(&types, policy);
                let control = Control::default();
                baseline.evaluate(selection, &args, &control).unwrap();
                let calls = control.calls();
                assert!(calls.contains(&256));
                // Both the original math and Float64 sanitizer traverse rows.
                assert!(calls.iter().map(|n| *n as usize).sum::<usize>() >= rows * 2);
                for error in [
                    KernelFailure::Cancelled,
                    KernelFailure::DeadlineExceeded,
                    KernelFailure::ResourceExhausted,
                ] {
                    for at in 0..calls.len() {
                        let mut call = instance(&types, policy);
                        let control = Control::refusing(at, error.clone());
                        assert_eq!(
                            call.evaluate(selection, &args, &control).unwrap_err(),
                            error
                        );
                        assert_eq!(control.calls(), calls[..=at]);
                        let retry = Control::default();
                        assert_eq!(
                            call.evaluate(selection, &args, &retry).unwrap_err(),
                            KernelFailure::InstanceFailed
                        );
                        assert!(retry.calls().is_empty());
                    }
                }
            }
        }
    }
}

#[test]
fn minimum_digits_keep_original_fatal_payload_instead_of_unsigned_magnitude() {
    use super::super::dround_core::{DroundComputation, evaluate_legacy_dround};
    use std::panic::{AssertUnwindSafe, catch_unwind};
    fn fatal(f: impl FnOnce() -> ArrayRef) -> (String, String) {
        let payload = catch_unwind(AssertUnwindSafe(f)).unwrap_err();
        if let Some(s) = payload.downcast_ref::<String>() {
            ("String".into(), s.clone())
        } else if let Some(s) = payload.downcast_ref::<&str>() {
            ("&str".into(), (*s).into())
        } else {
            panic!("unknown TRUNCATE fatal payload")
        }
    }
    let digits = [
        signed(vec![Some(i64::MIN)]),
        floats(vec![Some(-f64::MAX)]),
        Arc::new(Float32Array::from(vec![Some(-f32::MAX)])) as ArrayRef,
        Arc::new(
            Decimal128Array::from(vec![Some(-1)])
                .with_precision_and_scale(1, -19)
                .unwrap(),
        ) as ArrayRef,
    ];
    for digits in digits {
        for value in [12.99, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let value = floats(vec![Some(value)]);
            let original = fatal(|| {
                evaluate_legacy_dround(
                    DroundComputation::TruncateDigits,
                    &value,
                    Some(&digits),
                    1,
                    Some(&DataType::Float64),
                )
                .unwrap()
            });
            let selected = fatal(|| dense(vec![value.clone(), digits.clone()]));
            assert_eq!(original, selected);
        }
    }
}

#[test]
fn output_capacity_refuses_unrepresentable_rows_before_private_builder_work() {
    output_capacity(0).unwrap();
    assert_eq!(
        output_capacity(usize::MAX).unwrap_err(),
        KernelFailure::ResourceExhausted
    );
    for arity in [1, 2] {
        let (types, arrays) = control_fixture(arity);
        let args = arrays
            .iter()
            .map(EvaluatedArgument::Scalar)
            .collect::<Vec<_>>();
        let mut call = instance(&types, DecimalOverflowPolicy::OutputNull);
        let control = Control::default();
        assert_eq!(
            call.evaluate(Selection::all(usize::MAX), &args, &control)
                .unwrap_err(),
            KernelFailure::ResourceExhausted
        );
        assert!(control.calls().iter().all(|units| *units < 256));
    }
}

#[test]
fn empty_selection_skips_private_body_and_keeps_exact_result_carrier() {
    for arity in [1, 2] {
        let (types, arrays) = control_fixture(arity);
        let args = arrays
            .iter()
            .map(EvaluatedArgument::Scalar)
            .collect::<Vec<_>>();
        let mut call = instance(&types, DecimalOverflowPolicy::OutputNull);
        let control = Control::default();
        let output = call.evaluate(Selection::all(0), &args, &control).unwrap();
        assert!(output.values().is_empty());
        assert!(output.errors().is_empty());
        assert_eq!(
            output.values().data_type(),
            if arity == 1 {
                &DataType::Int64
            } else {
                &DataType::Float64
            }
        );
        assert_eq!(
            control.calls().iter().filter(|units| **units == 0).count(),
            arity + 1
        );
        assert!(control.calls().iter().all(|units| *units < 256));
    }
}
