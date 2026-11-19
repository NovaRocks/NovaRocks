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
use crate::{
    ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionValueType, ScalarEvaluationInstance,
    Selection,
};
use arrow_array::{types::*, *};
use novarocks_type_contract::CompilePhase;
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
        panic!("round must not wait");
    }
}
fn instance(
    types: &[FunctionValueType],
    policy: DecimalOverflowPolicy,
) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        super::super::round_owner::prepared_for_test(types, &vec![None; types.len()], policy)
            .unwrap(),
    )
    .unwrap()
}
fn dense(arrays: Vec<ArrayRef>) -> ArrayRef {
    let rows = arrays[0].len();
    let types = arrays
        .iter()
        .map(|a| FunctionValueType::from_legacy_declared_projection(a.data_type().clone(), true))
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
                if value.is_nan() {
                    assert!(actual.is_nan(), "NaN row {row}");
                } else if value.is_infinite() {
                    assert_eq!(actual, *value, "infinite row {row}");
                } else if *value == 0.0 {
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
fn raw_text_cast(
    source: &ArrayRef,
    target: super::super::rounding_binding::CastTarget,
) -> ArrayRef {
    use super::super::round_cast::{CastRecipe, CastValue};
    use super::super::rounding_binding::CastTarget;
    use crate::builtin::binding_control;
    let source_type =
        FunctionValueType::from_legacy_declared_projection(source.data_type().clone(), true);
    let recipe = binding_control::scope(crate::binding_test_control(), |work| {
        binding_control::value_type(&source_type, work)?;
        CastRecipe::prepare(&source_type, target, work)
    })
    .unwrap();
    recipe.check_available().unwrap();
    let controller = Control::default();
    let mut work = crate::kernel_input::EvaluationCheckpoints::new(&controller);
    let values = (0..source.len())
        .map(|row| recipe.read(source.as_ref(), row, &mut work).unwrap())
        .collect::<Vec<_>>();
    work.finish().unwrap();
    match target {
        CastTarget::Float64 => Arc::new(Float64Array::from(
            values
                .into_iter()
                .map(|value| match value {
                    CastValue::Float(value) => Some(value),
                    CastValue::Null => None,
                    _ => panic!("raw float text cast returned a foreign value"),
                })
                .collect::<Vec<_>>(),
        )),
        CastTarget::Int64 => Arc::new(Int64Array::from(
            values
                .into_iter()
                .map(|value| match value {
                    CastValue::Integer(value) => Some(value),
                    CastValue::Null => None,
                    _ => panic!("raw integer text cast returned a foreign value"),
                })
                .collect::<Vec<_>>(),
        )),
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
fn unary_round_preserves_rust_saturation_nan_zero_and_half_away_ties() {
    assert_integers(
        &dense(vec![floats(vec![
            Some(2.5),
            Some(-2.5),
            Some(f64::NAN),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(-0.0),
            Some(9223372036854775808.0),
            None,
        ])]),
        &[
            Some(3),
            Some(-3),
            Some(0),
            Some(i64::MAX),
            Some(i64::MIN),
            Some(0),
            Some(i64::MAX),
            None,
        ],
    );
    assert_integers(
        &dense(vec![signed(vec![
            Some(i64::MAX),
            Some(i64::MIN),
            Some(9007199254740993),
        ])]),
        &[Some(i64::MAX), Some(i64::MIN), Some(9007199254740992)],
    );
}
#[test]
fn binary_nonfinite_and_huge_digits_preserve_original_branch_order() {
    assert_floats(
        &dense(vec![
            floats(vec![
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(f64::NAN),
                Some(-0.0),
                Some(f64::NAN),
                Some(-123.0),
                Some(1.25),
                Some(1.25),
                Some(1.25),
                None,
            ]),
            signed(vec![
                Some(0),
                Some(309),
                Some(309),
                Some(0),
                Some(-309),
                Some(-309),
                Some(i64::MIN),
                Some(i64::MAX),
                Some(4294967296),
                Some(0),
            ]),
        ]),
        &[
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::NAN),
            Some(-0.0),
            Some(0.0),
            Some(0.0),
            Some(1.0),
            Some(0.0),
            Some(1.0),
            None,
        ],
    );
}
#[test]
fn signed_runtime_digits_are_lossless_while_float_digits_use_checked_arrow_cast() {
    assert_floats(
        &dense(vec![
            floats(vec![Some(1.25); 4]),
            signed(vec![
                Some(9007199254740993),
                Some(-9007199254740993),
                Some(i64::MIN),
                Some(4294967296),
            ]),
        ]),
        &[Some(1.3), Some(0.0), Some(1.0), Some(1.0)],
    );
    assert_floats(
        &dense(vec![
            floats(vec![Some(1.25); 5]),
            floats(vec![
                Some(1.9),
                Some(-1.9),
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(9223372036854775808.0),
            ]),
        ]),
        &[Some(1.3), Some(0.0), None, None, None],
    );
}
#[test]
fn decimal_unary_keeps_source_scale_and_exact_raw_values() {
    for (scale, raw) in [
        (2, 155),
        (0, 9007199254740993),
        (-2, 123),
        (38, 123),
        (i8::MIN, 123),
    ] {
        assert_decimals(
            &dense(vec![decimals(vec![Some(raw), Some(-raw), None], scale)]),
            scale,
            &[Some(raw), Some(-raw), None],
        );
    }
}
#[test]
fn decimal_final_scale_adjustment_rounds_real_wrapped_constant_digits_twice() {
    let source: ArrayRef = Arc::new(
        Decimal128Array::from(vec![155, -155])
            .with_precision_and_scale(3, 2)
            .unwrap(),
    );
    let digit = signed(vec![Some(257)]);
    let types = [
        FunctionValueType::from_legacy_declared_projection(source.data_type().clone(), true),
        FunctionValueType::new(DataType::Int64, true),
    ];
    let prepared = super::super::round_owner::prepared_for_test(
        &types,
        &[None, Some(cv::i64(257, true))],
        DecimalOverflowPolicy::OutputNull,
    )
    .unwrap();
    assert_eq!(
        prepared.contract().result_type().data_type().clone(),
        DataType::Decimal128(38, 1)
    );
    let mut call = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let arguments = [
        EvaluatedArgument::Column(&source),
        EvaluatedArgument::Scalar(&digit),
    ];
    let output = call
        .evaluate(Selection::all(2), &arguments, &Control::default())
        .unwrap();
    assert_decimals(output.values(), 1, &[Some(16), Some(-16)]);
    assert!(output.errors().is_empty());
}
#[test]
fn decimal_binary_exact_rescale_and_precision_overflow_keep_selected_error_ordinal() {
    assert_decimals(
        &dense(vec![
            decimals(vec![Some(155), Some(-155), Some(154), None], 2),
            signed(vec![Some(1), Some(1), Some(1), Some(1)]),
        ]),
        2,
        &[Some(160), Some(-160), Some(150), None],
    );
    let max = 10_i128.pow(38) - 1;
    let source = decimals(vec![Some(11), Some(0), None, Some(12), Some(max)], 0);
    let digits = signed(vec![Some(0), Some(0), None, Some(0), Some(-1)]);
    let types = [
        FunctionValueType::from_legacy_declared_projection(source.data_type().clone(), true),
        FunctionValueType::new(DataType::Int64, true),
    ];
    let rows = [1, 4];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let mut call = instance(&types, policy);
        let arguments = [
            EvaluatedArgument::Column(&source),
            EvaluatedArgument::Column(&digits),
        ];
        let output = call
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_decimals(output.values(), 0, &[Some(0), None]);
        if policy == DecimalOverflowPolicy::ReportError {
            assert_eq!(output.errors().len(), 1);
            assert_eq!(output.errors()[0].selected_ordinal(), 1);
            assert_eq!(
                output.errors()[0].message(),
                "decimal overflow in round output"
            );
        } else {
            assert!(output.errors().is_empty());
        }
    }
}
#[test]
fn decimal_digits_native_overflow_uses_policy_only_for_checked_wide_carriers() {
    type Wide = <Decimal256Type as ArrowPrimitiveType>::Native;
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(
            Decimal32Array::from(vec![Some(0), Some(300000000)])
                .with_precision_and_scale(9, -1)
                .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from(vec![Some(0), Some(10000000000000000000)])
                .with_precision_and_scale(38, 0)
                .unwrap(),
        ),
        Arc::new(
            Decimal256Array::from(vec![
                Some(Wide::ZERO),
                Some(Wide::from_i128(10000000000000000000)),
            ])
            .with_precision_and_scale(76, 0)
            .unwrap(),
        ),
    ];
    for (index, digits) in arrays.into_iter().enumerate() {
        let source = floats(vec![Some(1.25); 2]);
        let types = [
            FunctionValueType::new(DataType::Float64, true),
            FunctionValueType::from_legacy_declared_projection(digits.data_type().clone(), true),
        ];
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let mut call = instance(&types, policy);
            let arguments = [
                EvaluatedArgument::Column(&source),
                EvaluatedArgument::Column(&digits),
            ];
            let output = call
                .evaluate(Selection::all(2), &arguments, &Control::default())
                .unwrap();
            assert_floats(output.values(), &[Some(1.0), None]);
            if index > 0 && policy == DecimalOverflowPolicy::ReportError {
                assert_eq!(output.errors().len(), 1);
                assert_eq!(output.errors()[0].selected_ordinal(), 1);
                assert_eq!(
                    output.errors()[0].message(),
                    "decimal overflow in round digits"
                );
            } else {
                assert!(output.errors().is_empty());
            }
        }
    }
}
#[test]
fn static_decimal_factor_and_interval_failures_remain_outer_and_poison_instance() {
    let digits: Vec<ArrayRef> = vec![
        Arc::new(
            Decimal32Array::from(vec![None])
                .with_precision_and_scale(9, -10)
                .unwrap(),
        ),
        Arc::new(
            Decimal64Array::from(vec![None])
                .with_precision_and_scale(18, -19)
                .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from(vec![None])
                .with_precision_and_scale(38, -39)
                .unwrap(),
        ),
        Arc::new(
            Decimal256Array::from(vec![None])
                .with_precision_and_scale(76, -77)
                .unwrap(),
        ),
        Arc::new(IntervalYearMonthArray::from(vec![None])),
        Arc::new(IntervalDayTimeArray::from(vec![None])),
    ];
    let source = floats(vec![None]);
    for digits in digits {
        let types = [
            FunctionValueType::new(DataType::Float64, true),
            FunctionValueType::from_legacy_declared_projection(digits.data_type().clone(), true),
        ];
        let arguments = [
            EvaluatedArgument::Scalar(&source),
            EvaluatedArgument::Scalar(&digits),
        ];
        let mut call = instance(&types, DecimalOverflowPolicy::OutputNull);
        assert!(matches!(
            call.evaluate(Selection::all(1), &arguments, &Control::default()),
            Err(KernelFailure::Operational(_))
        ));
        let retry = Control::default();
        assert_eq!(
            call.evaluate(Selection::all(1), &arguments, &retry)
                .unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert!(retry.calls().is_empty());
        // A fresh empty invocation never enters the private body or static caster.
        let mut empty = instance(&types, DecimalOverflowPolicy::OutputNull);
        let output = empty
            .evaluate(Selection::all(0), &arguments, &Control::default())
            .unwrap();
        assert_floats(output.values(), &[]);
        assert!(output.errors().is_empty());
    }
}

#[test]
fn sparse_column_scalar_and_compact_arguments_preserve_independent_addresses() {
    let rows = [1, 4];
    let selection = Selection::try_sparse(6, &rows).unwrap();
    let source = floats(vec![
        Some(f64::NAN),
        Some(1.25),
        None,
        Some(f64::INFINITY),
        Some(-199.9),
        None,
    ]);
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Int64,
        signed(vec![Some(1), Some(-2)]),
        Box::default(),
    )
    .unwrap();
    let types = [
        FunctionValueType::new(DataType::Float64, true),
        FunctionValueType::new(DataType::Int64, true),
    ];
    let mut call = instance(&types, DecimalOverflowPolicy::OutputNull);
    let arguments = [
        EvaluatedArgument::Column(&source),
        EvaluatedArgument::SelectedColumn(&compact),
    ];
    let output = call
        .evaluate(selection, &arguments, &Control::default())
        .unwrap();
    assert_floats(output.values(), &[Some(1.3), Some(-200.0)]);
    assert_eq!(output.selection().iter().collect::<Vec<_>>(), rows);
    let scalar = floats(vec![Some(123.45)]);
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
        &[Some(123.5), Some(100.0)],
    );
}
fn pool(ty: FunctionValueType, array: ArrayRef) -> ConstantPool {
    ConstantPool::try_new(
        Arc::new(ty.try_to_field("round-input").unwrap()),
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
fn constant_pool_nonzero_ordinals_broadcast_only_the_selected_scalar() {
    let value_type = FunctionValueType::new(DataType::Float64, true);
    let digit_type = FunctionValueType::new(DataType::Int64, true);
    let values = pool(
        value_type.clone(),
        floats(vec![None, Some(12.94), Some(-12.94)]),
    );
    let digits = pool(digit_type.clone(), signed(vec![None, Some(-2), Some(1)]));
    let value = values.value(1).unwrap();
    let digit = digits.value(2).unwrap();
    assert!(Arc::ptr_eq(value.pool().array(), values.array()));
    let mut call = instance(
        &[value_type.clone(), digit_type],
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
    let mut call = instance(&[value_type], DecimalOverflowPolicy::OutputNull);
    assert_integers(
        call.evaluate(
            Selection::all(2),
            &[EvaluatedArgument::Constant(&null)],
            &Control::default(),
        )
        .unwrap()
        .values(),
        &[None; 2],
    );
}
#[test]
fn explicit_dictionary_plain_ingress_preserves_values_and_raw_run_end_is_refused() {
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(2), Some(0), Some(1), None, Some(2)]),
        floats(vec![Some(1.25), None, Some(-2.5)]),
    )
    .unwrap();
    let source: ArrayRef = Arc::new(dictionary.slice(1, 4));
    let plain = arrow_cast::cast(source.as_ref(), &DataType::Float64).unwrap();
    assert_integers(&dense(vec![plain]), &[Some(1), None, None, Some(-3)]);
    let run_ends = Int16Array::from(vec![2, 4, 6]);
    let values = Float64Array::from(vec![Some(1.25), None, Some(-2.5)]);
    let source: ArrayRef = Arc::new(
        RunArray::<Int16Type>::try_new(&run_ends, &values)
            .unwrap()
            .slice(1, 4),
    );
    let types = [FunctionValueType::from_legacy_declared_projection(
        source.data_type().clone(),
        true,
    )];
    let rows = [0, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let mut call = instance(&types, DecimalOverflowPolicy::OutputNull);
    let arguments = [EvaluatedArgument::Column(&source)];
    assert!(
        matches!(
            call.evaluate(selection, &arguments, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ),
        "unsupported raw run-end encoding cannot enter the installed ROUND consumer"
    );
}
#[test]
fn fixed_size_list_parent_null_and_dense_union_selected_tag_survive_slices() {
    use arrow_schema::{Field, UnionFields};
    let mask = Int8Array::from(vec![Some(0), None, Some(0), Some(0)]);
    let list = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float64, true)),
        1,
        floats(vec![Some(99.0), Some(1.25), Some(-2.5), None]),
        mask.nulls().cloned(),
    )
    .unwrap();
    assert_integers(
        &dense(vec![Arc::new(list.slice(1, 3))]),
        &[None, Some(-3), None],
    );
    // Exact Float64 wins over the earlier numeric Int64 field. Other tags
    // produce NULL rather than choosing a different caster for each row.
    let union = UnionArray::try_new(
        UnionFields::try_new(
            [2, 7],
            [
                Field::new("integer", DataType::Int64, true),
                Field::new("floating", DataType::Float64, true),
            ],
        )
        .unwrap(),
        vec![2_i8, 7, 7, 2, 7].into(),
        Some(vec![0_i32, 0, 1, 1, 2].into()),
        vec![
            signed(vec![Some(99), Some(99)]),
            floats(vec![Some(1.25), None, Some(-2.5)]),
        ],
    )
    .unwrap();
    assert_integers(
        &dense(vec![Arc::new(union.slice(1, 4))]),
        &[Some(1), None, None, Some(-3)],
    );
}
#[test]
fn primitive_boolean_unsigned_and_temporal_carriers_match_actual_arrow_cast_values() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![Some(u64::MAX), Some(0), None])),
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
        Arc::new(TimestampSecondArray::from(vec![Some(-2), Some(3), None])),
        Arc::new(DurationMicrosecondArray::from(vec![
            Some(-2),
            Some(3),
            None,
        ])),
    ];
    for source in arrays {
        let cast = arrow_cast::cast(source.as_ref(), &DataType::Float64).unwrap();
        let floats = cast.as_any().downcast_ref::<Float64Array>().unwrap();
        let expected = floats
            .iter()
            .map(|v| v.map(|v| (v + if v < 0.0 { -0.5 } else { 0.5 }) as i64))
            .collect::<Vec<_>>();
        assert_integers(&dense(vec![source]), &expected);
    }
    let source = floats(vec![Some(1.25); 3]);
    let digits: ArrayRef = Arc::new(Date32Array::from(vec![Some(1), Some(0), None]));
    let cast = arrow_cast::cast(digits.as_ref(), &DataType::Int64).unwrap();
    assert_eq!(
        cast.as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(1), Some(0), None]
    );
    assert_floats(&dense(vec![source, digits]), &[Some(1.3), Some(1.0), None]);
}
fn control_fixture(arity: usize) -> (Vec<FunctionValueType>, Vec<ArrayRef>) {
    let mut arrays = vec![floats(vec![Some(1.25)])];
    if arity == 2 {
        arrays.push(signed(vec![Some(1)]));
    }
    let types = arrays
        .iter()
        .map(|a| FunctionValueType::from_legacy_declared_projection(a.data_type().clone(), true))
        .collect();
    (types, arrays)
}
#[test]
fn entry_body_quantum_tail_and_publication_controls_poison_without_replay() {
    for arity in [1, 2] {
        let (types, arrays) = control_fixture(arity);
        let arguments = arrays
            .iter()
            .map(EvaluatedArgument::Scalar)
            .collect::<Vec<_>>();
        let selection = Selection::all(320);
        let mut baseline = instance(&types, DecimalOverflowPolicy::OutputNull);
        let control = Control::default();
        baseline.evaluate(selection, &arguments, &control).unwrap();
        let calls = control.calls();
        let body = calls
            .iter()
            .enumerate()
            .filter(|(_, n)| **n == 0)
            .nth(arity + 1)
            .unwrap()
            .0;
        let quantum = calls.iter().position(|n| *n == 256).unwrap();
        assert!(body < quantum);
        let tail = calls
            .iter()
            .enumerate()
            .rfind(|(at, n)| *at > quantum && **n > 0 && **n < 256)
            .unwrap()
            .0;
        for error in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            for at in [0, body, quantum, tail, calls.len() - 1] {
                let mut call = instance(&types, DecimalOverflowPolicy::OutputNull);
                let control = Control::refusing(at, error.clone());
                assert_eq!(
                    call.evaluate(selection, &arguments, &control).unwrap_err(),
                    error
                );
                assert_eq!(control.calls().len(), at + 1);
                let retry = Control::default();
                assert_eq!(
                    call.evaluate(selection, &arguments, &retry).unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(retry.calls().is_empty());
            }
        }
    }
}
#[test]
fn capacity_refusal_precedes_builder_and_empty_selection_skips_body() {
    capacity(0).unwrap();
    assert_eq!(
        capacity(usize::MAX).unwrap_err(),
        KernelFailure::ResourceExhausted
    );
    for arity in [1, 2] {
        let (types, arrays) = control_fixture(arity);
        let arguments = arrays
            .iter()
            .map(EvaluatedArgument::Scalar)
            .collect::<Vec<_>>();
        let mut call = instance(&types, DecimalOverflowPolicy::OutputNull);
        let control = Control::default();
        assert_eq!(
            call.evaluate(Selection::all(usize::MAX), &arguments, &control)
                .unwrap_err(),
            KernelFailure::ResourceExhausted
        );
        assert!(control.calls().iter().all(|n| *n < 256));
        let mut empty = instance(&types, DecimalOverflowPolicy::OutputNull);
        let control = Control::default();
        let output = empty
            .evaluate(Selection::all(0), &arguments, &control)
            .unwrap();
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
            control.calls().iter().filter(|n| **n == 0).count(),
            arity + 1
        );
    }
}

fn text_carriers(values: &[Option<&str>]) -> Vec<ArrayRef> {
    vec![
        Arc::new(StringArray::from(values.to_vec())),
        Arc::new(LargeStringArray::from(values.to_vec())),
        Arc::new(StringViewArray::from(values.to_vec())),
    ]
}
#[test]
fn actual_text_value_carriers_preserve_arrow_parse_nulls_and_round_saturation() {
    let values = [
        Some("2.5"),
        Some("-2.5"),
        Some("-0"),
        Some("NaN"),
        Some("inf"),
        Some("-INFINITY"),
        Some("bad"),
        Some(" 1"),
        Some("1 "),
        Some("+001"),
        Some("1e-999"),
        None,
    ];
    let expected = [
        Some(3),
        Some(-3),
        Some(0),
        Some(0),
        Some(i64::MAX),
        Some(i64::MIN),
        None,
        None,
        None,
        Some(1),
        Some(0),
        None,
    ];
    for source in text_carriers(&values) {
        // Literal old raw parse expectations check the reached cast author.
        let cast = raw_text_cast(&source, super::super::rounding_binding::CastTarget::Float64);
        let parsed = cast.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(parsed.value(0), 2.5);
        assert_eq!(parsed.value(1), -2.5);
        assert!(parsed.value(3).is_nan());
        assert_eq!(parsed.value(4), f64::INFINITY);
        assert_eq!(parsed.value(5), f64::NEG_INFINITY);
        for row in [6, 7, 8, 11] {
            assert!(parsed.is_null(row), "Arrow source row {row}");
        }
        assert_integers(&dense(vec![source]), &expected);
    }
}
#[test]
fn actual_text_digits_carriers_keep_exact_int64_grammar_and_runtime_magnitude() {
    let texts = [
        Some("1"),
        Some("+001"),
        Some("-1"),
        Some("1.0"),
        Some("1e1"),
        Some(" 1"),
        Some("-9223372036854775808"),
        Some("9223372036854775807"),
        Some("9223372036854775808"),
        None,
    ];
    let parsed = [
        Some(1),
        Some(1),
        Some(-1),
        None,
        None,
        None,
        Some(i64::MIN),
        Some(i64::MAX),
        None,
        None,
    ];
    let expected = [
        Some(1.3),
        Some(1.3),
        Some(0.0),
        None,
        None,
        None,
        Some(1.0),
        Some(0.0),
        None,
        None,
    ];
    for digits in text_carriers(&texts) {
        let cast = raw_text_cast(&digits, super::super::rounding_binding::CastTarget::Int64);
        assert_integers(&cast, &parsed);
        assert_floats(
            &dense(vec![floats(vec![Some(1.25); texts.len()]), digits]),
            &expected,
        );
    }
}
#[test]
fn long_selected_text_is_observed_mid_parse_and_control_failure_never_replays() {
    let long_value = format!("{}2.5", "0".repeat(1536));
    let long_digits = format!("{}1", "0".repeat(1536));
    let unselected = "x".repeat(8192);
    let rows = [2];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    for carrier in 0..3 {
        for arity in [1, 2] {
            let text = if arity == 1 {
                &long_value
            } else {
                &long_digits
            };
            let texts = [Some(unselected.as_str()), None, Some(text.as_str()), None];
            let selected_text = text_carriers(&texts).swap_remove(carrier);
            let arrays = if arity == 1 {
                vec![selected_text]
            } else {
                vec![floats(vec![Some(1.25); 4]), selected_text]
            };
            let types = arrays
                .iter()
                .map(|array| {
                    FunctionValueType::from_legacy_declared_projection(
                        array.data_type().clone(),
                        true,
                    )
                })
                .collect::<Vec<_>>();
            let arguments = arrays
                .iter()
                .map(EvaluatedArgument::Column)
                .collect::<Vec<_>>();
            let mut baseline = instance(&types, DecimalOverflowPolicy::OutputNull);
            let control = Control::default();
            let output = baseline.evaluate(selection, &arguments, &control).unwrap();
            assert_eq!(output.selection().iter().collect::<Vec<_>>(), rows);
            assert!(output.errors().is_empty());
            if arity == 1 {
                assert_integers(output.values(), &[Some(3)]);
            } else {
                assert_floats(output.values(), &[Some(1.3)]);
            }
            let calls = control.calls();
            let body = calls
                .iter()
                .enumerate()
                .filter(|(_, units)| **units == 0)
                .nth(arity + 1)
                .unwrap()
                .0;
            let quantum = calls.iter().position(|units| *units == 256).unwrap();
            assert!(body < quantum, "the long selected token owns the quantum");
            for error in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
            ] {
                for at in [0, quantum] {
                    let mut call = instance(&types, DecimalOverflowPolicy::OutputNull);
                    let control = Control::refusing(at, error.clone());
                    assert_eq!(
                        call.evaluate(selection, &arguments, &control).unwrap_err(),
                        error
                    );
                    assert_eq!(control.calls().len(), at + 1);
                    if at == quantum {
                        assert_eq!(control.calls().last(), Some(&256));
                    }
                    let retry = Control::default();
                    assert_eq!(
                        call.evaluate(selection, &arguments, &retry).unwrap_err(),
                        KernelFailure::InstanceFailed
                    );
                    assert!(retry.calls().is_empty());
                }
            }
        }
    }
}

use super::super::catalogue::constant_binding_tests as cv;
