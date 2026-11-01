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
use crate::{KernelEvaluationControl, builtin::binding_control};
use arrow_schema::{Field, UnionFields};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct Control {
    calls: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
        let mut calls = self.calls.lock().unwrap();
        let at = calls.len();
        calls.push(units);
        if let Some((when, error)) = &self.refusal
            && *when == at
        {
            return Err(error.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("casts must not wait")
    }
}
fn prepared(array: &ArrayRef, target: CastTarget) -> CastRecipe {
    let source =
        FunctionValueType::from_legacy_declared_projection(array.data_type().clone(), true);
    binding_control::scope(crate::binding_test_control(), |work| {
        binding_control::value_type(&source, work)?;
        // Raw Arrow recipe behavior is independent of installed Plain kernels.
        CastRecipe::prepare_typed(array.data_type(), source.value_type(), target, work)
    })
    .unwrap()
}
fn observed(
    recipe: &CastRecipe,
    array: &ArrayRef,
    control: &Control,
) -> Result<Vec<CastValue>, KernelFailure> {
    control.checkpoint(0)?;
    recipe.check_available()?;
    let mut work = EvaluationCheckpoints::new(control);
    let mut output = Vec::new();
    for row in 0..array.len() {
        output.push(recipe.read(array.as_ref(), row, &mut work)?);
    }
    work.finish()?;
    Ok(output)
}
fn arrow_oracle(array: ArrayRef) {
    arrow_oracle_with_frozen_float(array, None)
}
fn frozen_decimal_oracle(array: ArrayRef, float_bits: &[Option<u64>]) {
    arrow_oracle_with_frozen_float(array, Some(float_bits))
}
fn arrow_oracle_with_frozen_float(array: ArrayRef, float_bits: Option<&[Option<u64>]>) {
    for target in [CastTarget::Float64, CastTarget::Int64] {
        let target_type = match target {
            CastTarget::Float64 => DataType::Float64,
            CastTarget::Int64 => DataType::Int64,
        };
        if !arrow_cast::can_cast_types(array.data_type(), &target_type) {
            continue;
        }
        let recipe = prepared(&array, target);
        let text_oracle: Option<ArrayRef> = match array.data_type() {
            DataType::Utf8 => Some(match target {
                CastTarget::Float64 => Arc::new(Float64Array::from(vec![
                    Some(-1.5),
                    Some(9.0),
                    None,
                    Some(f64::INFINITY),
                    Some(f64::INFINITY),
                    Some(-9_223_372_036_854_775_808.0),
                    None,
                ])),
                CastTarget::Int64 => Arc::new(Int64Array::from(vec![
                    None,
                    Some(9),
                    None,
                    None,
                    None,
                    Some(i64::MIN),
                    None,
                ])),
            }),
            DataType::LargeUtf8 => Some(match target {
                CastTarget::Float64 => Arc::new(Float64Array::from(vec![
                    Some(1.5),
                    Some(0.0),
                    Some(9_223_372_036_854_775_808.0),
                    None,
                ])),
                CastTarget::Int64 => Arc::new(Int64Array::from(vec![None, None, None, None])),
            }),
            DataType::Utf8View => Some(match target {
                CastTarget::Float64 => Arc::new(Float64Array::from(vec![
                    Some(-0.0),
                    Some(f64::NAN),
                    None,
                    None,
                ])),
                CastTarget::Int64 => Arc::new(Int64Array::from(vec![Some(0), None, None, None])),
            }),
            _ => None,
        };
        let expected = if let Some(expected) = text_oracle {
            Ok(expected)
        } else if target_type == DataType::Float64 {
            match float_bits {
                Some(bits) => Ok(Arc::new(Float64Array::from_iter(
                    bits.iter().map(|bits| bits.map(f64::from_bits)),
                )) as ArrayRef),
                None => arrow_cast::cast(array.as_ref(), &target_type),
            }
        } else {
            arrow_cast::cast(array.as_ref(), &target_type)
        };
        if expected.is_err() {
            assert!(
                matches!(recipe.check_available(), Err(KernelFailure::Operational(_))),
                "{}",
                array.data_type()
            );
            continue;
        }
        let expected = expected.unwrap();
        let actual = observed(&recipe, &array, &Control::default()).unwrap();
        for (row, value) in actual.into_iter().enumerate() {
            match value {
                CastValue::Null | CastValue::CheckedDecimalOverflow => {
                    assert!(expected.is_null(row), "{} row{row}", array.data_type())
                }
                CastValue::Float(value) => {
                    assert!(!expected.is_null(row));
                    let oracle = expected
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(row);
                    if oracle.is_nan() {
                        assert!(value.is_nan());
                    } else {
                        assert_eq!(
                            value.to_bits(),
                            oracle.to_bits(),
                            "{} row{row}",
                            array.data_type()
                        );
                    }
                }
                CastValue::Integer(value) => {
                    assert!(!expected.is_null(row));
                    assert_eq!(
                        value,
                        expected
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(row),
                        "{} row{row}",
                        array.data_type()
                    );
                }
            }
        }
    }
}
#[test]
fn primitive_borrowed_leaves_match_actual_arrow_numeric_and_text_casts() {
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(NullArray::new(4)),
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
        Arc::new(Int8Array::from(vec![Some(i8::MIN), Some(i8::MAX), None])),
        Arc::new(Int16Array::from(vec![Some(i16::MIN), Some(i16::MAX), None])),
        Arc::new(Int32Array::from(vec![Some(i32::MIN), Some(i32::MAX), None])),
        Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            Some(i64::MAX),
            Some(9_007_199_254_740_993),
            None,
        ])),
        Arc::new(UInt8Array::from(vec![Some(0), Some(u8::MAX), None])),
        Arc::new(UInt16Array::from(vec![Some(0), Some(u16::MAX), None])),
        Arc::new(UInt32Array::from(vec![Some(0), Some(u32::MAX), None])),
        Arc::new(UInt64Array::from(vec![
            Some(0),
            Some(u64::MAX),
            Some(i64::MAX as u64),
            None,
        ])),
        Arc::new(Float32Array::from(vec![
            Some(-1.9),
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
            Some(f32::NAN),
            Some(-0.0),
            None,
        ])),
        Arc::new(Float64Array::from(vec![
            Some(-1.9),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::NAN),
            Some(-0.0),
            Some(i64::MIN as f64),
            Some(i64::MAX as f64),
            None,
        ])),
        Arc::new(StringArray::from(vec![
            Some("-1.5"),
            Some("+009"),
            Some(" NaN"),
            Some("1e309"),
            Some("inf"),
            Some("-9223372036854775808"),
            None,
        ])),
        Arc::new(LargeStringArray::from(vec![
            Some("1.5"),
            Some("1e-325"),
            Some("9223372036854775808"),
            None,
        ])),
        Arc::new(StringViewArray::from(vec![
            Some("-0"),
            Some("NaN"),
            Some("oops"),
            None,
        ])),
    ];
    arrays.push(
        arrow_cast::cast(
            &Float64Array::from(vec![
                Some(1.5),
                Some(-0.0),
                Some(f64::NAN),
                Some(f64::INFINITY),
                None,
            ]),
            &DataType::Float16,
        )
        .unwrap(),
    );
    for array in arrays {
        arrow_oracle(array);
    }
}
#[test]
fn all_decimal_native_widths_and_signs_keep_frozen_cast_before_policy_projection() {
    // Literal IEEE bits derive from the original conversion, independently of the SDK.
    for (scale, d32, d64, d128, d256) in [
        (
            -2,
            [Some(0xc2374876e79c0000), Some(0x40ce460000000000), None],
            [Some(0xc415af1d78b58c40), Some(0x40ce460000000000), None],
            [Some(0x483d6329f1c35ca4), Some(0xc0ce460000000000), None],
            [Some(0x502145b7e285bf99), Some(0xc0ce460000000000), None],
        ),
        (
            0,
            [Some(0xc1cdcd64ff800000), Some(0x4063600000000000), None],
            [Some(0xc3abc16d674ec800), Some(0x4063600000000000), None],
            [Some(0x47d2ced32a16a1b1), Some(0xc063600000000000), None],
            [Some(0x4fb61bcca7119916), Some(0xc063600000000000), None],
        ),
        (
            2,
            [Some(0xc16312cfffae147b), Some(0x3ff8cccccccccccd), None],
            [Some(0xc341c37937e08000), Some(0x3ff8cccccccccccd), None],
            [Some(0x476812f9cf7920e3), Some(0xbff8cccccccccccd), None],
            [Some(0x4f4c4c8b1349b9b6), Some(0xbff8cccccccccccd), None],
        ),
    ] {
        frozen_decimal_oracle(
            Arc::new(
                Decimal32Array::from(vec![Some(-999_999_999), Some(155), None])
                    .with_precision_and_scale(9, scale)
                    .unwrap(),
            ),
            &d32,
        );
        frozen_decimal_oracle(
            Arc::new(
                Decimal64Array::from(vec![Some(-999_999_999_999_999_999), Some(155), None])
                    .with_precision_and_scale(18, scale)
                    .unwrap(),
            ),
            &d64,
        );
        frozen_decimal_oracle(
            Arc::new(
                Decimal128Array::from(vec![Some(10_i128.pow(38) - 1), Some(-155), None])
                    .with_precision_and_scale(38, scale)
                    .unwrap(),
            ),
            &d128,
        );
        frozen_decimal_oracle(
            Arc::new(
                Decimal256Array::from(vec![
                    Some(
                        Wide::from_i128(10)
                            .checked_pow(76)
                            .unwrap()
                            .checked_sub(Wide::from_i128(1))
                            .unwrap(),
                    ),
                    Some(Wide::from_i128(-155)),
                    None,
                ])
                .with_precision_and_scale(76, scale)
                .unwrap(),
            ),
            &d256,
        );
    }
    frozen_decimal_oracle(
        Arc::new(
            Decimal32Array::from(vec![None, Some(0)])
                .with_precision_and_scale(9, -10)
                .unwrap(),
        ),
        &[None, Some(0)],
    );
    frozen_decimal_oracle(
        Arc::new(
            Decimal64Array::from(vec![None, Some(0)])
                .with_precision_and_scale(18, -19)
                .unwrap(),
        ),
        &[None, Some(0)],
    );
    frozen_decimal_oracle(
        Arc::new(
            Decimal128Array::from(vec![None, Some(0)])
                .with_precision_and_scale(38, -39)
                .unwrap(),
        ),
        &[None, Some(0)],
    );
    frozen_decimal_oracle(
        Arc::new(
            Decimal256Array::from(vec![None, Some(Wide::from_i128(0))])
                .with_precision_and_scale(76, -77)
                .unwrap(),
        ),
        &[None, Some(0)],
    );
}
#[test]
fn temporal_storage_is_reinterpreted_without_unit_or_timezone_conversion() {
    let values = vec![Some(-15), Some(99), None];
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(TimestampSecondArray::from(values.clone()).with_timezone("+05:30")),
        Arc::new(TimestampMillisecondArray::from(values.clone())),
        Arc::new(TimestampMicrosecondArray::from(values.clone())),
        Arc::new(TimestampNanosecondArray::from(values.clone())),
        Arc::new(DurationSecondArray::from(values.clone())),
        Arc::new(DurationMillisecondArray::from(values.clone())),
        Arc::new(DurationMicrosecondArray::from(values.clone())),
        Arc::new(DurationNanosecondArray::from(values.clone())),
        Arc::new(Date32Array::from(vec![Some(-15), Some(99), None])),
        Arc::new(Date64Array::from(values.clone())),
        Arc::new(Time32SecondArray::from(vec![Some(-15), Some(99), None])),
        Arc::new(Time32MillisecondArray::from(vec![
            Some(-15),
            Some(99),
            None,
        ])),
        Arc::new(Time64MicrosecondArray::from(values.clone())),
        Arc::new(Time64NanosecondArray::from(values)),
        Arc::new(IntervalYearMonthArray::from(vec![
            Some(-15),
            Some(99),
            None,
        ])),
        Arc::new(IntervalDayTimeArray::from(vec![
            Some(IntervalDayTimeType::make_value(1, -15)),
            None,
        ])),
    ];
    for array in arrays {
        arrow_oracle(array);
    }
}
#[test]
fn every_dictionary_key_width_keeps_null_keys_and_child_rows_without_unpacking() {
    let values: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.9), None, Some(-2.9)]));
    macro_rules! dictionary {
        ($t:ty) => {{
            let keys = PrimitiveArray::<$t>::from(vec![Some(2), None, Some(1), Some(0)]);
            let array: ArrayRef =
                Arc::new(DictionaryArray::<$t>::try_new(keys, values.clone()).unwrap());
            arrow_oracle(array.slice(1, 3));
        }};
    }
    dictionary!(Int8Type);
    dictionary!(Int16Type);
    dictionary!(Int32Type);
    dictionary!(Int64Type);
    dictionary!(UInt8Type);
    dictionary!(UInt16Type);
    dictionary!(UInt32Type);
    dictionary!(UInt64Type);
}
#[test]
fn sliced_run_arrays_keep_logical_offsets_and_native_end_widths() {
    let values: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.9), None, Some(-2.9)]));
    macro_rules! run {
        ($t:ty) => {{
            let ends = PrimitiveArray::<$t>::from(vec![2, 5, 7]);
            let a: RunArray<$t> = RunArray::try_new(&ends, values.as_ref()).unwrap();
            let array: ArrayRef = Arc::new(a);
            arrow_oracle(array.slice(1, 5));
        }};
    }
    run!(Int16Type);
    run!(Int32Type);
    run!(Int64Type);
}
#[test]
fn singleton_list_slices_respect_accepted_parent_and_child_sql_nulls() {
    let values: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(99.0),
        Some(1.9),
        Some(7.0),
        None,
        Some(-2.9),
    ]));
    let field = Arc::new(Field::new("item", DataType::Float64, true));
    let plain: ArrayRef = Arc::new(FixedSizeListArray::new(
        field.clone(),
        1,
        values.clone(),
        None,
    ));
    arrow_oracle(plain.slice(1, 4));
    let masked: ArrayRef = Arc::new(FixedSizeListArray::new(
        field,
        1,
        values,
        Some(vec![true, true, false, true, true].into()),
    ));
    let masked = masked.slice(1, 4);
    let recipe = prepared(&masked, CastTarget::Float64);
    let actual = observed(&recipe, &masked, &Control::default()).unwrap();
    assert!(matches!(actual[0],CastValue::Float(v) if v==1.9));
    assert!(matches!(actual[1], CastValue::Null)); // non-null child under NULL parent
    assert!(matches!(actual[2], CastValue::Null)); // NULL child under valid parent
    assert!(matches!(actual[3],CastValue::Float(v) if v== -2.9));
}
#[test]
fn dense_and_sparse_union_slices_keep_selected_tag_and_child_offsets() {
    let fields = UnionFields::try_new(
        [7, 1],
        [
            Field::new("text", DataType::Utf8, true),
            Field::new("exact", DataType::Float64, true),
        ],
    )
    .unwrap();
    let sparse: ArrayRef = Arc::new(
        UnionArray::try_new(
            fields.clone(),
            vec![7, 1, 7, 1].into(),
            None,
            vec![
                Arc::new(StringArray::from(vec![Some("1"), None, Some("3"), None])),
                Arc::new(Float64Array::from(vec![None, Some(2.5), None, None])),
            ],
        )
        .unwrap(),
    );
    let dense: ArrayRef = Arc::new(
        UnionArray::try_new(
            fields,
            vec![7, 1, 7, 1].into(),
            Some(vec![0, 1, 1, 0].into()),
            vec![
                Arc::new(StringArray::from(vec!["1", "3"])),
                Arc::new(Float64Array::from(vec![Some(2.5), None])),
            ],
        )
        .unwrap(),
    );
    arrow_oracle(sparse.slice(1, 3));
    arrow_oracle(dense.slice(1, 3));
}
#[test]
fn original_control_observes_each_binary_search_comparison_and_leaf_work() {
    let values: ArrayRef = Arc::new(Int64Array::from_iter_values(0..1024));
    let ends = Int32Array::from_iter_values(1..=1024);
    let a: RunArray<Int32Type> = RunArray::try_new(&ends, values.as_ref()).unwrap();
    let array: ArrayRef = Arc::new(a);
    let recipe = prepared(&array, CastTarget::Int64);
    let baseline = Control::default();
    let result = observed(&recipe, &array, &baseline).unwrap();
    assert_eq!(result.len(), 1024);
    let calls = baseline.calls.lock().unwrap().clone();
    assert_eq!(calls[0], 0);
    assert!(calls.contains(&256));
    assert!(calls.len() > 40);
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        for at in [0, 1, calls.len() - 1] {
            let control = Control {
                calls: Mutex::new(Vec::new()),
                refusal: Some((at, failure.clone())),
            };
            assert!(matches!(observed(&recipe,&array,&control),Err(error) if error==failure));
            assert_eq!(control.calls.lock().unwrap().len(), at + 1);
        }
    }
}

#[test]
fn decimal256_round_argument_retains_original_midpoint_and_null_bits() {
    let array: ArrayRef = Arc::new(
        Decimal256Array::from(vec![
            Some(Wide::from_i128(9_223_372_036_854_776_833)),
            None,
            Some(Wide::from_i128(-9_223_372_036_854_776_833)),
        ])
        .with_precision_and_scale(19, 0)
        .unwrap(),
    );
    let actual = observed(
        &prepared(&array, CastTarget::Float64),
        &array,
        &Control::default(),
    )
    .unwrap();
    for (value, expected) in actual.into_iter().zip([
        Some(0x43e0_0000_0000_0000),
        None,
        Some(0xc3e0_0000_0000_0001),
    ]) {
        match (value, expected) {
            (CastValue::Float(value), Some(bits)) => assert_eq!(value.to_bits(), bits),
            (CastValue::Null, None) => {}
            _ => panic!("original decimal ROUND argument projection"),
        }
    }
}

#[test]
fn interval_digits_keep_prepared_deferred_nonempty_refusal() {
    for source in [
        Arc::new(IntervalYearMonthArray::from(vec![Some(1), None])) as ArrayRef,
        Arc::new(IntervalDayTimeArray::from(vec![
            Some(IntervalDayTimeType::make_value(1, 0)),
            None,
        ])),
    ] {
        let recipe = prepared(&source, CastTarget::Int64);
        let error = recipe.check_available().unwrap_err();
        assert!(matches!(error, KernelFailure::Operational(_)));
        assert_eq!(
            error.to_string(),
            "kernel operational failure: round cannot cast interval digits to Int64"
        );
    }
}
