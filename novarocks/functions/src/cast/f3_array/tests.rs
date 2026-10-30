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

//! Exact values and schema properties for the selected array CAST port.
use super::*;
use arrow_array::{
    BinaryArray, FixedSizeBinaryArray, Int32Array, LargeBinaryArray, LargeStringArray,
    Time64MicrosecondArray,
};
use arrow_schema::Field;
use std::time::Duration;
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("selected CAST does not wait")
    }
}
fn recipe(
    source: DataType,
    target: DataType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::new(source, true),
        &FunctionValueType::new(target, true),
        policy,
        allow,
        &Control,
    )
    .unwrap()
}
fn output<'a>(
    r: &PreparedCastRecipe,
    input: &ArrayRef,
    selection: Selection<'a>,
) -> SelectedValues<'a> {
    assert!(r.uses_selected_array());
    r.evaluate_selected_array(EvaluatedArgument::Column(input), selection, &[], &Control)
        .unwrap()
}
#[test]
fn fixed_bytes_keep_exact_width_and_selected_addresses() {
    let input = Arc::new(BinaryArray::from(vec![
        Some(&b"abcd"[..]),
        Some(&b"xy"[..]),
        Some(&b"wxyz"[..]),
        None,
    ])) as ArrayRef;
    let rows = [0, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let r = recipe(
        DataType::Binary,
        DataType::FixedSizeBinary(4),
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let out = output(&r, &input, selection);
    assert_eq!(out.values().data_type(), &DataType::FixedSizeBinary(4));
    let fixed = out
        .values()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(fixed.value(0), b"abcd");
    assert_eq!(fixed.value(1), b"wxyz");
    assert!(fixed.is_null(2));
    let selected = output(&r, &input, Selection::all(4));
    assert!(selected.values().is_null(1));
    assert!(selected.errors().is_empty());
    let r = recipe(
        DataType::FixedSizeBinary(4),
        DataType::Binary,
        DecimalOverflowPolicy::ReportError,
        true,
    );
    let restored = output(&r, out.values(), Selection::all(3));
    let bytes = restored
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(bytes.value(0), b"abcd");
    assert_eq!(bytes.value(1), b"wxyz");
    assert!(bytes.is_null(2));
}
#[test]
fn wide_decimal_targets_keep_value_precision_and_policy() {
    for (policy, allow) in [
        (DecimalOverflowPolicy::OutputNull, false),
        (DecimalOverflowPolicy::OutputNull, true),
        (DecimalOverflowPolicy::ReportError, false),
        (DecimalOverflowPolicy::ReportError, true),
    ] {
        let input = Arc::new(Int64Array::from(vec![Some(12), Some(-12), None])) as ArrayRef;
        let r = recipe(DataType::Int64, DataType::Decimal256(40, 2), policy, allow);
        let out = output(&r, &input, Selection::all(3));
        assert_eq!(out.values().data_type(), &DataType::Decimal256(40, 2));
        let values = out
            .values()
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap();
        assert_eq!(values.value(0), arrow_buffer::i256::from_i128(1200));
        assert_eq!(values.value(1), arrow_buffer::i256::from_i128(-1200));
        assert!(values.is_null(2));
        let bools = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef;
        let r = recipe(
            DataType::Boolean,
            DataType::Decimal256(40, 2),
            policy,
            allow,
        );
        let out = output(&r, &bools, Selection::all(3));
        assert_eq!(out.values().data_type(), &DataType::Decimal256(40, 2));
        let values = out
            .values()
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap();
        assert_eq!(values.value(0), arrow_buffer::i256::from_i128(100));
        assert_eq!(values.value(1), arrow_buffer::i256::ZERO);
        assert!(values.is_null(2));
        let input = Arc::new(Int64Array::from(vec![1000_i64])) as ArrayRef;
        let r = recipe(DataType::Int64, DataType::Decimal256(3, 0), policy, allow);
        let out = output(&r, &input, Selection::all(1));
        assert!(out.values().is_null(0));
        assert_eq!(
            out.errors().len(),
            usize::from(policy == DecimalOverflowPolicy::ReportError)
        );
    }
}
#[test]
fn struct_names_use_complete_by_name_mapping_or_complete_ordinal_mapping() {
    let from = vec![
        Arc::new(Field::new("a", DataType::Int32, false)),
        Arc::new(Field::new("b", DataType::Int32, false)),
    ]
    .into();
    let input = Arc::new(StructArray::new(
        from,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![10, 20])),
        ],
        None,
    )) as ArrayRef;
    for (names, expected) in [(["b", "a"], [10, 1]), (["new_a", "b"], [1, 10])] {
        let target = DataType::Struct(
            vec![
                Arc::new(Field::new(names[0], DataType::Int32, true)),
                Arc::new(Field::new(names[1], DataType::Int32, true)),
            ]
            .into(),
        );
        let r = recipe(
            input.data_type().clone(),
            target.clone(),
            DecimalOverflowPolicy::OutputNull,
            false,
        );
        let out = output(&r, &input, Selection::all(2));
        assert_eq!(out.values().data_type(), &target);
        let values = out.values().as_any().downcast_ref::<StructArray>().unwrap();
        for (i, expected_value) in expected.iter().enumerate() {
            assert_eq!(
                values
                    .column(i)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0),
                *expected_value
            );
        }
    }
}
#[test]
fn renamed_struct_keeps_nested_fixed_bytes_without_generic_value_casting() {
    let fixed =
        Arc::new(FixedSizeBinaryArray::try_from_iter([b"abcd", b"wxyz"].into_iter()).unwrap())
            as ArrayRef;
    let input = Arc::new(StructArray::new(
        vec![Arc::new(Field::new(
            "old",
            DataType::FixedSizeBinary(4),
            false,
        ))]
        .into(),
        vec![fixed],
        None,
    )) as ArrayRef;
    let target = DataType::Struct(
        vec![Arc::new(Field::new(
            "new",
            DataType::FixedSizeBinary(4),
            true,
        ))]
        .into(),
    );
    let r = recipe(
        input.data_type().clone(),
        target.clone(),
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let rows = [1];
    let out = output(&r, &input, Selection::try_sparse(2, &rows).unwrap());
    assert_eq!(out.values().data_type(), &target);
    let values = out.values().as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(
        values
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
        b"wxyz"
    );
}
#[test]
fn selected_array_route_preserves_inherited_journal() {
    let selection = Selection::all(2);
    let input = SelectedValues::try_new(
        selection,
        &DataType::Binary,
        Arc::new(BinaryArray::from(vec![Some(&b"abcd"[..]), None])),
        vec![RowDataError::new(1, "original child error")].into_boxed_slice(),
    )
    .unwrap();
    let r = recipe(
        DataType::Binary,
        DataType::FixedSizeBinary(4),
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let out = r
        .evaluate_selected_array(
            EvaluatedArgument::SelectedColumn(&input),
            selection,
            input.errors(),
            &Control,
        )
        .unwrap();
    assert_eq!(out.errors(), input.errors());
    assert!(out.values().is_null(1));
}

#[test]
fn physical_fixed16_numeric_profile_keeps_big_endian_values_without_nominal_rebinding() {
    let input = Arc::new(Int64Array::from(vec![Some(12_i64), Some(-12_i64), None])) as ArrayRef;
    let r = recipe(
        DataType::Int64,
        DataType::FixedSizeBinary(16),
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let out = output(&r, &input, Selection::all(3));
    let fixed = out
        .values()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(fixed.value(0), 12_i128.to_be_bytes());
    assert_eq!(fixed.value(1), (-12_i128).to_be_bytes());
    assert!(fixed.is_null(2));
    let r = recipe(
        DataType::FixedSizeBinary(16),
        DataType::Int64,
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let restored = output(&r, out.values(), Selection::all(3));
    assert_eq!(
        restored
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        input.as_any().downcast_ref::<Int64Array>().unwrap()
    );
    let text = Arc::new(StringArray::from(vec![
        Some(" 170141183460469231731687303715884105727 "),
        Some("bad"),
        None,
    ])) as ArrayRef;
    let r = recipe(
        DataType::Utf8,
        DataType::FixedSizeBinary(16),
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let out = output(&r, &text, Selection::all(3));
    let fixed = out
        .values()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(fixed.value(0), i128::MAX.to_be_bytes());
    assert!(fixed.is_null(1));
    assert!(fixed.is_null(2));
}

#[test]
fn selected_scalar_array_families_keep_independent_values_under_every_policy() {
    let cases: Vec<(ArrayRef, ArrayRef)> = vec![
        (
            Arc::new(UInt64Array::from(vec![Some(12), None])),
            Arc::new(
                Decimal128Array::from(vec![Some(1200), None])
                    .with_precision_and_scale(6, 2)
                    .unwrap(),
            ),
        ),
        (
            Arc::new(
                Decimal128Array::from(vec![Some(1299), None])
                    .with_precision_and_scale(6, 2)
                    .unwrap(),
            ),
            Arc::new(UInt16Array::from(vec![Some(12), None])),
        ),
        (
            Arc::new(TimestampMicrosecondArray::from(vec![Some(123456), None])),
            Arc::new(Int64Array::from(vec![Some(123456), None])),
        ),
        (
            Arc::new(Time64MicrosecondArray::from(vec![Some(123456), None])),
            Arc::new(Int64Array::from(vec![Some(123456), None])),
        ),
        (
            Arc::new(Int64Array::from(vec![Some(123456), None])),
            Arc::new(Time64MicrosecondArray::from(vec![Some(123456), None])),
        ),
        (
            Arc::new(LargeStringArray::from(vec![Some("12"), None])),
            Arc::new(Int32Array::from(vec![Some(12), None])),
        ),
        // The i64 offset carrier keeps Arrow's own parser grammar. In
        // particular, LargeUtf8 Boolean "2" is NULL and decimal blank is 0;
        // neither is rewritten through the distinct Utf8 special rules.
        (
            Arc::new(LargeStringArray::from(vec![
                Some("2147483647"),
                Some("2147483648"),
                Some("bad"),
                None,
            ])),
            Arc::new(Int32Array::from(vec![Some(i32::MAX), None, None, None])),
        ),
        (
            Arc::new(LargeStringArray::from(vec![
                Some("1.25"),
                Some("-0"),
                Some("NaN"),
                Some("inf"),
                Some("-inf"),
                Some("1e1000"),
                Some("bad"),
                None,
            ])),
            // 1.25 is binary-exact. The accepted ordinary-CAST text rule
            // masks every parsed non-finite value; finite negative zero stays.
            Arc::new(arrow_array::Float32Array::from(vec![
                Some(1.25),
                Some(-0.0),
                None,
                None,
                None,
                None,
                None,
                None,
            ])),
        ),
        (
            Arc::new(LargeStringArray::from(vec![
                Some("1.25"),
                Some("-0"),
                Some("NaN"),
                Some("inf"),
                Some("-inf"),
                Some("1e1000"),
                Some("bad"),
                None,
            ])),
            Arc::new(arrow_array::Float64Array::from(vec![
                Some(1.25),
                Some(-0.0),
                None,
                None,
                None,
                None,
                None,
                None,
            ])),
        ),
        (
            Arc::new(LargeStringArray::from(vec![
                Some("1.25"),
                Some("-1.25"),
                Some(""),
                Some("bad"),
                Some("999.95"),
                None,
            ])),
            // Half-away rounding gives +/-13. Rounding 999.95 to one
            // decimal needs coefficient 10000, outside precision 4.
            Arc::new(
                Decimal128Array::from(vec![Some(13), Some(-13), Some(0), None, None, None])
                    .with_precision_and_scale(4, 1)
                    .unwrap(),
            ),
        ),
        (
            Arc::new(LargeStringArray::from(vec![
                Some(" true "),
                Some("off"),
                Some("1"),
                Some("2"),
                Some(""),
                None,
            ])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                None,
                None,
                None,
            ])),
        ),
        (
            Arc::new(LargeStringArray::from(vec![
                Some("1970-01-02"),
                Some("2024-02-29"),
                Some("2023-02-29"),
                Some("bad"),
                None,
            ])),
            // Epoch-day arithmetic: 1970-01-02 is 1; 2024 leap-day is 19782.
            Arc::new(arrow_array::Date32Array::from(vec![
                Some(1),
                Some(19_782),
                None,
                None,
                None,
            ])),
        ),
        (
            Arc::new(StringArray::from(vec![Some("alpha"), None])),
            Arc::new(LargeStringArray::from(vec![Some("alpha"), None])),
        ),
        (
            Arc::new(LargeBinaryArray::from(vec![Some(b"a\0b".as_slice()), None])),
            Arc::new(BinaryArray::from(vec![Some(b"a\0b".as_slice()), None])),
        ),
        (
            Arc::new(Int32Array::from(vec![Some(12), None])),
            Arc::new(BinaryArray::from(vec![
                Some(12_i32.to_ne_bytes().as_slice()),
                None,
            ])),
        ),
        (
            Arc::new(StringArray::from(vec![Some("12.25"), None])),
            Arc::new(
                Decimal256Array::from(vec![Some(arrow_buffer::i256::from_i128(1225)), None])
                    .with_precision_and_scale(40, 2)
                    .unwrap(),
            ),
        ),
        (
            Arc::new(
                Decimal128Array::from(vec![Some(1225), None])
                    .with_precision_and_scale(6, 2)
                    .unwrap(),
            ),
            Arc::new(
                Decimal256Array::from(vec![Some(arrow_buffer::i256::from_i128(1225)), None])
                    .with_precision_and_scale(40, 2)
                    .unwrap(),
            ),
        ),
    ];
    for (source, expected) in cases {
        for (policy, allow) in [
            (DecimalOverflowPolicy::OutputNull, false),
            (DecimalOverflowPolicy::OutputNull, true),
            (DecimalOverflowPolicy::ReportError, false),
            (DecimalOverflowPolicy::ReportError, true),
        ] {
            let recipe = recipe(
                source.data_type().clone(),
                expected.data_type().clone(),
                policy,
                allow,
            );
            let actual = output(&recipe, &source, Selection::all(source.len()));
            assert_eq!(
                actual.values().to_data(),
                expected.to_data(),
                "{:?} -> {:?} {policy:?} allow={allow}",
                source.data_type(),
                expected.data_type()
            );
            assert!(actual.errors().is_empty());
        }
    }
}

#[test]
fn fixed_numeric_precision_masks_retain_exact_decimal128_and_decimal256_targets() {
    let input = crate::largeint::array_from_i128(&[Some(12), Some(1000), None]).unwrap();
    for target in [DataType::Decimal128(3, 1), DataType::Decimal256(3, 1)] {
        for (policy, allow) in [
            (DecimalOverflowPolicy::OutputNull, false),
            (DecimalOverflowPolicy::OutputNull, true),
            (DecimalOverflowPolicy::ReportError, false),
            (DecimalOverflowPolicy::ReportError, true),
        ] {
            let r = recipe(DataType::FixedSizeBinary(16), target.clone(), policy, allow);
            let out = output(&r, &input, Selection::all(3));
            assert_eq!(out.values().data_type(), &target);
            match target {
                DataType::Decimal128(..) => {
                    let values = out
                        .values()
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .unwrap();
                    assert_eq!(values.value(0), 120);
                }
                DataType::Decimal256(..) => {
                    let values = out
                        .values()
                        .as_any()
                        .downcast_ref::<Decimal256Array>()
                        .unwrap();
                    assert_eq!(values.value(0), arrow_buffer::i256::from_i128(120));
                }
                _ => unreachable!(),
            }
            assert!(out.values().is_null(1));
            assert!(out.values().is_null(2));
            assert_eq!(
                out.errors().len(),
                usize::from(policy == DecimalOverflowPolicy::ReportError)
            );
            if policy == DecimalOverflowPolicy::ReportError {
                assert_eq!(out.errors()[0].selected_ordinal(), 1);
            }
        }
    }
}

#[test]
fn selected_cast_arrow_failure_journal_preserves_only_actual_data_and_its_source() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;
    for (source, expected) in [
        (
            arrow_schema::ArrowError::CastError("invalid integer".into()),
            crate::DataFailureReason::InvalidValue,
        ),
        (
            arrow_schema::ArrowError::ArithmeticOverflow("coefficient".into()),
            crate::DataFailureReason::NumericOutOfRange,
        ),
    ] {
        let row = ArrayCastFailure::Arrow(source).into_row(7).unwrap();
        assert_eq!(row.selected_ordinal(), 7);
        assert_eq!(row.projected_fact(), expected.failure_fact());
        let mut cause = row.source();
        let mut found = false;
        while let Some(source) = cause {
            found |= source.is::<arrow_schema::ArrowError>();
            cause = source.source();
        }
        assert!(found, "journal retains the actual Arrow source");
    }
    for (source, expected) in [
        (
            arrow_schema::ArrowError::MemoryError("allocation".into()),
            FailureClass::Resource,
        ),
        (
            arrow_schema::ArrowError::OffsetOverflowError(usize::MAX),
            FailureClass::Representation,
        ),
        (
            arrow_schema::ArrowError::SchemaError("carrier".into()),
            FailureClass::Invariant,
        ),
    ] {
        let outer = ArrayCastFailure::Arrow(source).into_row(7).unwrap_err();
        assert_eq!(outer.failure_fact().class(), expected);
        let mut cause = outer.source();
        let mut found = false;
        while let Some(source) = cause {
            found |= source.is::<arrow_schema::ArrowError>();
            cause = source.source();
        }
        assert!(found, "outer failure retains the actual Arrow source");
    }
    let original = KernelFailure::Cancelled;
    assert_eq!(
        ArrayCastFailure::Outer(original.clone())
            .into_row(7)
            .unwrap_err(),
        original
    );
}

fn p05_modes() -> [(DecimalOverflowPolicy, bool); 4] {
    [
        (DecimalOverflowPolicy::OutputNull, false),
        (DecimalOverflowPolicy::OutputNull, true),
        (DecimalOverflowPolicy::ReportError, false),
        (DecimalOverflowPolicy::ReportError, true),
    ]
}
fn p05_selected_value_case(source: ArrayRef, expected: ArrayRef) {
    for (policy, allow) in p05_modes() {
        let r = recipe(
            source.data_type().clone(),
            expected.data_type().clone(),
            policy,
            allow,
        );
        let result = output(&r, &source, Selection::all(source.len()));
        assert_eq!(
            result.values().to_data(),
            expected.to_data(),
            "{:?} -> {:?}",
            source.data_type(),
            expected.data_type()
        );
        assert!(result.errors().is_empty());
    }
}

#[test]
fn p05_numeric_source_widths_and_nonfinite_to_wide_decimal() {
    use arrow_array::*;
    p05_boolean_wide_decimal_precision_boundary();
    // Decimal coefficient 1200 at scale 2 denotes exactly integer 12.
    let expected = Arc::new(
        Decimal256Array::from(vec![Some(arrow_buffer::i256::from_i128(1200)), None])
            .with_precision_and_scale(40, 2)
            .unwrap(),
    ) as ArrayRef;
    let sources: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(vec![Some(12), None])),
        Arc::new(Int16Array::from(vec![Some(12), None])),
        Arc::new(Int32Array::from(vec![Some(12), None])),
        Arc::new(Int64Array::from(vec![Some(12), None])),
        Arc::new(UInt8Array::from(vec![Some(12), None])),
        Arc::new(UInt16Array::from(vec![Some(12), None])),
        Arc::new(UInt32Array::from(vec![Some(12), None])),
        Arc::new(UInt64Array::from(vec![Some(12), None])),
        Arc::new(Float32Array::from(vec![Some(12.0), None])),
        Arc::new(Float64Array::from(vec![Some(12.0), None])),
    ];
    for source in sources {
        if matches!(
            source.data_type(),
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        ) {
            let narrow = Arc::new(
                Decimal128Array::from(vec![Some(1200), None])
                    .with_precision_and_scale(20, 2)
                    .unwrap(),
            ) as ArrayRef;
            p05_selected_value_case(source.clone(), narrow);
        }
        p05_selected_value_case(source, expected.clone());
    }
    p05_selected_value_case(
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
        Arc::new(
            Decimal256Array::from(vec![
                Some(arrow_buffer::i256::ZERO),
                Some(arrow_buffer::i256::ZERO),
                None,
            ])
            .with_precision_and_scale(40, -2)
            .unwrap(),
        ),
    );
    for source in [
        Arc::new(Float32Array::from(vec![
            Some(12.0),
            Some(1000.0),
            Some(f32::NAN),
            Some(f32::INFINITY),
            None,
        ])) as ArrayRef,
        Arc::new(Float64Array::from(vec![
            Some(12.0),
            Some(1000.0),
            Some(f64::NAN),
            Some(f64::INFINITY),
            None,
        ])) as ArrayRef,
    ] {
        for (policy, allow) in p05_modes() {
            let r = recipe(
                source.data_type().clone(),
                DataType::Decimal256(3, 0),
                policy,
                allow,
            );
            let result = output(&r, &source, Selection::all(5));
            let values = result
                .values()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap();
            assert_eq!(values.value(0), arrow_buffer::i256::from_i128(12));
            for row in 1..5 {
                assert!(values.is_null(row));
            }
            assert_eq!(
                result.errors().len(),
                usize::from(policy == DecimalOverflowPolicy::ReportError)
            );
            if policy == DecimalOverflowPolicy::ReportError {
                assert_eq!(result.errors()[0].selected_ordinal(), 1);
            }
        }
    }
}

#[test]
fn p05_decimal_unsigned_width_limits_and_wide_rescale_policy() {
    use arrow_array::*;
    p05_wide_decimal_minimum_scale_zero_is_exact();
    p05_selected_wide_decimal_delta_proofs();
    p05_decimal_safe_table_miss_retains_static_typed_refusal();
    // Integer endpoints are fixed by their widths, independent of Arrow's
    // decimal implementation. Negative-to-unsigned and max+1 cannot fit.
    macro_rules! width {
        ($array:ty, $target:expr, $max:expr) => {{
            let max = $max as i128;
            for source in [
                Arc::new(
                    Decimal128Array::from(vec![Some(0), Some(max), Some(-1), Some(max + 1), None])
                        .with_precision_and_scale(38, 0)
                        .unwrap(),
                ) as ArrayRef,
                Arc::new(
                    Decimal256Array::from(vec![
                        Some(arrow_buffer::i256::ZERO),
                        Some(arrow_buffer::i256::from_i128(max)),
                        Some(arrow_buffer::i256::from_i128(-1)),
                        Some(arrow_buffer::i256::from_i128(max + 1)),
                        None,
                    ])
                    .with_precision_and_scale(76, 0)
                    .unwrap(),
                ) as ArrayRef,
            ] {
                for (policy, allow) in p05_modes() {
                    let target = $target;
                    let r = recipe(source.data_type().clone(), target, policy, allow);
                    let result = output(&r, &source, Selection::all(5));
                    let values = result.values().as_any().downcast_ref::<$array>().unwrap();
                    assert_eq!(values.value(0) as u128, 0);
                    assert_eq!(values.value(1) as u128, max as u128);
                    for row in 2..5 {
                        assert!(values.is_null(row));
                    }
                    assert_eq!(
                        result.errors().len(),
                        if policy == DecimalOverflowPolicy::ReportError {
                            2
                        } else {
                            0
                        }
                    );
                    if policy == DecimalOverflowPolicy::ReportError {
                        assert_eq!(
                            result
                                .errors()
                                .iter()
                                .map(|e| e.selected_ordinal())
                                .collect::<Vec<_>>(),
                            vec![2, 3]
                        );
                    }
                }
            }
        }};
    }
    width!(UInt8Array, DataType::UInt8, u8::MAX);
    width!(UInt16Array, DataType::UInt16, u16::MAX);
    width!(UInt32Array, DataType::UInt32, u32::MAX);
    width!(UInt64Array, DataType::UInt64, u64::MAX);
    let source = Arc::new(
        Decimal256Array::from(vec![
            Some(arrow_buffer::i256::from_i128(125)),
            Some(arrow_buffer::i256::from_i128(-125)),
            None,
        ])
        .with_precision_and_scale(40, 2)
        .unwrap(),
    ) as ArrayRef;
    let expected = Arc::new(
        Decimal256Array::from(vec![
            Some(arrow_buffer::i256::from_i128(13)),
            Some(arrow_buffer::i256::from_i128(-13)),
            None,
        ])
        .with_precision_and_scale(40, 1)
        .unwrap(),
    ) as ArrayRef;
    // Decimal rescale rounds the two half ties away from zero.
    p05_selected_value_case(source, expected);
    p05_selected_value_case(
        Arc::new(
            Decimal256Array::from(vec![Some(arrow_buffer::i256::from_i128(12)), None])
                .with_precision_and_scale(40, -2)
                .unwrap(),
        ),
        Arc::new(
            Decimal256Array::from(vec![Some(arrow_buffer::i256::from_i128(1200)), None])
                .with_precision_and_scale(40, 0)
                .unwrap(),
        ),
    );
    // D256 retains the pinned Arrow grammar: trim, empty integer/fraction
    // parts, then scale padding give coefficient0 for empty/blank input.
    // The separate D128 owner deliberately preprocesses these to NULL; its
    // actual contrast remains text_parsers_preserve_invalid_null_and_decimal_rounding.
    p05_selected_value_case(
        Arc::new(StringArray::from(vec![
            Some("1.25"),
            Some("-1.25"),
            Some(""),
            Some("  "),
            Some("bad"),
            None,
        ])),
        Arc::new(
            Decimal256Array::from(vec![
                Some(arrow_buffer::i256::from_i128(13)),
                Some(arrow_buffer::i256::from_i128(-13)),
                Some(arrow_buffer::i256::ZERO),
                Some(arrow_buffer::i256::ZERO),
                None,
                None,
            ])
            .with_precision_and_scale(8, 1)
            .unwrap(),
        ),
    );
    let source = Arc::new(
        Decimal256Array::from(vec![Some(arrow_buffer::i256::from_i128(1000)), None])
            .with_precision_and_scale(40, 0)
            .unwrap(),
    ) as ArrayRef;
    for (policy, allow) in p05_modes() {
        let r = recipe(
            source.data_type().clone(),
            DataType::Decimal256(3, 0),
            policy,
            allow,
        );
        let result = output(&r, &source, Selection::all(2));
        assert_eq!(result.values().data_type(), &DataType::Decimal256(3, 0));
        assert!(result.values().is_null(0));
        assert!(result.values().is_null(1));
        assert_eq!(
            result.errors().len(),
            usize::from(policy == DecimalOverflowPolicy::ReportError || allow)
        );
        if !result.errors().is_empty() {
            assert_eq!(result.errors()[0].selected_ordinal(), 0);
        }
    }
}

#[test]
fn p05_offset_width_and_fixed_bytes_are_exact_under_every_mode() {
    use arrow_array::*;
    let cases: Vec<(ArrayRef, ArrayRef)> = vec![
        (
            Arc::new(BinaryArray::from(vec![Some(b"a\0b".as_slice()), None])),
            Arc::new(LargeBinaryArray::from(vec![Some(b"a\0b".as_slice()), None])),
        ),
        (
            Arc::new(LargeStringArray::from(vec![Some("a\0b"), None])),
            Arc::new(StringArray::from(vec![Some("a\0b"), None])),
        ),
        (
            Arc::new(StringArray::from(vec![Some("a\0b"), None])),
            Arc::new(BinaryArray::from(vec![Some(b"a\0b".as_slice()), None])),
        ),
        (
            Arc::new(StringArray::from(vec![Some("a\0b"), None])),
            Arc::new(LargeBinaryArray::from(vec![Some(b"a\0b".as_slice()), None])),
        ),
        (
            Arc::new(LargeBinaryArray::from(vec![Some(b"a\0b".as_slice()), None])),
            Arc::new(LargeStringArray::from(vec![Some("a\0b"), None])),
        ),
    ];
    for (source, expected) in cases {
        p05_selected_value_case(source, expected);
    }
    // Integer-to-binary exposes native-endian bytes of its actual width.
    macro_rules! integer_bytes {
        ($array:ty, $value:expr) => {
            p05_selected_value_case(
                Arc::new(<$array>::from(vec![Some($value), None])),
                Arc::new(BinaryArray::from(vec![
                    Some($value.to_ne_bytes().as_slice()),
                    None,
                ])),
            );
        };
    }
    integer_bytes!(Int8Array, 12_i8);
    integer_bytes!(Int16Array, 12_i16);
    integer_bytes!(Int32Array, 12_i32);
    integer_bytes!(Int64Array, 12_i64);
    integer_bytes!(UInt8Array, 12_u8);
    integer_bytes!(UInt16Array, 12_u16);
    integer_bytes!(UInt32Array, 12_u32);
    integer_bytes!(UInt64Array, 12_u64);
    for width in [4, 16] {
        let bytes = vec![b'x'; width as usize];
        let source = Arc::new(LargeBinaryArray::from(vec![
            Some(bytes.as_slice()),
            Some(b"bad".as_slice()),
            None,
        ])) as ArrayRef;
        for (policy, allow) in p05_modes() {
            let r = recipe(
                DataType::LargeBinary,
                DataType::FixedSizeBinary(width),
                policy,
                allow,
            );
            let fixed = output(&r, &source, Selection::all(3));
            let values = fixed
                .values()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            assert_eq!(values.value(0), bytes);
            assert!(values.is_null(1));
            assert!(values.is_null(2));
            assert!(fixed.errors().is_empty());
            let r = recipe(
                DataType::FixedSizeBinary(width),
                DataType::LargeBinary,
                policy,
                allow,
            );
            let restored = output(&r, fixed.values(), Selection::all(3));
            let values = restored
                .values()
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap();
            assert_eq!(values.value(0), bytes);
            assert!(values.is_null(1));
            assert!(values.is_null(2));
            assert!(restored.errors().is_empty());
        }
    }
}

#[test]
fn p05_timestamp_units_and_time_native_carriers_preserve_exact_counts() {
    use arrow_array::*;
    // Timestamp-to-numeric exposes the source unit count, not seconds.
    for source in [
        Arc::new(TimestampSecondArray::from(vec![Some(123456), None])) as ArrayRef,
        Arc::new(TimestampMillisecondArray::from(vec![Some(123456), None])),
        Arc::new(TimestampMicrosecondArray::from(vec![Some(123456), None])),
        Arc::new(TimestampNanosecondArray::from(vec![Some(123456), None])),
    ] {
        p05_selected_value_case(
            source.clone(),
            Arc::new(Int64Array::from(vec![Some(123456), None])),
        );
        p05_selected_value_case(
            source.clone(),
            Arc::new(Float64Array::from(vec![Some(123456.0), None])),
        );
        // The same raw count, not epoch seconds, becomes coefficient12345600.
        p05_selected_value_case(
            source.clone(),
            Arc::new(
                Decimal128Array::from(vec![Some(12345600), None])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
        );
        p05_selected_value_case(
            source,
            Arc::new(
                Decimal256Array::from(vec![Some(arrow_buffer::i256::from_i128(12345600)), None])
                    .with_precision_and_scale(76, 2)
                    .unwrap(),
            ),
        );
    }
    for source in [
        Arc::new(Time32SecondArray::from(vec![Some(1), None])) as ArrayRef,
        Arc::new(Time32MillisecondArray::from(vec![Some(1000), None])),
        Arc::new(Time64MicrosecondArray::from(vec![Some(1_000_000), None])),
        Arc::new(Time64NanosecondArray::from(vec![Some(1_000_000_000), None])),
    ] {
        if source.data_type() == &DataType::Time64(TimeUnit::Microsecond) {
            p05_selected_value_case(
                source,
                Arc::new(Time64NanosecondArray::from(vec![Some(1_000_000_000), None])),
            );
        } else {
            p05_selected_value_case(
                source,
                Arc::new(Time64MicrosecondArray::from(vec![Some(1_000_000), None])),
            );
        }
    }
    // Chrono's default time rendering for an exact second has no fraction.
    p05_selected_value_case(
        Arc::new(Time64MicrosecondArray::from(vec![Some(1_000_000), None])),
        Arc::new(StringArray::from(vec![Some("00:00:01"), None])),
    );
    p05_timestamp_decimal_overflow_selected_cases();
    p05_timestamp_decimal_preparation_contract();
}

#[test]
fn p05_list_and_large_list_retag_keep_offsets_nulls_and_metadata_independently() {
    use arrow_array::{LargeListArray, ListArray};
    use std::collections::HashMap;
    let mut metadata = HashMap::new();
    metadata.insert("owner".to_string(), "p05-target".to_string());
    // Independent input rows are [1,2], NULL, [3]. No old cast/take/core
    // produces the expected child values or the expected compact offsets.
    let list = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>([
        Some(vec![Some(1), Some(2)]),
        None,
        Some(vec![Some(3)]),
    ]);
    let large = LargeListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>([
        Some(vec![Some(1), Some(2)]),
        None,
        Some(vec![Some(3)]),
    ]);
    for source in [Arc::new(list) as ArrayRef, Arc::new(large) as ArrayRef] {
        let child =
            Arc::new(Field::new("renamed", DataType::Int32, true).with_metadata(metadata.clone()));
        let target = if matches!(source.data_type(), DataType::List(_)) {
            DataType::List(child)
        } else {
            DataType::LargeList(child)
        };
        // Pure declarations canonicalize List role names/annotations. Keep
        // that original metadata-only case explicitly classified as Identity.
        // A genuine child NULL widening additionally creates a distinct exact
        // frozen layout and exercises the actual structural execution author.
        let canonical_target = if matches!(target, DataType::List(_)) {
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
        } else {
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, true)))
        };
        let required_child = Arc::new(Field::new("item", DataType::Int32, false));
        let structural_source: ArrayRef =
            if let Some(list) = source.as_any().downcast_ref::<ListArray>() {
                Arc::new(
                    ListArray::try_new(
                        required_child,
                        list.offsets().clone(),
                        list.values().clone(),
                        list.nulls().cloned(),
                    )
                    .unwrap(),
                )
            } else {
                let list = source.as_any().downcast_ref::<LargeListArray>().unwrap();
                Arc::new(
                    LargeListArray::try_new(
                        required_child,
                        list.offsets().clone(),
                        list.values().clone(),
                        list.nulls().cloned(),
                    )
                    .unwrap(),
                )
            };
        for (policy, allow) in p05_modes() {
            let metadata_only = recipe(source.data_type().clone(), target.clone(), policy, allow);
            assert!(matches!(&metadata_only.body, CastBody::Identity));
            assert_eq!(metadata_only.source_type().data_type(), &canonical_target);
            assert_eq!(metadata_only.result_type().data_type(), &canonical_target);
            let r = recipe(
                structural_source.data_type().clone(),
                target.clone(),
                policy,
                allow,
            );
            assert!(matches!(&r.body, CastBody::SelectedArray(Body::Structure)));
            assert!(r.uses_selected_array());
            assert_eq!(r.result_type().data_type(), &canonical_target);
            let selected = [0, 1, 2];
            let result = output(
                &r,
                &structural_source,
                Selection::try_sparse(3, &selected).unwrap(),
            );
            assert_eq!(result.values().data_type(), &canonical_target);
            let field = match result.values().data_type() {
                DataType::List(field) | DataType::LargeList(field) => field,
                _ => panic!("exact List carrier"),
            };
            assert_eq!(field.name(), "item");
            assert!(field.is_nullable());
            assert!(field.metadata().as_arc().is_none());
            let original_field = match &target {
                DataType::List(field) | DataType::LargeList(field) => field,
                _ => unreachable!(),
            };
            assert_eq!(original_field.name(), "renamed");
            assert_eq!(original_field.metadata()["owner"], "p05-target");
            assert!(result.values().is_null(1));
            assert!(result.errors().is_empty());
            let (offsets, values): (Vec<i64>, &ArrayRef) =
                if let Some(list) = result.values().as_any().downcast_ref::<ListArray>() {
                    (
                        list.value_offsets().iter().map(|v| *v as i64).collect(),
                        list.values(),
                    )
                } else {
                    let list = result
                        .values()
                        .as_any()
                        .downcast_ref::<LargeListArray>()
                        .unwrap();
                    (list.value_offsets().to_vec(), list.values())
                };
            assert_eq!(offsets, vec![0, 2, 2, 3]);
            assert_eq!(
                values
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                &[1, 2, 3]
            );
            let rows = [2];
            let sparse = output(
                &r,
                &structural_source,
                Selection::try_sparse(3, &rows).unwrap(),
            );
            assert_eq!(sparse.values().len(), 1);
            assert!(!sparse.values().is_null(0));
            let sparse_values =
                if let Some(list) = sparse.values().as_any().downcast_ref::<ListArray>() {
                    list.values()
                } else {
                    sparse
                        .values()
                        .as_any()
                        .downcast_ref::<LargeListArray>()
                        .unwrap()
                        .values()
                };
            assert_eq!(
                sparse_values
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                &[3]
            );
        }
    }
}

#[test]
fn p05_struct_parent_null_and_metadata_follow_actual_by_name_permutation() {
    use std::collections::HashMap;
    let mut metadata = HashMap::new();
    metadata.insert("owner".to_string(), "p05-target".to_string());
    let source = Arc::new(StructArray::new(
        vec![
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Field::new("b", DataType::FixedSizeBinary(4), true)),
        ]
        .into(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([b"abcd", b"wxyz", b"uvwx"].into_iter())
                    .unwrap(),
            ),
        ],
        Some(arrow_buffer::NullBuffer::from(vec![true, false, true])),
    )) as ArrayRef;
    let target = DataType::Struct(
        vec![
            Arc::new(
                Field::new("b", DataType::FixedSizeBinary(4), true).with_metadata(metadata.clone()),
            ),
            Arc::new(Field::new("a", DataType::Int32, true).with_metadata(metadata)),
        ]
        .into(),
    );
    // Struct field names are semantic correspondence; provider annotations
    // remain in the original request but not in the execution layout.
    let canonical_target = DataType::Struct(
        vec![
            Arc::new(Field::new("b", DataType::FixedSizeBinary(4), true)),
            Arc::new(Field::new("a", DataType::Int32, true)),
        ]
        .into(),
    );
    // Every target name exists: output b,a is a permutation, not ordinal
    // relabeling. The selected NULL parent remains NULL independent of children.
    for (policy, allow) in p05_modes() {
        let r = recipe(source.data_type().clone(), target.clone(), policy, allow);
        assert!(matches!(&r.body, CastBody::SelectedArray(Body::Structure)));
        assert_eq!(r.result_type().data_type(), &canonical_target);
        let rows = [1, 2];
        let result = output(&r, &source, Selection::try_sparse(3, &rows).unwrap());
        assert_eq!(result.values().data_type(), &canonical_target);
        let DataType::Struct(fields) = result.values().data_type() else {
            panic!("exact Struct")
        };
        assert_eq!(
            fields
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["b", "a"]
        );
        assert!(
            fields
                .iter()
                .all(|field| field.metadata().as_arc().is_none())
        );
        let DataType::Struct(original_fields) = &target else {
            unreachable!()
        };
        assert_eq!(
            original_fields
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["b", "a"]
        );
        for field in original_fields {
            assert_eq!(field.metadata()["owner"], "p05-target");
        }
        assert!(result.errors().is_empty());
        let values = result
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(values.is_null(0));
        assert!(!values.is_null(1));
        assert_eq!(
            values
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(1),
            b"uvwx"
        );
        assert_eq!(
            values
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(1),
            3
        );
    }
}

fn p05_timestamp_decimal_overflow_selected_cases() {
    let raw = vec![
        Some(123_i64),
        Some(-123),
        Some(0),
        None,
        Some(1000),
        Some(-1000),
    ];
    let input: ArrayRef = Arc::new(TimestampMicrosecondArray::from(raw));
    let sparse = [1, 3, 4];
    let empty = [];
    let targets = [
        DataType::Decimal128(5, 2),
        DataType::Decimal256(5, 2),
        DataType::Decimal128(5, -2),
        DataType::Decimal256(5, -2),
    ];
    for target in targets {
        let scale = match &target {
            DataType::Decimal128(_, s) | DataType::Decimal256(_, s) => *s,
            _ => unreachable!(),
        };
        let expected_full: Vec<Option<i128>> = if scale == 2 {
            // Raw counts 123/-123 become coefficients +/-12300. Counts
            // +/-1000 need six digits, so precision5 returns typed NULL.
            vec![Some(12300), Some(-12300), Some(0), None, None, None]
        } else {
            // Raw integer counts /100 truncate toward zero at scale=-2.
            vec![Some(1), Some(-1), Some(0), None, Some(10), Some(-10)]
        };
        let expected_sparse = if scale == 2 {
            vec![Some(-12300), None, None]
        } else {
            vec![Some(-1), None, Some(10)]
        };
        assert!(!novarocks_type_contract::is_checked_decimal_numeric_cast(
            input.data_type(),
            &target
        ));
        for (policy, allow) in [
            (DecimalOverflowPolicy::OutputNull, false),
            (DecimalOverflowPolicy::OutputNull, true),
            (DecimalOverflowPolicy::ReportError, false),
            (DecimalOverflowPolicy::ReportError, true),
        ] {
            let r = recipe(input.data_type().clone(), target.clone(), policy, allow);
            for (selection, coefficients) in [
                (Selection::all(6), expected_full.clone()),
                (
                    Selection::try_sparse(6, &sparse).unwrap(),
                    expected_sparse.clone(),
                ),
                (Selection::try_sparse(6, &empty).unwrap(), vec![]),
            ] {
                let actual = output(&r, &input, selection);
                let expected: ArrayRef = match &target {
                    DataType::Decimal128(p, s) => Arc::new(
                        Decimal128Array::from(coefficients)
                            .with_precision_and_scale(*p, *s)
                            .unwrap(),
                    ),
                    DataType::Decimal256(p, s) => Arc::new(
                        Decimal256Array::from(
                            coefficients
                                .into_iter()
                                .map(|v| v.map(arrow_buffer::i256::from_i128))
                                .collect::<Vec<_>>(),
                        )
                        .with_precision_and_scale(*p, *s)
                        .unwrap(),
                    ),
                    _ => unreachable!(),
                };
                assert_eq!(
                    actual.selection().iter().collect::<Vec<_>>(),
                    selection.iter().collect::<Vec<_>>()
                );
                assert_eq!(
                    actual.values().to_data(),
                    expected.to_data(),
                    "{:?} -> {target:?} {policy:?} allow={allow}",
                    input.data_type()
                );
                // Timestamp is not in the existing checked-decimal source
                // classifier. Do not add ReportError/ALLOW row errors here.
                assert!(actual.errors().is_empty());
            }
        }
    }
}

fn p05_timestamp_decimal_preparation_contract() {
    let context = ExpressionEffectContext {
        use_id: novarocks_type_contract::ExpressionUseId::new(805),
        domain: novarocks_type_contract::EvaluationDomainId::new(5),
        demand: novarocks_type_contract::EvaluationDemand::Value,
    };
    for target in [DataType::Decimal128(5, 2), DataType::Decimal256(5, 2)] {
        for (policy, allow) in [
            (DecimalOverflowPolicy::OutputNull, false),
            (DecimalOverflowPolicy::OutputNull, true),
            (DecimalOverflowPolicy::ReportError, false),
            (DecimalOverflowPolicy::ReportError, true),
        ] {
            for source_nullable in [false, true] {
                for target_nullable in [false, true] {
                    let source = FunctionValueType::new(
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        source_nullable,
                    );
                    let result = FunctionValueType::new(target.clone(), target_nullable);
                    let actual = PreparedCastRecipe::try_new(
                        CastOperation::Carrier,
                        &source,
                        &result,
                        policy,
                        allow,
                        &Control,
                    );
                    if !target_nullable {
                        assert_eq!(actual, Err(CastPrepareError::TypeMismatch));
                    } else {
                        let r = actual.unwrap();
                        assert!(r.uses_selected_array());
                        assert_eq!(r.source_type(), &source);
                        assert_eq!(r.result_type(), &result);
                        assert_eq!(r.policy(), policy);
                        assert_eq!(r.allow_throw_exception(), allow);
                        assert!(
                            r.own_effects(context)
                                .for_use(context)
                                .unwrap()
                                .may_raise_row_error
                        );
                        assert!(carrier_cast_can_produce_null_with_policy(
                            source.data_type(),
                            &target,
                            policy,
                            allow
                        ));
                    }
                }
            }
            // No timezone policy is invented by this carrier-only profile.
            let zoned = FunctionValueType::new(
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            );
            assert_eq!(
                PreparedCastRecipe::try_new(
                    CastOperation::Carrier,
                    &zoned,
                    &FunctionValueType::new(target.clone(), true),
                    policy,
                    allow,
                    &Control
                ),
                Err(CastPrepareError::Unsupported)
            );
        }
    }
}

// These cases qualify the current body against mathematical results. The old
// helper's invalid precision result and scale-delta panic are retained as FAIL
// evidence; neither is a successful behavior to reproduce.
fn p05_boolean_wide_decimal_precision_boundary() {
    let source = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef;
    for (policy, allow) in p05_modes() {
        let r = recipe(DataType::Boolean, DataType::Decimal256(1, 1), policy, allow);
        for rows in [vec![0, 1, 2], vec![0, 2], vec![]] {
            let result = output(&r, &source, Selection::try_sparse(3, &rows).unwrap());
            let values = result
                .values()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap();
            assert_eq!(values.data_type(), &DataType::Decimal256(1, 1));
            assert_eq!(values.len(), rows.len());
            for (ordinal, &row) in rows.iter().enumerate() {
                // true denotes 1, requiring coefficient10 at scale1: it cannot
                // fit precision1. false is exactly0 and source NULL stays NULL.
                if row == 1 {
                    assert!(values.is_valid(ordinal));
                    assert_eq!(values.value(ordinal), arrow_buffer::i256::ZERO);
                } else {
                    assert!(values.is_null(ordinal));
                }
            }
            let expected_errors: Vec<_> = rows
                .iter()
                .enumerate()
                .filter_map(|(ordinal, &row)| {
                    (row == 0 && policy == DecimalOverflowPolicy::ReportError).then_some(ordinal)
                })
                .collect();
            assert_eq!(
                result
                    .errors()
                    .iter()
                    .map(|error| error.selected_ordinal())
                    .collect::<Vec<_>>(),
                expected_errors
            );
        }
    }
}
fn p05_wide_decimal_minimum_scale_zero_is_exact() {
    for (precision, source_scale, target_scale, downscale) in
        [(1, i8::MIN, 0, false), (76, 76, i8::MIN, true)]
    {
        let source = Arc::new(
            Decimal256Array::from(vec![
                Some(arrow_buffer::i256::ZERO),
                Some(arrow_buffer::i256::ONE),
                Some(arrow_buffer::i256::MINUS_ONE),
                None,
            ])
            .with_precision_and_scale(precision, source_scale)
            .unwrap(),
        ) as ArrayRef;
        for (policy, allow) in p05_modes() {
            let r = recipe(
                source.data_type().clone(),
                DataType::Decimal128(38, target_scale),
                policy,
                allow,
            );
            // This pair genuinely selects the scalar DecimalNarrow owner; do
            // not force a selected-array route to bypass the actual author.
            assert!(!r.uses_selected_array());
            assert_eq!(
                r.source_type().data_type(),
                &DataType::Decimal256(precision, source_scale)
            );
            assert_eq!(
                r.result_type().data_type(),
                &DataType::Decimal128(38, target_scale)
            );
            for row in 0..4 {
                let actual = r
                    .evaluate_row(EvaluatedArgument::Column(&source), row, row, &Control)
                    .unwrap();
                if row == 3 {
                    assert_eq!(actual, CastRowResult::Null);
                } else if row == 0 || downscale {
                    // Zero stays zero; dividing any coefficient here by10^204
                    // rounds to0 independently of precision and source sign.
                    assert_eq!(actual, CastRowResult::Decimal128(0));
                } else if policy == DecimalOverflowPolicy::ReportError || allow {
                    let CastRowResult::RowError(error) = actual else {
                        panic!("decimal overflow must retain its selected row error");
                    };
                    assert_eq!(error.selected_ordinal(), row);
                } else {
                    assert_eq!(actual, CastRowResult::Null);
                }
            }
        }
    }
}

fn p05_selected_wide_decimal_delta_proofs() {
    let ten52: i256 = "10000000000000000000000000000000000000000000000000000"
        .parse()
        .unwrap();
    let ten30: i256 = "1000000000000000000000000000000".parse().unwrap();
    let max76: i256 =
        "9999999999999999999999999999999999999999999999999999999999999999999999999999"
            .parse()
            .unwrap();
    let cases: Vec<(ArrayRef, DataType, Vec<Option<i256>>)> = vec![
        (
            Arc::new(
                Decimal256Array::from(vec![Some(i256::ONE), Some(-i256::ONE), Some(ten30), None])
                    .with_precision_and_scale(76, -52)
                    .unwrap(),
            ),
            DataType::Decimal256(76, 0),
            vec![Some(ten52), Some(-ten52), None, None],
        ),
        (
            Arc::new(
                Decimal128Array::from(vec![Some(0), Some(1), Some(-1), None])
                    .with_precision_and_scale(38, i8::MIN)
                    .unwrap(),
            ),
            DataType::Decimal256(76, 0),
            vec![Some(i256::ZERO), None, None, None],
        ),
        (
            Arc::new(
                Decimal256Array::from(vec![Some(max76), Some(-max76), Some(i256::ZERO), None])
                    .with_precision_and_scale(76, 76)
                    .unwrap(),
            ),
            DataType::Decimal256(76, i8::MIN),
            vec![Some(i256::ZERO), Some(i256::ZERO), Some(i256::ZERO), None],
        ),
    ];
    for (source, target, expected) in cases {
        for (policy, allow) in p05_modes() {
            let r = recipe(source.data_type().clone(), target.clone(), policy, allow);
            for rows in [vec![0, 1, 2, 3], vec![1, 2, 3], vec![]] {
                let result = output(&r, &source, Selection::try_sparse(4, &rows).unwrap());
                assert_eq!(result.values().data_type(), &target);
                let values = result
                    .values()
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .unwrap();
                assert_eq!(values.len(), rows.len());
                let mut errors = vec![];
                for (ordinal, &row) in rows.iter().enumerate() {
                    if let Some(value) = expected[row] {
                        assert!(values.is_valid(ordinal));
                        assert_eq!(values.value(ordinal), value);
                    } else {
                        assert!(values.is_null(ordinal));
                        if source.is_valid(row)
                            && (policy == DecimalOverflowPolicy::ReportError || allow)
                        {
                            errors.push(ordinal);
                        }
                    }
                }
                assert_eq!(
                    result
                        .errors()
                        .iter()
                        .map(|error| error.selected_ordinal())
                        .collect::<Vec<_>>(),
                    errors
                );
            }
        }
    }
}

fn p05_decimal_safe_table_miss_retains_static_typed_refusal() {
    use novarocks_failure_contract::FailureProjection;
    use std::error::Error;
    fn assert_static(error: &RowDataError, ordinal: usize) {
        assert_eq!(error.selected_ordinal(), ordinal);
        assert_eq!(
            error.failure_fact().reason(),
            crate::DataFailureReason::InvalidValue
                .failure_fact()
                .reason()
        );
        let mut cause: Option<&(dyn Error + 'static)> = Some(error);
        let mut found = false;
        while let Some(error) = cause {
            if let Some(arrow_schema::ArrowError::CastError(_)) =
                error.downcast_ref::<arrow_schema::ArrowError>()
            {
                found = true;
            }
            cause = error.source();
        }
        assert!(
            found,
            "static scale-table refusal must retain original Arrow CastError"
        );
    }
    // Arrow's target-carrier table is checked before source_precision+delta.
    // Delta52/90 fit i8 but miss tables38/76, so even zero/NULL retain the
    // existing static refusal. These are not unsafe-intermediate repairs.
    let narrow = Arc::new(
        Decimal256Array::from(vec![Some(i256::ZERO), None])
            .with_precision_and_scale(76, -52)
            .unwrap(),
    ) as ArrayRef;
    let wide = Arc::new(
        Decimal128Array::from(vec![Some(0), None])
            .with_precision_and_scale(38, -90)
            .unwrap(),
    ) as ArrayRef;
    for (policy, allow) in p05_modes() {
        let r = recipe(
            narrow.data_type().clone(),
            DataType::Decimal128(38, 0),
            policy,
            allow,
        );
        for row in 0..2 {
            let CastRowResult::RowError(error) = r
                .evaluate_row(EvaluatedArgument::Column(&narrow), row, row, &Control)
                .unwrap()
            else {
                panic!("safe delta beyond target table must retain static refusal");
            };
            assert_static(&error, row);
        }
        let r = recipe(
            wide.data_type().clone(),
            DataType::Decimal256(76, 0),
            policy,
            allow,
        );
        let result = output(&r, &wide, Selection::all(2));
        assert_eq!(result.values().null_count(), 2);
        assert_eq!(result.errors().len(), 2);
        for (ordinal, error) in result.errors().iter().enumerate() {
            assert_static(error, ordinal);
        }
    }
}

#[test]
fn utf8_offset_widening_preserves_exact_successful_null_obligation() {
    for (policy, allow) in p05_modes() {
        assert!(!carrier_cast_can_produce_null_with_policy(
            &DataType::Utf8,
            &DataType::LargeUtf8,
            policy,
            allow,
        ));
        for source_nullable in [false, true] {
            for result_nullable in [false, true] {
                let prepared = PreparedCastRecipe::try_new(
                    CastOperation::Carrier,
                    &FunctionValueType::new(DataType::Utf8, source_nullable),
                    &FunctionValueType::new(DataType::LargeUtf8, result_nullable),
                    policy,
                    allow,
                    &Control,
                );
                if source_nullable && !result_nullable {
                    assert_eq!(prepared, Err(CastPrepareError::TypeMismatch));
                    continue;
                }
                let r = prepared.unwrap();
                let input: ArrayRef = Arc::new(StringArray::from(vec![
                    Some("alpha"),
                    if source_nullable { None } else { Some("") },
                    Some("尾"),
                ]));
                let rows = [0, 2];
                for selection in [
                    Selection::all(3),
                    Selection::try_sparse(3, &rows).unwrap(),
                    Selection::try_sparse(3, &[]).unwrap(),
                ] {
                    let out = output(&r, &input, selection);
                    assert!(out.errors().is_empty());
                    let values = out
                        .values()
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .unwrap();
                    assert_eq!(values.len(), selection.len());
                    for (ordinal, row) in selection.iter().enumerate() {
                        if row == 1 && source_nullable {
                            assert!(values.is_null(ordinal));
                        } else {
                            assert!(!values.is_null(ordinal));
                            assert_eq!(values.value(ordinal), ["alpha", "", "尾"][row]);
                        }
                    }
                }
            }
        }
        // The narrow fact never admits another body or weakens NULL sources.
        assert!(carrier_cast_can_produce_null_with_policy(
            &DataType::Binary,
            &DataType::Utf8,
            policy,
            allow,
        ));
        let r = PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &FunctionValueType::new(DataType::Utf8, false),
            &FunctionValueType::new(DataType::LargeUtf8, false),
            policy,
            allow,
            &Control,
        )
        .unwrap();
        let invalid: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>]));
        assert!(matches!(
            r.evaluate_selected_array(
                EvaluatedArgument::Column(&invalid),
                Selection::all(1),
                &[],
                &Control
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
}

#[test]
fn large_date_text_selected_cast_keeps_original_raw_calendar_domain() {
    let source: ArrayRef = Arc::new(arrow_array::LargeStringArray::from(vec![
        Some("1970-01-01"),
        Some(" 1970-01-01 "),
        Some("+5881580-07-11"),
        None,
    ]));
    for (policy, allow) in p05_modes() {
        let r = recipe(DataType::LargeUtf8, DataType::Date32, policy, allow);
        let result = output(&r, &source, Selection::all(4));
        let values = result
            .values()
            .as_any()
            .downcast_ref::<arrow_array::Date32Array>()
            .unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(0), None, None, None]
        );
    }
}

#[test]
fn large_raw_numeric_text_preserves_strict_boundaries_for_every_admitted_primitive() {
    let input = Arc::new(LargeStringArray::from(vec![
        Some("7"),
        Some("+7"),
        Some("-0"),
        Some(" 7"),
        Some("7 "),
        Some("\t7"),
        Some("7\n"),
        Some("\u{a0}7"),
        Some("7\u{2003}"),
        Some("\u{85}7"),
        Some("7\u{3000}"),
        Some("\u{feff}7"),
        Some("7\0"),
        Some(""),
        None,
    ])) as ArrayRef;
    for target in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float32,
        DataType::Float64,
    ] {
        let r = recipe(
            DataType::LargeUtf8,
            target.clone(),
            DecimalOverflowPolicy::OutputNull,
            false,
        );
        let actual = output(&r, &input, Selection::all(input.len()));
        assert!(actual.errors().is_empty());
        assert_eq!(actual.values().len(), 15);
        for row in 0..15 {
            assert_eq!(
                actual.values().is_null(row),
                row >= 3,
                "{target:?} row {row}"
            );
        }
        macro_rules! values {
            ($array:ty, $seven:expr, $zero:expr) => {{
                let array = actual.values().as_any().downcast_ref::<$array>().unwrap();
                assert_eq!(array.value(0), $seven);
                assert_eq!(array.value(1), $seven);
                assert_eq!(array.value(2), $zero);
            }};
        }
        match target {
            DataType::Int8 => values!(Int8Array, 7, 0),
            DataType::Int16 => values!(Int16Array, 7, 0),
            DataType::Int32 => values!(Int32Array, 7, 0),
            DataType::Int64 => values!(Int64Array, 7, 0),
            DataType::UInt8 => values!(UInt8Array, 7, 0),
            DataType::UInt16 => values!(UInt16Array, 7, 0),
            DataType::UInt32 => values!(UInt32Array, 7, 0),
            DataType::UInt64 => values!(UInt64Array, 7, 0),
            DataType::Float32 => values!(Float32Array, 7.0, -0.0),
            DataType::Float64 => values!(Float64Array, 7.0, -0.0),
            _ => unreachable!(),
        }
    }
}

#[test]
fn raw_decimal_text_retains_unicode_trim_rounding_blank_and_exponent_rejection() {
    let input = Arc::new(LargeStringArray::from(vec![
        Some("\u{a0}1.25\u{2003}"),
        Some("-1.255"),
        Some("1.255"),
        Some("1e2"),
        Some(""),
        Some("\u{3000}"),
        Some("999.995"),
        None,
    ])) as ArrayRef;
    for target in [DataType::Decimal128(5, 2), DataType::Decimal256(5, 2)] {
        let r = recipe(
            DataType::LargeUtf8,
            target.clone(),
            DecimalOverflowPolicy::OutputNull,
            false,
        );
        let actual = output(&r, &input, Selection::all(8));
        assert!(actual.errors().is_empty());
        let expected = vec![
            Some(125_i128),
            Some(-126),
            Some(126),
            None,
            Some(0),
            Some(0),
            None,
            None,
        ];
        match target {
            DataType::Decimal128(..) => assert_eq!(
                actual
                    .values()
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                expected
            ),
            DataType::Decimal256(..) => assert_eq!(
                actual
                    .values()
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                expected
                    .into_iter()
                    .map(|v| v.map(i256::from_i128))
                    .collect::<Vec<_>>()
            ),
            _ => unreachable!(),
        }
    }
    // Utf8 -> Decimal256 is also an installed raw array profile.
    let input = Arc::new(StringArray::from(vec![
        Some("\u{a0}1.25\u{2003}"),
        Some("1e2"),
    ])) as ArrayRef;
    let r = recipe(
        DataType::Utf8,
        DataType::Decimal256(5, 2),
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let actual = output(&r, &input, Selection::all(2));
    assert_eq!(
        actual
            .values()
            .as_any()
            .downcast_ref::<Decimal256Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(i256::from_i128(125)), None]
    );
}

#[test]
fn raw_decimal_negative_scale_remains_outer_even_for_null_source() {
    let input = Arc::new(LargeStringArray::from(vec![None::<&str>])) as ArrayRef;
    for target in [DataType::Decimal128(5, -1), DataType::Decimal256(5, -1)] {
        let r = recipe(
            DataType::LargeUtf8,
            target,
            DecimalOverflowPolicy::OutputNull,
            false,
        );
        let failure = r
            .evaluate_selected_array(
                EvaluatedArgument::Column(&input),
                Selection::all(1),
                &[],
                &Control,
            )
            .unwrap_err();
        assert!(
            failure
                .to_string()
                .contains("Cannot cast string to decimal with negative scale -1")
        );
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        let mut cause = std::error::Error::source(&failure);
        let mut owned = false;
        while let Some(source) = cause {
            owned |= source.is::<crate::math_numeric::RawNumericCastFailure>();
            assert!(!source.is::<arrow_schema::ArrowError>());
            cause = source.source();
        }
        assert!(owned);
    }
}

fn checked_time_input(ty: &DataType, values: Vec<Option<i64>>) -> ArrayRef {
    use arrow_array::*;
    match ty {
        DataType::Time32(TimeUnit::Second) => Arc::new(Time32SecondArray::from(
            values
                .into_iter()
                .map(|v| v.map(|v| i32::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Time32(TimeUnit::Millisecond) => Arc::new(Time32MillisecondArray::from(
            values
                .into_iter()
                .map(|v| v.map(|v| i32::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Time64(TimeUnit::Microsecond) => Arc::new(Time64MicrosecondArray::from(values)),
        DataType::Time64(TimeUnit::Nanosecond) => Arc::new(Time64NanosecondArray::from(values)),
        _ => panic!("test requires an exact TIME carrier"),
    }
}
fn checked_time_values(array: &ArrayRef) -> Vec<Option<i64>> {
    use arrow_array::*;
    match array.data_type() {
        DataType::Time32(TimeUnit::Second) => array
            .as_any()
            .downcast_ref::<Time32SecondArray>()
            .unwrap()
            .iter()
            .map(|v| v.map(i64::from))
            .collect(),
        DataType::Time32(TimeUnit::Millisecond) => array
            .as_any()
            .downcast_ref::<Time32MillisecondArray>()
            .unwrap()
            .iter()
            .map(|v| v.map(i64::from))
            .collect(),
        DataType::Time64(TimeUnit::Microsecond) => array
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .unwrap()
            .iter()
            .collect(),
        DataType::Time64(TimeUnit::Nanosecond) => array
            .as_any()
            .downcast_ref::<Time64NanosecondArray>()
            .unwrap()
            .iter()
            .collect(),
        _ => panic!("test requires an exact TIME carrier"),
    }
}
fn checked_time_types() -> [DataType; 4] {
    [
        DataType::Time32(TimeUnit::Second),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Nanosecond),
    ]
}
#[test]
fn checked_time_all_unit_pairs_keep_negative_duration_truncation_and_null() {
    let types = checked_time_types();
    let durations = [90_000, 90_000_000, 90_000_000_000, 90_000_000_000_000];
    // Independent literal counts for one source tick in each target unit.
    let one = [
        [1, 1_000, 1_000_000, 1_000_000_000],
        [0, 1, 1_000, 1_000_000],
        [0, 0, 1, 1_000],
        [0, 0, 0, 1],
    ];
    for (i, source) in types.iter().enumerate() {
        let input = checked_time_input(
            source,
            vec![
                Some(durations[i]),
                Some(-durations[i]),
                Some(1),
                Some(-1),
                None,
            ],
        );
        for (j, target) in types.iter().enumerate() {
            if i == j {
                continue;
            }
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let r = recipe(source.clone(), target.clone(), policy, allow);
                    let actual = output(&r, &input, Selection::all(5));
                    assert_eq!(
                        checked_time_values(actual.values()),
                        vec![
                            Some(durations[j]),
                            Some(-durations[j]),
                            Some(one[i][j]),
                            Some(-one[i][j]),
                            None
                        ],
                        "{source:?}->{target:?}"
                    );
                    assert!(actual.errors().is_empty());
                }
            }
        }
    }
}

#[test]
fn checked_time_width_edges_follow_allow_and_retain_real_numeric_sources() {
    let types = checked_time_types();
    // (source, target, positive in-range, positive overflow, negative in-range,
    // negative overflow, positive output, negative output). No SDK creates these.
    type TimeWidthBoundaryCase = (usize, usize, i64, i64, i64, i64, i64, i64);
    let cases: [TimeWidthBoundaryCase; 6] = [
        (
            0,
            1,
            2_147_483,
            2_147_484,
            -2_147_483,
            -2_147_484,
            2_147_483_000,
            -2_147_483_000,
        ),
        (
            2,
            0,
            2_147_483_647_999_999,
            2_147_483_648_000_000,
            -2_147_483_648_999_999,
            -2_147_483_649_000_000,
            2_147_483_647,
            -2_147_483_648,
        ),
        (
            2,
            1,
            2_147_483_647_999,
            2_147_483_648_000,
            -2_147_483_648_999,
            -2_147_483_649_000,
            2_147_483_647,
            -2_147_483_648,
        ),
        (
            3,
            0,
            2_147_483_647_999_999_999,
            2_147_483_648_000_000_000,
            -2_147_483_648_999_999_999,
            -2_147_483_649_000_000_000,
            2_147_483_647,
            -2_147_483_648,
        ),
        (
            3,
            1,
            2_147_483_647_999_999,
            2_147_483_648_000_000,
            -2_147_483_648_999_999,
            -2_147_483_649_000_000,
            2_147_483_647,
            -2_147_483_648,
        ),
        (
            2,
            3,
            9_223_372_036_854_775,
            9_223_372_036_854_776,
            -9_223_372_036_854_775,
            -9_223_372_036_854_776,
            9_223_372_036_854_775_000,
            -9_223_372_036_854_775_000,
        ),
    ];
    for (i, j, pos, over, neg, under, positive, negative) in cases {
        let input = checked_time_input(
            &types[i],
            vec![
                Some(pos),
                Some(over),
                Some(neg),
                Some(under),
                None,
                Some(if i == 0 {
                    i64::from(i32::MAX)
                } else {
                    i64::MAX
                }),
                Some(if i == 0 {
                    i64::from(i32::MIN)
                } else {
                    i64::MIN
                }),
            ],
        );
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                let r = recipe(types[i].clone(), types[j].clone(), policy, allow);
                let actual = output(&r, &input, Selection::all(7));
                assert_eq!(
                    checked_time_values(actual.values()),
                    vec![Some(positive), None, Some(negative), None, None, None, None]
                );
                assert_eq!(
                    actual
                        .errors()
                        .iter()
                        .map(RowDataError::selected_ordinal)
                        .collect::<Vec<_>>(),
                    if allow { vec![1, 3, 5, 6] } else { vec![] }
                );
                for error in actual.errors() {
                    use novarocks_failure_contract::FailureProjection;
                    assert_eq!(
                        error.projected_fact(),
                        crate::DataFailureReason::NumericOutOfRange.failure_fact()
                    );
                    let mut cause = std::error::Error::source(error);
                    let (mut temporal, mut numeric) = (false, false);
                    while let Some(source) = cause {
                        temporal |= source.is::<crate::temporal_carrier::TemporalCastFailure>();
                        numeric |= source.is::<std::num::TryFromIntError>();
                        assert!(!source.is::<arrow_schema::ArrowError>());
                        cause = source.source();
                    }
                    assert!(temporal && numeric);
                }
            }
        }
    }
}

#[test]
fn checked_time_demand_keeps_hidden_nulls_inherited_errors_and_scalar_addresses() {
    let from = DataType::Time64(TimeUnit::Microsecond);
    let to = DataType::Time64(TimeUnit::Nanosecond);
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            let r = recipe(from.clone(), to.clone(), policy, allow);
            let input = checked_time_input(&from, vec![Some(i64::MAX), Some(-90_000_000_000)]);
            let rows = [1];
            let selection = Selection::try_sparse(2, &rows).unwrap();
            let actual = output(&r, &input, selection);
            assert_eq!(
                checked_time_values(actual.values()),
                vec![Some(-90_000_000_000_000)]
            );
            assert!(actual.errors().is_empty());
            let data = input
                .to_data()
                .into_builder()
                .nulls(Some(arrow_buffer::NullBuffer::from(vec![false, true])))
                .build()
                .unwrap();
            let hidden_null = arrow_array::make_array(data);
            let actual = output(&r, &hidden_null, Selection::all(2));
            assert_eq!(
                checked_time_values(actual.values()),
                vec![None, Some(-90_000_000_000_000)]
            );
            assert!(actual.errors().is_empty());
            let inherited = [RowDataError::new(0, "original inherited TIME refusal")];
            let original = SelectedValues::try_new(
                Selection::all(2),
                &from,
                hidden_null.clone(),
                inherited.to_vec().into_boxed_slice(),
            )
            .unwrap();
            let actual = r
                .evaluate_selected_array(
                    EvaluatedArgument::SelectedColumn(&original),
                    original.selection(),
                    original.errors(),
                    &Control,
                )
                .unwrap();
            assert_eq!(actual.errors(), &inherited);
            assert_eq!(
                checked_time_values(actual.values()),
                vec![None, Some(-90_000_000_000_000)]
            );
            let scalar = input.slice(0, 1);
            let actual = r
                .evaluate_selected_array(
                    EvaluatedArgument::Scalar(&scalar),
                    Selection::all(3),
                    &[],
                    &Control,
                )
                .unwrap();
            assert_eq!(checked_time_values(actual.values()), vec![None, None, None]);
            assert_eq!(
                actual
                    .errors()
                    .iter()
                    .map(RowDataError::selected_ordinal)
                    .collect::<Vec<_>>(),
                if allow { vec![0, 1, 2] } else { vec![] }
            );
            let nonnull_source = PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &FunctionValueType::new(from.clone(), false),
                &FunctionValueType::new(to.clone(), true),
                policy,
                allow,
                &Control,
            )
            .unwrap();
            let null = checked_time_input(&from, vec![None]);
            let failure = nonnull_source
                .evaluate_selected_array(
                    EvaluatedArgument::Column(&null),
                    Selection::all(1),
                    &[],
                    &Control,
                )
                .unwrap_err();
            assert!(
                failure
                    .to_string()
                    .contains("non-null selected CAST source contains NULL")
            );
            // The original conservative output-nullability admission is unchanged.
            let failure = PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &FunctionValueType::new(from.clone(), false),
                &FunctionValueType::new(to.clone(), false),
                policy,
                allow,
                &Control,
            )
            .unwrap_err();
            assert!(matches!(failure, CastPrepareError::TypeMismatch));
        }
    }
}

#[test]
fn checked_time_control_refusal_has_no_followup_callbacks() {
    use std::sync::Mutex;
    struct Stop {
        calls: Mutex<usize>,
        at: Option<usize>,
    }
    impl KernelEvaluationControl for Stop {
        fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
            assert!(units <= 256);
            let mut calls = self.calls.lock().unwrap();
            let at = *calls;
            if let Some(stop) = self.at {
                assert!(at <= stop, "callback after TIME refusal");
            }
            *calls += 1;
            if self.at == Some(at) {
                Err(KernelFailure::Cancelled)
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("TIME CAST never waits")
        }
    }
    let r = recipe(
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Nanosecond),
        DecimalOverflowPolicy::ReportError,
        true,
    );
    let input = checked_time_input(
        &DataType::Time64(TimeUnit::Microsecond),
        vec![Some(i64::MAX)],
    );
    let baseline = Stop {
        calls: Mutex::new(0),
        at: None,
    };
    let actual = r
        .evaluate_selected_array(
            EvaluatedArgument::Column(&input),
            Selection::all(1),
            &[],
            &baseline,
        )
        .unwrap();
    assert_eq!(actual.errors().len(), 1);
    let count = *baseline.calls.lock().unwrap();
    assert!(count > 0);
    for at in 0..count {
        let stop = Stop {
            calls: Mutex::new(0),
            at: Some(at),
        };
        let failure = r
            .evaluate_selected_array(
                EvaluatedArgument::Column(&input),
                Selection::all(1),
                &[],
                &stop,
            )
            .unwrap_err();
        assert_eq!(failure, KernelFailure::Cancelled);
        assert_eq!(*stop.calls.lock().unwrap(), at + 1);
    }
}
