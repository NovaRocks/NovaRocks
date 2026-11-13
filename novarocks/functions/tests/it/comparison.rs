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

use arrow_array::types::*;
use arrow_array::*;
use arrow_schema::DataType;
use arrow_schema::TimeUnit;
use novarocks_functions::*;
use novarocks_functions::{ConstantPolicy, ConstantPool, ConstantValue, SelectedValues, Selection};
use novarocks_type_contract::CompileControlError;
use novarocks_type_contract::CompilePhase;
use novarocks_type_contract::FunctionValueType;
use novarocks_type_contract::PureCompileControl;
use novarocks_type_contract::ValueLogicalType;
use std::cmp::Ordering;
use std::error::Error;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(matches!(
            phase,
            CompilePhase::FunctionSpecialization | CompilePhase::Validate
        ));
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        if let Some((at, cause)) = self.refusal.clone()
            && trace.len() == at + 1
        {
            Err(cause)
        } else {
            Ok(())
        }
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        if let Some((at, cause)) = &self.refusal
            && trace.len() == *at + 1
        {
            Err(cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("equality must not wait")
    }
}
fn ty(data_type: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(data_type, nullable)
}
fn recipe(left: &ArrayRef, right: &ArrayRef) -> PreparedComparisonRecipe {
    PreparedComparisonRecipe::try_new(
        ComparisonOperator::Eq,
        &ty(left.data_type().clone(), true),
        &ty(right.data_type().clone(), true),
        &CompileControl::default(),
    )
    .unwrap()
}
fn compare_arrays(left: ArrayRef, right: ArrayRef, expected: &[Option<bool>]) {
    for operator in [ComparisonOperator::Eq, ComparisonOperator::Ne] {
        let prepared = PreparedComparisonRecipe::try_new(
            operator,
            &ty(left.data_type().clone(), true),
            &ty(right.data_type().clone(), true),
            &CompileControl::default(),
        )
        .unwrap();
        assert_eq!(prepared.operator(), operator);
        for (row, expected) in expected.iter().enumerate() {
            let expected = expected.map(|value| {
                if operator == ComparisonOperator::Eq {
                    value
                } else {
                    !value
                }
            });
            assert_eq!(
                prepared
                    .compare_rows(
                        EvaluatedArgument::Column(&left),
                        row,
                        row,
                        EvaluatedArgument::Column(&right),
                        row,
                        row,
                        &Control::default()
                    )
                    .unwrap(),
                expected
            );
        }
    }
}
const ORDERS: [ComparisonOperator; 4] = [
    ComparisonOperator::Lt,
    ComparisonOperator::Le,
    ComparisonOperator::Gt,
    ComparisonOperator::Ge,
];
fn order_expected(operator: ComparisonOperator, order: Ordering) -> bool {
    match operator {
        ComparisonOperator::Lt => matches!(order, Ordering::Less),
        ComparisonOperator::Le => matches!(order, Ordering::Less | Ordering::Equal),
        ComparisonOperator::Gt => matches!(order, Ordering::Greater),
        ComparisonOperator::Ge => matches!(order, Ordering::Greater | Ordering::Equal),
        _ => panic!("the independent order fixture uses four relational operators"),
    }
}
fn compare_ordered_arrays(left: &ArrayRef, right: &ArrayRef, expected: &[Option<Ordering>]) {
    for operator in ORDERS {
        let prepared = PreparedComparisonRecipe::try_new(
            operator,
            &ty(left.data_type().clone(), true),
            &ty(right.data_type().clone(), true),
            &CompileControl::default(),
        )
        .unwrap();
        assert_eq!(prepared.operator(), operator);
        let arrow = match operator {
            ComparisonOperator::Lt => arrow_ord::cmp::lt(&left.as_ref(), &right.as_ref()),
            ComparisonOperator::Le => arrow_ord::cmp::lt_eq(&left.as_ref(), &right.as_ref()),
            ComparisonOperator::Gt => arrow_ord::cmp::gt(&left.as_ref(), &right.as_ref()),
            ComparisonOperator::Ge => arrow_ord::cmp::gt_eq(&left.as_ref(), &right.as_ref()),
            _ => unreachable!(),
        }
        .unwrap();
        let expected: Vec<_> = expected
            .iter()
            .map(|value| value.map(|value| order_expected(operator, value)))
            .collect();
        assert_eq!(
            arrow.iter().collect::<Vec<_>>(),
            expected,
            "{operator:?} {:?}",
            left.data_type()
        );
        for (row, expected) in expected.iter().enumerate() {
            assert_eq!(
                prepared
                    .compare_rows(
                        EvaluatedArgument::Column(left),
                        row,
                        row,
                        EvaluatedArgument::Column(right),
                        row,
                        row,
                        &Control::default()
                    )
                    .unwrap(),
                *expected,
                "{operator:?} {:?} row {row}",
                left.data_type()
            );
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 8,
        max_array_nodes: 32,
        max_logical_elements: 4096,
        max_retained_buffer_bytes: 65536,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 8,
        max_metadata_bytes: 8192,
        max_library_validation_work: 65536,
        max_library_validation_bytes: 8192,
    }
}
fn constant(
    array: ArrayRef,
    nullable: bool,
    logical: ValueLogicalType,
    ordinal: u32,
) -> ConstantValue {
    let ty = FunctionValueType::try_with_logical_type(array.data_type().clone(), nullable, logical)
        .unwrap();
    let field = Arc::new(ty.try_to_field("equality-constant").unwrap());
    let pool = ConstantPool::try_new(
        field,
        ty,
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &CompileControl::default(),
    )
    .unwrap();
    pool.value(ordinal).unwrap()
}

#[test]
fn all_flat_primitive_decimal_temporal_interval_profiles_compute_equality() {
    macro_rules! p {
        ($t:ty, $a:expr, $b:expr) => {{
            let a: <$t as ArrowPrimitiveType>::Native = $a;
            let b: <$t as ArrowPrimitiveType>::Native = $b;
            let left: ArrayRef =
                Arc::new(PrimitiveArray::<$t>::from_iter([Some(a), Some(a), None]));
            let right: ArrayRef =
                Arc::new(PrimitiveArray::<$t>::from_iter([Some(a), Some(b), None]));
            compare_ordered_arrays(
                &left,
                &right,
                &[Some(Ordering::Equal), Some(Ordering::Less), None],
            );
            compare_arrays(left, right, &[Some(true), Some(false), None]);
        }};
    }
    p!(Int8Type, -3, 8);
    p!(Int16Type, -300, 800);
    p!(Int32Type, -3, 8);
    p!(Int64Type, i64::MIN, i64::MAX);
    p!(UInt8Type, 0, u8::MAX);
    p!(UInt16Type, 0, u16::MAX);
    p!(UInt32Type, 0, u32::MAX);
    p!(UInt64Type, 0, u64::MAX);
    p!(
        Float16Type,
        <Float16Type as ArrowPrimitiveType>::Native::from_f32(2.0),
        <Float16Type as ArrowPrimitiveType>::Native::from_f32(3.0)
    );
    p!(Float32Type, 2.0, 3.0);
    p!(Float64Type, 2.0, 3.0);
    p!(Decimal32Type, -123, 456);
    p!(Decimal64Type, -123, 456);
    p!(Decimal128Type, -123, 456);
    p!(
        Decimal256Type,
        <Decimal256Type as ArrowPrimitiveType>::Native::from_i128(-123),
        <Decimal256Type as ArrowPrimitiveType>::Native::from_i128(456)
    );
    p!(Date32Type, -1, 1);
    p!(Date64Type, -1000, 1000);
    p!(Time32SecondType, 1, 2);
    p!(Time32MillisecondType, 1000, 2000);
    p!(Time64MicrosecondType, 1000, 2000);
    p!(Time64NanosecondType, 1000, 2000);
    p!(TimestampSecondType, -1, 1);
    p!(TimestampMillisecondType, -1, 1);
    p!(TimestampMicrosecondType, -1, 1);
    p!(TimestampNanosecondType, -1, 1);
    p!(DurationSecondType, -1, 1);
    p!(DurationMillisecondType, -1, 1);
    p!(DurationMicrosecondType, -1, 1);
    p!(DurationNanosecondType, -1, 1);
    p!(IntervalYearMonthType, -1, 1);
    p!(
        IntervalDayTimeType,
        <IntervalDayTimeType as ArrowPrimitiveType>::Native::new(1, 2),
        <IntervalDayTimeType as ArrowPrimitiveType>::Native::new(1, 3)
    );
    p!(
        IntervalMonthDayNanoType,
        <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native::new(1, 2, 3),
        <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native::new(1, 2, 4)
    );
    compare_arrays(
        Arc::new(BooleanArray::from(vec![Some(true), Some(true), None])),
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
        &[Some(true), Some(false), None],
    );
    compare_arrays(
        Arc::new(NullArray::new(2)),
        Arc::new(NullArray::new(2)),
        &[None, None],
    );
    let left: ArrayRef = Arc::new(
        Decimal128Array::from(vec![123, 456])
            .with_precision_and_scale(12, -2)
            .unwrap(),
    );
    let right: ArrayRef = Arc::new(
        Decimal128Array::from(vec![123, 789])
            .with_precision_and_scale(12, -2)
            .unwrap(),
    );
    compare_ordered_arrays(
        &left,
        &right,
        &[Some(Ordering::Equal), Some(Ordering::Less)],
    );
    let bool_left: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), Some(true), None]));
    let bool_right: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]));
    compare_ordered_arrays(
        &bool_left,
        &bool_right,
        &[Some(Ordering::Equal), Some(Ordering::Greater), None],
    );
    let null: ArrayRef = Arc::new(NullArray::new(2));
    compare_ordered_arrays(&null, &null, &[None, None]);
    let a: ArrayRef = Arc::new(StringArray::from(vec!["z", "a"]));
    let b: ArrayRef = Arc::new(StringArray::from(vec!["aa", "aa"]));
    compare_ordered_arrays(&a, &b, &[Some(Ordering::Greater), Some(Ordering::Less)]);
    let a: ArrayRef = Arc::new(LargeStringArray::from(vec!["z", "a"]));
    let b: ArrayRef = Arc::new(LargeStringArray::from(vec!["aa", "aa"]));
    compare_ordered_arrays(&a, &b, &[Some(Ordering::Greater), Some(Ordering::Less)]);
    let a: ArrayRef = Arc::new(BinaryArray::from(vec![&b"z"[..], &b"a"[..]]));
    let b: ArrayRef = Arc::new(BinaryArray::from(vec![&b"aa"[..], &b"aa"[..]]));
    compare_ordered_arrays(&a, &b, &[Some(Ordering::Greater), Some(Ordering::Less)]);
    let a: ArrayRef = Arc::new(LargeBinaryArray::from(vec![&b"z"[..], &b"a"[..]]));
    let b: ArrayRef = Arc::new(LargeBinaryArray::from(vec![&b"aa"[..], &b"aa"[..]]));
    compare_ordered_arrays(&a, &b, &[Some(Ordering::Greater), Some(Ordering::Less)]);
    let a: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_iter([&[0xff, 0][..], &[0, 0][..]].into_iter()).unwrap(),
    );
    let b: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_iter([&[0x80, 0][..], &[0, 1][..]].into_iter()).unwrap(),
    );
    compare_ordered_arrays(&a, &b, &[Some(Ordering::Greater), Some(Ordering::Less)]);
    compare_arrays(left, right, &[Some(true), Some(false)]);
}

#[test]
fn floating_equality_matches_real_arrow_bits_including_signed_zero_and_nan_payloads() {
    let nan = f64::from_bits(0x7ff8_0000_0000_0042);
    let other = f64::from_bits(0x7ff8_0000_0000_0043);
    let left: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(0.0),
        Some(-0.0),
        Some(nan),
        Some(nan),
        Some(nan),
        Some(f64::INFINITY),
        None,
    ]));
    let right: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(-0.0),
        Some(-0.0),
        Some(nan),
        Some(other),
        Some(1.0),
        Some(f64::INFINITY),
        Some(0.0),
    ]));
    let expected = [
        Some(false),
        Some(true),
        Some(true),
        Some(false),
        Some(false),
        Some(true),
        None,
    ];
    let arrow = arrow_ord::cmp::eq(&left.as_ref(), &right.as_ref()).unwrap();
    assert_eq!(arrow.iter().collect::<Vec<_>>(), expected);
    compare_arrays(left, right, &expected);
    let nan = f32::from_bits(0x7fc0_0042);
    let other = f32::from_bits(0x7fc0_0043);
    let left: ArrayRef = Arc::new(Float32Array::from(vec![0.0, nan, nan, nan]));
    let right: ArrayRef = Arc::new(Float32Array::from(vec![-0.0, nan, other, 1.0]));
    let expected = [Some(false), Some(true), Some(false), Some(false)];
    let arrow = arrow_ord::cmp::eq(&left.as_ref(), &right.as_ref()).unwrap();
    assert_eq!(arrow.iter().collect::<Vec<_>>(), expected);
    compare_arrays(left, right, &expected);
}

#[test]
fn byte_profiles_and_nonzero_constant_compact_slice_addresses_are_independent() {
    let left: ArrayRef = Arc::new(StringArray::from(vec![
        Some("prefix"),
        Some("prefix"),
        None,
    ]));
    let right: ArrayRef = Arc::new(StringArray::from(vec![
        Some("prefix"),
        Some("prefiy"),
        Some("hidden"),
    ]));
    compare_arrays(left, right, &[Some(true), Some(false), None]);
    compare_arrays(
        Arc::new(LargeStringArray::from(vec!["尾", "尾"])),
        Arc::new(LargeStringArray::from(vec!["尾", "头"])),
        &[Some(true), Some(false)],
    );
    compare_arrays(
        Arc::new(BinaryArray::from(vec![&[0xff, 0][..], &[0xff, 0][..]])),
        Arc::new(BinaryArray::from(vec![&[0xff, 0][..], &[0x80, 0][..]])),
        &[Some(true), Some(false)],
    );
    compare_arrays(
        Arc::new(LargeBinaryArray::from(vec![&[0xff][..], &[0xff][..]])),
        Arc::new(LargeBinaryArray::from(vec![&[0xff][..], &[0x80][..]])),
        &[Some(true), Some(false)],
    );
    compare_arrays(
        Arc::new(
            FixedSizeBinaryArray::try_from_iter([&[0xff, 0][..], &[0xff, 0][..]].into_iter())
                .unwrap(),
        ),
        Arc::new(
            FixedSizeBinaryArray::try_from_iter([&[0xff, 0][..], &[0x80, 0][..]].into_iter())
                .unwrap(),
        ),
        &[Some(true), Some(false)],
    );
    let bytes = [0x80u8; 16];
    let different = [0x7fu8; 16];
    let left: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            [Some(&bytes[..]), Some(&bytes[..]), None].into_iter(),
            16,
        )
        .unwrap(),
    );
    let right: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_iter(
            [&bytes[..], &different[..], &different[..]].into_iter(),
        )
        .unwrap(),
    );
    for logical in [ValueLogicalType::LargeInt, ValueLogicalType::Uuid] {
        let source =
            FunctionValueType::try_with_logical_type(DataType::FixedSizeBinary(16), true, logical)
                .unwrap();
        let target =
            FunctionValueType::try_with_logical_type(DataType::FixedSizeBinary(16), false, logical)
                .unwrap();
        let prepared = PreparedComparisonRecipe::try_new(
            ComparisonOperator::Eq,
            &source,
            &target,
            &CompileControl::default(),
        )
        .unwrap();
        for (row, expected) in [Some(true), Some(false), None].into_iter().enumerate() {
            assert_eq!(
                prepared
                    .compare_rows(
                        EvaluatedArgument::Column(&left),
                        row,
                        row,
                        EvaluatedArgument::Column(&right),
                        row,
                        row,
                        &Control::default()
                    )
                    .unwrap(),
                expected
            );
        }
        assert_eq!(
            PreparedComparisonRecipe::try_new(
                ComparisonOperator::Eq,
                &source,
                &ty(DataType::FixedSizeBinary(16), true),
                &CompileControl::default()
            ),
            Err(ComparisonPrepareError::TypeMismatch)
        );
    }
    let source: ArrayRef = Arc::new(StringArray::from(vec![
        None,
        Some("ignored"),
        Some("needle"),
    ]));
    let value = constant(source, true, ValueLogicalType::Physical, 2);
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let array: ArrayRef = Arc::new(StringArray::from(vec![
        "outside", "needle", "other", "outside",
    ]));
    let array = array.slice(1, 2);
    let selected =
        SelectedValues::try_new(selection, &DataType::Utf8, array, Box::new([])).unwrap();
    let scalar: ArrayRef = Arc::new(StringArray::from(vec!["needle"]));
    let full: ArrayRef = Arc::new(StringArray::from(vec![
        "unused", "needle", "unused", "other",
    ]));
    let prepared = PreparedComparisonRecipe::try_new(
        ComparisonOperator::Eq,
        &ty(DataType::Utf8, true),
        &ty(DataType::Utf8, false),
        &CompileControl::default(),
    )
    .unwrap();
    assert_eq!(
        prepared
            .compare_rows(
                EvaluatedArgument::Constant(&value),
                0,
                1,
                EvaluatedArgument::SelectedColumn(&selected),
                0,
                1,
                &Control::default()
            )
            .unwrap(),
        Some(true)
    );
    assert_eq!(
        prepared
            .compare_rows(
                EvaluatedArgument::Constant(&value),
                1,
                3,
                EvaluatedArgument::SelectedColumn(&selected),
                1,
                3,
                &Control::default()
            )
            .unwrap(),
        Some(false)
    );
    assert_eq!(
        prepared
            .compare_rows(
                EvaluatedArgument::Scalar(&scalar),
                123,
                3,
                EvaluatedArgument::Column(&full),
                1,
                3,
                &Control::default()
            )
            .unwrap(),
        Some(false)
    );
}

#[test]
fn unsupported_domains_and_bad_selected_addresses_refuse_before_null_short_circuit() {
    let physical = ty(DataType::Utf8, true);
    for logical in [ValueLogicalType::Json, ValueLogicalType::Variant] {
        let data_type = if logical == ValueLogicalType::Json {
            DataType::Utf8
        } else {
            DataType::LargeBinary
        };
        let value = FunctionValueType::try_with_logical_type(data_type, true, logical).unwrap();
        assert_eq!(
            PreparedComparisonRecipe::try_new(
                ComparisonOperator::Eq,
                &value,
                &value,
                &CompileControl::default()
            ),
            Err(ComparisonPrepareError::Unsupported)
        );
    }
    for data_type in [
        DataType::Utf8View,
        DataType::BinaryView,
        DataType::List(Arc::new(arrow_schema::Field::new(
            "item",
            DataType::Utf8,
            true,
        ))),
    ] {
        let value = ty(data_type, true);
        assert_eq!(
            PreparedComparisonRecipe::try_new(
                ComparisonOperator::Eq,
                &value,
                &value,
                &CompileControl::default()
            ),
            Err(ComparisonPrepareError::Unsupported)
        );
    }
    // Encoding is not value identity. The semantic recipe is reusable after
    // a concrete BE consumer has explicitly normalized its actual carrier.
    let encoded_projection = ty(
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        true,
    );
    assert!(
        PreparedComparisonRecipe::try_new(
            ComparisonOperator::Eq,
            &encoded_projection,
            &encoded_projection,
            &CompileControl::default()
        )
        .is_ok()
    );
    assert_eq!(
        PreparedComparisonRecipe::try_new(
            ComparisonOperator::Eq,
            &physical,
            &ty(DataType::LargeUtf8, true),
            &CompileControl::default()
        ),
        Err(ComparisonPrepareError::TypeMismatch)
    );
    let prepared = PreparedComparisonRecipe::try_new(
        ComparisonOperator::Eq,
        &physical,
        &physical,
        &CompileControl::default(),
    )
    .unwrap();
    let null: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>]));
    let wrong: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    assert!(matches!(
        prepared.compare_rows(
            EvaluatedArgument::Scalar(&null),
            0,
            0,
            EvaluatedArgument::Column(&wrong),
            0,
            0,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let correct: ArrayRef = Arc::new(StringArray::from(vec!["x"]));
    assert!(matches!(
        prepared.compare_rows(
            EvaluatedArgument::Scalar(&null),
            0,
            0,
            EvaluatedArgument::Column(&correct),
            0,
            1,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let many: ArrayRef = Arc::new(StringArray::from(vec!["x", "y"]));
    assert!(matches!(
        prepared.compare_rows(
            EvaluatedArgument::Scalar(&many),
            0,
            0,
            EvaluatedArgument::Scalar(&correct),
            0,
            0,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let selected = SelectedValues::try_new(
        Selection::all(1),
        &DataType::Utf8,
        correct.clone(),
        Box::new([]),
    )
    .unwrap();
    assert!(matches!(
        prepared.compare_rows(
            EvaluatedArgument::Scalar(&correct),
            0,
            0,
            EvaluatedArgument::SelectedColumn(&selected),
            0,
            1,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let json = constant(correct.clone(), false, ValueLogicalType::Json, 0);
    assert!(matches!(
        prepared.compare_rows(
            EvaluatedArgument::Constant(&json),
            0,
            0,
            EvaluatedArgument::Scalar(&correct),
            0,
            0,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let strict = PreparedComparisonRecipe::try_new(
        ComparisonOperator::Eq,
        &ty(DataType::Utf8, false),
        &physical,
        &CompileControl::default(),
    )
    .unwrap();
    assert!(matches!(
        strict.compare_rows(
            EvaluatedArgument::Scalar(&null),
            0,
            0,
            EvaluatedArgument::Scalar(&correct),
            0,
            0,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let value = constant(correct.clone(), true, ValueLogicalType::Physical, 0);
    assert!(matches!(
        strict.compare_rows(
            EvaluatedArgument::Constant(&value),
            0,
            0,
            EvaluatedArgument::Scalar(&correct),
            0,
            0,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn long_common_prefix_checks_entry_every_quantum_tail_and_keeps_first_refusal() {
    let text = "x".repeat(1024);
    let left: ArrayRef = Arc::new(StringArray::from(vec![text.as_str()]));
    let right: ArrayRef = Arc::new(StringArray::from(vec![text.as_str()]));
    let prepared = recipe(&left, &right);
    let success = Control::default();
    assert_eq!(
        prepared
            .compare_rows(
                EvaluatedArgument::Scalar(&left),
                0,
                0,
                EvaluatedArgument::Scalar(&right),
                0,
                0,
                &success
            )
            .unwrap(),
        Some(true)
    );
    let trace = success.trace.into_inner().unwrap();
    assert_eq!(trace[0], 0);
    assert!(trace.iter().filter(|units| **units == 256).count() >= 4);
    assert!(*trace.last().unwrap() < 256);
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        for refused in 0..trace.len() {
            let control = Control {
                refusal: Some((refused, cause.clone())),
                ..Control::default()
            };
            let result = prepared.compare_rows(
                EvaluatedArgument::Scalar(&left),
                0,
                0,
                EvaluatedArgument::Scalar(&right),
                0,
                0,
                &control,
            );
            assert_eq!(result, Err(cause.clone()));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=refused]);
        }
    }
    // An ordinary bad-address failure still observes its pending tail.
    let success = Control::default();
    assert!(
        prepared
            .compare_rows(
                EvaluatedArgument::Column(&left),
                0,
                2,
                EvaluatedArgument::Scalar(&right),
                0,
                0,
                &success
            )
            .is_err()
    );
    let failed = Control {
        refusal: Some((1, KernelFailure::DeadlineExceeded)),
        ..Control::default()
    };
    assert_eq!(
        prepared.compare_rows(
            EvaluatedArgument::Column(&left),
            0,
            2,
            EvaluatedArgument::Scalar(&right),
            0,
            0,
            &failed
        ),
        Err(KernelFailure::DeadlineExceeded)
    );
    assert_eq!(failed.trace.lock().unwrap().len(), 2);
}

#[test]
fn compile_boundaries_preserve_original_causes_and_full_temporal_identity() {
    let zone: Arc<str> = "A".repeat(640).into();
    let source = ty(
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone)),
        false,
    );
    let expected = ty(source.data_type().clone(), true);
    let success = CompileControl::default();
    let prepared =
        PreparedComparisonRecipe::try_new(ComparisonOperator::Eq, &source, &expected, &success)
            .unwrap();
    assert_eq!(prepared.left_type(), &source);
    assert_eq!(prepared.right_type(), &expected);
    assert!(prepared.nullable_result());
    let trace = success.trace.into_inner().unwrap();
    assert_eq!(trace[0], 0);
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for refused in 0..trace.len() {
            let control = CompileControl {
                refusal: Some((refused, cause.clone())),
                ..CompileControl::default()
            };
            let error = PreparedComparisonRecipe::try_new(
                ComparisonOperator::Eq,
                &source,
                &expected,
                &control,
            )
            .unwrap_err();
            assert_eq!(error.control_error(), Some(cause.clone()));
            assert!(error.source().is_some());
            assert_eq!(*control.trace.lock().unwrap(), trace[..=refused]);
        }
    }
    // The actual admitted source walker, before unsupported capability refusal,
    // supplies a real multi-quantum preparation path without inventing work.
    let fields: Vec<_> = (0..320)
        .map(|i| arrow_schema::Field::new(format!("field-{i}"), DataType::Int64, true))
        .collect();
    let wide = ty(DataType::Struct(fields.into()), true);
    let success = CompileControl::default();
    assert_eq!(
        PreparedComparisonRecipe::try_new(ComparisonOperator::Eq, &wide, &wide, &success),
        Err(ComparisonPrepareError::Unsupported)
    );
    let trace = success.trace.into_inner().unwrap();
    assert!(trace.contains(&256));
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for refused in 0..trace.len() {
            let control = CompileControl {
                refusal: Some((refused, cause.clone())),
                ..CompileControl::default()
            };
            let error =
                PreparedComparisonRecipe::try_new(ComparisonOperator::Eq, &wide, &wide, &control)
                    .unwrap_err();
            assert_eq!(error.control_error(), Some(cause.clone()));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=refused]);
        }
    }
    for target in [
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Millisecond, Some("A".repeat(640).into())),
        DataType::Timestamp(TimeUnit::Microsecond, Some("B".repeat(640).into())),
    ] {
        assert!(matches!(
            PreparedComparisonRecipe::try_new(
                ComparisonOperator::Eq,
                &source,
                &ty(target, false),
                &CompileControl::default()
            ),
            Err(ComparisonPrepareError::TypeMismatch)
        ));
    }
}

#[test]
fn nullable_result_and_source_constant_covariance_do_not_capture_control() {
    let source = ty(DataType::Int64, false);
    let control = Arc::new(CompileControl::default());
    let weak = Arc::downgrade(&control);
    let prepared = PreparedComparisonRecipe::try_new(
        ComparisonOperator::Eq,
        &source,
        &source,
        control.as_ref(),
    )
    .unwrap();
    drop(control);
    assert!(weak.upgrade().is_none());
    assert!(!prepared.nullable_result());
    let source = ty(DataType::Int64, true);
    let prepared = PreparedComparisonRecipe::try_new(
        ComparisonOperator::Eq,
        &source,
        &source,
        &CompileControl::default(),
    )
    .unwrap();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![42, 43]));
    let value = constant(array, false, ValueLogicalType::Physical, 1);
    let scalar: ArrayRef = Arc::new(Int64Array::from(vec![43]));
    assert!(prepared.nullable_result());
    assert_eq!(
        prepared
            .compare_rows(
                EvaluatedArgument::Constant(&value),
                usize::MAX,
                usize::MAX,
                EvaluatedArgument::Scalar(&scalar),
                0,
                0,
                &Control::default()
            )
            .unwrap(),
        Some(true)
    );
    let physical_null = ty(DataType::Null, true);
    assert!(
        PreparedComparisonRecipe::try_new(
            ComparisonOperator::Eq,
            &physical_null,
            &physical_null,
            &CompileControl::default()
        )
        .unwrap()
        .nullable_result()
    );
}

#[test]
fn null_carrier_is_successful_sql_null_even_with_nonnullable_source_fact() {
    let source = ty(DataType::Null, false);
    source.validate().unwrap();
    let array: ArrayRef = Arc::new(NullArray::new(1));
    for operator in [
        ComparisonOperator::Eq,
        ComparisonOperator::Ne,
        ComparisonOperator::Lt,
        ComparisonOperator::Le,
        ComparisonOperator::Gt,
        ComparisonOperator::Ge,
    ] {
        let prepared = PreparedComparisonRecipe::try_new(
            operator,
            &source,
            &source,
            &CompileControl::default(),
        )
        .unwrap();
        assert!(prepared.nullable_result());
        assert_eq!(
            prepared
                .compare_rows(
                    EvaluatedArgument::Column(&array),
                    0,
                    0,
                    EvaluatedArgument::Column(&array),
                    0,
                    0,
                    &Control::default()
                )
                .unwrap(),
            None
        );
        let scalar: ArrayRef = Arc::new(Int64Array::from(vec![None::<i64>]));
        let nonnull = ty(DataType::Int64, false);
        let strict = PreparedComparisonRecipe::try_new(
            operator,
            &nonnull,
            &nonnull,
            &CompileControl::default(),
        )
        .unwrap();
        assert!(matches!(
            strict.compare_rows(
                EvaluatedArgument::Scalar(&scalar),
                0,
                0,
                EvaluatedArgument::Scalar(&scalar),
                0,
                0,
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
}

#[test]
fn four_float_operators_keep_total_order_bit_oracles_for_float16_32_64() {
    macro_rules! float {
        ($ty:ty, $zero:expr, $negative_zero:expr, $nan:expr, $other:expr, $negative_nan:expr, $negative_other:expr, $inf:expr, $negative_inf:expr) => {{
            let rows = [
                (Some($zero), Some($negative_zero), Some(Ordering::Greater)),
                (Some($negative_zero), Some($zero), Some(Ordering::Less)),
                (Some($nan), Some($nan), Some(Ordering::Equal)),
                (Some($nan), Some($other), Some(Ordering::Less)),
                (Some($other), Some($nan), Some(Ordering::Greater)),
                (
                    Some($negative_nan),
                    Some($negative_other),
                    Some(Ordering::Greater),
                ),
                (
                    Some($negative_other),
                    Some($negative_nan),
                    Some(Ordering::Less),
                ),
                (Some($nan), Some($inf), Some(Ordering::Greater)),
                (
                    Some($negative_nan),
                    Some($negative_inf),
                    Some(Ordering::Less),
                ),
                (Some($inf), Some($inf), Some(Ordering::Equal)),
                (None, Some($nan), None),
            ];
            let left: ArrayRef = Arc::new(PrimitiveArray::<$ty>::from_iter(
                rows.iter()
                    .map(|(a, _, _)| a.map(<$ty as ArrowPrimitiveType>::Native::from_bits)),
            ));
            let right: ArrayRef = Arc::new(PrimitiveArray::<$ty>::from_iter(
                rows.iter()
                    .map(|(_, a, _)| a.map(<$ty as ArrowPrimitiveType>::Native::from_bits)),
            ));
            let expected: Vec<_> = rows.iter().map(|(_, _, order)| *order).collect();
            compare_ordered_arrays(&left, &right, &expected);
        }};
    }
    float!(
        Float16Type,
        0x0000,
        0x8000,
        0x7e42,
        0x7e43,
        0xfe42,
        0xfe43,
        0x7c00,
        0xfc00
    );
    float!(
        Float32Type,
        0x0000_0000,
        0x8000_0000,
        0x7fc0_0042,
        0x7fc0_0043,
        0xffc0_0042,
        0xffc0_0043,
        0x7f80_0000,
        0xff80_0000
    );
    float!(
        Float64Type,
        0x0000_0000_0000_0000,
        0x8000_0000_0000_0000,
        0x7ff8_0000_0000_0042,
        0x7ff8_0000_0000_0043,
        0xfff8_0000_0000_0042,
        0xfff8_0000_0000_0043,
        0x7ff0_0000_0000_0000,
        0xfff0_0000_0000_0000
    );
}

#[test]
fn offset_bytes_use_common_prefix_before_length_for_all_four_operators() {
    let left = [
        Some("z"),
        Some("a"),
        Some("aa"),
        Some("a\0"),
        Some("a\0b"),
        Some("α"),
        Some("ab"),
        None,
    ];
    let right = [
        Some("aa"),
        Some("aa"),
        Some("a"),
        Some("a\0"),
        Some("a\0a"),
        Some("β"),
        Some("ac"),
        Some("a"),
    ];
    let expected = [
        Some(Ordering::Greater),
        Some(Ordering::Less),
        Some(Ordering::Greater),
        Some(Ordering::Equal),
        Some(Ordering::Greater),
        Some(Ordering::Less),
        Some(Ordering::Less),
        None,
    ];
    compare_ordered_arrays(
        &(Arc::new(StringArray::from(left.to_vec())) as ArrayRef),
        &(Arc::new(StringArray::from(right.to_vec())) as ArrayRef),
        &expected,
    );
    compare_ordered_arrays(
        &(Arc::new(LargeStringArray::from(left.to_vec())) as ArrayRef),
        &(Arc::new(LargeStringArray::from(right.to_vec())) as ArrayRef),
        &expected,
    );
    let left: Vec<_> = left
        .into_iter()
        .map(|value| value.map(str::as_bytes))
        .collect();
    let right: Vec<_> = right
        .into_iter()
        .map(|value| value.map(str::as_bytes))
        .collect();
    compare_ordered_arrays(
        &(Arc::new(BinaryArray::from(left.clone())) as ArrayRef),
        &(Arc::new(BinaryArray::from(right.clone())) as ArrayRef),
        &expected,
    );
    compare_ordered_arrays(
        &(Arc::new(LargeBinaryArray::from(left)) as ArrayRef),
        &(Arc::new(LargeBinaryArray::from(right)) as ArrayRef),
        &expected,
    );
}

#[test]
fn accurate_largeint_uses_signed_order_while_physical_and_uuid_keep_byte_order() {
    let values = [i128::MIN, -1, 0, 1, i128::MAX];
    let bytes = values.map(i128::to_be_bytes);
    let left_bytes: Vec<_> = (0..5)
        .flat_map(|i| (0..5).map(move |_| Some(bytes[i])))
        .chain([None])
        .collect();
    let right_bytes: Vec<_> = (0..5)
        .flat_map(|_| (0..5).map(move |j| Some(bytes[j])))
        .chain([Some(bytes[0])])
        .collect();
    let left: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            left_bytes.iter().map(|v| v.as_ref().map(|v| v.as_slice())),
            16,
        )
        .unwrap(),
    );
    let right: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            right_bytes.iter().map(|v| v.as_ref().map(|v| v.as_slice())),
            16,
        )
        .unwrap(),
    );
    let unsigned_rank = [3, 4, 0, 1, 2];
    for logical in [
        ValueLogicalType::LargeInt,
        ValueLogicalType::Physical,
        ValueLogicalType::Uuid,
    ] {
        let value =
            FunctionValueType::try_with_logical_type(DataType::FixedSizeBinary(16), true, logical)
                .unwrap();
        for operator in [
            ComparisonOperator::Eq,
            ComparisonOperator::Ne,
            ComparisonOperator::Lt,
            ComparisonOperator::Le,
            ComparisonOperator::Gt,
            ComparisonOperator::Ge,
        ] {
            let prepared = PreparedComparisonRecipe::try_new(
                operator,
                &value,
                &value,
                &CompileControl::default(),
            )
            .unwrap();
            for i in 0..5 {
                for j in 0..5 {
                    let ordering = if logical == ValueLogicalType::LargeInt {
                        i.cmp(&j)
                    } else {
                        unsigned_rank[i].cmp(&unsigned_rank[j])
                    };
                    let expected = match operator {
                        ComparisonOperator::Eq => i == j,
                        ComparisonOperator::Ne => i != j,
                        operator => order_expected(operator, ordering),
                    };
                    let row = i * 5 + j;
                    assert_eq!(
                        prepared
                            .compare_rows(
                                EvaluatedArgument::Column(&left),
                                row,
                                row,
                                EvaluatedArgument::Column(&right),
                                row,
                                row,
                                &Control::default()
                            )
                            .unwrap(),
                        Some(expected),
                        "{logical:?} {operator:?} {} vs {}",
                        values[i],
                        values[j]
                    );
                }
            }
            assert_eq!(
                prepared
                    .compare_rows(
                        EvaluatedArgument::Column(&left),
                        25,
                        25,
                        EvaluatedArgument::Column(&right),
                        25,
                        25,
                        &Control::default()
                    )
                    .unwrap(),
                None
            );
            let different = FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                if logical == ValueLogicalType::Uuid {
                    ValueLogicalType::LargeInt
                } else {
                    ValueLogicalType::Uuid
                },
            )
            .unwrap();
            assert_eq!(
                PreparedComparisonRecipe::try_new(
                    operator,
                    &value,
                    &different,
                    &CompileControl::default()
                ),
                Err(ComparisonPrepareError::TypeMismatch)
            );
        }
    }
}

#[test]
fn every_ordering_operator_preserves_exact_addresses_and_long_prefix_primary_controls() {
    let source: ArrayRef = Arc::new(StringArray::from(vec![
        None,
        Some("unused"),
        Some("needle"),
    ]));
    let value = constant(source, true, ValueLogicalType::Physical, 2);
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let selected_array: ArrayRef = Arc::new(StringArray::from(vec![
        "ignored", "needle", "other", "ignored",
    ]));
    let selected_array = selected_array.slice(1, 2);
    let selected =
        SelectedValues::try_new(selection, &DataType::Utf8, selected_array, Box::new([])).unwrap();
    let scalar: ArrayRef = Arc::new(StringArray::from(vec!["needle"]));
    let full: ArrayRef = Arc::new(StringArray::from(vec![
        "unused", "needle", "unused", "other",
    ]));
    let text = "x".repeat(1024);
    let later = format!("{}y", "x".repeat(1023));
    let left: ArrayRef = Arc::new(StringArray::from(vec![text.as_str()]));
    let right: ArrayRef = Arc::new(StringArray::from(vec![later.as_str()]));
    for operator in ORDERS {
        let prepared = PreparedComparisonRecipe::try_new(
            operator,
            &ty(DataType::Utf8, true),
            &ty(DataType::Utf8, false),
            &CompileControl::default(),
        )
        .unwrap();
        for (ordinal, batch_row, order) in [(0, 1, Ordering::Equal), (1, 3, Ordering::Less)] {
            assert_eq!(
                prepared
                    .compare_rows(
                        EvaluatedArgument::Constant(&value),
                        ordinal,
                        batch_row,
                        EvaluatedArgument::SelectedColumn(&selected),
                        ordinal,
                        batch_row,
                        &Control::default()
                    )
                    .unwrap(),
                Some(order_expected(operator, order))
            );
        }
        assert_eq!(
            prepared
                .compare_rows(
                    EvaluatedArgument::Scalar(&scalar),
                    9,
                    3,
                    EvaluatedArgument::Column(&full),
                    1,
                    3,
                    &Control::default()
                )
                .unwrap(),
            Some(order_expected(operator, Ordering::Less))
        );
        assert!(matches!(
            prepared.compare_rows(
                EvaluatedArgument::Scalar(&scalar),
                0,
                0,
                EvaluatedArgument::SelectedColumn(&selected),
                0,
                0,
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
        let success = Control::default();
        assert_eq!(
            prepared
                .compare_rows(
                    EvaluatedArgument::Scalar(&left),
                    0,
                    0,
                    EvaluatedArgument::Scalar(&right),
                    0,
                    0,
                    &success
                )
                .unwrap(),
            Some(order_expected(operator, Ordering::Less))
        );
        let trace = success.trace.into_inner().unwrap();
        assert_eq!(trace[0], 0);
        assert!(trace.iter().filter(|units| **units == 256).count() >= 4);
        for cause in [
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
        ] {
            for refused in 0..trace.len() {
                let control = Control {
                    refusal: Some((refused, cause.clone())),
                    ..Control::default()
                };
                assert_eq!(
                    prepared.compare_rows(
                        EvaluatedArgument::Scalar(&left),
                        0,
                        0,
                        EvaluatedArgument::Scalar(&right),
                        0,
                        0,
                        &control
                    ),
                    Err(cause.clone())
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=refused]);
            }
        }
    }
}
