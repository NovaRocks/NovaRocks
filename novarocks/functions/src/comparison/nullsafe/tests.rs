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
use crate::{ConstantPolicy, ConstantPool, RowDataError, SelectedValues, Selection};
use arrow_schema::Field;
use novarocks_type_contract::{EvaluationDemand, EvaluationDomainId, ExpressionUseId};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
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
        panic!("null-safe equality must not wait")
    }
}
fn ty(carrier: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(carrier, nullable)
}
fn recipe(left: &ArrayRef, right: &ArrayRef) -> PreparedNullSafeComparisonRecipe {
    PreparedNullSafeComparisonRecipe::try_new(
        &ty(left.data_type().clone(), true),
        &ty(right.data_type().clone(), true),
        &CompileControl::default(),
    )
    .unwrap()
}
fn row(
    recipe: &PreparedNullSafeComparisonRecipe,
    left: &ArrayRef,
    right: &ArrayRef,
    row: usize,
) -> bool {
    recipe
        .compare_rows(
            EvaluatedArgument::Column(left),
            row,
            row,
            EvaluatedArgument::Column(right),
            row,
            row,
            &Control::default(),
        )
        .unwrap()
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(91),
        domain: EvaluationDomainId::new(21),
        demand: EvaluationDemand::Value,
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original callback invalid program"),
        internal("original callback internal failure"),
        KernelFailure::Operational(crate::KernelDiagnostic::new("original callback operation")),
        KernelFailure::InstanceFailed,
    ]
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 8,
        max_array_nodes: 8,
        max_logical_elements: 64,
        max_retained_buffer_bytes: 4096,
        max_type_depth: 8,
        max_type_nodes: 64,
        max_dictionary_depth: 4,
        max_metadata_bytes: 1024,
        max_library_validation_work: 4096,
        max_library_validation_bytes: 8192,
    }
}

#[test]
fn all_fourteen_legacy_nonnull_profiles_and_null_have_total_independent_truth_tables() {
    let mut profiles = Vec::<(ArrayRef, ArrayRef)>::new();
    macro_rules! p {
        ($ty:ty,$a:expr,$b:expr) => {{
            let left: ArrayRef = Arc::new(PrimitiveArray::<$ty>::from(vec![
                None,
                None,
                Some($a),
                Some($a),
                Some($a),
            ]));
            let right: ArrayRef = Arc::new(PrimitiveArray::<$ty>::from(vec![
                None,
                Some($a),
                None,
                Some($a),
                Some($b),
            ]));
            profiles.push((left, right));
        }};
    }
    profiles.push((
        Arc::new(BooleanArray::from(vec![
            None,
            None,
            Some(true),
            Some(true),
            Some(true),
        ])),
        Arc::new(BooleanArray::from(vec![
            None,
            Some(true),
            None,
            Some(true),
            Some(false),
        ])),
    ));
    p!(Int8Type, -7, 9);
    p!(Int16Type, -7, 9);
    p!(Int32Type, -7, 9);
    p!(Int64Type, -7, 9);
    p!(Float32Type, -7.0, 9.0);
    p!(Float64Type, -7.0, 9.0);
    p!(Date32Type, -7, 9);
    p!(TimestampSecondType, -7, 9);
    p!(TimestampMillisecondType, -7, 9);
    p!(TimestampMicrosecondType, -7, 9);
    p!(TimestampNanosecondType, -7, 9);
    profiles.push((
        Arc::new(StringArray::from(vec![
            None,
            None,
            Some("é😀"),
            Some("é😀"),
            Some("é😀"),
        ])),
        Arc::new(StringArray::from(vec![
            None,
            Some("é😀"),
            None,
            Some("é😀"),
            Some("é😁"),
        ])),
    ));
    // Legacy null-safe comparison does not validate input coefficient precision.
    profiles.push((
        Arc::new(
            Decimal128Array::from(vec![None, None, Some(999_i128), Some(999), Some(999)])
                .with_precision_and_scale(1, 0)
                .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from(vec![None, Some(999_i128), None, Some(999), Some(998)])
                .with_precision_and_scale(1, 0)
                .unwrap(),
        ),
    ));
    assert_eq!(profiles.len(), 14);
    for (left, right) in profiles {
        let prepared = recipe(&left, &right);
        assert!(!prepared.nullable_result());
        assert_eq!(prepared.left_type(), &ty(left.data_type().clone(), true));
        assert_eq!(prepared.right_type(), &ty(right.data_type().clone(), true));
        assert_eq!(
            prepared.own_effects(context()).for_use(context()).unwrap(),
            ExpressionEffects::PURE_VALUE
        );
        let foreign = ExpressionEffectContext {
            domain: EvaluationDomainId::new(22),
            ..context()
        };
        assert!(prepared.own_effects(context()).for_use(foreign).is_err());
        assert_eq!(
            (0..5)
                .map(|r| row(&prepared, &left, &right, r))
                .collect::<Vec<_>>(),
            vec![true, false, false, true, false],
            "{:?}",
            left.data_type()
        );
    }
    let null: ArrayRef = Arc::new(NullArray::new(3));
    for nullable in [false, true] {
        let prepared = PreparedNullSafeComparisonRecipe::try_new(
            &ty(DataType::Null, nullable),
            &ty(DataType::Null, nullable),
            &CompileControl::default(),
        )
        .unwrap();
        for r in 0..3 {
            assert!(row(&prepared, &null, &null, r));
        }
    }
}

#[test]
fn ieee_partial_comparison_nan_and_signed_zero_remain_distinct_from_ordinary_bit_equality() {
    for narrow in [false, true] {
        let (left, right): (ArrayRef, ArrayRef) = if narrow {
            (
                Arc::new(Float32Array::from(vec![
                    Some(0.0),
                    Some(-0.0),
                    Some(f32::from_bits(0x7fc00001)),
                    Some(f32::from_bits(0xffc00002)),
                    Some(f32::NAN),
                    Some(f32::INFINITY),
                    Some(f32::INFINITY),
                    None,
                ])),
                Arc::new(Float32Array::from(vec![
                    Some(-0.0),
                    Some(0.0),
                    Some(3.0),
                    Some(f32::from_bits(0x7fc00003)),
                    Some(f32::INFINITY),
                    Some(f32::INFINITY),
                    Some(f32::NEG_INFINITY),
                    Some(f32::NAN),
                ])),
            )
        } else {
            (
                Arc::new(Float64Array::from(vec![
                    Some(0.0),
                    Some(-0.0),
                    Some(f64::from_bits(0x7ff8000000000001)),
                    Some(f64::from_bits(0xfff8000000000002)),
                    Some(f64::NAN),
                    Some(f64::INFINITY),
                    Some(f64::INFINITY),
                    None,
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(-0.0),
                    Some(0.0),
                    Some(3.0),
                    Some(f64::from_bits(0x7ff8000000000003)),
                    Some(f64::INFINITY),
                    Some(f64::INFINITY),
                    Some(f64::NEG_INFINITY),
                    Some(f64::NAN),
                ])),
            )
        };
        let prepared = recipe(&left, &right);
        assert_eq!(
            (0..8)
                .map(|r| row(&prepared, &left, &right, r))
                .collect::<Vec<_>>(),
            vec![true, true, true, true, true, true, false, false]
        );
        let ordinary = PreparedComparisonRecipe::try_new(
            ComparisonOperator::Eq,
            &ty(left.data_type().clone(), true),
            &ty(right.data_type().clone(), true),
            &CompileControl::default(),
        )
        .unwrap();
        for r in 0..5 {
            assert_eq!(
                ordinary
                    .compare_rows(
                        EvaluatedArgument::Column(&left),
                        r,
                        r,
                        EvaluatedArgument::Column(&right),
                        r,
                        r,
                        &Control::default()
                    )
                    .unwrap(),
                Some(false)
            );
            assert!(
                prepared
                    .compare_rows(
                        EvaluatedArgument::Column(&right),
                        r,
                        r,
                        EvaluatedArgument::Column(&left),
                        r,
                        r,
                        &Control::default()
                    )
                    .unwrap()
            );
        }
    }
}

#[test]
fn capability_is_static_exact_and_does_not_expand_from_null_only_data_or_nominal_carriers() {
    let target = ty(DataType::Int64, true);
    for carrier in [
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Decimal32(5, 0),
        DataType::Decimal64(10, 0),
        DataType::Decimal256(40, 0),
        DataType::Date64,
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Duration(TimeUnit::Second),
        DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::FixedSizeBinary(16),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    ] {
        let source = ty(carrier, true);
        assert_eq!(
            PreparedNullSafeComparisonRecipe::try_new(&source, &source, &CompileControl::default()),
            Err(ComparisonPrepareError::Unsupported)
        );
    }
    for source in [
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap(),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::LargeInt,
        )
        .unwrap(),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    ] {
        assert_eq!(
            PreparedNullSafeComparisonRecipe::try_new(&source, &source, &CompileControl::default()),
            Err(ComparisonPrepareError::Unsupported)
        );
        assert_eq!(
            PreparedNullSafeComparisonRecipe::try_new(&source, &target, &CompileControl::default()),
            Err(ComparisonPrepareError::TypeMismatch)
        );
    }
    for (left, right) in [
        (ty(DataType::Int64, true), ty(DataType::Int32, true)),
        (
            ty(DataType::Decimal128(10, 2), true),
            ty(DataType::Decimal128(10, 3), true),
        ),
        (
            ty(DataType::Timestamp(TimeUnit::Second, None), true),
            ty(DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ),
    ] {
        assert_eq!(
            PreparedNullSafeComparisonRecipe::try_new(&left, &right, &CompileControl::default()),
            Err(ComparisonPrepareError::TypeMismatch)
        );
    }
    let nonnull = ty(DataType::Int64, false);
    let prepared =
        PreparedNullSafeComparisonRecipe::try_new(&nonnull, &target, &CompileControl::default())
            .unwrap();
    assert_eq!(prepared.left_type(), &nonnull);
    assert_eq!(prepared.right_type(), &target);
    assert!(!prepared.nullable_result());
}

#[test]
fn constant_pool_ordinal_and_scalar_column_compact_slice_addresses_stay_independent() {
    let t = ty(DataType::Int64, true);
    let pool = ConstantPool::try_new(
        Arc::new(t.try_to_field("actual-pool").unwrap()),
        t.clone(),
        Int64Array::from(vec![Some(7), None, Some(21)]).to_data(),
        policy(),
        CompilePhase::Validate,
        &CompileControl::default(),
    )
    .unwrap();
    let constant = pool.value(2).unwrap();
    assert_eq!(constant.ordinal(), 2);
    let null = pool.value(1).unwrap();
    let dense: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(999),
        Some(21),
        Some(999),
        None,
        Some(7),
    ]));
    let sliced = dense.slice(1, 4);
    let rows = [0, 2, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Int64,
        Arc::new(Int64Array::from(vec![Some(21), None, Some(7)])),
        Box::default(),
    )
    .unwrap();
    let prepared =
        PreparedNullSafeComparisonRecipe::try_new(&t, &t, &CompileControl::default()).unwrap();
    let scalar: ArrayRef = Arc::new(Int64Array::from(vec![Some(21)]));
    for (ordinal, row) in selection.iter().enumerate() {
        assert!(
            prepared
                .compare_rows(
                    EvaluatedArgument::Column(&sliced),
                    ordinal,
                    row,
                    EvaluatedArgument::SelectedColumn(&compact),
                    ordinal,
                    row,
                    &Control::default()
                )
                .unwrap()
        );
        assert_eq!(
            prepared
                .compare_rows(
                    EvaluatedArgument::Constant(&constant),
                    88,
                    999,
                    EvaluatedArgument::SelectedColumn(&compact),
                    ordinal,
                    row,
                    &Control::default()
                )
                .unwrap(),
            ordinal == 0
        );
        assert_eq!(
            prepared
                .compare_rows(
                    EvaluatedArgument::Scalar(&scalar),
                    88,
                    999,
                    EvaluatedArgument::Column(&sliced),
                    ordinal,
                    row,
                    &Control::default()
                )
                .unwrap(),
            ordinal == 0
        );
    }
    assert!(
        prepared
            .compare_rows(
                EvaluatedArgument::Constant(&null),
                88,
                999,
                EvaluatedArgument::Column(&sliced),
                1,
                2,
                &Control::default()
            )
            .unwrap()
    );
    // The two compact arguments may have entirely different local ordinals.
    let other_rows = [0, 1];
    let other_selection = Selection::try_sparse(2, &other_rows).unwrap();
    let other = SelectedValues::try_new(
        other_selection,
        &DataType::Int64,
        Arc::new(Int64Array::from(vec![Some(7), Some(21)])),
        Box::default(),
    )
    .unwrap();
    assert!(
        prepared
            .compare_rows(
                EvaluatedArgument::SelectedColumn(&compact),
                0,
                0,
                EvaluatedArgument::SelectedColumn(&other),
                1,
                1,
                &Control::default()
            )
            .unwrap()
    );
}

#[test]
fn successful_null_does_not_mask_bad_address_null_promise_or_required_child_error() {
    let t = ty(DataType::Int64, true);
    let prepared =
        PreparedNullSafeComparisonRecipe::try_new(&t, &t, &CompileControl::default()).unwrap();
    let null: ArrayRef = Arc::new(Int64Array::from(vec![None]));
    let nonnull: ArrayRef = Arc::new(Int64Array::from(vec![Some(7)]));
    let wrong: ArrayRef = Arc::new(Float64Array::from(vec![None]));
    let many: ArrayRef = Arc::new(Int64Array::from(vec![None, None]));
    for argument in [
        EvaluatedArgument::Column(&wrong),
        EvaluatedArgument::Scalar(&many),
    ] {
        assert!(matches!(
            prepared.compare_rows(
                EvaluatedArgument::Column(&null),
                0,
                0,
                argument,
                0,
                0,
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
    assert!(matches!(
        prepared.compare_rows(
            EvaluatedArgument::Column(&null),
            0,
            0,
            EvaluatedArgument::Column(&nonnull),
            0,
            1,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let strict = PreparedNullSafeComparisonRecipe::try_new(
        &t,
        &ty(DataType::Int64, false),
        &CompileControl::default(),
    )
    .unwrap();
    assert!(matches!(
        strict.compare_rows(
            EvaluatedArgument::Column(&null),
            0,
            0,
            EvaluatedArgument::Column(&null),
            0,
            0,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let selection = Selection::all(1);
    let unresolved = SelectedValues::try_new(
        selection,
        &DataType::Int64,
        null.clone(),
        vec![RowDataError::new(0, "required source error")].into_boxed_slice(),
    )
    .unwrap();
    assert!(matches!(
        prepared.compare_rows(
            EvaluatedArgument::Column(&null),
            0,
            0,
            EvaluatedArgument::SelectedColumn(&unresolved),
            0,
            0,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert!(matches!(
        prepared.compare_rows(
            EvaluatedArgument::SelectedColumn(&unresolved),
            0,
            0,
            EvaluatedArgument::Column(&null),
            0,
            0,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let empty: ArrayRef = Arc::new(Int64Array::from(Vec::<Option<i64>>::new()));
    assert!(matches!(
        prepared.compare_rows(
            EvaluatedArgument::Column(&null),
            0,
            0,
            EvaluatedArgument::Column(&empty),
            0,
            0,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn compile_original_three_causes_keep_entry_observed_metadata_quantum_and_tail() {
    let flat = ty(DataType::Int64, true);
    let wide = ty(
        DataType::Struct(
            (0..320)
                .map(|i| {
                    Arc::new(
                        Field::new(format!("source-{i}"), DataType::Int64, true).with_metadata(
                            HashMap::from([("provider".to_owned(), "actual-source".to_owned())]),
                        ),
                    )
                })
                .collect::<Vec<_>>()
                .into(),
        ),
        true,
    );
    for source in [flat, wide] {
        let baseline = CompileControl::default();
        let _ = PreparedNullSafeComparisonRecipe::try_new(&source, &source, &baseline);
        let trace = baseline.trace.lock().unwrap().clone();
        assert_eq!(trace[0], 0);
        assert!(trace.last().copied().unwrap() > 0);
        if matches!(source.data_type(), DataType::Struct(_)) {
            assert!(trace.contains(&256));
        }
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = CompileControl {
                    refusal: Some((at, cause.clone())),
                    ..Default::default()
                };
                let error = PreparedNullSafeComparisonRecipe::try_new(&source, &source, &control)
                    .unwrap_err();
                assert_eq!(error.control_error(), Some(cause));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn long_prefix_original_seven_causes_and_success_null_ordinary_error_tails_never_replay() {
    let common = "x".repeat(700);
    let left: ArrayRef = Arc::new(StringArray::from(vec![Some(common.as_str()), None]));
    let different = format!("{common}z");
    let right: ArrayRef = Arc::new(StringArray::from(vec![
        Some(common.as_str()),
        Some(different.as_str()),
    ]));
    let prepared = recipe(&left, &right);
    for (lr, rr) in [(0, 0), (1, 1), (0, 2)] {
        let baseline = Control::default();
        let _ = prepared.compare_rows(
            EvaluatedArgument::Column(&left),
            lr,
            lr,
            EvaluatedArgument::Column(&right),
            rr,
            rr,
            &baseline,
        );
        let trace = baseline.trace.lock().unwrap().clone();
        assert_eq!(trace[0], 0);
        assert!(trace.last().copied().unwrap() > 0);
        if (lr, rr) == (0, 0) {
            assert!(trace.contains(&256));
        }
        for at in 0..trace.len() {
            for cause in causes() {
                let control = Control {
                    refusal: Some((at, cause.clone())),
                    ..Default::default()
                };
                assert_eq!(
                    prepared.compare_rows(
                        EvaluatedArgument::Column(&left),
                        lr,
                        lr,
                        EvaluatedArgument::Column(&right),
                        rr,
                        rr,
                        &control
                    ),
                    Err(cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
    let first: ArrayRef = Arc::new(StringArray::from(vec!["y"]));
    let second: ArrayRef = Arc::new(StringArray::from(vec!["x".repeat(100_000)]));
    let control = Control::default();
    assert!(
        !prepared
            .compare_rows(
                EvaluatedArgument::Column(&first),
                0,
                0,
                EvaluatedArgument::Column(&second),
                0,
                0,
                &control
            )
            .unwrap()
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
}
