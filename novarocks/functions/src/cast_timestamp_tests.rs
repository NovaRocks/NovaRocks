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
use crate::{ConstantPolicy, ConstantPool, SelectedValues, Selection};
use arrow_array::ArrayRef;
use arrow_schema::Field;
use novarocks_type_contract::{EvaluationDemand, EvaluationDomainId, ExpressionUseId};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

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
            assert!(at <= stop, "compile callback after refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
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
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "runtime callback after refusal");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("timestamp cast never waits")
    }
}
fn units() -> [TimeUnit; 4] {
    [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ]
}
fn ty(unit: TimeUnit, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Timestamp(unit, None), nullable)
}
fn array(unit: TimeUnit, values: Vec<Option<i64>>) -> ArrayRef {
    match unit {
        TimeUnit::Second => Arc::new(TimestampSecondArray::from(values)),
        TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(values)),
        TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(values)),
        TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(values)),
    }
}
fn prepare(
    from: TimeUnit,
    to: TimeUnit,
    nullable: bool,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &ty(from, nullable),
        &ty(to, true),
        policy,
        allow,
        &CompileControl::default(),
    )
    .unwrap()
}
fn policies() -> [DecimalOverflowPolicy; 2] {
    [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ]
}
fn at(recipe: &PreparedCastRecipe, input: &ArrayRef, row: usize) -> CastRowResult {
    recipe
        .evaluate_row(
            EvaluatedArgument::Column(input),
            row,
            row,
            &Control::default(),
        )
        .unwrap()
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(7),
        domain: EvaluationDomainId::new(11),
        demand: EvaluationDemand::Value,
    }
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
fn pool(input: &ArrayRef, nullable: bool) -> ConstantPool {
    let source =
        FunctionValueType::from_legacy_declared_projection(input.data_type().clone(), nullable);
    let field = Arc::new(
        Field::new("original-timestamp", input.data_type().clone(), nullable).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "source-note".into(),
                "retain-exact-pool".into(),
            )]),
        ),
    );
    let pool = ConstantPool::try_new(
        field.clone(),
        source,
        input.to_data(),
        policy(),
        CompilePhase::Validate,
        &CompileControl::default(),
    )
    .unwrap();
    assert!(Arc::ptr_eq(pool.field_ref(), &field));
    pool
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original callback invalid"),
        internal("original callback internal"),
        KernelFailure::Operational(crate::KernelDiagnostic::new(
            "original callback operational",
        )),
        KernelFailure::InstanceFailed,
    ]
}

#[test]
fn timestamp_none_all_sixteen_profiles_keep_independent_hand_values_and_arrow_oracle() {
    // Rows of this independent table are source units; columns are target units.
    // Values are the exact result of source coefficient 1001, written independently.
    let positive = [
        [1001, 1_001_000, 1_001_000_000, 1_001_000_000_000],
        [1, 1001, 1_001_000, 1_001_000_000],
        [0, 1, 1001, 1_001_000],
        [0, 0, 1, 1001],
    ];
    for (left, from) in units().into_iter().enumerate() {
        let input = array(from, vec![Some(-1001), Some(0), Some(1001), None]);
        for (right, to) in units().into_iter().enumerate() {
            let oracle = arrow_cast::cast(input.as_ref(), &DataType::Timestamp(to, None)).unwrap();
            let data = oracle.as_any();
            let actual: Vec<Option<i64>> = match to {
                TimeUnit::Second => data
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap()
                    .iter()
                    .collect(),
                TimeUnit::Millisecond => data
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .iter()
                    .collect(),
                TimeUnit::Microsecond => data
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .iter()
                    .collect(),
                TimeUnit::Nanosecond => data
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .iter()
                    .collect(),
            };
            assert_eq!(
                actual,
                vec![
                    Some(-positive[left][right]),
                    Some(0),
                    Some(positive[left][right]),
                    None
                ]
            );
            for policy in policies() {
                for allow in [false, true] {
                    let recipe = prepare(from, to, true, policy, allow);
                    assert_eq!(recipe.operation(), CastOperation::Carrier);
                    assert_eq!(recipe.source_type(), &ty(from, true));
                    assert_eq!(recipe.result_type(), &ty(to, true));
                    assert_eq!(recipe.policy(), policy);
                    assert_eq!(recipe.allow_throw_exception(), allow);
                    for (row, expected) in actual.iter().enumerate() {
                        assert_eq!(
                            at(&recipe, &input, row),
                            expected
                                .map(CastRowResult::Timestamp)
                                .unwrap_or(CastRowResult::Null)
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn timestamp_widening_boundaries_and_us_ns_error_ignore_allow_and_decimal_policy() {
    for (from, to, factor, special) in [
        (TimeUnit::Second, TimeUnit::Millisecond, 1000, false),
        (TimeUnit::Second, TimeUnit::Microsecond, 1_000_000, false),
        (TimeUnit::Second, TimeUnit::Nanosecond, 1_000_000_000, false),
        (TimeUnit::Millisecond, TimeUnit::Microsecond, 1000, false),
        (
            TimeUnit::Millisecond,
            TimeUnit::Nanosecond,
            1_000_000,
            false,
        ),
        (TimeUnit::Microsecond, TimeUnit::Nanosecond, 1000, true),
    ] {
        let lo = i64::MIN / factor;
        let hi = i64::MAX / factor;
        let input = array(
            from,
            vec![
                Some(lo),
                Some(hi),
                Some(lo - 1),
                Some(hi + 1),
                Some(i64::MIN),
                Some(i64::MAX),
                None,
            ],
        );
        for policy in policies() {
            for allow in [false, true] {
                let recipe = prepare(from, to, true, policy, allow);
                assert_eq!(
                    at(&recipe, &input, 0),
                    CastRowResult::Timestamp(lo * factor)
                );
                assert_eq!(
                    at(&recipe, &input, 1),
                    CastRowResult::Timestamp(hi * factor)
                );
                for row in 2..6 {
                    let result = at(&recipe, &input, row);
                    if special {
                        let CastRowResult::RowError(error) = result else {
                            panic!("us->ns must keep its original error")
                        };
                        assert_eq!(error.selected_ordinal(), row);
                        assert_eq!(error.reason(), crate::DataFailureReason::NumericOutOfRange);
                        let raw = match row {
                            2 => lo - 1,
                            3 => hi + 1,
                            4 => i64::MIN,
                            _ => i64::MAX,
                        };
                        assert_eq!(
                            error.message(),
                            format!(
                                "CAST failed: from Timestamp(Microsecond, None) to Timestamp(Nanosecond, None): CAST timestamp microsecond->nanosecond overflow: value {raw} cannot be represented as nanoseconds in i64"
                            )
                        );
                    } else {
                        assert_eq!(result, CastRowResult::Null);
                    }
                }
                assert_eq!(at(&recipe, &input, 6), CastRowResult::Null);
            }
        }
    }
    // Full i64 extrema remain legal coefficients without a Chrono date gate.
    for (from, to, divisor) in [
        (TimeUnit::Millisecond, TimeUnit::Second, 1000),
        (TimeUnit::Microsecond, TimeUnit::Second, 1_000_000),
        (TimeUnit::Microsecond, TimeUnit::Millisecond, 1000),
        (TimeUnit::Nanosecond, TimeUnit::Second, 1_000_000_000),
        (TimeUnit::Nanosecond, TimeUnit::Millisecond, 1_000_000),
        (TimeUnit::Nanosecond, TimeUnit::Microsecond, 1000),
    ] {
        let input = array(from, vec![Some(i64::MIN), Some(i64::MAX), Some(-1)]);
        let recipe = prepare(from, to, false, DecimalOverflowPolicy::OutputNull, false);
        assert_eq!(
            at(&recipe, &input, 0),
            CastRowResult::Timestamp(i64::MIN / divisor)
        );
        assert_eq!(
            at(&recipe, &input, 1),
            CastRowResult::Timestamp(i64::MAX / divisor)
        );
        assert_eq!(at(&recipe, &input, 2), CastRowResult::Timestamp(0));
    }
    for unit in units() {
        let input = array(unit, vec![Some(i64::MIN), Some(i64::MAX)]);
        let recipe = prepare(unit, unit, false, DecimalOverflowPolicy::ReportError, true);
        assert_eq!(at(&recipe, &input, 0), CastRowResult::Timestamp(i64::MIN));
        assert_eq!(at(&recipe, &input, 1), CastRowResult::Timestamp(i64::MAX));
    }
}

#[test]
fn timestamp_nullability_and_effects_follow_exact_five_null_pairs_and_one_error_pair() {
    for from in units() {
        for to in units() {
            for source_nullable in [false, true] {
                for policy in policies() {
                    for allow in [false, true] {
                        let null_pair = matches!(
                            (from, to),
                            (
                                TimeUnit::Second,
                                TimeUnit::Millisecond
                                    | TimeUnit::Microsecond
                                    | TimeUnit::Nanosecond
                            ) | (
                                TimeUnit::Millisecond,
                                TimeUnit::Microsecond | TimeUnit::Nanosecond
                            )
                        );
                        assert_eq!(
                            carrier_cast_can_produce_null(
                                ty(from, false).data_type(),
                                ty(to, false).data_type(),
                                allow
                            ),
                            null_pair
                        );
                        let result = PreparedCastRecipe::try_new(
                            CastOperation::Carrier,
                            &ty(from, source_nullable),
                            &ty(to, false),
                            policy,
                            allow,
                            &CompileControl::default(),
                        );
                        if source_nullable || null_pair {
                            assert_eq!(result, Err(CastPrepareError::TypeMismatch));
                        } else {
                            result.unwrap();
                        }
                        let recipe = prepare(from, to, source_nullable, policy, allow);
                        assert_eq!(
                            recipe
                                .own_effects(context())
                                .for_use(context())
                                .unwrap()
                                .may_raise_row_error,
                            from == TimeUnit::Microsecond && to == TimeUnit::Nanosecond
                        );
                    }
                }
            }
        }
    }
    super::tests::assert_f3_selected_literal_case(
        Arc::new(arrow_array::TimestampSecondArray::from(vec![
            Some(123456),
            None,
        ])),
        Arc::new(arrow_array::Int64Array::from(vec![Some(123456), None])),
    );
    for (source, target) in [
        (
            DataType::Int64,
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
        ),
        (
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            DataType::Timestamp(TimeUnit::Second, None),
        ),
        (
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Second, Some("".into())),
        ),
        (
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
    ] {
        assert_eq!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &FunctionValueType::from_legacy_declared_projection(source, true),
                &FunctionValueType::from_legacy_declared_projection(target, true),
                DecimalOverflowPolicy::OutputNull,
                false,
                &CompileControl::default()
            ),
            Err(CastPrepareError::Unsupported)
        );
    }
    for operation in [CastOperation::Time, CastOperation::TimeFromDatetime] {
        assert_eq!(
            PreparedCastRecipe::try_new(
                operation,
                &ty(TimeUnit::Second, true),
                &ty(TimeUnit::Second, true),
                DecimalOverflowPolicy::OutputNull,
                false,
                &CompileControl::default()
            ),
            Err(CastPrepareError::Unsupported)
        );
    }
}

#[test]
fn timestamp_constant_real_ordinal_scalar_slice_and_compact_addresses_stay_independent() {
    for unit in units() {
        let recipe = prepare(unit, unit, true, DecimalOverflowPolicy::OutputNull, false);
        let original = array(unit, vec![Some(111), None, Some(i64::MIN)]);
        let pool = pool(&original, true);
        let chosen = pool.value(2).unwrap();
        assert_eq!(chosen.ordinal(), 2);
        assert!(Arc::ptr_eq(chosen.pool().array(), pool.array()));
        assert_eq!(
            chosen.field().metadata()["source-note"],
            "retain-exact-pool"
        );
        assert_eq!(
            recipe
                .evaluate_row(
                    EvaluatedArgument::Constant(&chosen),
                    77,
                    999,
                    &Control::default()
                )
                .unwrap(),
            CastRowResult::Timestamp(i64::MIN)
        );
        let null = pool.value(1).unwrap();
        assert_eq!(
            recipe
                .evaluate_row(
                    EvaluatedArgument::Constant(&null),
                    77,
                    999,
                    &Control::default()
                )
                .unwrap(),
            CastRowResult::Null
        );
        let original = array(unit, vec![Some(11), Some(22), None, Some(44)]);
        let sliced = original.slice(1, 3);
        assert_eq!(at(&recipe, &sliced, 2), CastRowResult::Timestamp(44));
        let scalar = sliced.slice(0, 1);
        assert_eq!(
            recipe
                .evaluate_row(
                    EvaluatedArgument::Scalar(&scalar),
                    0,
                    999,
                    &Control::default()
                )
                .unwrap(),
            CastRowResult::Timestamp(22)
        );
        let rows = [1, 3];
        let selection = Selection::try_sparse(4, &rows).unwrap();
        let compact = SelectedValues::try_new(
            selection,
            recipe.source_type().data_type(),
            array(unit, vec![Some(51), Some(52)]),
            Box::default(),
        )
        .unwrap();
        assert_eq!(
            recipe
                .evaluate_row(
                    EvaluatedArgument::SelectedColumn(&compact),
                    1,
                    3,
                    &Control::default()
                )
                .unwrap(),
            CastRowResult::Timestamp(52)
        );
        assert!(matches!(
            recipe.evaluate_row(
                EvaluatedArgument::SelectedColumn(&compact),
                1,
                1,
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
}

#[derive(Debug)]
struct ForeignTimestamp(TimestampMicrosecondArray);
// SAFETY: The immutable canonical array supplies every buffer/layout/lifetime
// method. Only Any identity differs to exercise the concrete-class gate.
unsafe impl Array for ForeignTimestamp {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn to_data(&self) -> arrow_data::ArrayData {
        self.0.to_data()
    }
    fn into_data(self) -> arrow_data::ArrayData {
        self.0.into_data()
    }
    fn data_type(&self) -> &DataType {
        self.0.data_type()
    }
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(self.0.slice(offset, length))
    }
    fn len(&self) -> usize {
        self.0.len()
    }
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    fn offset(&self) -> usize {
        self.0.offset()
    }
    fn nulls(&self) -> Option<&arrow_buffer::NullBuffer> {
        self.0.nulls()
    }
    fn get_buffer_memory_size(&self) -> usize {
        self.0.get_buffer_memory_size()
    }
    fn get_array_memory_size(&self) -> usize {
        self.0.get_array_memory_size()
    }
}
#[test]
fn timestamp_class_source_domain_addresses_and_required_journal_are_checked_before_null() {
    let recipe = prepare(
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
        true,
        DecimalOverflowPolicy::ReportError,
        true,
    );
    let foreign: ArrayRef = Arc::new(ForeignTimestamp(TimestampMicrosecondArray::from(vec![
        None,
    ])));
    assert!(matches!(
        at_error(&recipe, EvaluatedArgument::Column(&foreign), 0, 0),
        KernelFailure::Internal(_)
    ));
    let null = array(TimeUnit::Microsecond, vec![None]);
    let nonnull = prepare(
        TimeUnit::Microsecond,
        TimeUnit::Microsecond,
        false,
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    assert!(matches!(
        at_error(&nonnull, EvaluatedArgument::Column(&null), 0, 0),
        KernelFailure::InvalidProgram(_)
    ));
    let values = array(TimeUnit::Microsecond, vec![None, Some(i64::MAX)]);
    assert!(matches!(
        at_error(&recipe, EvaluatedArgument::Scalar(&values), 0, 0),
        KernelFailure::InvalidProgram(_)
    ));
    assert!(matches!(
        at_error(&recipe, EvaluatedArgument::Column(&values), 2, 2),
        KernelFailure::InvalidProgram(_)
    ));
    let wrong = array(TimeUnit::Millisecond, vec![None]);
    let wrong_pool = pool(&wrong, true);
    let wrong = wrong_pool.value(0).unwrap();
    assert!(matches!(
        at_error(&recipe, EvaluatedArgument::Constant(&wrong), 0, 0),
        KernelFailure::InvalidProgram(_)
    ));
    let required = SelectedValues::try_new(
        Selection::all(2),
        ty(TimeUnit::Microsecond, true).data_type(),
        array(TimeUnit::Microsecond, vec![None, None]),
        vec![RowDataError::new(1, "required child failed")].into_boxed_slice(),
    )
    .unwrap();
    assert_eq!(
        recipe
            .evaluate_row(
                EvaluatedArgument::SelectedColumn(&required),
                0,
                0,
                &Control::default()
            )
            .unwrap(),
        CastRowResult::Null
    );
    assert!(matches!(
        at_error(&recipe, EvaluatedArgument::SelectedColumn(&required), 1, 1),
        KernelFailure::InvalidProgram(_)
    ));
    // An unselected overflow coefficient is not speculatively evaluated.
    assert_eq!(at(&recipe, &values, 0), CastRowResult::Null);
}
fn at_error(
    recipe: &PreparedCastRecipe,
    arg: EvaluatedArgument<'_>,
    ordinal: usize,
    row: usize,
) -> KernelFailure {
    recipe
        .evaluate_row(arg, ordinal, row, &Control::default())
        .unwrap_err()
}

#[test]
fn timestamp_compile_all_actual_callbacks_keep_original_three_causes_and_ordinary_tails() {
    for (source, result, success) in [
        (
            ty(TimeUnit::Microsecond, true),
            ty(TimeUnit::Nanosecond, true),
            true,
        ),
        (
            ty(TimeUnit::Second, false),
            ty(TimeUnit::Millisecond, false),
            false,
        ),
        (
            FunctionValueType::new(
                DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                true,
            ),
            ty(TimeUnit::Second, true),
            false,
        ),
    ] {
        let good = CompileControl::default();
        let result_good = PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &source,
            &result,
            DecimalOverflowPolicy::ReportError,
            false,
            &good,
        );
        assert_eq!(result_good.is_ok(), success);
        let trace = good.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = CompileControl {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let error = PreparedCastRecipe::try_new(
                    CastOperation::Carrier,
                    &source,
                    &result,
                    DecimalOverflowPolicy::ReportError,
                    false,
                    &control,
                )
                .unwrap_err();
                assert_eq!(error.control_error(), Some(cause));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn timestamp_runtime_all_actual_callbacks_keep_seven_causes_including_diagnostic_boundaries() {
    for (from, to, value, row) in [
        (TimeUnit::Nanosecond, TimeUnit::Microsecond, Some(-1001), 0),
        (TimeUnit::Second, TimeUnit::Millisecond, Some(i64::MAX), 0),
        (
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
            Some(i64::MAX),
            0,
        ),
        (TimeUnit::Microsecond, TimeUnit::Nanosecond, None, 0),
        (TimeUnit::Second, TimeUnit::Second, Some(0), 1),
    ] {
        let input = array(from, vec![value]);
        let recipe = prepare(from, to, true, DecimalOverflowPolicy::ReportError, false);
        let good = Control::default();
        let result = recipe.evaluate_row(EvaluatedArgument::Column(&input), row, row, &good);
        if row == 1 {
            assert!(matches!(result, Err(KernelFailure::InvalidProgram(_))));
        } else {
            result.unwrap();
        }
        let trace = good.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        // Each row has a finite O(1) owned operation count; no fake256 prescan.
        for at in 0..trace.len() {
            for cause in causes() {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                assert_eq!(
                    recipe
                        .evaluate_row(EvaluatedArgument::Column(&input), row, row, &control)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
