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
use arrow_array::{ArrayRef, Float64Array};
use novarocks_type_contract::arithmetic_result_value_type_with_op;
use novarocks_type_contract::{EvaluationDemand, EvaluationDomainId, ExpressionUseId};
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
        panic!("arithmetic must not wait")
    }
}
fn ty(carrier: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(carrier, nullable)
}
fn prepare(
    op: ArithmeticOperator,
    left: FunctionValueType,
    right: FunctionValueType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedArithmeticRecipe {
    let mut result = arithmetic_result_value_type_with_op(&left, &right, op).unwrap();
    result = result
        .with_nullable(true)
        .expect("valid function nullability declaration");
    PreparedArithmeticRecipe::try_new(
        op,
        &left,
        &right,
        &result,
        policy,
        allow,
        &CompileControl::default(),
    )
    .unwrap()
}
fn i64_recipe(op: ArithmeticOperator) -> PreparedArithmeticRecipe {
    prepare(
        op,
        ty(DataType::Int64, true),
        ty(DataType::Int64, true),
        DecimalOverflowPolicy::OutputNull,
        false,
    )
}
fn array(carrier: &DataType, values: &[Option<i64>]) -> ArrayRef {
    macro_rules! make {
        ($array:ty, $native:ty) => {
            Arc::new(<$array>::from(
                values
                    .iter()
                    .map(|value| value.map(|value| <$native>::try_from(value).unwrap()))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        };
    }
    match carrier {
        DataType::Int8 => make!(Int8Array, i8),
        DataType::Int16 => make!(Int16Array, i16),
        DataType::Int32 => make!(Int32Array, i32),
        DataType::Int64 => make!(Int64Array, i64),
        _ => panic!("fixture requires its explicit signed carrier"),
    }
}
fn eval(
    prepared: &PreparedArithmeticRecipe,
    left: &ArrayRef,
    right: &ArrayRef,
    row: usize,
) -> ArithmeticRowResult {
    prepared
        .evaluate_row(
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
        use_id: ExpressionUseId::new(31),
        domain: EvaluationDomainId::new(9),
        demand: EvaluationDemand::Value,
    }
}

#[test]
fn all_eighty_signed_profiles_preserve_frozen_result_width_and_independent_policies() {
    use ArithmeticOperator::*;
    let carriers = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ];
    for (li, left) in carriers.iter().enumerate() {
        for (ri, right) in carriers.iter().enumerate() {
            for op in [Add, Subtract, Multiply, Divide, Modulo] {
                for nullable in [[false, false], [false, true], [true, false], [true, true]] {
                    for policy in [
                        DecimalOverflowPolicy::OutputNull,
                        DecimalOverflowPolicy::ReportError,
                    ] {
                        for allow in [false, true] {
                            let recipe = prepare(
                                op,
                                ty(left.clone(), nullable[0]),
                                ty(right.clone(), nullable[1]),
                                policy,
                                allow,
                            );
                            let expected_carrier = carriers[li.max(ri)].clone();
                            assert_eq!(recipe.result_type(), &ty(expected_carrier, true));
                            assert_eq!(recipe.operator(), op);
                            assert_eq!(recipe.left_type(), &ty(left.clone(), nullable[0]));
                            assert_eq!(recipe.right_type(), &ty(right.clone(), nullable[1]));
                            assert_eq!(recipe.decimal_overflow_policy(), policy);
                            assert_eq!(recipe.allow_throw_exception(), allow);
                            let actual = eval(
                                &recipe,
                                &array(left, &[Some(7)]),
                                &array(right, &[Some(2)]),
                                0,
                            );
                            assert_eq!(
                                actual,
                                match op {
                                    Add => ArithmeticRowResult::Signed(9),
                                    Subtract => ArithmeticRowResult::Signed(5),
                                    Multiply => ArithmeticRowResult::Signed(14),
                                    Divide => ArithmeticRowResult::Signed(3),
                                    Modulo => ArithmeticRowResult::Signed(1),
                                }
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn checked_signed_boundaries_and_zero_errors_ignore_decimal_and_allow_policies() {
    use ArithmeticOperator::*;
    for (carrier, min, max) in [
        (DataType::Int8, i64::from(i8::MIN), i64::from(i8::MAX)),
        (DataType::Int16, i64::from(i16::MIN), i64::from(i16::MAX)),
        (DataType::Int32, i64::from(i32::MIN), i64::from(i32::MAX)),
        (DataType::Int64, i64::MIN, i64::MAX),
    ] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                for (op, left, right, reason) in [
                    (Add, max, 1, crate::DataFailureReason::NumericOutOfRange),
                    (
                        Subtract,
                        min,
                        1,
                        crate::DataFailureReason::NumericOutOfRange,
                    ),
                    (
                        Multiply,
                        max,
                        2,
                        crate::DataFailureReason::NumericOutOfRange,
                    ),
                    (Divide, min, -1, crate::DataFailureReason::NumericOutOfRange),
                    (Divide, 8, 0, crate::DataFailureReason::DivisionByZero),
                    (Modulo, 8, 0, crate::DataFailureReason::DivisionByZero),
                ] {
                    let recipe = prepare(
                        op,
                        ty(carrier.clone(), true),
                        ty(carrier.clone(), true),
                        policy,
                        allow,
                    );
                    let ArithmeticRowResult::RowError(error) = eval(
                        &recipe,
                        &array(&carrier, &[Some(4), Some(left)]),
                        &array(&carrier, &[Some(2), Some(right)]),
                        1,
                    ) else {
                        panic!("required signed data error disappeared")
                    };
                    assert_eq!(error.selected_ordinal(), 1);
                    assert_eq!(error.reason(), reason);
                    assert!(!error.message().is_empty());
                }
                for (op, left, right, expected) in [
                    (Modulo, min, -1, 0),
                    (Divide, -7, 3, -2),
                    (Divide, 7, -3, -2),
                    (Divide, -7, -3, 2),
                ] {
                    let recipe = prepare(
                        op,
                        ty(carrier.clone(), true),
                        ty(carrier.clone(), true),
                        policy,
                        allow,
                    );
                    assert_eq!(
                        eval(
                            &recipe,
                            &array(&carrier, &[Some(left)]),
                            &array(&carrier, &[Some(right)]),
                            0
                        ),
                        ArithmeticRowResult::Signed(expected)
                    );
                }
                for op in [Divide, Modulo] {
                    let recipe = prepare(
                        op,
                        ty(carrier.clone(), true),
                        ty(carrier.clone(), true),
                        policy,
                        allow,
                    );
                    assert_eq!(
                        eval(
                            &recipe,
                            &array(&carrier, &[None]),
                            &array(&carrier, &[Some(0)]),
                            0
                        ),
                        ArithmeticRowResult::Null
                    );
                }
            }
        }
    }
    // Overflow belongs to the shared result width, not either operand's narrower domain.
    let recipe = prepare(
        Divide,
        ty(DataType::Int8, false),
        ty(DataType::Int16, false),
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    assert_eq!(
        eval(
            &recipe,
            &array(&DataType::Int8, &[Some(-128)]),
            &array(&DataType::Int16, &[Some(-1)]),
            0
        ),
        ArithmeticRowResult::Signed(128)
    );
}

#[test]
fn signed_effects_keep_actual_context_and_do_not_borrow_error_policy_as_a_safety_proof() {
    for op in [
        ArithmeticOperator::Add,
        ArithmeticOperator::Subtract,
        ArithmeticOperator::Multiply,
        ArithmeticOperator::Divide,
        ArithmeticOperator::Modulo,
    ] {
        for carrier in [DataType::Int8, DataType::Int32, DataType::Int64] {
            let recipe = prepare(
                op,
                ty(carrier.clone(), false),
                ty(carrier.clone(), false),
                DecimalOverflowPolicy::OutputNull,
                false,
            );
            let effects = recipe.own_effects(context()).for_use(context()).unwrap();
            assert!(effects.may_raise_row_error);
            assert!(!effects.has_instance_state);
            assert_eq!(
                effects.observable_effects,
                ExpressionEffects::PURE_VALUE.observable_effects
            );
            let foreign = ExpressionEffectContext {
                use_id: ExpressionUseId::new(32),
                ..context()
            };
            assert!(recipe.own_effects(context()).for_use(foreign).is_err());
        }
    }
}

fn pool_constant() -> crate::ConstantValue {
    let source = ty(DataType::Int64, true);
    let data = Int64Array::from(vec![Some(i64::MAX), Some(7), None]).to_data();
    let pool = ConstantPool::try_new(
        Arc::new(source.try_to_field("actual-selected-pool").unwrap()),
        source,
        data,
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
            // The owner's single-node diagnostic bound already reserves 4096
            // bytes, in addition to actual buffer bytes and ArrayData headers.
            max_library_validation_bytes: 8192,
        },
        CompilePhase::Validate,
        &CompileControl::default(),
    )
    .unwrap();
    assert!(pool.resource_facts().library_validation_bytes_upper_bound <= 8192);
    pool.value(1).unwrap()
}

#[test]
fn selected_constant_scalar_column_and_compact_addresses_never_read_unselected_rows() {
    let recipe = i64_recipe(ArithmeticOperator::Add);
    let constant = pool_constant();
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Int64,
        array(&DataType::Int64, &[Some(2), Some(5)]),
        Box::new([]),
    )
    .unwrap();
    for (ordinal, row, expected) in [(0, 1, 9), (1, 3, 12)] {
        assert_eq!(
            recipe
                .evaluate_row(
                    EvaluatedArgument::Constant(&constant),
                    ordinal,
                    row,
                    EvaluatedArgument::SelectedColumn(&compact),
                    ordinal,
                    row,
                    &Control::default()
                )
                .unwrap(),
            ArithmeticRowResult::Signed(expected)
        );
    }
    let scalar = array(&DataType::Int64, &[Some(2)]);
    let column = array(&DataType::Int64, &[Some(i64::MAX), Some(7), None, Some(5)]);
    assert_eq!(
        recipe
            .evaluate_row(
                EvaluatedArgument::Scalar(&scalar),
                1,
                3,
                EvaluatedArgument::Column(&column),
                1,
                3,
                &Control::default()
            )
            .unwrap(),
        ArithmeticRowResult::Signed(7)
    );
    assert!(matches!(
        recipe.evaluate_row(
            EvaluatedArgument::Constant(&constant),
            0,
            3,
            EvaluatedArgument::SelectedColumn(&compact),
            0,
            3,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let error_compact = SelectedValues::try_new(
        selection,
        &DataType::Int64,
        array(&DataType::Int64, &[None, Some(5)]),
        Box::from([RowDataError::new(0, "inherited")]),
    )
    .unwrap();
    assert!(matches!(
        recipe.evaluate_row(
            EvaluatedArgument::Constant(&constant),
            0,
            1,
            EvaluatedArgument::SelectedColumn(&error_compact),
            0,
            1,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn both_addresses_and_nonnull_promises_are_checked_before_strict_null() {
    let recipe = prepare(
        ArithmeticOperator::Add,
        ty(DataType::Int64, true),
        ty(DataType::Int64, false),
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let null = array(&DataType::Int64, &[None]);
    let wrong: ArrayRef = Arc::new(Float64Array::from(vec![2.0]));
    for right in [&null, &wrong] {
        assert!(matches!(
            recipe.evaluate_row(
                EvaluatedArgument::Column(&null),
                0,
                0,
                EvaluatedArgument::Column(right),
                0,
                0,
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
    let good = array(&DataType::Int64, &[Some(2)]);
    assert!(matches!(
        recipe.evaluate_row(
            EvaluatedArgument::Column(&null),
            0,
            0,
            EvaluatedArgument::Column(&good),
            0,
            9,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let too_many_scalar = array(&DataType::Int64, &[Some(2), Some(3)]);
    assert!(matches!(
        recipe.evaluate_row(
            EvaluatedArgument::Column(&null),
            0,
            0,
            EvaluatedArgument::Scalar(&too_many_scalar),
            0,
            0,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let constant = pool_constant();
    assert!(matches!(
        recipe.evaluate_row(
            EvaluatedArgument::Column(&null),
            0,
            0,
            EvaluatedArgument::Constant(&constant),
            0,
            0,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn preparation_refuses_other_domains_and_wrong_result_or_nonnullable_output() {
    let signed = ty(DataType::Int64, false);
    for other in [
        ty(DataType::UInt64, false),
        ty(DataType::FixedSizeBinary(16), false),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    ] {
        assert!(matches!(
            PreparedArithmeticRecipe::try_new(
                ArithmeticOperator::Add,
                &signed,
                &other,
                &ty(DataType::Int64, true),
                DecimalOverflowPolicy::OutputNull,
                false,
                &CompileControl::default()
            ),
            Err(ArithmeticPrepareError::Unsupported)
        ));
    }
    for other in [
        ty(DataType::Float32, false),
        ty(DataType::Float64, false),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::LargeInt,
        )
        .unwrap(),
        ty(DataType::Decimal128(18, 6), false),
    ] {
        assert_eq!(
            PreparedArithmeticRecipe::try_new(
                ArithmeticOperator::Add,
                &signed,
                &other,
                &ty(DataType::Int64, true),
                DecimalOverflowPolicy::OutputNull,
                false,
                &CompileControl::default()
            ),
            Err(ArithmeticPrepareError::TypeMismatch)
        );
    }
    for result in [
        ty(DataType::Int64, false),
        ty(DataType::Int32, true),
        ty(DataType::Float64, true),
    ] {
        assert_eq!(
            PreparedArithmeticRecipe::try_new(
                ArithmeticOperator::Add,
                &signed,
                &signed,
                &result,
                DecimalOverflowPolicy::OutputNull,
                false,
                &CompileControl::default()
            ),
            Err(ArithmeticPrepareError::TypeMismatch)
        );
    }
    assert_eq!(
        PreparedArithmeticRecipe::try_new(
            ArithmeticOperator::Divide,
            &signed,
            &signed,
            &ty(DataType::Float64, true),
            DecimalOverflowPolicy::OutputNull,
            false,
            &CompileControl::default()
        ),
        Err(ArithmeticPrepareError::TypeMismatch)
    );
}

#[test]
fn prepare_original_control_refusal_keeps_every_exact_prefix_on_success_and_ordinary_error() {
    let signed = ty(DataType::Int64, false);
    for result in [ty(DataType::Int64, true), ty(DataType::Int64, false)] {
        let baseline = CompileControl::default();
        let _ = PreparedArithmeticRecipe::try_new(
            ArithmeticOperator::Add,
            &signed,
            &signed,
            &result,
            DecimalOverflowPolicy::ReportError,
            true,
            &baseline,
        );
        let trace = baseline.trace.lock().unwrap().clone();
        assert_eq!(trace[0], 0);
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
                let error = PreparedArithmeticRecipe::try_new(
                    ArithmeticOperator::Add,
                    &signed,
                    &signed,
                    &result,
                    DecimalOverflowPolicy::ReportError,
                    true,
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
fn evaluation_original_control_preserves_all_seven_causes_and_ordinary_error_tail() {
    let recipe = i64_recipe(ArithmeticOperator::Add);
    let left = array(&DataType::Int64, &[Some(3)]);
    let right = array(&DataType::Int64, &[Some(4)]);
    let wrong: ArrayRef = Arc::new(Float64Array::from(vec![4.0]));
    for right in [&right, &wrong] {
        let baseline = Control::default();
        let _ = recipe.evaluate_row(
            EvaluatedArgument::Column(&left),
            0,
            0,
            EvaluatedArgument::Column(right),
            0,
            0,
            &baseline,
        );
        let trace = baseline.trace.lock().unwrap().clone();
        assert_eq!(trace[0], 0);
        assert!(trace.last().copied().unwrap() > 0);
        for at in 0..trace.len() {
            for cause in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
                invalid("control-owned invalid program"),
                internal("control-owned internal failure"),
                KernelFailure::Operational(crate::KernelDiagnostic::new("control-owned operation")),
                KernelFailure::InstanceFailed,
            ] {
                let control = Control {
                    refusal: Some((at, cause.clone())),
                    ..Default::default()
                };
                assert_eq!(
                    recipe.evaluate_row(
                        EvaluatedArgument::Column(&left),
                        0,
                        0,
                        EvaluatedArgument::Column(right),
                        0,
                        0,
                        &control
                    ),
                    Err(cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
