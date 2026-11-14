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

use super::super::PreparedArithmeticRecipe;
use super::*;
use crate::{
    ConstantPolicy, ConstantPool, EvaluatedArgument, KernelDiagnostic, KernelEvaluationControl,
    SelectedValues, Selection,
};
use arrow_array::{
    ArrayRef, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    builder::FixedSizeBinaryBuilder,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand, EvaluationDomainId,
    ExpressionEffectContext, ExpressionUseId, PureCompileControl,
    arithmetic_result_value_type_with_op,
};
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
        if let Some((index, _)) = self.refusal {
            assert!(trace.len() <= index);
        }
        let index = trace.len();
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((at, cause)) if at == index => Err(cause),
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
        if let Some((index, _)) = &self.refusal {
            assert!(trace.len() <= *index);
        }
        let index = trace.len();
        trace.push(units);
        match &self.refusal {
            Some((at, cause)) if *at == index => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("integer arithmetic must not wait")
    }
}
fn large(nullable: bool) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        nullable,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(77),
        domain: EvaluationDomainId::new(3),
        demand: EvaluationDemand::Value,
    }
}
fn array(ty: &FunctionValueType, values: &[Option<i128>]) -> ArrayRef {
    macro_rules! signed {
        ($array:ty, $native:ty) => {
            Arc::new(<$array>::from(
                values
                    .iter()
                    .map(|value| value.map(|value| <$native>::try_from(value).unwrap()))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        };
    }
    match ty.data_type() {
        DataType::FixedSizeBinary(16) => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
            for value in values {
                match value {
                    Some(value) => builder.append_value(value.to_be_bytes()).unwrap(),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::Int8 => signed!(Int8Array, i8),
        DataType::Int16 => signed!(Int16Array, i16),
        DataType::Int32 => signed!(Int32Array, i32),
        DataType::Int64 => signed!(Int64Array, i64),
        _ => panic!("explicit fixture requires a signed integer carrier"),
    }
}
fn recipe(
    op: ArithmeticOperator,
    left: &FunctionValueType,
    right: &FunctionValueType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedArithmeticRecipe {
    let mut result = arithmetic_result_value_type_with_op(left, right, op).unwrap();
    result = result
        .with_nullable(true)
        .expect("valid function nullability declaration");
    PreparedArithmeticRecipe::try_new(
        op,
        left,
        right,
        &result,
        policy,
        allow,
        &CompileControl::default(),
    )
    .unwrap()
}
fn eval(
    recipe: &PreparedArithmeticRecipe,
    left: &ArrayRef,
    left_row: usize,
    right: &ArrayRef,
    right_row: usize,
    control: &dyn KernelEvaluationControl,
) -> Result<ArithmeticRowResult, KernelFailure> {
    recipe.evaluate_row(
        EvaluatedArgument::Column(left),
        left_row,
        left_row,
        EvaluatedArgument::Column(right),
        right_row,
        right_row,
        control,
    )
}
fn policies() -> [DecimalOverflowPolicy; 2] {
    [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ]
}

#[test]
fn all_forty_five_integral_largeint_profiles_preserve_ordered_widths_policies_and_pure_effects() {
    use ArithmeticOperator::*;
    let types = [
        FunctionValueType::new(DataType::Int8, false),
        FunctionValueType::new(DataType::Int16, false),
        FunctionValueType::new(DataType::Int32, false),
        FunctionValueType::new(DataType::Int64, false),
        large(false),
    ];
    let mut profiles = 0;
    for (li, left) in types.iter().enumerate() {
        for (ri, right) in types.iter().enumerate() {
            if li != 4 && ri != 4 {
                continue;
            }
            for op in [Add, Subtract, Multiply, Divide, Modulo] {
                profiles += 1;
                for policy in policies() {
                    for allow in [false, true] {
                        let prepared = recipe(op, left, right, policy, allow);
                        assert_eq!(prepared.left_type(), left);
                        assert_eq!(prepared.right_type(), right);
                        assert_eq!(
                            prepared.result_type(),
                            &if op == Divide {
                                FunctionValueType::new(DataType::Float64, true)
                            } else {
                                large(true)
                            }
                        );
                        assert_eq!(prepared.operator(), op);
                        assert_eq!(prepared.decimal_overflow_policy(), policy);
                        assert_eq!(prepared.allow_throw_exception(), allow);
                        assert_eq!(
                            prepared.own_effects(context()).for_use(context()).unwrap(),
                            ExpressionEffects::PURE_VALUE
                        );
                        assert_eq!(
                            eval(
                                &prepared,
                                &array(left, &[Some(-7)]),
                                0,
                                &array(right, &[Some(3)]),
                                0,
                                &Control::default()
                            )
                            .unwrap(),
                            match op {
                                Add => ArithmeticRowResult::LargeInt(-4),
                                Subtract => ArithmeticRowResult::LargeInt(-10),
                                Multiply => ArithmeticRowResult::LargeInt(-21),
                                Divide => ArithmeticRowResult::Float(-7.0 / 3.0),
                                Modulo => ArithmeticRowResult::LargeInt(-1),
                            }
                        );
                    }
                }
            }
        }
    }
    assert_eq!(profiles, 45);
}

#[test]
fn largeint_wrap_extrema_and_zero_modulo_do_not_become_checked_signed_row_errors() {
    use ArithmeticOperator::*;
    let ty = large(true);
    for (op, lhs, rhs, expected) in [
        (Add, i128::MAX, 1, Some(i128::MIN)),
        (Subtract, i128::MIN, 1, Some(i128::MAX)),
        (Multiply, i128::MIN, -1, Some(i128::MIN)),
        (Multiply, i128::MAX, 2, Some(-2)),
        (Modulo, i128::MIN, -1, Some(0)),
        (Modulo, -7, 3, Some(-1)),
        (Modulo, -7, -3, Some(-1)),
        (Modulo, 7, -3, Some(1)),
        (Modulo, i128::MAX, 0, None),
    ] {
        for policy in policies() {
            for allow in [false, true] {
                let prepared = recipe(op, &ty, &ty, policy, allow);
                assert_eq!(
                    eval(
                        &prepared,
                        &array(&ty, &[Some(lhs)]),
                        0,
                        &array(&ty, &[Some(rhs)]),
                        0,
                        &Control::default()
                    )
                    .unwrap(),
                    expected.map_or(ArithmeticRowResult::Null, ArithmeticRowResult::LargeInt)
                );
            }
        }
    }
    // The actual signed-width extension must preserve its sign and exact bound.
    for (carrier, minimum, maximum) in [
        (DataType::Int8, i128::from(i8::MIN), i128::from(i8::MAX)),
        (DataType::Int16, i128::from(i16::MIN), i128::from(i16::MAX)),
        (DataType::Int32, i128::from(i32::MIN), i128::from(i32::MAX)),
        (DataType::Int64, i128::from(i64::MIN), i128::from(i64::MAX)),
    ] {
        let signed = FunctionValueType::from_legacy_declared_projection(carrier, false);
        for number in [minimum, maximum] {
            for reversed in [false, true] {
                let (left, right, a, b) = if reversed {
                    (
                        &signed,
                        &ty,
                        array(&signed, &[Some(number)]),
                        array(&ty, &[Some(0)]),
                    )
                } else {
                    (
                        &ty,
                        &signed,
                        array(&ty, &[Some(0)]),
                        array(&signed, &[Some(number)]),
                    )
                };
                assert_eq!(
                    eval(
                        &recipe(Add, left, right, DecimalOverflowPolicy::ReportError, true),
                        &a,
                        0,
                        &b,
                        0,
                        &Control::default()
                    )
                    .unwrap(),
                    ArithmeticRowResult::LargeInt(number)
                );
            }
        }
    }
}

#[test]
fn legal_largeint_division_uses_signed_ties_even_float_conversion_and_fractional_result() {
    let ty = large(false);
    let prepared = recipe(
        ArithmeticOperator::Divide,
        &ty,
        &ty,
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    // Independent IEEE bit fixtures from integer significand/round-to-even
    // calculation, not the implementation's conversion or legacy failed DIV.
    for (input, expected_bits) in [
        (0, 0x0000_0000_0000_0000),
        (1, 0x3ff0_0000_0000_0000),
        (-1, 0xbff0_0000_0000_0000),
        ((1_i128 << 53) + 1, 0x4340_0000_0000_0000),
        ((1_i128 << 53) + 3, 0x4340_0000_0000_0002),
        (-((1_i128 << 53) + 1), 0xc340_0000_0000_0000),
        (i128::MIN, 0xc7e0_0000_0000_0000),
        (i128::MAX, 0x47e0_0000_0000_0000),
    ] {
        let ArithmeticRowResult::Float(value) = eval(
            &prepared,
            &array(&ty, &[Some(input)]),
            0,
            &array(&ty, &[Some(1)]),
            0,
            &Control::default(),
        )
        .unwrap() else {
            panic!("fractional profile must return F64")
        };
        assert_eq!(value.to_bits(), expected_bits);
        assert!(value.is_finite());
    }
    for (lhs, rhs, expected) in [
        (i128::MIN, -1, 0x47e0_0000_0000_0000),
        (-1, 2, 0xbfe0_0000_0000_0000),
        (1, 2, 0x3fe0_0000_0000_0000),
        (0, -1, 0x8000_0000_0000_0000),
        (1, i128::MAX, 0x3800_0000_0000_0000),
    ] {
        let ArithmeticRowResult::Float(value) = eval(
            &prepared,
            &array(&ty, &[Some(lhs)]),
            0,
            &array(&ty, &[Some(rhs)]),
            0,
            &Control::default(),
        )
        .unwrap() else {
            panic!("fractional profile must return F64")
        };
        assert_eq!(value.to_bits(), expected);
    }
    assert_eq!(
        eval(
            &prepared,
            &array(&ty, &[Some(i128::MAX)]),
            0,
            &array(&ty, &[Some(0)]),
            0,
            &Control::default()
        )
        .unwrap(),
        ArithmeticRowResult::Null
    );
}

fn constant() -> crate::ConstantValue {
    let ty = large(true);
    let data = array(&ty, &[Some(i128::MIN), Some(7), None]).to_data();
    ConstantPool::try_new(
        Arc::new(ty.try_to_field("authored-largeint").unwrap()),
        ty,
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
            max_library_validation_bytes: 8192,
        },
        CompilePhase::Validate,
        &CompileControl::default(),
    )
    .unwrap()
    .value(1)
    .unwrap()
}
#[test]
fn independently_selected_scalar_compact_column_pool_and_sliced_be_addresses_are_preserved() {
    let ty = large(true);
    let prepared = recipe(
        ArithmeticOperator::Add,
        &ty,
        &ty,
        DecimalOverflowPolicy::ReportError,
        true,
    );
    let value = constant();
    let rows = [1, 4];
    let selected = Selection::try_sparse(5, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selected,
        ty.data_type(),
        array(&ty, &[Some(2), Some(-3)]),
        Box::new([]),
    )
    .unwrap();
    for (ordinal, row, expected) in [(0, 1, 9), (1, 4, 4)] {
        assert_eq!(
            prepared
                .evaluate_row(
                    EvaluatedArgument::Constant(&value),
                    ordinal,
                    row,
                    EvaluatedArgument::SelectedColumn(&compact),
                    ordinal,
                    row,
                    &Control::default()
                )
                .unwrap(),
            ArithmeticRowResult::LargeInt(expected)
        );
    }
    let scalar = array(&ty, &[Some(-3)]);
    let column = array(
        &ty,
        &[Some(i128::MIN), Some(5), None, Some(i128::MAX), Some(9)],
    );
    assert_eq!(
        prepared
            .evaluate_row(
                EvaluatedArgument::Scalar(&scalar),
                1,
                4,
                EvaluatedArgument::Column(&column),
                1,
                4,
                &Control::default()
            )
            .unwrap(),
        ArithmeticRowResult::LargeInt(6)
    );
    let sliced = array(&ty, &[Some(i128::MIN), Some(-17), Some(23), None]).slice(1, 2);
    assert_eq!(
        eval(&prepared, &sliced, 1, &sliced, 0, &Control::default()).unwrap(),
        ArithmeticRowResult::LargeInt(6)
    );
    assert!(matches!(
        prepared.evaluate_row(
            EvaluatedArgument::Constant(&value),
            0,
            4,
            EvaluatedArgument::SelectedColumn(&compact),
            0,
            4,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn exact_domain_class_null_and_result_checks_precede_any_numeric_admission() {
    let ty = large(true);
    let prepared = recipe(
        ArithmeticOperator::Modulo,
        &ty,
        &ty,
        DecimalOverflowPolicy::ReportError,
        true,
    );
    assert_eq!(
        eval(
            &prepared,
            &array(&ty, &[None]),
            0,
            &array(&ty, &[Some(0)]),
            0,
            &Control::default()
        )
        .unwrap(),
        ArithmeticRowResult::Null
    );
    let wrong: ArrayRef = Arc::new(Float64Array::from(vec![None]));
    assert!(matches!(
        eval(
            &prepared,
            &array(&ty, &[None]),
            0,
            &wrong,
            0,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert!(matches!(
        eval(
            &prepared,
            &array(&ty, &[Some(1)]),
            1,
            &array(&ty, &[Some(1)]),
            0,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    for domain in [ValueLogicalType::Physical, ValueLogicalType::Uuid] {
        let foreign =
            FunctionValueType::try_with_logical_type(DataType::FixedSizeBinary(16), true, domain)
                .unwrap();
        let control = CompileControl::default();
        let mut work =
            CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
        assert!(
            LargeIntArithmetic::prepare(ArithmeticOperator::Add, &foreign, &ty, &mut work)
                .unwrap()
                .is_none()
        );
        work.finish().unwrap();
    }
    for result in [
        large(false),
        FunctionValueType::new(DataType::FixedSizeBinary(16), true),
        FunctionValueType::new(DataType::Int64, true),
    ] {
        assert_eq!(
            PreparedArithmeticRecipe::try_new(
                ArithmeticOperator::Add,
                &ty,
                &ty,
                &result,
                DecimalOverflowPolicy::OutputNull,
                false,
                &CompileControl::default()
            )
            .unwrap_err(),
            ArithmeticPrepareError::TypeMismatch
        );
    }
    let nonnullable = large(false);
    let prepared = recipe(
        ArithmeticOperator::Add,
        &nonnullable,
        &ty,
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    assert!(matches!(
        eval(
            &prepared,
            &array(&ty, &[None]),
            0,
            &array(&ty, &[Some(1)]),
            0,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn actual_largeint_prepare_callbacks_keep_three_primary_compile_causes_and_exact_prefix() {
    let ty = large(true);
    let recorder = CompileControl::default();
    PreparedArithmeticRecipe::try_new(
        ArithmeticOperator::Add,
        &ty,
        &ty,
        &ty,
        DecimalOverflowPolicy::ReportError,
        true,
        &recorder,
    )
    .unwrap();
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.iter().any(|(_, units)| *units > 0));
    for index in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = CompileControl {
                refusal: Some((index, cause.clone())),
                ..Default::default()
            };
            let error = PreparedArithmeticRecipe::try_new(
                ArithmeticOperator::Add,
                &ty,
                &ty,
                &ty,
                DecimalOverflowPolicy::ReportError,
                true,
                &control,
            )
            .unwrap_err();
            assert_eq!(error.control_error(), Some(cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=index]);
        }
    }
}
fn failures() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("control invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("control internal")),
        KernelFailure::Operational(KernelDiagnostic::new("control operational")),
        KernelFailure::InstanceFailed,
    ]
}
fn observed_non_null_rows(control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure> {
    let ty = large(false);
    let compile_control = CompileControl::default();
    let mut compile_work =
        CompileCheckpoints::try_new(&compile_control, CompilePhase::FunctionSpecialization)
            .unwrap();
    let recipe = LargeIntArithmetic::prepare(ArithmeticOperator::Add, &ty, &ty, &mut compile_work)
        .unwrap()
        .unwrap();
    compile_work.finish().unwrap();
    let left = array(&ty, &vec![Some(i128::MAX); 320]);
    let right = array(&ty, &vec![Some(1); 320]);
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        for ordinal in 0..320 {
            let result = recipe.evaluate_non_null(
                left.as_ref(),
                ordinal,
                right.as_ref(),
                319 - ordinal,
                ordinal,
                &mut work,
            )?;
            assert_eq!(result, ArithmeticRowResult::LargeInt(i128::MIN));
        }
        Ok(())
    })();
    work.finish()?;
    result
}
#[test]
fn entry_quantum_tail_and_actual_row_callbacks_preserve_all_seven_failures_without_rechecking() {
    let recorder = Control::default();
    observed_non_null_rows(&recorder).unwrap();
    let wide_trace = recorder.trace.lock().unwrap().clone();
    assert_eq!(wide_trace[0], 0);
    assert!(wide_trace.contains(&256));
    assert!(
        wide_trace
            .last()
            .is_some_and(|units| *units > 0 && *units < 256)
    );
    for index in 0..wide_trace.len() {
        for cause in failures() {
            let control = Control {
                refusal: Some((index, cause.clone())),
                ..Default::default()
            };
            assert_eq!(observed_non_null_rows(&control), Err(cause));
            assert_eq!(*control.trace.lock().unwrap(), wide_trace[..=index]);
        }
    }
    let ty = large(false);
    let prepared = recipe(
        ArithmeticOperator::Divide,
        &ty,
        &ty,
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let left = array(&ty, &[Some(i128::MIN)]);
    let right = array(&ty, &[Some(-1)]);
    let recorder = Control::default();
    eval(&prepared, &left, 0, &right, 0, &recorder).unwrap();
    let trace = recorder.trace.lock().unwrap().clone();
    for index in 0..trace.len() {
        for cause in failures() {
            let control = Control {
                refusal: Some((index, cause.clone())),
                ..Default::default()
            };
            assert_eq!(eval(&prepared, &left, 0, &right, 0, &control), Err(cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=index]);
        }
    }
}
