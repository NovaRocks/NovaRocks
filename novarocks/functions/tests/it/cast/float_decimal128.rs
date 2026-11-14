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

//! Actual original Float64 Decimal128 full type/effect and refusal contracts.
use arrow_array::ArrayRef;
use arrow_array::Float64Array;
use arrow_buffer::NullBuffer;
use arrow_schema::DataType;
use novarocks_functions::KernelDiagnostic;
use novarocks_functions::*;
use novarocks_type_contract::CompileControlError;
use novarocks_type_contract::CompilePhase;
use novarocks_type_contract::DecimalOverflowPolicy;
use novarocks_type_contract::ExpressionEffectContext;
use novarocks_type_contract::FunctionValueType;
use novarocks_type_contract::PureCompileControl;
use novarocks_type_contract::{EvaluationDemand, EvaluationDomainId, ExpressionUseId};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Control {
    calls: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        assert!(n <= 256);
        Ok(())
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, n: u32) -> Result<(), KernelFailure> {
        assert!(n <= 256);
        let mut calls = self.calls.lock().unwrap();
        let at = calls.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after original refusal");
        }
        calls.push(n);
        if let Some((stop, cause)) = &self.refusal
            && at == *stop
        {
            return Err(cause.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("float Decimal128 never waits")
    }
}

fn input(values: Vec<Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from(values))
}
fn recipe(p: u8, s: i8, nullable: bool) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::new(DataType::Float64, nullable),
        &FunctionValueType::new(DataType::Decimal128(p, s), true),
        DecimalOverflowPolicy::OutputNull,
        false,
        &Control::default(),
    )
    .unwrap()
}
#[test]
fn float_decimal128_complete_metadata_effect_nullable_and_policy() {
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(1),
        domain: EvaluationDomainId::new(2),
        demand: EvaluationDemand::Value,
    };
    for p in 1..=38 {
        for s in i8::MIN..=p as i8 {
            for nullable in [false, true] {
                for target_nullable in [false, true] {
                    for policy in [
                        DecimalOverflowPolicy::OutputNull,
                        DecimalOverflowPolicy::ReportError,
                    ] {
                        for allow in [false, true] {
                            let source = FunctionValueType::new(DataType::Float64, nullable);
                            let target =
                                FunctionValueType::new(DataType::Decimal128(p, s), target_nullable);
                            let result = PreparedCastRecipe::try_new(
                                CastOperation::Carrier,
                                &source,
                                &target,
                                policy,
                                allow,
                                &Control::default(),
                            );
                            if !target_nullable {
                                assert_eq!(result, Err(CastPrepareError::TypeMismatch));
                                continue;
                            }
                            let r = result.unwrap();
                            assert_eq!(r.source_type(), &source);
                            assert_eq!(r.result_type(), &target);
                            assert_eq!(r.policy(), policy);
                            assert_eq!(r.allow_throw_exception(), allow);
                            assert!(
                                r.own_effects(context)
                                    .for_use(context)
                                    .unwrap()
                                    .may_raise_row_error
                            );
                            assert!(carrier_cast_can_produce_null(
                                source.data_type(),
                                target.data_type(),
                                allow
                            ));
                        }
                    }
                }
            }
        }
    }
    for target in [
        DataType::Decimal128(0, 0),
        DataType::Decimal128(39, 0),
        DataType::Decimal128(1, 2),
    ] {
        assert!(
            FunctionValueType::try_with_logical_type(
                target,
                true,
                novarocks_type_contract::ValueLogicalType::Physical
            )
            .is_err()
        );
    }
    {
        let source = DataType::Float16;
        assert_eq!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &FunctionValueType::from_legacy_declared_projection(source, true),
                &FunctionValueType::new(DataType::Decimal128(18, 4), true),
                DecimalOverflowPolicy::OutputNull,
                false,
                &Control::default()
            ),
            Err(CastPrepareError::Unsupported)
        );
    }
}
#[test]
fn float_decimal128_original_value_static_error_and_hidden_null() {
    let values = input(vec![
        Some(1.25),
        Some(-1.25),
        Some(12.0),
        Some(f64::NAN),
        Some(f64::INFINITY),
        None,
    ]);
    let r = recipe(18, 1, true);
    for (row, expected) in [
        CastRowResult::Decimal128(13),
        CastRowResult::Decimal128(-13),
        CastRowResult::Decimal128(120),
        CastRowResult::Null,
        CastRowResult::Null,
        CastRowResult::Null,
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            r.evaluate_row(
                EvaluatedArgument::Column(&values),
                row,
                row,
                &Control::default()
            )
            .unwrap(),
            expected
        );
    }
    // Original factors precede even NULL; the full short legacy message keeps the selected ordinal.
    let r = recipe(38, -39, true);
    match r
        .evaluate_row(
            EvaluatedArgument::Column(&values),
            5,
            5,
            &Control::default(),
        )
        .unwrap()
    {
        CastRowResult::RowError(error) => {
            assert_eq!(error.selected_ordinal(), 5);
            assert_eq!(
                error.message(),
                "CAST failed: from Float64 to Decimal128(38, -39): decimal scale overflow while casting float to DECIMAL: scale=-39"
            );
        }
        other => panic!("unexpected original factor outcome {other:?}"),
    }
    let finite = input(vec![Some(12.0), Some(1e18)]);
    for allow in [false, true] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let r = PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &FunctionValueType::new(DataType::Float64, false),
                &FunctionValueType::new(DataType::Decimal128(1, 0), true),
                policy,
                allow,
                &Control::default(),
            )
            .unwrap();
            for row in 0..finite.len() {
                let got = r
                    .evaluate_row(
                        EvaluatedArgument::Column(&finite),
                        row,
                        row,
                        &Control::default(),
                    )
                    .unwrap();
                if policy == DecimalOverflowPolicy::OutputNull {
                    assert_eq!(got, CastRowResult::Null);
                } else {
                    match got {
                        CastRowResult::RowError(error) => assert_eq!(
                            error.message(),
                            "Expr evaluate meet error: The numeric type cast involving decimal overflows"
                        ),
                        other => panic!("unexpected original precision outcome {other:?}"),
                    }
                }
            }
        }
    }
    let hidden: ArrayRef = Arc::new(Float64Array::new(
        vec![i128::MIN as f64].into(),
        Some(NullBuffer::from(vec![false])),
    ));
    assert_eq!(
        recipe(38, 0, true)
            .evaluate_row(
                EvaluatedArgument::Column(&hidden),
                0,
                0,
                &Control::default()
            )
            .unwrap(),
        CastRowResult::Null
    );
    assert!(matches!(
        recipe(38, 0, false).evaluate_row(
            EvaluatedArgument::Column(&hidden),
            0,
            0,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    // Extreme scale keeps the existing static-factor error channel even for
    // NULL input; the minimum signed coefficient follows the overflow policy.
    let scale = recipe(38, i8::MIN, true)
        .evaluate_row(
            EvaluatedArgument::Column(&input(vec![None])),
            0,
            0,
            &Control::default(),
        )
        .unwrap();
    let CastRowResult::RowError(error) = scale else {
        panic!("unrepresentable scale must refuse")
    };
    assert_eq!(
        error.message(),
        "CAST failed: from Float64 to Decimal128(38, -128): decimal scale overflow while casting float to DECIMAL: scale=-128"
    );
    let minimum = input(vec![Some(i128::MIN as f64)]);
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let r = PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &FunctionValueType::new(DataType::Float64, true),
            &FunctionValueType::new(DataType::Decimal128(38, 0), true),
            policy,
            false,
            &Control::default(),
        )
        .unwrap();
        let got = r
            .evaluate_row(
                EvaluatedArgument::Column(&minimum),
                0,
                0,
                &Control::default(),
            )
            .unwrap();
        match (policy, got) {
            (DecimalOverflowPolicy::OutputNull, CastRowResult::Null) => {}
            (DecimalOverflowPolicy::ReportError, CastRowResult::RowError(error)) => {
                assert_eq!(error.selected_ordinal(), 0);
                assert_eq!(
                    error.message(),
                    "Expr evaluate meet error: The numeric type cast involving decimal overflows"
                );
            }
            (_, other) => panic!("coefficient overflow policy changed: {other:?}"),
        }
    }
}
#[test]
fn float_decimal128_actual_callbacks_preserve_seven_causes_no_tail() {
    for (p, s, value) in [
        (18, 1, Some(1.25)),
        (18, 1, None),
        (18, 1, Some(f64::NAN)),
        (38, -39, None),
    ] {
        let a = input(vec![value]);
        let r = recipe(p, s, true);
        let baseline = Control::default();
        r.evaluate_row(EvaluatedArgument::Column(&a), 0, 0, &baseline)
            .unwrap();
        let trace = baseline.calls.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
                KernelFailure::InvalidProgram(KernelDiagnostic::new("actual invalid")),
                KernelFailure::Internal(KernelDiagnostic::new("actual internal")),
                KernelFailure::Operational(KernelDiagnostic::new("actual operational")),
                KernelFailure::InstanceFailed,
            ] {
                let control = Control {
                    refusal: Some((at, cause.clone())),
                    ..Default::default()
                };
                assert_eq!(
                    r.evaluate_row(EvaluatedArgument::Column(&a), 0, 0, &control)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*control.calls.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
#[test]
fn float_decimal128_compile_callbacks_preserve_three_causes_no_tail() {
    struct CompileControl {
        calls: Mutex<Vec<(CompilePhase, u32)>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for CompileControl {
        fn checkpoint(&self, phase: CompilePhase, items: u32) -> Result<(), CompileControlError> {
            assert!(items <= 256);
            let mut calls = self.calls.lock().unwrap();
            let at = calls.len();
            if let Some((stop, _)) = self.refusal {
                assert!(at <= stop, "callback after primary compile refusal");
            }
            calls.push((phase, items));
            if let Some((stop, cause)) = self.refusal.clone()
                && at == stop
            {
                return Err(cause);
            }
            Ok(())
        }
    }
    for (source, result, op, expected_error) in [
        (
            DataType::Float64,
            FunctionValueType::new(DataType::Decimal128(38, i8::MIN), true),
            CastOperation::Carrier,
            None,
        ),
        (
            DataType::Float64,
            FunctionValueType::new(DataType::Decimal128(18, 4), false),
            CastOperation::Carrier,
            Some(CastPrepareError::TypeMismatch),
        ),
        (
            DataType::Float32,
            FunctionValueType::new(DataType::Decimal128(18, 4), true),
            CastOperation::Carrier,
            None,
        ),
        (
            DataType::Float64,
            FunctionValueType::new(DataType::Decimal128(18, 4), true),
            CastOperation::Time,
            Some(CastPrepareError::Unsupported),
        ),
    ] {
        let source = FunctionValueType::from_legacy_declared_projection(source, true);
        let baseline = CompileControl {
            calls: Mutex::new(vec![]),
            refusal: None,
        };
        let original = PreparedCastRecipe::try_new(
            op,
            &source,
            &result,
            DecimalOverflowPolicy::ReportError,
            true,
            &baseline,
        );
        // Both floating carriers are admitted; malformed result nullability
        // and the unrelated time operation retain their exact refusal kinds.
        match expected_error {
            Some(expected) => assert_eq!(original, Err(expected)),
            None => {
                let prepared = original.unwrap();
                assert_eq!(prepared.operation(), op);
                assert_eq!(prepared.source_type(), &source);
                assert_eq!(prepared.result_type(), &result);
            }
        }
        let trace = baseline.calls.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = CompileControl {
                    calls: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let failure = PreparedCastRecipe::try_new(
                    op,
                    &source,
                    &result,
                    DecimalOverflowPolicy::ReportError,
                    true,
                    &control,
                )
                .unwrap_err();
                assert_eq!(failure.control_error(), Some(cause));
                assert_eq!(*control.calls.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
