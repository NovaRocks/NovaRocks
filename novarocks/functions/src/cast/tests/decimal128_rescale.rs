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

//! Full Decimal128 raw carrier, exact policy/effects/address and refusal witnesses.
use super::super::*;
use crate::{KernelDiagnostic, SelectedValues, Selection};
use arrow_array::ArrayRef;
use novarocks_type_contract::{EvaluationDemand, EvaluationDomainId, ExpressionUseId};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
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
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after refusal");
        }
        trace.push(n);
        if let Some((stop, cause)) = &self.refusal
            && at == *stop
        {
            return Err(cause.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("Decimal128 CAST never waits")
    }
}
fn input(p: u8, s: i8, values: Vec<Option<i128>>) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(p, s)
            .unwrap(),
    )
}
fn recipe(
    p: u8,
    s: i8,
    tp: u8,
    ts: i8,
    nullable: bool,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::new(DataType::Decimal128(p, s), nullable),
        &FunctionValueType::new(DataType::Decimal128(tp, ts), true),
        policy,
        allow,
        &Control::default(),
    )
    .unwrap()
}
fn modes() -> [(DecimalOverflowPolicy, bool); 4] {
    [
        (DecimalOverflowPolicy::OutputNull, false),
        (DecimalOverflowPolicy::OutputNull, true),
        (DecimalOverflowPolicy::ReportError, false),
        (DecimalOverflowPolicy::ReportError, true),
    ]
}
fn compare(
    r: &PreparedCastRecipe,
    array: &ArrayRef,
    argument: EvaluatedArgument<'_>,
    ordinal: usize,
    row: usize,
    physical: usize,
) {
    let DataType::Decimal128(p, s) = r.result.data_type() else {
        unreachable!()
    };
    let expected = crate::decimal128_rescale::evaluate_legacy(
        &array.slice(physical, 1),
        *p,
        *s,
        r.policy(),
        r.allow_throw_exception(),
    );
    let result = r
        .evaluate_row(argument, ordinal, row, &Control::default())
        .unwrap();
    match expected {
        Ok(out) => {
            let out = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
            assert_eq!(out.data_type(), r.result.data_type());
            assert_eq!(out.is_null(0), matches!(result, CastRowResult::Null));
            if out.is_null(0) {
                assert_eq!(result, CastRowResult::Null);
            } else {
                assert_eq!(result, CastRowResult::Decimal128(out.value(0)));
            }
        }
        Err(message) => {
            let CastRowResult::RowError(error) = result else {
                panic!("missing original Data error: {result:?}")
            };
            assert_eq!(error.selected_ordinal(), ordinal);
            assert_eq!(error.message(), message.to_string());
        }
    }
}

#[test]
fn decimal128_rescale_recipe_full_raw_precision_all_legal_identity_metadata_and_policies() {
    for p in 1..=38 {
        for s in i8::MIN..=p as i8 {
            for (policy, allow) in modes() {
                let r = recipe(p, s, p, s, true, policy, allow);
                assert!(!r.is_identity());
                let a = input(
                    p,
                    s,
                    vec![Some(i128::MIN), Some(i128::MAX), None, Some(1), Some(-1)],
                );
                for row in 0..a.len() {
                    compare(&r, &a, EvaluatedArgument::Column(&a), row, row, row);
                }
            }
        }
    }
}
#[test]
fn decimal128_rescale_recipe_distinct_metadata_rounding_scale_errors_addresses_and_constants() {
    for (p, s, tp, ts) in [
        (7, 2, 9, 3),
        (14, 13, 38, 13),
        (10, 4, 10, 2),
        (18, -2, 20, 0),
        (18, 2, 20, -2),
        (38, 0, 38, 1),
        (38, 0, 38, -39),
    ] {
        for (policy, allow) in modes() {
            let r = recipe(p, s, tp, ts, true, policy, allow);
            let full = input(
                p,
                s,
                vec![
                    Some(0),
                    Some(i128::MAX),
                    None,
                    Some(3185),
                    Some(-3185),
                    Some(1),
                ],
            );
            let a = full.slice(1, 5);
            for row in 0..a.len() {
                compare(&r, &a, EvaluatedArgument::Column(&a), row, row, row);
            }
            let rows = [0, 2, 4];
            let selection = Selection::try_sparse(a.len(), &rows).unwrap();
            let dense: ArrayRef = Arc::new(
                Decimal128Array::from(
                    rows.map(|r| {
                        if a.is_null(r) {
                            None
                        } else {
                            Some(
                                a.as_any()
                                    .downcast_ref::<Decimal128Array>()
                                    .unwrap()
                                    .value(r),
                            )
                        }
                    })
                    .to_vec(),
                )
                .with_precision_and_scale(p, s)
                .unwrap(),
            );
            let selected =
                SelectedValues::try_new(selection, dense.data_type(), dense.clone(), Box::new([]))
                    .unwrap();
            for (ordinal, row) in rows.into_iter().enumerate() {
                compare(
                    &r,
                    &dense,
                    EvaluatedArgument::SelectedColumn(&selected),
                    ordinal,
                    row,
                    ordinal,
                );
            }
            let scalar = input(p, s, vec![Some(1)]);
            for row in [0, 8, 777] {
                compare(&r, &scalar, EvaluatedArgument::Scalar(&scalar), row, row, 0);
            }
        }
    }
}
#[test]
fn decimal128_rescale_recipe_exact_nullable_effects_policy_and_foreign_address_guards() {
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(31),
        domain: EvaluationDomainId::new(4),
        demand: EvaluationDemand::Value,
    };
    for (policy, allow) in modes() {
        for source_nullable in [false, true] {
            for result_nullable in [false, true] {
                let source = FunctionValueType::new(DataType::Decimal128(7, 2), source_nullable);
                let result = FunctionValueType::new(DataType::Decimal128(9, 3), result_nullable);
                let out = PreparedCastRecipe::try_new(
                    CastOperation::Carrier,
                    &source,
                    &result,
                    policy,
                    allow,
                    &Control::default(),
                );
                if (source_nullable || crate::decimal128_rescale::can_produce_null(policy, allow))
                    && !result_nullable
                {
                    assert_eq!(out, Err(CastPrepareError::TypeMismatch));
                    continue;
                }
                let r = out.unwrap();
                assert_eq!(r.source_type(), &source);
                assert_eq!(r.result_type(), &result);
                assert_eq!(r.policy(), policy);
                assert_eq!(r.allow_throw_exception(), allow);
                assert_eq!(
                    r.own_effects(context)
                        .for_use(context)
                        .unwrap()
                        .may_raise_row_error,
                    policy == DecimalOverflowPolicy::ReportError || allow
                );
                assert_eq!(
                    carrier_cast_can_produce_null_with_policy(
                        source.data_type(),
                        result.data_type(),
                        policy,
                        allow
                    ),
                    policy == DecimalOverflowPolicy::OutputNull && !allow
                );
            }
        }
    }
    let r = recipe(7, 2, 9, 3, false, DecimalOverflowPolicy::ReportError, false);
    let null = input(7, 2, vec![None]);
    assert!(matches!(
        r.evaluate_row(EvaluatedArgument::Column(&null), 0, 0, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let foreign = input(9, 3, vec![Some(1)]);
    assert!(matches!(
        r.evaluate_row(
            EvaluatedArgument::Column(&foreign),
            0,
            0,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let source = input(7, 2, vec![Some(1)]);
    assert!(matches!(
        r.evaluate_row(
            EvaluatedArgument::Column(&source),
            0,
            1,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("original internal")),
        KernelFailure::Operational(KernelDiagnostic::new("original operational")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn decimal128_rescale_recipe_every_runtime_callback_preserves_seven_original_causes_no_footer() {
    for (p, s, tp, ts, value, policy, allow) in [
        (
            7,
            2,
            9,
            3,
            Some(12000),
            DecimalOverflowPolicy::OutputNull,
            false,
        ),
        (
            1,
            0,
            1,
            0,
            Some(i128::MAX),
            DecimalOverflowPolicy::ReportError,
            false,
        ),
        (
            38,
            0,
            38,
            -39,
            Some(1),
            DecimalOverflowPolicy::OutputNull,
            false,
        ),
        (7, 2, 9, 3, None, DecimalOverflowPolicy::ReportError, true),
    ] {
        let r = recipe(p, s, tp, ts, true, policy, allow);
        let a = input(p, s, vec![value]);
        let recorder = Control::default();
        r.evaluate_row(EvaluatedArgument::Column(&a), 0, 0, &recorder)
            .unwrap();
        let trace = recorder.trace.lock().unwrap().clone();
        for stop in 0..trace.len() {
            for cause in causes() {
                let c = Control {
                    trace: Mutex::new(Vec::new()),
                    refusal: Some((stop, cause.clone())),
                };
                assert_eq!(
                    r.evaluate_row(EvaluatedArgument::Column(&a), 0, 0, &c),
                    Err(cause)
                );
                assert_eq!(*c.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}
#[test]
fn decimal128_rescale_original_core_large_work_observation_and_host_refusal_keeps_primary() {
    let array = input(7, 2, vec![Some(1); 1001]);
    fn run(array: &ArrayRef, control: &Control) -> Result<ArrayRef, KernelFailure> {
        let mut work = EvaluationCheckpoints::new(control);
        let mut observer = |event| match event {
            crate::decimal128_rescale::DecimalRescaleObservation::Step => work.step(),
            crate::decimal128_rescale::DecimalRescaleObservation::OpaqueBoundary => work.flush(),
        };
        match crate::decimal128_rescale::evaluate_observed(
            array,
            9,
            3,
            DecimalOverflowPolicy::OutputNull,
            false,
            &mut observer,
        ) {
            Ok(array) => {
                work.finish()?;
                Ok(array)
            }
            Err(crate::decimal128_rescale::DecimalRescaleError::Host(cause)) => Err(cause),
            Err(crate::decimal128_rescale::DecimalRescaleError::Domain(message)) => {
                panic!("valid original input failed: {message}")
            }
        }
    }
    let recorder = Control::default();
    assert_eq!(run(&array, &recorder).unwrap().len(), 1001);
    let trace = recorder.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    for stop in 0..trace.len() {
        for cause in causes() {
            let c = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((stop, cause.clone())),
            };
            assert!(matches!(run(&array,&c),Err(actual) if actual==cause));
            assert_eq!(*c.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}
#[test]
fn decimal128_rescale_prepare_every_callback_preserves_three_compile_causes_no_footer() {
    struct CompileControl {
        trace: Mutex<Vec<u32>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for CompileControl {
        fn checkpoint(&self, _: CompilePhase, n: u32) -> Result<(), CompileControlError> {
            assert!(n <= 256);
            let mut t = self.trace.lock().unwrap();
            let at = t.len();
            if let Some((stop, _)) = self.refusal {
                assert!(at <= stop);
            }
            t.push(n);
            if let Some((stop, cause)) = self.refusal.clone()
                && at == stop
            {
                return Err(cause);
            }
            Ok(())
        }
    }
    let source = FunctionValueType::new(DataType::Decimal128(7, 2), true);
    let target = FunctionValueType::new(DataType::Decimal128(9, 3), true);
    let recorder = CompileControl {
        trace: Mutex::new(Vec::new()),
        refusal: None,
    };
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &source,
        &target,
        DecimalOverflowPolicy::OutputNull,
        false,
        &recorder,
    )
    .unwrap();
    let trace = recorder.trace.lock().unwrap().clone();
    for stop in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let c = CompileControl {
                trace: Mutex::new(Vec::new()),
                refusal: Some((stop, cause.clone())),
            };
            let error = PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &source,
                &target,
                DecimalOverflowPolicy::OutputNull,
                false,
                &c,
            )
            .unwrap_err();
            assert_eq!(error.control_error(), Some(cause));
            assert_eq!(*c.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}

#[test]
fn decimal128_rescale_extreme_delta_is_exact_and_never_overflows_i8() {
    for (policy, allow) in modes() {
        let r = recipe(38, 38, 38, i8::MIN, true, policy, allow);
        let values = input(38, 38, vec![Some(i128::MIN), Some(i128::MAX), Some(1)]);
        for row in 0..values.len() {
            assert_eq!(
                r.evaluate_row(
                    EvaluatedArgument::Column(&values),
                    row,
                    row,
                    &Control::default()
                )
                .unwrap(),
                CastRowResult::Decimal128(0)
            );
        }
        let hidden: ArrayRef = Arc::new(
            Decimal128Array::new(
                vec![i128::MIN].into(),
                Some(arrow_buffer::NullBuffer::from(vec![false])),
            )
            .with_precision_and_scale(38, 38)
            .unwrap(),
        );
        assert_eq!(
            r.evaluate_row(
                EvaluatedArgument::Column(&hidden),
                0,
                0,
                &Control::default()
            )
            .unwrap(),
            CastRowResult::Null
        );
    }
}

#[test]
fn decimal128_rescale_single_pow_option_and_original_result_projection() {
    for exp in [0, 1, 2, 18, 38] {
        assert_eq!(
            crate::legacy_decimal::checked_pow10_i128(exp),
            Some(10_i128.pow(exp as u32))
        );
        assert_eq!(
            crate::legacy_decimal::pow10_i128(exp).unwrap(),
            10_i128.pow(exp as u32)
        );
    }
    for exp in [39, 128, u32::MAX as usize] {
        assert_eq!(crate::legacy_decimal::checked_pow10_i128(exp), None);
        assert_eq!(
            crate::legacy_decimal::pow10_i128(exp),
            Err(crate::legacy_decimal::DecimalMathFailure::Power {
                exponent: exp,
                bits: 128
            })
        );
    }
}
