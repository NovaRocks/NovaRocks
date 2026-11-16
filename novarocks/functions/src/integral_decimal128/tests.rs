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

//! Exact original value/profile/constant/address and seven typed refusal witnesses.
use super::*;
use crate::{
    CastOperation, CastPrepareError, CastRowResult, ConstantPolicy, ConstantPool,
    EvaluatedArgument, EvaluationCheckpoints, KernelDiagnostic, KernelEvaluationControl,
    KernelFailure, PreparedCastRecipe, SelectedValues, Selection,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, EvaluationDemand, EvaluationDomainId,
    ExpressionEffectContext, ExpressionUseId, FunctionValueType, PureCompileControl,
};
use std::{sync::Mutex, time::Duration};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    stop: Option<(usize, KernelFailure)>,
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
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        if let Some((stop, _)) = &self.stop {
            assert!(at <= *stop, "no callback after first refusal");
        }
        t.push(n);
        if let Some((stop, e)) = &self.stop
            && at == *stop
        {
            return Err(e.clone());
        }
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("integral Decimal CAST never waits")
    }
}
fn input(dtype: &DataType, values: Vec<Option<i64>>) -> ArrayRef {
    macro_rules! array {
        ($a:ty,$v:ty) => {
            Arc::new(<$a>::from(
                values
                    .into_iter()
                    .map(|v| v.map(|n| <$v>::try_from(n).unwrap()))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        };
    }
    match dtype {
        DataType::Int8 => array!(Int8Array, i8),
        DataType::Int16 => array!(Int16Array, i16),
        DataType::Int32 => array!(Int32Array, i32),
        DataType::Int64 => Arc::new(Int64Array::from(values)),
        _ => panic!("actual signed carrier"),
    }
}
fn recipe(
    dtype: &DataType,
    p: u8,
    s: i8,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::from_legacy_declared_projection(dtype.clone(), true),
        &FunctionValueType::new(DataType::Decimal128(p, s), true),
        policy,
        allow,
        &Control::default(),
    )
    .unwrap()
}
fn compare(
    recipe: &PreparedCastRecipe,
    array: &ArrayRef,
    arg: EvaluatedArgument<'_>,
    ordinal: usize,
    row: usize,
    physical: usize,
) {
    let DataType::Decimal128(p, s) = recipe.result_type().data_type() else {
        unreachable!()
    };
    let expected = evaluate_legacy(
        &array.slice(physical, 1),
        *p,
        *s,
        recipe.policy(),
        recipe.allow_throw_exception(),
    );
    let out = recipe
        .evaluate_row(arg, ordinal, row, &Control::default())
        .unwrap();
    match expected {
        Ok(a) => {
            let a = a.as_any().downcast_ref::<Decimal128Array>().unwrap();
            assert_eq!(a.is_null(0), matches!(out, CastRowResult::Null));
            if !a.is_null(0) {
                assert_eq!(out, CastRowResult::Decimal128(a.value(0)));
            }
        }
        Err(message) => {
            let CastRowResult::RowError(error) = out else {
                panic!("original data error lost")
            };
            assert_eq!(error.selected_ordinal(), ordinal);
            assert_eq!(error.message(), message.to_string());
        }
    }
}
#[test]
fn integral_decimal128_recipe_all_widths_scales_policy_and_original_precision() {
    for dtype in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        let (min, max) = endpoints(&dtype).unwrap();
        for (p, s) in [
            (4, 0),
            (7, 2),
            (7, -2),
            (38, -38),
            (38, 38),
            (18, 1),
            (19, 1),
            (38, 1),
        ] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let r = recipe(&dtype, p, s, policy, allow);
                    let a = input(
                        &dtype,
                        vec![Some(min as i64), None, Some(max as i64), Some(-1), Some(1)],
                    );
                    for row in 0..a.len() {
                        compare(&r, &a, EvaluatedArgument::Column(&a), row, row, row);
                    }
                }
            }
        }
    }
}
#[test]
fn integral_decimal128_recipe_exact_column_slice_selected_scalar_and_pool_ordinal() {
    for dtype in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        let r = recipe(&dtype, 4, 0, DecimalOverflowPolicy::OutputNull, true);
        let a = input(
            &dtype,
            vec![Some(99), Some(-100), None, Some(100), Some(0), Some(99)],
        )
        .slice(1, 4);
        let rows = [0, 2];
        let selection = Selection::try_sparse(4, &rows).unwrap();
        let dense = input(&dtype, vec![Some(-100), Some(100)]);
        let selected =
            SelectedValues::try_new(selection, &dtype, dense.clone(), Box::default()).unwrap();
        for (ordinal, row) in rows.into_iter().enumerate() {
            compare(
                &r,
                &dense,
                EvaluatedArgument::SelectedColumn(&selected),
                ordinal,
                row,
                ordinal,
            );
            compare(&r, &a, EvaluatedArgument::Column(&a), ordinal, row, row);
        }
        let scalar = input(&dtype, vec![Some(71)]);
        compare(&r, &scalar, EvaluatedArgument::Scalar(&scalar), 8, 777, 0);
        let backing = input(&dtype, vec![None, Some(71), Some(99)]);
        let ty = FunctionValueType::from_legacy_declared_projection(dtype.clone(), true);
        let pool = ConstantPool::try_new(
            Arc::new(ty.try_to_field("original-integral-pool").unwrap()),
            ty,
            backing.to_data(),
            ConstantPolicy {
                max_rows: 8,
                max_array_nodes: 8,
                max_logical_elements: 8,
                max_retained_buffer_bytes: 4096,
                max_type_depth: 8,
                max_type_nodes: 8,
                max_dictionary_depth: 8,
                max_metadata_bytes: 4096,
                max_library_validation_work: 4096,
                // Original three-element CAST fixtures admit the diagnostic
                // reserve plus actual ArrayData/backing with this finite bound.
                max_library_validation_bytes: 8192,
            },
            CompilePhase::Validate,
            &Control::default(),
        )
        .unwrap();
        let facts = pool.resource_facts();
        assert!(facts.library_validation_bytes_upper_bound > 4096);
        assert!(facts.library_validation_bytes_upper_bound <= 8192);
        let value = pool.value(1).unwrap();
        compare(&r, &backing, EvaluatedArgument::Constant(&value), 0, 777, 1);
    }
}
#[test]
fn integral_decimal128_recipe_exact_policy_null_effects_and_wide_negative_scale_values() {
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(31),
        domain: EvaluationDomainId::new(4),
        demand: EvaluationDemand::Value,
    };
    for dtype in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                let source =
                    FunctionValueType::from_legacy_declared_projection(dtype.clone(), false);
                let target = FunctionValueType::new(DataType::Decimal128(4, 0), false);
                let out = PreparedCastRecipe::try_new(
                    CastOperation::Carrier,
                    &source,
                    &target,
                    policy,
                    allow,
                    &Control::default(),
                );
                let overflow = dtype != DataType::Int8;
                if policy == DecimalOverflowPolicy::OutputNull && overflow {
                    assert_eq!(out, Err(CastPrepareError::TypeMismatch));
                } else {
                    let r = out.unwrap();
                    assert_eq!(r.source_type(), &source);
                    assert_eq!(r.result_type(), &target);
                    assert_eq!(r.policy(), policy);
                    assert_eq!(r.allow_throw_exception(), allow);
                    assert_eq!(
                        r.own_effects(context)
                            .for_use(context)
                            .unwrap()
                            .may_raise_row_error,
                        policy == DecimalOverflowPolicy::ReportError && overflow
                    );
                }
                assert_eq!(
                    crate::carrier_cast_can_produce_null_with_policy(
                        &dtype,
                        target.data_type(),
                        policy,
                        allow
                    ),
                    policy == DecimalOverflowPolicy::OutputNull && overflow
                );
            }
        }
        for scale in [-39, i8::MIN] {
            let prepared = PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &FunctionValueType::from_legacy_declared_projection(dtype.clone(), true),
                &FunctionValueType::new(DataType::Decimal128(38, scale), true),
                DecimalOverflowPolicy::OutputNull,
                false,
                &Control::default(),
            )
            .unwrap();
            let (minimum, maximum) = match dtype {
                DataType::Int8 => (i8::MIN as i64, i8::MAX as i64),
                DataType::Int16 => (i16::MIN as i64, i16::MAX as i64),
                DataType::Int32 => (i32::MIN as i64, i32::MAX as i64),
                DataType::Int64 => (i64::MIN, i64::MAX),
                _ => unreachable!("signed test carrier"),
            };
            let array = input(&dtype, vec![Some(minimum), Some(0), Some(maximum), None]);
            for row in 0..3 {
                assert_eq!(
                    prepared
                        .evaluate_row(
                            EvaluatedArgument::Column(&array),
                            row,
                            row,
                            &Control::default()
                        )
                        .unwrap(),
                    CastRowResult::Decimal128(0)
                );
            }
            assert_eq!(
                prepared
                    .evaluate_row(EvaluatedArgument::Column(&array), 3, 3, &Control::default())
                    .unwrap(),
                CastRowResult::Null
            );
        }
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("integral-origin-invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("integral-origin-internal")),
        KernelFailure::Operational(KernelDiagnostic::new("integral-origin-operational")),
        KernelFailure::InstanceFailed,
    ]
}
#[test]
fn integral_decimal128_recipe_every_callback_seven_causes_no_footer_or_tail() {
    for (p, s, value, policy) in [
        (4, 0, Some(150), DecimalOverflowPolicy::OutputNull),
        (3, 0, Some(1000), DecimalOverflowPolicy::ReportError),
        (38, 38, Some(i64::MAX), DecimalOverflowPolicy::OutputNull),
        (4, 0, None, DecimalOverflowPolicy::OutputNull),
    ] {
        let r = recipe(&DataType::Int64, p, s, policy, true);
        let a = input(&DataType::Int64, vec![value]);
        let record = Control::default();
        r.evaluate_row(EvaluatedArgument::Column(&a), 0, 0, &record)
            .unwrap();
        let trace = record.trace.lock().unwrap().clone();
        for stop in 0..trace.len() {
            for cause in causes() {
                let c = Control {
                    trace: Mutex::new(vec![]),
                    stop: Some((stop, cause.clone())),
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
fn integral_decimal128_observed_original_capacity_and_large_work_refusal_is_typed() {
    fn run(a: &ArrayRef, c: &Control) -> Result<ArrayRef, KernelFailure> {
        let mut work = EvaluationCheckpoints::new(c);
        let mut observe = |event| match event {
            DecimalRescaleObservation::Step => work.step(),
            DecimalRescaleObservation::OpaqueBoundary => work.flush(),
        };
        match evaluate_observed(
            a,
            7,
            2,
            DecimalOverflowPolicy::OutputNull,
            true,
            &mut observe,
        ) {
            Ok(a) => {
                work.finish()?;
                Ok(a)
            }
            Err(DecimalRescaleError::Host(cause)) => Err(cause),
            Err(DecimalRescaleError::Domain(message)) => {
                panic!("original success fixture failed: {message}")
            }
        }
    }
    let a = input(&DataType::Int64, vec![Some(150); 1001]);
    let record = Control::default();
    assert_eq!(run(&a, &record).unwrap().len(), 1001);
    let trace = record.trace.lock().unwrap().clone();
    assert!(trace.len() > 1001);
    for stop in 0..trace.len() {
        for cause in causes() {
            let c = Control {
                trace: Mutex::new(vec![]),
                stop: Some((stop, cause.clone())),
            };
            assert!(matches!(run(&a,&c),Err(actual) if actual==cause));
            assert_eq!(*c.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}

struct CompileRefusal {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileRefusal {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let ordinal = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(
                ordinal <= stop,
                "no preparation callback after first refusal"
            );
        }
        trace.push((phase, units));
        match self.stop.clone() {
            Some((stop, cause)) if ordinal == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
#[test]
fn integral_decimal128_preparation_every_callback_preserves_three_compile_causes() {
    for dtype in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        let source = FunctionValueType::from_legacy_declared_projection(dtype, true);
        let target = FunctionValueType::new(DataType::Decimal128(4, 0), true);
        let prepare = |control: &CompileRefusal| {
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &source,
                &target,
                DecimalOverflowPolicy::OutputNull,
                false,
                control,
            )
        };
        let record = CompileRefusal {
            trace: Mutex::new(vec![]),
            stop: None,
        };
        prepare(&record).unwrap();
        let trace = record.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for stop in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let refused = CompileRefusal {
                    trace: Mutex::new(vec![]),
                    stop: Some((stop, cause.clone())),
                };
                let error = prepare(&refused).unwrap_err();
                assert_eq!(error.control_error(), Some(cause));
                assert_eq!(*refused.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}
