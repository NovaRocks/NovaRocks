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

//! Complete decimal Float64 recipe admission, selected origins and original failure controls.
use super::*;
use crate::{ConstantPolicy, ConstantPool, KernelDiagnostic, SelectedValues, Selection};
use arrow_array::ArrayRef;
use arrow_buffer::{NullBuffer, i256};
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
        panic!("decimal float never waits")
    }
}

fn dtype(wide: bool, p: u8, s: i8) -> DataType {
    if wide {
        DataType::Decimal256(p, s)
    } else {
        DataType::Decimal128(p, s)
    }
}
fn input(wide: bool, p: u8, s: i8, values: Vec<Option<i128>>) -> ArrayRef {
    if wide {
        Arc::new(
            Decimal256Array::from(
                values
                    .into_iter()
                    .map(|x| x.map(i256::from_i128))
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(p, s)
            .unwrap(),
        )
    } else {
        Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(p, s)
                .unwrap(),
        )
    }
}
fn recipe(ty: DataType, nullable: bool) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::from_legacy_declared_projection(ty, nullable),
        &FunctionValueType::new(DataType::Float64, nullable),
        DecimalOverflowPolicy::ReportError,
        true,
        &Control::default(),
    )
    .unwrap()
}
#[test]
fn decimal_float_complete_metadata_policy_nullability_and_effect_contract() {
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(1),
        domain: EvaluationDomainId::new(2),
        demand: EvaluationDemand::Value,
    };
    for wide in [false, true] {
        let max = if wide { 76 } else { 38 };
        for p in 1..=max {
            for scale in i8::MIN..=p as i8 {
                for nullable in [false, true] {
                    for target_nullable in [false, true] {
                        for policy in [
                            DecimalOverflowPolicy::OutputNull,
                            DecimalOverflowPolicy::ReportError,
                        ] {
                            for allow in [false, true] {
                                let source = FunctionValueType::from_legacy_declared_projection(
                                    dtype(wide, p, scale),
                                    nullable,
                                );
                                let target =
                                    FunctionValueType::new(DataType::Float64, target_nullable);
                                let prepared = PreparedCastRecipe::try_new(
                                    CastOperation::Carrier,
                                    &source,
                                    &target,
                                    policy,
                                    allow,
                                    &Control::default(),
                                );
                                if nullable && !target_nullable {
                                    assert_eq!(prepared, Err(CastPrepareError::TypeMismatch));
                                    continue;
                                }
                                let prepared = prepared.unwrap();
                                assert_eq!(prepared.source_type(), &source);
                                assert_eq!(prepared.result_type(), &target);
                                assert_eq!(prepared.policy(), policy);
                                assert_eq!(prepared.allow_throw_exception(), allow);
                                assert!(
                                    !prepared
                                        .own_effects(context)
                                        .for_use(context)
                                        .unwrap()
                                        .may_raise_row_error
                                );
                                assert!(!carrier_cast_can_produce_null(
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
        for (p, s) in [(0, 0), (max + 1, 0), (1, 2)] {
            let data_type = if wide {
                DataType::Decimal256(p, s)
            } else {
                DataType::Decimal128(p, s)
            };
            assert!(
                FunctionValueType::try_with_logical_type(
                    data_type,
                    true,
                    novarocks_type_contract::ValueLogicalType::Physical
                )
                .is_err()
            );
        }
        for operation in [CastOperation::Time, CastOperation::TimeFromDatetime] {
            assert_eq!(
                PreparedCastRecipe::try_new(
                    operation,
                    &FunctionValueType::from_legacy_declared_projection(dtype(wide, max, 0), true),
                    &FunctionValueType::new(DataType::Float64, true),
                    DecimalOverflowPolicy::ReportError,
                    true,
                    &Control::default()
                ),
                Err(CastPrepareError::Unsupported)
            );
        }
    }
    let largeint = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    assert_eq!(
        PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &largeint,
            &FunctionValueType::new(DataType::Float64, false),
            DecimalOverflowPolicy::ReportError,
            true,
            &Control::default()
        ),
        Err(CastPrepareError::Unsupported)
    );
}
#[test]
fn decimal_float_selected_slice_nonzero_constant_and_inherited_error_contract() {
    for wide in [false, true] {
        let p = if wide { 76 } else { 38 };
        let ty = dtype(wide, p, 2);
        let r = recipe(ty.clone(), true);
        let c = Control::default();
        let a = input(wide, p, 2, vec![Some(0), Some(12345), None, Some(-7)]).slice(1, 3);
        assert_eq!(
            r.evaluate_row(EvaluatedArgument::Column(&a), 77, 1, &c)
                .unwrap(),
            CastRowResult::Null
        );
        let selection = Selection::try_sparse(3, &[0, 2]).unwrap();
        let selected = SelectedValues::try_new(
            selection,
            &ty,
            input(wide, p, 2, vec![Some(12345), Some(-7)]),
            Box::new([]),
        )
        .unwrap();
        for (ordinal, row) in selection.iter().enumerate() {
            assert_eq!(
                r.evaluate_row(EvaluatedArgument::Column(&a), ordinal, row, &c)
                    .unwrap(),
                r.evaluate_row(
                    EvaluatedArgument::SelectedColumn(&selected),
                    ordinal,
                    row,
                    &c
                )
                .unwrap()
            );
        }
        let value_type = FunctionValueType::from_legacy_declared_projection(ty.clone(), true);
        let pool = ConstantPool::try_new(
            Arc::new(value_type.try_to_field("authored-decimal").unwrap()),
            value_type,
            a.to_data(),
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
            CompilePhase::FunctionSpecialization,
            &c,
        )
        .unwrap();
        let constant = pool.value(2).unwrap();
        assert_eq!(
            r.evaluate_row(EvaluatedArgument::Constant(&constant), 999, 10000, &c)
                .unwrap(),
            r.evaluate_row(EvaluatedArgument::Column(&a), 0, 2, &c)
                .unwrap()
        );
        let null = input(wide, p, 2, vec![None]);
        let inherited = SelectedValues::try_new(
            Selection::try_sparse(5, &[4]).unwrap(),
            &ty,
            null,
            vec![RowDataError::new(0, "original inherited failure")].into_boxed_slice(),
        )
        .unwrap();
        assert!(matches!(
            r.evaluate_row(EvaluatedArgument::SelectedColumn(&inherited), 0, 4, &c),
            Err(KernelFailure::InvalidProgram(_))
        ));
        assert!(matches!(
            r.evaluate_row(EvaluatedArgument::Column(&a), 0, 3, &c),
            Err(KernelFailure::InvalidProgram(_))
        ));
        let foreign: ArrayRef = Arc::new(Int32Array::from(vec![None]));
        assert!(matches!(
            r.evaluate_row(EvaluatedArgument::Column(&foreign), 0, 0, &c),
            Err(KernelFailure::InvalidProgram(_))
        ));
        assert!(matches!(
            recipe(ty, false).evaluate_row(EvaluatedArgument::Column(&a), 0, 1, &c),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
}
#[test]
fn decimal_float_original_hidden_payload_and_scale_min_failure_contract() {
    let a = Decimal128Array::new(
        vec![i128::MIN, 12345].into(),
        Some(NullBuffer::from(vec![false, true])),
    )
    .with_precision_and_scale(38, 2)
    .unwrap();
    let converted = crate::decimal_float_cast::decimal128_array_to_f64(&a, 2);
    assert!(converted.is_null(0));
    assert_eq!(
        converted.value(0).to_bits(),
        (i128::MIN as f64 / 100.0).to_bits()
    );
    assert_eq!(converted.value(1).to_bits(), 123.45_f64.to_bits());
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for allow in [false, true] {
            let a = input(true, 76, i8::MIN, vec![Some(0), None, Some(1), Some(-1)]);
            let r = PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &FunctionValueType::from_legacy_declared_projection(a.data_type().clone(), true),
                &FunctionValueType::new(DataType::Float64, true),
                policy,
                allow,
                &Control::default(),
            )
            .unwrap();
            assert_eq!(
                r.evaluate_row(EvaluatedArgument::Column(&a), 0, 1, &Control::default())
                    .unwrap(),
                CastRowResult::Null
            );
            assert_eq!(
                r.evaluate_row(EvaluatedArgument::Column(&a), 0, 0, &Control::default())
                    .unwrap(),
                CastRowResult::Float64(0.0),
            );
            for (row, expected) in [(2, 1e128), (3, -1e128)] {
                let CastRowResult::Float64(got) = r
                    .evaluate_row(EvaluatedArgument::Column(&a), row, row, &Control::default())
                    .unwrap()
                else {
                    panic!("exact Float64 minimum-scale result")
                };
                assert!(got.is_finite());
                assert!((got - expected).abs() <= expected.abs() * f64::EPSILON * 4.0);
            }
        }
    }
}
#[test]
fn decimal_float_all_actual_callbacks_preserve_seven_causes_and_no_tail() {
    for wide in [false, true] {
        for value in [Some(12345), None] {
            let a = input(wide, if wide { 76 } else { 38 }, 2, vec![value]);
            let r = recipe(a.data_type().clone(), true);
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
                    let c = Control {
                        refusal: Some((at, cause.clone())),
                        ..Default::default()
                    };
                    assert_eq!(
                        r.evaluate_row(EvaluatedArgument::Column(&a), 0, 0, &c)
                            .unwrap_err(),
                        cause
                    );
                    assert_eq!(*c.calls.lock().unwrap(), trace[..=at]);
                }
            }
        }
    }
}

#[test]
fn decimal_float_compile_callbacks_preserve_primary_causes_on_success_and_rejection() {
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
    for wide in [false, true] {
        for (source_nullable, result_nullable, operation) in [
            (false, false, CastOperation::Carrier),
            (true, false, CastOperation::Carrier),
            (false, true, CastOperation::Time),
        ] {
            let source = FunctionValueType::from_legacy_declared_projection(
                dtype(wide, if wide { 76 } else { 38 }, 2),
                source_nullable,
            );
            let result = FunctionValueType::new(DataType::Float64, result_nullable);
            let baseline = CompileControl {
                calls: Mutex::new(Vec::new()),
                refusal: None,
            };
            let original = PreparedCastRecipe::try_new(
                operation,
                &source,
                &result,
                DecimalOverflowPolicy::ReportError,
                true,
                &baseline,
            );
            match (source_nullable, result_nullable, operation) {
                (false, false, CastOperation::Carrier) => {
                    original.unwrap();
                }
                (true, false, CastOperation::Carrier) => {
                    assert_eq!(original, Err(CastPrepareError::TypeMismatch))
                }
                (_, _, CastOperation::Time) => {
                    assert_eq!(original, Err(CastPrepareError::Unsupported))
                }
                _ => unreachable!(),
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
                        calls: Mutex::new(Vec::new()),
                        refusal: Some((at, cause.clone())),
                    };
                    let failure = PreparedCastRecipe::try_new(
                        operation,
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
}

#[test]
fn decimal256_selected_and_scalar_casts_keep_independent_midpoint_bits() {
    let array = input(
        true,
        19,
        0,
        vec![
            Some(9_223_372_036_854_776_833),
            None,
            Some(-9_223_372_036_854_776_833),
        ],
    );
    let prepared = recipe(array.data_type().clone(), true);
    for (row, expected) in [(0, 0x43e0_0000_0000_0000), (2, 0xc3e0_0000_0000_0001)] {
        let CastRowResult::Float64(value) = prepared
            .evaluate_row(
                EvaluatedArgument::Column(&array),
                17,
                row,
                &Control::default(),
            )
            .unwrap()
        else {
            panic!("original decimal float value");
        };
        assert_eq!(value.to_bits(), expected);
        let scalar = array.slice(row, 1);
        let CastRowResult::Float64(value) = prepared
            .evaluate_row(
                EvaluatedArgument::Scalar(&scalar),
                31,
                999,
                &Control::default(),
            )
            .unwrap()
        else {
            panic!("original scalar decimal float value");
        };
        assert_eq!(value.to_bits(), expected);
    }
}
