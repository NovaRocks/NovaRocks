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

//! Exact original strict float-DATE profiles, frozen policies, constants and refusals.
use arrow_array::ArrayRef;
use arrow_array::Float32Array;
use arrow_array::Float64Array;
use arrow_schema::DataType;
use arrow_schema::TimeUnit;
use novarocks_functions::*;
use novarocks_functions::{
    ConstantPolicy, ConstantPool, KernelDiagnostic, SelectedValues, Selection,
};
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
            assert!(at <= *stop, "callback after refusal");
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
        panic!("float DATE does not wait")
    }
}
fn ty(wide: bool) -> DataType {
    if wide {
        DataType::Float64
    } else {
        DataType::Float32
    }
}
fn input(wide: bool, values: Vec<Option<f64>>) -> ArrayRef {
    if wide {
        Arc::new(Float64Array::from(values))
    } else {
        Arc::new(Float32Array::from(
            values
                .into_iter()
                .map(|v| v.map(|v| v as f32))
                .collect::<Vec<_>>(),
        ))
    }
}
fn recipe(
    wide: bool,
    source_nullable: bool,
    result_nullable: bool,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::from_legacy_declared_projection(ty(wide), source_nullable),
        &FunctionValueType::new(DataType::Date32, result_nullable),
        policy,
        allow,
        &Control::default(),
    )
    .unwrap()
}
#[test]
fn float_date_exact_nullability_successful_null_effect_and_all_policy_profiles() {
    for wide in [false, true] {
        for source_nullable in [false, true] {
            for result_nullable in [false, true] {
                for policy in [
                    DecimalOverflowPolicy::OutputNull,
                    DecimalOverflowPolicy::ReportError,
                ] {
                    for allow in [false, true] {
                        assert!(!carrier_cast_can_produce_null(
                            &ty(wide),
                            &DataType::Date32,
                            allow
                        ));
                        assert!(carrier_cast_can_produce_null(
                            &ty(wide),
                            &DataType::Timestamp(TimeUnit::Microsecond, None),
                            allow
                        ));
                        assert!(carrier_cast_can_produce_null(
                            &DataType::Int64,
                            &DataType::Date32,
                            allow
                        ));
                        let result = PreparedCastRecipe::try_new(
                            CastOperation::Carrier,
                            &FunctionValueType::from_legacy_declared_projection(
                                ty(wide),
                                source_nullable,
                            ),
                            &FunctionValueType::new(DataType::Date32, result_nullable),
                            policy,
                            allow,
                            &Control::default(),
                        );
                        if source_nullable && !result_nullable {
                            assert_eq!(result, Err(CastPrepareError::TypeMismatch));
                            continue;
                        }
                        let r = result.unwrap();
                        assert_eq!(
                            r.source_type(),
                            &FunctionValueType::from_legacy_declared_projection(
                                ty(wide),
                                source_nullable
                            )
                        );
                        assert_eq!(
                            r.result_type(),
                            &FunctionValueType::new(DataType::Date32, result_nullable)
                        );
                        assert_eq!(r.allow_throw_exception(), allow);
                        assert_eq!(r.policy(), policy);
                        let context = ExpressionEffectContext {
                            use_id: ExpressionUseId::new(31),
                            domain: EvaluationDomainId::new(4),
                            demand: EvaluationDemand::Value,
                        };
                        assert!(
                            r.own_effects(context)
                                .for_use(context)
                                .unwrap()
                                .may_raise_row_error
                        );
                        let valid = input(wide, vec![Some(19700102.0)]);
                        assert_eq!(
                            r.evaluate_row(
                                EvaluatedArgument::Column(&valid),
                                0,
                                0,
                                &Control::default()
                            )
                            .unwrap(),
                            CastRowResult::Signed(1)
                        );
                    }
                }
            }
        }
    }
}
#[test]
fn float_date_nonzero_pool_ordinal_scalar_selected_origin_and_null_constant() {
    for wide in [false, true] {
        let values = input(
            wide,
            vec![Some(f64::NAN), Some(19700102.0), None, Some(20240228.0)],
        );
        let source = FunctionValueType::from_legacy_declared_projection(ty(wide), true);
        let pool = ConstantPool::try_new(
            Arc::new(source.try_to_field("original-float-constant").unwrap()),
            source,
            values.to_data(),
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
            &Control::default(),
        )
        .unwrap();
        let rows = [1, 2, 3];
        let selection = Selection::try_sparse(4, &rows).unwrap();
        let compact =
            SelectedValues::try_new(selection, &ty(wide), values.slice(1, 3), Box::new([]))
                .unwrap();
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                let r = recipe(wide, true, true, policy, allow);
                for (ordinal, row) in selection.iter().enumerate() {
                    let expected = match row {
                        1 => CastRowResult::Signed(1),
                        2 => CastRowResult::Null,
                        3 => CastRowResult::Signed(19781),
                        _ => unreachable!(),
                    };
                    let constant = pool.value(u32::try_from(row).unwrap()).unwrap();
                    let scalar = values.slice(row, 1);
                    for arg in [
                        EvaluatedArgument::Column(&values),
                        EvaluatedArgument::SelectedColumn(&compact),
                    ] {
                        assert_eq!(
                            r.evaluate_row(arg, ordinal, row, &Control::default())
                                .unwrap(),
                            expected
                        );
                    }
                    for arg in [
                        EvaluatedArgument::Constant(&constant),
                        EvaluatedArgument::Scalar(&scalar),
                    ] {
                        assert_eq!(
                            r.evaluate_row(arg, 37, 999, &Control::default()).unwrap(),
                            expected
                        );
                    }
                }
                let bad = pool.value(0).unwrap();
                match r
                    .evaluate_row(
                        EvaluatedArgument::Constant(&bad),
                        23,
                        999,
                        &Control::default(),
                    )
                    .unwrap()
                {
                    CastRowResult::RowError(e) => {
                        assert_eq!(e.selected_ordinal(), 23);
                        assert_eq!(
                            e.message(),
                            format!(
                                "CAST failed: from {:?} to Date32: invalid date literal NaN",
                                ty(wide)
                            )
                        );
                    }
                    other => panic!("wrong original nonfinite constant {other:?}"),
                }
            }
        }
    }
}
#[test]
fn float_date_nonfinite_hidden_null_and_inherited_errors_keep_original_validation_order() {
    for wide in [false, true] {
        let hidden: ArrayRef = if wide {
            Arc::new(Float64Array::new(
                vec![f64::NAN].into(),
                Some(arrow_buffer::NullBuffer::from(vec![false])),
            ))
        } else {
            Arc::new(Float32Array::new(
                vec![f32::NAN].into(),
                Some(arrow_buffer::NullBuffer::from(vec![false])),
            ))
        };
        let r = recipe(wide, true, true, DecimalOverflowPolicy::ReportError, true);
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
        assert!(matches!(
            recipe(wide, false, false, DecimalOverflowPolicy::ReportError, true).evaluate_row(
                EvaluatedArgument::Column(&hidden),
                0,
                0,
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
        assert!(matches!(
            r.evaluate_row(
                EvaluatedArgument::Column(&hidden),
                0,
                1,
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
        let wrong = input(!wide, vec![None]);
        assert!(matches!(
            r.evaluate_row(EvaluatedArgument::Column(&wrong), 0, 0, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
        let rows = [5];
        let inherited = SelectedValues::try_new(
            Selection::try_sparse(6, &rows).unwrap(),
            &ty(wide),
            hidden.clone(),
            vec![RowDataError::new(0, "original child error")].into_boxed_slice(),
        )
        .unwrap();
        assert!(matches!(
            r.evaluate_row(
                EvaluatedArgument::SelectedColumn(&inherited),
                0,
                5,
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
}
#[test]
fn float_date_entire_float_carrier_extremes_do_not_panic_or_successfully_null() {
    for wide in [false, true] {
        for value in [
            0.0,
            -0.0,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MAX,
            -f64::MAX,
            9223372036854775808.0,
            -9223372036854775808.0,
            20240230.0,
            20241301.0,
            20240229240000.0,
        ] {
            let source = input(wide, vec![Some(value)]);
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let r = recipe(wide, false, false, policy, allow);
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        r.evaluate_row(
                            EvaluatedArgument::Column(&source),
                            11,
                            0,
                            &Control::default(),
                        )
                    }));
                    assert!(matches!(
                        outcome.unwrap().unwrap(),
                        CastRowResult::RowError(_)
                    ));
                }
            }
        }
    }
}
#[test]
fn float_date_every_actual_callback_keeps_all_seven_original_control_causes() {
    for wide in [false, true] {
        for value in [Some(19700102.0), Some(f64::NAN), None] {
            let input = input(wide, vec![value]);
            let r = recipe(wide, true, true, DecimalOverflowPolicy::OutputNull, false);
            let baseline = Control::default();
            r.evaluate_row(EvaluatedArgument::Column(&input), 0, 0, &baseline)
                .unwrap();
            let calls = baseline.calls.lock().unwrap().clone();
            assert!(!calls.is_empty());
            for at in 0..calls.len() {
                for cause in [
                    KernelFailure::Cancelled,
                    KernelFailure::DeadlineExceeded,
                    KernelFailure::ResourceExhausted,
                    KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid")),
                    KernelFailure::Internal(KernelDiagnostic::new("original internal")),
                    KernelFailure::Operational(KernelDiagnostic::new("original operational")),
                    KernelFailure::InstanceFailed,
                ] {
                    let control = Control {
                        refusal: Some((at, cause.clone())),
                        ..Default::default()
                    };
                    assert_eq!(
                        r.evaluate_row(EvaluatedArgument::Column(&input), 0, 0, &control)
                            .unwrap_err(),
                        cause
                    );
                    assert_eq!(*control.calls.lock().unwrap(), calls[..=at]);
                }
            }
        }
    }
}
