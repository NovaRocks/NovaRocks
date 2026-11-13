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

//! Exact Date32 float recipe shape, effect, demand and original refusal contracts.
use arrow_array::Int32Array;
use arrow_array::{ArrayRef, Date32Array};
use arrow_schema::DataType;
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
use novarocks_type_contract::ValueLogicalType;
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
        panic!("date float never waits")
    }
}
fn prepare(target: DataType, nullable: bool) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &FunctionValueType::new(DataType::Date32, nullable),
        &FunctionValueType::from_legacy_declared_projection(target, nullable),
        DecimalOverflowPolicy::ReportError,
        true,
        &Control::default(),
    )
    .unwrap()
}
#[test]
fn date_float_exact_profiles_nullability_effects_and_nominal_admission() {
    for target in [DataType::Float32, DataType::Float64] {
        for source_nullable in [false, true] {
            for result_nullable in [false, true] {
                for policy in [
                    DecimalOverflowPolicy::OutputNull,
                    DecimalOverflowPolicy::ReportError,
                ] {
                    for allow in [false, true] {
                        let result = PreparedCastRecipe::try_new(
                            CastOperation::Carrier,
                            &FunctionValueType::new(DataType::Date32, source_nullable),
                            &FunctionValueType::from_legacy_declared_projection(
                                target.clone(),
                                result_nullable,
                            ),
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
                            &FunctionValueType::new(DataType::Date32, source_nullable)
                        );
                        assert_eq!(
                            r.result_type(),
                            &FunctionValueType::from_legacy_declared_projection(
                                target.clone(),
                                result_nullable
                            )
                        );
                        assert_eq!(r.policy(), policy);
                        assert_eq!(r.allow_throw_exception(), allow);
                        let context = ExpressionEffectContext {
                            use_id: ExpressionUseId::new(1),
                            domain: EvaluationDomainId::new(2),
                            demand: EvaluationDemand::Value,
                        };
                        assert!(
                            r.own_effects(context)
                                .for_use(context)
                                .unwrap()
                                .may_raise_row_error
                        );
                    }
                }
            }
        }
        // A valid non-Physical source stays refused; neither a DATE nor a target is retagged.
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
                &FunctionValueType::from_legacy_declared_projection(target.clone(), false),
                DecimalOverflowPolicy::ReportError,
                true,
                &Control::default()
            ),
            Err(CastPrepareError::Unsupported)
        );
        for operation in [CastOperation::Time, CastOperation::TimeFromDatetime] {
            assert_eq!(
                PreparedCastRecipe::try_new(
                    operation,
                    &FunctionValueType::new(DataType::Date32, false),
                    &FunctionValueType::from_legacy_declared_projection(target.clone(), false),
                    DecimalOverflowPolicy::ReportError,
                    true,
                    &Control::default()
                ),
                Err(CastPrepareError::Unsupported)
            );
        }
    }
}
#[test]
fn date_float_selected_addresses_constant_producer_and_hidden_null_policy() {
    let input: ArrayRef = Arc::new(Date32Array::new(
        vec![i32::MAX, 0, i32::MIN, -1].into(),
        Some(arrow_buffer::NullBuffer::from(vec![
            false, true, true, true,
        ])),
    ));
    let control = Control::default();
    for target in [DataType::Float32, DataType::Float64] {
        let r = prepare(target.clone(), true);
        assert_eq!(
            r.evaluate_row(EvaluatedArgument::Column(&input), 0, 0, &control)
                .unwrap(),
            CastRowResult::Null
        );
        let rows = [1, 3];
        let selection = Selection::try_sparse(4, &rows).unwrap();
        let selected: ArrayRef = Arc::new(Date32Array::from(vec![0, -1]));
        let compact =
            SelectedValues::try_new(selection, &DataType::Date32, selected, Box::new([])).unwrap();
        for (ordinal, row) in selection.iter().enumerate() {
            assert_eq!(
                r.evaluate_row(EvaluatedArgument::Column(&input), ordinal, row, &control)
                    .unwrap(),
                r.evaluate_row(
                    EvaluatedArgument::SelectedColumn(&compact),
                    ordinal,
                    row,
                    &control
                )
                .unwrap()
            );
        }
        let source_type = FunctionValueType::new(DataType::Date32, true);
        let scalar = input.slice(3, 1);
        let pool = ConstantPool::try_new(
            Arc::new(source_type.try_to_field("actual-date-constant").unwrap()),
            source_type,
            scalar.to_data(),
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
            &control,
        )
        .unwrap();
        let value = pool.value(0).unwrap();
        assert_eq!(
            r.evaluate_row(EvaluatedArgument::Constant(&value), 77, 999, &control)
                .unwrap(),
            r.evaluate_row(EvaluatedArgument::Column(&input), 77, 3, &control)
                .unwrap()
        );
        assert!(matches!(
            r.evaluate_row(EvaluatedArgument::Column(&input), 0, 4, &control),
            Err(KernelFailure::InvalidProgram(_))
        ));
        let other: ArrayRef = Arc::new(Int32Array::from(vec![None]));
        assert!(matches!(
            r.evaluate_row(EvaluatedArgument::Column(&other), 0, 0, &control),
            Err(KernelFailure::InvalidProgram(_))
        ));
        assert!(matches!(
            prepare(target, false).evaluate_row(EvaluatedArgument::Column(&input), 0, 0, &control),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
}
#[test]
fn date_float_selected_row_error_preserves_full_original_message_and_excludes_inherited_error() {
    let input: ArrayRef = Arc::new(Date32Array::from(vec![i32::MIN]));
    let hidden: ArrayRef = Arc::new(Date32Array::new(
        vec![i32::MAX].into(),
        Some(arrow_buffer::NullBuffer::from(vec![false])),
    ));
    for target in [DataType::Float32, DataType::Float64] {
        let r = prepare(target.clone(), true);
        match r
            .evaluate_row(
                EvaluatedArgument::Column(&input),
                23,
                0,
                &Control::default(),
            )
            .unwrap()
        {
            CastRowResult::RowError(e) => {
                assert_eq!(e.selected_ordinal(), 23);
                assert_eq!(
                    e.reason(),
                    novarocks_functions::DataFailureReason::NumericOutOfRange
                );
                assert_eq!(
                    e.message(),
                    format!(
                        "CAST failed: from Date32 to {target:?}: invalid Date32 value -2147483648"
                    )
                );
            }
            other => panic!("wrong original error projection {other:?}"),
        }
        let rows = [5];
        let selection = Selection::try_sparse(6, &rows).unwrap();
        let inherited = SelectedValues::try_new(
            selection,
            &DataType::Date32,
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
fn date_float_every_actual_control_callback_preserves_all_seven_causes_without_tail() {
    for target in [DataType::Float32, DataType::Float64] {
        for value in [Some(0), Some(i32::MIN), None] {
            let input: ArrayRef = Arc::new(Date32Array::from(vec![value]));
            let r = prepare(target.clone(), true);
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
                    KernelFailure::InvalidProgram(KernelDiagnostic::new(
                        "original invalid refusal",
                    )),
                    KernelFailure::Internal(KernelDiagnostic::new("original internal refusal")),
                    KernelFailure::Operational(KernelDiagnostic::new(
                        "original operational refusal",
                    )),
                    KernelFailure::InstanceFailed,
                ] {
                    let c = Control {
                        refusal: Some((at, cause.clone())),
                        ..Default::default()
                    };
                    assert_eq!(
                        r.evaluate_row(EvaluatedArgument::Column(&input), 0, 0, &c)
                            .unwrap_err(),
                        cause
                    );
                    assert_eq!(*c.calls.lock().unwrap(), calls[..=at]);
                }
            }
        }
    }
}
