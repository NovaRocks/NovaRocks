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

use super::super::*;
use crate::{ConstantPolicy, ConstantPool, SelectedValues, Selection};
use arrow_array::{ArrayRef, Float32Array, Float64Array};
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
        panic!("pure cast must not wait")
    }
}
fn ty(carrier: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(carrier, nullable)
}
fn prepare(
    source: FunctionValueType,
    result: FunctionValueType,
    policy: DecimalOverflowPolicy,
    allow: bool,
) -> PreparedCastRecipe {
    PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &source,
        &result,
        policy,
        allow,
        &CompileControl::default(),
    )
    .unwrap()
}
fn carriers() -> [DataType; 4] {
    [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ]
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(71),
        domain: EvaluationDomainId::new(91),
        demand: EvaluationDemand::Value,
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("control-owned invalid program"),
        internal("control-owned internal failure"),
        KernelFailure::Operational(crate::KernelDiagnostic::new("control-owned operation")),
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
fn floating(carrier: &DataType, values: &[Option<f64>]) -> ArrayRef {
    match carrier {
        DataType::Float32 => Arc::new(Float32Array::from(
            values
                .iter()
                .map(|v| v.map(|v| v as f32))
                .collect::<Vec<_>>(),
        )),
        DataType::Float64 => Arc::new(Float64Array::from(values.to_vec())),
        _ => panic!("explicit floating fixture"),
    }
}
fn frozen(source: &DataType, target: &DataType, allow: bool) -> PreparedCastRecipe {
    prepare(
        ty(source.clone(), true),
        ty(target.clone(), true),
        DecimalOverflowPolicy::OutputNull,
        allow,
    )
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

#[test]
fn all_eight_float_signed_profiles_keep_native_boundaries_and_two_independent_policies() {
    for source in [DataType::Float32, DataType::Float64] {
        for target in carriers() {
            // Hand-authored expected values, not num_cast or the recipe oracle.
            let (values, expected): (Vec<f64>, Vec<Option<i64>>) = match (&source, &target) {
                (_, DataType::Int8) => (
                    vec![127.9, -128.5, -129.0, 128.0],
                    vec![Some(127), Some(-128), None, None],
                ),
                (_, DataType::Int16) => (
                    vec![32767.5, -32768.5, -32769.0, 32768.0],
                    vec![Some(32767), Some(-32768), None, None],
                ),
                (DataType::Float32, DataType::Int32) => (
                    [0x4eff_ffff, 0x4f00_0000, 0xcf00_0000, 0xcf00_0001]
                        .map(|v| f64::from(f32::from_bits(v)))
                        .to_vec(),
                    vec![Some(2147483520), None, Some(i64::from(i32::MIN)), None],
                ),
                (DataType::Float64, DataType::Int32) => (
                    vec![2147483647.5, -2147483648.5, -2147483649.0, 2147483648.0],
                    vec![
                        Some(i64::from(i32::MAX)),
                        Some(i64::from(i32::MIN)),
                        None,
                        None,
                    ],
                ),
                (DataType::Float32, DataType::Int64) => (
                    [0x5eff_ffff, 0x5f00_0000, 0xdf00_0000, 0xdf00_0001]
                        .map(|v| f64::from(f32::from_bits(v)))
                        .to_vec(),
                    vec![Some(9223371487098961920), None, Some(i64::MIN), None],
                ),
                (DataType::Float64, DataType::Int64) => (
                    [
                        0x43df_ffff_ffff_ffff,
                        0x43e0_0000_0000_0000,
                        0xc3e0_0000_0000_0000,
                        0xc3e0_0000_0000_0001,
                    ]
                    .map(f64::from_bits)
                    .to_vec(),
                    vec![Some(9223372036854774784), None, Some(i64::MIN), None],
                ),
                _ => unreachable!("eight exact profiles"),
            };
            let mut inputs = values.into_iter().map(Some).collect::<Vec<_>>();
            inputs.extend([
                Some(0.0),
                Some(-0.0),
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                None,
            ]);
            let mut expected = expected;
            expected.extend([Some(0), Some(0), None, None, None, None]);
            let input = floating(&source, &inputs);
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for allow in [false, true] {
                    let recipe = prepare(
                        ty(source.clone(), true),
                        ty(target.clone(), true),
                        policy,
                        allow,
                    );
                    assert_eq!(recipe.source_type(), &ty(source.clone(), true));
                    assert_eq!(recipe.result_type(), &ty(target.clone(), true));
                    assert_eq!(recipe.policy(), policy);
                    assert_eq!(recipe.allow_throw_exception(), allow);
                    let effects = recipe.own_effects(context()).for_use(context()).unwrap();
                    assert_eq!(
                        effects,
                        ExpressionEffects {
                            may_raise_row_error: allow,
                            ..ExpressionEffects::PURE_VALUE
                        }
                    );
                    for (ordinal, expected) in expected.iter().enumerate() {
                        let actual = at(&recipe, &input, ordinal);
                        match (expected, inputs[ordinal], allow) {
                            (Some(expected), _, _) => assert_eq!(
                                actual,
                                CastRowResult::Signed(*expected),
                                "{source:?}->{target:?} row {ordinal}"
                            ),
                            (None, Some(_), true) => {
                                let CastRowResult::RowError(error) = actual else {
                                    panic!(
                                        "failed non-NULL conversion must be a row error: {actual:?}"
                                    )
                                };
                                assert_eq!(error.selected_ordinal(), ordinal);
                                assert_eq!(
                                    error.reason(),
                                    crate::DataFailureReason::NumericOutOfRange
                                );
                                assert!(error.message().contains("conflict with range of"));
                            }
                            _ => assert_eq!(actual, CastRowResult::Null),
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn range_diagnostics_preserve_nonfinite_target_names_and_float32_as_float64_value() {
    for (target, name) in [
        (DataType::Int8, "TINYINT"),
        (DataType::Int16, "SMALLINT"),
        (DataType::Int32, "INT"),
        (DataType::Int64, "BIGINT"),
    ] {
        let recipe = frozen(&DataType::Float64, &target, true);
        for (value, display) in [
            (f64::NAN, "NaN"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
        ] {
            let input: ArrayRef = Arc::new(Float64Array::from(vec![value]));
            let CastRowResult::RowError(error) = at(&recipe, &input, 0) else {
                panic!("strict nonfinite must fail")
            };
            assert_eq!(
                error.message(),
                format!(
                    "Expr evaluate meet error: CAST failed: from Float64 to {target:?}: {display} conflict with range of {name}"
                )
            );
        }
    }
    let input: ArrayRef = Arc::new(Float32Array::from(vec![f32::from_bits(0x4300_0001)]));
    let CastRowResult::RowError(error) = at(
        &frozen(&DataType::Float32, &DataType::Int8, true),
        &input,
        0,
    ) else {
        panic!("exact f32 overflow")
    };
    assert_eq!(
        error.message(),
        "Expr evaluate meet error: CAST failed: from Float32 to Int8: 128.00001525878906 conflict with range of TINYINT"
    );
    let input: ArrayRef = Arc::new(Float64Array::from(vec![2147483648.0]));
    let CastRowResult::RowError(error) = at(
        &frozen(&DataType::Float64, &DataType::Int32, true),
        &input,
        0,
    ) else {
        panic!("exact f64 overflow")
    };
    assert_eq!(
        error.message(),
        "Expr evaluate meet error: CAST failed: from Float64 to Int32: 2147483648 conflict with range of INT"
    );
}

#[test]
fn nullable_proof_depends_on_actual_allow_and_source_not_decimal_policy_or_value() {
    for source in [DataType::Float32, DataType::Float64] {
        for target in carriers() {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                for source_nullable in [false, true] {
                    for result_nullable in [false, true] {
                        for allow in [false, true] {
                            let result = PreparedCastRecipe::try_new(
                                CastOperation::Carrier,
                                &ty(source.clone(), source_nullable),
                                &ty(target.clone(), result_nullable),
                                policy,
                                allow,
                                &CompileControl::default(),
                            );
                            if result_nullable || (!source_nullable && allow) {
                                let recipe = result.unwrap();
                                let finite = floating(&source, &[Some(1.9)]);
                                assert_eq!(at(&recipe, &finite, 0), CastRowResult::Signed(1));
                                let failed = floating(&source, &[Some(f64::NAN)]);
                                assert!(
                                    matches!(at(&recipe, &failed, 0), CastRowResult::RowError(_))
                                        == allow
                                );
                            } else {
                                assert_eq!(result, Err(CastPrepareError::TypeMismatch));
                            }
                        }
                    }
                }
            }
        }
        for target in [
            DataType::Float16,
            DataType::Binary,
            DataType::Date64,
            DataType::Decimal128(10, 0),
        ]
        .into_iter()
        // Both Float32 and Float64 decimal routes now have independent P05 value tests.
        .filter(|target| !matches!(target, DataType::Decimal128(..)))
        {
            assert_eq!(
                PreparedCastRecipe::try_new(
                    CastOperation::Carrier,
                    &ty(source.clone(), true),
                    &ty(target, true),
                    DecimalOverflowPolicy::OutputNull,
                    false,
                    &CompileControl::default()
                ),
                Err(CastPrepareError::Unsupported)
            );
        }
        super::super::tests::assert_f3_admitted_preparation(
            source.clone(),
            DataType::Decimal128(10, 0),
        );
        for operation in [CastOperation::Time, CastOperation::TimeFromDatetime] {
            assert_eq!(
                PreparedCastRecipe::try_new(
                    operation,
                    &ty(source.clone(), true),
                    &ty(DataType::Int64, true),
                    DecimalOverflowPolicy::OutputNull,
                    false,
                    &CompileControl::default()
                ),
                Err(CastPrepareError::Unsupported)
            );
        }
    }
}

#[test]
fn constant_original_ordinal_sparse_compact_slice_and_broadcast_keep_error_addresses() {
    for source in [DataType::Float32, DataType::Float64] {
        let t = ty(source.clone(), true);
        let pool = ConstantPool::try_new(
            Arc::new(t.try_to_field("actual-floating-pool").unwrap()),
            t,
            floating(&source, &[Some(7.9), None, Some(f64::NAN)]).to_data(),
            policy(),
            CompilePhase::Validate,
            &CompileControl::default(),
        )
        .unwrap();
        let constant = pool.value(2).unwrap();
        assert_eq!(constant.ordinal(), 2);
        let null = pool.value(1).unwrap();
        let dense = floating(
            &source,
            &[
                Some(999.0),
                Some(-7.9),
                None,
                Some(f64::INFINITY),
                Some(2.9),
            ],
        )
        .slice(1, 4);
        let rows = [0, 2, 3];
        let selection = Selection::try_sparse(4, &rows).unwrap();
        let compact = SelectedValues::try_new(
            selection,
            &source,
            floating(&source, &[Some(-7.9), Some(f64::INFINITY), Some(2.9)]),
            Box::default(),
        )
        .unwrap();
        let strict = frozen(&source, &DataType::Int8, true);
        for (ordinal, row) in selection.iter().enumerate() {
            let expected = match ordinal {
                0 => CastRowResult::Signed(-7),
                1 => CastRowResult::RowError(RowDataError::with_reason(
                    1,
                    crate::DataFailureReason::NumericOutOfRange,
                    &format!(
                        "Expr evaluate meet error: CAST failed: from {source:?} to Int8: inf conflict with range of TINYINT"
                    ),
                )),
                _ => CastRowResult::Signed(2),
            };
            for argument in [
                EvaluatedArgument::Column(&dense),
                EvaluatedArgument::SelectedColumn(&compact),
            ] {
                assert_eq!(
                    strict
                        .evaluate_row(argument, ordinal, row, &Control::default())
                        .unwrap(),
                    expected
                );
            }
        }
        let scalar = floating(&source, &[Some(-7.9)]);
        assert_eq!(
            strict
                .evaluate_row(
                    EvaluatedArgument::Scalar(&scalar),
                    90,
                    900,
                    &Control::default()
                )
                .unwrap(),
            CastRowResult::Signed(-7)
        );
        let CastRowResult::RowError(error) = strict
            .evaluate_row(
                EvaluatedArgument::Constant(&constant),
                91,
                999,
                &Control::default(),
            )
            .unwrap()
        else {
            panic!("constant NaN fail")
        };
        assert_eq!(error.selected_ordinal(), 91);
        assert!(error.message().contains("NaN conflict"));
        assert_eq!(
            strict
                .evaluate_row(
                    EvaluatedArgument::Constant(&null),
                    92,
                    999,
                    &Control::default()
                )
                .unwrap(),
            CastRowResult::Null
        );
    }
}

#[test]
fn null_never_masks_foreign_shape_address_journal_or_nonnull_constant_promise() {
    for source in [DataType::Float32, DataType::Float64] {
        let recipe = frozen(&source, &DataType::Int8, true);
        let null = floating(&source, &[None]);
        let many = floating(&source, &[None, None]);
        let wrong: ArrayRef = Arc::new(Int64Array::from(vec![None]));
        for argument in [
            EvaluatedArgument::Scalar(&many),
            EvaluatedArgument::Column(&wrong),
        ] {
            assert!(matches!(
                recipe.evaluate_row(argument, 0, 0, &Control::default()),
                Err(KernelFailure::InvalidProgram(_))
            ));
        }
        assert!(matches!(
            recipe.evaluate_row(EvaluatedArgument::Column(&null), 0, 1, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
        let rows = [1];
        let selection = Selection::try_sparse(2, &rows).unwrap();
        let unresolved = SelectedValues::try_new(
            selection,
            &source,
            null.clone(),
            vec![RowDataError::new(0, "actual required child error")].into_boxed_slice(),
        )
        .unwrap();
        for row in [0, 1] {
            assert!(matches!(
                recipe.evaluate_row(
                    EvaluatedArgument::SelectedColumn(&unresolved),
                    0,
                    row,
                    &Control::default()
                ),
                Err(KernelFailure::InvalidProgram(_))
            ));
        }
        let strict = prepare(
            ty(source.clone(), false),
            ty(DataType::Int8, false),
            DecimalOverflowPolicy::ReportError,
            true,
        );
        assert!(matches!(
            strict.evaluate_row(EvaluatedArgument::Column(&null), 0, 0, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
        let t = ty(source.clone(), true);
        let nullable_constant = ConstantPool::try_new(
            Arc::new(t.try_to_field("nullable-source").unwrap()),
            t,
            floating(&source, &[Some(1.0)]).to_data(),
            policy(),
            CompilePhase::Validate,
            &CompileControl::default(),
        )
        .unwrap()
        .value(0)
        .unwrap();
        assert!(matches!(
            strict.evaluate_row(
                EvaluatedArgument::Constant(&nullable_constant),
                0,
                0,
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
}

#[test]
fn float_compile_original_three_causes_keep_all_refusal_prefixes_and_metadata_quantum() {
    let nested = ty(
        DataType::Struct(
            (0..320)
                .map(|i| {
                    Arc::new(
                        Field::new(format!("source-{i}"), DataType::Float64, true).with_metadata(
                            HashMap::from([("provider".to_owned(), "actual-source".to_owned())]),
                        ),
                    )
                })
                .collect::<Vec<_>>()
                .into(),
        ),
        true,
    );
    for source in [
        ty(DataType::Float32, true),
        ty(DataType::Float64, true),
        nested,
    ] {
        let target = ty(DataType::Int8, true);
        let baseline = CompileControl::default();
        let _ = PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &source,
            &target,
            DecimalOverflowPolicy::ReportError,
            true,
            &baseline,
        );
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
                let error = PreparedCastRecipe::try_new(
                    CastOperation::Carrier,
                    &source,
                    &target,
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
fn float_runtime_original_seven_causes_keep_diagnostic_success_null_and_error_tails() {
    for source in [DataType::Float32, DataType::Float64] {
        let recipe = frozen(&source, &DataType::Int8, true);
        let input = floating(&source, &[Some(7.9), Some(f64::NAN), None]);
        for row in [0, 1, 2, 3] {
            let baseline = Control::default();
            let _ = recipe.evaluate_row(EvaluatedArgument::Column(&input), row, row, &baseline);
            let trace = baseline.trace.lock().unwrap().clone();
            assert_eq!(trace[0], 0);
            assert!(trace.iter().skip(1).any(|n| *n > 0));
            if row == 1 {
                assert!(
                    trace.len() >= 4,
                    "diagnostic has original before/after boundaries: {trace:?}"
                );
            }
            for at in 0..trace.len() {
                for cause in causes() {
                    let control = Control {
                        refusal: Some((at, cause.clone())),
                        ..Default::default()
                    };
                    assert_eq!(
                        recipe.evaluate_row(EvaluatedArgument::Column(&input), row, row, &control),
                        Err(cause)
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
    }
}
