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

fn output_bits(value: CastRowResult) -> Option<u64> {
    match value {
        CastRowResult::Float32(value) => Some(u64::from(value.to_bits())),
        CastRowResult::Float64(value) => Some(value.to_bits()),
        CastRowResult::Null => None,
        other => panic!("expected successful floating value: {other:?}"),
    }
}
fn raw(carrier: &DataType, bits: &[Option<u64>]) -> ArrayRef {
    match carrier {
        DataType::Float32 => Arc::new(Float32Array::from(
            bits.iter()
                .map(|v| v.map(|v| f32::from_bits(u32::try_from(v).unwrap())))
                .collect::<Vec<_>>(),
        )),
        DataType::Float64 => Arc::new(Float64Array::from(
            bits.iter()
                .map(|v| v.map(f64::from_bits))
                .collect::<Vec<_>>(),
        )),
        _ => panic!("explicit floating fixture"),
    }
}
fn sources() -> [DataType; 2] {
    [DataType::Float32, DataType::Float64]
}
fn same_bits(source: &DataType) -> Vec<Option<u64>> {
    match source {
        DataType::Float32 => vec![
            Some(0),
            Some(0x80000000),
            Some(0x7f800000),
            Some(0xff800000),
            Some(0x7fc00123),
            Some(0xffc00456),
            Some(0x7f800001),
            Some(0xff800002),
            Some(0x3fc00000),
            None,
        ],
        DataType::Float64 => vec![
            Some(0),
            Some(0x8000000000000000),
            Some(0x7ff0000000000000),
            Some(0xfff0000000000000),
            Some(0x7ff8000000000123),
            Some(0xfff8000000000456),
            Some(0x7ff0000000000001),
            Some(0xfff0000000000002),
            Some(0x3ff8000000000000),
            None,
        ],
        _ => panic!("explicit float profile"),
    }
}

#[test]
fn same_width_float_identity_keeps_every_original_nan_payload_signed_zero_and_infinity_bit() {
    for source in sources() {
        let expected = same_bits(&source);
        let input = raw(&source, &expected);
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                let recipe = prepare(
                    ty(source.clone(), true),
                    ty(source.clone(), true),
                    policy,
                    allow,
                );
                assert_eq!(recipe.source_type(), &ty(source.clone(), true));
                assert_eq!(recipe.result_type(), &ty(source.clone(), true));
                assert_eq!(recipe.policy(), policy);
                assert_eq!(recipe.allow_throw_exception(), allow);
                assert_eq!(
                    recipe.own_effects(context()).for_use(context()).unwrap(),
                    ExpressionEffects::PURE_VALUE
                );
                for (row, expected) in expected.iter().enumerate() {
                    assert_eq!(
                        output_bits(at(&recipe, &input, row)),
                        *expected,
                        "{source:?} row {row}"
                    );
                }
            }
        }
    }
}

#[test]
fn cross_width_float_cast_keeps_nonfinite_overflow_infinity_underflow_zero_and_independent_bits() {
    let fixtures = [
        (
            DataType::Float32,
            DataType::Float64,
            vec![
                (0, 0),
                (0x80000000, 0x8000000000000000),
                (0x7f800000, 0x7ff0000000000000),
                (0xff800000, 0xfff0000000000000),
                (0x00000001, 0x36a0000000000000),
                (0x80000001, 0xb6a0000000000000),
                (0x7f7fffff, 0x47efffffe0000000),
                (0xff7fffff, 0xc7efffffe0000000),
                (0x3fc00000, 0x3ff8000000000000),
                (0xbfc00000, 0xbff8000000000000),
            ],
        ),
        (
            DataType::Float64,
            DataType::Float32,
            vec![
                (0, 0),
                (0x8000000000000000, 0x80000000),
                (0x7ff0000000000000, 0x7f800000),
                (0xfff0000000000000, 0xff800000),
                (0x0000000000000001, 0),
                (0x8000000000000001, 0x80000000),
                (0x7fefffffffffffff, 0x7f800000),
                (0xffefffffffffffff, 0xff800000),
                (0x47efffffe0000000, 0x7f7fffff),
                (0xc7efffffe0000000, 0xff7fffff),
                (0x3ff8000000000000, 0x3fc00000),
                (0xbff8000000000000, 0xbfc00000),
            ],
        ),
    ];
    for (source, target, fixtures) in fixtures {
        let input = raw(
            &source,
            &fixtures
                .iter()
                .map(|(source, _)| Some(*source))
                .collect::<Vec<_>>(),
        );
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                let recipe = prepare(
                    ty(source.clone(), false),
                    ty(target.clone(), false),
                    policy,
                    allow,
                );
                assert_eq!(
                    recipe.own_effects(context()).for_use(context()).unwrap(),
                    ExpressionEffects::PURE_VALUE
                );
                for (row, (_, expected)) in fixtures.iter().enumerate() {
                    assert_eq!(
                        output_bits(at(&recipe, &input, row)),
                        Some(*expected),
                        "{source:?}->{target:?} row {row}"
                    );
                }
            }
        }
        let nanbits = if source == DataType::Float32 {
            0x7fc00123
        } else {
            0x7ff8000000000123
        };
        let nan = raw(&source, &[Some(nanbits), None]);
        let recipe = frozen(&source, &target, true);
        match at(&recipe, &nan, 0) {
            CastRowResult::Float32(value) => assert!(value.is_nan()),
            CastRowResult::Float64(value) => assert!(value.is_nan()),
            other => panic!("cross-width NaN remains a successful value: {other:?}"),
        }
        assert_eq!(at(&recipe, &nan, 1), CastRowResult::Null);
    }
}

#[test]
fn float_result_nullability_depends_only_on_source_and_types_are_not_domain_guesses() {
    for source in sources() {
        for target in sources() {
            for source_nullable in [false, true] {
                for result_nullable in [false, true] {
                    for policy in [
                        DecimalOverflowPolicy::OutputNull,
                        DecimalOverflowPolicy::ReportError,
                    ] {
                        for allow in [false, true] {
                            let recipe = PreparedCastRecipe::try_new(
                                CastOperation::Carrier,
                                &ty(source.clone(), source_nullable),
                                &ty(target.clone(), result_nullable),
                                policy,
                                allow,
                                &CompileControl::default(),
                            );
                            if source_nullable && !result_nullable {
                                assert_eq!(recipe, Err(CastPrepareError::TypeMismatch));
                            } else {
                                assert_eq!(
                                    recipe
                                        .unwrap()
                                        .own_effects(context())
                                        .for_use(context())
                                        .unwrap(),
                                    ExpressionEffects::PURE_VALUE
                                );
                            }
                        }
                    }
                }
            }
        }
        for target in [
            DataType::Float16,
            DataType::Date64,
            DataType::Binary,
            DataType::Decimal128(10, 0),
        ]
        .into_iter()
        // Both Float32 and Float64 decimal routes now have independent P05 value tests.
        .filter(|target| !matches!(target, DataType::Decimal128(..)))
        {
            assert_eq!(
                PreparedCastRecipe::try_new(
                    CastOperation::Carrier,
                    &ty(source.clone(), false),
                    &ty(target, false),
                    DecimalOverflowPolicy::OutputNull,
                    false,
                    &CompileControl::default()
                ),
                Err(CastPrepareError::Unsupported)
            );
        }
    }
    for source in sources() {
        super::super::tests::assert_f3_admitted_preparation(
            source.clone(),
            DataType::Decimal128(10, 0),
        );
        assert_eq!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &ty(source, false),
                &ty(DataType::Decimal128(10, 0), false),
                DecimalOverflowPolicy::OutputNull,
                false,
                &CompileControl::default(),
            ),
            Err(CastPrepareError::TypeMismatch)
        );
    }
    let foreign_sources = [
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::LargeInt,
        )
        .unwrap(),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    ];
    for source in foreign_sources {
        assert_eq!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &source,
                &ty(DataType::Float64, false),
                DecimalOverflowPolicy::OutputNull,
                false,
                &CompileControl::default()
            ),
            Err(CastPrepareError::Unsupported)
        );
    }
    assert_eq!(
        PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &ty(DataType::FixedSizeBinary(16), false),
            &ty(DataType::Float64, false),
            DecimalOverflowPolicy::OutputNull,
            false,
            &CompileControl::default(),
        ),
        Err(CastPrepareError::TypeMismatch)
    );
    super::super::tests::assert_f3_selected_literal_case(
        Arc::new(
            arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [
                    Some(12_i128.to_be_bytes()),
                    Some((-12_i128).to_be_bytes()),
                    None,
                ]
                .into_iter(),
                16,
            )
            .unwrap(),
        ),
        Arc::new(arrow_array::Float64Array::from(vec![
            Some(12.0),
            Some(-12.0),
            None,
        ])),
    );
}

#[test]
fn exact_constant_pool_ordinal_compact_column_slice_and_scalar_preserve_floating_addresses() {
    for source in sources() {
        let expected = same_bits(&source);
        let payload = expected[6].unwrap(); // Signaling NaN identity.
        let t = ty(source.clone(), true);
        let pool = ConstantPool::try_new(
            Arc::new(t.try_to_field("actual-identity-pool").unwrap()),
            t,
            raw(&source, &[Some(0), None, Some(payload)]).to_data(),
            policy(),
            CompilePhase::Validate,
            &CompileControl::default(),
        )
        .unwrap();
        let constant = pool.value(2).unwrap();
        assert_eq!(constant.ordinal(), 2);
        let null = pool.value(1).unwrap();
        let recipe = frozen(&source, &source, true);
        assert_eq!(
            output_bits(
                recipe
                    .evaluate_row(
                        EvaluatedArgument::Constant(&constant),
                        90,
                        999,
                        &Control::default()
                    )
                    .unwrap()
            ),
            Some(payload)
        );
        assert_eq!(
            recipe
                .evaluate_row(
                    EvaluatedArgument::Constant(&null),
                    90,
                    999,
                    &Control::default()
                )
                .unwrap(),
            CastRowResult::Null
        );
        let dense = raw(
            &source,
            &[Some(0), Some(payload), None, expected[1], expected[2]],
        )
        .slice(1, 4);
        let rows = [0, 2, 3];
        let selection = Selection::try_sparse(4, &rows).unwrap();
        let compact = SelectedValues::try_new(
            selection,
            &source,
            raw(&source, &[Some(payload), expected[1], expected[2]]),
            Box::default(),
        )
        .unwrap();
        for (ordinal, row) in selection.iter().enumerate() {
            let expected = [Some(payload), expected[1], expected[2]][ordinal];
            for argument in [
                EvaluatedArgument::Column(&dense),
                EvaluatedArgument::SelectedColumn(&compact),
            ] {
                assert_eq!(
                    output_bits(
                        recipe
                            .evaluate_row(argument, ordinal, row, &Control::default())
                            .unwrap()
                    ),
                    expected
                );
            }
        }
        let scalar = raw(&source, &[Some(payload)]);
        assert_eq!(
            output_bits(
                recipe
                    .evaluate_row(
                        EvaluatedArgument::Scalar(&scalar),
                        91,
                        999,
                        &Control::default()
                    )
                    .unwrap()
            ),
            Some(payload)
        );
    }
}

#[test]
fn float_identity_checks_required_journal_foreign_shape_and_null_promise_before_returning_bits() {
    for source in sources() {
        let recipe = frozen(&source, &source, false);
        let null = raw(&source, &[None]);
        let wrong: ArrayRef = Arc::new(Int64Array::from(vec![None]));
        let many = raw(&source, &[None, None]);
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
            vec![RowDataError::new(0, "actual required child")].into_boxed_slice(),
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
            ty(source.clone(), false),
            DecimalOverflowPolicy::OutputNull,
            false,
        );
        assert!(matches!(
            strict.evaluate_row(EvaluatedArgument::Column(&null), 0, 0, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
        let t = ty(source.clone(), true);
        let constant = ConstantPool::try_new(
            Arc::new(t.try_to_field("nullable-actual-pool").unwrap()),
            t,
            raw(&source, &[Some(0)]).to_data(),
            policy(),
            CompilePhase::Validate,
            &CompileControl::default(),
        )
        .unwrap()
        .value(0)
        .unwrap();
        assert!(matches!(
            strict.evaluate_row(
                EvaluatedArgument::Constant(&constant),
                0,
                0,
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
}

#[test]
fn float_identity_compile_three_causes_keep_entry_metadata_quantum_and_exact_refusing_prefix() {
    let wide = ty(
        DataType::Struct(
            (0..320)
                .map(|i| {
                    Arc::new(
                        Field::new(format!("actual-{i}"), DataType::Float64, true).with_metadata(
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
        ty(DataType::Float32, false),
        ty(DataType::Float64, false),
        wide,
    ] {
        let target = ty(DataType::Float32, true);
        let baseline = CompileControl::default();
        let _ = PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &source,
            &target,
            DecimalOverflowPolicy::OutputNull,
            false,
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
                    DecimalOverflowPolicy::OutputNull,
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
fn float_identity_runtime_seven_causes_keep_all_original_success_null_and_error_tail_prefixes() {
    for source in sources() {
        for target in sources() {
            let recipe = frozen(&source, &target, true);
            let input = raw(
                &source,
                &[same_bits(&source)[6], same_bits(&source)[2], None],
            );
            for row in [0, 1, 2, 3] {
                let baseline = Control::default();
                let _ = recipe.evaluate_row(EvaluatedArgument::Column(&input), row, row, &baseline);
                let trace = baseline.trace.lock().unwrap().clone();
                assert_eq!(trace[0], 0);
                assert!(trace.last().copied().unwrap() > 0);
                for at in 0..trace.len() {
                    for cause in causes() {
                        let control = Control {
                            refusal: Some((at, cause.clone())),
                            ..Default::default()
                        };
                        assert_eq!(
                            recipe.evaluate_row(
                                EvaluatedArgument::Column(&input),
                                row,
                                row,
                                &control
                            ),
                            Err(cause)
                        );
                        assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                    }
                }
            }
        }
    }
}
