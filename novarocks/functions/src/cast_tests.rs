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
        if let Some((at, cause)) = self.refusal
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
    FunctionValueType::new(carrier, nullable)
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
fn array(carrier: &DataType, values: &[Option<i64>]) -> ArrayRef {
    macro_rules! make {
        ($array:ty, $native:ty) => {
            Arc::new(<$array>::from(
                values
                    .iter()
                    .map(|v| v.map(|v| <$native>::try_from(v).unwrap()))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        };
    }
    match carrier {
        DataType::Int8 => make!(Int8Array, i8),
        DataType::Int16 => make!(Int16Array, i16),
        DataType::Int32 => make!(Int32Array, i32),
        DataType::Int64 => make!(Int64Array, i64),
        _ => panic!("explicit signed fixture"),
    }
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
fn pool(nullable: bool) -> crate::ConstantValue {
    let source = ty(DataType::Int64, nullable);
    let values = if nullable {
        vec![Some(11), None, Some(256)]
    } else {
        vec![Some(11), Some(0), Some(256)]
    };
    ConstantPool::try_new(
        Arc::new(source.try_to_field("actual-pool").unwrap()),
        source,
        Int64Array::from(values).to_data(),
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
    .value(2)
    .unwrap()
}

#[test]
fn all_twenty_four_profiles_freeze_types_nullable_and_inert_policies_without_row_errors() {
    let sources = carriers();
    let targets = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
    ];
    for (si, source) in sources.iter().enumerate() {
        for (ti, target) in targets.iter().enumerate() {
            for source_nullable in [false, true] {
                let result_nullable = source_nullable || ti < si;
                for policy in [
                    DecimalOverflowPolicy::OutputNull,
                    DecimalOverflowPolicy::ReportError,
                ] {
                    for allow in [false, true] {
                        let source_type = ty(source.clone(), source_nullable);
                        let result_type = ty(target.clone(), result_nullable);
                        let recipe =
                            prepare(source_type.clone(), result_type.clone(), policy, allow);
                        assert_eq!(recipe.operation(), CastOperation::Carrier);
                        assert_eq!(recipe.source_type(), &source_type);
                        assert_eq!(recipe.result_type(), &result_type);
                        assert_eq!(recipe.decimal_overflow_policy(), policy);
                        assert_eq!(recipe.policy(), policy);
                        assert_eq!(recipe.allow_throw_exception(), allow);
                        assert_eq!(
                            recipe.own_effects(context()).for_use(context()).unwrap(),
                            ExpressionEffects::PURE_VALUE
                        );
                        let foreign = ExpressionEffectContext {
                            domain: EvaluationDomainId::new(92),
                            ..context()
                        };
                        assert!(recipe.own_effects(context()).for_use(foreign).is_err());
                        let values = if source_nullable {
                            vec![Some(-7), Some(7), None]
                        } else {
                            vec![Some(-7), Some(7)]
                        };
                        let input = array(source, &values);
                        for (row, value) in values.iter().enumerate() {
                            let expected = match (value, target) {
                                (None, _) => CastRowResult::Null,
                                (Some(v), DataType::Float32) => {
                                    CastRowResult::Float32(if *v < 0 { -7.0 } else { 7.0 })
                                }
                                (Some(v), DataType::Float64) => {
                                    CastRowResult::Float64(if *v < 0 { -7.0 } else { 7.0 })
                                }
                                (Some(v), _) => CastRowResult::Signed(*v),
                            };
                            assert_eq!(
                                recipe
                                    .evaluate_row(
                                        EvaluatedArgument::Column(&input),
                                        row,
                                        row,
                                        &Control::default()
                                    )
                                    .unwrap(),
                                expected
                            );
                        }
                        // Independently retain a more conservative nullable result.
                        let wider = prepare(source_type, ty(target.clone(), true), policy, allow);
                        assert!(wider.result_type().nullable);
                    }
                }
            }
        }
    }
}

#[test]
fn signed_narrowing_is_successful_null_and_nonnullable_result_is_rejected() {
    for (target, minimum, maximum) in [
        (DataType::Int8, -128, 127),
        (DataType::Int16, -32768, 32767),
        (DataType::Int32, -2147483648, 2147483647),
    ] {
        let input = array(
            &DataType::Int64,
            &[
                Some(minimum - 1),
                Some(minimum),
                Some(maximum),
                Some(maximum + 1),
                None,
            ],
        );
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            for allow in [false, true] {
                let recipe = prepare(
                    ty(DataType::Int64, true),
                    ty(target.clone(), true),
                    policy,
                    allow,
                );
                for (row, expected) in [
                    CastRowResult::Null,
                    CastRowResult::Signed(minimum),
                    CastRowResult::Signed(maximum),
                    CastRowResult::Null,
                    CastRowResult::Null,
                ]
                .into_iter()
                .enumerate()
                {
                    assert_eq!(
                        recipe
                            .evaluate_row(
                                EvaluatedArgument::Column(&input),
                                row,
                                row,
                                &Control::default()
                            )
                            .unwrap(),
                        expected
                    );
                }
                assert_eq!(
                    PreparedCastRecipe::try_new(
                        CastOperation::Carrier,
                        &ty(DataType::Int64, false),
                        &ty(target.clone(), false),
                        policy,
                        allow,
                        &CompileControl::default()
                    ),
                    Err(CastPrepareError::TypeMismatch)
                );
            }
        }
    }
    assert_eq!(
        PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &ty(DataType::Int8, true),
            &ty(DataType::Float64, false),
            DecimalOverflowPolicy::OutputNull,
            false,
            &CompileControl::default()
        ),
        Err(CastPrepareError::TypeMismatch)
    );
}

#[test]
fn direct_integer_float_casts_keep_independent_rounding_bits_and_original_arrow_oracle() {
    // This I64 is one above an F32 halfway point. An intermediate F64 would
    // erase that bit and incorrectly choose the lower even F32.
    let input = array(
        &DataType::Int64,
        &[
            Some((1_i64 << 62) + (1_i64 << 38) + 1),
            Some((1_i64 << 24) + 1),
            Some((1_i64 << 53) + 1),
            Some((1_i64 << 53) + 3),
            Some(i64::MIN),
            Some(i64::MAX),
        ],
    );
    let f32_bits = [
        0x5e800001_u32,
        0x4b800000,
        0x5a000000,
        0x5a000000,
        0xdf000000,
        0x5f000000,
    ];
    let f64_bits = [
        0x43d0000010000000_u64,
        0x4170000010000000,
        0x4340000000000000,
        0x4340000000000002,
        0xc3e0000000000000,
        0x43e0000000000000,
    ];
    for target in [DataType::Float32, DataType::Float64] {
        let recipe = prepare(
            ty(DataType::Int64, false),
            ty(target.clone(), false),
            DecimalOverflowPolicy::ReportError,
            true,
        );
        let legacy = arrow_cast::cast(input.as_ref(), &target).unwrap();
        for row in 0..input.len() {
            match recipe
                .evaluate_row(
                    EvaluatedArgument::Column(&input),
                    row,
                    row,
                    &Control::default(),
                )
                .unwrap()
            {
                CastRowResult::Float32(value) => {
                    assert_eq!(value.to_bits(), f32_bits[row]);
                    assert_eq!(
                        value.to_bits(),
                        legacy
                            .as_any()
                            .downcast_ref::<arrow_array::Float32Array>()
                            .unwrap()
                            .value(row)
                            .to_bits()
                    );
                }
                CastRowResult::Float64(value) => {
                    assert_eq!(value.to_bits(), f64_bits[row]);
                    assert_eq!(
                        value.to_bits(),
                        legacy
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                            .value(row)
                            .to_bits()
                    );
                }
                other => panic!("unexpected cast result: {other:?}"),
            }
        }
    }
}

#[test]
fn original_constant_ordinal_scalar_slice_and_compact_addresses_are_independent() {
    let recipe = prepare(
        ty(DataType::Int64, true),
        ty(DataType::Int8, true),
        DecimalOverflowPolicy::OutputNull,
        true,
    );
    let constant = pool(true);
    assert_eq!(constant.ordinal(), 2);
    assert_eq!(
        recipe
            .evaluate_row(
                EvaluatedArgument::Constant(&constant),
                99,
                1001,
                &Control::default()
            )
            .unwrap(),
        CastRowResult::Null
    );
    let broad = prepare(
        ty(DataType::Int64, true),
        ty(DataType::Float64, true),
        DecimalOverflowPolicy::ReportError,
        false,
    );
    assert_eq!(
        broad
            .evaluate_row(
                EvaluatedArgument::Constant(&constant),
                99,
                1001,
                &Control::default()
            )
            .unwrap(),
        CastRowResult::Float64(256.0)
    );
    let null_value = ConstantPool::try_new(
        Arc::new(ty(DataType::Int64, true).try_to_field("null").unwrap()),
        ty(DataType::Int64, true),
        Int64Array::from(vec![Some(7), None, Some(9)]).to_data(),
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
    .unwrap();
    assert_eq!(
        recipe
            .evaluate_row(
                EvaluatedArgument::Constant(&null_value),
                100,
                900,
                &Control::default()
            )
            .unwrap(),
        CastRowResult::Null
    );
    let input = array(
        &DataType::Int64,
        &[Some(999), Some(-7), None, Some(i64::MAX)],
    )
    .slice(1, 3);
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let compact_array = array(&DataType::Int64, &[Some(-7), Some(i64::MAX)]);
    let compact =
        SelectedValues::try_new(selection, &DataType::Int64, compact_array, Box::default())
            .unwrap();
    for (ordinal, row) in selection.iter().enumerate() {
        let expected = if ordinal == 0 {
            CastRowResult::Signed(-7)
        } else {
            CastRowResult::Null
        };
        assert_eq!(
            recipe
                .evaluate_row(
                    EvaluatedArgument::Column(&input),
                    ordinal,
                    row,
                    &Control::default()
                )
                .unwrap(),
            expected
        );
        assert_eq!(
            recipe
                .evaluate_row(
                    EvaluatedArgument::SelectedColumn(&compact),
                    ordinal,
                    row,
                    &Control::default()
                )
                .unwrap(),
            expected
        );
    }
    let scalar = array(&DataType::Int64, &[Some(-7)]);
    assert_eq!(
        recipe
            .evaluate_row(
                EvaluatedArgument::Scalar(&scalar),
                71,
                999,
                &Control::default()
            )
            .unwrap(),
        CastRowResult::Signed(-7)
    );
    // A nullable source promise is not silently tightened for a non-NULL value.
    let nonnull = prepare(
        ty(DataType::Int64, false),
        ty(DataType::Float64, false),
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    assert!(matches!(
        nonnull.evaluate_row(
            EvaluatedArgument::Constant(&constant),
            0,
            0,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(
        nonnull
            .evaluate_row(
                EvaluatedArgument::Constant(&pool(false)),
                0,
                0,
                &Control::default()
            )
            .unwrap(),
        CastRowResult::Float64(256.0)
    );
}

#[test]
fn unsupported_operations_domains_and_foreign_runtime_addresses_are_never_null_fallbacks() {
    let source = ty(DataType::Int64, true);
    let target = ty(DataType::Int8, true);
    for operation in [CastOperation::Time, CastOperation::TimeFromDatetime] {
        assert_eq!(
            PreparedCastRecipe::try_new(
                operation,
                &source,
                &target,
                DecimalOverflowPolicy::OutputNull,
                false,
                &CompileControl::default()
            ),
            Err(CastPrepareError::Unsupported)
        );
    }
    for source in [
        ty(DataType::Binary, true),
        ty(DataType::Float16, true),
        ty(DataType::Decimal128(10, 0), true),
        ty(DataType::FixedSizeBinary(16), true),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::LargeInt,
        )
        .unwrap(),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    ] {
        assert_eq!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &source,
                &target,
                DecimalOverflowPolicy::ReportError,
                true,
                &CompileControl::default()
            ),
            Err(CastPrepareError::Unsupported)
        );
    }
    let recipe = prepare(
        ty(DataType::Int64, false),
        ty(DataType::Int8, true),
        DecimalOverflowPolicy::OutputNull,
        false,
    );
    let null = array(&DataType::Int64, &[None]);
    let wrong: ArrayRef = Arc::new(Float64Array::from(vec![None]));
    let many = array(&DataType::Int64, &[Some(7), Some(8)]);
    for argument in [
        EvaluatedArgument::Column(&null),
        EvaluatedArgument::Column(&wrong),
        EvaluatedArgument::Scalar(&many),
    ] {
        assert!(matches!(
            recipe.evaluate_row(argument, 0, 0, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
    assert!(matches!(
        recipe.evaluate_row(EvaluatedArgument::Column(&many), 0, 2, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let rows = [1];
    let selection = Selection::try_sparse(2, &rows).unwrap();
    let values = SelectedValues::try_new(
        selection,
        &DataType::Int64,
        array(&DataType::Int64, &[None]),
        vec![RowDataError::new(0, "required child failure")].into_boxed_slice(),
    )
    .unwrap();
    assert!(matches!(
        recipe.evaluate_row(
            EvaluatedArgument::SelectedColumn(&values),
            0,
            1,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert!(matches!(
        recipe.evaluate_row(
            EvaluatedArgument::SelectedColumn(&values),
            0,
            0,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let empty = array(&DataType::Int64, &[]);
    assert!(matches!(
        recipe.evaluate_row(EvaluatedArgument::Column(&empty), 0, 0, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn compile_original_control_keeps_entry_quantum_and_tail_on_every_exact_prefix() {
    let nested = ty(
        DataType::Struct(
            (0..320)
                .map(|i| {
                    Arc::new(
                        Field::new(format!("actual-source-{i:04}"), DataType::Int64, true)
                            .with_metadata(HashMap::from([(
                                "source-key".to_owned(),
                                "source-value".to_owned(),
                            )])),
                    )
                })
                .collect::<Vec<_>>()
                .into(),
        ),
        true,
    );
    let target = ty(DataType::Int8, true);
    for source in [ty(DataType::Int64, true), nested] {
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
        if matches!(source.data_type, DataType::Struct(_)) {
            assert!(trace.contains(&256));
        }
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = CompileControl {
                    refusal: Some((at, cause)),
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
fn runtime_original_seven_causes_keep_success_null_and_ordinary_error_tails_without_replay() {
    let recipe = prepare(
        ty(DataType::Int64, true),
        ty(DataType::Int8, true),
        DecimalOverflowPolicy::ReportError,
        true,
    );
    let valid = array(&DataType::Int64, &[Some(7), Some(256), None]);
    let wrong: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
    for (input, row) in [
        (&valid, 0),
        (&valid, 1),
        (&valid, 2),
        (&wrong, 0),
        (&valid, 3),
    ] {
        let baseline = Control::default();
        let _ = recipe.evaluate_row(EvaluatedArgument::Column(input), row, row, &baseline);
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
                    recipe.evaluate_row(EvaluatedArgument::Column(input), row, row, &control),
                    Err(cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
    // Each scalar row is bounded in constant work, with its own entry/tail.
    let wide = array(&DataType::Int64, &vec![Some(7); 320]);
    let control = Control::default();
    for row in 0..320 {
        assert_eq!(
            recipe
                .evaluate_row(EvaluatedArgument::Column(&wide), row, row, &control)
                .unwrap(),
            CastRowResult::Signed(7)
        );
    }
    assert_eq!(control.trace.lock().unwrap().len(), 640);
}

// A carrier with no primitive conversion kernel casts exactly to itself:
// the value passes unchanged, only nullability may widen, and no row can
// fail. Kernel-backed carriers keep their own row operation.
#[test]
fn same_carrier_without_a_kernel_prepares_an_identity_with_no_row_operation() {
    for carrier in [DataType::Utf8, DataType::Binary, DataType::Date32] {
        let identity = PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &ty(carrier.clone(), false),
            &ty(carrier.clone(), true),
            DecimalOverflowPolicy::OutputNull,
            true,
            &CompileControl::default(),
        )
        .unwrap_or_else(|error| panic!("{carrier:?} identity: {error:?}"));
        assert!(identity.is_identity(), "{carrier:?}");
        assert!(
            !identity
                .own_effects(context())
                .for_use(context())
                .unwrap()
                .may_raise_row_error,
            "{carrier:?}"
        );
        let array: ArrayRef = arrow_array::new_null_array(&carrier, 1);
        assert!(matches!(
            identity.evaluate_row(EvaluatedArgument::Column(&array), 0, 0, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
        assert!(matches!(
            PreparedCastRecipe::try_new(
                CastOperation::Carrier,
                &ty(carrier.clone(), true),
                &ty(carrier.clone(), false),
                DecimalOverflowPolicy::OutputNull,
                true,
                &CompileControl::default(),
            ),
            Err(CastPrepareError::TypeMismatch)
        ));
    }
    let kernel = PreparedCastRecipe::try_new(
        CastOperation::Carrier,
        &ty(DataType::Int64, false),
        &ty(DataType::Int64, true),
        DecimalOverflowPolicy::OutputNull,
        true,
        &CompileControl::default(),
    )
    .unwrap();
    assert!(
        !kernel.is_identity(),
        "a kernel-backed carrier keeps its row operation"
    );
    assert!(matches!(
        PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &ty(DataType::Utf8, false),
            &ty(DataType::Binary, true),
            DecimalOverflowPolicy::OutputNull,
            true,
            &CompileControl::default(),
        ),
        Err(CastPrepareError::Unsupported)
    ));
}

#[test]
fn signed_successful_null_fact_matches_exact_carriers_independently_of_policy_and_allow() {
    let signed = carriers();
    for (source_width, source) in signed.iter().enumerate() {
        for (target_width, target) in signed.iter().enumerate() {
            let expected = target_width < source_width;
            assert_eq!(
                signed_integer_cast_can_produce_null(source, target),
                expected
            );
            for allow in [false, true] {
                assert_eq!(
                    carrier_cast_can_produce_null(source, target, allow),
                    expected
                );
                for policy in [
                    DecimalOverflowPolicy::OutputNull,
                    DecimalOverflowPolicy::ReportError,
                ] {
                    assert_eq!(
                        carrier_cast_can_produce_null_with_policy(source, target, policy, allow),
                        expected
                    );
                }
            }
        }
    }
    for (source, target) in [
        (DataType::UInt64, DataType::Int32),
        (DataType::Int64, DataType::UInt64),
        (DataType::Float64, DataType::Int32),
        (DataType::Utf8, DataType::Int32),
        (DataType::Boolean, DataType::Int8),
        (DataType::Date32, DataType::Int32),
        (DataType::FixedSizeBinary(16), DataType::Int64),
    ] {
        assert!(!signed_integer_cast_can_produce_null(&source, &target));
    }
    assert!(carrier_cast_can_produce_null(
        &DataType::UInt64,
        &DataType::Int32,
        true
    ));
    assert!(carrier_cast_can_produce_null(
        &DataType::Int64,
        &DataType::UInt64,
        true
    ));
    assert!(carrier_cast_can_produce_null(
        &DataType::Float64,
        &DataType::Int32,
        false
    ));
    assert!(!carrier_cast_can_produce_null(
        &DataType::Float64,
        &DataType::Int32,
        true
    ));
    assert!(carrier_cast_can_produce_null(
        &DataType::Utf8,
        &DataType::Int32,
        true
    ));
}
