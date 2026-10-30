// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::super::calendar_diff_owner::{
    effects, operation, owner_for_test, prepared_for_test_with_control,
    prepared_for_test_with_policy,
};
use super::*;
use crate::{
    ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionArgument, FunctionBindingRequest,
    FunctionBindingResolver, FunctionResultType, FunctionSpecializationFailure,
    PureFunctionMetadataOwner, PureScalarImplementation, ScalarEvaluationInstance,
    ScopedExpressionEffects, Selection, specialize_frozen_scalar, specialize_scalar,
};
use arrow_array::Int32Array;
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionEffectContext, ExpressionUseId, FunctionInstanceState,
    FunctionNullBehavior, FunctionVolatility, PureCompileControl, SemanticParameters,
};
use std::{sync::Mutex, time::Duration};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after first cause");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("calendar difference never waits")
    }
}
#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "compile callback after first cause");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}

const NAMES: [&str; 2] = ["datediff", "days_diff"];
fn text_type(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Utf8, nullable)
}
fn result_type() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, true)
}
fn profiles(nullable: bool) -> [FunctionValueType; 3] {
    [
        FunctionValueType::new(DataType::Timestamp(TimeUnit::Microsecond, None), nullable),
        FunctionValueType::new(DataType::Date32, nullable),
        text_type(nullable),
    ]
}
fn strings(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
fn instance(name: &str, source: &FunctionValueType) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_policy(
            name,
            &[source.clone(), source.clone()],
            DecimalOverflowPolicy::OutputNull,
        )
        .unwrap(),
    )
    .unwrap()
}
fn output(result: &SelectedValues<'_>) -> Vec<Option<i64>> {
    assert!(result.errors().is_empty());
    result
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}
fn matrices() -> [(ArrayRef, ArrayRef); 3] {
    [
        (
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1709210096000000),
                Some(1709251200000000),
                Some(1451606400000000),
                Some(-1),
                Some(1709210096000000),
                Some(i64::MAX),
                None,
                Some(0),
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1709078400000000),
                Some(1709078400000000),
                Some(1451606399000000),
                Some(0),
                Some(1709164800000000),
                Some(0),
                Some(0),
                None,
            ])),
        ),
        (
            Arc::new(Date32Array::from(vec![
                Some(19782),
                Some(19783),
                Some(16801),
                Some(-1),
                Some(19782),
                Some(i32::MAX),
                None,
                Some(0),
            ])),
            Arc::new(Date32Array::from(vec![
                Some(19781),
                Some(19781),
                Some(16800),
                Some(0),
                Some(19782),
                Some(0),
                Some(0),
                None,
            ])),
        ),
        (
            strings(vec![
                Some("2024-02-29 12:34:56"),
                Some("2024-03-01"),
                Some("2016-01-01 00:00:00"),
                Some("1969-12-31 23:59:59.999999"),
                Some("20240229123456"),
                Some("20230229"),
                None,
                Some("1970-01-01"),
            ]),
            strings(vec![
                Some("2024-02-28"),
                Some("2024-02-28"),
                Some("2015-12-31 23:59:59"),
                Some("1970-01-01"),
                Some("2024-02-29 00:00:00"),
                Some("1970-01-01"),
                Some("1970-01-01"),
                None,
            ]),
        ),
    ]
}
fn pool(array: &ArrayRef, source: FunctionValueType) -> ConstantPool {
    ConstantPool::try_new(
        Arc::new(source.try_to_field("original").unwrap().with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "source-note".into(),
                "selected date-only difference".into(),
            )]),
        )),
        source,
        array.to_data(),
        ConstantPolicy {
            max_rows: 8,
            max_array_nodes: 1,
            max_logical_elements: 8,
            max_retained_buffer_bytes: 2 * 1024 * 1024,
            max_type_depth: 1,
            max_type_nodes: 1,
            max_dictionary_depth: 0,
            max_metadata_bytes: 4096,
            max_library_validation_work: 4 * 1024 * 1024,
            max_library_validation_bytes: 4 * 1024 * 1024,
        },
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap()
}

#[test]
fn calendar_diff_all_six_actual_records_preserve_fresh_frozen_selected_identity_and_effects() {
    for name in NAMES {
        let owner = owner_for_test(name);
        assert_eq!(owner.binding_declaration().overloads().len(), 3);
        assert_eq!(owner.implementation_declarations().len(), 3);
        assert_eq!(
            owner.binding_declaration().function_id().as_str(),
            format!("builtin.scalar/{name}/v1")
        );
        for record in owner.implementation_declarations() {
            assert_eq!(
                record.implementation.as_str(),
                format!("builtin.scalar/{name}/selected-v1")
            );
        }
        for source in profiles(true) {
            let arguments = [
                FunctionArgument::Value {
                    value_type: source.clone(),
                    constant: None,
                },
                FunctionArgument::Value {
                    value_type: source,
                    constant: None,
                },
            ];
            let request = FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: 2,
                expected_result_type: None,
            };
            let selected = Arc::new(
                owner
                    .resolve(request, crate::binding_test_control())
                    .unwrap(),
            );
            assert_eq!(
                selected.result_type,
                FunctionResultType::Scalar(result_type())
            );
            let context = ExpressionEffectContext {
                use_id: ExpressionUseId::new(41),
                domain: EvaluationDomainId::new(7),
                demand: EvaluationDemand::Value,
            };
            let parameters = SemanticParameters::try_new([]).unwrap();
            let uses = [
                Some(ExpressionUseId::new(42)),
                Some(ExpressionUseId::new(43)),
            ];
            let input = crate::CallEffectInput {
                context,
                argument_uses: crate::CallArgumentUses::SelectedChannels(&uses),
                function_id: owner.binding_declaration().function_id(),
                kind: crate::FunctionKind::Scalar,
                selected: &selected,
                request,
                environment: &[],
                parameters: &parameters,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                proof_scope: CallProofScope::Domain(context.domain),
            };
            let fresh = specialize_scalar(
                &owner,
                input,
                selected.clone(),
                ScopedExpressionEffects::pure_value(context),
                crate::binding_test_control(),
            )
            .unwrap();
            let canonical = fresh.prepared().contract().clone();
            let direct = owner
                .prepare_scalar(input, canonical.clone(), crate::binding_test_control())
                .unwrap();
            assert!(Arc::ptr_eq(direct.contract(), &canonical));
            let frozen = specialize_frozen_scalar(
                &owner,
                input,
                selected.clone(),
                canonical.effects(),
                ScopedExpressionEffects::pure_value(context),
                crate::binding_test_control(),
            )
            .unwrap();
            assert!(std::ptr::eq(
                frozen.prepared().contract().selected(),
                selected.as_ref()
            ));
            assert_eq!(
                canonical.effects().value_stability,
                FunctionVolatility::Immutable
            );
            assert_eq!(
                canonical.effects().own_row_error,
                crate::FunctionIntrinsicRowError::NoRowError
            );
            assert_eq!(
                canonical.effects().null_behavior,
                FunctionNullBehavior::Strict
            );
            assert_eq!(
                canonical.effects().argument_control,
                novarocks_type_contract::ArgumentControl::Eager
            );
            assert_eq!(
                canonical.effects().instance_state,
                FunctionInstanceState::None
            );
            assert!(canonical.effects().observable_effects.is_empty());
            assert!(canonical.effects().environment.is_empty());
            assert_eq!(
                canonical.decimal_overflow_policy(),
                DecimalOverflowPolicy::ReportError
            );
            let foreign = crate::FunctionId::try_new("foreign.calendar/selected/v1").unwrap();
            let mut wrong = input;
            wrong.function_id = &foreign;
            assert!(
                owner
                    .prepare_scalar(wrong, canonical.clone(), crate::binding_test_control())
                    .is_err()
            );
            wrong = input;
            wrong.kind = crate::FunctionKind::Aggregate;
            assert!(
                owner
                    .prepare_scalar(wrong, canonical.clone(), crate::binding_test_control())
                    .is_err()
            );
            let copy = (*selected).clone();
            let mut stale = input;
            stale.selected = &copy;
            assert!(
                owner
                    .prepare_scalar(stale, canonical.clone(), crate::binding_test_control())
                    .is_err()
            );
            stale = input;
            stale.decimal_overflow_policy = DecimalOverflowPolicy::OutputNull;
            assert!(
                owner
                    .prepare_scalar(stale, canonical.clone(), crate::binding_test_control())
                    .is_err()
            );
            stale = input;
            stale.context.domain = EvaluationDomainId::new(999);
            assert!(
                owner
                    .prepare_scalar(stale, canonical.clone(), crate::binding_test_control())
                    .is_err()
            );
            let mut forged = (*selected).clone();
            forged.result_type =
                FunctionResultType::Scalar(FunctionValueType::new(DataType::Int64, false));
            assert!(
                owner
                    .validate_selected(&forged, request, crate::binding_test_control())
                    .is_err()
            );
        }
        for overload in owner.binding_declaration().overloads() {
            assert_eq!(overload.effects.as_ref(), Some(&effects()));
        }
    }
    assert_eq!(operation("datediff"), operation("days_diff"));
    for ghost in ["date_diff", "timestampdiff", "days_add", "date_difference"] {
        assert!(operation(ghost).is_none());
    }
}

#[test]
fn calendar_diff_all_six_profiles_preserve_calendar_day_oracles_nullable_and_policy() {
    for name in NAMES {
        for ((left, right), source) in matrices().iter().zip(profiles(true)) {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                let prepared =
                    prepared_for_test_with_policy(name, &[source.clone(), source.clone()], policy)
                        .unwrap();
                assert_eq!(prepared.contract().result_type(), &result_type());
                assert_eq!(prepared.contract().decimal_overflow_policy(), policy);
                let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
                let args = [
                    EvaluatedArgument::Column(left),
                    EvaluatedArgument::Column(right),
                ];
                assert_eq!(
                    output(
                        &kernel
                            .evaluate(Selection::all(8), &args, &Control::default())
                            .unwrap()
                    ),
                    vec![
                        Some(1),
                        Some(2),
                        Some(1),
                        Some(-1),
                        Some(0),
                        None,
                        None,
                        None
                    ]
                );
                for mask in 0..4 {
                    let mut types = [source.clone(), source.clone()];
                    for (index, ty) in types.iter_mut().enumerate() {
                        *ty = ty
                            .with_nullable(mask & (1 << index) != 0)
                            .expect("valid function nullability declaration");
                    }
                    let prepared = prepared_for_test_with_policy(name, &types, policy).unwrap();
                    assert_eq!(prepared.contract().result_type(), &result_type());
                    let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
                    let left = left.slice(0, 6);
                    let right = right.slice(0, 6);
                    assert_eq!(
                        output(
                            &kernel
                                .evaluate(
                                    Selection::all(6),
                                    &[
                                        EvaluatedArgument::Column(&left),
                                        EvaluatedArgument::Column(&right)
                                    ],
                                    &Control::default()
                                )
                                .unwrap()
                        ),
                        vec![Some(1), Some(2), Some(1), Some(-1), Some(0), None]
                    );
                }
            }
        }
    }
}

#[test]
fn calendar_diff_full_chrono_date_domain_and_signed_extremes_do_not_add_timestamp_year_gates() {
    // Independently calculated proleptic Gregorian epoch days for Chrono's
    // source bounds -262143-01-01 and 262142-12-31, not StarRocks 0..9999.
    let left: ArrayRef = Arc::new(Date32Array::from(vec![
        -96465292, 95026236, -96465293, 95026237,
    ]));
    let right: ArrayRef = Arc::new(Date32Array::from(vec![95026236, -96465292, 0, 0]));
    for name in NAMES {
        let mut kernel = instance(name, &profiles(true)[1]);
        assert_eq!(
            output(
                &kernel
                    .evaluate(
                        Selection::all(4),
                        &[
                            EvaluatedArgument::Column(&left),
                            EvaluatedArgument::Column(&right)
                        ],
                        &Control::default()
                    )
                    .unwrap()
            ),
            vec![Some(-191491528), Some(191491528), None, None]
        );
    }
}

#[test]
fn calendar_diff_independent_original_compact_scalar_slice_and_cv_addresses_are_preserved() {
    for ((left, right), source) in matrices().iter().zip(profiles(true)) {
        let left_pool = pool(left, source.clone());
        let right_pool = pool(right, source.clone());
        let left_value = left_pool.value(1).unwrap();
        let right_value = right_pool.value(1).unwrap();
        assert!(Arc::ptr_eq(left_value.pool().array(), left_pool.array()));
        assert!(Arc::ptr_eq(right_value.pool().array(), right_pool.array()));
        let rows = [0, 2];
        let selection = Selection::try_sparse(8, &rows).unwrap();
        let scalar = left.slice(1, 1);
        let compact = SelectedValues::try_new(
            selection,
            source.data_type(),
            right.slice(0, 2),
            Box::default(),
        )
        .unwrap();
        for (args, expected) in [
            (
                [
                    EvaluatedArgument::Column(left),
                    EvaluatedArgument::Column(right),
                ],
                vec![Some(1), Some(1)],
            ),
            (
                [
                    EvaluatedArgument::Column(left),
                    EvaluatedArgument::SelectedColumn(&compact),
                ],
                vec![Some(1), Some(-2980)],
            ),
            (
                [
                    EvaluatedArgument::Constant(&left_value),
                    EvaluatedArgument::Column(right),
                ],
                vec![Some(2), Some(2983)],
            ),
            (
                [
                    EvaluatedArgument::Scalar(&scalar),
                    EvaluatedArgument::Constant(&right_value),
                ],
                vec![Some(2), Some(2)],
            ),
            (
                [
                    EvaluatedArgument::Constant(&left_value),
                    EvaluatedArgument::Constant(&right_value),
                ],
                vec![Some(2), Some(2)],
            ),
        ] {
            let mut kernel = instance("days_diff", &source);
            let output = kernel
                .evaluate(selection, &args, &Control::default())
                .unwrap();
            assert_eq!(output.selection(), selection);
            assert_eq!(self::output(&output), expected);
        }
        let left = left.slice(1, 2);
        let right = right.slice(1, 2);
        let mut kernel = instance("datediff", &source);
        assert_eq!(
            output(
                &kernel
                    .evaluate(
                        Selection::all(2),
                        &[
                            EvaluatedArgument::Column(&left),
                            EvaluatedArgument::Column(&right)
                        ],
                        &Control::default()
                    )
                    .unwrap()
            ),
            vec![Some(2), Some(1)]
        );
    }
}

#[test]
fn calendar_diff_strict_null_hidden_inactive_empty_child_and_bad_address_never_bypass_protocol() {
    let huge = "9".repeat(320 * 1024);
    let mut bytes = huge.as_bytes().to_vec();
    bytes.extend_from_slice(b"2024-02-29");
    let mut validity = BooleanBufferBuilder::new(2);
    validity.append(false);
    validity.append(true);
    let hidden: ArrayRef = Arc::new(StringArray::new(
        arrow_buffer::OffsetBuffer::new(
            vec![0, huge.len() as i32, (huge.len() + 10) as i32].into(),
        ),
        arrow_buffer::Buffer::from(bytes),
        Some(NullBuffer::new(validity.finish())),
    ));
    let ordinary = strings(vec![Some("2024-02-28"), Some("2024-02-28")]);
    for position in 0..2 {
        let args = std::array::from_fn::<_, 2, _>(|i| {
            EvaluatedArgument::Column(if i == position { &hidden } else { &ordinary })
        });
        let mut kernel = instance("datediff", &text_type(true));
        let control = Control::default();
        assert_eq!(
            output(&kernel.evaluate(Selection::all(2), &args, &control).unwrap()),
            vec![None, Some(if position == 0 { 1 } else { -1 })]
        );
        assert!(!control.trace.lock().unwrap().contains(&256));
        let inactive = strings(vec![Some(&huge), Some("2024-02-29")]);
        let rows = [1];
        let selection = Selection::try_sparse(2, &rows).unwrap();
        let args = std::array::from_fn::<_, 2, _>(|i| {
            EvaluatedArgument::Column(if i == position { &inactive } else { &ordinary })
        });
        let control = Control::default();
        assert_eq!(
            output(&kernel.evaluate(selection, &args, &control).unwrap()),
            vec![Some(if position == 0 { 1 } else { -1 })]
        );
        assert!(!control.trace.lock().unwrap().contains(&256));
    }
    let empty = [];
    let selection = Selection::try_sparse(2, &empty).unwrap();
    let mut kernel = instance("days_diff", &text_type(true));
    assert!(
        output(
            &kernel
                .evaluate(
                    selection,
                    &[
                        EvaluatedArgument::Column(&hidden),
                        EvaluatedArgument::Column(&ordinary)
                    ],
                    &Control::default()
                )
                .unwrap()
        )
        .is_empty()
    );
    let failed = SelectedValues::try_new(
        Selection::all(2),
        &DataType::Utf8,
        strings(vec![None, None]),
        Box::from([crate::RowDataError::new(0, "required date child failed")]),
    )
    .unwrap();
    for position in 0..2 {
        let args = std::array::from_fn::<_, 2, _>(|i| {
            if i == position {
                EvaluatedArgument::SelectedColumn(&failed)
            } else {
                EvaluatedArgument::Column(&ordinary)
            }
        });
        let mut kernel = instance("days_diff", &text_type(true));
        assert!(matches!(
            kernel.evaluate(Selection::all(2), &args, &Control::default()),
            Err(ref cause) if crate::kernel_control::test_contract::input(cause)
        ));
        let after = Control::default();
        assert_eq!(
            kernel
                .evaluate(
                    Selection::all(2),
                    &[
                        EvaluatedArgument::Column(&ordinary),
                        EvaluatedArgument::Column(&ordinary)
                    ],
                    &after
                )
                .unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert!(after.trace.lock().unwrap().is_empty());
    }
    let null = strings(vec![None]);
    let empty = strings(vec![]);
    let mut kernel = instance("days_diff", &text_type(true));
    assert!(matches!(
        kernel.evaluate(
            Selection::all(1),
            &[
                EvaluatedArgument::Scalar(&null),
                EvaluatedArgument::Column(&empty)
            ],
            &Control::default()
        ),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
    let types = [text_type(false), text_type(true)];
    let ordinary_scalar = ordinary.slice(0, 1);
    let mut kernel = ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_policy("datediff", &types, DecimalOverflowPolicy::OutputNull)
            .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        kernel.evaluate(
            Selection::all(1),
            &[
                EvaluatedArgument::Scalar(&null),
                EvaluatedArgument::Scalar(&ordinary_scalar)
            ],
            &Control::default()
        ),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
}

#[test]
fn calendar_diff_exact_selected_full_profiles_arity_and_output_request_layout_are_closed() {
    for name in NAMES {
        for source in [
            FunctionValueType::new(DataType::Int64, true),
            FunctionValueType::new(DataType::Timestamp(TimeUnit::Second, None), true),
            FunctionValueType::new(
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            FunctionValueType::new(DataType::LargeUtf8, true),
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::LargeInt,
            )
            .unwrap(),
        ] {
            assert!(
                prepared_for_test_with_policy(
                    name,
                    &[source.clone(), source],
                    DecimalOverflowPolicy::OutputNull
                )
                .is_err()
            );
        }
        for args in [
            vec![],
            vec![text_type(true)],
            vec![text_type(true); 3],
            vec![
                text_type(true),
                FunctionValueType::new(DataType::Date32, true),
            ],
        ] {
            assert!(
                prepared_for_test_with_policy(name, &args, DecimalOverflowPolicy::OutputNull)
                    .is_err()
            );
        }
    }
    assert!(output_capacity(0).is_ok());
    assert_eq!(
        output_capacity(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(
        output_capacity(isize::MAX as usize / 8 + 1),
        Err(KernelFailure::ResourceExhausted)
    );
}

#[test]
fn calendar_diff_every_actual_compile_callback_preserves_three_causes_success_and_ordinary_tail() {
    for source in [
        text_type(true),
        FunctionValueType::new(DataType::Int64, true),
    ] {
        let types = [source.clone(), source.clone()];
        let good = CompileControl::default();
        assert_eq!(
            prepared_for_test_with_control(
                "datediff",
                &types,
                DecimalOverflowPolicy::OutputNull,
                &good
            )
            .is_ok(),
            source.data_type() == &DataType::Utf8
        );
        let trace = good.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = CompileControl {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let error = prepared_for_test_with_control(
                    "datediff",
                    &types,
                    DecimalOverflowPolicy::OutputNull,
                    &control,
                )
                .err()
                .unwrap();
                let actual = match error {
                    FunctionSpecializationFailure::Control(cause) => Some(cause),
                    FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled) => {
                        Some(CompileControlError::Cancelled)
                    }
                    FunctionSpecializationFailure::Kernel(KernelFailure::DeadlineExceeded) => {
                        Some(CompileControlError::DeadlineExceeded)
                    }
                    FunctionSpecializationFailure::Kernel(KernelFailure::ResourceExhausted) => {
                        Some(CompileControlError::ResourceExhausted)
                    }
                    _ => None,
                };
                assert_eq!(actual, Some(cause));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn calendar_diff_every_runtime_callback_preserves_seven_causes_real_parser_quantum_and_failed_latch()
 {
    let long = format!("2024-{}02-29", "!".repeat(320));
    let cases = [
        (
            strings(vec![Some("2024-02-29")]),
            strings(vec![Some("2024-02-28")]),
            text_type(true),
            true,
        ),
        (
            strings(vec![Some(&long)]),
            strings(vec![Some("2024-02-28")]),
            text_type(true),
            true,
        ),
        (
            strings(vec![Some("invalid")]),
            strings(vec![Some(&long)]),
            text_type(true),
            true,
        ),
        (
            Arc::new(Date32Array::from(vec![1])) as ArrayRef,
            Arc::new(Date32Array::from(vec![0])) as ArrayRef,
            profiles(true)[1].clone(),
            true,
        ),
        (
            Arc::new(TimestampMicrosecondArray::from(vec![-1])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(vec![0])) as ArrayRef,
            profiles(true)[0].clone(),
            true,
        ),
        (
            strings(vec![None]),
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            text_type(true),
            false,
        ),
    ];
    for (left, right, source, success) in cases {
        let args = [
            EvaluatedArgument::Column(&left),
            EvaluatedArgument::Column(&right),
        ];
        let prepared = prepared_for_test_with_policy(
            "datediff",
            &[source.clone(), source],
            DecimalOverflowPolicy::OutputNull,
        )
        .unwrap();
        let good = Control::default();
        let mut kernel = ScalarEvaluationInstance::instantiate(prepared.clone()).unwrap();
        assert_eq!(
            kernel.evaluate(Selection::all(1), &args, &good).is_ok(),
            success
        );
        let trace = good.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        if [&left, &right].iter().any(|array| {
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .is_some_and(|s| !s.is_null(0) && s.value(0).len() > 256)
        }) {
            assert!(trace.contains(&256));
        }
        for at in 0..trace.len() {
            for cause in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
                invalid("original refusal"),
                internal("original refusal"),
                KernelFailure::Operational(crate::KernelDiagnostic::new("original refusal")),
                KernelFailure::InstanceFailed,
            ] {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let mut kernel = ScalarEvaluationInstance::instantiate(prepared.clone()).unwrap();
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(1), &args, &control)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                let after = Control::default();
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(1), &args, &after)
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}
