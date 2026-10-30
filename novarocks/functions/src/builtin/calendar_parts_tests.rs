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

use super::super::calendar_parts_owner::{
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
use arrow_array::{Int64Array, builder::FixedSizeBinaryBuilder};
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
        panic!("calendar parts never waits")
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

const NAMES: [&str; 12] = [
    "year",
    "month",
    "day",
    "dayofmonth",
    "hour",
    "minute",
    "second",
    "dayofweek",
    "yearweek",
    "dayofyear",
    "weekofyear",
    "quarter",
];
const ISO: [i32; 12] = [2016, 1, 1, 1, 12, 34, 56, 6, 201553, 1, 53, 1];
const LEAP: [i32; 12] = [2024, 2, 29, 29, 12, 34, 56, 5, 202409, 60, 9, 1];
fn text_type(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Utf8, nullable)
}
fn large_type(nullable: bool) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        nullable,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
}
fn result_type() -> FunctionValueType {
    FunctionValueType::new(DataType::Int32, true)
}
fn profiles(nullable: bool) -> [FunctionValueType; 4] {
    [
        FunctionValueType::new(DataType::Timestamp(TimeUnit::Microsecond, None), nullable),
        large_type(nullable),
        FunctionValueType::new(DataType::Date32, nullable),
        text_type(nullable),
    ]
}
fn strings(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
fn large(values: &[Option<i128>]) -> ArrayRef {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
    for value in values {
        match value {
            Some(value) => builder.append_value(value.to_be_bytes()).unwrap(),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}
fn instance(name: &str, source: &FunctionValueType) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_policy(
            name,
            std::slice::from_ref(source),
            DecimalOverflowPolicy::OutputNull,
        )
        .unwrap(),
    )
    .unwrap()
}
fn output(result: &SelectedValues<'_>) -> Vec<Option<i32>> {
    assert!(result.errors().is_empty());
    result
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .collect()
}
fn matrix_arrays() -> [ArrayRef; 4] {
    [
        Arc::new(TimestampMicrosecondArray::from(vec![
            Some(1451651696000000),
            Some(1709210096000000),
            None,
        ])),
        large(&[Some(20160101123456), Some(20240229123456), None]),
        Arc::new(Date32Array::from(vec![Some(16801), Some(19782), None])),
        strings(vec![
            Some("2016-01-01 12:34:56"),
            Some("2024-02-29 12:34:56"),
            None,
        ]),
    ]
}
fn pool(array: &ArrayRef, source: FunctionValueType) -> ConstantPool {
    ConstantPool::try_new(
        Arc::new(source.try_to_field("original").unwrap().with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "source-note".into(),
                "selected calendar".into(),
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
fn calendar_parts_all_48_actual_records_preserve_fresh_frozen_selected_identity_and_effects() {
    for name in NAMES {
        let owner = owner_for_test(name);
        assert_eq!(owner.binding_declaration().overloads().len(), 4);
        assert_eq!(owner.implementation_declarations().len(), 4);
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
            let arguments = [FunctionArgument::Value {
                value_type: source,
                constant: None,
            }];
            let request = FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: 1,
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
            let uses = [Some(ExpressionUseId::new(42))];
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
                FunctionResultType::Scalar(FunctionValueType::new(DataType::Int32, false));
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
    assert_eq!(operation("day"), operation("dayofmonth"));
    for ghost in [
        "dayname",
        "monthname",
        "week",
        "weekday",
        "dayofweek_iso",
        "last_day",
    ] {
        assert!(operation(ghost).is_none());
    }
}

#[test]
fn calendar_parts_all_profiles_preserve_iso_leap_midnight_nullable_and_policy_oracles() {
    let arrays = matrix_arrays();
    for (index, name) in NAMES.iter().enumerate() {
        for (profile, source) in profiles(true).iter().enumerate() {
            for nullable in [true, false] {
                for policy in [
                    DecimalOverflowPolicy::OutputNull,
                    DecimalOverflowPolicy::ReportError,
                ] {
                    let mut source = source.clone();
                    source = source
                        .with_nullable(nullable)
                        .expect("valid function nullability declaration");
                    let array = if nullable {
                        arrays[profile].clone()
                    } else {
                        arrays[profile].slice(0, 2)
                    };
                    let prepared = prepared_for_test_with_policy(name, &[source], policy).unwrap();
                    assert_eq!(prepared.contract().result_type(), &result_type());
                    assert_eq!(prepared.contract().decimal_overflow_policy(), policy);
                    let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
                    let arguments = [EvaluatedArgument::Column(&array)];
                    let result = kernel
                        .evaluate(Selection::all(array.len()), &arguments, &Control::default())
                        .unwrap();
                    let midnight = profile == 2 && (4..=6).contains(&index);
                    let mut expected = vec![
                        Some(if midnight { 0 } else { ISO[index] }),
                        Some(if midnight { 0 } else { LEAP[index] }),
                    ];
                    if nullable {
                        expected.push(None);
                    }
                    assert_eq!(output(&result), expected, "{name}/{profile}/{nullable}");
                }
            }
        }
    }
}

#[test]
fn calendar_parts_negative_epoch_out_of_range_and_numeric_calendar_are_successful_null() {
    let negative: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        Some(-1),
        Some(i64::MIN),
        Some(i64::MAX),
        None,
    ]));
    let expected = [1969, 12, 31, 31, 23, 59, 59, 4, 197001, 365, 1, 4];
    for (index, name) in NAMES.iter().enumerate() {
        let mut kernel = instance(name, &profiles(true)[0]);
        assert_eq!(
            output(
                &kernel
                    .evaluate(
                        Selection::all(4),
                        &[EvaluatedArgument::Column(&negative)],
                        &Control::default()
                    )
                    .unwrap()
            ),
            vec![Some(expected[index]), None, None, None]
        );
    }
    let dates: ArrayRef = Arc::new(Date32Array::from(vec![
        Some(0),
        Some(-1),
        Some(i32::MIN),
        Some(i32::MAX),
    ]));
    let mut kernel = instance("year", &profiles(true)[2]);
    assert_eq!(
        output(
            &kernel
                .evaluate(
                    Selection::all(4),
                    &[EvaluatedArgument::Column(&dates)],
                    &Control::default()
                )
                .unwrap()
        ),
        vec![Some(1970), Some(1969), None, None]
    );
    let numeric = large(&[
        Some(690101),
        Some(700101),
        Some(101),
        Some(20240229123456),
        Some(20230229),
        Some(0),
        Some(-1),
        Some(i128::MAX),
        Some(i128::MIN),
        Some(i64::MAX as i128),
    ]);
    let mut kernel = instance("year", &large_type(true));
    assert_eq!(
        output(
            &kernel
                .evaluate(
                    Selection::all(numeric.len()),
                    &[EvaluatedArgument::Column(&numeric)],
                    &Control::default()
                )
                .unwrap()
        ),
        vec![
            Some(2069),
            Some(1970),
            Some(2000),
            Some(2024),
            None,
            None,
            None,
            None,
            None,
            None
        ]
    );
}

#[test]
fn calendar_parts_text_grammar_reuses_selected_parser_without_epoch_or_case_reinterpretation() {
    let text = strings(vec![
        Some("20240229123456"),
        Some("2024-02-29T01:02:03.456789"),
        Some("2024/02/29 01:02:03"),
        Some(" 2024-02-29 "),
        Some("700101"),
        Some("20230229"),
        Some("not a date"),
        None,
    ]);
    for (name, expected) in [
        (
            "year",
            vec![
                Some(2024),
                Some(2024),
                Some(2024),
                Some(2024),
                Some(1970),
                None,
                None,
                None,
            ],
        ),
        (
            "hour",
            vec![
                Some(12),
                Some(1),
                Some(1),
                Some(0),
                Some(0),
                None,
                None,
                None,
            ],
        ),
        (
            "second",
            vec![
                Some(56),
                Some(3),
                Some(3),
                Some(0),
                Some(0),
                None,
                None,
                None,
            ],
        ),
    ] {
        let mut kernel = instance(name, &text_type(true));
        assert_eq!(
            output(
                &kernel
                    .evaluate(
                        Selection::all(text.len()),
                        &[EvaluatedArgument::Column(&text)],
                        &Control::default()
                    )
                    .unwrap()
            ),
            expected
        );
    }
}

#[test]
fn calendar_parts_original_slices_compact_scalar_and_cv_ordinals_retain_source_backing() {
    for (profile, array) in matrix_arrays().iter().enumerate() {
        let source = profiles(true)[profile].clone();
        let pool = pool(array, source.clone());
        let value = pool.value(1).unwrap();
        assert!(Arc::ptr_eq(value.pool().array(), pool.array()));
        let rows = [0, 2];
        let selection = Selection::try_sparse(3, &rows).unwrap();
        let scalar = array.slice(1, 1);
        let compact = SelectedValues::try_new(
            selection,
            source.data_type(),
            array.slice(0, 2),
            Box::default(),
        )
        .unwrap();
        for argument in [
            EvaluatedArgument::Constant(&value),
            EvaluatedArgument::Scalar(&scalar),
        ] {
            let mut kernel = instance("day", &source);
            assert_eq!(
                output(
                    &kernel
                        .evaluate(selection, &[argument], &Control::default())
                        .unwrap()
                ),
                vec![Some(29), Some(29)]
            );
        }
        let mut kernel = instance("day", &source);
        assert_eq!(
            output(
                &kernel
                    .evaluate(
                        selection,
                        &[EvaluatedArgument::SelectedColumn(&compact)],
                        &Control::default()
                    )
                    .unwrap()
            ),
            vec![Some(1), Some(29)]
        );
        let sliced = array.slice(1, 2);
        let selected = [0];
        let selection = Selection::try_sparse(2, &selected).unwrap();
        assert_eq!(
            output(
                &kernel
                    .evaluate(
                        selection,
                        &[EvaluatedArgument::Column(&sliced)],
                        &Control::default()
                    )
                    .unwrap()
            ),
            vec![Some(29)]
        );
    }
}

#[test]
fn calendar_parts_strict_null_inactive_hidden_payload_empty_and_child_failures_are_exact() {
    let huge = "9".repeat(320 * 1024);
    let mut bytes = huge.as_bytes().to_vec();
    bytes.extend_from_slice(b"2024-02-29");
    let mut validity = BooleanBufferBuilder::new(2);
    validity.append(false);
    validity.append(true);
    let hidden: ArrayRef = Arc::new(StringArray::new(
        arrow_buffer::OffsetBuffer::new(
            vec![0i32, huge.len() as i32, (huge.len() + 10) as i32].into(),
        ),
        arrow_buffer::Buffer::from(bytes),
        Some(NullBuffer::new(validity.finish())),
    ));
    let mut kernel = instance("day", &text_type(true));
    let control = Control::default();
    assert_eq!(
        output(
            &kernel
                .evaluate(
                    Selection::all(2),
                    &[EvaluatedArgument::Column(&hidden)],
                    &control
                )
                .unwrap()
        ),
        vec![None, Some(29)]
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
    let inactive = strings(vec![Some(&huge), Some("2024-02-29")]);
    let rows = [1];
    let selection = Selection::try_sparse(2, &rows).unwrap();
    let control = Control::default();
    assert_eq!(
        output(
            &kernel
                .evaluate(selection, &[EvaluatedArgument::Column(&inactive)], &control)
                .unwrap()
        ),
        vec![Some(29)]
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
    let rows = [];
    let selection = Selection::try_sparse(2, &rows).unwrap();
    assert!(
        output(
            &kernel
                .evaluate(
                    selection,
                    &[EvaluatedArgument::Column(&hidden)],
                    &Control::default()
                )
                .unwrap()
        )
        .is_empty()
    );
    let failed = SelectedValues::try_new(
        Selection::all(2),
        &DataType::Utf8,
        strings(vec![None, Some("2024-02-29")]),
        Box::from([crate::RowDataError::new(0, "required date child failed")]),
    )
    .unwrap();
    assert!(matches!(
        kernel.evaluate(
            Selection::all(2),
            &[EvaluatedArgument::SelectedColumn(&failed)],
            &Control::default()
        ),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
    let after = Control::default();
    assert_eq!(
        kernel
            .evaluate(
                Selection::all(2),
                &[EvaluatedArgument::Column(&hidden)],
                &after
            )
            .unwrap_err(),
        KernelFailure::InstanceFailed
    );
    assert!(after.trace.lock().unwrap().is_empty());
    let mut nonnull = instance("day", &text_type(false));
    assert!(matches!(
        nonnull.evaluate(
            Selection::all(2),
            &[EvaluatedArgument::Column(&hidden)],
            &Control::default()
        ),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
}

#[test]
fn calendar_parts_exact_profiles_refuse_raw_units_zones_nominal_carriers_and_bad_arity() {
    for name in NAMES {
        for source in [
            FunctionValueType::new(DataType::Int64, true),
            FunctionValueType::new(DataType::Timestamp(TimeUnit::Second, None), true),
            FunctionValueType::new(
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            FunctionValueType::new(DataType::FixedSizeBinary(16), true),
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::Uuid,
            )
            .unwrap(),
            FunctionValueType::new(DataType::LargeUtf8, true),
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
        ] {
            assert!(
                prepared_for_test_with_policy(name, &[source], DecimalOverflowPolicy::OutputNull)
                    .is_err(),
                "{name}"
            );
        }
        for sources in [vec![], vec![text_type(true), text_type(true)]] {
            assert!(
                prepared_for_test_with_policy(name, &sources, DecimalOverflowPolicy::OutputNull)
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
        output_capacity((isize::MAX as usize / 4) + 1),
        Err(KernelFailure::ResourceExhausted)
    );
}

#[test]
fn calendar_parts_every_actual_compile_callback_preserves_three_causes_success_and_ordinary_tail() {
    for source in [
        text_type(true),
        FunctionValueType::new(DataType::Int64, true),
    ] {
        let good = CompileControl::default();
        let success = source.data_type() == &DataType::Utf8;
        assert_eq!(
            prepared_for_test_with_control(
                "year",
                std::slice::from_ref(&source),
                DecimalOverflowPolicy::OutputNull,
                &good
            )
            .is_ok(),
            success
        );
        let trace = good.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
                CompileControlError::from_domain(novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel),
            ] {
                let control = CompileControl {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let error = prepared_for_test_with_control(
                    "year",
                    std::slice::from_ref(&source),
                    DecimalOverflowPolicy::OutputNull,
                    &control,
                )
                .err()
                .unwrap();
                let actual = match error {
                    FunctionSpecializationFailure::Control(actual) => Some(actual),
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
fn calendar_parts_every_runtime_callback_preserves_seven_causes_real_quantum_and_failed_latch() {
    let long = format!("2024-{}02-29", "!".repeat(320));
    let cases = [
        (strings(vec![Some("2024-02-29")]), text_type(true), true),
        (strings(vec![Some(&long)]), text_type(true), true),
        (
            Arc::new(Date32Array::from(vec![Some(0); 3])) as ArrayRef,
            profiles(true)[2].clone(),
            true,
        ),
        (large(&[Some(20240229123456)]), large_type(true), true),
        (
            Arc::new(TimestampMicrosecondArray::from(vec![Some(-1)])) as ArrayRef,
            profiles(true)[0].clone(),
            true,
        ),
        (
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            text_type(true),
            false,
        ),
    ];
    for (array, source, success) in cases {
        let args = [EvaluatedArgument::Column(&array)];
        let selection = Selection::all(array.len());
        let good = Control::default();
        let prepared = prepared_for_test_with_policy(
            "year",
            std::slice::from_ref(&source),
            DecimalOverflowPolicy::OutputNull,
        )
        .unwrap();
        let mut kernel = ScalarEvaluationInstance::instantiate(prepared.clone()).unwrap();
        assert_eq!(kernel.evaluate(selection, &args, &good).is_ok(), success);
        let trace = good.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        if array
            .as_any()
            .downcast_ref::<StringArray>()
            .is_some_and(|s| s.value(0).len() > 256)
        {
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
                    kernel.evaluate(selection, &args, &control).unwrap_err(),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                let after = Control::default();
                assert_eq!(
                    kernel.evaluate(selection, &args, &after).unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}
