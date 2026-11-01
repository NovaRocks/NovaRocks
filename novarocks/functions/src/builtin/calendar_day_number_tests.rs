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

use super::super::calendar_day_number_owner::{
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
        panic!("calendar day number never waits")
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

const NAMES: [&str; 1] = ["to_days"];
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
fn instance(source: &FunctionValueType) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_policy(
            "to_days",
            std::slice::from_ref(source),
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
fn arrays() -> [ArrayRef; 3] {
    [
        Arc::new(TimestampMicrosecondArray::from(vec![
            Some(0),
            Some(-1),
            Some(1709210096000000),
            Some(1709164800000000),
            Some(1451606400000000),
            Some(i64::MAX),
            None,
        ])),
        Arc::new(Date32Array::from(vec![
            Some(0),
            Some(-1),
            Some(19782),
            Some(19782),
            Some(16801),
            Some(i32::MAX),
            None,
        ])),
        strings(vec![
            Some("1970-01-01"),
            Some("1969-12-31 23:59:59.999999"),
            Some("2024-02-29 12:34:56"),
            Some("20240229"),
            Some("2016-01-01T00:00:00"),
            Some("20230229"),
            None,
        ]),
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
fn calendar_day_number_three_actual_records_preserve_fresh_frozen_selected_identity_and_effects() {
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
    assert_eq!(operation("to_days"), Some(CalendarDayNumberOp::ToDays));
    for ghost in ["from_days", "time_to_sec", "day_number", "to_day"] {
        assert!(operation(ghost).is_none());
    }
}

#[test]
fn calendar_day_number_three_profiles_preserve_hand_oracles_nullable_and_policy() {
    for (source, array) in profiles(true).iter().zip(arrays().iter()) {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let prepared =
                prepared_for_test_with_policy("to_days", std::slice::from_ref(source), policy)
                    .unwrap();
            assert_eq!(prepared.contract().result_type(), &result_type());
            assert_eq!(prepared.contract().decimal_overflow_policy(), policy);
            let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
            assert_eq!(
                output(
                    &kernel
                        .evaluate(
                            Selection::all(7),
                            &[EvaluatedArgument::Column(array)],
                            &Control::default()
                        )
                        .unwrap()
                ),
                vec![
                    Some(719528),
                    Some(719527),
                    Some(739310),
                    Some(739310),
                    Some(736329),
                    None,
                    None
                ]
            );
            let mut nonnull = source.clone();
            nonnull = nonnull
                .with_nullable(false)
                .expect("valid function nullability declaration");
            let prepared = prepared_for_test_with_policy("to_days", &[nonnull], policy).unwrap();
            assert_eq!(prepared.contract().result_type(), &result_type());
            let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
            let no_null = array.slice(0, 6);
            assert_eq!(
                output(
                    &kernel
                        .evaluate(
                            Selection::all(6),
                            &[EvaluatedArgument::Column(&no_null)],
                            &Control::default()
                        )
                        .unwrap()
                ),
                vec![
                    Some(719528),
                    Some(719527),
                    Some(739310),
                    Some(739310),
                    Some(736329),
                    None
                ]
            );
        }
    }
}

#[test]
fn calendar_day_number_date32_full_domain_retains_negative_year_truncation() {
    // Exact CE epoch days were calculated independently of the Julian helper.
    let values: ArrayRef = Arc::new(Date32Array::from(vec![
        -96465292, 95026236, -96465293, 95026237, -719528, -719560, -2472692, -2473057,
    ]));
    let mut kernel = instance(&profiles(false)[1]);
    assert_eq!(
        output(
            &kernel
                .evaluate(
                    Selection::all(8),
                    &[EvaluatedArgument::Column(&values)],
                    &Control::default()
                )
                .unwrap()
        ),
        vec![
            Some(-95745764),
            Some(95745764),
            None,
            None,
            Some(0),
            Some(-32),
            Some(-1753163),
            Some(-1753528)
        ]
    );
}

#[test]
fn calendar_day_number_selected_cv_scalar_compact_slice_and_batch_addresses_are_exact() {
    for (source, array) in profiles(true).iter().zip(arrays().iter()) {
        let backing = pool(array, source.clone());
        let value = backing.value(2).unwrap();
        assert!(Arc::ptr_eq(value.pool().array(), backing.array()));
        let rows = [1, 4, 6];
        let selection = Selection::try_sparse(7, &rows).unwrap();
        let scalar = array.slice(2, 1);
        let compact = SelectedValues::try_new(
            selection,
            source.data_type(),
            array.slice(0, 3),
            Box::default(),
        )
        .unwrap();
        for (arg, expected) in [
            (
                EvaluatedArgument::Column(array),
                vec![Some(719527), Some(736329), None],
            ),
            (EvaluatedArgument::Scalar(&scalar), vec![Some(739310); 3]),
            (EvaluatedArgument::Constant(&value), vec![Some(739310); 3]),
            (
                EvaluatedArgument::SelectedColumn(&compact),
                vec![Some(719528), Some(719527), Some(739310)],
            ),
        ] {
            let mut kernel = instance(source);
            let arguments = [arg];
            let result = kernel
                .evaluate(selection, &arguments, &Control::default())
                .unwrap();
            assert_eq!(result.selection(), selection);
            assert_eq!(output(&result), expected);
        }
        let mut kernel = instance(source);
        let sliced = array.slice(1, 4);
        assert_eq!(
            output(
                &kernel
                    .evaluate(
                        Selection::all(4),
                        &[EvaluatedArgument::Column(&sliced)],
                        &Control::default()
                    )
                    .unwrap()
            ),
            vec![Some(719527), Some(739310), Some(739310), Some(736329)]
        );
        let mut split = Vec::new();
        for (at, count) in [(0, 2), (2, 3), (5, 2)] {
            let sliced = array.slice(at, count);
            split.extend(output(
                &kernel
                    .evaluate(
                        Selection::all(count),
                        &[EvaluatedArgument::Column(&sliced)],
                        &Control::default(),
                    )
                    .unwrap(),
            ));
        }
        assert_eq!(
            split,
            vec![
                Some(719528),
                Some(719527),
                Some(739310),
                Some(739310),
                Some(736329),
                None,
                None
            ]
        );
    }
}

#[test]
fn calendar_day_number_null_inactive_empty_and_required_errors_never_bypass_protocol() {
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
    let mut kernel = instance(&text_type(true));
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
        vec![None, Some(739310)]
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
        vec![Some(739310)]
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
    let empty = Selection::try_sparse(2, &[]).unwrap();
    assert!(
        output(
            &kernel
                .evaluate(
                    empty,
                    &[EvaluatedArgument::Column(&inactive)],
                    &Control::default()
                )
                .unwrap()
        )
        .is_empty()
    );
    let null = strings(vec![None]);
    let mut kernel = instance(&text_type(false));
    assert!(matches!(
        kernel.evaluate(
            Selection::all(1),
            &[EvaluatedArgument::Scalar(&null)],
            &Control::default()
        ),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
    let empty = strings(vec![]);
    let mut kernel = instance(&text_type(true));
    assert!(matches!(
        kernel.evaluate(
            Selection::all(1),
            &[EvaluatedArgument::Column(&empty)],
            &Control::default()
        ),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
    let failed = SelectedValues::try_new(
        Selection::all(2),
        &DataType::Utf8,
        strings(vec![None, None]),
        Box::from([crate::RowDataError::new(0, "required date child failed")]),
    )
    .unwrap();
    let mut kernel = instance(&text_type(true));
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
                &[EvaluatedArgument::Column(&inactive)],
                &after
            )
            .unwrap_err(),
        KernelFailure::InstanceFailed
    );
    assert!(after.trace.lock().unwrap().is_empty());
}

#[test]
fn calendar_day_number_exact_source_arity_and_output_layout_refuse_ghost_profiles() {
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
            prepared_for_test_with_policy("to_days", &[source], DecimalOverflowPolicy::OutputNull)
                .is_err()
        );
    }
    for args in [vec![], vec![text_type(true); 2]] {
        assert!(
            prepared_for_test_with_policy("to_days", &args, DecimalOverflowPolicy::OutputNull)
                .is_err()
        );
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
fn calendar_day_number_every_compile_callback_preserves_three_primary_causes_and_ordinary_tail() {
    for source in [
        text_type(true),
        FunctionValueType::new(DataType::Int64, true),
    ] {
        let good = CompileControl::default();
        assert_eq!(
            prepared_for_test_with_control(
                "to_days",
                std::slice::from_ref(&source),
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
                    "to_days",
                    std::slice::from_ref(&source),
                    DecimalOverflowPolicy::OutputNull,
                    &control,
                )
                .err()
                .unwrap();
                let actual = match error {
                    FunctionSpecializationFailure::Control(cause) => Some(cause),
                    FunctionSpecializationFailure::Kernel(KernelFailure::CompileControl(cause)) => {
                        Some(cause)
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
fn calendar_day_number_every_runtime_callback_keeps_seven_primary_causes_quantum_and_failed_latch()
{
    let long = format!("2024-{}02-29", "!".repeat(320));
    let cases = [
        (strings(vec![Some("2024-02-29")]), text_type(true), true),
        (strings(vec![Some(&long)]), text_type(true), true),
        (
            Arc::new(Date32Array::from(vec![0])) as ArrayRef,
            profiles(true)[1].clone(),
            true,
        ),
        (
            Arc::new(TimestampMicrosecondArray::from(vec![-1])) as ArrayRef,
            profiles(true)[0].clone(),
            true,
        ),
        (
            Arc::new(Date32Array::from(vec![0; 320])) as ArrayRef,
            profiles(true)[1].clone(),
            true,
        ),
        (
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            text_type(true),
            false,
        ),
    ];
    for (array, source, success) in cases {
        let args = [EvaluatedArgument::Column(&array)];
        let prepared =
            prepared_for_test_with_policy("to_days", &[source], DecimalOverflowPolicy::OutputNull)
                .unwrap();
        let good = Control::default();
        let mut kernel = ScalarEvaluationInstance::instantiate(prepared.clone()).unwrap();
        assert_eq!(
            kernel
                .evaluate(Selection::all(array.len()), &args, &good)
                .is_ok(),
            success
        );
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
                    kernel
                        .evaluate(Selection::all(array.len()), &args, &control)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                let after = Control::default();
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(array.len()), &args, &after)
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}

fn with_installed_day_number<T>(
    source: FunctionValueType,
    action: impl FnOnce(
        &super::super::calendar_day_number_owner::CalendarDayNumberOwner,
        crate::CallEffectInput<'_>,
        Arc<crate::ScalarCallContract>,
    ) -> T,
) -> T {
    let catalog = super::super::catalogue::builtin_engine_function_catalog();
    let owner = owner_for_test("to_days");
    let arguments = [FunctionArgument::Value {
        value_type: source,
        constant: None,
    }];
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: 1,
        expected_result_type: None,
    };
    let bound = catalog
        .resolve_bound_user(
            "to_days",
            crate::FunctionKind::Scalar,
            request,
            crate::binding_test_control(),
        )
        .unwrap();
    let selected = Arc::new(bound.selected);
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(841),
        domain: EvaluationDomainId::new(847),
        demand: EvaluationDemand::Value,
    };
    let uses = [Some(ExpressionUseId::new(842))];
    let parameters = SemanticParameters::default();
    let input = crate::CallEffectInput {
        context,
        argument_uses: crate::CallArgumentUses::SelectedChannels(&uses),
        function_id: &bound.function_id,
        kind: crate::FunctionKind::Scalar,
        selected: &selected,
        request,
        environment: &[],
        parameters: &parameters,
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Domain(context.domain),
    };
    let fresh = catalog
        .prepare_fresh_selected(
            input,
            selected.clone(),
            crate::PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(context),
            },
            crate::binding_test_control(),
        )
        .unwrap();
    let frozen = catalog
        .prepare_frozen_selected(
            input,
            selected.clone(),
            fresh.call_contract().effects(),
            crate::PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(context),
            },
            crate::binding_test_control(),
        )
        .unwrap();
    assert_eq!(fresh.implementation(), frozen.implementation());
    let crate::PreparedPureKernel::Scalar(prepared) = fresh.into_prepared() else {
        panic!("the installed day-number author must produce its real ScalarV1 kernel");
    };
    assert!(std::ptr::eq(
        prepared.contract().selected(),
        selected.as_ref()
    ));
    action(&owner, input, prepared.contract().clone())
}

#[test]
fn calendar_day_number_captured_owner_errors_return_without_a_later_compile_callback() {
    use crate::{FunctionBindingError, FunctionEffectOwner, FunctionEffectOwnerError};
    for source in profiles(true) {
        with_installed_day_number(source, |owner, input, contract| {
            let foreign = crate::FunctionId::try_new("foreign.calendar/day-number/v1").unwrap();
            for mismatch in 0..3 {
                let mut wrong = input;
                match mismatch {
                    0 => wrong.function_id = &foreign,
                    1 => wrong.kind = crate::FunctionKind::Aggregate,
                    2 => wrong.proof_scope = CallProofScope::Domain(EvaluationDomainId::new(999)),
                    _ => unreachable!(),
                }
                let control = CompileControl::default();
                let error = owner.validate_and_refine(wrong, &control).unwrap_err();
                let expected = if mismatch < 2 {
                    FunctionBindingError::UnknownFunction
                } else {
                    FunctionBindingError::InvalidBinding("calendar day number has no environment dependencies and requires an exact proof scope".into())
                };
                match error {
                    FunctionEffectOwnerError::Owner(actual) => assert_eq!(actual, expected),
                    other => panic!("the actual owner error must remain the result: {other:?}"),
                }
                assert_eq!(
                    *control.trace.lock().unwrap(),
                    [(CompilePhase::FunctionSpecialization, 0)],
                    "an authored outer failure does not start the pending-work footer"
                );

                let control = CompileControl::default();
                let error = owner
                    .prepare_scalar(wrong, contract.clone(), &control)
                    .unwrap_err();
                assert_eq!(
                    error,
                    KernelFailure::InvalidProgram(crate::KernelDiagnostic::new(
                        "calendar day number preparation differs from its exact checked call"
                    ))
                );
                assert_eq!(
                    *control.trace.lock().unwrap(),
                    [(CompilePhase::FunctionSpecialization, 0)]
                );
            }
        });
    }
}

#[test]
fn calendar_day_number_nested_binding_error_has_no_enclosing_owner_footer() {
    use crate::{FunctionEffectOwner, FunctionEffectOwnerError};
    for source in profiles(true) {
        with_installed_day_number(source, |owner, input, _| {
            let mut forged = (*input.selected).clone();
            forged.result_type =
                FunctionResultType::Scalar(FunctionValueType::new(DataType::Int64, false));
            let binding_control = CompileControl::default();
            let expected = owner
                .validate_selected(&forged, input.request, &binding_control)
                .unwrap_err();
            let binding_trace = binding_control.trace.lock().unwrap().clone();
            assert!(!binding_trace.is_empty());
            let mut wrong = input;
            wrong.selected = &forged;
            let control = CompileControl::default();
            let error = owner.validate_and_refine(wrong, &control).unwrap_err();
            match error {
                FunctionEffectOwnerError::Owner(actual) => assert_eq!(actual, expected),
                other => panic!("nested binding author error must remain intact: {other:?}"),
            }
            let mut expected_trace = vec![
                (CompilePhase::FunctionSpecialization, 0),
                (CompilePhase::FunctionSpecialization, 1),
            ];
            expected_trace.extend_from_slice(&binding_trace);
            assert_eq!(
                *control.trace.lock().unwrap(),
                expected_trace,
                "the enclosing owner must not append its zero-tail callback after a nested author failed"
            );
        });
    }
}

#[test]
fn calendar_day_number_real_success_flushes_and_each_actual_callback_preserves_original_refusal() {
    use crate::{FunctionEffectOwner, FunctionEffectOwnerError};
    for source in profiles(true) {
        with_installed_day_number(source, |owner, input, contract| {
            let binding_control = CompileControl::default();
            owner
                .validate_selected(input.selected, input.request, &binding_control)
                .unwrap();
            let mut expected_trace = vec![
                (CompilePhase::FunctionSpecialization, 0),
                (CompilePhase::FunctionSpecialization, 1),
            ];
            expected_trace.extend_from_slice(&binding_control.trace.lock().unwrap());
            expected_trace.push((CompilePhase::FunctionSpecialization, 0));
            let refined = CompileControl::default();
            assert_eq!(
                &owner.validate_and_refine(input, &refined).unwrap(),
                contract.effects()
            );
            assert_eq!(
                *refined.trace.lock().unwrap(),
                expected_trace,
                "success must still flush its real final scope"
            );
            let prepared = CompileControl::default();
            let kernel = owner
                .prepare_scalar(input, contract.clone(), &prepared)
                .unwrap();
            assert!(Arc::ptr_eq(kernel.contract(), &contract));
            assert_eq!(*prepared.trace.lock().unwrap(), expected_trace);
            for at in 0..expected_trace.len() {
                for cause in [
                    CompileControlError::Cancelled,
                    CompileControlError::DeadlineExceeded,
                    CompileControlError::ResourceExhausted,
                    CompileControlError::from_domain(novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel),
                ] {
                    let control = CompileControl { trace: Mutex::new(vec![]), refusal: Some((at, cause.clone())) };
                    match owner.validate_and_refine(input, &control).unwrap_err() {
                        FunctionEffectOwnerError::Control(actual) => assert_eq!(actual, cause),
                        other => panic!("the same-scope compile refusal must stay original: {other:?}"),
                    }
                    assert_eq!(*control.trace.lock().unwrap(), expected_trace[..=at]);
                    let control = CompileControl { trace: Mutex::new(vec![]), refusal: Some((at, cause.clone())) };
                    assert_eq!(owner.prepare_scalar(input, contract.clone(), &control).unwrap_err(), KernelFailure::CompileControl(cause));
                    assert_eq!(*control.trace.lock().unwrap(), expected_trace[..=at]);
                }
            }
        });
    }
}
