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

use super::super::date_owner::{
    effects, operation, owner_for_test, prepared_for_test_with_control,
    prepared_for_test_with_policy,
};
use super::*;
use crate::{
    ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionArgument, FunctionBindingRequest,
    FunctionBindingResolver, FunctionResultType, FunctionSpecializationFailure, FunctionValueType,
    PureFunctionMetadataOwner, PureScalarImplementation, ScalarEvaluationInstance,
    ScopedExpressionEffects, Selection, specialize_frozen_scalar, specialize_scalar,
};
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
        panic!("date extraction never waits")
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
const NAMES: [&str; 1] = ["date"];
fn source(data_type: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(data_type, nullable)
}
fn target() -> FunctionValueType {
    source(DataType::Date32, true)
}
fn instance(ty: &FunctionValueType) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_policy("date", ty, DecimalOverflowPolicy::OutputNull).unwrap(),
    )
    .unwrap()
}
fn strings(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
fn output(result: &SelectedValues<'_>) -> Vec<Option<i32>> {
    assert!(result.errors().is_empty());
    result
        .values()
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap()
        .iter()
        .collect()
}
fn pool(array: ArrayRef) -> ConstantPool {
    let ty = source(array.data_type().clone(), true);
    ConstantPool::try_new(
        Arc::new(
            ty.try_to_field("original")
                .unwrap()
                .with_metadata(std::collections::HashMap::<String, String>::from([(
                    "source-note".into(),
                    "kept selected backing".into(),
                )])),
        ),
        ty,
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
fn date_actual_three_profiles_keep_calendar_ranges_negative_epochs_and_text_grammar() {
    let fixtures: Vec<(ArrayRef, Vec<Option<i32>>)> = vec![
        (
            Arc::new(Date32Array::from(vec![0, -1, 1, 19782, i32::MIN, i32::MAX])),
            vec![Some(0), Some(-1), Some(1), Some(19782), None, None],
        ),
        (
            Arc::new(TimestampMicrosecondArray::from(vec![
                -1,
                0,
                1,
                86_400_000_000,
                -86_400_000_001,
                i64::MIN,
                i64::MAX,
            ])),
            vec![Some(-1), Some(0), Some(0), Some(1), Some(-2), None, None],
        ),
        (
            strings(vec![
                Some("1970-01-01"),
                Some("19691231"),
                Some("2024-02-29T12:34:56.123456789"),
                Some(" 2024/02/29 12:34:56 "),
                Some("20000229"),
                Some("691231235959"),
                Some("700101000000"),
                Some("2023-02-29"),
                Some("2016-12-31 23:59:60"),
                Some("garbage"),
                Some(""),
            ]),
            vec![
                Some(0),
                Some(-1),
                Some(19782),
                Some(19782),
                Some(11016),
                Some(36524),
                Some(0),
                None,
                None,
                None,
                None,
            ],
        ),
    ];
    for (array, expected) in fixtures {
        for nullable in [false, true] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                let prepared = prepared_for_test_with_policy(
                    "date",
                    &source(array.data_type().clone(), nullable),
                    policy,
                )
                .unwrap();
                assert_eq!(prepared.contract().result_type(), &target());
                assert_eq!(prepared.contract().decimal_overflow_policy(), policy);
                assert_eq!(prepared.instance_retained_upper_bound(), 0);
                let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
                let arguments = [EvaluatedArgument::Column(&array)];
                for _ in 0..2 {
                    let result = kernel
                        .evaluate(Selection::all(array.len()), &arguments, &Control::default())
                        .unwrap();
                    assert_eq!(output(&result), expected);
                }
            }
        }
    }
    // Calendar MIN/MAX are the original Chrono domain, rather than a new SQL year cap.
    let min = NaiveDate::MIN.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET;
    let max = NaiveDate::MAX.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET;
    let array = Arc::new(Date32Array::from(vec![min - 1, min, max, max + 1])) as ArrayRef;
    let arguments = [EvaluatedArgument::Column(&array)];
    assert_eq!(
        output(
            &instance(&source(DataType::Date32, false))
                .evaluate(Selection::all(4), &arguments, &Control::default())
                .unwrap()
        ),
        vec![None, Some(min), Some(max), None]
    );
}

#[test]
fn date_sparse_sliced_compact_scalar_and_pool_ordinal_are_independent_addresses() {
    let backing = strings(vec![
        Some("unused"),
        Some("1969-12-31"),
        None,
        Some("2024-02-29"),
        Some("unused"),
    ]);
    let sliced = backing.slice(1, 3);
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Utf8,
        strings(vec![Some("1970-01-01"), Some("2000-02-29")]),
        Box::default(),
    )
    .unwrap();
    for (arg, expected) in [
        (
            EvaluatedArgument::Column(&sliced),
            vec![Some(-1), Some(19782)],
        ),
        (
            EvaluatedArgument::SelectedColumn(&compact),
            vec![Some(0), Some(11016)],
        ),
    ] {
        let arguments = [arg];
        let mut kernel = instance(&source(DataType::Utf8, true));
        let result = kernel
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(result.selection(), selection);
        assert_eq!(output(&result), expected);
    }
    let scalar = strings(vec![Some("2024-02-29")]);
    let arguments = [EvaluatedArgument::Scalar(&scalar)];
    assert_eq!(
        output(
            &instance(&source(DataType::Utf8, true))
                .evaluate(selection, &arguments, &Control::default())
                .unwrap()
        ),
        vec![Some(19782); 2]
    );
    for array in [
        strings(vec![Some("garbage"), None, Some("1969-12-31")]),
        Arc::new(Date32Array::from(vec![Some(i32::MAX), None, Some(-1)])) as ArrayRef,
        Arc::new(TimestampMicrosecondArray::from(vec![
            Some(i64::MAX),
            None,
            Some(-1),
        ])) as ArrayRef,
    ] {
        let constants = pool(array);
        let constant = constants.value(2).unwrap();
        let arguments = [EvaluatedArgument::Constant(&constant)];
        assert_eq!(
            output(
                &instance(constant.value_type())
                    .evaluate(selection, &arguments, &Control::default())
                    .unwrap()
            ),
            vec![Some(-1); 2]
        );
        assert_eq!(constant.ordinal(), 2);
        assert!(Arc::ptr_eq(constant.pool().array(), constants.array()));
        assert_eq!(
            constant.pool().field().metadata()["source-note"],
            "kept selected backing"
        );
    }
}

#[test]
fn date_hidden_null_inactive_text_and_empty_selection_do_not_parse_payload() {
    use arrow_buffer::{Buffer, OffsetBuffer};
    let huge = " ".repeat(320 * 1024);
    let mut bytes = huge.as_bytes().to_vec();
    bytes.extend_from_slice(b"1970-01-01");
    let mut validity = BooleanBufferBuilder::new(2);
    validity.append(false);
    validity.append(true);
    let hidden = Arc::new(StringArray::new(
        OffsetBuffer::new(vec![0i32, huge.len() as i32, (huge.len() + 10) as i32].into()),
        Buffer::from(bytes),
        Some(NullBuffer::new(validity.finish())),
    )) as ArrayRef;
    let arguments = [EvaluatedArgument::Column(&hidden)];
    let control = Control::default();
    let mut kernel = instance(&source(DataType::Utf8, true));
    assert_eq!(
        output(
            &kernel
                .evaluate(Selection::all(2), &arguments, &control)
                .unwrap()
        ),
        vec![None, Some(0)]
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
    let inactive = strings(vec![Some(&huge), Some("1970-01-01")]);
    let arguments = [EvaluatedArgument::Column(&inactive)];
    let rows = [1];
    assert_eq!(
        output(
            &kernel
                .evaluate(
                    Selection::try_sparse(2, &rows).unwrap(),
                    &arguments,
                    &Control::default()
                )
                .unwrap()
        ),
        vec![Some(0)]
    );
    let rows = [];
    assert!(
        output(
            &kernel
                .evaluate(
                    Selection::try_sparse(2, &rows).unwrap(),
                    &arguments,
                    &Control::default()
                )
                .unwrap()
        )
        .is_empty()
    );
    for array in [
        strings(vec![None, None]),
        Arc::new(Date32Array::from(vec![None, None])) as ArrayRef,
        Arc::new(TimestampMicrosecondArray::from(vec![None, None])) as ArrayRef,
    ] {
        let arguments = [EvaluatedArgument::Column(&array)];
        assert_eq!(
            output(
                &instance(&source(array.data_type().clone(), true))
                    .evaluate(Selection::all(2), &arguments, &Control::default())
                    .unwrap()
            ),
            vec![None, None]
        );
    }
    let mut nonnull = instance(&source(DataType::Utf8, false));
    assert!(matches!(
        nonnull.evaluate(
            Selection::all(2),
            &[EvaluatedArgument::Column(&hidden)],
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let after = Control::default();
    assert_eq!(
        nonnull
            .evaluate(Selection::all(2), &arguments, &after)
            .unwrap_err(),
        KernelFailure::InstanceFailed
    );
    assert!(after.trace.lock().unwrap().is_empty());
}

#[test]
fn date_child_error_is_refused_before_the_ordinary_body_and_capacity_is_checked() {
    let selection = Selection::all(1);
    let child = SelectedValues::try_new(
        selection,
        &DataType::Utf8,
        strings(vec![None]),
        Box::from([crate::RowDataError::new(0, "required child failed")]),
    )
    .unwrap();
    let arguments = [EvaluatedArgument::SelectedColumn(&child)];
    let mut kernel = instance(&source(DataType::Utf8, true));
    assert!(matches!(
        kernel.evaluate(selection, &arguments, &Control::default()),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
    let after = Control::default();
    assert_eq!(
        kernel.evaluate(selection, &arguments, &after).unwrap_err(),
        KernelFailure::InstanceFailed
    );
    assert!(after.trace.lock().unwrap().is_empty());
    assert_eq!(
        output_capacity(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    );
    assert!(output_capacity(0).is_ok());
    assert!(output_capacity(320).is_ok());
}

#[test]
fn date_runtime_success_ordinary_and_real_parser_row_quantum_preserve_all_seven_first_causes() {
    use crate::kernel_control::KernelDiagnostic;
    let separators = format!("1970{}-01-01", " ".repeat(320));
    for (array, ty, all) in [
        (
            strings(vec![Some("1970-01-01"), Some("bad"), None]),
            source(DataType::Utf8, true),
            true,
        ),
        (
            Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef,
            source(DataType::Utf8, true),
            true,
        ),
        (
            Arc::new(Date32Array::from(vec![0; 320])) as ArrayRef,
            source(DataType::Date32, false),
            false,
        ),
        (
            strings(vec![Some(&separators)]),
            source(DataType::Utf8, false),
            false,
        ),
    ] {
        let good = Control::default();
        let arguments = [EvaluatedArgument::Column(&array)];
        let mut kernel = instance(&ty);
        let actual = kernel.evaluate(Selection::all(array.len()), &arguments, &good);
        if array.data_type() == &DataType::Int64 {
            assert!(matches!(actual, Err(KernelFailure::InvalidProgram(_))));
        } else {
            let actual = actual.unwrap();
            if array.len() == 1 {
                assert_eq!(output(&actual), vec![Some(0)]);
            }
        }
        let trace = good.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        if !all {
            assert!(trace.contains(&256));
        }
        for (at, units) in trace.iter().enumerate() {
            if !all && at != 0 && at + 1 != trace.len() && *units != 256 {
                continue;
            }
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
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let mut kernel = instance(&ty);
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(array.len()), &arguments, &control)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                let after = Control::default();
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(array.len()), &arguments, &after)
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}
#[test]
fn date_owner_preserves_exact_fresh_frozen_arc_effects_and_rejects_stale_sources() {
    assert!(operation("date").is_some());
    assert!(operation("to_date").is_none());
    for name in NAMES {
        let owner = owner_for_test(name);
        let arguments = [FunctionArgument::Value {
            value_type: source(DataType::Utf8, false),
            constant: None,
        }];
        let request = FunctionBindingRequest {
            expected_result_type: None,
            arguments: &arguments,
            logical_argument_count: 1,
        };
        let selected = Arc::new(
            owner
                .resolve(request, crate::binding_test_control())
                .unwrap(),
        );
        assert_eq!(selected.result_type, FunctionResultType::Scalar(target()));
        let context = ExpressionEffectContext {
            use_id: ExpressionUseId::new(41),
            domain: EvaluationDomainId::new(7),
            demand: EvaluationDemand::Value,
        };
        let uses = [Some(ExpressionUseId::new(42))];
        let params = SemanticParameters::try_new([]).unwrap();
        let input = crate::CallEffectInput {
            context,
            argument_uses: crate::CallArgumentUses::SelectedChannels(&uses),
            function_id: owner.binding_declaration().function_id(),
            kind: crate::FunctionKind::Scalar,
            selected: &selected,
            request,
            environment: &[],
            parameters: &params,
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
        assert_eq!(frozen.prepared().contract().effects(), canonical.effects());
        assert_eq!(
            canonical.effects().value_stability,
            FunctionVolatility::Immutable
        );
        assert_eq!(
            canonical.effects().instance_state,
            FunctionInstanceState::None
        );
        assert_eq!(
            canonical.effects().null_behavior,
            FunctionNullBehavior::Strict
        );
        assert!(canonical.effects().observable_effects.is_empty());
        assert_eq!(
            canonical.effects().own_row_error,
            crate::FunctionIntrinsicRowError::NoRowError
        );
        assert_eq!(
            canonical.effects().argument_control,
            novarocks_type_contract::ArgumentControl::Eager
        );
        assert_eq!(
            canonical.effects().failure_behavior,
            crate::FunctionFailureBehavior::Propagate
        );
        assert!(canonical.effects().environment.is_empty());
        assert_eq!(
            owner.binding_declaration().function_id().as_str(),
            "builtin.scalar/date/v1"
        );
        assert_eq!(owner.implementation_declarations().len(), 3);
        assert_eq!(
            owner.implementation_declarations()[0]
                .implementation
                .as_str(),
            "builtin.scalar/date/selected-v1"
        );
        assert_eq!(
            owner.binding_declaration().overloads()[0].effects.as_ref(),
            Some(&effects())
        );
        let pattern_arguments = [arguments[0].clone(), arguments[0].clone()];
        assert!(
            owner
                .resolve(
                    FunctionBindingRequest {
                        arguments: &pattern_arguments,
                        logical_argument_count: 2,
                        expected_result_type: None,
                    },
                    crate::binding_test_control()
                )
                .is_err()
        );
        assert!(std::ptr::eq(canonical.selected(), selected.as_ref()));
        let mut forged = (*selected).clone();
        forged.result_type = FunctionResultType::Scalar(source(DataType::Date32, false));
        assert!(
            owner
                .validate_selected(&forged, request, crate::binding_test_control())
                .is_err()
        );
        let foreign_id = crate::FunctionId::try_new("builtin.scalar/trim/v1").unwrap();
        let mut foreign = input;
        foreign.function_id = &foreign_id;
        assert!(
            owner
                .prepare_scalar(foreign, canonical.clone(), crate::binding_test_control())
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
        stale.context.use_id = ExpressionUseId::new(999);
        assert!(
            owner
                .prepare_scalar(stale, canonical, crate::binding_test_control())
                .is_err()
        );
        // These have no installed date source profile. Timestamp units/zones
        // and nominal text anchors may legitimately resolve through coercion;
        // rejection below concerns canonical body inputs, not those requests.
        for source in [
            FunctionValueType::new(DataType::Binary, true),
            FunctionValueType::from_legacy_declared_projection(
                DataType::List(Arc::new(arrow_schema::Field::new(
                    "item",
                    DataType::Int64,
                    true,
                ))),
                true,
            ),
        ] {
            assert!(
                prepared_for_test_with_policy(name, &source, DecimalOverflowPolicy::OutputNull)
                    .is_err()
            );
        }
        let raw = source(
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            true,
        );
        let raw_arguments = [FunctionArgument::Value {
            value_type: raw.clone(),
            constant: None,
        }];
        let resolved = owner
            .resolve(
                FunctionBindingRequest {
                    expected_result_type: None,
                    arguments: &raw_arguments,
                    logical_argument_count: 1,
                },
                crate::binding_test_control(),
            )
            .unwrap();
        let canonical_source = source(DataType::Timestamp(TimeUnit::Microsecond, None), true);
        assert_eq!(
            resolved.argument_types.as_ref(),
            &[crate::FunctionArgumentType::Value(canonical_source.clone())]
        );
        // Metadata resolution specifies coercion, whereas specialization requires
        // the actual request to have already undergone that coercion.
        assert!(matches!(
            prepared_for_test_with_policy(name, &raw, DecimalOverflowPolicy::OutputNull),
            Err(FunctionSpecializationFailure::InvalidInput(_))
        ));
        let prepared = prepared_for_test_with_policy(
            name,
            &canonical_source,
            DecimalOverflowPolicy::OutputNull,
        )
        .unwrap();
        let canonical_array = Arc::new(TimestampMicrosecondArray::from(vec![0])) as ArrayRef;
        let canonical_arguments = [EvaluatedArgument::Column(&canonical_array)];
        let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
        assert_eq!(
            output(
                &kernel
                    .evaluate(Selection::all(1), &canonical_arguments, &Control::default())
                    .unwrap()
            ),
            vec![Some(0)]
        );
        let array = Arc::new(arrow_array::TimestampSecondArray::from(vec![0]).with_timezone("UTC"))
            as ArrayRef;
        let arguments = [EvaluatedArgument::Column(&array)];
        assert!(matches!(
            kernel.evaluate(Selection::all(1), &arguments, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
}

#[test]
fn date_compile_success_and_ordinary_tail_preserve_all_callback_causes() {
    for ty in [
        source(DataType::Utf8, true),
        FunctionValueType::new(DataType::Binary, true),
    ] {
        let good = CompileControl::default();
        let success = ty.data_type() == &DataType::Utf8;
        assert_eq!(
            prepared_for_test_with_control("date", &ty, DecimalOverflowPolicy::OutputNull, &good)
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
            ] {
                let control = CompileControl {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let actual = prepared_for_test_with_control(
                    "date",
                    &ty,
                    DecimalOverflowPolicy::OutputNull,
                    &control,
                )
                .err()
                .and_then(|error| match error {
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
                });
                assert_eq!(actual, Some(cause));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn date_invalid_and_null_constants_prepare_without_reading_payload() {
    let constants = pool(strings(vec![Some("1970-01-01"), Some("bad"), None]));
    for ordinal in [1, 2] {
        let constant = constants.value(ordinal).unwrap();
        let owner = owner_for_test("date");
        let arguments = [FunctionArgument::Value {
            value_type: source(DataType::Utf8, true),
            constant: Some(constant.clone()),
        }];
        let request = FunctionBindingRequest {
            expected_result_type: None,
            arguments: &arguments,
            logical_argument_count: 1,
        };
        let selected = Arc::new(
            owner
                .resolve(request, crate::binding_test_control())
                .unwrap(),
        );
        let context = ExpressionEffectContext {
            use_id: ExpressionUseId::new(41),
            domain: EvaluationDomainId::new(7),
            demand: EvaluationDemand::Value,
        };
        let uses = [Some(ExpressionUseId::new(42))];
        let parameters = SemanticParameters::try_new([]).unwrap();
        let input = crate::CallEffectInput {
            context,
            argument_uses: crate::CallArgumentUses::SelectedChannels(&uses),
            function_id: owner.binding_declaration().function_id(),
            kind: crate::FunctionKind::Scalar,
            selected: &selected,
            request,
            environment: &[],
            parameters: &parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            proof_scope: CallProofScope::Unconditional,
        };
        let prepared = specialize_scalar(
            &owner,
            input,
            selected.clone(),
            ScopedExpressionEffects::pure_value(context),
            crate::binding_test_control(),
        )
        .unwrap()
        .into_prepared();
        assert!(std::ptr::eq(
            prepared.contract().selected(),
            selected.as_ref()
        ));
        let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
        let arguments = [EvaluatedArgument::Constant(&constant)];
        let empty_rows = [];
        let empty = Selection::try_sparse(4, &empty_rows).unwrap();
        let result = kernel
            .evaluate(empty, &arguments, &Control::default())
            .unwrap();
        assert!(output(&result).is_empty());
        let rows = [1, 3];
        let selected = Selection::try_sparse(4, &rows).unwrap();
        let result = kernel
            .evaluate(selected, &arguments, &Control::default())
            .unwrap();
        assert_eq!(result.values().null_count(), 2);
        assert!(result.errors().is_empty());
        assert_eq!(constant.ordinal(), ordinal);
        assert!(Arc::ptr_eq(constant.pool().array(), constants.array()));
    }
}
