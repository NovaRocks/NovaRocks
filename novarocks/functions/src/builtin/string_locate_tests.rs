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

use super::super::string_locate_owner::{
    effects, operation, owner_for_test, prepared_for_test_with_control,
    prepared_for_test_with_policy, strpos_effects,
};
use super::*;
use crate::{
    ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionArgument, FunctionBindingRequest,
    FunctionBindingResolver, FunctionResultType, FunctionSpecializationFailure, FunctionValueType,
    PureFunctionMetadataOwner, PureScalarImplementation, ScalarEvaluationInstance,
    ScopedExpressionEffects, Selection, specialize_frozen_scalar, specialize_scalar,
};
use arrow_buffer::{Buffer, OffsetBuffer};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, FunctionInstanceState,
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
        panic!("locate/instr never waits")
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
fn source(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Utf8, nullable)
}
fn instance(name: &str, arity: usize) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_policy(name, &sources(arity), DecimalOverflowPolicy::OutputNull)
            .unwrap(),
    )
    .unwrap()
}
fn strings(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
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
fn pool(array: ArrayRef) -> ConstantPool {
    let ty = source(true);
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

const NAMES: [&str; 1] = ["strpos"];
fn sources(arity: usize) -> Vec<FunctionValueType> {
    (0..arity)
        .map(|i| {
            FunctionValueType::from_legacy_declared_projection(
                if i < 2 {
                    DataType::Utf8
                } else {
                    DataType::Int64
                },
                true,
            )
        })
        .collect()
}
fn target() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, true)
}
fn integers(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}
fn ordinary_args<'a>(haystack: &'a ArrayRef, needle: &'a ArrayRef) -> [EvaluatedArgument<'a>; 2] {
    [
        EvaluatedArgument::Column(haystack),
        EvaluatedArgument::Column(needle),
    ]
}

#[test]
fn strpos_preserves_overlapping_unicode_and_signed_occurrences() {
    let cases = [
        ("aé中é", "é", 1, Some(2)),
        ("aé中é", "é", 2, Some(4)),
        ("aé中é", "é", -1, Some(4)),
        ("aé中é", "é", -2, Some(2)),
        ("👩‍💻x", "x", 1, Some(4)),
        ("a\u{301}b", "\u{301}", 1, Some(2)),
        ("x\0y", "\0", 1, Some(2)),
        ("aaaa", "aa", 2, Some(2)),
        ("aaaa", "aa", -2, Some(2)),
        ("abc", "z", 1, Some(0)),
        ("abc", "", 3, Some(1)),
        ("", "", -1, Some(1)),
        ("abc", "a", i64::MAX, Some(0)),
        ("abc", "a", 0, None),
        ("abc", "", i64::MIN, None),
    ];
    let haystack = strings(cases.iter().map(|case| Some(case.0)).collect());
    let needle = strings(cases.iter().map(|case| Some(case.1)).collect());
    let instances = integers(cases.iter().map(|case| Some(case.2)).collect());
    let arguments = [
        EvaluatedArgument::Column(&haystack),
        EvaluatedArgument::Column(&needle),
        EvaluatedArgument::Column(&instances),
    ];
    let result = instance("strpos", 3)
        .evaluate(Selection::all(cases.len()), &arguments, &Control::default())
        .unwrap();
    let values = result
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(values.iter().collect::<Vec<_>>(), cases.map(|case| case.3));
    let invalid = crate::DataFailureReason::Domain(
        novarocks_function_contract::kernel_failure::INVALID_FUNCTION_ARGUMENT,
    );
    assert_eq!(
        result
            .errors()
            .iter()
            .map(|error| (error.selected_ordinal(), error.reason()))
            .collect::<Vec<_>>(),
        [(13, invalid), (14, invalid)]
    );
    let cases = [
        ("aé中é", "é", 2),
        ("👩‍💻x", "x", 4),
        ("a\u{301}b", "\u{301}", 2),
        ("abc", "", 1),
        ("", "", 1),
        ("abc", "z", 0),
    ];
    let haystack = strings(cases.iter().map(|(h, _, _)| Some(*h)).collect());
    let needle = strings(cases.iter().map(|(_, n, _)| Some(*n)).collect());
    for name in NAMES {
        let args = ordinary_args(&haystack, &needle);
        let mut kernel = instance(name, 2);
        assert_eq!(
            output(
                &kernel
                    .evaluate(Selection::all(cases.len()), &args, &Control::default())
                    .unwrap()
            ),
            cases
                .iter()
                .map(|(_, _, expected)| Some(*expected))
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn string_locate_sparse_slice_compact_scalar_and_nonzero_constant_ordinals_are_independent() {
    let haystack = strings(vec![
        Some("prefix"),
        Some("aé中é"),
        Some("inactive"),
        Some("aé中é"),
        Some("suffix"),
    ])
    .slice(1, 3);
    let needle = strings(vec![Some("é")]);
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Int64,
        integers(vec![Some(1), Some(2)]),
        Box::default(),
    )
    .unwrap();
    let args = [
        EvaluatedArgument::Column(&haystack),
        EvaluatedArgument::Scalar(&needle),
        EvaluatedArgument::SelectedColumn(&compact),
    ];
    let mut kernel = instance("strpos", 3);
    let result = kernel
        .evaluate(selection, &args, &Control::default())
        .unwrap();
    assert_eq!(result.selection(), selection);
    assert_eq!(output(&result), vec![Some(2), Some(4)]);
    let constants = pool(strings(vec![None, Some("unused"), Some("aé中é")]));
    let constant = constants.value(2).unwrap();
    let compact_needle = SelectedValues::try_new(
        selection,
        &DataType::Utf8,
        strings(vec![Some("é"), Some("中")]),
        Box::default(),
    )
    .unwrap();
    for name in NAMES {
        let args = [
            EvaluatedArgument::Constant(&constant),
            EvaluatedArgument::SelectedColumn(&compact_needle),
        ];
        let mut kernel = instance(name, 2);
        assert_eq!(
            output(
                &kernel
                    .evaluate(selection, &args, &Control::default())
                    .unwrap()
            ),
            vec![Some(2), Some(3)]
        );
    }
    assert_eq!(constant.ordinal(), 2);
    assert!(Arc::ptr_eq(constant.pool().array(), constants.array()));
    assert_eq!(
        constant.pool().field().metadata()["source-note"],
        "kept selected backing"
    );
}

#[test]
fn string_locate_strict_null_and_inactive_rows_never_search_hidden_payload() {
    let huge = "中".repeat(320 * 1024);
    let mut bytes = huge.as_bytes().to_vec();
    bytes.push(b'x');
    let mut valid = BooleanBufferBuilder::new(2);
    valid.append(false);
    valid.append(true);
    let hidden = Arc::new(StringArray::new(
        OffsetBuffer::new(vec![0i32, huge.len() as i32, (huge.len() + 1) as i32].into()),
        Buffer::from(bytes),
        Some(NullBuffer::new(valid.finish())),
    )) as ArrayRef;
    let inactive = strings(vec![Some(&huge), Some("x")]);
    let needles = strings(vec![Some("z"), Some("x")]);
    let null_needles = strings(vec![None, Some("x")]);
    let starts = integers(vec![Some(1), Some(1)]);
    let null_starts = integers(vec![None, Some(1)]);
    for (haystack, needle, start, first) in [
        (&hidden, &needles, &starts, None),
        (&inactive, &null_needles, &starts, None),
        (&inactive, &needles, &null_starts, None),
    ] {
        let args = [
            EvaluatedArgument::Column(haystack),
            EvaluatedArgument::Column(needle),
            EvaluatedArgument::Column(start),
        ];
        let control = Control::default();
        let mut kernel = instance("strpos", 3);
        assert_eq!(
            output(&kernel.evaluate(Selection::all(2), &args, &control).unwrap()),
            vec![first, Some(1)]
        );
        assert!(!control.trace.lock().unwrap().contains(&256));
    }
    let rows = [1];
    let selection = Selection::try_sparse(2, &rows).unwrap();
    let args = ordinary_args(&inactive, &needles);
    let control = Control::default();
    let mut kernel = instance("strpos", 2);
    assert_eq!(
        output(&kernel.evaluate(selection, &args, &control).unwrap()),
        vec![Some(1)]
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
}

#[test]
fn string_locate_empty_selection_wrong_carrier_and_child_errors_preserve_failed_latch() {
    let haystack = strings(vec![Some("abc")]);
    let needle = strings(vec![Some("b")]);
    let starts = integers(vec![Some(1)]);
    let wrong = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
    let args = [
        EvaluatedArgument::Column(&haystack),
        EvaluatedArgument::Column(&needle),
        EvaluatedArgument::Column(&starts),
    ];
    let empty = [];
    let selection = Selection::try_sparse(1, &empty).unwrap();
    let mut kernel = instance("strpos", 3);
    assert!(
        output(
            &kernel
                .evaluate(selection, &args, &Control::default())
                .unwrap()
        )
        .is_empty()
    );
    assert_eq!(
        output(
            &kernel
                .evaluate(Selection::all(1), &args, &Control::default())
                .unwrap()
        ),
        vec![Some(2)]
    );
    let wrong = [
        EvaluatedArgument::Column(&haystack),
        EvaluatedArgument::Column(&needle),
        EvaluatedArgument::Column(&wrong),
    ];
    assert!(matches!(
        kernel.evaluate(Selection::all(1), &wrong, &Control::default()),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
    let after = Control::default();
    assert_eq!(
        kernel
            .evaluate(Selection::all(1), &args, &after)
            .unwrap_err(),
        KernelFailure::InstanceFailed
    );
    assert!(after.trace.lock().unwrap().is_empty());
    let failed = SelectedValues::try_new(
        Selection::all(1),
        &DataType::Utf8,
        strings(vec![None]),
        Box::from([crate::RowDataError::new(0, "required needle failed")]),
    )
    .unwrap();
    let args = [
        EvaluatedArgument::Column(&haystack),
        EvaluatedArgument::SelectedColumn(&failed),
    ];
    let mut kernel = instance("strpos", 2);
    assert!(matches!(
        kernel.evaluate(Selection::all(1), &args, &Control::default()),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
}

#[test]
fn string_locate_layout_gate_precedes_output_requests() {
    assert_eq!(
        output_capacity(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    );
    assert!(output_capacity(0).is_ok());
    assert!(output_capacity(320).is_ok());
}

#[test]
fn string_locate_every_small_callback_and_real_character_quantum_keep_original_three_causes() {
    for (haystack, wide, wrong) in [
        (strings(vec![Some("aé中é"), None]), false, false),
        (strings(vec![Some(&"中".repeat(320))]), true, false),
        (strings(vec![Some("abc")]), false, true),
    ] {
        let needle = strings(vec![Some("é"); haystack.len()]);
        let start = if wrong {
            Arc::new(Int32Array::from(vec![1; haystack.len()])) as ArrayRef
        } else {
            integers(vec![Some(1); haystack.len()])
        };
        let args = [
            EvaluatedArgument::Column(&haystack),
            EvaluatedArgument::Column(&needle),
            EvaluatedArgument::Column(&start),
        ];
        let good = Control::default();
        let mut kernel = instance("strpos", 3);
        let result = kernel.evaluate(Selection::all(haystack.len()), &args, &good);
        if wrong {
            assert!(matches!(result, Err(KernelFailure::InvalidProgram(_))));
        } else {
            result.unwrap();
        }
        let trace = good.trace.lock().unwrap().clone();
        if wide {
            assert!(trace.contains(&256));
        }
        for (at, units) in trace.iter().enumerate() {
            if wide && at != 0 && at + 1 != trace.len() && *units != 256 {
                continue;
            }
            for cause in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
            ] {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let mut kernel = instance("strpos", 3);
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(haystack.len()), &args, &control)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                let after = Control::default();
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(haystack.len()), &args, &after)
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}
#[test]
fn string_locate_owner_preserves_exact_fresh_frozen_arc_effects_and_rejects_stale_sources() {
    for name in NAMES {
        for arity in 2..=3 {
            assert_eq!(operation(name), Some(StringLocateOp::Strpos));
            assert!(operation("substring_index").is_none());
            let owner = owner_for_test(name);
            let arguments: Vec<_> = sources(arity)
                .into_iter()
                .map(|value_type| FunctionArgument::Value {
                    value_type,
                    constant: None,
                })
                .collect();
            let request = FunctionBindingRequest {
                expected_result_type: None,
                arguments: &arguments,
                logical_argument_count: arity,
            };
            let selected = Arc::new(
                owner
                    .resolve(request, crate::binding_test_control())
                    .unwrap(),
            );
            assert_eq!(selected.result_type, FunctionResultType::Scalar(target()));
            let context = ExpressionEffectContext {
                use_id: ExpressionUseId::new(41),
                region: EvaluationRegionId::new(7),
                demand: EvaluationDemand::Value,
            };
            let uses: Vec<_> = (0..arity)
                .map(|i| Some(ExpressionUseId::new(42 + i as u32)))
                .collect();
            let params = SemanticParameters::try_new([]).unwrap();
            let input = crate::CallEffectInput {
                context: context.into(),
                argument_uses: crate::CallArgumentUses::SelectedChannels(&uses),
                function_id: owner.binding_declaration().function_id(),
                kind: crate::FunctionKind::Scalar,
                selected: &selected,
                request,
                environment: &[],
                parameters: &params,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                proof_scope: CallProofScope::Region(context.region),
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
                strpos_effects(arity).own_row_error
            );
            assert_eq!(
                canonical.effects().argument_control,
                novarocks_type_contract::ArgumentControl::OrderedNull
            );
            assert_eq!(
                canonical.effects().failure_behavior,
                crate::FunctionFailureBehavior::Propagate
            );
            assert!(canonical.effects().environment.is_empty());
            assert_eq!(
                owner.binding_declaration().function_id().as_str(),
                format!("builtin.scalar/{name}/v1")
            );
            assert_eq!(owner.implementation_declarations().len(), 2);
            assert_eq!(
                owner.implementation_declarations()[0]
                    .implementation
                    .as_str(),
                format!("builtin.scalar/{name}/selected-v1")
            );
            assert_eq!(
                owner.binding_declaration().overloads()[0].effects.as_ref(),
                Some(&effects())
            );
            assert_eq!(owner.binding_declaration().overloads().len(), 2);
            for count in [0, 1, 4] {
                let bad_args: Vec<_> = (0..count)
                    .map(|i| arguments[i.min(arity - 1)].clone())
                    .collect();
                assert!(
                    owner
                        .resolve(
                            FunctionBindingRequest {
                                arguments: &bad_args,
                                logical_argument_count: count,
                                expected_result_type: None
                            },
                            crate::binding_test_control()
                        )
                        .is_err()
                );
            }
            let pattern_arguments = [arguments[0].clone()];
            assert!(
                owner
                    .resolve(
                        FunctionBindingRequest {
                            arguments: &pattern_arguments,
                            logical_argument_count: 1,
                            expected_result_type: None,
                        },
                        crate::binding_test_control()
                    )
                    .is_err()
            );
            assert!(std::ptr::eq(canonical.selected(), selected.as_ref()));
            let mut forged = (*selected).clone();
            forged.result_type =
                FunctionResultType::Scalar(FunctionValueType::new(DataType::Int32, false));
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
            stale.context = novarocks_type_contract::ExpressionEffectContext {
                use_id: ExpressionUseId::new(999),
                ..stale.context.expression().unwrap()
            }
            .into();
            assert!(
                owner
                    .prepare_scalar(stale, canonical, crate::binding_test_control())
                    .is_err()
            );
            for source in [
                FunctionValueType::new(DataType::LargeUtf8, true),
                FunctionValueType::new(DataType::Binary, true),
                FunctionValueType::from_legacy_declared_projection(
                    DataType::List(Arc::new(arrow_schema::Field::new(
                        "item",
                        DataType::Int64,
                        true,
                    ))),
                    true,
                ),
                FunctionValueType::try_with_logical_type(
                    DataType::LargeBinary,
                    true,
                    ValueLogicalType::Json,
                )
                .unwrap(),
            ] {
                assert!(
                    prepared_for_test_with_policy(
                        name,
                        &[source, FunctionValueType::new(DataType::Utf8, true)],
                        DecimalOverflowPolicy::OutputNull
                    )
                    .is_err()
                );
            }
        }
    }
}

#[test]
fn string_locate_compile_success_and_pre_error_callbacks_preserve_all_causes() {
    for ty in [source(true), FunctionValueType::new(DataType::Binary, true)] {
        let good = CompileControl::default();
        let success = ty.data_type() == &DataType::Utf8;
        assert_eq!(
            prepared_for_test_with_control(
                "strpos",
                &[ty.clone(), source(true)],
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
                CompileControlError::from_domain(
                    novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
                ),
            ] {
                let control = CompileControl {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let actual = prepared_for_test_with_control(
                    "strpos",
                    &[ty.clone(), source(true)],
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
fn string_locate_constant_null_prepares_and_empty_invocation_does_not_search() {
    let owner = owner_for_test("strpos");
    let constants = pool(strings(vec![Some("unused"), None, Some("abc")]));
    let constant = constants.value(1).unwrap();
    let types = sources(2);
    let args = [
        FunctionArgument::Value {
            value_type: types[0].clone(),
            constant: Some(constant.clone()),
        },
        FunctionArgument::Value {
            value_type: types[1].clone(),
            constant: None,
        },
    ];
    let request = FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: 2,
        expected_result_type: None,
    };
    let selected = Arc::new(
        owner
            .resolve(request, crate::binding_test_control())
            .unwrap(),
    );
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        region: EvaluationRegionId::new(7),
        demand: EvaluationDemand::Value,
    };
    let uses = [
        Some(ExpressionUseId::new(42)),
        Some(ExpressionUseId::new(43)),
    ];
    let params = SemanticParameters::try_new([]).unwrap();
    let input = crate::CallEffectInput {
        context: context.into(),
        argument_uses: crate::CallArgumentUses::SelectedChannels(&uses),
        function_id: owner.binding_declaration().function_id(),
        kind: crate::FunctionKind::Scalar,
        selected: &selected,
        request,
        environment: &[],
        parameters: &params,
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
    let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let pos = strings(vec![Some("unused needle")]);
    let evaluated = [
        EvaluatedArgument::Constant(&constant),
        EvaluatedArgument::Scalar(&pos),
    ];
    let empty = [];
    let selection = Selection::try_sparse(3, &empty).unwrap();
    assert!(
        output(
            &kernel
                .evaluate(selection, &evaluated, &Control::default())
                .unwrap()
        )
        .is_empty()
    );
    assert_eq!(
        output(
            &kernel
                .evaluate(Selection::all(3), &evaluated, &Control::default())
                .unwrap()
        ),
        vec![None; 3]
    );
    assert_eq!(constant.ordinal(), 1);
    assert!(Arc::ptr_eq(constant.pool().array(), constants.array()));
}

/// The original production registry and owner factory, not a second resolver
/// or a testing implementation of the compile contract.
struct LocateCompileFixture {
    owner: super::super::string_locate_owner::StringLocateOwner,
    arguments: Vec<FunctionArgument>,
    selected: Arc<crate::FunctionBindingSelection>,
    parameters: SemanticParameters,
    uses: Vec<Option<ExpressionUseId>>,
}
impl LocateCompileFixture {
    fn new(name: &str, arity: usize) -> Self {
        let owner = owner_for_test(name);
        let arguments = sources(arity)
            .into_iter()
            .map(|value_type| FunctionArgument::Value {
                value_type,
                constant: None,
            })
            .collect::<Vec<_>>();
        let request = FunctionBindingRequest {
            arguments: &arguments,
            logical_argument_count: arity,
            expected_result_type: None,
        };
        let selected = Arc::new(
            owner
                .resolve(request, crate::binding_test_control())
                .unwrap(),
        );
        Self {
            owner,
            arguments,
            selected,
            parameters: SemanticParameters::try_new([]).unwrap(),
            uses: (0..arity)
                .map(|at| Some(ExpressionUseId::new(42 + at as u32)))
                .collect(),
        }
    }
    fn input(&self) -> crate::CallEffectInput<'_> {
        let context = ExpressionEffectContext {
            use_id: ExpressionUseId::new(41),
            region: EvaluationRegionId::new(7),
            demand: EvaluationDemand::Value,
        };
        crate::CallEffectInput {
            context: context.into(),
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.uses),
            function_id: self.owner.binding_declaration().function_id(),
            kind: crate::FunctionKind::Scalar,
            selected: &self.selected,
            request: FunctionBindingRequest {
                arguments: &self.arguments,
                logical_argument_count: self.arguments.len(),
                expected_result_type: None,
            },
            environment: &[],
            parameters: &self.parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Region(context.region),
        }
    }
    fn canonical(&self) -> Arc<crate::ScalarCallContract> {
        let input = self.input();
        let actual = specialize_scalar(
            &self.owner,
            input,
            self.selected.clone(),
            ScopedExpressionEffects::pure_value(input.context.expression().unwrap()),
            crate::binding_test_control(),
        )
        .unwrap();
        actual.prepared().contract().clone()
    }
}
fn locate_compile_causes() -> [CompileControlError; 4] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
        CompileControlError::from_domain(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        ),
    ]
}

#[test]
fn string_locate_actual_refiner_keeps_identity_and_scope_refusals_before_late_control() {
    use crate::{FunctionBindingError, FunctionEffectOwner, FunctionEffectOwnerError};
    for (name, arity) in [("strpos", 2), ("strpos", 3)] {
        let fixture = LocateCompileFixture::new(name, arity);
        let foreign = crate::FunctionId::try_new("fixture/foreign/string-owner").unwrap();
        let environment = [novarocks_type_contract::SemanticParameterRef {
            id: novarocks_type_contract::SemanticParameterId::new(0),
            expected_key: novarocks_type_contract::SemanticParameterKey::StatementStartUtc,
        }];
        for field in 0..3 {
            let mut wrong = fixture.input();
            match field {
                0 => wrong.function_id = &foreign,
                1 => wrong.proof_scope = CallProofScope::Region(EvaluationRegionId::new(99)),
                _ => wrong.environment = &environment,
            }
            let before = Arc::strong_count(&fixture.selected);
            let baseline = CompileControl::default();
            let original = fixture
                .owner
                .validate_and_refine(wrong, &baseline)
                .unwrap_err();
            if field == 0 {
                assert!(matches!(
                    original,
                    FunctionEffectOwnerError::Owner(FunctionBindingError::UnknownFunction)
                ));
            } else {
                assert!(matches!(
                    original,
                    FunctionEffectOwnerError::Owner(FunctionBindingError::InvalidBinding(_))
                ));
            }
            let trace = baseline.trace.lock().unwrap().clone();
            assert_eq!(trace, [(CompilePhase::FunctionSpecialization, 0)]);
            for cause in locate_compile_causes() {
                let earlier = CompileControl {
                    trace: Mutex::default(),
                    refusal: Some((0, cause.clone())),
                };
                assert!(matches!(fixture.owner.validate_and_refine(wrong, &earlier),
                    Err(FunctionEffectOwnerError::Control(actual)) if actual == cause));
                assert_eq!(*earlier.trace.lock().unwrap(), trace);
                let late = CompileControl {
                    trace: Mutex::default(),
                    refusal: Some((1, cause)),
                };
                let actual = fixture.owner.validate_and_refine(wrong, &late).unwrap_err();
                if field == 0 {
                    assert!(matches!(
                        actual,
                        FunctionEffectOwnerError::Owner(FunctionBindingError::UnknownFunction)
                    ));
                } else {
                    assert!(matches!(
                        actual,
                        FunctionEffectOwnerError::Owner(FunctionBindingError::InvalidBinding(_))
                    ));
                }
                assert_eq!(
                    *late.trace.lock().unwrap(),
                    trace,
                    "the actual owner refused before this armed completion callback"
                );
                assert_eq!(Arc::strong_count(&fixture.selected), before);
            }
        }
    }
}

#[test]
fn string_locate_actual_prepare_keeps_stale_contract_refusal_and_releases_original_arc() {
    for (name, arity) in [("strpos", 2), ("strpos", 3)] {
        let fixture = LocateCompileFixture::new(name, arity);
        let canonical = fixture.canonical();
        let foreign = crate::FunctionId::try_new("fixture/foreign/string-owner").unwrap();
        let copied_selected = fixture.selected.as_ref().clone();
        for field in 0..5 {
            let mut wrong = fixture.input();
            match field {
                0 => wrong.function_id = &foreign,
                1 => {
                    wrong.context = novarocks_type_contract::ExpressionEffectContext {
                        use_id: ExpressionUseId::new(999),
                        ..wrong.context.expression().unwrap()
                    }
                    .into()
                }
                2 => wrong.selected = &copied_selected,
                3 => wrong.decimal_overflow_policy = DecimalOverflowPolicy::OutputNull,
                _ => wrong.proof_scope = CallProofScope::Unconditional,
            }
            let original_arcs = Arc::strong_count(&canonical);
            let selected_arcs = Arc::strong_count(&fixture.selected);
            let baseline = CompileControl::default();
            let original = fixture
                .owner
                .prepare_scalar(wrong, canonical.clone(), &baseline)
                .unwrap_err();
            assert!(matches!(&original, KernelFailure::InvalidProgram(_)));
            let trace = baseline.trace.lock().unwrap().clone();
            assert_eq!(trace, [(CompilePhase::FunctionSpecialization, 0)]);
            assert_eq!(Arc::strong_count(&canonical), original_arcs);
            for cause in locate_compile_causes() {
                let earlier = CompileControl {
                    trace: Mutex::default(),
                    refusal: Some((0, cause.clone())),
                };
                assert!(
                    matches!(fixture.owner.prepare_scalar(wrong, canonical.clone(), &earlier),
                    Err(KernelFailure::CompileControl(actual)) if actual == cause)
                );
                assert_eq!(*earlier.trace.lock().unwrap(), trace);
                let late = CompileControl {
                    trace: Mutex::default(),
                    refusal: Some((1, cause)),
                };
                assert_eq!(
                    fixture
                        .owner
                        .prepare_scalar(wrong, canonical.clone(), &late)
                        .unwrap_err(),
                    original
                );
                assert_eq!(*late.trace.lock().unwrap(), trace);
                assert_eq!(Arc::strong_count(&canonical), original_arcs);
                assert_eq!(Arc::strong_count(&fixture.selected), selected_arcs);
            }
        }
    }
}

#[test]
fn string_locate_actual_success_keeps_refine_prepare_tails_sources_and_prepared_owner_drop() {
    use crate::{FunctionEffectOwner, FunctionEffectOwnerError};
    for (name, arity) in [("strpos", 2), ("strpos", 3)] {
        let fixture = LocateCompileFixture::new(name, arity);
        let input = fixture.input();
        let baseline = CompileControl::default();
        fixture.owner.validate_and_refine(input, &baseline).unwrap();
        let refine_trace = baseline.trace.lock().unwrap().clone();
        assert!(refine_trace.len() >= 2);
        assert!(refine_trace.iter().any(|(_, units)| *units > 0));
        for at in 0..refine_trace.len() {
            for cause in locate_compile_causes() {
                let control = CompileControl {
                    trace: Mutex::default(),
                    refusal: Some((at, cause.clone())),
                };
                assert!(matches!(fixture.owner.validate_and_refine(input, &control),
                    Err(FunctionEffectOwnerError::Control(actual)) if actual == cause));
                assert_eq!(*control.trace.lock().unwrap(), refine_trace[..=at]);
            }
        }
        let canonical = fixture.canonical();
        let original_arcs = Arc::strong_count(&canonical);
        let baseline = CompileControl::default();
        let prepared = fixture
            .owner
            .prepare_scalar(input, canonical.clone(), &baseline)
            .unwrap();
        assert!(Arc::ptr_eq(prepared.contract(), &canonical));
        assert_eq!(Arc::strong_count(&canonical), original_arcs + 1);
        let prepared_weak = Arc::downgrade(&prepared);
        drop(prepared);
        assert!(prepared_weak.upgrade().is_none());
        drop(prepared_weak);
        assert_eq!(Arc::strong_count(&canonical), original_arcs);
        let prepare_trace = baseline.trace.lock().unwrap().clone();
        assert!(prepare_trace.len() >= 2);
        assert!(prepare_trace.iter().any(|(_, units)| *units > 0));
        for at in 0..prepare_trace.len() {
            for cause in locate_compile_causes() {
                let control = CompileControl {
                    trace: Mutex::default(),
                    refusal: Some((at, cause.clone())),
                };
                assert!(
                    matches!(fixture.owner.prepare_scalar(input, canonical.clone(), &control),
                    Err(KernelFailure::CompileControl(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), prepare_trace[..=at]);
                // A late successful-body footer can reject after constructing
                // PreparedStringLocate. Its real contract Arc must still drop.
                assert_eq!(Arc::strong_count(&canonical), original_arcs);
            }
        }
    }
}
