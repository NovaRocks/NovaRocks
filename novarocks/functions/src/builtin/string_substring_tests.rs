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

use super::super::string_substring_owner::{
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
        panic!("substring never waits")
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
fn output(result: &SelectedValues<'_>) -> Vec<Option<String>> {
    assert!(result.errors().is_empty());
    result
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|v| v.map(str::to_owned))
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

const NAMES: [&str; 2] = ["substr", "substring"];
fn sources(arity: usize) -> Vec<FunctionValueType> {
    (0..arity)
        .map(|i| {
            FunctionValueType::from_legacy_declared_projection(
                if i == 0 {
                    DataType::Utf8
                } else {
                    DataType::Int32
                },
                true,
            )
        })
        .collect()
}
fn integers(values: Vec<Option<i32>>) -> ArrayRef {
    Arc::new(Int32Array::from(values))
}

#[test]
fn string_substring_actual_profiles_preserve_independent_unicode_and_signed_boundaries() {
    // These explicit expectations are authored independently of the candidate span walker.
    let rows = [
        ("aé中👩‍💻", 1, Some(2), "aé"),
        ("aé中👩‍💻", -2, Some(1), "‍"),
        ("aé中👩‍💻", -3, None, "👩‍💻"),
        ("aé中👩‍💻", 2, None, "é中👩‍💻"),
        ("aé中👩‍💻", -1, None, "💻"),
        ("a\u{301}b", 2, Some(1), "\u{301}"),
        ("abc", 0, Some(2), ""),
        ("abc", 1, Some(0), ""),
        ("abc", -1, Some(-1), ""),
        ("abc", 4, Some(1), ""),
        ("abc", -4, Some(1), ""),
        ("abc", i32::MIN, None, ""),
        ("abc", i32::MAX, None, ""),
        ("abc", -3, Some(i32::MAX), "abc"),
        ("", -1, None, ""),
        ("x\0y", 2, Some(1), "\0"),
    ];
    for name in NAMES {
        for arity in [2, 3] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                let chosen: Vec<_> = rows
                    .iter()
                    .filter(|(_, _, length, _)| arity == 3 || length.is_none())
                    .collect();
                let text = strings(chosen.iter().map(|(text, _, _, _)| Some(*text)).collect());
                let start = integers(chosen.iter().map(|(_, pos, _, _)| Some(*pos)).collect());
                let length = integers(
                    chosen
                        .iter()
                        .map(|(_, _, length, _)| Some(length.unwrap_or(i32::MAX)))
                        .collect(),
                );
                let mut arguments = vec![
                    EvaluatedArgument::Column(&text),
                    EvaluatedArgument::Column(&start),
                ];
                if arity == 3 {
                    arguments.push(EvaluatedArgument::Column(&length));
                }
                let mut exact_sources = sources(arity);
                for ty in &mut exact_sources {
                    *ty = ty
                        .with_nullable(false)
                        .expect("valid function nullability declaration");
                }
                let prepared = prepared_for_test_with_policy(name, &exact_sources, policy).unwrap();
                assert_eq!(prepared.contract().result_type(), &source(true));
                assert_eq!(prepared.contract().decimal_overflow_policy(), policy);
                let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
                let result = kernel
                    .evaluate(Selection::all(text.len()), &arguments, &Control::default())
                    .unwrap();
                assert_eq!(
                    output(&result),
                    chosen
                        .iter()
                        .map(|(_, _, _, expected)| Some((*expected).into()))
                        .collect::<Vec<_>>()
                );
            }
        }
    }
}

#[test]
fn string_substring_each_argument_keeps_sparse_slice_compact_scalar_and_constant_addresses() {
    let text = strings(vec![
        Some("prefix"),
        Some("aé中"),
        Some("inactive"),
        Some("👩‍💻"),
        Some("suffix"),
    ])
    .slice(1, 3);
    let start = integers(vec![Some(99), Some(2), Some(99), Some(-1), Some(99)]).slice(1, 3);
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Int32,
        integers(vec![Some(1), Some(1)]),
        Box::default(),
    )
    .unwrap();
    let constants = pool(strings(vec![None, Some("unused"), Some("aé中")]));
    let constant = constants.value(2).unwrap();
    let scalar = integers(vec![Some(2)]);
    for name in NAMES {
        let args = [
            EvaluatedArgument::Column(&text),
            EvaluatedArgument::Column(&start),
            EvaluatedArgument::SelectedColumn(&compact),
        ];
        let mut kernel = instance(name, 3);
        let result = kernel
            .evaluate(selection, &args, &Control::default())
            .unwrap();
        assert_eq!(result.selection(), selection);
        assert_eq!(output(&result), vec![Some("é".into()), Some("💻".into())]);
        let args = [
            EvaluatedArgument::Constant(&constant),
            EvaluatedArgument::Scalar(&scalar),
            EvaluatedArgument::SelectedColumn(&compact),
        ];
        let result = kernel
            .evaluate(selection, &args, &Control::default())
            .unwrap();
        assert_eq!(output(&result), vec![Some("é".into()); 2]);
    }
    assert_eq!(constant.ordinal(), 2);
    assert!(Arc::ptr_eq(constant.pool().array(), constants.array()));
    assert_eq!(
        constant.pool().field().metadata()["source-note"],
        "kept selected backing"
    );
}

#[test]
fn string_substring_strict_null_and_inactive_payload_never_scan_hidden_text() {
    let huge = "中".repeat(320 * 1024);
    let mut bytes = huge.as_bytes().to_vec();
    bytes.extend_from_slice(b"abc");
    let mut valid = BooleanBufferBuilder::new(2);
    valid.append(false);
    valid.append(true);
    let hidden = Arc::new(StringArray::new(
        OffsetBuffer::new(vec![0i32, huge.len() as i32, (huge.len() + 3) as i32].into()),
        Buffer::from(bytes),
        Some(NullBuffer::new(valid.finish())),
    )) as ArrayRef;
    let inactive = strings(vec![Some(&huge), Some("abc")]);
    let positions = integers(vec![Some(1), Some(2)]);
    let lengths = integers(vec![Some(1), Some(1)]);
    let null_positions = integers(vec![None, Some(2)]);
    let null_lengths = integers(vec![None, Some(1)]);
    for name in NAMES {
        for (text, start, len) in [
            (&hidden, &positions, &lengths),
            (&inactive, &null_positions, &lengths),
            (&inactive, &positions, &null_lengths),
        ] {
            let control = Control::default();
            let mut kernel = instance(name, 3);
            let args = [
                EvaluatedArgument::Column(text),
                EvaluatedArgument::Column(start),
                EvaluatedArgument::Column(len),
            ];
            assert_eq!(
                output(&kernel.evaluate(Selection::all(2), &args, &control).unwrap()),
                vec![None, Some("b".into())]
            );
            assert!(!control.trace.lock().unwrap().contains(&256));
        }
        let rows = [1];
        let selection = Selection::try_sparse(2, &rows).unwrap();
        let args = [
            EvaluatedArgument::Column(&inactive),
            EvaluatedArgument::Column(&positions),
        ];
        let mut kernel = instance(name, 2);
        let control = Control::default();
        assert_eq!(
            output(&kernel.evaluate(selection, &args, &control).unwrap()),
            vec![Some("bc".into())]
        );
        assert!(!control.trace.lock().unwrap().contains(&256));
    }
}

#[test]
fn string_substring_empty_selection_and_wrong_selected_shape_preserve_failed_latch() {
    let text = strings(vec![Some("abc")]);
    let pos = integers(vec![Some(2)]);
    let wrong = Arc::new(arrow_array::Int64Array::from(vec![2])) as ArrayRef;
    let empty = [];
    let selection = Selection::try_sparse(1, &empty).unwrap();
    for name in NAMES {
        let mut kernel = instance(name, 2);
        let args = [
            EvaluatedArgument::Column(&text),
            EvaluatedArgument::Column(&pos),
        ];
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
            vec![Some("bc".into())]
        );
        let wrong_args = [
            EvaluatedArgument::Column(&text),
            EvaluatedArgument::Column(&wrong),
        ];
        assert!(matches!(
            kernel.evaluate(Selection::all(1), &wrong_args, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
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

#[test]
fn string_substring_required_child_error_cannot_become_successful_null() {
    let selection = Selection::all(1);
    let text = strings(vec![Some("abc")]);
    let failed = SelectedValues::try_new(
        selection,
        &DataType::Int32,
        integers(vec![None]),
        Box::from([crate::RowDataError::new(0, "required position failed")]),
    )
    .unwrap();
    let args = [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::SelectedColumn(&failed),
    ];
    let mut kernel = instance("substr", 2);
    assert!(matches!(
        kernel.evaluate(selection, &args, &Control::default()),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
}

#[test]
fn string_substring_output_layout_checks_all_extents_before_requests() {
    assert_eq!(
        output_capacity(usize::MAX, 0),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(
        output_capacity(1, i32::MAX as usize + 1),
        Err(KernelFailure::ResourceExhausted)
    );
    assert!(output_capacity(0, 0).is_ok());
    assert!(output_capacity(2, 8).is_ok());
}

#[test]
fn string_substring_runtime_callback_prefix_keeps_all_three_causes_and_real_256() {
    for (text, wide, wrong) in [
        (strings(vec![Some("aé中"), None, Some("abc")]), false, false),
        (strings(vec![Some(&"中".repeat(320))]), true, false),
        (strings(vec![Some("abc")]), false, true),
    ] {
        let pos = if wrong {
            Arc::new(arrow_array::Int64Array::from(vec![-2; text.len()])) as ArrayRef
        } else {
            integers(vec![Some(-2); text.len()])
        };
        let len = integers(vec![Some(1); text.len()]);
        let args = [
            EvaluatedArgument::Column(&text),
            EvaluatedArgument::Column(&pos),
            EvaluatedArgument::Column(&len),
        ];
        let good = Control::default();
        let mut kernel = instance("substring", 3);
        let result = kernel.evaluate(Selection::all(text.len()), &args, &good);
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
                let mut kernel = instance("substring", 3);
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(text.len()), &args, &control)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                let after = Control::default();
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(text.len()), &args, &after)
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}
#[test]
fn string_substring_owner_preserves_exact_fresh_frozen_arc_effects_and_rejects_stale_sources() {
    for name in NAMES {
        for arity in [2, 3] {
            assert_eq!(operation(name), Some(StringSubstringOp::Substring));
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
            assert_eq!(
                selected.result_type,
                FunctionResultType::Scalar(source(true))
            );
            let context = ExpressionEffectContext {
                use_id: ExpressionUseId::new(41),
                domain: EvaluationDomainId::new(7),
                demand: EvaluationDemand::Value,
            };
            let uses: Vec<_> = (0..arity)
                .map(|i| Some(ExpressionUseId::new(42 + i as u32)))
                .collect();
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
            for count in [0, 4] {
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
            forged.result_type = FunctionResultType::Scalar(source(false));
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
                    DataType::Utf8,
                    true,
                    ValueLogicalType::Json,
                )
                .unwrap(),
            ] {
                assert!(
                    prepared_for_test_with_policy(
                        name,
                        &[source, FunctionValueType::new(DataType::Int32, true)],
                        DecimalOverflowPolicy::OutputNull
                    )
                    .is_err()
                );
            }
        }
    }
}

#[test]
fn string_substring_compile_success_and_ordinary_tail_preserve_all_callback_causes() {
    for ty in [source(true), FunctionValueType::new(DataType::Binary, true)] {
        let good = CompileControl::default();
        let success = ty.data_type() == &DataType::Utf8;
        assert_eq!(
            prepared_for_test_with_control(
                "substring",
                &[ty.clone(), FunctionValueType::new(DataType::Int32, true)],
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
            ] {
                let control = CompileControl {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let actual = prepared_for_test_with_control(
                    "substring",
                    &[ty.clone(), FunctionValueType::new(DataType::Int32, true)],
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
fn string_substring_constant_null_is_deferred_and_no_unrelated_string_size_cap_is_added() {
    let owner = owner_for_test("substring");
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
        domain: EvaluationDomainId::new(7),
        demand: EvaluationDemand::Value,
    };
    let uses = [
        Some(ExpressionUseId::new(42)),
        Some(ExpressionUseId::new(43)),
    ];
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
    let pos = integers(vec![Some(1)]);
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
    // SUBSTRING has no CONCAT-style 1 MiB successful-NULL limit.
    let large = "x".repeat(1024 * 1024 + 1);
    let text = strings(vec![Some(&large)]);
    let evaluated = [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::Scalar(&pos),
    ];
    let mut kernel = instance("substr", 2);
    let result = kernel
        .evaluate(Selection::all(1), &evaluated, &Control::default())
        .unwrap();
    let result = result
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(!result.is_null(0));
    assert_eq!(result.value(0).as_bytes(), large.as_bytes());
}
