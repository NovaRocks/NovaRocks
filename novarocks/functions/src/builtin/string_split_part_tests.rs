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

use super::super::string_split_part_owner::{
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
        panic!("split_part never waits")
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
    let ty = FunctionValueType::from_legacy_declared_projection(array.data_type().clone(), true);
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

const NAMES: [&str; 1] = ["split_part"];
fn sources(arity: usize) -> Vec<FunctionValueType> {
    (0..arity)
        .map(|i| {
            FunctionValueType::from_legacy_declared_projection(
                if i < 2 {
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
fn string_split_part_exact_profile_preserves_direction_empty_delimiter_and_unicode_oracles() {
    let cases = [
        ("a,b,c", ",", 1, "a"),
        ("a,b,c", ",", 2, "b"),
        ("a,b,c", ",", 3, "c"),
        ("a,b,c", ",", 4, ""),
        ("a,b,c", ",", -1, "c"),
        ("a,b,c", ",", -2, "b"),
        ("a,b,c", ",", -3, "a"),
        ("a,b,c", ",", -4, ""),
        ("abc", ",", 1, "abc"),
        ("abc", ",", -1, "abc"),
        ("abc", ",", 2, ""),
        (",a,,", ",", 2, "a"),
        (",a,,", ",", -2, ""),
        ("aaa", "aa", 1, ""),
        ("aaa", "aa", 2, "a"),
        ("aaa", "aa", -1, ""),
        ("aaa", "aa", -2, "a"),
        ("é中👩‍💻", "", 1, "é"),
        ("é中👩‍💻", "", 2, "中"),
        ("é中👩‍💻", "", 4, "‍"),
        ("é中👩‍💻", "", 6, ""),
        ("é中👩‍💻", "", -1, "é"),
        ("é中👩‍💻", "", -99, "é"),
        ("é中👩‍💻", "", i32::MIN, "é"),
        ("é中", "", i32::MAX, ""),
        ("a\u{301}", "", 2, "\u{301}"),
        ("a\0b\0c", "\0", -2, "b"),
        ("你好::世界", "::", -1, "世界"),
        ("", "", -1, ""),
        ("abc", "", 0, ""),
        ("abc", ",", i32::MIN, ""),
        ("abc", ",", i32::MAX, ""),
    ];
    let text = strings(cases.iter().map(|c| Some(c.0)).collect());
    let delim = strings(cases.iter().map(|c| Some(c.1)).collect());
    let index = integers(cases.iter().map(|c| Some(c.2)).collect());
    let arguments = [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::Column(&delim),
        EvaluatedArgument::Column(&index),
    ];
    for nullable in [false, true] {
        let types: Vec<_> = sources(3)
            .into_iter()
            .map(|mut ty| {
                ty = ty
                    .with_nullable(nullable)
                    .expect("valid function nullability declaration");
                ty
            })
            .collect();
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let prepared = prepared_for_test_with_policy("split_part", &types, policy).unwrap();
            assert_eq!(prepared.contract().result_type(), &source(true));
            assert_eq!(prepared.instance_retained_upper_bound(), 0);
            assert_eq!(prepared.contract().decimal_overflow_policy(), policy);
            let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
            for _ in 0..2 {
                let result = kernel
                    .evaluate(Selection::all(cases.len()), &arguments, &Control::default())
                    .unwrap();
                assert_eq!(
                    output(&result),
                    cases
                        .iter()
                        .map(|c| Some(c.3.to_string()))
                        .collect::<Vec<_>>()
                );
            }
        }
    }
}

#[test]
fn string_split_part_each_argument_has_its_own_sparse_compact_scalar_and_constant_address() {
    let backing = strings(vec![
        Some("unused"),
        Some("a,b,c"),
        None,
        Some("é::中::z"),
        Some("unused"),
    ]);
    let text = backing.slice(1, 3);
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let delimiters = SelectedValues::try_new(
        selection,
        &DataType::Utf8,
        strings(vec![Some(","), Some("::")]),
        Box::default(),
    )
    .unwrap();
    let indices = pool(integers(vec![Some(i32::MIN), None, Some(2)]));
    let index = indices.value(2).unwrap();
    let arguments = [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::SelectedColumn(&delimiters),
        EvaluatedArgument::Constant(&index),
    ];
    let mut kernel = instance("split_part", 3);
    let result = kernel
        .evaluate(selection, &arguments, &Control::default())
        .unwrap();
    assert_eq!(result.selection(), selection);
    assert_eq!(output(&result), vec![Some("b".into()), Some("中".into())]);
    assert_eq!(index.ordinal(), 2);
    assert!(Arc::ptr_eq(index.pool().array(), indices.array()));
    let texts = pool(strings(vec![
        Some("wrong"),
        Some("first/second/third"),
        None,
    ]));
    let text = texts.value(1).unwrap();
    let delims = pool(strings(vec![None, Some("wrong"), Some("/")]));
    let delim = delims.value(2).unwrap();
    let selected_indices = SelectedValues::try_new(
        selection,
        &DataType::Int32,
        integers(vec![Some(-1), Some(1)]),
        Box::default(),
    )
    .unwrap();
    let arguments = [
        EvaluatedArgument::Constant(&text),
        EvaluatedArgument::Constant(&delim),
        EvaluatedArgument::SelectedColumn(&selected_indices),
    ];
    assert_eq!(
        output(
            &kernel
                .evaluate(selection, &arguments, &Control::default())
                .unwrap()
        ),
        vec![Some("third".into()), Some("first".into())]
    );
    let text = strings(vec![Some("α|β")]);
    let delim = strings(vec![Some("|")]);
    let index = integers(vec![Some(-1)]);
    let arguments = [
        EvaluatedArgument::Scalar(&text),
        EvaluatedArgument::Scalar(&delim),
        EvaluatedArgument::Scalar(&index),
    ];
    assert_eq!(
        output(
            &kernel
                .evaluate(selection, &arguments, &Control::default())
                .unwrap()
        ),
        vec![Some("β".into()); 2]
    );
}

#[test]
fn string_split_part_null_is_called_on_null_empty_without_reading_hidden_or_other_payloads() {
    use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
    let huge = "a".repeat(320 * 1024);
    let mut bytes = huge.as_bytes().to_vec();
    bytes.extend_from_slice(b"a,b");
    // NULL is authored by the actual bitmap; hidden bytes remain present.
    let mut validity = BooleanBufferBuilder::new(2);
    validity.append(false);
    validity.append(true);
    let hidden = Arc::new(StringArray::new(
        OffsetBuffer::new(vec![0i32, huge.len() as i32, (huge.len() + 3) as i32].into()),
        Buffer::from(bytes),
        Some(NullBuffer::new(validity.finish())),
    )) as ArrayRef;
    let delim = strings(vec![Some(","), Some(",")]);
    let index = integers(vec![Some(2), Some(2)]);
    let arguments = [
        EvaluatedArgument::Column(&hidden),
        EvaluatedArgument::Column(&delim),
        EvaluatedArgument::Column(&index),
    ];
    let control = Control::default();
    let mut kernel = instance("split_part", 3);
    assert_eq!(
        output(
            &kernel
                .evaluate(Selection::all(2), &arguments, &control)
                .unwrap()
        ),
        vec![Some("".into()), Some("b".into())]
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
    for null_arg in 0..3 {
        let text = strings(vec![Some(&huge)]);
        let delimiter = strings(vec![Some("z")]);
        let indices = integers(vec![Some(i32::MAX)]);
        let null_text = strings(vec![None]);
        let null_index = integers(vec![None]);
        let arguments = [
            EvaluatedArgument::Column(if null_arg == 0 { &null_text } else { &text }),
            EvaluatedArgument::Column(if null_arg == 1 {
                &null_text
            } else {
                &delimiter
            }),
            EvaluatedArgument::Column(if null_arg == 2 { &null_index } else { &indices }),
        ];
        let control = Control::default();
        assert_eq!(
            output(
                &kernel
                    .evaluate(Selection::all(1), &arguments, &control)
                    .unwrap()
            ),
            vec![Some("".into())]
        );
        assert!(!control.trace.lock().unwrap().contains(&256));
    }
    let text = strings(vec![Some(&huge), Some("a,b")]);
    let delim = strings(vec![Some("z"), Some(",")]);
    let index = integers(vec![Some(i32::MAX), Some(2)]);
    let rows = [1];
    let selection = Selection::try_sparse(2, &rows).unwrap();
    let arguments = [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::Column(&delim),
        EvaluatedArgument::Column(&index),
    ];
    let control = Control::default();
    assert_eq!(
        output(&kernel.evaluate(selection, &arguments, &control).unwrap()),
        vec![Some("b".into())]
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
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
}

#[test]
fn string_split_part_required_child_errors_and_wrong_canonical_shapes_fail_and_latch() {
    let selection = Selection::all(1);
    let text = strings(vec![Some("a,b")]);
    let delimiter = strings(vec![Some(",")]);
    let indices = integers(vec![Some(2)]);
    for failed in 0..3 {
        let data_type = if failed < 2 {
            DataType::Utf8
        } else {
            DataType::Int32
        };
        let values = if failed < 2 {
            strings(vec![None])
        } else {
            integers(vec![None])
        };
        let child = SelectedValues::try_new(
            selection,
            &data_type,
            values,
            Box::from([crate::RowDataError::new(0, "required child failed")]),
        )
        .unwrap();
        let mut arguments = [
            EvaluatedArgument::Column(&text),
            EvaluatedArgument::Column(&delimiter),
            EvaluatedArgument::Column(&indices),
        ];
        arguments[failed] = EvaluatedArgument::SelectedColumn(&child);
        let mut kernel = instance("split_part", 3);
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
    }
    let wrong = Arc::new(arrow_array::Int64Array::from(vec![2])) as ArrayRef;
    let arguments = [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::Column(&delimiter),
        EvaluatedArgument::Column(&wrong),
    ];
    assert!(matches!(
        instance("split_part", 3).evaluate(selection, &arguments, &Control::default()),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
    let mut types = sources(3);
    types[0] = types[0]
        .with_nullable(false)
        .expect("valid function nullability declaration");
    let prepared =
        prepared_for_test_with_policy("split_part", &types, DecimalOverflowPolicy::OutputNull)
            .unwrap();
    let null = strings(vec![None]);
    let arguments = [
        EvaluatedArgument::Column(&null),
        EvaluatedArgument::Column(&delimiter),
        EvaluatedArgument::Column(&indices),
    ];
    assert!(matches!(
        ScalarEvaluationInstance::instantiate(prepared)
            .unwrap()
            .evaluate(selection, &arguments, &Control::default()),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
}

#[test]
fn string_split_part_output_capacity_precedes_requests() {
    assert_eq!(
        output_capacity(usize::MAX, 0),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(
        output_capacity(1, i32::MAX as usize + 1),
        Err(KernelFailure::ResourceExhausted)
    );
    assert!(output_capacity(0, 0).is_ok());
    assert!(output_capacity(320, 1024).is_ok());
}

#[test]
fn string_split_part_runtime_all_seven_causes_keep_small_prefix_and_actual_search_quantum() {
    use crate::kernel_control::KernelDiagnostic;
    let long = "x".repeat(320);
    for (text, delim, index, wide, wrong) in [
        (
            strings(vec![Some("a,b"), None]),
            strings(vec![Some(","), Some(",")]),
            integers(vec![Some(-1), Some(1)]),
            false,
            false,
        ),
        (
            strings(vec![Some("a,b")]),
            strings(vec![Some(",")]),
            Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef,
            false,
            true,
        ),
        (
            strings(vec![Some(&long)]),
            strings(vec![Some("y")]),
            integers(vec![Some(1)]),
            true,
            false,
        ),
        (
            strings(vec![Some(&long)]),
            strings(vec![Some("")]),
            integers(vec![Some(320)]),
            true,
            false,
        ),
    ] {
        let arguments = [
            EvaluatedArgument::Column(&text),
            EvaluatedArgument::Column(&delim),
            EvaluatedArgument::Column(&index),
        ];
        let good = Control::default();
        let mut kernel = instance("split_part", 3);
        let result = kernel.evaluate(Selection::all(text.len()), &arguments, &good);
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
                KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid")),
                KernelFailure::Internal(KernelDiagnostic::new("original internal")),
                KernelFailure::Operational(KernelDiagnostic::new("original operational")),
                KernelFailure::InstanceFailed,
            ] {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let mut kernel = instance("split_part", 3);
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(text.len()), &arguments, &control)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                let after = Control::default();
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(text.len()), &arguments, &after)
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}
#[test]
fn string_split_part_owner_preserves_exact_fresh_frozen_arc_effects_and_rejects_stale_sources() {
    for name in NAMES {
        let arity = 3;
        assert_eq!(operation(name), Some(()));
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
            FunctionNullBehavior::CalledOnNull
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
        assert_eq!(owner.implementation_declarations().len(), 1);
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
        assert_eq!(owner.binding_declaration().overloads().len(), 1);
        for count in [0, 2, 4] {
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
        let raw_types = [
            source(true),
            source(true),
            FunctionValueType::new(DataType::Int64, true),
        ];
        let raw_arguments: Vec<_> = raw_types
            .iter()
            .cloned()
            .map(|value_type| FunctionArgument::Value {
                value_type,
                constant: None,
            })
            .collect();
        let raw_request = FunctionBindingRequest {
            arguments: &raw_arguments,
            logical_argument_count: 3,
            expected_result_type: None,
        };
        let raw_selected = owner
            .resolve(raw_request, crate::binding_test_control())
            .unwrap();
        assert_eq!(
            raw_selected.argument_types.as_ref(),
            selected.argument_types.as_ref()
        );
        assert!(matches!(
            prepared_for_test_with_policy(name, &raw_types, DecimalOverflowPolicy::OutputNull),
            Err(FunctionSpecializationFailure::InvalidInput(_))
        ));
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
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
        ] {
            assert!(
                prepared_for_test_with_policy(
                    name,
                    &[
                        source,
                        FunctionValueType::new(DataType::Utf8, true),
                        FunctionValueType::new(DataType::Int32, true)
                    ],
                    DecimalOverflowPolicy::OutputNull
                )
                .is_err()
            );
        }
    }
}

#[test]
fn string_split_part_compile_success_and_ordinary_tail_preserve_all_callback_causes() {
    for ty in [source(true), FunctionValueType::new(DataType::Binary, true)] {
        let good = CompileControl::default();
        let success = ty.data_type() == &DataType::Utf8;
        assert_eq!(
            prepared_for_test_with_control(
                "split_part",
                &[
                    ty.clone(),
                    source(true),
                    FunctionValueType::new(DataType::Int32, true)
                ],
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
                    "split_part",
                    &[
                        ty.clone(),
                        source(true),
                        FunctionValueType::new(DataType::Int32, true),
                    ],
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
fn string_split_part_constant_null_is_deferred_and_no_unrelated_string_size_cap_is_added() {
    let owner = owner_for_test("split_part");
    let constants = pool(strings(vec![Some("unused"), None, Some("abc")]));
    let constant = constants.value(1).unwrap();
    let types = sources(3);
    let args = [
        FunctionArgument::Value {
            value_type: types[0].clone(),
            constant: Some(constant.clone()),
        },
        FunctionArgument::Value {
            value_type: types[1].clone(),
            constant: None,
        },
        FunctionArgument::Value {
            value_type: types[2].clone(),
            constant: None,
        },
    ];
    let request = FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: 3,
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
        Some(ExpressionUseId::new(44)),
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
    let delimiter = strings(vec![Some(",")]);
    let evaluated = [
        EvaluatedArgument::Constant(&constant),
        EvaluatedArgument::Scalar(&delimiter),
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
        vec![Some("".into()); 3]
    );
    assert_eq!(constant.ordinal(), 1);
    assert!(Arc::ptr_eq(constant.pool().array(), constants.array()));
    // SPLIT_PART has no CONCAT-style 1 MiB successful-NULL limit.
    let large = "x".repeat(1024 * 1024 + 1);
    let text = strings(vec![Some(&large)]);
    let evaluated = [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::Scalar(&delimiter),
        EvaluatedArgument::Scalar(&pos),
    ];
    let mut kernel = instance("split_part", 3);
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
