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

use super::super::string_concat_ws_owner::{
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
        panic!("concat_ws never waits")
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
        match self.refusal {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn source(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Utf8, nullable)
}
fn instance(sources: &[FunctionValueType]) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_policy("concat_ws", sources, DecimalOverflowPolicy::OutputNull)
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
    let ty = FunctionValueType::new(array.data_type().clone(), true);
    ConstantPool::try_new(
        Arc::new(
            ty.try_to_field("original")
                .unwrap()
                .with_metadata([("source-note".into(), "kept selected backing".into())].into()),
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

const NAMES: [&str; 1] = ["concat_ws"];
fn sources() -> Vec<FunctionValueType> {
    vec![source(true); 2]
}

#[test]
fn string_concat_ws_owner_preserves_exact_fresh_frozen_arc_effects_and_rejects_stale_sources() {
    for name in NAMES {
        let arity = 2;
        assert_eq!(operation(name), Some(()));
        assert!(operation("md5").is_none());
        let owner = owner_for_test(name);
        let arguments: Vec<_> = sources()
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
        let child_context = ExpressionEffectContext {
            use_id: ExpressionUseId::new(42),
            ..context
        };
        let arguments_effects = ScopedExpressionEffects::pure_value(context)
            .join_same_domain(ScopedExpressionEffects::primitive(
                child_context,
                novarocks_type_contract::ExpressionEffects {
                    may_raise_row_error: true,
                    ..novarocks_type_contract::ExpressionEffects::PURE_VALUE
                },
            ))
            .unwrap();
        let inherited = specialize_scalar(
            &owner,
            input,
            selected.clone(),
            arguments_effects,
            crate::binding_test_control(),
        )
        .unwrap();
        assert!(
            inherited
                .effects()
                .for_use(context)
                .unwrap()
                .may_raise_row_error
        );
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
        for count in [0, 1] {
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
        // The obsolete repeated-only declaration admitted zero/one inputs.
        // The actual minimum-two declaration has a distinct overload identity.
        let old_signature = super::super::signature::Signature::variadic(
            vec![super::super::signature::TypeSpec::Utf8],
            super::super::signature::TypeSpec::Utf8,
        )
        .canonical();
        let (old_declaration, _) = super::super::catalogue::scalar_definition_parts(
            name,
            &[old_signature],
            crate::FunctionKind::Scalar,
        )
        .unwrap();
        let mut stale_arity = (*selected).clone();
        stale_arity.overload = old_declaration.overloads()[0].identity.clone();
        assert_ne!(stale_arity.overload, selected.overload);
        assert!(
            owner
                .validate_selected(&stale_arity, request, crate::binding_test_control())
                .is_err()
        );
        let mut wrong_null = canonical.effects().clone();
        wrong_null.null_behavior = FunctionNullBehavior::Strict;
        assert!(
            specialize_frozen_scalar(
                &owner,
                input,
                selected.clone(),
                &wrong_null,
                ScopedExpressionEffects::pure_value(context),
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
        let mut wrong_effects = canonical.effects().clone();
        wrong_effects.instance_state = FunctionInstanceState::ScalarInstance;
        assert!(
            specialize_frozen_scalar(
                &owner,
                input,
                selected.clone(),
                &wrong_effects,
                ScopedExpressionEffects::pure_value(context),
                crate::binding_test_control()
            )
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
            FunctionValueType::new(
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
                    &[source, self::source(true)],
                    DecimalOverflowPolicy::OutputNull
                )
                .is_err()
            );
        }
    }
}

#[test]
fn string_concat_ws_compile_success_and_ordinary_tail_preserve_all_callback_causes() {
    for ty in [source(true), FunctionValueType::new(DataType::Binary, true)] {
        let good = CompileControl::default();
        let success = ty.data_type == DataType::Utf8;
        assert_eq!(
            prepared_for_test_with_control(
                "concat_ws",
                &[source(true), ty.clone()],
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
                    refusal: Some((at, cause)),
                };
                let actual = prepared_for_test_with_control(
                    "concat_ws",
                    &[source(true), ty.clone()],
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
fn string_concat_ws_minimum_two_unicode_null_empty_and_policy_oracles_are_exact() {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for nullable in [false, true] {
            for count in [2, 3, 5] {
                let separator = strings(vec![Some("💫"), Some("")]);
                let text = strings(vec![Some("é\0"), Some("")]);
                let types = vec![source(nullable); count];
                let prepared = prepared_for_test_with_policy("concat_ws", &types, policy).unwrap();
                assert_eq!(prepared.contract().result_type(), &source(true));
                assert_eq!(prepared.contract().decimal_overflow_policy(), policy);
                let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
                let mut arguments = vec![EvaluatedArgument::Column(&text); count];
                arguments[0] = EvaluatedArgument::Column(&separator);
                let expected = match count {
                    2 => "é\0",
                    3 => "é\0💫é\0",
                    5 => "é\0💫é\0💫é\0💫é\0",
                    _ => unreachable!(),
                };
                assert_eq!(
                    output(
                        &kernel
                            .evaluate(Selection::all(2), &arguments, &Control::default())
                            .unwrap()
                    ),
                    vec![Some(expected.into()), Some("".into())]
                );
            }
        }
    }
    let sep = strings(vec![
        Some("-"),
        None,
        Some(""),
        Some("::"),
        Some("\0"),
        Some("💫"),
    ]);
    let a = strings(vec![
        Some("a"),
        Some("a"),
        Some("a"),
        None,
        Some(""),
        Some(""),
    ]);
    let b = strings(vec![Some("b"), None, Some(""), None, None, Some("é")]);
    let c = strings(vec![
        Some("c"),
        Some("c"),
        Some("b"),
        None,
        Some(""),
        Some(""),
    ]);
    let arguments = [
        EvaluatedArgument::Column(&sep),
        EvaluatedArgument::Column(&a),
        EvaluatedArgument::Column(&b),
        EvaluatedArgument::Column(&c),
    ];
    assert_eq!(
        output(
            &instance(&[source(true), source(true), source(true), source(true)])
                .evaluate(Selection::all(6), &arguments, &Control::default())
                .unwrap()
        ),
        vec![
            Some("a-b-c".into()),
            None,
            Some("ab".into()),
            Some("".into()),
            Some("\0".into()),
            Some("💫é💫".into())
        ]
    );
    for other in ["concat", "format", "elt", "CONCAT_WS"] {
        assert!(operation(other).is_none());
    }
}

#[test]
fn string_concat_ws_independent_sparse_slice_compact_scalar_and_cv_addresses_are_exact() {
    let backing = strings(vec![
        Some("unused"),
        Some("A"),
        None,
        Some("中"),
        Some("unused"),
    ]);
    let sliced = backing.slice(1, 3);
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Utf8,
        strings(vec![Some("é"), None]),
        Box::default(),
    )
    .unwrap();
    let separator_pool = pool(strings(vec![Some("unused"), None, Some("💫")]));
    let separator = separator_pool.value(2).unwrap();
    let tail_pool = pool(strings(vec![Some("unused"), None, Some("\0!")]));
    let tail = tail_pool.value(2).unwrap();
    let scalar = strings(vec![Some("")]);
    let arguments = [
        EvaluatedArgument::Constant(&separator),
        EvaluatedArgument::Column(&sliced),
        EvaluatedArgument::SelectedColumn(&compact),
        EvaluatedArgument::Scalar(&scalar),
        EvaluatedArgument::Constant(&tail),
    ];
    let mut kernel = instance(&[
        source(true),
        source(true),
        source(true),
        source(false),
        source(true),
    ]);
    for _ in 0..2 {
        let result = kernel
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(result.selection(), selection);
        assert_eq!(
            output(&result),
            vec![Some("A💫é💫💫\0!".into()), Some("中💫💫\0!".into())]
        );
    }
    assert_eq!(separator.ordinal(), 2);
    assert_eq!(tail.ordinal(), 2);
    assert!(Arc::ptr_eq(
        separator.pool().array(),
        separator_pool.array()
    ));
    assert!(Arc::ptr_eq(tail.pool().array(), tail_pool.array()));
    let null_separator = separator_pool.value(1).unwrap();
    let arguments = [
        EvaluatedArgument::Constant(&null_separator),
        EvaluatedArgument::Column(&sliced),
    ];
    assert_eq!(
        output(
            &instance(&[source(true), source(true)])
                .evaluate(selection, &arguments, &Control::default())
                .unwrap()
        ),
        vec![None, None]
    );
    let null_value = tail_pool.value(1).unwrap();
    let arguments = [
        EvaluatedArgument::Constant(&separator),
        EvaluatedArgument::Constant(&null_value),
    ];
    assert_eq!(
        output(
            &instance(&[source(true), source(true)])
                .evaluate(selection, &arguments, &Control::default())
                .unwrap()
        ),
        vec![Some("".into()), Some("".into())]
    );
}

#[test]
fn string_concat_ws_original_one_mib_byte_limit_includes_only_used_separators() {
    let at = "a".repeat(1_048_576);
    let below = "b".repeat(1_048_575);
    let huge_separator = "💫".repeat(1_048_576 / 4 + 1);
    let separators = strings(vec![
        Some(""),
        Some("-"),
        Some("é"),
        Some("-"),
        Some(&huge_separator),
        Some(&huge_separator),
        Some(&huge_separator),
    ]);
    let a = strings(vec![
        Some(&at),
        Some(&below),
        Some(&below),
        Some(&at),
        Some("v"),
        Some(""),
        None,
    ]);
    let b = strings(vec![
        Some(""),
        Some(""),
        Some(""),
        None,
        None,
        Some(""),
        None,
    ]);
    let arguments = [
        EvaluatedArgument::Column(&separators),
        EvaluatedArgument::Column(&a),
        EvaluatedArgument::Column(&b),
    ];
    let result = instance(&[source(true), source(true), source(true)])
        .evaluate(Selection::all(7), &arguments, &Control::default())
        .unwrap();
    assert!(result.errors().is_empty());
    let values = result
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), at);
    assert_eq!(values.value(1), format!("{below}-"));
    assert!(values.is_null(2));
    assert_eq!(values.value(3), at);
    assert_eq!(values.value(4), "v");
    assert!(values.is_null(5));
    assert!(!values.is_null(6));
    assert_eq!(values.value(6), "");
}

fn hidden_null(payload: &str, next: &str) -> ArrayRef {
    let mut bytes = payload.as_bytes().to_vec();
    bytes.extend_from_slice(next.as_bytes());
    let mut validity = BooleanBufferBuilder::new(2);
    validity.append(false);
    validity.append(true);
    Arc::new(StringArray::new(
        OffsetBuffer::new(
            vec![0, payload.len() as i32, (payload.len() + next.len()) as i32].into(),
        ),
        Buffer::from(bytes),
        Some(NullBuffer::new(validity.finish())),
    ))
}
#[test]
fn string_concat_ws_hidden_null_and_inactive_spans_never_create_payload_copy_work() {
    let huge = "x".repeat(320 * 1024);
    let separator = hidden_null(&huge, "-");
    let value = strings(vec![Some(&huge), Some("A")]);
    let hidden = hidden_null(&huge, "é");
    let arguments = [
        EvaluatedArgument::Column(&separator),
        EvaluatedArgument::Column(&value),
        EvaluatedArgument::Column(&hidden),
    ];
    let good = Control::default();
    assert_eq!(
        output(
            &instance(&[source(true), source(true), source(true)])
                .evaluate(Selection::all(2), &arguments, &good)
                .unwrap()
        ),
        vec![None, Some("A-é".into())]
    );
    assert!(!good.trace.lock().unwrap().contains(&256));
    let sep = strings(vec![Some("-"), Some("-")]);
    let value = hidden_null(&huge, "A");
    let arguments = [
        EvaluatedArgument::Column(&sep),
        EvaluatedArgument::Column(&value),
        EvaluatedArgument::Column(&hidden),
    ];
    let good = Control::default();
    assert_eq!(
        output(
            &instance(&[source(true), source(true), source(true)])
                .evaluate(Selection::all(2), &arguments, &good)
                .unwrap()
        ),
        vec![Some("".into()), Some("A-é".into())]
    );
    assert!(!good.trace.lock().unwrap().contains(&256));
    let inactive = strings(vec![Some(&huge), Some("A")]);
    let arguments = [
        EvaluatedArgument::Column(&sep),
        EvaluatedArgument::Column(&inactive),
    ];
    let rows = [1];
    let selected = Selection::try_sparse(2, &rows).unwrap();
    let good = Control::default();
    assert_eq!(
        output(
            &instance(&[source(true), source(true)])
                .evaluate(selected, &arguments, &good)
                .unwrap()
        ),
        vec![Some("A".into())]
    );
    assert!(!good.trace.lock().unwrap().contains(&256));
}

#[test]
fn string_concat_ws_separator_null_and_cap_reject_never_mask_bad_required_children() {
    let null_sep = strings(vec![None]);
    let sep = strings(vec![Some("-")]);
    let huge = "x".repeat(1_048_577);
    let oversized = strings(vec![Some(&huge)]);
    let good = strings(vec![Some("a")]);
    let null = strings(vec![None]);
    let missing = strings(vec![]);
    let wrong = Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef;
    let failed = SelectedValues::try_new(
        Selection::all(1),
        &DataType::Utf8,
        strings(vec![None]),
        Box::from([crate::RowDataError::new(0, "required child failed")]),
    )
    .unwrap();
    for leading in [
        EvaluatedArgument::Column(&null_sep),
        EvaluatedArgument::Column(&sep),
    ] {
        for bad in [
            EvaluatedArgument::SelectedColumn(&failed),
            EvaluatedArgument::Column(&missing),
            EvaluatedArgument::Column(&wrong),
            EvaluatedArgument::Column(&null),
        ] {
            let arguments = [leading, EvaluatedArgument::Column(&oversized), bad];
            let types = [source(true), source(false), source(false)];
            let mut kernel = instance(&types);
            assert!(matches!(
                kernel.evaluate(Selection::all(1), &arguments, &Control::default()),
                Err(KernelFailure::InvalidProgram(_))
            ));
            let after = Control::default();
            let valid_arguments = [
                EvaluatedArgument::Column(&sep),
                EvaluatedArgument::Column(&good),
                EvaluatedArgument::Column(&good),
            ];
            assert_eq!(
                kernel
                    .evaluate(Selection::all(1), &valid_arguments, &after)
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
}

#[test]
fn string_concat_ws_empty_selection_and_repeated_batches_preserve_exact_protocol() {
    let sep = strings(vec![Some("-")]);
    let a = strings(vec![Some("a")]);
    let arguments = [
        EvaluatedArgument::Column(&sep),
        EvaluatedArgument::Column(&a),
    ];
    let empty = Selection::try_sparse(1, &[]).unwrap();
    let mut kernel = instance(&[source(false), source(false)]);
    assert!(
        output(
            &kernel
                .evaluate(empty, &arguments, &Control::default())
                .unwrap()
        )
        .is_empty()
    );
    for _ in 0..2 {
        assert_eq!(
            output(
                &kernel
                    .evaluate(Selection::all(1), &arguments, &Control::default())
                    .unwrap()
            ),
            vec![Some("a".into())]
        );
    }
}

#[test]
fn string_concat_ws_runtime_success_ordinary_and_real_quantum_keep_all_seven_prefixes() {
    let wide = "中".repeat(320);
    let null = strings(vec![None]);
    let separator = strings(vec![Some("-")]);
    let small = strings(vec![Some("é")]);
    let wide_array = strings(vec![Some(&wide)]);
    let wrong = Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef;
    for (a, b, c, success) in [
        (&separator, &small, &small, true),
        (&null, &null, &small, true),
        (&separator, &wide_array, &small, true),
        (&null, &small, &wrong, false),
    ] {
        let arguments = [
            EvaluatedArgument::Column(a),
            EvaluatedArgument::Column(b),
            EvaluatedArgument::Column(c),
        ];
        let types = [source(true), source(true), source(true)];
        let good = Control::default();
        assert_eq!(
            instance(&types)
                .evaluate(Selection::all(1), &arguments, &good)
                .is_ok(),
            success
        );
        let trace = good.trace.lock().unwrap().clone();
        if std::ptr::eq(b, &wide_array) {
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
                let mut kernel = instance(&types);
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(1), &arguments, &control)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                let after = Control::default();
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(1), &arguments, &after)
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}

#[test]
fn string_concat_ws_many_arguments_observe_actual_headers_without_new_arity_caps() {
    let sep = strings(vec![Some("-")]);
    let value = strings(vec![Some("a")]);
    let mut arguments = vec![EvaluatedArgument::Column(&value); 320];
    arguments[0] = EvaluatedArgument::Column(&sep);
    let types = vec![source(false); 320];
    let good = Control::default();
    let result = instance(&types)
        .evaluate(Selection::all(1), &arguments, &good)
        .unwrap();
    let expected = std::iter::repeat_n("a", 319).collect::<Vec<_>>().join("-");
    assert_eq!(output(&result), vec![Some(expected)]);
    let trace = good.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    // Sample every real work quantum plus entry/tail, avoiding a quadratic
    // callback sweep over the 320 separate argument-validation owners.
    for (at, units) in trace.iter().enumerate() {
        if at != 0 && at + 1 != trace.len() && *units != 256 {
            continue;
        }
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
            let mut kernel = instance(&types);
            assert_eq!(
                kernel
                    .evaluate(Selection::all(1), &arguments, &control)
                    .unwrap_err(),
                cause
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            let after = Control::default();
            assert_eq!(
                kernel
                    .evaluate(Selection::all(1), &arguments, &after)
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
}

#[test]
fn string_concat_ws_output_layout_is_checked_before_any_output_request() {
    assert!(output_capacity(0, 0).is_ok());
    assert!(output_capacity(320, 1_048_576).is_ok());
    assert_eq!(
        output_capacity(usize::MAX, 0),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(
        output_capacity(1, i32::MAX as usize + 1),
        Err(KernelFailure::ResourceExhausted)
    );
}

#[test]
fn string_concat_ws_complete_success_traces_and_bounded_all_seven_causes() {
    let wide = "x".repeat(1025);
    let oversized = "x".repeat(1_048_577);
    let mut checks = 0;
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for case in 0..5 {
            let mut arrays = match case {
                0 => vec![
                    strings(vec![Some("é\0"), None, Some(""), Some("x")]),
                    strings(vec![Some("尾"), Some(""), None, Some("b")]),
                ],
                1 => vec![strings(vec![Some("a")]); 320],
                2 => vec![strings(vec![Some(&wide)]), strings(vec![Some("z")])],
                3 => vec![
                    strings(vec![Some("x"), None]),
                    strings(vec![Some("y"), Some("z")]),
                ],
                4 => vec![
                    strings(vec![Some(&oversized)]),
                    strings(vec![Some("never-copied")]),
                ],
                _ => unreachable!(),
            };
            let separator = if case == 3 {
                strings(vec![None, Some("-")])
            } else {
                strings(vec![Some("-"); arrays[0].len()])
            };
            arrays.insert(0, separator);
            let types = vec![source(true); arrays.len()];
            let prepared = prepared_for_test_with_policy("concat_ws", &types, policy).unwrap();
            let make = || ScalarEvaluationInstance::instantiate(prepared.clone()).unwrap();
            let arguments = arrays
                .iter()
                .map(EvaluatedArgument::Column)
                .collect::<Vec<_>>();
            let selection = Selection::all(arrays[0].len());
            let good = Control::default();
            let result = make().evaluate(selection, &arguments, &good).unwrap();
            let actual = output(&result);
            let trace = good.trace.lock().unwrap().clone();
            println!(
                "C12-P2 trace name=concat_ws policy={policy:?} case={case} output={actual:?} units={trace:?}"
            );
            assert!(!trace.is_empty());
            for at in 0..trace.len() {
                // The byte-cap oracle retains the complete success trace, but
                // samples originating/refusal positions to avoid quadratic work.
                if case == 4 && at != 0 && at + 1 != trace.len() {
                    continue;
                }
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
                    let mut kernel = make();
                    assert_eq!(
                        kernel
                            .evaluate(selection, &arguments, &control)
                            .unwrap_err(),
                        cause
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                    let after = Control::default();
                    assert_eq!(
                        kernel.evaluate(selection, &arguments, &after).unwrap_err(),
                        KernelFailure::InstanceFailed
                    );
                    assert!(after.trace.lock().unwrap().is_empty());
                    checks += 1;
                }
            }
        }
    }
    println!(
        "C12-P2 string_concat_ws first-cause checks={checks}; cap-mask entry/tail only; other fixtures complete"
    );
}
