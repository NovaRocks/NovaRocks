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

use super::super::string_concat_owner::{
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
        panic!("string concatenation never waits")
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
        prepared_for_test_with_policy("concat", sources, DecimalOverflowPolicy::OutputNull)
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

#[test]
fn string_concat_variadic_unicode_null_and_policy_oracles_preserve_actual_minimum_arity() {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for nullable in [false, true] {
            for count in [1, 2, 4] {
                let array = strings(vec![Some("é👩‍💻\0"), Some("")]);
                let types = vec![source(nullable); count];
                let prepared = prepared_for_test_with_policy("concat", &types, policy).unwrap();
                assert_eq!(prepared.contract().result_type(), &source(true));
                assert_eq!(prepared.contract().decimal_overflow_policy(), policy);
                let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
                let arguments = vec![EvaluatedArgument::Column(&array); count];
                let result = kernel
                    .evaluate(Selection::all(2), &arguments, &Control::default())
                    .unwrap();
                let expected = match count {
                    1 => "é👩‍💻\0",
                    2 => "é👩‍💻\0é👩‍💻\0",
                    4 => "é👩‍💻\0é👩‍💻\0é👩‍💻\0é👩‍💻\0",
                    _ => unreachable!(),
                };
                assert_eq!(
                    output(&result),
                    vec![Some(expected.into()), Some("".into())]
                );
            }
        }
    }
    let a = strings(vec![Some("A"), None, Some("A"), Some(""), None]);
    let b = strings(vec![Some("中"), Some("B"), None, Some(""), None]);
    let c = strings(vec![
        Some("a\u{301}"),
        Some("C"),
        Some("C"),
        Some("!"),
        None,
    ]);
    let arguments = [
        EvaluatedArgument::Column(&a),
        EvaluatedArgument::Column(&b),
        EvaluatedArgument::Column(&c),
    ];
    let mut kernel = instance(&[source(true), source(true), source(true)]);
    let result = kernel
        .evaluate(Selection::all(5), &arguments, &Control::default())
        .unwrap();
    assert_eq!(
        output(&result),
        vec![
            Some("A中a\u{301}".into()),
            None,
            None,
            Some("!".into()),
            None
        ]
    );
    assert!(
        prepared_for_test_with_policy("concat", &[], DecimalOverflowPolicy::OutputNull).is_err()
    );
    for other in ["concat_ws", "format", "elt", "str_concat"] {
        assert!(operation(other).is_none());
    }
}

#[test]
fn string_concat_independent_sliced_compact_scalar_and_constant_addresses_keep_original_ordinal() {
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
        strings(vec![Some("é"), Some("ß")]),
        Box::default(),
    )
    .unwrap();
    let scalar = strings(vec![Some("👩‍💻")]);
    let constants = pool(strings(vec![Some("unused"), None, Some("\0!")]));
    let constant = constants.value(2).unwrap();
    let arguments = [
        EvaluatedArgument::Column(&sliced),
        EvaluatedArgument::SelectedColumn(&compact),
        EvaluatedArgument::Scalar(&scalar),
        EvaluatedArgument::Constant(&constant),
    ];
    let mut kernel = instance(&[source(true), source(true), source(false), source(true)]);
    for _ in 0..2 {
        let result = kernel
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(result.selection(), selection);
        assert_eq!(
            output(&result),
            vec![Some("Aé👩‍💻\0!".into()), Some("中ß👩‍💻\0!".into())]
        );
    }
    assert_eq!(constant.ordinal(), 2);
    assert!(Arc::ptr_eq(constant.pool().array(), constants.array()));
    assert_eq!(
        constant.field().metadata()["source-note"],
        "kept selected backing"
    );
    let null_constant = constants.value(1).unwrap();
    let arguments = [
        EvaluatedArgument::Column(&sliced),
        EvaluatedArgument::Constant(&null_constant),
    ];
    let mut null_kernel = instance(&[source(true), source(true)]);
    let result = null_kernel
        .evaluate(selection, &arguments, &Control::default())
        .unwrap();
    assert_eq!(output(&result), vec![None, None]);
}

#[test]
fn string_concat_original_one_mib_row_boundary_is_successful_null_not_resource_failure() {
    // The expected row limit is authored from the legacy 1_048_576 rule.
    let at = "a".repeat(1_048_576);
    let below = "b".repeat(1_048_575);
    let a = strings(vec![Some(&at), Some(&below), Some(&at)]);
    let b = strings(vec![Some(""), Some("é"), Some("x")]);
    let arguments = [EvaluatedArgument::Column(&a), EvaluatedArgument::Column(&b)];
    let mut kernel = instance(&[source(false), source(false)]);
    let result = kernel
        .evaluate(Selection::all(3), &arguments, &Control::default())
        .unwrap();
    assert!(result.errors().is_empty());
    let values = result
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), at);
    assert!(!values.is_null(0));
    assert!(values.is_null(1));
    assert!(values.is_null(2));
}

#[test]
fn string_concat_hidden_null_and_inactive_payloads_do_not_create_copy_work() {
    let huge = "x".repeat(320 * 1024);
    let mut bytes = huge.as_bytes().to_vec();
    bytes.push(b'A');
    let mut validity = BooleanBufferBuilder::new(2);
    validity.append(false);
    validity.append(true);
    let hidden = Arc::new(StringArray::new(
        OffsetBuffer::new(vec![0i32, huge.len() as i32, (huge.len() + 1) as i32].into()),
        Buffer::from(bytes),
        Some(NullBuffer::new(validity.finish())),
    )) as ArrayRef;
    let later = strings(vec![Some(&huge), Some("é")]);
    let arguments = [
        EvaluatedArgument::Column(&hidden),
        EvaluatedArgument::Column(&later),
    ];
    let good = Control::default();
    let mut kernel = instance(&[source(true), source(false)]);
    let result = kernel
        .evaluate(Selection::all(2), &arguments, &good)
        .unwrap();
    assert_eq!(output(&result), vec![None, Some("Aé".into())]);
    assert!(!good.trace.lock().unwrap().contains(&256));
    let rows = [1];
    let selection = Selection::try_sparse(2, &rows).unwrap();
    let inactive = strings(vec![Some(&huge), Some("A")]);
    let arguments = [
        EvaluatedArgument::Column(&inactive),
        EvaluatedArgument::Column(&later),
    ];
    let good = Control::default();
    let result = kernel.evaluate(selection, &arguments, &good).unwrap();
    assert_eq!(output(&result), vec![Some("Aé".into())]);
    assert!(!good.trace.lock().unwrap().contains(&256));
}

#[test]
fn string_concat_empty_child_error_and_wrong_shapes_never_publish_or_retry() {
    let array = strings(vec![Some("A")]);
    let arguments = [EvaluatedArgument::Column(&array)];
    let rows = [];
    let empty = Selection::try_sparse(1, &rows).unwrap();
    let mut kernel = instance(&[source(false)]);
    let result = kernel
        .evaluate(empty, &arguments, &Control::default())
        .unwrap();
    assert!(output(&result).is_empty());
    assert_eq!(
        output(
            &kernel
                .evaluate(Selection::all(1), &arguments, &Control::default())
                .unwrap()
        ),
        vec![Some("A".into())]
    );
    let selection = Selection::all(1);
    let child = SelectedValues::try_new(
        selection,
        &DataType::Utf8,
        strings(vec![None]),
        Box::from([crate::RowDataError::new(0, "required child failed")]),
    )
    .unwrap();
    let good_child = strings(vec![Some("must not mask required failure")]);
    let arguments = [
        EvaluatedArgument::Column(&good_child),
        EvaluatedArgument::SelectedColumn(&child),
    ];
    let mut kernel = instance(&[source(false), source(true)]);
    assert!(matches!(
        kernel.evaluate(selection, &arguments, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let after = Control::default();
    assert_eq!(
        kernel.evaluate(selection, &arguments, &after).unwrap_err(),
        KernelFailure::InstanceFailed
    );
    assert!(after.trace.lock().unwrap().is_empty());
    let wrong = Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef;
    let arguments = [EvaluatedArgument::Column(&wrong)];
    let mut kernel = instance(&[source(false)]);
    assert!(matches!(
        kernel.evaluate(selection, &arguments, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let hidden_null = strings(vec![None]);
    let arguments = [EvaluatedArgument::Column(&hidden_null)];
    let mut kernel = instance(&[source(false)]);
    assert!(matches!(
        kernel.evaluate(selection, &arguments, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
}

#[test]
fn string_concat_output_layout_is_checked_before_any_output_request() {
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
fn string_concat_runtime_small_and_argument_and_copy_quantum_keep_first_cause_and_latch() {
    for (a, count, sample) in [
        (strings(vec![Some("é"), None, Some("ß")]), 2, false),
        (strings(vec![Some("a")]), 320, true),
        (strings(vec![Some(&"中".repeat(320))]), 2, true),
    ] {
        let arguments = vec![EvaluatedArgument::Column(&a); count];
        let sources = vec![source(true); count];
        let good = Control::default();
        let mut kernel = instance(&sources);
        kernel
            .evaluate(Selection::all(a.len()), &arguments, &good)
            .unwrap();
        let trace = good.trace.lock().unwrap().clone();
        if sample {
            assert!(trace.contains(&256));
        }
        for (at, units) in trace.iter().enumerate() {
            if sample && at != 0 && at + 1 != trace.len() && *units != 256 {
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
                let mut kernel = instance(&sources);
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(a.len()), &arguments, &control)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                let after = Control::default();
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(a.len()), &arguments, &after)
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}
#[test]
fn string_concat_owner_preserves_exact_fresh_frozen_arc_effects_and_rejects_stale_sources() {
    for name in ["concat"] {
        let owner = owner_for_test(name);
        let arguments = [FunctionArgument::Value {
            value_type: source(false),
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
        assert_eq!(
            selected.result_type,
            FunctionResultType::Scalar(source(true))
        );
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
            "builtin.scalar/concat/v1"
        );
        assert_eq!(owner.implementation_declarations().len(), 1);
        assert_eq!(
            owner.implementation_declarations()[0]
                .implementation
                .as_str(),
            "builtin.scalar/concat/selected-v1"
        );
        assert_eq!(
            owner.binding_declaration().overloads()[0].effects.as_ref(),
            Some(&effects())
        );
        let pattern_arguments = [];
        assert!(
            owner
                .resolve(
                    FunctionBindingRequest {
                        arguments: &pattern_arguments,
                        logical_argument_count: 0,
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
        // The previously declared one-spec variadic profile licensed zero
        // arguments. Its identity cannot be reused for the corrected prefix.
        let old_signature = super::super::signature::Signature::variadic(
            vec![super::super::signature::TypeSpec::Utf8],
            super::super::signature::TypeSpec::Utf8,
        )
        .canonical();
        let (old_declaration, _) = super::super::catalogue::scalar_definition_parts(
            "concat",
            &[old_signature],
            crate::FunctionKind::Scalar,
        )
        .unwrap();
        let mut old_selected = (*selected).clone();
        old_selected.overload = old_declaration.overloads()[0].identity.clone();
        assert_ne!(old_selected.overload, selected.overload);
        assert!(
            owner
                .validate_selected(&old_selected, request, crate::binding_test_control())
                .is_err()
        );
        assert_eq!(fresh.prepared().instance_retained_upper_bound(), 0);
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
                prepared_for_test_with_policy(name, &[source], DecimalOverflowPolicy::OutputNull)
                    .is_err()
            );
        }
    }
}

#[test]
fn string_concat_compile_success_and_ordinary_tail_preserve_all_callback_causes() {
    for ty in [source(true), FunctionValueType::new(DataType::Binary, true)] {
        let good = CompileControl::default();
        let success = ty.data_type == DataType::Utf8;
        assert_eq!(
            prepared_for_test_with_control(
                "concat",
                std::slice::from_ref(&ty),
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
                    "concat",
                    std::slice::from_ref(&ty),
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
fn string_concat_complete_success_traces_and_bounded_all_seven_causes() {
    let wide = "x".repeat(1025);
    let oversized = "x".repeat(1_048_577);
    let mut checks = 0;
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for case in 0..5 {
            let arrays = match case {
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
            let types = vec![source(true); arrays.len()];
            let prepared = prepared_for_test_with_policy("concat", &types, policy).unwrap();
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
                "C12-P2 trace name=concat policy={policy:?} case={case} output={actual:?} units={trace:?}"
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
        "C12-P2 string_concat first-cause checks={checks}; cap-mask entry/tail only; other fixtures complete"
    );
}
