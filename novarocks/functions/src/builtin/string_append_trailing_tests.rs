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

use super::super::string_append_trailing_owner::{
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
use arrow_buffer::{Buffer, OffsetBuffer};
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
        panic!("append_trailing_char_if_absent never waits")
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
        .map(|value| value.map(str::to_owned))
        .collect()
}
fn expected(values: &[Option<&str>]) -> Vec<Option<String>> {
    values
        .iter()
        .map(|value| value.map(str::to_owned))
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

const NAMES: [&str; 1] = ["append_trailing_char_if_absent"];
fn sources(arity: usize) -> Vec<FunctionValueType> {
    vec![source(true); arity]
}
fn target() -> FunctionValueType {
    FunctionValueType::new(DataType::Utf8, true)
}

#[test]
fn string_append_trailing_original_byte_suffix_empty_unicode_and_null_oracles() {
    let cases = [
        (Some(""), Some("/"), Some("")),
        (Some("abc"), Some("/"), Some("abc/")),
        (Some("abc/"), Some("/"), Some("abc/")),
        (Some("é"), Some("x"), Some("éx")),
        (Some("👩‍💻"), Some("."), Some("👩‍💻.")),
        (Some("a\u{301}"), Some("x"), Some("a\u{301}x")),
        (Some("abc"), Some("é"), None),
        (Some("abc"), Some(""), None),
        (Some("abc"), Some("ab"), None),
        (Some(""), Some("é"), None),
        (Some("a"), Some("\0"), Some("a\0")),
        (Some("a\0"), Some("\0"), Some("a\0")),
        (None, Some("/"), None),
        (Some("abc"), None, None),
    ];
    let left = strings(cases.iter().map(|c| c.0).collect());
    let right = strings(cases.iter().map(|c| c.1).collect());
    let arguments = [
        EvaluatedArgument::Column(&left),
        EvaluatedArgument::Column(&right),
    ];
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let prepared = prepared_for_test_with_policy(NAMES[0], &sources(2), policy).unwrap();
        assert_eq!(prepared.contract().result_type(), &target());
        assert_eq!(prepared.contract().decimal_overflow_policy(), policy);
        assert_eq!(prepared.instance_retained_upper_bound(), 0);
        let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
        for _ in 0..2 {
            let result = kernel
                .evaluate(Selection::all(cases.len()), &arguments, &Control::default())
                .unwrap();
            assert_eq!(
                output(&result),
                expected(&cases.iter().map(|c| c.2).collect::<Vec<_>>())
            );
        }
    }
    // Even non-null source declarations keep nullable output: bad suffix is NULL.
    for left_nullable in [false, true] {
        for right_nullable in [false, true] {
            let prepared = prepared_for_test_with_policy(
                NAMES[0],
                &[source(left_nullable), source(right_nullable)],
                DecimalOverflowPolicy::ReportError,
            )
            .unwrap();
            assert_eq!(prepared.contract().result_type(), &target());
            let left = strings(vec![Some("abc")]);
            let right = strings(vec![Some("é")]);
            let arguments = [
                EvaluatedArgument::Column(&left),
                EvaluatedArgument::Column(&right),
            ];
            assert_eq!(
                output(
                    &ScalarEvaluationInstance::instantiate(prepared)
                        .unwrap()
                        .evaluate(Selection::all(1), &arguments, &Control::default())
                        .unwrap()
                ),
                vec![None]
            );
        }
    }
}

#[test]
fn string_append_trailing_sparse_slice_compact_scalar_cv_use_independent_addresses() {
    let backing = strings(vec![
        Some("unused"),
        Some("é"),
        None,
        Some("x/"),
        Some("unused"),
    ]);
    let source = backing.slice(1, 3);
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let suffix = SelectedValues::try_new(
        selection,
        &DataType::Utf8,
        strings(vec![Some("x"), Some("/")]),
        Box::default(),
    )
    .unwrap();
    let arguments = [
        EvaluatedArgument::Column(&source),
        EvaluatedArgument::SelectedColumn(&suffix),
    ];
    let mut kernel = instance(NAMES[0], 2);
    let result = kernel
        .evaluate(selection, &arguments, &Control::default())
        .unwrap();
    assert_eq!(result.selection(), selection);
    assert_eq!(output(&result), expected(&[Some("éx"), Some("x/")]));
    let suffix_pool = pool(strings(vec![Some("wrong"), None, Some("/")]));
    let suffix = suffix_pool.value(2).unwrap();
    let arguments = [
        EvaluatedArgument::Column(&source),
        EvaluatedArgument::Constant(&suffix),
    ];
    assert_eq!(
        output(
            &kernel
                .evaluate(selection, &arguments, &Control::default())
                .unwrap()
        ),
        expected(&[Some("é/"), Some("x/")])
    );
    assert_eq!(suffix.ordinal(), 2);
    assert!(Arc::ptr_eq(suffix.pool().array(), suffix_pool.array()));
    let source_pool = pool(strings(vec![Some("wrong"), Some(""), None]));
    let source = source_pool.value(1).unwrap();
    let suffixes_backing = strings(vec![
        Some("unused"),
        Some("x"),
        None,
        Some("é"),
        Some("unused"),
    ]);
    let suffixes = suffixes_backing.slice(1, 3);
    let arguments = [
        EvaluatedArgument::Constant(&source),
        EvaluatedArgument::Column(&suffixes),
    ];
    assert_eq!(
        output(
            &kernel
                .evaluate(selection, &arguments, &Control::default())
                .unwrap()
        ),
        expected(&[Some(""), None])
    );
    assert_eq!(source.ordinal(), 1);
    assert_eq!(
        source.field().metadata()["source-note"],
        "kept selected backing"
    );
    let source = strings(vec![Some("hello")]);
    let suffix = strings(vec![Some("!")]);
    let arguments = [
        EvaluatedArgument::Scalar(&source),
        EvaluatedArgument::Scalar(&suffix),
    ];
    assert_eq!(
        output(
            &kernel
                .evaluate(selection, &arguments, &Control::default())
                .unwrap()
        ),
        expected(&[Some("hello!"), Some("hello!")])
    );
}

#[test]
fn string_append_trailing_null_hidden_and_inactive_payloads_skip_copy_and_no_business_cap() {
    let huge = "x".repeat(320 * 1024);
    let mut bytes = huge.as_bytes().to_vec();
    bytes.extend_from_slice(b"b");
    let mut validity = BooleanBufferBuilder::new(2);
    validity.append(false);
    validity.append(true);
    let source = Arc::new(StringArray::new(
        OffsetBuffer::new(vec![0i32, huge.len() as i32, (huge.len() + 1) as i32].into()),
        Buffer::from(bytes),
        Some(NullBuffer::new(validity.finish())),
    )) as ArrayRef;
    let suffix = strings(vec![Some("/"), Some("/")]);
    let arguments = [
        EvaluatedArgument::Column(&source),
        EvaluatedArgument::Column(&suffix),
    ];
    let control = Control::default();
    let mut kernel = instance(NAMES[0], 2);
    assert_eq!(
        output(
            &kernel
                .evaluate(Selection::all(2), &arguments, &control)
                .unwrap()
        ),
        expected(&[None, Some("b/")])
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
    for null_index in 0..2 {
        let left = strings(vec![Some(&huge)]);
        let right = strings(vec![Some(&huge)]);
        let null = strings(vec![None]);
        let arguments = [
            EvaluatedArgument::Column(if null_index == 0 { &null } else { &left }),
            EvaluatedArgument::Column(if null_index == 1 { &null } else { &right }),
        ];
        let control = Control::default();
        assert_eq!(
            output(
                &kernel
                    .evaluate(Selection::all(1), &arguments, &control)
                    .unwrap()
            ),
            vec![None]
        );
        assert!(!control.trace.lock().unwrap().contains(&256));
    }
    let left = strings(vec![Some(&huge), Some("b")]);
    let right = strings(vec![Some(&huge), Some("/")]);
    let arguments = [
        EvaluatedArgument::Column(&left),
        EvaluatedArgument::Column(&right),
    ];
    let rows = [1];
    let control = Control::default();
    assert_eq!(
        output(
            &kernel
                .evaluate(
                    Selection::try_sparse(2, &rows).unwrap(),
                    &arguments,
                    &control
                )
                .unwrap()
        ),
        expected(&[Some("b/")])
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
    let empty = [];
    assert!(
        output(
            &kernel
                .evaluate(
                    Selection::try_sparse(2, &empty).unwrap(),
                    &arguments,
                    &Control::default()
                )
                .unwrap()
        )
        .is_empty()
    );
    // The original function has no CONCAT-style 1MiB successful-NULL cap.
    let over_mib = "a".repeat(1_048_577);
    let left = strings(vec![Some(&over_mib)]);
    let right = strings(vec![Some("/")]);
    let arguments = [
        EvaluatedArgument::Column(&left),
        EvaluatedArgument::Column(&right),
    ];
    let result = kernel
        .evaluate(Selection::all(1), &arguments, &Control::default())
        .unwrap();
    let values = result
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(!values.is_null(0));
    assert_eq!(values.value(0).len(), over_mib.len() + 1);
    assert!(values.value(0).ends_with('/'));
}

#[test]
fn string_append_trailing_required_children_and_canonical_shape_errors_poison_once() {
    let selection = Selection::all(1);
    let left = strings(vec![Some("abc")]);
    let right = strings(vec![Some("é")]);
    for failed in 0..2 {
        let child = SelectedValues::try_new(
            selection,
            &DataType::Utf8,
            strings(vec![None]),
            Box::from([crate::RowDataError::new(0, "required child failed")]),
        )
        .unwrap();
        let mut arguments = [
            EvaluatedArgument::Column(&left),
            EvaluatedArgument::Column(&right),
        ];
        arguments[failed] = EvaluatedArgument::SelectedColumn(&child);
        let mut kernel = instance(NAMES[0], 2);
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
    let wrong = Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef;
    let arguments = [
        EvaluatedArgument::Column(&wrong),
        EvaluatedArgument::Column(&right),
    ];
    assert!(matches!(
        instance(NAMES[0], 2).evaluate(selection, &arguments, &Control::default()),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
    let null = strings(vec![None]);
    let arguments = [
        EvaluatedArgument::Column(&null),
        EvaluatedArgument::Column(&right),
    ];
    let prepared = prepared_for_test_with_policy(
        NAMES[0],
        &[source(false), source(true)],
        DecimalOverflowPolicy::OutputNull,
    )
    .unwrap();
    assert!(matches!(
        ScalarEvaluationInstance::instantiate(prepared)
            .unwrap()
            .evaluate(selection, &arguments, &Control::default()),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
}

#[test]
fn string_append_trailing_output_layout_is_checked_before_reserve() {
    assert_eq!(
        output_capacity(usize::MAX, 0),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(
        output_capacity(1, usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(
        output_capacity(1, i32::MAX as usize + 1),
        Err(KernelFailure::ResourceExhausted)
    );
    assert!(output_capacity(0, 0).is_ok());
    assert!(output_capacity(320, 320).is_ok());
}

#[test]
fn string_append_trailing_all_seven_runtime_causes_preserve_prefix_latch_and_real_quantum() {
    use crate::kernel_control::KernelDiagnostic;
    let long = "x".repeat(320);
    for (left, right, wide, wrong) in [
        (
            strings(vec![Some("abc"), None]),
            strings(vec![Some("/"), Some("x")]),
            false,
            false,
        ),
        (
            Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef,
            strings(vec![Some("x")]),
            false,
            true,
        ),
        (
            strings(vec![Some(&long)]),
            strings(vec![Some("/")]),
            true,
            false,
        ),
        (
            strings(vec![Some("x"); 320]),
            strings(vec![Some("/"); 320]),
            true,
            false,
        ),
    ] {
        let arguments = [
            EvaluatedArgument::Column(&left),
            EvaluatedArgument::Column(&right),
        ];
        let good = Control::default();
        let mut kernel = instance(NAMES[0], 2);
        let result = kernel.evaluate(Selection::all(left.len()), &arguments, &good);
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
                let mut kernel = instance(NAMES[0], 2);
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(left.len()), &arguments, &control)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                let after = Control::default();
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(left.len()), &arguments, &after)
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}

#[test]
fn string_append_trailing_owner_preserves_exact_fresh_frozen_arc_effects_and_rejects_stale_sources()
{
    for name in NAMES {
        let arity = 2;
        assert_eq!(operation(name), Some(()));
        assert!(operation("split_part").is_none());
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
        for count in [0, 3] {
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
            FunctionResultType::Scalar(FunctionValueType::new(DataType::Utf8, false));
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
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
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

#[test]
fn string_append_trailing_compile_success_and_ordinary_tail_preserve_all_callback_causes() {
    for ty in [source(true), FunctionValueType::new(DataType::Binary, true)] {
        let good = CompileControl::default();
        let success = ty.data_type() == &DataType::Utf8;
        assert_eq!(
            prepared_for_test_with_control(
                "append_trailing_char_if_absent",
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
            ] {
                let control = CompileControl {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let actual = prepared_for_test_with_control(
                    "append_trailing_char_if_absent",
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
fn string_append_trailing_constant_null_and_invalid_suffix_are_deferred_until_selected() {
    let owner = owner_for_test(NAMES[0]);
    let constants = pool(strings(vec![Some("unused"), None, Some("é")]));
    for (constant_index, ordinal) in [(0, 1), (1, 2)] {
        let constant = constants.value(ordinal).unwrap();
        let types = sources(2);
        let args: Vec<_> = types
            .into_iter()
            .enumerate()
            .map(|(index, value_type)| FunctionArgument::Value {
                value_type,
                constant: (index == constant_index).then(|| constant.clone()),
            })
            .collect();
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
        let source = strings(vec![Some("abc")]);
        let suffix = strings(vec![Some("/")]);
        let mut evaluated = [
            EvaluatedArgument::Scalar(&source),
            EvaluatedArgument::Scalar(&suffix),
        ];
        evaluated[constant_index] = EvaluatedArgument::Constant(&constant);
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
        assert_eq!(constant.ordinal(), ordinal);
        assert!(Arc::ptr_eq(constant.pool().array(), constants.array()));
    }
}
