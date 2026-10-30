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

use super::super::string_translate_owner::{
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
        panic!("translate never waits")
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
fn instance(name: &str) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_policy(name, &sources(), DecimalOverflowPolicy::OutputNull).unwrap(),
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

const NAMES: [&str; 1] = ["translate"];
fn sources() -> Vec<FunctionValueType> {
    vec![source(true); 3]
}

#[test]
fn string_translate_owner_preserves_exact_fresh_frozen_arc_effects_and_rejects_stale_sources() {
    for name in NAMES {
        let arity = 3;
        assert_eq!(operation(name), Some(StringTranslateOp::Translate));
        assert!(operation("substring_index").is_none());
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
        assert!(std::ptr::eq(
            frozen.prepared().contract().selected(),
            selected.as_ref()
        ));
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
        for count in [0, 1, 2, 4] {
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
                    &[source, self::source(true), self::source(true)],
                    DecimalOverflowPolicy::OutputNull
                )
                .is_err()
            );
        }
    }
}

#[test]
fn string_translate_compile_success_and_ordinary_tail_preserve_all_callback_causes() {
    for ty in [source(true), FunctionValueType::new(DataType::Binary, true)] {
        let good = CompileControl::default();
        let success = ty.data_type() == &DataType::Utf8;
        assert_eq!(
            prepared_for_test_with_control(
                "translate",
                &[ty.clone(), source(true), source(true)],
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
                    "translate",
                    &[ty.clone(), source(true), source(true)],
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
fn string_translate_actual_profile_preserves_independent_first_duplicate_delete_and_unicode_index_oracles()
 {
    let cases = [
        ("ababa", "aba", "XYZ", "XYXYX"),
        ("aaa", "aa", "XY", "XXX"),
        ("aaab", "baa", "X", "X"),
        ("ab", "ab", "ba", "ba"),
        ("abc", "ac", "X", "Xb"),
        ("", "", ":", ""),
        ("é中", "", ":", "é中"),
        ("é中é", "é中é", "甲乙丙", "甲乙甲"),
        ("ééa", "aéé", "X", "X"),
        ("éa", "éa", "中", "中"),
        ("a\u{301}", "a\u{301}", "X", "X"),
        ("👩‍💻", "👩‍💻", "xy", "xy"),
        ("a\0b\0", "\0b", "Z", "aZZ"),
        ("Σσς", "Σσ", "σΣ", "σΣς"),
        ("éé", "a", "X", "éé"),
        ("abc", "", "unusedé", "abc"),
    ];
    let text = strings(cases.iter().map(|(s, _, _, _)| Some(*s)).collect());
    let needles = strings(cases.iter().map(|(_, n, _, _)| Some(*n)).collect());
    let replacements = strings(cases.iter().map(|(_, _, r, _)| Some(*r)).collect());
    let args = [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::Column(&needles),
        EvaluatedArgument::Column(&replacements),
    ];
    for mask in 0..8 {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let mut types = sources();
            for (i, ty) in types.iter_mut().enumerate() {
                *ty = ty
                    .with_nullable(mask & (1 << i) != 0)
                    .expect("valid function nullability declaration");
            }
            let prepared = prepared_for_test_with_policy("translate", &types, policy).unwrap();
            assert_eq!(prepared.contract().result_type(), &source(true));
            assert_eq!(prepared.contract().decimal_overflow_policy(), policy);
            let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
            let result = kernel
                .evaluate(Selection::all(text.len()), &args, &Control::default())
                .unwrap();
            assert_eq!(
                output(&result),
                cases
                    .iter()
                    .map(|(_, _, _, expected)| Some(expected.to_string()))
                    .collect::<Vec<_>>()
            );
        }
    }
    for i in 0..3 {
        for ty in [
            FunctionValueType::new(DataType::LargeUtf8, true),
            FunctionValueType::new(DataType::Binary, true),
            FunctionValueType::new(DataType::Int64, true),
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
        ] {
            let mut wrong = sources();
            wrong[i] = ty;
            assert!(
                prepared_for_test_with_policy(
                    "translate",
                    &wrong,
                    DecimalOverflowPolicy::OutputNull
                )
                .is_err()
            );
        }
    }
    for alias in ["str_translate", "translate_all"] {
        assert!(operation(alias).is_none());
    }
}

#[test]
fn string_translate_per_argument_slice_compact_scalar_and_constant_ordinal_addresses_are_exact() {
    let text_backing = strings(vec![
        Some("unused"),
        Some("ababa"),
        None,
        Some("aéa"),
        Some("unused"),
    ]);
    let needle_backing = strings(vec![
        Some("unused"),
        Some("aba"),
        None,
        Some("a"),
        Some("unused"),
    ]);
    let replacement_backing = strings(vec![
        Some("unused"),
        Some("X"),
        None,
        Some("中"),
        Some("unused"),
    ]);
    let text = text_backing.slice(1, 3);
    let needle = needle_backing.slice(1, 3);
    let replacement = replacement_backing.slice(1, 3);
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Utf8,
        strings(vec![Some("aba"), Some("a")]),
        Box::default(),
    )
    .unwrap();
    for argument in [
        EvaluatedArgument::Column(&needle),
        EvaluatedArgument::SelectedColumn(&compact),
    ] {
        let mut kernel = instance("translate");
        let arguments = [
            EvaluatedArgument::Column(&text),
            argument,
            EvaluatedArgument::Column(&replacement),
        ];
        let result = kernel
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert_eq!(result.selection(), selection);
        assert_eq!(
            output(&result),
            vec![Some("XXX".into()), Some("中é中".into())]
        );
    }
    let source_pool = pool(strings(vec![None, Some("unused"), Some("aa")]));
    let needle_pool = pool(strings(vec![None, Some("unused"), Some("a")]));
    let replacement_pool = pool(strings(vec![None, Some("unused"), Some("é")]));
    let source_cv = source_pool.value(2).unwrap();
    let needle_cv = needle_pool.value(2).unwrap();
    let replacement_cv = replacement_pool.value(2).unwrap();
    let scalar_source = strings(vec![Some("aa")]);
    let scalar_needle = strings(vec![Some("a")]);
    for (args, expected) in [
        (
            [
                EvaluatedArgument::Constant(&source_cv),
                EvaluatedArgument::Scalar(&scalar_needle),
                EvaluatedArgument::Column(&replacement),
            ],
            vec![Some("XX".to_owned()), Some("中中".to_owned())],
        ),
        (
            [
                EvaluatedArgument::Scalar(&scalar_source),
                EvaluatedArgument::Constant(&needle_cv),
                EvaluatedArgument::Constant(&replacement_cv),
            ],
            vec![Some("éé".to_owned()); 2],
        ),
        (
            [
                EvaluatedArgument::Constant(&source_cv),
                EvaluatedArgument::Constant(&needle_cv),
                EvaluatedArgument::Constant(&replacement_cv),
            ],
            vec![Some("éé".to_owned()); 2],
        ),
    ] {
        let mut kernel = instance("translate");
        assert_eq!(
            output(
                &kernel
                    .evaluate(selection, &args, &Control::default())
                    .unwrap()
            ),
            expected
        );
    }
    assert!(Arc::ptr_eq(source_cv.pool().array(), source_pool.array()));
    assert!(Arc::ptr_eq(needle_cv.pool().array(), needle_pool.array()));
    assert!(Arc::ptr_eq(
        replacement_cv.pool().array(),
        replacement_pool.array()
    ));
}

#[test]
fn string_translate_strict_null_and_inactive_payload_never_search_or_copy_hidden_spans() {
    let huge = "a".repeat(320 * 1024);
    let mut bytes = huge.as_bytes().to_vec();
    bytes.push(b'a');
    let mut validity = BooleanBufferBuilder::new(2);
    validity.append(false);
    validity.append(true);
    let hidden = Arc::new(StringArray::new(
        OffsetBuffer::new(vec![0i32, huge.len() as i32, (huge.len() + 1) as i32].into()),
        Buffer::from(bytes),
        Some(NullBuffer::new(validity.finish())),
    )) as ArrayRef;
    let ordinary = strings(vec![Some("a"), Some("a")]);
    for position in 0..3 {
        let args = std::array::from_fn::<_, 3, _>(|i| {
            EvaluatedArgument::Column(if i == position { &hidden } else { &ordinary })
        });
        let control = Control::default();
        let mut kernel = instance("translate");
        let result = kernel.evaluate(Selection::all(2), &args, &control).unwrap();
        assert_eq!(output(&result), vec![None, Some("a".into())]);
        assert!(
            control
                .trace
                .lock()
                .unwrap()
                .iter()
                .all(|units| *units < 256)
        );
        let mut types = sources();
        types[position] = types[position]
            .with_nullable(false)
            .expect("valid function nullability declaration");
        let prepared =
            prepared_for_test_with_policy("translate", &types, DecimalOverflowPolicy::OutputNull)
                .unwrap();
        let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
        assert!(matches!(
            kernel.evaluate(Selection::all(2), &args, &Control::default()),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
    let inactive = strings(vec![Some(&huge), Some("a")]);
    let rows = [1];
    let selection = Selection::try_sparse(2, &rows).unwrap();
    for position in 0..3 {
        let args = std::array::from_fn::<_, 3, _>(|i| {
            EvaluatedArgument::Column(if i == position { &inactive } else { &ordinary })
        });
        let control = Control::default();
        let mut kernel = instance("translate");
        assert_eq!(
            output(&kernel.evaluate(selection, &args, &control).unwrap()),
            vec![Some("a".into())]
        );
        assert!(
            control
                .trace
                .lock()
                .unwrap()
                .iter()
                .all(|units| *units < 256)
        );
    }
}

#[test]
fn string_translate_empty_and_required_child_errors_do_not_bypass_eager_argument_validation() {
    let text = strings(vec![Some("abc")]);
    let needle = strings(vec![Some("")]);
    let replacement = strings(vec![Some("x")]);
    let args = [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::Column(&needle),
        EvaluatedArgument::Column(&replacement),
    ];
    let empty_rows = [];
    let selection = Selection::try_sparse(1, &empty_rows).unwrap();
    let mut kernel = instance("translate");
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
        vec![Some("abc".into())]
    );
    let empty_column = strings(vec![]);
    let null_scalar = strings(vec![None]);
    let bad_addresses = [
        EvaluatedArgument::Scalar(&null_scalar),
        EvaluatedArgument::Column(&needle),
        EvaluatedArgument::Column(&empty_column),
    ];
    let mut kernel = instance("translate");
    assert!(matches!(
        kernel.evaluate(Selection::all(1), &bad_addresses, &Control::default()),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
    let failed = SelectedValues::try_new(
        Selection::all(1),
        &DataType::Utf8,
        strings(vec![None]),
        Box::from([crate::RowDataError::new(0, "required child failed")]),
    )
    .unwrap();
    for position in 0..3 {
        let arguments = std::array::from_fn::<_, 3, _>(|i| {
            if i == position {
                EvaluatedArgument::SelectedColumn(&failed)
            } else {
                args[i]
            }
        });
        let mut kernel = instance("translate");
        assert!(matches!(
            kernel.evaluate(Selection::all(1), &arguments, &Control::default()),
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
    }
}

#[test]
fn string_translate_runtime_success_ordinary_tail_and_real_256_preserve_every_seven_cause_prefix() {
    let long = "é".repeat(320);
    let mapping = (0..320)
        .map(|i| char::from_u32(0x400 + i).unwrap())
        .collect::<String>();
    for (text, needle, replacement, good_shape) in [
        (
            strings(vec![Some("éaa")]),
            strings(vec![Some("a")]),
            strings(vec![Some("x")]),
            true,
        ),
        (
            strings(vec![Some(&long)]),
            strings(vec![Some("missing")]),
            strings(vec![Some("x")]),
            true,
        ),
        (
            strings(vec![Some("é")]),
            strings(vec![Some(&mapping)]),
            strings(vec![Some("x")]),
            true,
        ),
        (
            strings(vec![Some("a")]),
            Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef,
            strings(vec![Some("x")]),
            false,
        ),
    ] {
        let args = [
            EvaluatedArgument::Column(&text),
            EvaluatedArgument::Column(&needle),
            EvaluatedArgument::Column(&replacement),
        ];
        let good = Control::default();
        let mut kernel = instance("translate");
        assert_eq!(
            kernel.evaluate(Selection::all(1), &args, &good).is_ok(),
            good_shape
        );
        let trace = good.trace.lock().unwrap().clone();
        if text
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .len()
            > 256
        {
            assert!(trace.contains(&256));
        }
        assert!(!trace.is_empty());
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
                let mut kernel = instance("translate");
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

#[test]
fn string_translate_measured_output_layout_and_byte_extent_refuse_before_requests() {
    assert!(output_capacity(0, 0).is_ok());
    assert_eq!(
        output_capacity(usize::MAX, 0),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(
        output_capacity(1, i32::MAX as usize + 1),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(
        span_length(usize::MAX, 1),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(span_length(3, 5).unwrap(), 8);
    assert!(mapping_capacity(0).is_ok());
    assert_eq!(
        mapping_capacity(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    );
    let representable = (isize::MAX as usize) / std::mem::size_of::<MappingEntry>();
    assert!(mapping_capacity(representable).is_ok());
    assert_eq!(
        mapping_capacity(representable + 1),
        Err(KernelFailure::ResourceExhausted)
    );
}

#[test]
fn string_translate_original_ascii_uncapped_unicode_cap_and_partitioning_are_exact() {
    let huge = "a".repeat(1024 * 1024 + 1);
    let unicode_source = "é".repeat(512 * 1024 + 1);
    let cases = [
        (huge.as_str(), "", "", Some(huge.as_str())),
        (
            unicode_source.as_str(),
            "a",
            "x",
            Some(unicode_source.as_str()),
        ),
        (huge.as_str(), "", "unusedé", None),
        (huge.as_str(), "unusedé", "", None),
        (huge.as_str(), "aé", "", Some("")),
        ("ababa", "aba", "X", Some("XXX")),
    ];
    let text = strings(cases.iter().map(|c| Some(c.0)).collect());
    let from = strings(cases.iter().map(|c| Some(c.1)).collect());
    let to = strings(cases.iter().map(|c| Some(c.2)).collect());
    let args = [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::Column(&from),
        EvaluatedArgument::Column(&to),
    ];
    let mut kernel = instance("translate");
    let result = kernel
        .evaluate(Selection::all(cases.len()), &args, &Control::default())
        .unwrap();
    assert_eq!(
        output(&result),
        cases
            .iter()
            .map(|c| c.3.map(str::to_owned))
            .collect::<Vec<_>>()
    );
    for (row, case) in cases.iter().enumerate() {
        let rows = [row];
        let single = kernel
            .evaluate(
                Selection::try_sparse(cases.len(), &rows).unwrap(),
                &args,
                &Control::default(),
            )
            .unwrap();
        assert_eq!(output(&single), vec![case.3.map(str::to_owned)]);
    }
    let exact = "a".repeat(1024 * 1024);
    let exact = strings(vec![Some(&exact)]);
    let from = strings(vec![Some("")]);
    let to = strings(vec![Some("é")]);
    let args = [
        EvaluatedArgument::Column(&exact),
        EvaluatedArgument::Column(&from),
        EvaluatedArgument::Column(&to),
    ];
    let result = kernel
        .evaluate(Selection::all(1), &args, &Control::default())
        .unwrap();
    assert_eq!(
        result
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .len(),
        1024 * 1024
    );
}
