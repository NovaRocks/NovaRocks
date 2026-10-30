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

use super::super::string_pad_owner::{
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
        panic!("lpad/rpad never waits")
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

const NAMES: [&str; 2] = ["lpad", "rpad"];
fn sources() -> Vec<FunctionValueType> {
    vec![
        source(true),
        FunctionValueType::new(DataType::Int64, true),
        source(true),
    ]
}
fn integers(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}
fn expected(name: &str, left: &str, right: &str) -> String {
    if name == "lpad" {
        left.into()
    } else {
        right.into()
    }
}

#[test]
fn string_pad_owner_preserves_exact_fresh_frozen_arc_effects_and_rejects_stale_sources() {
    for name in NAMES {
        let arity = 3;
        assert_eq!(
            operation(name),
            Some(if name == "lpad" {
                StringPadOp::Left
            } else {
                StringPadOp::Right
            })
        );
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
                    &[
                        source,
                        FunctionValueType::new(DataType::Int64, true),
                        self::source(true)
                    ],
                    DecimalOverflowPolicy::OutputNull
                )
                .is_err()
            );
        }
    }
}

#[test]
fn string_pad_compile_success_and_ordinary_tail_preserve_all_callback_causes() {
    for ty in [source(true), FunctionValueType::new(DataType::Binary, true)] {
        let good = CompileControl::default();
        let success = ty.data_type() == &DataType::Utf8;
        assert_eq!(
            prepared_for_test_with_control(
                "lpad",
                &[
                    ty.clone(),
                    FunctionValueType::new(DataType::Int64, true),
                    source(true)
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
                    "lpad",
                    &[
                        ty.clone(),
                        FunctionValueType::new(DataType::Int64, true),
                        source(true),
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
fn string_pad_two_actual_profiles_preserve_independent_unicode_cycle_oracles() {
    let cases = [
        ("abc", 5, "xy", Some("xyabc"), Some("abcxy")),
        ("abc", 6, "xy", Some("xyxabc"), Some("abcxyx")),
        ("aé", 4, "中ß", Some("中ßaé"), Some("aé中ß")),
        ("a\u{301}b", 2, "x", Some("a\u{301}"), Some("a\u{301}")),
        ("👩‍💻", 2, "x", Some("👩‍"), Some("👩‍")),
        ("abc", 5, "", Some("abc"), Some("abc")),
        ("abc", 2, "", Some("ab"), Some("ab")),
        ("", 3, "é中", Some("é中é"), Some("é中é")),
        ("x\0y", 2, "p", Some("x\0"), Some("x\0")),
        ("abc", 0, "x", Some(""), Some("")),
        ("abc", -1, "x", None, None),
        ("abc", i64::MIN, "x", None, None),
        ("abc", i64::MAX, "x", None, None),
        ("abc", 1i64 << 32, "x", None, None),
        ("abc", MAX_PAD_LENGTH as i64 + 1, "", None, None),
        ("abc", i64::from(i32::MAX) + 1, "x", None, None),
    ];
    let text = strings(cases.iter().map(|(s, _, _, _, _)| Some(*s)).collect());
    let lengths = integers(cases.iter().map(|(_, n, _, _, _)| Some(*n)).collect());
    let pad = strings(cases.iter().map(|(_, _, p, _, _)| Some(*p)).collect());
    let args = [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::Column(&lengths),
        EvaluatedArgument::Column(&pad),
    ];
    for name in NAMES {
        // Every argument's nullable flag is an independent binding fact.
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
                let prepared = prepared_for_test_with_policy(name, &types, policy).unwrap();
                assert_eq!(prepared.contract().result_type(), &source(true));
                assert_eq!(prepared.contract().decimal_overflow_policy(), policy);
                let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
                let result = kernel
                    .evaluate(Selection::all(text.len()), &args, &Control::default())
                    .unwrap();
                let wanted: Vec<_> = cases
                    .iter()
                    .map(|(_, _, _, l, r)| (if name == "lpad" { l } else { r }).map(str::to_owned))
                    .collect();
                assert_eq!(output(&result), wanted);
            }
        }
    }
    let declarations = super::super::registry::builtin_scalar_declarations();
    for alias in ["pad_left", "pad_right"] {
        assert!(operation(alias).is_none());
        assert!(!declarations.iter().any(|(name, _)| name == alias));
    }
    for name in NAMES {
        for i in 0..3 {
            let mut wrong = sources();
            wrong[i] = if i == 1 {
                FunctionValueType::new(DataType::Int32, true)
            } else {
                FunctionValueType::new(DataType::LargeUtf8, true)
            };
            assert!(
                prepared_for_test_with_policy(name, &wrong, DecimalOverflowPolicy::OutputNull)
                    .is_err()
            );
        }
    }
}

#[test]
fn string_pad_sparse_slice_compact_scalar_and_each_constant_ordinal_remain_independent() {
    let text_backing = strings(vec![
        Some("unused"),
        Some("aé"),
        None,
        Some("中b"),
        Some("unused"),
    ]);
    let length_backing = integers(vec![Some(999), Some(4), None, Some(3), Some(999)]);
    let pad_backing = strings(vec![
        Some("unused"),
        Some("ßx"),
        None,
        Some("🙂"),
        Some("unused"),
    ]);
    let text = text_backing.slice(1, 3);
    let lengths = length_backing.slice(1, 3);
    let pad = pad_backing.slice(1, 3);
    let selected_rows = [0, 2];
    let selection = Selection::try_sparse(3, &selected_rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Utf8,
        strings(vec![Some("ßx"), Some("🙂")]),
        Box::default(),
    )
    .unwrap();
    let source_pool = pool(strings(vec![None, Some("unused"), Some("é")]));
    let pad_pool = pool(strings(vec![None, Some("unused"), Some("中ß")]));
    let length_pool = pool(integers(vec![None, Some(777), Some(4)]));
    let source_cv = source_pool.value(2).unwrap();
    let pad_cv = pad_pool.value(2).unwrap();
    let length_cv = length_pool.value(2).unwrap();
    let scalar_source = strings(vec![Some("é")]);
    let scalar_length = integers(vec![Some(4)]);
    let scalar_pad = strings(vec![Some("中ß")]);
    for name in NAMES {
        for pad_argument in [
            EvaluatedArgument::Column(&pad),
            EvaluatedArgument::SelectedColumn(&compact),
        ] {
            let mut kernel = instance(name);
            let arguments = [
                EvaluatedArgument::Column(&text),
                EvaluatedArgument::Column(&lengths),
                pad_argument,
            ];
            let result = kernel
                .evaluate(selection, &arguments, &Control::default())
                .unwrap();
            assert_eq!(result.selection(), selection);
            assert_eq!(
                output(&result),
                vec![
                    Some(expected(name, "ßxaé", "aéßx")),
                    Some(expected(name, "🙂中b", "中b🙂"))
                ]
            );
        }
        for args in [
            [
                EvaluatedArgument::Constant(&source_cv),
                EvaluatedArgument::Scalar(&scalar_length),
                EvaluatedArgument::Constant(&pad_cv),
            ],
            [
                EvaluatedArgument::Scalar(&scalar_source),
                EvaluatedArgument::Constant(&length_cv),
                EvaluatedArgument::Scalar(&scalar_pad),
            ],
        ] {
            let mut kernel = instance(name);
            let result = kernel
                .evaluate(selection, &args, &Control::default())
                .unwrap();
            assert_eq!(
                output(&result),
                vec![Some(expected(name, "中ß中é", "é中ß中")); 2]
            );
        }
    }
    assert!(Arc::ptr_eq(source_cv.pool().array(), source_pool.array()));
    assert!(Arc::ptr_eq(pad_cv.pool().array(), pad_pool.array()));
    assert!(Arc::ptr_eq(length_cv.pool().array(), length_pool.array()));
}

#[test]
fn string_pad_strict_null_and_inactive_payload_skip_work_but_do_not_mask_bad_metadata() {
    let huge = "a".repeat(320 * 1024);
    let mut hidden_bytes = huge.as_bytes().to_vec();
    hidden_bytes.push(b'B');
    let mut validity = BooleanBufferBuilder::new(2);
    validity.append(false);
    validity.append(true);
    let hidden = Arc::new(StringArray::new(
        OffsetBuffer::new(vec![0i32, huge.len() as i32, (huge.len() + 1) as i32].into()),
        Buffer::from(hidden_bytes),
        Some(NullBuffer::new(validity.finish())),
    )) as ArrayRef;
    let lengths = integers(vec![Some(3), Some(3)]);
    let pads = strings(vec![Some("xy"), Some("xy")]);
    for name in NAMES {
        for args in [
            [
                EvaluatedArgument::Column(&hidden),
                EvaluatedArgument::Column(&lengths),
                EvaluatedArgument::Column(&pads),
            ],
            [
                EvaluatedArgument::Column(&pads),
                EvaluatedArgument::Column(&lengths),
                EvaluatedArgument::Column(&hidden),
            ],
        ] {
            let control = Control::default();
            let mut kernel = instance(name);
            let result = kernel.evaluate(Selection::all(2), &args, &control).unwrap();
            assert!(output(&result)[0].is_none());
            assert!(
                control
                    .trace
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|units| *units < 256)
            );
        }
        let text = strings(vec![Some(&huge), Some("B")]);
        let selected_rows = [1];
        let selection = Selection::try_sparse(2, &selected_rows).unwrap();
        let control = Control::default();
        let mut kernel = instance(name);
        let arguments = [
            EvaluatedArgument::Column(&text),
            EvaluatedArgument::Column(&lengths),
            EvaluatedArgument::Column(&pads),
        ];
        let result = kernel.evaluate(selection, &arguments, &control).unwrap();
        assert_eq!(output(&result), vec![Some(expected(name, "xyB", "Bxy"))]);
        assert!(
            control
                .trace
                .lock()
                .unwrap()
                .iter()
                .all(|units| *units < 256)
        );
        for position in 0..3 {
            let mut types = sources();
            types[position] = types[position]
                .with_nullable(false)
                .expect("valid function nullability declaration");
            let prepared =
                prepared_for_test_with_policy(name, &types, DecimalOverflowPolicy::OutputNull)
                    .unwrap();
            let mut nonnull = ScalarEvaluationInstance::instantiate(prepared).unwrap();
            let null_lengths = integers(vec![None, Some(3)]);
            let args = [
                EvaluatedArgument::Column(if position == 0 { &hidden } else { &pads }),
                EvaluatedArgument::Column(if position == 1 {
                    &null_lengths
                } else {
                    &lengths
                }),
                EvaluatedArgument::Column(if position == 2 { &hidden } else { &pads }),
            ];
            assert!(matches!(
                nonnull.evaluate(Selection::all(2), &args, &Control::default()),
                Err(KernelFailure::InvalidProgram(_))
            ));
            let after = Control::default();
            assert_eq!(
                nonnull
                    .evaluate(Selection::all(2), &args, &after)
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            assert!(after.trace.lock().unwrap().is_empty());
        }
    }
}

#[test]
fn string_pad_character_and_byte_size_gates_are_independent_inclusive_successful_null() {
    let empty = strings(vec![Some(""), Some(""), Some(""), Some("a"), Some("abc")]);
    let lengths = integers(vec![
        Some(MAX_PAD_LENGTH as i64),
        Some(MAX_PAD_LENGTH as i64 + 1),
        Some((MAX_PAD_LENGTH / 2) as i64 + 1),
        Some((MAX_PAD_LENGTH / 2) as i64),
        Some(MAX_PAD_LENGTH as i64),
    ]);
    let pads = strings(vec![Some("x"), Some("x"), Some("é"), Some("é"), Some("")]);
    let args = [
        EvaluatedArgument::Column(&empty),
        EvaluatedArgument::Column(&lengths),
        EvaluatedArgument::Column(&pads),
    ];
    for name in NAMES {
        let mut kernel = instance(name);
        let result = kernel
            .evaluate(Selection::all(5), &args, &Control::default())
            .unwrap();
        let values = result
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0).len(), MAX_PAD_LENGTH);
        assert!(values.value(0).bytes().all(|byte| byte == b'x'));
        assert!(values.is_null(1));
        assert!(values.is_null(2));
        assert_eq!(values.value(3).len(), MAX_PAD_LENGTH - 1);
        assert_eq!(values.value(3).chars().count(), MAX_PAD_LENGTH / 2);
        assert_eq!(values.value(4), "abc");
        assert!(result.errors().is_empty());
    }
    assert!(output_capacity(0, 0).is_ok());
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
fn string_pad_empty_wrong_carrier_and_eager_required_child_errors_poison_once() {
    let text = strings(vec![Some("abc")]);
    let lengths = integers(vec![Some(1)]);
    let pads = strings(vec![Some("xy")]);
    let wrong = Arc::new(arrow_array::Int32Array::from(vec![1])) as ArrayRef;
    let rows = [];
    let selection = Selection::try_sparse(1, &rows).unwrap();
    for name in NAMES {
        let args = [
            EvaluatedArgument::Column(&text),
            EvaluatedArgument::Column(&lengths),
            EvaluatedArgument::Column(&pads),
        ];
        let mut kernel = instance(name);
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
            vec![Some("a".into())]
        );
        assert!(matches!(
            kernel.evaluate(
                Selection::all(1),
                &[
                    EvaluatedArgument::Column(&text),
                    EvaluatedArgument::Column(&wrong),
                    EvaluatedArgument::Column(&pads)
                ],
                &Control::default()
            ),
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
    // The empty-pad branch would not read pad contents, but an already computed
    // required child error cannot be hidden by this eager scalar implementation.
    let failed = SelectedValues::try_new(
        Selection::all(1),
        &DataType::Utf8,
        strings(vec![None]),
        Box::from([crate::RowDataError::new(0, "required pad failed")]),
    )
    .unwrap();
    for name in NAMES {
        let mut kernel = instance(name);
        assert!(matches!(
            kernel.evaluate(
                Selection::all(1),
                &[
                    EvaluatedArgument::Column(&text),
                    EvaluatedArgument::Column(&lengths),
                    EvaluatedArgument::SelectedColumn(&failed)
                ],
                &Control::default()
            ),
            Err(ref cause) if crate::kernel_control::test_contract::input(cause)
        ));
    }
}

#[test]
fn string_pad_every_runtime_callback_preserves_all_seven_causes_and_failure_latch() {
    for (text, length, pad) in [("aé", 5, "中ß"), ("abc", 320, "🙂x")] {
        let text = strings(vec![Some(text)]);
        let lengths = integers(vec![Some(length)]);
        let pads = strings(vec![Some(pad)]);
        let args = [
            EvaluatedArgument::Column(&text),
            EvaluatedArgument::Column(&lengths),
            EvaluatedArgument::Column(&pads),
        ];
        for name in NAMES {
            let good = Control::default();
            instance(name)
                .evaluate(Selection::all(1), &args, &good)
                .unwrap();
            let trace = good.trace.lock().unwrap().clone();
            if length == 320 {
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
                    let mut kernel = instance(name);
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
}

#[test]
fn string_pad_exact_unicode_byte_limit_and_truncated_multibyte_overflow_keep_null_semantics() {
    let long_source = "🙂".repeat(MAX_PAD_LENGTH / 4 + 1);
    let text = strings(vec![Some(""), Some(&long_source)]);
    let lengths = integers(vec![
        Some((MAX_PAD_LENGTH / 2) as i64),
        Some((MAX_PAD_LENGTH / 4 + 1) as i64),
    ]);
    let pad = strings(vec![Some("é"), Some("")]);
    let arguments = [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::Column(&lengths),
        EvaluatedArgument::Column(&pad),
    ];
    for name in NAMES {
        let mut kernel = instance(name);
        let result = kernel
            .evaluate(Selection::all(2), &arguments, &Control::default())
            .unwrap();
        let values = result
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0).len(), MAX_PAD_LENGTH);
        assert_eq!(values.value(0).chars().count(), MAX_PAD_LENGTH / 2);
        assert!(values.value(0).chars().all(|ch| ch == 'é'));
        assert!(values.is_null(1));
        assert!(result.errors().is_empty());
    }
}
