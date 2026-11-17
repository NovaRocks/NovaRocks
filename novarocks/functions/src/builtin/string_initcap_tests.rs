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

use super::super::string_initcap_owner::{
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
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, FunctionInstanceState,
    FunctionNullBehavior, FunctionVolatility, PureCompileControl, SemanticParameters,
};
use std::{sync::Mutex, time::Duration};

const NAMES: [&str; 1] = ["initcap"];
// Independent legacy per-char rules, including non-contextual Greek sigma.
const ORACLES: [(&str, &str); 16] = [
    ("", ""),
    ("hELLO wORLD", "Hello World"),
    ("aBC\0dEF", "Abc\0Def"),
    ("123ABC", "123abc"),
    ("a12BC_34DEF", "A12bc_34def"),
    ("ßß", "SSß"),
    ("ﬃABC", "FFIabc"),
    ("İ", "İ"),
    ("aİ", "Ai\u{307}"),
    ("ΟΣ", "Οσ"),
    ("ΟΣΑ", "Οσα"),
    ("aΣ", "Aσ"),
    ("a\u{301}b", "A\u{301}B"),
    ("Ο\u{301}ΣΑ", "Ο\u{301}Σα"),
    ("你好👩‍💻hELLO", "你好👩‍💻Hello"),
    ("٣ABC", "٣abc"),
];
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
        panic!("initcap never waits")
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
fn instance(name: &str, nullable: bool) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_policy(name, &source(nullable), DecimalOverflowPolicy::OutputNull)
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
        Arc::new(ty.try_to_field("original").unwrap()),
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
fn string_initcap_actual_profile_preserves_independent_word_and_unicode_oracles() {
    let array = strings(ORACLES.iter().map(|(text, _)| Some(*text)).collect());
    for name in NAMES {
        for nullable in [false, true] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                let prepared =
                    prepared_for_test_with_policy(name, &source(nullable), policy).unwrap();
                assert_eq!(prepared.contract().result_type(), &source(true));
                assert_eq!(prepared.contract().decimal_overflow_policy(), policy);
                let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
                let arguments = [EvaluatedArgument::Column(&array)];
                let result = kernel
                    .evaluate(Selection::all(array.len()), &arguments, &Control::default())
                    .unwrap();
                assert_eq!(
                    output(&result),
                    ORACLES
                        .iter()
                        .map(|(_, expected)| Some(expected.to_string()))
                        .collect::<Vec<_>>()
                );
            }
        }
    }
    let declarations = super::super::registry::builtin_scalar_declarations();
    for alias in ["init_cap", "titlecase"] {
        assert!(operation(alias).is_none());
        assert!(!declarations.iter().any(|(name, _)| name == alias));
    }
}

#[test]
fn string_initcap_sparse_sliced_compact_scalar_and_selected_constant_addresses_are_independent() {
    let backing = strings(vec![
        Some("unused"),
        Some("ΟΣ"),
        None,
        Some("Straße"),
        Some("unused"),
    ]);
    let sliced = backing.slice(1, 3);
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Utf8,
        strings(vec![Some("ΟΣ"), Some("Straße")]),
        Box::default(),
    )
    .unwrap();
    let constants = pool(strings(vec![None, Some("unused"), Some("ΟΣ")]));
    let constant = constants.value(2).unwrap();
    let scalar = strings(vec![Some("ΟΣ")]);
    for name in NAMES {
        for arg in [
            EvaluatedArgument::Column(&sliced),
            EvaluatedArgument::SelectedColumn(&compact),
        ] {
            let mut kernel = instance(name, true);
            let arguments = [arg];
            let result = kernel
                .evaluate(selection, &arguments, &Control::default())
                .unwrap();
            assert_eq!(result.selection(), selection);
            assert_eq!(
                output(&result),
                vec![Some("Οσ".into()), Some("Straße".into())]
            );
        }
        for arg in [
            EvaluatedArgument::Scalar(&scalar),
            EvaluatedArgument::Constant(&constant),
        ] {
            let mut kernel = instance(name, true);
            let arguments = [arg];
            let result = kernel
                .evaluate(selection, &arguments, &Control::default())
                .unwrap();
            assert_eq!(output(&result), vec![Some("Οσ".to_owned()); 2]);
        }
        assert!(Arc::ptr_eq(constant.pool().array(), constants.array()));
    }
}

#[test]
fn string_initcap_skips_inactive_and_hidden_null_payload_and_preserves_nullability() {
    let huge = "a".repeat(320 * 1024);
    let mut bytes = huge.as_bytes().to_vec();
    bytes.push(b'B');
    let mut validity = BooleanBufferBuilder::new(2);
    validity.append(false);
    validity.append(true);
    let hidden = Arc::new(StringArray::new(
        OffsetBuffer::new(vec![0i32, huge.len() as i32, (huge.len() + 1) as i32].into()),
        Buffer::from(bytes),
        Some(NullBuffer::new(validity.finish())),
    )) as ArrayRef;
    let inactive = strings(vec![Some(&huge), Some("B")]);
    let rows = [1];
    let selected = Selection::try_sparse(2, &rows).unwrap();
    for name in NAMES {
        let control = Control::default();
        let mut kernel = instance(name, true);
        let hidden_arguments = [EvaluatedArgument::Column(&hidden)];
        let result = kernel
            .evaluate(Selection::all(2), &hidden_arguments, &control)
            .unwrap();
        assert_eq!(output(&result), vec![None, Some("B".into())]);
        assert!(
            control
                .trace
                .lock()
                .unwrap()
                .iter()
                .all(|units| *units < 256)
        );
        let inactive_arguments = [EvaluatedArgument::Column(&inactive)];
        let result = kernel
            .evaluate(selected, &inactive_arguments, &Control::default())
            .unwrap();
        assert_eq!(output(&result), vec![Some("B".into())]);
        let mut nonnull = instance(name, false);
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
                .evaluate(
                    Selection::all(2),
                    &[EvaluatedArgument::Column(&hidden)],
                    &after
                )
                .unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert!(after.trace.lock().unwrap().is_empty());
    }
}

#[test]
fn string_initcap_empty_selection_bypasses_body_and_selected_shape_errors_poison_once() {
    let array = strings(vec![Some("ΟΣ")]);
    let rows = [];
    let selection = Selection::try_sparse(1, &rows).unwrap();
    for name in NAMES {
        let mut kernel = instance(name, false);
        let arguments = [EvaluatedArgument::Column(&array)];
        let result = kernel
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        assert!(output(&result).is_empty());
        assert_eq!(
            output(
                &kernel
                    .evaluate(
                        Selection::all(1),
                        &[EvaluatedArgument::Column(&array)],
                        &Control::default()
                    )
                    .unwrap()
            )
            .len(),
            1
        );
        let wrong = Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef;
        assert!(matches!(
            kernel.evaluate(
                Selection::all(1),
                &[EvaluatedArgument::Column(&wrong)],
                &Control::default()
            ),
            Err(KernelFailure::InvalidProgram(_))
        ));
        let after = Control::default();
        assert_eq!(
            kernel
                .evaluate(
                    Selection::all(1),
                    &[EvaluatedArgument::Column(&array)],
                    &after
                )
                .unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert!(after.trace.lock().unwrap().is_empty());
    }
}

#[test]
fn string_initcap_output_layouts_refuse_before_requests() {
    assert_eq!(
        output_capacity(usize::MAX, 0),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(
        output_capacity(1, i32::MAX as usize + 1),
        Err(KernelFailure::ResourceExhausted)
    );
    assert!(output_capacity(0, 0).is_ok());
    assert!(output_capacity(1, i32::MAX as usize).is_ok());
}

#[test]
fn string_initcap_runtime_all_callbacks_and_real_256_keep_all_seven_causes_and_latch() {
    for array in [
        strings(vec![Some("ΟΣ"), None, Some("ß")]),
        strings(vec![Some(&"İΣ".repeat(320))]),
    ] {
        for name in NAMES {
            let good = Control::default();
            let mut kernel = instance(name, true);
            kernel
                .evaluate(
                    Selection::all(array.len()),
                    &[EvaluatedArgument::Column(&array)],
                    &good,
                )
                .unwrap();
            let trace = good.trace.lock().unwrap().clone();
            if array.len() == 1 {
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
                    let mut kernel = instance(name, true);
                    assert_eq!(
                        kernel
                            .evaluate(
                                Selection::all(array.len()),
                                &[EvaluatedArgument::Column(&array)],
                                &control
                            )
                            .unwrap_err(),
                        cause
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                    let after = Control::default();
                    assert_eq!(
                        kernel
                            .evaluate(
                                Selection::all(array.len()),
                                &[EvaluatedArgument::Column(&array)],
                                &after
                            )
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
fn string_initcap_owner_preserves_exact_fresh_frozen_arc_effects_and_rejects_stale_sources() {
    for name in NAMES {
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
            region: EvaluationRegionId::new(7),
            demand: EvaluationDemand::Value,
        };
        let uses = [Some(ExpressionUseId::new(42))];
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
        assert!(Arc::ptr_eq(canonical.call().selected_owner(), &selected));
        assert_eq!(owner.implementation_declarations().len(), 1);
        assert_eq!(owner.binding_declaration().overloads().len(), 1);
        assert_eq!(
            owner.implementation_declarations()[0].abi,
            crate::PureKernelAbi::ScalarV1
        );
        assert_eq!(
            owner.implementation_declarations()[0]
                .implementation
                .as_str(),
            "builtin.scalar/initcap/selected-v1"
        );
        assert_eq!(
            canonical.effects().own_row_error,
            crate::FunctionIntrinsicRowError::NoRowError
        );
        assert_eq!(
            canonical.effects().argument_control,
            novarocks_type_contract::ArgumentControl::Eager
        );
        assert!(canonical.effects().environment.is_empty());
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
            owner.binding_declaration().overloads()[0].effects.as_ref(),
            Some(&effects())
        );
        let foreign = super::super::string_case_owner::owner_for_test("lower");
        assert!(
            foreign
                .prepare_scalar(input, canonical.clone(), crate::binding_test_control())
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
        stale.context = novarocks_type_contract::ExpressionEffectContext {
            region: EvaluationRegionId::new(999),
            ..stale.context.expression().unwrap()
        }
        .into();
        assert!(
            owner
                .prepare_scalar(stale, canonical.clone(), crate::binding_test_control())
                .is_err()
        );
        stale = input;
        stale.proof_scope = CallProofScope::Region(EvaluationRegionId::new(999));
        assert!(
            owner
                .prepare_scalar(stale, canonical.clone(), crate::binding_test_control())
                .is_err()
        );
        let mut wrong_facts = canonical.effects().clone();
        wrong_facts.instance_state = FunctionInstanceState::ScalarInstance;
        assert!(
            specialize_frozen_scalar(
                &owner,
                input,
                selected.clone(),
                &wrong_facts,
                ScopedExpressionEffects::pure_value(context),
                crate::binding_test_control()
            )
            .is_err()
        );
        let mut wrong_selected = (*selected).clone();
        wrong_selected.result_type = FunctionResultType::Scalar(source(false));
        assert!(
            owner
                .validate_selected(&wrong_selected, request, crate::binding_test_control())
                .is_err()
        );
        for count in [0usize, 2] {
            let invalid_arguments = vec![arguments[0].clone(); count];
            let bad_request = FunctionBindingRequest {
                expected_result_type: None,
                arguments: &invalid_arguments,
                logical_argument_count: count,
            };
            assert!(
                owner
                    .resolve(bad_request, crate::binding_test_control())
                    .is_err()
            );
            assert!(
                owner
                    .validate_selected(&selected, bad_request, crate::binding_test_control())
                    .is_err()
            );
        }
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
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap(),
        ] {
            assert!(
                prepared_for_test_with_policy(name, &source, DecimalOverflowPolicy::OutputNull)
                    .is_err()
            );
        }
    }
}

#[test]
fn string_initcap_compile_success_and_ordinary_tail_preserve_all_callback_causes() {
    for ty in [source(true), FunctionValueType::new(DataType::Binary, true)] {
        let good = CompileControl::default();
        let success = ty.data_type() == &DataType::Utf8;
        assert_eq!(
            prepared_for_test_with_control(
                "initcap",
                &ty,
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
                    "initcap",
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
fn string_initcap_long_output_has_no_legacy_absent_one_mib_cap_or_batch_dependence() {
    let text = "a".repeat(1024 * 1024 + 1);
    let array = strings(vec![Some(&text), Some("123ABC"), None]);
    let mut kernel = instance("initcap", true);
    let control = Control::default();
    let arguments = [EvaluatedArgument::Column(&array)];
    let whole = kernel
        .evaluate(Selection::all(3), &arguments, &control)
        .unwrap();
    let values = whole
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0).len(), text.len());
    assert_eq!(values.value(0).as_bytes()[0], b'A');
    assert!(
        values.value(0).as_bytes()[1..]
            .iter()
            .all(|byte| *byte == b'a')
    );
    assert_eq!(values.value(1), "123abc");
    assert!(values.is_null(2));
    for row in 0..3 {
        let selected_rows = [row];
        let selection = Selection::try_sparse(3, &selected_rows).unwrap();
        let result = kernel
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        let single = result
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(single.is_null(0), values.is_null(row));
        if !values.is_null(row) {
            assert_eq!(single.value(0), values.value(row));
        }
    }
    assert!(control.trace.lock().unwrap().contains(&256));
}
