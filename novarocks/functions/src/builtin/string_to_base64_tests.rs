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

use super::super::string_to_base64_owner::{
    operation, owner_for_test, prepared_for_test_with_control, prepared_for_test_with_policy,
};
use super::*;
use crate::{
    ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionArgument, FunctionBindingRequest,
    FunctionBindingResolver, FunctionSpecializationFailure, FunctionValueType,
    PureFunctionMetadataOwner, PureScalarImplementation, ScalarEvaluationInstance,
    ScopedExpressionEffects, Selection, specialize_scalar,
};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionEffectContext, ExpressionUseId, FunctionNullBehavior,
    PureCompileControl, SemanticParameters,
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
        panic!("to_base64 never waits")
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
fn instance(source: novarocks_type_contract::ToBase64ByteSource) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_policy(
            "to_base64",
            &[self::source(true)],
            source,
            DecimalOverflowPolicy::OutputNull,
        )
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

use novarocks_type_contract::ToBase64ByteSource as Source;

#[test]
fn string_to_base64_whole_profile_explicit_source_and_original_fallback_vectors() {
    let input = strings(vec![
        Some("ÿ"),
        Some("é"),
        Some("Ā"),
        Some("ÿĀ"),
        Some("a\0b"),
        Some(""),
        None,
    ]);
    let args = [EvaluatedArgument::Column(&input)];
    for source in [Source::Ordinary, Source::NativeV1EncryptionLatin1] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let prepared =
                prepared_for_test_with_policy("to_base64", &[self::source(true)], source, policy)
                    .unwrap();
            assert_eq!(prepared.contract().to_base64_byte_source(), Some(source));
            assert_eq!(prepared.contract().result_type(), &self::source(true));
            assert_eq!(
                prepared.contract().effects().own_row_error,
                crate::FunctionIntrinsicRowError::NoRowError
            );
            assert_eq!(
                prepared.contract().effects().argument_control,
                novarocks_type_contract::ArgumentControl::Eager
            );
            assert_eq!(
                prepared.contract().effects().null_behavior,
                FunctionNullBehavior::Strict
            );
            assert!(prepared.contract().effects().environment.is_empty());
            let result = ScalarEvaluationInstance::instantiate(prepared)
                .unwrap()
                .evaluate(Selection::all(7), &args, &Control::default())
                .unwrap();
            assert_eq!(
                output(&result),
                if source == Source::Ordinary {
                    vec![
                        Some("w78=".into()),
                        Some("w6k=".into()),
                        Some("xIA=".into()),
                        Some("w7/EgA==".into()),
                        Some("YQBi".into()),
                        None,
                        None,
                    ]
                } else {
                    vec![
                        Some("/w==".into()),
                        Some("6Q==".into()),
                        Some("xIA=".into()),
                        Some("w7/EgA==".into()),
                        Some("YQBi".into()),
                        None,
                        None,
                    ]
                }
            );
        }
    }
    assert_eq!(operation("to_base64"), Some(()));
    assert_eq!(operation("from_base64"), None);
}
#[test]
fn string_to_base64_slices_selected_compact_scalar_and_nonzero_pool_keep_exact_backing() {
    let original = strings(vec![
        Some("guard"),
        Some("ÿ"),
        None,
        Some("Ā"),
        Some("guard"),
    ]);
    let input = original.slice(1, 3);
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Utf8,
        strings(vec![Some("ÿ"), Some("Ā")]),
        Box::default(),
    )
    .unwrap();
    for source in [Source::Ordinary, Source::NativeV1EncryptionLatin1] {
        for arg in [
            EvaluatedArgument::Column(&input),
            EvaluatedArgument::SelectedColumn(&compact),
        ] {
            let arguments = [arg];
            let result = instance(source)
                .evaluate(selection, &arguments, &Control::default())
                .unwrap();
            assert_eq!(result.selection(), selection);
            assert_eq!(
                output(&result),
                vec![
                    Some(
                        if source == Source::Ordinary {
                            "w78="
                        } else {
                            "/w=="
                        }
                        .into()
                    ),
                    Some("xIA=".into())
                ]
            );
        }
        let original_pool = pool(strings(vec![None, Some("unused"), Some("ÿ")]));
        let value = original_pool.value(2).unwrap();
        let scalar = strings(vec![Some("ÿ")]);
        for arg in [
            EvaluatedArgument::Constant(&value),
            EvaluatedArgument::Scalar(&scalar),
        ] {
            assert_eq!(
                output(
                    &instance(source)
                        .evaluate(selection, &[arg], &Control::default())
                        .unwrap()
                ),
                vec![
                    Some(
                        if source == Source::Ordinary {
                            "w78="
                        } else {
                            "/w=="
                        }
                        .into()
                    );
                    2
                ]
            );
        }
        assert!(Arc::ptr_eq(value.pool().array(), original_pool.array()));
        assert!(
            instance(source)
                .evaluate(
                    Selection::try_sparse(3, &[]).unwrap(),
                    &[EvaluatedArgument::Column(&input)],
                    &Control::default()
                )
                .unwrap()
                .values()
                .is_empty()
        );
    }
}
#[test]
fn string_to_base64_hidden_null_payload_and_nonnull_empty_remain_successful_null() {
    let text = strings(vec![Some("ÿ"), Some("Ā")]);
    let t = text.as_any().downcast_ref::<StringArray>().unwrap();
    let hidden = Arc::new(StringArray::new(
        t.offsets().clone(),
        t.values().clone(),
        Some(NullBuffer::from(vec![false, true])),
    )) as ArrayRef;
    for source in [Source::Ordinary, Source::NativeV1EncryptionLatin1] {
        assert_eq!(
            output(
                &instance(source)
                    .evaluate(
                        Selection::all(2),
                        &[EvaluatedArgument::Column(&hidden)],
                        &Control::default()
                    )
                    .unwrap()
            ),
            vec![None, Some("xIA=".into())]
        );
        let prepared = prepared_for_test_with_policy(
            "to_base64",
            &[self::source(false)],
            source,
            DecimalOverflowPolicy::OutputNull,
        )
        .unwrap();
        let empty = strings(vec![Some("")]);
        assert_eq!(
            output(
                &ScalarEvaluationInstance::instantiate(prepared)
                    .unwrap()
                    .evaluate(
                        Selection::all(1),
                        &[EvaluatedArgument::Column(&empty)],
                        &Control::default()
                    )
                    .unwrap()
            ),
            vec![None]
        );
    }
}
#[test]
fn string_to_base64_actual_runtime_every_callback_seven_causes_and_failed_latch() {
    let long = "ÿĀ\0".repeat(300);
    for source in [Source::Ordinary, Source::NativeV1EncryptionLatin1] {
        for input in [
            strings(vec![Some(&long)]),
            strings(vec![None; 321]),
            Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef,
        ] {
            let args = [EvaluatedArgument::Column(&input)];
            let good = Control::default();
            let ok = instance(source)
                .evaluate(Selection::all(input.len()), &args, &good)
                .is_ok();
            assert_eq!(ok, input.data_type() == &DataType::Utf8);
            let trace = good.trace.lock().unwrap().clone();
            assert!(!trace.is_empty());
            if ok {
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
                    let refusal = Control {
                        trace: Mutex::new(vec![]),
                        refusal: Some((at, cause.clone())),
                    };
                    let mut kernel = instance(source);
                    assert_eq!(
                        kernel
                            .evaluate(Selection::all(input.len()), &args, &refusal)
                            .unwrap_err(),
                        cause
                    );
                    assert_eq!(*refusal.trace.lock().unwrap(), trace[..=at]);
                    let after = Control::default();
                    assert_eq!(
                        kernel
                            .evaluate(Selection::all(input.len()), &args, &after)
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
fn string_to_base64_compile_prefixes_preserve_all_three_causes_for_both_source_facts() {
    for source in [Source::Ordinary, Source::NativeV1EncryptionLatin1] {
        for ty in [
            self::source(true),
            FunctionValueType::new(DataType::Binary, true),
        ] {
            let good = CompileControl::default();
            assert_eq!(
                prepared_for_test_with_control(
                    "to_base64",
                    std::slice::from_ref(&ty),
                    source,
                    DecimalOverflowPolicy::OutputNull,
                    &good
                )
                .is_ok(),
                ty.data_type() == &DataType::Utf8
            );
            let trace = good.trace.lock().unwrap().clone();
            assert!(!trace.is_empty());
            for at in 0..trace.len() {
                for cause in [
                    CompileControlError::Cancelled,
                    CompileControlError::DeadlineExceeded,
                    CompileControlError::ResourceExhausted,
                ] {
                    let refusal = CompileControl {
                        trace: Mutex::new(vec![]),
                        refusal: Some((at, cause.clone())),
                    };
                    let actual = prepared_for_test_with_control(
                        "to_base64",
                        std::slice::from_ref(&ty),
                        source,
                        DecimalOverflowPolicy::OutputNull,
                        &refusal,
                    )
                    .err()
                    .and_then(|error| match error {
                        FunctionSpecializationFailure::Control(c) => Some(c),
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
                    assert_eq!(*refusal.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
    }
}
#[test]
fn string_to_base64_source_receipt_is_mandatory_and_frozen_source_cannot_be_substituted() {
    let owner = owner_for_test("to_base64");
    let arguments = [FunctionArgument::Value {
        value_type: source(true),
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
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        domain: EvaluationDomainId::new(7),
        demand: EvaluationDemand::Value,
    };
    let params = SemanticParameters::try_new([]).unwrap();
    let channels = [Some(ExpressionUseId::new(42))];
    let mut input = crate::CallEffectInput {
        context,
        argument_uses: crate::CallArgumentUses::SelectedChannels(&channels),
        function_id: owner.binding_declaration().function_id(),
        kind: crate::FunctionKind::Scalar,
        selected: &selected,
        request,
        environment: &[],
        parameters: &params,
        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        proof_scope: CallProofScope::Unconditional,
    };
    assert!(
        specialize_scalar(
            &owner,
            input,
            selected.clone(),
            ScopedExpressionEffects::pure_value(context),
            crate::binding_test_control()
        )
        .is_err()
    );
    input.argument_uses = crate::CallArgumentUses::ToBase64Bytes {
        source: Source::Ordinary,
        channels: &channels,
    };
    let fresh = specialize_scalar(
        &owner,
        input,
        selected.clone(),
        ScopedExpressionEffects::pure_value(context),
        crate::binding_test_control(),
    )
    .unwrap();
    let contract = fresh.prepared().contract().clone();
    assert!(Arc::ptr_eq(
        owner
            .prepare_scalar(input, contract.clone(), crate::binding_test_control())
            .unwrap()
            .contract(),
        &contract
    ));
    input.argument_uses = crate::CallArgumentUses::ToBase64Bytes {
        source: Source::NativeV1EncryptionLatin1,
        channels: &channels,
    };
    assert!(
        owner
            .prepare_scalar(input, contract, crate::binding_test_control())
            .is_err()
    );
}

#[test]
fn string_to_base64_original_latin1_loop_observes_every_character_and_early_exit() {
    use super::super::to_base64_shared::{latin1_string_to_bytes, latin1_string_to_bytes_observed};
    for (text, expected_chars, expected) in [
        ("ÿ".repeat(769), 769, Some(vec![255; 769])),
        (format!("{}Āunused", "ÿ".repeat(769)), 770, None),
        (String::new(), 0, Some(Vec::new())),
    ] {
        assert_eq!(latin1_string_to_bytes(&text), expected);
        let mut trace = Vec::new();
        let result = latin1_string_to_bytes_observed(&text, &mut |observation| {
            trace.push(matches!(observation, Observation::Step));
            Ok::<(), KernelFailure>(())
        })
        .unwrap();
        assert_eq!(result, expected);
        assert_eq!(
            trace.iter().filter(|is_step| **is_step).count(),
            expected_chars
        );
        if expected_chars == 0 {
            assert!(trace.is_empty(), "zero-capacity Vec is not opaque work");
        } else {
            assert_eq!(&trace[..2], &[false, false]);
            assert!(trace[2..].iter().all(|is_step| *is_step));
        }
        for at in 0..trace.len() {
            for cause in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
            ] {
                let mut seen = Vec::new();
                let actual = latin1_string_to_bytes_observed(&text, &mut |observation| {
                    let ordinal = seen.len();
                    assert!(ordinal <= at, "original loop continued after first cause");
                    seen.push(matches!(observation, Observation::Step));
                    if ordinal == at {
                        Err(cause.clone())
                    } else {
                        Ok(())
                    }
                });
                assert_eq!(actual.unwrap_err(), cause);
                assert_eq!(seen, trace[..=at]);
            }
        }
    }
}

#[test]
fn string_to_base64_long_complete_latin1_and_empty_rows_preserve_quantum_and_failure_prefix() {
    let long = "ÿ".repeat(769);
    for input in [
        strings(vec![Some(&long)]),
        strings(vec![Some(""); 321]),
        strings(vec![None; 321]),
    ] {
        let arguments = [EvaluatedArgument::Column(&input)];
        let selection = Selection::all(input.len());
        let good = Control::default();
        let result = instance(Source::NativeV1EncryptionLatin1)
            .evaluate(selection, &arguments, &good)
            .unwrap();
        if input.len() == 1 {
            assert_eq!(
                output(&result),
                vec![Some(super::super::to_base64_shared::encode_base64(
                    &vec![255; 769]
                ))]
            );
        } else {
            assert_eq!(output(&result), vec![None; 321]);
        }
        let trace = good.trace.lock().unwrap().clone();
        assert!(
            trace.contains(&256),
            "actual owned work must reach its quantum"
        );
        for at in 0..trace.len() {
            for cause in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
            ] {
                let refusal = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let mut kernel = instance(Source::NativeV1EncryptionLatin1);
                assert_eq!(
                    kernel
                        .evaluate(selection, &arguments, &refusal)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*refusal.trace.lock().unwrap(), trace[..=at]);
                let after = Control::default();
                assert_eq!(
                    kernel.evaluate(selection, &arguments, &after).unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}
