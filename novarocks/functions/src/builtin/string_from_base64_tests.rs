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

use super::super::string_from_base64_owner::{
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
        panic!("from_base64 never waits")
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

const NAMES: [&str; 1] = ["from_base64"];
fn sources() -> Vec<FunctionValueType> {
    vec![source(true)]
}

#[test]
fn string_from_base64_owner_preserves_exact_fresh_frozen_arc_effects_and_rejects_stale_sources() {
    for name in NAMES {
        let arity = 1;
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
        for count in [0, 2, 3, 4] {
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
                prepared_for_test_with_policy(name, &[source], DecimalOverflowPolicy::OutputNull)
                    .is_err()
            );
        }
    }
}

#[test]
fn string_from_base64_compile_success_and_ordinary_tail_preserve_all_callback_causes() {
    for ty in [source(true), FunctionValueType::new(DataType::Binary, true)] {
        let good = CompileControl::default();
        let success = ty.data_type() == &DataType::Utf8;
        assert_eq!(
            prepared_for_test_with_control(
                "from_base64",
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
                    refusal: Some((at, cause.clone())),
                };
                let actual = prepared_for_test_with_control(
                    "from_base64",
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
fn string_from_base64_handwritten_standard_and_latin1_oracles_are_exact() {
    let cases = [
        ("", None),
        ("Zg==", Some("f")),
        ("Zm8=", Some("fo")),
        ("Zm9v", Some("foo")),
        ("Zm9vYmFy", Some("foobar")),
        ("AA==", Some("\0")),
        ("/w==", Some("ÿ")),
        ("gA==", Some("\u{80}")),
        ("w6k=", Some("Ã©")),
        ("Zg", None),
        ("Zg=", None),
        ("Zg===", None),
        ("Zh==", None),
        ("Zm9=", None),
        ("Zg==AAAA", None),
        ("Zg==\n", None),
        (" Zg==", None),
        ("Zg\t==", None),
        ("_w==", None),
        ("-w==", None),
        ("é", None),
        ("A", None),
    ];
    let text = strings(cases.iter().map(|(text, _)| Some(*text)).collect());
    let arguments = [EvaluatedArgument::Column(&text)];
    for nullable in [false, true] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let prepared =
                prepared_for_test_with_policy("from_base64", &[source(nullable)], policy).unwrap();
            assert_eq!(prepared.contract().result_type(), &source(true));
            assert_eq!(prepared.contract().decimal_overflow_policy(), policy);
            let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
            assert_eq!(
                output(
                    &kernel
                        .evaluate(Selection::all(text.len()), &arguments, &Control::default())
                        .unwrap()
                ),
                cases.map(|(_, expected)| expected.map(str::to_owned))
            );
        }
    }
    for alias in ["FROM_BASE64", "base64_decode", "from_base64url"] {
        assert!(operation(alias).is_none());
    }
}

#[test]
fn string_from_base64_chunk_boundaries_match_whole_standard_without_padding_loopholes() {
    let mut cases = Vec::new();
    for length in [243, 252, 255, 256, 257, 258, 512] {
        cases.push("A".repeat(length));
        cases.push(format!("{}=", "A".repeat(length.saturating_sub(1))));
    }
    for prefix in [248, 252, 256, 508] {
        cases.push(format!("{}YQ==", "A".repeat(prefix)));
        cases.push(format!("{}YQ==AAAA", "A".repeat(prefix)));
        cases.push(format!("{}YR==", "A".repeat(prefix)));
        cases.push(format!("{}AA=A", "A".repeat(prefix)));
        cases.push(format!("{}AAAA\n", "A".repeat(prefix)));
    }
    for length in [
        1, 2, 3, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 383, 384, 385,
    ] {
        cases.push(STANDARD.encode(vec![255; length]));
    }
    for text in cases {
        let expected = STANDARD
            .decode(text.as_bytes())
            .ok()
            .filter(|_| !text.is_empty())
            .map(|bytes| bytes.into_iter().map(char::from).collect::<String>());
        let array = strings(vec![Some(&text)]);
        let arguments = [EvaluatedArgument::Column(&array)];
        assert_eq!(
            output(
                &instance("from_base64")
                    .evaluate(Selection::all(1), &arguments, &Control::default())
                    .unwrap()
            ),
            vec![expected],
            "input length {}",
            text.len()
        );
    }
    // Handwritten attack: valid padding at the end of a nonfinal256 chunk.
    let attack = format!("{}YQ==AAAA", "A".repeat(252));
    assert!(STANDARD.decode(&attack).is_err());
    let array = strings(vec![Some(&attack)]);
    assert_eq!(
        output(
            &instance("from_base64")
                .evaluate(
                    Selection::all(1),
                    &[EvaluatedArgument::Column(&array)],
                    &Control::default()
                )
                .unwrap()
        ),
        vec![None]
    );
}

#[test]
fn string_from_base64_all_byte_codepoints_and_late_invalid_rows_keep_exact_extent() {
    let raw: Vec<u8> = (0..=255).collect();
    let encoded = STANDARD.encode(&raw);
    let expected: String = raw.into_iter().map(char::from).collect();
    let array = strings(vec![Some(&encoded)]);
    let arguments = [EvaluatedArgument::Column(&array)];
    assert_eq!(
        output(
            &instance("from_base64")
                .evaluate(Selection::all(1), &arguments, &Control::default())
                .unwrap()
        ),
        vec![Some(expected)]
    );
    let invalid = format!("{}!", "A".repeat(512));
    for cases in [
        vec![Some(invalid.as_str())],
        vec![Some(invalid.as_str()), Some("/w==")],
        vec![Some("/w=="), Some(invalid.as_str()), Some("Zg==")],
    ] {
        let expected = cases
            .iter()
            .map(|text| match *text {
                Some("/w==") => Some("ÿ".into()),
                Some("Zg==") => Some("f".into()),
                _ => None,
            })
            .collect::<Vec<Option<String>>>();
        let array = strings(cases);
        let arguments = [EvaluatedArgument::Column(&array)];
        assert_eq!(
            output(
                &instance("from_base64")
                    .evaluate(Selection::all(array.len()), &arguments, &Control::default())
                    .unwrap()
            ),
            expected
        );
    }
    // Real private emitter invariant: invalid prefix must neither grow the
    // admitted destination nor leave its speculative bytes behind.
    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    let mut empty = Vec::new();
    assert!(!emit_row(&invalid, &mut empty, 0, &mut work).unwrap());
    assert!(empty.is_empty());
    assert_eq!(empty.capacity(), 0);
    let mut small = Vec::with_capacity(2);
    let capacity = small.capacity();
    assert!(!emit_row(&invalid, &mut small, 2, &mut work).unwrap());
    assert!(small.is_empty());
    assert_eq!(small.capacity(), capacity);
    assert!(emit_row("/w==", &mut small, 2, &mut work).unwrap());
    assert_eq!(small, "ÿ".as_bytes());
    assert_eq!(small.capacity(), capacity);
    assert!(!emit_row(&invalid, &mut small, 2, &mut work).unwrap());
    assert_eq!(small, "ÿ".as_bytes());
    assert_eq!(small.capacity(), capacity);
    work.finish().unwrap();
}

#[test]
fn string_from_base64_slice_sparse_compact_scalar_cv_ordinals_are_exact() {
    let original = strings(vec![
        Some("unused"),
        Some("Zg=="),
        None,
        Some("/w=="),
        Some("unused"),
    ]);
    let text = original.slice(1, 3);
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Utf8,
        strings(vec![Some("Zg=="), Some("/w==")]),
        Box::default(),
    )
    .unwrap();
    let mut kernel = instance("from_base64");
    for arg in [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::SelectedColumn(&compact),
    ] {
        let arguments = [arg];
        for _ in 0..2 {
            let result = kernel
                .evaluate(selection, &arguments, &Control::default())
                .unwrap();
            assert_eq!(result.selection(), selection);
            assert_eq!(output(&result), vec![Some("f".into()), Some("ÿ".into())]);
        }
    }
    let original_pool = pool(original);
    let cv = original_pool.value(3).unwrap();
    let scalar = strings(vec![Some("/w==")]);
    for arg in [
        EvaluatedArgument::Constant(&cv),
        EvaluatedArgument::Scalar(&scalar),
    ] {
        let arguments = [arg];
        assert_eq!(
            output(
                &kernel
                    .evaluate(selection, &arguments, &Control::default())
                    .unwrap()
            ),
            vec![Some("ÿ".into()); 2]
        );
    }
    assert!(Arc::ptr_eq(cv.pool().array(), original_pool.array()));
}

#[test]
fn string_from_base64_null_and_inactive_payloads_do_not_decode_hidden_spans() {
    let huge = "A".repeat(320 * 1024);
    let mut data = huge.as_bytes().to_vec();
    data.extend_from_slice(b"Zg==");
    let mut validity = BooleanBufferBuilder::new(2);
    validity.append(false);
    validity.append(true);
    let hidden = Arc::new(StringArray::new(
        OffsetBuffer::new(vec![0, huge.len() as i32, (huge.len() + 4) as i32].into()),
        Buffer::from(data),
        Some(NullBuffer::new(validity.finish())),
    )) as ArrayRef;
    let control = Control::default();
    assert_eq!(
        output(
            &instance("from_base64")
                .evaluate(
                    Selection::all(2),
                    &[EvaluatedArgument::Column(&hidden)],
                    &control
                )
                .unwrap()
        ),
        vec![None, Some("f".into())]
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
    let inactive = strings(vec![Some(&huge), Some("Zg==")]);
    let rows = [1];
    let selection = Selection::try_sparse(2, &rows).unwrap();
    let control = Control::default();
    assert_eq!(
        output(
            &instance("from_base64")
                .evaluate(selection, &[EvaluatedArgument::Column(&inactive)], &control)
                .unwrap()
        ),
        vec![Some("f".into())]
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
}

#[test]
fn string_from_base64_empty_child_journal_bad_shape_address_and_nonnull_poison_no_replay() {
    let array = strings(vec![Some("Zg==")]);
    let arguments = [EvaluatedArgument::Column(&array)];
    let mut kernel = instance("from_base64");
    let empty = Selection::try_sparse(1, &[]).unwrap();
    assert!(
        output(
            &kernel
                .evaluate(empty, &arguments, &Control::default())
                .unwrap()
        )
        .is_empty()
    );
    let failed = SelectedValues::try_new(
        Selection::all(1),
        &DataType::Utf8,
        strings(vec![None]),
        Box::from([crate::RowDataError::new(0, "required child failed")]),
    )
    .unwrap();
    let missing = strings(vec![]);
    let wrong = Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef;
    for bad in [
        EvaluatedArgument::SelectedColumn(&failed),
        EvaluatedArgument::Column(&missing),
        EvaluatedArgument::Column(&wrong),
    ] {
        let bad_arguments = [bad];
        let mut kernel = instance("from_base64");
        assert!(matches!(
            kernel.evaluate(Selection::all(1), &bad_arguments, &Control::default()),
            Err(ref cause) if crate::kernel_control::test_contract::input(cause)
        ));
        let after = Control::default();
        assert_eq!(
            kernel
                .evaluate(Selection::all(1), &arguments, &after)
                .unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert!(after.trace.lock().unwrap().is_empty());
    }
    let nonnull = prepared_for_test_with_policy(
        "from_base64",
        &[source(false)],
        DecimalOverflowPolicy::OutputNull,
    )
    .unwrap();
    let null = strings(vec![None]);
    assert!(matches!(
        ScalarEvaluationInstance::instantiate(nonnull)
            .unwrap()
            .evaluate(
                Selection::all(1),
                &[EvaluatedArgument::Column(&null)],
                &Control::default()
            ),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
}

#[test]
fn string_from_base64_runtime_real_256_and_success_ordinary_tail_keep_seven_cause_prefixes() {
    let wide = STANDARD.encode(vec![255; 320]);
    let invalid_text = format!("{}!", "A".repeat(256));
    for (array, success) in [
        (strings(vec![Some("Zg==")]), true),
        (strings(vec![Some(&wide)]), true),
        (strings(vec![Some(&invalid_text)]), true),
        (
            Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef,
            false,
        ),
    ] {
        let arguments = [EvaluatedArgument::Column(&array)];
        let good = Control::default();
        assert_eq!(
            instance("from_base64")
                .evaluate(Selection::all(1), &arguments, &good)
                .is_ok(),
            success
        );
        let trace = good.trace.lock().unwrap().clone();
        if array
            .as_any()
            .downcast_ref::<StringArray>()
            .is_some_and(|s| s.value(0).len() > 256)
        {
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
                let mut kernel = instance("from_base64");
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
fn string_from_base64_output_extent_and_private_emitter_refuse_before_growth() {
    assert!(output_capacity(0, 0).is_ok());
    assert_eq!(
        output_capacity(usize::MAX, 0),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(
        output_capacity(1, i32::MAX as usize + 1),
        Err(KernelFailure::ResourceExhausted)
    );
    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    let mut bytes = Vec::new();
    assert_eq!(
        emit_row("/w==", &mut bytes, 0, &mut work),
        Err(internal("from_base64 exceeded its measured output extent"))
    );
    assert_eq!(bytes.capacity(), 0);
    work.finish().unwrap();
}

#[test]
fn string_from_base64_large_latin1_output_has_no_one_mib_semantic_cap() {
    let raw = vec![255u8; 1024 * 1024 / 2 + 1];
    let encoded = STANDARD.encode(&raw);
    let array = strings(vec![Some(&encoded), None, Some("")]);
    let arguments = [EvaluatedArgument::Column(&array)];
    let mut kernel = instance("from_base64");
    let result = kernel
        .evaluate(Selection::all(3), &arguments, &Control::default())
        .unwrap();
    let text = result
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(text.value(0), "ÿ".repeat(raw.len()));
    assert!(text.is_null(1));
    assert!(text.is_null(2));
}
