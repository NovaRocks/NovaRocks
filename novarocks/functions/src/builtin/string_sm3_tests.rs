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

use super::super::string_sm3_owner::{
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
        panic!("sm3 never waits")
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

const NAMES: [&str; 1] = ["sm3"];
fn sources() -> Vec<FunctionValueType> {
    vec![source(true)]
}

#[test]
fn string_sm3_owner_preserves_exact_fresh_frozen_arc_effects_and_rejects_stale_sources() {
    for name in NAMES {
        let arity = 1;
        assert_eq!(operation(name), Some(()));
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
fn string_sm3_compile_success_and_ordinary_tail_preserve_all_callback_causes() {
    for ty in [source(true), FunctionValueType::new(DataType::Binary, true)] {
        let good = CompileControl::default();
        let success = ty.data_type() == &DataType::Utf8;
        assert_eq!(
            prepared_for_test_with_control(
                "sm3",
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
                    "sm3",
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

const SM3_ABC: &str = "66c7f0f4 62eeedd9 d1f2d46b dc10e4e2 4167c487 5cf2f7a2 297da02b 8f4ba8e0";
const SM3_EMPTY: &str = "";

#[test]
fn string_sm3_published_vectors_preserve_nullable_result_and_both_policies() {
    // Published SM3 vectors with the original output grouping; empty input
    // deliberately uses the original successful empty-string special case.
    let block = "abcd".repeat(16);
    let cases = [
        ("", ""),
        (
            "abc",
            "66c7f0f4 62eeedd9 d1f2d46b dc10e4e2 4167c487 5cf2f7a2 297da02b 8f4ba8e0",
        ),
        (
            block.as_str(),
            "debe9ff9 2275b8a1 38604889 c18e5a4d 6fdb70e5 387e5765 293dcba3 9c0c5732",
        ),
    ];
    let text = strings(cases.iter().map(|(s, _)| Some(*s)).collect());
    let arguments = [EvaluatedArgument::Column(&text)];
    for nullable in [false, true] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let prepared =
                prepared_for_test_with_policy("sm3", &[source(nullable)], policy).unwrap();
            assert_eq!(prepared.contract().result_type(), &source(true));
            assert_eq!(prepared.contract().decimal_overflow_policy(), policy);
            let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
            let result = kernel
                .evaluate(Selection::all(text.len()), &arguments, &Control::default())
                .unwrap();
            assert_eq!(
                output(&result),
                cases.map(|(_, digest)| Some(digest.to_owned()))
            );
        }
    }
    for alias in ["md5sum", "SM3", "sm3_hex", "md5"] {
        assert!(operation(alias).is_none());
    }
}

#[test]
fn string_sm3_slices_sparse_compact_scalar_and_nonzero_constant_ordinal_are_exact() {
    let backing = strings(vec![
        Some("unused"),
        Some("abc"),
        None,
        Some(""),
        Some("unused"),
    ]);
    let text = backing.slice(1, 3);
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Utf8,
        strings(vec![Some("abc"), Some("")]),
        Box::default(),
    )
    .unwrap();
    let mut kernel = instance("sm3");
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
            assert_eq!(
                output(&result),
                vec![Some(SM3_ABC.into()), Some(SM3_EMPTY.into())]
            );
        }
    }
    let original_pool = pool(strings(vec![None, Some("unused"), Some("abc")]));
    let value = original_pool.value(2).unwrap();
    let scalar = strings(vec![Some("abc")]);
    for arg in [
        EvaluatedArgument::Constant(&value),
        EvaluatedArgument::Scalar(&scalar),
    ] {
        let arguments = [arg];
        assert_eq!(
            output(
                &kernel
                    .evaluate(selection, &arguments, &Control::default())
                    .unwrap()
            ),
            vec![Some(SM3_ABC.into()); 2]
        );
    }
    assert!(Arc::ptr_eq(value.pool().array(), original_pool.array()));
}

#[test]
fn string_sm3_null_and_inactive_rows_never_hash_hidden_payload() {
    let huge = "x".repeat(1024 * 1024 + 1);
    let mut bytes = huge.as_bytes().to_vec();
    bytes.extend_from_slice(b"abc");
    let mut validity = BooleanBufferBuilder::new(2);
    validity.append(false);
    validity.append(true);
    let hidden = Arc::new(StringArray::new(
        OffsetBuffer::new(vec![0, huge.len() as i32, (huge.len() + 3) as i32].into()),
        Buffer::from(bytes),
        Some(NullBuffer::new(validity.finish())),
    )) as ArrayRef;
    let control = Control::default();
    assert_eq!(
        output(
            &instance("sm3")
                .evaluate(
                    Selection::all(2),
                    &[EvaluatedArgument::Column(&hidden)],
                    &control
                )
                .unwrap()
        ),
        vec![None, Some(SM3_ABC.into())]
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
    let inactive = strings(vec![Some(&huge), Some("abc")]);
    let rows = [1];
    let selection = Selection::try_sparse(2, &rows).unwrap();
    let control = Control::default();
    assert_eq!(
        output(
            &instance("sm3")
                .evaluate(selection, &[EvaluatedArgument::Column(&inactive)], &control)
                .unwrap()
        ),
        vec![Some(SM3_ABC.into())]
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
}

#[test]
fn string_sm3_empty_child_errors_bad_carrier_and_addresses_poison_without_replay() {
    let text = strings(vec![Some("abc")]);
    let arguments = [EvaluatedArgument::Column(&text)];
    let empty = Selection::try_sparse(1, &[]).unwrap();
    let mut kernel = instance("sm3");
    assert!(
        output(
            &kernel
                .evaluate(empty, &arguments, &Control::default())
                .unwrap()
        )
        .is_empty()
    );
    assert_eq!(
        output(
            &kernel
                .evaluate(Selection::all(1), &arguments, &Control::default())
                .unwrap()
        ),
        vec![Some(SM3_ABC.into())]
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
        let mut kernel = instance("sm3");
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
}

#[test]
fn string_sm3_runtime_actual_quantum_and_ordinary_tail_preserve_all_seven_cause_prefixes() {
    let long = "é".repeat(320);
    for (text, success) in [
        (strings(vec![Some("abc")]), true),
        (strings(vec![Some(&long)]), true),
        (
            Arc::new(arrow_array::Int64Array::from(vec![1])) as ArrayRef,
            false,
        ),
    ] {
        let arguments = [EvaluatedArgument::Column(&text)];
        let good = Control::default();
        assert_eq!(
            instance("sm3")
                .evaluate(Selection::all(1), &arguments, &good)
                .is_ok(),
            success
        );
        let trace = good.trace.lock().unwrap().clone();
        if text
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
                let mut kernel = instance("sm3");
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
fn string_sm3_output_extent_is_checked_before_requests() {
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
fn string_sm3_raw_utf8_nul_and_large_selected_bytes_keep_exact_oracles_and_nonnull_gate() {
    let large = "x".repeat(1024 * 1024 + 1);
    let text = strings(vec![Some("é中"), Some("a\0b"), Some(&large)]);
    let arguments = [EvaluatedArgument::Column(&text)];
    let result = instance("sm3")
        .evaluate(Selection::all(3), &arguments, &Control::default())
        .unwrap();
    // Independently frozen Python hashlib byte-stream oracles, not this recipe.
    assert_eq!(
        output(&result),
        vec![
            Some("5cb1724c eb464abf 24fb8d1e 424af61e ee86d8a2 93e9b8a2 0430cf20 4f414a6e".into()),
            Some("35b867ed 6528bb46 099058ba f776e4ee fcf98d6d accc0f67 8541899d f16fd639".into()),
            Some("60030b6f 1313c0b9 b334bc34 1a8b6edb 8e1a55e6 2fe9fadd 0f78c6c3 bb3ef85b".into())
        ]
    );
    let nonnull =
        prepared_for_test_with_policy("sm3", &[source(false)], DecimalOverflowPolicy::OutputNull)
            .unwrap();
    let null = strings(vec![None]);
    let null_arguments = [EvaluatedArgument::Column(&null)];
    let mut kernel = ScalarEvaluationInstance::instantiate(nonnull).unwrap();
    assert!(matches!(
        kernel.evaluate(Selection::all(1), &null_arguments, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
}
