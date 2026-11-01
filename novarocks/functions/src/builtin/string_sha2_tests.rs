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

use super::super::string_sha2_owner::{
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
        panic!("sha2 never waits")
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

const NAMES: [&str; 1] = ["sha2"];
fn sources() -> Vec<FunctionValueType> {
    vec![source(true), FunctionValueType::new(DataType::Int64, true)]
}

#[test]
fn string_sha2_owner_preserves_exact_fresh_frozen_arc_effects_and_rejects_stale_sources() {
    for name in NAMES {
        let arity = 2;
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
        for count in [0, 1, 3, 4] {
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
                    &[source, FunctionValueType::new(DataType::Int64, true)],
                    DecimalOverflowPolicy::OutputNull
                )
                .is_err()
            );
        }
    }
}

#[test]
fn string_sha2_compile_success_and_ordinary_tail_preserve_all_callback_causes() {
    for ty in [source(true), FunctionValueType::new(DataType::Binary, true)] {
        let good = CompileControl::default();
        let success = ty.data_type() == &DataType::Utf8;
        assert_eq!(
            prepared_for_test_with_control(
                "sha2",
                &[ty.clone(), FunctionValueType::new(DataType::Int64, true)],
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
                    "sha2",
                    &[ty.clone(), FunctionValueType::new(DataType::Int64, true)],
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

const SHA224_ABC: &str = "23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7";
const SHA256_ABC: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
const SHA384_ABC: &str = "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed8086072ba1e7cc2358baeca134c825a7";
const SHA512_ABC: &str = "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f";
const SHA256_EMPTY: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
fn lengths(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

#[test]
fn string_sha2_published_digests_and_selector_nullability_are_exact() {
    // Published standard SHA-2 test vectors; expectations do not call the kernel.
    let selectors = [224, 0, 256, 384, 512, -1, 1, i64::MAX];
    let expected = [
        Some(SHA224_ABC),
        Some(SHA256_ABC),
        Some(SHA256_ABC),
        Some(SHA384_ABC),
        Some(SHA512_ABC),
        None,
        None,
        None,
    ];
    let text = strings(vec![Some("abc"); selectors.len()]);
    let bits = lengths(selectors.into_iter().map(Some).collect());
    for nullable in 0..4 {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let types = [
                source(nullable & 1 != 0),
                FunctionValueType::new(DataType::Int64, nullable & 2 != 0),
            ];
            let prepared = prepared_for_test_with_policy("sha2", &types, policy).unwrap();
            assert_eq!(prepared.contract().result_type(), &source(true));
            assert_eq!(prepared.contract().decimal_overflow_policy(), policy);
            let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
            let arguments = [
                EvaluatedArgument::Column(&text),
                EvaluatedArgument::Column(&bits),
            ];
            let result = kernel
                .evaluate(Selection::all(text.len()), &arguments, &Control::default())
                .unwrap();
            assert_eq!(output(&result), expected.map(|s| s.map(str::to_owned)));
        }
    }
    let empty = strings(vec![Some("")]);
    let bits = lengths(vec![Some(256)]);
    assert_eq!(
        output(
            &instance("sha2")
                .evaluate(
                    Selection::all(1),
                    &[
                        EvaluatedArgument::Column(&empty),
                        EvaluatedArgument::Column(&bits)
                    ],
                    &Control::default()
                )
                .unwrap()
        ),
        vec![Some(SHA256_EMPTY.into())]
    );
    assert!(operation("sha256").is_none());
}

#[test]
fn string_sha2_slices_sparse_scalar_and_distinct_constant_ordinals_preserve_addresses() {
    let backing = strings(vec![
        Some("unused"),
        Some("abc"),
        None,
        Some(""),
        Some("unused"),
    ]);
    let bit_backing = lengths(vec![Some(-1), Some(224), None, Some(256), Some(1)]);
    let text = backing.slice(1, 3);
    let bits = bit_backing.slice(1, 3);
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Int64,
        lengths(vec![Some(224), Some(256)]),
        Box::default(),
    )
    .unwrap();
    for selector in [
        EvaluatedArgument::Column(&bits),
        EvaluatedArgument::SelectedColumn(&compact),
    ] {
        for _ in 0..2 {
            let mut kernel = instance("sha2");
            let arguments = [EvaluatedArgument::Column(&text), selector];
            let result = kernel
                .evaluate(selection, &arguments, &Control::default())
                .unwrap();
            assert_eq!(result.selection(), selection);
            assert_eq!(
                output(&result),
                vec![Some(SHA224_ABC.into()), Some(SHA256_EMPTY.into())]
            );
        }
    }
    let text_pool = pool(strings(vec![None, Some("unused"), Some("abc")]));
    let bit_pool = pool(lengths(vec![None, Some(256), Some(512)]));
    let text_cv = text_pool.value(2).unwrap();
    let bit_cv = bit_pool.value(1).unwrap();
    let scalar_text = strings(vec![Some("abc")]);
    let scalar_bits = lengths(vec![Some(256)]);
    for args in [
        [
            EvaluatedArgument::Constant(&text_cv),
            EvaluatedArgument::Scalar(&scalar_bits),
        ],
        [
            EvaluatedArgument::Scalar(&scalar_text),
            EvaluatedArgument::Constant(&bit_cv),
        ],
        [
            EvaluatedArgument::Constant(&text_cv),
            EvaluatedArgument::Constant(&bit_cv),
        ],
    ] {
        assert_eq!(
            output(
                &instance("sha2")
                    .evaluate(selection, &args, &Control::default())
                    .unwrap()
            ),
            vec![Some(SHA256_ABC.into()); 2]
        );
    }
    assert!(Arc::ptr_eq(text_cv.pool().array(), text_pool.array()));
    assert!(Arc::ptr_eq(bit_cv.pool().array(), bit_pool.array()));
}

#[test]
fn string_sha2_null_and_inactive_or_unsupported_rows_do_not_hash_hidden_payload() {
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
    let bits = lengths(vec![Some(256), Some(256)]);
    let control = Control::default();
    assert_eq!(
        output(
            &instance("sha2")
                .evaluate(
                    Selection::all(2),
                    &[
                        EvaluatedArgument::Column(&hidden),
                        EvaluatedArgument::Column(&bits)
                    ],
                    &control
                )
                .unwrap()
        ),
        vec![None, Some(SHA256_ABC.into())]
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
    let text = strings(vec![Some(&huge), Some("abc")]);
    let rows = [1];
    let sparse = Selection::try_sparse(2, &rows).unwrap();
    let control = Control::default();
    assert_eq!(
        output(
            &instance("sha2")
                .evaluate(
                    sparse,
                    &[
                        EvaluatedArgument::Column(&text),
                        EvaluatedArgument::Column(&bits)
                    ],
                    &control
                )
                .unwrap()
        ),
        vec![Some(SHA256_ABC.into())]
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
    let unsupported = lengths(vec![Some(-1), None]);
    let control = Control::default();
    assert_eq!(
        output(
            &instance("sha2")
                .evaluate(
                    Selection::all(2),
                    &[
                        EvaluatedArgument::Column(&text),
                        EvaluatedArgument::Column(&unsupported)
                    ],
                    &control
                )
                .unwrap()
        ),
        vec![None, None]
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
}

#[test]
fn string_sha2_empty_child_errors_bad_carrier_and_addresses_poison_without_replay() {
    let text = strings(vec![Some("abc")]);
    let bits = lengths(vec![Some(256)]);
    let args = [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::Column(&bits),
    ];
    let empty = Selection::try_sparse(1, &[]).unwrap();
    let mut kernel = instance("sha2");
    assert!(output(&kernel.evaluate(empty, &args, &Control::default()).unwrap()).is_empty());
    assert_eq!(
        output(
            &kernel
                .evaluate(Selection::all(1), &args, &Control::default())
                .unwrap()
        ),
        vec![Some(SHA256_ABC.into())]
    );
    let failed_text = SelectedValues::try_new(
        Selection::all(1),
        &DataType::Utf8,
        strings(vec![None]),
        Box::from([crate::RowDataError::new(0, "required child failed")]),
    )
    .unwrap();
    let failed_bits = SelectedValues::try_new(
        Selection::all(1),
        &DataType::Int64,
        lengths(vec![None]),
        Box::from([crate::RowDataError::new(0, "required child failed")]),
    )
    .unwrap();
    let null = strings(vec![None]);
    let missing = lengths(vec![]);
    for bad in [
        [EvaluatedArgument::SelectedColumn(&failed_text), args[1]],
        [args[0], EvaluatedArgument::SelectedColumn(&failed_bits)],
        [
            EvaluatedArgument::Scalar(&null),
            EvaluatedArgument::Column(&missing),
        ],
        [args[0], EvaluatedArgument::Column(&text)],
    ] {
        let mut kernel = instance("sha2");
        assert!(matches!(
            kernel.evaluate(Selection::all(1), &bad, &Control::default()),
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
fn string_sha2_runtime_actual_quantum_and_ordinary_tail_preserve_all_seven_cause_prefixes() {
    let long = "é".repeat(320);
    for (text, bits, success) in [
        (strings(vec![Some("abc")]), lengths(vec![Some(256)]), true),
        (strings(vec![Some(&long)]), lengths(vec![Some(512)]), true),
        (
            strings(vec![Some("abc")]),
            strings(vec![Some("256")]),
            false,
        ),
    ] {
        let args = [
            EvaluatedArgument::Column(&text),
            EvaluatedArgument::Column(&bits),
        ];
        let good = Control::default();
        assert_eq!(
            instance("sha2")
                .evaluate(Selection::all(1), &args, &good)
                .is_ok(),
            success
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
                let mut kernel = instance("sha2");
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
fn string_sha2_output_extent_is_checked_before_requests_and_standard_widths_are_closed() {
    assert!(output_capacity(0, 0).is_ok());
    assert_eq!(
        output_capacity(usize::MAX, 0),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(
        output_capacity(1, i32::MAX as usize + 1),
        Err(KernelFailure::ResourceExhausted)
    );
    for (bits, width) in [(224, 28), (0, 32), (256, 32), (384, 48), (512, 64)] {
        assert_eq!(digest_bytes(bits), Some(width));
    }
    for bits in [i64::MIN, -1, 1, 128, 257, i64::MAX] {
        assert_eq!(digest_bytes(bits), None);
    }
}

#[test]
fn string_sha2_selected_utf8_bytes_have_no_one_mib_cap_and_nonnull_source_stays_checked() {
    let large = "x".repeat(1024 * 1024 + 1);
    let text = strings(vec![Some("é中"), Some(&large)]);
    let bits = lengths(vec![Some(256), Some(256)]);
    let arguments = [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::Column(&bits),
    ];
    let result = instance("sha2")
        .evaluate(Selection::all(2), &arguments, &Control::default())
        .unwrap();
    // Independently frozen Python hashlib byte-stream oracles, not this recipe.
    assert_eq!(
        output(&result),
        vec![
            Some("3f604033720373903cc2a7f1555480bc2b976e5dafacc8e46335fec6a326aa0e".into()),
            Some("154b8ed3c2383ce429058768595935faf7851b5c38db2b1732594be1d88bc05a".into())
        ]
    );
    let nonnull = prepared_for_test_with_policy(
        "sha2",
        &[
            source(false),
            FunctionValueType::new(DataType::Int64, false),
        ],
        DecimalOverflowPolicy::OutputNull,
    )
    .unwrap();
    let null = strings(vec![None]);
    let selector = lengths(vec![Some(256)]);
    let mut kernel = ScalarEvaluationInstance::instantiate(nonnull).unwrap();
    assert!(matches!(
        kernel.evaluate(
            Selection::all(1),
            &[
                EvaluatedArgument::Column(&null),
                EvaluatedArgument::Column(&selector)
            ],
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
}
