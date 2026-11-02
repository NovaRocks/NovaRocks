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

use super::super::string_hex_owner::{
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
        panic!("hex never waits")
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

const NAMES: [&str; 1] = ["hex"];
fn sources() -> Vec<FunctionValueType> {
    vec![source(true)]
}

#[test]
fn string_hex_owner_preserves_exact_fresh_frozen_arc_effects_and_rejects_stale_sources() {
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
        assert_eq!(owner.implementation_declarations().len(), 3);
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
        assert_eq!(owner.binding_declaration().overloads().len(), 3);
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
            FunctionValueType::new(DataType::LargeBinary, true),
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
fn string_hex_compile_success_and_ordinary_tail_preserve_all_callback_causes() {
    for ty in [
        source(true),
        FunctionValueType::new(DataType::Binary, true),
        FunctionValueType::new(DataType::Int64, true),
        FunctionValueType::new(DataType::Float64, true),
    ] {
        let good = CompileControl::default();
        let success = matches!(
            ty.data_type(),
            DataType::Utf8 | DataType::Binary | DataType::Int64
        );
        assert_eq!(
            prepared_for_test_with_control(
                "hex",
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
                    "hex",
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

fn instance_for(ty: FunctionValueType) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_policy("hex", &[ty], DecimalOverflowPolicy::OutputNull).unwrap(),
    )
    .unwrap()
}
fn bytes(values: Vec<Option<&[u8]>>) -> ArrayRef {
    Arc::new(BinaryArray::from(values))
}
fn integers(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

#[test]
fn string_hex_all_three_profiles_preserve_handwritten_bytes_and_integer_oracles() {
    let profiles = [
        (
            strings(vec![
                Some(""),
                Some("é中"),
                Some("a\0b"),
                Some("Σ"),
                Some("abc"),
            ]),
            vec!["", "C3A9E4B8AD", "610062", "CEA3", "616263"],
        ),
        (
            bytes(vec![
                Some(b""),
                Some(&[0, 127, 128, 255]),
                Some(b"\0\xFF"),
                Some(b"\xAB\xCD"),
                Some(b"abc"),
            ]),
            vec!["", "007F80FF", "00FF", "ABCD", "616263"],
        ),
        (
            integers(vec![
                Some(0),
                Some(15),
                Some(i64::MIN),
                Some(i64::MAX),
                Some(-1),
                Some(-16),
                Some(256),
            ]),
            vec![
                "0",
                "F",
                "8000000000000000",
                "7FFFFFFFFFFFFFFF",
                "FFFFFFFFFFFFFFFF",
                "FFFFFFFFFFFFFFF0",
                "100",
            ],
        ),
    ];
    for (array, expected) in profiles {
        let arguments = [EvaluatedArgument::Column(&array)];
        for nullable in [false, true] {
            for policy in [
                DecimalOverflowPolicy::OutputNull,
                DecimalOverflowPolicy::ReportError,
            ] {
                let ty = FunctionValueType::from_legacy_declared_projection(
                    array.data_type().clone(),
                    nullable,
                );
                let prepared = prepared_for_test_with_policy("hex", &[ty], policy).unwrap();
                assert_eq!(prepared.contract().result_type(), &source(true));
                assert_eq!(prepared.contract().decimal_overflow_policy(), policy);
                let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
                let result = kernel
                    .evaluate(Selection::all(array.len()), &arguments, &Control::default())
                    .unwrap();
                assert_eq!(
                    output(&result),
                    expected
                        .iter()
                        .map(|s| Some((*s).into()))
                        .collect::<Vec<_>>()
                );
            }
        }
    }
    for name in ["unhex", "HEX", "hex_upper", "to_hex"] {
        assert!(operation(name).is_none());
    }
}

#[test]
fn string_hex_profile_slices_sparse_compact_scalar_and_original_cv_ordinals_are_exact() {
    let profiles: [(ArrayRef, ArrayRef, ArrayRef, Vec<Option<String>>); 3] = [
        (
            strings(vec![
                Some("unused"),
                Some("é"),
                None,
                Some(""),
                Some("unused"),
            ]),
            strings(vec![Some("é")]),
            strings(vec![Some("é"), Some("")]),
            vec![Some("C3A9".into()), Some("".into())],
        ),
        (
            bytes(vec![
                Some(b"unused"),
                Some(&[255]),
                None,
                Some(b""),
                Some(b"unused"),
            ]),
            bytes(vec![Some(&[255])]),
            bytes(vec![Some(&[255]), Some(b"")]),
            vec![Some("FF".into()), Some("".into())],
        ),
        (
            integers(vec![Some(99), Some(-1), None, Some(0), Some(99)]),
            integers(vec![Some(-1)]),
            integers(vec![Some(-1), Some(0)]),
            vec![Some("FFFFFFFFFFFFFFFF".into()), Some("0".into())],
        ),
    ];
    for (original, scalar, compact_values, expected) in profiles {
        let ty =
            FunctionValueType::from_legacy_declared_projection(original.data_type().clone(), true);
        let sliced = original.slice(1, 3);
        let rows = [0, 2];
        let selection = Selection::try_sparse(3, &rows).unwrap();
        let compact =
            SelectedValues::try_new(selection, ty.data_type(), compact_values, Box::default())
                .unwrap();
        let mut kernel = instance_for(ty);
        for arg in [
            EvaluatedArgument::Column(&sliced),
            EvaluatedArgument::SelectedColumn(&compact),
        ] {
            let arguments = [arg];
            for _ in 0..2 {
                let result = kernel
                    .evaluate(selection, &arguments, &Control::default())
                    .unwrap();
                assert_eq!(result.selection(), selection);
                assert_eq!(output(&result), expected);
            }
        }
        let original_pool = pool(original);
        let value = original_pool.value(1).unwrap();
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
                vec![expected[0].clone(); 2]
            );
        }
        assert!(Arc::ptr_eq(value.pool().array(), original_pool.array()));
    }
}

#[test]
fn string_hex_strict_null_and_inactive_bytes_never_read_or_emit_hidden_payload() {
    let huge = vec![255u8; 320 * 1024];
    let mut data = huge.clone();
    data.push(127);
    let mut validity = BooleanBufferBuilder::new(2);
    validity.append(false);
    validity.append(true);
    let hidden = Arc::new(BinaryArray::new(
        OffsetBuffer::new(vec![0, huge.len() as i32, (huge.len() + 1) as i32].into()),
        Buffer::from(data),
        Some(NullBuffer::new(validity.finish())),
    )) as ArrayRef;
    let ty = FunctionValueType::new(DataType::Binary, true);
    let control = Control::default();
    assert_eq!(
        output(
            &instance_for(ty.clone())
                .evaluate(
                    Selection::all(2),
                    &[EvaluatedArgument::Column(&hidden)],
                    &control
                )
                .unwrap()
        ),
        vec![None, Some("7F".into())]
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
    let inactive = bytes(vec![Some(&huge), Some(&[127])]);
    let rows = [1];
    let selection = Selection::try_sparse(2, &rows).unwrap();
    let control = Control::default();
    assert_eq!(
        output(
            &instance_for(ty)
                .evaluate(selection, &[EvaluatedArgument::Column(&inactive)], &control)
                .unwrap()
        ),
        vec![Some("7F".into())]
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
    for array in [
        strings(vec![None, Some("")]),
        bytes(vec![None, Some(b"")]),
        integers(vec![None, Some(0)]),
    ] {
        let ty =
            FunctionValueType::from_legacy_declared_projection(array.data_type().clone(), true);
        let expected = if array.data_type() == &DataType::Int64 {
            "0"
        } else {
            ""
        };
        assert_eq!(
            output(
                &instance_for(ty)
                    .evaluate(
                        Selection::all(2),
                        &[EvaluatedArgument::Column(&array)],
                        &Control::default()
                    )
                    .unwrap()
            ),
            vec![None, Some(expected.into())]
        );
    }
}

#[test]
fn string_hex_empty_child_errors_bad_carrier_addresses_and_nonnull_poison_without_replay() {
    for array in [
        strings(vec![Some("abc")]),
        bytes(vec![Some(b"abc")]),
        integers(vec![Some(0)]),
    ] {
        let ty =
            FunctionValueType::from_legacy_declared_projection(array.data_type().clone(), true);
        let arguments = [EvaluatedArgument::Column(&array)];
        let empty = Selection::try_sparse(1, &[]).unwrap();
        let mut kernel = instance_for(ty.clone());
        assert!(
            output(
                &kernel
                    .evaluate(empty, &arguments, &Control::default())
                    .unwrap()
            )
            .is_empty()
        );
        let null = arrow_array::new_null_array(ty.data_type(), 1);
        let failed = SelectedValues::try_new(
            Selection::all(1),
            ty.data_type(),
            null.clone(),
            Box::from([crate::RowDataError::new(0, "required child failed")]),
        )
        .unwrap();
        let missing = arrow_array::new_empty_array(ty.data_type());
        let wrong = Arc::new(arrow_array::Float64Array::from(vec![1.0])) as ArrayRef;
        for bad in [
            EvaluatedArgument::SelectedColumn(&failed),
            EvaluatedArgument::Column(&missing),
            EvaluatedArgument::Column(&wrong),
        ] {
            let bad_arguments = [bad];
            let mut kernel = instance_for(ty.clone());
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
        let mut nonnull = ty;
        nonnull = nonnull
            .with_nullable(false)
            .expect("valid function nullability declaration");
        assert!(matches!(
            instance_for(nonnull).evaluate(
                Selection::all(1),
                &[EvaluatedArgument::Column(&null)],
                &Control::default()
            ),
            Err(ref cause) if crate::kernel_control::test_contract::input(cause)
        ));
    }
}

#[test]
fn string_hex_runtime_actual_256_and_ordinary_tail_preserve_all_seven_cause_prefixes() {
    let long = "é".repeat(160);
    let wide = vec![255u8; 320];
    for (ty, array, success) in [
        (source(true), strings(vec![Some("é")]), true),
        (source(true), strings(vec![Some(&long)]), true),
        (
            FunctionValueType::new(DataType::Binary, true),
            bytes(vec![Some(&wide)]),
            true,
        ),
        (
            FunctionValueType::new(DataType::Int64, true),
            integers(vec![Some(i64::MIN)]),
            true,
        ),
        (source(true), integers(vec![Some(1)]), false),
    ] {
        let arguments = [EvaluatedArgument::Column(&array)];
        let good = Control::default();
        assert_eq!(
            instance_for(ty.clone())
                .evaluate(Selection::all(1), &arguments, &good)
                .is_ok(),
            success
        );
        let trace = good.trace.lock().unwrap().clone();
        if array
            .as_any()
            .downcast_ref::<StringArray>()
            .is_some_and(|s| s.value(0).len() > 256)
            || array
                .as_any()
                .downcast_ref::<BinaryArray>()
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
                let mut kernel = instance_for(ty.clone());
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
fn string_hex_measured_extent_and_integer_widths_are_checked_before_requests() {
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
        byte_hex_length(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    );
    for (value, length) in [
        (0, 1),
        (15, 1),
        (16, 2),
        (255, 2),
        (256, 3),
        (u64::MAX, 16),
        (i64::MIN as u64, 16),
    ] {
        assert_eq!(integer_hex_length(value), length);
    }
}

#[test]
fn string_hex_output_above_one_mib_is_not_capped_and_partitioning_is_exact() {
    let wide = vec![255u8; 1024 * 1024 / 2 + 1];
    let array = bytes(vec![Some(&wide), Some(&[0, 128]), None]);
    let arguments = [EvaluatedArgument::Column(&array)];
    let mut kernel = instance_for(FunctionValueType::new(DataType::Binary, true));
    let result = kernel
        .evaluate(Selection::all(3), &arguments, &Control::default())
        .unwrap();
    let rendered = result
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(rendered.value(0), "FF".repeat(wide.len()));
    assert_eq!(rendered.value(1), "0080");
    assert!(rendered.is_null(2));
    for row in 0..3 {
        let rows = [row];
        let selection = Selection::try_sparse(3, &rows).unwrap();
        let single = kernel
            .evaluate(selection, &arguments, &Control::default())
            .unwrap();
        let values = single
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.is_null(0), rendered.is_null(row));
        if !values.is_null(0) {
            assert_eq!(values.value(0), rendered.value(row));
        }
    }
}
