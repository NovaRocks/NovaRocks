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

use super::super::string_repeat_owner::{
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
        panic!("repeat/space never waits")
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
        prepared_for_test_with_policy(name, &sources(name), DecimalOverflowPolicy::OutputNull)
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

const NAMES: [&str; 2] = ["repeat", "space"];
fn sources(name: &str) -> Vec<FunctionValueType> {
    if name == "repeat" {
        vec![source(true), FunctionValueType::new(DataType::Int64, true)]
    } else {
        vec![FunctionValueType::new(DataType::Int64, true)]
    }
}
fn integers(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

#[test]
fn string_repeat_actual_profiles_preserve_negative_empty_null_unicode_and_i64_boundaries() {
    let text = strings(vec![
        Some("é中"),
        Some("x\0"),
        Some("👩‍💻"),
        Some("abc"),
        Some("abc"),
        Some(""),
        Some("abc"),
        None,
    ]);
    let counts = integers(vec![
        Some(2),
        Some(2),
        Some(1),
        Some(-1),
        Some(i64::MIN),
        Some(i64::MAX),
        Some(0),
        Some(1),
    ]);
    let args = [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::Column(&counts),
    ];
    let expected = vec![
        Some("é中é中".into()),
        Some("x\0x\0".into()),
        Some("👩‍💻".into()),
        Some("".into()),
        Some("".into()),
        Some("".into()),
        Some("".into()),
        None,
    ];
    let spaces = integers(vec![
        Some(0),
        Some(1),
        Some(3),
        Some(-1),
        Some(i64::MIN),
        Some(i64::MAX),
        None,
    ]);
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        let mut repeat = ScalarEvaluationInstance::instantiate(
            prepared_for_test_with_policy("repeat", &sources("repeat"), policy).unwrap(),
        )
        .unwrap();
        assert_eq!(
            output(
                &repeat
                    .evaluate(Selection::all(text.len()), &args, &Control::default())
                    .unwrap()
            ),
            expected
        );
        let mut space = ScalarEvaluationInstance::instantiate(
            prepared_for_test_with_policy("space", &sources("space"), policy).unwrap(),
        )
        .unwrap();
        let args = [EvaluatedArgument::Column(&spaces)];
        assert_eq!(
            output(
                &space
                    .evaluate(Selection::all(spaces.len()), &args, &Control::default())
                    .unwrap()
            ),
            vec![
                Some("".into()),
                Some(" ".into()),
                Some("   ".into()),
                None,
                None,
                None,
                None
            ]
        );
    }
}

#[test]
fn string_repeat_row_cap_exact_boundary_remains_success_or_null_not_resource_error() {
    const CAP: usize = 1_048_576;
    let text = strings(vec![Some("x"), Some("x"), Some("é"), Some("é"), Some("x")]);
    let counts = integers(vec![
        Some(CAP as i64),
        Some(CAP as i64 + 1),
        Some((CAP / 2) as i64),
        Some((CAP / 2) as i64 + 1),
        Some(i64::MAX),
    ]);
    let types = [
        source(false),
        FunctionValueType::new(DataType::Int64, false),
    ];
    let prepared =
        prepared_for_test_with_policy("repeat", &types, DecimalOverflowPolicy::OutputNull).unwrap();
    assert_eq!(prepared.contract().result_type(), &source(true));
    let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let args = [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::Column(&counts),
    ];
    let result = kernel
        .evaluate(Selection::all(5), &args, &Control::default())
        .unwrap();
    assert!(result.errors().is_empty());
    let array = result
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(array.value(0).len(), CAP);
    assert!(array.value(0).bytes().all(|b| b == b'x'));
    assert_eq!(array.value(2).len(), CAP);
    assert!(
        array
            .value(2)
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .all(|p| *p == [0xc3, 0xa9])
    );
    for row in [1, 3, 4] {
        assert!(array.is_null(row));
    }
    let spaces = integers(vec![Some(CAP as i64), Some(CAP as i64 + 1)]);
    let prepared = prepared_for_test_with_policy(
        "space",
        &[FunctionValueType::new(DataType::Int64, false)],
        DecimalOverflowPolicy::ReportError,
    )
    .unwrap();
    assert_eq!(prepared.contract().result_type(), &source(true));
    let mut kernel = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let args = [EvaluatedArgument::Column(&spaces)];
    let result = kernel
        .evaluate(Selection::all(2), &args, &Control::default())
        .unwrap();
    let array = result
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(array.value(0).len(), CAP);
    assert!(array.value(0).bytes().all(|b| b == b' '));
    assert!(array.is_null(1));
    // The same live instance remains usable after successful over-cap NULLs.
    let one = integers(vec![Some(1)]);
    let args = [EvaluatedArgument::Column(&one)];
    assert_eq!(
        output(
            &kernel
                .evaluate(Selection::all(1), &args, &Control::default())
                .unwrap()
        ),
        vec![Some(" ".into())]
    );
}

#[test]
fn string_repeat_sparse_slice_compact_scalar_and_constant_addressing_is_independent() {
    let text = strings(vec![
        Some("prefix"),
        Some("é"),
        Some("inactive"),
        Some("中"),
        Some("suffix"),
    ])
    .slice(1, 3);
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Int64,
        integers(vec![Some(1), Some(2)]),
        Box::default(),
    )
    .unwrap();
    let args = [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::SelectedColumn(&compact),
    ];
    let mut kernel = instance("repeat");
    let result = kernel
        .evaluate(selection, &args, &Control::default())
        .unwrap();
    assert_eq!(result.selection(), selection);
    assert_eq!(output(&result), vec![Some("é".into()), Some("中中".into())]);
    let constants = pool(strings(vec![None, Some("unused"), Some("é")]));
    let constant = constants.value(2).unwrap();
    let count = integers(vec![Some(2)]);
    let args = [
        EvaluatedArgument::Constant(&constant),
        EvaluatedArgument::Scalar(&count),
    ];
    assert_eq!(
        output(
            &kernel
                .evaluate(selection, &args, &Control::default())
                .unwrap()
        ),
        vec![Some("éé".into()); 2]
    );
    assert_eq!(constant.ordinal(), 2);
    assert!(Arc::ptr_eq(constant.pool().array(), constants.array()));
    let args = [EvaluatedArgument::SelectedColumn(&compact)];
    let mut kernel = instance("space");
    assert_eq!(
        output(
            &kernel
                .evaluate(selection, &args, &Control::default())
                .unwrap()
        ),
        vec![Some(" ".into()), Some("  ".into())]
    );
}

#[test]
fn string_repeat_skips_null_inactive_and_zero_output_without_scanning_hidden_payload() {
    let huge = "中".repeat(320 * 1024);
    let mut bytes = huge.as_bytes().to_vec();
    bytes.push(b'x');
    let mut valid = BooleanBufferBuilder::new(2);
    valid.append(false);
    valid.append(true);
    let hidden = Arc::new(StringArray::new(
        OffsetBuffer::new(vec![0i32, huge.len() as i32, (huge.len() + 1) as i32].into()),
        Buffer::from(bytes),
        Some(NullBuffer::new(valid.finish())),
    )) as ArrayRef;
    let inactive = strings(vec![Some(&huge), Some("x")]);
    let counts = integers(vec![Some(0), Some(2)]);
    for (text, first) in [(&hidden, None), (&inactive, Some(String::new()))] {
        let control = Control::default();
        let args = [
            EvaluatedArgument::Column(text),
            EvaluatedArgument::Column(&counts),
        ];
        let mut kernel = instance("repeat");
        assert_eq!(
            output(&kernel.evaluate(Selection::all(2), &args, &control).unwrap()),
            vec![first, Some("xx".into())]
        );
        assert!(!control.trace.lock().unwrap().contains(&256));
    }
    let counts = integers(vec![Some(i64::MAX), Some(2)]);
    let rows = [1];
    let selection = Selection::try_sparse(2, &rows).unwrap();
    let args = [
        EvaluatedArgument::Column(&inactive),
        EvaluatedArgument::Column(&counts),
    ];
    let mut kernel = instance("repeat");
    let control = Control::default();
    assert_eq!(
        output(&kernel.evaluate(selection, &args, &control).unwrap()),
        vec![Some("xx".into())]
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
}

#[test]
fn string_repeat_empty_selection_wrong_i32_and_required_child_errors_preserve_failure_latch() {
    let text = strings(vec![Some("abc")]);
    let count = integers(vec![Some(1)]);
    let wrong = Arc::new(arrow_array::Int32Array::from(vec![1])) as ArrayRef;
    let empty = [];
    let selection = Selection::try_sparse(1, &empty).unwrap();
    for name in NAMES {
        let mut args = vec![];
        let mut bad = vec![];
        if name == "repeat" {
            args.push(EvaluatedArgument::Column(&text));
            bad.push(EvaluatedArgument::Column(&text));
        }
        args.push(EvaluatedArgument::Column(&count));
        bad.push(EvaluatedArgument::Column(&wrong));
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
            vec![Some(if name == "repeat" {
                "abc".into()
            } else {
                " ".into()
            })]
        );
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
    let failed = SelectedValues::try_new(
        Selection::all(1),
        &DataType::Int64,
        integers(vec![None]),
        Box::from([crate::RowDataError::new(0, "required count failed")]),
    )
    .unwrap();
    let args = [EvaluatedArgument::SelectedColumn(&failed)];
    let mut kernel = instance("space");
    assert!(matches!(
        kernel.evaluate(Selection::all(1), &args, &Control::default()),
        Err(ref cause) if crate::kernel_control::test_contract::input(cause)
    ));
}

#[test]
fn string_repeat_output_layout_gate_is_separate_from_row_semantic_cap() {
    assert_eq!(
        output_capacity(usize::MAX, 0),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(
        output_capacity(1, i32::MAX as usize + 1),
        Err(KernelFailure::ResourceExhausted)
    );
    assert!(output_capacity(0, 0).is_ok());
    assert!(output_capacity(2, 2 * MAX_ROW_BYTES).is_ok());
}

#[test]
fn string_repeat_runtime_small_and_quantum_callbacks_preserve_three_causes_and_no_replay() {
    for name in NAMES {
        for (count, wide, wrong) in [(2, false, false), (320, true, false), (1, false, true)] {
            let text = strings(vec![Some("é")]);
            let counts = if wrong {
                Arc::new(arrow_array::Int32Array::from(vec![count as i32])) as ArrayRef
            } else {
                integers(vec![Some(count)])
            };
            let mut args = vec![];
            if name == "repeat" {
                args.push(EvaluatedArgument::Column(&text));
            }
            args.push(EvaluatedArgument::Column(&counts));
            let good = Control::default();
            let mut kernel = instance(name);
            let result = kernel.evaluate(Selection::all(1), &args, &good);
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
fn string_repeat_owner_preserves_exact_fresh_frozen_arc_effects_and_rejects_stale_sources() {
    for name in NAMES {
        let arity = if name == "repeat" { 2 } else { 1 };
        assert_eq!(
            operation(name),
            Some(if name == "repeat" {
                StringRepeatOp::Repeat
            } else {
                StringRepeatOp::Space
            })
        );
        assert!(operation("substring_index").is_none());
        let owner = owner_for_test(name);
        let arguments: Vec<_> = sources(name)
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
        for count in [0, 3, 4] {
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
        stale.context.use_id = ExpressionUseId::new(999);
        assert!(
            owner
                .prepare_scalar(stale, canonical, crate::binding_test_control())
                .is_err()
        );

        let mut bad_types = sources(name);
        bad_types[0] =
            FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
                .unwrap();
        assert!(
            prepared_for_test_with_policy(name, &bad_types, DecimalOverflowPolicy::OutputNull)
                .is_err()
        );
    }
}

#[test]
fn string_repeat_compile_success_and_ordinary_tail_preserve_all_callback_causes() {
    for ty in [source(true), FunctionValueType::new(DataType::Binary, true)] {
        let good = CompileControl::default();
        let success = ty.data_type() == &DataType::Utf8;
        assert_eq!(
            prepared_for_test_with_control(
                "repeat",
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
                    "repeat",
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

#[test]
fn string_repeat_constant_null_and_empty_max_count_remain_deferred_until_selected() {
    let owner = owner_for_test("repeat");
    let constants = pool(strings(vec![Some("unused"), None, Some("abc")]));
    let constant = constants.value(1).unwrap();
    let types = sources("repeat");
    let args = [
        FunctionArgument::Value {
            value_type: types[0].clone(),
            constant: Some(constant.clone()),
        },
        FunctionArgument::Value {
            value_type: types[1].clone(),
            constant: None,
        },
    ];
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
    let pos = integers(vec![Some(i64::MAX)]);
    let evaluated = [
        EvaluatedArgument::Constant(&constant),
        EvaluatedArgument::Scalar(&pos),
    ];
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
    assert_eq!(constant.ordinal(), 1);
    assert!(Arc::ptr_eq(constant.pool().array(), constants.array()));
}

#[test]
fn string_repeat_empty_source_max_count_is_constant_work_and_never_loops_over_count() {
    let text = strings(vec![Some("")]);
    let counts = integers(vec![Some(i64::MAX)]);
    let args = [
        EvaluatedArgument::Column(&text),
        EvaluatedArgument::Column(&counts),
    ];
    let mut kernel = instance("repeat");
    let control = Control::default();
    assert_eq!(
        output(&kernel.evaluate(Selection::all(1), &args, &control).unwrap()),
        vec![Some(String::new())]
    );
    assert!(!control.trace.lock().unwrap().contains(&256));
}
