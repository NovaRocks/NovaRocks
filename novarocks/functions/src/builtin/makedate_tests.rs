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

use super::super::makedate_owner::{
    effects, operation, owner_for_test, prepared_for_test_with_control,
    prepared_for_test_with_policy,
};
use super::*;
use crate::{
    ConstantPolicy, ConstantPool, EvaluatedArgument, FunctionArgument, FunctionArgumentType,
    FunctionBindingRequest, FunctionBindingResolver, FunctionResultType,
    FunctionSpecializationFailure, FunctionValueType, PureFunctionMetadataOwner,
    PureScalarImplementation, ScalarEvaluationInstance, ScopedExpressionEffects, Selection,
    specialize_frozen_scalar, specialize_scalar,
};
use arrow_array::{Array, ArrayRef, Date32Array, Int32Array, Int64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionEffectContext, ExpressionUseId, FunctionInstanceState,
    FunctionNullBehavior, FunctionVolatility, PureCompileControl, SemanticParameters,
    ValueLogicalType,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

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
        panic!("makedate never waits")
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
    FunctionValueType::new(DataType::Int64, nullable)
}
fn target() -> FunctionValueType {
    FunctionValueType::new(DataType::Date32, true)
}
fn ints(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}
fn instance(types: &[FunctionValueType]) -> ScalarEvaluationInstance {
    ScalarEvaluationInstance::instantiate(
        prepared_for_test_with_policy("makedate", types, DecimalOverflowPolicy::OutputNull)
            .unwrap(),
    )
    .unwrap()
}
fn output(result: &SelectedValues<'_>) -> Vec<Option<i32>> {
    assert!(result.errors().is_empty());
    result
        .values()
        .as_any()
        .downcast_ref::<Date32Array>()
        .unwrap()
        .iter()
        .collect()
}
fn pool(array: &ArrayRef) -> ConstantPool {
    let ty = source(true);
    ConstantPool::try_new(
        Arc::new(
            ty.try_to_field("original")
                .unwrap()
                .with_metadata(std::collections::HashMap::<String, String>::from([(
                    "source-note".into(),
                    "original makedate ordinal".into(),
                )])),
        ),
        ty,
        array.to_data(),
        ConstantPolicy {
            max_rows: 8,
            max_array_nodes: 1,
            max_logical_elements: 8,
            max_retained_buffer_bytes: 4096,
            max_type_depth: 1,
            max_type_nodes: 1,
            max_dictionary_depth: 0,
            max_metadata_bytes: 4096,
            max_library_validation_work: 65536,
            max_library_validation_bytes: 65536,
        },
        CompilePhase::Validate,
        crate::binding_test_control(),
    )
    .unwrap()
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original refusal"),
        internal("original refusal"),
        KernelFailure::Operational(crate::KernelDiagnostic::new("original refusal")),
        KernelFailure::InstanceFailed,
    ]
}

#[test]
fn makedate_actual_owner_fresh_frozen_full_facts_and_raw_preparation_are_exact() {
    let owner = owner_for_test("makedate");
    assert_eq!(operation("makedate"), Some(()));
    for other in ["MAKEDATE", "make_date", "from_days", "date"] {
        assert!(operation(other).is_none());
    }
    assert_eq!(owner.binding_declaration().overloads().len(), 1);
    assert_eq!(owner.implementation_declarations().len(), 1);
    assert_eq!(
        owner.binding_declaration().function_id().as_str(),
        "builtin.scalar/makedate/v1"
    );
    assert_eq!(
        owner.implementation_declarations()[0]
            .implementation
            .as_str(),
        "builtin.scalar/makedate/selected-v1"
    );
    assert_eq!(
        owner.binding_declaration().overloads()[0].effects.as_ref(),
        Some(&effects())
    );
    let args = [
        FunctionArgument::Value {
            value_type: source(false),
            constant: None,
        },
        FunctionArgument::Value {
            value_type: source(true),
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
    assert_eq!(selected.result_type, FunctionResultType::Scalar(target()));
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(41),
        domain: EvaluationDomainId::new(7),
        demand: EvaluationDemand::Value,
    };
    let uses = [
        Some(ExpressionUseId::new(42)),
        Some(ExpressionUseId::new(43)),
    ];
    let parameters = SemanticParameters::try_new([]).unwrap();
    let input = crate::CallEffectInput {
        context,
        argument_uses: crate::CallArgumentUses::SelectedChannels(&uses),
        function_id: owner.binding_declaration().function_id(),
        kind: crate::FunctionKind::Scalar,
        selected: &selected,
        request,
        environment: &[],
        parameters: &parameters,
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
    let child_context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(42),
        ..context
    };
    let arguments_effects = ScopedExpressionEffects::pure_value(context)
        .join_same_domain(ScopedExpressionEffects::primitive(
            child_context,
            novarocks_type_contract::ExpressionEffects {
                may_raise_row_error: true,
                ..novarocks_type_contract::ExpressionEffects::PURE_VALUE
            },
        ))
        .unwrap();
    let inherited = specialize_scalar(
        &owner,
        input,
        selected.clone(),
        arguments_effects,
        crate::binding_test_control(),
    )
    .unwrap();
    assert!(
        inherited
            .effects()
            .for_use(context)
            .unwrap()
            .may_raise_row_error
    );
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
    assert!(std::ptr::eq(
        frozen.prepared().contract().selected(),
        selected.as_ref()
    ));
    assert_eq!(
        canonical.effects().value_stability,
        FunctionVolatility::Immutable
    );
    assert_eq!(
        canonical.effects().own_row_error,
        crate::FunctionIntrinsicRowError::NoRowError
    );
    assert_eq!(
        canonical.effects().null_behavior,
        FunctionNullBehavior::Strict
    );
    assert_eq!(
        canonical.effects().argument_control,
        novarocks_type_contract::ArgumentControl::Eager
    );
    assert_eq!(
        canonical.effects().instance_state,
        FunctionInstanceState::None
    );
    assert!(canonical.effects().observable_effects.is_empty());
    assert!(canonical.effects().environment.is_empty());
    let copy = (*selected).clone();
    let mut stale = input;
    stale.selected = &copy;
    assert!(
        owner
            .prepare_scalar(stale, canonical.clone(), crate::binding_test_control())
            .is_err()
    );
    stale = input;
    stale.context.domain = EvaluationDomainId::new(9);
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
    let mut wrong = canonical.effects().clone();
    wrong.null_behavior = FunctionNullBehavior::CalledOnNull;
    assert!(
        specialize_frozen_scalar(
            &owner,
            input,
            selected.clone(),
            &wrong,
            ScopedExpressionEffects::pure_value(context),
            crate::binding_test_control()
        )
        .is_err()
    );
    let mut forged = (*selected).clone();
    forged.result_type =
        FunctionResultType::Scalar(FunctionValueType::new(DataType::Date32, false));
    assert!(
        owner
            .validate_selected(&forged, request, crate::binding_test_control())
            .is_err()
    );
    // Selection may legitimately coerce a source; already-coerced preparation cannot.
    let raw = [
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Int32, true),
            constant: None,
        },
        args[1].clone(),
    ];
    let resolved = owner
        .resolve(
            FunctionBindingRequest {
                arguments: &raw,
                logical_argument_count: 2,
                expected_result_type: None,
            },
            crate::binding_test_control(),
        )
        .unwrap();
    assert!(
        matches!(&resolved.argument_types[0], FunctionArgumentType::Value(ty) if ty.data_type()==&DataType::Int64)
    );
    for bad in [
        DataType::Int32,
        DataType::Float64,
        DataType::Utf8,
        DataType::Date32,
    ] {
        assert!(
            prepared_for_test_with_policy(
                "makedate",
                &[
                    FunctionValueType::from_legacy_declared_projection(bad, true),
                    source(true)
                ],
                DecimalOverflowPolicy::OutputNull
            )
            .is_err()
        );
    }
    for count in [0, 1, 3] {
        assert!(
            prepared_for_test_with_policy(
                "makedate",
                &vec![source(true); count],
                DecimalOverflowPolicy::OutputNull
            )
            .is_err()
        );
    }
    let nominal = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    assert!(
        prepared_for_test_with_policy(
            "makedate",
            &[nominal, source(true)],
            DecimalOverflowPolicy::OutputNull
        )
        .is_err()
    );
}

#[test]
fn makedate_handwritten_year_zero_leap_and_nonrolling_boundary_oracles() {
    let cases = [
        (1970, 1, Some(0)),
        (2020, 32, Some(18293)),
        (2024, 60, Some(19782)),
        (0, 1, Some(-719528)),
        (2020, 366, Some(18627)),
        (2021, 365, Some(18992)),
        (2021, 366, None),
        (2020, 367, None),
        (2024, 0, None),
        (2024, -1, None),
        (-1, 1, None),
        (10000, 1, None),
        (i64::MIN, 1, None),
        (i64::MAX, 1, None),
        (2024, i64::MIN, None),
        (2024, i64::MAX, None),
    ];
    let years = ints(cases.iter().map(|(y, _, _)| Some(*y)).collect());
    let days = ints(cases.iter().map(|(_, d, _)| Some(*d)).collect());
    let args = [
        EvaluatedArgument::Column(&years),
        EvaluatedArgument::Column(&days),
    ];
    for nullable in [false, true] {
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let prepared = prepared_for_test_with_policy(
                "makedate",
                &[source(nullable), source(nullable)],
                policy,
            )
            .unwrap();
            assert_eq!(prepared.contract().result_type(), &target());
            assert_eq!(prepared.contract().decimal_overflow_policy(), policy);
            let result = ScalarEvaluationInstance::instantiate(prepared)
                .unwrap()
                .evaluate(Selection::all(cases.len()), &args, &Control::default())
                .unwrap();
            assert_eq!(
                output(&result),
                cases
                    .iter()
                    .map(|(_, _, expected)| *expected)
                    .collect::<Vec<_>>()
            );
        }
    }
}

#[test]
fn makedate_independent_slice_compact_scalar_and_nonzero_cv_addresses() {
    let years_back = ints(vec![Some(9999), Some(1970), None, Some(2024), Some(9999)]);
    let years = years_back.slice(1, 3);
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let compact = SelectedValues::try_new(
        selection,
        &DataType::Int64,
        ints(vec![Some(1), Some(60)]),
        Box::default(),
    )
    .unwrap();
    let args = [
        EvaluatedArgument::Column(&years),
        EvaluatedArgument::SelectedColumn(&compact),
    ];
    let mut kernel = instance(&[source(true), source(true)]);
    for _ in 0..2 {
        let result = kernel
            .evaluate(selection, &args, &Control::default())
            .unwrap();
        assert_eq!(result.selection(), selection);
        assert_eq!(output(&result), vec![Some(0), Some(19782)]);
    }
    let yp = pool(&ints(vec![Some(9999), None, Some(2020)]));
    let year = yp.value(2).unwrap();
    let dp = pool(&ints(vec![Some(367), None, Some(32)]));
    let day = dp.value(2).unwrap();
    let scalar_year = ints(vec![Some(2020)]);
    let scalar_day = ints(vec![Some(32)]);
    for args in [
        [
            EvaluatedArgument::Constant(&year),
            EvaluatedArgument::Scalar(&scalar_day),
        ],
        [
            EvaluatedArgument::Scalar(&scalar_year),
            EvaluatedArgument::Constant(&day),
        ],
    ] {
        let result = kernel
            .evaluate(selection, &args, &Control::default())
            .unwrap();
        assert_eq!(output(&result), vec![Some(18293); 2]);
    }
    assert_eq!(year.ordinal(), 2);
    assert_eq!(day.ordinal(), 2);
    assert!(Arc::ptr_eq(year.pool().array(), yp.array()));
    assert!(Arc::ptr_eq(day.pool().array(), dp.array()));
    let null_year = yp.value(1).unwrap();
    let args = [
        EvaluatedArgument::Constant(&null_year),
        EvaluatedArgument::Constant(&day),
    ];
    assert_eq!(
        output(
            &kernel
                .evaluate(selection, &args, &Control::default())
                .unwrap()
        ),
        vec![None, None]
    );
}

#[test]
fn makedate_null_inactive_empty_and_required_errors_preserve_protocol() {
    let years = ints(vec![Some(i64::MAX), None, Some(2024), Some(2020)]);
    let days = ints(vec![Some(i64::MIN), Some(60), None, Some(32)]);
    let args = [
        EvaluatedArgument::Column(&years),
        EvaluatedArgument::Column(&days),
    ];
    assert_eq!(
        output(
            &instance(&[source(true), source(true)])
                .evaluate(Selection::all(4), &args, &Control::default())
                .unwrap()
        ),
        vec![None, None, None, Some(18293)]
    );
    let rows = [3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    assert_eq!(
        output(
            &instance(&[source(true), source(true)])
                .evaluate(selection, &args, &Control::default())
                .unwrap()
        ),
        vec![Some(18293)]
    );
    let empty = Selection::try_sparse(4, &[]).unwrap();
    assert!(
        output(
            &instance(&[source(true), source(true)])
                .evaluate(empty, &args, &Control::default())
                .unwrap()
        )
        .is_empty()
    );
    let year = ints(vec![Some(2024)]);
    let null = ints(vec![None]);
    let short = ints(vec![]);
    let foreign = Arc::new(Int32Array::from(vec![60])) as ArrayRef;
    let failed = SelectedValues::try_new(
        Selection::all(1),
        &DataType::Int64,
        null.clone(),
        Box::from([crate::RowDataError::new(0, "required child failed")]),
    )
    .unwrap();
    for bad in [
        EvaluatedArgument::SelectedColumn(&failed),
        EvaluatedArgument::Column(&short),
        EvaluatedArgument::Column(&foreign),
    ] {
        let args = [EvaluatedArgument::Column(&null), bad];
        let mut kernel = instance(&[source(true), source(true)]);
        assert!(matches!(
            kernel.evaluate(Selection::all(1), &args, &Control::default()),
            Err(ref cause) if crate::kernel_control::test_contract::input(cause)
        ));
        let good = [
            EvaluatedArgument::Column(&year),
            EvaluatedArgument::Column(&year),
        ];
        let after = Control::default();
        assert_eq!(
            kernel
                .evaluate(Selection::all(1), &good, &after)
                .unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert!(after.trace.lock().unwrap().is_empty());
    }
    for args in [
        [
            EvaluatedArgument::Column(&null),
            EvaluatedArgument::Column(&year),
        ],
        [
            EvaluatedArgument::Column(&year),
            EvaluatedArgument::Column(&null),
        ],
    ] {
        assert!(matches!(
            instance(&[source(false), source(false)]).evaluate(
                Selection::all(1),
                &args,
                &Control::default()
            ),
            Err(ref cause) if crate::kernel_control::test_contract::input(cause)
        ));
    }
}

#[test]
fn makedate_every_compile_callback_preserves_three_original_causes_and_ordinary_tail() {
    for types in [
        vec![source(true), source(true)],
        vec![source(true)],
        vec![FunctionValueType::new(DataType::Int32, true), source(true)],
    ] {
        let good = CompileControl::default();
        let success = types.len() == 2 && types[0].data_type() == &DataType::Int64;
        assert_eq!(
            prepared_for_test_with_control(
                "makedate",
                &types,
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
                let error = prepared_for_test_with_control(
                    "makedate",
                    &types,
                    DecimalOverflowPolicy::OutputNull,
                    &control,
                )
                .err()
                .unwrap();
                let actual = match error {
                    FunctionSpecializationFailure::Control(e) => Some(e),
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
                };
                assert_eq!(actual, Some(cause));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn makedate_every_small_runtime_callback_keeps_seven_causes_and_failed_latch() {
    let year = ints(vec![Some(2024)]);
    let day = ints(vec![Some(60)]);
    let null = ints(vec![None]);
    let wrong = Arc::new(Int32Array::from(vec![60])) as ArrayRef;
    for (left, right, success) in [
        (&year, &day, true),
        (&year, &null, true),
        (&null, &wrong, false),
    ] {
        let args = [
            EvaluatedArgument::Column(left),
            EvaluatedArgument::Column(right),
        ];
        let good = Control::default();
        assert_eq!(
            instance(&[source(true), source(true)])
                .evaluate(Selection::all(1), &args, &good)
                .is_ok(),
            success
        );
        let trace = good.trace.lock().unwrap().clone();
        assert!(!trace.is_empty());
        for at in 0..trace.len() {
            for cause in causes() {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let mut kernel = instance(&[source(true), source(true)]);
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
fn makedate_dense_selected_rows_have_real_quantum_and_hand_output_with_control_samples() {
    for null in [false, true] {
        let year = ints(vec![if null { None } else { Some(2024) }; 320]);
        let day = ints(
            (0..320)
                .map(|i| Some(if i % 2 == 0 { 60 } else { 1 }))
                .collect(),
        );
        let args = [
            EvaluatedArgument::Column(&year),
            EvaluatedArgument::Column(&day),
        ];
        let types = [source(null), source(null)];
        let good = Control::default();
        let result = instance(&types)
            .evaluate(Selection::all(320), &args, &good)
            .unwrap();
        assert_eq!(
            output(&result),
            (0..320)
                .map(|i| if null {
                    None
                } else {
                    Some(if i % 2 == 0 { 19782 } else { 19723 })
                })
                .collect::<Vec<_>>()
        );
        let trace = good.trace.lock().unwrap().clone();
        assert!(trace.contains(&256));
        // Nonnullable inputs exercise the shared selected NULL scan. Nullable
        // NULL rows bypass opaque calendar calls: their own address/NULL/output
        // loop reaches a real quantum. Sample actual quanta plus entry/tail.
        for (at, units) in trace.iter().enumerate() {
            if at != 0 && at + 1 != trace.len() && *units != 256 {
                continue;
            }
            for cause in causes() {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let mut kernel = instance(&types);
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(320), &args, &control)
                        .unwrap_err(),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                let after = Control::default();
                assert_eq!(
                    kernel
                        .evaluate(Selection::all(320), &args, &after)
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}

#[test]
fn makedate_date32_output_layout_rejects_unrepresentable_rows_before_request() {
    assert!(output_capacity(0).is_ok());
    assert!(output_capacity(320).is_ok());
    assert_eq!(
        output_capacity(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(
        output_capacity(isize::MAX as usize / std::mem::size_of::<i32>() + 1),
        Err(KernelFailure::ResourceExhausted)
    );
}
