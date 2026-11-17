// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::{compiler::SqlFunctionCatalog, functions::build_builtin_engine_function_catalog};
use arrow::{
    array::{Array, StringArray},
    datatypes::{DataType, Field},
};
use novarocks_functions::{
    CallEffectInput, ConstantPool, EngineFunctionCatalog, FunctionArgumentType,
    FunctionSpecializationFailure, KernelFailure, PureCallPreparation, PureCallSpecialization,
    PureKernelAbi, ScopedExpressionEffects,
};
use novarocks_physical_plan::{
    ConstantPoolId, ConstantReference, LiteralValue, NodeId, PlanLimits, ValueId,
};
use novarocks_type_contract::{
    CallProofScope, DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId,
    ExpressionEffectContext, ExpressionUseId, FunctionArgumentEvaluation, FunctionFailureBehavior,
    FunctionIntrinsicRowError, FunctionVolatility, PureCompileControl, SemanticParameterId,
    SemanticParameterKey, SemanticParameterRef, SemanticParameters, ValueLogicalType,
};
use std::{collections::HashMap, sync::Mutex};

const PHASE: CompilePhase = CompilePhase::FunctionSpecialization;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after the original refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 8,
        max_array_nodes: 32,
        max_logical_elements: 4096,
        max_retained_buffer_bytes: 4 << 20,
        max_type_depth: 16,
        max_type_nodes: 64,
        max_dictionary_depth: 4,
        max_metadata_bytes: 65536,
        max_library_validation_work: 64 << 20,
        max_library_validation_bytes: 64 << 20,
    }
}
fn ty(carrier: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(carrier, nullable)
}
fn node(id: u32, ty: FunctionValueType, kind: ExprKind) -> ExprNode {
    ExprNode {
        id: ExprId::new(id),
        owner: NodeId::new(7),
        lambda_scope: None,
        ty,
        kind,
    }
}
fn value(id: u32, ty: FunctionValueType) -> ExprNode {
    node(id, ty, ExprKind::Value(ValueId::new(id)))
}
fn arena(nodes: Vec<ExprNode>) -> ExprArena {
    ExprArena::try_from_definitions_observed(
        nodes.into_iter(),
        &PlanLimits::FROZEN,
        &Control::default(),
    )
    .unwrap()
}
fn binding(
    catalog: &EngineFunctionCatalog,
    name: &str,
    types: &[FunctionValueType],
) -> BoundFunction {
    let arguments: Vec<_> = types
        .iter()
        .map(|ty| FunctionArgument::Value {
            value_type: ty.clone(),
            constant: None,
        })
        .collect();
    let resolved =
        SqlFunctionCatalog::resolve_scalar_binding(catalog, name, &arguments, &Control::default())
            .unwrap();
    let FunctionResultType::Scalar(result_type) = resolved.selected.result_type else {
        panic!("scalar result")
    };
    BoundFunction {
        function_id: resolved.function_id,
        overload: resolved.selected.overload,
        kind: resolved.kind,
        argument_types: resolved.selected.argument_types,
        result_type,
        legacy_metadata: Some(novarocks_physical_plan::LegacyBindingMetadata {
            volatility: resolved.semantics.volatility,
            argument_evaluation: resolved.semantics.argument_evaluation,
            failure_behavior: resolved.semantics.failure_behavior,
            intrinsic_row_error: resolved.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        }),
    }
}
fn call(function: BoundFunction, ids: &[u32]) -> ExprNode {
    let result = function.result_type.clone();
    node(
        17,
        result,
        ExprKind::FunctionCall {
            function,
            args: ids.iter().copied().map(ExprId::new).collect(),
        },
    )
}
fn invoke<'a>(
    source: &'a ExprNode,
    expressions: &ExprArena,
    pools: &ConstantPools,
    control: &Control,
) -> Result<AuthoredPhysicalScalarRequest<'a>, PhysicalScalarRequestError> {
    let mut work = CompileCheckpoints::try_new(control, PHASE)?;
    let result =
        author_physical_scalar_request_observed(source, expressions, pools, policy(), &mut work);
    if matches!(&result, Err(PhysicalScalarRequestError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(u32::MAX),
        region: EvaluationRegionId::new(9),
        demand: EvaluationDemand::Value,
    }
}
fn prepare(
    catalog: &EngineFunctionCatalog,
    authored: &AuthoredPhysicalScalarRequest<'_>,
    control_intrinsic: bool,
    policy: DecimalOverflowPolicy,
    control: &Control,
) -> Result<PureCallSpecialization, FunctionSpecializationFailure> {
    let parameters = SemanticParameters::try_new([]).unwrap();
    let uses: Vec<_> = (0..authored.request().arguments.len())
        .map(|i| Some(ExpressionUseId::new(u32::try_from(i).unwrap())))
        .collect();
    let input = CallEffectInput {
        context: context(),
        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&uses),
        function_id: &authored.function.function_id,
        kind: authored.function.kind,
        selected: authored.selected.as_ref(),
        request: authored.request(),
        environment: &[],
        parameters: &parameters,
        decimal_overflow_policy: policy,
        proof_scope: CallProofScope::Region(context().region),
    };
    let arguments = ScopedExpressionEffects::pure_value(context());
    let options = if control_intrinsic {
        PureCallPreparation::ControlIntrinsic { arguments }
    } else {
        PureCallPreparation::Scalar { arguments }
    };
    catalog.prepare_fresh_selected(input, authored.selected.clone(), options, control)
}
fn assert_owner_cause(error: &FunctionSpecializationFailure, cause: CompileControlError) {
    match error {
        FunctionSpecializationFailure::Control(actual) => assert_eq!(*actual, cause),
        FunctionSpecializationFailure::Binding(
            novarocks_functions::FunctionBindingError::Control(actual),
        ) => assert_eq!(*actual, cause),
        FunctionSpecializationFailure::Kernel(actual) => assert!(matches!(
            (actual, cause),
            (KernelFailure::Cancelled, CompileControlError::Cancelled)
                | (
                    KernelFailure::DeadlineExceeded,
                    CompileControlError::DeadlineExceeded
                )
                | (
                    KernelFailure::ResourceExhausted,
                    CompileControlError::ResourceExhausted
                )
        )),
        other => panic!("original typed control replaced: {other:?}"),
    }
}
fn prefixes(
    source: &ExprNode,
    expressions: &ExprArena,
    pools: &ConstantPools,
    success: bool,
    sampled: bool,
) -> Vec<(CompilePhase, u32)> {
    let control = Control::default();
    assert_eq!(
        invoke(source, expressions, pools, &control).is_ok(),
        success
    );
    let trace = control.trace();
    assert_eq!(trace.first(), Some(&(PHASE, 0)));
    let direct = Control::default();
    let mut work = CompileCheckpoints::try_new(&direct, PHASE).unwrap();
    let _ =
        author_physical_scalar_request_observed(source, expressions, pools, policy(), &mut work);
    assert_eq!(&trace[..trace.len() - 1], direct.trace());
    assert_eq!(
        trace.len(),
        direct.trace().len() + 1,
        "caller owns an ordinary/success footer"
    );
    let positions: Vec<_> = if sampled {
        vec![0, trace.len() / 2, trace.len() - 1]
    } else {
        (0..trace.len()).collect()
    };
    for at in positions {
        for cause in CAUSES {
            let refused = Control {
                refusal: Some((at, cause.clone())),
                ..Control::default()
            };
            assert!(matches!(invoke(source,expressions,pools,&refused),
                Err(PhysicalScalarRequestError::Control(actual)) if actual==cause));
            assert_eq!(refused.trace(), trace[..=at]);
        }
    }
    trace
}

#[test]
fn scalar_requests_actual_lower_coalesce_if_preserve_order_repetition_sparse_ids_and_same_selected_arc()
 {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let text = ty(DataType::Utf8, true);
    let boolean = ty(DataType::Boolean, true);
    let expressions = arena(vec![
        value(0, text.clone()),
        value(u32::MAX, text.clone()),
        value(7, boolean.clone()),
    ]);
    for (name, types, ids, control_intrinsic) in [
        ("lower", vec![text.clone()], vec![u32::MAX], false),
        (
            "coalesce",
            vec![text.clone(); 3],
            vec![u32::MAX, 0, u32::MAX],
            true,
        ),
        (
            "if",
            vec![boolean, text.clone(), text.clone()],
            vec![7, u32::MAX, 0],
            true,
        ),
    ] {
        let source = call(binding(&catalog, name, &types), &ids);
        let pools = ConstantPools::empty();
        let authored = invoke(&source, &expressions, &pools, &Control::default()).unwrap();
        let ExprKind::FunctionCall { function, .. } = &source.kind else {
            unreachable!()
        };
        assert!(std::ptr::eq(authored.function, function));
        assert_eq!(authored.selected.overload, function.overload);
        assert_eq!(authored.selected.argument_types, function.argument_types);
        assert_eq!(authored.request().logical_argument_count, ids.len());
        assert!(std::ptr::eq(
            authored.request().expected_result_type.unwrap(),
            &source.ty
        ));
        for (argument, id) in authored.request().arguments.iter().zip(&ids) {
            let FunctionArgument::Value {
                value_type,
                constant,
            } = argument
            else {
                panic!("value")
            };
            assert_eq!(value_type, &expressions.get(ExprId::new(*id)).unwrap().ty);
            assert!(constant.is_none());
        }
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let prepared = prepare(
                &catalog,
                &authored,
                control_intrinsic,
                policy,
                &Control::default(),
            )
            .unwrap();
            assert!(Arc::ptr_eq(
                prepared.call_contract().selected_owner(),
                &authored.selected
            ));
            assert_eq!(prepared.call_contract().decimal_overflow_policy(), policy);
            assert_eq!(
                prepared.implementation().abi,
                if control_intrinsic {
                    PureKernelAbi::ControlIntrinsicV1
                } else {
                    PureKernelAbi::ScalarV1
                }
            );
        }
    }
}

fn string_pool() -> (ConstantPools, ConstantPool, FunctionValueType) {
    let source_type = ty(DataType::Utf8, true);
    let field = Arc::new(
        Field::new("source text", DataType::Utf8, true)
            .with_metadata(HashMap::from([("provider.field-id".into(), "71".into())])),
    );
    let array = StringArray::from(vec![Some("unused"), Some("Chosen"), None, Some("tail")]);
    let pool = ConstantPool::try_new(
        field,
        source_type.clone(),
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    let mut pools = ConstantPools::empty();
    pools
        .insert(ConstantPoolId::new(u32::MAX), pool.clone())
        .unwrap();
    (pools, pool, source_type)
}
fn reference(ordinal: u32) -> ConstantReference {
    ConstantReference {
        pool: ConstantPoolId::new(u32::MAX),
        ordinal,
    }
}
#[test]
fn scalar_requests_checked_constants_keep_nonzero_ordinal_typed_null_original_field_and_backing() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (pools, pool, text) = string_pool();
    let expressions = arena(vec![
        node(0, text.clone(), ExprKind::Constant(reference(2))),
        node(u32::MAX, text.clone(), ExprKind::Constant(reference(1))),
    ]);
    let source = call(
        binding(&catalog, "coalesce", &[text.clone(), text.clone(), text]),
        &[0, u32::MAX, 0],
    );
    let authored = invoke(&source, &expressions, &pools, &Control::default()).unwrap();
    for (argument, ordinal) in authored.request().arguments.iter().zip([2, 1, 2]) {
        let FunctionArgument::Value {
            value_type,
            constant: Some(value),
        } = argument
        else {
            panic!("Some typed CV")
        };
        assert_eq!(value_type, pool.value_type());
        assert_eq!(value.ordinal(), ordinal);
        assert_eq!(value.pool().backing_identity(), pool.backing_identity());
        assert!(Arc::ptr_eq(value.pool().field_ref(), pool.field_ref()));
        assert_eq!(
            value.pool().field_ref().metadata()["provider.field-id"],
            "71"
        );
        assert_eq!(
            value.try_utf8().unwrap(),
            if ordinal == 1 { Some("Chosen") } else { None }
        );
    }
    prepare(
        &catalog,
        &authored,
        true,
        DecimalOverflowPolicy::ReportError,
        &Control::default(),
    )
    .unwrap();
    prefixes(&source, &expressions, &pools, true, false);
    let wrong = arena(vec![node(
        0,
        ty(DataType::Utf8, false),
        ExprKind::Constant(reference(2)),
    )]);
    let bad = call(
        binding(&catalog, "lower", &[ty(DataType::Utf8, false)]),
        &[0],
    );
    assert!(matches!(invoke(&bad,&wrong,&pools,&Control::default()),
        Err(PhysicalScalarRequestError::Argument(PhysicalArgumentError::Reference(
            novarocks_physical_plan::ConstantReferenceError::SourceTypeMismatch(actual)))) if actual==reference(2)));
}

#[test]
fn scalar_requests_keep_actual_nullable_covariance_separate_from_selected_owner_validation() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let actual = ty(DataType::Utf8, false);
    let selected = ty(DataType::Utf8, true);
    let expressions = arena(vec![value(u32::MAX, actual.clone())]);
    let source = call(
        binding(&catalog, "lower", std::slice::from_ref(&selected)),
        &[u32::MAX],
    );
    let authored = invoke(
        &source,
        &expressions,
        &ConstantPools::empty(),
        &Control::default(),
    )
    .unwrap();
    let FunctionArgument::Value {
        value_type,
        constant,
    } = &authored.request().arguments[0]
    else {
        unreachable!()
    };
    assert_eq!(value_type, &actual);
    assert!(constant.is_none());
    assert!(actual.fits_value_type(&selected));
    assert_eq!(
        authored.selected.argument_types.as_ref(),
        &[FunctionArgumentType::Value(selected)]
    );
    // The static adapter does not retag actual source. The real resolver may
    // require its original exact selected root nullability after covariance.
    assert!(
        catalog
            .validate_frozen_selection(
                &authored.function.function_id,
                FunctionKind::Scalar,
                &authored.selected,
                authored.request(),
                &Control::default()
            )
            .is_err()
    );
    let exact = call(
        binding(&catalog, "lower", std::slice::from_ref(&actual)),
        &[u32::MAX],
    );
    let exact = invoke(
        &exact,
        &expressions,
        &ConstantPools::empty(),
        &Control::default(),
    )
    .unwrap();
    catalog
        .validate_frozen_selection(
            &exact.function.function_id,
            FunctionKind::Scalar,
            &exact.selected,
            exact.request(),
            &Control::default(),
        )
        .unwrap();
}

#[test]
fn scalar_requests_legacy_effect_fields_do_not_author_effects_and_real_owner_rejects_type_result_substitution()
 {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let text = ty(DataType::Utf8, true);
    let expressions = arena(vec![value(0, text.clone())]);
    let mut source = call(
        binding(&catalog, "lower", std::slice::from_ref(&text)),
        &[0],
    );
    let ExprKind::FunctionCall { function, .. } = &mut source.kind else {
        unreachable!()
    };
    function
        .legacy_metadata
        .as_mut()
        .expect("actual legacy fixture")
        .volatility = FunctionVolatility::Volatile;
    function
        .legacy_metadata
        .as_mut()
        .expect("actual legacy fixture")
        .argument_evaluation = FunctionArgumentEvaluation::ShortCircuit;
    function
        .legacy_metadata
        .as_mut()
        .expect("actual legacy fixture")
        .failure_behavior = FunctionFailureBehavior::ReturnsNull;
    function
        .legacy_metadata
        .as_mut()
        .expect("actual legacy fixture")
        .intrinsic_row_error = FunctionIntrinsicRowError::MayRaise;
    function
        .legacy_metadata
        .as_mut()
        .expect("actual legacy fixture")
        .semantic_parameters = Box::from([SemanticParameterRef {
        id: SemanticParameterId::new(u32::MAX),
        expected_key: SemanticParameterKey::TimeZone,
    }]);
    let authored = invoke(
        &source,
        &expressions,
        &ConstantPools::empty(),
        &Control::default(),
    )
    .unwrap();
    let prepared = prepare(
        &catalog,
        &authored,
        false,
        DecimalOverflowPolicy::OutputNull,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        prepared.call_contract().effects().value_stability,
        FunctionVolatility::Immutable
    );
    assert!(prepared.call_contract().effects().environment.is_empty());
    assert_eq!(
        prepared.call_contract().selected(),
        authored.selected.as_ref()
    );
    let mut wrong = source.clone();
    let ExprKind::FunctionCall { function, .. } = &mut wrong.kind else {
        unreachable!()
    };
    function.argument_types =
        novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
            FunctionArgumentType::Value(ty(DataType::Int64, true)),
        ]));
    let authored = invoke(
        &wrong,
        &expressions,
        &ConstantPools::empty(),
        &Control::default(),
    )
    .unwrap();
    assert!(
        catalog
            .validate_frozen_selection(
                &authored.function.function_id,
                FunctionKind::Scalar,
                &authored.selected,
                authored.request(),
                &Control::default()
            )
            .is_err()
    );
    let mut wrong_result = source.clone();
    wrong_result.ty = ty(DataType::Int64, true);
    let ExprKind::FunctionCall { function, .. } = &mut wrong_result.kind else {
        unreachable!()
    };
    function.result_type = wrong_result.ty.clone();
    let authored = invoke(
        &wrong_result,
        &expressions,
        &ConstantPools::empty(),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        authored.selected.result_type,
        FunctionResultType::Scalar(ty(DataType::Int64, true))
    );
    assert!(
        prepare(
            &catalog,
            &authored,
            false,
            DecimalOverflowPolicy::ReportError,
            &Control::default()
        )
        .is_err()
    );
}

#[test]
fn scalar_requests_reject_wrong_kind_shape_missing_argument_and_excess_before_expansion() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let text = ty(DataType::Utf8, true);
    let expressions = arena(vec![value(0, text.clone())]);
    let ordinary = value(17, text.clone());
    assert!(matches!(
        invoke(
            &ordinary,
            &expressions,
            &ConstantPools::empty(),
            &Control::default()
        ),
        Err(PhysicalScalarRequestError::InvalidSource(
            "physical scalar request requires an actual function call"
        ))
    ));
    let mut non_scalar = call(
        binding(&catalog, "lower", std::slice::from_ref(&text)),
        &[0],
    );
    let ExprKind::FunctionCall { function, .. } = &mut non_scalar.kind else {
        unreachable!()
    };
    function.kind = FunctionKind::Window;
    assert!(matches!(
        invoke(
            &non_scalar,
            &expressions,
            &ConstantPools::empty(),
            &Control::default()
        ),
        Err(PhysicalScalarRequestError::InvalidSource(
            "physical scalar request carries a non-scalar binding"
        ))
    ));
    let mismatch = call(
        binding(&catalog, "lower", std::slice::from_ref(&text)),
        &[0, 0],
    );
    assert!(matches!(
        invoke(
            &mismatch,
            &expressions,
            &ConstantPools::empty(),
            &Control::default()
        ),
        Err(PhysicalScalarRequestError::InvalidSource(
            "physical scalar argument count differs from its selected signature"
        ))
    ));
    let missing = call(
        binding(&catalog, "lower", std::slice::from_ref(&text)),
        &[u32::MAX],
    );
    assert!(
        matches!(invoke(&missing,&expressions,&ConstantPools::empty(),&Control::default()),
        Err(PhysicalScalarRequestError::MissingArgument(id)) if id==ExprId::new(u32::MAX))
    );
    let mut oversized = call(binding(&catalog, "lower", std::slice::from_ref(&text)), &[]);
    let ExprKind::FunctionCall { args, .. } = &mut oversized.kind else {
        unreachable!()
    };
    *args = vec![ExprId::new(u32::MAX); MAX_CALL_EFFECT_ARGUMENTS + 1].into_boxed_slice();
    let control = Control::default();
    assert!(matches!(
        invoke(&oversized, &expressions, &ConstantPools::empty(), &control),
        Err(PhysicalScalarRequestError::TooManyArguments)
    ));
    assert_eq!(control.trace(), vec![(PHASE, 0), (PHASE, 1)]);
    for source in [&ordinary, &non_scalar, &mismatch, &missing, &oversized] {
        prefixes(source, &expressions, &ConstantPools::empty(), false, false);
    }
}

fn declared_json_list(nullable: bool, child_nullable: bool) -> FunctionValueType {
    let child = FunctionValueType::try_with_logical_type(
        DataType::Utf8,
        child_nullable,
        ValueLogicalType::Json,
    )
    .unwrap();
    let layout = FunctionValueType::new(
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, child_nullable))),
        nullable,
    );
    let value = novarocks_type_contract::ValueTypeRef::try_new(
        novarocks_type_contract::ValueKind::List(child.value_type().clone()),
        nullable,
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
    )
    .unwrap();
    FunctionValueType::try_from_value(value, layout.typed_binding().layout().clone()).unwrap()
}
#[test]
fn scalar_requests_lambda_preserves_full_static_signature_and_observes_actual_parameter_quantum() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let item = Arc::new(Field::new("json item", DataType::Utf8, true).with_metadata(
        HashMap::from([
            ("nr_logical_type".into(), "json".into()),
            ("provider.id".into(), "71".into()),
        ]),
    ));
    let parameter = declared_json_list(true, true);
    let DataType::List(original_layout) = parameter.data_type() else {
        unreachable!()
    };
    let original_layout = original_layout.clone();
    let result =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    for count in [2, 320] {
        let mut parameters = vec![parameter.clone(); count];
        parameters[0] = FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::LargeInt,
        )
        .unwrap();
        let expressions = arena(vec![node(
            u32::MAX,
            result.clone(),
            ExprKind::Lambda {
                parameter_types: parameters.clone().into_boxed_slice(),
                body: ExprId::new(0),
            },
        )]);
        let mut function = binding(&catalog, "lower", &[ty(DataType::Utf8, true)]);
        function.argument_types =
            novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
                FunctionArgumentType::Lambda {
                    parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                        parameters.clone().into_boxed_slice(),
                    ),
                    result_type: result.clone(),
                },
            ]));
        let source = call(function, &[u32::MAX]);
        let pools = ConstantPools::empty();
        let authored = invoke(&source, &expressions, &pools, &Control::default()).unwrap();
        let FunctionArgument::Lambda {
            parameter_types,
            result_type,
        } = &authored.request().arguments[0]
        else {
            panic!("lambda")
        };
        assert_eq!(parameter_types.as_ref(), parameters.as_slice());
        assert_eq!(result_type, &result);
        let DataType::List(actual) = &parameter_types[1].data_type() else {
            unreachable!()
        };
        assert!(Arc::ptr_eq(actual, &original_layout));
        assert!(actual.metadata().is_empty());
        assert_eq!(
            parameter_types[1].child_at(0).unwrap().logical_type(),
            ValueLogicalType::Json
        );
        assert_eq!(item.metadata()["provider.id"], "71");
        assert_eq!(
            authored.selected.argument_types[0],
            authored.request().arguments[0].argument_type()
        );
        // This intentionally tests a static signature carrier, not an installed
        // LOWER/HOF capability or lexical/body validation.
        assert!(
            catalog
                .validate_frozen_selection(
                    &authored.function.function_id,
                    FunctionKind::Scalar,
                    &authored.selected,
                    authored.request(),
                    &Control::default()
                )
                .is_err()
        );
        let trace = prefixes(&source, &expressions, &pools, true, count == 320);
        if count == 320 {
            assert!(trace.contains(&(PHASE, 256)));
            for (at, _) in trace
                .iter()
                .enumerate()
                .filter(|(_, (_, units))| *units == 256)
            {
                for cause in CAUSES {
                    let refused = Control {
                        refusal: Some((at, cause.clone())),
                        ..Control::default()
                    };
                    assert!(
                        matches!(invoke(&source,&expressions,&pools,&refused),Err(PhysicalScalarRequestError::Control(actual)) if actual==cause)
                    );
                    assert_eq!(refused.trace(), trace[..=at]);
                }
            }
        }
    }
}

#[test]
fn scalar_requests_every_small_source_and_installed_owner_callback_preserves_original_three_causes()
{
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let text = ty(DataType::Utf8, true);
    let expressions = arena(vec![value(0, text.clone())]);
    let source = call(
        binding(&catalog, "lower", std::slice::from_ref(&text)),
        &[0],
    );
    let pools = ConstantPools::empty();
    prefixes(&source, &expressions, &pools, true, false);
    let authored = invoke(&source, &expressions, &pools, &Control::default()).unwrap();
    let mut wrong_result = source.clone();
    wrong_result.ty = ty(DataType::Int64, true);
    let ExprKind::FunctionCall { function, .. } = &mut wrong_result.kind else {
        unreachable!()
    };
    function.result_type = wrong_result.ty.clone();
    let rejected = invoke(&wrong_result, &expressions, &pools, &Control::default()).unwrap();
    // Original owner scopes, including its ordinary-error footer, are tested
    // independently from the static adapter's scope/profile above.
    for (request, success) in [(&authored, true), (&rejected, false)] {
        let control = Control::default();
        let result = prepare(
            &catalog,
            request,
            false,
            DecimalOverflowPolicy::ReportError,
            &control,
        );
        assert_eq!(result.is_ok(), success);
        if !success {
            assert!(matches!(
                result,
                Err(FunctionSpecializationFailure::Binding(_))
            ));
        }
        let trace = control.trace();
        for at in 0..trace.len() {
            for cause in CAUSES {
                let refused = Control {
                    refusal: Some((at, cause.clone())),
                    ..Control::default()
                };
                let error = prepare(
                    &catalog,
                    request,
                    false,
                    DecimalOverflowPolicy::ReportError,
                    &refused,
                )
                .unwrap_err();
                assert_owner_cause(&error, cause);
                assert_eq!(refused.trace(), trace[..=at]);
            }
        }
    }
    let null = node(
        0,
        ty(DataType::Utf8, false),
        ExprKind::Literal(LiteralValue::Null),
    );
    let bad_arena = arena(vec![null]);
    prefixes(&source, &bad_arena, &pools, false, false);
}

#[test]
fn scalar_requests_wide_actual_values_and_exact_4096_boundary_preserve_count_and_source_order() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let text = ty(DataType::Utf8, true);
    let nodes: Vec<_> = (0..320)
        .map(|id| value(id, ty(DataType::Utf8, id % 2 == 0)))
        .collect();
    let expressions = arena(nodes);
    let ids: Vec<u32> = (0..320).rev().collect();
    let types: Vec<_> = ids
        .iter()
        .map(|id| expressions.get(ExprId::new(*id)).unwrap().ty.clone())
        .collect();
    let source = call(binding(&catalog, "coalesce", &types), &ids);
    let pools = ConstantPools::empty();
    let authored = invoke(&source, &expressions, &pools, &Control::default()).unwrap();
    assert_eq!(authored.request().arguments.len(), 320);
    assert_eq!(authored.selected.argument_types.len(), 320);
    for (argument, id) in authored.request().arguments.iter().zip(&ids) {
        let FunctionArgument::Value {
            value_type,
            constant,
        } = argument
        else {
            unreachable!()
        };
        assert_eq!(value_type, &expressions.get(ExprId::new(*id)).unwrap().ty);
        assert!(constant.is_none());
    }
    catalog
        .validate_frozen_selection(
            &authored.function.function_id,
            FunctionKind::Scalar,
            &authored.selected,
            authored.request(),
            &Control::default(),
        )
        .unwrap();
    // Each actual Value leaf flushes. These sampled callbacks do not presume a
    // 256-unit callback; the lambda test isolates that real owned loop.
    prefixes(&source, &expressions, &pools, true, true);
    assert_eq!(MAX_CALL_EFFECT_ARGUMENTS, 4096);
    let mut boundary = source;
    let ExprKind::FunctionCall { function, args } = &mut boundary.kind else {
        unreachable!()
    };
    function.argument_types = novarocks_type_contract::TypedFactSlice::from_pure_boxed(
        vec![FunctionArgumentType::Value(text); 4096].into_boxed_slice(),
    );
    *args = vec![ExprId::new(319); 4096].into_boxed_slice();
    let authored = invoke(&boundary, &expressions, &pools, &Control::default()).unwrap();
    assert_eq!(authored.request().logical_argument_count, 4096);
    assert_eq!(authored.request().arguments.len(), 4096);
    // The boundary test is the static adapter's exact limit, not full owner
    // specialization of a fabricated 4096-channel selected signature.
}
