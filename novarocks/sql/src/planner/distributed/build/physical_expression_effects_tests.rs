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

use super::super::expression_component_test_fixture::{FixtureBuilder, UnitFixture};
use super::*;
use crate::functions::build_builtin_engine_function_catalog;
use crate::test_constants as checked;
use arrow::{
    array::{Array, StringArray},
    datatypes::{DataType, Field, TimeUnit},
};
use novarocks_functions::{
    ConstantPool, EngineFunctionCatalog, FunctionArgument, FunctionResultType,
};
use novarocks_type_contract::{
    ControlShape, EvaluationDemand, ExpressionEffects, FunctionIntrinsicRowError, FunctionKind,
    FunctionValueType, FunctionVolatility, GuardKind, RegionGuard, SemanticParameterId,
    SemanticParameterKey,
};
use std::sync::{Arc, Mutex};
use {
    novarocks_expression_contract::{BoundFunction, RootOrdinal},
    novarocks_physical_plan::{
        ConstantPoolId, ConstantReference, FragmentId, FragmentSink, NodeId, NodeKind,
        PhysicalCallSite, PipelineDopDomain, PlanLimits, ValueOrigin, WindowCallSpec, WindowSpec,
    },
};
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
            assert!(at <= stop, "callback after primary refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
fn ty(carrier: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(carrier, nullable)
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
        legacy_metadata: Some(novarocks_expression_contract::LegacyBindingMetadata {
            volatility: resolved.semantics.volatility,
            argument_evaluation: resolved.semantics.argument_evaluation,
            failure_behavior: resolved.semantics.failure_behavior,
            intrinsic_row_error: resolved.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        }),
    }
}

fn literal(
    builder: &mut FixtureBuilder,
    owner: NodeId,
    ty: FunctionValueType,
    value: checked::Source,
) -> ExprId {
    assert_eq!(owner, builder.node);
    let kind = checked::insert(&mut builder.constants, value(&ty));
    builder.add(ty, kind)
}
fn call(
    builder: &mut FixtureBuilder,
    owner: NodeId,
    function: BoundFunction,
    args: &[ExprId],
) -> ExprId {
    assert_eq!(owner, builder.node);
    builder.add(
        function.result_type.clone(),
        ExprKind::FunctionCall {
            function,
            args: args.into(),
        },
    )
}
fn value_roots(builder: &mut FixtureBuilder, definitions: &[ExprId]) {
    for &definition in definitions {
        builder.root(definition, EvaluationDemand::Value);
    }
}
fn lower(catalog: &EngineFunctionCatalog) -> (UnitFixture, ExprId) {
    let mut builder = FixtureBuilder::new();
    let owner = builder.node;
    let arg = literal(
        &mut builder,
        owner,
        ty(DataType::Utf8, false),
        checked::utf8("Straße".into()),
    );
    let root = call(
        &mut builder,
        owner,
        binding(catalog, "lower", &[ty(DataType::Utf8, false)]),
        &[arg],
    );
    value_roots(&mut builder, &[root]);
    (builder.seal(catalog, policy()), root)
}

fn source_scopes(fixture: &UnitFixture) -> BTreeMap<ExpressionUseId, PhysicalCallSourceScope<'_>> {
    fixture
        .flow()
        .uses()
        .iter()
        .filter_map(|(&id, item)| {
            let source = fixture.definition(item.definition);
            matches!(source.kind, ExprKind::FunctionCall { .. }).then_some((
                id,
                PhysicalCallSourceScope {
                    source,
                    decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                    environment: &[],
                    proof_scope: CallProofScope::Region(item.context.region),
                },
            ))
        })
        .collect()
}
fn run(
    fixture: &UnitFixture,
    pools: &ConstantPools,
    params: &SemanticParameters,
    scopes: &BTreeMap<ExpressionUseId, PhysicalCallSourceScope<'_>>,
    catalog: &EngineFunctionCatalog,
    control: &Control,
) -> Result<AuthoredPhysicalExpressionEffects, PhysicalExpressionEffectsError> {
    let mut sources = pools.clone();
    checked::merge(&mut sources, &fixture.constants);
    author_physical_expression_effects_observed(
        PhysicalExpressionEffectsInput {
            owner: crate::planner::distributed::build::unit_source::ScalarUnitOwner::Node(
                fixture.node,
            ),
            unit: &fixture.sealed,
            constants: &sources,
            parameters: params,
            literal_policy: policy(),
            call_scopes: scopes,
        },
        catalog,
        control,
    )
}
fn root_use(fixture: &UnitFixture, definition: ExprId) -> ExpressionUseId {
    let ordinal = fixture
        .sealed
        .roots()
        .iter()
        .position(|root| root.definition == definition)
        .unwrap();
    fixture.root_use(RootOrdinal::new(u32::try_from(ordinal).unwrap()))
}
fn invocation_use(fixture: &UnitFixture, definition: ExprId) -> ExpressionUseId {
    let mut uses = fixture
        .flow()
        .uses()
        .iter()
        .filter(|(_, item)| item.definition == definition);
    let id = *uses.next().unwrap().0;
    assert!(
        uses.next().is_none(),
        "this source must have one invocation"
    );
    id
}
fn summary(
    result: &AuthoredPhysicalExpressionEffects,
    fixture: &UnitFixture,
    definition: ExprId,
) -> ExpressionEffects {
    let id = root_use(fixture, definition);
    result.summaries[&id]
        .for_use(fixture.flow().uses()[&id].context)
        .unwrap()
}
fn prefixes(
    mut invoke: impl FnMut(
        &Control,
    )
        -> Result<AuthoredPhysicalExpressionEffects, PhysicalExpressionEffectsError>,
    success: bool,
    sampled: bool,
) -> Vec<(CompilePhase, u32)> {
    let baseline = Control::default();
    let baseline_result = invoke(&baseline);
    assert_eq!(baseline_result.is_ok(), success);
    let trace = baseline.trace();
    assert_eq!(trace.first(), Some(&(PHASE, 0)));
    if success {
        assert!(trace.len() > 1);
    } else {
        // A checked SealedUnit needs no repeated structural validation here.
        // An originating refusal can precede the first positive quantum; a
        // later armed control must neither replace it nor create a footer.
        let original = format!("{:?}", baseline_result.unwrap_err());
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((trace.len(), cause.clone())),
            };
            let actual = invoke(&control).unwrap_err();
            assert_eq!(format!("{actual:?}"), original);
            assert_eq!(control.trace(), trace);
        }
    }
    let indices: Vec<_> = if sampled {
        let mut ids = vec![0, trace.len() - 1];
        ids.extend(
            trace
                .iter()
                .enumerate()
                .filter_map(|(at, (_, units))| (*units == 256).then_some(at)),
        );
        ids.sort_unstable();
        ids.dedup();
        ids
    } else {
        (0..trace.len()).collect()
    };
    for at in indices {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(invoke(&control),Err(PhysicalExpressionEffectsError::Control(actual)) if actual==cause),
                "at {at}: {cause:?}"
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
    trace
}
fn allow_ref() -> SemanticParameterRef {
    SemanticParameterRef {
        id: SemanticParameterId::new(71),
        expected_key: SemanticParameterKey::AllowThrowException,
    }
}
fn parameters() -> SemanticParameters {
    SemanticParameters::try_new([(
        allow_ref().id,
        SemanticParameterValue::AllowThrowException(false),
    )])
    .unwrap()
}
fn cast(
    builder: &mut FixtureBuilder,
    owner: NodeId,
    unit: TimeUnit,
    target: TimeUnit,
    nullable: bool,
) -> ExprId {
    let child = literal(
        builder,
        owner,
        ty(DataType::Timestamp(unit, None), false),
        checked::timestamp(i64::MAX),
    );
    builder.add(
        ty(DataType::Timestamp(target, None), nullable),
        ExprKind::Cast {
            expr: child,
            array_failure_mode: None,
            target: DataType::Timestamp(target, None),
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            allow_throw_exception: allow_ref(),
        },
    )
}

#[test]
fn expression_effects_actual_edges_schedule_reordered_sparse_uses_and_shared_definitions_child_first()
 {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let mut builder = FixtureBuilder::new();
    let owner = builder.node;
    let text = literal(
        &mut builder,
        owner,
        ty(DataType::Utf8, false),
        checked::utf8("Shared".into()),
    );
    let lower = call(
        &mut builder,
        owner,
        binding(&catalog, "lower", &[ty(DataType::Utf8, false)]),
        &[text],
    );
    let lower_ty = builder.definition(lower).ty.clone();
    let root = call(
        &mut builder,
        owner,
        binding(&catalog, "coalesce", &[lower_ty.clone(), lower_ty]),
        &[lower, lower],
    );
    value_roots(&mut builder, &[root]);
    let fixture = builder.seal(&catalog, policy());
    let roots = &fixture;

    let params = SemanticParameters::try_new([]).unwrap();
    let pools = ConstantPools::empty();
    let scopes = source_scopes(&fixture);
    let result = run(
        &fixture,
        &pools,
        &params,
        &scopes,
        &catalog,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(result.summaries.len(), 5);
    assert_eq!(result.calls.len(), 3);
    assert_eq!(summary(&result, roots, root), ExpressionEffects::PURE_VALUE);
    let lower_uses: Vec<_> = roots
        .flow()
        .uses()
        .iter()
        .filter(|(_, item)| item.definition == lower)
        .collect();
    assert_eq!(lower_uses.len(), 2);
    assert_ne!(
        lower_uses[0].1.context.region,
        lower_uses[1].1.context.region
    );
    for (&id, item) in lower_uses {
        assert_eq!(result.summaries[&id].context(), item.context);
    }
    let unit = fixture.validate_claims(result.calls);
    assert_eq!(unit.claims().len(), 3);
}

#[test]
fn expression_effects_primitive_cast_successful_null_and_errorful_cast_arithmetic_are_separate() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let mut builder = FixtureBuilder::new();
    let owner = builder.node;
    let successful_null = cast(
        &mut builder,
        owner,
        TimeUnit::Second,
        TimeUnit::Microsecond,
        true,
    );
    let row_error = cast(
        &mut builder,
        owner,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
        false,
    );
    let a = literal(
        &mut builder,
        owner,
        ty(DataType::Int64, false),
        checked::i64(i64::MAX),
    );
    let b = literal(
        &mut builder,
        owner,
        ty(DataType::Int64, false),
        checked::i64(1),
    );
    let add = builder.add(
        ty(DataType::Int64, true),
        ExprKind::Binary {
            left: a,
            op: BinaryOperator::Add,
            right: b,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            allow_throw_exception: Some(allow_ref()),
        },
    );
    value_roots(&mut builder, &[successful_null, row_error, add]);
    let fixture = builder.seal(&catalog, policy());
    let roots = &fixture;
    let scopes = source_scopes(&fixture);
    let pools = ConstantPools::empty();
    let params = parameters();
    let result = run(
        &fixture,
        &pools,
        &params,
        &scopes,
        &catalog,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        summary(&result, roots, successful_null),
        ExpressionEffects::PURE_VALUE
    );
    assert!(summary(&result, roots, row_error).may_raise_row_error);
    assert!(summary(&result, roots, add).may_raise_row_error);
    assert!(result.calls.is_empty());
    fixture.validate_claims(result.calls);
    assert!(fixture.definition(successful_null).ty.nullable());
    assert!(!fixture.definition(row_error).ty.nullable());
}

#[test]
fn expression_effects_case_and_boolean_guards_keep_rand_state_and_errorful_cast_children() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let mut builder = FixtureBuilder::new();
    let owner = builder.node;
    let boolean = ty(DataType::Boolean, false);
    let condition = literal(&mut builder, owner, boolean.clone(), checked::boolean(true));
    let false_ = literal(
        &mut builder,
        owner,
        boolean.clone(),
        checked::boolean(false),
    );
    let errorful = cast(
        &mut builder,
        owner,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
        false,
    );
    let isnull = builder.add(
        boolean.clone(),
        ExprKind::IsNull {
            expr: errorful,
            negated: false,
        },
    );
    let rand = call(&mut builder, owner, binding(&catalog, "rand", &[]), &[]);
    let rand_ty = builder.definition(rand).ty.clone();
    let zero = literal(
        &mut builder,
        owner,
        rand_ty.clone(),
        checked::f64_bits(0.0f64.to_bits()),
    );
    let compare = builder.add(
        ty(DataType::Boolean, rand_ty.nullable()),
        ExprKind::Binary {
            left: rand,
            op: BinaryOperator::Gt,
            right: zero,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            allow_throw_exception: None,
        },
    );
    let case = builder.add(
        ty(DataType::Boolean, rand_ty.nullable()),
        ExprKind::Case {
            operand: None,
            when_then: Box::from([(condition, isnull)]),
            else_expr: Some(compare),
        },
    );
    let root = builder.add(
        ty(DataType::Boolean, rand_ty.nullable()),
        ExprKind::Conjunction {
            args: Box::from([false_, case]),
        },
    );
    value_roots(&mut builder, &[root]);
    let fixture = builder.seal(&catalog, policy());
    let roots = &fixture;
    let scopes = source_scopes(&fixture);
    let pools = ConstantPools::empty();
    let params = parameters();
    let result = run(
        &fixture,
        &pools,
        &params,
        &scopes,
        &catalog,
        &Control::default(),
    )
    .unwrap();
    let effects = summary(&result, roots, root);
    assert_eq!(effects.value_stability, FunctionVolatility::Volatile);
    assert!(effects.may_raise_row_error);
    assert!(effects.has_instance_state);
    assert!(effects.observable_effects.rng_sampling);
    assert_eq!(result.calls.len(), 1);
    let case_id = invocation_use(roots, case);
    let case_use = &roots.flow().uses()[&case_id];
    assert_eq!(
        case_use.control,
        ControlShape::Case {
            simple: false,
            arms: 1,
            has_else: true
        }
    );
    for (ordinal, kind) in [
        (1, GuardKind::CaseThen { arm: 0 }),
        (2, GuardKind::CaseElse),
    ] {
        let child = &roots.flow().uses()[&case_use.arguments[ordinal]];
        let domain = roots.flow().regions()[&child.context.region];
        assert_eq!(domain.parent, Some(case_use.context.region));
        assert_eq!(
            domain.guard,
            Some(RegionGuard {
                owner: case_id,
                kind
            })
        );
    }
    let unit = fixture.validate_claims(result.calls);
    assert_eq!(
        unit.claims()[&invocation_use(roots, rand)]
            .effects
            .own_row_error,
        FunctionIntrinsicRowError::NoRowError
    );
}

#[test]
fn expression_effects_checked_constant_nonzero_ordinal_typed_null_and_full_source_refusal() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let source_type = ty(DataType::Utf8, true);
    let field = Arc::new(
        Field::new("original text", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([("provider.field-id".into(), "71".into())]),
        ),
    );
    let pool = ConstantPool::try_new(
        field.clone(),
        source_type.clone(),
        StringArray::from(vec![Some("unused"), Some("Chosen"), None]).to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    let mut pools = ConstantPools::empty();
    pools
        .insert(ConstantPoolId::new(u32::MAX), pool.clone())
        .unwrap();
    let build = |nullable| {
        let mut builder = FixtureBuilder::new();
        let owner = builder.node;
        builder.constants = pools.clone();
        let mut ids = Vec::new();
        for ordinal in [1, 2] {
            ids.push(builder.add(
                ty(DataType::Utf8, nullable),
                ExprKind::Constant(ConstantReference {
                    pool: ConstantPoolId::new(u32::MAX),
                    ordinal,
                }),
            ));
        }
        let root = call(
            &mut builder,
            owner,
            binding(
                &catalog,
                "coalesce",
                &[ty(DataType::Utf8, nullable), ty(DataType::Utf8, nullable)],
            ),
            &ids,
        );
        value_roots(&mut builder, &[root, ids[0], ids[1]]);
        let fixture = if nullable {
            builder.seal(&catalog, policy())
        } else {
            builder.seal_without_requests(&catalog, policy())
        };
        (fixture, root)
    };
    let (fixture, root) = build(true);
    let roots = &fixture;
    let scopes = source_scopes(&fixture);
    let params = SemanticParameters::try_new([]).unwrap();
    let result = run(
        &fixture,
        &pools,
        &params,
        &scopes,
        &catalog,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(summary(&result, roots, root), ExpressionEffects::PURE_VALUE);
    assert_eq!(result.calls.len(), 1);
    fixture.validate_claims(result.calls);
    let selected = pool.value(1).unwrap();
    let null = pool.value(2).unwrap();
    assert_eq!(selected.ordinal(), 1);
    assert_eq!(null.ordinal(), 2);
    assert_eq!(selected.value_type(), &source_type);
    assert_eq!(null.value_type(), &source_type);
    assert_eq!(null.pool().backing_identity(), pool.backing_identity());
    assert!(Arc::ptr_eq(null.pool().field_ref(), &field));
    assert_eq!(selected.pool().backing_identity(), pool.backing_identity());
    assert!(Arc::ptr_eq(selected.pool().field_ref(), &field));
    assert_eq!(
        selected
            .utf8_observed(CompilePhase::Validate, &Control::default())
            .unwrap(),
        Some("Chosen")
    );
    assert!(
        null.is_null_observed(CompilePhase::Validate, &Control::default())
            .unwrap()
    );
    prefixes(
        |control| run(&fixture, &pools, &params, &scopes, &catalog, control),
        true,
        false,
    );
    let (bad, _) = build(false);
    let bad_scopes = source_scopes(&bad);
    assert!(matches!(
        run(
            &bad,
            &pools,
            &params,
            &bad_scopes,
            &catalog,
            &Control::default()
        ),
        Err(PhysicalExpressionEffectsError::Request(_))
    ));
    prefixes(
        |control| run(&bad, &pools, &params, &bad_scopes, &catalog, control),
        false,
        false,
    );
}

#[test]
fn expression_effects_missing_spurious_and_stale_same_id_scalar_source_scopes_refuse() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (fixture, root) = lower(&catalog);
    let roots = &fixture;
    let mut scopes = source_scopes(&fixture);
    let pools = ConstantPools::empty();
    let params = SemanticParameters::try_new([]).unwrap();
    let id = root_use(roots, root);
    scopes.remove(&id);
    assert!(
        matches!(run(&fixture,&pools,&params,&scopes,&catalog,&Control::default()),Err(PhysicalExpressionEffectsError::MissingCallScope(actual)) if actual==id)
    );
    prefixes(
        |control| run(&fixture, &pools, &params, &scopes, &catalog, control),
        false,
        false,
    );
    let foreign = fixture.definition(root).clone();
    scopes.insert(
        id,
        PhysicalCallSourceScope {
            source: &foreign,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            environment: &[],
            proof_scope: CallProofScope::Region(roots.flow().uses()[&id].context.region),
        },
    );
    assert!(matches!(
        run(
            &fixture,
            &pools,
            &params,
            &scopes,
            &catalog,
            &Control::default()
        ),
        Err(PhysicalExpressionEffectsError::InvalidSource(
            "call scope does not loan this actual source invocation"
        ))
    ));
    prefixes(
        |control| run(&fixture, &pools, &params, &scopes, &catalog, control),
        false,
        false,
    );
    let mut valid = source_scopes(&fixture);
    valid.insert(
        ExpressionUseId::new(u32::MAX),
        PhysicalCallSourceScope {
            source: fixture.definition(root),
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            environment: &[],
            proof_scope: CallProofScope::Unconditional,
        },
    );
    assert!(matches!(
        run(
            &fixture,
            &pools,
            &params,
            &valid,
            &catalog,
            &Control::default()
        ),
        Err(PhysicalExpressionEffectsError::InvalidSource(_))
    ));
}

#[test]
fn expression_effects_primitive_invalid_nullable_and_parameter_sources_are_typed_refusals() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let build = |nullable| {
        let mut builder = FixtureBuilder::new();
        let owner = builder.node;
        let root = cast(
            &mut builder,
            owner,
            TimeUnit::Second,
            TimeUnit::Microsecond,
            nullable,
        );
        value_roots(&mut builder, &[root]);
        builder.seal(&catalog, policy())
    };
    let pools = ConstantPools::empty();
    let invalid = build(false);
    let scopes = source_scopes(&invalid);
    let params = parameters();
    assert!(matches!(
        run(
            &invalid,
            &pools,
            &params,
            &scopes,
            &catalog,
            &Control::default()
        ),
        Err(PhysicalExpressionEffectsError::Cast(
            CastPrepareError::TypeMismatch
        ))
    ));
    prefixes(
        |control| run(&invalid, &pools, &params, &scopes, &catalog, control),
        false,
        false,
    );
    let valid = build(true);
    let scopes = source_scopes(&valid);
    let empty = SemanticParameters::try_new([]).unwrap();
    let wrong = SemanticParameters::try_new([(
        allow_ref().id,
        SemanticParameterValue::TimeZone("UTC".into()),
    )])
    .unwrap();
    for params in [&empty, &wrong] {
        assert!(matches!(
            run(
                &valid,
                &pools,
                params,
                &scopes,
                &catalog,
                &Control::default()
            ),
            Err(PhysicalExpressionEffectsError::Parameter(_))
        ));
        prefixes(
            |control| run(&valid, &pools, params, &scopes, &catalog, control),
            false,
            false,
        );
    }
}

#[test]
fn expression_effects_runtime_window_requires_explicit_scope_and_keeps_its_lifecycle() {
    // Window is a relational call. No scalar definition, synthetic expression
    // scope, NodeExpressionEffects owner, or SQL credential is created here.
    use super::super::{
        expression_occurrences::author_physical_occurrences_observed,
        physical_fragment_effects::{
            AuthoredPhysicalFragmentEffects, PhysicalFragmentEffectsError,
            PhysicalFragmentEffectsInput, PhysicalRelationalCallSourceScope,
            author_physical_fragment_effects_observed,
        },
        physical_window_occurrences::{
            PhysicalWindowOccurrenceInput, prepare_physical_window_occurrence_observed,
        },
        physical_window_requests::author_physical_window_request_observed,
    };
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let mut builder = novarocks_physical_plan::FragmentBuilder::new(FragmentId::new(71));
    let leaf = NodeId::new(7);
    let owner = NodeId::new(901);
    builder
        .add_values(
            leaf,
            Box::from([Box::<[RootOrdinal]>::default()]),
            Box::default(),
        )
        .unwrap();
    let resolved =
        SqlFunctionCatalog::resolve_window_binding(&catalog, "rank", &[], &Control::default())
            .unwrap();
    let FunctionResultType::Scalar(result) = resolved.selected.result_type else {
        panic!("scalar result")
    };
    let function = BoundFunction {
        function_id: resolved.function_id,
        overload: resolved.selected.overload,
        kind: FunctionKind::Window,
        argument_types: resolved.selected.argument_types,
        result_type: result.clone(),
        legacy_metadata: Some(novarocks_expression_contract::LegacyBindingMetadata {
            volatility: resolved.semantics.volatility,
            argument_evaluation: resolved.semantics.argument_evaluation,
            failure_behavior: resolved.semantics.failure_behavior,
            intrinsic_row_error: resolved.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        }),
    };
    let output = builder
        .add_value(
            result,
            ValueOrigin::NodeOutput {
                node: owner,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .add_row_widening(
            owner,
            leaf,
            Box::from([output]),
            NodeKind::Window(WindowSpec {
                partition_by: Box::default(),
                order_by: Box::default(),
                calls: Box::from([WindowCallSpec {
                    binding: function,
                    distinct: false,
                    arguments: Box::default(),
                    order_by: Box::default(),
                    frame: None,
                    ignore_nulls: false,
                    aggregate_binding: None,
                    output,
                }]),
            }),
        )
        .unwrap();
    let fragment = builder
        .finish_structure(
            owner,
            FragmentSink::Noop,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            PlanLimits::FROZEN,
            &Control::default(),
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    assert!(
        fragment
            .nodes()
            .values()
            .all(|node| node.expressions.is_none())
    );
    let occurrences =
        author_physical_occurrences_observed(&fragment, &catalog, &Control::default()).unwrap();
    let [(site, context)] = occurrences.relational_contexts.as_slice() else {
        panic!("one actual Window call")
    };
    let site = *site;
    let context = *context;
    assert_eq!(
        site,
        PhysicalCallSite::Window {
            node: owner,
            call: 0
        }
    );
    let source = &fragment.nodes()[&owner];
    let NodeKind::Window(spec) = &source.kind else {
        panic!("actual Window node")
    };
    let call = &spec.calls[0];
    let pools = ConstantPools::empty();
    let params = SemanticParameters::try_new([]).unwrap();
    let effects = BTreeMap::new();
    let mut scopes = BTreeMap::new();
    let run = |scopes: &BTreeMap<PhysicalCallSite, PhysicalRelationalCallSourceScope<'_>>,
               control: &Control|
     -> Result<AuthoredPhysicalFragmentEffects, PhysicalFragmentEffectsError> {
        author_physical_fragment_effects_observed(
            PhysicalFragmentEffectsInput {
                fragment: &fragment,
                occurrences: &occurrences,
                constants: &pools,
                parameters: &params,
                literal_policy: policy(),
                node_effects: &effects,
                relational_scopes: scopes,
            },
            &catalog,
            control,
        )
    };
    // This is the same three-cause exact-prefix contract, now at its real
    // relational author rather than the retired Window expression lifecycle.
    let prefixes = |scopes: &BTreeMap<PhysicalCallSite, PhysicalRelationalCallSourceScope<'_>>,
                    success: bool| {
        let baseline = Control::default();
        let baseline_result = run(scopes, &baseline);
        assert_eq!(baseline_result.is_ok(), success);
        let trace = baseline.trace();
        assert_eq!(trace.first(), Some(&(PHASE, 0)));
        if success {
            assert!(trace.len() > 1);
        } else {
            let original = format!("{:?}", baseline_result.unwrap_err());
            for cause in CAUSES {
                let control = Control {
                    trace: Mutex::default(),
                    refusal: Some((trace.len(), cause.clone())),
                };
                assert_eq!(
                    format!("{:?}", run(scopes, &control).unwrap_err()),
                    original
                );
                assert_eq!(control.trace(), trace);
            }
        }
        for at in 0..trace.len() {
            for cause in CAUSES {
                let control = Control {
                    trace: Mutex::default(),
                    refusal: Some((at, cause.clone())),
                };
                assert!(
                    matches!(run(scopes, &control), Err(PhysicalFragmentEffectsError::Control(actual)) if actual == cause),
                    "at {at}: {cause:?}"
                );
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    };
    assert!(
        matches!(run(&scopes, &Control::default()), Err(PhysicalFragmentEffectsError::MissingScope(actual)) if actual == site)
    );
    prefixes(&scopes, false);
    scopes.insert(
        site,
        PhysicalRelationalCallSourceScope {
            source,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            environment: &[],
            proof_scope: CallProofScope::Relation(context.identity),
        },
    );
    let authored = run(&scopes, &Control::default()).unwrap();
    assert_eq!(authored.calls.entries().len(), 1);
    assert_eq!(
        authored.calls.entries()[&site].effects.instance_state,
        novarocks_type_contract::FunctionInstanceState::WindowPartition
    );
    authored
        .calls
        .validate_fragment(&fragment, &Control::default())
        .unwrap();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
    let request =
        author_physical_window_request_observed(call, source, &pools, policy(), &mut work).unwrap();
    let fresh = prepare_physical_window_occurrence_observed(
        PhysicalWindowOccurrenceInput {
            node: source,
            site,
            source: call,
            request: &request,
            occurrences: &occurrences,
            child_effects: None,
            parameters: &params,
            environment: &[],
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Relation(context.identity),
        },
        &catalog,
        &mut work,
    )
    .unwrap();
    assert!(
        fresh
            .preparation
            .effects()
            .for_use(context)
            .unwrap()
            .has_instance_state
    );
    assert_eq!(&fresh.frozen, &authored.calls.entries()[&site]);
    work.finish().unwrap();
    prefixes(&scopes, true);
}

#[test]
fn expression_effects_whole_success_prefixes_and_wide_actual_traversal_sampling() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (fixture, _) = lower(&catalog);
    let scopes = source_scopes(&fixture);
    let pools = ConstantPools::empty();
    let params = SemanticParameters::try_new([]).unwrap();
    let trace = prefixes(
        |control| run(&fixture, &pools, &params, &scopes, &catalog, control),
        true,
        false,
    );
    assert_eq!(trace.last().unwrap().0, PHASE);
    let mut builder = FixtureBuilder::new();
    let owner = builder.node;
    let boolean = ty(DataType::Boolean, false);
    let mut args = Vec::new();
    for _ in 0..320 {
        args.push(literal(
            &mut builder,
            owner,
            boolean.clone(),
            checked::boolean(true),
        ));
    }
    let root = builder.add(
        boolean,
        ExprKind::Conjunction {
            args: args.into_boxed_slice(),
        },
    );
    value_roots(&mut builder, &[root]);
    let fixture = builder.seal(&catalog, policy());
    let roots = &fixture;
    let scopes = source_scopes(&fixture);
    let result = run(
        &fixture,
        &pools,
        &params,
        &scopes,
        &catalog,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(result.summaries.len(), 321);
    assert!(result.calls.is_empty());
    assert_eq!(summary(&result, roots, root), ExpressionEffects::PURE_VALUE);
    fixture.validate_claims(result.calls);
    let trace = prefixes(
        |control| run(&fixture, &pools, &params, &scopes, &catalog, control),
        true,
        true,
    );
    assert!(trace.contains(&(PHASE, 256)));
}

#[test]
fn standalone_sealed_sources_reject_foreign_environment_loans_at_the_original_gate() {
    use super::super::contract_lowering::sealed_standalone_source_for_effects;
    use super::super::unit_source::ScalarUnitOwner;
    let first = sealed_standalone_source_for_effects();
    let second = sealed_standalone_source_for_effects();
    assert_ne!(first.brand, second.brand);
    assert!(!std::ptr::eq(
        first.functions.as_ref(),
        second.functions.as_ref()
    ));
    let loan = first.loan();
    let scopes = BTreeMap::new();
    let input = || PhysicalExpressionEffectsInput {
        owner: ScalarUnitOwner::Standalone(first.brand.clone()),
        unit: &first.sealed,
        constants: &first.constants,
        parameters: &first.parameters,
        literal_policy: crate::constant::test_constant_policy(),
        call_scopes: &scopes,
    };
    let control = Control::default();
    let positive = compose_expression_effects_observed(
        input(),
        Some(&loan),
        first.functions.as_ref(),
        &control,
    )
    .unwrap();
    assert!(positive.calls.is_empty());
    assert_eq!(positive.summaries.len(), first.sealed.uses().len());
    for changed in 0..4 {
        let mut actual = input();
        let functions = match changed {
            0 => {
                actual.unit = &second.sealed;
                first.functions.as_ref()
            }
            1 => {
                actual.constants = &second.constants;
                first.functions.as_ref()
            }
            2 => {
                actual.parameters = &second.parameters;
                first.functions.as_ref()
            }
            3 => second.functions.as_ref(),
            _ => unreachable!(),
        };
        assert!(matches!(
            compose_expression_effects_observed(
                actual,
                Some(&loan),
                functions,
                &Control::default()
            ),
            Err(PhysicalExpressionEffectsError::InvalidSource(
                "SQL scalar composition loans a foreign sealed unit, pool, parameters or catalogue"
            ))
        ));
    }
}
