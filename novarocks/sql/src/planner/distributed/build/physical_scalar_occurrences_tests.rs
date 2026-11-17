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

use super::super::{
    expression_component_test_fixture::{FixtureBuilder, UnitFixture},
    physical_expression_effects::{
        PhysicalCallSourceScope, PhysicalExpressionEffectsInput,
        author_physical_expression_effects_observed,
    },
    physical_scalar_requests::author_physical_scalar_request_observed,
    unit_source::UnitDefinitions,
};
use super::*;
use crate::functions::build_builtin_engine_function_catalog;
use crate::test_constants as checked;
use arrow::datatypes::{DataType, Field};
use novarocks_functions::{
    ConstantPolicy, EngineFunctionCatalog, FunctionArgument, FunctionResolutionError,
    FunctionResultType, FunctionSpecializationFailure, ResolvedAggregateSignature,
};
use novarocks_type_contract::{
    CompilePhase, EvaluationDemand, EvaluationRegionId, ExpressionEffectContext, ExpressionEffects,
    ExpressionEvaluationRegion, ExpressionInvocation, FunctionArgumentEvaluation,
    FunctionFailureBehavior, FunctionId, FunctionIntrinsicRowError, FunctionValueType,
    FunctionVolatility, GuardKind, PureCompileControl, SemanticParameterId, SemanticParameterKey,
    SemanticParameterValue,
};
use std::sync::Mutex;
use {
    novarocks_expression_contract::{BoundExprSet, BoundFunction, RootOrdinal},
    novarocks_physical_plan::{ConstantPools, NodeId},
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
// Generic malformed-flow tests borrow only real definition membership. This
// supplies no control shape, SQL provenance, installed capability or new graph.
struct Membership<'a>(UnitDefinitions<'a>);
impl novarocks_type_contract::ExpressionDefinitionMembership<ExprId> for Membership<'_> {
    fn definition_count(&self) -> usize {
        self.0.len()
    }
    fn contains_definition(&self, id: ExprId) -> bool {
        self.0.definition(id).is_some()
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
fn shared_if(catalog: &EngineFunctionCatalog) -> (UnitFixture, ExprId, [RootOrdinal; 2]) {
    let mut builder = FixtureBuilder::new();
    let owner = builder.node;
    let boolean = ty(DataType::Boolean, false);
    let a = literal(&mut builder, owner, boolean.clone(), checked::boolean(true));
    let b = literal(
        &mut builder,
        owner,
        boolean.clone(),
        checked::boolean(false),
    );
    let mut function = binding(catalog, "if", &[boolean.clone(), boolean.clone(), boolean]);
    // These contradictory legacy fields are not authority for the real owner.
    let legacy = function
        .legacy_metadata
        .as_mut()
        .expect("actual legacy fixture");
    legacy.argument_evaluation = FunctionArgumentEvaluation::Eager;
    legacy.volatility = FunctionVolatility::Volatile;
    legacy.intrinsic_row_error = FunctionIntrinsicRowError::MayRaise;
    legacy.failure_behavior = FunctionFailureBehavior::ReturnsNull;
    let root = call(&mut builder, owner, function, &[a, b, a]);
    let truth = builder.root(root, EvaluationDemand::TruthOnly);
    let value = builder.root(root, EvaluationDemand::Value);
    (builder.seal(catalog, policy()), root, [truth, value])
}
fn request<'a>(source: &'a ExprNode, fixture: &UnitFixture) -> AuthoredPhysicalScalarRequest<'a> {
    request_from(source, fixture.definitions(), &fixture.constants)
}
fn request_from<'a>(
    source: &'a ExprNode,
    definitions: UnitDefinitions<'_>,
    pools: &ConstantPools,
) -> AuthoredPhysicalScalarRequest<'a> {
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
    let result =
        author_physical_scalar_request_observed(source, definitions, pools, policy(), &mut work)
            .unwrap();
    work.finish().unwrap();
    result
}

fn leaves(
    flow: &ExpressionControlFlow<ExprId>,
) -> BTreeMap<ExpressionUseId, ScopedExpressionEffects> {
    flow.uses()
        .iter()
        .filter(|(_, item)| item.arguments.is_empty())
        .map(|(&id, item)| (id, ScopedExpressionEffects::pure_value(item.context)))
        .collect()
}
fn input<'a>(
    source: &'a ExprNode,
    request: &'a AuthoredPhysicalScalarRequest<'a>,
    flow: &'a ExpressionControlFlow<ExprId>,
    id: ExpressionUseId,
    children: &'a BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    parameters: &'a SemanticParameters,
    policy: DecimalOverflowPolicy,
) -> PhysicalScalarOccurrenceInput<'a> {
    PhysicalScalarOccurrenceInput {
        definitions: None,
        temporal_source: None,
        source,
        request,
        flow,
        use_id: id,
        child_effects: children,
        parameters,
        environment: &[],
        decimal_overflow_policy: policy,
        proof_scope: CallProofScope::Region(flow.uses()[&id].context.region),
    }
}
fn run(
    input: PhysicalScalarOccurrenceInput<'_>,
    catalog: &dyn SqlFunctionCatalog,
    control: &Control,
) -> Result<FreshPhysicalScalarOccurrence, PhysicalScalarOccurrenceError> {
    let mut work = CompileCheckpoints::try_new(control, PHASE)?;
    let result = prepare_physical_scalar_occurrence_observed(input, catalog, &mut work);
    if matches!(&result, Err(PhysicalScalarOccurrenceError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn prefixes(
    mut invoke: impl FnMut(
        &Control,
    ) -> Result<FreshPhysicalScalarOccurrence, PhysicalScalarOccurrenceError>,
    success: bool,
    sampled: bool,
) -> Vec<(CompilePhase, u32)> {
    let baseline = Control::default();
    assert_eq!(invoke(&baseline).is_ok(), success);
    let trace = baseline.trace();
    assert_eq!(trace.first(), Some(&(PHASE, 0)));
    assert!(trace.len() > 1);
    let indices: Vec<_> = if sampled {
        vec![0, trace.len() / 2, trace.len() - 1]
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
                matches!(invoke(&control),Err(PhysicalScalarOccurrenceError::Control(actual)) if actual==cause),
                "at {at}: {cause:?}"
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
    trace
}
fn root_use(flow: &ExpressionControlFlow<ExprId>, definition: ExprId) -> ExpressionUseId {
    *flow
        .uses()
        .iter()
        .find(|(_, item)| item.definition == definition)
        .unwrap()
        .0
}
fn prepare_all(
    fixture: &UnitFixture,
    catalog: &EngineFunctionCatalog,
    decimal_policy: DecimalOverflowPolicy,
) -> (
    BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    BoundExprSet,
) {
    let params = SemanticParameters::try_new([]).unwrap();
    let scopes = fixture
        .flow()
        .uses()
        .iter()
        .filter_map(|(&id, item)| {
            let source = fixture.definition(item.definition);
            matches!(source.kind, ExprKind::FunctionCall { .. }).then_some((
                id,
                PhysicalCallSourceScope {
                    source,
                    decimal_overflow_policy: decimal_policy,
                    environment: &[],
                    proof_scope: CallProofScope::Region(item.context.region),
                },
            ))
        })
        .collect();
    // Reuse the existing direct-component composer, whose original owner walks
    // actual child edges. Numeric use order has no dependency meaning.
    let authored = author_physical_expression_effects_observed(
        PhysicalExpressionEffectsInput {
            owner: crate::planner::distributed::build::unit_source::ScalarUnitOwner::Node(
                fixture.node,
            ),
            unit: &fixture.sealed,
            constants: &fixture.constants,
            parameters: &params,
            literal_policy: policy(),
            call_scopes: &scopes,
        },
        catalog,
        &Control::default(),
    )
    .unwrap();
    let mut summaries = authored.summaries.clone();
    let mut claims = BTreeMap::new();
    // The real composer's verified summaries supply every child. Re-enter the
    // original occurrence leaf independently per use to preserve the exact
    // selected Arc assertion without duplicating dependency traversal.
    for (&id, item) in fixture.flow().uses() {
        let source = fixture.definition(item.definition);
        if !matches!(source.kind, ExprKind::FunctionCall { .. }) {
            continue;
        }
        let req = request(source, fixture);
        let fresh = run(
            input(
                source,
                &req,
                fixture.flow(),
                id,
                &authored.summaries,
                &params,
                decimal_policy,
            ),
            catalog,
            &Control::default(),
        )
        .unwrap();
        assert!(Arc::ptr_eq(
            req.selected(),
            fresh.preparation.call_contract().selected_owner()
        ));
        assert_eq!(fresh.claim.context, item.context);
        assert_eq!(fresh.claim, authored.calls[&id]);
        let effects = fresh.preparation.effects().for_use(item.context).unwrap();
        let summary = ScopedExpressionEffects::primitive(item.context, effects);
        assert_eq!(summary, authored.summaries[&id]);
        summaries.insert(id, summary);
        assert!(claims.insert(id, fresh.claim).is_none());
    }
    assert_eq!(claims.len(), authored.calls.len());
    let unit = fixture.validate_claims(claims);
    (summaries, unit)
}

#[test]
fn scalar_occurrences_shared_if_keeps_two_demands_guards_policies_and_same_selected_owner() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (fragment, root, roots) = shared_if(&catalog);
    let source = fragment.definition(root);
    let req = request(source, &fragment);
    let children = leaves(fragment.flow());
    let params = SemanticParameters::try_new([]).unwrap();
    let truth = fragment.root_use(roots[0]);
    let value = fragment.root_use(roots[1]);
    assert_ne!(truth, value);
    let mut calls = BTreeMap::new();
    for (id, demand, policy) in [
        (
            truth,
            EvaluationDemand::TruthOnly,
            DecimalOverflowPolicy::ReportError,
        ),
        (
            value,
            EvaluationDemand::Value,
            DecimalOverflowPolicy::OutputNull,
        ),
    ] {
        let invocation = &fragment.flow().uses()[&id];
        assert_eq!(invocation.definition, root);
        assert_eq!(invocation.control, ControlShape::If);
        assert_eq!(invocation.context.demand, demand);
        for (ordinal, kind) in [(1, GuardKind::IfThen), (2, GuardKind::IfElse)] {
            let child = &fragment.flow().uses()[&invocation.arguments[ordinal]];
            let domain = fragment.flow().regions()[&child.context.region];
            assert_eq!(domain.parent, Some(invocation.context.region));
            assert_eq!(domain.guard.unwrap().owner, id);
            assert_eq!(domain.guard.unwrap().kind, kind);
            assert_eq!(child.context.demand, demand);
        }
        let result = run(
            input(
                source,
                &req,
                fragment.flow(),
                id,
                &children,
                &params,
                policy,
            ),
            &catalog,
            &Control::default(),
        )
        .unwrap();
        assert!(Arc::ptr_eq(
            req.selected(),
            result.preparation.call_contract().selected_owner()
        ));
        assert_eq!(
            result
                .preparation
                .effects()
                .for_use(invocation.context)
                .unwrap(),
            ExpressionEffects::PURE_VALUE
        );
        assert_eq!(result.claim.decimal_overflow_policy, policy);
        assert_eq!(
            result.claim.effects.value_stability,
            FunctionVolatility::Immutable
        );
        assert_eq!(
            result.claim.effects.own_row_error,
            FunctionIntrinsicRowError::NoRowError
        );
        assert_eq!(
            result.claim.effects.proof_scope,
            CallProofScope::Region(invocation.context.region)
        );
        assert!(calls.insert(id, result.claim).is_none());
    }
    let calls = fragment.validate_claims(calls);
    assert_eq!(calls.claims().len(), 2);
}

#[test]
fn scalar_occurrences_actual_coalesce_preserves_rng_instance_and_decimal_row_error_children() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let mut builder = FixtureBuilder::new();
    let owner = builder.node;
    let rand = call(&mut builder, owner, binding(&catalog, "rand", &[]), &[]);
    let rand_ty = builder.definition(rand).ty.clone();
    let coalesce_rand = call(
        &mut builder,
        owner,
        binding(&catalog, "coalesce", &[rand_ty.clone(), rand_ty]),
        &[rand, rand],
    );
    let decimal = ty(DataType::Decimal128(38, 3), true);
    let arg = literal(
        &mut builder,
        owner,
        decimal.clone(),
        checked::decimal128(12345),
    );
    let round = call(
        &mut builder,
        owner,
        binding(&catalog, "round", &[decimal]),
        &[arg],
    );
    let round_ty = builder.definition(round).ty.clone();
    let coalesce_round = call(
        &mut builder,
        owner,
        binding(&catalog, "coalesce", &[round_ty.clone(), round_ty]),
        &[round, round],
    );
    value_roots(&mut builder, &[coalesce_rand, coalesce_round]);
    let fragment = builder.seal(&catalog, policy());
    for policy in [
        DecimalOverflowPolicy::ReportError,
        DecimalOverflowPolicy::OutputNull,
    ] {
        let (summaries, calls) = prepare_all(&fragment, &catalog, policy);
        let rand_use = root_use(fragment.flow(), coalesce_rand);
        let round_use = root_use(fragment.flow(), coalesce_round);
        let rng = summaries[&rand_use]
            .for_use(fragment.flow().uses()[&rand_use].context)
            .unwrap();
        assert_eq!(rng.value_stability, FunctionVolatility::Volatile);
        assert!(rng.has_instance_state);
        assert!(rng.observable_effects.rng_sampling);
        let decimal = summaries[&round_use]
            .for_use(fragment.flow().uses()[&round_use].context)
            .unwrap();
        assert_eq!(
            decimal.may_raise_row_error,
            policy == DecimalOverflowPolicy::ReportError
        );
        // Own COALESCE facts do not absorb the child's mutable instance; the
        // composed scoped summary above retains it for the next parent.
        assert_eq!(
            calls.claims()[&rand_use].effects.own_row_error,
            FunctionIntrinsicRowError::NoRowError
        );
        assert_eq!(calls.claims().len(), 6);
    }
}

#[test]
fn scalar_occurrences_reject_foreign_static_source_missing_use_and_wrong_definition() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (fragment, root) = lower(&catalog);
    let flow = fragment.flow();
    let id = root_use(flow, root);
    let source = fragment.definition(root);
    let req = request(source, &fragment);
    let cloned = source.clone();
    let foreign = request(&cloned, &fragment);
    let children = leaves(flow);
    let params = SemanticParameters::try_new([]).unwrap();
    assert!(matches!(
        run(
            input(
                source,
                &foreign,
                flow,
                id,
                &children,
                &params,
                DecimalOverflowPolicy::ReportError
            ),
            &catalog,
            &Control::default()
        ),
        Err(PhysicalScalarOccurrenceError::InvalidSource(
            "scalar occurrence and static request have different source definitions"
        ))
    ));
    let mut missing = input(
        source,
        &req,
        flow,
        id,
        &children,
        &params,
        DecimalOverflowPolicy::ReportError,
    );
    missing.use_id = ExpressionUseId::new(u32::MAX);
    assert!(
        matches!(run(missing,&catalog,&Control::default()),Err(PhysicalScalarOccurrenceError::MissingUse(actual)) if actual==ExpressionUseId::new(u32::MAX))
    );
    let child = flow.uses()[&id].arguments[0];
    assert!(matches!(
        run(
            input(
                source,
                &req,
                flow,
                child,
                &children,
                &params,
                DecimalOverflowPolicy::ReportError
            ),
            &catalog,
            &Control::default()
        ),
        Err(PhysicalScalarOccurrenceError::InvalidSource(_))
    ));
    prefixes(
        |control| {
            run(
                input(
                    source,
                    &foreign,
                    flow,
                    id,
                    &children,
                    &params,
                    DecimalOverflowPolicy::ReportError,
                ),
                &catalog,
                control,
            )
        },
        false,
        false,
    );
    // A generically valid use graph is not proof that its child definition is
    // the actual ordered source edge. This child is a leaf use of the wrong
    // existing definition; no cycle is introduced into the invocation graph.
    let mut invocations: Vec<_> = flow.uses().values().cloned().collect();
    invocations
        .iter_mut()
        .find(|item| item.context.use_id == child)
        .unwrap()
        .definition = root;
    let wrong = ExpressionControlFlow::try_new(
        flow.regions().values().copied().collect(),
        invocations,
        &Membership(fragment.definitions()),
        PHASE,
        &Control::default(),
    )
    .unwrap();
    let wrong_children = leaves(&wrong);
    assert!(matches!(
        run(
            input(
                source,
                &req,
                &wrong,
                id,
                &wrong_children,
                &params,
                DecimalOverflowPolicy::ReportError
            ),
            &catalog,
            &Control::default()
        ),
        Err(PhysicalScalarOccurrenceError::InvalidSource(
            "ordered scalar argument use has a different source definition"
        ))
    ));
    prefixes(
        |control| {
            run(
                input(
                    source,
                    &req,
                    &wrong,
                    id,
                    &wrong_children,
                    &params,
                    DecimalOverflowPolicy::ReportError,
                ),
                &catalog,
                control,
            )
        },
        false,
        false,
    );
}

#[test]
fn scalar_occurrences_missing_or_foreign_guarded_child_summary_is_not_erased() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (fragment, root, _) = shared_if(&catalog);
    let flow = fragment.flow();
    let id = root_use(flow, root);
    let source = fragment.definition(root);
    let req = request(source, &fragment);
    let params = SemanticParameters::try_new([]).unwrap();
    let child = flow.uses()[&id].arguments[1];
    let mut children = leaves(flow);
    children.remove(&child);
    assert!(
        matches!(run(input(source,&req,flow,id,&children,&params,DecimalOverflowPolicy::ReportError),&catalog,&Control::default()),Err(PhysicalScalarOccurrenceError::MissingChildEffects(actual)) if actual==child)
    );
    prefixes(
        |control| {
            run(
                input(
                    source,
                    &req,
                    flow,
                    id,
                    &children,
                    &params,
                    DecimalOverflowPolicy::ReportError,
                ),
                &catalog,
                control,
            )
        },
        false,
        false,
    );
    children.insert(
        child,
        ScopedExpressionEffects::pure_value(flow.uses()[&id].context),
    );
    assert!(matches!(
        run(
            input(
                source,
                &req,
                flow,
                id,
                &children,
                &params,
                DecimalOverflowPolicy::ReportError
            ),
            &catalog,
            &Control::default()
        ),
        Err(PhysicalScalarOccurrenceError::Effects(
            EffectContractError::ProofScopeMismatch
        ))
    ));
    prefixes(
        |control| {
            run(
                input(
                    source,
                    &req,
                    flow,
                    id,
                    &children,
                    &params,
                    DecimalOverflowPolicy::ReportError,
                ),
                &catalog,
                control,
            )
        },
        false,
        false,
    );
}

#[test]
fn scalar_occurrences_exact_environment_parameters_and_domain_proof_are_explicit() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (fragment, root) = lower(&catalog);
    let flow = fragment.flow();
    let id = root_use(flow, root);
    let source = fragment.definition(root);
    let req = request(source, &fragment);
    let children = leaves(flow);
    let empty = SemanticParameters::try_new([]).unwrap();
    let parameter = SemanticParameterId::new(u32::MAX);
    let refs = [SemanticParameterRef {
        id: parameter,
        expected_key: SemanticParameterKey::TimeZone,
    }];
    let valid =
        SemanticParameters::try_new([(parameter, SemanticParameterValue::TimeZone("UTC".into()))])
            .unwrap();
    let wrong = SemanticParameters::try_new([(
        parameter,
        SemanticParameterValue::AllowThrowException(true),
    )])
    .unwrap();
    for params in [&empty, &valid, &wrong] {
        let mut call = input(
            source,
            &req,
            flow,
            id,
            &children,
            params,
            DecimalOverflowPolicy::ReportError,
        );
        call.environment = &refs;
        assert!(matches!(
            run(call, &catalog, &Control::default()),
            Err(PhysicalScalarOccurrenceError::Occurrence(
                ExpressionOccurrenceError::Function(_)
            ))
        ));
    }
    prefixes(
        |control| {
            let mut call = input(
                source,
                &req,
                flow,
                id,
                &children,
                &valid,
                DecimalOverflowPolicy::ReportError,
            );
            call.environment = &refs;
            run(call, &catalog, control)
        },
        false,
        false,
    );
    let mut call = input(
        source,
        &req,
        flow,
        id,
        &children,
        &empty,
        DecimalOverflowPolicy::ReportError,
    );
    call.proof_scope = CallProofScope::Region(EvaluationRegionId::new(u32::MAX));
    assert!(matches!(
        run(call, &catalog, &Control::default()),
        Err(PhysicalScalarOccurrenceError::Occurrence(
            ExpressionOccurrenceError::Function(_)
        ))
    ));
    let result = run(
        input(
            source,
            &req,
            flow,
            id,
            &children,
            &empty,
            DecimalOverflowPolicy::OutputNull,
        ),
        &catalog,
        &Control::default(),
    )
    .unwrap();
    assert!(result.claim.effects.environment.is_empty());
    assert_eq!(
        result.claim.decimal_overflow_policy,
        DecimalOverflowPolicy::OutputNull
    );
}

#[test]
fn scalar_occurrences_typeof_unknown_and_metadata_only_catalog_never_supply_owner_facts() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (fragment, root) = lower(&catalog);
    let flow = fragment.flow();
    let id = root_use(flow, root);
    let source = fragment.definition(root);
    let children = leaves(flow);
    let params = SemanticParameters::try_new([]).unwrap();
    // A metadata-only SQL snapshot has no installation authority, even though
    // this source/request was resolved against the real builtin catalogue.
    #[derive(Clone, Debug)]
    struct MetadataOnly;
    impl SqlFunctionCatalog for MetadataOnly {
        fn snapshot(&self) -> Arc<dyn SqlFunctionCatalog> {
            Arc::new(self.clone())
        }
        fn resolve_scalar_signature(
            &self,
            _: &str,
            _: &[DataType],
            _: &dyn PureCompileControl,
        ) -> Result<crate::functions::ResolvedScalarFunction, crate::functions::ResolveError>
        {
            panic!("no name resolution fallback")
        }
        fn resolve_aggregate_signature(
            &self,
            _: &str,
            _: &[DataType],
            _: &dyn PureCompileControl,
        ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
            panic!("no aggregate resolution fallback")
        }
        fn resolve_aggregate_trusted(
            &self,
            _: &str,
            _: &[DataType],
            _: &dyn PureCompileControl,
        ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
            panic!("no trusted resolution fallback")
        }
        fn contains_aggregate(&self, _: &str) -> bool {
            panic!("no discovery fallback")
        }
        fn volatility(&self, _: &str) -> FunctionVolatility {
            panic!("no legacy effect inference")
        }
    }
    let req = request(source, &fragment);
    prefixes(
        |control| {
            run(
                input(
                    source,
                    &req,
                    flow,
                    id,
                    &children,
                    &params,
                    DecimalOverflowPolicy::ReportError,
                ),
                &MetadataOnly,
                control,
            )
        },
        false,
        false,
    );
    let mut unknown = source.clone();
    let ExprKind::FunctionCall { function, .. } = &mut unknown.kind else {
        unreachable!()
    };
    function.function_id = FunctionId::try_new("builtin.scalar/typeof/v1").unwrap();
    let unknown_req = request(&unknown, &fragment);
    let result = run(
        input(
            &unknown,
            &unknown_req,
            flow,
            id,
            &children,
            &params,
            DecimalOverflowPolicy::ReportError,
        ),
        &catalog,
        &Control::default(),
    );
    assert!(matches!(
        result,
        Err(PhysicalScalarOccurrenceError::Occurrence(
            ExpressionOccurrenceError::Function(FunctionSpecializationFailure::Binding(
                novarocks_functions::FunctionBindingError::UnknownFunction
            ))
        ))
    ));
    prefixes(
        |control| {
            run(
                input(
                    &unknown,
                    &unknown_req,
                    flow,
                    id,
                    &children,
                    &params,
                    DecimalOverflowPolicy::ReportError,
                ),
                &catalog,
                control,
            )
        },
        false,
        false,
    );
    // ARRAY_MAP is an actual metadata declaration, with no installed pure HOF
    // attachment. The static fixture below intentionally does not claim full
    // lexical/Fragment admission: base-owner refusal precedes HOF shape use.
    let integer = ty(DataType::Int64, false);
    let list = ty(
        DataType::List(Arc::new(Field::new("item", DataType::Int64, false))),
        true,
    );
    let arguments = [
        FunctionArgument::Lambda {
            parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
                integer.clone(),
            ])),
            result_type: integer.clone(),
        },
        FunctionArgument::Value {
            value_type: list.clone(),
            constant: None,
        },
    ];
    let resolved = SqlFunctionCatalog::resolve_scalar_binding(
        &catalog,
        "array_map",
        &arguments,
        &Control::default(),
    )
    .unwrap();
    let FunctionResultType::Scalar(result) = resolved.selected.result_type else {
        panic!("scalar result")
    };
    let function = BoundFunction {
        function_id: resolved.function_id,
        overload: resolved.selected.overload,
        kind: resolved.kind,
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
    let mut draft = FixtureBuilder::new();
    let constant_kind = checked::insert(&mut draft.constants, checked::i64(7)(&integer));
    let body = draft.add(integer.clone(), constant_kind);
    let lambda = draft.add(
        integer.clone(),
        ExprKind::Lambda {
            parameter_types: Box::from([integer]),
            body,
        },
    );
    let value = draft.add(
        list,
        ExprKind::Value(novarocks_physical_plan::ValueId::new(3)),
    );
    let source_id = draft.add(
        result,
        ExprKind::FunctionCall {
            function,
            args: Box::from([lambda, value]),
        },
    );
    let source = draft.definition(source_id);
    let req = request_from(source, draft.definitions(), &draft.constants);
    let domain = EvaluationRegionId::new(0);
    let context = |id| ExpressionEffectContext {
        use_id: ExpressionUseId::new(id),
        region: domain,
        demand: EvaluationDemand::Value,
    };
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationRegion {
            id: domain,
            parent: None,
            guard: None,
        }],
        vec![
            ExpressionInvocation {
                context: context(0),
                definition: source.id,
                control: ControlShape::Eager,
                arguments: Box::from([ExpressionUseId::new(1), ExpressionUseId::new(2)]),
            },
            ExpressionInvocation {
                context: context(1),
                definition: lambda,
                control: ControlShape::Eager,
                arguments: Box::default(),
            },
            ExpressionInvocation {
                context: context(2),
                definition: value,
                control: ControlShape::Eager,
                arguments: Box::default(),
            },
        ],
        &Membership(draft.definitions()),
        PHASE,
        &Control::default(),
    )
    .unwrap();
    let children = leaves(&flow);
    assert!(matches!(
        run(
            input(
                source,
                &req,
                &flow,
                flow.root_use_ids()[0],
                &children,
                &params,
                DecimalOverflowPolicy::ReportError
            ),
            &catalog,
            &Control::default()
        ),
        Err(PhysicalScalarOccurrenceError::Occurrence(
            ExpressionOccurrenceError::Function(
                FunctionSpecializationFailure::MissingPureImplementation(_)
            )
        ))
    ));
    prefixes(
        |control| {
            run(
                input(
                    source,
                    &req,
                    &flow,
                    flow.root_use_ids()[0],
                    &children,
                    &params,
                    DecimalOverflowPolicy::ReportError,
                ),
                &catalog,
                control,
            )
        },
        false,
        false,
    );
}

#[test]
fn scalar_occurrences_every_actual_small_success_callback_preserves_three_first_causes_and_caller_tail()
 {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (shared, shared_root, _) = shared_if(&catalog);
    for (fragment, root) in [lower(&catalog), (shared, shared_root)] {
        let flow = fragment.flow();
        let source = fragment.definition(root);
        let req = request(source, &fragment);
        let children = leaves(flow);
        let params = SemanticParameters::try_new([]).unwrap();
        for (&id, item) in flow
            .uses()
            .iter()
            .filter(|(_, item)| item.definition == root)
        {
            let trace = prefixes(
                |control| {
                    run(
                        input(
                            source,
                            &req,
                            flow,
                            id,
                            &children,
                            &params,
                            DecimalOverflowPolicy::ReportError,
                        ),
                        &catalog,
                        control,
                    )
                },
                true,
                false,
            );
            assert_eq!(trace.last().unwrap().0, PHASE);
            let control = Control::default();
            let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
            prepare_physical_scalar_occurrence_observed(
                input(
                    source,
                    &req,
                    flow,
                    id,
                    &children,
                    &params,
                    DecimalOverflowPolicy::ReportError,
                ),
                &catalog,
                &mut work,
            )
            .unwrap();
            let before = control.trace();
            work.finish().unwrap();
            assert_eq!(control.trace(), trace);
            assert_eq!(before.len() + 1, trace.len());
            assert_eq!(item.definition, root);
        }
    }
}

#[test]
fn scalar_occurrences_wide_coalesce_preserves_actual_order_and_samples_only_real_callbacks() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let mut builder = FixtureBuilder::new();
    let owner = builder.node;
    let nullable = ty(DataType::Utf8, true);
    let mut args = Vec::new();
    for at in 0..320 {
        args.push(literal(
            &mut builder,
            owner,
            nullable.clone(),
            if at == 319 {
                checked::utf8("last".into())
            } else {
                checked::null()
            },
        ));
    }
    let root = call(
        &mut builder,
        owner,
        binding(&catalog, "coalesce", &vec![nullable; 320]),
        &args,
    );
    value_roots(&mut builder, &[root]);
    let fragment = builder.seal(&catalog, policy());
    let flow = fragment.flow();
    let id = root_use(flow, root);
    let source = fragment.definition(root);
    let req = request(source, &fragment);
    let children = leaves(flow);
    let params = SemanticParameters::try_new([]).unwrap();
    let invocation = &flow.uses()[&id];
    assert_eq!(invocation.arguments.len(), 320);
    for (ordinal, &child) in invocation.arguments.iter().enumerate() {
        assert_eq!(flow.uses()[&child].definition, args[ordinal]);
    }
    let trace = prefixes(
        |control| {
            run(
                input(
                    source,
                    &req,
                    flow,
                    id,
                    &children,
                    &params,
                    DecimalOverflowPolicy::ReportError,
                ),
                &catalog,
                control,
            )
        },
        true,
        true,
    );
    // This is the actual occurrence edge/summary loop, not a claim about opaque
    // builtin preparation internals. Wide cancellation samples three callbacks.
    assert!(trace.contains(&(PHASE, 256)));
    let (_, calls) = prepare_all(&fragment, &catalog, DecimalOverflowPolicy::ReportError);
    assert_eq!(calls.claims().len(), 1);
}
