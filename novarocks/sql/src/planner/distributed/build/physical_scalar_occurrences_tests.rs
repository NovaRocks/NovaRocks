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
    expression_occurrences::author_physical_occurrences_observed,
    physical_scalar_requests::author_physical_scalar_request_observed,
};
use super::*;
use crate::functions::build_builtin_engine_function_catalog;
use arrow::datatypes::{DataType, Field};
use novarocks_functions::{
    ConstantPolicy, EngineFunctionCatalog, FunctionArgument, FunctionResolutionError,
    FunctionResultType, FunctionSpecializationFailure, ResolvedAggregateSignature,
};
use novarocks_physical_plan::{
    BoundFunction, ChangeEventSpec, ConstantPools, ExprArena, ExpressionRootRole,
    ExpressionRootSite, Fragment, FragmentBuilder, FragmentId, FragmentSink, FrozenFragmentCalls,
    LiteralValue, NodeId, NodeKind, PhysicalRootUses, PipelineDopDomain, PlanLimits, ValueOrigin,
};
use novarocks_type_contract::{
    CompilePhase, EvaluationDemand, EvaluationDomainId, ExpressionEffectContext, ExpressionEffects,
    ExpressionEvaluationDomain, ExpressionInvocation, FunctionArgumentEvaluation,
    FunctionFailureBehavior, FunctionId, FunctionIntrinsicRowError, FunctionValueType,
    FunctionVolatility, GuardKind, PureCompileControl, SemanticParameterId, SemanticParameterKey,
    SemanticParameterValue,
};
use std::sync::Mutex;

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
        legacy_metadata: Some(novarocks_physical_plan::LegacyBindingMetadata {
            volatility: resolved.semantics.volatility,
            argument_evaluation: resolved.semantics.argument_evaluation,
            failure_behavior: resolved.semantics.failure_behavior,
            intrinsic_row_error: resolved.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        }),
    }
}
fn empty() -> (FragmentBuilder, NodeId, NodeId) {
    let mut builder = FragmentBuilder::new(FragmentId::new(71));
    let input = NodeId::new(7);
    let owner = NodeId::new(901);
    builder
        .add_values(
            input,
            Box::from([Box::<[ExprId]>::default()]),
            Box::default(),
        )
        .unwrap();
    (builder, input, owner)
}
fn literal(
    builder: &mut FragmentBuilder,
    owner: NodeId,
    ty: FunctionValueType,
    value: LiteralValue,
) -> ExprId {
    builder
        .add_expression(owner, ty, ExprKind::Literal(value))
        .unwrap()
}
fn call(
    builder: &mut FragmentBuilder,
    owner: NodeId,
    function: BoundFunction,
    args: &[ExprId],
) -> ExprId {
    builder
        .add_expression(
            owner,
            function.result_type.clone(),
            ExprKind::FunctionCall {
                function,
                args: args.into(),
            },
        )
        .unwrap()
}
fn finish(builder: FragmentBuilder, root: NodeId) -> Fragment {
    builder
        .finish_structure(
            root,
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
        .unwrap()
}
fn project(builder: &mut FragmentBuilder, input: NodeId, owner: NodeId, roots: &[ExprId]) {
    let mut pairs = Vec::new();
    let mut outputs = Vec::new();
    for &expr in roots {
        let value = builder
            .add_value(
                builder.expressions().get(expr).unwrap().ty.clone(),
                ValueOrigin::Expr { node: owner, expr },
            )
            .unwrap();
        pairs.push((expr, value));
        outputs.push(value);
    }
    builder
        .add_project(
            owner,
            input,
            pairs.into_boxed_slice(),
            outputs.into_boxed_slice(),
        )
        .unwrap();
}
fn lower(catalog: &EngineFunctionCatalog) -> (Fragment, ExprId) {
    let (mut builder, input, owner) = empty();
    let arg = literal(
        &mut builder,
        owner,
        ty(DataType::Utf8, false),
        LiteralValue::Utf8("Straße".into()),
    );
    let root = call(
        &mut builder,
        owner,
        binding(catalog, "lower", &[ty(DataType::Utf8, false)]),
        &[arg],
    );
    project(&mut builder, input, owner, &[root]);
    (finish(builder, owner), root)
}
fn shared_if(catalog: &EngineFunctionCatalog) -> (Fragment, ExprId) {
    let (mut builder, input, owner) = empty();
    let boolean = ty(DataType::Boolean, false);
    let a = literal(
        &mut builder,
        owner,
        boolean.clone(),
        LiteralValue::Boolean(true),
    );
    let b = literal(
        &mut builder,
        owner,
        boolean.clone(),
        LiteralValue::Boolean(false),
    );
    let mut function = binding(
        catalog,
        "if",
        &[boolean.clone(), boolean.clone(), boolean.clone()],
    );
    // These contradictory legacy fields are not authority for the new owner.
    function
        .legacy_metadata
        .as_mut()
        .expect("actual legacy fixture")
        .argument_evaluation = FunctionArgumentEvaluation::Eager;
    function
        .legacy_metadata
        .as_mut()
        .expect("actual legacy fixture")
        .volatility = FunctionVolatility::Volatile;
    function
        .legacy_metadata
        .as_mut()
        .expect("actual legacy fixture")
        .intrinsic_row_error = FunctionIntrinsicRowError::MayRaise;
    function
        .legacy_metadata
        .as_mut()
        .expect("actual legacy fixture")
        .failure_behavior = FunctionFailureBehavior::ReturnsNull;
    let root = ExprId::new(u32::MAX - 1);
    builder
        .insert_expression(ExprNode {
            id: root,
            owner,
            lambda_scope: None,
            ty: function.result_type.clone(),
            kind: ExprKind::FunctionCall {
                function,
                args: Box::from([a, b, a]),
            },
        })
        .unwrap();
    let value = builder
        .add_value(
            boolean,
            ValueOrigin::NodeOutput {
                node: owner,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let effect = builder
        .add_value(
            ty(DataType::Int8, false),
            ValueOrigin::NodeOutput {
                node: owner,
                output_ordinal: 1,
            },
        )
        .unwrap();
    builder
        .add_row_rewriting(
            owner,
            input,
            Some(&BTreeMap::new()),
            Box::from([value, effect]),
            NodeKind::ChangeEventExpand {
                events: Box::from([ChangeEventSpec {
                    predicate: Some(root),
                    effect: novarocks_connector_contract::ConnectorRowMutationEffect::Insert,
                    assignments: Box::from([novarocks_physical_plan::ChangeAssignment::exact(
                        value,
                        Some(root),
                    )]),
                }]),
                effect_output: effect,
            },
        )
        .unwrap();
    (finish(builder, owner), root)
}
fn uses(fragment: &Fragment, catalog: &EngineFunctionCatalog) -> PhysicalRootUses {
    author_physical_occurrences_observed(fragment, catalog, &Control::default())
        .unwrap()
        .root_uses
}
fn request<'a>(source: &'a ExprNode, fragment: &Fragment) -> AuthoredPhysicalScalarRequest<'a> {
    request_from(source, fragment.expressions())
}
fn request_from<'a>(source: &'a ExprNode, arena: &ExprArena) -> AuthoredPhysicalScalarRequest<'a> {
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
    let result = author_physical_scalar_request_observed(
        source,
        arena,
        &ConstantPools::empty(),
        policy(),
        &mut work,
    )
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
        proof_scope: CallProofScope::Domain(flow.uses()[&id].context.domain),
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
    fragment: &Fragment,
    roots: &PhysicalRootUses,
    catalog: &EngineFunctionCatalog,
    policy: DecimalOverflowPolicy,
) -> (
    BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    FrozenFragmentCalls,
) {
    let params = SemanticParameters::try_new([]).unwrap();
    let mut summaries = BTreeMap::new();
    let mut frozen = Vec::new();
    // The author assigns each parent before its ordered descendants. Each
    // invocation is prepared independently, including shared definitions.
    for (&id, item) in roots.flow().uses().iter().rev() {
        let source = fragment.expressions().get(item.definition).unwrap();
        if matches!(source.kind, ExprKind::FunctionCall { .. }) {
            let req = request(source, fragment);
            let result = run(
                input(source, &req, roots.flow(), id, &summaries, &params, policy),
                catalog,
                &Control::default(),
            )
            .unwrap();
            assert!(Arc::ptr_eq(
                req.selected(),
                result.preparation.call_contract().selected_owner()
            ));
            assert_eq!(result.frozen.context, item.context);
            summaries.insert(id, result.preparation.effects());
            frozen.push(result.frozen);
        } else {
            summaries.insert(id, ScopedExpressionEffects::pure_value(item.context));
        }
    }
    let calls = FrozenFragmentCalls::try_new(fragment, roots, frozen, &Control::default()).unwrap();
    calls
        .validate_fragment(fragment, roots, &Control::default())
        .unwrap();
    (summaries, calls)
}

#[test]
fn scalar_occurrences_shared_if_keeps_two_demands_guards_policies_and_same_selected_owner() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (fragment, root) = shared_if(&catalog);
    let roots = uses(&fragment, &catalog);
    let source = fragment.expressions().get(root).unwrap();
    let req = request(source, &fragment);
    let children = leaves(roots.flow());
    let params = SemanticParameters::try_new([]).unwrap();
    let owner = NodeId::new(901);
    let truth = roots.bindings()[&ExpressionRootSite {
        node: owner,
        role: ExpressionRootRole::ChangePredicate { event: 0 },
    }];
    let value = roots.bindings()[&ExpressionRootSite {
        node: owner,
        role: ExpressionRootRole::ChangeAssignment {
            event: 0,
            assignment: 0,
        },
    }];
    assert_ne!(truth, value);
    let mut calls = Vec::new();
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
        let invocation = &roots.flow().uses()[&id];
        assert_eq!(invocation.definition, root);
        assert_eq!(invocation.control, ControlShape::If);
        assert_eq!(invocation.context.demand, demand);
        for (ordinal, kind) in [(1, GuardKind::IfThen), (2, GuardKind::IfElse)] {
            let child = &roots.flow().uses()[&invocation.arguments[ordinal]];
            let domain = roots.flow().domains()[&child.context.domain];
            assert_eq!(domain.parent, Some(invocation.context.domain));
            assert_eq!(domain.guard.unwrap().owner, id);
            assert_eq!(domain.guard.unwrap().kind, kind);
            assert_eq!(child.context.demand, demand);
        }
        let result = run(
            input(source, &req, roots.flow(), id, &children, &params, policy),
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
        assert_eq!(result.frozen.decimal_overflow_policy, policy);
        assert_eq!(
            result.frozen.effects.value_stability,
            FunctionVolatility::Immutable
        );
        assert_eq!(
            result.frozen.effects.own_row_error,
            FunctionIntrinsicRowError::NoRowError
        );
        assert_eq!(
            result.frozen.effects.proof_scope,
            CallProofScope::Domain(invocation.context.domain)
        );
        calls.push(result.frozen);
    }
    let calls =
        FrozenFragmentCalls::try_new(&fragment, &roots, calls, &Control::default()).unwrap();
    assert_eq!(calls.entries().len(), 2);
    calls
        .validate_fragment(&fragment, &roots, &Control::default())
        .unwrap();
}

#[test]
fn scalar_occurrences_actual_coalesce_preserves_rng_instance_and_decimal_row_error_children() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (mut builder, leaf, owner) = empty();
    let rand = call(&mut builder, owner, binding(&catalog, "rand", &[]), &[]);
    let rand_ty = builder.expressions().get(rand).unwrap().ty.clone();
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
        LiteralValue::Decimal128(12345),
    );
    let round = call(
        &mut builder,
        owner,
        binding(&catalog, "round", &[decimal]),
        &[arg],
    );
    let round_ty = builder.expressions().get(round).unwrap().ty.clone();
    let coalesce_round = call(
        &mut builder,
        owner,
        binding(&catalog, "coalesce", &[round_ty.clone(), round_ty]),
        &[round, round],
    );
    project(&mut builder, leaf, owner, &[coalesce_rand, coalesce_round]);
    let fragment = finish(builder, owner);
    let roots = uses(&fragment, &catalog);
    for policy in [
        DecimalOverflowPolicy::ReportError,
        DecimalOverflowPolicy::OutputNull,
    ] {
        let (summaries, calls) = prepare_all(&fragment, &roots, &catalog, policy);
        let rand_use = root_use(roots.flow(), coalesce_rand);
        let round_use = root_use(roots.flow(), coalesce_round);
        let rng = summaries[&rand_use]
            .for_use(roots.flow().uses()[&rand_use].context)
            .unwrap();
        assert_eq!(rng.value_stability, FunctionVolatility::Volatile);
        assert!(rng.has_instance_state);
        assert!(rng.observable_effects.rng_sampling);
        let decimal = summaries[&round_use]
            .for_use(roots.flow().uses()[&round_use].context)
            .unwrap();
        assert_eq!(
            decimal.may_raise_row_error,
            policy == DecimalOverflowPolicy::ReportError
        );
        // Own COALESCE facts do not absorb the child's mutable instance; the
        // composed scoped summary above retains it for the next parent.
        assert_eq!(
            calls.entries()[&PhysicalCallSite::Expression(rand_use)]
                .effects
                .own_row_error,
            FunctionIntrinsicRowError::NoRowError
        );
        assert_eq!(calls.entries().len(), 6);
    }
}

#[test]
fn scalar_occurrences_reject_foreign_static_source_missing_use_and_wrong_definition() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (fragment, root) = lower(&catalog);
    let roots = uses(&fragment, &catalog);
    let flow = roots.flow();
    let id = root_use(flow, root);
    let source = fragment.expressions().get(root).unwrap();
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
        flow.domains().values().copied().collect(),
        invocations,
        fragment.expressions(),
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
    let (fragment, root) = shared_if(&catalog);
    let roots = uses(&fragment, &catalog);
    let flow = roots.flow();
    let id = root_use(flow, root);
    let source = fragment.expressions().get(root).unwrap();
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
    let roots = uses(&fragment, &catalog);
    let flow = roots.flow();
    let id = root_use(flow, root);
    let source = fragment.expressions().get(root).unwrap();
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
    call.proof_scope = CallProofScope::Domain(EvaluationDomainId::new(u32::MAX));
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
    assert!(result.frozen.effects.environment.is_empty());
    assert_eq!(
        result.frozen.decimal_overflow_policy,
        DecimalOverflowPolicy::OutputNull
    );
}

#[test]
fn scalar_occurrences_typeof_unknown_and_metadata_only_catalog_never_supply_owner_facts() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (fragment, root) = lower(&catalog);
    let roots = uses(&fragment, &catalog);
    let flow = roots.flow();
    let id = root_use(flow, root);
    let source = fragment.expressions().get(root).unwrap();
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
        legacy_metadata: Some(novarocks_physical_plan::LegacyBindingMetadata {
            volatility: resolved.semantics.volatility,
            argument_evaluation: resolved.semantics.argument_evaluation,
            failure_behavior: resolved.semantics.failure_behavior,
            intrinsic_row_error: resolved.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        }),
    };
    let owner = NodeId::new(901);
    let nodes = vec![
        ExprNode {
            id: ExprId::new(1),
            owner,
            lambda_scope: None,
            ty: integer.clone(),
            kind: ExprKind::Literal(LiteralValue::Int64(7)),
        },
        ExprNode {
            id: ExprId::new(2),
            owner,
            lambda_scope: None,
            ty: integer.clone(),
            kind: ExprKind::Lambda {
                parameter_types: Box::from([integer]),
                body: ExprId::new(1),
            },
        },
        ExprNode {
            id: ExprId::new(3),
            owner,
            lambda_scope: None,
            ty: list,
            kind: ExprKind::Value(novarocks_physical_plan::ValueId::new(3)),
        },
    ];
    let arena = ExprArena::try_from_definitions_observed(
        nodes.into_iter(),
        &PlanLimits::FROZEN,
        &Control::default(),
    )
    .unwrap();
    let source = ExprNode {
        id: ExprId::new(17),
        owner,
        lambda_scope: None,
        ty: result,
        kind: ExprKind::FunctionCall {
            function,
            args: Box::from([ExprId::new(2), ExprId::new(3)]),
        },
    };
    // Membership includes the root without manufacturing an installed owner.
    let mut nodes: Vec<_> = arena.iter().map(|(_, node)| node.clone()).collect();
    nodes.push(source.clone());
    let arena = ExprArena::try_from_definitions_observed(
        nodes.into_iter(),
        &PlanLimits::FROZEN,
        &Control::default(),
    )
    .unwrap();
    let source = arena.get(ExprId::new(17)).unwrap();
    let req = request_from(source, &arena);
    let domain = EvaluationDomainId::new(0);
    let context = |id| ExpressionEffectContext {
        use_id: ExpressionUseId::new(id),
        domain,
        demand: EvaluationDemand::Value,
    };
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
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
                definition: ExprId::new(2),
                control: ControlShape::Eager,
                arguments: Box::default(),
            },
            ExpressionInvocation {
                context: context(2),
                definition: ExprId::new(3),
                control: ControlShape::Eager,
                arguments: Box::default(),
            },
        ],
        &arena,
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
                ExpressionUseId::new(0),
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
                    ExpressionUseId::new(0),
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
    for (fragment, root) in [lower(&catalog), shared_if(&catalog)] {
        let roots = uses(&fragment, &catalog);
        let flow = roots.flow();
        let source = fragment.expressions().get(root).unwrap();
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
    let (mut builder, leaf, owner) = empty();
    let nullable = ty(DataType::Utf8, true);
    let mut args = Vec::new();
    for at in 0..320 {
        args.push(literal(
            &mut builder,
            owner,
            nullable.clone(),
            if at == 319 {
                LiteralValue::Utf8("last".into())
            } else {
                LiteralValue::Null
            },
        ));
    }
    let root = call(
        &mut builder,
        owner,
        binding(&catalog, "coalesce", &vec![nullable; 320]),
        &args,
    );
    project(&mut builder, leaf, owner, &[root]);
    let fragment = finish(builder, owner);
    let roots = uses(&fragment, &catalog);
    let flow = roots.flow();
    let id = root_use(flow, root);
    let source = fragment.expressions().get(root).unwrap();
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
    let (_, calls) = prepare_all(
        &fragment,
        &roots,
        &catalog,
        DecimalOverflowPolicy::ReportError,
    );
    assert_eq!(calls.entries().len(), 1);
}
