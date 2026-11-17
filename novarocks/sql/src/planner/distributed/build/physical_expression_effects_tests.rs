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

use super::super::expression_occurrences::author_physical_occurrences_observed;
use super::*;
use crate::functions::build_builtin_engine_function_catalog;
use crate::test_constants::{self as checked, Fragment, FragmentBuilder};
use arrow::{
    array::{Array, StringArray},
    datatypes::{DataType, Field, TimeUnit},
};
use novarocks_functions::{
    ConstantPool, EngineFunctionCatalog, FunctionArgument, FunctionResultType,
};
use novarocks_physical_plan::{
    BoundFunction, ConstantPoolId, ConstantReference, FragmentId, FragmentSink,
    FrozenFragmentCalls, NodeId, NodeKind, PhysicalCallSite, PipelineDopDomain, PlanLimits,
    ValueOrigin, WindowExpression, WindowSpec,
};
use novarocks_type_contract::{
    ControlShape, ExpressionEffectContext, ExpressionEffects, ExpressionEvaluationRegion,
    ExpressionInvocation, FunctionIntrinsicRowError, FunctionKind, FunctionValueType,
    FunctionVolatility, GuardKind, RegionGuard, SemanticParameterId, SemanticParameterKey,
};
use std::sync::{Arc, Mutex};
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
    value: checked::Source,
) -> ExprId {
    builder.constant(owner, ty, value)
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
        checked::utf8("Straße".into()),
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

fn uses(fragment: &Fragment, catalog: &EngineFunctionCatalog) -> PhysicalRootUses {
    author_physical_occurrences_observed(fragment, catalog, &Control::default())
        .unwrap()
        .root_uses
}

fn source_scopes<'a>(
    fragment: &'a Fragment,
    roots: &PhysicalRootUses,
) -> BTreeMap<ExpressionUseId, PhysicalCallSourceScope<'a>> {
    roots
        .flow()
        .uses()
        .iter()
        .filter_map(|(&id, item)| {
            let source = fragment.expressions().get(item.definition).unwrap();
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
    fragment: &Fragment,
    roots: &PhysicalRootUses,
    pools: &ConstantPools,
    params: &SemanticParameters,
    scopes: &BTreeMap<ExpressionUseId, PhysicalCallSourceScope<'_>>,
    catalog: &EngineFunctionCatalog,
    control: &Control,
) -> Result<AuthoredPhysicalExpressionEffects, PhysicalExpressionEffectsError> {
    let mut sources = pools.clone();
    checked::merge(&mut sources, fragment.constants());
    author_physical_expression_effects_observed(
        PhysicalExpressionEffectsInput {
            temporal_sources: None,
            fragment,
            roots,
            constants: &sources,
            parameters: params,
            literal_policy: policy(),
            call_scopes: scopes,
        },
        catalog,
        control,
    )
}
fn root_use(roots: &PhysicalRootUses, definition: ExprId) -> ExpressionUseId {
    *roots
        .flow()
        .uses()
        .iter()
        .find(|(_, item)| item.definition == definition)
        .unwrap()
        .0
}
fn summary(
    result: &AuthoredPhysicalExpressionEffects,
    roots: &PhysicalRootUses,
    definition: ExprId,
) -> ExpressionEffects {
    let id = root_use(roots, definition);
    result.summaries[&id]
        .for_use(roots.flow().uses()[&id].context)
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
    assert_eq!(invoke(&baseline).is_ok(), success);
    let trace = baseline.trace();
    assert_eq!(trace.first(), Some(&(PHASE, 0)));
    assert!(trace.len() > 1);
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
fn reordered(fragment: &Fragment, roots: &PhysicalRootUses) -> PhysicalRootUses {
    let ids: BTreeMap<_, _> = roots
        .flow()
        .uses()
        .keys()
        .enumerate()
        .map(|(index, &old)| {
            (
                old,
                ExpressionUseId::new(u32::MAX - u32::try_from(index * 17).unwrap()),
            )
        })
        .collect();
    let domains = roots
        .flow()
        .regions()
        .values()
        .map(|domain| ExpressionEvaluationRegion {
            guard: domain.guard.map(|guard| RegionGuard {
                owner: ids[&guard.owner],
                kind: guard.kind,
            }),
            ..*domain
        })
        .collect();
    let invocations = roots
        .flow()
        .uses()
        .values()
        .map(|item| ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id: ids[&item.context.use_id],
                ..item.context
            },
            definition: item.definition,
            control: item.control,
            arguments: item.arguments.iter().map(|id| ids[id]).collect(),
        })
        .collect();
    let flow = novarocks_type_contract::ExpressionControlFlow::try_new(
        domains,
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    let bindings = roots
        .bindings()
        .iter()
        .map(|(&site, id)| (site, ids[id]))
        .collect();
    PhysicalRootUses::try_new(fragment, flow, bindings, &Control::default()).unwrap()
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
    builder: &mut FragmentBuilder,
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
    builder
        .add_expression(
            owner,
            ty(DataType::Timestamp(target, None), nullable),
            ExprKind::Cast {
                expr: child,
                target: DataType::Timestamp(target, None),
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                allow_throw_exception: allow_ref(),
            },
        )
        .unwrap()
}

#[test]
fn expression_effects_actual_edges_schedule_reordered_sparse_uses_and_shared_definitions_child_first()
 {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (mut builder, leaf, owner) = empty();
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
    let lower_ty = builder.expressions().get(lower).unwrap().ty.clone();
    let root = call(
        &mut builder,
        owner,
        binding(&catalog, "coalesce", &[lower_ty.clone(), lower_ty]),
        &[lower, lower],
    );
    project(&mut builder, leaf, owner, &[root]);
    let fragment = finish(builder, owner);
    let original = uses(&fragment, &catalog);
    let reordered = reordered(&fragment, &original);
    let params = SemanticParameters::try_new([]).unwrap();
    let pools = ConstantPools::empty();
    for roots in [&original, &reordered] {
        let scopes = source_scopes(&fragment, roots);
        let result = run(
            &fragment,
            roots,
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
        let calls =
            FrozenFragmentCalls::try_new(&fragment, roots, result.calls, &Control::default())
                .unwrap();
        calls
            .validate_fragment(&fragment, roots, &Control::default())
            .unwrap();
    }
}

#[test]
fn expression_effects_primitive_cast_successful_null_and_errorful_cast_arithmetic_are_separate() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (mut builder, leaf, owner) = empty();
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
    let add = builder
        .add_expression(
            owner,
            ty(DataType::Int64, true),
            ExprKind::Binary {
                left: a,
                op: BinaryOperator::Add,
                right: b,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                allow_throw_exception: Some(allow_ref()),
            },
        )
        .unwrap();
    project(
        &mut builder,
        leaf,
        owner,
        &[successful_null, row_error, add],
    );
    let fragment = finish(builder, owner);
    let roots = uses(&fragment, &catalog);
    let scopes = source_scopes(&fragment, &roots);
    let pools = ConstantPools::empty();
    let params = parameters();
    let result = run(
        &fragment,
        &roots,
        &pools,
        &params,
        &scopes,
        &catalog,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        summary(&result, &roots, successful_null),
        ExpressionEffects::PURE_VALUE
    );
    assert!(summary(&result, &roots, row_error).may_raise_row_error);
    assert!(summary(&result, &roots, add).may_raise_row_error);
    assert!(result.calls.is_empty());
    assert!(
        fragment
            .expressions()
            .get(successful_null)
            .unwrap()
            .ty
            .nullable()
    );
    assert!(!fragment.expressions().get(row_error).unwrap().ty.nullable());
}

#[test]
fn expression_effects_case_and_boolean_guards_keep_rand_state_and_errorful_cast_children() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (mut builder, leaf, owner) = empty();
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
    let isnull = builder
        .add_expression(
            owner,
            boolean.clone(),
            ExprKind::IsNull {
                expr: errorful,
                negated: false,
            },
        )
        .unwrap();
    let rand = call(&mut builder, owner, binding(&catalog, "rand", &[]), &[]);
    let rand_ty = builder.expressions().get(rand).unwrap().ty.clone();
    let zero = literal(
        &mut builder,
        owner,
        rand_ty.clone(),
        checked::f64_bits(0.0f64.to_bits()),
    );
    let compare = builder
        .add_expression(
            owner,
            ty(DataType::Boolean, rand_ty.nullable()),
            ExprKind::Binary {
                left: rand,
                op: BinaryOperator::Gt,
                right: zero,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                allow_throw_exception: None,
            },
        )
        .unwrap();
    let case = builder
        .add_expression(
            owner,
            ty(DataType::Boolean, rand_ty.nullable()),
            ExprKind::Case {
                operand: None,
                when_then: Box::from([(condition, isnull)]),
                else_expr: Some(compare),
            },
        )
        .unwrap();
    let root = builder
        .add_expression(
            owner,
            ty(DataType::Boolean, rand_ty.nullable()),
            ExprKind::Conjunction {
                args: Box::from([false_, case]),
            },
        )
        .unwrap();
    project(&mut builder, leaf, owner, &[root]);
    let fragment = finish(builder, owner);
    let roots = uses(&fragment, &catalog);
    let scopes = source_scopes(&fragment, &roots);
    let pools = ConstantPools::empty();
    let params = parameters();
    let result = run(
        &fragment,
        &roots,
        &pools,
        &params,
        &scopes,
        &catalog,
        &Control::default(),
    )
    .unwrap();
    let effects = summary(&result, &roots, root);
    assert_eq!(effects.value_stability, FunctionVolatility::Volatile);
    assert!(effects.may_raise_row_error);
    assert!(effects.has_instance_state);
    assert!(effects.observable_effects.rng_sampling);
    assert_eq!(result.calls.len(), 1);
    let case_id = root_use(&roots, case);
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
    let calls =
        FrozenFragmentCalls::try_new(&fragment, &roots, result.calls, &Control::default()).unwrap();
    assert_eq!(
        calls.entries()[&PhysicalCallSite::Expression(root_use(&roots, rand))]
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
        let (mut builder, leaf, owner) = empty();
        let mut ids = Vec::new();
        for ordinal in [1, 2] {
            ids.push(
                builder
                    .add_expression(
                        owner,
                        ty(DataType::Utf8, nullable),
                        ExprKind::Constant(ConstantReference {
                            pool: ConstantPoolId::new(u32::MAX),
                            ordinal,
                        }),
                    )
                    .unwrap(),
            );
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
        project(&mut builder, leaf, owner, &[root, ids[0], ids[1]]);
        (finish(builder, owner), root)
    };
    let (fragment, root) = build(true);
    let roots = uses(&fragment, &catalog);
    let scopes = source_scopes(&fragment, &roots);
    let params = SemanticParameters::try_new([]).unwrap();
    let result = run(
        &fragment,
        &roots,
        &pools,
        &params,
        &scopes,
        &catalog,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        summary(&result, &roots, root),
        ExpressionEffects::PURE_VALUE
    );
    assert_eq!(result.calls.len(), 1);
    let selected = pool.value(1).unwrap();
    let null = pool.value(2).unwrap();
    assert_eq!(selected.ordinal(), 1);
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
        |control| {
            run(
                &fragment, &roots, &pools, &params, &scopes, &catalog, control,
            )
        },
        true,
        false,
    );
    let (bad, _) = build(false);
    let bad_roots = uses(&bad, &catalog);
    let bad_scopes = source_scopes(&bad, &bad_roots);
    assert!(matches!(
        run(
            &bad,
            &bad_roots,
            &pools,
            &params,
            &bad_scopes,
            &catalog,
            &Control::default()
        ),
        Err(PhysicalExpressionEffectsError::Request(_))
    ));
    prefixes(
        |control| {
            run(
                &bad,
                &bad_roots,
                &pools,
                &params,
                &bad_scopes,
                &catalog,
                control,
            )
        },
        false,
        false,
    );
}

#[test]
fn expression_effects_missing_spurious_and_stale_same_id_scalar_source_scopes_refuse() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (fragment, root) = lower(&catalog);
    let roots = uses(&fragment, &catalog);
    let mut scopes = source_scopes(&fragment, &roots);
    let pools = ConstantPools::empty();
    let params = SemanticParameters::try_new([]).unwrap();
    let id = root_use(&roots, root);
    scopes.remove(&id);
    assert!(
        matches!(run(&fragment,&roots,&pools,&params,&scopes,&catalog,&Control::default()),Err(PhysicalExpressionEffectsError::MissingCallScope(actual)) if actual==id)
    );
    prefixes(
        |control| {
            run(
                &fragment, &roots, &pools, &params, &scopes, &catalog, control,
            )
        },
        false,
        false,
    );
    let foreign = fragment.expressions().get(root).unwrap().clone();
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
            &fragment,
            &roots,
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
        |control| {
            run(
                &fragment, &roots, &pools, &params, &scopes, &catalog, control,
            )
        },
        false,
        false,
    );
    let mut valid = source_scopes(&fragment, &roots);
    valid.insert(
        ExpressionUseId::new(u32::MAX),
        PhysicalCallSourceScope {
            source: fragment.expressions().get(root).unwrap(),
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            environment: &[],
            proof_scope: CallProofScope::Unconditional,
        },
    );
    assert!(matches!(
        run(
            &fragment,
            &roots,
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
        let (mut builder, leaf, owner) = empty();
        let root = cast(
            &mut builder,
            owner,
            TimeUnit::Second,
            TimeUnit::Microsecond,
            nullable,
        );
        project(&mut builder, leaf, owner, &[root]);
        finish(builder, owner)
    };
    let pools = ConstantPools::empty();
    let invalid = build(false);
    let roots = uses(&invalid, &catalog);
    let scopes = source_scopes(&invalid, &roots);
    let params = parameters();
    assert!(matches!(
        run(
            &invalid,
            &roots,
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
        |control| {
            run(
                &invalid, &roots, &pools, &params, &scopes, &catalog, control,
            )
        },
        false,
        false,
    );
    let valid = build(true);
    let roots = uses(&valid, &catalog);
    let scopes = source_scopes(&valid, &roots);
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
                &roots,
                &pools,
                params,
                &scopes,
                &catalog,
                &Control::default()
            ),
            Err(PhysicalExpressionEffectsError::Parameter(_))
        ));
        prefixes(
            |control| run(&valid, &roots, &pools, params, &scopes, &catalog, control),
            false,
            false,
        );
    }
}

#[test]
fn expression_effects_runtime_window_requires_explicit_scope_and_keeps_its_lifecycle() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (mut builder, leaf, owner) = empty();
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
        legacy_metadata: Some(novarocks_physical_plan::LegacyBindingMetadata {
            volatility: resolved.semantics.volatility,
            argument_evaluation: resolved.semantics.argument_evaluation,
            failure_behavior: resolved.semantics.failure_behavior,
            intrinsic_row_error: resolved.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        }),
    };
    let root = builder
        .add_expression(
            owner,
            result.clone(),
            ExprKind::WindowCall {
                function,
                distinct: false,
                args: Box::default(),
                function_order_by: Box::default(),
                frame: None,
                ignore_nulls: false,
                aggregate_binding: None,
            },
        )
        .unwrap();
    let output = builder
        .add_value(
            result,
            ValueOrigin::Expr {
                node: owner,
                expr: root,
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
                expressions: Box::from([WindowExpression {
                    expression: root,
                    output,
                }]),
            }),
        )
        .unwrap();
    let fragment = finish(builder, owner);
    let roots = uses(&fragment, &catalog);
    let scopes = source_scopes(&fragment, &roots);
    let pools = ConstantPools::empty();
    let params = SemanticParameters::try_new([]).unwrap();
    assert!(
        matches!(run(&fragment,&roots,&pools,&params,&scopes,&catalog,&Control::default()),Err(PhysicalExpressionEffectsError::MissingCallScope(actual)) if actual==root_use(&roots,root))
    );
    prefixes(
        |control| {
            run(
                &fragment, &roots, &pools, &params, &scopes, &catalog, control,
            )
        },
        false,
        false,
    );
    let mut scopes = scopes;
    let id = root_use(&roots, root);
    scopes.insert(
        id,
        PhysicalCallSourceScope {
            source: fragment.expressions().get(root).unwrap(),
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            environment: &[],
            proof_scope: CallProofScope::Region(roots.flow().uses()[&id].context.region),
        },
    );
    let result = run(
        &fragment,
        &roots,
        &pools,
        &params,
        &scopes,
        &catalog,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(result.calls.len(), 1);
    assert!(summary(&result, &roots, root).has_instance_state);
    let calls =
        FrozenFragmentCalls::try_new(&fragment, &roots, result.calls, &Control::default()).unwrap();
    assert_eq!(
        calls.entries()[&PhysicalCallSite::Expression(id)]
            .effects
            .instance_state,
        novarocks_type_contract::FunctionInstanceState::WindowPartition
    );
    prefixes(
        |control| {
            run(
                &fragment, &roots, &pools, &params, &scopes, &catalog, control,
            )
        },
        true,
        false,
    );
}

#[test]
fn expression_effects_whole_success_prefixes_and_wide_actual_traversal_sampling() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let (fragment, _) = lower(&catalog);
    let roots = uses(&fragment, &catalog);
    let scopes = source_scopes(&fragment, &roots);
    let pools = ConstantPools::empty();
    let params = SemanticParameters::try_new([]).unwrap();
    let trace = prefixes(
        |control| {
            run(
                &fragment, &roots, &pools, &params, &scopes, &catalog, control,
            )
        },
        true,
        false,
    );
    assert_eq!(trace.last().unwrap().0, PHASE);
    let reordered_roots = reordered(&fragment, &roots);
    let reordered_scopes = source_scopes(&fragment, &reordered_roots);
    prefixes(
        |control| {
            run(
                &fragment,
                &reordered_roots,
                &pools,
                &params,
                &reordered_scopes,
                &catalog,
                control,
            )
        },
        true,
        false,
    );
    let (mut builder, leaf, owner) = empty();
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
    let root = builder
        .add_expression(
            owner,
            boolean,
            ExprKind::Conjunction {
                args: args.into_boxed_slice(),
            },
        )
        .unwrap();
    project(&mut builder, leaf, owner, &[root]);
    let fragment = finish(builder, owner);
    let roots = uses(&fragment, &catalog);
    let scopes = source_scopes(&fragment, &roots);
    let result = run(
        &fragment,
        &roots,
        &pools,
        &params,
        &scopes,
        &catalog,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(result.summaries.len(), 321);
    assert!(result.calls.is_empty());
    assert_eq!(
        summary(&result, &roots, root),
        ExpressionEffects::PURE_VALUE
    );
    let trace = prefixes(
        |control| {
            run(
                &fragment, &roots, &pools, &params, &scopes, &catalog, control,
            )
        },
        true,
        true,
    );
    assert!(trace.contains(&(PHASE, 256)));
}
