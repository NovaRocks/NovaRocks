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

// These tests seal actual builtin owners only. They do not claim the Server
// catalogue or the Execution controller's guarded evaluation is complete.
use super::*;
use arrow_schema::DataType;
use novarocks_functions::{
    CallEffectInput, ConstantPolicy, EngineFunctionCatalogBuilder, FunctionArgument,
    FunctionBindingRequest, FunctionId, FunctionKind, FunctionOverloadId, FunctionResultType,
    InstalledPureKernel, PureCallPreparation, PureEngineFunctionCatalog,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi, PurePreparationSource,
    ScopedExpressionEffects,
};
use novarocks_local_program::{
    KernelAbiVersion, ProgramCallSite, ProgramExpressionArena, ProgramStateTemplate,
};
use novarocks_physical_plan::{
    BoundFunction, ExprId, ExprKind, FragmentBuilder, FragmentCuts, FragmentId, FragmentPackage,
    FragmentPackageInput, FragmentSink, FrozenCallError, FrozenFragmentCalls,
    FrozenFragmentPruning, FrozenPhysicalCall, LiteralValue, NodeId, PhysicalCallSite,
    PhysicalExpressionRoots, PhysicalRootUses, PipelineDopDomain, PlanVersionId, RequiredContracts,
    ResultField, ResultPort, ValueOrigin,
};
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, CompileControlError, CompilePhase, ControlShape,
    DecimalOverflowPolicy, DomainGuard, EvaluationDemand, EvaluationDomainId,
    ExpressionControlFlow, ExpressionEffectContext, ExpressionEvaluationDomain,
    ExpressionInvocation, ExpressionUseId, FunctionInstanceState, FunctionNullBehavior,
    FunctionValueType, FunctionVolatility, GuardKind, PureCompileControl, SemanticParameters,
};
use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn catalogue() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    for name in ["if", "coalesce", "rand"] {
        builder
            .register(
                actual
                    .definition(name, FunctionKind::Scalar)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
    }
    let mut records = Vec::new();
    for (name, overloads, abi) in [
        (
            "if",
            &["(bool,any<T>,any<T>)->any<T>;widen;legacy"][..],
            PureKernelAbi::ControlIntrinsicV1,
        ),
        (
            "coalesce",
            &["(any<T>...)->any<T>;widen;legacy"][..],
            PureKernelAbi::ControlIntrinsicV1,
        ),
        (
            "rand",
            &["()->f64;strict;legacy", "(i64)->f64;strict;legacy"][..],
            PureKernelAbi::ScalarV1,
        ),
    ] {
        for overload in overloads {
            records.push(InstalledPureKernel {
                function: FunctionId::try_new(format!("builtin.scalar/{name}/v1")).unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(format!(
                        "builtin.scalar/{name}/{overload}"
                    ))
                    .unwrap(),
                    implementation: PureImplementationId::try_new(format!(
                        "builtin.scalar/{name}/selected-v1"
                    ))
                    .unwrap(),
                    abi,
                },
                aggregate_state_format: None,
            });
        }
    }
    builder.seal_pure(records).unwrap()
}

fn options() -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(1).unwrap(),
        root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        // Explicit fixture limits, never production admission defaults.
        constants: ConstantPolicy {
            max_rows: 16,
            max_array_nodes: 128,
            max_logical_elements: 1024,
            max_retained_buffer_bytes: 1 << 20,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 64,
            max_metadata_bytes: 1 << 20,
            max_library_validation_work: 1 << 20,
            max_library_validation_bytes: 1 << 20,
        },
    }
}

fn context(id: u32, domain: u32, demand: EvaluationDemand) -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(id),
        domain: EvaluationDomainId::new(domain),
        demand,
    }
}
fn invocation(
    context: ExpressionEffectContext,
    definition: ExprId,
    control: ControlShape,
    arguments: &[u32],
) -> ExpressionInvocation<ExprId> {
    ExpressionInvocation {
        context,
        definition,
        control,
        arguments: arguments
            .iter()
            .copied()
            .map(ExpressionUseId::new)
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    }
}
fn value_argument(ty: &FunctionValueType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: ty.clone(),
        constant: None,
    }
}
fn physical_call(bound: &novarocks_functions::ResolvedFunctionBinding) -> BoundFunction {
    let FunctionResultType::Scalar(result_type) = &bound.selected.result_type else {
        panic!("scalar result")
    };
    BoundFunction {
        function_id: bound.function_id.clone(),
        overload: bound.selected.overload.clone(),
        kind: bound.kind,
        argument_types: bound.selected.argument_types.clone(),
        result_type: result_type.clone(),
        legacy_metadata: Some(novarocks_physical_plan::LegacyBindingMetadata {
            volatility: bound.semantics.volatility,
            argument_evaluation: bound.semantics.argument_evaluation,
            failure_behavior: bound.semantics.failure_behavior,
            intrinsic_row_error: bound.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        }),
    }
}

#[derive(Clone, Copy)]
enum Claims {
    Accurate,
    WrongControl,
    WrongNullBehavior,
    WrongStability,
}

fn package(
    functions: &PureEngineFunctionCatalog,
    name: &str,
    claims: Claims,
) -> Result<Arc<FragmentPackage>, FrozenCallError> {
    let ty = FunctionValueType::new(DataType::Float64, true);
    let boolean = FunctionValueType::new(DataType::Boolean, false);
    let arguments = if name == "if" {
        vec![
            value_argument(&boolean),
            value_argument(&ty),
            value_argument(&ty),
        ]
    } else {
        vec![value_argument(&ty), value_argument(&ty)]
    };
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: arguments.len(),
        expected_result_type: None,
    };
    let bound = functions
        .metadata()
        .resolve_bound_user(name, FunctionKind::Scalar, request, &Control)
        .unwrap();
    let selected = Arc::new(bound.selected.clone());
    let FunctionResultType::Scalar(result_type) = &selected.result_type else {
        panic!("scalar")
    };
    let result_type = result_type.clone();
    let rng_request = FunctionBindingRequest {
        arguments: &[],
        logical_argument_count: 0,
        expected_result_type: None,
    };
    let rng = functions
        .metadata()
        .resolve_bound_user("rand", FunctionKind::Scalar, rng_request, &Control)
        .unwrap();
    let rng_selected = Arc::new(rng.selected.clone());
    let fragment_id = FragmentId::new(u32::MAX);
    let source_node = NodeId::new(41);
    let project_node = NodeId::new(0);
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(source_node, Box::from([Box::default()]), Box::default())
        .unwrap();
    let rng_expr = builder
        .add_expression(
            project_node,
            ty.clone(),
            ExprKind::FunctionCall {
                function: physical_call(&rng),
                args: Box::default(),
            },
        )
        .unwrap();
    let literal = builder
        .add_expression(
            project_node,
            ty.clone(),
            if name == "if" {
                ExprKind::Literal(LiteralValue::Float64Bits(9.5f64.to_bits()))
            } else {
                ExprKind::Literal(LiteralValue::Null)
            },
        )
        .unwrap();
    let (args, shape, contexts, mut uses, domains) = if name == "if" {
        let condition = builder
            .add_expression(
                project_node,
                boolean,
                ExprKind::Literal(LiteralValue::Boolean(false)),
            )
            .unwrap();
        let contexts = vec![
            context(0, u32::MAX, EvaluationDemand::TruthOnly),
            context(20, 8, EvaluationDemand::Value),
            context(30, 9, EvaluationDemand::Value),
        ];
        let uses = vec![
            invocation(contexts[0], condition, ControlShape::Eager, &[]),
            invocation(contexts[1], rng_expr, ControlShape::Eager, &[]),
            invocation(contexts[2], literal, ControlShape::Eager, &[]),
        ];
        (
            vec![condition, rng_expr, literal],
            ControlShape::If,
            contexts,
            uses,
            vec![
                ExpressionEvaluationDomain {
                    id: EvaluationDomainId::new(8),
                    parent: Some(EvaluationDomainId::new(u32::MAX)),
                    guard: Some(DomainGuard {
                        owner: ExpressionUseId::new(10),
                        kind: GuardKind::IfThen,
                    }),
                },
                ExpressionEvaluationDomain {
                    id: EvaluationDomainId::new(9),
                    parent: Some(EvaluationDomainId::new(u32::MAX)),
                    guard: Some(DomainGuard {
                        owner: ExpressionUseId::new(10),
                        kind: GuardKind::IfElse,
                    }),
                },
            ],
        )
    } else {
        let contexts = vec![
            context(0, u32::MAX, EvaluationDemand::Value),
            context(20, 8, EvaluationDemand::Value),
        ];
        let uses = vec![
            invocation(contexts[0], literal, ControlShape::Eager, &[]),
            invocation(contexts[1], rng_expr, ControlShape::Eager, &[]),
        ];
        (
            vec![literal, rng_expr],
            ControlShape::Coalesce,
            contexts,
            uses,
            vec![ExpressionEvaluationDomain {
                id: EvaluationDomainId::new(8),
                parent: Some(EvaluationDomainId::new(u32::MAX)),
                guard: Some(DomainGuard {
                    owner: ExpressionUseId::new(10),
                    kind: GuardKind::CoalesceAfterNull { ordinal: 1 },
                }),
            }],
        )
    };
    let parent_context = context(10, u32::MAX, EvaluationDemand::Value);
    let root = builder
        .add_expression(
            project_node,
            result_type.clone(),
            ExprKind::FunctionCall {
                function: physical_call(&bound),
                args: args.into_boxed_slice(),
            },
        )
        .unwrap();
    let value = builder
        .add_value(
            result_type.clone(),
            ValueOrigin::Expr {
                node: project_node,
                expr: root,
            },
        )
        .unwrap();
    builder
        .add_project(
            project_node,
            source_node,
            Box::from([(root, value)]),
            Box::from([value]),
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            project_node,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap()
        .with_call_requests_observed(
            vec![
                (
                    novarocks_physical_plan::PhysicalCallDefinition::Expression(rng_expr),
                    novarocks_physical_plan::PhysicalCallRequest {
                        arguments: Box::default(),
                        logical_argument_count: rng_request.logical_argument_count,
                        expected_result_type: rng_request.expected_result_type.cloned(),
                        constant_policy: options().constants,
                    },
                ),
                (
                    novarocks_physical_plan::PhysicalCallDefinition::Expression(root),
                    novarocks_physical_plan::PhysicalCallRequest {
                        arguments: request
                            .arguments
                            .iter()
                            .map(|argument| {
                                let FunctionArgument::Value {
                                    value_type,
                                    constant: None,
                                } = argument
                                else {
                                    panic!(
                                        "original control fixture has nonconstant Value channels"
                                    )
                                };
                                novarocks_physical_plan::StaticFunctionArgument::Value {
                                    value_type: value_type.clone(),
                                    constant: None,
                                }
                            })
                            .collect::<Vec<_>>()
                            .into_boxed_slice(),
                        logical_argument_count: request.logical_argument_count,
                        expected_result_type: request.expected_result_type.cloned(),
                        constant_policy: options().constants,
                    },
                ),
            ],
            &Control,
        )
        .unwrap();
    uses.push(invocation(
        parent_context,
        root,
        shape,
        &contexts.iter().map(|c| c.use_id.get()).collect::<Vec<_>>(),
    ));
    let mut all_domains = vec![ExpressionEvaluationDomain {
        id: EvaluationDomainId::new(u32::MAX),
        parent: None,
        guard: None,
    }];
    all_domains.extend(domains);
    let flow = ExpressionControlFlow::try_new(
        all_domains,
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control).unwrap();
    assert_eq!(roots.sites().len(), 1);
    let expression_uses = PhysicalRootUses::try_new(
        &fragment,
        flow.clone(),
        vec![(*roots.sites().keys().next().unwrap(), parent_context.use_id)],
        &Control,
    )
    .unwrap();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let rng_context = contexts[1];
    let rng_token = functions
        .prepare_fresh(
            CallEffectInput {
                context: rng_context,
                argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&[]),
                function_id: &rng.function_id,
                kind: rng.kind,
                selected: rng_selected.as_ref(),
                request: rng_request,
                environment: &[],
                parameters: &parameters,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                proof_scope: CallProofScope::Domain(rng_context.domain),
            },
            rng_selected.clone(),
            PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(rng_context),
            },
            &Control,
        )
        .unwrap();
    let mut children = ScopedExpressionEffects::pure_value(parent_context);
    for (ordinal, &child_context) in contexts.iter().enumerate() {
        let child = if ordinal == 1 {
            rng_token.effects()
        } else {
            ScopedExpressionEffects::pure_value(child_context)
        };
        children = children
            .join_control_argument(child, &flow, ordinal)
            .unwrap();
    }
    let child_uses = contexts.iter().map(|c| Some(c.use_id)).collect::<Vec<_>>();
    let token = functions
        .prepare_fresh(
            CallEffectInput {
                context: parent_context,
                argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&child_uses),
                function_id: &bound.function_id,
                kind: bound.kind,
                selected: selected.as_ref(),
                request,
                environment: &[],
                parameters: &parameters,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                proof_scope: CallProofScope::Domain(parent_context.domain),
            },
            selected.clone(),
            PureCallPreparation::ControlIntrinsic {
                arguments: children,
            },
            &Control,
        )
        .unwrap();
    let mut parent_effects = token.call_contract().effects().clone();
    match claims {
        Claims::Accurate => {}
        Claims::WrongControl => parent_effects.argument_control = ArgumentControl::Eager,
        Claims::WrongNullBehavior => parent_effects.null_behavior = FunctionNullBehavior::Strict,
        Claims::WrongStability => parent_effects.value_stability = FunctionVolatility::Volatile,
    }
    let calls = FrozenFragmentCalls::try_new(
        &fragment,
        &expression_uses,
        vec![
            FrozenPhysicalCall {
                materialized_writer_input: None,
                regexp_count_pattern_source: None,
                to_base64_byte_source: None,
                temporal_source: None,
                site: PhysicalCallSite::Expression(parent_context.use_id),
                context: parent_context,
                effects: parent_effects,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            },
            FrozenPhysicalCall {
                materialized_writer_input: None,
                regexp_count_pattern_source: None,
                to_base64_byte_source: None,
                temporal_source: None,
                site: PhysicalCallSite::Expression(rng_context.use_id),
                context: rng_context,
                effects: rng_token.call_contract().effects().clone(),
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            },
        ],
        &Control,
    )?;
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: fragment.nodes()[&project_node].output.clone(),
        fields: Box::from([ResultField {
            domain: novarocks_physical_plan::ResultValueDomain::Plain,
            name: "controlled_sample".into(),
            alias: None,
            value,
            ty: result_type,
        }]),
    };
    Ok(Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([17; 16]).unwrap(),
                required: RequiredContracts::default(),
                constants: novarocks_physical_plan::ConstantPools::empty(),
                fragment,
                expression_uses,
                calls,
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                pruning: FrozenFragmentPruning::try_new(fragment_id, vec![], &Control).unwrap(),
            },
            package_admission(),
            &Control,
        )
        .unwrap(),
    ))
}

fn compile(
    source: Arc<FragmentPackage>,
    functions: &PureEngineFunctionCatalog,
) -> Result<novarocks_local_program::LocalProgram, FragmentCompileError> {
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Control).unwrap();
    compile_fragment(
        validate_fragment_providers(source, &providers, &Control).unwrap(),
        functions,
        options(),
        &Control,
    )
}

fn assert_control_call(name: &str, expected: ControlShape, argument_control: ArgumentControl) {
    let functions = catalogue();
    let source = package(&functions, name, Claims::Accurate).unwrap();
    let program = compile(source, &functions).unwrap();
    let resolved = program.checked().channels().expressions().resolved_calls();
    assert_eq!(resolved.calls().len(), 2);
    let flow = &resolved.snapshot().flows()[&ProgramExpressionArena::Main];
    let parent = &flow.uses()[&ExpressionUseId::new(10)];
    assert_eq!(parent.control, expected);
    assert_eq!(
        parent.context,
        context(10, u32::MAX, EvaluationDemand::Value)
    );
    let (site, call) = resolved
        .calls()
        .iter()
        .find(|(site, _)| {
            matches!(site,
        ProgramCallSite::Expression(occurrence) if occurrence.use_id == ExpressionUseId::new(10))
        })
        .unwrap();
    assert_eq!(
        call.specialization().source(),
        PurePreparationSource::Frozen
    );
    assert_eq!(
        call.specialization().implementation().abi,
        PureKernelAbi::ControlIntrinsicV1
    );
    assert_eq!(
        call.call_contract().effects().argument_control,
        argument_control
    );
    assert_eq!(
        call.call_contract().effects().null_behavior,
        FunctionNullBehavior::ControlDefined
    );
    assert_eq!(
        call.call_contract().effects().instance_state,
        FunctionInstanceState::None
    );
    // A conservative parent summary retains the guarded RAND's effects; this
    // does not hoist it or give the parent a scalar instance.
    assert_eq!(
        call.effects()
            .for_use(parent.context)
            .unwrap()
            .value_stability,
        FunctionVolatility::Volatile
    );
    assert!(
        call.effects()
            .for_use(parent.context)
            .unwrap()
            .has_instance_state
    );
    assert!(
        call.effects()
            .for_use(parent.context)
            .unwrap()
            .observable_effects
            .rng_sampling
    );
    let ProgramStateTemplate::ControlIntrinsic {
        scope,
        call: retained,
    } = program.state_template(*site).unwrap()
    else {
        panic!("actual control intrinsic")
    };
    assert_eq!(scope.occurrence.use_id, parent.context.use_id);
    assert!(std::ptr::eq(retained, call.call_contract()));
    assert_eq!(parent.arguments[1], ExpressionUseId::new(20));
    let guarded = &flow.uses()[&ExpressionUseId::new(20)];
    assert_eq!(guarded.context, context(20, 8, EvaluationDemand::Value));
    assert_eq!(
        flow.domains()[&EvaluationDomainId::new(8)].guard.unwrap(),
        DomainGuard {
            owner: ExpressionUseId::new(10),
            kind: if name == "if" {
                GuardKind::IfThen
            } else {
                GuardKind::CoalesceAfterNull { ordinal: 1 }
            },
        }
    );
    if name == "if" {
        assert_eq!(
            flow.uses()[&ExpressionUseId::new(0)].context.demand,
            EvaluationDemand::TruthOnly
        );
        assert_eq!(
            flow.uses()[&ExpressionUseId::new(30)].context.domain,
            EvaluationDomainId::new(9)
        );
        assert_eq!(
            flow.domains()[&EvaluationDomainId::new(9)]
                .guard
                .unwrap()
                .kind,
            GuardKind::IfElse
        );
    } else {
        assert_eq!(
            flow.uses()[&ExpressionUseId::new(0)].context,
            context(0, u32::MAX, EvaluationDemand::Value)
        );
    }
}

#[test]
fn if_compilation_retains_exact_guards_control_abi_and_volatile_child_summary() {
    assert_control_call("if", ControlShape::If, ArgumentControl::If);
}
#[test]
fn coalesce_compilation_retains_after_null_domain_and_volatile_child_summary() {
    assert_control_call(
        "coalesce",
        ControlShape::Coalesce,
        ArgumentControl::Coalesce,
    );
}
#[test]
fn frozen_control_claim_cannot_replace_actual_flow_shape() {
    let functions = catalogue();
    for name in ["if", "coalesce"] {
        assert!(matches!(
            package(&functions, name, Claims::WrongControl),
            Err(FrozenCallError::WrongControl)
        ));
    }
}
#[test]
fn structurally_checked_changed_control_facts_fail_exact_owner_frozen_compilation() {
    let functions = catalogue();
    for name in ["if", "coalesce"] {
        assert!(matches!(
            package(&functions, name, Claims::WrongNullBehavior),
            Err(FrozenCallError::InvalidEffects(_))
        ));
        let source = package(&functions, name, Claims::WrongStability).unwrap();
        let error = compile(source, &functions).unwrap_err();
        let FragmentCompileError::Owner {
            phase: "expressions",
            error,
        } = error
        else {
            panic!("typed expression owner refusal: {error:?}")
        };
        assert!(matches!(
            error.downcast_ref::<crate::expressions::ExpressionLoweringError>(),
            Some(crate::expressions::ExpressionLoweringError::Specialization(
                novarocks_functions::FunctionSpecializationFailure::InvalidInput(_)
            ))
        ));
    }
}

// Conservative retained-source invoice and independent projection ceilings for
// these small fixtures only; this is not a production default or a MEM grant.
fn package_admission() -> novarocks_physical_plan::FragmentPackageAdmission {
    novarocks_physical_plan::FragmentPackageAdmission {
        plan_limits: novarocks_physical_plan::PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: novarocks_physical_plan::PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}
