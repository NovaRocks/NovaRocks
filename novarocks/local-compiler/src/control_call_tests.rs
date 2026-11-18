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

use novarocks_expression::evaluator_view::{UnitStateTemplate, state_template};
use novarocks_expression_contract::{
    BoundFunction, DefinitionControl, ExprId, ExprKind, ExpressionCallClaim, RootScope, SealedUnit,
    UnitError,
};

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
use novarocks_local_program::{KernelAbiVersion, ProgramNodeKind};
use novarocks_physical_plan::{
    FragmentBuilder, FragmentCuts, FragmentId, FragmentPackage, FragmentPackageInput, FragmentSink,
    FrozenFragmentPruning, NodeId, PipelineDopDomain, PlanVersionId, RequiredContracts,
    ResultField, ResultPort, ValueOrigin,
};
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, CompileControlError, CompilePhase, ControlShape,
    DecimalOverflowPolicy, EvaluationDemand, ExpressionUseId, FunctionInstanceState,
    FunctionNullBehavior, FunctionValueType, FunctionVolatility, GuardKind, PureCompileControl,
    RegionGuard, SemanticParameters,
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
        legacy_metadata: Some(novarocks_expression_contract::LegacyBindingMetadata {
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
) -> Result<Arc<FragmentPackage>, UnitError> {
    let ty = FunctionValueType::new(DataType::Float64, true);
    let boolean = FunctionValueType::new(DataType::Boolean, false);
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
    let FunctionResultType::Scalar(rng_type) = &rng_selected.result_type else {
        panic!("scalar RAND")
    };
    let arguments = if name == "if" {
        vec![
            value_argument(&boolean),
            value_argument(rng_type),
            value_argument(&ty),
        ]
    } else {
        vec![value_argument(&ty), value_argument(rng_type)]
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
        panic!("scalar control")
    };
    let result_type = result_type.clone();
    let fragment_id = FragmentId::new(u32::MAX);
    let source_node = NodeId::new(41);
    let project_node = NodeId::new(0);
    let mut constants = novarocks_constant_contract::ConstantPools::empty();
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(source_node, Box::from([Box::default()]), Box::default())
        .unwrap();
    let rng_expr = builder.unit(project_node).unwrap().add(
        None,
        rng_type.clone(),
        ExprKind::FunctionCall {
            function: physical_call(&rng),
            args: Box::default(),
        },
    )?;
    let literal_kind = if name == "if" {
        crate::test_constants::f64_bits(&mut constants, &ty, 9.5f64.to_bits(), options().constants)
    } else {
        crate::test_constants::null(&mut constants, &ty, options().constants)
    };
    let literal = builder
        .unit(project_node)
        .unwrap()
        .add(None, ty.clone(), literal_kind)?;
    let (args, shape) = if name == "if" {
        let kind =
            crate::test_constants::boolean(&mut constants, &boolean, false, options().constants);
        let condition = builder
            .unit(project_node)
            .unwrap()
            .add(None, boolean, kind)?;
        (vec![condition, rng_expr, literal], ControlShape::If)
    } else {
        (vec![literal, rng_expr], ControlShape::Coalesce)
    };
    let definition = builder.unit(project_node).unwrap().add(
        None,
        result_type.clone(),
        ExprKind::FunctionCall {
            function: physical_call(&bound),
            args: args.into_boxed_slice(),
        },
    )?;
    for (id, request, shape) in [
        (rng_expr, rng_request, ControlShape::Eager),
        (definition, request, shape),
    ] {
        let arguments = request
            .arguments
            .iter()
            .map(|argument| match argument {
                FunctionArgument::Value {
                    value_type,
                    constant: None,
                } => novarocks_physical_plan::StaticFunctionArgument::Value {
                    value_type: value_type.clone(),
                    constant: None,
                },
                _ => panic!("original fixture declares nonconstant Value request channels"),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let draft = builder.unit(project_node).unwrap();
        draft.request(
            id,
            novarocks_expression_contract::PhysicalCallRequest {
                arguments,
                logical_argument_count: request.logical_argument_count,
                expected_result_type: None,
                constant_policy: options().constants,
            },
        )?;
        draft.definition_control(
            id,
            DefinitionControl {
                shape,
                temporal_sources: None,
            },
        )?;
    }
    let root = builder.unit(project_node).unwrap().root(
        definition,
        EvaluationDemand::Value,
        RootScope::Port(0),
    )?;
    let value = builder
        .add_value(
            result_type.clone(),
            ValueOrigin::Expr {
                node: project_node,
                root,
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
    let ports = builder.node_ports(project_node).unwrap();
    let mut work =
        novarocks_type_contract::CompileCheckpoints::try_new(&Control, CompilePhase::Validate)?;
    let sealed = builder
        .take_unit_builder(project_node)
        .unwrap()
        .seal(ports, &mut work)?;
    let parameters = SemanticParameters::try_new([]).unwrap();
    let owners = BTreeMap::from([
        (rng_expr, (rng_selected, rng_request)),
        (definition, (selected, request)),
    ]);
    fn summary<'a>(
        functions: &PureEngineFunctionCatalog,
        sealed: &SealedUnit,
        owners: &BTreeMap<
            ExprId,
            (
                Arc<novarocks_functions::FunctionBindingSelection>,
                FunctionBindingRequest<'a>,
            ),
        >,
        id: ExpressionUseId,
        parameters: &SemanticParameters,
        summaries: &mut BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
        claims: &mut BTreeMap<ExpressionUseId, ExpressionCallClaim>,
    ) -> ScopedExpressionEffects {
        if let Some(value) = summaries.get(&id) {
            return *value;
        }
        let flow = sealed.control_flow();
        let invocation = &flow.uses()[&id];
        let mut children = ScopedExpressionEffects::pure_value(invocation.context);
        for (ordinal, &child) in invocation.arguments.iter().enumerate() {
            children = children
                .join_control_argument(
                    summary(
                        functions, sealed, owners, child, parameters, summaries, claims,
                    ),
                    flow,
                    ordinal,
                )
                .unwrap();
        }
        let result = if let Some((selected, request)) = owners.get(&invocation.definition) {
            let ExprKind::FunctionCall { function, .. } =
                &sealed.definition(invocation.definition).unwrap().kind
            else {
                unreachable!()
            };
            let uses = invocation
                .arguments
                .iter()
                .copied()
                .map(Some)
                .collect::<Vec<_>>();
            let options = if invocation.control == ControlShape::Eager {
                PureCallPreparation::Scalar {
                    arguments: children,
                }
            } else {
                PureCallPreparation::ControlIntrinsic {
                    arguments: children,
                }
            };
            let token = functions
                .prepare_fresh(
                    CallEffectInput {
                        context: invocation.context.into(),
                        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(
                            &uses,
                        ),
                        function_id: &function.function_id,
                        kind: function.kind,
                        selected: selected.as_ref(),
                        request: *request,
                        environment: &[],
                        parameters,
                        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                        proof_scope: CallProofScope::Region(invocation.context.region),
                    },
                    selected.clone(),
                    options,
                    &Control,
                )
                .unwrap();
            claims.insert(
                id,
                ExpressionCallClaim {
                    context: invocation.context,
                    effects: token.call_contract().effects().clone(),
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    temporal_source: None,
                    regexp_count_pattern_source: None,
                    to_base64_byte_source: None,
                },
            );
            ScopedExpressionEffects::primitive(
                invocation.context,
                token.effects().for_use(invocation.context).unwrap(),
            )
        } else {
            children
        };
        summaries.insert(id, result);
        result
    }
    let mut summaries = BTreeMap::new();
    let mut authored = BTreeMap::new();
    let parent = sealed.root_use_ids()[0];
    summary(
        functions,
        &sealed,
        &owners,
        parent,
        &parameters,
        &mut summaries,
        &mut authored,
    );
    let parent_effects = &mut authored.get_mut(&parent).unwrap().effects;
    match claims {
        Claims::Accurate => {}
        Claims::WrongControl => parent_effects.argument_control = ArgumentControl::Eager,
        Claims::WrongNullBehavior => parent_effects.null_behavior = FunctionNullBehavior::Strict,
        Claims::WrongStability => parent_effects.value_stability = FunctionVolatility::Volatile,
    }
    let unit = sealed.finish(authored, &mut work)?;
    builder.install_unit(project_node, Arc::new(unit)).unwrap();
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
        .unwrap();
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
                constants,
                fragment,
                statement_constant_policy: options().constants,
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                pruning: FrozenFragmentPruning::try_new(fragment_id, vec![], &Control).unwrap(),
                layouts: Default::default(),
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
    assert!(program.resolved_calls().calls().is_empty());
    let project = &program.graph().nodes()[program.graph().root().index()];
    let ProgramNodeKind::Project { exprs, .. } = project.kind() else {
        panic!("actual control project")
    };
    let unit = program.unit(exprs[0].unit).unwrap();
    let root = unit.root(exprs[0].root).unwrap();
    assert_eq!(unit.calls().len(), 2);
    let flow = unit.control_flow();
    let parent = &flow.uses()[&root.use_id];
    assert_eq!(parent.control, expected);
    assert_eq!(parent.context.demand, EvaluationDemand::Value);
    let call = &unit.calls()[&root.use_id];
    assert_eq!(call.source(), PurePreparationSource::Frozen);
    assert_eq!(call.implementation().abi, PureKernelAbi::ControlIntrinsicV1);
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
    let effects = unit.effects()[&root.use_id]
        .for_use(parent.context)
        .unwrap();
    assert_eq!(effects.value_stability, FunctionVolatility::Volatile);
    assert!(effects.has_instance_state);
    assert!(effects.observable_effects.rng_sampling);
    let UnitStateTemplate::ControlIntrinsic {
        scope,
        call: retained,
    } = state_template(unit, root.use_id).unwrap()
    else {
        panic!("actual control intrinsic")
    };
    assert_eq!(scope.occurrence, parent.context.use_id);
    assert!(std::ptr::eq(retained, call.call_contract()));
    let guarded = &flow.uses()[&parent.arguments[1]];
    assert_eq!(guarded.context.demand, EvaluationDemand::Value);
    assert_eq!(
        flow.regions()[&guarded.context.region].guard.unwrap(),
        RegionGuard {
            owner: parent.context.use_id,
            kind: if name == "if" {
                GuardKind::IfThen
            } else {
                GuardKind::CoalesceAfterNull { ordinal: 1 }
            }
        }
    );
    if name == "if" {
        assert_eq!(
            flow.uses()[&parent.arguments[0]].context.demand,
            EvaluationDemand::TruthOnly
        );
        let otherwise = &flow.uses()[&parent.arguments[2]];
        assert_ne!(guarded.context.region, otherwise.context.region);
        assert_eq!(
            flow.regions()[&otherwise.context.region]
                .guard
                .unwrap()
                .kind,
            GuardKind::IfElse
        );
    } else {
        assert_eq!(
            flow.uses()[&parent.arguments[0]].context.region,
            parent.context.region
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
            Err(UnitError::Invalid {message,..}) if message=="call claim context/control differs from its invocation"
        ));
    }
}
#[test]
fn structurally_checked_changed_control_facts_fail_exact_owner_frozen_compilation() {
    let functions = catalogue();
    for name in ["if", "coalesce"] {
        assert!(matches!(
            package(&functions, name, Claims::WrongNullBehavior),
            Err(UnitError::Effects(
                novarocks_type_contract::EffectContractError::ControlMismatch
            ))
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
            Some(crate::expressions::ExpressionLoweringError::Unit(
                novarocks_expression::ExprCompileError::Specialization(
                    novarocks_functions::FunctionSpecializationFailure::InvalidInput(_)
                )
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
