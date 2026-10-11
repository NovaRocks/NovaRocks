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

use super::*;
use arrow_array::Float64Array;
use arrow_schema::DataType;
use novarocks_functions::{
    CallEffectInput, ConstantPolicy, EngineFunctionCatalogBuilder, FunctionBindingRequest,
    FunctionId, FunctionKind, FunctionOverloadId, FunctionResultType, InstalledPureKernel,
    KernelEvaluationControl, KernelFailure, PureCallPreparation, PureEngineFunctionCatalog,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi, PurePreparationSource,
    ScalarEvaluationInstance, ScopedExpressionEffects, Selection,
};
use novarocks_local_program::{
    KernelAbiVersion, ProgramCallSite, ProgramExpressionArena, ProgramExpressionRootSite,
    ProgramNodeExpressionRole, ProgramNodeId, ProgramNodeKind, ProgramStateTemplate,
    StaticExprKind, StaticSinkProgram,
};
use novarocks_physical_plan::{
    BoundFunction, ExprKind, FragmentBuilder, FragmentCuts, FragmentId, FragmentPackageInput,
    FragmentSink, FrozenFragmentCalls, FrozenFragmentPruning, FrozenPhysicalCall, LiteralValue,
    NodeId, PhysicalCallSite, PhysicalExpressionRoots, PhysicalRootUses, PipelineDopDomain,
    PlanVersionId, RequiredContracts, ResultField, ResultPort, ValueOrigin,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, ControlShape, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext, ExpressionEvaluationDomain,
    ExpressionInvocation, ExpressionUseId, FunctionInstanceState, FunctionValueType,
    PureCompileControl, SemanticParameters,
};
use std::{
    collections::BTreeMap,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Duration,
};

struct FixtureControl;
impl PureCompileControl for FixtureControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn rng_subset() -> PureEngineFunctionCatalog {
    let actual =
        novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let definition = actual
        .definition("rand", FunctionKind::Scalar)
        .unwrap()
        .clone();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(definition).unwrap();
    // This deliberately seals a real RAND-only subset, not the Server catalogue.
    // These independent records name the actual installed implementation ABI.
    builder
        .seal_pure(
            [
                "builtin.scalar/rand/()->f64;strict;legacy",
                "builtin.scalar/rand/(i64)->f64;strict;legacy",
            ]
            .into_iter()
            .map(|overload| InstalledPureKernel {
                function: FunctionId::try_new("builtin.scalar/rand/v1").unwrap(),
                kind: FunctionKind::Scalar,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(overload).unwrap(),
                    implementation: PureImplementationId::try_new(
                        "builtin.scalar/rand/selected-v1",
                    )
                    .unwrap(),
                    abi: PureKernelAbi::ScalarV1,
                },
                aggregate_state_format: None,
            }),
        )
        .unwrap()
}

fn options(dop: usize) -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(dop).unwrap(),
        root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        exchange_wait: std::time::Duration::from_secs(120),
        // Explicit fixture admission; these values are not production defaults.
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

fn package(functions: &PureEngineFunctionCatalog, projections: usize) -> Arc<FragmentPackage> {
    package_with_case(functions, projections, PackageCase::Ordinary)
}

#[derive(Clone, Copy)]
enum PackageCase {
    Ordinary,
    EmptyProject,
    NestedProject,
    PowerOfTwoDomain,
    DuplicateProducedValue,
    ForgedEffects,
    MissingFunction,
    TwoEmptyRows,
}

fn package_with_case(
    functions: &PureEngineFunctionCatalog,
    projections: usize,
    case: PackageCase,
) -> Arc<FragmentPackage> {
    assert!(projections > 0 || matches!(case, PackageCase::EmptyProject));
    let request = FunctionBindingRequest {
        arguments: &[],
        logical_argument_count: 0,
        expected_result_type: None,
    };
    let bound = functions
        .metadata()
        .resolve_bound_user("rand", FunctionKind::Scalar, request, &FixtureControl)
        .unwrap();
    let selected = Arc::new(bound.selected.clone());
    let FunctionResultType::Scalar(result_type) = &selected.result_type else {
        panic!("RAND result is scalar")
    };
    let result_type = result_type.clone();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let fragment_id = FragmentId::new(27);
    // Child-first order is MAX -> 0 -> 41 -> 7, not BTreeMap numeric order.
    let values_node = NodeId::new(u32::MAX);
    let filter_node = NodeId::new(0);
    let project_node = NodeId::new(41);
    let limit_node = NodeId::new(7);
    let mut builder = FragmentBuilder::new(fragment_id);
    builder
        .add_values(
            values_node,
            if matches!(case, PackageCase::TwoEmptyRows) {
                vec![Box::default(), Box::default()].into_boxed_slice()
            } else {
                Box::from([Box::default()])
            },
            Box::default(),
        )
        .unwrap();
    let predicate = builder
        .add_expression(
            filter_node,
            FunctionValueType::new(DataType::Boolean, false),
            ExprKind::Literal(LiteralValue::Boolean(true)),
        )
        .unwrap();
    builder
        .add_filter(filter_node, values_node, Box::from([predicate]))
        .unwrap();
    let mut original_requests = Vec::new();
    let mut projected = Vec::new();
    let mut output = Vec::new();
    for _ in 0..projections {
        let function = BoundFunction {
            function_id: if matches!(case, PackageCase::MissingFunction) {
                FunctionId::try_new("fixture.uninstalled.scalar/zero/v1").unwrap()
            } else {
                bound.function_id.clone()
            },
            overload: selected.overload.clone(),
            kind: bound.kind,
            argument_types: selected.argument_types.clone(),
            result_type: result_type.clone(),
            legacy_metadata: Some(novarocks_physical_plan::LegacyBindingMetadata {
                volatility: bound.semantics.volatility,
                argument_evaluation: bound.semantics.argument_evaluation,
                failure_behavior: bound.semantics.failure_behavior,
                intrinsic_row_error: bound.semantics.intrinsic_row_error,
                semantic_parameters: Box::default(),
            }),
        };
        let expr = builder
            .add_expression(
                project_node,
                result_type.clone(),
                ExprKind::FunctionCall {
                    function,
                    args: Box::default(),
                },
            )
            .unwrap();
        original_requests.push((
            novarocks_physical_plan::PhysicalCallDefinition::Expression(expr),
            novarocks_physical_plan::PhysicalCallRequest {
                arguments: Box::default(),
                logical_argument_count: request.logical_argument_count,
                expected_result_type: request.expected_result_type.cloned(),
                constant_policy: options(1).constants,
            },
        ));
        let value = builder
            .add_value(
                result_type.clone(),
                ValueOrigin::Expr {
                    node: project_node,
                    expr,
                },
            )
            .unwrap();
        projected.push((expr, value));
        output.push(value);
    }
    if matches!(case, PackageCase::DuplicateProducedValue) {
        // Repeated expression/value entries remain a checked physical input;
        // this first compiler family must not execute them as fresh values.
        projected.push(projected[0]);
        output.push(output[0]);
    }
    builder
        .add_project(
            project_node,
            filter_node,
            projected.into_boxed_slice(),
            output.clone().into_boxed_slice(),
        )
        .unwrap();
    let limit_input = if matches!(case, PackageCase::NestedProject) {
        let final_project = NodeId::new(42);
        let expr = builder
            .add_expression(
                final_project,
                result_type.clone(),
                ExprKind::Value(output[0]),
            )
            .unwrap();
        let value = builder
            .add_value(
                result_type.clone(),
                ValueOrigin::Expr {
                    node: final_project,
                    expr,
                },
            )
            .unwrap();
        builder
            .add_project(
                final_project,
                project_node,
                Box::from([(expr, value)]),
                Box::from([value]),
            )
            .unwrap();
        output = vec![value];
        final_project
    } else {
        project_node
    };
    builder
        .add_limit(limit_node, limit_input, Some(1), 0)
        .unwrap();
    let fragment = builder
        .finish_definition(
            limit_node,
            FragmentSink::Result,
            PipelineDopDomain {
                min: 1,
                max: if matches!(case, PackageCase::PowerOfTwoDomain) {
                    4
                } else {
                    1
                },
                requires_power_of_two: matches!(case, PackageCase::PowerOfTwoDomain),
            },
        )
        .unwrap()
        .with_call_requests_observed(original_requests, &FixtureControl)
        .unwrap();
    let roots = PhysicalExpressionRoots::try_new(&fragment, &FixtureControl).unwrap();
    let domain = EvaluationDomainId::new(u32::MAX);
    let mut uses = Vec::new();
    let mut bindings = Vec::new();
    let mut calls = Vec::new();
    for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
        let use_id = ExpressionUseId::new(u32::MAX - ordinal as u32);
        let context = ExpressionEffectContext {
            use_id,
            domain,
            demand: root.demand,
        };
        uses.push(ExpressionInvocation {
            context,
            definition: root.expr,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        bindings.push((*site, use_id));
        if matches!(
            fragment.expressions().get(root.expr).unwrap().kind,
            ExprKind::FunctionCall { .. }
        ) {
            let authored = functions
                .prepare_fresh(
                    CallEffectInput {
                        context,
                        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&[]),
                        function_id: &bound.function_id,
                        kind: bound.kind,
                        selected: selected.as_ref(),
                        request,
                        environment: &[],
                        parameters: &parameters,
                        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                        proof_scope: novarocks_type_contract::CallProofScope::Domain(domain),
                    },
                    selected.clone(),
                    PureCallPreparation::Scalar {
                        arguments: ScopedExpressionEffects::pure_value(context),
                    },
                    &FixtureControl,
                )
                .unwrap();
            calls.push(FrozenPhysicalCall {
                regexp_count_pattern_source: None,
                to_base64_byte_source: None,
                temporal_source: None,
                site: PhysicalCallSite::Expression(use_id),
                context,
                effects: authored.call_contract().effects().clone(),
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            });
        }
    }
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        uses,
        fragment.expressions(),
        CompilePhase::Validate,
        &FixtureControl,
    )
    .unwrap();
    let expression_uses =
        PhysicalRootUses::try_new(&fragment, flow, bindings, &FixtureControl).unwrap();
    if matches!(case, PackageCase::ForgedEffects) {
        // Structurally valid claims deliberately contradict the actual RAND
        // implementation. Only exact-owner frozen preparation can reject this.
        calls[0].effects.value_stability = novarocks_functions::FunctionVolatility::Immutable;
    }
    let calls =
        FrozenFragmentCalls::try_new(&fragment, &expression_uses, calls, &FixtureControl).unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: fragment.nodes()[&limit_node].output.clone(),
        fields: output
            .into_iter()
            .enumerate()
            .map(|(ordinal, value)| ResultField {
                domain: novarocks_physical_plan::ResultValueDomain::Plain,
                name: format!("sample_{ordinal}").into_boxed_str(),
                alias: None,
                value,
                ty: result_type.clone(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    };
    Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([27; 16]).unwrap(),
                required: RequiredContracts::default(),
                constants: novarocks_physical_plan::ConstantPools::empty(),
                pruning: FrozenFragmentPruning::try_new(fragment_id, vec![], &FixtureControl)
                    .unwrap(),
                fragment,
                expression_uses,
                calls,
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters,
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
            },
            package_admission(),
            &FixtureControl,
        )
        .unwrap(),
    )
}

fn providers(package: Arc<FragmentPackage>) -> ProviderValidatedFragment {
    let catalog =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &FixtureControl)
            .unwrap();
    validate_fragment_providers(package, &catalog, &FixtureControl).unwrap()
}

#[test]
fn scalar_result_lowering_preserves_one_empty_row_sparse_sources_and_full_owner() {
    let functions = rng_subset();
    let source = package(&functions, 2);
    let program = compile_fragment(
        providers(source.clone()),
        &functions,
        options(1),
        &FixtureControl,
    )
    .unwrap();
    let graph = program.graph();
    assert_eq!(graph.nodes().len(), 4);
    assert_eq!(graph.root(), ProgramNodeId::new(3));
    for (node, physical) in graph.nodes().iter().zip([u32::MAX, 0, 41, 7]) {
        assert_eq!(node.physical_sources()[0].get(), physical);
        assert!(node.legacy_native_node_id().is_none());
    }
    let ProgramNodeKind::Values { values } = graph.nodes()[0].kind() else {
        panic!("actual Values")
    };
    assert_eq!(values.batch().unwrap().num_rows(), 1);
    assert_eq!(values.batch().unwrap().num_columns(), 0);
    assert!(matches!(graph.sink(), Some(StaticSinkProgram::Result)));
    assert_eq!(graph.profile().pipeline_dop().get(), 1);
    assert_eq!(graph.profile().root_sink_dop().unwrap().get(), 1);
    assert_eq!(
        graph.nodes()[3].output_layout().schema().field(0).name(),
        "sample_0"
    );
    assert_eq!(
        graph.nodes()[3].output_layout().schema().field(1).name(),
        "sample_1"
    );
    let ProgramNodeKind::Project { exprs, .. } = graph.nodes()[2].kind() else {
        panic!("actual Project")
    };
    assert_ne!(exprs[0], exprs[1]);
    for expr in exprs {
        assert!(
            matches!(graph.expressions().node(*expr).unwrap().kind(), StaticExprKind::BoundCall { args } if args.is_empty())
        );
    }
    let resolved = program.checked().channels().expressions().resolved_calls();
    assert_eq!(resolved.calls().len(), 2);
    let roots = resolved.snapshot().bindings();
    for (site, call) in resolved.calls() {
        assert_eq!(
            call.specialization().source(),
            PurePreparationSource::Frozen
        );
        assert_eq!(
            call.call_contract().context().domain,
            EvaluationDomainId::new(u32::MAX)
        );
        assert_eq!(
            call.call_contract().context().demand,
            EvaluationDemand::Value
        );
        assert_eq!(
            call.call_contract().effects().instance_state,
            FunctionInstanceState::ScalarInstance
        );
        let ProgramCallSite::Expression(occurrence) = site else {
            panic!("scalar occurrence")
        };
        assert_eq!(occurrence.arena, ProgramExpressionArena::Main);
        // Call ordering follows sparse use IDs, so locate the exact root by use.
        let actual_root = roots
            .iter()
            .find(|(_, id)| **id == occurrence.use_id)
            .unwrap()
            .0;
        assert!(
            matches!(actual_root, ProgramExpressionRootSite::Node { node, role: ProgramNodeExpressionRole::ProjectOutput { .. } } if *node == ProgramNodeId::new(2))
        );
        let ProgramStateTemplate::Scalar { scope, kernel } = program.state_template(*site).unwrap()
        else {
            panic!("actual scalar state template")
        };
        assert_eq!(scope.root, *actual_root);
        assert_eq!(scope.occurrence, *occurrence);
        assert!(std::ptr::eq(kernel.contract().call(), call.call_contract()));
        assert_eq!(
            kernel.contract().call().decimal_overflow_policy(),
            DecimalOverflowPolicy::OutputNull
        );
    }
    assert_eq!(program.provenance().operators().len(), 4);
    assert!(program.write_recipes().is_empty());
}

struct EvaluationControl;
impl KernelEvaluationControl for EvaluationControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("RAND must not wait")
    }
}

#[test]
fn lowered_rand_templates_create_real_independent_instances_and_run_two_batches() {
    let functions = rng_subset();
    let program = compile_fragment(
        providers(package(&functions, 2)),
        &functions,
        options(1),
        &FixtureControl,
    )
    .unwrap();
    let sites: Vec<_> = program
        .checked()
        .channels()
        .expressions()
        .resolved_calls()
        .calls()
        .keys()
        .copied()
        .collect();
    let mut instances = Vec::new();
    for site in sites {
        let ProgramStateTemplate::Scalar { kernel, .. } = program.state_template(site).unwrap()
        else {
            panic!("scalar template")
        };
        instances.push(ScalarEvaluationInstance::instantiate(kernel.clone()).unwrap());
    }
    for instance in &mut instances {
        for rows in [2, 3] {
            let output = instance
                .evaluate(Selection::all(rows), &[], &EvaluationControl)
                .unwrap();
            assert!(output.errors().is_empty());
            let array = output
                .values()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(array.values().len(), rows);
            assert!(
                array
                    .values()
                    .iter()
                    .all(|value| (0.0..1.0).contains(value))
            );
        }
    }
    // Unseeded streams have no deterministic cross-instance numerical oracle;
    // both instances genuinely run the production owner and remain healthy.
    assert_eq!(
        instances[0].retained_upper_bound(),
        instances[1].retained_upper_bound()
    );
}

#[derive(Clone, Copy)]
enum Stop {
    Entry,
    Quantum,
    Positive,
    Call(usize),
}
struct RefusingControl {
    cause: CompileControlError,
    stop: Stop,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refused: Mutex<bool>,
}
impl RefusingControl {
    fn new(cause: CompileControlError, stop: Stop) -> Self {
        Self {
            cause,
            stop,
            trace: Mutex::new(vec![]),
            refused: Mutex::new(false),
        }
    }
}
impl PureCompileControl for RefusingControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut refused = self.refused.lock().unwrap();
        assert!(
            !*refused,
            "a primary control refusal must not be checked again"
        );
        let mut trace = self.trace.lock().unwrap();
        trace.push((phase, units));
        let stop = match self.stop {
            Stop::Entry => trace.len() == 1,
            Stop::Quantum => units == 256,
            Stop::Positive => units > 0,
            Stop::Call(index) => trace.len() == index,
        };
        if stop {
            *refused = true;
            Err(self.cause)
        } else {
            Ok(())
        }
    }
}
fn causes() -> [CompileControlError; 3] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ]
}

#[test]
fn actual_fragment_lowering_entry_control_is_typed_without_retry() {
    let functions = rng_subset();
    let source = package(&functions, 2);
    for cause in causes() {
        let control = RefusingControl::new(cause, Stop::Entry);
        let result = compile_fragment(providers(source.clone()), &functions, options(1), &control);
        assert!(matches!(result, Err(FragmentCompileError::Control(actual)) if actual == cause));
        assert_eq!(
            *control.trace.lock().unwrap(),
            vec![(CompilePhase::LowerProgram, 0)]
        );
    }
}

#[test]
fn wide_actual_rand_project_observes_interior_quantum_and_returns_no_partial_program() {
    let functions = rng_subset();
    let source = package(&functions, 320);
    for cause in causes() {
        let control = RefusingControl::new(cause, Stop::Quantum);
        let result = compile_fragment(providers(source.clone()), &functions, options(1), &control);
        assert!(matches!(result, Err(FragmentCompileError::Control(actual)) if actual == cause));
        let trace = control.trace.lock().unwrap();
        assert_eq!(trace.last().unwrap().1, 256);
        assert!(trace.iter().all(|(_, units)| *units <= 256));
    }
}

#[test]
fn physical_dop_domain_refusal_observes_ordinary_error_tail() {
    let functions = rng_subset();
    let source = package(&functions, 2);
    assert!(matches!(
        compile_fragment(
            providers(source.clone()),
            &functions,
            options(2),
            &FixtureControl
        ),
        Err(FragmentCompileError::Invalid(
            "DOP is outside physical domain"
        ))
    ));
    for cause in causes() {
        let control = RefusingControl::new(cause, Stop::Positive);
        let result = compile_fragment(providers(source.clone()), &functions, options(2), &control);
        assert!(matches!(result, Err(FragmentCompileError::Control(actual)) if actual == cause));
        assert_eq!(control.trace.lock().unwrap().last().unwrap().1, 1);
    }
}

#[test]
fn admitted_non_power_of_two_dop_and_wide_result_sink_are_refused() {
    let functions = rng_subset();
    let source = package_with_case(&functions, 2, PackageCase::PowerOfTwoDomain);
    assert!(matches!(
        compile_fragment(providers(source), &functions, options(3), &FixtureControl),
        Err(FragmentCompileError::Invalid(
            "DOP is outside physical domain"
        ))
    ));
    let mut wide_sink = options(1);
    wide_sink.root_sink_dop = Some(NonZeroUsize::new(2).unwrap());
    assert!(matches!(
        compile_fragment(
            providers(package(&functions, 2)),
            &functions,
            wide_sink,
            &FixtureControl
        ),
        Err(FragmentCompileError::Unsupported {
            node: None,
            feature: "result sink width for singleton source"
        })
    ));
}

#[test]
fn duplicate_produced_value_refuses_and_multiple_empty_source_rows_preserve_cardinality() {
    let functions = rng_subset();
    let duplicate = package_with_case(&functions, 2, PackageCase::DuplicateProducedValue);
    let project = &duplicate.fragment().nodes()[&NodeId::new(41)];
    assert_eq!(project.output.columns[0], project.output.columns[2]);
    assert!(matches!(
        compile_fragment(
            providers(duplicate),
            &functions,
            options(1),
            &FixtureControl
        ),
        Err(FragmentCompileError::Invalid(
            "independent project roots share a produced value"
        ))
    ));
    let two_rows = package_with_case(&functions, 2, PackageCase::TwoEmptyRows);
    let program =
        compile_fragment(providers(two_rows), &functions, options(1), &FixtureControl).unwrap();
    let ProgramNodeKind::Values { values } = program.graph().nodes()[0].kind() else {
        panic!("actual Values source");
    };
    assert_eq!(values.batch().unwrap().num_rows(), 2);
    assert_eq!(values.batch().unwrap().num_columns(), 0);
    assert_eq!(
        program
            .checked()
            .channels()
            .expressions()
            .resolved_calls()
            .calls()
            .len(),
        2
    );
}

#[test]
fn frozen_rand_claims_and_missing_installed_function_fail_exact_preparation() {
    let functions = rng_subset();
    let forged = package_with_case(&functions, 2, PackageCase::ForgedEffects);
    assert!(matches!(
        compile_fragment(providers(forged), &functions, options(1), &FixtureControl),
        Err(FragmentCompileError::Owner {
            phase: "expressions",
            ..
        })
    ));
    let missing = package_with_case(&functions, 2, PackageCase::MissingFunction);
    assert!(matches!(
        compile_fragment(providers(missing), &functions, options(1), &FixtureControl),
        Err(FragmentCompileError::Owner {
            phase: "expressions",
            ..
        })
    ));
}

#[test]
fn every_actual_small_fragment_callback_preserves_primary_control_and_exact_prefix() {
    let functions = rng_subset();
    let source = package(&functions, 2);
    let recorder = RefusingControl::new(CompileControlError::Cancelled, Stop::Call(usize::MAX));
    let _program =
        compile_fragment(providers(source.clone()), &functions, options(1), &recorder).unwrap();
    let successful = recorder.trace.lock().unwrap().clone();
    assert!(successful.iter().any(|(_, units)| *units > 0));
    for index in 1..=successful.len() {
        for cause in causes() {
            let control = RefusingControl::new(cause, Stop::Call(index));
            let result =
                compile_fragment(providers(source.clone()), &functions, options(1), &control);
            assert!(
                matches!(result, Err(FragmentCompileError::Control(actual)) if actual == cause),
                "callback {index} must preserve {cause:?}"
            );
            assert_eq!(
                *control.trace.lock().unwrap(),
                successful[..index],
                "callback {index} must preserve the actual original-control prefix"
            );
        }
    }
}

#[test]
fn metadata_static_selection_checks_exact_identity_overload_and_kind_without_effects() {
    let functions = rng_subset();
    let request = FunctionBindingRequest {
        arguments: &[],
        logical_argument_count: 0,
        expected_result_type: None,
    };
    let bound = functions
        .metadata()
        .resolve_bound_user("rand", FunctionKind::Scalar, request, &FixtureControl)
        .unwrap();
    // No effect context, environment, preparation options or executable token
    // is used: static author checks must also work for TypeOnly definitions.
    functions
        .metadata()
        .validate_frozen_selection(
            &bound.function_id,
            bound.kind,
            &bound.selected,
            request,
            &FixtureControl,
        )
        .unwrap();
    let missing = FunctionId::try_new("fixture.uninstalled.scalar/zero/v1").unwrap();
    assert!(matches!(
        functions.metadata().validate_frozen_selection(
            &missing,
            bound.kind,
            &bound.selected,
            request,
            &FixtureControl,
        ),
        Err(novarocks_functions::FunctionBindingError::UnknownFunction)
    ));
    let mut wrong_overload = bound.selected.clone();
    wrong_overload.overload =
        FunctionOverloadId::try_new("fixture.uninstalled.overload/zero/v1").unwrap();
    assert!(
        functions
            .metadata()
            .validate_frozen_selection(
                &bound.function_id,
                bound.kind,
                &wrong_overload,
                request,
                &FixtureControl,
            )
            .is_err()
    );
    assert!(
        functions
            .metadata()
            .validate_frozen_selection(
                &bound.function_id,
                FunctionKind::Aggregate,
                &bound.selected,
                request,
                &FixtureControl,
            )
            .is_err()
    );
}

#[path = "literal_lowering_tests.rs"]
mod literal_lowering_tests;

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

#[path = "project_metadata_tests.rs"]
mod project_metadata_tests;
