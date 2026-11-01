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

//! Shared lawful Native package and actual host fixture constructors.
//! This authors inputs for the original compiler; it is not a decoder or kernel.
use arrow::{array::ArrayRef, record_batch::RecordBatch};
use novarocks_execution::{
    runtime::function_set::ExecutionFunctionSetBuilder,
    runtime::{
        execution_runtime::{ExecutionRuntime, ExecutionRuntimeConfig},
        preparation_metadata::CompiledSchemaSite,
        query_memory::QueryMemoryBinding,
    },
};
use novarocks_execution_contract::TaskIdentity;
use novarocks_functions::{
    CallArgumentUses, CallEffectInput, ConstantPolicy, EngineFunctionCatalogBuilder,
    FunctionArgument, FunctionBindingRequest, FunctionKind, FunctionResultType,
    InstalledPureKernel, PureCallPreparation, PureEngineFunctionCatalog, ScopedExpressionEffects,
};
use novarocks_local_program::{
    LocalProgram, ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeKind,
};
use novarocks_native_adapter::backend_task_execution::node_test_support::NativeNodePreparation;
use novarocks_native_adapter::backend_task_execution::{
    CompiledPackageInterpreter, CompiledTaskOptions, PreparedProjectMetadataForTest,
    ProjectMetadataHost, ProjectMetadataJournal, TypeMaterializationHost,
    TypeMaterializationJournal, prepare_project_metadata_for_test,
};
use novarocks_physical_plan::*;
use novarocks_plan_codec::{
    physical_package_v2::{
        encode_fragment_package,
        test_support::{decode_limits, encode_limits},
    },
    resource_preflight_v2::FragmentDecodeResourceModel,
};
use novarocks_type_contract::*;
use novarocks_types::identity::{BackendProcessId, StageId, TaskId};
use prost::Message;
use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc, time::Duration};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn constants() -> ConstantPolicy {
    ConstantPolicy {
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
    }
}
fn functions_for_names(names: &[&str]) -> PureEngineFunctionCatalog {
    let actual = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut installed = vec![];
    for &name in names {
        let definition = actual.definition(name, FunctionKind::Scalar).unwrap();
        builder.register(definition.clone()).unwrap();
        let binding = definition.binding_declaration().unwrap();
        for overload in binding.overloads() {
            let declaration = actual
                .pure_overload_declaration_observed(
                    binding.function_id(),
                    definition.kind(),
                    &overload.identity,
                    &Control,
                )
                .unwrap();
            installed.push(InstalledPureKernel {
                function: binding.function_id().clone(),
                kind: definition.kind(),
                implementation: declaration.implementation().clone(),
                aggregate_state_format: None,
            });
        }
    }
    builder.seal_pure(installed).unwrap()
}
pub(super) fn runtime_for_names(
    binding: &QueryMemoryBinding,
    names: &[&str],
) -> Arc<ExecutionRuntime> {
    let actual = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
    let mut functions = ExecutionFunctionSetBuilder::new();
    for &name in names {
        functions
            .catalog_builder_mut()
            .register(
                actual
                    .definition(name, FunctionKind::Scalar)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
    }
    Arc::new(
        ExecutionRuntime::new(
            ExecutionRuntimeConfig {
                driver_threads: 1,
                exchange_wait_ms: 50,
                exchange_io_threads: 1,
                exchange_io_max_inflight_bytes: 1 << 20,
                exchange_max_transmit_batched_bytes: 1 << 20,
                operator_buffer_chunks: 1,
                local_exchange_buffer_mem_limit_per_driver: 1 << 20,
                local_exchange_max_buffered_rows: 4096,
                runtime_filter_scan_wait_time_ms_override: None,
                runtime_filter_wait_timeout_ms_override: None,
                sink_io_worker_threads: 1,
                sink_io_max_blocking_threads: 1,
            },
            Arc::new(functions.seal().unwrap()),
            binding.authority().clone(),
        )
        .unwrap(),
    )
}
pub(super) fn task(binding: &QueryMemoryBinding) -> TaskIdentity {
    TaskIdentity::new(
        binding.execution(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    )
}

// Author the complete original binding request, uses and frozen effects.
// The interpreter selects and prepares the installed owner from these facts.
fn source_for_arguments(
    name: &str,
    types: &[FunctionValueType],
    functions: &PureEngineFunctionCatalog,
) -> Vec<u8> {
    let arguments = types
        .iter()
        .map(|ty| FunctionArgument::Value {
            value_type: ty.clone(),
            constant: None,
        })
        .collect::<Vec<_>>();
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: types.len(),
        expected_result_type: None,
    };
    let bound = functions
        .metadata()
        .resolve_bound_user(name, FunctionKind::Scalar, request, &Control)
        .unwrap();
    let selected = Arc::new(bound.selected.clone());
    let FunctionResultType::Scalar(result_type) = &selected.result_type else {
        panic!("exact scalar integer fixture")
    };
    let function = BoundFunction {
        function_id: bound.function_id.clone(),
        overload: selected.overload.clone(),
        kind: bound.kind,
        argument_types: selected.argument_types.clone(),
        result_type: result_type.clone(),
        legacy_metadata: Some(LegacyBindingMetadata {
            volatility: bound.semantics.volatility,
            argument_evaluation: bound.semantics.argument_evaluation,
            failure_behavior: bound.semantics.failure_behavior,
            intrinsic_row_error: bound.semantics.intrinsic_row_error,
            semantic_parameters: Box::default(),
        }),
    };
    let values = NodeId::new(1);
    let project = NodeId::new(2);
    let mut builder = FragmentBuilder::new(FragmentId::new(303));
    let mut inputs = vec![];
    let mut children = vec![];
    for (ordinal, ty) in types.iter().enumerate() {
        let input = builder
            .add_value(
                ty.clone(),
                ValueOrigin::NodeOutput {
                    node: values,
                    output_ordinal: u32::try_from(ordinal).unwrap(),
                },
            )
            .unwrap();
        inputs.push(input);
        children.push(
            builder
                .add_expression(project, ty.clone(), ExprKind::Value(input))
                .unwrap(),
        );
    }
    builder
        .add_values(values, Box::default(), inputs.into_boxed_slice())
        .unwrap();
    let expr = builder
        .add_expression(
            project,
            result_type.clone(),
            ExprKind::FunctionCall {
                function,
                args: children.clone().into_boxed_slice(),
            },
        )
        .unwrap();
    let output = builder
        .add_value(
            result_type.clone(),
            ValueOrigin::Expr {
                node: project,
                expr,
            },
        )
        .unwrap();
    builder
        .add_project(
            project,
            values,
            Box::from([(expr, output)]),
            Box::from([output]),
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            project,
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
            vec![(
                PhysicalCallDefinition::Expression(expr),
                PhysicalCallRequest {
                    arguments: types
                        .iter()
                        .map(|ty| StaticFunctionArgument::Value {
                            value_type: ty.clone(),
                            constant: None,
                        })
                        .collect(),
                    logical_argument_count: types.len(),
                    expected_result_type: None,
                    constant_policy: constants(),
                },
            )],
            &Control,
        )
        .unwrap();
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control).unwrap();
    assert_eq!(roots.sites().len(), 1);
    let (site, root) = roots.sites().iter().next().unwrap();
    let context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(0),
        domain: EvaluationDomainId::new(0),
        demand: root.demand,
    };
    let child_ids = (1..=types.len())
        .map(|ordinal| ExpressionUseId::new(u32::try_from(ordinal).unwrap()))
        .collect::<Vec<_>>();
    let mut invocations = vec![];
    for (&definition, &use_id) in children.iter().zip(&child_ids) {
        invocations.push(ExpressionInvocation {
            context: ExpressionEffectContext { use_id, ..context },
            definition,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
    }
    invocations.push(ExpressionInvocation {
        context,
        definition: expr,
        control: ControlShape::Eager,
        arguments: child_ids.clone().into_boxed_slice(),
    });
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: context.domain,
            parent: None,
            guard: None,
        }],
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let uses = PhysicalRootUses::try_new(&fragment, flow, vec![(*site, context.use_id)], &Control)
        .unwrap();
    let parameters = SemanticParameters::try_new([]).unwrap();
    let argument_uses = child_ids.iter().copied().map(Some).collect::<Vec<_>>();
    let token = functions
        .prepare_fresh(
            CallEffectInput {
                context,
                argument_uses: CallArgumentUses::SelectedChannels(&argument_uses),
                function_id: &bound.function_id,
                kind: FunctionKind::Scalar,
                selected: &selected,
                request,
                environment: &[],
                parameters: &parameters,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                proof_scope: CallProofScope::Domain(context.domain),
            },
            selected.clone(),
            PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(context),
            },
            &Control,
        )
        .unwrap();
    let calls = FrozenFragmentCalls::try_new(
        &fragment,
        &uses,
        vec![FrozenPhysicalCall {
            site: PhysicalCallSite::Expression(context.use_id),
            context,
            effects: token.call_contract().effects().clone(),
            materialized_writer_input: None,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            regexp_count_pattern_source: None,
            to_base64_byte_source: None,
            temporal_source: None,
        }],
        &Control,
    )
    .unwrap();
    let id = fragment.id();
    let result = ResultPort {
        scalar_schema: None,
        fragment: id,
        output: fragment.nodes()[&project].output.clone(),
        fields: Box::from([ResultField {
            domain: match result_type.logical_type() {
                ValueLogicalType::Physical => ResultValueDomain::Plain,
                ValueLogicalType::Json => ResultValueDomain::Json,
                _ => panic!("fixture requires its authored integer or JSON result domain"),
            },
            name: "r".into(),
            alias: None,
            value: output,
            ty: result_type.clone(),
        }]),
    };
    let package = FragmentPackage::try_new(
        FragmentPackageInput {
            version: PlanVersionId::try_new([63; 16]).unwrap(),
            required: RequiredContracts::default(),
            fragment,
            constants: ConstantPools::empty(),
            expression_uses: uses,
            calls,
            pruning: FrozenFragmentPruning::try_new(id, vec![], &Control).unwrap(),
            cuts: FragmentCuts::default(),
            result: Some(result),
            parameters,
            scans: BTreeMap::new(),
            writes: BTreeMap::new(),
            annotations: Box::default(),
        },
        FragmentPackageAdmission {
            plan_limits: PlanLimits::FROZEN,
            source_retained_bytes: 64 << 20,
            property_projection_limits: PropertyProofProjectionLimits {
                max_request_bytes: 16 << 20,
                max_coexisting_bytes: 256 << 20,
                max_projection_work: 16 << 20,
            },
        },
        &Control,
    )
    .unwrap();
    encode_fragment_package(&package, &encode_limits(), &Control)
        .unwrap()
        .encode_to_vec()
}
pub(super) fn program_for_arguments(
    name: &str,
    types: &[FunctionValueType],
    names: &[&str],
    binding: &QueryMemoryBinding,
) -> Arc<LocalProgram> {
    let functions = functions_for_names(names);
    let bytes = source_for_arguments(name, types, &functions);
    let providers = novarocks_connector_contract::PureProviderProgramCatalog::<
        std::convert::Infallible,
    >::try_new(&[], vec![], &Control)
    .unwrap();
    let interpreter = CompiledPackageInterpreter::new(
        FragmentDecodeResourceModel::try_new(&Control).unwrap(),
        decode_limits(),
        Arc::new(functions),
        Arc::new(providers),
        constants(),
    );
    let owner = NativeNodePreparation::new(preparation_identity(binding.execution()));
    let loan = owner.loan();
    let mut types = TypeMaterializationJournal::default();
    let mut projects = ProjectMetadataJournal::default();
    let mut type_host = TypeMaterializationHost::new(binding, &loan, &mut types);
    let mut project_host = ProjectMetadataHost::new(binding, &loan, &mut projects);
    let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
        "shift funding type construction",
    );
    tracker.install_limit_once(64 << 20).unwrap();
    let compiled = novarocks_native_adapter::backend_task_execution::compile_native_package_with_funding_for_test(
            &interpreter,
            &bytes,
            CompiledTaskOptions {
                pipeline_dop: NonZeroUsize::MIN,
                exchange_wait: Duration::from_millis(50),
            },
            Arc::clone(&tracker),
            &Control,
            &mut type_host,
            &mut project_host,
        )
        .unwrap();
    let root = compiled.program().graph().root();
    let PreparedProjectMetadataForTest {
        program,
        factory,
        schema,
    } = prepare_project_metadata_for_test(
        compiled,
        CompiledSchemaSite::Project(root),
        binding,
        Arc::clone(&tracker),
        &loan,
        &Control,
        &mut projects,
    )
    .unwrap();
    drop(factory);
    drop(schema);
    program
}
pub(super) fn site(program: &LocalProgram) -> ProgramExpressionRootSite {
    ProgramExpressionRootSite::Node {
        node: program.graph().root(),
        role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
    }
}
pub(super) fn batch_for_arguments(program: &LocalProgram, columns: Vec<ArrayRef>) -> RecordBatch {
    let ProgramNodeKind::Project { input, .. } =
        program.graph().nodes()[program.graph().root().index()].kind()
    else {
        panic!("original computing Project")
    };
    RecordBatch::try_new(
        program.graph().nodes()[input.index()]
            .output_layout()
            .schema()
            .clone(),
        columns,
    )
    .unwrap()
}
fn preparation_identity(
    execution: novarocks_types::identity::QueryExecutionId,
) -> novarocks_execution_contract::TaskIdentity {
    use novarocks_types::identity::{BackendProcessId, StageId, TaskId};
    novarocks_execution_contract::TaskIdentity::new(
        execution,
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    )
}
