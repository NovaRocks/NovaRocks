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

//! Shared lawful Native package and actual host fixture for integer GLOBAL tests.
//! This authors inputs for the original compiler; it is not a decoder or kernel.
use arrow::{
    array::{ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array},
    buffer::Buffer,
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_execution::{
    exec::expr::agg::ExecutionFunctionSetBuilder,
    runtime::{
        execution_runtime::{ExecutionRuntime, ExecutionRuntimeConfig},
        preparation_metadata::CompiledSchemaSite,
        query_memory::QueryMemoryBinding,
        scalar_memory::test_support::RuntimeScalarWrapperReceiptForTest,
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
use novarocks_memory::{
    AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority, TeardownEvidence, TopUpPolicy,
    lane::{RecordRef, global_store},
};
use novarocks_native_adapter::backend_task_execution::{
    CompiledPackageCompiler, CompiledPackageInterpreter, CompiledTaskOptions,
    PreparedProjectMetadataForTest, ProjectMetadataHost, ProjectMetadataJournal,
    TypeMaterializationHost, TypeMaterializationJournal, prepare_project_metadata_for_test,
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
use novarocks_types::{
    QueryId,
    identity::{AttemptId, BackendProcessId, QueryExecutionId, StageId, TaskId},
};
use novarocks_worker::TestPreparationControl;
use prost::Message;
use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc, time::Duration};

pub(super) const NAMES: [&str; 3] = [
    "bit_shift_left",
    "bit_shift_right",
    "bit_shift_right_logical",
];
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
pub(super) fn binding() -> QueryMemoryBinding {
    const BYTES: u64 = 64 << 20;
    let mut cfg = AuthorityConfig::new(BYTES, BYTES * 3 / 4, BYTES / 4);
    cfg.max_accounts = 8;
    cfg.max_active_owners = 16;
    cfg.metadata_budget_bytes = 1 << 20;
    cfg.top_up = TopUpPolicy::uniform(1);
    let authority = Arc::new(MemoryAuthority::new(cfg).unwrap());
    let account = authority
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    QueryMemoryBinding::try_new(
        QueryExecutionId::new(QueryId::new(37, 303), AttemptId::new(1).unwrap()).unwrap(),
        authority,
        account,
    )
    .unwrap()
}
pub(super) fn runtime(binding: &QueryMemoryBinding) -> Arc<ExecutionRuntime> {
    runtime_for_names(binding, &NAMES)
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
            domain: match result_type.logical_type {
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
pub(super) fn program_typed(
    name: &str,
    source_type: &DataType,
    binding: &QueryMemoryBinding,
) -> Arc<LocalProgram> {
    program_for_arguments(
        name,
        &[
            FunctionValueType::new(source_type.clone(), true),
            FunctionValueType::new(DataType::Int64, true),
        ],
        &NAMES,
        binding,
    )
}
pub(super) fn program_for_arguments(
    name: &str,
    types: &[FunctionValueType],
    names: &[&str],
    binding: &QueryMemoryBinding,
) -> Arc<LocalProgram> {
    let functions = functions_for_names(names);
    let bytes = source_for_arguments(name, types, &functions);
    let providers =
        novarocks_connector_contract::PureProviderProgramCatalog::<std::io::Error>::try_new(
            &[],
            vec![],
            &Control,
        )
        .unwrap();
    let interpreter = CompiledPackageInterpreter::new(
        FragmentDecodeResourceModel::try_new(&Control).unwrap(),
        decode_limits(),
        Arc::new(functions),
        Arc::new(providers),
        constants(),
    );
    let owner = TestPreparationControl::new(Duration::from_millis(1));
    let loan = owner.loan();
    let mut types = TypeMaterializationJournal::default();
    let mut projects = ProjectMetadataJournal::default();
    let mut type_host = TypeMaterializationHost::new(binding, &loan, &mut types);
    let mut project_host = ProjectMetadataHost::new(binding, &loan, &mut projects);
    let compiled = interpreter
        .compile(
            &bytes,
            CompiledTaskOptions {
                pipeline_dop: NonZeroUsize::MIN,
                exchange_wait: Duration::from_millis(50),
            },
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
pub(super) fn batch(program: &LocalProgram, nullable: bool, rows: usize) -> RecordBatch {
    let ProgramNodeKind::Project { input, .. } =
        program.graph().nodes()[program.graph().root().index()].kind()
    else {
        panic!("original computing Project")
    };
    let source_values = (0..rows)
        .map(|row| {
            if nullable && row % 3 == 0 {
                None
            } else {
                Some(-7)
            }
        })
        .collect::<Vec<_>>();
    let values: ArrayRef = match program.graph().nodes()[input.index()]
        .output_layout()
        .schema()
        .field(0)
        .data_type()
    {
        DataType::Int8 => Arc::new(Int8Array::from(
            source_values
                .into_iter()
                .map(|v| v.map(|v| i8::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Int16 => Arc::new(Int16Array::from(
            source_values
                .into_iter()
                .map(|v| v.map(|v| i16::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Int32 => Arc::new(Int32Array::from(
            source_values
                .into_iter()
                .map(|v| v.map(|v| i32::try_from(v).unwrap()))
                .collect::<Vec<_>>(),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(source_values)),
        _ => panic!("exact shift fixture physical source"),
    };
    batch_for_arguments(
        program,
        vec![
            values,
            Arc::new(Int64Array::from(vec![1; rows])) as ArrayRef,
        ],
    )
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
// This unsafe reader is restricted to the original standard producer in this
// target. Capacity alone would not prove GLOBAL or exclude custom backing.
pub(super) unsafe fn original_shift_tag(buffer: &Buffer) -> RecordRef {
    // SAFETY: the caller supplies the same standard producer/GLOBAL premises.
    unsafe { original_primitive_tag(buffer) }
}
// Same restricted tail contract, also used for the original bitwise producer.
pub(super) unsafe fn original_primitive_tag(buffer: &Buffer) -> RecordRef {
    assert!(buffer.capacity() >= 512);
    // SAFETY: caller supplies live original primitive shift output or original
    // Vec input from this GLOBAL-only target. The standard producer requests
    // capacity bytes; GLOBAL initializes its 8-byte tail at the SAME base.
    unsafe { RecordRef::read(buffer.data_ptr().as_ptr().add(buffer.capacity())) }
}
pub(super) fn assert_origin(reference: RecordRef, binding: &QueryMemoryBinding) {
    let snapshot = global_store().snapshot_ref(reference).unwrap();
    assert_eq!(snapshot.origin, binding.account().id().get());
    assert!(snapshot.outstanding > 0 && snapshot.tagged_bytes > 0);
}
pub(super) fn assert_settled(receipt: &RuntimeScalarWrapperReceiptForTest) {
    use novarocks_memory::attribution::scope::{AmbientEntryObservation, AmbientExitObservation};
    assert!(receipt.workset_bytes.unwrap() > 0);
    assert_eq!(
        receipt.body.observation.entry(),
        AmbientEntryObservation::Bound
    );
    assert_eq!(
        receipt.body.observation.exit(),
        AmbientExitObservation::Restored
    );
    let settlement = receipt.body.settlement.as_ref().unwrap();
    assert!(settlement.accepted_live > 0);
    assert_eq!(settlement.debt, 0);
    assert_eq!(settlement.next_step, Ok(()));
    assert_eq!(receipt.body.stopped, Some(Ok(())));
}
pub(super) fn last_free(
    binding: &QueryMemoryBinding,
    aliases: Vec<Buffer>,
    references: Vec<RecordRef>,
) {
    let before = binding.authority().root().committed_bytes();
    let transfer = binding
        .account()
        .retire(&TeardownEvidence {
            tasks_exited: true,
            operators_destroyed: true,
            io: &[],
            now_ns: 1,
        })
        .unwrap();
    let retained = before.checked_sub(transfer.returned_idle).unwrap();
    assert!(retained > 0);
    assert_eq!(binding.authority().root().committed_bytes(), retained);
    for &reference in &references {
        assert_origin(reference, binding);
    }
    assert_eq!(aliases.len(), references.len());
    for (alias, reference) in aliases.into_iter().zip(&references) {
        let before_free = global_store().snapshot_ref(*reference).unwrap();
        let requested = i64::try_from(alias.capacity()).unwrap() + 8;
        std::thread::spawn(move || drop(alias)).join().unwrap();
        let after_free = global_store().snapshot_ref(*reference).unwrap();
        assert_eq!(after_free.outstanding, before_free.outstanding - 1);
        assert_eq!(
            after_free.tagged_bytes,
            before_free.tagged_bytes - requested
        );
        eprintln!(
            "Physical free record={reference:?} request={requested} outstanding={}->{} tagged_bytes={}->{}",
            before_free.outstanding,
            after_free.outstanding,
            before_free.tagged_bytes,
            after_free.tagged_bytes
        );
    }
    for reference in references {
        let freed = global_store().snapshot_ref(reference).unwrap();
        assert_eq!((freed.outstanding, freed.tagged_bytes), (0, 0));
        eprintln!(
            "Last physical free record={reference:?} outstanding={} tagged_bytes={}",
            freed.outstanding, freed.tagged_bytes
        );
    }
    binding
        .authority()
        .request_maintenance(novarocks_memory::MaintenanceReason::ExplicitLocalReclaim);
    binding
        .authority()
        .maintain(binding.authority().maintenance_scan_bound().unwrap());
    assert!(binding.authority().root().committed_bytes() < retained);
}
