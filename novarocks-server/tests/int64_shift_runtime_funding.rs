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

//! Shipping GLOBAL: original Frame values and complete-wrapper nullable
//! buffers are separate witnesses. Explicit retirement is test protocol only.
use arrow::{
    array::{Array, ArrayRef, Int64Array},
    buffer::Buffer,
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_execution::{
    exec::expr::agg::ExecutionFunctionSetBuilder,
    runtime::{
        execution_runtime::{ExecutionRuntime, ExecutionRuntimeConfig},
        preparation_metadata::ProjectSchemaSite,
        query_memory::QueryMemoryBinding,
        scalar_memory::{
            RuntimeScalarEvaluationFailure, RuntimeScalarMemoryRefusal,
            test_support::{
                RuntimeScalarFrameForTest, RuntimeScalarWrapperForTest,
                RuntimeScalarWrapperReceiptForTest,
            },
        },
    },
};
use novarocks_execution_contract::TaskIdentity;
use novarocks_functions::{
    CallArgumentUses, CallEffectInput, ConstantPolicy, EngineFunctionCatalogBuilder,
    EvaluatedArgument, FunctionArgument, FunctionBindingRequest, FunctionKind, FunctionResultType,
    InstalledPureKernel, PureCallPreparation, PureEngineFunctionCatalog, ScopedExpressionEffects,
    Selection,
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

const NAMES: [&str; 3] = [
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
fn functions() -> PureEngineFunctionCatalog {
    let actual = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut installed = vec![];
    for name in NAMES {
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
fn binding() -> QueryMemoryBinding {
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
fn runtime(binding: &QueryMemoryBinding) -> Arc<ExecutionRuntime> {
    let actual = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
    let mut functions = ExecutionFunctionSetBuilder::new();
    for name in NAMES {
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
fn task(binding: &QueryMemoryBinding) -> TaskIdentity {
    TaskIdentity::new(
        binding.execution(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    )
}

// Author the complete original binding request, uses and frozen effects.
// The interpreter selects and prepares the installed owner from these facts.
fn source(name: &str, functions: &PureEngineFunctionCatalog) -> Vec<u8> {
    let ty = FunctionValueType::new(DataType::Int64, true);
    let arguments = vec![
        FunctionArgument::Value {
            value_type: ty.clone(),
            constant: None
        };
        2
    ];
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: 2,
        expected_result_type: None,
    };
    let bound = functions
        .metadata()
        .resolve_bound_user(name, FunctionKind::Scalar, request, &Control)
        .unwrap();
    let selected = Arc::new(bound.selected.clone());
    let FunctionResultType::Scalar(result_type) = &selected.result_type else {
        panic!("scalar shift")
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
    for ordinal in 0..2 {
        let input = builder
            .add_value(
                ty.clone(),
                ValueOrigin::NodeOutput {
                    node: values,
                    output_ordinal: ordinal,
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
                    arguments: vec![
                        StaticFunctionArgument::Value {
                            value_type: ty,
                            constant: None
                        };
                        2
                    ]
                    .into_boxed_slice(),
                    logical_argument_count: 2,
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
    let child_ids = [ExpressionUseId::new(1), ExpressionUseId::new(2)];
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
        arguments: Box::from(child_ids),
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
    let argument_uses = child_ids.map(Some);
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
            domain: ResultValueDomain::Plain,
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
fn program(name: &str, binding: &QueryMemoryBinding) -> Arc<LocalProgram> {
    let functions = functions();
    let bytes = source(name, &functions);
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
        ProjectSchemaSite::Project(root),
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
fn site(program: &LocalProgram) -> ProgramExpressionRootSite {
    ProgramExpressionRootSite::Node {
        node: program.graph().root(),
        role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
    }
}
fn batch(program: &LocalProgram, nullable: bool, rows: usize) -> RecordBatch {
    let ProgramNodeKind::Project { input, .. } =
        program.graph().nodes()[program.graph().root().index()].kind()
    else {
        panic!("original computing Project")
    };
    let values = Int64Array::from(
        (0..rows)
            .map(|row| {
                if nullable && row % 3 == 0 {
                    None
                } else {
                    Some(-7)
                }
            })
            .collect::<Vec<_>>(),
    );
    RecordBatch::try_new(
        program.graph().nodes()[input.index()]
            .output_layout()
            .schema()
            .clone(),
        vec![
            Arc::new(values) as ArrayRef,
            Arc::new(Int64Array::from(vec![1; rows])) as ArrayRef,
        ],
    )
    .unwrap()
}
// This unsafe reader is restricted to the original standard producer in this
// target. Capacity alone would not prove GLOBAL or exclude custom backing.
unsafe fn original_shift_tag(buffer: &Buffer) -> RecordRef {
    assert!(buffer.capacity() >= 512);
    // SAFETY: caller supplies live original Int64 shift output or original
    // Vec input from this GLOBAL-only target. The standard producer requests
    // capacity bytes; GLOBAL initializes its 8-byte tail at the SAME base.
    unsafe { RecordRef::read(buffer.data_ptr().as_ptr().add(buffer.capacity())) }
}
fn assert_origin(reference: RecordRef, binding: &QueryMemoryBinding) {
    let snapshot = global_store().snapshot_ref(reference).unwrap();
    assert_eq!(snapshot.origin, binding.account().id().get());
    assert!(snapshot.outstanding > 0 && snapshot.tagged_bytes > 0);
}
fn assert_settled(receipt: &RuntimeScalarWrapperReceiptForTest) {
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
fn last_free(binding: &QueryMemoryBinding, aliases: Vec<Buffer>, references: Vec<RecordRef>) {
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
    std::thread::spawn(move || drop(aliases)).join().unwrap();
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
#[test]
fn original_production_frame_values_keep_account_through_alias_last_free() {
    let _ = novarocks_server::memory_observation::snapshot();
    for name in NAMES {
        let binding = binding();
        let runtime = runtime(&binding);
        let program = program(name, &binding);
        let input = batch(&program, false, 4096);
        let mut frame = RuntimeScalarFrameForTest::try_new(
            program.clone(),
            site(&program),
            runtime.clone(),
            task(&binding),
            Some(binding.clone()),
        )
        .unwrap();
        let output = frame.evaluate(&input).unwrap();
        let output_array = output.as_any().downcast_ref::<Int64Array>().unwrap();
        let expected = match name {
            "bit_shift_left" => -14,
            "bit_shift_right" => -4,
            "bit_shift_right_logical" => ((-7i64) as u64 >> 1) as i64,
            _ => unreachable!(),
        };
        assert_eq!(
            output_array.iter().collect::<Vec<_>>(),
            vec![Some(expected); 4096]
        );
        let backing = output_array.values().inner().clone();
        // SAFETY: the actual Frame returned the standard original shift value
        // allocation, which remains alive via output and this Buffer alias.
        let reference = unsafe { original_shift_tag(&backing) };
        assert_origin(reference, &binding);
        eprintln!(
            "Frame source={name} rows=4096 values_capacity={} record={reference:?} account={}",
            backing.capacity(),
            binding.account().id().get()
        );
        for argument in input.columns() {
            let source = argument
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .inner();
            assert_ne!(source.data_ptr(), backing.data_ptr());
            // SAFETY: this fixture's Int64Array came from its actual standard
            // Vec producer under the same shipping GLOBAL, outside any grant.
            let original = unsafe { original_shift_tag(source) };
            assert_ne!(
                global_store().snapshot_ref(original).unwrap().origin,
                binding.account().id().get()
            );
        }
        let slice = output.slice(10, 3000);
        let sliced_backing = slice
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .inner()
            .clone();
        assert_eq!(backing.data_ptr(), sliced_backing.data_ptr());
        assert_eq!(backing.capacity(), sliced_backing.capacity());
        drop(frame);
        drop(program);
        drop(input);
        drop(output);
        drop(slice);
        assert_origin(reference, &binding);
        drop(sliced_backing);
        assert_origin(reference, &binding);
        runtime.shutdown_driver_execution().unwrap();
        drop(runtime);
        last_free(&binding, vec![backing], vec![reference]);
    }
}
#[test]
fn original_complete_wrapper_nullable_buffers_keep_account_through_alias_last_free() {
    let _ = novarocks_server::memory_observation::snapshot();
    for name in NAMES {
        let binding = binding();
        let runtime = runtime(&binding);
        let program = program(name, &binding);
        let input = batch(&program, true, 4096);
        let mut wrapper = RuntimeScalarWrapperForTest::try_new(
            &program,
            site(&program),
            runtime.clone(),
            task(&binding),
            Some(binding.clone()),
        )
        .unwrap();
        let arguments = [
            EvaluatedArgument::Column(&input.columns()[0]),
            EvaluatedArgument::Column(&input.columns()[1]),
        ];
        let (output, receipt) = wrapper.evaluate(Selection::all(4096), &arguments);
        assert_settled(&receipt);
        let (_, values, errors) = output.unwrap().into_parts();
        assert!(errors.is_empty());
        let array = values.as_any().downcast_ref::<Int64Array>().unwrap();
        let expected = match name {
            "bit_shift_left" => -14,
            "bit_shift_right" => -4,
            "bit_shift_right_logical" => ((-7i64) as u64 >> 1) as i64,
            _ => unreachable!(),
        };
        assert_eq!(
            array.iter().collect::<Vec<_>>(),
            (0..4096)
                .map(|row| if row % 3 == 0 { None } else { Some(expected) })
                .collect::<Vec<_>>()
        );
        assert_eq!(array.null_count(), 4096_usize.div_ceil(3));
        let value_alias = array.values().inner().clone();
        let null_alias = array.nulls().unwrap().buffer().clone();
        assert_ne!(value_alias.data_ptr(), null_alias.data_ptr());
        // SAFETY: both live standard allocations were born in the original
        // complete ScalarV1 wrapper under this target's sole shipping GLOBAL.
        let value_tag = unsafe { original_shift_tag(&value_alias) };
        let null_tag = unsafe { original_shift_tag(&null_alias) };
        eprintln!(
            "Wrapper source={name} rows=4096 values_capacity={} bitmap_capacity={} values_record={value_tag:?} bitmap_record={null_tag:?} workset={} settlement={:?}",
            value_alias.capacity(),
            null_alias.capacity(),
            receipt.workset_bytes.unwrap(),
            receipt.body.settlement
        );
        for tag in [value_tag, null_tag] {
            assert_origin(tag, &binding);
        }
        let slice = values.slice(11, 3000);
        let sliced = slice.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(sliced.values().inner().data_ptr(), value_alias.data_ptr());
        assert_eq!(
            sliced.nulls().unwrap().buffer().data_ptr(),
            null_alias.data_ptr()
        );
        let bitmap_slice_alias = sliced.nulls().unwrap().buffer().clone();
        assert_eq!(bitmap_slice_alias.capacity(), null_alias.capacity());
        // SAFETY: the slice keeps the SAME live original wrapper allocation;
        // the tail uses its base and capacity, never its visible bit offset.
        assert_eq!(unsafe { original_shift_tag(&bitmap_slice_alias) }, null_tag);
        drop(wrapper);
        drop(program);
        drop(input);
        drop(values);
        drop(slice);
        assert_origin(null_tag, &binding);
        drop(bitmap_slice_alias);
        for tag in [value_tag, null_tag] {
            assert_origin(tag, &binding);
        }
        runtime.shutdown_driver_execution().unwrap();
        drop(runtime);
        last_free(
            &binding,
            vec![value_alias, null_alias],
            vec![value_tag, null_tag],
        );
    }
}
#[test]
fn original_complete_wrapper_empty_small_and_missing_keep_existing_policy() {
    let _ = novarocks_server::memory_observation::snapshot();
    for rows in [0, 3] {
        let binding = binding();
        let runtime = runtime(&binding);
        let program = program(NAMES[0], &binding);
        let input = batch(&program, false, rows);
        let mut wrapper = RuntimeScalarWrapperForTest::try_new(
            &program,
            site(&program),
            runtime.clone(),
            task(&binding),
            Some(binding.clone()),
        )
        .unwrap();
        let arguments = [
            EvaluatedArgument::Column(&input.columns()[0]),
            EvaluatedArgument::Column(&input.columns()[1]),
        ];
        let (values, receipt) = wrapper.evaluate(Selection::all(rows), &arguments);
        let (_, values, errors) = values.unwrap().into_parts();
        assert!(errors.is_empty());
        assert_eq!(values.len(), rows);
        eprintln!(
            "Wrapper small rows={rows} workset={} settlement={:?}",
            receipt.workset_bytes.unwrap(),
            receipt.body.settlement
        );
        assert!(receipt.workset_bytes.unwrap() >= 512);
        assert_eq!(receipt.body.settlement.as_ref().unwrap().accepted_live, 0);
        assert_eq!(receipt.body.settlement.as_ref().unwrap().debt, 0);
        assert!(
            values
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .inner()
                .capacity()
                < 512
        );
        let mut missing = RuntimeScalarWrapperForTest::try_new(
            &program,
            site(&program),
            runtime.clone(),
            task(&binding),
            None,
        )
        .unwrap();
        let (failure, refused) = missing.evaluate(Selection::all(rows), &arguments);
        assert_eq!(
            failure.unwrap_err(),
            RuntimeScalarEvaluationFailure::Host(RuntimeScalarMemoryRefusal::MissingQueryMemory)
        );
        assert!(refused.body.settlement.is_none());
        drop(wrapper);
        drop(missing);
        drop(values);
        drop(input);
        drop(program);
        runtime.shutdown_driver_execution().unwrap();
    }
}
