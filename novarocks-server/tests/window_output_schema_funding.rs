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

//! Actual original Analytic constructor under Server's sole GLOBAL allocator.
//! Explicit test retirement does not prove production Work teardown.
use arrow::datatypes::{DataType, Field};
use novarocks_execution::{
    exec::expr::agg::ExecutionFunctionSetBuilder,
    runtime::{preparation_metadata::CompiledSchemaSite, query_memory::QueryMemoryBinding},
};
use novarocks_functions::{
    CallArgumentUses, CallEffectInput, ConstantPolicy, EngineFunctionCatalogBuilder,
    FunctionBindingRequest, FunctionKind, FunctionResultType, InstalledPureKernel,
    PureCallPreparation, PureEngineFunctionCatalog, ScopedExpressionEffects, WindowCallOptions,
};
use novarocks_memory::{
    AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority, TeardownEvidence, TopUpPolicy,
    lane::{RecordRef, global_store},
};
use novarocks_native_adapter::backend_task_execution::{
    CompiledPackageCompiler, CompiledPackageInterpreter, CompiledTaskOptions, CompiledTaskProgram,
    PreparedWindowMetadataForTest, ProjectMetadataHost, ProjectMetadataJournal,
    TypeMaterializationHost, TypeMaterializationJournal, prepare_window_metadata_for_test,
    prepare_window_metadata_graph_for_test,
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
    identity::{AttemptId, QueryExecutionId},
};
use novarocks_worker::TestPreparationControl;
use prost::Message;
use std::{
    collections::{BTreeMap, HashMap},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn binding(bytes: u64) -> QueryMemoryBinding {
    let mut cfg = AuthorityConfig::new(bytes, bytes * 3 / 4, bytes / 4);
    cfg.max_accounts = 8;
    cfg.max_active_owners = 16;
    cfg.metadata_budget_bytes = 1 << 20;
    cfg.top_up = TopUpPolicy::uniform(1);
    let authority = Arc::new(MemoryAuthority::new(cfg).unwrap());
    let account = authority
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    QueryMemoryBinding::try_new(
        QueryExecutionId::new(QueryId::new(37, 5), AttemptId::new(1).unwrap()).unwrap(),
        authority,
        account,
    )
    .unwrap()
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
    let definition = actual
        .definition("row_number", FunctionKind::Window)
        .unwrap();
    let declaration = definition.binding_declaration().unwrap();
    let installed = declaration
        .overloads()
        .iter()
        .map(|overload| {
            let record = actual
                .pure_overload_declaration_observed(
                    declaration.function_id(),
                    declaration.kind(),
                    &overload.identity,
                    &Control,
                )
                .unwrap();
            InstalledPureKernel {
                function: declaration.function_id().clone(),
                kind: declaration.kind(),
                implementation: record.implementation().clone(),
                aggregate_state_format: None,
            }
        })
        .collect::<Vec<_>>();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(definition.clone()).unwrap();
    builder.seal_pure(installed).unwrap()
}
fn interpreter() -> CompiledPackageInterpreter<std::io::Error> {
    let providers =
        novarocks_connector_contract::PureProviderProgramCatalog::<std::io::Error>::try_new(
            &[],
            vec![],
            &Control,
        )
        .unwrap();
    CompiledPackageInterpreter::new(
        FragmentDecodeResourceModel::try_new(&Control).unwrap(),
        decode_limits(),
        Arc::new(functions()),
        Arc::new(providers),
        constants(),
    )
}
fn bigint() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, true)
}
fn nested() -> FunctionValueType {
    let child =
        Field::new("child_".repeat(100), DataType::Utf8, true).with_metadata(HashMap::from([
            ("nr_logical_type".to_owned(), "json".to_owned()),
            ("source_note".to_owned(), "original metadata".repeat(32)),
        ]));
    FunctionValueType::new(DataType::Struct(vec![Arc::new(child)].into()), true)
}
// A legal public builtin binding and a complete original Window result port.
// The root labels are authored before encoding, never patched after compilation.
fn source(input_type: FunctionValueType, label_bytes: usize) -> Vec<u8> {
    let catalog = functions();
    let request = FunctionBindingRequest {
        arguments: &[],
        logical_argument_count: 0,
        expected_result_type: None,
    };
    let bound = catalog
        .metadata()
        .resolve_bound_user("row_number", FunctionKind::Window, request, &Control)
        .unwrap();
    let selection = Arc::new(bound.selected.clone());
    let FunctionResultType::Scalar(result_type) = &selection.result_type else {
        panic!("window scalar result")
    };
    let mut function = BoundFunction::from_exact_signature(
        bound.function_id.clone(),
        selection.overload.clone(),
        FunctionKind::Window,
        selection.argument_types.clone(),
        result_type.clone(),
    );
    function.legacy_metadata = Some(LegacyBindingMetadata {
        volatility: bound.semantics.volatility,
        argument_evaluation: bound.semantics.argument_evaluation,
        failure_behavior: bound.semantics.failure_behavior,
        intrinsic_row_error: bound.semantics.intrinsic_row_error,
        semantic_parameters: Box::default(),
    });
    let values = NodeId::new(1);
    let window = NodeId::new(2);
    let mut builder = FragmentBuilder::new(FragmentId::new(374));
    let input = builder
        .add_value(
            input_type.clone(),
            ValueOrigin::NodeOutput {
                node: values,
                output_ordinal: 0,
            },
        )
        .unwrap();
    builder
        .add_values(values, Box::default(), Box::from([input]))
        .unwrap();
    let expression = builder
        .add_expression(
            window,
            result_type.clone(),
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
            result_type.clone(),
            ValueOrigin::Expr {
                node: window,
                expr: expression,
            },
        )
        .unwrap();
    builder
        .add_row_widening(
            window,
            values,
            Box::from([input, output]),
            NodeKind::Window(WindowSpec {
                partition_by: Box::default(),
                order_by: Box::default(),
                expressions: Box::from([WindowExpression { expression, output }]),
            }),
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            window,
            FragmentSink::RootResult(Box::new(
                novarocks_result_contract::RootOutputContract::new(
                    novarocks_result_contract::RootProfileId::V1,
                    novarocks_result_contract::FrozenRootOutput::CountOnly,
                ),
            )),
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap()
        .with_call_requests_observed(
            vec![(
                PhysicalCallDefinition::Expression(expression),
                PhysicalCallRequest {
                    arguments: Box::default(),
                    logical_argument_count: 0,
                    expected_result_type: None,
                    constant_policy: constants(),
                },
            )],
            &Control,
        )
        .unwrap();
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control).unwrap();
    let domain = EvaluationDomainId::new(0);
    let mut invocations = Vec::new();
    let mut bindings = Vec::new();
    let mut frozen = Vec::new();
    let parameters = SemanticParameters::try_new([]).unwrap();
    for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
        let use_id = ExpressionUseId::new(u32::try_from(ordinal).unwrap());
        let context = ExpressionEffectContext {
            use_id,
            domain,
            demand: root.demand,
        };
        invocations.push(ExpressionInvocation {
            context,
            definition: root.expr,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        bindings.push((*site, use_id));
        if root.expr == expression {
            let token = catalog
                .prepare_fresh(
                    CallEffectInput {
                        context,
                        argument_uses: CallArgumentUses::SelectedChannels(&[]),
                        function_id: &bound.function_id,
                        kind: FunctionKind::Window,
                        selected: &selection,
                        request,
                        environment: &[],
                        parameters: &parameters,
                        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                        proof_scope: CallProofScope::Domain(domain),
                    },
                    Arc::clone(&selection),
                    PureCallPreparation::Window {
                        arguments: ScopedExpressionEffects::primitive(
                            context,
                            ExpressionEffects::PURE_VALUE,
                        ),
                        options: WindowCallOptions::try_new(None, false, &Control).unwrap(),
                    },
                    &Control,
                )
                .unwrap();
            frozen.push(FrozenPhysicalCall {
                regexp_count_pattern_source: None,
                to_base64_byte_source: None,
                temporal_source: None,
                site: PhysicalCallSite::Expression(use_id),
                context,
                effects: token.call_contract().effects().clone(),
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            });
        }
    }
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        }],
        invocations,
        fragment.expressions(),
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    let uses = PhysicalRootUses::try_new(&fragment, flow, bindings, &Control).unwrap();
    let calls = FrozenFragmentCalls::try_new(&fragment, &uses, frozen, &Control).unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output: fragment.nodes()[&window].output.clone(),
        fields: Box::from([
            ResultField {
                domain: ResultValueDomain::Plain,
                name: "input".into(),
                alias: None,
                value: input,
                ty: input_type,
            },
            ResultField {
                domain: ResultValueDomain::Plain,
                name: "r".repeat(label_bytes).into_boxed_str(),
                alias: None,
                value: output,
                ty: result_type.clone(),
            },
        ]),
    };
    let id = fragment.id();
    let package = FragmentPackage::try_new(
        FragmentPackageInput {
            version: PlanVersionId::try_new([74; 16]).unwrap(),
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
fn compile(
    bytes: &[u8],
    binding: &QueryMemoryBinding,
    preparation: &novarocks_worker::PreparationControlLoan<'_>,
    journal: &mut ProjectMetadataJournal,
) -> CompiledTaskProgram {
    let mut types = TypeMaterializationJournal::default();
    let mut type_host = TypeMaterializationHost::new(binding, preparation, &mut types);
    let mut project_host = ProjectMetadataHost::new(binding, preparation, journal);
    interpreter()
        .compile(
            bytes,
            CompiledTaskOptions {
                pipeline_dop: NonZeroUsize::MIN,
                exchange_wait: Duration::from_millis(50),
            },
            &Control,
            &mut type_host,
            &mut project_host,
        )
        .unwrap()
}
fn token(value: &String) -> RecordRef {
    assert!(value.capacity() >= 512);
    // SAFETY: this isolated target links Server's sole GLOBAL wrapper. The
    // initialized tag immediately follows this live original String backing.
    unsafe { RecordRef::read(value.as_ptr().add(value.capacity())) }
}
fn settled(journal: &ProjectMetadataJournal, has_tagged_label: bool) {
    use novarocks_memory::attribution::scope::{AmbientEntryObservation, AmbientExitObservation};
    assert_eq!(
        journal.body.observation.entry(),
        AmbientEntryObservation::Bound
    );
    assert_eq!(
        journal.body.observation.exit(),
        AmbientExitObservation::Restored
    );
    let receipt = journal.body.settlement.as_ref().unwrap();
    if has_tagged_label {
        assert!(receipt.accepted_live > 0);
    }
    assert_eq!(receipt.debt, 0);
    assert_eq!(receipt.next_step, Ok(()));
    assert_eq!(journal.body.stopped, Some(Ok(())));
    let facts = journal.facts.unwrap();
    assert_eq!(
        journal.workset_bytes,
        Some(
            facts.allocation_request_bytes_upper_bound + 8 * facts.allocation_requests_upper_bound
        )
    );
}

#[test]
fn analytic_original_schema_independent_aliases_keep_query_origin_to_last_free() {
    let _ = novarocks_server::memory_observation::snapshot();
    for (input_type, schema_first) in [
        (bigint(), true),
        (nested(), true),
        (bigint(), false),
        (nested(), false),
    ] {
        let binding = binding(64 << 20);
        let bytes = source(input_type, 512);
        let owner = TestPreparationControl::new(Duration::from_millis(1));
        let loan = owner.loan();
        let mut compile_journal = ProjectMetadataJournal::default();
        let compiled = compile(&bytes, &binding, &loan, &mut compile_journal);
        let node = compiled.program().graph().root();
        assert!(matches!(
            compiled.program().graph().nodes()[node.index()].kind(),
            novarocks_local_program::ProgramNodeKind::Analytic { .. }
        ));
        let earlier = Arc::clone(
            &compiled.program().graph().nodes()[node.index()]
                .output_layout()
                .schema()
                .fields()[1],
        );
        let PreparedWindowMetadataForTest {
            program,
            factory,
            occurrence,
        } = prepare_window_metadata_for_test(compiled, node, &binding, &loan, &Control).unwrap();
        settled(&occurrence.journal, true);
        assert_eq!(occurrence.site, CompiledSchemaSite::AnalyticOutput(node));
        let arrow = occurrence.schema.arrow_schema_ref();
        let root_field = Arc::clone(occurrence.schema.slots()[1].field_ref());
        let arrow_field = &arrow.fields()[1];
        assert_eq!(root_field.as_ref(), earlier.as_ref());
        assert_eq!(arrow_field.as_ref(), earlier.as_ref());
        assert_ne!(root_field.name().as_ptr(), earlier.name().as_ptr());
        assert_ne!(arrow_field.name().as_ptr(), earlier.name().as_ptr());
        assert_ne!(root_field.name().as_ptr(), arrow_field.name().as_ptr());
        let first = token(root_field.name());
        let second = token(arrow_field.name());
        eprintln!(
            "WINDOW_GLOBAL account={} schema_first={} slot_base={:p} slot_capacity={} arrow_base={:p} arrow_capacity={} slot_token={:?} arrow_token={:?} facts={:?} workset={:?} settlement={:?}",
            binding.account().id().get(),
            schema_first,
            root_field.name().as_ptr(),
            root_field.name().capacity(),
            arrow_field.name().as_ptr(),
            arrow_field.name().capacity(),
            first,
            second,
            occurrence.journal.facts,
            occurrence.journal.workset_bytes,
            occurrence.journal.body.settlement
        );
        for reference in [first, second] {
            let snapshot = global_store().snapshot_ref(reference).unwrap();
            eprintln!("WINDOW_GLOBAL live token={reference:?} snapshot={snapshot:?}");
            assert_eq!(snapshot.origin, binding.account().id().get());
            assert!(snapshot.outstanding > 0 && snapshot.tagged_bytes > 0);
        }
        if let (DataType::Struct(original), DataType::Struct(copied)) = (
            program.graph().nodes()[node.index()]
                .output_layout()
                .schema()
                .fields()[0]
                .data_type(),
            occurrence.schema.slots()[0].field_ref().data_type(),
        ) {
            assert!(
                Arc::ptr_eq(&original[0], &copied[0]),
                "shared descendants are not reconstructed or retagged"
            );
        }
        drop(program);
        drop(factory);
        drop(occurrence);
        drop(earlier);
        drop(bytes);
        let committed = binding.authority().root().committed_bytes();
        let transfer = binding
            .account()
            .retire(&TeardownEvidence {
                tasks_exited: true,
                operators_destroyed: true,
                io: &[],
                now_ns: 1,
            })
            .unwrap();
        let retained = committed.checked_sub(transfer.returned_idle).unwrap();
        assert!(retained > 0);
        assert_eq!(binding.authority().root().committed_bytes(), retained);
        if schema_first {
            std::thread::spawn(move || drop(arrow)).join().unwrap();
            assert!(
                global_store().snapshot_ref(first).unwrap().outstanding > 0,
                "the independently retained root Field outlives SchemaRef"
            );
            assert_eq!(root_field.name(), &"r".repeat(512));
            eprintln!(
                "WINDOW_GLOBAL surviving_field token={first:?} snapshot={:?}",
                global_store().snapshot_ref(first).unwrap()
            );
            std::thread::spawn(move || drop(root_field)).join().unwrap();
        } else {
            std::thread::spawn(move || drop(root_field)).join().unwrap();
            assert!(
                global_store().snapshot_ref(second).unwrap().outstanding > 0,
                "the independently retained SchemaRef outlives the root FieldRef"
            );
            assert_eq!(arrow.fields()[1].name(), &"r".repeat(512));
            eprintln!(
                "WINDOW_GLOBAL surviving_schema token={second:?} snapshot={:?}",
                global_store().snapshot_ref(second).unwrap()
            );
            std::thread::spawn(move || drop(arrow)).join().unwrap();
        }
        for reference in [first, second] {
            let freed = global_store().snapshot_ref(reference).unwrap();
            eprintln!("WINDOW_GLOBAL last_free token={reference:?} snapshot={freed:?}");
            assert_eq!((freed.outstanding, freed.tagged_bytes), (0, 0));
        }
        binding
            .authority()
            .request_maintenance(novarocks_memory::MaintenanceReason::ExplicitLocalReclaim);
        binding
            .authority()
            .maintain(binding.authority().maintenance_scan_bound().unwrap());
        assert!(binding.authority().root().committed_bytes() < retained);
    }
}
#[test]
fn analytic_actual_private_host_and_original_builder_capture_separate_occurrences() {
    let _ = novarocks_server::memory_observation::snapshot();
    for label_bytes in [1, 512] {
        let binding = binding(64 << 20);
        let bytes = source(bigint(), label_bytes);
        let owner = TestPreparationControl::new(Duration::from_millis(1));
        let loan = owner.loan();
        let mut journal = ProjectMetadataJournal::default();
        let compiled = compile(&bytes, &binding, &loan, &mut journal);
        let node = compiled.program().graph().root();
        let mut functions = ExecutionFunctionSetBuilder::new();
        functions
            .catalog_builder_mut()
            .register(
                novarocks_functions::builtin::catalogue::builtin_engine_function_catalog()
                    .definition("row_number", FunctionKind::Window)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
        let process_before = novarocks_server::memory_observation::attribution_snapshot().process;
        let output = prepare_window_metadata_graph_for_test(
            compiled,
            Arc::new(functions.seal().unwrap()),
            1,
            Some(1),
            &binding,
            &loan,
            &Control,
        )
        .unwrap();
        let process_after = novarocks_server::memory_observation::attribution_snapshot().process;
        if label_bytes < 512 {
            assert!(
                output.occurrences.iter().all(|o| o.schema.slots()[1]
                    .field_ref()
                    .name()
                    .capacity()
                    < 512)
            );
            assert!(
                process_after.small.allocated_total_bytes
                    > process_before.small.allocated_total_bytes
            );
            eprintln!(
                "WINDOW_GRAPH process_small_inclusive before={:?} after={:?}; this includes original graph/test scaffolding, not per-query small lifetime",
                process_before.small, process_after.small
            );
        }
        let sites = output
            .occurrences
            .iter()
            .map(|o| o.site)
            .collect::<Vec<_>>();
        assert_eq!(
            sites,
            vec![
                CompiledSchemaSite::AnalyticOutput(node),
                CompiledSchemaSite::FinalResult
            ]
        );
        for occurrence in &output.occurrences {
            eprintln!(
                "WINDOW_GRAPH account={} label_bytes={} site={:?} facts={:?} workset={:?} settlement={:?}",
                binding.account().id().get(),
                label_bytes,
                occurrence.site,
                occurrence.journal.facts,
                occurrence.journal.workset_bytes,
                occurrence.journal.body.settlement
            );
            settled(&occurrence.journal, label_bytes >= 512);
            assert_eq!(
                occurrence.schema.arrow_schema_ref().fields()[1].name(),
                &"r".repeat(label_bytes)
            );
        }
        assert_ne!(
            Arc::as_ptr(&output.occurrences[0].schema),
            Arc::as_ptr(&output.occurrences[1].schema)
        );
        if label_bytes == 512 {
            let a = token(output.occurrences[0].schema.slots()[1].field_ref().name());
            let b = token(output.occurrences[1].schema.slots()[1].field_ref().name());
            assert_ne!(a, b, "distinct real funded constructor invocations");
            assert_eq!(
                global_store().snapshot_ref(a).unwrap().origin,
                binding.account().id().get()
            );
            assert_eq!(
                global_store().snapshot_ref(b).unwrap().origin,
                binding.account().id().get()
            );
        }
        drop(output);
    }
}

#[test]
fn analytic_original_schema_actual_capacity_and_worker_stop_remain_nominal() {
    use novarocks_execution::runtime::{
        fragment::ExecutionFailureCause,
        preparation_memory::{PreparationMemoryRefusal, PreparationMemoryStop},
        preparation_metadata::PreparationMetadataFailure,
    };
    use novarocks_worker::PreparationStop;
    let _ = novarocks_server::memory_observation::snapshot();
    let binding = binding(64 << 20);
    let bytes = source(bigint(), 512);
    let owner = TestPreparationControl::new(Duration::from_millis(1));
    let loan = owner.loan();
    let mut journal = ProjectMetadataJournal::default();
    let first = compile(&bytes, &binding, &loan, &mut journal);
    let second = compile(&bytes, &binding, &loan, &mut journal);
    let node = first.program().graph().root();
    binding
        .account()
        .install_policy(0, novarocks_memory::LimitDimension::Work);
    let error = prepare_window_metadata_for_test(first, node, &binding, &loan, &Control)
        .err()
        .unwrap();
    assert!(matches!(
        error.cause(),
        ExecutionFailureCause::PreparationMetadata(PreparationMetadataFailure::Host(
            PreparationMemoryRefusal::Capacity(_)
        ))
    ));
    assert!(owner.stop(PreparationStop::Abort(
        novarocks_execution_contract::AbortCause::QueryFailed
    )));
    let error = prepare_window_metadata_for_test(second, node, &binding, &loan, &Control)
        .err()
        .unwrap();
    assert_eq!(
        error.cause(),
        &ExecutionFailureCause::PreparationMetadata(PreparationMetadataFailure::Host(
            PreparationMemoryRefusal::Stopped(PreparationMemoryStop::Abort(
                novarocks_execution_contract::AbortCause::QueryFailed
            )),
        ))
    );
}
#[test]
fn analytic_original_schema_actual_occupied_capacity_retains_qualified_shortage() {
    use novarocks_execution::runtime::{
        fragment::ExecutionFailureCause,
        kernel_memory::{KernelMemoryAdmission, KernelMemoryJournal, request_complete_operation},
        preparation_memory::PreparationMemoryRefusal,
        preparation_metadata::PreparationMetadataFailure,
    };
    let _ = novarocks_server::memory_observation::snapshot();
    const TOTAL: u64 = 2 << 20;
    const CAPACITY: u64 = TOTAL * 3 / 4;
    let binding = binding(TOTAL);
    let bytes = source(bigint(), 512);
    let owner = TestPreparationControl::new(Duration::from_millis(1));
    let loan = owner.loan();
    let mut journal = ProjectMetadataJournal::default();
    let compiled = compile(&bytes, &binding, &loan, &mut journal);
    let node = compiled.program().graph().root();
    let payload = usize::try_from(
        CAPACITY
            .checked_sub(binding.authority().root().committed_bytes())
            .unwrap()
            .checked_sub(1024 + 8)
            .unwrap(),
    )
    .unwrap();
    assert!(payload >= 512);
    let ready = match request_complete_operation(Some(&binding), payload + 8) {
        KernelMemoryAdmission::Granted(ready) => ready,
        other => panic!("real filler admission: {other:?}"),
    };
    let mut filler_journal = KernelMemoryJournal::default();
    let filler = ready
        .run(&mut filler_journal, || vec![0u8; payload])
        .unwrap();
    assert_eq!(
        filler_journal.settlement.as_ref().unwrap().accepted_live,
        (payload + 8) as u64
    );
    let error = prepare_window_metadata_for_test(compiled, node, &binding, &loan, &Control)
        .err()
        .unwrap();
    let ExecutionFailureCause::PreparationMetadata(PreparationMetadataFailure::Host(
        PreparationMemoryRefusal::SharedShortage(receipt),
    )) = error.cause()
    else {
        panic!("real qualified shortage: {error:?}")
    };
    assert!(receipt.coverage.complete);
    // The helper consumed the real producer. Its genuine frees after this
    // verdict may invalidate freshness; the nominal cause remains unchanged.
    drop(filler);
}
