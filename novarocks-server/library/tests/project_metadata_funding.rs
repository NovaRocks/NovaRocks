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

//! Actual Server GLOBAL and the two original Project metadata birth scopes.
//! Isolated with --test-threads=1; explicit retirement is protocol evidence,
//! not production retirement or MEM cost acceptance.
use arrow::{
    array::{ArrayRef, StructArray, new_null_array},
    datatypes::{DataType, Field},
};
use novarocks_execution::runtime::{
    kernel_memory::{KernelMemoryAdmission, KernelMemoryJournal, request_complete_operation},
    preparation_metadata::CompiledSchemaSite,
    query_memory::QueryMemoryBinding,
};
use novarocks_functions::{
    ConstantPolicy, EngineFunctionCatalogBuilder, FunctionId, FunctionKind, FunctionOverloadId,
    InstalledPureKernel, PureEngineFunctionCatalog, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi,
};
use novarocks_memory::{
    AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority, TeardownEvidence, TopUpPolicy,
    lane::{RecordRef, global_store},
};
use novarocks_native_adapter::backend_task_execution::{
    CompiledPackageInterpreter, CompiledTaskOptions, CompiledTaskProgram,
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
    identity::{AttemptId, QueryExecutionId},
};
use novarocks_worker::TestPreparationControl;
use prost::Message;
use std::{collections::BTreeMap, num::NonZeroUsize, sync::Arc, time::Duration};

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
fn interpreter() -> CompiledPackageInterpreter<std::convert::Infallible> {
    let providers = novarocks_connector_contract::PureProviderProgramCatalog::<
        std::convert::Infallible,
    >::try_new(&[], vec![], &Control)
    .unwrap();
    CompiledPackageInterpreter::new(
        FragmentDecodeResourceModel::try_new(&Control).unwrap(),
        decode_limits(),
        Arc::new(functions()),
        Arc::new(providers),
        constants(),
    )
}
fn json() -> FunctionValueType {
    FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json).unwrap()
}
fn nested() -> FunctionValueType {
    let storage = ValueTypeStorageLifetime::pure();
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let mut fields = vec![ValueField::new(
        "child_".repeat(100),
        json.value_type().clone(),
        storage.clone(),
    )];
    let mut physical = vec![Arc::new(Field::new(
        "child_".repeat(100),
        DataType::Utf8,
        true,
    ))];
    let leaf = FunctionValueType::new(DataType::Int64, true);
    fields.push(ValueField::new(
        "leaf",
        leaf.value_type().clone(),
        storage.clone(),
    ));
    physical.push(Arc::new(Field::new("leaf", DataType::Int64, true)));
    let value = ValueTypeRef::try_new(
        ValueKind::Struct(fields.into_boxed_slice()),
        true,
        storage.clone(),
    )
    .unwrap();
    let layout = ValueLayout::try_new(&DataType::Struct(physical.into()), storage).unwrap();
    FunctionValueType::try_from_value(value, layout).unwrap()
}
// A legal zero-row Values input and a computing Project value root. No data
// calculation is necessary to exercise the original immutable metadata births.
fn source(ty: FunctionValueType, label_bytes: usize) -> Vec<u8> {
    let values = NodeId::new(1);
    let project = NodeId::new(2);
    let mut builder = FragmentBuilder::new(FragmentId::new(57));
    let input = builder
        .add_value(
            ty.clone(),
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
        .add_expression(project, ty.clone(), ExprKind::Value(input))
        .unwrap();
    let output = builder
        .add_value(
            ty.clone(),
            ValueOrigin::Expr {
                node: project,
                expr: expression,
            },
        )
        .unwrap();
    builder
        .add_project(
            project,
            values,
            Box::from([(expression, output)]),
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
        .unwrap();
    let roots = PhysicalExpressionRoots::try_new(&fragment, &Control).unwrap();
    let mut invocations = Vec::new();
    let mut bindings = Vec::new();
    for (ordinal, (site, root)) in roots.sites().iter().enumerate() {
        let use_id = ExpressionUseId::new(u32::try_from(ordinal).unwrap());
        invocations.push(ExpressionInvocation {
            context: ExpressionEffectContext {
                use_id,
                domain: EvaluationDomainId::new(0),
                demand: root.demand,
            },
            definition: root.expr,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        bindings.push((*site, use_id));
    }
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(0),
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
    let calls = FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &Control).unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output: fragment.nodes()[&project].output.clone(),
        fields: Box::from([ResultField {
            domain: if ty.logical_type() == ValueLogicalType::Json {
                ResultValueDomain::Json
            } else {
                ResultValueDomain::Plain
            },
            name: "r".repeat(label_bytes).into_boxed_str(),
            alias: None,
            value: output,
            ty,
        }]),
    };
    let id = fragment.id();
    let package = FragmentPackage::try_new(
        FragmentPackageInput {
            version: PlanVersionId::try_new([57; 16]).unwrap(),
            required: RequiredContracts::default(),
            fragment,
            constants: ConstantPools::empty(),
            expression_uses: uses,
            calls,
            pruning: FrozenFragmentPruning::try_new(id, vec![], &Control).unwrap(),
            cuts: FragmentCuts::default(),
            result: Some(result),
            parameters: SemanticParameters::try_new([]).unwrap(),
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
fn construction_tracker() -> Arc<novarocks_execution::runtime::mem_tracker::MemTracker> {
    let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
        "typed schema funding test",
    );
    tracker.install_limit_once(64 << 20).unwrap();
    tracker
}
fn compile(
    bytes: &[u8],
    binding: &QueryMemoryBinding,
    tracker: &Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
    preparation: &novarocks_worker::PreparationControlLoan<'_>,
    journal: &mut ProjectMetadataJournal,
) -> CompiledTaskProgram {
    let mut types = TypeMaterializationJournal::default();
    let mut type_host = TypeMaterializationHost::new(binding, preparation, &mut types);
    let mut project_host = ProjectMetadataHost::new(binding, preparation, journal);
    novarocks_native_adapter::backend_task_execution::compile_native_package_with_funding_for_test(
        &interpreter(),
        bytes,
        CompiledTaskOptions {
            pipeline_dop: NonZeroUsize::MIN,
            exchange_wait: Duration::from_millis(50),
        },
        Arc::clone(tracker),
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
fn settled(journal: &ProjectMetadataJournal, has_fresh_large_backing: bool) {
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
    if has_fresh_large_backing {
        assert!(receipt.accepted_live > 0);
    } else {
        assert_eq!(
            receipt.accepted_live, 0,
            "shared Fields are not charged as fresh backing"
        );
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
fn project_original_compile_and_schema_aliases_keep_tagged_origin_to_cross_thread_last_free() {
    let _ = novarocks_server_application::memory_observation::snapshot();
    for ty in [json(), nested()] {
        let binding = binding(64 << 20);
        let tracker = construction_tracker();
        let bytes = source(ty.clone(), 512);
        let owner = TestPreparationControl::new(
            preparation_identity(binding.execution()),
            Duration::from_millis(1),
        );
        let loan = owner.loan();
        let mut journal = ProjectMetadataJournal::default();
        let compiled = compile(&bytes, &binding, &tracker, &loan, &mut journal);
        settled(&journal, true);
        let root = compiled.program().graph().root();
        let first = Arc::clone(
            &compiled.program().graph().nodes()[root.index()]
                .output_layout()
                .schema()
                .fields()[0],
        );
        let first_token = token(first.name());
        assert_eq!(
            compiled.program().graph().nodes()[root.index()]
                .output_layout()
                .value_binding_at(0),
            Some(ty.typed_binding()),
        );
        assert!(first.metadata().as_arc().is_none());
        if let DataType::Struct(children) = first.data_type() {
            for child in children {
                assert!(child.metadata().as_arc().is_none());
            }
        }
        let PreparedProjectMetadataForTest {
            program,
            factory,
            schema,
        } = prepare_project_metadata_for_test(
            compiled,
            CompiledSchemaSite::Project(root),
            &binding,
            Arc::clone(&tracker),
            &loan,
            &Control,
            &mut journal,
        )
        .unwrap();
        settled(&journal, false);
        let last = Arc::clone(&schema.arrow_schema_ref().fields()[0]);
        let last_token = token(last.name());
        assert!(
            Arc::ptr_eq(&first, &last),
            "runtime shares the accurately bound immutable Field"
        );
        assert_eq!(
            first_token, last_token,
            "shared backing keeps its original funded record"
        );
        assert_eq!(first.as_ref(), last.as_ref());
        for reference in [first_token, last_token] {
            let snapshot = global_store().snapshot_ref(reference).unwrap();
            assert_eq!(snapshot.origin, binding.account().id().get());
            assert!(snapshot.outstanding > 0 && snapshot.tagged_bytes > 0);
        }
        let array = StructArray::new(
            vec![Arc::clone(&last)].into(),
            vec![new_null_array(last.data_type(), 1) as ArrayRef],
            None,
        );
        drop(program);
        drop(factory);
        drop(schema);
        drop(array);
        drop(bytes);
        for reference in [first_token, last_token] {
            assert!(global_store().snapshot_ref(reference).unwrap().outstanding > 0);
        }
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
        // The earlier type producer has already exited. Only its genuinely
        // idle rights may return; independent aliases keep the original
        // shared backing responsibility through retirement.
        let retained_committed = committed.checked_sub(transfer.returned_idle).unwrap();
        assert!(retained_committed > 0);
        assert_eq!(
            binding.authority().root().committed_bytes(),
            retained_committed
        );
        for reference in [first_token, last_token] {
            let retained = global_store().snapshot_ref(reference).unwrap();
            assert!(retained.outstanding > 0 && retained.tagged_bytes > 0);
            assert_eq!(retained.origin, binding.account().id().get());
        }
        std::thread::spawn(move || {
            drop(first);
            assert!(
                global_store().snapshot_ref(last_token).unwrap().outstanding > 0,
                "the last independent Field alias still retains the original backing"
            );
            drop(last);
        })
        .join()
        .unwrap();
        for reference in [first_token, last_token] {
            let freed = global_store().snapshot_ref(reference).unwrap();
            assert_eq!((freed.outstanding, freed.tagged_bytes), (0, 0));
        }
        binding
            .authority()
            .request_maintenance(novarocks_memory::MaintenanceReason::ExplicitLocalReclaim);
        binding
            .authority()
            .maintain(binding.authority().maintenance_scan_bound().unwrap());
        assert!(binding.authority().root().committed_bytes() < retained_committed);
    }
}
fn functions() -> PureEngineFunctionCatalog {
    let actual = novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog()
        .expect("builtin catalogue");
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            actual
                .definition("rand", FunctionKind::Scalar)
                .expect("rand")
                .clone(),
        )
        .expect("register rand");
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
        .expect("sealed rand subset")
}

#[test]
fn project_original_schema_host_preserves_capacity_and_worker_stop_before_body() {
    use novarocks_execution::runtime::{
        fragment::ExecutionFailureCause, preparation_memory::PreparationMemoryRefusal,
        preparation_metadata::PreparationMetadataFailure,
    };
    use novarocks_worker::TestPreparationIntent;
    let _ = novarocks_server_application::memory_observation::snapshot();
    let binding = binding(64 << 20);
    let tracker = construction_tracker();
    let bytes = source(json(), 512);
    let owner = TestPreparationControl::new(
        preparation_identity(binding.execution()),
        Duration::from_millis(1),
    );
    let loan = owner.loan();
    let mut journal = ProjectMetadataJournal::default();
    let first = compile(&bytes, &binding, &tracker, &loan, &mut journal);
    let second = compile(&bytes, &binding, &tracker, &loan, &mut journal);
    let site = CompiledSchemaSite::Project(first.program().graph().root());
    binding
        .account()
        .install_policy(0, novarocks_memory::LimitDimension::Work);
    let error = prepare_project_metadata_for_test(
        first,
        site,
        &binding,
        Arc::clone(&tracker),
        &loan,
        &Control,
        &mut journal,
    )
    .err()
    .expect("actual account policy refuses schema admission");
    assert!(matches!(
        error.cause(),
        ExecutionFailureCause::PreparationMetadata(PreparationMetadataFailure::Host(
            PreparationMemoryRefusal::Capacity(_)
        ))
    ));
    assert!(journal.capacity.is_some());
    assert!(journal.body.settlement.is_none());
    assert!(owner.stop(TestPreparationIntent::Abort(
        novarocks_execution_contract::AbortCause::QueryFailed
    )));
    let accepted = loan
        .checkpoint()
        .expect_err("original Worker accepted abort");
    let error = prepare_project_metadata_for_test(
        second,
        site,
        &binding,
        Arc::clone(&tracker),
        &loan,
        &Control,
        &mut journal,
    )
    .err()
    .expect("original Worker stop precedes another admission");
    let ExecutionFailureCause::PreparationMetadata(PreparationMetadataFailure::Host(
        PreparationMemoryRefusal::Interrupted(original),
    )) = error.cause()
    else {
        panic!("expected original Worker interruption: {error:?}");
    };
    assert!(std::ptr::eq(original.cause(), accepted.cause()));
    assert!(journal.body.settlement.is_none());
    assert!(journal.capacity.is_none());
}

#[test]
fn project_original_schema_host_preserves_real_qualified_shared_shortage_before_body() {
    use novarocks_execution::runtime::{
        fragment::ExecutionFailureCause, preparation_memory::PreparationMemoryRefusal,
        preparation_metadata::PreparationMetadataFailure,
    };
    let _ = novarocks_server_application::memory_observation::snapshot();
    const TOTAL: u64 = 2 << 20;
    const CAPACITY: u64 = TOTAL * 3 / 4;
    let binding = binding(TOTAL);
    let tracker = construction_tracker();
    let bytes = source(json(), 512);
    let owner = TestPreparationControl::new(
        preparation_identity(binding.execution()),
        Duration::from_millis(1),
    );
    let loan = owner.loan();
    let mut journal = ProjectMetadataJournal::default();
    let compiled = compile(&bytes, &binding, &tracker, &loan, &mut journal);
    let site = CompiledSchemaSite::Project(compiled.program().graph().root());
    let original = token(
        compiled.program().graph().nodes()[compiled.program().graph().root().index()]
            .output_layout()
            .schema()
            .fields()[0]
            .name(),
    );
    // Settle the original producers before measuring real remaining capacity.
    // The runtime now shares their labels; reclaimable old idle is not filler.
    binding
        .authority()
        .request_maintenance(novarocks_memory::MaintenanceReason::ExplicitLocalReclaim);
    let coverage = binding
        .authority()
        .maintain(binding.authority().maintenance_scan_bound().unwrap());
    assert!(coverage.complete);
    assert_eq!(coverage.deferred_active, 0);
    let token_bytes = novarocks_memory::attribution::ATTRIBUTION_TOKEN_BYTES;
    let payload = usize::try_from(
        CAPACITY
            .checked_sub(binding.authority().root().committed_bytes())
            .unwrap()
            .checked_sub(novarocks_memory::OWNER_METADATA_BYTES)
            .unwrap()
            .checked_sub(u64::try_from(token_bytes).unwrap())
            .unwrap(),
    )
    .unwrap();
    assert!(payload >= 512);
    let ready = match request_complete_operation(Some(&binding), payload + token_bytes) {
        KernelMemoryAdmission::Granted(ready) => ready,
        other => panic!("actual filler request must be granted: {other:?}"),
    };
    let mut filler_journal = KernelMemoryJournal::default();
    let filler = ready
        .run(&mut filler_journal, || vec![0u8; payload])
        .unwrap();
    let settlement = filler_journal.settlement.as_ref().unwrap();
    assert_eq!(settlement.accepted_live, (payload + token_bytes) as u64);
    assert_eq!(settlement.debt, 0);
    assert_eq!(filler_journal.settlement.as_ref().unwrap().debt, 0);
    assert_eq!(binding.authority().root().committed_bytes(), CAPACITY);
    let error = prepare_project_metadata_for_test(
        compiled,
        site,
        &binding,
        Arc::clone(&tracker),
        &loan,
        &Control,
        &mut journal,
    )
    .err()
    .expect("actual occupied capacity refuses the schema request");
    let ExecutionFailureCause::PreparationMetadata(PreparationMetadataFailure::Host(
        PreparationMemoryRefusal::SharedShortage(receipt),
    )) = error.cause()
    else {
        panic!("qualified shortage must remain nominal: {error:?}");
    };
    assert_eq!(journal.shortage.as_ref(), Some(receipt));
    assert!(receipt.coverage.complete);
    // This helper consumes the actual native producer. Error rollback drops
    // its last program owner after the host issued a complete receipt; those
    // genuine frees invalidate freshness. The nominal receipt survives, but
    // cannot be reused as a fresh admission verdict after that lifecycle cut.
    assert_eq!(
        global_store().snapshot_ref(original).unwrap().outstanding,
        0
    );
    assert!(
        !binding
            .authority()
            .shortage_is_fresh(receipt, Duration::from_secs(1))
    );
    assert_eq!(
        receipt.refusal.requested,
        journal.workset_bytes.unwrap() as u64
    );
    assert!(journal.body.settlement.is_none());
    assert!(journal.capacity.is_none());
    drop(filler);
    drop(ready);
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
