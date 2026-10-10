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
    preparation_metadata::ProjectSchemaSite,
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
    CompiledPackageCompiler, CompiledPackageInterpreter, CompiledTaskOptions, CompiledTaskProgram,
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
fn json() -> FunctionValueType {
    FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json).unwrap()
}
fn nested() -> FunctionValueType {
    let child =
        Field::new("child_".repeat(100), DataType::Utf8, true).with_metadata(HashMap::from([
            ("nr_logical_type".to_owned(), "json".to_owned()),
            ("source_note".to_owned(), "original metadata".repeat(32)),
        ]));
    FunctionValueType::new(
        DataType::Struct(
            vec![
                Arc::new(child),
                Arc::new(Field::new("leaf", DataType::Int64, true)),
            ]
            .into(),
        ),
        true,
    )
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
            domain: if ty.logical_type == ValueLogicalType::Json {
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
fn settled(journal: &ProjectMetadataJournal) {
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
    assert!(receipt.accepted_live > 0);
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
    let _ = novarocks_server::memory_observation::snapshot();
    for ty in [json(), nested()] {
        let binding = binding(64 << 20);
        let bytes = source(ty.clone(), 512);
        let owner = TestPreparationControl::new(Duration::from_millis(1));
        let loan = owner.loan();
        let mut journal = ProjectMetadataJournal::default();
        let compiled = compile(&bytes, &binding, &loan, &mut journal);
        settled(&journal);
        let root = compiled.program().graph().root();
        let first = Arc::clone(
            &compiled.program().graph().nodes()[root.index()]
                .output_layout()
                .schema()
                .fields()[0],
        );
        let first_token = token(first.name());
        assert_eq!(FunctionValueType::try_from_field(&first).unwrap(), ty);
        let PreparedProjectMetadataForTest {
            program,
            factory,
            schema,
        } = prepare_project_metadata_for_test(
            compiled,
            ProjectSchemaSite::Project(root),
            &binding,
            &loan,
            &Control,
            &mut journal,
        )
        .unwrap();
        settled(&journal);
        let last = Arc::clone(&schema.arrow_schema_ref().fields()[0]);
        let last_token = token(last.name());
        assert_ne!(
            first_token, last_token,
            "the two original births retain distinct real records"
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
        // idle rights may return; the two independent live alias records keep
        // their exact responsibility through retirement.
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
        fragment::ExecutionFailureCause,
        preparation_memory::{PreparationMemoryRefusal, PreparationMemoryStop},
        preparation_metadata::PreparationMetadataFailure,
    };
    use novarocks_worker::PreparationStop;
    let _ = novarocks_server::memory_observation::snapshot();
    let binding = binding(64 << 20);
    let bytes = source(json(), 512);
    let owner = TestPreparationControl::new(Duration::from_millis(1));
    let loan = owner.loan();
    let mut journal = ProjectMetadataJournal::default();
    let first = compile(&bytes, &binding, &loan, &mut journal);
    let second = compile(&bytes, &binding, &loan, &mut journal);
    let site = ProjectSchemaSite::Project(first.program().graph().root());
    binding
        .account()
        .install_policy(0, novarocks_memory::LimitDimension::Work);
    let error =
        prepare_project_metadata_for_test(first, site, &binding, &loan, &Control, &mut journal)
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
    assert!(owner.stop(PreparationStop::Abort(
        novarocks_execution_contract::AbortCause::QueryFailed
    )));
    let error =
        prepare_project_metadata_for_test(second, site, &binding, &loan, &Control, &mut journal)
            .err()
            .expect("original Worker stop precedes another admission");
    assert_eq!(
        error.cause(),
        &ExecutionFailureCause::PreparationMetadata(PreparationMetadataFailure::Host(
            PreparationMemoryRefusal::Stopped(PreparationMemoryStop::Abort(
                novarocks_execution_contract::AbortCause::QueryFailed
            ))
        ))
    );
    assert!(journal.body.settlement.is_none());
    assert!(journal.capacity.is_none());
}

#[test]
fn project_original_schema_host_preserves_real_qualified_shared_shortage_before_body() {
    use novarocks_execution::runtime::{
        fragment::ExecutionFailureCause, preparation_memory::PreparationMemoryRefusal,
        preparation_metadata::PreparationMetadataFailure,
    };
    let _ = novarocks_server::memory_observation::snapshot();
    const TOTAL: u64 = 2 << 20;
    const CAPACITY: u64 = TOTAL * 3 / 4;
    let binding = binding(TOTAL);
    let bytes = source(json(), 512);
    let owner = TestPreparationControl::new(Duration::from_millis(1));
    let loan = owner.loan();
    let mut journal = ProjectMetadataJournal::default();
    let compiled = compile(&bytes, &binding, &loan, &mut journal);
    let site = ProjectSchemaSite::Project(compiled.program().graph().root());
    let original = token(
        compiled.program().graph().nodes()[compiled.program().graph().root().index()]
            .output_layout()
            .schema()
            .fields()[0]
            .name(),
    );
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
        other => panic!("actual filler request must be granted: {other:?}"),
    };
    let mut filler_journal = KernelMemoryJournal::default();
    let filler = ready
        .run(&mut filler_journal, || vec![0u8; payload])
        .unwrap();
    let settlement = filler_journal.settlement.as_ref().unwrap();
    assert_eq!(settlement.accepted_live, (payload + 8) as u64);
    assert_eq!(settlement.debt, 0);
    let error =
        prepare_project_metadata_for_test(compiled, site, &binding, &loan, &Control, &mut journal)
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
