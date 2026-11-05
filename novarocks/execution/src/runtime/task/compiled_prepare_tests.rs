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

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use arrow::array::{Float64Array, Int64Array};
use novarocks_types::{QueryId, UniqueId};

use super::*;
use crate::exec::chunk::Chunk;
use crate::exec::expr::compiled_program::tests::{SEED_42_FIRST, SeedMode, program};
use crate::exec::fragment::program::FragmentContractVersion;
use crate::exec::fragment::program::FragmentNodeId;
use crate::exec::node::scan::{BoundScanRanges, ScanSource};
use crate::runtime::observable::Observable;
use crate::runtime::query_options::QueryOptions;
use crate::runtime::task::instance::{
    ExchangeInputAssignment, ExchangeInputAssignments, InstanceOrdinal, ScanAssignments, TaskKey,
    TaskRuntimeOptions, TaskSinkAssignment,
};
use crate::runtime::task::io::{
    ResultAbort, ResultWriteAdmission, ResultWriteCredit, ResultWriteSpec, TaskIoError,
    TaskResultSession, TaskResultWriter,
};
use crate::runtime::task::scan::compiled_fixture::{
    FixtureScanOp, FixtureScanSource, SCAN_NODE, rows, scan_chunk, scan_program,
};
use crate::runtime::task::{ExecutionFailureCause, ExecutionResult};

#[derive(Default)]
struct CollectingSession {
    chunks: Mutex<Vec<Chunk>>,
    finished: Mutex<bool>,
}

impl TaskResultSession for CollectingSession {
    fn reservation_bytes(
        &self,
        input: crate::runtime::task::io::ResultInputMetadata,
    ) -> Result<usize, TaskIoError> {
        Ok(input.retained_bytes())
    }

    fn try_acquire(&self, bytes: usize) -> Result<ResultWriteAdmission, TaskIoError> {
        Ok(ResultWriteAdmission::Granted(ResultWriteCredit::new(
            bytes,
            |_| {},
        )))
    }

    fn writable_observable(&self) -> Option<Arc<Observable>> {
        None
    }

    fn write_with_credit(
        &self,
        chunk: Chunk,
        _credit: ResultWriteCredit,
        _factory: &dyn crate::exec::pipeline::operator::InputConstructionFactory,
        _control: &dyn novarocks_functions::KernelEvaluationControl,
    ) -> Result<(), crate::runtime::task::io::ResultWriteFailure> {
        self.chunks.lock().unwrap().push(chunk);
        Ok(())
    }

    fn finish(&self) -> Result<(), TaskIoError> {
        *self.finished.lock().unwrap() = true;
        Ok(())
    }

    fn abort(&self, _reason: ResultAbort) {}
}

struct CollectingWriter {
    session: Arc<CollectingSession>,
}

impl TaskResultWriter for CollectingWriter {
    fn open(&self, _spec: ResultWriteSpec) -> Result<Arc<dyn TaskResultSession>, TaskIoError> {
        Ok(self.session.clone())
    }
}

fn refused(result: Result<PreparedTaskExecution, LocalTaskLaunchError>) -> LocalTaskLaunchError {
    match result {
        Ok(_) => panic!("compiled preparation must be refused"),
        Err(error) => error,
    }
}

fn instance(
    task_key: UniqueId,
    pipeline_dop: usize,
    exchange_inputs: ExchangeInputAssignments,
) -> TaskInstanceSpec {
    TaskInstanceSpec::new_native(
        FragmentContractVersion::CURRENT,
        QueryId::new(task_key.high() - 2, task_key.low() - 2),
        TaskKey::new(task_key),
        ScanAssignments::default(),
        exchange_inputs,
        TaskSinkAssignment::None,
        TaskRuntimeOptions::new(QueryOptions::default(), false),
        NonZeroUsize::new(pipeline_dop).expect("nonzero DOP"),
        InstanceOrdinal::try_new(1).expect("backend number"),
    )
}

pub(in crate::runtime::task) fn sql_gather_receiver_program() -> Arc<LocalProgram> {
    use novarocks_connector_contract::PureProviderProgramCatalog;
    use novarocks_functions::ConstantPolicy;
    use novarocks_local_compiler::{
        LocalCompileOptions, compile_fragment, validate_fragment_providers,
    };
    use novarocks_local_program::KernelAbiVersion;
    use novarocks_physical_plan::{
        FragmentPackageAdmission, NodeKind, PipelineDopDomain, PlanLimits, PlanVersionId,
        PropertyProofProjectionLimits, ScanReadBudget, extract_fragment_packages,
    };
    use novarocks_sql::compiler::{
        DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlCompileControl, SqlCompileIntent,
        SqlCompiler, SqlFinalPlanCompileRequest, SqlPlanningEnvironment, SqlSessionContext,
        SqlStatementInput, author_fragment_package_semantics, builtin_sql_function_catalog,
        noop_constant_evaluator,
    };

    // Explicit table-free fixture limits; no scan or runtime creator is mocked.
    let policy = ConstantPolicy {
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
    };
    let control = SqlCompileControl::unbounded();
    let request = SqlFinalPlanCompileRequest::new(
        PlanVersionId::try_new([73; 16]).unwrap(),
        SqlStatementInput::sql("SELECT 1"),
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            current_catalog: Some("iceberg".into()),
            current_database: "db".into(),
            optimizer_settings: SessionOptimizerSettings::default(),
        },
        SqlPlanningEnvironment::Distributed,
        builtin_sql_function_catalog().snapshot(),
        noop_constant_evaluator(),
        policy,
        SqlCompileControl::unbounded(),
        PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        ScanReadBudget {
            max_batch_rows: 4096,
            max_batch_bytes: 1 << 20,
        },
        DEFAULT_COMPLETION_LIMITS,
    )
    .try_into_completion()
    .unwrap();
    let source = SqlCompiler::start(request, &control)
        .unwrap()
        .into_complete()
        .expect("SELECT 1 needs no provider observation")
        .into_parts()
        .0;
    let semantics = author_fragment_package_semantics(&source, policy, &control).unwrap();
    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for (id, authored) in semantics {
        uses.insert(id, authored.expression_uses);
        calls.insert(id, authored.calls);
        pruning.insert(id, authored.pruning);
        admissions.insert(
            id,
            FragmentPackageAdmission {
                plan_limits: PlanLimits::FROZEN,
                source_retained_bytes: 64 << 20,
                property_projection_limits: PropertyProofProjectionLimits {
                    max_request_bytes: 16 << 20,
                    max_coexisting_bytes: 256 << 20,
                    max_projection_work: 16 << 20,
                },
            },
        );
    }
    let receiver_fragment = source
        .plan()
        .fragments()
        .iter()
        .find(|(_, fragment)| {
            fragment
                .nodes()
                .values()
                .any(|node| matches!(node.kind, NodeKind::ExchangeSource { .. }))
        })
        .map(|(id, _)| *id)
        .expect("distributed SELECT 1 has a real gather receiver");
    let mut packages = extract_fragment_packages(
        source.plan(),
        &BTreeMap::new(),
        &BTreeMap::new(),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &control,
    )
    .unwrap();
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &control).unwrap();
    let validated = validate_fragment_providers(
        Arc::new(packages.remove(&receiver_fragment).unwrap()),
        &providers,
        &control,
    )
    .unwrap();
    Arc::new(
        compile_fragment(
            validated,
            &crate::exec::expr::compiled_program::tests::rng_subset(),
            LocalCompileOptions {
                pipeline_dop: NonZeroUsize::new(1).unwrap(),
                root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
                kernel_abi: KernelAbiVersion::CURRENT,
                constants: policy,
            },
            &control,
        )
        .unwrap(),
    )
}

#[test]
fn compiled_wire_recipe_and_local_schema_outlive_program_until_last_buffer_alias() {
    use crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory;
    use crate::runtime::exchange::{
        ExchangeColumnBinding, ExchangeKey, ExchangeSenderIdentity, ExecutionExchangeRegistry,
        online_unit,
    };
    use crate::runtime::runtime_state::RuntimeErrorState;
    let program = sql_gather_receiver_program();
    let weak = Arc::downgrade(&program);
    let (&id, input) = program.exchange_inputs().iter().next().unwrap();
    let tracker =
        crate::runtime::mem_tracker::MemTracker::new_root("wire and local receiver custody");
    let factory = try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker.clone(),
        Arc::new(RuntimeErrorState::default()),
    )
    .unwrap();
    let wire = factory
        .create_compiled_chunk_schema(&input.wire_layout)
        .unwrap();
    let local = factory
        .create_compiled_chunk_schema(program.graph().nodes()[id.index()].output_layout())
        .unwrap();
    assert_eq!(wire.slots().len(), 1);
    assert_eq!(
        wire.slots()[0].data_type(),
        &arrow::datatypes::DataType::Int64
    );
    let registry = ExecutionExchangeRegistry::default();
    let key = ExchangeKey {
        task_key_hi: 88021,
        task_key_lo: 88022,
        node_id: input.receiver_node as i32,
    };
    registry
        .try_register_expected_chunk_schema_with_binding(
            key,
            1,
            wire.clone(),
            local.clone(),
            ExchangeColumnBinding::Positional,
            factory.clone(),
        )
        .unwrap();
    drop(program);
    assert!(
        weak.upgrade().is_none(),
        "receiver facts cannot pin the whole Program"
    );
    let source =
        Chunk::try_new_with_columns(wire.clone(), vec![Arc::new(Int64Array::from(vec![7, 19]))])
            .unwrap();
    let recipe = online_unit::prepare_recipe(&wire, &[], factory.as_ref()).unwrap();
    let control = crate::exec::operators::compiled_expression::RuntimeKernelControl::new(Arc::new(
        RuntimeErrorState::default(),
    ));
    let payload = online_unit::encode(&recipe, &source, factory.as_ref(), &control).unwrap();
    let mut decoded = registry
        .decode_chunks_for_sender(key, ExchangeSenderIdentity::local(1, 1), &payload)
        .unwrap();
    let chunk = decoded.pop().unwrap();
    let schema = chunk.chunk_schema_ref();
    let slot = schema.slots()[0].clone();
    let bare_buffer = chunk.columns()[0].to_data().buffers()[0].clone();
    assert_eq!(chunk.len(), 2);
    assert_eq!(schema.as_ref(), local.as_ref());
    drop(chunk);
    drop(decoded);
    drop(source);
    drop(wire);
    drop(local);
    drop(recipe);
    registry.remove_exchange_key(key);
    drop(registry);
    drop(factory);
    assert!(tracker.current() > 0);
    drop(schema);
    assert!(tracker.current() > 0);
    drop(slot);
    assert!(
        tracker.current() > 0,
        "bare Arrow alias retains its original creator storage"
    );
    drop(bare_buffer);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn wire_and_local_schema_memory_refusals_release_all_partial_construction() {
    use crate::exec::pipeline::operator::{
        ConstructionFailure, InputConstructionFactory, InputConstructionOwner,
    };
    use crate::runtime::mem_tracker::MemTracker;
    struct RefusingSchemaFactory {
        inner: Arc<dyn InputConstructionFactory>,
        tracker: Arc<MemTracker>,
        begins: std::sync::atomic::AtomicUsize,
        refuse_at: usize,
    }
    impl InputConstructionFactory for RefusingSchemaFactory {
        fn observe_input_work(&self, units: usize) -> Result<(), ConstructionFailure> {
            self.inner.observe_input_work(units)
        }
        fn begin_typed_construction(
            &self,
            schema: &crate::exec::chunk::ChunkSchema,
        ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
            self.inner.begin_typed_construction(schema)
        }
        fn begin_compiled_layout_construction(
            &self,
            layout: &novarocks_local_program::StaticLayout,
        ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
            if self
                .begins
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1
                == self.refuse_at
            {
                self.tracker
                    .install_limit_once(self.tracker.current() + 1)
                    .unwrap();
            }
            self.inner.begin_compiled_layout_construction(layout)
        }
    }
    let program = sql_gather_receiver_program();
    let receiver_id = FragmentNodeId::new(
        program
            .exchange_inputs()
            .values()
            .next()
            .unwrap()
            .receiver_node as i32,
    );
    let assignments = ExchangeInputAssignments::new(BTreeMap::from([(
        receiver_id,
        ExchangeInputAssignment::new(NonZeroUsize::new(1).unwrap()),
    )]));
    for refuse_at in [1, 2] {
        let tracker = MemTracker::new_root("wire and local schema refusal");
        let inner = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
            tracker.clone(), Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
        ).unwrap();
        let baseline = tracker.current();
        let factory = Arc::new(RefusingSchemaFactory {
            inner,
            tracker: tracker.clone(),
            begins: std::sync::atomic::AtomicUsize::new(0),
            refuse_at,
        });
        let original: Arc<dyn InputConstructionFactory> = factory.clone();
        let result = crate::runtime::task::exchange::materialize_compiled_exchange_receivers(
            &program,
            UniqueId::new(88023, refuse_at as i64),
            &assignments,
            Arc::new(crate::runtime::task::io::UnavailableExchangeReceiverPort),
            Some(&original),
        );
        assert!(matches!(result, Err(ConstructionFailure::Memory(_))));
        assert_eq!(
            factory.begins.load(std::sync::atomic::Ordering::SeqCst),
            refuse_at
        );
        assert_eq!(
            tracker.current(),
            baseline,
            "partial wire/local schema storage must actually exit"
        );
        drop(original);
        drop(factory);
        assert_eq!(tracker.current(), 0);
    }
}

#[test]
fn compiled_receiver_after_exchange_failure_preserves_primary_cleanup_and_control() {
    use super::super::owner_tests::ReceiverConstructionPort;
    use crate::exec::pipeline::operator::ConstructionFailure;
    use crate::runtime::task::io::ExchangeReceiverPort;
    use crate::runtime::task::resources::ResourceKind;
    use crate::runtime::task::{LocalTaskLaunchErrorKind, LocalTaskLaunchStage};

    let program = sql_gather_receiver_program();
    assert_eq!(program.exchange_inputs().len(), 1);
    let (&node, input) = program.exchange_inputs().iter().next().unwrap();
    assert!(matches!(
        program.graph().nodes()[node.index()].kind(),
        ProgramNodeKind::ExchangeSource { .. }
    ));
    let receiver_id = FragmentNodeId::new(i32::try_from(input.receiver_node).unwrap());
    let addresses = ExchangeInputAssignments::new(BTreeMap::from([(
        receiver_id,
        ExchangeInputAssignment::new(NonZeroUsize::new(1).unwrap()),
    )]));
    let submission = CompiledTaskSubmission::try_new(
        Arc::clone(&program),
        CompiledScanSources::new(),
        instance(UniqueId::new(247, 248), 1, addresses),
    )
    .unwrap();
    assert!(matches!(
        program.graph().sink(),
        Some(StaticSinkProgram::Result)
    ));
    let session = Arc::new(CollectingSession::default());
    let runtime = crate::runtime::execution_runtime::test_execution_runtime();
    let port = ReceiverConstructionPort::new(Arc::clone(&runtime));
    let tracker = crate::runtime::mem_tracker::MemTracker::new_root("compiled receiver rollback");
    tracker.install_limit_once(64 << 20).unwrap();
    let mut context = LocalTaskPrepareContext {
        result_writer: Arc::new(CollectingWriter {
            session: Arc::clone(&session),
        }),
        ..LocalTaskPrepareContext::default()
    }
    .with_execution_runtime(runtime)
    .with_registration_owned_exchange_receivers()
    .with_prepare_failure(PrepareFailurePoint::AfterExchange)
    .with_cleanup_failure(ResourceKind::Exchange);
    context.mem_tracker = Some(Arc::clone(&tracker));
    context.exchange_receiver_port = port.clone();
    let error = refused(prepare_compiled_task(submission, context));
    assert_eq!(error.stage(), LocalTaskLaunchStage::Register);
    assert_eq!(error.kind(), LocalTaskLaunchErrorKind::ResourceUnavailable);
    assert_eq!(
        error.detail(),
        "injected failure after exchange registration"
    );
    assert_eq!(
        error
            .cleanup_diagnostics()
            .iter()
            .map(|cause| cause.detail())
            .collect::<Vec<_>>(),
        &["injected exchange registration cleanup failure".to_owned()]
    );
    let registration = {
        let registrations = port.registrations.lock().unwrap();
        assert_eq!(registrations.len(), 1);
        registrations[0].clone()
    };
    assert_eq!(registration.key.node_id, receiver_id.get());
    assert!(port.snapshot(registration.key).is_none());
    match registration
        .creation_factory
        .begin_typed_construction(&registration.expected_chunk_schema)
    {
        Err(ConstructionFailure::Caller(_)) => {}
        Err(other) => panic!("rollback lost its stopped receiver control: {other:?}"),
        Ok(_) => panic!("rolled-back receiver must refuse new construction"),
    }
    assert!(
        tracker.current() > 0,
        "the original factory loan stays owned"
    );
    drop(registration);
    port.registrations.lock().unwrap().clear();
    drop(port);
    drop(session);
    drop(program);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn incomplete_or_foreign_task_capabilities_are_refused_before_host_acquisition() {
    use crate::runtime::task_control::test_support::Owner;
    use novarocks_execution_contract::{TaskConclusionPort, TaskFailurePhase};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct FirstHostCall(AtomicUsize);
    impl crate::runtime::task::io::TaskCommitPort for FirstHostCall {
        fn acquire(
            &self,
            _: UniqueId,
        ) -> ExecutionResult<Box<dyn crate::runtime::task::io::TaskCommitRegistration>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(novarocks_functions::KernelFailure::ResourceExhausted.into())
        }
    }
    // No first-host-call refusal is allowed to hide a missing role/control
    // error. Each malformed Task binding must fail at the submission boundary.
    for scenario in 0..6 {
        let owner = Owner::new();
        let foreign = Owner::new();
        let calls = Arc::new(FirstHostCall(AtomicUsize::new(0)));
        let control = Arc::new(owner.view(TaskFailurePhase::Execution));
        let mut context = LocalTaskPrepareContext::default().with_task_commit_port(calls.clone());
        context = match scenario {
            0 => context
                .with_result_identity(owner.identity())
                .with_task_control(control),
            1 => context
                .with_result_identity(owner.identity())
                .with_completion_work(owner.issue_fixture_completion_work()),
            2 => context
                .with_result_identity(owner.identity())
                .with_task_control(Arc::new(foreign.view(TaskFailurePhase::Execution)))
                .with_completion_work(owner.issue_fixture_completion_work()),
            3 => context
                .with_result_identity(owner.identity())
                .with_task_control(control)
                .with_completion_work(foreign.issue_fixture_completion_work()),
            4 => context.with_task_control(control),
            5 => context.with_completion_work(owner.issue_fixture_completion_work()),
            _ => unreachable!(),
        };
        let submission = CompiledTaskSubmission::try_new(
            program(SeedMode::Input, false),
            CompiledScanSources::new(),
            instance(
                UniqueId::new(201, 202),
                1,
                ExchangeInputAssignments::default(),
            ),
        )
        .unwrap();
        let error = refused(prepare_compiled_task(submission, context));
        assert_eq!(
            error.stage(),
            LocalTaskLaunchStage::ValidateSubmission,
            "scenario {scenario}"
        );
        assert_eq!(
            error.kind(),
            LocalTaskLaunchErrorKind::Binding,
            "scenario {scenario}"
        );
        assert_eq!(
            calls.0.load(Ordering::SeqCst),
            0,
            "scenario {scenario} reached host acquisition"
        );
        assert!(error.cleanup_diagnostics().is_empty());
    }
}

// The Task-facing entry prepares a compiled program into the ordinary dormant
// handle: its result rows reach the opened result session, computed only by
// compiled roots.
#[test]
fn compiled_submission_prepares_and_runs_into_the_result_session() {
    let program = program(SeedMode::Input, false);
    let submission = CompiledTaskSubmission::try_new(
        program,
        CompiledScanSources::new(),
        instance(
            UniqueId::new(201, 202),
            1,
            ExchangeInputAssignments::default(),
        ),
    )
    .expect("compiled submission");
    assert_eq!(submission.sink_kind(), FragmentSinkKind::Result);
    let session = Arc::new(CollectingSession::default());
    let context = LocalTaskPrepareContext {
        result_writer: Arc::new(CollectingWriter {
            session: Arc::clone(&session),
        }),
        ..LocalTaskPrepareContext::default()
    };
    let handle = prepare_compiled_task(submission, context).expect("compiled prepare");
    assert!(matches!(
        handle.start().join().outcome(),
        LocalTaskOutcome::Succeeded
    ));
    let chunks = session.chunks.lock().unwrap();
    let rows: usize = chunks.iter().map(Chunk::len).sum();
    assert_eq!(rows, 1);
    let batch = chunks
        .iter()
        .find(|chunk| chunk.len() == 1)
        .unwrap()
        .record_batch();
    let sample = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("RAND result is Float64");
    assert_eq!(sample.value(0).to_bits(), SEED_42_FIRST);
    let seed = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("seed passthrough is Int64");
    assert_eq!(seed.value(0), 42);
}

#[test]
fn compiled_submission_refuses_a_foreign_dop_and_unaddressed_inputs() {
    let program = program(SeedMode::Input, false);
    let error = CompiledTaskSubmission::try_new(
        Arc::clone(&program),
        CompiledScanSources::new(),
        instance(
            UniqueId::new(203, 204),
            2,
            ExchangeInputAssignments::default(),
        ),
    )
    .expect_err("compiled DOP is a frozen profile fact");
    assert!(error.to_string().contains("pipeline DOP"), "{error}");

    // An exchange assignment for a receiver the program does not have is
    // refused at registration, and nothing stays registered.
    let stray = ExchangeInputAssignments::new(std::collections::BTreeMap::from([(
        crate::exec::fragment::program::FragmentNodeId::new(7),
        ExchangeInputAssignment::new(NonZeroUsize::new(1).unwrap()),
    )]));
    let submission = CompiledTaskSubmission::try_new(
        program,
        CompiledScanSources::new(),
        instance(UniqueId::new(205, 206), 1, stray),
    )
    .expect("submission facts are consistent");
    let error = refused(prepare_compiled_task(
        submission,
        LocalTaskPrepareContext::default(),
    ));
    assert!(
        error
            .to_string()
            .contains("has no compiled exchange source"),
        "{error}"
    );
}

#[test]
fn a_host_root_sink_width_must_equal_the_compiled_width() {
    let program = program(SeedMode::Input, false);
    let submission = CompiledTaskSubmission::try_new(
        program,
        CompiledScanSources::new(),
        instance(
            UniqueId::new(207, 208),
            1,
            ExchangeInputAssignments::default(),
        ),
    )
    .expect("compiled submission");
    let context = LocalTaskPrepareContext {
        root_sink_dop: Some(4),
        ..LocalTaskPrepareContext::default()
    };
    let error = refused(prepare_compiled_task(submission, context));
    assert!(error.to_string().contains("root sink width"), "{error}");
}

fn scan_instance(task_key: UniqueId, assignments: ScanAssignments) -> TaskInstanceSpec {
    TaskInstanceSpec::new_native(
        FragmentContractVersion::CURRENT,
        QueryId::new(task_key.high() - 2, task_key.low() - 2),
        TaskKey::new(task_key),
        assignments,
        ExchangeInputAssignments::default(),
        TaskSinkAssignment::None,
        TaskRuntimeOptions::new(QueryOptions::default(), false),
        NonZeroUsize::new(1).expect("nonzero DOP"),
        InstanceOrdinal::try_new(1).expect("backend number"),
    )
}

fn scan_assignments(nodes: &[i32], ranges: BoundScanRanges) -> ScanAssignments {
    ScanAssignments::try_new(
        nodes
            .iter()
            .map(|node| (FragmentNodeId::new(*node), ranges.clone()))
            .collect(),
    )
    .expect("scan assignments")
}

fn scan_sources(program: &LocalProgram, op: &Arc<FixtureScanOp>) -> CompiledScanSources {
    let scan = *program
        .scan_inputs()
        .keys()
        .next()
        .expect("the fixture has one scan");
    BTreeMap::from([(
        scan,
        Arc::new(FixtureScanSource(Arc::clone(op))) as Arc<dyn ScanSource>,
    )])
}

fn submission_refusal(
    program: Arc<LocalProgram>,
    scans: CompiledScanSources,
    instance: TaskInstanceSpec,
) -> String {
    CompiledTaskSubmission::try_new(program, scans, instance)
        .expect_err("the compiled submission must be refused")
        .to_string()
}

// The Task's scan source is bound to the instance's empty range binding of
// the physical scan node, and its rows reach the opened result session
// through the compiled residual and projection.
#[test]
fn a_compiled_scan_submission_binds_its_task_source_and_runs_into_the_result_session() {
    let program = scan_program(1, true);
    let op = FixtureScanOp::new(
        vec![
            scan_chunk(&program, &[1, 8], &[10, 80]),
            scan_chunk(&program, &[20], &[200]),
        ],
        false,
    );
    let submission = CompiledTaskSubmission::try_new(
        Arc::clone(&program),
        scan_sources(&program, &op),
        scan_instance(
            UniqueId::new(211, 212),
            scan_assignments(&[SCAN_NODE], BoundScanRanges::None),
        ),
    )
    .expect("compiled scan submission");
    assert_eq!(submission.sink_kind(), FragmentSinkKind::Result);
    let session = Arc::new(CollectingSession::default());
    let context = LocalTaskPrepareContext {
        result_writer: Arc::new(CollectingWriter {
            session: Arc::clone(&session),
        }),
        ..LocalTaskPrepareContext::default()
    };
    let handle = prepare_compiled_task(submission, context).expect("compiled prepare");
    assert!(matches!(
        handle.start().join().outcome(),
        LocalTaskOutcome::Succeeded
    ));
    assert_eq!(
        rows(&session.chunks.lock().unwrap()),
        vec![(80, 8), (200, 20)]
    );
    assert_eq!(op.claims(), 1, "one driver owns the scan stream");
}

#[test]
fn a_compiled_scan_submission_refuses_unbound_and_unaddressed_scans() {
    let scanned = scan_program(1, true);
    let op = FixtureScanOp::new(Vec::new(), false);
    let assigned = || scan_assignments(&[SCAN_NODE], BoundScanRanges::None);

    let unbound = submission_refusal(
        Arc::clone(&scanned),
        CompiledScanSources::new(),
        scan_instance(UniqueId::new(213, 214), assigned()),
    );
    assert!(unbound.contains("has no Task scan source"), "{unbound}");

    let unassigned = submission_refusal(
        Arc::clone(&scanned),
        scan_sources(&scanned, &op),
        scan_instance(UniqueId::new(215, 216), ScanAssignments::default()),
    );
    assert!(
        unassigned.contains("has no instance scan assignment"),
        "{unassigned}"
    );

    let stray = submission_refusal(
        Arc::clone(&scanned),
        scan_sources(&scanned, &op),
        scan_instance(
            UniqueId::new(217, 218),
            scan_assignments(&[SCAN_NODE, SCAN_NODE + 2], BoundScanRanges::None),
        ),
    );
    assert!(stray.contains("names no compiled scan"), "{stray}");

    // A program without a scan takes no scan source.
    let scanless = submission_refusal(
        program(SeedMode::Input, false),
        scan_sources(&scanned, &op),
        scan_instance(UniqueId::new(219, 220), ScanAssignments::default()),
    );
    assert!(
        scanless.contains("which is not a compiled scan"),
        "{scanless}"
    );

    // The source checks its range variant when preparation binds it, and the
    // refusal rolls back everything acquired before it.
    let submission = CompiledTaskSubmission::try_new(
        Arc::clone(&scanned),
        scan_sources(&scanned, &op),
        scan_instance(
            UniqueId::new(221, 222),
            scan_assignments(
                &[SCAN_NODE],
                BoundScanRanges::SchemaSelection { should_scan: true },
            ),
        ),
    )
    .expect("the range variant is the source's to check");
    let error = refused(prepare_compiled_task(
        submission,
        LocalTaskPrepareContext::default(),
    ));
    assert_eq!(error.stage(), LocalTaskLaunchStage::Materialize);
    assert_eq!(error.kind(), LocalTaskLaunchErrorKind::Materialization);
    assert_eq!(
        novarocks_failure_contract::FailureProjection::failure_fact(error.cause()).class(),
        novarocks_failure_contract::FailureClass::Invariant
    );
    assert!(
        error
            .detail()
            .contains("fixture scan expects no frozen range"),
        "{error}"
    );
    assert_eq!(op.claims(), 0, "no refused submission claims the stream");
}

struct RecordingProjectSchemaHost<'a> {
    program: &'a Arc<LocalProgram>,
    sites: Vec<crate::runtime::preparation_metadata::CompiledSchemaSite>,
    body_calls: usize,
    refusal: Option<(
        usize,
        crate::runtime::preparation_metadata::PreparationMetadataFailure,
    )>,
}
impl crate::runtime::preparation_metadata::CompiledSchemaPreparationScope
    for RecordingProjectSchemaHost<'_>
{
    fn prepare(
        &mut self,
        program: &Arc<LocalProgram>,
        site: crate::runtime::preparation_metadata::CompiledSchemaSite,
        layout: &novarocks_local_program::StaticLayout,
        body: &mut dyn FnMut() -> ExecutionResult<()>,
    ) -> ExecutionResult<()> {
        use crate::runtime::preparation_metadata::CompiledSchemaSite;
        assert!(Arc::ptr_eq(program, self.program));
        let node = match site {
            CompiledSchemaSite::Project(id) => {
                let node = &program.graph().nodes()[id.index()];
                assert!(matches!(
                    node.kind(),
                    novarocks_local_program::ProgramNodeKind::Project { .. }
                ));
                node
            }
            CompiledSchemaSite::FinalResult => {
                &program.graph().nodes()[program.graph().root().index()]
            }
            CompiledSchemaSite::AnalyticOutput(id) => {
                let node = &program.graph().nodes()[id.index()];
                assert!(matches!(
                    node.kind(),
                    novarocks_local_program::ProgramNodeKind::Analytic { .. }
                ));
                node
            }
        };
        assert!(std::ptr::eq(layout, node.output_layout()));
        self.sites.push(site);
        if let Some((at, cause)) = &self.refusal
            && self.sites.len() == *at
        {
            return Err(cause.clone().into());
        }
        self.body_calls += 1;
        body()
    }
}

#[test]
fn project_metadata_borrow_reaches_nested_projects_before_running() {
    use crate::runtime::preparation_metadata::CompiledSchemaSite;
    let program = program(SeedMode::Input, false);
    let submission = CompiledTaskSubmission::try_new(
        Arc::clone(&program),
        CompiledScanSources::new(),
        instance(
            UniqueId::new(231, 232),
            1,
            ExchangeInputAssignments::default(),
        ),
    )
    .unwrap();
    let session = Arc::new(CollectingSession::default());
    let context = LocalTaskPrepareContext {
        result_writer: Arc::new(CollectingWriter {
            session: Arc::clone(&session),
        }),
        ..LocalTaskPrepareContext::default()
    };
    let mut host = RecordingProjectSchemaHost {
        program: &program,
        sites: Vec::new(),
        body_calls: 0,
        refusal: None,
    };
    let handle = prepare_compiled_task_with_metadata_host(submission, context, &mut host).unwrap();
    let expected: Vec<_> = program
        .graph()
        .nodes()
        .iter()
        .enumerate()
        .filter(|(_, node)| {
            matches!(
                node.kind(),
                novarocks_local_program::ProgramNodeKind::Project { .. }
            )
        })
        .map(|(index, _)| {
            CompiledSchemaSite::Project(novarocks_local_program::ProgramNodeId::new(index))
        })
        .collect();
    assert_eq!(host.sites.len(), expected.len());
    for site in expected {
        assert_eq!(
            host.sites.iter().filter(|&&actual| actual == site).count(),
            1
        );
    }
    assert_eq!(host.body_calls, host.sites.len());
    assert!(session.chunks.lock().unwrap().is_empty());
    // A caller-local host can be destroyed before any driver is scheduled.
    drop(host);
    assert!(matches!(
        handle.start().join().outcome(),
        LocalTaskOutcome::Succeeded
    ));
    let chunks = session.chunks.lock().unwrap();
    assert_eq!(chunks.iter().map(Chunk::len).sum::<usize>(), 1);
    let batch = chunks
        .iter()
        .find(|chunk| chunk.len() == 1)
        .unwrap()
        .record_batch();
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0)
            .to_bits(),
        SEED_42_FIRST
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );
}

#[test]
fn project_metadata_refusal_keeps_nominal_cause_through_the_original_prepare_chain() {
    use crate::runtime::{
        preparation_memory::{PreparationMemoryRefusal, PreparationMemoryStop},
        preparation_metadata::PreparationMetadataFailure,
    };
    use novarocks_execution_contract::task_execution::status::AbortCause;
    let causes = [
        PreparationMetadataFailure::Host(PreparationMemoryRefusal::Interrupted(
            crate::runtime::task::ExecutionFailure::from_domain(PreparationMemoryStop::Abort(
                AbortCause::LeaseExpired,
            )),
        )),
        PreparationMetadataFailure::Host(PreparationMemoryRefusal::Capacity(
            novarocks_memory::CapacityError::Invalid {
                detail: "original host capacity refusal",
            },
        )),
        PreparationMetadataFailure::Request(
            novarocks_type_contract::MetadataRequestError::SourceModel("original source refusal"),
        ),
    ];
    for cause in causes {
        for at in [1, 2] {
            let program = program(SeedMode::Input, false);
            let submission = CompiledTaskSubmission::try_new(
                Arc::clone(&program),
                CompiledScanSources::new(),
                instance(
                    UniqueId::new(233, 234),
                    1,
                    ExchangeInputAssignments::default(),
                ),
            )
            .unwrap();
            let session = Arc::new(CollectingSession::default());
            let context = LocalTaskPrepareContext {
                result_writer: Arc::new(CollectingWriter {
                    session: Arc::clone(&session),
                }),
                ..LocalTaskPrepareContext::default()
            };
            let mut host = RecordingProjectSchemaHost {
                program: &program,
                sites: Vec::new(),
                body_calls: 0,
                refusal: Some((at, cause.clone())),
            };
            let error = refused(prepare_compiled_task_with_metadata_host(
                submission, context, &mut host,
            ));
            assert_eq!(error.stage(), LocalTaskLaunchStage::BuildPipelines);
            assert_eq!(error.kind(), LocalTaskLaunchErrorKind::PipelineBuild);
            assert_eq!(
                error.cause().cause(),
                &ExecutionFailureCause::PreparationMetadata(cause.clone())
            );
            assert_eq!(host.sites.len(), at);
            assert_eq!(host.body_calls, at - 1);
            assert!(session.chunks.lock().unwrap().is_empty());
        }
    }
}

#[test]
fn project_metadata_borrow_reaches_the_actual_final_result_boundary() {
    use crate::runtime::preparation_metadata::{CompiledSchemaSite, PreparationMetadataFailure};
    for project in [None, Some(false), Some(true)] {
        for refuse_final in [false, true] {
            let program = crate::exec::pipeline::builder::compiled_root_result_fixture(project);
            let submission = CompiledTaskSubmission::try_new(
                Arc::clone(&program),
                CompiledScanSources::new(),
                instance(
                    UniqueId::new(235, 236),
                    1,
                    ExchangeInputAssignments::default(),
                ),
            )
            .unwrap();
            let context = super::super::compiled_root_result_context_fixture();
            let cause = PreparationMetadataFailure::Request(
                novarocks_type_contract::MetadataRequestError::Arithmetic,
            );
            let expected = 1 + usize::from(project.is_some());
            let mut host = RecordingProjectSchemaHost {
                program: &program,
                sites: Vec::new(),
                body_calls: 0,
                refusal: refuse_final.then(|| (expected, cause.clone())),
            };
            let result = prepare_compiled_task_with_metadata_host(submission, context, &mut host);
            assert_eq!(host.sites.len(), expected);
            assert_eq!(host.sites.last(), Some(&CompiledSchemaSite::FinalResult));
            assert_eq!(host.body_calls, expected - usize::from(refuse_final));
            if refuse_final {
                assert_eq!(
                    refused(result).cause().cause(),
                    &ExecutionFailureCause::PreparationMetadata(cause)
                );
            } else {
                // Original RootResult production is already covered by its own
                // oracle; this probe ends before scheduling its producer.
                let dormant = result.unwrap();
                drop(host);
                drop(dormant);
            }
        }
    }
}
