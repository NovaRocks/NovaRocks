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

//! One real compiled CountOnly Root Task on the same original Node lifecycle. The host
//! replaces only codec/connector I/O; Execution owns its real kernel lifecycle.

mod activation_contract;
mod completion_contracts;
mod late_update_contract;

use super::node_flow::{
    HeldTaskDelivery, NodeClose, Scenario, operation_number, retain_failure,
    run_node_lifecycle_with_task, wait_delivery, wait_until,
};
use super::*;
use crate::{
    PreparedTaskInstallation, RunnableTask, TaskExecutionHost, TaskPreparationCapture,
    TaskPreparationContext, TaskStatusReporter, TaskTerminalAction,
    control_runtime::{
        host_executor::{HostExecutor, HostExecutorLimits},
        loading::TaskCreatePayload,
        loading_host_job::LoadingHostJob,
        node::WorkerNodeHost,
        participation_host::{TaskIoPorts, task_io::TaskIoHostJob},
    },
};
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_execution::runtime::task::scan::CompiledScanSources;
use novarocks_execution::{
    exec::fragment::program::FragmentContractVersion,
    runtime::{
        execution_runtime::{ExecutionRuntime, ExecutionRuntimeConfig},
        function_set::ExecutionFunctionSetBuilder,
        query_options::QueryOptions,
        task::{
            CompiledTaskSubmission, ExchangeInputAssignments, ExecutionResult, InstanceOrdinal,
            LocalTaskOutcome, LocalTaskPrepareContext, PreparedTaskExecution, RunningTaskExecution,
            ScanAssignments, TaskCancelDetail, TaskInstanceSpec, TaskKey, TaskRuntimeOptions,
            TaskSinkAssignment,
            io::{
                ExchangeFrameTransmitter, NoopTaskEventSink, ResultWriteSpec, TabletCommitInfo,
                TabletFailInfo, TaskCommitPort, TaskCommitRegistration, TaskCommitReport,
                TaskIoError, TaskResultSession, TaskResultWriter, TaskSinkLoadStats,
            },
            prepare_compiled_task,
        },
    },
};
use novarocks_execution_contract::{
    AbortCause, CancelReason, CreateTask, CreationContent, ExchangeTopology, FragmentSinkKind,
    FrozenBytes, PreparedTaskFacts, TaskCreationInput, TaskDescriptor, TaskDomainUpdate,
    TaskIdentity, TaskOutputFacts, TaskState,
};
use novarocks_local_compiler::{
    LocalCompileOptions, compile_fragment, validate_fragment_providers,
};
use novarocks_local_program::{KernelAbiVersion, LocalProgram};
use novarocks_physical_plan::{
    ConstantPools, ExprId, FragmentBuilder, FragmentCuts, FragmentId, FragmentPackage,
    FragmentPackageAdmission, FragmentPackageInput, FragmentSink, FrozenFragmentCalls,
    FrozenFragmentPruning, NodeId, PhysicalRootUses, PipelineDopDomain, PlanLimits, PlanVersionId,
    PropertyProofProjectionLimits, RequiredContracts, ResultPort, StagePlacementPolicy,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, EvaluationDomainId, ExpressionControlFlow,
    ExpressionEvaluationDomain, PureCompileControl, SemanticParameters,
};
use novarocks_types::{StageId, TaskId, UniqueId};
use novarocks_worker_protocol::participation::{ParticipationCommand, ParticipationReplyBody};
use std::{any::Any, collections::BTreeMap, num::NonZeroU64, sync::Weak};

type Failure = Box<dyn Any + Send>;

// This component owns one real ingress position. Native permit geometry is
// verified separately at the actual adapter boundary.
struct ComponentIngressBacking {
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl crate::WorkerIngressBacking for ComponentIngressBacking {}
#[derive(Debug)]
struct UnexpectedInbound;
impl std::fmt::Display for UnexpectedInbound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the compiled CountOnly fixture has no inbound stream or payment")
    }
}
impl std::error::Error for UnexpectedInbound {}
impl novarocks_execution_contract::failure::FailureProjection for UnexpectedInbound {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        novarocks_execution_contract::failure::FailureFact::new(
            novarocks_execution_contract::failure::FailureClass::Invariant,
            crate::failure_reason::WorkerFailureReason::InboundCloseContract.reason(),
        )
    }
}
struct CompileControl;
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn function_metadata() -> novarocks_functions::FunctionDefinition {
    novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog()
        .unwrap()
        .definition("rand", novarocks_functions::FunctionKind::Scalar)
        .unwrap()
        .clone()
}
fn compiled_count_root() -> Arc<LocalProgram> {
    let mut builder = FragmentBuilder::new(FragmentId::new(1));
    builder
        .add_values(
            NodeId::new(1),
            Box::from([Box::<[ExprId]>::default()]),
            Box::default(),
        )
        .unwrap();
    let fragment = builder
        .finish_definition(
            NodeId::new(1),
            FragmentSink::RootResult(Box::new(root_contract())),
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let flow = ExpressionControlFlow::<ExprId>::try_new(
        vec![ExpressionEvaluationDomain {
            id: EvaluationDomainId::new(1),
            parent: None,
            guard: None,
        }],
        vec![],
        fragment.expressions(),
        CompilePhase::Validate,
        &CompileControl,
    )
    .unwrap();
    let uses = PhysicalRootUses::try_new(&fragment, flow, vec![], &CompileControl).unwrap();
    let calls = FrozenFragmentCalls::try_new(&fragment, &uses, vec![], &CompileControl).unwrap();
    let pruning = FrozenFragmentPruning::try_new(fragment.id(), vec![], &CompileControl).unwrap();
    let result = ResultPort {
        scalar_schema: None,
        fragment: fragment.id(),
        output: fragment.nodes()[&fragment.root()].output.clone(),
        fields: Box::default(),
    };
    let package = Arc::new(
        FragmentPackage::try_new(
            FragmentPackageInput {
                version: PlanVersionId::try_new([1; 16]).unwrap(),
                required: RequiredContracts::default(),
                constants: ConstantPools::empty(),
                fragment,
                expression_uses: uses,
                calls,
                cuts: FragmentCuts::default(),
                result: Some(result),
                parameters: SemanticParameters::try_new([]).unwrap(),
                scans: BTreeMap::new(),
                writes: BTreeMap::new(),
                annotations: Box::default(),
                pruning,
            },
            FragmentPackageAdmission {
                plan_limits: PlanLimits::FROZEN,
                source_retained_bytes: 4096,
                property_projection_limits: PropertyProofProjectionLimits {
                    max_request_bytes: 4096,
                    max_coexisting_bytes: 8192,
                    max_projection_work: 4096,
                },
            },
            &CompileControl,
        )
        .unwrap(),
    );
    // The real catalogue forbids an empty closure. Use its actual RAND-only
    // metadata/installed ABI subset for both compiler and execution, although
    // this Noop program itself has no function calls.
    let mut functions = novarocks_functions::EngineFunctionCatalogBuilder::new();
    functions.register(function_metadata()).unwrap();
    let catalog = functions
        .seal_pure(
            [
                "builtin.scalar/rand/()->f64;strict;legacy",
                "builtin.scalar/rand/(i64)->f64;strict;legacy",
            ]
            .into_iter()
            .map(|overload| novarocks_functions::InstalledPureKernel {
                function: novarocks_functions::FunctionId::try_new("builtin.scalar/rand/v1")
                    .unwrap(),
                kind: novarocks_functions::FunctionKind::Scalar,
                implementation: novarocks_functions::PureImplementationDeclaration {
                    overload: novarocks_functions::FunctionOverloadId::try_new(overload).unwrap(),
                    implementation: novarocks_functions::PureImplementationId::try_new(
                        "builtin.scalar/rand/selected-v1",
                    )
                    .unwrap(),
                    abi: novarocks_functions::PureKernelAbi::ScalarV1,
                },
                aggregate_state_format: None,
            }),
        )
        .unwrap();
    let providers =
        PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &CompileControl)
            .unwrap();
    let validated = validate_fragment_providers(package, &providers, &CompileControl).unwrap();
    Arc::new(
        compile_fragment(
            validated,
            &catalog,
            LocalCompileOptions {
                pipeline_dop: NonZeroUsize::new(1).unwrap(),
                root_sink_dop: NonZeroUsize::new(1),
                kernel_abi: KernelAbiVersion::CURRENT,
                constants: novarocks_functions::ConstantPolicy {
                    max_rows: 1,
                    max_array_nodes: 1,
                    max_logical_elements: 1,
                    max_retained_buffer_bytes: 4096,
                    max_type_depth: 1,
                    max_type_nodes: 1,
                    max_dictionary_depth: 1,
                    max_metadata_bytes: 4096,
                    max_library_validation_work: 4096,
                    max_library_validation_bytes: 4096,
                },
            },
            &CompileControl,
        )
        .unwrap(),
    )
}
fn execution_runtime() -> Arc<ExecutionRuntime> {
    let bound = 64 * 1024 * 1024;
    let memory = Arc::new(
        novarocks_memory::MemoryAuthority::new(novarocks_memory::AuthorityConfig::new(
            bound,
            bound * 3 / 4,
            bound / 4,
        ))
        .unwrap(),
    );
    let mut functions = ExecutionFunctionSetBuilder::new();
    functions
        .catalog_builder_mut()
        .register(function_metadata())
        .unwrap();
    Arc::new(
        ExecutionRuntime::new(
            ExecutionRuntimeConfig {
                driver_threads: 1,
                exchange_io_max_inflight_bytes: 1024,
                exchange_max_transmit_batched_bytes: 1024,
                operator_buffer_chunks: 1,
                local_exchange_buffer_mem_limit_per_driver: 1024,
                local_exchange_max_buffered_rows: 1,
                runtime_filter_scan_wait_time_ms_override: None,
                runtime_filter_wait_timeout_ms_override: None,
                sink_io_worker_threads: 1,
                sink_io_max_blocking_threads: 1,
            },
            Arc::new(functions.seal().unwrap()),
            memory,
        )
        .unwrap(),
    )
}
fn root_contract() -> novarocks_result_contract::RootOutputContract {
    novarocks_result_contract::RootOutputContract::new(
        novarocks_result_contract::RootProfileId::V1,
        novarocks_result_contract::FrozenRootOutput::CountOnly,
    )
}
use crate::root_result_channel::{RootProducerExit, RootResultChannel};
use novarocks_execution::runtime::{
    observable::Observable,
    task::io::{
        ResultAbort, ResultWriteAdmission, RootInputAdmission, RootInputAuthority,
        RootInputReservationAdmission, RootProducerState, RootResultInput, RootResultSession,
        RootResultWriteSpec, RootWorkExitProgress, TaskIoErrorKind, TaskIoOperation,
    },
};
use std::task::Waker;
struct CountSession {
    channel: Arc<RootResultChannel>,
    authority: RootInputAuthority,
    encoder: Mutex<Option<RootProducerExit>>,
    exited: Arc<Observable>,
    _metadata: Arc<crate::root_result_channel::RootMetadataReservation>,
    observed: Arc<Observations>,
}
fn root_io(error: crate::root_result_channel::RootChannelError) -> TaskIoError {
    TaskIoError::from_domain(
        TaskIoOperation::ResultWrite,
        TaskIoErrorKind::Internal,
        error,
    )
}
impl CountSession {
    fn open(
        channel: Arc<RootResultChannel>,
        health: Arc<novarocks_execution::runtime::execution_health::ExecutionHealth>,
        observed: Arc<Observations>,
    ) -> Arc<Self> {
        let arc_header = 2 * std::mem::size_of::<usize>();
        let mut bytes = RootInputAuthority::initial_backing_bytes()
            + std::mem::size_of::<Self>()
            + arc_header
            + std::mem::size_of::<crate::root_result_channel::RootMetadataReservation>()
            + arc_header
            + std::mem::size_of::<Observable>()
            + arc_header
            + Observable::bounded_backing_bytes(1).unwrap();
        #[cfg(target_vendor = "apple")]
        {
            bytes += std::mem::size_of::<(isize, [u8; 56])>();
        }
        let metadata = Arc::new(channel.try_reserve_metadata(bytes).unwrap());
        let authority =
            RootInputAuthority::try_new_retained(channel.spec(), health, metadata.clone()).unwrap();
        let exited = Arc::new(Observable::try_bounded_retained(1, metadata.clone()).unwrap());
        let encoder = channel.start_producer().unwrap();
        Arc::new(Self {
            channel,
            authority,
            encoder: Mutex::new(Some(encoder)),
            exited,
            _metadata: metadata,
            observed,
        })
    }
}
impl RootResultSession for CountSession {
    fn spec(&self) -> &RootResultWriteSpec {
        self.channel.spec()
    }
    fn try_acquire_input(&self) -> Result<RootInputAdmission, TaskIoError> {
        let RootInputReservationAdmission::Granted(mut reservation) =
            self.authority.try_reserve()?
        else {
            return Ok(RootInputAdmission::Blocked);
        };
        let credit = self
            .channel
            .try_reserve(reservation.required_bytes())
            .map_err(root_io)?;
        let ResultWriteAdmission::Granted(credit) = credit else {
            let _actual_reservation_exit = reservation.release_unfunded();
            return Ok(RootInputAdmission::Blocked);
        };
        let mut credit = Some(credit);
        match reservation.try_bind(&mut credit) {
            Ok(admission) => Ok(admission),
            Err(error) => {
                reservation.retire_credit(&mut credit);
                Err(error)
            }
        }
    }
    fn writable_observable(&self) -> Arc<Observable> {
        self.authority.observable()
    }
    fn submit_input(&self, offered: &mut Option<RootResultInput>) -> Result<(), TaskIoError> {
        assert!(
            self.authority
                .owns_original(offered.as_ref().unwrap().permit())
        );
        // CountOnly is synchronous and has no payload encoder job: this actual
        // driver invocation owns and destroys its complete original input.
        let input = offered.take().unwrap();
        let (chunk, mut permit) = input.into_cleanup_parts();
        self.observed
            .input_rows
            .fetch_add(chunk.len(), Ordering::SeqCst);
        self.channel
            .note_rows(chunk.len() as u64)
            .map_err(root_io)?;
        drop(chunk);
        assert_eq!(permit.retire(), RootWorkExitProgress::Exited);
        self.observed.input_submitted.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn finish_input(&self) -> Result<(), TaskIoError> {
        self.channel.request_finish().map_err(root_io)?;
        self.channel.publish_end().map_err(root_io)?;
        self.authority.close();
        let encoder = self
            .encoder
            .lock()
            .unwrap()
            .take()
            .expect("one actual CountOnly encoder return");
        assert!(encoder.finish_encoder().is_none());
        self.observed.input_finished.fetch_add(1, Ordering::SeqCst);
        self.exited.notify_observers();
        Ok(())
    }
    fn producer_state(&self) -> RootProducerState {
        self.channel.producer_state()
    }
    fn producer_exit_observable(&self) -> Arc<Observable> {
        self.exited.clone()
    }
    fn poll_producer_exit(&self, waker: &Waker) -> RootWorkExitProgress {
        if self.encoder.lock().unwrap().is_some() {
            return RootWorkExitProgress::Pending;
        }
        let actual = self.authority.poll_input_exit(waker);
        if actual == RootWorkExitProgress::Exited {
            self.observed
                .producer_exit_observed
                .fetch_add(1, Ordering::SeqCst);
        }
        actual
    }
    fn abort(&self, reason: ResultAbort) {
        {
            let mut original = self.observed.abort.lock().unwrap();
            if original.is_none() {
                *original = Some(reason);
            }
        }
        // This synchronous owner has no queued encoder/input capture. Close
        // its original input authority and channel before reporting the actual
        // encoder return; never overwrite the originating preparation failure.
        self.authority.close();
        self.channel.close(
            novarocks_execution_contract::root_lifetime::RootRetentionClose::ParticipationAborted,
        );
        let encoder = self.encoder.lock().unwrap().take();
        if let Some(encoder) = encoder {
            let _original_failure = encoder.finish_encoder();
        }
        self.exited.notify_observers();
    }
}
struct Ports;
struct NoopTaskCommitRegistration;
impl TaskCommitPort for Ports {
    fn acquire(&self, _: UniqueId) -> ExecutionResult<Box<dyn TaskCommitRegistration>> {
        Ok(Box::new(NoopTaskCommitRegistration))
    }
}
impl TaskCommitRegistration for NoopTaskCommitRegistration {
    fn add_load_stats(&mut self, _: TaskSinkLoadStats) -> ExecutionResult<()> {
        panic!("Noop has no load stats")
    }
    fn add_tablet_commit_info(&mut self, _: TabletCommitInfo) -> ExecutionResult<()> {
        panic!("Noop has no tablet commit")
    }
    fn add_tablet_fail_info(&mut self, _: TabletFailInfo) -> ExecutionResult<()> {
        panic!("Noop has no tablet failure")
    }
    fn finish(&mut self) -> ExecutionResult<TaskCommitReport> {
        Ok(TaskCommitReport::default())
    }
    fn handoff(&mut self) -> ExecutionResult<()> {
        Ok(())
    }
    fn rollback(&mut self) -> ExecutionResult<()> {
        Ok(())
    }
}
// This compiled CountOnly Root has no Exchange input or output. Like
// Execution's result-only fixture, every active Unit operation must fail here
// instead of manufacturing a grant, custody acknowledgement or call exit.
impl ExchangeFrameTransmitter for Ports {
    fn deliver_unit(
        &self,
        _: novarocks_execution::runtime::task::io::exchange_unit::ExchangeUnitFrame,
        _: tokio_util::sync::CancellationToken,
        _: novarocks_execution::runtime::task::io::exchange_unit::ExchangeCallActivity,
        _: novarocks_execution::runtime::task::io::exchange_unit::ExchangeDeliveryCallback,
    ) {
        panic!("CountOnly Root fixture attempted Unit delivery")
    }
    fn register_unit_demands(
        &self,
        _: novarocks_execution_contract::RuntimeEndpoint,
        _: Vec<novarocks_execution::runtime::task::io::exchange_unit::ExchangeDemandEntry>,
        _: tokio_util::sync::CancellationToken,
        _: novarocks_execution::runtime::task::io::exchange_unit::ExchangeCallActivity,
        _: novarocks_execution::runtime::task::io::exchange_unit::ExchangeDemandCallback,
    ) {
        panic!("CountOnly Root fixture attempted demand registration")
    }
    fn watch_unit_facts(
        &self,
        _: novarocks_execution_contract::RuntimeEndpoint,
        _: novarocks_execution_contract::ExchangeStreamIdentity,
        _: novarocks_execution::runtime::task::io::exchange_unit::ExchangeGrantWake,
    ) -> Result<(), novarocks_execution_contract::failure::FailureFact> {
        panic!("CountOnly Root fixture attempted grant subscription")
    }
    fn forget_unit_facts(&self, _: novarocks_execution_contract::ExchangeStreamIdentity) {}
    fn schedule_unit_retry(
        &self,
        _: Duration,
        _: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<(), novarocks_execution_contract::failure::FailureFact> {
        panic!("CountOnly Root fixture attempted Unit retry")
    }
}
impl TaskResultWriter for Ports {
    fn open(&self, _: ResultWriteSpec) -> Result<Arc<dyn TaskResultSession>, TaskIoError> {
        panic!("Noop has no result output")
    }
}
#[derive(Debug)]
struct Assignment;
impl CreationContent for Assignment {
    fn encoded_len(&self) -> usize {
        0
    }
    fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn into_stored(self: Box<Self>) -> Box<dyn Any + Send> {
        self
    }
}
#[derive(Default)]
struct RunnableState {
    dormant: Option<PreparedTaskExecution>,
    running: Option<RunningTaskExecution>,
    start: Option<crate::TaskStartPermit>,
    terminal: Option<crate::TaskTerminalPermit>,
    statistics: Option<crate::TaskStatisticsProducer>,
    dynamic_filters: Option<crate::TaskDynamicFilterProducer>,
    final_statistics: Option<crate::TaskStatisticsFinalizer>,
    completion_exit: Option<crate::CompletionExitNotice>,
    root: Option<Arc<RootResultChannel>>,
    completion: Option<crate::TaskCompletionReservation>,
}
#[derive(Default)]
struct Observations {
    // Same original weak execution view and sealed read binding; no runnable
    // or kernel is kept alive merely for a fast completion assertion.
    reporter: Mutex<Option<TaskStatusReporter>>,
    activated: AtomicUsize,
    completed: AtomicUsize,
    abort: Mutex<Option<ResultAbort>>,
    input_submitted: AtomicUsize,
    input_rows: AtomicUsize,
    input_finished: AtomicUsize,
    producer_exit_observed: AtomicUsize,
    kernel_published: AtomicUsize,
    completion_entered: AtomicUsize,
    created: AtomicUsize,
    protocol_events: Mutex<Vec<crate::TaskProtocolEvent>>,
}
impl crate::TaskProtocolObserver for Observations {
    fn observe(&self, event: crate::TaskProtocolEvent) {
        self.protocol_events.lock().unwrap().push(event);
    }
}
impl crate::TaskExecutionMetrics for Observations {
    fn record_task_created(&self) {
        self.created.fetch_add(1, Ordering::SeqCst);
    }
}
impl crate::TaskResultLifecycle for Observations {
    fn discard_task(&self, identity: TaskIdentity) {
        crate::result_buffer::discard_task(identity);
    }
    fn retire_task_result(&self, identity: TaskIdentity) {
        crate::result_buffer::retire_task_result(identity);
    }
}
struct Runnable {
    identity: TaskIdentity,
    state: Mutex<RunnableState>,
    observed: Arc<Observations>,
}
impl std::fmt::Debug for Runnable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("OriginalCompiledRunnable")
            .field(&self.identity)
            .finish()
    }
}
// This gate holds a real physical callback/job. Releasing it supplies no exit
// fact: the original body/child still has to return through its normal owner.
#[derive(Debug, Default)]
struct PreparationGate {
    entered: std::sync::atomic::AtomicBool,
    released: std::sync::atomic::AtomicBool,
    mutex: Mutex<()>,
    changed: Condvar,
}
impl PreparationGate {
    fn wait(&self) {
        let mut guard = self.mutex.lock().unwrap();
        self.entered.store(true, Ordering::Release);
        self.changed.notify_all();
        while !self.released.load(Ordering::Acquire) {
            guard = self.changed.wait(guard).unwrap();
        }
    }
    fn release(&self) {
        let guard = self.mutex.lock().unwrap();
        self.released.store(true, Ordering::Release);
        drop(guard);
        self.changed.notify_all();
    }
    fn entered(&self) -> bool {
        self.entered.load(Ordering::Acquire)
    }
}
struct ReleasePreparationGate(Arc<PreparationGate>);
impl Drop for ReleasePreparationGate {
    fn drop(&mut self) {
        self.0.release();
    }
}
struct BundleFixture {
    stopped_before_host: bool,
    prepare: Arc<PreparationGate>,
    adoption: Arc<PreparationGate>,
    original: Mutex<Option<(Weak<WorkerNodeHost>, ParticipationRef)>>,
    host_entries: AtomicUsize,
    whole_refusals: AtomicUsize,
    transfers: AtomicUsize,
    second_take_refusals: AtomicUsize,
    activation_window: Mutex<Option<Arc<activation_contract::InstalledBeforeBody>>>,
}
impl BundleFixture {
    fn new(stopped_before_host: bool) -> Self {
        Self {
            stopped_before_host,
            prepare: Arc::new(PreparationGate::default()),
            adoption: Arc::new(PreparationGate::default()),
            original: Mutex::new(None),
            host_entries: AtomicUsize::new(0),
            whole_refusals: AtomicUsize::new(0),
            transfers: AtomicUsize::new(0),
            second_take_refusals: AtomicUsize::new(0),
            activation_window: Mutex::new(None),
        }
    }
    fn inventory(
        &self,
        task: TaskIdentity,
    ) -> crate::control_runtime::node::PreparationFixtureInventory {
        let (node, participation) = self
            .original
            .lock()
            .unwrap()
            .clone()
            .expect("same Node fixture binding");
        node.upgrade()
            .expect("actual Node still owns this preparation")
            .preparation_fixture_inventory(participation, task)
            .expect("same original preparation inventory")
    }
    fn take_original_capture(
        &self,
        preparation: &mut TaskPreparationContext<'_>,
        health: Arc<novarocks_execution::runtime::execution_health::ExecutionHealth>,
    ) -> TaskPreparationCapture {
        self.host_entries.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.stopped_before_host,
            "the stopped preparation must not enter the host"
        );
        let identity = preparation.view().identity();
        let children = preparation.original_children_for_test().clone();
        let activation_window = self.activation_window.lock().unwrap().clone();
        if let Some(window) = activation_window {
            window.arm(preparation);
        }
        let (owner, cadence) = preparation.view().control();
        let owner = owner.expect("same original execution port");
        let waker = preparation.readiness_waker();
        let roles = preparation.take_capture().unwrap();
        let before = self.inventory(identity);
        let wrong_identity = TaskIdentity::new(
            identity.query_execution_id(),
            identity.stage_id(),
            TaskId::new(999).unwrap(),
            identity.backend_process_id(),
        );
        let wrong = Arc::new(crate::task_preparation::TaskPreparationScope::new(
            wrong_identity,
            health,
        ));
        let (_, returned) = match TaskPreparationContext::from_admitted(
            owner.clone(),
            &wrong,
            cadence,
            roles,
            waker.clone(),
        ) {
            Err(original) => original,
            Ok(_) => panic!("wrong child must return the whole original bundle"),
        };
        assert!(returned.matches(identity));
        assert_eq!(
            self.inventory(identity),
            before,
            "wrong-child refusal cannot change original role IDs, inventory or payment"
        );
        self.whole_refusals.fetch_add(1, Ordering::SeqCst);
        let mut rebound =
            TaskPreparationContext::from_admitted(owner, &children, cadence, returned, waker)
                .unwrap_or_else(|_| {
                    panic!("the returned whole bundle binds to its original children")
                });
        assert_eq!(
            self.inventory(identity),
            before,
            "rebinding moves originals without reserving replacement roles"
        );
        let original = rebound.take_capture().unwrap();
        assert!(original.matches(identity));
        assert!(matches!(
            rebound.take_capture(),
            Err(crate::TaskPreparationCaptureTaken)
        ));
        assert!(matches!(
            preparation.take_capture(),
            Err(crate::TaskPreparationCaptureTaken)
        ));
        assert_eq!(
            self.inventory(identity),
            before,
            "a repeated take cannot issue another original role"
        );
        self.transfers.fetch_add(1, Ordering::SeqCst);
        self.second_take_refusals.fetch_add(1, Ordering::SeqCst);
        original
    }
}
struct PreparedChild {
    dormant: Mutex<Option<PreparedTaskExecution>>,
    target: Mutex<Weak<Runnable>>,
    root_exit: Mutex<Option<crate::root_result_channel::RootPhysicalExitObservation>>,
    rollback_requested: std::sync::atomic::AtomicBool,
    rolling_back: std::sync::atomic::AtomicBool,
    adoption_gate: Option<Arc<PreparationGate>>,
}
impl crate::TaskPreparationChild for PreparedChild {
    fn request_rollback(&self) {
        self.rollback_requested.store(true, Ordering::Release);
        if self.rolling_back.swap(true, Ordering::AcqRel) {
            return;
        }
        let dormant = self.dormant.lock().unwrap().take();
        // Dormant FragmentResources performs its original preparation rollback.
        // Foreign resource destructors run outside this child metadata lock.
        drop(dormant);
        self.rolling_back.store(false, Ordering::Release);
    }
    fn poll_exit(&self) -> crate::TaskPreparationChildExit {
        if self.rolling_back.load(Ordering::Acquire) {
            return crate::TaskPreparationChildExit::Pending;
        }
        if !self.rollback_requested.load(Ordering::Acquire)
            || self.dormant.lock().unwrap().is_some()
        {
            return crate::TaskPreparationChildExit::Pending;
        }
        let original = self.root_exit.lock().unwrap();
        match original.as_ref() {
            Some(original) if original.output_exited() => crate::TaskPreparationChildExit::Exited {
                output: crate::TaskPreparationOutputExit::ActualOutputExited,
            },
            Some(_) => crate::TaskPreparationChildExit::Pending,
            // Construction did not register an actual Root observation. A
            // failed constructor is Unknown, never proof that no output existed.
            None => crate::TaskPreparationChildExit::Unresolved,
        }
    }
    fn adopt_runtime(&self) {
        if let Some(gate) = &self.adoption_gate {
            gate.wait();
        }
        let dormant = self
            .dormant
            .lock()
            .unwrap()
            .take()
            .expect("one original dormant adoption");
        let target = self
            .target
            .lock()
            .unwrap()
            .upgrade()
            .expect("the actual host retains its dormant runnable");
        let mut state = target.state.lock().unwrap();
        assert!(state.dormant.is_none());
        state.dormant = Some(dormant);
    }
}
impl RunnableTask for Runnable {
    fn commit_creation(&self) {
        let (dormant, start, completion) = {
            let mut state = self.state.lock().unwrap();
            (
                state.dormant.take().expect("actual adopted dormant"),
                state.start.take().expect("original start"),
                state.completion.take().expect("original completion slot"),
            )
        };
        start.authorize_running().unwrap();
        let signal = completion.commit_creation();
        let running = dormant.start();
        let observed = self.observed.clone();
        let mut publisher: Option<
            novarocks_execution::runtime::task::handle::LocalTaskTerminalPublisher,
        > = Some(Box::new(move |fact| {
            observed.kernel_published.fetch_add(1, Ordering::SeqCst);
            signal.publish(fact.clone());
        }));
        self.state.lock().unwrap().running = Some(running.clone());
        self.observed.activated.fetch_add(1, Ordering::SeqCst);
        running.bind_terminal_publisher(&mut publisher).unwrap();
    }
    fn cancel(&self, reason: CancelReason) {
        let running = self.state.lock().unwrap().running.clone();
        if let Some(running) = running {
            running.cancel(TaskCancelDetail::new(format_args!("{reason:?}")));
        }
    }
    fn abort(&self, cause: AbortCause) {
        let running = self.state.lock().unwrap().running.clone();
        if let Some(running) = running {
            running.cancel(TaskCancelDetail::new(format_args!("{cause:?}")));
        }
    }
}
struct CompiledTaskHost {
    program: Arc<LocalProgram>,
    execution: Arc<ExecutionRuntime>,
    completion: Arc<crate::TaskCompletionSupervisor>,
    tasks: Mutex<BTreeMap<TaskIdentity, Arc<Runnable>>>,
    observed: Arc<Observations>,
    result_budget: Arc<crate::result_buffer::ResultRetainedBudget>,
    result_limits: crate::WorkerResultRetainedLimits,
    bundle: Option<Arc<BundleFixture>>,
    completion_contract: Mutex<Option<Arc<completion_contracts::CompletionFixture>>>,
}
impl TaskExecutionHost for CompiledTaskHost {
    fn close_participation_admission(&self, _: ParticipationRef) {}
    fn retire_participation_execution(&self, _: ParticipationRef) {}
    fn forget_participation_admission(&self, _: ParticipationRef) {}
    fn install_receiver(
        &self,
        descriptor: &TaskDescriptor,
        input: TaskCreationInput,
        mut preparation: TaskPreparationContext<'_>,
    ) -> Result<PreparedTaskInstallation, HostRejection> {
        // Validate the whole binding before moving/dropping the actual input;
        // its source still belongs to this original physical preparation.
        let original_roles = match &self.bundle {
            Some(bundle) => {
                bundle.take_original_capture(&mut preparation, self.execution.execution_health())
            }
            None => preparation.take_capture().unwrap(),
        };
        if let Some(bundle) = &self.bundle {
            let activation_window = bundle.activation_window.lock().unwrap().clone();
            if let Some(window) = activation_window {
                window.after_original_roles();
            }
        }
        let (fragment, assignment) = input.into_parts();
        assert_eq!(fragment.bytes().as_ref(), b"root");
        assert!(assignment.into_stored().downcast::<Assignment>().is_ok());
        let control = preparation.runtime_control();
        let TaskPreparationCapture {
            completion_work,
            start,
            terminal,
            statistics,
            dynamic_filters,
            final_statistics,
            completion_exit,
        } = original_roles;
        let child = Arc::new(PreparedChild {
            dormant: Mutex::new(None),
            target: Mutex::new(Weak::new()),
            root_exit: Mutex::new(None),
            rollback_requested: std::sync::atomic::AtomicBool::new(false),
            rolling_back: std::sync::atomic::AtomicBool::new(false),
            adoption_gate: self.bundle.as_ref().map(|bundle| bundle.adoption.clone()),
        });
        // Register the same physical child before opening output. Construction
        // unwind cannot turn a lost, already-born Root into NoOutputCreated.
        preparation.register_child(child.clone()).unwrap();
        let instance = TaskInstanceSpec::new_native(
            FragmentContractVersion::CURRENT,
            descriptor.identity().query_execution_id().query_id(),
            TaskKey::new(descriptor.task_key()),
            ScanAssignments::default(),
            ExchangeInputAssignments::default(),
            TaskSinkAssignment::None,
            TaskRuntimeOptions::new(QueryOptions::default(), false),
            NonZeroUsize::new(1).unwrap(),
            InstanceOrdinal::try_new(1).unwrap(),
        );
        let submission = CompiledTaskSubmission::try_new(
            self.program.clone(),
            CompiledScanSources::new(),
            instance,
        )
        .unwrap();
        let health = self.execution.execution_health();
        let root = RootResultChannel::try_open(
            RootResultWriteSpec {
                task: descriptor.identity(),
                contract: Arc::new(root_contract()),
            },
            self.result_budget.clone(),
            self.result_limits,
            health.clone(),
        )
        .unwrap();
        *child.root_exit.lock().unwrap() = Some(root.physical_exit_observation());
        root.bind_preparation_exit_waker(preparation.readiness_waker())
            .unwrap_or_else(|_| panic!("the original preparation wake binds this Root once"));
        let session = CountSession::open(root.clone(), health, self.observed.clone());
        let participation = LocalTaskPrepareContext::new(
            None,
            None,
            None,
            Arc::new(Ports),
            Arc::new(Ports),
            Arc::new(NoopTaskEventSink),
        )
        .with_root_result_session(session)
        .with_execution_runtime(self.execution.clone())
        .with_task_commit_port(Arc::new(Ports))
        .with_result_identity(descriptor.identity())
        .with_task_control(control)
        .with_completion_work(completion_work);
        let dormant = prepare_compiled_task(submission, participation).unwrap();
        let runnable = Arc::new(Runnable {
            identity: descriptor.identity(),
            state: Mutex::new(RunnableState {
                start: Some(start),
                terminal: Some(terminal),
                statistics: Some(statistics),
                dynamic_filters: Some(dynamic_filters),
                final_statistics: Some(final_statistics),
                completion_exit: Some(completion_exit),
                root: Some(root.clone()),
                ..RunnableState::default()
            }),
            observed: self.observed.clone(),
        });
        *child.dormant.lock().unwrap() = Some(dormant);
        *child.target.lock().unwrap() = Arc::downgrade(&runnable);
        assert!(
            self.tasks
                .lock()
                .unwrap()
                .insert(descriptor.identity(), runnable)
                .is_none()
        );
        PreparedTaskInstallation::new(PreparedTaskFacts::new(FragmentSinkKind::Result), Some(root))
    }
    fn remove_receiver(&self, descriptor: &TaskDescriptor) {
        let original = self.tasks.lock().unwrap().remove(&descriptor.identity());
        drop(original);
    }
    fn retire_receiver_checked(
        &self,
        descriptor: &TaskDescriptor,
        retirement: crate::TaskIngressRetirement,
    ) -> Result<(), HostRejection> {
        if retirement == crate::TaskIngressRetirement::PreserveNormalClose {
            // Zero frozen inbound streams have zero close facts. The actual
            // common validator rejects any nonempty descriptor here.
            crate::TaskInboundCapabilities::validate_normal_close_facts(descriptor, &[])?;
        }
        self.remove_receiver(descriptor);
        Ok(())
    }
    fn install_inbound_capability(&self, descriptor: &TaskDescriptor) -> Result<(), HostRejection> {
        if !descriptor.topology().inbound().is_empty() {
            return Err(HostRejection::from_domain(
                novarocks_execution_contract::TaskFailureCategory::Protocol,
                UnexpectedInbound,
            ));
        }
        Ok(())
    }
    fn remove_inbound_capability(&self, descriptor: &TaskDescriptor) {
        assert!(descriptor.topology().inbound().is_empty());
    }
    fn submit_runnable(
        &self,
        descriptor: &TaskDescriptor,
        reporter: TaskStatusReporter,
    ) -> Result<Arc<dyn RunnableTask>, HostRejection> {
        let runnable = self.tasks.lock().unwrap()[&descriptor.identity()].clone();
        let (terminal, exit, mut final_statistics, root) = {
            let mut state = runnable.state.lock().unwrap();
            *self.observed.reporter.lock().unwrap() = Some(reporter.clone());
            (
                state.terminal.take().unwrap(),
                state.completion_exit.take().unwrap(),
                state.final_statistics.take(),
                state.root.take().expect("original installed Root"),
            )
        };
        let contract = self.completion_contract.lock().unwrap().clone();
        let mut completion = match &contract {
            Some(contract) => contract.reserve(&self.completion, descriptor, reporter, exit),
            None => self
                .completion
                .reserve_task_slot(descriptor.identity(), descriptor.task_key(), reporter, exit)
                .unwrap(),
        };
        let original_capture = contract.as_ref().and_then(|contract| contract.capture());
        let weak = Arc::downgrade(&runnable);
        completion
            .bind_action(TaskTerminalAction::new(
                terminal,
                Box::new(move |fact, observation| {
                    let runnable = weak
                        .upgrade()
                        .expect("original runnable stays owned through actual completion");
                    runnable
                        .observed
                        .completion_entered
                        .fetch_add(1, Ordering::SeqCst);
                    if let Some(capture) = &original_capture {
                        capture.observe_body();
                    }
                    assert!(matches!(fact.outcome(), LocalTaskOutcome::Succeeded));
                    let final_statistics = final_statistics.take().unwrap().seal_for_terminal(None);
                    let (statistics, dynamic_filters, running) = {
                        let mut state = runnable.state.lock().unwrap();
                        (
                            state.statistics.take(),
                            state.dynamic_filters.take(),
                            state.running.take(),
                        )
                    };
                    drop(statistics);
                    drop(dynamic_filters);
                    drop(running);
                    observation
                        .handoff(crate::TaskTerminalObservation {
                            intent: crate::TaskTerminalIntent::SuccessfulProduction {
                                completed_output: Some(TaskOutputFacts::new(matches!(
                                    root.producer_state(),
                                    RootProducerState::ParticipationHeld
                                ))),
                            },
                            normal_close_validated: false,
                            output_released: false,
                            final_statistics,
                        })
                        .unwrap();
                    runnable.observed.completed.fetch_add(1, Ordering::SeqCst);
                }),
            ))
            .unwrap_or_else(|original| {
                retain_failure("original terminal action binding", original)
            });
        runnable.state.lock().unwrap().completion = Some(completion);
        Ok(runnable)
    }
    fn apply_task_domain(
        &self,
        _: &TaskDescriptor,
        _: &TaskDomainUpdate,
        _: TaskOperationId,
    ) -> Result<Option<u64>, HostRejection> {
        panic!("empty initial journal has no host domain operation")
    }
}
fn loading_body(job: LoadingHostJob) {
    job.run_on_host();
}
fn preparation_body(job: TaskIoHostJob) {
    job.run_on_host();
}
type PreparationRunner = Box<dyn Fn(TaskIoHostJob) + Send + Sync>;
pub(super) struct TaskResources {
    host: Arc<CompiledTaskHost>,
    loading: HostExecutor<LoadingHostJob, fn(LoadingHostJob)>,
    preparation: HostExecutor<TaskIoHostJob, PreparationRunner>,
    lifecycle: HostExecutor<TaskIoHostJob, PreparationRunner>,
    _async_runtime: tokio::runtime::Runtime,
}
fn observation_projection() -> crate::ObservationProjection {
    use novarocks_task_codec::operation::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES;
    crate::ObservationProjection {
        fact_bytes: observation_fact_bytes,
        max_frame_bytes: NonZeroUsize::new(NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES).unwrap(),
        // The component uses the same explicit catch-up profile as Native.
        max_initial_bytes: NonZeroUsize::new(64 * 1024 * 1024).unwrap(),
    }
}
fn observation_fact_bytes(
    fact: &novarocks_worker_protocol::observation::CoveredObservationFact,
) -> Result<usize, crate::ObservationProjectionError> {
    use novarocks_task_codec::operation::{
        CoveredStatusStreamEvent, CoveredStatusStreamFact as WireFact,
        covered_quiesce_fact_size_bound, encode_covered_status_event,
    };
    use novarocks_worker_protocol::observation::CoveredObservationFact as Fact;
    use prost::Message;
    let value = match fact {
        Fact::Status(value) => WireFact::Status(value.clone()),
        Fact::StatusUnchanged(value) => WireFact::StatusUnchanged(*value),
        Fact::Unknown(value) => WireFact::Unknown(*value),
        Fact::Gone(value) => WireFact::Gone(*value),
        Fact::TaskConvergence(value) => WireFact::TaskConvergence(*value),
        Fact::TaskConvergenceUnchanged(value) => WireFact::TaskConvergenceUnchanged(*value),
        Fact::ParticipationConvergence(value) => WireFact::ParticipationConvergence(*value),
        Fact::Quiesce(receipt) => {
            return covered_quiesce_fact_size_bound(receipt)
                .ok_or(crate::ObservationProjectionError::InvalidFact);
        }
    };
    encode_covered_status_event(&CoveredStatusStreamEvent {
        fact: value,
        source_revision: Some(u64::MAX),
    })
    .map(|wire| wire.encoded_len())
    .map_err(|_| crate::ObservationProjectionError::InvalidFact)
}
pub(super) fn start_tasks(
    root: &mut ProcessControlOwner,
) -> Result<(TaskIoPorts, TaskResources), Failure> {
    start_tasks_with_bundle(root, None)
}
fn start_tasks_with_bundle(
    root: &mut ProcessControlOwner,
    mode: Option<bool>,
) -> Result<(TaskIoPorts, TaskResources), Failure> {
    start_tasks_with_lifecycle_gate(root, mode, None)
}
fn start_tasks_with_lifecycle_gate(
    root: &mut ProcessControlOwner,
    mode: Option<bool>,
    disposal_gate: Option<Arc<PreparationGate>>,
) -> Result<(TaskIoPorts, TaskResources), Failure> {
    let bundle = mode.map(|stopped| Arc::new(BundleFixture::new(stopped)));
    let capacity = root
        .task_capacity_signal()
        .map_err(|original| Box::new(original) as Failure)?;
    let one = NonZeroUsize::new(1).unwrap();
    // One admitted Task plus one physical tail; two participations are metadata
    // groups, and no pool-size choice signs a Task exit.
    let limits = HostExecutorLimits {
        jobs: NonZeroUsize::new(2).unwrap(),
        per_group: NonZeroUsize::new(2).unwrap(),
        workers: one,
    };
    let loading =
        match HostExecutor::start_with_waker(limits, loading_body as fn(_), capacity.waker()) {
            Ok(owner) => owner,
            Err(original) => return Err(Box::new(original)),
        };
    let original_bundle = bundle.clone();
    let runner: PreparationRunner = Box::new(move |job| {
        if matches!(&job, TaskIoHostJob::Prepare { .. })
            && let Some(bundle) = &original_bundle
        {
            bundle.prepare.wait();
        }
        job.run_on_host();
    });
    let preparation = match HostExecutor::start_with_waker(limits, runner, capacity.waker()) {
        Ok(owner) => owner,
        Err(original) => return Err(Box::new((loading, original))),
    };
    // Cleanup execution is independent of preparation, whose body can wait
    // for its own real cancellation/settlement. More preparation threads do
    // not provide that progress guarantee.
    let lifecycle_body: PreparationRunner = Box::new(move |job| {
        if matches!(&job, TaskIoHostJob::DisposeRejectedDomain(_))
            && let Some(gate) = &disposal_gate
            && !gate.entered()
        {
            // Hold this original paid R's physical disposal before raw Drop.
            // Metadata inspection has already started the same authority.
            gate.wait();
        }
        preparation_body(job);
    });
    let lifecycle = match HostExecutor::start_with_waker(limits, lifecycle_body, capacity.waker()) {
        Ok(owner) => owner,
        Err(original) => return Err(Box::new((loading, preparation, original))),
    };
    let async_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let completion = crate::TaskCompletionSupervisor::start(async_runtime.handle().clone(), 1);
    let geometry =
        novarocks_execution_contract::native_result_support::NativeResultSupportGeometry::V1;
    let result_bytes = usize::try_from(
        geometry.root_fixed_schema_cursor_driver_capacity_bytes
            + geometry.root_original_input_backing_capacity_bytes,
    )
    .unwrap();
    let result_limits =
        crate::WorkerResultRetainedLimits::try_new(result_bytes, result_bytes).unwrap();
    let result_budget =
        crate::result_buffer::ResultRetainedBudget::new(result_limits.per_process());
    let host = Arc::new(CompiledTaskHost {
        program: compiled_count_root(),
        execution: execution_runtime(),
        completion,
        tasks: Mutex::new(BTreeMap::new()),
        observed: Arc::new(Observations::default()),
        result_budget,
        result_limits,
        bundle,
        completion_contract: Mutex::new(None),
    });
    let ports = TaskIoPorts {
        protocol_events: Some(crate::TaskExecutionPorts::new(
            host.observed.clone(),
            host.observed.clone(),
            host.observed.clone(),
            host.execution.execution_health(),
        )),
        observation: Some(observation_projection()),
        execution: Some(host.clone()),
        loading: Some(loading.handle()),
        preparation: Some(preparation.handle()),
        lifecycle: Some(lifecycle.handle()),
        cadence: Duration::from_millis(1),
        capacity: Some(capacity),
    };
    Ok((
        ports,
        TaskResources {
            host,
            loading,
            preparation,
            lifecycle,
            _async_runtime: async_runtime,
        },
    ))
}
fn exercise(
    scene: &Scenario<TaskResources>,
    participation: ParticipationRef,
    resources: &mut TaskResources,
) -> Result<Option<HeldTaskDelivery>, Failure> {
    exercise_root(scene, participation, resources, true)
}
fn exercise_unread_root(
    scene: &Scenario<TaskResources>,
    participation: ParticipationRef,
    resources: &mut TaskResources,
) -> Result<Option<HeldTaskDelivery>, Failure> {
    exercise_root(scene, participation, resources, false)
}
fn trace_original_preparation_stage(
    scene: &Scenario<TaskResources>,
    participation: ParticipationRef,
    task: TaskIdentity,
    bundle: &BundleFixture,
    stage: &'static str,
) {
    eprintln!(
        "original stopped preparation stage={stage} task={task:?} prepare_entered={} prepare_released={} host_entries={} requests={} preparing={} bytes={} inventory={:?}",
        bundle.prepare.entered(),
        bundle.prepare.released.load(Ordering::Acquire),
        bundle.host_entries.load(Ordering::SeqCst),
        scene.ledger.used(Dimension::Requests.id()),
        scene.ledger.used(Dimension::PreparingTasks.id()),
        scene.ledger.used(Dimension::PreparingBytes.id()),
        scene
            .node
            .preparation_fixture_inventory(participation, task),
    );
    eprintln!(
        "original stopped preparation stage={stage} original_request_inventory={:?}",
        scene
            .node
            .original_request_inventory_for_test(participation),
    );
}
fn exercise_root(
    scene: &Scenario<TaskResources>,
    participation: ParticipationRef,
    resources: &mut TaskResources,
    read_root: bool,
) -> Result<Option<HeldTaskDelivery>, Failure> {
    exercise_root_with_preparing_read_hook(scene, participation, resources, read_root, None)
}
type PreparingReadHeldHook = Box<
    dyn FnOnce(
        &Scenario<TaskResources>,
        ParticipationRef,
        &mut TaskResources,
        TaskIdentity,
        novarocks_worker_protocol::creation::RequestContext,
    ) -> Result<(), Failure>,
>;
fn exercise_root_with_preparing_read_hook(
    scene: &Scenario<TaskResources>,
    participation: ParticipationRef,
    resources: &mut TaskResources,
    read_root: bool,
    on_preparing_read_held: Option<PreparingReadHeldHook>,
) -> Result<Option<HeldTaskDelivery>, Failure> {
    let mut preparing_read: Option<HeldTaskDelivery> = None;
    let task = TaskIdentity::new(
        participation.query_execution_id(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        participation.backend_process_id(),
    );
    let _prepare_release = resources
        .host
        .bundle
        .as_ref()
        .map(|bundle| ReleasePreparationGate(bundle.prepare.clone()));
    let _adoption_release = resources
        .host
        .bundle
        .as_ref()
        .map(|bundle| ReleasePreparationGate(bundle.adoption.clone()));
    let _activation_body_release = resources
        .host
        .bundle
        .as_ref()
        .and_then(|bundle| bundle.activation_window.lock().unwrap().clone())
        .map(|window| ReleasePreparationGate(window.body_gate()));
    if let Some(bundle) = &resources.host.bundle {
        *bundle.original.lock().unwrap() = Some((Arc::downgrade(&scene.node), participation));
    }
    *scene.diagnostic_task.lock().unwrap() = Some((participation, task));
    let descriptor = TaskDescriptor::try_new(
        task,
        UniqueId::new(77, 88),
        NonZeroUsize::new(1).unwrap(),
        vec![],
        ExchangeTopology::default(),
    )
    .unwrap();
    let request = Arc::new(
        CreateTask::try_new(operation_number(21), participation, descriptor, vec![]).unwrap(),
    );
    let payload = TaskCreatePayload {
        id: PayloadId(9),
        request,
        input: TaskCreationInput::new(
            FrozenBytes::freeze_encoded(b"root".to_vec()),
            Box::new(Assignment),
        ),
        ingress_retained_bytes: 0,
    };
    let ingress_gate = Arc::new(tokio::sync::Semaphore::new(1));
    let ingress = Arc::new(ComponentIngressBacking {
        _permit: ingress_gate
            .clone()
            .try_acquire_owned()
            .expect("the caller owns one actual component Create ingress position"),
    });
    let handle = scene
        .node
        .submit_task_create(
            ActorId::new(3).unwrap(),
            MonotonicInstant::from_origin(Duration::from_secs(100)),
            payload,
            crate::WorkerIngressResponsibility::from_admitted(ingress.clone()),
        )
        .map_err(|original| Box::new(original) as Failure)?;
    let mut handle = match handle {
        crate::control_runtime::node::NodeCreateHandle::Participation(handle) => handle,
        original => {
            return Err(Box::new((
                "Create unexpectedly routed before Participation adoption",
                original,
            )));
        }
    };
    let Some(delivery) = wait_delivery(&mut handle) else {
        return Err(Box::new(("Create delivery timeout", handle)));
    };
    if !matches!(
        delivery.body(),
        novarocks_worker_protocol::participation::ParticipationReplyBody::Create {
            outcome: OperationOutcome::Accepted, receipt,
        } if receipt.identity() == task
    ) {
        return Err(Box::new(("actual Create refusal", delivery, handle)));
    }
    drop(delivery);
    drop(handle);
    drop(ingress);
    if let Some(bundle) = &resources.host.bundle {
        trace_original_preparation_stage(
            scene,
            participation,
            task,
            bundle,
            "before preparation-entry/Create-R-retirement wait",
        );
        if !wait_until(|| {
            bundle.prepare.entered() && scene.ledger.used(Dimension::Requests.id()) == 0
        }) {
            trace_original_preparation_stage(
                scene,
                participation,
                task,
                bundle,
                "ERR preparation-entry/Create-R-retirement wait",
            );
            return Err(Box::new(
                "original preparation was not handed to its physical runner",
            ));
        }
        let held_bytes = scene.ledger.used(Dimension::PreparingBytes.id());
        let before = bundle.inventory(task);
        assert!(before.preparation_key.is_some());
        assert!(!before.body_exited);
        assert!(!before.whole_report_retired);
        assert_eq!(scene.ledger.used(Dimension::PreparingTasks.id()), 1);
        assert!(held_bytes > 0);
        if on_preparing_read_held.is_some() {
            if bundle.stopped_before_host {
                return Err(Box::new(
                    "the preparing read hook requires the original normal bundle",
                ));
            }
            let operation = operation_number(54);
            let deadline = MonotonicInstant::from_origin(Duration::from_secs(100));
            let mut read = scene
                .node
                .submit_task_command(
                    participation,
                    operation,
                    deadline,
                    ParticipationCommand::GetDynamicFilters {
                        task,
                        acknowledged: None,
                    },
                )
                .map_err(|original| Box::new(original) as Failure)?;
            let Some((actual_actor, work)) = read.endpoint.external.slot().context_author() else {
                return Err(Box::new((
                    "the original preparing read has no actual R address",
                    read,
                )));
            };
            let metadata = read.request();
            let Some(delivery) = wait_delivery(&mut read) else {
                return Err(Box::new((
                    "the original preparing GetDynamicFilters did not answer",
                    read,
                )));
            };
            // The original accepted but still-preparing CountOnly Task has no
            // dynamic-filter publication. Pure Core authors Idempotent here;
            // no payload is borrowed and no final-info tail can detach.
            if actual_actor != context_actor()
                || delivery.request() != metadata
                || metadata.operation != operation
                || metadata.deadline != deadline
                || !matches!(
                    delivery.body(),
                    ParticipationReplyBody::Verdict(
                        novarocks_worker_protocol::participation::RequestResult::Outcome(
                            OperationOutcome::Idempotent
                        )
                    )
                )
            {
                return Err(Box::new((
                    "the original preparing read metadata/verdict changed",
                    delivery,
                    read,
                )));
            }
            let Some(saved) = scene
                .node
                .original_task_request_for_test(participation, work)
            else {
                return Err(Box::new((
                    "the same original preparing Task R was not retained",
                    delivery,
                    read,
                )));
            };
            if saved.0.author != work
                || saved.0.operation != operation
                || saved.0.deadline != deadline
                || saved.1 != task
                || !saved.2
            {
                return Err(Box::new((
                    "the original preparing read lost its attached Task route",
                    saved,
                    delivery,
                    read,
                )));
            }
            drop(read);
            if scene.ledger.used(Dimension::Requests.id()) != 1 {
                return Err(Box::new((
                    "the original preparing read did not retain one real R",
                    delivery,
                )));
            }
            preparing_read = Some((delivery, saved.0));
        }
        if bundle.stopped_before_host {
            trace_original_preparation_stage(
                scene,
                participation,
                task,
                bundle,
                "before GetFinalInfo admission",
            );
            let operation = operation_number(26);
            let mut final_info = scene
                .node
                .submit_task_command(
                    participation,
                    operation,
                    MonotonicInstant::from_origin(Duration::from_secs(100)),
                    ParticipationCommand::GetFinalInfo { task },
                )
                .map_err(|original| {
                    trace_original_preparation_stage(
                        scene,
                        participation,
                        task,
                        bundle,
                        "ERR GetFinalInfo admission/submission",
                    );
                    eprintln!("original GetFinalInfo Node refusal: {original:?}");
                    Box::new(original) as Failure
                })?;
            let (actual_actor, original_work) = final_info
                .endpoint
                .external
                .slot()
                .participation_author()
                .expect("the actual Task R has a registered address");
            assert_eq!(actual_actor, participation_actor());
            let original_metadata = final_info.request();
            trace_original_preparation_stage(
                scene,
                participation,
                task,
                bundle,
                "before GetFinalInfo delivery wait",
            );
            let Some(final_delivery) = wait_delivery(&mut final_info) else {
                trace_original_preparation_stage(
                    scene,
                    participation,
                    task,
                    bundle,
                    "ERR GetFinalInfo delivery wait",
                );
                eprintln!(
                    "original GetFinalInfo address={original_work:?} metadata={original_metadata:?} saved_request={:?}",
                    scene
                        .node
                        .original_task_request_for_test(participation, original_work)
                );
                return Err(Box::new((
                    "original GetFinalInfo delivery timeout",
                    final_info,
                )));
            };
            trace_original_preparation_stage(
                scene,
                participation,
                task,
                bundle,
                "GetFinalInfo delivery taken before Cancel",
            );
            match final_delivery.body() {
                ParticipationReplyBody::Verdict(result) => eprintln!(
                    "original GetFinalInfo actual verdict={result:?} work={original_work:?} metadata={original_metadata:?}"
                ),
                ParticipationReplyBody::FinalInfo(_) => eprintln!(
                    "original GetFinalInfo actual body=FinalInfo work={original_work:?} metadata={original_metadata:?}"
                ),
                _ => eprintln!(
                    "original GetFinalInfo actual body=other original variant work={original_work:?} metadata={original_metadata:?}"
                ),
            }
            assert_eq!(final_delivery.request(), original_metadata);
            assert_eq!(original_metadata.operation, operation);
            assert!(
                matches!(
                    final_delivery.body(),
                    ParticipationReplyBody::Verdict(
                        novarocks_worker_protocol::participation::RequestResult::Outcome(
                            OperationOutcome::InvalidStateOrRequest
                        )
                    )
                ),
                "the still-preparing Task has no successful final-info body"
            );
            // This same Node saved the real event token before its runtime
            // input retired. Neither Work nor operation can synthesize it.
            let original_request = scene
                .node
                .original_task_request_for_test(participation, original_work)
                .expect("the same live Task R remains owned");
            assert_eq!(original_request.0.author, original_work);
            assert_eq!(original_request.0.operation, operation);
            assert_eq!(original_request.0.deadline, original_metadata.deadline);
            assert_eq!(original_request.1, task);
            drop(final_info); // Only the actual delivery now owns its external leg.
            assert_eq!(scene.ledger.used(Dimension::Requests.id()), 1);
            let mut cancel = scene
                .node
                .submit_task_command(
                    participation,
                    operation_number(25),
                    MonotonicInstant::from_origin(Duration::from_secs(100)),
                    ParticipationCommand::Cancel {
                        task,
                        reason: CancelReason::UpstreamNoLongerNeeded,
                    },
                )
                .map_err(|original| {
                    trace_original_preparation_stage(
                        scene,
                        participation,
                        task,
                        bundle,
                        "ERR Cancel admission/submission",
                    );
                    eprintln!("original Cancel Node refusal: {original:?}");
                    Box::new(original) as Failure
                })?;
            let Some(delivery) = wait_delivery(&mut cancel) else {
                trace_original_preparation_stage(
                    scene,
                    participation,
                    task,
                    bundle,
                    "ERR Cancel delivery wait",
                );
                return Err(Box::new(("original Cancel delivery timeout", cancel)));
            };
            assert!(
                matches!(delivery.body(), ParticipationReplyBody::Task { outcome: OperationOutcome::Accepted, status }
                if status.identity() == task)
            );
            drop(delivery);
            drop(cancel);
            trace_original_preparation_stage(
                scene,
                participation,
                task,
                bundle,
                "before actual Stop observation wait",
            );
            if !wait_until(|| {
                scene
                    .node
                    .preparation_fixture_inventory(participation, task)
                    .is_some_and(|current| current.stopped && current.stop_call_exited)
            }) {
                trace_original_preparation_stage(
                    scene,
                    participation,
                    task,
                    bundle,
                    "ERR actual Stop observation wait",
                );
                return Err(Box::new(
                    "original Stop did not physically return before releasing preparation",
                ));
            }
            assert_eq!(bundle.host_entries.load(Ordering::SeqCst), 0);
            assert_eq!(scene.ledger.used(Dimension::PreparingTasks.id()), 1);
            assert_eq!(
                scene.ledger.used(Dimension::PreparingBytes.id()),
                held_bytes
            );
            trace_original_preparation_stage(
                scene,
                participation,
                task,
                bundle,
                "explicit gate release after actual Stop observed",
            );
            bundle.prepare.release();
            if !wait_until(|| {
                scene.ledger.used(Dimension::PreparingTasks.id()) == 0
                && scene.ledger.used(Dimension::PreparingBytes.id()) == 0
                && scene.ledger.used(Dimension::ActiveTasks.id()) == 0
                && scene.ledger.used(Dimension::Requests.id()) == 1
                && resources.host.observed.protocol_events.lock().unwrap().iter().any(|event|
                    matches!(event, crate::TaskProtocolEvent::TaskTerminalRetained { identity, state }
                        if *identity == task && *state == TaskState::Canceled))
            }) {
                scene.node.print_neutral_task_inventory(participation, task);
                trace_original_preparation_stage(
                    scene,
                    participation,
                    task,
                    bundle,
                    "ERR stopped actual physical retirement wait",
                );
                return Err(Box::new(
                    "stopped original bundle did not reach real physical/retained retirement",
                ));
            }
            assert_eq!(bundle.host_entries.load(Ordering::SeqCst), 0);
            assert_eq!(bundle.transfers.load(Ordering::SeqCst), 0);
            assert_eq!(resources.host.observed.activated.load(Ordering::SeqCst), 0);
            assert_eq!(resources.host.observed.completed.load(Ordering::SeqCst), 0);
            assert!(resources.host.tasks.lock().unwrap().is_empty());
            // The compiled Task and its real preparation are now gone; this
            // one original Task-causal delivery remains physically held.
            assert_eq!(scene.ledger.used(Dimension::Requests.id()), 1);
            return Ok(Some((final_delivery, original_request.0)));
        }
        bundle.prepare.release();
        let activation_window = bundle.activation_window.lock().unwrap().clone();
        if let Some(window) = activation_window {
            window.before_adoption(scene, participation, task, resources, bundle)?;
        }
        if !wait_until(|| bundle.adoption.entered()) {
            return Err(Box::new(
                "the actual provider adoption callback did not start",
            ));
        }
        let body = bundle.inventory(task);
        assert_eq!(body.preparation_key, before.preparation_key);
        assert!(
            body.body_exited,
            "the actual preparation body already returned"
        );
        assert!(
            !body.host_borrowers_exited,
            "the original provider callback/capture is still executing"
        );
        assert!(!body.whole_report_retired);
        assert_eq!(scene.ledger.used(Dimension::PreparingTasks.id()), 1);
        assert_eq!(
            scene.ledger.used(Dimension::PreparingBytes.id()),
            held_bytes
        );
        assert_eq!(bundle.whole_refusals.load(Ordering::SeqCst), 1);
        assert_eq!(bundle.transfers.load(Ordering::SeqCst), 1);
        assert_eq!(bundle.second_take_refusals.load(Ordering::SeqCst), 1);
        bundle.adoption.release();
        let activation_window = bundle.activation_window.lock().unwrap().clone();
        if let Some(window) = activation_window {
            window.after_adoption()?;
        }
    }
    let completion_contract = resources.host.completion_contract.lock().unwrap().clone();
    if let Some(contract) = &completion_contract {
        contract.observe_before_exit(scene, participation, task, resources)?;
    }
    let expected_state = completion_contract
        .as_ref()
        .map_or(TaskState::Finished, |contract| contract.expected_state());
    if !wait_until(|| {
        let reporter = resources.host.observed.reporter.lock().unwrap().clone();
        resources.host.observed.activated.load(Ordering::SeqCst) == 1
            && resources.host.observed.completed.load(Ordering::SeqCst) == 1
            && reporter.is_some_and(|reporter| {
                reporter
                    .current()
                    .is_ok_and(|status| status.installed() && status.state() == expected_state)
            })
            && scene.ledger.used(Dimension::PreparingTasks.id()) == 0
            && scene.ledger.used(Dimension::PreparingBytes.id()) == 0
    }) {
        let reporter = resources.host.observed.reporter.lock().unwrap().clone();
        let status = reporter.as_ref().map(TaskStatusReporter::current);
        let abort = resources.host.observed.abort.lock().unwrap();
        let abort_kind = abort.as_ref().map(|reason| match reason {
            ResultAbort::PrepareRollback => "PrepareRollback",
            ResultAbort::NeverStarted => "NeverStarted",
            ResultAbort::Failed(_) => "Failed",
            ResultAbort::Cancelled(_) => "Cancelled",
        });
        eprintln!(
            "actual Task timeout: activated={}, completed={}, status={:?}, root_abort={abort_kind:?}, preparing_tasks={}, preparing_bytes={}",
            resources.host.observed.activated.load(Ordering::SeqCst),
            resources.host.observed.completed.load(Ordering::SeqCst),
            status.as_ref().map(|value| value
                .as_ref()
                .map(|status| (status.state(), status.installed()))),
            scene.ledger.used(Dimension::PreparingTasks.id()),
            scene.ledger.used(Dimension::PreparingBytes.id()),
        );
        eprintln!(
            "actual Execution stages: inputs={}, rows={}, finish_input={}, producer_exit={}, kernel_publisher={}, completion_entered={}",
            resources
                .host
                .observed
                .input_submitted
                .load(Ordering::SeqCst),
            resources.host.observed.input_rows.load(Ordering::SeqCst),
            resources
                .host
                .observed
                .input_finished
                .load(Ordering::SeqCst),
            resources
                .host
                .observed
                .producer_exit_observed
                .load(Ordering::SeqCst),
            resources
                .host
                .observed
                .kernel_published
                .load(Ordering::SeqCst),
            resources
                .host
                .observed
                .completion_entered
                .load(Ordering::SeqCst),
        );
        scene.node.print_neutral_task_inventory(participation, task);
        return Err(Box::new(
            "real execution / installed / preparation whole-exit timeout",
        ));
    }
    if let Some((old_delivery, old_request)) = preparing_read.take() {
        let attached = scene
            .node
            .original_task_request_for_test(participation, old_request.author);
        if attached.is_none_or(|facts| facts.0 != old_request || facts.1 != task || !facts.2) {
            return Err(Box::new((
                "the original non-final-info Task R detached before its held hook",
                attached,
                old_delivery,
            )));
        }
        let hook = on_preparing_read_held.expect("the same original preparing read has its hook");
        if let Err(original) = hook(scene, participation, resources, task, old_request) {
            return Err(Box::new((original, old_delivery)));
        }
        if scene.ledger.used(Dimension::Requests.id()) != 1 {
            return Err(Box::new((
                "the same original preparing R must remain after its held hook",
                old_delivery,
            )));
        }
        drop(old_delivery);
        if !wait_until(|| scene.ledger.used(Dimension::Requests.id()) == 0) {
            return Err(Box::new(
                "the original preparing Task R did not really retire before Root reading",
            ));
        }
    }
    if !read_root {
        // The compiled producer and original Completion really finished, but
        // the client never reads or acknowledges this Root. Its explicit Abort
        // must seal it and observe actual physical exit; Release cannot discard
        // unacknowledged results.
        // No synthetic RootDrained or terminal/cancellation fact is supplied.
        return Ok(None);
    }
    // A delivered End is an offer, never a client acknowledgement. The
    // first original R reads consumed=0 and must not carry a drain permit.
    let offered = novarocks_execution_contract::root_result::RootResultRead::try_new(
        task,
        novarocks_result_contract::RootProfileId::V1,
        novarocks_result_contract::RootOutputKind::CountOnly,
        NonZeroU64::new(1),
        0,
        Duration::from_secs(1),
    )
    .unwrap();
    let read = scene
        .node
        .begin_root_read(
            participation,
            ActorId::new(3).unwrap(),
            operation_number(22),
            MonotonicInstant::from_origin(Duration::from_secs(100)),
            &offered,
        )
        .map_err(|original| Box::new(original) as Failure)?;
    let mut end_delivery = resources
        ._async_runtime
        .block_on(read.read())
        .map_err(|original| Box::new(original) as Failure)?;
    let end = match end_delivery.reply().outcome {
        novarocks_execution_contract::root_result::RootReadOutcome::End(end)
            if end.sequence.get() == 1 && end.output_rows == 1 =>
        {
            end
        }
        _ => return Err(Box::new(("actual CountOnly End mismatch", end_delivery))),
    };
    assert_eq!(end_delivery.reply().root_task, task);
    assert_eq!(end_delivery.reply().accepted_consumed, 0);
    if let Some(original) = end_delivery.take_drain_permit() {
        return Err(Box::new((
            "offered End cannot mint a client ACK drain permit",
            original,
            end_delivery,
        )));
    }
    // The response's physical holder exits independently of the test client
    // deciding it has consumed End. Its original unused R must really retire.
    drop(end_delivery);
    if !wait_until(|| scene.ledger.used(Dimension::Requests.id()) == 0) {
        return Err(Box::new("original End R did not physically retire"));
    }

    // Now the client explicitly sends a new ACK-only request with consumed=1.
    // Only that actual channel-locked acknowledgement may mint RootDrained.
    let acknowledged = novarocks_execution_contract::root_result::RootResultRead::try_new(
        task,
        novarocks_result_contract::RootProfileId::V1,
        novarocks_result_contract::RootOutputKind::CountOnly,
        None,
        end.sequence.get(),
        Duration::from_secs(1),
    )
    .unwrap();
    let read = scene
        .node
        .begin_root_read(
            participation,
            ActorId::new(3).unwrap(),
            operation_number(23),
            MonotonicInstant::from_origin(Duration::from_secs(100)),
            &acknowledged,
        )
        .map_err(|original| Box::new(original) as Failure)?;
    let mut ack_delivery = resources
        ._async_runtime
        .block_on(read.read())
        .map_err(|original| Box::new(original) as Failure)?;
    if ack_delivery.reply().accepted_consumed != end.sequence.get()
        || !matches!(
            ack_delivery.reply().outcome,
            novarocks_execution_contract::root_result::RootReadOutcome::AckOnly
        )
    {
        return Err(Box::new((
            "actual End acknowledgement mismatch",
            ack_delivery,
        )));
    }
    let Some(permit) = ack_delivery.take_drain_permit() else {
        return Err(Box::new((
            "acknowledged End missing its original drain permit",
            ack_delivery,
        )));
    };
    assert_eq!(permit.identity(), task);
    let mut original_drain = permit.submit();
    let outcome = match resources
        ._async_runtime
        .block_on(async { tokio::time::timeout(Duration::from_secs(5), &mut original_drain).await })
    {
        Ok(outcome) => outcome,
        Err(_) => {
            return Err(Box::new((
                "original RootDrained reply timeout",
                original_drain,
                ack_delivery,
            )));
        }
    };
    // Native successful Participation handoff already completed this Task before
    // the ACK. RootDrained adjudicates AlreadyTerminal(Finished), rather than
    // inventing another status publication or reversing the terminal state.
    if !matches!(
        outcome,
        crate::TaskRootDrainOutcome::AlreadyTerminal {
            state: TaskState::Finished
        }
    ) {
        return Err(Box::new((
            "original finished RootDrained outcome",
            outcome,
            ack_delivery,
        )));
    }
    drop(original_drain);
    if scene.ledger.used(Dimension::Requests.id()) != 1 {
        return Err(Box::new((
            "ACK delivery must retain its original R",
            ack_delivery,
        )));
    }
    drop(ack_delivery);
    if !wait_until(|| scene.ledger.used(Dimension::Requests.id()) == 0) {
        return Err(Box::new("original ACK R did not physically retire"));
    }
    Ok(None)
}
pub(super) fn shutdown(resources: TaskResources) -> Result<(), Failure> {
    let TaskResources {
        host,
        loading,
        preparation,
        lifecycle,
        _async_runtime,
    } = resources;
    if let Err(original) = loading.shutdown() {
        return Err(Box::new((
            host,
            original,
            preparation,
            lifecycle,
            _async_runtime,
        )));
    }
    if let Err(original) = preparation.shutdown() {
        return Err(Box::new((host, original, lifecycle, _async_runtime)));
    }
    if let Err(original) = lifecycle.shutdown() {
        return Err(Box::new((host, original, _async_runtime)));
    }
    if let Err(original) = host.completion.shutdown() {
        return Err(Box::new((host, original, _async_runtime)));
    }
    if let Err(original) = host.execution.shutdown_driver_execution() {
        return Err(Box::new((host, original, _async_runtime)));
    }
    drop(host);
    drop(_async_runtime);
    Ok(())
}
pub(super) fn compiled_task_geometry() -> ReservationResources {
    let policy = ControlRuntimePolicy::try_new(IngressPositionLimits {
        ordinary_running: 1,
        ordinary_waiting: 0,
        control_running: 1,
        control_waiting: 0,
        max_batch_items: 1,
        root_read_holders: 1,
    })
    .unwrap();
    // Exact empty input/descriptor retained geometry: zero assignment/domain/
    // topology members, four owned frozen bytes, no inbound normal-close row.
    let preparation_bytes =
        (std::mem::size_of::<TaskCreationInput>() + 4 + std::mem::size_of::<CreateTask>()) as u64;
    policy
        .process_task_storage(1, 1)
        .unwrap()
        .resources()
        .unwrap()
        .checked_add(&ReservationResources::new(
            ResourceVector::new([
                (Dimension::ActiveTasks.id(), 1),
                (Dimension::PreparingTasks.id(), 1),
                (Dimension::PreparingBytes.id(), preparation_bytes),
            ])
            .unwrap(),
            Default::default(),
        ))
        .unwrap()
}
#[test]
fn real_node_compiled_task_runs_and_finishes_on_one_and_two_pollers() {
    let geometry = compiled_task_geometry();
    for workers in [1, 2] {
        run_node_lifecycle_with_task(
            workers,
            NodeClose::Release,
            geometry.clone(),
            start_tasks,
            exercise,
            shutdown,
        );
        run_node_lifecycle_with_task(
            workers,
            NodeClose::Abort,
            geometry.clone(),
            start_tasks,
            exercise_unread_root,
            shutdown,
        );
    }
}

struct LateUpdateContent {
    getters: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    drop_entered: Arc<std::sync::atomic::AtomicBool>,
    disposal: Arc<PreparationGate>,
}
impl std::fmt::Debug for LateUpdateContent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LateUpdateContent")
    }
}
impl novarocks_execution_contract::CodecOwnedContent for LateUpdateContent {
    fn fingerprint(&self) -> novarocks_execution_contract::ContentFingerprint {
        self.getters.fetch_add(1, Ordering::SeqCst);
        novarocks_execution_contract::ContentFingerprint::from_bytes([31; 16])
    }
    fn encoded_len(&self) -> usize {
        self.getters.fetch_add(1, Ordering::SeqCst);
        1
    }
}
impl Drop for LateUpdateContent {
    fn drop(&mut self) {
        self.drop_entered.store(true, Ordering::Release);
        self.disposal.wait();
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn exercise_retired_task_update(
    scene: &Scenario<TaskResources>,
    participation: ParticipationRef,
    failures: &mut Vec<String>,
) {
    let task = scene.diagnostic_task.lock().unwrap().unwrap().1;
    if !wait_until(|| {
        scene
            .node
            .task_update_retirement_for_test(participation, task)
            == Some((false, true))
    }) {
        failures.push("the original Task did not physically retire before late Update".into());
        return;
    }
    let getters = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let drop_entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let disposal = Arc::new(PreparationGate::default());
    let _release = ReleasePreparationGate(disposal.clone());
    let original = novarocks_execution_contract::UpdateTask::try_new(
        operation_number(31),
        task,
        vec![TaskDomainUpdate::TaskDynamicFilter {
            version: novarocks_execution_contract::DomainVersion::FIRST,
            payload: Arc::new(LateUpdateContent {
                getters: getters.clone(),
                drops: drops.clone(),
                drop_entered: drop_entered.clone(),
                disposal: disposal.clone(),
            }),
        }],
    )
    .unwrap();
    let ingress_gate = Arc::new(tokio::sync::Semaphore::new(1));
    let ingress =
        crate::WorkerIngressResponsibility::from_admitted(Arc::new(ComponentIngressBacking {
            _permit: ingress_gate.clone().try_acquire_owned().unwrap(),
        }));
    let deadline = MonotonicInstant::from_origin(Duration::from_secs(100));
    let submitted = scene
        .node
        .submit_task_domain(participation, original, deadline, ingress);
    match submitted {
        Ok(mut handle) => {
            let request = handle.request();
            if request.operation != operation_number(31) || request.deadline != deadline {
                failures.push("late Update changed its original request metadata".into());
            }
            let delivery = wait_delivery(&mut handle);
            if !delivery.as_ref().is_some_and(|delivery| {
                delivery.request() == request
                    && matches!(
                        delivery.body(),
                        ParticipationReplyBody::Verdict(
                            novarocks_worker_protocol::participation::RequestResult::Outcome(
                                OperationOutcome::TerminalRejected
                            )
                        )
                    )
            }) {
                failures
                    .push("late Update did not receive its original Core TerminalRejected".into());
            }
            // End the actual external delivery while the original raw destructor
            // remains in progress. Neither the verdict nor external exit refunds R.
            drop(delivery);
            drop(handle);
            if !wait_until(|| drop_entered.load(Ordering::Acquire)) {
                failures.push("the original late Update raw destructor did not enter".into());
            } else if scene.ledger.used(Dimension::Requests.id()) != 1
                || ingress_gate.available_permits() != 0
                || drops.load(Ordering::SeqCst) != 0
            {
                failures.push(
                    "late Update refunded its original R/ingress before actual raw Drop".into(),
                );
            }
        }
        Err(original) => {
            failures.push(format!(
                "late Update was refused before its Core decision: {original:?}"
            ));
            // A reintroduced old admission bug returns the actual raw here.
            // Release its destructor first, then drop those returned originals
            // and let the same Node finish normally before asserting the failure.
            disposal.release();
            drop(original);
        }
    }
    disposal.release();
    if !wait_until(|| {
        drops.load(Ordering::SeqCst) == 1
            && scene.ledger.used(Dimension::Requests.id()) == 0
            && ingress_gate.available_permits() == 1
    }) {
        failures
            .push("late Update originals did not retire after the real destructor returned".into());
    }
    if getters.load(Ordering::SeqCst) != 0 {
        failures.push("terminal inspection evaluated an unused rich domain payload".into());
    }
}

#[test]
fn late_update_after_real_task_retirement_keeps_core_verdict_and_original_raw_exit() {
    let mut failures = Vec::new();
    run_node_lifecycle_with_task(
        1,
        NodeClose::Release,
        compiled_task_geometry(),
        start_tasks,
        |scene, participation, resources| {
            let held = exercise(scene, participation, resources)?;
            exercise_retired_task_update(scene, participation, &mut failures);
            Ok(held)
        },
        shutdown,
    );
    assert!(failures.is_empty(), "{}", failures.join("; "));
}

// These are the same two named bundle contracts moved from actors/tests.rs.
// The actual Node now owns their RF/read gate, Core RetireTask and payment tail.
#[test]
fn the_original_preparation_bundle_returns_whole_on_refusal_and_transfers_once_to_the_host() {
    run_node_lifecycle_with_task(
        1,
        NodeClose::Release,
        compiled_task_geometry(),
        |root| start_tasks_with_bundle(root, Some(false)),
        exercise,
        shutdown,
    );
}
#[test]
fn stopped_preparation_really_drops_its_unused_bundle_without_entering_the_host() {
    run_node_lifecycle_with_task(
        1,
        NodeClose::Release,
        compiled_task_geometry(),
        |root| start_tasks_with_bundle(root, Some(true)),
        exercise,
        shutdown,
    );
}
