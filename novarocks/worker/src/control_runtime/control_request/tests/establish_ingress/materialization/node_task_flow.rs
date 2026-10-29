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

use super::node_flow::{
    NodeClose, Scenario, operation_number, retain_failure, run_node_lifecycle_with_task,
    wait_delivery, wait_until,
};
use super::*;
use crate::{
    PreparedTaskInstallation, RunnableTask, TaskExecutionHost, TaskPreparationContext,
    TaskPreparationRoles, TaskStatusReporter, TaskTerminalAction,
    control_runtime::{
        context_host::{TaskDriverPorts, task_driver::TaskDriverHostJob},
        host_executor::{HostExecutor, HostExecutorLimits},
        loading::TaskCreatePayload,
        loading_host_job::LoadingHostJob,
    },
};
use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_execution::runtime::fragment::scan::CompiledScanSources;
use novarocks_execution::{
    exec::fragment::program::FragmentContractVersion,
    runtime::{
        execution_runtime::{ExecutionRuntime, ExecutionRuntimeConfig},
        fragment::{
            BackendNum, CompiledFragmentSubmission, DormantFragmentHandle,
            ExchangeInputAssignments, ExecutionResult, FragmentCancelReason, FragmentInstanceId,
            FragmentInstanceSpec, FragmentOutcome, FragmentPrepareContext, FragmentRuntimeOptions,
            FragmentSinkAssignment, RunningFragmentHandle, ScanAssignments,
            io::{
                ExchangeFrame, ExchangeFrameTransmitter, ExchangeTransmitRejection,
                FragmentCommitLease, FragmentCommitPort, FragmentCommitReport, FragmentIoError,
                FragmentResultSession, FragmentResultWriter, FragmentSinkLoadStats,
                NoopFragmentEventSink, ResultWriteSpec, TabletCommitInfo, TabletFailInfo,
            },
            prepare_compiled_fragment,
        },
        function_set::ExecutionFunctionSetBuilder,
        query_options::QueryOptions,
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
use std::{any::Any, collections::BTreeMap, num::NonZeroU64, sync::Weak};

type Failure = Box<dyn Any + Send>;
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
                exchange_wait: Duration::from_secs(1),
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
                exchange_wait_ms: 1000,
                exchange_io_threads: 1,
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
    fragment::io::{
        FragmentIoErrorKind, FragmentIoOperation, ResultAbort, ResultWriteAdmission,
        RootInputAdmission, RootInputAuthority, RootInputReservationAdmission, RootProducerState,
        RootResultInput, RootResultSession, RootResultWriteSpec, RootWorkExitProgress,
    },
    observable::Observable,
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
fn root_io(error: crate::root_result_channel::RootChannelError) -> FragmentIoError {
    FragmentIoError::from_domain(
        FragmentIoOperation::ResultWrite,
        FragmentIoErrorKind::Internal,
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
    fn try_acquire_input(&self) -> Result<RootInputAdmission, FragmentIoError> {
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
    fn submit_input(&self, offered: &mut Option<RootResultInput>) -> Result<(), FragmentIoError> {
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
    fn finish_input(&self) -> Result<(), FragmentIoError> {
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
        self.channel
            .close(novarocks_execution_contract::root_lifetime::RootRetentionClose::ContextAborted);
        let encoder = self.encoder.lock().unwrap().take();
        if let Some(encoder) = encoder {
            let _original_failure = encoder.finish_encoder();
        }
        self.exited.notify_observers();
    }
}
struct Ports;
struct CommitLease;
impl FragmentCommitPort for Ports {
    fn acquire(&self, _: UniqueId) -> ExecutionResult<Box<dyn FragmentCommitLease>> {
        Ok(Box::new(CommitLease))
    }
}
impl FragmentCommitLease for CommitLease {
    fn add_load_stats(&mut self, _: FragmentSinkLoadStats) -> ExecutionResult<()> {
        panic!("Noop has no load stats")
    }
    fn add_tablet_commit_info(&mut self, _: TabletCommitInfo) -> ExecutionResult<()> {
        panic!("Noop has no tablet commit")
    }
    fn add_tablet_fail_info(&mut self, _: TabletFailInfo) -> ExecutionResult<()> {
        panic!("Noop has no tablet failure")
    }
    fn finish(&mut self) -> ExecutionResult<FragmentCommitReport> {
        Ok(FragmentCommitReport::default())
    }
    fn handoff(&mut self) -> ExecutionResult<()> {
        Ok(())
    }
    fn rollback(&mut self) -> ExecutionResult<()> {
        Ok(())
    }
}
impl ExchangeFrameTransmitter for Ports {
    fn transmit(&self, _: ExchangeFrame) -> Result<(), ExchangeTransmitRejection> {
        panic!("Noop has no exchange")
    }
}
impl FragmentResultWriter for Ports {
    fn open(&self, _: ResultWriteSpec) -> Result<Arc<dyn FragmentResultSession>, FragmentIoError> {
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
    dormant: Option<DormantFragmentHandle>,
    running: Option<RunningFragmentHandle>,
    start: Option<crate::TaskStartPermit>,
    terminal: Option<crate::TaskTerminalPermit>,
    statistics: Option<crate::TaskStatisticsProducer>,
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
struct PreparedChild {
    dormant: Mutex<Option<DormantFragmentHandle>>,
    target: Weak<Runnable>,
    rolling_back: std::sync::atomic::AtomicBool,
}
impl crate::TaskPreparationChild for PreparedChild {
    fn request_rollback(&self) {
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
        let original_moved = self.dormant.lock().unwrap().is_none();
        if original_moved && !self.rolling_back.load(Ordering::Acquire) {
            crate::TaskPreparationChildExit::Exited
        } else {
            crate::TaskPreparationChildExit::Pending
        }
    }
    fn adopt_runtime(&self) {
        let dormant = self
            .dormant
            .lock()
            .unwrap()
            .take()
            .expect("one original dormant adoption");
        let target = self
            .target
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
            novarocks_execution::runtime::fragment::handle::FragmentTerminalPublisher,
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
            running.cancel(FragmentCancelReason::new(format_args!("{reason:?}")));
        }
    }
    fn abort(&self, cause: AbortCause) {
        let running = self.state.lock().unwrap().running.clone();
        if let Some(running) = running {
            running.cancel(FragmentCancelReason::new(format_args!("{cause:?}")));
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
}
impl TaskExecutionHost for CompiledTaskHost {
    fn close_context_admission(&self, _: QueryContextRef) {}
    fn retire_context_execution(&self, _: QueryContextRef) {}
    fn forget_context_admission(&self, _: QueryContextRef) {}
    fn install_receiver(
        &self,
        descriptor: &TaskDescriptor,
        input: TaskCreationInput,
        mut preparation: TaskPreparationContext<'_>,
    ) -> Result<PreparedTaskInstallation, HostRejection> {
        let (fragment, assignment) = input.into_parts();
        assert_eq!(fragment.bytes().as_ref(), b"root");
        assert!(assignment.into_stored().downcast::<Assignment>().is_ok());
        let control = preparation.runtime_control();
        let TaskPreparationRoles {
            fragments,
            start,
            terminal,
            statistics,
            final_statistics,
            completion_exit,
        } = preparation.take_roles().unwrap();
        let instance = FragmentInstanceSpec::new_native(
            FragmentContractVersion::CURRENT,
            descriptor.identity().query_execution_id().query_id(),
            FragmentInstanceId::new(descriptor.fragment_instance_id()),
            ScanAssignments::default(),
            ExchangeInputAssignments::default(),
            FragmentSinkAssignment::None,
            FragmentRuntimeOptions::new(QueryOptions::default(), false),
            NonZeroUsize::new(1).unwrap(),
            BackendNum::try_new(1).unwrap(),
        );
        let submission = CompiledFragmentSubmission::try_new(
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
        let session = CountSession::open(root.clone(), health, self.observed.clone());
        let context = FragmentPrepareContext::new(
            None,
            None,
            None,
            Arc::new(Ports),
            Arc::new(Ports),
            Arc::new(NoopFragmentEventSink),
        )
        .with_root_result_session(session)
        .with_execution_runtime(self.execution.clone())
        .with_fragment_commit_port(Arc::new(Ports))
        .with_result_identity(descriptor.identity())
        .with_task_control(control)
        .with_completion_roles(fragments);
        let dormant = prepare_compiled_fragment(submission, context).unwrap();
        let runnable = Arc::new(Runnable {
            identity: descriptor.identity(),
            state: Mutex::new(RunnableState {
                start: Some(start),
                terminal: Some(terminal),
                statistics: Some(statistics),
                final_statistics: Some(final_statistics),
                completion_exit: Some(completion_exit),
                root: Some(root.clone()),
                ..RunnableState::default()
            }),
            observed: self.observed.clone(),
        });
        preparation
            .register_child(Arc::new(PreparedChild {
                dormant: Mutex::new(Some(dormant)),
                target: Arc::downgrade(&runnable),
                rolling_back: std::sync::atomic::AtomicBool::new(false),
            }))
            .unwrap();
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
    fn install_inbound_capability(&self, _: &TaskDescriptor) -> Result<(), HostRejection> {
        Ok(())
    }
    fn remove_inbound_capability(&self, _: &TaskDescriptor) {}
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
        let mut completion = self
            .completion
            .reserve_task_slot(
                descriptor.identity(),
                descriptor.fragment_instance_id(),
                reporter,
                exit,
            )
            .unwrap();
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
                    assert!(matches!(fact.outcome(), FragmentOutcome::Succeeded));
                    let final_statistics = final_statistics.take().unwrap().seal_for_terminal(None);
                    let (statistics, running) = {
                        let mut state = runnable.state.lock().unwrap();
                        (state.statistics.take(), state.running.take())
                    };
                    drop(statistics);
                    drop(running);
                    observation
                        .handoff(crate::TaskTerminalObservation {
                            intent: crate::TaskTerminalIntent::SuccessfulProduction {
                                completed_output: Some(TaskOutputFacts::new(matches!(
                                    root.producer_state(),
                                    RootProducerState::ContextHeld
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
fn preparation_body(job: TaskDriverHostJob) {
    job.run_on_host();
}
struct TaskResources {
    host: Arc<CompiledTaskHost>,
    loading: HostExecutor<LoadingHostJob, fn(LoadingHostJob)>,
    preparation: HostExecutor<TaskDriverHostJob, fn(TaskDriverHostJob)>,
    lifecycle: HostExecutor<TaskDriverHostJob, fn(TaskDriverHostJob)>,
    _async_runtime: tokio::runtime::Runtime,
}
fn start_tasks(
    root: &mut ProcessControlOwner,
) -> Result<(TaskDriverPorts, TaskResources), Failure> {
    let capacity = root
        .task_capacity_signal()
        .map_err(|original| Box::new(original) as Failure)?;
    let one = NonZeroUsize::new(1).unwrap();
    // One admitted Task plus one physical tail; two contexts are metadata
    // groups, and no pool-size choice signs a Task exit.
    let limits = HostExecutorLimits {
        contexts: NonZeroUsize::new(3).unwrap(),
        jobs: NonZeroUsize::new(2).unwrap(),
        per_context: NonZeroUsize::new(2).unwrap(),
        workers: one,
    };
    let loading =
        match HostExecutor::start_with_waker(limits, loading_body as fn(_), capacity.waker()) {
            Ok(owner) => owner,
            Err(original) => return Err(Box::new(original)),
        };
    let preparation =
        match HostExecutor::start_with_waker(limits, preparation_body as fn(_), capacity.waker()) {
            Ok(owner) => owner,
            Err(original) => return Err(Box::new((loading, original))),
        };
    // Cleanup execution is independent of preparation, whose body can wait
    // for its own real cancellation/settlement. More preparation threads do
    // not provide that progress guarantee.
    let lifecycle =
        match HostExecutor::start_with_waker(limits, preparation_body as fn(_), capacity.waker()) {
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
    });
    let ports = TaskDriverPorts {
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
    context: QueryContextRef,
    resources: &mut TaskResources,
) -> Result<(), Failure> {
    exercise_root(scene, context, resources, true)
}
fn exercise_unread_root(
    scene: &Scenario<TaskResources>,
    context: QueryContextRef,
    resources: &mut TaskResources,
) -> Result<(), Failure> {
    exercise_root(scene, context, resources, false)
}
fn exercise_root(
    scene: &Scenario<TaskResources>,
    context: QueryContextRef,
    resources: &mut TaskResources,
    read_root: bool,
) -> Result<(), Failure> {
    let task = TaskIdentity::new(
        context.query_execution_id(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        context.backend_process_id(),
    );
    *scene.diagnostic_task.lock().unwrap() = Some((context, task));
    let descriptor = TaskDescriptor::try_new(
        task,
        UniqueId::new(77, 88),
        NonZeroUsize::new(1).unwrap(),
        vec![],
        ExchangeTopology::default(),
    )
    .unwrap();
    let request =
        Arc::new(CreateTask::try_new(operation_number(21), context, descriptor, vec![]).unwrap());
    let payload = TaskCreatePayload {
        id: PayloadId(9),
        request,
        input: TaskCreationInput::new(
            FrozenBytes::freeze_encoded(b"root".to_vec()),
            Box::new(Assignment),
        ),
        ingress_retained_bytes: 0,
    };
    let mut handle = scene
        .node
        .submit_task_create(
            ActorId::new(3).unwrap(),
            MonotonicInstant::from_origin(Duration::from_secs(100)),
            payload,
        )
        .map_err(|original| Box::new(original) as Failure)?;
    let Some(delivery) = wait_delivery(&mut handle) else {
        return Err(Box::new(("Create delivery timeout", handle)));
    };
    if !matches!(
        delivery.body(),
        novarocks_worker_protocol::context::ContextReplyBody::Create {
            outcome: OperationOutcome::Accepted, receipt,
        } if receipt.identity() == task
    ) {
        return Err(Box::new(("actual Create refusal", delivery, handle)));
    }
    drop(delivery);
    drop(handle);
    if !wait_until(|| {
        let reporter = resources.host.observed.reporter.lock().unwrap().clone();
        resources.host.observed.activated.load(Ordering::SeqCst) == 1
            && resources.host.observed.completed.load(Ordering::SeqCst) == 1
            && reporter.is_some_and(|reporter| {
                reporter
                    .current()
                    .is_ok_and(|status| status.installed() && status.state() == TaskState::Finished)
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
        scene.node.print_neutral_task_inventory(context, task);
        return Err(Box::new(
            "real execution / installed / preparation whole-exit timeout",
        ));
    }
    if !read_root {
        // The compiled producer and original Completion really finished, but
        // the client never reads or acknowledges this Root. Its explicit Abort
        // must seal it and observe actual physical exit; Release cannot discard
        // unacknowledged results.
        // No synthetic RootDrained or terminal/cancellation fact is supplied.
        return Ok(());
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
            context,
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
            context,
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
    // Native successful Context handoff already completed this Task before
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
    Ok(())
}
fn shutdown(resources: TaskResources) -> Result<(), Failure> {
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
#[test]
fn real_node_compiled_task_runs_and_finishes_on_one_and_two_pollers() {
    let policy = ControlRuntimePolicy::try_new(IngressRoleLimits {
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
    let geometry = policy
        .process_task_storage(1, 1)
        .unwrap()
        .resources()
        .unwrap()
        .checked_add(
            &ResourceVector::new([
                (Dimension::ActiveTasks.id(), 1),
                (Dimension::PreparingTasks.id(), 1),
                (Dimension::PreparingBytes.id(), preparation_bytes),
            ])
            .unwrap(),
        )
        .unwrap();
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
