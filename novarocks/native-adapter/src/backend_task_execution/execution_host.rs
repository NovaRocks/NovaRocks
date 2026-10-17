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

//! The native backend task side of execution: one frozen descriptor becomes one
//! prepared, then running, fragment on this backend process.
//!
//! The owner in [`super::registry`] calls the four install steps in commit
//! order and undoes them in reverse, so this host's whole job is to make each
//! step separately undoable:
//!
//! ```text
//! install_receiver          decode + prepare_fragment -> dormant handle
//! install_inbound_capability register the descriptor for frame admission
//! apply_task_domain         initial splits / edge opens / filters
//! submit_runnable           reserve completion and return a dormant runnable
//! commit_creation           start and attach after Worker installs Live
//! ```
//!
//! The dormant handle is what makes that split honest. Receiver registration
//! is not a step the fragment kernel exposes on its own: it happens inside
//! `prepare_fragment`, which also builds the pipeline and hands back a
//! [`DormantFragmentHandle`]. So `install_receiver` prepares the whole
//! fragment and parks the handle, transferring its physical receivers into
//! the task's retirement owner. `remove_receiver` synchronously retires those
//! receivers under the Worker's inbound gate, including when another task
//! handle still exists. The Worker's Live commit starts the prepared execution.
//!
//! This host reads nothing from the fragment-based lifecycle registry. Every
//! query-scoped fact it needs arrives through [`TaskQueryContextFacts`], which
//! the query context half of execution implements.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use novarocks_execution::connector::{
    SplitQueueConfig, SplitQueueError, SplitQueueErrorKind, SplitQueueRegistry,
    SplitSequenceEvidence, TaskAttemptKey, TaskAttemptSplitQueues,
};
use novarocks_execution::exec::fragment::program::{FragmentNodeId, FragmentSinkKind};
use novarocks_execution::exec::node::scan::BoundScanRanges;
use novarocks_execution::runtime::execution_runtime::ExecutionRuntime;
use novarocks_execution::runtime::fragment::io::{
    ExchangeDestinationKey, ExchangeEdgeGates, ExchangeFrameTransmitter, ExchangeReceiverPort,
    FragmentCommitPort, FragmentEvent, FragmentEventSink, FragmentResultWriter,
};
use novarocks_execution::runtime::fragment::scan::compiled_scan_nodes;
use novarocks_execution::runtime::fragment::{
    CompiledFragmentSubmission, DormantFragmentHandle, ExchangeReceiverRetirement,
    ExecutionFailure, ExecutionFailureCause, FragmentCancelReason, FragmentExecutionError,
    FragmentInstanceSpec, FragmentLaunchError, FragmentOutcome, FragmentRuntimeOptions,
    FragmentTerminalFact, RunningFragmentHandle, ScanAssignments, compiled_sink_kind,
    prepare_compiled_fragment, prepare_fragment,
};
use novarocks_execution::runtime::operator_statistics::project_operator_statistics;
use novarocks_execution::runtime::profile::{Profiler, RuntimeProfileTree, fragment_root_profiler};
use novarocks_execution::runtime_filter::RuntimeFilterSessionRef;
use novarocks_execution_contract::task_execution::creation::{
    PreparedTaskFacts, TaskCreationInput,
};
use novarocks_execution_contract::task_execution::descriptor::TaskDescriptor;
use novarocks_execution_contract::task_execution::domain::{CodecOwnedContent, DomainVersion};
use novarocks_execution_contract::task_execution::identity::{QueryContextRef, TaskIdentity};
use novarocks_execution_contract::task_execution::operation::TaskDomainUpdate;
use novarocks_execution_contract::task_execution::status::{
    AbortCause, CancelReason, SafeDetail, TaskFailure, TaskFailureCategory, TaskOutputFacts,
    TaskState, TerminationDetail,
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_codec::connector_read::{
    ConnectorReadDecoder, MAX_ASSIGNMENT_RETAINED_BYTES, SplitAssignment,
};
use novarocks_proto_models::connector_read as connector_dto;
use novarocks_spi::connector::{
    CatalogHandle, ConnectorExecutionReadBinding, ConnectorExecutionWriteBinding,
    ConnectorStopOwner, ConnectorStorageResolver, read_stack::ConnectorSession,
};
use novarocks_task_codec::creation::{
    decode_static_fragment, decode_static_package, take_task_assignment,
};
use novarocks_task_codec::domain::stored_message;
use novarocks_types::{QueryExecutionId, UniqueId};
use novarocks_worker::{TaskCompletionSignal, TaskCompletionSupervisor, TaskInboundCapabilities};
use tracing::debug;

use super::compiled_package::{
    CompiledPackageCompiler, CompiledPackageError, CompiledTaskOptions, CompiledTaskProgram,
    TaskPreparationControl,
};
use crate::compiled_runtime_filter::program_runtime_filter_bindings;
use crate::compiled_scan_binding::{CompiledScanTask, bind_compiled_scans};
use crate::compiled_writer_binding::{CompiledWriteTask, bind_compiled_writers};
use crate::fragment_instance::{NativeFragmentInstanceInput, project_task_instance};
use crate::fragment_request::NativeFragmentRequest;
use crate::fragment_submission::compiled_fragment_sink_assignment;
use crate::native_fragment_query::{NativeFragmentQueryRuntime, NativeFragmentRegistrationLease};
use crate::root_result_session::{NativeRootResultSession, RootProducerPool};
#[cfg(novarocks_dev_controls)]
use crate::task_protocol_fault as fault;
use novarocks_local_program::LocalProgram;
use novarocks_worker::read_attempt::{ReceivedReadSplit, TypedReadAttemptContext};
use novarocks_worker::{
    CatalogReadExecutionResolver, CatalogWriteExecutionResolver, HostRejection, RunnableTask,
    RuntimeFilterSessionResolver, TaskExecutionHost, TaskStatusReporter, TypedScanRuntime,
};

/// Everything one query context contributes to preparing and running a task.
///
/// A task's plan names catalogs, filter channels, and object stores that
/// belong to the *query*, not to the task. The old stack reached into the
/// fragment lifecycle registry for each of them, which is what made a fragment
/// depend on the lifecycle owner's admission permit. Here they are one
/// injected port, implemented by the query-context half of execution, so the
/// task side holds no query-wide authority of its own.
pub use crate::task_query_context_options::QueryContextOptions;

pub trait TaskQueryContextFacts: Send + Sync {
    /// The immutable execution options installed by this query context.
    fn query_options(
        &self,
        execution: QueryExecutionId,
    ) -> Result<QueryContextOptions, HostRejection>;

    /// The runtime-filter session this query installed on this backend.
    ///
    /// `None` is the ordinary answer for a query with no filter participant.
    /// `expects_bindings` says whether this task's plan actually binds a
    /// filter, so a task that needs one and finds none fails here rather than
    /// silently reading unfiltered.
    fn runtime_filter_session(
        &self,
        execution: QueryExecutionId,
        fragment_instance_id: UniqueId,
        expects_bindings: bool,
    ) -> Result<Option<RuntimeFilterSessionRef>, HostRejection>;

    /// The runtime-filter session of a task whose compiled program binds
    /// exactly `bindings`, by plan-global binding identity.
    ///
    /// The participant this query installed on this backend must host exactly
    /// these bindings for this fragment instance; any difference in either
    /// direction refuses the task. An empty set with no hosted binding is the
    /// ordinary answer `None`.
    fn runtime_filter_session_for_bindings(
        &self,
        execution: QueryExecutionId,
        fragment_instance_id: UniqueId,
        bindings: &BTreeSet<u32>,
    ) -> Result<Option<RuntimeFilterSessionRef>, HostRejection>;

    /// Where a running fragment's runtime-filter evidence goes.
    ///
    /// Row effects and scan-unit outcomes are folded by the participant that
    /// owns the installed consumer identity, and that participant belongs to
    /// the query context.
    fn runtime_filter_event_sink(
        &self,
        execution: QueryExecutionId,
        fragment_instance_id: UniqueId,
    ) -> Arc<dyn FragmentEventSink>;

    /// Offers this task as the query context's dynamic filter carrier.
    ///
    /// The filter participant is the query context's, but a status version and
    /// a retained payload are a task's, so one task has to carry the context's
    /// terminal feedback to the frontend. The first submitted task of a context
    /// that installed a participant takes the role and keeps it; later ones are
    /// told `false`. A query with no filter participant on this backend also
    /// answers `false`, which is the ordinary case and not a failure.
    fn bind_runtime_filter_feedback(
        &self,
        execution: QueryExecutionId,
        carrier: TaskIdentity,
        reporter: &TaskStatusReporter,
    ) -> bool;

    /// Delivers one task-scoped dynamic filter the frontend pushed down.
    fn deliver_task_dynamic_filter(
        &self,
        execution: QueryExecutionId,
        fragment_instance_id: UniqueId,
        version: DomainVersion,
        payload: &Arc<dyn CodecOwnedContent>,
    ) -> Result<(), HostRejection>;

    /// Binds one catalog for connector read execution inside this query.
    fn catalog_read_execution(
        &self,
        execution: QueryExecutionId,
        handle: &CatalogHandle,
    ) -> Result<ConnectorExecutionReadBinding, String>;

    /// Binds one catalog for connector write execution inside this query.
    fn catalog_write_execution(
        &self,
        execution: QueryExecutionId,
        handle: &CatalogHandle,
    ) -> Result<ConnectorExecutionWriteBinding, String>;

    /// The authorized object-store access this query's reads and writes use.
    ///
    /// A context that was never established, or was already released, has no
    /// resolver at all. That refuses the task's preparation rather than
    /// deferring to read time, because there is no process-level credential a
    /// scan could legitimately fall back to.
    fn storage_resolver(
        &self,
        execution: QueryExecutionId,
    ) -> Result<Arc<dyn ConnectorStorageResolver>, HostRejection>;
}

/// The production [`TaskExecutionHost`].
pub struct NativeTaskExecutionHost {
    queries: NativeFragmentQueryRuntime,
    context_facts: Arc<dyn TaskQueryContextFacts>,
    capabilities: Arc<TaskInboundCapabilities>,
    exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
    result_writer: Arc<dyn FragmentResultWriter>,
    result_retained_budget: Arc<novarocks_worker::result_buffer::ResultRetainedBudget>,
    result_retained_limits: novarocks_worker::WorkerResultRetainedLimits,
    root_producer_pool: Arc<RootProducerPool>,
    exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
    commit_port: Arc<dyn FragmentCommitPort>,
    execution_runtime: Arc<ExecutionRuntime>,
    scan_stream_host: novarocks_worker::ScanStreamHost,
    completion_supervisor: Arc<TaskCompletionSupervisor>,
    /// Split delivery, keyed by execution and kernel key so a replaced attempt
    /// gets a fresh queue set and can never inherit a sequence space.
    split_queues: Arc<SplitQueueRegistry<ReceivedReadSplit>>,
    tasks: Mutex<HashMap<TaskIdentity, Arc<TaskRuntime>>>,
    /// The one static plan interpreter this process composed: the plan-tree
    /// decoder when absent, otherwise the compiled-package compiler. A task's
    /// carrier never selects it.
    compiled: Option<Arc<dyn CompiledPackageCompiler>>,
    #[cfg(novarocks_dev_controls)]
    test_controls: Option<Arc<crate::test_controls::NativeTestControls>>,
}

impl fmt::Debug for NativeTaskExecutionHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeTaskExecutionHost")
            .field(
                "prepared_tasks",
                &self.tasks.lock().map(|tasks| tasks.len()),
            )
            .finish_non_exhaustive()
    }
}

/// One task's execution-side state, from `install_receiver` to
/// `remove_receiver`.
struct TaskRuntime {
    attempt: TaskAttemptKey,
    stop: ConnectorStopOwner,
    sink_kind: FragmentSinkKind,
    /// Only retained output survives task execution. A Session here would
    /// keep its metadata live while context release waits for that same exit.
    root_channel: Option<Arc<novarocks_worker::root_result_channel::RootResultChannel>>,
    /// Taken exactly once, by `submit_runnable`. While it is still here the
    /// fragment is prepared but not started, and dropping it rolls every
    /// acquired resource back.
    dormant: Mutex<Option<DormantFragmentHandle>>,
    /// Physical ingress survives actual driver stop until the original
    /// capability owner closes its route. Taken synchronously on retirement.
    exchange_retirement: Mutex<Option<ExchangeReceiverRetirement>>,
    edges: Arc<ExchangeEdgeGates>,
    splits: Arc<TaskAttemptSplitQueues<ReceivedReadSplit>>,
    #[cfg(novarocks_dev_controls)]
    replay_gate: Mutex<Option<Arc<super::task_update_replay_gate::TaskUpdateReplayGate>>>,
    read_context: Arc<TypedReadAttemptContext>,
    /// Retains preparation accounting until activation or receiver rollback.
    registration: Mutex<Option<NativeFragmentRegistrationLease>>,
    /// This task's metrics owner, retained so `submit_runnable` can hand it
    /// the status reporter that only exists once the task is runnable.
    operator_statistics: Arc<TaskOperatorStatisticsSink>,
}

/// A failed preparation must wake any provider work before resources roll
/// back; an installed task transfers stop responsibility to `remove_receiver`
/// and the runnable's stand-down latch.
struct PreparationStopGuard(Option<ConnectorStopOwner>);

impl PreparationStopGuard {
    fn new(stop: ConnectorStopOwner) -> Self {
        Self(Some(stop))
    }

    fn transfer_to_task(&mut self) {
        self.0 = None;
    }
}

impl Drop for PreparationStopGuard {
    fn drop(&mut self) {
        if let Some(stop) = self.0.take() {
            stop.request_stop();
        }
    }
}

/// Delivers one fragment's events to every owner that has a claim on them.
///
/// A task's events answer to two owners at once: the query context folds
/// runtime-filter evidence under the installed consumer identity, and the task
/// itself owns its metrics. Neither may see the other's facts, and neither may
/// be the one that decides the other's delivery, so the fragment is handed a
/// fan-out rather than a sink that quietly does both jobs.
struct CompositeFragmentEventSink {
    sinks: Vec<Arc<dyn FragmentEventSink>>,
}

impl CompositeFragmentEventSink {
    fn new(sinks: Vec<Arc<dyn FragmentEventSink>>) -> Self {
        Self { sinks }
    }
}

impl FragmentEventSink for CompositeFragmentEventSink {
    fn record(&self, event: FragmentEvent) {
        for sink in &self.sinks {
            sink.record(event.clone());
        }
    }
}

/// The task's own metrics owner: it turns this fragment's profile into the
/// operator statistics its final info carries.
///
/// The reporter is bound late because it does not exist yet when the fragment
/// is prepared: the creation transaction mints it only for `submit_runnable`,
/// the last install step. Until then this sink has nothing to report onto and
/// says so by dropping the event, rather than buffering statistics for a task
/// that may never become runnable.
struct TaskOperatorStatisticsSink {
    fragment_instance_id: UniqueId,
    /// `None` when the query did not ask for a profile. Without one there is
    /// no counter to read, which is a different statement from an operator
    /// that ran and counted nothing.
    profiler: Option<Profiler>,
    reporter: OnceLock<TaskStatusReporter>,
}

impl TaskOperatorStatisticsSink {
    fn new(fragment_instance_id: UniqueId, profiler: Option<Profiler>) -> Self {
        Self {
            fragment_instance_id,
            profiler,
            reporter: OnceLock::new(),
        }
    }

    /// Binds the status reporter this task's statistics are recorded onto.
    fn bind(&self, reporter: TaskStatusReporter) {
        // Set once: a second reporter would address a second task, and this
        // sink belongs to exactly one.
        let _ = self.reporter.set(reporter);
    }

    /// Records the operator statistics one profile tree attributes.
    ///
    /// The reporter owns the entry bound and its truncation marker, so the
    /// whole attributed list is handed over rather than pre-trimmed here.
    fn record_profile(&self, profile: &RuntimeProfileTree) {
        let Some(reporter) = self.reporter.get() else {
            return;
        };
        let projection = project_operator_statistics(profile);
        if projection.unattributed_operators() > 0 {
            // Named, not folded: these operators belong to no plan node, so
            // there is no honest node to report them under.
            debug!(
                "fragment {} profile carries {} operator(s) with no plan node; they are not \
                 reported as operator statistics",
                self.fragment_instance_id,
                projection.unattributed_operators()
            );
        }
        reporter.record_operator_statistics(projection.into_statistics());
    }

    /// Records from the profiler this sink holds, if the query asked for one.
    fn record_current(&self) {
        if let Some(profiler) = self.profiler.as_ref() {
            self.record_profile(&profiler.to_native_tree());
        }
    }
}

impl FragmentEventSink for TaskOperatorStatisticsSink {
    fn record(&self, event: FragmentEvent) {
        match event {
            // A snapshot carries its own tree, so it is reported as given
            // rather than re-read from this sink's profiler: the two are not
            // required to be the same profile.
            FragmentEvent::ProfileSnapshot(snapshot) => {
                debug_assert_eq!(snapshot.fragment_instance_id(), self.fragment_instance_id);
                self.record_profile(snapshot.profile());
            }
            // Progress is the sampling tick. It carries whole-fragment totals,
            // which say nothing per operator, so the tick is used only as the
            // moment to read the operators' own counters.
            FragmentEvent::Progress(progress) => {
                debug_assert_eq!(progress.fragment_instance_id(), self.fragment_instance_id);
                self.record_current();
            }
            // Runtime-filter evidence answers to the query context's filter
            // participant. Folding it here would put two unrelated facts
            // behind one identity.
            FragmentEvent::RuntimeFilterRowEffect(_) => {}
        }
    }
}

const TASK_LOCK: &str = "native task execution host lock";
const DORMANT_LOCK: &str = "dormant fragment handle lock";

impl NativeTaskExecutionHost {
    #[expect(
        clippy::too_many_arguments,
        reason = "Every execution port this host drives is injected explicitly; \
                  a bundle struct would only move the same list one level away."
    )]
    pub(crate) fn new(
        queries: NativeFragmentQueryRuntime,
        context_facts: Arc<dyn TaskQueryContextFacts>,
        capabilities: Arc<TaskInboundCapabilities>,
        exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
        result_writer: Arc<dyn FragmentResultWriter>,
        result_retained_budget: Arc<novarocks_worker::result_buffer::ResultRetainedBudget>,
        result_retained_limits: novarocks_worker::WorkerResultRetainedLimits,
        root_producer_pool: Arc<RootProducerPool>,
        exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
        commit_port: Arc<dyn FragmentCommitPort>,
        execution_runtime: Arc<ExecutionRuntime>,
        scan_stream_host: novarocks_worker::ScanStreamHost,
        completion_supervisor: Arc<TaskCompletionSupervisor>,
    ) -> Self {
        Self {
            queries,
            context_facts,
            capabilities,
            exchange_transmitter,
            result_writer,
            result_retained_budget,
            result_retained_limits,
            root_producer_pool,
            exchange_receiver_port,
            commit_port,
            execution_runtime,
            scan_stream_host,
            completion_supervisor,
            split_queues: Arc::new(SplitQueueRegistry::new()),
            tasks: Mutex::new(HashMap::new()),
            compiled: None,
            #[cfg(novarocks_dev_controls)]
            test_controls: None,
        }
    }

    #[cfg(novarocks_dev_controls)]
    pub(crate) fn with_test_controls(
        mut self,
        controls: Arc<crate::test_controls::NativeTestControls>,
    ) -> Self {
        self.test_controls = Some(controls);
        self
    }

    /// Composes the compiled-package interpreter instead of the plan-tree
    /// decoder. Composition makes this choice once per process.
    pub(crate) fn with_compiled_package_compiler(
        mut self,
        compiler: Arc<dyn CompiledPackageCompiler>,
    ) -> Self {
        self.compiled = Some(compiler);
        self
    }

    fn open_root_result_session(
        &self,
        identity: TaskIdentity,
        sink: Option<&novarocks_local_program::StaticSinkProgram>,
        statistics_source_is_unpivot: bool,
    ) -> Result<Option<Arc<NativeRootResultSession>>, HostRejection> {
        let root_session = match sink {
            Some(novarocks_local_program::StaticSinkProgram::RootResult(contract)) => {
                // Every closed internal domain has its BE producer; only a
                // scalar domain identity without its typed schema has none.
                if contract.validate_purpose().is_err() {
                    return Err(protocol("explicit internal root codec is not installed"));
                }
                if contract.kind()
                    == novarocks_result_contract::RootOutputKind::InternalFacts(
                        novarocks_result_contract::InternalResultDomain::StatisticsArtifactV1,
                    )
                    && !statistics_source_is_unpivot
                {
                    return Err(protocol(
                        "StatisticsArtifactV1 requires its bounded final Unpivot source",
                    ));
                }
                // V1 input/hydrate coexistence uses the frozen joint root
                // allowance. Legacy Arrow results keep their original limit;
                // both paths still debit the same process budget below.
                let bounded_limits = novarocks_worker::WorkerResultRetainedLimits::try_new(
                    usize::try_from(
                        novarocks_execution_contract::native_result_support::NativeResultSupportGeometry::V1
                            .root_joint_retained_bytes_per_root,
                    )
                    .map_err(|_| resource_exhausted("bounded root limit exceeds the target"))?,
                    self.result_retained_limits.per_process().get(),
                )
                .map_err(|error| resource_exhausted(format!("bind bounded root capacity: {error}")))?;
                let channel = novarocks_worker::root_result_channel::RootResultChannel::try_open(
                    novarocks_execution::runtime::fragment::io::RootResultWriteSpec {
                        task: identity,
                        contract: Arc::clone(contract),
                    },
                    Arc::clone(&self.result_retained_budget),
                    bounded_limits,
                )
                .map_err(|error| {
                    resource_exhausted(format!("open task {identity} root channel: {error}"))
                })?;
                Some(
                    NativeRootResultSession::try_open(channel, &self.root_producer_pool).map_err(
                        |error| {
                            resource_exhausted(format!(
                                "open task {identity} root producer: {error}"
                            ))
                        },
                    )?,
                )
            }
            _ => None,
        };
        Ok(root_session)
    }

    /// The compiled-package half of `install_receiver`: the task's package is
    /// received, provider-validated and compiled into a LocalProgram, then
    /// prepared into the same dormant handle and task runtime as a plan-tree
    /// task. The original registration lease protects early query funding and
    /// rolls back on every refusal before installation.
    fn install_compiled_receiver(
        &self,
        descriptor: &TaskDescriptor,
        input: TaskCreationInput,
        compiler: &dyn CompiledPackageCompiler,
        preparation_stop: novarocks_worker::TaskPreparationStopView,
    ) -> Result<novarocks_worker::PreparedTaskInstallation, HostRejection> {
        let identity = descriptor.identity();
        let execution = identity.query_execution_id();
        let kernel_key = descriptor.fragment_instance_id();
        let (static_fragment, assignment) = input.into_parts();
        let carrier = decode_static_package(&static_fragment, FieldPath::root("frozen_fragment"))
            .map_err(|error| {
            protocol(format!(
                "task {identity} static package is not decodable: {error}"
            ))
        })?;
        drop(static_fragment);
        let assignment = take_task_assignment(assignment).ok_or_else(|| {
            internal(format!(
                "task {identity} creation assignment is not a codec-produced assignment"
            ))
        })?;

        let context_options = self.context_facts.query_options(execution)?;
        let (delivery_expire, query_expire) =
            novarocks_execution::runtime::query_options::query_expire_durations(Some(
                context_options.runtime().as_ref(),
            ));
        // The original fragment lease protects the early SAME query account.
        // A refused decoder drops only this fragment's registration.
        let registration = self
            .queries
            .register_fragment_execution(execution, kernel_key, delivery_expire, query_expire)
            .map_err(|error| {
                resource_exhausted(format!("task {identity} could not be registered: {error}"))
            })?;
        let memory = self
            .queries
            .prepare_query_memory_typed(
                execution,
                delivery_expire,
                query_expire,
                context_options.runtime().exec_mem_limit(),
            )
            .map_err(|error| memory_admission_failure_to_host(identity, &error))?;
        memory
            .binding()
            .validate_task(identity, self.execution_runtime.memory_authority())
            .map_err(|error| {
                resource_exhausted(format!(
                    "task {identity} query memory does not bind: {error}"
                ))
            })?;

        let attempt = TaskAttemptKey::new(execution, kernel_key);
        let splits = self.split_queues.open_attempt(
            attempt,
            SplitQueueConfig {
                max_queued_bytes: MAX_ASSIGNMENT_RETAINED_BYTES,
            },
        );
        // The queue set is opened before the scans bind so a scan can start
        // and block before its first split arrives; the lease rolls it back.
        let mut lease = SplitQueueLease::held(&self.split_queues, attempt);
        let task_stop = ConnectorStopOwner::new();
        let mut stop_guard = PreparationStopGuard::new(task_stop.clone());
        let read_context = Arc::new(TypedReadAttemptContext::new());
        let typed_runtime = self.typed_scan_runtime(
            execution,
            kernel_key,
            Arc::clone(&read_context),
            Arc::clone(&splits),
        )?;

        let preparation = preparation_stop.borrow_control();
        let control = TaskPreparationControl::new(task_stop.view(), preparation_stop.clone());
        let type_tracker = self
            .queries
            .type_construction_tracker(execution, context_options.runtime().exec_mem_limit())
            .map_err(|error| memory_admission_failure_to_host(identity, &error))?;
        let type_creation = crate::native_type_construction::NativeTypeConstruction::try_new(
            type_tracker,
            task_stop.view(),
            preparation_stop.clone(),
        )
        .map_err(|error| native_type_construction_failure_to_host(identity, &error))?;
        let type_scopes = super::compiled_package::CompiledTypeScopes::new(&type_creation);
        let mut request = |layout| type_creation.request_layout(layout);
        let mut opaque = |bytes| type_creation.request_opaque_upper_bound(bytes);
        let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
            type_creation.storage_lifetime(),
            &mut request,
            &mut opaque,
        );
        let mut metadata_journal = super::TypeMaterializationJournal::default();
        let mut type_host = super::TypeMaterializationHost::new(
            memory.binding(),
            &preparation,
            &mut metadata_journal,
        );
        let mut project_journal = super::ProjectMetadataJournal::default();
        let mut project_host =
            super::ProjectMetadataHost::new(memory.binding(), &preparation, &mut project_journal);
        let compiled = compiler.compile_with_funding(
            carrier.package(),
            CompiledTaskOptions {
                pipeline_dop: descriptor.pipeline_dop(),
                exchange_wait: Duration::from_millis(
                    self.execution_runtime.config().exchange_wait_ms,
                ),
            },
            &mut creation,
            &type_scopes,
            &control,
            &mut type_host,
            &mut project_host,
        );
        // Fixed local facts only: no metadata text or query-id metric labels.
        tracing::debug!(resource_scope = "be-type-materialization", receipt = ?metadata_journal, "type materialization resource receipt");
        tracing::debug!(resource_scope = "project-compile-metadata", receipt = ?project_journal, "Project compilation resource receipt");
        let compiled = compiled.map_err(|error| compiled_package_rejection(identity, error))?;
        let program = compiled.program();
        let runtime_filters = &compiled.runtime_filters;
        // The bindings this program's runtime-filter sites bind. They decide
        // whether the task needs the query context's filter session, exactly
        // as a plan-tree program's bindings do, and the session must host
        // exactly them for this instance.
        let runtime_filter_bindings =
            program_runtime_filter_bindings(program.graph().requirements(), &runtime_filters)
                .map_err(|error| {
                    protocol(format!(
                        "task {identity} runtime-filter bindings do not match its package: {error}"
                    ))
                })?;
        let sink = program.graph().sink().ok_or_else(|| {
            internal(format!(
                "task {identity} compiled program has no static sink"
            ))
        })?;
        let sink_kind = compiled_sink_kind(Some(sink))
            .map_err(|error| internal(format!("task {identity} compiled sink: {error}")))?;

        let instance_input = project_task_instance(
            descriptor,
            assignment
                .into_wire()
                .map_err(|error| internal(format!("restore validated task assignment: {error}")))?,
            context_options.runtime().as_ref().clone(),
            sink_kind,
        )
        .map_err(|error| {
            protocol(format!(
                "task {identity} assignment is not a legal instance: {error}"
            ))
        })?;
        let scan_assignments =
            compiled_scan_assignments(identity, descriptor, &program, &instance_input)?;
        // Every compiled scan binds its typed source here, before anything is
        // published: a refusal rolls back this attempt's lease and local state.
        let scans = bind_compiled_scans(
            &program,
            &CompiledScanTask {
                runtime: &typed_runtime,
                fragment_instance_id: kernel_key,
                query_options: &instance_input.query_options,
                stop: task_stop.view(),
                runtime_filters: &runtime_filters,
                #[cfg(novarocks_dev_controls)]
                observation_controls: self.queries.observation_controls(),
            },
        )
        .map_err(|error| protocol(format!("task {identity} scan does not bind: {error}")))?;
        // Every compiled writer binds its write capability and every finish
        // its carrier validator here, before anything is published; a write
        // execution is opened only when a driver activates.
        let writers = bind_compiled_writers(
            &program,
            &CompiledWriteTask {
                #[cfg(novarocks_dev_controls)]
                test_controls: self.test_controls.clone(),
                #[cfg(novarocks_dev_controls)]
                observation_controls: self.queries.observation_controls(),
                backend_process_id: identity.backend_process_id(),
                runtime: &typed_runtime,
                fragment_instance_id: kernel_key,
                query_options: &instance_input.query_options,
                stop: task_stop.view(),
            },
        )
        .map_err(|error| protocol(format!("task {identity} writer does not bind: {error}")))?;
        let sink_assignment = compiled_fragment_sink_assignment(
            sink,
            &instance_input.sink_edge_ids,
            kernel_key,
            descriptor.topology(),
        )
        .map_err(|error| {
            protocol(format!(
                "task {identity} sink edges do not bind its compiled sink: {error}"
            ))
        })?;
        let instance = FragmentInstanceSpec::new_native(
            novarocks_execution_contract::FragmentContractVersion::CURRENT,
            instance_input.query_id,
            instance_input.fragment_instance_id,
            scan_assignments,
            instance_input.exchange_inputs,
            sink_assignment,
            FragmentRuntimeOptions::new(
                instance_input.query_options,
                instance_input.typed_result_sink,
            ),
            instance_input.pipeline_dop,
            instance_input.backend_num,
        );
        let root_session = self.open_root_result_session(
            identity,
            program.graph().sink(),
            matches!(
                program.graph().nodes()[program.graph().root().index()].kind(),
                novarocks_local_program::ProgramNodeKind::Unpivot { .. }
            ),
        )?;
        let root_channel = root_session
            .as_ref()
            .map(|session| Arc::clone(session.channel()));
        let root_display_id = i32::try_from(program.graph().root().index())
            .map_err(|_| internal(format!("task {identity} compiled root index exceeds i32")))?;
        // This is the original Arc birth. Submission and the private loan
        // borrow this same immutable backing, with no recreated program.
        let compiled = compiled.into_preparation();
        let submission = CompiledFragmentSubmission::try_new_with_writers(
            Arc::clone(compiled.program()),
            scans,
            writers,
            instance,
        )
        .map_err(|error| {
            protocol(format!(
                "task {identity} compiled program does not fit its instance: {error}"
            ))
        })?;

        let profiler = context_options
            .runtime()
            .enable_profile()
            .then(|| fragment_root_profiler(root_display_id));
        let edges = ExchangeEdgeGates::from_frozen_edges(descriptor.topology().outbound())
            .map_err(|error| {
                protocol(format!(
                    "task {identity} outbound topology has no legal gate set: {error}"
                ))
            })?;
        let operator_statistics = Arc::new(TaskOperatorStatisticsSink::new(
            kernel_key,
            profiler.clone(),
        ));
        let event_sink: Arc<dyn FragmentEventSink> =
            Arc::new(CompositeFragmentEventSink::new(vec![
                self.context_facts
                    .runtime_filter_event_sink(execution, kernel_key),
                Arc::clone(&operator_statistics) as Arc<dyn FragmentEventSink>,
            ]));
        // Resolved before installation, so a task whose bindings differ from
        // what its query context installed for it is refused with nothing to
        // roll back but this attempt's local state.
        let runtime_filter = self.context_facts.runtime_filter_session_for_bindings(
            execution,
            kernel_key,
            &runtime_filter_bindings,
        )?;
        let admission =
            self.queries
                .prepare_admission_with_memory(kernel_key, memory, runtime_filter);
        // Scan sources open their providers only when preparation binds them,
        // which needs the admitted fragment tracker installed first.
        typed_runtime
            .install_connector_resource_tracker(admission.fragment_mem_tracker())
            .map_err(|error| {
                internal(format!(
                    "task {identity} could not install connector resource accounting: {error}"
                ))
            })?;
        // A binding clone borrows the same authority/account as the original
        // context transport; it creates no new query registration or domain.
        let project_binding = admission
            .query_memory()
            .cloned()
            .expect("validated preparation memory reaches admission");
        let mut context = admission
            .into_prepare_context(
                profiler,
                Arc::clone(&self.exchange_transmitter),
                Arc::clone(&self.result_writer),
                event_sink,
            )
            .with_fragment_commit_port(Arc::clone(&self.commit_port))
            .with_exchange_receiver_port(Arc::clone(&self.exchange_receiver_port))
            .with_registration_owned_exchange_receivers()
            .with_execution_runtime(Arc::clone(&self.execution_runtime))
            .with_result_identity(identity)
            .with_edge_gates(Arc::clone(&edges));
        #[cfg(novarocks_dev_controls)]
        if let Some(observations) = self.queries.observation_controls() {
            if let Some(observer) = observations
                .observation_for("exchange-snapshot", execution)
                .map_err(internal)?
            {
                let receiver = novarocks_execution::runtime::fragment::io::exchange_receiver::ExecutionRuntimeExchangeReceiverPort::new(self.execution_runtime.clone()).with_test_observer(observer.clone());
                context = context
                    .with_exchange_test_observer(Some(observer))
                    .with_exchange_receiver_port(Arc::new(receiver));
            }
        }
        if let Some(session) = root_session {
            context = context.with_root_result_session(session);
        }
        let prepared = match compiled.native_metadata_loan() {
            Some(source) => {
                let mut host = super::project_materialization::CompiledProjectMetadataHost::new(
                    source,
                    &project_binding,
                    &preparation,
                    &control,
                    &mut project_journal,
                );
                novarocks_execution::runtime::fragment::prepare_compiled_fragment_with_metadata_host(
                    submission, context, &mut host,
                )
            }
            // Independently assembled products use their original route before
            // any bounded-native author or preparation metadata admission.
            None => prepare_compiled_fragment(submission, context),
        };
        tracing::debug!(resource_scope = "project-prepare-metadata", receipt = ?project_journal, "Project preparation resource receipt");
        let mut dormant =
            prepared.map_err(|error| preparation_failure_to_host(identity, &error))?;
        let exchange_retirement = dormant.take_exchange_retirement();

        #[cfg(novarocks_dev_controls)]
        crate::task_execution_observation::emit_prepared_task_dop(
            self.queries.observation_controls().as_deref(),
            identity,
            descriptor.fragment_instance_id(),
            descriptor.pipeline_dop().get(),
        );
        let installation = novarocks_worker::PreparedTaskInstallation::new(
            PreparedTaskFacts::new(sink_kind),
            root_channel.clone(),
        )?;
        if let Some(channel) = &root_channel {
            #[cfg(novarocks_dev_controls)]
            crate::task_execution_observation::emit_prepared_client_root(
                self.queries.observation_controls().as_deref(),
                identity,
                &channel.spec().contract,
            );
        }
        self.tasks.lock().expect(TASK_LOCK).insert(
            identity,
            Arc::new(TaskRuntime {
                attempt,
                stop: task_stop,
                sink_kind,
                dormant: Mutex::new(Some(dormant)),
                exchange_retirement: Mutex::new(exchange_retirement),
                edges,
                splits,
                #[cfg(novarocks_dev_controls)]
                replay_gate: Mutex::new(None),
                read_context,
                registration: Mutex::new(Some(registration)),
                operator_statistics,
                root_channel,
            }),
        );
        lease.retain();
        stop_guard.transfer_to_task();
        Ok(installation)
    }

    fn task_runtime(&self, identity: TaskIdentity) -> Option<Arc<TaskRuntime>> {
        self.tasks.lock().expect(TASK_LOCK).get(&identity).cloned()
    }

    /// Assembles the runtime inputs a typed connector scan needs.
    ///
    /// The queue set is opened here rather than on first delivery so a task's
    /// scan can start, and block, before any split has arrived.
    fn typed_scan_runtime(
        &self,
        execution: QueryExecutionId,
        fragment_instance_id: UniqueId,
        read_context: Arc<TypedReadAttemptContext>,
        splits: Arc<TaskAttemptSplitQueues<ReceivedReadSplit>>,
    ) -> Result<TypedScanRuntime, HostRejection> {
        // Role-local and credential-free: object-store access stays with the
        // binding owner, which is the query context.
        let session = ConnectorSession::try_new(
            format!(
                "{}:{}:{}",
                execution.query_id().high(),
                execution.query_id().low(),
                execution.attempt_id().get()
            ),
            "novarocks",
            "UTC",
            "en_US",
            std::time::SystemTime::now(),
        )
        .map_err(|error| internal(format!("connector session for {execution:?}: {error}")))?;

        let facts = Arc::clone(&self.context_facts);
        let runtime_filter: RuntimeFilterSessionResolver = Arc::new(move || {
            // Resolution is deferred to bind time on purpose: decode happens
            // before the task is installed, and a scan that binds a filter
            // must see the same session the fragment was prepared with.
            facts
                .runtime_filter_session(execution, fragment_instance_id, true)
                .map_err(|error| format!("resolve task runtime-filter session: {error}"))
        });
        let facts = Arc::clone(&self.context_facts);
        let catalog_read_execution: CatalogReadExecutionResolver =
            Arc::new(move |handle| facts.catalog_read_execution(execution, handle));
        let facts = Arc::clone(&self.context_facts);
        let catalog_write_execution: CatalogWriteExecutionResolver =
            Arc::new(move |handle| facts.catalog_write_execution(execution, handle));

        Ok(TypedScanRuntime::new(
            execution,
            catalog_read_execution,
            catalog_write_execution,
            splits,
            session,
            runtime_filter,
            read_context,
            self.context_facts.storage_resolver(execution)?,
            self.scan_stream_host.clone(),
        ))
    }

    /// Enqueues one split batch into the queue of the plan node it names.
    ///
    /// Only the delivery is here. Which batches are new, which are idempotent
    /// and which conflict is the domain owner's classification, and the
    /// receipt it publishes comes from its own watermark — not from the queue.
    fn deliver_splits(
        &self,
        runtime: &TaskRuntime,
        identity: TaskIdentity,
        fragment_instance_id: UniqueId,
        assignment: &SplitAssignment,
        #[cfg(novarocks_dev_controls)] operation: Option<
            novarocks_execution_contract::TaskOperationId,
        >,
    ) -> Result<u64, HostRejection> {
        let node = assignment.plan_node_id();
        let queue = runtime.splits.queue(node);
        let sequences: Vec<SplitSequenceEvidence> = assignment
            .splits()
            .iter()
            .map(|split| SplitSequenceEvidence::new(split.sequence_id(), split.plan_node_id()))
            .collect();
        // Queue state classifies duplicates before any provider payload is
        // decoded, so a retransmission at or below the watermark never
        // recovers provider data.
        // A delivery that lands after this plan node stopped taking input is
        // moot, not illegal. The queue closes when the consumer needs nothing
        // more, which is ordinary early termination: a LIMIT that already has
        // its rows, an exchange whose downstream went away. The frontend
        // cannot know that happened, so it has done nothing wrong by still
        // delivering, and the splits are genuinely unneeded -- refusing them
        // fails a query that had already read everything it asked for.
        //
        // The answer is the node's measured depth, not an absent one: the
        // frontend refuses a receipt with no depth, correctly, because an
        // unreported depth is not zero. A closed queue can still be measured,
        // so this reports what it actually holds rather than defaulting.
        //
        // The case that stays a refusal is the frontend contradicting itself,
        // sending more splits to a node it already sealed. The queue reports
        // that separately as AfterNoMoreSplits under the Protocol category,
        // so respecting its own distinction is all this needs.
        let preflight = match queue.preflight_sequences(node, &sequences) {
            Ok(preflight) => preflight,
            Err(error) if error.kind() == SplitQueueErrorKind::Closed => {
                return Ok(queue.stats().queued_splits as u64);
            }
            Err(error) => return Err(split_queue_rejection(error)),
        };
        let new_splits: Vec<_> = assignment
            .splits()
            .iter()
            .filter(|split| !preflight.is_duplicate(split.sequence_id()))
            .collect();
        let received = if new_splits.is_empty() {
            Vec::new()
        } else {
            let binding = runtime.read_context.resolve(node).ok_or_else(|| {
                protocol(format!(
                    "split assignment names plan node {node}, which has no typed connector read execution"
                ))
            })?;
            let decoder = binding.decoder();
            new_splits
                .into_iter()
                .map(|split| {
                    decoder.decode_scheduled_split(split).map(|decoded| {
                        let (evidence, split) = decoded.into_parts();
                        ReceivedReadSplit::new(
                            evidence.sequence_id(),
                            evidence.plan_node_id(),
                            split,
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| protocol(format!("split payload is not decodable: {error}")))?
        };
        let outcome = match queue.offer_splits(node, received, assignment.no_more_splits()) {
            Ok(outcome) => outcome,
            Err(error) if error.kind() == SplitQueueErrorKind::Closed => {
                return Ok(queue.stats().queued_splits as u64);
            }
            Err(error) => return Err(split_queue_rejection(error)),
        };
        // Acceptance evidence for distributed runs: it proves a real remote
        // assignment reached this task, which a single-process smoke cannot
        // show. The retired fragment service emitted this from its own
        // delivery path; the cutover moved the work here and has to move the
        // evidence with it, or every case asserting it counts zero on a
        // cluster where splits are in fact being delivered.
        #[cfg(novarocks_dev_controls)]
        if let Some(sink) = self
            .queries
            .observation_controls()
            .as_deref()
            .map(|controls| {
                controls.observation_for("reader-marker", identity.query_execution_id())
            })
            .transpose()
            .map_err(internal)?
            .flatten()
        {
            let execution_id = identity.query_execution_id();
            let finst = fragment_instance_id;
            let duplicate_count = preflight.duplicate_sequences().len();
            sink.observe(&format!(
                "NOVAROCKS_TASK_SPLIT_ASSIGNMENT_ACCEPTED execution_id={}:{}:{} finst={:x}:{:x} plan_node={} enqueued={} duplicate={} accepted_through={}",
                execution_id.query_id().high(),
                execution_id.query_id().low(),
                execution_id.attempt_id().get(),
                finst.high(),
                finst.low(),
                node,
                outcome.enqueued.len(),
                duplicate_count,
                outcome.max_accepted_sequence.unwrap_or_default(),
            ));
            if duplicate_count > 0 {
                sink.observe(&format!(
                    "NOVAROCKS_TASK_SPLIT_ASSIGNMENT_DUPLICATE execution_id={}:{}:{} finst={:x}:{:x} plan_node={} duplicate={} duplicate_splits_skipped={}",
                    execution_id.query_id().high(),
                    execution_id.query_id().low(),
                    execution_id.attempt_id().get(),
                    finst.high(),
                    finst.low(),
                    node,
                    duplicate_count,
                    queue.stats().duplicate_splits_skipped,
                ));
            }
            if outcome.no_more_splits {
                sink.observe(&format!(
                    "NOVAROCKS_TASK_SPLIT_NO_MORE execution_id={}:{}:{} finst={:x}:{:x} plan_node={}",
                    execution_id.query_id().high(),
                    execution_id.query_id().low(),
                    execution_id.attempt_id().get(),
                    finst.high(),
                    finst.low(),
                    node,
                ));
            }
        }
        // Measured after the offer, because that is what the sender needs:
        // how full this node's queue is now, not how full it was before it
        // sent. Defaulting it to zero would tell the sender the task is idle
        // and invite it to keep filling a queue that is already full.
        // Only this successful, open-queue duplicate path can release input.
        // Closed/retired answers and an Idempotent registry verdict cannot.
        #[cfg(novarocks_dev_controls)]
        if let Some(operation) = operation {
            if let Some(gate) = runtime
                .replay_gate
                .lock()
                .expect("task replay gate lock")
                .as_ref()
            {
                if assignment.no_more_splits()
                    && !sequences.is_empty()
                    && preflight.duplicate_sequences().len() == sequences.len()
                {
                    gate.observe_duplicate(
                        identity,
                        operation,
                        node,
                        sequences.first().expect("nonempty").sequence_id(),
                        sequences.last().expect("nonempty").sequence_id(),
                    );
                }
            }
        }
        Ok(queue.stats().queued_splits as u64)
    }

    /// Recovers the typed split batch a neutral domain payload carries.
    ///
    /// The neutral layer cannot name a generated message, so the payload
    /// arrives as an opaque handle whose stored representation is the exact
    /// assignment the central codec decoded. Re-parsing it here re-applies the
    /// codec's own bounds rather than trusting the handle's shape.
    fn split_assignment(payload: &dyn CodecOwnedContent) -> Result<SplitAssignment, HostRejection> {
        let raw = stored_message::<connector_dto::SplitAssignment>(payload)
            .ok_or_else(|| internal("split domain payload is not a split assignment"))?;
        SplitAssignment::parse(
            raw.clone(),
            novarocks_proto_codec::FieldPath::root("split_assignment"),
        )
        .map_err(|error| protocol(format!("split assignment is invalid: {error}")))
    }
}

impl TaskExecutionHost for NativeTaskExecutionHost {
    fn close_context_admission(&self, context: QueryContextRef) {
        self.capabilities.close_context(context);
    }

    fn abort_context_admission(&self, context: QueryContextRef) {
        self.capabilities.abort_context(context);
    }

    fn retire_context_execution(&self, context: QueryContextRef) {
        self.queries
            .retire_idle_execution(context.query_execution_id());
    }

    fn forget_context_admission(&self, context: QueryContextRef) {
        self.capabilities.forget_context(context);
    }

    fn retains_normal_close(&self, context: QueryContextRef) -> bool {
        self.capabilities.retains_normal_close(context)
    }

    /// Interprets the winner's static plan and prepares the whole fragment.
    ///
    /// This is the only place a task's static plan is decoded, and only the
    /// backend that won the task's creation reaches it; a create that named an
    /// existing identity was answered by the owner without calling here, so
    /// its body was never read. Everything the plan and the assignment must
    /// agree on is proved before anything is published: the frozen header and
    /// the task's parallelism against the fragment's DOP domain, the scan
    /// nodes the assignment names against the plan's scan sources, and every
    /// static sink branch against the outbound edge bound to it.
    ///
    /// Receiver registration is not separable from preparation: the kernel
    /// registers a task's exchange receivers inside `prepare_fragment`, which
    /// also builds the pipeline. Preparing here and parking the dormant handle
    /// is what makes this step undoable, because dropping that handle rolls
    /// the registration back through `FragmentResources`. A refusal returns
    /// only after this host released everything it prepared -- the split
    /// queue lease and every local value are dropped on the way out -- since
    /// the owner records nothing as installed until this returns `Ok`.
    // Design: ADR-0158 (docs/adr/ADR-0158-task-creation-is-frozen-once-and-replayed-by-identity.md)
    fn install_receiver(
        &self,
        descriptor: &TaskDescriptor,
        input: TaskCreationInput,
        preparation_stop: novarocks_worker::TaskPreparationStopView,
    ) -> Result<novarocks_worker::PreparedTaskInstallation, HostRejection> {
        let identity = descriptor.identity();
        let execution = identity.query_execution_id();
        let kernel_key = descriptor.fragment_instance_id();
        if self.task_runtime(identity).is_some() {
            return Err(internal(format!(
                "task {identity} already has a prepared fragment"
            )));
        }

        #[cfg(novarocks_dev_controls)]
        if let Some(controls) = self.test_controls.as_ref() {
            crate::task_protocol_fault::hold_native_task_preparation(controls, identity)
                .map_err(internal)?;
        }
        if let Some(compiler) = self.compiled.clone() {
            return self.install_compiled_receiver(
                descriptor,
                input,
                compiler.as_ref(),
                preparation_stop,
            );
        }
        let (static_fragment, assignment) = input.into_parts();
        let fragment = decode_static_fragment(&static_fragment, FieldPath::root("frozen_fragment"))
            .map_err(|error| {
                protocol(format!(
                    "task {identity} static fragment is not decodable: {error}"
                ))
            })?;
        // The frozen bytes end here: the decoded fragment is the one copy the
        // rest of preparation reads.
        drop(static_fragment);
        fragment
            .verify_task_dop(
                descriptor.pipeline_dop(),
                FieldPath::root("creation_metadata")
                    .field("descriptor")
                    .field("pipeline_dop"),
            )
            .map_err(|error| {
                protocol(format!(
                    "task {identity} parallelism does not fit its fragment: {error}"
                ))
            })?;
        let assignment = take_task_assignment(assignment).ok_or_else(|| {
            internal(format!(
                "task {identity} creation assignment is not a codec-produced assignment"
            ))
        })?;
        let sink_kind = fragment.sink_kind();

        // Every kernel fact comes from its one owner: the identity, the
        // descriptor, the assignment, and -- for every query-wide option -- the
        // established context. The task carries no second copy of any of them.
        let context_options = self.context_facts.query_options(execution)?;
        let instance = project_task_instance(
            descriptor,
            assignment
                .into_wire()
                .map_err(|error| internal(format!("restore validated task assignment: {error}")))?,
            context_options.runtime().as_ref().clone(),
            sink_kind,
        )
        .map_err(|error| {
            protocol(format!(
                "task {identity} assignment is not a legal instance: {error}"
            ))
        })?;

        let attempt = TaskAttemptKey::new(execution, kernel_key);
        let splits = self.split_queues.open_attempt(
            attempt,
            SplitQueueConfig {
                max_queued_bytes: MAX_ASSIGNMENT_RETAINED_BYTES,
            },
        );
        // The queue set is opened before full decode so a scan can start and
        // block before its first split arrives. Every later refusal is rolled
        // back by this structural lease.
        let mut lease = SplitQueueLease::held(&self.split_queues, attempt);
        let task_stop = ConnectorStopOwner::new();
        let mut stop_guard = PreparationStopGuard::new(task_stop.clone());
        let read_context = Arc::new(TypedReadAttemptContext::new());
        let typed_runtime = self.typed_scan_runtime(
            execution,
            kernel_key,
            Arc::clone(&read_context),
            Arc::clone(&splits),
        )?;

        // The full static-to-assignment cross-check: scan node membership and
        // sink edge binding both run in this decode, before any receiver is
        // registered. The decoded plan is moved in rather than cloned.
        let type_tracker = self
            .queries
            .type_construction_tracker(execution, context_options.runtime().exec_mem_limit())
            .map_err(|error| memory_admission_failure_to_host(identity, &error))?;
        let type_creation = crate::native_type_construction::NativeTypeConstruction::try_new(
            type_tracker,
            task_stop.view(),
            preparation_stop.clone(),
        )
        .map_err(|error| native_type_construction_failure_to_host(identity, &error))?;
        let request = NativeFragmentRequest::try_decode_task(
            execution,
            fragment.into_plan(),
            instance,
            descriptor.topology(),
            task_stop.view(),
            Duration::from_millis(self.execution_runtime.config().exchange_wait_ms),
            Some(typed_runtime.clone()),
            Arc::clone(self.execution_runtime.function_catalog()),
            #[cfg(novarocks_dev_controls)]
            self.test_controls.clone(),
            #[cfg(novarocks_dev_controls)]
            self.queries.observation_controls(),
            &type_creation,
        )
        .map_err(|error| {
            if let Some(cause) = error.specialization_cause() {
                return native_specialization_failure_to_host(identity, cause);
            }
            match error.construction_cause() {
                Some(cause) => native_type_construction_failure_to_host(identity, cause),
                None => protocol(format!("task {identity} plan is not decodable: {error}")),
            }
        })?;
        if request.sink_kind() != sink_kind {
            // Both answers come from the same decoded plan, so a disagreement
            // is a defect in this binary rather than something a peer sent.
            return Err(internal(format!(
                "task {identity} static sink {sink_kind:?} decoded as {:?}",
                request.sink_kind()
            )));
        }
        let expects_bindings = request.has_runtime_filter_bindings();
        let (delivery_expire, query_expire) =
            novarocks_execution::runtime::query_options::query_expire_durations(Some(
                context_options.runtime().as_ref(),
            ));
        let exec_mem_limit = context_options.runtime().exec_mem_limit();
        // Profiling is the query's decision, exactly as it is on the
        // fragment-based path. Without it there is no per-operator counter to
        // read, and this task's final info reports no operator statistics
        // rather than reporting zeroes it never measured.
        let profiler = context_options
            .runtime()
            .enable_profile()
            .then(|| fragment_root_profiler(request.root_plan_node_id()));
        let submission = request.into_submission();
        // The frozen program, not the shared Result sink kind, chooses the
        // new producer. Its complete metadata/input pool exists before any
        // factory or driver can grow. The registry takes the provisional
        // channel under its existing creation/context fence below.
        let root_session = self.open_root_result_session(
            identity,
            submission.program().local_program().sink(),
            matches!(
                submission.program().local_program().nodes()
                    [submission.program().local_program().root().index()]
                .kind(),
                novarocks_local_program::ProgramNodeKind::Unpivot { .. }
            ),
        )?;
        let root_channel = root_session
            .as_ref()
            .map(|session| Arc::clone(session.channel()));

        let edges = ExchangeEdgeGates::from_frozen_edges(descriptor.topology().outbound())
            .map_err(|error| {
                protocol(format!(
                    "task {identity} outbound topology has no legal gate set: {error}"
                ))
            })?;

        // Installed before preparation because preparation may already create
        // workers that retain the sink; binding its reporter comes later,
        // because the reporter is minted only for `submit_runnable`.
        let operator_statistics = Arc::new(TaskOperatorStatisticsSink::new(
            kernel_key,
            profiler.clone(),
        ));
        let event_sink: Arc<dyn FragmentEventSink> =
            Arc::new(CompositeFragmentEventSink::new(vec![
                self.context_facts
                    .runtime_filter_event_sink(execution, kernel_key),
                Arc::clone(&operator_statistics) as Arc<dyn FragmentEventSink>,
            ]));

        let runtime_filter =
            self.context_facts
                .runtime_filter_session(execution, kernel_key, expects_bindings)?;
        // Resource ownership starts with preparation. This registration does
        // not activate drivers; its lease rolls back every pre-submit exit.
        let registration = self
            .queries
            .register_fragment_execution(execution, kernel_key, delivery_expire, query_expire)
            .map_err(|error| {
                resource_exhausted(format!("task {identity} could not be registered: {error}"))
            })?;
        let admission = self
            .queries
            .prepare_admission_execution_typed(
                execution,
                kernel_key,
                delivery_expire,
                query_expire,
                exec_mem_limit,
                runtime_filter,
            )
            .map_err(|error| memory_admission_failure_to_host(identity, &error))?;
        typed_runtime
            .install_connector_resource_tracker(admission.fragment_mem_tracker())
            .map_err(|error| {
                internal(format!(
                    "task {identity} could not install connector resource accounting: {error}"
                ))
            })?;
        let mut context = admission
            .into_prepare_context(
                profiler,
                Arc::clone(&self.exchange_transmitter),
                Arc::clone(&self.result_writer),
                event_sink,
            )
            .with_fragment_commit_port(Arc::clone(&self.commit_port))
            .with_exchange_receiver_port(Arc::clone(&self.exchange_receiver_port))
            .with_registration_owned_exchange_receivers()
            .with_execution_runtime(Arc::clone(&self.execution_runtime))
            .with_result_identity(identity)
            // Binding the gates here is what makes the closed-edge barrier
            // real: the sinks this fragment builds consult them before every
            // send, so a producer cannot reach a destination that has not
            // acknowledged its own creation.
            .with_edge_gates(Arc::clone(&edges));
        #[cfg(novarocks_dev_controls)]
        if let Some(observations) = self.queries.observation_controls() {
            if let Some(observer) = observations
                .observation_for("exchange-snapshot", execution)
                .map_err(internal)?
            {
                let receiver = novarocks_execution::runtime::fragment::io::exchange_receiver::ExecutionRuntimeExchangeReceiverPort::new(self.execution_runtime.clone()).with_test_observer(observer.clone());
                context = context
                    .with_exchange_test_observer(Some(observer))
                    .with_exchange_receiver_port(Arc::new(receiver));
            }
        }
        if let Some(session) = root_session {
            context = context.with_root_result_session(session);
        }

        // This is the receiver install. It registers every inbound exchange
        // receiver and builds the pipeline in one step, and its rollback is
        // dropping the handle it returns.
        let mut dormant = prepare_fragment(submission, context)
            .map_err(|error| preparation_failure_to_host(identity, &error))?;
        let exchange_retirement = dormant.take_exchange_retirement();

        #[cfg(novarocks_dev_controls)]
        crate::task_execution_observation::emit_prepared_task_dop(
            self.queries.observation_controls().as_deref(),
            identity,
            descriptor.fragment_instance_id(),
            descriptor.pipeline_dop().get(),
        );

        let installation = novarocks_worker::PreparedTaskInstallation::new(
            PreparedTaskFacts::new(sink_kind),
            root_channel.clone(),
        )?;
        if let Some(channel) = &root_channel {
            #[cfg(novarocks_dev_controls)]
            crate::task_execution_observation::emit_prepared_client_root(
                self.queries.observation_controls().as_deref(),
                identity,
                &channel.spec().contract,
            );
        }

        self.tasks.lock().expect(TASK_LOCK).insert(
            identity,
            Arc::new(TaskRuntime {
                attempt,
                stop: task_stop,
                sink_kind,
                root_channel,
                dormant: Mutex::new(Some(dormant)),
                exchange_retirement: Mutex::new(exchange_retirement),
                edges,
                splits,
                #[cfg(novarocks_dev_controls)]
                replay_gate: Mutex::new(None),
                read_context,
                registration: Mutex::new(Some(registration)),
                operator_statistics,
            }),
        );
        lease.retain();
        stop_guard.transfer_to_task();
        Ok(installation)
    }

    /// Drops everything `install_receiver` prepared.
    ///
    /// Idempotent, because the owner calls it both to roll a creation back and
    /// to retire a task that already ran. In the first case the dormant handle
    /// is still parked and its drop rolls the registrations back; in the
    /// second it was taken by `submit_runnable`. Physical ingress is retired
    /// here in both cases, without waiting for the last TaskRuntime alias.
    fn remove_receiver(&self, descriptor: &TaskDescriptor) {
        let removed = self
            .tasks
            .lock()
            .expect(TASK_LOCK)
            .remove(&descriptor.identity());
        if let Some(runtime) = removed {
            runtime.stop.request_stop();
            // Closing the queues wakes any scan still blocked on a split that
            // will now never arrive.
            self.split_queues.close_attempt(runtime.attempt);
            // The caller has already closed the capability or holds its
            // normal-close writer gate. Do not defer this to Arc::drop.
            if let Some(retirement) = runtime
                .exchange_retirement
                .lock()
                .expect(DORMANT_LOCK)
                .take()
            {
                retirement.retire();
            }
            drop(runtime);
        }
    }

    fn install_inbound_capability(&self, descriptor: &TaskDescriptor) -> Result<(), HostRejection> {
        self.capabilities.install(Arc::new(descriptor.clone()))
    }

    fn reserve_inbound_close_capacity(
        &self,
        descriptor: &TaskDescriptor,
    ) -> Result<(), HostRejection> {
        self.capabilities.reserve_normal_close(descriptor)
    }

    fn remove_inbound_capability(&self, descriptor: &TaskDescriptor) {
        self.capabilities.remove(descriptor);
    }

    fn retire_receiver_normally(&self, descriptor: &TaskDescriptor) {
        self.capabilities
            .close_normally(descriptor, || self.remove_receiver(descriptor));
    }

    /// Reserves the prepared fragment's terminal supervision and returns a
    /// dormant runnable. The Worker starts it after installing the Live record.
    ///
    /// This is the last fallible install step, so a refusal leaves no running
    /// worker behind.
    fn submit_runnable(
        &self,
        descriptor: &TaskDescriptor,
        reporter: TaskStatusReporter,
    ) -> Result<Arc<dyn RunnableTask>, HostRejection> {
        let identity = descriptor.identity();
        let execution = identity.query_execution_id();
        let kernel_key = descriptor.fragment_instance_id();
        let runtime = self.task_runtime(identity).ok_or_else(|| {
            internal(format!(
                "task {identity} was submitted without a prepared fragment"
            ))
        })?;
        let dormant = runtime
            .dormant
            .lock()
            .expect(DORMANT_LOCK)
            .take()
            .ok_or_else(|| internal(format!("task {identity} was submitted twice")))?;

        let registration = runtime
            .registration
            .lock()
            .expect(DORMANT_LOCK)
            .take()
            .ok_or_else(|| internal(format!("task {identity} has no preparation registration")))?;
        // A split may arrive the moment this create is acknowledged, so the
        // decoded read bindings become resolvable exactly here — after
        // preparation proved every plan node decodes, and before any worker
        // can block on an empty queue.
        runtime.read_context.publish();

        // The metrics owner can only report onto a task that has a reporter,
        // and this is the first moment one exists.
        runtime.operator_statistics.bind(reporter.clone());
        // Same reason for the filter feedback carrier: the domain a channel
        // reduces is advertised through a task's status, and a task has a
        // status owner only once it is being submitted. Before the worker
        // starts, because a producer can close its channel almost immediately
        // after the drivers begin.
        if self
            .context_facts
            .bind_runtime_filter_feedback(execution, identity, &reporter)
        {
            debug!("task {identity} carries its query context's dynamic filter feedback");
        }

        // Claimed here rather than in the worker: a malformed arming is still
        // reportable as a typed rejection on this path, and the worker has no
        // way to report one. The failure itself is applied after the task
        // publishes RUNNING, so the subject is a task that started.
        #[cfg(novarocks_dev_controls)]
        let injected_execution_failure = if let Some(controls) = self.test_controls.as_ref() {
            fault::task_execution_failure_injected(controls, identity).map_err(internal)?
        } else {
            None
        };

        let task = Arc::new(NativeRunnableTask::new_with_stop(
            identity,
            kernel_key,
            runtime.stop.clone(),
            Arc::downgrade(&runtime),
        ));
        let completion_worker = Arc::clone(&task);
        let queries = self.queries.clone();
        let split_queues = Arc::clone(&self.split_queues);
        let attempt = runtime.attempt;
        let sink_kind = runtime.sink_kind;
        let root_channel = runtime.root_channel.clone();
        let operator_statistics = Arc::clone(&runtime.operator_statistics);
        let completion_reporter = reporter.clone();
        let completion_runtime = Arc::clone(&runtime);
        let completion = self
            .completion_supervisor
            .reserve(
                identity,
                kernel_key,
                Box::new(move |fact| {
                    // Preserve the kernel's original removal/cancellation
                    // disposition before any fact can make the task retire.
                    // Only the inbound-close owner performs physical cleanup.
                    if let Some(retirement) = completion_runtime
                        .exchange_retirement
                        .lock()
                        .expect(DORMANT_LOCK)
                        .as_mut()
                    {
                        retirement.record_terminal(&fact);
                    }
                    // The stopped observer is the positive local evidence
                    // that pipeline work ended. A terminal status, abort
                    // acknowledgement, or timeout cannot manufacture it.
                    completion_reporter.note_actual_stopped();
                    // The sampling tick stops firing the moment the drivers
                    // finish, so the last event-driven snapshot predates the
                    // rows the operators counted on their way out. The
                    // terminal fact carries the final frozen profile.
                    if let Some(profile) = fact.profile() {
                        operator_statistics.record_profile(profile);
                    }
                    report_terminal_with_root(
                        &completion_reporter,
                        sink_kind,
                        root_channel.as_deref(),
                        &fact,
                        completion_worker.stand_down(),
                    );
                    completion_worker.finish(
                        (sink_kind == FragmentSinkKind::Result && root_channel.is_none())
                            .then_some(&completion_reporter),
                    );
                    split_queues.close_attempt(attempt);
                    queries.unregister_fragment_execution(execution, kernel_key);
                    queries.finish_fragment(execution);
                    // Every runtime owner named above has now returned its
                    // local responsibility. Registry-owned ingress state is
                    // released later, by retirement.
                    completion_reporter.note_resources_converged();
                }),
            )
            .map_err(|error| {
                resource_exhausted(format!("reserve native task completion failed: {error}"))
            })?;
        task.bind_completion(completion.clone());

        // The Worker must make the Live record findable before any driver can
        // start or publish completion. The runnable owns this dormant start
        // until the registry calls commit_creation after installing Live.
        task.prepare_start(PreparedNativeStart {
            dormant,
            registration,
            reporter,
            completion,
            #[cfg(novarocks_dev_controls)]
            injected_execution_failure,
        });
        Ok(task)
    }

    #[cfg(novarocks_dev_controls)]
    fn reserve_test_update_consumption(
        &self,
        request: &novarocks_execution_contract::UpdateTask,
        control: novarocks_failpoint::scope::ControlCancellation,
    ) -> Result<Box<dyn novarocks_worker::TestTaskUpdateConsumptionReservation>, HostRejection>
    {
        let runtime = self
            .task_runtime(request.identity())
            .ok_or_else(|| internal("controlled replay gate requires the actual installed task"))?;
        let controls = self
            .test_controls
            .as_ref()
            .ok_or_else(|| internal("controlled replay gate has no installed Native controls"))?;
        let mut slot = runtime.replay_gate.lock().expect("task replay gate lock");
        if slot.as_ref().is_some_and(|gate| gate.is_held()) {
            return Err(internal("task already has an active exact replay gate"));
        }
        let gate = super::task_update_replay_gate::TaskUpdateReplayGate::reserve(
            request,
            &runtime.splits,
            control,
            runtime.stop.view(),
        )
        .map_err(internal)?;
        *slot = Some(Arc::clone(&gate));
        Ok(Box::new(
            super::task_update_replay_gate::ReplayReservation::new(
                gate,
                Arc::clone(controls),
                self.scan_stream_host.runtime().clone(),
            ),
        ))
    }

    #[cfg(novarocks_dev_controls)]
    fn apply_task_domain_for_operation(
        &self,
        descriptor: &TaskDescriptor,
        domain: &TaskDomainUpdate,
        operation: novarocks_execution_contract::TaskOperationId,
    ) -> Result<Option<u64>, HostRejection> {
        let TaskDomainUpdate::SplitAssignment(intent) = domain else {
            return self.apply_task_domain(descriptor, domain);
        };
        let runtime = self
            .task_runtime(descriptor.identity())
            .ok_or_else(|| internal("split update has no actual installed task"))?;
        let assignment = Self::split_assignment(intent.payload().as_ref())?;
        if assignment.plan_node_id() != intent.node().get() {
            return Err(protocol(
                "split intent and payload name different plan nodes",
            ));
        }
        self.deliver_splits(
            &runtime,
            descriptor.identity(),
            descriptor.fragment_instance_id(),
            &assignment,
            Some(operation),
        )
        .map(Some)
    }

    fn apply_task_domain(
        &self,
        descriptor: &TaskDescriptor,
        domain: &TaskDomainUpdate,
    ) -> Result<Option<u64>, HostRejection> {
        let identity = descriptor.identity();
        let runtime = self.task_runtime(identity).ok_or_else(|| {
            internal(format!(
                "domain update reached task {identity}, which has no prepared fragment"
            ))
        })?;
        match domain {
            TaskDomainUpdate::SplitAssignment(intent) => {
                let assignment = Self::split_assignment(intent.payload().as_ref())?;
                if assignment.plan_node_id() != intent.node().get() {
                    return Err(protocol(format!(
                        "split intent names plan node {} but its payload names {}",
                        intent.node(),
                        assignment.plan_node_id()
                    )));
                }
                self.deliver_splits(
                    &runtime,
                    identity,
                    descriptor.fragment_instance_id(),
                    &assignment,
                    #[cfg(novarocks_dev_controls)]
                    None,
                )
                .map(Some)
            }
            TaskDomainUpdate::TaskDynamicFilter { version, payload } => {
                self.context_facts
                    .deliver_task_dynamic_filter(
                        identity.query_execution_id(),
                        descriptor.fragment_instance_id(),
                        *version,
                        payload,
                    )
                    // A filter domain has no queue to report.
                    .map(|()| None)
            }
            TaskDomainUpdate::OpenExchangeEdges { version, edges } => {
                // The one place a gated edge starts sending. Logged because a
                // sink whose edge never opens waits silently, and the absence
                // of this line is the evidence that distinguishes "the
                // frontend never decided" from "the producer never applied it".
                tracing::debug!(
                    task = %identity,
                    version = ?version,
                    edges = ?edges,
                    "opening this task's frozen exchange edges"
                );
                runtime
                    .edges
                    .open(*version, edges)
                    .map(|_| None)
                    .map_err(|conflict| {
                        protocol(format!("task {identity} edge open is illegal: {conflict}"))
                    })
            }
            TaskDomainUpdate::CloseExchangeDestination {
                edge, destination, ..
            } => {
                let frozen = descriptor.topology().edge(*edge)
                    .and_then(|outbound| outbound.destinations().iter().find(|member| member.task() == *destination))
                    .ok_or_else(|| protocol(format!("task {identity} did not freeze destination {destination} on edge {edge}")))?;
                let key = ExchangeDestinationKey::new(
                    frozen.fragment_instance_id(),
                    frozen.destination_node_id().get(),
                );
                let gate = runtime.edges.gate_for_destination(key)
                    .filter(|gate| gate.edge_id() == *edge)
                    .ok_or_else(|| protocol(format!("task {identity} has no matching runtime gate for destination {destination} on edge {edge}")))?;
                self.execution_runtime
                    .exchange_send_queue()
                    .close_destination_normally(gate);
                Ok(None)
            }
        }
    }
}

/// Closes one attempt's split queues unless the preparation that opened them
/// reaches its commit point.
///
/// It is a guard rather than a call on each error path because
/// `install_receiver` has several refusals after the queues exist. One missed
/// cleanup would leave a queue set that nothing owns and that only a later
/// attempt on the same key could ever notice.
struct SplitQueueLease<'a> {
    registry: &'a SplitQueueRegistry<ReceivedReadSplit>,
    attempt: TaskAttemptKey,
    retained: bool,
}

impl<'a> SplitQueueLease<'a> {
    const fn held(
        registry: &'a SplitQueueRegistry<ReceivedReadSplit>,
        attempt: TaskAttemptKey,
    ) -> Self {
        Self {
            registry,
            attempt,
            retained: false,
        }
    }

    /// Hands the queues to the installed task, which owns them until
    /// `remove_receiver`.
    const fn retain(&mut self) {
        self.retained = true;
    }
}

impl Drop for SplitQueueLease<'_> {
    fn drop(&mut self) {
        if !self.retained {
            self.registry.close_attempt(self.attempt);
        }
    }
}

/// What a stand-down actually reaches.
///
/// The kernel's running handle is the only production implementor. Naming it
/// as a capability is what lets the pre-start race be exercised without
/// building a pipeline.
trait FragmentStandDown: Send + Sync {
    fn cancel(&self, reason: FragmentCancelReason);
}

impl FragmentStandDown for RunningFragmentHandle {
    fn cancel(&self, reason: FragmentCancelReason) {
        Self::cancel(self, reason);
    }
}

/// The first stand-down a task was asked for. It is latched, not queued: a
/// later reason never rewrites the first one. The latch only decides what the
/// fragment is told; the task's terminal is arbitrated by its status owner,
/// where an abort may still supersede a published cancellation.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum StandDown {
    Cancel(CancelReason),
    Quiesce,
    Abort(AbortCause),
}

impl StandDown {
    /// The termination this stand-down proposes to the status owner.
    const fn proposal(self) -> TerminationDetail {
        match self {
            Self::Cancel(reason) => TerminationDetail::Canceled(reason),
            Self::Quiesce => TerminationDetail::Canceled(CancelReason::UpstreamNoLongerNeeded),
            Self::Abort(cause) => TerminationDetail::Aborted(cause),
        }
    }

    fn reason(self) -> FragmentCancelReason {
        match self {
            Self::Cancel(reason) => FragmentCancelReason::new(reason.as_str()),
            Self::Quiesce => {
                FragmentCancelReason::new(CancelReason::UpstreamNoLongerNeeded.as_str())
            }
            Self::Abort(cause) => FragmentCancelReason::new(cause.as_str()),
        }
    }
}

#[derive(Default)]
struct RunnableState {
    prepared_start: Option<PreparedNativeStart>,
    handle: Option<Arc<dyn FragmentStandDown>>,
    stand_down: Option<StandDown>,
    finished: bool,
    finished_root_reporter: Option<TaskStatusReporter>,
}

struct PreparedNativeStart {
    dormant: DormantFragmentHandle,
    registration: NativeFragmentRegistrationLease,
    reporter: TaskStatusReporter,
    completion: TaskCompletionSignal,
    #[cfg(novarocks_dev_controls)]
    injected_execution_failure: Option<crate::test_controls::NativeFaultClaim>,
}

/// One submitted task, as the owner may address it.
pub struct NativeRunnableTask {
    identity: TaskIdentity,
    fragment_instance_id: UniqueId,
    stop: ConnectorStopOwner,
    receiver_runtime: Option<Weak<TaskRuntime>>,
    state: Mutex<RunnableState>,
    completion: OnceLock<TaskCompletionSignal>,
}

impl fmt::Debug for NativeRunnableTask {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.lock().expect(RUNNABLE_LOCK);
        formatter
            .debug_struct("NativeRunnableTask")
            .field("identity", &self.identity)
            .field("fragment_instance_id", &self.fragment_instance_id)
            .field("started", &state.handle.is_some())
            .field("stand_down", &state.stand_down)
            .field("finished", &state.finished)
            .finish()
    }
}

const RUNNABLE_LOCK: &str = "native runnable task lock";

impl NativeRunnableTask {
    #[cfg(test)]
    fn new(identity: TaskIdentity, fragment_instance_id: UniqueId) -> Self {
        Self::new_without_receiver_with_stop(
            identity,
            fragment_instance_id,
            ConnectorStopOwner::new(),
        )
    }

    fn new_with_stop(
        identity: TaskIdentity,
        fragment_instance_id: UniqueId,
        stop: ConnectorStopOwner,
        receiver_runtime: Weak<TaskRuntime>,
    ) -> Self {
        Self {
            identity,
            fragment_instance_id,
            stop,
            receiver_runtime: Some(receiver_runtime),
            state: Mutex::new(RunnableState::default()),
            completion: OnceLock::new(),
        }
    }

    #[cfg(test)]
    fn new_without_receiver_with_stop(
        identity: TaskIdentity,
        fragment_instance_id: UniqueId,
        stop: ConnectorStopOwner,
    ) -> Self {
        Self {
            identity,
            fragment_instance_id,
            stop,
            receiver_runtime: None,
            state: Mutex::new(RunnableState::default()),
            completion: OnceLock::new(),
        }
    }

    fn bind_completion(&self, completion: TaskCompletionSignal) {
        assert!(
            self.completion.set(completion).is_ok(),
            "a native runnable task binds one completion slot"
        );
    }

    fn prepare_start(&self, prepared: PreparedNativeStart) {
        let mut state = self.state.lock().expect(RUNNABLE_LOCK);
        assert!(state.prepared_start.replace(prepared).is_none());
    }

    fn start_committed(&self) {
        let mut state = self.state.lock().expect(RUNNABLE_LOCK);
        let prepared = state
            .prepared_start
            .take()
            .expect("a committed native task has one dormant start");
        let PreparedNativeStart {
            dormant,
            registration,
            reporter,
            completion,
            #[cfg(novarocks_dev_controls)]
            injected_execution_failure,
        } = prepared;
        if let Some(stand_down) = state.stand_down {
            // The task is Live before this call, but a Context stop can win
            // while its kernel is still dormant. Complete the stopped fact
            // without submitting any driver or rolling back its registration.
            registration.into_running();
            drop(dormant);
            drop(state);
            completion.publish(FragmentTerminalFact::new(
                self.identity.query_execution_id().query_id(),
                self.fragment_instance_id,
                FragmentOutcome::Cancelled {
                    reason: stand_down.reason(),
                },
                None,
            ));
            return;
        }
        registration.into_running();
        // RUNNING precedes driver submission, including synchronous stop.
        reporter.running();
        #[cfg(novarocks_dev_controls)]
        let running = if let Some(claim) =
            injected_execution_failure.filter(|claim| claim.cancellation().is_active())
        {
            match fault::record_task_execution_failure(self.identity, &claim) {
                Ok(()) => dormant.start_failed(fault::TASK_EXECUTION_FAILURE_DETAIL),
                Err(error) => dormant.start_failed(error),
            }
        } else {
            dormant.start()
        };
        #[cfg(not(novarocks_dev_controls))]
        let running = dormant.start();
        // Serialize the start with request(): a stop observed before this
        // lock was taken never starts work; a later stop sees this handle.
        state.handle = Some(Arc::new(running.clone()));
        drop(state);
        // The kernel retains an already-stopped fact for this subscription.
        running.subscribe_stopped(move |fact| completion.publish(fact));
    }

    /// Publishes the started fragment and replays any latched stand-down.
    #[cfg(test)]
    fn attach(&self, handle: Arc<dyn FragmentStandDown>) {
        let latched = {
            let mut state = self.state.lock().expect(RUNNABLE_LOCK);
            state.handle = Some(Arc::clone(&handle));
            state.stand_down
        };
        if let Some(stand_down) = latched {
            handle.cancel(stand_down.reason());
        }
    }

    /// The stand-down this task was asked for, if any.
    fn stand_down(&self) -> Option<StandDown> {
        self.state.lock().expect(RUNNABLE_LOCK).stand_down
    }

    /// Releases this handle's reference to the running fragment once the
    /// completion owner has its terminal fact, so the kernel handle's last
    /// drop belongs to that owner rather than to a later canceller.
    fn finish(&self, root_reporter: Option<&TaskStatusReporter>) {
        let late_stand_down = {
            let mut state = self.state.lock().expect(RUNNABLE_LOCK);
            state.finished = true;
            state.handle = None;
            state.finished_root_reporter = root_reporter.cloned();
            state.stand_down.and_then(|stand_down| {
                state
                    .finished_root_reporter
                    .take()
                    .map(|reporter| (stand_down, reporter))
            })
        };
        if let Some((stand_down, reporter)) = late_stand_down {
            settle_finished_root_result(&reporter, stand_down);
        }
    }

    fn request(&self, stand_down: StandDown) {
        let (handle, finished_root_reporter) = {
            let mut state = self.state.lock().expect(RUNNABLE_LOCK);
            if state.stand_down.is_some() {
                return;
            }
            state.stand_down = Some(stand_down);
            self.stop.request_stop();
            if state.finished {
                (None, state.finished_root_reporter.take())
            } else {
                (state.handle.as_ref().map(Arc::clone), None)
            }
        };
        // The kernel may have stopped while a root result is still FLUSHING.
        // The finished runnable supplies the missing terminal/output facts.
        if let Some(reporter) = finished_root_reporter {
            settle_finished_root_result(&reporter, stand_down);
        }
        // An absent handle means the fragment has not been attached yet. The
        // latch set above is what `attach` replays.
        if let Some(handle) = handle {
            handle.cancel(stand_down.reason());
        }
    }
}

fn settle_finished_root_result(reporter: &TaskStatusReporter, stand_down: StandDown) {
    let current = reporter.current();
    if current.state() == TaskState::Finished {
        return;
    }
    // Context termination and CancelTask normally discard this first. A
    // creation that lost to context termination can install its result after
    // that fan-out, so the execution owner must close its own late buffer too.
    if !matches!(stand_down, StandDown::Quiesce) {
        novarocks_worker::result_buffer::discard_task(reporter.identity());
    }
    if !current.is_terminal() {
        reporter.conclude_termination(stand_down.proposal(), TaskOutputFacts::default());
    }
    if !matches!(stand_down, StandDown::Quiesce) {
        reporter.release_output();
    }
}

impl RunnableTask for NativeRunnableTask {
    fn commit_creation(&self) {
        self.completion
            .get()
            .expect("a submitted native task has a completion slot")
            .commit_creation();
        self.start_committed();
    }

    fn cancel(&self, reason: CancelReason) {
        self.request(StandDown::Cancel(reason));
    }

    fn quiesce(&self) {
        self.request(StandDown::Quiesce);
    }

    fn abort(&self, cause: AbortCause) {
        // Receiver construction follows the registration's lifetime, not the
        // first kernel stand-down. A later true abort must still stop it.
        if let Some(runtime) = self.receiver_runtime.as_ref().and_then(Weak::upgrade) {
            if let Some(retirement) = runtime
                .exchange_retirement
                .lock()
                .expect(DORMANT_LOCK)
                .as_ref()
            {
                retirement.stop_construction(ExecutionFailure::from(
                    StandDown::Abort(cause).reason().detail().to_owned(),
                ));
            }
        }
        // Release the retirement lock before reaching the original latch or
        // kernel cancellation, which may run completion callbacks.
        self.request(StandDown::Abort(cause));
    }
}

/// Publishes one fragment's terminal fact as this task's terminal status.
///
/// Bounded roots finish after actual production and context handoff. The S0
/// legacy Result program still waits in FLUSHING for its old result reader.
#[cfg(test)]
fn report_terminal(
    reporter: &TaskStatusReporter,
    sink_kind: FragmentSinkKind,
    fact: &FragmentTerminalFact,
    stand_down: Option<StandDown>,
) {
    report_terminal_with_root(reporter, sink_kind, None, fact, stand_down);
}

fn report_terminal_with_root(
    reporter: &TaskStatusReporter,
    sink_kind: FragmentSinkKind,
    root_channel: Option<&novarocks_worker::root_result_channel::RootResultChannel>,
    fact: &FragmentTerminalFact,
    stand_down: Option<StandDown>,
) {
    // Context sealing publishes the Worker-owned task cause before waking
    // the root. Its runnable fan-out can arrive later; a closed root error
    // during that interval must not invent an originating execution failure.
    let stand_down = stand_down.or_else(|| {
        if !root_channel.is_some_and(|channel| channel.is_closed()) {
            return None;
        }
        match reporter.current().termination() {
            Some(
                novarocks_execution_contract::task_execution::status::TerminationDetail::Aborted(
                    cause,
                ),
            ) => Some(StandDown::Abort(*cause)),
            Some(
                novarocks_execution_contract::task_execution::status::TerminationDetail::Canceled(
                    reason,
                ),
            ) => Some(StandDown::Cancel(*reason)),
            _ => None,
        }
    });
    let context_handoff = root_channel.is_some_and(|channel| {
        matches!(
            channel.producer_state(),
            novarocks_execution::runtime::fragment::io::RootProducerState::ContextHeld,
        )
    });
    // A task that was told to stand down completes that stand-down whatever
    // its pipeline did next. The owner latched the termination and already
    // published the terminating state, and first-wins means the pipeline's own
    // outcome — including a success that raced the cancel, which is exactly
    // what a `LIMIT` query produces — cannot rewrite it. The latch is this
    // runnable's first request only; the status owner arbitrates it against
    // the stand-down it published, which may since have escalated to an abort.
    if let Some(stand_down) = stand_down {
        // The stand-down won the lifecycle race, but a non-root sink or a
        // bounded root with completed Context handoff may still have run to
        // success. Preserve that independent output fact; it does not prove
        // consumer acknowledgement or physical allocation reclamation.
        let output = if !matches!(stand_down, StandDown::Abort(_))
            && matches!(fact.outcome(), FragmentOutcome::Succeeded)
            && (!matches!(sink_kind, FragmentSinkKind::Result) || context_handoff)
        {
            TaskOutputFacts::new(true)
        } else {
            TaskOutputFacts::default()
        };
        match (stand_down, fact.outcome()) {
            (StandDown::Quiesce, FragmentOutcome::Failed(error)) => {
                report_failure(reporter, execution_failure_to_task(error))
            }
            _ => {
                reporter.conclude_termination(stand_down.proposal(), output);
            }
        }
        if !matches!(stand_down, StandDown::Quiesce)
            || !matches!(sink_kind, FragmentSinkKind::Result)
            || root_channel.is_some()
        {
            reporter.release_output();
        }
        return;
    }
    match fact.outcome() {
        FragmentOutcome::Succeeded => match sink_kind {
            // The root's output responsibility ends when the frontend drains
            // the result stream, not when the pipeline stops producing, so it
            // is the result plane that publishes FINISHED.
            FragmentSinkKind::Result if root_channel.is_some() => {
                if context_handoff {
                    reporter.finished(TaskOutputFacts::new(true));
                    // This is responsibility handoff to Context, not free or
                    // a consumer acknowledgement. Retired tasks keep reading
                    // through the context-owned map.
                    reporter.release_output();
                } else {
                    report_failure(
                        reporter,
                        TaskFailure::new(
                            TaskFailureCategory::Internal,
                            SafeDetail::truncating(
                                "root stopped without completed context handoff",
                            ),
                        ),
                    );
                }
            }
            FragmentSinkKind::Result => {
                // Logged because this is the one terminal that deliberately
                // is not FINISHED, and the frontend's read completion waits on
                // FINISHED. A root parked in FLUSHING with no further line is
                // the signature of a result stream the frontend never drained,
                // and without this it cannot be told apart from a root whose
                // pipeline never stopped producing at all.
                debug!("root task reached FLUSHING; the result plane publishes FINISHED");
                reporter.flushing();
            }
            _ => {
                // The Worker publishes a stand-down before it tells this
                // runnable, so a completion in between still finds the task
                // terminating and must complete that stand-down instead.
                reporter.conclude_success(TaskOutputFacts::new(true));
                reporter.release_output();
            }
        },
        // Nothing asked this task to stand down, so the kernel cancelled
        // itself. Reporting that as a clean cancellation would claim the query
        // may still succeed, which is a different and unproven statement.
        FragmentOutcome::Cancelled { reason } => {
            report_failure(
                reporter,
                TaskFailure::new(
                    TaskFailureCategory::Execution,
                    SafeDetail::truncating(reason.detail()),
                ),
            );
        }
        FragmentOutcome::Failed(error) => {
            report_failure(reporter, execution_failure_to_task(error));
        }
    }
}

/// Classify the original failure payload at the native reporting boundary.
/// Pipeline text is opaque; matching a diagnostic cannot grant a category.
fn execution_failure_to_task(error: &FragmentExecutionError) -> TaskFailure {
    let category = failure_cause_category(error.cause().cause(), TaskFailureCategory::Execution);
    TaskFailure::new(category, SafeDetail::truncating(&error.to_string()))
}

/// Preserve typed preparation causes. Opaque legacy launch sources retain
/// their original category until their own authors supply a typed failure.
fn preparation_failure_to_host(
    identity: TaskIdentity,
    error: &FragmentLaunchError,
) -> HostRejection {
    HostRejection::new(
        failure_cause_category(
            error.cause().cause(),
            TaskFailureCategory::ResourceExhausted,
        ),
        format!("task {identity} could not be prepared: {error}"),
    )
}

fn failure_cause_category(
    cause: &ExecutionFailureCause,
    opaque_category: TaskFailureCategory,
) -> TaskFailureCategory {
    use novarocks_execution::exec::pipeline::operator::input_construction::ConstructionFailure;
    use novarocks_functions::KernelFailure;
    use novarocks_type_contract::CompileControlError;
    match cause {
        ExecutionFailureCause::Construction(ConstructionFailure::Memory(_)) => {
            TaskFailureCategory::ResourceExhausted
        }
        ExecutionFailureCause::Construction(ConstructionFailure::Control(
            CompileControlError::ResourceExhausted,
        )) => TaskFailureCategory::ResourceExhausted,
        ExecutionFailureCause::Construction(ConstructionFailure::Resource(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel,
        )) => TaskFailureCategory::Internal,
        ExecutionFailureCause::Construction(ConstructionFailure::Resource(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::ArcHeader
            | novarocks_type_contract::owned_resources::layout::LayoutResourceError::ArcBacking
            | novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic
            | novarocks_type_contract::owned_resources::layout::LayoutResourceError::BytesShared,
        )) => TaskFailureCategory::ResourceExhausted,
        ExecutionFailureCause::Construction(ConstructionFailure::MissingOwner) => {
            TaskFailureCategory::Internal
        }
        ExecutionFailureCause::Construction(
            ConstructionFailure::InvalidSource(_)
            | ConstructionFailure::Control(
                CompileControlError::Cancelled | CompileControlError::DeadlineExceeded,
            ),
        ) => TaskFailureCategory::Execution,
        ExecutionFailureCause::Construction(ConstructionFailure::Caller(error)) => {
            failure_cause_category(error.cause(), opaque_category)
        }
        ExecutionFailureCause::Kernel(KernelFailure::ResourceExhausted) => {
            TaskFailureCategory::ResourceExhausted
        }
        ExecutionFailureCause::Kernel(
            KernelFailure::InvalidProgram(_)
            | KernelFailure::Internal(_)
            | KernelFailure::InstanceFailed,
        ) => TaskFailureCategory::Internal,
        ExecutionFailureCause::Kernel(
            KernelFailure::Cancelled
            | KernelFailure::DeadlineExceeded
            | KernelFailure::Operational(_),
        )
        | ExecutionFailureCause::RequiredRow(_)
        | ExecutionFailureCause::InvocationData(_)
        | ExecutionFailureCause::ScalarInvocationData(_)
        | ExecutionFailureCause::WindowInvocationData(_)
        | ExecutionFailureCause::RuntimeFilter(_) => TaskFailureCategory::Execution,
        ExecutionFailureCause::PreparationMetadata(cause) => match cause {
            novarocks_execution::runtime::preparation_metadata::PreparationMetadataFailure::Request(error) => metadata_request_category(error),
            novarocks_execution::runtime::preparation_metadata::PreparationMetadataFailure::Host(error) => preparation_memory_refusal_category(error),
        },
        ExecutionFailureCause::RuntimeScalarSource(novarocks_functions::ScalarResourceError::Arithmetic)
        | ExecutionFailureCause::RuntimeScalarMemory(_) => TaskFailureCategory::ResourceExhausted,
        ExecutionFailureCause::RuntimeScalarSource(novarocks_functions::ScalarResourceError::SourceModel(_)
            | novarocks_functions::ScalarResourceError::Invariant(_)) => TaskFailureCategory::Internal,
        ExecutionFailureCause::Pipeline(_) => opaque_category,
    }
}

fn preparation_memory_refusal_category(
    error: &novarocks_execution::runtime::preparation_memory::PreparationMemoryRefusal,
) -> TaskFailureCategory {
    use novarocks_execution::runtime::preparation_memory::PreparationMemoryRefusal;
    match error {
        PreparationMemoryRefusal::Stopped(_) => TaskFailureCategory::Execution,
        PreparationMemoryRefusal::MissingQueryMemory
        | PreparationMemoryRefusal::SharedShortage(_)
        | PreparationMemoryRefusal::Capacity(_) => TaskFailureCategory::ResourceExhausted,
    }
}
fn metadata_request_category(
    error: &novarocks_type_contract::MetadataRequestError,
) -> TaskFailureCategory {
    use novarocks_type_contract::owned_resources::{
        hashmap::HashMapResourceError, metadata_materialization::MetadataMaterializationError,
    };
    use novarocks_type_contract::{CompileControlError, MetadataRequestError};
    match error {
        MetadataRequestError::Control(
            CompileControlError::Cancelled | CompileControlError::DeadlineExceeded,
        ) => TaskFailureCategory::Execution,
        MetadataRequestError::Control(CompileControlError::ResourceExhausted)
        | MetadataRequestError::Arithmetic
        | MetadataRequestError::Metadata(
            MetadataMaterializationError::Arithmetic
            | MetadataMaterializationError::Model(HashMapResourceError::Arithmetic(_)),
        ) => TaskFailureCategory::ResourceExhausted,
        MetadataRequestError::SourceModel(_)
        | MetadataRequestError::ValueType(_)
        | MetadataRequestError::Metadata(_) => TaskFailureCategory::Internal,
    }
}

/// Concludes a task on its own failure, through FAILING to FAILED.
///
/// The status owner moves through the terminating state the state machine
/// requires, and keeps an abort that already won: a failure cannot rewrite a
/// stand-down the Worker published before this runnable heard of it.
fn report_failure(reporter: &TaskStatusReporter, failure: TaskFailure) {
    reporter.conclude_termination(
        TerminationDetail::Failed(failure),
        TaskOutputFacts::default(),
    );
    reporter.release_output();
}

fn split_queue_rejection(error: SplitQueueError) -> HostRejection {
    let category = match error.kind() {
        SplitQueueErrorKind::ResourceExhausted => TaskFailureCategory::ResourceExhausted,
        SplitQueueErrorKind::Closed => TaskFailureCategory::Execution,
        SplitQueueErrorKind::PlanNodeMismatch
        | SplitQueueErrorKind::SequenceConflict
        | SplitQueueErrorKind::AfterNoMoreSplits => TaskFailureCategory::Protocol,
    };
    if error.kind() == SplitQueueErrorKind::Closed {
        return HostRejection::from_closed_queue(category, error.to_string());
    }
    HostRejection::new(category, error.to_string())
}

/// The instance scan assignments of a compiled program: an empty range
/// binding for each compiled scan's physical node, because a compiled scan
/// takes all of its work as runtime splits.
///
/// The descriptor must name exactly those nodes as split-driven: a scan it
/// omits would never be told that no more splits come, and a node it adds
/// has no source to read them. The assignment's initial ranges may name only
/// those nodes, and none of them may carry a frozen range.
fn compiled_scan_assignments(
    identity: TaskIdentity,
    descriptor: &TaskDescriptor,
    program: &LocalProgram,
    instance: &NativeFragmentInstanceInput,
) -> Result<ScanAssignments, HostRejection> {
    let scan_nodes = compiled_scan_nodes(program)
        .map_err(|error| {
            protocol(format!(
                "task {identity} compiled scans are not addressable: {error}"
            ))
        })?
        .into_values()
        .collect::<BTreeSet<FragmentNodeId>>();
    let split_nodes = descriptor
        .split_plan_nodes()
        .iter()
        .map(|node| FragmentNodeId::new(node.get()))
        .collect::<BTreeSet<_>>();
    if split_nodes != scan_nodes {
        return Err(protocol(format!(
            "task {identity} names split-driven nodes {:?}, but its compiled scans are {:?}",
            split_nodes
                .iter()
                .map(|node| node.get())
                .collect::<Vec<_>>(),
            scan_nodes.iter().map(|node| node.get()).collect::<Vec<_>>()
        )));
    }
    for (node, ranges) in &instance.raw_scan_ranges {
        if !scan_nodes.contains(node) {
            return Err(protocol(format!(
                "task {identity} assigns scan ranges to node {}, which is not a compiled scan",
                node.get()
            )));
        }
        if !ranges.is_empty() {
            return Err(protocol(format!(
                "task {identity} assigns frozen ranges to compiled scan node {}, whose work \
                 arrives only as splits",
                node.get()
            )));
        }
    }
    ScanAssignments::try_new(
        scan_nodes
            .into_iter()
            .map(|node| (node, BoundScanRanges::None))
            .collect(),
    )
    .map_err(|error| internal(format!("task {identity} scan assignments: {error}")))
}

/// The task's own stop stays primary; a package this interpreter refuses is
/// content a peer of the same island must not have sent.
fn compiled_package_rejection(
    identity: TaskIdentity,
    error: CompiledPackageError,
) -> HostRejection {
    use novarocks_connector_contract::ConnectorErrorKind;
    use novarocks_physical_plan::CallRequestError;
    use novarocks_plan_codec::ipc_flat_stream_v2::{FlatPoolResourceError, FlatReaderError};
    use novarocks_plan_codec::physical_constant_v2::PhysicalConstantCodecError;
    use novarocks_plan_codec::physical_package_v2::PackageDecodeError;
    use novarocks_plan_codec::physical_public_source_schema_v2::PublicSourceSchemaCodecError;
    use novarocks_plan_codec::physical_read_scan_v2::ReadScanCodecError;
    use novarocks_plan_codec::physical_schema_v2::SchemaCodecError;
    use novarocks_plan_codec::physical_type_v2::TypeCodecError;
    use novarocks_plan_codec::physical_writer_recipe_v2::WriterRecipeCodecError;
    use novarocks_type_contract::CompileControlError;
    use novarocks_type_contract::owned_resources::btree::BTreeResourceError;
    use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
    if let CompiledPackageError::Construction(cause) = &error {
        return native_type_construction_failure_to_host(identity, cause);
    }
    let category = match &error {
        CompiledPackageError::Control(CompileControlError::ResourceExhausted)
        | CompiledPackageError::Decode(PackageDecodeError::Control(
            CompileControlError::ResourceExhausted,
        )) => TaskFailureCategory::ResourceExhausted,
        CompiledPackageError::Control(
            CompileControlError::Cancelled | CompileControlError::DeadlineExceeded,
        )
        | CompiledPackageError::Decode(PackageDecodeError::Control(
            CompileControlError::Cancelled | CompileControlError::DeadlineExceeded,
        )) => TaskFailureCategory::Execution,
        CompiledPackageError::Refused(_) => TaskFailureCategory::Protocol,
        CompiledPackageError::ProjectMetadata(error) => preparation_memory_refusal_category(error),
        CompiledPackageError::ProjectMetadataRequest(error) => metadata_request_category(error),
        CompiledPackageError::TypeMaterialization(super::TypeMaterializationRefusal::Stopped(
            _,
        )) => TaskFailureCategory::Execution,
        CompiledPackageError::TypeMaterialization(_) => TaskFailureCategory::ResourceExhausted,
        CompiledPackageError::Construction(_) => unreachable!("construction cause handled above"),
        CompiledPackageError::Decode(PackageDecodeError::Constant(
            PhysicalConstantCodecError::Reader(FlatReaderError::Projection(
                FlatPoolResourceError::ConstructionResource(LayoutResourceError::SourceModel)
                | FlatPoolResourceError::SourceMetadata(_),
            )),
        ))
        | CompiledPackageError::Decode(PackageDecodeError::Type(TypeCodecError::ResourceSource(
            _,
        )))
        | CompiledPackageError::Decode(PackageDecodeError::PublicSourceSchema(
            PublicSourceSchemaCodecError::SourceModel(_)
            | PublicSourceSchemaCodecError::Layout(LayoutResourceError::SourceModel),
        ))
        | CompiledPackageError::Decode(PackageDecodeError::Scan(
            ReadScanCodecError::SourceModel(_)
            | ReadScanCodecError::ConstructionResource(LayoutResourceError::SourceModel)
            | ReadScanCodecError::InvalidSourceSchema(
                PublicSourceSchemaCodecError::SourceModel(_)
                | PublicSourceSchemaCodecError::Layout(LayoutResourceError::SourceModel),
            ),
        ))
        | CompiledPackageError::Decode(PackageDecodeError::Schema(
            SchemaCodecError::SourceModel(_)
            | SchemaCodecError::ConstructionResource(LayoutResourceError::SourceModel),
        ))
        | CompiledPackageError::Decode(PackageDecodeError::Writer(
            WriterRecipeCodecError::ConstructionResource(LayoutResourceError::SourceModel),
        ))
        | CompiledPackageError::Decode(PackageDecodeError::Requests(
            CallRequestError::ConstructionResource(LayoutResourceError::SourceModel)
            | CallRequestError::ConstructionTree(BTreeResourceError::SourceModel(_)),
        )) => TaskFailureCategory::Internal,
        CompiledPackageError::Decode(PackageDecodeError::PublicSourceSchema(
            PublicSourceSchemaCodecError::Layout(_),
        ))
        | CompiledPackageError::Decode(PackageDecodeError::Scan(
            ReadScanCodecError::ConstructionResource(_)
            | ReadScanCodecError::InvalidSourceSchema(PublicSourceSchemaCodecError::Layout(_)),
        ))
        | CompiledPackageError::Decode(PackageDecodeError::Schema(
            SchemaCodecError::ConstructionResource(_),
        ))
        | CompiledPackageError::Decode(PackageDecodeError::Writer(
            WriterRecipeCodecError::ConstructionResource(_),
        ))
        | CompiledPackageError::Decode(PackageDecodeError::Requests(
            CallRequestError::ConstructionResource(_) | CallRequestError::ConstructionTree(_),
        )) => TaskFailureCategory::ResourceExhausted,
        CompiledPackageError::Decode(PackageDecodeError::Constant(
            PhysicalConstantCodecError::Reader(FlatReaderError::Projection(
                FlatPoolResourceError::ConstructionResource(_),
            )),
        )) => TaskFailureCategory::ResourceExhausted,
        CompiledPackageError::Decode(PackageDecodeError::PublicSourceSchema(
            PublicSourceSchemaCodecError::Connector(cause),
        ))
        | CompiledPackageError::Decode(PackageDecodeError::Scan(
            ReadScanCodecError::Provider(cause)
            | ReadScanCodecError::InvalidSourceSchema(PublicSourceSchemaCodecError::Connector(cause)),
        )) if cause.kind() == ConnectorErrorKind::ResourceExhausted => {
            TaskFailureCategory::ResourceExhausted
        }
        CompiledPackageError::Decode(_) => TaskFailureCategory::Protocol,
    };
    HostRejection::new(
        category,
        format!("task {identity} package could not be compiled: {error}"),
    )
}

/// An invariant violation inside this binary rather than something a peer can
/// cause.
fn internal(detail: impl AsRef<str>) -> HostRejection {
    HostRejection::new(TaskFailureCategory::Internal, detail)
}

/// Content that arrived well-formed and says something illegal.
fn protocol(detail: impl AsRef<str>) -> HostRejection {
    HostRejection::new(TaskFailureCategory::Protocol, detail)
}

/// Admission already uses the preparation resource category. Keep the exact
/// typed memory cause through this boundary without guessing from its text.
fn memory_admission_failure_to_host(
    identity: TaskIdentity,
    error: &crate::native_fragment_query::NativeFragmentAdmissionError,
) -> HostRejection {
    resource_exhausted(format!("task {identity} could not be admitted: {error}"))
}

fn native_specialization_failure_to_host(
    identity: TaskIdentity,
    cause: &novarocks_functions::FunctionSpecializationFailure,
) -> HostRejection {
    use novarocks_functions::FunctionSpecializationFailure as F;
    use novarocks_type_contract::CompileControlError as C;
    let category = match cause {
        F::Control(C::ResourceExhausted) => TaskFailureCategory::ResourceExhausted,
        F::Control(C::Cancelled | C::DeadlineExceeded) => TaskFailureCategory::Execution,
        F::Kernel(cause) => failure_cause_category(
            &ExecutionFailureCause::Kernel(cause.clone()),
            TaskFailureCategory::Protocol,
        ),
        _ => TaskFailureCategory::Protocol,
    };
    HostRejection::new(
        category,
        format!("task {identity} writer preparation failed: {cause}"),
    )
}

fn native_type_construction_failure_to_host(
    identity: TaskIdentity,
    error: &crate::native_type_construction::NativeTypeConstructionError,
) -> HostRejection {
    use crate::native_type_construction::NativeTypeConstructionError;
    use novarocks_type_contract::CompileControlError;
    use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
    let category = match error {
        NativeTypeConstructionError::Funding(cause) => match cause.as_ref() {
            super::TypeMaterializationRefusal::Stopped(_) => TaskFailureCategory::Execution,
            _ => TaskFailureCategory::ResourceExhausted,
        },
        NativeTypeConstructionError::Control(
            CompileControlError::Cancelled | CompileControlError::DeadlineExceeded,
        ) => TaskFailureCategory::Execution,
        NativeTypeConstructionError::Resource(LayoutResourceError::SourceModel) => {
            TaskFailureCategory::Internal
        }
        NativeTypeConstructionError::Control(CompileControlError::ResourceExhausted)
        | NativeTypeConstructionError::Capacity(_)
        | NativeTypeConstructionError::Resource(_) => TaskFailureCategory::ResourceExhausted,
    };
    HostRejection::new(
        category,
        format!("task {identity} could not construct native types: {error}"),
    )
}

/// This process cannot supply what the task needs right now.
fn resource_exhausted(detail: impl AsRef<str>) -> HostRejection {
    HostRejection::new(TaskFailureCategory::ResourceExhausted, detail)
}

#[cfg(test)]
pub(super) mod tests {
    use super::{
        CompositeFragmentEventSink, FragmentStandDown, NativeRunnableTask, NativeTaskExecutionHost,
        PreparationStopGuard, QueryContextOptions, RootProducerPool, StandDown,
        TaskCompletionSupervisor, TaskOperatorStatisticsSink, TaskQueryContextFacts,
        report_terminal,
    };
    use crate::task_query_context_options::query_options_fingerprint;

    use std::collections::BTreeSet;
    use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use novarocks_execution::exec::fragment::program::{FragmentNodeId, FragmentSinkKind};
    use novarocks_execution::exec::fragment::sink::DataStreamPartitionType;
    use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
    use novarocks_execution::runtime::execution_runtime::{
        ExecutionRuntime, ExecutionRuntimeConfig,
    };
    use novarocks_execution::runtime::fragment::io::{
        ExchangeDestinationKey, FragmentEvent, FragmentEventSink, FragmentProgress,
        NoopFragmentEventSink, UnavailableExchangeReceiverPort,
    };
    use novarocks_execution::runtime::fragment::{
        FragmentCancelReason, FragmentExecutionError, FragmentExecutionErrorKind, FragmentOutcome,
        FragmentTerminalFact,
    };
    use novarocks_execution::runtime::profile::{ProfileUnit, RuntimeProfile};
    use novarocks_execution::runtime::query_options::QueryOptions;
    use novarocks_execution::runtime_filter::RuntimeFilterSessionRef;
    use novarocks_execution_contract::task_execution::creation::{
        CreationContent, FrozenBytes, PreparedTaskFacts, TaskCreationInput,
    };
    use novarocks_execution_contract::task_execution::descriptor::{
        ExchangeDestination, ExchangeEdge, ExchangeInbound, ExchangeSource, ExchangeTopology,
        TaskDescriptor,
    };
    use novarocks_execution_contract::task_execution::domain::{
        CodecOwnedContent, ConfidentialContent, ContentFingerprint, CredentialEpoch,
        CredentialLeaseId, DomainVersion, EdgeOpenVersion, ExchangeEdgeId, PlanNodeId, SplitOffer,
        SplitSequence,
    };
    use novarocks_execution_contract::task_execution::identity::{
        QueryContextRef, TaskIdentity, TaskOperationId,
    };
    use novarocks_execution_contract::task_execution::lease::LeaseValidFor;
    use novarocks_execution_contract::task_execution::operation::{
        AcquireQueryContextAdmissionTicket, CreateTask, CredentialUpdate, EstablishQueryContext,
        OperationEnvelope, OperationKind, OperationOutcome, QueryContextDomainUpdate,
        ReleaseQueryContext, TaskDomainUpdate, UpdateQueryContext,
    };
    use novarocks_execution_contract::task_execution::status::{
        AbortCause, CancelReason, TaskFailureCategory, TaskOutputFacts, TaskState,
        TerminationDetail,
    };
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_models::{connector_read as connector_dto, novarocks as proto, plan};
    use novarocks_spi::connector::ConnectorStopOwner;
    use novarocks_spi::connector::{
        CatalogHandle, ConnectorError, ConnectorErrorKind, ConnectorExecutionReadBinding,
        ConnectorExecutionWriteBinding, ConnectorStorageResolver, ResolvedVendedS3Access,
        StorageAccessRequest,
    };
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use novarocks_types::{NativeCompatibilityId, UniqueId};
    use novarocks_worker::query_context::QueryExecutionKey;
    use novarocks_worker::{InboundFrameClaim, IngressRejection, TaskInboundCapabilities};
    use prost::Message;

    use novarocks_native_adapter::exchange_data_plane::{
        ExchangeRouteQuery, NativeExchangeDataPlane, TaskInboundCapabilitiesRouteAuthority,
    };

    use novarocks_native_adapter::native_fragment_query::NativeFragmentQueryRuntime;
    use novarocks_worker::ProcessMonotonicClock;
    use novarocks_worker::{
        HostRejection, METRIC_PUBLISH_MIN_INTERVAL, ManualClock, QueryContextHost,
        ReleasedContextEvidence, RunnableTask, SharedFactsRequest, StatusAdvance,
        TaskExecutionHost, TaskExecutionRegistry, TaskExecutionRegistryConfig, TaskStatusOwner,
        TaskStatusReporter, TaskStatusSource, WorkerMonotonicClock,
    };

    #[test]
    fn runtime_scalar_memory_nominal_categories_preserve_original_transport() {
        use novarocks_execution::runtime::{
            fragment::{ExecutionFailure, ExecutionFailureCause},
            scalar_memory::{RuntimeScalarEvaluationFailure, RuntimeScalarMemoryRefusal},
        };
        use novarocks_functions::{KernelFailure, ScalarResourceError};
        for (failure, expected) in [
            (
                RuntimeScalarEvaluationFailure::Source(ScalarResourceError::Arithmetic),
                TaskFailureCategory::ResourceExhausted,
            ),
            (
                RuntimeScalarEvaluationFailure::Source(ScalarResourceError::SourceModel(
                    "cancelled ResourceExhausted",
                )),
                TaskFailureCategory::Internal,
            ),
            (
                RuntimeScalarEvaluationFailure::Source(ScalarResourceError::Invariant(
                    "ResourceExhausted",
                )),
                TaskFailureCategory::Internal,
            ),
            (
                RuntimeScalarEvaluationFailure::Host(
                    RuntimeScalarMemoryRefusal::MissingQueryMemory,
                ),
                TaskFailureCategory::ResourceExhausted,
            ),
            (
                RuntimeScalarEvaluationFailure::Host(RuntimeScalarMemoryRefusal::Capacity(
                    novarocks_memory::CapacityError::Invalid {
                        detail: "cancelled Internal",
                    },
                )),
                TaskFailureCategory::ResourceExhausted,
            ),
            (
                RuntimeScalarEvaluationFailure::Kernel(KernelFailure::Cancelled),
                TaskFailureCategory::Execution,
            ),
            (
                RuntimeScalarEvaluationFailure::Kernel(KernelFailure::ResourceExhausted),
                TaskFailureCategory::ResourceExhausted,
            ),
            (
                RuntimeScalarEvaluationFailure::Kernel(KernelFailure::InstanceFailed),
                TaskFailureCategory::Internal,
            ),
        ] {
            let error = ExecutionFailure::from(failure.clone());
            assert_eq!(
                super::failure_cause_category(error.cause(), TaskFailureCategory::Protocol),
                expected
            );
            match (failure, error.cause()) {
                (
                    RuntimeScalarEvaluationFailure::Source(original),
                    ExecutionFailureCause::RuntimeScalarSource(actual),
                ) => assert_eq!(&original, actual),
                (
                    RuntimeScalarEvaluationFailure::Host(original),
                    ExecutionFailureCause::RuntimeScalarMemory(actual),
                ) => assert_eq!(&original, actual),
                (
                    RuntimeScalarEvaluationFailure::Kernel(original),
                    ExecutionFailureCause::Kernel(actual),
                ) => assert_eq!(&original, actual),
                _ => {
                    panic!("nominal source or host cause must survive without text classification")
                }
            }
        }
    }

    #[test]
    fn project_metadata_nominal_categories_match_compile_and_prepare_without_parsing_text() {
        use super::{
            CompiledPackageError, ExecutionFailureCause, compiled_package_rejection,
            failure_cause_category,
        };
        use novarocks_execution::runtime::{
            preparation_memory::{PreparationMemoryRefusal, PreparationMemoryStop},
            preparation_metadata::PreparationMetadataFailure,
        };
        use novarocks_type_contract::{
            CompileControlError, MetadataRequestError, ValueTypeError,
            owned_resources::{
                hashmap::HashMapResourceError,
                metadata_materialization::MetadataMaterializationError,
            },
        };
        let task = identity(371, 1, 1);
        let requests = [
            (
                MetadataRequestError::Control(CompileControlError::Cancelled),
                TaskFailureCategory::Execution,
            ),
            (
                MetadataRequestError::Control(CompileControlError::DeadlineExceeded),
                TaskFailureCategory::Execution,
            ),
            (
                MetadataRequestError::Control(CompileControlError::ResourceExhausted),
                TaskFailureCategory::ResourceExhausted,
            ),
            (
                MetadataRequestError::Arithmetic,
                TaskFailureCategory::ResourceExhausted,
            ),
            (
                MetadataRequestError::SourceModel("ResourceExhausted: cancelled"),
                TaskFailureCategory::Internal,
            ),
            (
                MetadataRequestError::ValueType(ValueTypeError::UnknownLogicalMetadata),
                TaskFailureCategory::Internal,
            ),
            (
                MetadataRequestError::Metadata(MetadataMaterializationError::Arithmetic),
                TaskFailureCategory::ResourceExhausted,
            ),
            (
                MetadataRequestError::Metadata(MetadataMaterializationError::Model(
                    HashMapResourceError::Arithmetic("original arithmetic"),
                )),
                TaskFailureCategory::ResourceExhausted,
            ),
            (
                MetadataRequestError::Metadata(MetadataMaterializationError::Model(
                    HashMapResourceError::SourceModel("cancelled"),
                )),
                TaskFailureCategory::Internal,
            ),
            (
                MetadataRequestError::Metadata(MetadataMaterializationError::ValueType(
                    ValueTypeError::TooDeep,
                )),
                TaskFailureCategory::Internal,
            ),
            (
                MetadataRequestError::Metadata(
                    MetadataMaterializationError::InsertCountExceededOriginalReservation,
                ),
                TaskFailureCategory::Internal,
            ),
            (
                MetadataRequestError::Metadata(
                    MetadataMaterializationError::MissingOriginalFieldOrigin,
                ),
                TaskFailureCategory::Internal,
            ),
        ];
        for (request, expected) in requests {
            let cause = ExecutionFailureCause::PreparationMetadata(
                PreparationMetadataFailure::Request(request),
            );
            assert_eq!(
                failure_cause_category(&cause, TaskFailureCategory::Protocol),
                expected
            );
            assert_eq!(
                compiled_package_rejection(
                    task,
                    CompiledPackageError::ProjectMetadataRequest(request)
                )
                .category(),
                expected
            );
        }
        for stop in [
            PreparationMemoryStop::Cancel(CancelReason::UpstreamNoLongerNeeded),
            PreparationMemoryStop::Abort(AbortCause::QueryFailed),
            PreparationMemoryStop::Abort(AbortCause::LeaseExpired),
            PreparationMemoryStop::Abort(AbortCause::PeerTaskFailed),
        ] {
            let refusal = PreparationMemoryRefusal::Stopped(stop);
            let cause = ExecutionFailureCause::PreparationMetadata(
                PreparationMetadataFailure::Host(refusal.clone()),
            );
            assert_eq!(
                failure_cause_category(&cause, TaskFailureCategory::Internal),
                TaskFailureCategory::Execution
            );
            assert_eq!(
                compiled_package_rejection(task, CompiledPackageError::ProjectMetadata(refusal))
                    .category(),
                TaskFailureCategory::Execution
            );
        }
        for refusal in [
            PreparationMemoryRefusal::MissingQueryMemory,
            PreparationMemoryRefusal::Capacity(novarocks_memory::CapacityError::Invalid {
                detail: "cancelled Internal: original capacity",
            }),
        ] {
            let cause = ExecutionFailureCause::PreparationMetadata(
                PreparationMetadataFailure::Host(refusal.clone()),
            );
            assert_eq!(
                failure_cause_category(&cause, TaskFailureCategory::Internal),
                TaskFailureCategory::ResourceExhausted
            );
            assert_eq!(
                compiled_package_rejection(task, CompiledPackageError::ProjectMetadata(refusal))
                    .category(),
                TaskFailureCategory::ResourceExhausted
            );
        }
        let original = ExecutionFailureCause::Pipeline("ResourceExhausted: cancelled".into());
        for category in [
            TaskFailureCategory::Internal,
            TaskFailureCategory::Protocol,
            TaskFailureCategory::ResourceExhausted,
        ] {
            assert_eq!(failure_cause_category(&original, category), category);
        }
    }

    // ------------------------------------------------------------- fixtures

    fn execution(query: i64) -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(query, query + 1),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("nonzero query id")
    }

    fn identity(query: i64, stage: u32, task: u32) -> TaskIdentity {
        TaskIdentity::new(
            execution(query),
            StageId::new(stage).expect("nonzero stage"),
            TaskId::new(task).expect("nonzero task"),
            BackendProcessId::new_v7(),
        )
    }

    struct FixtureTypeControl;
    impl novarocks_type_contract::PureCompileControl for FixtureTypeControl {
        fn checkpoint(
            &self,
            _: novarocks_type_contract::CompilePhase,
            _: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            Ok(())
        }
    }

    fn fixture_type_reference(id: u32) -> novarocks_proto_models::common::TypeDesc {
        novarocks_proto_models::common::TypeDesc {
            value_binding_id: Some(id),
        }
    }

    fn encode_fixture_type_table(
        declarations: &[(u32, &novarocks_type_contract::FunctionValueType)],
    ) -> plan::ExecutionTypeTable {
        use novarocks_plan_codec::execution_type_table::{
            ExecutionTypeRoot, ExecutionTypeTableBuilder, ExecutionTypeTableLimits,
        };
        use novarocks_plan_codec::host_projection_v2::AdmissionRefusal;
        use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
        let roots = declarations
            .iter()
            .map(|(id, value)| ExecutionTypeRoot { id: *id, value })
            .collect::<Vec<_>>();
        let mut checkpoints =
            CompileCheckpoints::try_new(&FixtureTypeControl, CompilePhase::Encode).unwrap();
        // This is a pure author of known test declarations, with ordinary
        // ownership and no BE grant or invented memory invoice.
        let mut request =
            |_: std::alloc::Layout| Ok::<(), AdmissionRefusal<std::convert::Infallible>>(());
        let table = ExecutionTypeTableBuilder::new(&roots)
            .encode_observed(
                ExecutionTypeTableLimits::for_validated_root_count(roots.len()).unwrap(),
                &mut request,
                &mut checkpoints,
            )
            .unwrap()
            .into_wire();
        checkpoints.finish().unwrap();
        table
    }

    fn encode_fixture_arrow_schema(
        fields: &[(u32, &str, &novarocks_type_contract::FunctionValueType)],
    ) -> plan::ArrowPhysicalSchema {
        let roots = fields
            .iter()
            .enumerate()
            .map(|(id, (_, _, value))| (id as u32, *value))
            .collect::<Vec<_>>();
        let table = encode_fixture_type_table(&roots);
        let columns = fields
            .iter()
            .zip(table.value_bindings)
            .map(
                |((slot_id, name, value), binding)| plan::ArrowPhysicalColumn {
                    slot_id: *slot_id,
                    is_internal: false,
                    field: Some(plan::ArrowPhysicalField {
                        name: (*name).into(),
                        nullable: value.nullable(),
                        r#type: binding.value_layout.map(Box::new),

                        logical_type_id: binding.logical_type_id,
                    }),
                },
            )
            .collect();
        plan::ArrowPhysicalSchema {
            columns,

            logical_types: table.logical_types,
        }
    }

    /// Actual scalar source pools and their independent field namespace. The
    /// fixture uses the same pure IPC author as FE; no inline value decoder or
    /// transported capacity grant participates in BE preparation.
    fn fixture_constant_sources(
        sources: &[(
            u32,
            novarocks_type_contract::FunctionValueType,
            arrow::array::ArrayRef,
        )],
    ) -> (
        novarocks_proto_models::physical_type_v2::SourceFieldTable,
        Vec<novarocks_proto_models::call_request::IpcConstantPool>,
    ) {
        use novarocks_plan_codec::{
            physical_constant_v2::prepare_constant_record_write_pure_in,
            physical_type_v2::{TypeProjectionLimits, encode_source_field_table_pure_in},
        };
        use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
        let mut pools = Vec::new();
        for (id, value, array) in sources {
            let name = format!("host source {id}");
            let field = Arc::new(arrow::datatypes::Field::new(
                &name,
                value.data_type().clone(),
                value.nullable(),
            ));
            pools.push(
                novarocks_functions::ConstantPool::try_new(
                    field,
                    value.clone(),
                    array.to_data(),
                    // The existing canonical scalar fixture envelope. Subject
                    // reader/resource/row limits in each test are unchanged.
                    novarocks_functions::ConstantPolicy {
                        max_rows: u64::try_from(array.len()).unwrap(),
                        max_array_nodes: 1,
                        max_logical_elements: u64::try_from(array.len()).unwrap(),
                        max_retained_buffer_bytes: 4096,
                        max_type_depth: 1,
                        max_type_nodes: 1,
                        max_dictionary_depth: 1,
                        max_metadata_bytes: u64::try_from(name.len()).unwrap(),
                        max_library_validation_work: 65536,
                        max_library_validation_bytes: 65536,
                    },
                    CompilePhase::Validate,
                    &FixtureTypeControl,
                )
                .unwrap(),
            );
        }
        let roots = sources
            .iter()
            .zip(&pools)
            .map(|((id, _, _), pool)| (*id, pool.field_ref()))
            .collect::<Vec<_>>();
        let mut work =
            CompileCheckpoints::try_new(&FixtureTypeControl, CompilePhase::Encode).unwrap();
        let fields = encode_source_field_table_pure_in(
            &roots,
            TypeProjectionLimits::for_validated_source_root_count(roots.len()).unwrap(),
            &mut work,
        )
        .unwrap();
        let records = sources
            .iter()
            .zip(&pools)
            .map(|((id, _, _), pool)| {
                prepare_constant_record_write_pure_in(
                    novarocks_physical_plan::ConstantPoolId::new(*id),
                    *id,
                    *id,
                    pool,
                    &mut work,
                )
                .unwrap()
                .emit_in(&mut work)
                .unwrap()
            })
            .collect();
        work.finish().unwrap();
        (fields, records)
    }

    /// One task's creation body: the static fragment every task of the
    /// fragment shares, and this task's assignment.
    #[derive(Clone)]
    struct Body {
        frozen: proto::FrozenFragment,
        assignment: proto::TaskAssignment,
    }

    impl Body {
        fn bounded_root(output: novarocks_result_contract::FrozenRootOutput, rows: &[i64]) -> Self {
            use novarocks_proto_models::{common, expr};
            let contract = novarocks_result_contract::RootOutputContract::new(
                novarocks_result_contract::RootProfileId::V1,
                output,
            );
            let mut body = Self::with_sink(
                1,
                plan::DataSink {
                    kind: Some(plan::data_sink::Kind::RootResult(
                        novarocks_proto_codec::root_result::encode_root_contract(&contract),
                    )),
                },
                Vec::new(),
            );
            let ty = novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            );
            let table = encode_fixture_type_table(&[(0, &ty)]);
            let data_type = fixture_type_reference(0);
            let column = common::OutputColumn {
                column_id: 10,
                name: "v".into(),
                r#type: Some(data_type.clone()),
                nullable: false,
                is_internal: false,
            };
            let (sources, records) = fixture_constant_sources(&[(
                0,
                ty.clone(),
                Arc::new(arrow::array::Int64Array::from(rows.to_vec())),
            )]);
            let fragment = body.frozen.plan.as_mut().unwrap();
            fragment.constant_source_fields = Some(sources);
            fragment.constants = records;
            fragment.execution_types = Some(table);
            fragment.output_columns = vec![column.clone()];
            let node = fragment.root.as_mut().unwrap();
            let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_mut()
            else {
                unreachable!()
            };
            physical.output_columns = vec![column.clone()];
            physical.kind = Some(plan::plan_node::Kind::Values(plan::ValuesNode {
                columns: vec![column],
                rows: rows
                    .iter()
                    .enumerate()
                    .map(|(ordinal, _)| plan::ExprList {
                        values: vec![expr::Expr {
                            original_definition_id: None,
                            r#type: Some(data_type.clone()),
                            nullable: false,
                            kind: Some(expr::expr::Kind::ConstantReference(
                                novarocks_proto_models::call_request::ConstantReference {
                                    pool_id: Some(0),
                                    row_ordinal: u32::try_from(ordinal).unwrap(),
                                },
                            )),
                        }],
                    })
                    .collect(),
            }));
            body
        }
        fn bounded_statistics() -> Self {
            use arrow::datatypes::{DataType, Field, Schema};
            use novarocks_proto_models::{common, expr};
            let mut body = Self::bounded_root(
                novarocks_result_contract::FrozenRootOutput::InternalFacts(
                    novarocks_result_contract::InternalResultDomain::StatisticsArtifactV1,
                ),
                &[1],
            );
            let root = body.frozen.plan.as_mut().unwrap().root.as_mut().unwrap();
            let mut child = root.clone();
            child.node_id = 9;
            let Some(plan::distributed_node::Payload::Physical(physical)) = child.payload.as_mut()
            else {
                unreachable!()
            };
            let binary = fixture_type_reference(0);
            let column = common::OutputColumn {
                column_id: 10,
                name: "aggregate".into(),
                r#type: Some(binary.clone()),
                nullable: false,
                is_internal: false,
            };
            physical.output_columns = vec![column.clone()];
            physical.kind = Some(plan::plan_node::Kind::Values(plan::ValuesNode {
                columns: vec![column],
                rows: vec![plan::ExprList {
                    values: vec![expr::Expr {
                        original_definition_id: None,
                        r#type: Some(binary),
                        nullable: false,
                        kind: Some(expr::expr::Kind::ConstantReference(
                            novarocks_proto_models::call_request::ConstantReference {
                                pool_id: Some(0),
                                row_ordinal: 0,
                            },
                        )),
                    }],
                }],
            }));
            let label = expr::Expr {
                original_definition_id: None,
                r#type: Some(fixture_type_reference(1)),
                nullable: false,
                kind: Some(expr::expr::Kind::ConstantReference(
                    novarocks_proto_models::call_request::ConstantReference {
                        pool_id: Some(1),
                        row_ordinal: 0,
                    },
                )),
            };
            let entries = Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Utf8, false)),
                        Arc::new(Field::new("value", DataType::Utf8, false)),
                    ]
                    .into(),
                ),
                false,
            ));
            let schema = Schema::new(vec![
                Field::new(
                    "input_fields",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                    false,
                ),
                Field::new("blob_type", DataType::Utf8, false),
                Field::new("body", DataType::Binary, false),
                Field::new("properties", DataType::Map(entries, false), false),
            ]);
            use novarocks_type_contract::{
                FunctionValueType, ValueKind, ValueLayout, ValueTypeRef,
                ValueTypeStorageLifetime as S,
            };
            let binary_value = FunctionValueType::new(DataType::Binary, false);
            let string_value = FunctionValueType::new(DataType::Utf8, false);
            let item = ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int32), false, S::pure())
                .unwrap();
            let list = FunctionValueType::try_from_value(
                ValueTypeRef::try_new(ValueKind::List(item), false, S::pure()).unwrap(),
                ValueLayout::try_new(schema.field(0).data_type(), S::pure()).unwrap(),
            )
            .unwrap();
            let string = ValueTypeRef::try_new(ValueKind::String, false, S::pure()).unwrap();
            let properties = FunctionValueType::try_from_value(
                ValueTypeRef::try_new(
                    ValueKind::Map {
                        key: string.clone(),
                        value: string,
                    },
                    false,
                    S::pure(),
                )
                .unwrap(),
                ValueLayout::try_new(schema.field(3).data_type(), S::pure()).unwrap(),
            )
            .unwrap();
            let output_schema = encode_fixture_arrow_schema(&[
                (1, "input_fields", &list),
                (2, "blob_type", &string_value),
                (3, "body", &binary_value),
                (4, "properties", &properties),
            ]);
            let execution_types = encode_fixture_type_table(&[
                (0, &binary_value),
                (1, &string_value),
                (2, &list),
                (3, &string_value),
                (4, &binary_value),
                (5, &properties),
            ]);
            root.children = vec![child];
            root.payload = Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns: Vec::new(),
                kind: Some(plan::plan_node::Kind::Unpivot(plan::UnpivotNode {
                    passthrough_columns: Vec::new(),
                    value_output_column_id: 3,
                    literal_output_column_ids: vec![1, 2, 4],
                    value_mappings: vec![plan::UnpivotValueMapping {
                        input_value_column_id: 10,
                        constants: vec![
                            plan::UnpivotConstant {
                                value: Some(plan::unpivot_constant::Value::Int32List(
                                    plan::Int32List { values: vec![7] },
                                )),
                            },
                            plan::UnpivotConstant {
                                value: Some(plan::unpivot_constant::Value::ScalarLiteral(label)),
                            },
                            plan::UnpivotConstant {
                                value: Some(plan::unpivot_constant::Value::Utf8Map(
                                    plan::Utf8Map {
                                        entries: Vec::new(),
                                    },
                                )),
                            },
                        ],
                    }],
                    max_output_rows: 4096,
                    max_output_bytes: 32 << 20,
                    output_schema: Some(output_schema),
                })),
            }));
            let (sources, records) = fixture_constant_sources(&[
                (
                    0,
                    binary_value,
                    Arc::new(arrow::array::BinaryArray::from(vec![
                        [255_u8, 0, 3].as_slice(),
                    ])),
                ),
                (
                    1,
                    string_value,
                    Arc::new(arrow::array::StringArray::from(vec!["theta"])),
                ),
            ]);
            let fragment = body.frozen.plan.as_mut().unwrap();
            fragment.constant_source_fields = Some(sources);
            fragment.constants = records;
            fragment.execution_types = Some(execution_types);
            fragment.output_columns = [
                (1, "input_fields", 2),
                (2, "blob_type", 3),
                (3, "body", 4),
                (4, "properties", 5),
            ]
            .into_iter()
            .map(|(column_id, name, binding)| common::OutputColumn {
                column_id,
                name: name.into(),
                r#type: Some(fixture_type_reference(binding)),
                nullable: false,
                is_internal: false,
            })
            .collect();
            body
        }

        /// A decodable, self-contained fragment: one VALUES node into a NOOP
        /// sink, frozen for exactly `pipeline_dop`.
        fn values(pipeline_dop: u32) -> Self {
            Self::with_sink(
                pipeline_dop,
                plan::DataSink {
                    kind: Some(plan::data_sink::Kind::Noop(true)),
                },
                Vec::new(),
            )
        }

        fn with_sink(pipeline_dop: u32, sink: plan::DataSink, sink_edge_ids: Vec<u32>) -> Self {
            Self {
                frozen: proto::FrozenFragment {
                    package: Default::default(),
                    plan_version: vec![1; 16],
                    plan_contract_revision: 1,
                    fragment_contract_version: 1,
                    pipeline_dop_domain: Some(proto::PipelineDopDomain {
                        min: pipeline_dop,
                        max: pipeline_dop,
                        requires_power_of_two: false,
                    }),
                    plan: Some(plan::PlanFragment {
                        fragment_id: 7,
                        execution_types: Some(plan::ExecutionTypeTable::default()),
                        // These closed fixtures contain no ordinary function or
                        // aggregate binding requests, and no IPC constant pools.
                        original_call_requests: Some(
                            novarocks_proto_models::call_request::FragmentCallRequests {
                                entries: Vec::new(),
                            },
                        ),
                        constants: Vec::new(),
                        constant_source_fields: Some(
                            novarocks_proto_models::physical_type_v2::SourceFieldTable {
                                shape: Some(
                                    novarocks_proto_models::physical_type_v2::TypeTable::default(),
                                ),
                                field_facts: Vec::new(),
                            },
                        ),
                        root: Some(plan::DistributedNode {
                            node_id: 10,
                            fragment_id: 7,
                            limit: -1,
                            payload: Some(plan::distributed_node::Payload::Physical(
                                plan::PlanNode {
                                    output_columns: Vec::new(),
                                    kind: Some(plan::plan_node::Kind::Values(plan::ValuesNode {
                                        rows: Vec::new(),
                                        columns: Vec::new(),
                                    })),
                                },
                            )),
                            ..Default::default()
                        }),
                        sink: Some(sink),
                        runtime_filter_bindings: Some(plan::RuntimeFilterBindingTable {
                            fragment_id: 7,
                            bindings: Vec::new(),
                        }),
                        ..Default::default()
                    }),
                },
                assignment: proto::TaskAssignment {
                    instance_ordinal: 3,
                    initial_scan_ranges: Vec::new(),
                    sink_edge_ids,
                },
            }
        }

        fn frozen_bytes(&self) -> FrozenBytes {
            FrozenBytes::freeze(self.frozen.encode_to_vec().into())
        }

        /// The input a creation winner receives, with its assignment decoded
        /// by the codec's own assignment decoder against `descriptor`.
        fn input(&self, descriptor: &TaskDescriptor) -> TaskCreationInput {
            let assignment = novarocks_task_codec::creation::decode_task_assignment(
                self.assignment.clone(),
                descriptor,
                FieldPath::root("assignment"),
            )
            .expect("a structurally legal assignment");
            TaskCreationInput::new(self.frozen_bytes(), Box::new(assignment))
        }
    }

    /// An assignment this backend's codec did not produce.
    #[derive(Debug)]
    struct ForeignAssignment;

    impl CreationContent for ForeignAssignment {
        fn retained_bytes(&self) -> usize {
            std::mem::size_of::<Self>()
        }
        fn encoded_len(&self) -> usize {
            16
        }

        fn into_stored(self: Box<Self>) -> Box<dyn std::any::Any + Send> {
            self
        }
    }

    fn descriptor_with(
        identity: TaskIdentity,
        kernel_key: UniqueId,
        pipeline_dop: usize,
        topology: ExchangeTopology,
    ) -> TaskDescriptor {
        TaskDescriptor::try_new(
            identity,
            kernel_key,
            NonZeroUsize::new(pipeline_dop).expect("nonzero dop"),
            vec![PlanNodeId::new(10).expect("nonnegative node")],
            topology,
        )
        .expect("a legal descriptor")
    }

    /// A single-driver descriptor with no exchange topology.
    fn consistent_descriptor(identity: TaskIdentity, kernel_key: UniqueId) -> TaskDescriptor {
        descriptor_with(identity, kernel_key, 1, ExchangeTopology::default())
    }

    fn establish_fixture_resources(
        queries: &NativeFragmentQueryRuntime,
        facts: &dyn TaskQueryContextFacts,
        execution: QueryExecutionId,
    ) -> Result<(), HostRejection> {
        let options = facts.query_options(execution)?;
        let (delivery, query) = novarocks_execution::runtime::query_options::query_expire_durations(
            Some(options.runtime()),
        );
        queries
            .establish_query_resources(
                execution,
                delivery,
                query,
                options.runtime().exec_mem_limit(),
            )
            .map(|_| ())
            .map_err(|error| {
                HostRejection::new(
                    TaskFailureCategory::ResourceExhausted,
                    format!("fixture context could not establish execution resources: {error}"),
                )
            })
    }

    fn establish_fixture_context(host: &NativeTaskExecutionHost, identity: TaskIdentity) {
        establish_fixture_resources(
            &host.queries,
            host.context_facts.as_ref(),
            identity.query_execution_id(),
        )
        .expect("the declared fixture context establishes before its task");
    }

    /// Prepares `descriptor` from a VALUES body frozen for its parallelism.
    fn install(
        host: &NativeTaskExecutionHost,
        descriptor: &TaskDescriptor,
    ) -> Result<PreparedTaskFacts, HostRejection> {
        establish_fixture_context(host, descriptor.identity());
        let dop = u32::try_from(descriptor.pipeline_dop().get()).expect("small dop");
        host.install_receiver(
            descriptor,
            Body::values(dop).input(descriptor),
            novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
        )
        .map(|installed| installed.facts())
    }

    fn inbound_topology(node: FragmentNodeId, sources: Vec<ExchangeSource>) -> ExchangeTopology {
        ExchangeTopology::try_new(
            Vec::new(),
            vec![ExchangeInbound::try_new(node, sources).expect("a legal inbound")],
        )
        .expect("a legal topology")
    }

    fn claim_frame(
        capabilities: &TaskInboundCapabilities,
        query: ExchangeRouteQuery,
    ) -> InboundFrameClaim {
        capabilities.claim_frame(
            query.destination_fragment_instance_id,
            FragmentNodeId::new(query.destination_node_id),
            query.source_fragment_instance_id,
            query.sender_ordinal,
            query.sender_count,
        )
    }

    fn outbound_topology(edge: ExchangeEdgeId, target: TaskIdentity) -> ExchangeTopology {
        let destination = ExchangeDestination::new(
            target,
            UniqueId::new(900, 901),
            RuntimeEndpoint::new("127.0.0.1", 9060).expect("a legal endpoint"),
            FragmentNodeId::new(11),
        );
        ExchangeTopology::try_new(
            vec![
                ExchangeEdge::try_new(
                    edge,
                    FragmentNodeId::new(11),
                    DataStreamPartitionType::Unpartitioned,
                    vec![destination],
                    0,
                    NonZeroU32::new(1).expect("nonzero"),
                )
                .expect("a legal edge"),
            ],
            Vec::new(),
        )
        .expect("a legal topology")
    }

    /// A stream sink sending one unpartitioned stream to exchange node 11.
    fn stream_sink(partition: plan::PartitionKind) -> plan::DataSink {
        plan::DataSink {
            kind: Some(plan::data_sink::Kind::DataStream(plan::DataStreamSink {
                dest_node_id: 11,
                output_partition: Some(plan::DataPartition {
                    kind: partition as i32,
                    exprs: Vec::new(),
                }),
                ..Default::default()
            })),
        }
    }

    /// A context host that answers every query-scoped question with the
    /// smallest legal value, and counts what it was asked.
    struct StubContextFacts {
        memory_authority: Arc<novarocks_memory::MemoryAuthority>,
        query_manager: Arc<novarocks_worker::query_context::QueryContextManager>,
        query_options: Mutex<QueryOptions>,
        query_options_fingerprint: Mutex<ContentFingerprint>,
        filter_sessions_requested: AtomicUsize,
        /// The binding set of every compiled task's session request, in
        /// request order.
        compiled_filter_bindings: Mutex<Vec<BTreeSet<u32>>>,
        dynamic_filters_delivered: AtomicUsize,
        /// Every task this host offered as the context's feedback carrier.
        ///
        /// It is the supply observable of the backend half of the dynamic
        /// filter loop: it records the offer, so a submission path that stopped
        /// making one is visible from here.
        feedback_carriers: Mutex<Vec<TaskIdentity>>,
    }

    impl Default for StubContextFacts {
        fn default() -> Self {
            let wire = proto::QueryOptions {
                pipeline_dop: 1,
                ..proto::QueryOptions::default()
            };
            Self {
                memory_authority:
                    novarocks_native_adapter::backend_test_support::test_memory_authority(),
                query_manager: novarocks_worker::query_context::QueryContextManager::new_for_test(),
                query_options: Mutex::new(QueryOptions {
                    pipeline_dop: Some(1),
                    ..QueryOptions::default()
                }),
                query_options_fingerprint: Mutex::new(query_options_fingerprint(wire)),
                filter_sessions_requested: AtomicUsize::new(0),
                compiled_filter_bindings: Mutex::new(Vec::new()),
                dynamic_filters_delivered: AtomicUsize::new(0),
                feedback_carriers: Mutex::new(Vec::new()),
            }
        }
    }

    impl StubContextFacts {
        fn set_context_dop(&self, pipeline_dop: i32) {
            self.query_options
                .lock()
                .expect("stub query options")
                .pipeline_dop = Some(pipeline_dop);
            self.set_wire_options(proto::QueryOptions {
                pipeline_dop,
                ..Default::default()
            });
        }

        fn set_wire_options(&self, wire: proto::QueryOptions) {
            *self
                .query_options_fingerprint
                .lock()
                .expect("stub query options fingerprint") = query_options_fingerprint(wire);
        }
    }

    impl TaskQueryContextFacts for StubContextFacts {
        fn query_options(
            &self,
            _execution: QueryExecutionId,
        ) -> Result<QueryContextOptions, HostRejection> {
            Ok(QueryContextOptions::new(
                Arc::new(
                    self.query_options
                        .lock()
                        .expect("stub query options")
                        .clone(),
                ),
                *self
                    .query_options_fingerprint
                    .lock()
                    .expect("stub query options fingerprint"),
            ))
        }

        fn runtime_filter_session(
            &self,
            _execution: QueryExecutionId,
            _fragment_instance_id: UniqueId,
            _expects_bindings: bool,
        ) -> Result<Option<RuntimeFilterSessionRef>, HostRejection> {
            self.filter_sessions_requested
                .fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }

        fn runtime_filter_session_for_bindings(
            &self,
            _execution: QueryExecutionId,
            _fragment_instance_id: UniqueId,
            bindings: &BTreeSet<u32>,
        ) -> Result<Option<RuntimeFilterSessionRef>, HostRejection> {
            self.compiled_filter_bindings
                .lock()
                .expect("stub compiled filter bindings")
                .push(bindings.clone());
            // No participant is installed in this stub, so it answers exactly
            // as the real host answers a context that installed none.
            if bindings.is_empty() {
                Ok(None)
            } else {
                Err(HostRejection::new(
                    TaskFailureCategory::Protocol,
                    format!("stub context installed no participant for {bindings:?}"),
                ))
            }
        }

        fn runtime_filter_event_sink(
            &self,
            _execution: QueryExecutionId,
            _fragment_instance_id: UniqueId,
        ) -> Arc<dyn FragmentEventSink> {
            Arc::new(NoopFragmentEventSink)
        }

        fn bind_runtime_filter_feedback(
            &self,
            _execution: QueryExecutionId,
            carrier: TaskIdentity,
            _reporter: &TaskStatusReporter,
        ) -> bool {
            self.feedback_carriers
                .lock()
                .expect("stub feedback carriers")
                .push(carrier);
            // No participant is installed in this stub, so the offer is
            // recorded and declined exactly as the real host would decline it.
            false
        }

        fn deliver_task_dynamic_filter(
            &self,
            _execution: QueryExecutionId,
            _fragment_instance_id: UniqueId,
            _version: DomainVersion,
            _payload: &Arc<dyn CodecOwnedContent>,
        ) -> Result<(), HostRejection> {
            self.dynamic_filters_delivered
                .fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn catalog_read_execution(
            &self,
            _execution: QueryExecutionId,
            _handle: &CatalogHandle,
        ) -> Result<ConnectorExecutionReadBinding, String> {
            Err("this fixture binds no catalog".to_owned())
        }

        fn catalog_write_execution(
            &self,
            _execution: QueryExecutionId,
            _handle: &CatalogHandle,
        ) -> Result<ConnectorExecutionWriteBinding, String> {
            Err("this fixture binds no catalog".to_owned())
        }

        fn storage_resolver(
            &self,
            _execution: QueryExecutionId,
        ) -> Result<Arc<dyn ConnectorStorageResolver>, HostRejection> {
            Ok(Arc::new(RefusingStorageResolver))
        }
    }

    /// The fixture vends no credential, so every storage request is refused
    /// rather than answered with a placeholder secret.
    struct RefusingStorageResolver;

    impl ConnectorStorageResolver for RefusingStorageResolver {
        fn resolve_vended_s3(
            &self,
            _request: &StorageAccessRequest,
        ) -> Result<ResolvedVendedS3Access, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "this fixture vends no storage credential",
            ))
        }
    }

    fn test_execution_runtime(
        memory_authority: Arc<novarocks_memory::MemoryAuthority>,
    ) -> Arc<ExecutionRuntime> {
        let mut function_set =
            novarocks_execution::exec::expr::agg::ExecutionFunctionSetBuilder::new();
        novarocks_sql::compiler::contribute_builtin_functions(function_set.catalog_builder_mut())
            .expect("builtin function metadata");
        novarocks_execution::exec::expr::agg::contribute_builtin_aggregate_implementations(
            &mut function_set,
        )
        .expect("builtin aggregate implementations");
        let function_set = Arc::new(function_set.seal().expect("builtin execution function set"));
        Arc::new(
            ExecutionRuntime::new(
                ExecutionRuntimeConfig {
                    driver_threads: 1,
                    exchange_wait_ms: 120_000,
                    exchange_io_threads: 1,
                    exchange_io_max_inflight_bytes: 1,
                    exchange_max_transmit_batched_bytes: 1,
                    operator_buffer_chunks: 1,
                    local_exchange_buffer_mem_limit_per_driver: 1,
                    local_exchange_max_buffered_rows: 1,
                    runtime_filter_scan_wait_time_ms_override: None,
                    runtime_filter_wait_timeout_ms_override: None,
                    sink_io_worker_threads: 1,
                    sink_io_max_blocking_threads: 1,
                },
                function_set,
                memory_authority,
            )
            .expect("test execution runtime"),
        )
    }

    fn host(facts: Arc<StubContextFacts>) -> NativeTaskExecutionHost {
        let limits = novarocks_worker::WorkerResultRetainedLimits::try_new(
            256 * 1024 * 1024,
            4 * 1024 * 1024 * 1024,
        )
        .expect("valid root test limits");
        let budget =
            novarocks_worker::result_buffer::ResultRetainedBudget::new(limits.per_process());
        let pool = RootProducerPool::try_new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(64).unwrap(),
            1024 * 1024,
            Arc::clone(&budget),
        )
        .expect("finite test producer pool");
        let data_runtime =
            novarocks_native_adapter::backend_test_support::test_backend_data_runtime();
        let completion_supervisor =
            TaskCompletionSupervisor::start(data_runtime.handle().clone(), 64);
        let runtime = test_execution_runtime(Arc::clone(&facts.memory_authority));
        NativeTaskExecutionHost::new(
            NativeFragmentQueryRuntime::new_for_test(
                Arc::clone(&facts.query_manager),
                Arc::clone(&facts.memory_authority),
            ),
            facts,
            TaskInboundCapabilities::new(),
            novarocks_native_adapter::exchange_transmitter::grpc_exchange_transmitter(
                data_runtime,
                Duration::from_millis(120_000),
                #[cfg(novarocks_dev_controls)]
                None,
            ),
            novarocks_native_adapter::fragment_result_writer::native_result_writer(
                Arc::clone(&budget),
                limits.per_root(),
            ),
            budget,
            limits,
            pool,
            Arc::new(UnavailableExchangeReceiverPort),
            Arc::new(novarocks_worker::sink_commit::WorkerSinkCommitPort),
            runtime,
            novarocks_worker::ScanStreamHost::new(
                novarocks_worker::ScanPreparationConfig::default(),
                novarocks_native_adapter::backend_test_support::test_scan_stream_runtime(),
            ),
            completion_supervisor,
        )
    }

    // ---------------------------------------------- inbound capability tests

    #[test]
    fn an_inbound_frame_is_admitted_only_while_its_task_holds_a_capability() {
        let consumer = identity(10, 1, 1);
        let producer = identity(10, 2, 1);
        let producer_key = UniqueId::new(21, 22);
        let node = FragmentNodeId::new(11);
        let kernel_key = UniqueId::new(31, 32);
        let descriptor = descriptor_with(
            consumer,
            kernel_key,
            1,
            inbound_topology(node, vec![ExchangeSource::new(producer, producer_key, 0)]),
        );
        let capabilities = TaskInboundCapabilities::new();

        // Before installation the destination simply does not exist.
        assert_eq!(
            capabilities.authorize_frame(kernel_key, node, producer_key, 0, 1),
            Err(IngressRejection::UnknownDestinationTask)
        );

        capabilities
            .install(Arc::new(descriptor.clone()))
            .expect("first claim on this kernel key");
        let admission = capabilities
            .authorize_frame(kernel_key, node, producer_key, 0, 1)
            .expect("the frozen route is admitted");
        assert_eq!(admission.destination(), consumer);
        assert_eq!(
            admission.source(),
            ExchangeSource::new(producer, producer_key, 0)
        );

        // Retirement must actually close the door: a capability that outlives
        // its task keeps admitting frames into a receiver nothing drains.
        capabilities.remove(&descriptor);
        assert_eq!(
            capabilities.authorize_frame(kernel_key, node, producer_key, 0, 1),
            Err(IngressRejection::UnknownDestinationTask)
        );
    }

    #[test]
    fn a_frame_from_a_sender_the_topology_never_froze_is_refused() {
        let consumer = identity(11, 1, 1);
        let producer = identity(11, 2, 1);
        let producer_key = UniqueId::new(41, 42);
        let node = FragmentNodeId::new(11);
        let kernel_key = UniqueId::new(51, 52);
        let capabilities = TaskInboundCapabilities::new();
        capabilities
            .install(Arc::new(descriptor_with(
                consumer,
                kernel_key,
                1,
                inbound_topology(node, vec![ExchangeSource::new(producer, producer_key, 0)]),
            )))
            .expect("a legal install");

        // A forged or stale sender key must not reach the receiver.
        assert_eq!(
            capabilities.authorize_frame(kernel_key, node, UniqueId::new(99, 99), 0, 1),
            Err(IngressRejection::SourceNotFrozen)
        );
        // An inflated sender count would make the receiver wait for senders
        // that will never exist.
        assert_eq!(
            capabilities.authorize_frame(kernel_key, node, producer_key, 0, 2),
            Err(IngressRejection::SenderCountMismatch {
                expected: 1,
                received: 2,
            })
        );
    }

    #[test]
    fn closing_a_context_fences_an_installed_capability_and_any_late_install() {
        let consumer = identity(16, 1, 1);
        let producer = identity(16, 2, 1);
        let producer_key = UniqueId::new(121, 122);
        let node = FragmentNodeId::new(11);
        let kernel_key = UniqueId::new(131, 132);
        let descriptor = descriptor_with(
            consumer,
            kernel_key,
            1,
            inbound_topology(node, vec![ExchangeSource::new(producer, producer_key, 0)]),
        );
        let context = QueryContextRef::new(
            consumer.query_execution_id(),
            FrontendProcessId::new_v7(),
            consumer.backend_process_id(),
        );
        let capabilities = TaskInboundCapabilities::new();
        capabilities
            .install(Arc::new(descriptor.clone()))
            .expect("a legal install");

        capabilities.close_context(context);
        let claim = claim_frame(
            &capabilities,
            ExchangeRouteQuery {
                destination_fragment_instance_id: kernel_key,
                destination_node_id: node.get(),
                source_fragment_instance_id: producer_key,
                sender_ordinal: 0,
                sender_count: 1,
            },
        );
        let InboundFrameClaim::Refused(detail) = claim else {
            panic!("a closed context must refuse its still-installed destination");
        };
        assert!(
            detail.contains("data-plane admission is closed"),
            "{detail}"
        );
        assert_eq!(
            capabilities.authorize_frame(kernel_key, node, producer_key, 0, 1),
            Err(IngressRejection::UnknownDestinationTask)
        );

        capabilities.remove(&descriptor);
        let late = identity(16, 1, 2);
        let late_key = UniqueId::new(141, 142);
        let late_descriptor = descriptor_with(
            late,
            late_key,
            1,
            inbound_topology(node, vec![ExchangeSource::new(producer, producer_key, 0)]),
        );
        let rejection = capabilities
            .install(Arc::new(late_descriptor))
            .expect_err("a create already in flight cannot reopen a closed context");
        assert!(rejection.detail().as_str().contains("context closed"));
    }

    /// The defect this catches: the exchange data plane asked only the
    /// fragment-based lifecycle registry, which has no entry and no
    /// participant manifest for a query that runs on the task protocol. Every
    /// cross-backend frame of every such query was refused as "absent from
    /// every active participant manifest", so every distributed query with an
    /// exchange failed. This is the authority the data plane needs instead:
    /// the task substrate answers from the frozen descriptor, distinguishing
    /// a destination it does not hold from a route it holds and refuses.
    #[test]
    fn the_task_substrate_claims_its_own_destinations_and_refuses_each_mismatch_by_reason() {
        let consumer = identity(14, 1, 1);
        let producer = identity(14, 2, 1);
        let producer_key = UniqueId::new(81, 82);
        let node = FragmentNodeId::new(11);
        let kernel_key = UniqueId::new(91, 92);
        let descriptor = descriptor_with(
            consumer,
            kernel_key,
            1,
            inbound_topology(node, vec![ExchangeSource::new(producer, producer_key, 0)]),
        );
        let capabilities = TaskInboundCapabilities::new();

        let query = |destination: UniqueId,
                     destination_node_id: i32,
                     source: UniqueId,
                     sender_ordinal: u32,
                     sender_count: u32| {
            ExchangeRouteQuery {
                destination_fragment_instance_id: destination,
                destination_node_id,
                source_fragment_instance_id: source,
                sender_ordinal,
                sender_count,
            }
        };

        // With nothing installed this owner holds no destination. It must not
        // refuse, because the fragment lifecycle may hold the same frame.
        assert_eq!(
            claim_frame(
                &capabilities,
                query(kernel_key, node.get(), producer_key, 0, 1)
            ),
            InboundFrameClaim::NotHeld
        );

        capabilities
            .install(Arc::new(descriptor.clone()))
            .expect("first claim on this kernel key");

        // The frozen topology authorizes exactly the route the frontend froze.
        assert_eq!(
            claim_frame(
                &capabilities,
                query(kernel_key, node.get(), producer_key, 0, 1)
            ),
            InboundFrameClaim::Authorized
        );

        // A destination this process never created stays a disclaimer, not a
        // refusal: another owner may hold it.
        assert_eq!(
            claim_frame(
                &capabilities,
                query(UniqueId::new(1, 1), node.get(), producer_key, 0, 1),
            ),
            InboundFrameClaim::NotHeld
        );

        // Every remaining mismatch is a refusal by this owner, each carrying
        // its own reason so an operator is not told the wrong thing.
        for (case, claim) in [
            (
                "unknown node",
                claim_frame(&capabilities, query(kernel_key, 12, producer_key, 0, 1)),
            ),
            (
                "unknown source key",
                claim_frame(
                    &capabilities,
                    query(kernel_key, node.get(), UniqueId::new(99, 99), 0, 1),
                ),
            ),
            (
                "wrong sender count",
                claim_frame(
                    &capabilities,
                    query(kernel_key, node.get(), producer_key, 0, 2),
                ),
            ),
            (
                "ordinal above the frozen count",
                claim_frame(
                    &capabilities,
                    query(kernel_key, node.get(), producer_key, 3, 1),
                ),
            ),
        ] {
            let InboundFrameClaim::Refused(detail) = claim else {
                panic!("{case} must be refused by the holding owner, got {claim:?}");
            };
            assert!(
                detail.contains(&consumer.to_string()),
                "{case} refusal must name the holding task: {detail}"
            );
        }

        let unknown_node = claim_frame(&capabilities, query(kernel_key, 12, producer_key, 0, 1));
        let unknown_source = claim_frame(
            &capabilities,
            query(kernel_key, node.get(), UniqueId::new(99, 99), 0, 1),
        );
        let wrong_count = claim_frame(
            &capabilities,
            query(kernel_key, node.get(), producer_key, 0, 2),
        );
        assert_ne!(unknown_node, unknown_source);
        assert_ne!(unknown_source, wrong_count);
        assert_ne!(unknown_node, wrong_count);

        // Retirement withdraws the claim rather than converting it into a
        // refusal, so a frame arriving after teardown reads as unowned.
        capabilities.remove(&descriptor);
        assert_eq!(
            claim_frame(
                &capabilities,
                query(kernel_key, node.get(), producer_key, 0, 1)
            ),
            InboundFrameClaim::NotHeld
        );
    }

    /// The same defect, asserted at the composition boundary the cluster
    /// actually runs: the RPC data plane must reach the task substrate. Before
    /// this wiring existed the plane held only the fragment lifecycle owner,
    /// so a created task's destination was refused as absent from every
    /// participant manifest and no distributed query with an exchange could
    /// move a single frame.
    #[test]
    fn the_composed_data_plane_admits_a_frame_only_the_task_substrate_holds() {
        let consumer = identity(15, 1, 1);
        let producer = identity(15, 2, 1);
        let producer_key = UniqueId::new(101, 102);
        let node = FragmentNodeId::new(11);
        let kernel_key = UniqueId::new(111, 112);
        let capabilities = TaskInboundCapabilities::new();
        capabilities
            .install(Arc::new(descriptor_with(
                consumer,
                kernel_key,
                1,
                inbound_topology(node, vec![ExchangeSource::new(producer, producer_key, 0)]),
            )))
            .expect("a legal install");

        let plane = NativeExchangeDataPlane::new(
            Arc::new(UnavailableExchangeReceiverPort),
            vec![Arc::new(TaskInboundCapabilitiesRouteAuthority::new(
                Arc::clone(&capabilities),
            ))],
        );
        let request = |destination: UniqueId, source: UniqueId| proto::ExchangeRequest {
            finst_id_hi: destination.high(),
            finst_id_lo: destination.low(),
            node_id: node.get(),
            source_finst_id_hi: source.high(),
            source_finst_id_lo: source.low(),
            sender_ordinal: 0,
            sender_count: 1,
            sender_id: 7,
            be_number: 0,
            eos: false,
            sequence: 1,
            payload: vec![0x01],
        };

        // The receiver port is deliberately unavailable, so reaching delivery
        // is the proof the route was authorized.
        let status = plane
            .transmit(request(kernel_key, producer_key))
            .status
            .expect("status");
        assert_eq!(status.code, 1);
        assert!(
            status.message.contains("exchange ingress failed"),
            "the composed plane must admit a task-held route: {}",
            status.message
        );

        // A destination neither owner holds is still refused, and the refusal
        // names no owner.
        let status = plane
            .transmit(request(UniqueId::new(1, 1), producer_key))
            .status
            .expect("status");
        assert_eq!(status.code, 1);
        assert!(
            status
                .message
                .contains("no active exchange owner on this backend holds this destination"),
            "unexpected refusal: {}",
            status.message
        );
    }

    #[test]
    fn two_tasks_cannot_claim_one_kernel_key() {
        let kernel_key = UniqueId::new(61, 62);
        let first = identity(12, 1, 1);
        let second = identity(12, 1, 2);
        let capabilities = TaskInboundCapabilities::new();
        capabilities
            .install(Arc::new(consistent_descriptor(first, kernel_key)))
            .expect("first claim");

        // Silently replacing the entry would re-route the first task's frames
        // to the second task's receivers.
        let rejection = capabilities
            .install(Arc::new(consistent_descriptor(second, kernel_key)))
            .expect_err("a kernel key has exactly one owner");
        assert!(
            rejection.detail().as_str().contains("already holds"),
            "{rejection}"
        );
        assert_eq!(
            capabilities
                .authorize_frame(kernel_key, FragmentNodeId::new(11), kernel_key, 0, 1)
                .unwrap_err(),
            IngressRejection::UnknownDestinationNode(FragmentNodeId::new(11)),
            "the original owner still holds the key"
        );
    }

    #[test]
    fn withdrawing_a_capability_never_removes_another_tasks_entry() {
        let kernel_key = UniqueId::new(71, 72);
        let holder = identity(13, 1, 1);
        let stranger = identity(13, 1, 2);
        let capabilities = TaskInboundCapabilities::new();
        capabilities
            .install(Arc::new(consistent_descriptor(holder, kernel_key)))
            .expect("a legal install");

        // A rollback and a retirement can name the same kernel key at
        // different times; the loser must not evict the winner.
        capabilities.remove(&consistent_descriptor(stranger, kernel_key));
        assert_eq!(capabilities.len(), 1);
        capabilities.remove(&consistent_descriptor(holder, kernel_key));
        assert!(capabilities.is_empty());
    }

    // ------------------------------------------------------- stand-down race

    #[derive(Default)]
    struct RecordingStandDown {
        reasons: Mutex<Vec<String>>,
    }

    impl FragmentStandDown for RecordingStandDown {
        fn cancel(&self, reason: FragmentCancelReason) {
            self.reasons
                .lock()
                .expect("reasons")
                .push(reason.detail().to_owned());
        }
    }

    #[test]
    fn a_stand_down_before_the_fragment_starts_is_replayed_when_it_attaches() {
        let stop = ConnectorStopOwner::new();
        let task = NativeRunnableTask::new_without_receiver_with_stop(
            identity(14, 1, 1),
            UniqueId::new(81, 82),
            stop.clone(),
        );
        let handle = Arc::new(RecordingStandDown::default());

        // The owner may cancel between `submit_runnable` returning and the
        // worker starting the fragment. Dropping that cancel would let the
        // task run to completion after being told to stop.
        task.cancel(CancelReason::UpstreamNoLongerNeeded);
        assert!(stop.is_stopped(), "connector I/O must wake before attach");
        assert!(handle.reasons.lock().expect("reasons").is_empty());

        task.attach(Arc::clone(&handle) as Arc<dyn super::FragmentStandDown>);
        assert_eq!(
            *handle.reasons.lock().expect("reasons"),
            vec![CancelReason::UpstreamNoLongerNeeded.as_str().to_owned()]
        );
        assert_eq!(
            task.stand_down(),
            Some(StandDown::Cancel(CancelReason::UpstreamNoLongerNeeded))
        );
    }

    #[test]
    fn the_first_stand_down_wins_and_a_later_one_is_not_replayed() {
        let task = NativeRunnableTask::new(identity(15, 1, 1), UniqueId::new(91, 92));
        let handle = Arc::new(RecordingStandDown::default());

        task.cancel(CancelReason::UpstreamNoLongerNeeded);
        task.abort(AbortCause::QueryFailed);
        task.attach(Arc::clone(&handle) as Arc<dyn super::FragmentStandDown>);

        // A second reason must not rewrite the first: the terminal status is
        // composed from this latch, so a rewrite would report the wrong cause.
        assert_eq!(
            *handle.reasons.lock().expect("reasons"),
            vec![CancelReason::UpstreamNoLongerNeeded.as_str().to_owned()]
        );
        assert_eq!(
            task.stand_down(),
            Some(StandDown::Cancel(CancelReason::UpstreamNoLongerNeeded))
        );
    }

    #[test]
    fn a_stand_down_after_the_fragment_started_reaches_it_directly() {
        let stop = ConnectorStopOwner::new();
        let task = NativeRunnableTask::new_without_receiver_with_stop(
            identity(16, 1, 1),
            UniqueId::new(101, 102),
            stop.clone(),
        );
        let handle = Arc::new(RecordingStandDown::default());
        task.attach(Arc::clone(&handle) as Arc<dyn super::FragmentStandDown>);
        assert!(handle.reasons.lock().expect("reasons").is_empty());
        assert!(!stop.is_stopped());

        task.abort(AbortCause::LeaseExpired);
        assert!(stop.is_stopped(), "connector I/O must wake after attach");
        assert_eq!(
            *handle.reasons.lock().expect("reasons"),
            vec![AbortCause::LeaseExpired.as_str().to_owned()]
        );

        // Once the worker has its terminal fact a stand-down has nothing left
        // to reach, and must not resurrect a reference to the kernel handle.
        task.finish(None);
        task.abort(AbortCause::PeerTaskFailed);
        assert_eq!(handle.reasons.lock().expect("reasons").len(), 1);
    }

    #[test]
    fn failed_preparation_stops_issued_connector_views() {
        let stop = ConnectorStopOwner::new();
        let guard = PreparationStopGuard::new(stop.clone());
        let view = stop.view();
        assert!(!view.is_stopped());
        drop(guard);
        assert!(view.is_stopped());

        let committed = ConnectorStopOwner::new();
        let mut guard = PreparationStopGuard::new(committed.clone());
        guard.transfer_to_task();
        drop(guard);
        assert!(!committed.is_stopped());
    }

    // ------------------------------------------------------ install refusals

    /// Asserts a refused install left nothing of this host's own behind: no
    /// prepared task, no open split queue, no requested filter session.
    fn assert_nothing_prepared(host: &NativeTaskExecutionHost, facts: &StubContextFacts) {
        assert!(
            host.tasks.lock().expect("host tasks").is_empty(),
            "a refused install parks no dormant fragment"
        );
        assert!(
            host.split_queues.is_empty(),
            "a refused install returns its split queue lease"
        );
        assert_eq!(
            facts.filter_sessions_requested.load(Ordering::SeqCst),
            0,
            "a refusal precedes every downstream fragment construction"
        );
    }

    #[test]
    fn an_assignment_this_codec_did_not_produce_cannot_be_installed() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let descriptor = consistent_descriptor(identity(17, 1, 1), UniqueId::new(111, 112));

        establish_fixture_context(&host, descriptor.identity());

        let rejection = host
            .install_receiver(
                &descriptor,
                TaskCreationInput::new(Body::values(1).frozen_bytes(), Box::new(ForeignAssignment)),
                novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
            )
            .expect_err("a foreign assignment has no decodable representation");
        assert_eq!(rejection.category(), TaskFailureCategory::Internal);
        assert!(
            rejection
                .detail()
                .as_str()
                .contains("not a codec-produced assignment"),
            "{rejection}"
        );
        assert_nothing_prepared(&host, &facts);
    }

    #[test]
    fn an_unreadable_static_fragment_is_refused_before_anything_is_prepared() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let descriptor = consistent_descriptor(identity(18, 1, 1), UniqueId::new(121, 122));
        let assignment = Body::values(1).input(&descriptor).into_parts().1;

        establish_fixture_context(&host, descriptor.identity());

        let rejection = host
            .install_receiver(
                &descriptor,
                TaskCreationInput::new(
                    FrozenBytes::freeze(bytes::Bytes::from_static(&[0x0a, 0x80])),
                    assignment,
                ),
                novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
            )
            .expect_err("a creation winner must interpret its static fragment");
        assert_eq!(rejection.category(), TaskFailureCategory::Protocol);
        assert!(
            rejection
                .detail()
                .as_str()
                .contains("static fragment is not decodable"),
            "{rejection}"
        );
        assert_nothing_prepared(&host, &facts);
    }

    #[test]
    fn a_task_dop_outside_its_fragment_domain_is_refused_before_anything_is_prepared() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        // The descriptor is the only authority over the task's parallelism,
        // and the static fragment froze the domain it must lie in. Preparing
        // at four drivers a fragment frozen for one would run the task at a
        // parallelism its plan was never compiled for.
        let descriptor = descriptor_with(
            identity(19, 1, 1),
            UniqueId::new(141, 142),
            4,
            ExchangeTopology::default(),
        );

        establish_fixture_context(&host, descriptor.identity());

        let rejection = host
            .install_receiver(
                &descriptor,
                Body::values(1).input(&descriptor),
                novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
            )
            .expect_err("a parallelism outside the frozen domain is not this fragment");
        assert_eq!(rejection.category(), TaskFailureCategory::Protocol);
        assert!(
            rejection.detail().as_str().contains("DOP domain"),
            "{rejection}"
        );
        assert_nothing_prepared(&host, &facts);
    }

    #[test]
    fn a_scan_range_for_a_node_the_static_plan_lacks_is_refused_before_receivers_register() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let descriptor = consistent_descriptor(identity(47, 1, 1), UniqueId::new(149, 150));
        let mut body = Body::values(1);
        body.assignment.initial_scan_ranges = vec![proto::TaskScanRanges {
            plan_node_id: 99,
            ranges: Vec::new(),
        }];

        establish_fixture_context(&host, descriptor.identity());

        let rejection = host
            .install_receiver(
                &descriptor,
                body.input(&descriptor),
                novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
            )
            .expect_err("a scan assignment must name a scan node of the static plan");
        assert_eq!(rejection.category(), TaskFailureCategory::Protocol);
        assert!(
            rejection
                .detail()
                .as_str()
                .contains("scan ranges assigned to unknown scan node 99"),
            "{rejection}"
        );
        assert!(
            rejection
                .detail()
                .as_str()
                .contains("creation_metadata.assignment.initial_scan_ranges"),
            "the refusal names the field the frontend actually sent: {rejection}"
        );
        assert_nothing_prepared(&host, &facts);
    }

    #[test]
    fn a_static_sink_that_disagrees_with_its_assigned_edge_is_refused_before_receivers_register() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let edge = ExchangeEdgeId::new(1).expect("nonzero edge");
        let descriptor = descriptor_with(
            identity(48, 1, 1),
            UniqueId::new(151, 152),
            1,
            outbound_topology(edge, identity(48, 2, 1)),
        );
        // The frozen edge is unpartitioned; the static sink hashes. Binding
        // the one to the other would send rows to destinations the frontend
        // never planned for that partitioning.
        let body = Body::with_sink(1, stream_sink(plan::PartitionKind::Hash), vec![edge.get()]);

        establish_fixture_context(&host, descriptor.identity());

        let rejection = host
            .install_receiver(
                &descriptor,
                body.input(&descriptor),
                novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
            )
            .expect_err("a static sink branch must agree with its assigned edge");
        assert_eq!(rejection.category(), TaskFailureCategory::Protocol);
        assert!(
            rejection
                .detail()
                .as_str()
                .contains("partitioning disagrees with its assigned edge"),
            "{rejection}"
        );
        assert_nothing_prepared(&host, &facts);
    }

    #[test]
    fn a_fragment_may_use_a_lower_dop_than_its_query_context() {
        let facts = Arc::new(StubContextFacts::default());
        facts.set_context_dop(8);
        let host = host(Arc::clone(&facts));
        let task = identity(46, 1, 1);
        let kernel_key = UniqueId::new(147, 148);
        let descriptor = consistent_descriptor(task, kernel_key);

        let prepared =
            install(&host, &descriptor).expect("task-local DOP 1 is legal under context DOP 8");
        assert_eq!(prepared.sink_kind(), FragmentSinkKind::Noop);
        assert_eq!(facts.filter_sessions_requested.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_domain_update_for_a_task_that_was_never_installed_is_refused() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let descriptor = consistent_descriptor(identity(20, 1, 1), UniqueId::new(151, 152));

        // Applying a domain to an absent task would advance a token the owner
        // then believes the execution side honoured.
        let rejection = host
            .apply_task_domain(
                &descriptor,
                &TaskDomainUpdate::OpenExchangeEdges {
                    version: EdgeOpenVersion::FIRST,
                    edges: vec![ExchangeEdgeId::new(1).expect("nonzero edge")],
                },
            )
            .expect_err("no prepared fragment exists");
        assert!(
            rejection
                .detail()
                .as_str()
                .contains("has no prepared fragment"),
            "{rejection}"
        );
        assert_eq!(facts.dynamic_filters_delivered.load(Ordering::SeqCst), 0);
    }

    // ----------------------------------------------------- prepared lifecycle

    fn isolated_resource_host() -> (
        NativeTaskExecutionHost,
        Arc<novarocks_worker::query_context::QueryContextManager>,
    ) {
        let facts = Arc::new(StubContextFacts::default());
        let manager = Arc::clone(&facts.query_manager);
        (host(facts), manager)
    }

    #[test]
    fn typed_failure_native_preparation_opaque_result_refusal_keeps_category_and_rolls_back() {
        use novarocks_execution::runtime::fragment::io::{
            FragmentIoError, FragmentIoErrorKind, FragmentIoOperation, FragmentResultSession,
            FragmentResultWriter, ResultWriteSpec,
        };
        struct RefusingWriter {
            opens: AtomicUsize,
            message: &'static str,
        }
        impl FragmentResultWriter for RefusingWriter {
            fn open(
                &self,
                _spec: ResultWriteSpec,
            ) -> Result<Arc<dyn FragmentResultSession>, FragmentIoError> {
                self.opens.fetch_add(1, Ordering::SeqCst);
                Err(FragmentIoError::new(
                    FragmentIoOperation::ResultOpen,
                    FragmentIoErrorKind::Internal,
                    self.message,
                ))
            }
        }
        for (index, message) in [
            "Internal: same launch text",
            "Cancelled",
            "ResourceExhausted",
        ]
        .into_iter()
        .enumerate()
        {
            let (mut host, manager) = isolated_resource_host();
            let writer = Arc::new(RefusingWriter {
                opens: AtomicUsize::new(0),
                message,
            });
            host.result_writer = writer.clone();
            let task = identity(80_030 + index as i64, 1, 1);
            let descriptor = consistent_descriptor(task, UniqueId::new(80_030 + index as i64, 1));
            let body = Body::with_sink(
                1,
                plan::DataSink {
                    kind: Some(plan::data_sink::Kind::Result(true)),
                },
                Vec::new(),
            );
            establish_fixture_context(&host, descriptor.identity());
            let refusal = host
                .install_receiver(
                    &descriptor,
                    body.input(&descriptor),
                    novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
                )
                .unwrap_err();
            assert_eq!(writer.opens.load(Ordering::SeqCst), 1);
            assert_eq!(refusal.category(), TaskFailureCategory::ResourceExhausted);
            assert!(refusal.detail().as_str().contains(message));
            assert!(host.task_runtime(task).is_none());
            assert!(host.split_queues.is_empty());
            assert!(host.capabilities.is_empty());
            let released = manager.native_execution_resource_snapshot();
            assert_eq!(released.active_contexts, 0);
            assert_eq!(released.active_fragments, 0);
            // The actual refusal releases the registration and split attempt,
            // so a later legal body can prepare under the same exact identity.
            install(&host, &descriptor).unwrap();
            assert!(host.task_runtime(task).is_some());
            host.remove_receiver(&descriptor);
            assert!(host.split_queues.is_empty());
            let released = manager.native_execution_resource_snapshot();
            assert_eq!(released.active_contexts, 0);
            assert_eq!(released.active_fragments, 0);
        }
    }

    #[test]
    fn preparation_registration_rolls_back_when_a_prepared_receiver_is_removed() {
        let (host, manager) = isolated_resource_host();
        let descriptor = consistent_descriptor(identity(521, 1, 1), UniqueId::new(521, 522));
        install(&host, &descriptor).expect("prepares without submitting drivers");
        let prepared = manager.native_execution_resource_snapshot();
        assert_eq!(prepared.active_contexts, 1);
        assert_eq!(prepared.active_fragments, 1);
        host.remove_receiver(&descriptor);
        host.remove_receiver(&descriptor);
        let removed = manager.native_execution_resource_snapshot();
        assert_eq!(removed.active_contexts, 0);
        assert_eq!(removed.active_fragments, 0);
    }

    #[test]
    fn preparation_registration_rollback_preserves_a_submitted_sibling() {
        let (host, manager) = isolated_resource_host();
        let first = identity(523, 1, 1);
        let sibling = TaskIdentity::new(
            first.query_execution_id(),
            first.stage_id(),
            TaskId::new(2).expect("task"),
            first.backend_process_id(),
        );
        let first_descriptor = consistent_descriptor(first, UniqueId::new(523, 524));
        let sibling_descriptor = consistent_descriptor(sibling, UniqueId::new(525, 526));
        install(&host, &first_descriptor).expect("first prepares");
        let (owner, reporter) = reporter_for(first);
        // Submission retains the registration in PreparedNativeStart. No
        // commit has occurred, so this test cannot accidentally run drivers.
        let runnable = host
            .submit_runnable(&first_descriptor, reporter)
            .expect("submits");
        install(&host, &sibling_descriptor).expect("sibling prepares");
        assert_eq!(
            manager
                .native_execution_resource_snapshot()
                .active_fragments,
            2
        );
        host.remove_receiver(&sibling_descriptor);
        let retained = manager.native_execution_resource_snapshot();
        assert_eq!(retained.active_contexts, 1);
        assert_eq!(retained.active_fragments, 1);
        host.remove_receiver(&first_descriptor);
        assert_eq!(
            manager
                .native_execution_resource_snapshot()
                .active_fragments,
            1,
            "the submitted start owns its registration independently of receiver removal"
        );
        runnable.commit_creation();
        assert_eq!(await_terminal(&owner), TaskState::Finished);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while manager.native_execution_resource_snapshot().active_contexts != 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "completion must release runtime accounting"
            );
            std::thread::yield_now();
        }
        let removed = manager.native_execution_resource_snapshot();
        assert_eq!(removed.active_contexts, 0);
        assert_eq!(removed.active_fragments, 0);
    }

    #[test]
    fn a_prepared_task_opens_only_the_edges_its_descriptor_froze() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let producer = identity(21, 1, 1);
        let consumer = identity(21, 2, 1);
        let kernel_key = UniqueId::new(161, 162);
        let edge = ExchangeEdgeId::new(1).expect("nonzero edge");
        let descriptor =
            descriptor_with(producer, kernel_key, 1, outbound_topology(edge, consumer));
        let body = Body::with_sink(
            1,
            stream_sink(plan::PartitionKind::Unpartitioned),
            vec![edge.get()],
        );

        establish_fixture_context(&host, descriptor.identity());

        let prepared = host
            .install_receiver(
                &descriptor,
                body.input(&descriptor),
                novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
            )
            .expect("a consistent descriptor prepares");
        assert_eq!(
            prepared.facts().sink_kind(),
            FragmentSinkKind::DataStream,
            "the prepared facts report the validated static sink"
        );
        assert_eq!(facts.filter_sessions_requested.load(Ordering::SeqCst), 1);

        // An edge the descriptor never froze has no gate, so granting it would
        // be inventing a destination.
        let unknown = host
            .apply_task_domain(
                &descriptor,
                &TaskDomainUpdate::OpenExchangeEdges {
                    version: EdgeOpenVersion::FIRST,
                    edges: vec![ExchangeEdgeId::new(9).expect("nonzero edge")],
                },
            )
            .expect_err("an unfrozen edge cannot be opened");
        assert!(unknown.detail().as_str().contains("edge open is illegal"));

        host.apply_task_domain(
            &descriptor,
            &TaskDomainUpdate::OpenExchangeEdges {
                version: EdgeOpenVersion::FIRST,
                edges: vec![edge],
            },
        )
        .expect("a frozen edge opens at version one");

        host.remove_receiver(&descriptor);
        // Removal must be idempotent: the owner calls it for a rollback and
        // again when the task retires.
        host.remove_receiver(&descriptor);
    }

    #[test]
    fn a_prepared_task_closes_only_its_exact_frozen_destination() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let producer = identity(121, 1, 1);
        let consumer = identity(121, 2, 1);
        let edge = ExchangeEdgeId::new(1).expect("edge");
        let descriptor = descriptor_with(
            producer,
            UniqueId::new(261, 262),
            1,
            outbound_topology(edge, consumer),
        );
        let body = Body::with_sink(
            1,
            stream_sink(plan::PartitionKind::Unpartitioned),
            vec![edge.get()],
        );
        establish_fixture_context(&host, descriptor.identity());
        host.install_receiver(
            &descriptor,
            body.input(&descriptor),
            novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
        )
        .expect("prepares");

        let replacement_process = TaskIdentity::new(
            consumer.query_execution_id(),
            consumer.stage_id(),
            consumer.task_id(),
            BackendProcessId::new_v7(),
        );
        let close = |destination| TaskDomainUpdate::CloseExchangeDestination {
            version: DomainVersion::FIRST,
            edge,
            destination,
        };
        assert!(
            host.apply_task_domain(&descriptor, &close(replacement_process))
                .is_err()
        );
        let gate = host
            .task_runtime(producer)
            .expect("runtime")
            .edges
            .gate_for_destination(ExchangeDestinationKey::new(UniqueId::new(900, 901), 11))
            .expect("gate")
            .clone();
        assert!(!gate.is_normally_canceled());

        host.apply_task_domain(&descriptor, &close(consumer))
            .expect("exact destination closes");
        assert!(gate.is_normally_canceled());
        host.apply_task_domain(&descriptor, &close(consumer))
            .expect("repeat close is harmless");
        host.apply_task_domain(
            &descriptor,
            &TaskDomainUpdate::OpenExchangeEdges {
                version: EdgeOpenVersion::FIRST,
                edges: vec![edge],
            },
        )
        .expect("edge grant after close is recorded");
        assert!(
            gate.is_normally_canceled(),
            "opening the edge cannot revive a closed destination"
        );
        host.remove_receiver(&descriptor);
    }

    #[test]
    fn a_task_dynamic_filter_is_handed_to_the_query_context_participant() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(22, 1, 1);
        let kernel_key = UniqueId::new(171, 172);
        let descriptor = consistent_descriptor(task, kernel_key);
        install(&host, &descriptor).expect("prepares");

        // The filter participant belongs to the query context, not to this
        // task: delivering it here would create a second owner of the same
        // installed contract.
        host.apply_task_domain(
            &descriptor,
            &TaskDomainUpdate::TaskDynamicFilter {
                version: DomainVersion::new(1).expect("nonzero"),
                payload: Arc::new(novarocks_task_codec::domain::WireContent::new(
                    b"filter",
                    novarocks_proto_models::filter::RuntimeFilterEnvelope::default(),
                )),
            },
        )
        .expect("the context host accepts it");
        assert_eq!(facts.dynamic_filters_delivered.load(Ordering::SeqCst), 1);

        host.remove_receiver(&descriptor);
    }

    #[test]
    fn a_task_cannot_be_prepared_twice_under_one_identity() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(23, 1, 1);
        let descriptor = consistent_descriptor(task, UniqueId::new(181, 182));
        install(&host, &descriptor).expect("prepares");

        // A second preparation would leave the first one's receivers and
        // pipeline with nothing that can ever roll them back.
        let rejection = install(&host, &descriptor).expect_err("one identity, one fragment");
        assert!(
            rejection
                .detail()
                .as_str()
                .contains("already has a prepared fragment"),
            "{rejection}"
        );

        host.remove_receiver(&descriptor);
    }

    // ------------------------------------------------------- split delivery

    fn split_intent(
        node: PlanNodeId,
        payload_node: i32,
        splits: Vec<connector_dto::ScheduledSplit>,
        offer: SplitOffer,
    ) -> TaskDomainUpdate {
        let no_more = offer.no_more_splits();
        let assignment = connector_dto::SplitAssignment {
            plan_node_id: payload_node,
            splits,
            no_more_splits: no_more,
        };
        let payload: Arc<dyn CodecOwnedContent> = Arc::new(
            novarocks_task_codec::domain::WireContent::new(b"split", assignment),
        );
        TaskDomainUpdate::SplitAssignment(
            novarocks_execution_contract::task_execution::operation::SplitAssignmentIntent::new(
                node, offer, payload,
            ),
        )
    }

    #[test]
    fn a_split_payload_of_another_domain_is_refused_rather_than_reinterpreted() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(27, 1, 1);
        let descriptor = consistent_descriptor(task, UniqueId::new(221, 222));
        install(&host, &descriptor).expect("prepares");

        // A well-formed handle of a *different* domain must not be read as a
        // split batch: the queue would then accept sequence numbers that no
        // scheduler ever issued.
        let update = TaskDomainUpdate::SplitAssignment(
            novarocks_execution_contract::task_execution::operation::SplitAssignmentIntent::new(
                PlanNodeId::new(10).expect("nonnegative node"),
                SplitOffer::Seal,
                Arc::new(novarocks_task_codec::domain::WireContent::new(
                    b"filter",
                    novarocks_proto_models::filter::RuntimeFilterEnvelope::default(),
                )),
            ),
        );
        let rejection = host
            .apply_task_domain(&descriptor, &update)
            .expect_err("a filter envelope is not a split batch");
        assert!(
            rejection
                .detail()
                .as_str()
                .contains("not a split assignment"),
            "{rejection}"
        );

        host.remove_receiver(&descriptor);
    }

    #[test]
    fn a_split_intent_whose_payload_names_another_plan_node_is_refused() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(28, 1, 1);
        let descriptor = consistent_descriptor(task, UniqueId::new(231, 232));
        install(&host, &descriptor).expect("prepares");

        // The intent's node is what the domain classifier advanced a
        // watermark for. Delivering the payload to the node the payload names
        // instead would move a different node's queue than the one the
        // receipt reports.
        let rejection = host
            .apply_task_domain(
                &descriptor,
                &split_intent(
                    PlanNodeId::new(10).expect("nonnegative node"),
                    11,
                    Vec::new(),
                    SplitOffer::Seal,
                ),
            )
            .expect_err("two plan nodes is not one assignment");
        assert!(
            rejection
                .detail()
                .as_str()
                .contains("but its payload names"),
            "{rejection}"
        );

        host.remove_receiver(&descriptor);
    }

    #[test]
    fn a_standalone_terminal_marker_seals_a_node_that_never_bound_a_reader() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(29, 1, 1);
        let descriptor = consistent_descriptor(task, UniqueId::new(241, 242));
        install(&host, &descriptor).expect("prepares");

        // An assignment with no splits carries only the terminal marker. It
        // must not demand a typed read execution: a node that was pruned to
        // zero splits still has to be sealed, or its scan waits forever.
        host.apply_task_domain(
            &descriptor,
            &split_intent(
                PlanNodeId::new(10).expect("nonnegative node"),
                10,
                Vec::new(),
                SplitOffer::Seal,
            ),
        )
        .expect("a terminal marker needs no provider payload");

        host.remove_receiver(&descriptor);
    }

    #[test]
    fn splits_for_a_plan_node_with_no_typed_read_execution_are_refused() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(30, 1, 1);
        let descriptor = consistent_descriptor(task, UniqueId::new(251, 252));
        install(&host, &descriptor).expect("prepares");

        // This fragment's only node is a VALUES node, so nothing registered a
        // connector read. Enqueuing the split anyway would fill a queue that
        // no scan operator will ever drain.
        let rejection = host
            .apply_task_domain(
                &descriptor,
                &split_intent(
                    PlanNodeId::new(10).expect("nonnegative node"),
                    10,
                    vec![novarocks_native_adapter::typed_connector_test_support::test_support::split_proto(
                        10, 1,
                    )],
                    SplitOffer::batch(SplitSequence::FIRST, SplitSequence::FIRST, false)
                        .expect("one split"),
                ),
            )
            .expect_err("no typed connector read execution exists");
        assert!(
            rejection
                .detail()
                .as_str()
                .contains("no typed connector read execution"),
            "{rejection}"
        );

        host.remove_receiver(&descriptor);
    }

    // ------------------------------------------------------ end-to-end submit

    fn planned_reporter_for(identity: TaskIdentity) -> (Arc<TaskStatusOwner>, TaskStatusReporter) {
        let owner = Arc::new(TaskStatusOwner::new(
            identity,
            Arc::new(TaskStatusSource::new()),
            Arc::new(ProcessMonotonicClock::new()),
            METRIC_PUBLISH_MIN_INTERVAL,
        ));
        owner.release_to_observers();
        (Arc::clone(&owner), TaskStatusReporter::new(owner))
    }

    fn reporter_for(identity: TaskIdentity) -> (Arc<TaskStatusOwner>, TaskStatusReporter) {
        let (owner, reporter) = planned_reporter_for(identity);
        // Direct Host tests bypass the registry's creation commit. Reproduce
        // its irreversible Installed fact before the runnable may start.
        assert!(matches!(
            owner.note_installed(),
            StatusAdvance::Published(_)
        ));
        assert!(owner.current().installed());
        (owner, reporter)
    }

    /// Waits for a task to reach a terminal status, or gives up.
    ///
    /// Completion is published by the process owner asynchronously. The bound
    /// only exists so a hang fails the test instead of blocking the suite.
    fn await_terminal(owner: &TaskStatusOwner) -> TaskState {
        for _ in 0..600 {
            if owner.is_terminal() {
                return owner.state();
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("task did not reach a terminal state: {:?}", owner.state());
    }

    fn submit_committed(
        host: &NativeTaskExecutionHost,
        descriptor: &TaskDescriptor,
        reporter: TaskStatusReporter,
    ) -> Arc<dyn RunnableTask> {
        let runnable = host
            .submit_runnable(descriptor, reporter)
            .expect("a prepared fragment starts");
        runnable.commit_creation();
        runnable
    }

    #[test]
    fn submitted_runnable_stays_dormant_until_creation_commit() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(23, 1, 1);
        let descriptor = consistent_descriptor(task, UniqueId::new(189, 190));
        let (owner, reporter) = planned_reporter_for(task);

        install(&host, &descriptor).expect("prepares");
        host.install_inbound_capability(&descriptor)
            .expect("installs");
        let runnable = host
            .submit_runnable(&descriptor, reporter)
            .expect("reserves completion without starting drivers");
        assert_eq!(owner.state(), TaskState::Planned);
        assert!(!owner.current().installed());
        assert!(!owner.convergence().actual_stopped());

        // Worker publishes Installed while the kernel remains dormant.
        assert!(matches!(
            owner.note_installed(),
            StatusAdvance::Published(_)
        ));
        assert_eq!(owner.state(), TaskState::Planned);
        assert!(!owner.convergence().actual_stopped());
        runnable.commit_creation();
        assert_eq!(await_terminal(&owner), TaskState::Finished);
        await_runtime_convergence(&owner);
        host.remove_inbound_capability(&descriptor);
        host.remove_receiver(&descriptor);
    }

    #[test]
    fn stand_down_before_creation_commit_stops_without_starting_drivers() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(231, 1, 1);
        let descriptor = consistent_descriptor(task, UniqueId::new(1891, 1901));
        let (owner, reporter) = planned_reporter_for(task);

        install(&host, &descriptor).expect("prepares");
        host.install_inbound_capability(&descriptor)
            .expect("installs");
        let runnable = host
            .submit_runnable(&descriptor, reporter)
            .expect("reserves completion without starting drivers");
        runnable.cancel(CancelReason::UpstreamNoLongerNeeded);
        runnable.commit_creation();

        assert_eq!(await_terminal(&owner), TaskState::Canceled);
        await_runtime_convergence(&owner);
        host.remove_inbound_capability(&descriptor);
        host.remove_receiver(&descriptor);
    }

    fn await_runtime_convergence(owner: &TaskStatusOwner) {
        for _ in 0..600 {
            let convergence = owner.convergence();
            if convergence.actual_stopped() && convergence.resources_converged() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!(
            "task runtime did not converge after its terminal: {:?}",
            owner.convergence()
        );
    }

    #[test]
    fn a_submitted_task_runs_and_publishes_its_own_terminal_status() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(24, 1, 1);
        let kernel_key = UniqueId::new(191, 192);
        let descriptor = consistent_descriptor(task, kernel_key);
        let (owner, reporter) = reporter_for(task);

        install(&host, &descriptor).expect("prepares");
        host.install_inbound_capability(&descriptor)
            .expect("installs");
        let runnable = submit_committed(&host, &descriptor, reporter);

        // A NOOP sink owes nothing further, so the completion owner is the
        // only thing that may publish FINISHED. If it never reported, this
        // would hang at PLANNED and the poll would fail.
        assert_eq!(await_terminal(&owner), TaskState::Finished);
        assert!(
            owner.output_released(),
            "a finished non-root task has released its output responsibility"
        );
        await_runtime_convergence(&owner);
        let convergence = owner.convergence();
        assert!(convergence.conclusion_stable());
        assert!(convergence.retirement_ready());
        // Standing a finished task down must be inert rather than a panic on
        // an already-consumed handle.
        runnable.abort(AbortCause::QueryFailed);

        host.remove_inbound_capability(&descriptor);
        host.remove_receiver(&descriptor);
    }

    #[test]
    fn submitting_a_task_offers_it_as_the_context_dynamic_filter_carrier() {
        // The defect this catches: the dynamic filter feedback sink was fully
        // built and fully unit-tested, and nothing in the submission path ever
        // installed it. A reduced filter domain then reached no frontend, every
        // channel stayed pending, and each connector split source waited out
        // its own cap and enumerated unpruned -- with identical rows, so no
        // result assertion anywhere could see it.
        //
        // The offer is what this asserts, because the offer is the supply.
        // Whether a query has a participant to accept it is the context host's
        // question, and that is asserted where the context host lives.
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(26, 1, 1);
        let descriptor = consistent_descriptor(task, UniqueId::new(213, 214));
        let (owner, reporter) = reporter_for(task);

        install(&host, &descriptor).expect("prepares");
        host.install_inbound_capability(&descriptor)
            .expect("installs");
        submit_committed(&host, &descriptor, reporter);
        assert_eq!(await_terminal(&owner), TaskState::Finished);

        assert_eq!(
            *facts
                .feedback_carriers
                .lock()
                .expect("stub feedback carriers"),
            vec![task],
            "submitting a task must offer it as its query context's feedback carrier"
        );

        host.remove_inbound_capability(&descriptor);
        host.remove_receiver(&descriptor);
    }

    #[test]
    fn submitting_a_task_that_was_never_prepared_is_refused_before_execution_starts() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(25, 1, 1);
        let descriptor = consistent_descriptor(task, UniqueId::new(201, 202));
        let (_owner, reporter) = reporter_for(task);

        // `submit_runnable` is the only step that can start execution, so it
        // must refuse an unprepared task rather than run nothing.
        let rejection = host
            .submit_runnable(&descriptor, reporter)
            .expect_err("no prepared fragment exists");
        assert!(
            rejection
                .detail()
                .as_str()
                .contains("without a prepared fragment"),
            "{rejection}"
        );
    }

    fn terminal_fact(outcome: FragmentOutcome) -> FragmentTerminalFact {
        FragmentTerminalFact::new(
            novarocks_types::QueryId::new(1, 2),
            UniqueId::new(3, 4),
            outcome,
            None,
        )
    }

    #[test]
    fn typed_input_construction_failures_preserve_their_native_category() {
        use novarocks_execution::exec::pipeline::operator::input_construction::ConstructionFailure;
        use novarocks_execution::runtime::fragment::ExecutionFailure;
        use novarocks_type_contract::CompileControlError;
        for (failure, expected) in [
            (
                ConstructionFailure::MissingOwner,
                TaskFailureCategory::Internal,
            ),
            (
                ConstructionFailure::InvalidSource("ResourceExhausted".into()),
                TaskFailureCategory::Execution,
            ),
            (
                ConstructionFailure::Control(CompileControlError::ResourceExhausted),
                TaskFailureCategory::ResourceExhausted,
            ),
            (
                ConstructionFailure::Control(CompileControlError::Cancelled),
                TaskFailureCategory::Execution,
            ),
            (
                ConstructionFailure::Control(CompileControlError::DeadlineExceeded),
                TaskFailureCategory::Execution,
            ),
        ] {
            let original = ExecutionFailure::from(failure);
            assert_eq!(
                super::failure_cause_category(original.cause(), TaskFailureCategory::Exchange),
                expected,
            );
        }
        let original = ExecutionFailure::from(novarocks_functions::KernelFailure::Cancelled);
        let forwarded =
            ExecutionFailure::from(ConstructionFailure::Caller(Box::new(original.clone())));
        assert_eq!(forwarded, original);
        assert_eq!(
            super::failure_cause_category(forwarded.cause(), TaskFailureCategory::Exchange),
            TaskFailureCategory::Execution,
        );
    }

    #[test]
    fn typed_failure_native_preparation_preserves_kernel_category_without_stage_or_text_guessing() {
        use novarocks_execution::runtime::fragment::{
            FragmentLaunchError, FragmentLaunchErrorKind, FragmentLaunchStage,
        };
        use novarocks_functions::{KernelDiagnostic, KernelFailure};
        let diagnostic = || KernelDiagnostic::new("ResourceExhausted: identical diagnostic text");
        for (cause, expected) in [
            (
                KernelFailure::ResourceExhausted,
                TaskFailureCategory::ResourceExhausted,
            ),
            (
                KernelFailure::InvalidProgram(diagnostic()),
                TaskFailureCategory::Internal,
            ),
            (
                KernelFailure::Internal(diagnostic()),
                TaskFailureCategory::Internal,
            ),
            (KernelFailure::InstanceFailed, TaskFailureCategory::Internal),
            (KernelFailure::Cancelled, TaskFailureCategory::Execution),
            (
                KernelFailure::DeadlineExceeded,
                TaskFailureCategory::Execution,
            ),
            (
                KernelFailure::Operational(diagnostic()),
                TaskFailureCategory::Execution,
            ),
        ] {
            for (stage, kind) in [
                (
                    FragmentLaunchStage::BuildPipelines,
                    FragmentLaunchErrorKind::PipelineBuild,
                ),
                (
                    FragmentLaunchStage::BuildRuntimeState,
                    FragmentLaunchErrorKind::ResourceUnavailable,
                ),
            ] {
                let error = FragmentLaunchError::from_failure(stage, kind, cause.clone().into())
                    .with_cleanup_diagnostics(vec!["ResourceExhausted: cleanup failed".to_owned()]);
                let task = identity(80_024, 1, 1);
                let refusal = super::preparation_failure_to_host(task, &error);
                assert_eq!(refusal.category(), expected);
                assert_eq!(
                    refusal.detail().as_str(),
                    format!("task {task} could not be prepared: {error}")
                );
                assert_eq!(
                    error.cause().cause(),
                    &super::ExecutionFailureCause::Kernel(cause.clone())
                );
            }
        }
        // Legacy launch authors have no typed cause yet. Diagnostic text,
        // launch stage and cleanup cannot reclassify their original refusal.
        for text in [
            "Internal: identical diagnostic text",
            "Cancelled",
            "required expression row failed",
        ] {
            let error = FragmentLaunchError::new(
                FragmentLaunchStage::BuildPipelines,
                FragmentLaunchErrorKind::PipelineBuild,
                text,
            );
            assert_eq!(
                super::preparation_failure_to_host(identity(80_025, 1, 1), &error).category(),
                TaskFailureCategory::ResourceExhausted,
            );
        }
    }

    #[test]
    fn typed_failure_native_preparation_preserves_required_row_address_in_final_detail() {
        use novarocks_execution::runtime::fragment::{
            FragmentLaunchError, FragmentLaunchErrorKind, FragmentLaunchStage,
            RequiredExpressionRowError,
        };
        use novarocks_functions::{RowDataError, Selection};
        use novarocks_local_program::{
            ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
        };
        let required = RequiredExpressionRowError::try_new(
            ProgramExpressionRootSite::Node {
                node: ProgramNodeId::new(41),
                role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
            },
            Selection::try_sparse(101, &[0, 50, 100]).unwrap(),
            RowDataError::new(1, "required root cannot publish a placeholder NULL"),
        )
        .unwrap();
        let error = FragmentLaunchError::from_failure(
            FragmentLaunchStage::BuildPipelines,
            FragmentLaunchErrorKind::PipelineBuild,
            required.clone().into(),
        );
        let refusal = super::preparation_failure_to_host(identity(80_026, 1, 1), &error);
        assert_eq!(refusal.category(), TaskFailureCategory::Execution);
        assert_eq!(
            error.cause().cause(),
            &super::ExecutionFailureCause::RequiredRow(required)
        );
        assert!(refusal.detail().as_str().contains("selected ordinal 1"));
        assert!(refusal.detail().as_str().contains("batch row 50"));
    }

    #[test]
    fn typed_failure_native_reporting_uses_original_kernel_cause_including_normal_quiesce() {
        use novarocks_execution_contract::task_execution::status::{
            TaskFailurePhase, TerminationDetail,
        };
        use novarocks_functions::{KernelDiagnostic, KernelFailure};
        let diagnostic = || KernelDiagnostic::new("ResourceExhausted: identical diagnostic text");
        for (index, (cause, expected)) in [
            (
                KernelFailure::ResourceExhausted,
                TaskFailureCategory::ResourceExhausted,
            ),
            (
                KernelFailure::InvalidProgram(diagnostic()),
                TaskFailureCategory::Internal,
            ),
            (
                KernelFailure::Internal(diagnostic()),
                TaskFailureCategory::Internal,
            ),
            (KernelFailure::InstanceFailed, TaskFailureCategory::Internal),
            (KernelFailure::Cancelled, TaskFailureCategory::Execution),
            (
                KernelFailure::DeadlineExceeded,
                TaskFailureCategory::Execution,
            ),
            (
                KernelFailure::Operational(diagnostic()),
                TaskFailureCategory::Execution,
            ),
        ]
        .into_iter()
        .enumerate()
        {
            for quiesce in [false, true] {
                let (owner, reporter) = reporter_for(identity(
                    80_000 + index as i64 * 2 + i64::from(quiesce),
                    1,
                    1,
                ));
                reporter.running();
                if quiesce {
                    reporter.canceling(CancelReason::UpstreamNoLongerNeeded);
                }
                let error = FragmentExecutionError::from_failure(
                    FragmentExecutionErrorKind::Pipeline,
                    cause.clone().into(),
                );
                assert_eq!(
                    error.cause().cause(),
                    &novarocks_execution::runtime::fragment::ExecutionFailureCause::Kernel(
                        cause.clone()
                    )
                );
                report_terminal(
                    &reporter,
                    FragmentSinkKind::Noop,
                    &terminal_fact(FragmentOutcome::Failed(error)),
                    quiesce.then_some(StandDown::Quiesce),
                );
                assert_eq!(owner.state(), TaskState::Failed);
                assert!(owner.output_released());
                let status = owner.current();
                let Some(TerminationDetail::Failed(failure)) = status.termination() else {
                    panic!("original failure must be terminal");
                };
                assert_eq!(failure.category(), expected);
                assert_eq!(failure.phase(), TaskFailurePhase::Execution);
            }
        }
    }

    /// Component-only nominal RF classification through the original reporter.
    /// Misleading message words must not become a Kernel resource category.
    #[test]
    fn typed_failure_native_runtime_filter_reports_the_original_nominal_payload() {
        use novarocks_execution::runtime::fragment::{ExecutionFailure, ExecutionFailureCause};
        use novarocks_execution::runtime_filter::contribution::{
            ContributionCodecError, OrderedTupleError,
        };
        use novarocks_execution::runtime_filter::{
            AggregateTopNBoundaryError, RuntimeFilterContractViolation,
            RuntimeFilterContractViolationKind, RuntimeFilterFailure,
        };
        use novarocks_execution_contract::task_execution::status::{
            TaskFailurePhase, TerminationDetail,
        };
        let cases = [
            RuntimeFilterFailure::Contract(RuntimeFilterContractViolation::new(
                RuntimeFilterContractViolationKind::ContractMismatch,
                "ResourceExhausted: kernel evaluation was cancelled",
            )),
            RuntimeFilterFailure::Boundary(AggregateTopNBoundaryError::BoundLoosened),
            RuntimeFilterFailure::Tuple(OrderedTupleError::TypeMismatch),
            RuntimeFilterFailure::Codec(ContributionCodecError::ResourceLimit),
        ];
        for (index, original) in cases.into_iter().enumerate() {
            let failure: ExecutionFailure = original.clone().into();
            assert_eq!(
                failure.cause(),
                &ExecutionFailureCause::RuntimeFilter(original.clone())
            );
            assert_eq!(
                super::failure_cause_category(
                    failure.cause(),
                    TaskFailureCategory::ResourceExhausted
                ),
                TaskFailureCategory::Execution
            );
            let expected_detail = format!("fragment execution error (pipeline): {failure}");
            let (owner, reporter) = reporter_for(identity(80_060 + index as i64, 1, 1));
            reporter.running();
            report_terminal(
                &reporter,
                FragmentSinkKind::Noop,
                &terminal_fact(FragmentOutcome::Failed(
                    FragmentExecutionError::from_failure(
                        FragmentExecutionErrorKind::Pipeline,
                        failure,
                    ),
                )),
                None,
            );
            assert_eq!(owner.state(), TaskState::Failed);
            assert!(owner.output_released());
            let status = owner.current();
            let Some(TerminationDetail::Failed(failure)) = status.termination() else {
                panic!("original nominal failure must be terminal")
            };
            assert_eq!(failure.category(), TaskFailureCategory::Execution);
            assert_eq!(failure.phase(), TaskFailurePhase::Execution);
            assert_eq!(failure.detail().as_str(), expected_detail);
        }
    }

    #[test]
    fn typed_failure_native_pipeline_text_does_not_claim_kernel_resource_category() {
        use novarocks_execution_contract::task_execution::status::TerminationDetail;
        let (owner, reporter) = reporter_for(identity(80_020, 1, 1));
        reporter.running();
        report_terminal(
            &reporter,
            FragmentSinkKind::Noop,
            &terminal_fact(FragmentOutcome::Failed(FragmentExecutionError::new(
                FragmentExecutionErrorKind::Pipeline,
                "kernel evaluation resources were exhausted: ResourceExhausted",
            ))),
            None,
        );
        let status = owner.current();
        let Some(TerminationDetail::Failed(failure)) = status.termination() else {
            panic!("expected explicit pipeline failure");
        };
        assert_eq!(failure.category(), TaskFailureCategory::Execution);
        assert_eq!(owner.state(), TaskState::Failed);
        assert!(owner.output_released());
    }

    #[test]
    fn typed_failure_native_required_root_reports_original_selected_and_batch_row() {
        use novarocks_execution::runtime::fragment::RequiredExpressionRowError;
        use novarocks_execution_contract::task_execution::status::{
            TaskFailurePhase, TerminationDetail,
        };
        use novarocks_functions::{RowDataError, Selection};
        use novarocks_local_program::{
            ProgramExpressionRootSite, ProgramNodeExpressionRole, ProgramNodeId,
        };
        let selection = Selection::try_sparse(101, &[0, 50, 100]).unwrap();
        let original = RowDataError::new(1, "required root cannot publish a placeholder NULL");
        let required = RequiredExpressionRowError::try_new(
            ProgramExpressionRootSite::Node {
                node: ProgramNodeId::new(41),
                role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
            },
            selection,
            original,
        )
        .unwrap();
        let (owner, reporter) = reporter_for(identity(80_021, 1, 1));
        reporter.running();
        report_terminal(
            &reporter,
            FragmentSinkKind::Noop,
            &terminal_fact(FragmentOutcome::Failed(
                FragmentExecutionError::from_failure(
                    FragmentExecutionErrorKind::Pipeline,
                    required.into(),
                ),
            )),
            None,
        );
        let status = owner.current();
        let Some(TerminationDetail::Failed(failure)) = status.termination() else {
            panic!("required row is an execution failure");
        };
        assert_eq!(failure.category(), TaskFailureCategory::Execution);
        assert_eq!(failure.phase(), TaskFailurePhase::Execution);
        assert!(
            failure
                .detail()
                .as_str()
                .contains("batch row 50 (selected ordinal 1)")
        );
        assert!(
            failure
                .detail()
                .as_str()
                .contains("required root cannot publish a placeholder NULL")
        );
        assert_eq!(owner.state(), TaskState::Failed);
        assert!(owner.output_released());
    }

    #[test]
    fn a_failing_fragment_reaches_a_terminal_status_rather_than_stalling() {
        let task = identity(31, 1, 1);
        let (owner, reporter) = reporter_for(task);
        reporter.running();

        // FAILED is not reachable from RUNNING in one step. Publishing only
        // the terminal would be refused as illegal and leave the owner
        // believing the task is still executing.
        report_terminal(
            &reporter,
            FragmentSinkKind::Noop,
            &terminal_fact(FragmentOutcome::Failed(FragmentExecutionError::new(
                FragmentExecutionErrorKind::Pipeline,
                "driver failed",
            ))),
            None,
        );

        assert_eq!(owner.state(), TaskState::Failed);
        assert!(owner.output_released());
    }

    #[test]
    fn a_kernel_cancellation_nobody_asked_for_is_reported_as_a_failure() {
        let task = identity(32, 1, 1);
        let (owner, reporter) = reporter_for(task);
        reporter.running();

        // A clean CANCELED says the query may still succeed. Nothing asked
        // this task to stand down, so that claim would be unproven.
        report_terminal(
            &reporter,
            FragmentSinkKind::Noop,
            &terminal_fact(FragmentOutcome::Cancelled {
                reason: FragmentCancelReason::new("running fragment handle dropped"),
            }),
            None,
        );

        assert_eq!(owner.state(), TaskState::Failed);
    }

    #[test]
    fn a_stood_down_task_completes_its_stand_down_even_when_its_pipeline_succeeded() {
        let task = identity(33, 1, 1);
        let (owner, reporter) = reporter_for(task);
        reporter.running();

        // This is exactly the LIMIT race: the owner already published
        // CANCELING, from which FINISHED is illegal. Reporting the pipeline's
        // success would leave the task with no terminal at all.
        report_terminal(
            &reporter,
            FragmentSinkKind::Noop,
            &terminal_fact(FragmentOutcome::Succeeded),
            Some(StandDown::Cancel(CancelReason::UpstreamNoLongerNeeded)),
        );

        assert_eq!(owner.state(), TaskState::Canceled);
        assert!(
            owner.current().output().responsibility_complete(),
            "a non-root sink that reached success preserves output completion despite the cancel race"
        );
        assert!(owner.output_released());
    }

    #[test]
    fn a_stood_down_task_that_did_not_succeed_cannot_claim_output_completion() {
        let task = identity(33, 2, 1);
        let (owner, reporter) = reporter_for(task);
        reporter.running();

        report_terminal(
            &reporter,
            FragmentSinkKind::Noop,
            &terminal_fact(FragmentOutcome::Cancelled {
                reason: FragmentCancelReason::new("cancel reached the fragment"),
            }),
            Some(StandDown::Cancel(CancelReason::UpstreamNoLongerNeeded)),
        );

        assert_eq!(owner.state(), TaskState::Canceled);
        assert!(!owner.current().output().responsibility_complete());
        assert!(owner.output_released());
    }

    /// The runnable's latch keeps the first stand-down it was asked for, while
    /// the Worker may still escalate the task's published cancellation to an
    /// abort. The terminal report completes the abort instead of proposing a
    /// cancellation the state machine no longer admits.
    #[test]
    fn a_latched_stand_down_completes_the_abort_that_superseded_it() {
        let outcomes = || {
            [
                FragmentOutcome::Succeeded,
                FragmentOutcome::Cancelled {
                    reason: FragmentCancelReason::new("cancel reached the fragment"),
                },
                FragmentOutcome::Failed(FragmentExecutionError::new(
                    FragmentExecutionErrorKind::Pipeline,
                    "driver failed",
                )),
            ]
        };
        let mut case = 0;
        for latched in [
            StandDown::Cancel(CancelReason::UpstreamNoLongerNeeded),
            StandDown::Quiesce,
        ] {
            for outcome in outcomes() {
                case += 1;
                let (owner, reporter) = reporter_for(identity(35, 1, case));
                reporter.running();
                owner.advance(
                    TaskState::Canceling,
                    Some(TerminationDetail::Canceled(
                        CancelReason::UpstreamNoLongerNeeded,
                    )),
                    TaskOutputFacts::default(),
                );
                owner.advance(
                    TaskState::Aborting,
                    Some(TerminationDetail::Aborted(AbortCause::PeerTaskFailed)),
                    TaskOutputFacts::default(),
                );

                report_terminal(
                    &reporter,
                    FragmentSinkKind::Noop,
                    &terminal_fact(outcome),
                    Some(latched),
                );

                let current = owner.current();
                assert_eq!(
                    (current.state(), current.termination().cloned()),
                    (
                        TaskState::Aborted,
                        Some(TerminationDetail::Aborted(AbortCause::PeerTaskFailed))
                    ),
                    "case {case}: {latched:?}"
                );
                assert!(owner.convergence().conclusion_stable(), "case {case}");
            }
        }
    }

    /// The Worker publishes a stand-down before it tells the runnable. A
    /// completion that lands in between finds no latch, and still completes
    /// the stand-down that won rather than proposing its own outcome.
    #[test]
    fn a_completion_before_the_stand_down_reaches_the_runnable_completes_it() {
        let (owner, reporter) = reporter_for(identity(36, 1, 1));
        reporter.running();
        owner.advance(
            TaskState::Canceling,
            Some(TerminationDetail::Canceled(
                CancelReason::UpstreamNoLongerNeeded,
            )),
            TaskOutputFacts::default(),
        );
        report_terminal(
            &reporter,
            FragmentSinkKind::Noop,
            &terminal_fact(FragmentOutcome::Succeeded),
            None,
        );
        assert_eq!(owner.state(), TaskState::Canceled);
        assert!(
            owner.current().output().responsibility_complete(),
            "the non-root sink that ran to success keeps that fact"
        );
        assert!(owner.output_released());

        let (owner, reporter) = reporter_for(identity(36, 1, 2));
        reporter.running();
        owner.advance(
            TaskState::Aborting,
            Some(TerminationDetail::Aborted(AbortCause::QueryFailed)),
            TaskOutputFacts::default(),
        );
        report_terminal(
            &reporter,
            FragmentSinkKind::Noop,
            &terminal_fact(FragmentOutcome::Failed(FragmentExecutionError::new(
                FragmentExecutionErrorKind::Pipeline,
                "driver failed",
            ))),
            None,
        );
        let current = owner.current();
        assert_eq!(
            (current.state(), current.termination().cloned()),
            (
                TaskState::Aborted,
                Some(TerminationDetail::Aborted(AbortCause::QueryFailed))
            ),
            "a late failure cannot rewrite the abort that already won"
        );
        assert!(owner.output_released());
    }

    #[test]
    fn a_successful_root_task_waits_for_its_result_stream_to_drain() {
        let task = identity(34, 1, 1);
        let (owner, reporter) = reporter_for(task);
        reporter.running();

        // Finishing here would declare the query's answer delivered while its
        // rows are still buffered on this backend.
        report_terminal(
            &reporter,
            FragmentSinkKind::Result,
            &terminal_fact(FragmentOutcome::Succeeded),
            None,
        );

        assert_eq!(owner.state(), TaskState::Flushing);
        assert!(!owner.output_released());
        assert!(matches!(
            owner.note_root_result_drained(),
            StatusAdvance::Published(_)
        ));
        assert_eq!(owner.state(), TaskState::Finished);
    }

    #[test]
    fn a_finished_root_result_stands_down_after_frontend_loss() {
        for stand_down_before_finish in [false, true] {
            let task_id = identity(if stand_down_before_finish { 341 } else { 342 }, 1, 1);
            let (owner, reporter) = reporter_for(task_id);
            let runnable = NativeRunnableTask::new(task_id, UniqueId::new(341, 342));
            novarocks_worker::result_buffer::create_task_typed_sender(task_id);
            reporter.running();
            reporter.note_actual_stopped();
            report_terminal(
                &reporter,
                FragmentSinkKind::Result,
                &terminal_fact(FragmentOutcome::Succeeded),
                None,
            );
            assert_eq!(owner.state(), TaskState::Flushing);

            if !stand_down_before_finish {
                runnable.finish(Some(&reporter));
            }
            // The Worker asks the runnable to stand down after retracting a
            // known result. Both orders around `finish` must also close any
            // result that a losing create installed after that retraction.
            reporter.aborting(AbortCause::LeaseExpired);
            runnable.abort(AbortCause::LeaseExpired);
            if stand_down_before_finish {
                runnable.finish(Some(&reporter));
            }
            reporter.note_resources_converged();
            assert_eq!(owner.state(), TaskState::Aborted);
            assert!(owner.output_released());
            assert!(owner.retirement_ready());
            assert_eq!(
                novarocks_worker::result_buffer::retire_task_result(task_id),
                novarocks_worker::result_buffer::ResultPublication::NoChange,
                "stand_down_before_finish={stand_down_before_finish}",
            );
            let settled_version = owner.current().version();
            runnable.abort(AbortCause::PeerTaskFailed);
            assert_eq!(owner.current().version(), settled_version);
        }
    }

    #[tokio::test]
    async fn normal_quiesce_preserves_running_and_flushing_root_payloads() {
        use novarocks_execution_contract::ResultByteLimit;
        use novarocks_worker::result_buffer::{TryFetchTypedResult, wait_fetch_task_typed};

        for already_flushing in [false, true] {
            let task_id = identity(if already_flushing { 345 } else { 346 }, 1, 1);
            let (owner, reporter) = reporter_for(task_id);
            let runnable = NativeRunnableTask::new(task_id, UniqueId::new(345, 346));
            novarocks_worker::result_buffer::create_task_typed_sender(task_id);
            novarocks_worker::result_buffer::insert_task_typed(task_id, vec![7, 8, 9])
                .expect("root payload");
            owner.note_installed();
            reporter.running();
            if already_flushing {
                reporter.note_actual_stopped();
                report_terminal(
                    &reporter,
                    FragmentSinkKind::Result,
                    &terminal_fact(FragmentOutcome::Succeeded),
                    None,
                );
                runnable.finish(Some(&reporter));
                assert_eq!(owner.state(), TaskState::Flushing);
            }
            runnable.quiesce();
            if !already_flushing {
                // The kernel's completion owner calls finish after the stop.
                runnable.finish(Some(&reporter));
            }
            assert_eq!(owner.state(), TaskState::Canceled);
            assert!(
                !owner.output_released(),
                "normal stop leaves root output owned until drain"
            );
            assert!(
                !owner.retirement_ready(),
                "retirement would remove unread root payload"
            );
            let fetched = wait_fetch_task_typed(
                task_id,
                None,
                Duration::ZERO,
                ResultByteLimit::new(1024).expect("byte cap"),
            )
            .await;
            let TryFetchTypedResult::Ready(packet) = fetched else {
                panic!("normal quiesce must preserve root payload: {fetched:?}");
            };
            assert_eq!(packet.payload.as_ref(), &[7, 8, 9]);
            novarocks_worker::result_buffer::close_task_ok(task_id);
            let eos = wait_fetch_task_typed(
                task_id,
                Some(packet.packet_seq),
                Duration::ZERO,
                ResultByteLimit::new(1024).expect("byte cap"),
            )
            .await;
            let TryFetchTypedResult::Ready(eos) = eos else {
                panic!("root EOS must remain readable: {eos:?}");
            };
            assert!(eos.eos);
            assert!(matches!(
                wait_fetch_task_typed(
                    task_id,
                    Some(eos.packet_seq),
                    Duration::ZERO,
                    ResultByteLimit::new(1024).expect("byte cap")
                )
                .await,
                TryFetchTypedResult::EndAcknowledged
            ));
            owner.note_root_result_drained();
            assert!(owner.output_released());
            novarocks_worker::result_buffer::discard_task(task_id);
        }
    }

    #[test]
    fn normal_quiesce_does_not_mask_a_real_fragment_failure() {
        let task_id = identity(347, 1, 1);
        let (owner, reporter) = reporter_for(task_id);
        owner.note_installed();
        reporter.running();
        reporter.canceling(CancelReason::UpstreamNoLongerNeeded);
        report_terminal(
            &reporter,
            FragmentSinkKind::Noop,
            &terminal_fact(FragmentOutcome::Failed(FragmentExecutionError::new(
                FragmentExecutionErrorKind::Pipeline,
                "driver failed during normal stop",
            ))),
            Some(StandDown::Quiesce),
        );
        assert_eq!(owner.state(), TaskState::Failed);
    }

    #[test]
    fn a_drained_root_result_keeps_its_finished_terminal_after_stand_down() {
        let task_id = identity(343, 1, 1);
        let (owner, reporter) = reporter_for(task_id);
        let runnable = NativeRunnableTask::new(task_id, UniqueId::new(343, 344));
        novarocks_worker::result_buffer::create_task_typed_sender(task_id);
        reporter.running();
        reporter.note_actual_stopped();
        report_terminal(
            &reporter,
            FragmentSinkKind::Result,
            &terminal_fact(FragmentOutcome::Succeeded),
            None,
        );
        runnable.finish(Some(&reporter));
        owner.note_root_result_drained();
        reporter.note_resources_converged();
        let finished_version = owner.current().version();

        runnable.abort(AbortCause::LeaseExpired);
        runnable.cancel(CancelReason::UpstreamNoLongerNeeded);
        assert_eq!(owner.state(), TaskState::Finished);
        assert_eq!(owner.current().version(), finished_version);
        assert!(owner.retirement_ready());
        assert_eq!(
            novarocks_worker::result_buffer::retire_task_result(task_id),
            novarocks_worker::result_buffer::ResultPublication::Removed,
        );
    }

    /// Only observes the original port's lifecycle; every registration and
    /// delivery still reaches the real execution-runtime registry.
    struct RetirementPort {
        inner: Arc<dyn novarocks_execution::runtime::fragment::io::ExchangeReceiverPort>,
        removed: AtomicUsize,
        cancelled: AtomicUsize,
        expected_schema: Mutex<Option<novarocks_execution::exec::chunk::ChunkSchemaRef>>,
    }

    impl novarocks_execution::runtime::fragment::io::ExchangeReceiverPort for RetirementPort {
        fn register(
            &self,
            registration: novarocks_execution::runtime::fragment::io::ExchangeReceiverRegistration,
        ) -> Result<(), String> {
            let schema = registration.expected_chunk_schema.clone();
            self.inner.register(registration)?;
            *self.expected_schema.lock().unwrap() = Some(schema);
            Ok(())
        }
        fn push(
            &self,
            key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey,
            frame: novarocks_execution::runtime::fragment::io::ExchangeReceiverFrame,
        ) -> Result<(), novarocks_execution::runtime::exchange::ExchangeReceiveFailure> {
            self.inner.push(key, frame)
        }
        fn cancel(&self, key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey) {
            self.cancelled.fetch_add(1, Ordering::SeqCst);
            self.inner.cancel(key);
        }
        fn remove(&self, key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey) {
            self.removed.fetch_add(1, Ordering::SeqCst);
            self.inner.remove(key);
        }
        fn cancel_fragment(&self, id: UniqueId) {
            self.inner.cancel_fragment(id);
        }
        fn receiver_handle(
            &self,
            key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey,
            expected: usize,
        ) -> Result<novarocks_execution::runtime::exchange::ExchangeReceiverHandle, String>
        {
            self.inner.receiver_handle(key, expected)
        }
        fn ensure_mem_tracker(
            &self,
            key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey,
            root: &Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
        ) -> Result<Arc<novarocks_execution::runtime::mem_tracker::MemTracker>, String> {
            self.inner.ensure_mem_tracker(key, root)
        }
        fn push_local(
            &self,
            key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey,
            sender: i32,
            backend: i32,
            chunks: Vec<novarocks_execution::exec::chunk::Chunk>,
            eos: bool,
        ) {
            self.inner.push_local(key, sender, backend, chunks, eos);
        }
        fn snapshot(
            &self,
            key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey,
        ) -> Option<novarocks_execution::runtime::exchange::ExchangeReceiverSnapshot> {
            self.inner.snapshot(key)
        }
    }

    #[derive(Clone, Copy)]
    enum ReceiverStop {
        Success,
        Quiesce,
        QuiesceThenAbort,
        BeforeStart,
        Abort,
        Rollback,
    }

    fn receiver_data_payload(port: &RetirementPort) -> Vec<u8> {
        use arrow::array::Int64Array;
        use arrow::datatypes::DataType;
        use novarocks_execution::exec::chunk::Chunk;
        use novarocks_type_contract::ValueKind;
        let schema = port
            .expected_schema
            .lock()
            .unwrap()
            .clone()
            .expect("actual registered declaration");
        assert_eq!(
            schema.slots().len(),
            1,
            "the original single-column receiver declaration"
        );
        let binding = schema.slots()[0].typed_binding();
        assert_eq!(
            binding.value_type().kind(),
            &ValueKind::Scalar(DataType::Int64)
        );
        assert_eq!(binding.layout().data_type(), &DataType::Int64);
        // These are declared Int64 rows, not inferred execution types.
        // The same original frozen schema validates the actual source chunk.
        let chunk =
            Chunk::try_new_with_columns(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        assert_eq!(chunk.len(), 1);
        let payload =
            novarocks_execution::runtime::exchange::encode_chunks(&[chunk], true).unwrap();
        assert!(
            !payload.is_empty(),
            "DATA must enter the original IPC decoder"
        );
        payload
    }

    fn receiver_retirement_case(
        mut host: NativeTaskExecutionHost,
        descriptor: TaskDescriptor,
        input: TaskCreationInput,
        stop: ReceiverStop,
        nonempty_data: bool,
    ) {
        use novarocks_execution::runtime::fragment::io::{
            ExchangeReceiverFrame, ExchangeReceiverKey, ExchangeReceiverPort,
        };
        let port = Arc::new(RetirementPort {
            inner: Arc::new(novarocks_execution::runtime::fragment::io::exchange_receiver::ExecutionRuntimeExchangeReceiverPort::new(Arc::clone(&host.execution_runtime))),
            removed: AtomicUsize::new(0),
            cancelled: AtomicUsize::new(0),
            expected_schema: Mutex::new(None),
        });
        host.exchange_receiver_port = port.clone();
        let inbound = &descriptor.topology().inbound()[0];
        let source = inbound.sources()[0];
        let key = ExchangeReceiverKey {
            fragment_instance_id: descriptor.fragment_instance_id(),
            node_id: inbound.node_id().get(),
        };
        establish_fixture_context(&host, descriptor.identity());
        host.install_receiver(
            &descriptor,
            input,
            novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
        )
        .expect("real receiver preparation");
        let runtime_alias = host.task_runtime(descriptor.identity()).unwrap();
        assert!(runtime_alias.exchange_retirement.lock().unwrap().is_some());
        assert!(port.snapshot(key).is_some());
        if matches!(stop, ReceiverStop::Rollback) {
            host.remove_receiver(&descriptor);
            host.remove_receiver(&descriptor);
            assert!(port.snapshot(key).is_none());
            assert_eq!(port.cancelled.load(Ordering::SeqCst), 1);
            assert_eq!(port.removed.load(Ordering::SeqCst), 0);
            assert!(runtime_alias.exchange_retirement.lock().unwrap().is_none());
            return;
        }
        host.reserve_inbound_close_capacity(&descriptor).unwrap();
        host.install_inbound_capability(&descriptor).unwrap();
        let status = Arc::new(TaskStatusSource::new());
        let owner = Arc::new(TaskStatusOwner::new(
            descriptor.identity(),
            status.clone(),
            Arc::new(ProcessMonotonicClock::new()),
            METRIC_PUBLISH_MIN_INTERVAL,
        ));
        owner.release_to_observers();
        owner.note_installed();
        let context = QueryContextRef::new(
            descriptor.identity().query_execution_id(),
            FrontendProcessId::new_v7(),
            descriptor.identity().backend_process_id(),
        );
        let observed = status
            .begin_covered_subscription(context, 1, &[], &[], None, &[descriptor.identity()], 1)
            .unwrap();
        let data_payload = nonempty_data.then(|| receiver_data_payload(&port));
        let reporter = TaskStatusReporter::new(owner.clone());
        let runnable = host.submit_runnable(&descriptor, reporter).unwrap();
        if matches!(stop, ReceiverStop::BeforeStart) {
            runnable.quiesce();
        }
        runnable.commit_creation();
        let frame = || ExchangeReceiverFrame {
            source_fragment_instance_id: source.fragment_instance_id(),
            sender_ordinal: source.sender_ordinal(),
            sender_count: 1,
            sender_id: 0,
            backend_number: 0,
            sequence: 0,
            eos: true,
            payload: Vec::new(),
        };
        if matches!(stop, ReceiverStop::Success) {
            if let Some(payload) = &data_payload {
                let mut data = frame();
                data.payload = payload.clone();
                data.eos = false;
                let delivery = host.capabilities.deliver_frame_with_receiver_error(
                    key.fragment_instance_id,
                    inbound.node_id(),
                    source.fragment_instance_id(),
                    source.sender_ordinal(),
                    1,
                    || port.push(key, data),
                );
                assert!(
                    matches!(
                        &delivery,
                        novarocks_worker::InboundFrameDelivery::Claim(
                            InboundFrameClaim::Authorized
                        )
                    ),
                    "the original DATA declaration is accepted before stop: {delivery:?}"
                );
            }
            port.push(key, frame()).expect("original exact sender EOS");
        } else if matches!(stop, ReceiverStop::Quiesce | ReceiverStop::QuiesceThenAbort) {
            runnable.quiesce();
        } else if matches!(stop, ReceiverStop::Abort) {
            runnable.abort(AbortCause::QueryFailed);
        }
        // Observe the original covered source's actual-stop publication.
        // Its concrete subscription stays inferred behind the Worker's port.
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        let cut = observed.source_cut();
                        if owner.convergence().actual_stopped() {
                            break;
                        }
                        observed.wait_for_change(cut).await;
                    }
                })
                .await
                .expect("the actual receiver drivers stop");
            });
        assert_eq!(port.removed.load(Ordering::SeqCst), 0);
        assert_eq!(port.cancelled.load(Ordering::SeqCst), 0);
        assert!(
            port.snapshot(key).is_some(),
            "actual-stop is not ingress retirement"
        );
        if matches!(stop, ReceiverStop::QuiesceThenAbort) {
            // The kernel has already quiesced and published actual-stop.
            // Its original first-wins latch must not suppress this abort.
            runnable.abort(AbortCause::QueryFailed);
        }
        if matches!(stop, ReceiverStop::Abort | ReceiverStop::QuiesceThenAbort)
            && let Some(payload) = &data_payload
        {
            use novarocks_execution::exec::pipeline::operator::ConstructionFailure;
            use novarocks_execution::runtime::exchange::ExchangeReceiveFailure;
            // No DATA or EOS was sent before stop in either abort scenario.
            // The original capability still authorizes delivery; construction
            // must preserve the true abort's original typed cause instead.
            let mut data = frame();
            data.payload = payload.clone();
            data.eos = false;
            let delivery = host.capabilities.deliver_frame_with_receiver_error(
                key.fragment_instance_id,
                inbound.node_id(),
                source.fragment_instance_id(),
                source.sender_ordinal(),
                1,
                || port.push(key, data),
            );
            let abort_preserved = matches!(
                &delivery,
                novarocks_worker::InboundFrameDelivery::Receiver(
                    ExchangeReceiveFailure::Construction(ConstructionFailure::Caller(error))
                ) if matches!(error.cause(), novarocks_execution::runtime::fragment::ExecutionFailureCause::Pipeline(detail)
                    if detail == AbortCause::QueryFailed.as_str())
            );
            if !abort_preserved {
                host.remove_inbound_capability(&descriptor);
                host.remove_receiver(&descriptor);
            }
            assert!(
                abort_preserved,
                "true abort must stop DATA creation with its original typed cause: {delivery:?}"
            );
        }
        if matches!(stop, ReceiverStop::Quiesce)
            && let Some(payload) = &data_payload
        {
            // No DATA or EOS entered this lane before quiesce. The sender is
            // still open: this is legal in-flight DATA, not data after EOS.
            let mut data = frame();
            data.payload = payload.clone();
            data.eos = false;
            let delivery = host.capabilities.deliver_frame_with_receiver_error(
                key.fragment_instance_id,
                inbound.node_id(),
                source.fragment_instance_id(),
                source.sender_ordinal(),
                1,
                || port.push(key, data),
            );
            if !matches!(
                &delivery,
                novarocks_worker::InboundFrameDelivery::Claim(InboundFrameClaim::Authorized)
            ) {
                // Preserve the original typed delivery cause through teardown.
                // A failed normative assertion must not leave ingress installed.
                host.remove_inbound_capability(&descriptor);
                host.remove_receiver(&descriptor);
            }
            assert!(
                matches!(
                    &delivery,
                    novarocks_worker::InboundFrameDelivery::Claim(InboundFrameClaim::Authorized)
                ),
                "normal quiesce must retain original DATA creation until the inbound gate closes: {delivery:?}"
            );
        }
        assert_eq!(
            host.capabilities.deliver_frame(
                key.fragment_instance_id,
                inbound.node_id(),
                source.fragment_instance_id(),
                source.sender_ordinal(),
                1,
                || port.push(key, frame()).map_err(|error| error.to_string())
            ),
            InboundFrameClaim::Authorized
        );
        if matches!(stop, ReceiverStop::Abort | ReceiverStop::QuiesceThenAbort) {
            host.remove_inbound_capability(&descriptor);
            host.remove_receiver(&descriptor);
        } else {
            host.retire_receiver_normally(&descriptor);
        }
        assert!(
            port.snapshot(key).is_none(),
            "physical retirement cannot wait for TaskRuntime's last Arc"
        );
        assert!(runtime_alias.exchange_retirement.lock().unwrap().is_none());
        let success = matches!(stop, ReceiverStop::Success);
        assert_eq!(port.removed.load(Ordering::SeqCst), usize::from(success));
        assert_eq!(port.cancelled.load(Ordering::SeqCst), usize::from(!success));
        let final_claim = host.capabilities.claim_frame(
            key.fragment_instance_id,
            inbound.node_id(),
            source.fragment_instance_id(),
            source.sender_ordinal(),
            1,
        );
        if matches!(stop, ReceiverStop::Abort | ReceiverStop::QuiesceThenAbort) {
            assert_eq!(final_claim, InboundFrameClaim::NotHeld);
        } else {
            assert!(matches!(final_claim, InboundFrameClaim::NormallyClosed(_)));
        }
        if !matches!(stop, ReceiverStop::Abort | ReceiverStop::QuiesceThenAbort) {
            assert!(matches!(
                host.capabilities.claim_frame(
                    key.fragment_instance_id,
                    inbound.node_id(),
                    UniqueId::new(-1, -1),
                    0,
                    1
                ),
                InboundFrameClaim::Refused(_)
            ));
        }
        host.remove_receiver(&descriptor);
        assert_eq!(
            port.removed.load(Ordering::SeqCst) + port.cancelled.load(Ordering::SeqCst),
            1
        );
        drop(runtime_alias);
    }

    #[test]
    fn plan_tree_receiver_retirement_waits_for_the_original_inbound_gate() {
        for (ordinal, stop) in [
            ReceiverStop::Success,
            ReceiverStop::Quiesce,
            ReceiverStop::BeforeStart,
            ReceiverStop::Abort,
            ReceiverStop::Rollback,
        ]
        .into_iter()
        .enumerate()
        {
            let facts = Arc::new(StubContextFacts::default());
            let host = host(facts);
            let query = 83_100 + ordinal as i64;
            let descriptor = descriptor_with(
                identity(query, 1, 1),
                UniqueId::new(query, 1),
                1,
                inbound_topology(
                    FragmentNodeId::new(10),
                    vec![ExchangeSource::new(
                        identity(query, 2, 1),
                        UniqueId::new(query, 2),
                        0,
                    )],
                ),
            );
            let mut body = Body::values(1);
            body.frozen
                .plan
                .as_mut()
                .unwrap()
                .root
                .as_mut()
                .unwrap()
                .payload = Some(plan::distributed_node::Payload::Exchange(
                plan::ExchangeReceiver {
                    partition_type: plan::PartitionType::Unpartitioned as i32,
                    source_fragment_id: 8,
                    flavor: Some(plan::ExchangeFlavor {
                        kind: Some(plan::exchange_flavor::Kind::Distribution(true)),
                    }),
                    ..Default::default()
                },
            ));
            let input = body.input(&descriptor);
            receiver_retirement_case(host, descriptor, input, stop, false);
        }
    }

    #[test]
    fn plan_tree_receiver_nonempty_data_remains_authorized_after_normal_quiesce() {
        for (ordinal, stop) in [ReceiverStop::Success, ReceiverStop::Quiesce]
            .into_iter()
            .enumerate()
        {
            let host = host(Arc::new(StubContextFacts::default()));
            let query = 83_400 + ordinal as i64;
            let descriptor = descriptor_with(
                identity(query, 1, 1),
                UniqueId::new(query, 1),
                1,
                inbound_topology(
                    FragmentNodeId::new(10),
                    vec![ExchangeSource::new(
                        identity(query, 2, 1),
                        UniqueId::new(query, 2),
                        0,
                    )],
                ),
            );
            let mut body = Body::values(1);
            let ty = novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            );
            let column = novarocks_proto_models::common::OutputColumn {
                column_id: 10,
                name: "v".into(),
                r#type: Some(fixture_type_reference(0)),
                nullable: false,
                is_internal: false,
            };
            let fragment = body.frozen.plan.as_mut().unwrap();
            fragment.execution_types = Some(encode_fixture_type_table(&[(0, &ty)]));
            fragment.output_columns = vec![column.clone()];
            fragment.root.as_mut().unwrap().payload = Some(
                plan::distributed_node::Payload::Exchange(plan::ExchangeReceiver {
                    partition_type: plan::PartitionType::Unpartitioned as i32,
                    source_fragment_id: 8,
                    output_columns: vec![column],
                    flavor: Some(plan::ExchangeFlavor {
                        kind: Some(plan::exchange_flavor::Kind::Distribution(true)),
                    }),
                    ..Default::default()
                }),
            );
            let input = body.input(&descriptor);
            receiver_retirement_case(host, descriptor, input, stop, true);
        }
    }

    #[test]
    fn plan_tree_receiver_nonempty_data_stops_on_true_abort() {
        for (ordinal, stop) in [ReceiverStop::Abort, ReceiverStop::QuiesceThenAbort]
            .into_iter()
            .enumerate()
        {
            let host = host(Arc::new(StubContextFacts::default()));
            let query = 83_500 + ordinal as i64;
            let descriptor = descriptor_with(
                identity(query, 1, 1),
                UniqueId::new(query, 1),
                1,
                inbound_topology(
                    FragmentNodeId::new(10),
                    vec![ExchangeSource::new(
                        identity(query, 2, 1),
                        UniqueId::new(query, 2),
                        0,
                    )],
                ),
            );
            let mut body = Body::values(1);
            let ty = novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            );
            let column = novarocks_proto_models::common::OutputColumn {
                column_id: 10,
                name: "v".into(),
                r#type: Some(fixture_type_reference(0)),
                nullable: false,
                is_internal: false,
            };
            let fragment = body.frozen.plan.as_mut().unwrap();
            fragment.execution_types = Some(encode_fixture_type_table(&[(0, &ty)]));
            fragment.output_columns = vec![column.clone()];
            fragment.root.as_mut().unwrap().payload = Some(
                plan::distributed_node::Payload::Exchange(plan::ExchangeReceiver {
                    partition_type: plan::PartitionType::Unpartitioned as i32,
                    source_fragment_id: 8,
                    output_columns: vec![column],
                    flavor: Some(plan::ExchangeFlavor {
                        kind: Some(plan::exchange_flavor::Kind::Distribution(true)),
                    }),
                    ..Default::default()
                }),
            );
            let input = body.input(&descriptor);
            receiver_retirement_case(host, descriptor, input, stop, true);
        }
    }

    #[test]
    fn an_abort_racing_a_submitted_task_still_reaches_a_terminal_status() {
        let facts = Arc::new(StubContextFacts::default());
        let mut host = host(Arc::clone(&facts));
        host.exchange_receiver_port = Arc::new(
            novarocks_execution::runtime::fragment::io::exchange_receiver::ExecutionRuntimeExchangeReceiverPort::new(
                Arc::clone(&host.execution_runtime),
            ),
        );
        let task = identity(35, 1, 1);
        let node = FragmentNodeId::new(10);
        let descriptor = descriptor_with(
            task,
            UniqueId::new(261, 262),
            1,
            inbound_topology(
                node,
                vec![ExchangeSource::new(
                    identity(35, 2, 1),
                    UniqueId::new(263, 264),
                    0,
                )],
            ),
        );
        let (owner, reporter) = reporter_for(task);

        // This sender never sends data or EOS, so the fragment cannot finish
        // before the abort wins the first-wins terminal latch.
        let mut body = Body::values(1);
        body.frozen
            .plan
            .as_mut()
            .expect("a plan")
            .root
            .as_mut()
            .expect("a root")
            .payload = Some(plan::distributed_node::Payload::Exchange(
            plan::ExchangeReceiver {
                partition_type: plan::PartitionType::Unpartitioned as i32,
                source_fragment_id: 8,
                flavor: Some(plan::ExchangeFlavor {
                    kind: Some(plan::exchange_flavor::Kind::Distribution(true)),
                }),
                ..Default::default()
            },
        ));
        establish_fixture_context(&host, descriptor.identity());
        host.install_receiver(
            &descriptor,
            body.input(&descriptor),
            novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
        )
        .expect("prepares");
        let runnable = submit_committed(&host, &descriptor, reporter);

        // The owner publishes ABORTING and then asks the task to stand down,
        // which may land before or after the worker started the fragment.
        // Either way the task must terminate: a stand-down lost in that window
        // would leave the query waiting for a task nothing can stop.
        owner.force_conclusion(AbortCause::QueryFailed);
        runnable.abort(AbortCause::QueryFailed);

        assert_eq!(await_terminal(&owner), TaskState::Aborted);
        host.remove_receiver(&descriptor);
    }

    #[test]
    fn a_task_cannot_be_submitted_twice() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(26, 1, 1);
        let descriptor = consistent_descriptor(task, UniqueId::new(211, 212));
        let (owner, reporter) = reporter_for(task);

        install(&host, &descriptor).expect("prepares");
        let _runnable = submit_committed(&host, &descriptor, reporter.clone());

        // The dormant handle is taken exactly once. A second submit must not
        // start a second execution over the same pipeline.
        let rejection = host
            .submit_runnable(&descriptor, reporter)
            .expect_err("one fragment, one execution");
        assert!(
            rejection.detail().as_str().contains("submitted twice"),
            "{rejection}"
        );

        await_terminal(&owner);
        host.remove_receiver(&descriptor);
    }

    // -------------------------------------------------- operator statistics

    /// Records every event it is handed, so a fan-out can be observed.
    #[derive(Default)]
    struct CountingEventSink {
        seen: AtomicUsize,
    }

    impl FragmentEventSink for CountingEventSink {
        fn record(&self, _event: FragmentEvent) {
            self.seen.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Catches a composition that hands a task's events to one owner only.
    /// The query context folds runtime-filter evidence and the task owns its
    /// metrics; whichever sink were dropped would lose its facts silently,
    /// because a fragment reports through exactly one sink handle.
    #[test]
    fn a_composed_event_sink_delivers_each_event_to_every_owner() {
        let filters = Arc::new(CountingEventSink::default());
        let metrics = Arc::new(CountingEventSink::default());
        let composite = CompositeFragmentEventSink::new(vec![
            Arc::clone(&filters) as Arc<dyn FragmentEventSink>,
            Arc::clone(&metrics) as Arc<dyn FragmentEventSink>,
        ]);

        composite.record(FragmentEvent::Progress(FragmentProgress::new(
            UniqueId::new(1, 2),
            0,
            0,
            0,
        )));

        assert_eq!(filters.seen.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.seen.load(Ordering::SeqCst), 1);
    }

    /// Catches a metrics sink that reports onto a task that has no reporter
    /// yet. Preparation can already create workers that hold the sink, and the
    /// reporter is minted only when the task becomes runnable.
    #[test]
    fn a_metrics_sink_without_a_reporter_records_nothing_rather_than_panicking() {
        let kernel_key = UniqueId::new(11, 12);
        let profiler = RuntimeProfile::new("execute_fragment_native (plan_node_id=10)");
        let sink = TaskOperatorStatisticsSink::new(kernel_key, Some(profiler));

        sink.record(FragmentEvent::Progress(FragmentProgress::new(
            kernel_key, 0, 0, 0,
        )));
    }

    /// Catches a metrics sink that reports statistics for a query that asked
    /// for no profile. Without a profiler there is no counter to read, and a
    /// list of zeroes would claim measurements nothing took.
    #[test]
    fn a_metrics_sink_without_a_profiler_reports_no_statistics() {
        let task = identity(40, 1, 1);
        let kernel_key = UniqueId::new(13, 14);
        let (owner, reporter) = reporter_for(task);
        let sink = TaskOperatorStatisticsSink::new(kernel_key, None);
        sink.bind(reporter.clone());

        sink.record(FragmentEvent::Progress(FragmentProgress::new(
            kernel_key, 0, 0, 0,
        )));
        reporter.running();
        reporter.finished(TaskOutputFacts::new(true));

        let final_info = owner.final_info().expect("a terminal task has final info");
        assert!(final_info.operator_statistics().is_empty());
        assert!(!final_info.operator_statistics_truncated());
    }

    /// Catches a progress tick that reports whole-fragment totals as if they
    /// were one operator's, instead of reading the operators' own counters.
    #[test]
    fn a_progress_tick_reports_the_operators_counters_not_the_fragment_totals() {
        let task = identity(41, 1, 1);
        let kernel_key = UniqueId::new(15, 16);
        let (owner, reporter) = reporter_for(task);
        let profiler = RuntimeProfile::new("execute_fragment_native (plan_node_id=10)");
        let common = profiler
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)")
            .child("ValuesSource (id=10)")
            .child("CommonMetrics");
        common.counter_set("PushRowNum", ProfileUnit::Unit, 0);
        common.counter_set("PullRowNum", ProfileUnit::Unit, 7);
        let sink = TaskOperatorStatisticsSink::new(kernel_key, Some(profiler));
        sink.bind(reporter.clone());

        // The tick carries zeroes for the whole fragment; the operator counted
        // seven rows. The recorded statistics must be the operator's.
        sink.record(FragmentEvent::Progress(FragmentProgress::new(
            kernel_key, 0, 0, 0,
        )));
        reporter.running();
        reporter.finished(TaskOutputFacts::new(true));

        let final_info = owner.final_info().expect("a terminal task has final info");
        let entries = final_info.operator_statistics();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].plan_node_id(), 10);
        assert_eq!(entries[0].operator().as_str(), "ValuesSource");
        assert_eq!(entries[0].output_rows(), Some(7));
    }

    /// Catches the whole production wiring: a profiled task must carry its
    /// operators' facts in the final info the frontend reads, keyed by the
    /// plan node the plan named. Before this producer existed the field was
    /// always empty, so an `EXPLAIN ANALYZE` over the task protocol had
    /// nothing to render.
    #[test]
    fn a_profiled_task_reports_its_operator_statistics_in_its_final_info() {
        let facts = Arc::new(StubContextFacts::default());
        facts
            .query_options
            .lock()
            .expect("stub query options")
            .enable_profile = true;
        facts.set_wire_options(proto::QueryOptions {
            pipeline_dop: 1,
            enable_profile: true,
            ..Default::default()
        });
        let host = host(Arc::clone(&facts));
        let task = identity(42, 1, 1);
        // Profiling is the established context's option alone; the task's
        // creation body carries no copy of it.
        let descriptor = consistent_descriptor(task, UniqueId::new(271, 272));
        let (owner, reporter) = reporter_for(task);

        install(&host, &descriptor).expect("prepares");
        let _runnable = submit_committed(&host, &descriptor, reporter);
        assert_eq!(await_terminal(&owner), TaskState::Finished);

        let final_info = owner.final_info().expect("a terminal task has final info");
        let entries = final_info.operator_statistics();
        // The fixture plan is one VALUES node feeding a NOOP sink. The VALUES
        // operator names plan node 10; the NOOP sink names no plan node at all
        // and is therefore refused rather than attributed to node 10.
        assert_eq!(
            entries.len(),
            1,
            "expected only the plan-node-bearing operator, got {entries:?}"
        );
        assert_eq!(entries[0].plan_node_id(), 10);
        assert_eq!(entries[0].operator().as_str(), "ValuesSource");
        assert!(!final_info.operator_statistics_truncated());

        host.remove_receiver(&descriptor);
    }

    /// Catches a wiring that profiles every task regardless of what the query
    /// asked for. Profiling is the query's decision, and a task that was not
    /// asked to measure must report nothing rather than zeroes.
    #[test]
    fn an_unprofiled_task_reports_no_operator_statistics() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(43, 1, 1);
        let descriptor = consistent_descriptor(task, UniqueId::new(281, 282));
        let (owner, reporter) = reporter_for(task);

        install(&host, &descriptor).expect("prepares");
        let _runnable = submit_committed(&host, &descriptor, reporter);
        assert_eq!(await_terminal(&owner), TaskState::Finished);

        let final_info = owner.final_info().expect("a terminal task has final info");
        assert!(final_info.operator_statistics().is_empty());

        host.remove_receiver(&descriptor);
    }

    // --------------------------------------- codec -> task owner -> this host

    /// This host behind the real task owner, counting how often the owner
    /// reaches its creation-time interpretation.
    struct CountingHost {
        inner: NativeTaskExecutionHost,
        installs: AtomicUsize,
    }

    impl TaskExecutionHost for CountingHost {
        fn close_context_admission(&self, context: QueryContextRef) {
            self.inner.close_context_admission(context);
        }

        fn retire_context_execution(&self, context: QueryContextRef) {
            self.inner.retire_context_execution(context);
        }

        fn forget_context_admission(&self, context: QueryContextRef) {
            self.inner.forget_context_admission(context);
        }

        fn install_receiver(
            &self,
            descriptor: &TaskDescriptor,
            input: TaskCreationInput,
            _preparation_stop: novarocks_worker::TaskPreparationStopView,
        ) -> Result<novarocks_worker::PreparedTaskInstallation, HostRejection> {
            self.installs.fetch_add(1, Ordering::SeqCst);
            self.inner
                .install_receiver(descriptor, input, _preparation_stop)
        }

        fn remove_receiver(&self, descriptor: &TaskDescriptor) {
            self.inner.remove_receiver(descriptor);
        }

        fn install_inbound_capability(
            &self,
            descriptor: &TaskDescriptor,
        ) -> Result<(), HostRejection> {
            self.inner.install_inbound_capability(descriptor)
        }

        fn remove_inbound_capability(&self, descriptor: &TaskDescriptor) {
            self.inner.remove_inbound_capability(descriptor);
        }

        fn submit_runnable(
            &self,
            descriptor: &TaskDescriptor,
            reporter: TaskStatusReporter,
        ) -> Result<Arc<dyn RunnableTask>, HostRejection> {
            self.inner.submit_runnable(descriptor, reporter)
        }

        fn apply_task_domain(
            &self,
            descriptor: &TaskDescriptor,
            domain: &TaskDomainUpdate,
        ) -> Result<Option<u64>, HostRejection> {
            self.inner.apply_task_domain(descriptor, domain)
        }
    }

    /// The fixture owns the actual query resources while keeping its unrelated
    /// catalog and runtime-filter ports closed, as those tests declare.
    struct AcceptingContextHost {
        queries: NativeFragmentQueryRuntime,
        facts: Arc<StubContextFacts>,
    }

    impl QueryContextHost for AcceptingContextHost {
        fn materialize(&self, request: SharedFactsRequest<'_>) -> Result<(), HostRejection> {
            establish_fixture_resources(
                &self.queries,
                self.facts.as_ref(),
                request.context().query_execution_id(),
            )
        }
        fn release(&self, context: QueryContextRef) -> ReleasedContextEvidence {
            self.queries
                .retire_idle_execution(context.query_execution_id());
            ReleasedContextEvidence::none()
        }
        fn advance_shared_domain(
            &self,
            _context: QueryContextRef,
            _domain: &QueryContextDomainUpdate,
        ) -> Result<(), HostRejection> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct PayloadStub(u8);

    impl CodecOwnedContent for PayloadStub {
        fn fingerprint(&self) -> ContentFingerprint {
            ContentFingerprint::from_bytes([self.0; 16])
        }

        fn encoded_len(&self) -> usize {
            16
        }
    }

    struct SecretStub;

    impl ConfidentialContent for SecretStub {
        fn encoded_len(&self) -> usize {
            16
        }

        fn matches(&self, other: &dyn ConfidentialContent) -> bool {
            other.encoded_len() == self.encoded_len()
        }
    }

    /// One established query context on a real task owner whose task host is
    /// this production host.
    struct OwnerFixture {
        registry: Arc<TaskExecutionRegistry>,
        host: Arc<CountingHost>,
        context: QueryContextRef,
        manager: Arc<novarocks_worker::query_context::QueryContextManager>,
    }

    impl OwnerFixture {
        fn new(query: i64) -> Self {
            Self::new_with_context_dop(query, 1)
        }
        fn new_with_context_dop(query: i64, dop: i32) -> Self {
            Self::new_with_legacy_root_limit(query, dop, 256 * 1024 * 1024)
        }
        fn new_with_legacy_root_limit(query: i64, dop: i32, legacy_root_bytes: usize) -> Self {
            let backend = BackendProcessId::new_v7();
            let facts = Arc::new(StubContextFacts::default());
            facts.set_context_dop(dop);
            let manager = Arc::clone(&facts.query_manager);
            let mut inner = host(Arc::clone(&facts));
            inner.result_retained_limits = novarocks_worker::WorkerResultRetainedLimits::try_new(
                legacy_root_bytes,
                inner.result_retained_limits.per_process().get(),
            )
            .unwrap();
            let counting = Arc::new(CountingHost {
                inner,
                installs: AtomicUsize::new(0),
            });
            let mut config = TaskExecutionRegistryConfig::for_process(backend, 16, 16);
            config.gate_poll_interval = Duration::from_secs(3600);
            let registry = TaskExecutionRegistry::new(
                config,
                Arc::new(ManualClock::new()) as Arc<dyn WorkerMonotonicClock>,
                Arc::new(AcceptingContextHost {
                    queries: counting.inner.queries.clone(),
                    facts,
                }),
                Arc::clone(&counting) as Arc<dyn TaskExecutionHost>,
                novarocks_native_adapter::task_execution_observation::backend_task_execution_ports(
                    #[cfg(novarocks_dev_controls)]
                    None,
                ),
            );
            let context =
                QueryContextRef::new(execution(query), FrontendProcessId::new_v7(), backend);
            let ticket = registry
                .acquire_query_context_admission_ticket(AcquireQueryContextAdmissionTicket::new(
                    TaskOperationId::new_v7(),
                    context,
                    LeaseValidFor::new(Duration::from_secs(10)).expect("a legal ticket lease"),
                    NativeCompatibilityId::new([0x71; 32]),
                    registry.admission_epoch_capability(),
                ))
                .acknowledgement()
                .expect("an admission ticket")
                .ticket_id();
            let established = registry.update_query_context(&UpdateQueryContext::Establish(
                EstablishQueryContext::new(
                    TaskOperationId::new_v7(),
                    context,
                    ticket,
                    Arc::new(PayloadStub(1)),
                    Arc::new(PayloadStub(2)),
                    Arc::new(PayloadStub(3)),
                    CredentialUpdate::new(
                        CredentialLeaseId::new(1),
                        CredentialEpoch::FIRST,
                        Arc::new(SecretStub),
                    ),
                    LeaseValidFor::new(Duration::from_secs(30)).expect("a legal context lease"),
                ),
            ));
            assert_eq!(
                established.outcome(),
                OperationOutcome::Accepted,
                "{established:?}"
            );
            Self {
                registry,
                host: counting,
                context,
                manager,
            }
        }

        fn identity(&self, task: u32) -> TaskIdentity {
            TaskIdentity::new(
                self.context.query_execution_id(),
                StageId::new(1).expect("nonzero stage"),
                TaskId::new(task).expect("nonzero task"),
                self.context.backend_process_id(),
            )
        }

        /// Sends one create through the real codec, exactly as the ingress
        /// does: encode the two carriers, decode the operation once, and hand
        /// the owner the neutral request and the creation input it carries.
        fn create(
            &self,
            descriptor: &TaskDescriptor,
            body: &Body,
        ) -> novarocks_worker::CreateTaskOutcome {
            let metadata = novarocks_task_codec::creation::encode_creation_metadata(
                self.context,
                descriptor,
                body.assignment.clone(),
                Vec::new(),
            );
            let operation = novarocks_task_codec::operation::encode_create_task(
                OperationEnvelope::with_default_wait(
                    TaskOperationId::new_v7(),
                    OperationKind::CreateTask,
                ),
                &body.frozen_bytes(),
                &FrozenBytes::freeze(metadata.encode_to_vec().into()),
            );
            let novarocks_task_codec::operation::DecodedOperation::CreateTask(decoded) =
                novarocks_task_codec::operation::decode_operation(
                    &operation,
                    FieldPath::root("operation"),
                )
                .expect("the codec admits the create")
            else {
                panic!("a create decodes as a create");
            };
            let (request, input): (CreateTask, TaskCreationInput) = decoded.into_parts();
            self.registry.create_task(&request, input)
        }
    }

    #[test]
    fn terminal_task_context_retires_prepared_unregistered_query_context() {
        let fixture = OwnerFixture::new(60_901);
        let execution = fixture.context.query_execution_id();
        let query = QueryExecutionKey::native_attempt(
            QueryId::new(execution.query_id().high(), execution.query_id().low()),
            NonZeroU64::new(execution.attempt_id().get()).unwrap(),
        );
        fixture
            .host
            .inner
            .queries
            .prepare_admission_execution(
                execution,
                UniqueId::new(60_903, 1),
                Duration::from_secs(1),
                Duration::from_secs(300),
                None,
                None,
            )
            .expect("preparation creates the zero-fragment query context");
        assert!(fixture.manager.query_mem_tracker_execution(query).is_some());

        let quiesced = fixture.registry.quiesce_query_context(
            &novarocks_execution_contract::task_execution::operation::QuiesceQueryContext::new(
                TaskOperationId::new_v7(),
                fixture.context,
            ),
        );
        assert_eq!(quiesced.outcome(), OperationOutcome::Accepted);

        let released = fixture
            .registry
            .release_query_context(&ReleaseQueryContext::new(
                TaskOperationId::new_v7(),
                fixture.context,
            ));
        assert_eq!(released.outcome(), OperationOutcome::Accepted);
        assert!(fixture.manager.query_mem_tracker_execution(query).is_none());
    }

    /// A winning round interprets its body once; a replay of the same
    /// identity, whatever it carries, is answered from the task that round
    /// prepared and never reaches this host.
    #[test]
    fn a_winning_round_prepares_once_and_a_replay_reuses_the_prepared_task() {
        let fixture = OwnerFixture::new(61);
        let identity = fixture.identity(1);
        let descriptor = consistent_descriptor(identity, UniqueId::new(611, 1));
        let accepted = fixture.create(&descriptor, &Body::values(1));
        assert_eq!(
            accepted.outcome(),
            OperationOutcome::Accepted,
            "{accepted:?}"
        );

        // Every fact a body carries changes -- the parallelism and its frozen
        // domain, the kernel key, the instance ordinal and the sink -- and
        // the replay is still only a request for the task that exists.
        let changed = descriptor_with(
            identity,
            UniqueId::new(611, 2),
            2,
            ExchangeTopology::default(),
        );
        let mut changed_body = Body::with_sink(
            2,
            plan::DataSink {
                kind: Some(plan::data_sink::Kind::Result(true)),
            },
            Vec::new(),
        );
        changed_body.assignment.instance_ordinal = 9;
        let replay = fixture.create(&changed, &changed_body);
        assert_eq!(replay.outcome(), OperationOutcome::Idempotent, "{replay:?}");
        assert_eq!(replay.acknowledgement(), accepted.acknowledgement());
        assert_eq!(
            fixture.host.installs.load(Ordering::SeqCst),
            1,
            "the host interpreted the winner's body and never the replay's"
        );
        // The NOOP sink the winner proved still governs the task, live or
        // retired: the replay's result sink promoted nothing.
        let route = fixture.registry.root_result_route(identity);
        assert!(
            !matches!(
                route,
                novarocks_worker::RootResultRoute::Serve(_)
                    | novarocks_worker::RootResultRoute::TerminalResultOwner(_)
            ),
            "{route:?}"
        );
    }

    /// A first round whose static plan this host refuses leaves nothing of the
    /// host's behind, and a later legal round of the same identity wins.
    #[test]
    fn a_refused_first_round_leaves_no_native_state_and_a_later_round_wins() {
        let fixture = OwnerFixture::new(62);
        let identity = fixture.identity(1);
        let descriptor = descriptor_with(
            identity,
            UniqueId::new(621, 1),
            2,
            ExchangeTopology::default(),
        );

        // Frozen for one driver, created for two.
        let refused = fixture.create(&descriptor, &Body::values(1));
        assert_eq!(
            refused.outcome(),
            OperationOutcome::InvalidStateOrRequest,
            "{refused:?}"
        );
        assert!(
            refused
                .detail()
                .is_some_and(|detail| detail.as_str().contains("DOP domain")),
            "{refused:?}"
        );
        assert_eq!(fixture.host.installs.load(Ordering::SeqCst), 1);
        assert!(
            fixture
                .host
                .inner
                .tasks
                .lock()
                .expect("host tasks")
                .is_empty()
        );
        assert!(fixture.host.inner.split_queues.is_empty());
        assert!(fixture.host.inner.capabilities.is_empty());
        assert!(!fixture.registry.has_live_task(identity));

        let accepted = fixture.create(&descriptor, &Body::values(2));
        assert_eq!(
            accepted.outcome(),
            OperationOutcome::Accepted,
            "{accepted:?}"
        );
        assert_eq!(fixture.host.installs.load(Ordering::SeqCst), 2);
    }

    fn wait_bounded_root_finished(fixture: &OwnerFixture, root: TaskIdentity) {
        let source = fixture.registry.status_source(fixture.context).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = source.latest(root) {
                if status.state() == TaskState::Finished {
                    return;
                }
                assert!(
                    !status.is_terminal(),
                    "unexpected root terminal: {status:?}"
                );
            }
            assert!(
                std::time::Instant::now() < deadline,
                "root did not finish without ACK: {:?}",
                source.latest(root)
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    struct PausedRootWorker {
        open: Mutex<bool>,
        changed: std::sync::Condvar,
        started: Mutex<Option<std::sync::mpsc::SyncSender<()>>>,
    }
    impl crate::root_producer_pool::RootProducerJob for PausedRootWorker {
        fn turn(&self) -> crate::root_producer_pool::RootProducerTurn {
            if let Some(sender) = self.started.lock().unwrap().take() {
                sender.send(()).unwrap();
            }
            let mut open = self.open.lock().unwrap();
            while !*open {
                open = self.changed.wait(open).unwrap();
            }
            crate::root_producer_pool::RootProducerTurn::Complete
        }
        fn cancel(&self) {
            *self.open.lock().unwrap() = true;
            self.changed.notify_all();
        }
        fn exited(&self) {}
    }
    struct RootWorkerPause {
        job: Arc<dyn crate::root_producer_pool::RootProducerJob>,
        _registration: crate::root_producer_pool::RootProducerRegistration,
    }
    impl RootWorkerPause {
        fn new(pool: &Arc<RootProducerPool>) -> Self {
            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            let job: Arc<dyn crate::root_producer_pool::RootProducerJob> =
                Arc::new(PausedRootWorker {
                    open: Mutex::new(false),
                    changed: std::sync::Condvar::new(),
                    started: Mutex::new(Some(sender)),
                });
            let registration = pool.register(Arc::downgrade(&job)).unwrap();
            let pause = Self {
                job,
                _registration: registration,
            };
            pause._registration.wake().unwrap();
            receiver.recv_timeout(Duration::from_secs(5)).unwrap();
            pause
        }
        fn open(&self) {
            self.job.cancel();
        }
    }
    impl Drop for RootWorkerPause {
        fn drop(&mut self) {
            self.open();
        }
    }
    struct CallbackPause(Arc<PausedRootWorker>);
    impl Drop for CallbackPause {
        fn drop(&mut self) {
            *self.0.open.lock().unwrap() = true;
            self.0.changed.notify_all();
        }
    }

    #[test]
    fn bounded_root_seal_preserves_worker_abort_before_runnable_fanout() {
        use novarocks_execution_contract::task_execution::status::TerminationDetail;
        for (query, cause) in [
            (91_205, AbortCause::QueryFailed),
            (91_206, AbortCause::PeerTaskFailed),
            (91_207, AbortCause::LeaseExpired),
        ] {
            let fixture = OwnerFixture::new(query);
            let pause = RootWorkerPause::new(&fixture.host.inner.root_producer_pool);
            let root = fixture.identity(1);
            let descriptor = consistent_descriptor(root, UniqueId::new(query, 1));
            let receipt = fixture.create(
                &descriptor,
                &Body::bounded_root(novarocks_result_contract::FrozenRootOutput::CountOnly, &[1]),
            );
            assert_eq!(receipt.outcome(), OperationOutcome::Accepted);
            let channel = fixture
                .host
                .inner
                .task_runtime(root)
                .unwrap()
                .root_channel
                .clone()
                .unwrap();
            let source = fixture.registry.status_source(fixture.context).unwrap();
            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            let fanout_gate = Arc::new(PausedRootWorker {
                open: Mutex::new(false),
                changed: std::sync::Condvar::new(),
                started: Mutex::new(None),
            });
            let callback_gate = fanout_gate.clone();
            let fanout_pause = CallbackPause(fanout_gate.clone());
            let abort_thread_id = Arc::new(Mutex::new(None));
            let callback_thread_id = abort_thread_id.clone();
            let source_at_seal = source.clone();
            let weak_channel = Arc::downgrade(&channel);
            let once = std::sync::atomic::AtomicBool::new(true);
            let observer = channel.writable_observable().subscribe(Arc::new(move || {
                if weak_channel
                    .upgrade()
                    .is_some_and(|channel| channel.is_closed())
                    && *callback_thread_id.lock().unwrap() == Some(std::thread::current().id())
                    && once.swap(false, Ordering::AcqRel)
                {
                    sender.send(source_at_seal.latest(root).unwrap()).unwrap();
                    let mut open = callback_gate.open.lock().unwrap();
                    while !*open {
                        open = callback_gate.changed.wait(open).unwrap();
                    }
                }
            }));
            let registry = fixture.registry.clone();
            let context = fixture.context;
            let abort = std::thread::spawn(move || {
                *abort_thread_id.lock().unwrap() = Some(std::thread::current().id());
                registry.abort_query_context(&novarocks_execution_contract::task_execution::operation::AbortQueryContext::new(TaskOperationId::new_v7(), context, cause))
            });
            let at_seal = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
            // Unblock the actual producer while the runnable abort is still
            // held behind finish_seal's callback. The driver must conclude
            // from the cause fixed under the Worker fence.
            pause.open();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let terminal = loop {
                let current = source.latest(root).unwrap();
                if current.is_terminal() {
                    break current;
                }
                if std::time::Instant::now() >= deadline {
                    break current;
                }
                std::thread::sleep(Duration::from_millis(1));
            };
            drop(fanout_pause);
            let receipt = abort.join().unwrap();
            assert_eq!(receipt.outcome(), OperationOutcome::Accepted);
            assert_eq!(at_seal.state(), TaskState::Aborting);
            assert_eq!(
                at_seal.termination(),
                Some(&TerminationDetail::Aborted(cause))
            );
            assert_eq!(
                terminal.state(),
                TaskState::Aborted,
                "real driver must complete while runnable fan-out is paused: {terminal:?}"
            );
            assert_eq!(
                terminal.termination(),
                Some(&TerminationDetail::Aborted(cause))
            );
            drop(observer);
            fixture.host.inner.root_producer_pool.shutdown().unwrap();
        }
    }

    #[test]
    fn bounded_root_exit_observer_panic_still_wakes_real_pending_finish_driver() {
        let fixture = OwnerFixture::new(91_008);
        let pause = RootWorkerPause::new(&fixture.host.inner.root_producer_pool);
        let root = fixture.identity(1);
        let descriptor = consistent_descriptor(root, UniqueId::new(91_008, 1));
        let receipt = fixture.create(
            &descriptor,
            &Body::bounded_root(novarocks_result_contract::FrozenRootOutput::CountOnly, &[1]),
        );
        assert_eq!(receipt.outcome(), OperationOutcome::Accepted);
        let channel = fixture
            .host
            .inner
            .task_runtime(root)
            .unwrap()
            .root_channel
            .clone()
            .unwrap();
        let weak = Arc::downgrade(&channel);
        let panicked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let callback_panicked = panicked.clone();
        channel.writable_observable().try_add_observer(Arc::new(move || {
            if weak.upgrade().is_some_and(|channel| channel.producer_state() == novarocks_execution::runtime::fragment::io::RootProducerState::ContextHeld)
                && !callback_panicked.swap(true, Ordering::AcqRel)
            { panic!("injected root exit observer failure"); }
        })).unwrap();
        pause.open();
        wait_bounded_root_finished(&fixture, root);
        assert!(panicked.load(Ordering::Acquire));
        fixture.registry.abort_query_context(
            &novarocks_execution_contract::task_execution::operation::AbortQueryContext::new(
                TaskOperationId::new_v7(),
                fixture.context,
                AbortCause::QueryFailed,
            ),
        );
        fixture.host.inner.root_producer_pool.shutdown().unwrap();
    }

    fn bounded_read(
        root: TaskIdentity,
        kind: novarocks_result_contract::RootOutputKind,
    ) -> novarocks_execution_contract::root_result::RootResultRead {
        novarocks_execution_contract::root_result::RootResultRead::try_new(
            root,
            novarocks_result_contract::RootProfileId::V1,
            kind,
            Some(NonZeroU64::new(1).unwrap()),
            0,
            Duration::from_millis(1),
        )
        .unwrap()
    }

    fn assert_single_row_end_without_ack(
        fixture: &OwnerFixture,
        root: TaskIdentity,
        data: &novarocks_execution_contract::root_result::RootResultData,
        runtime: &tokio::runtime::Runtime,
    ) {
        use novarocks_execution_contract::root_result::{RootReadOutcome, RootResultRead};
        use novarocks_result_contract::RootProfileId;
        use novarocks_worker::root_result_channel::ContextRootRoute;
        assert_eq!(data.sequence().get(), 1);
        if let Some(end) = data.end_after_data() {
            assert_eq!(end.output_rows, 1);
            assert_eq!(end.sequence.get(), 2);
        }
        // A complete small row may precede knowledge of EOS. Published Data
        // is immutable, so a separate End is equally valid. Read it without
        // ACKing Data; FINISHED was already observed before either read.
        let request = RootResultRead::try_new(
            root,
            RootProfileId::V1,
            data.kind(),
            Some(NonZeroU64::new(2).unwrap()),
            0,
            Duration::from_millis(1),
        )
        .unwrap();
        let ContextRootRoute::Read(read) = fixture.registry.context_root_result_route(&request)
        else {
            panic!("finished root must retain its independent End");
        };
        let delivery = runtime.block_on(read.read()).unwrap();
        assert_eq!(delivery.reply().accepted_consumed, 0);
        let RootReadOutcome::End(end) = &delivery.reply().outcome else {
            panic!("finished root must offer End: {:?}", delivery.reply());
        };
        assert_eq!(end.output_rows, 1);
        assert_eq!(end.sequence.get(), 2);
    }

    #[test]
    fn bounded_root_count_finishes_before_read_and_survives_task_retirement() {
        use novarocks_execution_contract::root_result::RootReadOutcome;
        use novarocks_result_contract::{FrozenRootOutput, RootOutputKind};
        use novarocks_worker::root_result_channel::ContextRootRoute;
        let fixture = OwnerFixture::new(91_001);
        let root = fixture.identity(1);
        let descriptor = consistent_descriptor(root, UniqueId::new(91_001, 1));
        let receipt = fixture.create(
            &descriptor,
            &Body::bounded_root(FrozenRootOutput::CountOnly, &[4, 5, 6]),
        );
        assert_eq!(receipt.outcome(), OperationOutcome::Accepted, "{receipt:?}");
        wait_bounded_root_finished(&fixture, root);
        fixture.registry.advance_deadlines();
        assert!(
            fixture.host.inner.task_runtime(root).is_none(),
            "producer task retired"
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let read = bounded_read(root, RootOutputKind::CountOnly);
        let ContextRootRoute::Read(first) = fixture.registry.context_root_result_route(&read)
        else {
            panic!("retired producer lost context output")
        };
        let first = runtime.block_on(first.read()).unwrap();
        assert!(
            matches!(first.reply().outcome, RootReadOutcome::End(end) if end.output_rows == 3 && end.sequence.get() == 1)
        );
        let reply = first.reply().clone();
        drop(first);
        let ContextRootRoute::Read(replay) = fixture.registry.context_root_result_route(&read)
        else {
            panic!("context cannot replay End")
        };
        let replay = runtime.block_on(replay.read()).unwrap();
        assert_eq!(replay.reply(), &reply);
        let quiesced = fixture.registry.quiesce_query_context(
            &novarocks_execution_contract::task_execution::operation::QuiesceQueryContext::new(
                TaskOperationId::new_v7(),
                fixture.context,
            ),
        );
        assert_eq!(quiesced.outcome(), OperationOutcome::Accepted);
        let released = fixture
            .registry
            .release_query_context(&ReleaseQueryContext::new(
                TaskOperationId::new_v7(),
                fixture.context,
            ));
        assert_eq!(
            released.outcome(),
            OperationOutcome::Accepted,
            "no final ACK needed"
        );
        assert_eq!(
            fixture.registry.context_state(fixture.context),
            novarocks_execution_contract::QueryContextState::Releasing,
            "End delivery guard remains a physical holder"
        );
        assert!(matches!(
            fixture.registry.context_root_result_route(&read),
            ContextRootRoute::AwaitTerminalControl {
                accepted_consumed: 0
            }
        ));
        drop(replay);
        fixture.registry.advance_deadlines();
        assert_eq!(
            fixture.registry.context_state(fixture.context),
            novarocks_execution_contract::QueryContextState::TerminalRetained
        );
        fixture.host.inner.root_producer_pool.shutdown().unwrap();
    }

    #[test]
    fn bounded_root_dop_64_values_collapse_counts_actual_rows_once() {
        use novarocks_execution_contract::root_result::RootReadOutcome;
        use novarocks_result_contract::{FrozenRootOutput, RootOutputKind};
        use novarocks_worker::root_result_channel::ContextRootRoute;
        let fixture = OwnerFixture::new_with_context_dop(91_009, 64);
        let root = fixture.identity(1);
        let descriptor = descriptor_with(
            root,
            UniqueId::new(91_009, 1),
            64,
            ExchangeTopology::default(),
        );
        let mut body = Body::bounded_root(FrozenRootOutput::CountOnly, &[1, 2]);
        body.frozen.pipeline_dop_domain = Some(proto::PipelineDopDomain {
            min: 64,
            max: 64,
            requires_power_of_two: false,
        });
        let receipt = fixture.create(&descriptor, &body);
        assert_eq!(receipt.outcome(), OperationOutcome::Accepted, "{receipt:?}");
        wait_bounded_root_finished(&fixture, root);
        let ContextRootRoute::Read(read) = fixture
            .registry
            .context_root_result_route(&bounded_read(root, RootOutputKind::CountOnly))
        else {
            panic!("context lost count")
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let delivery = runtime.block_on(read.read()).unwrap();
        assert!(
            matches!(delivery.reply().outcome, RootReadOutcome::End(end) if end.output_rows == 2)
        );
        drop(delivery);
        fixture.registry.abort_query_context(
            &novarocks_execution_contract::task_execution::operation::AbortQueryContext::new(
                TaskOperationId::new_v7(),
                fixture.context,
                AbortCause::QueryFailed,
            ),
        );
        fixture.host.inner.root_producer_pool.shutdown().unwrap();
    }

    #[test]
    fn bounded_root_client_bytes_finish_without_legacy_arrow_or_ack() {
        use novarocks_execution_contract::root_result::RootReadOutcome;
        use novarocks_result_contract::{
            ClientRenderSchema, FrozenRootOutput, NativeRenderType, RenderColumn, RenderField,
            RenderPresentation, RootOutputKind,
        };
        use novarocks_worker::root_result_channel::ContextRootRoute;
        let schema = ClientRenderSchema::try_new(
            vec![RenderColumn {
                source_ordinal: 0,
                source_slot: Some(10),
                name: "v".into(),
                field: RenderField {
                    nullable: false,
                    native_type: NativeRenderType::SignedInteger(64),
                    presentation: RenderPresentation::ScalarText,
                },
            }],
            1,
        )
        .unwrap();
        let fixture = OwnerFixture::new_with_legacy_root_limit(91_002, 1, 16 * 1024 * 1024);
        assert_eq!(
            fixture.host.inner.result_retained_limits.per_root().get(),
            16 * 1024 * 1024
        );
        let root = fixture.identity(1);
        let descriptor = consistent_descriptor(root, UniqueId::new(91_002, 1));
        let receipt = fixture.create(
            &descriptor,
            &Body::bounded_root(FrozenRootOutput::ClientRows(schema), &[42]),
        );
        assert_eq!(receipt.outcome(), OperationOutcome::Accepted, "{receipt:?}");
        wait_bounded_root_finished(&fixture, root);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let ContextRootRoute::Read(read) = fixture
            .registry
            .context_root_result_route(&bounded_read(root, RootOutputKind::ClientRows))
        else {
            panic!("finished root absent")
        };
        let delivery = runtime.block_on(read.read()).unwrap();
        let RootReadOutcome::Data(data) = &delivery.reply().outcome else {
            panic!("root did not encode client bytes")
        };
        assert_eq!(data.body().as_ref(), &[3, 0, 0, 0, 2, b'4', b'2']);
        assert_single_row_end_without_ack(&fixture, root, data, &runtime);
        assert!(
            matches!(
                runtime.block_on(novarocks_worker::result_buffer::wait_fetch_task_typed(
                    root,
                    None,
                    Duration::ZERO,
                    novarocks_execution_contract::task_execution::operation::ResultByteLimit::new(
                        1024
                    )
                    .unwrap()
                )),
                novarocks_worker::result_buffer::TryFetchTypedResult::Error(_)
            ),
            "bounded root never registered legacy Arrow result"
        );
        drop(delivery);
        fixture.registry.abort_query_context(
            &novarocks_execution_contract::task_execution::operation::AbortQueryContext::new(
                TaskOperationId::new_v7(),
                fixture.context,
                AbortCause::QueryFailed,
            ),
        );
        fixture.host.inner.root_producer_pool.shutdown().unwrap();
    }

    #[test]
    fn bounded_statistics_root_runs_real_wire_decode_driver_and_context_channel() {
        use novarocks_execution_contract::root_result::RootReadOutcome;
        use novarocks_result_contract::{InternalResultDomain, RootOutputKind};
        use novarocks_worker::root_result_channel::ContextRootRoute;
        let fixture = OwnerFixture::new(91_005);
        let root = fixture.identity(1);
        let descriptor = consistent_descriptor(root, UniqueId::new(91_005, 1));
        let receipt = fixture.create(&descriptor, &Body::bounded_statistics());
        assert_eq!(receipt.outcome(), OperationOutcome::Accepted, "{receipt:?}");
        wait_bounded_root_finished(&fixture, root);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let ContextRootRoute::Read(read) =
            fixture.registry.context_root_result_route(&bounded_read(
                root,
                RootOutputKind::InternalFacts(InternalResultDomain::StatisticsArtifactV1),
            ))
        else {
            panic!("finished statistics root absent");
        };
        let delivery = runtime.block_on(read.read()).unwrap();
        let RootReadOutcome::Data(data) = &delivery.reply().outcome else {
            panic!("statistics root did not encode STA1 bytes");
        };
        assert_eq!(
            data.body().as_ref(),
            &[
                83, 84, 65, 49, 36, 0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 7, 0,
                0, 0, 116, 104, 101, 116, 97, 255, 0, 3
            ]
        );
        assert_single_row_end_without_ack(&fixture, root, data, &runtime);
        drop(delivery);
        fixture.registry.abort_query_context(
            &novarocks_execution_contract::task_execution::operation::AbortQueryContext::new(
                TaskOperationId::new_v7(),
                fixture.context,
                AbortCause::QueryFailed,
            ),
        );
        fixture.host.inner.root_producer_pool.shutdown().unwrap();
    }

    #[test]
    fn statistics_host_refuses_an_unprotected_source_before_channel_installation() {
        let (host, _) = isolated_resource_host();
        let root = identity(91_007, 1, 1);
        let descriptor = consistent_descriptor(root, UniqueId::new(91_007, 1));
        let body = Body::bounded_root(
            novarocks_result_contract::FrozenRootOutput::InternalFacts(
                novarocks_result_contract::InternalResultDomain::StatisticsArtifactV1,
            ),
            &[1],
        );
        establish_fixture_context(&host, descriptor.identity());
        let error = host
            .install_receiver(
                &descriptor,
                body.input(&descriptor),
                novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
            )
            .unwrap_err();
        assert_eq!(error.category(), TaskFailureCategory::Protocol);
        assert!(host.task_runtime(root).is_none());
        host.root_producer_pool.shutdown().unwrap();
    }

    #[test]
    fn statistics_host_rejects_a_profile_that_the_generic_unpivot_would_accept() {
        let (host, _) = isolated_resource_host();
        let root = identity(91_006, 1, 1);
        let descriptor = consistent_descriptor(root, UniqueId::new(91_006, 1));
        let mut body = Body::bounded_statistics();
        let node = body.frozen.plan.as_mut().unwrap().root.as_mut().unwrap();
        let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_mut()
        else {
            unreachable!()
        };
        let Some(plan::plan_node::Kind::Unpivot(unpivot)) = physical.kind.as_mut() else {
            unreachable!()
        };
        unpivot.max_output_bytes -= 1;
        let available = host.result_retained_limits.per_process().get()
            - host.root_producer_pool.reserved_bytes();
        let reserve_remaining = |phase| {
            let novarocks_execution::runtime::fragment::io::ResultWriteAdmission::Granted(credit) =
                host.result_retained_budget
                    .try_reserve_process(available)
                    .unwrap()
            else {
                panic!("Statistics capacity is still retained {phase}");
            };
            drop(credit);
        };
        reserve_remaining("before installation");
        establish_fixture_context(&host, descriptor.identity());
        let error = match host.install_receiver(
            &descriptor,
            body.input(&descriptor),
            novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
        ) {
            Err(error) => error,
            Ok(_) => panic!("generic Unpivot must not replace the protected Statistics source"),
        };
        assert!(
            format!("{error:?}")
                .contains("statistics materializer roles or bounds differ from its frozen domain"),
            "unexpected Statistics preparation refusal: {error:?}"
        );
        assert!(host.task_runtime(root).is_none());
        // Prepare rollback activates asynchronous producer cleanup. Joining
        // proves the last worker pin has exited; shutdown keeps the pool's
        // original fixed reservation, so the full-capacity oracle is unchanged.
        host.root_producer_pool.shutdown().unwrap();
        reserve_remaining("after rollback and physical worker exit");
    }

    #[test]
    fn bounded_root_provisional_install_rolls_back_actual_session_metadata() {
        let (host, _) = isolated_resource_host();
        let root = identity(91_003, 1, 1);
        let descriptor = consistent_descriptor(root, UniqueId::new(91_003, 1));
        let body = Body::bounded_root(novarocks_result_contract::FrozenRootOutput::CountOnly, &[1]);
        establish_fixture_context(&host, descriptor.identity());
        let installation = host
            .install_receiver(
                &descriptor,
                body.input(&descriptor),
                novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
            )
            .unwrap();
        let (_, channel) = installation.into_parts();
        let channel = channel.expect("explicit bounded root installs its provisional channel");
        assert_eq!(channel.spec().task, root);
        assert!(
            !channel.physical_idle(),
            "prepared drivers retain covered session metadata"
        );
        host.remove_receiver(&descriptor);
        host.root_producer_pool.shutdown().unwrap();
        assert!(channel.is_closed());
        assert!(
            channel.physical_idle(),
            "task runtime cannot retain a Session after rollback"
        );
    }

    #[test]
    fn bounded_root_untyped_scalar_identity_is_protocol_refusal_before_channel_install() {
        let (host, _) = isolated_resource_host();
        let root = identity(91_004, 1, 1);
        let descriptor = consistent_descriptor(root, UniqueId::new(91_004, 1));
        let body = Body::bounded_root(
            novarocks_result_contract::FrozenRootOutput::InternalFacts(
                novarocks_result_contract::InternalResultDomain::ScalarValueV1,
            ),
            &[],
        );
        establish_fixture_context(&host, descriptor.identity());
        let rejected = host
            .install_receiver(
                &descriptor,
                body.input(&descriptor),
                novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
            )
            .unwrap_err();
        assert_eq!(rejected.category(), TaskFailureCategory::Protocol);
        assert!(
            rejected
                .detail()
                .as_str()
                .contains("root schema does not match its frozen purpose")
        );
        assert!(host.task_runtime(root).is_none());
        host.root_producer_pool.shutdown().unwrap();
    }
    #[test]
    fn bounded_root_installed_scalar_domain_admits_its_typed_producer() {
        let (host, _) = isolated_resource_host();
        let root = identity(91_005, 1, 1);
        let descriptor = consistent_descriptor(root, UniqueId::new(91_005, 1));
        let body = Body::bounded_root(
            novarocks_result_contract::FrozenRootOutput::ScalarValue(
                novarocks_result_contract::ScalarSchema::try_new(
                    novarocks_result_contract::ScalarField {
                        nullable: false,
                        value_type: novarocks_result_contract::ScalarValueType::SignedInteger(64),
                    },
                )
                .unwrap()
                .bind_native_slots(&[10])
                .unwrap(),
            ),
            &[],
        );
        establish_fixture_context(&host, descriptor.identity());
        host.install_receiver(
            &descriptor,
            body.input(&descriptor),
            novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
        )
        .expect("an installed scalar producer admits its typed root");
        assert!(host.task_runtime(root).is_some());
        host.remove_receiver(&descriptor);
        host.root_producer_pool.shutdown().unwrap();
    }
    #[test]
    fn real_owner_result_revocation_waits_for_terminal_control() {
        use crate::task_protocol::TaskExecutionIngress;
        let fixture = OwnerFixture::new(90_503);
        let root = fixture.identity(1);
        let descriptor = consistent_descriptor(root, UniqueId::new(90_503, 1));
        let body = Body::with_sink(
            1,
            plan::DataSink {
                kind: Some(plan::data_sink::Kind::Result(true)),
            },
            Vec::new(),
        );
        let receipt = fixture.create(&descriptor, &body);
        assert_eq!(receipt.outcome(), OperationOutcome::Accepted);
        assert!(
            matches!(
                fixture.registry.root_result_route(root),
                novarocks_worker::RootResultRoute::Serve(_)
            ),
            "the production host installed the exact result before create returned"
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let limit =
            novarocks_execution_contract::task_execution::operation::ResultByteLimit::new(1024)
                .unwrap();
        assert!(
            !matches!(
                runtime.block_on(novarocks_worker::result_buffer::wait_fetch_task_typed(
                    root,
                    None,
                    Duration::ZERO,
                    limit,
                )),
                novarocks_worker::result_buffer::TryFetchTypedResult::Error(_)
            ),
            "production preparation registered the result carrier"
        );
        let abort = fixture.registry.abort_query_context(
            &novarocks_execution_contract::task_execution::operation::AbortQueryContext::new(
                TaskOperationId::new_v7(),
                fixture.context,
                AbortCause::PeerTaskFailed,
            ),
        );
        assert_eq!(abort.outcome(), OperationOutcome::Accepted);
        assert!(matches!(
            fixture.registry.root_result_route(root),
            novarocks_worker::RootResultRoute::AwaitTerminalControl
        ));
        let ingress = crate::task_protocol_ingress::RegistryTaskExecutionIngress::new(
            Arc::clone(&fixture.registry),
            NativeCompatibilityId::new([0x71; 32]),
        );
        let response = runtime
            .block_on(ingress.fetch_task_result(proto::FetchTaskResultRequest {
                root_task: Some(novarocks_task_codec::identity::encode_task_identity(root)),
                max_wait_millis: 1_000,
                acknowledged_packet_sequence: None,
                max_result_bytes: 1024,
            }))
            .unwrap();
        assert_eq!(
            response.status,
            proto::fetch_result_response::Status::AwaitTerminalControl as i32
        );
        assert!(response.message.is_empty() && response.result_arrow_ipc.is_empty());
        assert!(!response.eos);
    }
    /// Hosts composed with the compiled-package interpreter: the static
    /// carrier is a physical package, received, provider-validated and
    /// compiled during preparation.
    pub(crate) mod compiled_package_host {
        mod expression_owner_tests {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/backend_task_execution/execution_host/expression_owner_tests.rs"
            ));
        }
        mod schema_owner_tests {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/backend_task_execution/execution_host/schema_owner_tests.rs"
            ));
        }
        use super::*;
        use std::collections::BTreeMap;

        use crate::backend_task_execution::compiled_package::{
            CompiledPackageCompiler, CompiledPackageError, CompiledPackageInterpreter,
            CompiledTaskOptions, CompiledTaskProgram,
        };
        use crate::compiled_runtime_filter::CompiledRuntimeFilterEndpoints;
        use novarocks_connector_contract::PureProviderProgramCatalog;
        use novarocks_functions::{
            ConstantPolicy, EngineFunctionCatalogBuilder, FunctionId, FunctionKind,
            FunctionOverloadId, InstalledPureKernel, PureEngineFunctionCatalog,
            PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
        };
        use novarocks_physical_plan::{
            FragmentId, FragmentPackageAdmission, FragmentSink, MAX_SCAN_BATCH_BYTES,
            MAX_SCAN_BATCH_ROWS, NodeKind, PipelineDopDomain, PlanLimits, PlanVersionId,
            PropertyProofProjectionLimits, ScanReadBudget, extract_fragment_packages,
        };
        use novarocks_plan_codec::physical_package_v2::encode_fragment_package;
        use novarocks_plan_codec::physical_package_v2::test_support::{
            decode_limits, encode_limits,
        };
        use novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel;
        use novarocks_sql::compiler::{
            DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlCompileControl,
            SqlCompileIntent, SqlCompiler, SqlFinalPlanCompileRequest, SqlPlanningEnvironment,
            SqlSessionContext, SqlStatementInput, author_fragment_package_semantics,
            builtin_sql_function_catalog, noop_constant_evaluator,
        };
        use novarocks_task_codec::creation::decode_task_assignment;
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};

        struct Unbounded;
        impl PureCompileControl for Unbounded {
            fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
                Ok(())
            }
        }

        // Explicit fixture admission; these values are not production defaults.
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

        /// A real sealed RAND-only subset; `SELECT 1` binds no function, but
        /// the compiler refuses an empty pure catalog. Not the Server catalogue.
        fn sealed_rand_subset() -> PureEngineFunctionCatalog {
            let actual =
                novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog()
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

        pub(crate) fn interpreter() -> CompiledPackageInterpreter<std::io::Error> {
            let providers =
                PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Unbounded)
                    .expect("empty provider catalog");
            CompiledPackageInterpreter::new(
                FragmentDecodeResourceModel::try_new(&Unbounded).expect("decode model"),
                decode_limits(),
                Arc::new(sealed_rand_subset()),
                Arc::new(providers),
                constants(),
            )
        }

        pub(crate) fn window_interpreter() -> CompiledPackageInterpreter<std::io::Error> {
            let actual =
                novarocks_functions::builtin::catalogue::build_builtin_engine_function_catalog()
                    .unwrap();
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
                            &Unbounded,
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
            let functions = builder.seal_pure(installed).unwrap();
            let providers =
                PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Unbounded)
                    .unwrap();
            CompiledPackageInterpreter::new(
                FragmentDecodeResourceModel::try_new(&Unbounded).unwrap(),
                decode_limits(),
                Arc::new(functions),
                Arc::new(providers),
                constants(),
            )
        }

        fn compiled_host(facts: Arc<StubContextFacts>) -> NativeTaskExecutionHost {
            host(facts).with_compiled_package_compiler(Arc::new(interpreter()))
        }

        /// The real interpreter, with the package's runtime-filter binding
        /// table replaced, so a host can be handed a program and a table that
        /// disagree.
        struct RenumberedCompiler {
            inner: CompiledPackageInterpreter<std::io::Error>,
            runtime_filters: CompiledRuntimeFilterEndpoints,
        }

        impl CompiledPackageCompiler for RenumberedCompiler {
            fn compile(
                &self,
                package: &[u8],
                options: CompiledTaskOptions,
                creation: &mut novarocks_type_contract::ValueSchemaCreation<
                    '_,
                    crate::backend_task_execution::NativeTypeConstructionError,
                >,
                type_scopes: &crate::backend_task_execution::CompiledTypeScopes<'_, '_>,
                control: &dyn PureCompileControl,
            ) -> Result<CompiledTaskProgram, CompiledPackageError> {
                let mut compiled =
                    self.inner
                        .compile(package, options, creation, type_scopes, control)?;
                compiled.runtime_filters = self.runtime_filters.clone();
                Ok(compiled)
            }
            fn compile_with_funding(
                &self,
                package: &[u8],
                options: CompiledTaskOptions,
                creation: &mut novarocks_type_contract::ValueSchemaCreation<
                    '_,
                    crate::backend_task_execution::NativeTypeConstructionError,
                >,
                type_scopes: &crate::backend_task_execution::CompiledTypeScopes<'_, '_>,
                control: &dyn PureCompileControl,
                type_host: &mut crate::backend_task_execution::TypeMaterializationHost<'_>,
                project_host: &mut crate::backend_task_execution::ProjectMetadataHost<'_>,
            ) -> Result<CompiledTaskProgram, CompiledPackageError> {
                let mut compiled = self.inner.compile_with_funding(
                    package,
                    options,
                    creation,
                    type_scopes,
                    control,
                    type_host,
                    project_host,
                )?;
                compiled.runtime_filters = self.runtime_filters.clone();
                Ok(compiled)
            }
        }

        /// The producer fragment of a real `SELECT 1` in v2 bytes, and the
        /// consumer exchange node it streams to.
        struct CandidateTypeFeeAudit {
            verdict: Result<
                usize,
                novarocks_plan_codec::host_projection_v2::ProjectionFailure<
                    novarocks_plan_codec::physical_type_v2::TypeCodecError,
                    crate::native_type_construction::NativeTypeConstructionError,
                >,
            >,
            original_creator_bytes: usize,
            scoped_creator_bytes: usize,
            remaining_after_exit: i64,
        }

        fn candidate_pool_namespace_audit(
            cap: Option<usize>,
        ) -> (
            Result<
                (
                    novarocks_physical_plan::ConstantPools,
                    novarocks_plan_codec::physical_constant_v2::ConstantNamespaceResourceFacts,
                ),
                novarocks_plan_codec::host_projection_v2::ProjectionFailure<
                    novarocks_plan_codec::physical_constant_v2::PhysicalConstantCodecError,
                    crate::native_type_construction::NativeTypeConstructionError,
                >,
            >,
            Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
            usize,
        ) {
            use crate::native_type_construction::{NativeTypeConstruction, NativeTypeFactory};
            use novarocks_execution::runtime::mem_tracker::MemTracker;
            use novarocks_plan_codec::physical_constant_v2::prepare_constant_namespace_with_factory_in;
            use novarocks_plan_codec::physical_package_v2::prepare_package_wire_in;
            use novarocks_plan_codec::physical_type_v2::decode_package_type_table_with_factory_in;
            use novarocks_type_contract::{CompileCheckpoints, ValueSchemaCreation};
            let (raw, _) = select_one_producer();
            // Preserve the actual complete FE package/request/control graph.
            // A second unused record has its own ID and the same real source
            // Field/FVT; unused records remain subject to the whole namespace.
            let mut authored =
                novarocks_proto_models::physical_package_v2::FragmentPackage::decode(
                    raw.as_slice(),
                )
                .unwrap();
            let mut extra = authored
                .constants
                .first()
                .expect("SELECT 1 uses an admitted constant record")
                .clone();
            extra.id = authored
                .constants
                .iter()
                .map(|record| record.id)
                .max()
                .unwrap()
                .checked_add(1)
                .unwrap();
            authored.constants.push(extra);
            let raw = authored.encode_to_vec();
            let tracker = MemTracker::new_root("candidate cumulative pool namespace");
            let actual =
                NativeTypeConstruction::try_with_control(tracker.clone(), &Unbounded).unwrap();
            let mut work = CompileCheckpoints::try_new(&Unbounded, CompilePhase::Decode).unwrap();
            let model = FragmentDecodeResourceModel::try_new(&Unbounded).unwrap();
            let prepared = prepare_package_wire_in(
                &raw,
                &model,
                decode_limits().wire,
                &mut |_| Ok(()),
                &mut work,
            )
            .unwrap();
            let usage = prepared.projection().usage;
            actual
                .request_opaque_upper_bound(usage.peak_requested_heap_bytes_upper)
                .unwrap();
            let wire = prepared.materialize_in(&mut |_| Ok(()), &mut work).unwrap();
            let mut source = usage
                .input_bytes
                .checked_add(usage.root_inline_bytes)
                .unwrap()
                .checked_add(usage.peak_requested_heap_bytes_upper)
                .unwrap();
            let mut exact = |layout| actual.request_layout(layout);
            let mut opaque = |bytes| actual.request_opaque_upper_bound(bytes);
            let mut creator =
                ValueSchemaCreation::new(actual.storage_lifetime(), &mut exact, &mut opaque);
            let type_factory = NativeTypeFactory::new(&actual);
            let types = decode_package_type_table_with_factory_in(
                &wire,
                source,
                decode_limits().types,
                &mut creator,
                &type_factory,
                &mut work,
            )
            .unwrap();
            source = source
                .checked_add(types.creation_request_bytes_upper_bound().unwrap())
                .unwrap()
                .checked_add(std::mem::size_of_val(&types))
                .unwrap();
            let factory = NativeTypeFactory::new(&actual);
            let mut limits = decode_limits().constants;
            if let Some(cap) = cap {
                limits.max_new_allocation_request_bytes = cap;
            }
            let result = prepare_constant_namespace_with_factory_in(
                &wire.constants,
                &types,
                source,
                types.source_metadata_namespace(),
                decode_limits().constant_policy,
                decode_limits().constant_records,
                limits,
                &decode_limits().verifier,
                &factory,
                &mut |_| Ok(()),
                &mut work,
            )
            .and_then(|prepared| {
                prepared.materialize_with_factory_in(&factory, &mut |_| Ok(()), &mut work)
            });
            let quoted = factory.construction_request_bytes_upper_bound();
            drop(factory);
            drop(types);
            drop(type_factory);
            drop(creator);
            drop(wire);
            drop(work);
            drop(actual);
            (result, tracker, quoted)
        }

        #[test]
        fn candidate_pool_namespace_keeps_one_cumulative_cap_and_source_invoice() {
            use novarocks_plan_codec::host_projection_v2::ProjectionFailure;
            use novarocks_plan_codec::physical_constant_v2::PhysicalConstantCodecError;
            let (result, tracker, quoted) = candidate_pool_namespace_audit(None);
            let (pools, facts) = result.unwrap();
            assert_eq!(facts.record_count, 2);
            assert_eq!(pools.entries().len(), 2);
            for pool in pools.entries().values() {
                assert_eq!(pool.value(0).unwrap().try_i64().unwrap(), Some(1));
                assert!(!pool.value_type().nullable());
            }
            assert_eq!(
                facts.coexisting_source_and_request_bytes_upper_bound,
                facts
                    .source_retained_bytes
                    .checked_add(facts.new_allocation_request_bytes_upper_bound)
                    .unwrap()
            );
            assert_eq!(
                quoted, facts.new_allocation_request_bytes_upper_bound,
                "actual classified requests/scope headers equal the original complete model once"
            );
            let completed = facts.new_allocation_request_bytes_upper_bound;
            drop(pools);
            assert_eq!(tracker.current(), 0);
            let (exact, tracker, _) = candidate_pool_namespace_audit(Some(completed));
            drop(exact.unwrap());
            assert_eq!(tracker.current(), 0);
            let (refused, tracker, _) =
                candidate_pool_namespace_audit(Some(completed.checked_sub(1).unwrap()));
            assert!(matches!(
                refused,
                Err(ProjectionFailure::Codec(
                    PhysicalConstantCodecError::Control(CompileControlError::ResourceExhausted)
                ))
            ));
            assert_eq!(tracker.current(), 0);
        }

        fn compiled_constant_after_unrelated_main(
            scratch_len: usize,
        ) -> (
            novarocks_functions::ConstantPool,
            Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
        ) {
            use crate::backend_task_execution::CompiledTypeScopes;
            use crate::native_type_construction::NativeTypeConstruction;
            use novarocks_execution::runtime::mem_tracker::MemTracker;
            use novarocks_type_contract::ValueSchemaCreation;
            let (bytes, _) = select_one_producer();
            let tracker = MemTracker::new_root("compiled constant late pool");
            let actual =
                NativeTypeConstruction::try_with_control(tracker.clone(), &Unbounded).unwrap();
            actual
                .request_layout(std::alloc::Layout::array::<u64>(scratch_len).unwrap())
                .unwrap();
            let scratch = Vec::<u64>::with_capacity(scratch_len);
            let scopes = CompiledTypeScopes::new(&actual);
            let mut exact = |layout| actual.request_layout(layout);
            let mut opaque = |bytes| actual.request_opaque_upper_bound(bytes);
            let mut creation =
                ValueSchemaCreation::new(actual.storage_lifetime(), &mut exact, &mut opaque);
            let compiled = interpreter()
                .compile(
                    &bytes,
                    CompiledTaskOptions {
                        pipeline_dop: NonZeroUsize::MIN,
                        exchange_wait: Duration::from_millis(1),
                    },
                    &mut creation,
                    &scopes,
                    &Unbounded,
                )
                .unwrap();
            let late = compiled
                .program()
                .expressions()
                .graph()
                .expressions()
                .nodes()
                .iter()
                .find_map(|node| match node.kind() {
                    novarocks_local_program::StaticExprKind::Constant(value) => {
                        Some(value.pool().clone())
                    }
                    _ => None,
                })
                .expect("SELECT 1 retains its actual admitted constant source");
            assert_eq!(late.value(0).unwrap().try_i64().unwrap(), Some(1));
            drop(compiled);
            drop(creation);
            drop(scopes);
            drop(scratch);
            drop(actual);
            (late, tracker)
        }

        #[test]
        fn compiled_constant_pool_outlives_program_without_unrelated_main_or_namespace_map() {
            let (small, small_tracker) = compiled_constant_after_unrelated_main(0);
            let (late, tracker) = compiled_constant_after_unrelated_main(1024);
            assert_eq!(late.value(0).unwrap().try_i64().unwrap(), Some(1));
            assert_eq!(late.field_ref().metadata().len(), 0);
            assert!(tracker.current() > 0);
            assert_eq!(tracker.current(), small_tracker.current());
            let array = late.array_projection();
            let before = tracker.current();
            drop(late);
            assert!(
                tracker.current() < before,
                "array does not retain pool/container scope"
            );
            assert_eq!(array.array().len(), 1);
            drop(array);
            assert_eq!(tracker.current(), 0);
            drop(small);
            assert_eq!(small_tracker.current(), 0);
        }

        fn candidate_type_fee_audit(
            limits: novarocks_plan_codec::physical_type_v2::PackageTypeProjectionLimits,
        ) -> CandidateTypeFeeAudit {
            use crate::native_type_construction::{NativeTypeConstruction, NativeTypeFactory};
            use novarocks_execution::runtime::mem_tracker::MemTracker;
            use novarocks_plan_codec::physical_package_v2::prepare_package_wire_in;
            use novarocks_plan_codec::physical_type_v2::decode_package_type_table_with_factory_in;
            use novarocks_type_contract::{CompileCheckpoints, ValueSchemaCreation};
            use std::cell::Cell;

            let (raw, _) = select_one_producer();
            let tracker = MemTracker::new_root("candidate type fee audit");
            let actual =
                NativeTypeConstruction::try_with_control(tracker.clone(), &Unbounded).unwrap();
            let mut work = CompileCheckpoints::try_new(&Unbounded, CompilePhase::Decode).unwrap();
            let model = FragmentDecodeResourceModel::try_new(&Unbounded).unwrap();
            let prepared = prepare_package_wire_in(
                &raw,
                &model,
                decode_limits().wire,
                &mut |_| Ok(()),
                &mut work,
            )
            .unwrap();
            let usage = prepared.projection().usage;
            // The same wire-author upper and inline input provenance as the real
            // package receiver. It is neither encoded-length floor nor a grant.
            let source = usage
                .input_bytes
                .checked_add(usage.root_inline_bytes)
                .unwrap()
                .checked_add(usage.peak_requested_heap_bytes_upper)
                .unwrap();
            actual
                .request_opaque_upper_bound(usage.peak_requested_heap_bytes_upper)
                .unwrap();
            let wire = prepared.materialize_in(&mut |_| Ok(()), &mut work).unwrap();

            let original = Cell::new(0usize);
            let mut exact = |layout: std::alloc::Layout| {
                actual.request_layout(layout)?;
                original.set(original.get().checked_add(layout.size()).unwrap());
                Ok::<_, crate::native_type_construction::NativeTypeConstructionError>(())
            };
            let mut opaque = |bytes: usize| {
                actual.request_opaque_upper_bound(bytes)?;
                original.set(original.get().checked_add(bytes).unwrap());
                Ok::<_, crate::native_type_construction::NativeTypeConstructionError>(())
            };
            let mut creator =
                ValueSchemaCreation::new(actual.storage_lifetime(), &mut exact, &mut opaque);
            let factory = NativeTypeFactory::new(&actual);
            let verdict = decode_package_type_table_with_factory_in(
                &wire,
                source,
                limits,
                &mut creator,
                &factory,
                &mut work,
            )
            .map(|decoded| {
                decoded
                    .creation_request_bytes_upper_bound()
                    .expect("actual strong author completion receipt")
            });
            let original_creator_bytes = original.get();
            let scoped_creator_bytes = factory.construction_request_bytes_upper_bound();
            drop(creator);
            drop(factory);
            drop(wire);
            drop(work);
            drop(actual);
            CandidateTypeFeeAudit {
                verdict,
                original_creator_bytes,
                scoped_creator_bytes,
                remaining_after_exit: tracker.current(),
            }
        }

        #[test]
        fn candidate_logical_scopes_classified_actual_requests_equal_the_sole_completed_model_once()
        {
            let audit = candidate_type_fee_audit(decode_limits().types);
            let completed = audit.verdict.unwrap();
            assert!(
                audit.original_creator_bytes > 0,
                "physical/source creator remains responsible for its original part"
            );
            assert!(
                audit.scoped_creator_bytes > 0,
                "actual logical scopes include their private reservation holder"
            );
            assert_eq!(
                audit
                    .original_creator_bytes
                    .checked_add(audit.scoped_creator_bytes)
                    .unwrap(),
                completed,
                "no duplicated logical charge, omitted scope Arc, or doubled source invoice"
            );
            assert_eq!(audit.remaining_after_exit, 0);
        }

        #[test]
        fn candidate_logical_scopes_keep_the_original_near_boundary_cumulative_allocation_cap() {
            use novarocks_plan_codec::host_projection_v2::ProjectionFailure;
            use novarocks_plan_codec::physical_type_v2::TypeCodecError;
            let baseline = candidate_type_fee_audit(decode_limits().types);
            let completed = baseline.verdict.unwrap();
            let mut limits = decode_limits().types;
            limits.max_allocation_request_bytes = completed.checked_sub(1).unwrap();
            let refused = candidate_type_fee_audit(limits);
            assert!(matches!(
                refused.verdict,
                Err(ProjectionFailure::Codec(TypeCodecError::Control(
                    CompileControlError::ResourceExhausted
                )))
            ));
            assert_eq!(
                refused.scoped_creator_bytes, baseline.scoped_creator_bytes,
                "all original logical scopes were traversed; the complete-table cap is not reset for each entity"
            );
            assert!(
                refused.original_creator_bytes + refused.scoped_creator_bytes < completed,
                "the final refused request does not reach either actual payer"
            );
            assert_eq!(refused.remaining_after_exit, 0);
            assert_eq!(baseline.remaining_after_exit, 0);
        }

        fn select_sql_producer(sql: &str) -> (Vec<u8>, u32) {
            sql_producer(sql, false)
        }

        fn sql_producer(sql: &str, window: bool) -> (Vec<u8>, u32) {
            sql_fragment(sql, window, false)
        }

        fn sql_fragment(sql: &str, window: bool, receive: bool) -> (Vec<u8>, u32) {
            let control = SqlCompileControl::unbounded();
            let request = SqlFinalPlanCompileRequest::new(
                PlanVersionId::try_new([9; 16]).expect("plan version"),
                SqlStatementInput::sql(sql),
                SqlCompileIntent::Query,
                SqlSessionContext {
                    sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
                    current_catalog: Some("iceberg".to_string()),
                    current_database: "db".to_string(),
                    optimizer_settings: SessionOptimizerSettings::default(),
                },
                SqlPlanningEnvironment::Distributed,
                builtin_sql_function_catalog().snapshot(),
                noop_constant_evaluator(),
                constants(),
                novarocks_sql::compiler::SqlPhysicalEmissionMode::ExactComputedWithOriginalDeclaration,
                SqlCompileControl::unbounded(),
                PipelineDopDomain {
                    min: 1,
                    max: 8,
                    requires_power_of_two: true,
                },
                ScanReadBudget {
                    max_batch_rows: MAX_SCAN_BATCH_ROWS,
                    max_batch_bytes: MAX_SCAN_BATCH_BYTES,
                },
                DEFAULT_COMPLETION_LIMITS,
            )
            .try_into_completion()
            .expect("completion request");
            let owner = SqlCompiler::start(request, &control)
                .expect("table-free statement")
                .into_complete()
                .expect("no observation need")
                .into_parts()
                .0;
            let semantics = author_fragment_package_semantics(&owner, constants(), &control)
                .expect("package semantics");
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
            let plan = owner.plan();
            let mut packages = extract_fragment_packages(
                plan,
                &BTreeMap::new(),
                &BTreeMap::new(),
                &uses,
                &calls,
                &pruning,
                &admissions,
                &control,
            )
            .expect("checked packages");
            let producer = plan
                .fragments()
                .iter()
                .find(|(_, fragment)| {
                    if receive {
                        fragment
                            .nodes()
                            .values()
                            .any(|node| matches!(node.kind, NodeKind::ExchangeSource { .. }))
                    } else if window {
                        fragment
                            .nodes()
                            .values()
                            .any(|node| matches!(node.kind, NodeKind::Window(_)))
                    } else {
                        matches!(fragment.sink(), FragmentSink::Stream { .. })
                    }
                })
                .map(|(id, _)| *id)
                .expect("the requested actual SQL fragment");
            let receiver = plan
                .fragments()
                .values()
                .flat_map(|fragment| fragment.nodes().values())
                .find(|node| matches!(node.kind, NodeKind::ExchangeSource { .. }))
                .map(|node| node.id.get())
                .expect("a gather receiver");
            let package: novarocks_physical_plan::FragmentPackage =
                packages.remove(&producer).expect("producer package");
            let _: FragmentId = producer;
            let bytes = encode_fragment_package(&package, &encode_limits(), &control)
                .expect("v2 bytes")
                .encode_to_vec();
            (bytes, receiver)
        }

        pub(crate) fn window_producer() -> Vec<u8> {
            sql_producer("SELECT row_number() OVER () AS r", true).0
        }

        pub(crate) fn select_one_producer() -> (Vec<u8>, u32) {
            select_sql_producer("SELECT 1")
        }

        fn package_carrier(package: &[u8]) -> FrozenBytes {
            FrozenBytes::freeze(
                proto::FrozenFragment {
                    package: bytes::Bytes::copy_from_slice(package),
                    ..Default::default()
                }
                .encode_to_vec()
                .into(),
            )
        }

        /// A single-driver producer task streaming to `receiver` over edge 3,
        /// whose `split_nodes` take their work as runtime splits.
        fn producer_descriptor(
            identity: TaskIdentity,
            receiver: u32,
            split_nodes: &[i32],
        ) -> TaskDescriptor {
            let node = FragmentNodeId::new(i32::try_from(receiver).expect("small node"));
            let destination = ExchangeDestination::new(
                super::identity(70, 2, 1),
                UniqueId::new(900, 901),
                RuntimeEndpoint::new("127.0.0.1", 9060).expect("a legal endpoint"),
                node,
            );
            let topology = ExchangeTopology::try_new(
                vec![
                    ExchangeEdge::try_new(
                        ExchangeEdgeId::new(3).expect("nonzero edge"),
                        node,
                        DataStreamPartitionType::Unpartitioned,
                        vec![destination],
                        0,
                        NonZeroU32::new(1).expect("nonzero"),
                    )
                    .expect("a legal edge"),
                ],
                Vec::new(),
            )
            .expect("a legal topology");
            TaskDescriptor::try_new(
                identity,
                UniqueId::new(701, 702),
                NonZeroUsize::new(1).expect("nonzero dop"),
                split_nodes
                    .iter()
                    .map(|node| PlanNodeId::new(*node).expect("nonnegative node"))
                    .collect(),
                topology,
            )
            .expect("a legal descriptor")
        }

        fn producer_input(descriptor: &TaskDescriptor, carrier: FrozenBytes) -> TaskCreationInput {
            scan_input(descriptor, carrier, Vec::new())
        }

        /// The producer's assignment, with `initial_scan_ranges` frozen in.
        fn scan_input(
            descriptor: &TaskDescriptor,
            carrier: FrozenBytes,
            initial_scan_ranges: Vec<proto::TaskScanRanges>,
        ) -> TaskCreationInput {
            let assignment = decode_task_assignment(
                proto::TaskAssignment {
                    instance_ordinal: 0,
                    initial_scan_ranges,
                    sink_edge_ids: vec![3],
                },
                descriptor,
                FieldPath::root("assignment"),
            )
            .expect("a structurally legal assignment");
            TaskCreationInput::new(carrier, Box::new(assignment))
        }

        // Normative regression: a normal stop does not revoke a sender's
        // already-authorized DATA path. The old empty-EOS coverage remains.
        #[test]
        fn compiled_receiver_nonempty_data_remains_authorized_after_normal_quiesce() {
            let (package, receiver) = sql_fragment("SELECT 1", false, true);
            for (ordinal, stop) in [ReceiverStop::Success, ReceiverStop::Quiesce]
                .into_iter()
                .enumerate()
            {
                let host = compiled_host(Arc::new(StubContextFacts::default()));
                let query = 83_300 + ordinal as i64;
                let descriptor = TaskDescriptor::try_new(
                    identity(query, 1, 1),
                    UniqueId::new(query, 1),
                    NonZeroUsize::new(1).unwrap(),
                    vec![],
                    inbound_topology(
                        FragmentNodeId::new(receiver as i32),
                        vec![ExchangeSource::new(
                            identity(query, 2, 1),
                            UniqueId::new(query, 2),
                            0,
                        )],
                    ),
                )
                .unwrap();
                let assignment = decode_task_assignment(
                    proto::TaskAssignment {
                        instance_ordinal: 0,
                        initial_scan_ranges: vec![],
                        sink_edge_ids: vec![],
                    },
                    &descriptor,
                    FieldPath::root("assignment"),
                )
                .unwrap();
                let input = TaskCreationInput::new(package_carrier(&package), Box::new(assignment));
                receiver_retirement_case(host, descriptor, input, stop, true);
            }
        }

        #[test]
        fn compiled_receiver_nonempty_data_stops_on_true_abort() {
            let (package, receiver) = sql_fragment("SELECT 1", false, true);
            for (ordinal, stop) in [ReceiverStop::Abort, ReceiverStop::QuiesceThenAbort]
                .into_iter()
                .enumerate()
            {
                let host = compiled_host(Arc::new(StubContextFacts::default()));
                let query = 83_600 + ordinal as i64;
                let descriptor = TaskDescriptor::try_new(
                    identity(query, 1, 1),
                    UniqueId::new(query, 1),
                    NonZeroUsize::new(1).unwrap(),
                    vec![],
                    inbound_topology(
                        FragmentNodeId::new(receiver as i32),
                        vec![ExchangeSource::new(
                            identity(query, 2, 1),
                            UniqueId::new(query, 2),
                            0,
                        )],
                    ),
                )
                .unwrap();
                let assignment = decode_task_assignment(
                    proto::TaskAssignment {
                        instance_ordinal: 0,
                        initial_scan_ranges: vec![],
                        sink_edge_ids: vec![],
                    },
                    &descriptor,
                    FieldPath::root("assignment"),
                )
                .unwrap();
                let input = TaskCreationInput::new(package_carrier(&package), Box::new(assignment));
                receiver_retirement_case(host, descriptor, input, stop, true);
            }
        }

        #[test]
        fn compiled_receiver_retirement_waits_for_the_original_inbound_gate() {
            for (ordinal, stop) in [
                ReceiverStop::Success,
                ReceiverStop::Quiesce,
                ReceiverStop::BeforeStart,
                ReceiverStop::Abort,
                ReceiverStop::Rollback,
            ]
            .into_iter()
            .enumerate()
            {
                let (package, receiver) = sql_fragment("SELECT 1", false, true);
                let facts = Arc::new(StubContextFacts::default());
                let host = compiled_host(facts);
                let query = 83_200 + ordinal as i64;
                let descriptor = TaskDescriptor::try_new(
                    identity(query, 1, 1),
                    UniqueId::new(query, 1),
                    NonZeroUsize::new(1).unwrap(),
                    vec![],
                    inbound_topology(
                        FragmentNodeId::new(receiver as i32),
                        vec![ExchangeSource::new(
                            identity(query, 2, 1),
                            UniqueId::new(query, 2),
                            0,
                        )],
                    ),
                )
                .unwrap();
                let assignment = decode_task_assignment(
                    proto::TaskAssignment {
                        instance_ordinal: 0,
                        initial_scan_ranges: vec![],
                        sink_edge_ids: vec![],
                    },
                    &descriptor,
                    FieldPath::root("assignment"),
                )
                .unwrap();
                let input = TaskCreationInput::new(package_carrier(&package), Box::new(assignment));
                receiver_retirement_case(host, descriptor, input, stop, false);
            }
        }

        // The FE's own producer package is received, compiled and prepared
        // by the compiled host into an ordinary task runtime.
        #[test]
        fn a_compiled_host_installs_the_fes_own_stream_producer_package() {
            let (package, receiver) = select_one_producer();
            let facts = Arc::new(StubContextFacts::default());
            let host = compiled_host(Arc::clone(&facts));
            let identity = super::identity(70, 1, 1);
            let descriptor = producer_descriptor(identity, receiver, &[]);
            establish_fixture_context(&host, descriptor.identity());
            let prepared = host
                .install_receiver(
                    &descriptor,
                    producer_input(&descriptor, package_carrier(&package)),
                    novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
                )
                .expect("the compiled producer installs");
            assert_eq!(prepared.facts().sink_kind(), FragmentSinkKind::DataStream);
            assert!(host.task_runtime(identity).is_some());
            // A program with no runtime-filter site asks for the session of
            // exactly no binding, which is the plan-tree task's
            // `expects_bindings = false`; it never asks the plan-tree way.
            assert_eq!(
                *facts
                    .compiled_filter_bindings
                    .lock()
                    .expect("stub compiled filter bindings"),
                vec![BTreeSet::new()]
            );
            assert_eq!(facts.filter_sessions_requested.load(Ordering::SeqCst), 0);
            host.remove_receiver(&descriptor);
            assert!(host.task_runtime(identity).is_none());
        }

        // A package that numbers a runtime-filter binding its compiled
        // program has no site for would bind a session that disagrees with
        // what the frontend deployed for the fragment. The task is refused
        // as content before any session is requested or anything prepared.
        #[test]
        fn a_compiled_task_whose_program_drops_a_package_binding_is_refused() {
            use crate::compiled_runtime_filter::{
                CompiledRuntimeFilterEndpoint, CompiledRuntimeFilterRole,
            };

            let (package, receiver) = select_one_producer();
            let facts = Arc::new(StubContextFacts::default());
            let host = host(Arc::clone(&facts)).with_compiled_package_compiler(Arc::new(
                RenumberedCompiler {
                    inner: interpreter(),
                    runtime_filters: CompiledRuntimeFilterEndpoints::try_from_endpoints([(
                        3,
                        CompiledRuntimeFilterEndpoint::new(
                            7,
                            receiver,
                            CompiledRuntimeFilterRole::Producer,
                        ),
                    )])
                    .expect("one binding"),
                },
            ));
            let identity = super::identity(73, 1, 1);
            let descriptor = producer_descriptor(identity, receiver, &[]);
            establish_fixture_context(&host, descriptor.identity());
            let refused = host
                .install_receiver(
                    &descriptor,
                    producer_input(&descriptor, package_carrier(&package)),
                    novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
                )
                .expect_err("binding 3 has no site in the program");
            assert_eq!(refused.category(), TaskFailureCategory::Protocol);
            assert!(
                refused.detail().as_str().contains(
                    "runtime-filter bindings do not match its package: package runtime-filter \
                     binding_id=3 has no requirement in its compiled program"
                ),
                "{refused}"
            );
            assert!(
                facts
                    .compiled_filter_bindings
                    .lock()
                    .expect("stub compiled filter bindings")
                    .is_empty(),
                "no session is requested for a refused program"
            );
            assert!(host.task_runtime(identity).is_none());
        }

        // Exactly one interpreter per process: neither host reads the other
        // interpreter's carrier, and a package it cannot compile is refused
        // as content, before anything is prepared.
        #[test]
        fn each_host_refuses_the_other_interpreters_carrier() {
            let (package, receiver) = select_one_producer();
            let compiled = compiled_host(Arc::new(StubContextFacts::default()));
            let identity = super::identity(71, 1, 1);
            let descriptor = producer_descriptor(identity, receiver, &[]);
            establish_fixture_context(&compiled, descriptor.identity());
            let plan_tree = compiled
                .install_receiver(
                    &descriptor,
                    producer_input(&descriptor, Body::values(1).frozen_bytes()),
                    novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
                )
                .expect_err("a compiled host reads no plan tree");
            assert_eq!(plan_tree.category(), TaskFailureCategory::Protocol);
            assert!(
                plan_tree
                    .detail()
                    .as_str()
                    .contains("static package is not decodable"),
                "{plan_tree}"
            );

            let legacy = host(Arc::new(StubContextFacts::default()));
            establish_fixture_context(&legacy, descriptor.identity());
            let refused = legacy
                .install_receiver(
                    &descriptor,
                    producer_input(&descriptor, package_carrier(&package)),
                    novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
                )
                .expect_err("a plan-tree host reads no package");
            assert_eq!(refused.category(), TaskFailureCategory::Protocol);

            establish_fixture_context(&compiled, descriptor.identity());

            let garbage = compiled
                .install_receiver(
                    &descriptor,
                    producer_input(&descriptor, package_carrier(b"not a package")),
                    novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
                )
                .expect_err("an unreceivable package");
            assert_eq!(garbage.category(), TaskFailureCategory::Protocol);
            assert!(
                garbage
                    .detail()
                    .as_str()
                    .contains("package could not be compiled"),
                "{garbage}"
            );
            assert!(compiled.task_runtime(identity).is_none());
        }

        #[test]
        fn compiled_interpreter_scoped_logical_leaf_outlives_program_without_parent_charge() {
            use crate::backend_task_execution::CompiledTypeScopes;
            use crate::native_type_construction::{NativeTypeConstruction, NativeTypeScope};
            use novarocks_execution::runtime::mem_tracker::MemTracker;
            use novarocks_type_contract::{ValueKind, ValueSchemaCreation, ValueTypeRef};
            let (bytes, _) = select_one_producer();
            let tracker = MemTracker::new_root("compiled logical late leaf");
            let actual =
                NativeTypeConstruction::try_with_control(tracker.clone(), &Unbounded).unwrap();
            let scopes = CompiledTypeScopes::new(&actual);
            let mut exact = |layout| actual.request_layout(layout);
            let mut opaque = |bytes| actual.request_opaque_upper_bound(bytes);
            let mut creation =
                ValueSchemaCreation::new(actual.storage_lifetime(), &mut exact, &mut opaque);
            let compiled = interpreter()
                .compile(
                    &bytes,
                    CompiledTaskOptions {
                        pipeline_dop: NonZeroUsize::MIN,
                        exchange_wait: Duration::from_millis(1),
                    },
                    &mut creation,
                    &scopes,
                    &Unbounded,
                )
                .unwrap();
            let late = compiled
                .program()
                .graph()
                .nodes()
                .iter()
                .find_map(|node| node.output_layout().value_binding_at(0))
                .expect("SELECT 1 has a frozen output binding")
                .value_type()
                .clone();
            assert!(matches!(
                late.kind(),
                ValueKind::Scalar(arrow::datatypes::DataType::Int64)
            ));
            drop(compiled);
            drop(creation);
            drop(scopes);
            drop(actual);
            let own = NativeTypeScope::reservation_allocation_layout()
                .unwrap()
                .size()
                + ValueTypeRef::storage_allocation_layout().unwrap().size();
            assert_eq!(
                tracker.current(),
                i64::try_from(own).unwrap(),
                "logical child does not retain package/prost/physical/compiler scratch"
            );
            drop(late);
            assert_eq!(tracker.current(), 0);
        }

        fn complete_binding_after_unrelated_main_backing(
            scratch_len: usize,
        ) -> (
            novarocks_type_contract::TypedValueBinding,
            Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
        ) {
            use crate::backend_task_execution::CompiledTypeScopes;
            use crate::native_type_construction::NativeTypeConstruction;
            use novarocks_execution::runtime::mem_tracker::MemTracker;
            use novarocks_type_contract::ValueSchemaCreation;
            let (bytes, _) = select_one_producer();
            let tracker = MemTracker::new_root("compiled complete binding late reference");
            let actual =
                NativeTypeConstruction::try_with_control(tracker.clone(), &Unbounded).unwrap();
            actual
                .request_layout(std::alloc::Layout::array::<u64>(scratch_len).unwrap())
                .unwrap();
            let scratch = Vec::<u64>::with_capacity(scratch_len);
            let scopes = CompiledTypeScopes::new(&actual);
            let mut exact = |layout| actual.request_layout(layout);
            let mut opaque = |bytes| actual.request_opaque_upper_bound(bytes);
            let mut creation =
                ValueSchemaCreation::new(actual.storage_lifetime(), &mut exact, &mut opaque);
            let compiled = interpreter()
                .compile(
                    &bytes,
                    CompiledTaskOptions {
                        pipeline_dop: NonZeroUsize::MIN,
                        exchange_wait: Duration::from_millis(1),
                    },
                    &mut creation,
                    &scopes,
                    &Unbounded,
                )
                .unwrap();
            // Preserve both semantic and physical owners, unlike the previous
            // logical-only child probe. This is the real compiled caller.
            let late = compiled
                .program()
                .graph()
                .nodes()
                .iter()
                .find_map(|node| node.output_layout().value_binding_at(0))
                .expect("SELECT 1 has a complete frozen binding")
                .clone();
            drop(compiled);
            drop(creation);
            drop(scopes);
            drop(scratch);
            drop(actual);
            (late, tracker)
        }

        fn candidate_nested_type_child_after_unrelated_backing(
            scratch_len: usize,
        ) -> (
            novarocks_type_contract::TypedValueBinding,
            Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
        ) {
            use crate::native_type_construction::{
                NativeTypeConstruction, NativeTypeConstructionError, NativeTypeFactory,
            };
            use novarocks_execution::runtime::mem_tracker::MemTracker;
            use novarocks_plan_codec::physical_package_v2::prepare_package_wire_in;
            use novarocks_plan_codec::physical_type_v2::{
                BorrowedFieldTypeRoot, TypeProjectionLimits,
                decode_package_type_table_with_factory_in, decode_type_table_pure,
                encode_borrowed_type_table_associated_sources_pure_in,
            };
            use novarocks_type_contract::{
                CompileCheckpoints, FunctionValueType, ValueSchemaCreation,
            };
            // Keep the actual SELECT 1 graph, controls, requests and sources.
            // A legal unused type is admitted by the real type owner; this
            // proves no registration or execution capability for SQL ARRAY.
            let (bytes, _) = select_one_producer();
            let mut authored =
                novarocks_proto_models::physical_package_v2::FragmentPackage::decode(
                    bytes.as_slice(),
                )
                .unwrap();
            let old = authored.types.as_ref().unwrap();
            let roots = old.value_types.len().checked_add(old.fields.len()).unwrap();
            let limits = TypeProjectionLimits::for_validated_source_root_count(roots).unwrap();
            let original = decode_type_table_pure(old, limits, &Unbounded).unwrap();
            let fresh = old
                .value_types
                .iter()
                .map(|v| v.id)
                .max()
                .unwrap()
                .checked_add(1)
                .unwrap();
            let nested = FunctionValueType::new(
                arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "item",
                    arrow::datatypes::DataType::Int64,
                    false,
                ))),
                false,
            );
            let mut values = original.value_types().collect::<Vec<_>>();
            values.push((fresh, &nested));
            let fields = old
                .fields
                .iter()
                .map(|field| BorrowedFieldTypeRoot {
                    id: field.id,
                    field: original.field(field.id).unwrap(),
                    value_type_id: field.value_binding_id,
                })
                .collect::<Vec<_>>();
            let mut encode = CompileCheckpoints::try_new(&Unbounded, CompilePhase::Encode).unwrap();
            let encoded = encode_borrowed_type_table_associated_sources_pure_in(
                &values,
                &fields,
                TypeProjectionLimits::for_validated_source_root_count(
                    roots.checked_add(1).unwrap(),
                )
                .unwrap(),
                &mut encode,
            )
            .unwrap()
            .into_wire();
            encode.finish().unwrap();
            authored.types = Some(encoded);
            let bytes = authored.encode_to_vec();
            let tracker = MemTracker::new_root("candidate complete nested type child");
            let actual =
                NativeTypeConstruction::try_with_control(tracker.clone(), &Unbounded).unwrap();
            actual
                .request_layout(std::alloc::Layout::array::<u64>(scratch_len).unwrap())
                .unwrap();
            let scratch = Vec::<u64>::with_capacity(scratch_len);
            let mut work = CompileCheckpoints::try_new(&Unbounded, CompilePhase::Decode).unwrap();
            let model = FragmentDecodeResourceModel::try_new(&Unbounded).unwrap();
            let prepared = prepare_package_wire_in(
                &bytes,
                &model,
                decode_limits().wire,
                &mut |_| Ok(()),
                &mut work,
            )
            .unwrap();
            let usage = prepared.projection().usage;
            let source = usage
                .input_bytes
                .checked_add(usage.root_inline_bytes)
                .unwrap()
                .checked_add(usage.peak_requested_heap_bytes_upper)
                .unwrap();
            actual
                .request_opaque_upper_bound(usage.peak_requested_heap_bytes_upper)
                .unwrap();
            let wire = prepared.materialize_in(&mut |_| Ok(()), &mut work).unwrap();
            let factory = NativeTypeFactory::new(&actual);
            let decoded = {
                let mut exact = |layout| actual.request_layout(layout);
                let mut opaque = |bytes| actual.request_opaque_upper_bound(bytes);
                let mut creation =
                    ValueSchemaCreation::new(actual.storage_lifetime(), &mut exact, &mut opaque);
                decode_package_type_table_with_factory_in(
                    &wire,
                    source,
                    decode_limits().types,
                    &mut creation,
                    &factory,
                    &mut work,
                )
                .unwrap()
            };
            let root = decoded
                .value_type(fresh)
                .expect("sole author published the complete unused List declaration");
            assert_eq!(root, &nested);
            let child = root
                .typed_binding()
                .child_at_observed(
                    0,
                    actual.storage_lifetime(),
                    |_| panic!("actual cached child must not allocate a projection"),
                    |units| {
                        for _ in 0..units {
                            work.step().map_err(NativeTypeConstructionError::Control)?;
                        }
                        Ok::<_, NativeTypeConstructionError>(())
                    },
                )
                .unwrap()
                .unwrap();
            drop(decoded);
            drop(wire);
            work.finish().unwrap();
            drop(factory);
            drop(scratch);
            drop(actual);
            (child, tracker)
        }

        #[test]
        fn candidate_type_owner_late_nested_child_releases_parent_layout_and_unrelated_main() {
            let (minimal, minimal_tracker) = candidate_nested_type_child_after_unrelated_backing(0);
            let (late, tracker) = candidate_nested_type_child_after_unrelated_backing(512);
            assert_eq!(late, minimal);
            assert_eq!(
                late.layout().data_type(),
                &arrow::datatypes::DataType::Int64
            );
            assert!(tracker.current() > 0);
            assert_eq!(
                tracker.current(),
                minimal_tracker.current(),
                "the child cannot pin parent layout/name/pointer backing or package scratch"
            );
            let own = 2
                * crate::native_type_construction::NativeTypeScope::reservation_allocation_layout()
                    .unwrap()
                    .size()
                + novarocks_type_contract::ValueTypeRef::storage_allocation_layout()
                    .unwrap()
                    .size()
                + novarocks_type_contract::ValueLayout::storage_allocation_layout()
                    .unwrap()
                    .size();
            assert_eq!(tracker.current(), i64::try_from(own).unwrap());
            drop(late);
            assert_eq!(tracker.current(), 0);
            drop(minimal);
            assert_eq!(minimal_tracker.current(), 0);
        }

        #[test]
        fn compiled_interpreter_complete_binding_does_not_pin_unrelated_main_backing() {
            let (minimal, minimal_tracker) = complete_binding_after_unrelated_main_backing(0);
            let (late, tracker) = complete_binding_after_unrelated_main_backing(512);
            assert_eq!(late, minimal);
            assert_eq!(
                late.layout().data_type(),
                &arrow::datatypes::DataType::Int64
            );
            assert!(!late.value_type().nullable());
            assert!(
                tracker.current() > 0,
                "the complete semantic and physical holders are live"
            );
            assert_eq!(
                tracker.current(),
                minimal_tracker.current(),
                "the unrelated actual Vec and main package/compiler allowance have exited while the complete binding remains live"
            );
            drop(late);
            assert_eq!(
                tracker.current(),
                0,
                "last full binding releases its real finite responsibility"
            );
            drop(minimal);
            assert_eq!(minimal_tracker.current(), 0);
        }

        #[test]
        fn compiled_interpreter_scoped_decode_keeps_original_whole_type_caps() {
            use crate::backend_task_execution::CompiledTypeScopes;
            use crate::native_type_construction::NativeTypeConstruction;
            use novarocks_execution::runtime::mem_tracker::MemTracker;
            use novarocks_type_contract::ValueSchemaCreation;
            let (bytes, _) = select_one_producer();
            let tracker = MemTracker::new_root("compiled scoped global caps");
            let actual =
                NativeTypeConstruction::try_with_control(tracker.clone(), &Unbounded).unwrap();
            let mut limits = decode_limits();
            limits.types.max_definitions = 0;
            let providers =
                PureProviderProgramCatalog::<std::io::Error>::try_new(&[], vec![], &Unbounded)
                    .unwrap();
            let compiler = CompiledPackageInterpreter::new(
                FragmentDecodeResourceModel::try_new(&Unbounded).unwrap(),
                limits,
                Arc::new(sealed_rand_subset()),
                Arc::new(providers),
                constants(),
            );
            let scopes = CompiledTypeScopes::new(&actual);
            let mut exact = |layout| actual.request_layout(layout);
            let mut opaque = |bytes| actual.request_opaque_upper_bound(bytes);
            let mut creation =
                ValueSchemaCreation::new(actual.storage_lifetime(), &mut exact, &mut opaque);
            let result = compiler.compile(
                &bytes,
                CompiledTaskOptions {
                    pipeline_dop: NonZeroUsize::MIN,
                    exchange_wait: Duration::from_millis(1),
                },
                &mut creation,
                &scopes,
                &Unbounded,
            );
            assert!(
                matches!(
                    result,
                    Err(CompiledPackageError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                ),
                "entity scopes cannot replace the original complete-table cap"
            );
            drop(creation);
            drop(scopes);
            drop(actual);
            assert_eq!(tracker.current(), 0);
        }

        #[test]
        fn compiled_interpreter_first_actual_tracker_cause_has_no_control_tail() {
            use crate::backend_task_execution::CompiledTypeScopes;
            use crate::native_type_construction::{
                NativeTypeConstruction, NativeTypeConstructionError,
            };
            use novarocks_execution::runtime::mem_tracker::MemTracker;
            use novarocks_type_contract::ValueSchemaCreation;
            use std::sync::atomic::{AtomicUsize, Ordering};
            struct Trace(AtomicUsize);
            impl PureCompileControl for Trace {
                fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
                    self.0.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }
            let (bytes, _) = select_one_producer();
            let tracker = MemTracker::new_root("compiled first typed refusal");
            let control = Trace(AtomicUsize::new(0));
            let actual =
                NativeTypeConstruction::try_with_control(tracker.clone(), &control).unwrap();
            let prior = tracker.current();
            tracker.install_limit_once(prior).unwrap();
            let scopes = CompiledTypeScopes::new(&actual);
            let refused_at = std::cell::Cell::new(None);
            let mut exact = |layout| {
                let result = actual.request_layout(layout);
                if result.is_err() {
                    refused_at.set(Some(control.0.load(Ordering::SeqCst)));
                }
                result
            };
            let mut opaque = |bytes| {
                let result = actual.request_opaque_upper_bound(bytes);
                if result.is_err() {
                    refused_at.set(Some(control.0.load(Ordering::SeqCst)));
                }
                result
            };
            let mut creation =
                ValueSchemaCreation::new(actual.storage_lifetime(), &mut exact, &mut opaque);
            let result = interpreter().compile(
                &bytes,
                CompiledTaskOptions {
                    pipeline_dop: NonZeroUsize::MIN,
                    exchange_wait: Duration::from_millis(1),
                },
                &mut creation,
                &scopes,
                &control,
            );
            let Err(CompiledPackageError::Construction(NativeTypeConstructionError::Capacity(
                original,
            ))) = result
            else {
                panic!("original typed tracker cause was replaced");
            };
            assert!(original.contains("compiled first typed refusal"));
            assert_eq!(
                Some(control.0.load(Ordering::SeqCst)),
                refused_at.get(),
                "no control footer after first H"
            );
            assert_eq!(
                tracker.current(),
                prior,
                "failed prospective request is rolled back"
            );
            drop(creation);
            drop(scopes);
            drop(actual);
            assert_eq!(tracker.current(), 0);
        }

        /// A query context that leases the fixture catalog with its installed
        /// read execution; every other fact is the stub's.
        #[derive(Default)]
        struct ScanContextFacts(StubContextFacts);

        impl TaskQueryContextFacts for ScanContextFacts {
            fn query_options(
                &self,
                execution: QueryExecutionId,
            ) -> Result<QueryContextOptions, HostRejection> {
                self.0.query_options(execution)
            }

            fn runtime_filter_session(
                &self,
                execution: QueryExecutionId,
                fragment_instance_id: UniqueId,
                expects_bindings: bool,
            ) -> Result<Option<RuntimeFilterSessionRef>, HostRejection> {
                self.0
                    .runtime_filter_session(execution, fragment_instance_id, expects_bindings)
            }

            fn runtime_filter_session_for_bindings(
                &self,
                execution: QueryExecutionId,
                fragment_instance_id: UniqueId,
                bindings: &BTreeSet<u32>,
            ) -> Result<Option<RuntimeFilterSessionRef>, HostRejection> {
                self.0.runtime_filter_session_for_bindings(
                    execution,
                    fragment_instance_id,
                    bindings,
                )
            }

            fn runtime_filter_event_sink(
                &self,
                execution: QueryExecutionId,
                fragment_instance_id: UniqueId,
            ) -> Arc<dyn FragmentEventSink> {
                self.0
                    .runtime_filter_event_sink(execution, fragment_instance_id)
            }

            fn bind_runtime_filter_feedback(
                &self,
                execution: QueryExecutionId,
                carrier: TaskIdentity,
                reporter: &TaskStatusReporter,
            ) -> bool {
                self.0
                    .bind_runtime_filter_feedback(execution, carrier, reporter)
            }

            fn deliver_task_dynamic_filter(
                &self,
                execution: QueryExecutionId,
                fragment_instance_id: UniqueId,
                version: DomainVersion,
                payload: &Arc<dyn CodecOwnedContent>,
            ) -> Result<(), HostRejection> {
                self.0.deliver_task_dynamic_filter(
                    execution,
                    fragment_instance_id,
                    version,
                    payload,
                )
            }

            fn catalog_read_execution(
                &self,
                _execution: QueryExecutionId,
                handle: &CatalogHandle,
            ) -> Result<ConnectorExecutionReadBinding, String> {
                let installed =
                    crate::typed_connector_test_support::test_support::installed_read_execution();
                if installed.binding().catalog_handle() == handle {
                    Ok(installed)
                } else {
                    Err("this fixture leases only the fixture catalog".to_owned())
                }
            }

            fn catalog_write_execution(
                &self,
                execution: QueryExecutionId,
                handle: &CatalogHandle,
            ) -> Result<ConnectorExecutionWriteBinding, String> {
                self.0.catalog_write_execution(execution, handle)
            }

            fn storage_resolver(
                &self,
                execution: QueryExecutionId,
            ) -> Result<Arc<dyn ConnectorStorageResolver>, HostRejection> {
                self.0.storage_resolver(execution)
            }
        }

        /// A compiled host whose provider catalog seals fixture reads.
        fn scan_host(facts: Arc<ScanContextFacts>) -> NativeTaskExecutionHost {
            let data_runtime =
                novarocks_native_adapter::backend_test_support::test_backend_data_runtime();
            let completion_supervisor =
                TaskCompletionSupervisor::start(data_runtime.handle().clone(), 64);
            let limits = novarocks_worker::WorkerResultRetainedLimits::try_new(
                256 * 1024 * 1024,
                4 * 1024 * 1024 * 1024,
            )
            .expect("valid root test limits");
            let budget =
                novarocks_worker::result_buffer::ResultRetainedBudget::new(limits.per_process());
            let pool = RootProducerPool::try_new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(64).unwrap(),
                1024 * 1024,
                Arc::clone(&budget),
            )
            .expect("finite test producer pool");
            let runtime = test_execution_runtime(Arc::clone(&facts.0.memory_authority));
            NativeTaskExecutionHost::new(
                NativeFragmentQueryRuntime::new_for_test(
                    Arc::clone(&facts.0.query_manager),
                    Arc::clone(&facts.0.memory_authority),
                ),
                facts,
                TaskInboundCapabilities::new(),
                novarocks_native_adapter::exchange_transmitter::grpc_exchange_transmitter(
                    data_runtime,
                    Duration::from_millis(120_000),
                    #[cfg(novarocks_dev_controls)]
                    None,
                ),
                novarocks_native_adapter::fragment_result_writer::native_result_writer(
                    Arc::clone(&budget),
                    limits.per_root(),
                ),
                budget,
                limits,
                pool,
                Arc::new(UnavailableExchangeReceiverPort),
                Arc::new(novarocks_worker::sink_commit::WorkerSinkCommitPort),
                runtime,
                novarocks_worker::ScanStreamHost::new(
                    novarocks_worker::ScanPreparationConfig::default(),
                    novarocks_native_adapter::backend_test_support::test_scan_stream_runtime(),
                ),
                completion_supervisor,
            )
            .with_compiled_package_compiler(Arc::new(CompiledPackageInterpreter::new(
                FragmentDecodeResourceModel::try_new(&Unbounded).expect("decode model"),
                decode_limits(),
                Arc::new(sealed_rand_subset()),
                Arc::new(crate::compiled_scan_binding::tests::fixture_providers()),
                constants(),
            )))
        }

        fn empty_scan_ranges(node: i32) -> proto::TaskScanRanges {
            proto::TaskScanRanges {
                plan_node_id: node,
                ranges: Vec::new(),
            }
        }

        // A compiled scan package is compiled, its scan bound to the
        // installed read execution, and prepared; a split delivered to the
        // task is decoded by that execution and read by the compiled scan's
        // own driver, which reaches the fixture provider.
        #[test]
        fn a_compiled_host_runs_a_scan_package_from_its_delivered_splits() {
            use crate::compiled_scan_binding::tests::{SCAN_NODE, scan_producer_package};
            use novarocks_execution_contract::task_execution::status::TerminationDetail;

            let (package, receiver) = scan_producer_package();
            let facts = Arc::new(ScanContextFacts::default());
            let host = scan_host(Arc::clone(&facts));
            let identity = super::identity(72, 1, 1);
            let descriptor = producer_descriptor(identity, receiver, &[SCAN_NODE]);
            establish_fixture_context(&host, descriptor.identity());
            let prepared = host
                .install_receiver(
                    &descriptor,
                    scan_input(
                        &descriptor,
                        package_carrier(&package),
                        vec![empty_scan_ranges(SCAN_NODE)],
                    ),
                    novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
                )
                .expect("the compiled scan producer installs");
            assert_eq!(prepared.facts().sink_kind(), FragmentSinkKind::DataStream);
            // An unfiltered scan binds no runtime filter.
            assert_eq!(
                *facts
                    .0
                    .compiled_filter_bindings
                    .lock()
                    .expect("stub compiled filter bindings"),
                vec![BTreeSet::new()]
            );
            host.install_inbound_capability(&descriptor)
                .expect("installs");
            let (owner, reporter) = reporter_for(identity);
            submit_committed(&host, &descriptor, reporter);

            host.apply_task_domain(
                &descriptor,
                &split_intent(
                    PlanNodeId::new(SCAN_NODE).expect("nonnegative node"),
                    SCAN_NODE,
                    vec![
                        crate::typed_connector_test_support::test_support::split_proto(
                            SCAN_NODE, 1,
                        ),
                    ],
                    SplitOffer::batch(SplitSequence::FIRST, SplitSequence::FIRST, true)
                        .expect("one split"),
                ),
            )
            .expect("the compiled scan's read execution decodes its split");
            // The fixture provider refuses every page stream, so the read of
            // that split is what ends the task.
            assert_eq!(await_terminal(&owner), TaskState::Failed);
            let status = owner.current();
            let Some(TerminationDetail::Failed(failure)) = status.termination() else {
                panic!("the provider's refusal is the task's terminal failure");
            };
            assert!(
                failure
                    .detail()
                    .as_str()
                    .contains("fixture page provider is never read"),
                "{failure:?}"
            );

            host.remove_inbound_capability(&descriptor);
            host.remove_receiver(&descriptor);
            assert!(host.task_runtime(identity).is_none());
        }

        // The descriptor and the assignment must address exactly the compiled
        // scans, and only with split-driven work; every refusal leaves no
        // prepared task and no open split queue behind.
        #[test]
        fn a_compiled_scan_task_addresses_exactly_its_scans_by_splits() {
            use crate::compiled_scan_binding::tests::{SCAN_NODE, scan_producer_package};

            let (package, receiver) = scan_producer_package();
            let host = scan_host(Arc::new(ScanContextFacts::default()));
            let refusal = |identity: TaskIdentity, split_nodes: &[i32], initial| {
                let descriptor = producer_descriptor(identity, receiver, split_nodes);
                establish_fixture_context(&host, descriptor.identity());
                let rejection = host
                    .install_receiver(
                        &descriptor,
                        scan_input(&descriptor, package_carrier(&package), initial),
                        novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
                    )
                    .expect_err("the compiled scan task must be refused");
                assert_eq!(rejection.category(), TaskFailureCategory::Protocol);
                assert!(host.task_runtime(identity).is_none());
                assert!(host.split_queues.is_empty(), "the split lease rolled back");
                rejection.detail().as_str().to_owned()
            };

            let unsplit = refusal(super::identity(73, 1, 1), &[], Vec::new());
            assert!(
                unsplit.contains("names split-driven nodes [], but its compiled scans are [10]"),
                "{unsplit}"
            );
            let foreign = refusal(
                super::identity(74, 1, 1),
                &[SCAN_NODE, SCAN_NODE + 2],
                Vec::new(),
            );
            assert!(foreign.contains("split-driven nodes [10, 12]"), "{foreign}");
            let stray = refusal(
                super::identity(75, 1, 1),
                &[SCAN_NODE],
                vec![empty_scan_ranges(SCAN_NODE + 2)],
            );
            assert!(
                stray.contains("assigns scan ranges to node 12, which is not a compiled scan"),
                "{stray}"
            );
        }

        /// A query context that leases the test write catalog with a recording
        /// write execution; every other fact is the stub's.
        #[derive(Default)]
        struct WriteContextFacts {
            stub: StubContextFacts,
            execution: Option<Arc<crate::connector_write_test_support::RecordingWriteExecution>>,
        }

        impl TaskQueryContextFacts for WriteContextFacts {
            fn query_options(
                &self,
                execution: QueryExecutionId,
            ) -> Result<QueryContextOptions, HostRejection> {
                self.stub.query_options(execution)
            }

            fn runtime_filter_session(
                &self,
                execution: QueryExecutionId,
                fragment_instance_id: UniqueId,
                expects_bindings: bool,
            ) -> Result<Option<RuntimeFilterSessionRef>, HostRejection> {
                self.stub
                    .runtime_filter_session(execution, fragment_instance_id, expects_bindings)
            }

            fn runtime_filter_session_for_bindings(
                &self,
                execution: QueryExecutionId,
                fragment_instance_id: UniqueId,
                bindings: &BTreeSet<u32>,
            ) -> Result<Option<RuntimeFilterSessionRef>, HostRejection> {
                self.stub.runtime_filter_session_for_bindings(
                    execution,
                    fragment_instance_id,
                    bindings,
                )
            }

            fn runtime_filter_event_sink(
                &self,
                execution: QueryExecutionId,
                fragment_instance_id: UniqueId,
            ) -> Arc<dyn FragmentEventSink> {
                self.stub
                    .runtime_filter_event_sink(execution, fragment_instance_id)
            }

            fn bind_runtime_filter_feedback(
                &self,
                execution: QueryExecutionId,
                carrier: TaskIdentity,
                reporter: &TaskStatusReporter,
            ) -> bool {
                self.stub
                    .bind_runtime_filter_feedback(execution, carrier, reporter)
            }

            fn deliver_task_dynamic_filter(
                &self,
                execution: QueryExecutionId,
                fragment_instance_id: UniqueId,
                version: DomainVersion,
                payload: &Arc<dyn CodecOwnedContent>,
            ) -> Result<(), HostRejection> {
                self.stub.deliver_task_dynamic_filter(
                    execution,
                    fragment_instance_id,
                    version,
                    payload,
                )
            }

            fn catalog_read_execution(
                &self,
                execution: QueryExecutionId,
                handle: &CatalogHandle,
            ) -> Result<ConnectorExecutionReadBinding, String> {
                self.stub.catalog_read_execution(execution, handle)
            }

            fn catalog_write_execution(
                &self,
                execution: QueryExecutionId,
                handle: &CatalogHandle,
            ) -> Result<ConnectorExecutionWriteBinding, String> {
                let leased = crate::connector_write_test_support::test_write_catalog_handle();
                match &self.execution {
                    Some(recording) if handle == &leased => {
                        Ok(crate::connector_write_test_support::test_write_binding(
                            Arc::clone(recording),
                        ))
                    }
                    _ => self.stub.catalog_write_execution(execution, handle),
                }
            }

            fn storage_resolver(
                &self,
                execution: QueryExecutionId,
            ) -> Result<Arc<dyn ConnectorStorageResolver>, HostRejection> {
                self.stub.storage_resolver(execution)
            }
        }

        /// A compiled host whose provider catalog seals the test provider's
        /// writer recipes.
        fn writer_host(facts: Arc<WriteContextFacts>) -> NativeTaskExecutionHost {
            let data_runtime =
                novarocks_native_adapter::backend_test_support::test_backend_data_runtime();
            let completion_supervisor =
                TaskCompletionSupervisor::start(data_runtime.handle().clone(), 64);
            let limits = novarocks_worker::WorkerResultRetainedLimits::try_new(
                256 * 1024 * 1024,
                4 * 1024 * 1024 * 1024,
            )
            .expect("valid root test limits");
            let budget =
                novarocks_worker::result_buffer::ResultRetainedBudget::new(limits.per_process());
            let pool = RootProducerPool::try_new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(64).unwrap(),
                1024 * 1024,
                Arc::clone(&budget),
            )
            .expect("finite test producer pool");
            let runtime = test_execution_runtime(Arc::clone(&facts.stub.memory_authority));
            NativeTaskExecutionHost::new(
                NativeFragmentQueryRuntime::new_for_test(
                    Arc::clone(&facts.stub.query_manager),
                    Arc::clone(&facts.stub.memory_authority),
                ),
                facts,
                TaskInboundCapabilities::new(),
                novarocks_native_adapter::exchange_transmitter::grpc_exchange_transmitter(
                    data_runtime,
                    Duration::from_millis(120_000),
                    #[cfg(novarocks_dev_controls)]
                    None,
                ),
                novarocks_native_adapter::fragment_result_writer::native_result_writer(
                    Arc::clone(&budget),
                    limits.per_root(),
                ),
                budget,
                limits,
                pool,
                Arc::new(UnavailableExchangeReceiverPort),
                Arc::new(novarocks_worker::sink_commit::WorkerSinkCommitPort),
                runtime,
                novarocks_worker::ScanStreamHost::new(
                    novarocks_worker::ScanPreparationConfig::default(),
                    novarocks_native_adapter::backend_test_support::test_scan_stream_runtime(),
                ),
                completion_supervisor,
            )
            .with_compiled_package_compiler(Arc::new(CompiledPackageInterpreter::new(
                FragmentDecodeResourceModel::try_new(&Unbounded).expect("decode model"),
                decode_limits(),
                Arc::new(sealed_rand_subset()),
                Arc::new(crate::compiled_writer_binding::tests::writer_providers()),
                constants(),
            )))
        }

        /// A single-driver writer task streaming to `receiver` over edge 3, as
        /// fragment instance `finst`. The sink-commit port is process-wide, so
        /// each test task owns its own instance.
        fn writer_descriptor(
            identity: TaskIdentity,
            receiver: u32,
            finst: UniqueId,
        ) -> TaskDescriptor {
            let node = FragmentNodeId::new(i32::try_from(receiver).expect("small node"));
            let destination = ExchangeDestination::new(
                super::identity(70, 2, 1),
                UniqueId::new(900, 901),
                RuntimeEndpoint::new("127.0.0.1", 9060).expect("a legal endpoint"),
                node,
            );
            let topology = ExchangeTopology::try_new(
                vec![
                    ExchangeEdge::try_new(
                        ExchangeEdgeId::new(3).expect("nonzero edge"),
                        node,
                        DataStreamPartitionType::Unpartitioned,
                        vec![destination],
                        0,
                        NonZeroU32::new(1).expect("nonzero"),
                    )
                    .expect("a legal edge"),
                ],
                Vec::new(),
            )
            .expect("a legal topology");
            TaskDescriptor::try_new(
                identity,
                finst,
                NonZeroUsize::new(1).expect("nonzero dop"),
                Vec::new(),
                topology,
            )
            .expect("a legal descriptor")
        }

        // A compiled writer package is received, provider-validated and
        // compiled, its writer bound to the write execution the query leased
        // for the recipe's own catalog, and prepared into an ordinary task
        // runtime streaming its writer relation. Nothing is opened yet.
        #[test]
        fn a_compiled_host_installs_a_writer_package_with_its_leased_write_capability() {
            use crate::compiled_writer_binding::tests::writer_producer_package;

            let (package, receiver) = writer_producer_package();
            let recording =
                Arc::new(crate::connector_write_test_support::RecordingWriteExecution::new());
            let host = writer_host(Arc::new(WriteContextFacts {
                stub: StubContextFacts::default(),
                execution: Some(Arc::clone(&recording)),
            }));
            let identity = super::identity(76, 1, 1);
            let descriptor = writer_descriptor(identity, receiver, UniqueId::new(801, 802));
            establish_fixture_context(&host, descriptor.identity());
            let prepared = host
                .install_receiver(
                    &descriptor,
                    producer_input(&descriptor, package_carrier(&package)),
                    novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
                )
                .expect("the compiled writer producer installs");
            assert_eq!(prepared.facts().sink_kind(), FragmentSinkKind::DataStream);
            assert!(host.task_runtime(identity).is_some());
            assert!(
                recording.opened().is_empty(),
                "a writer opens only when a driver activates"
            );
            host.remove_receiver(&descriptor);
            assert!(host.task_runtime(identity).is_none());
        }

        // Without a write execution leased for the recipe's catalog the
        // writer has no capability on this backend: the task is refused
        // before anything is registered.
        #[test]
        fn a_compiled_writer_without_a_leased_write_execution_is_refused() {
            use crate::compiled_writer_binding::tests::writer_producer_package;

            let (package, receiver) = writer_producer_package();
            let host = writer_host(Arc::new(WriteContextFacts::default()));
            let identity = super::identity(77, 1, 1);
            let descriptor = writer_descriptor(identity, receiver, UniqueId::new(803, 804));
            establish_fixture_context(&host, descriptor.identity());
            let rejection = host
                .install_receiver(
                    &descriptor,
                    producer_input(&descriptor, package_carrier(&package)),
                    novarocks_worker::TaskPreparationStopView::unstopped_for_test(),
                )
                .expect_err("an unleased writer must be refused");
            assert_eq!(rejection.category(), TaskFailureCategory::Protocol);
            assert!(
                rejection.detail().as_str().contains("writer does not bind"),
                "{rejection}"
            );
            assert!(host.task_runtime(identity).is_none());
            assert!(host.split_queues.is_empty(), "the split lease rolled back");
        }
    }
}

#[cfg(test)]
#[path = "memory_admission_projection_tests.rs"]
mod memory_admission_projection_tests;
