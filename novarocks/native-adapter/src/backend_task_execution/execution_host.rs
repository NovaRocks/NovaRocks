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
//! install_receiver          decode + prepare_compiled_fragment -> dormant handle
//! install_inbound_capability register the descriptor for frame admission
//! apply_task_domain         initial splits / edge opens / filters
//! submit_runnable           reserve completion and return a dormant runnable
//! commit_creation           start and attach after Worker installs Live
//! ```
//!
//! The dormant handle is what makes that split honest. Receiver registration
//! is not a step the fragment kernel exposes on its own: it happens inside
//! `prepare_compiled_fragment`, which also builds the pipeline and hands back a
//! [`DormantFragmentHandle`]. So `install_receiver` prepares the whole
//! fragment and parks the handle, transferring its physical receivers into
//! the task's retirement owner. `remove_receiver` synchronously retires those
//! receivers under the Worker's inbound gate, including when another task
//! handle still exists. The Worker's Live commit starts the prepared execution.
//!
//! This host reads nothing from the fragment-based lifecycle registry. Every
//! query-scoped fact it needs arrives through [`TaskQueryContextFacts`], which
//! the query context half of execution implements.

use super::prepared_owner::{
    NativePreparationChild, NativePreparationUnresolved, PreparedKernelOffer,
};
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::mem::ManuallyDrop;
use std::sync::{Arc, Mutex, Weak};

use novarocks_execution::connector::{
    SplitQueueConfig, SplitQueueError, SplitQueueErrorKind, SplitQueueRegistry,
    SplitSequenceEvidence, TaskAttemptKey, TaskAttemptSplitQueues,
};
use novarocks_execution::exec::node::scan::BoundScanRanges;
use novarocks_execution::runtime::execution_runtime::ExecutionRuntime;
#[cfg(test)]
use novarocks_execution::runtime::fragment::ExecutionFailureCause;
use novarocks_execution::runtime::fragment::io::{
    ExchangeDestinationKey, ExchangeEdgeGates, ExchangeFrameTransmitter, ExchangeReceiverPort,
    FragmentCommitPort, FragmentEvent, FragmentEventSink, FragmentResultWriter,
};
use novarocks_execution::runtime::fragment::scan::compiled_scan_nodes;
use novarocks_execution::runtime::fragment::{
    CompiledFragmentSubmission, ExchangeReceiverRetirement, ExecutionFailure, FragmentCancelReason,
    FragmentInstanceSpec, FragmentLaunchError, FragmentOutcome, FragmentRuntimeOptions,
    FragmentTerminalFact, RunningFragmentHandle, ScanAssignments, compiled_sink_kind,
    prepare_compiled_fragment,
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
    AbortCause, CancelReason, SafeDetail, TaskFailureCategory, TaskOutputFacts,
};
use novarocks_execution_contract::{FragmentNodeId, FragmentSinkKind};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_codec::connector_read::{
    ConnectorReadDecoder, MAX_ASSIGNMENT_RETAINED_BYTES, SplitAssignment,
};
use novarocks_proto_models::connector_read as connector_dto;
use novarocks_spi::connector::{
    CatalogHandle, ConnectorExecutionReadBinding, ConnectorExecutionWriteBinding,
    ConnectorStopOwner, ConnectorStorageResolver, read_stack::ConnectorSession,
};
use novarocks_task_codec::creation::{decode_static_package, take_task_assignment};
use novarocks_task_codec::domain::stored_message;
use novarocks_types::{QueryExecutionId, UniqueId};
use novarocks_worker::{TaskCompletionSupervisor, TaskInboundCapabilities};
use tracing::debug;

use super::compiled_package::{
    CompiledPackageCompiler, CompiledPackageError, CompiledTaskOptions, TaskPreparationControl,
};
use crate::compiled_runtime_filter::program_runtime_filter_bindings;
use crate::compiled_scan_binding::{CompiledScanTask, bind_compiled_scans};
use crate::compiled_writer_binding::{CompiledWriteTask, bind_compiled_writers};
use crate::fragment_instance::{NativeFragmentInstanceInput, project_task_instance};
use crate::fragment_submission::compiled_fragment_sink_assignment;
use crate::native_fragment_query::NativeFragmentQueryRuntime;
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
    ) -> Result<Option<RuntimeFilterSessionRef>, novarocks_worker::RuntimeFilterContractError>;

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
    ) -> Result<Option<RuntimeFilterSessionRef>, novarocks_worker::RuntimeFilterContractError>;

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
        producer: novarocks_worker::TaskDynamicFilterWriter,
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
    ) -> Result<ConnectorExecutionReadBinding, novarocks_worker::CatalogExecutionAccessError>;

    /// Binds one catalog for connector write execution inside this query.
    fn catalog_write_execution(
        &self,
        execution: QueryExecutionId,
        handle: &CatalogHandle,
    ) -> Result<ConnectorExecutionWriteBinding, novarocks_worker::CatalogExecutionAccessError>;

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
    /// The process's exact compiled interpreter. A task cannot select another path.
    compiler: Arc<dyn CompiledPackageCompiler>,
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

type NativeInboundCloseFacts =
    Arc<Mutex<Option<Box<[novarocks_execution_contract::ExchangeStreamClose]>>>>;

/// One task's execution-side state, from `install_receiver` to
/// `remove_receiver`.
struct TaskRuntime {
    attempt: TaskAttemptKey,
    stop: ConnectorStopOwner,
    sink_kind: FragmentSinkKind,
    /// Only retained output survives task execution. A Session here would
    /// keep its metadata live while context release waits for that same exit.
    root_channel: Option<Arc<novarocks_worker::root_result_channel::RootResultChannel>>,
    #[cfg(test)]
    root_session_observation: Option<Weak<NativeRootResultSession>>,
    /// The original registered preparation owner survives receiver removal and
    /// submit refusal until explicit runtime handoff or actual stopped drain.
    preparation: Arc<NativePreparationChild>,
    /// The original Worker capability moves once into the dormant start.
    start: Mutex<Option<novarocks_worker::TaskStartPermit>>,
    terminal: Mutex<Option<novarocks_worker::TaskTerminalPermit>>,
    final_statistics: Mutex<Option<novarocks_worker::TaskStatisticsFinalizer>>,
    completion_exit: Mutex<Option<novarocks_worker::CompletionExitNotice>>,
    /// Physical ingress survives actual driver stop until the original
    /// capability owner closes its route. Taken synchronously on retirement.
    exchange_retirement: Mutex<Option<ExchangeReceiverRetirement>>,
    // Frozen receiver-authored facts survive removal from the live registry.
    normal_close_facts: Mutex<Option<Vec<novarocks_execution_contract::ExchangeStreamClose>>>,
    /// Original source-authored facts captured by pre-success validation.
    inbound_close_facts: NativeInboundCloseFacts,
    edges: Arc<ExchangeEdgeGates>,
    splits: Arc<TaskAttemptSplitQueues<ReceivedReadSplit>>,
    #[cfg(novarocks_dev_controls)]
    replay_gate: Mutex<Option<Arc<super::task_update_replay_gate::TaskUpdateReplayGate>>>,
    read_context: Arc<TypedReadAttemptContext>,
    /// This actual sampling producer is bound during original preparation.
    /// Terminal sealing withdraws its owner; concurrent samples keep only
    /// their original producer loan until that actual call exits.
    operator_statistics: Arc<TaskOperatorStatisticsSink>,
    dynamic_filters: novarocks_worker::TaskDynamicFilterProducer,
}

/// The same original receiver captures, moved under the inbound writer fence.
/// This is not Work admission or an exit receipt. The caller must retain it in
/// the original, already admitted host cleanup Work and invoke its independent
/// stages outside every dependency lock. Dropping an unconsumed capture keeps
/// its original objects retained; it never silently releases a source, receiver
/// or runtime. The production Service/Host trait connection remains separate.
#[must_use = "the original supervised receiver cleanup Work must retain and dispose every capture"]
pub struct NativeReceiverRetirement {
    task: TaskIdentity,
    runtime: ManuallyDrop<Option<Arc<TaskRuntime>>>,
    retirement: ManuallyDrop<Option<ExchangeReceiverRetirement>>,
    queues: ManuallyDrop<Option<Arc<SplitQueueRegistry<ReceivedReadSplit>>>>,
}
impl fmt::Debug for NativeReceiverRetirement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeReceiverRetirement")
            .field("task", &self.task)
            .finish_non_exhaustive()
    }
}
impl NativeReceiverRetirement {
    pub fn identity(&self) -> TaskIdentity {
        self.task
    }

    /// Runs actual stop observers. Only the original supervised host lane may
    /// call this; the writer-fenced capture operation never invokes it.
    pub fn request_stop(&self) {
        self.runtime
            .as_ref()
            .expect("original receiver runtime capture")
            .stop
            .request_stop();
    }

    /// Wakes/disposes the original split queues on that same host lane.
    pub fn close_split_queues(&self) {
        let runtime = self
            .runtime
            .as_ref()
            .expect("original receiver runtime capture");
        self.queues
            .as_ref()
            .expect("original receiver queue capture")
            .close_attempt(runtime.attempt);
    }

    /// Irreversibly neutralize the live construction signal and move its same
    /// rich source to the caller. No source destructor or registration hook is
    /// run here, and absence is not a source/Work-exit observation.
    pub fn take_source_after_ingress(&mut self) -> Option<ExecutionFailure> {
        self.retirement
            .as_mut()?
            .take_construction_stop()?
            .close_after_ingress()
    }

    /// Move the original registration and any still-attached source together.
    /// The caller supervises their independent destruction. Even if source
    /// extraction was not called separately, registration transfer first
    /// closes the real signal and cannot leave a resurrectable rich cache.
    pub fn take_registration_after_ingress(
        &mut self,
    ) -> (Option<ExecutionFailure>, Option<ExchangeReceiverRetirement>) {
        let mut retirement = self.retirement.take();
        let source = retirement
            .as_mut()
            .and_then(|retirement| retirement.take_construction_stop())
            .and_then(|owner| owner.close_after_ingress());
        (source, retirement)
    }

    /// Dispose only this original map's runtime capture on the actual host
    /// lane. Other metadata aliases may remain; this supplies no whole-exit
    /// fact. A moved registration still owes its own supervised exit.
    pub fn dispose_runtime_capture(&mut self) {
        assert!(
            self.retirement.is_none(),
            "move the original receiver registration before runtime disposal"
        );
        drop(self.runtime.take());
    }

    pub fn dispose_queue_capture(&mut self) {
        drop(self.queues.take());
    }
}

#[derive(Clone, Copy, Debug)]
enum ReceiverCaptureLock {
    Tasks,
    Retirement,
}
impl fmt::Display for ReceiverCaptureLock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Tasks => "receiver capture task inventory is poisoned",
            Self::Retirement => "receiver capture retirement inventory is poisoned",
        })
    }
}
impl std::error::Error for ReceiverCaptureLock {}
impl novarocks_execution_contract::failure::FailureProjection for ReceiverCaptureLock {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        let reason = match self {
            Self::Tasks => NativePreparationReason::ReceiverTaskInventoryPoisoned,
            Self::Retirement => NativePreparationReason::ReceiverRetirementInventoryPoisoned,
        };
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(
                domain::NATIVE,
                std::num::NonZeroU16::new(reason as u16).unwrap(),
            ),
        )
    }
}

fn receiver_close_facts(
    runtime: &TaskRuntime,
    descriptor: &TaskDescriptor,
) -> Result<Vec<novarocks_execution_contract::ExchangeStreamClose>, HostRejection> {
    if let Some(closes) = runtime
        .inbound_close_facts
        .lock()
        .expect("original pre-success receiver facts")
        .as_ref()
    {
        return Ok(closes.to_vec());
    }
    let retirement = runtime
        .exchange_retirement
        .lock()
        .expect("original receiver author");
    let mut closes = Vec::new();
    for node in descriptor.topology().inbound() {
        let expected = node
            .sources()
            .iter()
            .map(|source| {
                novarocks_execution_contract::ExchangeStreamIdentity::try_new(
                    *source,
                    descriptor.identity(),
                    descriptor.fragment_instance_id(),
                    node.node_id(),
                )
                .expect("original frozen inbound stream")
            })
            .collect::<Vec<_>>();
        let author = retirement.as_ref().ok_or_else(|| {
            internal_failure(NativePreparationContractFailure::new(
                NativePreparationReason::ReceiverCloseFactsMissing,
                "registered inbound input has no original receiver close observer",
            ))
        })?;
        let key = novarocks_execution::runtime::fragment::io::ExchangeReceiverKey {
            fragment_instance_id: descriptor.fragment_instance_id(),
            node_id: node.node_id().get(),
        };
        closes.extend(
            author
                .stream_close_facts(key, &expected)
                .map_err(host_failure)?,
        );
    }
    closes.sort_unstable_by_key(|close| close.stream);
    Ok(closes)
}

fn dispose_receiver_capture(captured: Option<NativeReceiverRetirement>) {
    if let Some(mut captured) = captured {
        captured.request_stop();
        captured.close_split_queues();
        let (source, registration) = captured.take_registration_after_ingress();
        // The original physical retirement body owns each foreign destructor.
        drop(source);
        drop(registration);
        captured.dispose_runtime_capture();
        captured.dispose_queue_capture();
    }
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
/// Original preparation transfers its prepaid producer before any fragment
/// event can run. A terminal observation seals the same producer and withdraws
/// this owner's handle; in-flight calls retain their actual producer loan.
struct TaskOperatorStatisticsSink {
    fragment_instance_id: UniqueId,
    /// `None` when the query did not ask for a profile. Without one there is
    /// no counter to read, which is a different statement from an operator
    /// that ran and counted nothing.
    profiler: Option<Profiler>,
    producer: Mutex<Option<novarocks_worker::TaskStatisticsProducer>>,
}

impl TaskOperatorStatisticsSink {
    fn new(
        fragment_instance_id: UniqueId,
        profiler: Option<Profiler>,
        producer: novarocks_worker::TaskStatisticsProducer,
    ) -> Self {
        Self {
            fragment_instance_id,
            profiler,
            producer: Mutex::new(Some(producer)),
        }
    }

    fn stop_sampling(&self) {
        let producer = self
            .producer
            .lock()
            .expect("original statistics producer")
            .take();
        // A concurrent record call keeps the same original producer alive.
        // Last producer/report destruction must occur outside this lock.
        drop(producer);
    }

    /// Records the operator statistics one profile tree attributes.
    ///
    /// The original producer owns the entry bound and truncation marker, so the
    /// whole attributed list is handed over rather than pre-trimmed here.
    fn record_profile(
        &self,
        producer: &novarocks_worker::TaskStatisticsProducer,
        profile: &RuntimeProfileTree,
    ) {
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
        producer.submit(novarocks_worker::TaskTerminalStatistics {
            statistics: projection.into_statistics(),
            truncated: false,
        });
    }

    /// Records from the profiler this sink holds, if the query asked for one.
    fn record_current(&self, producer: &novarocks_worker::TaskStatisticsProducer) {
        if let Some(profiler) = self.profiler.as_ref() {
            self.record_profile(producer, &profiler.to_native_tree());
        }
    }
}

impl FragmentEventSink for TaskOperatorStatisticsSink {
    fn record(&self, event: FragmentEvent) {
        let producer = self
            .producer
            .lock()
            .expect("original statistics producer")
            .clone();
        let Some(producer) = producer else {
            return;
        };
        // Keep this same loan across profiling, projection and disposal of the
        // actual event capture, even if terminal sealing races the sample.
        match event {
            // A snapshot carries its own tree, so it is reported as given
            // rather than re-read from this sink's profiler: the two are not
            // required to be the same profile.
            FragmentEvent::ProfileSnapshot(snapshot) => {
                debug_assert_eq!(snapshot.fragment_instance_id(), self.fragment_instance_id);
                self.record_profile(&producer, snapshot.profile());
            }
            // Progress is the sampling tick. It carries whole-fragment totals,
            // which say nothing per operator, so the tick is used only as the
            // moment to read the operators' own counters.
            FragmentEvent::Progress(progress) => {
                debug_assert_eq!(progress.fragment_instance_id(), self.fragment_instance_id);
                self.record_current(&producer);
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
        compiler: Arc<dyn CompiledPackageCompiler>,
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
            compiler,
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

    /// Replaces the fixture compiler while retaining the same package-only path.
    #[cfg(test)]
    pub(crate) fn with_compiled_package_compiler(
        mut self,
        compiler: Arc<dyn CompiledPackageCompiler>,
    ) -> Self {
        self.compiler = compiler;
        self
    }

    fn open_root_result_session(
        &self,
        identity: TaskIdentity,
        sink: Option<&novarocks_local_program::StaticSinkProgram>,
        statistics_source_is_unpivot: bool,
        preparation: &novarocks_worker::TaskPreparationContext,
        preparation_child: &Arc<NativePreparationChild>,
    ) -> Result<Option<Arc<NativeRootResultSession>>, HostRejection> {
        let root_session = match sink {
            Some(novarocks_local_program::StaticSinkProgram::RootResult(contract)) => {
                // Every closed internal domain has its BE producer; only a
                // scalar domain identity without its typed schema has none.
                contract.validate_purpose().map_err(host_failure)?;
                if contract.kind()
                    == novarocks_result_contract::RootOutputKind::InternalFacts(
                        novarocks_result_contract::InternalResultDomain::StatisticsArtifactV1,
                    )
                    && !statistics_source_is_unpivot
                {
                    return Err(host_failure(RootPreparationFailure::StatisticsSource));
                }
                // V1 input/hydrate coexistence uses the frozen joint root
                // allowance. Legacy Arrow results keep their original limit;
                // both paths still debit the same process budget below.
                let bounded_limits = novarocks_worker::WorkerResultRetainedLimits::try_new(
                    usize::try_from(
                        novarocks_execution_contract::native_result_support::NativeResultSupportGeometry::V1
                            .root_joint_retained_bytes_per_root,
                    )
                    .map_err(|source| host_failure(RootPreparationFailure::LimitWidth {
                        required: novarocks_execution_contract::native_result_support::NativeResultSupportGeometry::V1.root_joint_retained_bytes_per_root,
                        source,
                    }))?,
                    self.result_retained_limits.per_process().get(),
                )
                .map_err(host_failure)?;
                preparation_child.begin_root_birth().map_err(host_failure)?;
                let channel = match novarocks_worker::root_result_channel::RootResultChannel::try_open_with_owner_metadata(
                    novarocks_execution::runtime::fragment::io::RootResultWriteSpec {
                        task: identity,
                        contract: Arc::clone(contract),
                    },
                    Arc::clone(&self.result_retained_budget),
                    bounded_limits,
                    self.execution_runtime.execution_health(),
                    NativePreparationChild::root_birth_metadata_bytes(),
                ) {
                    Ok(channel) => channel,
                    Err(error) => {
                        preparation_child.root_birth_refused();
                        return Err(host_failure(error));
                    }
                };
                preparation_child.record_root_birth(&channel);
                let waker = preparation.readiness_waker();
                if let Err(original) = channel.bind_preparation_exit_waker(waker) {
                    // The original birth is already retained. Its real close
                    // precedes returning this invariant refusal to rollback.
                    channel.close(novarocks_execution_contract::root_lifetime::RootRetentionClose::ContextAborted);
                    drop(original);
                    return Err(internal_failure(NativePreparationContractFailure::new(
                        NativePreparationReason::RootExitWakeMismatch,
                        "Root output exit has a different original readiness source",
                    )));
                }
                match NativeRootResultSession::try_open_prepared(
                    channel.clone(),
                    &self.root_producer_pool,
                    preparation,
                    preparation_child,
                ) {
                    Ok(session) => Some(session),
                    Err(error) => {
                        // A session refusal after channel birth still owes an
                        // actual sealed output. This is physical host cleanup,
                        // never an inference from a missing child or Weak.
                        channel.close(novarocks_execution_contract::root_lifetime::RootRetentionClose::ContextAborted);
                        return Err(host_failure(error));
                    }
                }
            }
            _ => None,
        };
        Ok(root_session)
    }

    /// Receives, provider-validates and compiles the task's package into a
    /// LocalProgram, then prepares its dormant handle and task runtime.
    /// The registration lease protects early query funding and
    /// rolls back on every refusal before installation.
    fn install_compiled_receiver(
        &self,
        descriptor: &TaskDescriptor,
        input: TaskCreationInput,
        compiler: &dyn CompiledPackageCompiler,
        mut preparation_scope: novarocks_worker::TaskPreparationContext<'_>,
        preparation_child: Arc<NativePreparationChild>,
    ) -> Result<novarocks_worker::PreparedTaskInstallation, HostRejection> {
        let preparation_stop = preparation_scope.view();
        let identity = descriptor.identity();
        let execution = identity.query_execution_id();
        let kernel_key = descriptor.fragment_instance_id();
        let (static_fragment, assignment) = input.into_parts();
        let carrier = decode_static_package(&static_fragment, FieldPath::root("frozen_fragment"))
            .map_err(protocol_failure)?;
        drop(static_fragment);
        let assignment = take_task_assignment(assignment).ok_or_else(|| {
            internal_failure(NativePreparationContractFailure::new(
                NativePreparationReason::MissingAssignment,
                format_args!(
                    "task {identity} creation assignment is not a codec-produced assignment"
                ),
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
            .map_err(host_failure)?;
        let memory = self
            .queries
            .prepare_query_memory_typed(
                execution,
                delivery_expire,
                query_expire,
                context_options.runtime().exec_mem_limit(),
            )
            .map_err(|error| memory_admission_failure_to_host(identity, error))?;
        memory
            .binding()
            .validate_task(identity, self.execution_runtime.memory_authority())
            .map_err(host_failure)?;

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

        #[cfg(novarocks_dev_controls)]
        let prepared_exit_readiness = preparation_scope.readiness_waker();
        let preparation = preparation_stop.borrow_control();
        let control = TaskPreparationControl::new(task_stop.view(), preparation_stop.clone());
        let type_tracker = self
            .queries
            .type_construction_tracker(execution, context_options.runtime().exec_mem_limit())
            .map_err(|error| memory_admission_failure_to_host(identity, error))?;
        let type_creation = crate::native_type_construction::NativeTypeConstruction::try_new(
            type_tracker,
            task_stop.view(),
            preparation_stop.clone(),
        )
        .map_err(|error| native_type_construction_failure_to_host(&error))?;
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
        // The typed invariant reason is static local compiler vocabulary.
        // Keep arbitrary error/provider details out of diagnostics and the wire.
        if let Err(CompiledPackageError::Compile(
            novarocks_local_compiler::FragmentCompileError::Invalid(reason),
        )) = &compiled
        {
            tracing::warn!(
                task = %identity,
                pipeline_dop = descriptor.pipeline_dop().get(),
                reason = *reason,
                "local compiler rejected a fragment invariant"
            );
        }
        if let Err(CompiledPackageError::Compile(
            novarocks_local_compiler::FragmentCompileError::Unsupported { node, feature },
        )) = &compiled
        {
            // Both fields are static compiler vocabulary, never Provider payload.
            tracing::warn!(
                task = %identity,
                pipeline_dop = descriptor.pipeline_dop().get(),
                node = ?node,
                feature = *feature,
                "local compiler rejected an unsupported fragment"
            );
        }
        let compiled = compiled.map_err(|error| compiled_package_rejection(identity, error))?;
        let program = compiled.program();
        let runtime_filters = &compiled.runtime_filters;
        // The bindings this program's runtime-filter sites bind. They decide
        // whether the task needs the query context's filter session. The
        // session must host exactly these bindings for this instance.
        let runtime_filter_bindings =
            program_runtime_filter_bindings(program.graph().requirements(), runtime_filters)
                .map_err(protocol_failure)?;
        let sink = program.graph().sink().ok_or_else(|| {
            internal_failure(NativePreparationContractFailure::new(
                NativePreparationReason::MissingSink,
                format_args!("task {identity} compiled program has no static sink"),
            ))
        })?;
        let sink_kind = compiled_sink_kind(Some(sink)).map_err(internal_failure)?;

        let instance_input = project_task_instance(
            descriptor,
            assignment.into_wire().map_err(internal_failure)?,
            context_options.runtime().as_ref().clone(),
            sink_kind,
        )
        .map_err(protocol_failure)?;
        let scan_assignments =
            compiled_scan_assignments(identity, descriptor, program, &instance_input)?;
        // Every compiled scan binds its typed source here, before anything is
        // published: a refusal rolls back this attempt's lease and local state.
        let scans = bind_compiled_scans(
            program,
            &CompiledScanTask {
                runtime: &typed_runtime,
                fragment_instance_id: kernel_key,
                query_options: &instance_input.query_options,
                stop: task_stop.view(),
                runtime_filters,
                #[cfg(novarocks_dev_controls)]
                observation_controls: self.queries.observation_controls(),
            },
        )
        .map_err(protocol_failure)?;
        // Every compiled writer binds its write capability and every finish
        // its carrier validator here, before anything is published; a write
        // execution is opened only when a driver activates.
        let writers = bind_compiled_writers(
            program,
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
            descriptor.topology(),
        )
        .map_err(protocol_failure)?;
        let sink_assignment = compiled_fragment_sink_assignment(
            sink,
            &instance_input.sink_edge_ids,
            kernel_key,
            descriptor.topology(),
        )
        .map_err(protocol_failure)?;
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
        let task_control = preparation_scope.runtime_control();
        let novarocks_worker::TaskPreparationRoles {
            fragments: completion_roles,
            start,
            terminal,
            statistics,
            dynamic_filters,
            final_statistics,
            completion_exit,
        } = preparation_scope.take_roles().map_err(host_failure)?;
        let kernel_control = Arc::clone(&task_control);
        let root_session = self.open_root_result_session(
            identity,
            program.graph().sink(),
            matches!(
                program.graph().nodes()[program.graph().root().index()].kind(),
                novarocks_local_program::ProgramNodeKind::Unpivot { .. }
            ),
            &preparation_scope,
            &preparation_child,
        )?;
        let root_channel = root_session
            .as_ref()
            .map(|session| Arc::clone(session.channel()));
        #[cfg(test)]
        let root_session_observation = root_session.as_ref().map(Arc::downgrade);
        let root_display_id = i32::try_from(program.graph().root().index()).map_err(|source| {
            internal_failure(NativePreparationIndexError {
                index: program.graph().root().index(),
                source,
            })
        })?;
        // This is the original Arc birth. Submission and the private loan
        // borrow this same immutable backing, with no recreated program.
        let compiled = compiled.into_preparation();
        let submission = CompiledFragmentSubmission::try_new_with_writers(
            Arc::clone(compiled.program()),
            scans,
            writers,
            instance,
        )
        .map_err(protocol_failure)?;

        let profiler = context_options
            .runtime()
            .enable_profile()
            .then(|| fragment_root_profiler(root_display_id));
        let edges = ExchangeEdgeGates::from_frozen_edges(descriptor.topology().outbound())
            .map_err(protocol_failure)?;
        let operator_statistics = Arc::new(TaskOperatorStatisticsSink::new(
            kernel_key,
            profiler.clone(),
            statistics,
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
        let runtime_filter = self
            .context_facts
            .runtime_filter_session_for_bindings(execution, kernel_key, &runtime_filter_bindings)
            .map_err(|cause| HostRejection::from_domain(TaskFailureCategory::Protocol, cause))?;
        let admission =
            self.queries
                .prepare_admission_with_memory(kernel_key, memory, runtime_filter);
        // Scan sources open their providers only when preparation binds them,
        // which needs the admitted fragment tracker installed first.
        typed_runtime
            .install_connector_resource_tracker(admission.fragment_mem_tracker())
            .map_err(internal_failure)?;
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
            .with_task_control(task_control)
            .with_completion_roles(completion_roles)
            .with_result_identity(identity)
            .with_edge_gates(Arc::clone(&edges));
        #[cfg(novarocks_dev_controls)]
        if let Some(observations) = self.queries.observation_controls()
            && let Some(observer) = observations
                .observation_for("exchange-snapshot", execution)
                .map_err(|detail| {
                    internal_failure(NativePreparationContractFailure::new(
                        NativePreparationReason::TestControl,
                        detail,
                    ))
                })?
        {
            let receiver = novarocks_execution::runtime::fragment::io::exchange_receiver::ExecutionRuntimeExchangeReceiverPort::new(self.execution_runtime.clone()).with_test_observer(observer.clone());
            context = context
                .with_exchange_test_observer(Some(observer))
                .with_exchange_receiver_port(Arc::new(receiver));
        }
        if let Some(session) = root_session {
            context = context.with_root_result_session(session);
        }
        let kernel_construction = preparation_child
            .reserve_kernel()
            .map_err(|cause| HostRejection::from_failure(failure_category(&cause), cause))?;
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
        // Bind the complete frozen stream roster and the same Task conclusion
        // owner before Installed can authorize native delivery. Decoding failures
        // remain originating proposals even when their RPC reply has been lost.
        let prepared = prepared.map_err(|error| preparation_failure_to_host(identity, &error))?;
        for input in descriptor.topology().inbound() {
            let streams = input
                .sources()
                .iter()
                .map(|source| {
                    novarocks_execution_contract::ExchangeStreamIdentity::try_new(
                        *source,
                        identity,
                        descriptor.fragment_instance_id(),
                        input.node_id(),
                    )
                    .map_err(|source| {
                        host_failure(NativeInboundCloseFailure::InvalidStream(source))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.exchange_receiver_port
                .bind_unit_receiver(
                    novarocks_execution::runtime::fragment::io::ExchangeReceiverKey {
                        fragment_instance_id: descriptor.fragment_instance_id(),
                        node_id: input.node_id().get(),
                    },
                    streams,
                    Arc::new(
                        novarocks_execution::runtime::runtime_state::RuntimeErrorState::for_task(
                            Arc::clone(&kernel_control),
                        ),
                    ),
                    novarocks_execution_contract::native_result_support::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES as u64,
                )
                .map_err(host_failure)?;
        }
        let inbound_close_facts = Arc::new(Mutex::new(None));
        let captured_facts = Arc::clone(&inbound_close_facts);
        let receiver = Arc::clone(&self.exchange_receiver_port);
        let inbound_descriptor = descriptor.clone();
        let dormant = prepared.with_success_validator(move || {
            let closes = native_inbound_close_facts(receiver.as_ref(), &inbound_descriptor)?;
            validate_native_strict_inputs(&inbound_descriptor, &closes)?;
            *captured_facts.lock().expect("native inbound close facts") =
                Some(closes.into_boxed_slice());
            Ok(())
        });
        kernel_construction.install(dormant, registration, kernel_control);
        #[cfg(novarocks_dev_controls)]
        if let Some(controls) = self.test_controls.as_deref()
            && let Some(cause) = preparation_child
                .inject_prepared_exit_failure(
                    controls,
                    self.scan_stream_host.runtime(),
                    prepared_exit_readiness,
                )
                .map_err(|detail| {
                    internal_failure(NativePreparationContractFailure::new(
                        NativePreparationReason::TestControl,
                        detail,
                    ))
                })?
        {
            return Err(HostRejection::from_failure(failure_category(&cause), cause));
        }

        tracing::debug!(resource_scope = "project-prepare-metadata", receipt = ?project_journal, "Project preparation resource receipt");
        let exchange_retirement = preparation_child.take_exchange_retirement();

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
                preparation: preparation_child,
                start: Mutex::new(Some(start)),
                terminal: Mutex::new(Some(terminal)),
                final_statistics: Mutex::new(Some(final_statistics)),
                completion_exit: Mutex::new(Some(completion_exit)),
                exchange_retirement: Mutex::new(exchange_retirement),
                normal_close_facts: Mutex::new(None),
                inbound_close_facts,
                edges,
                splits,
                #[cfg(novarocks_dev_controls)]
                replay_gate: Mutex::new(None),
                read_context,
                operator_statistics,
                dynamic_filters,
                root_channel,
                #[cfg(test)]
                root_session_observation,
            }),
        );
        lease.retain();
        stop_guard.transfer_to_task();
        Ok(installation)
    }

    fn task_runtime(&self, identity: TaskIdentity) -> Option<Arc<TaskRuntime>> {
        self.tasks.lock().expect(TASK_LOCK).get(&identity).cloned()
    }

    /// Capture only the original receiver metadata while the caller's real
    /// inbound writer fence is held. A poisoned inventory remains installed;
    /// neither a lock failure nor an absent field supplies exit evidence.
    fn take_receiver_retirement(
        &self,
        descriptor: &TaskDescriptor,
    ) -> Result<Option<NativeReceiverRetirement>, ReceiverCaptureLock> {
        let mut tasks = self.tasks.lock().map_err(|_| ReceiverCaptureLock::Tasks)?;
        let Some(runtime) = tasks.get(&descriptor.identity()).cloned() else {
            return Ok(None);
        };
        // Until the last fallible lock succeeds, the map retains its original
        // strong runtime. The temporary metadata loan cannot destroy it even
        // when a poisoned-lock error unwinds this short capture operation.
        let retirement = runtime
            .exchange_retirement
            .lock()
            .map_err(|_| ReceiverCaptureLock::Retirement)?
            .take();
        let captured = NativeReceiverRetirement {
            task: descriptor.identity(),
            runtime: ManuallyDrop::new(Some(runtime)),
            retirement: ManuallyDrop::new(retirement),
            queues: ManuallyDrop::new(Some(self.split_queues.clone())),
        };
        // captured strongly owns the same runtime before the map's metadata
        // reference is released. No last-reference Drop can run in this fence.
        drop(tasks.remove(&descriptor.identity()));
        Ok(Some(captured))
    }

    /// Withdraw ingress and move the same original captures under that exact
    /// delivery writer fence. Execute their independent cleanup stages only
    /// after this method returns, in the original admitted host cleanup Work.
    /// This is not connected to the old synchronous Registry retirement path.
    pub fn capture_receiver_after_removal(
        &self,
        descriptor: &TaskDescriptor,
    ) -> Result<Option<NativeReceiverRetirement>, HostRejection> {
        self.capabilities
            .remove_with_capture(descriptor, || self.take_receiver_retirement(descriptor))
            .map_err(host_failure)
    }

    /// Close using the actual receiver author's exact facts, preserving strict
    /// MustDrain validation and the original normal-close retained reservation.
    /// Invalid facts transfer no captures. A valid closure only moves metadata
    /// under its writer fence; no receiver hook or opaque destructor runs there.
    /// The Service must bind returned captures to its original cleanup Work;
    /// this method supplies no work/source exit or Task-retirement proof.
    pub fn capture_receiver_after_normal_close(
        &self,
        descriptor: &TaskDescriptor,
        closes: Vec<novarocks_execution_contract::ExchangeStreamClose>,
    ) -> Result<Option<NativeReceiverRetirement>, HostRejection> {
        let mut captured = None;
        self.capabilities
            .close_normally_with_facts(descriptor, closes, || {
                captured = Some(self.take_receiver_retirement(descriptor));
            })?;
        // The checked capability operation has returned, releasing both its
        // writer fence and capability state lock. Only neutral error projection
        // happens here; actual capture disposal remains the host's next phase.
        captured
            .expect("the checked normal close invoked its original capture once")
            .map_err(host_failure)
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
        .map_err(internal_failure)?;

        let facts = Arc::clone(&self.context_facts);
        let runtime_filter: RuntimeFilterSessionResolver = Arc::new(move || {
            // Resolution is deferred to bind time on purpose: decode happens
            // before the task is installed, and a scan that binds a filter
            // must see the same session the fragment was prepared with.
            facts.runtime_filter_session(execution, fragment_instance_id, true)
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
        _operation: novarocks_execution_contract::TaskOperationId,
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
                protocol_failure(NativePreparationContractFailure::new(NativePreparationReason::UnknownSplitRead, format_args!(
                    "split assignment names plan node {node}, which has no typed connector read execution"
                )))
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
                .map_err(protocol_failure)?
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
            .map_err(|detail| {
                internal_failure(NativePreparationContractFailure::new(
                    NativePreparationReason::TestControl,
                    detail,
                ))
            })?
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
        if let Some(gate) = runtime
            .replay_gate
            .lock()
            .expect("task replay gate lock")
            .as_ref()
            && assignment.no_more_splits()
            && !sequences.is_empty()
            && preflight.duplicate_sequences().len() == sequences.len()
        {
            gate.observe_duplicate(
                identity,
                _operation,
                node,
                sequences.first().expect("nonempty").sequence_id(),
                sequences.last().expect("nonempty").sequence_id(),
            );
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
        let raw = stored_message::<connector_dto::SplitAssignment>(payload).ok_or_else(|| {
            internal_failure(NativePreparationContractFailure::new(
                NativePreparationReason::WrongSplitCarrier,
                "split domain payload is not a split assignment",
            ))
        })?;
        SplitAssignment::parse(
            raw.clone(),
            novarocks_proto_codec::FieldPath::root("split_assignment"),
        )
        .map_err(protocol_failure)
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
    /// registers a task's exchange receivers inside `prepare_compiled_fragment`, which
    /// also builds the pipeline. Before the fallible installation tail, its
    /// dormant kernel and registration enter the original registered child.
    /// A refusal preserves them for the Worker's stopped rollback; this call
    /// returning does not prove that their asynchronous cleanup has exited.
    // Design: ADR-0158 (docs/adr/ADR-0158-task-creation-is-frozen-once-and-replayed-by-identity.md)
    fn install_receiver(
        &self,
        descriptor: &TaskDescriptor,
        input: TaskCreationInput,
        preparation_scope: novarocks_worker::TaskPreparationContext<'_>,
    ) -> Result<novarocks_worker::PreparedTaskInstallation, HostRejection> {
        let preparation_stop = preparation_scope.view();
        let identity = descriptor.identity();
        if preparation_stop.identity() != identity {
            return Err(internal_failure(NativePreparationContractFailure::new(
                NativePreparationReason::PreparationTaskMismatch,
                format_args!(
                    "preparation Task {} does not match descriptor {identity}",
                    preparation_stop.identity()
                ),
            )));
        }
        preparation_stop
            .borrow_control()
            .checkpoint()
            .map_err(|error| HostRejection::from_failure(failure_category(&error), error))?;
        if self.task_runtime(identity).is_some() {
            return Err(internal_failure(NativePreparationContractFailure::new(
                NativePreparationReason::AlreadyPrepared,
                format_args!("task {identity} already has a prepared fragment"),
            )));
        }

        #[cfg(novarocks_dev_controls)]
        if let Some(controls) = self.test_controls.as_ref() {
            crate::task_protocol_fault::hold_native_task_preparation(controls, identity).map_err(
                |detail| {
                    internal_failure(NativePreparationContractFailure::new(
                        NativePreparationReason::TestControl,
                        detail,
                    ))
                },
            )?;
        }
        let preparation_child = NativePreparationChild::register(
            &preparation_scope,
            self.execution_runtime.execution_health(),
        )
        .map_err(|cause| HostRejection::from_failure(failure_category(&cause), cause))?;
        self.install_compiled_receiver(
            descriptor,
            input,
            self.compiler.as_ref(),
            preparation_scope,
            preparation_child,
        )
    }

    /// Retires the installed ingress synchronously. The original preparation
    /// child separately drains any unadopted kernel through actual stopped;
    /// aliases or a late submit refusal cannot turn Pending into Driver Drop.
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

    fn remove_inbound_capability(&self, descriptor: &TaskDescriptor) {
        self.capabilities.remove(descriptor);
    }

    fn retire_receiver_checked(
        &self,
        descriptor: &TaskDescriptor,
        retirement: novarocks_worker::TaskIngressRetirement,
    ) -> Result<(), HostRejection> {
        if retirement == novarocks_worker::TaskIngressRetirement::Remove {
            let captured = self.capture_receiver_after_removal(descriptor)?;
            dispose_receiver_capture(captured);
            return Ok(());
        }
        let runtime = self.task_runtime(descriptor.identity()).ok_or_else(|| {
            host_failure(NativeInboundCloseFailure::MissingRuntime(
                descriptor.identity(),
            ))
        })?;
        let closes = runtime
            .normal_close_facts
            .lock()
            .expect("original terminal receiver facts")
            .take()
            .ok_or_else(|| {
                internal_failure(NativePreparationContractFailure::new(
                    NativePreparationReason::ReceiverCloseFactsMissing,
                    "checked normal retirement has no original terminal close facts",
                ))
            })?;
        let captured = self.capture_receiver_after_normal_close(descriptor, closes)?;
        dispose_receiver_capture(captured);
        Ok(())
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
        if reporter.identity() != identity {
            return Err(internal_failure(
                novarocks_execution::runtime::task_control::TaskControlIdentityMismatch,
            ));
        }
        let execution = identity.query_execution_id();
        let kernel_key = descriptor.fragment_instance_id();
        let runtime = self.task_runtime(identity).ok_or_else(|| {
            internal_failure(NativePreparationContractFailure::new(
                NativePreparationReason::SubmitWithoutPreparation,
                format_args!("task {identity} was submitted without a prepared fragment"),
            ))
        })?;
        if !runtime.preparation.offer_available() {
            return Err(internal_failure(NativePreparationContractFailure::new(
                NativePreparationReason::SubmitTwice,
                format_args!("task {identity} was submitted twice"),
            )));
        }
        // A split may arrive the moment this create is acknowledged, so the
        // decoded read bindings become resolvable exactly here — after
        // preparation proved every plan node decodes, and before any worker
        // can block on an empty queue.
        runtime.read_context.publish();

        // Same reason for the filter feedback carrier: the domain a channel
        // reduces is advertised through a task's status, and a task has a
        // status owner only once it is being submitted. Before the worker
        // starts, because a producer can close its channel almost immediately
        // after the drivers begin.
        if self.context_facts.bind_runtime_filter_feedback(
            execution,
            identity,
            runtime.dynamic_filters.writer(),
        ) {
            debug!("task {identity} carries its query context's dynamic filter feedback");
        }

        // Claimed here rather than in the worker: a malformed arming is still
        // reportable as a typed rejection on this path, and the worker has no
        // way to report one. The failure itself is applied after the task
        // publishes RUNNING, so the subject is a task that started.
        #[cfg(novarocks_dev_controls)]
        let injected_execution_failure = if let Some(controls) = self.test_controls.as_ref() {
            fault::task_execution_failure_injected(
                controls,
                identity,
                runtime.root_channel.is_some(),
            )
            .map_err(|detail| {
                internal_failure(NativePreparationContractFailure::new(
                    NativePreparationReason::TestControl,
                    detail,
                ))
            })?
        } else {
            None
        };

        let task = Arc::new(NativeRunnableTask::new_with_stop(
            identity,
            kernel_key,
            runtime.stop.clone(),
            Arc::downgrade(&runtime),
            reporter.clone(),
        ));
        let completion_worker = Arc::downgrade(&task);
        let queries = self.queries.clone();
        let split_queues = Arc::clone(&self.split_queues);
        let attempt = runtime.attempt;
        let sink_kind = runtime.sink_kind;
        let root_channel = runtime.root_channel.clone();
        let terminal = runtime
            .terminal
            .lock()
            .expect("original terminal observation permit")
            .take()
            .expect("the original prepared Task owns one terminal observation");
        let completion_reporter = reporter.clone();
        let completion_runtime = Arc::clone(&runtime);
        let close_descriptor = descriptor.clone();
        let mut final_statistics = runtime
            .final_statistics
            .lock()
            .expect("original statistics finalizer")
            .take();
        assert!(
            final_statistics.is_some(),
            "the original prepared Task owns one statistics finalizer"
        );
        let exit = runtime
            .completion_exit
            .lock()
            .expect("original completion exit notice")
            .take()
            .expect("the original prepared Task owns one completion exit notice");
        let mut completion = self
            .completion_supervisor
            .reserve_task_slot(identity, kernel_key, reporter.clone(), exit)
            .map_err(|refusal| {
                let (error, reporter, unused_exit) = refusal.into_task_parts();
                // No action was attached. These captures remain under the
                // original preparation invocation until their actual Drop.
                drop(reporter);
                drop(unused_exit);
                host_failure(error)
            })?;
        completion
            .bind_action(novarocks_worker::TaskTerminalAction::new(
                terminal,
                Box::new(move |fact, observation| {
                    let completion_worker = completion_worker
                        .upgrade()
                        .expect("an installed Task retains its runnable until actual completion");
                    // Preserve the kernel's original removal/cancellation
                    // disposition before any fact can make the task retire.
                    // Only the inbound-close owner performs physical cleanup.
                    if let Some(retirement) = completion_runtime
                        .exchange_retirement
                        .lock()
                        .expect(DORMANT_LOCK)
                        .as_mut()
                    {
                        retirement.record_terminal(fact);
                    }
                    let close_facts = receiver_close_facts(&completion_runtime, &close_descriptor);
                    let close_failure = match close_facts {
                        Ok(closes) => {
                            let checked = novarocks_worker::TaskInboundCapabilities::validate_normal_close_facts(
                                &close_descriptor, &closes,
                            );
                            match checked {
                                Ok(()) => {
                                    *completion_runtime.normal_close_facts.lock()
                                        .expect("original receiver close facts") = Some(closes);
                                    None
                                }
                                Err(error) => Some(error),
                            }
                        }
                        Err(error) => Some(error),
                    };
                    let normal_close_validated = close_failure.is_none();
                    let final_snapshot = fact.profile().map(|profile| {
                        let mut statistics = project_operator_statistics(profile).into_statistics();
                        let truncated = statistics.len()
                            > novarocks_execution_contract::FINAL_TASK_INFO_MAX_OPERATORS;
                        statistics
                            .truncate(novarocks_execution_contract::FINAL_TASK_INFO_MAX_OPERATORS);
                        novarocks_worker::TaskTerminalStatistics {
                            statistics,
                            truncated,
                        }
                    });
                    let final_statistics = final_statistics
                        .take()
                        .expect("one original terminal observation")
                        .seal_for_terminal(final_snapshot);
                    completion_runtime.operator_statistics.stop_sampling();
                    let mut observed = observe_compiled_terminal(
                        &completion_reporter,
                        sink_kind,
                        root_channel.as_deref(),
                        fact,
                        completion_worker.stand_down(),
                        final_statistics,
                    );
                    observed.normal_close_validated = normal_close_validated;
                    if let Some(error) = close_failure
                        && matches!(observed.intent, novarocks_worker::TaskTerminalIntent::SuccessfulProduction { .. })
                    {
                        completion_reporter.failing(error.into_failure(),
                            novarocks_execution_contract::TaskFailurePhase::Execution);
                        observed.intent = novarocks_worker::TaskTerminalIntent::FinishExistingTermination { output: None };
                    }
                    observation.handoff(observed).unwrap_or_else(|_retained| {
                        std::panic::panic_any(NativePreparationUnresolved)
                    });
                    completion_worker.finish();
                    split_queues.close_attempt(attempt);
                    queries.unregister_fragment_execution(execution, kernel_key);
                    queries.finish_fragment(execution);
                }),
            ))
            .map_err(|(error, action)| {
                // A refusal returns the original action without disposing it.
                // Its actual Drop remains inside the preparation scope.
                drop(action);
                host_failure(error)
            })?;

        // The Worker must make the Live record findable before any driver can
        // start or publish completion. The runnable owns this dormant start
        // until the registry calls commit_creation after installing Live.
        let kernel = runtime
            .preparation
            .offer()
            .map_err(|cause| HostRejection::from_failure(failure_category(&cause), cause))?;
        let start = runtime
            .start
            .lock()
            .expect("original Task start permit")
            .take()
            .expect("the same original prepared kernel owns one Worker start permit");
        task.prepare_start(PreparedNativeStart {
            kernel,
            completion,
            start,
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
        let runtime = self.task_runtime(request.identity()).ok_or_else(|| {
            internal_failure(
                super::task_update_replay_gate::ReplayReservationFailure::InstalledTaskRequired(
                    request.identity(),
                ),
            )
        })?;
        let controls = self.test_controls.as_ref().ok_or_else(|| {
            internal_failure(
                super::task_update_replay_gate::ReplayReservationFailure::ControlsRequired,
            )
        })?;
        let mut slot = runtime.replay_gate.lock().expect("task replay gate lock");
        if slot.as_ref().is_some_and(|gate| gate.is_held()) {
            return Err(internal_failure(
                super::task_update_replay_gate::ReplayReservationFailure::AlreadyHeld(
                    request.identity(),
                ),
            ));
        }
        let gate = super::task_update_replay_gate::TaskUpdateReplayGate::reserve(
            request,
            &runtime.splits,
            control,
            runtime.stop.view(),
        )
        .map_err(internal_failure)?;
        *slot = Some(Arc::clone(&gate));
        Ok(Box::new(
            super::task_update_replay_gate::ReplayReservation::new(
                gate,
                Arc::clone(controls),
                self.scan_stream_host.runtime().clone(),
            ),
        ))
    }

    fn apply_task_domain(
        &self,
        descriptor: &TaskDescriptor,
        domain: &TaskDomainUpdate,
        operation: novarocks_execution_contract::TaskOperationId,
    ) -> Result<Option<u64>, HostRejection> {
        let identity = descriptor.identity();
        let runtime = self.task_runtime(identity).ok_or_else(|| {
            internal_failure(NativePreparationContractFailure::new(
                NativePreparationReason::DomainWithoutPreparation,
                format_args!(
                    "domain update reached task {identity}, which has no prepared fragment"
                ),
            ))
        })?;
        match domain {
            TaskDomainUpdate::SplitAssignment(intent) => {
                let assignment = Self::split_assignment(intent.payload().as_ref())?;
                if assignment.plan_node_id() != intent.node().get() {
                    return Err(protocol_failure(NativePreparationContractFailure::new(
                        NativePreparationReason::SplitIntentNodeMismatch,
                        format_args!(
                            "split intent names plan node {} but its payload names {}",
                            intent.node(),
                            assignment.plan_node_id()
                        ),
                    )));
                }
                self.deliver_splits(
                    &runtime,
                    identity,
                    descriptor.fragment_instance_id(),
                    &assignment,
                    operation,
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
                    .map_err(protocol_failure)
            }
            TaskDomainUpdate::CloseExchangeDestination {
                edge, destination, ..
            } => {
                let frozen = descriptor.topology().edge(*edge)
                    .and_then(|outbound| outbound.destinations().iter().find(|member| member.task() == *destination))
                    .ok_or_else(|| protocol_failure(NativePreparationContractFailure::new(NativePreparationReason::UnfrozenExchangeDestination, format_args!("task {identity} did not freeze destination {destination} on edge {edge}"))))?;
                let key = ExchangeDestinationKey::new(
                    frozen.fragment_instance_id(),
                    frozen.destination_node_id().get(),
                );
                let gate = runtime.edges.gate_for_destination(key)
                    .filter(|gate| gate.edge_id() == *edge)
                    .ok_or_else(|| protocol_failure(NativePreparationContractFailure::new(NativePreparationReason::MissingExchangeGate, format_args!("task {identity} has no matching runtime gate for destination {destination} on edge {edge}"))))?;
                runtime
                    .edges
                    .close_destination_normally(gate.destination())
                    .map_err(protocol_failure)?;
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

#[derive(Debug)]
enum NativeInboundCloseFailure {
    InvalidStream(novarocks_execution_contract::ExchangeFlowContractError),
    MissingRuntime(TaskIdentity),
    InvalidFacts(TaskIdentity),
    StrictWithdrawn(novarocks_execution_contract::ExchangeStreamIdentity),
}
impl fmt::Display for NativeInboundCloseFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidStream(source) => write!(f, "invalid frozen inbound stream: {source}"),
            Self::MissingRuntime(task) => {
                write!(f, "normal inbound retirement has no runtime for {task}")
            }
            Self::InvalidFacts(task) => write!(
                f,
                "receiver close facts do not cover the frozen streams for {task}"
            ),
            Self::StrictWithdrawn(stream) => {
                write!(f, "required inbound stream was not consumed: {stream:?}")
            }
        }
    }
}
impl std::error::Error for NativeInboundCloseFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidStream(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_execution_contract::failure::FailureProjection for NativeInboundCloseFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(
                domain::NATIVE,
                std::num::NonZeroU16::new(match self {
                    Self::InvalidStream(_) => 701,
                    Self::MissingRuntime(_) => 702,
                    Self::StrictWithdrawn(_) => 703,
                    Self::InvalidFacts(_) => 704,
                })
                .unwrap(),
            ),
        )
    }
}

fn native_inbound_close_facts(
    receiver: &dyn ExchangeReceiverPort,
    descriptor: &TaskDescriptor,
) -> Result<Vec<novarocks_execution_contract::ExchangeStreamClose>, ExecutionFailure> {
    use novarocks_execution_contract::ExchangeStreamIdentity;
    let mut closes = Vec::new();
    for input in descriptor.topology().inbound() {
        let streams = input
            .sources()
            .iter()
            .map(|source| {
                ExchangeStreamIdentity::try_new(
                    *source,
                    descriptor.identity(),
                    descriptor.fragment_instance_id(),
                    input.node_id(),
                )
                .map_err(|source| {
                    ExecutionFailure::from_domain(NativeInboundCloseFailure::InvalidStream(source))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut actual = receiver
            .stream_close_facts(
                novarocks_execution::runtime::fragment::io::ExchangeReceiverKey {
                    fragment_instance_id: descriptor.fragment_instance_id(),
                    node_id: input.node_id().get(),
                },
                &streams,
            )
            .map_err(|cause| cause.into_execution())?;
        actual.sort_unstable_by_key(|close| close.stream);
        let mut expected = streams;
        expected.sort_unstable();
        if actual.len() != expected.len()
            || actual
                .iter()
                .zip(&expected)
                .any(|(close, expected)| close.stream != *expected)
        {
            return Err(ExecutionFailure::from_domain(
                NativeInboundCloseFailure::InvalidFacts(descriptor.identity()),
            ));
        }
        closes.extend(actual);
    }
    Ok(closes)
}

fn validate_native_strict_inputs(
    descriptor: &TaskDescriptor,
    closes: &[novarocks_execution_contract::ExchangeStreamClose],
) -> Result<(), ExecutionFailure> {
    use novarocks_execution_contract::{ExchangeCloseDisposition, ExchangeDeliveryRequirement};
    let strict_nodes = descriptor
        .topology()
        .inbound()
        .iter()
        .filter(|input| input.delivery() == ExchangeDeliveryRequirement::MustDrain)
        .map(|input| input.node_id())
        .collect::<BTreeSet<_>>();
    for close in closes
        .iter()
        .filter(|close| strict_nodes.contains(&close.stream.destination_node()))
    {
        if !matches!(close.disposition, ExchangeCloseDisposition::Consumed { .. }) {
            return Err(ExecutionFailure::from_domain(
                NativeInboundCloseFailure::StrictWithdrawn(close.stream),
            ));
        }
    }
    Ok(())
}

/// The first stand-down a task was asked for. It is latched, not queued: a
/// later reason never rewrites the first one. The latch only decides what the
/// fragment is told; the Task's sole owner accepts commands before physical
/// stop and never lets a later abort or failure rewrite accepted cancellation.
#[derive(Debug)]
struct ReceiverConstructionAborted(AbortCause);
impl std::fmt::Display for ReceiverConstructionAborted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "receiver construction aborted: {:?}", self.0)
    }
}
impl std::error::Error for ReceiverConstructionAborted {}
impl novarocks_execution_contract::failure::FailureProjection for ReceiverConstructionAborted {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        // The Task command was accepted before the receiver was stopped.
        // Seeing that stop is never a new originating failure, including
        // when the accepted command is a derived peer-failure placeholder.
        FailureFact::stop_observation(FailureReason::new(
            domain::NATIVE,
            std::num::NonZeroU16::new(22).unwrap(),
        ))
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum StandDown {
    Cancel(CancelReason),
    Quiesce,
    Abort(AbortCause),
}

impl StandDown {
    /// The termination this stand-down proposes to the status owner.
    const fn proposal(self) -> novarocks_worker::TaskStopIntent {
        match self {
            Self::Cancel(reason) => novarocks_worker::TaskStopIntent::Canceled(reason),
            Self::Quiesce => {
                novarocks_worker::TaskStopIntent::Canceled(CancelReason::UpstreamNoLongerNeeded)
            }
            Self::Abort(cause) => novarocks_worker::TaskStopIntent::Aborted(cause),
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
    /// Only an original StartRuntime already attempted this same dormant
    /// start. An earlier stop must not start a never-issued action itself.
    start_deferred: bool,
    handle: Option<Arc<dyn FragmentStandDown>>,
    stand_down: Option<StandDown>,
    finished: bool,
}

struct PreparedNativeStart {
    kernel: PreparedKernelOffer,
    completion: novarocks_worker::TaskCompletionReservation,
    start: novarocks_worker::TaskStartPermit,
    #[cfg(novarocks_dev_controls)]
    injected_execution_failure: Option<crate::test_controls::NativeFaultClaim>,
}

/// One submitted task, as the owner may address it.
pub struct NativeRunnableTask {
    identity: TaskIdentity,
    fragment_instance_id: UniqueId,
    stop: ConnectorStopOwner,
    receiver_runtime: Option<Weak<TaskRuntime>>,
    // A real runnable owns a lifecycle reporter. The no-receiver constructor
    // is an explicit test-only physical stand-down fixture, never a Task.
    status: Option<TaskStatusReporter>,
    state: Mutex<RunnableState>,
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
        status: TaskStatusReporter,
    ) -> Self {
        Self {
            identity,
            fragment_instance_id,
            stop,
            receiver_runtime: Some(receiver_runtime),
            status: Some(status),
            state: Mutex::new(RunnableState::default()),
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
            status: None,
            state: Mutex::new(RunnableState::default()),
        }
    }

    fn prepare_start(&self, prepared: PreparedNativeStart) {
        let mut state = self.state.lock().expect(RUNNABLE_LOCK);
        assert!(
            state.prepared_start.is_none(),
            "one dormant start per runnable"
        );
        state.prepared_start = Some(prepared);
    }

    fn start_committed(&self) {
        let (prepared, latched_stop) = {
            let mut state = self.state.lock().expect(RUNNABLE_LOCK);
            let prepared = state
                .prepared_start
                .take()
                .expect("a committed native task has one dormant start");
            state.start_deferred = false;
            (prepared, state.stand_down.is_some())
        };
        let stopped_before_start = latched_stop
            || self.status.as_ref().is_some_and(|status| {
                let current = status.current().unwrap_or_else(|_missing_owner| {
                    std::panic::panic_any(NativePreparationUnresolved)
                });
                current.termination().is_some() || current.is_terminal()
            });
        tracing::debug!(pid = std::process::id(), task = ?self.identity,
            stopped_before_start, latched_stop,
            "Original dormant Native start entered its committed physical path");
        let PreparedNativeStart {
            kernel,
            completion,
            start,
            #[cfg(novarocks_dev_controls)]
            injected_execution_failure,
        } = prepared;
        // A stopped Task still drains the already prepared operators through
        // their real cleanup/exit protocol. The shared Task control prevents
        // provider activation; no synthetic stopped fact bypasses those jobs.
        if stopped_before_start {
            // Never started: withdraw only this unused Running input. The
            // prepared runtime still owes its independent physical teardown.
            drop(start);
        } else {
            match start.authorize_running() {
                Ok(()) => {}
                Err(novarocks_worker::TaskStartRefusal::Stopped { permit }) => {
                    // A normal Context stop may win after the observation
                    // above, before its ordered Task stop effect is applied.
                    // Retain this exact dormant start until that effect can
                    // drive real cleanup; do not activate or invent a cause.
                    self.defer_prepared_start(PreparedNativeStart {
                        kernel,
                        completion,
                        start: permit,
                        #[cfg(novarocks_dev_controls)]
                        injected_execution_failure,
                    });
                    return;
                }
                Err(_original_refusal) => {
                    // A lost owner or refused causal handoff has its original
                    // finite custody. No kernel activation or synthetic exit
                    // is authorized here; failure takeover retains the rest.
                    std::panic::panic_any(NativePreparationUnresolved)
                }
            }
        }
        let completion = completion.commit_creation();
        #[cfg(novarocks_dev_controls)]
        let initial_failure = if stopped_before_start {
            None
        } else {
            injected_execution_failure
                .filter(|claim| claim.cancellation().is_active())
                .map(
                    |claim| match fault::record_task_execution_failure(self.identity, &claim) {
                        Ok(()) => fault::injected_task_execution_failure(),
                        Err(error) => error,
                    },
                )
        };
        #[cfg(not(novarocks_dev_controls))]
        let initial_failure = None;
        let running = kernel
            .start(initial_failure)
            .unwrap_or_else(|_| std::panic::panic_any(NativePreparationUnresolved));
        // The shared Task control closes ordinary activation before this
        // attachment. Replay a concurrent physical stop after releasing the
        // native mutex; callbacks may inspect or address this same runnable.
        self.attach(Arc::new(running.clone()));
        // One Core publisher combines the original completion signal and the
        // weak prepared-owner receipt. The actual runnable is already attached.
        kernel
            .finish_handoff(completion)
            .unwrap_or_else(|_| std::panic::panic_any(NativePreparationUnresolved));
    }

    fn defer_prepared_start(&self, prepared: PreparedNativeStart) {
        let stop_already_applied = {
            let mut state = self.state.lock().expect(RUNNABLE_LOCK);
            assert!(
                state.prepared_start.is_none(),
                "retain one original deferred start"
            );
            state.prepared_start = Some(prepared);
            state.start_deferred = true;
            state.stand_down.is_some()
        };
        // Stop and restore share this lock. Either the already-applied stop
        // resumes here, or its actual later handoff resumes the same original.
        // The resumed path sees that exact stop and performs cleanup once.
        if stop_already_applied {
            self.start_committed();
        }
    }

    /// Publishes the same handle and replays any latched physical stand-down.
    fn attach(&self, handle: Arc<dyn FragmentStandDown>) {
        let (latched, replaced) = {
            let mut state = self.state.lock().expect(RUNNABLE_LOCK);
            let replaced = state.handle.replace(Arc::clone(&handle));
            (state.stand_down, replaced)
        };
        drop(replaced);
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
    fn finish(&self) {
        let handle = {
            let mut state = self.state.lock().expect(RUNNABLE_LOCK);
            state.finished = true;
            state.handle.take()
        };
        // Last kernel destruction belongs to the actual completion owner,
        // outside the runnable metadata lock. Root drain is its own original
        // Worker role and is never inferred from a later control request.
        drop(handle);
    }

    fn accept_control(&self, stand_down: StandDown) {
        if let Some(status) = &self.status {
            match stand_down {
                StandDown::Cancel(reason) => {
                    status.canceling(reason);
                }
                StandDown::Quiesce => {
                    status.canceling(CancelReason::UpstreamNoLongerNeeded);
                }
                StandDown::Abort(cause) => {
                    status.aborting(cause);
                }
            }
        }
    }

    fn request(&self, stand_down: StandDown) {
        self.accept_control(stand_down);
        self.request_physical(stand_down);
    }

    fn request_physical(&self, stand_down: StandDown) {
        let (handle, resume_start) = {
            let mut state = self.state.lock().expect(RUNNABLE_LOCK);
            if state.stand_down.is_some() {
                return;
            }
            state.stand_down = Some(stand_down);
            self.stop.request_stop();
            if state.finished {
                (None, false)
            } else {
                (
                    state.handle.as_ref().map(Arc::clone),
                    state.start_deferred && state.prepared_start.is_some(),
                )
            }
        };
        if resume_start {
            self.start_committed();
        }
        // An absent handle leaves the original stand-down latched for attach.
        if let Some(handle) = handle {
            handle.cancel(stand_down.reason());
        }
    }
}

impl RunnableTask for NativeRunnableTask {
    fn commit_creation(&self) {
        self.start_committed();
    }

    fn cancel(&self, reason: CancelReason) {
        self.request(StandDown::Cancel(reason));
    }

    fn quiesce(&self) {
        self.request(StandDown::Quiesce);
    }

    fn abort(&self, cause: AbortCause) {
        self.accept_control(StandDown::Abort(cause));
        // Receiver construction follows the registration's lifetime, not the
        // first kernel stand-down. A later true abort must still stop it.
        if let Some(runtime) = self.receiver_runtime.as_ref().and_then(Weak::upgrade)
            && let Some(retirement) = runtime
                .exchange_retirement
                .lock()
                .expect(DORMANT_LOCK)
                .as_ref()
        {
            retirement.stop_construction(ExecutionFailure::from_domain(
                ReceiverConstructionAborted(cause),
            ));
        }
        // Release the retirement lock before reaching the original latch or
        // kernel cancellation, which may run completion callbacks.
        self.request_physical(StandDown::Abort(cause));
    }
}

/// One compiled terminal callback supplies neutral observation facts; the
/// original rich failure always transfers before the same causal fence.
fn observe_compiled_terminal(
    reporter: &TaskStatusReporter,
    sink_kind: FragmentSinkKind,
    root_channel: Option<&novarocks_worker::root_result_channel::RootResultChannel>,
    fact: &FragmentTerminalFact,
    stand_down: Option<StandDown>,
    final_statistics: Option<novarocks_worker::TaskTerminalStatistics>,
) -> novarocks_worker::TaskTerminalObservation {
    use novarocks_worker::{TaskTerminalIntent, TaskTerminalObservation};
    let stand_down = stand_down.or_else(|| {
        if !root_channel.is_some_and(|channel| channel.is_closed()) {
            return None;
        }
        match reporter
            .current()
            .unwrap_or_else(|_missing_owner| std::panic::panic_any(NativePreparationUnresolved))
            .termination()
        {
            Some(novarocks_execution_contract::TerminationDetail::Aborted(cause)) => {
                Some(StandDown::Abort(*cause))
            }
            Some(novarocks_execution_contract::TerminationDetail::Canceled(reason)) => {
                Some(StandDown::Cancel(*reason))
            }
            _ => None,
        }
    });
    let context_handoff = root_channel.is_some_and(|channel| {
        matches!(
            channel.producer_state(),
            novarocks_execution::runtime::fragment::io::RootProducerState::ContextHeld
        )
    });
    let intent = if let Some(stand_down) = stand_down {
        let output = (!matches!(stand_down, StandDown::Abort(_))
            && matches!(fact.outcome(), FragmentOutcome::Succeeded)
            && (sink_kind != FragmentSinkKind::Result || context_handoff))
            .then_some(TaskOutputFacts::new(true));
        match fact.outcome() {
            FragmentOutcome::Failed(error) => reporter.failing(
                ExecutionFailure::from_domain(error.clone()),
                novarocks_execution_contract::TaskFailurePhase::Execution,
            ),
            _ => match stand_down.proposal() {
                novarocks_worker::TaskStopIntent::Canceled(reason) => reporter.canceling(reason),
                novarocks_worker::TaskStopIntent::Aborted(cause) => reporter.aborting(cause),
            },
        }
        TaskTerminalIntent::FinishExistingTermination { output }
    } else {
        match fact.outcome() {
            FragmentOutcome::Succeeded
                if sink_kind != FragmentSinkKind::Result || context_handoff =>
            {
                TaskTerminalIntent::SuccessfulProduction {
                    completed_output: Some(TaskOutputFacts::new(true)),
                }
            }
            FragmentOutcome::Succeeded => {
                reporter.failing(
                    ExecutionFailure::from_domain(RootHandoffMissing),
                    novarocks_execution_contract::TaskFailurePhase::Execution,
                );
                TaskTerminalIntent::FinishExistingTermination { output: None }
            }
            FragmentOutcome::Cancelled { reason } => {
                reporter.failing(
                    ExecutionFailure::from_domain(reason.clone()),
                    novarocks_execution_contract::TaskFailurePhase::Execution,
                );
                TaskTerminalIntent::FinishExistingTermination { output: None }
            }
            FragmentOutcome::Failed(error) => {
                reporter.failing(
                    ExecutionFailure::from_domain(error.clone()),
                    novarocks_execution_contract::TaskFailurePhase::Execution,
                );
                TaskTerminalIntent::FinishExistingTermination { output: None }
            }
        }
    };
    TaskTerminalObservation {
        intent,
        output_released: true,
        normal_close_validated: false,
        final_statistics,
    }
}

/// The completion owner knows the missing handoff invariant. Native does not
/// infer it from producer text or treat a canceled kernel as client intent.
#[derive(Debug)]
struct RootHandoffMissing;
impl fmt::Display for RootHandoffMissing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("root stopped without completed context handoff")
    }
}
impl std::error::Error for RootHandoffMissing {}
impl novarocks_execution_contract::failure::FailureProjection for RootHandoffMissing {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(700).unwrap()),
        )
    }
}

/// Native's exact Task location is context, not another classification of the
/// launch cause. Keep the actual launch envelope and its original source.
#[derive(Debug)]
struct NativeTaskPreparationFailure {
    identity: TaskIdentity,
    source: FragmentLaunchError,
}
impl fmt::Display for NativeTaskPreparationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "task {} could not be prepared: {}",
            self.identity, self.source
        )
    }
}
impl std::error::Error for NativeTaskPreparationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_execution_contract::failure::FailureProjection for NativeTaskPreparationFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        novarocks_execution_contract::failure::FailureProjection::failure_fact(&self.source)
    }
}
fn preparation_failure_to_host(
    identity: TaskIdentity,
    error: &FragmentLaunchError,
) -> HostRejection {
    let detail = SafeDetail::truncating_display(format_args!(
        "task {identity} could not be prepared during {} ({}): {}",
        error.stage(),
        error.kind(),
        error.cause().safe_detail()
    ));
    HostRejection::from_domain_with_detail(
        failure_category(error),
        NativeTaskPreparationFailure {
            identity,
            source: error.clone(),
        },
        detail,
    )
}

/// This is only the legacy status category projection. The producer's neutral
/// fact remains the source of semantic classification; no domain is inspected.
pub(super) fn failure_category(
    cause: &(impl novarocks_execution_contract::failure::FailureProjection + ?Sized),
) -> TaskFailureCategory {
    use novarocks_execution_contract::failure::FailureClass;
    match cause.failure_fact().class() {
        FailureClass::Resource => TaskFailureCategory::ResourceExhausted,
        FailureClass::Unsupported => TaskFailureCategory::Unsupported,
        FailureClass::Invariant | FailureClass::Unexpected => TaskFailureCategory::Internal,
        FailureClass::Data
        | FailureClass::Dependency
        | FailureClass::Representation
        | FailureClass::Control => TaskFailureCategory::Execution,
    }
}

fn split_queue_rejection(error: SplitQueueError) -> HostRejection {
    let category = failure_category(&error);
    if error.kind() == SplitQueueErrorKind::Closed {
        return HostRejection::from_closed_queue(category, error);
    }
    HostRejection::from_domain(category, error)
}

/// The instance scan assignments of a compiled program: an empty range
/// binding for each compiled scan's physical node, because a compiled scan
/// takes every provider relation's work units as runtime splits.
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
        .map_err(protocol_failure)?
        .into_values()
        .collect::<BTreeSet<FragmentNodeId>>();
    let split_nodes = descriptor
        .split_plan_nodes()
        .iter()
        .map(|node| FragmentNodeId::new(node.get()))
        .collect::<BTreeSet<_>>();
    if split_nodes != scan_nodes {
        return Err(protocol_failure(NativePreparationContractFailure::new(
            NativePreparationReason::CompiledSplitCoverage,
            format_args!(
                "task {identity} names split-driven nodes {:?}, but its compiled scans are {:?}",
                split_nodes, scan_nodes
            ),
        )));
    }
    for (node, ranges) in &instance.raw_scan_ranges {
        if !scan_nodes.contains(node) {
            return Err(protocol_failure(NativePreparationContractFailure::new(
                NativePreparationReason::UnknownCompiledRangeNode,
                format_args!(
                    "task {identity} assigns scan ranges to node {}, which is not a compiled scan",
                    node.get()
                ),
            )));
        }
        if !ranges.is_empty() {
            return Err(protocol_failure(NativePreparationContractFailure::new(
                NativePreparationReason::FrozenRangesForSplitScan,
                format_args!(
                    "task {identity} assigns frozen ranges to compiled scan node {}, whose work \
                 arrives only as splits",
                    node.get()
                ),
            )));
        }
    }
    ScanAssignments::try_new(
        scan_nodes
            .into_iter()
            .map(|node| (node, BoundScanRanges::None))
            .collect(),
    )
    .map_err(internal_failure)
}

/// The task's own stop stays primary; a package this interpreter refuses is
/// content a peer of the same island must not have sent.
fn compiled_package_rejection(
    identity: TaskIdentity,
    error: CompiledPackageError,
) -> HostRejection {
    let failure = ExecutionFailure::from_domain(error);
    let detail = SafeDetail::truncating_display(format_args!(
        "task {identity} package could not be compiled: {}",
        failure.safe_detail()
    ));
    HostRejection::from_domain_with_detail(failure_category(&failure), failure, detail)
}

#[derive(Debug)]
enum RootPreparationFailure {
    StatisticsSource,
    LimitWidth {
        required: u64,
        source: std::num::TryFromIntError,
    },
}
impl std::fmt::Display for RootPreparationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StatisticsSource => {
                f.write_str("StatisticsArtifactV1 requires its bounded final Unpivot source")
            }
            Self::LimitWidth { required, .. } => write!(
                f,
                "bounded root limit {required} exceeds the target size range"
            ),
        }
    }
}
impl std::error::Error for RootPreparationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::LimitWidth { source, .. } => Some(source),
            _ => None,
        }
    }
}
impl novarocks_execution_contract::failure::FailureProjection for RootPreparationFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        match self {
            Self::StatisticsSource => FailureFact::new(
                FailureClass::Invariant,
                FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(9400).unwrap()),
            ),
            Self::LimitWidth { required, .. } => FailureFact::new(
                FailureClass::Resource,
                FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(9401).unwrap()),
            )
            .with_resource(ResourceFacts {
                request_scope: Some(ResourceScope::Task),
                constraint_scope: None,
                dimension: ResourceDimension::Bytes,
                requested: Some(*required),
                required_growth: None,
                limit: u64::try_from(usize::MAX).ok(),
                live: None,
                committed: None,
            }),
        }
    }
}
fn host_failure<E>(source: E) -> HostRejection
where
    E: std::error::Error
        + novarocks_execution_contract::failure::FailureProjection
        + Send
        + Sync
        + 'static,
{
    HostRejection::from_domain(failure_category(&source), source)
}

#[derive(Clone, Copy, Debug)]
#[repr(u16)]
enum NativePreparationReason {
    MissingAssignment = 9600,
    MissingSink = 9601,
    UnknownSplitRead = 9602,
    WrongSplitCarrier = 9603,
    AlreadyPrepared = 9604,
    SubmitWithoutPreparation = 9606,
    SubmitTwice = 9607,
    DomainWithoutPreparation = 9609,
    SplitIntentNodeMismatch = 9610,
    UnfrozenExchangeDestination = 9611,
    MissingExchangeGate = 9612,
    CompiledSplitCoverage = 9613,
    UnknownCompiledRangeNode = 9614,
    FrozenRangesForSplitScan = 9615,
    TestControl = 9620,
    RootIndex = 9621,
    PreparationTaskMismatch = 9622,
    ReceiverTaskInventoryPoisoned = 9623,
    ReceiverRetirementInventoryPoisoned = 9624,
    ReceiverCloseFactsMissing = 9625,
    RootExitWakeMismatch = 9626,
}
#[derive(Debug)]
struct NativePreparationContractFailure {
    reason: NativePreparationReason,
    detail: novarocks_execution_contract::failure::BoundedDetail,
}
impl NativePreparationContractFailure {
    fn new(reason: NativePreparationReason, detail: impl std::fmt::Display) -> Self {
        Self {
            reason,
            detail: novarocks_execution_contract::failure::BoundedDetail::truncating_display(
                detail,
            ),
        }
    }
}
impl std::fmt::Display for NativePreparationContractFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.detail.fmt(f)
    }
}
impl std::error::Error for NativePreparationContractFailure {}
impl novarocks_execution_contract::failure::FailureProjection for NativePreparationContractFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        FailureFact::new(
            if matches!(self.reason, NativePreparationReason::TestControl) {
                FailureClass::Unexpected
            } else {
                FailureClass::Invariant
            },
            FailureReason::new(
                domain::NATIVE,
                std::num::NonZeroU16::new(self.reason as u16).unwrap(),
            ),
        )
    }
}
#[derive(Debug)]
struct NativePreparationIndexError {
    index: usize,
    source: std::num::TryFromIntError,
}
impl std::fmt::Display for NativePreparationIndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "compiled root index {} exceeds i32", self.index)
    }
}
impl std::error::Error for NativePreparationIndexError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_execution_contract::failure::FailureProjection for NativePreparationIndexError {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(
                domain::NATIVE,
                std::num::NonZeroU16::new(NativePreparationReason::RootIndex as u16).unwrap(),
            ),
        )
    }
}
fn internal_failure<E>(source: E) -> HostRejection
where
    E: std::error::Error
        + novarocks_execution_contract::failure::FailureProjection
        + Send
        + Sync
        + 'static,
{
    HostRejection::from_domain(TaskFailureCategory::Internal, source)
}
fn protocol_failure<E>(source: E) -> HostRejection
where
    E: std::error::Error
        + novarocks_execution_contract::failure::FailureProjection
        + Send
        + Sync
        + 'static,
{
    HostRejection::from_domain(TaskFailureCategory::Protocol, source)
}

/// Admission already uses the preparation resource category. Keep the exact
/// typed memory cause through this boundary without guessing from its text.
fn memory_admission_failure_to_host(
    identity: TaskIdentity,
    error: crate::native_fragment_query::NativeFragmentAdmissionError,
) -> HostRejection {
    let failure = ExecutionFailure::from_domain(error);
    let detail = SafeDetail::truncating_display(format_args!(
        "task {identity} could not be admitted: {}",
        failure.safe_detail()
    ));
    HostRejection::from_domain_with_detail(failure_category(&failure), failure, detail)
}

fn native_type_construction_failure_to_host(
    error: &crate::native_type_construction::NativeTypeConstructionError,
) -> HostRejection {
    HostRejection::from_domain(
        failure_category(error),
        ExecutionFailure::from_domain(error.clone()),
    )
}

#[cfg(test)]
pub(crate) mod tests {
    use super::{
        CompositeFragmentEventSink, FragmentStandDown, NativePreparationContractFailure,
        NativePreparationReason, NativeRunnableTask, NativeTaskExecutionHost, PreparationStopGuard,
        QueryContextOptions, ReceiverConstructionAborted, RootProducerPool, StandDown,
        TaskCompletionSupervisor, TaskQueryContextFacts, internal_failure,
        native_inbound_close_facts, validate_native_strict_inputs,
    };
    use crate::backend_task_execution::CompiledPackageError;
    use crate::task_query_context_options::query_options_fingerprint;

    use std::collections::{BTreeMap, BTreeSet};
    use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, Weak};
    use std::time::Duration;

    use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
    use novarocks_execution::runtime::execution_runtime::{
        ExecutionRuntime, ExecutionRuntimeConfig,
    };
    use novarocks_execution::runtime::fragment::io::{
        ExchangeDestinationKey, FragmentEvent, FragmentEventSink, FragmentProgress,
        NoopFragmentEventSink, UnavailableExchangeReceiverPort,
    };
    use novarocks_execution::runtime::fragment::{
        ExecutionFailure, FragmentCancelReason, FragmentExecutionError, FragmentExecutionErrorKind,
    };
    use novarocks_execution::runtime::profile::{ProfileUnit, RuntimeProfile};
    use novarocks_execution::runtime::query_options::QueryOptions;
    use novarocks_execution::runtime_filter::RuntimeFilterSessionRef;
    use novarocks_execution_contract::DataStreamPartitionType;
    use novarocks_execution_contract::task_execution::creation::{
        CreationContent, FrozenBytes, TaskCreationInput,
    };
    use novarocks_execution_contract::task_execution::descriptor::{
        ExchangeDestination, ExchangeEdge, ExchangeInbound, ExchangeSource, ExchangeTopology,
        TaskDescriptor,
    };
    use novarocks_execution_contract::task_execution::domain::{
        CodecOwnedContent, ContentFingerprint, DomainVersion, EdgeOpenVersion, ExchangeEdgeId,
        PlanNodeId, SplitOffer, SplitSequence,
    };
    use novarocks_execution_contract::task_execution::identity::{
        QueryContextRef, TaskIdentity, TaskOperationId,
    };
    use novarocks_execution_contract::task_execution::operation::{
        CreateTask, OperationEnvelope, OperationKind, OperationOutcome, QueryContextDomainUpdate,
        ReleaseQueryContext, TaskDomainUpdate,
    };
    use novarocks_execution_contract::task_execution::status::{
        AbortCause, CancelReason, TaskFailureCategory, TaskOutputFacts, TaskState,
        TerminationDetail,
    };
    use novarocks_execution_contract::{FragmentNodeId, FragmentSinkKind};
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_models::{connector_read as connector_dto, novarocks as proto};
    use novarocks_spi::connector::ConnectorStopOwner;
    use novarocks_spi::connector::{
        CatalogHandle, ConnectorError, ConnectorErrorKind, ConnectorExecutionReadBinding,
        ConnectorExecutionWriteBinding, ConnectorStorageResolver, ResolvedVendedS3Access,
        StorageAccessRequest,
    };
    use novarocks_types::UniqueId;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use novarocks_worker::query_context::QueryExecutionKey;
    use novarocks_worker::{InboundFrameClaim, IngressRejection, TaskInboundCapabilities};
    use prost::Message;

    use novarocks_native_adapter::exchange_data_plane::{
        ExchangeRouteQuery, NativeExchangeDataPlane,
    };

    use novarocks_native_adapter::native_fragment_query::NativeFragmentQueryRuntime;
    use novarocks_worker::{
        HostRejection, QueryContextHost, ReleasedContextEvidence, RunnableTask, SharedFactsRequest,
        TaskExecutionHost, TaskStatusReporter,
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
                TaskFailureCategory::Internal,
            ),
            (
                RuntimeScalarEvaluationFailure::Host(RuntimeScalarMemoryRefusal::Capacity(
                    novarocks_memory::CapacityError::Invalid {
                        detail: "cancelled Internal",
                    },
                )),
                TaskFailureCategory::Internal,
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
            assert_eq!(super::failure_category(error.cause()), expected);
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
            failure_category,
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
                TaskFailureCategory::ResourceExhausted,
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
            use novarocks_execution_contract::failure::FailureProjection;
            let expected_fact = request.failure_fact();
            let cause = ExecutionFailureCause::PreparationMetadata(
                PreparationMetadataFailure::Request(request.clone()),
            );
            assert_eq!(cause.failure_fact(), expected_fact, "{request:?}");
            assert_eq!(failure_category(&cause), expected, "{request:?}");
            let host = compiled_package_rejection(
                task,
                CompiledPackageError::ProjectMetadataRequest(request.clone()),
            );
            assert_eq!(host.category(), expected, "{request:?}");
            assert_eq!(host.failure_fact(), expected_fact, "{request:?}");
            assert_eq!(
                crate::backend_test_support::find_source::<MetadataRequestError>(&host),
                Some(&request)
            );
        }
        for stop in [
            PreparationMemoryStop::Cancel(CancelReason::UpstreamNoLongerNeeded),
            PreparationMemoryStop::Abort(AbortCause::QueryFailed),
            PreparationMemoryStop::Abort(AbortCause::LeaseExpired),
            PreparationMemoryStop::Abort(AbortCause::PeerTaskFailed),
        ] {
            let refusal = PreparationMemoryRefusal::Interrupted(
                novarocks_execution::runtime::fragment::ExecutionFailure::from_domain(stop),
            );
            let cause = ExecutionFailureCause::PreparationMetadata(
                PreparationMetadataFailure::Host(refusal.clone()),
            );
            assert_eq!(failure_category(&cause), TaskFailureCategory::Execution);
            assert_eq!(
                compiled_package_rejection(task, CompiledPackageError::ProjectMetadata(refusal))
                    .category(),
                TaskFailureCategory::Execution
            );
        }
        for (refusal, expected) in [
            (
                PreparationMemoryRefusal::MissingQueryMemory,
                TaskFailureCategory::Internal,
            ),
            (
                PreparationMemoryRefusal::Capacity(novarocks_memory::CapacityError::Invalid {
                    detail: "cancelled Internal: original capacity",
                }),
                TaskFailureCategory::Internal,
            ),
            (
                PreparationMemoryRefusal::Capacity(
                    novarocks_memory::CapacityError::MetadataExhausted {
                        registry: novarocks_memory::MetadataRegistryLabel::Accounts,
                        limit: 7,
                    },
                ),
                TaskFailureCategory::ResourceExhausted,
            ),
        ] {
            use novarocks_execution_contract::failure::FailureProjection;
            let expected_fact = refusal.failure_fact();
            let cause = ExecutionFailureCause::PreparationMetadata(
                PreparationMetadataFailure::Host(refusal.clone()),
            );
            assert_eq!(failure_category(&cause), expected, "{refusal:?}");
            assert_eq!(cause.failure_fact(), expected_fact);
            let host = compiled_package_rejection(
                task,
                CompiledPackageError::ProjectMetadata(refusal.clone()),
            );
            assert_eq!(host.category(), expected, "{refusal:?}");
            assert_eq!(host.failure_fact(), expected_fact);
            assert_eq!(
                crate::backend_test_support::find_source::<PreparationMemoryRefusal>(&host),
                Some(&refusal)
            );
        }
        let original = ExecutionFailureCause::Kernel(novarocks_functions::KernelFailure::Internal(
            novarocks_functions::KernelDiagnostic::new("ResourceExhausted: cancelled"),
        ));
        assert_eq!(failure_category(&original), TaskFailureCategory::Internal);
    }

    fn unexpected_fixture_failure(
        detail: &str,
    ) -> novarocks_execution::runtime::fragment::ExecutionFailure {
        novarocks_functions::KernelFailure::Operational(novarocks_functions::KernelDiagnostic::new(
            detail,
        ))
        .into()
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

    pub(crate) mod creation_body {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/backend_task_execution/execution_host/creation_body.rs"
        ));
    }
    use creation_body::{Body, BodySink};

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
            Vec::new(),
            topology,
        )
        .expect("a legal descriptor")
    }

    /// A single-driver Values descriptor with no scans or exchange topology.
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
                HostRejection::from_domain(TaskFailureCategory::ResourceExhausted, error)
            })
    }

    fn inspect_native_receiver(
        host: &Arc<NativeTaskExecutionHost>,
        facts: Arc<dyn TaskQueryContextFacts>,
        descriptor: TaskDescriptor,
        input: TaskCreationInput,
        foreign_descriptor: Option<TaskDescriptor>,
        assertion: impl FnOnce(&Result<novarocks_worker::PreparedTaskInstallation, HostRejection>)
        + Send
        + 'static,
    ) {
        let factory = crate::backend_task_execution::tests::node_fixture::factory_from_host(
            Arc::new(AcceptingContextHost {
                queries: host.queries.clone(),
                facts,
            }),
        );
        crate::backend_task_execution::tests::node_fixture::inspect_install(
            host.clone(),
            factory,
            host.execution_runtime.execution_health(),
            descriptor,
            input,
            foreign_descriptor,
            assertion,
        );
    }

    fn inbound_topology(node: FragmentNodeId, sources: Vec<ExchangeSource>) -> ExchangeTopology {
        ExchangeTopology::try_new(
            Vec::new(),
            vec![
                ExchangeInbound::try_new(
                    node,
                    sources,
                    novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                )
                .expect("a legal inbound"),
            ],
        )
        .expect("a legal topology")
    }

    fn claim_frame(
        capabilities: &TaskInboundCapabilities,
        query: ExchangeRouteQuery,
    ) -> InboundFrameClaim {
        capabilities.claim_frame(
            query.stream.destination_fragment_instance_id(),
            query.stream.destination_node(),
            query.stream.source().fragment_instance_id(),
            query.stream.source().sender_ordinal(),
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
    fn stream_sink(partition: DataStreamPartitionType) -> BodySink {
        BodySink::Stream(partition)
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
        ) -> Result<Option<RuntimeFilterSessionRef>, novarocks_worker::RuntimeFilterContractError>
        {
            self.filter_sessions_requested
                .fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }

        fn runtime_filter_session_for_bindings(
            &self,
            _execution: QueryExecutionId,
            _fragment_instance_id: UniqueId,
            bindings: &BTreeSet<u32>,
        ) -> Result<Option<RuntimeFilterSessionRef>, novarocks_worker::RuntimeFilterContractError>
        {
            self.compiled_filter_bindings
                .lock()
                .expect("stub compiled filter bindings")
                .push(bindings.clone());
            // No participant is installed in this stub, so it answers exactly
            // as the real host answers a context that installed none.
            if bindings.is_empty() {
                Ok(None)
            } else {
                Err(
                    novarocks_worker::RuntimeFilterContractError::invalid_contract(format!(
                        "stub context installed no participant for {bindings:?}"
                    )),
                )
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
            _producer: novarocks_worker::TaskDynamicFilterWriter,
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
            handle: &CatalogHandle,
        ) -> Result<ConnectorExecutionReadBinding, novarocks_worker::CatalogExecutionAccessError>
        {
            Err(
                novarocks_worker::CatalogExecutionAccessError::LeaseUnavailable {
                    catalog: handle.clone(),
                },
            )
        }

        fn catalog_write_execution(
            &self,
            _execution: QueryExecutionId,
            handle: &CatalogHandle,
        ) -> Result<ConnectorExecutionWriteBinding, novarocks_worker::CatalogExecutionAccessError>
        {
            Err(
                novarocks_worker::CatalogExecutionAccessError::LeaseUnavailable {
                    catalog: handle.clone(),
                },
            )
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
            novarocks_execution::runtime::function_set::ExecutionFunctionSetBuilder::new();
        novarocks_sql::compiler::contribute_builtin_functions(function_set.catalog_builder_mut())
            .expect("builtin function metadata");
        let function_set = Arc::new(function_set.seal().expect("builtin execution function set"));
        Arc::new(
            ExecutionRuntime::new(
                ExecutionRuntimeConfig {
                    driver_threads: 1,
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
        let runtime = test_execution_runtime(Arc::clone(&facts.memory_authority));
        let pool = RootProducerPool::try_new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(64).unwrap(),
            1024 * 1024,
            Arc::clone(&budget),
            runtime.execution_health(),
        )
        .expect("finite test producer pool");
        let data_runtime =
            novarocks_native_adapter::backend_test_support::test_backend_data_runtime();
        let completion_supervisor =
            TaskCompletionSupervisor::start(data_runtime.handle().clone(), 64);

        NativeTaskExecutionHost::new(
            Arc::new(compiled_package_host::interpreter()),
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
                stream: novarocks_execution_contract::ExchangeStreamIdentity::try_new(
                    ExchangeSource::new(producer, producer_key, 0),
                    consumer,
                    kernel_key,
                    node,
                )
                .unwrap(),
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
                stream: novarocks_execution_contract::ExchangeStreamIdentity::try_new(
                    ExchangeSource::new(producer, source, sender_ordinal),
                    consumer,
                    destination,
                    FragmentNodeId::new(destination_node_id),
                )
                .unwrap(),
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
            capabilities,
            consumer.backend_process_id(),
        );
        let request = |destination: UniqueId, source: UniqueId| {
            let stream = novarocks_execution_contract::ExchangeStreamIdentity::try_new(
                ExchangeSource::new(producer, source, 0),
                consumer,
                destination,
                node,
            )
            .unwrap();
            novarocks_task_codec::exchange_unit_codec::encode_delivery_request(
                novarocks_task_codec::exchange_unit_codec::DecodedExchangeUnitFrame {
                    stream,sender_count:1,sender_id:7,backend_number:0,
                    item:novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeUnitItem::End(
                        novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeUnitSequence::FIRST),
                    payload:bytes::Bytes::new(),next_demand:None,
                })
        };
        let reply = plane
            .deliver_unit(
                request(kernel_key, producer_key),
                producer.backend_process_id(),
            )
            .unwrap();
        let proto::deliver_exchange_unit_response::Outcome::Refused(authorized_refusal) =
            reply.outcome.unwrap()
        else {
            panic!("unavailable receiver must refuse")
        };
        let reply = plane
            .deliver_unit(
                request(UniqueId::new(1, 1), producer_key),
                producer.backend_process_id(),
            )
            .unwrap();
        let proto::deliver_exchange_unit_response::Outcome::Refused(absent_refusal) =
            reply.outcome.unwrap()
        else {
            panic!("unknown destination must refuse")
        };
        // The authorized route reaches the explicit unavailable receiver; an
        // absent kernel key is rejected by Worker before the receiver port.
        assert_ne!(authorized_refusal, absent_refusal);
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
    fn replacing_and_finishing_a_runnable_drop_original_handles_outside_its_metadata_lock() {
        struct ReentrantDrop {
            owner: Weak<NativeRunnableTask>,
            unlocked_drops: Arc<std::sync::atomic::AtomicUsize>,
            locked_drops: Arc<std::sync::atomic::AtomicUsize>,
        }
        impl FragmentStandDown for ReentrantDrop {
            fn cancel(&self, _: FragmentCancelReason) {}
        }
        impl Drop for ReentrantDrop {
            fn drop(&mut self) {
                let owner = self
                    .owner
                    .upgrade()
                    .expect("the actual runnable outlives this handle");
                let unlocked = owner.state.try_lock().is_ok();
                let observed = if unlocked {
                    &self.unlocked_drops
                } else {
                    &self.locked_drops
                };
                observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let task = Arc::new(NativeRunnableTask::new_without_receiver_with_stop(
            identity(14, 1, 9),
            UniqueId::new(81, 89),
            ConnectorStopOwner::new(),
        ));
        let unlocked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let locked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let make = || {
            Arc::new(ReentrantDrop {
                owner: Arc::downgrade(&task),
                unlocked_drops: unlocked.clone(),
                locked_drops: locked.clone(),
            }) as Arc<dyn FragmentStandDown>
        };
        task.attach(make());
        task.attach(make());
        task.finish();
        assert_eq!(unlocked.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(locked.load(std::sync::atomic::Ordering::SeqCst), 0);
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
        task.finish();
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
            "a refused preparation cannot bind a scan runtime-filter session"
        );
        assert!(
            facts
                .compiled_filter_bindings
                .lock()
                .unwrap()
                .iter()
                .all(BTreeSet::is_empty),
            "a refused fixture cannot borrow any undeclared filter binding"
        );
        assert!(
            host.capabilities.is_empty(),
            "a refused install publishes no ingress capability"
        );
    }

    #[test]
    fn a_foreign_preparation_task_is_refused_before_static_decode_or_install() {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureProjection, FailureReason, domain,
        };
        let facts = Arc::new(StubContextFacts::default());
        let host = Arc::new(host(facts.clone()));
        let task = identity(90_801, 1, 1);
        let descriptor = consistent_descriptor(task, UniqueId::new(90_801, 1));
        let foreign = consistent_descriptor(identity(90_801, 1, 2), UniqueId::new(90_801, 2));
        inspect_native_receiver(
            &host,
            facts,
            foreign,
            Body::values(1).input(&descriptor),
            Some(descriptor),
            move |result| {
                let refusal = result
                    .as_ref()
                    .expect_err("another Task cannot authorize preparation");
                let fact = refusal.failure_fact();
                assert_eq!(fact.class(), FailureClass::Invariant);
                assert_eq!(
                    fact.reason(),
                    FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(9622).unwrap())
                );
            },
        );
        assert!(host.task_runtime(task).is_none());
        assert!(host.split_queues.is_empty());
    }

    /// Assert the operation projection separately from the unchanged domain fact.
    /// Local source inspection must never depend on the safe wire diagnostic.
    fn assert_host_refusal_source<'a, T>(
        refusal: &'a novarocks_worker::HostRejection,
        category: TaskFailureCategory,
        expected: novarocks_execution_contract::failure::FailureFact,
        safe_detail: &str,
    ) -> &'a T
    where
        T: std::error::Error + novarocks_execution_contract::failure::FailureProjection + 'static,
    {
        use novarocks_execution_contract::failure::FailureProjection;
        assert_eq!(refusal.category(), category);
        assert_eq!(refusal.failure_fact(), expected);
        assert_eq!(refusal.detail().as_str(), safe_detail);
        let carrier = crate::backend_test_support::find_source::<ExecutionFailure>(refusal)
            .expect("the original execution carrier remains local");
        assert_eq!(carrier.failure_fact(), expected);
        let author = crate::backend_test_support::find_source::<T>(refusal)
            .expect("the exact producing domain source remains in the original chain");
        assert_eq!(author.failure_fact(), expected);
        author
    }
    fn expected_refusal_fact(
        class: novarocks_execution_contract::failure::FailureClass,
        namespace: std::num::NonZeroU16,
        code: u16,
    ) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{FailureFact, FailureReason};
        FailureFact::new(
            class,
            FailureReason::new(namespace, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
    fn assert_protocol_refusal(
        refusal: &novarocks_worker::HostRejection,
        category: TaskFailureCategory,
        code: u16,
        kind: novarocks_proto_codec::error::ProtocolErrorKind,
        path: FieldPath,
        detail: &str,
    ) {
        use novarocks_execution_contract::failure::{FailureClass, domain};
        let author = assert_host_refusal_source::<novarocks_proto_codec::error::ProtocolError>(
            refusal,
            category,
            expected_refusal_fact(FailureClass::Data, domain::PROTOCOL, code),
            &format!("Data failure (reason 15:{code})"),
        );
        assert_eq!(author.kind(), &kind);
        assert_eq!(author.path(), &path);
        assert_eq!(author.detail(), detail);
    }

    fn assert_native_preparation_refusal(
        refusal: &novarocks_worker::HostRejection,
        reason: NativePreparationReason,
        original_detail: &str,
    ) {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureFact, FailureProjection, FailureReason, domain,
        };
        assert_eq!(
            refusal.failure_fact(),
            FailureFact::new(
                FailureClass::Invariant,
                FailureReason::new(
                    domain::NATIVE,
                    std::num::NonZeroU16::new(reason as u16).unwrap()
                ),
            )
        );
        let carrier = std::error::Error::source(refusal)
            .unwrap()
            .downcast_ref::<novarocks_execution::runtime::fragment::ExecutionFailure>()
            .expect("the host retains its original execution carrier");
        let novarocks_execution::runtime::fragment::ExecutionFailureCause::Domain(source) =
            carrier.cause()
        else {
            panic!("the preparation refusal must retain its actual Native author: {carrier:?}");
        };
        let cause = source
            .downcast_ref::<NativePreparationContractFailure>()
            .expect("the preparation author is retained across the Host boundary");
        assert_eq!(cause.reason as u16, reason as u16);
        assert!(cause.detail.as_str().contains(original_detail), "{cause}");
        // Safe wire detail remains the bounded neutral projection. The local
        // source above, not wire text, carries domain-specific diagnostics.
    }

    #[test]
    fn an_assignment_this_codec_did_not_produce_cannot_be_installed() {
        let facts = Arc::new(StubContextFacts::default());
        let host = Arc::new(host(Arc::clone(&facts)));
        let descriptor = consistent_descriptor(identity(17, 1, 1), UniqueId::new(111, 112));

        inspect_native_receiver(
            &host,
            facts.clone(),
            descriptor.clone(),
            TaskCreationInput::new(Body::values(1).frozen_bytes(), Box::new(ForeignAssignment)),
            None,
            move |result| {
                let rejection = result
                    .as_ref()
                    .expect_err("the original Native receiver refuses this input");
                assert_eq!(rejection.category(), TaskFailureCategory::Internal);
                assert_native_preparation_refusal(
                    rejection,
                    NativePreparationReason::MissingAssignment,
                    "not a codec-produced assignment",
                );
            },
        );
        assert_nothing_prepared(&host, &facts);
    }

    #[test]
    fn an_unreadable_static_fragment_is_refused_before_anything_is_prepared() {
        let facts = Arc::new(StubContextFacts::default());
        let host = Arc::new(host(Arc::clone(&facts)));
        let descriptor = consistent_descriptor(identity(18, 1, 1), UniqueId::new(121, 122));
        let assignment = Body::values(1).input(&descriptor).into_parts().1;

        inspect_native_receiver(
            &host,
            facts.clone(),
            descriptor.clone(),
            TaskCreationInput::new(
                FrozenBytes::freeze(bytes::Bytes::from_static(&[0x0a, 0x80])),
                assignment,
            ),
            None,
            move |result| {
                let rejection = result
                    .as_ref()
                    .expect_err("the original Native receiver refuses this input");
                let original_decoder = proto::FrozenFragment::decode(&[0x0a_u8, 0x80][..])
                    .expect_err("the exact original byte input is malformed protobuf");
                assert_protocol_refusal(
                    rejection,
                    TaskFailureCategory::Protocol,
                    3,
                    novarocks_proto_codec::error::ProtocolErrorKind::InvalidValue,
                    FieldPath::root("frozen_fragment"),
                    &format!("invalid frozen fragment: {original_decoder}"),
                );
            },
        );
        assert_nothing_prepared(&host, &facts);
    }

    #[test]
    fn a_task_dop_outside_its_fragment_domain_is_refused_before_anything_is_prepared() {
        let facts = Arc::new(StubContextFacts::default());
        let host = Arc::new(host(Arc::clone(&facts)));
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

        inspect_native_receiver(
            &host,
            facts.clone(),
            descriptor.clone(),
            Body::values(1).input(&descriptor),
            None,
            move |result| {
                let rejection = result
                    .as_ref()
                    .expect_err("the original Native receiver refuses this input");
                use novarocks_execution_contract::failure::{FailureClass, domain};
                let author = assert_host_refusal_source::<CompiledPackageError>(
                    rejection,
                    TaskFailureCategory::Internal,
                    expected_refusal_fact(FailureClass::Invariant, domain::LOCAL_COMPILER, 2),
                    &format!(
                        "task {} package could not be compiled: Invariant failure (reason 19:2)",
                        descriptor.identity()
                    ),
                );
                assert!(matches!(
                    author,
                    CompiledPackageError::Compile(
                        novarocks_local_compiler::FragmentCompileError::Invalid(
                            "DOP is outside physical domain"
                        )
                    )
                ));
            },
        );
        assert_nothing_prepared(&host, &facts);
    }

    #[test]
    fn a_values_task_cannot_declare_a_split_driven_scan_node() {
        let facts = Arc::new(StubContextFacts::default());
        let host = Arc::new(host(Arc::clone(&facts)));
        let descriptor = TaskDescriptor::try_new(
            identity(47_001, 1, 1),
            UniqueId::new(47_001, 1),
            NonZeroUsize::new(1).unwrap(),
            vec![PlanNodeId::new(10).unwrap()],
            ExchangeTopology::default(),
        )
        .unwrap();
        inspect_native_receiver(
            &host,
            facts.clone(),
            descriptor.clone(),
            Body::values(1).input(&descriptor),
            None,
            move |result| {
                let rejection = result
                    .as_ref()
                    .expect_err("the original Native receiver refuses this input");
                assert_eq!(rejection.category(), TaskFailureCategory::Protocol);
                assert_native_preparation_refusal(
                    rejection,
                    NativePreparationReason::CompiledSplitCoverage,
                    &format!(
                        "names split-driven nodes {:?}, but its compiled scans are {{}}",
                        BTreeSet::from([FragmentNodeId::new(10)])
                    ),
                );
            },
        );
        assert_nothing_prepared(&host, &facts);
    }

    #[test]
    fn a_scan_range_for_a_node_the_static_plan_lacks_is_refused_before_receivers_register() {
        let facts = Arc::new(StubContextFacts::default());
        let host = Arc::new(host(Arc::clone(&facts)));
        let descriptor = consistent_descriptor(identity(47, 1, 1), UniqueId::new(149, 150));
        let mut body = Body::values(1);
        body.assignment.initial_scan_ranges = vec![proto::TaskScanRanges {
            plan_node_id: 99,
            ranges: Vec::new(),
        }];

        inspect_native_receiver(
            &host,
            facts.clone(),
            descriptor.clone(),
            body.input(&descriptor),
            None,
            move |result| {
                let rejection = result
                    .as_ref()
                    .expect_err("the original Native receiver refuses this input");
                assert_eq!(rejection.category(), TaskFailureCategory::Protocol);
                assert_native_preparation_refusal(
                    rejection,
                    NativePreparationReason::UnknownCompiledRangeNode,
                    "assigns scan ranges to node 99, which is not a compiled scan",
                );
            },
        );
        assert_nothing_prepared(&host, &facts);
    }

    #[test]
    fn a_static_sink_that_disagrees_with_its_assigned_edge_is_refused_before_receivers_register() {
        let facts = Arc::new(StubContextFacts::default());
        let host = Arc::new(host(Arc::clone(&facts)));
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
        let body = Body::with_sink(
            1,
            stream_sink(DataStreamPartitionType::HashPartitioned),
            vec![edge.get()],
        );

        inspect_native_receiver(
            &host,
            facts.clone(),
            descriptor.clone(),
            body.input(&descriptor),
            None,
            move |result| {
                let rejection = result
                    .as_ref()
                    .expect_err("the original Native receiver refuses this input");
                assert_protocol_refusal(
                    rejection,
                    TaskFailureCategory::Protocol,
                    6,
                    novarocks_proto_codec::error::ProtocolErrorKind::InconsistentFields,
                    FieldPath::root("fragment_package").field("sink"),
                    "static sink branch partitioning disagrees with its assigned edge",
                );
            },
        );
        assert_nothing_prepared(&host, &facts);
    }

    #[test]
    fn a_fragment_may_use_a_lower_dop_than_its_query_context() {
        let facts = Arc::new(StubContextFacts::default());
        facts.set_context_dop(8);
        let host = Arc::new(host(Arc::clone(&facts)));
        let task = identity(46, 1, 1);
        let kernel_key = UniqueId::new(147, 148);
        let descriptor = consistent_descriptor(task, kernel_key);

        inspect_native_receiver(
            &host,
            facts.clone(),
            descriptor.clone(),
            Body::values(1).input(&descriptor),
            None,
            |result| {
                let prepared = result
                    .as_ref()
                    .expect("task-local DOP 1 is legal under context DOP 8");
                assert_eq!(prepared.facts().sink_kind(), FragmentSinkKind::Result);
            },
        );
        assert_eq!(facts.compiled_filter_bindings.lock().unwrap().len(), 1);
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
                TaskOperationId::new_v7(),
            )
            .expect_err("no prepared fragment exists");
        assert_native_preparation_refusal(
            &rejection,
            NativePreparationReason::DomainWithoutPreparation,
            "has no prepared fragment",
        );
        assert_eq!(facts.dynamic_filters_delivered.load(Ordering::SeqCst), 0);
    }

    // ----------------------------------------------------- prepared lifecycle

    #[test]
    fn opaque_result_writer_failure_keeps_nominal_source_in_preparation_projection() {
        use novarocks_execution::runtime::fragment::io::{
            FragmentIoError, FragmentIoErrorKind, FragmentIoOperation, FragmentResultSession,
            FragmentResultWriter, ResultPresentation, ResultWriteSpec,
        };
        use novarocks_execution::runtime::fragment::{
            FragmentLaunchError, FragmentLaunchErrorKind, FragmentLaunchStage,
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
            let task = identity(80_030 + index as i64, 1, 1);
            let writer = RefusingWriter {
                opens: AtomicUsize::new(0),
                message,
            };
            let source = match writer.open(
                ResultWriteSpec::new(
                    UniqueId::new(80_030 + index as i64, 1),
                    ResultPresentation::MysqlText,
                    None,
                    true,
                )
                .with_task_identity(task),
            ) {
                Err(original) => original,
                Ok(_) => panic!("the actual callback must refuse opening"),
            };
            let error = FragmentLaunchError::from_failure(
                FragmentLaunchStage::BuildPipelines,
                FragmentLaunchErrorKind::PipelineBuild,
                ExecutionFailure::from_domain(source),
            );
            let refusal = super::preparation_failure_to_host(task, &error);
            assert_eq!(writer.opens.load(Ordering::SeqCst), 1);
            assert_eq!(refusal.category(), TaskFailureCategory::Internal);
            let original = crate::backend_test_support::find_source::<FragmentIoError>(&refusal)
                .expect("the actual writer refusal survives the projection");
            assert_eq!(original.kind(), FragmentIoErrorKind::Internal);
            assert_eq!(
                original.to_string(),
                format!("{} {}: {message}", original.operation(), original.kind())
            );
            assert!(!refusal.detail().as_str().contains(message));
            assert!(refusal.detail().as_str().contains("Unexpected failure"));
        }
    }

    #[test]
    fn preparation_registration_rolls_back_when_a_prepared_receiver_is_removed() {
        let facts = Arc::new(StubContextFacts::default());
        let manager = facts.query_manager.clone();
        let host = Arc::new(host(facts.clone()));
        let descriptor = consistent_descriptor(identity(521, 1, 1), UniqueId::new(521, 522));
        let prepared_manager = manager.clone();
        inspect_native_receiver(
            &host,
            facts,
            descriptor.clone(),
            Body::values(1).input(&descriptor),
            None,
            move |result| {
                result
                    .as_ref()
                    .expect("actual preparation registers its original fragment");
                let prepared = prepared_manager.native_execution_resource_snapshot();
                assert_eq!(prepared.active_contexts, 1);
                assert_eq!(prepared.active_fragments, 1);
            },
        );
        let removed = manager.native_execution_resource_snapshot();
        assert_eq!(removed.active_contexts, 0);
        assert_eq!(removed.active_fragments, 0);
    }

    #[test]
    fn a_prepared_task_opens_only_the_edges_its_descriptor_froze() {
        let facts = Arc::new(StubContextFacts::default());
        let host = Arc::new(host(Arc::clone(&facts)));
        let producer = identity(21, 1, 1);
        let consumer = identity(21, 2, 1);
        let kernel_key = UniqueId::new(161, 162);
        let edge = ExchangeEdgeId::new(1).expect("nonzero edge");
        let descriptor =
            descriptor_with(producer, kernel_key, 1, outbound_topology(edge, consumer));
        let body = Body::with_sink(
            1,
            stream_sink(DataStreamPartitionType::Unpartitioned),
            vec![edge.get()],
        );

        let prepared_host = host.clone();
        let prepared_descriptor = descriptor.clone();
        let prepared_facts = facts.clone();
        inspect_native_receiver(
            &host,
            facts,
            descriptor.clone(),
            body.input(&descriptor),
            None,
            move |result| {
                let host = &prepared_host;
                let descriptor = &prepared_descriptor;
                let facts = &prepared_facts;
                let prepared = result
                    .as_ref()
                    .expect("the actual consistent Native receiver prepares");
                assert_eq!(
                    prepared.facts().sink_kind(),
                    FragmentSinkKind::DataStream,
                    "the prepared facts report the validated static sink"
                );
                assert_eq!(facts.compiled_filter_bindings.lock().unwrap().len(), 1);

                // An edge the descriptor never froze has no gate, so granting it would
                // be inventing a destination.
                let unknown = host
                    .apply_task_domain(
                        descriptor,
                        &TaskDomainUpdate::OpenExchangeEdges {
                            version: EdgeOpenVersion::FIRST,
                            edges: vec![ExchangeEdgeId::new(9).expect("nonzero edge")],
                        },
                        TaskOperationId::new_v7(),
                    )
                    .expect_err("an unfrozen edge cannot be opened");
                use novarocks_execution_contract::{
                    DomainConflict,
                    failure::{FailureClass, domain},
                };
                let conflict = assert_host_refusal_source::<DomainConflict>(
                    &unknown,
                    TaskFailureCategory::Protocol,
                    expected_refusal_fact(FailureClass::Invariant, domain::PROTOCOL, 1013),
                    "Invariant failure (reason 15:1013)",
                );
                assert_eq!(conflict, &DomainConflict::UnknownMember);
                assert_eq!(
                    conflict.to_string(),
                    "domain member is not part of the frozen descriptor"
                );

                host.apply_task_domain(
                    descriptor,
                    &TaskDomainUpdate::OpenExchangeEdges {
                        version: EdgeOpenVersion::FIRST,
                        edges: vec![edge],
                    },
                    TaskOperationId::new_v7(),
                )
                .expect("a frozen edge opens at version one");
            },
        );
        assert!(host.task_runtime(producer).is_none());
        host.remove_receiver(&descriptor);
        host.remove_receiver(&descriptor);
        host.root_producer_pool.shutdown().unwrap();
    }

    #[test]
    fn a_prepared_task_closes_only_its_exact_frozen_destination() {
        let facts = Arc::new(StubContextFacts::default());
        let host = Arc::new(host(Arc::clone(&facts)));
        let producer = identity(121, 1, 1);
        let consumer = identity(121, 2, 1);
        let edge = ExchangeEdgeId::new(1).expect("edge");
        let descriptor = descriptor_with(
            producer,
            UniqueId::new(261, 263),
            1,
            outbound_topology(edge, consumer),
        );
        let body = Body::with_sink(
            1,
            stream_sink(DataStreamPartitionType::Unpartitioned),
            vec![edge.get()],
        );
        let prepared_host = host.clone();
        let prepared_descriptor = descriptor.clone();
        let prepared_facts = facts.clone();
        inspect_native_receiver(
            &host,
            facts,
            descriptor.clone(),
            body.input(&descriptor),
            None,
            move |result| {
                let host = &prepared_host;
                let descriptor = &prepared_descriptor;
                let _facts = &prepared_facts;
                let _prepared = result
                    .as_ref()
                    .expect("the actual consistent Native receiver prepares");
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
                    host.apply_task_domain(
                        descriptor,
                        &close(replacement_process),
                        TaskOperationId::new_v7()
                    )
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

                host.apply_task_domain(descriptor, &close(consumer), TaskOperationId::new_v7())
                    .expect("exact destination closes");
                assert!(gate.is_normally_canceled());
                host.apply_task_domain(descriptor, &close(consumer), TaskOperationId::new_v7())
                    .expect("repeat close is harmless");
                host.apply_task_domain(
                    descriptor,
                    &TaskDomainUpdate::OpenExchangeEdges {
                        version: EdgeOpenVersion::FIRST,
                        edges: vec![edge],
                    },
                    TaskOperationId::new_v7(),
                )
                .expect("edge grant after close is recorded");
                assert!(
                    gate.is_normally_canceled(),
                    "opening the edge cannot revive a closed destination"
                );
            },
        );
        assert!(host.task_runtime(producer).is_none());
        host.remove_receiver(&descriptor);
        host.remove_receiver(&descriptor);
        host.root_producer_pool.shutdown().unwrap();
    }

    #[test]
    fn a_task_dynamic_filter_is_handed_to_the_query_context_participant() {
        let facts = Arc::new(StubContextFacts::default());
        let host = Arc::new(host(Arc::clone(&facts)));
        let task = identity(22, 1, 1);
        let kernel_key = UniqueId::new(171, 172);
        let descriptor = consistent_descriptor(task, kernel_key);
        let prepared_host = host.clone();
        let prepared_descriptor = descriptor.clone();
        let prepared_facts = facts.clone();
        inspect_native_receiver(
            &host,
            facts,
            descriptor.clone(),
            Body::values(1).input(&descriptor),
            None,
            move |result| {
                result
                    .as_ref()
                    .expect("the actual Native receiver prepares");
                let host = &prepared_host;
                let descriptor = &prepared_descriptor;
                let facts = &prepared_facts;

                // The filter participant belongs to the query context, not to this
                // task: delivering it here would create a second owner of the same
                // installed contract.
                host.apply_task_domain(
                    descriptor,
                    &TaskDomainUpdate::TaskDynamicFilter {
                        version: DomainVersion::new(1).expect("nonzero"),
                        payload: Arc::new(novarocks_task_codec::domain::WireContent::new(
                            b"filter",
                            novarocks_proto_models::filter::RuntimeFilterEnvelope::default(),
                        )),
                    },
                    TaskOperationId::new_v7(),
                )
                .expect("the context host accepts it");
                assert_eq!(facts.dynamic_filters_delivered.load(Ordering::SeqCst), 1);
            },
        );
        assert!(host.task_runtime(task).is_none());
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
        let host = Arc::new(host(Arc::clone(&facts)));
        let task = identity(27, 1, 1);
        let descriptor = consistent_descriptor(task, UniqueId::new(221, 222));
        let prepared_host = host.clone();
        let prepared_descriptor = descriptor.clone();
        inspect_native_receiver(
            &host,
            facts,
            descriptor.clone(),
            Body::values(1).input(&descriptor),
            None,
            move |result| {
                result
                    .as_ref()
                    .expect("the actual Native receiver prepares");
                let host = &prepared_host;
                let descriptor = &prepared_descriptor;

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
                    .apply_task_domain(descriptor, &update, TaskOperationId::new_v7())
                    .expect_err("a filter envelope is not a split batch");
                assert_native_preparation_refusal(
                    &rejection,
                    NativePreparationReason::WrongSplitCarrier,
                    "not a split assignment",
                );
            },
        );
        assert!(
            host.task_runtime(task).is_none(),
            "original Node cleanup retires the prepared receiver"
        );
    }

    #[test]
    fn a_split_intent_whose_payload_names_another_plan_node_is_refused() {
        let facts = Arc::new(StubContextFacts::default());
        let host = Arc::new(host(Arc::clone(&facts)));
        let task = identity(28, 1, 1);
        let descriptor = consistent_descriptor(task, UniqueId::new(231, 232));
        let prepared_host = host.clone();
        let prepared_descriptor = descriptor.clone();
        inspect_native_receiver(
            &host,
            facts,
            descriptor.clone(),
            Body::values(1).input(&descriptor),
            None,
            move |result| {
                result
                    .as_ref()
                    .expect("the actual Native receiver prepares");
                let host = &prepared_host;
                let descriptor = &prepared_descriptor;

                // The intent's node is what the domain classifier advanced a
                // watermark for. Delivering the payload to the node the payload names
                // instead would move a different node's queue than the one the
                // receipt reports.
                let rejection = host
                    .apply_task_domain(
                        descriptor,
                        &split_intent(
                            PlanNodeId::new(10).expect("nonnegative node"),
                            11,
                            Vec::new(),
                            SplitOffer::Seal,
                        ),
                        TaskOperationId::new_v7(),
                    )
                    .expect_err("two plan nodes is not one assignment");
                assert_native_preparation_refusal(
                    &rejection,
                    NativePreparationReason::SplitIntentNodeMismatch,
                    "but its payload names",
                );
            },
        );
        assert!(
            host.task_runtime(task).is_none(),
            "original Node cleanup retires the prepared receiver"
        );
    }

    #[test]
    fn a_standalone_terminal_marker_seals_a_node_that_never_bound_a_reader() {
        let facts = Arc::new(StubContextFacts::default());
        let host = Arc::new(host(Arc::clone(&facts)));
        let task = identity(29, 1, 1);
        let descriptor = consistent_descriptor(task, UniqueId::new(241, 242));
        let prepared_host = host.clone();
        let prepared_descriptor = descriptor.clone();
        inspect_native_receiver(
            &host,
            facts,
            descriptor.clone(),
            Body::values(1).input(&descriptor),
            None,
            move |result| {
                result
                    .as_ref()
                    .expect("the actual Native receiver prepares");
                let host = &prepared_host;
                let descriptor = &prepared_descriptor;

                // An assignment with no splits carries only the terminal marker. It
                // must not demand a typed read execution: a node that was pruned to
                // zero splits still has to be sealed, or its scan waits forever.
                host.apply_task_domain(
                    descriptor,
                    &split_intent(
                        PlanNodeId::new(10).expect("nonnegative node"),
                        10,
                        Vec::new(),
                        SplitOffer::Seal,
                    ),
                    TaskOperationId::new_v7(),
                )
                .expect("a terminal marker needs no provider payload");
            },
        );
        assert!(
            host.task_runtime(task).is_none(),
            "original Node cleanup retires the prepared receiver"
        );
    }

    #[test]
    fn splits_for_a_plan_node_with_no_typed_read_execution_are_refused() {
        let facts = Arc::new(StubContextFacts::default());
        let host = Arc::new(host(Arc::clone(&facts)));
        let task = identity(30, 1, 1);
        let descriptor = consistent_descriptor(task, UniqueId::new(251, 252));
        let prepared_host = host.clone();
        let prepared_descriptor = descriptor.clone();
        inspect_native_receiver(
            &host,
            facts,
            descriptor.clone(),
            Body::values(1).input(&descriptor),
            None,
            move |result| {
                result
                    .as_ref()
                    .expect("the actual Native receiver prepares");
                let host = &prepared_host;
                let descriptor = &prepared_descriptor;

                // This fragment's only node is a VALUES node, so nothing registered a
                // connector read. Enqueuing the split anyway would fill a queue that
                // no scan operator will ever drain.
                let rejection = host
            .apply_task_domain(
                descriptor,
                &split_intent(
                    PlanNodeId::new(10).expect("nonnegative node"),
                    10,
                    vec![novarocks_native_adapter::typed_connector_test_support::test_support::split_proto(
                        10, 1,
                    )],
                    SplitOffer::batch(SplitSequence::FIRST, SplitSequence::FIRST, false)
                        .expect("one split"),
                ),
                TaskOperationId::new_v7(),
            )
            .expect_err("no typed connector read execution exists");
                assert_native_preparation_refusal(
                    &rejection,
                    NativePreparationReason::UnknownSplitRead,
                    "no typed connector read execution",
                );
            },
        );
        assert!(
            host.task_runtime(task).is_none(),
            "original Node cleanup retires the prepared receiver"
        );
    }

    // ------------------------------------------------------ end-to-end submit

    /// Waits for a task to reach a terminal status, or gives up.
    ///
    /// Completion is published by the process owner asynchronously. The bound
    /// only exists so a hang fails the test instead of blocking the suite.

    #[test]
    fn submitting_a_task_offers_it_as_the_context_dynamic_filter_carrier() {
        let fixture = OwnerFixture::new(26);
        let task = fixture.identity(1);
        let descriptor = consistent_descriptor(task, UniqueId::new(213, 214));
        let reply = fixture.create(&descriptor, &Body::values(1));
        assert_eq!(reply.outcome(), OperationOutcome::Accepted);
        wait_bounded_root_finished(&fixture, task);
        assert_eq!(
            *fixture.facts.feedback_carriers.lock().unwrap(),
            vec![task],
            "actual Native submission must offer its original weak RF writer"
        );
        fixture.abort();
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
                TaskFailureCategory::Internal,
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
            assert_eq!(super::failure_category(original.cause()), expected,);
        }
        let original = ExecutionFailure::from(novarocks_functions::KernelFailure::Cancelled);
        let forwarded =
            ExecutionFailure::from(ConstructionFailure::Caller(Box::new(original.clone())));
        assert_eq!(forwarded, original);
        assert_eq!(
            super::failure_category(forwarded.cause()),
            TaskFailureCategory::Execution,
        );
    }

    fn execution_failure_wire_value(
        task: TaskIdentity,
        origin: &ExecutionFailure,
    ) -> novarocks_execution_contract::TaskStatus {
        use novarocks_execution_contract::failure::FailureProjection;
        use novarocks_execution_contract::{TaskFailure, TaskStatus, TaskStatusVersion};
        let fact = origin.failure_fact();
        TaskStatus::try_new_with_installed(
            task,
            TaskStatusVersion::FIRST,
            TaskState::Failed,
            Some(TerminationDetail::Failed(TaskFailure::new(
                fact,
                origin.safe_detail_for_fact(fact),
            ))),
            TaskOutputFacts::default(),
            true,
        )
        .expect("legal neutral codec fixture")
    }

    #[test]
    fn value_representation_keeps_complete_fact_original_source_site_and_native_status_wire() {
        use novarocks_execution::runtime::fragment::{
            FragmentLaunchError, FragmentLaunchErrorKind, FragmentLaunchStage, PipelineOperation,
        };
        use novarocks_execution_contract::failure::{
            FailureClass, FailureEvidence, FailureProjection, RepresentationBoundary,
        };
        use novarocks_functions::array_projection::ArrayProjectionFailure;
        use novarocks_value_arrays::{ByteLayout, ValueError};
        for (index, boundary) in [
            RepresentationBoundary::MinimumUnit,
            RepresentationBoundary::SingleOutput,
        ]
        .into_iter()
        .enumerate()
        {
            let representation = ByteLayout::Utf8.representation(
                boundary,
                Some(i32::MAX as u64 + 1),
                Some(i32::MAX as u64),
                Some(index as u64),
            );
            let source = ValueError::Representation(representation);
            let expected = source.failure_fact();
            let kernel =
                novarocks_functions::KernelFailure::from(ArrayProjectionFailure::Value(source));
            let failure = ExecutionFailure::from(kernel).at_operator(7, PipelineOperation::Push);
            let source_pointer = crate::backend_test_support::find_source::<ValueError>(&failure)
                .unwrap() as *const _;
            assert_eq!(failure.failure_fact(), expected);
            assert_eq!(expected.class(), FailureClass::Representation);
            assert_eq!(expected.evidence(), FailureEvidence::Originating);
            assert_eq!(
                super::failure_category(&failure),
                TaskFailureCategory::Execution
            );
            let forwarded = ExecutionFailure::from(
                novarocks_execution::exec::pipeline::operator::ConstructionFailure::Caller(
                    Box::new(failure.clone()),
                ),
            );
            assert!(std::ptr::eq(forwarded.cause(), failure.cause()));
            assert_eq!(forwarded.context(), failure.context());
            let launch = FragmentLaunchError::from_failure(
                FragmentLaunchStage::BuildPipelines,
                FragmentLaunchErrorKind::PipelineBuild,
                failure.clone(),
            );
            let task = identity(80_030 + index as i64, 1, 1);
            let host = super::preparation_failure_to_host(task, &launch);
            assert_eq!(host.failure_fact(), expected);
            assert_eq!(
                crate::backend_test_support::find_source::<ValueError>(&host).unwrap() as *const _,
                source_pointer
            );
            // Source forwarding and wire projection are pure Native concerns;
            // peer refinement belongs to the original ContextCore tests.
            let origin = &forwarded;
            assert_eq!(origin.failure_fact(), expected);
            assert_eq!(origin.context(), failure.context());
            assert_eq!(origin.context().unwrap().operator_ordinal, 7);
            assert_eq!(origin.context().unwrap().operation, PipelineOperation::Push);
            assert_eq!(
                crate::backend_test_support::find_source::<ValueError>(origin).unwrap() as *const _,
                source_pointer
            );
            let status = execution_failure_wire_value(task, origin);
            let wire = novarocks_task_codec::status::encode_task_status(&status);
            let decoded = novarocks_task_codec::status::decode_task_status(
                &wire,
                FieldPath::root("value_status"),
            )
            .unwrap();
            let Some(TerminationDetail::Failed(wire_failure)) = decoded.termination() else {
                panic!("native status lost value failure")
            };
            assert_eq!(wire_failure.fact(), expected);
            assert_eq!(wire_failure.fact().representation(), Some(representation));
            assert_eq!(wire_failure.category(), TaskFailureCategory::Execution);
            assert!(wire_failure.detail().as_str().contains("operator 7"));
        }
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
                TaskFailureCategory::Internal,
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
                    .with_cleanup_diagnostics(vec![
                        KernelFailure::Internal(KernelDiagnostic::new(
                            "ResourceExhausted: cleanup failed",
                        ))
                        .into(),
                    ]);
                let task = identity(80_024, 1, 1);
                let refusal = super::preparation_failure_to_host(task, &error);
                assert_eq!(refusal.category(), expected);
                assert!(refusal.detail().as_str().contains(&task.to_string()));
                assert!(refusal.detail().as_str().contains(&stage.to_string()));
                assert!(refusal.detail().as_str().contains(&kind.to_string()));
                assert!(
                    refusal
                        .detail()
                        .as_str()
                        .contains(&format!("reason {}", error.cause().failure_fact().reason()))
                );
                assert!(
                    !refusal
                        .detail()
                        .as_str()
                        .contains("ResourceExhausted: cleanup failed")
                );
                assert!(refusal.detail().as_str().len() <= 512);
                // Follow the actual public Error chain rather than accepting
                // a formatted copy as evidence that the source survived.
                use novarocks_execution_contract::failure::FailureProjection;
                use std::error::Error;
                let outer = refusal
                    .source()
                    .unwrap()
                    .downcast_ref::<super::ExecutionFailure>()
                    .unwrap();
                assert_eq!(outer.failure_fact(), error.failure_fact());
                let mut source = refusal.source();
                let mut saw_launch = false;
                let mut saw_original = false;
                while let Some(current) = source {
                    if let Some(carried) = current.downcast_ref::<FragmentLaunchError>() {
                        saw_launch = true;
                        assert_eq!((carried.stage(), carried.kind()), (stage, kind));
                        assert!(std::ptr::eq(carried.cause().cause(), error.cause().cause()));
                        assert_eq!(
                            carried.cleanup_diagnostics().len(),
                            error.cleanup_diagnostics().len()
                        );
                        for (held, original) in carried
                            .cleanup_diagnostics()
                            .iter()
                            .zip(error.cleanup_diagnostics())
                        {
                            assert!(std::ptr::eq(held.cause(), original.cause()));
                        }
                    }
                    if let Some(carried) = current.downcast_ref::<super::ExecutionFailure>() {
                        saw_original |= std::ptr::eq(carried.cause(), error.cause().cause());
                    }
                    source = current.source();
                }
                assert!(saw_launch && saw_original);
                assert_eq!(
                    error.cause().cause(),
                    &super::ExecutionFailureCause::Kernel(cause.clone())
                );
            }
        }
        // An opaque operational diagnostic has no external-dependency or
        // capacity fact. Its author projects Unexpected, independent of text,
        // launch stage and secondary cleanup.
        for text in [
            "Internal: identical diagnostic text",
            "Cancelled",
            "required expression row failed",
        ] {
            let error = FragmentLaunchError::from_failure(
                FragmentLaunchStage::BuildPipelines,
                FragmentLaunchErrorKind::PipelineBuild,
                unexpected_fixture_failure(text),
            );
            assert_eq!(
                super::preparation_failure_to_host(identity(80_025, 1, 1), &error).category(),
                TaskFailureCategory::Internal,
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

    fn assert_original_fragment_source(
        origin: &ExecutionFailure,
        original: &FragmentExecutionError,
    ) {
        let actual = crate::backend_test_support::find_source::<FragmentExecutionError>(origin)
            .expect("actual FragmentExecutionError remains in the source chain");
        assert_eq!(actual.kind(), original.kind());
        assert!(std::ptr::eq(
            actual.cause().cause(),
            original.cause().cause()
        ));
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
            (
                RuntimeFilterFailure::Contract(RuntimeFilterContractViolation::new(
                    RuntimeFilterContractViolationKind::ContractMismatch,
                    "ResourceExhausted: kernel evaluation was cancelled",
                )),
                TaskFailureCategory::Internal,
            ),
            (
                RuntimeFilterFailure::Boundary(AggregateTopNBoundaryError::BoundLoosened),
                TaskFailureCategory::Internal,
            ),
            (
                RuntimeFilterFailure::Tuple(OrderedTupleError::TypeMismatch),
                TaskFailureCategory::Internal,
            ),
            (
                RuntimeFilterFailure::Codec(ContributionCodecError::ResourceLimit),
                TaskFailureCategory::ResourceExhausted,
            ),
        ];
        for (index, (original, expected)) in cases.into_iter().enumerate() {
            let failure: ExecutionFailure = original.clone().into();
            assert_eq!(
                failure.cause(),
                &ExecutionFailureCause::RuntimeFilter(original.clone())
            );
            assert_eq!(super::failure_category(failure.cause()), expected);
            let error =
                FragmentExecutionError::from_failure(FragmentExecutionErrorKind::Pipeline, failure);
            let origin = ExecutionFailure::from_domain(error.clone());
            let status =
                execution_failure_wire_value(identity(80_060 + index as i64, 1, 1), &origin);
            let Some(TerminationDetail::Failed(failure)) = status.termination() else {
                panic!("original nominal failure must be terminal")
            };
            assert_eq!(failure.category(), expected);
            assert_eq!(failure.phase(), TaskFailurePhase::Execution);
            use novarocks_execution_contract::failure::FailureProjection;
            assert_original_fragment_source(&origin, &error);
            assert_eq!(origin.failure_fact(), error.failure_fact());
            assert!(
                failure
                    .detail()
                    .as_str()
                    .contains(&format!("reason {}", error.failure_fact().reason()))
            );
            assert!(
                !failure
                    .detail()
                    .as_str()
                    .contains("ResourceExhausted: kernel evaluation was cancelled")
            );
        }
    }

    #[test]
    fn typed_failure_native_pipeline_text_does_not_claim_kernel_resource_category() {
        let error = FragmentExecutionError::from_failure(
            FragmentExecutionErrorKind::Pipeline,
            unexpected_fixture_failure(
                "kernel evaluation resources were exhausted: ResourceExhausted",
            ),
        );
        assert_eq!(
            super::failure_category(&error),
            TaskFailureCategory::Internal
        );
        let origin = ExecutionFailure::from_domain(error);
        let status = execution_failure_wire_value(identity(80_020, 1, 1), &origin);
        let Some(TerminationDetail::Failed(failure)) = status.termination() else {
            panic!("expected the explicit failure carrier")
        };
        assert_eq!(failure.category(), TaskFailureCategory::Internal);
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
        let error = FragmentExecutionError::from_failure(
            FragmentExecutionErrorKind::Pipeline,
            required.into(),
        );
        let origin = ExecutionFailure::from_domain(error.clone());
        let status = execution_failure_wire_value(identity(80_021, 1, 1), &origin);
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
            !failure
                .detail()
                .as_str()
                .contains("required root cannot publish a placeholder NULL")
        );
        assert_original_fragment_source(&origin, &error);
        use novarocks_execution_contract::failure::{FailureProjection, FailureRowPosition};
        let origin = &origin;
        assert_eq!(
            origin.failure_fact().row_position(),
            Some(FailureRowPosition {
                selected_ordinal: 1,
                batch_row: 50
            })
        );
        use std::error::Error;
        let mut source = origin.source();
        let mut saw_row = false;
        while let Some(current) = source {
            if let Some(row) = current.downcast_ref::<RequiredExpressionRowError>() {
                saw_row = true;
                assert_eq!(row.batch_row(), 50);
                assert_eq!(row.error().selected_ordinal(), 1);
                assert_eq!(
                    row.error().message(),
                    "required root cannot publish a placeholder NULL"
                );
            }
            source = current.source();
        }
        assert!(saw_row);
    }

    #[test]
    fn receiver_construction_abort_remains_a_stop_observation() {
        use novarocks_execution_contract::failure::{FailureEvidence, FailureProjection};
        let observed =
            ExecutionFailure::from_domain(ReceiverConstructionAborted(AbortCause::PeerTaskFailed));
        assert_eq!(
            observed.failure_fact().evidence(),
            FailureEvidence::StopObservation
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
        fn register_with_close_observer(
            &self,
            registration: novarocks_execution::runtime::fragment::io::ExchangeReceiverRegistration,
        ) -> Result<
            novarocks_execution::runtime::exchange::ExchangeReceiverCloseObserver,
            novarocks_execution::runtime::exchange::ExchangeFailure,
        > {
            let schema = registration.expected_chunk_schema.clone();
            let original = self.inner.register_with_close_observer(registration)?;
            *self.expected_schema.lock().unwrap() = Some(schema);
            Ok(original)
        }
        fn close_observer(
            &self,
            key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey,
        ) -> Result<
            novarocks_execution::runtime::exchange::ExchangeReceiverCloseObserver,
            novarocks_execution::runtime::exchange::ExchangeFailure,
        > {
            self.inner.close_observer(key)
        }

        fn register(
            &self,
            registration: novarocks_execution::runtime::fragment::io::ExchangeReceiverRegistration,
        ) -> Result<(), novarocks_execution::runtime::exchange::ExchangeFailure> {
            let schema = registration.expected_chunk_schema.clone();
            self.inner.register(registration)?;
            *self.expected_schema.lock().unwrap() = Some(schema);
            Ok(())
        }
        fn bind_unit_receiver(
            &self,
            key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey,
            streams: Vec<novarocks_execution_contract::ExchangeStreamIdentity>,
            control: Arc<novarocks_execution::runtime::runtime_state::RuntimeErrorState>,
            max_bytes: u64,
        ) -> Result<(), novarocks_execution::runtime::exchange::ExchangeFailure> {
            self.inner
                .bind_unit_receiver(key, streams, control, max_bytes)
        }
        fn register_unit_demand(
            &self,
            key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey,
            entry: novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeDemandEntry,
        ) -> Result<
            novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeStreamObservation,
            novarocks_execution_contract::failure::FailureFact,
        > {
            self.inner.register_unit_demand(key, entry)
        }
        fn receive_unit(
            &self,
            key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey,
            frame:novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeUnitReceiverFrame,
        ) -> Result<
            novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeUnitReceipt,
            novarocks_execution_contract::failure::FailureFact,
        > {
            self.inner.receive_unit(key, frame)
        }
        fn unit_sender_states(&self, sender:novarocks_types::BackendProcessId,
            after:Option<novarocks_execution_contract::ExchangeStreamIdentity>,limit:usize,
        ) -> Result<Vec<novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeStreamObservation>,novarocks_execution_contract::failure::FailureFact>{
            self.inner.unit_sender_states(sender, after, limit)
        }
        fn set_unit_state_listener(
            &self,
            listener:Arc<dyn novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeUnitStateListener>,
        ) -> Result<(), novarocks_execution_contract::failure::FailureFact> {
            self.inner.set_unit_state_listener(listener)
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
        ) -> Result<
            novarocks_execution::runtime::exchange::ExchangeReceiverHandle,
            novarocks_execution::runtime::exchange::ExchangeFailure,
        > {
            self.inner.receiver_handle(key, expected)
        }
        fn stream_close_facts(
            &self,
            key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey,
            expected: &[novarocks_execution_contract::ExchangeStreamIdentity],
        ) -> Result<
            Vec<novarocks_execution_contract::ExchangeStreamClose>,
            novarocks_execution::runtime::exchange::ExchangeFailure,
        > {
            self.inner.stream_close_facts(key, expected)
        }
        fn ensure_mem_tracker(
            &self,
            key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey,
            root: &Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
        ) -> Result<
            Arc<novarocks_execution::runtime::mem_tracker::MemTracker>,
            novarocks_execution::runtime::exchange::ExchangeFailure,
        > {
            self.inner.ensure_mem_tracker(key, root)
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

    fn receiver_unit_frame(
        stream: novarocks_execution_contract::ExchangeStreamIdentity,
        payload: Vec<u8>,
        end: bool,
    ) -> novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeUnitReceiverFrame {
        use novarocks_execution_contract::ordered_retained_stream::exchange::{
            ExchangeUnitDemand, ExchangeUnitItem, ExchangeUnitSequence,
        };
        let item = if payload.is_empty() {
            ExchangeUnitItem::End(ExchangeUnitSequence::FIRST)
        } else {
            ExchangeUnitItem::Unit {
                demand: ExchangeUnitDemand {
                    sequence: ExchangeUnitSequence::FIRST,
                    wire_bytes: payload.len() as u64,
                },
                end_after_unit: end,
            }
        };
        novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeUnitReceiverFrame {
            stream,
            sender_count: 1,
            sender_id: 0,
            backend_number: 0,
            item,
            payload: bytes::Bytes::from(payload),
            next_demand: None,
        }
    }
    fn register_receiver_unit(
        port: &dyn novarocks_execution::runtime::fragment::io::ExchangeReceiverPort,
        key: novarocks_execution::runtime::fragment::io::ExchangeReceiverKey,
        frame:&novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeUnitReceiverFrame,
    ) {
        use novarocks_execution::runtime::fragment::io::exchange_unit::{
            ExchangeDemandEntry, ExchangeDemandOperation,
        };
        if let novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeUnitItem::Unit{demand,..}=frame.item {
            port.register_unit_demand(key,ExchangeDemandEntry{stream:frame.stream,operation:ExchangeDemandOperation::Register(demand)}).unwrap();
        }
    }

    #[test]
    fn native_strict_input_validation_reads_actual_data_and_empty_end_consumption() {
        use novarocks_execution::runtime::fragment::io::{
            ExchangeReceiverKey, ExchangeReceiverRegistration,
        };
        use novarocks_execution_contract::{
            ExchangeCloseDisposition, ExchangeDeliveryRequirement, ExchangeInbound, ExchangeSource,
            ExchangeStreamIdentity,
        };
        struct Control;
        impl novarocks_type_contract::PureCompileControl for Control {
            fn checkpoint(
                &self,
                _: novarocks_type_contract::CompilePhase,
                _: u32,
            ) -> Result<(), novarocks_type_contract::CompileControlError> {
                Ok(())
            }
        }
        for data in [false, true] {
            let (mut host, _) = isolated_resource_host();
            host.exchange_receiver_port = Arc::new(
                novarocks_execution::runtime::fragment::io::exchange_receiver::ExecutionRuntimeExchangeReceiverPort::new(
                    Arc::clone(&host.execution_runtime),
                ),
            );
            let task = identity(84090 + i64::from(data), 2, 1);
            let source_task = identity(84090 + i64::from(data), 1, 1);
            let kernel = UniqueId::new(84090, 2);
            let source_kernel = UniqueId::new(84090, 1);
            let node = FragmentNodeId::new(73);
            let source = ExchangeSource::new(source_task, source_kernel, 0);
            let topology = ExchangeTopology::try_new(
                Vec::new(),
                vec![
                    ExchangeInbound::try_new(
                        node,
                        vec![source],
                        ExchangeDeliveryRequirement::MustDrain,
                    )
                    .unwrap(),
                ],
            )
            .unwrap();
            let descriptor = descriptor_with(task, kernel, 1, topology);
            let port = host.exchange_receiver_port.clone();
            let tracker = novarocks_execution::runtime::mem_tracker::MemTracker::new_root(
                "strict input close fixture",
            );
            let factory = novarocks_execution::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
                novarocks_execution::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                tracker, Arc::new(novarocks_execution::runtime::runtime_state::RuntimeErrorState::default()),
            ).unwrap();
            let ty = novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Int64,
                false,
            );
            let declaration = novarocks_local_program::StaticLayout::try_new_typed_for_compile(
                Arc::new(arrow::datatypes::Schema::new(vec![
                    arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, false),
                ])),
                Arc::from([novarocks_types::SlotId::new(1)]),
                vec![(ty.typed_binding().clone(), None)],
                novarocks_type_contract::ValueTypeStorageLifetime::pure(),
                |_| Ok(()),
                &Control,
            )
            .unwrap();
            let schema = factory.create_compiled_chunk_schema(&declaration).unwrap();
            let key = ExchangeReceiverKey {
                fragment_instance_id: kernel,
                node_id: node.get(),
            };
            port.register(ExchangeReceiverRegistration {
                key,
                expected_senders: 1,
                expected_chunk_schema: schema.clone(),
                column_binding:
                    novarocks_execution::runtime::exchange::ExchangeColumnBinding::Positional,
                creation_factory: factory,
            })
            .unwrap();
            let stream = ExchangeStreamIdentity::try_new(source, task, kernel, node).unwrap();
            let payload = if data {
                let chunk = novarocks_execution::exec::chunk::Chunk::try_new_with_columns(
                    schema,
                    vec![Arc::new(arrow::array::Int64Array::from(vec![7]))],
                )
                .unwrap();
                novarocks_execution::runtime::exchange::encode_chunks(&[chunk], true).unwrap()
            } else {
                Vec::new()
            };
            let preparation = super::super::tests::node_fixture::NativeNodePreparation::new(task);
            let control = Arc::new(
                novarocks_execution::runtime::runtime_state::RuntimeErrorState::for_task(
                    preparation.execution_control(),
                ),
            );
            port.bind_unit_receiver(key,vec![stream],control,novarocks_execution_contract::native_result_support::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES as u64).unwrap();
            let frame = receiver_unit_frame(stream, payload, true);
            register_receiver_unit(port.as_ref(), key, &frame);
            port.receive_unit(key, frame).unwrap();
            let closes = native_inbound_close_facts(port.as_ref(), &descriptor).unwrap();
            assert_eq!(closes[0].disposition, ExchangeCloseDisposition::Withdrawn);
            assert!(validate_native_strict_inputs(&descriptor, &closes).is_err());
            let receiver = port.receiver_handle(key, 1).unwrap();
            assert!(receiver.has_output_or_finished(1));
            assert!(
                validate_native_strict_inputs(
                    &descriptor,
                    &native_inbound_close_facts(port.as_ref(), &descriptor).unwrap()
                )
                .is_err(),
                "readiness cannot author consumption"
            );
            if data {
                assert!(
                    receiver.complete_input_consumption(1).is_err(),
                    "unread DATA+End cannot be consumed"
                );
                assert!(matches!(
                    receiver.try_pop_next_with_stats(1).unwrap(),
                    Some(novarocks_execution::runtime::exchange::ExchangePopResult::Chunk(_))
                ));
            }
            receiver.complete_input_consumption(1).unwrap();
            let closes = native_inbound_close_facts(port.as_ref(), &descriptor).unwrap();
            assert!(matches!(
                closes[0].disposition,
                ExchangeCloseDisposition::Consumed { .. }
            ));
            validate_native_strict_inputs(&descriptor, &closes).unwrap();
            port.remove(key);
            assert!(
                native_inbound_close_facts(port.as_ref(), &descriptor).is_err(),
                "missing receiver cannot imply Consumed"
            );
        }
    }

    fn receiver_retirement_case(
        mut native: NativeTaskExecutionHost,
        descriptor: TaskDescriptor,
        input: TaskCreationInput,
        stop: ReceiverStop,
        nonempty_data: bool,
    ) {
        use super::super::tests::node_fixture::{
            NativeNodeFixture, await_original, factory_from_host,
        };
        use novarocks_execution::runtime::fragment::io::{
            ExchangeReceiverKey, ExchangeReceiverPort,
        };
        let port = Arc::new(RetirementPort {
            inner: Arc::new(novarocks_execution::runtime::fragment::io::exchange_receiver::ExecutionRuntimeExchangeReceiverPort::new(native.execution_runtime.clone())),
            removed: AtomicUsize::new(0), cancelled: AtomicUsize::new(0), expected_schema: Mutex::new(None),
        });
        native.exchange_receiver_port = port.clone();
        let host = Arc::new(CountingHost::from_native(native));
        let context = QueryContextRef::new(
            descriptor.identity().query_execution_id(),
            FrontendProcessId::new_v7(),
            descriptor.identity().backend_process_id(),
        );
        let node = NativeNodeFixture::start(
            context.backend_process_id(),
            factory_from_host(Arc::new(AcceptingContextHost {
                queries: host.inner.queries.clone(),
                facts: host.inner.context_facts.clone(),
            })),
            host.clone(),
            host.inner.execution_runtime.execution_health(),
            1,
            1,
        );
        node.establish(context);
        let (started, reached_retirement) = std::sync::mpsc::sync_channel(1);
        let gate = Arc::new(PausedRootWorker {
            open: Mutex::new(false),
            changed: std::sync::Condvar::new(),
            started: Mutex::new(Some(started)),
        });
        let retirement_pause = CallbackPause(gate.clone());
        if matches!(stop, ReceiverStop::Rollback) {
            host.reject_submit.store(true, Ordering::Release);
            *host.remove_pause.lock().unwrap() = Some(gate);
        } else {
            *host.retire_pause.lock().unwrap() = Some(gate);
        }
        let commit_gate = matches!(stop, ReceiverStop::BeforeStart).then(|| {
            let gate = Arc::new(PausedRootWorker {
                open: Mutex::new(false),
                changed: std::sync::Condvar::new(),
                started: Mutex::new(None),
            });
            *host.commit_pause.lock().unwrap() = Some(gate.clone());
            CallbackPause(gate)
        });
        let request = CreateTask::try_new(
            TaskOperationId::new_v7(),
            context,
            descriptor.clone(),
            vec![],
        )
        .unwrap();
        let accepted = node.create(request, input);
        assert!(matches!(
            accepted.reply(),
            novarocks_worker::WorkerCreateReply::Context(
                novarocks_worker::WorkerContextReplyBody::Create {
                    outcome: OperationOutcome::Accepted,
                    ..
                }
            )
        ));
        drop(accepted);
        let until = std::time::Instant::now() + Duration::from_secs(5);
        let reporter = loop {
            if let Some(view) = host
                .reads
                .lock()
                .unwrap()
                .get(&descriptor.identity())
                .cloned()
            {
                break view;
            }
            assert!(
                std::time::Instant::now() < until,
                "original Native submit was not reached"
            );
            std::thread::sleep(Duration::from_millis(1));
        };
        if !matches!(stop, ReceiverStop::Rollback) {
            while !reporter.current().unwrap().installed() {
                assert!(
                    std::time::Instant::now() < until,
                    "original Installed publication did not commit"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        if matches!(stop, ReceiverStop::BeforeStart) {
            assert_eq!(
                reporter.current().unwrap().state(),
                TaskState::Planned,
                "actual Installed precedes the paused commit callback"
            );
            assert!(
                !reporter.convergence().unwrap().actual_stopped(),
                "a dormant prepared kernel is not an actual stop receipt"
            );
        }
        let runtime_alias = host
            .inner
            .task_runtime(descriptor.identity())
            .expect("the original receiver runtime remains held before retirement");
        let inbound = &descriptor.topology().inbound()[0];
        let source = inbound.sources()[0];
        let key = ExchangeReceiverKey {
            fragment_instance_id: descriptor.fragment_instance_id(),
            node_id: inbound.node_id().get(),
        };
        assert!(port.snapshot(key).is_some());
        let stream = novarocks_execution_contract::ExchangeStreamIdentity::try_new(
            source,
            descriptor.identity(),
            key.fragment_instance_id,
            inbound.node_id(),
        )
        .unwrap();
        let deliver = |unit: novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeUnitReceiverFrame| {
            register_receiver_unit(port.as_ref(), key, &unit);
            host.inner.capabilities.with_claimed_stream(unit.stream, |_| {
                host.inner.execution_runtime.exchange_registry().receive_unit(
                    novarocks_execution::runtime::exchange::ExchangeKey {
                        finst_id_hi: key.fragment_instance_id.high(),
                        finst_id_lo: key.fragment_instance_id.low(),
                        node_id: key.node_id,
                    },
                    unit,
                )
            })
        };
        let payload = nonempty_data.then(|| receiver_data_payload(&port));
        let control = node.control.clone();
        let deadline = node.deadline();
        let control_reply = if matches!(stop, ReceiverStop::Success) {
            if let Some(payload) = &payload {
                let delivered = deliver(receiver_unit_frame(stream, payload.clone(), false));
                assert!(matches!(delivered, Ok(Ok(_))));
            }
            let mut end = receiver_unit_frame(stream, Vec::new(), true);
            end.item = novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeUnitItem::End(
                novarocks_execution_contract::ordered_retained_stream::exchange::ExchangeUnitSequence::try_new(
                    if payload.is_some() { 2 } else { 1 },
                ).unwrap(),
            );
            port.receive_unit(key, end)
                .expect("actual exact sender End");
            None
        } else if matches!(stop, ReceiverStop::Rollback) {
            None
        } else {
            Some(std::thread::spawn(move || match stop {
                ReceiverStop::Abort => await_original(control.abort(
                    novarocks_execution_contract::AbortQueryContext::new(
                        TaskOperationId::new_v7(),
                        context,
                        AbortCause::QueryFailed,
                    ),
                    deadline,
                )),
                _ => await_original(control.quiesce(
                    novarocks_execution_contract::QuiesceQueryContext::new(
                        TaskOperationId::new_v7(),
                        context,
                    ),
                    deadline,
                )),
            }))
        };
        if let Some(commit_gate) = commit_gate {
            while !reporter.current().unwrap().state().is_cancellation() {
                assert!(
                    std::time::Instant::now() < until,
                    "original cancellation was not published before the start callback"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            drop(commit_gate);
        }
        reached_retirement
            .recv_timeout(until.saturating_duration_since(std::time::Instant::now()))
            .expect("the original physical retirement callback");
        assert_eq!(port.removed.load(Ordering::SeqCst), 0);
        assert_eq!(port.cancelled.load(Ordering::SeqCst), 0);
        assert!(
            port.snapshot(key).is_some(),
            "actual runtime stop does not retire ingress"
        );
        if matches!(stop, ReceiverStop::Quiesce)
            && let Some(payload) = &payload
        {
            let delivered = deliver(receiver_unit_frame(stream, payload.clone(), false));
            assert!(
                matches!(delivered, Ok(Ok(_))),
                "DemandDriven normal withdrawal preserves the actual authorized DATA path until physical retirement: {delivered:?}"
            );
        }
        drop(retirement_pause);
        if let Some(reply) = control_reply {
            drop(
                reply
                    .join()
                    .expect("original control waiter")
                    .expect("original control delivery"),
            );
        }
        if matches!(stop, ReceiverStop::QuiesceThenAbort) {
            drop(
                await_original(node.control.abort(
                    novarocks_execution_contract::AbortQueryContext::new(
                        TaskOperationId::new_v7(),
                        context,
                        AbortCause::QueryFailed,
                    ),
                    node.deadline(),
                ))
                .unwrap(),
            );
        }
        let until = std::time::Instant::now() + Duration::from_secs(5);
        while port.snapshot(key).is_some() {
            assert!(
                std::time::Instant::now() < until,
                "original receiver was not physically removed"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            runtime_alias.exchange_retirement.lock().unwrap().is_none(),
            "retirement must not wait for TaskRuntime's last Arc"
        );
        let normal = matches!(
            stop,
            ReceiverStop::Success
                | ReceiverStop::Quiesce
                | ReceiverStop::QuiesceThenAbort
                | ReceiverStop::BeforeStart
        );
        if !matches!(stop, ReceiverStop::Rollback) {
            assert_eq!(
                *host.retired_mode.lock().unwrap(),
                Some(if normal {
                    novarocks_worker::TaskIngressRetirement::PreserveNormalClose
                } else {
                    novarocks_worker::TaskIngressRetirement::Remove
                })
            );
        }
        let claim = host.inner.capabilities.claim_frame(
            key.fragment_instance_id,
            inbound.node_id(),
            source.fragment_instance_id(),
            source.sender_ordinal(),
            1,
        );
        if normal && !matches!(stop, ReceiverStop::QuiesceThenAbort) {
            assert!(matches!(claim, InboundFrameClaim::NormallyClosed(_)));
        } else {
            assert_eq!(claim, InboundFrameClaim::NotHeld);
        }
        if normal && !matches!(stop, ReceiverStop::QuiesceThenAbort) {
            assert!(
                matches!(
                    host.inner.capabilities.claim_frame(
                        key.fragment_instance_id,
                        inbound.node_id(),
                        UniqueId::new(-1, -1),
                        0,
                        1
                    ),
                    InboundFrameClaim::Refused(_)
                ),
                "normal retention never authorizes a foreign sender identity"
            );
        }
        assert_eq!(
            port.removed.load(Ordering::SeqCst) + port.cancelled.load(Ordering::SeqCst),
            1,
            "one original registration exits exactly once"
        );
        drop(runtime_alias);
        drop(node);
        host.inner.root_producer_pool.shutdown().unwrap();
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

    /// Catches a progress tick that reports whole-fragment totals as if they
    /// were one operator's, instead of reading the operators' own counters.
    #[test]
    fn a_progress_tick_reports_the_operators_counters_not_the_fragment_totals() {
        let profiler = RuntimeProfile::new("execute_fragment_native (plan_node_id=10)");
        let common = profiler
            .child("Pipeline (id=0)")
            .child("PipelineDriver (id=0)")
            .child("ValuesSource (id=10)")
            .child("CommonMetrics");
        common.counter_set("PushRowNum", ProfileUnit::Unit, 0);
        common.counter_set("PullRowNum", ProfileUnit::Unit, 7);
        let actual =
            novarocks_execution::runtime::operator_statistics::project_operator_statistics(
                &profiler.to_native_tree(),
            )
            .into_statistics();
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].plan_node_id(), 10);
        assert_eq!(actual[0].operator().as_str(), "ValuesSource");
        assert_eq!(actual[0].output_rows(), Some(7));
    }

    /// Catches the whole production wiring: a profiled task must carry its
    /// operators' facts in the final info the frontend reads, keyed by the
    /// plan node the plan named. Before this producer existed the field was
    /// always empty, so an `EXPLAIN ANALYZE` over the task protocol had
    /// nothing to render.
    #[test]
    fn a_profiled_task_reports_its_operator_statistics_in_its_final_info() {
        let fixture = OwnerFixture::profiled(42);
        let task = fixture.identity(1);
        let descriptor = consistent_descriptor(task, UniqueId::new(271, 272));
        let reply = fixture.create(&descriptor, &Body::values(1));
        assert_eq!(reply.outcome(), OperationOutcome::Accepted);
        wait_bounded_root_finished(&fixture, task);
        let final_info = fixture.final_info(task);
        let entries = final_info.operator_statistics();
        assert_eq!(
            entries.len(),
            1,
            "only the actual plan-node-bearing Values operator is attributed"
        );
        assert_eq!(entries[0].plan_node_id(), 10);
        assert_eq!(entries[0].operator().as_str(), "ValuesSource");
        assert!(!final_info.operator_statistics_truncated());
        fixture.abort();
    }

    /// Catches a wiring that profiles every task regardless of what the query
    /// asked for. Profiling is the query's decision, and a task that was not
    /// asked to measure must report nothing rather than zeroes.
    #[test]
    fn an_unprofiled_task_reports_no_operator_statistics() {
        let fixture = OwnerFixture::new(43);
        let task = fixture.identity(1);
        let descriptor = consistent_descriptor(task, UniqueId::new(281, 282));
        let reply = fixture.create(&descriptor, &Body::values(1));
        assert_eq!(reply.outcome(), OperationOutcome::Accepted);
        wait_bounded_root_finished(&fixture, task);
        let final_info = fixture.final_info(task);
        assert!(final_info.operator_statistics().is_empty());
        assert!(!final_info.operator_statistics_truncated());
        fixture.abort();
    }

    // --------------------------------------- codec -> task owner -> this host

    /// This host behind the real task owner, counting how often the owner
    /// reaches its creation-time interpretation.
    struct CountingHost {
        inner: NativeTaskExecutionHost,
        installs: AtomicUsize,
        submit_refusal: Mutex<Option<TaskStatusReporter>>,
        reads: Mutex<BTreeMap<TaskIdentity, TaskStatusReporter>>,
        controls: Mutex<
            BTreeMap<
                TaskIdentity,
                Arc<novarocks_execution::runtime::task_control::TaskExecutionControl>,
            >,
        >,
        reject_submit: AtomicBool,
        abort_pause: Mutex<Option<Arc<PausedRootWorker>>>,
        commit_pause: Mutex<Option<Arc<PausedRootWorker>>>,
        retire_pause: Mutex<Option<Arc<PausedRootWorker>>>,
        remove_pause: Mutex<Option<Arc<PausedRootWorker>>>,
        retired_mode: Mutex<Option<novarocks_worker::TaskIngressRetirement>>,
    }

    impl CountingHost {
        fn from_native(inner: NativeTaskExecutionHost) -> Self {
            Self {
                inner,
                installs: AtomicUsize::new(0),
                submit_refusal: Mutex::new(None),
                reads: Mutex::new(BTreeMap::new()),
                controls: Mutex::new(BTreeMap::new()),
                reject_submit: AtomicBool::new(false),
                abort_pause: Mutex::new(None),
                commit_pause: Mutex::new(None),
                retire_pause: Mutex::new(None),
                remove_pause: Mutex::new(None),
                retired_mode: Mutex::new(None),
            }
        }
    }

    /// Holds the real runnable's abort delegation, not a readiness callback
    /// whose synchronous borrower can belong to another notifying thread.
    struct PausedNativeAbort {
        inner: Arc<dyn RunnableTask>,
        gate: Arc<PausedRootWorker>,
    }
    impl std::fmt::Debug for PausedNativeAbort {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("PausedNativeAbort")
                .field("inner", &self.inner)
                .finish()
        }
    }
    impl RunnableTask for PausedNativeAbort {
        fn commit_creation(&self) {
            self.inner.commit_creation();
        }
        fn cancel(&self, reason: CancelReason) {
            self.inner.cancel(reason);
        }
        fn quiesce(&self) {
            self.inner.quiesce();
        }
        fn abort(&self, cause: AbortCause) {
            if let Some(sender) = self.gate.started.lock().unwrap().take() {
                sender.send(()).unwrap();
            }
            let mut open = self.gate.open.lock().unwrap();
            while !*open {
                open = self.gate.changed.wait(open).unwrap();
            }
            drop(open);
            self.inner.abort(cause);
        }
    }

    fn wait_callback_gate(gate: &PausedRootWorker) {
        if let Some(sender) = gate.started.lock().unwrap().take() {
            let _ = sender.send(());
        }
        let mut open = gate.open.lock().unwrap();
        while !*open {
            open = gate.changed.wait(open).unwrap();
        }
    }
    struct PausedNativeStart {
        inner: Arc<dyn RunnableTask>,
        gate: Arc<PausedRootWorker>,
    }
    impl std::fmt::Debug for PausedNativeStart {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("PausedNativeStart").finish_non_exhaustive()
        }
    }
    impl RunnableTask for PausedNativeStart {
        fn commit_creation(&self) {
            wait_callback_gate(&self.gate);
            self.inner.commit_creation();
        }
        fn cancel(&self, reason: CancelReason) {
            self.inner.cancel(reason);
        }
        fn quiesce(&self) {
            self.inner.quiesce();
        }
        fn abort(&self, cause: AbortCause) {
            self.inner.abort(cause);
        }
    }

    impl TaskExecutionHost for CountingHost {
        fn abort_context_admission(&self, context: QueryContextRef) {
            self.inner.abort_context_admission(context);
        }
        fn retains_normal_close(&self, context: QueryContextRef) -> bool {
            self.inner.retains_normal_close(context)
        }
        fn retire_receiver_checked(
            &self,
            descriptor: &TaskDescriptor,
            retirement: novarocks_worker::TaskIngressRetirement,
        ) -> Result<(), HostRejection> {
            *self.retired_mode.lock().unwrap() = Some(retirement);
            if let Some(gate) = self.retire_pause.lock().unwrap().clone() {
                wait_callback_gate(&gate);
            }
            self.inner.retire_receiver_checked(descriptor, retirement)
        }

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
            _preparation_stop: novarocks_worker::TaskPreparationContext<'_>,
        ) -> Result<novarocks_worker::PreparedTaskInstallation, HostRejection> {
            self.controls
                .lock()
                .unwrap()
                .insert(descriptor.identity(), _preparation_stop.runtime_control());
            self.installs.fetch_add(1, Ordering::SeqCst);
            self.inner
                .install_receiver(descriptor, input, _preparation_stop)
        }

        fn remove_receiver(&self, descriptor: &TaskDescriptor) {
            if let Some(gate) = self.remove_pause.lock().unwrap().clone() {
                wait_callback_gate(&gate);
            }
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
            // The actual submit callback lends this read-only view; cloning
            // it retains no Task Work, source, or lifecycle author.
            self.reads
                .lock()
                .unwrap()
                .insert(descriptor.identity(), reporter.clone());
            if self.reject_submit.swap(false, Ordering::AcqRel) {
                *self.submit_refusal.lock().unwrap() = Some(reporter);
                return Err(internal_failure(NativePreparationContractFailure::new(
                    NativePreparationReason::TestControl,
                    "injected submit refusal after actual Native receiver preparation",
                )));
            }
            let inner = self.inner.submit_runnable(descriptor, reporter)?;
            let inner: Arc<dyn RunnableTask> = match self.commit_pause.lock().unwrap().clone() {
                Some(gate) => Arc::new(PausedNativeStart { inner, gate }),
                None => inner,
            };

            let gate = self.abort_pause.lock().unwrap().clone();
            Ok(match gate {
                Some(gate) => Arc::new(PausedNativeAbort { inner, gate }) as Arc<dyn RunnableTask>,
                None => inner,
            })
        }

        fn apply_task_domain(
            &self,
            descriptor: &TaskDescriptor,
            domain: &TaskDomainUpdate,
            operation: novarocks_execution_contract::TaskOperationId,
        ) -> Result<Option<u64>, HostRejection> {
            self.inner.apply_task_domain(descriptor, domain, operation)
        }
    }

    /// The fixture owns the actual query resources while keeping its unrelated
    /// catalog and runtime-filter ports closed, as those tests declare.
    struct AcceptingContextHost {
        queries: NativeFragmentQueryRuntime,
        facts: Arc<dyn TaskQueryContextFacts>,
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

    fn original_node_for(
        host: &Arc<CountingHost>,
        task: TaskIdentity,
    ) -> (
        super::super::tests::node_fixture::NativeNodeFixture,
        QueryContextRef,
    ) {
        let context = QueryContextRef::new(
            task.query_execution_id(),
            FrontendProcessId::new_v7(),
            task.backend_process_id(),
        );
        let node = super::super::tests::node_fixture::NativeNodeFixture::start(
            task.backend_process_id(),
            super::super::tests::node_fixture::factory_from_host(Arc::new(AcceptingContextHost {
                queries: host.inner.queries.clone(),
                facts: host.inner.context_facts.clone(),
            })),
            host.clone(),
            host.inner.execution_runtime.execution_health(),
            1,
            1,
        );
        node.establish(context);
        (node, context)
    }
    fn original_installed_read(host: &CountingHost, task: TaskIdentity) -> TaskStatusReporter {
        let until = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(read) = host.reads.lock().unwrap().get(&task).cloned()
                && read.current().unwrap().installed()
            {
                return read;
            }
            assert!(
                std::time::Instant::now() < until,
                "original Native installation did not publish"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    struct OriginalCreate(novarocks_worker::WorkerContextReplyBody);
    impl OriginalCreate {
        fn acknowledgement(&self) -> &novarocks_execution_contract::CreateTaskReceipt {
            match &self.0 {
                novarocks_worker::WorkerContextReplyBody::Create { receipt, .. } => receipt,
                _ => unreachable!("OriginalCreate is constructed only from the actual Create body"),
            }
        }

        fn outcome(&self) -> OperationOutcome {
            match &self.0 {
                novarocks_worker::WorkerContextReplyBody::Create { outcome, .. } => *outcome,
                _ => panic!("the original established Context must reply to Create"),
            }
        }
    }
    impl std::fmt::Debug for OriginalCreate {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_tuple("OriginalCreate")
                .field(&self.outcome())
                .finish()
        }
    }

    /// One established query context on a real task owner whose task host is
    /// this production host.
    struct OwnerFixture {
        node: super::super::tests::node_fixture::NativeNodeFixture,
        host: Arc<CountingHost>,
        context: QueryContextRef,
        manager: Arc<novarocks_worker::query_context::QueryContextManager>,
        facts: Arc<StubContextFacts>,
    }

    impl OwnerFixture {
        fn new(query: i64) -> Self {
            Self::new_with_context_dop(query, 1)
        }
        fn new_with_context_dop(query: i64, dop: i32) -> Self {
            Self::new_with_legacy_root_limit(query, dop, 256 * 1024 * 1024)
        }
        fn new_with_legacy_root_limit(query: i64, dop: i32, legacy_root_bytes: usize) -> Self {
            Self::with_settings(query, dop, legacy_root_bytes, false)
        }
        fn profiled(query: i64) -> Self {
            Self::with_settings(query, 1, 256 * 1024 * 1024, true)
        }
        fn with_settings(query: i64, dop: i32, legacy_root_bytes: usize, profile: bool) -> Self {
            let backend = BackendProcessId::new_v7();
            let facts = Arc::new(StubContextFacts::default());
            facts.set_context_dop(dop);
            facts.query_options.lock().unwrap().enable_profile = profile;
            facts.set_wire_options(proto::QueryOptions {
                pipeline_dop: dop,
                enable_profile: profile,
                ..Default::default()
            });

            let manager = Arc::clone(&facts.query_manager);
            let mut inner = host(Arc::clone(&facts));
            inner.result_retained_limits = novarocks_worker::WorkerResultRetainedLimits::try_new(
                legacy_root_bytes,
                inner.result_retained_limits.per_process().get(),
            )
            .unwrap();
            let counting = Arc::new(CountingHost::from_native(inner));
            let node = super::super::tests::node_fixture::NativeNodeFixture::start(
                backend,
                super::super::tests::node_fixture::factory_from_host(Arc::new(
                    AcceptingContextHost {
                        queries: counting.inner.queries.clone(),
                        facts: facts.clone(),
                    },
                )),
                counting.clone(),
                counting.inner.execution_runtime.execution_health(),
                16,
                1,
            );
            let context =
                QueryContextRef::new(execution(query), FrontendProcessId::new_v7(), backend);
            node.establish(context);
            Self {
                node,
                host: counting,
                context,
                manager,
                facts,
            }
        }

        fn final_info(
            &self,
            task: TaskIdentity,
        ) -> Arc<novarocks_execution_contract::FinalTaskInfo> {
            let original = super::super::tests::node_fixture::await_original(
                self.node.control.get_final_task_info(
                    self.context,
                    novarocks_execution_contract::GetFinalTaskInfo::new(
                        TaskOperationId::new_v7(),
                        task,
                    ),
                    self.node.deadline(),
                ),
            )
            .expect("original Task final-info R");
            let novarocks_worker::WorkerContextReplyBody::FinalInfo(frozen) = original.body()
            else {
                panic!("expected the original final-info body")
            };
            frozen.clone()
        }
        fn reporter(&self, task: TaskIdentity) -> TaskStatusReporter {
            let until = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                if let Some(original) = self.host.reads.lock().unwrap().get(&task).cloned() {
                    return original;
                }
                assert!(
                    std::time::Instant::now() < until,
                    "actual Native submit did not lend its read view"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        fn begin_read(
            &self,
            request: &novarocks_execution_contract::root_result::RootResultRead,
        ) -> Result<novarocks_worker::WorkerRootRead, novarocks_worker::WorkerRootReadError>
        {
            self.node.control.begin_root_read(
                self.context,
                TaskOperationId::new_v7(),
                self.node.deadline(),
                request,
            )
        }
        fn quiesce(&self) {
            let original =
                super::super::tests::node_fixture::await_original(self.node.control.quiesce(
                    novarocks_execution_contract::QuiesceQueryContext::new(
                        TaskOperationId::new_v7(),
                        self.context,
                    ),
                    self.node.deadline(),
                ))
                .unwrap();
            assert!(matches!(
                original.body(),
                novarocks_worker::WorkerContextReplyBody::Quiesce {
                    outcome: OperationOutcome::Accepted | OperationOutcome::Idempotent,
                    ..
                }
            ));
        }
        fn abort(&self) {
            let original =
                super::super::tests::node_fixture::await_original(self.node.control.abort(
                    novarocks_execution_contract::AbortQueryContext::new(
                        TaskOperationId::new_v7(),
                        self.context,
                        AbortCause::QueryFailed,
                    ),
                    self.node.deadline(),
                ))
                .unwrap();
            assert!(matches!(
                original.body(),
                novarocks_worker::WorkerContextReplyBody::Context(_)
            ));
        }
        fn release(&self) {
            let until = std::time::Instant::now() + Duration::from_secs(10);
            loop {
                let original =
                    super::super::tests::node_fixture::await_original(self.node.control.release(
                        ReleaseQueryContext::new(TaskOperationId::new_v7(), self.context),
                        self.node.deadline(),
                    ))
                    .unwrap();
                match original.body() {
                    novarocks_worker::WorkerContextReplyBody::Release {
                        release:
                            novarocks_execution_contract::ReleaseOutcome::Released
                            | novarocks_execution_contract::ReleaseOutcome::AlreadyTerminal,
                        ..
                    } => return,
                    novarocks_worker::WorkerContextReplyBody::Release {
                        release: novarocks_execution_contract::ReleaseOutcome::NotReady,
                        ..
                    } => {}
                    other => panic!("unexpected original Release: {other:?}"),
                }
                drop(original);
                assert!(
                    std::time::Instant::now() < until,
                    "actual Context release did not finish"
                );
                std::thread::sleep(Duration::from_millis(1));
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
        fn create(&self, descriptor: &TaskDescriptor, body: &Body) -> OriginalCreate {
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
            let original = self.node.create(request, input);
            let novarocks_worker::WorkerCreateReply::Context(body) = original.reply() else {
                panic!("an established Context must author the Create reply");
            };
            // Copy only the Core's frozen neutral reply before the original
            // external delivery exits; no Task/Source authority is retained.
            let frozen = body.clone();
            drop(original);
            OriginalCreate(frozen)
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

        fixture.quiesce();
        fixture.release();
        assert!(fixture.manager.query_mem_tracker_execution(query).is_none());
    }

    /// A winning round interprets its body once; a replay of the same
    /// identity, whatever it carries, is answered from the task that round
    /// prepared and never reaches this host.
    #[test]
    fn a_winning_round_prepares_once_and_a_replay_reuses_the_prepared_task() {
        let fixture = OwnerFixture::new(61);
        let identity = fixture.identity(1);
        let edge = ExchangeEdgeId::new(1).expect("nonzero edge");
        let descriptor = descriptor_with(
            identity,
            UniqueId::new(611, 1),
            1,
            outbound_topology(edge, self::identity(61, 2, 1)),
        );
        let winner = Body::with_sink(
            1,
            BodySink::Stream(DataStreamPartitionType::Unpartitioned),
            vec![edge.get()],
        );
        let accepted = fixture.create(&descriptor, &winner);
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
        let mut changed_body = Body::with_sink(2, BodySink::Result, Vec::new());
        changed_body.assignment.instance_ordinal = 9;
        let replay = fixture.create(&changed, &changed_body);
        assert_eq!(replay.outcome(), OperationOutcome::Idempotent, "{replay:?}");
        let until = std::time::Instant::now() + Duration::from_secs(5);
        while fixture.host.installs.load(Ordering::SeqCst) == 0 {
            assert!(
                std::time::Instant::now() < until,
                "the original Native install did not run"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            fixture.host.installs.load(Ordering::SeqCst),
            1,
            "only the winning body reaches actual Native interpretation"
        );
        let refused = fixture
            .begin_read(&bounded_read(
                identity,
                novarocks_result_contract::RootOutputKind::CountOnly,
            ))
            .err()
            .expect("replaying a Result body must not create a Root for the Stream winner");
        assert_eq!(
            refused.kind(),
            novarocks_worker::WorkerRootReadErrorKind::NoRoot
        );
        let original_read = fixture.reporter(identity);
        fixture.abort();
        let until = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let status = original_read
                .current()
                .expect("the exact read binding freezes its actual terminal");
            if status.is_terminal() {
                break;
            }
            assert!(
                std::time::Instant::now() < until,
                "the original Stream must actually stop"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Accepted preparation spends the Task identity even when the host
    /// refuses its static plan. Replay observes that same terminal; only a
    /// different exact Task identity may own a later legal preparation.
    #[test]
    fn a_refused_first_round_keeps_its_task_terminal_and_a_later_identity_wins() {
        let fixture = OwnerFixture::new(62);
        let identity = fixture.identity(1);
        let descriptor = descriptor_with(
            identity,
            UniqueId::new(621, 1),
            2,
            ExchangeTopology::default(),
        );
        let accepted = fixture.create(&descriptor, &Body::values(1));
        assert_eq!(accepted.outcome(), OperationOutcome::Accepted);
        let original = fixture.node.wait_status(
            fixture.context,
            identity,
            Duration::from_secs(5),
            |status| status.is_terminal(),
        );
        assert_eq!(original.state(), TaskState::Failed);
        assert!(!original.installed());
        let Some(TerminationDetail::Failed(failure)) = original.termination() else {
            panic!("compiler failure must remain originating")
        };
        assert_eq!(
            failure.phase(),
            novarocks_execution_contract::TaskFailurePhase::Preparation
        );
        use novarocks_execution_contract::failure::FailureProjection;
        let author = novarocks_local_compiler::FragmentCompileError::Invalid(
            "DOP is outside physical domain",
        );
        assert_eq!(failure.fact(), author.failure_fact());
        assert_eq!(failure.detail().as_str(), "Invariant failure (reason 19:2)");
        let replay = fixture.create(&descriptor, &Body::values(2));
        assert_eq!(replay.outcome(), OperationOutcome::Idempotent);
        assert_eq!(replay.acknowledgement().current_status(), &original);
        assert_eq!(fixture.host.installs.load(Ordering::SeqCst), 1);
        let later = fixture.identity(2);
        let later_descriptor =
            descriptor_with(later, UniqueId::new(621, 2), 2, ExchangeTopology::default());
        let accepted = fixture.create(&later_descriptor, &Body::values(2));
        assert_eq!(accepted.outcome(), OperationOutcome::Accepted);
        wait_bounded_root_finished(&fixture, later);
        assert_eq!(fixture.host.installs.load(Ordering::SeqCst), 2);
        assert!(fixture.node.status(fixture.context, later).installed());
        assert_eq!(fixture.node.status(fixture.context, identity), original);
        fixture.abort();
    }

    fn wait_bounded_root_finished(fixture: &OwnerFixture, root: TaskIdentity) {
        let status =
            fixture
                .node
                .wait_status(fixture.context, root, Duration::from_secs(5), |status| {
                    status.is_terminal()
                });
        assert_eq!(
            status.state(),
            TaskState::Finished,
            "unexpected actual Root terminal: {status:?}"
        );
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
        fn cleanup_step(
            &self,
            _phase: u16,
            _predecessors: crate::root_producer_pool::RootCleanupPredecessors,
        ) -> crate::root_producer_pool::RootCleanupProgress {
            crate::root_producer_pool::RootCleanupProgress::Exited
        }
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
            let registration = pool
                .reserve_for_test(crate::root_producer_pool::RootCleanupPlan::new(&[]))
                .unwrap()
                .commit(job.clone())
                .unwrap_or_else(|refusal| panic!("fixture work refused: {}", refusal.cause));
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
    fn preparation_rollback_waits_for_the_real_native_root_cleanup_job() {
        let fixture = OwnerFixture::new(91_210);
        let pause = RootWorkerPause::new(&fixture.host.inner.root_producer_pool);
        fixture.host.reject_submit.store(true, Ordering::Release);
        let root = fixture.identity(1);
        let descriptor = consistent_descriptor(root, UniqueId::new(91_210, 1));
        let receipt = fixture.create(
            &descriptor,
            &Body::bounded_root(novarocks_result_contract::FrozenRootOutput::CountOnly, &[]),
        );
        assert_eq!(receipt.outcome(), OperationOutcome::Accepted);
        let by = std::time::Instant::now() + Duration::from_secs(5);
        let reporter = loop {
            if let Some(original) = fixture.host.submit_refusal.lock().unwrap().take() {
                break original;
            }
            assert!(
                std::time::Instant::now() < by,
                "actual Native submit was not reached"
            );
            std::thread::sleep(Duration::from_millis(1));
        };
        let until = std::time::Instant::now() + Duration::from_secs(5);
        while !reporter.current().unwrap().state().is_failure() {
            assert!(
                std::time::Instant::now() < until,
                "the original preparation refusal did not publish"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        let before_exit = reporter
            .convergence()
            .expect("the original read binding is still live");
        // The actual Root job is queued behind the original CPU worker. Open
        // it and join before assertions, so an oracle failure cannot strand it.
        pause.open();
        fixture.host.inner.root_producer_pool.shutdown().unwrap();
        assert!(
            !before_exit.actual_stopped(),
            "a queued Native cleanup still owns preparation"
        );
        assert!(
            !before_exit.resources_converged(),
            "actual child exit is independent of source acceptance"
        );
        let status =
            fixture
                .node
                .wait_status(fixture.context, root, Duration::from_secs(5), |status| {
                    status.is_terminal()
                });
        assert_eq!(status.state(), TaskState::Failed);
        assert!(!status.installed());
        let original_host = fixture.host.clone();
        let original_manager = fixture.manager.clone();
        drop(pause);
        drop(fixture);
        assert_eq!(
            original_manager
                .native_execution_resource_snapshot()
                .active_fragments,
            0
        );
        let available = original_host
            .inner
            .result_retained_limits
            .per_process()
            .get()
            - original_host.inner.root_producer_pool.reserved_bytes();
        let novarocks_execution::runtime::fragment::io::ResultWriteAdmission::Granted(original) =
            original_host
                .inner
                .result_retained_budget
                .try_reserve_process(available)
                .unwrap()
        else {
            panic!("actual Node retirement still retains original rollback capacity")
        };
        drop(original);
    }

    #[test]
    fn prepared_rollback_after_driver_shutdown_releases_its_original_registration() {
        let facts = Arc::new(StubContextFacts::default());
        let manager = facts.query_manager.clone();
        let host = Arc::new(host(facts.clone()));
        let pause = RootWorkerPause::new(&host.root_producer_pool);
        let root = identity(91_232, 1, 1);
        let descriptor = consistent_descriptor(root, UniqueId::new(91_232, 1));
        let actual = host.clone();
        let callback_host = host.clone();
        let (prepared, prepared_rx) = std::sync::mpsc::sync_channel(1);
        let (done, done_rx) = std::sync::mpsc::sync_channel(1);
        let inspector = std::thread::spawn(move || {
            inspect_native_receiver(
                &actual,
                facts,
                descriptor.clone(),
                Body::bounded_root(novarocks_result_contract::FrozenRootOutput::CountOnly, &[])
                    .input(&descriptor),
                None,
                move |result| {
                    result
                        .as_ref()
                        .expect("the actual prepared kernel exists before driver shutdown");
                    callback_host
                        .execution_runtime
                        .driver_executor()
                        .shutdown()
                        .unwrap();
                    prepared.send(()).unwrap();
                },
            );
            done.send(()).unwrap();
        });
        prepared_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the original preparation shut down its actual driver executor");
        assert_eq!(
            manager
                .native_execution_resource_snapshot()
                .active_fragments,
            1,
            "the original registration remains owned while its Root cleanup cannot run"
        );
        pause.open();
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("actual Node rollback must exit after the original Root CPU owner releases");
        inspector.join().unwrap();
        host.root_producer_pool.shutdown().unwrap();
        assert_eq!(
            manager
                .native_execution_resource_snapshot()
                .active_fragments,
            0
        );
        assert!(host.execution_runtime.execution_health().is_healthy());
    }

    #[test]
    fn scalar_root_origin_is_accepted_before_its_real_producer_cleanup_notifies() {
        use novarocks_execution_contract::task_execution::status::TerminationDetail;
        let fixture = OwnerFixture::new(91_208);
        let pause = RootWorkerPause::new(&fixture.host.inner.root_producer_pool);
        let root = fixture.identity(1);
        let descriptor = consistent_descriptor(root, UniqueId::new(91_208, 1));
        let scalar = novarocks_result_contract::ScalarSchema::try_new(
            novarocks_result_contract::ScalarField {
                nullable: false,
                value_type: novarocks_result_contract::ScalarValueType::SignedInteger(64),
            },
        )
        .unwrap();
        // The package declares semantic fields; local compilation binds the actual slot.
        assert_eq!(scalar.source_slot(), None);
        let receipt = fixture.create(
            &descriptor,
            &Body::bounded_root(
                novarocks_result_contract::FrozenRootOutput::ScalarValue(scalar),
                &[1, 2],
            ),
        );
        assert_eq!(receipt.outcome(), OperationOutcome::Accepted);
        let source = fixture.reporter(root);
        let (channel, original_session) = {
            let runtime = fixture.host.inner.task_runtime(root).unwrap();
            (
                runtime.root_channel.clone().unwrap(),
                runtime.root_session_observation.clone().unwrap(),
            )
        };
        let original_control = fixture
            .host
            .controls
            .lock()
            .unwrap()
            .get(&root)
            .cloned()
            .unwrap();
        let cleanup_entered = Arc::new(AtomicBool::new(false));
        let (stopped, stopped_rx) = std::sync::mpsc::sync_channel(1);
        let notifications = Arc::new(AtomicUsize::new(0));
        let observed = notifications.clone();
        let source_for_stop = original_control.clone();
        let actual_cleanup_entered = cleanup_entered.clone();
        let stop_observer = original_control
            .stop_observable()
            .subscribe(Arc::new(move || {
                // The existing real cleanup gate fixes the observation window;
                // no concurrent producer turn can hold either Root mutex here.
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                while !actual_cleanup_entered.load(Ordering::Acquire) {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "the original producer must enter its existing cleanup gate"
                    );
                    std::thread::yield_now();
                }
                let session = original_session
                    .upgrade()
                    .expect("the actual adopted Root Session remains alive at its cleanup gate");
                let accepted = source_for_stop.accepted().expect(
                    "the actual Core published the original Root failure before its stop callback",
                );
                session.assert_scalar_origin_stop_reentry(
                    accepted
                        .originating_failure()
                        .expect("the accepted conclusion owns its original cause"),
                );
                observed.fetch_add(1, Ordering::Release);
                stopped.send(accepted).unwrap();
            }));
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let gate = Arc::new(PausedRootWorker {
            open: Mutex::new(false),
            changed: std::sync::Condvar::new(),
            started: Mutex::new(None),
        });
        let cleanup_pause = CallbackPause(gate.clone());
        let source_at_cleanup = source.clone();
        let weak_channel = Arc::downgrade(&channel);
        let once = std::sync::atomic::AtomicBool::new(true);
        let observer = channel.writable_observable().subscribe(Arc::new(move || {
            if weak_channel
                .upgrade()
                .is_some_and(|channel| channel.is_closed())
                && once.swap(false, Ordering::AcqRel)
            {
                // This is the real codec's cleanup callback, while its input,
                // cursor and producer guard have not all left the pool turn.
                cleanup_entered.store(true, Ordering::Release);
                sender.send(source_at_cleanup.current().unwrap()).unwrap();
                let mut open = gate.open.lock().unwrap();
                while !*open {
                    open = gate.changed.wait(open).unwrap();
                }
            }
        }));
        pause.open();
        let at_cleanup = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        let accepted_at_stop = stopped_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the actual adopted runtime cut must notify its original Root observer");
        let accepted_at_cleanup = original_control
            .accepted()
            .expect("the same original accepted source remains owned during cleanup");
        assert!(
            Arc::ptr_eq(&accepted_at_stop, &accepted_at_cleanup),
            "the callback and real cleanup observe the same original accepted source"
        );
        assert_eq!(notifications.load(Ordering::Acquire), 1);
        // All source/reentry/one-notification assertions belong to this real
        // still-live Root cleanup window. Retiring the Task later also wakes
        // stop observers; that different lifetime is not this subscription.
        drop(stop_observer);
        drop(accepted_at_stop);
        drop(accepted_at_cleanup);
        // Release real work and observe its actual terminal before assertions;
        // an intended RED must not strand the producer or its retained input.
        drop(cleanup_pause);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let terminal = loop {
            let status = source.current().unwrap();
            if status.is_terminal() || std::time::Instant::now() >= deadline {
                break status;
            }
            std::thread::sleep(Duration::from_millis(1));
        };
        drop(observer);
        assert_eq!(
            terminal.state(),
            TaskState::Failed,
            "actual codec failure must finish after cleanup"
        );
        assert_eq!(
            at_cleanup.state(),
            TaskState::Failing,
            "the actual originating codec error must reach the sole Task owner before cleanup callbacks"
        );
        let Some(TerminationDetail::Failed(failure)) = at_cleanup.termination() else {
            panic!("actual originating failure required");
        };
        assert_eq!(failure.category(), TaskFailureCategory::Execution);
        assert_eq!(
            at_cleanup.termination(),
            terminal.termination(),
            "cleanup/stop observations cannot replace the accepted origin"
        );
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
            let (sender, receiver) = std::sync::mpsc::sync_channel(1);
            let fanout_gate = Arc::new(PausedRootWorker {
                open: Mutex::new(false),
                changed: std::sync::Condvar::new(),
                started: Mutex::new(Some(sender)),
            });
            let fanout_pause = CallbackPause(fanout_gate.clone());
            *fixture.host.abort_pause.lock().unwrap() = Some(fanout_gate);
            let root = fixture.identity(1);
            let descriptor = consistent_descriptor(root, UniqueId::new(query, 1));
            let receipt = fixture.create(
                &descriptor,
                &Body::bounded_root(novarocks_result_contract::FrozenRootOutput::CountOnly, &[1]),
            );
            assert_eq!(receipt.outcome(), OperationOutcome::Accepted, "{receipt:?}");
            let source = fixture.reporter(root);
            // Accepted and the host's receiver map precede Worker installation.
            // The original five-second observation budget covers installation
            // and the exact abort-delegation gate; it is never restarted.
            let abort_deadline = std::time::Instant::now() + Duration::from_secs(5);
            let installed = loop {
                let current = source.current().unwrap();
                if current.installed() && current.state() == TaskState::Running {
                    break Some(current);
                }
                if current.is_terminal() || std::time::Instant::now() >= abort_deadline {
                    break None;
                }
                std::thread::sleep(Duration::from_millis(1));
            };
            if installed.is_none() {
                pause.open();
                drop(fanout_pause);
                let pool_exit = fixture.host.inner.root_producer_pool.shutdown();
                let completion_exit = fixture.host.inner.completion_supervisor.shutdown();
                assert!(pool_exit.is_ok(), "{pool_exit:?}");
                assert!(completion_exit.is_ok(), "{completion_exit:?}");
                panic!(
                    "the real root was not Installed and Running: {:?}",
                    source.current()
                );
            }
            let channel = fixture
                .host
                .inner
                .task_runtime(root)
                .unwrap()
                .root_channel
                .clone()
                .unwrap();
            let control = fixture.node.control.clone();
            let original_deadline = fixture.node.deadline();
            let context = fixture.context;
            let abort = std::thread::spawn(move || {
                super::super::tests::node_fixture::await_original(control.abort(
                    novarocks_execution_contract::AbortQueryContext::new(
                        TaskOperationId::new_v7(),
                        context,
                        cause,
                    ),
                    original_deadline,
                ))
                .expect("the original Abort delivery")
            });
            let reached_fanout = receiver
                .recv_timeout(abort_deadline.saturating_duration_since(std::time::Instant::now()));
            let at_seal = source.current().unwrap();
            let root_sealed = channel.is_closed();
            // The actual Worker fence and seal already ran, but the wrapper
            // has not delegated to the real NativeRunnableTask.abort yet.
            pause.open();
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let terminal = loop {
                let current = source.current().unwrap();
                if current.is_terminal() || std::time::Instant::now() >= deadline {
                    break current;
                }
                std::thread::sleep(Duration::from_millis(1));
            };
            // Every final assertion follows actual fan-out exit and completion
            // cleanup, even when the intended observation would be RED.
            // End this internal metadata loan while the original Stop body
            // still keeps Task/Context retirement behind its actual exit.
            drop(channel);
            drop(fanout_pause);
            let abort_result = abort.join();
            let pool_exit = fixture.host.inner.root_producer_pool.shutdown();
            let completion_exit = fixture.host.inner.completion_supervisor.shutdown();
            assert!(
                reached_fanout.is_ok(),
                "real runnable abort gate was not reached: {reached_fanout:?}; pool exit={pool_exit:?}; completion exit={completion_exit:?}"
            );
            assert!(
                root_sealed,
                "Worker must seal the real root before runnable fan-out"
            );
            let receipt = abort_result.unwrap();
            assert!(
                matches!(receipt.body(), novarocks_worker::WorkerContextReplyBody::Context(ack) if ack.outcome == OperationOutcome::Accepted)
            );
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
            assert!(pool_exit.is_ok(), "{pool_exit:?}");
            assert!(completion_exit.is_ok(), "{completion_exit:?}");
        }
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
        let read = fixture
            .begin_read(&request)
            .expect("finished root must retain its independent End");
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
        let fixture = OwnerFixture::new(91_001);
        let root = fixture.identity(1);
        let descriptor = consistent_descriptor(root, UniqueId::new(91_001, 1));
        let receipt = fixture.create(
            &descriptor,
            &Body::bounded_root(FrozenRootOutput::CountOnly, &[4, 5, 6]),
        );
        assert_eq!(receipt.outcome(), OperationOutcome::Accepted, "{receipt:?}");
        wait_bounded_root_finished(&fixture, root);
        let retired_by = std::time::Instant::now() + Duration::from_secs(5);
        while fixture.host.inner.task_runtime(root).is_some() {
            assert!(
                std::time::Instant::now() < retired_by,
                "actual producer receiver did not retire"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let read = bounded_read(root, RootOutputKind::CountOnly);
        let first = fixture
            .begin_read(&read)
            .expect("retired producer lost context output");
        let first = runtime.block_on(first.read()).unwrap();
        assert!(
            matches!(first.reply().outcome, RootReadOutcome::End(end) if end.output_rows == 3 && end.sequence.get() == 1)
        );
        let reply = first.reply().clone();
        drop(first);
        let replay = fixture
            .begin_read(&read)
            .expect("context cannot replay End");
        let replay = runtime.block_on(replay.read()).unwrap();
        assert_eq!(replay.reply(), &reply);
        fixture.quiesce();
        let control = fixture.node.control.clone();
        let context = fixture.context;
        let deadline = fixture.node.deadline();
        let (sent, received) = std::sync::mpsc::sync_channel(1);
        let release = std::thread::spawn(move || {
            loop {
                let original = super::super::tests::node_fixture::await_original(control.release(
                    ReleaseQueryContext::new(TaskOperationId::new_v7(), context),
                    deadline,
                ))
                .unwrap();
                match original.body() {
                    novarocks_worker::WorkerContextReplyBody::Release {
                        release: novarocks_execution_contract::ReleaseOutcome::NotReady,
                        ..
                    } => {
                        drop(original);
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    novarocks_worker::WorkerContextReplyBody::Release {
                        release:
                            novarocks_execution_contract::ReleaseOutcome::Released
                            | novarocks_execution_contract::ReleaseOutcome::AlreadyTerminal,
                        ..
                    } => {
                        sent.send(original)
                            .unwrap_or_else(|_| panic!("original Release receiver exited"));
                        break;
                    }
                    other => panic!("unexpected original Release reply: {other:?}"),
                }
            }
        });

        let sealed_by = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if fixture
                .node
                .control
                .try_root_ownership_snapshot()
                .is_some_and(|actual| actual.sealed == 1)
            {
                break;
            }
            assert!(
                std::time::Instant::now() < sealed_by,
                "original Release did not seal Root"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            matches!(
                received.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ),
            "End delivery remains an actual holder"
        );
        let refused = fixture
            .begin_read(&read)
            .err()
            .expect("sealed Root must refuse a new read");
        assert!(
            matches!(
                refused.kind(),
                novarocks_worker::WorkerRootReadErrorKind::AdmissionRefused
            ) || matches!(
                refused.channel_error(),
                Some(novarocks_worker::root_result_channel::RootChannelError::Closed)
            )
        );
        drop(refused);
        drop(replay);
        let released = received
            .recv_timeout(Duration::from_secs(5))
            .expect("last delivery exit must release original R");
        assert!(matches!(
            released.body(),
            novarocks_worker::WorkerContextReplyBody::Release {
                release: novarocks_execution_contract::ReleaseOutcome::Released,
                ..
            }
        ));
        drop(released);
        release.join().unwrap();
        fixture.host.inner.root_producer_pool.shutdown().unwrap();
    }

    #[test]
    fn bounded_root_dop_64_values_collapse_counts_actual_rows_once() {
        use novarocks_execution_contract::root_result::RootReadOutcome;
        use novarocks_result_contract::{FrozenRootOutput, RootOutputKind};
        let fixture = OwnerFixture::new_with_context_dop(91_009, 64);
        let root = fixture.identity(1);
        let descriptor = descriptor_with(
            root,
            UniqueId::new(91_009, 1),
            64,
            ExchangeTopology::default(),
        );
        let mut body = Body::bounded_root(FrozenRootOutput::CountOnly, &[1, 2]);
        body.set_dop(64);
        let receipt = fixture.create(&descriptor, &body);
        assert_eq!(receipt.outcome(), OperationOutcome::Accepted, "{receipt:?}");
        wait_bounded_root_finished(&fixture, root);
        let read = fixture
            .begin_read(&bounded_read(root, RootOutputKind::CountOnly))
            .expect("context lost count");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let delivery = runtime.block_on(read.read()).unwrap();
        assert!(
            matches!(delivery.reply().outcome, RootReadOutcome::End(end) if end.output_rows == 2)
        );
        drop(delivery);
        fixture.abort();
        fixture.host.inner.root_producer_pool.shutdown().unwrap();
    }

    #[test]
    fn bounded_root_client_bytes_finish_without_legacy_arrow_or_ack() {
        use novarocks_execution_contract::root_result::RootReadOutcome;
        use novarocks_result_contract::{
            ClientRenderSchema, FrozenRootOutput, NativeRenderType, RenderColumn, RenderField,
            RenderPresentation, RootOutputKind,
        };
        let schema = ClientRenderSchema::try_new(
            novarocks_result_contract::ClientPresentationFamily::MysqlText,
            vec![RenderColumn {
                source_ordinal: 0,
                source_slot: Some(0),
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
        let read = fixture
            .begin_read(&bounded_read(root, RootOutputKind::ClientRows))
            .expect("finished root absent");
        let delivery = runtime.block_on(read.read()).unwrap();
        let RootReadOutcome::Data(data) = &delivery.reply().outcome else {
            panic!("root did not encode client bytes")
        };
        assert_eq!(data.body().as_ref(), &[3, 0, 0, 0, 2, b'4', b'2']);
        assert_single_row_end_without_ack(&fixture, root, data, &runtime);
        drop(delivery);
        fixture.abort();
        fixture.host.inner.root_producer_pool.shutdown().unwrap();
    }

    #[test]
    fn bounded_statistics_root_runs_real_wire_decode_driver_and_context_channel() {
        use novarocks_execution_contract::root_result::RootReadOutcome;
        use novarocks_result_contract::{InternalResultDomain, RootOutputKind};
        let fixture = OwnerFixture::new(91_005);
        let root = fixture.identity(1);
        let descriptor = consistent_descriptor(root, UniqueId::new(91_005, 2));
        let receipt = fixture.create(&descriptor, &Body::bounded_statistics());
        assert_eq!(receipt.outcome(), OperationOutcome::Accepted, "{receipt:?}");
        wait_bounded_root_finished(&fixture, root);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let read = fixture
            .begin_read(&bounded_read(
                root,
                RootOutputKind::InternalFacts(InternalResultDomain::StatisticsArtifactV1),
            ))
            .expect("finished statistics root absent");
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
        fixture.abort();
        fixture.host.inner.root_producer_pool.shutdown().unwrap();
    }

    #[test]
    fn statistics_host_refuses_an_unprotected_source_before_channel_installation() {
        let facts = Arc::new(StubContextFacts::default());
        let host = Arc::new(host(facts.clone()));
        let root = identity(91_007, 1, 1);
        let descriptor = consistent_descriptor(root, UniqueId::new(91_007, 1));
        let body = Body::bounded_root(
            novarocks_result_contract::FrozenRootOutput::InternalFacts(
                novarocks_result_contract::InternalResultDomain::StatisticsArtifactV1,
            ),
            &[1],
        );
        inspect_native_receiver(
            &host,
            facts,
            descriptor.clone(),
            body.input(&descriptor),
            None,
            move |result| {
                let error = result
                    .as_ref()
                    .expect_err("the actual Native install must refuse this input");
                use novarocks_execution_contract::failure::{FailureClass, domain};
                let author = assert_host_refusal_source::<super::RootPreparationFailure>(
                    error,
                    TaskFailureCategory::Internal,
                    expected_refusal_fact(FailureClass::Invariant, domain::NATIVE, 9400),
                    "Invariant failure (reason 13:9400)",
                );
                assert!(matches!(
                    author,
                    super::RootPreparationFailure::StatisticsSource
                ));
                assert_eq!(
                    author.to_string(),
                    "StatisticsArtifactV1 requires its bounded final Unpivot source"
                );
            },
        );
        assert!(host.task_runtime(root).is_none());
        host.root_producer_pool.shutdown().unwrap();
    }

    #[test]
    fn statistics_host_rejects_a_profile_that_the_generic_unpivot_would_accept() {
        let facts = Arc::new(StubContextFacts::default());
        let host = Arc::new(host(facts.clone()));
        let root = identity(91_006, 1, 1);
        let descriptor = consistent_descriptor(root, UniqueId::new(91_006, 1));
        let mut body = Body::bounded_statistics();
        body.mutate_package(|package| {
            let node = package.fragment.as_mut().unwrap().nodes.iter_mut()
                .find(|node| matches!(node.kind, Some(novarocks_proto_models::physical_package_v2::physical_node::Kind::Unpivot(_))))
                .expect("real Unpivot root");
            let Some(novarocks_proto_models::physical_package_v2::physical_node::Kind::Unpivot(unpivot)) = node.kind.as_mut() else { unreachable!() };
            unpivot.max_output_bytes -= 1;
        });
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
        inspect_native_receiver(
            &host,
            facts,
            descriptor.clone(),
            body.input(&descriptor),
            None,
            move |result| {
                let error = result
                    .as_ref()
                    .expect_err("generic Unpivot must not replace the protected Statistics source");
                assert!(
                    format!("{error:?}").contains(
                        "statistics materializer roles or bounds differ from its frozen domain"
                    ),
                    "unexpected Statistics preparation refusal: {error:?}"
                );
            },
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
        let facts = Arc::new(StubContextFacts::default());
        let host = Arc::new(host(facts.clone()));
        let root = identity(91_003, 1, 1);
        let descriptor = consistent_descriptor(root, UniqueId::new(91_003, 1));
        let body = Body::bounded_root(novarocks_result_contract::FrozenRootOutput::CountOnly, &[1]);
        let prepared_host = host.clone();
        inspect_native_receiver(
            &host,
            facts.clone(),
            descriptor.clone(),
            body.input(&descriptor),
            None,
            move |result| {
                assert_eq!(
                    result
                        .as_ref()
                        .expect("actual bounded Root installs")
                        .facts()
                        .sink_kind(),
                    FragmentSinkKind::Result
                );
                let runtime = prepared_host
                    .task_runtime(root)
                    .expect("the actual provisional receiver");
                let channel = runtime.root_channel.as_ref().expect("the same actual Root");
                assert_eq!(channel.spec().task, root);
                assert!(
                    !channel.physical_idle(),
                    "prepared drivers retain covered Session metadata"
                );
            },
        );
        assert!(host.task_runtime(root).is_none());
        assert_eq!(
            facts
                .query_manager
                .native_execution_resource_snapshot()
                .active_fragments,
            0
        );
        host.root_producer_pool.shutdown().unwrap();
        let available = host.result_retained_limits.per_process().get()
            - host.root_producer_pool.reserved_bytes();
        let novarocks_execution::runtime::fragment::io::ResultWriteAdmission::Granted(original) =
            host.result_retained_budget
                .try_reserve_process(available)
                .unwrap()
        else {
            panic!("actual Node rollback and H still retain original Root capacity")
        };
        drop(original);
    }

    #[test]
    fn bounded_root_untyped_scalar_identity_keeps_author_protocol_fact_before_channel_install() {
        let facts = Arc::new(StubContextFacts::default());
        let host = Arc::new(host(facts.clone()));
        let root = identity(91_004, 1, 1);
        let descriptor = consistent_descriptor(root, UniqueId::new(91_004, 1));
        let body = Body::bounded_root(
            novarocks_result_contract::FrozenRootOutput::InternalFacts(
                novarocks_result_contract::InternalResultDomain::ScalarValueV1,
            ),
            &[],
        );
        inspect_native_receiver(
            &host,
            facts,
            descriptor.clone(),
            body.input(&descriptor),
            None,
            move |result| {
                let error = result
                    .as_ref()
                    .expect_err("the actual Native install must refuse this input");
                use novarocks_execution_contract::failure::{
                    FailureClass, FailureFact, FailureProjection, FailureReason, domain,
                };
                let failure = error;
                let author = crate::backend_test_support::find_source::<
                    novarocks_proto_codec::error::ProtocolError,
                >(failure)
                .expect("the exact codec source remains in the local error chain");
                assert!(matches!(
                    author.kind(),
                    novarocks_proto_codec::error::ProtocolErrorKind::InvalidValue
                ));
                assert_eq!(
                    author.path(),
                    &novarocks_proto_codec::error::FieldPath::root("fragment.sink.root_result")
                );
                let expected = FailureFact::new(
                    FailureClass::Data,
                    FailureReason::new(domain::PROTOCOL, std::num::NonZeroU16::new(3).unwrap()),
                );
                assert_eq!(author.failure_fact(), expected);
                assert_eq!(failure.failure_fact(), expected);
            },
        );
        assert!(host.task_runtime(root).is_none());
        host.root_producer_pool.shutdown().unwrap();
    }
    #[test]
    fn bounded_root_installed_scalar_domain_admits_its_typed_producer() {
        let facts = Arc::new(StubContextFacts::default());
        let host = Arc::new(host(facts.clone()));
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
                .unwrap(),
            ),
            &[],
        );
        let installed_host = host.clone();
        inspect_native_receiver(
            &host,
            facts,
            descriptor.clone(),
            body.input(&descriptor),
            None,
            move |result| {
                let installed = result
                    .as_ref()
                    .expect("an admitted scalar producer installs its actual typed root");
                assert_eq!(installed.facts().sink_kind(), FragmentSinkKind::Result);
                assert!(installed_host.task_runtime(root).is_some());
            },
        );
        assert!(
            host.task_runtime(root).is_none(),
            "actual Node drain retires the prepared kernel"
        );
        host.root_producer_pool.shutdown().unwrap();
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

        pub(crate) fn interpreter() -> CompiledPackageInterpreter<std::convert::Infallible> {
            let providers = PureProviderProgramCatalog::<std::convert::Infallible>::try_new(
                &[],
                vec![],
                &Unbounded,
            )
            .expect("empty provider catalog");
            CompiledPackageInterpreter::new(
                FragmentDecodeResourceModel::try_new(&Unbounded).expect("decode model"),
                decode_limits(),
                Arc::new(sealed_rand_subset()),
                Arc::new(providers),
                constants(),
            )
        }

        pub(crate) fn window_interpreter() -> CompiledPackageInterpreter<std::convert::Infallible> {
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
            let providers = PureProviderProgramCatalog::<std::convert::Infallible>::try_new(
                &[],
                vec![],
                &Unbounded,
            )
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
            inner: CompiledPackageInterpreter<std::convert::Infallible>,
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

        type NamespaceAuditResult = Result<
            (
                novarocks_physical_plan::ConstantPools,
                novarocks_plan_codec::physical_constant_v2::ConstantNamespaceResourceFacts,
            ),
            novarocks_plan_codec::host_projection_v2::ProjectionFailure<
                novarocks_plan_codec::physical_constant_v2::PhysicalConstantCodecError,
                crate::native_type_construction::NativeTypeConstructionError,
            >,
        >;

        fn candidate_pool_namespace_audit(
            cap: Option<usize>,
        ) -> (
            NamespaceAuditResult,
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
            {
                let _factory = factory;
            }
            drop(types);
            {
                let _type_factory = type_factory;
            }
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
            {
                let _scopes = scopes;
            }
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
            {
                let _factory = factory;
            }
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
                .0
                .with_root_output(novarocks_result_contract::RootOutputContract::new(
                    novarocks_result_contract::RootProfileId::V1,
                    novarocks_result_contract::FrozenRootOutput::CountOnly,
                ))
                .expect("the fixture declares its actual CountOnly RootV1 output");
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
            static NEXT_FRAGMENT_INSTANCE_ID: std::sync::atomic::AtomicI64 =
                std::sync::atomic::AtomicI64::new(1);
            let node = FragmentNodeId::new(i32::try_from(receiver).expect("small node"));
            let destination = ExchangeDestination::new(
                TaskIdentity::new(
                    identity.query_execution_id(),
                    StageId::new(2).expect("nonzero stage"),
                    TaskId::new(1).expect("nonzero task"),
                    BackendProcessId::new_v7(),
                ),
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
                UniqueId::new(
                    701,
                    NEXT_FRAGMENT_INSTANCE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                ),
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
            let host = Arc::new(compiled_host(Arc::clone(&facts)));
            let identity = super::identity(70, 1, 1);
            let descriptor = producer_descriptor(identity, receiver, &[]);
            let installed_host = host.clone();
            let installed_facts = facts.clone();
            inspect_native_receiver(
                &host,
                facts.clone(),
                descriptor.clone(),
                producer_input(&descriptor, package_carrier(&package)),
                None,
                move |result| {
                    let prepared = result.as_ref().expect("the compiled producer installs");
                    assert_eq!(prepared.facts().sink_kind(), FragmentSinkKind::DataStream);
                    assert!(installed_host.task_runtime(identity).is_some());
                    // A program with no runtime-filter site asks for the session of
                    // exactly no binding; no scan binding requests a runtime-filter session.
                    assert_eq!(
                        *installed_facts
                            .compiled_filter_bindings
                            .lock()
                            .expect("stub compiled filter bindings"),
                        vec![BTreeSet::new()]
                    );
                    assert_eq!(
                        installed_facts
                            .filter_sessions_requested
                            .load(Ordering::SeqCst),
                        0
                    );
                },
            );
            host.root_producer_pool.shutdown().unwrap();
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
            let host = Arc::new(
                host(Arc::clone(&facts)).with_compiled_package_compiler(Arc::new(
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
                )),
            );
            let identity = super::identity(73, 1, 1);
            let descriptor = producer_descriptor(identity, receiver, &[]);
            inspect_native_receiver(
                &host,
                facts.clone(),
                descriptor.clone(),
                producer_input(&descriptor, package_carrier(&package)),
                None,
                move |result| {
                    let refused = result
                        .as_ref()
                        .expect_err("binding 3 has no site in the program");
                    use novarocks_execution_contract::failure::{FailureClass, domain};
                    let author = assert_host_refusal_source::<
                        crate::compiled_runtime_filter::CompiledRuntimeFilterBindingError,
                    >(
                        refused,
                        TaskFailureCategory::Protocol,
                        expected_refusal_fact(FailureClass::Invariant, domain::NATIVE, 9308),
                        "Invariant failure (reason 13:9308)",
                    );
                    assert!(matches!(author, crate::compiled_runtime_filter::CompiledRuntimeFilterBindingError::MissingRequirement { binding: 3 }));
                    assert_eq!(
                        author.to_string(),
                        "package runtime-filter binding_id=3 has no requirement in its compiled program"
                    );
                },
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
        fn compiled_host_refuses_absent_or_malformed_package_before_installation() {
            let (_, receiver) = select_one_producer();
            let facts = Arc::new(StubContextFacts::default());
            let compiled = Arc::new(compiled_host(facts.clone()));
            let identity = super::identity(71, 1, 1);
            let descriptor = producer_descriptor(identity, receiver, &[]);
            inspect_native_receiver(
                &compiled,
                facts.clone(),
                descriptor.clone(),
                producer_input(
                    &descriptor,
                    FrozenBytes::freeze(proto::FrozenFragment::default().encode_to_vec().into()),
                ),
                None,
                move |result| {
                    let absent = result
                        .as_ref()
                        .expect_err("a compiled host requires its package");
                    assert_protocol_refusal(
                        absent,
                        TaskFailureCategory::Protocol,
                        1,
                        novarocks_proto_codec::error::ProtocolErrorKind::MissingField,
                        FieldPath::root("frozen_fragment").field("package"),
                        "a compiled carrier requires its fragment package",
                    );
                },
            );
            inspect_native_receiver(
                &compiled,
                facts.clone(),
                descriptor.clone(),
                producer_input(&descriptor, package_carrier(b"not a package")),
                None,
                move |result| {
                    let garbage = result.as_ref().expect_err("an unreceivable package");
                    use novarocks_execution_contract::failure::{FailureClass, domain};
                    use novarocks_plan_codec::physical_package_v2::{
                        PackageDecodeError, PackageWireError,
                    };
                    let author = assert_host_refusal_source::<CompiledPackageError>(
                        garbage,
                        TaskFailureCategory::Execution,
                        expected_refusal_fact(FailureClass::Data, domain::PLAN_CODEC, 34),
                        &format!(
                            "task {identity} package could not be compiled: Data failure (reason 17:34)"
                        ),
                    );
                    assert!(
                        matches!(
                            author,
                            CompiledPackageError::Decode(PackageDecodeError::Wire(
                                PackageWireError::Protobuf(_)
                            ))
                        ),
                        "a malformed resource prefix is not the original decoder verdict: {author:?}"
                    );
                    let protobuf = crate::backend_test_support::find_source::<prost::DecodeError>(
                        garbage,
                    )
                    .expect("the exact original Prost refusal remains in the local source chain");
                    let direct =
                        novarocks_proto_models::physical_package_v2::FragmentPackage::decode(
                            &b"not a package"[..],
                        )
                        .expect_err(
                            "the original garbage package has an invalid protobuf wire type",
                        );
                    assert_eq!(protobuf.to_string(), direct.to_string());
                    assert_eq!(author.to_string(), protobuf.to_string());
                },
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
            {
                let _scopes = scopes;
            }
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
            {
                let _scopes = scopes;
            }
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
            {
                let _factory = factory;
            }
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
            let providers = PureProviderProgramCatalog::<std::convert::Infallible>::try_new(
                &[],
                vec![],
                &Unbounded,
            )
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
            {
                let _scopes = scopes;
            }
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
            assert!(original.detail().contains("compiled first typed refusal"));
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
            {
                let _scopes = scopes;
            }
            drop(actual);
            assert_eq!(tracker.current(), 0);
        }

        /// A query context that leases the fixture catalog with its installed
        /// read execution; every other fact is the stub's.
        #[derive(Default)]
        struct ScanContextFacts(StubContextFacts, Option<ConnectorExecutionReadBinding>);

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
            ) -> Result<Option<RuntimeFilterSessionRef>, novarocks_worker::RuntimeFilterContractError>
            {
                self.0
                    .runtime_filter_session(execution, fragment_instance_id, expects_bindings)
            }

            fn runtime_filter_session_for_bindings(
                &self,
                execution: QueryExecutionId,
                fragment_instance_id: UniqueId,
                bindings: &BTreeSet<u32>,
            ) -> Result<Option<RuntimeFilterSessionRef>, novarocks_worker::RuntimeFilterContractError>
            {
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
                producer: novarocks_worker::TaskDynamicFilterWriter,
            ) -> bool {
                self.0
                    .bind_runtime_filter_feedback(execution, carrier, producer)
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
            ) -> Result<ConnectorExecutionReadBinding, novarocks_worker::CatalogExecutionAccessError>
            {
                let installed = self.1.clone().unwrap_or_else(
                    crate::typed_connector_test_support::test_support::installed_read_execution,
                );
                if installed.binding().catalog_handle() == handle {
                    Ok(installed)
                } else {
                    Err(
                        novarocks_worker::CatalogExecutionAccessError::LeaseUnavailable {
                            catalog: handle.clone(),
                        },
                    )
                }
            }

            fn catalog_write_execution(
                &self,
                execution: QueryExecutionId,
                handle: &CatalogHandle,
            ) -> Result<ConnectorExecutionWriteBinding, novarocks_worker::CatalogExecutionAccessError>
            {
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
            let runtime = test_execution_runtime(Arc::clone(&facts.0.memory_authority));
            let pool = RootProducerPool::try_new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(64).unwrap(),
                1024 * 1024,
                Arc::clone(&budget),
                runtime.execution_health(),
            )
            .expect("finite test producer pool");

            NativeTaskExecutionHost::new(
                Arc::new(interpreter()),
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
            use super::super::super::tests::node_fixture::await_original;
            use crate::compiled_scan_binding::tests::{SCAN_NODE, scan_producer_package};
            let (package, receiver) = scan_producer_package();
            let facts = Arc::new(ScanContextFacts::default());
            let host = Arc::new(CountingHost::from_native(scan_host(facts.clone())));
            let identity = super::identity(72, 1, 1);
            let descriptor = producer_descriptor(identity, receiver, &[SCAN_NODE]);
            let (node, context) = original_node_for(&host, identity);
            let (started, reached) = std::sync::mpsc::sync_channel(1);
            let gate = Arc::new(PausedRootWorker {
                open: Mutex::new(false),
                changed: std::sync::Condvar::new(),
                started: Mutex::new(Some(started)),
            });
            let retirement = CallbackPause(gate.clone());
            *host.retire_pause.lock().unwrap() = Some(gate);
            let accepted = node.create(
                CreateTask::try_new(
                    TaskOperationId::new_v7(),
                    context,
                    descriptor.clone(),
                    vec![],
                )
                .unwrap(),
                scan_input(
                    &descriptor,
                    package_carrier(&package),
                    vec![empty_scan_ranges(SCAN_NODE)],
                ),
            );
            assert!(matches!(
                accepted.reply(),
                novarocks_worker::WorkerCreateReply::Context(
                    novarocks_worker::WorkerContextReplyBody::Create {
                        outcome: OperationOutcome::Accepted,
                        ..
                    }
                )
            ));
            drop(accepted);
            let read = original_installed_read(&host, identity);
            assert_eq!(
                *facts.0.compiled_filter_bindings.lock().unwrap(),
                vec![BTreeSet::new()]
            );
            let updated = await_original(
                node.control.update_task(
                    context,
                    novarocks_execution_contract::UpdateTask::try_new(
                        TaskOperationId::new_v7(),
                        identity,
                        vec![split_intent(
                            PlanNodeId::new(SCAN_NODE).unwrap(),
                            SCAN_NODE,
                            vec![
                                crate::typed_connector_test_support::test_support::split_proto(
                                    SCAN_NODE, 1,
                                ),
                            ],
                            SplitOffer::batch(SplitSequence::FIRST, SplitSequence::FIRST, true)
                                .unwrap(),
                        )],
                    )
                    .unwrap(),
                    node.deadline(),
                    node.ingress_responsibility(),
                ),
            )
            .expect("the original split R");
            assert!(matches!(
                updated.body(),
                novarocks_worker::WorkerContextReplyBody::TaskDomain {
                    outcome: OperationOutcome::Accepted,
                    ..
                }
            ));
            drop(updated);
            reached
                .recv_timeout(Duration::from_secs(5))
                .expect("actual provider exit precedes its original retirement callback");
            let status = read.current().unwrap();
            assert_eq!(status.state(), TaskState::Failed);
            let Some(TerminationDetail::Failed(failure)) = status.termination() else {
                panic!("the actual provider refusal remains originating")
            };
            use novarocks_execution_contract::failure::{
                FailureClass, FailureFact, FailureProjection, FailureReason, domain,
            };
            assert_eq!(failure.category(), TaskFailureCategory::Internal);
            assert_eq!(
                failure.phase(),
                novarocks_execution_contract::TaskFailurePhase::Execution
            );
            assert_eq!(
                failure.detail().as_str(),
                "Unexpected failure (reason 4:10) at operator 0 during Pull"
            );
            let control = host
                .controls
                .lock()
                .unwrap()
                .get(&identity)
                .cloned()
                .expect("the original preparation lent its detached runtime control");
            let accepted = control
                .accepted()
                .expect("the actual provider refusal is accepted");
            let original = accepted
                .originating_failure()
                .expect("retain the original local provider source");
            assert_eq!(
                original.failure_fact(),
                FailureFact::new(
                    FailureClass::Unexpected,
                    FailureReason::new(domain::CONNECTOR, std::num::NonZeroU16::new(10).unwrap())
                )
            );
            let mut cursor: Option<&(dyn std::error::Error + 'static)> = Some(original);
            let mut provider = None;
            while let Some(current) = cursor {
                if let Some(source) =
                    current.downcast_ref::<novarocks_spi::connector::ConnectorError>()
                {
                    provider = Some(source);
                    break;
                }
                cursor = current.source();
            }
            let provider =
                provider.expect("the actual ConnectorError survives its original source chain");
            assert_eq!(
                provider.kind(),
                novarocks_spi::connector::ConnectorErrorKind::Internal
            );
            assert_eq!(provider.message(), "fixture page provider is never read");
            assert!(!provider.retryable_before_progress());
            drop(accepted);
            drop(control);
            drop(retirement);
            drop(node);
            assert!(host.inner.task_runtime(identity).is_none());
            host.inner.root_producer_pool.shutdown().unwrap();
        }

        #[test]
        fn a_compiled_system_scan_opens_its_delivered_unit_and_closes_on_abort() {
            use super::super::super::tests::node_fixture::await_original;
            use crate::compiled_scan_binding::tests::{SCAN_NODE, system_scan_producer_package};
            use crate::typed_connector_test_support::test_support::{
                PendingReadProbe, installed_pending_read_execution,
            };
            let probe = Arc::new(PendingReadProbe::default());
            let facts = Arc::new(ScanContextFacts(
                StubContextFacts::default(),
                Some(installed_pending_read_execution(probe.clone())),
            ));
            let host = Arc::new(CountingHost::from_native(scan_host(facts)));
            let identity = super::identity(74, 1, 1);
            let (package, receiver) = system_scan_producer_package();
            let descriptor = producer_descriptor(identity, receiver, &[SCAN_NODE]);
            let (node, context) = original_node_for(&host, identity);
            let accepted = node.create(
                CreateTask::try_new(
                    TaskOperationId::new_v7(),
                    context,
                    descriptor.clone(),
                    vec![],
                )
                .unwrap(),
                scan_input(
                    &descriptor,
                    package_carrier(&package),
                    vec![empty_scan_ranges(SCAN_NODE)],
                ),
            );
            assert!(matches!(
                accepted.reply(),
                novarocks_worker::WorkerCreateReply::Context(
                    novarocks_worker::WorkerContextReplyBody::Create {
                        outcome: OperationOutcome::Accepted,
                        ..
                    }
                )
            ));
            drop(accepted);
            let read = original_installed_read(&host, identity);
            let updated = await_original(
                node.control.update_task(
                    context,
                    novarocks_execution_contract::UpdateTask::try_new(
                        TaskOperationId::new_v7(),
                        identity,
                        vec![split_intent(
                            PlanNodeId::new(SCAN_NODE).unwrap(),
                            SCAN_NODE,
                            vec![
                                crate::typed_connector_test_support::test_support::split_proto(
                                    SCAN_NODE, 1,
                                ),
                            ],
                            SplitOffer::batch(SplitSequence::FIRST, SplitSequence::FIRST, true)
                                .unwrap(),
                        )],
                    )
                    .unwrap(),
                    node.deadline(),
                    node.ingress_responsibility(),
                ),
            )
            .expect("the original system-split R");
            assert!(matches!(
                updated.body(),
                novarocks_worker::WorkerContextReplyBody::TaskDomain {
                    outcome: OperationOutcome::Accepted,
                    ..
                }
            ));
            drop(updated);
            let until = std::time::Instant::now() + Duration::from_secs(6);
            while probe.opens.load(Ordering::Acquire) != 1 {
                assert!(
                    std::time::Instant::now() < until,
                    "actual provider reader did not open"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            assert!(
                !read.current().unwrap().is_terminal(),
                "the pending reader still owns its Task"
            );
            drop(
                await_original(node.control.abort(
                    novarocks_execution_contract::AbortQueryContext::new(
                        TaskOperationId::new_v7(),
                        context,
                        AbortCause::QueryFailed,
                    ),
                    node.deadline(),
                ))
                .expect("the original abort R"),
            );
            assert_eq!(read.current().unwrap().state(), TaskState::Aborted);
            drop(node);
            assert_eq!(
                probe.closes.load(Ordering::Acquire),
                1,
                "the original opened page stream actually closes once"
            );
            assert!(host.inner.task_runtime(identity).is_none());
            host.inner.root_producer_pool.shutdown().unwrap();
        }

        // The descriptor and the assignment must address exactly the compiled
        // scans, and only with split-driven work; every refusal leaves no
        // prepared task and no open split queue behind.
        #[test]
        fn a_compiled_scan_task_addresses_exactly_its_scans_by_splits() {
            use crate::compiled_scan_binding::tests::{SCAN_NODE, scan_producer_package};

            let (package, receiver) = scan_producer_package();
            let facts = Arc::new(ScanContextFacts::default());
            let host = Arc::new(scan_host(facts.clone()));
            let refusal = |identity: TaskIdentity,
                           split_nodes: &[i32],
                           initial,
                           reason: NativePreparationReason,
                           original_detail: &str| {
                let descriptor = producer_descriptor(identity, receiver, split_nodes);
                let original_detail = original_detail.to_owned();
                inspect_native_receiver(
                    &host,
                    facts.clone(),
                    descriptor.clone(),
                    scan_input(&descriptor, package_carrier(&package), initial),
                    None,
                    move |result| {
                        let rejection = result
                            .as_ref()
                            .expect_err("the actual compiled scan input is refused");
                        use novarocks_execution_contract::failure::{FailureClass, domain};
                        let author = assert_host_refusal_source::<NativePreparationContractFailure>(
                            rejection,
                            TaskFailureCategory::Protocol,
                            expected_refusal_fact(
                                FailureClass::Invariant,
                                domain::NATIVE,
                                reason as u16,
                            ),
                            &format!("Invariant failure (reason 13:{})", reason as u16),
                        );
                        assert_eq!(author.reason as u16, reason as u16);
                        assert_eq!(author.detail.as_str(), original_detail);
                    },
                );
                assert!(host.task_runtime(identity).is_none());
                assert!(host.split_queues.is_empty(), "the split lease rolled back");
            };

            let unsplit = super::identity(73, 1, 1);
            refusal(
                unsplit,
                &[],
                Vec::new(),
                NativePreparationReason::CompiledSplitCoverage,
                &format!(
                    "task {unsplit} names split-driven nodes {{}}, but its compiled scans are {{FragmentNodeId(10)}}"
                ),
            );
            let foreign = super::identity(74, 1, 1);
            refusal(
                foreign,
                &[SCAN_NODE, SCAN_NODE + 2],
                Vec::new(),
                NativePreparationReason::CompiledSplitCoverage,
                &format!(
                    "task {foreign} names split-driven nodes {{FragmentNodeId(10), FragmentNodeId(12)}}, but its compiled scans are {{FragmentNodeId(10)}}"
                ),
            );
            let stray = super::identity(75, 1, 1);
            refusal(
                stray,
                &[SCAN_NODE],
                vec![empty_scan_ranges(SCAN_NODE + 2)],
                NativePreparationReason::UnknownCompiledRangeNode,
                &format!(
                    "task {stray} assigns scan ranges to node 12, which is not a compiled scan"
                ),
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
            ) -> Result<Option<RuntimeFilterSessionRef>, novarocks_worker::RuntimeFilterContractError>
            {
                self.stub
                    .runtime_filter_session(execution, fragment_instance_id, expects_bindings)
            }

            fn runtime_filter_session_for_bindings(
                &self,
                execution: QueryExecutionId,
                fragment_instance_id: UniqueId,
                bindings: &BTreeSet<u32>,
            ) -> Result<Option<RuntimeFilterSessionRef>, novarocks_worker::RuntimeFilterContractError>
            {
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
                producer: novarocks_worker::TaskDynamicFilterWriter,
            ) -> bool {
                self.stub
                    .bind_runtime_filter_feedback(execution, carrier, producer)
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
            ) -> Result<ConnectorExecutionReadBinding, novarocks_worker::CatalogExecutionAccessError>
            {
                self.stub.catalog_read_execution(execution, handle)
            }

            fn catalog_write_execution(
                &self,
                execution: QueryExecutionId,
                handle: &CatalogHandle,
            ) -> Result<ConnectorExecutionWriteBinding, novarocks_worker::CatalogExecutionAccessError>
            {
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
            let runtime = test_execution_runtime(Arc::clone(&facts.stub.memory_authority));
            let pool = RootProducerPool::try_new(
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(64).unwrap(),
                1024 * 1024,
                Arc::clone(&budget),
                runtime.execution_health(),
            )
            .expect("finite test producer pool");

            NativeTaskExecutionHost::new(
                Arc::new(interpreter()),
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
                TaskIdentity::new(
                    identity.query_execution_id(),
                    StageId::new(2).expect("nonzero stage"),
                    TaskId::new(1).expect("nonzero task"),
                    BackendProcessId::new_v7(),
                ),
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
            let facts = Arc::new(WriteContextFacts {
                stub: StubContextFacts::default(),
                execution: Some(recording.clone()),
            });
            let host = Arc::new(writer_host(facts.clone()));
            let identity = super::identity(76, 1, 1);
            let descriptor = writer_descriptor(identity, receiver, UniqueId::new(801, 802));
            let prepared_host = host.clone();
            inspect_native_receiver(
                &host,
                facts,
                descriptor.clone(),
                producer_input(&descriptor, package_carrier(&package)),
                None,
                move |result| {
                    let prepared = result
                        .as_ref()
                        .expect("actual compiled writer producer installs");
                    assert_eq!(prepared.facts().sink_kind(), FragmentSinkKind::DataStream);
                    assert!(prepared_host.task_runtime(identity).is_some());
                    assert!(
                        recording.opened().is_empty(),
                        "a writer opens only when a driver activates"
                    );
                },
            );
            assert!(host.task_runtime(identity).is_none());
        }

        // Without a write execution leased for the recipe's catalog the
        // writer has no capability on this backend: the task is refused
        // before anything is registered.
        #[test]
        fn a_compiled_writer_without_a_leased_write_execution_is_refused() {
            use crate::compiled_writer_binding::tests::writer_producer_package;

            let (package, receiver) = writer_producer_package();
            let facts = Arc::new(WriteContextFacts::default());
            let host = Arc::new(writer_host(facts.clone()));
            let identity = super::identity(77, 1, 1);
            let descriptor = writer_descriptor(identity, receiver, UniqueId::new(803, 804));
            inspect_native_receiver(
                &host,
                facts,
                descriptor.clone(),
                producer_input(&descriptor, package_carrier(&package)),
                None,
                move |result| {
                    let rejection = result.as_ref().expect_err("an unleased writer is refused");
                    use novarocks_execution_contract::failure::{FailureClass, domain};
                    let author = assert_host_refusal_source::<
                        novarocks_worker::CatalogExecutionAccessError,
                    >(
                        rejection,
                        TaskFailureCategory::Protocol,
                        expected_refusal_fact(FailureClass::Dependency, domain::WORKER, 140),
                        "Dependency failure (reason 10:140)",
                    );
                    assert_eq!(
                        author,
                        &novarocks_worker::CatalogExecutionAccessError::LeaseUnavailable {
                            catalog: crate::connector_write_test_support::test_write_catalog_handle(
                            ),
                        }
                    );
                    let binding = crate::backend_test_support::find_source::<
                        crate::compiled_writer_binding::CompiledWriterBindingError,
                    >(rejection)
                    .expect("the original compiled writer site is retained");
                    assert_eq!(binding.node(), Some(1));
                    assert_eq!(binding.detail(), author.to_string());
                },
            );
            assert!(host.task_runtime(identity).is_none());
            assert!(host.split_queues.is_empty(), "the split lease rolled back");
        }
    }
}

#[cfg(test)]
#[path = "memory_admission_projection_tests.rs"]
mod memory_admission_projection_tests;
