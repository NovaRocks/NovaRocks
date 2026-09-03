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

//! The production task side of execution: one frozen descriptor becomes one
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
//! submit_runnable           start the dormant handle on its own thread
//! ```
//!
//! The dormant handle is what makes that split honest. Receiver registration
//! is not a step the fragment kernel exposes on its own: it happens inside
//! `prepare_fragment`, which also builds the pipeline and hands back a
//! [`DormantFragmentHandle`]. So `install_receiver` prepares the whole
//! fragment and parks the handle; `remove_receiver` drops it, and the
//! `FragmentResources` it owns roll every registration back. `submit_runnable`
//! is the only step that can start a thread, and it runs last.
//!
//! This host reads nothing from the fragment-based lifecycle registry. Every
//! query-scoped fact it needs arrives through [`TaskQueryContextFacts`], which
//! the query context half of execution implements.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use novarocks_execution::connector::{
    SplitQueueConfig, SplitQueueError, SplitQueueErrorKind, SplitQueueRegistry,
    SplitSequenceEvidence, TaskAttemptKey, TaskAttemptSplitQueues,
};
use novarocks_execution::exec::fragment::program::{FragmentNodeId, FragmentSinkKind};
use novarocks_execution::runtime::execution_runtime::ExecutionRuntime;
use novarocks_execution::runtime::fragment::io::{
    ExchangeEdgeGates, ExchangeFrameTransmitter, ExchangeReceiverPort, FragmentCommitPort,
    FragmentEventSink, FragmentLookupClient, FragmentResultWriter,
};
use novarocks_execution::runtime::fragment::{
    DormantFragmentHandle, FragmentCancelReason, FragmentOutcome, FragmentTerminalFact,
    RunningFragmentHandle, prepare_fragment,
};
use novarocks_execution::runtime_filter::RuntimeFilterSessionRef;
use novarocks_execution::task_execution::descriptor::{
    ExchangeSource, IngressRejection, TaskDescriptor,
};
use novarocks_execution::task_execution::domain::{CodecOwnedContent, DomainVersion};
use novarocks_execution::task_execution::identity::TaskIdentity;
use novarocks_execution::task_execution::operation::TaskDomainUpdate;
use novarocks_execution::task_execution::status::{
    AbortCause, CancelReason, SafeDetail, TaskFailure, TaskFailureCategory, TaskOutputFacts,
};
use novarocks_proto_codec::connector_read::{MAX_ASSIGNMENT_RETAINED_BYTES, SplitAssignment};
use novarocks_proto_codec::task_execution::domain::stored_message;
use novarocks_proto_models::connector_read as connector_dto;
use novarocks_spi::connector::{
    CatalogHandle, ConnectorStorageResolver, read_stack::ConnectorSession,
};
use novarocks_types::{QueryExecutionId, UniqueId};

use crate::connector::{ConnectorExecutionReadBinding, ConnectorExecutionWriteBinding};
use crate::fragment::decode::plan::context::{
    CatalogReadExecutionResolver, CatalogWriteExecutionResolver, RuntimeFilterSessionResolver,
    TypedScanRuntime,
};
use crate::fragment::decode::request::NativeFragmentRequest;
use crate::fragment::ingress::{ReceivedReadSplit, TypedReadAttemptContext};
use crate::runtime::native_fragment_query::NativeFragmentQueryRuntime;

use super::host::{HostRejection, RunnableTask, TaskExecutionHost};
use super::shared_facts::fragment_plan;
use super::status::TaskStatusReporter;

/// Everything one query context contributes to preparing and running a task.
///
/// A task's plan names catalogs, filter channels, and object stores that
/// belong to the *query*, not to the task. The old stack reached into the
/// fragment lifecycle registry for each of them, which is what made a fragment
/// depend on the lifecycle owner's admission permit. Here they are one
/// injected port, implemented by the query-context half of execution, so the
/// task side holds no query-wide authority of its own.
pub trait TaskQueryContextFacts: Send + Sync {
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

/// Which task, if any, may receive one inbound exchange frame.
///
/// The old stack answered this with a linear scan over every active lifecycle
/// entry. A descriptor already froze its complete inbound topology, so the
/// answer is a single lookup on the kernel key the frame carries, followed by
/// the descriptor's own [`TaskDescriptor::authorize_inbound_frame`].
///
/// Installation is exclusive on that key: two live tasks sharing one kernel
/// key would make a frame ambiguous, and no later check could disambiguate it.
#[derive(Debug, Default)]
pub struct TaskInboundCapabilities {
    installed: Mutex<HashMap<UniqueId, Arc<TaskDescriptor>>>,
}

/// The task and the frozen source one admitted frame belongs to.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct InboundFrameAdmission {
    destination: TaskIdentity,
    source: ExchangeSource,
}

impl InboundFrameAdmission {
    pub const fn destination(self) -> TaskIdentity {
        self.destination
    }

    pub const fn source(self) -> ExchangeSource {
        self.source
    }
}

impl TaskInboundCapabilities {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Whether an inbound exchange frame may be admitted.
    ///
    /// The parameters are exactly the fields a frame carries. An unknown
    /// kernel key answers `UnknownDestinationTask`, which is the same refusal
    /// a live task gives for a frame aimed elsewhere: the data plane must not
    /// be able to tell "not created yet" from "not yours".
    pub fn authorize_frame(
        &self,
        destination_kernel_key: UniqueId,
        destination_node_id: FragmentNodeId,
        source_kernel_key: UniqueId,
        sender_ordinal: u32,
        sender_count: u32,
    ) -> Result<InboundFrameAdmission, IngressRejection> {
        let descriptor = {
            let installed = self.installed.lock().expect(CAPABILITY_LOCK);
            installed.get(&destination_kernel_key).map(Arc::clone)
        };
        let descriptor = descriptor.ok_or(IngressRejection::UnknownDestinationTask)?;
        let source = descriptor.authorize_inbound_frame(
            destination_kernel_key,
            destination_node_id,
            source_kernel_key,
            sender_ordinal,
            sender_count,
        )?;
        Ok(InboundFrameAdmission {
            destination: descriptor.identity(),
            source,
        })
    }

    /// How many tasks currently accept inbound frames.
    pub fn len(&self) -> usize {
        self.installed.lock().expect(CAPABILITY_LOCK).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn install(&self, descriptor: Arc<TaskDescriptor>) -> Result<(), HostRejection> {
        let key = descriptor.fragment_instance_id();
        let mut installed = self.installed.lock().expect(CAPABILITY_LOCK);
        if let Some(existing) = installed.get(&key) {
            // Replacing it would silently re-route the other task's frames,
            // so the second claimant is refused instead.
            return Err(protocol(format!(
                "task {} cannot claim kernel key {key}, which task {} already holds",
                descriptor.identity(),
                existing.identity()
            )));
        }
        installed.insert(key, descriptor);
        Ok(())
    }

    /// Withdraws exactly this task's capability.
    ///
    /// The identity is re-checked because a rollback and a retirement can name
    /// the same kernel key at different times; removing another task's entry
    /// would open a hole no later step closes.
    fn remove(&self, descriptor: &TaskDescriptor) {
        let mut installed = self.installed.lock().expect(CAPABILITY_LOCK);
        let key = descriptor.fragment_instance_id();
        if installed
            .get(&key)
            .is_some_and(|held| held.identity() == descriptor.identity())
        {
            installed.remove(&key);
        }
    }
}

/// The production [`TaskExecutionHost`].
pub struct NativeTaskExecutionHost {
    queries: NativeFragmentQueryRuntime,
    context_facts: Arc<dyn TaskQueryContextFacts>,
    capabilities: Arc<TaskInboundCapabilities>,
    exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
    lookup_client: Arc<dyn FragmentLookupClient>,
    result_writer: Arc<dyn FragmentResultWriter>,
    exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
    commit_port: Arc<dyn FragmentCommitPort>,
    execution_runtime: Arc<ExecutionRuntime>,
    /// Split delivery, keyed by execution and kernel key so a replaced attempt
    /// gets a fresh queue set and can never inherit a sequence space.
    split_queues: Arc<SplitQueueRegistry<ReceivedReadSplit>>,
    tasks: Mutex<HashMap<TaskIdentity, Arc<TaskRuntime>>>,
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
    sink_kind: FragmentSinkKind,
    /// Taken exactly once, by `submit_runnable`. While it is still here the
    /// fragment is prepared but not started, and dropping it rolls every
    /// acquired resource back.
    dormant: Mutex<Option<DormantFragmentHandle>>,
    edges: Arc<ExchangeEdgeGates>,
    splits: Arc<TaskAttemptSplitQueues<ReceivedReadSplit>>,
    read_context: Arc<TypedReadAttemptContext>,
    delivery_expire: Duration,
    query_expire: Duration,
}

const CAPABILITY_LOCK: &str = "task inbound capability lock";
const TASK_LOCK: &str = "native task execution host lock";
const DORMANT_LOCK: &str = "dormant fragment handle lock";

impl NativeTaskExecutionHost {
    #[expect(
        clippy::too_many_arguments,
        reason = "Every execution port this host drives is injected explicitly; \
                  a bundle struct would only move the same list one level away."
    )]
    pub fn new(
        queries: NativeFragmentQueryRuntime,
        context_facts: Arc<dyn TaskQueryContextFacts>,
        capabilities: Arc<TaskInboundCapabilities>,
        exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
        lookup_client: Arc<dyn FragmentLookupClient>,
        result_writer: Arc<dyn FragmentResultWriter>,
        exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
        commit_port: Arc<dyn FragmentCommitPort>,
        execution_runtime: Arc<ExecutionRuntime>,
    ) -> Self {
        Self {
            queries,
            context_facts,
            capabilities,
            exchange_transmitter,
            lookup_client,
            result_writer,
            exchange_receiver_port,
            commit_port,
            execution_runtime,
            split_queues: Arc::new(SplitQueueRegistry::new()),
            tasks: Mutex::new(HashMap::new()),
        }
    }

    /// The frame-admission lookup a data-plane handler consults.
    pub fn inbound_capabilities(&self) -> Arc<TaskInboundCapabilities> {
        Arc::clone(&self.capabilities)
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
        assignment: &SplitAssignment,
    ) -> Result<(), HostRejection> {
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
        let preflight = queue
            .preflight_sequences(node, &sequences)
            .map_err(split_queue_rejection)?;
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
                        ReceivedReadSplit::new(evidence, split)
                    })
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| protocol(format!("split payload is not decodable: {error}")))?
        };
        queue
            .offer_splits(node, received, assignment.no_more_splits())
            .map(|_| ())
            .map_err(split_queue_rejection)
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
    /// Decodes the descriptor's plan and prepares the whole fragment.
    ///
    /// Receiver registration is not separable from preparation: the kernel
    /// registers a task's exchange receivers inside `prepare_fragment`, which
    /// also builds the pipeline. Preparing here and parking the dormant handle
    /// is what makes this step undoable, because dropping that handle rolls
    /// the registration back through `FragmentResources`.
    fn install_receiver(&self, descriptor: &TaskDescriptor) -> Result<(), HostRejection> {
        let identity = descriptor.identity();
        let execution = identity.query_execution_id();
        let kernel_key = descriptor.fragment_instance_id();
        if self.task_runtime(identity).is_some() {
            return Err(internal(format!(
                "task {identity} already has a prepared fragment"
            )));
        }

        let wire = fragment_plan(descriptor.plan().as_ref())?;
        let attempt = TaskAttemptKey::new(execution, kernel_key);
        let splits = self.split_queues.open_attempt(
            attempt,
            SplitQueueConfig {
                max_queued_bytes: MAX_ASSIGNMENT_RETAINED_BYTES,
            },
        );
        // The queue set is opened before decode so a scan can start and block
        // before its first split arrives, which means every refusal below has
        // to close it again. The lease does that structurally.
        let mut lease = SplitQueueLease::held(&self.split_queues, attempt);
        let read_context = Arc::new(TypedReadAttemptContext::new());
        let typed_runtime = self.typed_scan_runtime(
            execution,
            kernel_key,
            Arc::clone(&read_context),
            Arc::clone(&splits),
        )?;

        let request = NativeFragmentRequest::try_decode_with_runtime(
            execution,
            wire.plan().clone(),
            wire.instance_params().clone(),
            self.queries.connector_cancellation_for_execution(execution),
            Duration::from_millis(self.execution_runtime.config().exchange_wait_ms),
            Some(typed_runtime),
        )
        .map_err(|error| protocol(format!("task {identity} plan is not decodable: {error}")))?;

        // The descriptor is the protocol authority over this task's kernel
        // key; the plan carries its own copy. Two different answers would let
        // frames be admitted under one key and delivered under another, so
        // they are compared rather than reconciled.
        if request.fragment_instance_id() != kernel_key {
            return Err(protocol(format!(
                "task {identity} froze kernel key {kernel_key} but its plan carries {}",
                request.fragment_instance_id()
            )));
        }
        let expects_bindings = request.has_runtime_filter_bindings();
        let (delivery_expire, query_expire) = request.query_expire_durations();
        let submission = request.into_submission();
        // Same reasoning for parallelism: preparing at the plan's value would
        // run the task at a degree the frontend never agreed to.
        if submission.instance().pipeline_dop() != descriptor.pipeline_dop() {
            return Err(protocol(format!(
                "task {identity} froze pipeline dop {} but its plan carries {}",
                descriptor.pipeline_dop(),
                submission.instance().pipeline_dop()
            )));
        }

        let edges = ExchangeEdgeGates::from_frozen_edges(descriptor.topology().outbound())
            .map_err(|error| {
                protocol(format!(
                    "task {identity} outbound topology has no legal gate set: {error}"
                ))
            })?;

        let runtime_filter =
            self.context_facts
                .runtime_filter_session(execution, kernel_key, expects_bindings)?;
        let admission = self
            .queries
            .prepare_admission_execution(
                execution,
                kernel_key,
                delivery_expire,
                query_expire,
                runtime_filter,
            )
            .map_err(|error| {
                resource_exhausted(format!("task {identity} could not be admitted: {error}"))
            })?;
        let context = admission
            .into_prepare_context(
                None,
                Arc::clone(&self.exchange_transmitter),
                Arc::clone(&self.lookup_client),
                Arc::clone(&self.result_writer),
                self.context_facts
                    .runtime_filter_event_sink(execution, kernel_key),
            )
            .with_fragment_commit_port(Arc::clone(&self.commit_port))
            .with_exchange_receiver_port(Arc::clone(&self.exchange_receiver_port))
            .with_execution_runtime(Arc::clone(&self.execution_runtime))
            // Binding the gates here is what makes the closed-edge barrier
            // real: the sinks this fragment builds consult them before every
            // send, so a producer cannot reach a destination that has not
            // acknowledged its own creation.
            .with_edge_gates(Arc::clone(&edges));

        // This is the receiver install. It registers every inbound exchange
        // receiver and builds the pipeline in one step, and its rollback is
        // dropping the handle it returns.
        let dormant = prepare_fragment(submission, context).map_err(|error| {
            resource_exhausted(format!("task {identity} could not be prepared: {error}"))
        })?;

        self.tasks.lock().expect(TASK_LOCK).insert(
            identity,
            Arc::new(TaskRuntime {
                attempt,
                sink_kind: descriptor.sink_kind(),
                dormant: Mutex::new(Some(dormant)),
                edges,
                splits,
                read_context,
                delivery_expire,
                query_expire,
            }),
        );
        lease.retain();
        Ok(())
    }

    /// Drops everything `install_receiver` prepared.
    ///
    /// Idempotent, because the owner calls it both to roll a creation back and
    /// to retire a task that already ran. In the first case the dormant handle
    /// is still parked and its drop rolls the registrations back; in the
    /// second it was taken by `submit_runnable` and the running fragment
    /// already finished them.
    fn remove_receiver(&self, descriptor: &TaskDescriptor) {
        let removed = self
            .tasks
            .lock()
            .expect(TASK_LOCK)
            .remove(&descriptor.identity());
        if let Some(runtime) = removed {
            // Closing the queues wakes any scan still blocked on a split that
            // will now never arrive.
            self.split_queues.close_attempt(runtime.attempt);
            drop(runtime);
        }
    }

    fn install_inbound_capability(&self, descriptor: &TaskDescriptor) -> Result<(), HostRejection> {
        self.capabilities.install(Arc::new(descriptor.clone()))
    }

    fn remove_inbound_capability(&self, descriptor: &TaskDescriptor) {
        self.capabilities.remove(descriptor);
    }

    /// Starts the prepared fragment on its own thread.
    ///
    /// This is the last install step and the only one that can start a
    /// thread. Everything that could fail has already run, so a failure here
    /// is a resource failure and leaves no worker behind: if the spawn is
    /// refused the dormant handle and the pre-start registration lease are
    /// dropped, which rolls both back.
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

        let registration = self
            .queries
            .register_fragment_execution(
                execution,
                kernel_key,
                runtime.delivery_expire,
                runtime.query_expire,
            )
            .map_err(|error| {
                resource_exhausted(format!("task {identity} could not be registered: {error}"))
            })?;
        // A split may arrive the moment this create is acknowledged, so the
        // decoded read bindings become resolvable exactly here — after
        // preparation proved every plan node decodes, and before any worker
        // can block on an empty queue.
        runtime.read_context.publish();

        let task = Arc::new(NativeRunnableTask::new(identity, kernel_key));
        let worker = Arc::clone(&task);
        let queries = self.queries.clone();
        let split_queues = Arc::clone(&self.split_queues);
        let attempt = runtime.attempt;
        let sink_kind = runtime.sink_kind;
        std::thread::Builder::new()
            .name(format!(
                "native-task-{:x}-{:x}",
                kernel_key.high(),
                kernel_key.low()
            ))
            .spawn(move || {
                // The pre-start lease keeps the query route rollback-capable
                // while this worker is dormant; only the thread that starts it
                // may make it live.
                registration.into_running();
                // RUNNING is published before the drivers are submitted
                // because it is the route becoming live that the status
                // describes, and because no terminal state is reachable from
                // PLANNED: a task that never publishes RUNNING could not
                // publish FINISHED either.
                reporter.running();
                let running = dormant.start();
                // Replays a stand-down that arrived while this task was
                // submitted but not yet started. Without it, a cancel racing
                // the worker's first instruction would be dropped and the
                // task would run to completion after being told to stop.
                worker.attach(Arc::new(running.clone()));
                let fact = running.join();
                report_terminal(&reporter, sink_kind, &fact, worker.stand_down());
                worker.finish();
                split_queues.close_attempt(attempt);
                queries.unregister_fragment_execution(execution, kernel_key);
                queries.finish_fragment(execution);
            })
            .map_err(|error| {
                resource_exhausted(format!("spawn native task worker failed: {error}"))
            })?;
        Ok(task)
    }

    fn apply_task_domain(
        &self,
        descriptor: &TaskDescriptor,
        domain: &TaskDomainUpdate,
    ) -> Result<(), HostRejection> {
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
                self.deliver_splits(&runtime, &assignment)
            }
            TaskDomainUpdate::TaskDynamicFilter { version, payload } => {
                self.context_facts.deliver_task_dynamic_filter(
                    identity.query_execution_id(),
                    descriptor.fragment_instance_id(),
                    *version,
                    payload,
                )
            }
            TaskDomainUpdate::OpenExchangeEdges { version, edges } => runtime
                .edges
                .open(*version, edges)
                .map(|_| ())
                .map_err(|conflict| {
                    protocol(format!("task {identity} edge open is illegal: {conflict}"))
                }),
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
/// later reason never rewrites the first one, which is the same first-wins
/// rule the status owner applies to termination.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum StandDown {
    Cancel(CancelReason),
    Abort(AbortCause),
}

impl StandDown {
    fn reason(self) -> FragmentCancelReason {
        match self {
            Self::Cancel(reason) => FragmentCancelReason::new(reason.as_str()),
            Self::Abort(cause) => FragmentCancelReason::new(cause.as_str()),
        }
    }
}

#[derive(Default)]
struct RunnableState {
    handle: Option<Arc<dyn FragmentStandDown>>,
    stand_down: Option<StandDown>,
    finished: bool,
}

/// One submitted task, as the owner may address it.
pub struct NativeRunnableTask {
    identity: TaskIdentity,
    fragment_instance_id: UniqueId,
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
    fn new(identity: TaskIdentity, fragment_instance_id: UniqueId) -> Self {
        Self {
            identity,
            fragment_instance_id,
            state: Mutex::new(RunnableState::default()),
        }
    }

    /// Publishes the started fragment and replays any latched stand-down.
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
    /// worker has its terminal fact, so the kernel handle's last drop belongs
    /// to the worker rather than to a later canceller.
    fn finish(&self) {
        let mut state = self.state.lock().expect(RUNNABLE_LOCK);
        state.finished = true;
        state.handle = None;
    }

    fn request(&self, stand_down: StandDown) {
        let handle = {
            let mut state = self.state.lock().expect(RUNNABLE_LOCK);
            if state.finished || state.stand_down.is_some() {
                return;
            }
            state.stand_down = Some(stand_down);
            state.handle.as_ref().map(Arc::clone)
        };
        // An absent handle means the worker has not started the fragment yet.
        // The latch set above is what `attach` replays.
        if let Some(handle) = handle {
            handle.cancel(stand_down.reason());
        }
    }
}

impl RunnableTask for NativeRunnableTask {
    fn cancel(&self, reason: CancelReason) {
        self.request(StandDown::Cancel(reason));
    }

    fn abort(&self, cause: AbortCause) {
        self.request(StandDown::Abort(cause));
    }
}

/// Publishes one fragment's terminal fact as this task's terminal status.
///
/// The root task is the one asymmetry, and it is deliberate: its output
/// responsibility ends when the frontend drains the result stream, not when
/// the pipeline stops producing. So a successful root reports `FLUSHING` and
/// lets the result plane finish it; every other sink owes nothing further.
fn report_terminal(
    reporter: &TaskStatusReporter,
    sink_kind: FragmentSinkKind,
    fact: &FragmentTerminalFact,
    stand_down: Option<StandDown>,
) {
    // A task that was told to stand down completes that stand-down whatever
    // its pipeline did next. The owner latched the termination and already
    // published the terminating state, and first-wins means the pipeline's own
    // outcome — including a success that raced the cancel, which is exactly
    // what a `LIMIT` query produces — cannot rewrite it.
    if let Some(stand_down) = stand_down {
        match stand_down {
            StandDown::Cancel(reason) => {
                reporter.canceling(reason);
                reporter.canceled(reason);
            }
            StandDown::Abort(cause) => {
                reporter.aborting(cause);
                reporter.aborted(cause);
            }
        }
        reporter.release_output();
        return;
    }
    match fact.outcome() {
        FragmentOutcome::Succeeded => match sink_kind {
            // The root's output responsibility ends when the frontend drains
            // the result stream, not when the pipeline stops producing, so it
            // is the result plane that publishes FINISHED.
            FragmentSinkKind::Result => {
                reporter.flushing();
            }
            _ => {
                reporter.finished(TaskOutputFacts::new(true));
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
            report_failure(
                reporter,
                TaskFailure::new(
                    TaskFailureCategory::Execution,
                    SafeDetail::truncating(&error.to_string()),
                ),
            );
        }
    }
}

/// Publishes FAILING before FAILED.
///
/// The terminal is unreachable in one step from RUNNING: the state machine
/// requires the terminating state first, so skipping it would leave the task
/// running forever in the owner's view.
fn report_failure(reporter: &TaskStatusReporter, failure: TaskFailure) {
    reporter.failing(failure.clone());
    reporter.failed(failure);
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
    HostRejection::new(category, error.to_string())
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

/// This process cannot supply what the task needs right now.
fn resource_exhausted(detail: impl AsRef<str>) -> HostRejection {
    HostRejection::new(TaskFailureCategory::ResourceExhausted, detail)
}

#[cfg(test)]
mod tests {
    use super::{
        FragmentStandDown, InboundFrameAdmission, NativeRunnableTask, NativeTaskExecutionHost,
        StandDown, TaskInboundCapabilities, TaskQueryContextFacts, report_terminal,
    };

    use std::num::{NonZeroU32, NonZeroUsize};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use novarocks_execution::exec::fragment::program::{FragmentNodeId, FragmentSinkKind};
    use novarocks_execution::exec::fragment::sink::DataStreamPartitionType;
    use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
    use novarocks_execution::runtime::execution_runtime::{
        ExecutionRuntime, ExecutionRuntimeConfig, ExecutionSpillStorageConfig,
    };
    use novarocks_execution::runtime::fragment::io::{
        FragmentEventSink, NoopFragmentEventSink, UnavailableExchangeReceiverPort,
    };
    use novarocks_execution::runtime::fragment::{
        FragmentCancelReason, FragmentExecutionError, FragmentExecutionErrorKind, FragmentOutcome,
        FragmentTerminalFact,
    };
    use novarocks_execution::runtime_filter::RuntimeFilterSessionRef;
    use novarocks_execution::task_execution::descriptor::{
        ExchangeDestination, ExchangeEdge, ExchangeInbound, ExchangeSource, ExchangeTopology,
        IngressRejection, PhysicalFragmentPlan, TaskDescriptor,
    };
    use novarocks_execution::task_execution::domain::{
        CodecOwnedContent, ContentFingerprint, DomainVersion, EdgeOpenVersion, ExchangeEdgeId,
        PlanNodeId, SplitSequence,
    };
    use novarocks_execution::task_execution::identity::TaskIdentity;
    use novarocks_execution::task_execution::operation::TaskDomainUpdate;
    use novarocks_execution::task_execution::status::{AbortCause, CancelReason, TaskState};
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_codec::task_execution::descriptor::WireFragmentPlan;
    use novarocks_proto_models::{
        common, connector_read as connector_dto, novarocks as proto, plan,
    };
    use novarocks_spi::connector::{
        CatalogHandle, ConnectorError, ConnectorErrorKind, ConnectorStorageResolver,
        ResolvedVendedS3Access, StorageAccessRequest,
    };
    use novarocks_types::UniqueId;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    use crate::connector::{ConnectorExecutionReadBinding, ConnectorExecutionWriteBinding};
    use crate::runtime::native_fragment_query::NativeFragmentQueryRuntime;
    use crate::task_execution::clock::ProcessMonotonicClock;
    use crate::task_execution::host::{HostRejection, RunnableTask, TaskExecutionHost};
    use crate::task_execution::observation::TaskStatusSource;
    use crate::task_execution::status::{
        METRIC_PUBLISH_MIN_INTERVAL, StatusAdvance, TaskStatusOwner, TaskStatusReporter,
    };

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

    /// A decodable, self-contained fragment: one VALUES node into a NOOP sink.
    fn wire_plan(
        query: QueryId,
        kernel_key: UniqueId,
        pipeline_dop: i32,
    ) -> Arc<dyn PhysicalFragmentPlan> {
        let wire = WireFragmentPlan::parse(
            proto::TaskFragmentPlan {
                plan: Some(plan::PlanFragment {
                    fragment_id: 7,
                    root: Some(plan::DistributedNode {
                        node_id: 10,
                        fragment_id: 7,
                        limit: -1,
                        payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                            output_columns: Vec::new(),
                            kind: Some(plan::plan_node::Kind::Values(plan::ValuesNode {
                                rows: Vec::new(),
                                columns: Vec::new(),
                            })),
                        })),
                        ..Default::default()
                    }),
                    sink: Some(plan::DataSink {
                        kind: Some(plan::data_sink::Kind::Noop(true)),
                    }),
                    runtime_filter_bindings: Some(plan::RuntimeFilterBindingTable {
                        fragment_id: 7,
                        bindings: Vec::new(),
                    }),
                    ..Default::default()
                }),
                instance_params: Some(proto::InstanceParams {
                    query_id: Some(common::UniqueId {
                        hi: query.high(),
                        lo: query.low(),
                    }),
                    fragment_instance_id: Some(common::UniqueId {
                        hi: kernel_key.high(),
                        lo: kernel_key.low(),
                    }),
                    backend_num: 3,
                    query_options: Some(proto::QueryOptions {
                        pipeline_dop,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            },
            FieldPath::root("plan"),
        )
        .expect("a legal fragment plan");
        Arc::new(wire)
    }

    /// A plan handle this backend's codec did not produce.
    #[derive(Debug)]
    struct ForeignPlan;

    impl CodecOwnedContent for ForeignPlan {
        fn fingerprint(&self) -> ContentFingerprint {
            ContentFingerprint::from_bytes([0x5a; 16])
        }

        fn encoded_len(&self) -> usize {
            16
        }
    }

    impl PhysicalFragmentPlan for ForeignPlan {
        fn contract_version(
            &self,
        ) -> novarocks_execution::exec::fragment::program::FragmentContractVersion {
            novarocks_execution::exec::fragment::program::FragmentContractVersion::CURRENT
        }

        fn sink_kind(&self) -> FragmentSinkKind {
            FragmentSinkKind::Noop
        }
    }

    fn descriptor_with(
        identity: TaskIdentity,
        kernel_key: UniqueId,
        pipeline_dop: usize,
        topology: ExchangeTopology,
        plan: Arc<dyn PhysicalFragmentPlan>,
    ) -> TaskDescriptor {
        TaskDescriptor::try_new(
            identity,
            kernel_key,
            NonZeroUsize::new(pipeline_dop).expect("nonzero dop"),
            vec![PlanNodeId::new(10).expect("nonnegative node")],
            topology,
            plan,
        )
        .expect("a legal descriptor")
    }

    /// A descriptor whose plan agrees with it on every frozen fact.
    fn consistent_descriptor(identity: TaskIdentity, kernel_key: UniqueId) -> TaskDescriptor {
        descriptor_with(
            identity,
            kernel_key,
            1,
            ExchangeTopology::default(),
            wire_plan(identity.query_execution_id().query_id(), kernel_key, 1),
        )
    }

    fn inbound_topology(node: FragmentNodeId, sources: Vec<ExchangeSource>) -> ExchangeTopology {
        ExchangeTopology::try_new(
            Vec::new(),
            vec![ExchangeInbound::try_new(node, sources).expect("a legal inbound")],
        )
        .expect("a legal topology")
    }

    fn outbound_topology(edge: ExchangeEdgeId, target: TaskIdentity) -> ExchangeTopology {
        let destination = ExchangeDestination::try_new(
            target,
            UniqueId::new(900, 901),
            RuntimeEndpoint::new("127.0.0.1", 9060).expect("a legal endpoint"),
            FragmentNodeId::new(11),
            0,
            NonZeroU32::new(1).expect("nonzero"),
        )
        .expect("a legal destination");
        ExchangeTopology::try_new(
            vec![
                ExchangeEdge::try_new(
                    edge,
                    FragmentNodeId::new(11),
                    DataStreamPartitionType::Unpartitioned,
                    vec![destination],
                )
                .expect("a legal edge"),
            ],
            Vec::new(),
        )
        .expect("a legal topology")
    }

    /// A context host that answers every query-scoped question with the
    /// smallest legal value, and counts what it was asked.
    #[derive(Default)]
    struct StubContextFacts {
        filter_sessions_requested: AtomicUsize,
        dynamic_filters_delivered: AtomicUsize,
    }

    impl TaskQueryContextFacts for StubContextFacts {
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

        fn runtime_filter_event_sink(
            &self,
            _execution: QueryExecutionId,
            _fragment_instance_id: UniqueId,
        ) -> Arc<dyn FragmentEventSink> {
            Arc::new(NoopFragmentEventSink)
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

    fn test_execution_runtime() -> Arc<ExecutionRuntime> {
        Arc::new(
            ExecutionRuntime::new(ExecutionRuntimeConfig {
                driver_threads: 1,
                scan_threads: 1,
                scan_queue_capacity: 1,
                spill_io_threads: 1,
                spill_io_queue_capacity: 1,
                spill_storage: ExecutionSpillStorageConfig::default(),
                exchange_wait_ms: 120_000,
                exchange_io_threads: 1,
                exchange_io_max_inflight_bytes: 1,
                exchange_max_transmit_batched_bytes: 1,
                operator_buffer_chunks: 1,
                local_exchange_buffer_mem_limit_per_driver: 1,
                local_exchange_max_buffered_rows: 1,
                connector_io_tasks_per_scan_operator: 1,
                scan_submit_fail_max: 1,
                scan_submit_fail_timeout_ms: 1,
                runtime_filter_scan_wait_time_ms_override: None,
                runtime_filter_wait_timeout_ms_override: None,
                sink_io_worker_threads: 1,
                sink_io_max_blocking_threads: 1,
            })
            .expect("test execution runtime"),
        )
    }

    fn host(facts: Arc<StubContextFacts>) -> NativeTaskExecutionHost {
        let data_runtime = crate::rpc::runtime::test_backend_data_runtime();
        NativeTaskExecutionHost::new(
            NativeFragmentQueryRuntime::global(),
            facts,
            TaskInboundCapabilities::new(),
            crate::fragment::grpc_exchange_transmitter(data_runtime.clone()),
            crate::fragment::grpc_fragment_lookup_client(data_runtime),
            crate::fragment::native_result_writer(),
            Arc::new(UnavailableExchangeReceiverPort),
            Arc::new(crate::runtime::sink_commit::BackendSinkCommitPort),
            test_execution_runtime(),
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
            inbound_topology(node, vec![ExchangeSource::new(producer, producer_key)]),
            wire_plan(consumer.query_execution_id().query_id(), kernel_key, 1),
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
        assert_eq!(
            capabilities.authorize_frame(kernel_key, node, producer_key, 0, 1),
            Ok(InboundFrameAdmission {
                destination: consumer,
                source: ExchangeSource::new(producer, producer_key),
            })
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
                inbound_topology(node, vec![ExchangeSource::new(producer, producer_key)]),
                wire_plan(consumer.query_execution_id().query_id(), kernel_key, 1),
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
        let task = NativeRunnableTask::new(identity(14, 1, 1), UniqueId::new(81, 82));
        let handle = Arc::new(RecordingStandDown::default());

        // The owner may cancel between `submit_runnable` returning and the
        // worker starting the fragment. Dropping that cancel would let the
        // task run to completion after being told to stop.
        task.cancel(CancelReason::UpstreamNoLongerNeeded);
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
        let task = NativeRunnableTask::new(identity(16, 1, 1), UniqueId::new(101, 102));
        let handle = Arc::new(RecordingStandDown::default());
        task.attach(Arc::clone(&handle) as Arc<dyn super::FragmentStandDown>);
        assert!(handle.reasons.lock().expect("reasons").is_empty());

        task.abort(AbortCause::LeaseExpired);
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

    // ------------------------------------------------------ install refusals

    #[test]
    fn a_plan_this_codec_did_not_produce_cannot_be_installed() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(17, 1, 1);
        let descriptor = descriptor_with(
            task,
            UniqueId::new(111, 112),
            1,
            ExchangeTopology::default(),
            Arc::new(ForeignPlan),
        );

        let rejection = host
            .install_receiver(&descriptor)
            .expect_err("a foreign plan has no decodable representation");
        assert!(
            rejection
                .detail()
                .as_str()
                .contains("not a codec-produced fragment plan"),
            "{rejection}"
        );
        // Nothing was admitted, so nothing needs undoing.
        assert_eq!(facts.filter_sessions_requested.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a_descriptor_whose_kernel_key_disagrees_with_its_plan_is_refused() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(18, 1, 1);
        // The descriptor freezes one kernel key; the encoded plan carries
        // another. Accepting this would install receivers under one key while
        // frames were admitted under the other.
        let descriptor = descriptor_with(
            task,
            UniqueId::new(121, 122),
            1,
            ExchangeTopology::default(),
            wire_plan(
                task.query_execution_id().query_id(),
                UniqueId::new(131, 132),
                1,
            ),
        );

        let rejection = host
            .install_receiver(&descriptor)
            .expect_err("two kernel keys is not a task");
        assert!(
            rejection.detail().as_str().contains("froze kernel key"),
            "{rejection}"
        );
    }

    #[test]
    fn a_descriptor_whose_pipeline_dop_disagrees_with_its_plan_is_refused() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(19, 1, 1);
        let kernel_key = UniqueId::new(141, 142);
        // The descriptor is the protocol authority over dop. Preparing at the
        // plan's value instead would silently run the task at a parallelism
        // the frontend never agreed to.
        let descriptor = descriptor_with(
            task,
            kernel_key,
            4,
            ExchangeTopology::default(),
            wire_plan(task.query_execution_id().query_id(), kernel_key, 1),
        );

        let rejection = host
            .install_receiver(&descriptor)
            .expect_err("two pipeline dops is not a task");
        assert!(
            rejection.detail().as_str().contains("froze pipeline dop"),
            "{rejection}"
        );
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

    #[test]
    fn a_prepared_task_opens_only_the_edges_its_descriptor_froze() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let producer = identity(21, 1, 1);
        let consumer = identity(21, 2, 1);
        let kernel_key = UniqueId::new(161, 162);
        let edge = ExchangeEdgeId::new(1).expect("nonzero edge");
        let descriptor = descriptor_with(
            producer,
            kernel_key,
            1,
            outbound_topology(edge, consumer),
            wire_plan(producer.query_execution_id().query_id(), kernel_key, 1),
        );

        host.install_receiver(&descriptor)
            .expect("a consistent descriptor prepares");
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
    fn a_task_dynamic_filter_is_handed_to_the_query_context_participant() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(22, 1, 1);
        let kernel_key = UniqueId::new(171, 172);
        let descriptor = consistent_descriptor(task, kernel_key);
        host.install_receiver(&descriptor).expect("prepares");

        // The filter participant belongs to the query context, not to this
        // task: delivering it here would create a second owner of the same
        // installed contract.
        host.apply_task_domain(
            &descriptor,
            &TaskDomainUpdate::TaskDynamicFilter {
                version: DomainVersion::new(1).expect("nonzero"),
                payload: Arc::new(
                    novarocks_proto_codec::task_execution::domain::WireContent::new(
                        b"filter",
                        novarocks_proto_models::filter::RuntimeFilterEnvelope::default(),
                    ),
                ),
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
        host.install_receiver(&descriptor).expect("prepares");

        // A second preparation would leave the first one's receivers and
        // pipeline with nothing that can ever roll them back.
        let rejection = host
            .install_receiver(&descriptor)
            .expect_err("one identity, one fragment");
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
        no_more: bool,
    ) -> TaskDomainUpdate {
        let assignment = connector_dto::SplitAssignment {
            plan_node_id: payload_node,
            splits,
            no_more_splits: no_more,
        };
        let payload: Arc<dyn CodecOwnedContent> = Arc::new(
            novarocks_proto_codec::task_execution::domain::WireContent::new(b"split", assignment),
        );
        TaskDomainUpdate::SplitAssignment(
            novarocks_execution::task_execution::operation::SplitAssignmentIntent::new(
                node,
                SplitSequence::FIRST,
                SplitSequence::FIRST,
                no_more,
                payload,
            )
            .expect("first equals last"),
        )
    }

    #[test]
    fn a_split_payload_of_another_domain_is_refused_rather_than_reinterpreted() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(27, 1, 1);
        let descriptor = consistent_descriptor(task, UniqueId::new(221, 222));
        host.install_receiver(&descriptor).expect("prepares");

        // A well-formed handle of a *different* domain must not be read as a
        // split batch: the queue would then accept sequence numbers that no
        // scheduler ever issued.
        let update = TaskDomainUpdate::SplitAssignment(
            novarocks_execution::task_execution::operation::SplitAssignmentIntent::new(
                PlanNodeId::new(10).expect("nonnegative node"),
                SplitSequence::FIRST,
                SplitSequence::FIRST,
                true,
                Arc::new(
                    novarocks_proto_codec::task_execution::domain::WireContent::new(
                        b"filter",
                        novarocks_proto_models::filter::RuntimeFilterEnvelope::default(),
                    ),
                ),
            )
            .expect("first equals last"),
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
        host.install_receiver(&descriptor).expect("prepares");

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
                    true,
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
        host.install_receiver(&descriptor).expect("prepares");

        // An assignment with no splits carries only the terminal marker. It
        // must not demand a typed read execution: a node that was pruned to
        // zero splits still has to be sealed, or its scan waits forever.
        host.apply_task_domain(
            &descriptor,
            &split_intent(
                PlanNodeId::new(10).expect("nonnegative node"),
                10,
                Vec::new(),
                true,
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
        host.install_receiver(&descriptor).expect("prepares");

        // This fragment's only node is a VALUES node, so nothing registered a
        // connector read. Enqueuing the split anyway would fill a queue that
        // no scan operator will ever drain.
        let rejection = host
            .apply_task_domain(
                &descriptor,
                &split_intent(
                    PlanNodeId::new(10).expect("nonnegative node"),
                    10,
                    vec![crate::connector::typed_runtime::test_support::split_proto(
                        10, 1,
                    )],
                    false,
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

    fn reporter_for(identity: TaskIdentity) -> (Arc<TaskStatusOwner>, TaskStatusReporter) {
        let owner = Arc::new(TaskStatusOwner::new(
            identity,
            Arc::new(TaskStatusSource::new()),
            Arc::new(ProcessMonotonicClock::new()),
            METRIC_PUBLISH_MIN_INTERVAL,
        ));
        owner.release_to_observers();
        (Arc::clone(&owner), TaskStatusReporter::new(owner))
    }

    /// Waits for a task to reach a terminal status, or gives up.
    ///
    /// The worker runs on its own thread, so a poll is the only way to observe
    /// it. The bound is generous and only exists so a hang fails the test
    /// instead of blocking the suite forever.
    fn await_terminal(owner: &TaskStatusOwner) -> TaskState {
        for _ in 0..600 {
            if owner.is_terminal() {
                return owner.state();
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("task did not reach a terminal state: {:?}", owner.state());
    }

    #[test]
    fn a_submitted_task_runs_and_publishes_its_own_terminal_status() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(24, 1, 1);
        let kernel_key = UniqueId::new(191, 192);
        let descriptor = consistent_descriptor(task, kernel_key);
        let (owner, reporter) = reporter_for(task);

        host.install_receiver(&descriptor).expect("prepares");
        host.install_inbound_capability(&descriptor)
            .expect("installs");
        let runnable = host
            .submit_runnable(&descriptor, reporter)
            .expect("a prepared fragment starts");

        // A NOOP sink owes nothing further, so the worker itself is the only
        // thing that may publish FINISHED. If the worker never reported, this
        // would hang at PLANNED and the poll would fail.
        assert_eq!(await_terminal(&owner), TaskState::Finished);
        assert!(
            owner.output_released(),
            "a finished non-root task has released its output responsibility"
        );
        // Standing a finished task down must be inert rather than a panic on
        // an already-consumed handle.
        runnable.abort(AbortCause::QueryFailed);

        host.remove_inbound_capability(&descriptor);
        host.remove_receiver(&descriptor);
    }

    #[test]
    fn submitting_a_task_that_was_never_prepared_is_refused_before_a_thread_exists() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(25, 1, 1);
        let descriptor = consistent_descriptor(task, UniqueId::new(201, 202));
        let (_owner, reporter) = reporter_for(task);

        // `submit_runnable` is the only step that can start a thread, so it
        // must refuse an unprepared task rather than spawn a worker with
        // nothing to run.
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
            Vec::new(),
        )
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
    fn an_abort_racing_a_submitted_task_still_reaches_a_terminal_status() {
        let facts = Arc::new(StubContextFacts::default());
        let host = host(Arc::clone(&facts));
        let task = identity(35, 1, 1);
        let descriptor = consistent_descriptor(task, UniqueId::new(261, 262));
        let (owner, reporter) = reporter_for(task);

        host.install_receiver(&descriptor).expect("prepares");
        let runnable = host
            .submit_runnable(&descriptor, reporter)
            .expect("a prepared fragment starts");

        // The owner publishes ABORTING and then asks the task to stand down,
        // which may land before or after the worker started the fragment.
        // Either way the task must terminate: a stand-down lost in that window
        // would leave the query waiting for a task nothing can stop.
        owner.force_terminal(AbortCause::QueryFailed);
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

        host.install_receiver(&descriptor).expect("prepares");
        let _runnable = host
            .submit_runnable(&descriptor, reporter.clone())
            .expect("first submit starts the fragment");

        // The dormant handle is taken exactly once. A second submit must not
        // start a second worker over the same pipeline.
        let rejection = host
            .submit_runnable(&descriptor, reporter)
            .expect_err("one fragment, one worker");
        assert!(
            rejection.detail().as_str().contains("submitted twice"),
            "{rejection}"
        );

        await_terminal(&owner);
        host.remove_receiver(&descriptor);
    }
}
