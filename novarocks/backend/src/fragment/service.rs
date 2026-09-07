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

use std::collections::{BTreeMap, BTreeSet};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(test)]
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use crate::runtime::native_fragment_query::NativeFragmentQueryRuntime;
use crate::runtime::sink_commit::{BackendSinkCommitPort, ConfiguredBackendSinkCommitPort};
use novarocks_execution::runtime::execution_runtime::ExecutionRuntime;
#[cfg(test)]
use novarocks_execution::runtime::execution_runtime::ExecutionRuntimeConfig;
use novarocks_execution::runtime::fragment::io::FragmentLookupClient;
use novarocks_execution::runtime::fragment::io::{
    ExchangeFrameTransmitter, ExchangeReceiverPort, FragmentCommitPort, FragmentResultWriter,
    UnavailableExchangeReceiverPort,
};
use novarocks_execution::runtime::fragment::{
    FragmentCancelReason, FragmentOutcome, RunningFragmentHandle, prepare_fragment,
};
use novarocks_execution::runtime::profile::Profiler;
use novarocks_proto_codec::lifecycle::{QueryExecutionId, StageFragment};
use novarocks_spi::connector::WriteCommitEvidenceLimits;
use tracing::error;

use super::control::{FragmentControlHandle, FragmentControlRegistry};
#[cfg(test)]
use super::failure_injection::start_with_configured_fragment_failure_trigger;
use super::failure_injection::{
    FRAGMENT_EXECUTOR_FAILURE_MESSAGE, claim_configured_fragment_failure_trigger,
};
use crate::fragment::decode::request::NativeFragmentRequest;
use crate::fragment::ingress::{
    NativeFragmentCancelRequest, NativeFragmentIngress, NativeFragmentIngressError,
};
use crate::query_lifecycle::{
    QueryLifecycleRegistry, QueryLifecycleTerminalCleanup, stage::StartGate,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NativeFragmentLifecycleEvent {
    Prepared,
    Registered,
    Cancelled,
    Accepted,
    Started,
}

type LifecycleObserver = Arc<dyn Fn(NativeFragmentLifecycleEvent) + Send + Sync>;

/// The native task-update protocol owns this ceiling.  Execution receives the
/// value as configuration and therefore does not depend on ProtoCodec.
fn split_queue_config() -> novarocks_execution::connector::SplitQueueConfig {
    novarocks_execution::connector::SplitQueueConfig {
        max_queued_bytes: novarocks_proto_codec::connector_read::MAX_ASSIGNMENT_RETAINED_BYTES,
    }
}

fn split_queue_rejection(
    error: novarocks_execution::connector::SplitQueueError,
) -> crate::query_lifecycle::task_update::TaskUpdateAck {
    use crate::query_lifecycle::task_update::{TaskUpdateAck, TaskUpdateRejectionReason};
    use novarocks_execution::connector::SplitQueueErrorKind;

    let reason = match error.kind() {
        SplitQueueErrorKind::PlanNodeMismatch => TaskUpdateRejectionReason::UnknownPlanNode,
        SplitQueueErrorKind::SequenceConflict => TaskUpdateRejectionReason::SequenceConflict,
        SplitQueueErrorKind::AfterNoMoreSplits => TaskUpdateRejectionReason::AfterNoMoreSplits,
        SplitQueueErrorKind::Closed => TaskUpdateRejectionReason::Terminated,
        SplitQueueErrorKind::ResourceExhausted => TaskUpdateRejectionReason::ResourceExhausted,
    };
    TaskUpdateAck::rejected(reason, error.to_string())
}

#[cfg(test)]
fn test_execution_runtime() -> Arc<ExecutionRuntime> {
    Arc::new(
        ExecutionRuntime::new(ExecutionRuntimeConfig {
            driver_threads: 1,
            scan_threads: 1,
            scan_queue_capacity: 1,
            spill_io_threads: 1,
            spill_io_queue_capacity: 1,
            spill_storage: novarocks_execution::runtime::execution_runtime::ExecutionSpillStorageConfig::default(),
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

pub struct NativeFragmentService {
    pub(super) controls: Arc<FragmentControlRegistry>,
    lifecycle: Arc<QueryLifecycleRegistry>,
    queries: NativeFragmentQueryRuntime,
    exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
    lookup_client: Arc<dyn FragmentLookupClient>,
    result_writer: Arc<dyn FragmentResultWriter>,
    execution_runtime: Arc<ExecutionRuntime>,
    commit_port: Arc<dyn FragmentCommitPort>,
    exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
    /// Runtime split delivery for admitted tasks. It is keyed by execution id
    /// and fragment instance, so a replaced attempt gets a fresh queue set and
    /// can never inherit a sequence space.
    split_queues: Arc<
        novarocks_execution::connector::SplitQueueRegistry<
            crate::fragment::ingress::ReceivedReadSplit,
        >,
    >,
    read_contexts: Arc<
        Mutex<
            BTreeMap<
                novarocks_execution::connector::TaskAttemptKey,
                Arc<crate::fragment::ingress::TypedReadAttemptContext>,
            >,
        >,
    >,
    lifecycle_observer: Option<LifecycleObserver>,
    #[cfg(test)]
    after_lifecycle_admission: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(test)]
    fail_worker_spawn_on_submission: Option<usize>,
    #[cfg(test)]
    submission_count: AtomicUsize,
}

impl std::fmt::Debug for NativeFragmentService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeFragmentService")
            .finish_non_exhaustive()
    }
}

impl NativeFragmentService {
    #[cfg(test)]
    fn new(
        exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
        lookup_client: Arc<dyn FragmentLookupClient>,
        result_writer: Arc<dyn FragmentResultWriter>,
        lifecycle: Arc<QueryLifecycleRegistry>,
    ) -> Self {
        Self::new_with_controls(
            exchange_transmitter,
            lookup_client,
            result_writer,
            Arc::new(FragmentControlRegistry::default()),
            lifecycle,
            test_execution_runtime(),
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "The frozen native boundary keeps independently validated inputs explicit."
    )]
    pub(crate) fn new_with_controls(
        exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
        lookup_client: Arc<dyn FragmentLookupClient>,
        result_writer: Arc<dyn FragmentResultWriter>,
        controls: Arc<FragmentControlRegistry>,
        lifecycle: Arc<QueryLifecycleRegistry>,
        execution_runtime: Arc<ExecutionRuntime>,
    ) -> Self {
        Self {
            controls,
            lifecycle,
            queries: NativeFragmentQueryRuntime::global(),
            exchange_transmitter,
            lookup_client,
            result_writer,
            execution_runtime,
            commit_port: Arc::new(BackendSinkCommitPort),
            exchange_receiver_port: Arc::new(UnavailableExchangeReceiverPort),
            split_queues: Arc::new(novarocks_execution::connector::SplitQueueRegistry::new()),
            read_contexts: Arc::new(Mutex::new(BTreeMap::new())),
            lifecycle_observer: None,
            #[cfg(test)]
            after_lifecycle_admission: None,
            #[cfg(test)]
            fail_worker_spawn_on_submission: None,
            #[cfg(test)]
            submission_count: AtomicUsize::new(0),
        }
    }

    pub(crate) fn with_exchange_receiver_port(
        mut self,
        exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
    ) -> Self {
        self.exchange_receiver_port = exchange_receiver_port;
        self
    }

    pub(crate) fn with_write_commit_evidence_limits(
        mut self,
        limits: WriteCommitEvidenceLimits,
    ) -> Self {
        self.commit_port = Arc::new(ConfiguredBackendSinkCommitPort::new(limits));
        self
    }

    #[cfg(test)]
    fn with_lifecycle_observer(
        observer: impl Fn(NativeFragmentLifecycleEvent) + Send + Sync + 'static,
    ) -> Self {
        let controls = Arc::new(FragmentControlRegistry::default());
        let lifecycle = test_lifecycle_registry(Arc::clone(&controls));
        let mut service = Self::new_with_controls(
            crate::fragment::grpc_exchange_transmitter(
                crate::rpc::runtime::test_backend_data_runtime(),
            ),
            crate::fragment::grpc_fragment_lookup_client(
                crate::rpc::runtime::test_backend_data_runtime(),
            ),
            crate::fragment::native_result_writer(),
            controls,
            lifecycle,
            test_execution_runtime(),
        );
        service.lifecycle_observer = Some(Arc::new(observer));
        service
    }

    #[cfg(test)]
    fn with_lifecycle_observer_and_worker_spawn_failure(
        observer: impl Fn(NativeFragmentLifecycleEvent) + Send + Sync + 'static,
        fail_worker_spawn_on_submission: usize,
    ) -> Self {
        let mut service = Self::with_lifecycle_observer(observer);
        service.fail_worker_spawn_on_submission = Some(fail_worker_spawn_on_submission);
        service
    }

    #[cfg(test)]
    fn with_lifecycle_observer_and_admission_pause(
        observer: impl Fn(NativeFragmentLifecycleEvent) + Send + Sync + 'static,
        after_lifecycle_admission: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        let mut service = Self::with_lifecycle_observer(observer);
        service.after_lifecycle_admission = Some(Arc::new(after_lifecycle_admission));
        service
    }

    fn observe(&self, event: NativeFragmentLifecycleEvent) {
        if let Some(observer) = self.lifecycle_observer.as_ref() {
            observer(event);
        }
    }

    /// Materializes a complete fragment bundle without starting its drivers.
    /// Every spawned worker waits on the query-owned gate; gate abort is a
    /// pure cleanup path and never calls `DormantFragmentHandle::start`.
    /// Assemble the runtime inputs a typed connector scan needs.
    ///
    /// The queue set is opened here rather than on first delivery so a task
    /// scan can start, and block, before any split has arrived.
    fn typed_scan_runtime(
        &self,
        execution_id: QueryExecutionId,
        instance_params: &novarocks_proto_models::novarocks::InstanceParams,
    ) -> crate::fragment::decode::plan::context::TypedScanRuntime {
        let fragment_instance_id = instance_params
            .fragment_instance_id
            .as_ref()
            .map(|id| novarocks_types::UniqueId::new(id.hi, id.lo))
            .unwrap_or_else(|| novarocks_types::UniqueId::new(0, 0));
        let queues = self.split_queues.open_attempt(
            novarocks_execution::connector::TaskAttemptKey::new(execution_id, fragment_instance_id),
            split_queue_config(),
        );
        let attempt_key =
            novarocks_execution::connector::TaskAttemptKey::new(execution_id, fragment_instance_id);
        let read_contexts = {
            let mut contexts = self
                .read_contexts
                .lock()
                .expect("typed read context map lock");
            contexts
                .entry(attempt_key)
                .or_insert_with(|| {
                    Arc::new(crate::fragment::ingress::TypedReadAttemptContext::new())
                })
                .clone()
        };
        // The session is role-local and carries no credential: object-store
        // access stays with the binding owner.
        let session = novarocks_spi::connector::read_stack::ConnectorSession::try_new(
            format!(
                "{}:{}:{}",
                execution_id.query_id().high(),
                execution_id.query_id().low(),
                execution_id.attempt_id().get()
            ),
            "novarocks",
            "UTC",
            "en_US",
            std::time::SystemTime::now(),
        )
        .expect("a query execution id is never an empty session query id");
        // Resolving is deliberately deferred until bind: decode has no
        // admission permit, while the lifecycle exposes a runtime-filter
        // session only to an admitted fragment.  `None` is the ordinary
        // no-filter result; a lifecycle error is returned to the scan and must
        // fail the reader rather than silently widening it to CompleteAll.
        use crate::fragment::decode::plan::context::RuntimeFilterSessionResolver;
        let lifecycle = Arc::clone(&self.lifecycle);
        let runtime_filter: RuntimeFilterSessionResolver = Arc::new(move || {
            lifecycle
                .runtime_filter_session_for_fragment(execution_id, fragment_instance_id, false)
                .map_err(|error| format!("resolve typed scan runtime-filter session: {error}"))
        });

        let lifecycle = Arc::clone(&self.lifecycle);
        use crate::fragment::decode::plan::context::CatalogReadExecutionResolver;
        let catalog_read_execution: CatalogReadExecutionResolver = Arc::new(move |handle| {
            lifecycle.catalog_read_execution_for_query(execution_id, handle)
        });
        let lifecycle = Arc::clone(&self.lifecycle);
        use crate::fragment::decode::plan::context::CatalogWriteExecutionResolver;
        let catalog_write_execution: CatalogWriteExecutionResolver = Arc::new(move |handle| {
            lifecycle.catalog_write_execution_for_query(execution_id, handle)
        });
        let storage_resolver = self.lifecycle.storage_resolver_for_query(execution_id);

        crate::fragment::decode::plan::context::TypedScanRuntime::new(
            execution_id,
            catalog_read_execution,
            catalog_write_execution,
            queues,
            session,
            runtime_filter,
            read_contexts,
            storage_resolver,
        )
    }

    /// Deliver one admitted task update into the per-plan-node split queues.
    ///
    /// The lifecycle owner has already decided this attempt may receive work;
    /// this only enqueues it. A queue set is opened lazily because a task scan
    /// may legitimately start before its first split arrives.
    pub(crate) fn deliver_split_assignments(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: novarocks_types::UniqueId,
        assignments: &[novarocks_proto_codec::connector_read::SplitAssignment],
    ) -> crate::query_lifecycle::task_update::TaskUpdateAck {
        use crate::query_lifecycle::task_update::{TaskUpdateAcceptedNode, TaskUpdateAck};
        use novarocks_execution::connector::TaskAttemptKey;

        let key = TaskAttemptKey::new(execution_id, fragment_instance_id);
        let attempt = self.split_queues.open_attempt(key, split_queue_config());
        let mut accepted = Vec::with_capacity(assignments.len());
        for assignment in assignments {
            let queue = attempt.queue(assignment.plan_node_id());
            let sequences = assignment
                .splits()
                .iter()
                .map(|split| {
                    novarocks_execution::connector::SplitSequenceEvidence::new(
                        split.sequence_id(),
                        split.plan_node_id(),
                    )
                })
                .collect::<Vec<_>>();
            // Closed-carrier validation precedes this point. Queue state then
            // classifies duplicate sequence numbers before provider decoding,
            // so an at-or-below-watermark retransmission never recovers a
            // provider payload.
            let preflight = match queue.preflight_sequences(assignment.plan_node_id(), &sequences) {
                Ok(preflight) => preflight,
                Err(error) => return split_queue_rejection(error),
            };
            let has_new_splits = assignment
                .splits()
                .iter()
                .any(|split| !preflight.is_duplicate(split.sequence_id()));
            let read_execution = if has_new_splits {
                let read_execution = self
                    .read_contexts
                    .lock()
                    .ok()
                    .and_then(|contexts| contexts.get(&key).cloned())
                    .and_then(|context| context.resolve(assignment.plan_node_id()));
                let Some(read_execution) = read_execution else {
                    return TaskUpdateAck::rejected(
                        crate::query_lifecycle::task_update::TaskUpdateRejectionReason::UnknownPlanNode,
                        format!(
                            "no typed connector read context exists for plan node {}",
                            assignment.plan_node_id()
                        ),
                    );
                };
                Some(read_execution)
            } else {
                None
            };
            let received = assignment
                .splits()
                .iter()
                .filter(|split| !preflight.is_duplicate(split.sequence_id()))
                .map(|split| {
                    read_execution
                        .as_ref()
                        .expect("new splits require a typed read execution")
                        .decoder()
                        .decode_scheduled_split(split)
                        .map(|decoded| {
                            let (evidence, split) = decoded.into_parts();
                            crate::fragment::ingress::ReceivedReadSplit::new(evidence, split)
                        })
                })
                .collect::<Result<Vec<_>, _>>();
            let received = match received {
                Ok(received) => received,
                Err(error) => {
                    return TaskUpdateAck::rejected(
                        crate::query_lifecycle::task_update::TaskUpdateRejectionReason::InvalidAssignment,
                        error.to_string(),
                    );
                }
            };
            match queue.offer_splits(
                assignment.plan_node_id(),
                received,
                assignment.no_more_splits(),
            ) {
                Ok(outcome) => {
                    let preflight_duplicate_count = preflight.duplicate_sequences().len();
                    queue.record_preflight_duplicate_splits(preflight_duplicate_count);
                    let duplicate_count =
                        preflight_duplicate_count.saturating_add(outcome.duplicate_sequences.len());
                    // Stable acceptance evidence for distributed acceptance
                    // runs: it proves a real remote assignment reached this
                    // task, which a single-process smoke cannot show.
                    if crate::config::debug_emit_connector_reader_marker() {
                        println!(
                            "NOVAROCKS_TASK_SPLIT_ASSIGNMENT_ACCEPTED execution_id={}:{}:{} finst={:x}:{:x} plan_node={} enqueued={} duplicate={} accepted_through={}",
                            execution_id.query_id().high(),
                            execution_id.query_id().low(),
                            execution_id.attempt_id().get(),
                            fragment_instance_id.high(),
                            fragment_instance_id.low(),
                            assignment.plan_node_id(),
                            outcome.enqueued.len(),
                            duplicate_count,
                            outcome.max_accepted_sequence.unwrap_or_default(),
                        );
                        if duplicate_count > 0 {
                            println!(
                                "NOVAROCKS_TASK_SPLIT_ASSIGNMENT_DUPLICATE execution_id={}:{}:{} finst={:x}:{:x} plan_node={} duplicate={} duplicate_splits_skipped={}",
                                execution_id.query_id().high(),
                                execution_id.query_id().low(),
                                execution_id.attempt_id().get(),
                                fragment_instance_id.high(),
                                fragment_instance_id.low(),
                                assignment.plan_node_id(),
                                duplicate_count,
                                queue.stats().duplicate_splits_skipped,
                            );
                        }
                        if outcome.no_more_splits {
                            println!(
                                "NOVAROCKS_TASK_SPLIT_NO_MORE execution_id={}:{}:{} finst={:x}:{:x} plan_node={}",
                                execution_id.query_id().high(),
                                execution_id.query_id().low(),
                                execution_id.attempt_id().get(),
                                fragment_instance_id.high(),
                                fragment_instance_id.low(),
                                assignment.plan_node_id(),
                            );
                        }
                        let _ = std::io::Write::flush(&mut std::io::stdout());
                    }
                    accepted.push(TaskUpdateAcceptedNode {
                        plan_node_id: assignment.plan_node_id(),
                        accepted_through_sequence: outcome
                            .max_accepted_sequence
                            .unwrap_or_default(),
                        no_more_splits: outcome.no_more_splits,
                        queued_splits: attempt
                            .existing_queue(assignment.plan_node_id())
                            .map(|queue| queue.stats().queued_splits as u64)
                            .unwrap_or_default(),
                    });
                }
                Err(error) => return split_queue_rejection(error),
            }
        }
        TaskUpdateAck::Accepted(accepted)
    }

    pub(crate) fn stage_fragments(
        &self,
        execution_id: QueryExecutionId,
        fragments: &[StageFragment],
        gate: Arc<StartGate>,
    ) -> Result<(), NativeFragmentIngressError> {
        for fragment in fragments {
            let fragment_instance_id = fragment
                .instance_params()
                .fragment_instance_id
                .as_ref()
                .map(|id| novarocks_types::UniqueId::new(id.hi, id.lo))
                .unwrap_or_else(|| novarocks_types::UniqueId::new(0, 0));
            // The context is provisional through decode and admission.  Its
            // Drop path removes it unless this fragment reaches the existing
            // registration/commit boundary below, so TaskUpdate can never
            // resolve a half-decoded plan node.
            let mut provisional_read_context = ProvisionalTypedReadContext::new(
                Arc::clone(&self.read_contexts),
                novarocks_execution::connector::TaskAttemptKey::new(
                    execution_id,
                    fragment_instance_id,
                ),
            );
            let typed_runtime = self.typed_scan_runtime(execution_id, fragment.instance_params());
            let request = NativeFragmentRequest::try_decode_with_runtime(
                execution_id,
                fragment.plan().clone(),
                fragment.instance_params().clone(),
                self.queries
                    .connector_cancellation_for_execution(execution_id),
                std::time::Duration::from_millis(self.execution_runtime.config().exchange_wait_ms),
                Some(typed_runtime),
            );
            let request = match request {
                Ok(request) => request,
                Err(error) => return Err(error),
            };
            if let Err(error) = self.stage_one(request, Arc::clone(&gate)) {
                return Err(error);
            }
            provisional_read_context.publish();
        }
        Ok(())
    }

    fn stage_one(
        &self,
        request: NativeFragmentRequest,
        gate: Arc<StartGate>,
    ) -> Result<(), NativeFragmentIngressError> {
        let execution_id = request.execution_id();
        let fragment_instance_id = request.fragment_instance_id();
        let lifecycle_permit = self
            .lifecycle
            .admit_fragment(execution_id, fragment_instance_id)
            .map_err(NativeFragmentIngressError::new)?;
        let runtime_filter = self
            .lifecycle
            .runtime_filter_session_for_fragment(
                execution_id,
                fragment_instance_id,
                request.has_runtime_filter_bindings(),
            )
            .map_err(NativeFragmentIngressError::new)?;
        let backend_num = request.backend_num();
        let enable_profile = request.enable_profile();
        let (delivery_expire, query_expire) = request.query_expire_durations();
        let profiler =
            enable_profile.then(|| profiler_for_native_fragment(request.root_plan_node_id()));
        let admission = self
            .queries
            .prepare_admission_execution(
                execution_id,
                fragment_instance_id,
                delivery_expire,
                query_expire,
                runtime_filter,
            )
            .map_err(NativeFragmentIngressError::new)?;
        let _query_mem_tracker = admission.query_mem_tracker();
        let _fragment_mem_tracker = admission.fragment_mem_tracker();
        // Staged execution can safely rendezvous after Start even when this
        // is the root result fragment: the runner-owned trigger is consumed
        // only after the gate releases and never exists in production.
        let failure_injection_eligible = true;
        let event_sink = crate::fragment::lifecycle_fragment_event_sink(
            Arc::clone(&self.lifecycle),
            execution_id,
            fragment_instance_id,
            profiler.clone(),
        );
        let dormant = prepare_fragment(
            request.into_submission(),
            admission
                .into_prepare_context(
                    profiler.clone(),
                    Arc::clone(&self.exchange_transmitter),
                    Arc::clone(&self.lookup_client),
                    Arc::clone(&self.result_writer),
                    event_sink,
                )
                .with_fragment_commit_port(Arc::clone(&self.commit_port))
                .with_exchange_receiver_port(Arc::clone(&self.exchange_receiver_port))
                .with_execution_runtime(Arc::clone(&self.execution_runtime)),
        )
        .map_err(NativeFragmentIngressError::new)?;
        self.observe(NativeFragmentLifecycleEvent::Prepared);
        let reservation = self
            .controls
            .reserve(fragment_instance_id)
            .map_err(NativeFragmentIngressError::new)?;
        let registration = self
            .queries
            .register_fragment_execution(
                execution_id,
                fragment_instance_id,
                delivery_expire,
                query_expire,
            )
            .map_err(NativeFragmentIngressError::new)?;
        if let Some(context) = self.read_contexts.lock().ok().and_then(|contexts| {
            contexts
                .get(&novarocks_execution::connector::TaskAttemptKey::new(
                    execution_id,
                    fragment_instance_id,
                ))
                .cloned()
        }) {
            context.publish();
        }
        let pending_control =
            Arc::new(PendingFragmentControl::new(self.lifecycle_observer.clone()));
        let control_handle: Arc<dyn FragmentControlHandle> = pending_control.clone();
        let token = reservation.publish(control_handle);
        let queries = self.queries.clone();
        let split_queues = Arc::clone(&self.split_queues);
        let read_contexts = Arc::clone(&self.read_contexts);
        let lifecycle = Arc::clone(&self.lifecycle);
        let observer = self.lifecycle_observer.clone();
        std::thread::Builder::new()
            .name(format!(
                "native-fragment-{:x}-{:x}",
                fragment_instance_id.high(), fragment_instance_id.low()
            ))
            .spawn(move || {
                if gate.wait() != crate::query_lifecycle::stage::StartGateState::Released {
                    close_task_split_queues(&split_queues, execution_id, fragment_instance_id);
                    queries.unregister_fragment_execution(execution_id, fragment_instance_id);
                    queries.finish_fragment(execution_id);
                    token.complete();
                    return;
                }
                // The pre-start lease keeps the query route rollback-capable
                // while this worker is dormant. Only the Start gate winner may
                // make it live.
                registration.into_running();
                let staged_failure = claim_configured_fragment_failure_trigger(
                    failure_injection_eligible,
                );
                let (running, staged_failure_token) = match staged_failure {
                    Ok(Some(release)) => match release.wait() {
                        Ok(token) => (
                            dormant.start_failed(FRAGMENT_EXECUTOR_FAILURE_MESSAGE),
                            Some(token),
                        ),
                        Err(error) => (dormant.start_failed(error), None),
                    },
                    Ok(None) => (dormant.start(), None),
                    Err(error) => (dormant.start_failed(error), None),
                };
                pending_control.attach(running.clone());
                if let Some(observer) = observer.as_ref() {
                    observer(NativeFragmentLifecycleEvent::Started);
                }
                if let Some(token) = staged_failure_token {
                    eprintln!(
                        "NOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED token={} query_hi={} query_lo={} finst_hi={} finst_lo={}",
                        token,
                        execution_id.query_id().high(),
                        execution_id.query_id().low(),
                        fragment_instance_id.high(),
                        fragment_instance_id.low()
                    );
                }
                consume_terminal_fact(
                    running,
                    token,
                    queries,
                    split_queues,
                    read_contexts,
                    lifecycle,
                    execution_id,
                    backend_num,
                );
            })
            .map_err(|error| {
                NativeFragmentIngressError::new(format!(
                    "spawn staged native fragment worker failed: {error}"
                ))
            })?;
        self.observe(NativeFragmentLifecycleEvent::Registered);
        lifecycle_permit
            .commit()
            .map_err(NativeFragmentIngressError::new)?;
        self.observe(NativeFragmentLifecycleEvent::Accepted);
        Ok(())
    }
}

#[cfg(test)]
impl NativeFragmentService {
    fn submit(&self, request: NativeFragmentRequest) -> Result<(), NativeFragmentIngressError> {
        let query_id = request.query_id();
        let execution_id = request.execution_id();
        let fragment_instance_id = request.fragment_instance_id();
        let lifecycle_permit = self
            .lifecycle
            .admit_fragment(execution_id, fragment_instance_id)
            .map_err(NativeFragmentIngressError::new)?;
        let runtime_filter = self
            .lifecycle
            .runtime_filter_session_for_fragment(
                execution_id,
                fragment_instance_id,
                request.has_runtime_filter_bindings(),
            )
            .map_err(NativeFragmentIngressError::new)?;
        #[cfg(test)]
        if let Some(after_lifecycle_admission) = self.after_lifecycle_admission.as_ref() {
            after_lifecycle_admission();
        }
        let backend_num = request.backend_num();
        let enable_profile = request.enable_profile();
        let (delivery_expire, query_expire) = request.query_expire_durations();
        let profiler =
            enable_profile.then(|| profiler_for_native_fragment(request.root_plan_node_id()));
        let admission = self
            .queries
            .prepare_admission_execution(
                execution_id,
                fragment_instance_id,
                delivery_expire,
                query_expire,
                runtime_filter,
            )
            .map_err(NativeFragmentIngressError::new)?;
        let _query_mem_tracker = admission.query_mem_tracker();
        let _fragment_mem_tracker = admission.fragment_mem_tracker();
        let failure_injection_eligible = !request.uses_result_sink();
        let event_sink = crate::fragment::lifecycle_fragment_event_sink(
            Arc::clone(&self.lifecycle),
            execution_id,
            fragment_instance_id,
            profiler.clone(),
        );
        let dormant = prepare_fragment(
            request.into_submission(),
            admission
                .into_prepare_context(
                    profiler.clone(),
                    Arc::clone(&self.exchange_transmitter),
                    Arc::clone(&self.lookup_client),
                    Arc::clone(&self.result_writer),
                    event_sink,
                )
                .with_fragment_commit_port(Arc::clone(&self.commit_port))
                .with_exchange_receiver_port(Arc::clone(&self.exchange_receiver_port))
                .with_execution_runtime(Arc::clone(&self.execution_runtime)),
        )
        .map_err(NativeFragmentIngressError::new)?;
        self.observe(NativeFragmentLifecycleEvent::Prepared);

        let reservation = self
            .controls
            .reserve(fragment_instance_id)
            .map_err(NativeFragmentIngressError::new)?;
        let registration = self
            .queries
            .register_fragment_execution(
                execution_id,
                fragment_instance_id,
                delivery_expire,
                query_expire,
            )
            .map_err(NativeFragmentIngressError::new)?;

        let pending_control =
            Arc::new(PendingFragmentControl::new(self.lifecycle_observer.clone()));
        let control_handle: Arc<dyn FragmentControlHandle> = pending_control.clone();
        let token = reservation.publish(control_handle);
        let (start_tx, start_rx) = mpsc::sync_channel::<()>(0);
        let queries = self.queries.clone();
        let split_queues = Arc::clone(&self.split_queues);
        let read_contexts = Arc::clone(&self.read_contexts);
        let lifecycle = Arc::clone(&self.lifecycle);
        let observer = self.lifecycle_observer.clone();
        #[cfg(test)]
        if self.fail_worker_spawn_on_submission.is_some_and(|target| {
            self.submission_count.fetch_add(1, Ordering::SeqCst) + 1 == target
        }) {
            return Err(NativeFragmentIngressError::new(
                "injected native fragment adapter worker spawn failure",
            ));
        }
        std::thread::Builder::new()
            .name(format!(
                "native-fragment-{:x}-{:x}",
                fragment_instance_id.high(), fragment_instance_id.low()
            ))
            .spawn(move || {
                if start_rx.recv().is_err() {
                    let error = "native fragment start signal was dropped".to_string();
                    error!(target: "novarocks_execution", finst_id = %fragment_instance_id, %error, "native fragment start signal was dropped");
                    close_task_split_queues(&split_queues, execution_id, fragment_instance_id);
                    queries.unregister_fragment_execution(execution_id, fragment_instance_id);
                    queries.finish_fragment(execution_id);
                    token.complete();
                    return;
                }
                let (running, failure_release) =
                    start_with_configured_fragment_failure_trigger(
                        dormant,
                        failure_injection_eligible,
                    );
                pending_control.attach(running.clone());
                if let Some(observer) = observer.as_ref() {
                    observer(NativeFragmentLifecycleEvent::Started);
                }
                if let Some(release) = failure_release {
                    match release.wait() {
                        Ok(evidence_token) => {
                            eprintln!(
                                "NOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_INJECTED token={} query_hi={} query_lo={} finst_hi={} finst_lo={}",
                                evidence_token,
                                query_id.high(),
                                query_id.low(),
                                fragment_instance_id.high(),
                                fragment_instance_id.low()
                            );
                        }
                        Err(error) => {
                            eprintln!(
                                "NOVAROCKS_FRAGMENT_EXECUTOR_FAILURE_RELEASE_FAILED query_hi={} query_lo={} finst_hi={} finst_lo={} error={}",
                                query_id.high(),
                                query_id.low(),
                                fragment_instance_id.high(),
                                fragment_instance_id.low(),
                                error
                            );
                        }
                    }
                }
                consume_terminal_fact(
                    running,
                    token,
                    queries,
                    split_queues,
                    read_contexts,
                    lifecycle,
                    execution_id,
                    backend_num,
                );
            })
            .map_err(|error| {
                NativeFragmentIngressError::new(format!(
                    "spawn native fragment adapter worker failed: {error}"
                ))
            })?;
        registration.into_running();

        self.observe(NativeFragmentLifecycleEvent::Registered);
        lifecycle_permit
            .commit()
            .map_err(NativeFragmentIngressError::new)?;
        self.observe(NativeFragmentLifecycleEvent::Accepted);
        start_tx.send(()).map_err(|_| {
            NativeFragmentIngressError::new(
                "native fragment adapter worker terminated before start",
            )
        })?;
        Ok(())
    }
}

impl NativeFragmentIngress for NativeFragmentService {
    fn cancel(
        &self,
        request: NativeFragmentCancelRequest,
    ) -> Result<(), NativeFragmentIngressError> {
        // Design: ADR-0010 (docs/adr/ADR-0010-explicit-query-cancellation-surface.md)
        let mut fragment_instance_ids = request
            .fragment_instance_ids()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        fragment_instance_ids.extend(
            self.queries
                .cancel_query(request.query_id(), request.reason().to_string()),
        );
        let fragment_instance_ids = fragment_instance_ids.into_iter().collect::<Vec<_>>();
        self.controls
            .cancel_many(&fragment_instance_ids, request.reason());
        Ok(())
    }
}

#[cfg(test)]
struct RunningFragmentControl {
    handle: RunningFragmentHandle,
}

struct PendingFragmentControl {
    state: Mutex<PendingFragmentControlState>,
    observer: Option<LifecycleObserver>,
}

#[derive(Default)]
struct PendingFragmentControlState {
    running: Option<RunningFragmentHandle>,
    cancellation: Option<String>,
}

impl PendingFragmentControl {
    fn new(observer: Option<LifecycleObserver>) -> Self {
        Self {
            state: Mutex::new(PendingFragmentControlState::default()),
            observer,
        }
    }

    fn attach(&self, running: RunningFragmentHandle) {
        let cancellation = {
            let mut state = self.state.lock().expect("pending fragment control");
            state.running = Some(running.clone());
            state.cancellation.clone()
        };
        if let Some(reason) = cancellation {
            running.cancel(FragmentCancelReason::new(reason));
        }
    }
}

impl FragmentControlHandle for PendingFragmentControl {
    fn cancel(&self, reason: &str) {
        let (running, first_cancellation) = {
            let mut state = self.state.lock().expect("pending fragment control");
            let first_cancellation = state.cancellation.is_none();
            state.cancellation.get_or_insert_with(|| reason.to_string());
            (state.running.clone(), first_cancellation)
        };
        if first_cancellation && let Some(observer) = self.observer.as_ref() {
            observer(NativeFragmentLifecycleEvent::Cancelled);
        }
        if let Some(running) = running {
            running.cancel(FragmentCancelReason::new(reason));
        }
    }
}

#[cfg(test)]
impl FragmentControlHandle for RunningFragmentControl {
    fn cancel(&self, reason: &str) {
        self.handle.cancel(FragmentCancelReason::new(reason));
    }
}

/// Close a task's split queues so every parked scan wakes exactly once.
/// Release the split queues of one finished or cancelled task.
///
/// Closing wakes every parked scan exactly once so a terminated task never
/// leaves a driver waiting for splits that will not arrive, and makes every
/// later queue for that attempt born closed.
fn close_task_split_queues(
    split_queues: &novarocks_execution::connector::SplitQueueRegistry<
        crate::fragment::ingress::ReceivedReadSplit,
    >,
    execution_id: QueryExecutionId,
    fragment_instance_id: novarocks_types::UniqueId,
) {
    split_queues.close_attempt(novarocks_execution::connector::TaskAttemptKey::new(
        execution_id,
        fragment_instance_id,
    ));
}

fn remove_task_read_context(
    read_contexts: &Mutex<
        BTreeMap<
            novarocks_execution::connector::TaskAttemptKey,
            Arc<crate::fragment::ingress::TypedReadAttemptContext>,
        >,
    >,
    execution_id: QueryExecutionId,
    fragment_instance_id: novarocks_types::UniqueId,
) {
    if let Ok(mut contexts) = read_contexts.lock() {
        contexts.remove(&novarocks_execution::connector::TaskAttemptKey::new(
            execution_id,
            fragment_instance_id,
        ));
    }
}

/// Keeps an attempt/node read context invisible until the fragment has fully
/// decoded and passed the established admission/registration flow. The map is
/// role-local state; this guard owns neither the connector binding nor the
/// lifecycle permit, and only makes their already-admitted result visible to
/// TaskUpdate.
struct ProvisionalTypedReadContext {
    contexts: Arc<
        Mutex<
            BTreeMap<
                novarocks_execution::connector::TaskAttemptKey,
                Arc<crate::fragment::ingress::TypedReadAttemptContext>,
            >,
        >,
    >,
    key: novarocks_execution::connector::TaskAttemptKey,
    context: Arc<crate::fragment::ingress::TypedReadAttemptContext>,
    published: bool,
}

impl ProvisionalTypedReadContext {
    fn new(
        contexts: Arc<
            Mutex<
                BTreeMap<
                    novarocks_execution::connector::TaskAttemptKey,
                    Arc<crate::fragment::ingress::TypedReadAttemptContext>,
                >,
            >,
        >,
        key: novarocks_execution::connector::TaskAttemptKey,
    ) -> Self {
        let context = contexts
            .lock()
            .expect("typed read context map lock")
            .entry(key)
            .or_insert_with(|| Arc::new(crate::fragment::ingress::TypedReadAttemptContext::new()))
            .clone();
        Self {
            contexts,
            key,
            context,
            published: false,
        }
    }

    fn publish(&mut self) {
        self.context.publish();
        self.published = true;
    }
}

impl QueryLifecycleTerminalCleanup for NativeFragmentService {
    fn cleanup_terminal_execution(&self, execution_id: QueryExecutionId) {
        self.split_queues.close_execution(execution_id);
        if let Ok(mut contexts) = self.read_contexts.lock() {
            contexts.retain(|key, _| key.execution_id() != execution_id);
        }
    }
}

impl Drop for ProvisionalTypedReadContext {
    fn drop(&mut self) {
        if self.published {
            return;
        }
        if let Ok(mut contexts) = self.contexts.lock() {
            if contexts
                .get(&self.key)
                .is_some_and(|current| Arc::ptr_eq(current, &self.context))
            {
                contexts.remove(&self.key);
            }
        }
    }
}

fn consume_terminal_fact(
    running: RunningFragmentHandle,
    token: super::control::FragmentControlToken,
    queries: NativeFragmentQueryRuntime,
    split_queues: Arc<
        novarocks_execution::connector::SplitQueueRegistry<
            crate::fragment::ingress::ReceivedReadSplit,
        >,
    >,
    read_contexts: Arc<
        Mutex<
            BTreeMap<
                novarocks_execution::connector::TaskAttemptKey,
                Arc<crate::fragment::ingress::TypedReadAttemptContext>,
            >,
        >,
    >,
    lifecycle: Arc<QueryLifecycleRegistry>,
    execution_id: QueryExecutionId,
    backend_num: i32,
) {
    let fact = running.join();
    let fragment_instance_id = fact.fragment_instance_id();
    if let FragmentOutcome::Failed(execution_error) = fact.outcome() {
        error!(target: "novarocks_execution", finst_id = %fragment_instance_id, error = %execution_error, "native fragment execution failed");
    }
    let sink = crate::runtime::sink_commit::report_snapshot(fragment_instance_id);
    let succeeded = matches!(fact.outcome(), FragmentOutcome::Succeeded);
    // QLC terminal facts are transferred before local runtime cleanup.
    lifecycle.record_fragment_terminal_fact(execution_id, fact, backend_num, sink);
    // A successful terminal split assignment carries no_more_splits and must
    // retain its watermark until the lifecycle tombstone. The sender may have
    // lost the unary acknowledgement and retry after this worker has exited.
    // Failed and cancelled workers still close immediately to wake parked
    // scans; their lifecycle cannot accept more work.
    if !succeeded {
        close_task_split_queues(&split_queues, execution_id, fragment_instance_id);
    }
    remove_task_read_context(&read_contexts, execution_id, fragment_instance_id);
    queries.unregister_fragment_execution(execution_id, fragment_instance_id);
    queries.finish_fragment(execution_id);
    // Publish the terminal report before this fact can fail-close the local
    // lifecycle. Otherwise a sibling cancelled by the first terminal fact may
    // win the report slot and hide the fragment that actually failed.
    token.complete();
}

fn profiler_for_native_fragment(root_plan_node_id: i32) -> Profiler {
    novarocks_execution::runtime::profile::fragment_root_profiler(root_plan_node_id)
}

#[cfg(test)]
fn test_lifecycle_registry(controls: Arc<FragmentControlRegistry>) -> Arc<QueryLifecycleRegistry> {
    QueryLifecycleRegistry::new_with_process_id(
        novarocks_types::BackendProcessId::new_v7(),
        Arc::new(crate::query_lifecycle::NativeQueryLifecycleLocalRuntime::new(controls)),
        crate::query_lifecycle::QueryLifecycleRegistryConfig::new(
            4_096,
            16_384,
            std::time::Duration::from_millis(120_000),
            std::time::Duration::from_millis(5_000),
            std::time::Duration::from_millis(30_000),
            256,
            32,
            48 * 1024 * 1024,
            256 * 1024 * 1024,
            512,
            48 * 1024 * 1024,
            std::time::Duration::from_millis(30_000),
            std::time::Duration::from_millis(5_000),
            std::time::Duration::from_millis(5_000),
            5,
            std::time::Duration::from_millis(100),
            std::time::Duration::from_millis(1_000),
            std::time::Duration::from_millis(120_000),
            4_096,
            256 * 1024 * 1024,
        ),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::{Duration, Instant};

    use novarocks_execution::runtime::fragment::{
        DormantFragmentHandle, FragmentOutcome, prepare_fragment,
    };
    use novarocks_proto_codec::lifecycle::{AttemptId as ProtocolAttemptId, QueryExecutionId};
    use novarocks_proto_codec::lifecycle::{
        ParticipantBackendIdentity, ParticipantManifest, QueryControlAttach, QueryControlEndpoint,
        QueryInitOutcome, QueryInitRequest, QueryOptions, StageFragment,
    };
    use novarocks_proto_models as proto;
    use novarocks_types::QueryId as ExecutionQueryId;
    use novarocks_types::QueryId;
    use novarocks_types::UniqueId;

    use crate::fragment::control::{FragmentControlHandle, FragmentControlRegistry};
    use crate::fragment::failure_injection::{
        FRAGMENT_EXECUTOR_FAILURE_MESSAGE, start_with_fragment_failure_trigger,
    };
    use crate::fragment::ingress::{NativeFragmentCancelRequest, NativeFragmentIngress};
    use crate::query_lifecycle::{QueryControlAttachment, stage::StartGate};

    use super::{
        NativeFragmentLifecycleEvent, NativeFragmentRequest, NativeFragmentService,
        RunningFragmentControl, close_task_split_queues, consume_terminal_fact,
        test_lifecycle_registry,
    };

    static SERVICE_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[derive(Default)]
    struct RecordingControl {
        reasons: Mutex<Vec<String>>,
    }

    impl FragmentControlHandle for RecordingControl {
        fn cancel(&self, reason: &str) {
            self.reasons
                .lock()
                .expect("recording control reasons")
                .push(reason.to_string());
        }
    }

    #[test]
    fn cancel_latches_query_context_before_cancelling_all_local_controls() {
        let _service_guard = SERVICE_TEST_LOCK.lock().expect("service test lock");
        let service = NativeFragmentService::with_lifecycle_observer(|_| {});
        let query_id = QueryId::new(84_000, 84_001);
        let requested = UniqueId::new(84_002, 84_003);
        let local_sibling = UniqueId::new(84_004, 84_005);
        let mut controls = Vec::new();
        let mut control_tokens = Vec::new();
        for finst in [requested, local_sibling] {
            let registration = service
                .queries
                .register_fragment(
                    query_id,
                    finst,
                    Duration::from_secs(1),
                    Duration::from_secs(5),
                )
                .expect("register local fragment");
            registration.into_running();
            let control = Arc::new(RecordingControl::default());
            let control_handle: Arc<dyn FragmentControlHandle> = control.clone();
            let token = service
                .controls
                .reserve(finst)
                .expect("reserve local control")
                .publish(control_handle);
            controls.push(control);
            control_tokens.push(token);
        }

        service
            .cancel(NativeFragmentCancelRequest::new(
                query_id,
                vec![requested],
                "explicit query cancellation",
            ))
            .expect("cancel is idempotent");
        assert!(controls.iter().all(|control| {
            control
                .reasons
                .lock()
                .expect("recording control reasons")
                .iter()
                .any(|reason| reason == "explicit query cancellation")
        }));
        assert!(
            !service
                .queries
                .cancel_query(query_id, "repeat probe".to_string())
                .is_empty(),
            "canonical query cancellation retains the local fragment set until terminal cleanup"
        );
        service
            .cancel(NativeFragmentCancelRequest::new(
                query_id,
                vec![requested],
                "explicit query cancellation",
            ))
            .expect("repeat cancel is idempotent");
        drop(control_tokens);
    }

    fn values_result_request(query_base: i64, fragment_base: i64) -> NativeFragmentRequest {
        let fragment_id = 7;
        NativeFragmentRequest::try_decode(
            QueryExecutionId::new(
                ExecutionQueryId::new(query_base, query_base + 1),
                ProtocolAttemptId::new(1).expect("nonzero attempt"),
            )
            .expect("valid execution id"),
            proto::plan::PlanFragment {
                fragment_id,
                root: Some(proto::plan::DistributedNode {
                    node_id: 41,
                    fragment_id,
                    limit: -1,
                    payload: Some(proto::plan::distributed_node::Payload::Physical(
                        proto::plan::PlanNode {
                            output_columns: Vec::new(),
                            kind: Some(proto::plan::plan_node::Kind::Values(
                                proto::plan::ValuesNode {
                                    rows: Vec::new(),
                                    columns: Vec::new(),
                                },
                            )),
                        },
                    )),
                    ..Default::default()
                }),
                sink: Some(proto::plan::DataSink {
                    kind: Some(proto::plan::data_sink::Kind::Result(true)),
                }),
                output_columns: Vec::new(),
                runtime_filter_bindings: Some(proto::plan::RuntimeFilterBindingTable {
                    fragment_id,
                    bindings: Vec::new(),
                }),
                ..Default::default()
            },
            proto::novarocks::InstanceParams {
                query_id: Some(proto::common::UniqueId {
                    hi: query_base,
                    lo: query_base + 1,
                }),
                fragment_instance_id: Some(proto::common::UniqueId {
                    hi: fragment_base,
                    lo: fragment_base + 1,
                }),
                backend_num: 3,
                query_options: Some(proto::novarocks::QueryOptions {
                    batch_size: 1024,
                    pipeline_dop: 1,
                    ..Default::default()
                }),
                ..Default::default()
            },
            std::time::Duration::from_millis(120_000),
        )
        .expect("valid native fragment request")
    }

    fn make_control_ready(
        service: &NativeFragmentService,
        request: &NativeFragmentRequest,
        expected_fragments: impl IntoIterator<Item = UniqueId>,
    ) -> QueryControlAttachment {
        let execution_id = request.execution_id();
        let manifest = ParticipantManifest::new(
            execution_id,
            ParticipantBackendIdentity::new(
                service.lifecycle.local_process_id(),
                QueryControlEndpoint::new("127.0.0.1", 19030).expect("control endpoint"),
            )
            .expect("backend identity"),
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            expected_fragments
                .into_iter()
                .map(|id| proto::common::UniqueId {
                    hi: id.high(),
                    lo: id.low(),
                }),
            QueryOptions::parse(proto::novarocks::QueryOptions::default())
                .expect("valid default query options"),
            u64::MAX,
            [],
            None,
            Duration::from_secs(30),
            QueryControlEndpoint::new("127.0.0.1", 19031).expect("report endpoint"),
        )
        .expect("fragment participant manifest");
        let init = QueryInitRequest::from_manifest(manifest);
        assert_eq!(
            service
                .lifecycle
                .init_query(init.clone())
                .outcome()
                .expect("valid init acknowledgement"),
            QueryInitOutcome::QueryInitApplied
        );
        let mut attachment = service
            .lifecycle
            .attach_control(
                QueryControlAttach::new(
                    novarocks_proto_codec::lifecycle::ParticipantAttemptRef::new(
                        execution_id,
                        init.manifest()
                            .expect("validated init manifest")
                            .backend()
                            .expect("validated init backend")
                            .process_id()
                            .expect("validated init backend process"),
                    )
                    .expect("valid participant attempt ref"),
                )
                .expect("control attachment"),
            )
            .expect("control attaches");
        assert!(matches!(
            attachment
                .events
                .try_recv()
                .expect("ControlReady")
                .as_proto()
                .event,
            Some(novarocks_proto_models::novarocks::query_control_response::Event::ControlReady(_))
        ));
        attachment
    }

    fn prepare_request_for_test(
        service: &NativeFragmentService,
        request: NativeFragmentRequest,
    ) -> DormantFragmentHandle {
        let execution_id = request.execution_id();
        let fragment_instance_id = request.fragment_instance_id();
        let (delivery_expire, query_expire) = request.query_expire_durations();
        let admission = service
            .queries
            .prepare_admission_execution(
                execution_id,
                fragment_instance_id,
                delivery_expire,
                query_expire,
                None,
            )
            .expect("native fragment admission");
        prepare_fragment(
            request.into_submission(),
            admission
                .into_prepare_context(
                    None,
                    Arc::clone(&service.exchange_transmitter),
                    Arc::clone(&service.lookup_client),
                    Arc::clone(&service.result_writer),
                    crate::fragment::lifecycle_fragment_event_sink(
                        Arc::clone(&service.lifecycle),
                        execution_id,
                        fragment_instance_id,
                        None,
                    ),
                )
                .with_fragment_commit_port(Arc::clone(&service.commit_port))
                .with_exchange_receiver_port(Arc::clone(&service.exchange_receiver_port))
                .with_execution_runtime(Arc::clone(&service.execution_runtime)),
        )
        .expect("native fragment prepares")
    }

    #[test]
    fn submit_acceptance_point_follows_prepare_and_registration_before_start() {
        let _service_guard = SERVICE_TEST_LOCK.lock().expect("service test lock");
        let events = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&events);
        let service = NativeFragmentService::with_lifecycle_observer(move |event| {
            captured.lock().expect("lifecycle events").push(event);
        });

        let request = values_result_request(81_000, 81_002);
        make_control_ready(&service, &request, [request.fragment_instance_id()]);
        service.submit(request).expect("native fragment submit");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while events.lock().expect("lifecycle events").len() < 4
            && std::time::Instant::now() < deadline
        {
            std::thread::yield_now();
        }
        assert_eq!(
            *events.lock().expect("lifecycle events"),
            vec![
                NativeFragmentLifecycleEvent::Prepared,
                NativeFragmentLifecycleEvent::Registered,
                NativeFragmentLifecycleEvent::Accepted,
                NativeFragmentLifecycleEvent::Started,
            ]
        );
    }

    #[test]
    fn malformed_staged_fragment_does_not_consume_lifecycle_admission() {
        let _service_guard = SERVICE_TEST_LOCK.lock().expect("service test lock");
        let events = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&events);
        let service = NativeFragmentService::with_lifecycle_observer(move |event| {
            captured.lock().expect("lifecycle events").push(event);
        });
        let request = values_result_request(81_400, 81_402);
        let _control = make_control_ready(&service, &request, [request.fragment_instance_id()]);
        let malformed = StageFragment::new(
            proto::plan::PlanFragment::default(),
            proto::novarocks::InstanceParams {
                query_id: Some(proto::common::UniqueId {
                    hi: 81_400,
                    lo: 81_401,
                }),
                fragment_instance_id: Some(proto::common::UniqueId {
                    hi: 81_402,
                    lo: 81_403,
                }),
                backend_num: 3,
                query_options: Some(proto::novarocks::QueryOptions {
                    batch_size: 1024,
                    pipeline_dop: 1,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .expect("stage fragment retains a valid identity");

        let error = service
            .stage_fragments(
                request.execution_id(),
                &[malformed],
                Arc::new(StartGate::new()),
            )
            .expect_err("malformed wire fragment must fail during ingress decode");
        assert!(error.to_string().contains("plan_fragment.root"));
        assert!(events.lock().expect("lifecycle events").is_empty());

        service
            .submit(request)
            .expect("malformed Stage must not consume the exact lifecycle admission");
    }

    #[test]
    fn fragment_requires_query_control_ready() {
        let _service_guard = SERVICE_TEST_LOCK.lock().expect("service test lock");
        let service = NativeFragmentService::with_lifecycle_observer(|_| {});

        let error = service
            .submit(values_result_request(81_100, 81_102))
            .expect_err("native fragment without an initialized control-ready attempt must fail");

        assert!(
            error.to_string().contains("query is not active"),
            "unexpected admission error: {error}"
        );
    }

    #[test]
    fn query_abort_submit_race() {
        let _service_guard = SERVICE_TEST_LOCK.lock().expect("service test lock");
        let events = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&events);
        let (permit_tx, permit_rx) = mpsc::sync_channel(0);
        let (resume_tx, resume_rx) = mpsc::sync_channel(0);
        let resume_rx = Mutex::new(resume_rx);
        let service = Arc::new(
            NativeFragmentService::with_lifecycle_observer_and_admission_pause(
                move |event| captured.lock().expect("lifecycle events").push(event),
                move || {
                    permit_tx.send(()).expect("publish lifecycle permit");
                    resume_rx
                        .lock()
                        .expect("admission resume")
                        .recv()
                        .expect("resume fragment registration");
                },
            ),
        );
        let request = values_result_request(81_200, 81_202);
        let query_id = request.query_id();
        let fragment_instance_id = request.fragment_instance_id();
        let attachment = make_control_ready(&service, &request, [fragment_instance_id]);
        let submit_service = Arc::clone(&service);
        let submit = std::thread::spawn(move || submit_service.submit(request));

        permit_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("submit pauses after lifecycle permit issuance");
        attachment
            .control
            .abort("abort after permit issuance".to_string())
            .expect("query abort is accepted");
        resume_tx
            .send(())
            .expect("resume fragment registration and control publication");

        let error = submit
            .join()
            .expect("submit thread")
            .expect_err("late lifecycle admission must reject fragment start");
        assert!(
            error
                .to_string()
                .contains("terminated before fragment admission commit")
        );
        let deadline = Instant::now() + Duration::from_secs(2);
        while !events
            .lock()
            .expect("lifecycle events")
            .contains(&NativeFragmentLifecycleEvent::Cancelled)
            && Instant::now() < deadline
        {
            std::thread::yield_now();
        }
        let observed = events.lock().expect("lifecycle events").clone();
        assert!(
            observed.contains(&NativeFragmentLifecycleEvent::Registered),
            "the race must cross real fragment registration: {observed:?}"
        );
        assert!(
            observed.contains(&NativeFragmentLifecycleEvent::Cancelled),
            "late lifecycle cancellation must reach the published fragment control: {observed:?}"
        );
        assert!(
            !observed.contains(&NativeFragmentLifecycleEvent::Started),
            "an aborted lifecycle permit must never start the worker: {observed:?}"
        );

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match service.controls.reserve(fragment_instance_id) {
                Ok(reservation) => {
                    drop(reservation);
                    break;
                }
                Err(_) if Instant::now() < deadline => std::thread::yield_now(),
                Err(error) => panic!("aborted fragment route did not terminate: {error}"),
            }
        }
        assert!(
            service
                .queries
                .cancel_query(query_id, "post-abort probe".to_string())
                .is_empty(),
            "aborted query must not retain the late fragment registration"
        );
    }

    #[test]
    fn delivered_splits_reach_the_staged_decode_context_queue() {
        // Stage and delivery derive the task key independently. If they ever
        // disagreed, a task would block forever on a queue nobody fills.
        let _service_guard = SERVICE_TEST_LOCK.lock().expect("service test lock");
        let service = NativeFragmentService::new(
            crate::fragment::grpc_exchange_transmitter(
                crate::rpc::runtime::test_backend_data_runtime(),
            ),
            crate::fragment::grpc_fragment_lookup_client(
                crate::rpc::runtime::test_backend_data_runtime(),
            ),
            crate::fragment::native_result_writer(),
            test_lifecycle_registry(Arc::new(FragmentControlRegistry::default())),
        );

        let execution_id = QueryExecutionId::new(
            novarocks_types::QueryId::new(77, 5),
            novarocks_types::AttemptId::new(1).expect("attempt"),
        )
        .expect("execution id");
        let fragment_instance_id = novarocks_types::UniqueId::new(77, 9);
        let attempt_key =
            novarocks_execution::connector::TaskAttemptKey::new(execution_id, fragment_instance_id);
        let mut provisional_read_context = super::ProvisionalTypedReadContext::new(
            Arc::clone(&service.read_contexts),
            attempt_key,
        );
        let params = novarocks_proto_models::novarocks::InstanceParams {
            fragment_instance_id: Some(novarocks_proto_models::common::UniqueId {
                hi: fragment_instance_id.high(),
                lo: fragment_instance_id.low(),
            }),
            ..Default::default()
        };
        let runtime = service.typed_scan_runtime(execution_id, &params);
        runtime
            .register_read_execution(
                3,
                crate::connector::typed_runtime::test_support::installed_read_execution(),
            )
            .expect("stage registers the typed read execution");
        provisional_read_context.publish();

        let raw = novarocks_proto_models::connector_read::SplitAssignment {
            plan_node_id: 3,
            splits: vec![crate::connector::typed_runtime::test_support::split_proto(
                3, 0,
            )],
            no_more_splits: true,
        };
        let assignment = novarocks_proto_codec::connector_read::SplitAssignment::parse(
            raw,
            novarocks_proto_codec::FieldPath::root("split_assignment"),
        )
        .expect("valid assignment");

        let ack = service.deliver_split_assignments(
            execution_id,
            fragment_instance_id,
            std::slice::from_ref(&assignment),
        );
        assert!(matches!(
            ack,
            crate::query_lifecycle::task_update::TaskUpdateAck::Accepted(_)
        ));

        let queue = runtime
            .queues()
            .existing_queue(3)
            .expect("the delivered plan node has a queue");
        let stats = queue.stats();
        assert_eq!(stats.queued_splits, 1);
        assert!(stats.no_more_splits);

        close_task_split_queues(&service.split_queues, execution_id, fragment_instance_id);
        assert!(queue.is_closed());
    }

    #[test]
    fn registration_failure_drops_dormant_resources_before_retry() {
        let _service_guard = SERVICE_TEST_LOCK.lock().expect("service test lock");
        let service = NativeFragmentService::new(
            crate::fragment::grpc_exchange_transmitter(
                crate::rpc::runtime::test_backend_data_runtime(),
            ),
            crate::fragment::grpc_fragment_lookup_client(
                crate::rpc::runtime::test_backend_data_runtime(),
            ),
            crate::fragment::native_result_writer(),
            test_lifecycle_registry(Arc::new(FragmentControlRegistry::default())),
        );
        let first = values_result_request(82_000, 82_002);
        let finst_id = first.fragment_instance_id();
        make_control_ready(&service, &first, [finst_id]);
        let reservation = service
            .controls
            .reserve(finst_id)
            .expect("reserve conflicting service route");

        let error = service
            .submit(first)
            .expect_err("duplicate service registration must fail");
        assert!(error.to_string().contains("already registered"), "{error}");

        drop(reservation);
        service
            .submit(values_result_request(82_000, 82_002))
            .expect("retry must observe rolled-back dormant resources");
    }

    #[test]
    fn second_worker_spawn_failure_rolls_back_only_its_pre_start_registration() {
        let _service_guard = SERVICE_TEST_LOCK.lock().expect("service test lock");
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let release_rx = Arc::new(Mutex::new(release_rx));
        let worker_release = Arc::clone(&release_rx);
        let service = NativeFragmentService::with_lifecycle_observer_and_worker_spawn_failure(
            move |event| {
                if event == NativeFragmentLifecycleEvent::Started {
                    started_tx.send(()).expect("publish first worker start");
                    worker_release
                        .lock()
                        .expect("first worker release")
                        .recv()
                        .expect("release first worker");
                }
            },
            2,
        );
        let query_id = QueryId::new(83_000, 83_001);
        let first = UniqueId::new(83_002, 83_003);
        let second = UniqueId::new(83_004, 83_005);

        let first_request = values_result_request(83_000, 83_002);
        make_control_ready(&service, &first_request, [first, second]);
        service
            .submit(first_request)
            .expect("first fragment reaches running");
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("first worker remains registered");

        let error = service
            .submit(values_result_request(83_000, 83_004))
            .expect_err("second worker spawn is injected to fail");
        assert!(error.to_string().contains("spawn failure"), "{error}");
        assert!(
            service.controls.reserve(first).is_err(),
            "first running route must remain registered"
        );
        drop(
            service
                .controls
                .reserve(second)
                .expect("failed second registration must release its route"),
        );

        service
            .submit(values_result_request(83_000, 83_004))
            .expect("retry of the same fragment proves lifecycle admission rollback");
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("retried fragment reaches worker start");

        release_tx.send(()).expect("release first worker");
        release_tx.send(()).expect("release retried worker");
        let deadline = Instant::now() + Duration::from_secs(2);
        let first_reservation = loop {
            match service.controls.reserve(first) {
                Ok(reservation) => break reservation,
                Err(_) if Instant::now() < deadline => std::thread::yield_now(),
                Err(error) => panic!("first fragment did not terminate: {error}"),
            }
        };
        loop {
            match service.controls.reserve(second) {
                Ok(reservation) => {
                    drop(reservation);
                    break;
                }
                Err(_) if Instant::now() < deadline => std::thread::yield_now(),
                Err(error) => panic!("second fragment did not terminate: {error}"),
            }
        }
        drop(first_reservation);
        assert!(
            service
                .queries
                .cancel_query(query_id, "post-terminal probe".to_string())
                .is_empty(),
            "terminated query must not retain either fragment mapping"
        );
    }

    #[test]
    fn native_failed_terminal_fact_does_not_locally_cancel_siblings_before_frontend_ack() {
        let _service_guard = SERVICE_TEST_LOCK.lock().expect("service test lock");
        let service = NativeFragmentService::with_lifecycle_observer(|_| {});
        let request = values_result_request(83_100, 83_104);
        let query_id = request.query_id();
        let execution_id = request.execution_id();
        let failed_finst = request.fragment_instance_id();
        let sibling_finst = UniqueId::new(83_102, 83_103);
        let delivery_expire = Duration::from_secs(1);
        let query_expire = Duration::from_secs(5);

        let sibling_registration = service
            .queries
            .register_fragment(query_id, sibling_finst, delivery_expire, query_expire)
            .expect("register sibling fragment");
        sibling_registration.into_running();
        let sibling_control = Arc::new(RecordingControl::default());
        let sibling_token = service
            .controls
            .reserve(sibling_finst)
            .expect("reserve sibling control")
            .publish(sibling_control.clone());

        let failed_registration = service
            .queries
            .register_fragment(query_id, failed_finst, delivery_expire, query_expire)
            .expect("register failed fragment");
        failed_registration.into_running();
        let failed =
            prepare_request_for_test(&service, request).start_failed("native executor failure");
        let failed_token = service
            .controls
            .reserve(failed_finst)
            .expect("reserve failed control")
            .publish(Arc::new(RunningFragmentControl {
                handle: failed.clone(),
            }));

        consume_terminal_fact(
            failed,
            failed_token,
            service.queries.clone(),
            Arc::clone(&service.split_queues),
            Arc::clone(&service.read_contexts),
            Arc::clone(&service.lifecycle),
            execution_id,
            0,
        );

        assert!(
            sibling_control
                .reasons
                .lock()
                .expect("recording control reasons")
                .is_empty(),
            "native failure must be reported to the frontend before any query-wide sibling cancellation"
        );

        sibling_token.complete();
        service
            .queries
            .unregister_fragment_execution(execution_id, sibling_finst);
        service.queries.finish_fragment(execution_id);
    }

    /// A single-fragment write dataflow: `Values -> TableWriter -> TableFinish`
    /// into the ordinary RESULT sink. In production an exchange separates the
    /// writer from the finish node, but the sink, the result buffer, and the
    /// EOF the frontend reads are the same ones this exercises.
    fn write_dataflow_result_request(
        query_base: i64,
        fragment_base: i64,
        execution: Arc<crate::connector::write_test_support::RecordingWriteExecution>,
    ) -> NativeFragmentRequest {
        use crate::connector::write_test_support::{
            finish_node, never_cancelled, table_writer_payload, test_write_scan_runtime,
            writer_node,
        };
        use crate::fragment::decode::type_decode::encode_type;
        use arrow::datatypes::DataType;

        let fragment_id = 11;
        let execution_id = QueryExecutionId::new(
            ExecutionQueryId::new(query_base, query_base + 1),
            ProtocolAttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("valid execution id");
        let fragment_instance_id = UniqueId::new(fragment_base, fragment_base + 1);
        let int_type = encode_type(&DataType::Int64).expect("encode int64");
        let column = proto::common::OutputColumn {
            column_id: 1,
            name: "id".to_string(),
            r#type: Some(int_type.clone()),
            nullable: true,
            is_internal: false,
        };
        let values = proto::plan::DistributedNode {
            node_id: 40,
            fragment_id,
            limit: -1,
            payload: Some(proto::plan::distributed_node::Payload::Physical(
                proto::plan::PlanNode {
                    output_columns: vec![column.clone()],
                    kind: Some(proto::plan::plan_node::Kind::Values(
                        proto::plan::ValuesNode {
                            rows: vec![proto::plan::ExprList {
                                values: vec![proto::expr::Expr {
                                    r#type: Some(int_type.clone()),
                                    nullable: false,
                                    kind: Some(proto::expr::expr::Kind::Literal(
                                        proto::expr::LiteralExpr {
                                            value: Some(proto::common::LiteralValue {
                                                value: Some(
                                                    proto::common::literal_value::Value::IntValue(
                                                        7,
                                                    ),
                                                ),
                                            }),
                                        },
                                    )),
                                }],
                            }],
                            columns: vec![column.clone()],
                        },
                    )),
                },
            )),
            ..Default::default()
        };
        let writer = writer_node(
            41,
            table_writer_payload(
                proto::expr::Expr {
                    r#type: Some(int_type),
                    nullable: true,
                    kind: Some(proto::expr::expr::Kind::ColumnRef(proto::expr::ColumnRef {
                        column_id: 1,
                        qualifier: None,
                        column: None,
                    })),
                },
                vec![column],
            ),
            vec![values],
        );
        let root = finish_node(42, vec![0], vec![writer]);

        NativeFragmentRequest::try_decode_with_runtime(
            execution_id,
            proto::plan::PlanFragment {
                fragment_id,
                root: Some(root),
                sink: Some(proto::plan::DataSink {
                    kind: Some(proto::plan::data_sink::Kind::Result(true)),
                }),
                output_columns: Vec::new(),
                runtime_filter_bindings: Some(proto::plan::RuntimeFilterBindingTable {
                    fragment_id,
                    bindings: Vec::new(),
                }),
                ..Default::default()
            },
            proto::novarocks::InstanceParams {
                query_id: Some(proto::common::UniqueId {
                    hi: query_base,
                    lo: query_base + 1,
                }),
                fragment_instance_id: Some(proto::common::UniqueId {
                    hi: fragment_instance_id.high(),
                    lo: fragment_instance_id.low(),
                }),
                backend_num: 3,
                query_options: Some(proto::novarocks::QueryOptions {
                    batch_size: 1024,
                    pipeline_dop: 1,
                    ..Default::default()
                }),
                ..Default::default()
            },
            never_cancelled(),
            std::time::Duration::from_millis(120_000),
            Some(test_write_scan_runtime(
                execution_id,
                fragment_instance_id,
                execution,
            )),
        )
        .expect("valid native write dataflow request")
    }

    #[test]
    fn the_write_dataflow_root_relation_reaches_the_result_buffer_and_ends_with_eof() {
        use crate::runtime::result_buffer::{TryFetchResult, try_fetch};

        let _service_guard = SERVICE_TEST_LOCK.lock().expect("service test lock");
        let service = NativeFragmentService::with_lifecycle_observer(|_| {});
        let execution =
            Arc::new(crate::connector::write_test_support::RecordingWriteExecution::new());
        let request = write_dataflow_result_request(86_000, 86_002, Arc::clone(&execution));
        let finst_id = request.fragment_instance_id();

        let handle = prepare_request_for_test(&service, request);
        let outcome = handle.start().join();
        assert!(
            matches!(outcome.outcome(), FragmentOutcome::Succeeded),
            "the write dataflow fragment must succeed: {outcome:?}"
        );
        assert_eq!(
            execution.opened(),
            vec![(0, 0, 0)],
            "the single writer driver opened exactly one writer"
        );
        let terminals = execution.terminals();
        assert_eq!(
            terminals,
            crate::connector::write_test_support::WriterTerminals {
                finished: 1,
                aborted: 0,
                appended_rows: 1,
            },
            "the writer accepted its projected row and finished exactly once"
        );

        let TryFetchResult::Ready(first) = try_fetch(finst_id) else {
            panic!("expected the root relation in the result buffer");
        };
        assert!(!first.eos, "the root relation arrives before EOF");
        assert_eq!(
            first.result_batch.rows.len(),
            1,
            "one SUMMARY row, and no prepared fragment because the writer staged nothing"
        );
        let TryFetchResult::Ready(eof) = try_fetch(finst_id) else {
            panic!("expected EOF after the root relation");
        };
        assert!(
            eof.eos,
            "the frontend sees EOF once every sender reached EOS"
        );
    }

    #[test]
    fn an_aborted_write_dataflow_never_publishes_eof() {
        use crate::runtime::result_buffer::{TryFetchResult, try_fetch};

        let _service_guard = SERVICE_TEST_LOCK.lock().expect("service test lock");
        let service = NativeFragmentService::with_lifecycle_observer(|_| {});
        let execution =
            Arc::new(crate::connector::write_test_support::RecordingWriteExecution::new());
        let request = write_dataflow_result_request(86_100, 86_102, Arc::clone(&execution));
        let finst_id = request.fragment_instance_id();

        let handle = prepare_request_for_test(&service, request);
        let outcome = handle
            .start_failed("aborted before the write dataflow ran")
            .join();
        assert!(
            matches!(outcome.outcome(), FragmentOutcome::Failed(_)),
            "an aborted attempt never succeeds: {outcome:?}"
        );
        // Drivers, and therefore writers, are created while the fragment is
        // prepared. What an abort must guarantee is not that no writer opened,
        // but that none of them finished: a writer that never finished staged
        // nothing the frontend could commit.
        let terminals = execution.terminals();
        assert_eq!(
            terminals.finished, 0,
            "an aborted attempt must never finish a writer: {terminals:?}"
        );
        match try_fetch(finst_id) {
            TryFetchResult::Error(_) => {}
            TryFetchResult::Ready(result) => panic!(
                "an aborted attempt must not publish a result batch (eos={})",
                result.eos
            ),
            TryFetchResult::NotReady => {
                panic!("an aborted attempt must publish a terminal state, not stay pending")
            }
        }
    }

    #[test]
    fn failure_trigger_skips_ineligible_fragment_and_fails_exactly_one_eligible_fragment() {
        let _service_guard = SERVICE_TEST_LOCK.lock().expect("service test lock");
        let trigger = std::env::temp_dir().join(format!(
            "novarocks-fragment-failure-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos()
        ));
        std::fs::write(&trigger, b"step-token-17").expect("arm fragment failure");
        let service = NativeFragmentService::with_lifecycle_observer(|_| {});
        let first = prepare_request_for_test(&service, values_result_request(85_000, 85_002));

        let (first, release) =
            start_with_fragment_failure_trigger(first, Some(trigger.as_path()), false);
        assert!(
            release.is_none(),
            "an ineligible result fragment must not claim the trigger"
        );
        assert!(
            trigger.exists(),
            "an ineligible result fragment must leave the trigger armed"
        );
        let first = first.join();
        assert!(
            matches!(first.outcome(), FragmentOutcome::Succeeded),
            "the ineligible fragment must run normally: {first:?}"
        );

        let second = prepare_request_for_test(&service, values_result_request(85_100, 85_102));
        let (second, release) =
            start_with_fragment_failure_trigger(second, Some(trigger.as_path()), true);
        assert!(
            second.submitted_driver_count() > 0,
            "the injected failure must happen after the fragment enters started state"
        );
        std::fs::write(trigger.with_extension("release"), b"step-token-17")
            .expect("release fragment failure");
        assert_eq!(
            release
                .expect("armed fragment has a pending release")
                .wait()
                .expect("matching release token"),
            "step-token-17"
        );
        let failed = second.join();
        assert!(matches!(
            failed.outcome(),
            FragmentOutcome::Failed(error)
                if error.detail() == FRAGMENT_EXECUTOR_FAILURE_MESSAGE
        ));
        assert!(!trigger.exists(), "the trigger must be consumed once");

        let third = prepare_request_for_test(&service, values_result_request(85_200, 85_202));
        let (third, release) =
            start_with_fragment_failure_trigger(third, Some(trigger.as_path()), true);
        assert!(
            release.is_none(),
            "the consumed trigger must not create another release rendezvous"
        );
        let succeeded = third.join();
        assert!(
            matches!(succeeded.outcome(), FragmentOutcome::Succeeded),
            "the consumed trigger must not poison later fragments: {succeeded:?}"
        );
    }
}
