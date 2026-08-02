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

use std::collections::BTreeSet;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(test)]
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use novarocks::common::app_config;
use novarocks::connector::ConnectorRegistry;
use novarocks::novarocks_logging::error;
#[cfg(test)]
use novarocks::novarocks_logging::warn;
use novarocks::query_execution::lifecycle::StageFragment;
use novarocks::runtime::fragment::{
    ExchangeFrameTransmitter, FragmentEventSink, FragmentLookupClient, FragmentResultWriter,
};
use novarocks::runtime::fragment::{
    FragmentCancelReason, FragmentOutcome, RunningFragmentHandle, prepare_fragment,
};
use novarocks::runtime::native_fragment_query::NativeFragmentQueryRuntime;
use novarocks::runtime::profile::Profiler;
use novarocks_spi::connector::{
    ConnectorExecutionBindingKey, ConnectorExecutionDeclaration, ConnectorRequestContext,
};

use super::control::{FragmentControlHandle, FragmentControlRegistry};
#[cfg(test)]
use super::failure_injection::start_with_configured_fragment_failure_trigger;
use super::failure_injection::{
    FRAGMENT_EXECUTOR_FAILURE_MESSAGE, claim_configured_fragment_failure_trigger,
};
use crate::ConnectorExecutionHost;
use crate::native::decode::NativeFragmentRequest;
use crate::native::ingress::{
    NativeFragmentCancelRequest, NativeFragmentIngress, NativeFragmentIngressError,
};
use crate::query_lifecycle::{QueryLifecycleRegistry, stage::StartGate};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NativeFragmentLifecycleEvent {
    Prepared,
    Registered,
    Cancelled,
    Accepted,
    Started,
}

type LifecycleObserver = Arc<dyn Fn(NativeFragmentLifecycleEvent) + Send + Sync>;

#[cfg(debug_assertions)]
fn runner_stage_prepare_failure(
    available_fragments: usize,
) -> Result<
    Option<novarocks::common::query_lifecycle_fault::StagePrepareFailure>,
    NativeFragmentIngressError,
> {
    let Some(root) = app_config::config()
        .ok()
        .and_then(|config| config.debug.query_lifecycle_fault_dir())
    else {
        return Ok(None);
    };
    novarocks::common::query_lifecycle_fault::claim_stage_prepare_failure(root, available_fragments)
        .map_err(NativeFragmentIngressError::new)
}

#[cfg(not(debug_assertions))]
fn runner_stage_prepare_failure(
    _available_fragments: usize,
) -> Result<
    Option<novarocks::common::query_lifecycle_fault::StagePrepareFailure>,
    NativeFragmentIngressError,
> {
    Ok(None)
}

pub struct NativeFragmentService {
    pub(super) controls: Arc<FragmentControlRegistry>,
    lifecycle: Arc<QueryLifecycleRegistry>,
    queries: NativeFragmentQueryRuntime,
    exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
    lookup_client: Arc<dyn FragmentLookupClient>,
    result_writer: Arc<dyn FragmentResultWriter>,
    connector_registry: Arc<ConnectorRegistry>,
    execution_host: Arc<ConnectorExecutionHost>,
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
        connector_registry: Arc<ConnectorRegistry>,
    ) -> Self {
        Self::new_with_controls(
            exchange_transmitter,
            lookup_client,
            result_writer,
            Arc::new(FragmentControlRegistry::default()),
            lifecycle,
            connector_registry,
            Arc::new(ConnectorExecutionHost::new()),
        )
    }

    pub(crate) fn new_with_controls(
        exchange_transmitter: Arc<dyn ExchangeFrameTransmitter>,
        lookup_client: Arc<dyn FragmentLookupClient>,
        result_writer: Arc<dyn FragmentResultWriter>,
        controls: Arc<FragmentControlRegistry>,
        lifecycle: Arc<QueryLifecycleRegistry>,
        connector_registry: Arc<ConnectorRegistry>,
        execution_host: Arc<ConnectorExecutionHost>,
    ) -> Self {
        Self {
            controls,
            lifecycle,
            queries: NativeFragmentQueryRuntime::global(),
            exchange_transmitter,
            lookup_client,
            result_writer,
            connector_registry,
            execution_host,
            lifecycle_observer: None,
            #[cfg(test)]
            after_lifecycle_admission: None,
            #[cfg(test)]
            fail_worker_spawn_on_submission: None,
            #[cfg(test)]
            submission_count: AtomicUsize::new(0),
        }
    }

    #[cfg(test)]
    fn with_lifecycle_observer(
        observer: impl Fn(NativeFragmentLifecycleEvent) + Send + Sync + 'static,
    ) -> Self {
        let controls = Arc::new(FragmentControlRegistry::default());
        let lifecycle = test_lifecycle_registry(Arc::clone(&controls));
        let mut service = Self::new_with_controls(
            crate::fragment::grpc_exchange_transmitter(),
            crate::fragment::grpc_fragment_lookup_client(),
            crate::fragment::native_result_writer(),
            controls,
            lifecycle,
            Arc::new(ConnectorRegistry::new()),
            Arc::new(ConnectorExecutionHost::new()),
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
    pub(crate) fn stage_fragments(
        &self,
        execution_id: novarocks::query_execution::lifecycle::QueryExecutionId,
        fragments: &[StageFragment],
        gate: Arc<StartGate>,
    ) -> Result<(), NativeFragmentIngressError> {
        let injected_failure = runner_stage_prepare_failure(fragments.len())?;
        for (index, fragment) in fragments.iter().enumerate() {
            if injected_failure
                .as_ref()
                .is_some_and(|failure| failure.ordinal == index.saturating_add(1))
            {
                let failure = injected_failure.expect("checked injected Stage failure");
                eprintln!(
                    "NOVAROCKS_STAGE_PREPARE_FAILED execution_id={}:{}:{} ordinal={} token={}",
                    execution_id.query_id().high(),
                    execution_id.query_id().low(),
                    execution_id.attempt_id().get(),
                    failure.ordinal,
                    failure.token
                );
                return Err(NativeFragmentIngressError::new(format!(
                    "runner-owned Stage prepare failure at ordinal {}",
                    failure.ordinal
                )));
            }
            let request = NativeFragmentRequest::try_decode_with_execution_resolver(
                execution_id,
                fragment.plan().clone(),
                fragment.instance_params().clone(),
                Arc::clone(&self.connector_registry),
                Arc::new(self.execution_host.resolver_for(execution_id)),
                self.queries
                    .connector_cancellation_for_execution(execution_id),
            )?;
            self.stage_one(request, Arc::clone(&gate))?;
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
        let cache_options = request
            .cache_options()
            .map_err(NativeFragmentIngressError::new)?;
        let profiler =
            enable_profile.then(|| profiler_for_native_fragment(request.root_plan_node_id()));
        let admission = self
            .queries
            .prepare_admission_execution(
                execution_id,
                fragment_instance_id,
                delivery_expire,
                query_expire,
                cache_options,
                runtime_filter,
            )
            .map_err(NativeFragmentIngressError::new)?;
        let query_mem_tracker = admission.query_mem_tracker();
        let fragment_mem_tracker = admission.fragment_mem_tracker();
        let failure_injection_eligible = !request.uses_result_sink();
        let event_sink = crate::fragment::lifecycle_fragment_event_sink(
            Arc::clone(&self.lifecycle),
            execution_id,
            profiler.clone(),
        );
        let dormant = prepare_fragment(
            request.into_submission(),
            admission.into_prepare_context(
                profiler.clone(),
                Arc::clone(&self.exchange_transmitter),
                Arc::clone(&self.lookup_client),
                Arc::clone(&self.result_writer),
                event_sink,
            ),
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
        let queries = self.queries.clone();
        let lifecycle = Arc::clone(&self.lifecycle);
        let observer = self.lifecycle_observer.clone();
        std::thread::Builder::new()
            .name(format!(
                "native-fragment-{:x}-{:x}",
                fragment_instance_id.high(), fragment_instance_id.low()
            ))
            .spawn(move || {
                if gate.wait() != crate::query_lifecycle::stage::StartGateState::Released {
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
        let cache_options = request
            .cache_options()
            .map_err(NativeFragmentIngressError::new)?;
        let profiler =
            enable_profile.then(|| profiler_for_native_fragment(request.root_plan_node_id()));
        let admission = self
            .queries
            .prepare_admission_execution(
                execution_id,
                fragment_instance_id,
                delivery_expire,
                query_expire,
                cache_options,
                runtime_filter,
            )
            .map_err(NativeFragmentIngressError::new)?;
        let query_mem_tracker = admission.query_mem_tracker();
        let fragment_mem_tracker = admission.fragment_mem_tracker();
        let failure_injection_eligible = !request.uses_result_sink();
        let event_sink = crate::fragment::lifecycle_fragment_event_sink(
            Arc::clone(&self.lifecycle),
            execution_id,
            profiler.clone(),
        );
        let dormant = prepare_fragment(
            request.into_submission(),
            admission.into_prepare_context(
                profiler.clone(),
                Arc::clone(&self.exchange_transmitter),
                Arc::clone(&self.lookup_client),
                Arc::clone(&self.result_writer),
                event_sink,
            ),
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
                    error!(target: "novarocks::exec", finst_id = %fragment_instance_id, %error, "native fragment start signal was dropped");
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
    fn ensure_connector_execution_binding(
        &self,
        execution_id: novarocks::query_execution::lifecycle::QueryExecutionId,
        declaration: ConnectorExecutionDeclaration,
        context: ConnectorRequestContext,
    ) -> Result<(), NativeFragmentIngressError> {
        self.execution_host
            .ensure(execution_id, &declaration, &context)
            .map_err(NativeFragmentIngressError::new)
    }

    fn retire_connector_execution_binding(
        &self,
        key: ConnectorExecutionBindingKey,
    ) -> Result<(), NativeFragmentIngressError> {
        self.execution_host
            .retire(&key)
            .map_err(NativeFragmentIngressError::new)
    }

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

fn consume_terminal_fact(
    running: RunningFragmentHandle,
    token: super::control::FragmentControlToken,
    queries: NativeFragmentQueryRuntime,
    lifecycle: Arc<QueryLifecycleRegistry>,
    execution_id: novarocks::query_execution::lifecycle::QueryExecutionId,
    backend_num: i32,
) {
    let fact = running.join();
    let fragment_instance_id = fact.fragment_instance_id();
    if let FragmentOutcome::Failed(execution_error) = fact.outcome() {
        error!(target: "novarocks::exec", finst_id = %fragment_instance_id, error = %execution_error, "native fragment execution failed");
    }
    let sink = novarocks::runtime::sink_commit::report_snapshot(fragment_instance_id)
        .with_connector_staged_report_frames(running.take_connector_staged_report_frames());
    // QLC terminal facts are transferred before local runtime cleanup.
    lifecycle.record_fragment_terminal_fact(execution_id, fact, backend_num, sink);
    queries.unregister_fragment_execution(execution_id, fragment_instance_id);
    queries.finish_fragment(execution_id);
    // Publish the terminal report before this fact can fail-close the local
    // lifecycle. Otherwise a sibling cancelled by the first terminal fact may
    // win the report slot and hide the fragment that actually failed.
    token.complete();
}

fn profiler_for_native_fragment(root_plan_node_id: i32) -> Profiler {
    let profiler = Profiler::new(format!(
        "execute_fragment_native (plan_node_id={root_plan_node_id})"
    ));
    profiler.set_metadata(i64::from(root_plan_node_id));
    profiler
}

#[cfg(test)]
fn test_lifecycle_registry(controls: Arc<FragmentControlRegistry>) -> Arc<QueryLifecycleRegistry> {
    let registry = QueryLifecycleRegistry::new_unbound(
        1,
        Arc::new(
            crate::query_lifecycle::NativeQueryLifecycleLocalRuntime::new(
                controls,
                Arc::new(ConnectorExecutionHost::new()),
            ),
        ),
        crate::query_lifecycle::QueryLifecycleRegistryConfig::from_runtime_config(
            &novarocks::common::app_config::RuntimeConfig::default(),
        ),
    );
    registry
        .bind_backend_identity(7)
        .expect("test lifecycle backend identity");
    registry
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, mpsc};
    use std::time::{Duration, Instant};

    use novarocks::connector::ConnectorRegistry;
    use novarocks::query_execution::lifecycle::{
        AttemptId, ParticipantBackendIdentity, ParticipantManifest, ParticipantQueryOptions,
        ParticipantRole, QueryControlAttach, QueryControlAttachment, QueryControlEndpoint,
        QueryExecutionId, QueryInitOutcome, QueryInitRequest,
    };
    use novarocks::runtime::fragment::{DormantFragmentHandle, FragmentOutcome, prepare_fragment};
    use novarocks_protocol as proto;
    use novarocks_types::QueryId as ExecutionQueryId;
    use novarocks_types::QueryId;
    use novarocks_types::UniqueId;

    use crate::fragment::control::{FragmentControlHandle, FragmentControlRegistry};
    use crate::fragment::failure_injection::{
        FRAGMENT_EXECUTOR_FAILURE_MESSAGE, start_with_fragment_failure_trigger,
    };
    use crate::native::ingress::{NativeFragmentCancelRequest, NativeFragmentIngress};

    use super::{
        NativeFragmentLifecycleEvent, NativeFragmentRequest, NativeFragmentService,
        RunningFragmentControl, consume_terminal_fact, test_lifecycle_registry,
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
                AttemptId::new(1).expect("nonzero attempt"),
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
            Arc::new(ConnectorRegistry::new()),
        )
        .expect("valid native fragment request")
    }

    fn make_control_ready(
        service: &NativeFragmentService,
        request: &NativeFragmentRequest,
        expected_fragments: impl IntoIterator<Item = UniqueId>,
    ) -> QueryControlAttachment {
        let execution_id = request.execution_id();
        let manifest =
            ParticipantManifest::new(
                execution_id,
                ParticipantBackendIdentity::new(
                    7,
                    QueryControlEndpoint::new("127.0.0.1", 19030).expect("control endpoint"),
                    1,
                )
                .expect("backend identity"),
                [ParticipantRole::FragmentExecutor],
                expected_fragments,
                ParticipantQueryOptions::new(
                    novarocks::runtime::query_options::QueryOptions::default(),
                ),
                u64::MAX,
                [],
                None,
                Duration::from_secs(30),
                QueryControlEndpoint::new("127.0.0.1", 19031).expect("report endpoint"),
            )
            .expect("fragment participant manifest");
        let init = QueryInitRequest::from_manifest(manifest);
        assert_eq!(
            service.lifecycle.init_query(init.clone()).outcome(),
            QueryInitOutcome::Applied
        );
        let mut attachment = service
            .lifecycle
            .attach_control(
                QueryControlAttach::new(execution_id, init.digest(), 1)
                    .expect("control attachment"),
            )
            .expect("control attaches");
        assert_eq!(
            attachment.events.try_recv().expect("ControlReady"),
            novarocks::query_execution::lifecycle::QueryControlEvent::ControlReady
        );
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
                request.cache_options().expect("valid cache options"),
                None,
            )
            .expect("native fragment admission");
        prepare_fragment(
            request.into_submission(),
            admission.into_prepare_context(
                None,
                Arc::clone(&service.exchange_transmitter),
                Arc::clone(&service.lookup_client),
                Arc::clone(&service.result_writer),
                crate::fragment::lifecycle_fragment_event_sink(
                    Arc::clone(&service.lifecycle),
                    execution_id,
                    None,
                ),
            ),
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
    fn registration_failure_drops_dormant_resources_before_retry() {
        let _service_guard = SERVICE_TEST_LOCK.lock().expect("service test lock");
        let service = NativeFragmentService::new(
            crate::fragment::grpc_exchange_transmitter(),
            crate::fragment::grpc_fragment_lookup_client(),
            crate::fragment::native_result_writer(),
            test_lifecycle_registry(Arc::new(FragmentControlRegistry::default())),
            Arc::new(ConnectorRegistry::new()),
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
