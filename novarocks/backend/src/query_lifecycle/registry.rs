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

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use novarocks::novarocks_logging::{info, warn};
use novarocks::query_execution::lifecycle::metrics::BackendQueryLifecycleMetricsSnapshot;
use novarocks::query_execution::lifecycle::{
    BackendQueryControl, FragmentLiveObservation, FragmentTerminalOutcome,
    FragmentTerminalSnapshot, ImmutableQueryTerminalRecord, ParticipantManifestDigest,
    ParticipantRole, QueryAbortRequest, QueryControlAttach, QueryControlAttachment,
    QueryControlEvent, QueryExecutionId, QueryInitAck, QueryInitOutcome, QueryInitRequest,
    QueryLifecycleError, QueryLifecycleErrorCode, QueryLifecycleIngress,
    QueryLifecycleTransportError, QueryLifecycleTransportErrorKind, QueryStageAck,
    QueryStageOutcome, QueryStageRequest, QueryStartAck, QueryStartOutcome, QueryStartRequest,
    QueryTerminalAck, QueryTerminalFallbackTransport, QueryTerminalReportAck,
    QueryTerminalReportOutcome, QueryTerminalSnapshot, QueryTerminationAck, QueryTerminationReason,
    RuntimeFilterContribution, StageDigest, StageDigestVersion,
};
use novarocks::runtime::fragment::{FragmentOutcome, FragmentTerminalFact};
use novarocks::runtime::profile::RuntimeProfileTree;
use novarocks::runtime::sink_commit::SinkCommitReportSnapshot;
use novarocks::runtime_filter_transition::port::transport::{
    RuntimeFilterEnvelope, RuntimeFilterEnvelopeIngress, RuntimeFilterIngressResult,
};
use novarocks_execution::runtime_filter::RuntimeFilterSessionRef;
use novarocks_types::UniqueId;
use prost::Message;

use super::entry::{QueryLifecycleEntry, QueryLifecyclePhase};
use crate::runtime_filter::participant::{
    BackendRuntimeFilterParticipantFactory, RuntimeFilterParticipant,
    RuntimeFilterParticipantFactory,
};

const CONTROL_EVENT_BUFFER_CAPACITY: usize = 16;
const RESERVED_CONTROL_EVENT_CAPACITY: usize = 3;

fn send_reserved_control_event(
    permit: Option<tokio::sync::mpsc::OwnedPermit<QueryControlEvent>>,
    events: Option<tokio::sync::mpsc::Sender<QueryControlEvent>>,
    event: QueryControlEvent,
) {
    if let Some(permit) = permit {
        drop(permit.send(event));
    } else if let Some(events) = events {
        // The fallback is only reachable for entries created before a permit
        // was installed or after a duplicate terminal transition. Preserve
        // the existing best-effort behavior without blocking a runtime thread.
        let _ = events.try_send(event);
    }
}

impl RuntimeFilterEnvelopeIngress for QueryLifecycleRegistry {
    fn accept(&self, envelope: RuntimeFilterEnvelope) -> RuntimeFilterIngressResult {
        self.dispatch_runtime_filter_envelope(envelope)
    }
}

pub(crate) trait QueryLifecycleLocalRuntime: Send + Sync + 'static {
    fn terminate_query(
        &self,
        execution_id: QueryExecutionId,
        expected_instances: &[UniqueId],
        reason: QueryTerminationReason,
        detail: &str,
    );
}

pub(crate) trait MonotonicClock: Send + Sync + 'static {
    fn now(&self) -> Instant;
}

pub(crate) trait QueryLifecycleMetricsSink: Send + Sync + 'static {
    fn publish(
        &self,
        snapshot: BackendQueryLifecycleMetricsSnapshot,
        termination_reasons: [u64; 6],
    );
}

struct SystemMonotonicClock;

impl MonotonicClock for SystemMonotonicClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

struct PrometheusQueryLifecycleMetricsSink;

impl QueryLifecycleMetricsSink for PrometheusQueryLifecycleMetricsSink {
    fn publish(
        &self,
        snapshot: BackendQueryLifecycleMetricsSnapshot,
        termination_reasons: [u64; 6],
    ) {
        novarocks::service::publish_backend_query_lifecycle_metrics(snapshot, termination_reasons);
    }
}

#[derive(Clone, Copy)]
pub(crate) struct QueryLifecycleRegistryConfig {
    pub(crate) max_active_entries: usize,
    pub(crate) tombstone_capacity: usize,
    pub(crate) tombstone_retention: Duration,
    pub(crate) heartbeat_timeout: Duration,
    pub(crate) pre_start_timeout: Duration,
    pub(crate) stage_max_fragments: usize,
    pub(crate) max_active_staging: usize,
    pub(crate) stage_max_encoded_bytes: usize,
    pub(crate) stage_max_inflight_encoded_bytes: usize,
    pub(crate) stage_max_dormant_workers: usize,
    pub(crate) terminal_max_encoded_bytes: usize,
    pub(crate) terminal_drain_timeout: Duration,
    pub(crate) terminal_ack_timeout: Duration,
    pub(crate) terminal_fallback_rpc_timeout: Duration,
    pub(crate) terminal_fallback_max_attempts: usize,
    pub(crate) terminal_fallback_initial_backoff: Duration,
    pub(crate) terminal_fallback_max_backoff: Duration,
    pub(crate) terminal_retention: Duration,
    pub(crate) terminal_retained_capacity: usize,
    pub(crate) terminal_max_retained_bytes: usize,
}

impl QueryLifecycleRegistryConfig {
    pub(crate) fn from_runtime_config(
        runtime: &novarocks::common::app_config::RuntimeConfig,
    ) -> Self {
        Self {
            max_active_entries: runtime.query_control_max_active_entries,
            tombstone_capacity: runtime.query_control_tombstone_capacity,
            tombstone_retention: Duration::from_millis(
                runtime.query_control_tombstone_retention_ms,
            ),
            heartbeat_timeout: Duration::from_millis(runtime.query_control_heartbeat_timeout_ms),
            pre_start_timeout: Duration::from_millis(runtime.query_control_pre_start_timeout_ms),
            stage_max_fragments: runtime.query_control_stage_max_fragments,
            max_active_staging: runtime.query_control_max_active_staging,
            stage_max_encoded_bytes: runtime.query_control_stage_max_encoded_bytes,
            stage_max_inflight_encoded_bytes: runtime
                .query_control_stage_max_inflight_encoded_bytes,
            stage_max_dormant_workers: runtime.query_control_stage_max_dormant_workers,
            terminal_max_encoded_bytes: runtime.query_control_terminal_max_encoded_bytes,
            terminal_drain_timeout: Duration::from_millis(
                runtime.query_control_terminal_drain_timeout_ms,
            ),
            terminal_ack_timeout: Duration::from_millis(
                runtime.query_control_terminal_ack_timeout_ms,
            ),
            terminal_fallback_rpc_timeout: Duration::from_millis(
                runtime.query_control_terminal_fallback_rpc_timeout_ms,
            ),
            terminal_fallback_max_attempts: runtime.query_control_terminal_fallback_max_attempts,
            terminal_fallback_initial_backoff: Duration::from_millis(
                runtime.query_control_terminal_fallback_initial_backoff_ms,
            ),
            terminal_fallback_max_backoff: Duration::from_millis(
                runtime.query_control_terminal_fallback_max_backoff_ms,
            ),
            terminal_retention: Duration::from_millis(runtime.query_control_terminal_retention_ms),
            terminal_retained_capacity: runtime.query_control_terminal_retained_capacity,
            terminal_max_retained_bytes: runtime.query_control_terminal_max_retained_bytes,
        }
    }
}

/// Global, backend-local accounting for QLC-3 work which exists before a
/// query is allowed to run.  The counters deliberately cover the full
/// pre-start lifetime, not only the RPC handler: a completed Stage still owns
/// decoded plans and dormant workers until Start or Abort wins the lifecycle
/// race.
#[derive(Default)]
struct StageResourceLedger {
    active_builders: usize,
    encoded_bytes: usize,
    dormant_workers: usize,
}

impl StageResourceLedger {
    fn publish_snapshot(active_builders: usize, encoded_bytes: usize, dormant_workers: usize) {
        novarocks::service::publish_backend_query_execution_resource(
            "stage_active_builders",
            active_builders,
        );
        novarocks::service::publish_backend_query_execution_resource(
            "stage_encoded_bytes",
            encoded_bytes,
        );
        novarocks::service::publish_backend_query_execution_resource(
            "stage_dormant_workers",
            dormant_workers,
        );
    }
}

/// RAII reservation for one participant-local Stage bundle.  It first owns a
/// builder slot, then transfers the encoded-byte and dormant-worker portions
/// to the lifecycle entry after a successful commit.  Drop is intentionally
/// sufficient for every failure path, including panics while materializing a
/// fragment bundle.
pub(crate) struct StageResourceReservation {
    ledger: Arc<Mutex<StageResourceLedger>>,
    encoded_bytes: usize,
    dormant_workers: usize,
    builder_active: bool,
}

impl StageResourceReservation {
    fn try_acquire(
        ledger: Arc<Mutex<StageResourceLedger>>,
        config: QueryLifecycleRegistryConfig,
        encoded_bytes: usize,
        dormant_workers: usize,
    ) -> Result<Self, &'static str> {
        let mut state = ledger
            .lock()
            .expect("query lifecycle Stage resource ledger lock");
        if state.active_builders >= config.max_active_staging {
            return Err("backend has reached its active Stage builder limit");
        }
        let Some(next_bytes) = state.encoded_bytes.checked_add(encoded_bytes) else {
            return Err("backend Stage encoded-byte accounting overflowed");
        };
        if next_bytes > config.stage_max_inflight_encoded_bytes {
            return Err("backend has reached its Stage encoded-byte budget");
        }
        let Some(next_workers) = state.dormant_workers.checked_add(dormant_workers) else {
            return Err("backend Stage dormant-worker accounting overflowed");
        };
        if next_workers > config.stage_max_dormant_workers {
            return Err("backend has reached its dormant worker limit");
        }
        state.active_builders += 1;
        state.encoded_bytes = next_bytes;
        state.dormant_workers = next_workers;
        let snapshot = (
            state.active_builders,
            state.encoded_bytes,
            state.dormant_workers,
        );
        drop(state);
        StageResourceLedger::publish_snapshot(snapshot.0, snapshot.1, snapshot.2);
        Ok(Self {
            ledger,
            encoded_bytes,
            dormant_workers,
            builder_active: true,
        })
    }

    fn release_builder(&mut self) {
        if !self.builder_active {
            return;
        }
        let mut state = self
            .ledger
            .lock()
            .expect("query lifecycle Stage resource ledger lock");
        state.active_builders = state.active_builders.saturating_sub(1);
        let snapshot = (
            state.active_builders,
            state.encoded_bytes,
            state.dormant_workers,
        );
        drop(state);
        StageResourceLedger::publish_snapshot(snapshot.0, snapshot.1, snapshot.2);
        self.builder_active = false;
    }
}

impl Drop for StageResourceReservation {
    fn drop(&mut self) {
        let mut state = self
            .ledger
            .lock()
            .expect("query lifecycle Stage resource ledger lock");
        if self.builder_active {
            state.active_builders = state.active_builders.saturating_sub(1);
        }
        state.encoded_bytes = state.encoded_bytes.saturating_sub(self.encoded_bytes);
        state.dormant_workers = state.dormant_workers.saturating_sub(self.dormant_workers);
        let snapshot = (
            state.active_builders,
            state.encoded_bytes,
            state.dormant_workers,
        );
        drop(state);
        StageResourceLedger::publish_snapshot(snapshot.0, snapshot.1, snapshot.2);
    }
}

pub(crate) struct QueryLifecycleRegistry {
    state: Mutex<QueryLifecycleRegistryState>,
    local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
    runtime_filter_factory: Arc<dyn RuntimeFilterParticipantFactory>,
    config: QueryLifecycleRegistryConfig,
    local_backend_id: Mutex<Option<u64>>,
    local_start_epoch: u64,
    clock: Arc<dyn MonotonicClock>,
    metrics: Arc<dyn QueryLifecycleMetricsSink>,
    stage_resources: Arc<Mutex<StageResourceLedger>>,
    terminal_fallback: Arc<dyn QueryTerminalFallbackTransport>,
    self_weak: Weak<QueryLifecycleRegistry>,
}

struct GrpcQueryTerminalFallbackTransport;

impl QueryTerminalFallbackTransport for GrpcQueryTerminalFallbackTransport {
    fn report_query_terminal(
        &self,
        endpoint: &novarocks::query_execution::lifecycle::QueryControlEndpoint,
        snapshot: QueryTerminalSnapshot,
        timeout: Duration,
    ) -> Result<QueryTerminalReportAck, QueryLifecycleTransportError> {
        let client = novarocks::service::grpc_client::NovaRocksGrpcRemoteClient::new_host_port(
            endpoint.host().to_string(),
            endpoint.port(),
        )
        .map_err(|error| {
            QueryLifecycleTransportError::new(QueryLifecycleTransportErrorKind::Unavailable, error)
        })?;
        let response = client
            .blocking_report_query_terminal_with_timeout(
                novarocks::proto::novarocks::ReportQueryTerminalRequest {
                    snapshot: Some(
                        novarocks::query_execution::lifecycle::encode_query_terminal_snapshot(
                            &snapshot,
                        ),
                    ),
                },
                timeout,
            )
            .map_err(|error| {
                QueryLifecycleTransportError::new(
                    QueryLifecycleTransportErrorKind::Unavailable,
                    error,
                )
            })?;
        let outcome = match novarocks::proto::novarocks::ReportQueryTerminalOutcome::try_from(
            response.outcome,
        ) {
            Ok(novarocks::proto::novarocks::ReportQueryTerminalOutcome::Accepted) => {
                QueryTerminalReportOutcome::Accepted
            }
            Ok(novarocks::proto::novarocks::ReportQueryTerminalOutcome::AlreadyAccepted) => {
                QueryTerminalReportOutcome::AlreadyAccepted
            }
            Ok(novarocks::proto::novarocks::ReportQueryTerminalOutcome::RejectedConflict) => {
                QueryTerminalReportOutcome::RejectedConflict
            }
            Ok(novarocks::proto::novarocks::ReportQueryTerminalOutcome::RejectedGone) | Err(_) => {
                QueryTerminalReportOutcome::RejectedGone
            }
            Ok(novarocks::proto::novarocks::ReportQueryTerminalOutcome::Unspecified) => {
                QueryTerminalReportOutcome::RejectedGone
            }
        };
        Ok(QueryTerminalReportAck::new(outcome, response.detail))
    }
}

struct QueryLifecycleRegistryState {
    entries: BTreeMap<QueryExecutionId, Arc<QueryLifecycleEntry>>,
    fragment_executions: BTreeMap<UniqueId, QueryExecutionId>,
    tombstones: VecDeque<QueryExecutionId>,
    active_entries: usize,
    init_conflicts: u64,
    admission_rejected: u64,
    heartbeat_timeouts: u64,
    terminations: u64,
    termination_reasons: [u64; 6],
    pre_init_tombstones: BTreeMap<QueryExecutionId, PreInitTombstone>,
    terminal_retained: BTreeMap<QueryExecutionId, usize>,
    terminal_retained_bytes: usize,
    terminal_facts: u64,
    terminal_locally_drained: u64,
    terminal_records_frozen: u64,
    terminal_acknowledged: u64,
    terminal_retention_expired: u64,
    terminal_fallback_accepted: u64,
    terminal_fallback_rejected: u64,
}

struct PreInitTombstone {
    digest: ParticipantManifestDigest,
    reason: QueryTerminationReason,
    terminated_at: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueryLifecycleRestorationStatus {
    pub(crate) control_ready: usize,
    pub(crate) active_lifecycle: usize,
    pub(crate) fragment_admissions: usize,
    pub(crate) fragment_acceptances: usize,
    pub(crate) lifecycle_entries: usize,
    pub(crate) lifecycle_tombstones: usize,
    pub(crate) pre_init_tombstones: usize,
    pub(crate) tombstone_index: usize,
    pub(crate) restored: bool,
}

impl Default for QueryLifecycleRegistryState {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            fragment_executions: BTreeMap::new(),
            tombstones: VecDeque::new(),
            active_entries: 0,
            init_conflicts: 0,
            admission_rejected: 0,
            heartbeat_timeouts: 0,
            terminations: 0,
            termination_reasons: [0; 6],
            pre_init_tombstones: BTreeMap::new(),
            terminal_retained: BTreeMap::new(),
            terminal_retained_bytes: 0,
            terminal_facts: 0,
            terminal_locally_drained: 0,
            terminal_records_frozen: 0,
            terminal_acknowledged: 0,
            terminal_retention_expired: 0,
            terminal_fallback_accepted: 0,
            terminal_fallback_rejected: 0,
        }
    }
}

struct InitWorkspace {
    registry: Arc<QueryLifecycleRegistry>,
    entry: Arc<QueryLifecycleEntry>,
    execution_id: QueryExecutionId,
    digest: ParticipantManifestDigest,
}

/// Owns the single in-flight Stage build.  Dropping an uncommitted build
/// fail-closes the lifecycle entry and wakes every dormant worker through its
/// shared gate.
pub(crate) struct StageBuildPermit {
    registry: Arc<QueryLifecycleRegistry>,
    entry: Arc<QueryLifecycleEntry>,
    execution_id: QueryExecutionId,
    digest: StageDigest,
    gate: Arc<super::stage::StartGate>,
    resources: Option<StageResourceReservation>,
    committed: bool,
}

pub(crate) enum StageBuildDecision {
    Build(StageBuildPermit),
    Complete(QueryStageAck),
}

pub(crate) struct FragmentAdmissionPermit {
    registry: Weak<QueryLifecycleRegistry>,
    execution_id: QueryExecutionId,
    fragment_instance_id: UniqueId,
    entry: Arc<QueryLifecycleEntry>,
    committed: bool,
}

impl fmt::Debug for FragmentAdmissionPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FragmentAdmissionPermit")
            .field("execution_id", &self.execution_id)
            .field("fragment_instance_id", &self.fragment_instance_id)
            .field("committed", &self.committed)
            .finish()
    }
}

struct RegistryQueryControl {
    registry: Weak<QueryLifecycleRegistry>,
    execution_id: QueryExecutionId,
}

fn fragment_snapshot_from_outcome(
    fragment_instance_id: UniqueId,
    backend_num: i32,
    outcome: &FragmentOutcome,
) -> Result<FragmentTerminalSnapshot, QueryLifecycleError> {
    let outcome = match outcome {
        FragmentOutcome::Succeeded => FragmentTerminalOutcome::Succeeded,
        FragmentOutcome::Failed(error) => FragmentTerminalOutcome::Failed {
            code: "FRAGMENT_EXECUTION_FAILED".to_string(),
            detail: error.to_string(),
        },
        FragmentOutcome::Cancelled { reason } => FragmentTerminalOutcome::Cancelled {
            detail: reason.detail().to_string(),
        },
    };
    FragmentTerminalSnapshot::new(
        fragment_instance_id,
        backend_num,
        outcome,
        SinkCommitReportSnapshot::default(),
        None,
    )
}

impl QueryLifecycleRegistry {
    #[cfg(test)]
    pub(crate) fn hold_registry_state_lock_for_test(
        &self,
        acquired: &std::sync::Barrier,
        release: &std::sync::Barrier,
    ) {
        let _state = self.state.lock().expect("query lifecycle registry lock");
        acquired.wait();
        release.wait();
    }

    #[allow(dead_code)]
    pub(crate) fn new(
        local_backend_id: u64,
        local_start_epoch: u64,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
    ) -> Arc<Self> {
        Self::new_with_clock(
            local_backend_id,
            local_start_epoch,
            local_runtime,
            config,
            Arc::new(SystemMonotonicClock),
        )
    }

    pub(crate) fn new_with_clock(
        local_backend_id: u64,
        local_start_epoch: u64,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
        clock: Arc<dyn MonotonicClock>,
    ) -> Arc<Self> {
        Self::new_with_clock_and_metrics(
            local_backend_id,
            local_start_epoch,
            local_runtime,
            config,
            clock,
            Arc::new(PrometheusQueryLifecycleMetricsSink),
        )
    }

    pub(crate) fn new_with_clock_and_metrics(
        local_backend_id: u64,
        local_start_epoch: u64,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
        clock: Arc<dyn MonotonicClock>,
        metrics: Arc<dyn QueryLifecycleMetricsSink>,
    ) -> Arc<Self> {
        Self::new_with_clock_metrics_and_terminal_fallback(
            local_backend_id,
            local_start_epoch,
            local_runtime,
            config,
            clock,
            metrics,
            Arc::new(GrpcQueryTerminalFallbackTransport),
        )
    }

    pub(crate) fn new_with_clock_metrics_and_terminal_fallback(
        local_backend_id: u64,
        local_start_epoch: u64,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
        clock: Arc<dyn MonotonicClock>,
        metrics: Arc<dyn QueryLifecycleMetricsSink>,
        terminal_fallback: Arc<dyn QueryTerminalFallbackTransport>,
    ) -> Arc<Self> {
        Self::new_with_backend_identity(
            Some(local_backend_id),
            local_start_epoch,
            local_runtime,
            config,
            clock,
            metrics,
            terminal_fallback,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_clock_metrics_terminal_fallback_and_runtime_filter_factory(
        local_backend_id: u64,
        local_start_epoch: u64,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
        clock: Arc<dyn MonotonicClock>,
        metrics: Arc<dyn QueryLifecycleMetricsSink>,
        terminal_fallback: Arc<dyn QueryTerminalFallbackTransport>,
        runtime_filter_factory: Arc<dyn RuntimeFilterParticipantFactory>,
    ) -> Arc<Self> {
        Self::new_with_backend_identity_and_runtime_filter_factory(
            Some(local_backend_id),
            local_start_epoch,
            local_runtime,
            config,
            clock,
            metrics,
            terminal_fallback,
            runtime_filter_factory,
        )
    }

    pub(crate) fn new_unbound(
        local_start_epoch: u64,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
    ) -> Arc<Self> {
        Self::new_with_backend_identity(
            None,
            local_start_epoch,
            local_runtime,
            config,
            Arc::new(SystemMonotonicClock),
            Arc::new(PrometheusQueryLifecycleMetricsSink),
            Arc::new(GrpcQueryTerminalFallbackTransport),
        )
    }

    fn new_with_backend_identity(
        local_backend_id: Option<u64>,
        local_start_epoch: u64,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
        clock: Arc<dyn MonotonicClock>,
        metrics: Arc<dyn QueryLifecycleMetricsSink>,
        terminal_fallback: Arc<dyn QueryTerminalFallbackTransport>,
    ) -> Arc<Self> {
        Self::new_with_backend_identity_and_runtime_filter_factory(
            local_backend_id,
            local_start_epoch,
            local_runtime,
            config,
            clock,
            metrics,
            terminal_fallback,
            Arc::new(BackendRuntimeFilterParticipantFactory),
        )
    }

    fn new_with_backend_identity_and_runtime_filter_factory(
        local_backend_id: Option<u64>,
        local_start_epoch: u64,
        local_runtime: Arc<dyn QueryLifecycleLocalRuntime>,
        config: QueryLifecycleRegistryConfig,
        clock: Arc<dyn MonotonicClock>,
        metrics: Arc<dyn QueryLifecycleMetricsSink>,
        terminal_fallback: Arc<dyn QueryTerminalFallbackTransport>,
        runtime_filter_factory: Arc<dyn RuntimeFilterParticipantFactory>,
    ) -> Arc<Self> {
        assert!(config.max_active_entries > 0);
        assert!(config.tombstone_capacity > 0);
        assert!(!config.tombstone_retention.is_zero());
        assert!(!config.heartbeat_timeout.is_zero());
        assert!(!config.pre_start_timeout.is_zero());
        assert!(config.stage_max_fragments > 0);
        assert!(config.max_active_staging > 0);
        assert!(config.stage_max_encoded_bytes > 0);
        assert!(config.stage_max_inflight_encoded_bytes >= config.stage_max_encoded_bytes);
        assert!(config.stage_max_dormant_workers >= config.stage_max_fragments);
        assert!(!config.terminal_ack_timeout.is_zero());
        assert!(!config.terminal_fallback_rpc_timeout.is_zero());
        assert!(config.terminal_fallback_max_attempts > 0);
        assert!(!config.terminal_retention.is_zero());
        assert!(config.terminal_retained_capacity > 0);
        assert!(config.terminal_max_retained_bytes > 0);
        novarocks::service::publish_backend_query_lifecycle_terminal_limits(
            config.terminal_retained_capacity,
            config.terminal_max_retained_bytes,
        );
        StageResourceLedger::publish_snapshot(0, 0, 0);
        let registry = Arc::new_cyclic(|self_weak| Self {
            state: Mutex::new(QueryLifecycleRegistryState::default()),
            local_runtime,
            runtime_filter_factory,
            config,
            local_backend_id: Mutex::new(local_backend_id),
            local_start_epoch,
            clock,
            metrics,
            stage_resources: Arc::new(Mutex::new(StageResourceLedger::default())),
            terminal_fallback,
            self_weak: self_weak.clone(),
        });
        registry.publish_metrics();
        registry
    }

    fn local_backend_id(&self) -> Option<u64> {
        *self
            .local_backend_id
            .lock()
            .expect("query lifecycle backend identity lock")
    }

    pub(crate) fn bind_backend_identity(&self, backend_id: u64) -> Result<(), QueryLifecycleError> {
        let mut local_backend_id = self
            .local_backend_id
            .lock()
            .expect("query lifecycle backend identity lock");
        match *local_backend_id {
            None => {
                *local_backend_id = Some(backend_id);
                drop(local_backend_id);
                let status = self.restoration_status();
                if query_lifecycle_test_markers_enabled() {
                    eprintln!(
                        "NOVAROCKS_QUERY_LIFECYCLE_RESTORE_STATUS backend_id={} start_epoch={} control_ready={} active_lifecycle={} fragment_admissions={} fragment_acceptances={} lifecycle_entries={} lifecycle_tombstones={} pre_init_tombstones={} tombstone_index={} restored={}",
                        backend_id,
                        self.local_start_epoch,
                        status.control_ready,
                        status.active_lifecycle,
                        status.fragment_admissions,
                        status.fragment_acceptances,
                        status.lifecycle_entries,
                        status.lifecycle_tombstones,
                        status.pre_init_tombstones,
                        status.tombstone_index,
                        status.restored
                    );
                }
                Ok(())
            }
            Some(current) if current == backend_id => Ok(()),
            Some(current) => Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                format!(
                    "backend identity is already bound to {current}; refusing reassignment to {backend_id}"
                ),
            )),
        }
    }

    pub(crate) fn restoration_status(&self) -> QueryLifecycleRestorationStatus {
        let state = self.state.lock().expect("query lifecycle registry lock");
        let mut control_ready = 0;
        let mut fragment_admissions = 0;
        let mut fragment_acceptances = 0;
        let mut lifecycle_tombstones = 0;
        for entry in state.entries.values() {
            let entry_state = entry.state.lock().expect("query lifecycle entry lock");
            control_ready += usize::from(entry_state.phase == QueryLifecyclePhase::ControlAttached);
            fragment_admissions += entry_state.in_flight_fragments.len();
            fragment_acceptances += entry_state.accepted_fragments.len();
            lifecycle_tombstones +=
                usize::from(entry_state.phase == QueryLifecyclePhase::Tombstone);
        }
        fragment_acceptances = fragment_acceptances.max(state.fragment_executions.len());
        let active_lifecycle = state.active_entries;
        let lifecycle_entries = state.entries.len();
        let pre_init_tombstones = state.pre_init_tombstones.len();
        let tombstone_index = state.tombstones.len();
        let restored = control_ready != 0
            || active_lifecycle != 0
            || fragment_admissions != 0
            || fragment_acceptances != 0
            || lifecycle_entries != 0
            || lifecycle_tombstones != 0
            || pre_init_tombstones != 0
            || tombstone_index != 0;
        QueryLifecycleRestorationStatus {
            control_ready,
            active_lifecycle,
            fragment_admissions,
            fragment_acceptances,
            lifecycle_entries,
            lifecycle_tombstones,
            pre_init_tombstones,
            tombstone_index,
            restored,
        }
    }

    pub(crate) fn init_query(&self, request: QueryInitRequest) -> QueryInitAck {
        let execution_id = request.manifest().execution_id();
        let digest = request.digest();
        if request
            .manifest()
            .roles()
            .contains(&ParticipantRole::FragmentExecutor)
            && request
                .manifest()
                .expected_fragment_instance_ids()
                .is_empty()
        {
            let ack = QueryInitAck::new(
                execution_id,
                digest,
                QueryInitOutcome::RejectedInvalidManifest,
            );
            self.log_init(&ack);
            return ack;
        }
        if self.local_backend_id() != Some(request.manifest().backend().backend_id())
            || request.manifest().backend().start_epoch() != self.local_start_epoch
        {
            let ack =
                QueryInitAck::new(execution_id, digest, QueryInitOutcome::RejectedStaleBackend);
            self.log_init(&ack);
            return ack;
        }

        let entry = {
            let mut state = self.state.lock().expect("query lifecycle registry lock");
            self.clean_tombstones_locked(&mut state, self.clock.now(), 64);
            if let Some(tombstone) = state.pre_init_tombstones.get(&execution_id) {
                let outcome = if tombstone.digest == digest {
                    QueryInitOutcome::RejectedTerminated
                } else {
                    state.init_conflicts = state.init_conflicts.saturating_add(1);
                    QueryInitOutcome::RejectedConflict
                };
                let ack = QueryInitAck::new(execution_id, digest, outcome);
                drop(state);
                self.log_init(&ack);
                self.publish_metrics();
                return ack;
            }
            if let Some(entry) = state.entries.get(&execution_id).cloned() {
                if entry.digest != digest {
                    state.init_conflicts = state.init_conflicts.saturating_add(1);
                    let ack =
                        QueryInitAck::new(execution_id, digest, QueryInitOutcome::RejectedConflict);
                    drop(state);
                    self.log_init(&ack);
                    self.publish_metrics();
                    return ack;
                }
                drop(state);
                let ack = self.wait_for_existing_init(entry, execution_id, digest);
                self.log_init(&ack);
                return ack;
            }
            if state.active_entries >= self.config.max_active_entries {
                let ack =
                    QueryInitAck::new(execution_id, digest, QueryInitOutcome::RejectedCapacity);
                drop(state);
                self.log_init(&ack);
                return ack;
            }
            let entry = Arc::new(QueryLifecycleEntry::initializing(
                request.manifest().clone(),
                digest,
            ));
            state.entries.insert(execution_id, Arc::clone(&entry));
            state.active_entries += 1;
            entry
        };
        self.publish_metrics();
        let ack = InitWorkspace {
            registry: self
                .self_weak
                .upgrade()
                .expect("query lifecycle registry is alive during method call"),
            entry,
            execution_id,
            digest,
        }
        .install_and_publish();
        self.log_init(&ack);
        self.publish_metrics();
        ack
    }

    fn wait_for_existing_init(
        &self,
        entry: Arc<QueryLifecycleEntry>,
        execution_id: QueryExecutionId,
        digest: ParticipantManifestDigest,
    ) -> QueryInitAck {
        let mut state = entry.state.lock().expect("query lifecycle entry lock");
        while state.phase == QueryLifecyclePhase::Initializing && state.init_outcome.is_none() {
            state = entry
                .init_completed
                .wait(state)
                .expect("query lifecycle init wait");
        }
        let outcome = match (state.phase, state.init_outcome) {
            (_, Some(outcome)) if outcome != QueryInitOutcome::Applied => outcome,
            (
                QueryLifecyclePhase::Initialized
                | QueryLifecyclePhase::ControlAttached
                | QueryLifecyclePhase::Staging
                | QueryLifecyclePhase::Staged
                | QueryLifecyclePhase::Running,
                _,
            ) => QueryInitOutcome::AlreadyApplied,
            (
                QueryLifecyclePhase::TerminalRetained
                | QueryLifecyclePhase::Terminating
                | QueryLifecyclePhase::Tombstone,
                _,
            ) => QueryInitOutcome::RejectedTerminated,
            (QueryLifecyclePhase::Initializing, _) => state
                .init_outcome
                .unwrap_or(QueryInitOutcome::RejectedInvalidManifest),
        };
        QueryInitAck::new(execution_id, digest, outcome)
    }

    pub(crate) fn abort_query(
        &self,
        request: QueryAbortRequest,
    ) -> Result<QueryTerminationAck, QueryLifecycleError> {
        let execution_id = request.execution_id();
        let entry = {
            let mut state = self.state.lock().expect("query lifecycle registry lock");
            self.clean_tombstones_locked(&mut state, self.clock.now(), 64);
            if let Some(entry) = state.entries.get(&execution_id).cloned() {
                Some(entry)
            } else {
                let reason = state
                    .pre_init_tombstones
                    .get(&execution_id)
                    .map(|tombstone| tombstone.reason)
                    .unwrap_or(QueryTerminationReason::CoordinatorAbort);
                if !state.pre_init_tombstones.contains_key(&execution_id) {
                    state.pre_init_tombstones.insert(
                        execution_id,
                        PreInitTombstone {
                            digest: request.digest(),
                            reason,
                            terminated_at: self.clock.now(),
                        },
                    );
                    state.tombstones.push_back(execution_id);
                    state.terminations = state.terminations.saturating_add(1);
                    state.termination_reasons[termination_reason_index(reason)] = state
                        .termination_reasons[termination_reason_index(reason)]
                    .saturating_add(1);
                    self.enforce_tombstone_capacity_locked(&mut state);
                }
                drop(state);
                info!(
                    target: "novarocks::query_lifecycle",
                    query_id = ?execution_id.query_id(),
                    attempt_id = execution_id.attempt_id().get(),
                    backend_id = ?self.local_backend_id(),
                    start_epoch = self.local_start_epoch,
                    digest = %format_digest(request.digest()),
                    outcome = "terminated",
                    reason = ?reason,
                    "backend query lifecycle terminated before init"
                );
                self.publish_metrics();
                return Ok(QueryTerminationAck::new(execution_id, reason));
            }
        };
        let entry = entry.expect("existing entry");
        if entry.digest != request.digest() {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                "abort digest conflicts with initialized manifest",
            ));
        }
        let reason = self.request_termination_with_detail(
            entry,
            QueryTerminationReason::CoordinatorAbort,
            None,
            request.reason().to_string(),
        );
        Ok(QueryTerminationAck::new(execution_id, reason))
    }

    pub(crate) fn attach_control(
        &self,
        attach: QueryControlAttach,
    ) -> Result<QueryControlAttachment, QueryLifecycleError> {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&attach.execution_id())
            .cloned();
        let Some(entry) = entry else {
            return Err(self.attach_error(
                &attach,
                QueryLifecycleErrorCode::Terminated,
                "query lifecycle entry is not active",
                "missing",
            ));
        };
        if entry.digest != attach.digest() {
            return Err(self.attach_error(
                &attach,
                QueryLifecycleErrorCode::Conflict,
                "query control digest conflicts with initialized manifest",
                "digest_mismatch",
            ));
        }
        let (events_tx, events_rx) = tokio::sync::mpsc::channel(
            CONTROL_EVENT_BUFFER_CAPACITY + RESERVED_CONTROL_EVENT_CAPACITY + 1,
        );
        let (observations_tx, observations_rx) = tokio::sync::watch::channel(None);
        events_tx
            .try_send(QueryControlEvent::ControlReady)
            .map_err(|error| {
                QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Internal,
                    format!("publish ControlReady failed: {error}"),
                )
            })?;
        let local_drained_event_permit =
            events_tx.clone().try_reserve_owned().map_err(|error| {
                QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Internal,
                    format!("reserve LocalDrained control event failed: {error}"),
                )
            })?;
        let terminal_snapshot_event_permit =
            events_tx.clone().try_reserve_owned().map_err(|error| {
                QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Internal,
                    format!("reserve TerminalSnapshot control event failed: {error}"),
                )
            })?;
        let terminal_event_permit = events_tx.clone().try_reserve_owned().map_err(|error| {
            QueryLifecycleError::new(
                QueryLifecycleErrorCode::Internal,
                format!("reserve terminal control event failed: {error}"),
            )
        })?;
        {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            match state.phase {
                QueryLifecyclePhase::Initialized => {}
                QueryLifecyclePhase::TerminalRetained
                | QueryLifecyclePhase::Terminating
                | QueryLifecyclePhase::Tombstone => {
                    let phase = phase_name(state.phase);
                    drop(state);
                    return Err(self.attach_error(
                        &attach,
                        QueryLifecycleErrorCode::Terminated,
                        "query lifecycle entry has terminated",
                        phase,
                    ));
                }
                QueryLifecyclePhase::Initializing
                | QueryLifecyclePhase::ControlAttached
                | QueryLifecyclePhase::Staging
                | QueryLifecyclePhase::Staged
                | QueryLifecyclePhase::Running => {
                    let phase = phase_name(state.phase);
                    drop(state);
                    return Err(self.attach_error(
                        &attach,
                        QueryLifecycleErrorCode::Conflict,
                        "query control can attach only to an initialized entry",
                        phase,
                    ));
                }
            }
            state.phase = QueryLifecyclePhase::ControlAttached;
            state.frontend_owner_epoch = Some(attach.frontend_owner_epoch());
            state.last_heartbeat = Some(self.clock.now());
            state.events = Some(events_tx.clone());
            state.observations = Some(observations_tx);
            state.local_drained_event_permit = Some(local_drained_event_permit);
            state.terminal_snapshot_event_permit = Some(terminal_snapshot_event_permit);
            state.terminal_event_permit = Some(terminal_event_permit);
            if !entry
                .manifest
                .roles()
                .contains(&ParticipantRole::FragmentExecutor)
            {
                state.pre_start_deadline = None;
            }
        }
        info!(
            target: "novarocks::query_lifecycle",
            query_id = ?attach.execution_id().query_id(),
            attempt_id = attach.execution_id().attempt_id().get(),
            backend_id = ?self.local_backend_id(),
            start_epoch = self.local_start_epoch,
            digest = %format_digest(attach.digest()),
            outcome = "control_attached",
            reason = "none",
            "backend query lifecycle control attached"
        );
        if query_lifecycle_test_markers_enabled() {
            eprintln!(
                "NOVAROCKS_QUERY_CONTROL_READY execution_id={} backend_id={} expected_fragments={}",
                format_execution_id(attach.execution_id()),
                self.local_backend_id().unwrap_or_default(),
                entry.manifest.expected_fragment_instance_ids().len()
            );
        }
        self.publish_metrics();
        Ok(QueryControlAttachment {
            control: Arc::new(RegistryQueryControl {
                registry: self.self_weak.clone(),
                execution_id: attach.execution_id(),
            }),
            events: events_rx,
            observations: observations_rx,
        })
    }

    /// Publishes a best-effort, latest-only fragment observation. This path is
    /// intentionally unable to wait on transport I/O or mutate correctness
    /// state: a full/stalled stream may lose observations but must still carry
    /// heartbeat acknowledgements, drain barriers, and terminal facts.
    pub(crate) fn publish_fragment_observation(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
        input_rows: u64,
        output_rows: u64,
        elapsed_ms: u64,
        profile: Option<RuntimeProfileTree>,
    ) -> bool {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned();
        let Some(entry) = entry else {
            return false;
        };
        let (sender, observation) = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            if !matches!(
                state.phase,
                QueryLifecyclePhase::ControlAttached
                    | QueryLifecyclePhase::Staging
                    | QueryLifecyclePhase::Staged
                    | QueryLifecyclePhase::Running
            ) || !entry
                .manifest
                .expected_fragment_instance_ids()
                .contains(&fragment_instance_id)
            {
                return false;
            }
            let sequence = state
                .observation_sequences
                .get(&fragment_instance_id)
                .copied()
                .unwrap_or_default()
                .checked_add(1);
            let Some(sequence) = sequence else {
                return false;
            };
            let Some(sender) = state.observations.clone() else {
                return false;
            };
            let observation = FragmentLiveObservation::new(
                execution_id,
                entry.digest,
                entry.manifest.backend().clone(),
                fragment_instance_id,
                sequence,
                input_rows,
                output_rows,
                elapsed_ms,
                profile,
            )
            .expect("registry-owned fragment observation is structurally valid");
            state
                .observation_sequences
                .insert(fragment_instance_id, sequence);
            (sender, observation)
        };
        sender.send_replace(Some(observation));
        true
    }

    pub(crate) fn stage_fragments(&self, request: QueryStageRequest) -> QueryStageAck {
        match self.begin_stage(request) {
            StageBuildDecision::Build(permit) => permit.commit(),
            StageBuildDecision::Complete(ack) => ack,
        }
    }

    /// Reserves the entry for one complete local Stage build. The caller owns
    /// materialization outside registry locks and must either commit or drop
    /// the returned permit.
    pub(crate) fn begin_stage(&self, request: QueryStageRequest) -> StageBuildDecision {
        let execution_id = request.execution_id();
        let digest_version = request.digest_version();
        let stage_digest = request.digest();
        let fragment_count = request.fragments().len();
        if fragment_count > self.config.stage_max_fragments {
            return StageBuildDecision::Complete(QueryStageAck::new(
                execution_id,
                digest_version,
                stage_digest,
                QueryStageOutcome::RejectedCapacity,
                "stage fragment count exceeds the backend Stage limit",
            ));
        }
        let stage_encoded_bytes =
            novarocks::query_execution::lifecycle::contract::encode_query_stage_request(&request)
                .encoded_len();
        if stage_encoded_bytes > self.config.stage_max_encoded_bytes {
            return StageBuildDecision::Complete(QueryStageAck::new(
                execution_id,
                digest_version,
                stage_digest,
                QueryStageOutcome::RejectedCapacity,
                "stage request encoded bytes exceed the backend Stage limit",
            ));
        }
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned();
        let Some(entry) = entry else {
            return StageBuildDecision::Complete(QueryStageAck::new(
                execution_id,
                digest_version,
                stage_digest,
                QueryStageOutcome::RejectedTerminated,
                "query lifecycle entry is not active",
            ));
        };
        if entry.digest != request.init_digest() {
            return StageBuildDecision::Complete(QueryStageAck::new(
                execution_id,
                digest_version,
                stage_digest,
                QueryStageOutcome::RejectedConflict,
                "stage init digest conflicts with initialized manifest",
            ));
        }

        let requested_instances = request
            .fragments()
            .iter()
            .map(|fragment| fragment.fragment_instance_id())
            .collect::<std::collections::BTreeSet<_>>();
        let expected_instances = entry
            .manifest
            .expected_fragment_instance_ids()
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let mut state = entry.state.lock().expect("query lifecycle entry lock");
        let (outcome, detail, build) = match state.phase {
            QueryLifecyclePhase::ControlAttached => {
                if requested_instances != expected_instances {
                    (
                        QueryStageOutcome::RejectedInvalidBatch,
                        "stage fragment set differs from participant manifest",
                        None,
                    )
                } else {
                    state.phase = QueryLifecyclePhase::Staging;
                    state.stage_digest = Some(stage_digest);
                    let gate = Arc::new(super::stage::StartGate::new());
                    state.start_gate = Some(Arc::clone(&gate));
                    (
                        QueryStageOutcome::Applied,
                        "query participant staging",
                        Some(gate),
                    )
                }
            }
            QueryLifecyclePhase::Staging if state.stage_digest == Some(stage_digest) => {
                while state.phase == QueryLifecyclePhase::Staging
                    && state.termination_reason.is_none()
                {
                    state = entry
                        .stage_completed
                        .wait(state)
                        .expect("query lifecycle entry lock");
                }
                match state.phase {
                    QueryLifecyclePhase::Staged | QueryLifecyclePhase::Running
                        if state.stage_digest == Some(stage_digest) =>
                    {
                        (
                            QueryStageOutcome::AlreadyApplied,
                            "query participant was already staged",
                            None,
                        )
                    }
                    QueryLifecyclePhase::TerminalRetained
                    | QueryLifecyclePhase::Terminating
                    | QueryLifecyclePhase::Tombstone => (
                        QueryStageOutcome::RejectedTerminated,
                        "query lifecycle entry has terminated",
                        None,
                    ),
                    _ => (
                        QueryStageOutcome::RejectedInvalidState,
                        "query participant stage did not complete",
                        None,
                    ),
                }
            }
            QueryLifecyclePhase::Staging => (
                QueryStageOutcome::RejectedConflict,
                "stage digest conflicts with in-flight participant staging",
                None,
            ),
            QueryLifecyclePhase::Staged | QueryLifecyclePhase::Running => {
                if state.stage_digest == Some(stage_digest) {
                    (
                        QueryStageOutcome::AlreadyApplied,
                        "query participant was already staged",
                        None,
                    )
                } else {
                    (
                        QueryStageOutcome::RejectedConflict,
                        "stage digest conflicts with existing staged participant",
                        None,
                    )
                }
            }
            QueryLifecyclePhase::TerminalRetained
            | QueryLifecyclePhase::Terminating
            | QueryLifecyclePhase::Tombstone => (
                QueryStageOutcome::RejectedTerminated,
                "query lifecycle entry has terminated",
                None,
            ),
            QueryLifecyclePhase::Initializing | QueryLifecyclePhase::Initialized => (
                QueryStageOutcome::RejectedInvalidState,
                "query control must attach before staging",
                None,
            ),
        };
        drop(state);
        match build {
            Some(gate) => {
                let resources = match StageResourceReservation::try_acquire(
                    Arc::clone(&self.stage_resources),
                    self.config,
                    stage_encoded_bytes,
                    fragment_count,
                ) {
                    Ok(resources) => resources,
                    Err(detail) => {
                        let mut state = entry.state.lock().expect("query lifecycle entry lock");
                        if state.phase == QueryLifecyclePhase::Staging
                            && state.stage_digest == Some(stage_digest)
                        {
                            state.phase = QueryLifecyclePhase::ControlAttached;
                            state.stage_digest = None;
                            state.start_gate = None;
                            entry.stage_completed.notify_all();
                        }
                        return StageBuildDecision::Complete(QueryStageAck::new(
                            execution_id,
                            digest_version,
                            stage_digest,
                            QueryStageOutcome::RejectedCapacity,
                            detail,
                        ));
                    }
                };
                StageBuildDecision::Build(StageBuildPermit {
                    registry: self
                        .self_weak
                        .upgrade()
                        .expect("query lifecycle registry owns active entry"),
                    entry,
                    execution_id,
                    digest: stage_digest,
                    gate,
                    resources: Some(resources),
                    committed: false,
                })
            }
            None => StageBuildDecision::Complete(QueryStageAck::new(
                execution_id,
                digest_version,
                stage_digest,
                outcome,
                detail,
            )),
        }
    }

    /// Commits the single query-owned start decision.  Releasing the gate
    /// while holding the entry lock makes `Staged -> Running` and visibility to
    /// staged workers one atomic lifecycle event.
    pub(crate) fn start_prepared_query(&self, request: QueryStartRequest) -> QueryStartAck {
        let execution_id = request.execution_id();
        let digest_version = request.digest_version();
        let stage_digest = request.digest();
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned();
        let Some(entry) = entry else {
            return QueryStartAck::new(
                execution_id,
                digest_version,
                stage_digest,
                QueryStartOutcome::RejectedTerminated,
                "query lifecycle entry is not active",
            );
        };

        let mut state = entry.state.lock().expect("query lifecycle entry lock");
        let mut released_stage_resources = None;
        let mut local_drained_event = None;
        let (outcome, detail) = match state.phase {
            QueryLifecyclePhase::Staged => {
                if state.stage_digest != Some(stage_digest) {
                    (
                        QueryStartOutcome::RejectedConflict,
                        "start digest conflicts with staged participant",
                    )
                } else if let Some(gate) = state.start_gate.clone() {
                    state.phase = QueryLifecyclePhase::Running;
                    state.pre_start_deadline = None;
                    if entry.manifest.expected_fragment_instance_ids().is_empty()
                        && !state.local_drained_emitted
                    {
                        state.local_drained_emitted = true;
                        local_drained_event = Some((
                            state.local_drained_event_permit.take(),
                            state.events.clone(),
                        ));
                    }
                    let released = gate.release();
                    debug_assert!(released, "a staged start gate must be pending");
                    released_stage_resources = state.stage_resources.take();
                    (QueryStartOutcome::Applied, "query participant started")
                } else {
                    (
                        QueryStartOutcome::RejectedNotStaged,
                        "staged participant has no start gate",
                    )
                }
            }
            QueryLifecyclePhase::Running => {
                if state.stage_digest == Some(stage_digest) {
                    (
                        QueryStartOutcome::AlreadyStarted,
                        "query participant was already started",
                    )
                } else {
                    (
                        QueryStartOutcome::RejectedConflict,
                        "start digest conflicts with running participant",
                    )
                }
            }
            QueryLifecyclePhase::TerminalRetained
            | QueryLifecyclePhase::Terminating
            | QueryLifecyclePhase::Tombstone => (
                QueryStartOutcome::RejectedTerminated,
                "query lifecycle entry has terminated",
            ),
            QueryLifecyclePhase::Initializing
            | QueryLifecyclePhase::Initialized
            | QueryLifecyclePhase::ControlAttached
            | QueryLifecyclePhase::Staging => (
                QueryStartOutcome::RejectedNotStaged,
                "query participant has not finished staging",
            ),
        };
        drop(state);
        if let Some((permit, events)) = local_drained_event {
            send_reserved_control_event(permit, events, QueryControlEvent::LocalDrained);
        }
        // The gate has been released under the entry lock.  Once Running is
        // visible there can be no dormant workers or retained stage payload,
        // so return the Stage reservation outside lifecycle locks.
        drop(released_stage_resources);
        QueryStartAck::new(execution_id, digest_version, stage_digest, outcome, detail)
    }

    fn attach_error(
        &self,
        attach: &QueryControlAttach,
        code: QueryLifecycleErrorCode,
        detail: &'static str,
        phase: &'static str,
    ) -> QueryLifecycleError {
        warn!(
            target: "novarocks::query_lifecycle",
            query_id = ?attach.execution_id().query_id(),
            attempt_id = attach.execution_id().attempt_id().get(),
            backend_id = ?self.local_backend_id(),
            start_epoch = self.local_start_epoch,
            digest = %format_digest(attach.digest()),
            outcome = "attach_rejected",
            reason = detail,
            phase,
            "backend query lifecycle control attach rejected"
        );
        QueryLifecycleError::new(code, detail)
    }

    pub(crate) fn admit_fragment(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
    ) -> Result<FragmentAdmissionPermit, QueryLifecycleError> {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned();
        let Some(entry) = entry else {
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::Terminated,
                "query is not active",
            ));
        };
        let mut state = entry.state.lock().expect("query lifecycle entry lock");
        if state.termination_reason.is_some()
            || matches!(
                state.phase,
                QueryLifecyclePhase::Terminating | QueryLifecyclePhase::Tombstone
            )
        {
            drop(state);
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::Terminated,
                "query lifecycle has terminated",
            ));
        }
        if !matches!(
            state.phase,
            QueryLifecyclePhase::ControlAttached | QueryLifecyclePhase::Staging
        ) {
            drop(state);
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::Conflict,
                "query control is not ready",
            ));
        }
        if !entry
            .manifest
            .roles()
            .contains(&ParticipantRole::FragmentExecutor)
        {
            drop(state);
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::InvalidManifest,
                "service-only participant cannot admit fragments",
            ));
        }
        if !entry
            .manifest
            .expected_fragment_instance_ids()
            .contains(&fragment_instance_id)
        {
            drop(state);
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::InvalidManifest,
                "fragment instance is outside the participant manifest",
            ));
        }
        if state.accepted_fragments.contains(&fragment_instance_id)
            || !state.in_flight_fragments.insert(fragment_instance_id)
        {
            drop(state);
            return Err(self.admission_error(
                execution_id,
                QueryLifecycleErrorCode::Conflict,
                "fragment instance was already admitted",
            ));
        }
        drop(state);
        Ok(FragmentAdmissionPermit {
            registry: self.self_weak.clone(),
            execution_id,
            fragment_instance_id,
            entry,
            committed: false,
        })
    }

    /// Returns a fragment-bound execution capability from the already
    /// initialized exact attempt. This lookup never creates, revives, or
    /// extends lifecycle retention.
    pub(crate) fn runtime_filter_session_for_fragment(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
        required: bool,
    ) -> Result<Option<RuntimeFilterSessionRef>, QueryLifecycleError> {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned()
            .ok_or_else(|| {
                QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Terminated,
                    "runtime filter execution attempt is not active",
                )
            })?;
        let participant = {
            let state = entry.state.lock().expect("query lifecycle entry lock");
            if !state.in_flight_fragments.contains(&fragment_instance_id) {
                return Err(QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Conflict,
                    "runtime filter session requires a held fragment admission permit",
                ));
            }
            state.runtime_filter.clone()
        };
        match participant {
            Some(participant) => {
                participant.session_for_fragment(execution_id, fragment_instance_id, required)
            }
            None if required => Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::InvalidManifest,
                "fragment requires a runtime filter session but this participant has no runtime filter contribution",
            )),
            None => Ok(None),
        }
    }

    /// Dispatches an already decoded envelope through an existing exact
    /// attempt. A miss is deliberately lookup-only and cannot release a gate.
    pub(crate) fn dispatch_runtime_filter_envelope(
        &self,
        envelope: RuntimeFilterEnvelope,
    ) -> RuntimeFilterIngressResult {
        let participant = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .iter()
            .find(|(execution_id, _)| {
                execution_id.query_id().high() == envelope.query_id().high()
                    && execution_id.query_id().low() == envelope.query_id().low()
                    && execution_id.attempt_id().get() == envelope.deployment_epoch().get()
            })
            .map(|(_, entry)| entry)
            .and_then(|entry| {
                entry
                    .state
                    .lock()
                    .expect("query lifecycle entry lock")
                    .runtime_filter
                    .clone()
            });
        match participant {
            Some(participant) => participant.dispatch_envelope(envelope),
            None => RuntimeFilterIngressResult::rejected(
                "runtime filter ingress rejected [query-unavailable]: runtime filter query is not active or in delivery grace",
            ).expect("query-unavailable reason is non-empty"),
        }
    }

    fn admission_error(
        &self,
        execution_id: QueryExecutionId,
        code: QueryLifecycleErrorCode,
        detail: &'static str,
    ) -> QueryLifecycleError {
        let digest = {
            let mut state = self.state.lock().expect("query lifecycle registry lock");
            state.admission_rejected = state.admission_rejected.saturating_add(1);
            state
                .entries
                .get(&execution_id)
                .map(|entry| format_digest(entry.digest))
                .unwrap_or_else(|| "unknown".to_string())
        };
        warn!(
            target: "novarocks::query_lifecycle",
            query_id = ?execution_id.query_id(),
            attempt_id = execution_id.attempt_id().get(),
            backend_id = ?self.local_backend_id(),
            start_epoch = self.local_start_epoch,
            digest = %digest,
            outcome = "admission_rejected",
            reason = detail,
            "backend query lifecycle fragment admission rejected"
        );
        self.publish_metrics();
        QueryLifecycleError::new(code, detail)
    }

    pub(crate) fn sweep_expired(&self, now: Instant) {
        let entries = {
            let mut state = self.state.lock().expect("query lifecycle registry lock");
            self.clean_tombstones_locked(&mut state, now, 64);
            state.entries.values().cloned().collect::<Vec<_>>()
        };
        for entry in entries {
            let (termination_retry, expiration, terminal_retention_expired) = {
                let state = entry.state.lock().expect("query lifecycle entry lock");
                if state.phase == QueryLifecyclePhase::Terminating {
                    (
                        state.init_outcome.and(state.termination_reason),
                        None,
                        false,
                    )
                } else if state.phase == QueryLifecyclePhase::TerminalRetained {
                    (
                        None,
                        None,
                        state.terminated_at.is_some_and(|at| {
                            now.saturating_duration_since(at) >= self.config.terminal_retention
                        }),
                    )
                } else if state.phase == QueryLifecyclePhase::Tombstone {
                    (None, None, false)
                } else if state
                    .pre_start_deadline
                    .is_some_and(|deadline| now >= deadline)
                {
                    (None, Some(QueryTerminationReason::PreStartTimeout), false)
                } else if matches!(
                    state.phase,
                    QueryLifecyclePhase::ControlAttached
                        | QueryLifecyclePhase::Staging
                        | QueryLifecyclePhase::Staged
                        | QueryLifecyclePhase::Running
                ) && state.last_heartbeat.is_some_and(|heartbeat| {
                    now.saturating_duration_since(heartbeat) >= self.config.heartbeat_timeout
                }) {
                    (
                        None,
                        Some(QueryTerminationReason::CoordinatorHeartbeatTimeout),
                        false,
                    )
                } else {
                    (None, None, false)
                }
            };
            if let Some(reason) = termination_retry {
                let execution_id = entry.manifest.execution_id();
                if self.try_complete_runtime_filter_cleanup(&entry, execution_id) {
                    self.publish_tombstone(&entry, execution_id, reason);
                }
                continue;
            }
            if let Some(reason) = expiration {
                self.request_termination(entry, reason);
                continue;
            }
            if terminal_retention_expired {
                {
                    let mut state = entry.state.lock().expect("query lifecycle entry lock");
                    state.terminal_record = None;
                }
                entry.terminal_delivery_completed.notify_all();
                self.release_terminal_record(entry.manifest.execution_id());
                self.increment_terminal_metric(|metrics| {
                    metrics.terminal_retention_expired =
                        metrics.terminal_retention_expired.saturating_add(1);
                });
                self.publish_tombstone(
                    &entry,
                    entry.manifest.execution_id(),
                    QueryTerminationReason::CoordinatorFinalize,
                );
            }
        }
    }

    fn request_termination(
        &self,
        entry: Arc<QueryLifecycleEntry>,
        requested_reason: QueryTerminationReason,
    ) -> QueryTerminationReason {
        self.request_termination_with_detail(
            entry,
            requested_reason,
            None,
            termination_detail(requested_reason),
        )
    }

    fn request_termination_with_event(
        &self,
        entry: Arc<QueryLifecycleEntry>,
        requested_reason: QueryTerminationReason,
        terminal_event: Option<QueryControlEvent>,
    ) -> QueryTerminationReason {
        let detail = match terminal_event.as_ref() {
            Some(QueryControlEvent::LocalFailure { detail, .. }) => detail.clone(),
            _ => termination_detail(requested_reason),
        };
        self.request_termination_with_detail(entry, requested_reason, terminal_event, detail)
    }

    fn request_termination_with_detail(
        &self,
        entry: Arc<QueryLifecycleEntry>,
        requested_reason: QueryTerminationReason,
        terminal_event: Option<QueryControlEvent>,
        detail: String,
    ) -> QueryTerminationReason {
        let already_terminated = {
            let state = entry.state.lock().expect("query lifecycle entry lock");
            state
                .termination_reason
                .map(|reason| (reason, state.events.clone()))
        };
        if let Some((reason, events)) = already_terminated {
            // A LocalFailure consumes the reserved terminal event permit to
            // publish its cause.  A later coordinator Abort still needs an
            // acknowledgement so FE cleanup can keep the stream alive for
            // the drained immutable snapshot.
            if terminal_event.is_none()
                && let Some(events) = events
            {
                let _ = events.try_send(QueryControlEvent::TerminationAccepted { reason });
            }
            return reason;
        }
        let (
            execution_id,
            expected_instances,
            initializing,
            schedule_failure_drain,
            terminal_event_permit,
            start_gate,
            stage_resources,
        ) = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            // The early check above handles the normal idempotent case. A
            // racing caller can only observe the same first-wins reason.
            if let Some(reason) = state.termination_reason {
                return reason;
            }
            state.termination_reason = Some(requested_reason);
            let initializing = state.phase == QueryLifecyclePhase::Initializing;
            let running = state.phase == QueryLifecyclePhase::Running;
            let has_admitted_fragments = !state.accepted_fragments.is_empty();
            // A termination after Start must retain a complete immutable
            // terminal record, even when the coordinator initiated the
            // abort. Pre-start failures remain QLC-3 cleanup only.
            let schedule_failure_drain = (running || has_admitted_fragments)
                && requested_reason != QueryTerminationReason::CoordinatorFinalize
                && !state.failure_drain_scheduled;
            if schedule_failure_drain {
                state.failure_drain_scheduled = true;
            }
            state.phase = QueryLifecyclePhase::Terminating;
            entry.stage_completed.notify_all();
            (
                entry.manifest.execution_id(),
                entry
                    .manifest
                    .expected_fragment_instance_ids()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>(),
                initializing,
                schedule_failure_drain,
                state.terminal_event_permit.take(),
                state.start_gate.clone(),
                state.stage_resources.take(),
            )
        };

        if let Some(gate) = start_gate {
            // A gate released before termination stays released; otherwise
            // wake every dormant worker without allowing it to start.
            gate.abort();
        }
        // Abort is terminal for a pre-start bundle.  Free the associated
        // ledger reservation only after its gate has been fail-closed.
        drop(stage_resources);
        if let Some(permit) = terminal_event_permit {
            drop(permit.send(
                terminal_event.unwrap_or(QueryControlEvent::TerminationAccepted {
                    reason: requested_reason,
                }),
            ));
        }
        self.publish_metrics();
        self.local_runtime.terminate_query(
            execution_id,
            &expected_instances,
            requested_reason,
            &detail,
        );
        if query_lifecycle_test_markers_enabled() {
            eprintln!(
                "NOVAROCKS_QUERY_LIFECYCLE_TERMINATED execution_id={} backend_id={} reason={requested_reason:?} expected_fragments={}",
                format_execution_id(execution_id),
                self.local_backend_id().unwrap_or_default(),
                expected_instances.len()
            );
        }
        if requested_reason == QueryTerminationReason::CoordinatorHeartbeatTimeout {
            let mut state = self.state.lock().expect("query lifecycle registry lock");
            state.heartbeat_timeouts = state.heartbeat_timeouts.saturating_add(1);
        }
        let cleanup_complete = self.try_complete_runtime_filter_cleanup(&entry, execution_id);
        let failure_drain_pending = entry
            .state
            .lock()
            .expect("query lifecycle entry lock")
            .failure_drain_scheduled;
        if !initializing && cleanup_complete && !failure_drain_pending {
            self.publish_tombstone(&entry, execution_id, requested_reason);
        }
        if schedule_failure_drain {
            self.schedule_failed_terminal_drain(entry);
        }
        requested_reason
    }

    pub(crate) fn record_fragment_terminal(
        &self,
        execution_id: QueryExecutionId,
        fragment_instance_id: UniqueId,
        outcome: &FragmentOutcome,
    ) {
        let snapshot = match fragment_snapshot_from_outcome(
            fragment_instance_id,
            self.local_backend_id().unwrap_or_default() as i32,
            outcome,
        ) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                warn!(target: "novarocks::query_lifecycle", error = %error, "rejecting terminal fragment fact");
                return;
            }
        };
        self.record_fragment_terminal_snapshot(execution_id, snapshot);
    }

    pub(crate) fn record_fragment_terminal_fact(
        &self,
        execution_id: QueryExecutionId,
        fact: FragmentTerminalFact,
        backend_num: i32,
        sink: SinkCommitReportSnapshot,
    ) {
        let snapshot = match FragmentTerminalSnapshot::from_fact(fact, backend_num, sink) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                warn!(target: "novarocks::query_lifecycle", error = %error, "rejecting terminal fragment fact");
                return;
            }
        };
        self.record_fragment_terminal_snapshot(execution_id, snapshot);
    }

    fn record_fragment_terminal_snapshot(
        &self,
        execution_id: QueryExecutionId,
        snapshot: FragmentTerminalSnapshot,
    ) {
        let fragment_instance_id = snapshot.fragment_instance_id();
        let outcome = snapshot.outcome().clone();
        let committed_execution_id = {
            let mut state = self.state.lock().expect("query lifecycle registry lock");
            match state
                .fragment_executions
                .get(&fragment_instance_id)
                .copied()
            {
                Some(committed_execution_id) if committed_execution_id == execution_id => {
                    state.fragment_executions.remove(&fragment_instance_id);
                    Some(committed_execution_id)
                }
                Some(committed_execution_id) => {
                    warn!(
                        target: "novarocks::query_lifecycle",
                        finst_id = %fragment_instance_id,
                        terminal_execution_id = %format_execution_id(execution_id),
                        committed_execution_id = %format_execution_id(committed_execution_id),
                        "ignoring stale fragment terminal fact for a reused fragment instance"
                    );
                    None
                }
                None => {
                    warn!(
                        target: "novarocks::query_lifecycle",
                        finst_id = %fragment_instance_id,
                        terminal_execution_id = %format_execution_id(execution_id),
                        "fragment terminal fact has no committed query lifecycle admission"
                    );
                    None
                }
            }
        };
        let Some(execution_id) = committed_execution_id else {
            return;
        };
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned();
        let Some(entry) = entry else {
            return;
        };
        let local_drained = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            state.completed_fragments.insert(fragment_instance_id);
            if state
                .terminal_facts
                .insert(fragment_instance_id, snapshot)
                .is_some()
            {
                return;
            }
            let expected = entry.manifest.expected_fragment_instance_ids();
            let complete = expected
                .iter()
                .all(|id| state.completed_fragments.contains(id));
            let local_drained = if complete
                && matches!(outcome, FragmentTerminalOutcome::Succeeded)
                && !state.local_drained_emitted
            {
                state.local_drained_emitted = true;
                Some((
                    state.local_drained_event_permit.take(),
                    state.events.clone(),
                ))
            } else {
                None
            };
            local_drained
        };
        if let Some((permit, events)) = local_drained {
            self.increment_terminal_metric(|metrics| {
                metrics.terminal_locally_drained =
                    metrics.terminal_locally_drained.saturating_add(1);
            });
            send_reserved_control_event(permit, events, QueryControlEvent::LocalDrained);
        }
        self.increment_terminal_metric(|metrics| {
            metrics.terminal_facts = metrics.terminal_facts.saturating_add(1);
        });
        if matches!(outcome, FragmentTerminalOutcome::Succeeded) {
            return;
        }
        let (code, detail) = match outcome {
            FragmentTerminalOutcome::Failed { code, detail } => (code, detail),
            FragmentTerminalOutcome::Cancelled { detail } => {
                ("FRAGMENT_CANCELLED".to_string(), detail)
            }
            FragmentTerminalOutcome::IncompleteDrain { detail } => {
                ("INCOMPLETE_DRAIN".to_string(), detail)
            }
            FragmentTerminalOutcome::Succeeded => return,
        };
        self.request_termination_with_event(
            Arc::clone(&entry),
            QueryTerminationReason::LocalFailure,
            Some(QueryControlEvent::LocalFailure { code, detail }),
        );
    }

    fn schedule_failed_terminal_drain(&self, entry: Arc<QueryLifecycleEntry>) {
        let weak = self.self_weak.clone();
        let timeout = self.config.terminal_drain_timeout;
        std::thread::Builder::new()
            .name("query-terminal-failure-drain".to_string())
            .spawn(move || {
                let deadline = Instant::now()
                    .checked_add(timeout)
                    .unwrap_or_else(Instant::now);
                loop {
                    let complete = {
                        let state = entry.state.lock().expect("query lifecycle entry lock");
                        entry
                            .manifest
                            .expected_fragment_instance_ids()
                            .iter()
                            .all(|id| state.terminal_facts.contains_key(id))
                    };
                    if complete || Instant::now() >= deadline {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                if let Some(registry) = weak.upgrade() {
                    registry.freeze_failed_terminal_snapshot(entry, timeout);
                }
            })
            .expect("spawn failed query terminal drain");
    }

    fn freeze_failed_terminal_snapshot(&self, entry: Arc<QueryLifecycleEntry>, timeout: Duration) {
        let execution_id = entry.manifest.execution_id();
        let facts = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            if state.terminal_record.is_some() || !state.failure_drain_scheduled {
                return;
            }
            let backend_num =
                i32::try_from(self.local_backend_id().unwrap_or_default()).unwrap_or(i32::MAX);
            for fragment_instance_id in entry.manifest.expected_fragment_instance_ids() {
                if !state.terminal_facts.contains_key(fragment_instance_id) {
                    let detail = format!(
                        "fragment terminal fact was not observed within {}ms after local failure",
                        timeout.as_millis()
                    );
                    let snapshot = match FragmentTerminalSnapshot::new(
                        *fragment_instance_id,
                        backend_num,
                        FragmentTerminalOutcome::IncompleteDrain { detail },
                        SinkCommitReportSnapshot::default(),
                        None,
                    ) {
                        Ok(snapshot) => snapshot,
                        Err(error) => {
                            warn!(target: "novarocks::query_lifecycle", error = %error, "failed to synthesize incomplete terminal fact");
                            return;
                        }
                    };
                    state.terminal_facts.insert(*fragment_instance_id, snapshot);
                }
            }
            state.terminal_facts.values().cloned().collect::<Vec<_>>()
        };
        // The entry lock only freezes terminal facts. Canonical encoding and
        // digest construction can be expensive and must not block control,
        // fragment completion, or ACK handling.
        let snapshot = match QueryTerminalSnapshot::new(
            execution_id,
            entry.manifest.backend().clone(),
            entry.digest,
            facts,
        ) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                warn!(target: "novarocks::query_lifecycle", error = %error, "failed to build failed query terminal snapshot");
                return;
            }
        };
        let record = match ImmutableQueryTerminalRecord::new(
            snapshot,
            self.config.terminal_max_encoded_bytes,
        ) {
            Ok(record) => record,
            Err(error) => {
                warn!(target: "novarocks::query_lifecycle", error = %error, "failed to encode failed query terminal snapshot");
                return;
            }
        };
        if let Err(error) = self.reserve_terminal_record(execution_id, record.encoded_len()) {
            warn!(target: "novarocks::query_lifecycle", error = %error, "failed to retain failed query terminal snapshot");
            return;
        }
        let terminal_delivery = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            if state.terminal_record.is_some() || !state.failure_drain_scheduled {
                self.release_terminal_record(execution_id);
                return;
            }
            state.terminal_record = Some(record.clone());
            state.phase = QueryLifecyclePhase::TerminalRetained;
            state.terminated_at = Some(self.clock.now());
            (
                state.terminal_snapshot_event_permit.take(),
                state.events.clone(),
            )
        };
        let _ = self.try_complete_runtime_filter_cleanup(&entry, execution_id);
        self.emit_terminal_retained_marker(record.snapshot(), record.encoded_len());
        send_reserved_control_event(
            terminal_delivery.0,
            terminal_delivery.1,
            QueryControlEvent::TerminalSnapshot {
                snapshot: record.snapshot().clone(),
            },
        );
        self.schedule_terminal_fallback(entry, record.snapshot().clone());
        self.increment_terminal_metric(|metrics| {
            metrics.terminal_records_frozen = metrics.terminal_records_frozen.saturating_add(1);
        });
    }

    fn finalize_from_control(
        &self,
        execution_id: QueryExecutionId,
    ) -> Result<(), QueryLifecycleError> {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned()
            .ok_or_else(|| {
                QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Terminated,
                    "query lifecycle entry is not active",
                )
            })?;
        let (backend, facts, expected) = {
            let state = entry.state.lock().expect("query lifecycle entry lock");
            if state.phase != QueryLifecyclePhase::Running || !state.local_drained_emitted {
                return Err(QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Terminated,
                    "Finalize requires locally drained participant",
                ));
            }
            let expected = entry.manifest.expected_fragment_instance_ids();
            if expected.len() != state.terminal_facts.len() {
                return Err(QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Internal,
                    "locally drained participant is missing terminal facts",
                ));
            }
            let backend = entry.manifest.backend().clone();
            (
                backend,
                state.terminal_facts.values().cloned().collect::<Vec<_>>(),
                expected.iter().copied().collect::<Vec<_>>(),
            )
        };
        // Finish the immutable record outside the lifecycle entry lock. The
        // local-drained gate makes the cloned fact set stable.
        let snapshot = QueryTerminalSnapshot::new(execution_id, backend, entry.digest, facts)?;
        let record =
            ImmutableQueryTerminalRecord::new(snapshot, self.config.terminal_max_encoded_bytes)?;
        self.reserve_terminal_record(execution_id, record.encoded_len())?;
        let terminal_delivery = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            if state.phase != QueryLifecyclePhase::Running || state.terminal_record.is_some() {
                self.release_terminal_record(execution_id);
                return Err(QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Terminated,
                    "query lifecycle changed while terminal record was being reserved",
                ));
            }
            state.terminal_record = Some(record.clone());
            state.phase = QueryLifecyclePhase::TerminalRetained;
            state.terminated_at = Some(self.clock.now());
            (
                state.terminal_snapshot_event_permit.take(),
                state.events.clone(),
            )
        };
        // All execution-owned resources are detached before the immutable record is delivered.
        self.local_runtime.terminate_query(
            execution_id,
            &expected,
            QueryTerminationReason::CoordinatorFinalize,
            "query finalized after local drain",
        );
        let _ = self.try_complete_runtime_filter_cleanup(&entry, execution_id);
        self.emit_terminal_retained_marker(record.snapshot(), record.encoded_len());
        send_reserved_control_event(
            terminal_delivery.0,
            terminal_delivery.1.clone(),
            QueryControlEvent::TerminalSnapshot {
                snapshot: record.snapshot().clone(),
            },
        );
        if let Some(events) = terminal_delivery.1 {
            // Retain the QLC-3 acknowledgement as a compatibility latch. The
            // immutable snapshot above is the terminal payload; FE v4 stores
            // it before acknowledging the retained record.
            let _ = events.try_send(QueryControlEvent::TerminationAccepted {
                reason: QueryTerminationReason::CoordinatorFinalize,
            });
        }
        self.schedule_terminal_fallback(entry, record.snapshot().clone());
        self.increment_terminal_metric(|metrics| {
            metrics.terminal_records_frozen = metrics.terminal_records_frozen.saturating_add(1);
        });
        Ok(())
    }

    fn reserve_terminal_record(
        &self,
        execution_id: QueryExecutionId,
        bytes: usize,
    ) -> Result<(), QueryLifecycleError> {
        let mut state = self.state.lock().expect("query lifecycle registry lock");
        if state.terminal_retained.len() >= self.config.terminal_retained_capacity {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Capacity,
                "query terminal retained-record capacity is exhausted",
            ));
        }
        if state.terminal_retained_bytes.saturating_add(bytes)
            > self.config.terminal_max_retained_bytes
        {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Capacity,
                "query terminal retained-byte capacity is exhausted",
            ));
        }
        state.terminal_retained.insert(execution_id, bytes);
        state.terminal_retained_bytes = state.terminal_retained_bytes.saturating_add(bytes);
        Ok(())
    }

    fn release_terminal_record(&self, execution_id: QueryExecutionId) {
        let mut state = self.state.lock().expect("query lifecycle registry lock");
        if let Some(bytes) = state.terminal_retained.remove(&execution_id) {
            state.terminal_retained_bytes = state.terminal_retained_bytes.saturating_sub(bytes);
        }
    }

    fn emit_terminal_retained_marker(&self, snapshot: &QueryTerminalSnapshot, bytes: usize) {
        if query_lifecycle_test_markers_enabled() {
            eprintln!(
                "NOVAROCKS_QUERY_TERMINAL_RETAINED execution_id={} backend_id={} start_epoch={} digest={:?} bytes={}",
                format_execution_id(snapshot.execution_id()),
                self.local_backend_id().unwrap_or_default(),
                self.local_start_epoch,
                snapshot.digest(),
                bytes,
            );
        }
    }

    fn schedule_terminal_fallback(
        &self,
        entry: Arc<QueryLifecycleEntry>,
        snapshot: QueryTerminalSnapshot,
    ) {
        let endpoint = entry.manifest.report_endpoint().clone();
        let weak = self.self_weak.clone();
        let transport = Arc::clone(&self.terminal_fallback);
        let config = self.config;
        std::thread::Builder::new()
            .name("query-terminal-fallback".to_string())
            .spawn(move || {
                let retained = entry
                    .terminal_delivery_completed
                    .wait_timeout_while(
                        entry.state.lock().expect("query lifecycle entry lock"),
                        config.terminal_ack_timeout,
                        |state| {
                            state.terminal_record.as_ref().is_some_and(|record| {
                                record.snapshot().digest() == snapshot.digest()
                            })
                        },
                    )
                    .expect("query lifecycle terminal fallback wait")
                    .0
                    .terminal_record
                    .as_ref()
                    .is_some_and(|record| record.snapshot().digest() == snapshot.digest());
                if !retained {
                    return;
                }
                let mut backoff = config.terminal_fallback_initial_backoff;
                for attempt in 0..config.terminal_fallback_max_attempts {
                    let Some(registry) = weak.upgrade() else {
                        return;
                    };
                    let retained = entry
                        .state
                        .lock()
                        .expect("query lifecycle entry lock")
                        .terminal_record
                        .as_ref()
                        .is_some_and(|record| record.snapshot().digest() == snapshot.digest());
                    if !retained {
                        return;
                    }
                    match transport.report_query_terminal(
                        &endpoint,
                        snapshot.clone(),
                        config.terminal_fallback_rpc_timeout,
                    ) {
                        Ok(ack)
                            if matches!(
                                ack.outcome(),
                                QueryTerminalReportOutcome::Accepted
                                    | QueryTerminalReportOutcome::AlreadyAccepted
                            ) =>
                        {
                            registry.increment_terminal_metric(|metrics| {
                                metrics.terminal_fallback_accepted = metrics
                                    .terminal_fallback_accepted
                                    .saturating_add(1);
                            });
                            if query_lifecycle_test_markers_enabled() {
                                eprintln!(
                                    "NOVAROCKS_QUERY_TERMINAL_FALLBACK_ACCEPTED execution_id={} backend_id={} attempt={} outcome={:?}",
                                    format_execution_id(snapshot.execution_id()),
                                    registry.local_backend_id().unwrap_or_default(),
                                    attempt + 1,
                                    ack.outcome(),
                                );
                            }
                            let _ = registry.terminal_ack_from_control(
                                QueryTerminalAck::from_snapshot(&snapshot),
                            );
                            return;
                        }
                        Ok(ack) => {
                            registry.increment_terminal_metric(|metrics| {
                                metrics.terminal_fallback_rejected = metrics
                                    .terminal_fallback_rejected
                                    .saturating_add(1);
                            });
                            if query_lifecycle_test_markers_enabled() {
                                eprintln!(
                                    "NOVAROCKS_QUERY_TERMINAL_FALLBACK_RETRY execution_id={} backend_id={} attempt={} outcome={:?} detail={}",
                                    format_execution_id(snapshot.execution_id()),
                                    registry.local_backend_id().unwrap_or_default(),
                                    attempt + 1,
                                    ack.outcome(),
                                    ack.detail(),
                                );
                            }
                            warn!(
                                target: "novarocks::query_lifecycle",
                                attempt,
                                outcome = ?ack.outcome(),
                                detail = %ack.detail(),
                                "query terminal fallback was rejected"
                            );
                            if ack.outcome() == QueryTerminalReportOutcome::RejectedConflict {
                                registry.discard_terminal_record(
                                    &entry,
                                    snapshot.execution_id(),
                                    snapshot.digest(),
                                );
                                return;
                            }
                        }
                        Err(error) => {
                            registry.increment_terminal_metric(|metrics| {
                                metrics.terminal_fallback_rejected = metrics
                                    .terminal_fallback_rejected
                                    .saturating_add(1);
                            });
                            if query_lifecycle_test_markers_enabled() {
                                eprintln!(
                                    "NOVAROCKS_QUERY_TERMINAL_FALLBACK_RETRY execution_id={} backend_id={} attempt={} transport_error={}",
                                    format_execution_id(snapshot.execution_id()),
                                    registry.local_backend_id().unwrap_or_default(),
                                    attempt + 1,
                                    error,
                                );
                            }
                            warn!(
                                target: "novarocks::query_lifecycle",
                                attempt,
                                error = %error,
                                "query terminal fallback delivery failed"
                            );
                        }
                    }
                    if attempt + 1 < config.terminal_fallback_max_attempts {
                        std::thread::sleep(backoff);
                        backoff = backoff
                            .checked_mul(2)
                            .unwrap_or(config.terminal_fallback_max_backoff)
                            .min(config.terminal_fallback_max_backoff);
                    }
                }
            })
            .expect("spawn query terminal fallback delivery");
    }

    fn terminal_ack_from_control(&self, ack: QueryTerminalAck) -> Result<(), QueryLifecycleError> {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&ack.execution_id())
            .cloned()
            .ok_or_else(|| {
                QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Terminated,
                    "query terminal record is gone",
                )
            })?;
        let mut state = entry.state.lock().expect("query lifecycle entry lock");
        let record = state.terminal_record.as_ref().ok_or_else(|| {
            QueryLifecycleError::new(
                QueryLifecycleErrorCode::Terminated,
                "query terminal record is not retained",
            )
        })?;
        let snapshot = record.snapshot();
        if ack.init_digest() != snapshot.init_digest()
            || ack.version() != snapshot.version()
            || ack.digest() != snapshot.digest()
        {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                "query terminal ACK identity conflicts with retained snapshot",
            ));
        }
        if query_lifecycle_test_markers_enabled() {
            let query_id = ack.execution_id().query_id();
            eprintln!(
                "NOVAROCKS_QUERY_TERMINAL_ACK query_hi={} query_lo={} attempt={} backend_id={}",
                query_id.high(),
                query_id.low(),
                ack.execution_id().attempt_id().get(),
                self.local_backend_id().unwrap_or_default(),
            );
        }
        state.terminal_record = None;
        drop(state);
        entry.terminal_delivery_completed.notify_all();
        self.increment_terminal_metric(|metrics| {
            metrics.terminal_acknowledged = metrics.terminal_acknowledged.saturating_add(1);
        });
        self.release_terminal_record(ack.execution_id());
        self.publish_tombstone(
            &entry,
            ack.execution_id(),
            QueryTerminationReason::CoordinatorFinalize,
        );
        Ok(())
    }

    /// A conflict is a terminal answer from a live FE: retrying the immutable
    /// snapshot cannot change the rejected identity. Drop only this bounded
    /// delivery record; execution resources were detached before it existed.
    fn discard_terminal_record(
        &self,
        entry: &Arc<QueryLifecycleEntry>,
        execution_id: QueryExecutionId,
        digest: novarocks::query_execution::lifecycle::QueryTerminalSnapshotDigest,
    ) {
        let reason = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            let retained = state
                .terminal_record
                .as_ref()
                .is_some_and(|record| record.snapshot().digest() == digest);
            if !retained {
                return;
            }
            state.terminal_record = None;
            state
                .termination_reason
                .unwrap_or(QueryTerminationReason::CoordinatorFinalize)
        };
        entry.terminal_delivery_completed.notify_all();
        self.release_terminal_record(execution_id);
        self.publish_tombstone(entry, execution_id, reason);
    }

    fn try_complete_runtime_filter_cleanup(
        &self,
        entry: &Arc<QueryLifecycleEntry>,
        _execution_id: QueryExecutionId,
    ) -> bool {
        let participant = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            if state.runtime_filter_close_in_flight {
                return false;
            }
            if state.runtime_filter.is_none() {
                return true;
            }
            state.runtime_filter_close_in_flight = true;
            state.runtime_filter.take()
        };
        let participant = participant.expect("runtime-filter participant was checked present");
        let reason = entry
            .state
            .lock()
            .expect("query lifecycle entry lock")
            .termination_reason
            .unwrap_or(QueryTerminationReason::LocalFailure);
        // Service close has its own first-wins quiescence barrier. Keep it out
        // of the entry lock so inbound ingress and terminal callers cannot
        // deadlock each other around the participant owner.
        let close_result = participant.close(reason);
        let mut state = entry.state.lock().expect("query lifecycle entry lock");
        state.runtime_filter_close_in_flight = false;
        match close_result {
            Ok(()) => true,
            Err(_) => {
                // Preserve the same attempt-local owner for a later terminal
                // sweep. A failed close must not release active capacity or
                // publish a tombstone while the Service can still be live.
                state.runtime_filter = Some(participant);
                false
            }
        }
    }

    fn publish_tombstone(
        &self,
        entry: &Arc<QueryLifecycleEntry>,
        execution_id: QueryExecutionId,
        reason: QueryTerminationReason,
    ) {
        {
            let mut entry_state = entry.state.lock().expect("query lifecycle entry lock");
            if entry_state.phase == QueryLifecyclePhase::Tombstone {
                return;
            }
            if entry_state.runtime_filter.is_some() || entry_state.runtime_filter_close_in_flight {
                return;
            }
            entry_state.phase = QueryLifecyclePhase::Tombstone;
            entry_state.termination_reason.get_or_insert(reason);
            entry_state.terminated_at = Some(self.clock.now());
            entry_state
                .init_outcome
                .get_or_insert(QueryInitOutcome::RejectedTerminated);
            entry.init_completed.notify_all();
        }
        let mut state = self.state.lock().expect("query lifecycle registry lock");
        state.active_entries = state.active_entries.saturating_sub(1);
        state.tombstones.push_back(execution_id);
        state.terminations = state.terminations.saturating_add(1);
        state.termination_reasons[termination_reason_index(reason)] =
            state.termination_reasons[termination_reason_index(reason)].saturating_add(1);
        self.clean_tombstones_locked(&mut state, self.clock.now(), 64);
        self.enforce_tombstone_capacity_locked(&mut state);
        drop(state);
        info!(
            target: "novarocks::query_lifecycle",
            query_id = ?execution_id.query_id(),
            attempt_id = execution_id.attempt_id().get(),
            backend_id = ?self.local_backend_id(),
            start_epoch = self.local_start_epoch,
            digest = %format_digest(entry.digest),
            outcome = "terminated",
            reason = ?reason,
            "backend query lifecycle terminated"
        );
        self.publish_metrics();
        if query_lifecycle_test_markers_enabled() {
            eprintln!(
                "NOVAROCKS_QUERY_LIFECYCLE_CLEANUP execution_id={} backend_id={} active=false tombstone=true reason={reason:?}",
                format_execution_id(execution_id),
                self.local_backend_id().unwrap_or_default()
            );
        }
    }

    fn clean_tombstones_locked(
        &self,
        state: &mut QueryLifecycleRegistryState,
        now: Instant,
        limit: usize,
    ) {
        let mut removed = 0;
        while removed < limit {
            let Some(execution_id) = state.tombstones.front().copied() else {
                break;
            };
            let terminated_at = state
                .pre_init_tombstones
                .get(&execution_id)
                .map(|tombstone| tombstone.terminated_at)
                .or_else(|| {
                    state.entries.get(&execution_id).and_then(|entry| {
                        entry
                            .state
                            .lock()
                            .expect("query lifecycle entry lock")
                            .terminated_at
                    })
                });
            if !terminated_at.is_some_and(|at| {
                now.saturating_duration_since(at) >= self.config.tombstone_retention
            }) {
                break;
            }
            state.tombstones.pop_front();
            Self::evict_tombstone_execution_locked(state, execution_id);
            removed += 1;
        }
    }

    fn enforce_tombstone_capacity_locked(&self, state: &mut QueryLifecycleRegistryState) {
        while state.tombstones.len() > self.config.tombstone_capacity {
            let execution_id = state
                .tombstones
                .pop_front()
                .expect("tombstone length checked");
            Self::evict_tombstone_execution_locked(state, execution_id);
        }
    }

    fn evict_tombstone_execution_locked(
        state: &mut QueryLifecycleRegistryState,
        execution_id: QueryExecutionId,
    ) {
        state.pre_init_tombstones.remove(&execution_id);
        if state.entries.get(&execution_id).is_some_and(|entry| {
            entry
                .state
                .lock()
                .expect("query lifecycle entry lock")
                .phase
                == QueryLifecyclePhase::Tombstone
        }) {
            state.entries.remove(&execution_id);
        }
        state
            .fragment_executions
            .retain(|_, mapped_execution_id| *mapped_execution_id != execution_id);
    }

    fn heartbeat(
        &self,
        execution_id: QueryExecutionId,
        sequence: u64,
    ) -> Result<(), QueryLifecycleError> {
        let entry = self.active_entry(execution_id)?;
        let events = {
            let mut state = entry.state.lock().expect("query lifecycle entry lock");
            if !matches!(
                state.phase,
                QueryLifecyclePhase::ControlAttached
                    | QueryLifecyclePhase::Staging
                    | QueryLifecyclePhase::Staged
                    | QueryLifecyclePhase::Running
            ) || state.termination_reason.is_some()
            {
                return Err(QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Terminated,
                    "query control is not active",
                ));
            }
            state.last_heartbeat = Some(self.clock.now());
            state.events.clone()
        };
        if let Some(events) = events {
            events
                .try_send(QueryControlEvent::HeartbeatAck { sequence })
                .map_err(|error| {
                    QueryLifecycleError::new(
                        QueryLifecycleErrorCode::Internal,
                        format!("publish heartbeat ack failed: {error}"),
                    )
                })?;
        }
        Ok(())
    }

    fn terminate_from_control(
        &self,
        execution_id: QueryExecutionId,
        reason: QueryTerminationReason,
    ) -> Result<(), QueryLifecycleError> {
        let entry = self.active_entry(execution_id)?;
        let repeated = entry
            .state
            .lock()
            .expect("query lifecycle entry lock")
            .termination_reason
            .is_some();
        if matches!(
            reason,
            QueryTerminationReason::CoordinatorStreamLost
                | QueryTerminationReason::CoordinatorHeartbeatTimeout
        ) {
            warn!(
                target: "novarocks::query_lifecycle",
                query_id = ?execution_id.query_id(),
                attempt_id = execution_id.attempt_id().get(),
                backend_id = ?self.local_backend_id(),
                start_epoch = self.local_start_epoch,
                digest = %format_digest(entry.digest),
                outcome = "coordinator_lost",
                reason = ?reason,
                "backend query lifecycle coordinator lost"
            );
        }
        let accepted = self.request_termination(Arc::clone(&entry), reason);
        if repeated {
            let events = entry
                .state
                .lock()
                .expect("query lifecycle entry lock")
                .events
                .clone();
            if let Some(events) = events {
                let _ =
                    events.try_send(QueryControlEvent::TerminationAccepted { reason: accepted });
            }
        }
        Ok(())
    }

    fn active_entry(
        &self,
        execution_id: QueryExecutionId,
    ) -> Result<Arc<QueryLifecycleEntry>, QueryLifecycleError> {
        self.state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned()
            .ok_or_else(|| {
                QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Terminated,
                    "query lifecycle entry is not active",
                )
            })
    }

    #[cfg(test)]
    pub(crate) fn phase(&self, execution_id: QueryExecutionId) -> Option<QueryLifecyclePhase> {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned()?;
        let phase = entry
            .state
            .lock()
            .expect("query lifecycle entry lock")
            .phase;
        Some(phase)
    }

    #[cfg(test)]
    pub(crate) fn was_ever_initialized(&self, execution_id: QueryExecutionId) -> bool {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned();
        entry.is_some_and(|entry| {
            entry
                .state
                .lock()
                .expect("query lifecycle entry lock")
                .ever_initialized
        })
    }

    #[cfg(test)]
    pub(crate) fn termination_reason(
        &self,
        execution_id: QueryExecutionId,
    ) -> Option<QueryTerminationReason> {
        let entry = self
            .state
            .lock()
            .expect("query lifecycle registry lock")
            .entries
            .get(&execution_id)
            .cloned()?;
        let reason = entry
            .state
            .lock()
            .expect("query lifecycle entry lock")
            .termination_reason;
        reason
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, execution_id: QueryExecutionId) -> bool {
        let state = self.state.lock().expect("query lifecycle registry lock");
        state.entries.contains_key(&execution_id)
            || state.pre_init_tombstones.contains_key(&execution_id)
    }

    #[cfg(test)]
    pub(crate) fn metrics_snapshot(&self) -> BackendQueryLifecycleMetricsSnapshot {
        let state = self.state.lock().expect("query lifecycle registry lock");
        fold_metrics_locked(&state).0
    }

    fn publish_metrics(&self) {
        let (snapshot, termination_reasons, runtime_filter_services) = {
            let state = self.state.lock().expect("query lifecycle registry lock");
            let (snapshot, termination_reasons) = fold_metrics_locked(&state);
            let runtime_filter_services = state
                .entries
                .values()
                .filter(|entry| {
                    entry
                        .state
                        .lock()
                        .expect("query lifecycle entry lock")
                        .runtime_filter
                        .is_some()
                })
                .count();
            (snapshot, termination_reasons, runtime_filter_services)
        };
        self.metrics.publish(snapshot, termination_reasons);
        novarocks::service::publish_backend_query_execution_resource(
            "native_runtime_filter_services",
            runtime_filter_services,
        );
    }

    fn increment_terminal_metric(&self, update: impl FnOnce(&mut QueryLifecycleRegistryState)) {
        {
            let mut state = self.state.lock().expect("query lifecycle registry lock");
            update(&mut state);
        }
        self.publish_metrics();
    }

    fn log_init(&self, ack: &QueryInitAck) {
        info!(
            target: "novarocks::query_lifecycle",
            query_id = ?ack.execution_id().query_id(),
            attempt_id = ack.execution_id().attempt_id().get(),
            backend_id = ?self.local_backend_id(),
            start_epoch = self.local_start_epoch,
            digest = %format_digest(ack.digest()),
            outcome = ?ack.outcome(),
            reason = "none",
            "backend query lifecycle init"
        );
        if query_lifecycle_test_markers_enabled()
            && matches!(
                ack.outcome(),
                QueryInitOutcome::Applied | QueryInitOutcome::AlreadyApplied
            )
        {
            let expected_fragments = self
                .state
                .lock()
                .expect("query lifecycle registry lock")
                .entries
                .get(&ack.execution_id())
                .map(|entry| entry.manifest.expected_fragment_instance_ids().len())
                .unwrap_or_default();
            let marker = if ack.outcome() == QueryInitOutcome::Applied {
                "NOVAROCKS_QUERY_INIT_APPLIED"
            } else {
                "NOVAROCKS_QUERY_INIT_IDEMPOTENT"
            };
            eprintln!(
                "{marker} execution_id={} backend_id={} expected_fragments={expected_fragments}",
                format_execution_id(ack.execution_id()),
                self.local_backend_id().unwrap_or_default()
            );
        }
    }
}

impl InitWorkspace {
    fn install_and_publish(self) -> QueryInitAck {
        let contribution = self.entry.manifest.runtime_filter().cloned();
        let install_result = contribution.map_or(Ok(None), |contribution| {
            self.registry
                .runtime_filter_factory
                .install(self.execution_id, contribution)
                .map(Some)
        });
        if install_result.is_err() {
            let (reason, terminate_locally) = {
                let mut state = self.entry.state.lock().expect("query lifecycle entry lock");
                state.init_outcome = Some(QueryInitOutcome::RejectedInvalidManifest);
                let terminate_locally = state.termination_reason.is_none();
                let reason = *state
                    .termination_reason
                    .get_or_insert(QueryTerminationReason::LocalFailure);
                state.phase = QueryLifecyclePhase::Terminating;
                self.entry.init_completed.notify_all();
                (reason, terminate_locally)
            };
            if terminate_locally {
                let expected_instances = self
                    .entry
                    .manifest
                    .expected_fragment_instance_ids()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>();
                self.registry.local_runtime.terminate_query(
                    self.execution_id,
                    &expected_instances,
                    reason,
                    &termination_detail(reason),
                );
            }
            if self
                .registry
                .try_complete_runtime_filter_cleanup(&self.entry, self.execution_id)
            {
                self.registry
                    .publish_tombstone(&self.entry, self.execution_id, reason);
            }
            return QueryInitAck::new(
                self.execution_id,
                self.digest,
                QueryInitOutcome::RejectedInvalidManifest,
            );
        }

        let participant = install_result.expect("runtime-filter install result was checked");
        let terminated = {
            let mut state = self.entry.state.lock().expect("query lifecycle entry lock");
            if state.termination_reason.is_some() {
                state.runtime_filter = participant;
                state.init_outcome = Some(QueryInitOutcome::RejectedTerminated);
                self.entry.init_completed.notify_all();
                true
            } else {
                state.runtime_filter = participant;
                state.phase = QueryLifecyclePhase::Initialized;
                state.ever_initialized = true;
                state.init_outcome = Some(QueryInitOutcome::Applied);
                state.pre_start_deadline =
                    Some(self.registry.clock.now() + self.registry.config.pre_start_timeout);
                self.entry.init_completed.notify_all();
                false
            }
        };
        if terminated {
            let reason = self
                .entry
                .state
                .lock()
                .expect("query lifecycle entry lock")
                .termination_reason
                .expect("termination was observed");
            if self
                .registry
                .try_complete_runtime_filter_cleanup(&self.entry, self.execution_id)
            {
                self.registry
                    .publish_tombstone(&self.entry, self.execution_id, reason);
            }
            QueryInitAck::new(
                self.execution_id,
                self.digest,
                QueryInitOutcome::RejectedTerminated,
            )
        } else {
            QueryInitAck::new(self.execution_id, self.digest, QueryInitOutcome::Applied)
        }
    }
}

impl StageBuildPermit {
    pub(crate) fn gate(&self) -> Arc<super::stage::StartGate> {
        Arc::clone(&self.gate)
    }

    pub(crate) fn commit(mut self) -> QueryStageAck {
        let mut state = self.entry.state.lock().expect("query lifecycle entry lock");
        let (outcome, detail) = if state.termination_reason.is_some()
            || matches!(
                state.phase,
                QueryLifecyclePhase::Terminating | QueryLifecyclePhase::Tombstone
            ) {
            (
                QueryStageOutcome::RejectedTerminated,
                "query lifecycle terminated during staging",
            )
        } else if state.phase == QueryLifecyclePhase::Staging
            && state.stage_digest == Some(self.digest)
        {
            let mut resources = self
                .resources
                .take()
                .expect("Stage build permit owns its resource reservation");
            resources.release_builder();
            debug_assert!(state.stage_resources.is_none());
            state.stage_resources = Some(resources);
            state.phase = QueryLifecyclePhase::Staged;
            (QueryStageOutcome::Applied, "query participant staged")
        } else {
            (
                QueryStageOutcome::RejectedInvalidState,
                "query lifecycle stage ownership was lost",
            )
        };
        self.entry.stage_completed.notify_all();
        drop(state);
        self.committed = true;
        QueryStageAck::new(
            self.execution_id,
            StageDigestVersion::V1,
            self.digest,
            outcome,
            detail,
        )
    }
}

impl Drop for StageBuildPermit {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        self.entry.stage_completed.notify_all();
        self.registry.request_termination(
            Arc::clone(&self.entry),
            QueryTerminationReason::LocalFailure,
        );
    }
}

impl FragmentAdmissionPermit {
    #[cfg(test)]
    pub(crate) fn entry_for_test(&self) -> Arc<QueryLifecycleEntry> {
        Arc::clone(&self.entry)
    }

    pub(crate) fn commit(mut self) -> Result<(), QueryLifecycleError> {
        let registry = self
            .registry
            .upgrade()
            .ok_or_else(|| internal_error("query lifecycle registry was dropped"))?;
        let mut registry_state = registry
            .state
            .lock()
            .expect("query lifecycle registry lock");
        let mut state = self.entry.state.lock().expect("query lifecycle entry lock");
        if state.termination_reason.is_some()
            || matches!(
                state.phase,
                QueryLifecyclePhase::Terminating | QueryLifecyclePhase::Tombstone
            )
        {
            let reason = state.termination_reason;
            let expected_instances = self
                .entry
                .manifest
                .expected_fragment_instance_ids()
                .iter()
                .copied()
                .collect::<Vec<_>>();
            drop(state);
            drop(registry_state);
            if let Some(reason) = reason {
                // Termination may have raced ahead of the service registration/control
                // publication protected by this permit. Re-drive local termination after
                // those resources exist so the rejected admission cannot leave a live worker.
                registry.local_runtime.terminate_query(
                    self.execution_id,
                    &expected_instances,
                    reason,
                    &termination_detail(reason),
                );
            }
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Terminated,
                "query lifecycle terminated before fragment admission commit",
            ));
        }
        if !matches!(
            state.phase,
            QueryLifecyclePhase::ControlAttached | QueryLifecyclePhase::Staging
        ) {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                "query control is not ready for fragment admission commit",
            ));
        }
        if !state
            .in_flight_fragments
            .contains(&self.fragment_instance_id)
        {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                "fragment admission permit is no longer in flight",
            ));
        }
        if registry_state
            .fragment_executions
            .contains_key(&self.fragment_instance_id)
        {
            state.in_flight_fragments.remove(&self.fragment_instance_id);
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::Conflict,
                "fragment instance already belongs to a committed query lifecycle admission",
            ));
        }
        registry_state
            .fragment_executions
            .insert(self.fragment_instance_id, self.execution_id);
        state.in_flight_fragments.remove(&self.fragment_instance_id);
        state.accepted_fragments.insert(self.fragment_instance_id);
        // A staged worker is still pre-start. Only the StartPreparedQuery
        // transition clears this deadline after releasing the shared gate.
        if state.phase == QueryLifecyclePhase::ControlAttached {
            state.pre_start_deadline = None;
        }
        drop(state);
        drop(registry_state);
        self.committed = true;
        if query_lifecycle_test_markers_enabled() {
            eprintln!(
                "NOVAROCKS_QUERY_FRAGMENT_ACCEPTED execution_id={} backend_id={} finst_id={}",
                format_execution_id(self.execution_id),
                self.registry
                    .upgrade()
                    .and_then(|registry| registry.local_backend_id())
                    .unwrap_or_default(),
                self.fragment_instance_id
            );
        }
        Ok(())
    }
}

impl Drop for FragmentAdmissionPermit {
    fn drop(&mut self) {
        if !self.committed {
            self.entry
                .state
                .lock()
                .expect("query lifecycle entry lock")
                .in_flight_fragments
                .remove(&self.fragment_instance_id);
        }
    }
}

impl BackendQueryControl for RegistryQueryControl {
    fn heartbeat(&self, sequence: u64) -> Result<(), QueryLifecycleError> {
        self.registry
            .upgrade()
            .ok_or_else(|| internal_error("query lifecycle registry was dropped"))?
            .heartbeat(self.execution_id, sequence)
    }

    fn abort(&self, _reason: String) -> Result<(), QueryLifecycleError> {
        self.registry
            .upgrade()
            .ok_or_else(|| internal_error("query lifecycle registry was dropped"))?
            .terminate_from_control(self.execution_id, QueryTerminationReason::CoordinatorAbort)
    }

    fn finalize(&self) -> Result<(), QueryLifecycleError> {
        let registry = self
            .registry
            .upgrade()
            .ok_or_else(|| internal_error("query lifecycle registry was dropped"))?;
        match registry.finalize_from_control(self.execution_id) {
            Ok(()) => Ok(()),
            // QLC-3 callers may still finalize an attempt which never reached
            // Running.  Preserve their fail-close cleanup path; QLC-4 only
            // freezes a snapshot after LocalDrained.
            Err(error) if error.code() == QueryLifecycleErrorCode::Terminated => registry
                .terminate_from_control(
                    self.execution_id,
                    QueryTerminationReason::CoordinatorFinalize,
                ),
            Err(error) => Err(error),
        }
    }

    fn terminal_ack(&self, ack: QueryTerminalAck) -> Result<(), QueryLifecycleError> {
        self.registry
            .upgrade()
            .ok_or_else(|| internal_error("query lifecycle registry was dropped"))?
            .terminal_ack_from_control(ack)
    }

    fn coordinator_lost(&self, reason: QueryTerminationReason) -> Result<(), QueryLifecycleError> {
        if query_lifecycle_test_markers_enabled() {
            let backend_id = self
                .registry
                .upgrade()
                .and_then(|registry| registry.local_backend_id())
                .unwrap_or_default();
            eprintln!(
                "NOVAROCKS_QUERY_CONTROL_COORDINATOR_LOST execution_id={} backend_id={} reason={reason:?}",
                format_execution_id(self.execution_id),
                backend_id
            );
        }
        self.registry
            .upgrade()
            .ok_or_else(|| internal_error("query lifecycle registry was dropped"))?
            .terminate_from_control(self.execution_id, reason)
    }
}

fn format_execution_id(execution_id: QueryExecutionId) -> String {
    format!(
        "{}:{}:{}",
        execution_id.query_id().high(),
        execution_id.query_id().low(),
        execution_id.attempt_id().get()
    )
}

#[cfg(debug_assertions)]
pub(super) fn query_lifecycle_test_markers_enabled() -> bool {
    novarocks::common::app_config::config()
        .ok()
        .and_then(|config| config.debug.query_lifecycle_fault_dir())
        .is_some()
}

#[cfg(not(debug_assertions))]
pub(super) fn query_lifecycle_test_markers_enabled() -> bool {
    false
}

impl QueryLifecycleIngress for QueryLifecycleRegistry {
    fn bind_backend_identity(&self, backend_id: u64) -> Result<(), QueryLifecycleError> {
        QueryLifecycleRegistry::bind_backend_identity(self, backend_id)
    }

    fn init_query(&self, request: QueryInitRequest) -> QueryInitAck {
        QueryLifecycleRegistry::init_query(self, request)
    }

    fn stage_fragments(&self, request: QueryStageRequest) -> QueryStageAck {
        QueryLifecycleRegistry::stage_fragments(self, request)
    }

    fn start_prepared_query(&self, request: QueryStartRequest) -> QueryStartAck {
        QueryLifecycleRegistry::start_prepared_query(self, request)
    }

    fn abort_query(
        &self,
        request: QueryAbortRequest,
    ) -> Result<QueryTerminationAck, QueryLifecycleError> {
        QueryLifecycleRegistry::abort_query(self, request)
    }

    fn attach_control(
        &self,
        attach: QueryControlAttach,
    ) -> Result<QueryControlAttachment, QueryLifecycleError> {
        QueryLifecycleRegistry::attach_control(self, attach)
    }
}

fn fold_metrics_locked(
    state: &QueryLifecycleRegistryState,
) -> (BackendQueryLifecycleMetricsSnapshot, [u64; 6]) {
    let mut snapshot = BackendQueryLifecycleMetricsSnapshot {
        tombstones: state.tombstones.len(),
        admission_rejected: state.admission_rejected,
        init_conflicts: state.init_conflicts,
        heartbeat_timeouts: state.heartbeat_timeouts,
        terminations: state.terminations,
        terminal_facts: state.terminal_facts,
        terminal_locally_drained: state.terminal_locally_drained,
        terminal_records_frozen: state.terminal_records_frozen,
        terminal_acknowledged: state.terminal_acknowledged,
        terminal_retention_expired: state.terminal_retention_expired,
        terminal_fallback_accepted: state.terminal_fallback_accepted,
        terminal_fallback_rejected: state.terminal_fallback_rejected,
        terminal_retained: state.terminal_retained.len(),
        terminal_retained_bytes: state.terminal_retained_bytes,
        ..BackendQueryLifecycleMetricsSnapshot::default()
    };
    for entry in state.entries.values() {
        match entry
            .state
            .lock()
            .expect("query lifecycle entry lock")
            .phase
        {
            QueryLifecyclePhase::Initializing => snapshot.initializing += 1,
            QueryLifecyclePhase::Initialized => snapshot.initialized += 1,
            QueryLifecyclePhase::ControlAttached
            | QueryLifecyclePhase::Staging
            | QueryLifecyclePhase::Staged
            | QueryLifecyclePhase::Running
            | QueryLifecyclePhase::TerminalRetained => snapshot.control_attached += 1,
            QueryLifecyclePhase::Terminating => snapshot.terminating += 1,
            QueryLifecyclePhase::Tombstone => {}
        }
    }
    (snapshot, state.termination_reasons)
}

const fn phase_name(phase: QueryLifecyclePhase) -> &'static str {
    match phase {
        QueryLifecyclePhase::Initializing => "initializing",
        QueryLifecyclePhase::Initialized => "initialized",
        QueryLifecyclePhase::ControlAttached => "control_attached",
        QueryLifecyclePhase::Staging => "staging",
        QueryLifecyclePhase::Staged => "staged",
        QueryLifecyclePhase::Running => "running",
        QueryLifecyclePhase::TerminalRetained => "terminal_retained",
        QueryLifecyclePhase::Terminating => "terminating",
        QueryLifecyclePhase::Tombstone => "tombstone",
    }
}

fn termination_reason_index(reason: QueryTerminationReason) -> usize {
    match reason {
        QueryTerminationReason::CoordinatorAbort => 0,
        QueryTerminationReason::CoordinatorFinalize => 1,
        QueryTerminationReason::CoordinatorStreamLost => 2,
        QueryTerminationReason::CoordinatorHeartbeatTimeout => 3,
        QueryTerminationReason::LocalFailure => 4,
        QueryTerminationReason::PreStartTimeout => 5,
    }
}

fn termination_detail(reason: QueryTerminationReason) -> String {
    format!("query lifecycle terminated: {reason:?}")
}

fn format_digest(digest: ParticipantManifestDigest) -> String {
    use std::fmt::Write;

    let mut formatted = String::with_capacity(64);
    for byte in digest.as_bytes() {
        write!(&mut formatted, "{byte:02x}").expect("write digest to string");
    }
    formatted
}

#[allow(dead_code)]
fn internal_error(detail: impl Into<String>) -> QueryLifecycleError {
    QueryLifecycleError::new(QueryLifecycleErrorCode::Internal, detail)
}
