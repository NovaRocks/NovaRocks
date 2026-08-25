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

use std::sync::{Arc, Barrier, Condvar, Mutex, TryLockError};
use std::time::{Duration, Instant};

use crate::metrics::query_lifecycle::BackendQueryLifecycleMetricsSnapshot;
use novarocks_execution::runtime::fragment::{
    FragmentExecutionError, FragmentExecutionErrorKind, FragmentOutcome,
};
use novarocks_proto::{
    common, filter,
    lifecycle::{
        AttemptId, ParticipantBackendIdentity, ParticipantManifest, ParticipantRole,
        ParticipantTerminalOutcome as ProtocolParticipantTerminalOutcome, QueryAbortRequest,
        QueryControlAttach, QueryControlEndpoint, QueryExecutionId, QueryInitOutcome,
        QueryInitRequest, QueryOptions, QueryStageOutcome, QueryStageRequest, QueryStartOutcome,
        QueryStartRequest, QueryTerminalReportAck, QueryTerminalReportOutcome,
        QueryTerminationReason, RuntimeFilterContribution, StageDigest, StageDigestVersion,
        StageFragment,
    },
    novarocks as proto_novarocks, plan,
};
use novarocks_types::QueryId;
use novarocks_types::UniqueId;
use prost::Message;
use sha2::Digest;

use super::entry::QueryLifecyclePhase;
use super::registry::{
    MonotonicClock, QueryLifecycleLocalRuntime, QueryLifecycleMetricsSink, QueryLifecycleRegistry,
    QueryLifecycleRegistryConfig, StageBuildDecision,
};
use super::{
    QueryControlAttachment, QueryLifecycleError, QueryLifecycleErrorCode,
    QueryTerminalFallbackTransport, QueryTerminalFallbackTransportError,
};
use crate::rpc::runtime::test_backend_data_runtime;
use crate::runtime_filter::install_decode::DecodedRuntimeFilterContribution;
use crate::runtime_filter::participant::{
    BackendRuntimeFilterParticipantFactory, RuntimeFilterParticipantFactory,
};
use novarocks_failpoint::QueryLifecycleFaultKind;

const LOCAL_BACKEND_ID: u64 = 7;
const LOCAL_START_EPOCH: u64 = 11;
const ATTEMPT_1: u64 = 1;

type NegativeAttestationReason = proto_novarocks::NegativeAttestationReason;

trait TestManifestResult {
    fn execution_id(&self) -> QueryExecutionId;
}

impl TestManifestResult for Result<ParticipantManifest, novarocks_proto::lifecycle::ContractError> {
    fn execution_id(&self) -> QueryExecutionId {
        self.as_ref()
            .expect("validated init request retains a manifest")
            .execution_id()
            .expect("validated manifest retains an execution id")
    }
}

trait TestQueryAbortExpect {
    fn expect(self, message: &str) -> Self;
}

impl TestQueryAbortExpect for QueryAbortRequest {
    fn expect(self, _message: &str) -> Self {
        self
    }
}

#[derive(Clone)]
struct ManualClock {
    base: Instant,
    offset: Arc<Mutex<Duration>>,
}

impl Default for ManualClock {
    fn default() -> Self {
        Self {
            base: Instant::now(),
            offset: Arc::new(Mutex::new(Duration::ZERO)),
        }
    }
}

impl ManualClock {
    fn advance(&self, duration: Duration) {
        *self.offset.lock().expect("manual clock offset") += duration;
    }
}

impl MonotonicClock for ManualClock {
    fn now(&self) -> Instant {
        self.base + *self.offset.lock().expect("manual clock offset")
    }
}

#[derive(Clone, Default)]
struct RecordingLocalRuntime {
    state: Arc<RecordingLocalRuntimeState>,
}

#[derive(Default)]
struct RecordingLocalRuntimeState {
    install_calls: Mutex<Vec<QueryExecutionId>>,
    abort_calls: Mutex<Vec<QueryExecutionId>>,
    terminations: Mutex<Vec<TerminationCall>>,
    releases: Mutex<Vec<QueryExecutionId>>,
    lifecycle_order: Mutex<Vec<(&'static str, QueryExecutionId)>>,
    install_gate: Mutex<InstallGate>,
    install_gate_changed: Condvar,
    fail_install: Mutex<bool>,
    fail_abort: Mutex<bool>,
}

type TerminationCall = (
    QueryExecutionId,
    Vec<UniqueId>,
    QueryTerminationReason,
    String,
);

#[derive(Default)]
struct RecordingMetricsSink {
    snapshots: Mutex<Vec<BackendQueryLifecycleMetricsSnapshot>>,
    termination_reasons: Mutex<Vec<[u64; 6]>>,
}

struct RejectedTerminalFallback;

impl QueryTerminalFallbackTransport for RejectedTerminalFallback {
    fn report_query_terminal(
        &self,
        _endpoint: &QueryControlEndpoint,
        _outcome: ProtocolParticipantTerminalOutcome,
        _timeout: Duration,
    ) -> Result<QueryTerminalReportAck, QueryTerminalFallbackTransportError> {
        Ok(QueryTerminalReportAck::new(
            QueryTerminalReportOutcome::RejectedConflict,
            "injected terminal conflict",
        )
        .expect("fixed rejected-conflict terminal ack is valid"))
    }
}

struct GoneTerminalFallback;

impl QueryTerminalFallbackTransport for GoneTerminalFallback {
    fn report_query_terminal(
        &self,
        _endpoint: &QueryControlEndpoint,
        _outcome: ProtocolParticipantTerminalOutcome,
        _timeout: Duration,
    ) -> Result<QueryTerminalReportAck, QueryTerminalFallbackTransportError> {
        Ok(QueryTerminalReportAck::new(
            QueryTerminalReportOutcome::RejectedGone,
            "injected stale terminal ingress",
        )
        .expect("fixed rejected-gone terminal ack is valid"))
    }
}

#[derive(Clone, Default)]
struct AcceptedTerminalFallback {
    outcomes: Arc<Mutex<Vec<ProtocolParticipantTerminalOutcome>>>,
}

impl AcceptedTerminalFallback {
    fn outcomes(&self) -> Vec<ProtocolParticipantTerminalOutcome> {
        self.outcomes
            .lock()
            .expect("accepted terminal fallback outcomes")
            .clone()
    }
}

impl QueryTerminalFallbackTransport for AcceptedTerminalFallback {
    fn report_query_terminal(
        &self,
        _endpoint: &QueryControlEndpoint,
        outcome: ProtocolParticipantTerminalOutcome,
        _timeout: Duration,
    ) -> Result<QueryTerminalReportAck, QueryTerminalFallbackTransportError> {
        self.outcomes
            .lock()
            .expect("accepted terminal fallback outcomes")
            .push(outcome);
        Ok(QueryTerminalReportAck::new(
            QueryTerminalReportOutcome::Accepted,
            "accepted injected unary fallback",
        )
        .expect("fixed accepted terminal ack is valid"))
    }
}

impl RecordingMetricsSink {
    fn last_snapshot(&self) -> BackendQueryLifecycleMetricsSnapshot {
        *self
            .snapshots
            .lock()
            .expect("metrics snapshots")
            .last()
            .expect("published metrics snapshot")
    }

    fn last_termination_reasons(&self) -> [u64; 6] {
        *self
            .termination_reasons
            .lock()
            .expect("termination reason snapshots")
            .last()
            .expect("published termination reason snapshot")
    }
}

impl QueryLifecycleMetricsSink for RecordingMetricsSink {
    fn publish(
        &self,
        snapshot: BackendQueryLifecycleMetricsSnapshot,
        termination_reasons: [u64; 6],
    ) {
        self.snapshots
            .lock()
            .expect("metrics snapshots")
            .push(snapshot);
        self.termination_reasons
            .lock()
            .expect("termination reason snapshots")
            .push(termination_reasons);
    }
}

#[derive(Default)]
struct InstallGate {
    block: bool,
    entered: bool,
}

impl RecordingLocalRuntime {
    fn block_install(&self) {
        self.state.install_gate.lock().expect("install gate").block = true;
    }

    fn wait_until_install_enters(&self) {
        let mut gate = self.state.install_gate.lock().expect("install gate");
        while !gate.entered {
            gate = self
                .state
                .install_gate_changed
                .wait(gate)
                .expect("install gate wait");
        }
    }

    fn release_install(&self) {
        let mut gate = self.state.install_gate.lock().expect("install gate");
        gate.block = false;
        self.state.install_gate_changed.notify_all();
    }

    fn runtime_filter_install_calls(&self) -> usize {
        self.state
            .install_calls
            .lock()
            .expect("install calls")
            .len()
    }

    fn runtime_filter_abort_calls(&self) -> usize {
        self.state.abort_calls.lock().expect("abort calls").len()
    }

    fn release_calls(&self) -> usize {
        self.state.releases.lock().expect("release calls").len()
    }

    fn lifecycle_order(&self) -> Vec<(&'static str, QueryExecutionId)> {
        self.state
            .lifecycle_order
            .lock()
            .expect("lifecycle order")
            .clone()
    }

    fn fail_install(&self) {
        *self.state.fail_install.lock().expect("fail install") = true;
    }

    fn fail_abort(&self) {
        *self.state.fail_abort.lock().expect("fail abort") = true;
    }

    fn allow_abort(&self) {
        *self.state.fail_abort.lock().expect("fail abort") = false;
    }
}

impl QueryLifecycleLocalRuntime for RecordingLocalRuntime {
    fn quiesce_query(
        &self,
        execution_id: QueryExecutionId,
        expected_instances: &[UniqueId],
        reason: QueryTerminationReason,
        detail: &str,
    ) {
        self.state
            .lifecycle_order
            .lock()
            .expect("lifecycle order")
            .push(("quiesce", execution_id));
        self.state.terminations.lock().expect("terminations").push((
            execution_id,
            expected_instances.to_vec(),
            reason,
            detail.to_string(),
        ));
    }

    fn release_query_resources(&self, execution_id: QueryExecutionId) {
        self.state
            .lifecycle_order
            .lock()
            .expect("lifecycle order")
            .push(("release", execution_id));
        self.state
            .releases
            .lock()
            .expect("release calls")
            .push(execution_id);
    }
}

impl RuntimeFilterParticipantFactory for RecordingLocalRuntime {
    fn install(
        &self,
        execution_id: QueryExecutionId,
        contribution: DecodedRuntimeFilterContribution,
    ) -> Result<
        Arc<crate::runtime_filter::participant::RuntimeFilterParticipant>,
        QueryLifecycleError,
    > {
        {
            let mut gate = self.state.install_gate.lock().expect("install gate");
            gate.entered = true;
            self.state.install_gate_changed.notify_all();
            while gate.block {
                gate = self
                    .state
                    .install_gate_changed
                    .wait(gate)
                    .expect("install gate wait");
            }
        }
        self.state
            .install_calls
            .lock()
            .expect("install calls")
            .push(execution_id);
        if *self.state.fail_install.lock().expect("fail install") {
            return Err(QueryLifecycleError::new(
                QueryLifecycleErrorCode::InvalidManifest,
                "injected runtime-filter participant install failure",
            ));
        }
        let participant = BackendRuntimeFilterParticipantFactory::new(test_backend_data_runtime())
            .install(execution_id, contribution)?;
        let state = Arc::clone(&self.state);
        Ok(
            participant.with_close_hook_for_test(Arc::new(move |_participant, _reason| {
                state
                    .lifecycle_order
                    .lock()
                    .expect("lifecycle order")
                    .push(("close", execution_id));
                state
                    .abort_calls
                    .lock()
                    .expect("abort calls")
                    .push(execution_id);
                if *state.fail_abort.lock().expect("fail abort") {
                    return Err(QueryLifecycleError::new(
                        QueryLifecycleErrorCode::Internal,
                        "injected runtime-filter participant close failure",
                    ));
                }
                Ok(())
            })),
        )
    }
}

fn registry_config(max_active_entries: usize) -> QueryLifecycleRegistryConfig {
    QueryLifecycleRegistryConfig {
        max_active_entries,
        tombstone_capacity: 16_384,
        tombstone_retention: Duration::from_millis(120_000),
        heartbeat_timeout: Duration::from_millis(5_000),
        pre_start_timeout: Duration::from_millis(30_000),
        stage_max_fragments: 256,
        max_active_staging: 32,
        stage_max_encoded_bytes: 48 * 1024 * 1024,
        stage_max_inflight_encoded_bytes: 256 * 1024 * 1024,
        stage_max_dormant_workers: 512,
        terminal_max_encoded_bytes: 48 * 1024 * 1024,
        terminal_drain_timeout: Duration::from_secs(30),
        terminal_ack_timeout: Duration::from_millis(5_000),
        terminal_fallback_rpc_timeout: Duration::from_millis(5_000),
        terminal_fallback_max_attempts: 5,
        terminal_fallback_initial_backoff: Duration::from_millis(100),
        terminal_fallback_max_backoff: Duration::from_millis(1_000),
        terminal_retention: Duration::from_millis(120_000),
        terminal_retained_capacity: 4_096,
        terminal_max_retained_bytes: 256 * 1024 * 1024,
    }
}

fn wait_for_failed_terminal_freeze(registry: &QueryLifecycleRegistry) {
    for _ in 0..100 {
        if registry.metrics_snapshot().terminal_records_frozen > 0 {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("failed terminal snapshot was not frozen within 100ms");
}

#[test]
fn terminal_fallback_conflict_releases_bounded_delivery_record() {
    let runtime = RecordingLocalRuntime::default();
    let clock = Arc::new(ManualClock::default());
    let metrics = Arc::new(RecordingMetricsSink::default());
    let mut config = registry_config(8);
    config.terminal_ack_timeout = Duration::from_millis(1);
    config.terminal_drain_timeout = Duration::from_millis(1);
    let registry = QueryLifecycleRegistry::new_with_clock_metrics_and_terminal_fallback(
        LOCAL_BACKEND_ID,
        LOCAL_START_EPOCH,
        Arc::new(runtime),
        config,
        Arc::clone(&clock) as Arc<dyn MonotonicClock>,
        metrics.clone(),
        Arc::new(RejectedTerminalFallback),
    );
    let fragment_instance_id = UniqueId::new(863, 1);
    let request = fragment_init_request_fixture(863, &[fragment_instance_id]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let _control = attach_control(&registry, &request);
    registry
        .admit_fragment(execution_id, fragment_instance_id)
        .expect("fragment permit")
        .commit()
        .expect("fragment admission commits");
    registry
        .abort_query(
            QueryAbortRequest::new(
                execution_id,
                request.digest().expect("validated init digest"),
                "terminal conflict",
            )
            .expect("valid abort"),
        )
        .expect("abort is accepted");

    for _ in 0..100 {
        if registry.metrics_snapshot().terminal_retained == 0
            && registry.metrics_snapshot().terminal_fallback_rejected > 0
        {
            assert_eq!(metrics.last_snapshot().terminal_retained, 0);
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("terminal conflict did not release the retained delivery record");
}

#[test]
fn terminal_fallback_gone_releases_bounded_delivery_record() {
    let runtime = RecordingLocalRuntime::default();
    let clock = Arc::new(ManualClock::default());
    let metrics = Arc::new(RecordingMetricsSink::default());
    let mut config = registry_config(8);
    config.terminal_ack_timeout = Duration::from_millis(1);
    config.terminal_drain_timeout = Duration::from_millis(1);
    let registry = QueryLifecycleRegistry::new_with_clock_metrics_and_terminal_fallback(
        LOCAL_BACKEND_ID,
        LOCAL_START_EPOCH,
        Arc::new(runtime),
        config,
        Arc::clone(&clock) as Arc<dyn MonotonicClock>,
        metrics.clone(),
        Arc::new(GoneTerminalFallback),
    );
    let fragment_instance_id = UniqueId::new(864, 1);
    let request = fragment_init_request_fixture(864, &[fragment_instance_id]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let _control = attach_control(&registry, &request);
    registry
        .admit_fragment(execution_id, fragment_instance_id)
        .expect("fragment permit")
        .commit()
        .expect("fragment admission commits");
    registry
        .abort_query(
            QueryAbortRequest::new(
                execution_id,
                request.digest().expect("validated init digest"),
                "stale terminal ingress",
            )
            .expect("valid abort"),
        )
        .expect("abort is accepted");

    for _ in 0..100 {
        if registry.metrics_snapshot().terminal_retained == 0
            && registry.metrics_snapshot().terminal_fallback_rejected > 0
        {
            assert_eq!(metrics.last_snapshot().terminal_retained, 0);
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("gone terminal fallback did not release the retained delivery record");
}

fn registry_with(
    runtime: RecordingLocalRuntime,
    max_active_entries: usize,
) -> Arc<QueryLifecycleRegistry> {
    registry_with_clock(
        runtime,
        max_active_entries,
        Arc::new(ManualClock::default()),
    )
}

fn registry_with_config(
    runtime: RecordingLocalRuntime,
    config: QueryLifecycleRegistryConfig,
) -> Arc<QueryLifecycleRegistry> {
    QueryLifecycleRegistry::new_with_clock_metrics_terminal_fallback_and_runtime_filter_factory(
        LOCAL_BACKEND_ID,
        LOCAL_START_EPOCH,
        Arc::new(runtime.clone()),
        config,
        Arc::new(ManualClock::default()),
        Arc::new(RecordingMetricsSink::default()),
        Arc::new(RejectedTerminalFallback),
        Arc::new(runtime),
    )
}

fn registry_with_clock(
    runtime: RecordingLocalRuntime,
    max_active_entries: usize,
    clock: Arc<ManualClock>,
) -> Arc<QueryLifecycleRegistry> {
    QueryLifecycleRegistry::new_with_clock_metrics_terminal_fallback_and_runtime_filter_factory(
        LOCAL_BACKEND_ID,
        LOCAL_START_EPOCH,
        Arc::new(runtime.clone()),
        registry_config(max_active_entries),
        clock,
        Arc::new(RecordingMetricsSink::default()),
        Arc::new(RejectedTerminalFallback),
        Arc::new(runtime),
    )
}

#[test]
fn query_control_attachment_requires_backend_identity_binding() {
    let runtime = RecordingLocalRuntime::default();
    let registry = QueryLifecycleRegistry::new_unbound(
        LOCAL_START_EPOCH,
        Arc::new(runtime),
        registry_config(8),
    );
    let request = init_request_fixture(700, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);

    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::RejectedStaleBackend
    );
    registry
        .bind_backend_identity(LOCAL_BACKEND_ID)
        .expect("first FE-assigned identity binds");
    assert_eq!(
        registry
            .bind_backend_identity(LOCAL_BACKEND_ID + 1)
            .expect_err("backend identity takeover must fail")
            .code(),
        QueryLifecycleErrorCode::Conflict
    );
    assert_eq!(
        registry
            .init_query(request)
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
}

#[test]
fn attach_reserves_p0_before_control_ready_and_releases_on_terminal_cleanup() {
    let runtime = RecordingLocalRuntime::default();
    let mut config = registry_config(8);
    config.terminal_retained_capacity = 1;
    let registry = registry_with_config(runtime, config);
    let first = init_request_fixture(9_701, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let second = init_request_fixture(9_702, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    assert_eq!(
        registry
            .init_query(first.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    assert_eq!(
        registry
            .init_query(second.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );

    let mut first_attachment = attach_control(&registry, &first);
    assert_control_ready(&mut first_attachment);
    let error = match registry.attach_control(
        QueryControlAttach::new(
            second.manifest().execution_id(),
            second.digest().expect("validated init digest"),
            1,
        )
        .expect("valid control attach"),
    ) {
        Ok(_) => panic!("P0 capacity is consumed before a second ControlReady can be emitted"),
        Err(error) => error,
    };
    assert_eq!(error.code(), QueryLifecycleErrorCode::Capacity);

    registry
        .abort_query(
            QueryAbortRequest::new(
                first.manifest().execution_id(),
                first.digest().expect("validated init digest"),
                "release P0 reservation",
            )
            .expect("valid abort"),
        )
        .expect("first attached entry aborts");
    let mut second_attachment = attach_control(&registry, &second);
    assert_control_ready(&mut second_attachment);
}

#[test]
fn injected_p0_faults_reject_before_control_ready_and_leave_entry_retryable() {
    for (query_low, fault) in [
        (
            9_711,
            QueryLifecycleFaultKind::TerminalP0RetainedSlotExhausted,
        ),
        (9_712, QueryLifecycleFaultKind::TerminalP0BytesExhausted),
        (
            9_713,
            QueryLifecycleFaultKind::TerminalP0DeliveryPermitExhausted,
        ),
    ] {
        let registry = registry_with(RecordingLocalRuntime::default(), 8);
        let request = init_request_fixture(query_low, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
        let execution_id = request.manifest().execution_id();
        assert_eq!(
            registry
                .init_query(request.clone())
                .outcome()
                .expect("validated lifecycle acknowledgement"),
            QueryInitOutcome::Applied
        );
        registry.inject_terminal_fault_for_test(execution_id, fault);

        let error = match registry.attach_control(
            QueryControlAttach::new(
                execution_id,
                request.digest().expect("validated init digest"),
                1,
            )
            .expect("valid control attach"),
        ) {
            Ok(_) => panic!("injected P0 fault rejects before ControlReady"),
            Err(error) => error,
        };
        assert_eq!(error.code(), QueryLifecycleErrorCode::Capacity);
        assert_eq!(
            registry.phase(execution_id),
            Some(QueryLifecyclePhase::Initialized)
        );
        assert_eq!(registry.metrics_snapshot().terminal_retained, 0);

        let mut retry = attach_control(&registry, &request);
        assert_control_ready(&mut retry);
    }
}

#[test]
fn fresh_unbound_registry_reports_no_restoration_relevant_state_after_binding() {
    let registry = QueryLifecycleRegistry::new_unbound(
        LOCAL_START_EPOCH,
        Arc::new(RecordingLocalRuntime::default()),
        registry_config(8),
    );

    registry
        .bind_backend_identity(LOCAL_BACKEND_ID)
        .expect("first FE-assigned identity binds");
    let status = registry.restoration_status();

    assert_eq!(status.control_ready, 0);
    assert_eq!(status.active_lifecycle, 0);
    assert_eq!(status.fragment_admissions, 0);
    assert_eq!(status.fragment_acceptances, 0);
    assert_eq!(status.lifecycle_entries, 0);
    assert_eq!(status.lifecycle_tombstones, 0);
    assert_eq!(status.pre_init_tombstones, 0);
    assert_eq!(status.tombstone_index, 0);
    assert!(!status.restored);
}

#[test]
fn restoration_status_counts_all_retained_execution_indexes_without_clearing_them() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let active = init_request_fixture(120, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let lifecycle_tombstone = init_request_fixture(121, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let pre_init_tombstone = init_request_fixture(122, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);

    assert_eq!(
        registry
            .init_query(active.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    assert_eq!(
        registry
            .init_query(lifecycle_tombstone.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    registry
        .abort_query(
            QueryAbortRequest::new(
                lifecycle_tombstone.manifest().execution_id(),
                lifecycle_tombstone.digest().expect("validated init digest"),
                "retain lifecycle tombstone",
            )
            .expect("valid lifecycle tombstone abort"),
        )
        .expect("lifecycle tombstone abort is accepted");
    registry
        .abort_query(
            QueryAbortRequest::new(
                pre_init_tombstone.manifest().execution_id(),
                pre_init_tombstone.digest().expect("validated init digest"),
                "retain pre-init tombstone",
            )
            .expect("valid pre-init tombstone abort"),
        )
        .expect("pre-init tombstone abort is accepted");

    let status = registry.restoration_status();

    assert_eq!(status.active_lifecycle, 1);
    assert_eq!(status.lifecycle_entries, 2);
    assert_eq!(status.lifecycle_tombstones, 1);
    assert_eq!(status.pre_init_tombstones, 1);
    assert_eq!(status.tombstone_index, 2);
    assert!(status.restored);
    assert!(registry.contains(active.manifest().execution_id()));
    assert!(registry.contains(lifecycle_tombstone.manifest().execution_id()));
    assert!(registry.contains(pre_init_tombstone.manifest().execution_id()));
}

fn execution_id(query_low: i64, attempt: u64) -> QueryExecutionId {
    QueryExecutionId::new(
        QueryId::new(0x514c_4302, query_low),
        AttemptId::new(attempt).expect("nonzero attempt"),
    )
    .expect("nonzero query execution id")
}

fn default_query_options() -> QueryOptions {
    QueryOptions::parse(proto_novarocks::QueryOptions::default())
        .expect("default generated query options are valid")
}

fn protocol_unique_id(id: UniqueId) -> common::UniqueId {
    common::UniqueId {
        hi: id.high(),
        lo: id.low(),
    }
}

fn runtime_filter_contribution(
    execution_id: QueryExecutionId,
    participant_id: u32,
) -> RuntimeFilterContribution {
    let lifecycle = filter::RuntimeFilterQueryLifecycleOptions {
        delivery_expire_ms: 1,
        query_expire_ms: 1,
        transport_retry_interval_ms: 1,
        transport_max_attempts: 1,
        transport_deadline_ms: 1,
        transport_max_pending_entries: 1,
        transport_max_pending_bytes: 1,
    };
    let install = filter::RuntimeFilterParticipantInstall::default();
    let envelope = filter::InstallRuntimeFilterDeploymentRequest {
        query_id: Some(common::UniqueId {
            hi: execution_id.query_id().high(),
            lo: execution_id.query_id().low(),
        }),
        deployment_epoch: execution_id.attempt_id().get(),
        participant_id,
        lifecycle: Some(lifecycle),
        install: Some(install.clone()),
    };
    let mut digest = sha2::Sha256::new();
    digest.update(b"novarocks.query-lifecycle.runtime-filter-contribution.v1\0");
    digest.update(envelope.encode_to_vec());
    RuntimeFilterContribution::parse(proto_novarocks::RuntimeFilterContribution {
        participant_id,
        lifecycle: Some(lifecycle),
        install: Some(install),
        contribution_digest: digest.finalize().to_vec(),
    })
    .expect("valid runtime-filter contribution")
}

fn init_request_fixture(
    query_low: i64,
    attempt: u64,
    start_epoch: u64,
    query_deadline_unix_ms: u64,
) -> QueryInitRequest {
    let execution_id = execution_id(query_low, attempt);
    let runtime_filter = runtime_filter_contribution(execution_id, 3);
    let manifest = ParticipantManifest::new(
        execution_id,
        ParticipantBackendIdentity::new(
            LOCAL_BACKEND_ID,
            QueryControlEndpoint::new("127.0.0.1", 9030).expect("valid endpoint"),
            start_epoch,
        )
        .expect("valid backend identity"),
        [ParticipantRole::RuntimeFilterService],
        [],
        default_query_options(),
        query_deadline_unix_ms,
        [],
        Some(runtime_filter),
        Duration::from_secs(30),
        QueryControlEndpoint::new("127.0.0.1", 9031).expect("valid report endpoint"),
    )
    .expect("valid participant manifest");
    QueryInitRequest::from_manifest(manifest)
}

fn fragment_init_request_fixture(query_low: i64, expected: &[UniqueId]) -> QueryInitRequest {
    let execution_id = execution_id(query_low, ATTEMPT_1);
    let manifest = ParticipantManifest::new(
        execution_id,
        ParticipantBackendIdentity::new(
            LOCAL_BACKEND_ID,
            QueryControlEndpoint::new("127.0.0.1", 9030).expect("valid endpoint"),
            LOCAL_START_EPOCH,
        )
        .expect("valid backend identity"),
        [ParticipantRole::FragmentExecutor],
        expected.iter().copied().map(protocol_unique_id),
        default_query_options(),
        10_000,
        [],
        None,
        Duration::from_secs(30),
        QueryControlEndpoint::new("127.0.0.1", 9031).expect("valid report endpoint"),
    )
    .expect("valid fragment participant manifest");
    QueryInitRequest::from_manifest(manifest)
}

fn fragment_runtime_filter_init_request_fixture(
    query_low: i64,
    expected: &[UniqueId],
) -> QueryInitRequest {
    let execution_id = execution_id(query_low, ATTEMPT_1);
    let manifest = ParticipantManifest::new(
        execution_id,
        ParticipantBackendIdentity::new(
            LOCAL_BACKEND_ID,
            QueryControlEndpoint::new("127.0.0.1", 9030).expect("valid endpoint"),
            LOCAL_START_EPOCH,
        )
        .expect("valid backend identity"),
        [
            ParticipantRole::FragmentExecutor,
            ParticipantRole::RuntimeFilterService,
        ],
        expected.iter().copied().map(protocol_unique_id),
        default_query_options(),
        10_000,
        [],
        Some(runtime_filter_contribution(execution_id, 3)),
        Duration::from_secs(30),
        QueryControlEndpoint::new("127.0.0.1", 9031).expect("valid report endpoint"),
    )
    .expect("valid fragment and runtime-filter participant manifest");
    QueryInitRequest::from_manifest(manifest)
}

fn attach_control(
    registry: &Arc<QueryLifecycleRegistry>,
    request: &QueryInitRequest,
) -> QueryControlAttachment {
    registry
        .attach_control(
            QueryControlAttach::new(
                request.manifest().execution_id(),
                request.digest().expect("validated init digest"),
                1,
            )
            .expect("valid control attach"),
        )
        .expect("control attaches")
}

/// Production queues carry generated Protocol events. Tests inspect the
/// generated oneof directly so a sealed Protocol digest never re-enters the
/// retired Core codec.
fn try_recv_event(
    attachment: &mut QueryControlAttachment,
) -> Result<proto_novarocks::QueryControlResponse, tokio::sync::mpsc::error::TryRecvError> {
    attachment
        .events
        .try_recv()
        .map(|event| event.as_proto().clone())
}

fn assert_control_ready(attachment: &mut QueryControlAttachment) {
    assert!(matches!(
        try_recv_event(attachment),
        Ok(proto_novarocks::QueryControlResponse {
            event: Some(proto_novarocks::query_control_response::Event::ControlReady(_)),
        })
    ));
}

fn terminal_ack_from_outcome(
    outcome: proto_novarocks::ParticipantTerminalOutcome,
) -> novarocks_proto::lifecycle::QueryTerminalAck {
    let outcome = ProtocolParticipantTerminalOutcome::parse(outcome)
        .expect("registry emitted a Protocol-valid terminal outcome");
    novarocks_proto::lifecycle::QueryTerminalAck::parse(proto_novarocks::QueryControlTerminalAck {
        execution_id: Some(outcome.execution_id().to_proto()),
        init_digest: outcome.init_digest().as_bytes().to_vec(),
        snapshot_version: 1,
        snapshot_digest: outcome.digest().to_vec(),
    })
    .expect("terminal outcome forms a valid acknowledgement")
}

fn wait_for_terminal_outcome(
    attachment: &mut QueryControlAttachment,
) -> proto_novarocks::ParticipantTerminalOutcome {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match try_recv_event(attachment) {
            Ok(proto_novarocks::QueryControlResponse {
                event:
                    Some(proto_novarocks::query_control_response::Event::TerminalOutcome(outcome)),
            }) => return outcome,
            Ok(_) => {}
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("terminal snapshot was not delivered: {error}"),
        }
    }
}

fn stage_fragment(instance_id: UniqueId, semantic_nonce: u8) -> StageFragment {
    StageFragment::new(
        plan::PlanFragment {
            fragment_id: u32::from(semantic_nonce),
            ..Default::default()
        },
        proto_novarocks::InstanceParams {
            fragment_instance_id: Some(common::UniqueId {
                hi: instance_id.high(),
                lo: instance_id.low(),
            }),
            ..Default::default()
        },
    )
    .expect("valid stage fragment")
}

fn stage_request(
    request: &QueryInitRequest,
    digest_byte: u8,
    instances: &[UniqueId],
) -> QueryStageRequest {
    let execution_id = protocol_execution_id(request.manifest().execution_id());
    let init_digest = protocol_manifest_digest(request.digest().expect("validated init digest"));
    let fragments = instances
        .iter()
        .copied()
        .map(|instance| stage_fragment(instance, digest_byte))
        .collect::<Vec<_>>();
    let digest = StageDigest::compute_v1(execution_id, init_digest, &fragments)
        .expect("valid test Stage digest");
    QueryStageRequest::new(
        execution_id,
        init_digest,
        StageDigestVersion::V1,
        digest,
        fragments,
    )
    .expect("valid stage request")
}

fn protocol_execution_id(execution_id: QueryExecutionId) -> QueryExecutionId {
    execution_id
}

fn protocol_manifest_digest(
    digest: novarocks_proto::lifecycle::ParticipantManifestDigest,
) -> novarocks_proto::lifecycle::ParticipantManifestDigest {
    digest
}

fn start_request(request: &QueryInitRequest, stage: &QueryStageRequest) -> QueryStartRequest {
    QueryStartRequest::new(
        protocol_execution_id(request.manifest().execution_id()),
        StageDigestVersion::V1,
        stage.digest(),
    )
    .expect("valid test Start request")
}

#[test]
fn stage_and_start_are_idempotent_after_control_ready() {
    let expected = [UniqueId::new(8, 1), UniqueId::new(8, 2)];
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let request = fragment_init_request_fixture(1_801, &expected);
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let _attachment = attach_control(&registry, &request);
    let stage = stage_request(&request, 4, &[expected[1], expected[0]]);

    assert_eq!(
        registry.stage_fragments(stage.clone()).outcome(),
        QueryStageOutcome::Applied
    );
    assert_eq!(
        registry.phase(request.manifest().execution_id()),
        Some(QueryLifecyclePhase::Staged)
    );
    assert_eq!(
        registry.stage_fragments(stage.clone()).outcome(),
        QueryStageOutcome::AlreadyApplied
    );
    assert_eq!(
        registry
            .stage_fragments(stage_request(&request, 5, &expected))
            .outcome(),
        QueryStageOutcome::RejectedConflict
    );

    let start = start_request(&request, &stage);
    assert_eq!(
        registry.start_prepared_query(start.clone()).outcome(),
        QueryStartOutcome::Applied
    );
    assert_eq!(
        registry.phase(request.manifest().execution_id()),
        Some(QueryLifecyclePhase::Running)
    );
    assert_eq!(
        registry.start_prepared_query(start).outcome(),
        QueryStartOutcome::AlreadyStarted
    );
}

#[test]
fn stage_requires_matching_manifest_exact_set_and_control_attachment() {
    let expected = [UniqueId::new(9, 1)];
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let request = fragment_init_request_fixture(1_802, &expected);
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );

    assert_eq!(
        registry
            .stage_fragments(stage_request(&request, 1, &expected))
            .outcome(),
        QueryStageOutcome::RejectedInvalidState
    );
    let _attachment = attach_control(&registry, &request);
    assert_eq!(
        registry
            .stage_fragments(stage_request(&request, 1, &[]))
            .outcome(),
        QueryStageOutcome::RejectedInvalidBatch
    );

    let mismatched_digest = QueryStageRequest::new(
        protocol_execution_id(request.manifest().execution_id()),
        novarocks_proto::lifecycle::ParticipantManifestDigest::new([7; 32]),
        StageDigestVersion::V1,
        StageDigest::compute_v1(
            protocol_execution_id(request.manifest().execution_id()),
            novarocks_proto::lifecycle::ParticipantManifestDigest::new([7; 32]),
            &expected
                .iter()
                .copied()
                .map(|instance| stage_fragment(instance, 1))
                .collect::<Vec<_>>(),
        )
        .expect("well formed mismatched stage digest"),
        expected
            .iter()
            .copied()
            .map(|instance| stage_fragment(instance, 1))
            .collect(),
    )
    .expect("well formed mismatched stage request");
    assert_eq!(
        registry.stage_fragments(mismatched_digest).outcome(),
        QueryStageOutcome::RejectedConflict
    );
}

#[test]
fn service_only_empty_stage_starts_and_abort_prevents_late_start() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let request = init_request_fixture(1_803, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let _attachment = attach_control(&registry, &request);
    let stage = stage_request(&request, 6, &[]);
    assert_eq!(
        registry.stage_fragments(stage.clone()).outcome(),
        QueryStageOutcome::Applied
    );
    registry
        .abort_query(
            QueryAbortRequest::new(
                request.manifest().execution_id(),
                request.digest().expect("validated init digest"),
                "abort staged service participant",
            )
            .expect("valid abort"),
        )
        .expect("abort accepted");
    assert_eq!(
        registry
            .start_prepared_query(start_request(&request, &stage))
            .outcome(),
        QueryStartOutcome::RejectedTerminated
    );
}

#[test]
fn stage_resource_ledger_rejects_second_staged_bundle_and_releases_on_start() {
    let expected = [UniqueId::new(13, 1)];
    let first = fragment_init_request_fixture(1_804, &expected);
    let second = fragment_init_request_fixture(1_805, &expected);
    let first_stage = stage_request(&first, 13, &expected);
    let encoded_bytes = first_stage.as_proto().encoded_len();
    let mut config = registry_config(8);
    config.stage_max_fragments = 1;
    config.stage_max_encoded_bytes = encoded_bytes;
    config.stage_max_inflight_encoded_bytes = encoded_bytes;
    config.stage_max_dormant_workers = 1;
    let registry = registry_with_config(RecordingLocalRuntime::default(), config);

    for request in [&first, &second] {
        assert_eq!(
            registry
                .init_query(request.clone())
                .outcome()
                .expect("validated lifecycle acknowledgement"),
            QueryInitOutcome::Applied
        );
        let _attachment = attach_control(&registry, request);
    }

    assert_eq!(
        registry.stage_fragments(first_stage.clone()).outcome(),
        QueryStageOutcome::Applied
    );
    assert_eq!(
        registry
            .stage_fragments(stage_request(&second, 14, &expected))
            .outcome(),
        QueryStageOutcome::RejectedCapacity
    );

    assert_eq!(
        registry
            .start_prepared_query(start_request(&first, &first_stage))
            .outcome(),
        QueryStartOutcome::Applied
    );
    assert_eq!(
        registry
            .stage_fragments(stage_request(&second, 14, &expected))
            .outcome(),
        QueryStageOutcome::Applied
    );
}

#[test]
fn stage_builder_limit_is_held_until_commit_or_drop() {
    let expected = [UniqueId::new(14, 1)];
    let first = fragment_init_request_fixture(1_806, &expected);
    let second = fragment_init_request_fixture(1_807, &expected);
    let mut config = registry_config(8);
    config.max_active_staging = 1;
    let registry = registry_with_config(RecordingLocalRuntime::default(), config);

    for request in [&first, &second] {
        assert_eq!(
            registry
                .init_query(request.clone())
                .outcome()
                .expect("validated lifecycle acknowledgement"),
            QueryInitOutcome::Applied
        );
        let _attachment = attach_control(&registry, request);
    }

    let permit = match registry.begin_stage(stage_request(&first, 15, &expected)) {
        StageBuildDecision::Build(permit) => permit,
        StageBuildDecision::Complete(ack) => panic!("first Stage must reserve a builder: {ack:?}"),
    };
    assert_eq!(
        match registry.begin_stage(stage_request(&second, 16, &expected)) {
            StageBuildDecision::Build(_) => QueryStageOutcome::Applied,
            StageBuildDecision::Complete(ack) => ack.outcome(),
        },
        QueryStageOutcome::RejectedCapacity
    );
    drop(permit);
    assert!(matches!(
        registry.begin_stage(stage_request(&second, 16, &expected)),
        StageBuildDecision::Build(_)
    ));
}

#[test]
fn query_lifecycle_registry_same_digest_init_is_idempotent_and_installs_once() {
    let runtime = RecordingLocalRuntime::default();
    let registry = registry_with(runtime.clone(), 8);
    let request = init_request_fixture(1, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);

    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    assert_eq!(
        registry
            .init_query(request)
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::AlreadyApplied
    );
    assert_eq!(runtime.runtime_filter_install_calls(), 1);
}

#[test]
fn query_lifecycle_abort_digest_mismatch_keeps_live_entry_attachable() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let request = init_request_fixture(101, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let different = init_request_fixture(102, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );

    assert_eq!(
        registry
            .abort_query(
                QueryAbortRequest::new(
                    request.manifest().execution_id(),
                    different.digest().expect("validated init digest"),
                    "mismatched digest must not terminate",
                )
                .expect("valid mismatched abort request"),
            )
            .expect_err("digest mismatch is rejected")
            .code(),
        QueryLifecycleErrorCode::Conflict
    );

    registry
        .attach_control(
            QueryControlAttach::new(
                request.manifest().execution_id(),
                request.digest().expect("validated init digest"),
                1,
            )
            .expect("valid control attach"),
        )
        .expect("digest mismatch must leave the live entry attachable");
}

#[test]
fn query_lifecycle_terminal_event_survives_saturated_heartbeat_queue() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let request = init_request_fixture(103, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let mut attachment = attach_control(&registry, &request);

    // ControlReady plus sixteen ACKs saturate the normal event budget while the
    // dedicated terminal permit remains reserved.
    for sequence in 1..=16 {
        attachment
            .control
            .heartbeat(sequence)
            .expect("heartbeat ACK fits the normal event budget");
    }
    attachment
        .control
        .abort("saturated event queue".to_string())
        .expect("abort is accepted despite ACK backpressure");

    let mut events = Vec::new();
    while let Ok(event) = try_recv_event(&mut attachment) {
        events.push(event);
    }
    assert!(
        events.iter().any(|event| matches!(
            event.event,
            Some(proto_novarocks::query_control_response::Event::TerminationAccepted(ref accepted))
                if accepted.reason == proto_novarocks::QueryTerminationReason::QueryTerminationCoordinatorAbort as i32
        )),
        "terminal acceptance must not be dropped behind heartbeat ACKs: {events:?}"
    );
}

#[test]
fn query_lifecycle_observations_coalesce_without_consuming_correctness_capacity() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let fragment = UniqueId::new(81, 82);
    let request = fragment_init_request_fixture(181, &[fragment]);
    let current_execution_id = request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let mut attachment = attach_control(&registry, &request);

    assert!(registry.publish_fragment_observation(current_execution_id, fragment, 1, 2, 3, None));
    assert!(registry.publish_fragment_observation(current_execution_id, fragment, 4, 5, 6, None));
    assert!(
        attachment
            .observations
            .has_changed()
            .expect("observation sender lives")
    );
    let observation = attachment
        .observations
        .borrow_and_update()
        .clone()
        .expect("latest observation");
    assert_eq!(observation.sequence(), 2);
    assert_eq!(observation.input_rows(), 4);
    assert_eq!(observation.output_rows(), 5);
    assert_eq!(observation.elapsed_ms(), 6);

    assert_control_ready(&mut attachment);
    assert!(!registry.publish_fragment_observation(
        execution_id(181, ATTEMPT_1 + 1),
        fragment,
        0,
        0,
        0,
        None,
    ));
    assert!(!registry.publish_fragment_observation(
        current_execution_id,
        UniqueId::new(90, 91),
        0,
        0,
        0,
        None,
    ));
}

#[test]
fn query_lifecycle_drain_and_snapshot_survive_saturated_heartbeat_queue() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let request = init_request_fixture(104, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let mut attachment = attach_control(&registry, &request);
    let stage = stage_request(&request, 104, &[]);
    assert_eq!(
        registry.stage_fragments(stage.clone()).outcome(),
        QueryStageOutcome::Applied
    );

    // ControlReady and the three reserved correctness permits leave exactly
    // the normal sixteen-event heartbeat budget available.
    for sequence in 1..=16 {
        attachment
            .control
            .heartbeat(sequence)
            .expect("heartbeat ACK fits the normal event budget");
    }
    assert_eq!(
        registry
            .start_prepared_query(start_request(&request, &stage))
            .outcome(),
        QueryStartOutcome::Applied
    );
    let mut saw_local_drained = false;
    while let Ok(event) = try_recv_event(&mut attachment) {
        saw_local_drained |= matches!(
            event.event,
            Some(proto_novarocks::query_control_response::Event::LocalDrained(_))
        );
    }
    assert!(
        saw_local_drained,
        "LocalDrained must use its reserved correctness permit"
    );

    for sequence in 17..=32 {
        attachment
            .control
            .heartbeat(sequence)
            .expect("heartbeat ACK fits the normal event budget");
    }
    attachment
        .control
        .finalize()
        .expect("locally drained participant finalizes");
    let outcome = loop {
        match try_recv_event(&mut attachment) {
            Ok(proto_novarocks::QueryControlResponse {
                event:
                    Some(proto_novarocks::query_control_response::Event::TerminalOutcome(outcome)),
            }) => break outcome,
            Ok(_) => {}
            Err(error) => {
                panic!("TerminalSnapshot must use its reserved correctness permit: {error}")
            }
        }
    };
    attachment
        .control
        .terminal_ack(terminal_ack_from_outcome(outcome))
        .expect("terminal snapshot ACK");
}

#[test]
fn query_lifecycle_registry_different_digest_conflicts() {
    let runtime = RecordingLocalRuntime::default();
    let registry = registry_with(runtime.clone(), 8);

    assert_eq!(
        registry
            .init_query(init_request_fixture(
                2,
                ATTEMPT_1,
                LOCAL_START_EPOCH,
                10_000,
            ))
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    assert_eq!(
        registry
            .init_query(init_request_fixture(
                2,
                ATTEMPT_1,
                LOCAL_START_EPOCH,
                20_000,
            ))
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::RejectedConflict
    );
    assert_eq!(runtime.runtime_filter_install_calls(), 1);
}

#[test]
fn query_lifecycle_registry_capacity_rejects_without_install() {
    let runtime = RecordingLocalRuntime::default();
    let registry = registry_with(runtime.clone(), 1);

    assert_eq!(
        registry
            .init_query(init_request_fixture(
                3,
                ATTEMPT_1,
                LOCAL_START_EPOCH,
                10_000,
            ))
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    assert_eq!(
        registry
            .init_query(init_request_fixture(
                4,
                ATTEMPT_1,
                LOCAL_START_EPOCH,
                10_000,
            ))
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::RejectedCapacity
    );
    assert_eq!(runtime.runtime_filter_install_calls(), 1);
}

#[test]
fn query_lifecycle_registry_backend_epoch_mismatch_rejects() {
    let runtime = RecordingLocalRuntime::default();
    let registry = registry_with(runtime.clone(), 8);

    assert_eq!(
        registry
            .init_query(init_request_fixture(
                5,
                ATTEMPT_1,
                LOCAL_START_EPOCH + 1,
                10_000,
            ))
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::RejectedStaleBackend
    );
    assert_eq!(runtime.runtime_filter_install_calls(), 0);
}

#[test]
fn query_lifecycle_registry_unbound_application_identity_rejects_init() {
    let runtime = RecordingLocalRuntime::default();
    let registry = QueryLifecycleRegistry::new_unbound(
        LOCAL_START_EPOCH,
        Arc::new(runtime.clone()),
        registry_config(8),
    );

    assert_eq!(
        registry
            .init_query(init_request_fixture(
                51,
                ATTEMPT_1,
                LOCAL_START_EPOCH,
                10_000,
            ))
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::RejectedStaleBackend
    );
    assert_eq!(runtime.runtime_filter_install_calls(), 0);
}

#[test]
fn query_lifecycle_init_abort_race_never_publishes_initialized_and_rolls_back_once() {
    let runtime = RecordingLocalRuntime::default();
    runtime.block_install();
    let registry = registry_with(runtime.clone(), 8);
    let request = init_request_fixture(6, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let execution_id = request.manifest().execution_id();
    let digest = request.digest().expect("validated init digest");

    let init_registry = Arc::clone(&registry);
    let init_thread = std::thread::spawn(move || init_registry.init_query(request));
    runtime.wait_until_install_enters();

    let termination = registry
        .abort_query(
            QueryAbortRequest::new(execution_id, digest, "cancel init race")
                .expect("valid abort request"),
        )
        .expect("abort is accepted");
    assert_eq!(
        termination
            .accepted_reason()
            .expect("validated termination acknowledgement"),
        QueryTerminationReason::CoordinatorAbort
    );
    runtime.release_install();

    assert_eq!(
        init_thread
            .join()
            .expect("init thread")
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::RejectedTerminated
    );
    assert_eq!(runtime.runtime_filter_abort_calls(), 1);
    assert_eq!(
        registry.phase(execution_id),
        Some(QueryLifecyclePhase::Tombstone)
    );
    assert!(!registry.was_ever_initialized(execution_id));
}

#[test]
fn query_lifecycle_initializing_to_terminating_publishes_metrics_immediately() {
    let runtime = RecordingLocalRuntime::default();
    runtime.block_install();
    let metrics = Arc::new(RecordingMetricsSink::default());
    let registry =
        QueryLifecycleRegistry::new_with_clock_metrics_terminal_fallback_and_runtime_filter_factory(
            LOCAL_BACKEND_ID,
            LOCAL_START_EPOCH,
            Arc::new(runtime.clone()),
            registry_config(8),
            Arc::new(ManualClock::default()),
            Arc::clone(&metrics) as Arc<dyn QueryLifecycleMetricsSink>,
            Arc::new(RejectedTerminalFallback),
            Arc::new(runtime.clone()),
        );
    let request = init_request_fixture(7, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let execution_id = request.manifest().execution_id();
    let digest = request.digest().expect("validated init digest");

    let init_registry = Arc::clone(&registry);
    let init_thread = std::thread::spawn(move || init_registry.init_query(request));
    runtime.wait_until_install_enters();
    assert_eq!(metrics.last_snapshot().initializing, 1);

    registry
        .abort_query(
            QueryAbortRequest::new(execution_id, digest, "metrics while init blocks")
                .expect("valid abort"),
        )
        .expect("abort is accepted");
    let terminating = metrics.last_snapshot();
    assert_eq!(terminating.initializing, 0);
    assert_eq!(terminating.terminating, 1);
    assert_eq!(terminating.tombstones, 0);

    runtime.release_install();
    init_thread.join().expect("init thread");
}

#[test]
fn query_lifecycle_admission_requires_control_ready_and_commits_exactly_once() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let expected = UniqueId::new(71, 1);
    let request = fragment_init_request_fixture(71, &[expected]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );

    assert_eq!(
        registry
            .admit_fragment(execution_id, expected)
            .expect_err("fragment before ControlReady must fail")
            .code(),
        QueryLifecycleErrorCode::Conflict
    );

    let mut attachment = attach_control(&registry, &request);
    assert_control_ready(&mut attachment);
    registry
        .admit_fragment(execution_id, expected)
        .expect("exact fragment is admitted")
        .commit()
        .expect("fragment admission commits");
    assert_eq!(
        registry
            .admit_fragment(execution_id, expected)
            .expect_err("accepted fragment cannot be admitted twice")
            .code(),
        QueryLifecycleErrorCode::Conflict
    );
}

#[test]
fn query_lifecycle_admission_rejects_outside_set_and_service_only_participant() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let expected = UniqueId::new(72, 1);
    let unexpected = UniqueId::new(72, 2);
    let fragment_request = fragment_init_request_fixture(72, &[expected]);
    let fragment_execution = fragment_request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(fragment_request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let _fragment_control = attach_control(&registry, &fragment_request);
    assert_eq!(
        registry
            .admit_fragment(fragment_execution, unexpected)
            .expect_err("fragment outside exact set must fail")
            .code(),
        QueryLifecycleErrorCode::InvalidManifest
    );

    let service_request = init_request_fixture(73, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let service_execution = service_request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(service_request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let _service_control = attach_control(&registry, &service_request);
    assert_eq!(
        registry
            .admit_fragment(service_execution, expected)
            .expect_err("service-only participant cannot admit fragments")
            .code(),
        QueryLifecycleErrorCode::InvalidManifest
    );
}

#[test]
fn query_lifecycle_admission_dropped_permit_rolls_back_in_flight() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let expected = UniqueId::new(74, 1);
    let request = fragment_init_request_fixture(74, &[expected]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let _control = attach_control(&registry, &request);

    drop(
        registry
            .admit_fragment(execution_id, expected)
            .expect("first permit"),
    );
    registry
        .admit_fragment(execution_id, expected)
        .expect("dropped permit releases in-flight slot")
        .commit()
        .expect("fragment admission commits");
}

#[test]
fn query_lifecycle_admission_commit_does_not_hold_entry_while_waiting_for_registry() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let expected = UniqueId::new(741, 1);
    let request = fragment_init_request_fixture(741, &[expected]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let _control = attach_control(&registry, &request);
    let permit = registry
        .admit_fragment(execution_id, expected)
        .expect("fragment permit");
    let entry = permit.entry_for_test();

    let registry_acquired = Arc::new(Barrier::new(2));
    let release_registry = Arc::new(Barrier::new(2));
    let holder_registry = Arc::clone(&registry);
    let holder_acquired = Arc::clone(&registry_acquired);
    let holder_release = Arc::clone(&release_registry);
    let holder = std::thread::spawn(move || {
        holder_registry.hold_registry_state_lock_for_test(&holder_acquired, &holder_release);
    });
    registry_acquired.wait();

    let commit_started = Arc::new(Barrier::new(2));
    let commit_started_thread = Arc::clone(&commit_started);
    let commit = std::thread::spawn(move || {
        commit_started_thread.wait();
        permit.commit()
    });
    commit_started.wait();

    let deadline = Instant::now() + Duration::from_millis(250);
    let mut entry_was_locked = false;
    while Instant::now() < deadline {
        match entry.state.try_lock() {
            Ok(state) => drop(state),
            Err(TryLockError::WouldBlock) => {
                entry_was_locked = true;
                break;
            }
            Err(TryLockError::Poisoned(error)) => {
                panic!("query lifecycle entry lock poisoned: {error}")
            }
        }
        std::thread::yield_now();
    }

    release_registry.wait();
    holder.join().expect("registry lock holder");
    commit
        .join()
        .expect("fragment commit thread")
        .expect("fragment admission commits");
    assert!(
        !entry_was_locked,
        "fragment commit must acquire the registry lock before the entry lock"
    );
}

#[test]
fn query_lifecycle_registry_abort_rejects_late_permit_commit() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let expected = UniqueId::new(75, 1);
    let request = fragment_init_request_fixture(75, &[expected]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let _control = attach_control(&registry, &request);
    let permit = registry
        .admit_fragment(execution_id, expected)
        .expect("fragment permit");

    registry
        .abort_query(
            QueryAbortRequest::new(
                execution_id,
                request.digest().expect("validated init digest"),
                "abort before permit commit",
            )
            .expect("valid abort"),
        )
        .expect("abort is accepted");

    assert_eq!(
        permit
            .commit()
            .expect_err("late permit commit must not authorize fragment start")
            .code(),
        QueryLifecycleErrorCode::Terminated
    );
    assert_eq!(
        registry
            .admit_fragment(execution_id, expected)
            .expect_err("abort must reject every later fragment request")
            .code(),
        QueryLifecycleErrorCode::Terminated
    );
}

#[test]
fn fragment_failure_emits_query_local_failure() {
    let runtime = RecordingLocalRuntime::default();
    let registry = registry_with(runtime.clone(), 8);
    let expected = UniqueId::new(76, 1);
    let request = fragment_init_request_fixture(76, &[expected]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let mut attachment = attach_control(&registry, &request);
    assert_control_ready(&mut attachment);
    registry
        .admit_fragment(execution_id, expected)
        .expect("fragment permit")
        .commit()
        .expect("fragment admission commits");

    registry.record_fragment_terminal(
        execution_id,
        expected,
        &FragmentOutcome::Failed(FragmentExecutionError::new(
            FragmentExecutionErrorKind::Pipeline,
            "pipeline worker failed",
        )),
    );

    assert!(matches!(
        try_recv_event(&mut attachment).expect("LocalFailure event").event,
        Some(proto_novarocks::query_control_response::Event::LocalFailure(ref failure))
            if failure.code == "FRAGMENT_EXECUTION_FAILED"
                && failure.detail == "fragment execution error (pipeline): pipeline worker failed"
    ));
    assert_eq!(
        registry.termination_reason(execution_id),
        Some(QueryTerminationReason::LocalFailure)
    );
    assert_eq!(
        runtime
            .state
            .terminations
            .lock()
            .expect("terminations")
            .as_slice(),
        &[(
            execution_id,
            vec![expected],
            QueryTerminationReason::LocalFailure,
            "fragment execution error (pipeline): pipeline worker failed".to_string(),
        )]
    );
}

#[test]
fn running_fragment_failure_drains_and_freezes_a_failed_terminal_snapshot() {
    let runtime = RecordingLocalRuntime::default();
    let mut config = registry_config(8);
    config.terminal_drain_timeout = Duration::from_millis(1);
    let registry = registry_with_config(runtime, config);
    let expected = UniqueId::new(76, 2);
    let request = fragment_init_request_fixture(76_002, &[expected]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let mut attachment = attach_control(&registry, &request);
    assert_control_ready(&mut attachment);
    registry
        .admit_fragment(execution_id, expected)
        .expect("fragment permit")
        .commit()
        .expect("fragment admission commits");

    registry.record_fragment_terminal(
        execution_id,
        expected,
        &FragmentOutcome::Failed(FragmentExecutionError::new(
            FragmentExecutionErrorKind::Pipeline,
            "pipeline worker failed",
        )),
    );

    assert!(matches!(
        try_recv_event(&mut attachment)
            .expect("LocalFailure event")
            .event,
        Some(proto_novarocks::query_control_response::Event::LocalFailure(_))
    ));
    let deadline = Instant::now() + Duration::from_secs(1);
    let event = loop {
        match try_recv_event(&mut attachment) {
            Ok(event) => break event,
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("failed terminal snapshot is not delivered after drain: {error}"),
        }
    };
    let Some(proto_novarocks::query_control_response::Event::TerminalOutcome(outcome)) =
        event.event
    else {
        panic!("expected failed terminal snapshot");
    };
    let Some(snapshot) = outcome.snapshot else {
        panic!("terminal proof must carry a snapshot");
    };
    assert_eq!(
        snapshot
            .execution_id
            .expect("snapshot execution id")
            .query_id
            .expect("query id")
            .lo,
        execution_id.query_id().low()
    );
    assert_eq!(
        snapshot.fragments.first().expect("one fragment").outcome,
        proto_novarocks::QueryTerminalFragmentOutcome::Failed as i32
    );
    let metrics = registry.metrics_snapshot();
    assert_eq!(metrics.terminal_facts, 1);
    assert_eq!(metrics.terminal_records_frozen, 1);
    assert_eq!(metrics.terminal_locally_drained, 0);
}

#[test]
fn terminal_p1_faults_keep_the_attestation_delivery_permit() {
    for (query_low, fault, expected_reason) in [
        (
            76_101,
            QueryLifecycleFaultKind::TerminalP1EncodeFailure,
            NegativeAttestationReason::CorrectnessEvidenceEncodingFailed,
        ),
        (
            76_102,
            QueryLifecycleFaultKind::TerminalP1RetentionExhausted,
            NegativeAttestationReason::CorrectnessEvidenceRetentionExhausted,
        ),
    ] {
        let registry = registry_with(RecordingLocalRuntime::default(), 8);
        let expected = UniqueId::new(query_low, 1);
        let request = fragment_init_request_fixture(query_low, &[expected]);
        let execution_id = request.manifest().execution_id();
        assert_eq!(
            registry
                .init_query(request.clone())
                .outcome()
                .expect("validated lifecycle acknowledgement"),
            QueryInitOutcome::Applied
        );
        let mut attachment = attach_control(&registry, &request);
        assert_control_ready(&mut attachment);
        registry.inject_terminal_fault_for_test(execution_id, fault);
        registry
            .admit_fragment(execution_id, expected)
            .expect("fragment permit")
            .commit()
            .expect("fragment admission commits");
        registry.record_fragment_terminal(
            execution_id,
            expected,
            &FragmentOutcome::Failed(FragmentExecutionError::new(
                FragmentExecutionErrorKind::Pipeline,
                "inject terminal P1 fault",
            )),
        );

        let deadline = Instant::now() + Duration::from_secs(1);
        let outcome = loop {
            match try_recv_event(&mut attachment) {
                Ok(proto_novarocks::QueryControlResponse {
                    event:
                        Some(proto_novarocks::query_control_response::Event::TerminalOutcome(outcome)),
                }) => break outcome,
                Ok(_) => {}
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => panic!("terminal attestation was not delivered: {error}"),
            }
        };
        let Some(proto_novarocks::participant_terminal_outcome::Outcome::NegativeAttestation(
            attestation,
        )) = outcome.outcome.as_ref()
        else {
            panic!("P1 failure must deliver a negative attestation, got {outcome:?}");
        };
        assert_eq!(
            attestation.reason,
            match expected_reason {
                NegativeAttestationReason::Unspecified => {
                    proto_novarocks::NegativeAttestationReason::Unspecified as i32
                }
                NegativeAttestationReason::AttemptAborted => proto_novarocks::NegativeAttestationReason::AttemptAborted as i32,
                NegativeAttestationReason::AttemptTombstoned => proto_novarocks::NegativeAttestationReason::AttemptTombstoned as i32,
                NegativeAttestationReason::TerminalStateInvalid => proto_novarocks::NegativeAttestationReason::TerminalStateInvalid as i32,
                NegativeAttestationReason::CorrectnessEvidenceEncodingFailed => proto_novarocks::NegativeAttestationReason::CorrectnessEvidenceEncodingFailed as i32,
                NegativeAttestationReason::CorrectnessEvidenceRetentionExhausted => proto_novarocks::NegativeAttestationReason::CorrectnessEvidenceRetentionExhausted as i32,
            }
        );
        assert_eq!(
            registry.metrics_snapshot().terminal_retained,
            1,
            "the attach-time P0 permit must remain retained for attestation delivery"
        );
        attachment
            .control
            .terminal_ack(terminal_ack_from_outcome(outcome))
            .expect("attestation ACK releases the retained P0 permit");
        assert_eq!(registry.metrics_snapshot().terminal_retained, 0);
    }
}

#[test]
fn injected_p2_faults_keep_terminal_proof_and_publish_typed_unavailability() {
    for (query_low, fault, expected_code) in [
        (
            76_111,
            QueryLifecycleFaultKind::ObservationP2AssemblyFailure,
            "INJECTED_P2_ASSEMBLY_FAILURE",
        ),
        (
            76_112,
            QueryLifecycleFaultKind::ObservationP2BudgetPressure,
            "INJECTED_P2_BUDGET_PRESSURE",
        ),
    ] {
        let registry = registry_with(RecordingLocalRuntime::default(), 8);
        let fragment = UniqueId::new(query_low, 1);
        let request = fragment_runtime_filter_init_request_fixture(query_low, &[fragment]);
        let execution_id = request.manifest().execution_id();
        assert_eq!(
            registry
                .init_query(request.clone())
                .outcome()
                .expect("validated lifecycle acknowledgement"),
            QueryInitOutcome::Applied
        );
        let mut attachment = attach_control(&registry, &request);
        assert_control_ready(&mut attachment);
        registry.inject_terminal_fault_for_test(execution_id, fault);
        registry
            .admit_fragment(execution_id, fragment)
            .expect("fragment permit")
            .commit()
            .expect("fragment admission commits");
        registry.record_fragment_terminal(
            execution_id,
            fragment,
            &FragmentOutcome::Failed(FragmentExecutionError::new(
                FragmentExecutionErrorKind::Pipeline,
                "inject optional P2 fault",
            )),
        );

        let outcome = wait_for_terminal_outcome(&mut attachment);
        let snapshot = outcome.snapshot.expect("terminal proof has a snapshot");
        let Some(
            proto_novarocks::query_terminal_profile_contribution_telemetry::Telemetry::Unavailable(
                unavailable,
            ),
        ) = snapshot
            .profile_contribution
            .expect("P2 telemetry is present")
            .telemetry
        else {
            panic!("P2 fault is encoded as typed unavailable telemetry");
        };
        assert_eq!(unavailable.stage, "runtime_filter_terminal_capture");
        assert_eq!(unavailable.code, expected_code);
        assert_eq!(registry.metrics_snapshot().terminal_records_frozen, 1);
    }
}

#[test]
fn injected_terminal_stream_drops_use_unary_fallback_without_losing_outcomes() {
    for (query_low, faults, expect_attestation) in [
        (
            76_121,
            vec![QueryLifecycleFaultKind::TerminalProofStreamDrop],
            false,
        ),
        (
            76_122,
            vec![
                QueryLifecycleFaultKind::TerminalP1EncodeFailure,
                QueryLifecycleFaultKind::TerminalAttestationStreamDrop,
            ],
            true,
        ),
    ] {
        let runtime = RecordingLocalRuntime::default();
        let mut config = registry_config(8);
        config.terminal_ack_timeout = Duration::from_millis(1);
        let fallback = Arc::new(AcceptedTerminalFallback::default());
        let registry = QueryLifecycleRegistry::new_with_clock_metrics_and_terminal_fallback(
            LOCAL_BACKEND_ID,
            LOCAL_START_EPOCH,
            Arc::new(runtime),
            config,
            Arc::new(ManualClock::default()),
            Arc::new(RecordingMetricsSink::default()),
            Arc::clone(&fallback) as Arc<dyn QueryTerminalFallbackTransport>,
        );
        let fragment = UniqueId::new(query_low, 1);
        let request = fragment_init_request_fixture(query_low, &[fragment]);
        let execution_id = request.manifest().execution_id();
        assert_eq!(
            registry
                .init_query(request.clone())
                .outcome()
                .expect("validated lifecycle acknowledgement"),
            QueryInitOutcome::Applied
        );
        let mut attachment = attach_control(&registry, &request);
        assert_control_ready(&mut attachment);
        for fault in faults {
            registry.inject_terminal_fault_for_test(execution_id, fault);
        }
        registry
            .admit_fragment(execution_id, fragment)
            .expect("fragment permit")
            .commit()
            .expect("fragment admission commits");
        registry.record_fragment_terminal(
            execution_id,
            fragment,
            &FragmentOutcome::Failed(FragmentExecutionError::new(
                FragmentExecutionErrorKind::Pipeline,
                "inject terminal stream drop",
            )),
        );

        let deadline = Instant::now() + Duration::from_secs(1);
        while registry.metrics_snapshot().terminal_fallback_accepted == 0
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(registry.metrics_snapshot().terminal_fallback_accepted, 1);
        let outcomes = fallback.outcomes();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(
            outcomes
                .first()
                .expect("one unary fallback outcome")
                .negative_attestation()
                .is_some(),
            expect_attestation
        );
        while let Ok(event) = try_recv_event(&mut attachment) {
            assert!(
                !matches!(
                    event.event,
                    Some(proto_novarocks::query_control_response::Event::TerminalOutcome(_))
                ),
                "injected stream drop must not publish a terminal outcome on the attached stream"
            );
        }
    }
}

#[test]
fn failure_drain_sweep_does_not_close_runtime_filter_before_terminal_capture() {
    let runtime = RecordingLocalRuntime::default();
    let mut config = registry_config(8);
    config.terminal_drain_timeout = Duration::from_secs(5);
    let clock = Arc::new(ManualClock::default());
    let registry =
        QueryLifecycleRegistry::new_with_clock_metrics_terminal_fallback_and_runtime_filter_factory(
            LOCAL_BACKEND_ID,
            LOCAL_START_EPOCH,
            Arc::new(runtime.clone()),
            config,
            Arc::clone(&clock) as Arc<dyn MonotonicClock>,
            Arc::new(RecordingMetricsSink::default()),
            Arc::new(RejectedTerminalFallback),
            Arc::new(runtime.clone()),
        );
    let failed = UniqueId::new(76, 20);
    let pending = UniqueId::new(76, 21);
    let request = fragment_runtime_filter_init_request_fixture(76_020, &[failed, pending]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let _attachment = attach_control(&registry, &request);
    for fragment in [failed, pending] {
        registry
            .admit_fragment(execution_id, fragment)
            .expect("fragment permit")
            .commit()
            .expect("fragment admission commits");
    }
    registry.record_fragment_terminal(
        execution_id,
        failed,
        &FragmentOutcome::Failed(FragmentExecutionError::new(
            FragmentExecutionErrorKind::Pipeline,
            "pipeline worker failed",
        )),
    );

    registry.sweep_expired(clock.now());
    assert_eq!(
        runtime.runtime_filter_abort_calls(),
        0,
        "sweep must leave the participant alive while failure drain is pending"
    );

    registry.record_fragment_terminal(execution_id, pending, &FragmentOutcome::Succeeded);
    wait_for_failed_terminal_freeze(&registry);
    assert_eq!(runtime.runtime_filter_abort_calls(), 1);
    assert_eq!(runtime.release_calls(), 1);
    assert_eq!(
        runtime.lifecycle_order(),
        vec![
            ("quiesce", execution_id),
            ("release", execution_id),
            ("close", execution_id),
        ],
        "terminal capture and retention must complete between quiesce and resource release"
    );
}

#[test]
fn spi5b_local_failure_then_coordinator_abort_acknowledges_the_abort_command() {
    let runtime = RecordingLocalRuntime::default();
    let registry = registry_with(runtime, 8);
    let expected = UniqueId::new(76, 3);
    let request = fragment_init_request_fixture(76_003, &[expected]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let mut attachment = attach_control(&registry, &request);
    assert_control_ready(&mut attachment);
    registry
        .admit_fragment(execution_id, expected)
        .expect("fragment permit")
        .commit()
        .expect("fragment admission commits");
    registry.record_fragment_terminal(
        execution_id,
        expected,
        &FragmentOutcome::Failed(FragmentExecutionError::new(
            FragmentExecutionErrorKind::Pipeline,
            "pipeline worker failed",
        )),
    );
    assert!(matches!(
        try_recv_event(&mut attachment)
            .expect("LocalFailure event")
            .event,
        Some(proto_novarocks::query_control_response::Event::LocalFailure(_))
    ));

    attachment
        .control
        .abort("coordinator observes local failure".to_string())
        .expect("coordinator abort is accepted");

    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match try_recv_event(&mut attachment) {
            Ok(proto_novarocks::QueryControlResponse {
                event:
                    Some(proto_novarocks::query_control_response::Event::TerminationAccepted(accepted)),
            }) => {
                assert_eq!(
                    accepted.reason,
                    proto_novarocks::QueryTerminationReason::QueryTerminationCoordinatorAbort
                        as i32
                );
                return;
            }
            Ok(_) => {}
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("coordinator abort acknowledgement was not delivered: {error}"),
        }
    }
}

#[test]
fn terminal_closeout_preserves_first_wins_termination_reason_metrics() {
    let runtime = RecordingLocalRuntime::default();
    let metrics = Arc::new(RecordingMetricsSink::default());
    let clock = Arc::new(ManualClock::default());
    let mut config = registry_config(8);
    config.terminal_retention = Duration::from_millis(1);
    let registry =
        QueryLifecycleRegistry::new_with_clock_metrics_terminal_fallback_and_runtime_filter_factory(
            LOCAL_BACKEND_ID,
            LOCAL_START_EPOCH,
            Arc::new(runtime.clone()),
            config,
            Arc::clone(&clock) as Arc<dyn MonotonicClock>,
            Arc::clone(&metrics) as Arc<dyn QueryLifecycleMetricsSink>,
            Arc::new(RejectedTerminalFallback),
            Arc::new(runtime),
        );

    let failed_fragment = UniqueId::new(76, 4);
    let failed_request = fragment_init_request_fixture(76_004, &[failed_fragment]);
    let failed_execution = failed_request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(failed_request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let mut failed_attachment = attach_control(&registry, &failed_request);
    registry
        .admit_fragment(failed_execution, failed_fragment)
        .expect("fragment permit")
        .commit()
        .expect("fragment admission commits");
    registry.record_fragment_terminal(
        failed_execution,
        failed_fragment,
        &FragmentOutcome::Failed(FragmentExecutionError::new(
            FragmentExecutionErrorKind::Pipeline,
            "pipeline worker failed",
        )),
    );
    let failed_outcome = wait_for_terminal_outcome(&mut failed_attachment);
    failed_attachment
        .control
        .terminal_ack(terminal_ack_from_outcome(failed_outcome))
        .expect("local-failure terminal snapshot ACK");
    assert_eq!(metrics.last_termination_reasons(), [0, 0, 0, 0, 1, 0]);

    let aborted_fragment = UniqueId::new(76, 5);
    let aborted_request = fragment_init_request_fixture(76_005, &[aborted_fragment]);
    let aborted_execution = aborted_request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(aborted_request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let mut aborted_attachment = attach_control(&registry, &aborted_request);
    registry
        .admit_fragment(aborted_execution, aborted_fragment)
        .expect("fragment permit")
        .commit()
        .expect("fragment admission commits");
    aborted_attachment
        .control
        .abort("coordinator cancellation".to_string())
        .expect("coordinator abort is accepted");
    registry.record_fragment_terminal(
        aborted_execution,
        aborted_fragment,
        &FragmentOutcome::Succeeded,
    );
    let aborted_outcome = wait_for_terminal_outcome(&mut aborted_attachment);
    aborted_attachment
        .control
        .terminal_ack(terminal_ack_from_outcome(aborted_outcome))
        .expect("coordinator-abort terminal snapshot ACK");
    assert_eq!(metrics.last_termination_reasons(), [1, 0, 0, 0, 1, 0]);

    let expired_fragment = UniqueId::new(76, 6);
    let expired_request = fragment_init_request_fixture(76_006, &[expired_fragment]);
    let expired_execution = expired_request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(expired_request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let mut expired_attachment = attach_control(&registry, &expired_request);
    registry
        .admit_fragment(expired_execution, expired_fragment)
        .expect("fragment permit")
        .commit()
        .expect("fragment admission commits");
    expired_attachment
        .control
        .abort("coordinator cancellation".to_string())
        .expect("coordinator abort is accepted");
    registry.record_fragment_terminal(
        expired_execution,
        expired_fragment,
        &FragmentOutcome::Succeeded,
    );
    let _expired_outcome = wait_for_terminal_outcome(&mut expired_attachment);
    clock.advance(Duration::from_millis(2));
    registry.sweep_expired(clock.now());
    assert_eq!(metrics.last_termination_reasons(), [2, 0, 0, 0, 1, 0]);
}

#[test]
fn coordinator_abort_immediately_retains_incomplete_drain_proof_for_admitted_participant() {
    let runtime = RecordingLocalRuntime::default();
    let registry = registry_with(runtime, 8);
    let expected = UniqueId::new(76, 7);
    let request = fragment_init_request_fixture(76_007, &[expected]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let mut attachment = attach_control(&registry, &request);
    assert_control_ready(&mut attachment);
    registry
        .admit_fragment(execution_id, expected)
        .expect("fragment permit")
        .commit()
        .expect("fragment admission commits");

    attachment
        .control
        .abort("coordinator cancellation".to_string())
        .expect("coordinator abort is accepted");

    let deadline = Instant::now() + Duration::from_secs(1);
    let outcome = loop {
        match try_recv_event(&mut attachment) {
            Ok(proto_novarocks::QueryControlResponse {
                event:
                    Some(proto_novarocks::query_control_response::Event::TerminalOutcome(outcome)),
            }) => break outcome,
            Ok(_) => {}
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("coordinator abort terminal proof was not delivered: {error}"),
        }
    };
    let Some(snapshot) = outcome.snapshot else {
        panic!("coordinator abort must retain a terminal proof");
    };
    assert_eq!(
        snapshot.fragments.first().expect("one fragment").outcome,
        proto_novarocks::QueryTerminalFragmentOutcome::IncompleteDrain as i32
    );
    assert_eq!(registry.metrics_snapshot().terminal_records_frozen, 1);
}

#[test]
fn query_lifecycle_registry_rejects_fragment_executor_without_exact_set() {
    let runtime = RecordingLocalRuntime::default();
    let registry = registry_with(runtime.clone(), 8);

    assert_eq!(
        registry
            .init_query(fragment_init_request_fixture(76, &[]))
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::RejectedInvalidManifest
    );
    assert_eq!(runtime.runtime_filter_install_calls(), 0);
}

#[test]
fn query_lifecycle_attach_distinguishes_duplicate_active_from_terminated() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let request = init_request_fixture(77, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let _control = attach_control(&registry, &request);
    let attach = QueryControlAttach::new(
        execution_id,
        request.digest().expect("validated init digest"),
        1,
    )
    .expect("valid control attach");

    let Err(duplicate_error) = registry.attach_control(attach.clone()) else {
        panic!("duplicate active attach must conflict");
    };
    assert_eq!(duplicate_error.code(), QueryLifecycleErrorCode::Conflict);
    registry
        .abort_query(
            QueryAbortRequest::new(
                execution_id,
                request.digest().expect("validated init digest"),
                "terminate before attach",
            )
            .expect("valid abort"),
        )
        .expect("abort is accepted");
    let Err(terminated_error) = registry.attach_control(attach) else {
        panic!("terminated attach must be terminal");
    };
    assert_eq!(terminated_error.code(), QueryLifecycleErrorCode::Terminated);
}

#[test]
fn query_lifecycle_tombstone_capacity_evicts_only_oldest_tombstone() {
    let runtime = RecordingLocalRuntime::default();
    let mut config = registry_config(8);
    config.tombstone_capacity = 2;
    let registry = QueryLifecycleRegistry::new_with_clock(
        LOCAL_BACKEND_ID,
        LOCAL_START_EPOCH,
        Arc::new(runtime),
        config,
        Arc::new(ManualClock::default()),
    );
    let active = init_request_fixture(80, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    assert_eq!(
        registry
            .init_query(active.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let mut terminated = Vec::new();
    for query_low in [81, 82, 83] {
        let request = init_request_fixture(query_low, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
        let execution_id = request.manifest().execution_id();
        assert_eq!(
            registry
                .init_query(request.clone())
                .outcome()
                .expect("validated lifecycle acknowledgement"),
            QueryInitOutcome::Applied
        );
        registry
            .abort_query(
                QueryAbortRequest::new(
                    execution_id,
                    request.digest().expect("validated init digest"),
                    "bounded tombstone",
                )
                .expect("valid abort"),
            )
            .expect("abort is accepted");
        terminated.push(execution_id);
    }

    assert!(registry.contains(active.manifest().execution_id()));
    assert!(!registry.contains(terminated[0]));
    assert!(registry.contains(terminated[1]));
    assert!(registry.contains(terminated[2]));
}

#[test]
fn query_lifecycle_tombstone_capacity_evicts_committed_fragment_mapping() {
    let runtime = RecordingLocalRuntime::default();
    let mut config = registry_config(8);
    config.tombstone_capacity = 1;
    config.terminal_drain_timeout = Duration::from_millis(1);
    config.terminal_retention = Duration::from_millis(1);
    let clock = Arc::new(ManualClock::default());
    let registry = QueryLifecycleRegistry::new_with_clock(
        LOCAL_BACKEND_ID,
        LOCAL_START_EPOCH,
        Arc::new(runtime.clone()),
        config,
        Arc::clone(&clock) as Arc<dyn MonotonicClock>,
    );
    let fragment_instance_id = UniqueId::new(811, 1);
    let first = fragment_init_request_fixture(811, &[fragment_instance_id]);
    let first_execution = first.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(first.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let _first_control = attach_control(&registry, &first);
    registry
        .admit_fragment(first_execution, fragment_instance_id)
        .expect("first fragment permit")
        .commit()
        .expect("first fragment admission commits");
    registry
        .abort_query(
            QueryAbortRequest::new(
                first_execution,
                first.digest().expect("validated init digest"),
                "first tombstone",
            )
            .expect("valid abort"),
        )
        .expect("first abort is accepted");
    wait_for_failed_terminal_freeze(&registry);
    clock.advance(Duration::from_millis(2));
    registry.sweep_expired(clock.now());

    let second = init_request_fixture(812, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let second_execution = second.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(second.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    registry
        .abort_query(
            QueryAbortRequest::new(
                second_execution,
                second.digest().expect("validated init digest"),
                "evict first tombstone",
            )
            .expect("valid abort"),
        )
        .expect("second abort is accepted");
    assert!(!registry.contains(first_execution));
    registry.record_fragment_terminal(
        first_execution,
        fragment_instance_id,
        &FragmentOutcome::Failed(FragmentExecutionError::new(
            FragmentExecutionErrorKind::Pipeline,
            "late terminal after lifecycle eviction",
        )),
    );
    assert_eq!(
        runtime
            .state
            .terminations
            .lock()
            .expect("terminations")
            .len(),
        2,
        "late terminal after eviction must not target another lifecycle"
    );

    let replacement = fragment_init_request_fixture(813, &[fragment_instance_id]);
    let replacement_execution = replacement.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(replacement.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let _replacement_control = attach_control(&registry, &replacement);
    registry
        .admit_fragment(replacement_execution, fragment_instance_id)
        .expect("evicted fragment mapping permits reuse")
        .commit()
        .expect("replacement fragment admission commits");
}

#[test]
fn late_terminal_from_evicted_execution_cannot_target_reused_fragment_instance() {
    let runtime = RecordingLocalRuntime::default();
    let mut config = registry_config(8);
    config.tombstone_capacity = 1;
    config.terminal_drain_timeout = Duration::from_millis(1);
    config.terminal_retention = Duration::from_millis(1);
    let clock = Arc::new(ManualClock::default());
    let registry = QueryLifecycleRegistry::new_with_clock(
        LOCAL_BACKEND_ID,
        LOCAL_START_EPOCH,
        Arc::new(runtime),
        config,
        Arc::clone(&clock) as Arc<dyn MonotonicClock>,
    );
    let fragment_instance_id = UniqueId::new(814, 1);
    let first = fragment_init_request_fixture(814, &[fragment_instance_id]);
    let first_execution = first.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(first.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let _first_control = attach_control(&registry, &first);
    registry
        .admit_fragment(first_execution, fragment_instance_id)
        .expect("first fragment permit")
        .commit()
        .expect("first fragment admission commits");
    registry
        .abort_query(
            QueryAbortRequest::new(
                first_execution,
                first.digest().expect("validated init digest"),
                "first tombstone",
            )
            .expect("valid abort"),
        )
        .expect("first abort is accepted");
    wait_for_failed_terminal_freeze(&registry);
    clock.advance(Duration::from_millis(2));
    registry.sweep_expired(clock.now());

    let eviction = init_request_fixture(815, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let eviction_execution = eviction.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(eviction.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    registry
        .abort_query(
            QueryAbortRequest::new(
                eviction_execution,
                eviction.digest().expect("validated init digest"),
                "evict first tombstone",
            )
            .expect("valid abort"),
        )
        .expect("eviction abort is accepted");
    assert!(!registry.contains(first_execution));

    let replacement = fragment_init_request_fixture(816, &[fragment_instance_id]);
    let replacement_execution = replacement.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(replacement.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let _replacement_control = attach_control(&registry, &replacement);
    registry
        .admit_fragment(replacement_execution, fragment_instance_id)
        .expect("replacement fragment permit")
        .commit()
        .expect("replacement fragment admission commits");

    registry.record_fragment_terminal(
        first_execution,
        fragment_instance_id,
        &FragmentOutcome::Failed(FragmentExecutionError::new(
            FragmentExecutionErrorKind::Pipeline,
            "late failure from evicted execution",
        )),
    );

    assert_eq!(registry.termination_reason(replacement_execution), None);
    let competing = fragment_init_request_fixture(817, &[fragment_instance_id]);
    let competing_execution = competing.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(competing.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let _competing_control = attach_control(&registry, &competing);
    assert_eq!(
        registry
            .admit_fragment(competing_execution, fragment_instance_id)
            .expect("competing fragment permit")
            .commit()
            .expect_err("replacement execution must retain the fragment mapping")
            .code(),
        QueryLifecycleErrorCode::Conflict
    );
}

#[test]
fn query_lifecycle_tombstone_releases_active_capacity() {
    let registry = registry_with(RecordingLocalRuntime::default(), 1);
    let first = init_request_fixture(84, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    assert_eq!(
        registry
            .init_query(first.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    registry
        .abort_query(
            QueryAbortRequest::new(
                first.manifest().execution_id(),
                first.digest().expect("validated init digest"),
                "release capacity",
            )
            .expect("valid abort"),
        )
        .expect("abort is accepted");

    assert_eq!(
        registry
            .init_query(init_request_fixture(
                85,
                ATTEMPT_1,
                LOCAL_START_EPOCH,
                10_000,
            ))
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
}

#[test]
fn query_lifecycle_tombstone_retention_reclaims_expired_tombstone_incrementally() {
    let clock = Arc::new(ManualClock::default());
    let mut config = registry_config(8);
    config.tombstone_retention = Duration::from_millis(10);
    let registry = QueryLifecycleRegistry::new_with_clock(
        LOCAL_BACKEND_ID,
        LOCAL_START_EPOCH,
        Arc::new(RecordingLocalRuntime::default()),
        config,
        Arc::clone(&clock) as Arc<dyn MonotonicClock>,
    );
    let terminated = init_request_fixture(86, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let terminated_id = terminated.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(terminated.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    registry
        .abort_query(
            QueryAbortRequest::new(
                terminated_id,
                terminated.digest().expect("validated init digest"),
                "retention",
            )
            .expect("valid abort"),
        )
        .expect("abort is accepted");
    assert!(registry.contains(terminated_id));

    clock.advance(Duration::from_millis(11));
    assert_eq!(
        registry
            .init_query(fragment_init_request_fixture(87, &[UniqueId::new(87, 1)],))
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    assert!(!registry.contains(terminated_id));
}

#[test]
fn query_lifecycle_tombstone_retention_evicts_committed_fragment_mapping() {
    let clock = Arc::new(ManualClock::default());
    let mut config = registry_config(8);
    config.tombstone_retention = Duration::from_millis(10);
    config.terminal_drain_timeout = Duration::from_millis(1);
    config.terminal_retention = Duration::from_millis(1);
    let registry = QueryLifecycleRegistry::new_with_clock(
        LOCAL_BACKEND_ID,
        LOCAL_START_EPOCH,
        Arc::new(RecordingLocalRuntime::default()),
        config,
        Arc::clone(&clock) as Arc<dyn MonotonicClock>,
    );
    let fragment_instance_id = UniqueId::new(861, 1);
    let first = fragment_init_request_fixture(861, &[fragment_instance_id]);
    let first_execution = first.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(first.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let _first_control = attach_control(&registry, &first);
    registry
        .admit_fragment(first_execution, fragment_instance_id)
        .expect("first fragment permit")
        .commit()
        .expect("first fragment admission commits");
    registry
        .abort_query(
            QueryAbortRequest::new(
                first_execution,
                first.digest().expect("validated init digest"),
                "retention cleanup",
            )
            .expect("valid abort"),
        )
        .expect("abort is accepted");
    wait_for_failed_terminal_freeze(&registry);

    clock.advance(Duration::from_millis(11));
    registry.sweep_expired(clock.now());
    // The first sweep converts the expired retained record into a tombstone
    // and starts the independently configured tombstone TTL at that moment.
    // Advance it before the next incremental sweep reclaims the mapping.
    clock.advance(Duration::from_millis(11));
    registry.sweep_expired(clock.now());
    assert!(!registry.contains(first_execution));

    let replacement = fragment_init_request_fixture(862, &[fragment_instance_id]);
    let replacement_execution = replacement.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(replacement.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let _replacement_control = attach_control(&registry, &replacement);
    registry
        .admit_fragment(replacement_execution, fragment_instance_id)
        .expect("expired fragment mapping permits reuse")
        .commit()
        .expect("replacement fragment admission commits");
}

#[test]
fn query_lifecycle_pre_start_timeout_terminates_fragment_participant_without_accept() {
    let runtime = RecordingLocalRuntime::default();
    let clock = Arc::new(ManualClock::default());
    let registry = registry_with_clock(runtime.clone(), 8, Arc::clone(&clock));
    let expected = UniqueId::new(90, 1);
    let request = fragment_init_request_fixture(90, &[expected]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let _control = attach_control(&registry, &request);

    clock.advance(Duration::from_millis(30_001));
    registry.sweep_expired(clock.now());

    assert_eq!(
        registry.phase(execution_id),
        Some(QueryLifecyclePhase::Tombstone)
    );
    assert_eq!(
        registry.termination_reason(execution_id),
        Some(QueryTerminationReason::PreStartTimeout)
    );
    assert_eq!(
        runtime
            .state
            .terminations
            .lock()
            .expect("terminations")
            .len(),
        1
    );
}

#[test]
fn query_lifecycle_pre_start_timeout_is_disarmed_by_first_accept_and_service_control_ready() {
    let clock = Arc::new(ManualClock::default());
    let registry = registry_with_clock(RecordingLocalRuntime::default(), 8, Arc::clone(&clock));
    let expected = UniqueId::new(91, 1);
    let fragment_request = fragment_init_request_fixture(91, &[expected]);
    let fragment_execution = fragment_request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(fragment_request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let fragment_control = attach_control(&registry, &fragment_request);
    registry
        .admit_fragment(fragment_execution, expected)
        .expect("fragment permit")
        .commit()
        .expect("fragment admission commits");

    let service_request = init_request_fixture(92, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let service_execution = service_request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(service_request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let service_control = attach_control(&registry, &service_request);

    clock.advance(Duration::from_millis(30_001));
    fragment_control
        .control
        .heartbeat(1)
        .expect("fragment control heartbeat");
    service_control
        .control
        .heartbeat(1)
        .expect("service control heartbeat");
    registry.sweep_expired(clock.now());
    assert_eq!(
        registry.phase(fragment_execution),
        Some(QueryLifecyclePhase::ControlAttached)
    );
    assert_eq!(
        registry.phase(service_execution),
        Some(QueryLifecyclePhase::ControlAttached)
    );
}

#[test]
fn query_lifecycle_heartbeat_timeout_terminates_control_attached_entry() {
    let runtime = RecordingLocalRuntime::default();
    let clock = Arc::new(ManualClock::default());
    let registry = registry_with_clock(runtime.clone(), 8, Arc::clone(&clock));
    let expected = UniqueId::new(99, 1);
    let request = fragment_init_request_fixture(99, &[expected]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let _control = attach_control(&registry, &request);

    clock.advance(Duration::from_millis(5_001));
    registry.sweep_expired(clock.now());

    assert_eq!(
        registry.termination_reason(execution_id),
        Some(QueryTerminationReason::CoordinatorHeartbeatTimeout)
    );
    assert_eq!(registry.metrics_snapshot().heartbeat_timeouts, 1);
    assert_eq!(
        runtime
            .state
            .terminations
            .lock()
            .expect("terminations")
            .len(),
        1
    );
}

#[test]
fn query_lifecycle_registry_metrics_follow_state_rejection_and_termination() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let expected = UniqueId::new(93, 1);
    let request = fragment_init_request_fixture(93, &[expected]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let initialized = registry.metrics_snapshot();
    assert_eq!(initialized.initialized, 1);
    assert_eq!(initialized.control_attached, 0);

    let _ = registry
        .admit_fragment(execution_id, expected)
        .expect_err("admission before control is rejected");
    assert_eq!(registry.metrics_snapshot().admission_rejected, 1);

    let _control = attach_control(&registry, &request);
    let attached = registry.metrics_snapshot();
    assert_eq!(attached.initialized, 0);
    assert_eq!(attached.control_attached, 1);

    registry
        .abort_query(
            QueryAbortRequest::new(
                execution_id,
                request.digest().expect("validated init digest"),
                "metrics termination",
            )
            .expect("valid abort"),
        )
        .expect("abort is accepted");
    let terminated = registry.metrics_snapshot();
    assert_eq!(terminated.control_attached, 0);
    assert_eq!(terminated.tombstones, 1);
    assert_eq!(terminated.terminations, 1);
}

#[test]
fn query_lifecycle_registry_termination_is_first_wins_and_runs_local_cleanup_once() {
    let runtime = RecordingLocalRuntime::default();
    let registry = registry_with(runtime.clone(), 8);
    let request = init_request_fixture(94, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    let mut attachment = attach_control(&registry, &request);

    attachment
        .control
        .abort("first reason".to_string())
        .expect("first abort");
    attachment.control.finalize().expect("repeated finalize");
    assert_control_ready(&mut attachment);
    for _ in 0..2 {
        assert!(matches!(
            try_recv_event(&mut attachment)
                .expect("termination accepted")
                .event,
            Some(proto_novarocks::query_control_response::Event::TerminationAccepted(ref accepted))
                if accepted.reason == proto_novarocks::QueryTerminationReason::QueryTerminationCoordinatorAbort as i32
        ));
    }
    assert_eq!(
        registry.termination_reason(execution_id),
        Some(QueryTerminationReason::CoordinatorAbort)
    );
    assert_eq!(
        runtime
            .state
            .terminations
            .lock()
            .expect("terminations")
            .len(),
        1
    );
    assert_eq!(runtime.runtime_filter_abort_calls(), 1);
}

#[test]
fn query_lifecycle_registry_same_digest_concurrent_init_is_single_flight() {
    let runtime = RecordingLocalRuntime::default();
    runtime.block_install();
    let registry = registry_with(runtime.clone(), 8);
    let request = init_request_fixture(95, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);

    let first_registry = Arc::clone(&registry);
    let first_request = request.clone();
    let first = std::thread::spawn(move || {
        first_registry
            .init_query(first_request)
            .outcome()
            .expect("validated lifecycle acknowledgement")
    });
    runtime.wait_until_install_enters();
    let second_registry = Arc::clone(&registry);
    let second = std::thread::spawn(move || {
        second_registry
            .init_query(request)
            .outcome()
            .expect("validated lifecycle acknowledgement")
    });
    runtime.release_install();

    assert_eq!(first.join().expect("first init"), QueryInitOutcome::Applied);
    assert_eq!(
        second.join().expect("second init"),
        QueryInitOutcome::AlreadyApplied
    );
    assert_eq!(runtime.runtime_filter_install_calls(), 1);
}

#[test]
fn query_lifecycle_registry_runtime_filter_install_failure_rolls_back_workspace() {
    let runtime = RecordingLocalRuntime::default();
    runtime.fail_install();
    let registry = registry_with(runtime.clone(), 1);
    let request = init_request_fixture(96, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let execution_id = request.manifest().execution_id();

    assert_eq!(
        registry
            .init_query(request)
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::RejectedInvalidManifest
    );
    assert_eq!(runtime.runtime_filter_install_calls(), 1);
    assert_eq!(runtime.runtime_filter_abort_calls(), 0);
    assert_eq!(
        registry.phase(execution_id),
        Some(QueryLifecyclePhase::Tombstone)
    );
    assert_eq!(
        registry
            .init_query(fragment_init_request_fixture(97, &[UniqueId::new(97, 1)],))
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
}

#[test]
fn query_lifecycle_runtime_filter_abort_failure_retains_capacity_until_sweep_retry() {
    let runtime = RecordingLocalRuntime::default();
    let clock = Arc::new(ManualClock::default());
    let registry = registry_with_clock(runtime.clone(), 1, Arc::clone(&clock));
    let request = init_request_fixture(961, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry
            .init_query(request.clone())
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
    runtime.fail_abort();

    registry
        .abort_query(
            QueryAbortRequest::new(
                execution_id,
                request.digest().expect("validated init digest"),
                "abort with cleanup failure",
            )
            .expect("valid abort"),
        )
        .expect("abort is accepted");

    assert_eq!(
        registry.phase(execution_id),
        Some(QueryLifecyclePhase::Terminating)
    );
    assert_eq!(registry.metrics_snapshot().terminating, 1);
    assert_eq!(registry.metrics_snapshot().tombstones, 0);
    assert_eq!(runtime.runtime_filter_abort_calls(), 1);
    assert_eq!(
        registry
            .init_query(fragment_init_request_fixture(962, &[UniqueId::new(962, 1)],))
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::RejectedCapacity
    );

    runtime.allow_abort();
    registry.sweep_expired(clock.now());

    assert_eq!(
        registry.phase(execution_id),
        Some(QueryLifecyclePhase::Tombstone)
    );
    assert_eq!(runtime.runtime_filter_abort_calls(), 2);
    assert_eq!(
        registry
            .init_query(fragment_init_request_fixture(963, &[UniqueId::new(963, 1)],))
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::Applied
    );
}

#[test]
fn query_lifecycle_install_failure_racing_abort_preserves_first_reason_without_participant() {
    let runtime = RecordingLocalRuntime::default();
    runtime.block_install();
    runtime.fail_install();
    let registry = registry_with(runtime.clone(), 8);
    let request = init_request_fixture(97, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let execution_id = request.manifest().execution_id();
    let digest = request.digest().expect("validated init digest");

    let init_registry = Arc::clone(&registry);
    let init_thread = std::thread::spawn(move || init_registry.init_query(request));
    runtime.wait_until_install_enters();
    assert_eq!(
        registry
            .abort_query(
                QueryAbortRequest::new(execution_id, digest, "abort failed install")
                    .expect("valid abort"),
            )
            .expect("abort is accepted")
            .accepted_reason()
            .expect("validated termination acknowledgement"),
        QueryTerminationReason::CoordinatorAbort
    );
    runtime.release_install();

    assert_eq!(
        init_thread
            .join()
            .expect("init thread")
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::RejectedInvalidManifest
    );
    assert_eq!(
        registry.termination_reason(execution_id),
        Some(QueryTerminationReason::CoordinatorAbort)
    );
    assert_eq!(
        registry.phase(execution_id),
        Some(QueryLifecyclePhase::Tombstone)
    );
    assert_eq!(runtime.runtime_filter_abort_calls(), 0);
    assert_eq!(
        runtime
            .state
            .terminations
            .lock()
            .expect("terminations")
            .len(),
        1
    );
}

#[test]
fn query_lifecycle_registry_abort_before_init_leaves_fail_closed_tombstone() {
    let runtime = RecordingLocalRuntime::default();
    let registry = registry_with(runtime.clone(), 8);
    let request = init_request_fixture(98, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let execution_id = request.manifest().execution_id();
    registry
        .abort_query(
            QueryAbortRequest::new(
                execution_id,
                request.digest().expect("validated init digest"),
                "abort before init",
            )
            .expect("valid abort"),
        )
        .expect("abort-before-init is accepted");

    assert_eq!(
        registry
            .init_query(request)
            .outcome()
            .expect("validated lifecycle acknowledgement"),
        QueryInitOutcome::RejectedTerminated
    );
    assert_eq!(runtime.runtime_filter_install_calls(), 0);
}
