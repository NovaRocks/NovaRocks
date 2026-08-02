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

use novarocks::query_execution::lifecycle::metrics::BackendQueryLifecycleMetricsSnapshot;
use novarocks::query_execution::lifecycle::{
    AttemptId, FragmentTerminalOutcome, ParticipantBackendIdentity, ParticipantManifest,
    ParticipantQueryOptions, ParticipantRole, QueryAbortRequest, QueryControlAttach,
    QueryControlEndpoint, QueryControlEvent, QueryExecutionId, QueryInitOutcome, QueryInitRequest,
    QueryLifecycleError, QueryLifecycleErrorCode, QueryStageOutcome, QueryStageRequest,
    QueryStartOutcome, QueryStartRequest, QueryTerminalAck, QueryTerminalFallbackTransport,
    QueryTerminalReportAck, QueryTerminalReportOutcome, QueryTerminationReason,
    RuntimeFilterContribution, StageDigest, StageDigestVersion, StageFragment,
};
use novarocks::runtime::fragment::{
    FragmentExecutionError, FragmentExecutionErrorKind, FragmentOutcome,
};
use novarocks::runtime::query_options::QueryOptions;
use novarocks_protocol::{common, novarocks as proto_novarocks, plan};
use novarocks_types::QueryId;
use novarocks_types::UniqueId;
use prost::Message;

use super::entry::QueryLifecyclePhase;
use super::registry::{
    MonotonicClock, QueryLifecycleLocalRuntime, QueryLifecycleMetricsSink, QueryLifecycleRegistry,
    QueryLifecycleRegistryConfig, StageBuildDecision,
};
use crate::runtime_filter::participant::{
    BackendRuntimeFilterParticipantFactory, RuntimeFilterParticipantFactory,
};

const LOCAL_BACKEND_ID: u64 = 7;
const LOCAL_START_EPOCH: u64 = 11;
const ATTEMPT_1: u64 = 1;

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
    terminations: Mutex<
        Vec<(
            QueryExecutionId,
            Vec<UniqueId>,
            QueryTerminationReason,
            String,
        )>,
    >,
    install_gate: Mutex<InstallGate>,
    install_gate_changed: Condvar,
    fail_install: Mutex<bool>,
    fail_abort: Mutex<bool>,
}

#[derive(Default)]
struct RecordingMetricsSink {
    snapshots: Mutex<Vec<BackendQueryLifecycleMetricsSnapshot>>,
}

struct RejectedTerminalFallback;

impl QueryTerminalFallbackTransport for RejectedTerminalFallback {
    fn report_query_terminal(
        &self,
        _endpoint: &QueryControlEndpoint,
        _snapshot: novarocks::query_execution::lifecycle::QueryTerminalSnapshot,
        _timeout: Duration,
    ) -> Result<
        QueryTerminalReportAck,
        novarocks::query_execution::lifecycle::QueryLifecycleTransportError,
    > {
        Ok(QueryTerminalReportAck::new(
            QueryTerminalReportOutcome::RejectedConflict,
            "injected terminal conflict",
        ))
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
}

impl QueryLifecycleMetricsSink for RecordingMetricsSink {
    fn publish(
        &self,
        snapshot: BackendQueryLifecycleMetricsSnapshot,
        _termination_reasons: [u64; 6],
    ) {
        self.snapshots
            .lock()
            .expect("metrics snapshots")
            .push(snapshot);
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
    fn terminate_query(
        &self,
        execution_id: QueryExecutionId,
        expected_instances: &[UniqueId],
        reason: QueryTerminationReason,
        detail: &str,
    ) {
        self.state.terminations.lock().expect("terminations").push((
            execution_id,
            expected_instances.to_vec(),
            reason,
            detail.to_string(),
        ));
    }
}

impl RuntimeFilterParticipantFactory for RecordingLocalRuntime {
    fn install(
        &self,
        execution_id: QueryExecutionId,
        contribution: RuntimeFilterContribution,
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
        let participant =
            BackendRuntimeFilterParticipantFactory.install(execution_id, contribution)?;
        let state = Arc::clone(&self.state);
        Ok(
            participant.with_close_hook_for_test(Arc::new(move |service, _reason| {
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
                service.shutdown();
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
        metrics,
        Arc::new(RejectedTerminalFallback),
    );
    let fragment_instance_id = UniqueId::new(863, 1);
    let request = fragment_init_request_fixture(863, &[fragment_instance_id]);
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry.init_query(request.clone()).outcome(),
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
            QueryAbortRequest::new(execution_id, request.digest(), "terminal conflict")
                .expect("valid abort"),
        )
        .expect("abort is accepted");

    for _ in 0..100 {
        if registry.metrics_snapshot().terminal_retained == 0
            && registry.metrics_snapshot().terminal_fallback_rejected > 0
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    panic!("terminal conflict did not release the retained delivery record");
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
        registry.init_query(request.clone()).outcome(),
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
        registry.init_query(request).outcome(),
        QueryInitOutcome::Applied
    );
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
        registry.init_query(active.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    assert_eq!(
        registry.init_query(lifecycle_tombstone.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    registry
        .abort_query(
            QueryAbortRequest::new(
                lifecycle_tombstone.manifest().execution_id(),
                lifecycle_tombstone.digest(),
                "retain lifecycle tombstone",
            )
            .expect("valid lifecycle tombstone abort"),
        )
        .expect("lifecycle tombstone abort is accepted");
    registry
        .abort_query(
            QueryAbortRequest::new(
                pre_init_tombstone.manifest().execution_id(),
                pre_init_tombstone.digest(),
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

fn init_request_fixture(
    query_low: i64,
    attempt: u64,
    start_epoch: u64,
    query_deadline_unix_ms: u64,
) -> QueryInitRequest {
    let execution_id = execution_id(query_low, attempt);
    let runtime_filter = RuntimeFilterContribution::empty_for_contract_test(execution_id, 3)
        .expect("valid runtime filter contribution");
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
        ParticipantQueryOptions::new(QueryOptions::default()),
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
        expected.iter().copied(),
        ParticipantQueryOptions::new(QueryOptions::default()),
        10_000,
        [],
        None,
        Duration::from_secs(30),
        QueryControlEndpoint::new("127.0.0.1", 9031).expect("valid report endpoint"),
    )
    .expect("valid fragment participant manifest");
    QueryInitRequest::from_manifest(manifest)
}

fn attach_control(
    registry: &Arc<QueryLifecycleRegistry>,
    request: &QueryInitRequest,
) -> novarocks::query_execution::lifecycle::QueryControlAttachment {
    registry
        .attach_control(
            QueryControlAttach::new(request.manifest().execution_id(), request.digest(), 1)
                .expect("valid control attach"),
        )
        .expect("control attaches")
}

fn stage_fragment(instance_id: UniqueId) -> StageFragment {
    StageFragment::new(
        plan::PlanFragment::default(),
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
    QueryStageRequest::new(
        request.manifest().execution_id(),
        request.digest(),
        StageDigestVersion::V1,
        StageDigest::new([digest_byte; 32]),
        instances.iter().copied().map(stage_fragment).collect(),
    )
    .expect("valid stage request")
}

#[test]
fn stage_and_start_are_idempotent_after_control_ready() {
    let expected = [UniqueId::new(8, 1), UniqueId::new(8, 2)];
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let request = fragment_init_request_fixture(1_801, &expected);
    assert_eq!(
        registry.init_query(request.clone()).outcome(),
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

    let start = QueryStartRequest::new(
        request.manifest().execution_id(),
        StageDigestVersion::V1,
        stage.digest(),
    );
    assert_eq!(
        registry.start_prepared_query(start).outcome(),
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
        registry.init_query(request.clone()).outcome(),
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
        request.manifest().execution_id(),
        novarocks::query_execution::lifecycle::ParticipantManifestDigest::new([7; 32]),
        StageDigestVersion::V1,
        StageDigest::new([1; 32]),
        expected.iter().copied().map(stage_fragment).collect(),
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
        registry.init_query(request.clone()).outcome(),
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
                request.digest(),
                "abort staged service participant",
            )
            .expect("valid abort"),
        )
        .expect("abort accepted");
    assert_eq!(
        registry
            .start_prepared_query(QueryStartRequest::new(
                request.manifest().execution_id(),
                StageDigestVersion::V1,
                stage.digest(),
            ))
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
    let encoded_bytes =
        novarocks::query_execution::lifecycle::contract::encode_query_stage_request(&first_stage)
            .encoded_len();
    let mut config = registry_config(8);
    config.stage_max_fragments = 1;
    config.stage_max_encoded_bytes = encoded_bytes;
    config.stage_max_inflight_encoded_bytes = encoded_bytes;
    config.stage_max_dormant_workers = 1;
    let registry = registry_with_config(RecordingLocalRuntime::default(), config);

    for request in [&first, &second] {
        assert_eq!(
            registry.init_query(request.clone()).outcome(),
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
            .start_prepared_query(QueryStartRequest::new(
                first.manifest().execution_id(),
                StageDigestVersion::V1,
                first_stage.digest(),
            ))
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
            registry.init_query(request.clone()).outcome(),
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
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    assert_eq!(
        registry.init_query(request).outcome(),
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
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );

    assert_eq!(
        registry
            .abort_query(
                QueryAbortRequest::new(
                    request.manifest().execution_id(),
                    different.digest(),
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
            QueryControlAttach::new(request.manifest().execution_id(), request.digest(), 1)
                .expect("valid control attach"),
        )
        .expect("digest mismatch must leave the live entry attachable");
}

#[test]
fn query_lifecycle_terminal_event_survives_saturated_heartbeat_queue() {
    let registry = registry_with(RecordingLocalRuntime::default(), 8);
    let request = init_request_fixture(103, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    assert_eq!(
        registry.init_query(request.clone()).outcome(),
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
    while let Ok(event) = attachment.events.try_recv() {
        events.push(event);
    }
    assert!(
        events.contains(&QueryControlEvent::TerminationAccepted {
            reason: QueryTerminationReason::CoordinatorAbort,
        }),
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
        registry.init_query(request.clone()).outcome(),
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

    assert!(matches!(
        attachment.events.try_recv(),
        Ok(QueryControlEvent::ControlReady)
    ));
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
    let execution_id = request.manifest().execution_id();
    assert_eq!(
        registry.init_query(request.clone()).outcome(),
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
            .start_prepared_query(QueryStartRequest::new(
                execution_id,
                StageDigestVersion::V1,
                stage.digest(),
            ))
            .outcome(),
        QueryStartOutcome::Applied
    );
    let mut saw_local_drained = false;
    while let Ok(event) = attachment.events.try_recv() {
        saw_local_drained |= event == QueryControlEvent::LocalDrained;
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
    let snapshot = loop {
        match attachment.events.try_recv() {
            Ok(QueryControlEvent::TerminalSnapshot { snapshot }) => break snapshot,
            Ok(_) => {}
            Err(error) => {
                panic!("TerminalSnapshot must use its reserved correctness permit: {error}")
            }
        }
    };
    attachment
        .control
        .terminal_ack(QueryTerminalAck::from_snapshot(&snapshot))
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
            .outcome(),
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
            .outcome(),
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
            .outcome(),
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
            .outcome(),
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
            .outcome(),
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
            .outcome(),
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
    let digest = request.digest();

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
        termination.accepted_reason(),
        QueryTerminationReason::CoordinatorAbort
    );
    runtime.release_install();

    assert_eq!(
        init_thread.join().expect("init thread").outcome(),
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
    let digest = request.digest();

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
        registry.init_query(request.clone()).outcome(),
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
    assert_eq!(
        attachment.events.try_recv().expect("ControlReady event"),
        novarocks::query_execution::lifecycle::QueryControlEvent::ControlReady
    );
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
        registry.init_query(fragment_request.clone()).outcome(),
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
        registry.init_query(service_request.clone()).outcome(),
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
        registry.init_query(request.clone()).outcome(),
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
        registry.init_query(request.clone()).outcome(),
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
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    let _control = attach_control(&registry, &request);
    let permit = registry
        .admit_fragment(execution_id, expected)
        .expect("fragment permit");

    registry
        .abort_query(
            QueryAbortRequest::new(execution_id, request.digest(), "abort before permit commit")
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
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    let mut attachment = attach_control(&registry, &request);
    assert_eq!(
        attachment.events.try_recv().expect("ControlReady event"),
        QueryControlEvent::ControlReady
    );
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

    assert_eq!(
        attachment.events.try_recv().expect("LocalFailure event"),
        QueryControlEvent::LocalFailure {
            code: "FRAGMENT_EXECUTION_FAILED".to_string(),
            detail: "fragment execution error (pipeline): pipeline worker failed".to_string(),
        }
    );
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
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    let mut attachment = attach_control(&registry, &request);
    assert_eq!(
        attachment.events.try_recv().expect("ControlReady event"),
        QueryControlEvent::ControlReady
    );
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
        attachment.events.try_recv().expect("LocalFailure event"),
        QueryControlEvent::LocalFailure { .. }
    ));
    let deadline = Instant::now() + Duration::from_secs(1);
    let snapshot = loop {
        match attachment.events.try_recv() {
            Ok(event) => break event,
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => panic!("failed terminal snapshot is not delivered after drain: {error}"),
        }
    };
    let QueryControlEvent::TerminalSnapshot { snapshot } = snapshot else {
        panic!("expected failed terminal snapshot");
    };
    assert_eq!(snapshot.execution_id(), execution_id);
    assert!(matches!(
        snapshot
            .fragments()
            .first()
            .expect("one fragment")
            .outcome(),
        FragmentTerminalOutcome::Failed { .. }
    ));
    let metrics = registry.metrics_snapshot();
    assert_eq!(metrics.terminal_facts, 1);
    assert_eq!(metrics.terminal_records_frozen, 1);
    assert_eq!(metrics.terminal_locally_drained, 0);
}

#[test]
fn query_lifecycle_registry_rejects_fragment_executor_without_exact_set() {
    let runtime = RecordingLocalRuntime::default();
    let registry = registry_with(runtime.clone(), 8);

    assert_eq!(
        registry
            .init_query(fragment_init_request_fixture(76, &[]))
            .outcome(),
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
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    let _control = attach_control(&registry, &request);
    let attach =
        QueryControlAttach::new(execution_id, request.digest(), 1).expect("valid control attach");

    let Err(duplicate_error) = registry.attach_control(attach.clone()) else {
        panic!("duplicate active attach must conflict");
    };
    assert_eq!(duplicate_error.code(), QueryLifecycleErrorCode::Conflict);
    registry
        .abort_query(
            QueryAbortRequest::new(execution_id, request.digest(), "terminate before attach")
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
        registry.init_query(active.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    let mut terminated = Vec::new();
    for query_low in [81, 82, 83] {
        let request = init_request_fixture(query_low, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
        let execution_id = request.manifest().execution_id();
        assert_eq!(
            registry.init_query(request.clone()).outcome(),
            QueryInitOutcome::Applied
        );
        registry
            .abort_query(
                QueryAbortRequest::new(execution_id, request.digest(), "bounded tombstone")
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
        registry.init_query(first.clone()).outcome(),
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
            QueryAbortRequest::new(first_execution, first.digest(), "first tombstone")
                .expect("valid abort"),
        )
        .expect("first abort is accepted");
    wait_for_failed_terminal_freeze(&registry);
    clock.advance(Duration::from_millis(2));
    registry.sweep_expired(clock.now());

    let second = init_request_fixture(812, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let second_execution = second.manifest().execution_id();
    assert_eq!(
        registry.init_query(second.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    registry
        .abort_query(
            QueryAbortRequest::new(second_execution, second.digest(), "evict first tombstone")
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
        registry.init_query(replacement.clone()).outcome(),
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
        registry.init_query(first.clone()).outcome(),
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
            QueryAbortRequest::new(first_execution, first.digest(), "first tombstone")
                .expect("valid abort"),
        )
        .expect("first abort is accepted");
    wait_for_failed_terminal_freeze(&registry);
    clock.advance(Duration::from_millis(2));
    registry.sweep_expired(clock.now());

    let eviction = init_request_fixture(815, ATTEMPT_1, LOCAL_START_EPOCH, 10_000);
    let eviction_execution = eviction.manifest().execution_id();
    assert_eq!(
        registry.init_query(eviction.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    registry
        .abort_query(
            QueryAbortRequest::new(
                eviction_execution,
                eviction.digest(),
                "evict first tombstone",
            )
            .expect("valid abort"),
        )
        .expect("eviction abort is accepted");
    assert!(!registry.contains(first_execution));

    let replacement = fragment_init_request_fixture(816, &[fragment_instance_id]);
    let replacement_execution = replacement.manifest().execution_id();
    assert_eq!(
        registry.init_query(replacement.clone()).outcome(),
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
        registry.init_query(competing.clone()).outcome(),
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
        registry.init_query(first.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    registry
        .abort_query(
            QueryAbortRequest::new(
                first.manifest().execution_id(),
                first.digest(),
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
            .outcome(),
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
        registry.init_query(terminated.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    registry
        .abort_query(
            QueryAbortRequest::new(terminated_id, terminated.digest(), "retention")
                .expect("valid abort"),
        )
        .expect("abort is accepted");
    assert!(registry.contains(terminated_id));

    clock.advance(Duration::from_millis(11));
    assert_eq!(
        registry
            .init_query(fragment_init_request_fixture(87, &[UniqueId::new(87, 1)],))
            .outcome(),
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
        registry.init_query(first.clone()).outcome(),
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
            QueryAbortRequest::new(first_execution, first.digest(), "retention cleanup")
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
        registry.init_query(replacement.clone()).outcome(),
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
        registry.init_query(request.clone()).outcome(),
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
        registry.init_query(fragment_request.clone()).outcome(),
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
        registry.init_query(service_request.clone()).outcome(),
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
        registry.init_query(request.clone()).outcome(),
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
        registry.init_query(request.clone()).outcome(),
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
            QueryAbortRequest::new(execution_id, request.digest(), "metrics termination")
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
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    let mut attachment = attach_control(&registry, &request);

    attachment
        .control
        .abort("first reason".to_string())
        .expect("first abort");
    attachment.control.finalize().expect("repeated finalize");
    assert_eq!(
        attachment.events.try_recv().expect("ControlReady"),
        novarocks::query_execution::lifecycle::QueryControlEvent::ControlReady
    );
    for _ in 0..2 {
        assert_eq!(
            attachment.events.try_recv().expect("termination accepted"),
            novarocks::query_execution::lifecycle::QueryControlEvent::TerminationAccepted {
                reason: QueryTerminationReason::CoordinatorAbort,
            }
        );
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
    let first = std::thread::spawn(move || first_registry.init_query(first_request).outcome());
    runtime.wait_until_install_enters();
    let second_registry = Arc::clone(&registry);
    let second = std::thread::spawn(move || second_registry.init_query(request).outcome());
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
        registry.init_query(request).outcome(),
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
            .outcome(),
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
        registry.init_query(request.clone()).outcome(),
        QueryInitOutcome::Applied
    );
    runtime.fail_abort();

    registry
        .abort_query(
            QueryAbortRequest::new(execution_id, request.digest(), "abort with cleanup failure")
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
            .outcome(),
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
            .outcome(),
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
    let digest = request.digest();

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
            .accepted_reason(),
        QueryTerminationReason::CoordinatorAbort
    );
    runtime.release_install();

    assert_eq!(
        init_thread.join().expect("init thread").outcome(),
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
            QueryAbortRequest::new(execution_id, request.digest(), "abort before init")
                .expect("valid abort"),
        )
        .expect("abort-before-init is accepted");

    assert_eq!(
        registry.init_query(request).outcome(),
        QueryInitOutcome::RejectedTerminated
    );
    assert_eq!(runtime.runtime_filter_install_calls(), 0);
}
