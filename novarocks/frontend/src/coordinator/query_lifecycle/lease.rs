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
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::common::backend_topology::BackendTopologyService;
use crate::metrics::FrontendQueryLifecycleMetricsSnapshot;
use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use crate::query_execution::lifecycle_plan::{
    QueryCredentialLeaseRefresh, QueryCredentialLeases, QueryLifecycleAbortOutcome,
    QueryLifecycleLease, QueryLifecycleLeaseGuard, resolve_vended_s3_access,
};
use crate::query_execution::terminal_set::QueryTerminalSet;
use novarocks_proto_codec::lifecycle::{
    CredentialLeaseSecretEnvelope, FragmentLiveObservation, ParticipantAttemptRef,
    ParticipantManifestDigest, ParticipantTerminalOutcome, QueryAbortRequest, QueryControlCommand,
    QueryControlEvent, QueryExecutionId, QueryTerminalAck, QueryTerminationReason,
};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorStorageResolver, CredentialLeaseId,
    ResolvedVendedS3Access, StorageAccessRequest,
};

use super::barrier::FrontendQueryLifecycleConfig;
use super::manifest::MaterializedParticipant;
use super::{
    QueryControlSession, QueryLifecycleTarget, QueryLifecycleTransport,
    QueryLifecycleTransportErrorKind,
};
use crate::coordinator::query_registry::ActiveQueryAttemptBinding;
use crate::coordinator::query_registry::{
    ActiveQueryAttemptControl, FrontendQueryRegistry, LatchedQueryFailure, QueryFailureCause,
    QueryLifecycleConvergenceErrorSource, QueryLifecycleConvergenceSnapshot,
    RuntimeFilterTerminalRollupSnapshot, RuntimeFilterTerminalRollupUnavailable,
};
use crate::runtime_filter::feedback::{RuntimeFilterFeedbackAdmission, RuntimeFilterFeedbackState};

const ACTIVE: u8 = 0;
const ABORTED: u8 = 1;
const FINALIZING: u8 = 2;
const FINALIZED: u8 = 3;
const ABORT_TERMINAL_DELIVERY_GRACE: Duration = Duration::from_secs(1);
const CREDENTIAL_LEASE_SOFT_MARGIN_MIN: Duration = Duration::from_secs(5);
const CREDENTIAL_LEASE_SOFT_MARGIN_MAX: Duration = Duration::from_secs(5 * 60);
const CREDENTIAL_LEASE_HARD_MARGIN_MIN: Duration = Duration::from_secs(1);
const CREDENTIAL_LEASE_HARD_MARGIN_MAX: Duration = Duration::from_secs(30);
const CREDENTIAL_LEASE_JITTER_MAX: Duration = Duration::from_secs(30);
const CREDENTIAL_LEASE_RETRY_INITIAL: Duration = Duration::from_millis(100);
const CREDENTIAL_LEASE_RETRY_MAX: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CredentialLeaseRefreshTiming {
    soft_delay: Duration,
    hard_delay: Duration,
}

#[derive(Clone, Copy)]
struct CredentialLeaseRefreshSchedule {
    lease_id: CredentialLeaseId,
    soft_delay: Duration,
    hard_deadline: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CredentialLeaseRefreshFailure {
    /// The provider did not return a replacement before any prepare was sent.
    /// Retrying is safe because no participant could have observed an epoch.
    RetryableProvider,
    /// An epoch may have reached a participant, or immutable lease scope was
    /// violated. A retry could produce a conflicting value for the same epoch.
    Fatal,
    /// Terminalization/cancellation owns the attempt; do not issue another
    /// abort from the refresh supervisor.
    Stopped,
}

#[derive(Clone, Copy)]
enum SupervisorFailureKind {
    HeartbeatTimeout,
    CoordinatorLost,
    LocalFailure,
}

#[derive(Clone)]
struct TerminalConvergenceDecision {
    source: QueryLifecycleConvergenceErrorSource,
    message: String,
}

enum TerminalConvergenceFailure {
    BackendAttestation(String),
    FrontendLiveness(String),
    NoOutcome(String),
}

impl TerminalConvergenceFailure {
    fn into_decision(self) -> TerminalConvergenceDecision {
        match self {
            Self::BackendAttestation(message) => TerminalConvergenceDecision {
                source: QueryLifecycleConvergenceErrorSource::BackendAttestation,
                message,
            },
            Self::FrontendLiveness(message) => TerminalConvergenceDecision {
                source: QueryLifecycleConvergenceErrorSource::FrontendLiveness,
                message,
            },
            Self::NoOutcome(message) => TerminalConvergenceDecision {
                source: QueryLifecycleConvergenceErrorSource::NoOutcome,
                message,
            },
        }
    }
}

struct AbortCleanupFailure {
    target: QueryLifecycleTarget,
    digest: ParticipantManifestDigest,
    kind: QueryLifecycleTransportErrorKind,
    detail: String,
}

impl AbortCleanupFailure {
    fn new(
        participant: &MaterializedParticipant,
        kind: QueryLifecycleTransportErrorKind,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            target: participant.target.clone(),
            digest: participant.digest,
            kind,
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for AbortCleanupFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "backend {} {:?}: {}",
            self.target.backend_idx(),
            self.kind,
            self.detail
        )
    }
}

#[derive(Clone)]
pub(super) struct ActiveSession {
    pub target: QueryLifecycleTarget,
    pub digest: ParticipantManifestDigest,
    pub session: Arc<dyn QueryControlSession>,
    recv_gate: Arc<Mutex<()>>,
}

#[derive(Default)]
struct TerminalState {
    heartbeat_acks: BTreeMap<usize, u64>,
    backend_stream_closed: BTreeSet<usize>,
    locally_drained: BTreeSet<usize>,
    termination_accepted: BTreeMap<usize, QueryTerminationReason>,
    outcomes: BTreeMap<usize, RetainedTerminalOutcome>,
    reader_failure: Option<String>,
    stop_readers: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CredentialLeaseRefreshPhase {
    Vending,
    Preparing,
    Committing,
}

struct CredentialLeaseRefreshRound {
    lease_id: CredentialLeaseId,
    epoch: Option<u64>,
    phase: CredentialLeaseRefreshPhase,
    prepared: BTreeSet<usize>,
    committed: BTreeSet<usize>,
}

#[derive(Default)]
struct CredentialLeaseRefreshState {
    round: Option<CredentialLeaseRefreshRound>,
    stopped: bool,
}

/// FE-local retained terminal payload. Typed outcome equality is the only
/// duplicate/conflict authority; delivery receipt is keyed by its participant
/// attempt rather than a payload-derived identifier.
#[derive(Clone)]
// Design: ADR-0126 (docs/adr/ADR-0126-terminal-delivery-participant-attempt-ref.md)
struct RetainedTerminalOutcome {
    outcome: ParticipantTerminalOutcome,
}

/// Bounded, best-effort live state received on the participant's control
/// stream. It is deliberately separate from terminal state: no observation is
/// allowed to affect query completion or its primary failure.
#[derive(Default)]
struct FragmentObservationState {
    latest: BTreeMap<(usize, novarocks_types::UniqueId), FragmentLiveObservation>,
    accepted: u64,
    idempotent: u64,
    stale: u64,
    conflict: u64,
    rejected: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FragmentObservationStoreOutcome {
    Accepted,
    Idempotent,
    Stale,
    Conflict,
    Rejected,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq)]
pub(super) struct FragmentObservationSnapshot {
    pub latest: BTreeMap<(usize, novarocks_types::UniqueId), FragmentLiveObservation>,
    pub accepted: u64,
    pub idempotent: u64,
    pub stale: u64,
    pub conflict: u64,
    pub rejected: u64,
}

/// The result of storing a participant terminal outcome. The distinction is
/// intentionally preserved: both cases must be acknowledged, while only the
/// first one contributes to the terminal set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TerminalOutcomeStoreOutcome {
    Accepted,
    AlreadyAccepted,
}

impl TerminalOutcomeStoreOutcome {
    const fn is_accepted(self) -> bool {
        matches!(self, Self::Accepted)
    }
}

impl ActiveSession {
    pub fn new(
        target: QueryLifecycleTarget,
        digest: ParticipantManifestDigest,
        session: Arc<dyn QueryControlSession>,
    ) -> Self {
        Self {
            target,
            digest,
            session,
            recv_gate: Arc::new(Mutex::new(())),
        }
    }

    pub(super) fn recv(
        &self,
        timeout: Duration,
    ) -> Result<QueryControlEvent, super::QueryLifecycleTransportError> {
        let _recv = self.recv_gate.lock().expect("query control receive gate");
        self.session.recv_timeout(timeout)
    }
}

#[derive(Default)]
pub(crate) struct FrontendLifecycleMetrics {
    snapshot: Mutex<FrontendQueryLifecycleMetricsSnapshot>,
}

impl FrontendLifecycleMetrics {
    /// The one process-wide lifecycle counter set.
    ///
    /// Reachable from the coordinator so a task-protocol attempt publishes the
    /// same process counters the lifecycle attempt publishes. These are
    /// process facts either way -- neither protocol keeps an attempt-scoped
    /// copy -- so both carriers reporting the same set is what keeps the
    /// endpoint's `metrics` field meaning one thing.
    pub(crate) fn process_shared() -> Arc<Self> {
        static METRICS: OnceLock<Arc<FrontendLifecycleMetrics>> = OnceLock::new();
        Arc::clone(METRICS.get_or_init(|| Arc::new(Self::default())))
    }

    pub fn attempt_created(&self) {
        self.update(|snapshot| snapshot.active_attempts += 1);
    }

    pub fn attempt_terminated(&self) {
        self.update(|snapshot| {
            snapshot.active_attempts = snapshot.active_attempts.saturating_sub(1);
        });
    }

    pub fn observe_init(
        &self,
        applied: bool,
        idempotent: bool,
        uncertain_cleanup: bool,
        manifest_conflict: bool,
        latency: Duration,
    ) {
        self.update(|snapshot| {
            if applied {
                snapshot.init_applied += 1;
            } else if idempotent {
                snapshot.init_idempotent += 1;
            } else {
                snapshot.init_failed += 1;
            }
            snapshot.init_uncertain_cleanup += u64::from(uncertain_cleanup);
            snapshot.manifest_conflicts += u64::from(manifest_conflict);
            snapshot.init_latency_micros_total += latency.as_micros() as u64;
            snapshot.init_latency_samples += 1;
        });
    }

    pub fn observe_attach(&self, ready: bool, latency: Duration) {
        self.update(|snapshot| {
            snapshot.control_ready += u64::from(ready);
            snapshot.attach_failed += u64::from(!ready);
            snapshot.attach_latency_micros_total += latency.as_micros() as u64;
            snapshot.attach_latency_samples += 1;
        });
    }

    pub fn heartbeat_timeout(&self) {
        self.update(|snapshot| snapshot.heartbeat_timeouts += 1);
    }

    pub fn coordinator_lost(&self) {
        self.update(|snapshot| snapshot.coordinator_lost += 1);
    }

    pub fn local_failure(&self) {
        self.update(|snapshot| snapshot.local_failures += 1);
    }

    pub fn backend_epoch_mismatch(&self) {
        self.update(|snapshot| snapshot.backend_epoch_mismatches += 1);
    }

    pub fn cleanup_failure(&self) {
        self.update(|snapshot| snapshot.cleanup_failures += 1);
    }

    pub fn terminal_locally_drained(&self) {
        self.update(|snapshot| snapshot.terminal_locally_drained += 1);
    }

    pub fn terminal_snapshot_stored(&self, outcome: TerminalOutcomeStoreOutcome) {
        self.update(|snapshot| match outcome {
            TerminalOutcomeStoreOutcome::Accepted => snapshot.terminal_snapshots_accepted += 1,
            TerminalOutcomeStoreOutcome::AlreadyAccepted => {
                snapshot.terminal_snapshots_idempotent += 1
            }
        });
    }

    pub fn terminal_snapshot_conflict(&self) {
        self.update(|snapshot| snapshot.terminal_snapshot_conflicts += 1);
    }

    pub fn terminal_finalize_failure(&self) {
        self.update(|snapshot| snapshot.terminal_finalize_failures += 1);
    }

    pub(crate) fn snapshot(&self) -> FrontendQueryLifecycleMetricsSnapshot {
        *self.snapshot.lock().expect("frontend lifecycle metrics")
    }

    fn update(&self, update: impl FnOnce(&mut FrontendQueryLifecycleMetricsSnapshot)) {
        let snapshot = {
            let mut snapshot = self.snapshot.lock().expect("frontend lifecycle metrics");
            update(&mut snapshot);
            *snapshot
        };
        crate::metrics::publish_frontend_query_lifecycle_metrics(snapshot);
    }
}

pub(super) struct AttemptControl {
    execution_id: QueryExecutionId,
    transport: Arc<dyn QueryLifecycleTransport>,
    registry: Weak<FrontendQueryRegistry>,
    config: FrontendQueryLifecycleConfig,
    /// The complete participant plan frozen before InitQuery begins.
    planned: Mutex<BTreeMap<usize, MaterializedParticipant>>,
    /// Participants to which FE may still owe pre-ready cleanup. This remains
    /// distinct from the post-ControlReady admitted terminal set.
    init_attempted: Mutex<BTreeMap<usize, MaterializedParticipant>>,
    /// Ready evidence is accumulated while concurrent Attach calls complete.
    /// It is never itself a partially visible admitted set.
    control_ready: Mutex<BTreeSet<usize>>,
    /// Catalog readiness is independent from stream attachment. A warm
    /// participant joins this set in its first ControlReady frame; a cold
    /// participant joins only after its in-stream CatalogReady completion.
    catalog_ready: Mutex<BTreeSet<usize>>,
    /// Installed exactly once after every planned participant is both
    /// ControlReady and CatalogReady.
    admitted: Mutex<Option<BTreeMap<usize, MaterializedParticipant>>>,
    sessions: Mutex<BTreeMap<usize, ActiveSession>>,
    state: AtomicU8,
    // A running abort may finish its caller before every BE has delivered an
    // immutable terminal snapshot. Keep the active ingress binding alive so
    // stream delivery and unary fallback remain valid for the bounded BE
    // retention interval.
    retain_terminal_ingress: AtomicBool,
    primary_error: Mutex<Option<String>>,
    terminal_decision: Mutex<Option<TerminalConvergenceDecision>>,
    stop: (Mutex<bool>, Condvar),
    terminal: (Mutex<TerminalState>, Condvar),
    observations: Mutex<FragmentObservationState>,
    feedback: Mutex<Arc<RuntimeFilterFeedbackState>>,
    /// Read-only current topology. The attempt's participant manifest stays
    /// immutable; this projection only proves an exact process replacement
    /// after bounded terminal delivery fails.
    backend_topology: Mutex<Option<(BackendTopologyService, u64)>>,
    /// The only FE owner of vended values after Init has been admitted. The
    /// participant maps are scrubbed immediately after Init retries finish.
    credential_leases: Mutex<QueryCredentialLeases>,
    /// Successful connector writes retain their exact FE credential authority
    /// only until the SQL terminal commit or reconciliation consumes it.
    terminal_credential_holds: AtomicUsize,
    lease_refresh: (Mutex<CredentialLeaseRefreshState>, Condvar),
    readers: Mutex<Vec<JoinHandle<()>>>,
    metrics: Arc<FrontendLifecycleMetrics>,
}

impl AttemptControl {
    pub fn new(
        execution_id: QueryExecutionId,
        transport: Arc<dyn QueryLifecycleTransport>,
        registry: Weak<FrontendQueryRegistry>,
        config: FrontendQueryLifecycleConfig,
        metrics: Arc<FrontendLifecycleMetrics>,
    ) -> Arc<Self> {
        metrics.attempt_created();
        Arc::new(Self {
            execution_id,
            transport,
            registry,
            config,
            planned: Mutex::new(BTreeMap::new()),
            init_attempted: Mutex::new(BTreeMap::new()),
            control_ready: Mutex::new(BTreeSet::new()),
            catalog_ready: Mutex::new(BTreeSet::new()),
            admitted: Mutex::new(None),
            sessions: Mutex::new(BTreeMap::new()),
            state: AtomicU8::new(ACTIVE),
            retain_terminal_ingress: AtomicBool::new(false),
            primary_error: Mutex::new(None),
            terminal_decision: Mutex::new(None),
            stop: (Mutex::new(false), Condvar::new()),
            terminal: (Mutex::new(TerminalState::default()), Condvar::new()),
            observations: Mutex::new(FragmentObservationState::default()),
            feedback: Mutex::new(Arc::new(
                RuntimeFilterFeedbackState::new(execution_id, Default::default())
                    .expect("empty runtime filter feedback declaration is valid"),
            )),
            backend_topology: Mutex::new(None),
            credential_leases: Mutex::new(QueryCredentialLeases::empty()),
            terminal_credential_holds: AtomicUsize::new(0),
            lease_refresh: (
                Mutex::new(CredentialLeaseRefreshState::default()),
                Condvar::new(),
            ),
            readers: Mutex::new(Vec::new()),
            metrics,
        })
    }

    pub fn is_active(&self) -> bool {
        self.state.load(Ordering::Acquire) == ACTIVE
    }

    fn retain_terminal_storage_resolver(
        self: &Arc<Self>,
    ) -> Option<Arc<dyn ConnectorStorageResolver>> {
        if self
            .credential_leases
            .lock()
            .expect("query credential lease store")
            .is_empty()
        {
            return None;
        }
        self.terminal_credential_holds
            .fetch_add(1, Ordering::AcqRel);
        Some(Arc::new(TerminalCredentialLease {
            control: Arc::clone(self),
        }) as Arc<dyn ConnectorStorageResolver>)
    }

    fn release_terminal_credential_hold(&self) {
        debug_assert!(
            self.terminal_credential_holds.load(Ordering::Acquire) > 0,
            "terminal credential hold release must match acquisition"
        );
        if self
            .terminal_credential_holds
            .fetch_sub(1, Ordering::AcqRel)
            == 1
            && self.state.load(Ordering::Acquire) != ACTIVE
        {
            self.clear_credential_leases();
        }
    }

    pub(super) const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub(crate) fn configure_runtime_filter_feedback(
        &self,
        declaration: crate::runtime_filter::install_encoder::FrontendRuntimeFilterFeedbackDeclaration,
    ) -> Result<(), DistributedQueryError> {
        let state =
            RuntimeFilterFeedbackState::new(self.execution_id, declaration).map_err(|error| {
                DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, error)
            })?;
        *self.feedback.lock().expect("runtime filter feedback state") = Arc::new(state);
        Ok(())
    }

    pub(crate) fn install_runtime_filter_feedback_state(
        &self,
        state: Arc<RuntimeFilterFeedbackState>,
    ) {
        let mut current = self.feedback.lock().expect("runtime filter feedback state");
        current.close();
        *current = state;
    }

    pub(crate) fn install_backend_topology(
        &self,
        topology: BackendTopologyService,
        admission_revision: u64,
    ) {
        *self
            .backend_topology
            .lock()
            .expect("query lifecycle backend topology") = Some((topology, admission_revision));
    }

    pub(super) fn set_planned(&self, participants: &[MaterializedParticipant]) {
        let mut planned = self.planned.lock().expect("planned participant set");
        debug_assert!(planned.is_empty(), "planned participant set is immutable");
        planned.extend(
            participants
                .iter()
                .cloned()
                .map(|participant| (participant.target.backend_idx(), participant)),
        );
    }

    pub(super) fn install_credential_leases(
        self: &Arc<Self>,
        leases: QueryCredentialLeases,
    ) -> Result<(), DistributedQueryError> {
        let mut installed = self
            .credential_leases
            .lock()
            .expect("query credential lease store");
        if !installed.is_empty() {
            return Err(contract_violation(
                "query credential leases were installed more than once",
            ));
        }
        *installed = leases;
        installed.adopt_storage_resolver(Arc::clone(self) as Arc<dyn ConnectorStorageResolver>);
        drop(installed);
        Ok(())
    }

    /// Init uses the values only for its exact retry loop. Once every Init RPC
    /// has returned, retained participant facts must become secret-free; the
    /// attempt-local lease store remains the sole FE owner for refresh.
    pub(super) fn scrub_confidential_init_material(&self) -> Result<(), DistributedQueryError> {
        for participants in [&self.planned, &self.init_attempted] {
            let mut participants = participants.lock().expect("lifecycle participant store");
            for participant in participants.values_mut() {
                participant.clear_confidential_lease_material()?;
            }
        }
        let mut admitted = self.admitted.lock().expect("admitted participant set");
        if let Some(participants) = admitted.as_mut() {
            for participant in participants.values_mut() {
                participant.clear_confidential_lease_material()?;
            }
        }
        Ok(())
    }

    pub fn set_init_attempted(&self, participants: &[MaterializedParticipant]) {
        let mut init_attempted = self
            .init_attempted
            .lock()
            .expect("init-attempted participant set");
        init_attempted.extend(
            participants
                .iter()
                .cloned()
                .map(|participant| (participant.target.backend_idx(), participant)),
        );
    }

    pub(super) fn mark_control_ready(&self, backend_idx: usize) {
        self.control_ready
            .lock()
            .expect("control-ready participant set")
            .insert(backend_idx);
    }

    pub(super) fn mark_catalog_ready(&self, backend_idx: usize) {
        self.catalog_ready
            .lock()
            .expect("catalog-ready participant set")
            .insert(backend_idx);
    }

    pub(super) fn freeze_admitted(&self) -> Result<(), DistributedQueryError> {
        let planned = self.planned.lock().expect("planned participant set");
        let init_attempted = self
            .init_attempted
            .lock()
            .expect("init-attempted participant set");
        let control_ready = self
            .control_ready
            .lock()
            .expect("control-ready participant set");
        let catalog_ready = self
            .catalog_ready
            .lock()
            .expect("catalog-ready participant set");
        if planned.len() != init_attempted.len()
            || !planned.keys().eq(init_attempted.keys())
            || planned.len() != control_ready.len()
            || !planned
                .keys()
                .all(|backend_idx| control_ready.contains(backend_idx))
            || planned.len() != catalog_ready.len()
            || !planned
                .keys()
                .all(|backend_idx| catalog_ready.contains(backend_idx))
        {
            return Err(contract_violation(
                "cannot freeze admitted participants before every planned participant is ControlReady and CatalogReady",
            ));
        }
        let mut admitted = self.admitted.lock().expect("admitted participant set");
        if admitted.is_some() {
            return Err(contract_violation(
                "admitted participant set was already frozen",
            ));
        }
        *admitted = Some(planned.clone());
        Ok(())
    }

    fn admitted_len(&self) -> Result<usize, DistributedQueryError> {
        self.admitted
            .lock()
            .expect("admitted participant set")
            .as_ref()
            .map(BTreeMap::len)
            .ok_or_else(|| {
                contract_violation(
                    "terminal completeness is unavailable before the admitted participant set freezes",
                )
            })
    }

    #[cfg(test)]
    pub(super) fn admitted_for_test(&self) -> Option<Vec<usize>> {
        self.admitted
            .lock()
            .expect("admitted participant set")
            .as_ref()
            .map(|participants| participants.keys().copied().collect())
    }

    /// Applies a best-effort live observation after checking that it belongs to
    /// the active participant and to one of the exact fragment instances
    /// frozen in that participant's manifest. This never reports a lifecycle
    /// failure: stale or malformed telemetry is observable only through the
    /// bounded state counters.
    pub(super) fn store_fragment_observation(
        &self,
        session: &ActiveSession,
        observation: FragmentLiveObservation,
    ) -> FragmentObservationStoreOutcome {
        let backend_idx = session.target.backend_idx();
        let participant = self
            .init_attempted
            .lock()
            .expect("init-attempted participant set")
            .get(&backend_idx)
            .cloned();
        let valid = participant.is_some_and(|participant| {
            let Ok(manifest) = participant.request.manifest() else {
                return false;
            };
            let Ok(expected_participant) = self.participant_attempt_ref(&participant) else {
                return false;
            };
            let Ok(observation_participant) = observation.participant() else {
                return false;
            };
            let expected_fragment_instance_ids = manifest.expected_fragment_instance_ids();
            let Ok(fragment_instance_id) = observation.fragment_instance_id() else {
                return false;
            };
            participant.target == session.target
                && observation_participant == expected_participant
                && expected_fragment_instance_ids.contains(&fragment_instance_id)
        });
        if !valid {
            return self
                .record_fragment_observation_outcome(FragmentObservationStoreOutcome::Rejected);
        }

        // A terminal outcome is the immutable authority for its participant.
        // Keep the last live sample for diagnostics, but fence all later
        // updates rather than allowing telemetry to race terminal finalization.
        if self
            .terminal
            .0
            .lock()
            .expect("query terminal state")
            .outcomes
            .contains_key(&backend_idx)
        {
            return self
                .record_fragment_observation_outcome(FragmentObservationStoreOutcome::Rejected);
        }

        let fragment_instance_id = match observation.fragment_instance_id() {
            Ok(fragment_instance_id) => fragment_instance_id,
            Err(_) => {
                return self.record_fragment_observation_outcome(
                    FragmentObservationStoreOutcome::Rejected,
                );
            }
        };
        let key = (
            backend_idx,
            novarocks_types::UniqueId::new(fragment_instance_id.hi, fragment_instance_id.lo),
        );
        let outcome = {
            let mut state = self.observations.lock().expect("fragment observations");
            match state.latest.get(&key) {
                None => {
                    state.latest.insert(key, observation);
                    FragmentObservationStoreOutcome::Accepted
                }
                Some(existing) if observation.sequence() > existing.sequence() => {
                    state.latest.insert(key, observation);
                    FragmentObservationStoreOutcome::Accepted
                }
                Some(existing) if observation.sequence() < existing.sequence() => {
                    FragmentObservationStoreOutcome::Stale
                }
                Some(existing) if existing == &observation => {
                    FragmentObservationStoreOutcome::Idempotent
                }
                Some(_) => FragmentObservationStoreOutcome::Conflict,
            }
        };
        self.record_fragment_observation_outcome(outcome)
    }

    fn record_fragment_observation_outcome(
        &self,
        outcome: FragmentObservationStoreOutcome,
    ) -> FragmentObservationStoreOutcome {
        let mut state = self.observations.lock().expect("fragment observations");
        match outcome {
            FragmentObservationStoreOutcome::Accepted => state.accepted += 1,
            FragmentObservationStoreOutcome::Idempotent => state.idempotent += 1,
            FragmentObservationStoreOutcome::Stale => state.stale += 1,
            FragmentObservationStoreOutcome::Conflict => state.conflict += 1,
            FragmentObservationStoreOutcome::Rejected => state.rejected += 1,
        }
        outcome
    }

    #[cfg(test)]
    pub(super) fn fragment_observation_snapshot(&self) -> FragmentObservationSnapshot {
        let state = self.observations.lock().expect("fragment observations");
        FragmentObservationSnapshot {
            latest: state.latest.clone(),
            accepted: state.accepted,
            idempotent: state.idempotent,
            stale: state.stale,
            conflict: state.conflict,
            rejected: state.rejected,
        }
    }

    pub fn add_session(self: &Arc<Self>, session: ActiveSession) {
        let reader_session = session.clone();
        self.sessions
            .lock()
            .expect("active query control sessions")
            .insert(session.target.backend_idx(), session);
        let weak = Arc::downgrade(self);
        let reader = std::thread::Builder::new()
            .name(format!(
                "query-control-reader-{}/{}-{}-{}",
                self.execution_id.query_id().high(),
                self.execution_id.query_id().low(),
                self.execution_id.attempt_id().get(),
                reader_session.target.backend_idx(),
            ))
            .spawn(move || control_event_reader(weak, reader_session))
            .expect("spawn query control event reader");
        self.readers
            .lock()
            .expect("query control event readers")
            .push(reader);
    }

    pub fn sessions(&self) -> Vec<ActiveSession> {
        self.sessions
            .lock()
            .expect("active query control sessions")
            .values()
            .cloned()
            .collect()
    }

    /// Stores a terminal outcome through the same path used by the stream
    /// reader and unary fallback ingress. It validates the immutable outcome
    /// before changing any FE state, then makes same-digest retries idempotent
    /// and rejects conflicting payloads for the participant.
    pub(crate) fn store_terminal_outcome(
        &self,
        outcome: ParticipantTerminalOutcome,
    ) -> Result<TerminalOutcomeStoreOutcome, DistributedQueryError> {
        if outcome.execution_id() != self.execution_id {
            return Err(contract_violation(
                "query terminal outcome execution id differs from active lifecycle attempt",
            ));
        }
        let backend_idx = self.terminal_outcome_backend_idx(&outcome)?;
        self.store_terminal_outcome_at(backend_idx, outcome)
    }

    fn terminal_outcome_backend_idx(
        &self,
        outcome: &ParticipantTerminalOutcome,
    ) -> Result<usize, DistributedQueryError> {
        let participant_ref = outcome.participant();
        if participant_ref
            .execution_id()
            .map_err(|error| contract_violation(error.to_string()))?
            != self.execution_id
        {
            return Err(contract_violation(
                "query terminal outcome execution id differs from active lifecycle attempt",
            ));
        }
        let participants = self
            .init_attempted
            .lock()
            .expect("init-attempted participant set")
            .iter()
            .map(|(backend_idx, participant)| (*backend_idx, participant.clone()))
            .collect::<Vec<_>>();
        for (backend_idx, participant) in participants {
            if self.participant_attempt_ref(&participant)? == participant_ref {
                return Ok(backend_idx);
            }
        }
        Err(contract_violation(
            "query terminal outcome is not owned by an init-attempted lifecycle participant",
        ))
    }

    fn validate_terminal_outcome_session(
        &self,
        session: &ActiveSession,
        outcome: &ParticipantTerminalOutcome,
    ) -> Result<(), String> {
        let outcome_process_id = outcome
            .participant()
            .backend_process_id()
            .map_err(|error| error.to_string())?;
        if outcome_process_id != session.target.process_id() {
            return Err(format!(
                "query terminal outcome participant process differs from control session backend {}",
                session.target.backend_idx()
            ));
        }
        Ok(())
    }

    fn participant_attempt_ref(
        &self,
        participant: &MaterializedParticipant,
    ) -> Result<ParticipantAttemptRef, DistributedQueryError> {
        let manifest = participant
            .request
            .manifest()
            .map_err(|error| contract_violation(error.to_string()))?;
        let execution_id = manifest
            .execution_id()
            .map_err(|error| contract_violation(error.to_string()))?;
        if execution_id != self.execution_id {
            return Err(contract_violation(
                "init-attempted participant manifest execution id differs from active lifecycle attempt",
            ));
        }
        let backend = manifest
            .backend()
            .map_err(|error| contract_violation(error.to_string()))?;
        let process_id = backend
            .process_id()
            .map_err(|error| contract_violation(error.to_string()))?;
        ParticipantAttemptRef::new(execution_id, process_id)
            .map_err(|error| contract_violation(error.to_string()))
    }

    fn participant_attempt_ref_for_session(
        &self,
        session: &ActiveSession,
    ) -> Result<ParticipantAttemptRef, DistributedQueryError> {
        let participant = self
            .init_attempted
            .lock()
            .expect("init-attempted participant set")
            .get(&session.target.backend_idx())
            .cloned()
            .ok_or_else(|| {
                contract_violation(
                    "query control session is not owned by an init-attempted lifecycle participant",
                )
            })?;
        if participant.target != session.target {
            return Err(contract_violation(
                "query control session target differs from its init-attempted lifecycle participant",
            ));
        }
        self.participant_attempt_ref(&participant)
    }

    fn store_terminal_outcome_at(
        &self,
        backend_idx: usize,
        outcome: ParticipantTerminalOutcome,
    ) -> Result<TerminalOutcomeStoreOutcome, DistributedQueryError> {
        let mut terminal = self.terminal.0.lock().expect("query terminal store");
        if let Some(reason) = &terminal.reader_failure {
            return Err(contract_violation(format!(
                "query lifecycle terminal ingress is already failed: {reason}"
            )));
        }
        let store_outcome = match terminal.outcomes.get(&backend_idx) {
            Some(existing) if existing.outcome == outcome => {
                TerminalOutcomeStoreOutcome::AlreadyAccepted
            }
            Some(_) => {
                drop(terminal);
                self.metrics.terminal_snapshot_conflict();
                return Err(contract_violation(
                    "query terminal outcome conflicts with an already stored participant outcome",
                ));
            }
            None => {
                terminal
                    .outcomes
                    .insert(backend_idx, RetainedTerminalOutcome { outcome });
                TerminalOutcomeStoreOutcome::Accepted
            }
        };
        drop(terminal);
        if store_outcome.is_accepted() {
            super::barrier::record_lifecycle_phase_marker_for_execution(
                "terminal-retained",
                marker_execution_id(self.execution_id)?,
            )?;
        }
        self.terminal.1.notify_all();
        self.metrics.terminal_snapshot_stored(store_outcome);
        Ok(store_outcome)
    }

    #[cfg(debug_assertions)]
    fn store_terminal_outcome_conflict(
        &self,
        outcome: ParticipantTerminalOutcome,
        conflict: ParticipantTerminalOutcome,
    ) -> Result<(), DistributedQueryError> {
        let backend_idx = self.terminal_outcome_backend_idx(&outcome)?;
        if self.terminal_outcome_backend_idx(&conflict)? != backend_idx {
            return Err(contract_violation(
                "injected query terminal conflict changed the participant identity",
            ));
        }
        if outcome == conflict {
            return Err(contract_violation(
                "injected query terminal conflict did not change the typed outcome",
            ));
        }
        let mut terminal = self.terminal.0.lock().expect("query terminal store");
        if terminal.outcomes.contains_key(&backend_idx) {
            return Err(contract_violation(
                "injected query terminal conflict requires an empty participant slot",
            ));
        }
        // Store the primary immutable value and the conflicting value while
        // holding the same lock.  No finalizer can observe an apparently
        // complete terminal set between the two admissions.
        terminal
            .outcomes
            .insert(backend_idx, RetainedTerminalOutcome { outcome });
        let reason = "query terminal outcome conflicts with an already stored participant outcome";
        terminal.reader_failure = Some(reason.to_string());
        drop(terminal);
        self.terminal.1.notify_all();
        self.metrics
            .terminal_snapshot_stored(TerminalOutcomeStoreOutcome::Accepted);
        self.metrics.terminal_snapshot_conflict();
        Err(contract_violation(reason))
    }

    pub(crate) fn terminal_set(&self) -> Result<QueryTerminalSet, DistributedQueryError> {
        let expected = self.admitted_len()?;
        let terminal = self.terminal.0.lock().expect("query terminal store");
        if terminal.outcomes.len() != expected {
            return Err(failed(format!(
                "query lifecycle terminal outcomes are incomplete: received {}, expected {expected}",
                terminal.outcomes.len()
            )));
        }
        let snapshots = terminal
            .outcomes
            .values()
            .map(|retained| match retained.outcome.snapshot() {
                Some(snapshot) => Ok(snapshot),
                None => Err(failed(format!(
                    "query lifecycle participant returned negative attestation: {:?}",
                    retained
                        .outcome
                        .negative_attestation()
                        .expect("validated terminal outcome is proof or attestation")
                        .reason()
                ))),
            })
            .collect::<Result<Vec<_>, _>>()?;
        QueryTerminalSet::from_protocol_snapshots(snapshots)
            .map_err(|error| failed(error.to_string()))
    }

    #[cfg(test)]
    pub(super) fn terminal_outcomes_for_test(&self) -> Vec<ParticipantTerminalOutcome> {
        self.terminal
            .0
            .lock()
            .expect("query terminal store")
            .outcomes
            .values()
            .map(|retained| retained.outcome.clone())
            .collect()
    }

    pub fn abort_before_ready(&self, primary_error: String) -> String {
        self.abort(primary_error, true)
    }

    pub fn abort_preserving(&self, primary_error: String) -> String {
        self.abort(primary_error, false)
    }

    fn abort_with_terminal_outcome(&self, primary_error: String) -> QueryLifecycleAbortOutcome {
        self.retain_terminal_ingress.store(true, Ordering::Release);
        let primary_error = self.abort_preserving(primary_error);
        // A failed running query is allowed to finish draining after the
        // abort acknowledgement.  Keep the control readers alive for the
        // bounded terminal-delivery interval so their store-before-ACK path
        // releases the BE's retained P1 record.  The original execution
        // failure remains authoritative if convergence does not complete.
        let terminal_set = self
            .wait_for_all_outcomes(
                self.config
                    .terminal_snapshot_timeout()
                    .min(ABORT_TERMINAL_DELIVERY_GRACE),
            )
            .ok()
            .or_else(|| self.terminal_set().ok());
        QueryLifecycleAbortOutcome::new(primary_error, terminal_set)
    }

    fn abort(&self, primary_error: String, force_unary: bool) -> String {
        let mut observed = self.state.load(Ordering::Acquire);
        loop {
            match observed {
                ACTIVE | FINALIZING => match self.state.compare_exchange(
                    observed,
                    ABORTED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(current) => observed = current,
                },
                _ => {
                    return self
                        .primary_error
                        .lock()
                        .expect("query lifecycle primary error")
                        .clone()
                        .unwrap_or(primary_error);
                }
            }
        }
        *self
            .primary_error
            .lock()
            .expect("query lifecycle primary error") = Some(primary_error.clone());
        self.feedback
            .lock()
            .expect("runtime filter feedback state")
            .close();
        self.stop_and_clear_credential_leases();
        self.terminal.1.notify_all();
        self.stop_supervisor();
        self.metrics.attempt_terminated();
        tracing::warn!(
            query_id_high = self.execution_id.query_id().high(),
            query_id_low = self.execution_id.query_id().low(),
            attempt_id = self.execution_id.attempt_id().get(),
            reason = %primary_error,
            "frontend query lifecycle abort"
        );
        let errors = self.abort_targets(force_unary, &primary_error);
        let enriched = if errors.is_empty() {
            primary_error
        } else {
            format!(
                "{primary_error}; query lifecycle rollback failed: {}",
                errors
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ")
            )
        };
        *self
            .primary_error
            .lock()
            .expect("query lifecycle primary error") = Some(enriched.clone());
        enriched
    }

    fn abort_targets(&self, force_unary: bool, reason: &str) -> Vec<AbortCleanupFailure> {
        let attempted = self
            .init_attempted
            .lock()
            .expect("init-attempted participant set")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let sessions = self
            .sessions
            .lock()
            .expect("active query control sessions")
            .clone();
        let failures: Vec<AbortCleanupFailure> = std::thread::scope(|scope| {
            let handles = attempted
                .into_iter()
                .map(|participant| {
                    let session = sessions.get(&participant.target.backend_idx()).cloned();
                    let worker_participant = participant.clone();
                    (
                        participant,
                        scope.spawn(move || {
                            self.abort_target(
                                &worker_participant,
                                session.as_ref(),
                                force_unary,
                                reason,
                            )
                        }),
                    )
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .filter_map(|(participant, handle)| match handle.join() {
                    Ok(Ok(())) => None,
                    Ok(Err(error)) => Some(error),
                    Err(_) => Some(AbortCleanupFailure::new(
                        &participant,
                        QueryLifecycleTransportErrorKind::Unavailable,
                        "query lifecycle abort worker panicked",
                    )),
                })
                .collect()
        });
        for failure in &failures {
            self.metrics.cleanup_failure();
            tracing::error!(
                query_id_high = self.execution_id.query_id().high(),
                query_id_low = self.execution_id.query_id().low(),
                attempt_id = self.execution_id.attempt_id().get(),
                backend_idx = failure.target.backend_idx(),
                backend_process_id = %failure.target.process_id(),
                participant_digest = %hex::encode(failure.digest.as_bytes()),
                error_kind = ?failure.kind,
                error = %failure.detail,
                "frontend query lifecycle participant cleanup failed"
            );
        }
        failures
    }

    fn abort_target(
        &self,
        participant: &MaterializedParticipant,
        session: Option<&ActiveSession>,
        force_unary: bool,
        reason: &str,
    ) -> Result<(), AbortCleanupFailure> {
        if !force_unary && let Some(session) = session {
            let stream_result = (|| {
                session
                    .session
                    .send(
                        control_command(
                            novarocks_proto_models::novarocks::query_control_request::Command::Abort(
                                novarocks_proto_models::novarocks::QueryControlAbort {
                                    reason: reason.to_string(),
                                },
                            ),
                        )
                        .map_err(|error| {
                            (QueryLifecycleTransportErrorKind::InvalidResponse, error)
                        })?,
                    )
                    .map_err(|error| (error.kind(), error.to_string()))?;
                match self.wait_for_termination(
                    participant.target.backend_idx(),
                    self.config.attach_timeout(),
                ) {
                    Ok(accepted_reason) => {
                        tracing::info!(
                            query_id_high = self.execution_id.query_id().high(),
                            query_id_low = self.execution_id.query_id().low(),
                            attempt_id = self.execution_id.attempt_id().get(),
                            backend_idx = participant.target.backend_idx(),
                            backend_process_id = %participant.target.process_id(),
                            participant_digest = %hex::encode(participant.digest.as_bytes()),
                            accepted_reason = ?accepted_reason,
                            "frontend query lifecycle stream abort accepted"
                        );
                        Ok(())
                    }
                    Err(error) => Err((QueryLifecycleTransportErrorKind::InvalidResponse, error)),
                }
            })();
            if stream_result.is_ok() {
                return Ok(());
            }
        }

        let participant_ref = self.participant_attempt_ref(participant).map_err(|error| {
            AbortCleanupFailure::new(
                participant,
                QueryLifecycleTransportErrorKind::InvalidResponse,
                error.to_string(),
            )
        })?;
        // Unary abort can arrive before the control stream is attached. Keep
        // the Init digest exclusively as that retained cleanup fingerprint;
        // participant identity itself is the stable process-attempt reference.
        let request = QueryAbortRequest::new(participant_ref, participant.digest, reason);
        let ack = self
            .transport
            .abort_query(
                participant.target.clone(),
                request,
                self.config.attach_timeout(),
            )
            .map_err(|error| {
                AbortCleanupFailure::new(
                    participant,
                    error.kind(),
                    format!(
                        "backend {} unary abort: {error}",
                        participant.target.backend_idx()
                    ),
                )
            })?;
        if ack.execution_id().map_err(|error| {
            AbortCleanupFailure::new(
                participant,
                QueryLifecycleTransportErrorKind::InvalidResponse,
                error.to_string(),
            )
        })? != self.execution_id
        {
            return Err(AbortCleanupFailure::new(
                participant,
                QueryLifecycleTransportErrorKind::InvalidResponse,
                format!(
                    "backend {} unary abort acknowledgement execution id mismatch",
                    participant.target.backend_idx()
                ),
            ));
        }
        tracing::info!(
            query_id_high = self.execution_id.query_id().high(),
            query_id_low = self.execution_id.query_id().low(),
            attempt_id = self.execution_id.attempt_id().get(),
            backend_idx = participant.target.backend_idx(),
            backend_process_id = %participant.target.process_id(),
            participant_digest = %hex::encode(participant.digest.as_bytes()),
            accepted_reason = ?ack.accepted_reason().map_err(|error| AbortCleanupFailure::new(
                participant,
                QueryLifecycleTransportErrorKind::InvalidResponse,
                error.to_string(),
            ))?,
            "frontend query lifecycle unary abort accepted"
        );
        Ok(())
    }

    fn wait_for_heartbeat(
        &self,
        backend_idx: usize,
        sequence: u64,
        timeout: Duration,
    ) -> Result<(), String> {
        self.wait_terminal_event(timeout, |terminal| {
            if terminal.backend_stream_closed.contains(&backend_idx) {
                return Some(Err(format!(
                    "query lifecycle backend {backend_idx} control stream closed"
                )));
            }
            if let Some(error) = &terminal.reader_failure {
                return Some(Err(error.clone()));
            }
            terminal
                .heartbeat_acks
                .get(&backend_idx)
                .is_some_and(|ack| *ack >= sequence)
                .then_some(Ok(()))
        })
        .ok_or_else(|| format!("query lifecycle heartbeat timeout on backend {backend_idx}"))?
    }

    fn wait_for_termination(
        &self,
        backend_idx: usize,
        timeout: Duration,
    ) -> Result<QueryTerminationReason, String> {
        self.wait_terminal_event(timeout, |terminal| {
            if let Some(error) = &terminal.reader_failure {
                return Some(Err(error.clone()));
            }
            terminal
                .termination_accepted
                .get(&backend_idx)
                .copied()
                .map(Ok)
        })
        .ok_or_else(|| {
            format!("query lifecycle abort acknowledgement timed out on backend {backend_idx}")
        })?
    }

    fn wait_for_all_drained(&self, timeout: Duration) -> Result<(), String> {
        let expected = self.admitted_len().map_err(|error| error.to_string())?;
        self.wait_terminal_event(timeout, |terminal| {
            if self.state.load(Ordering::Acquire) == ABORTED
                && let Some(error) = self
                    .primary_error
                    .lock()
                    .expect("query lifecycle primary error")
                    .clone()
            {
                return Some(Err(error));
            }
            if let Some(error) = &terminal.reader_failure {
                return Some(Err(error.clone()));
            }
            (terminal.locally_drained.len() == expected).then_some(Ok(()))
        })
        .ok_or_else(|| {
            "query lifecycle timed out waiting for all participants to drain".to_string()
        })?
    }

    #[allow(
        dead_code,
        reason = "Retained for terminal-delivery state assertions in lifecycle coverage."
    )]
    fn terminal_delivery_started(&self, terminal: &TerminalState) -> bool {
        self.state.load(Ordering::Acquire) == FINALIZING
            && self
                .admitted
                .lock()
                .expect("admitted participant set")
                .as_ref()
                .is_some_and(|admitted| terminal.locally_drained.len() == admitted.len())
    }

    fn release_session(&self, backend_idx: usize) {
        self.sessions
            .lock()
            .expect("active query control sessions")
            .remove(&backend_idx);
    }

    fn wait_for_all_outcomes(
        &self,
        timeout: Duration,
    ) -> Result<QueryTerminalSet, TerminalConvergenceFailure> {
        let expected = self
            .admitted_len()
            .map_err(|error| TerminalConvergenceFailure::FrontendLiveness(error.to_string()))?;
        let result = self.wait_terminal_event(timeout, |terminal| {
            if let Some(error) = &terminal.reader_failure {
                return Some(Err(TerminalConvergenceFailure::FrontendLiveness(
                    error.clone(),
                )));
            }
            if terminal.outcomes.len() != expected {
                return None;
            }
            let snapshots = terminal
                .outcomes
                .values()
                .map(|retained| match retained.outcome.snapshot() {
                    Some(snapshot) => Ok(snapshot),
                    None => Err(TerminalConvergenceFailure::BackendAttestation(format!(
                        "query lifecycle participant returned negative attestation: {:?}",
                        retained
                            .outcome
                            .negative_attestation()
                            .expect("validated terminal outcome is proof or attestation")
                            .reason()
                    ))),
                })
                .collect::<Result<Vec<_>, _>>();
            Some(snapshots.and_then(|snapshots| {
                QueryTerminalSet::from_protocol_snapshots(snapshots).map_err(|error| {
                    TerminalConvergenceFailure::BackendAttestation(error.to_string())
                })
            }))
        });
        result.unwrap_or_else(|| {
            if let Some(error) = self.exact_replacement_error() {
                Err(TerminalConvergenceFailure::FrontendLiveness(error))
            } else {
                Err(TerminalConvergenceFailure::NoOutcome(
                    self.no_outcome_error(),
                ))
            }
        })
    }

    fn exact_replacement_error(&self) -> Option<String> {
        let (topology, admission_revision) = self
            .backend_topology
            .lock()
            .expect("query lifecycle backend topology")
            .clone()?;
        let admitted = self
            .admitted
            .lock()
            .expect("admitted participant set")
            .clone()?;
        let terminal = self.terminal.0.lock().expect("query terminal state");
        let missing = admitted
            .iter()
            .filter(|(backend_idx, _)| !terminal.outcomes.contains_key(backend_idx))
            .map(|(backend_idx, participant)| (*backend_idx, participant.clone()))
            .collect::<Vec<_>>();
        drop(terminal);

        let replacement_error =
            |snapshot: &crate::common::backend_topology::BackendTopologySnapshot| {
                missing.iter().find_map(|(backend_idx, participant)| {
                let frozen_process_id = participant.target.process_id();
                // `backend_idx` is a snapshot-local scheduling ordinal, not a
                // durable member identity. A replacement keeps the frozen
                // endpoint but mints a new process id, so compare current
                // eligible targets by endpoint and require the old identity
                // to be absent before classifying the missing terminal record.
                let current_process_ids = snapshot
                    .targets()
                    .iter()
                    .filter_map(|target| {
                        (target.endpoint().ok().as_ref() == Some(participant.target.endpoint()))
                            .then(|| target.process_id().ok())
                            .flatten()
                    })
                    .collect::<BTreeSet<_>>();
                (!current_process_ids.is_empty()
                    && !current_process_ids.contains(&frozen_process_id))
                .then(|| {
                    let current_process_id = current_process_ids
                        .iter()
                        .next()
                        .expect("non-empty replacement process ids");
                    format!(
                        "query lifecycle terminal ACK failed for backend {backend_idx}: frozen process {frozen_process_id} was replaced by {current_process_id}"
                    )
                })
            })
            };

        if let Some(error) = topology
            .snapshot()
            .ok()
            .and_then(|snapshot| replacement_error(&snapshot))
        {
            return Some(error);
        }

        // A restarted BE first announces and then proves its fresh identity
        // through the FE heartbeat. The terminal stream can close in between;
        // wait one bounded heartbeat window for that exact replacement event
        // before preserving a NoOutcome result.
        topology
            .wait_for_eligible_after(
                admission_revision,
                Instant::now() + self.config.heartbeat_timeout(),
            )
            .ok()
            .and_then(|snapshot| replacement_error(&snapshot))
    }

    fn no_outcome_error(&self) -> String {
        let admitted = self
            .admitted
            .lock()
            .expect("admitted participant set")
            .clone();
        let terminal = self.terminal.0.lock().expect("query terminal state");
        let Some(admitted) = admitted else {
            return "query lifecycle NoOutcome: admitted participant set was never frozen"
                .to_string();
        };
        let missing = admitted
            .iter()
            .filter(|(backend_idx, _)| !terminal.outcomes.contains_key(backend_idx))
            .map(|(backend_idx, participant)| {
                format!(
                    "backend={backend_idx} process_id={} digest={}",
                    participant.target.process_id(),
                    hex::encode(participant.digest.as_bytes())
                )
            })
            .collect::<Vec<_>>();
        if missing.is_empty() {
            "query lifecycle NoOutcome: terminal outcome convergence did not complete".to_string()
        } else {
            format!(
                "query lifecycle NoOutcome missing admitted participants: {}",
                missing.join(", ")
            )
        }
    }

    fn wait_terminal_event<T, E>(
        &self,
        timeout: Duration,
        condition: impl Fn(&TerminalState) -> Option<Result<T, E>>,
    ) -> Option<Result<T, E>> {
        let deadline = Instant::now().checked_add(timeout)?;
        let mut terminal = self.terminal.0.lock().expect("query terminal state");
        loop {
            if let Some(result) = condition(&terminal) {
                return Some(result);
            }
            let now = Instant::now();
            if now >= deadline {
                return None;
            }
            let (next, wait) = self
                .terminal
                .1
                .wait_timeout(terminal, deadline.saturating_duration_since(now))
                .expect("query terminal state wait");
            terminal = next;
            if wait.timed_out() {
                return condition(&terminal);
            }
        }
    }

    pub fn stop_supervisor(&self) {
        let mut stopped = self.stop.0.lock().expect("query lifecycle stop lock");
        *stopped = true;
        self.stop.1.notify_all();
    }

    fn stop_credential_lease_refresh(&self) {
        let mut refresh = self
            .lease_refresh
            .0
            .lock()
            .expect("credential lease refresh state");
        refresh.stopped = true;
        refresh.round = None;
        self.lease_refresh.1.notify_all();
    }

    fn clear_credential_leases(&self) {
        let mut credential_leases = self
            .credential_leases
            .lock()
            .expect("query credential lease store");
        credential_leases.revoke_storage_resolver();
        credential_leases.clear();
        if let Err(error) = self.scrub_confidential_init_material() {
            tracing::error!(
                query_id_high = self.execution_id.query_id().high(),
                query_id_low = self.execution_id.query_id().low(),
                attempt_id = self.execution_id.attempt_id().get(),
                error = %error.message(),
                "frontend query lifecycle could not scrub confidential Init material"
            );
        }
    }

    fn stop_and_clear_credential_leases(&self) {
        self.stop_credential_lease_refresh();
        self.clear_credential_leases();
    }

    fn wait_heartbeat_interval(&self) -> bool {
        let stopped = self.stop.0.lock().expect("query lifecycle stop lock");
        if *stopped {
            return false;
        }
        let (stopped, _) = self
            .stop
            .1
            .wait_timeout(stopped, self.config.heartbeat_interval())
            .expect("query lifecycle heartbeat wait");
        !*stopped
    }

    fn next_credential_lease_refresh(&self) -> Option<CredentialLeaseRefreshSchedule> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_millis() as u64;
        let now = Instant::now();
        self.credential_leases
            .lock()
            .expect("query credential lease store")
            .refreshable()
            .into_iter()
            .map(|(lease_id, expires_at_ms)| {
                let timing = credential_lease_refresh_timing(
                    lease_id,
                    Duration::from_millis(expires_at_ms.saturating_sub(now_ms)),
                );
                CredentialLeaseRefreshSchedule {
                    lease_id,
                    soft_delay: timing.soft_delay,
                    hard_deadline: now + timing.hard_delay,
                }
            })
            .min_by_key(|schedule| schedule.soft_delay)
    }

    fn wait_until_stopped_or_credential_refresh(&self, delay: Duration) -> bool {
        let stopped = self.stop.0.lock().expect("query lifecycle stop lock");
        if *stopped {
            return false;
        }
        let (stopped, _) = self
            .stop
            .1
            .wait_timeout(stopped, delay)
            .expect("query lifecycle credential lease wait");
        !*stopped
    }

    fn refresh_credential_lease(
        &self,
        lease_id: CredentialLeaseId,
        hard_deadline: Instant,
    ) -> Result<(), CredentialLeaseRefreshFailure> {
        let result = self.refresh_credential_lease_once(lease_id, hard_deadline);
        if result.is_err() {
            self.clear_credential_lease_refresh_round(lease_id);
        }
        result
    }

    fn refresh_credential_lease_once(
        &self,
        lease_id: CredentialLeaseId,
        hard_deadline: Instant,
    ) -> Result<(), CredentialLeaseRefreshFailure> {
        self.ensure_credential_lease_refresh_before(hard_deadline)?;
        if !self.is_active() {
            return Err(CredentialLeaseRefreshFailure::Stopped);
        }
        let (current, refresher) = {
            let leases = self
                .credential_leases
                .lock()
                .expect("query credential lease store");
            let lease = leases
                .lease(lease_id)
                .ok_or(CredentialLeaseRefreshFailure::Fatal)?;
            (
                lease.descriptor().clone(),
                lease
                    .refresher()
                    .cloned()
                    .ok_or(CredentialLeaseRefreshFailure::Fatal)?,
            )
        };
        {
            let mut state = self
                .lease_refresh
                .0
                .lock()
                .expect("credential lease refresh state");
            if state.stopped || !self.is_active() {
                return Err(CredentialLeaseRefreshFailure::Stopped);
            }
            if state.round.is_some() {
                return Err(CredentialLeaseRefreshFailure::Fatal);
            }
            state.round = Some(CredentialLeaseRefreshRound {
                lease_id,
                epoch: None,
                phase: CredentialLeaseRefreshPhase::Vending,
                prepared: BTreeSet::new(),
                committed: BTreeSet::new(),
            });
        }

        let refreshed = refresher
            .refresh(&current)
            .map_err(|_| CredentialLeaseRefreshFailure::RetryableProvider)?;
        self.ensure_credential_lease_refresh_before(hard_deadline)?;
        if !valid_credential_lease_refresh(&current, &refreshed) {
            return Err(CredentialLeaseRefreshFailure::Fatal);
        }
        let epoch = refreshed.descriptor().epoch();
        {
            let mut state = self
                .lease_refresh
                .0
                .lock()
                .expect("credential lease refresh state");
            if state.stopped {
                return Err(CredentialLeaseRefreshFailure::Stopped);
            }
            let Some(round) = state.round.as_mut() else {
                return Err(CredentialLeaseRefreshFailure::Stopped);
            };
            if round.lease_id != lease_id || round.phase != CredentialLeaseRefreshPhase::Vending {
                return Err(CredentialLeaseRefreshFailure::Fatal);
            }
            round.epoch = Some(epoch);
            round.phase = CredentialLeaseRefreshPhase::Preparing;
        }

        let sessions = self.sessions();
        if sessions.len()
            != self
                .admitted_len()
                .map_err(|_| CredentialLeaseRefreshFailure::Fatal)?
        {
            return Err(CredentialLeaseRefreshFailure::Fatal);
        }
        self.ensure_credential_lease_refresh_before(hard_deadline)?;
        let prepare = credential_lease_prepare_command(refreshed.envelope())
            .map_err(|_| CredentialLeaseRefreshFailure::Fatal)?;
        for session in &sessions {
            self.ensure_credential_lease_refresh_before(hard_deadline)?;
            if session.session.send(prepare.clone()).is_err() {
                return Err(CredentialLeaseRefreshFailure::Fatal);
            }
        }
        if self
            .wait_for_credential_lease_ack(
                lease_id,
                epoch,
                CredentialLeaseRefreshPhase::Preparing,
                sessions.len(),
                hard_deadline,
            )
            .is_err()
        {
            return Err(CredentialLeaseRefreshFailure::Fatal);
        }

        {
            let mut state = self
                .lease_refresh
                .0
                .lock()
                .expect("credential lease refresh state");
            if state.stopped {
                return Err(CredentialLeaseRefreshFailure::Stopped);
            }
            let Some(round) = state.round.as_mut() else {
                return Err(CredentialLeaseRefreshFailure::Stopped);
            };
            if round.lease_id != lease_id
                || round.epoch != Some(epoch)
                || round.phase != CredentialLeaseRefreshPhase::Preparing
            {
                state.round = None;
                self.lease_refresh.1.notify_all();
                drop(state);
                return Err(CredentialLeaseRefreshFailure::Fatal);
            }
            round.phase = CredentialLeaseRefreshPhase::Committing;
        }
        self.ensure_credential_lease_refresh_before(hard_deadline)?;
        let commit = credential_lease_commit_command(lease_id, epoch)
            .map_err(|_| CredentialLeaseRefreshFailure::Fatal)?;
        for session in &sessions {
            self.ensure_credential_lease_refresh_before(hard_deadline)?;
            if session.session.send(commit.clone()).is_err() {
                return Err(CredentialLeaseRefreshFailure::Fatal);
            }
        }
        if self
            .wait_for_credential_lease_ack(
                lease_id,
                epoch,
                CredentialLeaseRefreshPhase::Committing,
                sessions.len(),
                hard_deadline,
            )
            .is_err()
        {
            return Err(CredentialLeaseRefreshFailure::Fatal);
        }
        let replace_failed = {
            let mut leases = self
                .credential_leases
                .lock()
                .expect("query credential lease store");
            leases
                .replace(refreshed.descriptor().clone(), refreshed.envelope().clone())
                .is_err()
        };
        if replace_failed {
            return Err(CredentialLeaseRefreshFailure::Fatal);
        }
        let mut state = self
            .lease_refresh
            .0
            .lock()
            .expect("credential lease refresh state");
        if state.stopped {
            return Err(CredentialLeaseRefreshFailure::Stopped);
        }
        state.round = None;
        self.lease_refresh.1.notify_all();
        Ok(())
    }

    fn ensure_credential_lease_refresh_before(
        &self,
        hard_deadline: Instant,
    ) -> Result<(), CredentialLeaseRefreshFailure> {
        if Instant::now() >= hard_deadline {
            return Err(CredentialLeaseRefreshFailure::Fatal);
        }
        if self.credential_lease_refresh_stopped() {
            return Err(CredentialLeaseRefreshFailure::Stopped);
        }
        Ok(())
    }

    fn credential_lease_refresh_stopped(&self) -> bool {
        if *self.stop.0.lock().expect("query lifecycle stop lock") {
            return true;
        }
        self.lease_refresh
            .0
            .lock()
            .expect("credential lease refresh state")
            .stopped
    }

    fn clear_credential_lease_refresh_round(&self, lease_id: CredentialLeaseId) {
        let mut state = self
            .lease_refresh
            .0
            .lock()
            .expect("credential lease refresh state");
        if state
            .round
            .as_ref()
            .is_some_and(|round| round.lease_id == lease_id)
        {
            state.round = None;
            self.lease_refresh.1.notify_all();
        }
    }

    fn record_credential_lease_ack(
        &self,
        backend_idx: usize,
        lease_id: CredentialLeaseId,
        epoch: u64,
        phase: CredentialLeaseRefreshPhase,
    ) -> Result<(), String> {
        let mut state = self
            .lease_refresh
            .0
            .lock()
            .expect("credential lease refresh state");
        if state.stopped {
            return Err(
                "credential lease acknowledgement does not match the active refresh".to_string(),
            );
        }
        let round = state.round.as_mut().ok_or_else(|| {
            "credential lease acknowledgement arrived without an active refresh".to_string()
        })?;
        if round.lease_id != lease_id || round.epoch != Some(epoch) || round.phase != phase {
            return Err(
                "credential lease acknowledgement does not match the active refresh".to_string(),
            );
        }
        match phase {
            CredentialLeaseRefreshPhase::Preparing => {
                round.prepared.insert(backend_idx);
            }
            CredentialLeaseRefreshPhase::Committing => {
                round.committed.insert(backend_idx);
            }
            CredentialLeaseRefreshPhase::Vending => {
                return Err("credential lease acknowledgement arrived before prepare".to_string());
            }
        }
        self.lease_refresh.1.notify_all();
        Ok(())
    }

    fn wait_for_credential_lease_ack(
        &self,
        lease_id: CredentialLeaseId,
        epoch: u64,
        phase: CredentialLeaseRefreshPhase,
        expected: usize,
        hard_deadline: Instant,
    ) -> Result<(), String> {
        let deadline = hard_deadline.min(Instant::now() + self.config.attach_timeout());
        let mut state = self
            .lease_refresh
            .0
            .lock()
            .expect("credential lease refresh state");
        loop {
            let round = state.round.as_ref().ok_or_else(|| {
                "credential lease refresh stopped while awaiting acknowledgement".to_string()
            })?;
            if state.stopped
                || round.lease_id != lease_id
                || round.epoch != Some(epoch)
                || round.phase != phase
            {
                return Err(
                    "credential lease refresh acknowledgement barrier lost ownership".to_string(),
                );
            }
            let received = match phase {
                CredentialLeaseRefreshPhase::Preparing => round.prepared.len(),
                CredentialLeaseRefreshPhase::Committing => round.committed.len(),
                CredentialLeaseRefreshPhase::Vending => 0,
            };
            if received == expected {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                return Err("credential lease acknowledgement barrier timed out".to_string());
            }
            let (next, timeout) = self
                .lease_refresh
                .1
                .wait_timeout(state, deadline.saturating_duration_since(now))
                .expect("credential lease refresh wait");
            state = next;
            if timeout.timed_out() {
                return Err("credential lease acknowledgement barrier timed out".to_string());
            }
        }
    }

    fn supervisor_failed(&self, reason: String, kind: SupervisorFailureKind) {
        let cause = match kind {
            SupervisorFailureKind::HeartbeatTimeout => {
                self.metrics.heartbeat_timeout();
                QueryFailureCause::LifecycleHeartbeatTimeout
            }
            SupervisorFailureKind::CoordinatorLost => {
                self.metrics.coordinator_lost();
                QueryFailureCause::RemoteTransportObservation
            }
            SupervisorFailureKind::LocalFailure => {
                self.metrics.local_failure();
                QueryFailureCause::BackendLocalFailure
            }
        };
        if let Some(registry) = self.registry.upgrade() {
            let query_id = self.execution_id.query_id();
            // A LocalFailure is delivered by the same control-stream reader
            // that must receive TerminationAccepted. Record it synchronously,
            // then dispatch cancellation separately so the causal failure is
            // ordered before later liveness observations without deadlocking
            // abort acknowledgement behind this reader.
            let _ = registry.latch_failure_and_cancel_async(query_id, cause, reason);
        } else {
            let _ = self.abort_preserving(reason);
        }
    }

    /// Freezes the classification that the terminal finalizer returned to SQL.
    ///
    /// Stream-close and heartbeat callbacks are observations, not a terminal
    /// decision: a live backend may deliberately suppress delivery and still
    /// require `NoOutcome`. Only the finalizer knows whether convergence ended
    /// in an attestation, a reader/liveness failure, or a bounded timeout.
    fn commit_terminal_decision(&self, decision: TerminalConvergenceDecision) {
        let mut committed = self
            .terminal_decision
            .lock()
            .expect("query lifecycle terminal decision");
        debug_assert!(committed.is_none(), "terminal decision is immutable");
        if committed.is_none() {
            *committed = Some(decision);
        }
    }

    pub fn finalize(&self) -> Result<QueryTerminalSet, DistributedQueryError> {
        self.state
            .compare_exchange(ACTIVE, FINALIZING, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                failed(
                    self.primary_error
                        .lock()
                        .expect("query lifecycle primary error")
                        .clone()
                        .unwrap_or_else(|| {
                            "query lifecycle attempt is already terminal".to_string()
                        }),
                )
            })?;
        // A connector-write terminal may have retained an opaque FE-local
        // capability before entering this method. Stop refresh now, but defer
        // secret cleanup to that capability's Drop after commit/reconciliation.
        self.stop_credential_lease_refresh();
        if let Err(error) = self.wait_for_all_drained(self.config.terminal_drain_timeout()) {
            self.metrics.terminal_finalize_failure();
            self.clear_credential_leases();
            return Err(failed(error));
        }
        let sessions = self.sessions();
        let errors = std::thread::scope(|scope| {
            let handles = sessions
                .into_iter()
                .map(|session| {
                    scope.spawn(move || {
                        session
                            .session
                            .send(control_command(
                                novarocks_proto_models::novarocks::query_control_request::Command::Finalize(
                                    novarocks_proto_models::novarocks::QueryControlFinalize {},
                                ),
                            )?)
                            .map_err(|error| error.to_string())
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .filter_map(|handle| match handle.join() {
                    Ok(Ok(())) => None,
                    Ok(Err(error)) => Some(error),
                    Err(_) => Some("query lifecycle finalize worker panicked".to_string()),
                })
                .collect::<Vec<_>>()
        });
        self.metrics.attempt_terminated();
        if errors.is_empty() {
            let terminal_set =
                match self.wait_for_all_outcomes(self.config.terminal_snapshot_timeout()) {
                    Ok(terminal_set) => terminal_set,
                    Err(failure) => {
                        self.metrics.terminal_finalize_failure();
                        // A terminal convergence failure is itself immutable
                        // query evidence (attestation, liveness, or NoOutcome).
                        // Keep this attempt reachable after its active binding is
                        // dropped so the runner can read the same structured
                        // outcome that determined the SQL failure.
                        self.retain_terminal_ingress.store(true, Ordering::Release);
                        self.state.store(ABORTED, Ordering::Release);
                        self.feedback
                            .lock()
                            .expect("runtime filter feedback state")
                            .close();
                        let decision = failure.into_decision();
                        let primary = format!(
                            "query lifecycle terminal finalization failed: {}",
                            decision.message
                        );
                        let cleanup = self.abort_targets(true, &primary);
                        let message = if cleanup.is_empty() {
                            primary
                        } else {
                            format!(
                                "{primary}; query lifecycle rollback failed: {}",
                                cleanup
                                    .iter()
                                    .map(ToString::to_string)
                                    .collect::<Vec<_>>()
                                    .join("; ")
                            )
                        };
                        self.commit_terminal_decision(TerminalConvergenceDecision {
                            source: decision.source,
                            message: message.clone(),
                        });
                        *self
                            .primary_error
                            .lock()
                            .expect("query lifecycle primary error") = Some(message.clone());
                        self.clear_credential_leases();
                        return Err(failed(message));
                    }
                };
            self.state.store(FINALIZED, Ordering::Release);
            if self.terminal_credential_holds.load(Ordering::Acquire) == 0 {
                self.clear_credential_leases();
            }
            self.feedback
                .lock()
                .expect("runtime filter feedback state")
                .close();
            tracing::info!(
                query_id_high = self.execution_id.query_id().high(),
                query_id_low = self.execution_id.query_id().low(),
                attempt_id = self.execution_id.attempt_id().get(),
                "frontend query lifecycle finalized"
            );
            Ok(terminal_set)
        } else {
            self.metrics.terminal_finalize_failure();
            self.state.store(ABORTED, Ordering::Release);
            self.feedback
                .lock()
                .expect("runtime filter feedback state")
                .close();
            let primary = format!("query lifecycle finalize failed: {}", errors.join("; "));
            let cleanup = self.abort_targets(true, &primary);
            let message = if cleanup.is_empty() {
                primary
            } else {
                format!(
                    "{primary}; query lifecycle rollback failed: {}",
                    cleanup
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join("; ")
                )
            };
            *self
                .primary_error
                .lock()
                .expect("query lifecycle primary error") = Some(message.clone());
            self.clear_credential_leases();
            Err(failed(message))
        }
    }
}

impl ConnectorStorageResolver for AttemptControl {
    fn resolve_vended_s3(
        &self,
        request: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError> {
        if !self.is_active() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "vended storage query attempt is no longer active",
            ));
        }
        let leases = self
            .credential_leases
            .lock()
            .expect("query credential lease store");
        resolve_vended_s3_access(leases.leases(), request)
    }
}

/// Opaque FE-local capability for the final commit/reconciliation of one
/// successfully converged connector write. It never exposes a secret and is
/// deliberately unusable before `FINALIZED` or after terminal cleanup.
struct TerminalCredentialLease {
    control: Arc<AttemptControl>,
}

impl ConnectorStorageResolver for TerminalCredentialLease {
    fn resolve_vended_s3(
        &self,
        request: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError> {
        if self.control.state.load(Ordering::Acquire) != FINALIZED {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "vended storage terminal capability is unavailable for this query attempt",
            ));
        }
        let leases = self
            .control
            .credential_leases
            .lock()
            .expect("query credential lease store");
        resolve_vended_s3_access(leases.leases(), request)
    }
}

impl Drop for TerminalCredentialLease {
    fn drop(&mut self) {
        self.control.release_terminal_credential_hold();
    }
}

fn control_event_reader(control: Weak<AttemptControl>, session: ActiveSession) {
    loop {
        let Some(control) = control.upgrade() else {
            return;
        };
        {
            let terminal = control.terminal.0.lock().expect("query terminal state");
            if terminal.stop_readers {
                return;
            }
        }
        let event = match session.recv(control.config.heartbeat_timeout()) {
            Ok(event) => event,
            Err(error)
                if matches!(
                    error.kind(),
                    QueryLifecycleTransportErrorKind::DeadlineExceeded
                ) =>
            {
                continue;
            }
            Err(error)
                if matches!(error.kind(), QueryLifecycleTransportErrorKind::StreamClosed)
                    && control.terminal_set().is_ok() =>
            {
                // The stream's terminal send side may close immediately after
                // accepting the ACK.  Once every immutable snapshot is stored,
                // transport closure cannot revoke that completed terminal set.
                return;
            }
            Err(error)
                if matches!(error.kind(), QueryLifecycleTransportErrorKind::StreamClosed)
                    && control.state.load(Ordering::Acquire) == FINALIZING =>
            {
                // The unary fallback may still win, so retain this as an
                // observation while the finalizer classifies the eventual
                // terminal result.
                control.record_backend_stream_closed(session.target.backend_idx());
                return;
            }
            Err(error) => {
                if matches!(error.kind(), QueryLifecycleTransportErrorKind::StreamClosed) {
                    control.record_backend_stream_closed(session.target.backend_idx());
                } else {
                    control.record_reader_failure(format!(
                        "query lifecycle control stream lost on backend {} digest {}: {error}",
                        session.target.backend_idx(),
                        hex::encode(session.digest.as_bytes())
                    ));
                }
                return;
            }
        };
        if let Some(
            novarocks_proto_models::novarocks::query_control_response::Event::LocalFailure(failure),
        ) = event.as_proto().event.as_ref()
        {
            control.supervisor_failed(
                format!(
                    "query lifecycle local failure on backend {} ({}): {}",
                    session.target.backend_idx(),
                    failure.code,
                    failure.detail
                ),
                SupervisorFailureKind::LocalFailure,
            );
            // A running failure fences the attempt but does not end terminal
            // delivery: the BE still drains facts and may send a failed
            // immutable snapshot (or its unary retry) before this reader
            // leaves the stream.
            continue;
        }
        let terminal_outcome = matches!(
            event.as_proto().event.as_ref(),
            Some(
                novarocks_proto_models::novarocks::query_control_response::Event::TerminalOutcome(
                    _
                )
            )
        );
        if let Err(error) = control.handle_control_event(&session, event) {
            control.record_reader_failure(error);
            return;
        }
        if terminal_outcome {
            // Store-before-ACK completed this participant's terminal handoff.
            // Retained terminal ingress only needs the immutable participant
            // identity and stored outcome for unary fallback. Release the
            // live session so successful queries do not retain HTTP/2 control
            // streams for the full ingress TTL.
            control.release_session(session.target.backend_idx());
            return;
        }
    }
}

impl AttemptControl {
    pub(super) fn handle_control_event(
        &self,
        session: &ActiveSession,
        event: QueryControlEvent,
    ) -> Result<(), String> {
        match event.as_proto().event.as_ref() {
            Some(novarocks_proto_models::novarocks::query_control_response::Event::HeartbeatAck(
                heartbeat,
            )) => {
                let mut terminal = self.terminal.0.lock().expect("query terminal state");
                let prior = terminal
                    .heartbeat_acks
                    .entry(session.target.backend_idx())
                    .or_insert(0);
                *prior = (*prior).max(heartbeat.sequence);
                self.terminal.1.notify_all();
                Ok(())
            }
            Some(novarocks_proto_models::novarocks::query_control_response::Event::LocalDrained(_)) => {
                let newly_drained = self
                    .terminal
                    .0
                    .lock()
                    .expect("query terminal state")
                    .locally_drained
                    .insert(session.target.backend_idx());
                if newly_drained {
                    self.metrics.terminal_locally_drained();
                }
                self.terminal.1.notify_all();
                Ok(())
            }
            Some(
                novarocks_proto_models::novarocks::query_control_response::Event::TerminationAccepted(
                    accepted,
                ),
            ) => {
                let reason = QueryTerminationReason::try_from(accepted.reason)
                    .map_err(|_| format!("unknown query termination reason {}", accepted.reason))?;
                self.terminal
                    .0
                    .lock()
                    .expect("query terminal state")
                    .termination_accepted
                    .insert(session.target.backend_idx(), reason);
                self.terminal.1.notify_all();
                Ok(())
            }
            Some(
                novarocks_proto_models::novarocks::query_control_response::Event::FragmentObservation(
                    observation,
                ),
            ) => {
                let observation = FragmentLiveObservation::parse(observation.clone())
                    .map_err(|error| error.to_string())?;
                let _ = self.store_fragment_observation(session, observation);
                Ok(())
            }
            Some(
                novarocks_proto_models::novarocks::query_control_response::Event::RuntimeFilterFeedback(
                    _,
                ),
            ) => {
                let participant = self
                    .participant_attempt_ref_for_session(session)
                    .map_err(|error| error.to_string())?;
                let outcome = self
                    .feedback
                    .lock()
                    .expect("runtime filter feedback state")
                    .admit(&event, &participant)?;
                if outcome == RuntimeFilterFeedbackAdmission::RejectedForeignParticipant {
                    let execution_id = participant.execution_id().map_err(|error| error.to_string())?;
                    eprintln!(
                        "NOVAROCKS_RUNTIME_FILTER_FEEDBACK_REJECTED_FOREIGN_PARTICIPANT backend_index={} execution_id={}:{}:{}",
                        session.target.backend_idx(),
                        execution_id.query_id().high(),
                        execution_id.query_id().low(),
                        execution_id.attempt_id().get(),
                    );
                }
                Ok(())
            }
            Some(novarocks_proto_models::novarocks::query_control_response::Event::TerminalOutcome(
                outcome,
            )) => {
                let outcome = ParticipantTerminalOutcome::parse(outcome.clone())
                    .map_err(|error| error.to_string())?;
                self.validate_terminal_outcome_session(session, &outcome)?;
                #[cfg(debug_assertions)]
                if let Some(scope) = claim_terminal_snapshot_conflict(session, &outcome)? {
                    let conflict = conflicting_terminal_outcome(&outcome)?;
                    eprintln!(
                        "NOVAROCKS_QUERY_TERMINAL_SNAPSHOT_CONFLICT_INJECTED execution_id={}:{}:{} backend_index={} process_id={} token={}",
                        outcome.execution_id().query_id().high(),
                        outcome.execution_id().query_id().low(),
                        outcome.execution_id().attempt_id().get(),
                        scope.backend_index,
                        scope.process_id,
                        scope.token,
                    );
                    return self
                        .store_terminal_outcome_conflict(outcome, conflict)
                        .map_err(|error| error.to_string());
                }
                let _stored = self
                    .store_terminal_outcome(outcome.clone())
                    .map_err(|error| error.to_string())?;
                if claim_terminal_ack_drop(session, &outcome)? {
                    return Ok(());
                }
                match session.session.send(terminal_ack_command(&outcome)?)
                {
                    Ok(()) => Ok(()),
                    Err(error)
                        if matches!(error.kind(), QueryLifecycleTransportErrorKind::StreamClosed) =>
                    {
                        // The terminal outcome is already durably retained.
                        // A backend may close the command side immediately after
                        // emitting it, so an ACK send cannot revoke that completed
                        // terminal set.
                        Ok(())
                    }
                    Err(error) => Err(format!(
                        "query lifecycle terminal ACK failed for backend {}: {error}",
                        session.target.backend_idx()
                    )),
                }
            }
            Some(novarocks_proto_models::novarocks::query_control_response::Event::LocalFailure(
                failure,
            )) => Err(format!(
                "query lifecycle local failure on backend {} ({}): {}",
                session.target.backend_idx(),
                failure.code,
                failure.detail
            )),
            Some(novarocks_proto_models::novarocks::query_control_response::Event::ControlReady(_)) => {
                Err(format!(
                    "backend {} emitted duplicate ControlReady after attachment",
                    session.target.backend_idx()
                ))
            }
            Some(novarocks_proto_models::novarocks::query_control_response::Event::CatalogReady(_)) => {
                Err(format!(
                    "backend {} emitted CatalogReady before catalog lifecycle support was enabled",
                    session.target.backend_idx()
                ))
            }
            Some(novarocks_proto_models::novarocks::query_control_response::Event::CatalogLoadFailed(
                failure,
            )) => Err(format!(
                "backend {} reported catalog load failure ({}): {}",
                session.target.backend_idx(),
                failure.reason, failure.safe_detail
            )),
            Some(
                novarocks_proto_models::novarocks::query_control_response::Event::CredentialLeasePrepared(
                    prepared,
                ),
            ) => self.record_credential_lease_ack(
                session.target.backend_idx(),
                credential_lease_id_from_wire(&prepared.lease_id)?,
                prepared.epoch,
                CredentialLeaseRefreshPhase::Preparing,
            ),
            Some(
                novarocks_proto_models::novarocks::query_control_response::Event::CredentialLeaseCommitted(
                    committed,
                ),
            ) => self.record_credential_lease_ack(
                session.target.backend_idx(),
                credential_lease_id_from_wire(&committed.lease_id)?,
                committed.epoch,
                CredentialLeaseRefreshPhase::Committing,
            ),
            None => Err("validated query control event is missing its oneof".to_string()),
        }
    }

    fn record_reader_failure(&self, reason: String) {
        let mut terminal = self.terminal.0.lock().expect("query terminal state");
        if terminal.reader_failure.is_none() {
            terminal.reader_failure = Some(reason);
        }
        self.terminal.1.notify_all();
    }

    fn record_backend_stream_closed(&self, backend_idx: usize) {
        let mut terminal = self.terminal.0.lock().expect("query terminal state");
        terminal.backend_stream_closed.insert(backend_idx);
        self.terminal.1.notify_all();
        // A terminal reader may close after another participant's immutable
        // outcome has already put this attempt into Finalizing. Wake the
        // heartbeat owner in that narrow state to continue liveness
        // observation; the finalizer alone commits the public classification.
        // Earlier stream closes remain on their ordinary protocol paths and
        // must not perturb stage/start recovery.
        if self.state.load(Ordering::Acquire) == FINALIZING && !terminal.outcomes.is_empty() {
            self.stop.1.notify_all();
        }
    }

    #[allow(
        dead_code,
        reason = "Retained for explicit terminal-reader shutdown during lifecycle tests."
    )]
    fn stop_readers(&self) {
        self.terminal
            .0
            .lock()
            .expect("query terminal state")
            .stop_readers = true;
        self.terminal.1.notify_all();
    }

    #[allow(
        dead_code,
        reason = "Retained for explicit terminal-reader joining during lifecycle tests."
    )]
    fn join_readers(&self) {
        self.stop_readers();
        let readers =
            std::mem::take(&mut *self.readers.lock().expect("query control event readers"));
        for reader in readers {
            let _ = reader.join();
        }
    }
}

#[cfg(debug_assertions)]
fn claim_terminal_ack_drop(
    session: &ActiveSession,
    outcome: &ParticipantTerminalOutcome,
) -> Result<bool, String> {
    use novarocks_failpoint::{QueryLifecycleFaultKind, claim_matching_fault};

    let Some(root) = novarocks_failpoint::configured_root() else {
        return Ok(false);
    };
    let backend_index = session.target.backend_idx();
    let process_id = session.target.process_id();
    let Some(scope) = claim_matching_fault(
        &root,
        QueryLifecycleFaultKind::TerminalAckDrop,
        protocol_execution_id(outcome.execution_id())?,
        backend_index,
        process_id,
    )
    .map_err(|error| format!("claim terminal ACK drop fault: {error}"))?
    else {
        return Ok(false);
    };
    eprintln!(
        "NOVAROCKS_QUERY_TERMINAL_ACK_DROPPED execution_id={}:{}:{} backend_index={} process_id={} token={}",
        outcome.execution_id().query_id().high(),
        outcome.execution_id().query_id().low(),
        outcome.execution_id().attempt_id().get(),
        backend_index,
        scope.process_id,
        scope.token,
    );
    Ok(true)
}

#[cfg(debug_assertions)]
fn claim_terminal_snapshot_conflict(
    session: &ActiveSession,
    outcome: &ParticipantTerminalOutcome,
) -> Result<Option<novarocks_failpoint::QueryLifecycleFaultScope>, String> {
    use novarocks_failpoint::{QueryLifecycleFaultKind, claim_matching_fault};

    let Some(root) = novarocks_failpoint::configured_root() else {
        return Ok(None);
    };
    let backend_index = session.target.backend_idx();
    let process_id = session.target.process_id();
    claim_matching_fault(
        &root,
        QueryLifecycleFaultKind::TerminalSnapshotConflict,
        protocol_execution_id(outcome.execution_id())?,
        backend_index,
        process_id,
    )
    .map_err(|error| format!("claim terminal snapshot conflict fault: {error}"))
}

#[cfg(debug_assertions)]
fn protocol_execution_id(execution_id: QueryExecutionId) -> Result<QueryExecutionId, String> {
    Ok(execution_id)
}

#[cfg(debug_assertions)]
fn conflicting_terminal_outcome(
    outcome: &ParticipantTerminalOutcome,
) -> Result<ParticipantTerminalOutcome, String> {
    let attestation = novarocks_proto_codec::lifecycle::NegativeAttestation::parse(
        novarocks_proto_models::novarocks::NegativeAttestation {
            reason:
                novarocks_proto_models::novarocks::NegativeAttestationReason::TerminalStateInvalid
                    as i32,
            detail: "same participant produced a conflicting terminal outcome".to_string(),
            detail_truncated: false,
            participant: Some(outcome.participant().as_proto().clone()),
        },
    )
    .map_err(|error| error.to_string())?;
    ParticipantTerminalOutcome::parse(novarocks_proto_models::novarocks::ParticipantTerminalOutcome {
        outcome: Some(
            novarocks_proto_models::novarocks::participant_terminal_outcome::Outcome::NegativeAttestation(
                attestation.as_proto().clone(),
            ),
        ),
        snapshot: None,
    })
    .map_err(|error| error.to_string())
}

#[cfg(not(debug_assertions))]
fn claim_terminal_ack_drop(
    _session: &ActiveSession,
    _outcome: &ParticipantTerminalOutcome,
) -> Result<bool, String> {
    Ok(false)
}

impl ActiveQueryAttemptControl for AttemptControl {
    fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    fn request_abort(&self, failure: LatchedQueryFailure) {
        let enriched = self.abort_preserving(failure.message().to_string());
        if let Some(registry) = self.registry.upgrade() {
            let _ = registry.preserve_failure_context(
                self.execution_id.query_id(),
                failure.id(),
                enriched,
            );
        }
    }

    fn report_terminal_outcome(
        &self,
        outcome: ParticipantTerminalOutcome,
    ) -> Result<bool, DistributedQueryError> {
        self.store_terminal_outcome(outcome)
            .map(|outcome| outcome.is_accepted())
    }

    fn retain_terminal_ingress(&self) -> bool {
        self.retain_terminal_ingress.load(Ordering::Acquire) || self.terminal_set().is_ok()
    }

    fn convergence_snapshot(&self) -> Option<QueryLifecycleConvergenceSnapshot> {
        let outcomes = self
            .terminal
            .0
            .lock()
            .expect("query terminal state")
            .outcomes
            .values()
            .map(|retained| retained.outcome.clone())
            .collect::<Vec<_>>();
        let decision = self
            .terminal_decision
            .lock()
            .expect("query lifecycle terminal decision")
            .clone();
        // A negative attestation is already an immutable participant outcome,
        // so its source is observable before finalization commits the complete
        // decision. NoOutcome and liveness remain finalizer-owned because
        // neither has an equivalent immutable participant record.
        let error_source = decision
            .as_ref()
            .map(|decision| decision.source)
            .or_else(|| {
                outcomes
                    .iter()
                    .any(|outcome| outcome.negative_attestation().is_some())
                    .then_some(QueryLifecycleConvergenceErrorSource::BackendAttestation)
            });
        if outcomes.is_empty() && self.state.load(Ordering::Acquire) == ACTIVE && decision.is_none()
        {
            return None;
        }
        let runtime_filter = match self.terminal_set() {
            Ok(terminal_set) => RuntimeFilterTerminalRollupSnapshot::Available(
                terminal_set.runtime_filter_terminal_rollup(),
            ),
            Err(_)
                if outcomes
                    .iter()
                    .any(|outcome| outcome.negative_attestation().is_some()) =>
            {
                RuntimeFilterTerminalRollupSnapshot::Unavailable(
                    RuntimeFilterTerminalRollupUnavailable::NegativeAttestation,
                )
            }
            Err(_) => RuntimeFilterTerminalRollupSnapshot::Unavailable(
                RuntimeFilterTerminalRollupUnavailable::TerminalOutcomesIncomplete,
            ),
        };
        Some(QueryLifecycleConvergenceSnapshot {
            execution_id: self.execution_id,
            error_source,
            primary_error: decision.map(|decision| decision.message).or_else(|| {
                self.primary_error
                    .lock()
                    .expect("query lifecycle primary error")
                    .clone()
            }),
            participant_outcomes: outcomes,
            runtime_filter,
            metrics: self.metrics.snapshot(),
        })
    }
}

pub(super) fn spawn_supervisor(control: &Arc<AttemptControl>) -> JoinHandle<()> {
    let weak = Arc::downgrade(control);
    std::thread::Builder::new()
        .name(format!(
            "query-control-{}/{}-{}",
            control.execution_id.query_id().high(),
            control.execution_id.query_id().low(),
            control.execution_id.attempt_id().get()
        ))
        .spawn(move || heartbeat_supervisor(weak))
        .expect("spawn frontend query lifecycle supervisor")
}

/// Starts at most one attempt-local refresh owner. It is deliberately tied to
/// the same stop latch as heartbeat supervision, so FE terminalization and
/// control-owner loss leave no orphan provider refresh task behind.
pub(super) fn spawn_credential_lease_supervisor(
    control: &Arc<AttemptControl>,
) -> Option<JoinHandle<()>> {
    control.next_credential_lease_refresh()?;
    let weak = Arc::downgrade(control);
    Some(
        std::thread::Builder::new()
            .name(format!(
                "credential-lease-refresh-{}/{}-{}",
                control.execution_id.query_id().high(),
                control.execution_id.query_id().low(),
                control.execution_id.attempt_id().get(),
            ))
            .spawn(move || credential_lease_refresh_supervisor(weak))
            .expect("spawn frontend credential lease refresh supervisor"),
    )
}

fn credential_lease_refresh_supervisor(control: Weak<AttemptControl>) {
    loop {
        let Some(control) = control.upgrade() else {
            return;
        };
        let Some(schedule) = control.next_credential_lease_refresh() else {
            return;
        };
        if !control.wait_until_stopped_or_credential_refresh(schedule.soft_delay) {
            return;
        }

        let mut retry_delay = CREDENTIAL_LEASE_RETRY_INITIAL;
        loop {
            if control.credential_lease_refresh_stopped() {
                return;
            }
            if Instant::now() >= schedule.hard_deadline {
                let _ = control.abort_preserving("credential lease refresh failed".to_string());
                return;
            }
            match control.refresh_credential_lease(schedule.lease_id, schedule.hard_deadline) {
                Ok(()) => break,
                Err(CredentialLeaseRefreshFailure::Stopped) => return,
                Err(CredentialLeaseRefreshFailure::Fatal) => {
                    let _ = control.abort_preserving("credential lease refresh failed".to_string());
                    return;
                }
                Err(CredentialLeaseRefreshFailure::RetryableProvider) => {}
            }
            let remaining = schedule
                .hard_deadline
                .saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let _ = control.abort_preserving("credential lease refresh failed".to_string());
                return;
            }
            if !control.wait_until_stopped_or_credential_refresh(retry_delay.min(remaining)) {
                return;
            }
            retry_delay = retry_delay
                .saturating_mul(2)
                .min(CREDENTIAL_LEASE_RETRY_MAX);
        }
    }
}

fn heartbeat_supervisor(control: Weak<AttemptControl>) {
    let Some(control) = control.upgrade() else {
        return;
    };
    let started = Instant::now();
    let mut sequence = 0u64;
    while control.wait_heartbeat_interval() {
        sequence = match sequence.checked_add(1) {
            Some(sequence) => sequence,
            None => {
                control.supervisor_failed(
                    "query lifecycle heartbeat sequence exhausted".to_string(),
                    SupervisorFailureKind::CoordinatorLost,
                );
                return;
            }
        };
        let sessions = control.sessions();
        for session in &sessions {
            let command = match control_command(
                novarocks_proto_models::novarocks::query_control_request::Command::Heartbeat(
                    novarocks_proto_models::novarocks::QueryControlHeartbeat {
                        sequence,
                        sent_mono_ns: started.elapsed().as_nanos() as u64,
                    },
                ),
            ) {
                Ok(command) => command,
                Err(error) => {
                    control.supervisor_failed(error, SupervisorFailureKind::CoordinatorLost);
                    return;
                }
            };
            if let Err(error) = session.session.send(command) {
                if matches!(error.kind(), QueryLifecycleTransportErrorKind::StreamClosed) {
                    control.supervisor_failed(
                        format!(
                            "backend {} lost after heartbeat timeout",
                            session.target.backend_idx()
                        ),
                        SupervisorFailureKind::HeartbeatTimeout,
                    );
                    return;
                }
                control.supervisor_failed(
                    format!(
                        "query lifecycle control stream failed for backend {} digest {}: {error}",
                        session.target.backend_idx(),
                        hex::encode(session.digest.as_bytes())
                    ),
                    SupervisorFailureKind::CoordinatorLost,
                );
                return;
            }
        }
        for session in &sessions {
            match control.wait_for_heartbeat(
                session.target.backend_idx(),
                sequence,
                control.config.heartbeat_timeout(),
            ) {
                Ok(()) => {}
                Err(error) => {
                    let timeout = error.contains("heartbeat timeout")
                        || error.contains("control stream closed");
                    let failure = if timeout {
                        format!(
                            "backend {} lost after heartbeat timeout",
                            session.target.backend_idx()
                        )
                    } else {
                        format!(
                            "query lifecycle control event reader failed on backend {} digest {}: {error}",
                            session.target.backend_idx(),
                            hex::encode(session.digest.as_bytes())
                        )
                    };
                    control.supervisor_failed(
                        failure,
                        if timeout {
                            SupervisorFailureKind::HeartbeatTimeout
                        } else {
                            SupervisorFailureKind::CoordinatorLost
                        },
                    );
                    return;
                }
            }
        }
    }
}

pub(super) struct FrontendQueryLifecycleLeaseGuard {
    control: Arc<AttemptControl>,
    supervisor: Option<JoinHandle<()>>,
    credential_lease_supervisor: Option<JoinHandle<()>>,
    _registry_binding: ActiveQueryAttemptBinding,
}

impl FrontendQueryLifecycleLeaseGuard {
    pub fn lease(
        control: Arc<AttemptControl>,
        supervisor: JoinHandle<()>,
        credential_lease_supervisor: Option<JoinHandle<()>>,
        registry_binding: ActiveQueryAttemptBinding,
    ) -> QueryLifecycleLease {
        QueryLifecycleLease::new(Box::new(Self {
            control,
            supervisor: Some(supervisor),
            credential_lease_supervisor,
            _registry_binding: registry_binding,
        }))
    }

    fn stop_and_join(&mut self) {
        self.control.stop_supervisor();
        let Some(supervisor) = self.supervisor.take() else {
            return;
        };
        let (done_tx, done_rx) = mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let _ = supervisor.join();
            let _ = done_tx.send(());
        });
        let bound = self
            .control
            .config
            .heartbeat_timeout()
            .saturating_add(self.control.config.attach_timeout());
        let _ = done_rx.recv_timeout(bound.max(Duration::from_millis(1)));
        if let Some(refresh) = self.credential_lease_supervisor.take() {
            let (done_tx, done_rx) = mpsc::sync_channel(1);
            std::thread::spawn(move || {
                let _ = refresh.join();
                let _ = done_tx.send(());
            });
            let _ = done_rx.recv_timeout(bound.max(Duration::from_millis(1)));
        }
    }
}

impl QueryLifecycleLeaseGuard for FrontendQueryLifecycleLeaseGuard {
    fn retain_terminal_storage_resolver(&self) -> Option<Arc<dyn ConnectorStorageResolver>> {
        self.control.retain_terminal_storage_resolver()
    }

    fn finalize(mut self: Box<Self>) -> Result<QueryTerminalSet, DistributedQueryError> {
        let result = self.control.finalize();
        self.stop_and_join();
        result
    }

    fn abort_preserving(mut self: Box<Self>, primary_error: String) -> QueryLifecycleAbortOutcome {
        let outcome = self.control.abort_with_terminal_outcome(primary_error);
        self.stop_and_join();
        outcome
    }
}

impl Drop for FrontendQueryLifecycleLeaseGuard {
    fn drop(&mut self) {
        self.stop_and_join();
        if self.control.is_active() {
            let primary = "query lifecycle lease dropped before finalize".to_string();
            let outcome = self.control.abort_preserving(primary.clone());
            if outcome != primary {
                tracing::error!(
                    query_id_high = self.control.execution_id.query_id().high(),
                    query_id_low = self.control.execution_id.query_id().low(),
                    attempt_id = self.control.execution_id.attempt_id().get(),
                    error = %outcome,
                    "frontend query lifecycle drop cleanup was incomplete"
                );
            }
        }
    }
}

fn failed(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::Failed, message)
}

fn contract_violation(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

/// Phase-marker diagnostics are a Core-local orchestration residual. The
/// native transport carries the Protocol identity exclusively; this conversion
/// exists only until the marker registry moves with the remaining lease API in
/// CLS-R2.
fn marker_execution_id(
    execution_id: QueryExecutionId,
) -> Result<novarocks_proto_codec::lifecycle::QueryExecutionId, DistributedQueryError> {
    let attempt = novarocks_proto_codec::lifecycle::AttemptId::new(execution_id.attempt_id().get())
        .map_err(|error| contract_violation(error.to_string()))?;
    novarocks_proto_codec::lifecycle::QueryExecutionId::new(execution_id.query_id(), attempt)
        .map_err(|error| contract_violation(error.to_string()))
}

fn control_command(
    command: novarocks_proto_models::novarocks::query_control_request::Command,
) -> Result<QueryControlCommand, String> {
    QueryControlCommand::parse(novarocks_proto_models::novarocks::QueryControlRequest {
        command: Some(command),
    })
    .map_err(|error| error.to_string())
}

fn tls_control_command(
    command: novarocks_proto_models::novarocks::query_control_request::Command,
) -> Result<QueryControlCommand, String> {
    QueryControlCommand::parse_tls(novarocks_proto_models::novarocks::QueryControlRequest {
        command: Some(command),
    })
    .map_err(|error| error.to_string())
}

fn credential_lease_prepare_command(
    envelope: &CredentialLeaseSecretEnvelope,
) -> Result<QueryControlCommand, String> {
    tls_control_command(
        novarocks_proto_models::novarocks::query_control_request::Command::CredentialLeasePrepare(
            novarocks_proto_models::novarocks::CredentialLeasePrepare {
                envelope: Some(
                    novarocks_proto_codec::lifecycle::encode_credential_lease_secret_envelope(
                        envelope,
                    ),
                ),
            },
        ),
    )
}

fn credential_lease_commit_command(
    lease_id: CredentialLeaseId,
    epoch: u64,
) -> Result<QueryControlCommand, String> {
    tls_control_command(
        novarocks_proto_models::novarocks::query_control_request::Command::CredentialLeaseCommit(
            novarocks_proto_models::novarocks::CredentialLeaseCommit {
                lease_id: lease_id.as_bytes().to_vec(),
                epoch,
            },
        ),
    )
}

fn valid_credential_lease_refresh(
    current: &novarocks_spi::connector::CredentialLeaseDescriptor,
    refreshed: &QueryCredentialLeaseRefresh,
) -> bool {
    let descriptor = refreshed.descriptor();
    descriptor.has_same_refresh_scope(current)
        && descriptor.epoch() == current.epoch().saturating_add(1)
        && descriptor.not_after_unix_ms() > current.not_after_unix_ms()
        && refreshed.envelope().matches_descriptor(descriptor)
}

fn credential_lease_refresh_timing(
    lease_id: CredentialLeaseId,
    ttl: Duration,
) -> CredentialLeaseRefreshTiming {
    let soft_margin = (ttl / 5).clamp(
        CREDENTIAL_LEASE_SOFT_MARGIN_MIN,
        CREDENTIAL_LEASE_SOFT_MARGIN_MAX,
    );
    let hard_margin = (ttl / 20).clamp(
        CREDENTIAL_LEASE_HARD_MARGIN_MIN,
        CREDENTIAL_LEASE_HARD_MARGIN_MAX,
    );
    let max_jitter = (ttl / 20).min(CREDENTIAL_LEASE_JITTER_MAX);
    // Spread simultaneous non-secret lease identifiers deterministically. The
    // cap is independent of provider data and is always contained in the
    // configured jitter budget.
    let jitter_seed = lease_id
        .as_bytes()
        .iter()
        .fold(0_u64, |seed, byte| seed.rotate_left(5) ^ u64::from(*byte));
    let jitter = if max_jitter.is_zero() {
        Duration::ZERO
    } else {
        Duration::from_nanos(jitter_seed % (max_jitter.as_nanos() as u64 + 1))
    };
    CredentialLeaseRefreshTiming {
        soft_delay: ttl.saturating_sub(soft_margin.saturating_add(jitter)),
        hard_delay: ttl.saturating_sub(hard_margin),
    }
}

fn credential_lease_id_from_wire(bytes: &[u8]) -> Result<CredentialLeaseId, String> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| "credential lease acknowledgement has an invalid lease id".to_string())?;
    CredentialLeaseId::try_from_bytes(bytes)
        .map_err(|_| "credential lease acknowledgement has an invalid lease id".to_string())
}

fn terminal_ack_command(
    outcome: &ParticipantTerminalOutcome,
) -> Result<QueryControlCommand, String> {
    let ack = QueryTerminalAck::parse(novarocks_proto_models::novarocks::QueryControlTerminalAck {
        participant: Some(outcome.participant().as_proto().clone()),
    })
    .map_err(|error| error.to_string())?;
    control_command(
        novarocks_proto_models::novarocks::query_control_request::Command::TerminalAck(
            ack.as_proto().clone(),
        ),
    )
}

#[cfg(test)]
mod credential_lease_refresh_timing_tests {
    use super::*;

    fn lease_id(byte: u8) -> CredentialLeaseId {
        CredentialLeaseId::try_from_bytes([byte; 16]).expect("nonzero test lease id")
    }

    #[test]
    fn timing_uses_the_approved_soft_hard_and_jitter_windows() {
        let ttl = Duration::from_secs(100);
        let first = credential_lease_refresh_timing(lease_id(1), ttl);
        let second = credential_lease_refresh_timing(lease_id(1), ttl);

        assert_eq!(first, second, "lease jitter must be deterministic");
        assert_eq!(first.hard_delay, Duration::from_secs(95));
        assert!(first.soft_delay <= Duration::from_secs(80));
        assert!(first.soft_delay >= Duration::from_secs(75));
    }

    #[test]
    fn timing_clamps_short_and_long_ttls_without_crossing_the_hard_boundary() {
        let short = credential_lease_refresh_timing(lease_id(2), Duration::from_secs(1));
        assert_eq!(short.soft_delay, Duration::ZERO);
        assert_eq!(short.hard_delay, Duration::ZERO);

        let long = credential_lease_refresh_timing(lease_id(3), Duration::from_secs(60 * 60));
        assert_eq!(long.hard_delay, Duration::from_secs(60 * 60 - 30));
        assert!(long.soft_delay <= Duration::from_secs(60 * 60 - 5 * 60));
        assert!(long.soft_delay >= Duration::from_secs(60 * 60 - 5 * 60 - 30));
        assert!(long.soft_delay < long.hard_delay);
    }
}
