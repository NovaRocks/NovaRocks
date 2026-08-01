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
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use novarocks::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use novarocks::query_execution::lifecycle::metrics::FrontendQueryLifecycleMetricsSnapshot;
use novarocks::query_execution::lifecycle::{
    FragmentLiveObservation, ParticipantManifestDigest, QueryAbortRequest, QueryControlCommand,
    QueryControlEvent, QueryExecutionId, QueryLifecycleAbortOutcome, QueryLifecycleLease,
    QueryLifecycleLeaseGuard, QueryTerminalSet, QueryTerminalSnapshot, QueryTerminationReason,
};
#[cfg(debug_assertions)]
use novarocks::query_execution::lifecycle::{FragmentTerminalOutcome, FragmentTerminalSnapshot};

use super::barrier::FrontendQueryLifecycleConfig;
use super::manifest::MaterializedParticipant;
use super::{
    QueryControlSession, QueryLifecycleTarget, QueryLifecycleTransport,
    QueryLifecycleTransportErrorKind,
};
use crate::coordinator::query_registry::ActiveQueryAttemptBinding;
use crate::coordinator::query_registry::{ActiveQueryAttemptControl, FrontendQueryRegistry};

const ACTIVE: u8 = 0;
const ABORTED: u8 = 1;
const FINALIZING: u8 = 2;
const FINALIZED: u8 = 3;

#[derive(Clone, Copy)]
enum SupervisorFailureKind {
    HeartbeatTimeout,
    CoordinatorLost,
    LocalFailure,
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
            target: participant.target,
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
    locally_drained: BTreeSet<usize>,
    termination_accepted: BTreeMap<usize, QueryTerminationReason>,
    snapshots: BTreeMap<usize, QueryTerminalSnapshot>,
    reader_failure: Option<String>,
    stop_readers: bool,
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FragmentObservationSnapshot {
    pub latest: BTreeMap<(usize, novarocks_types::UniqueId), FragmentLiveObservation>,
    pub accepted: u64,
    pub idempotent: u64,
    pub stale: u64,
    pub conflict: u64,
    pub rejected: u64,
}

/// The result of storing a participant terminal snapshot.  The distinction is
/// intentionally preserved: both cases must be acknowledged, while only the
/// first one contributes to the terminal set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TerminalSnapshotStoreOutcome {
    Accepted,
    AlreadyAccepted,
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
pub(super) struct FrontendLifecycleMetrics {
    snapshot: Mutex<FrontendQueryLifecycleMetricsSnapshot>,
}

impl FrontendLifecycleMetrics {
    pub(super) fn process_shared() -> Arc<Self> {
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

    pub fn terminal_snapshot_stored(&self, outcome: TerminalSnapshotStoreOutcome) {
        self.update(|snapshot| match outcome {
            TerminalSnapshotStoreOutcome::Accepted => snapshot.terminal_snapshots_accepted += 1,
            TerminalSnapshotStoreOutcome::AlreadyAccepted => {
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

    #[cfg(test)]
    pub fn snapshot(&self) -> FrontendQueryLifecycleMetricsSnapshot {
        *self.snapshot.lock().expect("frontend lifecycle metrics")
    }

    fn update(&self, update: impl FnOnce(&mut FrontendQueryLifecycleMetricsSnapshot)) {
        let snapshot = {
            let mut snapshot = self.snapshot.lock().expect("frontend lifecycle metrics");
            update(&mut snapshot);
            *snapshot
        };
        novarocks::service::publish_frontend_query_lifecycle_metrics(snapshot);
    }
}

pub(super) struct AttemptControl {
    execution_id: QueryExecutionId,
    transport: Arc<dyn QueryLifecycleTransport>,
    registry: Weak<FrontendQueryRegistry>,
    config: FrontendQueryLifecycleConfig,
    attempted: Mutex<BTreeMap<usize, MaterializedParticipant>>,
    sessions: Mutex<BTreeMap<usize, ActiveSession>>,
    state: AtomicU8,
    // A running abort may finish its caller before every BE has delivered an
    // immutable terminal snapshot. Keep the active ingress binding alive so
    // stream delivery and unary fallback remain valid for the bounded BE
    // retention interval.
    retain_terminal_ingress: AtomicBool,
    primary_error: Mutex<Option<String>>,
    stop: (Mutex<bool>, Condvar),
    terminal: (Mutex<TerminalState>, Condvar),
    observations: Mutex<FragmentObservationState>,
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
            attempted: Mutex::new(BTreeMap::new()),
            sessions: Mutex::new(BTreeMap::new()),
            state: AtomicU8::new(ACTIVE),
            retain_terminal_ingress: AtomicBool::new(false),
            primary_error: Mutex::new(None),
            stop: (Mutex::new(false), Condvar::new()),
            terminal: (Mutex::new(TerminalState::default()), Condvar::new()),
            observations: Mutex::new(FragmentObservationState::default()),
            readers: Mutex::new(Vec::new()),
            metrics,
        })
    }

    pub fn is_active(&self) -> bool {
        self.state.load(Ordering::Acquire) == ACTIVE
    }

    pub(super) const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub fn set_attempted(&self, participants: &[MaterializedParticipant]) {
        let mut attempted = self.attempted.lock().expect("attempted participant set");
        attempted.extend(
            participants
                .iter()
                .cloned()
                .map(|participant| (participant.target.backend_idx(), participant)),
        );
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
            .attempted
            .lock()
            .expect("attempted participant set")
            .get(&backend_idx)
            .cloned();
        let valid = participant.is_some_and(|participant| {
            participant.target == session.target
                && participant.digest == session.digest
                && participant.request.manifest().execution_id() == self.execution_id
                && observation.execution_id() == self.execution_id
                && observation.init_digest() == participant.digest
                && observation.backend() == participant.request.manifest().backend()
                && participant
                    .request
                    .manifest()
                    .expected_fragment_instance_ids()
                    .contains(&observation.fragment_instance_id())
        });
        if !valid {
            return self
                .record_fragment_observation_outcome(FragmentObservationStoreOutcome::Rejected);
        }

        // A terminal snapshot is the immutable authority for its participant.
        // Keep the last live sample for diagnostics, but fence all later
        // updates rather than allowing telemetry to race terminal finalization.
        if self
            .terminal
            .0
            .lock()
            .expect("query terminal state")
            .snapshots
            .contains_key(&backend_idx)
        {
            return self
                .record_fragment_observation_outcome(FragmentObservationStoreOutcome::Rejected);
        }

        let key = (backend_idx, observation.fragment_instance_id());
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

    /// Stores a terminal snapshot through the same path used by the stream
    /// reader and the unary fallback ingress.  It validates the immutable
    /// snapshot before changing any FE state, then makes same-digest retries
    /// idempotent and rejects conflicting payloads for the participant.
    pub(crate) fn store_terminal_snapshot(
        &self,
        snapshot: QueryTerminalSnapshot,
    ) -> Result<TerminalSnapshotStoreOutcome, DistributedQueryError> {
        snapshot
            .validate()
            .map_err(|error| failed(error.to_string()))?;
        if snapshot.execution_id() != self.execution_id {
            return Err(contract_violation(
                "query terminal snapshot execution id differs from active lifecycle attempt",
            ));
        }
        let backend_idx = self.terminal_snapshot_backend_idx(&snapshot)?;
        self.store_terminal_snapshot_at(backend_idx, snapshot)
    }

    fn terminal_snapshot_backend_idx(
        &self,
        snapshot: &QueryTerminalSnapshot,
    ) -> Result<usize, DistributedQueryError> {
        let (backend_idx, participant) = self
            .attempted
            .lock()
            .expect("attempted participant set")
            .iter()
            .find(|(_, participant)| {
                participant.digest == snapshot.init_digest()
                    && participant.request.manifest().backend() == snapshot.backend()
            })
            .map(|(backend_idx, participant)| (*backend_idx, participant.clone()))
            .ok_or_else(|| {
                contract_violation(
                    "query terminal snapshot is not owned by an attempted lifecycle participant",
                )
            })?;
        if participant.request.manifest().execution_id() != self.execution_id {
            return Err(contract_violation(
                "attempted participant manifest execution id differs from active lifecycle attempt",
            ));
        }
        Ok(backend_idx)
    }

    fn store_terminal_snapshot_at(
        &self,
        backend_idx: usize,
        snapshot: QueryTerminalSnapshot,
    ) -> Result<TerminalSnapshotStoreOutcome, DistributedQueryError> {
        let mut terminal = self.terminal.0.lock().expect("query terminal store");
        if let Some(reason) = &terminal.reader_failure {
            return Err(contract_violation(format!(
                "query lifecycle terminal ingress is already failed: {reason}"
            )));
        }
        let outcome = match terminal.snapshots.get(&backend_idx) {
            Some(existing) if existing.digest() == snapshot.digest() => {
                TerminalSnapshotStoreOutcome::AlreadyAccepted
            }
            Some(_) => {
                drop(terminal);
                self.metrics.terminal_snapshot_conflict();
                return Err(contract_violation(
                    "query terminal snapshot conflicts with an already stored participant snapshot",
                ));
            }
            None => {
                terminal.snapshots.insert(backend_idx, snapshot);
                TerminalSnapshotStoreOutcome::Accepted
            }
        };
        drop(terminal);
        if outcome == TerminalSnapshotStoreOutcome::Accepted {
            super::barrier::record_lifecycle_phase_marker_for_execution(
                "terminal-retained",
                self.execution_id,
            )?;
        }
        self.terminal.1.notify_all();
        self.metrics.terminal_snapshot_stored(outcome);
        Ok(outcome)
    }

    #[cfg(debug_assertions)]
    fn store_terminal_snapshot_conflict(
        &self,
        snapshot: QueryTerminalSnapshot,
        conflict: QueryTerminalSnapshot,
    ) -> Result<(), DistributedQueryError> {
        snapshot
            .validate()
            .map_err(|error| failed(error.to_string()))?;
        conflict
            .validate()
            .map_err(|error| failed(error.to_string()))?;
        let backend_idx = self.terminal_snapshot_backend_idx(&snapshot)?;
        if self.terminal_snapshot_backend_idx(&conflict)? != backend_idx {
            return Err(contract_violation(
                "injected query terminal conflict changed the participant identity",
            ));
        }
        if snapshot.digest() == conflict.digest() {
            return Err(contract_violation(
                "injected query terminal conflict did not change the snapshot digest",
            ));
        }
        let mut terminal = self.terminal.0.lock().expect("query terminal store");
        if terminal.snapshots.contains_key(&backend_idx) {
            return Err(contract_violation(
                "injected query terminal conflict requires an empty participant slot",
            ));
        }
        // Store the primary immutable value and the conflicting value while
        // holding the same lock.  No finalizer can observe an apparently
        // complete terminal set between the two admissions.
        terminal.snapshots.insert(backend_idx, snapshot);
        let reason =
            "query terminal snapshot conflicts with an already stored participant snapshot";
        terminal.reader_failure = Some(reason.to_string());
        drop(terminal);
        self.terminal.1.notify_all();
        self.metrics
            .terminal_snapshot_stored(TerminalSnapshotStoreOutcome::Accepted);
        self.metrics.terminal_snapshot_conflict();
        Err(contract_violation(reason))
    }

    pub(crate) fn terminal_set(&self) -> Result<QueryTerminalSet, DistributedQueryError> {
        let expected = self
            .attempted
            .lock()
            .expect("attempted participant set")
            .len();
        let terminal = self.terminal.0.lock().expect("query terminal store");
        if terminal.snapshots.len() != expected {
            return Err(failed(format!(
                "query lifecycle terminal snapshots are incomplete: received {}, expected {expected}",
                terminal.snapshots.len()
            )));
        }
        QueryTerminalSet::new(terminal.snapshots.values().cloned().collect())
            .map_err(|error| failed(error.to_string()))
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
        // abort acknowledgement.  Preserve a set already delivered on the
        // stream without ever delaying or replacing the original failure.
        QueryLifecycleAbortOutcome::new(primary_error, self.terminal_set().ok())
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
            .attempted
            .lock()
            .expect("attempted participant set")
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
                backend_id = failure.target.backend_idx(),
                backend_start_epoch = failure.target.start_epoch(),
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
                    .send(QueryControlCommand::Abort {
                        reason: reason.to_string(),
                    })
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
                            backend_id = participant.target.backend_idx(),
                            backend_start_epoch = participant.target.start_epoch(),
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

        let request =
            QueryAbortRequest::new(self.execution_id, participant.digest, reason.to_string())
                .map_err(|error| {
                    AbortCleanupFailure::new(
                        participant,
                        QueryLifecycleTransportErrorKind::InvalidResponse,
                        error.to_string(),
                    )
                })?;
        let ack = self
            .transport
            .abort_query(participant.target, request, self.config.attach_timeout())
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
        if ack.execution_id() != self.execution_id {
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
            backend_id = participant.target.backend_idx(),
            backend_start_epoch = participant.target.start_epoch(),
            participant_digest = %hex::encode(participant.digest.as_bytes()),
            accepted_reason = ?ack.accepted_reason(),
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
            if self.terminal_delivery_started(terminal) {
                // A Finalize command can turn a locally drained backend into
                // TerminalRetained before its in-flight heartbeat is handled.
                // Terminal delivery still needs the stream reader, but no
                // longer needs liveness heartbeats once every participant has
                // drained.
                return Some(Ok(()));
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
        let expected = self
            .attempted
            .lock()
            .expect("attempted participant set")
            .len();
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

    fn terminal_delivery_started(&self, terminal: &TerminalState) -> bool {
        self.state.load(Ordering::Acquire) == FINALIZING
            && terminal.locally_drained.len()
                == self
                    .attempted
                    .lock()
                    .expect("attempted participant set")
                    .len()
    }

    fn release_session(&self, backend_idx: usize) {
        self.sessions
            .lock()
            .expect("active query control sessions")
            .remove(&backend_idx);
    }

    fn wait_for_all_snapshots(&self, timeout: Duration) -> Result<QueryTerminalSet, String> {
        let expected = self
            .attempted
            .lock()
            .expect("attempted participant set")
            .len();
        self.wait_terminal_event(timeout, |terminal| {
            if let Some(error) = &terminal.reader_failure {
                return Some(Err(error.clone()));
            }
            if terminal.snapshots.len() != expected {
                return None;
            }
            Some(
                QueryTerminalSet::new(terminal.snapshots.values().cloned().collect())
                    .map_err(|error| error.to_string()),
            )
        })
        .ok_or_else(|| "query lifecycle timed out waiting for all terminal snapshots".to_string())?
    }

    fn wait_terminal_event<T>(
        &self,
        timeout: Duration,
        condition: impl Fn(&TerminalState) -> Option<Result<T, String>>,
    ) -> Option<Result<T, String>> {
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

    fn supervisor_failed(&self, reason: String, kind: SupervisorFailureKind) {
        match kind {
            SupervisorFailureKind::HeartbeatTimeout => self.metrics.heartbeat_timeout(),
            SupervisorFailureKind::CoordinatorLost => self.metrics.coordinator_lost(),
            SupervisorFailureKind::LocalFailure => self.metrics.local_failure(),
        }
        if let Some(registry) = self.registry.upgrade() {
            let query_id = self.execution_id.query_id();
            // A LocalFailure is delivered by the same control-stream reader
            // that must receive TerminationAccepted. Dispatch cancellation on
            // a separate thread so abort acknowledgement cannot deadlock
            // behind its own event handler.
            std::thread::spawn(move || {
                let _ = registry.latch_failure_and_cancel(query_id, reason);
            });
        } else {
            let _ = self.abort_preserving(reason);
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
        if let Err(error) = self.wait_for_all_drained(self.config.terminal_drain_timeout()) {
            self.metrics.terminal_finalize_failure();
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
                            .send(QueryControlCommand::Finalize)
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
            let terminal_set = match self
                .wait_for_all_snapshots(self.config.terminal_snapshot_timeout())
            {
                Ok(terminal_set) => terminal_set,
                Err(error) => {
                    self.metrics.terminal_finalize_failure();
                    self.state.store(ABORTED, Ordering::Release);
                    let primary = format!("query lifecycle terminal finalization failed: {error}");
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
                    return Err(failed(message));
                }
            };
            self.state.store(FINALIZED, Ordering::Release);
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
            Err(failed(message))
        }
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
                // A Finalize command has already fenced normal execution. A
                // stream can disappear before the snapshot frame; retain the
                // participant slot and let its unary fallback complete it.
                tracing::info!(
                    backend_idx = session.target.backend_idx(),
                    error = %error,
                    "query lifecycle terminal stream closed; waiting for unary snapshot fallback"
                );
                return;
            }
            Err(error) => {
                control.record_reader_failure(format!(
                    "query lifecycle control stream lost on backend {} digest {}: {error}",
                    session.target.backend_idx(),
                    hex::encode(session.digest.as_bytes())
                ));
                return;
            }
        };
        if let QueryControlEvent::LocalFailure { code, detail } = event {
            control.supervisor_failed(
                format!(
                    "query lifecycle local failure on backend {} ({code}): {detail}",
                    session.target.backend_idx()
                ),
                SupervisorFailureKind::LocalFailure,
            );
            // A running failure fences the attempt but does not end terminal
            // delivery: the BE still drains facts and may send a failed
            // immutable snapshot (or its unary retry) before this reader
            // leaves the stream.
            continue;
        }
        let terminal_snapshot = matches!(&event, QueryControlEvent::TerminalSnapshot { .. });
        if let Err(error) = control.handle_control_event(&session, event) {
            control.record_reader_failure(error);
            return;
        }
        if terminal_snapshot {
            // Store-before-ACK completed this participant's terminal handoff.
            // Retained terminal ingress only needs the immutable participant
            // identity and stored snapshot for unary fallback. Release the
            // live session so successful queries do not retain HTTP/2 control
            // streams for the full ingress TTL.
            control.release_session(session.target.backend_idx());
            return;
        }
    }
}

impl AttemptControl {
    fn handle_control_event(
        &self,
        session: &ActiveSession,
        event: QueryControlEvent,
    ) -> Result<(), String> {
        match event {
            QueryControlEvent::HeartbeatAck { sequence } => {
                let mut terminal = self.terminal.0.lock().expect("query terminal state");
                let prior = terminal
                    .heartbeat_acks
                    .entry(session.target.backend_idx())
                    .or_insert(0);
                *prior = (*prior).max(sequence);
                self.terminal.1.notify_all();
                Ok(())
            }
            QueryControlEvent::LocalDrained => {
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
            QueryControlEvent::TerminationAccepted { reason } => {
                self.terminal
                    .0
                    .lock()
                    .expect("query terminal state")
                    .termination_accepted
                    .insert(session.target.backend_idx(), reason);
                self.terminal.1.notify_all();
                Ok(())
            }
            QueryControlEvent::FragmentObservation { observation } => {
                let _ = self.store_fragment_observation(session, observation);
                Ok(())
            }
            QueryControlEvent::TerminalSnapshot { snapshot } => {
                if let Some(scope) = claim_terminal_snapshot_conflict(session, &snapshot)? {
                    let conflict = conflicting_terminal_snapshot(&snapshot)?;
                    eprintln!(
                        "NOVAROCKS_QUERY_TERMINAL_SNAPSHOT_CONFLICT_INJECTED execution_id={}:{}:{} backend_index={} backend_id={} start_epoch={} token={}",
                        snapshot.execution_id().query_id().high(),
                        snapshot.execution_id().query_id().low(),
                        snapshot.execution_id().attempt_id().get(),
                        scope.backend_index,
                        scope.backend_id,
                        scope.start_epoch,
                        scope.token,
                    );
                    return self
                        .store_terminal_snapshot_conflict(snapshot, conflict)
                        .map_err(|error| error.to_string());
                }
                self.store_terminal_snapshot(snapshot.clone())
                    .map_err(|error| error.to_string())?;
                if claim_terminal_ack_drop(session, &snapshot)? {
                    return Ok(());
                }
                session
                    .session
                    .send(QueryControlCommand::TerminalAck {
                        ack: novarocks::query_execution::lifecycle::QueryTerminalAck::from_snapshot(
                            &snapshot,
                        ),
                    })
                    .map_err(|error| {
                        format!(
                            "query lifecycle terminal ACK failed for backend {}: {error}",
                            session.target.backend_idx()
                        )
                    })
            }
            QueryControlEvent::LocalFailure { code, detail } => Err(format!(
                "query lifecycle local failure on backend {} ({code}): {detail}",
                session.target.backend_idx()
            )),
            QueryControlEvent::ControlReady => Err(format!(
                "backend {} emitted duplicate ControlReady after attachment",
                session.target.backend_idx()
            )),
        }
    }

    fn record_reader_failure(&self, reason: String) {
        let mut terminal = self.terminal.0.lock().expect("query terminal state");
        if terminal.reader_failure.is_none() {
            terminal.reader_failure = Some(reason);
        }
        self.terminal.1.notify_all();
    }

    fn stop_readers(&self) {
        self.terminal
            .0
            .lock()
            .expect("query terminal state")
            .stop_readers = true;
        self.terminal.1.notify_all();
    }

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
    snapshot: &QueryTerminalSnapshot,
) -> Result<bool, String> {
    use novarocks::common::query_lifecycle_fault::{QueryLifecycleFaultKind, claim_matching_fault};

    let Some(root) = novarocks::common::app_config::config()
        .ok()
        .and_then(|config| config.debug.query_lifecycle_fault_dir())
    else {
        return Ok(false);
    };
    let backend_index = session.target.backend_idx();
    let backend_id = u64::try_from(backend_index)
        .map_err(|_| "backend index does not fit terminal ACK fault identity".to_string())?;
    let Some(scope) = claim_matching_fault(
        root,
        QueryLifecycleFaultKind::TerminalAckDrop,
        snapshot.execution_id(),
        backend_index,
        backend_id,
        session.target.start_epoch(),
    )
    .map_err(|error| format!("claim terminal ACK drop fault: {error}"))?
    else {
        return Ok(false);
    };
    eprintln!(
        "NOVAROCKS_QUERY_TERMINAL_ACK_DROPPED execution_id={}:{}:{} backend_index={} backend_id={} start_epoch={} token={}",
        snapshot.execution_id().query_id().high(),
        snapshot.execution_id().query_id().low(),
        snapshot.execution_id().attempt_id().get(),
        backend_index,
        backend_id,
        session.target.start_epoch(),
        scope.token,
    );
    Ok(true)
}

#[cfg(debug_assertions)]
fn claim_terminal_snapshot_conflict(
    session: &ActiveSession,
    snapshot: &QueryTerminalSnapshot,
) -> Result<Option<novarocks::common::query_lifecycle_fault::QueryLifecycleFaultScope>, String> {
    use novarocks::common::query_lifecycle_fault::{QueryLifecycleFaultKind, claim_matching_fault};

    let Some(root) = novarocks::common::app_config::config()
        .ok()
        .and_then(|config| config.debug.query_lifecycle_fault_dir())
    else {
        return Ok(None);
    };
    let backend_index = session.target.backend_idx();
    let backend_id = u64::try_from(backend_index)
        .map_err(|_| "backend index does not fit terminal conflict fault identity".to_string())?;
    claim_matching_fault(
        root,
        QueryLifecycleFaultKind::TerminalSnapshotConflict,
        snapshot.execution_id(),
        backend_index,
        backend_id,
        session.target.start_epoch(),
    )
    .map_err(|error| format!("claim terminal snapshot conflict fault: {error}"))
}

#[cfg(not(debug_assertions))]
fn claim_terminal_snapshot_conflict(
    _session: &ActiveSession,
    _snapshot: &QueryTerminalSnapshot,
) -> Result<Option<novarocks::common::query_lifecycle_fault::QueryLifecycleFaultScope>, String> {
    Ok(None)
}

#[cfg(debug_assertions)]
fn conflicting_terminal_snapshot(
    snapshot: &QueryTerminalSnapshot,
) -> Result<QueryTerminalSnapshot, String> {
    let mut fragments = snapshot.fragments().to_vec();
    let first = fragments.first().cloned().ok_or_else(|| {
        "terminal snapshot conflict fault requires a fragment participant".to_string()
    })?;
    fragments[0] = FragmentTerminalSnapshot::new(
        first.fragment_instance_id(),
        first.backend_num(),
        FragmentTerminalOutcome::Failed {
            code: "RUNNER_INJECTED_TERMINAL_CONFLICT".to_string(),
            detail: "same participant produced a conflicting terminal snapshot".to_string(),
        },
        first.sink().clone(),
        first.profile().cloned(),
    )
    .map_err(|error| error.to_string())?;
    QueryTerminalSnapshot::new(
        snapshot.execution_id(),
        snapshot.backend().clone(),
        snapshot.init_digest(),
        fragments,
    )
    .map_err(|error| error.to_string())
}

#[cfg(not(debug_assertions))]
fn conflicting_terminal_snapshot(
    _snapshot: &QueryTerminalSnapshot,
) -> Result<QueryTerminalSnapshot, String> {
    Err("terminal snapshot conflict injection is disabled in release builds".to_string())
}

#[cfg(not(debug_assertions))]
fn claim_terminal_ack_drop(
    _session: &ActiveSession,
    _snapshot: &QueryTerminalSnapshot,
) -> Result<bool, String> {
    Ok(false)
}

impl ActiveQueryAttemptControl for AttemptControl {
    fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    fn request_abort(&self, reason: String) {
        let enriched = self.abort_preserving(reason);
        if let Some(registry) = self.registry.upgrade() {
            let _ = registry.preserve_failure_context(self.execution_id.query_id(), enriched);
        }
    }

    fn report_terminal_snapshot(
        &self,
        snapshot: QueryTerminalSnapshot,
    ) -> Result<bool, DistributedQueryError> {
        self.store_terminal_snapshot(snapshot)
            .map(|outcome| outcome == TerminalSnapshotStoreOutcome::Accepted)
    }

    fn retain_terminal_ingress(&self) -> bool {
        self.retain_terminal_ingress.load(Ordering::Acquire) || self.terminal_set().is_ok()
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

fn heartbeat_supervisor(control: Weak<AttemptControl>) {
    let Some(control) = control.upgrade() else {
        return;
    };
    let started = Instant::now();
    let mut sequence = 0u64;
    while control.wait_heartbeat_interval() {
        {
            let terminal = control.terminal.0.lock().expect("query terminal state");
            if control.terminal_delivery_started(&terminal) {
                return;
            }
        }
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
            if let Err(error) = session.session.send(QueryControlCommand::Heartbeat {
                sequence,
                sent_mono_ns: started.elapsed().as_nanos() as u64,
            }) {
                let terminal = control.terminal.0.lock().expect("query terminal state");
                if control.terminal_delivery_started(&terminal) {
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
                    let timeout = error.contains("heartbeat timeout");
                    let failure = if timeout {
                        format!(
                            "query lifecycle heartbeat timeout on backend {} digest {}",
                            session.target.backend_idx(),
                            hex::encode(session.digest.as_bytes())
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
    _registry_binding: ActiveQueryAttemptBinding,
}

impl FrontendQueryLifecycleLeaseGuard {
    pub fn lease(
        control: Arc<AttemptControl>,
        supervisor: JoinHandle<()>,
        registry_binding: ActiveQueryAttemptBinding,
    ) -> QueryLifecycleLease {
        QueryLifecycleLease::new(Box::new(Self {
            control,
            supervisor: Some(supervisor),
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
    }
}

impl QueryLifecycleLeaseGuard for FrontendQueryLifecycleLeaseGuard {
    fn finalize(mut self: Box<Self>) -> Result<QueryTerminalSet, DistributedQueryError> {
        let result = self.control.finalize();
        self.stop_and_join();
        result
    }

    fn abort_preserving(mut self: Box<Self>, primary_error: String) -> QueryLifecycleAbortOutcome {
        self.stop_and_join();
        self.control.abort_with_terminal_outcome(primary_error)
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
