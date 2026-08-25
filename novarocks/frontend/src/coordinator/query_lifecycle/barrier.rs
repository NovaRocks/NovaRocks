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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::common::query_cancellation::QueryCancellationView;
use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use crate::query_execution::launch::{QueryLaunchBarrier, StageBatch};
use crate::query_execution::lifecycle_plan::{
    QueryInitBarrier, QueryInitPlan, QueryLifecycleLease,
};
use novarocks_proto::lifecycle::{
    AttemptId as CoreAttemptId, AttemptId as ProtocolAttemptId, QueryControlAttach,
    QueryExecutionId, QueryInitOutcome, QueryStageAck, QueryStageRequest, QueryStartAck,
    QueryStartRequest,
};
use novarocks_proto::novarocks as protocol_wire;

use super::QueryLifecycleTransport;
use super::lease::{
    ActiveSession, AttemptControl, FrontendLifecycleMetrics, FrontendQueryLifecycleLeaseGuard,
    spawn_supervisor,
};
use super::manifest::{MaterializedParticipant, materialize};
use crate::coordinator::query_registry::{
    ActiveQueryAttemptBinding, ActiveQueryAttemptControl, FrontendQueryRegistry,
};

#[derive(Clone, Copy)]
pub(crate) struct FrontendQueryLifecycleConfig {
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
    init_rpc_timeout: Duration,
    attach_timeout: Duration,
    stage_rpc_timeout: Duration,
    start_rpc_timeout: Duration,
    terminal_drain_timeout: Duration,
    terminal_ack_timeout: Duration,
}

impl FrontendQueryLifecycleConfig {
    pub(crate) fn new(
        heartbeat_interval: Duration,
        heartbeat_timeout: Duration,
        init_rpc_timeout: Duration,
        attach_timeout: Duration,
    ) -> Result<Self, DistributedQueryError> {
        if heartbeat_interval.is_zero()
            || heartbeat_timeout.is_zero()
            || init_rpc_timeout.is_zero()
            || attach_timeout.is_zero()
        {
            return Err(contract_error(
                "frontend query lifecycle timeouts must be nonzero",
            ));
        }
        let minimum_heartbeat_timeout = heartbeat_interval.checked_mul(3).ok_or_else(|| {
            contract_error("frontend query lifecycle heartbeat interval is too large to validate")
        })?;
        if heartbeat_timeout < minimum_heartbeat_timeout {
            return Err(contract_error(
                "frontend query lifecycle heartbeat timeout must be at least 3 times its interval",
            ));
        }
        Ok(Self {
            heartbeat_interval,
            heartbeat_timeout,
            init_rpc_timeout,
            attach_timeout,
            stage_rpc_timeout: Duration::from_secs(5),
            start_rpc_timeout: Duration::from_secs(2),
            terminal_drain_timeout: attach_timeout,
            terminal_ack_timeout: attach_timeout,
        })
    }

    pub(crate) fn with_stage_start_timeouts(
        mut self,
        stage_rpc_timeout: Duration,
        start_rpc_timeout: Duration,
    ) -> Result<Self, DistributedQueryError> {
        if stage_rpc_timeout.is_zero() || start_rpc_timeout.is_zero() {
            return Err(contract_error(
                "frontend Stage/Start RPC timeouts must be nonzero",
            ));
        }
        self.stage_rpc_timeout = stage_rpc_timeout;
        self.start_rpc_timeout = start_rpc_timeout;
        Ok(self)
    }

    pub(crate) fn with_terminal_timeouts(
        mut self,
        terminal_drain_timeout: Duration,
        terminal_ack_timeout: Duration,
    ) -> Result<Self, DistributedQueryError> {
        if terminal_drain_timeout.is_zero() || terminal_ack_timeout.is_zero() {
            return Err(contract_error(
                "frontend terminal drain and ACK timeouts must be nonzero",
            ));
        }
        self.terminal_drain_timeout = terminal_drain_timeout;
        self.terminal_ack_timeout = terminal_ack_timeout;
        Ok(self)
    }

    pub(super) const fn heartbeat_interval(self) -> Duration {
        self.heartbeat_interval
    }

    pub(super) const fn heartbeat_timeout(self) -> Duration {
        self.heartbeat_timeout
    }

    pub(super) const fn init_rpc_timeout(self) -> Duration {
        self.init_rpc_timeout
    }

    pub(super) const fn attach_timeout(self) -> Duration {
        self.attach_timeout
    }

    pub(super) const fn stage_rpc_timeout(self) -> Duration {
        self.stage_rpc_timeout
    }

    pub(super) const fn start_rpc_timeout(self) -> Duration {
        self.start_rpc_timeout
    }

    pub(super) const fn terminal_drain_timeout(self) -> Duration {
        self.terminal_drain_timeout
    }

    #[allow(
        dead_code,
        reason = "Retained for terminal ACK timeout policy consumers compiled in target-gated lifecycle paths."
    )]
    pub(super) const fn terminal_ack_timeout(self) -> Duration {
        self.terminal_ack_timeout
    }

    /// A stream-loss fallback starts only after the normal ACK window. The
    /// first unary RPC is allowed the same bounded window, so Finalize must
    /// not fail exactly when the fallback becomes eligible.
    pub(super) fn terminal_snapshot_timeout(self) -> Duration {
        self.terminal_ack_timeout
            .saturating_add(self.terminal_ack_timeout)
    }
}

pub(crate) struct FrontendQueryLifecycleBarrier {
    transport: Arc<dyn QueryLifecycleTransport>,
    registry: Arc<FrontendQueryRegistry>,
    config: FrontendQueryLifecycleConfig,
    metrics: Arc<FrontendLifecycleMetrics>,
    cancellation: Option<QueryCancellationView>,
}

pub(super) struct PreReadyAttemptGuard {
    control: Arc<AttemptControl>,
    registry_binding: Option<ActiveQueryAttemptBinding>,
    supervisor: Option<std::thread::JoinHandle<()>>,
    armed: bool,
}

impl PreReadyAttemptGuard {
    pub(super) fn new(
        control: Arc<AttemptControl>,
        registry_binding: ActiveQueryAttemptBinding,
    ) -> Self {
        Self {
            control,
            registry_binding: Some(registry_binding),
            supervisor: None,
            armed: true,
        }
    }

    fn start_supervisor(&mut self) {
        self.supervisor = Some(spawn_supervisor(&self.control));
    }

    fn into_lease(mut self) -> QueryLifecycleLease {
        let registry_binding = self
            .registry_binding
            .take()
            .expect("pre-ready lifecycle guard registry binding");
        let supervisor = self
            .supervisor
            .take()
            .expect("pre-ready lifecycle supervisor");
        let lease = FrontendQueryLifecycleLeaseGuard::lease(
            Arc::clone(&self.control),
            supervisor,
            registry_binding,
        );
        self.armed = false;
        lease
    }
}

impl Drop for PreReadyAttemptGuard {
    fn drop(&mut self) {
        if self.armed && self.control.is_active() {
            let error = self.control.abort_before_ready(
                "query lifecycle initialization interrupted before all-ready".to_string(),
            );
            tracing::error!(
                query_id_high = self.control.execution_id().query_id().high(),
                query_id_low = self.control.execution_id().query_id().low(),
                attempt_id = self.control.execution_id().attempt_id().get(),
                reason = %error,
                "frontend query lifecycle pre-ready guard cleaned up interrupted attempt"
            );
        }
        if let Some(supervisor) = self.supervisor.take() {
            let _ = supervisor.join();
        }
    }
}

impl FrontendQueryLifecycleBarrier {
    pub(crate) fn new(
        transport: Arc<dyn QueryLifecycleTransport>,
        registry: Arc<FrontendQueryRegistry>,
        config: FrontendQueryLifecycleConfig,
    ) -> Self {
        Self {
            transport,
            registry,
            config,
            #[cfg(test)]
            metrics: Arc::new(FrontendLifecycleMetrics::default()),
            #[cfg(not(test))]
            metrics: FrontendLifecycleMetrics::process_shared(),
            cancellation: None,
        }
    }

    pub(crate) fn with_cancellation(mut self, cancellation: QueryCancellationView) -> Self {
        self.cancellation = Some(cancellation);
        self
    }

    #[cfg(test)]
    pub(super) fn metrics_snapshot(&self) -> crate::metrics::FrontendQueryLifecycleMetricsSnapshot {
        self.metrics.snapshot()
    }

    fn cancellation_message(&self) -> Option<String> {
        self.cancellation
            .as_ref()
            .and_then(QueryCancellationView::reason)
            .map(|reason| format!("query lifecycle request cancelled: {reason:?}"))
    }
}

impl QueryInitBarrier for FrontendQueryLifecycleBarrier {
    fn initialize_all(
        &self,
        plan: QueryInitPlan,
    ) -> Result<QueryLifecycleLease, DistributedQueryError> {
        let materialized = materialize(plan)?;
        let execution_id = materialized.execution_id;
        let fragment_participants = materialized
            .participants
            .iter()
            .filter(|participant| participant.fragment_participant)
            .count();
        tracing::info!(
            query_id_high = execution_id.query_id().high(),
            query_id_low = execution_id.query_id().low(),
            attempt_id = execution_id.attempt_id().get(),
            participants = materialized.participants.len(),
            fragment_participants,
            service_only_participants = materialized.participants.len() - fragment_participants,
            "frontend query lifecycle attempt created"
        );

        let wire_execution_id = protocol_execution_id(execution_id).map_err(contract_error)?;
        let control = AttemptControl::new(
            wire_execution_id,
            Arc::clone(&self.transport),
            Arc::downgrade(&self.registry),
            self.config,
            Arc::clone(&self.metrics),
        );
        let ownership = materialized
            .participants
            .iter()
            .map(|participant| {
                (
                    participant.target.backend_idx(),
                    participant.target.start_epoch(),
                )
            })
            .collect::<Vec<_>>();
        control.set_planned(&materialized.participants);
        control.set_init_attempted(&materialized.participants);
        if let Err(error) = self
            .registry
            .extend_attempt_backend_ownership(execution_id.query_id(), &ownership)
        {
            if error.is_backend_epoch_mismatch() {
                self.metrics.backend_epoch_mismatch();
            }
            let error = error.into_error();
            let message = control.abort_before_ready(error.message().to_string());
            return Err(DistributedQueryError::new(error.kind(), message));
        }
        let active_control: Arc<dyn ActiveQueryAttemptControl> = control.clone();
        let registry_binding = match self
            .registry
            .bind_active_attempt(wire_execution_id, active_control)
        {
            Ok(binding) => binding,
            Err(error) => {
                let message = control.abort_before_ready(error.message().to_string());
                return Err(DistributedQueryError::new(error.kind(), message));
            }
        };
        let mut pre_ready_guard = PreReadyAttemptGuard::new(Arc::clone(&control), registry_binding);
        if let Some(reason) = self.cancellation_message() {
            return Err(failed(control.abort_before_ready(reason)));
        }
        if !control.is_active() {
            return Err(failed(
                "query lifecycle attempt was cancelled before InitQuery",
            ));
        }

        let init_errors = init_all(
            self.transport.as_ref(),
            &materialized.participants,
            self.config,
            self.metrics.as_ref(),
        );
        if let Some(primary) = init_errors.into_iter().next() {
            let message = control.abort_before_ready(primary);
            return Err(failed(message));
        }
        if let Some(reason) = self.cancellation_message() {
            return Err(failed(control.abort_before_ready(reason)));
        }
        if !control.is_active() {
            return Err(failed(control.abort_before_ready(
                "query lifecycle attempt was cancelled during InitQuery".to_string(),
            )));
        }

        pre_ready_guard.start_supervisor();
        let attach_errors = attach_all(
            self.transport.as_ref(),
            &materialized.participants,
            execution_id.attempt_id().get(),
            self.config,
            self.metrics.as_ref(),
            &control,
        );
        if let Some(primary) = attach_errors.into_iter().next() {
            let message = control.abort_before_ready(primary);
            return Err(failed(message));
        }
        if let Err(error) = control.freeze_admitted() {
            let message = control.abort_before_ready(error.message().to_string());
            return Err(DistributedQueryError::new(error.kind(), message));
        }
        if let Some(reason) = self.cancellation_message() {
            return Err(failed(control.abort_before_ready(reason)));
        }
        Ok(pre_ready_guard.into_lease())
    }
}

impl QueryLaunchBarrier for FrontendQueryLifecycleBarrier {
    fn stage_all(&self, batches: &[StageBatch]) -> Result<(), DistributedQueryError> {
        if let Some(reason) = self.cancellation_message() {
            return Err(failed(reason));
        }
        record_lifecycle_phase_marker("staging", batches)?;
        let mut failures = std::thread::scope(|scope| {
            let handles = batches
                .iter()
                .map(|batch| {
                    scope.spawn(move || stage_one(self.transport.as_ref(), batch, self.config))
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .filter_map(|handle| match handle.join() {
                    Ok(Ok(())) => None,
                    Ok(Err((backend, error))) => Some((backend, error)),
                    Err(_) => Some((usize::MAX, "StageFragments worker panicked".to_string())),
                })
                .collect::<Vec<_>>()
        });
        failures.sort_by_key(|(backend, _)| *backend);
        let outcome = failures
            .into_iter()
            .next()
            .map_or(Ok(()), |(_, error)| Err(failed(error)));
        if outcome.is_ok() {
            record_lifecycle_phase_marker("staged", batches)?;
            record_stage_barrier_marker(batches)?;
        }
        outcome
    }

    fn start_all(&self, batches: &[StageBatch]) -> Result<(), DistributedQueryError> {
        if let Some(reason) = self.cancellation_message() {
            return Err(failed(reason));
        }
        record_lifecycle_phase_marker("starting", batches)?;
        let mut failures = std::thread::scope(|scope| {
            let handles = batches
                .iter()
                .map(|batch| {
                    scope.spawn(move || start_one(self.transport.as_ref(), batch, self.config))
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .filter_map(|handle| match handle.join() {
                    Ok(Ok(())) => None,
                    Ok(Err((backend, error))) => Some((backend, error)),
                    Err(_) => Some((usize::MAX, "StartPreparedQuery worker panicked".to_string())),
                })
                .collect::<Vec<_>>()
        });
        failures.sort_by_key(|(backend, _)| *backend);
        let outcome = failures
            .into_iter()
            .next()
            .map_or(Ok(()), |(_, error)| Err(failed(error)));
        if outcome.is_ok() {
            record_lifecycle_phase_marker("running", batches)?;
        }
        outcome
    }
}

fn stage_one(
    transport: &dyn QueryLifecycleTransport,
    batch: &StageBatch,
    config: FrontendQueryLifecycleConfig,
) -> Result<(), (usize, String)> {
    let target = batch.binding().target();
    let request = batch.request();
    let first = transport.stage_fragments(target, request, config.stage_rpc_timeout());
    let ack = match first {
        Ok(ack) => ack,
        Err(error) if error.is_unknown_stage_or_start_outcome() => transport
            .stage_fragments(target, request, config.stage_rpc_timeout())
            .map_err(|retry| {
                (
                    target.backend_idx(),
                    format!(
                "backend {} StageFragments retry failed after unknown outcome ({error}): {retry}",
                target.backend_idx()
            ),
                )
            })?,
        Err(error) => {
            return Err((
                target.backend_idx(),
                format!(
                    "backend {} StageFragments failed: {error}",
                    target.backend_idx()
                ),
            ))
        }
    };
    validate_stage_ack(target.backend_idx(), request, &ack)
}

fn start_one(
    transport: &dyn QueryLifecycleTransport,
    batch: &StageBatch,
    config: FrontendQueryLifecycleConfig,
) -> Result<(), (usize, String)> {
    let target = batch.binding().target();
    let request = batch.start_request();
    let first = transport.start_prepared_query(target, &request, config.start_rpc_timeout());
    let ack = match first {
        Ok(ack) => ack,
        Err(error) if error.is_unknown_stage_or_start_outcome() => transport
            .start_prepared_query(target, &request, config.start_rpc_timeout())
            .map_err(|retry| (target.backend_idx(), format!(
                "backend {} StartPreparedQuery retry failed after unknown outcome ({error}): {retry}",
                target.backend_idx()
            )))?,
        Err(error) => return Err((target.backend_idx(), format!(
            "backend {} StartPreparedQuery failed: {error}", target.backend_idx()
        ))),
    };
    validate_start_ack(target.backend_idx(), &request, &ack)
}

fn validate_stage_ack(
    backend_idx: usize,
    request: &QueryStageRequest,
    ack: &QueryStageAck,
) -> Result<(), (usize, String)> {
    if ack.execution_id() != request.execution_id()
        || ack.digest_version() != request.digest_version()
        || ack.digest() != request.digest()
    {
        return Err((
            backend_idx,
            format!("backend {backend_idx} StageFragments ACK echo mismatch"),
        ));
    }
    if !ack.outcome().is_staged() {
        return Err((
            backend_idx,
            format!(
                "backend {backend_idx} StageFragments rejected with {:?}: {}",
                ack.outcome(),
                ack.detail()
            ),
        ));
    }
    Ok(())
}

fn validate_start_ack(
    backend_idx: usize,
    request: &QueryStartRequest,
    ack: &QueryStartAck,
) -> Result<(), (usize, String)> {
    if ack.execution_id() != request.execution_id()
        || ack.digest_version() != request.digest_version()
        || ack.digest() != request.digest()
    {
        return Err((
            backend_idx,
            format!("backend {backend_idx} StartPreparedQuery ACK echo mismatch"),
        ));
    }
    if !ack.outcome().is_running() {
        return Err((
            backend_idx,
            format!(
                "backend {backend_idx} StartPreparedQuery rejected with {:?}: {}",
                ack.outcome(),
                ack.detail()
            ),
        ));
    }
    Ok(())
}

fn protocol_execution_id(
    execution_id: QueryExecutionId,
) -> Result<novarocks_proto::lifecycle::QueryExecutionId, String> {
    let attempt = ProtocolAttemptId::new(execution_id.attempt_id().get())
        .map_err(|error| error.to_string())?;
    novarocks_proto::lifecycle::QueryExecutionId::new(execution_id.query_id(), attempt)
        .map_err(|error| error.to_string())
}

fn participant_execution_id(
    participant: &MaterializedParticipant,
) -> novarocks_proto::lifecycle::QueryExecutionId {
    participant
        .request
        .manifest()
        .and_then(|manifest| manifest.execution_id())
        .expect("materialized Protocol init request retains its validated execution id")
}

#[cfg(debug_assertions)]
fn record_lifecycle_phase_marker(
    phase: &str,
    batches: &[StageBatch],
) -> Result<(), DistributedQueryError> {
    let Some(execution_id) = batches.first().map(|batch| batch.request().execution_id()) else {
        return Ok(());
    };
    let attempt = CoreAttemptId::new(execution_id.attempt_id().get())
        .map_err(|error| contract_error(error.to_string()))?;
    let core_execution_id = QueryExecutionId::new(execution_id.query_id(), attempt)
        .map_err(|error| contract_error(error.to_string()))?;
    record_lifecycle_phase_marker_for_execution(phase, core_execution_id)
}

/// Runner-only lifecycle barrier.  The terminal snapshot reader uses this
/// after it has durably stored a participant record but before it sends the
/// ACK, which lets cross-process tests prove retained-record cleanup without
/// changing production timing or adding a direct all-in-one path.
#[cfg(debug_assertions)]
pub(super) fn record_lifecycle_phase_marker_for_execution(
    phase: &str,
    execution_id: QueryExecutionId,
) -> Result<(), DistributedQueryError> {
    let Some(root) = novarocks_failpoint::configured_root() else {
        return Ok(());
    };
    for (kind, action) in [("kill-query", "kill_query"), ("fe-crash", "kill_fe")] {
        let path = root.join(format!("{kind}-at-{phase}.trigger"));
        let contents = match std::fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(contract_error(format!(
                    "read runner-owned lifecycle phase trigger {}: {error}",
                    path.display()
                )));
            }
        };
        let fields = contents
            .lines()
            .filter_map(|line| line.split_once('='))
            .collect::<BTreeMap<_, _>>();
        let token = fields
            .get("token")
            .copied()
            .filter(|token| !token.is_empty())
            .ok_or_else(|| contract_error("runner-owned lifecycle phase trigger has no token"))?;
        if fields.get("phase").copied() != Some(phase) || fields.len() != 2 {
            return Err(contract_error(format!(
                "runner-owned lifecycle phase trigger {} has invalid contents",
                path.display()
            )));
        }
        eprintln!(
            "NOVAROCKS_QUERY_LIFECYCLE_PHASE execution_id={}:{}:{} phase={} action={} token={}",
            execution_id.query_id().high(),
            execution_id.query_id().low(),
            execution_id.attempt_id().get(),
            phase,
            action,
            token
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        while path.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        if path.exists() {
            return Err(failed(format!(
                "timed out waiting for runner to execute {action} at lifecycle phase {phase}"
            )));
        }
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
pub(super) fn record_lifecycle_phase_marker_for_execution(
    _phase: &str,
    _execution_id: QueryExecutionId,
) -> Result<(), DistributedQueryError> {
    Ok(())
}

#[cfg(debug_assertions)]
fn record_stage_barrier_marker(batches: &[StageBatch]) -> Result<(), DistributedQueryError> {
    let Some(execution_id) = batches.first().map(|batch| batch.request().execution_id()) else {
        return Ok(());
    };
    if novarocks_failpoint::configured_root().is_some() {
        eprintln!(
            "NOVAROCKS_QUERY_STAGE_BARRIER execution_id={}:{}:{} participants={}",
            execution_id.query_id().high(),
            execution_id.query_id().low(),
            execution_id.attempt_id().get(),
            batches.len()
        );
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
fn record_stage_barrier_marker(_batches: &[StageBatch]) -> Result<(), DistributedQueryError> {
    Ok(())
}

#[cfg(not(debug_assertions))]
fn record_lifecycle_phase_marker(
    _phase: &str,
    _batches: &[StageBatch],
) -> Result<(), DistributedQueryError> {
    Ok(())
}

fn init_all(
    transport: &dyn QueryLifecycleTransport,
    participants: &[MaterializedParticipant],
    config: FrontendQueryLifecycleConfig,
    metrics: &FrontendLifecycleMetrics,
) -> Vec<String> {
    std::thread::scope(|scope| {
        let handles = participants
            .iter()
            .map(|participant| {
                scope.spawn(move || {
                    let started = Instant::now();
                    let result = init_one(transport, participant, config.init_rpc_timeout());
                    let latency = started.elapsed();
                    match &result {
                        Ok(QueryInitOutcome::QueryInitApplied) => {
                            metrics.observe_init(true, false, false, false, latency)
                        }
                        Ok(QueryInitOutcome::QueryInitAlreadyApplied) => {
                            metrics.observe_init(false, true, false, false, latency)
                        }
                        Ok(_) => metrics.observe_init(false, false, false, false, latency),
                        Err(error) => metrics.observe_init(
                            false,
                            false,
                            error.uncertain_cleanup,
                            error.manifest_conflict,
                            latency,
                        ),
                    }
                    if result
                        .as_ref()
                        .is_err_and(|error| error.backend_epoch_mismatch)
                    {
                        metrics.backend_epoch_mismatch();
                    }
                    tracing::info!(
                        query_id_high = participant_execution_id(participant).query_id().high(),
                        query_id_low = participant_execution_id(participant).query_id().low(),
                        attempt_id = participant_execution_id(participant).attempt_id().get(),
                        backend_id = participant.target.backend_idx(),
                        backend_start_epoch = participant.target.start_epoch(),
                        participant_digest = %hex::encode(participant.digest.as_bytes()),
                        outcome = ?result,
                        latency_micros = latency.as_micros() as u64,
                        "frontend query lifecycle InitQuery completed"
                    );
                    result.map(|_| ()).map_err(|error| error.message)
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .filter_map(|handle| match handle.join() {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error),
                Err(_) => Some("query lifecycle InitQuery worker panicked".to_string()),
            })
            .collect()
    })
}

#[derive(Debug)]
struct InitFailure {
    message: String,
    uncertain_cleanup: bool,
    manifest_conflict: bool,
    backend_epoch_mismatch: bool,
}

impl InitFailure {
    fn failed(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            uncertain_cleanup: false,
            manifest_conflict: false,
            backend_epoch_mismatch: false,
        }
    }

    fn uncertain(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            uncertain_cleanup: true,
            manifest_conflict: false,
            backend_epoch_mismatch: false,
        }
    }

    fn manifest_conflict(message: impl Into<String>, uncertain_cleanup: bool) -> Self {
        Self {
            message: message.into(),
            uncertain_cleanup,
            manifest_conflict: true,
            backend_epoch_mismatch: false,
        }
    }

    fn backend_epoch_mismatch(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            uncertain_cleanup: false,
            manifest_conflict: false,
            backend_epoch_mismatch: true,
        }
    }
}

fn init_one(
    transport: &dyn QueryLifecycleTransport,
    participant: &MaterializedParticipant,
    timeout: Duration,
) -> Result<QueryInitOutcome, InitFailure> {
    let first = transport.init_query(participant.target, participant.request.clone(), timeout);
    let ack = match first {
        Ok(ack) => ack,
        Err(error) if error.is_unknown_init_outcome() => transport
            .init_query(participant.target, participant.request.clone(), timeout)
            .map_err(|retry| {
                InitFailure::uncertain(format!(
                    "backend {} InitQuery retry failed after unknown outcome ({error}): {retry}",
                    participant.target.backend_idx()
                ))
            })?,
        Err(error) => {
            return Err(InitFailure::failed(format!(
                "backend {} InitQuery failed: {error}",
                participant.target.backend_idx()
            )));
        }
    };
    let expected_execution_id = participant_execution_id(participant);
    if ack
        .execution_id()
        .map_err(|error| InitFailure::failed(error.to_string()))?
        != expected_execution_id
    {
        return Err(InitFailure::uncertain(format!(
            "backend {} InitAck execution id mismatch",
            participant.target.backend_idx()
        )));
    }
    if ack
        .digest()
        .map_err(|error| InitFailure::failed(error.to_string()))?
        .as_bytes()
        != participant.digest.as_bytes()
    {
        return Err(InitFailure::manifest_conflict(
            format!(
                "backend {} InitAck digest mismatch",
                participant.target.backend_idx()
            ),
            true,
        ));
    }
    let outcome = ack
        .outcome()
        .map_err(|error| InitFailure::failed(error.to_string()))?;
    if !matches!(
        outcome,
        QueryInitOutcome::QueryInitApplied | QueryInitOutcome::QueryInitAlreadyApplied
    ) {
        let message = format!(
            "backend {} InitQuery rejected with {:?}",
            participant.target.backend_idx(),
            outcome
        );
        return match outcome {
            QueryInitOutcome::QueryInitRejectedConflict
            | QueryInitOutcome::QueryInitRejectedInvalidManifest => {
                Err(InitFailure::manifest_conflict(message, false))
            }
            QueryInitOutcome::QueryInitRejectedStaleBackend => {
                Err(InitFailure::backend_epoch_mismatch(message))
            }
            _ => Err(InitFailure::failed(message)),
        };
    }
    Ok(outcome)
}

pub(super) fn attach_all(
    transport: &dyn QueryLifecycleTransport,
    participants: &[MaterializedParticipant],
    frontend_owner_epoch: u64,
    config: FrontendQueryLifecycleConfig,
    metrics: &FrontendLifecycleMetrics,
    control: &Arc<AttemptControl>,
) -> Vec<String> {
    let outcomes = std::thread::scope(|scope| {
        let handles = participants
            .iter()
            .map(|participant| {
                scope.spawn(move || {
                    let started = Instant::now();
                    let outcome = attach_one(transport, participant, frontend_owner_epoch, config);
                    let latency = started.elapsed();
                    metrics.observe_attach(outcome.is_ok(), latency);
                    tracing::info!(
                        query_id_high = participant_execution_id(participant).query_id().high(),
                        query_id_low = participant_execution_id(participant).query_id().low(),
                        attempt_id = participant_execution_id(participant).attempt_id().get(),
                        backend_id = participant.target.backend_idx(),
                        backend_start_epoch = participant.target.start_epoch(),
                        participant_digest = %hex::encode(participant.digest.as_bytes()),
                        ready = outcome.is_ok(),
                        latency_micros = latency.as_micros() as u64,
                        "frontend query lifecycle control attach completed"
                    );
                    match &outcome {
                        Ok(session) => {
                            control.add_session(session.clone());
                            control.mark_control_ready(participant.target.backend_idx());
                        }
                        Err((Some(session), _)) => control.add_session(session.clone()),
                        Err((None, _)) => {}
                    }
                    outcome
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| {
                handle.join().unwrap_or_else(|_| {
                    Err((
                        None,
                        "query lifecycle control attach worker panicked".to_string(),
                    ))
                })
            })
            .collect::<Vec<_>>()
    });

    let mut errors = Vec::new();
    for outcome in outcomes {
        if let Err((_, error)) = outcome {
            errors.push(error);
        }
    }
    errors
}

#[expect(
    clippy::result_large_err,
    reason = "The attach failure retains the session so the caller can close it deterministically."
)]
fn attach_one(
    transport: &dyn QueryLifecycleTransport,
    participant: &MaterializedParticipant,
    frontend_owner_epoch: u64,
    config: FrontendQueryLifecycleConfig,
) -> Result<ActiveSession, (Option<ActiveSession>, String)> {
    let attach = QueryControlAttach::parse(protocol_wire::QueryControlAttach {
        execution_id: Some(participant_execution_id(participant).to_proto()),
        init_digest: participant.digest.as_bytes().to_vec(),
        frontend_owner_epoch,
    })
    .map_err(|error| (None, error.to_string()))?;
    let session = transport
        .attach_control(participant.target, attach, config.attach_timeout())
        .map_err(|error| {
            (
                None,
                format!(
                    "backend {} control attach failed: {error}",
                    participant.target.backend_idx()
                ),
            )
        })?;
    let active = ActiveSession::new(participant.target, participant.digest, session);
    match active.recv(config.attach_timeout()) {
        Ok(event)
            if matches!(
                event.as_proto().event.as_ref(),
                Some(protocol_wire::query_control_response::Event::ControlReady(
                    _
                ))
            ) =>
        {
            if let Err(error) = record_control_ready_marker(participant) {
                return Err((Some(active), error));
            }
            Ok(active)
        }
        Ok(event) => Err((
            Some(active),
            format!(
                "backend {} returned {event:?} before ControlReady",
                participant.target.backend_idx()
            ),
        )),
        Err(error) => Err((
            Some(active),
            format!(
                "backend {} ControlReady failed: {error}",
                participant.target.backend_idx()
            ),
        )),
    }
}

#[cfg(debug_assertions)]
fn record_control_ready_marker(participant: &MaterializedParticipant) -> Result<(), String> {
    let Some(root) = novarocks_failpoint::configured_root() else {
        return Ok(());
    };
    let execution_id = participant_execution_id(participant);
    let backend_index = participant.target.backend_idx();
    let trigger_path = root.join("fe-crash-after-control-ready.trigger");
    let contents = match std::fs::read_to_string(&trigger_path) {
        Ok(contents) => Some(contents),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(format!(
                "read runner-owned FE crash trigger {}: {error}",
                trigger_path.display()
            ));
        }
    };
    let Some(contents) = contents else {
        eprintln!(
            "NOVAROCKS_QUERY_CONTROL_READY execution_id={}:{}:{} backend_index={backend_index} token=none ready_count=0",
            execution_id.query_id().high(),
            execution_id.query_id().low(),
            execution_id.attempt_id().get()
        );
        return Ok(());
    };
    let mut lines = contents.lines();
    let token = lines.next().unwrap_or_default().trim();
    let target = lines
        .next()
        .unwrap_or_default()
        .trim()
        .parse::<usize>()
        .map_err(|error| format!("invalid runner-owned FE crash ready count: {error}"))?;
    if target == 0
        || token.is_empty()
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        || lines.any(|line| !line.trim().is_empty())
    {
        return Err("runner-owned FE crash trigger has invalid tokenized contents".to_string());
    }
    static COUNTS: OnceLock<Mutex<BTreeMap<String, usize>>> = OnceLock::new();
    let observed = {
        let mut counts = COUNTS
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .map_err(|_| "lock FE crash ControlReady counter".to_string())?;
        let count = counts.entry(token.to_string()).or_default();
        *count = count.saturating_add(1);
        *count
    };
    eprintln!(
        "NOVAROCKS_QUERY_CONTROL_READY execution_id={}:{}:{} backend_index={backend_index} token={token} ready_count={observed}",
        execution_id.query_id().high(),
        execution_id.query_id().low(),
        execution_id.attempt_id().get()
    );
    if observed == target {
        let deadline = Instant::now() + Duration::from_secs(30);
        while trigger_path.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        return Err(if trigger_path.exists() {
            format!("timed out waiting for runner to kill FE after ControlReady count {target}")
        } else {
            "runner released FE crash trigger without killing FE".to_string()
        });
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
fn record_control_ready_marker(_participant: &MaterializedParticipant) -> Result<(), String> {
    Ok(())
}

fn contract_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

fn failed(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::Failed, message)
}
