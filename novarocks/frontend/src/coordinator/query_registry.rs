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

use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use novarocks::query_execution::backend::LiveBackendTarget;
use novarocks::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, DistributedQueryIntent,
};
use novarocks::service::query_lifecycle_metrics::FrontendQueryLifecycleMetricsSnapshot;
use novarocks_protocol::lifecycle::{ParticipantTerminalOutcome, QueryExecutionId};
use novarocks_types::QueryId;

type QueryKey = (i64, i64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueryLifecycleConvergenceErrorSource {
    BackendAttestation,
    FrontendLiveness,
    NoOutcome,
}

/// Immutable, query-scoped terminal convergence evidence retained alongside
/// the unary terminal ingress.  It is intentionally produced by the attempt
/// control that owns the control streams, never reconstructed from process
/// metrics or logs.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct QueryLifecycleConvergenceSnapshot {
    pub(crate) execution_id: QueryExecutionId,
    pub(crate) error_source: Option<QueryLifecycleConvergenceErrorSource>,
    pub(crate) primary_error: Option<String>,
    pub(crate) participant_outcomes: Vec<ParticipantTerminalOutcome>,
    pub(crate) metrics: FrontendQueryLifecycleMetricsSnapshot,
}

/// Read-only diagnostic seam for the immutable terminal evidence retained by
/// the query registry.  It deliberately has no access to active streams or
/// terminal ingress mutation.
pub(crate) trait QueryLifecycleConvergenceReader: Send + Sync {
    fn latest_convergence_snapshot(&self) -> Option<QueryLifecycleConvergenceSnapshot>;
}

pub(crate) trait ActiveQueryAttemptControl: Send + Sync {
    fn execution_id(&self) -> QueryExecutionId;

    fn request_abort(&self, reason: String);

    /// The terminal ingress is deliberately routed through the active attempt
    /// rather than the legacy execution-report registry.  This keeps the
    /// store-before-ACK identity check in one place for stream and unary
    /// delivery.
    fn report_terminal_outcome(
        &self,
        outcome: ParticipantTerminalOutcome,
    ) -> Result<bool, DistributedQueryError>;

    /// Once every participant outcome is stored, retain the FE ingress long
    /// enough for a BE whose stream ACK was lost to complete unary fallback.
    fn retain_terminal_ingress(&self) -> bool {
        false
    }

    fn convergence_snapshot(&self) -> Option<QueryLifecycleConvergenceSnapshot> {
        None
    }
}

const TERMINAL_INGRESS_RETENTION: Duration = Duration::from_secs(120);
const TERMINAL_INGRESS_RETAINED_CAPACITY: usize = 4_096;

struct RetainedTerminalIngress {
    control: Arc<dyn ActiveQueryAttemptControl>,
    expires_at: Instant,
}

struct ActiveQuery {
    scheduled_backends: BTreeMap<usize, u64>,
    /// The user-visible failure is the lexical minimum of all reported
    /// failures.  This deliberately makes concurrent failure reporting
    /// independent of arrival order until T9 introduces the richer typed
    /// ordering.
    first_failure: Option<String>,
    /// Keep every losing distinct failure for the later structured
    /// convergence snapshot instead of discarding it at the first latch.
    secondary_failures: BTreeSet<String>,
    cancellation_requested: bool,
    cancellation_dispatched: bool,
    active_attempt: Option<Arc<dyn ActiveQueryAttemptControl>>,
}

impl ActiveQuery {
    /// Records a failure using a commutative, idempotent minimum fold.
    ///
    /// `first_failure` remains the compatibility-facing name for the primary
    /// textual error while callers migrate to the typed T9 cause model.
    fn record_failure(&mut self, message: String) -> String {
        match self.first_failure.as_mut() {
            None => self.first_failure = Some(message),
            Some(primary) if message < *primary => {
                let displaced = std::mem::replace(primary, message.clone());
                self.secondary_failures.remove(&message);
                self.secondary_failures.insert(displaced);
            }
            Some(primary) if message != *primary => {
                self.secondary_failures.insert(message);
            }
            Some(_) => {}
        }
        self.first_failure
            .clone()
            .expect("recorded failure has a primary value")
    }
}

#[derive(Default)]
struct BackendTopologyState {
    initialized: bool,
    revision: u64,
    live_generations: BTreeMap<usize, u64>,
}

#[derive(Default)]
pub(crate) struct FrontendQueryRegistry {
    active: Mutex<BTreeMap<QueryKey, ActiveQuery>>,
    retained_terminal_ingress: Mutex<BTreeMap<QueryExecutionId, RetainedTerminalIngress>>,
    latest_retained_execution: Mutex<Option<QueryExecutionId>>,
    backend_topology: Mutex<BackendTopologyState>,
}

pub(crate) struct AttemptBackendOwnershipError {
    error: DistributedQueryError,
    backend_epoch_mismatch: bool,
}

impl AttemptBackendOwnershipError {
    fn new(error: DistributedQueryError, backend_epoch_mismatch: bool) -> Self {
        Self {
            error,
            backend_epoch_mismatch,
        }
    }

    pub(crate) const fn is_backend_epoch_mismatch(&self) -> bool {
        self.backend_epoch_mismatch
    }

    pub(crate) fn into_error(self) -> DistributedQueryError {
        self.error
    }
}

impl FrontendQueryRegistry {
    pub(crate) fn register(
        self: &Arc<Self>,
        query_id: QueryId,
        _intent: DistributedQueryIntent,
        _dispatcher: Arc<dyn novarocks::query_execution::fragment_transport::FragmentDispatcher>,
    ) -> Result<ActiveQueryGuard, DistributedQueryError> {
        let key = query_key(query_id);
        let mut active = self.active.lock().expect("frontend query registry lock");
        match active.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(ActiveQuery {
                    scheduled_backends: BTreeMap::new(),
                    first_failure: None,
                    secondary_failures: BTreeSet::new(),
                    cancellation_requested: false,
                    cancellation_dispatched: false,
                    active_attempt: None,
                });
            }
            Entry::Occupied(_) => {
                return Err(contract_violation(format!(
                    "frontend query {}/{} is already active",
                    query_id.high(),
                    query_id.low()
                )));
            }
        }
        Ok(ActiveQueryGuard {
            registry: Arc::clone(self),
            key,
        })
    }

    pub(crate) fn bind_active_attempt(
        self: &Arc<Self>,
        execution_id: QueryExecutionId,
        control: Arc<dyn ActiveQueryAttemptControl>,
    ) -> Result<ActiveQueryAttemptBinding, DistributedQueryError> {
        if control.execution_id() != execution_id {
            return Err(contract_violation(
                "frontend active attempt control execution id differs from binding",
            ));
        }
        let query_id = execution_id.query_id();
        let mut active = self.active.lock().expect("frontend query registry lock");
        let query = active
            .get_mut(&query_key(query_id))
            .ok_or_else(|| inactive_query(query_id))?;
        if let Some(message) = &query.first_failure {
            return Err(failed(message.clone()));
        }
        if query.cancellation_requested {
            return Err(failed(
                "frontend query cancellation was requested before lifecycle initialization",
            ));
        }
        if query.active_attempt.is_some() {
            return Err(contract_violation(
                "frontend query already has an active attempt control binding",
            ));
        }
        query.active_attempt = Some(control);
        Ok(ActiveQueryAttemptBinding {
            registry: Arc::downgrade(self),
            key: query_key(query_id),
            execution_id,
        })
    }

    pub(crate) fn extend_attempt_backend_ownership(
        &self,
        query_id: QueryId,
        backend_ownership: &[(usize, u64)],
    ) -> Result<(), AttemptBackendOwnershipError> {
        let topology = self
            .backend_topology
            .lock()
            .expect("frontend backend topology gate lock");
        if topology.initialized {
            for &(backend_idx, start_epoch) in backend_ownership {
                match topology.live_generations.get(&backend_idx) {
                    Some(current_epoch) if *current_epoch == start_epoch => {}
                    Some(current_epoch) => {
                        return Err(AttemptBackendOwnershipError::new(
                            DistributedQueryError::new(
                                DistributedQueryErrorKind::Rejected,
                                format!(
                                    "query lifecycle backend {backend_idx} generation {start_epoch} is stale; current generation is {current_epoch}"
                                ),
                            ),
                            true,
                        ));
                    }
                    None => {
                        return Err(AttemptBackendOwnershipError::new(
                            DistributedQueryError::new(
                                DistributedQueryErrorKind::Rejected,
                                format!(
                                    "query lifecycle backend {backend_idx} is no longer live in the current frontend topology"
                                ),
                            ),
                            false,
                        ));
                    }
                }
            }
        }
        drop(topology);

        let mut active = self.active.lock().expect("frontend query registry lock");
        let query = active
            .get_mut(&query_key(query_id))
            .ok_or_else(|| AttemptBackendOwnershipError::new(inactive_query(query_id), false))?;
        for &(backend_idx, start_epoch) in backend_ownership {
            match query.scheduled_backends.entry(backend_idx) {
                Entry::Vacant(entry) => {
                    entry.insert(start_epoch);
                }
                Entry::Occupied(entry) if *entry.get() == start_epoch => {}
                Entry::Occupied(_) => {
                    return Err(AttemptBackendOwnershipError::new(
                        contract_violation(format!(
                            "frontend query lifecycle backend {backend_idx} generation conflicts with scheduled ownership"
                        )),
                        false,
                    ));
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn request_active_attempt_abort(
        &self,
        query_id: QueryId,
        reason: String,
    ) -> Result<(), DistributedQueryError> {
        let control = self
            .active
            .lock()
            .expect("frontend query registry lock")
            .get(&query_key(query_id))
            .ok_or_else(|| inactive_query(query_id))?
            .active_attempt
            .clone()
            .ok_or_else(|| {
                DistributedQueryError::new(
                    DistributedQueryErrorKind::Rejected,
                    "frontend query has no active attempt control binding",
                )
            })?;
        control.request_abort(reason);
        Ok(())
    }

    pub(crate) fn report_query_terminal(
        &self,
        outcome: ParticipantTerminalOutcome,
    ) -> Result<bool, DistributedQueryError> {
        let query_id = outcome.execution_id().query_id();
        let active = self
            .active
            .lock()
            .expect("frontend query registry lock")
            .get(&query_key(query_id))
            .and_then(|query| query.active_attempt.clone());
        let control = match active {
            Some(control) if control.execution_id() == outcome.execution_id() => control,
            Some(_) | None => self.retained_terminal_control(outcome.execution_id())?,
        };
        control.report_terminal_outcome(outcome)
    }

    pub(crate) fn set_scheduled_backend_ownership(
        &self,
        query_id: QueryId,
        backend_ownership: &[(usize, u64)],
    ) -> Result<(), DistributedQueryError> {
        let topology = self
            .backend_topology
            .lock()
            .expect("frontend backend topology gate lock");
        if topology.initialized {
            for &(backend_idx, start_epoch) in backend_ownership {
                match topology.live_generations.get(&backend_idx) {
                    Some(current_epoch) if *current_epoch == start_epoch => {}
                    Some(current_epoch) => {
                        return Err(DistributedQueryError::new(
                            DistributedQueryErrorKind::Rejected,
                            format!(
                                "scheduled backend {backend_idx} generation {start_epoch} is stale; current generation is {current_epoch}"
                            ),
                        ));
                    }
                    None => {
                        return Err(DistributedQueryError::new(
                            DistributedQueryErrorKind::Rejected,
                            format!(
                                "scheduled backend {backend_idx} is no longer live in the current frontend topology"
                            ),
                        ));
                    }
                }
            }
        }
        let mut active = self.active.lock().expect("frontend query registry lock");
        let query = active
            .get_mut(&query_key(query_id))
            .ok_or_else(|| inactive_query(query_id))?;
        if !query.scheduled_backends.is_empty() {
            return Err(contract_violation(
                "frontend query scheduled backend ownership is already registered",
            ));
        }
        for &(backend_idx, start_epoch) in backend_ownership {
            if query
                .scheduled_backends
                .insert(backend_idx, start_epoch)
                .is_some()
            {
                return Err(contract_violation(
                    "frontend query scheduled backend ownership contains duplicate backend ids",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn replace_live_backends(&self, revision: u64, backends: &[LiveBackendTarget]) {
        let mut topology = self
            .backend_topology
            .lock()
            .expect("frontend backend topology gate lock");
        if topology.initialized && revision < topology.revision {
            return;
        }
        let previous_revision = topology.revision;
        let revision_changed = topology.initialized && revision != previous_revision;
        topology.initialized = true;
        topology.revision = revision;
        topology.live_generations = backends
            .iter()
            .map(|target| (target.backend_idx(), target.start_epoch()))
            .collect();
        drop(topology);

        // A captured statement is only valid for one revision. This includes a
        // backend join: accepting a new target mid-query would make planning,
        // scheduling and ownership disagree about the same request.
        if !revision_changed {
            return;
        }
        let cancellations = {
            let mut active = self.active.lock().expect("frontend query registry lock");
            active
                .values_mut()
                .filter_map(|query| {
                    query.record_failure(format!(
                        "backend topology revision changed from {previous_revision} to {revision}"
                    ));
                    Some(request_cancellation(query))
                })
                .collect::<Vec<_>>()
        };
        for cancellation in cancellations {
            dispatch_cancellation(Some(cancellation));
        }
    }

    #[cfg(test)]
    pub(crate) fn set_scheduled_backends(
        &self,
        query_id: QueryId,
        backend_ids: &[usize],
    ) -> Result<(), DistributedQueryError> {
        let ownership = backend_ids
            .iter()
            .map(|&backend_idx| (backend_idx, 0))
            .collect::<Vec<_>>();
        self.set_scheduled_backend_ownership(query_id, &ownership)
    }

    #[cfg(test)]
    pub(crate) fn finish_attempt(&self, query_id: QueryId) -> Result<(), DistributedQueryError> {
        if self
            .active
            .lock()
            .expect("frontend query registry lock")
            .contains_key(&query_key(query_id))
        {
            return Ok(());
        }
        Err(inactive_query(query_id))
    }

    pub(crate) fn first_failure(&self, query_id: QueryId) -> Option<String> {
        self.active
            .lock()
            .expect("frontend query registry lock")
            .get(&query_key(query_id))
            .and_then(|query| query.first_failure.clone())
    }

    pub(crate) fn retained_convergence_snapshot(
        &self,
        execution_id: QueryExecutionId,
    ) -> Option<QueryLifecycleConvergenceSnapshot> {
        self.retained_terminal_control(execution_id)
            .ok()
            .and_then(|control| control.convergence_snapshot())
    }

    fn latest_retained_convergence_snapshot(&self) -> Option<QueryLifecycleConvergenceSnapshot> {
        let execution_id = *self
            .latest_retained_execution
            .lock()
            .expect("frontend latest retained terminal ingress lock");
        execution_id.and_then(|execution_id| self.retained_convergence_snapshot(execution_id))
    }

    pub(crate) fn preserve_failure_context(
        &self,
        query_id: QueryId,
        message: String,
    ) -> Result<(), DistributedQueryError> {
        let mut active = self.active.lock().expect("frontend query registry lock");
        let query = active
            .get_mut(&query_key(query_id))
            .ok_or_else(|| inactive_query(query_id))?;
        query.record_failure(message);
        Ok(())
    }

    pub(crate) fn latch_failure_and_cancel(
        &self,
        query_id: QueryId,
        message: impl Into<String>,
    ) -> Result<String, DistributedQueryError> {
        let (message, cancellation) = {
            let mut active = self.active.lock().expect("frontend query registry lock");
            let query = active
                .get_mut(&query_key(query_id))
                .ok_or_else(|| inactive_query(query_id))?;
            let message = query.record_failure(message.into());
            (message, request_cancellation(query))
        };
        dispatch_cancellation(Some(cancellation));
        Ok(message)
    }

    pub(crate) fn backend_failed(&self, backend_idx: usize, message: String) -> Vec<QueryId> {
        let (affected, cancellations) = {
            let mut active = self.active.lock().expect("frontend query registry lock");
            let mut affected = Vec::new();
            let mut cancellations = Vec::new();
            for (&(high, low), query) in active.iter_mut() {
                if !query.scheduled_backends.contains_key(&backend_idx) {
                    continue;
                }
                if query.first_failure.is_none() {
                    query.record_failure(message.clone());
                    affected.push(QueryId::new(high, low));
                } else {
                    query.record_failure(message.clone());
                }
                cancellations.push(request_cancellation(query));
            }
            (affected, cancellations)
        };

        for cancellation in cancellations {
            dispatch_cancellation(Some(cancellation));
        }
        affected
    }

    pub(crate) fn backend_restarted(
        &self,
        backend_idx: usize,
        old_epoch: u64,
        message: String,
    ) -> Vec<QueryId> {
        let (affected, cancellations) = {
            let mut active = self.active.lock().expect("frontend query registry lock");
            let mut affected = Vec::new();
            let mut cancellations = Vec::new();
            for (&(high, low), query) in active.iter_mut() {
                if query.scheduled_backends.get(&backend_idx) != Some(&old_epoch) {
                    continue;
                }
                if query.first_failure.is_none() {
                    query.record_failure(message.clone());
                    affected.push(QueryId::new(high, low));
                } else {
                    query.record_failure(message.clone());
                }
                cancellations.push(request_cancellation(query));
            }
            (affected, cancellations)
        };

        for cancellation in cancellations {
            dispatch_cancellation(Some(cancellation));
        }
        affected
    }

    pub(crate) fn backend_has_active_queries(&self, backend_idx: usize) -> bool {
        self.active
            .lock()
            .expect("frontend query registry lock")
            .values()
            .any(|query| query.scheduled_backends.contains_key(&backend_idx))
    }

    fn unregister(&self, key: QueryKey) {
        self.active
            .lock()
            .expect("frontend query registry lock")
            .remove(&key);
    }

    fn clear_active_attempt(&self, key: QueryKey, execution_id: QueryExecutionId) {
        let control = {
            let mut active = self.active.lock().expect("frontend query registry lock");
            let Some(query) = active.get_mut(&key) else {
                return;
            };
            if query
                .active_attempt
                .as_ref()
                .is_some_and(|control| control.execution_id() == execution_id)
            {
                query.active_attempt.take()
            } else {
                None
            }
        };
        if let Some(control) = control {
            if control.retain_terminal_ingress() {
                self.retain_terminal_control(control);
            }
        }
    }

    fn retain_terminal_control(&self, control: Arc<dyn ActiveQueryAttemptControl>) {
        let execution_id = control.execution_id();
        let now = Instant::now();
        let mut retained = self
            .retained_terminal_ingress
            .lock()
            .expect("frontend retained terminal ingress lock");
        retained.retain(|_, ingress| ingress.expires_at > now);
        if retained.len() >= TERMINAL_INGRESS_RETAINED_CAPACITY {
            if let Some(oldest) = retained
                .iter()
                .min_by_key(|(_, ingress)| ingress.expires_at)
                .map(|(execution_id, _)| *execution_id)
            {
                retained.remove(&oldest);
            }
        }
        retained.insert(
            execution_id,
            RetainedTerminalIngress {
                control,
                expires_at: now + TERMINAL_INGRESS_RETENTION,
            },
        );
        *self
            .latest_retained_execution
            .lock()
            .expect("frontend latest retained terminal ingress lock") = Some(execution_id);
    }

    fn retained_terminal_control(
        &self,
        execution_id: QueryExecutionId,
    ) -> Result<Arc<dyn ActiveQueryAttemptControl>, DistributedQueryError> {
        let now = Instant::now();
        let mut retained = self
            .retained_terminal_ingress
            .lock()
            .expect("frontend retained terminal ingress lock");
        retained.retain(|_, ingress| ingress.expires_at > now);
        if self
            .latest_retained_execution
            .lock()
            .expect("frontend latest retained terminal ingress lock")
            .is_some_and(|latest| !retained.contains_key(&latest))
        {
            *self
                .latest_retained_execution
                .lock()
                .expect("frontend latest retained terminal ingress lock") = None;
        }
        retained
            .get(&execution_id)
            .map(|ingress| Arc::clone(&ingress.control))
            .ok_or_else(|| {
                DistributedQueryError::new(
                    DistributedQueryErrorKind::Rejected,
                    "query terminal snapshot execution id is stale or has no retained ingress",
                )
            })
    }
}

impl QueryLifecycleConvergenceReader for FrontendQueryRegistry {
    fn latest_convergence_snapshot(&self) -> Option<QueryLifecycleConvergenceSnapshot> {
        self.latest_retained_convergence_snapshot()
    }
}

pub(crate) struct ActiveQueryGuard {
    registry: Arc<FrontendQueryRegistry>,
    key: QueryKey,
}

pub(crate) struct ActiveQueryAttemptBinding {
    registry: std::sync::Weak<FrontendQueryRegistry>,
    key: QueryKey,
    execution_id: QueryExecutionId,
}

impl Drop for ActiveQueryAttemptBinding {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            registry.clear_active_attempt(self.key, self.execution_id);
        }
    }
}

impl Drop for ActiveQueryGuard {
    fn drop(&mut self) {
        self.registry.unregister(self.key);
    }
}

struct CancellationDispatch {
    active_attempt: Option<Arc<dyn ActiveQueryAttemptControl>>,
    reason: String,
}

fn request_cancellation(query: &mut ActiveQuery) -> CancellationDispatch {
    query.cancellation_requested = true;
    let active_attempt = if query.cancellation_dispatched {
        None
    } else {
        let control = query.active_attempt.clone();
        if control.is_some() {
            query.cancellation_dispatched = true;
        }
        control
    };
    CancellationDispatch {
        active_attempt,
        reason: query
            .first_failure
            .clone()
            .unwrap_or_else(|| "frontend query cancellation requested".to_string()),
    }
}

fn dispatch_cancellation(cancellation: Option<CancellationDispatch>) {
    if let Some(cancellation) = cancellation {
        if let Some(control) = cancellation.active_attempt {
            control.request_abort(cancellation.reason);
        }
    }
}

fn query_key(query_id: QueryId) -> QueryKey {
    (query_id.high(), query_id.low())
}

fn inactive_query(query_id: QueryId) -> DistributedQueryError {
    DistributedQueryError::new(
        DistributedQueryErrorKind::Rejected,
        format!(
            "frontend query {}/{} is not active",
            query_id.high(),
            query_id.low()
        ),
    )
}

fn contract_violation(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

fn failed(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::Failed, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use novarocks_protocol::lifecycle::{
        AttemptId, ParticipantTerminalOutcome, QueryTerminalSnapshot, TerminalizationProof,
    };
    use novarocks_protocol::{common, novarocks as proto};

    struct RetainedControl {
        execution_id: QueryExecutionId,
        reports: AtomicUsize,
    }

    impl ActiveQueryAttemptControl for RetainedControl {
        fn execution_id(&self) -> QueryExecutionId {
            self.execution_id
        }

        fn request_abort(&self, _reason: String) {}

        fn report_terminal_outcome(
            &self,
            _outcome: ParticipantTerminalOutcome,
        ) -> Result<bool, DistributedQueryError> {
            self.reports.fetch_add(1, Ordering::SeqCst);
            Ok(false)
        }

        fn retain_terminal_ingress(&self) -> bool {
            true
        }

        fn convergence_snapshot(&self) -> Option<QueryLifecycleConvergenceSnapshot> {
            Some(QueryLifecycleConvergenceSnapshot {
                execution_id: self.execution_id,
                error_source: None,
                primary_error: Some("stable test failure".to_string()),
                participant_outcomes: Vec::new(),
                metrics: FrontendQueryLifecycleMetricsSnapshot::default(),
            })
        }
    }

    fn terminal_outcome(execution_id: QueryExecutionId) -> ParticipantTerminalOutcome {
        let backend = proto::ParticipantBackendIdentity {
            backend_id: 7,
            endpoint: Some(proto::QueryControlEndpoint {
                host: "127.0.0.1".into(),
                port: 9030,
            }),
            start_epoch: 11,
        };
        let fragment = proto::QueryTerminalFragmentSnapshot {
            fragment_instance_id: Some(common::UniqueId { hi: 1, lo: 2 }),
            backend_num: 7,
            outcome: proto::QueryTerminalFragmentOutcome::Succeeded as i32,
            load_stats: Some(proto::QueryTerminalLoadStats::default()),
            profile: Some(proto::FragmentTerminalProfileTelemetry {
                telemetry: Some(
                    proto::fragment_terminal_profile_telemetry::Telemetry::Unavailable(
                        proto::TerminalTelemetryUnavailable {
                            stage: "test".into(),
                            code: "UNAVAILABLE".into(),
                        },
                    ),
                ),
            }),
            ..Default::default()
        };
        let snapshot = QueryTerminalSnapshot::seal(proto::QueryTerminalSnapshot {
            version: 1,
            execution_id: Some(execution_id.into()),
            backend: Some(backend.clone()),
            init_digest: vec![3; 32],
            fragments: vec![fragment],
            profile_contribution: Some(proto::QueryTerminalProfileContributionTelemetry {
                telemetry: Some(
                    proto::query_terminal_profile_contribution_telemetry::Telemetry::Unavailable(
                        proto::TerminalTelemetryUnavailable {
                            stage: "test".into(),
                            code: "UNAVAILABLE".into(),
                        },
                    ),
                ),
            }),
            ..Default::default()
        })
        .expect("terminal snapshot");
        let proof = TerminalizationProof::seal(proto::TerminalizationProof {
            version: 1,
            execution_id: Some(execution_id.into()),
            backend: Some(backend),
            init_digest: vec![3; 32],
            fragments: vec![proto::TerminalizationProofFragment {
                fragment_instance_id: Some(common::UniqueId { hi: 1, lo: 2 }),
                backend_num: 7,
                outcome: proto::QueryTerminalFragmentOutcome::Succeeded as i32,
                ..Default::default()
            }],
            ..Default::default()
        })
        .expect("terminal proof");
        ParticipantTerminalOutcome::parse(proto::ParticipantTerminalOutcome {
            outcome: Some(proto::participant_terminal_outcome::Outcome::Proof(
                proof.as_proto().clone(),
            )),
            snapshot: Some(snapshot.as_proto().clone()),
        })
        .expect("participant terminal outcome")
    }

    #[test]
    fn retained_terminal_ingress_accepts_same_execution_after_active_query_unregistered() {
        let registry = FrontendQueryRegistry::default();
        let execution_id =
            QueryExecutionId::new(QueryId::new(41, 42), AttemptId::new(1).expect("attempt"))
                .expect("execution id");
        let control = Arc::new(RetainedControl {
            execution_id,
            reports: AtomicUsize::new(0),
        });
        registry.retain_terminal_control(control.clone());

        assert!(
            !registry
                .report_query_terminal(terminal_outcome(execution_id))
                .expect("retained ingress accepts duplicate terminal delivery")
        );
        assert_eq!(control.reports.load(Ordering::SeqCst), 1);
        assert_eq!(
            registry
                .retained_convergence_snapshot(execution_id)
                .expect("retained control exposes its immutable convergence snapshot")
                .primary_error
                .as_deref(),
            Some("stable test failure")
        );
        assert_eq!(
            QueryLifecycleConvergenceReader::latest_convergence_snapshot(&registry)
                .expect("latest retained control exposes convergence snapshot")
                .execution_id,
            execution_id,
            "the read-only diagnostic seam returns retained attempt evidence"
        );
    }

    #[test]
    fn failure_primary_is_stable_when_reports_arrive_in_different_orders() {
        fn record_in_order(messages: &[&str]) -> (String, Vec<String>) {
            let registry = Arc::new(FrontendQueryRegistry::default());
            let query_id = QueryId::new(71, 72);
            registry
                .active
                .lock()
                .expect("frontend query registry lock")
                .insert(
                    query_key(query_id),
                    ActiveQuery {
                        scheduled_backends: BTreeMap::new(),
                        first_failure: None,
                        secondary_failures: BTreeSet::new(),
                        cancellation_requested: false,
                        cancellation_dispatched: false,
                        active_attempt: None,
                    },
                );
            for message in messages {
                registry
                    .latch_failure_and_cancel(query_id, (*message).to_string())
                    .expect("latch failure");
            }
            let active = registry
                .active
                .lock()
                .expect("frontend query registry lock");
            let query = active
                .get(&query_key(query_id))
                .expect("registered query remains active");
            (
                query.first_failure.clone().expect("primary failure"),
                query.secondary_failures.iter().cloned().collect(),
            )
        }

        let forward = record_in_order(&["zeta failure", "alpha failure", "middle failure"]);
        let reverse = record_in_order(&["middle failure", "alpha failure", "zeta failure"]);

        assert_eq!(forward, reverse);
        assert_eq!(forward.0, "alpha failure");
        assert_eq!(
            forward.1,
            vec!["middle failure".to_string(), "zeta failure".to_string()]
        );
    }
}
