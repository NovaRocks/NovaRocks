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

use crate::common::backend_topology::LiveBackendTarget;
use crate::metrics::FrontendProcessQueryCountersSnapshot;
use crate::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, DistributedQueryIntent,
};
use crate::query_execution::runtime_filter_terminal_rollup::RuntimeFilterTerminalRollup;
use novarocks_proto_codec::lifecycle::{ParticipantTerminalOutcome, QueryExecutionId};
use novarocks_types::{BackendProcessId, QueryId, QueryProcessNamespace};

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
    /// Runtime Filter terminal facts are normalized only from a complete,
    /// canonical `QueryTerminalSet`.  The unavailable variant records why no
    /// such set existed for this retained lifecycle snapshot.
    pub(crate) runtime_filter: RuntimeFilterTerminalRollupSnapshot,
    pub(crate) metrics: FrontendProcessQueryCountersSnapshot,
}

#[derive(Clone, Debug, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "The available variant owns the complete terminal runtime-filter snapshot."
)]
pub(crate) enum RuntimeFilterTerminalRollupSnapshot {
    Available(RuntimeFilterTerminalRollup),
    Unavailable(RuntimeFilterTerminalRollupUnavailable),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeFilterTerminalRollupUnavailable {
    TerminalOutcomesIncomplete,
    NegativeAttestation,
}

/// Read-only diagnostic seam for the immutable terminal evidence retained by
/// the query registry.  It deliberately has no access to active streams or
/// terminal ingress mutation.
pub(crate) trait QueryLifecycleConvergenceReader: Send + Sync {
    fn latest_convergence_snapshot(&self) -> Option<QueryLifecycleConvergenceSnapshot>;
}

pub(crate) trait ActiveQueryAttemptControl: Send + Sync {
    fn execution_id(&self) -> QueryExecutionId;

    fn request_abort(&self, failure: LatchedQueryFailure);

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

/// Typed origin of a query failure retained by the FE query owner.
///
/// The variants deliberately describe evidence, not rendered text. This lets
/// the registry preserve a concrete query failure when lifecycle supervision
/// subsequently observes the control stream closing as a consequence of that
/// failure.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum QueryFailureCause {
    LifecycleHeartbeatTimeout,
    RemoteTransportObservation,
    BackendProcessLoss,
    ClientCancellation,
    FrontendExecution,
    BackendLocalFailure,
}

impl QueryFailureCause {
    const fn priority(self) -> QueryFailurePriority {
        match self {
            Self::LifecycleHeartbeatTimeout | Self::RemoteTransportObservation => {
                QueryFailurePriority::LifecycleObservation
            }
            Self::BackendProcessLoss
            | Self::ClientCancellation
            | Self::FrontendExecution
            | Self::BackendLocalFailure => QueryFailurePriority::ConcreteCause,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum QueryFailurePriority {
    LifecycleObservation,
    ConcreteCause,
}

/// The primary failure selected by the query registry.
///
/// `id` identifies the selected causal record, so abort cleanup may enrich
/// only that exact failure and cannot overwrite a concurrently observed,
/// higher-priority cause.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LatchedQueryFailure {
    id: u64,
    cause: QueryFailureCause,
    message: String,
}

impl LatchedQueryFailure {
    fn new(id: u64, cause: QueryFailureCause, message: String) -> Self {
        Self { id, cause, message }
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    pub(crate) const fn id(&self) -> u64 {
        self.id
    }
}

struct ActiveQuery {
    /// Process identities, not durable membership ids. A `backend_idx` is a
    /// round-local scheduling ordinal and must never identify an active
    /// attempt after topology publication.
    scheduled_backends: BTreeSet<BackendProcessId>,
    /// A concrete cause supersedes a lifecycle observation. Within one
    /// priority class the first observed cause remains primary, preserving
    /// causal ordering without interpreting rendered error text.
    first_failure: Option<LatchedQueryFailure>,
    /// Keep every losing distinct failure for the later structured
    /// convergence snapshot instead of discarding it at the first latch.
    secondary_failures: BTreeSet<(QueryFailureCause, String)>,
    next_failure_id: u64,
    cancellation_requested: bool,
    cancellation_dispatched: bool,
    active_attempt: Option<Arc<dyn ActiveQueryAttemptControl>>,
}

impl ActiveQuery {
    fn record_failure(&mut self, cause: QueryFailureCause, message: String) -> LatchedQueryFailure {
        if let Some(primary) = &self.first_failure
            && primary.cause == cause
            && primary.message == message
        {
            return primary.clone();
        }

        let id = self.next_failure_id;
        self.next_failure_id = self
            .next_failure_id
            .checked_add(1)
            .expect("frontend query failure id exhausted");
        let candidate = LatchedQueryFailure::new(id, cause, message);
        match self.first_failure.as_mut() {
            None => self.first_failure = Some(candidate),
            Some(primary) if cause.priority() > primary.cause.priority() => {
                let displaced = std::mem::replace(primary, candidate);
                self.secondary_failures
                    .insert((displaced.cause, displaced.message));
            }
            Some(_) => {
                self.secondary_failures
                    .insert((candidate.cause, candidate.message));
            }
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
    live_process_ids: BTreeMap<usize, BackendProcessId>,
}

pub(crate) struct FrontendQueryRegistry {
    namespace: QueryProcessNamespace,
    active: Mutex<BTreeMap<QueryKey, ActiveQuery>>,
    retained_terminal_ingress: Mutex<BTreeMap<QueryExecutionId, RetainedTerminalIngress>>,
    latest_retained_execution: Mutex<Option<QueryExecutionId>>,
    /// The convergence evidence of the most recent attempt to finish, whatever
    /// protocol ran it.
    ///
    /// One slot, last publication wins. The two protocols each publish their
    /// own attempt's immutable evidence into it, which is not two authorities
    /// over one decision: an execution id is owned by exactly one attempt, and
    /// an attempt runs on exactly one protocol. What the slot must not become
    /// is a place where a reader chooses between two candidates for the same
    /// attempt.
    latest_convergence: Mutex<Option<RetainedConvergenceEvidence>>,
    backend_topology: Mutex<BackendTopologyState>,
}

/// Where the latest attempt's convergence evidence is read from.
///
/// The lifecycle variant is deliberately lazy. That attempt keeps folding late
/// unary terminal outcomes in after its registry binding drops, so freezing a
/// snapshot at retention time would publish evidence that is complete only by
/// accident of timing. The task protocol has no late ingress: its evidence is
/// complete when the last release settles, so it is published frozen.
enum RetainedConvergenceEvidence {
    LifecycleAttempt(Arc<dyn ActiveQueryAttemptControl>),
    TaskRound(Box<QueryLifecycleConvergenceSnapshot>),
}

impl RetainedConvergenceEvidence {
    fn snapshot(&self) -> Option<QueryLifecycleConvergenceSnapshot> {
        match self {
            Self::LifecycleAttempt(control) => control.convergence_snapshot(),
            Self::TaskRound(snapshot) => Some((**snapshot).clone()),
        }
    }
}

pub(crate) struct AttemptBackendOwnershipError {
    error: DistributedQueryError,
    backend_process_mismatch: bool,
}

impl AttemptBackendOwnershipError {
    fn new(error: DistributedQueryError, backend_process_mismatch: bool) -> Self {
        Self {
            error,
            backend_process_mismatch,
        }
    }

    pub(crate) const fn is_backend_process_mismatch(&self) -> bool {
        self.backend_process_mismatch
    }

    pub(crate) fn into_error(self) -> DistributedQueryError {
        self.error
    }
}

impl FrontendQueryRegistry {
    pub(crate) fn new(namespace: QueryProcessNamespace) -> Self {
        Self {
            namespace,
            active: Mutex::new(BTreeMap::new()),
            retained_terminal_ingress: Mutex::new(BTreeMap::new()),
            latest_retained_execution: Mutex::new(None),
            latest_convergence: Mutex::new(None),
            backend_topology: Mutex::new(BackendTopologyState::default()),
        }
    }

    fn describe_query_id(&self, query_id: QueryId) -> String {
        match query_id.process_attribution() {
            Some(attribution) => attribution.to_string(),
            None => format!(
                "raw_query_id={}/{} attribution=unavailable",
                query_id.high(),
                query_id.low()
            ),
        }
    }

    fn inactive_query(&self, query_id: QueryId) -> DistributedQueryError {
        let description = self.describe_query_id(query_id);
        let message = match query_id.process_attribution() {
            Some(attribution) if attribution.namespace() == self.namespace => {
                format!("frontend local query is not active ({description})")
            }
            Some(_) => format!(
                "frontend query belongs to a foreign process and is not active \
                 (local_namespace={} {description})",
                self.namespace
            ),
            None => format!(
                "frontend query has no valid process attribution and is not active \
                 (local_namespace={} {description})",
                self.namespace
            ),
        };
        DistributedQueryError::new(DistributedQueryErrorKind::Rejected, message)
    }

    pub(crate) fn register(
        self: &Arc<Self>,
        query_id: QueryId,
        _intent: DistributedQueryIntent,
        _dispatcher: Arc<dyn crate::native::fragment_transport::FragmentDispatcher>,
    ) -> Result<ActiveQueryGuard, DistributedQueryError> {
        let key = query_key(query_id);
        let mut active = self.active.lock().expect("frontend query registry lock");
        match active.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(ActiveQuery {
                    scheduled_backends: BTreeSet::new(),
                    first_failure: None,
                    secondary_failures: BTreeSet::new(),
                    next_failure_id: 1,
                    cancellation_requested: false,
                    cancellation_dispatched: false,
                    active_attempt: None,
                });
            }
            Entry::Occupied(_) => {
                return Err(contract_violation(format!(
                    "frontend query is already active ({})",
                    self.describe_query_id(query_id)
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
            .ok_or_else(|| self.inactive_query(query_id))?;
        if let Some(failure) = &query.first_failure {
            return Err(failed(failure.message.clone()));
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
        backend_ownership: &[(usize, BackendProcessId)],
    ) -> Result<(), AttemptBackendOwnershipError> {
        let topology = self
            .backend_topology
            .lock()
            .expect("frontend backend topology gate lock");
        if topology.initialized {
            for &(backend_idx, process_id) in backend_ownership {
                match topology.live_process_ids.get(&backend_idx) {
                    Some(current_process_id) if *current_process_id == process_id => {}
                    Some(current_process_id) => {
                        return Err(AttemptBackendOwnershipError::new(
                            DistributedQueryError::new(
                                DistributedQueryErrorKind::Rejected,
                                format!(
                                    "query lifecycle backend ordinal {backend_idx} process identity {process_id} is stale; current process identity is {current_process_id}"
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
                                    "query lifecycle backend ordinal {backend_idx} process identity {process_id} is no longer live in the current frontend topology"
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
        let query = active.get_mut(&query_key(query_id)).ok_or_else(|| {
            AttemptBackendOwnershipError::new(self.inactive_query(query_id), false)
        })?;
        for &(_, process_id) in backend_ownership {
            query.scheduled_backends.insert(process_id);
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
            .ok_or_else(|| self.inactive_query(query_id))?
            .active_attempt
            .clone()
            .ok_or_else(|| {
                DistributedQueryError::new(
                    DistributedQueryErrorKind::Rejected,
                    "frontend query has no active attempt control binding",
                )
            })?;
        control.request_abort(LatchedQueryFailure::new(
            0,
            QueryFailureCause::ClientCancellation,
            reason,
        ));
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
        backend_ownership: &[(usize, BackendProcessId)],
    ) -> Result<(), DistributedQueryError> {
        let topology = self
            .backend_topology
            .lock()
            .expect("frontend backend topology gate lock");
        if topology.initialized {
            for &(backend_idx, process_id) in backend_ownership {
                match topology.live_process_ids.get(&backend_idx) {
                    Some(current_process_id) if *current_process_id == process_id => {}
                    Some(current_process_id) => {
                        return Err(DistributedQueryError::new(
                            DistributedQueryErrorKind::Rejected,
                            format!(
                                "scheduled backend ordinal {backend_idx} process identity {process_id} is stale; current process identity is {current_process_id}"
                            ),
                        ));
                    }
                    None => {
                        return Err(DistributedQueryError::new(
                            DistributedQueryErrorKind::Rejected,
                            format!(
                                "scheduled backend ordinal {backend_idx} process identity {process_id} is no longer live in the current frontend topology"
                            ),
                        ));
                    }
                }
            }
        }
        let mut active = self.active.lock().expect("frontend query registry lock");
        let query = active
            .get_mut(&query_key(query_id))
            .ok_or_else(|| self.inactive_query(query_id))?;
        if !query.scheduled_backends.is_empty() {
            return Err(contract_violation(
                "frontend query scheduled backend ownership is already registered",
            ));
        }
        for &(_, process_id) in backend_ownership {
            if !query.scheduled_backends.insert(process_id) {
                return Err(contract_violation(
                    "frontend query scheduled backend ownership contains duplicate backend process identities",
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
        topology.initialized = true;
        topology.revision = revision;
        topology.live_process_ids = backends
            .iter()
            .map(|target| {
                (
                    target.backend_idx(),
                    target
                        .process_id()
                        .expect("published live backend target has a validated process id"),
                )
            })
            .collect();
        drop(topology);

        // A topology revision governs future statement admission only. Existing
        // attempts retain their frozen participant manifest and are failed only
        // by lifecycle/control/transport evidence, or an exact replacement
        // event for one of their process identities.
    }

    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "Registry test helper preserves explicit terminal-attempt assertions."
    )]
    pub(crate) fn finish_attempt(&self, query_id: QueryId) -> Result<(), DistributedQueryError> {
        if self
            .active
            .lock()
            .expect("frontend query registry lock")
            .contains_key(&query_key(query_id))
        {
            return Ok(());
        }
        Err(self.inactive_query(query_id))
    }

    pub(crate) fn first_failure(&self, query_id: QueryId) -> Option<String> {
        self.active
            .lock()
            .expect("frontend query registry lock")
            .get(&query_key(query_id))
            .and_then(|query| {
                query
                    .first_failure
                    .as_ref()
                    .map(|failure| failure.message.clone())
            })
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
        self.latest_convergence
            .lock()
            .expect("frontend latest convergence evidence lock")
            .as_ref()
            .and_then(RetainedConvergenceEvidence::snapshot)
    }

    /// Publishes the immutable convergence evidence of one task-protocol
    /// attempt.
    ///
    /// Called once per attempt, after its last query context released: that is
    /// the point at which every participant's contribution is either in hand
    /// or will never arrive. Nothing reads a partial attempt here, so the
    /// evidence is frozen rather than recomputed on read.
    pub(crate) fn publish_task_round_convergence(
        &self,
        snapshot: QueryLifecycleConvergenceSnapshot,
    ) {
        *self
            .latest_convergence
            .lock()
            .expect("frontend latest convergence evidence lock") =
            Some(RetainedConvergenceEvidence::TaskRound(Box::new(snapshot)));
    }

    pub(crate) fn preserve_failure_context(
        &self,
        query_id: QueryId,
        failure_id: u64,
        message: String,
    ) -> Result<(), DistributedQueryError> {
        let mut active = self.active.lock().expect("frontend query registry lock");
        let query = active
            .get_mut(&query_key(query_id))
            .ok_or_else(|| self.inactive_query(query_id))?;
        if let Some(primary) = query.first_failure.as_mut()
            && primary.id == failure_id
        {
            primary.message = message;
        }
        Ok(())
    }

    pub(crate) fn latch_failure_and_cancel(
        &self,
        query_id: QueryId,
        cause: QueryFailureCause,
        message: impl Into<String>,
    ) -> Result<LatchedQueryFailure, DistributedQueryError> {
        let (message, cancellation) = self.latch_failure(query_id, cause, message.into())?;
        dispatch_cancellation(Some(cancellation));
        Ok(message)
    }

    /// Records the failure before moving potentially blocking abort work off
    /// the caller. Control-stream readers use this form so observing a typed
    /// LocalFailure is linearized before that same reader continues receiving
    /// termination events.
    pub(crate) fn latch_failure_and_cancel_async(
        &self,
        query_id: QueryId,
        cause: QueryFailureCause,
        message: impl Into<String>,
    ) -> Result<LatchedQueryFailure, DistributedQueryError> {
        let (message, cancellation) = self.latch_failure(query_id, cause, message.into())?;
        if cancellation.active_attempt.is_some() {
            std::thread::spawn(move || dispatch_cancellation(Some(cancellation)));
        }
        Ok(message)
    }

    fn latch_failure(
        &self,
        query_id: QueryId,
        cause: QueryFailureCause,
        message: String,
    ) -> Result<(LatchedQueryFailure, CancellationDispatch), DistributedQueryError> {
        let mut active = self.active.lock().expect("frontend query registry lock");
        let query = active
            .get_mut(&query_key(query_id))
            .ok_or_else(|| self.inactive_query(query_id))?;
        let failure = query.record_failure(cause, message);
        let cancellation = request_cancellation(query);
        Ok((failure, cancellation))
    }

    pub(crate) fn backend_failed(
        &self,
        process_id: BackendProcessId,
        message: String,
    ) -> Vec<QueryId> {
        let (affected, cancellations) = {
            let mut active = self.active.lock().expect("frontend query registry lock");
            let mut affected = Vec::new();
            let mut cancellations = Vec::new();
            for (&(high, low), query) in active.iter_mut() {
                if !query.scheduled_backends.contains(&process_id) {
                    continue;
                }
                if query.first_failure.is_none() {
                    query.record_failure(QueryFailureCause::BackendProcessLoss, message.clone());
                    affected.push(QueryId::new(high, low));
                } else {
                    query.record_failure(QueryFailureCause::BackendProcessLoss, message.clone());
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
        if let Some(control) = control
            && control.retain_terminal_ingress()
        {
            self.retain_terminal_control(control);
        }
    }

    fn retain_terminal_control(&self, control: Arc<dyn ActiveQueryAttemptControl>) {
        let execution_id = control.execution_id();
        let convergence = Arc::clone(&control);
        let now = Instant::now();
        let mut retained = self
            .retained_terminal_ingress
            .lock()
            .expect("frontend retained terminal ingress lock");
        retained.retain(|_, ingress| ingress.expires_at > now);
        if retained.len() >= TERMINAL_INGRESS_RETAINED_CAPACITY
            && let Some(oldest) = retained
                .iter()
                .min_by_key(|(_, ingress)| ingress.expires_at)
                .map(|(execution_id, _)| *execution_id)
        {
            retained.remove(&oldest);
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
        *self
            .latest_convergence
            .lock()
            .expect("frontend latest convergence evidence lock") =
            Some(RetainedConvergenceEvidence::LifecycleAttempt(convergence));
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
    failure: LatchedQueryFailure,
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
        failure: query.first_failure.clone().unwrap_or_else(|| {
            LatchedQueryFailure::new(
                0,
                QueryFailureCause::ClientCancellation,
                "frontend query cancellation requested".to_string(),
            )
        }),
    }
}

fn dispatch_cancellation(cancellation: Option<CancellationDispatch>) {
    if let Some(cancellation) = cancellation
        && let Some(control) = cancellation.active_attempt
    {
        control.request_abort(cancellation.failure);
    }
}

fn query_key(query_id: QueryId) -> QueryKey {
    (query_id.high(), query_id.low())
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

    use novarocks_proto_codec::lifecycle::{
        AttemptId, ParticipantBackendIdentity, ParticipantTerminalOutcome, QueryControlEndpoint,
        QueryTerminalSnapshot, TerminalizationProof,
    };
    use novarocks_proto_models::{common, novarocks as proto};

    struct RetainedControl {
        execution_id: QueryExecutionId,
        reports: AtomicUsize,
    }

    impl ActiveQueryAttemptControl for RetainedControl {
        fn execution_id(&self) -> QueryExecutionId {
            self.execution_id
        }

        fn request_abort(&self, _failure: LatchedQueryFailure) {}

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
                runtime_filter: RuntimeFilterTerminalRollupSnapshot::Unavailable(
                    RuntimeFilterTerminalRollupUnavailable::TerminalOutcomesIncomplete,
                ),
                metrics: FrontendProcessQueryCountersSnapshot::default(),
            })
        }
    }

    fn terminal_outcome(execution_id: QueryExecutionId) -> ParticipantTerminalOutcome {
        let backend = ParticipantBackendIdentity::new(
            BackendProcessId::new_v7(),
            QueryControlEndpoint::new("127.0.0.1", 9030).expect("valid endpoint"),
        )
        .expect("valid backend identity")
        .as_proto()
        .clone();
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
        let participant = proto::ParticipantAttemptRef {
            execution_id: Some(novarocks_proto_codec::lifecycle::encode_query_execution_id(
                execution_id,
            )),
            backend_process_id: backend.process_id.clone(),
        };
        let snapshot = QueryTerminalSnapshot::parse(proto::QueryTerminalSnapshot {
            version: 1,
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
            participant: Some(participant.clone()),
        })
        .expect("terminal snapshot");
        let proof = TerminalizationProof::parse(proto::TerminalizationProof {
            version: 1,
            fragments: vec![proto::TerminalizationProofFragment {
                fragment_instance_id: Some(common::UniqueId { hi: 1, lo: 2 }),
                backend_num: 7,
                outcome: proto::QueryTerminalFragmentOutcome::Succeeded as i32,
                ..Default::default()
            }],
            participant: Some(participant),
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
        let registry = FrontendQueryRegistry::new(QueryProcessNamespace::new(41));
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

    /// The one convergence slot answers for whichever protocol ran the latest
    /// attempt.
    ///
    /// The defect this catches: a reader wired only to the retired lifecycle's
    /// retention. Every task-protocol query then leaves the endpoint reporting
    /// the last lifecycle attempt -- a real snapshot of the wrong query --
    /// which reads as stale data rather than as a missing producer.
    #[test]
    fn the_convergence_reader_answers_for_the_protocol_that_ran_the_latest_attempt() {
        let registry = FrontendQueryRegistry::new(QueryProcessNamespace::new(41));
        let lifecycle_execution =
            QueryExecutionId::new(QueryId::new(41, 42), AttemptId::new(1).expect("attempt"))
                .expect("execution id");
        let task_execution =
            QueryExecutionId::new(QueryId::new(41, 43), AttemptId::new(1).expect("attempt"))
                .expect("execution id");

        registry.retain_terminal_control(Arc::new(RetainedControl {
            execution_id: lifecycle_execution,
            reports: AtomicUsize::new(0),
        }));
        assert_eq!(
            QueryLifecycleConvergenceReader::latest_convergence_snapshot(&registry)
                .expect("a retained lifecycle attempt is readable")
                .execution_id,
            lifecycle_execution
        );

        registry.publish_task_round_convergence(QueryLifecycleConvergenceSnapshot {
            execution_id: task_execution,
            error_source: None,
            primary_error: None,
            participant_outcomes: Vec::new(),
            runtime_filter: RuntimeFilterTerminalRollupSnapshot::Unavailable(
                RuntimeFilterTerminalRollupUnavailable::TerminalOutcomesIncomplete,
            ),
            metrics: FrontendProcessQueryCountersSnapshot::default(),
        });
        let latest = QueryLifecycleConvergenceReader::latest_convergence_snapshot(&registry)
            .expect("a published task attempt is readable");
        assert_eq!(latest.execution_id, task_execution);
        assert!(
            latest.participant_outcomes.is_empty(),
            "the task protocol mints no participant terminal outcome, and none \
             may be invented for it"
        );

        // And back the other way: the slot is last-publication-wins, not
        // first-protocol-wins.
        registry.retain_terminal_control(Arc::new(RetainedControl {
            execution_id: lifecycle_execution,
            reports: AtomicUsize::new(0),
        }));
        assert_eq!(
            QueryLifecycleConvergenceReader::latest_convergence_snapshot(&registry)
                .expect("the retained lifecycle attempt is readable again")
                .execution_id,
            lifecycle_execution
        );
    }

    #[test]
    fn same_priority_failure_preserves_first_causal_report() {
        fn record_in_order(messages: &[&str]) -> (String, Vec<String>) {
            let registry = Arc::new(FrontendQueryRegistry::new(QueryProcessNamespace::new(71)));
            let query_id = QueryId::new(71, 72);
            registry
                .active
                .lock()
                .expect("frontend query registry lock")
                .insert(
                    query_key(query_id),
                    ActiveQuery {
                        scheduled_backends: BTreeSet::new(),
                        first_failure: None,
                        secondary_failures: BTreeSet::new(),
                        next_failure_id: 1,
                        cancellation_requested: false,
                        cancellation_dispatched: false,
                        active_attempt: None,
                    },
                );
            for message in messages {
                registry
                    .latch_failure_and_cancel(
                        query_id,
                        QueryFailureCause::FrontendExecution,
                        (*message).to_string(),
                    )
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
                query
                    .first_failure
                    .clone()
                    .expect("primary failure")
                    .message,
                query
                    .secondary_failures
                    .iter()
                    .map(|(_, message)| message.clone())
                    .collect(),
            )
        }

        let forward = record_in_order(&["zeta failure", "alpha failure", "middle failure"]);
        let reverse = record_in_order(&["middle failure", "alpha failure", "zeta failure"]);

        assert_eq!(forward.0, "zeta failure");
        assert_eq!(
            forward.1,
            vec!["alpha failure".to_string(), "middle failure".to_string()]
        );
        assert_eq!(reverse.0, "middle failure");
        assert_eq!(
            reverse.1,
            vec!["alpha failure".to_string(), "zeta failure".to_string()]
        );
    }

    #[test]
    fn concrete_failure_supersedes_lifecycle_observation_in_either_arrival_order() {
        fn primary_for_order(observation_first: bool) -> (QueryFailureCause, String) {
            let mut query = ActiveQuery {
                scheduled_backends: BTreeSet::new(),
                first_failure: None,
                secondary_failures: BTreeSet::new(),
                next_failure_id: 1,
                cancellation_requested: false,
                cancellation_dispatched: false,
                active_attempt: None,
            };
            let observation = || {
                (
                    QueryFailureCause::LifecycleHeartbeatTimeout,
                    "aaa heartbeat timeout".to_string(),
                )
            };
            let concrete = || {
                (
                    QueryFailureCause::BackendLocalFailure,
                    "zzz fragment scan failed".to_string(),
                )
            };
            let reports = if observation_first {
                [observation(), concrete()]
            } else {
                [concrete(), observation()]
            };
            for (cause, message) in reports {
                query.record_failure(cause, message);
            }
            let primary = query.first_failure.expect("primary failure");
            (primary.cause, primary.message)
        }

        let expected = (
            QueryFailureCause::BackendLocalFailure,
            "zzz fragment scan failed".to_string(),
        );
        assert_eq!(primary_for_order(true), expected);
        assert_eq!(primary_for_order(false), expected);
    }

    #[test]
    fn stale_abort_context_cannot_overwrite_upgraded_concrete_failure() {
        let registry = Arc::new(FrontendQueryRegistry::new(QueryProcessNamespace::new(73)));
        let query_id = QueryId::new(73, 74);
        registry
            .active
            .lock()
            .expect("frontend query registry lock")
            .insert(
                query_key(query_id),
                ActiveQuery {
                    scheduled_backends: BTreeSet::new(),
                    first_failure: None,
                    secondary_failures: BTreeSet::new(),
                    next_failure_id: 1,
                    cancellation_requested: false,
                    cancellation_dispatched: false,
                    active_attempt: None,
                },
            );

        let observation = registry
            .latch_failure_and_cancel(
                query_id,
                QueryFailureCause::LifecycleHeartbeatTimeout,
                "aaa heartbeat timeout",
            )
            .expect("latch lifecycle observation");
        let concrete = registry
            .latch_failure_and_cancel(
                query_id,
                QueryFailureCause::BackendLocalFailure,
                "zzz fragment scan failed",
            )
            .expect("latch concrete failure");
        registry
            .preserve_failure_context(
                query_id,
                observation.id(),
                "aaa heartbeat timeout; rollback failed".to_string(),
            )
            .expect("ignore stale lifecycle context");
        assert_eq!(
            registry.first_failure(query_id).as_deref(),
            Some("zzz fragment scan failed")
        );

        registry
            .preserve_failure_context(
                query_id,
                concrete.id(),
                "zzz fragment scan failed; rollback failed".to_string(),
            )
            .expect("enrich selected concrete failure");
        assert_eq!(
            registry.first_failure(query_id).as_deref(),
            Some("zzz fragment scan failed; rollback failed")
        );
    }

    #[test]
    fn inactive_query_diagnosis_distinguishes_local_and_foreign_processes() {
        let registry = FrontendQueryRegistry::new(QueryProcessNamespace::new(0x11));

        let local = registry.inactive_query(QueryId::new(0x11, 7));
        assert_eq!(local.kind(), DistributedQueryErrorKind::Rejected);
        assert!(
            local
                .message()
                .contains("frontend local query is not active")
        );
        assert!(
            local
                .message()
                .contains("namespace=0x0000000000000011 sequence=7")
        );

        let foreign = registry.inactive_query(QueryId::new(0x12, 8));
        assert_eq!(foreign.kind(), DistributedQueryErrorKind::Rejected);
        assert!(
            foreign
                .message()
                .contains("frontend query belongs to a foreign process")
        );
        assert!(
            foreign
                .message()
                .contains("local_namespace=0x0000000000000011")
        );
        assert!(
            foreign
                .message()
                .contains("namespace=0x0000000000000012 sequence=8")
        );
    }
}
