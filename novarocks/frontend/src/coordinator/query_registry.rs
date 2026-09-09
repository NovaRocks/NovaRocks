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
use std::sync::{Arc, Mutex};

use crate::common::backend_topology::LiveBackendTarget;
use crate::metrics::FrontendProcessQueryCountersSnapshot;
use crate::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, DistributedQueryIntent,
};
use crate::query_execution::runtime_filter_terminal_rollup::RuntimeFilterTerminalRollup;
use novarocks_proto_codec::lifecycle::QueryExecutionId;
use novarocks_query_application::coordination::{
    AttemptConvergenceFacts, AttemptDisposition, LogicalConclusion,
};
use novarocks_types::{BackendProcessId, QueryId, QueryProcessNamespace};

type QueryKey = (i64, i64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueryLifecycleConvergenceErrorSource {
    BackendAttestation,
    FrontendLiveness,
    NoOutcome,
}

/// Immutable, query-scoped terminal convergence evidence.  It is intentionally
/// produced by the attempt that owns the execution id, never reconstructed
/// from process metrics or logs.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct QueryLifecycleConvergenceSnapshot {
    pub(crate) execution_id: QueryExecutionId,
    pub(crate) error_source: Option<QueryLifecycleConvergenceErrorSource>,
    pub(crate) primary_error: Option<String>,
    /// Runtime Filter terminal facts are normalized only from a complete set
    /// of participant contributions.  The unavailable variant records why no
    /// such set existed for this attempt.
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
}

/// Read-only diagnostic seam for the immutable terminal evidence retained by
/// the query registry.  It deliberately has no access to attempt mutation.
pub(crate) trait QueryLifecycleConvergenceReader: Send + Sync {
    fn latest_convergence_snapshot(&self) -> Option<QueryLifecycleConvergenceSnapshot>;
}

/// Typed origin of a query failure retained by the FE query owner.
///
/// The variants deliberately describe evidence, not rendered text. This lets
/// the registry preserve a concrete query failure when transport supervision
/// subsequently observes a peer going away as a consequence of that failure.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum QueryFailureCause {
    RemoteTransportObservation,
    ClientCancellation,
    FrontendExecution,
    BackendLocalFailure,
}

impl QueryFailureCause {
    const fn priority(self) -> QueryFailurePriority {
        match self {
            Self::RemoteTransportObservation => QueryFailurePriority::LifecycleObservation,
            Self::ClientCancellation | Self::FrontendExecution | Self::BackendLocalFailure => {
                QueryFailurePriority::ConcreteCause
            }
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
}

#[derive(Default)]
struct AttemptState {
    /// Process identities, not durable membership ids. A `backend_idx` is a
    /// round-local scheduling ordinal and must never identify an active
    /// attempt after topology publication.
    scheduled_backends: Option<BTreeSet<BackendProcessId>>,
    /// A concrete cause supersedes a lifecycle observation. Within one
    /// priority class the first observed cause remains primary, preserving
    /// causal ordering without interpreting rendered error text.
    first_failure: Option<LatchedQueryFailure>,
    /// Keep every losing distinct failure for the later structured
    /// convergence snapshot instead of discarding it at the first latch.
    secondary_failures: BTreeSet<(QueryFailureCause, String)>,
    next_failure_id: u64,
    convergence: AttemptConvergenceFacts,
}

impl AttemptState {
    fn new() -> Self {
        Self {
            next_failure_id: 1,
            ..Self::default()
        }
    }

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

struct LogicalQuery {
    registration_generation: u64,
    /// Temporary state used only by the existing query-id-only registration
    /// API. Binding the initial attempt moves this state into the attempt map.
    unbound_attempt: AttemptState,
    legacy_registration_id: Option<u64>,
    current_attempt: Option<QueryExecutionId>,
    replacement: Option<PendingAttemptReplacement>,
    /// The attempt capability that fixed the logical conclusion.  Conclusion
    /// demotes that attempt from current immediately, but this exact owner is
    /// retained for idempotent conclusion and final registry retirement.
    terminal_attempt: Option<QueryExecutionId>,
    terminal_registration_generation: Option<u64>,
    attempts: BTreeMap<QueryExecutionId, AttemptState>,
    conclusion: Option<LogicalConclusion>,
}

impl LogicalQuery {
    fn legacy(registration_id: u64, registration_generation: u64) -> Self {
        Self {
            registration_generation,
            unbound_attempt: AttemptState::new(),
            legacy_registration_id: Some(registration_id),
            current_attempt: None,
            replacement: None,
            terminal_attempt: None,
            terminal_registration_generation: None,
            attempts: BTreeMap::new(),
            conclusion: None,
        }
    }

    fn with_initial_attempt(execution_id: QueryExecutionId, registration_generation: u64) -> Self {
        Self {
            registration_generation,
            unbound_attempt: AttemptState::new(),
            legacy_registration_id: None,
            current_attempt: Some(execution_id),
            replacement: None,
            terminal_attempt: None,
            terminal_registration_generation: None,
            attempts: BTreeMap::from([(execution_id, AttemptState::new())]),
            conclusion: None,
        }
    }

    fn current_state(&self) -> Option<&AttemptState> {
        self.current_attempt
            .or(self.terminal_attempt)
            .and_then(|execution_id| self.attempts.get(&execution_id))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PendingAttemptReplacement {
    failed: QueryExecutionId,
    replacement: QueryExecutionId,
    phase_generation: u64,
}

/// Process-lifetime non-reuse authority for locally minted query sequences.
/// Retired sequences are stored as merged inclusive intervals, so normal
/// contiguous allocation retains one small range rather than one tombstone per
/// query. Correctness requires these intervals to live until FE process exit:
/// the wire identity has no extra logical-registration generation.
#[derive(Default)]
struct RetiredQuerySequences {
    intervals: BTreeMap<u64, u64>,
}

impl RetiredQuerySequences {
    fn contains(&self, sequence: u64) -> bool {
        self.intervals
            .range(..=sequence)
            .next_back()
            .is_some_and(|(&start, &end)| start <= sequence && sequence <= end)
    }

    fn insert(&mut self, sequence: u64) {
        if self.contains(sequence) {
            return;
        }
        let mut start = sequence;
        let mut end = sequence;
        if let Some((&previous_start, &previous_end)) = self.intervals.range(..sequence).next_back()
            && previous_end.checked_add(1) == Some(sequence)
        {
            start = previous_start;
            self.intervals.remove(&previous_start);
        }
        if let Some((&next_start, &next_end)) = self.intervals.range(sequence..).next()
            && end.checked_add(1) == Some(next_start)
        {
            end = next_end;
            self.intervals.remove(&next_start);
        }
        self.intervals.insert(start, end);
    }
}

#[derive(Default)]
struct RegistryState {
    logical: BTreeMap<QueryKey, LogicalQuery>,
    /// Exact event-routing index. Attempt state remains owned by `logical` so
    /// both indexes are mutated under this one lock and never become competing
    /// authorities.
    attempt_owner: BTreeMap<QueryExecutionId, QueryKey>,
    next_legacy_registration_id: u64,
    next_logical_registration_generation: u64,
    next_replacement_phase_generation: u64,
    next_terminal_registration_generation: u64,
    retired_query_sequences: RetiredQuerySequences,
}

/// Opaque proof that an exact attempt was inserted into both registry indexes.
/// It has no `Drop` behavior: only explicit convergence retirement may remove
/// an attempt-aware logical execution.
#[derive(Debug)]
#[must_use = "the logical execution owner must retain the exact registered attempt"]
pub(crate) struct RegisteredAttempt {
    registry_instance_id: uuid::Uuid,
    logical_registration_generation: u64,
    query_key: QueryKey,
    execution_id: QueryExecutionId,
}

/// Opaque owner of the interval after the old attempt was atomically demoted
/// and before the proposed attempt is activated.
#[derive(Debug)]
#[must_use = "replacement qualification must activate or conclude the logical execution"]
pub(crate) struct RegisteredReplacement {
    registry_instance_id: uuid::Uuid,
    logical_registration_generation: u64,
    query_key: QueryKey,
    failed: QueryExecutionId,
    replacement: QueryExecutionId,
    phase_generation: u64,
}

/// Opaque owner of one fixed logical conclusion. It is independent of an
/// attempt because cancellation may win while replacement qualification owns
/// the logical execution and no attempt is current.
#[derive(Debug)]
#[must_use = "the terminal owner must retain the logical execution until every attempt retires"]
pub(crate) struct RegisteredLogicalTerminal {
    registry_instance_id: uuid::Uuid,
    logical_registration_generation: u64,
    query_key: QueryKey,
    terminal_registration_generation: u64,
}

impl RegisteredAttempt {
    pub(crate) const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }
}

/// Immutable result of routing an exact attempt-scoped event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AttemptRouteSnapshot {
    query_id: QueryId,
    execution_id: QueryExecutionId,
    disposition: AttemptDisposition,
    scheduled_backends: Option<BTreeSet<BackendProcessId>>,
    primary_failure: Option<LatchedQueryFailure>,
    convergence: AttemptConvergenceFacts,
}

impl AttemptRouteSnapshot {
    pub(crate) const fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub(crate) const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub(crate) const fn disposition(&self) -> AttemptDisposition {
        self.disposition
    }

    pub(crate) fn scheduled_backends(&self) -> Option<&BTreeSet<BackendProcessId>> {
        self.scheduled_backends.as_ref()
    }

    pub(crate) fn primary_failure(&self) -> Option<&LatchedQueryFailure> {
        self.primary_failure.as_ref()
    }

    pub(crate) const fn convergence(&self) -> AttemptConvergenceFacts {
        self.convergence
    }
}

#[derive(Default)]
struct BackendTopologyState {
    initialized: bool,
    revision: u64,
    live_process_ids: BTreeMap<usize, BackendProcessId>,
}

pub(crate) struct FrontendQueryRegistry {
    instance_id: uuid::Uuid,
    namespace: QueryProcessNamespace,
    state: Mutex<RegistryState>,
    /// The convergence evidence of the most recent attempt to finish.
    ///
    /// One slot, last publication wins. An execution id is owned by exactly
    /// one attempt, so the slot never becomes a place where a reader chooses
    /// between two candidates for the same attempt.
    ///
    /// The evidence is published frozen: the task protocol has no late
    /// ingress, so an attempt's contribution set is complete when its last
    /// query context releases.
    latest_convergence: Mutex<Option<Box<QueryLifecycleConvergenceSnapshot>>>,
    backend_topology: Mutex<BackendTopologyState>,
}

impl FrontendQueryRegistry {
    pub(crate) fn new(namespace: QueryProcessNamespace) -> Self {
        Self {
            instance_id: uuid::Uuid::new_v4(),
            namespace,
            state: Mutex::new(RegistryState {
                next_legacy_registration_id: 1,
                next_logical_registration_generation: 1,
                next_replacement_phase_generation: 1,
                next_terminal_registration_generation: 1,
                ..RegistryState::default()
            }),
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

    fn inactive_attempt(&self, execution_id: QueryExecutionId) -> DistributedQueryError {
        DistributedQueryError::new(
            DistributedQueryErrorKind::Rejected,
            format!(
                "frontend query attempt is not active ({execution_id:?}; {})",
                self.describe_query_id(execution_id.query_id())
            ),
        )
    }

    pub(crate) fn register(
        self: &Arc<Self>,
        query_id: QueryId,
        _intent: DistributedQueryIntent,
        _dispatcher: Arc<dyn crate::native::fragment_transport::FragmentDispatcher>,
    ) -> Result<ActiveQueryGuard, DistributedQueryError> {
        let key = query_key(query_id);
        let mut state = self.state.lock().expect("frontend query registry lock");
        let sequence = local_query_sequence(self.namespace, query_id)?;
        if state.retired_query_sequences.contains(sequence) {
            return Err(contract_violation(format!(
                "frontend query identity was already retired and cannot be reused ({})",
                self.describe_query_id(query_id)
            )));
        }
        if state.logical.contains_key(&key) {
            return Err(contract_violation(format!(
                "frontend query is already active ({})",
                self.describe_query_id(query_id)
            )));
        }
        let registration_id = state.next_legacy_registration_id;
        state.next_legacy_registration_id = state
            .next_legacy_registration_id
            .checked_add(1)
            .expect("frontend legacy query registration id exhausted");
        let logical_registration_generation = next_logical_registration_generation(&mut state)?;
        state.logical.insert(
            key,
            LogicalQuery::legacy(registration_id, logical_registration_generation),
        );
        Ok(ActiveQueryGuard {
            registry: Arc::clone(self),
            key,
            registration_id,
        })
    }

    /// Registers the first physical attempt of a logical execution.
    ///
    /// If the compatibility API registered the query first, its accumulated
    /// state is moved into this exact attempt. The compatibility guard is then
    /// disarmed structurally and cannot delete the attempt-aware entry.
    pub(crate) fn register_initial_attempt(
        &self,
        execution_id: QueryExecutionId,
    ) -> Result<RegisteredAttempt, DistributedQueryError> {
        let query_id = execution_id.query_id();
        if query_id
            .process_attribution()
            .is_none_or(|attribution| attribution.namespace() != self.namespace)
        {
            return Err(contract_violation(format!(
                "frontend query attempt does not belong to this registry ({execution_id:?}; local_namespace={})",
                self.namespace
            )));
        }
        let key = query_key(query_id);
        let mut state = self.state.lock().expect("frontend query registry lock");
        let sequence = local_query_sequence(self.namespace, query_id)?;
        if state.retired_query_sequences.contains(sequence) {
            return Err(contract_violation(format!(
                "frontend query execution identity was already retired and cannot be reused ({execution_id:?})"
            )));
        }
        if state.attempt_owner.contains_key(&execution_id) {
            return Err(contract_violation(format!(
                "frontend query attempt is already registered ({execution_id:?})"
            )));
        }

        let logical_registration_generation = match state.logical.get_mut(&key) {
            None => {
                let generation = next_logical_registration_generation(&mut state)?;
                state.logical.insert(
                    key,
                    LogicalQuery::with_initial_attempt(execution_id, generation),
                );
                generation
            }
            Some(logical) => {
                if logical.conclusion.is_some() {
                    return Err(contract_violation(
                        "cannot register an initial attempt after logical conclusion",
                    ));
                }
                if logical.current_attempt.is_some() || !logical.attempts.is_empty() {
                    return Err(contract_violation(format!(
                        "frontend logical query already has an initial attempt ({})",
                        self.describe_query_id(query_id)
                    )));
                }
                let initial_state =
                    std::mem::replace(&mut logical.unbound_attempt, AttemptState::new());
                logical.legacy_registration_id = None;
                logical.current_attempt = Some(execution_id);
                logical.attempts.insert(execution_id, initial_state);
                logical.registration_generation
            }
        };
        state.attempt_owner.insert(execution_id, key);
        Ok(RegisteredAttempt {
            registry_instance_id: self.instance_id,
            logical_registration_generation,
            query_key: key,
            execution_id,
        })
    }

    /// Atomically demotes the current attempt and installs the only owner of
    /// replacement qualification. No attempt is current until this exact phase
    /// capability activates the proposed identity.
    pub(crate) fn begin_attempt_replacement(
        &self,
        current: &RegisteredAttempt,
        replacement: QueryExecutionId,
    ) -> Result<RegisteredReplacement, DistributedQueryError> {
        if current.registry_instance_id != self.instance_id {
            return Err(contract_violation(
                "registered attempt belongs to a different query registry",
            ));
        }
        if replacement.query_id() != current.execution_id.query_id() {
            return Err(contract_violation(
                "replacement attempt belongs to a different logical query",
            ));
        }
        if replacement.attempt_id() <= current.execution_id.attempt_id() {
            return Err(contract_violation(
                "replacement attempt id must advance monotonically",
            ));
        }

        let mut state = self.state.lock().expect("frontend query registry lock");
        if state.attempt_owner.contains_key(&replacement) {
            return Err(contract_violation(format!(
                "replacement attempt is already registered ({replacement:?})"
            )));
        }
        let phase_generation = state.next_replacement_phase_generation;
        state.next_replacement_phase_generation = phase_generation
            .checked_add(1)
            .ok_or_else(|| contract_violation("frontend replacement phase generation exhausted"))?;
        let logical = state
            .logical
            .get_mut(&current.query_key)
            .ok_or_else(|| self.inactive_query(current.execution_id.query_id()))?;
        if logical.registration_generation != current.logical_registration_generation {
            return Err(contract_violation(
                "registered attempt belongs to a retired logical execution",
            ));
        }
        if logical.current_attempt != Some(current.execution_id) {
            return Err(contract_violation(
                "replacement token does not identify the current attempt",
            ));
        }
        if logical.conclusion.is_some() {
            return Err(contract_violation(
                "cannot replace an attempt after logical conclusion",
            ));
        }
        if logical.replacement.is_some() {
            return Err(contract_violation(
                "frontend logical query already has a replacement owner",
            ));
        }
        logical.current_attempt = None;
        logical.replacement = Some(PendingAttemptReplacement {
            failed: current.execution_id,
            replacement,
            phase_generation,
        });
        Ok(RegisteredReplacement {
            registry_instance_id: self.instance_id,
            logical_registration_generation: current.logical_registration_generation,
            query_key: current.query_key,
            failed: current.execution_id,
            replacement,
            phase_generation,
        })
    }

    /// Activates the proposed attempt only after the actor has completed every
    /// replacement prerequisite represented by its own sealed reducer token.
    pub(crate) fn activate_attempt_replacement(
        &self,
        replacement: &RegisteredReplacement,
    ) -> Result<RegisteredAttempt, DistributedQueryError> {
        let mut state = self.state.lock().expect("frontend query registry lock");
        let key = self.validate_registered_replacement(&state, replacement)?;
        if state.attempt_owner.contains_key(&replacement.replacement) {
            return Err(contract_violation(format!(
                "replacement attempt is already registered ({:?})",
                replacement.replacement
            )));
        }
        let logical = state
            .logical
            .get_mut(&key)
            .expect("frontend replacement owner points at a logical entry");
        logical.current_attempt = Some(replacement.replacement);
        logical.replacement = None;
        let previous = logical
            .attempts
            .insert(replacement.replacement, AttemptState::new());
        debug_assert!(previous.is_none());
        state
            .attempt_owner
            .insert(replacement.replacement, replacement.query_key);
        Ok(RegisteredAttempt {
            registry_instance_id: self.instance_id,
            logical_registration_generation: replacement.logical_registration_generation,
            query_key: replacement.query_key,
            execution_id: replacement.replacement,
        })
    }

    /// Resolves an exact attempt without consulting the current-attempt slot.
    /// Residual attempts therefore remain routable after replacement and after
    /// the logical result has been fixed.
    pub(crate) fn route_attempt(
        &self,
        execution_id: QueryExecutionId,
    ) -> Option<AttemptRouteSnapshot> {
        let state = self.state.lock().expect("frontend query registry lock");
        let key = *state.attempt_owner.get(&execution_id)?;
        let logical = state
            .logical
            .get(&key)
            .expect("frontend attempt owner points at a logical entry");
        let attempt = logical
            .attempts
            .get(&execution_id)
            .expect("frontend attempt owner points at attempt state");
        Some(AttemptRouteSnapshot {
            query_id: execution_id.query_id(),
            execution_id,
            disposition: if logical.current_attempt == Some(execution_id) {
                AttemptDisposition::Current
            } else {
                AttemptDisposition::Residual
            },
            scheduled_backends: attempt.scheduled_backends.clone(),
            primary_failure: attempt.first_failure.clone(),
            convergence: attempt.convergence,
        })
    }

    pub(crate) fn set_scheduled_backend_ownership(
        &self,
        query_id: QueryId,
        backend_ownership: &[(usize, BackendProcessId)],
    ) -> Result<(), DistributedQueryError> {
        self.validate_backend_ownership(backend_ownership)?;
        let mut state = self.state.lock().expect("frontend query registry lock");
        let query = state
            .logical
            .get_mut(&query_key(query_id))
            .ok_or_else(|| self.inactive_query(query_id))?;
        if !query.attempts.is_empty() {
            return Err(contract_violation(
                "query-id-only backend ownership cannot mutate an attempt-aware logical execution",
            ));
        }
        set_attempt_backend_ownership(&mut query.unbound_attempt, backend_ownership)
    }

    pub(crate) fn set_attempt_scheduled_backend_ownership(
        &self,
        registered: &RegisteredAttempt,
        backend_ownership: &[(usize, BackendProcessId)],
    ) -> Result<(), DistributedQueryError> {
        self.validate_backend_ownership(backend_ownership)?;
        let mut state = self.state.lock().expect("frontend query registry lock");
        let key = self.validate_registered_attempt(&state, registered)?;
        let logical = state
            .logical
            .get_mut(&key)
            .expect("frontend attempt owner points at a logical entry");
        let attempt = logical
            .attempts
            .get_mut(&registered.execution_id)
            .expect("frontend attempt owner points at attempt state");
        set_attempt_backend_ownership(attempt, backend_ownership)
    }

    fn validate_backend_ownership(
        &self,
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
            .state
            .lock()
            .expect("frontend query registry lock")
            .logical
            .contains_key(&query_key(query_id))
        {
            return Ok(());
        }
        Err(self.inactive_query(query_id))
    }

    pub(crate) fn first_failure(&self, query_id: QueryId) -> Option<String> {
        self.state
            .lock()
            .expect("frontend query registry lock")
            .logical
            .get(&query_key(query_id))
            .and_then(|query| {
                query
                    .current_state()
                    .unwrap_or(&query.unbound_attempt)
                    .first_failure
                    .as_ref()
                    .map(|failure| failure.message.clone())
            })
    }

    fn latest_retained_convergence_snapshot(&self) -> Option<QueryLifecycleConvergenceSnapshot> {
        self.latest_convergence
            .lock()
            .expect("frontend latest convergence evidence lock")
            .as_deref()
            .cloned()
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
            .expect("frontend latest convergence evidence lock") = Some(Box::new(snapshot));
    }

    /// Records why this query failed and returns the primary cause the
    /// registry selected.
    ///
    /// Standing the attempt's participants down is deliberately not done here:
    /// a task round aborts its own tasks, so this owner holds the causal record
    /// and nothing else.
    pub(crate) fn latch_failure(
        &self,
        query_id: QueryId,
        cause: QueryFailureCause,
        message: impl Into<String>,
    ) -> Result<LatchedQueryFailure, DistributedQueryError> {
        let mut state = self.state.lock().expect("frontend query registry lock");
        let query = state
            .logical
            .get_mut(&query_key(query_id))
            .ok_or_else(|| self.inactive_query(query_id))?;
        if !query.attempts.is_empty() {
            return Err(contract_violation(
                "query-id-only failure ingress cannot mutate an attempt-aware logical execution",
            ));
        }
        Ok(query.unbound_attempt.record_failure(cause, message.into()))
    }

    pub(crate) fn latch_attempt_failure(
        &self,
        registered: &RegisteredAttempt,
        cause: QueryFailureCause,
        message: impl Into<String>,
    ) -> Result<LatchedQueryFailure, DistributedQueryError> {
        let mut state = self.state.lock().expect("frontend query registry lock");
        let key = self.validate_registered_attempt(&state, registered)?;
        let logical = state
            .logical
            .get_mut(&key)
            .expect("frontend attempt owner points at a logical entry");
        let attempt = logical
            .attempts
            .get_mut(&registered.execution_id)
            .expect("frontend attempt owner points at attempt state");
        Ok(attempt.record_failure(cause, message.into()))
    }

    /// Monotonically merges exact convergence facts for either the current or
    /// a residual attempt. Logical conclusion does not close this ingress.
    pub(crate) fn merge_attempt_convergence(
        &self,
        registered: &RegisteredAttempt,
        facts: AttemptConvergenceFacts,
    ) -> Result<AttemptConvergenceFacts, DistributedQueryError> {
        let mut state = self.state.lock().expect("frontend query registry lock");
        let key = self.validate_registered_attempt(&state, registered)?;
        let logical = state
            .logical
            .get_mut(&key)
            .expect("frontend attempt owner points at a logical entry");
        let attempt = logical
            .attempts
            .get_mut(&registered.execution_id)
            .expect("frontend attempt owner points at attempt state");
        attempt.convergence.actual_stopped |= facts.actual_stopped;
        attempt.convergence.output_released |= facts.output_released;
        attempt.convergence.resources_converged |= facts.resources_converged;
        Ok(attempt.convergence)
    }

    /// Fixes the logical result once. Repeating the same conclusion is
    /// idempotent; a conflicting late conclusion is rejected.
    pub(crate) fn conclude_logical(
        &self,
        current: &RegisteredAttempt,
        conclusion: LogicalConclusion,
    ) -> Result<RegisteredLogicalTerminal, DistributedQueryError> {
        if current.registry_instance_id != self.instance_id {
            return Err(contract_violation(
                "registered attempt belongs to a different query registry",
            ));
        }
        let mut state = self.state.lock().expect("frontend query registry lock");
        let terminal_registration_generation = state.next_terminal_registration_generation;
        state.next_terminal_registration_generation = terminal_registration_generation
            .checked_add(1)
            .ok_or_else(|| {
                contract_violation("frontend terminal registration generation exhausted")
            })?;
        let logical = state
            .logical
            .get_mut(&current.query_key)
            .ok_or_else(|| self.inactive_query(current.execution_id.query_id()))?;
        if logical.registration_generation != current.logical_registration_generation {
            return Err(contract_violation(
                "registered attempt belongs to a retired logical execution",
            ));
        }
        match logical.conclusion {
            None if logical.current_attempt == Some(current.execution_id) => {
                logical.conclusion = Some(conclusion);
                logical.current_attempt = None;
                logical.terminal_attempt = Some(current.execution_id);
                logical.terminal_registration_generation = Some(terminal_registration_generation);
            }
            None => {
                return Err(contract_violation(
                    "logical conclusion requires the current registered attempt",
                ));
            }
            Some(existing)
                if existing == conclusion
                    && logical.terminal_attempt == Some(current.execution_id) =>
            {
                return Ok(RegisteredLogicalTerminal {
                    registry_instance_id: self.instance_id,
                    logical_registration_generation: current.logical_registration_generation,
                    query_key: current.query_key,
                    terminal_registration_generation: logical
                        .terminal_registration_generation
                        .expect("a conclusion has a terminal owner"),
                });
            }
            Some(existing) => {
                return Err(contract_violation(format!(
                    "frontend logical query conclusion is already fixed as {existing:?}"
                )));
            }
        }
        Ok(RegisteredLogicalTerminal {
            registry_instance_id: self.instance_id,
            logical_registration_generation: current.logical_registration_generation,
            query_key: current.query_key,
            terminal_registration_generation,
        })
    }

    /// Fixes a non-success conclusion while replacement qualification owns the
    /// logical execution. The failed attempt capability is residual here and
    /// cannot substitute for this exact phase capability.
    pub(crate) fn conclude_attempt_replacement(
        &self,
        replacement: &RegisteredReplacement,
        conclusion: LogicalConclusion,
    ) -> Result<RegisteredLogicalTerminal, DistributedQueryError> {
        if matches!(conclusion, LogicalConclusion::Succeeded) {
            return Err(contract_violation(
                "replacement qualification cannot conclude logical success",
            ));
        }
        let mut state = self.state.lock().expect("frontend query registry lock");
        let key = self.validate_registered_replacement(&state, replacement)?;
        let terminal_registration_generation = state.next_terminal_registration_generation;
        state.next_terminal_registration_generation = terminal_registration_generation
            .checked_add(1)
            .ok_or_else(|| {
                contract_violation("frontend terminal registration generation exhausted")
            })?;
        let logical = state
            .logical
            .get_mut(&key)
            .expect("frontend replacement owner points at a logical entry");
        logical.conclusion = Some(conclusion);
        logical.replacement = None;
        logical.terminal_attempt = None;
        logical.terminal_registration_generation = Some(terminal_registration_generation);
        Ok(RegisteredLogicalTerminal {
            registry_instance_id: self.instance_id,
            logical_registration_generation: replacement.logical_registration_generation,
            query_key: replacement.query_key,
            terminal_registration_generation,
        })
    }

    /// Removes a concluded logical execution only after every exact attempt is
    /// fully retired. Returns `false` while any current or residual attempt
    /// still owns runtime, output, or accounted resources.
    pub(crate) fn try_remove_converged(
        &self,
        terminal: &RegisteredLogicalTerminal,
    ) -> Result<bool, DistributedQueryError> {
        if terminal.registry_instance_id != self.instance_id {
            return Err(contract_violation(
                "registered attempt belongs to a different query registry",
            ));
        }
        let key = terminal.query_key;
        let mut state = self.state.lock().expect("frontend query registry lock");
        let logical = state
            .logical
            .get(&key)
            .ok_or_else(|| contract_violation("frontend logical terminal is not active"))?;
        if logical.registration_generation != terminal.logical_registration_generation {
            return Err(contract_violation(
                "registered attempt belongs to a retired logical execution",
            ));
        }
        if logical.terminal_registration_generation
            != Some(terminal.terminal_registration_generation)
        {
            return Err(contract_violation(
                "logical retirement requires the exact terminal owner",
            ));
        }
        if logical.conclusion.is_none()
            || logical.attempts.is_empty()
            || logical
                .attempts
                .values()
                .any(|attempt| !attempt.convergence.retired())
        {
            return Ok(false);
        }
        let execution_ids = logical.attempts.keys().copied().collect::<Vec<_>>();
        let query_id = execution_ids
            .first()
            .map(|execution| execution.query_id())
            .ok_or_else(|| contract_violation("concluded logical execution has no attempts"))?;
        let sequence = local_query_sequence(self.namespace, query_id)?;
        state.logical.remove(&key);
        for execution_id in execution_ids {
            let removed = state.attempt_owner.remove(&execution_id);
            debug_assert_eq!(removed, Some(key));
        }
        state.retired_query_sequences.insert(sequence);
        Ok(true)
    }

    fn validate_registered_attempt(
        &self,
        state: &RegistryState,
        registered: &RegisteredAttempt,
    ) -> Result<QueryKey, DistributedQueryError> {
        if registered.registry_instance_id != self.instance_id {
            return Err(contract_violation(
                "registered attempt belongs to a different query registry",
            ));
        }
        let key = *state
            .attempt_owner
            .get(&registered.execution_id)
            .ok_or_else(|| self.inactive_attempt(registered.execution_id))?;
        if key != registered.query_key {
            return Err(contract_violation(
                "registered attempt owner does not match its logical query",
            ));
        }
        let logical = state
            .logical
            .get(&key)
            .expect("frontend attempt owner points at a logical entry");
        if logical.registration_generation != registered.logical_registration_generation {
            return Err(contract_violation(
                "registered attempt belongs to a retired logical execution",
            ));
        }
        if !logical.attempts.contains_key(&registered.execution_id) {
            return Err(contract_violation(
                "registered attempt is absent from its logical execution",
            ));
        }
        Ok(key)
    }

    fn validate_registered_replacement(
        &self,
        state: &RegistryState,
        registered: &RegisteredReplacement,
    ) -> Result<QueryKey, DistributedQueryError> {
        if registered.registry_instance_id != self.instance_id {
            return Err(contract_violation(
                "registered replacement belongs to a different query registry",
            ));
        }
        let logical = state
            .logical
            .get(&registered.query_key)
            .ok_or_else(|| contract_violation("frontend replacement owner is not active"))?;
        if logical.registration_generation != registered.logical_registration_generation {
            return Err(contract_violation(
                "registered replacement belongs to a retired logical execution",
            ));
        }
        let expected = PendingAttemptReplacement {
            failed: registered.failed,
            replacement: registered.replacement,
            phase_generation: registered.phase_generation,
        };
        if logical.replacement != Some(expected) || logical.current_attempt.is_some() {
            return Err(contract_violation(
                "replacement capability does not own the current logical phase",
            ));
        }
        Ok(registered.query_key)
    }

    fn unregister_legacy(&self, key: QueryKey, registration_id: u64) {
        let mut state = self.state.lock().expect("frontend query registry lock");
        let should_remove = state.logical.get(&key).is_some_and(|logical| {
            logical.legacy_registration_id == Some(registration_id)
                && logical.current_attempt.is_none()
                && logical.attempts.is_empty()
        });
        if should_remove {
            state.logical.remove(&key);
        }
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
    registration_id: u64,
}

impl Drop for ActiveQueryGuard {
    fn drop(&mut self) {
        self.registry
            .unregister_legacy(self.key, self.registration_id);
    }
}

fn set_attempt_backend_ownership(
    attempt: &mut AttemptState,
    backend_ownership: &[(usize, BackendProcessId)],
) -> Result<(), DistributedQueryError> {
    if attempt.scheduled_backends.is_some() {
        return Err(contract_violation(
            "frontend query scheduled backend ownership is already registered",
        ));
    }
    let mut scheduled_backends = BTreeSet::new();
    for &(_, process_id) in backend_ownership {
        if !scheduled_backends.insert(process_id) {
            return Err(contract_violation(
                "frontend query scheduled backend ownership contains duplicate backend process identities",
            ));
        }
    }
    attempt.scheduled_backends = Some(scheduled_backends);
    Ok(())
}

fn next_logical_registration_generation(
    state: &mut RegistryState,
) -> Result<u64, DistributedQueryError> {
    let generation = state.next_logical_registration_generation;
    state.next_logical_registration_generation = generation
        .checked_add(1)
        .ok_or_else(|| contract_violation("frontend logical registration generation exhausted"))?;
    Ok(generation)
}

fn query_key(query_id: QueryId) -> QueryKey {
    (query_id.high(), query_id.low())
}

fn local_query_sequence(
    namespace: QueryProcessNamespace,
    query_id: QueryId,
) -> Result<u64, DistributedQueryError> {
    let attribution = query_id
        .process_attribution()
        .ok_or_else(|| contract_violation("frontend query id has no valid process attribution"))?;
    if attribution.namespace() != namespace {
        return Err(contract_violation(
            "frontend query id belongs to a different process namespace",
        ));
    }
    Ok(attribution.sequence().get())
}

fn contract_violation(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    use novarocks_proto_codec::lifecycle::AttemptId;

    fn execution(query_id: QueryId, attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(query_id, AttemptId::new(attempt).expect("attempt id"))
            .expect("query execution id")
    }

    /// The one convergence slot answers with the evidence the latest attempt
    /// published.
    ///
    /// The defect this catches: a reader wired to a producer that no longer
    /// exists. Every query then leaves the endpoint reporting nothing, which
    /// reads as "this attempt produced no evidence" rather than as a missing
    /// publisher.
    #[test]
    fn the_convergence_reader_answers_with_the_latest_published_attempt() {
        let registry = FrontendQueryRegistry::new(QueryProcessNamespace::new(41));
        let first =
            QueryExecutionId::new(QueryId::new(41, 42), AttemptId::new(1).expect("attempt"))
                .expect("execution id");
        let second =
            QueryExecutionId::new(QueryId::new(41, 43), AttemptId::new(1).expect("attempt"))
                .expect("execution id");

        assert!(
            QueryLifecycleConvergenceReader::latest_convergence_snapshot(&registry).is_none(),
            "a registry that has published nothing reports no evidence"
        );

        for execution_id in [first, second] {
            registry.publish_task_round_convergence(QueryLifecycleConvergenceSnapshot {
                execution_id,
                error_source: None,
                primary_error: None,
                runtime_filter: RuntimeFilterTerminalRollupSnapshot::Unavailable(
                    RuntimeFilterTerminalRollupUnavailable::TerminalOutcomesIncomplete,
                ),
                metrics: FrontendProcessQueryCountersSnapshot::default(),
            });
        }

        let latest = QueryLifecycleConvergenceReader::latest_convergence_snapshot(&registry)
            .expect("a published attempt is readable");
        assert_eq!(latest.execution_id, second);
        assert!(
            latest.primary_error.is_none(),
            "a published attempt reached this point already linearized as a success"
        );
    }

    #[test]
    fn same_priority_failure_preserves_first_causal_report() {
        fn record_in_order(messages: &[&str]) -> (String, Vec<String>) {
            let mut attempt = AttemptState::new();
            for message in messages {
                attempt
                    .record_failure(QueryFailureCause::FrontendExecution, (*message).to_string());
            }
            (
                attempt
                    .first_failure
                    .clone()
                    .expect("primary failure")
                    .message,
                attempt
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
    fn concrete_failure_supersedes_a_transport_observation_in_either_arrival_order() {
        fn primary_for_order(observation_first: bool) -> (QueryFailureCause, String) {
            let mut query = AttemptState::new();
            let observation = || {
                (
                    QueryFailureCause::RemoteTransportObservation,
                    "aaa control transport closed".to_string(),
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

    #[test]
    fn replacement_atomically_switches_current_and_keeps_exact_residual_routing() {
        let registry = FrontendQueryRegistry::new(QueryProcessNamespace::new(0x21));
        let query_id = QueryId::new(0x21, 1);
        let first = execution(query_id, 1);
        let second = execution(query_id, 2);

        let first_registration = registry
            .register_initial_attempt(first)
            .expect("register first attempt");
        assert_eq!(first_registration.execution_id(), first);
        assert_eq!(
            registry
                .route_attempt(first)
                .expect("first route")
                .disposition(),
            AttemptDisposition::Current
        );

        let replacement = registry
            .begin_attempt_replacement(&first_registration, second)
            .expect("begin replacement");
        assert!(registry.route_attempt(second).is_none());
        let second_registration = registry
            .activate_attempt_replacement(&replacement)
            .expect("activate replacement");
        assert_eq!(second_registration.execution_id(), second);
        assert_eq!(
            registry
                .route_attempt(first)
                .expect("residual route")
                .disposition(),
            AttemptDisposition::Residual
        );
        let current = registry.route_attempt(second).expect("current route");
        assert_eq!(current.query_id(), query_id);
        assert_eq!(current.execution_id(), second);
        assert_eq!(current.disposition(), AttemptDisposition::Current);

        assert!(
            registry
                .begin_attempt_replacement(&first_registration, execution(query_id, 3))
                .is_err(),
            "a stale current token cannot replace its successor"
        );
        assert!(
            registry
                .begin_attempt_replacement(&second_registration, first)
                .is_err(),
            "a replacement cannot reuse or move behind an earlier attempt id"
        );
    }

    #[test]
    fn backend_failure_and_convergence_facts_are_isolated_per_attempt() {
        let registry = FrontendQueryRegistry::new(QueryProcessNamespace::new(0x22));
        let query_id = QueryId::new(0x22, 1);
        let first = execution(query_id, 1);
        let second = execution(query_id, 2);
        let first_registration = registry
            .register_initial_attempt(first)
            .expect("register first attempt");
        let replacement = registry
            .begin_attempt_replacement(&first_registration, second)
            .expect("begin replacement");
        let _second_registration = registry
            .activate_attempt_replacement(&replacement)
            .expect("activate replacement");
        let first_backend = BackendProcessId::new_v7();
        let second_backend = BackendProcessId::new_v7();
        registry
            .set_attempt_scheduled_backend_ownership(&first_registration, &[(0, first_backend)])
            .expect("record residual backend");
        registry
            .set_attempt_scheduled_backend_ownership(&_second_registration, &[(1, second_backend)])
            .expect("record current backend");
        registry
            .latch_attempt_failure(
                &first_registration,
                QueryFailureCause::RemoteTransportObservation,
                "old backend unreachable",
            )
            .expect("record residual failure");
        registry
            .merge_attempt_convergence(
                &first_registration,
                AttemptConvergenceFacts {
                    actual_stopped: true,
                    ..AttemptConvergenceFacts::default()
                },
            )
            .expect("record residual convergence");

        let residual = registry.route_attempt(first).expect("residual route");
        assert_eq!(
            residual.scheduled_backends(),
            Some(&BTreeSet::from([first_backend]))
        );
        assert_eq!(
            residual
                .primary_failure()
                .expect("residual failure")
                .message(),
            "old backend unreachable"
        );
        assert!(residual.convergence().actual_stopped);

        let current = registry.route_attempt(second).expect("current route");
        assert_eq!(
            current.scheduled_backends(),
            Some(&BTreeSet::from([second_backend]))
        );
        assert!(current.primary_failure().is_none());
        assert_eq!(current.convergence(), AttemptConvergenceFacts::default());
    }

    #[test]
    fn logical_conclusion_is_first_wins_while_residual_convergence_continues() {
        let registry = FrontendQueryRegistry::new(QueryProcessNamespace::new(0x23));
        let query_id = QueryId::new(0x23, 1);
        let first = execution(query_id, 1);
        let second = execution(query_id, 2);
        let first_registration = registry
            .register_initial_attempt(first)
            .expect("register first attempt");
        let replacement = registry
            .begin_attempt_replacement(&first_registration, second)
            .expect("begin replacement");
        let second_registration = registry
            .activate_attempt_replacement(&replacement)
            .expect("activate replacement");

        assert!(
            registry
                .conclude_logical(&first_registration, LogicalConclusion::Failed)
                .is_err(),
            "a residual attempt cannot fix the logical conclusion"
        );
        let terminal = registry
            .conclude_logical(&second_registration, LogicalConclusion::Succeeded)
            .expect("fix logical conclusion");
        assert_eq!(
            registry
                .route_attempt(second)
                .expect("terminal attempt remains routable")
                .disposition(),
            AttemptDisposition::Residual,
            "fixing the logical result must demote the final attempt"
        );
        assert!(
            registry
                .latch_failure(
                    query_id,
                    QueryFailureCause::FrontendExecution,
                    "late query-id-only failure",
                )
                .is_err(),
            "demotion must not reopen the legacy query-id-only mutation path"
        );
        let idempotent_terminal = registry
            .conclude_logical(&second_registration, LogicalConclusion::Succeeded)
            .expect("same conclusion is idempotent");
        assert_eq!(
            terminal.terminal_registration_generation,
            idempotent_terminal.terminal_registration_generation
        );
        assert!(
            registry
                .conclude_logical(&second_registration, LogicalConclusion::Failed)
                .is_err(),
            "a late outcome cannot replace the stable logical result"
        );

        let retired = AttemptConvergenceFacts {
            actual_stopped: true,
            output_released: true,
            resources_converged: true,
        };
        registry
            .merge_attempt_convergence(&second_registration, retired)
            .expect("retire current attempt");
        assert!(!registry.try_remove_converged(&terminal).expect("retained"));

        registry
            .merge_attempt_convergence(
                &first_registration,
                AttemptConvergenceFacts {
                    actual_stopped: true,
                    ..AttemptConvergenceFacts::default()
                },
            )
            .expect("late residual stop fact");
        assert!(!registry.try_remove_converged(&terminal).expect("retained"));
        registry
            .merge_attempt_convergence(
                &first_registration,
                AttemptConvergenceFacts {
                    output_released: true,
                    resources_converged: true,
                    ..AttemptConvergenceFacts::default()
                },
            )
            .expect("late residual release facts");
        assert!(registry.try_remove_converged(&terminal).expect("removed"));
        assert!(registry.route_attempt(first).is_none());
        assert!(registry.route_attempt(second).is_none());
    }

    #[test]
    fn replacement_phase_owns_cancellation_and_old_attempt_only_converges() {
        let registry = FrontendQueryRegistry::new(QueryProcessNamespace::new(0x2a));
        let query_id = QueryId::new(0x2a, 1);
        let first = execution(query_id, 1);
        let proposed = execution(query_id, 2);
        let first_registration = registry
            .register_initial_attempt(first)
            .expect("register initial attempt");
        let replacement = registry
            .begin_attempt_replacement(&first_registration, proposed)
            .expect("begin replacement qualification");

        assert_eq!(
            registry
                .route_attempt(first)
                .expect("old attempt remains routable")
                .disposition(),
            AttemptDisposition::Residual
        );
        assert!(registry.route_attempt(proposed).is_none());
        assert!(
            registry
                .conclude_logical(&first_registration, LogicalConclusion::Cancelled)
                .is_err(),
            "a demoted attempt cannot conclude the replacement phase"
        );

        let terminal = registry
            .conclude_attempt_replacement(&replacement, LogicalConclusion::Cancelled)
            .expect("replacement owner fixes cancellation");
        assert!(registry.activate_attempt_replacement(&replacement).is_err());
        registry
            .merge_attempt_convergence(
                &first_registration,
                AttemptConvergenceFacts {
                    actual_stopped: true,
                    output_released: true,
                    resources_converged: true,
                },
            )
            .expect("residual attempt converges after cancellation");
        assert!(
            registry
                .try_remove_converged(&terminal)
                .expect("remove concluded replacement phase")
        );
        assert!(registry.route_attempt(first).is_none());
        assert!(registry.route_attempt(proposed).is_none());
    }

    #[test]
    fn retired_query_sequence_intervals_merge_without_per_query_tombstones() {
        let mut retired = RetiredQuerySequences::default();
        retired.insert(3);
        retired.insert(1);
        assert_eq!(retired.intervals, BTreeMap::from([(1, 1), (3, 3)]));
        retired.insert(2);
        assert_eq!(retired.intervals, BTreeMap::from([(1, 3)]));
        for sequence in 1..=3 {
            assert!(retired.contains(sequence));
        }
        assert!(!retired.contains(4));
    }

    #[test]
    fn compatibility_guard_cannot_remove_an_attempt_aware_entry() {
        let registry = Arc::new(FrontendQueryRegistry::new(QueryProcessNamespace::new(0x24)));
        let query_id = QueryId::new(0x24, 1);
        let registration_id = 7;
        {
            let mut state = registry.state.lock().expect("frontend query registry lock");
            state.logical.insert(
                query_key(query_id),
                LogicalQuery::legacy(registration_id, 1),
            );
            state.next_logical_registration_generation = 2;
        }
        let guard = ActiveQueryGuard {
            registry: Arc::clone(&registry),
            key: query_key(query_id),
            registration_id,
        };
        let execution_id = execution(query_id, 1);
        let _attempt_registration = registry
            .register_initial_attempt(execution_id)
            .expect("upgrade compatibility entry");

        assert!(
            registry
                .latch_failure(
                    query_id,
                    QueryFailureCause::FrontendExecution,
                    "ambiguous query-id-only event",
                )
                .is_err(),
            "attempt-aware mutation requires an exact execution id"
        );

        drop(guard);

        assert!(
            registry.route_attempt(execution_id).is_some(),
            "legacy guard drop must not delete the logical owner or attempt index"
        );
    }

    #[test]
    fn registered_attempt_is_bound_to_its_registry_instance() {
        let namespace = QueryProcessNamespace::new(0x25);
        let first_registry = FrontendQueryRegistry::new(namespace);
        let second_registry = FrontendQueryRegistry::new(namespace);
        let query_id = QueryId::new(0x25, 1);
        let initial = execution(query_id, 1);
        let first_registration = first_registry
            .register_initial_attempt(initial)
            .expect("register first registry attempt");
        let _second_registration = second_registry
            .register_initial_attempt(initial)
            .expect("register second registry attempt");

        assert!(
            second_registry
                .begin_attempt_replacement(&first_registration, execution(query_id, 2))
                .is_err(),
            "a token minted by another registry must not authorize replacement"
        );
    }

    #[test]
    fn retired_logical_registration_cannot_reuse_an_old_attempt_token() {
        let registry = FrontendQueryRegistry::new(QueryProcessNamespace::new(0x26));
        let query_id = QueryId::new(0x26, 1);
        let initial = execution(query_id, 1);
        let retired_registration = registry
            .register_initial_attempt(initial)
            .expect("register initial attempt");
        let terminal = registry
            .conclude_logical(&retired_registration, LogicalConclusion::Succeeded)
            .expect("conclude first logical registration");
        registry
            .merge_attempt_convergence(
                &retired_registration,
                AttemptConvergenceFacts {
                    actual_stopped: true,
                    output_released: true,
                    resources_converged: true,
                },
            )
            .expect("converge first logical registration");
        assert!(
            registry
                .try_remove_converged(&terminal)
                .expect("retire first")
        );

        assert!(
            registry.register_initial_attempt(initial).is_err(),
            "an exact wire execution identity must not be reused after retirement"
        );
        assert!(
            registry
                .latch_attempt_failure(
                    &retired_registration,
                    QueryFailureCause::FrontendExecution,
                    "late old-generation failure",
                )
                .is_err(),
            "an old-generation capability cannot mutate the replacement generation"
        );
        assert!(
            registry
                .merge_attempt_convergence(
                    &retired_registration,
                    AttemptConvergenceFacts {
                        actual_stopped: true,
                        output_released: true,
                        resources_converged: true,
                    },
                )
                .is_err(),
            "an old-generation capability cannot retire the replacement generation"
        );
        assert!(
            registry
                .begin_attempt_replacement(&retired_registration, execution(query_id, 2))
                .is_err(),
            "a token from a retired logical generation must remain invalid"
        );
        assert!(
            registry.try_remove_converged(&terminal).is_err(),
            "a stale logical owner cannot retire its replacement generation"
        );
    }

    #[test]
    fn empty_backend_ownership_is_still_sealed_one_shot() {
        let registry = FrontendQueryRegistry::new(QueryProcessNamespace::new(0x27));
        let query_id = QueryId::new(0x27, 1);
        let execution_id = execution(query_id, 1);
        let _registration = registry
            .register_initial_attempt(execution_id)
            .expect("register attempt");

        registry
            .set_attempt_scheduled_backend_ownership(&_registration, &[])
            .expect("seal empty ownership");
        assert_eq!(
            registry
                .route_attempt(execution_id)
                .expect("route sealed attempt")
                .scheduled_backends(),
            Some(&BTreeSet::new())
        );
        assert!(
            registry
                .set_attempt_scheduled_backend_ownership(
                    &_registration,
                    &[(0, BackendProcessId::new_v7())],
                )
                .is_err(),
            "a sealed empty ownership set cannot be replaced"
        );
    }

    #[test]
    fn initial_attempt_rejects_a_foreign_query_namespace() {
        let registry = FrontendQueryRegistry::new(QueryProcessNamespace::new(0x28));
        let foreign = execution(QueryId::new(0x29, 1), 1);

        assert!(registry.register_initial_attempt(foreign).is_err());
        assert!(registry.route_attempt(foreign).is_none());
    }
}
