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
use novarocks_execution_contract::task_execution::context_convergence::{
    QueryContextConvergenceReceipt, QueryContextConvergenceState,
};
use novarocks_execution_contract::task_execution::identity::QueryContextRef;
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
    /// Exact contexts frozen for this attempt, keyed by the scheduling
    /// ordinal that selected their backend process. The ordinal is retained
    /// only to qualify an exact process-replacement observation; it never
    /// identifies a context without the frozen process identity.
    scheduled_contexts: Option<BTreeMap<usize, QueryContextRef>>,
    /// Compatibility-only process ownership used before an exact attempt is
    /// registered. It cannot authorize context convergence facts.
    legacy_scheduled_backends: Option<BTreeMap<usize, BackendProcessId>>,
    /// A concrete cause supersedes a lifecycle observation. Within one
    /// priority class the first observed cause remains primary, preserving
    /// causal ordering without interpreting rendered error text.
    first_failure: Option<LatchedQueryFailure>,
    /// Keep every losing distinct failure for the later structured
    /// convergence snapshot instead of discarding it at the first latch.
    secondary_failures: BTreeSet<(QueryFailureCause, String)>,
    next_failure_id: u64,
    convergence: AttemptConvergenceFacts,
    /// Last versioned resource observation and positive lifecycle evidence for
    /// every frozen participant. A process replacement deliberately marks the
    /// old participant unknown; it never clears its last observed usage or
    /// claims that its local execution stopped.
    backend_responsibilities: BTreeMap<BackendProcessId, AttemptBackendResponsibilitySnapshot>,
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
    scheduled_contexts: Option<BTreeMap<usize, QueryContextRef>>,
    primary_failure: Option<LatchedQueryFailure>,
    convergence: AttemptConvergenceFacts,
    backend_responsibilities: BTreeMap<BackendProcessId, AttemptBackendResponsibilitySnapshot>,
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

    pub(crate) fn scheduled_contexts(&self) -> Option<&BTreeMap<usize, QueryContextRef>> {
        self.scheduled_contexts.as_ref()
    }

    pub(crate) fn primary_failure(&self) -> Option<&LatchedQueryFailure> {
        self.primary_failure.as_ref()
    }

    pub(crate) const fn convergence(&self) -> AttemptConvergenceFacts {
        self.convergence
    }

    pub(crate) fn backend_responsibilities(
        &self,
    ) -> &BTreeMap<BackendProcessId, AttemptBackendResponsibilitySnapshot> {
        &self.backend_responsibilities
    }
}

/// The latest resource observation retained for one exact backend process in
/// an attempt. Versions belong to that backend's attempt-scoped observation
/// stream, so stale samples cannot turn an unknown residual back into known
/// usage or replace a newer byte count.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AttemptResourceObservation {
    pub(crate) version: u64,
    pub(crate) last_known_usage_bytes: u64,
}

/// Positive evidence that the old execution cannot continue on one backend.
/// A Worker publication and an independently trusted fence remain distinct so
/// callers cannot manufacture `actual_stopped` from transport loss or an abort
/// acknowledgement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AttemptStopEvidence {
    TrustedExecutionFence { version: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AttemptProcessReplacementEvidence {
    pub(crate) topology_revision: u64,
    pub(crate) replacement: BackendProcessId,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct AttemptBackendResponsibilitySnapshot {
    pub(crate) resource: Option<AttemptResourceObservation>,
    pub(crate) current_unknown: bool,
    pub(crate) context_convergence: Option<QueryContextConvergenceReceipt>,
    pub(crate) trusted_fence_version: Option<u64>,
    pub(crate) process_replacement: Option<AttemptProcessReplacementEvidence>,
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
        self.register_initial_attempt_inner(execution_id, None)
    }

    /// Registers the first attempt and freezes its exact Worker contexts in
    /// the same registry transaction.
    pub(crate) fn register_initial_attempt_with_contexts(
        &self,
        execution_id: QueryExecutionId,
        contexts: &[(usize, QueryContextRef)],
    ) -> Result<RegisteredAttempt, DistributedQueryError> {
        let frozen = self.validate_query_contexts(execution_id, contexts)?;
        self.register_initial_attempt_inner(execution_id, Some(frozen))
    }

    fn register_initial_attempt_inner(
        &self,
        execution_id: QueryExecutionId,
        mut frozen_contexts: Option<BTreeMap<usize, QueryContextRef>>,
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
                let mut logical = LogicalQuery::with_initial_attempt(execution_id, generation);
                if let Some(contexts) = frozen_contexts.take() {
                    let attempt = logical
                        .attempts
                        .get_mut(&execution_id)
                        .expect("new logical query contains its initial attempt");
                    set_attempt_query_contexts(attempt, contexts)?;
                }
                state.logical.insert(key, logical);
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
                if let Some(contexts) = frozen_contexts.as_ref() {
                    let context_backends = contexts
                        .iter()
                        .map(|(&ordinal, context)| (ordinal, context.backend_process_id()))
                        .collect::<BTreeMap<_, _>>();
                    if logical
                        .unbound_attempt
                        .legacy_scheduled_backends
                        .as_ref()
                        .is_some_and(|legacy| legacy != &context_backends)
                    {
                        return Err(contract_violation(
                            "exact query contexts conflict with compatibility backend ownership",
                        ));
                    }
                }
                let initial_state =
                    std::mem::replace(&mut logical.unbound_attempt, AttemptState::new());
                let mut initial_state = initial_state;
                if let Some(contexts) = frozen_contexts.take() {
                    set_attempt_query_contexts(&mut initial_state, contexts)?;
                }
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
            scheduled_contexts: attempt.scheduled_contexts.clone(),
            primary_failure: attempt.first_failure.clone(),
            convergence: attempt.convergence,
            backend_responsibilities: attempt.backend_responsibilities.clone(),
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

    pub(crate) fn set_attempt_query_contexts(
        &self,
        registered: &RegisteredAttempt,
        contexts: &[(usize, QueryContextRef)],
    ) -> Result<(), DistributedQueryError> {
        let frozen = self.validate_query_contexts(registered.execution_id, contexts)?;
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
        set_attempt_query_contexts(attempt, frozen)
    }

    /// Records one exact, versioned Worker convergence publication.
    ///
    /// Only a context frozen into this attempt can advance the retained fact.
    /// Task terminal states and Abort acknowledgements do not carry this type
    /// and therefore cannot enter this API.
    pub(crate) fn record_query_context_convergence(
        &self,
        registered: &RegisteredAttempt,
        receipt: QueryContextConvergenceReceipt,
    ) -> Result<AttemptBackendResponsibilitySnapshot, DistributedQueryError> {
        if receipt.context().query_execution_id() != registered.execution_id {
            return Err(contract_violation(format!(
                "query context convergence belongs to a different attempt (expected={:?} actual={:?})",
                registered.execution_id,
                receipt.context().query_execution_id()
            )));
        }
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
        let contexts = attempt.scheduled_contexts.as_ref().ok_or_else(|| {
            contract_violation(
                "query context convergence cannot be recorded before exact contexts are frozen",
            )
        })?;
        let expected = contexts
            .values()
            .find(|context| context.backend_process_id() == receipt.context().backend_process_id())
            .copied()
            .ok_or_else(|| {
                contract_violation(format!(
                    "backend process {} is not a frozen participant of attempt {:?}",
                    receipt.context().backend_process_id(),
                    registered.execution_id
                ))
            })?;
        if expected != receipt.context() {
            return Err(contract_violation(format!(
                "query context convergence identity does not match the frozen context (expected={expected} actual={})",
                receipt.context()
            )));
        }
        match receipt.state() {
            QueryContextConvergenceState::WorkerStoppedAndContextFenced => {}
        }

        let responsibility = attempt
            .backend_responsibilities
            .get_mut(&receipt.context().backend_process_id())
            .expect("frozen context has a backend responsibility");
        match responsibility.context_convergence {
            None => responsibility.context_convergence = Some(receipt),
            Some(existing) if existing == receipt => {}
            Some(existing) if existing.version() == receipt.version() => {
                return Err(contract_violation(format!(
                    "query context convergence version {} has conflicting facts",
                    receipt.version()
                )));
            }
            Some(existing) if receipt.version() < existing.version() => {
                return Err(contract_violation(format!(
                    "query context convergence version {} is older than retained version {}",
                    receipt.version(),
                    existing.version()
                )));
            }
            Some(existing) => match (existing.state(), receipt.state()) {
                (
                    QueryContextConvergenceState::WorkerStoppedAndContextFenced,
                    QueryContextConvergenceState::WorkerStoppedAndContextFenced,
                ) => responsibility.context_convergence = Some(receipt),
            },
        }
        Ok(*responsibility)
    }

    /// Replaces the last observation for one frozen backend only when its
    /// backend-issued version advances. Equal observations are idempotent;
    /// equal-version conflicts and stale observations are rejected.
    pub(crate) fn observe_attempt_resource_usage(
        &self,
        registered: &RegisteredAttempt,
        backend_process_id: BackendProcessId,
        observation: AttemptResourceObservation,
    ) -> Result<AttemptBackendResponsibilitySnapshot, DistributedQueryError> {
        let mut state = self.state.lock().expect("frontend query registry lock");
        let responsibility =
            self.attempt_backend_responsibility_mut(&mut state, registered, backend_process_id)?;
        match responsibility.resource {
            None => responsibility.resource = Some(observation),
            Some(existing) if observation.version > existing.version => {
                responsibility.resource = Some(observation);
            }
            Some(existing) if observation == existing => {}
            Some(existing) if observation.version == existing.version => {
                return Err(contract_violation(format!(
                    "attempt resource observation version {} has conflicting facts",
                    observation.version
                )));
            }
            Some(existing) => {
                return Err(contract_violation(format!(
                    "attempt resource observation version {} is older than retained version {}",
                    observation.version, existing.version
                )));
            }
        }
        Ok(*responsibility)
    }

    /// Marks the current resource use of one participant unknown without
    /// discarding its last versioned byte observation. Unknown is monotonic in
    /// the registry: a later sample may update the retained byte count, while
    /// only explicit convergence releases the residual responsibility.
    pub(crate) fn mark_attempt_resource_unknown(
        &self,
        registered: &RegisteredAttempt,
        backend_process_id: BackendProcessId,
    ) -> Result<AttemptBackendResponsibilitySnapshot, DistributedQueryError> {
        let mut state = self.state.lock().expect("frontend query registry lock");
        let responsibility =
            self.attempt_backend_responsibility_mut(&mut state, registered, backend_process_id)?;
        responsibility.current_unknown = true;
        Ok(*responsibility)
    }

    /// Retains an independently trusted execution fence for one frozen
    /// backend. Worker-owned convergence must enter through its exact typed
    /// receipt instead.
    pub(crate) fn record_attempt_stop_evidence(
        &self,
        registered: &RegisteredAttempt,
        backend_process_id: BackendProcessId,
        evidence: AttemptStopEvidence,
    ) -> Result<AttemptBackendResponsibilitySnapshot, DistributedQueryError> {
        let mut state = self.state.lock().expect("frontend query registry lock");
        let responsibility =
            self.attempt_backend_responsibility_mut(&mut state, registered, backend_process_id)?;
        let AttemptStopEvidence::TrustedExecutionFence { version } = evidence;
        responsibility.trusted_fence_version = Some(
            responsibility
                .trusted_fence_version
                .map_or(version, |old| old.max(version)),
        );
        Ok(*responsibility)
    }

    /// Records that topology revision `topology_revision` replaced one exact
    /// process identity. The evidence revokes routing eligibility only: it
    /// marks the old process's usage unknown and does not imply Worker stop,
    /// resource release, or external-effect convergence.
    pub(crate) fn record_attempt_process_replacement(
        &self,
        registered: &RegisteredAttempt,
        old_backend_process_id: BackendProcessId,
        replacement: BackendProcessId,
        topology_revision: u64,
    ) -> Result<AttemptBackendResponsibilitySnapshot, DistributedQueryError> {
        if old_backend_process_id == replacement {
            return Err(contract_violation(
                "backend process replacement must change the exact process identity",
            ));
        }
        let topology = self
            .backend_topology
            .lock()
            .expect("frontend backend topology gate lock");
        if !topology.initialized || topology.revision != topology_revision {
            return Err(contract_violation(format!(
                "backend process replacement requires the exact current topology revision (current={:?} evidence={topology_revision})",
                topology.initialized.then_some(topology.revision)
            )));
        }
        let mut state = self.state.lock().expect("frontend query registry lock");
        let key = self.validate_registered_attempt(&state, registered)?;
        let logical = state
            .logical
            .get(&key)
            .expect("frontend attempt owner points at a logical entry");
        let attempt = logical
            .attempts
            .get(&registered.execution_id)
            .expect("frontend attempt owner points at attempt state");
        let ordinal = attempt
            .scheduled_contexts
            .as_ref()
            .and_then(|contexts| {
                contexts.iter().find_map(|(&ordinal, context)| {
                    (context.backend_process_id() == old_backend_process_id).then_some(ordinal)
                })
            })
            .ok_or_else(|| {
                contract_violation(format!(
                    "backend process {old_backend_process_id} has no frozen ordinal in attempt {:?}",
                    registered.execution_id
                ))
            })?;
        if topology.live_process_ids.get(&ordinal) != Some(&replacement) {
            return Err(contract_violation(format!(
                "backend process replacement does not match frozen ordinal {ordinal} at topology revision {topology_revision}"
            )));
        }
        let responsibility = self.attempt_backend_responsibility_mut(
            &mut state,
            registered,
            old_backend_process_id,
        )?;
        let evidence = AttemptProcessReplacementEvidence {
            topology_revision,
            replacement,
        };
        match responsibility.process_replacement {
            None => responsibility.process_replacement = Some(evidence),
            Some(existing) if topology_revision > existing.topology_revision => {
                responsibility.process_replacement = Some(evidence);
            }
            Some(existing) if existing == evidence => {}
            Some(existing) if topology_revision == existing.topology_revision => {
                return Err(contract_violation(format!(
                    "backend topology revision {topology_revision} has conflicting process replacement facts"
                )));
            }
            Some(existing) => {
                return Err(contract_violation(format!(
                    "backend topology revision {topology_revision} is older than retained replacement revision {}",
                    existing.topology_revision
                )));
            }
        }
        responsibility.current_unknown = true;
        Ok(*responsibility)
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

    fn validate_query_contexts(
        &self,
        execution_id: QueryExecutionId,
        contexts: &[(usize, QueryContextRef)],
    ) -> Result<BTreeMap<usize, QueryContextRef>, DistributedQueryError> {
        let mut frozen = BTreeMap::new();
        let mut process_ids = BTreeSet::new();
        let mut frontend_process_id = None;
        for &(ordinal, context) in contexts {
            if context.query_execution_id() != execution_id {
                return Err(contract_violation(format!(
                    "query context at ordinal {ordinal} belongs to a different attempt (expected={execution_id:?} actual={:?})",
                    context.query_execution_id()
                )));
            }
            if frozen.insert(ordinal, context).is_some() {
                return Err(contract_violation(format!(
                    "query context ownership contains duplicate scheduling ordinal {ordinal}"
                )));
            }
            if !process_ids.insert(context.backend_process_id()) {
                return Err(contract_violation(format!(
                    "query context ownership contains duplicate backend process identity {}",
                    context.backend_process_id()
                )));
            }
            match frontend_process_id {
                None => frontend_process_id = Some(context.frontend_process_id()),
                Some(expected) if expected == context.frontend_process_id() => {}
                Some(expected) => {
                    return Err(contract_violation(format!(
                        "query context ownership mixes frontend process identities {expected} and {}",
                        context.frontend_process_id()
                    )));
                }
            }
        }
        let topology = self
            .backend_topology
            .lock()
            .expect("frontend backend topology gate lock");
        if !topology.initialized {
            return Err(contract_violation(
                "exact query contexts require an initialized backend topology",
            ));
        }
        for (&ordinal, context) in &frozen {
            match topology.live_process_ids.get(&ordinal) {
                Some(process_id) if *process_id == context.backend_process_id() => {}
                Some(process_id) => {
                    return Err(DistributedQueryError::new(
                        DistributedQueryErrorKind::Rejected,
                        format!(
                            "query context ordinal {ordinal} process identity {} is stale; current process identity is {process_id}",
                            context.backend_process_id()
                        ),
                    ));
                }
                None => {
                    return Err(DistributedQueryError::new(
                        DistributedQueryErrorKind::Rejected,
                        format!(
                            "query context ordinal {ordinal} process identity {} is not present in the current frontend topology",
                            context.backend_process_id()
                        ),
                    ));
                }
            }
        }
        Ok(frozen)
    }

    fn attempt_backend_responsibility_mut<'a>(
        &self,
        state: &'a mut RegistryState,
        registered: &RegisteredAttempt,
        backend_process_id: BackendProcessId,
    ) -> Result<&'a mut AttemptBackendResponsibilitySnapshot, DistributedQueryError> {
        let key = self.validate_registered_attempt(state, registered)?;
        let logical = state
            .logical
            .get_mut(&key)
            .expect("frontend attempt owner points at a logical entry");
        let attempt = logical
            .attempts
            .get_mut(&registered.execution_id)
            .expect("frontend attempt owner points at attempt state");
        attempt
            .backend_responsibilities
            .get_mut(&backend_process_id)
            .ok_or_else(|| {
                contract_violation(format!(
                    "backend process {backend_process_id} is not a frozen participant of attempt {:?}",
                    registered.execution_id
                ))
            })
    }

    pub(crate) fn replace_live_backends(
        &self,
        revision: u64,
        backends: &[LiveBackendTarget],
    ) -> Result<(), DistributedQueryError> {
        let live_process_ids = backends
            .iter()
            .map(|target| {
                Ok((
                    target.backend_idx(),
                    target.process_id().map_err(|error| {
                        contract_violation(format!(
                            "published backend target has an invalid process identity: {error}"
                        ))
                    })?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, DistributedQueryError>>()?;
        if live_process_ids.len() != backends.len() {
            return Err(contract_violation(
                "published backend topology contains duplicate scheduling ordinals",
            ));
        }
        let mut topology = self
            .backend_topology
            .lock()
            .expect("frontend backend topology gate lock");
        if topology.initialized {
            if revision < topology.revision {
                return Err(contract_violation(format!(
                    "backend topology revision {revision} is older than retained revision {}",
                    topology.revision
                )));
            }
            if revision == topology.revision {
                if topology.live_process_ids == live_process_ids {
                    return Ok(());
                }
                return Err(contract_violation(format!(
                    "backend topology revision {revision} has conflicting process identities"
                )));
            }
        }
        topology.initialized = true;
        topology.revision = revision;
        topology.live_process_ids = live_process_ids;
        drop(topology);

        // A topology revision governs future statement admission only. Existing
        // attempts retain their frozen participant manifest and are failed only
        // by lifecycle/control/transport evidence, or an exact replacement
        // event for one of their process identities.
        Ok(())
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
    if attempt.legacy_scheduled_backends.is_some() {
        return Err(contract_violation(
            "frontend query scheduled backend ownership is already registered",
        ));
    }
    let mut scheduled_backends = BTreeMap::new();
    let mut process_ids = BTreeSet::new();
    for &(ordinal, process_id) in backend_ownership {
        if scheduled_backends.insert(ordinal, process_id).is_some() {
            return Err(contract_violation(
                "frontend query scheduled backend ownership contains duplicate scheduling ordinals",
            ));
        }
        if !process_ids.insert(process_id) {
            return Err(contract_violation(
                "frontend query scheduled backend ownership contains duplicate backend process identities",
            ));
        }
    }
    attempt.backend_responsibilities = process_ids
        .iter()
        .copied()
        .map(|process_id| (process_id, AttemptBackendResponsibilitySnapshot::default()))
        .collect();
    attempt.legacy_scheduled_backends = Some(scheduled_backends);
    Ok(())
}

fn set_attempt_query_contexts(
    attempt: &mut AttemptState,
    scheduled_contexts: BTreeMap<usize, QueryContextRef>,
) -> Result<(), DistributedQueryError> {
    if attempt.scheduled_contexts.is_some() {
        return Err(contract_violation(
            "frontend query exact context ownership is already registered",
        ));
    }
    let scheduled_backends = scheduled_contexts
        .values()
        .map(|context| context.backend_process_id())
        .collect::<BTreeSet<_>>();
    let scheduled_ownership = scheduled_contexts
        .iter()
        .map(|(&ordinal, context)| (ordinal, context.backend_process_id()))
        .collect::<BTreeMap<_, _>>();
    match &attempt.legacy_scheduled_backends {
        Some(legacy) if legacy != &scheduled_ownership => {
            return Err(contract_violation(
                "exact query contexts conflict with compatibility backend ownership",
            ));
        }
        Some(_) => {
            let retained_backends = attempt
                .backend_responsibilities
                .keys()
                .copied()
                .collect::<BTreeSet<_>>();
            if retained_backends != scheduled_backends {
                return Err(contract_violation(
                    "compatibility backend responsibility facts do not match frozen exact contexts",
                ));
            }
        }
        None if !attempt.backend_responsibilities.is_empty() => {
            return Err(contract_violation(
                "exact query contexts cannot replace unowned backend responsibility facts",
            ));
        }
        None => {
            attempt.backend_responsibilities = scheduled_backends
                .iter()
                .copied()
                .map(|process_id| (process_id, AttemptBackendResponsibilitySnapshot::default()))
                .collect();
        }
    }
    attempt.legacy_scheduled_backends = None;
    attempt.scheduled_contexts = Some(scheduled_contexts);
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

    use novarocks_execution::task_execution::AdmissionEpochCapability;
    use novarocks_execution_contract::QueryContextConvergenceVersion;
    use novarocks_proto_codec::lifecycle::{AttemptId, QueryControlEndpoint};
    use novarocks_proto_codec::membership::BackendProcessDescriptor;
    use novarocks_types::{FrontendProcessId, NativeCompatibilityId};

    fn execution(query_id: QueryId, attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(query_id, AttemptId::new(attempt).expect("attempt id"))
            .expect("query execution id")
    }

    fn context(
        execution_id: QueryExecutionId,
        frontend_process_id: FrontendProcessId,
        backend_process_id: BackendProcessId,
    ) -> QueryContextRef {
        QueryContextRef::new(execution_id, frontend_process_id, backend_process_id)
    }

    fn initialize_topology(
        registry: &FrontendQueryRegistry,
        revision: u64,
        contexts: &[(usize, QueryContextRef)],
    ) {
        let mut topology = registry
            .backend_topology
            .lock()
            .expect("frontend backend topology gate lock");
        topology.initialized = true;
        topology.revision = revision;
        topology.live_process_ids = contexts
            .iter()
            .map(|&(ordinal, context)| (ordinal, context.backend_process_id()))
            .collect();
    }

    fn live_target(ordinal: usize, process_id: BackendProcessId) -> LiveBackendTarget {
        let descriptor = BackendProcessDescriptor::new(
            process_id,
            QueryControlEndpoint::new("127.0.0.1", 19000 + ordinal as u16)
                .expect("query control endpoint"),
            "test-deployment",
            "test-build",
            NativeCompatibilityId::new([0x71; 32]),
        )
        .expect("backend process descriptor");
        LiveBackendTarget::new(
            ordinal,
            descriptor,
            AdmissionEpochCapability::try_from_bytes([0x61; 16])
                .expect("admission epoch capability"),
        )
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
        let frontend = FrontendProcessId::new_v7();
        let first_context = context(first, frontend, first_backend);
        let second_context = context(second, frontend, second_backend);
        initialize_topology(&registry, 1, &[(0, first_context), (1, second_context)]);
        registry
            .set_attempt_query_contexts(&first_registration, &[(0, first_context)])
            .expect("record residual backend");
        registry
            .set_attempt_query_contexts(&_second_registration, &[(1, second_context)])
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
            residual.scheduled_contexts(),
            Some(&BTreeMap::from([(0, first_context)]))
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
            current.scheduled_contexts(),
            Some(&BTreeMap::from([(1, second_context)]))
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
    fn empty_exact_context_ownership_is_still_sealed_one_shot() {
        let registry = FrontendQueryRegistry::new(QueryProcessNamespace::new(0x27));
        let query_id = QueryId::new(0x27, 1);
        let execution_id = execution(query_id, 1);
        let _registration = registry
            .register_initial_attempt(execution_id)
            .expect("register attempt");

        initialize_topology(&registry, 1, &[]);
        registry
            .set_attempt_query_contexts(&_registration, &[])
            .expect("seal empty ownership");
        assert_eq!(
            registry
                .route_attempt(execution_id)
                .expect("route sealed attempt")
                .scheduled_contexts(),
            Some(&BTreeMap::new())
        );
        let backend = BackendProcessId::new_v7();
        let replacement_context = context(execution_id, FrontendProcessId::new_v7(), backend);
        initialize_topology(&registry, 2, &[(0, replacement_context)]);
        assert!(
            registry
                .set_attempt_query_contexts(&_registration, &[(0, replacement_context)],)
                .is_err(),
            "a sealed empty ownership set cannot be replaced"
        );
    }

    #[test]
    fn exact_registration_validates_and_atomically_freezes_contexts() {
        let registry = FrontendQueryRegistry::new(QueryProcessNamespace::new(0x2d));
        let query_id = QueryId::new(0x2d, 1);
        let execution_id = execution(query_id, 1);
        let other_execution = execution(query_id, 2);
        let frontend = FrontendProcessId::new_v7();
        let first_backend = BackendProcessId::new_v7();
        let second_backend = BackendProcessId::new_v7();
        let first = context(execution_id, frontend, first_backend);
        let second = context(execution_id, frontend, second_backend);
        assert!(
            registry
                .register_initial_attempt_with_contexts(execution_id, &[(0, first)])
                .is_err(),
            "exact contexts require an initialized topology"
        );
        assert!(registry.route_attempt(execution_id).is_none());
        initialize_topology(&registry, 1, &[(0, first), (1, second)]);

        assert!(
            registry
                .register_initial_attempt_with_contexts(
                    execution_id,
                    &[(0, context(other_execution, frontend, first_backend))],
                )
                .is_err(),
            "a context from another attempt must fail before registry mutation"
        );
        assert!(registry.route_attempt(execution_id).is_none());
        assert!(
            registry
                .register_initial_attempt_with_contexts(execution_id, &[(0, first), (0, second)],)
                .is_err(),
            "scheduling ordinals are unique within an attempt"
        );
        assert!(
            registry
                .register_initial_attempt_with_contexts(execution_id, &[(0, first), (1, first)],)
                .is_err(),
            "one backend process has exactly one context in an attempt"
        );

        let registration = registry
            .register_initial_attempt_with_contexts(execution_id, &[(0, first), (1, second)])
            .expect("register exact attempt and contexts atomically");
        assert_eq!(registration.execution_id(), execution_id);
        assert_eq!(
            registry
                .route_attempt(execution_id)
                .expect("route exact attempt")
                .scheduled_contexts(),
            Some(&BTreeMap::from([(0, first), (1, second)]))
        );
    }

    #[test]
    fn exact_context_upgrade_preserves_legacy_responsibility_facts() {
        let execution_id = execution(QueryId::new(0x2e, 1), 1);
        let backend = BackendProcessId::new_v7();
        let mut attempt = AttemptState::new();
        set_attempt_backend_ownership(&mut attempt, &[(0, backend)])
            .expect("freeze compatibility ownership");
        let retained = AttemptBackendResponsibilitySnapshot {
            resource: Some(AttemptResourceObservation {
                version: 7,
                last_known_usage_bytes: 4096,
            }),
            current_unknown: true,
            trusted_fence_version: Some(3),
            ..AttemptBackendResponsibilitySnapshot::default()
        };
        *attempt
            .backend_responsibilities
            .get_mut(&backend)
            .expect("compatibility responsibility") = retained;

        let exact = context(execution_id, FrontendProcessId::new_v7(), backend);
        set_attempt_query_contexts(&mut attempt, BTreeMap::from([(0, exact)]))
            .expect("upgrade matching exact contexts");
        assert_eq!(
            attempt.backend_responsibilities.get(&backend),
            Some(&retained),
            "exact identity upgrade must not erase resource or fencing facts"
        );

        let mut inconsistent = AttemptState::new();
        set_attempt_backend_ownership(&mut inconsistent, &[(0, backend)])
            .expect("freeze compatibility ownership");
        inconsistent.backend_responsibilities.clear();
        assert!(
            set_attempt_query_contexts(&mut inconsistent, BTreeMap::from([(0, exact)])).is_err(),
            "incomplete responsibility ownership fails closed"
        );

        let other_backend = BackendProcessId::new_v7();
        let frontend = FrontendProcessId::new_v7();
        let mut reordered = AttemptState::new();
        set_attempt_backend_ownership(&mut reordered, &[(0, backend), (1, other_backend)])
            .expect("freeze ordinal-qualified compatibility ownership");
        assert!(
            set_attempt_query_contexts(
                &mut reordered,
                BTreeMap::from([
                    (0, context(execution_id, frontend, other_backend)),
                    (1, context(execution_id, frontend, backend)),
                ]),
            )
            .is_err(),
            "the same process set at different ordinals cannot qualify exact contexts"
        );
    }

    #[test]
    fn topology_publication_is_monotonic_and_equal_revision_is_exactly_idempotent() {
        let registry = FrontendQueryRegistry::new(QueryProcessNamespace::new(0x2f));
        let first = BackendProcessId::new_v7();
        let conflicting = BackendProcessId::new_v7();
        registry
            .replace_live_backends(7, &[live_target(0, first)])
            .expect("publish first topology");
        registry
            .replace_live_backends(7, &[live_target(0, first)])
            .expect("replay exact topology");
        assert!(
            registry
                .replace_live_backends(7, &[live_target(0, conflicting)])
                .is_err(),
            "equal revision with another mapping is a contract conflict"
        );
        assert!(
            registry
                .replace_live_backends(6, &[live_target(0, first)])
                .is_err(),
            "a stale topology publication fails closed"
        );
        registry
            .replace_live_backends(8, &[live_target(0, conflicting)])
            .expect("advance topology revision");
        let topology = registry
            .backend_topology
            .lock()
            .expect("frontend backend topology gate lock");
        assert_eq!(topology.revision, 8);
        assert_eq!(
            topology.live_process_ids,
            BTreeMap::from([(0, conflicting)])
        );
    }

    #[test]
    fn residual_resource_observations_are_versioned_per_exact_backend() {
        let registry = FrontendQueryRegistry::new(QueryProcessNamespace::new(0x2b));
        let query_id = QueryId::new(0x2b, 1);
        let execution_id = execution(query_id, 1);
        let registration = registry
            .register_initial_attempt(execution_id)
            .expect("register attempt");
        let backend = BackendProcessId::new_v7();
        let replacement = BackendProcessId::new_v7();
        let query_context = context(execution_id, FrontendProcessId::new_v7(), backend);
        initialize_topology(&registry, 1, &[(0, query_context)]);
        registry
            .set_attempt_query_contexts(&registration, &[(0, query_context)])
            .expect("seal participant");

        let observed = AttemptResourceObservation {
            version: 7,
            last_known_usage_bytes: 4096,
        };
        assert_eq!(
            registry
                .observe_attempt_resource_usage(&registration, backend, observed)
                .expect("record usage")
                .resource,
            Some(observed)
        );
        assert_eq!(
            registry
                .observe_attempt_resource_usage(&registration, backend, observed)
                .expect("replay usage")
                .resource,
            Some(observed),
            "an exact observation replay is idempotent"
        );
        assert!(
            registry
                .observe_attempt_resource_usage(
                    &registration,
                    backend,
                    AttemptResourceObservation {
                        version: 7,
                        last_known_usage_bytes: 8192,
                    },
                )
                .is_err(),
            "one backend-issued version cannot describe two byte counts"
        );
        assert!(
            registry
                .observe_attempt_resource_usage(
                    &registration,
                    backend,
                    AttemptResourceObservation {
                        version: 6,
                        last_known_usage_bytes: 0,
                    },
                )
                .is_err(),
            "a stale observation cannot clear retained usage"
        );

        assert!(
            registry
                .mark_attempt_resource_unknown(&registration, backend)
                .expect("mark usage unknown")
                .current_unknown
        );
        let newer = AttemptResourceObservation {
            version: 8,
            last_known_usage_bytes: 2048,
        };
        let newer_responsibility = registry
            .observe_attempt_resource_usage(&registration, backend, newer)
            .expect("record a newer byte observation");
        assert_eq!(newer_responsibility.resource, Some(newer));
        assert!(
            newer_responsibility.current_unknown,
            "a byte sample cannot clear residual uncertainty"
        );

        {
            let mut topology = registry
                .backend_topology
                .lock()
                .expect("frontend backend topology gate lock");
            topology.initialized = true;
            topology.revision = 11;
            topology.live_process_ids = BTreeMap::from([(1, replacement)]);
        }
        assert!(
            registry
                .record_attempt_process_replacement(&registration, backend, replacement, 11)
                .is_err(),
            "a replacement at another ordinal cannot fence the frozen process"
        );
        {
            registry
                .backend_topology
                .lock()
                .expect("frontend backend topology gate lock")
                .live_process_ids = BTreeMap::from([(0, replacement)]);
        }
        assert!(
            registry
                .record_attempt_process_replacement(&registration, backend, replacement, 10)
                .is_err(),
            "replacement evidence must identify the exact observed topology revision"
        );
        let responsibility = registry
            .record_attempt_process_replacement(&registration, backend, replacement, 11)
            .expect("record exact process replacement");
        assert_eq!(
            responsibility.resource,
            Some(newer),
            "replacement preserves the last byte count and makes its currency unknown"
        );
        assert!(responsibility.current_unknown);
        assert_eq!(
            responsibility.process_replacement,
            Some(AttemptProcessReplacementEvidence {
                topology_revision: 11,
                replacement,
            })
        );
        let late_old_process_observation = registry
            .observe_attempt_resource_usage(
                &registration,
                backend,
                AttemptResourceObservation {
                    version: 9,
                    last_known_usage_bytes: 0,
                },
            )
            .expect("retain a signed late observation from the exact old process");
        assert_eq!(
            late_old_process_observation
                .resource
                .expect("late resource observation")
                .last_known_usage_bytes,
            0
        );
        assert!(
            late_old_process_observation.current_unknown,
            "late old-process evidence cannot restore routing eligibility or clear uncertainty"
        );
        assert_eq!(
            registry
                .route_attempt(execution_id)
                .expect("route exact residual responsibility")
                .convergence(),
            AttemptConvergenceFacts::default(),
            "process replacement is not stop or resource-release evidence"
        );
    }

    #[test]
    fn exact_context_convergence_is_versioned_and_identity_bound() {
        let registry = FrontendQueryRegistry::new(QueryProcessNamespace::new(0x2c));
        let query_id = QueryId::new(0x2c, 1);
        let execution_id = execution(query_id, 1);
        let registration = registry
            .register_initial_attempt(execution_id)
            .expect("register attempt");
        let backend = BackendProcessId::new_v7();
        let frontend = FrontendProcessId::new_v7();
        let query_context = context(execution_id, frontend, backend);
        initialize_topology(&registry, 1, &[(0, query_context)]);
        registry
            .set_attempt_query_contexts(&registration, &[(0, query_context)])
            .expect("seal participant");

        let receipt = QueryContextConvergenceReceipt::new(
            query_context,
            QueryContextConvergenceVersion::new(4).expect("version"),
            QueryContextConvergenceState::WorkerStoppedAndContextFenced,
        );
        let worker_stop = registry
            .record_query_context_convergence(&registration, receipt)
            .expect("record Worker convergence fact");
        assert_eq!(worker_stop.context_convergence, Some(receipt));
        assert_eq!(
            registry
                .record_query_context_convergence(&registration, receipt)
                .expect("replay Worker convergence fact")
                .context_convergence,
            Some(receipt),
            "an equal version with equal facts is idempotent"
        );
        assert!(
            registry
                .record_query_context_convergence(
                    &registration,
                    QueryContextConvergenceReceipt::new(
                        query_context,
                        QueryContextConvergenceVersion::new(3).expect("version"),
                        QueryContextConvergenceState::WorkerStoppedAndContextFenced,
                    ),
                )
                .is_err(),
            "a stale convergence publication is rejected"
        );

        let fenced = registry
            .record_attempt_stop_evidence(
                &registration,
                backend,
                AttemptStopEvidence::TrustedExecutionFence { version: 2 },
            )
            .expect("record trusted fence fact");
        assert_eq!(fenced.context_convergence, Some(receipt));
        assert_eq!(fenced.trusted_fence_version, Some(2));
        assert_eq!(
            registry
                .route_attempt(execution_id)
                .expect("route exact attempt")
                .convergence(),
            AttemptConvergenceFacts::default(),
            "positive stop evidence cannot release output or resources"
        );

        let foreign_backend = BackendProcessId::new_v7();
        assert!(
            registry
                .record_query_context_convergence(
                    &registration,
                    QueryContextConvergenceReceipt::new(
                        context(execution_id, frontend, foreign_backend),
                        QueryContextConvergenceVersion::FIRST,
                        QueryContextConvergenceState::WorkerStoppedAndContextFenced,
                    ),
                )
                .is_err(),
            "evidence for a process outside the frozen participant set is rejected"
        );
        assert_eq!(
            registry
                .route_attempt(execution_id)
                .expect("route responsibility")
                .backend_responsibilities()
                .get(&backend)
                .expect("backend responsibility")
                .context_convergence,
            Some(receipt)
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
