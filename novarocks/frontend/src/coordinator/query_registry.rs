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

use crate::common::backend_topology::LiveBackendTarget;
use crate::metrics::FrontendProcessQueryCountersSnapshot;
use crate::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, DistributedQueryIntent,
};
use crate::query_execution::runtime_filter_terminal_rollup::RuntimeFilterTerminalRollup;
use novarocks_proto_codec::lifecycle::QueryExecutionId;
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
            namespace,
            active: Mutex::new(BTreeMap::new()),
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
        let mut active = self.active.lock().expect("frontend query registry lock");
        let query = active
            .get_mut(&query_key(query_id))
            .ok_or_else(|| self.inactive_query(query_id))?;
        Ok(query.record_failure(cause, message.into()))
    }

    fn unregister(&self, key: QueryKey) {
        self.active
            .lock()
            .expect("frontend query registry lock")
            .remove(&key);
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

impl Drop for ActiveQueryGuard {
    fn drop(&mut self) {
        self.registry.unregister(self.key);
    }
}

fn query_key(query_id: QueryId) -> QueryKey {
    (query_id.high(), query_id.low())
}

fn contract_violation(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    use novarocks_proto_codec::lifecycle::AttemptId;

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
                    },
                );
            for message in messages {
                registry
                    .latch_failure(
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
    fn concrete_failure_supersedes_a_transport_observation_in_either_arrival_order() {
        fn primary_for_order(observation_first: bool) -> (QueryFailureCause, String) {
            let mut query = ActiveQuery {
                scheduled_backends: BTreeSet::new(),
                first_failure: None,
                secondary_failures: BTreeSet::new(),
                next_failure_id: 1,
            };
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
}
