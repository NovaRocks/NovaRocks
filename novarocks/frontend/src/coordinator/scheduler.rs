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
use std::net::SocketAddr;

use novarocks::query_execution::artifact::{
    BackendPlacement, FragmentId, FragmentScheduleDraft, FragmentSchedulingView,
    SchedulingStreamKind, ValidatedFragmentSchedule,
};
use novarocks::query_execution::backend::LiveBackendTarget;
use novarocks::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
use novarocks::query_execution::lifecycle::QueryExecutionId;

#[derive(Clone)]
pub struct FrontendBackendSnapshot {
    entries: Vec<(usize, SocketAddr)>,
    generations: BTreeMap<usize, u64>,
}

impl FrontendBackendSnapshot {
    #[cfg(test)]
    pub fn for_test(entries: Vec<(usize, SocketAddr)>) -> Result<Self, DistributedQueryError> {
        let generations = entries
            .iter()
            .map(|(backend_idx, _)| {
                (
                    *backend_idx,
                    u64::try_from(*backend_idx)
                        .expect("test backend index fits u64")
                        .saturating_add(1),
                )
            })
            .collect();
        Self::validate(entries, generations)
    }

    pub(crate) fn from_live_targets(
        targets: Vec<LiveBackendTarget>,
    ) -> Result<Self, DistributedQueryError> {
        let entries = targets
            .iter()
            .map(|target| (target.backend_idx(), target.endpoint()))
            .collect();
        let generations = targets
            .into_iter()
            .map(|target| (target.backend_idx(), target.start_epoch()))
            .collect();
        Self::validate(entries, generations)
    }

    fn validate(
        entries: Vec<(usize, SocketAddr)>,
        generations: BTreeMap<usize, u64>,
    ) -> Result<Self, DistributedQueryError> {
        if entries.is_empty() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::Rejected,
                "no live backend available",
            ));
        }
        let ids = entries.iter().map(|(id, _)| *id).collect::<BTreeSet<_>>();
        if ids.len() != entries.len() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "frontend backend snapshot contains duplicate backend ids",
            ));
        }
        if generations.len() != entries.len() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "frontend backend snapshot contains duplicate backend generations",
            ));
        }
        Ok(Self {
            entries,
            generations,
        })
    }

    pub fn entries(&self) -> &[(usize, SocketAddr)] {
        &self.entries
    }

    fn generation(&self, backend_idx: usize) -> Option<u64> {
        self.generations.get(&backend_idx).copied()
    }

    fn live_targets(&self) -> Vec<LiveBackendTarget> {
        self.entries
            .iter()
            .map(|&(backend_idx, endpoint)| {
                LiveBackendTarget::new(backend_idx, endpoint, self.generations[&backend_idx])
            })
            .collect()
    }
}

#[derive(Clone)]
pub struct FrontendFragmentScheduler {
    backends: FrontendBackendSnapshot,
}

impl FrontendFragmentScheduler {
    pub const fn new(backends: FrontendBackendSnapshot) -> Self {
        Self { backends }
    }

    pub fn backend_entries(&self) -> &[(usize, SocketAddr)] {
        self.backends.entries()
    }

    #[cfg(test)]
    pub(crate) fn live_targets(&self) -> Vec<LiveBackendTarget> {
        self.backends.live_targets()
    }

    pub(crate) fn scheduled_backend_ownership(
        &self,
        backend_ids: &[usize],
    ) -> Result<Vec<(usize, u64)>, DistributedQueryError> {
        backend_ids
            .iter()
            .map(|&backend_idx| {
                self.backends
                    .generation(backend_idx)
                    .map(|generation| (backend_idx, generation))
                    .ok_or_else(|| {
                        DistributedQueryError::new(
                            DistributedQueryErrorKind::ContractViolation,
                            format!(
                                "scheduled backend {backend_idx} is absent from the frontend topology snapshot"
                            ),
                        )
                    })
            })
            .collect()
    }

    pub fn schedule(
        &self,
        view: FragmentSchedulingView<'_>,
        execution_id: QueryExecutionId,
    ) -> Result<ValidatedFragmentSchedule, DistributedQueryError> {
        bind_query_lifecycle_fault_scopes(execution_id, &self.backends)?;
        let fragments = view
            .fragments()
            .map(|fragment| (fragment.fragment_id(), fragment))
            .collect::<BTreeMap<_, _>>();
        let scheduled_ids = fragments.keys().copied().collect::<BTreeSet<_>>();
        let ordered_ids = view
            .topological_order()
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if ordered_ids.len() != view.topological_order().len() || ordered_ids != scheduled_ids {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "sealed topological order is not a permutation of scheduled fragments",
            ));
        }

        #[derive(Clone, Copy)]
        struct IncomingEdge {
            source_fragment_id: FragmentId,
            native_hash_partitioned: bool,
            stream_kind: SchedulingStreamKind,
        }

        let mut incoming = BTreeMap::<FragmentId, Vec<IncomingEdge>>::new();
        for edge in view.edges() {
            incoming
                .entry(edge.target_fragment_id())
                .or_default()
                .push(IncomingEdge {
                    source_fragment_id: edge.source_fragment_id(),
                    native_hash_partitioned: edge.is_native_hash_partitioned(),
                    stream_kind: edge.stream_kind(),
                });
        }

        let live_backend_count = self.backends.entries.len();
        let backend_count = query_control_fragment_backend_limit(execution_id, live_backend_count)?
            .unwrap_or(live_backend_count);
        let mut counts = BTreeMap::<FragmentId, usize>::new();
        for &fragment_id in view.topological_order() {
            let fragment = fragments.get(&fragment_id).ok_or_else(|| {
                DistributedQueryError::new(
                    DistributedQueryErrorKind::ContractViolation,
                    format!("fragment {fragment_id} is missing from scheduling view"),
                )
            })?;
            let has_gather = incoming.get(&fragment_id).is_some_and(|edges| {
                edges
                    .iter()
                    .any(|edge| edge.stream_kind == SchedulingStreamKind::Gather)
            });
            let count = if has_gather {
                1
            } else if fragment.has_scan_nodes() {
                fragment
                    .scan_node_ids()
                    .iter()
                    .map(|&node_id| {
                        let file_ranges = fragment.scan_range_count(node_id).unwrap_or_default();
                        let connector_splits =
                            fragment.connector_split_count(node_id).unwrap_or_default();
                        file_ranges.max(connector_splits)
                    })
                    .max()
                    .unwrap_or_default()
                    .clamp(1, backend_count)
            } else {
                incoming
                    .get(&fragment_id)
                    .into_iter()
                    .flatten()
                    .filter(|edge| edge.native_hash_partitioned)
                    .filter_map(|edge| counts.get(&edge.source_fragment_id).copied())
                    .max()
                    .unwrap_or(1)
            };
            counts.insert(fragment_id, count);
        }

        let root_fragment_id = view.execution_anchor();
        let root = fragments.get(&root_fragment_id).ok_or_else(|| {
            DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "execution anchor is not present in scheduling view",
            )
        })?;
        // A statistics root is an internal fanout terminal: unlike a client
        // result root it must retain its scan-derived cardinality so every
        // scheduled backend contributes a bounded partial report.
        if !root.is_terminal_write() && !root.is_statistics() {
            counts.insert(root_fragment_id, 1);
        }

        let preferred = (execution_id.query_id().low() as usize) % backend_count;
        let mut draft = FragmentScheduleDraft::new();
        draft.freeze_live_backends(self.backends.live_targets())?;
        for (&fragment_id, &count) in &counts {
            let placements = (0..count)
                .map(|instance_index| {
                    let live_index = if count == 1 {
                        preferred
                    } else if count == backend_count {
                        instance_index
                    } else {
                        (preferred + instance_index) % backend_count
                    };
                    let (backend_idx, endpoint) = self.backends.entries[live_index];
                    BackendPlacement::new(backend_idx, endpoint)
                })
                .collect();
            draft.assign_fragment(fragment_id, placements)?;
        }
        ValidatedFragmentSchedule::validate(view, execution_id, draft)
    }
}

#[cfg(debug_assertions)]
fn bind_query_lifecycle_fault_scopes(
    execution_id: QueryExecutionId,
    backends: &FrontendBackendSnapshot,
) -> Result<(), DistributedQueryError> {
    use novarocks::common::query_lifecycle_fault::{QueryLifecycleFaultKind, bind_armed_fault};

    let Some(root) = novarocks::common::app_config::config()
        .ok()
        .and_then(|config| config.debug.query_lifecycle_fault_dir())
    else {
        return Ok(());
    };
    for &(backend_index, _) in &backends.entries {
        let start_epoch = backends
            .generations
            .get(&backend_index)
            .copied()
            .ok_or_else(|| {
                contract_error(format!(
                    "runner-owned lifecycle fault binding has no generation for backend {backend_index}"
                ))
            })?;
        let backend_id = u64::try_from(backend_index)
            .map_err(|_| contract_error("backend index does not fit u64"))?;
        for kind in [
            QueryLifecycleFaultKind::InitAckDrop,
            QueryLifecycleFaultKind::StageAckDrop,
            QueryLifecycleFaultKind::StartAckDrop,
            QueryLifecycleFaultKind::StartAckSuppress,
            QueryLifecycleFaultKind::HeartbeatStop,
            QueryLifecycleFaultKind::HeartbeatStopAfterStage,
            QueryLifecycleFaultKind::RestartAfterInitAck,
            QueryLifecycleFaultKind::TerminalAckDrop,
            QueryLifecycleFaultKind::TerminalSnapshotStreamDrop,
            QueryLifecycleFaultKind::TerminalSnapshotConflict,
        ] {
            if let Some(scope) = bind_armed_fault(
                root,
                kind,
                execution_id,
                backend_index,
                backend_id,
                start_epoch,
            )
            .map_err(contract_error)?
            {
                eprintln!(
                    "NOVAROCKS_QUERY_FAULT_BOUND kind={} execution_id={}:{}:{} backend_index={} backend_id={} start_epoch={} token={}",
                    kind.file_stem(),
                    execution_id.query_id().high(),
                    execution_id.query_id().low(),
                    execution_id.attempt_id().get(),
                    scope.backend_index,
                    scope.backend_id,
                    scope.start_epoch,
                    scope.token
                );
            }
        }
    }
    Ok(())
}

#[cfg(not(debug_assertions))]
fn bind_query_lifecycle_fault_scopes(
    _execution_id: QueryExecutionId,
    _backends: &FrontendBackendSnapshot,
) -> Result<(), DistributedQueryError> {
    Ok(())
}

#[cfg(debug_assertions)]
fn query_control_fragment_backend_limit(
    execution_id: QueryExecutionId,
    live_backend_count: usize,
) -> Result<Option<usize>, DistributedQueryError> {
    let Some(root) = novarocks::common::app_config::config()
        .ok()
        .and_then(|config| config.debug.query_lifecycle_fault_dir())
    else {
        return Ok(None);
    };
    let path = root.join("fragment-backend-limit.trigger");
    let contents = match std::fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(contract_error(format!(
                "read runner-owned fragment backend limit trigger {}: {error}",
                path.display()
            )));
        }
    };
    std::fs::remove_file(&path).map_err(|error| {
        contract_error(format!(
            "consume runner-owned fragment backend limit trigger {}: {error}",
            path.display()
        ))
    })?;
    let mut lines = contents.lines();
    let token = lines.next().unwrap_or_default().trim();
    let limit = lines
        .next()
        .unwrap_or_default()
        .trim()
        .parse::<usize>()
        .map_err(|error| contract_error(format!("invalid fragment backend limit: {error}")))?;
    if token.is_empty()
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        || lines.any(|line| !line.trim().is_empty())
    {
        return Err(contract_error(
            "runner-owned fragment backend limit trigger has invalid tokenized contents",
        ));
    }
    if !(1..=live_backend_count).contains(&limit) {
        return Err(contract_error(format!(
            "runner-owned fragment backend limit {limit} is outside 1..={live_backend_count}"
        )));
    }
    eprintln!(
        "NOVAROCKS_QUERY_CONTROL_FRAGMENT_LIMIT execution_id={}:{}:{} limit={limit} live_backends={live_backend_count} token={token}",
        execution_id.query_id().high(),
        execution_id.query_id().low(),
        execution_id.attempt_id().get()
    );
    Ok(Some(limit))
}

#[cfg(not(debug_assertions))]
fn query_control_fragment_backend_limit(
    _execution_id: QueryExecutionId,
    _live_backend_count: usize,
) -> Result<Option<usize>, DistributedQueryError> {
    Ok(None)
}

fn contract_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

#[cfg(test)]
mod tests {
    use super::{FrontendBackendSnapshot, FrontendFragmentScheduler};
    use novarocks::query_execution::contract::DistributedQueryErrorKind;
    use novarocks::query_execution::lifecycle::{AttemptId, QueryExecutionId};
    use novarocks_types::QueryId;

    fn execution_id(attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(41, 73),
            AttemptId::new(attempt).expect("nonzero attempt"),
        )
        .expect("valid execution id")
    }

    #[test]
    #[cfg(any())]
    fn scheduler_attempt_identity_is_stable_within_attempt_and_changes_between_attempts() {
        let fixture =
            novarocks::query_execution::contract_test_support::non_empty_runtime_filter_contract_fixture();
        let snapshot =
            FrontendBackendSnapshot::for_test(fixture.backends().to_vec()).expect("valid backends");
        let scheduler = FrontendFragmentScheduler::new(snapshot);
        let request = fixture.into_request();
        let parts = request.into_parts();
        let view = parts.artifacts.scheduling_view();

        let first = scheduler
            .schedule(view, execution_id(1))
            .expect("first schedule");
        let repeated = scheduler
            .schedule(view, execution_id(1))
            .expect("repeated schedule");
        let next = scheduler
            .schedule(view, execution_id(2))
            .expect("next-attempt schedule");

        assert_eq!(
            first.fragment_instance_ids(),
            repeated.fragment_instance_ids()
        );
        assert!(
            first
                .fragment_instance_ids()
                .iter()
                .zip(next.fragment_instance_ids())
                .all(|(left, right)| left != &right)
        );
    }

    #[test]
    fn empty_captured_topology_rejects_distributed_scheduling() {
        let error = match FrontendBackendSnapshot::for_test(Vec::new()) {
            Ok(_) => panic!("distributed scheduling requires at least one captured backend"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), DistributedQueryErrorKind::Rejected);
        assert_eq!(error.message(), "no live backend available");
    }
}
