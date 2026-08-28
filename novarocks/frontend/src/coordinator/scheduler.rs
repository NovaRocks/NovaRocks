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
#[cfg(test)]
use std::net::SocketAddr;

use crate::common::backend_topology::LiveBackendTarget;
use crate::query_execution::artifact::{
    BackendPlacement, FragmentId, FragmentScheduleDraft, FragmentSchedulingView,
    SchedulingStreamKind, ValidatedFragmentSchedule,
};
use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};
#[cfg(debug_assertions)]
use novarocks_failpoint::{QueryLifecycleFaultKind, arm_path, configured_root};
#[cfg(test)]
use novarocks_proto_codec::lifecycle::QueryControlEndpoint;
use novarocks_proto_codec::lifecycle::QueryExecutionId;
#[cfg(test)]
use novarocks_proto_codec::membership::BackendProcessDescriptor;
use novarocks_spi::connector::read_stack::ConnectorReadWorkSource;
use novarocks_types::BackendProcessId;

#[derive(Clone)]
pub struct FrontendBackendSnapshot {
    entries: Vec<LiveBackendTarget>,
}

impl FrontendBackendSnapshot {
    #[cfg(test)]
    pub fn for_test(entries: Vec<(usize, SocketAddr)>) -> Result<Self, DistributedQueryError> {
        let targets = entries
            .into_iter()
            .map(|(backend_idx, endpoint)| {
                let endpoint =
                    QueryControlEndpoint::new(endpoint.ip().to_string(), endpoint.port())
                        .map_err(|error| contract_error(error.to_string()))?;
                let descriptor = BackendProcessDescriptor::new(
                    BackendProcessId::new_v7(),
                    endpoint,
                    "scheduler-test",
                    "scheduler-test",
                    novarocks_types::NativeCompatibilityId::new([0x71; 32]),
                )
                .map_err(|error| contract_error(error.to_string()))?;
                Ok(LiveBackendTarget::new(backend_idx, descriptor))
            })
            .collect::<Result<Vec<_>, DistributedQueryError>>()?;
        Self::validate(targets)
    }

    pub(crate) fn from_live_targets(
        targets: Vec<LiveBackendTarget>,
    ) -> Result<Self, DistributedQueryError> {
        Self::validate(targets)
    }

    fn validate(entries: Vec<LiveBackendTarget>) -> Result<Self, DistributedQueryError> {
        if entries.is_empty() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::Rejected,
                "no live backend available",
            ));
        }
        let ids = entries
            .iter()
            .map(LiveBackendTarget::backend_idx)
            .collect::<BTreeSet<_>>();
        if ids.len() != entries.len() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "frontend backend snapshot contains duplicate backend ids",
            ));
        }
        let process_ids = entries
            .iter()
            .map(|target| {
                target
                    .process_id()
                    .map_err(|error| contract_error(error.to_string()))
            })
            .collect::<Result<BTreeSet<_>, DistributedQueryError>>()?;
        if process_ids.len() != entries.len() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "frontend backend snapshot contains duplicate backend process identities",
            ));
        }
        Ok(Self { entries })
    }

    pub fn entries(&self) -> &[LiveBackendTarget] {
        &self.entries
    }

    fn target(&self, backend_idx: usize) -> Option<&LiveBackendTarget> {
        self.entries
            .iter()
            .find(|target| target.backend_idx() == backend_idx)
    }

    fn live_targets(&self) -> Vec<LiveBackendTarget> {
        self.entries.clone()
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

    pub fn backend_entries(&self) -> &[LiveBackendTarget] {
        self.backends.entries()
    }

    #[cfg(test)]
    pub(crate) fn live_targets(&self) -> Vec<LiveBackendTarget> {
        self.backends.live_targets()
    }

    pub(crate) fn scheduled_backend_ownership(
        &self,
        backend_ids: &[usize],
    ) -> Result<Vec<(usize, BackendProcessId)>, DistributedQueryError> {
        backend_ids
            .iter()
            .map(|&backend_idx| {
                self.backends
                    .target(backend_idx)
                    .map(|target| {
                        target
                            .process_id()
                            .map(|process_id| (backend_idx, process_id))
                            .map_err(|error| contract_error(error.to_string()))
                    })
                    .transpose()?
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
                scan_fragment_parallelism(
                    fragment.scan_node_ids().iter().map(|&node_id| {
                        let file_ranges = fragment.scan_range_count(node_id).unwrap_or_default();
                        (file_ranges, fragment.connector_work_source(node_id))
                    }),
                    backend_count,
                )
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

        let fault_preferred = query_lifecycle_fault_preferred_live_index(&self.backends)?;
        if fault_preferred.is_some_and(|live_index| live_index >= backend_count) {
            return Err(contract_error(
                "runner-owned lifecycle fault target is excluded by the fragment backend limit",
            ));
        }
        let preferred = fault_preferred
            .unwrap_or_else(|| (execution_id.query_id().low() as usize) % backend_count);
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
                    let target = &self.backends.entries[live_index];
                    let endpoint = target
                        .endpoint()
                        .map_err(|error| contract_error(error.to_string()))?;
                    Ok(BackendPlacement::new(target.backend_idx(), endpoint))
                })
                .collect::<Result<Vec<_>, DistributedQueryError>>()?;
            draft.assign_fragment(fragment_id, placements)?;
        }
        let schedule = ValidatedFragmentSchedule::validate(view, execution_id, draft)?;
        bind_query_lifecycle_fault_scopes(execution_id, &self.backends)?;
        Ok(schedule)
    }
}

/// Choose one fragment's scan fanout without enumerating connector splits.
///
/// Runtime split sources can feed every admitted task. A whole-relation scan
/// has no split at all, so any fragment containing one must have exactly one
/// instance; otherwise each instance would read and return the complete
/// relation. Other scans in the same fragment still run correctly on that one
/// instance, although with intentionally reduced parallelism.
fn scan_fragment_parallelism(
    scans: impl IntoIterator<Item = (usize, Option<ConnectorReadWorkSource>)>,
    backend_count: usize,
) -> usize {
    let mut parallelism = 1;
    for (file_ranges, work_source) in scans {
        match work_source {
            Some(ConnectorReadWorkSource::WholeRelation) => return 1,
            Some(ConnectorReadWorkSource::RuntimeSplits) => {
                parallelism = parallelism.max(backend_count);
            }
            None => {
                parallelism = parallelism.max(file_ranges);
            }
        }
    }
    parallelism.clamp(1, backend_count)
}

#[cfg(debug_assertions)]
fn bind_query_lifecycle_fault_scopes(
    execution_id: QueryExecutionId,
    backends: &FrontendBackendSnapshot,
) -> Result<(), DistributedQueryError> {
    use novarocks_failpoint::{QueryLifecycleFaultKind, bind_armed_fault};

    let Some(root) = novarocks_failpoint::configured_root() else {
        return Ok(());
    };
    for target in &backends.entries {
        let backend_index = target.backend_idx();
        let process_id = target
            .process_id()
            .map_err(|error| contract_error(error.to_string()))?;
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
            QueryLifecycleFaultKind::ObservationP2AssemblyFailure,
            QueryLifecycleFaultKind::ObservationP2BudgetPressure,
            QueryLifecycleFaultKind::TerminalP0RetainedSlotExhausted,
            QueryLifecycleFaultKind::TerminalP0BytesExhausted,
            QueryLifecycleFaultKind::TerminalP0DeliveryPermitExhausted,
            QueryLifecycleFaultKind::TerminalP1EncodeFailure,
            QueryLifecycleFaultKind::TerminalP1RetentionExhausted,
            QueryLifecycleFaultKind::TerminalProofStreamDrop,
            QueryLifecycleFaultKind::TerminalAttestationStreamDrop,
            QueryLifecycleFaultKind::TerminalOutcomeSuppress,
            QueryLifecycleFaultKind::RuntimeFilterContributionAckDrop,
            QueryLifecycleFaultKind::TaskUpdateTerminalAckDrop,
        ] {
            if let Some(scope) = bind_armed_fault(
                &root,
                kind,
                protocol_execution_id(execution_id)?,
                backend_index,
                process_id,
            )
            .map_err(contract_error)?
            {
                eprintln!(
                    "NOVAROCKS_QUERY_FAULT_BOUND kind={} execution_id={}:{}:{} backend_index={} process_id={} token={}",
                    kind.file_stem(),
                    execution_id.query_id().high(),
                    execution_id.query_id().low(),
                    execution_id.attempt_id().get(),
                    scope.backend_index,
                    scope.process_id,
                    scope.token
                );
            }
        }
    }
    Ok(())
}

#[cfg(debug_assertions)]
fn protocol_execution_id(
    execution_id: QueryExecutionId,
) -> Result<novarocks_proto_codec::lifecycle::QueryExecutionId, DistributedQueryError> {
    let attempt = novarocks_proto_codec::lifecycle::AttemptId::new(execution_id.attempt_id().get())
        .map_err(|error| contract_error(error.to_string()))?;
    novarocks_proto_codec::lifecycle::QueryExecutionId::new(execution_id.query_id(), attempt)
        .map_err(|error| contract_error(error.to_string()))
}

#[cfg(not(debug_assertions))]
fn bind_query_lifecycle_fault_scopes(
    _execution_id: QueryExecutionId,
    _backends: &FrontendBackendSnapshot,
) -> Result<(), DistributedQueryError> {
    Ok(())
}

/// A runner arm normally names one exact BE. Pin single-instance fragments to
/// that BE so owner-local faults are reachable; contribution ACK loss is
/// receiver-agnostic because remote materialization may accept it elsewhere.
#[cfg(debug_assertions)]
fn query_lifecycle_fault_preferred_live_index(
    backends: &FrontendBackendSnapshot,
) -> Result<Option<usize>, DistributedQueryError> {
    let Some(root) = configured_root() else {
        return Ok(None);
    };
    let fault_kinds = [
        QueryLifecycleFaultKind::RestartAfterInitAck,
        QueryLifecycleFaultKind::ObservationP2AssemblyFailure,
        QueryLifecycleFaultKind::ObservationP2BudgetPressure,
        QueryLifecycleFaultKind::TerminalP0RetainedSlotExhausted,
        QueryLifecycleFaultKind::TerminalP0BytesExhausted,
        QueryLifecycleFaultKind::TerminalP0DeliveryPermitExhausted,
        QueryLifecycleFaultKind::TerminalP1EncodeFailure,
        QueryLifecycleFaultKind::TerminalP1RetentionExhausted,
        QueryLifecycleFaultKind::TerminalProofStreamDrop,
        QueryLifecycleFaultKind::TerminalAttestationStreamDrop,
        QueryLifecycleFaultKind::TerminalOutcomeSuppress,
    ];
    let armed = backends
        .entries
        .iter()
        .enumerate()
        .filter_map(|(live_index, target)| {
            fault_kinds
                .iter()
                .any(|kind| arm_path(&root, target.backend_idx(), *kind).exists())
                .then_some(live_index)
        })
        .collect::<Vec<_>>();
    match armed.as_slice() {
        [] => Ok(None),
        [live_index] => Ok(Some(*live_index)),
        _ => Err(contract_error(
            "runner-owned query lifecycle faults target more than one live backend",
        )),
    }
}

#[cfg(not(debug_assertions))]
fn query_lifecycle_fault_preferred_live_index(
    _backends: &FrontendBackendSnapshot,
) -> Result<Option<usize>, DistributedQueryError> {
    Ok(None)
}

#[cfg(debug_assertions)]
fn query_control_fragment_backend_limit(
    execution_id: QueryExecutionId,
    live_backend_count: usize,
) -> Result<Option<usize>, DistributedQueryError> {
    let Some(root) = novarocks_failpoint::configured_root() else {
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
    use super::{FrontendBackendSnapshot, scan_fragment_parallelism};
    use crate::query_execution::contract::DistributedQueryErrorKind;
    use novarocks_proto_codec::lifecycle::{AttemptId, QueryExecutionId};
    use novarocks_spi::connector::read_stack::ConnectorReadWorkSource;
    use novarocks_types::QueryId;

    #[test]
    fn whole_relation_constrains_its_fragment_to_one_backend() {
        assert_eq!(
            scan_fragment_parallelism(
                [
                    (0, Some(ConnectorReadWorkSource::RuntimeSplits)),
                    (0, Some(ConnectorReadWorkSource::WholeRelation)),
                    (8, None),
                ],
                3,
            ),
            1
        );
    }

    #[test]
    fn runtime_splits_retain_live_backend_fanout() {
        assert_eq!(
            scan_fragment_parallelism([(0, Some(ConnectorReadWorkSource::RuntimeSplits))], 3,),
            3
        );
    }

    #[allow(
        dead_code,
        reason = "Disabled scheduler fixture retains stable attempt identity coverage."
    )]
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
            crate::query_execution::contract_test_support::non_empty_runtime_filter_contract_fixture();
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
