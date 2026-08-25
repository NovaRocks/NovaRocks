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
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use crate::common::backend_topology::{CoordinatorReportEndpoint, LiveBackendTarget};
use crate::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, ResolvedQueryOptions,
};
use crate::query_execution::schedule::FragmentLifecycleProjection;
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_proto::common;
use novarocks_proto::lifecycle::{
    ParticipantBackendIdentity, ParticipantManifest, ParticipantManifestDigest, ParticipantRole,
    QueryControlEndpoint, QueryExecutionId, QueryOptions as ProtocolQueryOptions,
    RuntimeFilterContribution,
};
use novarocks_proto::novarocks;

use crate::query_execution::launch::StageParticipantBinding;
use crate::query_execution::terminal_set::QueryTerminalSet;

/// Frozen target selected from one live backend snapshot.
///
/// This is coordinator orchestration state, not a native lifecycle message.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryLifecycleTarget {
    backend_idx: usize,
    endpoint: SocketAddr,
    start_epoch: u64,
}

impl QueryLifecycleTarget {
    pub const fn new(backend_idx: usize, endpoint: SocketAddr, start_epoch: u64) -> Self {
        Self {
            backend_idx,
            endpoint,
            start_epoch,
        }
    }

    pub const fn backend_idx(self) -> usize {
        self.backend_idx
    }

    pub const fn endpoint(self) -> SocketAddr {
        self.endpoint
    }

    pub const fn start_epoch(self) -> u64 {
        self.start_epoch
    }
}

fn contract_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

fn protocol_contract_error(
    error: novarocks_proto::lifecycle::ContractError,
) -> DistributedQueryError {
    contract_error(error.to_string())
}

fn protocol_report_endpoint(
    endpoint: CoordinatorReportEndpoint,
) -> Result<QueryControlEndpoint, DistributedQueryError> {
    let endpoint = endpoint.into_runtime_endpoint();
    let port = u32::try_from(endpoint.port())
        .map_err(|_| contract_error("report endpoint port is outside u32 range"))?;
    QueryControlEndpoint::parse(novarocks::QueryControlEndpoint {
        host: endpoint.host().to_string(),
        port,
    })
    .map_err(protocol_contract_error)
}

fn protocol_backend_identity(
    target: LiveBackendTarget,
) -> Result<ParticipantBackendIdentity, DistributedQueryError> {
    let backend_id = u64::try_from(target.backend_idx())
        .map_err(|_| contract_error("query initialization backend index is outside u64 range"))?;
    ParticipantBackendIdentity::parse(novarocks::ParticipantBackendIdentity {
        backend_id,
        endpoint: Some(novarocks::QueryControlEndpoint {
            host: target.endpoint().ip().to_string(),
            port: u32::from(target.endpoint().port()),
        }),
        start_epoch: target.start_epoch(),
    })
    .map_err(protocol_contract_error)
}

fn protocol_unique_id(id: novarocks_types::UniqueId) -> common::UniqueId {
    common::UniqueId {
        hi: id.high(),
        lo: id.low(),
    }
}

fn protocol_exchange_route(
    route: &novarocks_proto::lifecycle::ExchangeRouteManifest,
) -> novarocks::ExchangeRouteManifest {
    *route.as_proto()
}

fn duration_millis(duration: Duration) -> Result<u64, DistributedQueryError> {
    duration.as_millis().try_into().map_err(|_| {
        contract_error("query initialization pre-start timeout must fit in u64 milliseconds")
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct QueryInitPlanHeader {
    execution_id: QueryExecutionId,
    query_deadline_unix_ms: u64,
}

impl QueryInitPlanHeader {
    const fn new(execution_id: QueryExecutionId, query_deadline_unix_ms: u64) -> Self {
        Self {
            execution_id,
            query_deadline_unix_ms,
        }
    }

    pub(crate) const fn execution_id(self) -> QueryExecutionId {
        self.execution_id
    }
}

pub struct QueryInitOptions {
    execution_id: QueryExecutionId,
    live_backends: Vec<LiveBackendTarget>,
    /// Execution-owned options retained solely for sealed native fragment
    /// submission. They are not the lifecycle wire carrier.
    native_submission_options: QueryOptions,
    query_options: ProtocolQueryOptions,
    query_deadline_unix_ms: u64,
    pre_start_timeout: Duration,
    report_endpoint: QueryControlEndpoint,
}

impl QueryInitOptions {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        execution_id: QueryExecutionId,
        live_backends: Vec<LiveBackendTarget>,
        native_submission_options: &ResolvedQueryOptions,
        query_options: ProtocolQueryOptions,
        query_deadline_unix_ms: u64,
        pre_start_timeout: Duration,
        report_endpoint: CoordinatorReportEndpoint,
    ) -> Result<Self, DistributedQueryError> {
        if live_backends.is_empty() {
            return Err(contract_error(
                "query initialization requires at least one live backend",
            ));
        }
        if query_deadline_unix_ms == 0 {
            return Err(contract_error(
                "query initialization deadline must be nonzero",
            ));
        }
        if pre_start_timeout.is_zero() {
            return Err(contract_error(
                "query initialization pre-start timeout must be nonzero",
            ));
        }
        let mut backend_ids = BTreeSet::new();
        let mut endpoints = BTreeSet::new();
        for target in &live_backends {
            if target.start_epoch() == 0 {
                return Err(contract_error(format!(
                    "query initialization live backend {} has zero start epoch",
                    target.backend_idx()
                )));
            }
            if !backend_ids.insert(target.backend_idx()) {
                return Err(contract_error(format!(
                    "query initialization live snapshot repeats backend {}",
                    target.backend_idx()
                )));
            }
            if !endpoints.insert(target.endpoint()) {
                return Err(contract_error(format!(
                    "query initialization live snapshot repeats endpoint {}",
                    target.endpoint()
                )));
            }
        }
        let report_endpoint = protocol_report_endpoint(report_endpoint).map_err(|error| {
            contract_error(format!(
                "query initialization report endpoint is invalid: {error}"
            ))
        })?;
        Ok(Self {
            execution_id,
            live_backends,
            native_submission_options: native_submission_options.runtime_options().clone(),
            query_options,
            query_deadline_unix_ms,
            pre_start_timeout,
            report_endpoint,
        })
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub fn live_backends(&self) -> &[LiveBackendTarget] {
        &self.live_backends
    }

    /// Frozen runtime options carried from Init through the sealed native
    /// submission view.  They are read-only encoder input, never a route to
    /// reacquire lifecycle or topology state.
    pub fn native_submission_options(&self) -> &QueryOptions {
        &self.native_submission_options
    }

    /// The exact validated protocol options frozen into every participant
    /// manifest. Core does not project execution options into this carrier.
    pub const fn query_options(&self) -> &ProtocolQueryOptions {
        &self.query_options
    }
}

pub struct QueryInitPlan {
    execution_id: QueryExecutionId,
    #[allow(
        dead_code,
        reason = "Retained for staged query-execution contract and lifecycle integration."
    )]
    query_deadline_unix_ms: u64,
    participants: Vec<QueryInitParticipant>,
}

impl QueryInitPlan {
    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    #[allow(
        dead_code,
        reason = "Retained for staged query-execution contract and lifecycle integration."
    )]
    pub(crate) const fn query_deadline_unix_ms(&self) -> u64 {
        self.query_deadline_unix_ms
    }

    pub fn participant_count(&self) -> usize {
        self.participants.len()
    }

    pub fn backend_ids(&self) -> Vec<usize> {
        self.participants
            .iter()
            .map(QueryInitParticipant::backend_idx)
            .collect()
    }

    pub fn participant(&self, backend_idx: usize) -> Option<&QueryInitParticipant> {
        self.participants
            .iter()
            .find(|participant| participant.backend_idx() == backend_idx)
    }

    pub fn into_participants(self) -> Vec<QueryInitParticipant> {
        self.participants
    }

    /// Captures participant facts that must outlive consumption of the Init
    /// plan by the control-ready barrier. QLC-3 never re-resolves topology for
    /// Stage/Start after this point.
    pub fn stage_participant_bindings(
        &self,
    ) -> Result<Vec<StageParticipantBinding>, DistributedQueryError> {
        self.participants
            .iter()
            .map(|participant| {
                let endpoint = participant
                    .backend()
                    .endpoint()
                    .map_err(protocol_contract_error)?;
                let endpoint_ip = endpoint.host().parse::<IpAddr>().map_err(|error| {
                    contract_error(format!(
                        "query stage backend {} endpoint is not an IP address: {error}",
                        participant.backend_idx()
                    ))
                })?;
                StageParticipantBinding::new(
                    QueryLifecycleTarget::new(
                        participant.backend_idx(),
                        SocketAddr::new(endpoint_ip, endpoint.port()),
                        participant.backend().start_epoch(),
                    ),
                    participant.digest(),
                    participant
                        .manifest()
                        .roles()
                        .map_err(protocol_contract_error)?,
                    participant
                        .manifest()
                        .expected_fragment_instance_ids()
                        .into_iter()
                        .map(|id| novarocks_types::UniqueId::new(id.hi, id.lo)),
                )
                .map_err(|error| {
                    contract_error(format!(
                        "query stage participant {} is invalid: {error}",
                        participant.backend_idx()
                    ))
                })
            })
            .collect()
    }

    #[cfg(test)]
    pub fn from_manifests_for_contract_test(
        execution_id: QueryExecutionId,
        manifests: impl IntoIterator<Item = (usize, ParticipantManifest)>,
    ) -> Result<Self, DistributedQueryError> {
        let mut participants = manifests
            .into_iter()
            .map(|(backend_idx, manifest)| {
                if manifest.execution_id().map_err(protocol_contract_error)? != execution_id {
                    return Err(contract_error(
                        "contract-test participant execution id differs from query init plan",
                    ));
                }
                if manifest
                    .backend()
                    .map_err(protocol_contract_error)?
                    .backend_id()
                    != backend_idx as u64
                {
                    return Err(contract_error(
                        "contract-test participant backend identity differs from backend index",
                    ));
                }
                let digest = manifest.digest().map_err(protocol_contract_error)?;
                Ok(QueryInitParticipant {
                    backend_idx,
                    backend: manifest.backend().map_err(protocol_contract_error)?,
                    manifest,
                    digest,
                })
            })
            .collect::<Result<Vec<_>, DistributedQueryError>>()?;
        participants.sort_by_key(QueryInitParticipant::backend_idx);
        if participants
            .windows(2)
            .any(|pair| pair[0].backend_idx() == pair[1].backend_idx())
        {
            return Err(contract_error(
                "contract-test query init plan repeats a backend index",
            ));
        }
        if participants.is_empty() {
            return Err(contract_error(
                "contract-test query init plan requires a participant",
            ));
        }
        Ok(Self {
            execution_id,
            query_deadline_unix_ms: participants[0].manifest().query_deadline_unix_ms(),
            participants,
        })
    }
}

pub struct QueryInitParticipant {
    backend_idx: usize,
    backend: ParticipantBackendIdentity,
    manifest: ParticipantManifest,
    digest: ParticipantManifestDigest,
}

impl QueryInitParticipant {
    pub const fn backend_idx(&self) -> usize {
        self.backend_idx
    }

    pub const fn backend(&self) -> &ParticipantBackendIdentity {
        &self.backend
    }

    pub const fn manifest(&self) -> &ParticipantManifest {
        &self.manifest
    }

    pub const fn digest(&self) -> ParticipantManifestDigest {
        self.digest
    }

    pub fn into_parts(
        self,
    ) -> (
        usize,
        ParticipantBackendIdentity,
        ParticipantManifest,
        ParticipantManifestDigest,
    ) {
        (self.backend_idx, self.backend, self.manifest, self.digest)
    }
}

pub trait QueryInitBarrier: Send + Sync + 'static {
    fn initialize_all(
        &self,
        plan: QueryInitPlan,
    ) -> Result<QueryLifecycleLease, DistributedQueryError>;
}

/// The fail-closed result of aborting an attempt that had already entered
/// Running.  The original error is never replaced by terminal delivery
/// cleanup; a completed terminal set is supplemental evidence only.
#[derive(Clone, Debug)]
pub struct QueryLifecycleAbortOutcome {
    primary_error: String,
    terminal_set: Option<QueryTerminalSet>,
}

impl QueryLifecycleAbortOutcome {
    pub fn new(primary_error: impl Into<String>, terminal_set: Option<QueryTerminalSet>) -> Self {
        Self {
            primary_error: primary_error.into(),
            terminal_set,
        }
    }

    pub fn primary_error(&self) -> &str {
        &self.primary_error
    }

    pub fn terminal_set(&self) -> Option<&QueryTerminalSet> {
        self.terminal_set.as_ref()
    }

    pub fn into_primary_error(self) -> String {
        self.primary_error
    }
}

pub trait QueryLifecycleLeaseGuard: Send + 'static {
    fn finalize(self: Box<Self>) -> Result<QueryTerminalSet, DistributedQueryError>;

    fn abort_preserving(self: Box<Self>, primary_error: String) -> QueryLifecycleAbortOutcome;
}

#[must_use = "query lifecycle must be finalized or aborted"]
pub struct QueryLifecycleLease {
    guard: Option<Box<dyn QueryLifecycleLeaseGuard>>,
}

impl QueryLifecycleLease {
    pub fn new(guard: Box<dyn QueryLifecycleLeaseGuard>) -> Self {
        Self { guard: Some(guard) }
    }

    pub fn finalize(mut self) -> Result<QueryTerminalSet, DistributedQueryError> {
        self.guard
            .take()
            .expect("query lifecycle lease is consumed exactly once")
            .finalize()
    }

    pub fn abort_with_outcome(mut self, primary_error: String) -> QueryLifecycleAbortOutcome {
        self.guard
            .take()
            .expect("query lifecycle lease is consumed exactly once")
            .abort_preserving(primary_error)
    }

    pub fn abort_preserving(self, primary_error: String) -> String {
        self.abort_with_outcome(primary_error).into_primary_error()
    }
}

impl Drop for QueryLifecycleLease {
    fn drop(&mut self) {
        if let Some(guard) = self.guard.take() {
            let _ = guard
                .abort_preserving("query lifecycle lease dropped before completion".to_string());
        }
    }
}

pub(crate) fn compile_query_init_plan(
    fragments: &FragmentLifecycleProjection,
    runtime_filters: Vec<(usize, RuntimeFilterContribution)>,
    options: &QueryInitOptions,
) -> Result<QueryInitPlan, DistributedQueryError> {
    let live_by_backend = options
        .live_backends
        .iter()
        .map(|target| (target.backend_idx(), *target))
        .collect::<BTreeMap<_, _>>();
    if live_by_backend != fragments.frozen_live_backends {
        return Err(contract_error(
            "frozen schedule topology differs from query initialization snapshot",
        ));
    }
    for (&backend_idx, endpoint) in &fragments.endpoints_by_backend {
        let target = live_by_backend.get(&backend_idx).ok_or_else(|| {
            contract_error(format!(
                "scheduled backend {backend_idx} is absent from query initialization live snapshot"
            ))
        })?;
        if RuntimeEndpoint::from_socket_addr(target.endpoint()) != *endpoint {
            return Err(contract_error(format!(
                "scheduled backend {backend_idx} endpoint {} differs from query initialization snapshot endpoint {}",
                endpoint.as_host_port(),
                target.endpoint()
            )));
        }
    }

    let mut runtime_filter_by_backend = BTreeMap::new();
    for (backend_idx, contribution) in runtime_filters {
        if !live_by_backend.contains_key(&backend_idx) {
            return Err(contract_error(format!(
                "runtime filter backend {backend_idx} is absent from query initialization live snapshot"
            )));
        }
        if runtime_filter_by_backend
            .insert(backend_idx, contribution)
            .is_some()
        {
            return Err(contract_error(format!(
                "runtime filter contribution repeats backend {backend_idx}"
            )));
        }
    }

    let participant_ids = fragments
        .instances_by_backend
        .keys()
        .copied()
        .chain(runtime_filter_by_backend.keys().copied())
        .collect::<BTreeSet<_>>();
    let mut participants = Vec::with_capacity(participant_ids.len());
    for backend_idx in participant_ids {
        let target = *live_by_backend.get(&backend_idx).ok_or_else(|| {
            contract_error(format!(
                "query initialization participant backend {backend_idx} is not live"
            ))
        })?;
        let backend = protocol_backend_identity(target)?;
        let mut roles = BTreeSet::new();
        let expected_instances = fragments
            .instances_by_backend
            .get(&backend_idx)
            .cloned()
            .unwrap_or_default();
        if !expected_instances.is_empty() {
            roles.insert(ParticipantRole::FragmentExecutor);
        }
        let runtime_filter = runtime_filter_by_backend.remove(&backend_idx);
        if runtime_filter.is_some() {
            roles.insert(ParticipantRole::RuntimeFilterService);
        }
        let manifest = ParticipantManifest::parse(novarocks::ParticipantManifest {
            execution_id: Some(options.execution_id.to_proto()),
            backend: Some(backend.as_proto().clone()),
            participant_roles: roles.into_iter().map(i32::from).collect(),
            expected_fragment_instance_ids: expected_instances
                .into_iter()
                .map(protocol_unique_id)
                .collect(),
            query_options: Some(*options.query_options.as_proto()),
            query_deadline_unix_ms: options.query_deadline_unix_ms,
            exchange_routes: fragments
                .exchange_routes
                .iter()
                .map(protocol_exchange_route)
                .collect(),
            runtime_filter: runtime_filter.map(|contribution| contribution.as_proto().clone()),
            pre_start_timeout_ms: duration_millis(options.pre_start_timeout)?,
            report_endpoint: Some(options.report_endpoint.as_proto().clone()),
        })
        .map_err(|error| {
            contract_error(format!(
                "query initialization participant manifest is invalid: {error}"
            ))
        })?;
        let digest = manifest.digest().map_err(protocol_contract_error)?;
        participants.push(QueryInitParticipant {
            backend_idx,
            backend,
            manifest,
            digest,
        });
    }
    let header = QueryInitPlanHeader::new(options.execution_id, options.query_deadline_unix_ms);
    fragments.freeze_query_init_header(header)?;
    Ok(QueryInitPlan {
        execution_id: options.execution_id,
        query_deadline_unix_ms: header.query_deadline_unix_ms,
        participants,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::time::Duration;

    use super::{QueryInitOptions, compile_query_init_plan};
    use crate::common::backend_topology::{CoordinatorReportEndpoint, LiveBackendTarget};
    use crate::query_execution::contract::{QueryId, ResolvedQueryOptions};
    use crate::query_execution::schedule::FragmentLifecycleProjection;
    use novarocks_proto::lifecycle::{
        AttemptId, ParticipantRole, QueryExecutionId, QueryOptions, RuntimeFilterContribution,
    };
    use novarocks_proto::novarocks;
    use novarocks_types::UniqueId;

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(41, 73),
            AttemptId::new(7).expect("nonzero attempt"),
        )
        .expect("nonzero query id")
    }

    fn backend(backend_idx: usize) -> LiveBackendTarget {
        LiveBackendTarget::new(
            backend_idx,
            format!("127.0.0.1:{}", 19040 + backend_idx)
                .parse()
                .expect("valid endpoint"),
            100 + backend_idx as u64,
        )
    }

    fn runtime_filter(backend_idx: usize) -> (usize, RuntimeFilterContribution) {
        let participant_id = u32::try_from(backend_idx + 1).expect("participant");
        let contribution = RuntimeFilterContribution::parse(novarocks::RuntimeFilterContribution {
            participant_id,
            contribution_digest: vec![0; 32],
            ..Default::default()
        })
        .expect("valid opaque contribution");
        (backend_idx, contribution)
    }

    fn wire_query_options() -> QueryOptions {
        QueryOptions::parse(novarocks::QueryOptions::default()).expect("valid wire query options")
    }

    #[test]
    fn query_init_plan_unions_fragment_and_runtime_filter_participants() {
        let fragment_zero = UniqueId::new(10, 1);
        let fragment_one = UniqueId::new(10, 2);
        let fragments = FragmentLifecycleProjection::new(
            BTreeMap::from([
                (0, BTreeSet::from([fragment_zero])),
                (1, BTreeSet::from([fragment_one])),
            ]),
            BTreeMap::from([
                (
                    0,
                    novarocks_execution::runtime::endpoint::RuntimeEndpoint::from_socket_addr(
                        backend(0).endpoint(),
                    ),
                ),
                (
                    1,
                    novarocks_execution::runtime::endpoint::RuntimeEndpoint::from_socket_addr(
                        backend(1).endpoint(),
                    ),
                ),
            ]),
            Vec::new(),
        )
        .with_frozen_live_backends(vec![backend(0), backend(1), backend(2)])
        .expect("freeze schedule topology");
        let resolved = ResolvedQueryOptions::from_upstream(None);
        let options = QueryInitOptions::new(
            execution_id(),
            vec![backend(0), backend(1), backend(2)],
            &resolved,
            wire_query_options(),
            1_000,
            Duration::from_secs(30),
            CoordinatorReportEndpoint::from_socket_addr(
                "127.0.0.1:19030".parse().expect("valid report endpoint"),
            ),
        )
        .expect("valid init options");

        let plan = compile_query_init_plan(
            &fragments,
            vec![runtime_filter(1), runtime_filter(2)],
            &options,
        )
        .expect("valid init plan");

        assert_eq!(plan.backend_ids(), vec![0, 1, 2]);
        assert_eq!(
            plan.participant(2)
                .expect("service-only participant")
                .manifest()
                .expected_fragment_instance_ids(),
            Vec::new()
        );
        assert_eq!(
            plan.participant(2)
                .expect("service-only participant")
                .manifest()
                .roles()
                .expect("validated roles"),
            vec![ParticipantRole::RuntimeFilterService]
        );
    }

    #[test]
    fn runtime_filter_contribution_is_carried_opaquely() {
        let fragments =
            FragmentLifecycleProjection::new(BTreeMap::new(), BTreeMap::new(), Vec::new())
                .with_frozen_live_backends(vec![backend(2)])
                .expect("freeze schedule topology");
        let resolved = ResolvedQueryOptions::from_upstream(None);
        let options = QueryInitOptions::new(
            execution_id(),
            vec![backend(2)],
            &resolved,
            wire_query_options(),
            1_000,
            Duration::from_secs(30),
            CoordinatorReportEndpoint::from_socket_addr(
                "127.0.0.1:19030".parse().expect("valid report endpoint"),
            ),
        )
        .expect("valid init options");

        let plan = compile_query_init_plan(&fragments, vec![runtime_filter(2)], &options)
            .expect("valid init plan");
        let contribution = plan
            .participant(2)
            .expect("runtime filter participant")
            .manifest()
            .runtime_filter()
            .expect("validated runtime filter")
            .expect("runtime filter contribution");

        assert_eq!(contribution.participant_id(), 3);
        assert_eq!(contribution.digest(), &[0; 32]);
    }

    #[test]
    fn query_init_plan_rejects_backend_restart_at_the_same_endpoint() {
        let fragments = FragmentLifecycleProjection::new(
            BTreeMap::from([(0, BTreeSet::from([UniqueId::new(10, 1)]))]),
            BTreeMap::from([(
                0,
                novarocks_execution::runtime::endpoint::RuntimeEndpoint::from_socket_addr(
                    backend(0).endpoint(),
                ),
            )]),
            Vec::new(),
        )
        .with_frozen_live_backends(vec![backend(0)])
        .expect("freeze schedule topology");
        let resolved = ResolvedQueryOptions::from_upstream(None);
        let restarted = LiveBackendTarget::new(
            backend(0).backend_idx(),
            backend(0).endpoint(),
            backend(0).start_epoch() + 1,
        );
        let options = QueryInitOptions::new(
            execution_id(),
            vec![restarted],
            &resolved,
            wire_query_options(),
            1_000,
            Duration::from_secs(30),
            CoordinatorReportEndpoint::from_socket_addr(
                "127.0.0.1:19030".parse().expect("valid report endpoint"),
            ),
        )
        .expect("valid restarted snapshot");

        let error = match compile_query_init_plan(&fragments, Vec::new(), &options) {
            Ok(_) => panic!("same endpoint with a new start epoch must invalidate the schedule"),
            Err(error) => error,
        };

        assert!(
            error
                .message()
                .contains("frozen schedule topology differs from query initialization snapshot")
        );
    }

    #[test]
    fn query_init_plan_rejects_duplicate_runtime_filter_backend() {
        let fragments =
            FragmentLifecycleProjection::new(BTreeMap::new(), BTreeMap::new(), Vec::new())
                .with_frozen_live_backends(vec![backend(1)])
                .expect("freeze schedule topology");
        let resolved = ResolvedQueryOptions::from_upstream(None);
        let options = QueryInitOptions::new(
            execution_id(),
            vec![backend(1)],
            &resolved,
            wire_query_options(),
            9_000,
            Duration::from_secs(30),
            CoordinatorReportEndpoint::from_socket_addr(
                "127.0.0.1:19030".parse().expect("valid report endpoint"),
            ),
        )
        .expect("valid init options");

        let error = match compile_query_init_plan(
            &fragments,
            vec![runtime_filter(1), runtime_filter(1)],
            &options,
        ) {
            Ok(_) => panic!("one backend must have at most one runtime-filter contribution"),
            Err(error) => error,
        };

        assert!(
            error
                .message()
                .contains("runtime filter contribution repeats backend 1")
        );
    }

    #[test]
    fn query_init_plan_freezes_deadline() {
        let fragments =
            FragmentLifecycleProjection::new(BTreeMap::new(), BTreeMap::new(), Vec::new())
                .with_frozen_live_backends(vec![backend(2)])
                .expect("freeze schedule topology");
        let resolved = ResolvedQueryOptions::from_upstream(None);
        let options = QueryInitOptions::new(
            execution_id(),
            vec![backend(2)],
            &resolved,
            wire_query_options(),
            9_000,
            Duration::from_secs(30),
            CoordinatorReportEndpoint::from_socket_addr(
                "127.0.0.1:19030".parse().expect("valid report endpoint"),
            ),
        )
        .expect("valid init options");

        let plan = compile_query_init_plan(&fragments, vec![runtime_filter(2)], &options)
            .expect("valid init plan");

        assert_eq!(plan.query_deadline_unix_ms(), 9_000);

        let changed_deadline = QueryInitOptions::new(
            execution_id(),
            vec![backend(2)],
            &resolved,
            wire_query_options(),
            9_001,
            Duration::from_secs(30),
            CoordinatorReportEndpoint::from_socket_addr(
                "127.0.0.1:19030".parse().expect("valid report endpoint"),
            ),
        )
        .expect("valid changed options");
        let deadline_error =
            match compile_query_init_plan(&fragments, vec![runtime_filter(2)], &changed_deadline) {
                Ok(_) => panic!("one schedule cannot rebuild the same QEI with a new deadline"),
                Err(error) => error,
            };
        assert!(
            deadline_error
                .message()
                .contains("query initialization header differs")
        );
    }
}
