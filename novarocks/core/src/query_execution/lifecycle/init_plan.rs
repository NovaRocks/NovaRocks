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

use crate::query_execution::backend::{CoordinatorReportEndpoint, LiveBackendTarget};
use crate::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, ResolvedQueryOptions,
    RuntimeFilterLifecycleView,
};
use crate::query_execution::runtime_filter::RuntimeFilterContributionPlan;
use crate::query_execution::schedule::FragmentLifecycleProjection;
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::query_options::QueryOptions;

use super::manifest::{
    ParticipantBackendIdentity, ParticipantManifest, ParticipantManifestDigest,
    ParticipantQueryOptions, ParticipantRole, QueryControlEndpoint, RuntimeFilterContribution,
};
use super::{QueryExecutionId, QueryLifecycleTarget, QueryTerminalSet, StageParticipantBinding};

fn contract_error(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct QueryInitPlanHeader {
    execution_id: QueryExecutionId,
    query_deadline_unix_ms: u64,
    runtime_filter_strategy: Option<crate::protocol::native::RuntimeFilterQueryLifecycleOptions>,
}

impl QueryInitPlanHeader {
    const fn new(
        execution_id: QueryExecutionId,
        query_deadline_unix_ms: u64,
        runtime_filter_strategy: Option<
            crate::protocol::native::RuntimeFilterQueryLifecycleOptions,
        >,
    ) -> Self {
        Self {
            execution_id,
            query_deadline_unix_ms,
            runtime_filter_strategy,
        }
    }

    pub(crate) const fn execution_id(self) -> QueryExecutionId {
        self.execution_id
    }
}

pub struct QueryInitOptions {
    execution_id: QueryExecutionId,
    live_backends: Vec<LiveBackendTarget>,
    runtime_filter_worker_count: usize,
    runtime_filter_lifecycle: RuntimeFilterLifecycleView,
    query_options: QueryOptions,
    query_deadline_unix_ms: u64,
    pre_start_timeout: Duration,
    report_endpoint: QueryControlEndpoint,
}

impl QueryInitOptions {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        execution_id: QueryExecutionId,
        live_backends: Vec<LiveBackendTarget>,
        runtime_filter_worker_count: usize,
        runtime_filter_lifecycle: RuntimeFilterLifecycleView,
        query_options: &ResolvedQueryOptions,
        query_deadline_unix_ms: u64,
        pre_start_timeout: Duration,
        report_endpoint: CoordinatorReportEndpoint,
    ) -> Result<Self, DistributedQueryError> {
        if live_backends.is_empty() {
            return Err(contract_error(
                "query initialization requires at least one live backend",
            ));
        }
        if runtime_filter_worker_count == 0 {
            return Err(contract_error(
                "query initialization runtime-filter worker count must be nonzero",
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
        let report_endpoint = QueryControlEndpoint::try_from(report_endpoint).map_err(|error| {
            contract_error(format!(
                "query initialization report endpoint is invalid: {error}"
            ))
        })?;
        Ok(Self {
            execution_id,
            live_backends,
            runtime_filter_worker_count,
            runtime_filter_lifecycle,
            query_options: query_options.runtime_options().clone(),
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

    pub const fn runtime_filter_worker_count(&self) -> usize {
        self.runtime_filter_worker_count
    }

    pub const fn runtime_filter_lifecycle(&self) -> RuntimeFilterLifecycleView {
        self.runtime_filter_lifecycle
    }

    pub(crate) fn native_submission_context(
        &self,
    ) -> Result<crate::query_execution::artifact::NativeSubmissionContext, DistributedQueryError>
    {
        Ok(crate::query_execution::artifact::NativeSubmissionContext {
            query_id: self.execution_id.query_id(),
            options: self.query_options.clone(),
        })
    }
}

pub struct QueryInitPlan {
    execution_id: QueryExecutionId,
    query_deadline_unix_ms: u64,
    runtime_filter_strategy: Option<crate::protocol::native::RuntimeFilterQueryLifecycleOptions>,
    participants: Vec<QueryInitParticipant>,
}

impl QueryInitPlan {
    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub(crate) const fn query_deadline_unix_ms(&self) -> u64 {
        self.query_deadline_unix_ms
    }

    pub(crate) const fn runtime_filter_strategy(
        &self,
    ) -> Option<crate::protocol::native::RuntimeFilterQueryLifecycleOptions> {
        self.runtime_filter_strategy
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
                let endpoint_ip = participant
                    .backend()
                    .endpoint()
                    .host()
                    .parse::<IpAddr>()
                    .map_err(|error| {
                        contract_error(format!(
                            "query stage backend {} endpoint is not an IP address: {error}",
                            participant.backend_idx()
                        ))
                    })?;
                StageParticipantBinding::new(
                    QueryLifecycleTarget::new(
                        participant.backend_idx(),
                        SocketAddr::new(endpoint_ip, participant.backend().endpoint().port()),
                        participant.backend().start_epoch(),
                    ),
                    participant.digest(),
                    participant.manifest().roles().iter().copied(),
                    participant
                        .manifest()
                        .expected_fragment_instance_ids()
                        .iter()
                        .copied(),
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

    #[cfg(feature = "query-execution-contract-test-support")]
    pub fn from_manifests_for_contract_test(
        execution_id: QueryExecutionId,
        manifests: impl IntoIterator<Item = (usize, ParticipantManifest)>,
    ) -> Result<Self, DistributedQueryError> {
        let mut participants = manifests
            .into_iter()
            .map(|(backend_idx, manifest)| {
                if manifest.execution_id() != execution_id {
                    return Err(contract_error(
                        "contract-test participant execution id differs from query init plan",
                    ));
                }
                if manifest.backend().backend_id() != backend_idx as u64 {
                    return Err(contract_error(
                        "contract-test participant backend identity differs from backend index",
                    ));
                }
                let digest = manifest.digest();
                Ok(QueryInitParticipant {
                    backend_idx,
                    backend: manifest.backend().clone(),
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
            runtime_filter_strategy: participants
                .iter()
                .find_map(|participant| participant.manifest().runtime_filter())
                .map(RuntimeFilterContribution::lifecycle),
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
    runtime_filters: Vec<RuntimeFilterContributionPlan>,
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

    let runtime_filter_strategy = runtime_filters
        .first()
        .map(RuntimeFilterContributionPlan::lifecycle);
    if let Some(strategy) = runtime_filter_strategy {
        if strategy.delivery_expire != options.runtime_filter_lifecycle.delivery_expire()
            || strategy.query_expire != options.runtime_filter_lifecycle.query_expire()
        {
            return Err(contract_error(
                "runtime filter lifecycle strategy differs from frozen query options",
            ));
        }
        if runtime_filters
            .iter()
            .any(|contribution| contribution.lifecycle() != strategy)
        {
            return Err(contract_error(
                "runtime filter lifecycle strategy differs between init participants",
            ));
        }
    }

    let mut runtime_filter_by_backend = BTreeMap::new();
    for contribution in runtime_filters {
        let backend_idx = contribution.backend_idx();
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
        let backend = ParticipantBackendIdentity::from_live_backend(target).map_err(|error| {
            contract_error(format!(
                "query initialization backend identity is invalid: {error}"
            ))
        })?;
        let mut roles = BTreeSet::new();
        let expected_instances = fragments
            .instances_by_backend
            .get(&backend_idx)
            .cloned()
            .unwrap_or_default();
        if !expected_instances.is_empty() {
            roles.insert(ParticipantRole::FragmentExecutor);
        }
        let runtime_filter = runtime_filter_by_backend
            .remove(&backend_idx)
            .map(|contribution| {
                roles.insert(ParticipantRole::RuntimeFilterService);
                let (_, participant_id, lifecycle, install) = contribution.into_parts();
                RuntimeFilterContribution::from_compiled(
                    options.execution_id,
                    participant_id,
                    lifecycle,
                    install,
                )
            })
            .transpose()
            .map_err(|error| {
                contract_error(format!(
                    "query initialization runtime filter contribution is invalid: {error}"
                ))
            })?;
        let manifest = ParticipantManifest::new(
            options.execution_id,
            backend.clone(),
            roles,
            expected_instances,
            ParticipantQueryOptions::new(options.query_options.clone()),
            options.query_deadline_unix_ms,
            fragments.exchange_routes.iter().cloned(),
            runtime_filter,
            options.pre_start_timeout,
            options.report_endpoint.clone(),
        )
        .map_err(|error| {
            contract_error(format!(
                "query initialization participant manifest is invalid: {error}"
            ))
        })?;
        let digest = manifest.digest();
        participants.push(QueryInitParticipant {
            backend_idx,
            backend,
            manifest,
            digest,
        });
    }
    let header = QueryInitPlanHeader::new(
        options.execution_id,
        options.query_deadline_unix_ms,
        runtime_filter_strategy,
    );
    fragments.freeze_query_init_header(header)?;
    Ok(QueryInitPlan {
        execution_id: header.execution_id,
        query_deadline_unix_ms: header.query_deadline_unix_ms,
        runtime_filter_strategy: header.runtime_filter_strategy,
        participants,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::time::Duration;

    use super::{QueryInitOptions, compile_query_init_plan};
    use crate::common::types::UniqueId;
    use crate::query_execution::backend::{CoordinatorReportEndpoint, LiveBackendTarget};
    use crate::query_execution::contract::{QueryId, ResolvedQueryOptions};
    use crate::query_execution::lifecycle::{AttemptId, ParticipantRole, QueryExecutionId};
    use crate::query_execution::runtime_filter::RuntimeFilterContributionPlan;
    use crate::query_execution::schedule::FragmentLifecycleProjection;
    use crate::runtime_filter::port::identity::{DeploymentEpoch, RuntimeFilterParticipantId};
    use crate::runtime_filter::port::install::{
        RuntimeFilterInstallView, RuntimeFilterParticipantInstall,
    };
    use crate::runtime_filter::port::routing::RuntimeFilterRoutingShard;

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

    fn runtime_filter(backend_idx: usize) -> RuntimeFilterContributionPlan {
        runtime_filter_with_retry_interval(backend_idx, Duration::from_millis(200))
    }

    fn runtime_filter_with_retry_interval(
        backend_idx: usize,
        transport_retry_interval: Duration,
    ) -> RuntimeFilterContributionPlan {
        let participant =
            RuntimeFilterParticipantId::new(u32::try_from(backend_idx + 1).expect("participant"));
        let epoch = DeploymentEpoch::new(execution_id().attempt_id().get());
        let install = RuntimeFilterParticipantInstall::new(
            RuntimeFilterInstallView::new(epoch, participant, BTreeMap::new()),
            RuntimeFilterRoutingShard::new(epoch, participant, BTreeMap::new())
                .expect("empty routing shard"),
        );
        RuntimeFilterContributionPlan::new(
            backend_idx,
            participant.get(),
            crate::protocol::native::RuntimeFilterQueryLifecycleOptions {
                delivery_expire: Duration::from_secs(300),
                query_expire: Duration::from_secs(300),
                transport_retry_interval,
                transport_max_attempts: 3,
                transport_deadline: Duration::from_secs(2),
                transport_max_pending_entries: 1024,
                transport_max_pending_bytes: 1 << 20,
            },
            install,
        )
        .expect("valid contribution")
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
                    crate::runtime::endpoint::RuntimeEndpoint::from_socket_addr(
                        backend(0).endpoint(),
                    ),
                ),
                (
                    1,
                    crate::runtime::endpoint::RuntimeEndpoint::from_socket_addr(
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
            2,
            resolved.runtime_filter_lifecycle(),
            &resolved,
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
            &BTreeSet::new()
        );
        assert_eq!(
            plan.participant(2)
                .expect("service-only participant")
                .manifest()
                .roles(),
            &BTreeSet::from([ParticipantRole::RuntimeFilterService])
        );
    }

    #[test]
    fn runtime_filter_contribution_is_bound_to_outer_attempt() {
        let fragments =
            FragmentLifecycleProjection::new(BTreeMap::new(), BTreeMap::new(), Vec::new())
                .with_frozen_live_backends(vec![backend(2)])
                .expect("freeze schedule topology");
        let resolved = ResolvedQueryOptions::from_upstream(None);
        let options = QueryInitOptions::new(
            execution_id(),
            vec![backend(2)],
            2,
            resolved.runtime_filter_lifecycle(),
            &resolved,
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
            .expect("runtime filter contribution");

        assert_eq!(
            contribution.install().epoch().get(),
            execution_id().attempt_id().get()
        );
    }

    #[test]
    fn query_init_plan_rejects_backend_restart_at_the_same_endpoint() {
        let fragments = FragmentLifecycleProjection::new(
            BTreeMap::from([(0, BTreeSet::from([UniqueId::new(10, 1)]))]),
            BTreeMap::from([(
                0,
                crate::runtime::endpoint::RuntimeEndpoint::from_socket_addr(backend(0).endpoint()),
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
            2,
            resolved.runtime_filter_lifecycle(),
            &resolved,
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
    fn query_init_plan_rejects_per_participant_runtime_filter_strategy_drift() {
        let fragments =
            FragmentLifecycleProjection::new(BTreeMap::new(), BTreeMap::new(), Vec::new())
                .with_frozen_live_backends(vec![backend(1), backend(2)])
                .expect("freeze schedule topology");
        let resolved = ResolvedQueryOptions::from_upstream(None);
        let options = QueryInitOptions::new(
            execution_id(),
            vec![backend(1), backend(2)],
            2,
            resolved.runtime_filter_lifecycle(),
            &resolved,
            9_000,
            Duration::from_secs(30),
            CoordinatorReportEndpoint::from_socket_addr(
                "127.0.0.1:19030".parse().expect("valid report endpoint"),
            ),
        )
        .expect("valid init options");

        let error = match compile_query_init_plan(
            &fragments,
            vec![
                runtime_filter_with_retry_interval(1, Duration::from_millis(200)),
                runtime_filter_with_retry_interval(2, Duration::from_millis(201)),
            ],
            &options,
        ) {
            Ok(_) => panic!("one immutable init plan must have one runtime-filter strategy"),
            Err(error) => error,
        };

        assert!(
            error
                .message()
                .contains("runtime filter lifecycle strategy differs")
        );
    }

    #[test]
    fn query_init_plan_freezes_deadline_and_runtime_filter_strategy() {
        let fragments =
            FragmentLifecycleProjection::new(BTreeMap::new(), BTreeMap::new(), Vec::new())
                .with_frozen_live_backends(vec![backend(2)])
                .expect("freeze schedule topology");
        let resolved = ResolvedQueryOptions::from_upstream(None);
        let options = QueryInitOptions::new(
            execution_id(),
            vec![backend(2)],
            2,
            resolved.runtime_filter_lifecycle(),
            &resolved,
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
        assert_eq!(
            plan.runtime_filter_strategy()
                .expect("nonempty RF plan has one strategy")
                .transport_retry_interval,
            Duration::from_millis(200)
        );

        let changed_deadline = QueryInitOptions::new(
            execution_id(),
            vec![backend(2)],
            2,
            resolved.runtime_filter_lifecycle(),
            &resolved,
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

        let strategy_error = match compile_query_init_plan(
            &fragments,
            vec![runtime_filter_with_retry_interval(
                2,
                Duration::from_millis(201),
            )],
            &options,
        ) {
            Ok(_) => panic!("one schedule cannot rebuild the same QEI with a new RF strategy"),
            Err(error) => error,
        };
        assert!(
            strategy_error
                .message()
                .contains("query initialization header differs")
        );
    }
}
