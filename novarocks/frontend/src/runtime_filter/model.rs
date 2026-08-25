//! Frontend-owned runtime-filter deployment model.
//!
//! Core exposes only sealed schedule/topology facts and carries the resulting
//! Protocol contribution.  Policy derivation, participant coverage, liveness,
//! and canonical contribution construction belong to this module and its
//! encoder.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::query_execution::artifact::RuntimeFilterArtifactId;
use novarocks_proto::{common, filter};

use super::deployment::RuntimeFilterWaitGraph;

pub(crate) const CONTRIBUTION_DIGEST_DOMAIN: &[u8] =
    b"novarocks.query-lifecycle.runtime-filter-contribution.v1\0";

const BLOOM_BITS_PER_KEY: u64 = 8;
const BLOOM_HASH_COUNT: u32 = 5;
const BLOOM_SEED: u64 = 17;
const BLOOM_ALGORITHM_VERSION: u32 = 1;
const TRANSPORT_RETRY_INTERVAL_MS: u64 = 200;
const MAX_PENDING_ENTRIES: u64 = 1 << 16;
const MAX_PENDING_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FrontendRuntimeFilterLifecycle {
    delivery_expire_ms: u64,
    query_expire_ms: u64,
    transport_retry_interval_ms: u64,
    transport_max_attempts: u64,
    transport_deadline_ms: u64,
    transport_max_pending_entries: u64,
    transport_max_pending_bytes: u64,
}

impl FrontendRuntimeFilterLifecycle {
    /// Materialize the query-scoped lifecycle supplied by the Frontend
    /// admission owner. The remaining values are the existing frozen
    /// deployment transport policy and are only emitted with a nonempty
    /// participant contribution.
    pub(crate) fn for_query(
        delivery_expire_ms: u64,
        query_expire_ms: u64,
    ) -> Result<Self, RuntimeFilterDeploymentError> {
        Self::new(
            delivery_expire_ms,
            query_expire_ms,
            TRANSPORT_RETRY_INTERVAL_MS,
            3,
            query_expire_ms,
            MAX_PENDING_ENTRIES,
            MAX_PENDING_BYTES,
        )
    }

    pub(crate) fn new(
        delivery_expire_ms: u64,
        query_expire_ms: u64,
        transport_retry_interval_ms: u64,
        transport_max_attempts: u64,
        transport_deadline_ms: u64,
        transport_max_pending_entries: u64,
        transport_max_pending_bytes: u64,
    ) -> Result<Self, RuntimeFilterDeploymentError> {
        let fields = [
            ("delivery_expire_ms", delivery_expire_ms),
            ("query_expire_ms", query_expire_ms),
            ("transport_retry_interval_ms", transport_retry_interval_ms),
            ("transport_max_attempts", transport_max_attempts),
            ("transport_deadline_ms", transport_deadline_ms),
            (
                "transport_max_pending_entries",
                transport_max_pending_entries,
            ),
            ("transport_max_pending_bytes", transport_max_pending_bytes),
        ];
        if let Some((name, _)) = fields.into_iter().find(|(_, value)| *value == 0) {
            return Err(RuntimeFilterDeploymentError::Invalid(format!(
                "runtime filter lifecycle field {name} must be nonzero"
            )));
        }
        Ok(Self {
            delivery_expire_ms,
            query_expire_ms,
            transport_retry_interval_ms,
            transport_max_attempts,
            transport_deadline_ms,
            transport_max_pending_entries,
            transport_max_pending_bytes,
        })
    }

    pub(crate) fn to_wire(self) -> filter::RuntimeFilterQueryLifecycleOptions {
        filter::RuntimeFilterQueryLifecycleOptions {
            delivery_expire_ms: self.delivery_expire_ms,
            query_expire_ms: self.query_expire_ms,
            transport_retry_interval_ms: self.transport_retry_interval_ms,
            transport_max_attempts: self.transport_max_attempts,
            transport_deadline_ms: self.transport_deadline_ms,
            transport_max_pending_entries: self.transport_max_pending_entries,
            transport_max_pending_bytes: self.transport_max_pending_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterChannelPolicy {
    pub(crate) max_contribution_bytes: u64,
    pub(crate) max_artifact_bytes: u64,
    pub(crate) deadline_ms: u64,
    pub(crate) max_retries: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FrontendRuntimeFilterDeploymentPolicy {
    pub(crate) core_budget_bytes: u64,
    pub(crate) replica_redundancy: u32,
    pub(crate) bloom_bits_per_key: u64,
    pub(crate) bloom_hash_count: u32,
    pub(crate) bloom_seed: u64,
    pub(crate) bloom_algorithm_version: u32,
    pub(crate) max_total_retained_bytes: u64,
    pub(crate) max_scratch_bytes_per_job: u64,
    pub(crate) max_concurrent_jobs: u64,
    pub(crate) lifecycle: FrontendRuntimeFilterLifecycle,
}

impl FrontendRuntimeFilterDeploymentPolicy {
    /// Derive the query-wide policy from sealed channel limits and the frozen
    /// live-backend snapshot.  An empty graph intentionally has no policy or
    /// participant contribution; callers must bypass this method for it.
    pub(crate) fn derive(
        channel_policies: impl IntoIterator<Item = RuntimeFilterChannelPolicy>,
        live_backend_count: usize,
        runtime_worker_count: usize,
        delivery_expire_ms: u64,
        query_expire_ms: u64,
    ) -> Result<Self, RuntimeFilterDeploymentError> {
        if live_backend_count == 0 {
            return Err(RuntimeFilterDeploymentError::Invalid(
                "runtime filter deployment requires a nonempty frozen live-backend snapshot"
                    .to_string(),
            ));
        }
        if runtime_worker_count == 0 {
            return Err(RuntimeFilterDeploymentError::Invalid(
                "runtime filter deployment worker count must be nonzero".to_string(),
            ));
        }

        let mut channel_count = 0usize;
        let mut total_artifact_bytes = 0u64;
        let mut max_artifact_bytes = 0u64;
        let mut minimum_deadline_ms = u64::MAX;
        let mut minimum_max_retries = u32::MAX;
        for policy in channel_policies {
            if policy.max_contribution_bytes == 0
                || policy.max_artifact_bytes == 0
                || policy.deadline_ms == 0
            {
                return Err(RuntimeFilterDeploymentError::Invalid(
                    "runtime filter channel has a zero resource or deadline limit".to_string(),
                ));
            }
            channel_count = channel_count.checked_add(1).ok_or_else(|| {
                RuntimeFilterDeploymentError::Invalid(
                    "runtime filter channel count overflow".to_string(),
                )
            })?;
            total_artifact_bytes = total_artifact_bytes
                .checked_add(policy.max_artifact_bytes)
                .ok_or_else(|| {
                    RuntimeFilterDeploymentError::Invalid(
                        "runtime filter artifact budget overflow".to_string(),
                    )
                })?;
            max_artifact_bytes = max_artifact_bytes.max(policy.max_artifact_bytes);
            minimum_deadline_ms = minimum_deadline_ms.min(policy.deadline_ms);
            minimum_max_retries = minimum_max_retries.min(policy.max_retries);
        }
        if channel_count == 0 {
            return Err(RuntimeFilterDeploymentError::Invalid(
                "runtime filter deployment policy requires a nonempty graph".to_string(),
            ));
        }
        let replica_redundancy = u32::try_from(live_backend_count).map_err(|_| {
            RuntimeFilterDeploymentError::Invalid(
                "runtime filter live backend count exceeds replica-redundancy width".to_string(),
            )
        })?;
        let transport_max_attempts =
            u64::from(minimum_max_retries.checked_add(1).ok_or_else(|| {
                RuntimeFilterDeploymentError::Invalid(
                    "runtime filter transport attempt count overflow".to_string(),
                )
            })?);
        let max_concurrent_jobs =
            u64::try_from(channel_count.min(runtime_worker_count)).map_err(|_| {
                RuntimeFilterDeploymentError::Invalid(
                    "runtime filter concurrent job count exceeds wire width".to_string(),
                )
            })?;

        Ok(Self {
            core_budget_bytes: total_artifact_bytes,
            replica_redundancy,
            bloom_bits_per_key: BLOOM_BITS_PER_KEY,
            bloom_hash_count: BLOOM_HASH_COUNT,
            bloom_seed: BLOOM_SEED,
            bloom_algorithm_version: BLOOM_ALGORITHM_VERSION,
            max_total_retained_bytes: total_artifact_bytes,
            max_scratch_bytes_per_job: max_artifact_bytes,
            max_concurrent_jobs,
            lifecycle: FrontendRuntimeFilterLifecycle::new(
                delivery_expire_ms,
                query_expire_ms,
                TRANSPORT_RETRY_INTERVAL_MS,
                transport_max_attempts,
                minimum_deadline_ms,
                MAX_PENDING_ENTRIES,
                MAX_PENDING_BYTES,
            )?,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct FrontendRuntimeFilterParticipant {
    backend_idx: usize,
    participant_id: u32,
    install: filter::RuntimeFilterParticipantInstall,
}

impl FrontendRuntimeFilterParticipant {
    pub(crate) fn active(
        backend_idx: usize,
        install: filter::RuntimeFilterParticipantInstall,
    ) -> Result<Self, RuntimeFilterDeploymentError> {
        let participant = Self::new(backend_idx, install)?;
        if participant.install.core_channels.is_empty()
            && participant.install.routing_channels.is_empty()
        {
            return Err(RuntimeFilterDeploymentError::Invalid(format!(
                "runtime filter active participant {} has an empty install",
                participant.participant_id
            )));
        }
        Ok(participant)
    }

    /// A live backend without scheduled fragments still participates in the
    /// query lifecycle and receives a typed, explicitly empty install.
    pub(crate) fn service_only(backend_idx: usize) -> Result<Self, RuntimeFilterDeploymentError> {
        Self::new(
            backend_idx,
            filter::RuntimeFilterParticipantInstall {
                core_channels: Vec::new(),
                routing_channels: Vec::new(),
            },
        )
    }

    fn new(
        backend_idx: usize,
        install: filter::RuntimeFilterParticipantInstall,
    ) -> Result<Self, RuntimeFilterDeploymentError> {
        let participant_id = participant_id_for_backend(backend_idx)?;
        validate_install_keys(&install, participant_id)?;
        Ok(Self {
            backend_idx,
            participant_id,
            install,
        })
    }

    pub(crate) const fn backend_idx(&self) -> usize {
        self.backend_idx
    }

    pub(crate) const fn participant_id(&self) -> u32 {
        self.participant_id
    }

    pub(crate) fn install(&self) -> &filter::RuntimeFilterParticipantInstall {
        &self.install
    }
}

fn validate_install_keys(
    install: &filter::RuntimeFilterParticipantInstall,
    participant_id: u32,
) -> Result<(), RuntimeFilterDeploymentError> {
    let mut core_channels = BTreeSet::new();
    for channel in &install.core_channels {
        if channel.channel_id == 0 {
            return Err(RuntimeFilterDeploymentError::Invalid(format!(
                "runtime filter participant {participant_id} has core channel id zero"
            )));
        }
        if !core_channels.insert(channel.channel_id) {
            return Err(RuntimeFilterDeploymentError::Invalid(format!(
                "runtime filter participant {participant_id} repeats core channel {}",
                channel.channel_id
            )));
        }
    }
    let mut routing_channels = BTreeSet::new();
    for channel in &install.routing_channels {
        if channel.channel_id == 0 {
            return Err(RuntimeFilterDeploymentError::Invalid(format!(
                "runtime filter participant {participant_id} has routing channel id zero"
            )));
        }
        if !routing_channels.insert(channel.channel_id) {
            return Err(RuntimeFilterDeploymentError::Invalid(format!(
                "runtime filter participant {participant_id} repeats routing channel {}",
                channel.channel_id
            )));
        }
        if channel.local_roles.is_empty() {
            return Err(RuntimeFilterDeploymentError::Invalid(format!(
                "runtime filter participant {participant_id} has routing channel {} without a local role",
                channel.channel_id
            )));
        }
        // A loopback route is deliberately mirrored in both directions. The
        // Backend routing shard verifies that such cross-side duplicates are
        // byte-for-byte identical self edges; reject only duplicates within
        // either direction here.
        let mut inbound_route_edges = BTreeSet::new();
        let mut outbound_route_edges = BTreeSet::new();
        for (direction, edges, route_edges) in [
            ("inbound", &channel.inbound_edges, &mut inbound_route_edges),
            (
                "outbound",
                &channel.outbound_edges,
                &mut outbound_route_edges,
            ),
        ] {
            for edge in edges {
                if edge.route_edge_id == 0 {
                    return Err(RuntimeFilterDeploymentError::Invalid(format!(
                        "runtime filter participant {participant_id} has routing channel {} with route edge id zero",
                        channel.channel_id
                    )));
                }
                if !route_edges.insert(edge.route_edge_id) {
                    return Err(RuntimeFilterDeploymentError::Invalid(format!(
                        "runtime filter participant {participant_id} repeats {direction} route edge {} in routing channel {}",
                        edge.route_edge_id, channel.channel_id,
                    )));
                }
                let source = edge.source.as_ref().ok_or_else(|| {
                    RuntimeFilterDeploymentError::Invalid(format!(
                        "runtime filter participant {participant_id} route edge {} has no source",
                        edge.route_edge_id
                    ))
                })?;
                let target = edge.target.as_ref().ok_or_else(|| {
                    RuntimeFilterDeploymentError::Invalid(format!(
                        "runtime filter participant {participant_id} route edge {} has no target",
                        edge.route_edge_id
                    ))
                })?;
                if source.participant_id == 0 || target.participant_id == 0 {
                    return Err(RuntimeFilterDeploymentError::Invalid(format!(
                        "runtime filter participant {participant_id} route edge {} has a zero endpoint participant",
                        edge.route_edge_id
                    )));
                }
            }
        }
    }
    Ok(())
}

#[allow(
    dead_code,
    reason = "Retained for target-specific frontend integration and regression coverage."
)]
pub(crate) struct FrontendRuntimeFilterDeployment {
    artifact_id: RuntimeFilterArtifactId,
    query_id: common::UniqueId,
    deployment_epoch: u64,
    lifecycle: FrontendRuntimeFilterLifecycle,
    participants: BTreeMap<usize, FrontendRuntimeFilterParticipant>,
}

#[allow(
    dead_code,
    reason = "Retained for target-specific frontend integration and regression coverage."
)]
impl FrontendRuntimeFilterDeployment {
    pub(crate) fn new(
        artifact_id: RuntimeFilterArtifactId,
        query_id: common::UniqueId,
        deployment_epoch: u64,
        lifecycle: FrontendRuntimeFilterLifecycle,
        expected_live_backend_ids: impl IntoIterator<Item = usize>,
        participants: impl IntoIterator<Item = FrontendRuntimeFilterParticipant>,
        wait_graph: &RuntimeFilterWaitGraph,
    ) -> Result<Self, RuntimeFilterDeploymentError> {
        if query_id.hi == 0 && query_id.lo == 0 {
            return Err(RuntimeFilterDeploymentError::Invalid(
                "runtime filter deployment query id must be nonzero".to_string(),
            ));
        }
        if deployment_epoch == 0 {
            return Err(RuntimeFilterDeploymentError::Invalid(
                "runtime filter deployment epoch must be nonzero".to_string(),
            ));
        }
        wait_graph
            .validate()
            .map_err(|error| RuntimeFilterDeploymentError::Invalid(error.to_string()))?;

        let expected_live_backend_ids = expected_live_backend_ids
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut by_backend = BTreeMap::new();
        for participant in participants {
            let backend_idx = participant.backend_idx();
            if by_backend.insert(backend_idx, participant).is_some() {
                return Err(RuntimeFilterDeploymentError::Invalid(format!(
                    "runtime filter deployment repeats backend {backend_idx}"
                )));
            }
        }
        if !by_backend.is_empty() {
            let actual = by_backend.keys().copied().collect::<BTreeSet<_>>();
            if actual != expected_live_backend_ids {
                let missing = expected_live_backend_ids
                    .difference(&actual)
                    .copied()
                    .collect::<Vec<_>>();
                let unknown = actual
                    .difference(&expected_live_backend_ids)
                    .copied()
                    .collect::<Vec<_>>();
                return Err(RuntimeFilterDeploymentError::Invalid(format!(
                    "runtime filter deployment participant set mismatch: missing={missing:?} unknown={unknown:?}"
                )));
            }
        }
        Ok(Self {
            artifact_id,
            query_id,
            deployment_epoch,
            lifecycle,
            participants: by_backend,
        })
    }

    pub(crate) const fn artifact_id(&self) -> RuntimeFilterArtifactId {
        self.artifact_id
    }

    pub(crate) fn query_id(&self) -> common::UniqueId {
        self.query_id
    }

    pub(crate) const fn deployment_epoch(&self) -> u64 {
        self.deployment_epoch
    }

    pub(crate) const fn lifecycle(&self) -> FrontendRuntimeFilterLifecycle {
        self.lifecycle
    }

    pub(crate) fn participants(
        &self,
    ) -> impl ExactSizeIterator<Item = &FrontendRuntimeFilterParticipant> {
        self.participants.values()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.participants.is_empty()
    }
}

fn participant_id_for_backend(backend_idx: usize) -> Result<u32, RuntimeFilterDeploymentError> {
    let ordinal = backend_idx.checked_add(1).ok_or_else(|| {
        RuntimeFilterDeploymentError::Invalid(
            "runtime filter backend index overflows participant identity".to_string(),
        )
    })?;
    u32::try_from(ordinal).map_err(|_| {
        RuntimeFilterDeploymentError::Invalid(
            "runtime filter backend index exceeds participant identity width".to_string(),
        )
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeFilterDeploymentError {
    Invalid(String),
}

impl fmt::Display for RuntimeFilterDeploymentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for RuntimeFilterDeploymentError {}

#[cfg(test)]
mod tests {
    use super::{
        FrontendRuntimeFilterDeploymentPolicy, FrontendRuntimeFilterLifecycle,
        RuntimeFilterChannelPolicy,
    };

    #[test]
    fn query_lifecycle_uses_three_total_transport_attempts() {
        let lifecycle =
            FrontendRuntimeFilterLifecycle::for_query(600, 900).expect("valid lifecycle");
        assert_eq!(lifecycle.to_wire().transport_max_attempts, 3);
    }

    #[test]
    fn derives_policy_from_frozen_channel_limits() {
        let policy = FrontendRuntimeFilterDeploymentPolicy::derive(
            [
                RuntimeFilterChannelPolicy {
                    max_contribution_bytes: 64,
                    max_artifact_bytes: 128,
                    deadline_ms: 900,
                    max_retries: 4,
                },
                RuntimeFilterChannelPolicy {
                    max_contribution_bytes: 32,
                    max_artifact_bytes: 256,
                    deadline_ms: 700,
                    max_retries: 2,
                },
            ],
            3,
            8,
            100,
            1000,
        )
        .expect("sealed limits are valid");
        assert_eq!(policy.core_budget_bytes, 384);
        assert_eq!(policy.replica_redundancy, 3);
        assert_eq!(policy.max_concurrent_jobs, 2);
        assert_eq!(policy.lifecycle.to_wire().transport_deadline_ms, 700);
        assert_eq!(policy.lifecycle.to_wire().transport_max_attempts, 3);
    }
}
