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

pub(crate) mod compiler;
pub mod extension;
pub mod install_validation;
pub(crate) mod planning_adapter;
pub mod role_graph;
pub(crate) mod routing_shard;
pub(crate) mod shard;
pub(crate) mod wait_for;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::Duration;

use crate::common::types::UniqueId;
use crate::runtime_filter::deployment::role_graph::RoleGraph;
use crate::runtime_filter::model::contract::{BindingId, ChannelId, PlanFragmentId};
use crate::runtime_filter::port::artifact::ArtifactContractError;
use crate::runtime_filter::port::identity::{
    DeploymentEpoch, RouteEdgeId, RuntimeFilterParticipantId,
};
use crate::runtime_filter::port::install::{
    MaterializationPolicy, RuntimeFilterCoreBudget, RuntimeFilterInstallView,
};
use crate::runtime_filter::port::routing::RuntimeFilterRoutingShard;
use crate::sql::planner::runtime_filter::validation::GraphValidationError;

/// Deployment-time resource / routing policy. Read-only input to the compiler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterDeploymentPolicy {
    /// Per-channel core budget (`RuntimeFilterCoreBudget`) stamped into every shard.
    pub core_budget: RuntimeFilterCoreBudget,
    /// How many redundant replica producers an `AnyOf` channel may use.
    /// Never hardcode a fixed fanout; the compiler clamps this to the live topology.
    pub replica_redundancy: u32,
    /// Physical materialization contract (bloom parameters + resource limits)
    /// stamped into every channel deployment. A resource-policy input supplied by
    /// the caller (RFD-6 / query options), never a magic default.
    pub materialization: MaterializationPolicy,
}

/// Query-scoped transport limits derived from the same sealed graph as the
/// compiler policy. The live sender consumes this explicit contract instead of
/// falling back to service-internal defaults.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterQueryTransportPolicy {
    pub(crate) retry_interval: Duration,
    pub(crate) max_attempts: u32,
    pub(crate) deadline: Duration,
    pub(crate) max_pending_entries: usize,
    pub(crate) max_pending_bytes: usize,
}

/// Complete query-level deployment policy assembled by the coordinator before
/// compilation. Install and transport use one graph-derived deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterQueryDeploymentPolicy {
    pub(crate) compiler: RuntimeFilterDeploymentPolicy,
    pub(crate) transport: RuntimeFilterQueryTransportPolicy,
    pub(crate) install_rpc_deadline: Duration,
}

pub type BindingInstanceIndex =
    BTreeMap<(ChannelId, BindingId, RuntimeFilterParticipantId), BTreeSet<UniqueId>>;

/// Static contract failures. Every variant is caught before fragment submission.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DeploymentError {
    /// The graph failed RFD-1 validation (called first).
    GraphInvalid(GraphValidationError),
    /// A binding references a fragment with no placement in the scheduling plan.
    MissingPlacement { fragment: PlanFragmentId },
    /// A placement referenced a backend id absent from the live snapshot.
    UnknownBackend { backend_idx: usize },
    /// The live backend snapshot contains the same backend id more than once.
    DuplicateBackend { backend_idx: usize },
    /// A live backend id cannot be represented by the native participant id.
    BackendIdOutOfRange { backend_idx: usize },
    /// A routing participant has no endpoint in the live backend snapshot.
    UnknownRouteParticipant {
        participant: RuntimeFilterParticipantId,
    },
    /// A route-edge id is reused anywhere in the query-global role graph.
    DuplicateRouteEdge { edge_id: RouteEdgeId },
    /// One producer fragment instance is assigned to multiple participants.
    AmbiguousProducerInstance {
        channel: ChannelId,
        binding: BindingId,
        fragment_instance_id: UniqueId,
    },
    /// A projected routing shard violates the native routing DTO contract.
    InvalidRoutingShard { detail: String },
    /// A projected Core install view is incomplete or internally inconsistent.
    InvalidInstallProjection { detail: String },
    /// The fragment edges supplied to the compiler formed a cycle; the
    /// execution dependency graph could not be built.
    FragmentCycle,
    /// A `BlockingSnapshot` consumer's wait edge closes a cycle in the refined
    /// wait graph (data-flow + frontier + wait + backpressure edges).
    BlockingFeedbackCycle {
        channel: ChannelId,
        binding: BindingId,
        /// Deterministic cycle path through the refined graph, for diagnosis.
        cycle: Vec<String>,
    },
    /// A channel's coverage carries no witnesses / producers.
    EmptyCoverage { channel: ChannelId },
    /// M1 install only supports Membership logical domains.
    UnsupportedLogicalDomain { channel: ChannelId },
    /// A consumer's semantic capabilities could not be lowered into a valid
    /// physical `ConsumerArtifactProfile` (M2 artifact contract).
    InvalidArtifactProfile(ArtifactContractError),
}

impl fmt::Display for DeploymentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GraphInvalid(e) => write!(f, "runtime filter graph invalid: {e}"),
            Self::MissingPlacement { fragment } => {
                write!(f, "no placement for fragment {}", fragment.get())
            }
            Self::UnknownBackend { backend_idx } => {
                write!(f, "unknown backend index {backend_idx}")
            }
            Self::DuplicateBackend { backend_idx } => {
                write!(f, "duplicate backend index {backend_idx}")
            }
            Self::BackendIdOutOfRange { backend_idx } => {
                write!(
                    f,
                    "backend index {backend_idx} does not fit runtime filter participant identity"
                )
            }
            Self::UnknownRouteParticipant { participant } => {
                write!(
                    f,
                    "runtime filter route participant {} has no live endpoint",
                    participant.get()
                )
            }
            Self::DuplicateRouteEdge { edge_id } => {
                write!(f, "duplicate runtime filter route edge {}", edge_id.get())
            }
            Self::AmbiguousProducerInstance {
                channel,
                binding,
                fragment_instance_id,
            } => {
                write!(
                    f,
                    "runtime filter producer instance {} for binding {} on channel {} maps to multiple participants",
                    fragment_instance_id,
                    binding.get(),
                    channel.get()
                )
            }
            Self::InvalidRoutingShard { detail } => {
                write!(f, "invalid runtime filter routing shard: {detail}")
            }
            Self::InvalidInstallProjection { detail } => {
                write!(f, "invalid runtime filter install projection: {detail}")
            }
            Self::FragmentCycle => {
                write!(f, "fragment execution dependency graph contains a cycle")
            }
            Self::BlockingFeedbackCycle {
                channel,
                binding,
                cycle,
            } => write!(
                f,
                "blocking-snapshot consumer binding {} on channel {} forms an execution cycle: {}",
                binding.get(),
                channel.get(),
                cycle.join(", ")
            ),
            Self::EmptyCoverage { channel } => {
                write!(f, "channel {} has empty coverage", channel.get())
            }
            Self::UnsupportedLogicalDomain { channel } => {
                write!(
                    f,
                    "channel {} uses an unsupported logical domain",
                    channel.get()
                )
            }
            Self::InvalidArtifactProfile(e) => {
                write!(f, "invalid consumer artifact profile: {e:?}")
            }
        }
    }
}

impl std::error::Error for DeploymentError {}

pub(crate) fn participant_id_for_backend(
    backend_idx: usize,
) -> Result<RuntimeFilterParticipantId, DeploymentError> {
    let participant = backend_idx
        .checked_add(1)
        .and_then(|participant| u32::try_from(participant).ok())
        .ok_or(DeploymentError::BackendIdOutOfRange { backend_idx })?;
    Ok(RuntimeFilterParticipantId::new(participant))
}

/// The compiler's output. Coordinator-side; `role_graph` carries the full
/// topology, while `install_views` and `routing_shards` carry the per-participant
/// views compiled atomically under the same deployment epoch.
#[derive(Clone, Debug)]
pub struct RuntimeFilterDeploymentPlan {
    pub epoch: DeploymentEpoch,
    pub participants: BTreeSet<RuntimeFilterParticipantId>,
    pub install_views: BTreeMap<RuntimeFilterParticipantId, RuntimeFilterInstallView>,
    pub routing_shards: BTreeMap<RuntimeFilterParticipantId, RuntimeFilterRoutingShard>,
    pub role_graph: RoleGraph,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_filter::model::contract::{BindingId, ChannelId};
    use crate::runtime_filter::port::install::{MaterializationPolicy, RuntimeFilterCoreBudget};

    #[test]
    fn policy_and_error_are_constructible() {
        let policy = RuntimeFilterDeploymentPolicy {
            core_budget: RuntimeFilterCoreBudget::new(4096),
            replica_redundancy: 2,
            materialization: MaterializationPolicy::for_test(),
        };
        assert_eq!(policy.core_budget.max_reducer_bytes(), 4096);
        assert_eq!(policy.replica_redundancy, 2);

        let err = DeploymentError::BlockingFeedbackCycle {
            channel: ChannelId::new(1),
            binding: BindingId::new(2),
            cycle: vec![
                "frag 3 --dataflow--> frag 2".to_string(),
                "build-ready(frag 2, join 10) --wait ch1/b2--> frag 1".to_string(),
            ],
        };
        let rendered = format!("{err}");
        assert!(rendered.contains("blocking"));
        assert!(rendered.contains("binding 2"));
        assert!(rendered.contains("channel 1"));
        assert!(rendered.contains(
            "frag 3 --dataflow--> frag 2, build-ready(frag 2, join 10) --wait ch1/b2--> frag 1"
        ));
    }

    #[test]
    fn backend_participant_identity_is_nonzero_and_overflow_checked() {
        assert_eq!(
            participant_id_for_backend(0).unwrap(),
            RuntimeFilterParticipantId::new(1)
        );
        assert_eq!(
            participant_id_for_backend(8).unwrap(),
            RuntimeFilterParticipantId::new(9)
        );
        assert!(participant_id_for_backend(u32::MAX as usize).is_err());
    }
}
