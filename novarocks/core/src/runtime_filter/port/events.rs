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

use crate::common::types::UniqueId;
use crate::runtime_filter::model::contract::{BindingId, ChannelId};
use crate::runtime_filter::port::artifact::{ArtifactKind, ConsumerProfileId};

use super::identity::{
    ContributionIdentity, DeploymentEpoch, LogicalVersion, RouteEdgeId, RuntimeFilterParticipantId,
};
use super::producer::{ProducerFailureReason, RuntimeContractViolationKind};
use super::subscription::{ArtifactUnsupportedReason, LiveTerminal, UnavailableReason};
use super::transport::RuntimeFilterAcceptStatus;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RuntimeFilterEventIdentity {
    query_id: UniqueId,
    participant_id: RuntimeFilterParticipantId,
    channel_id: ChannelId,
    epoch: DeploymentEpoch,
}

impl RuntimeFilterEventIdentity {
    pub const fn new(
        query_id: UniqueId,
        participant_id: RuntimeFilterParticipantId,
        channel_id: ChannelId,
        epoch: DeploymentEpoch,
    ) -> Self {
        Self {
            query_id,
            participant_id,
            channel_id,
            epoch,
        }
    }

    pub const fn query_id(self) -> UniqueId {
        self.query_id
    }
    pub const fn participant_id(self) -> RuntimeFilterParticipantId {
        self.participant_id
    }
    pub const fn channel_id(self) -> ChannelId {
        self.channel_id
    }
    pub const fn epoch(self) -> DeploymentEpoch {
        self.epoch
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProducerEventIdentity {
    common: RuntimeFilterEventIdentity,
    producer_binding_id: BindingId,
    fragment_instance_id: UniqueId,
}

impl ProducerEventIdentity {
    pub const fn new(
        common: RuntimeFilterEventIdentity,
        producer_binding_id: BindingId,
        fragment_instance_id: UniqueId,
    ) -> Self {
        Self {
            common,
            producer_binding_id,
            fragment_instance_id,
        }
    }
    pub const fn common(self) -> RuntimeFilterEventIdentity {
        self.common
    }
    pub const fn producer_binding_id(self) -> BindingId {
        self.producer_binding_id
    }
    pub const fn fragment_instance_id(self) -> UniqueId {
        self.fragment_instance_id
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConsumerEventIdentity {
    common: RuntimeFilterEventIdentity,
    consumer_binding_id: BindingId,
    fragment_instance_id: UniqueId,
}

impl ConsumerEventIdentity {
    pub const fn new(
        common: RuntimeFilterEventIdentity,
        consumer_binding_id: BindingId,
        fragment_instance_id: UniqueId,
    ) -> Self {
        Self {
            common,
            consumer_binding_id,
            fragment_instance_id,
        }
    }
    pub const fn common(self) -> RuntimeFilterEventIdentity {
        self.common
    }
    pub const fn consumer_binding_id(self) -> BindingId {
        self.consumer_binding_id
    }
    pub const fn fragment_instance_id(self) -> UniqueId {
        self.fragment_instance_id
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RouteEventIdentity {
    consumer: ConsumerEventIdentity,
    route_edge_id: RouteEdgeId,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ArtifactMaterializationIdentity {
    common: RuntimeFilterEventIdentity,
    profile_id: ConsumerProfileId,
    version: LogicalVersion,
}

impl ArtifactMaterializationIdentity {
    pub const fn new(
        common: RuntimeFilterEventIdentity,
        profile_id: ConsumerProfileId,
        version: LogicalVersion,
    ) -> Self {
        Self {
            common,
            profile_id,
            version,
        }
    }

    pub const fn common(self) -> RuntimeFilterEventIdentity {
        self.common
    }

    pub const fn profile_id(self) -> ConsumerProfileId {
        self.profile_id
    }

    pub const fn version(self) -> LogicalVersion {
        self.version
    }
}

impl RouteEventIdentity {
    pub const fn new(
        common: RuntimeFilterEventIdentity,
        consumer_binding_id: BindingId,
        fragment_instance_id: UniqueId,
        route_edge_id: RouteEdgeId,
    ) -> Self {
        Self {
            consumer: ConsumerEventIdentity::new(common, consumer_binding_id, fragment_instance_id),
            route_edge_id,
        }
    }
    pub const fn common(self) -> RuntimeFilterEventIdentity {
        self.consumer.common()
    }
    pub const fn consumer_binding_id(self) -> BindingId {
        self.consumer.consumer_binding_id()
    }
    pub const fn fragment_instance_id(self) -> UniqueId {
        self.consumer.fragment_instance_id()
    }
    pub const fn route_edge_id(self) -> RouteEdgeId {
        self.route_edge_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalDomainRejectionKind {
    Contract(RuntimeContractViolationKind),
    ResourceLimit,
}

/// Sender-side transport delivery-route identity: the query/participant/channel/epoch
/// coordinates plus the delivery route edge. This is the smallest identity that
/// unambiguously names a remote delivery route on the SENDER side.
///
/// It is deliberately NOT a [`RouteEventIdentity`]: the sender does not know the remote
/// consumer's fragment instance (the peer's ingress fans one delivery out to its local
/// subscriptions), so a consumer-instance anchor cannot be formed here without inventing
/// data. `participant_id` in the common coordinates is the LOCAL (emitting) participant,
/// consistent with every other lifecycle event.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TransportRouteEventIdentity {
    common: RuntimeFilterEventIdentity,
    route_edge_id: RouteEdgeId,
}

impl TransportRouteEventIdentity {
    pub const fn new(common: RuntimeFilterEventIdentity, route_edge_id: RouteEdgeId) -> Self {
        Self {
            common,
            route_edge_id,
        }
    }

    pub const fn common(self) -> RuntimeFilterEventIdentity {
        self.common
    }

    pub const fn route_edge_id(self) -> RouteEdgeId {
        self.route_edge_id
    }
}

/// Why a sender-side reliable-transport delivery route failed open — degraded without
/// erroring the query (runtime filters are an optimization, never a correctness
/// dependency).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportFailOpenReason {
    /// The buffered frame outlived its retry/ack deadline and was released.
    Deadline,
    /// A self-owned sender-buffer ceiling refused the frame before it was buffered or
    /// put on the wire (M3 Task 4 `ResourceLimit`).
    ResourceLimit,
    /// The peer rejected the canonical envelope or returned a response that violated
    /// the ACK contract. The route degrades without failing the query.
    ContractRejected,
}

/// One step in a remote delivery frame's sender-side reliable-transport lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportEventKind {
    /// The frame was buffered for ack-release + bounded retry and handed to the sink once.
    Sent,
    /// A buffered, still-unacked frame was re-handed to the sink on a retry tick.
    Retried,
    /// An acknowledgement arrived and released the buffered frame; carries the peer's
    /// accept status (`Accepted` / `Duplicate` / `Rejected`).
    Acked(RuntimeFilterAcceptStatus),
    /// The route degraded without erroring the query: a deadline drop or a resource-limit
    /// refusal.
    FailedOpen(TransportFailOpenReason),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeFilterEvent {
    DeploymentInstalled {
        query_id: UniqueId,
        participant_id: RuntimeFilterParticipantId,
        epoch: DeploymentEpoch,
    },
    ChannelPlanned {
        identity: RuntimeFilterEventIdentity,
    },
    DeltaAccepted {
        identity: ContributionIdentity,
    },
    DeltaDuplicateIgnored {
        identity: ContributionIdentity,
    },
    FinalDomainShardAccepted {
        identity: ContributionIdentity,
    },
    FinalDomainShardDuplicate {
        identity: ContributionIdentity,
    },
    FinalDomainShardRejected {
        identity: ContributionIdentity,
        rejection: FinalDomainRejectionKind,
    },
    OrderedUpdateStale {
        identity: ContributionIdentity,
    },
    OrderedUpdateApplied {
        identity: ContributionIdentity,
    },
    OrderedUpdateRejected {
        identity: ContributionIdentity,
        violation: RuntimeContractViolationKind,
    },
    OrderedUpdateEqual {
        identity: ContributionIdentity,
    },
    OrderedStreamTightened {
        identity: ContributionIdentity,
    },
    TopKSummaryStale {
        identity: ContributionIdentity,
    },
    TopKSummaryApplied {
        identity: ContributionIdentity,
    },
    TopKSummaryRejected {
        identity: ContributionIdentity,
        violation: RuntimeContractViolationKind,
    },
    TopKSummaryEqual {
        identity: ContributionIdentity,
    },
    TopKStreamUpdated {
        identity: ContributionIdentity,
    },
    OrderedGlobalTightened {
        identity: ContributionIdentity,
        version: LogicalVersion,
    },
    OrderedAvailabilityReached {
        identity: RuntimeFilterEventIdentity,
    },
    LogicalVersionPublished {
        identity: RuntimeFilterEventIdentity,
        version: LogicalVersion,
    },
    SequenceGapObserved {
        identity: ContributionIdentity,
    },
    ProducerInstanceClosed {
        identity: ProducerEventIdentity,
    },
    ProducerInstanceFailed {
        identity: ProducerEventIdentity,
        reason: ProducerFailureReason,
    },
    ChannelCompleted {
        identity: RuntimeFilterEventIdentity,
        version: LogicalVersion,
    },
    ChannelCompletedWithoutArtifact {
        identity: RuntimeFilterEventIdentity,
    },
    ChannelLogicalDegraded {
        identity: RuntimeFilterEventIdentity,
        reason: UnavailableReason,
        retained_version: LogicalVersion,
    },
    ChannelUnavailable {
        identity: RuntimeFilterEventIdentity,
        reason: UnavailableReason,
    },
    ChannelCancelled {
        identity: RuntimeFilterEventIdentity,
    },
    MaterializationStarted {
        identity: ArtifactMaterializationIdentity,
    },
    ArtifactMaterialized {
        identity: ArtifactMaterializationIdentity,
        kind: ArtifactKind,
        bytes: usize,
        digest: [u8; 32],
    },
    ArtifactPublished {
        identity: ArtifactMaterializationIdentity,
        kind: ArtifactKind,
        bytes: usize,
        digest: [u8; 32],
    },
    ArtifactPublishStaleSkipped {
        identity: ArtifactMaterializationIdentity,
    },
    ArtifactUnsupported {
        identity: ArtifactMaterializationIdentity,
        reason: ArtifactUnsupportedReason,
    },
    ArtifactUnavailable {
        identity: ArtifactMaterializationIdentity,
        reason: UnavailableReason,
    },
    LoopbackDelivered {
        identity: RouteEventIdentity,
        version: LogicalVersion,
    },
    /// A sender-side reliable-transport lifecycle step for one remote delivery route:
    /// the frame was sent / retried / acked / failed open. `bytes` is the serialized
    /// frame payload length the event concerns (a broadcast frame shared across routes
    /// carries its own length on each route's event).
    TransportEnvelope {
        identity: TransportRouteEventIdentity,
        kind: TransportEventKind,
        bytes: usize,
    },
    SubscriptionAcquired {
        identity: ConsumerEventIdentity,
        version: LogicalVersion,
    },
    SubscriptionTimedOut {
        identity: ConsumerEventIdentity,
    },
    SubscriptionUnavailable {
        identity: ConsumerEventIdentity,
        reason: UnavailableReason,
    },
    SubscriptionUnsupported {
        identity: ConsumerEventIdentity,
        reason: ArtifactUnsupportedReason,
    },
    SubscriptionCancelled {
        identity: ConsumerEventIdentity,
    },
    LiveSubscriptionUpdated {
        identity: ConsumerEventIdentity,
        version: LogicalVersion,
        terminal: Option<LiveTerminal>,
    },
    LiveSubscriptionIdle {
        identity: ConsumerEventIdentity,
        latest_version: Option<LogicalVersion>,
        terminal: Option<LiveTerminal>,
    },
    LiveSubscriptionTerminal {
        identity: ConsumerEventIdentity,
        terminal: LiveTerminal,
        retained_version: Option<LogicalVersion>,
    },
}

pub trait RuntimeFilterEventSink: Send + Sync {
    fn record(&self, event: RuntimeFilterEvent);
}

#[cfg(test)]
mod tests {
    use crate::common::types::UniqueId;
    use crate::runtime_filter::model::contract::{BindingId, ChannelId};
    use crate::runtime_filter::port::identity::*;

    use super::*;

    #[test]
    fn event_identity_keeps_query_participant_channel_epoch_and_route_coordinates() {
        let common = RuntimeFilterEventIdentity::new(
            UniqueId::new(1, 2),
            RuntimeFilterParticipantId::new(3),
            ChannelId::new(4),
            DeploymentEpoch::new(5),
        );
        let route = RouteEventIdentity::new(
            common,
            BindingId::new(6),
            UniqueId::new(7, 8),
            RouteEdgeId::new(9),
        );
        let event = RuntimeFilterEvent::LoopbackDelivered {
            identity: route,
            version: LogicalVersion::FIRST,
        };

        let RuntimeFilterEvent::LoopbackDelivered { identity, version } = event else {
            panic!("expected loopback delivery event");
        };
        assert_eq!(identity.common(), common);
        assert_eq!(identity.consumer_binding_id().get(), 6);
        assert_eq!(identity.fragment_instance_id(), UniqueId::new(7, 8));
        assert_eq!(identity.route_edge_id().get(), 9);
        assert_eq!(version, LogicalVersion::FIRST);
    }
}
