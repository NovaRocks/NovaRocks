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
use std::error::Error;
use std::fmt;

use crate::common::types::UniqueId;
use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime_filter::model::contract::{BindingId, ChannelId};
use crate::runtime_filter::port::identity::{
    DeploymentEpoch, RouteEdgeId, RuntimeFilterParticipantId,
};
use crate::runtime_filter::port::transport::RuntimeFilterEnvelopeKind;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum RuntimeFilterRouteRole {
    Producer(BindingId),
    Aggregator,
    Relay,
    Consumer(BindingId),
}

pub fn canonical_route_allowed_kinds(
    source: RuntimeFilterRouteRole,
    target: RuntimeFilterRouteRole,
) -> Option<BTreeSet<RuntimeFilterEnvelopeKind>> {
    match (source, target) {
        (RuntimeFilterRouteRole::Producer(_), RuntimeFilterRouteRole::Consumer(_))
        | (RuntimeFilterRouteRole::Aggregator, RuntimeFilterRouteRole::Consumer(_)) => {
            Some(BTreeSet::from([
                RuntimeFilterEnvelopeKind::Artifact,
                RuntimeFilterEnvelopeKind::FinalArtifact,
                RuntimeFilterEnvelopeKind::Unavailable,
                RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
                RuntimeFilterEnvelopeKind::DegradedLogical,
            ]))
        }
        (RuntimeFilterRouteRole::Producer(_), RuntimeFilterRouteRole::Aggregator) => {
            Some(BTreeSet::from([
                RuntimeFilterEnvelopeKind::Contribution,
                RuntimeFilterEnvelopeKind::ProducerClosed,
                RuntimeFilterEnvelopeKind::ProducerUnavailable,
            ]))
        }
        _ => None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterRouteEndpointView {
    participant_id: RuntimeFilterParticipantId,
    role: RuntimeFilterRouteRole,
}

impl RuntimeFilterRouteEndpointView {
    pub const fn new(
        participant_id: RuntimeFilterParticipantId,
        role: RuntimeFilterRouteRole,
    ) -> Self {
        Self {
            participant_id,
            role,
        }
    }

    pub const fn participant_id(&self) -> RuntimeFilterParticipantId {
        self.participant_id
    }

    pub const fn role(&self) -> RuntimeFilterRouteRole {
        self.role
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeFilterRoutePeer {
    Loopback,
    Remote {
        participant_id: RuntimeFilterParticipantId,
        endpoint: RuntimeEndpoint,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterRoutingEdgeView {
    channel_id: ChannelId,
    route_edge_id: RouteEdgeId,
    source: RuntimeFilterRouteEndpointView,
    target: RuntimeFilterRouteEndpointView,
    peer: RuntimeFilterRoutePeer,
    allowed_kinds: BTreeSet<RuntimeFilterEnvelopeKind>,
}

impl RuntimeFilterRoutingEdgeView {
    pub fn new(
        channel_id: ChannelId,
        route_edge_id: RouteEdgeId,
        source: RuntimeFilterRouteEndpointView,
        target: RuntimeFilterRouteEndpointView,
        peer: RuntimeFilterRoutePeer,
        allowed_kinds: BTreeSet<RuntimeFilterEnvelopeKind>,
    ) -> Result<Self, RuntimeFilterRouteContractError> {
        reject_zero(channel_id.get(), "channel id")?;
        reject_zero(route_edge_id.get(), "route edge id")?;
        if allowed_kinds.is_empty() {
            return Err(RuntimeFilterRouteContractError::EmptyAllowedKinds {
                edge: route_edge_id,
            });
        }
        if allowed_kinds.contains(&RuntimeFilterEnvelopeKind::Ack) {
            return Err(RuntimeFilterRouteContractError::AckIsNotDataRoutable {
                edge: route_edge_id,
            });
        }
        match &peer {
            RuntimeFilterRoutePeer::Loopback => {
                if source.participant_id != target.participant_id {
                    return Err(RuntimeFilterRouteContractError::InvalidPeer {
                        edge: route_edge_id,
                    });
                }
            }
            RuntimeFilterRoutePeer::Remote { participant_id, .. } => {
                if source.participant_id == target.participant_id
                    || (*participant_id != source.participant_id
                        && *participant_id != target.participant_id)
                {
                    return Err(RuntimeFilterRouteContractError::InvalidPeer {
                        edge: route_edge_id,
                    });
                }
            }
        }

        Ok(Self {
            channel_id,
            route_edge_id,
            source,
            target,
            peer,
            allowed_kinds,
        })
    }

    pub const fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    pub const fn route_edge_id(&self) -> RouteEdgeId {
        self.route_edge_id
    }

    pub const fn source(&self) -> &RuntimeFilterRouteEndpointView {
        &self.source
    }

    pub const fn target(&self) -> &RuntimeFilterRouteEndpointView {
        &self.target
    }

    pub const fn peer(&self) -> &RuntimeFilterRoutePeer {
        &self.peer
    }

    pub const fn allowed_kinds(&self) -> &BTreeSet<RuntimeFilterEnvelopeKind> {
        &self.allowed_kinds
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterChannelRoutingView {
    channel_id: ChannelId,
    local_roles: BTreeSet<RuntimeFilterRouteRole>,
    producer_instances: BTreeMap<(BindingId, UniqueId), RuntimeFilterParticipantId>,
    inbound_edges: Vec<RuntimeFilterRoutingEdgeView>,
    outbound_edges: Vec<RuntimeFilterRoutingEdgeView>,
}

impl RuntimeFilterChannelRoutingView {
    pub fn new(
        channel_id: ChannelId,
        local_roles: BTreeSet<RuntimeFilterRouteRole>,
        producer_instances: BTreeMap<(BindingId, UniqueId), RuntimeFilterParticipantId>,
        mut inbound_edges: Vec<RuntimeFilterRoutingEdgeView>,
        mut outbound_edges: Vec<RuntimeFilterRoutingEdgeView>,
    ) -> Result<Self, RuntimeFilterRouteContractError> {
        reject_zero(channel_id.get(), "channel id")?;
        validate_channel_edges(channel_id, &mut inbound_edges)?;
        validate_channel_edges(channel_id, &mut outbound_edges)?;

        Ok(Self {
            channel_id,
            local_roles,
            producer_instances,
            inbound_edges,
            outbound_edges,
        })
    }

    pub const fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    pub const fn local_roles(&self) -> &BTreeSet<RuntimeFilterRouteRole> {
        &self.local_roles
    }

    pub fn producer_participant(
        &self,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
    ) -> Option<RuntimeFilterParticipantId> {
        self.producer_instances
            .get(&(binding_id, fragment_instance_id))
            .copied()
    }

    pub const fn producer_instances(
        &self,
    ) -> &BTreeMap<(BindingId, UniqueId), RuntimeFilterParticipantId> {
        &self.producer_instances
    }

    pub fn inbound_edges(&self) -> &[RuntimeFilterRoutingEdgeView] {
        &self.inbound_edges
    }

    pub fn outbound_edges(&self) -> &[RuntimeFilterRoutingEdgeView] {
        &self.outbound_edges
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterRoutingShard {
    deployment_epoch: DeploymentEpoch,
    local_participant_id: RuntimeFilterParticipantId,
    channels: BTreeMap<ChannelId, RuntimeFilterChannelRoutingView>,
}

impl RuntimeFilterRoutingShard {
    pub fn new(
        deployment_epoch: DeploymentEpoch,
        local_participant_id: RuntimeFilterParticipantId,
        channels: BTreeMap<ChannelId, RuntimeFilterChannelRoutingView>,
    ) -> Result<Self, RuntimeFilterRouteContractError> {
        reject_zero(deployment_epoch.get(), "deployment epoch")?;
        for (channel_key, channel) in &channels {
            if *channel_key != channel.channel_id {
                return Err(RuntimeFilterRouteContractError::ChannelKeyMismatch {
                    key: *channel_key,
                    view: channel.channel_id,
                });
            }
            validate_incident_edges(local_participant_id, channel)?;
        }

        Ok(Self {
            deployment_epoch,
            local_participant_id,
            channels,
        })
    }

    pub const fn deployment_epoch(&self) -> DeploymentEpoch {
        self.deployment_epoch
    }

    pub const fn local_participant_id(&self) -> RuntimeFilterParticipantId {
        self.local_participant_id
    }

    pub fn channel(&self, channel_id: ChannelId) -> Option<&RuntimeFilterChannelRoutingView> {
        self.channels.get(&channel_id)
    }

    pub const fn channels(&self) -> &BTreeMap<ChannelId, RuntimeFilterChannelRoutingView> {
        &self.channels
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterProducerRouteIntent {
    deployment_epoch: DeploymentEpoch,
    channel_id: ChannelId,
    producer_binding_id: BindingId,
    envelope_kind: RuntimeFilterEnvelopeKind,
}

impl RuntimeFilterProducerRouteIntent {
    pub fn new(
        deployment_epoch: DeploymentEpoch,
        channel_id: ChannelId,
        producer_binding_id: BindingId,
        envelope_kind: RuntimeFilterEnvelopeKind,
    ) -> Result<Self, RuntimeFilterRouteContractError> {
        reject_zero(deployment_epoch.get(), "deployment epoch")?;
        reject_zero(channel_id.get(), "channel id")?;
        let role = RuntimeFilterRouteRole::Producer(producer_binding_id);
        if !matches!(
            envelope_kind,
            RuntimeFilterEnvelopeKind::Contribution
                | RuntimeFilterEnvelopeKind::ProducerClosed
                | RuntimeFilterEnvelopeKind::ProducerUnavailable
        ) {
            return Err(RuntimeFilterRouteContractError::ForbiddenOutboundKind {
                channel: channel_id,
                role,
                kind: envelope_kind,
            });
        }
        Ok(Self {
            deployment_epoch,
            channel_id,
            producer_binding_id,
            envelope_kind,
        })
    }

    pub const fn deployment_epoch(&self) -> DeploymentEpoch {
        self.deployment_epoch
    }

    pub const fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    pub const fn producer_binding_id(&self) -> BindingId {
        self.producer_binding_id
    }

    pub const fn envelope_kind(&self) -> RuntimeFilterEnvelopeKind {
        self.envelope_kind
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterDeliveryRouteIntent {
    deployment_epoch: DeploymentEpoch,
    channel_id: ChannelId,
    route_edge_ids: Vec<RouteEdgeId>,
    envelope_kind: RuntimeFilterEnvelopeKind,
}

impl RuntimeFilterDeliveryRouteIntent {
    pub fn new(
        deployment_epoch: DeploymentEpoch,
        channel_id: ChannelId,
        mut route_edge_ids: Vec<RouteEdgeId>,
        envelope_kind: RuntimeFilterEnvelopeKind,
    ) -> Result<Self, RuntimeFilterRouteContractError> {
        reject_zero(deployment_epoch.get(), "deployment epoch")?;
        reject_zero(channel_id.get(), "channel id")?;
        if !matches!(
            envelope_kind,
            RuntimeFilterEnvelopeKind::Artifact
                | RuntimeFilterEnvelopeKind::FinalArtifact
                | RuntimeFilterEnvelopeKind::Unavailable
                | RuntimeFilterEnvelopeKind::CompletedWithoutArtifact
                | RuntimeFilterEnvelopeKind::DegradedLogical
        ) {
            return Err(RuntimeFilterRouteContractError::ForbiddenDeliveryKind {
                channel: channel_id,
                kind: envelope_kind,
            });
        }
        route_edge_ids.sort_unstable();
        let mut previous = None;
        for edge in &route_edge_ids {
            reject_zero(edge.get(), "route edge id")?;
            if previous == Some(*edge) {
                return Err(
                    RuntimeFilterRouteContractError::DuplicateRequestedRouteEdge { edge: *edge },
                );
            }
            previous = Some(*edge);
        }
        Ok(Self {
            deployment_epoch,
            channel_id,
            route_edge_ids,
            envelope_kind,
        })
    }

    pub const fn deployment_epoch(&self) -> DeploymentEpoch {
        self.deployment_epoch
    }

    pub const fn channel_id(&self) -> ChannelId {
        self.channel_id
    }

    pub fn route_edge_ids(&self) -> &[RouteEdgeId] {
        &self.route_edge_ids
    }

    pub const fn envelope_kind(&self) -> RuntimeFilterEnvelopeKind {
        self.envelope_kind
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterRemoteRoute {
    route_edge_id: RouteEdgeId,
    peer_participant_id: RuntimeFilterParticipantId,
    endpoint: RuntimeEndpoint,
    target_role: RuntimeFilterRouteRole,
}

impl RuntimeFilterRemoteRoute {
    pub fn new(
        route_edge_id: RouteEdgeId,
        peer_participant_id: RuntimeFilterParticipantId,
        endpoint: RuntimeEndpoint,
        target_role: RuntimeFilterRouteRole,
    ) -> Result<Self, RuntimeFilterRouteContractError> {
        reject_zero(route_edge_id.get(), "route edge id")?;
        Ok(Self {
            route_edge_id,
            peer_participant_id,
            endpoint,
            target_role,
        })
    }

    pub const fn route_edge_id(&self) -> RouteEdgeId {
        self.route_edge_id
    }

    pub const fn peer_participant_id(&self) -> RuntimeFilterParticipantId {
        self.peer_participant_id
    }

    pub const fn endpoint(&self) -> &RuntimeEndpoint {
        &self.endpoint
    }

    pub const fn target_role(&self) -> RuntimeFilterRouteRole {
        self.target_role
    }

    /// Deterministic retained charge for the per-pending-entry route clone.
    pub fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(self.endpoint.retained_host_capacity())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeFilterRouteDecision {
    loopback_route_edge_ids: Vec<RouteEdgeId>,
    remote_routes: Vec<RuntimeFilterRemoteRoute>,
}

impl RuntimeFilterRouteDecision {
    pub fn new(
        mut loopback_route_edge_ids: Vec<RouteEdgeId>,
        mut remote_routes: Vec<RuntimeFilterRemoteRoute>,
    ) -> Result<Self, RuntimeFilterRouteContractError> {
        loopback_route_edge_ids.sort_unstable();
        remote_routes.sort_unstable_by_key(RuntimeFilterRemoteRoute::route_edge_id);
        validate_route_edge_ids(loopback_route_edge_ids.iter().copied(), "route edge id")?;
        validate_route_edge_ids(
            remote_routes
                .iter()
                .map(RuntimeFilterRemoteRoute::route_edge_id),
            "route edge id",
        )?;
        let remote_route_edge_ids = remote_routes
            .iter()
            .map(RuntimeFilterRemoteRoute::route_edge_id)
            .collect::<BTreeSet<_>>();
        if let Some(edge) = loopback_route_edge_ids
            .iter()
            .find(|edge| remote_route_edge_ids.contains(edge))
        {
            return Err(RuntimeFilterRouteContractError::DuplicateRouteEdge { edge: *edge });
        }
        Ok(Self {
            loopback_route_edge_ids,
            remote_routes,
        })
    }

    pub fn loopback_route_edge_ids(&self) -> &[RouteEdgeId] {
        &self.loopback_route_edge_ids
    }

    pub fn remote_routes(&self) -> &[RuntimeFilterRemoteRoute] {
        &self.remote_routes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeFilterRouteContractError {
    ZeroIdentity(&'static str),
    ChannelKeyMismatch {
        key: ChannelId,
        view: ChannelId,
    },
    EdgeChannelMismatch {
        channel: ChannelId,
        edge: RouteEdgeId,
    },
    DuplicateRouteEdge {
        edge: RouteEdgeId,
    },
    DuplicateRequestedRouteEdge {
        edge: RouteEdgeId,
    },
    EmptyAllowedKinds {
        edge: RouteEdgeId,
    },
    AckIsNotDataRoutable {
        edge: RouteEdgeId,
    },
    InvalidPeer {
        edge: RouteEdgeId,
    },
    InvalidIncidentEdge {
        channel: ChannelId,
        edge: RouteEdgeId,
        detail: &'static str,
    },
    StaleEpoch {
        installed: DeploymentEpoch,
        incoming: DeploymentEpoch,
    },
    UnknownChannel {
        channel: ChannelId,
    },
    UnknownSourceRole {
        channel: ChannelId,
        role: RuntimeFilterRouteRole,
    },
    ForbiddenOutboundKind {
        channel: ChannelId,
        role: RuntimeFilterRouteRole,
        kind: RuntimeFilterEnvelopeKind,
    },
    ForbiddenDeliveryKind {
        channel: ChannelId,
        kind: RuntimeFilterEnvelopeKind,
    },
    UnknownOutboundRoute {
        channel: ChannelId,
        edge: RouteEdgeId,
    },
    AmbiguousOutboundRoute {
        channel: ChannelId,
        role: RuntimeFilterRouteRole,
    },
    UnknownProducerInstance {
        channel: ChannelId,
        binding: BindingId,
        fragment_instance_id: UniqueId,
    },
    UnknownInboundProducerRoute {
        channel: ChannelId,
        binding: BindingId,
        source_participant: RuntimeFilterParticipantId,
    },
    UnknownInboundRoute {
        channel: ChannelId,
        edge: RouteEdgeId,
    },
    AmbiguousInboundRoute {
        channel: ChannelId,
    },
    ForbiddenInboundKind {
        channel: ChannelId,
        edge: RouteEdgeId,
        kind: RuntimeFilterEnvelopeKind,
    },
    InboundTargetMismatch {
        channel: ChannelId,
        edge: RouteEdgeId,
        local_participant: RuntimeFilterParticipantId,
    },
}

impl fmt::Display for RuntimeFilterRouteContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid runtime filter routing contract: {self:?}"
        )
    }
}

impl Error for RuntimeFilterRouteContractError {}

fn reject_zero(
    raw: impl Into<u64>,
    identity: &'static str,
) -> Result<(), RuntimeFilterRouteContractError> {
    if raw.into() == 0 {
        Err(RuntimeFilterRouteContractError::ZeroIdentity(identity))
    } else {
        Ok(())
    }
}

fn validate_channel_edges(
    channel_id: ChannelId,
    edges: &mut [RuntimeFilterRoutingEdgeView],
) -> Result<(), RuntimeFilterRouteContractError> {
    edges.sort_unstable_by_key(RuntimeFilterRoutingEdgeView::route_edge_id);
    let mut previous = None;
    for edge in edges {
        if edge.channel_id != channel_id {
            return Err(RuntimeFilterRouteContractError::EdgeChannelMismatch {
                channel: channel_id,
                edge: edge.route_edge_id,
            });
        }
        if previous == Some(edge.route_edge_id) {
            return Err(RuntimeFilterRouteContractError::DuplicateRouteEdge {
                edge: edge.route_edge_id,
            });
        }
        previous = Some(edge.route_edge_id);
    }
    Ok(())
}

fn validate_route_edge_ids(
    route_edge_ids: impl IntoIterator<Item = RouteEdgeId>,
    identity: &'static str,
) -> Result<(), RuntimeFilterRouteContractError> {
    let mut previous = None;
    for edge in route_edge_ids {
        reject_zero(edge.get(), identity)?;
        if previous == Some(edge) {
            return Err(RuntimeFilterRouteContractError::DuplicateRouteEdge { edge });
        }
        previous = Some(edge);
    }
    Ok(())
}

fn validate_incident_edges(
    local_participant_id: RuntimeFilterParticipantId,
    channel: &RuntimeFilterChannelRoutingView,
) -> Result<(), RuntimeFilterRouteContractError> {
    let outbound = channel
        .outbound_edges
        .iter()
        .map(|edge| (edge.route_edge_id, edge))
        .collect::<BTreeMap<_, _>>();
    let inbound = channel
        .inbound_edges
        .iter()
        .map(|edge| (edge.route_edge_id, edge))
        .collect::<BTreeMap<_, _>>();

    for edge in &channel.outbound_edges {
        if edge.source.participant_id != local_participant_id {
            return invalid_incident(channel.channel_id, edge, "outbound source is not local");
        }
        if !channel.local_roles.contains(&edge.source.role) {
            return invalid_incident(
                channel.channel_id,
                edge,
                "outbound source role is not local",
            );
        }
        validate_relative_peer(local_participant_id, channel.channel_id, edge, true)?;
    }
    for edge in &channel.inbound_edges {
        if edge.target.participant_id != local_participant_id {
            return invalid_incident(channel.channel_id, edge, "inbound target is not local");
        }
        if !channel.local_roles.contains(&edge.target.role) {
            return invalid_incident(channel.channel_id, edge, "inbound target role is not local");
        }
        validate_relative_peer(local_participant_id, channel.channel_id, edge, false)?;
    }

    for (edge_id, outbound_edge) in &outbound {
        if let Some(inbound_edge) = inbound.get(edge_id) {
            let self_edge = outbound_edge.source.participant_id == local_participant_id
                && outbound_edge.target.participant_id == local_participant_id;
            if !self_edge || *outbound_edge != *inbound_edge {
                return invalid_incident(
                    channel.channel_id,
                    outbound_edge,
                    "cross-side route edge is not an identical self edge",
                );
            }
        } else if outbound_edge.target.participant_id == local_participant_id {
            return invalid_incident(
                channel.channel_id,
                outbound_edge,
                "self edge is missing from inbound routes",
            );
        }
    }
    for (edge_id, inbound_edge) in &inbound {
        if !outbound.contains_key(edge_id)
            && inbound_edge.source.participant_id == local_participant_id
        {
            return invalid_incident(
                channel.channel_id,
                inbound_edge,
                "self edge is missing from outbound routes",
            );
        }
    }
    Ok(())
}

fn validate_relative_peer(
    local_participant_id: RuntimeFilterParticipantId,
    channel_id: ChannelId,
    edge: &RuntimeFilterRoutingEdgeView,
    outbound: bool,
) -> Result<(), RuntimeFilterRouteContractError> {
    let peer_participant_id = if outbound {
        edge.target.participant_id
    } else {
        edge.source.participant_id
    };
    let valid = match &edge.peer {
        RuntimeFilterRoutePeer::Loopback => peer_participant_id == local_participant_id,
        RuntimeFilterRoutePeer::Remote { participant_id, .. } => {
            peer_participant_id != local_participant_id && *participant_id == peer_participant_id
        }
    };
    if valid {
        Ok(())
    } else {
        invalid_incident(channel_id, edge, "peer does not match the incident edge")
    }
}

fn invalid_incident<T>(
    channel: ChannelId,
    edge: &RuntimeFilterRoutingEdgeView,
    detail: &'static str,
) -> Result<T, RuntimeFilterRouteContractError> {
    Err(RuntimeFilterRouteContractError::InvalidIncidentEdge {
        channel,
        edge: edge.route_edge_id,
        detail,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use crate::common::types::UniqueId;
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::runtime_filter::model::contract::{BindingId, ChannelId};
    use crate::runtime_filter::port::identity::{
        DeploymentEpoch, RouteEdgeId, RuntimeFilterParticipantId,
    };
    use crate::runtime_filter::port::transport::RuntimeFilterEnvelopeKind;

    use super::*;

    fn endpoint(
        participant_id: u32,
        role: RuntimeFilterRouteRole,
    ) -> RuntimeFilterRouteEndpointView {
        RuntimeFilterRouteEndpointView::new(RuntimeFilterParticipantId::new(participant_id), role)
    }

    fn edge(
        edge_id: u32,
        source_participant: u32,
        source_role: RuntimeFilterRouteRole,
        target_participant: u32,
        target_role: RuntimeFilterRouteRole,
        peer: RuntimeFilterRoutePeer,
    ) -> RuntimeFilterRoutingEdgeView {
        RuntimeFilterRoutingEdgeView::new(
            ChannelId::new(1),
            RouteEdgeId::new(edge_id),
            endpoint(source_participant, source_role),
            endpoint(target_participant, target_role),
            peer,
            BTreeSet::from([
                RuntimeFilterEnvelopeKind::Contribution,
                RuntimeFilterEnvelopeKind::ProducerClosed,
                RuntimeFilterEnvelopeKind::ProducerUnavailable,
            ]),
        )
        .unwrap()
    }

    fn channel(
        inbound_edges: Vec<RuntimeFilterRoutingEdgeView>,
        outbound_edges: Vec<RuntimeFilterRoutingEdgeView>,
    ) -> RuntimeFilterChannelRoutingView {
        RuntimeFilterChannelRoutingView::new(
            ChannelId::new(1),
            BTreeSet::from([
                RuntimeFilterRouteRole::Producer(BindingId::new(10)),
                RuntimeFilterRouteRole::Aggregator,
            ]),
            BTreeMap::from([(
                (BindingId::new(10), UniqueId::new(1, 2)),
                RuntimeFilterParticipantId::new(2),
            )]),
            inbound_edges,
            outbound_edges,
        )
        .unwrap()
    }

    #[test]
    fn routing_shard_preserves_roles_edges_instances_and_epoch() {
        let edge = RuntimeFilterRoutingEdgeView::new(
            ChannelId::new(1),
            RouteEdgeId::new(9),
            RuntimeFilterRouteEndpointView::new(
                RuntimeFilterParticipantId::new(2),
                RuntimeFilterRouteRole::Producer(BindingId::new(10)),
            ),
            RuntimeFilterRouteEndpointView::new(
                RuntimeFilterParticipantId::new(7),
                RuntimeFilterRouteRole::Aggregator,
            ),
            RuntimeFilterRoutePeer::Remote {
                participant_id: RuntimeFilterParticipantId::new(7),
                endpoint: RuntimeEndpoint::new("be-7", 9060).unwrap(),
            },
            BTreeSet::from([
                RuntimeFilterEnvelopeKind::Contribution,
                RuntimeFilterEnvelopeKind::ProducerClosed,
                RuntimeFilterEnvelopeKind::ProducerUnavailable,
            ]),
        )
        .unwrap();

        let channel = RuntimeFilterChannelRoutingView::new(
            ChannelId::new(1),
            BTreeSet::from([RuntimeFilterRouteRole::Producer(BindingId::new(10))]),
            BTreeMap::from([(
                (BindingId::new(10), UniqueId::new(1, 2)),
                RuntimeFilterParticipantId::new(2),
            )]),
            Vec::new(),
            vec![edge.clone()],
        )
        .unwrap();
        let shard = RuntimeFilterRoutingShard::new(
            DeploymentEpoch::new(3),
            RuntimeFilterParticipantId::new(2),
            BTreeMap::from([(ChannelId::new(1), channel)]),
        )
        .unwrap();

        assert_eq!(shard.deployment_epoch(), DeploymentEpoch::new(3));
        assert_eq!(
            shard.local_participant_id(),
            RuntimeFilterParticipantId::new(2)
        );
        let channel = shard.channel(ChannelId::new(1)).unwrap();
        assert_eq!(channel.local_roles().len(), 1);
        assert_eq!(
            channel.producer_participant(BindingId::new(10), UniqueId::new(1, 2)),
            Some(RuntimeFilterParticipantId::new(2))
        );
        assert_eq!(channel.outbound_edges(), &[edge]);
    }

    #[test]
    fn canonical_delivery_family_includes_exact_terminal_kinds() {
        let expected = BTreeSet::from([
            RuntimeFilterEnvelopeKind::Artifact,
            RuntimeFilterEnvelopeKind::Unavailable,
            RuntimeFilterEnvelopeKind::CompletedWithoutArtifact,
            RuntimeFilterEnvelopeKind::DegradedLogical,
            RuntimeFilterEnvelopeKind::FinalArtifact,
        ]);
        assert_eq!(
            canonical_route_allowed_kinds(
                RuntimeFilterRouteRole::Producer(BindingId::new(10)),
                RuntimeFilterRouteRole::Consumer(BindingId::new(20)),
            ),
            Some(expected.clone())
        );
        assert_eq!(
            canonical_route_allowed_kinds(
                RuntimeFilterRouteRole::Aggregator,
                RuntimeFilterRouteRole::Consumer(BindingId::new(20)),
            ),
            Some(expected)
        );
    }

    #[test]
    fn routing_contract_rejects_zero_ids_wrong_channel_and_ack_data_edges() {
        let source = endpoint(2, RuntimeFilterRouteRole::Producer(BindingId::new(10)));
        let target = endpoint(7, RuntimeFilterRouteRole::Aggregator);
        let remote = RuntimeFilterRoutePeer::Remote {
            participant_id: RuntimeFilterParticipantId::new(7),
            endpoint: RuntimeEndpoint::new("be-7", 9060).unwrap(),
        };

        assert_eq!(
            RuntimeFilterRoutingEdgeView::new(
                ChannelId::new(1),
                RouteEdgeId::new(0),
                source.clone(),
                target.clone(),
                remote.clone(),
                BTreeSet::from([RuntimeFilterEnvelopeKind::Contribution]),
            )
            .unwrap_err(),
            RuntimeFilterRouteContractError::ZeroIdentity("route edge id")
        );
        assert_eq!(
            RuntimeFilterRoutingEdgeView::new(
                ChannelId::new(0),
                RouteEdgeId::new(1),
                source.clone(),
                target.clone(),
                remote.clone(),
                BTreeSet::from([RuntimeFilterEnvelopeKind::Contribution]),
            )
            .unwrap_err(),
            RuntimeFilterRouteContractError::ZeroIdentity("channel id")
        );
        assert_eq!(
            RuntimeFilterRoutingEdgeView::new(
                ChannelId::new(1),
                RouteEdgeId::new(1),
                source.clone(),
                target.clone(),
                remote.clone(),
                BTreeSet::new(),
            )
            .unwrap_err(),
            RuntimeFilterRouteContractError::EmptyAllowedKinds {
                edge: RouteEdgeId::new(1)
            }
        );
        assert_eq!(
            RuntimeFilterRoutingEdgeView::new(
                ChannelId::new(1),
                RouteEdgeId::new(1),
                source,
                target,
                remote,
                BTreeSet::from([RuntimeFilterEnvelopeKind::Ack]),
            )
            .unwrap_err(),
            RuntimeFilterRouteContractError::AckIsNotDataRoutable {
                edge: RouteEdgeId::new(1)
            }
        );

        let wrong_channel_edge = RuntimeFilterRoutingEdgeView::new(
            ChannelId::new(2),
            RouteEdgeId::new(1),
            endpoint(2, RuntimeFilterRouteRole::Producer(BindingId::new(10))),
            endpoint(7, RuntimeFilterRouteRole::Aggregator),
            RuntimeFilterRoutePeer::Remote {
                participant_id: RuntimeFilterParticipantId::new(7),
                endpoint: RuntimeEndpoint::new("be-7", 9060).unwrap(),
            },
            BTreeSet::from([RuntimeFilterEnvelopeKind::Contribution]),
        )
        .unwrap();
        assert_eq!(
            RuntimeFilterChannelRoutingView::new(
                ChannelId::new(1),
                BTreeSet::from([RuntimeFilterRouteRole::Producer(BindingId::new(10))]),
                BTreeMap::new(),
                Vec::new(),
                vec![wrong_channel_edge],
            )
            .unwrap_err(),
            RuntimeFilterRouteContractError::EdgeChannelMismatch {
                channel: ChannelId::new(1),
                edge: RouteEdgeId::new(1)
            }
        );

        let view = RuntimeFilterChannelRoutingView::new(
            ChannelId::new(1),
            BTreeSet::new(),
            BTreeMap::new(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        assert_eq!(
            RuntimeFilterRoutingShard::new(
                DeploymentEpoch::new(1),
                RuntimeFilterParticipantId::new(2),
                BTreeMap::from([(ChannelId::new(2), view)]),
            )
            .unwrap_err(),
            RuntimeFilterRouteContractError::ChannelKeyMismatch {
                key: ChannelId::new(2),
                view: ChannelId::new(1)
            }
        );
        assert_eq!(
            RuntimeFilterRoutingShard::new(
                DeploymentEpoch::new(0),
                RuntimeFilterParticipantId::new(2),
                BTreeMap::new(),
            )
            .unwrap_err(),
            RuntimeFilterRouteContractError::ZeroIdentity("deployment epoch")
        );
    }

    #[test]
    fn routing_channel_sorts_incident_edges_by_route_edge_id() {
        let edge_9 = edge(
            9,
            2,
            RuntimeFilterRouteRole::Producer(BindingId::new(10)),
            7,
            RuntimeFilterRouteRole::Aggregator,
            RuntimeFilterRoutePeer::Remote {
                participant_id: RuntimeFilterParticipantId::new(7),
                endpoint: RuntimeEndpoint::new("be-7", 9060).unwrap(),
            },
        );
        let edge_3 = edge(
            3,
            2,
            RuntimeFilterRouteRole::Producer(BindingId::new(10)),
            8,
            RuntimeFilterRouteRole::Aggregator,
            RuntimeFilterRoutePeer::Remote {
                participant_id: RuntimeFilterParticipantId::new(8),
                endpoint: RuntimeEndpoint::new("be-8", 9060).unwrap(),
            },
        );

        let view = channel(Vec::new(), vec![edge_9, edge_3]);

        assert_eq!(
            view.outbound_edges()
                .iter()
                .map(RuntimeFilterRoutingEdgeView::route_edge_id)
                .collect::<Vec<_>>(),
            vec![RouteEdgeId::new(3), RouteEdgeId::new(9)]
        );
    }

    #[test]
    fn routing_shard_requires_exact_local_incident_edges_and_mirrored_self_edges() {
        let outbound_wrong_source = edge(
            1,
            3,
            RuntimeFilterRouteRole::Producer(BindingId::new(10)),
            7,
            RuntimeFilterRouteRole::Aggregator,
            RuntimeFilterRoutePeer::Remote {
                participant_id: RuntimeFilterParticipantId::new(7),
                endpoint: RuntimeEndpoint::new("be-7", 9060).unwrap(),
            },
        );
        assert!(
            RuntimeFilterRoutingShard::new(
                DeploymentEpoch::new(1),
                RuntimeFilterParticipantId::new(2),
                BTreeMap::from([(
                    ChannelId::new(1),
                    channel(Vec::new(), vec![outbound_wrong_source])
                )]),
            )
            .is_err()
        );

        let outbound_missing_local_role = edge(
            2,
            2,
            RuntimeFilterRouteRole::Producer(BindingId::new(10)),
            7,
            RuntimeFilterRouteRole::Aggregator,
            RuntimeFilterRoutePeer::Remote {
                participant_id: RuntimeFilterParticipantId::new(7),
                endpoint: RuntimeEndpoint::new("be-7", 9060).unwrap(),
            },
        );
        let missing_role_channel = RuntimeFilterChannelRoutingView::new(
            ChannelId::new(1),
            BTreeSet::from([RuntimeFilterRouteRole::Aggregator]),
            BTreeMap::new(),
            Vec::new(),
            vec![outbound_missing_local_role],
        )
        .unwrap();
        assert!(
            RuntimeFilterRoutingShard::new(
                DeploymentEpoch::new(1),
                RuntimeFilterParticipantId::new(2),
                BTreeMap::from([(ChannelId::new(1), missing_role_channel)]),
            )
            .is_err()
        );

        let inbound_wrong_target = edge(
            3,
            7,
            RuntimeFilterRouteRole::Producer(BindingId::new(10)),
            3,
            RuntimeFilterRouteRole::Aggregator,
            RuntimeFilterRoutePeer::Remote {
                participant_id: RuntimeFilterParticipantId::new(7),
                endpoint: RuntimeEndpoint::new("be-7", 9060).unwrap(),
            },
        );
        assert!(
            RuntimeFilterRoutingShard::new(
                DeploymentEpoch::new(1),
                RuntimeFilterParticipantId::new(2),
                BTreeMap::from([(
                    ChannelId::new(1),
                    channel(vec![inbound_wrong_target], Vec::new())
                )]),
            )
            .is_err()
        );

        let self_edge = edge(
            4,
            2,
            RuntimeFilterRouteRole::Producer(BindingId::new(10)),
            2,
            RuntimeFilterRouteRole::Aggregator,
            RuntimeFilterRoutePeer::Loopback,
        );
        assert!(
            RuntimeFilterRoutingShard::new(
                DeploymentEpoch::new(1),
                RuntimeFilterParticipantId::new(2),
                BTreeMap::from([(
                    ChannelId::new(1),
                    channel(Vec::new(), vec![self_edge.clone()])
                )]),
            )
            .is_err()
        );

        assert!(
            RuntimeFilterRoutingShard::new(
                DeploymentEpoch::new(1),
                RuntimeFilterParticipantId::new(2),
                BTreeMap::from([(
                    ChannelId::new(1),
                    channel(vec![self_edge.clone()], Vec::new())
                )]),
            )
            .is_err()
        );

        let mut mismatched_self_edge = self_edge.clone();
        mismatched_self_edge.allowed_kinds =
            BTreeSet::from([RuntimeFilterEnvelopeKind::Contribution]);
        assert!(
            RuntimeFilterRoutingShard::new(
                DeploymentEpoch::new(1),
                RuntimeFilterParticipantId::new(2),
                BTreeMap::from([(
                    ChannelId::new(1),
                    channel(vec![mismatched_self_edge], vec![self_edge.clone()])
                )]),
            )
            .is_err()
        );

        let outbound = edge(
            5,
            2,
            RuntimeFilterRouteRole::Producer(BindingId::new(10)),
            7,
            RuntimeFilterRouteRole::Aggregator,
            RuntimeFilterRoutePeer::Remote {
                participant_id: RuntimeFilterParticipantId::new(7),
                endpoint: RuntimeEndpoint::new("be-7", 9060).unwrap(),
            },
        );
        let inbound = edge(
            5,
            7,
            RuntimeFilterRouteRole::Producer(BindingId::new(10)),
            2,
            RuntimeFilterRouteRole::Aggregator,
            RuntimeFilterRoutePeer::Remote {
                participant_id: RuntimeFilterParticipantId::new(7),
                endpoint: RuntimeEndpoint::new("be-7", 9060).unwrap(),
            },
        );
        assert!(
            RuntimeFilterRoutingShard::new(
                DeploymentEpoch::new(1),
                RuntimeFilterParticipantId::new(2),
                BTreeMap::from([(ChannelId::new(1), channel(vec![inbound], vec![outbound]))]),
            )
            .is_err()
        );

        let valid = RuntimeFilterRoutingShard::new(
            DeploymentEpoch::new(1),
            RuntimeFilterParticipantId::new(2),
            BTreeMap::from([(
                ChannelId::new(1),
                channel(vec![self_edge.clone()], vec![self_edge]),
            )]),
        )
        .unwrap();
        assert_eq!(valid.channels().len(), 1);
    }

    #[test]
    fn routing_channel_rejects_duplicate_edges_within_each_side() {
        let edge = edge(
            1,
            2,
            RuntimeFilterRouteRole::Producer(BindingId::new(10)),
            7,
            RuntimeFilterRouteRole::Aggregator,
            RuntimeFilterRoutePeer::Remote {
                participant_id: RuntimeFilterParticipantId::new(7),
                endpoint: RuntimeEndpoint::new("be-7", 9060).unwrap(),
            },
        );

        assert_eq!(
            RuntimeFilterChannelRoutingView::new(
                ChannelId::new(1),
                BTreeSet::from([RuntimeFilterRouteRole::Producer(BindingId::new(10))]),
                BTreeMap::new(),
                Vec::new(),
                vec![edge.clone(), edge.clone()],
            )
            .unwrap_err(),
            RuntimeFilterRouteContractError::DuplicateRouteEdge {
                edge: RouteEdgeId::new(1)
            }
        );

        assert_eq!(
            RuntimeFilterChannelRoutingView::new(
                ChannelId::new(1),
                BTreeSet::from([RuntimeFilterRouteRole::Producer(BindingId::new(10))]),
                BTreeMap::new(),
                vec![edge.clone(), edge],
                Vec::new(),
            )
            .unwrap_err(),
            RuntimeFilterRouteContractError::DuplicateRouteEdge {
                edge: RouteEdgeId::new(1)
            }
        );
    }

    #[test]
    fn routing_edges_reject_peers_that_do_not_match_their_endpoints() {
        let source = endpoint(2, RuntimeFilterRouteRole::Producer(BindingId::new(10)));
        let target = endpoint(7, RuntimeFilterRouteRole::Aggregator);
        let kinds = BTreeSet::from([RuntimeFilterEnvelopeKind::Contribution]);

        assert_eq!(
            RuntimeFilterRoutingEdgeView::new(
                ChannelId::new(1),
                RouteEdgeId::new(1),
                source.clone(),
                target.clone(),
                RuntimeFilterRoutePeer::Loopback,
                kinds.clone(),
            )
            .unwrap_err(),
            RuntimeFilterRouteContractError::InvalidPeer {
                edge: RouteEdgeId::new(1)
            }
        );
        assert_eq!(
            RuntimeFilterRoutingEdgeView::new(
                ChannelId::new(1),
                RouteEdgeId::new(1),
                source,
                target,
                RuntimeFilterRoutePeer::Remote {
                    participant_id: RuntimeFilterParticipantId::new(8),
                    endpoint: RuntimeEndpoint::new("be-8", 9060).unwrap(),
                },
                kinds,
            )
            .unwrap_err(),
            RuntimeFilterRouteContractError::InvalidPeer {
                edge: RouteEdgeId::new(1)
            }
        );
    }

    #[test]
    fn routing_intents_preserve_exact_producer_and_delivery_scope() {
        let producer = RuntimeFilterProducerRouteIntent::new(
            DeploymentEpoch::new(3),
            ChannelId::new(1),
            BindingId::new(10),
            RuntimeFilterEnvelopeKind::Contribution,
        )
        .unwrap();
        assert_eq!(producer.deployment_epoch(), DeploymentEpoch::new(3));
        assert_eq!(producer.channel_id(), ChannelId::new(1));
        assert_eq!(producer.producer_binding_id(), BindingId::new(10));
        assert_eq!(
            producer.envelope_kind(),
            RuntimeFilterEnvelopeKind::Contribution
        );
        assert_eq!(
            RuntimeFilterProducerRouteIntent::new(
                DeploymentEpoch::new(3),
                ChannelId::new(1),
                BindingId::new(10),
                RuntimeFilterEnvelopeKind::Artifact,
            )
            .unwrap_err(),
            RuntimeFilterRouteContractError::ForbiddenOutboundKind {
                channel: ChannelId::new(1),
                role: RuntimeFilterRouteRole::Producer(BindingId::new(10)),
                kind: RuntimeFilterEnvelopeKind::Artifact,
            }
        );

        let delivery = RuntimeFilterDeliveryRouteIntent::new(
            DeploymentEpoch::new(3),
            ChannelId::new(1),
            vec![RouteEdgeId::new(9), RouteEdgeId::new(3)],
            RuntimeFilterEnvelopeKind::Artifact,
        )
        .unwrap();
        assert_eq!(
            delivery.route_edge_ids(),
            &[RouteEdgeId::new(3), RouteEdgeId::new(9)]
        );
        assert_eq!(
            RuntimeFilterDeliveryRouteIntent::new(
                DeploymentEpoch::new(3),
                ChannelId::new(1),
                vec![RouteEdgeId::new(9), RouteEdgeId::new(9)],
                RuntimeFilterEnvelopeKind::Unavailable,
            )
            .unwrap_err(),
            RuntimeFilterRouteContractError::DuplicateRequestedRouteEdge {
                edge: RouteEdgeId::new(9)
            }
        );
        assert_eq!(
            RuntimeFilterDeliveryRouteIntent::new(
                DeploymentEpoch::new(3),
                ChannelId::new(1),
                vec![RouteEdgeId::new(3)],
                RuntimeFilterEnvelopeKind::ProducerClosed,
            )
            .unwrap_err(),
            RuntimeFilterRouteContractError::ForbiddenDeliveryKind {
                channel: ChannelId::new(1),
                kind: RuntimeFilterEnvelopeKind::ProducerClosed,
            }
        );
        assert_eq!(
            RuntimeFilterDeliveryRouteIntent::new(
                DeploymentEpoch::new(3),
                ChannelId::new(1),
                vec![RouteEdgeId::new(3)],
                RuntimeFilterEnvelopeKind::Contribution,
            )
            .unwrap_err(),
            RuntimeFilterRouteContractError::ForbiddenDeliveryKind {
                channel: ChannelId::new(1),
                kind: RuntimeFilterEnvelopeKind::Contribution,
            }
        );
    }

    #[test]
    fn route_decision_rejects_zero_duplicate_and_conflicting_edges() {
        assert_eq!(
            RuntimeFilterRemoteRoute::new(
                RouteEdgeId::new(0),
                RuntimeFilterParticipantId::new(7),
                RuntimeEndpoint::new("be-7", 9060).unwrap(),
                RuntimeFilterRouteRole::Consumer(BindingId::new(20)),
            )
            .unwrap_err(),
            RuntimeFilterRouteContractError::ZeroIdentity("route edge id")
        );

        let remote = || {
            RuntimeFilterRemoteRoute::new(
                RouteEdgeId::new(9),
                RuntimeFilterParticipantId::new(7),
                RuntimeEndpoint::new("be-7", 9060).unwrap(),
                RuntimeFilterRouteRole::Consumer(BindingId::new(20)),
            )
            .unwrap()
        };

        assert_eq!(
            RuntimeFilterRouteDecision::new(vec![RouteEdgeId::new(0)], Vec::new()).unwrap_err(),
            RuntimeFilterRouteContractError::ZeroIdentity("route edge id")
        );
        let invalid_remote = RuntimeFilterRemoteRoute {
            route_edge_id: RouteEdgeId::new(0),
            peer_participant_id: RuntimeFilterParticipantId::new(7),
            endpoint: RuntimeEndpoint::new("be-7", 9060).unwrap(),
            target_role: RuntimeFilterRouteRole::Consumer(BindingId::new(20)),
        };
        assert_eq!(
            RuntimeFilterRouteDecision::new(Vec::new(), vec![invalid_remote]).unwrap_err(),
            RuntimeFilterRouteContractError::ZeroIdentity("route edge id")
        );
        assert_eq!(
            RuntimeFilterRouteDecision::new(
                vec![RouteEdgeId::new(3), RouteEdgeId::new(3)],
                Vec::new(),
            )
            .unwrap_err(),
            RuntimeFilterRouteContractError::DuplicateRouteEdge {
                edge: RouteEdgeId::new(3)
            }
        );
        assert_eq!(
            RuntimeFilterRouteDecision::new(Vec::new(), vec![remote(), remote()]).unwrap_err(),
            RuntimeFilterRouteContractError::DuplicateRouteEdge {
                edge: RouteEdgeId::new(9)
            }
        );
        assert_eq!(
            RuntimeFilterRouteDecision::new(vec![RouteEdgeId::new(9)], vec![remote()]).unwrap_err(),
            RuntimeFilterRouteContractError::DuplicateRouteEdge {
                edge: RouteEdgeId::new(9)
            }
        );

        let decision = RuntimeFilterRouteDecision::new(
            vec![RouteEdgeId::new(5), RouteEdgeId::new(3)],
            vec![remote()],
        )
        .unwrap();
        assert_eq!(
            decision.loopback_route_edge_ids(),
            &[RouteEdgeId::new(3), RouteEdgeId::new(5)]
        );
        assert_eq!(
            decision.remote_routes()[0].route_edge_id(),
            RouteEdgeId::new(9)
        );
    }
}
