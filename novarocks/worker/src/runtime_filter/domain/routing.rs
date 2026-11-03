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

//! Backend-owned runtime-filter route authority.
//!
//! This graph owns participant-local routing facts only.  It deliberately
//! contains neither contribution payloads nor artifact/query evaluation.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::runtime_filter::{RuntimeFilterBindingId, RuntimeFilterChannelId};
use novarocks_types::{BackendProcessId, UniqueId};

use super::{BackendEnvelopeKind, BackendParticipantIdentity, BackendRouteEdgeId};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum BackendRouteRole {
    Producer(RuntimeFilterBindingId),
    Consumer(RuntimeFilterBindingId),
    Aggregator,
    Relay,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendRoutePeer {
    Loopback,
    Remote {
        participant_id: u32,
        backend_process_id: BackendProcessId,
        endpoint: RuntimeEndpoint,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendRouteEndpoint {
    participant_id: u32,
    role: BackendRouteRole,
}

impl BackendRouteEndpoint {
    pub fn new(participant_id: u32, role: BackendRouteRole) -> Result<Self, BackendRoutingError> {
        if participant_id == 0 {
            return Err(BackendRoutingError::ZeroParticipant);
        }
        Ok(Self {
            participant_id,
            role,
        })
    }

    pub const fn participant_id(&self) -> u32 {
        self.participant_id
    }

    pub const fn role(&self) -> BackendRouteRole {
        self.role
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendRoutingEdge {
    id: BackendRouteEdgeId,
    source: BackendRouteEndpoint,
    target: BackendRouteEndpoint,
    peer: BackendRoutePeer,
    allowed_kinds: BTreeSet<BackendEnvelopeKind>,
}

impl BackendRoutingEdge {
    pub fn new(
        id: BackendRouteEdgeId,
        source: BackendRouteEndpoint,
        target: BackendRouteEndpoint,
        peer: BackendRoutePeer,
        allowed_kinds: impl IntoIterator<Item = BackendEnvelopeKind>,
    ) -> Result<Self, BackendRoutingError> {
        if id.get() == 0 {
            return Err(BackendRoutingError::ZeroRouteEdge);
        }
        let allowed_kinds = allowed_kinds.into_iter().collect::<BTreeSet<_>>();
        if allowed_kinds.is_empty() {
            return Err(BackendRoutingError::EmptyAllowedKinds(id));
        }
        if matches!(
            &peer,
            BackendRoutePeer::Remote {
                participant_id: 0,
                ..
            }
        ) {
            return Err(BackendRoutingError::ZeroParticipant);
        }
        Ok(Self {
            id,
            source,
            target,
            peer,
            allowed_kinds,
        })
    }

    pub const fn id(&self) -> BackendRouteEdgeId {
        self.id
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn source(&self) -> &BackendRouteEndpoint {
        &self.source
    }

    pub const fn target(&self) -> &BackendRouteEndpoint {
        &self.target
    }

    pub const fn peer(&self) -> &BackendRoutePeer {
        &self.peer
    }

    pub fn allows(&self, kind: BackendEnvelopeKind) -> bool {
        self.allowed_kinds.contains(&kind)
    }

    fn is_loopback_self(&self) -> bool {
        matches!(self.peer, BackendRoutePeer::Loopback)
            && self.source.participant_id() == self.target.participant_id()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendRemoteRoute {
    edge_id: BackendRouteEdgeId,
    participant_id: u32,
    backend_process_id: BackendProcessId,
    endpoint: RuntimeEndpoint,
    target_role: BackendRouteRole,
}

impl BackendRemoteRoute {
    pub const fn edge_id(&self) -> BackendRouteEdgeId {
        self.edge_id
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn participant_id(&self) -> u32 {
        self.participant_id
    }

    pub const fn backend_process_id(&self) -> BackendProcessId {
        self.backend_process_id
    }

    pub const fn endpoint(&self) -> &RuntimeEndpoint {
        &self.endpoint
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn target_role(&self) -> BackendRouteRole {
        self.target_role
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendRouteDecision {
    loopback_route_edge_ids: Vec<BackendRouteEdgeId>,
    remote_routes: Vec<BackendRemoteRoute>,
}

impl BackendRouteDecision {
    pub fn loopback_route_edge_ids(&self) -> &[BackendRouteEdgeId] {
        &self.loopback_route_edge_ids
    }

    pub fn remote_routes(&self) -> &[BackendRemoteRoute] {
        &self.remote_routes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendRoutingChannel {
    channel_id: RuntimeFilterChannelId,
    local_roles: BTreeSet<BackendRouteRole>,
    inbound_edges: Vec<BackendRoutingEdge>,
    outbound_edges: Vec<BackendRoutingEdge>,
    producer_participants: BTreeMap<(RuntimeFilterBindingId, UniqueId), u32>,
}

impl BackendRoutingChannel {
    pub fn new(
        channel_id: RuntimeFilterChannelId,
        local_roles: impl IntoIterator<Item = BackendRouteRole>,
        inbound_edges: impl IntoIterator<Item = BackendRoutingEdge>,
        outbound_edges: impl IntoIterator<Item = BackendRoutingEdge>,
        producer_participants: impl IntoIterator<Item = ((RuntimeFilterBindingId, UniqueId), u32)>,
    ) -> Result<Self, BackendRoutingError> {
        let local_roles = local_roles.into_iter().collect::<BTreeSet<_>>();
        if local_roles.is_empty() {
            return Err(BackendRoutingError::NoLocalRoles(channel_id));
        }
        let inbound_edges = inbound_edges.into_iter().collect::<Vec<_>>();
        let outbound_edges = outbound_edges.into_iter().collect::<Vec<_>>();
        // A loopback route deliberately appears in both projections of the
        // local routing shard: it is an outbound materialization edge and an
        // inbound consumer-delivery edge at the same time. Keep duplicate
        // rejection within either projection, but accept the one valid
        // cross-projection duplicate only when it is the identical self edge.
        let mut inbound_by_id = BTreeMap::new();
        for edge in &inbound_edges {
            if inbound_by_id.insert(edge.id(), edge).is_some() {
                return Err(BackendRoutingError::DuplicateRouteEdge(edge.id()));
            }
        }
        let mut outbound_ids = BTreeSet::new();
        for edge in &outbound_edges {
            if !outbound_ids.insert(edge.id()) {
                return Err(BackendRoutingError::DuplicateRouteEdge(edge.id()));
            }
            if let Some(inbound) = inbound_by_id.get(&edge.id())
                && (*inbound != edge || !edge.is_loopback_self())
            {
                return Err(BackendRoutingError::DuplicateRouteEdge(edge.id()));
            }
        }
        let mut participants = BTreeMap::new();
        for (key, participant_id) in producer_participants {
            if participant_id == 0 {
                return Err(BackendRoutingError::ZeroParticipant);
            }
            if participants.insert(key, participant_id).is_some() {
                return Err(BackendRoutingError::DuplicateProducerInstance {
                    channel: channel_id,
                    binding: key.0,
                    task_key: key.1,
                });
            }
        }
        Ok(Self {
            channel_id,
            local_roles,
            inbound_edges,
            outbound_edges,
            producer_participants: participants,
        })
    }

    pub const fn channel_id(&self) -> RuntimeFilterChannelId {
        self.channel_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendRoutingShard {
    participant: BackendParticipantIdentity,
    local_participant_id: u32,
    channels: BTreeMap<RuntimeFilterChannelId, BackendRoutingChannel>,
}

impl BackendRoutingShard {
    pub fn new(
        participant: BackendParticipantIdentity,
        local_participant_id: u32,
        channels: impl IntoIterator<Item = BackendRoutingChannel>,
    ) -> Result<Self, BackendRoutingError> {
        if local_participant_id == 0 {
            return Err(BackendRoutingError::ZeroParticipant);
        }
        let mut by_id = BTreeMap::new();
        for channel in channels {
            let channel_id = channel.channel_id();
            if by_id.insert(channel_id, channel).is_some() {
                return Err(BackendRoutingError::DuplicateChannel(channel_id));
            }
        }
        Ok(Self {
            participant,
            local_participant_id,
            channels: by_id,
        })
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn participant(&self) -> BackendParticipantIdentity {
        self.participant
    }

    #[allow(
        dead_code,
        reason = "Retained for staged backend runtime-filter domain and materialization integration."
    )]
    pub const fn local_participant_id(&self) -> u32 {
        self.local_participant_id
    }

    pub fn route_producer(
        &self,
        channel_id: RuntimeFilterChannelId,
        binding_id: RuntimeFilterBindingId,
        kind: BackendEnvelopeKind,
    ) -> Result<BackendRouteDecision, BackendRoutingError> {
        let channel = self.channel(channel_id)?;
        let source = BackendRouteRole::Producer(binding_id);
        if !channel.local_roles.contains(&source) {
            return Err(BackendRoutingError::UnknownLocalRole {
                channel: channel_id,
                role: source,
            });
        }
        let edges = channel.outbound_edges.iter().filter(|edge| {
            edge.source.role() == source && edge.target.role() == BackendRouteRole::Aggregator
        });
        self.decision(channel_id, source, kind, edges)
    }

    pub fn route_delivery(
        &self,
        channel_id: RuntimeFilterChannelId,
        route_edge_ids: &[BackendRouteEdgeId],
        kind: BackendEnvelopeKind,
    ) -> Result<BackendRouteDecision, BackendRoutingError> {
        let channel = self.channel(channel_id)?;
        let mut edges = Vec::with_capacity(route_edge_ids.len());
        for route_edge_id in route_edge_ids {
            let edge = channel
                .outbound_edges
                .iter()
                .find(|edge| edge.id() == *route_edge_id)
                .ok_or(BackendRoutingError::UnknownRouteEdge {
                    channel: channel_id,
                    edge: *route_edge_id,
                })?;
            if !edge.allows(kind) {
                return Err(BackendRoutingError::ForbiddenOutboundKind {
                    channel: channel_id,
                    role: edge.source.role(),
                    kind,
                });
            }
            edges.push(edge);
        }
        self.make_decision(edges)
    }

    pub fn authorize_contribution(
        &self,
        channel_id: RuntimeFilterChannelId,
        binding_id: RuntimeFilterBindingId,
        task_key: UniqueId,
        kind: BackendEnvelopeKind,
    ) -> Result<&BackendRoutingEdge, BackendRoutingError> {
        if !matches!(
            kind,
            BackendEnvelopeKind::Contribution
                | BackendEnvelopeKind::ProducerClosed
                | BackendEnvelopeKind::ProducerUnavailable
        ) {
            return Err(BackendRoutingError::ForbiddenInboundKind {
                channel: channel_id,
                edge: None,
                kind,
            });
        }
        let channel = self.channel(channel_id)?;
        let source_participant = *channel
            .producer_participants
            .get(&(binding_id, task_key))
            .ok_or(BackendRoutingError::UnknownProducerInstance {
                channel: channel_id,
                binding: binding_id,
                task_key,
            })?;
        let source_role = BackendRouteRole::Producer(binding_id);
        let edges = channel
            .inbound_edges
            .iter()
            .filter(|edge| {
                edge.source.participant_id() == source_participant
                    && edge.source.role() == source_role
                    && edge.target.role() == BackendRouteRole::Aggregator
            })
            .collect::<Vec<_>>();
        let edge = match edges.as_slice() {
            [] => {
                return Err(BackendRoutingError::UnknownInboundProducerRoute {
                    channel: channel_id,
                    binding: binding_id,
                    participant: source_participant,
                });
            }
            [edge] => *edge,
            _ => {
                return Err(BackendRoutingError::AmbiguousInboundRoute {
                    channel: channel_id,
                });
            }
        };
        if edge.target.participant_id() != self.local_participant_id {
            return Err(BackendRoutingError::InboundTargetMismatch {
                channel: channel_id,
                edge: edge.id(),
                local_participant: self.local_participant_id,
            });
        }
        if !edge.allows(kind) {
            return Err(BackendRoutingError::ForbiddenInboundKind {
                channel: channel_id,
                edge: Some(edge.id()),
                kind,
            });
        }
        Ok(edge)
    }

    pub fn authorize_delivery(
        &self,
        channel_id: RuntimeFilterChannelId,
        route_edge_id: BackendRouteEdgeId,
        kind: BackendEnvelopeKind,
    ) -> Result<&BackendRoutingEdge, BackendRoutingError> {
        if !matches!(
            kind,
            BackendEnvelopeKind::Artifact
                | BackendEnvelopeKind::FinalArtifact
                | BackendEnvelopeKind::Unavailable
                | BackendEnvelopeKind::CompletedWithoutArtifact
                | BackendEnvelopeKind::DegradedLogical
        ) {
            return Err(BackendRoutingError::ForbiddenInboundKind {
                channel: channel_id,
                edge: Some(route_edge_id),
                kind,
            });
        }
        let channel = self.channel(channel_id)?;
        let edge = channel
            .inbound_edges
            .iter()
            .find(|edge| edge.id() == route_edge_id)
            .ok_or(BackendRoutingError::UnknownRouteEdge {
                channel: channel_id,
                edge: route_edge_id,
            })?;
        if edge.target.participant_id() != self.local_participant_id {
            return Err(BackendRoutingError::InboundTargetMismatch {
                channel: channel_id,
                edge: route_edge_id,
                local_participant: self.local_participant_id,
            });
        }
        if !edge.allows(kind) {
            return Err(BackendRoutingError::ForbiddenInboundKind {
                channel: channel_id,
                edge: Some(route_edge_id),
                kind,
            });
        }
        Ok(edge)
    }

    fn channel(
        &self,
        channel_id: RuntimeFilterChannelId,
    ) -> Result<&BackendRoutingChannel, BackendRoutingError> {
        self.channels
            .get(&channel_id)
            .ok_or(BackendRoutingError::UnknownChannel(channel_id))
    }

    fn decision<'a>(
        &self,
        channel: RuntimeFilterChannelId,
        source: BackendRouteRole,
        kind: BackendEnvelopeKind,
        edges: impl Iterator<Item = &'a BackendRoutingEdge>,
    ) -> Result<BackendRouteDecision, BackendRoutingError> {
        let edges = edges.collect::<Vec<_>>();
        let edge = match edges.as_slice() {
            [] => {
                return Err(BackendRoutingError::ForbiddenOutboundKind {
                    channel,
                    role: source,
                    kind,
                });
            }
            [edge] => *edge,
            _ => {
                return Err(BackendRoutingError::AmbiguousOutboundRoute {
                    channel,
                    role: source,
                });
            }
        };
        if !edge.allows(kind) {
            return Err(BackendRoutingError::ForbiddenOutboundKind {
                channel,
                role: source,
                kind,
            });
        }
        self.make_decision([edge])
    }

    fn make_decision<'a>(
        &self,
        edges: impl IntoIterator<Item = &'a BackendRoutingEdge>,
    ) -> Result<BackendRouteDecision, BackendRoutingError> {
        let mut loopback_route_edge_ids = Vec::new();
        let mut remote_routes = Vec::new();
        for edge in edges {
            match edge.peer() {
                BackendRoutePeer::Loopback => loopback_route_edge_ids.push(edge.id()),
                BackendRoutePeer::Remote {
                    participant_id,
                    backend_process_id,
                    endpoint,
                } => remote_routes.push(BackendRemoteRoute {
                    edge_id: edge.id(),
                    participant_id: *participant_id,
                    backend_process_id: *backend_process_id,
                    endpoint: endpoint.clone(),
                    target_role: edge.target().role(),
                }),
            }
        }
        Ok(BackendRouteDecision {
            loopback_route_edge_ids,
            remote_routes,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendRoutingError {
    ZeroParticipant,
    ZeroRouteEdge,
    EmptyAllowedKinds(BackendRouteEdgeId),
    DuplicateRouteEdge(BackendRouteEdgeId),
    DuplicateChannel(RuntimeFilterChannelId),
    DuplicateProducerInstance {
        channel: RuntimeFilterChannelId,
        binding: RuntimeFilterBindingId,
        task_key: UniqueId,
    },
    NoLocalRoles(RuntimeFilterChannelId),
    UnknownChannel(RuntimeFilterChannelId),
    UnknownLocalRole {
        channel: RuntimeFilterChannelId,
        role: BackendRouteRole,
    },
    UnknownRouteEdge {
        channel: RuntimeFilterChannelId,
        edge: BackendRouteEdgeId,
    },
    UnknownProducerInstance {
        channel: RuntimeFilterChannelId,
        binding: RuntimeFilterBindingId,
        task_key: UniqueId,
    },
    UnknownInboundProducerRoute {
        channel: RuntimeFilterChannelId,
        binding: RuntimeFilterBindingId,
        participant: u32,
    },
    AmbiguousOutboundRoute {
        channel: RuntimeFilterChannelId,
        role: BackendRouteRole,
    },
    AmbiguousInboundRoute {
        channel: RuntimeFilterChannelId,
    },
    ForbiddenOutboundKind {
        channel: RuntimeFilterChannelId,
        role: BackendRouteRole,
        kind: BackendEnvelopeKind,
    },
    ForbiddenInboundKind {
        channel: RuntimeFilterChannelId,
        edge: Option<BackendRouteEdgeId>,
        kind: BackendEnvelopeKind,
    },
    InboundTargetMismatch {
        channel: RuntimeFilterChannelId,
        edge: BackendRouteEdgeId,
        local_participant: u32,
    },
}

impl fmt::Display for BackendRoutingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid Backend runtime-filter route authority: {self:?}"
        )
    }
}

impl std::error::Error for BackendRoutingError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn channel() -> BackendRoutingChannel {
        let producer = BackendRouteEndpoint::new(
            2,
            BackendRouteRole::Producer(RuntimeFilterBindingId::new(7)),
        )
        .unwrap();
        let aggregator = BackendRouteEndpoint::new(1, BackendRouteRole::Aggregator).unwrap();
        let consumer = BackendRouteEndpoint::new(
            3,
            BackendRouteRole::Consumer(RuntimeFilterBindingId::new(8)),
        )
        .unwrap();
        BackendRoutingChannel::new(
            RuntimeFilterChannelId::new(9),
            [BackendRouteRole::Aggregator],
            [
                BackendRoutingEdge::new(
                    BackendRouteEdgeId::new(11),
                    producer.clone(),
                    aggregator.clone(),
                    BackendRoutePeer::Remote {
                        participant_id: 2,
                        backend_process_id: BackendProcessId::new_v7(),
                        endpoint: RuntimeEndpoint::new("be-2", 8060).unwrap(),
                    },
                    [
                        BackendEnvelopeKind::Contribution,
                        BackendEnvelopeKind::ProducerClosed,
                        BackendEnvelopeKind::ProducerUnavailable,
                    ],
                )
                .unwrap(),
                BackendRoutingEdge::new(
                    BackendRouteEdgeId::new(12),
                    aggregator.clone(),
                    consumer,
                    BackendRoutePeer::Remote {
                        participant_id: 3,
                        backend_process_id: BackendProcessId::new_v7(),
                        endpoint: RuntimeEndpoint::new("be-3", 8060).unwrap(),
                    },
                    [
                        BackendEnvelopeKind::Artifact,
                        BackendEnvelopeKind::FinalArtifact,
                    ],
                )
                .unwrap(),
            ],
            [],
            [((RuntimeFilterBindingId::new(7), UniqueId::new(5, 6)), 2)],
        )
        .unwrap()
    }

    fn remote_producer_decision(
        backend_process_id: BackendProcessId,
        participant_id: u32,
    ) -> BackendRouteDecision {
        let binding = RuntimeFilterBindingId::new(7);
        let source = BackendRouteEndpoint::new(1, BackendRouteRole::Producer(binding)).unwrap();
        let target =
            BackendRouteEndpoint::new(participant_id, BackendRouteRole::Aggregator).unwrap();
        let edge = BackendRoutingEdge::new(
            BackendRouteEdgeId::new(11),
            source,
            target,
            BackendRoutePeer::Remote {
                participant_id,
                backend_process_id,
                endpoint: RuntimeEndpoint::new("same-be", 8060).unwrap(),
            },
            [BackendEnvelopeKind::Contribution],
        )
        .unwrap();
        let channel = BackendRoutingChannel::new(
            RuntimeFilterChannelId::new(9),
            [BackendRouteRole::Producer(binding)],
            [],
            [edge],
            [],
        )
        .unwrap();
        BackendRoutingShard::new(
            BackendParticipantIdentity::new(UniqueId::new(1, 2), 3),
            1,
            [channel],
        )
        .unwrap()
        .route_producer(
            RuntimeFilterChannelId::new(9),
            binding,
            BackendEnvelopeKind::Contribution,
        )
        .unwrap()
    }

    #[test]
    fn remote_decision_preserves_frozen_process_independently_of_participant_id() {
        let process: BackendProcessId = "0194bc12-3456-7000-8000-123456789abc".parse().unwrap();
        for participant_id in [2, 73] {
            let decision = remote_producer_decision(process, participant_id);
            assert!(decision.loopback_route_edge_ids().is_empty());
            assert_eq!(decision.remote_routes().len(), 1);
            let route = &decision.remote_routes()[0];
            assert_eq!(route.backend_process_id(), process);
            assert_eq!(route.participant_id(), participant_id);
            assert_eq!(route.edge_id(), BackendRouteEdgeId::new(11));
            assert_eq!(
                route.endpoint(),
                &RuntimeEndpoint::new("same-be", 8060).unwrap()
            );
        }
    }

    #[test]
    fn remote_incarnations_at_the_same_endpoint_are_distinct() {
        let first: BackendProcessId = "0194bc12-3456-7000-8000-123456789abc".parse().unwrap();
        let replacement: BackendProcessId = "0194bc12-3456-7000-8000-123456789abd".parse().unwrap();
        let peer = |backend_process_id| BackendRoutePeer::Remote {
            participant_id: 2,
            backend_process_id,
            endpoint: RuntimeEndpoint::new("same-be", 8060).unwrap(),
        };
        assert_ne!(peer(first), peer(replacement));
        let first_decision = remote_producer_decision(first, 2);
        let replacement_decision = remote_producer_decision(replacement, 2);
        let first_route = &first_decision.remote_routes()[0];
        let replacement_route = &replacement_decision.remote_routes()[0];
        assert_eq!(first_route.endpoint(), replacement_route.endpoint());
        assert_eq!(
            first_route.participant_id(),
            replacement_route.participant_id()
        );
        assert_eq!(first_route.backend_process_id(), first);
        assert_eq!(replacement_route.backend_process_id(), replacement);
        assert_ne!(first_route, replacement_route);
        assert_ne!(first_decision, replacement_decision);
    }

    #[test]
    fn contribution_authorization_validates_the_full_stream_identity() {
        let shard = BackendRoutingShard::new(
            BackendParticipantIdentity::new(UniqueId::new(1, 2), 3),
            1,
            [channel()],
        )
        .unwrap();
        let edge = shard
            .authorize_contribution(
                RuntimeFilterChannelId::new(9),
                RuntimeFilterBindingId::new(7),
                UniqueId::new(5, 6),
                BackendEnvelopeKind::Contribution,
            )
            .unwrap();
        assert_eq!(edge.id(), BackendRouteEdgeId::new(11));
        assert!(matches!(
            shard.authorize_contribution(
                RuntimeFilterChannelId::new(9),
                RuntimeFilterBindingId::new(7),
                UniqueId::new(5, 7),
                BackendEnvelopeKind::Contribution
            ),
            Err(BackendRoutingError::UnknownProducerInstance { .. })
        ));
    }

    #[test]
    fn delivery_authorization_rejects_a_contribution_kind() {
        let shard = BackendRoutingShard::new(
            BackendParticipantIdentity::new(UniqueId::new(1, 2), 3),
            1,
            [channel()],
        )
        .unwrap();
        assert!(matches!(
            shard.authorize_delivery(
                RuntimeFilterChannelId::new(9),
                BackendRouteEdgeId::new(12),
                BackendEnvelopeKind::Contribution
            ),
            Err(BackendRoutingError::ForbiddenInboundKind { .. })
        ));
    }

    #[test]
    fn accepts_an_identical_loopback_edge_in_both_projections() {
        let producer = BackendRouteEndpoint::new(
            1,
            BackendRouteRole::Producer(RuntimeFilterBindingId::new(7)),
        )
        .unwrap();
        let consumer = BackendRouteEndpoint::new(
            1,
            BackendRouteRole::Consumer(RuntimeFilterBindingId::new(8)),
        )
        .unwrap();
        let loopback = BackendRoutingEdge::new(
            BackendRouteEdgeId::new(13),
            producer,
            consumer,
            BackendRoutePeer::Loopback,
            [BackendEnvelopeKind::Artifact],
        )
        .unwrap();

        assert!(
            BackendRoutingChannel::new(
                RuntimeFilterChannelId::new(9),
                [BackendRouteRole::Producer(RuntimeFilterBindingId::new(7))],
                [loopback.clone()],
                [loopback],
                [],
            )
            .is_ok()
        );
    }

    #[test]
    fn rejects_a_nonidentical_cross_projection_duplicate() {
        let producer = BackendRouteEndpoint::new(
            1,
            BackendRouteRole::Producer(RuntimeFilterBindingId::new(7)),
        )
        .unwrap();
        let consumer = BackendRouteEndpoint::new(
            1,
            BackendRouteRole::Consumer(RuntimeFilterBindingId::new(8)),
        )
        .unwrap();
        let inbound = BackendRoutingEdge::new(
            BackendRouteEdgeId::new(13),
            producer.clone(),
            consumer.clone(),
            BackendRoutePeer::Loopback,
            [BackendEnvelopeKind::Artifact],
        )
        .unwrap();
        let outbound = BackendRoutingEdge::new(
            BackendRouteEdgeId::new(13),
            producer,
            consumer,
            BackendRoutePeer::Loopback,
            [BackendEnvelopeKind::FinalArtifact],
        )
        .unwrap();

        let error = BackendRoutingChannel::new(
            RuntimeFilterChannelId::new(9),
            [BackendRouteRole::Producer(RuntimeFilterBindingId::new(7))],
            [inbound],
            [outbound],
            [],
        )
        .expect_err("mismatched cross-projection route must be rejected");
        assert!(matches!(error, BackendRoutingError::DuplicateRouteEdge(_)));
    }
}
