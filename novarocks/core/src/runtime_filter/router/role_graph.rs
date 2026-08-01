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

use std::sync::Arc;

use crate::common::types::UniqueId;
use crate::runtime_filter::model::contract::{BindingId, ChannelId};
use crate::runtime_filter::port::identity::{DeploymentEpoch, RouteEdgeId};
use crate::runtime_filter::port::routing::{
    RuntimeFilterChannelRoutingView, RuntimeFilterDeliveryRouteIntent,
    RuntimeFilterProducerRouteIntent, RuntimeFilterRemoteRoute, RuntimeFilterRouteContractError,
    RuntimeFilterRouteDecision, RuntimeFilterRoutePeer, RuntimeFilterRouteRole,
    RuntimeFilterRoutingEdgeView, RuntimeFilterRoutingShard,
};
use crate::runtime_filter::port::transport::RuntimeFilterEnvelopeKind;

pub(crate) struct RoleRouter {
    shard: Arc<RuntimeFilterRoutingShard>,
}

impl RoleRouter {
    pub(crate) fn new(shard: Arc<RuntimeFilterRoutingShard>) -> Self {
        Self { shard }
    }

    pub(crate) fn route_producer(
        &self,
        intent: RuntimeFilterProducerRouteIntent,
    ) -> Result<RuntimeFilterRouteDecision, RuntimeFilterRouteContractError> {
        let channel = self.channel(intent.deployment_epoch(), intent.channel_id())?;
        let source_role = RuntimeFilterRouteRole::Producer(intent.producer_binding_id());
        if !channel.local_roles().contains(&source_role) {
            return Err(RuntimeFilterRouteContractError::UnknownSourceRole {
                channel: intent.channel_id(),
                role: source_role,
            });
        }

        let edges = channel
            .outbound_edges()
            .iter()
            .filter(|edge| {
                edge.source().role() == source_role
                    && edge.target().role() == RuntimeFilterRouteRole::Aggregator
            })
            .collect::<Vec<_>>();
        let edge = match edges.as_slice() {
            [] => {
                return Err(RuntimeFilterRouteContractError::ForbiddenOutboundKind {
                    channel: intent.channel_id(),
                    role: source_role,
                    kind: intent.envelope_kind(),
                });
            }
            [edge] => *edge,
            _ => {
                return Err(RuntimeFilterRouteContractError::AmbiguousOutboundRoute {
                    channel: intent.channel_id(),
                    role: source_role,
                });
            }
        };
        if !edge.allowed_kinds().contains(&intent.envelope_kind()) {
            return Err(RuntimeFilterRouteContractError::ForbiddenOutboundKind {
                channel: intent.channel_id(),
                role: source_role,
                kind: intent.envelope_kind(),
            });
        }
        route_decision([edge])
    }

    pub(crate) fn route_delivery(
        &self,
        intent: RuntimeFilterDeliveryRouteIntent,
    ) -> Result<RuntimeFilterRouteDecision, RuntimeFilterRouteContractError> {
        let channel = self.channel(intent.deployment_epoch(), intent.channel_id())?;
        let mut edges = Vec::with_capacity(intent.route_edge_ids().len());
        for route_edge_id in intent.route_edge_ids() {
            let edge = channel
                .outbound_edges()
                .iter()
                .find(|edge| edge.route_edge_id() == *route_edge_id)
                .ok_or(RuntimeFilterRouteContractError::UnknownOutboundRoute {
                    channel: intent.channel_id(),
                    edge: *route_edge_id,
                })?;
            if !edge.allowed_kinds().contains(&intent.envelope_kind()) {
                return Err(RuntimeFilterRouteContractError::ForbiddenOutboundKind {
                    channel: intent.channel_id(),
                    role: edge.source().role(),
                    kind: intent.envelope_kind(),
                });
            }
            edges.push(edge);
        }
        route_decision(edges)
    }

    pub(crate) fn authorize_contribution(
        &self,
        epoch: DeploymentEpoch,
        channel_id: ChannelId,
        binding_id: BindingId,
        fragment_instance_id: UniqueId,
        kind: RuntimeFilterEnvelopeKind,
    ) -> Result<&RuntimeFilterRoutingEdgeView, RuntimeFilterRouteContractError> {
        let channel = self.channel(epoch, channel_id)?;
        let source_participant = channel
            .producer_participant(binding_id, fragment_instance_id)
            .ok_or(RuntimeFilterRouteContractError::UnknownProducerInstance {
                channel: channel_id,
                binding: binding_id,
                fragment_instance_id,
            })?;
        let source_role = RuntimeFilterRouteRole::Producer(binding_id);
        let edges = channel
            .inbound_edges()
            .iter()
            .filter(|edge| {
                edge.source().participant_id() == source_participant
                    && edge.source().role() == source_role
                    && edge.target().role() == RuntimeFilterRouteRole::Aggregator
            })
            .collect::<Vec<_>>();
        let edge = match edges.as_slice() {
            [] => {
                return Err(
                    RuntimeFilterRouteContractError::UnknownInboundProducerRoute {
                        channel: channel_id,
                        binding: binding_id,
                        source_participant,
                    },
                );
            }
            [edge] => *edge,
            _ => {
                return Err(RuntimeFilterRouteContractError::AmbiguousInboundRoute {
                    channel: channel_id,
                });
            }
        };
        if edge.target().participant_id() != self.shard.local_participant_id() {
            return Err(RuntimeFilterRouteContractError::InboundTargetMismatch {
                channel: channel_id,
                edge: edge.route_edge_id(),
                local_participant: self.shard.local_participant_id(),
            });
        }
        if !matches!(
            kind,
            RuntimeFilterEnvelopeKind::Contribution
                | RuntimeFilterEnvelopeKind::ProducerClosed
                | RuntimeFilterEnvelopeKind::ProducerUnavailable
        ) || !edge.allowed_kinds().contains(&kind)
        {
            return Err(RuntimeFilterRouteContractError::ForbiddenInboundKind {
                channel: channel_id,
                edge: edge.route_edge_id(),
                kind,
            });
        }
        Ok(edge)
    }

    pub(crate) fn authorize_delivery(
        &self,
        epoch: DeploymentEpoch,
        channel_id: ChannelId,
        route_edge_id: RouteEdgeId,
        kind: RuntimeFilterEnvelopeKind,
    ) -> Result<&RuntimeFilterRoutingEdgeView, RuntimeFilterRouteContractError> {
        let channel = self.channel(epoch, channel_id)?;
        let edge = channel
            .inbound_edges()
            .iter()
            .find(|edge| edge.route_edge_id() == route_edge_id)
            .ok_or(RuntimeFilterRouteContractError::UnknownInboundRoute {
                channel: channel_id,
                edge: route_edge_id,
            })?;
        if edge.target().participant_id() != self.shard.local_participant_id() {
            return Err(RuntimeFilterRouteContractError::InboundTargetMismatch {
                channel: channel_id,
                edge: route_edge_id,
                local_participant: self.shard.local_participant_id(),
            });
        }
        if !matches!(
            kind,
            RuntimeFilterEnvelopeKind::Artifact
                | RuntimeFilterEnvelopeKind::FinalArtifact
                | RuntimeFilterEnvelopeKind::Unavailable
                | RuntimeFilterEnvelopeKind::CompletedWithoutArtifact
                | RuntimeFilterEnvelopeKind::DegradedLogical
        ) || !edge.allowed_kinds().contains(&kind)
        {
            return Err(RuntimeFilterRouteContractError::ForbiddenInboundKind {
                channel: channel_id,
                edge: route_edge_id,
                kind,
            });
        }
        Ok(edge)
    }

    fn channel(
        &self,
        incoming_epoch: DeploymentEpoch,
        channel_id: ChannelId,
    ) -> Result<&RuntimeFilterChannelRoutingView, RuntimeFilterRouteContractError> {
        if incoming_epoch != self.shard.deployment_epoch() {
            return Err(RuntimeFilterRouteContractError::StaleEpoch {
                installed: self.shard.deployment_epoch(),
                incoming: incoming_epoch,
            });
        }
        self.shard
            .channel(channel_id)
            .ok_or(RuntimeFilterRouteContractError::UnknownChannel {
                channel: channel_id,
            })
    }
}

fn route_decision<'a>(
    edges: impl IntoIterator<Item = &'a RuntimeFilterRoutingEdgeView>,
) -> Result<RuntimeFilterRouteDecision, RuntimeFilterRouteContractError> {
    let mut loopback = Vec::new();
    let mut remote = Vec::new();
    for edge in edges {
        match edge.peer() {
            RuntimeFilterRoutePeer::Loopback => loopback.push(edge.route_edge_id()),
            RuntimeFilterRoutePeer::Remote {
                participant_id,
                endpoint,
            } => remote.push(RuntimeFilterRemoteRoute::new(
                edge.route_edge_id(),
                *participant_id,
                endpoint.clone(),
                edge.target().role(),
            )?),
        }
    }
    RuntimeFilterRouteDecision::new(loopback, remote)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use crate::common::types::UniqueId;
    use crate::query_execution::backend::LiveBackendSnapshot;
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::runtime_filter::deployment::BindingInstanceIndex;
    use crate::runtime_filter::deployment::role_graph::{
        ChannelRoleGraph, RoleGraph, RouteEdge, RouteEndpoint, RouteKind,
    };
    use crate::runtime_filter::deployment::routing_shard::project_routing_shards;
    use crate::runtime_filter::model::contract::{BindingId, ChannelId};
    use crate::runtime_filter::port::identity::{
        ContributionIdentity, DeploymentEpoch, PartitionId, ProducerSequence, ProducerStreamId,
        RouteEdgeId, RuntimeFilterParticipantId,
    };
    use crate::runtime_filter::port::routing::{
        RuntimeFilterChannelRoutingView, RuntimeFilterDeliveryRouteIntent,
        RuntimeFilterProducerRouteIntent, RuntimeFilterRouteContractError,
        RuntimeFilterRouteEndpointView, RuntimeFilterRoutePeer, RuntimeFilterRouteRole,
        RuntimeFilterRoutingEdgeView, RuntimeFilterRoutingShard,
    };
    use crate::runtime_filter::port::transport::RuntimeFilterEnvelopeKind;

    use super::RoleRouter;

    fn pid(raw: u32) -> RuntimeFilterParticipantId {
        RuntimeFilterParticipantId::new(raw)
    }

    fn finst(raw: i64) -> UniqueId {
        UniqueId::new(raw, raw + 100)
    }

    fn backends() -> LiveBackendSnapshot {
        LiveBackendSnapshot::new(vec![
            (1, "10.0.0.2:9060".parse().unwrap()),
            (6, "10.0.0.7:9060".parse().unwrap()),
            (10, "10.0.0.11:9060".parse().unwrap()),
        ])
    }

    fn all_of_fixture() -> (RoleGraph, BindingInstanceIndex) {
        let channel_id = ChannelId::new(1);
        let producer_binding = BindingId::new(10);
        let consumer_a = BindingId::new(20);
        let consumer_b = BindingId::new(30);
        let mut channel = ChannelRoleGraph::empty(channel_id);
        channel
            .producers
            .insert(pid(2), BTreeSet::from([producer_binding]));
        channel
            .producers
            .insert(pid(7), BTreeSet::from([producer_binding]));
        channel
            .consumers
            .insert(pid(2), BTreeSet::from([consumer_a]));
        channel
            .consumers
            .insert(pid(7), BTreeSet::from([consumer_a]));
        channel
            .consumers
            .insert(pid(11), BTreeSet::from([consumer_b]));
        channel.aggregator = Some(pid(2));
        channel.routes = vec![
            route(1, RouteKind::ToAggregator, 2, 10, 2, 10),
            route(2, RouteKind::ToAggregator, 7, 10, 2, 10),
            route(3, RouteKind::FromAggregator, 2, 20, 2, 20),
            route(4, RouteKind::FromAggregator, 2, 20, 7, 20),
            route(5, RouteKind::FromAggregator, 2, 30, 11, 30),
        ];
        let graph = RoleGraph {
            channels: BTreeMap::from([(channel_id, channel)]),
        };
        let instances = BTreeMap::from([
            (
                (channel_id, producer_binding, pid(2)),
                BTreeSet::from([finst(2)]),
            ),
            (
                (channel_id, producer_binding, pid(7)),
                BTreeSet::from([finst(7)]),
            ),
        ]);
        (graph, instances)
    }

    fn route(
        edge_id: u32,
        kind: RouteKind,
        from_participant: u32,
        from_binding: u32,
        to_participant: u32,
        to_binding: u32,
    ) -> RouteEdge {
        RouteEdge {
            channel: ChannelId::new(1),
            edge_id: RouteEdgeId::new(edge_id),
            kind,
            from: RouteEndpoint {
                participant: pid(from_participant),
                binding: BindingId::new(from_binding),
            },
            to: RouteEndpoint {
                participant: pid(to_participant),
                binding: BindingId::new(to_binding),
            },
        }
    }

    fn projected_shards() -> BTreeMap<RuntimeFilterParticipantId, RuntimeFilterRoutingShard> {
        let (graph, instances) = all_of_fixture();
        project_routing_shards(DeploymentEpoch::new(9), &graph, &instances, &backends()).unwrap()
    }

    fn router(participant: u32) -> RoleRouter {
        RoleRouter::new(Arc::new(
            projected_shards().remove(&pid(participant)).unwrap(),
        ))
    }

    fn edge(
        edge_id: u32,
        source_participant: u32,
        source_role: RuntimeFilterRouteRole,
        target_participant: u32,
        target_role: RuntimeFilterRouteRole,
        peer: RuntimeFilterRoutePeer,
        allowed_kinds: BTreeSet<RuntimeFilterEnvelopeKind>,
    ) -> RuntimeFilterRoutingEdgeView {
        RuntimeFilterRoutingEdgeView::new(
            ChannelId::new(1),
            RouteEdgeId::new(edge_id),
            RuntimeFilterRouteEndpointView::new(pid(source_participant), source_role),
            RuntimeFilterRouteEndpointView::new(pid(target_participant), target_role),
            peer,
            allowed_kinds,
        )
        .unwrap()
    }

    fn manual_router(
        local_participant: u32,
        local_roles: BTreeSet<RuntimeFilterRouteRole>,
        producer_instances: BTreeMap<(BindingId, UniqueId), RuntimeFilterParticipantId>,
        inbound_edges: Vec<RuntimeFilterRoutingEdgeView>,
        outbound_edges: Vec<RuntimeFilterRoutingEdgeView>,
    ) -> RoleRouter {
        let channel = RuntimeFilterChannelRoutingView::new(
            ChannelId::new(1),
            local_roles,
            producer_instances,
            inbound_edges,
            outbound_edges,
        )
        .unwrap();
        RoleRouter::new(Arc::new(
            RuntimeFilterRoutingShard::new(
                DeploymentEpoch::new(9),
                pid(local_participant),
                BTreeMap::from([(ChannelId::new(1), channel)]),
            )
            .unwrap(),
        ))
    }

    #[test]
    fn producer_route_selects_the_unique_aggregator_edge() {
        let decision = router(7)
            .route_producer(
                RuntimeFilterProducerRouteIntent::new(
                    DeploymentEpoch::new(9),
                    ChannelId::new(1),
                    BindingId::new(10),
                    RuntimeFilterEnvelopeKind::Contribution,
                )
                .unwrap(),
            )
            .unwrap();

        assert!(decision.loopback_route_edge_ids().is_empty());
        assert_eq!(decision.remote_routes().len(), 1);
        assert_eq!(
            decision.remote_routes()[0].route_edge_id(),
            RouteEdgeId::new(2)
        );
        assert_eq!(decision.remote_routes()[0].peer_participant_id(), pid(2));
    }

    #[test]
    fn delivery_route_uses_only_requested_profile_edges() {
        let decision = router(2)
            .route_delivery(
                RuntimeFilterDeliveryRouteIntent::new(
                    DeploymentEpoch::new(9),
                    ChannelId::new(1),
                    vec![RouteEdgeId::new(5), RouteEdgeId::new(3)],
                    RuntimeFilterEnvelopeKind::Artifact,
                )
                .unwrap(),
            )
            .unwrap();

        assert_eq!(decision.loopback_route_edge_ids(), &[RouteEdgeId::new(3)]);
        assert_eq!(
            decision
                .remote_routes()
                .iter()
                .map(|route| route.route_edge_id())
                .collect::<Vec<_>>(),
            vec![RouteEdgeId::new(5)]
        );
        assert!(
            decision
                .remote_routes()
                .iter()
                .all(|route| route.route_edge_id() != RouteEdgeId::new(4))
        );
    }

    #[test]
    fn empty_delivery_scope_produces_an_empty_decision() {
        let decision = router(2)
            .route_delivery(
                RuntimeFilterDeliveryRouteIntent::new(
                    DeploymentEpoch::new(9),
                    ChannelId::new(1),
                    Vec::new(),
                    RuntimeFilterEnvelopeKind::Artifact,
                )
                .unwrap(),
            )
            .unwrap();

        assert!(decision.loopback_route_edge_ids().is_empty());
        assert!(decision.remote_routes().is_empty());
    }

    #[test]
    fn contribution_authorization_resolves_source_from_binding_and_finst() {
        let router = router(2);
        let edge = router
            .authorize_contribution(
                DeploymentEpoch::new(9),
                ChannelId::new(1),
                BindingId::new(10),
                finst(7),
                RuntimeFilterEnvelopeKind::Contribution,
            )
            .unwrap();

        assert_eq!(edge.route_edge_id(), RouteEdgeId::new(2));
        assert_eq!(edge.source().participant_id(), pid(7));
        assert_eq!(edge.target().role(), RuntimeFilterRouteRole::Aggregator);
    }

    #[test]
    fn routing_shard_construction_rejects_an_inbound_edge_targeting_another_participant() {
        // A participant's routing shard only ever holds inbound edges that target itself, so an
        // inbound producer edge whose Aggregator target is a remote participant is rejected while
        // building the shard. That construction guard is the reachable enforcement point; the
        // matching `InboundTargetMismatch` branch inside `authorize_contribution` is defensive and
        // cannot be reached through a validly-constructed shard.
        let producer = RuntimeFilterRouteRole::Producer(BindingId::new(10));
        let channel = RuntimeFilterChannelRoutingView::new(
            ChannelId::new(1),
            BTreeSet::from([RuntimeFilterRouteRole::Aggregator]),
            BTreeMap::from([((BindingId::new(10), finst(7)), pid(7))]),
            vec![edge(
                6,
                7,
                producer,
                11,
                RuntimeFilterRouteRole::Aggregator,
                RuntimeFilterRoutePeer::Remote {
                    participant_id: pid(7),
                    endpoint: RuntimeEndpoint::new("10.0.0.7", 9060).unwrap(),
                },
                BTreeSet::from([RuntimeFilterEnvelopeKind::Contribution]),
            )],
            Vec::new(),
        )
        .unwrap();

        assert_eq!(
            RuntimeFilterRoutingShard::new(
                DeploymentEpoch::new(9),
                pid(2),
                BTreeMap::from([(ChannelId::new(1), channel)]),
            )
            .unwrap_err(),
            RuntimeFilterRouteContractError::InvalidIncidentEdge {
                channel: ChannelId::new(1),
                edge: RouteEdgeId::new(6),
                detail: "inbound target is not local",
            }
        );
    }

    #[test]
    fn contribution_authorization_is_independent_of_zero_based_stream_coordinates() {
        let identity = ContributionIdentity::new(
            UniqueId::new(1, 2),
            pid(7),
            ChannelId::new(1),
            DeploymentEpoch::new(9),
            ProducerStreamId::new(BindingId::new(10), finst(7), PartitionId::new(0)),
            ProducerSequence::new(0),
        );
        assert_eq!(identity.stream().partition_id(), PartitionId::new(0));
        assert_eq!(identity.sequence(), ProducerSequence::new(0));

        assert!(
            router(2)
                .authorize_contribution(
                    identity.epoch(),
                    identity.channel_id(),
                    identity.stream().binding_id(),
                    identity.stream().fragment_instance_id(),
                    RuntimeFilterEnvelopeKind::Contribution,
                )
                .is_ok()
        );
    }

    #[test]
    fn producer_unavailable_can_authorize_a_to_aggregator_edge() {
        let router = router(2);
        let edge = router
            .authorize_contribution(
                DeploymentEpoch::new(9),
                ChannelId::new(1),
                BindingId::new(10),
                finst(7),
                RuntimeFilterEnvelopeKind::ProducerUnavailable,
            )
            .unwrap();

        assert_eq!(edge.target().role(), RuntimeFilterRouteRole::Aggregator);
    }

    #[test]
    fn router_rejects_stale_epoch_unknown_source_and_unknown_finst() {
        let producer = router(7);
        assert_eq!(
            producer
                .route_producer(
                    RuntimeFilterProducerRouteIntent::new(
                        DeploymentEpoch::new(8),
                        ChannelId::new(1),
                        BindingId::new(10),
                        RuntimeFilterEnvelopeKind::Contribution,
                    )
                    .unwrap(),
                )
                .unwrap_err(),
            RuntimeFilterRouteContractError::StaleEpoch {
                installed: DeploymentEpoch::new(9),
                incoming: DeploymentEpoch::new(8),
            }
        );
        assert_eq!(
            producer
                .route_producer(
                    RuntimeFilterProducerRouteIntent::new(
                        DeploymentEpoch::new(9),
                        ChannelId::new(1),
                        BindingId::new(99),
                        RuntimeFilterEnvelopeKind::Contribution,
                    )
                    .unwrap(),
                )
                .unwrap_err(),
            RuntimeFilterRouteContractError::UnknownSourceRole {
                channel: ChannelId::new(1),
                role: RuntimeFilterRouteRole::Producer(BindingId::new(99)),
            }
        );
        assert_eq!(
            router(2)
                .authorize_contribution(
                    DeploymentEpoch::new(9),
                    ChannelId::new(1),
                    BindingId::new(10),
                    finst(99),
                    RuntimeFilterEnvelopeKind::Contribution,
                )
                .unwrap_err(),
            RuntimeFilterRouteContractError::UnknownProducerInstance {
                channel: ChannelId::new(1),
                binding: BindingId::new(10),
                fragment_instance_id: finst(99),
            }
        );
    }

    #[test]
    fn producer_route_reports_topology_ambiguity_before_kind_rejection() {
        let producer_role = RuntimeFilterRouteRole::Producer(BindingId::new(10));
        let allowed = BTreeSet::from([RuntimeFilterEnvelopeKind::Contribution]);
        let remote = || RuntimeFilterRoutePeer::Remote {
            participant_id: pid(2),
            endpoint: RuntimeEndpoint::new("10.0.0.2", 9060).unwrap(),
        };
        let router = manual_router(
            7,
            BTreeSet::from([producer_role]),
            BTreeMap::new(),
            Vec::new(),
            vec![
                edge(
                    6,
                    7,
                    producer_role,
                    2,
                    RuntimeFilterRouteRole::Aggregator,
                    remote(),
                    allowed.clone(),
                ),
                edge(
                    7,
                    7,
                    producer_role,
                    2,
                    RuntimeFilterRouteRole::Aggregator,
                    remote(),
                    allowed,
                ),
            ],
        );

        assert_eq!(
            router
                .route_producer(
                    RuntimeFilterProducerRouteIntent::new(
                        DeploymentEpoch::new(9),
                        ChannelId::new(1),
                        BindingId::new(10),
                        RuntimeFilterEnvelopeKind::ProducerClosed,
                    )
                    .unwrap(),
                )
                .unwrap_err(),
            RuntimeFilterRouteContractError::AmbiguousOutboundRoute {
                channel: ChannelId::new(1),
                role: producer_role,
            }
        );
    }

    #[test]
    fn producer_route_rejects_missing_route_and_forbidden_kind() {
        let producer_role = RuntimeFilterRouteRole::Producer(BindingId::new(10));
        let no_route = manual_router(
            7,
            BTreeSet::from([producer_role]),
            BTreeMap::new(),
            Vec::new(),
            Vec::new(),
        );
        assert_eq!(
            no_route
                .route_producer(
                    RuntimeFilterProducerRouteIntent::new(
                        DeploymentEpoch::new(9),
                        ChannelId::new(1),
                        BindingId::new(10),
                        RuntimeFilterEnvelopeKind::Contribution,
                    )
                    .unwrap(),
                )
                .unwrap_err(),
            RuntimeFilterRouteContractError::ForbiddenOutboundKind {
                channel: ChannelId::new(1),
                role: producer_role,
                kind: RuntimeFilterEnvelopeKind::Contribution,
            }
        );

        let route = edge(
            6,
            7,
            producer_role,
            2,
            RuntimeFilterRouteRole::Aggregator,
            RuntimeFilterRoutePeer::Remote {
                participant_id: pid(2),
                endpoint: RuntimeEndpoint::new("10.0.0.2", 9060).unwrap(),
            },
            BTreeSet::from([RuntimeFilterEnvelopeKind::Contribution]),
        );
        let forbidden = manual_router(
            7,
            BTreeSet::from([producer_role]),
            BTreeMap::new(),
            Vec::new(),
            vec![route],
        );
        assert_eq!(
            forbidden
                .route_producer(
                    RuntimeFilterProducerRouteIntent::new(
                        DeploymentEpoch::new(9),
                        ChannelId::new(1),
                        BindingId::new(10),
                        RuntimeFilterEnvelopeKind::ProducerClosed,
                    )
                    .unwrap(),
                )
                .unwrap_err(),
            RuntimeFilterRouteContractError::ForbiddenOutboundKind {
                channel: ChannelId::new(1),
                role: producer_role,
                kind: RuntimeFilterEnvelopeKind::ProducerClosed,
            }
        );
    }

    #[test]
    fn delivery_route_rejects_duplicate_unknown_and_inbound_only_edges() {
        assert_eq!(
            RuntimeFilterDeliveryRouteIntent::new(
                DeploymentEpoch::new(9),
                ChannelId::new(1),
                vec![RouteEdgeId::new(3), RouteEdgeId::new(3)],
                RuntimeFilterEnvelopeKind::Artifact,
            )
            .unwrap_err(),
            RuntimeFilterRouteContractError::DuplicateRequestedRouteEdge {
                edge: RouteEdgeId::new(3),
            }
        );

        let router = router(2);
        for edge_id in [99, 2] {
            assert_eq!(
                router
                    .route_delivery(
                        RuntimeFilterDeliveryRouteIntent::new(
                            DeploymentEpoch::new(9),
                            ChannelId::new(1),
                            vec![RouteEdgeId::new(edge_id)],
                            RuntimeFilterEnvelopeKind::Artifact,
                        )
                        .unwrap(),
                    )
                    .unwrap_err(),
                RuntimeFilterRouteContractError::UnknownOutboundRoute {
                    channel: ChannelId::new(1),
                    edge: RouteEdgeId::new(edge_id),
                }
            );
        }
    }

    #[test]
    fn inbound_authorization_enforces_identity_family_before_edge_allowed_kinds() {
        let router = router(2);
        assert_eq!(
            router
                .authorize_contribution(
                    DeploymentEpoch::new(9),
                    ChannelId::new(1),
                    BindingId::new(10),
                    finst(7),
                    RuntimeFilterEnvelopeKind::Unavailable,
                )
                .unwrap_err(),
            RuntimeFilterRouteContractError::ForbiddenInboundKind {
                channel: ChannelId::new(1),
                edge: RouteEdgeId::new(2),
                kind: RuntimeFilterEnvelopeKind::Unavailable,
            }
        );
        assert_eq!(
            router
                .authorize_delivery(
                    DeploymentEpoch::new(9),
                    ChannelId::new(1),
                    RouteEdgeId::new(2),
                    RuntimeFilterEnvelopeKind::ProducerClosed,
                )
                .unwrap_err(),
            RuntimeFilterRouteContractError::ForbiddenInboundKind {
                channel: ChannelId::new(1),
                edge: RouteEdgeId::new(2),
                kind: RuntimeFilterEnvelopeKind::ProducerClosed,
            }
        );
    }

    #[test]
    fn contribution_authorization_distinguishes_missing_and_ambiguous_routes() {
        let aggregator = RuntimeFilterRouteRole::Aggregator;
        let producer = RuntimeFilterRouteRole::Producer(BindingId::new(10));
        let producer_instances = BTreeMap::from([((BindingId::new(10), finst(7)), pid(7))]);
        let missing = manual_router(
            2,
            BTreeSet::from([aggregator]),
            producer_instances.clone(),
            Vec::new(),
            Vec::new(),
        );
        assert_eq!(
            missing
                .authorize_contribution(
                    DeploymentEpoch::new(9),
                    ChannelId::new(1),
                    BindingId::new(10),
                    finst(7),
                    RuntimeFilterEnvelopeKind::Contribution,
                )
                .unwrap_err(),
            RuntimeFilterRouteContractError::UnknownInboundProducerRoute {
                channel: ChannelId::new(1),
                binding: BindingId::new(10),
                source_participant: pid(7),
            }
        );

        let inbound = |edge_id| {
            edge(
                edge_id,
                7,
                producer,
                2,
                aggregator,
                RuntimeFilterRoutePeer::Remote {
                    participant_id: pid(7),
                    endpoint: RuntimeEndpoint::new("10.0.0.7", 9060).unwrap(),
                },
                BTreeSet::from([RuntimeFilterEnvelopeKind::Contribution]),
            )
        };
        let ambiguous = manual_router(
            2,
            BTreeSet::from([aggregator]),
            producer_instances,
            vec![inbound(6), inbound(7)],
            Vec::new(),
        );
        assert_eq!(
            ambiguous
                .authorize_contribution(
                    DeploymentEpoch::new(9),
                    ChannelId::new(1),
                    BindingId::new(10),
                    finst(7),
                    RuntimeFilterEnvelopeKind::ProducerClosed,
                )
                .unwrap_err(),
            RuntimeFilterRouteContractError::AmbiguousInboundRoute {
                channel: ChannelId::new(1),
            }
        );
    }

    #[test]
    fn delivery_authorization_is_exact_and_rejects_forbidden_kind() {
        let consumer = router(7);
        assert_eq!(
            consumer
                .authorize_delivery(
                    DeploymentEpoch::new(9),
                    ChannelId::new(1),
                    RouteEdgeId::new(99),
                    RuntimeFilterEnvelopeKind::Artifact,
                )
                .unwrap_err(),
            RuntimeFilterRouteContractError::UnknownInboundRoute {
                channel: ChannelId::new(1),
                edge: RouteEdgeId::new(99),
            }
        );

        let edge = consumer
            .authorize_delivery(
                DeploymentEpoch::new(9),
                ChannelId::new(1),
                RouteEdgeId::new(4),
                RuntimeFilterEnvelopeKind::Artifact,
            )
            .unwrap();
        assert_eq!(edge.route_edge_id(), RouteEdgeId::new(4));
        assert_eq!(
            edge.target().role(),
            RuntimeFilterRouteRole::Consumer(BindingId::new(20))
        );
    }
}
