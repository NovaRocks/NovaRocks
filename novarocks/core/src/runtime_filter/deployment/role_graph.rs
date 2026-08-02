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

use crate::runtime_filter::model::contract::{BindingId, ChannelId};
use crate::runtime_filter::model::coverage::Coverage;
use crate::runtime_filter::port::identity::{RouteEdgeId, RuntimeFilterParticipantId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteKind {
    Loopback,
    ReplicaDirect,
    ToAggregator,
    FromAggregator,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct RouteEndpoint {
    pub participant: RuntimeFilterParticipantId,
    pub binding: BindingId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouteEdge {
    pub channel: ChannelId,
    pub edge_id: RouteEdgeId,
    pub kind: RouteKind,
    pub from: RouteEndpoint,
    pub to: RouteEndpoint,
}

/// Per-channel role graph. `producers`/`consumers` map a participant to the
/// binding ids it hosts. `aggregator` is set only for `AllOf` (sharded) channels.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChannelRoleGraph {
    pub channel_id: ChannelId,
    pub producers: BTreeMap<RuntimeFilterParticipantId, BTreeSet<BindingId>>,
    pub consumers: BTreeMap<RuntimeFilterParticipantId, BTreeSet<BindingId>>,
    pub aggregator: Option<RuntimeFilterParticipantId>,
    pub routes: Vec<RouteEdge>,
}

impl ChannelRoleGraph {
    pub fn empty(channel_id: ChannelId) -> Self {
        Self {
            channel_id,
            producers: BTreeMap::new(),
            consumers: BTreeMap::new(),
            aggregator: None,
            routes: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RoleGraph {
    pub channels: BTreeMap<ChannelId, ChannelRoleGraph>,
}

/// Deterministic, query-global route-edge id allocator (starts at 1; 0 reserved).
#[derive(Debug)]
pub struct RouteEdgeAllocator {
    next: u32,
}

impl RouteEdgeAllocator {
    pub fn new() -> Self {
        Self { next: 1 }
    }
    fn alloc(&mut self) -> RouteEdgeId {
        let id = RouteEdgeId::new(self.next);
        self.next += 1;
        id
    }
}

/// Compiler-projected placement of one producer binding across participants.
#[derive(Clone, Debug)]
pub struct ProducerPlacement {
    pub binding: BindingId,
    pub participants: BTreeSet<RuntimeFilterParticipantId>,
}

/// Compiler-projected placement of one consumer binding across participants.
#[derive(Clone, Debug)]
pub struct ConsumerPlacement {
    pub binding: BindingId,
    pub participants: BTreeSet<RuntimeFilterParticipantId>,
}

#[derive(Clone, Debug)]
pub struct ChannelRoleInputs {
    pub channel_id: ChannelId,
    pub availability_coverage: Coverage,
    pub producers: Vec<ProducerPlacement>,
    pub consumers: Vec<ConsumerPlacement>,
}

fn producer_participants(inputs: &ChannelRoleInputs) -> BTreeSet<RuntimeFilterParticipantId> {
    inputs
        .producers
        .iter()
        .flat_map(|p| p.participants.iter().copied())
        .collect()
}
fn consumer_participants(inputs: &ChannelRoleInputs) -> BTreeSet<RuntimeFilterParticipantId> {
    inputs
        .consumers
        .iter()
        .flat_map(|c| c.participants.iter().copied())
        .collect()
}

/// Build one channel's role graph. `replica_redundancy` clamps how many
/// `AnyOf` replica producers deliver directly (never a hardcoded BE count).
pub fn build_channel_role_graph(
    inputs: &ChannelRoleInputs,
    replica_redundancy: u32,
    alloc: &mut RouteEdgeAllocator,
) -> ChannelRoleGraph {
    let mut cg = ChannelRoleGraph::empty(inputs.channel_id);
    for p in &inputs.producers {
        for part in &p.participants {
            cg.producers.entry(*part).or_default().insert(p.binding);
        }
    }
    for c in &inputs.consumers {
        for part in &c.participants {
            cg.consumers.entry(*part).or_default().insert(c.binding);
        }
    }

    let prod_parts = producer_participants(inputs);
    let cons_parts = consumer_participants(inputs);
    let co_located = prod_parts == cons_parts && prod_parts.len() == 1;

    if co_located {
        let part = *prod_parts.iter().next().expect("one participant");
        for p in &inputs.producers {
            for c in &inputs.consumers {
                cg.routes.push(RouteEdge {
                    channel: inputs.channel_id,
                    edge_id: alloc.alloc(),
                    kind: RouteKind::Loopback,
                    from: RouteEndpoint {
                        participant: part,
                        binding: p.binding,
                    },
                    to: RouteEndpoint {
                        participant: part,
                        binding: c.binding,
                    },
                });
            }
        }
        return cg;
    }

    match &inputs.availability_coverage {
        Coverage::AnyOf(_) => {
            let mut senders: Vec<RuntimeFilterParticipantId> = prod_parts.iter().copied().collect();
            let cap = (replica_redundancy as usize).max(1).min(senders.len());
            senders.truncate(cap);
            for p in &inputs.producers {
                for sp in &senders {
                    if !p.participants.contains(sp) {
                        continue;
                    }
                    for c in &inputs.consumers {
                        for cp in &c.participants {
                            cg.routes.push(RouteEdge {
                                channel: inputs.channel_id,
                                edge_id: alloc.alloc(),
                                kind: RouteKind::ReplicaDirect,
                                from: RouteEndpoint {
                                    participant: *sp,
                                    binding: p.binding,
                                },
                                to: RouteEndpoint {
                                    participant: *cp,
                                    binding: c.binding,
                                },
                            });
                        }
                    }
                }
            }
        }
        _ => {
            let aggregator = prod_parts.iter().copied().next();
            cg.aggregator = aggregator;
            if let Some(agg) = aggregator {
                for p in &inputs.producers {
                    for part in &p.participants {
                        cg.routes.push(RouteEdge {
                            channel: inputs.channel_id,
                            edge_id: alloc.alloc(),
                            kind: RouteKind::ToAggregator,
                            from: RouteEndpoint {
                                participant: *part,
                                binding: p.binding,
                            },
                            to: RouteEndpoint {
                                participant: agg,
                                binding: p.binding,
                            },
                        });
                    }
                }
                for c in &inputs.consumers {
                    for part in &c.participants {
                        cg.routes.push(RouteEdge {
                            channel: inputs.channel_id,
                            edge_id: alloc.alloc(),
                            kind: RouteKind::FromAggregator,
                            from: RouteEndpoint {
                                participant: agg,
                                binding: c.binding,
                            },
                            to: RouteEndpoint {
                                participant: *part,
                                binding: c.binding,
                            },
                        });
                    }
                }
            }
        }
    }
    cg
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_filter::model::contract::CoverageWitnessId;

    fn pid(x: u32) -> RuntimeFilterParticipantId {
        RuntimeFilterParticipantId::new(x)
    }

    #[test]
    fn co_located_channel_is_loopback() {
        let inputs = ChannelRoleInputs {
            channel_id: ChannelId::new(1),
            availability_coverage: Coverage::Leaf(CoverageWitnessId::new(1)),
            producers: vec![ProducerPlacement {
                binding: BindingId::new(10),
                participants: BTreeSet::from([pid(3)]),
            }],
            consumers: vec![ConsumerPlacement {
                binding: BindingId::new(11),
                participants: BTreeSet::from([pid(3)]),
            }],
        };
        let mut alloc = RouteEdgeAllocator::new();
        let cg = build_channel_role_graph(&inputs, 2, &mut alloc);
        assert!(!cg.routes.is_empty());
        assert!(cg.routes.iter().all(|r| r.kind == RouteKind::Loopback));
        assert_eq!(cg.aggregator, None);
    }

    #[test]
    fn any_of_channel_is_replica_direct() {
        let inputs = ChannelRoleInputs {
            channel_id: ChannelId::new(1),
            availability_coverage: Coverage::AnyOf(vec![
                Coverage::Leaf(CoverageWitnessId::new(1)),
                Coverage::Leaf(CoverageWitnessId::new(2)),
            ]),
            producers: vec![
                ProducerPlacement {
                    binding: BindingId::new(10),
                    participants: BTreeSet::from([pid(3)]),
                },
                ProducerPlacement {
                    binding: BindingId::new(10),
                    participants: BTreeSet::from([pid(1)]),
                },
            ],
            consumers: vec![ConsumerPlacement {
                binding: BindingId::new(11),
                participants: BTreeSet::from([pid(2)]),
            }],
        };
        let mut alloc = RouteEdgeAllocator::new();
        let cg = build_channel_role_graph(&inputs, 2, &mut alloc);
        assert!(cg.routes.iter().any(|r| r.kind == RouteKind::ReplicaDirect));
        assert_eq!(cg.aggregator, None);
    }

    #[test]
    fn all_of_channel_uses_aggregator() {
        let inputs = ChannelRoleInputs {
            channel_id: ChannelId::new(1),
            availability_coverage: Coverage::AllOf(vec![
                Coverage::Leaf(CoverageWitnessId::new(1)),
                Coverage::Leaf(CoverageWitnessId::new(2)),
            ]),
            producers: vec![
                ProducerPlacement {
                    binding: BindingId::new(10),
                    participants: BTreeSet::from([pid(3)]),
                },
                ProducerPlacement {
                    binding: BindingId::new(10),
                    participants: BTreeSet::from([pid(1)]),
                },
            ],
            consumers: vec![ConsumerPlacement {
                binding: BindingId::new(11),
                participants: BTreeSet::from([pid(2)]),
            }],
        };
        let mut alloc = RouteEdgeAllocator::new();
        let cg = build_channel_role_graph(&inputs, 2, &mut alloc);
        assert!(cg.aggregator.is_some());
        assert!(cg.routes.iter().any(|r| r.kind == RouteKind::ToAggregator));
        assert!(
            cg.routes
                .iter()
                .any(|r| r.kind == RouteKind::FromAggregator)
        );
    }

    #[test]
    fn route_edge_ids_are_unique() {
        let inputs = ChannelRoleInputs {
            channel_id: ChannelId::new(1),
            availability_coverage: Coverage::Leaf(CoverageWitnessId::new(1)),
            producers: vec![ProducerPlacement {
                binding: BindingId::new(10),
                participants: BTreeSet::from([pid(3)]),
            }],
            consumers: vec![
                ConsumerPlacement {
                    binding: BindingId::new(11),
                    participants: BTreeSet::from([pid(3)]),
                },
                ConsumerPlacement {
                    binding: BindingId::new(12),
                    participants: BTreeSet::from([pid(3)]),
                },
            ],
        };
        let mut alloc = RouteEdgeAllocator::new();
        let cg = build_channel_role_graph(&inputs, 2, &mut alloc);
        let ids: BTreeSet<u32> = cg.routes.iter().map(|r| r.edge_id.get()).collect();
        assert_eq!(ids.len(), cg.routes.len());
    }
}
