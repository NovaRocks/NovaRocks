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

//! Native fragment envelope and static execution-contract projections.
//!
//! This module validates only protobuf shape and projects immutable wire
//! values into Execution contracts.  It has no Backend context lookup, task
//! state, Connector I/O, or fragment lifecycle authority.

use std::collections::{BTreeMap, BTreeSet};

use novarocks_execution::exec::fragment::program::{
    FragmentNodeId, ScanAssignmentKind, ScanSourceContract,
};
use novarocks_execution::runtime::endpoint::FragmentDestination;
use novarocks_execution::runtime::fragment::FragmentSinkAssignment;
use novarocks_execution_contract::task_execution::descriptor::{
    DataStreamPartitionType, ExchangeEdge, ExchangeTopology,
};
use novarocks_proto_codec::lifecycle::ScanRangeParams;
use novarocks_proto_codec::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::plan;
use novarocks_types::UniqueId;

use crate::fragment_instance::task_sink_edge_ids_path;

pub fn require_root(
    fragment: &plan::PlanFragment,
) -> Result<&plan::DistributedNode, ProtocolError> {
    fragment.root.as_ref().ok_or_else(|| {
        error(
            FieldPath::root("plan_fragment").field("root"),
            ProtocolErrorKind::MissingField,
            "native PlanFragment requires root",
        )
    })
}

pub fn require_sink(fragment: &plan::PlanFragment) -> Result<&plan::DataSink, ProtocolError> {
    fragment.sink.as_ref().ok_or_else(|| {
        error(
            FieldPath::root("plan_fragment").field("sink"),
            ProtocolErrorKind::MissingField,
            "native PlanFragment requires sink",
        )
    })
}

pub fn decode_scan_source_contracts(
    root: &plan::DistributedNode,
    path: FieldPath,
) -> Result<BTreeMap<FragmentNodeId, ScanSourceContract>, ProtocolError> {
    let mut assignments = BTreeMap::new();
    visit_scan_contracts(root, path, &mut assignments)?;
    Ok(assignments)
}

/// Refuse scan ranges that do not name a scan source frozen by the plan.
///
/// Both inputs are immutable native-wire projections, so this check belongs
/// beside their decoding rather than in Backend plan lowering.
pub fn validate_scan_range_nodes(
    contracts: &BTreeMap<FragmentNodeId, ScanSourceContract>,
    raw_ranges: &BTreeMap<FragmentNodeId, Vec<ScanRangeParams>>,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    for node_id in raw_ranges.keys() {
        if !contracts.contains_key(node_id) {
            return Err(error(
                path.clone().map_key(node_id.get().to_string()),
                ProtocolErrorKind::InconsistentFields,
                format!(
                    "scan ranges assigned to unknown scan node {}",
                    node_id.get()
                ),
            ));
        }
    }
    Ok(())
}

fn visit_scan_contracts(
    node: &plan::DistributedNode,
    path: FieldPath,
    assignments: &mut BTreeMap<FragmentNodeId, ScanSourceContract>,
) -> Result<(), ProtocolError> {
    if let Some(plan::distributed_node::Payload::Physical(physical)) = node.payload.as_ref()
        && let Some(plan::plan_node::Kind::Scan(scan)) = physical.kind.as_ref()
    {
        let scan_path = path
            .clone()
            .field("payload")
            .field("physical")
            .field("scan");
        let table = scan.table.as_ref().ok_or_else(|| {
            error(
                scan_path.clone().field("table"),
                ProtocolErrorKind::MissingField,
                format!("native ScanNode node_id={} requires table", node.node_id),
            )
        })?;
        let source = table.source.as_ref().ok_or_else(|| {
            error(
                scan_path.clone().field("table").field("source"),
                ProtocolErrorKind::MissingField,
                format!("native ScanNode node_id={} requires source", node.node_id),
            )
        })?;
        let source = source.kind.as_ref().ok_or_else(|| {
            error(
                scan_path
                    .clone()
                    .field("table")
                    .field("source")
                    .field("kind"),
                ProtocolErrorKind::MissingField,
                format!(
                    "native ScanNode node_id={} requires source kind",
                    node.node_id
                ),
            )
        })?;
        let _ = source;
        if assignments
            .insert(
                FragmentNodeId::new(node.node_id),
                ScanSourceContract::new(ScanAssignmentKind::File),
            )
            .is_some()
        {
            return Err(error(
                path.clone().field("node_id"),
                ProtocolErrorKind::InconsistentFields,
                format!("native plan has duplicate scan node_id={}", node.node_id),
            ));
        }
    }
    for (index, child) in node.children.iter().enumerate() {
        visit_scan_contracts(
            child,
            path.clone().field("children").index(index),
            assignments,
        )?;
    }
    Ok(())
}

/// Binds a fragment's static sink to the task's frozen outbound edges.
///
/// `sink_edge_ids` is the task assignment's ordered binding: entry `i` names
/// the outbound topology edge serving static sink branch `i`. Every branch
/// must be bound exactly once, to an edge that targets the branch's exchange
/// node with the branch's partitioning; a mismatch is refused rather than
/// resolved by guessing an edge from its node. `source` is this task's own
/// kernel key, which every destination counts the frames it sends under.
pub fn decode_fragment_sink_assignment(
    sink: &plan::DataSink,
    sink_edge_ids: &[u32],
    source: UniqueId,
    topology: &ExchangeTopology,
) -> Result<FragmentSinkAssignment, ProtocolError> {
    let path = FieldPath::root("plan_fragment").field("sink");
    let kind = sink.kind.as_ref().ok_or_else(|| {
        error(
            path.clone().field("kind"),
            ProtocolErrorKind::MissingField,
            "native PlanFragment sink requires kind",
        )
    })?;
    let expected = match kind {
        plan::data_sink::Kind::DataStream(stream) => vec![(
            stream.dest_node_id,
            decode_stream_partition(stream, path.clone().field("data_stream"))?,
        )],
        plan::data_sink::Kind::MultiCastDataStream(grouped) => grouped
            .sinks
            .iter()
            .enumerate()
            .map(|(index, stream)| {
                Ok((
                    stream.dest_node_id,
                    decode_stream_partition(
                        stream,
                        path.clone()
                            .field("multi_cast_data_stream")
                            .field("sinks")
                            .index(index),
                    )?,
                ))
            })
            .collect::<Result<Vec<_>, ProtocolError>>()?,
        plan::data_sink::Kind::ChangeStreamRouter(router) => router
            .routes
            .iter()
            .enumerate()
            .map(|(index, route)| {
                let branch_path = path
                    .clone()
                    .field("change_stream_router")
                    .field("routes")
                    .index(index);
                let kind = route
                    .output_partition
                    .as_ref()
                    .map(|partition| partition.kind)
                    .unwrap_or_else(|| {
                        if route.output_partition_ordinals.is_empty() {
                            plan::PartitionKind::Unpartitioned as i32
                        } else {
                            plan::PartitionKind::Hash as i32
                        }
                    });
                Ok((
                    route.target_exchange_node_id,
                    decode_partition_kind(
                        kind,
                        branch_path.field("output_partition").field("kind"),
                    )?,
                ))
            })
            .collect::<Result<Vec<_>, ProtocolError>>()?,
        plan::data_sink::Kind::Result(_) | plan::data_sink::Kind::Noop(_) => Vec::new(),
    };
    let edges = decode_sink_edges(&expected, sink_edge_ids, topology, path.clone())?;
    let mut groups = edges
        .into_iter()
        .map(|edge| decode_edge_destinations(edge, source))
        .collect::<Result<Vec<_>, _>>()?;
    match kind {
        plan::data_sink::Kind::DataStream(_) => Ok(FragmentSinkAssignment::StreamDestinations {
            destinations: groups.pop().expect("one stream edge was validated"),
            sender_id: None,
        }),
        plan::data_sink::Kind::MultiCastDataStream(grouped) => {
            debug_assert_eq!(groups.len(), grouped.sinks.len());
            Ok(FragmentSinkAssignment::DestinationGroups {
                groups,
                sender_id: None,
            })
        }
        plan::data_sink::Kind::ChangeStreamRouter(router) => {
            debug_assert_eq!(groups.len(), router.routes.len());
            Ok(FragmentSinkAssignment::DestinationGroups {
                groups,
                sender_id: None,
            })
        }
        plan::data_sink::Kind::Result(_) | plan::data_sink::Kind::Noop(_) => {
            Ok(FragmentSinkAssignment::None)
        }
    }
}

fn decode_sink_edges<'a>(
    expected: &[(i32, DataStreamPartitionType)],
    sink_edge_ids: &[u32],
    topology: &'a ExchangeTopology,
    sink_path: FieldPath,
) -> Result<Vec<&'a ExchangeEdge>, ProtocolError> {
    let ids_path = task_sink_edge_ids_path();
    if sink_edge_ids.len() != expected.len() || topology.outbound().len() != expected.len() {
        return Err(error(
            ids_path,
            ProtocolErrorKind::InconsistentFields,
            "sink edge assignment must cover each static branch and outbound edge exactly once",
        ));
    }
    let mut seen = BTreeSet::new();
    expected
        .iter()
        .zip(sink_edge_ids)
        .enumerate()
        .map(|(index, ((node_id, partitioning), edge_id))| {
            let id_path = ids_path.index(index);
            if *edge_id == 0 || !seen.insert(*edge_id) {
                return Err(error(
                    id_path,
                    ProtocolErrorKind::InvalidValue,
                    "sink edge id must be nonzero and used only once",
                ));
            }
            let edge = topology
                .outbound()
                .iter()
                .find(|edge| edge.edge_id().get() == *edge_id)
                .ok_or_else(|| {
                    error(
                        id_path,
                        ProtocolErrorKind::InconsistentFields,
                        "sink edge id is absent from the task topology",
                    )
                })?;
            if edge.destination_node_id().get() != *node_id {
                return Err(error(
                    sink_path.clone(),
                    ProtocolErrorKind::InconsistentFields,
                    "static sink branch destination node disagrees with its assigned edge",
                ));
            }
            if edge.partitioning() != *partitioning {
                return Err(error(
                    sink_path.clone(),
                    ProtocolErrorKind::InconsistentFields,
                    "static sink branch partitioning disagrees with its assigned edge",
                ));
            }
            Ok(edge)
        })
        .collect()
}

fn decode_stream_partition(
    stream: &plan::DataStreamSink,
    path: FieldPath,
) -> Result<DataStreamPartitionType, ProtocolError> {
    let partition = stream.output_partition.as_ref().ok_or_else(|| {
        error(
            path.clone().field("output_partition"),
            ProtocolErrorKind::MissingField,
            "native data stream sink requires output partitioning",
        )
    })?;
    decode_partition_kind(partition.kind, path.field("output_partition").field("kind"))
}

fn decode_partition_kind(
    kind: i32,
    path: FieldPath,
) -> Result<DataStreamPartitionType, ProtocolError> {
    match plan::PartitionKind::try_from(kind) {
        Ok(plan::PartitionKind::Unpartitioned) => Ok(DataStreamPartitionType::Unpartitioned),
        Ok(plan::PartitionKind::Random) => Ok(DataStreamPartitionType::Random),
        Ok(plan::PartitionKind::Hash) => Ok(DataStreamPartitionType::HashPartitioned),
        Ok(plan::PartitionKind::Unspecified) | Err(_) => Err(error(
            path,
            ProtocolErrorKind::InvalidEnum,
            "native sink partitioning must be a known non-default value",
        )),
    }
}

/// Projects one outbound edge onto the kernel's destination list.
///
/// The producer's sender position belongs to the edge, not to any one
/// destination: every destination of the edge counts this producer at the
/// same place in its target node's complete producer union, so each kernel
/// destination is given the edge's one ordinal and count.
fn decode_edge_destinations(
    edge: &ExchangeEdge,
    source: UniqueId,
) -> Result<Vec<FragmentDestination>, ProtocolError> {
    edge.destinations()
        .iter()
        .enumerate()
        .map(|(index, destination)| {
            let path = FieldPath::root("task_descriptor")
                .field("topology")
                .field("outbound")
                .map_key(edge.edge_id().get().to_string())
                .field("destinations")
                .index(index);
            FragmentDestination::new(
                destination.fragment_instance_id(),
                destination.endpoint().clone(),
                source,
                edge.sender_ordinal(),
                edge.sender_count().get(),
            )
            .map(|value| value.with_task_identity(destination.task()))
            .map_err(|detail| error(path, ProtocolErrorKind::InvalidValue, detail))
        })
        .collect()
}

fn error(path: FieldPath, kind: ProtocolErrorKind, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, kind, detail)
}

#[cfg(test)]
mod tests {
    use super::{
        decode_fragment_sink_assignment, decode_scan_source_contracts, require_root, require_sink,
        validate_scan_range_nodes,
    };
    use std::collections::BTreeMap;
    use std::num::NonZeroU32;

    use novarocks_execution::exec::fragment::program::{FragmentNodeId, ScanAssignmentKind};
    use novarocks_execution::runtime::fragment::FragmentSinkAssignment;
    use novarocks_execution_contract::task_execution::descriptor::{
        DataStreamPartitionType, ExchangeDestination, ExchangeEdge, ExchangeTopology,
        RuntimeEndpoint,
    };
    use novarocks_execution_contract::task_execution::domain::ExchangeEdgeId;
    use novarocks_execution_contract::task_execution::identity::TaskIdentity;
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_models::plan;
    use novarocks_types::UniqueId;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };

    #[test]
    fn preserves_missing_fragment_envelope_errors() {
        let root = require_root(&plan::PlanFragment::default()).expect_err("root is required");
        assert_eq!(
            root.to_string(),
            "native protocol error at plan_fragment.root (missing field): native PlanFragment requires root"
        );
        let sink = require_sink(&plan::PlanFragment::default()).expect_err("sink is required");
        assert_eq!(
            sink.to_string(),
            "native protocol error at plan_fragment.sink (missing field): native PlanFragment requires sink"
        );
    }

    #[expect(
        clippy::needless_update,
        reason = "The fixture keeps explicit defaulted proto fields for wire-contract readability."
    )]
    #[test]
    fn classifies_typed_connector_read_as_file_assignment() {
        let root = plan::DistributedNode {
            node_id: 17,
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                kind: Some(plan::plan_node::Kind::Scan(plan::ScanNode {
                    table: Some(plan::TableDef {
                        source: Some(plan::ScanSource {
                            kind: Some(plan::scan_source::Kind::TypedConnectorRead(
                                Default::default(),
                            )),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            })),
            ..Default::default()
        };
        let contracts =
            decode_scan_source_contracts(&root, FieldPath::root("plan_fragment").field("root"))
                .expect("decode scan contract");
        assert_eq!(
            contracts
                .get(&FragmentNodeId::new(17))
                .map(|contract| contract.assignment_kind()),
            Some(ScanAssignmentKind::File)
        );
    }

    #[test]
    fn preserves_missing_scan_source_path() {
        let root = plan::DistributedNode {
            node_id: 17,
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                kind: Some(plan::plan_node::Kind::Scan(plan::ScanNode {
                    table: Some(plan::TableDef::default()),
                    ..Default::default()
                })),
                ..Default::default()
            })),
            ..Default::default()
        };
        let error =
            decode_scan_source_contracts(&root, FieldPath::root("plan_fragment").field("root"))
                .expect_err("missing source must fail");
        assert_eq!(
            error.to_string(),
            "native protocol error at plan_fragment.root.payload.physical.scan.table.source (missing field): native ScanNode node_id=17 requires source"
        );
    }

    #[test]
    fn rejects_scan_ranges_for_unknown_scan_node() {
        let mut contracts = BTreeMap::new();
        contracts.insert(
            FragmentNodeId::new(17),
            novarocks_execution::exec::fragment::program::ScanSourceContract::new(
                ScanAssignmentKind::File,
            ),
        );
        let mut ranges = BTreeMap::new();
        ranges.insert(FragmentNodeId::new(19), Vec::new());

        let error = validate_scan_range_nodes(
            &contracts,
            &ranges,
            crate::fragment_instance::task_scan_ranges_path(),
        )
        .expect_err("unknown scan range node must fail");
        assert_eq!(
            error.to_string(),
            "native protocol error at creation_metadata.assignment.initial_scan_ranges[\"19\"] (inconsistent fields): scan ranges assigned to unknown scan node 19"
        );
    }

    const SOURCE: UniqueId = UniqueId::new(5, 6);

    #[test]
    fn stream_sink_requires_exactly_one_assigned_edge() {
        let error = decode_fragment_sink_assignment(
            &plan::DataSink {
                kind: Some(plan::data_sink::Kind::DataStream(plan::DataStreamSink {
                    output_partition: Some(plan::DataPartition {
                        kind: plan::PartitionKind::Unpartitioned as i32,
                        exprs: Vec::new(),
                    }),
                    ..Default::default()
                })),
            },
            &[],
            SOURCE,
            &novarocks_execution_contract::task_execution::descriptor::ExchangeTopology::default(),
        )
        .expect_err("edge assignment is required");
        assert_eq!(
            error.to_string(),
            "native protocol error at creation_metadata.assignment.sink_edge_ids (inconsistent fields): sink edge assignment must cover each static branch and outbound edge exactly once"
        );
    }

    #[test]
    fn multicast_sink_requires_one_edge_per_branch() {
        let error = decode_fragment_sink_assignment(
            &plan::DataSink {
                kind: Some(plan::data_sink::Kind::MultiCastDataStream(
                    plan::MultiCastDataStreamSink {
                        sinks: vec![plan::DataStreamSink {
                            output_partition: Some(plan::DataPartition {
                                kind: plan::PartitionKind::Unpartitioned as i32,
                                exprs: Vec::new(),
                            }),
                            ..Default::default()
                        }],
                    },
                )),
            },
            &[],
            SOURCE,
            &novarocks_execution_contract::task_execution::descriptor::ExchangeTopology::default(),
        )
        .expect_err("the multicast branch needs an edge");
        assert_eq!(
            error.to_string(),
            "native protocol error at creation_metadata.assignment.sink_edge_ids (inconsistent fields): sink edge assignment must cover each static branch and outbound edge exactly once"
        );
    }

    fn task(execution: QueryExecutionId, task: u32) -> TaskIdentity {
        TaskIdentity::new(
            execution,
            StageId::new(2).expect("nonzero stage"),
            TaskId::new(task).expect("nonzero task"),
            BackendProcessId::new_v7(),
        )
    }

    fn execution() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(1, 2),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect("execution id")
    }

    fn unpartitioned_stream(dest_node_id: i32) -> plan::DataStreamSink {
        plan::DataStreamSink {
            dest_node_id,
            output_partition: Some(plan::DataPartition {
                kind: plan::PartitionKind::Unpartitioned as i32,
                exprs: Vec::new(),
            }),
            ..Default::default()
        }
    }

    /// The producer's sender position is one fact of its edge. Every
    /// destination the edge fans out to must count this producer at that one
    /// position, out of the target node's complete producer union -- which
    /// here is larger than this edge's own single producer.
    #[test]
    fn every_destination_of_an_edge_counts_the_producer_at_the_edges_position() {
        let execution = execution();
        let edge = ExchangeEdge::try_new(
            ExchangeEdgeId::new(3).expect("nonzero edge"),
            FragmentNodeId::new(9),
            DataStreamPartitionType::Unpartitioned,
            (1..=3)
                .map(|task_id| {
                    ExchangeDestination::new(
                        task(execution, task_id),
                        UniqueId::new(i64::from(task_id), 1),
                        RuntimeEndpoint::new("be.local", 8060).expect("endpoint"),
                        FragmentNodeId::new(9),
                    )
                })
                .collect(),
            4,
            NonZeroU32::new(6).expect("sender count"),
        )
        .expect("edge");
        let topology = ExchangeTopology::try_new(vec![edge], vec![]).expect("topology");
        let assignment = decode_fragment_sink_assignment(
            &plan::DataSink {
                kind: Some(plan::data_sink::Kind::DataStream(unpartitioned_stream(9))),
            },
            &[3],
            SOURCE,
            &topology,
        )
        .expect("the one stream branch is bound to the one edge");
        let FragmentSinkAssignment::StreamDestinations { destinations, .. } = assignment else {
            panic!("a stream sink binds one destination list");
        };
        assert_eq!(destinations.len(), 3);
        for destination in &destinations {
            assert_eq!(destination.source_finst_id(), SOURCE);
            assert_eq!(
                (destination.sender_ordinal(), destination.sender_count()),
                (4, 6),
                "each destination reads the edge's producer position"
            );
        }
    }

    #[test]
    fn multicast_sink_uses_explicit_edge_ids_even_when_branch_nodes_match() {
        let execution = execution();
        let destination = |edge_id: u32, kernel_id: i64| {
            ExchangeEdge::try_new(
                ExchangeEdgeId::new(edge_id).expect("nonzero edge"),
                FragmentNodeId::new(9),
                DataStreamPartitionType::Unpartitioned,
                vec![ExchangeDestination::new(
                    task(execution, edge_id),
                    UniqueId::new(kernel_id, 1),
                    RuntimeEndpoint::new("be.local", 8060).expect("endpoint"),
                    FragmentNodeId::new(9),
                )],
                0,
                NonZeroU32::new(1).expect("sender count"),
            )
            .expect("edge")
        };
        let topology =
            ExchangeTopology::try_new(vec![destination(1, 11), destination(2, 22)], vec![])
                .expect("topology");
        let sink = plan::DataSink {
            kind: Some(plan::data_sink::Kind::MultiCastDataStream(
                plan::MultiCastDataStreamSink {
                    sinks: vec![unpartitioned_stream(9), unpartitioned_stream(9)],
                },
            )),
        };
        let assignment = decode_fragment_sink_assignment(&sink, &[2, 1], SOURCE, &topology)
            .expect("explicit edge ids disambiguate the groups");
        let FragmentSinkAssignment::DestinationGroups { groups, .. } = assignment else {
            panic!("multicast requires grouped destinations");
        };
        assert_eq!(*groups[0][0].finst_id(), UniqueId::new(22, 1));
        assert_eq!(*groups[1][0].finst_id(), UniqueId::new(11, 1));
        assert_eq!(groups[0][0].source_finst_id(), UniqueId::new(5, 6));

        let error = decode_fragment_sink_assignment(&sink, &[1, 1], SOURCE, &topology)
            .expect_err("one edge cannot serve two branches");
        assert!(
            error
                .to_string()
                .contains("sink edge id must be nonzero and used only once")
        );

        let mut wrong_partition = sink;
        let Some(plan::data_sink::Kind::MultiCastDataStream(grouped)) =
            wrong_partition.kind.as_mut()
        else {
            unreachable!("test sink is multicast");
        };
        grouped.sinks[0]
            .output_partition
            .as_mut()
            .expect("partition")
            .kind = plan::PartitionKind::Hash as i32;
        let error = decode_fragment_sink_assignment(&wrong_partition, &[2, 1], SOURCE, &topology)
            .expect_err("static partition must agree with assigned edge");
        assert!(
            error
                .to_string()
                .contains("partitioning disagrees with its assigned edge")
        );
    }
}
