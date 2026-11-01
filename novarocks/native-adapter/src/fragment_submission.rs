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

//! Native static sink projection against the task's frozen outbound edges.

use crate::fragment_instance::task_sink_edge_ids_path;
use novarocks_execution::runtime::endpoint::FragmentDestination;
use novarocks_execution::runtime::fragment::FragmentSinkAssignment;
use novarocks_execution_contract::task_execution::descriptor::{
    DataStreamPartitionType, ExchangeEdge, ExchangeTopology,
};
use novarocks_proto_codec::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_types::UniqueId;
use std::collections::BTreeSet;

/// Binds a compiled program's static sink to the task's frozen outbound
/// edges, with the exact frozen edge law: each
/// branch's destination node and partitioning come from the compiled static
/// sink, never from a legacy plan tree.
pub fn compiled_fragment_sink_assignment(
    sink: &novarocks_local_program::StaticSinkProgram,
    sink_edge_ids: &[u32],
    source: UniqueId,
    topology: &ExchangeTopology,
) -> Result<FragmentSinkAssignment, ProtocolError> {
    use novarocks_local_program::StaticSinkProgram;
    let path = FieldPath::root("fragment_package").field("sink");
    let expected = match sink {
        StaticSinkProgram::DataStream { branch, .. } => {
            vec![(branch.dest_node_id(), branch.partition_type())]
        }
        StaticSinkProgram::Result | StaticSinkProgram::RootResult(_) | StaticSinkProgram::Noop => {
            Vec::new()
        }
        StaticSinkProgram::MultiCastDataStream { .. }
        | StaticSinkProgram::SplitDataStream { .. } => {
            return Err(error(
                path,
                ProtocolErrorKind::InvalidValue,
                "compiled multicast and router sinks are not executable yet",
            ));
        }
        StaticSinkProgram::ChangeStreamRouter { branches, .. } => branches
            .iter()
            .map(|branch| (branch.dest_node_id(), branch.partition_type()))
            .collect(),
    };
    let edges = decode_sink_edges(&expected, sink_edge_ids, topology, path)?;
    let mut groups = edges
        .into_iter()
        .map(|edge| decode_edge_destinations(edge, source))
        .collect::<Result<Vec<_>, _>>()?;
    match sink {
        StaticSinkProgram::DataStream { .. } => Ok(FragmentSinkAssignment::StreamDestinations {
            destinations: groups.pop().expect("one stream edge was validated"),
            sender_id: None,
        }),
        StaticSinkProgram::ChangeStreamRouter { .. } => {
            Ok(FragmentSinkAssignment::DestinationGroups {
                groups,
                sender_id: None,
            })
        }
        _ => Ok(FragmentSinkAssignment::None),
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
    use super::*;
    use novarocks_execution_contract::FragmentNodeId;
    use novarocks_execution_contract::task_execution::descriptor::{
        ExchangeDestination, RuntimeEndpoint,
    };
    use novarocks_execution_contract::task_execution::domain::ExchangeEdgeId;
    use novarocks_execution_contract::task_execution::identity::TaskIdentity;
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use std::num::NonZeroU32;
    const SOURCE: UniqueId = UniqueId::new(5, 6);
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
        let assignment = compiled_fragment_sink_assignment(
            &compiled_stream(9, DataStreamPartitionType::Unpartitioned),
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
    fn router_binds_provider_and_summary_to_their_exact_frozen_edges() {
        use arrow::datatypes::{DataType, Field, Schema};
        use novarocks_execution_contract::ExchangeDeliveryRequirement;
        use novarocks_local_program::{
            ImmutableExpressions, ProgramOutputRole, ProgramWriteLayoutPreference, StaticLayout,
            StaticRouterRoute, StaticSinkProgram, StaticStreamBranch, StaticStreamContract,
            StaticStreamRouting,
        };
        use std::sync::Arc;
        let slots = [10, 11, 12].map(novarocks_types::SlotId::new);
        let layout = StaticLayout::try_new(
            Arc::new(Schema::new(
                ["inserted", "replaced", "deleted"]
                    .map(|name| Field::new(name, DataType::Int64, false))
                    .to_vec(),
            )),
            Arc::from(slots),
        )
        .unwrap();
        let branch = |node, columns, role| {
            StaticStreamBranch::try_new(
                node,
                DataStreamPartitionType::Unpartitioned,
                vec![],
                columns,
                None,
                StaticStreamContract {
                    role,
                    delivery: ExchangeDeliveryRequirement::MustDrain,
                    routing: StaticStreamRouting::Ordinary,
                },
            )
            .unwrap()
        };
        let sink = StaticSinkProgram::try_change_stream_router(
            vec![
                branch(
                    9,
                    vec![novarocks_types::SlotId::new(2)],
                    ProgramOutputRole::ProviderRoute {
                        target: novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(
                            0,
                        )
                        .unwrap(),
                    },
                ),
                branch(10, slots.to_vec(), ProgramOutputRole::LogicalSummary),
            ],
            Arc::new(
                ImmutableExpressions::try_new(
                    vec![],
                    false,
                    std::collections::HashMap::new(),
                    None,
                )
                .unwrap(),
            ),
            novarocks_types::SlotId::new(1),
            vec![StaticRouterRoute {
                branch: 0,
                layout: ProgramWriteLayoutPreference::None,
                accepted_effects: Arc::from([
                    novarocks_spi::connector::ConnectorRowMutationEffect::Insert,
                ]),
            }],
            1,
            layout,
        )
        .unwrap();
        let execution = execution();
        let edge = |id, node| {
            ExchangeEdge::try_new(
                ExchangeEdgeId::new(id).unwrap(),
                FragmentNodeId::new(node),
                DataStreamPartitionType::Unpartitioned,
                vec![ExchangeDestination::new(
                    task(execution, id),
                    UniqueId::new(i64::from(id), 1),
                    RuntimeEndpoint::new("be.local", 8060).unwrap(),
                    FragmentNodeId::new(node),
                )],
                0,
                NonZeroU32::MIN,
            )
            .unwrap()
        };
        let topology = ExchangeTopology::try_new(vec![edge(2, 10), edge(7, 9)], vec![]).unwrap();
        let assignment =
            compiled_fragment_sink_assignment(&sink, &[7, 2], SOURCE, &topology).unwrap();
        let FragmentSinkAssignment::DestinationGroups { groups, .. } = assignment else {
            panic!("router requires independent output groups")
        };
        assert_eq!(groups.len(), 2);
        assert_eq!(*groups[0][0].finst_id(), UniqueId::new(7, 1));
        assert_eq!(*groups[1][0].finst_id(), UniqueId::new(2, 1));
        for edges in [vec![7], vec![2, 7], vec![7, 7]] {
            assert!(compiled_fragment_sink_assignment(&sink, &edges, SOURCE, &topology).is_err());
        }
    }

    fn compiled_stream(
        dest_node_id: i32,
        partitioning: DataStreamPartitionType,
    ) -> novarocks_local_program::StaticSinkProgram {
        novarocks_local_program::StaticSinkProgram::DataStream {
            branch: novarocks_local_program::StaticStreamBranch::try_new(
                dest_node_id,
                partitioning,
                Vec::new(),
                vec![novarocks_types::SlotId::new(0)],
                None,
                novarocks_local_program::StaticStreamContract {
                    role: novarocks_local_program::ProgramOutputRole::Relational,
                    delivery:
                        novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                    routing: novarocks_local_program::StaticStreamRouting::Ordinary,
                },
            )
            .expect("compiled stream branch"),
            arena: std::sync::Arc::new(
                novarocks_local_program::ImmutableExpressions::try_new(
                    Vec::new(),
                    false,
                    std::collections::HashMap::new(),
                    None,
                )
                .expect("empty sink arena"),
            ),
        }
    }

    fn one_edge_topology(partitioning: DataStreamPartitionType) -> ExchangeTopology {
        let execution = execution();
        let edge = ExchangeEdge::try_new(
            ExchangeEdgeId::new(3).expect("nonzero edge"),
            FragmentNodeId::new(9),
            partitioning,
            (1..=2)
                .map(|task_id| {
                    ExchangeDestination::new(
                        task(execution, task_id),
                        UniqueId::new(i64::from(task_id), 1),
                        RuntimeEndpoint::new("be.local", 8060).expect("endpoint"),
                        FragmentNodeId::new(9),
                    )
                })
                .collect(),
            0,
            NonZeroU32::new(1).expect("sender count"),
        )
        .expect("edge");
        ExchangeTopology::try_new(vec![edge], vec![]).expect("topology")
    }

    // The compiled static sink binds to the frozen outbound edge by the same
    // law as a legacy plan sink: destination node and partitioning must agree.
    #[test]
    fn compiled_stream_sink_binds_its_one_edge_by_node_and_partitioning() {
        let topology = one_edge_topology(DataStreamPartitionType::Unpartitioned);
        let assignment = compiled_fragment_sink_assignment(
            &compiled_stream(9, DataStreamPartitionType::Unpartitioned),
            &[3],
            SOURCE,
            &topology,
        )
        .expect("the compiled branch binds the one edge");
        let FragmentSinkAssignment::StreamDestinations { destinations, .. } = assignment else {
            panic!("a stream sink binds one destination list");
        };
        assert_eq!(destinations.len(), 2);
        assert!(
            destinations
                .iter()
                .all(|destination| destination.source_finst_id() == SOURCE)
        );

        let wrong_node = compiled_fragment_sink_assignment(
            &compiled_stream(8, DataStreamPartitionType::Unpartitioned),
            &[3],
            SOURCE,
            &topology,
        )
        .expect_err("destination node mismatch");
        assert!(
            wrong_node
                .to_string()
                .contains("destination node disagrees"),
            "{wrong_node}"
        );
        let wrong_partitioning = compiled_fragment_sink_assignment(
            &compiled_stream(9, DataStreamPartitionType::HashPartitioned),
            &[3],
            SOURCE,
            &topology,
        )
        .expect_err("partitioning mismatch");
        assert!(
            wrong_partitioning
                .to_string()
                .contains("partitioning disagrees"),
            "{wrong_partitioning}"
        );
    }

    #[test]
    fn compiled_result_sink_binds_no_edge_and_refuses_a_stray_one() {
        let assignment = compiled_fragment_sink_assignment(
            &novarocks_local_program::StaticSinkProgram::Result,
            &[],
            SOURCE,
            &ExchangeTopology::default(),
        )
        .expect("a result sink has no outbound edge");
        assert!(matches!(assignment, FragmentSinkAssignment::None));
        let stray = compiled_fragment_sink_assignment(
            &novarocks_local_program::StaticSinkProgram::Result,
            &[3],
            SOURCE,
            &one_edge_topology(DataStreamPartitionType::Unpartitioned),
        )
        .expect_err("a result sink cannot own an outbound edge");
        assert!(stray.to_string().contains("exactly once"), "{stray}");
    }

    #[test]
    fn sink_branch_allocation_uses_explicit_edge_ids_even_when_nodes_match() {
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
        let expected = [
            (9, DataStreamPartitionType::Unpartitioned),
            (9, DataStreamPartitionType::Unpartitioned),
        ];
        let edges = decode_sink_edges(
            &expected,
            &[2, 1],
            &topology,
            FieldPath::root("fragment_package").field("sink"),
        )
        .expect("explicit ids disambiguate equal branch nodes");
        let groups = edges
            .into_iter()
            .map(|edge| decode_edge_destinations(edge, SOURCE).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(*groups[0][0].finst_id(), UniqueId::new(22, 1));
        assert_eq!(*groups[1][0].finst_id(), UniqueId::new(11, 1));
        assert_eq!(groups[0][0].source_finst_id(), SOURCE);
        let error = decode_sink_edges(
            &expected,
            &[1, 1],
            &topology,
            FieldPath::root("fragment_package").field("sink"),
        )
        .expect_err("one edge cannot serve two branches");
        assert!(
            error
                .to_string()
                .contains("sink edge id must be nonzero and used only once")
        );
        let wrong = [
            (9, DataStreamPartitionType::HashPartitioned),
            (9, DataStreamPartitionType::Unpartitioned),
        ];
        let error = decode_sink_edges(
            &wrong,
            &[2, 1],
            &topology,
            FieldPath::root("fragment_package").field("sink"),
        )
        .expect_err("partitioning must agree with the exact edge");
        assert!(
            error
                .to_string()
                .contains("partitioning disagrees with its assigned edge")
        );
    }
    #[test]
    fn compiled_stream_requires_exactly_one_assigned_edge() {
        for ids in [vec![], vec![3, 3], vec![0], vec![7]] {
            assert!(
                compiled_fragment_sink_assignment(
                    &compiled_stream(9, DataStreamPartitionType::Unpartitioned),
                    &ids,
                    SOURCE,
                    &one_edge_topology(DataStreamPartitionType::Unpartitioned)
                )
                .is_err()
            );
        }
    }
}
