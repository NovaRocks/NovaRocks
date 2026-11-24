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

use crate::task_instance::task_sink_edge_ids_path;
use novarocks_execution::runtime::endpoint::TaskDestination;
use novarocks_execution::runtime::task::TaskSinkAssignment;
use novarocks_execution_contract::ExchangeEdgeId;
use novarocks_execution_contract::task_execution::descriptor::{
    DataStreamPartitionType, ExchangeEdge, ExchangeTopology,
};
use novarocks_physical_plan::EdgeId;
use novarocks_proto_codec::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_types::UniqueId;
use std::collections::BTreeSet;

/// Binds a compiled sink without package-owned multicast branches.
///
/// Native multicast requires the original package's ordered physical edges.
pub fn compiled_task_sink_assignment(
    sink: &novarocks_local_program::StaticSinkProgram,
    summaries: &[novarocks_local_program::StaticSummaryBranch],
    sink_edge_ids: &[u32],
    source: UniqueId,
    topology: &ExchangeTopology,
) -> Result<TaskSinkAssignment, ProtocolError> {
    compiled_task_sink_assignment_with_multicast_edges(
        sink,
        summaries,
        &[],
        sink_edge_ids,
        source,
        topology,
    )
}

pub(crate) fn compiled_task_sink_assignment_with_multicast_edges(
    sink: &novarocks_local_program::StaticSinkProgram,
    summaries: &[novarocks_local_program::StaticSummaryBranch],
    multicast_edges: &[EdgeId],
    sink_edge_ids: &[u32],
    source: UniqueId,
    topology: &ExchangeTopology,
) -> Result<TaskSinkAssignment, ProtocolError> {
    use novarocks_local_program::StaticSinkProgram;
    let path = FieldPath::root("fragment_package").field("sink");
    if matches!(sink, StaticSinkProgram::SplitDataStream { .. }) {
        return Err(error(
            path,
            ProtocolErrorKind::InvalidValue,
            "compiled split sinks are not executable yet",
        ));
    }
    if let StaticSinkProgram::MultiCastDataStream { branches } = sink {
        if multicast_edges.is_empty() || multicast_edges.len() != branches.len() {
            return Err(error(
                path,
                ProtocolErrorKind::InconsistentFields,
                "multicast sink branches differ from the frozen physical edges",
            ));
        }
        for (index, edge) in multicast_edges.iter().enumerate() {
            let expected = ExchangeEdgeId::from_physical_ordinal(edge.get()).ok_or_else(|| {
                error(
                    path.clone(),
                    ProtocolErrorKind::InvalidValue,
                    "multicast physical edge cannot be represented in the task topology",
                )
            })?;
            if sink_edge_ids.get(index).copied() != Some(expected.get()) {
                return Err(error(
                    task_sink_edge_ids_path().index(index),
                    ProtocolErrorKind::InconsistentFields,
                    "assigned edge differs from the frozen multicast branch",
                ));
            }
        }
    } else if !multicast_edges.is_empty() {
        return Err(error(
            path,
            ProtocolErrorKind::InconsistentFields,
            "frozen physical multicast edges require a compiled multicast sink",
        ));
    }
    let expected = sink
        .branches()
        .iter()
        .chain(summaries.iter().map(|output| output.branch()))
        .map(|branch| (branch.dest_node_id(), branch.partition_type()))
        .collect::<Vec<_>>();
    let edges = decode_sink_edges(&expected, sink_edge_ids, topology, path)?;
    let mut groups = edges
        .into_iter()
        .map(|edge| decode_edge_destinations(edge, source))
        .collect::<Result<Vec<_>, _>>()?;
    match sink {
        StaticSinkProgram::DataStream { .. } if summaries.is_empty() => {
            Ok(TaskSinkAssignment::StreamDestinations {
                destinations: groups.pop().expect("one stream edge was validated"),
                sender_id: None,
            })
        }
        _ if !groups.is_empty() => Ok(TaskSinkAssignment::DestinationGroups {
            groups,
            sender_id: None,
        }),
        _ => Ok(TaskSinkAssignment::None),
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
) -> Result<Vec<TaskDestination>, ProtocolError> {
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
            TaskDestination::new(
                destination.task_key(),
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
        let assignment = compiled_task_sink_assignment(
            &compiled_stream(9, DataStreamPartitionType::Unpartitioned),
            &[],
            &[3],
            SOURCE,
            &topology,
        )
        .expect("the one stream branch is bound to the one edge");
        let TaskSinkAssignment::StreamDestinations { destinations, .. } = assignment else {
            panic!("a stream sink binds one destination list");
        };
        assert_eq!(destinations.len(), 3);
        for destination in &destinations {
            assert_eq!(destination.source_task_key(), SOURCE);
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
            ProgramOutputRole, ProgramWriteLayoutPreference, StaticLayout, StaticRouterRoute,
            StaticSinkProgram, StaticStreamBranch, StaticStreamContract, StaticStreamRouting,
        };
        use std::sync::Arc;
        let slots = [10, 11, 12].map(novarocks_types::SlotId::new);
        struct Control;
        impl novarocks_type_contract::PureCompileControl for Control {
            fn checkpoint(
                &self,
                _: novarocks_type_contract::CompilePhase,
                _: u32,
            ) -> Result<(), novarocks_type_contract::CompileControlError> {
                Ok(())
            }
        }
        let layout = StaticLayout::try_new_typed_for_compile(
            Arc::new(Schema::new(
                ["inserted", "replaced", "deleted"]
                    .map(|name| Field::new(name, DataType::Int64, false))
                    .to_vec(),
            )),
            Arc::from(slots),
            (0..3)
                .map(|_| {
                    (
                        novarocks_type_contract::FunctionValueType::new(DataType::Int64, false)
                            .typed_binding()
                            .clone(),
                        None,
                    )
                })
                .collect(),
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
            &Control,
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
            vec![branch(
                9,
                vec![novarocks_types::SlotId::new(2)],
                ProgramOutputRole::ProviderRoute {
                    target: novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(0)
                        .unwrap(),
                },
            )],
            &[],
            novarocks_types::SlotId::new(1),
            vec![StaticRouterRoute {
                branch: 0,
                layout: ProgramWriteLayoutPreference::None,
                accepted_effects: Arc::from([
                    novarocks_spi::connector::ConnectorRowMutationEffect::Insert,
                ]),
            }],
        )
        .unwrap();
        let outputs = [novarocks_local_program::StaticSummaryBranch::try_new(
            novarocks_result_contract::SummaryKind::WriteCounts,
            novarocks_local_program::StaticSummaryAuthor::RouterSink,
            branch(
                10,
                slots.to_vec(),
                ProgramOutputRole::Summary(novarocks_result_contract::SummaryKind::WriteCounts),
            ),
            layout,
        )
        .unwrap()];
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
            compiled_task_sink_assignment(&sink, &outputs, &[7, 2], SOURCE, &topology).unwrap();
        let TaskSinkAssignment::DestinationGroups { groups, .. } = assignment else {
            panic!("router requires independent output groups")
        };
        assert_eq!(groups.len(), 2);
        assert_eq!(*groups[0][0].task_key(), UniqueId::new(7, 1));
        assert_eq!(*groups[1][0].task_key(), UniqueId::new(2, 1));
        for edges in [vec![7], vec![2, 7], vec![7, 7]] {
            assert!(
                compiled_task_sink_assignment(&sink, &outputs, &edges, SOURCE, &topology).is_err()
            );
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
        let assignment = compiled_task_sink_assignment(
            &compiled_stream(9, DataStreamPartitionType::Unpartitioned),
            &[],
            &[3],
            SOURCE,
            &topology,
        )
        .expect("the compiled branch binds the one edge");
        let TaskSinkAssignment::StreamDestinations { destinations, .. } = assignment else {
            panic!("a stream sink binds one destination list");
        };
        assert_eq!(destinations.len(), 2);
        assert!(
            destinations
                .iter()
                .all(|destination| destination.source_task_key() == SOURCE)
        );

        let wrong_node = compiled_task_sink_assignment(
            &compiled_stream(8, DataStreamPartitionType::Unpartitioned),
            &[],
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
        let wrong_partitioning = compiled_task_sink_assignment(
            &compiled_stream(9, DataStreamPartitionType::HashPartitioned),
            &[],
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
        let assignment = compiled_task_sink_assignment(
            &novarocks_local_program::StaticSinkProgram::Result,
            &[],
            &[],
            SOURCE,
            &ExchangeTopology::default(),
        )
        .expect("a result sink has no outbound edge");
        assert!(matches!(assignment, TaskSinkAssignment::None));
        let stray = compiled_task_sink_assignment(
            &novarocks_local_program::StaticSinkProgram::Result,
            &[],
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
        assert_eq!(*groups[0][0].task_key(), UniqueId::new(22, 1));
        assert_eq!(*groups[1][0].task_key(), UniqueId::new(11, 1));
        assert_eq!(groups[0][0].source_task_key(), SOURCE);
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
    fn multicast_projection_is_bound_to_its_own_destination_group() {
        use novarocks_local_program::{
            ProgramOutputRole, StaticSinkProgram, StaticStreamBranch, StaticStreamContract,
            StaticStreamRouting,
        };
        use novarocks_types::SlotId;

        let execution = execution();
        let node = FragmentNodeId::new(9);
        let partitioning = DataStreamPartitionType::Unpartitioned;
        let destination = |stage, key| {
            ExchangeDestination::new(
                TaskIdentity::new(
                    execution,
                    StageId::new(stage).unwrap(),
                    TaskId::new(1).unwrap(),
                    BackendProcessId::new_v7(),
                ),
                UniqueId::new(key, 1),
                RuntimeEndpoint::new("be.local", 8060).unwrap(),
                node,
            )
        };
        let left = destination(2, 51);
        let right = destination(3, 79);
        let edge = |id, destination, sender| {
            ExchangeEdge::try_new(
                ExchangeEdgeId::new(id).unwrap(),
                node,
                partitioning,
                vec![destination],
                sender,
                NonZeroU32::new(3).unwrap(),
            )
            .unwrap()
        };
        let topology = ExchangeTopology::try_new(
            vec![edge(3, right.clone(), 2), edge(10, left.clone(), 1)],
            vec![],
        )
        .unwrap();
        let branch = |slot| {
            StaticStreamBranch::try_new(
                node.get(),
                partitioning,
                vec![],
                vec![SlotId::new(slot)],
                None,
                StaticStreamContract {
                    role: ProgramOutputRole::Relational,
                    delivery:
                        novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
                    routing: StaticStreamRouting::Ordinary,
                },
            )
            .unwrap()
        };
        let sink = StaticSinkProgram::try_multicast(vec![branch(1), branch(0)], &[]).unwrap();
        let edges = [9, 2].map(EdgeId::new);
        let assignment = compiled_task_sink_assignment_with_multicast_edges(
            &sink,
            &[],
            &edges,
            &[10, 3],
            SOURCE,
            &topology,
        )
        .unwrap();
        let TaskSinkAssignment::DestinationGroups { groups, .. } = assignment else {
            panic!("one distinct destination group per multicast projection");
        };
        assert_eq!(groups.len(), 2);
        for ((branch, group), (slot, destination, sender)) in sink
            .branches()
            .iter()
            .zip(&groups)
            .zip([(1, left, 1), (0, right, 2)])
        {
            assert_eq!(branch.output_columns(), &[SlotId::new(slot)]);
            let [actual] = group.as_slice() else {
                panic!("expected exactly one frozen destination");
            };
            assert_eq!(actual.task_identity(), Some(destination.task()));
            assert_eq!(*actual.task_key(), destination.task_key());
            assert_eq!(actual.source_task_key(), SOURCE);
            assert_eq!(
                (actual.sender_ordinal(), actual.sender_count()),
                (sender, 3)
            );
        }
        let extra = compiled_task_sink_assignment_with_multicast_edges(
            &sink,
            &[],
            &edges,
            &[10, 3, 7],
            SOURCE,
            &topology,
        )
        .expect_err("the existing binding still owns coverage after the multicast prefix");
        assert!(extra.detail().contains("exactly once"));
        let unproved = compiled_task_sink_assignment(&sink, &[], &[10, 3], SOURCE, &topology)
            .expect_err("multicast requires its package-owned physical branch identities");
        assert!(unproved.detail().contains("frozen physical edges"));
    }

    #[test]
    fn split_remains_unavailable_through_native_sink_binding() {
        let stream = compiled_stream(9, DataStreamPartitionType::Unpartitioned);
        let split = novarocks_local_program::StaticSinkProgram::SplitDataStream {
            branches: stream.branches().to_vec().into(),
            split_exprs: [].into(),
            fanout: false,
        };
        let err = compiled_task_sink_assignment(
            &split,
            &[],
            &[3],
            SOURCE,
            &one_edge_topology(DataStreamPartitionType::Unpartitioned),
        )
        .expect_err("split keeps its named capability refusal");
        assert_eq!(err.kind(), &ProtocolErrorKind::InvalidValue);
        assert_eq!(err.detail(), "compiled split sinks are not executable yet");
    }

    #[test]
    fn compiled_stream_requires_exactly_one_assigned_edge() {
        for ids in [vec![], vec![3, 3], vec![0], vec![7]] {
            assert!(
                compiled_task_sink_assignment(
                    &compiled_stream(9, DataStreamPartitionType::Unpartitioned),
                    &[],
                    &ids,
                    SOURCE,
                    &one_edge_topology(DataStreamPartitionType::Unpartitioned)
                )
                .is_err()
            );
        }
    }
}
