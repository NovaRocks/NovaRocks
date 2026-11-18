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

//! Original frozen graph and manifest contracts, preserved through logical cutover.

use super::creation::tests as creation_tests;
use super::error::TaskExecutionError;
use super::graph::{
    TaskGraph, TaskNode, TaskPlanFacts, TaskPlanSource, build_task_graph_from_manifest,
};
use crate::native::fragment_encoder::frozen::tests as frozen_tests;
use crate::native::fragment_encoder::frozen::{FragmentArtifact, StaticSinkTarget};
use crate::query_execution::artifact::{
    FragmentId, NativePlacementAssignment, TaskManifestBinding, task_key_for_contract_test,
};
use crate::query_execution::attempt_plan_facts::AttemptPartitionKind;
use crate::query_execution::schedule::{SchedulingPlan, TaskPlacement};
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::task_execution::{OperationKind, TaskIdentity, TaskOperationId};
use novarocks_physical_plan::PipelineDopDomain;
use novarocks_proto_models::novarocks as wire;
use novarocks_task_codec::TransportBudget;
use novarocks_task_codec::descriptor::encode_task_descriptor;
use novarocks_types::{
    AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId, TaskId,
};
use prost::Message;
use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

/// The SAME frozen graph used by the old unstarted Harness::new(&[0], &[0], 64).
/// This creates no Task execution, round, status source or protocol work.
pub(crate) fn unstarted_chain_graph() -> TaskGraph {
    let schedule = chain_schedule(&[0], &[0]);
    build_graph(&schedule, &chain_edges(), &backends(1), 64)
        .expect("the chain schedule is a legal task graph")
}

pub(crate) fn delivery_chain_graph(
    delivery: novarocks_execution_contract::ExchangeDeliveryRequirement,
) -> TaskGraph {
    let schedule = chain_schedule(&[0], &[1]);
    let mut edges = chain_edges();
    for edge in &mut edges {
        edge.delivery = delivery;
    }
    build_graph(&schedule, &edges, &backends(2), 512)
        .expect("the delivery contract forms the original chain")
}

// A checked Result or single-Stream package. Multi-edge control tests keep
// neutral topology separate and use a real Result package; they do not claim
// admission of a multicast or router executable package.
// FakePlans preserves one frozen fragment per fragment and each instance
// retains its original assignment and ownership geometry.
// The temporary fixture manifest must drop before callers count original
// per-task Arc aliases.

const LEAF_FRAGMENT: FragmentId = 1;

const MIDDLE_FRAGMENT: FragmentId = 2;

const ROOT_FRAGMENT: FragmentId = 3;

const LEAF_TO_MIDDLE_NODE: i32 = 20;

const MIDDLE_TO_ROOT_NODE: i32 = 30;

const LEAF_TO_ROOT_NODE: i32 = 31;

const SCAN_NODE: i32 = 5;

fn frozen_fragment(
    fragment_id: FragmentId,
    targets: &[StaticSinkTarget],
    padding: usize,
) -> Arc<FragmentArtifact> {
    let target = match targets {
        [target] => Some((
            target.target_fragment_id(),
            target.target_exchange_node_id(),
            target.delivery(),
        )),
        _ => None,
    };
    frozen_tests::frozen_package(
        fragment_id,
        i32::try_from(fragment_id).expect("small fragment") * 100,
        [9; 16],
        PipelineDopDomain {
            min: 1,
            max: 4,
            requires_power_of_two: false,
        },
        target,
        padding,
    )
}

fn edge_sink(
    fragment_id: FragmentId,
    edges: &[crate::query_execution::attempt_plan_facts::AttemptEdgeFacts],
) -> Vec<StaticSinkTarget> {
    edges
        .iter()
        .filter(|edge| edge.source_fragment_id == fragment_id)
        .map(|edge| {
            StaticSinkTarget::for_test(
                edge.target_fragment_id,
                edge.target_exchange_node_id,
                edge.delivery,
            )
        })
        .collect()
}

fn multicast_sink(targets: [(FragmentId, i32); 2]) -> Vec<StaticSinkTarget> {
    targets
        .into_iter()
        .map(|(fragment, node)| {
            StaticSinkTarget::for_test(
                fragment,
                node,
                novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
            )
        })
        .collect()
}

fn router_sink(targets: [(FragmentId, i32); 2]) -> Vec<StaticSinkTarget> {
    multicast_sink(targets)
}

struct FakePlans<'a> {
    schedule: &'a SchedulingPlan,
    edges: &'a [crate::query_execution::attempt_plan_facts::AttemptEdgeFacts],
    padding: usize,
    leaf_sink: Option<Vec<StaticSinkTarget>>,
    frozen: BTreeMap<FragmentId, Arc<FragmentArtifact>>,
}

impl<'a> FakePlans<'a> {
    fn new(
        schedule: &'a SchedulingPlan,
        edges: &'a [crate::query_execution::attempt_plan_facts::AttemptEdgeFacts],
        padding: usize,
    ) -> Self {
        Self {
            schedule,
            edges,
            padding,
            leaf_sink: None,
            frozen: BTreeMap::new(),
        }
    }

    /// The same source, with the leaf fragment declaring `sink` instead of
    /// the one its edges imply.
    fn with_leaf_sink(mut self, sink: Vec<StaticSinkTarget>) -> Self {
        self.leaf_sink = Some(sink);
        self
    }
}

impl TaskPlanSource for FakePlans<'_> {
    fn control_topology_targets(&self, fragment_id: FragmentId) -> Option<Vec<StaticSinkTarget>> {
        Some(match (&self.leaf_sink, fragment_id) {
            (Some(targets), LEAF_FRAGMENT) => targets.clone(),
            _ => edge_sink(fragment_id, self.edges),
        })
    }

    fn take_plan(
        &mut self,
        fragment_id: FragmentId,
        instance_index: usize,
    ) -> Result<TaskPlanFacts, TaskExecutionError> {
        let placement = self
            .schedule
            .by_fragment
            .get(&fragment_id)
            .and_then(|placements| placements.get(instance_index))
            .ok_or_else(|| {
                TaskExecutionError::Schedule(format!(
                    "fragment {fragment_id} has no instance {instance_index}"
                ))
            })?;
        let sink = match (&self.leaf_sink, fragment_id) {
            (Some(sink), LEAF_FRAGMENT) => sink.clone(),
            _ => edge_sink(fragment_id, self.edges),
        };
        let padding = self.padding;
        let fragment = Arc::clone(
            self.frozen
                .entry(fragment_id)
                .or_insert_with(|| frozen_fragment(fragment_id, &sink, padding)),
        );
        Ok(TaskPlanFacts {
            fragment,
            assignment: NativePlacementAssignment::new(
                u32::try_from(instance_index).expect("a test instance ordinal fits"),
                NonZeroUsize::new(2).expect("two is nonzero"),
                placement.scan_ranges.clone(),
            ),
        })
    }
}

fn execution_id() -> QueryExecutionId {
    QueryExecutionId::new(
        QueryId::new(0x1234, 0x5678),
        AttemptId::new(1).expect("attempt one is nonzero"),
    )
    .expect("a nonzero query id")
}

fn endpoint(index: usize) -> RuntimeEndpoint {
    RuntimeEndpoint::new("127.0.0.1", 9000 + index as i32).expect("a valid endpoint")
}

fn backends(count: usize) -> BTreeMap<usize, BackendProcessId> {
    (0..count)
        .map(|index| (index, BackendProcessId::new_v7()))
        .collect()
}

fn placements(
    fragment_id: FragmentId,
    backend_indexes: &[usize],
    scan_nodes: &[i32],
) -> Vec<TaskPlacement> {
    backend_indexes
        .iter()
        .enumerate()
        .map(|(instance_index, &backend_idx)| TaskPlacement {
            fragment_id,
            instance_index,
            task_key: task_key_for_contract_test(
                execution_id().query_id(),
                fragment_id,
                instance_index,
            ),
            backend_idx,
            endpoint: endpoint(backend_idx),
            scan_ranges: scan_nodes
                .iter()
                .map(|&node_id| (node_id, Vec::new()))
                .collect(),
        })
        .collect()
}

fn stream_edge(
    source: FragmentId,
    target: FragmentId,
    node_id: i32,
) -> crate::query_execution::attempt_plan_facts::AttemptEdgeFacts {
    crate::query_execution::attempt_plan_facts::AttemptEdgeFacts {
        source_fragment_id: source,
        target_fragment_id: target,
        target_exchange_node_id: node_id,
        partition_kind: AttemptPartitionKind::Hash,
        delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
    }
}

fn chain_schedule(leaf_backends: &[usize], middle_backends: &[usize]) -> SchedulingPlan {
    let mut by_fragment = BTreeMap::new();
    by_fragment.insert(
        LEAF_FRAGMENT,
        placements(LEAF_FRAGMENT, leaf_backends, &[SCAN_NODE]),
    );
    by_fragment.insert(
        MIDDLE_FRAGMENT,
        placements(MIDDLE_FRAGMENT, middle_backends, &[]),
    );
    by_fragment.insert(ROOT_FRAGMENT, placements(ROOT_FRAGMENT, &[0], &[]));
    let root = by_fragment[&ROOT_FRAGMENT][0].clone();
    SchedulingPlan {
        root_fragment_id: ROOT_FRAGMENT,
        by_fragment,
        root_task_key: root.task_key,
        root_backend_idx: root.backend_idx,
    }
}

fn chain_edges() -> Vec<crate::query_execution::attempt_plan_facts::AttemptEdgeFacts> {
    vec![
        stream_edge(LEAF_FRAGMENT, MIDDLE_FRAGMENT, LEAF_TO_MIDDLE_NODE),
        stream_edge(MIDDLE_FRAGMENT, ROOT_FRAGMENT, MIDDLE_TO_ROOT_NODE),
    ]
}

fn multicast_edges() -> Vec<crate::query_execution::attempt_plan_facts::AttemptEdgeFacts> {
    vec![
        stream_edge(LEAF_FRAGMENT, MIDDLE_FRAGMENT, LEAF_TO_MIDDLE_NODE),
        stream_edge(MIDDLE_FRAGMENT, ROOT_FRAGMENT, MIDDLE_TO_ROOT_NODE),
        stream_edge(LEAF_FRAGMENT, ROOT_FRAGMENT, LEAF_TO_ROOT_NODE),
    ]
}

fn descriptor_of(
    graph: &TaskGraph,
    task_id: TaskId,
) -> novarocks_execution::task_execution::TaskDescriptor {
    graph
        .seed(task_id)
        .expect("a built graph owns every creation seed")
        .descriptor()
        .expect("a built graph derives every descriptor")
}

fn frozen_metadata(graph: TaskGraph) -> BTreeMap<TaskId, wire::CreationMetadata> {
    let (_graph, seeds) = graph.into_seeds();
    seeds
        .into_iter()
        .map(|(task_id, seed)| {
            let priced = seed.lengths().expect("a seed prices its creation");
            let parts = seed.freeze(priced).expect("a seed freezes at its price");
            let metadata = wire::CreationMetadata::decode(parts.metadata().bytes().clone())
                .expect("frozen creation metadata decodes");
            (task_id, metadata)
        })
        .collect()
}

fn sink_edge_ids(metadata: &wire::CreationMetadata) -> Vec<u32> {
    metadata
        .assignment
        .as_ref()
        .expect("every creation carries its assignment")
        .sink_edge_ids
        .clone()
}

#[test]
fn every_instance_of_one_fragment_shares_its_frozen_plan_and_keeps_its_own_assignment() {
    let processes = backends(3);
    let schedule = chain_schedule(&[0, 1, 2], &[0, 1]);
    let freezes = frozen_tests::freezes_on_this_thread();
    let metadata_freezes = creation_tests::metadata_freezes_on_this_thread();
    let graph = build_graph(&schedule, &chain_edges(), &processes, 64).expect("a legal graph");
    assert_eq!(
        frozen_tests::freezes_on_this_thread() - freezes,
        3,
        "each of the three fragments is frozen once, however many instances it has"
    );
    assert_eq!(
        creation_tests::metadata_freezes_on_this_thread(),
        metadata_freezes,
        "building the graph freezes no creation metadata"
    );

    let tasks = graph
        .tasks()
        .map(|task| (task.task_id(), task.fragment_id(), task.instance_index()))
        .collect::<Vec<_>>();
    assert_eq!(tasks.len(), 6, "three leaves, two middles, one root");
    for &(left, left_fragment, _) in &tasks {
        for &(right, right_fragment, _) in &tasks {
            let shared = Arc::ptr_eq(
                graph.seed(left).expect("a seed").fragment(),
                graph.seed(right).expect("a seed").fragment(),
            );
            assert_eq!(
                shared,
                left_fragment == right_fragment,
                "instances share exactly their own fragment's frozen plan"
            );
        }
    }

    let metadata = frozen_metadata(graph);
    for (task_id, _, instance_index) in tasks {
        let assignment = metadata[&task_id]
            .assignment
            .as_ref()
            .expect("every creation carries its assignment");
        assert_eq!(
            assignment.instance_ordinal,
            u32::try_from(instance_index).expect("a small ordinal"),
            "each instance keeps its own ordinal"
        );
    }
}

#[test]
fn a_fragment_is_frozen_once_however_many_instances_it_has() {
    for leaves in [1_usize, 3, 16, 64] {
        let processes = backends(3);
        let leaf_backends = (0..leaves).map(|index| index % 3).collect::<Vec<_>>();
        let schedule = chain_schedule(&leaf_backends, &[0, 1]);
        let freezes = frozen_tests::freezes_on_this_thread();
        let graph = build_graph(&schedule, &chain_edges(), &processes, 64).expect("a legal graph");
        assert_eq!(
            frozen_tests::freezes_on_this_thread() - freezes,
            3,
            "{leaves} leaf instances still freeze exactly the three fragments"
        );
        let leaf_tasks = graph
            .tasks()
            .filter(|task| task.fragment_id() == LEAF_FRAGMENT)
            .map(TaskNode::task_id)
            .collect::<Vec<_>>();
        assert_eq!(leaf_tasks.len(), leaves);
        let shared = graph.seed(leaf_tasks[0]).expect("a leaf seed").fragment();
        for task in &leaf_tasks {
            let fragment = graph.seed(*task).expect("a leaf seed").fragment();
            assert!(Arc::ptr_eq(fragment, shared));
            assert!(fragment.content().shares_backing_with(shared.content()));
        }
        assert_eq!(
            Arc::strong_count(shared),
            leaves,
            "the static plan is held once per leaf task and by nothing else"
        );
    }
}

#[test]
fn manifest_projection_keeps_creation_seeds_with_their_exact_quotes() {
    use novarocks_execution_contract::operation::OperationEnvelope;

    let schedule = chain_schedule(&[0, 1], &[1]);
    let graph = build_graph(&schedule, &chain_edges(), &backends(2), 64).expect("manifest graph");
    let root = graph.root_identity();
    let edges = graph.edges().cloned().collect::<Vec<_>>();
    let mut next = NonZeroU64::new(10).unwrap();
    let (frozen, mut originals) = graph
        .into_attempt_graph(&mut next)
        .expect("Core projection");
    assert_eq!(frozen.root, root);
    assert_eq!(frozen.edges.len(), edges.len());
    assert_eq!(next.get(), 10 + frozen.tasks.len() as u64);
    for edge in edges {
        let projected = &frozen.edges[&edge.edge_id()];
        assert_eq!(projected.destination_node, edge.destination_node_id());
        assert_eq!(&*projected.producers, edge.producers());
        assert_eq!(&*projected.destinations, edge.destinations());
        assert_eq!(projected.delivery, edge.delivery());
    }
    for task in frozen.tasks.values() {
        let original = originals
            .remove(&task.creation.resource)
            .expect("one original per Task");
        assert_eq!(original.seed.identity(), task.identity);
        assert_eq!(original.seed.participation(), task.participation);
        assert_eq!(original.seed.split_plan_nodes(), &*task.split_plan_nodes);
        assert_eq!(
            original.sizing.descriptor_encoded_bytes,
            encode_task_descriptor(&original.seed.descriptor().unwrap()).encoded_len()
        );
        let parts = original
            .seed
            .freeze(original.sizing.lengths)
            .expect("same priced creation");
        let envelope = OperationEnvelope::with_default_wait(
            TaskOperationId::new_v7(),
            OperationKind::CreateTask,
        );
        let intent = super::creation::CreateTaskIntent::new(envelope, task.identity, parts, None);
        assert_eq!(task.creation.queued_bytes, intent.queued_bytes());
    }
    assert!(originals.is_empty());
    novarocks_attempt_protocol::graph::TaskGraph::from_frozen(frozen)
        .expect("Core accepts the exact manifest topology");
}

#[test]
fn multicast_sink_edges_bind_actual_graph_ids_and_endpoint_changes_only_metadata() {
    let processes = backends(3);
    let schedule = chain_schedule(&[0, 1], &[1]);
    let edges = multicast_edges();
    // Intentionally reverse the graph edge order to prove the binding follows
    // static sink positions, not edge order.
    let leaf_targets = [
        (ROOT_FRAGMENT, LEAF_TO_ROOT_NODE),
        (MIDDLE_FRAGMENT, LEAF_TO_MIDDLE_NODE),
    ];
    let graph = build_protocol_graph(
        execution_id(),
        FrontendProcessId::new_v7(),
        &schedule,
        &edges,
        &processes,
        &mut FakePlans::new(&schedule, &edges, 64).with_leaf_sink(multicast_sink(leaf_targets)),
    )
    .expect("multicast graph");
    let leaves = graph
        .tasks()
        .filter(|task| task.fragment_id() == LEAF_FRAGMENT)
        .map(TaskNode::task_id)
        .collect::<Vec<_>>();
    assert_eq!(leaves.len(), 2);
    let leaf_fragment = Arc::clone(graph.seed(leaves[0]).expect("leaf seed").fragment());
    assert!(
        Arc::ptr_eq(
            &leaf_fragment,
            graph.seed(leaves[1]).expect("leaf seed").fragment()
        ),
        "placement cannot alter the frozen fragment"
    );
    let first_descriptor = descriptor_of(&graph, leaves[0]);
    let expected = [LEAF_TO_ROOT_NODE, LEAF_TO_MIDDLE_NODE]
        .into_iter()
        .map(|node_id| {
            first_descriptor
                .topology()
                .outbound()
                .iter()
                .find(|edge| edge.destination_node_id().get() == node_id)
                .expect("scheduled edge")
                .edge_id()
                .get()
        })
        .collect::<Vec<_>>();
    let metadata = frozen_metadata(graph);
    assert_eq!(sink_edge_ids(&metadata[&leaves[0]]), expected);
    assert_eq!(sink_edge_ids(&metadata[&leaves[1]]), expected);

    let mut moved = schedule.clone();
    for placement in moved.by_fragment.values_mut().flatten() {
        if placement.backend_idx == 1 {
            placement.endpoint = RuntimeEndpoint::new("127.0.0.9", 19099).expect("moved endpoint");
        }
    }
    let moved_graph = build_protocol_graph(
        execution_id(),
        FrontendProcessId::new_v7(),
        &moved,
        &edges,
        &processes,
        &mut FakePlans::new(&moved, &edges, 64).with_leaf_sink(multicast_sink(leaf_targets)),
    )
    .expect("moved graph");
    assert_eq!(
        leaf_fragment.content().bytes(),
        moved_graph
            .seed(leaves[0])
            .expect("moved leaf seed")
            .fragment()
            .content()
            .bytes(),
        "endpoint is absent from the frozen fragment"
    );
    assert_ne!(
        encode_task_descriptor(&first_descriptor).encode_to_vec(),
        encode_task_descriptor(&descriptor_of(&moved_graph, leaves[0])).encode_to_vec(),
        "the task-local topology carries the changed endpoint"
    );
}

#[test]
fn router_route_positions_bind_their_actual_exchange_edge_ids() {
    let processes = backends(3);
    let schedule = chain_schedule(&[0], &[1]);
    let edges = multicast_edges();
    let graph = build_protocol_graph(
        execution_id(),
        FrontendProcessId::new_v7(),
        &schedule,
        &edges,
        &processes,
        &mut FakePlans::new(&schedule, &edges, 64).with_leaf_sink(router_sink([
            (ROOT_FRAGMENT, LEAF_TO_ROOT_NODE),
            (MIDDLE_FRAGMENT, LEAF_TO_MIDDLE_NODE),
        ])),
    )
    .expect("router graph");
    let leaf = graph
        .tasks()
        .find(|task| task.fragment_id() == LEAF_FRAGMENT)
        .map(TaskNode::task_id)
        .expect("leaf task");
    let descriptor = descriptor_of(&graph, leaf);
    let expected = [LEAF_TO_ROOT_NODE, LEAF_TO_MIDDLE_NODE]
        .into_iter()
        .map(|node_id| {
            descriptor
                .topology()
                .outbound()
                .iter()
                .find(|edge| edge.destination_node_id().get() == node_id)
                .expect("router edge")
                .edge_id()
                .get()
        })
        .collect::<Vec<_>>();
    assert_eq!(sink_edge_ids(&frozen_metadata(graph)[&leaf]), expected);
}

fn same_node_targets_bind_distinct_edges(router: bool) {
    let processes = backends(3);
    let schedule = chain_schedule(&[0], &[1]);
    // Exchange node IDs are local to their target fragment. Both consumers
    // can legally name node zero, including a CTE multicast or MERGE router.
    let edges = vec![
        stream_edge(LEAF_FRAGMENT, MIDDLE_FRAGMENT, 0),
        stream_edge(MIDDLE_FRAGMENT, ROOT_FRAGMENT, MIDDLE_TO_ROOT_NODE),
        stream_edge(LEAF_FRAGMENT, ROOT_FRAGMENT, 0),
    ];
    let targets = [(ROOT_FRAGMENT, 0), (MIDDLE_FRAGMENT, 0)];
    let graph = build_protocol_graph(
        execution_id(),
        FrontendProcessId::new_v7(),
        &schedule,
        &edges,
        &processes,
        &mut FakePlans::new(&schedule, &edges, 64).with_leaf_sink(if router {
            router_sink(targets)
        } else {
            multicast_sink(targets)
        }),
    )
    .expect("same node IDs in different fragments have distinct edges");
    let leaf = graph
        .tasks()
        .find(|task| task.fragment_id() == LEAF_FRAGMENT)
        .map(TaskNode::task_id)
        .expect("leaf task");
    let descriptor = descriptor_of(&graph, leaf);
    let target_stages = [ROOT_FRAGMENT, MIDDLE_FRAGMENT].map(|fragment| {
        graph
            .tasks()
            .find(|task| task.fragment_id() == fragment)
            .expect("target stage")
            .stage_id()
    });
    let expected = target_stages.map(|stage| {
        descriptor
            .topology()
            .outbound()
            .iter()
            .find(|edge| {
                edge.destination_node_id().get() == 0
                    && edge
                        .destinations()
                        .iter()
                        .all(|destination| destination.task().stage_id() == stage)
            })
            .expect("exact target fragment and node")
            .edge_id()
            .get()
    });
    assert_ne!(expected[0], expected[1]);
    assert_eq!(sink_edge_ids(&frozen_metadata(graph)[&leaf]), expected);
}

#[test]
fn cte_multicast_same_node_ids_bind_by_target_fragment() {
    same_node_targets_bind_distinct_edges(false);
}

#[test]
fn merge_router_same_node_ids_bind_by_target_fragment() {
    same_node_targets_bind_distinct_edges(true);
}

#[test]
fn static_sink_unknown_target_fragment_fails_closed() {
    let processes = backends(3);
    let schedule = chain_schedule(&[0], &[1]);
    let edges = vec![
        stream_edge(LEAF_FRAGMENT, MIDDLE_FRAGMENT, 0),
        stream_edge(MIDDLE_FRAGMENT, ROOT_FRAGMENT, MIDDLE_TO_ROOT_NODE),
        stream_edge(LEAF_FRAGMENT, ROOT_FRAGMENT, 0),
    ];
    let error = build_protocol_graph(
        execution_id(),
        FrontendProcessId::new_v7(),
        &schedule,
        &edges,
        &processes,
        &mut FakePlans::new(&schedule, &edges, 64)
            .with_leaf_sink(multicast_sink([(ROOT_FRAGMENT, 0), (99, 0)])),
    )
    .expect_err("a static sink cannot guess the target from a reused node ID");
    assert!(
        error
            .to_string()
            .contains("static sink targets absent fragment 99")
    );
}

#[test]
fn a_static_plan_beyond_the_old_descriptor_bound_is_built() {
    let processes = backends(1);
    let schedule = chain_schedule(&[0], &[0]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 16 * 1024 * 1024 + 1)
        .expect("message and preparation owners govern large immutable plans");
    assert!(graph.stages().len() > 0);
}

fn protocol_frozen_fragments(
    schedule: &SchedulingPlan,
    plans: &mut FakePlans<'_>,
) -> Result<BTreeMap<FragmentId, Arc<FragmentArtifact>>, TaskExecutionError> {
    let mut fragments = BTreeMap::new();
    for &fragment in schedule.by_fragment.keys() {
        fragments.insert(fragment, plans.take_plan(fragment, 0)?.fragment);
    }
    Ok(fragments)
}

fn build_protocol_graph(
    execution: QueryExecutionId,
    frontend: FrontendProcessId,
    schedule: &SchedulingPlan,
    edges: &[crate::query_execution::attempt_plan_facts::AttemptEdgeFacts],
    processes: &BTreeMap<usize, BackendProcessId>,
    plans: &mut FakePlans<'_>,
) -> Result<TaskGraph, TaskExecutionError> {
    let fragments = protocol_frozen_fragments(schedule, plans)?;
    let manifest = TaskManifestBinding::protocol_fixture_for_test(
        execution, frontend, schedule, edges, processes, fragments,
    )
    .map_err(|error| TaskExecutionError::Schedule(error.to_string()))?;
    let graph = build_task_graph_from_manifest(&manifest, plans, TransportBudget::DEFAULT);
    // The graph now owns only its original per-task fragment aliases. The
    // temporary fixture template must really retire before callers count them.
    drop(manifest);
    graph
}

fn build_graph(
    schedule: &SchedulingPlan,
    edges: &[crate::query_execution::attempt_plan_facts::AttemptEdgeFacts],
    processes: &BTreeMap<usize, BackendProcessId>,
    plan_bytes: usize,
) -> Result<TaskGraph, TaskExecutionError> {
    build_protocol_graph(
        execution_id(),
        FrontendProcessId::new_v7(),
        schedule,
        edges,
        processes,
        &mut FakePlans::new(schedule, edges, plan_bytes),
    )
}

#[test]
fn stage_and_task_ids_follow_the_static_schedule_and_reuse_its_kernel_keys() {
    let processes = backends(3);
    let schedule = chain_schedule(&[0, 1], &[1, 2]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 512).expect("a legal graph");

    // One stage per fragment, in the schedule's own ascending fragment order.
    let stages = graph
        .stages()
        .map(|stage| (stage.stage_id().get(), stage.fragment_id()))
        .collect::<Vec<_>>();
    assert_eq!(
        stages,
        vec![(1, LEAF_FRAGMENT), (2, MIDDLE_FRAGMENT), (3, ROOT_FRAGMENT)]
    );

    // Task ids come from one counter over the attempt and are never reused.
    let ids = graph
        .tasks()
        .map(|task| task.task_id().get())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);

    // Every kernel key is the schedule's own frozen value, not a second
    // derivation.
    for task in graph.tasks() {
        let expected = task_key_for_contract_test(
            execution_id().query_id(),
            task.fragment_id(),
            task.instance_index(),
        );
        assert_eq!(task.task_key(), expected);
        let descriptor = descriptor_of(&graph, task.task_id());
        assert_eq!(descriptor.task_key(), expected);
    }
}

#[test]
fn every_destination_and_source_carries_both_addresses() {
    let processes = backends(3);
    let schedule = chain_schedule(&[0, 1], &[1, 2]);
    let graph = build_graph(&schedule, &chain_edges(), &processes, 512).expect("a legal graph");

    let key_of = |identity: TaskIdentity| {
        graph
            .task(identity.task_id())
            .expect("the destination is a graph task")
            .task_key()
    };

    for task in graph.tasks() {
        let descriptor = descriptor_of(&graph, task.task_id());
        for edge in descriptor.topology().outbound() {
            assert!(!edge.destinations().is_empty());
            for destination in edge.destinations() {
                assert_eq!(destination.task_key(), key_of(destination.task()));
                assert_eq!(
                    destination.destination_node_id(),
                    edge.destination_node_id()
                );
            }
        }
        for inbound in descriptor.topology().inbound() {
            for source in inbound.sources() {
                assert_eq!(source.task_key(), key_of(source.task()));
            }
            // The producer's sender count and the consumer's expectation are
            // derived once per exchange node, so they cannot disagree; each
            // producer counts at its own position in that union.
            let expected = inbound.expected_sender_count();
            let mut ordinals = BTreeSet::new();
            for producer in inbound.sources() {
                let producer_descriptor = descriptor_of(&graph, producer.task().task_id());
                let edge = producer_descriptor
                    .topology()
                    .outbound()
                    .iter()
                    .find(|edge| edge.destination_node_id() == inbound.node_id())
                    .expect("the producer has an edge into this node");
                assert_eq!(edge.sender_count(), expected);
                assert!(
                    ordinals.insert(edge.sender_ordinal()),
                    "two producers of one node may not count at one position"
                );
            }
        }
    }
}

#[test]
fn a_repeated_kernel_key_is_refused() {
    let mut schedule = chain_schedule(&[0, 1], &[1]);
    let leaf = schedule
        .by_fragment
        .get_mut(&LEAF_FRAGMENT)
        .expect("the leaf fragment is placed");
    leaf[1].task_key = leaf[0].task_key;

    let error =
        TaskManifestBinding::protocol_collision_for_test(leaf[0].task_key, leaf[1].task_key)
            .expect_err("two tasks may not share one kernel key");
    assert_eq!(
        error.kind(),
        crate::query_execution::contract::DistributedQueryErrorKind::ContractViolation
    );
    assert!(
        error
            .message()
            .contains("fragment-instance identity collision")
    );
    assert!(error.message().contains(&leaf[0].task_key.to_string()));
}

#[test]
fn a_scheduled_backend_without_a_frozen_process_identity_is_refused() {
    let processes = backends(2);
    let schedule = chain_schedule(&[0, 1], &[0]);
    let edges = chain_edges();
    let fragments =
        protocol_frozen_fragments(&schedule, &mut FakePlans::new(&schedule, &edges, 512))
            .expect("original fragments");
    let error = TaskManifestBinding::protocol_missing_backend_for_test(
        execution_id(),
        FrontendProcessId::new_v7(),
        &schedule,
        &edges,
        &processes,
        fragments,
        1,
    )
    .expect_err("a placement needs a frozen backend process identity");
    assert_eq!(
        error.kind(),
        crate::query_execution::contract::DistributedQueryErrorKind::ContractViolation
    );
    assert!(
        error
            .message()
            .contains("names backend process absent from the frozen endpoint snapshot")
    );
    assert!(error.message().contains(&processes[&1].to_string()));
}

#[test]
fn a_union_node_counts_every_producer_of_every_edge_that_feeds_it() {
    let processes = backends(3);
    let schedule = chain_schedule(&[0, 1, 2], &[1, 2]);
    // The leaves feed the middle and, with the middle, one root node.
    let edges = vec![
        stream_edge(LEAF_FRAGMENT, MIDDLE_FRAGMENT, LEAF_TO_MIDDLE_NODE),
        stream_edge(LEAF_FRAGMENT, ROOT_FRAGMENT, MIDDLE_TO_ROOT_NODE),
        stream_edge(MIDDLE_FRAGMENT, ROOT_FRAGMENT, MIDDLE_TO_ROOT_NODE),
    ];
    let graph = build_graph(&schedule, &edges, &processes, 64).expect("a union graph");
    let root = descriptor_of(&graph, graph.root_task());
    let inbound = root
        .topology()
        .inbound()
        .iter()
        .find(|inbound| inbound.node_id().get() == MIDDLE_TO_ROOT_NODE)
        .expect("the union node");
    assert_eq!(
        inbound.sources().len(),
        5,
        "three leaves and two middles feed the union node"
    );
    let mut ordinals = BTreeSet::new();
    for source in inbound.sources() {
        let producer = descriptor_of(&graph, source.task().task_id());
        let edge = producer
            .topology()
            .outbound()
            .iter()
            .find(|edge| edge.destination_node_id() == inbound.node_id())
            .expect("the producer has an edge into the union node");
        assert_eq!(edge.sender_count(), inbound.expected_sender_count());
        assert!(ordinals.insert(edge.sender_ordinal()));
    }
    assert_eq!(
        ordinals,
        (0..5).collect::<BTreeSet<_>>(),
        "every producer of the union counts at its own position"
    );
}

#[test]
fn original_manifest_and_passive_activation_form_one_attempt_host_projection() {
    use super::host::{geometry::AttemptHostGeometry, publication::PublicationRecord};
    use super::manifest_projection::{
        AttemptActivationInputs, AttemptManifestProjection, AttemptPlanPolicy,
        ManifestAttemptInputs,
    };
    use super::sources::{ParticipationEstablishFacts, ParticipationEstablishSource};
    use novarocks_attempt_protocol::{
        AttemptCore, Core,
        config::CoordinationBudgets,
        effect::Publication,
        ids::IdSeed,
        input::PublicationApplied,
        publication::{ProtocolMembership, ProtocolPublication},
    };
    use novarocks_control_runtime_core::identity::ScopeId;
    use novarocks_coordination_contract::{
        AttemptActivationIdentity, AttemptAuthorizationLimits, MonotonicInstant,
    };
    use novarocks_execution::task_execution::{
        CredentialDomainId, CredentialEpoch, CredentialUpdate, ParticipationRef,
    };
    use std::time::Duration;

    // The original fixture's exact opaque Establish facts, without its retired
    // QA drive or any Task execution, Work or runtime owner.
    struct FakeEstablish;
    impl ParticipationEstablishSource for FakeEstablish {
        fn facts_for(
            &self,
            _participation: ParticipationRef,
        ) -> Result<ParticipationEstablishFacts, TaskExecutionError> {
            Ok(ParticipationEstablishFacts {
                catalog_binding: Arc::new(novarocks_task_codec::domain::WireContent::new(
                    b"test.catalog",
                    novarocks_proto_models::catalog::CatalogSet::default(),
                )),
                initial_runtime_filter: Arc::new(novarocks_task_codec::domain::WireContent::new(
                    b"test.filter",
                    wire::RuntimeFilterContribution::default(),
                )),
                query_options: Arc::new(novarocks_task_codec::domain::WireContent::new(
                    b"test.options",
                    wire::QueryOptions {
                        batch_size: 4096,
                        query_timeout: 30,
                        pipeline_dop: 1,
                        ..Default::default()
                    },
                )),
                initial_credential: CredentialUpdate::new(
                    CredentialDomainId::new(7),
                    CredentialEpoch::FIRST,
                    Arc::new(
                        novarocks_task_codec::domain::WireCredential::decode(
                            &[],
                            novarocks_proto_codec::FieldPath::root("test.credential"),
                        )
                        .unwrap(),
                    ),
                ),
            })
        }
    }

    let schedule = chain_schedule(&[0, 1], &[1]);
    let edges = chain_edges();
    let processes = backends(2);
    let mut plans = FakePlans::new(&schedule, &edges, 64);
    let fragments = protocol_frozen_fragments(&schedule, &mut plans).unwrap();
    let manifest = TaskManifestBinding::protocol_fixture_for_test(
        execution_id(),
        FrontendProcessId::new_v7(),
        &schedule,
        &edges,
        &processes,
        fragments,
    )
    .unwrap();
    let compatibility = manifest.participations()[0]
        .backend()
        .native_compatibility_id();

    // Passive values are supplied by the LP activation owner. This test checks
    // their unchanged projection; authorization decisions are checked by LP.
    let activation = AttemptActivationIdentity::from_parts(1, manifest.execution(), 1);
    let establish_limit = NonZeroUsize::new(3).unwrap();
    let abort_limit = NonZeroUsize::new(5).unwrap();
    let authorization_limits = AttemptAuthorizationLimits {
        activation,
        establish_per_participation: establish_limit,
        abort_per_participation: abort_limit,
    };
    let root = manifest.root();
    let plan_seal = manifest.plan();
    let task_count = manifest.tasks().len();
    let roster = manifest
        .participations()
        .iter()
        .map(|member| {
            let backend = member.backend();
            (
                member.participation(),
                backend.target().descriptor().clone(),
                backend.admission_epoch_capability(),
            )
        })
        .collect::<Vec<_>>();
    let mut next = NonZeroU64::new(10).unwrap();
    let mut prepared = ManifestAttemptInputs::freeze(
        &manifest,
        &mut plans,
        compatibility,
        TransportBudget::DEFAULT,
    )
    .unwrap()
    .prepare_publications()
    .unwrap();
    let mut protocol_facts = prepared.publications.protocol_facts.take().unwrap();
    drop(manifest);

    let layout = crate::native::transport_capacity::NativeTransportCapacity::from_transport(
        TransportBudget::DEFAULT,
    )
    .unwrap();
    let geometry = AttemptHostGeometry::derive(
        ScopeId::new(1).unwrap(),
        NonZeroUsize::new(roster.len()).unwrap(),
        CoordinationBudgets::DEFAULT.dispatch,
        &layout,
        &[],
        true,
    )
    .unwrap();
    let lease = super::lease::RequestedLeaseDurations::DEFAULT;
    let mut establish_preparation = super::host::transport::EstablishMaterialPreparation::new();
    establish_preparation
        .freeze(&FakeEstablish, roster.iter().map(|row| row.0))
        .unwrap();
    let projected = AttemptManifestProjection::from_prepared_manifest(
        prepared,
        &mut establish_preparation,
        AttemptActivationInputs {
            identity: activation,
            authorization_limits,
            replacement_admissions: None,
            ids: IdSeed::new(1, 100),
            initial_now: MonotonicInstant::from_origin(Duration::from_secs(2)),
            retain_diagnostics: true,
            observe_dev_root: false,
            observe_lifecycle_diagnostics: false,
        },
        AttemptPlanPolicy {
            coordination: CoordinationBudgets::DEFAULT,
            retry: Default::default(),
            initial_lease: lease.initial(),
            steady_lease: lease.steady(),
        },
        Box::new([]),
        &mut next,
    )
    .unwrap()
    .bind_sources(geometry.sources);
    assert_eq!(
        geometry.actor_config(&projected.start).unwrap().mailbox,
        geometry.sources.mailbox,
    );
    let plan = &projected.start.plan;
    assert_eq!(projected.start.activation.identity, activation);
    assert_eq!(plan.authorization_limits, authorization_limits);
    assert_eq!(
        plan.authorization_limits.establish_per_participation,
        establish_limit
    );
    assert_eq!(
        plan.authorization_limits.abort_per_participation,
        abort_limit
    );
    assert_eq!(plan.graph.root, root);
    assert_eq!(plan.graph.tasks.len(), task_count);
    assert_eq!(plan.graph.edges.len(), edges.len());
    assert_eq!(plan.establish_material.len(), roster.len());
    assert!(
        projected
            .backends
            .iter()
            .eq(roster.iter().map(|entry| &entry.1))
    );
    assert_eq!(
        next.get() as usize,
        10 + projected.creations.len() + plan.establish_material.len()
    );
    assert!(
        projected
            .members
            .iter()
            .map(|member| &member.participation)
            .eq(plan.establish_material.keys())
    );
    for (participation, backend, admission_epoch) in roster {
        let member = projected
            .members
            .iter()
            .find(|member| member.participation == participation)
            .unwrap();
        assert_eq!(&member.endpoint, backend.endpoint());
        assert_eq!(
            plan.admission_epochs[&backend.process_id()],
            admission_epoch
        );
        assert_eq!(
            plan.preparation_positions[&backend.process_id()].get(),
            backend.preparing_positions()
        );
        assert!(
            !projected
                .creations
                .contains_key(&plan.establish_material[&participation].material)
        );
    }
    for task in plan.graph.tasks.values() {
        assert_eq!(
            projected.creations[&task.creation.resource].seed.identity(),
            task.identity
        );
    }
    let initial = ProtocolPublication {
        owner_revision: 0,
        initial_membership: Some(Arc::new(ProtocolMembership {
            writers: plan.writers.clone(),
            summary_sources: plan.summary_sources.clone(),
        })),
        changes: Vec::new(),
        selected_failure: None,
        root_success_sealed: false,
    };
    let mut publication =
        PublicationRecord::new(activation, projected.publications.protocol, None, None);
    assert_eq!(
        publication
            .publish(&Publication::ProtocolBatch(initial), None)
            .unwrap(),
        PublicationApplied::ProtocolBatch { published: true },
    );
    assert!(
        protocol_facts
            .take_changes(activation, root, plan_seal)
            .unwrap()
            .unwrap()
            .is_initial()
    );

    <Core as AttemptCore>::new(projected.start)
        .expect("Core accepts the original authorization limits and exact manifest projection");
}

/// The SAME frozen metadata as synchronous_root_control_round's old
/// Harness::new(&[0], &[1], 64). This grants no Task/native/Source work.
pub(crate) fn split_component_chain_graph() -> TaskGraph {
    let schedule = chain_schedule(&[0], &[1]);
    build_graph(&schedule, &chain_edges(), &backends(2), 64)
        .expect("the original split component chain is a legal graph")
}
