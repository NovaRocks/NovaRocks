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

//! Task protocol projection of the exact application-bound attempt manifest.
//!
//! Identities, placements, endpoints and sender ordinals come from the original
//! QA schedule and manifest binding. This projection creates only the Task
//! protocol values and creation seeds that retain those same frozen facts.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::native::fragment_encoder::frozen::{FragmentArtifact, StaticSinkTarget};
use crate::query_execution::artifact::FragmentId;
use crate::query_execution::artifact::NativePlacementAssignment;
use novarocks_execution::exec::fragment::program::FragmentNodeId;
use novarocks_execution::exec::fragment::sink::DataStreamPartitionType;
use novarocks_execution::task_execution::{
    ExchangeEdgeId, PlanNodeId, QueryContextRef, StageRef, TaskIdentity,
};
use novarocks_task_codec::TransportBudget;
use novarocks_types::UniqueId;
use novarocks_types::identity::{QueryExecutionId, StageId, TaskId};

use super::creation::{
    AttemptTopology, EdgeDestination, EdgeProducer, SharedEdge, TaskCreationFacts,
    TaskCreationSeed, TaskTopologyView,
};
use super::error::{CapacityBound, TaskExecutionError};
use crate::query_execution::artifact::{BoundManifestPartitionKind, TaskManifestBinding};

/// What one fragment instance is created from.
///
/// The frozen fragment is shared by every instance of its fragment; only the
/// assignment is this instance's own.
#[derive(Debug)]
pub struct FragmentPlanFacts {
    pub(crate) fragment: Arc<FragmentArtifact>,
    pub(crate) assignment: NativePlacementAssignment,
}

/// Binds each static sink branch of one producer fragment to the logical edge
/// the task graph minted for it.
///
/// The branches are read from the frozen fragment's typed facts in declared
/// order; the edges are the producer stage's own. Every task of the fragment
/// produces on the same logical edges, so one binding serves all of them.
fn bind_sink_edges(
    targets: &[StaticSinkTarget],
    outbound: &[&EdgeNode],
    stage_of_fragment: &BTreeMap<FragmentId, StageId>,
) -> Result<Arc<[ExchangeEdgeId]>, TaskExecutionError> {
    if targets.len() != outbound.len() {
        return Err(TaskExecutionError::Schedule(
            "static sink count differs from the task's outbound edge count".to_owned(),
        ));
    }
    let mut used = BTreeSet::new();
    let mut sink_edges = Vec::with_capacity(targets.len());
    for target in targets {
        let fragment_id = target.target_fragment_id();
        let node_id = target.target_exchange_node_id();
        let target_stage = stage_of_fragment.get(&fragment_id).ok_or_else(|| {
            TaskExecutionError::Schedule(format!(
                "static sink targets absent fragment {fragment_id}"
            ))
        })?;
        let matching = outbound
            .iter()
            .filter(|edge| {
                edge.destination_node_id().get() == node_id
                    && edge.consumer_stage() == *target_stage
            })
            .collect::<Vec<_>>();
        let [edge] = matching.as_slice() else {
            return Err(TaskExecutionError::Schedule(format!(
                "static sink target fragment {fragment_id} node {node_id} does not identify exactly one task edge"
            )));
        };
        if target.delivery() != edge.delivery() {
            return Err(TaskExecutionError::Schedule(
                "static sink delivery differs from its frozen task edge".to_owned(),
            ));
        }
        if !used.insert(edge.edge_id()) {
            return Err(TaskExecutionError::Schedule(format!(
                "task edge {} is bound to multiple static sinks",
                edge.edge_id()
            )));
        }
        sink_edges.push(edge.edge_id());
    }
    Ok(sink_edges.into())
}

/// Where a fragment instance's creation facts come from.
///
/// Keyed by instance, not merely by fragment. Every placement of a fragment
/// shares its frozen fragment, while its assignment carries that instance's
/// own ordinal, width and initial scan ranges. The graph binds sink branches
/// to edges only after it has minted the edges.
pub trait FragmentPlanSource {
    /// Control-owner tests may supply neutral graph targets independently of
    /// an executable package. This hook never exists in production: real task
    /// creation always binds exactly the checked package's own sink facts.
    #[cfg(test)]
    fn control_topology_targets(&self, _fragment_id: FragmentId) -> Option<Vec<StaticSinkTarget>> {
        None
    }

    /// Moves one instance's creation facts out of the source.
    ///
    /// Taken rather than borrowed: an instance's initial scan ranges are held
    /// by exactly one owner at a time, and after the graph binds them into a
    /// creation seed the source no longer has a copy.
    fn take_plan(
        &mut self,
        fragment_id: FragmentId,
        instance_index: usize,
    ) -> Result<FragmentPlanFacts, TaskExecutionError>;
}

/// Immutable per-task facts, ids only.
///
/// The descriptor is deliberately absent: it is moved into the owning
/// `RemoteTask` exactly once, and everything that only needs an address reads
/// this instead of holding a second copy.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskNode {
    identity: TaskIdentity,
    context: QueryContextRef,
    fragment_id: FragmentId,
    #[cfg(test)]
    instance_index: usize,
    fragment_instance_id: UniqueId,
    split_plan_nodes: Vec<PlanNodeId>,
}

impl TaskNode {
    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }

    pub const fn context(&self) -> QueryContextRef {
        self.context
    }

    pub const fn task_id(&self) -> TaskId {
        self.identity.task_id()
    }

    pub const fn stage_id(&self) -> StageId {
        self.identity.stage_id()
    }

    pub const fn fragment_id(&self) -> FragmentId {
        self.fragment_id
    }

    #[cfg(test)]
    pub const fn instance_index(&self) -> usize {
        self.instance_index
    }

    /// The execution kernel's key for this task, taken from the frozen
    /// schedule rather than derived again.
    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    /// The plan nodes of this task that accept split assignments.
    pub fn split_plan_nodes(&self) -> &[PlanNodeId] {
        &self.split_plan_nodes
    }
}

/// One stage: the complete, frozen task set of one fragment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StageNode {
    stage: StageRef,
    #[cfg(test)]
    fragment_id: FragmentId,
    #[cfg(test)]
    tasks: Vec<TaskId>,
}

impl StageNode {
    pub const fn stage(&self) -> StageRef {
        self.stage
    }

    pub const fn stage_id(&self) -> StageId {
        self.stage.stage_id()
    }

    #[cfg(test)]
    pub const fn fragment_id(&self) -> FragmentId {
        self.fragment_id
    }

    #[cfg(test)]
    pub fn tasks(&self) -> &[TaskId] {
        &self.tasks
    }
}

/// One frozen push exchange edge: the producers of one stage and the complete
/// destination set of one exchange node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeNode {
    edge_id: ExchangeEdgeId,
    producer_stage: StageId,
    consumer_stage: StageId,
    destination_node_id: FragmentNodeId,
    delivery: novarocks_execution_contract::ExchangeDeliveryRequirement,
    producers: Vec<TaskIdentity>,
    destinations: Vec<TaskIdentity>,
}

impl EdgeNode {
    pub const fn edge_id(&self) -> ExchangeEdgeId {
        self.edge_id
    }

    pub const fn producer_stage(&self) -> StageId {
        self.producer_stage
    }

    pub const fn consumer_stage(&self) -> StageId {
        self.consumer_stage
    }

    pub const fn destination_node_id(&self) -> FragmentNodeId {
        self.destination_node_id
    }

    pub const fn delivery(&self) -> novarocks_execution_contract::ExchangeDeliveryRequirement {
        self.delivery
    }

    pub fn producers(&self) -> &[TaskIdentity] {
        &self.producers
    }

    /// Every destination whose creation must be acknowledged before this edge
    /// may be opened.
    pub fn destinations(&self) -> &[TaskIdentity] {
        &self.destinations
    }
}

/// The complete stage and task graph of one query execution attempt.
#[derive(Debug)]
pub struct TaskGraph {
    execution_id: QueryExecutionId,
    root_task: TaskId,
    tasks: BTreeMap<TaskId, TaskNode>,
    stages: BTreeMap<StageId, StageNode>,
    edges: BTreeMap<ExchangeEdgeId, EdgeNode>,
    producer_stages: BTreeMap<StageId, BTreeSet<StageId>>,
    contexts: BTreeSet<QueryContextRef>,
    seeds: BTreeMap<TaskId, TaskCreationSeed>,
}

impl TaskGraph {
    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn root_task(&self) -> TaskId {
        self.root_task
    }

    pub fn root_identity(&self) -> TaskIdentity {
        self.tasks
            .get(&self.root_task)
            .expect("a validated graph contains its root task")
            .identity()
    }

    pub fn task(&self, task_id: TaskId) -> Option<&TaskNode> {
        self.tasks.get(&task_id)
    }

    pub fn tasks(&self) -> impl ExactSizeIterator<Item = &TaskNode> + '_ {
        self.tasks.values()
    }

    pub fn stage(&self, stage_id: StageId) -> Option<&StageNode> {
        self.stages.get(&stage_id)
    }

    pub fn stages(&self) -> impl ExactSizeIterator<Item = &StageNode> + '_ {
        self.stages.values()
    }

    pub fn edges(&self) -> impl ExactSizeIterator<Item = &EdgeNode> + '_ {
        self.edges.values()
    }

    /// The stages that feed `stage_id`, which are exactly the stages this one
    /// releases when it stops consuming.
    pub fn producer_stages(&self, stage_id: StageId) -> impl Iterator<Item = StageId> + '_ {
        self.producer_stages
            .get(&stage_id)
            .into_iter()
            .flatten()
            .copied()
    }

    /// Every query context this attempt establishes: exactly one per backend
    /// process carrying at least one task.
    pub fn contexts(&self) -> impl ExactSizeIterator<Item = &QueryContextRef> + '_ {
        self.contexts.iter()
    }

    /// Takes the creation seeds out so each one is owned by exactly one task
    /// from here on.
    pub(crate) fn into_seeds(mut self) -> (Self, BTreeMap<TaskId, TaskCreationSeed>) {
        let seeds = std::mem::take(&mut self.seeds);
        (self, seeds)
    }

    /// The creation seed of one task, while the graph still owns it.
    #[cfg(test)]
    pub(crate) fn seed(&self, task_id: TaskId) -> Option<&TaskCreationSeed> {
        self.seeds.get(&task_id)
    }
}

/// Every task's place in the shared topology: the edges it produces on, in
/// edge order, and per consumed exchange node, every edge feeding it.
fn task_topology_views(
    tasks: &BTreeMap<TaskId, TaskNode>,
    edges: &BTreeMap<ExchangeEdgeId, EdgeNode>,
) -> BTreeMap<TaskId, TaskTopologyView> {
    let task_of_identity = tasks
        .iter()
        .map(|(&task_id, node)| (node.identity, task_id))
        .collect::<BTreeMap<_, _>>();
    let mut outbound = BTreeMap::<TaskId, Vec<ExchangeEdgeId>>::new();
    let mut inbound = BTreeMap::<TaskId, BTreeMap<FragmentNodeId, Vec<ExchangeEdgeId>>>::new();
    for edge in edges.values() {
        for producer in &edge.producers {
            outbound
                .entry(task_of_identity[producer])
                .or_default()
                .push(edge.edge_id);
        }
        for destination in &edge.destinations {
            inbound
                .entry(task_of_identity[destination])
                .or_default()
                .entry(edge.destination_node_id)
                .or_default()
                .push(edge.edge_id);
        }
    }
    tasks
        .keys()
        .map(|&task_id| {
            (
                task_id,
                TaskTopologyView::new(
                    outbound.remove(&task_id).unwrap_or_default(),
                    inbound.remove(&task_id).unwrap_or_default(),
                ),
            )
        })
        .collect()
}

/// Binds one task's facts into its creation seed.
///
/// The static plan's own encoded size is checked here, before anything is
/// sent: the backend bounds the static plan together with the task
/// assignment, so a static plan already over that bound can never become a
/// legal creation. The combined bound is checked again once the assignment's
/// exact size is known.
fn bind_creation_seed(
    topology: &Arc<AttemptTopology>,
    views: &BTreeMap<TaskId, TaskTopologyView>,
    edges: &BTreeMap<ExchangeEdgeId, EdgeNode>,
    stage_of_fragment: &BTreeMap<FragmentId, StageId>,
    sink_bindings: &mut BTreeMap<StageId, Arc<[ExchangeEdgeId]>>,
    node: &TaskNode,
    facts: FragmentPlanFacts,
    transport_budget: TransportBudget,
    #[cfg(test)] control_topology_targets: Option<&[StaticSinkTarget]>,
) -> Result<TaskCreationSeed, TaskExecutionError> {
    let FragmentPlanFacts {
        fragment,
        assignment,
    } = facts;
    if fragment.facts().fragment_id() != node.fragment_id {
        return Err(TaskExecutionError::Schedule(format!(
            "task {} of fragment {} was handed the frozen plan of fragment {}",
            node.identity,
            node.fragment_id,
            fragment.facts().fragment_id()
        )));
    }
    let static_bytes = fragment.content().len();
    if static_bytes > transport_budget.max_descriptor_encoded_bytes() {
        return Err(CapacityBound::DescriptorBytes {
            limit: transport_budget.max_descriptor_encoded_bytes(),
            actual: static_bytes,
        }
        .into());
    }
    let view = views.get(&node.task_id()).cloned().ok_or_else(|| {
        TaskExecutionError::Schedule(format!("task {} has no view", node.identity))
    })?;
    let sink_edges = match sink_bindings.get(&node.stage_id()) {
        Some(binding) => Arc::clone(binding),
        None => {
            let outbound = view
                .outbound()
                .iter()
                .map(|edge_id| &edges[edge_id])
                .collect::<Vec<_>>();
            let targets = fragment.facts().sink_targets();
            #[cfg(test)]
            let targets = control_topology_targets.unwrap_or(targets);
            let binding = bind_sink_edges(targets, &outbound, stage_of_fragment)?;
            sink_bindings.insert(node.stage_id(), Arc::clone(&binding));
            binding
        }
    };
    Ok(TaskCreationSeed::new(
        Arc::clone(topology),
        view,
        fragment,
        sink_edges,
        TaskCreationFacts {
            identity: node.identity,
            context: node.context,
            kernel_key: node.fragment_instance_id,
            pipeline_dop: assignment.pipeline_dop(),
            split_plan_nodes: node.split_plan_nodes.clone(),
            instance_ordinal: assignment.instance_ordinal(),
            initial_scan_ranges: assignment.into_initial_scan_ranges(),
        },
    ))
}

/// Builds the Task protocol graph from the exact application-bound manifest.
///
/// This path mints no identity and consults no
/// scheduling plan. Every Task, context, endpoint, edge id, and sender ordinal
/// is projected from the manifest that already joined the actor schedule to
/// its originating topology snapshot.
pub(crate) fn build_task_graph_from_manifest(
    manifest: &TaskManifestBinding,
    plans: &mut dyn FragmentPlanSource,
    transport_budget: TransportBudget,
) -> Result<TaskGraph, TaskExecutionError> {
    let frontend_process_id = manifest
        .contexts()
        .first()
        .ok_or_else(|| TaskExecutionError::Schedule("attempt manifest has no context".to_owned()))?
        .context()
        .frontend_process_id();
    let mut endpoints_by_process = BTreeMap::new();
    let mut contexts = BTreeSet::new();
    for context in manifest.contexts() {
        if context.context().query_execution_id() != manifest.execution()
            || context.context().frontend_process_id() != frontend_process_id
            || context.context().backend_process_id() != context.backend().process_id()
        {
            return Err(TaskExecutionError::Schedule(format!(
                "attempt manifest context {} disagrees with its execution or backend binding",
                context.context()
            )));
        }
        if endpoints_by_process
            .insert(
                context.backend().process_id(),
                context.backend().endpoint().clone(),
            )
            .is_some()
        {
            return Err(TaskExecutionError::Schedule(format!(
                "attempt manifest repeats backend process {}",
                context.backend().process_id()
            )));
        }
        contexts.insert(context.context());
    }

    let mut tasks = BTreeMap::<TaskId, TaskNode>::new();
    let mut tasks_by_identity = BTreeMap::<TaskIdentity, TaskId>::new();
    let mut stage_fragments = BTreeMap::<StageId, FragmentId>::new();
    let mut kernel_keys = BTreeMap::<UniqueId, TaskId>::new();
    let mut context_task_counts = BTreeMap::<QueryContextRef, usize>::new();
    for task in manifest.tasks() {
        let identity = task.identity();
        if identity.query_execution_id() != manifest.execution()
            || identity.backend_process_id() != task.context().backend_process_id()
            || !contexts.contains(&task.context())
        {
            return Err(TaskExecutionError::Schedule(format!(
                "attempt manifest task {identity} disagrees with its execution or context"
            )));
        }
        match stage_fragments.insert(identity.stage_id(), task.fragment_id()) {
            Some(fragment) if fragment != task.fragment_id() => {
                return Err(TaskExecutionError::Schedule(format!(
                    "attempt manifest stage {} names both fragments {fragment} and {}",
                    identity.stage_id(),
                    task.fragment_id()
                )));
            }
            _ => {}
        }
        let count = context_task_counts.entry(task.context()).or_default();
        *count += 1;
        if *count > transport_budget.max_tasks_per_context() {
            return Err(CapacityBound::TasksPerContext {
                limit: transport_budget.max_tasks_per_context(),
            }
            .into());
        }
        if kernel_keys
            .insert(task.fragment_instance_id(), identity.task_id())
            .is_some()
        {
            return Err(TaskExecutionError::Schedule(format!(
                "attempt manifest repeats fragment instance {}",
                task.fragment_instance_id()
            )));
        }
        let split_plan_nodes = task
            .scan_work()
            .iter()
            .map(|work| {
                PlanNodeId::new(work.scan().node_id())
                    .map_err(|error| TaskExecutionError::Schedule(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let node = TaskNode {
            identity,
            context: task.context(),
            fragment_id: task.fragment_id(),
            #[cfg(test)]
            instance_index: task.instance_index(),
            fragment_instance_id: task.fragment_instance_id(),
            split_plan_nodes,
        };
        if tasks.insert(identity.task_id(), node).is_some()
            || tasks_by_identity
                .insert(identity, identity.task_id())
                .is_some()
        {
            return Err(TaskExecutionError::Schedule(format!(
                "attempt manifest repeats task identity {identity}"
            )));
        }
    }
    let root_task = tasks_by_identity
        .get(&manifest.root())
        .copied()
        .ok_or_else(|| {
            TaskExecutionError::Schedule("attempt manifest root is absent".to_owned())
        })?;

    let mut edges = BTreeMap::<ExchangeEdgeId, EdgeNode>::new();
    let mut shared_edges = Vec::with_capacity(manifest.edges().len());
    for edge in manifest.edges() {
        let producer_stage = edge
            .producers()
            .first()
            .ok_or_else(|| {
                TaskExecutionError::Schedule("manifest edge has no producer".to_owned())
            })?
            .task()
            .stage_id();
        let consumer_stage = edge
            .destinations()
            .first()
            .ok_or_else(|| {
                TaskExecutionError::Schedule("manifest edge has no destination".to_owned())
            })?
            .stage_id();
        let node_id = FragmentNodeId::new(edge.target_exchange_node_id());
        let mut producers = Vec::with_capacity(edge.producers().len());
        for producer in edge.producers() {
            let producer_task = *tasks_by_identity.get(&producer.task()).ok_or_else(|| {
                TaskExecutionError::Schedule(format!(
                    "manifest edge {} names missing producer {}",
                    edge.edge_id(),
                    producer.task()
                ))
            })?;
            producers.push(EdgeProducer::new(
                producer.task(),
                tasks[&producer_task].fragment_instance_id,
                producer.sender_ordinal(),
            ));
        }
        let mut destinations = Vec::with_capacity(edge.destinations().len());
        for destination in edge.destinations() {
            let task_id = *tasks_by_identity.get(destination).ok_or_else(|| {
                TaskExecutionError::Schedule(format!(
                    "manifest edge {} names missing destination {destination}",
                    edge.edge_id()
                ))
            })?;
            let endpoint = endpoints_by_process
                .get(&destination.backend_process_id())
                .ok_or_else(|| {
                    TaskExecutionError::Schedule(format!(
                        "manifest destination {destination} has no frozen endpoint"
                    ))
                })?;
            destinations.push(EdgeDestination::new(
                *destination,
                tasks[&task_id].fragment_instance_id,
                endpoint.clone(),
            ));
        }
        shared_edges.push(SharedEdge::new(
            edge.edge_id(),
            node_id,
            manifest_partition_type(edge.partition_kind()),
            edge.delivery(),
            edge.sender_count(),
            producers,
            destinations,
        ));
        if edges
            .insert(
                edge.edge_id(),
                EdgeNode {
                    edge_id: edge.edge_id(),
                    producer_stage,
                    consumer_stage,
                    destination_node_id: node_id,
                    delivery: edge.delivery(),
                    producers: edge.producers().iter().map(|p| p.task()).collect(),
                    destinations: edge.destinations().to_vec(),
                },
            )
            .is_some()
        {
            return Err(TaskExecutionError::Schedule(format!(
                "attempt manifest repeats exchange edge {}",
                edge.edge_id()
            )));
        }
    }
    let views = task_topology_views(&tasks, &edges);
    let topology = Arc::new(AttemptTopology::try_new(
        shared_edges,
        &views.values().collect::<Vec<_>>(),
    )?);

    let mut stage_of_fragment = BTreeMap::new();
    for (&stage_id, &fragment_id) in &stage_fragments {
        if stage_of_fragment.insert(fragment_id, stage_id).is_some() {
            return Err(TaskExecutionError::Schedule(format!(
                "attempt manifest repeats fragment {fragment_id} in multiple stages"
            )));
        }
    }
    let mut seeds = BTreeMap::new();
    let mut sink_bindings = BTreeMap::<StageId, Arc<[ExchangeEdgeId]>>::new();
    for task in manifest.tasks() {
        #[cfg(test)]
        let control_topology_targets = plans.control_topology_targets(task.fragment_id());
        let facts = plans.take_plan(task.fragment_id(), task.instance_index())?;
        let task_id = task.identity().task_id();
        let seed = bind_creation_seed(
            &topology,
            &views,
            &edges,
            &stage_of_fragment,
            &mut sink_bindings,
            &tasks[&task_id],
            facts,
            transport_budget,
            #[cfg(test)]
            control_topology_targets.as_deref(),
        )?;
        seeds.insert(task_id, seed);
    }

    let stages = stage_fragments
        .keys()
        .map(|&stage_id| {
            (
                stage_id,
                StageNode {
                    stage: StageRef::new(manifest.execution(), stage_id),
                    #[cfg(test)]
                    fragment_id: stage_fragments[&stage_id],
                    #[cfg(test)]
                    tasks: tasks
                        .iter()
                        .filter_map(|(&task_id, task)| {
                            (task.stage_id() == stage_id).then_some(task_id)
                        })
                        .collect(),
                },
            )
        })
        .collect();
    let mut producer_stages = BTreeMap::<StageId, BTreeSet<StageId>>::new();
    for edge in edges.values() {
        producer_stages
            .entry(edge.consumer_stage)
            .or_default()
            .insert(edge.producer_stage);
    }
    Ok(TaskGraph {
        execution_id: manifest.execution(),
        root_task,
        tasks,
        stages,
        edges,
        producer_stages,
        contexts,
        seeds,
    })
}

const fn manifest_partition_type(kind: BoundManifestPartitionKind) -> DataStreamPartitionType {
    match kind {
        BoundManifestPartitionKind::Unpartitioned => DataStreamPartitionType::Unpartitioned,
        BoundManifestPartitionKind::Random => DataStreamPartitionType::Random,
        BoundManifestPartitionKind::Hash => DataStreamPartitionType::HashPartitioned,
    }
}
