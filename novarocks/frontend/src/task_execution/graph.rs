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

//! The stage and task graph derived from the frozen static schedule.
//!
//! One fragment becomes one stage and one fragment instance becomes one task.
//! Both ids are minted here, once, in the schedule's own deterministic order,
//! and neither is reused inside a query execution.
//!
//! The kernel key is not minted here. `FragmentInstancePlacement::finst_id` is
//! already the one derivation of a fragment instance id in this crate, so a
//! descriptor carries that exact value for itself and for every destination
//! and source of its topology. Deriving it a second time would create a
//! second authority over the same fact; the only thing this builder adds is a
//! collision check, which is what makes the identity-to-kernel-key mapping
//! provably one-to-one rather than assumed to be.

use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;

use novarocks_execution::exec::fragment::program::FragmentNodeId;
use novarocks_execution::exec::fragment::sink::DataStreamPartitionType;
use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
use novarocks_execution::task_execution::{
    ExchangeDestination, ExchangeEdge, ExchangeEdgeId, ExchangeInbound, ExchangeSource,
    ExchangeTopology, PhysicalFragmentPlan, PlanNodeId, QueryContextRef, StageRef, TaskDescriptor,
    TaskIdentity, TransportBudget,
};
use novarocks_sql::plan_read::{FragmentEdge, FragmentId, PartitionKind};
use novarocks_types::UniqueId;
use novarocks_types::identity::{
    BackendProcessId, FrontendProcessId, QueryExecutionId, StageId, TaskId,
};

use super::error::{CapacityBound, TaskExecutionError, schedule_error};
use crate::query_execution::FragmentInstancePlacement;
use crate::query_execution::schedule::SchedulingPlan;

/// The plan facts a fragment's tasks are created with.
///
/// The physical plan has no transport-neutral value form in this engine, so it
/// arrives behind the codec-owned [`PhysicalFragmentPlan`] handle. The graph
/// builder never reaches into it; it only carries it into the descriptor.
#[derive(Clone, Debug)]
pub struct FragmentPlanFacts {
    pub plan: Arc<dyn PhysicalFragmentPlan>,
    pub pipeline_dop: NonZeroUsize,
}

/// Where a fragment instance's physical plan comes from.
///
/// Keyed by instance, not by fragment. The encoded plan carries this
/// instance's own parameters -- its fragment instance id, its destinations,
/// its per-exchange sender counts -- and the backend refuses a descriptor
/// whose plan names a different instance than the descriptor does. Sharing one
/// encoding across a fragment's instances would therefore be rejected for
/// every instance but one, which is every non-root fragment on a multi-backend
/// cluster.
pub trait FragmentPlanSource {
    fn plan_for(
        &self,
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
    instance_index: usize,
    backend_idx: usize,
    fragment_instance_id: UniqueId,
    split_plan_nodes: Vec<PlanNodeId>,
    is_root: bool,
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

    pub const fn instance_index(&self) -> usize {
        self.instance_index
    }

    pub const fn backend_idx(&self) -> usize {
        self.backend_idx
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

    /// Whether this task produces the query's client-visible result.
    pub const fn is_root(&self) -> bool {
        self.is_root
    }
}

/// One stage: the complete, frozen task set of one fragment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StageNode {
    stage: StageRef,
    fragment_id: FragmentId,
    tasks: Vec<TaskId>,
}

impl StageNode {
    pub const fn stage(&self) -> StageRef {
        self.stage
    }

    pub const fn stage_id(&self) -> StageId {
        self.stage.stage_id()
    }

    pub const fn fragment_id(&self) -> FragmentId {
        self.fragment_id
    }

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
    frontend_process_id: FrontendProcessId,
    root_task: TaskId,
    tasks: BTreeMap<TaskId, TaskNode>,
    stages: BTreeMap<StageId, StageNode>,
    edges: BTreeMap<ExchangeEdgeId, EdgeNode>,
    producer_stages: BTreeMap<StageId, BTreeSet<StageId>>,
    contexts: BTreeSet<QueryContextRef>,
    descriptors: BTreeMap<TaskId, TaskDescriptor>,
}

impl TaskGraph {
    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn frontend_process_id(&self) -> FrontendProcessId {
        self.frontend_process_id
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

    pub fn edge(&self, edge_id: ExchangeEdgeId) -> Option<&EdgeNode> {
        self.edges.get(&edge_id)
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

    /// Takes the frozen descriptors out so each one is owned by exactly one
    /// task from here on.
    pub fn into_descriptors(mut self) -> (Self, BTreeMap<TaskId, TaskDescriptor>) {
        let descriptors = std::mem::take(&mut self.descriptors);
        (self, descriptors)
    }

    /// The frozen descriptor of one task, while the graph still owns it.
    pub fn descriptor(&self, task_id: TaskId) -> Option<&TaskDescriptor> {
        self.descriptors.get(&task_id)
    }
}

/// Everything the graph builder reads.
#[derive(Clone, Debug)]
pub struct TaskGraphInputs<'a> {
    pub execution_id: QueryExecutionId,
    pub frontend_process_id: FrontendProcessId,
    pub root_fragment_id: FragmentId,
    pub placements: &'a BTreeMap<FragmentId, Vec<FragmentInstancePlacement>>,
    pub fragment_edges: &'a [FragmentEdge],
    pub backend_process_ids: &'a BTreeMap<usize, BackendProcessId>,
    pub transport_budget: TransportBudget,
}

impl<'a> TaskGraphInputs<'a> {
    /// Reads the frozen placement result of one validated schedule.
    #[allow(
        dead_code,
        reason = "The task protocol is not routed into production yet; this reads the crate-private schedule for the transport cutover and for this module's tests."
    )]
    pub(crate) fn from_schedule(
        execution_id: QueryExecutionId,
        frontend_process_id: FrontendProcessId,
        schedule: &'a SchedulingPlan,
        fragment_edges: &'a [FragmentEdge],
        backend_process_ids: &'a BTreeMap<usize, BackendProcessId>,
        transport_budget: TransportBudget,
    ) -> Self {
        Self {
            execution_id,
            frontend_process_id,
            root_fragment_id: schedule.root_fragment_id,
            placements: &schedule.by_fragment,
            fragment_edges,
            backend_process_ids,
            transport_budget,
        }
    }
}

/// One exchange node's merged sender set.
///
/// Sender ordinals and the sender count are derived once per exchange node
/// over every incoming edge, so the producer's outbound count and the
/// consumer's `expected_sender_count` are equal by construction. A union whose
/// exchange node is fed by two fragments therefore produces two edges over one
/// shared ordinal space, rather than two edges each claiming to be the whole
/// sender set.
struct SenderSet {
    ordinals: BTreeMap<TaskId, u32>,
    count: NonZeroU32,
    partitioning: DataStreamPartitionType,
}

/// Builds the stage and task graph of one attempt from its frozen schedule.
pub fn build_task_graph(
    inputs: TaskGraphInputs<'_>,
    plans: &dyn FragmentPlanSource,
) -> Result<TaskGraph, TaskExecutionError> {
    if inputs.placements.is_empty() {
        return Err(TaskExecutionError::Schedule(
            "schedule places no fragment".to_owned(),
        ));
    }
    if !inputs.placements.contains_key(&inputs.root_fragment_id) {
        return Err(TaskExecutionError::Schedule(format!(
            "root fragment {} is absent from the schedule",
            inputs.root_fragment_id
        )));
    }

    // Stage ids follow the schedule's own ascending fragment order and task
    // ids come from one counter spanning the attempt, so neither is reused
    // inside this query execution.
    let mut stage_of_fragment = BTreeMap::<FragmentId, StageId>::new();
    let mut next_stage = 1_u32;
    for &fragment_id in inputs.placements.keys() {
        stage_of_fragment.insert(
            fragment_id,
            StageId::new(next_stage)
                .map_err(|error| TaskExecutionError::Schedule(error.to_string()))?,
        );
        next_stage = next_stage
            .checked_add(1)
            .ok_or_else(|| TaskExecutionError::Schedule("stage id space exhausted".to_owned()))?;
    }

    let mut tasks = BTreeMap::<TaskId, TaskNode>::new();
    let mut task_of_instance = BTreeMap::<(FragmentId, usize), TaskId>::new();
    let mut stage_tasks = BTreeMap::<StageId, Vec<TaskId>>::new();
    let mut endpoints = BTreeMap::<TaskId, RuntimeEndpoint>::new();
    let mut contexts = BTreeSet::<QueryContextRef>::new();
    let mut context_task_counts = BTreeMap::<QueryContextRef, usize>::new();
    let mut kernel_keys = BTreeMap::<UniqueId, TaskId>::new();
    let mut next_task = 1_u32;

    for (&fragment_id, placements) in inputs.placements {
        if placements.is_empty() {
            return Err(TaskExecutionError::Schedule(format!(
                "fragment {fragment_id} has no placement"
            )));
        }
        let stage_id = stage_of_fragment[&fragment_id];
        for placement in placements {
            let backend_process_id = *inputs
                .backend_process_ids
                .get(&placement.backend_idx)
                .ok_or(TaskExecutionError::UnknownBackend {
                    backend_idx: placement.backend_idx,
                })?;
            let task_id = TaskId::new(next_task)
                .map_err(|error| schedule_error(placement, error.to_string()))?;
            next_task = next_task.checked_add(1).ok_or_else(|| {
                TaskExecutionError::Schedule("task id space exhausted".to_owned())
            })?;
            if let Some(previous) = kernel_keys.insert(placement.finst_id, task_id) {
                return Err(TaskExecutionError::KernelKeyCollision {
                    first: previous,
                    second: task_id,
                });
            }
            let identity =
                TaskIdentity::new(inputs.execution_id, stage_id, task_id, backend_process_id);
            let context = QueryContextRef::new(
                inputs.execution_id,
                inputs.frontend_process_id,
                backend_process_id,
            );
            let tasks_in_context = context_task_counts.entry(context).or_default();
            *tasks_in_context += 1;
            if *tasks_in_context > inputs.transport_budget.max_tasks_per_context() {
                return Err(CapacityBound::TasksPerContext {
                    limit: inputs.transport_budget.max_tasks_per_context(),
                }
                .into());
            }
            contexts.insert(context);
            let split_plan_nodes = placement
                .scan_ranges
                .keys()
                .map(|&node_id| {
                    PlanNodeId::new(node_id)
                        .map_err(|error| schedule_error(placement, error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            if task_of_instance
                .insert((fragment_id, placement.instance_index), task_id)
                .is_some()
            {
                return Err(schedule_error(placement, "instance index repeats"));
            }
            endpoints.insert(task_id, placement.endpoint.clone());
            tasks.insert(
                task_id,
                TaskNode {
                    identity,
                    context,
                    fragment_id,
                    instance_index: placement.instance_index,
                    backend_idx: placement.backend_idx,
                    fragment_instance_id: placement.finst_id,
                    split_plan_nodes,
                    is_root: false,
                },
            );
            stage_tasks.entry(stage_id).or_default().push(task_id);
        }
    }

    let root_task = *task_of_instance
        .get(&(inputs.root_fragment_id, 0))
        .ok_or_else(|| {
            TaskExecutionError::Schedule(format!(
                "root fragment {} has no instance zero",
                inputs.root_fragment_id
            ))
        })?;
    tasks
        .get_mut(&root_task)
        .expect("the root task was just minted")
        .is_root = true;

    let sender_sets = build_sender_sets(&inputs, &stage_of_fragment, &task_of_instance)?;
    let (edges, outbound, inbound) = build_topologies(
        &inputs,
        &stage_of_fragment,
        &task_of_instance,
        &tasks,
        &endpoints,
        &sender_sets,
    )?;

    let mut descriptors = BTreeMap::<TaskId, TaskDescriptor>::new();
    for (&fragment_id, placements) in inputs.placements {
        for placement in placements {
            let facts = plans.plan_for(fragment_id, placement.instance_index)?;
            if facts.plan.encoded_len() > inputs.transport_budget.max_descriptor_encoded_bytes() {
                return Err(CapacityBound::DescriptorBytes {
                    limit: inputs.transport_budget.max_descriptor_encoded_bytes(),
                    actual: facts.plan.encoded_len(),
                }
                .into());
            }
            let task_id = task_of_instance[&(fragment_id, placement.instance_index)];
            let node = &tasks[&task_id];
            let topology = ExchangeTopology::try_new(
                outbound.get(&task_id).cloned().unwrap_or_default(),
                inbound.get(&task_id).cloned().unwrap_or_default(),
            )?;
            descriptors.insert(
                task_id,
                TaskDescriptor::try_new(
                    node.identity,
                    node.fragment_instance_id,
                    facts.pipeline_dop,
                    node.split_plan_nodes.clone(),
                    topology,
                    Arc::clone(&facts.plan),
                )?,
            );
        }
    }

    let mut producer_stages = BTreeMap::<StageId, BTreeSet<StageId>>::new();
    for edge in edges.values() {
        producer_stages
            .entry(edge.consumer_stage)
            .or_default()
            .insert(edge.producer_stage);
    }

    let stages = stage_tasks
        .into_iter()
        .map(|(stage_id, task_ids)| {
            let fragment_id = *stage_of_fragment
                .iter()
                .find(|&(_, &stage)| stage == stage_id)
                .expect("every stage came from a fragment")
                .0;
            (
                stage_id,
                StageNode {
                    stage: StageRef::new(inputs.execution_id, stage_id),
                    fragment_id,
                    tasks: task_ids,
                },
            )
        })
        .collect();

    Ok(TaskGraph {
        execution_id: inputs.execution_id,
        frontend_process_id: inputs.frontend_process_id,
        root_task,
        tasks,
        stages,
        edges,
        producer_stages,
        contexts,
        descriptors,
    })
}

fn build_sender_sets(
    inputs: &TaskGraphInputs<'_>,
    stage_of_fragment: &BTreeMap<FragmentId, StageId>,
    task_of_instance: &BTreeMap<(FragmentId, usize), TaskId>,
) -> Result<BTreeMap<(FragmentId, i32), SenderSet>, TaskExecutionError> {
    let mut grouped = BTreeMap::<(FragmentId, i32), Vec<FragmentId>>::new();
    let mut partitionings = BTreeMap::<(FragmentId, i32), DataStreamPartitionType>::new();
    for edge in inputs.fragment_edges {
        if !stage_of_fragment.contains_key(&edge.source_fragment_id)
            || !stage_of_fragment.contains_key(&edge.target_fragment_id)
        {
            continue;
        }
        let key = (edge.target_fragment_id, edge.target_exchange_node_id);
        let partitioning = partition_type(edge.output_partition.kind);
        match partitionings.get(&key) {
            Some(&frozen) if frozen != partitioning => {
                return Err(TaskExecutionError::Schedule(format!(
                    "exchange node {} of fragment {} is fed with {} and {}",
                    edge.target_exchange_node_id,
                    edge.target_fragment_id,
                    frozen.display_name(),
                    partitioning.display_name()
                )));
            }
            Some(_) => {}
            None => {
                partitionings.insert(key, partitioning);
            }
        }
        let sources = grouped.entry(key).or_default();
        if sources.contains(&edge.source_fragment_id) {
            return Err(TaskExecutionError::Schedule(format!(
                "fragment {} reaches exchange node {} of fragment {} twice",
                edge.source_fragment_id, edge.target_exchange_node_id, edge.target_fragment_id
            )));
        }
        sources.push(edge.source_fragment_id);
    }

    grouped
        .into_iter()
        .map(|(key, mut sources)| {
            sources.sort_unstable();
            let mut ordinals = BTreeMap::new();
            let mut next_ordinal = 0_u32;
            for source_fragment_id in sources {
                let mut instance_index = 0_usize;
                while let Some(&task_id) =
                    task_of_instance.get(&(source_fragment_id, instance_index))
                {
                    ordinals.insert(task_id, next_ordinal);
                    next_ordinal += 1;
                    instance_index += 1;
                }
            }
            let count = NonZeroU32::new(next_ordinal).ok_or_else(|| {
                TaskExecutionError::Schedule(format!(
                    "exchange node {} of fragment {} has no sender",
                    key.1, key.0
                ))
            })?;
            let partitioning = partitionings[&key];
            Ok((
                key,
                SenderSet {
                    ordinals,
                    count,
                    partitioning,
                },
            ))
        })
        .collect()
}

type Topologies = (
    BTreeMap<ExchangeEdgeId, EdgeNode>,
    BTreeMap<TaskId, Vec<ExchangeEdge>>,
    BTreeMap<TaskId, Vec<ExchangeInbound>>,
);

fn build_topologies(
    inputs: &TaskGraphInputs<'_>,
    stage_of_fragment: &BTreeMap<FragmentId, StageId>,
    task_of_instance: &BTreeMap<(FragmentId, usize), TaskId>,
    tasks: &BTreeMap<TaskId, TaskNode>,
    endpoints: &BTreeMap<TaskId, RuntimeEndpoint>,
    sender_sets: &BTreeMap<(FragmentId, i32), SenderSet>,
) -> Result<Topologies, TaskExecutionError> {
    let mut edges = BTreeMap::<ExchangeEdgeId, EdgeNode>::new();
    let mut outbound = BTreeMap::<TaskId, Vec<ExchangeEdge>>::new();
    let mut inbound_sources = BTreeMap::<(TaskId, i32), Vec<ExchangeSource>>::new();
    let mut next_edge = 1_u32;

    // Edges follow the frozen order of the fragment-edge list, so the same
    // schedule always mints the same edge ids.
    for fragment_edge in inputs.fragment_edges {
        let (Some(&producer_stage), Some(&consumer_stage)) = (
            stage_of_fragment.get(&fragment_edge.source_fragment_id),
            stage_of_fragment.get(&fragment_edge.target_fragment_id),
        ) else {
            continue;
        };
        let node_id = FragmentNodeId::new(fragment_edge.target_exchange_node_id);
        let senders = &sender_sets[&(
            fragment_edge.target_fragment_id,
            fragment_edge.target_exchange_node_id,
        )];
        let edge_id = ExchangeEdgeId::new(next_edge)
            .map_err(|error| TaskExecutionError::Schedule(error.to_string()))?;
        next_edge = next_edge
            .checked_add(1)
            .ok_or_else(|| TaskExecutionError::Schedule("edge id space exhausted".to_owned()))?;

        let consumers = instances_of(task_of_instance, fragment_edge.target_fragment_id);
        if consumers.is_empty() {
            return Err(TaskExecutionError::Schedule(format!(
                "exchange edge into fragment {} has no destination",
                fragment_edge.target_fragment_id
            )));
        }
        let producers = instances_of(task_of_instance, fragment_edge.source_fragment_id);
        if producers.is_empty() {
            return Err(TaskExecutionError::Schedule(format!(
                "exchange edge out of fragment {} has no producer",
                fragment_edge.source_fragment_id
            )));
        }

        for &producer in &producers {
            let sender_ordinal = *senders.ordinals.get(&producer).ok_or_else(|| {
                TaskExecutionError::Schedule(format!(
                    "producer task {producer} is absent from the sender set of exchange node {}",
                    fragment_edge.target_exchange_node_id
                ))
            })?;
            let destinations = consumers
                .iter()
                .map(|&consumer| {
                    ExchangeDestination::try_new(
                        tasks[&consumer].identity,
                        tasks[&consumer].fragment_instance_id,
                        endpoints[&consumer].clone(),
                        node_id,
                        sender_ordinal,
                        senders.count,
                    )
                    .map_err(TaskExecutionError::from)
                })
                .collect::<Result<Vec<_>, _>>()?;
            outbound
                .entry(producer)
                .or_default()
                .push(ExchangeEdge::try_new(
                    edge_id,
                    node_id,
                    senders.partitioning,
                    destinations,
                )?);
        }
        for &consumer in &consumers {
            let sources = inbound_sources
                .entry((consumer, fragment_edge.target_exchange_node_id))
                .or_default();
            for &producer in &producers {
                sources.push(ExchangeSource::new(
                    tasks[&producer].identity,
                    tasks[&producer].fragment_instance_id,
                ));
            }
        }

        edges.insert(
            edge_id,
            EdgeNode {
                edge_id,
                producer_stage,
                consumer_stage,
                destination_node_id: node_id,
                producers: producers
                    .iter()
                    .map(|producer| tasks[producer].identity)
                    .collect(),
                destinations: consumers
                    .iter()
                    .map(|consumer| tasks[consumer].identity)
                    .collect(),
            },
        );
    }

    let mut inbound = BTreeMap::<TaskId, Vec<ExchangeInbound>>::new();
    for ((task_id, node_id), sources) in inbound_sources {
        inbound
            .entry(task_id)
            .or_default()
            .push(ExchangeInbound::try_new(
                FragmentNodeId::new(node_id),
                sources,
            )?);
    }
    Ok((edges, outbound, inbound))
}

fn instances_of(
    task_of_instance: &BTreeMap<(FragmentId, usize), TaskId>,
    fragment_id: FragmentId,
) -> Vec<TaskId> {
    let mut instances = Vec::new();
    let mut instance_index = 0_usize;
    while let Some(&task_id) = task_of_instance.get(&(fragment_id, instance_index)) {
        instances.push(task_id);
        instance_index += 1;
    }
    instances
}

const fn partition_type(kind: PartitionKind) -> DataStreamPartitionType {
    match kind {
        PartitionKind::Unpartitioned => DataStreamPartitionType::Unpartitioned,
        PartitionKind::Random => DataStreamPartitionType::Random,
        PartitionKind::Hash => DataStreamPartitionType::HashPartitioned,
    }
}
