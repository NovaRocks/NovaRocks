// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The exact host-frozen task graph, without executable creation custody.

use crate::error::TaskExecutionError;
use crate::start::{FrozenCreation, FrozenTaskGraph};
use novarocks_execution_contract::descriptor::FragmentNodeId;
use novarocks_execution_contract::{
    ExchangeDeliveryRequirement, ExchangeEdgeId, ParticipationRef, PlanNodeId, StageRef,
    TaskIdentity,
};
use novarocks_types::{QueryExecutionId, StageId, TaskId, UniqueId};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskNode {
    identity: TaskIdentity,
    participation: ParticipationRef,
    fragment_id: u32,
    task_key: UniqueId,
    split_plan_nodes: Box<[PlanNodeId]>,
}
impl TaskNode {
    pub const fn identity(&self) -> TaskIdentity {
        self.identity
    }
    pub const fn participation(&self) -> ParticipationRef {
        self.participation
    }
    pub const fn task_id(&self) -> TaskId {
        self.identity.task_id()
    }
    pub const fn stage_id(&self) -> StageId {
        self.identity.stage_id()
    }
    pub const fn fragment_id(&self) -> u32 {
        self.fragment_id
    }
    pub const fn task_key(&self) -> UniqueId {
        self.task_key
    }
    pub fn split_plan_nodes(&self) -> &[PlanNodeId] {
        &self.split_plan_nodes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StageNode {
    stage: StageRef,
}
impl StageNode {
    pub const fn stage(&self) -> StageRef {
        self.stage
    }
    pub const fn stage_id(&self) -> StageId {
        self.stage.stage_id()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeNode {
    edge_id: ExchangeEdgeId,
    producer_stage: StageId,
    consumer_stage: StageId,
    destination_node_id: FragmentNodeId,
    delivery: ExchangeDeliveryRequirement,
    producers: Box<[TaskIdentity]>,
    destinations: Box<[TaskIdentity]>,
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
    pub const fn delivery(&self) -> ExchangeDeliveryRequirement {
        self.delivery
    }
    pub fn producers(&self) -> &[TaskIdentity] {
        &self.producers
    }
    pub fn destinations(&self) -> &[TaskIdentity] {
        &self.destinations
    }
}

#[derive(Debug)]
pub struct TaskGraph {
    execution_id: QueryExecutionId,
    root_task: TaskId,
    tasks: BTreeMap<TaskId, TaskNode>,
    stages: BTreeMap<StageId, StageNode>,
    edges: BTreeMap<ExchangeEdgeId, EdgeNode>,
    producer_stages: BTreeMap<StageId, BTreeSet<StageId>>,
    participations: BTreeSet<ParticipationRef>,
    seeds: BTreeMap<TaskId, FrozenCreation>,
}
impl TaskGraph {
    /// Validate the passive projection before registering a runnable actor.
    /// This performs no decoding, encoding, topology lookup or admission.
    pub fn from_frozen(input: FrozenTaskGraph) -> Result<Self, TaskExecutionError> {
        let execution_id = input.root.query_execution_id();
        let mut tasks = BTreeMap::new();
        let mut seeds = BTreeMap::new();
        let mut participations = BTreeSet::new();
        let mut frontend = None;
        for (key, task) in input.tasks {
            if key != task.identity.task_id()
                || task.identity.query_execution_id() != execution_id
                || task.participation.query_execution_id() != execution_id
                || task.participation.backend_process_id() != task.identity.backend_process_id()
            {
                return Err(TaskExecutionError::Schedule(
                    "frozen Task identity or participation differs from its graph key".into(),
                ));
            }
            let process = task.participation.frontend_process_id();
            if frontend.is_some_and(|expected| expected != process) {
                return Err(TaskExecutionError::Schedule(
                    "frozen graph contains multiple frontend processes".into(),
                ));
            }
            frontend = Some(process);
            if task
                .split_plan_nodes
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len()
                != task.split_plan_nodes.len()
            {
                return Err(TaskExecutionError::Schedule(
                    "frozen Task repeats a split plan node".into(),
                ));
            }
            participations.insert(task.participation);
            seeds.insert(key, task.creation);
            tasks.insert(
                key,
                TaskNode {
                    identity: task.identity,
                    participation: task.participation,
                    fragment_id: task.fragment_id,
                    task_key: task.task_key,
                    split_plan_nodes: task.split_plan_nodes,
                },
            );
        }
        if tasks.get(&input.root.task_id()).map(TaskNode::identity) != Some(input.root) {
            return Err(TaskExecutionError::Schedule(
                "frozen graph omits its exact root Task".into(),
            ));
        }
        let mut stages = BTreeMap::new();
        let mut assigned = BTreeSet::new();
        for (id, members) in input.stages {
            for identity in &members {
                if identity.stage_id() != id
                    || tasks.get(&identity.task_id()).map(TaskNode::identity) != Some(*identity)
                    || !assigned.insert(*identity)
                {
                    return Err(TaskExecutionError::Schedule(
                        "frozen stage contains an absent, duplicate or foreign Task".into(),
                    ));
                }
            }
            stages.insert(
                id,
                StageNode {
                    stage: StageRef::new(execution_id, id),
                },
            );
        }
        if assigned.len() != tasks.len() {
            return Err(TaskExecutionError::Schedule(
                "frozen stages do not cover the exact Task set".into(),
            ));
        }
        let mut edges = BTreeMap::new();
        let mut producer_stages = BTreeMap::<StageId, BTreeSet<StageId>>::new();
        for (key, edge) in input.edges {
            if edge.producers.is_empty() || edge.destinations.is_empty() {
                return Err(TaskExecutionError::Schedule(
                    "frozen exchange edge requires nonempty producer and destination sets".into(),
                ));
            }
            if key != edge.edge
                || !stages.contains_key(&edge.producer_stage)
                || !stages.contains_key(&edge.consumer_stage)
            {
                return Err(TaskExecutionError::Schedule(
                    "frozen exchange edge names an absent stage or differs from its key".into(),
                ));
            }
            for (members, expected_stage) in [
                (&edge.producers, edge.producer_stage),
                (&edge.destinations, edge.consumer_stage),
            ] {
                let mut unique = BTreeSet::new();
                for identity in members.iter() {
                    if identity.stage_id() != expected_stage
                        || tasks.get(&identity.task_id()).map(TaskNode::identity) != Some(*identity)
                        || !unique.insert(*identity)
                    {
                        return Err(TaskExecutionError::Schedule(
                            "frozen exchange edge contains an absent, duplicate or foreign Task"
                                .into(),
                        ));
                    }
                }
            }
            producer_stages
                .entry(edge.consumer_stage)
                .or_default()
                .insert(edge.producer_stage);
            edges.insert(
                key,
                EdgeNode {
                    edge_id: edge.edge,
                    producer_stage: edge.producer_stage,
                    consumer_stage: edge.consumer_stage,
                    destination_node_id: edge.destination_node,
                    delivery: edge.delivery,
                    producers: edge.producers,
                    destinations: edge.destinations,
                },
            );
        }
        Ok(Self {
            execution_id,
            root_task: input.root.task_id(),
            tasks,
            stages,
            edges,
            producer_stages,
            participations,
            seeds,
        })
    }
    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }
    pub const fn root_task(&self) -> TaskId {
        self.root_task
    }
    pub fn root_identity(&self) -> TaskIdentity {
        self.tasks[&self.root_task].identity()
    }
    pub fn task(&self, id: TaskId) -> Option<&TaskNode> {
        self.tasks.get(&id)
    }
    pub fn tasks(&self) -> impl ExactSizeIterator<Item = &TaskNode> {
        self.tasks.values()
    }
    /// Ordered cursor over the original frozen table; never rescans a prefix.
    pub(crate) fn task_after(&self, after: Option<TaskId>) -> Option<&TaskNode> {
        match after {
            Some(after) => self
                .tasks
                .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
                .next(),
            None => self.tasks.first_key_value(),
        }
        .map(|(_, task)| task)
    }
    pub fn stage(&self, id: StageId) -> Option<&StageNode> {
        self.stages.get(&id)
    }
    pub fn stages(&self) -> impl ExactSizeIterator<Item = &StageNode> {
        self.stages.values()
    }
    pub fn edges(&self) -> impl ExactSizeIterator<Item = &EdgeNode> {
        self.edges.values()
    }
    pub fn producer_stages(&self, stage: StageId) -> impl Iterator<Item = StageId> + '_ {
        self.producer_stages
            .get(&stage)
            .into_iter()
            .flatten()
            .copied()
    }
    pub(crate) fn producer_stage_after(
        &self,
        consumer: StageId,
        after: Option<StageId>,
    ) -> Option<StageId> {
        let producers = self.producer_stages.get(&consumer)?;
        match after {
            Some(after) => producers
                .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
                .next()
                .copied(),
            None => producers.first().copied(),
        }
    }
    pub(crate) fn is_producer_stage(&self, consumer: StageId, producer: StageId) -> bool {
        self.producer_stages
            .get(&consumer)
            .is_some_and(|producers| producers.contains(&producer))
    }
    /// Resolve only an existing frozen row. Validation fixed one frontend
    /// for this graph, so this key lookup needs no backend-wide scan and
    /// creates no participation owner, access right or scheduling authority.
    pub(crate) fn participation_on_backend(
        &self,
        backend: novarocks_types::BackendProcessId,
    ) -> Option<ParticipationRef> {
        let root = self.tasks.get(&self.root_task)?;
        let key = ParticipationRef::new(
            self.execution_id,
            root.participation().frontend_process_id(),
            backend,
        );
        self.participations.get(&key).copied()
    }
    pub fn participations(&self) -> impl ExactSizeIterator<Item = &ParticipationRef> {
        self.participations.iter()
    }
    pub(crate) fn into_seeds(mut self) -> (Self, BTreeMap<TaskId, FrozenCreation>) {
        let seeds = std::mem::take(&mut self.seeds);
        (self, seeds)
    }
}
