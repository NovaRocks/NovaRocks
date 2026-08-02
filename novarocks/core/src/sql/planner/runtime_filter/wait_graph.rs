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

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::novarocks_logging::debug;
use crate::sql::planner::runtime_filter::contract::{BindingId, ChannelId, CompletionRequirement};
use crate::sql::planner::runtime_filter::graph::{
    RuntimeFilterBindingRoleData, RuntimeFilterGraphData,
};
use crate::sql::planner::runtime_filter::progress::{
    JoinBuildProgressCatalog, JoinBuildProgressProof, JoinBuildProgressSkip,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct RefinedFragmentEdge {
    pub source_fragment: u32,
    pub target_fragment: u32,
    pub target_exchange_node: i32,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProducerWaitInput {
    pub channel: ChannelId,
    pub binding: BindingId,
    pub fragment: u32,
    pub node_id: i32,
    pub completion_requirement: CompletionRequirement,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsumerWaitBehavior {
    BlocksUntilComplete,
    NeverBlocks,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsumerWaitInput {
    pub channel: ChannelId,
    pub binding: BindingId,
    pub consumer_fragment: u32,
    pub behavior: ConsumerWaitBehavior,
    pub producers: Vec<ProducerWaitInput>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct WaitProvenance {
    pub channel: ChannelId,
    pub consumer_binding: BindingId,
    pub producer_binding: BindingId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PureBlockingScc {
    pub waits: Vec<WaitProvenance>,
    pub witness: Vec<CycleStep>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum WaitNode {
    Frag(u32),
    BuildReady { fragment: u32, join_node: i32 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum WaitEdgeKind {
    DataFlow,
    Frontier,
    Wait(WaitProvenance),
    Backpressure {
        wait: WaitProvenance,
        multicast_fragment: u32,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct CycleStep {
    from: WaitNode,
    kind: WaitEdgeKind,
    to: WaitNode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProofRejection {
    PayloadChannelMismatch,
    PayloadProducerBindingMismatch,
    PayloadProducerFragmentMismatch,
    MissingProducerBinding,
    GraphBindingIdentityMismatch,
    ProducerRoleMismatch,
    CompletionMismatch,
    JoinOwnerMismatch,
    OverlappingPartition,
    PartitionMismatch,
    ConsumerOutsideProbeRegion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProofFallback {
    Rejected(ProofRejection),
    Skipped(JoinBuildProgressSkip),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefinedWaitGraphBuildError {
    FragmentCycle,
}

type WaitGraph = BTreeMap<WaitNode, BTreeSet<(WaitNode, WaitEdgeKind)>>;

#[derive(Clone, Debug)]
pub struct RefinedWaitGraph {
    succ: WaitGraph,
    fallbacks: BTreeMap<WaitProvenance, ProofFallback>,
    accepted_build_ready: BTreeSet<(u32, i32)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RefinedCycle {
    steps: Vec<CycleStep>,
    fallbacks: BTreeMap<WaitProvenance, ProofFallback>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExecutionDependencyGraph {
    deps: BTreeMap<u32, BTreeSet<u32>>,
}

impl ExecutionDependencyGraph {
    pub fn from_fragment_edges(
        edges: &[RefinedFragmentEdge],
    ) -> Result<Self, RefinedWaitGraphBuildError> {
        let mut direct: BTreeMap<u32, BTreeSet<u32>> = BTreeMap::new();
        let mut nodes = BTreeSet::new();
        for edge in edges {
            nodes.insert(edge.source_fragment);
            nodes.insert(edge.target_fragment);
            direct
                .entry(edge.target_fragment)
                .or_default()
                .insert(edge.source_fragment);
        }

        let mut in_degree: BTreeMap<u32, usize> = nodes.iter().map(|node| (*node, 0)).collect();
        let mut dependents: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        for (dependent, predecessors) in &direct {
            *in_degree.get_mut(dependent).expect("node tracked") += predecessors.len();
            for predecessor in predecessors {
                dependents.entry(*predecessor).or_default().push(*dependent);
            }
        }
        let mut queue: VecDeque<u32> = in_degree
            .iter()
            .filter(|(_, degree)| **degree == 0)
            .map(|(node, _)| *node)
            .collect();
        let mut visited = 0;
        while let Some(node) = queue.pop_front() {
            visited += 1;
            if let Some(children) = dependents.get(&node) {
                for child in children {
                    let degree = in_degree.get_mut(child).expect("node tracked");
                    *degree -= 1;
                    if *degree == 0 {
                        queue.push_back(*child);
                    }
                }
            }
        }
        if visited != nodes.len() {
            return Err(RefinedWaitGraphBuildError::FragmentCycle);
        }

        let mut deps = BTreeMap::new();
        for start in &nodes {
            let mut closure = BTreeSet::new();
            let mut frontier = VecDeque::new();
            if let Some(predecessors) = direct.get(start) {
                frontier.extend(predecessors.iter().copied());
            }
            while let Some(fragment) = frontier.pop_front() {
                if closure.insert(fragment)
                    && let Some(predecessors) = direct.get(&fragment)
                {
                    frontier.extend(predecessors.iter().copied());
                }
            }
            deps.insert(*start, closure);
        }
        Ok(Self { deps })
    }

    pub fn reaches(&self, fragment: u32, predecessor: u32) -> bool {
        self.deps
            .get(&fragment)
            .is_some_and(|deps| deps.contains(&predecessor))
    }
}

pub fn project_consumer_waits<A>(
    graph: &RuntimeFilterGraphData<A>,
    behavior: impl Fn(&A) -> ConsumerWaitBehavior,
) -> Vec<ConsumerWaitInput> {
    let mut waits = Vec::new();
    for channel in graph.channels() {
        let producers: Vec<ProducerWaitInput> = graph
            .bindings()
            .filter_map(|binding| {
                if binding.channel_id != channel.channel_id {
                    return None;
                }
                let RuntimeFilterBindingRoleData::Producer(requirement) = &binding.role else {
                    return None;
                };
                Some(ProducerWaitInput {
                    channel: binding.channel_id,
                    binding: binding.binding_id,
                    fragment: binding.location.fragment_id.get(),
                    node_id: binding.location.node_id.get(),
                    completion_requirement: requirement.completion_requirement,
                })
            })
            .collect();
        for binding in graph
            .bindings()
            .filter(|binding| binding.channel_id == channel.channel_id)
        {
            if let RuntimeFilterBindingRoleData::Consumer(requirement) = &binding.role {
                waits.push(ConsumerWaitInput {
                    channel: channel.channel_id,
                    binding: binding.binding_id,
                    consumer_fragment: binding.location.fragment_id.get(),
                    behavior: behavior(&requirement.activation),
                    producers: producers.clone(),
                });
            }
        }
    }
    waits
}

pub fn revalidate_proof(
    proof: &JoinBuildProgressProof,
    edges: &[RefinedFragmentEdge],
    deps: &ExecutionDependencyGraph,
    consumer_fragment: u32,
    expected_channel: ChannelId,
    producer: &ProducerWaitInput,
) -> Result<(), ProofRejection> {
    if proof.channel != expected_channel {
        return Err(ProofRejection::PayloadChannelMismatch);
    }
    if proof.producer_binding != producer.binding {
        return Err(ProofRejection::PayloadProducerBindingMismatch);
    }
    if proof.producer_fragment != producer.fragment {
        return Err(ProofRejection::PayloadProducerFragmentMismatch);
    }
    if producer.binding != proof.producer_binding
        || producer.channel != expected_channel
        || producer.fragment != proof.producer_fragment
    {
        return Err(ProofRejection::GraphBindingIdentityMismatch);
    }
    if producer.completion_requirement != CompletionRequirement::ProducerClosed {
        return Err(ProofRejection::CompletionMismatch);
    }
    if producer.node_id != proof.join_node_id {
        return Err(ProofRejection::JoinOwnerMismatch);
    }
    let sealed_in_edges: BTreeSet<(u32, i32)> = edges
        .iter()
        .filter(|edge| edge.target_fragment == proof.producer_fragment)
        .map(|edge| (edge.source_fragment, edge.target_exchange_node))
        .collect();
    let frontier: BTreeSet<(u32, i32)> = proof
        .build_frontier
        .iter()
        .map(|edge| (edge.source_fragment, edge.target_exchange_node))
        .collect();
    let non_build: BTreeSet<(u32, i32)> = proof
        .non_build_inputs
        .iter()
        .map(|edge| (edge.source_fragment, edge.target_exchange_node))
        .collect();
    if frontier.len() != proof.build_frontier.len()
        || non_build.len() != proof.non_build_inputs.len()
        || !frontier.is_disjoint(&non_build)
    {
        return Err(ProofRejection::OverlappingPartition);
    }
    let union: BTreeSet<(u32, i32)> = frontier.union(&non_build).copied().collect();
    if union != sealed_in_edges {
        return Err(ProofRejection::PartitionMismatch);
    }
    let sane = consumer_fragment == proof.producer_fragment
        || non_build.iter().any(|(source, _)| {
            *source == consumer_fragment || deps.reaches(*source, consumer_fragment)
        });
    if sane {
        Ok(())
    } else {
        Err(ProofRejection::ConsumerOutsideProbeRegion)
    }
}

fn ensure_wait_node(node: WaitNode, succ: &mut WaitGraph) {
    succ.entry(node).or_default();
}

fn add_wait_edge(from: WaitNode, to: WaitNode, kind: WaitEdgeKind, succ: &mut WaitGraph) {
    succ.entry(from).or_default().insert((to, kind));
    succ.entry(to).or_default();
}

pub fn build_refined_wait_graph(
    edges: &[RefinedFragmentEdge],
    consumers: &[ConsumerWaitInput],
    join_progress: &JoinBuildProgressCatalog,
) -> Result<RefinedWaitGraph, RefinedWaitGraphBuildError> {
    let deps = ExecutionDependencyGraph::from_fragment_edges(edges)?;
    let mut succ = WaitGraph::new();

    for edge in edges {
        add_wait_edge(
            WaitNode::Frag(edge.source_fragment),
            WaitNode::Frag(edge.target_fragment),
            WaitEdgeKind::DataFlow,
            &mut succ,
        );
    }

    let mut accepted_build_ready = BTreeSet::new();
    let mut fallbacks = BTreeMap::new();
    let mut wait_sources = Vec::with_capacity(consumers.len());
    for consumer in consumers {
        let mut sources = Vec::with_capacity(consumer.producers.len());
        for producer in &consumer.producers {
            let wait = WaitProvenance {
                channel: consumer.channel,
                consumer_binding: consumer.binding,
                producer_binding: producer.binding,
            };
            let source = if consumer.behavior == ConsumerWaitBehavior::BlocksUntilComplete {
                match join_progress.get(&(consumer.channel, producer.binding, producer.fragment)) {
                    Some(proof) => match revalidate_proof(
                        proof,
                        edges,
                        &deps,
                        consumer.consumer_fragment,
                        consumer.channel,
                        producer,
                    ) {
                        Ok(()) => {
                            let build_ready = WaitNode::BuildReady {
                                fragment: proof.producer_fragment,
                                join_node: proof.join_node_id,
                            };
                            if accepted_build_ready
                                .insert((proof.producer_fragment, proof.join_node_id))
                            {
                                for frontier in &proof.build_frontier {
                                    add_wait_edge(
                                        WaitNode::Frag(frontier.source_fragment),
                                        build_ready,
                                        WaitEdgeKind::Frontier,
                                        &mut succ,
                                    );
                                }
                                add_wait_edge(
                                    build_ready,
                                    WaitNode::Frag(proof.producer_fragment),
                                    WaitEdgeKind::Frontier,
                                    &mut succ,
                                );
                            }
                            build_ready
                        }
                        Err(reason) => {
                            fallbacks.insert(wait, ProofFallback::Rejected(reason));
                            WaitNode::Frag(producer.fragment)
                        }
                    },
                    None => {
                        if let Some(skip) = join_progress.skipped(&(
                            consumer.channel,
                            producer.binding,
                            producer.fragment,
                        )) {
                            fallbacks.insert(wait, ProofFallback::Skipped(*skip));
                        }
                        WaitNode::Frag(producer.fragment)
                    }
                }
            } else {
                WaitNode::Frag(producer.fragment)
            };
            sources.push(source);
        }
        wait_sources.push(sources);
    }

    for (consumer, sources) in consumers.iter().zip(&wait_sources) {
        if consumer.behavior != ConsumerWaitBehavior::BlocksUntilComplete {
            continue;
        }
        ensure_wait_node(WaitNode::Frag(consumer.consumer_fragment), &mut succ);
        for (producer, source) in consumer.producers.iter().zip(sources) {
            add_wait_edge(
                *source,
                WaitNode::Frag(consumer.consumer_fragment),
                WaitEdgeKind::Wait(WaitProvenance {
                    channel: consumer.channel,
                    consumer_binding: consumer.binding,
                    producer_binding: producer.binding,
                }),
                &mut succ,
            );
        }
    }

    let mut out_edges: BTreeMap<u32, BTreeSet<(u32, i32)>> = BTreeMap::new();
    for edge in edges {
        out_edges
            .entry(edge.source_fragment)
            .or_default()
            .insert((edge.target_fragment, edge.target_exchange_node));
    }
    for (multicast_fragment, branches) in &out_edges {
        if branches.len() < 2 {
            continue;
        }
        for (target, _) in branches {
            for (consumer, sources) in consumers.iter().zip(&wait_sources) {
                if consumer.behavior != ConsumerWaitBehavior::BlocksUntilComplete {
                    continue;
                }
                let on_branch = consumer.consumer_fragment == *target
                    || deps.reaches(consumer.consumer_fragment, *target);
                if !on_branch {
                    continue;
                }
                for (producer, source) in consumer.producers.iter().zip(sources) {
                    add_wait_edge(
                        *source,
                        WaitNode::Frag(*multicast_fragment),
                        WaitEdgeKind::Backpressure {
                            wait: WaitProvenance {
                                channel: consumer.channel,
                                consumer_binding: consumer.binding,
                                producer_binding: producer.binding,
                            },
                            multicast_fragment: *multicast_fragment,
                        },
                        &mut succ,
                    );
                }
            }
        }
    }

    Ok(RefinedWaitGraph {
        succ,
        fallbacks,
        accepted_build_ready,
    })
}

impl RefinedWaitGraph {
    pub fn find_cycle(&self) -> Option<RefinedCycle> {
        match find_cycle_steps(&self.succ) {
            Some(steps) => Some(RefinedCycle {
                steps,
                fallbacks: self.fallbacks.clone(),
            }),
            None => {
                for (fragment, join_node) in &self.accepted_build_ready {
                    debug!(
                        "runtime-filter join progress proof accepted: fragment={fragment} join_node={join_node}"
                    );
                }
                None
            }
        }
    }

    pub fn pure_blocking_sccs(&self) -> Vec<PureBlockingScc> {
        let mut result = Vec::new();
        for nodes in strongly_connected_components(&self.succ) {
            let node_set: BTreeSet<WaitNode> = nodes.iter().copied().collect();
            let cyclic = nodes.len() > 1
                || nodes.first().is_some_and(|node| {
                    self.succ
                        .get(node)
                        .is_some_and(|edges| edges.iter().any(|(to, _)| to == node))
                });
            if !cyclic {
                continue;
            }

            let mut waits = BTreeSet::new();
            let mut all_waits_refined = true;
            for from in &nodes {
                for (to, kind) in self.succ.get(from).expect("SCC node tracked") {
                    if !node_set.contains(to) {
                        continue;
                    }
                    match kind {
                        WaitEdgeKind::Wait(wait) => {
                            waits.insert(*wait);
                            all_waits_refined &= matches!(from, WaitNode::BuildReady { .. });
                        }
                        WaitEdgeKind::Backpressure { wait, .. } => {
                            if matches!(from, WaitNode::BuildReady { .. }) {
                                waits.insert(*wait);
                            } else {
                                all_waits_refined = false;
                            }
                        }
                        WaitEdgeKind::DataFlow | WaitEdgeKind::Frontier => {}
                    }
                }
            }
            if waits.is_empty() || !all_waits_refined {
                continue;
            }

            let induced = induced_graph(&self.succ, &node_set);
            let witness = find_cycle_steps(&induced).expect("nontrivial SCC has a cycle");
            result.push(PureBlockingScc {
                waits: waits.into_iter().collect(),
                witness,
            });
        }
        result.sort_by(|left, right| {
            left.waits
                .cmp(&right.waits)
                .then_with(|| left.witness.cmp(&right.witness))
        });
        result
    }
}

impl RefinedCycle {
    pub fn primary_wait(&self) -> WaitProvenance {
        self.steps
            .iter()
            .filter_map(|step| match step.kind {
                WaitEdgeKind::Wait(wait) | WaitEdgeKind::Backpressure { wait, .. } => Some(wait),
                WaitEdgeKind::DataFlow | WaitEdgeKind::Frontier => None,
            })
            .min()
            .expect("a refined-graph cycle must cross a wait or backpressure edge")
    }

    pub fn render(&self) -> Vec<String> {
        self.steps
            .iter()
            .map(|step| render_cycle_step(step, &self.fallbacks))
            .collect()
    }
}

fn induced_graph(succ: &WaitGraph, nodes: &BTreeSet<WaitNode>) -> WaitGraph {
    nodes
        .iter()
        .map(|node| {
            let edges = succ
                .get(node)
                .expect("induced node tracked")
                .iter()
                .filter(|(to, _)| nodes.contains(to))
                .copied()
                .collect();
            (*node, edges)
        })
        .collect()
}

fn strongly_connected_components(succ: &WaitGraph) -> Vec<Vec<WaitNode>> {
    fn visit(
        node: WaitNode,
        succ: &WaitGraph,
        visited: &mut BTreeSet<WaitNode>,
        finish: &mut Vec<WaitNode>,
    ) {
        if !visited.insert(node) {
            return;
        }
        for (next, _) in succ.get(&node).expect("node tracked") {
            visit(*next, succ, visited, finish);
        }
        finish.push(node);
    }

    let mut reverse: BTreeMap<WaitNode, BTreeSet<WaitNode>> =
        succ.keys().map(|node| (*node, BTreeSet::new())).collect();
    for (from, edges) in succ {
        for (to, _) in edges {
            reverse.entry(*to).or_default().insert(*from);
        }
    }

    fn collect(
        node: WaitNode,
        reverse: &BTreeMap<WaitNode, BTreeSet<WaitNode>>,
        visited: &mut BTreeSet<WaitNode>,
        component: &mut Vec<WaitNode>,
    ) {
        if !visited.insert(node) {
            return;
        }
        component.push(node);
        for predecessor in reverse.get(&node).expect("node tracked") {
            collect(*predecessor, reverse, visited, component);
        }
    }

    let mut visited = BTreeSet::new();
    let mut finish = Vec::new();
    for node in succ.keys() {
        visit(*node, succ, &mut visited, &mut finish);
    }
    visited.clear();
    let mut components = Vec::new();
    while let Some(node) = finish.pop() {
        if visited.contains(&node) {
            continue;
        }
        let mut component = Vec::new();
        collect(node, &reverse, &mut visited, &mut component);
        component.sort();
        components.push(component);
    }
    components.sort();
    components
}

fn find_cycle_steps(succ: &WaitGraph) -> Option<Vec<CycleStep>> {
    let mut in_degree: BTreeMap<WaitNode, usize> = succ.keys().map(|node| (*node, 0)).collect();
    let mut predecessors: BTreeMap<WaitNode, BTreeSet<(WaitNode, WaitEdgeKind)>> =
        succ.keys().map(|node| (*node, BTreeSet::new())).collect();
    for (from, edges) in succ {
        for (to, kind) in edges {
            *in_degree.get_mut(to).expect("node tracked") += 1;
            predecessors
                .get_mut(to)
                .expect("node tracked")
                .insert((*from, *kind));
        }
    }

    let mut queue: VecDeque<WaitNode> = in_degree
        .iter()
        .filter(|(_, degree)| **degree == 0)
        .map(|(node, _)| *node)
        .collect();
    let mut remaining = in_degree.len();
    while let Some(node) = queue.pop_front() {
        remaining -= 1;
        for (to, _) in succ.get(&node).expect("node tracked") {
            let degree = in_degree.get_mut(to).expect("node tracked");
            *degree -= 1;
            if *degree == 0 {
                queue.push_back(*to);
            }
        }
    }
    if remaining == 0 {
        return None;
    }

    let residual: BTreeSet<WaitNode> = in_degree
        .iter()
        .filter(|(_, degree)| **degree > 0)
        .map(|(node, _)| *node)
        .collect();
    let mut current = *residual.iter().next().expect("nonempty residual");
    let mut backward_path = Vec::new();
    let mut seen = BTreeMap::new();
    loop {
        if let Some(&cycle_start) = seen.get(&current) {
            let mut cycle = backward_path[cycle_start..].to_vec();
            cycle.reverse();
            return Some(cycle);
        }
        seen.insert(current, backward_path.len());
        let (from, kind) = predecessors
            .get(&current)
            .expect("residual node tracked")
            .iter()
            .find(|(from, _)| residual.contains(from))
            .copied()
            .expect("residual node keeps a residual predecessor");
        backward_path.push(CycleStep {
            from,
            kind,
            to: current,
        });
        current = from;
    }
}

fn render_wait_node(node: WaitNode) -> String {
    match node {
        WaitNode::Frag(fragment) => format!("frag {fragment}"),
        WaitNode::BuildReady {
            fragment,
            join_node,
        } => format!("build-ready(frag {fragment}, join {join_node})"),
    }
}

fn render_cycle_step(
    step: &CycleStep,
    fallbacks: &BTreeMap<WaitProvenance, ProofFallback>,
) -> String {
    let label = match step.kind {
        WaitEdgeKind::DataFlow => "dataflow".to_string(),
        WaitEdgeKind::Frontier => "frontier".to_string(),
        WaitEdgeKind::Wait(wait) => render_wait_provenance("wait", &wait, fallbacks),
        WaitEdgeKind::Backpressure {
            wait,
            multicast_fragment,
        } => render_wait_provenance(
            &format!("backpressure(frag {multicast_fragment})"),
            &wait,
            fallbacks,
        ),
    };
    format!(
        "{} --{label}--> {}",
        render_wait_node(step.from),
        render_wait_node(step.to)
    )
}

fn render_wait_provenance(
    edge: &str,
    wait: &WaitProvenance,
    fallbacks: &BTreeMap<WaitProvenance, ProofFallback>,
) -> String {
    let fallback = match fallbacks.get(wait) {
        Some(ProofFallback::Rejected(reason)) => format!(" (proof rejected: {reason:?})"),
        Some(ProofFallback::Skipped(skip)) => format!(
            " (proof skipped: join-node={} rule={:?})",
            skip.join_node_id, skip.rule
        ),
        None => String::new(),
    };
    format!(
        "{edge} channel={} consumer-binding={} producer-binding={}{fallback}",
        wait.channel.get(),
        wait.consumer_binding.get(),
        wait.producer_binding.get(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use arrow::datatypes::DataType;

    use super::{
        ConsumerWaitBehavior, ConsumerWaitInput, ProducerWaitInput, ProofRejection,
        RefinedFragmentEdge, RefinedWaitGraph, WaitEdgeKind, WaitGraph, WaitNode, WaitProvenance,
        add_wait_edge, build_refined_wait_graph,
    };
    use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
    use crate::sql::planner::runtime_filter::contract::{
        ArtifactCapability, BindingId, ChannelId, CompletionRequirement, ConsumerActivation,
        ContributionKind, CoverageWitnessId, NullSemantics, PlanFragmentId, PlanNodeId,
        ReductionRequirement, RuntimeFilterLifecycle, RuntimeFilterLogicalDomain,
        RuntimeFilterPolicyRequirement,
    };
    use crate::sql::planner::runtime_filter::coverage::Coverage;
    use crate::sql::planner::runtime_filter::graph::{
        ApplyPoint, ConsumerBindingTarget, ConsumerRequirementData, PlanLocation,
        ProducerBindingTarget, ProducerRequirement, RuntimeFilterBindingRoleData,
        RuntimeFilterBindingSpecData, RuntimeFilterChannelSpec, RuntimeFilterGraphData,
    };
    use crate::sql::planner::runtime_filter::progress::{
        FrontierEdge, FrontierSkip, JoinBuildProgressCatalog, JoinBuildProgressProof,
        JoinBuildProgressSkip,
    };

    fn edge(
        source_fragment: u32,
        target_fragment: u32,
        target_exchange_node: i32,
    ) -> RefinedFragmentEdge {
        RefinedFragmentEdge {
            source_fragment,
            target_fragment,
            target_exchange_node,
        }
    }

    fn proof() -> JoinBuildProgressProof {
        JoinBuildProgressProof {
            channel: ChannelId::new(7),
            producer_binding: BindingId::new(100),
            producer_fragment: 2,
            join_node_id: 10,
            build_frontier: vec![FrontierEdge {
                source_fragment: 4,
                target_exchange_node: 20,
            }],
            non_build_inputs: vec![FrontierEdge {
                source_fragment: 5,
                target_exchange_node: 23,
            }],
        }
    }

    fn catalog(proof: Option<JoinBuildProgressProof>) -> JoinBuildProgressCatalog {
        proof
            .into_iter()
            .map(|proof| {
                (
                    (
                        proof.channel,
                        proof.producer_binding,
                        proof.producer_fragment,
                    ),
                    proof,
                )
            })
            .collect()
    }

    fn producer(binding: u32, fragment: u32, node_id: i32) -> ProducerWaitInput {
        ProducerWaitInput {
            channel: ChannelId::new(7),
            binding: BindingId::new(binding),
            fragment,
            node_id,
            completion_requirement: CompletionRequirement::ProducerClosed,
        }
    }

    #[derive(Clone, Copy)]
    enum DraftActivation {
        BlockingOrBatchLive,
        LiveOnly,
    }

    fn wait_fixture<A>(activation: A) -> RuntimeFilterGraphData<A> {
        let mut graph = RuntimeFilterGraphData::default();
        graph
            .insert_channel(RuntimeFilterChannelSpec {
                channel_id: ChannelId::new(7),
                logical_domain: RuntimeFilterLogicalDomain::Membership {
                    value_type: DataType::Int64,
                    null_semantics: NullSemantics::NeverMatches,
                },
                lifecycle: RuntimeFilterLifecycle::CompleteOnce,
                availability_coverage: Coverage::Leaf(CoverageWitnessId::new(1)),
                terminal_coverage: Coverage::Leaf(CoverageWitnessId::new(1)),
                reduction_requirement: ReductionRequirement::SetUnion,
                allowed_contribution_kinds: BTreeSet::from([
                    ContributionKind::ValueDomainDelta,
                    ContributionKind::ProducerClosed,
                ]),
                required_consumer_capabilities: BTreeSet::from([
                    ArtifactCapability::Membership,
                    ArtifactCapability::EmptyDomain,
                ]),
                policy: RuntimeFilterPolicyRequirement {
                    max_contribution_bytes: 1024,
                    max_artifact_bytes: 4096,
                    deadline_ms: 30_000,
                    max_retries: 3,
                },
            })
            .unwrap();
        for (binding_id, fragment_id, node_id, role) in [(
            BindingId::new(100),
            2,
            10,
            RuntimeFilterBindingRoleData::Producer(ProducerRequirement {
                contribution_kinds: BTreeSet::from([
                    ContributionKind::ValueDomainDelta,
                    ContributionKind::ProducerClosed,
                ]),
                completion_requirement: CompletionRequirement::ProducerClosed,
                target: ProducerBindingTarget::JoinBuildKey { ordinal: 0 },
            }),
        )] {
            graph
                .insert_binding(RuntimeFilterBindingSpecData {
                    binding_id,
                    channel_id: ChannelId::new(7),
                    coverage_witness_id: Some(CoverageWitnessId::new(1)),
                    location: PlanLocation {
                        fragment_id: PlanFragmentId::new(fragment_id),
                        node_id: PlanNodeId::new(node_id),
                    },
                    expression: TypedExpr {
                        kind: ExprKind::Literal(LiteralValue::Int(1)),
                        data_type: DataType::Int64,
                        nullable: false,
                    },
                    apply_point: ApplyPoint::NodeOutput,
                    role,
                })
                .unwrap();
        }
        graph
            .insert_binding(RuntimeFilterBindingSpecData {
                binding_id: BindingId::new(10),
                channel_id: ChannelId::new(7),
                coverage_witness_id: None,
                location: PlanLocation {
                    fragment_id: PlanFragmentId::new(5),
                    node_id: PlanNodeId::new(24),
                },
                expression: TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(1)),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                apply_point: ApplyPoint::NodeInput,
                role: RuntimeFilterBindingRoleData::Consumer(ConsumerRequirementData {
                    capabilities: BTreeSet::from([
                        ArtifactCapability::Membership,
                        ArtifactCapability::EmptyDomain,
                    ]),
                    activation,
                    target: ConsumerBindingTarget::SourceBoundary,
                }),
            })
            .unwrap();
        graph
    }

    fn q23_edges() -> Vec<RefinedFragmentEdge> {
        vec![
            edge(1, 4, 21),
            edge(1, 3, 22),
            edge(4, 2, 20),
            edge(3, 5, 24),
            edge(5, 2, 23),
            edge(5, 4, 25),
        ]
    }

    fn q23_wait() -> ConsumerWaitInput {
        ConsumerWaitInput {
            channel: ChannelId::new(7),
            binding: BindingId::new(10),
            consumer_fragment: 5,
            behavior: ConsumerWaitBehavior::BlocksUntilComplete,
            producers: vec![producer(100, 2, 10)],
        }
    }

    fn unrelated_wait() -> ConsumerWaitInput {
        ConsumerWaitInput {
            channel: ChannelId::new(8),
            binding: BindingId::new(11),
            consumer_fragment: 9,
            behavior: ConsumerWaitBehavior::BlocksUntilComplete,
            producers: vec![ProducerWaitInput {
                channel: ChannelId::new(8),
                binding: BindingId::new(200),
                fragment: 8,
                node_id: 0,
                completion_requirement: CompletionRequirement::ProducerClosed,
            }],
        }
    }

    #[test]
    fn q23_shape_returns_one_pure_blocking_scc() {
        let proof = proof();
        let refined =
            build_refined_wait_graph(&q23_edges(), &[q23_wait()], &catalog(Some(proof.clone())))
                .unwrap();

        let sccs = refined.pure_blocking_sccs();
        assert_eq!(sccs.len(), 1);
        assert_eq!(sccs[0].waits.len(), 1);
        assert_eq!(sccs[0].waits[0].channel, ChannelId::new(7));
        assert!(refined.find_cycle().is_some());
    }

    #[test]
    fn generic_projection_keeps_draft_and_sealed_wait_results_equivalent() {
        let draft = wait_fixture(DraftActivation::BlockingOrBatchLive);
        let sealed = wait_fixture(ConsumerActivation::BlockingSnapshot);
        let draft_waits = super::project_consumer_waits(&draft, |activation| match activation {
            DraftActivation::BlockingOrBatchLive => ConsumerWaitBehavior::BlocksUntilComplete,
            DraftActivation::LiveOnly => ConsumerWaitBehavior::NeverBlocks,
        });
        let sealed_waits = super::project_consumer_waits(&sealed, |activation| match activation {
            ConsumerActivation::BlockingSnapshot => ConsumerWaitBehavior::BlocksUntilComplete,
            ConsumerActivation::NonBlockingLive { .. } => ConsumerWaitBehavior::NeverBlocks,
        });
        assert_eq!(draft_waits, sealed_waits);

        let proof = proof();
        let draft_refined =
            build_refined_wait_graph(&q23_edges(), &draft_waits, &catalog(Some(proof.clone())))
                .unwrap();
        let sealed_refined =
            build_refined_wait_graph(&q23_edges(), &sealed_waits, &catalog(Some(proof))).unwrap();
        assert_eq!(
            draft_refined.find_cycle().unwrap().render(),
            sealed_refined.find_cycle().unwrap().render()
        );
        assert_eq!(
            draft_refined.pure_blocking_sccs(),
            sealed_refined.pure_blocking_sccs()
        );

        let live_draft = wait_fixture(DraftActivation::LiveOnly);
        let live_sealed = wait_fixture(ConsumerActivation::NonBlockingLive {
            late_apply: crate::sql::planner::runtime_filter::contract::LateApplyGranularity::Batch,
        });
        for waits in [
            super::project_consumer_waits(&live_draft, |_| ConsumerWaitBehavior::NeverBlocks),
            super::project_consumer_waits(&live_sealed, |_| ConsumerWaitBehavior::NeverBlocks),
        ] {
            let refined = build_refined_wait_graph(
                &q23_edges(),
                &waits,
                &JoinBuildProgressCatalog::default(),
            )
            .unwrap();
            assert!(refined.find_cycle().is_none());
            assert!(refined.pure_blocking_sccs().is_empty());
        }
    }

    #[test]
    fn missing_proof_makes_blocking_scc_impure() {
        let refined = build_refined_wait_graph(
            &q23_edges(),
            &[q23_wait()],
            &JoinBuildProgressCatalog::default(),
        )
        .unwrap();

        assert!(refined.pure_blocking_sccs().is_empty());
        assert!(refined.find_cycle().is_some());
    }

    #[test]
    fn proof_backed_backpressure_with_external_wait_returns_pure_blocking_scc() {
        let proof = JoinBuildProgressProof {
            channel: ChannelId::new(7),
            producer_binding: BindingId::new(100),
            producer_fragment: 5,
            join_node_id: 24,
            build_frontier: vec![FrontierEdge {
                source_fragment: 6,
                target_exchange_node: 24,
            }],
            non_build_inputs: vec![FrontierEdge {
                source_fragment: 3,
                target_exchange_node: 25,
            }],
        };
        let edges = vec![
            edge(1, 6, 21),
            edge(1, 3, 22),
            edge(6, 5, 24),
            edge(3, 5, 25),
        ];
        let wait = ConsumerWaitInput {
            channel: ChannelId::new(7),
            binding: BindingId::new(10),
            consumer_fragment: 3,
            behavior: ConsumerWaitBehavior::BlocksUntilComplete,
            producers: vec![producer(100, 5, 24)],
        };
        let refined =
            build_refined_wait_graph(&edges, &[wait], &catalog(Some(proof.clone()))).unwrap();

        let sccs = refined.pure_blocking_sccs();
        assert_eq!(sccs.len(), 1);
        assert_eq!(sccs[0].waits.len(), 1);
        assert_eq!(sccs[0].waits[0].channel, ChannelId::new(7));
    }

    #[test]
    fn internal_backpressure_does_not_borrow_external_refined_wait_identity() {
        let wait = WaitProvenance {
            channel: ChannelId::new(7),
            consumer_binding: BindingId::new(10),
            producer_binding: BindingId::new(100),
        };
        let mut succ = WaitGraph::new();
        add_wait_edge(
            WaitNode::Frag(1),
            WaitNode::Frag(2),
            WaitEdgeKind::DataFlow,
            &mut succ,
        );
        add_wait_edge(
            WaitNode::Frag(2),
            WaitNode::Frag(1),
            WaitEdgeKind::Backpressure {
                wait,
                multicast_fragment: 1,
            },
            &mut succ,
        );
        add_wait_edge(
            WaitNode::Frag(2),
            WaitNode::Frag(3),
            WaitEdgeKind::Wait(wait),
            &mut succ,
        );
        add_wait_edge(
            WaitNode::BuildReady {
                fragment: 4,
                join_node: 40,
            },
            WaitNode::Frag(5),
            WaitEdgeKind::Wait(wait),
            &mut succ,
        );
        let refined = RefinedWaitGraph {
            succ,
            fallbacks: Default::default(),
            accepted_build_ready: Default::default(),
        };

        assert!(refined.find_cycle().is_some());
        assert!(refined.pure_blocking_sccs().is_empty());
    }

    #[test]
    fn every_proof_rejection_keeps_the_wait_coarse() {
        let canonical = proof();
        let payload_cases = [
            {
                let mut proof = canonical.clone();
                proof.channel = ChannelId::new(8);
                (proof, ProofRejection::PayloadChannelMismatch)
            },
            {
                let mut proof = canonical.clone();
                proof.producer_binding = BindingId::new(101);
                (proof, ProofRejection::PayloadProducerBindingMismatch)
            },
            {
                let mut proof = canonical.clone();
                proof.producer_fragment = 9;
                (proof, ProofRejection::PayloadProducerFragmentMismatch)
            },
            {
                let mut proof = canonical.clone();
                proof.join_node_id = 11;
                (proof, ProofRejection::JoinOwnerMismatch)
            },
            {
                let mut proof = canonical.clone();
                proof.build_frontier[0].target_exchange_node = 99;
                (proof, ProofRejection::PartitionMismatch)
            },
            {
                let mut proof = canonical.clone();
                proof.non_build_inputs = proof.build_frontier.clone();
                (proof, ProofRejection::OverlappingPartition)
            },
        ];

        let assert_coarse = |rejected: JoinBuildProgressProof,
                             wait: ConsumerWaitInput,
                             expected: ProofRejection| {
            let forged: JoinBuildProgressCatalog =
                [((ChannelId::new(7), BindingId::new(100), 2), rejected)]
                    .into_iter()
                    .collect();
            let refined = build_refined_wait_graph(&q23_edges(), &[wait], &forged).unwrap();
            assert!(
                refined.pure_blocking_sccs().is_empty(),
                "{expected:?} unexpectedly produced a pure SCC"
            );
            let expected = format!("proof rejected: {expected:?}");
            assert!(
                refined
                    .find_cycle()
                    .unwrap()
                    .render()
                    .iter()
                    .any(|step| { step.contains(&expected) })
            );
        };

        for (rejected, expected) in payload_cases {
            assert_coarse(rejected, q23_wait(), expected);
        }

        let mut identity_wait = q23_wait();
        identity_wait.producers[0].channel = ChannelId::new(8);
        assert_coarse(
            canonical.clone(),
            identity_wait,
            ProofRejection::GraphBindingIdentityMismatch,
        );

        let mut completion_wait = q23_wait();
        completion_wait.producers[0].completion_requirement =
            CompletionRequirement::FencedFinalDomain(
                crate::sql::planner::runtime_filter::contract::CompletionFenceKind::CommittedDomainFrozen,
            );
        assert_coarse(
            canonical.clone(),
            completion_wait,
            ProofRejection::CompletionMismatch,
        );

        let mut outside_probe = q23_wait();
        outside_probe.consumer_fragment = 4;
        assert_coarse(
            canonical,
            outside_probe,
            ProofRejection::ConsumerOutsideProbeRegion,
        );
    }

    #[test]
    fn skipped_proof_keeps_skip_provenance_on_coarse_wait() {
        let mut skipped = JoinBuildProgressCatalog::new();
        skipped.insert_skip(
            (ChannelId::new(7), BindingId::new(100), 2),
            JoinBuildProgressSkip {
                join_node_id: 10,
                rule: FrontierSkip::UnauditedNode { node_id: 44 },
            },
        );

        let refined = build_refined_wait_graph(&q23_edges(), &[q23_wait()], &skipped).unwrap();
        let cycle = refined.find_cycle().unwrap().render();
        assert!(cycle.iter().any(|step| {
            step.contains("proof skipped: join-node=10 rule=UnauditedNode { node_id: 44 }")
        }));
        assert!(refined.pure_blocking_sccs().is_empty());
    }

    #[test]
    fn acyclic_graph_has_no_cycle_or_blocking_scc() {
        let refined = build_refined_wait_graph(
            &[edge(2, 1, 20)],
            &[ConsumerWaitInput {
                channel: ChannelId::new(7),
                binding: BindingId::new(10),
                consumer_fragment: 1,
                behavior: ConsumerWaitBehavior::BlocksUntilComplete,
                producers: vec![producer(100, 2, 0)],
            }],
            &JoinBuildProgressCatalog::default(),
        )
        .unwrap();

        assert!(refined.find_cycle().is_none());
        assert!(refined.pure_blocking_sccs().is_empty());
    }

    #[test]
    fn reversed_inputs_produce_identical_cycle_and_scc_outputs() {
        let proof = proof();
        let mut reversed_edges = q23_edges();
        reversed_edges.reverse();
        let waits = vec![q23_wait(), unrelated_wait()];
        let mut reversed_waits = waits.clone();
        reversed_waits.reverse();

        let forward =
            build_refined_wait_graph(&q23_edges(), &waits, &catalog(Some(proof.clone()))).unwrap();
        let reversed =
            build_refined_wait_graph(&reversed_edges, &reversed_waits, &catalog(Some(proof)))
                .unwrap();

        assert_eq!(
            forward.find_cycle().unwrap().render(),
            reversed.find_cycle().unwrap().render()
        );
        assert_eq!(forward.pure_blocking_sccs(), reversed.pure_blocking_sccs());
    }
}
