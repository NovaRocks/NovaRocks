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

//! Input-order liveness analysis over the final, carrier-neutral physical graph.
//!
//! Every operator has separate completion (C) and output-backpressure (B)
//! vertices. C follows all upstream inputs, while B propagates only across
//! pipelined inputs in the opposite direction. The sole ordinary bridge is
//! C(op) -> B(op): an operator cannot finish while its output remains blocked.
//! A gate adds B(input) -> gate -> C(prerequisite). This represents the same
//! intersection of completion ancestry and backpressure ancestry without
//! materializing their transitive closures for every gate.
//!
//! Set-op gates use a chain of prefix-completion vertices, so waiting for all
//! preceding inputs also needs only linear graph space. Absorption boundaries
//! do not erase an input's own admission gate. Shared fragment roots and local
//! node identities remain shared on every branch.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

use novarocks_type_contract::{CompileCheckpoints, CompileControlError};

use crate::{
    Edge, EdgeId, Fragment, FragmentId, FragmentOutputPort, FragmentSink, NodeId, NodeKind,
    PhysicalNode, RuntimeFilter, RuntimeFilterApplyPoint, RuntimeFilterConsumerActivation,
    RuntimeFilterConsumerTarget, RuntimeFilterId, RuntimeFilterProducerTarget, SetOperationKind,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct OperatorRef {
    pub fragment: FragmentId,
    pub node: NodeId,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct FilterConsumerRef {
    pub filter: RuntimeFilterId,
    pub ordinal: usize,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum WaitNode {
    Completion(OperatorRef),
    Backpressure(OperatorRef),
    SummaryCompletion(FragmentId),
    SummaryBackpressure(FragmentId),
    InputPrefix {
        operator: OperatorRef,
        ordinal: usize,
    },
    Input {
        operator: OperatorRef,
        ordinal: usize,
    },
    Filter(RuntimeFilterId),
    FilterConsumer(FilterConsumerRef),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum WaitEdgeKind {
    Backpressure,
    DataWait,
    FilterWait(FilterConsumerRef),
}

/// A closed directed path, including the repeated first vertex at the end.
/// The edge kinds retain the actual dependency which justified each step.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CycleWitness {
    pub nodes: Box<[WaitNode]>,
    pub edges: Box<[BTreeSet<WaitEdgeKind>]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WaitResolution {
    pub filter_downgrades: Box<[FilterConsumerRef]>,
    pub data_cycles: Box<[CycleWitness]>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ActiveWaitCycles {
    pub filter: bool,
    pub data: bool,
}

impl WaitResolution {
    pub fn is_acyclic(&self) -> bool {
        self.data_cycles.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InputConsumption {
    Pipelined,
    DrainedBeforeOutput,
}

/// Only actual absorption boundaries stop backpressure. New or uncertain
/// operator kinds conservatively propagate it until their input contract is
/// described here. This table does not grant an input admission permission.
pub(crate) fn input_consumption(kind: &NodeKind, ordinal: usize) -> InputConsumption {
    match kind {
        NodeKind::HashJoin { build_side, .. } if ordinal == build_side.input_ordinal() as usize => {
            InputConsumption::DrainedBeforeOutput
        }
        NodeKind::NestLoopJoin { .. } if ordinal == 1 => InputConsumption::DrainedBeforeOutput,
        // The installed aggregate route has no streaming preaggregation
        // (local-compiler/src/aggregate.rs), including partial grouping.
        NodeKind::Aggregate { .. } | NodeKind::Sort { .. } | NodeKind::TopN { .. } => {
            InputConsumption::DrainedBeforeOutput
        }
        NodeKind::SetOp {
            kind: SetOperationKind::Intersect | SetOperationKind::Except,
            ..
        } => InputConsumption::DrainedBeforeOutput,
        _ => InputConsumption::Pipelined,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WaitGraphError {
    Control(CompileControlError),
    MissingNode(OperatorRef),
    MissingEdge(EdgeId),
    MissingFragment(FragmentId),
    InvalidSummaryOutput(EdgeId),
    InvalidInput {
        operator: OperatorRef,
        ordinal: usize,
    },
    InvalidFilterProducer {
        filter: RuntimeFilterId,
        operator: OperatorRef,
    },
    UnresolvableFilterConsumer(FilterConsumerRef),
}

impl fmt::Display for WaitGraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(cause) => cause.fmt(f),
            Self::MissingNode(node) => write!(f, "wait graph refers to missing operator {node:?}"),
            Self::MissingEdge(edge) => write!(f, "wait graph refers to missing edge {edge:?}"),
            Self::MissingFragment(fragment) => {
                write!(f, "wait graph refers to missing fragment {fragment:?}")
            }
            Self::InvalidSummaryOutput(edge) => {
                write!(
                    f,
                    "wait graph edge {edge:?} has no matching logical summary output"
                )
            }
            Self::InvalidInput { operator, ordinal } => {
                write!(f, "wait graph operator {operator:?} has no input {ordinal}")
            }
            Self::InvalidFilterProducer { filter, operator } => {
                write!(
                    f,
                    "runtime filter {filter:?} has an invalid build endpoint {operator:?}"
                )
            }
            Self::UnresolvableFilterConsumer(consumer) => {
                write!(
                    f,
                    "runtime-filter wait cannot be downgraded for {consumer:?}"
                )
            }
        }
    }
}

impl std::error::Error for WaitGraphError {}

impl From<CompileControlError> for WaitGraphError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}

type Dependencies = BTreeMap<WaitNode, BTreeMap<WaitNode, BTreeSet<WaitEdgeKind>>>;

#[derive(Clone, Debug, Default)]
pub struct WaitGraph {
    dependencies: Dependencies,
    non_downgradable: BTreeSet<FilterConsumerRef>,
}

impl WaitGraph {
    #[cfg(test)]
    pub fn build(
        fragments: &BTreeMap<FragmentId, Fragment>,
        edges: &BTreeMap<EdgeId, Edge>,
        filters: &[RuntimeFilter],
    ) -> Result<Self, WaitGraphError> {
        let control = TestCompileControl;
        let mut work =
            CompileCheckpoints::try_new(&control, novarocks_type_contract::CompilePhase::Validate)?;
        let result = Self::build_observed(fragments, edges, filters, &mut work);
        work.finish_result(result, WaitGraphError::Control)
    }

    pub fn build_observed<'a>(
        fragments: &BTreeMap<FragmentId, Fragment>,
        edges: &BTreeMap<EdgeId, Edge>,
        filters: impl IntoIterator<Item = &'a RuntimeFilter>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, WaitGraphError> {
        work.flush()?;
        let plan = PlanView { fragments, edges };
        let mut graph = Self::default();
        for fragment in fragments.values() {
            work.step()?;
            if matches!(fragment.sink(), FragmentSink::Router { .. }) {
                graph.add_summary_output(OperatorRef {
                    fragment: fragment.id(),
                    node: fragment.root(),
                });
            }
            for node in fragment.nodes().values() {
                work.step()?;
                let operator = OperatorRef {
                    fragment: fragment.id(),
                    node: node.id,
                };
                graph.add_edge(
                    WaitNode::Completion(operator),
                    WaitNode::Backpressure(operator),
                    WaitEdgeKind::DataWait,
                );
                for ordinal in 0..node.inputs.len() {
                    work.step()?;
                    let input = plan.input(operator, ordinal)?;
                    plan.node(input)?;
                    graph.add_operator_input(
                        operator,
                        input,
                        input_consumption(&node.kind, ordinal) == InputConsumption::Pipelined,
                    );
                }
                if let Some((source, port)) = plan.exchange_input(node)? {
                    plan.node(source)?;
                    match port {
                        FragmentOutputPort::Root => {
                            graph.add_operator_input(operator, source, true)
                        }
                        FragmentOutputPort::LogicalSummary => {
                            graph.add_summary_input(operator, source.fragment);
                        }
                    }
                }
                match &node.kind {
                    NodeKind::HashJoin { build_side, .. } => {
                        graph.add_input_gate(
                            &plan,
                            operator,
                            build_side.opposite().input_ordinal() as usize,
                            build_side.input_ordinal() as usize,
                        )?;
                    }
                    NodeKind::NestLoopJoin { .. } => graph.add_input_gate(&plan, operator, 0, 1)?,
                    NodeKind::SetOp {
                        kind: SetOperationKind::Intersect | SetOperationKind::Except,
                        ..
                    } => graph.add_ordered_prefix(&plan, operator, node.inputs.len(), work)?,
                    _ => {}
                }
            }
        }
        for filter in filters {
            work.step()?;
            let filter_node = WaitNode::Filter(filter.id);
            graph.dependencies.entry(filter_node).or_default();
            for producer in &filter.producers {
                work.step()?;
                let operator = OperatorRef {
                    fragment: producer.endpoint.fragment,
                    node: producer.endpoint.node,
                };
                let node = plan.node(operator)?;
                let input = match (&producer.target, &node.kind) {
                    (
                        RuntimeFilterProducerTarget::JoinBuildKey { .. },
                        NodeKind::HashJoin { build_side, .. },
                    ) => build_side.input_ordinal() as usize,
                    (
                        RuntimeFilterProducerTarget::AggregateTopNKey { .. },
                        NodeKind::Aggregate { .. },
                    ) => 0,
                    _ => {
                        return Err(WaitGraphError::InvalidFilterProducer {
                            filter: filter.id,
                            operator,
                        });
                    }
                };
                let root = plan.input(operator, input)?;
                graph.add_edge(
                    filter_node,
                    WaitNode::Completion(root),
                    WaitEdgeKind::DataWait,
                );
            }
            for (ordinal, consumer) in filter.consumers.iter().enumerate() {
                work.step()?;
                if consumer.activation != RuntimeFilterConsumerActivation::BlockingSnapshot {
                    continue;
                }
                let consumer_ref = FilterConsumerRef {
                    filter: filter.id,
                    ordinal,
                };
                let gate = WaitNode::FilterConsumer(consumer_ref);
                let operator = OperatorRef {
                    fragment: consumer.endpoint.fragment,
                    node: consumer.endpoint.node,
                };
                let root = match consumer.apply_point {
                    RuntimeFilterApplyPoint::NodeInput { input_ordinal } => {
                        plan.input(operator, input_ordinal as usize)?
                    }
                    RuntimeFilterApplyPoint::NodeOutput | RuntimeFilterApplyPoint::ScanSource => {
                        plan.node(operator)?;
                        operator
                    }
                };
                graph.add_edge(
                    WaitNode::Backpressure(root),
                    gate,
                    WaitEdgeKind::Backpressure,
                );
                graph.add_edge(gate, filter_node, WaitEdgeKind::FilterWait(consumer_ref));
                if matches!(
                    consumer.target,
                    RuntimeFilterConsumerTarget::AggregateTopNScanField { .. }
                ) {
                    graph.non_downgradable.insert(consumer_ref);
                }
            }
        }
        work.flush()?;
        Ok(graph)
    }

    fn add_operator_input(&mut self, operator: OperatorRef, input: OperatorRef, pipelined: bool) {
        self.add_edge(
            WaitNode::Completion(operator),
            WaitNode::Completion(input),
            WaitEdgeKind::DataWait,
        );
        if pipelined {
            self.add_edge(
                WaitNode::Backpressure(input),
                WaitNode::Backpressure(operator),
                WaitEdgeKind::Backpressure,
            );
        }
    }

    fn add_summary_output(&mut self, root: OperatorRef) {
        // Summary is emitted by a separate pipeline after every actual
        // computing driver finishes. Its downstream backpressure never
        // stops those computing drivers or their provider routes.
        self.add_edge(
            WaitNode::SummaryCompletion(root.fragment),
            WaitNode::Completion(root),
            WaitEdgeKind::DataWait,
        );
        self.add_edge(
            WaitNode::SummaryCompletion(root.fragment),
            WaitNode::SummaryBackpressure(root.fragment),
            WaitEdgeKind::DataWait,
        );
    }

    fn add_summary_input(&mut self, operator: OperatorRef, source: FragmentId) {
        self.add_edge(
            WaitNode::Completion(operator),
            WaitNode::SummaryCompletion(source),
            WaitEdgeKind::DataWait,
        );
        self.add_edge(
            WaitNode::SummaryBackpressure(source),
            WaitNode::Backpressure(operator),
            WaitEdgeKind::Backpressure,
        );
    }

    fn add_input_gate(
        &mut self,
        plan: &PlanView<'_>,
        operator: OperatorRef,
        ordinal: usize,
        awaited: usize,
    ) -> Result<(), WaitGraphError> {
        let gate = WaitNode::Input { operator, ordinal };
        let input = plan.input(operator, ordinal)?;
        let prerequisite = plan.input(operator, awaited)?;
        self.add_edge(
            WaitNode::Backpressure(input),
            gate,
            WaitEdgeKind::Backpressure,
        );
        self.add_edge(
            gate,
            WaitNode::Completion(prerequisite),
            WaitEdgeKind::DataWait,
        );
        Ok(())
    }

    fn add_ordered_prefix(
        &mut self,
        plan: &PlanView<'_>,
        operator: OperatorRef,
        inputs: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), WaitGraphError> {
        let mut previous = None;
        for ordinal in 0..inputs {
            work.step()?;
            let root = plan.input(operator, ordinal)?;
            let prefix = WaitNode::InputPrefix { operator, ordinal };
            self.add_edge(prefix, WaitNode::Completion(root), WaitEdgeKind::DataWait);
            if let Some(previous) = previous {
                self.add_edge(prefix, previous, WaitEdgeKind::DataWait);
                let gate = WaitNode::Input { operator, ordinal };
                // The target input's absorption property does not suppress
                // its own stage-admission dependency.
                self.add_edge(
                    WaitNode::Backpressure(root),
                    gate,
                    WaitEdgeKind::Backpressure,
                );
                self.add_edge(gate, previous, WaitEdgeKind::DataWait);
            }
            previous = Some(prefix);
        }
        Ok(())
    }

    fn add_edge(&mut self, from: WaitNode, to: WaitNode, kind: WaitEdgeKind) {
        self.dependencies.entry(to).or_default();
        self.dependencies
            .entry(from)
            .or_default()
            .entry(to)
            .or_default()
            .insert(kind);
    }

    #[cfg(test)]
    pub fn resolve(&mut self) -> Result<WaitResolution, WaitGraphError> {
        let control = TestCompileControl;
        let mut work =
            CompileCheckpoints::try_new(&control, novarocks_type_contract::CompilePhase::Validate)?;
        let result = self.resolve_observed(&mut work);
        work.finish_result(result, WaitGraphError::Control)
    }

    /// Remove only optional RF waits occurring inside a cyclic component,
    /// then recompute the whole graph. Required data waits are never removed.
    pub fn resolve_observed(
        &mut self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<WaitResolution, WaitGraphError> {
        work.flush()?;
        let components = self.components(work)?;
        let mut component_by_node = BTreeMap::new();
        for (ordinal, component) in components.iter().enumerate() {
            for node in component {
                work.step()?;
                component_by_node.insert(*node, ordinal);
            }
        }
        let mut downgrades = BTreeSet::new();
        for (from, destinations) in &self.dependencies {
            for (to, kinds) in destinations {
                work.step()?;
                if component_by_node[from] == component_by_node[to] {
                    for kind in kinds {
                        if let WaitEdgeKind::FilterWait(consumer) = kind
                            && !self.non_downgradable.contains(consumer)
                        {
                            downgrades.insert(*consumer);
                        }
                    }
                }
            }
        }
        for destinations in self.dependencies.values_mut() {
            for (target, mut kinds) in std::mem::take(destinations) {
                work.step()?;
                kinds.retain(|kind| match kind {
                    WaitEdgeKind::FilterWait(consumer) => !downgrades.contains(consumer),
                    WaitEdgeKind::Backpressure | WaitEdgeKind::DataWait => true,
                });
                if !kinds.is_empty() {
                    destinations.insert(target, kinds);
                }
            }
        }
        let remaining = self.components(work)?;
        // A required RF wait can share a component with an optional wait which
        // breaks its cycle. Reject only after removing all optional waits.
        for component in &remaining {
            for node in component {
                for (target, kinds) in &self.dependencies[node] {
                    work.step()?;
                    if component.contains(target) {
                        for kind in kinds {
                            if let WaitEdgeKind::FilterWait(consumer) = kind {
                                return Err(WaitGraphError::UnresolvableFilterConsumer(*consumer));
                            }
                        }
                    }
                }
            }
        }
        let mut data_cycles = Vec::new();
        for component in remaining {
            if let Some(witness) = self.cycle_witness(&component, work)? {
                data_cycles.push(witness);
            }
        }
        work.flush()?;
        Ok(WaitResolution {
            filter_downgrades: downgrades
                .into_iter()
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            data_cycles: data_cycles.into_boxed_slice(),
        })
    }

    /// Inspect the actual frozen waits without applying planning policy.
    /// This uses the same SCC and witness author as optional RF resolution,
    /// but neither removes an edge nor changes any stored activation.
    pub(crate) fn active_cycles_observed(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ActiveWaitCycles, WaitGraphError> {
        work.flush()?;
        let mut cycles = ActiveWaitCycles::default();
        for component in self.components(work)? {
            if self.cycle_witness(&component, work)?.is_none() {
                continue;
            }
            let mut filter_wait = false;
            for from in &component {
                for (to, kinds) in &self.dependencies[from] {
                    work.step()?;
                    if component.contains(to)
                        && kinds
                            .iter()
                            .any(|kind| matches!(kind, WaitEdgeKind::FilterWait(_)))
                    {
                        filter_wait = true;
                    }
                }
            }
            if filter_wait {
                cycles.filter = true;
            } else {
                cycles.data = true;
            }
        }
        work.flush()?;
        Ok(cycles)
    }

    /// Iterative Kosaraju traversal keeps physical-plan depth off the Rust stack.
    fn components(
        &self,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<BTreeSet<WaitNode>>, WaitGraphError> {
        let mut reverse = BTreeMap::<WaitNode, BTreeSet<WaitNode>>::new();
        for (node, dependencies) in &self.dependencies {
            work.step()?;
            reverse.entry(*node).or_default();
            for dependency in dependencies.keys() {
                work.step()?;
                reverse.entry(*dependency).or_default().insert(*node);
            }
        }
        let mut visited = BTreeSet::new();
        let mut finish_order = Vec::with_capacity(reverse.len());
        for start in self.dependencies.keys() {
            work.step()?;
            if visited.contains(start) {
                continue;
            }
            let mut stack = vec![(*start, false)];
            while let Some((node, exiting)) = stack.pop() {
                work.step()?;
                if exiting {
                    finish_order.push(node);
                    continue;
                }
                if !visited.insert(node) {
                    continue;
                }
                stack.push((node, true));
                if let Some(next) = self.dependencies.get(&node) {
                    for dependency in next.keys().rev() {
                        work.step()?;
                        stack.push((*dependency, false));
                    }
                }
            }
        }
        let mut assigned = BTreeSet::new();
        let mut components = Vec::new();
        while let Some(start) = finish_order.pop() {
            work.step()?;
            if assigned.contains(&start) {
                continue;
            }
            let mut component = BTreeSet::new();
            let mut stack = vec![start];
            while let Some(node) = stack.pop() {
                work.step()?;
                if !assigned.insert(node) {
                    continue;
                }
                component.insert(node);
                if let Some(next) = reverse.get(&node) {
                    for dependency in next.iter().rev() {
                        work.step()?;
                        stack.push(*dependency);
                    }
                }
            }
            components.push(component);
        }
        Ok(components)
    }

    fn cycle_witness(
        &self,
        component: &BTreeSet<WaitNode>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<CycleWitness>, WaitGraphError> {
        work.step()?;
        let Some(start) = component.first().copied() else {
            return Ok(None);
        };
        let mut next = None;
        for node in self.dependencies[&start].keys() {
            work.step()?;
            if component.contains(node) {
                next = Some(*node);
                break;
            }
        }
        let Some(next) = next else {
            return Ok(None);
        };
        let mut previous = BTreeMap::new();
        let mut pending = VecDeque::from([next]);
        let mut visited = BTreeSet::from([next]);
        while let Some(node) = pending.pop_front() {
            work.step()?;
            if node == start {
                break;
            }
            for destination in self.dependencies[&node].keys() {
                work.step()?;
                if component.contains(destination) && visited.insert(*destination) {
                    previous.insert(*destination, node);
                    pending.push_back(*destination);
                }
            }
        }
        let mut path = vec![start];
        let mut current = start;
        while current != next {
            work.step()?;
            current = previous[&current];
            path.push(current);
        }
        path.reverse();
        path.insert(0, start);
        let mut edges = Vec::new();
        for pair in path.windows(2) {
            work.step()?;
            edges.push(self.dependencies[&pair[0]][&pair[1]].clone());
        }
        Ok(Some(CycleWitness {
            nodes: path.into_boxed_slice(),
            edges: edges.into_boxed_slice(),
        }))
    }
}

struct PlanView<'a> {
    fragments: &'a BTreeMap<FragmentId, Fragment>,
    edges: &'a BTreeMap<EdgeId, Edge>,
}

impl PlanView<'_> {
    fn node(&self, operator: OperatorRef) -> Result<&PhysicalNode, WaitGraphError> {
        self.fragments
            .get(&operator.fragment)
            .and_then(|fragment| fragment.nodes().get(&operator.node))
            .ok_or(WaitGraphError::MissingNode(operator))
    }

    fn input(&self, operator: OperatorRef, ordinal: usize) -> Result<OperatorRef, WaitGraphError> {
        let node = self.node(operator)?;
        let input = node
            .inputs
            .get(ordinal)
            .ok_or(WaitGraphError::InvalidInput { operator, ordinal })?;
        Ok(OperatorRef {
            fragment: operator.fragment,
            node: *input,
        })
    }

    fn exchange_input(
        &self,
        node: &PhysicalNode,
    ) -> Result<Option<(OperatorRef, FragmentOutputPort)>, WaitGraphError> {
        let NodeKind::ExchangeSource { edge, .. } = &node.kind else {
            return Ok(None);
        };
        let edge = self
            .edges
            .get(edge)
            .ok_or(WaitGraphError::MissingEdge(*edge))?;
        let source = self
            .fragments
            .get(&edge.source.fragment)
            .ok_or(WaitGraphError::MissingFragment(edge.source.fragment))?;
        if edge.source.port == FragmentOutputPort::LogicalSummary
            && !matches!(source.sink(), FragmentSink::Router { summary, .. } if summary.edge == edge.id)
        {
            return Err(WaitGraphError::InvalidSummaryOutput(edge.id));
        }
        Ok(Some((
            OperatorRef {
                fragment: source.id(),
                node: source.root(),
            },
            edge.source.port,
        )))
    }
}

#[cfg(test)]
struct TestCompileControl;
#[cfg(test)]
impl novarocks_type_contract::PureCompileControl for TestCompileControl {
    fn checkpoint(
        &self,
        _phase: novarocks_type_contract::CompilePhase,
        _work: u32,
    ) -> Result<(), CompileControlError> {
        Ok(())
    }
}

#[cfg(test)]
#[path = "wait_graph_tests.rs"]
mod tests;
