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

use super::*;

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    Distribution, Edge, EdgeId, Fragment, FragmentId, FragmentSink, NodeId, NodeKind, PhysicalPlan,
    RowMultiplicity, ValueId, ValueOrigin,
};

#[cfg(test)]
#[path = "node_graph_tests.rs"]
mod node_graph_tests;

pub(crate) fn validate_fragment_graph(plan: &PhysicalPlan, errors: &mut ValidationContext) {
    let mut indegree = plan
        .fragments()
        .keys()
        .copied()
        .map(|fragment| (fragment, 0_usize))
        .collect::<BTreeMap<_, _>>();
    let mut successors: BTreeMap<FragmentId, BTreeMap<FragmentId, usize>> = BTreeMap::new();
    for edge in plan.edges().values() {
        if indegree.contains_key(&edge.source.fragment)
            && let Some(degree) = indegree.get_mut(&edge.destination.fragment)
        {
            *degree += 1;
            *successors
                .entry(edge.source.fragment)
                .or_default()
                .entry(edge.destination.fragment)
                .or_default() += 1;
        }
    }
    for fragment in plan.fragments().values() {
        if let crate::StagePlacementPolicy::FollowSources { sources } = fragment.stage_placement() {
            for source in sources {
                if !successors
                    .get(source)
                    .is_some_and(|targets| targets.contains_key(&fragment.id()))
                {
                    errors.push(ValidationError::new(
                        format!("fragments[{}].stage_placement", fragment.id().get()),
                        "placement source is not an actual predecessor",
                    ));
                }
            }
        }
    }
    for edge in plan.edges().values() {
        if matches!(edge.partitioning.destination, Distribution::Singleton)
            && plan
                .fragments()
                .get(&edge.destination.fragment)
                .is_some_and(|fragment| {
                    !matches!(
                        fragment.stage_placement(),
                        crate::StagePlacementPolicy::Singleton
                    )
                })
        {
            errors.push(ValidationError::new(
                "edges.placement",
                "gather destination must declare singleton placement",
            ));
        }
    }
    if let Some(result) = plan.result_port()
        && plan
            .fragments()
            .get(&result.fragment)
            .is_some_and(|fragment| {
                !matches!(
                    fragment.stage_placement(),
                    crate::StagePlacementPolicy::Singleton
                )
            })
    {
        errors.push(ValidationError::new(
            "result_port.placement",
            "execution anchor must declare singleton placement",
        ));
    }
    let mut ready = indegree
        .iter()
        .filter_map(|(fragment, degree)| (*degree == 0).then_some(*fragment))
        .collect::<Vec<_>>();
    let mut visited = 0_usize;
    while let Some(fragment) = ready.pop() {
        visited += 1;
        if let Some(next) = successors.get(&fragment) {
            for (successor, edges) in next {
                if let Some(degree) = indegree.get_mut(successor) {
                    *degree -= edges;
                    if *degree == 0 {
                        ready.push(*successor);
                    }
                }
            }
        }
    }
    if visited != plan.fragments().len() {
        errors.push(ValidationError::new(
            "edges",
            "fragment exchange graph contains a cycle",
        ));
    }
}

pub(crate) fn validate_fragment_sink(fragment: &Fragment, errors: &mut ValidationContext) {
    let path = format!("fragments[{}].sink", fragment.id().get());
    if let FragmentSink::RootResult(contract) = fragment.sink()
        && let Err(error) = contract.validate_purpose()
    {
        errors.push(ValidationError::new(&path, error.to_string()));
    }
    let root_output = fragment
        .nodes()
        .get(&fragment.root())
        .map(|root| root.output.columns.as_ref())
        .unwrap_or_default();
    let root_values = root_output.iter().copied().collect::<BTreeSet<_>>();
    let root_multiplicity = fragment
        .nodes()
        .get(&fragment.root())
        .map(|root| root.output_properties.row_multiplicity);
    validate_fragment_summary_outputs(fragment, errors);
    let mut edge_ids = fragment
        .summaries()
        .iter()
        .map(|summary| summary.edge)
        .collect::<BTreeSet<_>>();
    match fragment.sink() {
        FragmentSink::Multicast { edges } => {
            if edges.is_empty() {
                errors.push(ValidationError::new(
                    &path,
                    "multi-destination sink has no edges",
                ));
            }
            for edge in edges {
                if !edge_ids.insert(*edge) {
                    errors.push(ValidationError::new(
                        &path,
                        "sink contains a duplicate edge destination",
                    ));
                }
            }
        }
        FragmentSink::Router { effect, routes } => {
            if root_multiplicity != Some(RowMultiplicity::SingleCopy) {
                errors.push(ValidationError::new(
                    &path,
                    "router requires single-copy row ownership",
                ));
            }
            require_value(fragment, *effect, &path, errors);
            if !root_values.contains(effect) {
                errors.push(ValidationError::new(
                    &path,
                    "router effect is absent from the fragment root output",
                ));
            }
            let change_events = change_event_source(fragment, *effect);
            if change_events.is_none() {
                errors.push(ValidationError::new(
                    &path,
                    "router effect is not the exact output of a change-event expansion",
                ));
            }
            if routes.is_empty() {
                errors.push(ValidationError::new(
                    &path,
                    "multi-destination sink has no edges",
                ));
            }
            let mut route_ids = BTreeSet::new();
            let mut previous_target = None::<u32>;
            for route in routes {
                let target = route.write_target_ordinal.get();
                let dense = previous_target.map_or(target == 0, |previous| {
                    target == previous || previous.checked_add(1) == Some(target)
                });
                if route.route_id == crate::ConnectorWriteRouteId::from_bytes([0; 32])
                    || !route_ids.insert(route.route_id)
                    || !dense
                {
                    errors.push(ValidationError::new(
                        &path,
                        "router routes require unique identities and dense target ordinals in route order",
                    ));
                }
                previous_target = Some(target);
                let mut effects = BTreeSet::new();
                if route.accepted_effects.is_empty()
                    || route
                        .accepted_effects
                        .iter()
                        .any(|effect| !effects.insert(*effect))
                {
                    errors.push(ValidationError::new(
                        &path,
                        "router route requires unique accepted effects",
                    ));
                }
                let mut tokens = BTreeSet::new();
                let mut input_values = BTreeSet::new();
                if route.input_mapping.is_empty()
                    || route.input_mapping.iter().any(|(token, value)| {
                        !tokens.insert(*token) || !root_values.contains(value)
                    })
                {
                    errors.push(ValidationError::new(
                        &path,
                        "router route requires unique input tokens and root-output values",
                    ));
                }
                input_values.extend(route.input_mapping.iter().map(|(_, value)| *value));
                if route.input_mapping.iter().any(|(_, value)| value == effect) {
                    errors.push(ValidationError::new(
                        &path,
                        "router data input cannot contain its generated effect value",
                    ));
                }
                if matches!(&route.requirements.colocation, crate::WriteColocation::ByKey(keys) if keys.is_empty())
                    || matches!(&route.requirements.layout, crate::WriteLayoutPreference::ClusterBy(keys) if keys.is_empty())
                    || route
                        .requirements
                        .colocation_keys()
                        .iter()
                        .chain(route.requirements.layout_keys())
                        .any(|value| !input_values.contains(value))
                {
                    errors.push(ValidationError::new(
                        &path,
                        "router requirements must name nonempty exact route inputs",
                    ));
                }
                if !edge_ids.insert(route.edge) {
                    errors.push(ValidationError::new(
                        &path,
                        "sink contains a duplicate edge destination",
                    ));
                }
            }
            let covered_effects = routes
                .iter()
                .flat_map(|route| route.accepted_effects.iter().copied())
                .collect::<BTreeSet<_>>();
            if change_events.is_some_and(|events| {
                events
                    .iter()
                    .any(|event| !covered_effects.contains(&event.effect))
            }) {
                errors.push(ValidationError::new(
                    &path,
                    "router routes do not cover every emitted change-event effect",
                ));
            }
        }
        FragmentSink::Result | FragmentSink::RootResult(_) => {
            if root_multiplicity != Some(RowMultiplicity::SingleCopy) {
                errors.push(ValidationError::new(
                    &path,
                    "result sink requires single-copy row ownership",
                ));
            }
        }
        FragmentSink::Stream { .. } | FragmentSink::Noop => {}
    }
}

pub(crate) fn import_origin_matches(
    origin: &ValueOrigin,
    edge: EdgeId,
    kind: crate::EdgeKind,
    source_fragment: FragmentId,
    source_value: ValueId,
) -> bool {
    match (kind, origin) {
        (
            crate::EdgeKind::CteMulticast,
            ValueOrigin::CteImport {
                edge: value_edge,
                producer_fragment,
                producer_value,
            },
        ) => {
            *value_edge == edge
                && *producer_fragment == source_fragment
                && *producer_value == source_value
        }
        (
            crate::EdgeKind::Stream | crate::EdgeKind::ChangeStreamRouter,
            ValueOrigin::ExchangeImport {
                edge: value_edge,
                source_value: value_source,
            },
        ) => *value_edge == edge && *value_source == source_value,
        _ => false,
    }
}

/// One original Kahn author serves structural validation and observed property
/// consumers. Allocation facts remain a contribution to the caller's original
/// counter, not an allocator or property-publication grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NodeGraphEvent {
    Step,
    Ready(NodeId),
}

use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, ControlOwnedResourceFacts, ControlResourceCounter,
    ControlResourceError, control_resource_add, control_resource_mul,
    owned_resources::vec::reserve_for_push_in,
};

#[cfg(test)]
#[path = "node_graph_borrowed_tests.rs"]
mod node_graph_borrowed_tests;

trait GraphPolicy<E> {
    fn headers(&mut self, nodes: usize) -> Result<(), E>;
    fn inputs(&mut self, nodes: usize, inputs: usize) -> Result<(), E>;
    fn reachability(&mut self, nodes: usize) -> Result<(), E>;
    fn reachable_inputs(&mut self, inputs: usize) -> Result<(), E>;
    fn before_library(&mut self) -> Result<(), E>;
    fn after_library(&mut self) -> Result<(), E>;
    fn reserve(&mut self, values: &mut Vec<NodeId>) -> Result<(), E>;
    fn pending(&mut self, root: NodeId) -> Result<Vec<NodeId>, E>;
    fn push(&mut self, values: &mut Vec<NodeId>, value: NodeId) -> Result<(), E>;
    fn extend(&mut self, values: &mut Vec<NodeId>, inputs: &[NodeId]) -> Result<(), E>;
    fn reachable_step(&mut self) -> Result<(), E>;
    fn observe(&mut self, event: NodeGraphEvent) -> Result<bool, E>;
}

struct PlainGraph<F>(F);
impl<E, F: FnMut(NodeGraphEvent) -> Result<bool, E>> GraphPolicy<E> for PlainGraph<F> {
    fn headers(&mut self, _: usize) -> Result<(), E> {
        Ok(())
    }
    fn inputs(&mut self, _: usize, _: usize) -> Result<(), E> {
        Ok(())
    }
    fn reachability(&mut self, _: usize) -> Result<(), E> {
        Ok(())
    }
    fn reachable_inputs(&mut self, _: usize) -> Result<(), E> {
        Ok(())
    }
    fn before_library(&mut self) -> Result<(), E> {
        Ok(())
    }
    fn after_library(&mut self) -> Result<(), E> {
        Ok(())
    }
    fn reserve(&mut self, _: &mut Vec<NodeId>) -> Result<(), E> {
        Ok(())
    }
    fn pending(&mut self, root: NodeId) -> Result<Vec<NodeId>, E> {
        Ok(vec![root])
    }
    fn push(&mut self, values: &mut Vec<NodeId>, value: NodeId) -> Result<(), E> {
        values.push(value);
        Ok(())
    }
    fn extend(&mut self, values: &mut Vec<NodeId>, inputs: &[NodeId]) -> Result<(), E> {
        values.extend(inputs.iter().copied());
        Ok(())
    }
    fn reachable_step(&mut self) -> Result<(), E> {
        Ok(())
    }
    fn observe(&mut self, event: NodeGraphEvent) -> Result<bool, E> {
        (self.0)(event)
    }
}

struct CallerGraph<'a, 'control, F> {
    counter: &'a mut ControlResourceCounter,
    admit: &'a mut dyn FnMut(&ControlOwnedResourceFacts) -> Result<(), CompileControlError>,
    work: &'a mut CompileCheckpoints<'control>,
    observe: F,
    raw_inputs: usize,
}
impl<E, F> GraphPolicy<E> for CallerGraph<'_, '_, F>
where
    E: From<ControlResourceError> + From<CompileControlError>,
    F: FnMut(NodeGraphEvent, &mut CompileCheckpoints<'_>) -> Result<bool, E>,
{
    fn headers(&mut self, nodes: usize) -> Result<(), E> {
        self.counter.tree::<NodeId, usize>(nodes).map_err(E::from)?;
        self.counter
            .tree::<NodeId, Vec<NodeId>>(nodes)
            .map_err(E::from)?;
        // Header traversal and final Kahn processing are bounded by actual N.
        self.counter
            .work(control_resource_mul(nodes, 4).map_err(E::from)?)
            .map_err(E::from)?;
        (self.admit)(&self.counter.facts()).map_err(E::from)
    }
    fn inputs(&mut self, nodes: usize, inputs: usize) -> Result<(), E> {
        self.raw_inputs = control_resource_add(self.raw_inputs, inputs).map_err(E::from)?;
        self.counter.tree::<NodeId, ()>(inputs).map_err(E::from)?;
        let lookup = ControlResourceCounter::lookup_work(nodes).map_err(E::from)?;
        // contains_key, dependency-entry and remaining-degree lookups, plus
        // original input/dependency events. Duplicates keep their raw work.
        let per_input = control_resource_add(control_resource_mul(lookup, 4).map_err(E::from)?, 4)
            .map_err(E::from)?;
        self.counter
            .work(control_resource_mul(inputs, per_input).map_err(E::from)?)
            .map_err(E::from)?;
        (self.admit)(&self.counter.facts()).map_err(E::from)
    }
    fn reachability(&mut self, nodes: usize) -> Result<(), E> {
        // The original insert precedes node lookup: unknown IDs count too.
        // Every visited ID comes from the root or an actual raw input.
        let upper = control_resource_add(self.raw_inputs, 1).map_err(E::from)?;
        self.counter.tree::<NodeId, ()>(upper).map_err(E::from)?;
        let lookup = ControlResourceCounter::lookup_work(upper.max(nodes)).map_err(E::from)?;
        self.counter
            .work(
                control_resource_mul(
                    upper,
                    control_resource_add(control_resource_mul(lookup, 2).map_err(E::from)?, 4)
                        .map_err(E::from)?,
                )
                .map_err(E::from)?,
            )
            .map_err(E::from)?;
        (self.admit)(&self.counter.facts()).map_err(E::from)
    }
    fn reachable_inputs(&mut self, inputs: usize) -> Result<(), E> {
        self.counter.work(inputs).map_err(E::from)?;
        (self.admit)(&self.counter.facts()).map_err(E::from)
    }
    fn before_library(&mut self) -> Result<(), E> {
        self.work.flush().map_err(E::from)
    }
    fn after_library(&mut self) -> Result<(), E> {
        self.work.step().map_err(E::from)?;
        self.work.flush().map_err(E::from)
    }
    fn reserve(&mut self, values: &mut Vec<NodeId>) -> Result<(), E> {
        let counter = &mut *self.counter;
        let admit = &mut *self.admit;
        reserve_for_push_in::<_, E>(
            values,
            &mut |facts| {
                if let Some(layout) = facts.requested_backing {
                    counter.layout(layout, 1).map_err(E::from)?;
                }
                admit(&counter.facts()).map_err(E::from)
            },
            self.work,
        )
    }
    fn pending(&mut self, root: NodeId) -> Result<Vec<NodeId>, E> {
        let mut values = Vec::new();
        self.push(&mut values, root)?;
        Ok(values)
    }
    fn push(&mut self, values: &mut Vec<NodeId>, value: NodeId) -> Result<(), E> {
        self.reserve(values)?;
        values.push(value);
        Ok(())
    }
    fn extend(&mut self, values: &mut Vec<NodeId>, inputs: &[NodeId]) -> Result<(), E> {
        for input in inputs {
            self.push(values, *input)?;
            self.work.step().map_err(E::from)?;
        }
        Ok(())
    }
    fn reachable_step(&mut self) -> Result<(), E> {
        self.work.step().map_err(E::from)
    }
    fn observe(&mut self, event: NodeGraphEvent) -> Result<bool, E> {
        (self.observe)(event, self.work)
    }
}

fn visit_node_graph_core<E>(
    fragment: &Fragment,
    policy: &mut impl GraphPolicy<E>,
) -> Result<Option<usize>, E> {
    policy.headers(fragment.nodes().len())?;
    let mut remaining_inputs = BTreeMap::new();
    let mut dependents: BTreeMap<NodeId, Vec<NodeId>> = BTreeMap::new();
    let mut ready = Vec::new();
    for (id, node) in fragment.nodes() {
        policy.inputs(fragment.nodes().len(), node.inputs.len())?;
        let mut inputs = BTreeSet::new();
        for input in &node.inputs {
            policy.before_library()?;
            if fragment.nodes().contains_key(input) {
                inputs.insert(*input);
            }
            policy.after_library()?;
            if !policy.observe(NodeGraphEvent::Step)? {
                return Ok(None);
            }
        }
        if inputs.is_empty() {
            policy.reserve(&mut ready)?;
        }
        policy.before_library()?;
        remaining_inputs.insert(*id, inputs.len());
        // The header's potential ready growth is admitted before its completed
        // library observation; Plain retains the original infallible push.
        if inputs.is_empty() {
            policy.push(&mut ready, *id)?;
        }
        policy.after_library()?;
        if !policy.observe(NodeGraphEvent::Step)? {
            return Ok(None);
        }
        for input in inputs {
            policy.before_library()?;
            policy.push(dependents.entry(input).or_default(), *id)?;
            policy.after_library()?;
            if !policy.observe(NodeGraphEvent::Step)? {
                return Ok(None);
            }
        }
    }
    let mut processed = 0_usize;
    while let Some(id) = ready.pop() {
        processed += 1;
        if !policy.observe(NodeGraphEvent::Ready(id))? {
            return Ok(None);
        }
        policy.before_library()?;
        if let Some(users) = dependents.get(&id) {
            // No captured heap-producing value exists at this lookup.
            policy.after_library()?;
            for user in users {
                policy.before_library()?;
                if let Some(remaining) = remaining_inputs.get_mut(user) {
                    *remaining -= 1;
                    if *remaining == 0 {
                        policy.push(&mut ready, *user)?;
                    }
                }
                policy.after_library()?;
                if !policy.observe(NodeGraphEvent::Step)? {
                    return Ok(None);
                }
            }
        } else {
            policy.after_library()?;
        }
    }
    Ok(Some(processed))
}

/// `None` is an explicit caller stop, not successful graph completion. Missing
/// references keep the original structural author's treatment; its node-owner
/// validator independently rejects them before frozen property consumption.
pub(crate) fn visit_node_graph_child_first<E>(
    fragment: &Fragment,
    observe: impl FnMut(NodeGraphEvent) -> Result<bool, E>,
) -> Result<Option<usize>, E> {
    visit_node_graph_core(fragment, &mut PlainGraph(observe))
}

pub(crate) fn visit_node_graph_child_first_in<E>(
    fragment: &Fragment,
    counter: &mut ControlResourceCounter,
    admit: &mut dyn FnMut(&ControlOwnedResourceFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
    observe: impl FnMut(NodeGraphEvent, &mut CompileCheckpoints<'_>) -> Result<bool, E>,
) -> Result<Option<usize>, E>
where
    E: From<ControlResourceError> + From<CompileControlError>,
{
    visit_node_graph_core(
        fragment,
        &mut CallerGraph {
            counter,
            admit,
            work,
            observe,
            raw_inputs: 0,
        },
    )
}

fn validate_node_graph_core<E>(
    fragment: &Fragment,
    errors: &mut ValidationContext,
    policy: &mut impl GraphPolicy<E>,
) -> Result<(), E> {
    let path = format!("fragments[{}].nodes", fragment.id().get());
    let completed = visit_node_graph_core(fragment, policy)?;
    if completed != Some(fragment.nodes().len()) {
        errors.push(ValidationError::new(&path, "node graph contains a cycle"));
    }
    policy.reachability(fragment.nodes().len())?;
    let mut visited = BTreeSet::new();
    let mut pending = policy.pending(fragment.root())?;
    while let Some(id) = pending.pop() {
        policy.before_library()?;
        let new = visited.insert(id);
        policy.after_library()?;
        if !new {
            continue;
        }
        policy.before_library()?;
        if let Some(node) = fragment.nodes().get(&id) {
            policy.reachable_inputs(node.inputs.len())?;
            // Capture and admit each actual mutable pending growth before the
            // completed lookup observation; raw duplicates/order are retained.
            policy.extend(&mut pending, &node.inputs)?;
        }
        policy.after_library()?;
        policy.reachable_step()?;
    }
    if visited.len() != fragment.nodes().len() {
        errors.push(ValidationError::new(
            &path,
            "fragment contains nodes unreachable from its root",
        ));
    }
    Ok(())
}

pub(crate) fn validate_node_graph(fragment: &Fragment, errors: &mut ValidationContext) {
    validate_node_graph_core(
        fragment,
        errors,
        &mut PlainGraph(|_| Ok::<_, std::convert::Infallible>(true)),
    )
    .unwrap_or_else(|never| match never {});
}

pub(crate) fn validate_node_graph_in(
    fragment: &Fragment,
    errors: &mut ValidationContext,
    counter: &mut ControlResourceCounter,
    admit: &mut dyn FnMut(&ControlOwnedResourceFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ControlResourceError> {
    validate_node_graph_core(
        fragment,
        errors,
        &mut CallerGraph {
            counter,
            admit,
            work,
            observe: |_, work: &mut CompileCheckpoints<'_>| {
                work.step()?;
                Ok(true)
            },
            raw_inputs: 0,
        },
    )
}

pub(crate) fn validate_edge(
    plan: &PhysicalPlan,
    edge: &Edge,
    root_port_indexes: &mut BTreeMap<FragmentId, ValuePortIndex>,
    errors: &mut ValidationContext,
) {
    let path = format!("edges[{}]", edge.id.get());
    let Some(source) = plan.fragments().get(&edge.source.fragment) else {
        errors.push(ValidationError::new(
            &path,
            "source fragment is not defined",
        ));
        return;
    };
    let Some(destination) = plan.fragments().get(&edge.destination.fragment) else {
        errors.push(ValidationError::new(
            &path,
            "destination fragment is not defined",
        ));
        return;
    };
    if edge.source.fragment == edge.destination.fragment {
        errors.push(ValidationError::new(
            &path,
            "edge cannot connect a fragment to itself",
        ));
    }
    if edge.source.projection.len() != edge.destination.receive_mapping.len() {
        errors.push(ValidationError::new(
            &path,
            "source projection and receive mapping have different widths",
        ));
    }
    if let crate::FragmentOutputPort::Summary(kind) = edge.source.port {
        match source
            .summaries()
            .iter()
            .find(|summary| summary.kind() == kind)
        {
            Some(summary) => validate_summary_edge(source, summary, edge, errors),
            None => errors.push(ValidationError::new(
                &path,
                "summary source has no summary output",
            )),
        }
        if edge.kind != crate::EdgeKind::Stream
            || edge.partitioning.destination != Distribution::Singleton
            || edge.partitioning.source_multiplicity != RowMultiplicity::SingleCopy
            || edge.partitioning.destination_multiplicity != RowMultiplicity::SingleCopy
        {
            errors.push(ValidationError::new(
                &path,
                "summary requires a single-copy gather stream",
            ));
        }
    } else if let Some(root) = source.nodes().get(&source.root()) {
        let root_values = root_port_indexes
            .entry(source.id())
            .or_insert_with(|| ValuePortIndex::new(&root.output.columns));
        if edge
            .source
            .projection
            .iter()
            .any(|value| !root_values.contains(value))
        {
            errors.push(ValidationError::new(
                &path,
                "edge projects a value absent from the source fragment root output",
            ));
        }
        if root.output_properties.row_multiplicity != edge.partitioning.source_multiplicity {
            errors.push(ValidationError::new(
                &path,
                "edge source row multiplicity differs from the source fragment root",
            ));
        }
        let role_matches = match edge.role {
            crate::OutputRole::Summary(_) => false,
            crate::OutputRole::WriterResults => {
                matches!(root.kind, NodeKind::TableWriter { .. })
                    && edge.delivery == crate::DeliveryRequirement::MustDrain
            }
            crate::OutputRole::ProviderRoute { .. } => {
                matches!(source.sink(), FragmentSink::Router { .. })
                    && edge.delivery == crate::DeliveryRequirement::MustDrain
            }
            crate::OutputRole::PositionShards { group, target } => {
                let shard_root = matches!(&root.kind, NodeKind::PositionShard(spec)
                    if spec.group == group && spec.target == target
                        && spec.output.columns().as_slice() == edge.source.projection.as_ref());
                shard_root
                    && edge.kind == crate::EdgeKind::Stream
                    && edge.delivery == crate::DeliveryRequirement::MustDrain
                    && edge.partitioning.destination == Distribution::Singleton
            }
            crate::OutputRole::Relational => {
                !matches!(
                    root.kind,
                    NodeKind::TableWriter { .. } | NodeKind::PositionShard(_)
                ) && !matches!(source.sink(), FragmentSink::Router { .. })
            }
        };
        if !role_matches {
            errors.push(ValidationError::new(
                &path,
                "edge output role differs from its exact source",
            ));
        }
    }
    validate_distribution(
        source,
        &edge.partitioning.source,
        "edge.source_partitioning",
        errors,
    );
    validate_distribution(
        destination,
        &edge.partitioning.destination,
        "edge.destination_partitioning",
        errors,
    );
    for (ordinal, (projected, (mapped_source, imported))) in edge
        .source
        .projection
        .iter()
        .zip(edge.destination.receive_mapping.iter())
        .enumerate()
    {
        if projected != mapped_source {
            errors.push(ValidationError::new(
                &path,
                format!("receive mapping source differs at ordinal {ordinal}"),
            ));
        }
        match (
            source.values().get(projected),
            destination.values().get(imported),
        ) {
            (Some(source_value), Some(destination_value)) => {
                // The receiving side may declare a column that admits null
                // the sender never writes -- it is declared by the statement's
                // column layout, not by the value that happens to fill it. It
                // may not declare the reverse.
                if !super::value_bindings_match_ignoring_root_nullability(
                    &source_value.ty,
                    &destination_value.ty,
                ) || (source_value.ty.nullable() && !destination_value.ty.nullable())
                {
                    errors.push(ValidationError::new(
                        &path,
                        format!(
                            "source and destination types differ at ordinal {ordinal}: source {:?} ({:?}), destination {:?} ({:?})",
                            source_value.ty, source_value.origin, destination_value.ty, destination_value.origin
                        ),
                    ));
                }
                if !import_origin_matches(
                    &destination_value.origin,
                    edge.id,
                    edge.kind,
                    edge.source.fragment,
                    *projected,
                ) {
                    errors.push(ValidationError::new(
                        &path,
                        format!("destination value origin differs at ordinal {ordinal}"),
                    ));
                }
            }
            _ => errors.push(ValidationError::new(
                &path,
                format!("mapping references an undefined value at ordinal {ordinal}"),
            )),
        }
    }
    validate_edge_partitioning(edge, &path, errors);
    match destination.nodes().get(&edge.destination.node) {
        Some(node)
            if matches!(
                &node.kind,
                NodeKind::ExchangeSource { edge: node_edge, imports }
                    if *node_edge == edge.id
                        && imports.as_ref() == edge.destination.receive_mapping.as_ref()
            ) =>
        {
            if node.output_properties.distribution != edge.partitioning.destination
                || node.output_properties.row_multiplicity
                    != edge.partitioning.destination_multiplicity
                || !node.output_properties.ordering.is_empty()
            {
                errors.push(ValidationError::new(
                    &path,
                    "exchange receiver properties differ from the edge destination contract",
                ));
            }
        }
        Some(_) => errors.push(ValidationError::new(
            &path,
            "destination node is not the exact exchange receiver",
        )),
        None => errors.push(ValidationError::new(
            &path,
            "destination node is not defined",
        )),
    }
}

pub(crate) fn validate_edge_partitioning(edge: &Edge, path: &str, errors: &mut ValidationContext) {
    validate_mapped_partitioning(
        &edge.partitioning,
        &edge.destination.receive_mapping,
        path,
        errors,
    );
}

pub(crate) fn validate_mapped_partitioning(
    partitioning: &crate::EdgePartitioning,
    mapping: &[(ValueId, ValueId)],
    path: &str,
    errors: &mut ValidationContext,
) {
    let layout_valid = match (&partitioning.source, &partitioning.destination) {
        (Distribution::Unconstrained, Distribution::Unconstrained)
        | (Distribution::Singleton, Distribution::Singleton)
        | (Distribution::RoundRobin, Distribution::RoundRobin)
        | (Distribution::Broadcast, Distribution::Broadcast) => true,
        (
            Distribution::Hash {
                keys: source_keys,
                scheme: source_scheme,
            },
            Distribution::Hash {
                keys: destination_keys,
                scheme: destination_scheme,
            },
        ) => {
            source_scheme == destination_scheme
                && mapped_partition_keys_match(source_keys, destination_keys, mapping)
        }
        (
            Distribution::ConditionalKey {
                applies: source_applies,
                keys: source_keys,
                scheme: source_scheme,
            },
            Distribution::ConditionalKey {
                applies: destination_applies,
                keys: destination_keys,
                scheme: destination_scheme,
            },
        ) => {
            source_scheme == destination_scheme
                && mapped_partition_keys_match(
                    std::slice::from_ref(source_applies),
                    std::slice::from_ref(destination_applies),
                    mapping,
                )
                && mapped_partition_keys_match(source_keys, destination_keys, mapping)
        }
        (
            Distribution::BucketShuffle {
                keys: source_keys,
                scheme: source_scheme,
            },
            Distribution::BucketShuffle {
                keys: destination_keys,
                scheme: destination_scheme,
            },
        ) => {
            source_scheme == destination_scheme
                && mapped_partition_keys_match(source_keys, destination_keys, mapping)
        }
        _ => false,
    };
    let multiplicity_valid = if partitioning.destination == Distribution::Broadcast {
        partitioning.source_multiplicity == RowMultiplicity::SingleCopy
            && partitioning.destination_multiplicity == RowMultiplicity::Replicated
    } else {
        partitioning.source_multiplicity == partitioning.destination_multiplicity
    };
    if !layout_valid || !multiplicity_valid {
        errors.push(ValidationError::new(
            path,
            "edge source and destination partitioning or row multiplicity are inconsistent",
        ));
    }
}

pub(crate) fn mapped_partition_keys_match(
    source_keys: &[ValueId],
    destination_keys: &[ValueId],
    mapping: &[(ValueId, ValueId)],
) -> bool {
    let mapping = ValueMappingIndex::from_pairs(mapping);
    source_keys.len() == destination_keys.len()
        && source_keys
            .iter()
            .zip(destination_keys)
            .all(|(source, destination)| mapping.contains(*source, *destination))
}

pub(crate) fn distribution_values(
    distribution: &Distribution,
) -> impl Iterator<Item = ValueId> + '_ {
    distribution.value_references()
}

pub(crate) fn validate_sinks(plan: &PhysicalPlan, errors: &mut ValidationContext) {
    let mut referenced = BTreeSet::new();
    let mut position_groups = BTreeSet::new();
    for fragment in plan.fragments().values() {
        let path = format!("fragments[{}].sink", fragment.id().get());
        if let Some(root) = fragment.nodes().get(&fragment.root())
            && let NodeKind::PositionShard(spec) = &root.kind
            && !position_groups.insert(spec.group)
        {
            errors.push(ValidationError::new(
                &path,
                "position contribution group has more than one aggregation stage",
            ));
        }
        for summary in fragment.summaries() {
            if !referenced.insert(summary.edge) {
                errors.push(ValidationError::new(
                    &path,
                    "edge is referenced by more than one output",
                ));
            }
            match plan.edges().get(&summary.edge) {
                Some(edge) => validate_summary_edge(fragment, summary, edge, errors),
                None => errors.push(ValidationError::new(&path, "summary edge is not defined")),
            }
        }
        let edges: &[EdgeId] = match fragment.sink() {
            FragmentSink::Stream { edge } => std::slice::from_ref(edge),
            FragmentSink::Multicast { edges } => edges,
            FragmentSink::Router { routes, .. } => {
                for route in routes {
                    if !referenced.insert(route.edge) {
                        errors.push(ValidationError::new(
                            &path,
                            "edge is referenced by more than one sink",
                        ));
                    }
                    match plan.edges().get(&route.edge) {
                        Some(edge)
                            if edge.source.fragment == fragment.id()
                                && edge.kind == crate::EdgeKind::ChangeStreamRouter =>
                        {
                            if edge.source.port != crate::FragmentOutputPort::Root
                                || edge.role
                                    != (crate::OutputRole::ProviderRoute {
                                        target: route.write_target_ordinal,
                                    })
                                || edge.delivery != crate::DeliveryRequirement::MustDrain
                            {
                                errors.push(ValidationError::new(
                                    &path,
                                    "provider route edge role or delivery differs",
                                ));
                            }
                            if !edge
                                .source
                                .projection
                                .iter()
                                .eq(route.input_mapping.iter().map(|(_, value)| value))
                            {
                                errors.push(ValidationError::new(
                                    &path,
                                    "router edge projection differs from its exact route input sequence",
                                ));
                            }
                            validate_router_writer_contract(plan, route, edge, &path, errors);
                            validate_router_partitioning(
                                route,
                                &edge.partitioning.source,
                                &path,
                                errors,
                            );
                        }
                        Some(_) => errors.push(ValidationError::new(
                            &path,
                            "sink edge belongs to another source fragment",
                        )),
                        None => {
                            errors.push(ValidationError::new(&path, "sink edge is not defined"))
                        }
                    }
                }
                &[]
            }
            FragmentSink::Result | FragmentSink::RootResult(_) | FragmentSink::Noop => &[],
        };
        for edge_id in edges {
            if !referenced.insert(*edge_id) {
                errors.push(ValidationError::new(
                    &path,
                    "edge is referenced by more than one sink",
                ));
            }
            match plan.edges().get(edge_id) {
                Some(edge)
                    if edge.source.fragment == fragment.id()
                        && edge_kind_matches_sink(fragment.sink(), edge.kind) => {}
                Some(_) => errors.push(ValidationError::new(
                    &path,
                    "sink edge belongs to another source fragment",
                )),
                None => errors.push(ValidationError::new(&path, "sink edge is not defined")),
            }
        }
    }
    for edge in plan.edges().values() {
        if !referenced.contains(&edge.id) {
            errors.push(ValidationError::new(
                format!("edges[{}]", edge.id.get()),
                "edge is not owned by its source fragment sink",
            ));
        }
    }
}

pub(crate) fn validate_router_writer_contract(
    plan: &PhysicalPlan,
    route: &crate::ChangeStreamRoute,
    edge: &Edge,
    path: &str,
    errors: &mut ValidationContext,
) {
    let Some(destination) = plan.fragments().get(&edge.destination.fragment) else {
        return;
    };
    let Some(writer) = destination.nodes().get(&destination.root()) else {
        return;
    };
    if let NodeKind::PositionShard(spec) = &writer.kind {
        let exact_route = spec.target == route.write_target_ordinal
            && route.accepted_effects.iter().all(|effect| {
                !matches!(
                    effect,
                    novarocks_connector_contract::ConnectorRowMutationEffect::Insert
                )
            })
            && writer.inputs.iter().zip(&spec.inputs).any(|(node, input)| {
                *node == edge.destination.node
                    && input.route == route.route_id
                    && input.columns().into_iter().eq(edge
                        .destination
                        .receive_mapping
                        .iter()
                        .map(|(_, imported)| *imported))
            });
        if !exact_route {
            errors.push(ValidationError::new(
                path,
                "router edge differs from its exact position contribution",
            ));
        }
        return;
    }
    let NodeKind::TableWriter { target } = &writer.kind else {
        errors.push(ValidationError::new(
            path,
            "router edge destination fragment root is not its exact table writer",
        ));
        return;
    };
    if writer.inputs.as_ref() != [edge.destination.node] {
        errors.push(ValidationError::new(
            path,
            "router edge receiver is not the direct table writer input",
        ));
    }
    if route.write_target_ordinal != target.write_target_ordinal {
        errors.push(ValidationError::new(
            path,
            "router write target ordinal differs from its destination table writer",
        ));
    }
    if target.required_distribution != edge.partitioning.destination {
        errors.push(ValidationError::new(
            path,
            "router edge destination distribution differs from its table writer requirement",
        ));
    }
    let fields_match = route.input_mapping.len() == edge.destination.receive_mapping.len()
        && route.input_mapping.len() == target.target_fields.len()
        && route
            .input_mapping
            .iter()
            .zip(edge.destination.receive_mapping.iter())
            .zip(target.target_fields.iter())
            .all(
                |(((route_token, route_value), (mapped_source, imported)), target_field)| {
                    route_value == mapped_source
                        && route_token == &target_field.token
                        && imported == &target_field.input
                },
            );
    if !fields_match {
        errors.push(ValidationError::new(
            path,
            "router field mapping differs from its destination table writer contract",
        ));
    }
}

pub(crate) fn validate_router_partitioning(
    route: &crate::ChangeStreamRoute,
    source_distribution: &Distribution,
    path: &str,
    errors: &mut ValidationContext,
) {
    let matches = match (&route.requirements.colocation, source_distribution) {
        (crate::WriteColocation::None, Distribution::RoundRobin)
        | (crate::WriteColocation::Singleton, Distribution::Singleton) => true,
        (crate::WriteColocation::ByKey(required), Distribution::Hash { keys, scheme }) => {
            !required.is_empty()
                && keys.as_ref() == required.as_ref()
                && scheme.definition.algorithm
                    == novarocks_type_contract::PartitionHashAlgorithm::NativeHierarchicalV1
        }
        _ => false,
    };
    if !matches {
        errors.push(ValidationError::new(
            path,
            "router partition values differ from its exact edge distribution",
        ));
    }
}

pub(crate) fn edge_kind_matches_sink(sink: &FragmentSink, kind: crate::EdgeKind) -> bool {
    match sink {
        FragmentSink::Stream { .. } => kind == crate::EdgeKind::Stream,
        FragmentSink::Multicast { .. } => kind == crate::EdgeKind::CteMulticast,
        FragmentSink::Router { .. } => kind == crate::EdgeKind::ChangeStreamRouter,
        FragmentSink::Result | FragmentSink::RootResult(_) | FragmentSink::Noop => false,
    }
}

pub(crate) fn validate_writer_flows(plan: &PhysicalPlan, errors: &mut ValidationContext) {
    let writers = plan
        .fragments()
        .values()
        .flat_map(|fragment| {
            fragment.nodes().values().filter_map(|node| {
                matches!(node.kind, NodeKind::TableWriter { .. })
                    .then_some((fragment.id(), node.id))
            })
        })
        .collect::<BTreeSet<_>>();
    let mut writer_finish_counts = BTreeMap::<(FragmentId, NodeId), usize>::new();

    for fragment in plan.fragments().values() {
        for finish_node in fragment
            .nodes()
            .values()
            .filter(|node| matches!(node.kind, NodeKind::TableFinish(_)))
        {
            let path = format!(
                "fragments[{}].nodes[{}].writer_flow",
                fragment.id().get(),
                finish_node.id.get()
            );
            if finish_node.id != fragment.root() {
                errors.push(ValidationError::new(
                    &path,
                    "table finish must be the root of its fragment",
                ));
            }
            let NodeKind::TableFinish(finish) = &finish_node.kind else {
                unreachable!();
            };
            let finish_values = finish
                .input_schema
                .fields
                .iter()
                .map(|field| field.value)
                .collect::<Box<[_]>>();
            let mut pending = finish_node
                .inputs
                .get(finish.inputs.writer_results as usize)
                .iter()
                .map(|node| **node)
                .map(|node| (fragment.id(), node, finish_values.clone()))
                .collect::<Vec<_>>();
            let mut visited = BTreeSet::new();
            let mut finish_writers = Vec::new();
            while let Some((fragment_id, node_id, expected_values)) = pending.pop() {
                if !visited.insert((fragment_id, node_id)) {
                    errors.push(ValidationError::new(
                        &path,
                        "writer relation reaches table finish through more than one path",
                    ));
                    continue;
                }
                let Some(flow_fragment) = plan.fragments().get(&fragment_id) else {
                    continue;
                };
                let Some(node) = flow_fragment.nodes().get(&node_id) else {
                    continue;
                };
                match &node.kind {
                    NodeKind::TableWriter { target } => {
                        finish_writers.push((fragment_id, node_id, target));
                        *writer_finish_counts
                            .entry((fragment_id, node_id))
                            .or_default() += 1;
                        if !writer_schema_matches_finish_values(
                            &target.output_schema,
                            &finish.input_schema,
                            &expected_values,
                        ) {
                            errors.push(ValidationError::new(
                                &path,
                                "table writer fields do not map exactly to its table finish input roles",
                            ));
                        }
                    }
                    NodeKind::ExchangeSource { edge, .. } => match plan.edges().get(edge) {
                        Some(edge_contract) if edge_contract.kind == crate::EdgeKind::Stream => {
                            let source_values = edge_contract
                                .destination
                                .receive_mapping
                                .iter()
                                .zip(&expected_values)
                                .map(|((source, destination), expected)| {
                                    (*destination == *expected).then_some(*source)
                                })
                                .collect::<Option<Box<[_]>>>();
                            if let Some(source) =
                                plan.fragments().get(&edge_contract.source.fragment)
                            {
                                let Some(source_root) = source.nodes().get(&source.root()) else {
                                    continue;
                                };
                                match (&source_root.kind, source_values) {
                                    (NodeKind::TableWriter { target }, Some(source_values)) => {
                                        finish_writers.push((source.id(), source_root.id, target));
                                        *writer_finish_counts
                                            .entry((source.id(), source_root.id))
                                            .or_default() += 1;
                                        if !writer_schema_matches_finish_values(
                                            &target.output_schema,
                                            &finish.input_schema,
                                            &source_values,
                                        ) {
                                            errors.push(ValidationError::new(
                                                &path,
                                                "streamed table writer fields do not map exactly to its table finish input roles",
                                            ));
                                        }
                                    }
                                    _ => errors.push(ValidationError::new(
                                        &path,
                                        "table finish stream source fields do not map exactly to its table finish input roles",
                                    )),
                                }
                            }
                        }
                        _ => errors.push(ValidationError::new(
                            &path,
                            "table finish writer relation uses a non-stream exchange",
                        )),
                    },
                    NodeKind::SetOp {
                        kind: crate::SetOperationKind::UnionAll,
                        input_mappings,
                    } => {
                        if node.output.columns.as_ref() != expected_values.as_ref()
                            || input_mappings.len() != node.inputs.len()
                            || input_mappings
                                .iter()
                                .any(|mapping| mapping.len() != expected_values.len())
                        {
                            errors.push(ValidationError::new(
                                &path,
                                "writer UnionAll does not preserve the exact finish field occurrences",
                            ));
                            continue;
                        }
                        pending.extend(
                            node.inputs
                                .iter()
                                .copied()
                                .zip(input_mappings.iter().cloned())
                                .map(|(input, mapping)| (fragment_id, input, mapping)),
                        );
                    }
                    _ => errors.push(ValidationError::new(
                        &path,
                        "table finish input contains a non-preserving writer relation node",
                    )),
                }
            }
            let actual_ordinals = finish_writers
                .iter()
                .map(|(_, _, target)| target.write_target_ordinal)
                .collect::<BTreeSet<_>>();
            if actual_ordinals.len() != finish_writers.len()
                || !actual_ordinals
                    .iter()
                    .copied()
                    .eq(finish.expected_target_ordinals.iter().copied())
            {
                errors.push(ValidationError::new(
                    &path,
                    "table finish expected targets differ from its exact upstream writers",
                ));
            }
            for (_, _, target) in finish_writers {
                if !writer_schema_shapes_match(&target.output_schema, &finish.input_schema) {
                    errors.push(ValidationError::new(
                        &path,
                        "table writer output schema differs from its table finish input schema",
                    ));
                }
            }
        }
    }

    for writer in writers {
        if writer_finish_counts.get(&writer).copied() != Some(1) {
            errors.push(ValidationError::new(
                format!("fragments[{}].nodes[{}]", writer.0.get(), writer.1.get()),
                "table writer must feed exactly one table finish",
            ));
        }
    }
}

pub(crate) fn validate_result(plan: &PhysicalPlan, errors: &mut ValidationContext) {
    let result_sinks = plan
        .fragments()
        .values()
        .filter(|fragment| {
            matches!(
                fragment.sink(),
                FragmentSink::Result | FragmentSink::RootResult(_)
            )
        })
        .collect::<Vec<_>>();
    match (plan.result_port(), result_sinks.as_slice()) {
        (None, []) => {}
        (None, _) => errors.push(ValidationError::new(
            "result_port",
            "result sink has no result port",
        )),
        (Some(_), []) => errors.push(ValidationError::new(
            "result_port",
            "result port has no result sink",
        )),
        (Some(_), [_, _, ..]) => errors.push(ValidationError::new(
            "result_port",
            "plan has more than one result sink",
        )),
        (Some(result), [fragment]) => validate_result_port_fields(fragment, result, errors),
    }
}

pub(crate) fn validate_result_port_fields(
    fragment: &Fragment,
    result: &crate::ResultPort,
    errors: &mut ValidationContext,
) {
    if result.fragment != fragment.id() {
        errors.push(ValidationError::new(
            "result_port.fragment",
            "result port belongs to another fragment",
        ));
    }
    if result.output.node != fragment.root() {
        errors.push(ValidationError::new(
            "result_port.output",
            "result output is not the result fragment root",
        ));
    }
    match fragment.nodes().get(&result.output.node) {
        Some(node) if node.output == result.output => {}
        Some(_) => errors.push(ValidationError::new(
            "result_port.output",
            "result output differs from the node output port",
        )),
        None => errors.push(ValidationError::new(
            "result_port.output",
            "result node is not defined",
        )),
    }
    if let FragmentSink::RootResult(contract) = fragment.sink()
        && let novarocks_result_contract::FrozenRootOutput::ClientRows(schema) = contract.output()
    {
        if schema.columns().len() != result.fields.len() {
            errors.push(ValidationError::new(
                "result_port.render_schema",
                "render schema width differs from ordered result occurrences",
            ));
        }
        for (ordinal, (column, field)) in schema.columns().iter().zip(&result.fields).enumerate() {
            let name = field.alias.as_deref().unwrap_or(&field.name);
            if column.source_ordinal as usize != ordinal
                || column.name != name
                || !novarocks_type_contract::result_render_type::render_field_matches_storage(
                    &column.field,
                    field.ty.data_type(),
                    field.ty.nullable(),
                )
            {
                errors.push(ValidationError::new(
                    "result_port.render_schema",
                    format!("render occurrence differs at ordinal {ordinal}"),
                ));
            }
        }
    }
    if let FragmentSink::RootResult(contract) = fragment.sink()
        && let novarocks_result_contract::FrozenRootOutput::ScalarValue(schema) = contract.output()
    {
        match result.fields.as_ref() {
            [field] if result.scalar_schema.as_ref().is_some_and(|frozen| frozen.field() == schema.field()) && field.domain.matches_scalar(&schema.field().value_type) && novarocks_type_contract::result_scalar_type::scalar_field_matches_binding(
                schema.field(), field.ty.typed_binding()
            ) => {}
            _ => errors.push(ValidationError::new(
                "result_port.scalar_schema", "scalar schema differs from the sole ordered root occurrence"
            )),
        }
    }
    if let Some(schema) = &result.scalar_schema {
        let consistent = match result.fields.as_ref() {
            [field] => {
                field.domain.matches_scalar(&schema.field().value_type)
                    && novarocks_type_contract::result_scalar_type::scalar_field_matches_binding(
                        schema.field(),
                        field.ty.typed_binding(),
                    )
            }
            _ => false,
        };
        if !consistent {
            errors.push(ValidationError::new(
                "result_port.scalar_schema",
                "compiler scalar identity differs from the final result carrier",
            ));
        }
    }
    if result.fields.len() != result.output.columns.len() {
        errors.push(ValidationError::new(
            "result_port.fields",
            "result schema width differs from output width",
        ));
    }
    for (ordinal, (field, value)) in result.fields.iter().zip(&result.output.columns).enumerate() {
        if field.value != *value {
            errors.push(ValidationError::new(
                "result_port.fields",
                format!("result value differs at ordinal {ordinal}"),
            ));
        }
        if !field.domain.matches_storage(field.ty.data_type()) {
            errors.push(ValidationError::new(
                "result_port.fields",
                format!("result logical domain differs from storage at ordinal {ordinal}"),
            ));
        }
        if field.name.is_empty() {
            errors.push(ValidationError::new(
                "result_port.fields",
                format!("result name is empty at ordinal {ordinal}"),
            ));
        }
        if let Some(definition) = fragment.values().get(value)
            && definition.ty != field.ty
        {
            errors.push(ValidationError::new(
                "result_port.fields",
                format!("result type differs at ordinal {ordinal}"),
            ));
        }
    }
}

pub(crate) fn validate_cross_fragment_value_origins(
    plan: &PhysicalPlan,
    errors: &mut ValidationContext,
) {
    let receive_mappings = plan
        .edges()
        .iter()
        .map(|(id, edge)| {
            (
                *id,
                edge.destination
                    .receive_mapping
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for fragment in plan.fragments().values() {
        for value in fragment.values().values() {
            let path = format!(
                "fragments[{}].values[{}]",
                fragment.id().get(),
                value.id.get()
            );
            match value.origin {
                ValueOrigin::ExchangeImport { edge, source_value } => match plan.edges().get(&edge)
                {
                    Some(edge_contract)
                        if edge_contract.destination.fragment == fragment.id()
                            && receive_mappings.get(&edge).is_some_and(|mapping| {
                                mapping.contains(&(source_value, value.id))
                            }) => {}
                    Some(_) => errors.push(ValidationError::new(
                        &path,
                        "exchange import is not present in the edge receive mapping",
                    )),
                    None => {
                        errors.push(ValidationError::new(&path, "exchange edge is not defined"))
                    }
                },
                ValueOrigin::CteImport {
                    edge,
                    producer_fragment,
                    producer_value,
                } => match plan.edges().get(&edge) {
                    Some(edge_contract)
                        if edge_contract.kind == crate::EdgeKind::CteMulticast
                            && edge_contract.source.fragment == producer_fragment
                            && edge_contract.destination.fragment == fragment.id()
                            && receive_mappings.get(&edge).is_some_and(|mapping| {
                                mapping.contains(&(producer_value, value.id))
                            }) => {}
                    Some(_) => errors.push(ValidationError::new(
                        &path,
                        "CTE import is not present in the exact CTE edge mapping",
                    )),
                    None => errors.push(ValidationError::new(&path, "CTE edge is not defined")),
                },
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod scalar_binding_tests {
    use super::*;
    use crate::{FragmentBuilder, PipelineDopDomain, ResultField, ResultPort, ResultValueDomain};
    use novarocks_result_contract::{ScalarField, ScalarSchema, ScalarValueType};
    use novarocks_type_contract::{
        FunctionValueType, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
        ValueTypeStorageLifetime as Hold,
    };
    use std::sync::Arc;

    fn list(json: bool) -> FunctionValueType {
        let item = ValueTypeRef::try_new(
            if json {
                ValueKind::Nominal(ValueLogicalType::Json)
            } else {
                ValueKind::String
            },
            true,
            Hold::pure(),
        )
        .unwrap();
        let root = ValueTypeRef::try_new(ValueKind::List(item), false, Hold::pure()).unwrap();
        let layout = arrow_schema::DataType::List(Arc::new(arrow_schema::Field::new(
            "item",
            arrow_schema::DataType::Utf8,
            true,
        )));
        FunctionValueType::try_from_value(
            root,
            ValueLayout::try_new(&layout, Hold::pure()).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn actual_result_port_rejects_nested_json_text_drift_on_the_same_value_layout() {
        let expected = list(true);
        for json in [false, true] {
            let actual = list(json);
            assert_eq!(expected.data_type(), actual.data_type());
            assert!(!expected.value_type().shares_storage(actual.value_type()));
            let id = FragmentId::new(0);
            let root = NodeId::new(1);
            let mut builder = FragmentBuilder::new(id);
            let value = builder
                .add_value(
                    actual.clone(),
                    ValueOrigin::NodeOutput {
                        node: root,
                        output_ordinal: 0,
                    },
                )
                .unwrap();
            builder
                .add_values(root, Box::default(), Box::from([value]))
                .unwrap();
            let fragment = builder
                .finish_definition(
                    root,
                    FragmentSink::Result,
                    PipelineDopDomain {
                        min: 1,
                        max: 1,
                        requires_power_of_two: false,
                    },
                    crate::StagePlacementPolicy::Singleton,
                )
                .unwrap();
            let result = ResultPort {
                fragment: id,
                output: fragment.nodes()[&root].output.clone(),
                fields: Box::from([ResultField {
                    domain: ResultValueDomain::Plain,
                    name: "nested".into(),
                    alias: None,
                    value,
                    ty: actual,
                }]),
                scalar_schema: Some(
                    ScalarSchema::try_new(ScalarField {
                        nullable: false,
                        value_type: ScalarValueType::List(Box::new(ScalarField {
                            nullable: true,
                            value_type: ScalarValueType::Json,
                        })),
                    })
                    .unwrap(),
                ),
            };
            let mut errors = ValidationContext::new();
            validate_result_port_fields(&fragment, &result, &mut errors);
            assert_eq!(
                errors.is_empty(),
                json,
                "actual result publication checks complete child semantics: {:?}",
                *errors
            );
        }
    }
}
