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

//! Original state emissions and ordered transport links, retained only by SQL.
//! These facts certify provenance, never cross-phase state compatibility.

use super::*;
use novarocks_physical_plan::{EdgeId, NodeId};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct AggregateStateEndpoint {
    pub fragment: FragmentId,
    pub node: NodeId,
    pub value: ValueId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AggregateStateTransport {
    Project,
    Stream(EdgeId),
    UnionAll,
    PartialTopN,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AggregateStateLink {
    pub input_ordinal: u32,
    pub mapping_ordinal: u32,
    pub source: AggregateStateEndpoint,
}

#[derive(Debug)]
enum StateOrigin {
    Emission(PhysicalCallSite),
    WriterNoContribution { auxiliary_ordinal: u32 },
    Transport(AggregateStateTransport, Box<[AggregateStateLink]>),
}
#[derive(Debug)]
struct StateSource {
    endpoint: AggregateStateEndpoint,
    origin: StateOrigin,
}

#[derive(Debug, Default)]
pub(crate) struct AggregateStateSources {
    // Dense indices reflect actual insertions, never a sparse physical ID.
    sources: Vec<StateSource>,
    index: BTreeMap<AggregateStateEndpoint, usize>,
    inputs: BTreeMap<(FragmentId, PhysicalCallSite), AggregateStateEndpoint>,
    links: usize,
}
impl AggregateStateSources {
    pub fn contains_observed(
        &self,
        endpoint: AggregateStateEndpoint,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<bool, SqlSourceJournalError> {
        work.flush()?;
        let present = self.index.contains_key(&endpoint);
        work.step()?;
        Ok(present)
    }
    fn insert_observed(
        &mut self,
        endpoint: AggregateStateEndpoint,
        origin: StateOrigin,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlSourceJournalError> {
        work.flush()?;
        let duplicate = self.index.contains_key(&endpoint);
        work.step()?;
        if duplicate {
            return Err(SqlSourceJournalError::InvalidSource(
                "aggregate state output is duplicated",
            ));
        }
        self.sources
            .len()
            .checked_add(1)
            .ok_or(CompileControlError::ResourceExhausted)?;
        self.sources
            .try_reserve(1)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        work.step()?;
        let index = self.sources.len();
        self.sources.push(StateSource { endpoint, origin });
        work.flush()?;
        // The original caller admits BTree storage and retained sources.
        self.index.insert(endpoint, index);
        work.step()?;
        Ok(())
    }
    pub fn emission_observed(
        &mut self,
        endpoint: AggregateStateEndpoint,
        site: PhysicalCallSite,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlSourceJournalError> {
        self.insert_observed(endpoint, StateOrigin::Emission(site), work)
    }
    /// The ordinal is the original full output-schema field position.
    pub fn writer_no_contribution_observed(
        &mut self,
        endpoint: AggregateStateEndpoint,
        auxiliary_ordinal: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlSourceJournalError> {
        self.insert_observed(
            endpoint,
            StateOrigin::WriterNoContribution { auxiliary_ordinal },
            work,
        )
    }
    pub fn transport_observed(
        &mut self,
        endpoint: AggregateStateEndpoint,
        kind: AggregateStateTransport,
        links: Vec<AggregateStateLink>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlSourceJournalError> {
        if links.is_empty() {
            return Ok(());
        }
        let next = self
            .links
            .checked_add(links.len())
            .ok_or(CompileControlError::ResourceExhausted)?;
        // Each source must already exist: the immutable provenance graph is a
        // backward-only DAG, not a cloned or recursively flattened snapshot.
        for link in &links {
            if !self.contains_observed(link.source, work)? {
                return Err(SqlSourceJournalError::InvalidSource(
                    "aggregate state transport has no original source",
                ));
            }
        }
        work.flush()?;
        let links = links.into_boxed_slice();
        work.step()?;
        self.insert_observed(endpoint, StateOrigin::Transport(kind, links), work)?;
        self.links = next;
        Ok(())
    }
    pub fn input_observed(
        &mut self,
        fragment: FragmentId,
        site: PhysicalCallSite,
        endpoint: AggregateStateEndpoint,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlSourceJournalError> {
        work.flush()?;
        let key = (fragment, site);
        let duplicate = self.inputs.contains_key(&key);
        work.step()?;
        if duplicate {
            return Err(SqlSourceJournalError::InvalidSource(
                "aggregate state input is duplicated",
            ));
        }
        work.flush()?;
        self.inputs.insert(key, endpoint);
        work.step()?;
        Ok(())
    }
}

/// A loan into the same original journal. Merge still uses its own logical
/// request; this graph exposes every original emission without choosing one.
#[derive(Clone, Copy)]
pub(crate) struct CheckedAggregateStateInputs<'a> {
    owner: &'a SqlAuthoredPhysicalPlan,
    root: AggregateStateEndpoint,
}
impl<'a> CheckedAggregateStateInputs<'a> {
    pub const fn root(&self) -> AggregateStateEndpoint {
        self.root
    }
    pub fn visit_observed(
        &self,
        work: &mut CompileCheckpoints<'_>,
        mut emission: impl FnMut(
            CheckedAggregateLogicalSourceEntry<'a>,
            AggregateStateEndpoint,
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), SqlSourceJournalError>,
        transport: impl FnMut(
            AggregateStateEndpoint,
            AggregateStateTransport,
            &[AggregateStateLink],
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), SqlSourceJournalError>,
    ) -> Result<(), SqlSourceJournalError> {
        visit_state_sources_observed(
            self.owner,
            self.root,
            work,
            |source, endpoint, work| match source {
                CheckedStateEmission::Aggregate(source) => emission(source, endpoint, work),
                CheckedStateEmission::Writer(_) => Err(SqlSourceJournalError::InvalidSource(
                    "ordinary aggregate state route contains a Writer emission",
                )),
            },
            transport,
            |_, _| {
                Err(SqlSourceJournalError::InvalidSource(
                    "ordinary aggregate state route contains a Writer non-contribution",
                ))
            },
        )
    }
}

/// A Writer merge borrows its own logical request and every actual input route.
#[derive(Clone, Copy)]
pub(crate) struct CheckedWriterAggregateStateInputs<'a> {
    owner: &'a SqlAuthoredPhysicalPlan,
    root: AggregateStateEndpoint,
}
impl<'a> CheckedWriterAggregateStateInputs<'a> {
    pub const fn root(&self) -> AggregateStateEndpoint {
        self.root
    }
    pub fn visit_observed(
        &self,
        work: &mut CompileCheckpoints<'_>,
        mut emission: impl FnMut(
            CheckedWriterAggregateLogicalSourceEntry<'a>,
            AggregateStateEndpoint,
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), SqlSourceJournalError>,
        transport: impl FnMut(
            AggregateStateEndpoint,
            AggregateStateTransport,
            &[AggregateStateLink],
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), SqlSourceJournalError>,
        no_contribution: impl FnMut(
            AggregateStateEndpoint,
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), SqlSourceJournalError>,
    ) -> Result<(), SqlSourceJournalError> {
        visit_state_sources_observed(
            self.owner,
            self.root,
            work,
            |source, endpoint, work| match source {
                CheckedStateEmission::Writer(source) => emission(source, endpoint, work),
                CheckedStateEmission::Aggregate(_) => Err(SqlSourceJournalError::InvalidSource(
                    "Writer aggregate state route contains an ordinary emission",
                )),
            },
            transport,
            no_contribution,
        )
    }
}

enum CheckedStateEmission<'a> {
    Aggregate(CheckedAggregateLogicalSourceEntry<'a>),
    Writer(CheckedWriterAggregateLogicalSourceEntry<'a>),
}

/// The sole walk visits each endpoint once while retaining ordered, repeated
/// links. Writer terminals add no calls or cross-phase compatibility proof.
fn visit_state_sources_observed<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    root: AggregateStateEndpoint,
    work: &mut CompileCheckpoints<'_>,
    emission: impl FnMut(
        CheckedStateEmission<'a>,
        AggregateStateEndpoint,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlSourceJournalError>,
    transport: impl FnMut(
        AggregateStateEndpoint,
        AggregateStateTransport,
        &[AggregateStateLink],
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlSourceJournalError>,
    no_contribution: impl FnMut(
        AggregateStateEndpoint,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlSourceJournalError>,
) -> Result<(), SqlSourceJournalError> {
    visit_state_graph_observed(
        &PublishedStateGraph(owner),
        root,
        work,
        emission,
        transport,
        no_contribution,
    )
}
fn visit_state_graph_observed<'a, G: StateGraph<'a>>(
    graph: &G,
    root: AggregateStateEndpoint,
    work: &mut CompileCheckpoints<'_>,
    mut emission: impl FnMut(
        G::Emission,
        AggregateStateEndpoint,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlSourceJournalError>,
    mut transport: impl FnMut(
        AggregateStateEndpoint,
        AggregateStateTransport,
        &[AggregateStateLink],
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlSourceJournalError>,
    mut no_contribution: impl FnMut(
        AggregateStateEndpoint,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), SqlSourceJournalError>,
) -> Result<(), SqlSourceJournalError> {
    let sources = &graph.journal().state_sources;
    if !sources.contains_observed(root, work)? {
        return Err(SqlSourceJournalError::InvalidSource(
            "aggregate state input has no original emitted route",
        ));
    }
    // Dense scratch and BTree membership remain caller-admitted storage. The
    // Writer output set is prepared once per actual Writer, not per channel.
    work.flush()?;
    let mut pending = Vec::new();
    pending
        .try_reserve(sources.sources.len())
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    let mut queued = BTreeSet::new();
    let mut writer_outputs = BTreeMap::new();
    pending.push(root);
    work.step()?;
    work.flush()?;
    queued.insert(root);
    work.step()?;
    while let Some(endpoint) = pending.pop() {
        work.flush()?;
        let index = sources.index.get(&endpoint).copied();
        work.step()?;
        let source = index.and_then(|index| sources.sources.get(index));
        work.step()?;
        let source = source.ok_or(SqlSourceJournalError::InvalidSource(
            "aggregate state input has no original emitted route",
        ))?;
        match &source.origin {
            StateOrigin::Emission(site) => {
                let (entry, input) = graph.emission(endpoint, *site, work)?;
                emission(entry, endpoint, work)?;
                if let Some(input) = input {
                    enqueue_observed(sources, input, &mut queued, &mut pending, work)?;
                }
            }
            StateOrigin::WriterNoContribution { auxiliary_ordinal } => {
                check_writer_no_contribution_observed(
                    graph,
                    endpoint,
                    *auxiliary_ordinal,
                    &mut writer_outputs,
                    work,
                )?;
                no_contribution(endpoint, work)?;
            }
            StateOrigin::Transport(kind, links) => {
                check_transport_observed(graph, endpoint, *kind, links, work)?;
                transport(endpoint, *kind, links, work)?;
                for link in links.iter().rev() {
                    enqueue_observed(sources, link.source, &mut queued, &mut pending, work)?;
                }
            }
        }
    }
    Ok(())
}

fn check_writer_no_contribution_observed<'a, G: StateGraph<'a>>(
    graph: &G,
    endpoint: AggregateStateEndpoint,
    auxiliary_ordinal: u32,
    writer_outputs: &mut BTreeMap<(FragmentId, NodeId), BTreeSet<ValueId>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), SqlSourceJournalError> {
    use novarocks_physical_plan::{ValueOrigin, WriterDerivedKind, WriterRelationFieldRole};

    work.flush()?;
    let node = graph.node(endpoint.fragment, endpoint.node);
    work.step()?;
    let node = node.ok_or(SqlSourceJournalError::MissingEntry)?;
    let target = match &node.kind {
        NodeKind::TableWriter { target } => Some(target),
        _ => None,
    };
    work.step()?;
    let target = target.ok_or(SqlSourceJournalError::InvalidSource(
        "Writer non-contribution has no original Writer",
    ))?;
    let field = target.output_schema.fields.get(auxiliary_ordinal as usize);
    work.step()?;
    let field = field.ok_or(SqlSourceJournalError::InvalidSource(
        "Writer non-contribution has no original auxiliary field",
    ))?;
    let same_field = field.role == WriterRelationFieldRole::Auxiliary
        && field.value == endpoint.value
        && field.ty.nullable()
        && node.output.columns.get(auxiliary_ordinal as usize) == Some(&endpoint.value);
    work.step()?;
    if !same_field {
        return Err(SqlSourceJournalError::InvalidSource(
            "Writer non-contribution differs from its original auxiliary field",
        ));
    }
    work.flush()?;
    let value = graph.value(endpoint.fragment, endpoint.value);
    work.step()?;
    let value = value.ok_or(SqlSourceJournalError::MissingEntry)?;
    let same_origin = matches!(value.origin, ValueOrigin::WriterDerived {
        writer_node, kind: WriterDerivedKind::RelationAuxiliary,
    } if writer_node == node.id);
    work.step()?;
    if !same_origin {
        return Err(SqlSourceJournalError::InvalidSource(
            "Writer non-contribution has a foreign value origin",
        ));
    }
    work.flush()?;
    let exact = field.ty.exactly_equals_observed(&value.ty, || {
        work.step().map_err(SqlOperationalProjectionError::from)
    });
    let exact = exact.map_err(|error| match error {
        SqlOperationalProjectionError::Control(cause) => SqlSourceJournalError::Control(cause),
        _ => SqlSourceJournalError::InvalidSource(
            "Writer non-contribution has an invalid declared type",
        ),
    });
    let exact = match exact {
        Err(SqlSourceJournalError::Control(cause)) => {
            return Err(SqlSourceJournalError::Control(cause));
        }
        result => {
            work.flush()?;
            result?
        }
    };
    if !exact {
        return Err(SqlSourceJournalError::InvalidSource(
            "Writer non-contribution differs from its complete declared type",
        ));
    }
    let key = (endpoint.fragment, endpoint.node);
    work.flush()?;
    let prepared = writer_outputs.contains_key(&key);
    work.step()?;
    if !prepared {
        let mut outputs = BTreeSet::new();
        for call in target.partial_aggregates.iter() {
            work.flush()?;
            outputs.insert(call.output);
            work.step()?;
        }
        work.flush()?;
        writer_outputs.insert(key, outputs);
        work.step()?;
    }
    work.flush()?;
    let outputs = writer_outputs.get(&key);
    work.step()?;
    let outputs = outputs.ok_or(SqlSourceJournalError::MissingEntry)?;
    work.flush()?;
    let emitted = outputs.contains(&endpoint.value);
    work.step()?;
    if emitted {
        return Err(SqlSourceJournalError::InvalidSource(
            "Writer non-contribution is an actual Partial output",
        ));
    }
    Ok(())
}

/// Check the retained record against this immutable emission, not against a
/// separately inferred state grammar or implementation compatibility rule.
fn check_transport_observed<'a, G: StateGraph<'a>>(
    graph: &G,
    endpoint: AggregateStateEndpoint,
    kind: AggregateStateTransport,
    links: &[AggregateStateLink],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), SqlSourceJournalError> {
    work.flush()?;
    let node = graph.node(endpoint.fragment, endpoint.node);
    work.step()?;
    let node = node.ok_or(SqlSourceJournalError::MissingEntry)?;
    let edge = if let AggregateStateTransport::Stream(id) = kind {
        work.flush()?;
        let edge = graph.edge(id);
        work.step()?;
        Some(edge.ok_or(SqlSourceJournalError::MissingEntry)?)
    } else {
        None
    };
    for link in links {
        let ordinal = link.mapping_ordinal as usize;
        let local_input = link.source.fragment == endpoint.fragment
            && node.inputs.get(link.input_ordinal as usize) == Some(&link.source.node);
        let output = node.output.columns.get(ordinal) == Some(&endpoint.value);
        let same = match (&node.kind, kind) {
            (NodeKind::Project { expressions }, AggregateStateTransport::Project) => {
                let pair = expressions.get(ordinal).copied();
                work.step()?;
                work.flush()?;
                let expression = pair
                    .and_then(|(expression, _)| graph.expression(endpoint.fragment, expression));
                // ExprArena is an opaque owner lookup on the original meter.
                work.step()?;
                local_input && output && pair.is_some_and(|(_, value)| value == endpoint.value)
                    && expression.is_some_and(|expr| matches!(expr.kind, novarocks_physical_plan::ExprKind::Value(value) if value == link.source.value))
            }
            (
                NodeKind::SetOp {
                    kind: novarocks_physical_plan::SetOperationKind::UnionAll,
                    input_mappings,
                },
                AggregateStateTransport::UnionAll,
            ) => {
                local_input
                    && output
                    && input_mappings
                        .get(link.input_ordinal as usize)
                        .and_then(|mapping| mapping.get(ordinal))
                        == Some(&link.source.value)
            }
            (
                NodeKind::TopN {
                    phase: novarocks_physical_plan::TopNPhase::Partial { .. },
                    reduction: novarocks_physical_plan::TopNReduction::Rows,
                    ..
                },
                AggregateStateTransport::PartialTopN,
            ) => local_input && output && link.source.value == endpoint.value,
            (
                NodeKind::ExchangeSource { edge: id, .. },
                AggregateStateTransport::Stream(expected),
            ) => {
                let edge = edge.ok_or(SqlSourceJournalError::MissingEntry)?;
                work.flush()?;
                let sender = graph.root(link.source.fragment);
                work.step()?;
                *id == expected
                    && edge.kind == novarocks_physical_plan::EdgeKind::Stream
                    && edge.destination.fragment == endpoint.fragment
                    && edge.destination.node == endpoint.node
                    && edge.source.fragment == link.source.fragment
                    && sender == Some(link.source.node)
                    && link.input_ordinal == 0
                    && output
                    && edge.source.projection.get(ordinal) == Some(&link.source.value)
                    && edge.destination.receive_mapping.get(ordinal)
                        == Some(&(link.source.value, endpoint.value))
            }
            _ => false,
        };
        work.step()?;
        if !same {
            return Err(SqlSourceJournalError::InvalidSource(
                "aggregate state transport differs from its original mapping",
            ));
        }
    }
    Ok(())
}

fn enqueue_observed(
    sources: &AggregateStateSources,
    endpoint: AggregateStateEndpoint,
    queued: &mut BTreeSet<AggregateStateEndpoint>,
    pending: &mut Vec<AggregateStateEndpoint>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), SqlSourceJournalError> {
    if !sources.contains_observed(endpoint, work)? {
        return Err(SqlSourceJournalError::InvalidSource(
            "aggregate state input has no original emitted route",
        ));
    }
    work.flush()?;
    let fresh = queued.insert(endpoint);
    work.step()?;
    if fresh {
        work.flush()?;
        pending
            .try_reserve(1)
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        pending.push(endpoint);
        work.step()?;
    }
    Ok(())
}

pub(super) fn check_state_inputs_observed<'a>(
    entry: &CheckedAggregateLogicalSourceEntry<'a>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CheckedAggregateStateInputs<'a>, SqlSourceJournalError> {
    let AggregateRuntimeDemand::ExpressionState(expression) = entry.runtime else {
        return Err(SqlSourceJournalError::InvalidSource(
            "aggregate update has no merge state inputs",
        ));
    };
    work.flush()?;
    let endpoint = entry
        .owner
        .call_sources
        .state_sources
        .inputs
        .get(&(entry.fragment.id(), entry.site))
        .copied();
    work.step()?;
    let endpoint = endpoint.ok_or(SqlSourceJournalError::InvalidSource(
        "aggregate merge has no original state input association",
    ))?;
    work.flush()?;
    let actual = entry.fragment.expressions().get(expression);
    work.step()?;
    if endpoint.fragment != entry.fragment.id() || entry.node.inputs.as_ref() != [endpoint.node] || !actual.is_some_and(|expr| matches!(expr.kind, novarocks_physical_plan::ExprKind::Value(value) if value == endpoint.value)) {
        return Err(SqlSourceJournalError::InvalidSource("aggregate state association differs from its actual merge channel"));
    }
    work.flush()?;
    let child = entry.fragment.nodes().get(&endpoint.node);
    work.step()?;
    let child = child.ok_or(SqlSourceJournalError::MissingEntry)?;
    let mut present = false;
    for value in &child.output.columns {
        work.step()?;
        present |= *value == endpoint.value;
    }
    if !present {
        return Err(SqlSourceJournalError::InvalidSource(
            "aggregate state input is absent from its original child port",
        ));
    }
    Ok(CheckedAggregateStateInputs {
        owner: entry.owner,
        root: endpoint,
    })
}

/// A Final aggregate reads only its explicitly declared writer-results child.
/// The independent summary child is not an aggregate-state contributor.
fn writer_state_input_node(node: &novarocks_physical_plan::PhysicalNode) -> Option<NodeId> {
    let NodeKind::TableFinish(spec) = &node.kind else {
        return None;
    };
    let logical = matches!(
        spec.count_mode,
        novarocks_physical_plan::FinishCountMode::LogicalChanges
    );
    if node.inputs.len() != 1 + spec.inputs.summaries.iter().count()
        || logical != spec.inputs.summaries.write_counts.is_some()
        || novarocks_physical_plan::SummaryKind::ALL
            .into_iter()
            .any(|kind| {
                let port = spec.inputs.summaries.get(kind);
                let columns = spec.summaries.get(kind);
                port.is_some() != columns.is_some()
                    || columns.is_some_and(|columns| columns.kind() != kind)
                    || port.is_some_and(|ordinal| {
                        *ordinal == spec.inputs.writer_results
                            || usize::try_from(*ordinal)
                                .map_or(true, |index| index >= node.inputs.len())
                    })
            })
        || spec.inputs.summaries.write_counts.is_some()
            && spec.inputs.summaries.write_counts == spec.inputs.summaries.delete_verification
    {
        return None;
    }
    node.inputs
        .get(usize::try_from(spec.inputs.writer_results).ok()?)
        .copied()
}

/// Check the actual Writer ValueId demand without fabricating an expression.
pub(super) fn check_writer_state_inputs_observed<'a>(
    entry: &CheckedWriterAggregateLogicalSourceEntry<'a>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<CheckedWriterAggregateStateInputs<'a>, SqlSourceJournalError> {
    let demand = match entry.runtime {
        AggregateRuntimeDemand::WriterState(value) => Some(value),
        _ => None,
    };
    work.step()?;
    let demand = demand.ok_or(SqlSourceJournalError::InvalidSource(
        "Writer update has no merge state inputs",
    ))?;
    work.flush()?;
    let endpoint = entry
        .owner
        .call_sources
        .state_sources
        .inputs
        .get(&(entry.fragment.id(), entry.site))
        .copied();
    work.step()?;
    let endpoint = endpoint.ok_or(SqlSourceJournalError::InvalidSource(
        "Writer merge has no original state input association",
    ))?;
    let same = endpoint.fragment == entry.fragment.id()
        && writer_state_input_node(entry.node) == Some(endpoint.node)
        && endpoint.value == demand
        && demand == entry.source.input;
    work.step()?;
    if !same {
        return Err(SqlSourceJournalError::InvalidSource(
            "Writer state association differs from its actual merge channel",
        ));
    }
    work.flush()?;
    let child = entry.fragment.nodes().get(&endpoint.node);
    work.step()?;
    let child = child.ok_or(SqlSourceJournalError::MissingEntry)?;
    let mut present = false;
    for value in &child.output.columns {
        let same = *value == demand;
        work.step()?;
        present |= same;
    }
    if !present {
        return Err(SqlSourceJournalError::InvalidSource(
            "Writer state input is absent from its original child port",
        ));
    }
    Ok(CheckedWriterAggregateStateInputs {
        owner: entry.owner,
        root: endpoint,
    })
}

/// Read capabilities borrow one exact actual graph owner. Construction cannot
/// mint a completed owner, clone a graph or manufacture logical captures.
trait StateGraph<'a> {
    type Emission;
    fn journal(&self) -> &'a super::SqlLogicalSourceJournal;
    fn node(
        &self,
        fragment: FragmentId,
        node: NodeId,
    ) -> Option<&'a novarocks_physical_plan::PhysicalNode>;
    fn expression(
        &self,
        fragment: FragmentId,
        expr: novarocks_physical_plan::ExprId,
    ) -> Option<&'a novarocks_physical_plan::ExprNode>;
    fn value(
        &self,
        fragment: FragmentId,
        value: ValueId,
    ) -> Option<&'a novarocks_physical_plan::ValueDef>;
    fn edge(
        &self,
        edge: novarocks_physical_plan::EdgeId,
    ) -> Option<&'a novarocks_physical_plan::Edge>;
    fn root(&self, fragment: FragmentId) -> Option<NodeId>;
    fn emission(
        &self,
        endpoint: AggregateStateEndpoint,
        site: PhysicalCallSite,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(Self::Emission, Option<AggregateStateEndpoint>), SqlSourceJournalError>;
}
struct PublishedStateGraph<'a>(&'a SqlAuthoredPhysicalPlan);
impl<'a> StateGraph<'a> for PublishedStateGraph<'a> {
    type Emission = CheckedStateEmission<'a>;
    fn journal(&self) -> &'a super::SqlLogicalSourceJournal {
        &self.0.call_sources
    }
    fn node(&self, f: FragmentId, n: NodeId) -> Option<&'a novarocks_physical_plan::PhysicalNode> {
        self.0.plan.fragments().get(&f)?.nodes().get(&n)
    }
    fn expression(
        &self,
        f: FragmentId,
        e: novarocks_physical_plan::ExprId,
    ) -> Option<&'a novarocks_physical_plan::ExprNode> {
        self.0.plan.fragments().get(&f)?.expressions().get(e)
    }
    fn value(&self, f: FragmentId, v: ValueId) -> Option<&'a novarocks_physical_plan::ValueDef> {
        self.0.plan.fragments().get(&f)?.values().get(&v)
    }
    fn edge(
        &self,
        e: novarocks_physical_plan::EdgeId,
    ) -> Option<&'a novarocks_physical_plan::Edge> {
        self.0.plan.edges().get(&e)
    }
    fn root(&self, f: FragmentId) -> Option<NodeId> {
        Some(self.0.plan.fragments().get(&f)?.root())
    }
    fn emission(
        &self,
        endpoint: AggregateStateEndpoint,
        site: PhysicalCallSite,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(Self::Emission, Option<AggregateStateEndpoint>), SqlSourceJournalError> {
        work.flush()?;
        let owner = self.0;
        let fragment = owner.plan.fragments().get(&endpoint.fragment);
        work.step()?;
        let fragment = fragment.ok_or(SqlSourceJournalError::MissingEntry)?;
        work.flush()?;
        let node = fragment.nodes().get(&endpoint.node);
        work.step()?;
        let node = node.ok_or(SqlSourceJournalError::MissingEntry)?;
        let (entry, input) = match (&site, &node.kind) {
            (
                PhysicalCallSite::WriterPartial { node: id, call },
                NodeKind::TableWriter { target },
            ) if *id == node.id => {
                let call = target.partial_aggregates.get(*call as usize);
                work.step()?;
                let call = call.ok_or(SqlSourceJournalError::InvalidSource(
                    "Writer state route differs from its actual emission",
                ))?;
                let same_output = call.output == endpoint.value
                    && matches!(call.binding.phase, AggregatePhase::Partial { .. });
                work.step()?;
                if !same_output {
                    return Err(SqlSourceJournalError::InvalidSource(
                        "Writer state route loans a result or foreign output",
                    ));
                }
                let entry = owner
                    .checked_writer_aggregate_source_observed(fragment, node, site, call, work)?;
                (CheckedStateEmission::Writer(entry), None)
            }
            _ => {
                let call = match (&site, &node.kind) {
                    (
                        PhysicalCallSite::Aggregate { node: id, call },
                        NodeKind::Aggregate { calls, .. },
                    ) if *id == node.id => calls.get(*call as usize),
                    (
                        PhysicalCallSite::TopNState { node: id, call },
                        NodeKind::TopN {
                            reduction:
                                novarocks_physical_plan::TopNReduction::GroupedStates { calls, .. },
                            ..
                        },
                    ) if *id == node.id => calls.get(*call as usize),
                    _ => None,
                };
                work.step()?;
                let call = call.ok_or(SqlSourceJournalError::InvalidSource(
                    "aggregate state route differs from its actual emission",
                ))?;
                let same_output =
                    call.output == endpoint.value && !call.binding.phase.produces_final_result();
                work.step()?;
                if !same_output {
                    return Err(SqlSourceJournalError::InvalidSource(
                        "aggregate state route loans a result or foreign output",
                    ));
                }
                let entry =
                    owner.checked_aggregate_source_observed(fragment, node, site, call, work)?;
                let input = if entry.phase().consumes_logical_arguments() {
                    None
                } else {
                    Some(check_state_inputs_observed(&entry, work)?.root)
                };
                (CheckedStateEmission::Aggregate(entry), input)
            }
        };
        Ok((entry, input))
    }
}

pub(in crate::planner::distributed::build) struct ConstructionStateGraph<'a> {
    journal: &'a super::SqlLogicalSourceJournal,
    fragments: &'a BTreeMap<FragmentId, novarocks_physical_plan::FragmentBuilder>,
    completions: &'a BTreeMap<FragmentId, (NodeId, novarocks_physical_plan::FragmentSink)>,
    edges: &'a BTreeMap<novarocks_physical_plan::EdgeId, novarocks_physical_plan::Edge>,
}
pub(in crate::planner::distributed::build) struct ConstructionStateEmission<'a> {
    pub(in crate::planner::distributed::build) site: PhysicalCallSite,
    pub(in crate::planner::distributed::build) entry: &'a super::LoweredAggregateSourceEntry,
    pub(in crate::planner::distributed::build) binding:
        &'a novarocks_physical_plan::AggregateBinding,
    pub(in crate::planner::distributed::build) writer: bool,
}
impl<'a> ConstructionStateGraph<'a> {
    pub(in crate::planner::distributed::build) fn borrow(
        journal: &'a super::SqlLogicalSourceJournal,
        fragments: &'a BTreeMap<FragmentId, novarocks_physical_plan::FragmentBuilder>,
        completions: &'a BTreeMap<FragmentId, (NodeId, novarocks_physical_plan::FragmentSink)>,
        edges: &'a BTreeMap<novarocks_physical_plan::EdgeId, novarocks_physical_plan::Edge>,
    ) -> Self {
        Self {
            journal,
            fragments,
            completions,
            edges,
        }
    }
    pub(in crate::planner::distributed::build) fn visit_merge_observed(
        &self,
        fragment: FragmentId,
        site: PhysicalCallSite,
        work: &mut CompileCheckpoints<'_>,
        emission: impl FnMut(
            ConstructionStateEmission<'a>,
            AggregateStateEndpoint,
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), SqlSourceJournalError>,
    ) -> Result<(), SqlSourceJournalError> {
        self.visit_merge_sources_observed(fragment, site, work, emission, |_, _| Ok(()))
    }
    pub(in crate::planner::distributed::build) fn visit_merge_sources_observed(
        &self,
        fragment: FragmentId,
        site: PhysicalCallSite,
        work: &mut CompileCheckpoints<'_>,
        emission: impl FnMut(
            ConstructionStateEmission<'a>,
            AggregateStateEndpoint,
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), SqlSourceJournalError>,
        no_contribution: impl FnMut(
            AggregateStateEndpoint,
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), SqlSourceJournalError>,
    ) -> Result<(), SqlSourceJournalError> {
        let entry = self
            .journal
            .entries
            .get(&(fragment, site))
            .ok_or(SqlSourceJournalError::MissingEntry)?;
        work.step()?;
        let node_id = match site {
            PhysicalCallSite::Aggregate { node, .. }
            | PhysicalCallSite::TopNState { node, .. }
            | PhysicalCallSite::WriterFinal { node, .. }
            | PhysicalCallSite::WriterPartial { node, .. } => node,
            _ => {
                return Err(SqlSourceJournalError::InvalidSource(
                    "merge uses a different actual lifecycle",
                ));
            }
        };
        let node = self
            .node(fragment, node_id)
            .ok_or(SqlSourceJournalError::MissingEntry)?;
        let root = construction_input_root(self, fragment, node, site, entry.runtime, work)?;
        visit_state_graph_observed(
            self,
            root,
            work,
            emission,
            |_, _, _, _| Ok(()),
            no_contribution,
        )
    }
}
impl<'a> StateGraph<'a> for ConstructionStateGraph<'a> {
    type Emission = ConstructionStateEmission<'a>;
    fn journal(&self) -> &'a super::SqlLogicalSourceJournal {
        self.journal
    }
    fn node(&self, f: FragmentId, n: NodeId) -> Option<&'a novarocks_physical_plan::PhysicalNode> {
        self.fragments.get(&f)?.construction_node(n)
    }
    fn expression(
        &self,
        f: FragmentId,
        e: novarocks_physical_plan::ExprId,
    ) -> Option<&'a novarocks_physical_plan::ExprNode> {
        self.fragments.get(&f)?.expressions().get(e)
    }
    fn value(&self, f: FragmentId, v: ValueId) -> Option<&'a novarocks_physical_plan::ValueDef> {
        self.fragments.get(&f)?.value(v)
    }
    fn edge(
        &self,
        e: novarocks_physical_plan::EdgeId,
    ) -> Option<&'a novarocks_physical_plan::Edge> {
        self.edges.get(&e)
    }
    fn root(&self, f: FragmentId) -> Option<NodeId> {
        self.completions.get(&f).map(|pair| pair.0)
    }
    fn emission(
        &self,
        endpoint: AggregateStateEndpoint,
        site: PhysicalCallSite,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(Self::Emission, Option<AggregateStateEndpoint>), SqlSourceJournalError> {
        use super::{AggregateSourceTarget, LoweredAggregateLogicalSource};
        let node = self
            .node(endpoint.fragment, endpoint.node)
            .ok_or(SqlSourceJournalError::MissingEntry)?;
        work.step()?;
        let (binding, target, writer) = match (site, &node.kind) {
            (PhysicalCallSite::Aggregate { node: id, call }, NodeKind::Aggregate { calls, .. })
                if id == node.id =>
            {
                let c = calls
                    .get(call as usize)
                    .ok_or(SqlSourceJournalError::MissingEntry)?;
                if c.output != endpoint.value || c.binding.phase.produces_final_result() {
                    return Err(SqlSourceJournalError::InvalidSource(
                        "construction state route loans a result or foreign output",
                    ));
                }
                (&c.binding, AggregateSourceTarget::Aggregate(c.id), false)
            }
            (
                PhysicalCallSite::TopNState { node: id, call },
                NodeKind::TopN {
                    reduction: novarocks_physical_plan::TopNReduction::GroupedStates { calls, .. },
                    ..
                },
            ) if id == node.id => {
                let c = calls
                    .get(call as usize)
                    .ok_or(SqlSourceJournalError::MissingEntry)?;
                if c.output != endpoint.value || c.binding.phase.produces_final_result() {
                    return Err(SqlSourceJournalError::InvalidSource(
                        "construction state route loans a result or foreign output",
                    ));
                }
                (&c.binding, AggregateSourceTarget::Aggregate(c.id), false)
            }
            (
                PhysicalCallSite::WriterPartial { node: id, call },
                NodeKind::TableWriter { target },
            ) if id == node.id => {
                let c = target
                    .partial_aggregates
                    .get(call as usize)
                    .ok_or(SqlSourceJournalError::MissingEntry)?;
                if c.output != endpoint.value
                    || !matches!(c.binding.phase, AggregatePhase::Partial { .. })
                {
                    return Err(SqlSourceJournalError::InvalidSource(
                        "construction Writer route loans a result or foreign output",
                    ));
                }
                (&c.binding, AggregateSourceTarget::Writer(c.output), true)
            }
            _ => {
                return Err(SqlSourceJournalError::InvalidSource(
                    "construction emission differs from its actual site",
                ));
            }
        };
        work.step()?;
        let entry = self
            .journal
            .entries
            .get(&(endpoint.fragment, site))
            .ok_or(SqlSourceJournalError::MissingEntry)?;
        work.step()?;
        if entry.target != target
            || entry.phase != binding.phase
            || matches!(&entry.logical, LoweredAggregateLogicalSource::Uncertified)
        {
            return Err(SqlSourceJournalError::InvalidSource(
                "construction state source differs from its original producer",
            ));
        }
        let input = if binding.phase.consumes_logical_arguments() {
            None
        } else {
            Some(construction_input_root(
                self,
                endpoint.fragment,
                node,
                site,
                entry.runtime,
                work,
            )?)
        };
        Ok((
            ConstructionStateEmission {
                site,
                entry,
                binding,
                writer,
            },
            input,
        ))
    }
}
fn construction_input_root<'a, G: StateGraph<'a>>(
    graph: &G,
    fragment: FragmentId,
    node: &novarocks_physical_plan::PhysicalNode,
    site: PhysicalCallSite,
    demand: AggregateRuntimeDemand,
    work: &mut CompileCheckpoints<'_>,
) -> Result<AggregateStateEndpoint, SqlSourceJournalError> {
    let root = graph
        .journal()
        .state_sources
        .inputs
        .get(&(fragment, site))
        .copied();
    work.step()?;
    let root = root.ok_or(SqlSourceJournalError::InvalidSource(
        "merge lacks its original state input association",
    ))?;
    let original_child = match (site, &node.kind) {
        (PhysicalCallSite::WriterFinal { node: owner, .. }, NodeKind::TableFinish(_))
            if owner == node.id =>
        {
            writer_state_input_node(node) == Some(root.node)
        }
        _ => node.inputs.as_ref() == [root.node],
    };
    if root.fragment != fragment || !original_child {
        return Err(SqlSourceJournalError::InvalidSource(
            "merge differs from its original child association",
        ));
    }
    let actual=match demand {
        AggregateRuntimeDemand::ExpressionState(expr)=>graph.expression(fragment,expr).is_some_and(|expr|matches!(expr.kind,novarocks_physical_plan::ExprKind::Value(value) if value==root.value)),
        AggregateRuntimeDemand::WriterState(value)=>value==root.value,
        AggregateRuntimeDemand::Update=>false,
    };
    work.step()?;
    if !actual {
        return Err(SqlSourceJournalError::InvalidSource(
            "merge state demand differs from its actual emitted channel",
        ));
    }
    let child = graph
        .node(fragment, root.node)
        .ok_or(SqlSourceJournalError::MissingEntry)?;
    let mut present = false;
    for value in child.output.columns.iter() {
        work.step()?;
        present |= *value == root.value;
    }
    if !present {
        return Err(SqlSourceJournalError::InvalidSource(
            "merge state is absent from its original child port",
        ));
    }
    Ok(root)
}
