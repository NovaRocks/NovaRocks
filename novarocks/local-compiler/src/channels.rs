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

//! Ordered channels and exact input sources for the supported source tree.
//! Repeated publication is proved from transparent reads, never from sharing
//! an expression definition or from a function's apparent determinism.

use std::{collections::BTreeMap, error::Error, fmt, sync::Arc};

use novarocks_local_program::{ProgramChannelLayoutRole, ProgramChannelSite, ProgramNodeId};
use novarocks_physical_plan::{ExprId, ExprKind, FragmentPackage, NodeId, NodeKind, ValueId};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
    ValueTypeError,
};
use novarocks_types::SlotId;

use crate::assert_rows::reserve_vec;
use crate::unpivot::{UnpivotInputPort, UnpivotLoweringError, plan_unpivot_channels};

use crate::repeat::{RepeatInputPort, RepeatLoweringError, plan_repeat_channels};

pub(crate) struct PlannedChannels {
    pub nodes: BTreeMap<NodeId, NodeChannels>,
    pub inputs: BTreeMap<ExprId, ResolvedInput>,
    pub unpivot_sources: BTreeMap<NodeId, BTreeMap<ValueId, SlotId>>,
    pub assertion_keys: BTreeMap<NodeId, Vec<SlotId>>,
    pub unions: BTreeMap<NodeId, Vec<UnionChannelBranch>>,
    /// Fresh slots of each TableWriter's projected provider input, in target
    /// field order. Its output slots are its multiplex relation's.
    pub writer_projections: BTreeMap<NodeId, Arc<[SlotId]>>,
    /// Fresh internal slots of each TableFinish's statistics: the grouped
    /// Unpivot's grouping output followed by one final output per call.
    /// Empty without statistics.
    pub finish_statistics: BTreeMap<NodeId, Arc<[SlotId]>>,
    /// Each join's planned channels. A join whose physical output is not
    /// canonical also has one selection branch in `unions`.
    pub joins: BTreeMap<NodeId, crate::join::PlannedJoin>,
    /// Join-owned value reads; their actual source is chosen per use.
    pub join_values: BTreeMap<ExprId, crate::join::JoinValueSource>,
    /// Each table function's planned channels. Its argument Project's
    /// pass-through reads are its one branch in `unions`.
    pub table_functions: BTreeMap<NodeId, crate::table_function::PlannedTableFunction>,
    pub series: BTreeMap<NodeId, crate::generate_series::PlannedGenerateSeries>,
}

pub(crate) struct NodeChannels {
    pub local: ProgramNodeId,
    pub slots: Arc<[SlotId]>,
}

pub(crate) struct UnionChannelBranch {
    pub normalizer: ProgramNodeId,
    pub input: ProgramNodeId,
    pub sources: Vec<UnionChannelSource>,
}
pub(crate) struct UnionChannelSource {
    pub input: ResolvedInput,
    pub ty: FunctionValueType,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedInput {
    pub slot: SlotId,
    pub source: ProgramChannelSite,
}

#[derive(Debug)]
pub(crate) enum ChannelLoweringError {
    Control(CompileControlError),
    ValueType(ValueTypeError),
    Repeat(RepeatLoweringError),
    Unpivot(UnpivotLoweringError),
    /// A join planning refusal keeps its own category.
    Join(crate::lowering::FragmentCompileError),
    Invalid(&'static str),
}

impl From<crate::lowering::FragmentCompileError> for ChannelLoweringError {
    fn from(error: crate::lowering::FragmentCompileError) -> Self {
        match error {
            crate::lowering::FragmentCompileError::Control(cause) => Self::Control(cause),
            crate::lowering::FragmentCompileError::Invalid(message) => Self::Invalid(message),
            error => Self::Join(error),
        }
    }
}

impl From<CompileControlError> for ChannelLoweringError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ValueTypeError> for ChannelLoweringError {
    fn from(error: ValueTypeError) -> Self {
        Self::ValueType(error)
    }
}
impl From<RepeatLoweringError> for ChannelLoweringError {
    fn from(error: RepeatLoweringError) -> Self {
        match error {
            RepeatLoweringError::Control(cause) => Self::Control(cause),
            RepeatLoweringError::Invalid(message) => Self::Invalid(message),
            error => Self::Repeat(error),
        }
    }
}
impl From<UnpivotLoweringError> for ChannelLoweringError {
    fn from(error: UnpivotLoweringError) -> Self {
        match error {
            UnpivotLoweringError::Control(c) => Self::Control(c),
            UnpivotLoweringError::Invalid(m) => Self::Invalid(m),
            e => Self::Unpivot(e),
        }
    }
}
impl fmt::Display for ChannelLoweringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::ValueType(error) => error.fmt(f),
            Self::Repeat(error) => error.fmt(f),
            Self::Unpivot(error) => error.fmt(f),
            Self::Join(error) => error.fmt(f),
            Self::Invalid(message) => write!(f, "invalid planned channels: {message}"),
        }
    }
}
impl Error for ChannelLoweringError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            Self::ValueType(error) => Some(error),
            Self::Repeat(error) => Some(error),
            Self::Unpivot(error) => Some(error),
            Self::Join(error) => Some(error),
            Self::Invalid(_) => None,
        }
    }
}

// A representative is inserted only after every occurrence of that value in
// this port has been proved equal by the supported operator's construction.
// It is an actual column ordinal, not an ordinal inferred from a ValueId.
type Port = BTreeMap<ValueId, usize>;

pub(crate) fn resolve_tree_channels(
    package: &FragmentPackage,
    root_first: &[NodeId],
    control: &dyn PureCompileControl,
) -> Result<PlannedChannels, ChannelLoweringError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = resolve_core(package, root_first, &mut work);
    if matches!(&result, Err(ChannelLoweringError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn resolve_core(
    package: &FragmentPackage,
    root_first: &[NodeId],
    work: &mut CompileCheckpoints<'_>,
) -> Result<PlannedChannels, ChannelLoweringError> {
    let fragment = package.fragment();
    if root_first.first() != Some(&fragment.root()) || root_first.len() != fragment.nodes().len() {
        return Err(ChannelLoweringError::Invalid("tree node coverage differs"));
    }
    let mut nodes = BTreeMap::<NodeId, NodeChannels>::new();
    let mut ports = BTreeMap::<NodeId, Port>::new();
    let mut next_slot = 0_u64;
    let mut unpivot_sources = BTreeMap::new();
    let mut assertion_keys = BTreeMap::new();
    let mut next_node = 0usize;
    let mut unions = BTreeMap::new();
    let mut writer_projections = BTreeMap::new();
    let mut finish_statistics = BTreeMap::new();
    let mut joins = BTreeMap::new();
    let mut table_functions = BTreeMap::new();
    let mut series = BTreeMap::new();
    for &source in root_first.iter().rev() {
        let node = fragment
            .nodes()
            .get(&source)
            .ok_or(ChannelLoweringError::Invalid("missing physical node"))?;
        if nodes.contains_key(&source) {
            return Err(ChannelLoweringError::Invalid("duplicate tree node"));
        }
        // A join whose physical output is not its canonical output publishes
        // it through a selection Project that directly follows the join.
        let mut join_shape = match node.kind {
            NodeKind::HashJoin { .. } | NodeKind::NestLoopJoin { .. } => {
                Some(crate::join::shape_join(fragment, node, &ports, work)?)
            }
            _ => None,
        };
        let additional = if matches!(
            node.kind,
            NodeKind::SetOp {
                kind: novarocks_physical_plan::SetOperationKind::UnionAll,
                ..
            }
        ) {
            node.inputs.len()
        } else if matches!(
            node.kind,
            NodeKind::TableFunction { .. } | NodeKind::GenerateSeries { .. }
        ) {
            // The argument Project or bounds Values directly precedes its owner.
            1
        } else {
            join_shape
                .as_ref()
                .map_or(0, |shape| usize::from(shape.selection()))
        };
        let local_index = next_node
            .checked_add(additional)
            .ok_or(CompileControlError::ResourceExhausted)?;
        next_node = local_index
            .checked_add(1)
            .ok_or(CompileControlError::ResourceExhausted)?;
        if next_node > novarocks_local_program::MAX_PROGRAM_NODES {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        let local = ProgramNodeId::new(local_index);
        work.step()?;
        let (slots, port) = match &node.kind {
            NodeKind::Values { rows } if node.inputs.is_empty() => {
                let mut slots = Vec::new();
                reserve_vec(&mut slots, node.output.columns.len(), work)?;
                let mut port = Port::new();
                for (ordinal, &value) in node.output.columns.iter().enumerate() {
                    let output = fragment
                        .values()
                        .get(&value)
                        .ok_or(ChannelLoweringError::Invalid("missing Values output value"))?;
                    for row in rows {
                        let expression = *row.get(ordinal).ok_or(ChannelLoweringError::Invalid(
                            "Values occurrence width differs",
                        ))?;
                        let definition = fragment
                            .expressions()
                            .get(expression)
                            .ok_or(ChannelLoweringError::Invalid("missing Values definition"))?;
                        work.step()?;
                        exact_type(&definition.ty, &output.ty, work)?;
                        if let Some(&representative) = port.get(&value) {
                            // This proof is transparent only for the same definition
                            // at every row. Values materialization separately admits
                            // constant cells, so it cannot merge independent calls.
                            if row.get(representative) != Some(&expression) {
                                return Err(ChannelLoweringError::Invalid(
                                    "independent Values cells share a produced value",
                                ));
                            }
                        }
                    }
                    let slot = u32::try_from(next_slot)
                        .map_err(|_| ChannelLoweringError::Invalid("slot identity exhausted"))?;
                    next_slot = next_slot
                        .checked_add(1)
                        .ok_or(ChannelLoweringError::Invalid("slot identity exhausted"))?;
                    slots.push(SlotId::new(slot));
                    port.entry(value).or_insert(ordinal);
                    work.step()?;
                }
                work.flush()?;
                let slots: Arc<[SlotId]> = Arc::from(slots);
                work.flush()?;
                (slots, port)
            }
            NodeKind::ExchangeSource { .. } if node.inputs.is_empty() => {
                // Each received occurrence is its own fresh channel. A repeated
                // import is one sent value, so its first ordinal represents it.
                let mut slots = Vec::new();
                reserve_vec(&mut slots, node.output.columns.len(), work)?;
                let mut port = Port::new();
                for (ordinal, &value) in node.output.columns.iter().enumerate() {
                    if !fragment.values().contains_key(&value) {
                        return Err(ChannelLoweringError::Invalid(
                            "missing ExchangeSource output value",
                        ));
                    }
                    let slot = u32::try_from(next_slot)
                        .map_err(|_| ChannelLoweringError::Invalid("slot identity exhausted"))?;
                    next_slot = next_slot
                        .checked_add(1)
                        .ok_or(ChannelLoweringError::Invalid("slot identity exhausted"))?;
                    slots.push(SlotId::new(slot));
                    port.entry(value).or_insert(ordinal);
                    work.step()?;
                }
                work.flush()?;
                let slots: Arc<[SlotId]> = Arc::from(slots);
                work.flush()?;
                (slots, port)
            }
            NodeKind::Scan { .. } if node.inputs.is_empty() => {
                // Each provider output occurrence is its own fresh channel in
                // the public schema order. A repeated provider value is one
                // read column, so its first ordinal represents it.
                let mut slots = Vec::new();
                reserve_vec(&mut slots, node.output.columns.len(), work)?;
                let mut port = Port::new();
                for (ordinal, &value) in node.output.columns.iter().enumerate() {
                    if !fragment.values().contains_key(&value) {
                        return Err(ChannelLoweringError::Invalid("missing Scan output value"));
                    }
                    let slot = u32::try_from(next_slot)
                        .map_err(|_| ChannelLoweringError::Invalid("slot identity exhausted"))?;
                    next_slot = next_slot
                        .checked_add(1)
                        .ok_or(ChannelLoweringError::Invalid("slot identity exhausted"))?;
                    slots.push(SlotId::new(slot));
                    port.entry(value).or_insert(ordinal);
                    work.step()?;
                }
                work.flush()?;
                let slots: Arc<[SlotId]> = Arc::from(slots);
                work.flush()?;
                (slots, port)
            }
            NodeKind::SetOp {
                kind: novarocks_physical_plan::SetOperationKind::UnionAll,
                input_mappings,
            } => {
                if node.inputs.len() < 2 || input_mappings.len() != node.inputs.len() {
                    return Err(ChannelLoweringError::Invalid(
                        "UnionAll input occurrence shape differs",
                    ));
                }
                let mut slots = Vec::new();
                reserve_vec(&mut slots, node.output.columns.len(), work)?;
                let mut port = Port::new();
                for (ordinal, &value) in node.output.columns.iter().enumerate() {
                    if let Some(&representative) = port.get(&value) {
                        for mapping in input_mappings {
                            let same = mapping
                                .get(representative)
                                .zip(mapping.get(ordinal))
                                .is_some_and(|(a, b)| a == b);
                            work.step()?;
                            if !same {
                                return Err(ChannelLoweringError::Invalid(
                                    "independent UnionAll mappings share one produced value",
                                ));
                            }
                        }
                    } else {
                        port.insert(value, ordinal);
                    }
                    let slot = u32::try_from(next_slot)
                        .map_err(|_| CompileControlError::ResourceExhausted)?;
                    next_slot = next_slot
                        .checked_add(1)
                        .ok_or(CompileControlError::ResourceExhausted)?;
                    slots.push(SlotId::new(slot));
                    work.step()?;
                }
                let mut branches = Vec::new();
                reserve_vec(&mut branches, node.inputs.len(), work)?;
                for (branch, (&child, mapping)) in
                    node.inputs.iter().zip(input_mappings).enumerate()
                {
                    let child_channels = nodes.get(&child).ok_or(ChannelLoweringError::Invalid(
                        "UnionAll child has not been lowered",
                    ))?;
                    let child_port = ports.get(&child).ok_or(ChannelLoweringError::Invalid(
                        "UnionAll child port is missing",
                    ))?;
                    if mapping.len() != node.output.columns.len() {
                        return Err(ChannelLoweringError::Invalid(
                            "UnionAll mapping width differs",
                        ));
                    }
                    let mut sources = Vec::new();
                    reserve_vec(&mut sources, mapping.len(), work)?;
                    for (&value, &output) in mapping.iter().zip(&node.output.columns) {
                        let input = resolve_input(value, child_channels, child_port)?;
                        work.step()?;
                        let source_ty = &fragment
                            .values()
                            .get(&value)
                            .ok_or(ChannelLoweringError::Invalid(
                                "UnionAll mapped value missing",
                            ))?
                            .ty;
                        let target_ty = &fragment
                            .values()
                            .get(&output)
                            .ok_or(ChannelLoweringError::Invalid(
                                "UnionAll output value missing",
                            ))?
                            .ty;
                        if source_ty.nullable && !target_ty.nullable {
                            return Err(ChannelLoweringError::Invalid(
                                "UnionAll narrows nullable input",
                            ));
                        }
                        work.flush()?;
                        let mut widened = source_ty.clone();
                        widened.nullable = target_ty.nullable;
                        exact_type(&widened, target_ty, work)?;
                        sources.push(UnionChannelSource {
                            input,
                            ty: source_ty.clone(),
                        });
                        work.step()?;
                        work.flush()?;
                    }
                    branches.push(UnionChannelBranch {
                        normalizer: ProgramNodeId::new(local_index - node.inputs.len() + branch),
                        input: child_channels.local,
                        sources,
                    });
                    work.step()?;
                }
                unions.insert(source, branches);
                work.flush()?;
                (Arc::from(slots), port)
            }
            NodeKind::Project { expressions } => {
                let child = single_child(node.inputs.as_ref())?;
                let child_node = &fragment.nodes()[&child];
                let child_channels = nodes
                    .get(&child)
                    .ok_or(ChannelLoweringError::Invalid("missing lowered child"))?;
                let child_port = &ports[&child];
                if expressions.len() != node.output.columns.len() {
                    return Err(ChannelLoweringError::Invalid(
                        "project occurrence width differs",
                    ));
                }
                let mut slots = Vec::with_capacity(expressions.len());
                let mut port = Port::new();
                let mut producers = BTreeMap::<ValueId, Option<ResolvedInput>>::new();
                for (ordinal, &(expression, value)) in expressions.iter().enumerate() {
                    if node.output.columns[ordinal] != value {
                        return Err(ChannelLoweringError::Invalid(
                            "ordered project values differ",
                        ));
                    }
                    let definition = fragment
                        .expressions()
                        .get(expression)
                        .ok_or(ChannelLoweringError::Invalid("missing project definition"))?;
                    let output = fragment
                        .values()
                        .get(&value)
                        .ok_or(ChannelLoweringError::Invalid("missing project value"))?;
                    if definition.owner != source {
                        return Err(ChannelLoweringError::Invalid(
                            "project expression has a foreign owner",
                        ));
                    }
                    work.step()?;
                    exact_type(&definition.ty, &output.ty, work)?;
                    let input = match definition.kind {
                        ExprKind::Value(value) => {
                            let input = resolve_input(value, child_channels, child_port)?;
                            let source_type = &fragment
                                .values()
                                .get(&value)
                                .ok_or(ChannelLoweringError::Invalid("missing input value"))?
                                .ty;
                            work.step()?;
                            exact_type(&definition.ty, source_type, work)?;
                            // The child port is complete before this node is
                            // visited; a duplicate source was already proved.
                            if child_node.output.columns[input_ordinal(input)?] != value {
                                return Err(ChannelLoweringError::Invalid(
                                    "input representative differs",
                                ));
                            }
                            Some(input)
                        }
                        _ => None,
                    };
                    let publication = record_producer(&mut producers, value, input);
                    // The comparison/publication completed even when it found
                    // an ordinary conflict. Observe that work before returning
                    // it, so an originating control refusal remains primary.
                    work.step()?;
                    publication?;
                    let slot = u32::try_from(next_slot)
                        .map_err(|_| ChannelLoweringError::Invalid("slot identity exhausted"))?;
                    next_slot = next_slot
                        .checked_add(1)
                        .ok_or(ChannelLoweringError::Invalid("slot identity exhausted"))?;
                    slots.push(SlotId::new(slot));
                    port.entry(value).or_insert(ordinal);
                    work.step()?;
                }
                work.flush()?;
                // Vec -> Arc allocation is opaque; the surrounding observations
                // do not claim an allocation grant or internal cooperation.
                let slots: Arc<[SlotId]> = Arc::from(slots);
                work.flush()?;
                (slots, port)
            }
            NodeKind::Filter { predicates } if !predicates.is_empty() => {
                passthrough(fragment, node, &nodes, &ports, work)?
            }
            NodeKind::Limit { .. } => passthrough(fragment, node, &nodes, &ports, work)?,
            NodeKind::Sort {
                mode:
                    novarocks_physical_plan::SortMode::Global
                    | novarocks_physical_plan::SortMode::Analytic { .. },
                ..
            } => passthrough(fragment, node, &nodes, &ports, work)?,
            // A window passes its input occurrences through on their own
            // channels and produces each call's value on a fresh channel.
            NodeKind::Window(spec) => window_channels(
                fragment,
                node,
                spec.expressions.len(),
                &nodes,
                &ports,
                &mut next_slot,
                work,
            )?,
            // Every row-count TopN phase passes its input occurrences through.
            NodeKind::TopN {
                reduction: novarocks_physical_plan::TopNReduction::Rows,
                ..
            } => passthrough(fragment, node, &nodes, &ports, work)?,
            NodeKind::AssertOneRow(spec) => {
                let child = single_child(&node.inputs)?;
                let planned = passthrough(fragment, node, &nodes, &ports, work)?;
                let mut keys = Vec::new();
                if let novarocks_physical_plan::RowCountAssertionSpec::PerKeyAtMostOne {
                    keys: source_keys,
                    ..
                } = spec
                {
                    reserve_vec(&mut keys, source_keys.len(), work)?;
                    for &value in source_keys {
                        let resolved = resolve_input(value, &nodes[&child], &ports[&child]);
                        work.step()?;
                        let resolved = resolved?;
                        let matches = fragment.nodes()[&child]
                            .output
                            .columns
                            .get(input_ordinal(resolved)?)
                            == Some(&value);
                        work.step()?;
                        if !matches {
                            return Err(ChannelLoweringError::Invalid(
                                "assertion key representative differs",
                            ));
                        }
                        keys.push(resolved.slot);
                        work.step()?;
                    }
                }
                assertion_keys.insert(source, keys);
                work.step()?;
                planned
            }
            // Group values and call outputs are produced by the aggregate
            // itself: each output occurrence is a fresh channel, and a
            // repeated group value is one key, so its first ordinal
            // represents it. Group and argument roots read the child port.
            NodeKind::Aggregate { .. }
            | NodeKind::TopN {
                reduction: novarocks_physical_plan::TopNReduction::GroupedStates { .. },
                ..
            } => {
                single_child(&node.inputs)?;
                let mut slots = Vec::new();
                let mut port = Port::new();
                reserve_vec(&mut slots, node.output.columns.len(), work)?;
                for (ordinal, &value) in node.output.columns.iter().enumerate() {
                    if !fragment.values().contains_key(&value) {
                        return Err(ChannelLoweringError::Invalid(
                            "missing Aggregate output value",
                        ));
                    }
                    let slot = u32::try_from(next_slot)
                        .map_err(|_| ChannelLoweringError::Invalid("slot identity exhausted"))?;
                    next_slot = next_slot
                        .checked_add(1)
                        .ok_or(ChannelLoweringError::Invalid("slot identity exhausted"))?;
                    slots.push(SlotId::new(slot));
                    port.entry(value).or_insert(ordinal);
                    work.step()?;
                }
                work.flush()?;
                let slots: Arc<[SlotId]> = Arc::from(slots);
                work.flush()?;
                (slots, port)
            }
            NodeKind::ChangeEventExpand { .. } => {
                single_child(&node.inputs)?;
                let mut slots = Vec::new();
                let mut port = Port::new();
                reserve_vec(&mut slots, node.output.columns.len(), work)?;
                for (ordinal, &value) in node.output.columns.iter().enumerate() {
                    let slot = u32::try_from(next_slot)
                        .map_err(|_| ChannelLoweringError::Invalid("slot identity exhausted"))?;
                    next_slot = next_slot
                        .checked_add(1)
                        .ok_or(ChannelLoweringError::Invalid("slot identity exhausted"))?;
                    slots.push(SlotId::new(slot));
                    port.entry(value).or_insert(ordinal);
                    work.step()?;
                }
                work.flush()?;
                let slots: Arc<[SlotId]> = Arc::from(slots);
                work.flush()?;
                (slots, port)
            }
            NodeKind::Unpivot { .. } => {
                let child = single_child(&node.inputs)?;
                let child_channels = nodes.get(&child).ok_or(ChannelLoweringError::Invalid(
                    "missing lowered Unpivot child",
                ))?;
                let planned = plan_unpivot_channels(
                    fragment,
                    node,
                    UnpivotInputPort {
                        node: &fragment.nodes()[&child],
                        slots: &child_channels.slots,
                        representatives: &ports[&child],
                    },
                    &mut next_slot,
                    work,
                )?;
                unpivot_sources.insert(source, planned.sources);
                work.step()?;
                (planned.slots, planned.port)
            }
            NodeKind::Repeat { .. } => {
                let child = single_child(&node.inputs)?;
                let child_channels = nodes.get(&child).ok_or(ChannelLoweringError::Invalid(
                    "missing lowered Repeat child",
                ))?;
                let planned = plan_repeat_channels(
                    fragment,
                    node,
                    RepeatInputPort {
                        node: &fragment.nodes()[&child],
                        slots: &child_channels.slots,
                        representatives: &ports[&child],
                    },
                    &mut next_slot,
                    work,
                )?;
                (planned.slots, planned.port)
            }
            // A writer produces its multiplex relation and its projected
            // provider input itself: every relation ordinal is a fresh
            // channel. Its projection reads the child's port.
            NodeKind::TableWriter { target } => {
                single_child(&node.inputs)?;
                let projection = fresh_slots(target.input.len(), &mut next_slot, work)?;
                writer_projections.insert(source, projection);
                fresh_relation(fragment, node, &mut next_slot, work)?
            }
            // A finish produces its root relation itself, and its statistics
            // their internal grouping and final channels.
            NodeKind::TableFinish(spec) => {
                single_child(&node.inputs)?;
                let relation = fresh_relation(fragment, node, &mut next_slot, work)?;
                let statistics = fresh_slots(
                    crate::writer_statistics::finish_statistics_slots(spec),
                    &mut next_slot,
                    work,
                )?;
                finish_statistics.insert(source, statistics);
                relation
            }
            // A join produces its canonical output itself; a selection
            // Project, when present, publishes the physical output.
            NodeKind::HashJoin { .. } | NodeKind::NestLoopJoin { .. } => {
                let shape = join_shape
                    .take()
                    .ok_or(ChannelLoweringError::Invalid("missing join shape"))?;
                let (probe, build) = shape.children();
                let side_slots = |child: NodeId| {
                    nodes
                        .get(&child)
                        .map(|channels| Arc::clone(&channels.slots))
                        .ok_or(ChannelLoweringError::Invalid(
                            "join input has not been lowered",
                        ))
                };
                let (probe_slots, build_slots) = (side_slots(probe)?, side_slots(build)?);
                let planned = crate::join::plan_join_channels(
                    node,
                    shape,
                    local,
                    probe_slots,
                    build_slots,
                    &mut next_slot,
                    work,
                )?;
                if let Some(branch) = planned.selection {
                    unions.insert(source, vec![branch]);
                }
                joins.insert(source, planned.planned);
                (planned.slots, planned.port)
            }
            NodeKind::GenerateSeries { .. } => {
                let width = crate::generate_series::resource_bound(node)
                    .ok_or(ChannelLoweringError::Invalid("missing series shape"))?
                    .1;
                let parameter_slots = fresh_slots(width, &mut next_slot, work)?;
                let (slots, port) = fresh_relation(fragment, node, &mut next_slot, work)?;
                series.insert(
                    source,
                    crate::generate_series::PlannedGenerateSeries {
                        bounds: ProgramNodeId::new(local.index().checked_sub(1).ok_or(
                            ChannelLoweringError::Invalid("series bounds schedule underflow"),
                        )?),
                        node: local,
                        parameter_slots,
                    },
                );
                (slots, port)
            }
            // A table function produces its output itself; its argument
            // Project reads the one outer input.
            NodeKind::TableFunction { .. } => {
                let child = single_child(&node.inputs)?;
                let child_channels = nodes.get(&child).ok_or(ChannelLoweringError::Invalid(
                    "table function input has not been lowered",
                ))?;
                let planned = crate::table_function::plan_table_function_channels(
                    fragment,
                    node,
                    local,
                    child_channels,
                    &ports[&child],
                    &mut next_slot,
                    work,
                )?;
                unions.insert(source, vec![planned.selection]);
                table_functions.insert(source, planned.planned);
                (planned.slots, planned.port)
            }
            _ => {
                return Err(ChannelLoweringError::Invalid("unsupported channel node"));
            }
        };
        nodes.insert(source, NodeChannels { local, slots });
        ports.insert(source, port);
        work.step()?;
    }
    let mut inputs = BTreeMap::new();
    let mut join_values = BTreeMap::new();
    for (&id, definition) in fragment.expressions().iter() {
        if let ExprKind::Value(value) = definition.kind {
            let owner =
                fragment
                    .nodes()
                    .get(&definition.owner)
                    .ok_or(ChannelLoweringError::Invalid(
                        "missing value-reference owner",
                    ))?;
            // A join-owned read keeps its side's slot; whether a use reads
            // its side or the join scope is decided per use by its root.
            if let Some(planned) = joins.get(&definition.owner) {
                let (input, source) = crate::join::resolve_join_value(planned, value, &ports)?;
                let source_type = &fragment
                    .values()
                    .get(&value)
                    .ok_or(ChannelLoweringError::Invalid("missing referenced value"))?
                    .ty;
                work.step()?;
                exact_type(&definition.ty, source_type, work)?;
                inputs.insert(id, input);
                join_values.insert(id, source);
                work.step()?;
                continue;
            }
            // A scan-owned root reads the scan's own output port, exactly the
            // input layout that root binding names; every other owner reads
            // its one exact input.
            let scope = if matches!(owner.kind, NodeKind::Scan { .. }) {
                definition.owner
            } else {
                if owner.inputs.len() != 1 {
                    return Err(ChannelLoweringError::Invalid(
                        "value reference requires one exact input",
                    ));
                }
                owner.inputs[0]
            };
            let scope_channels = nodes.get(&scope).ok_or(ChannelLoweringError::Invalid(
                "value input is outside the source tree",
            ))?;
            let input = resolve_input(value, scope_channels, &ports[&scope])?;
            let source_type = &fragment
                .values()
                .get(&value)
                .ok_or(ChannelLoweringError::Invalid("missing referenced value"))?
                .ty;
            work.step()?;
            exact_type(&definition.ty, source_type, work)?;
            inputs.insert(id, input);
        }
        work.step()?;
    }
    Ok(PlannedChannels {
        nodes,
        inputs,
        unpivot_sources,
        assertion_keys,
        unions,
        writer_projections,
        finish_statistics,
        joins,
        join_values,
        table_functions,
        series,
    })
}

/// `count` fresh channels, in order.
fn fresh_slots(
    count: usize,
    next_slot: &mut u64,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Arc<[SlotId]>, ChannelLoweringError> {
    let mut slots = Vec::new();
    reserve_vec(&mut slots, count, work)?;
    for _ in 0..count {
        let slot = u32::try_from(*next_slot)
            .map_err(|_| ChannelLoweringError::Invalid("slot identity exhausted"))?;
        *next_slot = next_slot
            .checked_add(1)
            .ok_or(ChannelLoweringError::Invalid("slot identity exhausted"))?;
        slots.push(SlotId::new(slot));
        work.step()?;
    }
    work.flush()?;
    let slots: Arc<[SlotId]> = Arc::from(slots);
    work.flush()?;
    Ok(slots)
}

/// One fresh channel per output occurrence of a relation the node produces
/// itself. A repeated value is one produced value; its first ordinal
/// represents it.
fn fresh_relation(
    fragment: &novarocks_physical_plan::Fragment,
    node: &novarocks_physical_plan::PhysicalNode,
    next_slot: &mut u64,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Arc<[SlotId]>, Port), ChannelLoweringError> {
    let mut port = Port::new();
    for (ordinal, value) in node.output.columns.iter().enumerate() {
        if !fragment.values().contains_key(value) {
            return Err(ChannelLoweringError::Invalid(
                "missing writer relation value",
            ));
        }
        port.entry(*value).or_insert(ordinal);
        work.step()?;
    }
    Ok((
        fresh_slots(node.output.columns.len(), next_slot, work)?,
        port,
    ))
}

/// A window's output is its whole input followed by one value per call.
fn window_channels(
    fragment: &novarocks_physical_plan::Fragment,
    node: &novarocks_physical_plan::PhysicalNode,
    calls: usize,
    nodes: &BTreeMap<NodeId, NodeChannels>,
    ports: &BTreeMap<NodeId, Port>,
    next_slot: &mut u64,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Arc<[SlotId]>, Port), ChannelLoweringError> {
    let child = single_child(&node.inputs)?;
    let child_node = &fragment.nodes()[&child];
    let width = child_node.output.columns.len();
    if width.checked_add(calls) != Some(node.output.columns.len()) {
        return Err(ChannelLoweringError::Invalid(
            "window output is not its input followed by its calls",
        ));
    }
    for (actual, expected) in node.output.columns.iter().zip(&child_node.output.columns) {
        let same = actual == expected;
        work.step()?;
        if !same {
            return Err(ChannelLoweringError::Invalid(
                "window pass-through occurrence order differs",
            ));
        }
    }
    let produced = fresh_slots(calls, next_slot, work)?;
    let mut slots = Vec::new();
    reserve_vec(&mut slots, node.output.columns.len(), work)?;
    slots.extend(nodes[&child].slots.iter().copied());
    slots.extend(produced.iter().copied());
    let mut port = Port::new();
    for (&value, &ordinal) in &ports[&child] {
        port.insert(value, ordinal);
        work.step()?;
    }
    for (ordinal, &value) in node.output.columns.iter().enumerate().skip(width) {
        if !fragment.values().contains_key(&value) {
            return Err(ChannelLoweringError::Invalid("missing window output value"));
        }
        port.entry(value).or_insert(ordinal);
        work.step()?;
    }
    work.flush()?;
    let slots: Arc<[SlotId]> = Arc::from(slots);
    work.flush()?;
    Ok((slots, port))
}

fn single_child(inputs: &[NodeId]) -> Result<NodeId, ChannelLoweringError> {
    if inputs.len() != 1 {
        return Err(ChannelLoweringError::Invalid(
            "operator requires one exact input",
        ));
    }
    Ok(inputs[0])
}

fn passthrough(
    fragment: &novarocks_physical_plan::Fragment,
    node: &novarocks_physical_plan::PhysicalNode,
    nodes: &BTreeMap<NodeId, NodeChannels>,
    ports: &BTreeMap<NodeId, Port>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Arc<[SlotId]>, Port), ChannelLoweringError> {
    let child = single_child(&node.inputs)?;
    let child_node = &fragment.nodes()[&child];
    if node.output.columns.len() != child_node.output.columns.len() {
        return Err(ChannelLoweringError::Invalid(
            "passthrough occurrence width differs",
        ));
    }
    for (actual, expected) in node.output.columns.iter().zip(&child_node.output.columns) {
        let same = actual == expected;
        work.step()?;
        if !same {
            return Err(ChannelLoweringError::Invalid(
                "passthrough occurrence order differs",
            ));
        }
    }
    let mut port = Port::new();
    for (&value, &ordinal) in &ports[&child] {
        port.insert(value, ordinal);
        work.step()?;
    }
    Ok((Arc::clone(&nodes[&child].slots), port))
}

pub(crate) fn resolve_input(
    value: ValueId,
    channels: &NodeChannels,
    port: &Port,
) -> Result<ResolvedInput, ChannelLoweringError> {
    let ordinal = *port.get(&value).ok_or(ChannelLoweringError::Invalid(
        "value is outside its owner input",
    ))?;
    let slot = *channels
        .slots
        .get(ordinal)
        .ok_or(ChannelLoweringError::Invalid(
            "input occurrence has no slot",
        ))?;
    let ordinal = u32::try_from(ordinal)
        .map_err(|_| ChannelLoweringError::Invalid("input ordinal exhausted"))?;
    Ok(ResolvedInput {
        slot,
        source: ProgramChannelSite::Layout {
            node: channels.local,
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal,
        },
    })
}

fn input_ordinal(input: ResolvedInput) -> Result<usize, ChannelLoweringError> {
    match input.source {
        ProgramChannelSite::Layout { ordinal, .. } => usize::try_from(ordinal)
            .map_err(|_| ChannelLoweringError::Invalid("input ordinal exceeds host range")),
        _ => Err(ChannelLoweringError::Invalid(
            "input source is not an output occurrence",
        )),
    }
}

fn record_producer(
    producers: &mut BTreeMap<ValueId, Option<ResolvedInput>>,
    value: ValueId,
    input: Option<ResolvedInput>,
) -> Result<(), ChannelLoweringError> {
    if let Some(previous) = producers.get(&value) {
        if input.is_none() || previous != &input {
            return Err(ChannelLoweringError::Invalid(
                "independent project roots share a produced value",
            ));
        }
    } else {
        producers.insert(value, input);
    }
    Ok(())
}

fn exact_type(
    left: &FunctionValueType,
    right: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ChannelLoweringError> {
    work.flush()?;
    let equal = left.exactly_equals_observed::<ChannelLoweringError>(right, || {
        // The shared type walk observes before borrowed operations. Zero-unit
        // observations preserve control without claiming completed work.
        work.control()
            .checkpoint(CompilePhase::LowerProgram, 0)
            .map_err(ChannelLoweringError::Control)
    })?;
    work.flush()?;
    if equal {
        Ok(())
    } else {
        Err(ChannelLoweringError::Invalid("complete value types differ"))
    }
}
