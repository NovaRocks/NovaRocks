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

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::{
    AggregatePhase, EdgeId, ExprId, Fragment, FragmentId, NodeId, NodeKind, PhysicalNode, ValueId,
};

use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, ControlOwnedResourceFacts, ControlResourceCounter,
    ControlResourceError, control_resource_add, control_resource_mul,
    owned_resources::{
        copy::reserve_exit,
        layout::{LayoutResourceError, arc_layout},
        vec::boxed_slice_in,
    },
};
use std::alloc::Layout;

#[derive(Clone, Debug, Default)]
pub(crate) struct ValuePortIndex {
    pub(crate) occurrences: BTreeMap<ValueId, usize>,
}

struct IndexAdmission<'a, 'control> {
    counter: &'a mut ControlResourceCounter,
    admit: &'a mut dyn FnMut(&ControlOwnedResourceFacts) -> Result<(), CompileControlError>,
    work: &'a mut CompileCheckpoints<'control>,
}
impl IndexAdmission<'_, '_> {
    fn gate(&mut self) -> Result<(), ControlResourceError> {
        (self.admit)(&self.counter.facts())?;
        Ok(())
    }
    fn reserve<T>(
        &mut self,
        values: &mut Vec<T>,
        count: usize,
    ) -> Result<(), ControlResourceError> {
        if count != 0 {
            self.work.flush()?;
            let reserved = values.try_reserve_exact(count);
            if reserved.is_ok() {
                self.work.step()?;
            }
            reserve_exit::<ControlResourceError>(reserved, self.work)?;
        }
        Ok(())
    }
}

impl ValuePortIndex {
    pub(crate) fn new(values: &[ValueId]) -> Self {
        Self::new_core(values, None).expect("plain value-port indexing is infallible")
    }

    fn new_core(
        values: &[ValueId],
        mut admission: Option<&mut IndexAdmission<'_, '_>>,
    ) -> Result<Self, ControlResourceError> {
        if let Some(owner) = admission.as_deref_mut() {
            owner.counter.tree::<ValueId, usize>(values.len())?;
            owner.gate()?;
        }
        Self::build_core(values, admission)
    }

    fn build_core(
        values: &[ValueId],
        mut admission: Option<&mut IndexAdmission<'_, '_>>,
    ) -> Result<Self, ControlResourceError> {
        let mut occurrences = BTreeMap::new();
        for value in values {
            if let Some(owner) = admission.as_deref_mut() {
                owner.work.flush()?;
            }
            *occurrences.entry(*value).or_default() += 1;
            if let Some(owner) = admission.as_deref_mut() {
                owner.work.step()?;
                owner.work.flush()?;
            }
        }
        Ok(Self { occurrences })
    }

    pub(crate) fn contains(&self, value: &ValueId) -> bool {
        self.occurrences.contains_key(value)
    }
}

pub(crate) struct FragmentValidationIndexes {
    pub(crate) output_ports: BTreeMap<NodeId, Arc<ValuePortIndex>>,
    pub(crate) visible_inputs: BTreeMap<NodeId, VisibleInputIndex>,
    pub(crate) projected_inputs: BTreeMap<ValueId, (NodeId, usize)>,
}

pub(crate) enum VisibleInputIndex {
    Empty,
    One(Arc<ValuePortIndex>),
    Many(Box<[Arc<ValuePortIndex>]>),
}

impl VisibleInputIndex {
    pub(crate) fn contains(&self, value: &ValueId) -> bool {
        match self {
            Self::Empty => false,
            Self::One(port) => port.contains(value),
            Self::Many(ports) => ports.iter().any(|port| port.contains(value)),
        }
    }
}

impl FragmentValidationIndexes {
    pub(crate) fn new(fragment: &Fragment) -> Self {
        Self::new_core(fragment, None).expect("plain fragment indexing is infallible")
    }

    /// Quantity capture on the caller's original scope. This does not prove
    /// fragment structure, allocator admission or arbitrary element cleanup.
    pub(crate) fn new_in(
        fragment: &Fragment,
        counter: &mut ControlResourceCounter,
        admit: &mut dyn FnMut(&ControlOwnedResourceFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ControlResourceError> {
        Self::new_core(
            fragment,
            Some(IndexAdmission {
                counter,
                admit,
                work,
            }),
        )
    }

    fn new_core(
        fragment: &Fragment,
        mut admission: Option<IndexAdmission<'_, '_>>,
    ) -> Result<Self, ControlResourceError> {
        let output_ports = if let Some(owner) = admission.as_mut() {
            let count = fragment.nodes().len();
            owner.counter.tree::<NodeId, Arc<ValuePortIndex>>(count)?;
            owner.counter.tree::<NodeId, VisibleInputIndex>(count)?;
            let arc = arc_layout(Layout::new::<ValuePortIndex>()).map_err(|error| match error {
                LayoutResourceError::SourceModel => {
                    ControlResourceError::SourceModel("Index Arc source model drift")
                }
                _ => CompileControlError::ResourceExhausted.into(),
            })?;
            owner.counter.layout(arc, count)?;
            // Two source traversals, including each terminal lookup, use the
            // sole locked tree work bound. No source B or maximum ID is used.
            owner.counter.work(control_resource_mul(
                control_resource_mul(control_resource_add(count, 1)?, 2)?,
                ControlResourceCounter::lookup_work(count)?,
            )?)?;
            owner.gate()?;
            let mut ports = BTreeMap::new();
            for node in fragment.nodes().values() {
                let port = ValuePortIndex::new_core(&node.output.columns, Some(owner))?;
                owner.work.flush()?;
                let port = Arc::new(port);
                owner.work.step()?;
                owner.work.flush()?;
                // Source map keys need not equal node.id in an invalid plan.
                // Explicit insertion preserves the original last winner.
                ports.insert(node.id, port);
                owner.work.step()?;
                owner.work.flush()?;
            }
            ports
        } else {
            // Preserve the original Plain bulk-build/sort strategy.
            fragment
                .nodes()
                .values()
                .map(|node| (node.id, Arc::new(ValuePortIndex::new(&node.output.columns))))
                .collect::<BTreeMap<_, _>>()
        };
        let mut visible_inputs = BTreeMap::new();
        for node in fragment.nodes().values() {
            let visible = Self::visible_core(node, &output_ports, admission.as_mut())?;
            if let Some(owner) = admission.as_mut() {
                owner.work.flush()?;
            }
            visible_inputs.insert(node.id, visible);
            if let Some(owner) = admission.as_mut() {
                owner.work.step()?;
                owner.work.flush()?;
            }
        }
        let mut projected_count = 0;
        for node in fragment.nodes().values() {
            if let NodeKind::TableWriter { target } = &node.kind {
                projected_count =
                    control_resource_add(projected_count, target.target_fields.len())?;
            }
            if let Some(owner) = admission.as_mut() {
                owner.work.step()?;
            }
        }
        if let Some(owner) = admission.as_mut() {
            owner
                .counter
                .tree::<ValueId, (NodeId, usize)>(projected_count)?;
            owner.counter.work(control_resource_mul(
                projected_count,
                ControlResourceCounter::lookup_work(projected_count)?,
            )?)?;
            owner.gate()?;
        }
        let mut projected_inputs = BTreeMap::new();
        for node in fragment.nodes().values() {
            if let NodeKind::TableWriter { target } = &node.kind {
                for field in &target.target_fields {
                    if let Some(owner) = admission.as_mut() {
                        owner.work.flush()?;
                    }
                    let entry = projected_inputs
                        .entry(field.projected)
                        .or_insert((node.id, 0));
                    entry.1 += 1;
                    if let Some(owner) = admission.as_mut() {
                        owner.work.step()?;
                    }
                }
            }
            if let Some(owner) = admission.as_mut() {
                owner.work.step()?;
            }
        }
        Ok(Self {
            output_ports,
            visible_inputs,
            projected_inputs,
        })
    }

    fn visible_core(
        node: &PhysicalNode,
        output_ports: &BTreeMap<NodeId, Arc<ValuePortIndex>>,
        mut admission: Option<&mut IndexAdmission<'_, '_>>,
    ) -> Result<VisibleInputIndex, ControlResourceError> {
        // A scan reads both provider columns and its original derived values.
        let visible = if let NodeKind::Scan {
            provider_outputs,
            derived_values,
            ..
        } = &node.kind
        {
            let values = if let Some(owner) = admission.as_deref_mut() {
                let count = control_resource_add(provider_outputs.len(), derived_values.len())?;
                owner.counter.buffer::<ValueId>(count, 1)?;
                owner.counter.tree::<ValueId, usize>(count)?;
                owner.counter.arc::<ValuePortIndex>(1)?;
                owner.counter.work(count)?;
                owner.gate()?;
                let mut values = Vec::new();
                owner.reserve(&mut values, count)?;
                for value in provider_outputs
                    .iter()
                    .map(|(_, value)| *value)
                    .chain(derived_values.iter().copied())
                {
                    values.push(value);
                    owner.work.step()?;
                }
                values
            } else {
                provider_outputs
                    .iter()
                    .map(|(_, value)| *value)
                    .chain(derived_values.iter().copied())
                    .collect::<Vec<_>>()
            };
            let port = if admission.is_some() {
                ValuePortIndex::build_core(&values, admission.as_deref_mut())?
            } else {
                ValuePortIndex::new(&values)
            };
            if let Some(owner) = admission.as_deref_mut() {
                owner.work.flush()?;
            }
            let port = Arc::new(port);
            if let Some(owner) = admission.as_deref_mut() {
                owner.work.step()?;
                owner.work.flush()?;
            }
            VisibleInputIndex::One(port)
        } else {
            let ports = if let Some(owner) = admission.as_deref_mut() {
                let count = node.inputs.len();
                owner.counter.buffer::<Arc<ValuePortIndex>>(count, 1)?;
                owner.counter.work(control_resource_mul(
                    count,
                    ControlResourceCounter::lookup_work(output_ports.len())?,
                )?)?;
                // Each possible matched handle clone/drop and the One branch's
                // additional clone/drop are closed Arc operations, not backing.
                owner
                    .counter
                    .work(control_resource_add(control_resource_mul(count, 8)?, 4)?)?;
                owner.gate()?;
                let mut ports = Vec::new();
                owner.reserve(&mut ports, count)?;
                for input in &node.inputs {
                    owner.work.flush()?;
                    let port = output_ports.get(input).cloned();
                    owner.work.step()?;
                    owner.work.flush()?;
                    if let Some(port) = port {
                        ports.push(port);
                        owner.work.step()?;
                    }
                }
                ports
            } else {
                node.inputs
                    .iter()
                    .filter_map(|input| output_ports.get(input).cloned())
                    .collect::<Vec<_>>()
            };
            match ports.as_slice() {
                [] => VisibleInputIndex::Empty,
                [port] => {
                    let port = port.clone();
                    if let Some(owner) = admission.as_deref_mut() {
                        owner.work.step()?;
                    }
                    VisibleInputIndex::One(port)
                }
                _ => {
                    if let Some(owner) = admission {
                        VisibleInputIndex::Many(boxed_slice_in::<_, ControlResourceError>(
                            ports,
                            &mut |facts| {
                                if let Some(layout) = facts.requested_backing {
                                    owner.counter.layout(layout, 1)?;
                                }
                                (owner.admit)(&owner.counter.facts())?;
                                Ok(())
                            },
                            owner.work,
                        )?)
                    } else {
                        VisibleInputIndex::Many(ports.into_boxed_slice())
                    }
                }
            }
        };
        Ok(visible)
    }

    pub(crate) fn output(&self, node: NodeId) -> Option<&ValuePortIndex> {
        self.output_ports.get(&node).map(Arc::as_ref)
    }

    pub(crate) fn visible_input(&self, node: NodeId) -> Option<&VisibleInputIndex> {
        self.visible_inputs.get(&node)
    }
}

#[cfg(test)]
#[path = "index_borrowed_tests.rs"]
mod index_borrowed_tests;

#[derive(Clone, Debug, Default)]
pub(crate) struct ValueMappingIndex {
    pub(crate) by_destination: BTreeMap<ValueId, BTreeMap<Option<ValueId>, usize>>,
}

impl ValueMappingIndex {
    pub(crate) fn from_pairs(mapping: &[(ValueId, ValueId)]) -> Self {
        Self::from_pairs_iter(mapping.iter().copied())
    }

    pub(crate) fn from_pairs_iter(mapping: impl IntoIterator<Item = (ValueId, ValueId)>) -> Self {
        let mut index = Self::default();
        for (source, destination) in mapping {
            index.insert(Some(source), destination);
        }
        index
    }

    pub(crate) fn insert(&mut self, source: Option<ValueId>, destination: ValueId) {
        *self
            .by_destination
            .entry(destination)
            .or_default()
            .entry(source)
            .or_default() += 1;
    }

    pub(crate) fn contains(&self, source: ValueId, destination: ValueId) -> bool {
        self.by_destination
            .get(&destination)
            .is_some_and(|sources| sources.contains_key(&Some(source)))
    }

    pub(crate) fn resolve(
        &self,
        destination: ValueId,
        allow_identical_duplicates: bool,
    ) -> Option<ValueId> {
        let sources = self.by_destination.get(&destination)?;
        if sources.len() != 1 {
            return None;
        }
        let (source, occurrences) = sources.first_key_value()?;
        if !allow_identical_duplicates && *occurrences != 1 {
            return None;
        }
        *source
    }
}

pub(crate) struct SemanticTraceWorkBudget {
    pub(crate) remaining: usize,
}

impl SemanticTraceWorkBudget {
    pub(crate) const fn new(limits: &PlanLimits) -> Self {
        Self {
            remaining: limits.plan_semantic_trace_work,
        }
    }

    pub(crate) fn charge(&mut self, work: usize) -> bool {
        let Some(remaining) = self.remaining.checked_sub(work) else {
            return false;
        };
        self.remaining = remaining;
        true
    }
}

#[derive(Default)]
pub(crate) struct SemanticTraceIndexes {
    pub(crate) ports: BTreeMap<(FragmentId, NodeId), ValuePortIndex>,
    pub(crate) edges: BTreeMap<EdgeId, ValueMappingIndex>,
    pub(crate) projects: BTreeMap<(FragmentId, NodeId), ValueMappingIndex>,
    pub(crate) unions: BTreeMap<(FragmentId, NodeId, usize), ValueMappingIndex>,
    pub(crate) aggregate_sequences:
        BTreeMap<(FragmentId, NodeId), BTreeMap<crate::AggregateSequenceId, Option<usize>>>,
}

#[derive(Default)]
pub(crate) struct RuntimeFilterLineageIndexes {
    pub(crate) parents: BTreeMap<FragmentId, BTreeMap<NodeId, Option<NodeId>>>,
    pub(crate) ports: BTreeMap<(FragmentId, NodeId), ValuePortIndex>,
    pub(crate) apply_ports:
        BTreeMap<(FragmentId, NodeId, RuntimeFilterApplyPortKey), Option<ValuePortIndex>>,
    pub(crate) scan_provider_ports: BTreeMap<(FragmentId, NodeId), ValuePortIndex>,
    pub(crate) build_frontiers: BTreeMap<(FragmentId, NodeId), RuntimeFilterFrontierIndex>,
}

pub(crate) struct RuntimeFilterFrontierIndex {
    pub(crate) build: BTreeSet<EdgeId>,
    pub(crate) non_build: BTreeSet<EdgeId>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum RuntimeFilterApplyPortKey {
    NodeInput(u32),
    NodeOutput,
    ScanSource,
}

impl From<crate::RuntimeFilterApplyPoint> for RuntimeFilterApplyPortKey {
    fn from(value: crate::RuntimeFilterApplyPoint) -> Self {
        match value {
            crate::RuntimeFilterApplyPoint::NodeInput { input_ordinal } => {
                Self::NodeInput(input_ordinal)
            }
            crate::RuntimeFilterApplyPoint::NodeOutput => Self::NodeOutput,
            crate::RuntimeFilterApplyPoint::ScanSource => Self::ScanSource,
        }
    }
}

impl RuntimeFilterLineageIndexes {
    pub(crate) fn apply_port_contains_all(
        &mut self,
        fragment: &Fragment,
        node: &PhysicalNode,
        apply_point: crate::RuntimeFilterApplyPoint,
        values: &[ValueId],
    ) -> Option<bool> {
        if apply_point == crate::RuntimeFilterApplyPoint::ScanSource {
            return matches!(node.kind, NodeKind::Scan { .. }).then(|| {
                values
                    .iter()
                    .all(|value| self.scan_provider_contains(fragment, node, *value))
            });
        }
        let port = self
            .apply_ports
            .entry((fragment.id(), node.id, apply_point.into()))
            .or_insert_with(|| {
                let available = match apply_point {
                    crate::RuntimeFilterApplyPoint::NodeInput { input_ordinal } => {
                        usize::try_from(input_ordinal)
                            .ok()
                            .and_then(|ordinal| node.inputs.get(ordinal))
                            .and_then(|input| fragment.nodes().get(input))
                            .map(|input| input.output.columns.as_ref())
                    }
                    crate::RuntimeFilterApplyPoint::NodeOutput => {
                        Some(node.output.columns.as_ref())
                    }
                    crate::RuntimeFilterApplyPoint::ScanSource => unreachable!(),
                };
                available.map(ValuePortIndex::new)
            })
            .as_ref()?;
        Some(values.iter().all(|value| port.contains(value)))
    }

    pub(crate) fn scan_provider_contains(
        &mut self,
        fragment: &Fragment,
        node: &PhysicalNode,
        value: ValueId,
    ) -> bool {
        let NodeKind::Scan {
            provider_outputs, ..
        } = &node.kind
        else {
            return false;
        };
        self.scan_provider_ports
            .entry((fragment.id(), node.id))
            .or_insert_with(|| {
                ValuePortIndex::new(
                    &provider_outputs
                        .iter()
                        .map(|(_, value)| *value)
                        .collect::<Vec<_>>(),
                )
            })
            .contains(&value)
    }

    pub(crate) fn build_frontier(
        &mut self,
        fragment: &Fragment,
        root: NodeId,
        inbound_edges: &BTreeSet<EdgeId>,
    ) -> &RuntimeFilterFrontierIndex {
        self.build_frontiers
            .entry((fragment.id(), root))
            .or_insert_with(|| {
                let build = collect_subtree_exchange_edges(fragment, root);
                let non_build = inbound_edges.difference(&build).copied().collect();
                RuntimeFilterFrontierIndex { build, non_build }
            })
    }

    pub(crate) fn port_contains(
        &mut self,
        fragment: &Fragment,
        node: &PhysicalNode,
        value: ValueId,
    ) -> bool {
        self.ports
            .entry((fragment.id(), node.id))
            .or_insert_with(|| ValuePortIndex::new(&node.output.columns))
            .contains(&value)
    }
}

impl SemanticTraceIndexes {
    pub(crate) fn aggregate_sequence_call<'a>(
        &mut self,
        fragment: FragmentId,
        node: NodeId,
        calls: &'a [crate::AggregateCall],
        sequence: crate::AggregateSequenceId,
        budget: &mut SemanticTraceWorkBudget,
    ) -> Option<&'a crate::AggregateCall> {
        let key = (fragment, node);
        if let Entry::Vacant(entry) = self.aggregate_sequences.entry(key) {
            if !budget.charge(calls.len()) {
                return None;
            }
            let mut by_sequence = BTreeMap::new();
            for (ordinal, call) in calls.iter().enumerate() {
                let Some(call_sequence) = call.binding.phase.sequence() else {
                    continue;
                };
                if matches!(call.binding.phase, AggregatePhase::Final { .. }) {
                    continue;
                }
                match by_sequence.entry(call_sequence) {
                    Entry::Vacant(entry) => {
                        entry.insert(Some(ordinal));
                    }
                    Entry::Occupied(mut entry) => {
                        entry.insert(None);
                    }
                }
            }
            entry.insert(by_sequence);
        }
        let ordinal = self
            .aggregate_sequences
            .get(&key)?
            .get(&sequence)?
            .as_ref()?;
        calls.get(*ordinal)
    }

    pub(crate) fn ensure_port(
        &mut self,
        key: (FragmentId, NodeId),
        values: &[ValueId],
        budget: &mut SemanticTraceWorkBudget,
    ) -> bool {
        if let Entry::Vacant(entry) = self.ports.entry(key) {
            if !budget.charge(values.len()) {
                return false;
            }
            entry.insert(ValuePortIndex::new(values));
        }
        true
    }

    pub(crate) fn map_edge_values(
        &mut self,
        edge: EdgeId,
        mapping: &[(ValueId, ValueId)],
        values: &[ValueId],
        allow_identical_duplicates: bool,
        budget: &mut SemanticTraceWorkBudget,
    ) -> Option<Vec<ValueId>> {
        if let Entry::Vacant(entry) = self.edges.entry(edge) {
            if !budget.charge(mapping.len()) {
                return None;
            }
            entry.insert(ValueMappingIndex::from_pairs(mapping));
        }
        if !budget.charge(values.len()) {
            return None;
        }
        let index = self.edges.get(&edge)?;
        values
            .iter()
            .map(|value| index.resolve(*value, allow_identical_duplicates))
            .collect()
    }

    pub(crate) fn map_union_values(
        &mut self,
        fragment: FragmentId,
        node: NodeId,
        input_ordinal: usize,
        outputs: &[ValueId],
        input: &[ValueId],
        values: &[ValueId],
        allow_identical_duplicates: bool,
        budget: &mut SemanticTraceWorkBudget,
    ) -> Option<Vec<ValueId>> {
        if outputs.len() != input.len() {
            return None;
        }
        let key = (fragment, node, input_ordinal);
        if let Entry::Vacant(entry) = self.unions.entry(key) {
            if !budget.charge(outputs.len()) {
                return None;
            }
            let mut index = ValueMappingIndex::default();
            for (destination, source) in outputs.iter().copied().zip(input.iter().copied()) {
                index.insert(Some(source), destination);
            }
            entry.insert(index);
        }
        if !budget.charge(values.len()) {
            return None;
        }
        let index = self.unions.get(&key)?;
        values
            .iter()
            .map(|value| index.resolve(*value, allow_identical_duplicates))
            .collect()
    }

    pub(crate) fn map_project_values(
        &mut self,
        fragment: &Fragment,
        node: &PhysicalNode,
        child: &PhysicalNode,
        expressions: &[(ExprId, ValueId)],
        values: &[ValueId],
        budget: &mut SemanticTraceWorkBudget,
    ) -> Option<Vec<ValueId>> {
        let child_key = (fragment.id(), child.id);
        if !self.ensure_port(child_key, &child.output.columns, budget) {
            return None;
        }
        let project_key = (fragment.id(), node.id);
        if let Entry::Vacant(entry) = self.projects.entry(project_key) {
            if !budget.charge(expressions.len()) {
                return None;
            }
            let mut index = ValueMappingIndex::default();
            for (expression, output) in expressions {
                index.insert(
                    crate::expression_value(fragment.expressions(), *expression),
                    *output,
                );
            }
            entry.insert(index);
        }
        if !budget.charge(values.len()) {
            return None;
        }
        let child_values = self.ports.get(&child_key)?;
        let expression_sources = self.projects.get(&project_key)?;
        values
            .iter()
            .map(|expected| {
                if child_values.contains(expected) {
                    Some(*expected)
                } else {
                    expression_sources.resolve(*expected, false)
                }
            })
            .collect()
    }
}

pub(crate) fn bounded_count(
    errors: &mut ValidationContext,
    path: &str,
    actual: usize,
    maximum: usize,
) {
    if actual > maximum {
        errors.push(ValidationError::resource_limit(
            path,
            format!("contains {actual} items, exceeding {maximum}"),
        ));
    }
}
