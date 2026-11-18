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

//! Borrowed structural facts for a provider-domain q and its preserved p sites.
//! This is deliberately not pruning authorization. Implication, complete
//! accurate effects, legal relational transforms and global consumer coverage
//! must be checked by the FE before a validated pruning consumer can exist.

use crate::{
    ExprKind, FragmentId, FragmentPackage, NodeId, NodeKind, PredicateConjunctSource,
    PredicateResponsibilityRef, PredicateSourceError, ProviderReadOccurrenceId, ValueId,
    ValueOrigin,
};
use novarocks_connector_contract::{FrozenConnectorRead, ScanColumnId, TupleDomain};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ExpressionEffectContext,
    MAX_CONTROL_DEPTH, MAX_CONTROL_USE_REFERENCES, PureCompileControl,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

/// A real field of the exact frozen ConnectorScan, not an orphan side table.
/// Enforced describes provider responsibility for q, never permission to remove
/// a different original p. Existing scan-owned Exact(p) keeps its own contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PruningDomainField {
    Enforced,
    Unenforced,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PruningDomainSite {
    pub fragment: FragmentId,
    pub scan: NodeId,
    pub occurrence: ProviderReadOccurrenceId,
    pub field: PruningDomainField,
}

/// A consumer input occurrence. Two inputs of one parent are two consumers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PruningInputEdge {
    pub consumer: NodeId,
    pub input_ordinal: u32,
    pub producer: NodeId,
}

/// Ordered source-to-scan value identities, one per path node. Aliases and
/// NULL extensions must be actual immutable node fields; their presence says
/// nothing about whether moving/copying q through that operator is legal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PruningColumnTrace {
    pub column: ScanColumnId,
    pub values: Box<[ValueId]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PruningSourceWitness {
    /// Exact original p root; this is a reference, not movement permission.
    pub responsibility: PredicateResponsibilityRef,
    /// The claimed selected conjunct occurrence, including demand and domain.
    /// Reconstructed from the actual root/path before any structural use.
    pub context: ExpressionEffectContext,
    pub conjunct_path: Box<[u32]>,
    pub input_path: Box<[PruningInputEdge]>,
    pub columns: Box<[PruningColumnTrace]>,
}

/// All sources are in this local snapshot. Cross-fragment implication and
/// placement require the complete FE plan, not remote proof loaded by the BE.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PruningDomainWitness {
    pub target: PruningDomainSite,
    pub sources: Box<[PruningSourceWitness]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PruningStructureError {
    Control(CompileControlError),
    Source(PredicateSourceError),
    TooLarge,
    EmptySources,
    InvalidFragment,
    InvalidScan,
    InvalidOccurrence,
    WrongResponsibility,
    WrongContext,
    WrongSnapshot,
    InvalidPath,
    SharedLocalProducer,
    DuplicateSource,
    InvalidColumn,
    InvalidTransport,
    MissingDomainColumn,
}
impl fmt::Display for PruningStructureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid pruning structure: {self:?}")
    }
}
impl std::error::Error for PruningStructureError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            Self::Source(source) => Some(source),
            _ => None,
        }
    }
}
impl From<CompileControlError> for PruningStructureError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<PredicateSourceError> for PruningStructureError {
    fn from(error: PredicateSourceError) -> Self {
        match error {
            PredicateSourceError::Control(error) => Self::Control(error),
            error => Self::Source(error),
        }
    }
}

/// Same-snapshot borrowed structure only. There is no execute/prune method or
/// conversion to a validated semantic receipt. A stronger arbitrary q, an
/// observable Project or an illegal outer-join path may be structurally sound;
/// that must never make them executable. The later finite-rule checker must
/// recheck implication, accurate effects, multiplicity/NULL/required work and
/// complete FE consumers. Local edge uniqueness is not global coverage.
#[derive(Debug)]
pub struct PruningDomainStructure<'package, 'witness> {
    package: &'package FragmentPackage,
    witness: &'witness PruningDomainWitness,
    read: &'package FrozenConnectorRead,
    domain: &'package TupleDomain<ScanColumnId>,
    sources: Vec<PredicateConjunctSource>,
}
impl<'package, 'witness> PruningDomainStructure<'package, 'witness> {
    pub fn try_new(
        package: &'package FragmentPackage,
        witness: &'witness PruningDomainWitness,
        control: &dyn PureCompileControl,
    ) -> Result<Self, PruningStructureError> {
        let mut work = PruningWork::try_new(control)?;
        let index = PruningConsumerIndex::try_new(package, &mut work)?;
        let checked = Self::try_new_indexed(package, witness, &index, control, &mut work)?;
        work.finish()?;
        Ok(checked)
    }

    /// Borrow the caller's observation scope while retaining the original
    /// combined structural fuel. This establishes only same-snapshot shape.
    pub fn try_new_in(
        package: &'package FragmentPackage,
        witness: &'witness PruningDomainWitness,
        observed: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, PruningStructureError> {
        let control = observed.control();
        let mut work = PruningWork::borrowed(observed);
        let index = PruningConsumerIndex::try_new(package, &mut work)?;
        Self::try_new_indexed(package, witness, &index, control, &mut work)
    }

    pub(crate) fn try_new_indexed(
        package: &'package FragmentPackage,
        witness: &'witness PruningDomainWitness,
        index: &PruningConsumerIndex<'_>,
        control: &dyn PureCompileControl,
        work: &mut PruningWork<'_, '_>,
    ) -> Result<Self, PruningStructureError> {
        if !std::ptr::eq(index.package, package) {
            return Err(PruningStructureError::WrongSnapshot);
        }
        preflight(witness, work)?;
        let fragment = package.fragment();
        if witness.target.fragment != fragment.id() {
            return Err(PruningStructureError::InvalidFragment);
        }
        let node = fragment
            .nodes()
            .get(&witness.target.scan)
            .ok_or(PruningStructureError::InvalidScan)?;
        let NodeKind::Scan {
            occurrence,
            provider_outputs,
            ..
        } = &node.kind
        else {
            return Err(PruningStructureError::InvalidScan);
        };
        if *occurrence != witness.target.occurrence {
            return Err(PruningStructureError::InvalidOccurrence);
        }
        let read = package
            .scans()
            .get(&node.id)
            .ok_or(PruningStructureError::InvalidScan)?;
        let domain = match witness.target.field {
            PruningDomainField::Enforced => read.scan().enforced_predicate(),
            PruningDomainField::Unenforced => read.scan().unenforced_predicate(),
        };
        let mut sources = Vec::with_capacity(witness.sources.len());
        let mut source_sites = BTreeSet::new();
        let mut expected_inputs = BTreeMap::new();
        let mut domain_columns = BTreeSet::new();
        for source in &witness.sources {
            let mut conjunct_path = Vec::with_capacity(source.conjunct_path.len());
            for ordinal in &source.conjunct_path {
                work.step()?;
                conjunct_path.push(*ordinal);
            }
            let checked = if work.is_borrowed() {
                PredicateConjunctSource::try_new_in(
                    fragment,
                    source.responsibility.site,
                    conjunct_path,
                    work.observed_mut(),
                )?
            } else {
                PredicateConjunctSource::try_new(
                    fragment,
                    source.responsibility.site,
                    conjunct_path,
                    control,
                )?
            };
            if checked.responsibility().anchor() != source.responsibility {
                return Err(PruningStructureError::WrongResponsibility);
            }
            if checked.context() != source.context {
                return Err(PruningStructureError::WrongContext);
            }
            if !source_sites.insert(checked.context().use_id) {
                return Err(PruningStructureError::DuplicateSource);
            }
            let mut current = source.responsibility.site.node;
            for edge in &source.input_path {
                if edge.consumer != current || edge.producer == current {
                    return Err(PruningStructureError::InvalidPath);
                }
                let consumer = fragment
                    .nodes()
                    .get(&current)
                    .ok_or(PruningStructureError::InvalidPath)?;
                if consumer.inputs.get(edge.input_ordinal as usize) != Some(&edge.producer) {
                    return Err(PruningStructureError::InvalidPath);
                }
                if let Some(previous) = expected_inputs.insert(edge.producer, *edge)
                    && previous != *edge
                {
                    return Err(PruningStructureError::SharedLocalProducer);
                }
                current = edge.producer;
                work.step()?;
            }
            if current != node.id {
                return Err(PruningStructureError::InvalidPath);
            }
            let mut previous = None;
            for trace in &source.columns {
                if previous.is_some_and(|column| column >= trace.column)
                    || trace.values.len() != source.input_path.len() + 1
                {
                    return Err(PruningStructureError::InvalidColumn);
                }
                previous = Some(trace.column);
                let target_value = provider_outputs
                    .get(trace.column.index())
                    .ok_or(PruningStructureError::InvalidColumn)?
                    .1;
                if trace.values.last() != Some(&target_value) {
                    return Err(PruningStructureError::InvalidColumn);
                }
                let source_node = fragment
                    .nodes()
                    .get(&source.responsibility.site.node)
                    .ok_or(PruningStructureError::InvalidPath)?;
                contains_value(&source_node.output.columns, trace.values[0], work)?;
                for (edge, pair) in source.input_path.iter().zip(trace.values.windows(2)) {
                    let parent = fragment
                        .nodes()
                        .get(&edge.consumer)
                        .ok_or(PruningStructureError::InvalidPath)?;
                    let child = fragment
                        .nodes()
                        .get(&edge.producer)
                        .ok_or(PruningStructureError::InvalidPath)?;
                    contains_value(&parent.output.columns, pair[0], work)?;
                    contains_value(&child.output.columns, pair[1], work)?;
                    if !actual_transport(fragment, parent, *edge, pair[0], pair[1], work)? {
                        return Err(PruningStructureError::InvalidTransport);
                    }
                }
                domain_columns.insert(trace.column);
                work.step()?;
            }
            sources.push(checked);
            work.step()?;
        }
        // This index belongs to this exact package and is shared by the
        // complete declaration table. Repeated actual input occurrences are
        // retained as ambiguity, rather than deduplicated by parent identity.
        for (producer, expected) in expected_inputs {
            if index.inputs.get(&producer) != Some(&Some(expected)) {
                return Err(PruningStructureError::SharedLocalProducer);
            }
            work.step()?;
        }
        if let Some(columns) = domain.domains() {
            for column in columns.keys() {
                if !domain_columns.contains(column) {
                    return Err(PruningStructureError::MissingDomainColumn);
                }
                work.step()?;
            }
        }
        Ok(Self {
            package,
            witness,
            read,
            domain,
            sources,
        })
    }
    pub const fn package(&self) -> &'package FragmentPackage {
        self.package
    }
    pub const fn witness(&self) -> &'witness PruningDomainWitness {
        self.witness
    }
    pub const fn read(&self) -> &'package FrozenConnectorRead {
        self.read
    }
    pub const fn domain(&self) -> &'package TupleDomain<ScanColumnId> {
        self.domain
    }
    pub fn sources(&self) -> &[PredicateConjunctSource] {
        &self.sources
    }
}

/// Indexed actual local consumers, built once under the table's work budget.
/// An ambiguous entry records multiple input occurrences, even on one parent.
pub(crate) struct PruningConsumerIndex<'package> {
    package: &'package FragmentPackage,
    inputs: BTreeMap<NodeId, Option<PruningInputEdge>>,
}
impl<'package> PruningConsumerIndex<'package> {
    pub(crate) fn try_new(
        package: &'package FragmentPackage,
        work: &mut PruningWork<'_, '_>,
    ) -> Result<Self, PruningStructureError> {
        let fragment = package.fragment();
        let mut inputs = BTreeMap::new();
        for consumer in fragment.nodes().values() {
            for (ordinal, producer) in consumer.inputs.iter().enumerate() {
                let input_ordinal =
                    u32::try_from(ordinal).map_err(|_| PruningStructureError::TooLarge)?;
                let edge = PruningInputEdge {
                    consumer: consumer.id,
                    input_ordinal,
                    producer: *producer,
                };
                match inputs.entry(*producer) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(Some(edge));
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        entry.insert(None);
                    }
                }
                work.step()?;
            }
            work.step()?;
        }
        Ok(Self { package, inputs })
    }
}

/// Local structural proof fuel. Exhaustion declines this proof; it is not a
/// new whole-plan admission limit or a memory allowance. Nested conjunct-source
/// work remains preflighted by the same reference bound. Borrowed mode observes
/// it on the caller's meter without adding it to this original structural fuel.
pub const MAX_PRUNING_STRUCTURE_WORK: usize = crate::MAX_FRAGMENT_DYNAMIC_ITEMS;
enum PruningObservation<'control, 'borrow> {
    Owned(CompileCheckpoints<'control>),
    Borrowed(&'borrow mut CompileCheckpoints<'control>),
}
pub(crate) struct PruningWork<'control, 'borrow> {
    observed: PruningObservation<'control, 'borrow>,
    units: usize,
}
impl<'control, 'borrow> PruningWork<'control, 'borrow> {
    pub(crate) fn try_new(
        control: &'control dyn PureCompileControl,
    ) -> Result<Self, PruningStructureError> {
        Ok(Self {
            observed: PruningObservation::Owned(CompileCheckpoints::try_new(
                control,
                CompilePhase::Validate,
            )?),
            units: 0,
        })
    }
    pub(crate) fn borrowed(observed: &'borrow mut CompileCheckpoints<'control>) -> Self {
        Self {
            observed: PruningObservation::Borrowed(observed),
            units: 0,
        }
    }
    fn is_borrowed(&self) -> bool {
        matches!(&self.observed, PruningObservation::Borrowed(_))
    }
    fn observed_mut(&mut self) -> &mut CompileCheckpoints<'control> {
        match &mut self.observed {
            PruningObservation::Owned(observed) => observed,
            PruningObservation::Borrowed(observed) => observed,
        }
    }
    fn step(&mut self) -> Result<(), PruningStructureError> {
        if self.units == MAX_PRUNING_STRUCTURE_WORK {
            return Err(PruningStructureError::TooLarge);
        }
        self.units += 1;
        self.observed_mut().step()?;
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<(), PruningStructureError> {
        if let PruningObservation::Owned(observed) = self.observed {
            observed.finish()?;
        }
        Ok(())
    }
}

fn preflight(
    witness: &PruningDomainWitness,
    work: &mut PruningWork<'_, '_>,
) -> Result<(), PruningStructureError> {
    if witness.sources.is_empty() {
        return Err(PruningStructureError::EmptySources);
    }
    let mut count = witness.sources.len();
    if count > MAX_CONTROL_USE_REFERENCES {
        return Err(PruningStructureError::TooLarge);
    }
    for source in &witness.sources {
        if source.conjunct_path.len() >= MAX_CONTROL_DEPTH {
            return Err(PruningStructureError::TooLarge);
        }
        for size in [
            source.conjunct_path.len(),
            source.input_path.len(),
            source.columns.len(),
        ] {
            count = count
                .checked_add(size)
                .ok_or(PruningStructureError::TooLarge)?;
        }
        if count > MAX_CONTROL_USE_REFERENCES {
            return Err(PruningStructureError::TooLarge);
        }
        for trace in &source.columns {
            count = count
                .checked_add(trace.values.len())
                .ok_or(PruningStructureError::TooLarge)?;
            if count > MAX_CONTROL_USE_REFERENCES {
                return Err(PruningStructureError::TooLarge);
            }
            work.step()?;
        }
        work.step()?;
    }
    Ok(())
}
fn contains_value(
    columns: &[ValueId],
    value: ValueId,
    work: &mut PruningWork<'_, '_>,
) -> Result<(), PruningStructureError> {
    for column in columns {
        work.step()?;
        if *column == value {
            return Ok(());
        }
    }
    Err(PruningStructureError::InvalidTransport)
}
fn actual_transport(
    fragment: &crate::Fragment,
    parent: &crate::PhysicalNode,
    edge: PruningInputEdge,
    output: ValueId,
    input: ValueId,
    work: &mut PruningWork<'_, '_>,
) -> Result<bool, PruningStructureError> {
    // A Project's real field is checked even when IDs happen to be equal.
    if let NodeKind::Project { expressions } = &parent.kind {
        for (expression, value) in expressions {
            work.step()?;
            if *value == output && fragment.root_expression(parent.id,*expression)
                .is_some_and(|expression| matches!(expression.kind, ExprKind::Value(value) if value == input))
            { return Ok(true); }
        }
        return Ok(false);
    }
    if let NodeKind::SetOp { input_mappings, .. } = &parent.kind {
        let Some(mapping) = input_mappings.get(edge.input_ordinal as usize) else {
            return Ok(false);
        };
        for (value, mapped) in parent.output.columns.iter().zip(mapping) {
            work.step()?;
            if (*value, *mapped) == (output, input) {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    if let NodeKind::Unpivot { spec } = &parent.kind {
        for (original, replacement) in &spec.passthrough {
            work.step()?;
            if (*original, *replacement) == (input, output) {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    if output == input {
        return Ok(true);
    }
    if let Some(definition) = fragment.values().get(&output) {
        match &definition.origin {
            ValueOrigin::NullExtended { node, of } if *node == parent.id && *of == input => {
                match &parent.kind {
                    NodeKind::HashJoin { null_extended, .. }
                    | NodeKind::NestLoopJoin { null_extended, .. } => {
                        contains_value(null_extended, output, work)?;
                        return Ok(true);
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
    if let NodeKind::Repeat {
        grouping_values, ..
    } = &parent.kind
    {
        for (original, replacement) in grouping_values {
            work.step()?;
            if (*original, *replacement) == (input, output) {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests;
