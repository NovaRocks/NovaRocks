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

//! Projection of pruning declarations after typed DTO carrier admission.
//! This component preserves references; it grants no implication or pruning
//! authority. Exact scan/source/consumer closure is checked by the package.

use novarocks_connector_contract::ScanColumnId;
use novarocks_physical_plan::{
    FragmentId, FrozenFragmentPruning, FrozenPruningError, NodeId, PredicateResponsibilityRef,
    ProviderReadOccurrenceId, PruningColumnTrace, PruningDomainField, PruningDomainSite,
    PruningDomainWitness, PruningInputEdge, PruningSourceWitness, ValueId,
};
use novarocks_proto_models::physical_semantics_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, ExpressionUseId, MAX_CONTROL_DEPTH, MAX_CONTROL_USE_REFERENCES,
    PureCompileControl,
};

use super::calls::{decode_context, encode_context};
use super::owned_resources::Projection;
use super::{SemanticsCodecError, required_id};
use crate::physical_node_v2::{NodeProjectionFacts, NodeProjectionLimits};
use novarocks_type_contract::{CompileControlError, ControlOwnedResourceFacts};

type Error = SemanticsCodecError;

fn add_items(items: &mut usize, count: usize) -> Result<(), Error> {
    *items = items
        .checked_add(count)
        .filter(|total| *total <= MAX_CONTROL_USE_REFERENCES)
        .ok_or(Error::Pruning(FrozenPruningError::TooLarge))?;
    Ok(())
}

fn check_path_depth(length: usize) -> Result<(), Error> {
    // The root also occupies one level, matching FrozenFragmentPruning.
    if length >= MAX_CONTROL_DEPTH {
        return Err(Error::Pruning(FrozenPruningError::TooLarge));
    }
    Ok(())
}

fn encode_field(field: PruningDomainField) -> i32 {
    match field {
        PruningDomainField::Enforced => wire::PruningDomainField::Enforced as i32,
        PruningDomainField::Unenforced => wire::PruningDomainField::Unenforced as i32,
    }
}

fn decode_field(field: i32) -> Result<PruningDomainField, Error> {
    match wire::PruningDomainField::try_from(field) {
        Ok(wire::PruningDomainField::Enforced) => Ok(PruningDomainField::Enforced),
        Ok(wire::PruningDomainField::Unenforced) => Ok(PruningDomainField::Unenforced),
        _ => Err(Error::InvalidShape(
            "unknown or unspecified pruning domain field",
        )),
    }
}

fn column_ordinal(column: ScanColumnId) -> Result<u32, Error> {
    u32::try_from(column.index())
        .map_err(|_| Error::InvalidShape("pruning column ordinal does not fit uint32"))
}

// Every length contributes to one component bound before any projection Vec
// or boxed slice is allocated. Scratch allocation and Vec-to-box conversion
// are not MEM grants; these DTOs already passed carrier resource admission.
fn preflight_encode(
    input: &FrozenFragmentPruning,
    resources: &mut Projection<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let mut items = 0;
    add_items(&mut items, input.witnesses().len())?;
    resources.items(input.witnesses().len())?;
    resources.buffers::<wire::PruningDomainWitness>(input.witnesses().len(), 1)?;
    resources.known::<FrozenFragmentPruning>(1)?;
    resources.known::<PruningDomainWitness>(input.witnesses().len())?;
    for witness in input.witnesses() {
        resources.items(witness.sources.len())?;
        resources.buffers::<wire::PruningSourceWitness>(witness.sources.len(), 1)?;
        resources.known::<PruningSourceWitness>(witness.sources.len())?;
        resources.gate()?;
        work.step()?;
        add_items(&mut items, witness.sources.len())?;
        for source in witness.sources.iter() {
            resources.items(source.conjunct_path.len())?;
            resources.items(source.input_path.len())?;
            resources.items(source.columns.len())?;
            resources.buffers::<u32>(source.conjunct_path.len(), 1)?;
            resources.buffers::<wire::PruningInputEdge>(source.input_path.len(), 1)?;
            resources.buffers::<wire::PruningColumnTrace>(source.columns.len(), 1)?;
            resources.known::<u32>(source.conjunct_path.len())?;
            resources.known::<PruningInputEdge>(source.input_path.len())?;
            resources.known::<PruningColumnTrace>(source.columns.len())?;
            resources.gate()?;
            work.step()?;
            check_path_depth(source.conjunct_path.len())?;
            add_items(&mut items, source.conjunct_path.len())?;
            add_items(&mut items, source.input_path.len())?;
            add_items(&mut items, source.columns.len())?;
            for column in source.columns.iter() {
                resources.items(column.values.len())?;
                resources.buffers::<u32>(column.values.len(), 1)?;
                resources.known::<ValueId>(column.values.len())?;
                resources.gate()?;
                work.step()?;
                add_items(&mut items, column.values.len())?;
                column_ordinal(column.column)?;
            }
        }
    }
    Ok(())
}

fn preflight_decode(
    input: &wire::FrozenPruning,
    resources: &mut Projection<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let mut items = 0;
    add_items(&mut items, input.witnesses.len())?;
    resources.items(input.witnesses.len())?;
    resources.buffers::<PruningDomainWitness>(input.witnesses.len(), 1)?;
    resources.known::<wire::FrozenPruning>(1)?;
    resources.known::<wire::PruningDomainWitness>(input.witnesses.capacity())?;
    if resources.observed_mode() {
        resources.gate()?;
        resources.child(
            &FrozenFragmentPruning::construction_resources(input.witnesses.len())?,
            &mut ControlOwnedResourceFacts::default(),
        )?;
    }
    for witness in &input.witnesses {
        resources.items(witness.sources.len())?;
        resources.buffers::<PruningSourceWitness>(witness.sources.len(), 2)?;
        resources.known::<wire::PruningSourceWitness>(witness.sources.capacity())?;
        resources.gate()?;
        work.step()?;
        add_items(&mut items, witness.sources.len())?;
        for source in &witness.sources {
            resources.items(source.conjunct_path.len())?;
            resources.items(source.input_path.len())?;
            resources.items(source.columns.len())?;
            resources.buffers::<u32>(source.conjunct_path.len(), 2)?;
            resources.buffers::<PruningInputEdge>(source.input_path.len(), 2)?;
            resources.buffers::<PruningColumnTrace>(source.columns.len(), 2)?;
            resources.known::<u32>(source.conjunct_path.capacity())?;
            resources.known::<wire::PruningInputEdge>(source.input_path.capacity())?;
            resources.known::<wire::PruningColumnTrace>(source.columns.capacity())?;
            resources.gate()?;
            work.step()?;
            check_path_depth(source.conjunct_path.len())?;
            add_items(&mut items, source.conjunct_path.len())?;
            add_items(&mut items, source.input_path.len())?;
            add_items(&mut items, source.columns.len())?;
            for column in &source.columns {
                resources.items(column.value_ids.len())?;
                resources.buffers::<ValueId>(column.value_ids.len(), 2)?;
                resources.known::<u32>(column.value_ids.capacity())?;
                resources.gate()?;
                work.step()?;
                add_items(&mut items, column.value_ids.len())?;
                usize::try_from(column.column_ordinal).map_err(|_| {
                    Error::InvalidShape("pruning column ordinal does not fit usize")
                })?;
            }
        }
    }
    Ok(())
}

pub(super) fn encode_pruning(
    input: &FrozenFragmentPruning,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::FrozenPruning, Error> {
    encode_pruning_core(input, &mut Projection::plain(), work)
}
fn encode_pruning_core(
    input: &FrozenFragmentPruning,
    resources: &mut Projection<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::FrozenPruning, Error> {
    preflight_encode(input, resources, work)?;
    let mut witnesses = resources.reserve(input.witnesses().len(), work)?;
    for witness in input.witnesses() {
        work.step()?;
        let mut sources = resources.reserve(witness.sources.len(), work)?;
        for source in witness.sources.iter() {
            work.step()?;
            let mut conjunct_path = resources.reserve(source.conjunct_path.len(), work)?;
            for ordinal in source.conjunct_path.iter() {
                work.step()?;
                conjunct_path.push(*ordinal);
            }
            let mut input_path = resources.reserve(source.input_path.len(), work)?;
            for edge in source.input_path.iter() {
                work.step()?;
                input_path.push(wire::PruningInputEdge {
                    consumer_id: Some(edge.consumer.get()),
                    input_ordinal: edge.input_ordinal,
                    producer_id: Some(edge.producer.get()),
                });
            }
            let mut columns = resources.reserve(source.columns.len(), work)?;
            for column in source.columns.iter() {
                work.step()?;
                let mut value_ids = resources.reserve(column.values.len(), work)?;
                for value in column.values.iter() {
                    work.step()?;
                    value_ids.push(value.get());
                }
                columns.push(wire::PruningColumnTrace {
                    column_ordinal: column_ordinal(column.column)?,
                    value_ids,
                });
            }
            sources.push(wire::PruningSourceWitness {
                responsibility: Some(wire::PredicateResponsibilityRef {
                    fragment_id: Some(source.responsibility.fragment.get()),
                    node_id: Some(source.responsibility.site.node.get()),
                    root_ordinal: Some(source.responsibility.site.root.get()),
                    use_id: Some(source.responsibility.use_id.get()),
                }),
                context: Some(encode_context(&source.context)),
                conjunct_path,
                input_path,
                columns,
            });
        }
        witnesses.push(wire::PruningDomainWitness {
            target: Some(wire::PruningDomainSite {
                fragment_id: Some(witness.target.fragment.get()),
                scan_id: Some(witness.target.scan.get()),
                occurrence_id: Some(witness.target.occurrence.get()),
                field: encode_field(witness.target.field),
            }),
            sources,
        });
    }
    Ok(wire::FrozenPruning { witnesses })
}

pub(super) fn decode_pruning(
    fragment: FragmentId,
    input: &wire::FrozenPruning,
    work: &mut CompileCheckpoints<'_>,
    control: &dyn PureCompileControl,
) -> Result<FrozenFragmentPruning, Error> {
    decode_pruning_core(fragment, input, &mut Projection::plain(), work, control)
}
fn decode_pruning_core(
    fragment: FragmentId,
    input: &wire::FrozenPruning,
    resources: &mut Projection<'_>,
    work: &mut CompileCheckpoints<'_>,
    control: &dyn PureCompileControl,
) -> Result<FrozenFragmentPruning, Error> {
    preflight_decode(input, resources, work)?;
    let mut witnesses = resources.reserve(input.witnesses.len(), work)?;
    for witness in &input.witnesses {
        work.step()?;
        let target = witness
            .target
            .as_ref()
            .ok_or(Error::InvalidShape("pruning target is missing"))?;
        let target = PruningDomainSite {
            fragment: FragmentId::new(required_id(
                target.fragment_id,
                "pruning target fragment is missing",
            )?),
            scan: NodeId::new(required_id(target.scan_id, "pruning scan is missing")?),
            occurrence: ProviderReadOccurrenceId::new(required_id(
                target.occurrence_id,
                "pruning occurrence is missing",
            )?),
            field: decode_field(target.field)?,
        };
        let mut sources = resources.reserve(witness.sources.len(), work)?;
        for source in &witness.sources {
            work.step()?;
            let responsibility = source.responsibility.as_ref().ok_or(Error::InvalidShape(
                "pruning source responsibility is missing",
            ))?;
            let responsibility = PredicateResponsibilityRef {
                fragment: FragmentId::new(required_id(
                    responsibility.fragment_id,
                    "predicate responsibility fragment is missing",
                )?),
                site: novarocks_physical_plan::PredicateSourceSite {
                    node: novarocks_physical_plan::NodeId::new(required_id(
                        responsibility.node_id,
                        "missing predicate node ID",
                    )?),
                    root: novarocks_expression_contract::RootOrdinal::new(required_id(
                        responsibility.root_ordinal,
                        "missing predicate root ordinal",
                    )?),
                },
                use_id: ExpressionUseId::new(required_id(
                    responsibility.use_id,
                    "predicate responsibility use is missing",
                )?),
            };
            let context = decode_context(
                source
                    .context
                    .as_ref()
                    .ok_or(Error::InvalidShape("pruning source context is missing"))?,
            )?;
            let mut conjunct_path = resources.reserve(source.conjunct_path.len(), work)?;
            for ordinal in &source.conjunct_path {
                work.step()?;
                conjunct_path.push(*ordinal);
            }
            let mut input_path = resources.reserve(source.input_path.len(), work)?;
            for edge in &source.input_path {
                work.step()?;
                input_path.push(PruningInputEdge {
                    consumer: NodeId::new(required_id(
                        edge.consumer_id,
                        "pruning consumer is missing",
                    )?),
                    // The ordinal is a position: protobuf zero is its actual value.
                    input_ordinal: edge.input_ordinal,
                    producer: NodeId::new(required_id(
                        edge.producer_id,
                        "pruning producer is missing",
                    )?),
                });
            }
            let mut columns = resources.reserve(source.columns.len(), work)?;
            for column in &source.columns {
                work.step()?;
                let mut values = resources.reserve(column.value_ids.len(), work)?;
                for value in &column.value_ids {
                    work.step()?;
                    values.push(ValueId::new(*value));
                }
                columns.push(PruningColumnTrace {
                    column: ScanColumnId::new(usize::try_from(column.column_ordinal).map_err(
                        |_| Error::InvalidShape("pruning column ordinal does not fit usize"),
                    )?),
                    values: resources.boxed(values, work)?,
                });
            }
            sources.push(PruningSourceWitness {
                responsibility,
                context,
                conjunct_path: resources.boxed(conjunct_path, work)?,
                input_path: resources.boxed(input_path, work)?,
                columns: resources.boxed(columns, work)?,
            });
        }
        witnesses.push(PruningDomainWitness {
            target,
            sources: resources.boxed(sources, work)?,
        });
    }
    // Keep the original phase's tail before delegating to the real owner.
    // That owner validates declarations, not package closure or implication.
    work.flush()?;
    if resources.observed_mode() {
        let mut previous = FrozenFragmentPruning::construction_resources(input.witnesses.len())?;
        FrozenFragmentPruning::try_new_in(
            fragment,
            witnesses,
            &mut |facts| resources.child(facts, &mut previous),
            work,
        )
        .map_err(Error::from)
    } else {
        FrozenFragmentPruning::try_new(fragment, witnesses, control).map_err(Error::from)
    }
}

/// Preserve original declarations in the caller's scope; no implication or
/// Package consumer authority follows from these representation facts.
pub fn encode_frozen_pruning_observed(
    input: &FrozenFragmentPruning,
    source_retained_bytes: usize,
    limits: NodeProjectionLimits,
    admit: &mut impl FnMut(&NodeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(wire::FrozenPruning, NodeProjectionFacts), Error> {
    let mut resources = Projection::observed(source_retained_bytes, limits, admit, 0)?;
    let output = encode_pruning_core(input, &mut resources, work)?;
    Ok((output, resources.facts()?))
}
pub fn decode_frozen_pruning_observed(
    fragment: FragmentId,
    input: &wire::FrozenPruning,
    source_retained_bytes: usize,
    limits: NodeProjectionLimits,
    admit: &mut impl FnMut(&NodeProjectionFacts) -> Result<(), CompileControlError>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(FrozenFragmentPruning, NodeProjectionFacts), Error> {
    let mut resources = Projection::observed(source_retained_bytes, limits, admit, 0)?;
    let control = work.control();
    let output = decode_pruning_core(fragment, input, &mut resources, work, control)?;
    Ok((output, resources.facts()?))
}

#[cfg(test)]
#[path = "pruning/tests.rs"]
mod tests;
