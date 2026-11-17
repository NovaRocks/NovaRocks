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
use crate::physical_control_v2::{decode_site, encode_site};
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
                    site: Some(encode_site(source.responsibility.site)),
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
                site: decode_site(responsibility.site.as_ref().ok_or(Error::InvalidShape(
                    "predicate responsibility root site is missing",
                ))?)?,
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
mod tests {
    use super::*;
    use novarocks_physical_plan::{ExpressionRootRole, ExpressionRootSite};
    use novarocks_proto_models::physical_control_v2 as control_wire;
    use novarocks_type_contract::{
        CompileControlError, CompilePhase, EvaluationDemand, EvaluationRegionId,
        ExpressionEffectContext,
    };
    use std::sync::Mutex;

    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }

    fn source() -> PruningSourceWitness {
        PruningSourceWitness {
            responsibility: PredicateResponsibilityRef {
                fragment: FragmentId::new(0),
                site: ExpressionRootSite {
                    node: NodeId::new(u32::MAX),
                    role: ExpressionRootRole::FilterPredicate { predicate: 0 },
                },
                use_id: ExpressionUseId::new(0),
            },
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(0),
                region: EvaluationRegionId::new(u32::MAX),
                demand: EvaluationDemand::TruthOnly,
            },
            conjunct_path: Box::from([1, 0, 1]),
            input_path: Box::from([
                PruningInputEdge {
                    consumer: NodeId::new(u32::MAX),
                    input_ordinal: 0,
                    producer: NodeId::new(0),
                },
                PruningInputEdge {
                    consumer: NodeId::new(0),
                    input_ordinal: u32::MAX,
                    producer: NodeId::new(u32::MAX),
                },
            ]),
            columns: Box::from([
                PruningColumnTrace {
                    column: ScanColumnId::new(0),
                    values: Box::from([
                        ValueId::new(u32::MAX),
                        ValueId::new(0),
                        ValueId::new(u32::MAX),
                    ]),
                },
                PruningColumnTrace {
                    column: ScanColumnId::new(u32::MAX as usize),
                    values: Box::from([ValueId::new(0)]),
                },
            ]),
        }
    }

    fn frozen_fixture() -> FrozenFragmentPruning {
        FrozenFragmentPruning::try_new(
            FragmentId::new(0),
            vec![
                PruningDomainWitness {
                    target: PruningDomainSite {
                        fragment: FragmentId::new(0),
                        scan: NodeId::new(u32::MAX),
                        occurrence: ProviderReadOccurrenceId::new(0),
                        field: PruningDomainField::Enforced,
                    },
                    sources: Box::from([source()]),
                },
                PruningDomainWitness {
                    target: PruningDomainSite {
                        fragment: FragmentId::new(0),
                        scan: NodeId::new(0),
                        occurrence: ProviderReadOccurrenceId::new(u32::MAX),
                        field: PruningDomainField::Unenforced,
                    },
                    sources: Box::from([source()]),
                },
            ],
            &Control,
        )
        .unwrap()
    }

    // These are declaration DTOs, not evidence of an actual package path or
    // source-to-scan implication. The package checks those separate contracts.
    fn expected_source() -> wire::PruningSourceWitness {
        wire::PruningSourceWitness {
            responsibility: Some(wire::PredicateResponsibilityRef {
                fragment_id: Some(0),
                site: Some(control_wire::RootSite {
                    node_id: Some(u32::MAX),
                    role: Some(control_wire::root_site::Role::FilterPredicate(0)),
                }),
                use_id: Some(0),
            }),
            context: Some(wire::EffectContext {
                use_id: Some(0),
                domain_id: Some(u32::MAX),
                demand: control_wire::EvaluationDemand::TruthOnly as i32,
            }),
            conjunct_path: vec![1, 0, 1],
            input_path: vec![
                wire::PruningInputEdge {
                    consumer_id: Some(u32::MAX),
                    input_ordinal: 0,
                    producer_id: Some(0),
                },
                wire::PruningInputEdge {
                    consumer_id: Some(0),
                    input_ordinal: u32::MAX,
                    producer_id: Some(u32::MAX),
                },
            ],
            columns: vec![
                wire::PruningColumnTrace {
                    column_ordinal: 0,
                    value_ids: vec![u32::MAX, 0, u32::MAX],
                },
                wire::PruningColumnTrace {
                    column_ordinal: u32::MAX,
                    value_ids: vec![0],
                },
            ],
        }
    }

    fn expected_wire() -> wire::FrozenPruning {
        wire::FrozenPruning {
            witnesses: vec![
                wire::PruningDomainWitness {
                    target: Some(wire::PruningDomainSite {
                        fragment_id: Some(0),
                        scan_id: Some(u32::MAX),
                        occurrence_id: Some(0),
                        field: wire::PruningDomainField::Enforced as i32,
                    }),
                    sources: vec![expected_source()],
                },
                wire::PruningDomainWitness {
                    target: Some(wire::PruningDomainSite {
                        fragment_id: Some(0),
                        scan_id: Some(0),
                        occurrence_id: Some(u32::MAX),
                        field: wire::PruningDomainField::Unenforced as i32,
                    }),
                    sources: vec![expected_source()],
                },
            ],
        }
    }

    fn encode(input: &FrozenFragmentPruning) -> Result<wire::FrozenPruning, Error> {
        let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Encode)?;
        let output = encode_pruning(input, &mut work)?;
        work.finish()?;
        Ok(output)
    }

    fn decode(
        fragment: FragmentId,
        input: &wire::FrozenPruning,
    ) -> Result<FrozenFragmentPruning, Error> {
        let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Decode)?;
        let output = decode_pruning(fragment, input, &mut work, &Control)?;
        work.finish()?;
        Ok(output)
    }

    #[test]
    fn pruning_projection_matches_independent_sparse_ordered_dto_with_zero_and_max_presence() {
        let input = frozen_fixture();
        let expected = expected_wire();
        assert_eq!(encode(&input).unwrap(), expected);
        assert_eq!(decode(FragmentId::new(0), &expected).unwrap(), input);
        let mut max_fragment = expected;
        for witness in &mut max_fragment.witnesses {
            witness.target.as_mut().unwrap().fragment_id = Some(u32::MAX);
            for source in &mut witness.sources {
                source.responsibility.as_mut().unwrap().fragment_id = Some(u32::MAX);
                source.responsibility.as_mut().unwrap().use_id = Some(u32::MAX);
                source.context.as_mut().unwrap().use_id = Some(u32::MAX);
            }
        }
        let decoded = decode(FragmentId::new(u32::MAX), &max_fragment).unwrap();
        assert_eq!(encode(&decoded).unwrap(), max_fragment);
        assert_eq!(
            decoded.witnesses()[0].sources[0]
                .responsibility
                .use_id
                .get(),
            u32::MAX
        );
    }

    #[test]
    fn pruning_missing_mandatory_fields_and_closed_vocabularies_are_rejected() {
        type Mutation = fn(&mut wire::FrozenPruning);
        let mutations: &[Mutation] = &[
            |dto| dto.witnesses[0].target = None,
            |dto| dto.witnesses[0].target.as_mut().unwrap().fragment_id = None,
            |dto| dto.witnesses[0].target.as_mut().unwrap().scan_id = None,
            |dto| dto.witnesses[0].target.as_mut().unwrap().occurrence_id = None,
            |dto| dto.witnesses[0].target.as_mut().unwrap().field = 0,
            |dto| dto.witnesses[0].target.as_mut().unwrap().field = i32::MAX,
            |dto| dto.witnesses[0].sources[0].responsibility = None,
            |dto| {
                dto.witnesses[0].sources[0]
                    .responsibility
                    .as_mut()
                    .unwrap()
                    .fragment_id = None
            },
            |dto| {
                dto.witnesses[0].sources[0]
                    .responsibility
                    .as_mut()
                    .unwrap()
                    .site = None
            },
            |dto| {
                dto.witnesses[0].sources[0]
                    .responsibility
                    .as_mut()
                    .unwrap()
                    .use_id = None
            },
            |dto| {
                dto.witnesses[0].sources[0]
                    .responsibility
                    .as_mut()
                    .unwrap()
                    .site
                    .as_mut()
                    .unwrap()
                    .node_id = None
            },
            |dto| {
                dto.witnesses[0].sources[0]
                    .responsibility
                    .as_mut()
                    .unwrap()
                    .site
                    .as_mut()
                    .unwrap()
                    .role = None
            },
            |dto| dto.witnesses[0].sources[0].context = None,
            |dto| dto.witnesses[0].sources[0].context.as_mut().unwrap().use_id = None,
            |dto| {
                dto.witnesses[0].sources[0]
                    .context
                    .as_mut()
                    .unwrap()
                    .domain_id = None
            },
            |dto| dto.witnesses[0].sources[0].context.as_mut().unwrap().demand = 0,
            |dto| dto.witnesses[0].sources[0].context.as_mut().unwrap().demand = i32::MAX,
            |dto| dto.witnesses[0].sources[0].input_path[0].consumer_id = None,
            |dto| dto.witnesses[0].sources[0].input_path[0].producer_id = None,
        ];
        for mutate in mutations {
            let mut input = expected_wire();
            mutate(&mut input);
            assert!(decode(FragmentId::new(0), &input).is_err());
        }
    }

    #[test]
    fn pruning_owner_rejects_wrong_fragment_duplicate_target_and_empty_sources() {
        let input = expected_wire();
        assert!(matches!(
            decode(FragmentId::new(1), &input),
            Err(Error::Pruning(FrozenPruningError::WrongFragment))
        ));
        let mut duplicate = input.clone();
        duplicate.witnesses.push(duplicate.witnesses[0].clone());
        assert!(matches!(
            decode(FragmentId::new(0), &duplicate),
            Err(Error::Pruning(FrozenPruningError::DuplicateTarget))
        ));
        let mut empty = input;
        empty.witnesses[0].sources.clear();
        assert!(matches!(
            decode(FragmentId::new(0), &empty),
            Err(Error::Pruning(FrozenPruningError::EmptySources))
        ));
        let empty = wire::FrozenPruning::default();
        assert!(
            decode(FragmentId::new(u32::MAX), &empty)
                .unwrap()
                .witnesses()
                .is_empty()
        );
    }

    #[test]
    fn pruning_conjunct_depth_uses_actual_root_inclusive_boundary() {
        let mut input = expected_wire();
        input.witnesses[0].sources[0].conjunct_path = vec![1; MAX_CONTROL_DEPTH - 1];
        let frozen = decode(FragmentId::new(0), &input).unwrap();
        assert_eq!(encode(&frozen).unwrap(), input);
        input.witnesses[0].sources[0].conjunct_path.push(0);
        assert!(matches!(
            decode(FragmentId::new(0), &input),
            Err(Error::Pruning(FrozenPruningError::TooLarge))
        ));
    }

    fn budget_wire(first_values: usize, second_values: usize) -> wire::FrozenPruning {
        let witness = |scan: u32, count| wire::PruningDomainWitness {
            target: Some(wire::PruningDomainSite {
                fragment_id: Some(0),
                scan_id: Some(scan),
                occurrence_id: Some(0),
                field: wire::PruningDomainField::Enforced as i32,
            }),
            sources: vec![wire::PruningSourceWitness {
                conjunct_path: vec![],
                input_path: vec![],
                columns: vec![wire::PruningColumnTrace {
                    column_ordinal: 0,
                    value_ids: vec![u32::MAX; count],
                }],
                ..expected_source()
            }],
        };
        wire::FrozenPruning {
            witnesses: vec![witness(0, first_values), witness(u32::MAX, second_values)],
        }
    }

    #[test]
    fn pruning_combined_budget_is_not_reset_per_witness_or_source() {
        // Two witnesses + two sources + two columns plus their actual values.
        let half = (MAX_CONTROL_USE_REFERENCES - 6) / 2;
        let near = budget_wire(half, half);
        let frozen = decode(FragmentId::new(0), &near).unwrap();
        assert_eq!(
            frozen.dynamic_items_observed(&Control).unwrap(),
            MAX_CONTROL_USE_REFERENCES
        );
        assert_eq!(encode(&frozen).unwrap(), near);
        let over = budget_wire(half, half + 1);
        assert!(matches!(
            decode(FragmentId::new(0), &over),
            Err(Error::Pruning(FrozenPruningError::TooLarge))
        ));
        let mut combined_source = near;
        let second_source = combined_source
            .witnesses
            .pop()
            .unwrap()
            .sources
            .pop()
            .unwrap();
        combined_source.witnesses[0].sources.push(second_source);
        // Removing one witness leaves 65535 items; two additional path edges
        // cross the same aggregate table bound even within a single target.
        combined_source.witnesses[0].sources[0].conjunct_path = vec![0, 1];
        assert!(matches!(
            decode(FragmentId::new(0), &combined_source),
            Err(Error::Pruning(FrozenPruningError::TooLarge))
        ));
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn pruning_encode_refuses_unrepresentable_host_column_ordinal_before_projection() {
        let mut witness = frozen_fixture().witnesses()[0].clone();
        witness.sources[0].columns[0].column = ScanColumnId::new(u32::MAX as usize + 1);
        let frozen =
            FrozenFragmentPruning::try_new(FragmentId::new(0), vec![witness], &Control).unwrap();
        assert!(matches!(
            encode(&frozen),
            Err(Error::InvalidShape(
                "pruning column ordinal does not fit uint32"
            ))
        ));
    }

    #[derive(Clone, Copy)]
    enum Stop {
        Entry,
        Quantum,
        Tail,
    }
    struct Refuse {
        phase: CompilePhase,
        error: CompileControlError,
        stop: Stop,
        calls: Mutex<Vec<(CompilePhase, u32)>>,
    }
    impl PureCompileControl for Refuse {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            self.calls.lock().unwrap().push((phase, units));
            if phase == self.phase
                && match self.stop {
                    Stop::Entry => units == 0,
                    Stop::Quantum => units == 256,
                    Stop::Tail => units > 0 && units < 256,
                }
            {
                Err(self.error.clone())
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn pruning_preserves_all_three_control_failures_at_entry_quantum_and_tail() {
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for phase in [CompilePhase::Encode, CompilePhase::Decode] {
                for stop in [Stop::Entry, Stop::Quantum, Stop::Tail] {
                    let control = Refuse {
                        phase,
                        error: error.clone(),
                        stop,
                        calls: Mutex::new(vec![]),
                    };
                    let input = match stop {
                        Stop::Quantum => budget_wire(320, 0),
                        _ => expected_wire(),
                    };
                    let frozen = decode(FragmentId::new(0), &input).unwrap();
                    let result = (|| -> Result<(), Error> {
                        let mut work = CompileCheckpoints::try_new(&control, phase)?;
                        if phase == CompilePhase::Encode {
                            encode_pruning(&frozen, &mut work)?;
                        } else {
                            decode_pruning(FragmentId::new(0), &input, &mut work, &control)?;
                        }
                        work.finish()?;
                        Ok(())
                    })();
                    assert!(matches!(result, Err(Error::Control(actual)) if actual == error));
                    let calls = control.calls.lock().unwrap();
                    let last = calls.last().unwrap();
                    assert_eq!(last.0, phase);
                    match stop {
                        Stop::Entry => assert_eq!(last.1, 0),
                        Stop::Quantum => assert_eq!(last.1, 256),
                        Stop::Tail => assert!((1..256).contains(&last.1)),
                    }
                }
            }
        }
    }

    #[test]
    fn pruning_real_owner_validation_control_is_flattened_after_dto_projection() {
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Refuse {
                phase: CompilePhase::Validate,
                error: error.clone(),
                stop: Stop::Entry,
                calls: Mutex::new(vec![]),
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let result = decode_pruning(FragmentId::new(0), &expected_wire(), &mut work, &control);
            assert!(matches!(result, Err(Error::Control(actual)) if actual == error));
            assert_eq!(
                control.calls.lock().unwrap().last(),
                Some(&(CompilePhase::Validate, 0))
            );
        }
    }
}
