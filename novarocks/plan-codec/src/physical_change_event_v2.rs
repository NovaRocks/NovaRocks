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

//! Complete ChangeEventExpand representation through original Values and
//! Expressions loans. Fragment owns mutation typing, graph and property proofs.

pub use crate::physical_node_v2::{
    NodeCodecError as ChangeEventNodeCodecError,
    NodeProjectionFacts as ChangeEventNodeProjectionFacts,
    NodeProjectionLimits as ChangeEventNodeProjectionLimits,
};
use crate::{
    physical_expression_v2::{DecodedExpressions, EncodedExpressions},
    physical_node_v2::*,
    physical_properties_v2 as properties,
    physical_type_v2::{self, OriginalValueTypeSourceIndex},
    physical_value_v2::EncodedValues,
};
use novarocks_connector_contract::ConnectorRowMutationEffect;
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
type Error = ChangeEventNodeCodecError;

/// Sole v2 mapping. Effects describe SQL row mutation, not provider routing.
pub(crate) fn encode_mutation_effect(effect: ConnectorRowMutationEffect) -> i32 {
    match effect {
        ConnectorRowMutationEffect::Delete => wire::RowMutationEffect::Delete as i32,
        ConnectorRowMutationEffect::Replace => wire::RowMutationEffect::Replace as i32,
        ConnectorRowMutationEffect::Insert => wire::RowMutationEffect::Insert as i32,
    }
}
pub(crate) fn decode_mutation_effect(
    effect: i32,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ConnectorRowMutationEffect, Error> {
    let result = match wire::RowMutationEffect::try_from(effect) {
        Ok(wire::RowMutationEffect::Delete) => Ok(ConnectorRowMutationEffect::Delete),
        Ok(wire::RowMutationEffect::Replace) => Ok(ConnectorRowMutationEffect::Replace),
        Ok(wire::RowMutationEffect::Insert) => Ok(ConnectorRowMutationEffect::Insert),
        _ => Err(invalid("change-event effect is unspecified or unknown")),
    };
    work.step()?;
    result
}
fn required(id: Option<u32>, work: &mut CompileCheckpoints<'_>) -> Result<u32, Error> {
    let result = id.ok_or_else(|| invalid("change-event required reference is absent"));
    work.step()?;
    result
}
fn expression_encode(
    id: novarocks_expression_contract::RootOrdinal,
    expressions: &EncodedExpressions<'_, '_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let found = expressions
        .root_expression_observed(id.get(), work)?
        .is_some();
    work.step()?;
    if found {
        Ok(())
    } else {
        Err(invalid(
            "change-event expression is absent from original emission",
        ))
    }
}
fn expression_decode(
    id: u32,
    expressions: &DecodedExpressions<'_, '_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let found = expressions.root_definition_observed(id, work)?.is_some();
    work.step()?;
    if found {
        Ok(())
    } else {
        Err(invalid(
            "change-event expression is absent from original reception",
        ))
    }
}
fn physical_payload(input: &p::PhysicalNode) -> Result<(&[p::ChangeEventSpec], p::ValueId), Error> {
    match &input.kind {
        p::NodeKind::ChangeEventExpand {
            events,
            effect_output,
        } => Ok((events, *effect_output)),
        _ => Err(invalid("physical node is not change-event expansion")),
    }
}
fn wire_payload(input: &wire::PhysicalNode) -> Result<&wire::ChangeEventExpandNode, Error> {
    match input.kind.as_ref() {
        Some(wire::physical_node::Kind::ChangeEventExpand(v)) => Ok(v),
        _ => Err(invalid("wire node is not change-event expansion")),
    }
}
fn prepare_encode<'source>(
    input: &p::PhysicalNode,
    values: &EncodedValues<'_, 'source, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    source: usize,
    l: Option<ChangeEventNodeProjectionLimits>,
    w: &mut CompileCheckpoints<'_>,
    mut parent: Option<&mut NodeAdmit<'_>>,
) -> Result<
    (
        ChangeEventNodeProjectionFacts,
        Option<OriginalValueTypeSourceIndex<'source>>,
    ),
    Error,
> {
    let namespace_floor = if parent.is_some() {
        values
            .retained_floor_header()?
            .max(expressions.retained_floor_header_in()?)
    } else {
        0
    };
    // Synchronous snapshots borrow the sole containing-node numerical author.
    // They replace this contribution; they do not create a meter or another B.
    let gate = |model: &Model,
                known: usize,
                parent: &mut Option<&mut NodeAdmit<'_>>|
     -> Result<(), Error> {
        if let Some(admit) = parent.as_deref_mut() {
            model.admit_in(source, values.count(), l, admit)?;
            if source < known.max(namespace_floor) {
                return Err(invalid("Repeat source invoice omits original backing"));
            }
        }
        Ok(())
    };

    let same = std::ptr::eq(values.types(), expressions.types())
        && if parent.is_some() {
            std::ptr::addr_eq(values.original_control(), expressions.original_control())
        } else {
            std::ptr::eq(values.original_control(), expressions.original_control())
        };
    if parent.is_none() {
        w.step()?;
    }
    if !same {
        return Err(invalid(
            "change-event namespaces differ in original type table or control",
        ));
    }
    let (events, effect_output) = physical_payload(input)?;
    let mut known = add(
        physical_header_floor(input)?,
        bytes::<p::ChangeEventSpec>(events.len())?,
    )?;
    let mut model = Model {
        inputs: input.inputs.len(),
        items: add(
            add(input.required_inputs.len(), input.output.columns.len())?,
            events.len(),
        )?,
        refs: add(input.output.columns.len(), 1)?,
        ..Model::default()
    };
    if parent.is_none() {
        gate(&model, known, &mut parent)?;
        count_prefix(model.inputs, model.items, source, known, l, w)?;
        floor(source, values.retained_floor(w)?, w)?;
        floor(source, expressions.retained_floor_observed(w)?, w)?;
    }
    encode_header_requests(input, &mut model)?;
    model.request::<wire::ChangeEvent>(events.len(), 1)?;
    let mut occurrences = 0;
    let mut type_occurrences = 0;
    for event in events {
        model.items = add(model.items, event.assignments.len())?;
        model.refs = add(model.refs, event.assignments.len())?;
        known = add(
            known,
            bytes::<p::ChangeAssignment>(event.assignments.len())?,
        )?;
        model.request::<wire::ChangeAssignment>(event.assignments.len(), 1)?;
        if parent.is_some() {
            occurrences = add(occurrences, usize::from(event.predicate.is_some()))?;
            model.delegated_work = mul(occurrences, expressions.lookup_work_upper_bound()?)?;
        }
        gate(&model, known, &mut parent)?;
        count_prefix(model.inputs, model.items, source, known, l, w)?;
        if parent.is_none() {
            occurrences = add(occurrences, usize::from(event.predicate.is_some()))?;
        }
        gate(&model, known, &mut parent)?;
        w.step()?;
        for assignment in &event.assignments {
            let expression = &assignment.expression;
            let count = assignment
                .write_input
                .as_ref()
                .map_or(0, |d| 1 + usize::from(d.storage.is_some()));
            type_occurrences = add(type_occurrences, count)?;
            // W/T are TypeIds, not fragment ValueIds. Their bounded
            // occurrences feed the original type-index/clone work below;
            // assignment/list and raw scalar charges retain their owners.
            occurrences = add(occurrences, usize::from(expression.is_some()))?;
            if parent.is_some() {
                model.delegated_work = mul(occurrences, expressions.lookup_work_upper_bound()?)?;
            }
            gate(&model, known, &mut parent)?;
            w.step()?;
        }
    }
    model.delegated_work = mul(occurrences, expressions.lookup_work_upper_bound()?)?;
    if parent.is_some() {
        gate(&model, known, &mut parent)?;
        count_prefix(model.inputs, model.items, source, known, l, w)?;
        floor(source, values.retained_floor(w)?, w)?;
        floor(source, expressions.retained_floor_observed(w)?, w)?;
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(&input.output_properties))
    {
        if parent.is_some() {
            let pf = properties::properties_encode_numerical_facts_in(property, source)?;
            properties::check_properties_numerical_facts(pf, l.map(|limits| limits.properties))?;
            model.property(pf)?;
            gate(&model, known, &mut parent)?;
            properties::preflight_encode_observed(
                property,
                source,
                l.map(|limits| limits.properties),
                w,
            )?;
        } else {
            model.property(properties::preflight_encode_observed(
                property,
                source,
                l.map(|limits| limits.properties),
                w,
            )?)?;
        }
        gate(&model, known, &mut parent)?;
        w.step()?;
    }
    let type_sources = if type_occurrences == 0 {
        None
    } else {
        Some(OriginalValueTypeSourceIndex::prepare(
            values.types(),
            mul(type_occurrences, 2)?,
            &mut |facts| {
                model.layout_request(facts.node_layout, facts.allocation_requests_upper_bound)?;
                model.delegated_work =
                    add(model.delegated_work, facts.cumulative_work_upper_bound)?;
                gate(&model, known, &mut parent)?;
                if parent.is_none() {
                    model.numerical_facts(source, values.count(), l)?;
                }
                Ok::<(), Error>(())
            },
            &mut invalid,
            w,
        )?)
    };
    let facts = if let Some(admit) = parent {
        model.facts_in(source, values.count(), l, admit, w)?
    } else {
        model.facts(source, values.count(), l, w)?
    };
    for id in &input.output.columns {
        reference(id.get(), values, w)?;
    }
    reference(effect_output.get(), values, w)?;
    for event in events {
        if let Some(predicate) = event.predicate {
            expression_encode(predicate, expressions, w)?;
        }
        for assignment in &event.assignments {
            let (output, expression) = (&assignment.output, &assignment.expression);
            if let Some(declaration) = &assignment.write_input {
                encode_type_ids(declaration, type_sources.as_ref(), w)?;
            }
            reference(output.get(), values, w)?;
            if let Some(expression) = expression {
                expression_encode(*expression, expressions, w)?;
            }
            w.step()?;
        }
        w.step()?;
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(&input.output_properties))
    {
        physical_property_refs(property, values, w)?;
    }
    Ok((facts, type_sources))
}
fn prepare_decode(
    input: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    source: usize,
    l: Option<ChangeEventNodeProjectionLimits>,
    w: &mut CompileCheckpoints<'_>,
    mut parent: Option<&mut NodeAdmit<'_>>,
) -> Result<ChangeEventNodeProjectionFacts, Error> {
    let values = expressions.values();
    let namespace_floor = if parent.is_some() {
        values
            .retained_floor_header()?
            .max(expressions.retained_floor_header_in()?)
    } else {
        0
    };
    // Synchronous snapshots borrow the sole containing-node numerical author.
    // They replace this contribution; they do not create a meter or another B.
    let gate = |model: &Model,
                known: usize,
                parent: &mut Option<&mut NodeAdmit<'_>>|
     -> Result<(), Error> {
        if let Some(admit) = parent.as_deref_mut() {
            model.admit_in(source, values.count(), l, admit)?;
            if source < known.max(namespace_floor) {
                return Err(invalid("Repeat source invoice omits original backing"));
            }
        }
        Ok(())
    };

    let payload = wire_payload(input)?;
    let port = input
        .output
        .as_ref()
        .ok_or_else(|| invalid("change-event output port is absent"))?;
    if parent.is_none() {
        required(port.node_id, w)?;
    } else {
        port.node_id
            .ok_or_else(|| invalid("node output ID is absent"))?;
    }
    let output_property = input
        .output_properties
        .as_ref()
        .ok_or_else(|| invalid("change-event output properties are absent"))?;
    let mut known = add(
        wire_header_floor(input, port)?,
        bytes::<wire::ChangeEvent>(payload.events.capacity())?,
    )?;
    let mut model = Model {
        inputs: input.input_node_ids.len(),
        items: add(
            add(input.required_inputs.len(), port.value_ids.len())?,
            payload.events.len(),
        )?,
        refs: add(port.value_ids.len(), 1)?,
        ..Model::default()
    };
    if parent.is_none() {
        gate(&model, known, &mut parent)?;
        count_prefix(model.inputs, model.items, source, known, l, w)?;
        floor(source, values.retained_floor(w)?, w)?;
        floor(source, expressions.retained_floor_observed(w)?, w)?;
    }
    decode_header_requests(input, port, &mut model)?;
    model.request::<p::ChangeEventSpec>(payload.events.len(), 2)?;
    let mut occurrences = 0;
    let mut type_work = 0;
    for event in &payload.events {
        model.items = add(model.items, event.assignments.len())?;
        model.refs = add(model.refs, event.assignments.len())?;
        known = add(
            known,
            bytes::<wire::ChangeAssignment>(event.assignments.capacity())?,
        )?;
        model.request::<p::ChangeAssignment>(event.assignments.len(), 2)?;
        if parent.is_some() {
            occurrences = add(occurrences, usize::from(event.predicate_root.is_some()))?;
            model.delegated_work = add(
                mul(occurrences, expressions.lookup_work_upper_bound()?)?,
                type_work,
            )?;
        }
        gate(&model, known, &mut parent)?;
        count_prefix(model.inputs, model.items, source, known, l, w)?;
        if parent.is_none() {
            occurrences = add(occurrences, usize::from(event.predicate_root.is_some()))?;
        }
        gate(&model, known, &mut parent)?;
        w.step()?;
        for assignment in &event.assignments {
            let type_count = usize::from(assignment.write_input_value_type_id.is_some())
                + usize::from(assignment.storage_value_type_id.is_some());
            // Type lookups/clones charge their original work and request
            // contribution; only the assignment output increments Value refs.
            if type_count != 0 {
                let lookup =
                    crate::btree_resources_v2::lookup_work_typed(values.types().value_type_count())
                        .map_err(|error| match error {
                            crate::btree_resources_v2::BTreeResourceError::Arithmetic(_) => {
                                Error::from(
                                    novarocks_type_contract::CompileControlError::ResourceExhausted,
                                )
                            }
                            crate::btree_resources_v2::BTreeResourceError::SourceModel(message) => {
                                invalid(message)
                            }
                        })?;
                type_work = add(
                    type_work,
                    mul(
                        type_count,
                        add(
                            mul(lookup, 2)?,
                            mul(
                                physical_type_v2::value_type_clone_preflight_work_upper_bound(),
                                2,
                            )?,
                        )?,
                    )?,
                )?;
                model.delegated_work = add(
                    mul(occurrences, expressions.lookup_work_upper_bound()?)?,
                    type_work,
                )?;
            }
            occurrences = add(occurrences, usize::from(assignment.root_ordinal.is_some()))?;
            if parent.is_some() {
                model.delegated_work = add(
                    mul(occurrences, expressions.lookup_work_upper_bound()?)?,
                    type_work,
                )?;
            }
            gate(&model, known, &mut parent)?;
            if type_count != 0 && parent.is_none() {
                model.numerical_facts(source, values.count(), l)?;
            }
            w.step()?;
            for id in decode_type_ids(assignment, w)?.into_iter().flatten() {
                let ty = values
                    .types()
                    .value_type_observed(id, w)?
                    .ok_or_else(|| invalid("change-event type reference is absent"))?;
                physical_type_v2::preflight_value_type_clone_admitted(
                    ty,
                    &mut |facts, _| {
                        model.requests =
                            add(model.requests, facts.allocation_requests_upper_bound())?;
                        model.requested = add(
                            model.requested,
                            facts.allocation_request_bytes_upper_bound(),
                        )?;
                        if facts.work_upper_bound()
                            > physical_type_v2::value_type_clone_preflight_work_upper_bound()
                        {
                            return Err(Error::from(
                                novarocks_type_contract::CompileControlError::ResourceExhausted,
                            ));
                        }
                        gate(&model, known, &mut parent)?;
                        if parent.is_none() {
                            model.numerical_facts(source, values.count(), l)?;
                        }
                        Ok::<(), Error>(())
                    },
                    w,
                )?;
            }
        }
    }
    model.delegated_work = add(
        mul(occurrences, expressions.lookup_work_upper_bound()?)?,
        type_work,
    )?;
    if parent.is_some() {
        gate(&model, known, &mut parent)?;
        count_prefix(model.inputs, model.items, source, known, l, w)?;
        floor(source, values.retained_floor(w)?, w)?;
        floor(source, expressions.retained_floor_observed(w)?, w)?;
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(output_property))
    {
        if parent.is_some() {
            let pf = properties::properties_decode_numerical_facts_in(property, source)?;
            properties::check_properties_numerical_facts(pf, l.map(|limits| limits.properties))?;
            model.property(pf)?;
            gate(&model, known, &mut parent)?;
            properties::preflight_decode_observed(
                property,
                source,
                l.map(|limits| limits.properties),
                w,
            )?;
        } else {
            model.property(properties::preflight_decode_observed(
                property,
                source,
                l.map(|limits| limits.properties),
                w,
            )?)?;
        }
        gate(&model, known, &mut parent)?;
        w.step()?;
    }
    let facts = if let Some(admit) = parent {
        model.facts_in(source, values.count(), l, admit, w)?
    } else {
        model.facts(source, values.count(), l, w)?
    };
    for id in &port.value_ids {
        reference(*id, values, w)?;
    }
    reference(required(payload.effect_output_value_id, w)?, values, w)?;
    for event in &payload.events {
        decode_mutation_effect(event.effect, w)?;
        if let Some(predicate) = event.predicate_root {
            expression_decode(predicate, expressions, w)?;
        }
        for assignment in &event.assignments {
            reference(required(assignment.value_id, w)?, values, w)?;
            if let Some(expression) = assignment.root_ordinal {
                expression_decode(expression, expressions, w)?;
            }
            w.step()?;
        }
        w.step()?;
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(output_property))
    {
        wire_property_refs(property, values, w)?;
    }
    Ok(facts)
}
fn encode_type_ids(
    declaration: &novarocks_type_contract::WriteInputDeclaration,
    sources: Option<&OriginalValueTypeSourceIndex<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(u32, Option<u32>), Error> {
    let sources = sources.ok_or_else(|| invalid("change-event original type locator is absent"))?;
    let input = sources
        .source_id_observed(&declaration.input, work)?
        .ok_or_else(|| invalid("change-event input type has no original source author"))?;
    let storage = declaration
        .storage
        .as_ref()
        .map(|storage| {
            sources
                .source_id_observed(storage, work)?
                .ok_or_else(|| invalid("change-event storage type has no original source author"))
        })
        .transpose()?;
    Ok((input, storage))
}
fn decode_type_ids(
    assignment: &wire::ChangeAssignment,
    work: &mut CompileCheckpoints<'_>,
) -> Result<[Option<u32>; 2], Error> {
    let result = if assignment.write_input_value_type_id.is_none()
        && assignment.storage_value_type_id.is_some()
    {
        Err(invalid("change-event storage type requires an input type"))
    } else {
        Ok([
            assignment.write_input_value_type_id,
            assignment.storage_value_type_id,
        ])
    };
    if assignment.write_input_value_type_id.is_some() || assignment.storage_value_type_id.is_some()
    {
        work.step()?;
    }
    result
}
fn emit_encode(
    input: &p::PhysicalNode,
    type_sources: Option<&OriginalValueTypeSourceIndex<'_>>,
    source: usize,
    limits: Option<ChangeEventNodeProjectionLimits>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::PhysicalNode, Error> {
    let (inputs, required_inputs, output_properties, output) =
        encode_header(input, source, limits, work)?;
    let (events, effect_output) = physical_payload(input)?;
    let mut emitted = reserve(events.len(), work)?;
    for event in events {
        let mut assignments = reserve(event.assignments.len(), work)?;
        for assignment in &event.assignments {
            let types = assignment
                .write_input
                .as_ref()
                .map(|declaration| encode_type_ids(declaration, type_sources, work))
                .transpose()?;
            let (value, expression) = (&assignment.output, &assignment.expression);
            assignments.push(wire::ChangeAssignment {
                value_id: Some(value.get()),
                root_ordinal: expression.map(novarocks_expression_contract::RootOrdinal::get),
                write_input_value_type_id: types.map(|(input, _)| input),
                storage_value_type_id: types.and_then(|(_, storage)| storage),
            });
            work.step()?;
        }
        emitted.push(wire::ChangeEvent {
            predicate_root: event
                .predicate
                .map(novarocks_expression_contract::RootOrdinal::get),
            effect: encode_mutation_effect(event.effect),
            assignments,
        });
        work.step()?;
    }
    let node = wire::PhysicalNode {
        expressions: None,
        id: input.id.get(),
        input_node_ids: inputs,
        required_inputs,
        output_properties: Some(output_properties),
        output: Some(output),
        kind: Some(wire::physical_node::Kind::ChangeEventExpand(
            wire::ChangeEventExpandNode {
                events: emitted,
                effect_output_value_id: Some(effect_output.get()),
            },
        )),
    };
    work.step()?;
    Ok(node)
}
fn emit_decode(
    input: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    source: usize,
    limits: Option<ChangeEventNodeProjectionLimits>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<p::PhysicalNode, Error> {
    let (inputs, required_inputs, output_properties, output) =
        decode_header(input, source, limits, work)?;
    let payload = wire_payload(input)?;
    let mut emitted = reserve(payload.events.len(), work)?;
    for event in &payload.events {
        let mut assignments = reserve(event.assignments.len(), work)?;
        for assignment in &event.assignments {
            let [input, storage] = decode_type_ids(assignment, work)?;
            let write_input = input
                .map(|id| -> Result<_, Error> {
                    let types = expressions.values().types();
                    let input = physical_type_v2::clone_value_type_observed(
                        types
                            .value_type_observed(id, work)?
                            .ok_or_else(|| invalid("change-event type reference is absent"))?,
                        work,
                    )?;
                    let storage = storage
                        .map(|id| -> Result<_, Error> {
                            Ok(physical_type_v2::clone_value_type_observed(
                                types.value_type_observed(id, work)?.ok_or_else(|| {
                                    invalid("change-event type reference is absent")
                                })?,
                                work,
                            )?)
                        })
                        .transpose()?;
                    Ok(novarocks_type_contract::WriteInputDeclaration { input, storage })
                })
                .transpose()?;
            assignments.push(p::ChangeAssignment {
                output: p::ValueId::new(required(assignment.value_id, work)?),
                expression: assignment
                    .root_ordinal
                    .map(novarocks_expression_contract::RootOrdinal::new),
                write_input,
            });
            work.step()?;
        }
        emitted.push(p::ChangeEventSpec {
            predicate: event
                .predicate_root
                .map(novarocks_expression_contract::RootOrdinal::new),
            effect: decode_mutation_effect(event.effect, work)?,
            assignments: boxed(assignments, work)?,
        });
        work.step()?;
    }
    let node = p::PhysicalNode {
        expressions: None,
        id: p::NodeId::new(input.id),
        inputs,
        required_inputs,
        output_properties,
        output,
        kind: p::NodeKind::ChangeEventExpand {
            events: boxed(emitted, work)?,
            effect_output: p::ValueId::new(required(payload.effect_output_value_id, work)?),
        },
    };
    work.step()?;
    Ok(node)
}

/// Immutable preparation retains both actual emission loans. It does not rerun
/// source counting or reference validation when consumed.
pub struct PreparedChangeEventNodeEncode<'node, 'namespace, 'loan, 'source, 'control> {
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source: usize,
    limits: Option<ChangeEventNodeProjectionLimits>,
    facts: ChangeEventNodeProjectionFacts,
    type_sources: Option<OriginalValueTypeSourceIndex<'source>>,
}
impl PreparedChangeEventNodeEncode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &ChangeEventNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(wire::PhysicalNode, ChangeEventNodeProjectionFacts), Error> {
        // Retaining this Values loan prevents exchanging the admitted namespace.
        let mut work =
            CompileCheckpoints::try_new(self.values.original_control(), CompilePhase::Encode)?;
        debug_assert!(std::ptr::eq(
            self.values.original_control(),
            self.expressions.original_control()
        ));
        let result = emit_encode(
            self.input,
            self.type_sources.as_ref(),
            self.source,
            self.limits,
            &mut work,
        )
        .map(|node| (node, self.facts));
        finish(work, result)
    }

    /// Emit the already admitted original body in the containing caller scope.
    pub fn emit_in(
        self,
        admit: &mut NodeAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(wire::PhysicalNode, ChangeEventNodeProjectionFacts), Error> {
        if !std::ptr::addr_eq(self.values.original_control(), work.control()) {
            return Err(invalid(
                "node caller does not borrow the original controller",
            ));
        }
        admit(&self.facts)?;
        let node = emit_encode(
            self.input,
            self.type_sources.as_ref(),
            self.source,
            self.limits,
            work,
        )?;
        Ok((node, self.facts))
    }
}
pub fn prepare_change_event_node_encode<'node, 'namespace, 'loan, 'source, 'control>(
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source_retained_bytes: usize,
    limits: ChangeEventNodeProjectionLimits,
) -> Result<PreparedChangeEventNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
    let limits = Some(limits);
    let mut work = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let result = prepare_encode(
        input,
        values,
        expressions,
        source_retained_bytes,
        limits,
        &mut work,
        None,
    );
    let (facts, type_sources) = finish(work, result)?;
    Ok(PreparedChangeEventNodeEncode {
        type_sources,
        input,
        values,
        expressions,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
/// Caller-owned preparation; no entry, footer, or second namespace author.
pub fn prepare_change_event_node_encode_in<'node, 'namespace, 'loan, 'source, 'control>(
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source_retained_bytes: usize,
    limits: impl Into<Option<ChangeEventNodeProjectionLimits>>,
    admit: &mut NodeAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedChangeEventNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
    let limits = limits.into();
    if !std::ptr::addr_eq(values.original_control(), work.control()) {
        return Err(invalid(
            "node caller does not borrow the original controller",
        ));
    }
    let (facts, type_sources) = prepare_encode(
        input,
        values,
        expressions,
        source_retained_bytes,
        limits,
        work,
        Some(admit),
    )?;
    Ok(PreparedChangeEventNodeEncode {
        type_sources,
        input,
        values,
        expressions,
        source: source_retained_bytes,
        limits,
        facts,
    })
}

pub struct PreparedChangeEventNodeDecode<'node, 'namespace, 'loan, 'wire, 'control> {
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    source: usize,
    limits: Option<ChangeEventNodeProjectionLimits>,
    facts: ChangeEventNodeProjectionFacts,
}
impl PreparedChangeEventNodeDecode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &ChangeEventNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(p::PhysicalNode, ChangeEventNodeProjectionFacts), Error> {
        let mut work =
            CompileCheckpoints::try_new(self.expressions.original_control(), CompilePhase::Decode)?;
        let result = emit_decode(
            self.input,
            self.expressions,
            self.source,
            self.limits,
            &mut work,
        )
        .map(|node| (node, self.facts));
        finish(work, result)
    }

    /// Emit the already admitted original body in the containing caller scope.
    pub fn emit_in(
        self,
        admit: &mut NodeAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(p::PhysicalNode, ChangeEventNodeProjectionFacts), Error> {
        if !std::ptr::addr_eq(self.expressions.original_control(), work.control()) {
            return Err(invalid(
                "node caller does not borrow the original controller",
            ));
        }
        admit(&self.facts)?;
        let node = emit_decode(self.input, self.expressions, self.source, self.limits, work)?;
        Ok((node, self.facts))
    }
}
pub fn prepare_change_event_node_decode<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    source_retained_bytes: usize,
    limits: ChangeEventNodeProjectionLimits,
) -> Result<PreparedChangeEventNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
    let limits = Some(limits);
    let mut work =
        CompileCheckpoints::try_new(expressions.original_control(), CompilePhase::Decode)?;
    let result = prepare_decode(
        input,
        expressions,
        source_retained_bytes,
        limits,
        &mut work,
        None,
    );
    let facts = finish(work, result)?;
    Ok(PreparedChangeEventNodeDecode {
        input,
        expressions,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
/// Caller-owned preparation; no entry, footer, or second namespace author.
pub fn prepare_change_event_node_decode_in<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    source_retained_bytes: usize,
    limits: impl Into<Option<ChangeEventNodeProjectionLimits>>,
    admit: &mut NodeAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedChangeEventNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
    let limits = limits.into();
    if !std::ptr::addr_eq(expressions.original_control(), work.control()) {
        return Err(invalid(
            "node caller does not borrow the original controller",
        ));
    }
    let facts = prepare_decode(
        input,
        expressions,
        source_retained_bytes,
        limits,
        work,
        Some(admit),
    )?;
    Ok(PreparedChangeEventNodeDecode {
        input,
        expressions,
        source: source_retained_bytes,
        limits,
        facts,
    })
}

pub fn encode_change_event_node(
    input: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: ChangeEventNodeProjectionLimits,
) -> Result<(wire::PhysicalNode, ChangeEventNodeProjectionFacts), Error> {
    let limits = Some(limits);
    let mut work = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let result = (|| {
        let (facts, type_sources) = prepare_encode(
            input,
            values,
            expressions,
            source_retained_bytes,
            limits,
            &mut work,
            None,
        )?;
        let node = emit_encode(
            input,
            type_sources.as_ref(),
            source_retained_bytes,
            limits,
            &mut work,
        )?;
        Ok((node, facts))
    })();
    finish(work, result)
}
pub fn decode_change_event_node(
    input: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: ChangeEventNodeProjectionLimits,
) -> Result<(p::PhysicalNode, ChangeEventNodeProjectionFacts), Error> {
    let limits = Some(limits);
    let mut work =
        CompileCheckpoints::try_new(expressions.original_control(), CompilePhase::Decode)?;
    let result = (|| {
        let facts = prepare_decode(
            input,
            expressions,
            source_retained_bytes,
            limits,
            &mut work,
            None,
        )?;
        let node = emit_decode(input, expressions, source_retained_bytes, limits, &mut work)?;
        Ok((node, facts))
    })();
    finish(work, result)
}
#[cfg(test)]
mod tests;
