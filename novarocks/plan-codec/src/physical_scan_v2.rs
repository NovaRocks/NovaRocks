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

//! Complete Scan representation through original relation, value and expression
//! namespaces. Fragment retains provider, residual and graph responsibilities.

pub use crate::physical_node_v2::{
    NodeCodecError as ScanNodeCodecError, NodeProjectionFacts as ScanNodeProjectionFacts,
};
use crate::{
    physical_connector_payload_v2::{bytes_shared_upper, bytes_shared_upper_for_mode},
    physical_expression_v2::{DecodedExpressions, EncodedExpressions},
    physical_node_v2::*,
    physical_properties_v2 as properties,
    physical_relation_v2::{
        DecodedRelations, EncodedRelations, PreparedRelationMaterialization,
        RelationProjectionLimits, prepare_relation_materialization_observed,
    },
    physical_value_v2::EncodedValues,
};
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{CompileCheckpoints, CompileControlError, CompilePhase};

#[derive(Clone, Copy, Debug)]
pub struct ScanProjectionLimits<N = NodeProjectionLimits> {
    pub node: N,
    pub relation: RelationProjectionLimits,
}
impl<N: Into<Option<NodeProjectionLimits>>> ScanProjectionLimits<N> {
    fn capture_node_profile(self) -> ScanProjectionLimits<Option<NodeProjectionLimits>> {
        ScanProjectionLimits {
            node: self.node.into(),
            relation: self.relation,
        }
    }
}
type Error = ScanNodeCodecError;
struct Physical<'a> {
    occurrence: p::ProviderReadOccurrenceId,
    relation: &'a p::Relation,
    columns: &'a [(p::ProviderColumnReference, p::ValueId)],
    residuals: &'a [novarocks_expression_contract::RootOrdinal],
    derived: &'a [p::ValueId],
}
fn physical(input: &p::PhysicalNode) -> Result<Physical<'_>, Error> {
    match &input.kind {
        p::NodeKind::Scan {
            occurrence,
            relation,
            provider_outputs,
            residuals,
            derived_values,
        } => Ok(Physical {
            occurrence: *occurrence,
            relation,
            columns: provider_outputs,
            residuals,
            derived: derived_values,
        }),
        _ => Err(invalid("physical node is not Scan")),
    }
}
fn raw(input: &wire::PhysicalNode) -> Result<&wire::ScanNode, Error> {
    match input.kind.as_ref() {
        Some(wire::physical_node::Kind::Scan(v)) => Ok(v),
        _ => Err(invalid("wire node is not Scan")),
    }
}
fn required(id: Option<u32>, work: &mut CompileCheckpoints<'_>) -> Result<u32, Error> {
    let result = id.ok_or_else(|| invalid("Scan required reference is absent"));
    work.step()?;
    result
}
fn expression_encode(
    id: novarocks_expression_contract::RootOrdinal,
    namespace: &EncodedExpressions<'_, '_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let found = namespace
        .root_expression_observed(id.get(), work)?
        .is_some();
    work.step()?;
    if found {
        Ok(())
    } else {
        Err(invalid(
            "Scan residual is absent from original expression emission",
        ))
    }
}
fn expression_decode(
    id: u32,
    namespace: &DecodedExpressions<'_, '_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let found = namespace.root_definition_observed(id, work)?.is_some();
    work.step()?;
    if found {
        Ok(())
    } else {
        Err(invalid(
            "Scan residual is absent from original receiving expressions",
        ))
    }
}
fn lookup_work(entries: usize) -> Result<usize, Error> {
    crate::btree_resources_v2::lookup_work(entries).map_err(invalid)
}
struct SourceWork {
    relations: usize,
    payloads: usize,
    expressions: usize,
    values: usize,
}
fn physical_model(
    input: &p::PhysicalNode,
    body: &Physical<'_>,
    source: usize,
    limits: ScanProjectionLimits<Option<NodeProjectionLimits>>,
    source_work: SourceWork,
    admit: &mut Option<&mut NodeAdmit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Model, Error> {
    let mut model = Model {
        inputs: input.inputs.len(),
        items: add(
            add(input.required_inputs.len(), input.output.columns.len())?,
            add(
                body.columns.len(),
                add(body.residuals.len(), body.derived.len())?,
            )?,
        )?,
        refs: add(
            input.output.columns.len(),
            add(body.columns.len(), body.derived.len())?,
        )?,
        ..Model::default()
    };
    let backing = add(
        bytes::<p::Relation>(1)?,
        add(
            bytes::<(p::ProviderColumnReference, p::ValueId)>(body.columns.len())?,
            add(
                bytes::<novarocks_expression_contract::RootOrdinal>(body.residuals.len())?,
                bytes::<p::ValueId>(body.derived.len())?,
            )?,
        )?,
    )?;
    if admit.is_none() {
        count_prefix(
            model.inputs,
            model.items,
            source,
            add(physical_header_floor(input)?, backing)?,
            limits.node,
            work,
        )?;
    }
    encode_header_requests(input, &mut model)?;
    model.request::<wire::ProviderOutput>(body.columns.len(), 1)?;
    model.request::<u32>(body.residuals.len(), 1)?;
    model.request::<u32>(body.derived.len(), 1)?;
    // The retained relation floor visits at most two fixed-size headers per
    // definition. This conservative source-count term gates it before walking.
    // Pointer associations are full scans in both validation and emission;
    // equal payload contents never permit selecting another original owner.
    model.delegated_work = add(
        mul(source_work.relations, 8)?,
        add(
            mul(source_work.relations, 2)?,
            add(
                mul(mul(body.columns.len(), source_work.payloads)?, 2)?,
                mul(body.residuals.len(), source_work.expressions)?,
            )?,
        )?,
    )?;
    if let Some(callback) = admit.as_mut() {
        model.admit_in(source, source_work.values, limits.node, &mut **callback)?;
        count_prefix(
            model.inputs,
            model.items,
            source,
            add(physical_header_floor(input)?, backing)?,
            limits.node,
            work,
        )?;
    }
    cap(
        add(
            add(256, mul(add(model.items, model.inputs)?, 32)?)?,
            model.delegated_work,
        )?,
        limits.node.map(|limits| limits.max_work),
        work,
    )?;
    Ok(model)
}
fn prepare_encode(
    input: &p::PhysicalNode,
    relations: &EncodedRelations<'_, '_, '_>,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    source: usize,
    limits: ScanProjectionLimits<Option<NodeProjectionLimits>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ScanNodeProjectionFacts, Error> {
    prepare_encode_core(
        input,
        relations,
        values,
        expressions,
        source,
        limits,
        None,
        work,
    )
}
fn prepare_encode_core(
    input: &p::PhysicalNode,
    relations: &EncodedRelations<'_, '_, '_>,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    source: usize,
    limits: ScanProjectionLimits<Option<NodeProjectionLimits>>,
    mut admit: Option<&mut NodeAdmit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ScanNodeProjectionFacts, Error> {
    if admit.is_some() && !std::ptr::addr_eq(work.control(), values.original_control()) {
        return Err(invalid("Scan caller has another original control"));
    }
    let same = std::ptr::eq(relations.types(), values.types())
        && std::ptr::eq(values.types(), expressions.types())
        && (if admit.is_some() {
            std::ptr::addr_eq(relations.original_control(), values.original_control())
        } else {
            std::ptr::eq(relations.original_control(), values.original_control())
        })
        && (if admit.is_some() {
            std::ptr::addr_eq(values.original_control(), expressions.original_control())
        } else {
            std::ptr::eq(values.original_control(), expressions.original_control())
        })
        && std::ptr::eq(relations.reads().payloads(), values.payloads());
    if admit.is_none() {
        work.step()?;
    }
    if !same {
        return Err(invalid(
            "Scan namespaces differ in original type, payload or control owner",
        ));
    }
    let body = physical(input)?;
    let mut model = physical_model(
        input,
        &body,
        source,
        limits,
        SourceWork {
            relations: relations.source_count(),
            payloads: values.payloads().source_count(),
            expressions: expressions.lookup_work_upper_bound()?,
            values: values.count(),
        },
        &mut admit,
        work,
    )?;
    if admit.is_some() {
        work.step()?;
    }
    floor(source, relations.retained_floor_observed(work)?, work)?;
    floor(source, values.retained_floor(work)?, work)?;
    floor(source, expressions.retained_floor_observed(work)?, work)?;
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(&input.output_properties))
    {
        if let Some(callback) = admit.as_mut() {
            let property_facts =
                properties::properties_encode_numerical_facts_in(property, source)?;
            properties::check_properties_numerical_facts(
                property_facts,
                limits.node.map(|limits| limits.properties),
            )?;
            model.property(property_facts)?;
            model.admit_in(source, values.count(), limits.node, &mut **callback)?;
            properties::preflight_encode_observed(
                property,
                source,
                limits.node.map(|limits| limits.properties),
                work,
            )?;
        } else {
            model.property(properties::preflight_encode_observed(
                property,
                source,
                limits.node.map(|limits| limits.properties),
                work,
            )?)?;
        }
        work.step()?;
    }
    let facts = match admit.as_mut() {
        Some(callback) => {
            model.facts_in(source, values.count(), limits.node, &mut **callback, work)?
        }
        None => model.facts(source, values.count(), limits.node, work)?,
    };
    relations.source_id_observed(body.relation, work)?;
    work.step()?;
    for (column, value) in body.columns {
        values
            .payloads()
            .source_id_observed(&column.column_payload, work)?;
        work.step()?;
        reference(value.get(), values, work)?;
    }
    for expression in body.residuals {
        expression_encode(*expression, expressions, work)?;
    }
    for id in body.derived.iter().chain(input.output.columns.iter()) {
        reference(id.get(), values, work)?;
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(&input.output_properties))
    {
        physical_property_refs(property, values, work)?;
    }
    Ok(facts)
}
struct ReadPreparation<'namespace, 'loan, 'wire, 'control> {
    relation: PreparedRelationMaterialization<'namespace, 'loan, 'wire, 'control>,
    facts: ScanNodeProjectionFacts,
}
fn prepare_decode<'namespace, 'loan, 'wire, 'control>(
    input: &wire::PhysicalNode,
    relations: &'namespace DecodedRelations<'loan, 'wire, 'control>,
    expressions: &DecodedExpressions<'loan, 'wire, 'control>,
    source: usize,
    limits: ScanProjectionLimits<Option<NodeProjectionLimits>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ReadPreparation<'namespace, 'loan, 'wire, 'control>, Error> {
    prepare_decode_core(input, relations, expressions, source, limits, None, work)
}
fn prepare_decode_core<'namespace, 'loan, 'wire, 'control>(
    input: &wire::PhysicalNode,
    relations: &'namespace DecodedRelations<'loan, 'wire, 'control>,
    expressions: &DecodedExpressions<'loan, 'wire, 'control>,
    source: usize,
    limits: ScanProjectionLimits<Option<NodeProjectionLimits>>,
    mut admit: Option<&mut NodeAdmit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ReadPreparation<'namespace, 'loan, 'wire, 'control>, Error> {
    if admit.is_some() && !std::ptr::addr_eq(work.control(), relations.original_control()) {
        return Err(invalid("Scan caller has another original control"));
    }
    let values = expressions.values();
    let counted =
        |body: &wire::ScanNode, port: &wire::OutputPort| -> Result<(Model, usize), Error> {
            let model = Model {
                inputs: input.input_node_ids.len(),
                items: add(
                    add(input.required_inputs.len(), port.value_ids.len())?,
                    add(
                        body.provider_outputs.len(),
                        add(body.residual_roots.len(), body.derived_value_ids.len())?,
                    )?,
                )?,
                refs: add(
                    port.value_ids.len(),
                    add(body.provider_outputs.len(), body.derived_value_ids.len())?,
                )?,
                ..Model::default()
            };
            let backing = add(
                bytes::<wire::ProviderOutput>(body.provider_outputs.capacity())?,
                add(
                    bytes::<u32>(body.residual_roots.capacity())?,
                    bytes::<u32>(body.derived_value_ids.capacity())?,
                )?,
            )?;

            Ok((model, backing))
        };

    let same = std::ptr::eq(relations.types(), values.types())
        && (if admit.is_some() {
            std::ptr::addr_eq(relations.original_control(), values.original_control())
        } else {
            std::ptr::eq(relations.original_control(), values.original_control())
        })
        && std::ptr::eq(relations.reads().payloads(), values.payloads());
    if admit.is_some() && !same {
        return Err(invalid(
            "receiving Scan namespaces differ in original type, payload or control owner",
        ));
    }
    let early = if let Some(callback) = admit.as_mut() {
        let body = raw(input)?;
        let port = input
            .output
            .as_ref()
            .ok_or_else(|| invalid("Scan output port is absent"))?;
        let (mut model, backing) = counted(body, port)?;
        decode_header_requests(input, port, &mut model)?;
        model.request::<p::Relation>(1, 1)?;
        model
            .request::<(p::ProviderColumnReference, p::ValueId)>(body.provider_outputs.len(), 2)?;
        model
            .request::<novarocks_expression_contract::RootOrdinal>(body.residual_roots.len(), 2)?;
        model.request::<p::ValueId>(body.derived_value_ids.len(), 2)?;
        model.delegated_work = add(
            mul(relations.source_count(), 8)?,
            add(
                mul(
                    mul(
                        body.provider_outputs.len(),
                        lookup_work(values.payloads().source_count())?,
                    )?,
                    2,
                )?,
                mul(
                    body.residual_roots.len(),
                    expressions.lookup_work_upper_bound()?,
                )?,
            )?,
        )?;
        model.admit_in(source, values.count(), limits.node, &mut **callback)?;
        Some((model, backing))
    } else {
        None
    };
    work.step()?;
    if !same {
        return Err(invalid(
            "receiving Scan namespaces differ in original type, payload or control owner",
        ));
    }
    let body = raw(input)?;
    required(body.occurrence_id, work)?;
    let relation_id = required(body.relation_id, work)?;
    let port = input
        .output
        .as_ref()
        .ok_or_else(|| invalid("Scan output port is absent"))?;
    required(port.node_id, work)?;
    let output_properties = input
        .output_properties
        .as_ref()
        .ok_or_else(|| invalid("Scan output properties are absent"))?;
    let (mut model, backing) = match early {
        Some(counted) => counted,
        None => counted(body, port)?,
    };
    count_prefix(
        model.inputs,
        model.items,
        source,
        add(wire_header_floor(input, port)?, backing)?,
        limits.node,
        work,
    )?;
    model.delegated_work = add(
        mul(relations.source_count(), 8)?,
        add(
            mul(
                mul(
                    body.provider_outputs.len(),
                    lookup_work(values.payloads().source_count())?,
                )?,
                2,
            )?,
            mul(
                body.residual_roots.len(),
                expressions.lookup_work_upper_bound()?,
            )?,
        )?,
    )?;
    cap(
        add(
            add(256, mul(add(model.items, model.inputs)?, 32)?)?,
            model.delegated_work,
        )?,
        limits.node.map(|limits| limits.max_work),
        work,
    )?;
    floor(source, relations.retained_floor_observed(work)?, work)?;
    floor(source, values.retained_floor(work)?, work)?;
    floor(source, expressions.retained_floor_observed(work)?, work)?;
    if admit.is_none() {
        decode_header_requests(input, port, &mut model)?;
        model.request::<p::Relation>(1, 1)?;
        model
            .request::<(p::ProviderColumnReference, p::ValueId)>(body.provider_outputs.len(), 2)?;
        model
            .request::<novarocks_expression_contract::RootOrdinal>(body.residual_roots.len(), 2)?;
        model.request::<p::ValueId>(body.derived_value_ids.len(), 2)?;
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(output_properties))
    {
        if let Some(callback) = admit.as_mut() {
            let property_facts =
                properties::properties_decode_numerical_facts_in(property, source)?;
            properties::check_properties_numerical_facts(
                property_facts,
                limits.node.map(|limits| limits.properties),
            )?;
            model.property(property_facts)?;
            model.admit_in(source, values.count(), limits.node, &mut **callback)?;
            properties::preflight_decode_observed(
                property,
                source,
                limits.node.map(|limits| limits.properties),
                work,
            )?;
        } else {
            model.property(properties::preflight_decode_observed(
                property,
                source,
                limits.node.map(|limits| limits.properties),
                work,
            )?)?;
        }
        work.step()?;
    }
    // These bounded lookups determine whether each actual Bytes clone can
    // promote a Shared owner. Even equal payload aliases count per occurrence.
    match admit.as_mut() {
        Some(callback) => {
            model.facts_in(source, values.count(), limits.node, &mut **callback, work)?
        }
        None => model.facts(source, values.count(), limits.node, work)?,
    };
    for column in &body.provider_outputs {
        let id = required(column.column_payload_id, work)?;
        let payload = if let Some(callback) = admit.as_mut() {
            values.payloads().payload_captured_in(
                id,
                &mut |payload, _| {
                    if !payload.payload().is_empty() {
                        model.request::<u8>(bytes_shared_upper_for_mode(true)?, 1)?;
                    }
                    model.admit_in(source, values.count(), limits.node, &mut **callback)?;
                    Ok::<(), Error>(())
                },
                work,
            )?
        } else {
            values.payloads().payload_observed(id, work)?
        };
        work.step()?;
        let payload = payload.ok_or_else(|| invalid("Scan column payload reference is unknown"))?;
        if admit.is_none() && !payload.payload().is_empty() {
            model.request::<u8>(bytes_shared_upper()?, 1)?;
        }
        reference(required(column.value_id, work)?, values, work)?;
    }
    for id in body.derived_value_ids.iter().chain(port.value_ids.iter()) {
        reference(*id, values, work)?;
    }
    for expression in &body.residual_roots {
        expression_decode(*expression, expressions, work)?;
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(output_properties))
    {
        wire_property_refs(property, values, work)?;
    }
    let prefix = match admit.as_mut() {
        Some(callback) => {
            model.facts_in(source, values.count(), limits.node, &mut **callback, work)?
        }
        None => model.facts(source, values.count(), limits.node, work)?,
    };
    let remaining = limits
        .node
        .max_work
        .checked_sub(prefix.cumulative_work_upper_bound)
        .ok_or_else(|| invalid("Scan relation has no remaining work envelope"))?;
    let relation_limits = RelationProjectionLimits {
        max_work: limits.relation.max_work.min(remaining),
        ..limits.relation
    };
    let relation = if let Some(callback) = admit.as_mut() {
        let base = model;
        let mut update = |child: &crate::physical_relation_v2::RelationProjectionFacts| {
            let mut merged = base;
            merged.requests = merged
                .requests
                .checked_add(child.allocation_requests_upper_bound)
                .ok_or(CompileControlError::ResourceExhausted)?;
            merged.requested = merged
                .requested
                .checked_add(child.allocation_request_bytes_upper_bound)
                .ok_or(CompileControlError::ResourceExhausted)?;
            merged.delegated_work = merged
                .delegated_work
                .checked_add(child.cumulative_work_upper_bound)
                .ok_or(CompileControlError::ResourceExhausted)?;
            let facts = merged
                .numerical_facts(source, values.count(), limits.node)
                .map_err(|error| match error {
                    Error::Control(cause) => cause,
                    _ => CompileControlError::ResourceExhausted,
                })?;
            callback(&facts)
        };
        crate::physical_relation_v2::prepare_relation_materialization_in(
            relations,
            relation_id,
            source,
            relation_limits,
            &mut update,
            work,
        )?
    } else {
        prepare_relation_materialization_observed(
            relations,
            relation_id,
            source,
            relation_limits,
            work,
        )?
    };
    let child = relation.facts();
    model.requests = add(model.requests, child.allocation_requests_upper_bound)?;
    model.requested = add(model.requested, child.allocation_request_bytes_upper_bound)?;
    // The selected owner's fact already covers preparation plus its sole
    // decode-core preflight/emission; do not add a fourth grammar pass.
    model.delegated_work = add(model.delegated_work, child.cumulative_work_upper_bound)?;
    let facts = match admit.as_mut() {
        Some(callback) => {
            model.facts_in(source, values.count(), limits.node, &mut **callback, work)?
        }
        None => model.facts(source, values.count(), limits.node, work)?,
    };
    Ok(ReadPreparation { relation, facts })
}
fn emit_encode(
    input: &p::PhysicalNode,
    relations: &EncodedRelations<'_, '_, '_>,
    values: &EncodedValues<'_, '_, '_>,
    source: usize,
    limits: ScanProjectionLimits<Option<NodeProjectionLimits>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::PhysicalNode, Error> {
    let body = physical(input)?;
    let relation_id = relations.source_id_observed(body.relation, work)?;
    work.step()?;
    let (inputs, required_inputs, output_properties, output) =
        encode_header(input, source, limits.node, work)?;
    let mut columns = reserve(body.columns.len(), work)?;
    for (column, value) in body.columns {
        let id = values
            .payloads()
            .source_id_observed(&column.column_payload, work)?;
        columns.push(wire::ProviderOutput {
            column_payload_id: Some(id),
            value_id: Some(value.get()),
        });
        work.step()?;
    }
    let mut residuals = reserve(body.residuals.len(), work)?;
    for id in body.residuals {
        residuals.push(id.get());
        work.step()?;
    }
    let node = wire::PhysicalNode {
        expressions: None,
        id: input.id.get(),
        input_node_ids: inputs,
        required_inputs,
        output_properties: Some(output_properties),
        output: Some(output),
        kind: Some(wire::physical_node::Kind::Scan(wire::ScanNode {
            occurrence_id: Some(body.occurrence.get()),
            relation_id: Some(relation_id),
            provider_outputs: columns,
            residual_roots: residuals,
            derived_value_ids: encode_ids(body.derived, work)?,
        })),
    };
    work.step()?;
    Ok(node)
}
fn emit_decode(
    input: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    prepared: ReadPreparation<'_, '_, '_, '_>,
    source: usize,
    limits: ScanProjectionLimits<Option<NodeProjectionLimits>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(p::PhysicalNode, ScanNodeProjectionFacts), Error> {
    emit_decode_core(input, expressions, prepared, source, limits, None, work)
}
fn emit_decode_core(
    input: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    prepared: ReadPreparation<'_, '_, '_, '_>,
    source: usize,
    limits: ScanProjectionLimits<Option<NodeProjectionLimits>>,
    admit: Option<&mut NodeAdmit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(p::PhysicalNode, ScanNodeProjectionFacts), Error> {
    let body = raw(input)?;
    let relation = if let Some(callback) = admit {
        let sealed = *prepared.relation.facts();
        let mut update = |child: &crate::physical_relation_v2::RelationProjectionFacts| {
            if child.allocation_requests_upper_bound > sealed.allocation_requests_upper_bound
                || child.allocation_request_bytes_upper_bound
                    > sealed.allocation_request_bytes_upper_bound
                || child.cumulative_work_upper_bound > sealed.cumulative_work_upper_bound
            {
                return Err(CompileControlError::ResourceExhausted);
            }
            callback(&prepared.facts)
        };
        prepared.relation.emit_in(&mut update, work)?
    } else {
        prepared.relation.emit_observed(work)?
    };
    work.flush()?;
    let relation = Box::new(relation);
    work.flush()?;
    let (inputs, required_inputs, output_properties, output) =
        decode_header(input, source, limits.node, work)?;
    let mut columns = reserve(body.provider_outputs.len(), work)?;
    for column in &body.provider_outputs {
        let id = required(column.column_payload_id, work)?;
        let payload = expressions.values().payloads().payload_observed(id, work)?;
        work.step()?;
        let payload =
            payload.ok_or_else(|| invalid("prepared Scan column payload reference is unknown"))?;
        work.flush()?;
        let column_ref = p::ProviderColumnReference {
            column_payload: payload.clone(),
        };
        work.flush()?;
        columns.push((
            column_ref,
            p::ValueId::new(required(column.value_id, work)?),
        ));
        work.step()?;
    }
    let mut residuals = reserve(body.residual_roots.len(), work)?;
    for id in &body.residual_roots {
        residuals.push(novarocks_expression_contract::RootOrdinal::new(*id));
        work.step()?;
    }
    let node = p::PhysicalNode {
        expressions: None,
        id: p::NodeId::new(input.id),
        inputs,
        required_inputs,
        output_properties,
        output,
        kind: p::NodeKind::Scan {
            occurrence: p::ProviderReadOccurrenceId::new(required(body.occurrence_id, work)?),
            relation,
            provider_outputs: boxed(columns, work)?,
            residuals: boxed(residuals, work)?,
            derived_values: decode_ids(&body.derived_value_ids, work)?,
        },
    };
    work.step()?;
    Ok((node, prepared.facts))
}

/// Sealed preparation retains the original full namespace loans.
pub struct PreparedScanNodeEncode<'node, 'namespace, 'loan, 'source, 'control> {
    input: &'node p::PhysicalNode,
    relations: &'namespace EncodedRelations<'loan, 'source, 'control>,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source: usize,
    limits: ScanProjectionLimits<Option<NodeProjectionLimits>>,
    facts: ScanNodeProjectionFacts,
}
impl PreparedScanNodeEncode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &ScanNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(wire::PhysicalNode, ScanNodeProjectionFacts), Error> {
        let mut work =
            CompileCheckpoints::try_new(self.values.original_control(), CompilePhase::Encode)?;
        debug_assert!(std::ptr::eq(
            self.values.original_control(),
            self.expressions.original_control()
        ));
        let result = emit_encode(
            self.input,
            self.relations,
            self.values,
            self.source,
            self.limits,
            &mut work,
        )
        .map(|node| (node, self.facts));
        finish(work, result)
    }
}
pub fn prepare_scan_node_encode<'node, 'namespace, 'loan, 'source, 'control>(
    input: &'node p::PhysicalNode,
    relations: &'namespace EncodedRelations<'loan, 'source, 'control>,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source_retained_bytes: usize,
    limits: ScanProjectionLimits,
) -> Result<PreparedScanNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
    let limits = limits.capture_node_profile();
    let mut work = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let result = prepare_encode(
        input,
        relations,
        values,
        expressions,
        source_retained_bytes,
        limits,
        &mut work,
    );
    let facts = finish(work, result)?;
    Ok(PreparedScanNodeEncode {
        input,
        relations,
        values,
        expressions,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
pub struct PreparedScanNodeDecode<'node, 'namespace, 'loan, 'wire, 'control> {
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    prepared: ReadPreparation<'namespace, 'loan, 'wire, 'control>,
    source: usize,
    limits: ScanProjectionLimits<Option<NodeProjectionLimits>>,
}
impl PreparedScanNodeDecode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &ScanNodeProjectionFacts {
        &self.prepared.facts
    }
    pub fn emit(self) -> Result<(p::PhysicalNode, ScanNodeProjectionFacts), Error> {
        let mut work =
            CompileCheckpoints::try_new(self.expressions.original_control(), CompilePhase::Decode)?;
        let result = emit_decode(
            self.input,
            self.expressions,
            self.prepared,
            self.source,
            self.limits,
            &mut work,
        );
        finish(work, result)
    }
}
pub fn prepare_scan_node_decode<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    relations: &'namespace DecodedRelations<'loan, 'wire, 'control>,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    source_retained_bytes: usize,
    limits: ScanProjectionLimits,
) -> Result<PreparedScanNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
    let limits = limits.capture_node_profile();
    let mut work =
        CompileCheckpoints::try_new(expressions.original_control(), CompilePhase::Decode)?;
    let result = prepare_decode(
        input,
        relations,
        expressions,
        source_retained_bytes,
        limits,
        &mut work,
    );
    let prepared = finish(work, result)?;
    Ok(PreparedScanNodeDecode {
        input,
        expressions,
        prepared,
        source: source_retained_bytes,
        limits,
    })
}
pub fn encode_scan_node(
    input: &p::PhysicalNode,
    relations: &EncodedRelations<'_, '_, '_>,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: ScanProjectionLimits,
) -> Result<(wire::PhysicalNode, ScanNodeProjectionFacts), Error> {
    let limits = limits.capture_node_profile();
    let mut work = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let result = (|| {
        let facts = prepare_encode(
            input,
            relations,
            values,
            expressions,
            source_retained_bytes,
            limits,
            &mut work,
        )?;
        let node = emit_encode(
            input,
            relations,
            values,
            source_retained_bytes,
            limits,
            &mut work,
        )?;
        Ok((node, facts))
    })();
    finish(work, result)
}
pub fn decode_scan_node(
    input: &wire::PhysicalNode,
    relations: &DecodedRelations<'_, '_, '_>,
    expressions: &DecodedExpressions<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: ScanProjectionLimits,
) -> Result<(p::PhysicalNode, ScanNodeProjectionFacts), Error> {
    let limits = limits.capture_node_profile();
    let mut work =
        CompileCheckpoints::try_new(expressions.original_control(), CompilePhase::Decode)?;
    let result = prepare_decode(
        input,
        relations,
        expressions,
        source_retained_bytes,
        limits,
        &mut work,
    )
    .and_then(|prepared| {
        emit_decode(
            input,
            expressions,
            prepared,
            source_retained_bytes,
            limits,
            &mut work,
        )
    });
    finish(work, result)
}

/// Compose the original Scan author in the caller's scope, replacing this node's current contribution.
pub fn prepare_scan_node_encode_in<'node, 'namespace, 'loan, 'source, 'control>(
    input: &'node p::PhysicalNode,
    relations: &'namespace EncodedRelations<'loan, 'source, 'control>,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source: usize,
    limits: ScanProjectionLimits<impl Into<Option<NodeProjectionLimits>>>,
    admit: &mut NodeAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedScanNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
    let limits = limits.capture_node_profile();
    let facts = prepare_encode_core(
        input,
        relations,
        values,
        expressions,
        source,
        limits,
        Some(admit),
        work,
    )?;
    Ok(PreparedScanNodeEncode {
        input,
        relations,
        values,
        expressions,
        source,
        limits,
        facts,
    })
}
pub fn prepare_scan_node_decode_in<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    relations: &'namespace DecodedRelations<'loan, 'wire, 'control>,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    source: usize,
    limits: ScanProjectionLimits<impl Into<Option<NodeProjectionLimits>>>,
    admit: &mut NodeAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedScanNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
    let limits = limits.capture_node_profile();
    let prepared = prepare_decode_core(
        input,
        relations,
        expressions,
        source,
        limits,
        Some(admit),
        work,
    )?;
    Ok(PreparedScanNodeDecode {
        input,
        expressions,
        source,
        limits,
        prepared,
    })
}
impl PreparedScanNodeEncode<'_, '_, '_, '_, '_> {
    pub fn emit_in(
        self,
        admit: &mut NodeAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(wire::PhysicalNode, ScanNodeProjectionFacts), Error> {
        if !std::ptr::addr_eq(work.control(), self.values.original_control()) {
            return Err(invalid("Scan caller has another original control"));
        }
        admit(&self.facts)?;
        Ok((
            emit_encode(
                self.input,
                self.relations,
                self.values,
                self.source,
                self.limits,
                work,
            )?,
            self.facts,
        ))
    }
}
impl PreparedScanNodeDecode<'_, '_, '_, '_, '_> {
    pub fn emit_in(
        self,
        admit: &mut NodeAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(p::PhysicalNode, ScanNodeProjectionFacts), Error> {
        if !std::ptr::addr_eq(work.control(), self.expressions.original_control()) {
            return Err(invalid("Scan caller has another original control"));
        }
        admit(&self.prepared.facts)?;
        emit_decode_core(
            self.input,
            self.expressions,
            self.prepared,
            self.source,
            self.limits,
            Some(admit),
            work,
        )
    }
}
#[cfg(test)]
mod tests;
