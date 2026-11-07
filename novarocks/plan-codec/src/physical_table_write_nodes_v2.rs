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

//! Complete writer/finisher representation with original sealed namespaces.
//! Provider recipes, placement, writer schema roles and aggregate compatibility
//! remain the original Fragment/Package owners. Source invoices are not grants.

pub use crate::physical_node_v2::{
    NodeCodecError as TableWriteNodeCodecError,
    NodeProjectionFacts as TableWriteNodeProjectionFacts,
};
use crate::{
    physical_aggregate_binding_v2::{
        MaterializedAggregateBindings, copy_aggregate_binding_observed,
        preflight_aggregate_binding_copy_counts, preflight_aggregate_binding_copy_counts_in,
        preflight_aggregate_binding_copy_types, preflight_aggregate_binding_copy_types_in,
    },
    physical_binding_v2::{BindingProjectionLimits, MaterializationModel},
    physical_connector_payload_v2::{bytes_shared_upper, bytes_shared_upper_for_mode},
    physical_expression_v2::{DecodedExpressions, EncodedExpressions},
    physical_node_v2::*,
    physical_properties_v2 as properties,
    physical_value_v2::EncodedValues,
    physical_writer_grouped_unpivot_v2::{
        self as grouped, GroupedNodeAdmission, GroupedProjection,
    },
    physical_writer_schema_v2::{
        self as schema, WriterSchemaNodeAdmission, WriterSchemaProjection,
        WriterSchemaProjectionLimits,
    },
};
use novarocks_connector_contract::WriteTargetOrdinal;
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
type Error = TableWriteNodeCodecError;
#[derive(Clone, Copy, Debug)]
pub struct TableWriteNodeProjectionLimits {
    pub node: NodeProjectionLimits,
    pub binding: BindingProjectionLimits,
    pub schema: WriterSchemaProjectionLimits,
}
/// Ordered original type roots, separate from the values a target reads.
#[derive(Clone, Copy, Debug)]
pub enum TableWriteTypeIds<'a> {
    Writer {
        target_fields: &'a [u32],
        output_schema: &'a [u32],
    },
    Finish {
        input_schema: &'a [u32],
        output_schema: &'a [u32],
    },
}
fn completed<T>(r: Result<T, Error>, w: &mut CompileCheckpoints<'_>) -> Result<T, Error> {
    if matches!(&r, Err(Error::Control(_))) {
        return r;
    }
    w.step()?;
    r
}
fn required(id: Option<u32>, w: &mut CompileCheckpoints<'_>) -> Result<u32, Error> {
    completed(
        id.ok_or_else(|| invalid("writer required reference is absent")),
        w,
    )
}
fn ordinal(id: u32, w: &mut CompileCheckpoints<'_>) -> Result<WriteTargetOrdinal, Error> {
    completed(
        WriteTargetOrdinal::try_new(id)
            .map_err(|_| invalid("writer target ordinal exceeds the contract limit")),
        w,
    )
}
enum Body<'a> {
    Writer(&'a p::WriterTarget),
    Finish(&'a p::WriterFinishSpec),
}
fn physical(n: &p::PhysicalNode) -> Result<Body<'_>, Error> {
    match &n.kind {
        p::NodeKind::TableWriter { target } => Ok(Body::Writer(target)),
        p::NodeKind::TableFinish(v) => Ok(Body::Finish(v)),
        _ => Err(invalid("node is not TableWriter or TableFinish")),
    }
}
enum Raw<'a> {
    Writer(&'a wire::WriterTarget),
    Finish(&'a wire::WriterFinish),
}
fn raw(n: &wire::PhysicalNode) -> Result<Raw<'_>, Error> {
    match &n.kind {
        Some(wire::physical_node::Kind::TableWriter(v)) => Ok(Raw::Writer(v)),
        Some(wire::physical_node::Kind::TableFinish(v)) => Ok(Raw::Finish(v)),
        _ => Err(invalid("node is not TableWriter or TableFinish")),
    }
}
fn calls<'a>(b: &Body<'a>) -> &'a [p::WriterAggregateCall] {
    match b {
        Body::Writer(v) => &v.partial_aggregates,
        Body::Finish(v) => &v.final_aggregates,
    }
}
fn raw_calls<'a>(b: &Raw<'a>) -> &'a [wire::WriterAggregateCall] {
    match b {
        Raw::Writer(v) => &v.partial_aggregates,
        Raw::Finish(v) => &v.final_aggregates,
    }
}
fn raw_schema<'a>(
    s: Option<&'a wire::WriterRelationSchema>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a wire::WriterRelationSchema, Error> {
    completed(
        s.ok_or_else(|| invalid("writer relation schema is absent")),
        w,
    )
}
fn binding<'a>(
    id: u32,
    a: &'a MaterializedAggregateBindings<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a p::AggregateBinding, Error> {
    let b = a.definition_observed(id, w)?;
    completed(
        b.ok_or_else(|| invalid("writer aggregate binding is not in original namespace")),
        w,
    )
}
fn parent(
    model: Model,
    values: usize,
    l: TableWriteNodeProjectionLimits,
    source: usize,
) -> WriterSchemaProjection {
    WriterSchemaProjection {
        source,
        limits: l.schema,
        parent: WriterSchemaNodeAdmission {
            base: model,
            values,
            limits: l.node,
        },
    }
}
fn merge_schema(
    model: &mut Model,
    f: schema::WriterSchemaProjectionFacts,
    values: usize,
    l: TableWriteNodeProjectionLimits,
) -> Result<(), Error> {
    *model = WriterSchemaNodeAdmission {
        base: *model,
        values,
        limits: l.node,
    }
    .merge(f)?;
    Ok(())
}
fn physical_distribution_refs(
    d: &p::Distribution,
    values: &EncodedValues<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    for id in d.value_references() {
        reference(id.get(), values, w)?;
        w.step()?;
    }
    Ok(())
}
fn wire_distribution_refs(
    d: &wire::Distribution,
    e: &DecodedExpressions<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let keys = match d.kind.as_ref() {
        Some(wire::distribution::Kind::Hash(v)) => &v.key_value_ids[..],
        Some(wire::distribution::Kind::ConditionalKey(v)) => {
            reference(
                v.applies_value_id
                    .ok_or_else(|| invalid("conditional partition applicability is absent"))?,
                e.values(),
                w,
            )?;
            &v.key_value_ids[..]
        }
        Some(wire::distribution::Kind::BucketShuffle(v)) => &v.key_value_ids[..],
        _ => &[],
    };
    for id in keys {
        reference(*id, e.values(), w)?;
        w.step()?;
    }
    Ok(())
}
fn distribution_limits(
    f: properties::PhysicalPropertyProjectionFacts,
    l: properties::PhysicalPropertyProjectionLimits,
) -> Result<(), Error> {
    check_cap(f.value_reference_count, l.max_value_references)?;
    check_cap(f.allocation_requests_upper_bound, l.max_allocation_requests)?;
    check_cap(
        f.allocation_request_bytes_upper_bound,
        l.max_allocation_request_bytes,
    )?;
    check_cap(
        f.coexisting_source_and_request_bytes_upper_bound,
        l.max_coexisting_source_and_request_bytes,
    )?;
    check_cap(f.cumulative_work_upper_bound, l.max_work)
}
fn base(
    inputs: usize,
    required: usize,
    outputs: usize,
    list: usize,
    refs: usize,
) -> Result<Model, Error> {
    Ok(Model {
        inputs,
        refs: add(outputs, refs)?,
        items: add(add(required, outputs)?, list)?,
        ..Model::default()
    })
}
fn finish_mode(value: i32) -> Result<p::FinishCountMode, Error> {
    match wire::FinishCountMode::try_from(value) {
        Ok(wire::FinishCountMode::LogicalChanges) => Ok(p::FinishCountMode::LogicalChanges),
        Ok(wire::FinishCountMode::WriterReported) => Ok(p::FinishCountMode::WriterReported),
        Ok(wire::FinishCountMode::ExternalOwner) => Ok(p::FinishCountMode::ExternalOwner),
        _ => Err(invalid("finish count mode is absent or unknown")),
    }
}
fn finish_roles(
    mode: p::FinishCountMode,
    writer: u32,
    inputs: &p::PerKind<u32>,
    columns: &p::PerKind<p::SummaryColumns>,
) -> Result<(), Error> {
    let logical = matches!(mode, p::FinishCountMode::LogicalChanges);
    if logical != inputs.write_counts.is_some() {
        return Err(invalid("finish count mode and WriteCounts input disagree"));
    }
    for kind in p::SummaryKind::ALL {
        if inputs.get(kind).is_some() != columns.get(kind).is_some()
            || columns
                .get(kind)
                .is_some_and(|columns| columns.kind() != kind)
            || inputs.get(kind) == Some(&writer)
        {
            return Err(invalid("finish exact summary input roles disagree"));
        }
    }
    if inputs.write_counts.is_some() && inputs.write_counts == inputs.delete_verification {
        return Err(invalid("finish summary input ports must be distinct"));
    }
    Ok(())
}
fn physical_finish_references(value: &p::WriterFinishSpec) -> Result<usize, Error> {
    finish_roles(
        value.count_mode,
        value.inputs.writer_results,
        &value.inputs.summaries,
        &value.summaries,
    )?;
    Ok(value
        .summaries
        .iter()
        .map(|(_, columns)| columns.values().len())
        .sum())
}
/// The closed kind cardinality is a representation law, not a resource quota.
/// At most two fixed-width entries are read before the original parent gate.
fn wire_finish_columns(
    value: &wire::WriterFinish,
) -> Result<(p::PerKind<u32>, p::PerKind<p::SummaryColumns>), Error> {
    if value.summaries.len() > p::SummaryKind::ALL.len() {
        return Err(invalid("finish repeats a closed summary kind"));
    }
    let writer = value
        .writer_results_input
        .ok_or_else(|| invalid("finish writer input role is absent"))?;
    let mut inputs = p::PerKind::empty();
    let mut columns = p::PerKind::empty();
    for entry in &value.summaries {
        let kind = crate::physical_summary_v2::decode_kind(entry.kind).map_err(invalid)?;
        let input = entry
            .input
            .ok_or_else(|| invalid("finish summary input is absent"))?;
        let fields = entry
            .columns
            .as_ref()
            .ok_or_else(|| invalid("finish summary columns are absent"))?;
        let fields = crate::physical_summary_v2::decode_columns_shape(fields)?;
        if fields.kind() != kind || inputs.get(kind).is_some() {
            return Err(invalid("finish summary kinds differ or repeat"));
        }
        inputs.insert(kind, input);
        columns.insert(kind, fields);
    }
    finish_roles(finish_mode(value.count_mode)?, writer, &inputs, &columns)?;
    Ok((inputs, columns))
}
fn wire_finish_references(value: &wire::WriterFinish) -> Result<usize, Error> {
    Ok(wire_finish_columns(value)?
        .1
        .iter()
        .map(|(_, c)| c.values().len())
        .sum())
}
fn wire_finish_backing(value: &wire::WriterFinish) -> Result<usize, Error> {
    wire_finish_columns(value)?;
    let mut known = bytes::<wire::SummaryFinishInput>(value.summaries.capacity())?;
    for entry in &value.summaries {
        known = add(
            known,
            bytes::<u32>(
                entry
                    .columns
                    .as_ref()
                    .expect("checked columns")
                    .value_ids
                    .capacity(),
            )?,
        )?;
    }
    Ok(known)
}
fn finish_encode_requests(value: &p::WriterFinishSpec, m: &mut Model) -> Result<(), Error> {
    m.request::<wire::SummaryFinishInput>(value.summaries.iter().count(), 1)?;
    for (_, columns) in value.summaries.iter() {
        m.request::<u32>(columns.values().len(), 1)?;
    }
    Ok(())
}
fn encode_finish_summaries(
    value: &p::WriterFinishSpec,
    w: &mut CompileCheckpoints<'_>,
) -> Result<Vec<wire::SummaryFinishInput>, Error> {
    let mut out = reserve(value.summaries.iter().count(), w)?;
    for (kind, columns) in value.summaries.iter() {
        out.push(wire::SummaryFinishInput {
            kind: crate::physical_summary_v2::encode_kind(kind),
            input: value.inputs.summaries.get(kind).copied(),
            columns: Some(crate::physical_summary_v2::encode_columns(*columns, w)?),
        });
        w.step()?;
    }
    Ok(out)
}
fn encode_model(n: &p::PhysicalNode, b: &Body<'_>) -> Result<(Model, usize), Error> {
    let c = calls(b).len();
    let (list, refs, backing) = match b {
        Body::Writer(t) => (
            add(t.input.len(), c)?,
            add(
                add(t.input.len(), t.target_fields.len())?,
                add(t.output_schema.fields.len(), mul(2, c)?)?,
            )?,
            add(
                bytes::<p::ValueId>(t.input.len())?,
                add(
                    bytes::<p::WriterTargetField>(t.target_fields.len())?,
                    add(
                        bytes::<p::WriterRelationField>(t.output_schema.fields.len())?,
                        bytes::<p::WriterAggregateCall>(c)?,
                    )?,
                )?,
            )?,
        ),
        Body::Finish(t) => (
            add(
                add(
                    add(t.expected_target_ordinals.len(), c)?,
                    t.summaries.iter().count(),
                )?,
                physical_finish_references(t)?,
            )?,
            add(
                add(
                    add(t.input_schema.fields.len(), t.output_schema.fields.len())?,
                    mul(2, c)?,
                )?,
                physical_finish_references(t)?,
            )?,
            add(
                bytes::<WriteTargetOrdinal>(t.expected_target_ordinals.len())?,
                add(
                    bytes::<p::WriterRelationField>(t.input_schema.fields.len())?,
                    add(
                        bytes::<p::WriterRelationField>(t.output_schema.fields.len())?,
                        bytes::<p::WriterAggregateCall>(c)?,
                    )?,
                )?,
            )?,
        ),
    };
    Ok((
        base(
            n.inputs.len(),
            n.required_inputs.len(),
            n.output.columns.len(),
            list,
            refs,
        )?,
        add(physical_header_floor(n)?, backing)?,
    ))
}
fn decode_model(
    n: &wire::PhysicalNode,
    b: &Raw<'_>,
    port: &wire::OutputPort,
    observed: bool,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(Model, usize), Error> {
    let c = raw_calls(b).len();
    let (list, refs, backing) = match b {
        Raw::Writer(t) => {
            let s = if observed {
                t.output_schema
                    .as_ref()
                    .ok_or_else(|| invalid("writer relation schema is absent"))?
            } else {
                raw_schema(t.output_schema.as_ref(), w)?
            };
            (
                add(t.input_value_ids.len(), c)?,
                add(
                    add(t.input_value_ids.len(), t.target_fields.len())?,
                    add(s.fields.len(), mul(2, c)?)?,
                )?,
                add(
                    bytes::<u32>(t.input_value_ids.capacity())?,
                    add(
                        bytes::<wire::WriterTargetField>(t.target_fields.capacity())?,
                        add(
                            bytes::<wire::WriterRelationField>(s.fields.capacity())?,
                            bytes::<wire::WriterAggregateCall>(t.partial_aggregates.capacity())?,
                        )?,
                    )?,
                ),
            )
        }
        Raw::Finish(t) => {
            let i = if observed {
                t.input_schema
                    .as_ref()
                    .ok_or_else(|| invalid("writer relation schema is absent"))?
            } else {
                raw_schema(t.input_schema.as_ref(), w)?
            };
            let o = if observed {
                t.output_schema
                    .as_ref()
                    .ok_or_else(|| invalid("writer relation schema is absent"))?
            } else {
                raw_schema(t.output_schema.as_ref(), w)?
            };
            (
                add(
                    add(add(t.expected_target_ordinals.len(), c)?, t.summaries.len())?,
                    wire_finish_references(t)?,
                )?,
                add(
                    add(add(i.fields.len(), o.fields.len())?, mul(2, c)?)?,
                    wire_finish_references(t)?,
                )?,
                add(
                    add(
                        bytes::<u32>(t.expected_target_ordinals.capacity())?,
                        wire_finish_backing(t)?,
                    )?,
                    add(
                        bytes::<wire::WriterRelationField>(i.fields.capacity())?,
                        add(
                            bytes::<wire::WriterRelationField>(o.fields.capacity())?,
                            bytes::<wire::WriterAggregateCall>(t.final_aggregates.capacity())?,
                        )?,
                    )?,
                ),
            )
        }
    };
    Ok((
        base(
            n.input_node_ids.len(),
            n.required_inputs.len(),
            port.value_ids.len(),
            list,
            refs,
        )?,
        add(wire_header_floor(n, port)?, backing?)?,
    ))
}
fn prepare_encode(
    n: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    e: &EncodedExpressions<'_, '_, '_>,
    ids: TableWriteTypeIds<'_>,
    projection: (usize, TableWriteNodeProjectionLimits),
    w: &mut CompileCheckpoints<'_>,
) -> Result<TableWriteNodeProjectionFacts, Error> {
    prepare_encode_core(n, values, e, ids, projection, None, w)
}
fn prepare_encode_core(
    n: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    e: &EncodedExpressions<'_, '_, '_>,
    ids: TableWriteTypeIds<'_>,
    projection: (usize, TableWriteNodeProjectionLimits),
    mut admit: Option<&mut NodeAdmit<'_>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<TableWriteNodeProjectionFacts, Error> {
    let (source, l) = projection;
    if admit.is_some()
        && (!std::ptr::addr_eq(w.control(), values.original_control())
            || !std::ptr::eq(values.types(), e.types())
            || !std::ptr::addr_eq(values.original_control(), e.original_control()))
    {
        return Err(invalid(
            "writer namespaces have different type or control loans",
        ));
    }
    let early = if let Some(callback) = admit.as_mut() {
        let b = physical(n)?;
        let (mut m, known) = encode_model(n, &b)?;
        m.delegated_work = mul(mul(2, calls(&b).len())?, e.aggregates().source_counts())?;
        if matches!(b, Body::Writer(_)) {
            m.delegated_work = add(m.delegated_work, mul(2, values.payloads().source_count())?)?;
        }
        encode_header_requests(n, &mut m)?;
        m.request::<wire::WriterAggregateCall>(calls(&b).len(), 1)?;
        match b {
            Body::Writer(t) => m.request::<u32>(t.input.len(), 1)?,
            Body::Finish(t) => {
                m.request::<u32>(t.expected_target_ordinals.len(), 1)?;
                finish_encode_requests(t, &mut m)?;
            }
        }
        m.admit_in(source, values.count(), l.node, &mut **callback)?;
        Some((m, known))
    } else {
        None
    };
    completed(
        if std::ptr::eq(values.types(), e.types())
            && if admit.is_some() {
                std::ptr::addr_eq(values.original_control(), e.original_control())
            } else {
                std::ptr::eq(values.original_control(), e.original_control())
            }
        {
            Ok(())
        } else {
            Err(invalid(
                "writer namespaces have different type or control loans",
            ))
        },
        w,
    )?;
    let b = completed(physical(n), w)?;
    let (mut m, known) = match early {
        Some(counted) => counted,
        None => encode_model(n, &b)?,
    };
    m.delegated_work = mul(mul(2, calls(&b).len())?, e.aggregates().source_counts())?;
    if matches!(b, Body::Writer(_)) {
        m.delegated_work = add(m.delegated_work, mul(2, values.payloads().source_count())?)?;
    }
    match admit.as_mut() {
        Some(callback) => m.admit_in(source, values.count(), l.node, &mut **callback)?,
        None => m.numerical_facts(source, values.count(), l.node)?,
    };
    count_prefix(m.inputs, m.items, source, known, l.node, w)?;
    floor(source, values.retained_floor(w)?, w)?;
    floor(source, e.retained_floor_observed(w)?, w)?;
    if admit.is_none() {
        encode_header_requests(n, &mut m)?;
        m.request::<wire::WriterAggregateCall>(calls(&b).len(), 1)?;
        if let Body::Writer(t) = &b {
            m.request::<u32>(t.input.len(), 1)?;
        } else if let Body::Finish(t) = &b {
            m.request::<u32>(t.expected_target_ordinals.len(), 1)?;
            finish_encode_requests(t, &mut m)?;
        }
    }
    match admit.as_mut() {
        Some(callback) => m.admit_in(source, values.count(), l.node, &mut **callback)?,
        None => m.numerical_facts(source, values.count(), l.node)?,
    };
    for prop in n
        .required_inputs
        .iter()
        .chain(std::iter::once(&n.output_properties))
    {
        let f = if admit.is_some() {
            properties::properties_encode_numerical_facts_in(prop, source)?
        } else {
            properties::properties_encode_numerical_facts(prop, source)?
        };
        distribution_limits(f, l.node.properties)?;
        m.property(f)?;
        match admit.as_mut() {
            Some(callback) => m.admit_in(source, values.count(), l.node, &mut **callback)?,
            None => m.numerical_facts(source, values.count(), l.node)?,
        };
        properties::preflight_encode_observed(prop, source, l.node.properties, w)?;
        match admit.as_mut() {
            Some(callback) => m.admit_in(source, values.count(), l.node, &mut **callback)?,
            None => m.numerical_facts(source, values.count(), l.node)?,
        };
        w.step()?;
    }
    match (&b, ids) {
        (
            Body::Writer(t),
            TableWriteTypeIds::Writer {
                target_fields,
                output_schema,
            },
        ) => {
            let f = if admit.is_some() {
                properties::distribution_encode_numerical_facts_in(
                    &t.required_distribution,
                    source,
                )?
            } else {
                properties::distribution_encode_numerical_facts(&t.required_distribution, source)?
            };
            distribution_limits(f, l.node.properties)?;
            // Model::property deliberately retains its conservative two-pass
            // bound even though distribution emission now uses the sole body.
            m.property(f)?;
            match admit.as_mut() {
                Some(callback) => m.admit_in(source, values.count(), l.node, &mut **callback)?,
                None => m.numerical_facts(source, values.count(), l.node)?,
            };
            properties::preflight_distribution_encode_observed(
                &t.required_distribution,
                source,
                l.node.properties,
                w,
            )?;
            let f = if let Some(callback) = admit.as_mut() {
                schema::preflight_targets_encode_in(
                    &t.target_fields,
                    target_fields,
                    e.types(),
                    parent(m, values.count(), l, source),
                    &mut **callback,
                    w,
                )
            } else {
                schema::preflight_targets_encode_observed(
                    &t.target_fields,
                    target_fields,
                    e.types(),
                    parent(m, values.count(), l, source),
                    w,
                )
            }?;
            merge_schema(&mut m, f, values.count(), l)?;
            let f = if let Some(callback) = admit.as_mut() {
                schema::preflight_schema_encode_in(
                    &t.output_schema,
                    output_schema,
                    e.types(),
                    parent(m, values.count(), l, source),
                    &mut **callback,
                    w,
                )
            } else {
                schema::preflight_schema_encode_observed(
                    &t.output_schema,
                    output_schema,
                    e.types(),
                    parent(m, values.count(), l, source),
                    w,
                )
            }?;
            merge_schema(&mut m, f, values.count(), l)?;
        }
        (
            Body::Finish(t),
            TableWriteTypeIds::Finish {
                input_schema,
                output_schema,
            },
        ) => {
            let f = if let Some(callback) = admit.as_mut() {
                schema::preflight_schema_encode_in(
                    &t.input_schema,
                    input_schema,
                    e.types(),
                    parent(m, values.count(), l, source),
                    &mut **callback,
                    w,
                )
            } else {
                schema::preflight_schema_encode_observed(
                    &t.input_schema,
                    input_schema,
                    e.types(),
                    parent(m, values.count(), l, source),
                    w,
                )
            }?;
            merge_schema(&mut m, f, values.count(), l)?;
            let f = if let Some(callback) = admit.as_mut() {
                schema::preflight_schema_encode_in(
                    &t.output_schema,
                    output_schema,
                    e.types(),
                    parent(m, values.count(), l, source),
                    &mut **callback,
                    w,
                )
            } else {
                schema::preflight_schema_encode_observed(
                    &t.output_schema,
                    output_schema,
                    e.types(),
                    parent(m, values.count(), l, source),
                    w,
                )
            }?;
            merge_schema(&mut m, f, values.count(), l)?;
            if let Some(g) = &t.grouped_unpivot {
                let p = GroupedNodeAdmission {
                    base: m,
                    values: values.count(),
                    limits: l.node,
                };
                let f = if let Some(callback) = admit.as_mut() {
                    grouped::preflight_grouped_encode_in(
                        g,
                        values,
                        e,
                        GroupedProjection {
                            source,
                            limits: l.node,
                            parent: Some(p),
                        },
                        &mut **callback,
                        w,
                    )
                } else {
                    grouped::preflight_grouped_encode_observed(
                        g,
                        values,
                        e,
                        GroupedProjection {
                            source,
                            limits: l.node,
                            parent: Some(p),
                        },
                        w,
                    )
                }?;
                m = p.merge(f)?;
            }
        }
        _ => {
            return completed(
                Err(invalid("writer type ID view has a different node kind")),
                w,
            );
        }
    }
    let facts = match admit.as_mut() {
        Some(callback) => m.facts_in(source, values.count(), l.node, &mut **callback, w)?,
        None => m.facts(source, values.count(), l.node, w)?,
    };
    match &b {
        Body::Writer(t) => {
            values.payloads().source_id_observed(&t.handle, w)?;
            ordinal(t.write_target_ordinal.get(), w)?;
            for id in &t.input {
                reference(id.get(), values, w)?;
                w.step()?;
            }
            for f in &t.target_fields {
                reference(f.input.get(), values, w)?;
                w.step()?;
            }
            for f in &t.output_schema.fields {
                reference(f.value.get(), values, w)?;
                w.step()?;
            }
            physical_distribution_refs(&t.required_distribution, values, w)?;
        }
        Body::Finish(t) => {
            for (_, columns) in t.summaries.iter() {
                for id in columns.values() {
                    reference(id.get(), values, w)?;
                    w.step()?;
                }
            }
            for id in &t.expected_target_ordinals {
                ordinal(id.get(), w)?;
                w.step()?;
            }
            for f in t
                .input_schema
                .fields
                .iter()
                .chain(t.output_schema.fields.iter())
            {
                reference(f.value.get(), values, w)?;
                w.step()?;
            }
        }
    }
    for c in calls(&b) {
        e.aggregates().source_id_observed(&c.binding, w)?;
        reference(c.input.get(), values, w)?;
        reference(c.output.get(), values, w)?;
        w.step()?;
    }
    for id in &n.output.columns {
        reference(id.get(), values, w)?;
        w.step()?;
    }
    for prop in n
        .required_inputs
        .iter()
        .chain(std::iter::once(&n.output_properties))
    {
        physical_property_refs(prop, values, w)?;
    }
    Ok(facts)
}
fn prepare_decode(
    n: &wire::PhysicalNode,
    e: &DecodedExpressions<'_, '_, '_>,
    a: &MaterializedAggregateBindings<'_, '_, '_>,
    source: usize,
    l: TableWriteNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<TableWriteNodeProjectionFacts, Error> {
    prepare_decode_core(n, e, a, source, l, None, w)
}
fn prepare_decode_core(
    n: &wire::PhysicalNode,
    e: &DecodedExpressions<'_, '_, '_>,
    a: &MaterializedAggregateBindings<'_, '_, '_>,
    source: usize,
    l: TableWriteNodeProjectionLimits,
    mut admit: Option<&mut NodeAdmit<'_>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<TableWriteNodeProjectionFacts, Error> {
    if admit.is_some()
        && (!std::ptr::addr_eq(w.control(), e.original_control())
            || !std::ptr::eq(a.headers(), e.aggregates())
            || !std::ptr::eq(a.functions().headers(), e.functions())
            || !std::ptr::addr_eq(a.original_control(), e.original_control()))
    {
        return Err(invalid(
            "writer owned namespaces have different original loans",
        ));
    }
    let early = if let Some(callback) = admit.as_mut() {
        let b = raw(n)?;
        let port = n
            .output
            .as_ref()
            .ok_or_else(|| invalid("writer output port is absent"))?;
        let (mut m, known) = decode_model(n, &b, port, true, w)?;
        m.delegated_work = mul(mul(3, raw_calls(&b).len())?, add(a.definitions().len(), 2)?)?;
        if matches!(b, Raw::Writer(_)) {
            m.delegated_work = add(
                m.delegated_work,
                mul(2, add(e.values().payloads().source_count(), 2)?)?,
            )?;
        }
        decode_header_requests(n, port, &mut m)?;
        m.request::<p::WriterAggregateCall>(raw_calls(&b).len(), 2)?;
        match b {
            Raw::Writer(t) => m.request::<p::ValueId>(t.input_value_ids.len(), 2)?,
            Raw::Finish(t) => {
                m.request::<WriteTargetOrdinal>(t.expected_target_ordinals.len(), 2)?
            }
        }
        m.admit_in(source, e.values().count(), l.node, &mut **callback)?;
        Some((m, known))
    } else {
        None
    };
    completed(
        if std::ptr::eq(a.headers(), e.aggregates())
            && std::ptr::eq(a.functions().headers(), e.functions())
            && if admit.is_some() {
                std::ptr::addr_eq(a.original_control(), e.original_control())
            } else {
                std::ptr::eq(a.original_control(), e.original_control())
            }
        {
            Ok(())
        } else {
            Err(invalid(
                "writer owned namespaces have different original loans",
            ))
        },
        w,
    )?;
    let b = completed(raw(n), w)?;
    let port = completed(
        n.output
            .as_ref()
            .ok_or_else(|| invalid("writer output port is absent")),
        w,
    )?;
    required(port.node_id, w)?;
    let props = completed(
        n.output_properties
            .as_ref()
            .ok_or_else(|| invalid("writer output properties are absent")),
        w,
    )?;
    let (mut m, known) = match early {
        Some(counted) => counted,
        None => decode_model(n, &b, port, false, w)?,
    };
    m.delegated_work = mul(mul(3, raw_calls(&b).len())?, add(a.definitions().len(), 2)?)?;
    if matches!(b, Raw::Writer(_)) {
        m.delegated_work = add(
            m.delegated_work,
            mul(2, add(e.values().payloads().source_count(), 2)?)?,
        )?;
    }
    match admit.as_mut() {
        Some(callback) => m.admit_in(source, e.values().count(), l.node, &mut **callback)?,
        None => m.numerical_facts(source, e.values().count(), l.node)?,
    };
    count_prefix(m.inputs, m.items, source, known, l.node, w)?;
    let dependency = add(
        e.retained_floor_observed(w)?,
        add(
            a.functions().retained_output_floor()?,
            a.retained_output_floor()?,
        )?,
    )?;
    floor(source, dependency, w)?;
    if admit.is_none() {
        decode_header_requests(n, port, &mut m)?;
        m.request::<p::WriterAggregateCall>(raw_calls(&b).len(), 2)?;
    }
    match &b {
        Raw::Writer(t) => {
            if admit.is_none() {
                m.request::<p::ValueId>(t.input_value_ids.len(), 2)?;
            }
            match admit.as_mut() {
                Some(callback) => {
                    m.admit_in(source, e.values().count(), l.node, &mut **callback)?
                }
                None => m.numerical_facts(source, e.values().count(), l.node)?,
            };
            let id = required(t.handle_payload_id, w)?;
            let payload = if let Some(callback) = admit.as_mut() {
                e.values().payloads().payload_captured_in(
                    id,
                    &mut |payload, _| {
                        if !payload.payload().is_empty() {
                            m.requests = add(m.requests, 1)?;
                            m.requested = add(m.requested, bytes_shared_upper_for_mode(true)?)?;
                        }
                        m.admit_in(source, e.values().count(), l.node, &mut **callback)?;
                        Ok::<(), Error>(())
                    },
                    w,
                )?
            } else {
                e.values().payloads().payload_observed(id, w)?
            };
            let payload = completed(
                payload.ok_or_else(|| invalid("writer payload reference is unknown")),
                w,
            )?;
            if admit.is_none() && !payload.payload().is_empty() {
                m.requests = add(m.requests, 1)?;
                m.requested = add(m.requested, bytes_shared_upper()?)?;
            }
            match admit.as_mut() {
                Some(callback) => {
                    m.admit_in(source, e.values().count(), l.node, &mut **callback)?
                }
                None => m.numerical_facts(source, e.values().count(), l.node)?,
            };
        }
        Raw::Finish(t) => {
            if admit.is_none() {
                m.request::<WriteTargetOrdinal>(t.expected_target_ordinals.len(), 2)?;
            }
            match admit.as_mut() {
                Some(callback) => {
                    m.admit_in(source, e.values().count(), l.node, &mut **callback)?
                }
                None => m.numerical_facts(source, e.values().count(), l.node)?,
            };
        }
    }
    for prop in n.required_inputs.iter().chain(std::iter::once(props)) {
        let f = if admit.is_some() {
            properties::properties_decode_numerical_facts_in(prop, source)?
        } else {
            properties::properties_decode_numerical_facts(prop, source)?
        };
        distribution_limits(f, l.node.properties)?;
        m.property(f)?;
        match admit.as_mut() {
            Some(callback) => m.admit_in(source, e.values().count(), l.node, &mut **callback)?,
            None => m.numerical_facts(source, e.values().count(), l.node)?,
        };
        properties::preflight_decode_observed(prop, source, l.node.properties, w)?;
        properties::validate_properties_source_observed(prop, w)?;
        match admit.as_mut() {
            Some(callback) => m.admit_in(source, e.values().count(), l.node, &mut **callback)?,
            None => m.numerical_facts(source, e.values().count(), l.node)?,
        };
        w.step()?;
    }
    match &b {
        Raw::Writer(t) => {
            let d = completed(
                t.required_distribution
                    .as_ref()
                    .ok_or_else(|| invalid("writer required distribution is absent")),
                w,
            )?;
            let f = if admit.is_some() {
                properties::distribution_decode_numerical_facts_in(d, source)?
            } else {
                properties::distribution_decode_numerical_facts(d, source)?
            };
            distribution_limits(f, l.node.properties)?;
            m.property(f)?;
            match admit.as_mut() {
                Some(callback) => {
                    m.admit_in(source, e.values().count(), l.node, &mut **callback)?
                }
                None => m.numerical_facts(source, e.values().count(), l.node)?,
            };
            properties::preflight_distribution_decode_observed(d, source, l.node.properties, w)?;
            properties::validate_distribution_source_observed(d, w)?;
            let f = if let Some(callback) = admit.as_mut() {
                schema::preflight_targets_decode_in(
                    &t.target_fields,
                    e.types(),
                    parent(m, e.values().count(), l, source),
                    &mut **callback,
                    w,
                )
            } else {
                schema::preflight_targets_decode_observed(
                    &t.target_fields,
                    e.types(),
                    parent(m, e.values().count(), l, source),
                    w,
                )
            }?;
            merge_schema(&mut m, f, e.values().count(), l)?;
            let f = if let Some(callback) = admit.as_mut() {
                schema::preflight_schema_decode_in(
                    raw_schema(t.output_schema.as_ref(), w)?,
                    e.types(),
                    parent(m, e.values().count(), l, source),
                    &mut **callback,
                    w,
                )
            } else {
                schema::preflight_schema_decode_observed(
                    raw_schema(t.output_schema.as_ref(), w)?,
                    e.types(),
                    parent(m, e.values().count(), l, source),
                    w,
                )
            }?;
            merge_schema(&mut m, f, e.values().count(), l)?;
        }
        Raw::Finish(t) => {
            let f = if let Some(callback) = admit.as_mut() {
                schema::preflight_schema_decode_in(
                    raw_schema(t.input_schema.as_ref(), w)?,
                    e.types(),
                    parent(m, e.values().count(), l, source),
                    &mut **callback,
                    w,
                )
            } else {
                schema::preflight_schema_decode_observed(
                    raw_schema(t.input_schema.as_ref(), w)?,
                    e.types(),
                    parent(m, e.values().count(), l, source),
                    w,
                )
            }?;
            merge_schema(&mut m, f, e.values().count(), l)?;
            let f = if let Some(callback) = admit.as_mut() {
                schema::preflight_schema_decode_in(
                    raw_schema(t.output_schema.as_ref(), w)?,
                    e.types(),
                    parent(m, e.values().count(), l, source),
                    &mut **callback,
                    w,
                )
            } else {
                schema::preflight_schema_decode_observed(
                    raw_schema(t.output_schema.as_ref(), w)?,
                    e.types(),
                    parent(m, e.values().count(), l, source),
                    w,
                )
            }?;
            merge_schema(&mut m, f, e.values().count(), l)?;
            if let Some(g) = &t.grouped_unpivot {
                let p = GroupedNodeAdmission {
                    base: m,
                    values: e.values().count(),
                    limits: l.node,
                };
                let f = if let Some(callback) = admit.as_mut() {
                    grouped::preflight_grouped_decode_in(
                        g,
                        e,
                        GroupedProjection {
                            source,
                            limits: l.node,
                            parent: Some(p),
                        },
                        &mut **callback,
                        w,
                    )
                } else {
                    grouped::preflight_grouped_decode_observed(
                        g,
                        e,
                        GroupedProjection {
                            source,
                            limits: l.node,
                            parent: Some(p),
                        },
                        w,
                    )
                }?;
                m = p.merge(f)?;
            }
        }
    }
    let facts = if raw_calls(&b).is_empty() {
        match admit.as_mut() {
            Some(callback) => m.facts_in(source, e.values().count(), l.node, &mut **callback, w)?,
            None => m.facts(source, e.values().count(), l.node, w)?,
        }
    } else {
        let observed_child = admit.is_some();
        let mut child = MaterializationModel::for_composition(
            raw_calls(&b).len(),
            add(e.types().value_types().len(), 1)?,
            source,
            dependency,
        );
        if let Some(callback) = admit.as_mut() {
            child.compose_in_node_in(m, e.values().count(), l.node, l.binding, &mut **callback)?;
        } else {
            child.compose_in_node(m, e.values().count(), l.node, l.binding)?;
        }
        for c in raw_calls(&b) {
            if observed_child {
                a.definition_captured(
                    required(c.aggregate_binding_id, w)?,
                    &mut |binding, work| {
                        preflight_aggregate_binding_copy_counts_in(
                            binding,
                            &mut child,
                            l.binding,
                            &mut |_| Ok(()),
                            work,
                        )
                    },
                    w,
                )?
                .ok_or_else(|| invalid("writer aggregate binding is not in original namespace"))?;
            } else {
                preflight_aggregate_binding_copy_counts(
                    binding(required(c.aggregate_binding_id, w)?, a, w)?,
                    &mut child,
                    l.binding,
                    w,
                )?;
            }
            w.step()?;
        }
        child.node_facts(0, w)?;
        for c in raw_calls(&b) {
            if observed_child {
                a.definition_captured(
                    required(c.aggregate_binding_id, w)?,
                    &mut |binding, work| {
                        preflight_aggregate_binding_copy_types_in(
                            binding,
                            &mut child,
                            l.binding,
                            &mut |_| Ok(()),
                            work,
                        )
                    },
                    w,
                )?
                .ok_or_else(|| invalid("writer aggregate binding is not in original namespace"))?;
            } else {
                preflight_aggregate_binding_copy_types(
                    binding(required(c.aggregate_binding_id, w)?, a, w)?,
                    &mut child,
                    l.binding,
                    w,
                )?;
            }
            w.step()?;
        }
        child.node_facts(mul(2, child.facts.cumulative_work_upper_bound)?, w)?
    };
    match &b {
        Raw::Writer(t) => {
            ordinal(t.write_target_ordinal, w)?;
            for id in &t.input_value_ids {
                reference(*id, e.values(), w)?;
                w.step()?;
            }
            for f in &t.target_fields {
                reference(required(f.input_value_id, w)?, e.values(), w)?;
                w.step()?;
            }
            for f in &raw_schema(t.output_schema.as_ref(), w)?.fields {
                reference(required(f.value_id, w)?, e.values(), w)?;
                w.step()?;
            }
            wire_distribution_refs(t.required_distribution.as_ref().unwrap(), e, w)?;
        }
        Raw::Finish(t) => {
            for (_, columns) in completed(wire_finish_columns(t), w)?.1.iter() {
                for id in columns.values() {
                    reference(id.get(), e.values(), w)?;
                    w.step()?;
                }
            }
            for id in &t.expected_target_ordinals {
                ordinal(*id, w)?;
                w.step()?;
            }
            for f in raw_schema(t.input_schema.as_ref(), w)?
                .fields
                .iter()
                .chain(raw_schema(t.output_schema.as_ref(), w)?.fields.iter())
            {
                reference(required(f.value_id, w)?, e.values(), w)?;
                w.step()?;
            }
        }
    }
    for c in raw_calls(&b) {
        reference(required(c.input_value_id, w)?, e.values(), w)?;
        reference(required(c.output_value_id, w)?, e.values(), w)?;
        w.step()?;
    }
    for id in &port.value_ids {
        reference(*id, e.values(), w)?;
        w.step()?;
    }
    for prop in n.required_inputs.iter().chain(std::iter::once(props)) {
        wire_property_refs(prop, e.values(), w)?;
    }
    Ok(facts)
}
fn encode_calls(
    input: &[p::WriterAggregateCall],
    e: &EncodedExpressions<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<Vec<wire::WriterAggregateCall>, Error> {
    let mut v = reserve(input.len(), w)?;
    for c in input {
        v.push(wire::WriterAggregateCall {
            input_value_id: Some(c.input.get()),
            aggregate_binding_id: Some(e.aggregates().source_id_observed(&c.binding, w)?),
            output_value_id: Some(c.output.get()),
        });
        w.step()?;
    }
    Ok(v)
}
fn decode_calls(
    input: &[wire::WriterAggregateCall],
    a: &MaterializedAggregateBindings<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<Box<[p::WriterAggregateCall]>, Error> {
    let mut v = reserve(input.len(), w)?;
    for c in input {
        v.push(p::WriterAggregateCall {
            input: p::ValueId::new(required(c.input_value_id, w)?),
            binding: copy_aggregate_binding_observed(
                binding(required(c.aggregate_binding_id, w)?, a, w)?,
                w,
            )?,
            output: p::ValueId::new(required(c.output_value_id, w)?),
        });
        w.step()?;
    }
    boxed(v, w)
}
fn encode_ids(input: &[p::ValueId], w: &mut CompileCheckpoints<'_>) -> Result<Vec<u32>, Error> {
    let mut v = reserve(input.len(), w)?;
    for id in input {
        v.push(id.get());
        w.step()?;
    }
    Ok(v)
}
fn emit_encode(
    n: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    e: &EncodedExpressions<'_, '_, '_>,
    ids: TableWriteTypeIds<'_>,
    projection: (usize, TableWriteNodeProjectionLimits),
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::PhysicalNode, Error> {
    let (source, l) = projection;
    let (inputs, required_inputs, output_properties, output) = encode_header(n, source, l.node, w)?;
    let kind = match (physical(n)?, ids) {
        (
            Body::Writer(t),
            TableWriteTypeIds::Writer {
                target_fields,
                output_schema,
            },
        ) => wire::physical_node::Kind::TableWriter(wire::WriterTarget {
            handle_payload_id: Some(values.payloads().source_id_observed(&t.handle, w)?),
            write_target_ordinal: t.write_target_ordinal.get(),
            input_value_ids: encode_ids(&t.input, w)?,
            required_distribution: Some(properties::emit_distribution_observed(
                &t.required_distribution,
                w,
            )?),
            target_fields: schema::emit_targets(&t.target_fields, target_fields, w)?,
            output_schema: Some(schema::emit_schema(&t.output_schema, output_schema, w)?),
            partial_aggregates: encode_calls(&t.partial_aggregates, e, w)?,
        }),
        (
            Body::Finish(t),
            TableWriteTypeIds::Finish {
                input_schema,
                output_schema,
            },
        ) => {
            let mut ordinals = reserve(t.expected_target_ordinals.len(), w)?;
            for id in &t.expected_target_ordinals {
                ordinals.push(id.get());
                w.step()?;
            }
            wire::physical_node::Kind::TableFinish(wire::WriterFinish {
                count_mode: match t.count_mode {
                    p::FinishCountMode::LogicalChanges => {
                        wire::FinishCountMode::LogicalChanges as i32
                    }
                    p::FinishCountMode::WriterReported => {
                        wire::FinishCountMode::WriterReported as i32
                    }
                    p::FinishCountMode::ExternalOwner => {
                        wire::FinishCountMode::ExternalOwner as i32
                    }
                },
                writer_results_input: Some(t.inputs.writer_results),
                summaries: encode_finish_summaries(t, w)?,
                expected_target_ordinals: ordinals,
                input_schema: Some(schema::emit_schema(&t.input_schema, input_schema, w)?),
                output_schema: Some(schema::emit_schema(&t.output_schema, output_schema, w)?),
                final_aggregates: encode_calls(&t.final_aggregates, e, w)?,
                grouped_unpivot: t
                    .grouped_unpivot
                    .as_ref()
                    .map(|g| grouped::emit_encode(g, w))
                    .transpose()?,
            })
        }
        _ => return Err(invalid("writer type ID view has a different node kind")),
    };
    let out = wire::PhysicalNode {
        id: n.id.get(),
        input_node_ids: inputs,
        required_inputs,
        output_properties: Some(output_properties),
        output: Some(output),
        kind: Some(kind),
    };
    w.step()?;
    Ok(out)
}
fn emit_decode(
    n: &wire::PhysicalNode,
    e: &DecodedExpressions<'_, '_, '_>,
    a: &MaterializedAggregateBindings<'_, '_, '_>,
    source: usize,
    l: TableWriteNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::PhysicalNode, Error> {
    let (inputs, required_inputs, output_properties, output) = decode_header(n, source, l.node, w)?;
    let kind = match raw(n)? {
        Raw::Writer(t) => {
            let payload = e
                .values()
                .payloads()
                .payload_observed(required(t.handle_payload_id, w)?, w)?;
            let payload = completed(
                payload.ok_or_else(|| invalid("prepared writer payload reference is unknown")),
                w,
            )?;
            w.flush()?;
            let handle = payload.clone();
            w.step()?;
            w.flush()?;
            let mut input = reserve(t.input_value_ids.len(), w)?;
            for id in &t.input_value_ids {
                input.push(p::ValueId::new(*id));
                w.step()?;
            }
            p::NodeKind::TableWriter {
                target: p::WriterTarget {
                    handle,
                    write_target_ordinal: ordinal(t.write_target_ordinal, w)?,
                    input: boxed(input, w)?,
                    required_distribution: properties::materialize_distribution_source_observed(
                        t.required_distribution.as_ref().unwrap(),
                        w,
                    )?,
                    target_fields: schema::read_targets(&t.target_fields, e.types(), w)?,
                    output_schema: schema::read_schema(
                        t.output_schema.as_ref().unwrap(),
                        e.types(),
                        w,
                    )?,
                    partial_aggregates: decode_calls(&t.partial_aggregates, a, w)?,
                },
            }
        }
        Raw::Finish(t) => {
            let mut ordinals = reserve(t.expected_target_ordinals.len(), w)?;
            for id in &t.expected_target_ordinals {
                ordinals.push(ordinal(*id, w)?);
                w.step()?;
            }
            p::NodeKind::TableFinish(p::WriterFinishSpec {
                count_mode: completed(finish_mode(t.count_mode), w)?,
                inputs: p::FinishInputs {
                    writer_results: required(t.writer_results_input, w)?,
                    summaries: completed(wire_finish_columns(t), w)?.0,
                },
                summaries: completed(wire_finish_columns(t), w)?.1,
                expected_target_ordinals: boxed(ordinals, w)?,
                input_schema: schema::read_schema(t.input_schema.as_ref().unwrap(), e.types(), w)?,
                output_schema: schema::read_schema(
                    t.output_schema.as_ref().unwrap(),
                    e.types(),
                    w,
                )?,
                final_aggregates: decode_calls(&t.final_aggregates, a, w)?,
                grouped_unpivot: t
                    .grouped_unpivot
                    .as_ref()
                    .map(|g| grouped::emit_decode(g, w))
                    .transpose()?,
            })
        }
    };
    let out = p::PhysicalNode {
        id: p::NodeId::new(n.id),
        inputs,
        required_inputs,
        output_properties,
        output,
        kind,
    };
    w.step()?;
    Ok(out)
}

pub struct PreparedTableWriteNodeEncode<'node, 'namespace, 'loan, 'source, 'control> {
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    type_ids: TableWriteTypeIds<'node>,
    source: usize,
    limits: TableWriteNodeProjectionLimits,
    facts: TableWriteNodeProjectionFacts,
}
impl PreparedTableWriteNodeEncode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &TableWriteNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(wire::PhysicalNode, TableWriteNodeProjectionFacts), Error> {
        let mut w =
            CompileCheckpoints::try_new(self.values.original_control(), CompilePhase::Encode)?;
        let r = emit_encode(
            self.input,
            self.values,
            self.expressions,
            self.type_ids,
            (self.source, self.limits),
            &mut w,
        )
        .map(|n| (n, self.facts));
        finish(w, r)
    }
}
pub fn prepare_table_write_node_encode<'node, 'namespace, 'loan, 'source, 'control>(
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    type_ids: TableWriteTypeIds<'node>,
    source_retained_bytes: usize,
    limits: TableWriteNodeProjectionLimits,
) -> Result<PreparedTableWriteNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
    let mut w = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let r = prepare_encode(
        input,
        values,
        expressions,
        type_ids,
        (source_retained_bytes, limits),
        &mut w,
    );
    let facts = finish(w, r)?;
    Ok(PreparedTableWriteNodeEncode {
        input,
        values,
        expressions,
        type_ids,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
pub struct PreparedTableWriteNodeDecode<'node, 'namespace, 'loan, 'wire, 'control> {
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    aggregates: &'namespace MaterializedAggregateBindings<'namespace, 'loan, 'wire>,
    source: usize,
    limits: TableWriteNodeProjectionLimits,
    facts: TableWriteNodeProjectionFacts,
}
impl PreparedTableWriteNodeDecode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &TableWriteNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(p::PhysicalNode, TableWriteNodeProjectionFacts), Error> {
        let mut w =
            CompileCheckpoints::try_new(self.expressions.original_control(), CompilePhase::Decode)?;
        let r = emit_decode(
            self.input,
            self.expressions,
            self.aggregates,
            self.source,
            self.limits,
            &mut w,
        )
        .map(|n| (n, self.facts));
        finish(w, r)
    }
}
pub fn prepare_table_write_node_decode<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    aggregates: &'namespace MaterializedAggregateBindings<'namespace, 'loan, 'wire>,
    source_retained_bytes: usize,
    limits: TableWriteNodeProjectionLimits,
) -> Result<PreparedTableWriteNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
    let mut w = CompileCheckpoints::try_new(expressions.original_control(), CompilePhase::Decode)?;
    let r = prepare_decode(
        input,
        expressions,
        aggregates,
        source_retained_bytes,
        limits,
        &mut w,
    );
    let facts = finish(w, r)?;
    Ok(PreparedTableWriteNodeDecode {
        input,
        expressions,
        aggregates,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
pub fn encode_table_write_node(
    input: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    type_ids: TableWriteTypeIds<'_>,
    source_retained_bytes: usize,
    limits: TableWriteNodeProjectionLimits,
) -> Result<(wire::PhysicalNode, TableWriteNodeProjectionFacts), Error> {
    let mut w = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let r = (|| {
        let facts = prepare_encode(
            input,
            values,
            expressions,
            type_ids,
            (source_retained_bytes, limits),
            &mut w,
        )?;
        Ok((
            emit_encode(
                input,
                values,
                expressions,
                type_ids,
                (source_retained_bytes, limits),
                &mut w,
            )?,
            facts,
        ))
    })();
    finish(w, r)
}
pub fn decode_table_write_node(
    input: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    aggregates: &MaterializedAggregateBindings<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: TableWriteNodeProjectionLimits,
) -> Result<(p::PhysicalNode, TableWriteNodeProjectionFacts), Error> {
    let mut w = CompileCheckpoints::try_new(expressions.original_control(), CompilePhase::Decode)?;
    let r = (|| {
        let facts = prepare_decode(
            input,
            expressions,
            aggregates,
            source_retained_bytes,
            limits,
            &mut w,
        )?;
        Ok((
            emit_decode(
                input,
                expressions,
                aggregates,
                source_retained_bytes,
                limits,
                &mut w,
            )?,
            facts,
        ))
    })();
    finish(w, r)
}

pub fn prepare_table_write_node_encode_in<'node, 'namespace, 'loan, 'source, 'control>(
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    type_ids: TableWriteTypeIds<'node>,
    source: usize,
    limits: TableWriteNodeProjectionLimits,
    admit: &mut NodeAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedTableWriteNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
    let facts = prepare_encode_core(
        input,
        values,
        expressions,
        type_ids,
        (source, limits),
        Some(admit),
        work,
    )?;
    Ok(PreparedTableWriteNodeEncode {
        input,
        values,
        expressions,
        type_ids,
        source,
        limits,
        facts,
    })
}
pub fn prepare_table_write_node_decode_in<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    aggregates: &'namespace MaterializedAggregateBindings<'namespace, 'loan, 'wire>,
    source: usize,
    limits: TableWriteNodeProjectionLimits,
    admit: &mut NodeAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedTableWriteNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
    let facts = prepare_decode_core(
        input,
        expressions,
        aggregates,
        source,
        limits,
        Some(admit),
        work,
    )?;
    Ok(PreparedTableWriteNodeDecode {
        input,
        expressions,
        aggregates,
        source,
        limits,
        facts,
    })
}
impl PreparedTableWriteNodeEncode<'_, '_, '_, '_, '_> {
    pub fn emit_in(
        self,
        admit: &mut NodeAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(wire::PhysicalNode, TableWriteNodeProjectionFacts), Error> {
        if !std::ptr::addr_eq(work.control(), self.values.original_control()) {
            return Err(invalid("writer caller has another original control"));
        }
        admit(&self.facts)?;
        Ok((
            emit_encode(
                self.input,
                self.values,
                self.expressions,
                self.type_ids,
                (self.source, self.limits),
                work,
            )?,
            self.facts,
        ))
    }
}
impl PreparedTableWriteNodeDecode<'_, '_, '_, '_, '_> {
    pub fn emit_in(
        self,
        admit: &mut NodeAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(p::PhysicalNode, TableWriteNodeProjectionFacts), Error> {
        if !std::ptr::addr_eq(work.control(), self.expressions.original_control()) {
            return Err(invalid("writer caller has another original control"));
        }
        admit(&self.facts)?;
        Ok((
            emit_decode(
                self.input,
                self.expressions,
                self.aggregates,
                self.source,
                self.limits,
                work,
            )?,
            self.facts,
        ))
    }
}
#[cfg(test)]
mod tests;
