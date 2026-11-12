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

//! Projection for simple node payloads, including position shard aggregation.
//! This component retains original namespace loans; Fragment owns all kind
//! semantics, effect proofs, roles, widths and full-type relations.

pub use crate::physical_node_v2::{
    NodeCodecError as SimpleNodeCodecError, NodeProjectionFacts as SimpleNodeProjectionFacts,
    NodeProjectionLimits as SimpleNodeProjectionLimits,
};
use crate::{
    physical_expression_v2::{DecodedExpressions, EncodedExpressions},
    physical_node_v2::*,
    physical_properties_v2 as properties,
    physical_value_v2::EncodedValues,
};
use novarocks_connector_contract::{
    ConnectorMutationContributionGroupId, ConnectorWriteRouteId, WriteTargetOrdinal,
};
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
type Error = SimpleNodeCodecError;

fn expression_encode(
    id: p::ExprId,
    expressions: &EncodedExpressions<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let found = expressions.expression_observed(id.get(), w)?.is_some();
    w.step()?;
    if found {
        Ok(())
    } else {
        Err(invalid(
            "simple node expression is absent from original emission",
        ))
    }
}
fn expression_decode(
    id: u32,
    expressions: &DecodedExpressions<'_, '_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let found = expressions.definition_observed(id, w)?.is_some();
    w.step()?;
    if found {
        Ok(())
    } else {
        Err(invalid(
            "simple node expression is absent from original receiving namespace",
        ))
    }
}
fn required(id: Option<u32>, w: &mut CompileCheckpoints<'_>) -> Result<u32, Error> {
    w.step()?;
    id.ok_or_else(|| invalid("simple node required payload reference is absent"))
}
/// O(1) outer shape facts, before the first variable-length source walk.
fn physical_outer(input: &p::PhysicalNode) -> Result<(usize, usize), Error> {
    let (items, backing) = match &input.kind {
        p::NodeKind::Filter { predicates } => {
            (predicates.len(), bytes::<p::ExprId>(predicates.len())?)
        }
        p::NodeKind::Project { expressions } => (
            expressions.len(),
            bytes::<(p::ExprId, p::ValueId)>(expressions.len())?,
        ),
        p::NodeKind::Values { rows } => (rows.len(), bytes::<Box<[p::ExprId]>>(rows.len())?),
        p::NodeKind::PositionShard(spec) => (
            spec.inputs.len(),
            bytes::<p::PositionShardInput>(spec.inputs.len())?,
        ),
        p::NodeKind::SystemRelation { .. } | p::NodeKind::Limit { .. } => (0, 0),
        p::NodeKind::GenerateSeries { step, .. } => (2 + usize::from(step.is_some()), 0),
        _ => return Err(invalid("physical node is outside the simple node family")),
    };
    Ok((items, backing))
}
fn wire_outer(input: &wire::PhysicalNode) -> Result<(usize, usize), Error> {
    let (items, backing) = match input.kind.as_ref() {
        Some(wire::physical_node::Kind::Filter(v)) => (
            v.predicate_expr_ids.len(),
            bytes::<u32>(v.predicate_expr_ids.capacity())?,
        ),
        Some(wire::physical_node::Kind::Project(v)) => (
            v.expressions.len(),
            bytes::<wire::ExpressionOutput>(v.expressions.capacity())?,
        ),
        Some(wire::physical_node::Kind::Values(v)) => (
            v.rows.len(),
            bytes::<wire::ExpressionIds>(v.rows.capacity())?,
        ),
        Some(wire::physical_node::Kind::PositionShard(v)) => (
            v.inputs.len(),
            add(
                bytes::<wire::PositionShardInput>(v.inputs.capacity())?,
                v.contribution_group_id.capacity(),
            )?,
        ),
        Some(wire::physical_node::Kind::SystemRelation(_))
        | Some(wire::physical_node::Kind::Limit(_)) => (0, 0),
        Some(wire::physical_node::Kind::GenerateSeries(v)) => {
            (2 + usize::from(v.step_expr_id.is_some()), 0)
        }
        _ => return Err(invalid("wire node is outside the simple node family")),
    };
    Ok((items, backing))
}
fn prepare_encode(
    input: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    source: usize,
    l: SimpleNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
    mut parent: Option<&mut NodeAdmit<'_>>,
) -> Result<SimpleNodeProjectionFacts, Error> {
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
                return Err(invalid("simple node source invoice omits original backing"));
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
            "simple node namespaces differ in type table or original control",
        ));
    }
    let (payload_items, backing) = physical_outer(input)?;
    let items = add(
        payload_items,
        add(input.required_inputs.len(), input.output.columns.len())?,
    )?;
    let mut known = add(physical_header_floor(input)?, backing)?;
    if parent.is_none() {
        count_prefix(input.inputs.len(), items, source, known, l, w)?;
        floor(source, values.retained_floor(w)?, w)?;
        floor(source, expressions.retained_floor_observed(w)?, w)?;
    }
    let mut model = Model {
        inputs: input.inputs.len(),
        items,
        refs: input.output.columns.len(),
        ..Model::default()
    };
    encode_header_requests(input, &mut model)?;
    let occurrences = match &input.kind {
        p::NodeKind::Filter { predicates } => {
            model.request::<u32>(predicates.len(), 1)?;
            predicates.len()
        }
        p::NodeKind::Project { expressions } => {
            model.request::<wire::ExpressionOutput>(expressions.len(), 1)?;
            model.refs = add(model.refs, expressions.len())?;
            expressions.len()
        }
        p::NodeKind::Values { rows } => {
            model.request::<wire::ExpressionIds>(rows.len(), 1)?;
            let mut total = 0;
            for row in rows {
                total = add(total, row.len())?;
                model.items = add(model.items, row.len())?;
                known = add(known, bytes::<p::ExprId>(row.len())?)?;
                model.request::<u32>(row.len(), 1)?;
                if parent.is_some() {
                    model.delegated_work = mul(total, expressions.lookup_work_upper_bound()?)?;
                }
                gate(&model, known, &mut parent)?;
                count_prefix(model.inputs, model.items, source, known, l, w)?;
                gate(&model, known, &mut parent)?;
                w.step()?;
            }
            total
        }
        p::NodeKind::GenerateSeries { step, .. } => 2 + usize::from(step.is_some()),
        p::NodeKind::PositionShard(spec) => {
            model.refs = add(model.refs, add(mul(spec.inputs.len(), 4)?, 9)?)?;
            model.request::<wire::PositionShardInput>(spec.inputs.len(), 1)?;
            model.request::<u8>(32, spec.inputs.len())?;
            model.request::<u8>(32, 1)?;
            0
        }
        p::NodeKind::SystemRelation { .. } | p::NodeKind::Limit { .. } => 0,
        _ => return Err(invalid("prepared physical kind is outside simple family")),
    };
    model.delegated_work = mul(occurrences, expressions.lookup_work_upper_bound()?)?;
    if parent.is_some() {
        gate(&model, known, &mut parent)?;
        count_prefix(input.inputs.len(), items, source, known, l, w)?;
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
            properties::check_properties_numerical_facts(pf, l.properties)?;
            model.property(pf)?;
            gate(&model, known, &mut parent)?;
            properties::preflight_encode_observed(property, source, l.properties, w)?;
        } else {
            model.property(properties::preflight_encode_observed(
                property,
                source,
                l.properties,
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
    for id in &input.output.columns {
        reference(id.get(), values, w)?;
    }
    match &input.kind {
        p::NodeKind::Filter { predicates } => {
            for id in predicates {
                expression_encode(*id, expressions, w)?;
            }
        }
        p::NodeKind::Project {
            expressions: outputs,
        } => {
            for (expr, output) in outputs {
                expression_encode(*expr, expressions, w)?;
                reference(output.get(), values, w)?;
            }
        }
        p::NodeKind::Values { rows } => {
            for row in rows {
                for expr in row {
                    expression_encode(*expr, expressions, w)?;
                }
                w.step()?;
            }
        }
        p::NodeKind::GenerateSeries { start, stop, step } => {
            expression_encode(*start, expressions, w)?;
            expression_encode(*stop, expressions, w)?;
            if let Some(step) = step {
                expression_encode(*step, expressions, w)?;
            }
        }
        p::NodeKind::PositionShard(spec) => {
            for input in &spec.inputs {
                for id in input.columns() {
                    reference(id.get(), values, w)?;
                }
                w.step()?;
            }
            for id in spec.output.columns() {
                reference(id.get(), values, w)?;
            }
        }
        p::NodeKind::SystemRelation { .. } | p::NodeKind::Limit { .. } => {
            w.step()?;
        }
        _ => unreachable!(),
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(&input.output_properties))
    {
        physical_property_refs(property, values, w)?;
    }
    Ok(facts)
}
fn prepare_decode(
    input: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    source: usize,
    l: SimpleNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
    mut parent: Option<&mut NodeAdmit<'_>>,
) -> Result<SimpleNodeProjectionFacts, Error> {
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
                return Err(invalid("simple node source invoice omits original backing"));
            }
        }
        Ok(())
    };

    let (payload_items, backing) = wire_outer(input)?;
    let port = input
        .output
        .as_ref()
        .ok_or_else(|| invalid("simple node output port is absent"))?;
    if parent.is_none() {
        required(port.node_id, w)?;
    } else {
        port.node_id
            .ok_or_else(|| invalid("node output ID is absent"))?;
    }
    let output_properties = input
        .output_properties
        .as_ref()
        .ok_or_else(|| invalid("simple node output properties are absent"))?;
    let items = add(
        payload_items,
        add(input.required_inputs.len(), port.value_ids.len())?,
    )?;
    let mut known = add(wire_header_floor(input, port)?, backing)?;
    if parent.is_none() {
        count_prefix(input.input_node_ids.len(), items, source, known, l, w)?;
        floor(source, values.retained_floor(w)?, w)?;
        floor(source, expressions.retained_floor_observed(w)?, w)?;
    }
    let mut model = Model {
        inputs: input.input_node_ids.len(),
        items,
        refs: port.value_ids.len(),
        ..Model::default()
    };
    decode_header_requests(input, port, &mut model)?;
    let occurrences = match input.kind.as_ref() {
        Some(wire::physical_node::Kind::Filter(v)) => {
            model.request::<p::ExprId>(v.predicate_expr_ids.len(), 2)?;
            v.predicate_expr_ids.len()
        }
        Some(wire::physical_node::Kind::Project(v)) => {
            model.request::<(p::ExprId, p::ValueId)>(v.expressions.len(), 2)?;
            model.refs = add(model.refs, v.expressions.len())?;
            v.expressions.len()
        }
        Some(wire::physical_node::Kind::Values(v)) => {
            model.request::<Box<[p::ExprId]>>(v.rows.len(), 2)?;
            let mut total = 0;
            for row in &v.rows {
                total = add(total, row.expr_ids.len())?;
                model.items = add(model.items, row.expr_ids.len())?;
                known = add(known, bytes::<u32>(row.expr_ids.capacity())?)?;
                model.request::<p::ExprId>(row.expr_ids.len(), 2)?;
                if parent.is_some() {
                    model.delegated_work = mul(total, expressions.lookup_work_upper_bound()?)?;
                }
                gate(&model, known, &mut parent)?;
                count_prefix(model.inputs, model.items, source, known, l, w)?;
                gate(&model, known, &mut parent)?;
                w.step()?;
            }
            total
        }
        Some(wire::physical_node::Kind::GenerateSeries(v)) => {
            2 + usize::from(v.step_expr_id.is_some())
        }
        Some(wire::physical_node::Kind::PositionShard(v)) => {
            model.refs = add(model.refs, add(mul(v.inputs.len(), 4)?, 9)?)?;
            model.request::<p::PositionShardInput>(v.inputs.len(), 2)?;
            // Account for the original retained wire IDs before decoding them.
            gate(&model, known, &mut parent)?;
            for route in &v.inputs {
                known = add(known, route.route_id.capacity())?;
                gate(&model, known, &mut parent)?;
                floor(source, known, w)?;
                w.step()?;
            }
            0
        }
        Some(wire::physical_node::Kind::SystemRelation(_))
        | Some(wire::physical_node::Kind::Limit(_)) => 0,
        _ => return Err(invalid("prepared wire kind is outside simple family")),
    };
    model.delegated_work = mul(occurrences, expressions.lookup_work_upper_bound()?)?;
    if parent.is_some() {
        gate(&model, known, &mut parent)?;
        count_prefix(input.input_node_ids.len(), items, source, known, l, w)?;
        floor(source, values.retained_floor(w)?, w)?;
        floor(source, expressions.retained_floor_observed(w)?, w)?;
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(output_properties))
    {
        if parent.is_some() {
            let pf = properties::properties_decode_numerical_facts_in(property, source)?;
            properties::check_properties_numerical_facts(pf, l.properties)?;
            model.property(pf)?;
            gate(&model, known, &mut parent)?;
            properties::preflight_decode_observed(property, source, l.properties, w)?;
        } else {
            model.property(properties::preflight_decode_observed(
                property,
                source,
                l.properties,
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
    match input.kind.as_ref() {
        Some(wire::physical_node::Kind::Filter(v)) => {
            for id in &v.predicate_expr_ids {
                expression_decode(*id, expressions, w)?;
            }
        }
        Some(wire::physical_node::Kind::Project(v)) => {
            for pair in &v.expressions {
                expression_decode(required(pair.expr_id, w)?, expressions, w)?;
                reference(required(pair.value_id, w)?, values, w)?;
            }
        }
        Some(wire::physical_node::Kind::Values(v)) => {
            for row in &v.rows {
                for expr in &row.expr_ids {
                    expression_decode(*expr, expressions, w)?;
                }
                w.step()?;
            }
        }
        Some(wire::physical_node::Kind::GenerateSeries(v)) => {
            expression_decode(required(v.start_expr_id, w)?, expressions, w)?;
            expression_decode(required(v.stop_expr_id, w)?, expressions, w)?;
            if let Some(step) = v.step_expr_id {
                expression_decode(step, expressions, w)?;
            }
        }
        Some(wire::physical_node::Kind::PositionShard(v)) => {
            shard_identity(&v.contribution_group_id, w)?;
            for input in &v.inputs {
                let input = decode_shard_input(input, w)?;
                for id in input.columns() {
                    reference(id.get(), values, w)?;
                }
            }
            for id in decode_shard_output(v.output.as_ref(), w)?.columns() {
                reference(id.get(), values, w)?;
            }
        }
        Some(wire::physical_node::Kind::SystemRelation(_))
        | Some(wire::physical_node::Kind::Limit(_)) => {
            w.step()?;
        }
        _ => unreachable!(),
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(output_properties))
    {
        wire_property_refs(property, values, w)?;
    }
    Ok(facts)
}
fn shard_identity(bytes: &[u8], w: &mut CompileCheckpoints<'_>) -> Result<[u8; 32], Error> {
    w.step()?;
    let id = bytes
        .try_into()
        .map_err(|_| invalid("position shard identity must contain 32 bytes"))?;
    if id == [0; 32] {
        return Err(invalid("position shard identity must be nonzero"));
    }
    Ok(id)
}

fn encode_shard_id(id: [u8; 32], w: &mut CompileCheckpoints<'_>) -> Result<Vec<u8>, Error> {
    let mut encoded = reserve(id.len(), w)?;
    encoded.extend_from_slice(&id);
    Ok(encoded)
}

fn decode_shard_input(
    input: &wire::PositionShardInput,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::PositionShardInput, Error> {
    Ok(p::PositionShardInput {
        route: ConnectorWriteRouteId::from_bytes(shard_identity(&input.route_id, w)?),
        file: p::ValueId::new(required(input.file_value_id, w)?),
        position: p::ValueId::new(required(input.position_value_id, w)?),
        spec_id: p::ValueId::new(required(input.spec_id_value_id, w)?),
        partition: p::ValueId::new(required(input.partition_value_id, w)?),
    })
}

fn decode_shard_output(
    output: Option<&wire::PositionShardOutput>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::PositionShardOutput, Error> {
    let output = output.ok_or_else(|| invalid("position shard output is absent"))?;
    Ok(p::PositionShardOutput {
        file: p::ValueId::new(required(output.file_value_id, w)?),
        spec_id: p::ValueId::new(required(output.spec_id_value_id, w)?),
        partition: p::ValueId::new(required(output.partition_value_id, w)?),
        driver_ordinal: p::ValueId::new(required(output.driver_ordinal_value_id, w)?),
        high32: p::ValueId::new(required(output.high32_value_id, w)?),
        container_key: p::ValueId::new(required(output.container_key_value_id, w)?),
        index: p::ValueId::new(required(output.index_value_id, w)?),
        count: p::ValueId::new(required(output.count_value_id, w)?),
        bitmap: p::ValueId::new(required(output.bitmap_value_id, w)?),
    })
}

fn encode_expr_ids(input: &[p::ExprId], w: &mut CompileCheckpoints<'_>) -> Result<Vec<u32>, Error> {
    let mut output = reserve(input.len(), w)?;
    for id in input {
        output.push(id.get());
        w.step()?;
    }
    Ok(output)
}
fn decode_expr_ids(
    input: &[u32],
    w: &mut CompileCheckpoints<'_>,
) -> Result<Box<[p::ExprId]>, Error> {
    let mut output = reserve(input.len(), w)?;
    for id in input {
        output.push(p::ExprId::new(*id));
        w.step()?;
    }
    boxed(output, w)
}
fn emit_encode(
    input: &p::PhysicalNode,
    source: usize,
    l: SimpleNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::PhysicalNode, Error> {
    let (inputs, required_inputs, output_properties, output) = encode_header(input, source, l, w)?;
    let kind = match &input.kind {
        p::NodeKind::Filter { predicates } => wire::physical_node::Kind::Filter(wire::FilterNode {
            predicate_expr_ids: encode_expr_ids(predicates, w)?,
        }),
        p::NodeKind::Project { expressions } => {
            let mut output = reserve(expressions.len(), w)?;
            for (expr, value) in expressions {
                output.push(wire::ExpressionOutput {
                    expr_id: Some(expr.get()),
                    value_id: Some(value.get()),
                });
                w.step()?;
            }
            wire::physical_node::Kind::Project(wire::ProjectNode {
                expressions: output,
            })
        }
        p::NodeKind::Values { rows } => {
            let mut output = reserve(rows.len(), w)?;
            for row in rows {
                output.push(wire::ExpressionIds {
                    expr_ids: encode_expr_ids(row, w)?,
                });
                w.step()?;
            }
            wire::physical_node::Kind::Values(wire::ValuesNode { rows: output })
        }
        p::NodeKind::SystemRelation { data, .. } => {
            if matches!(data, p::SystemRelationData::Deferred) {
                return Err(invalid(
                    "execution package contains deferred system relation",
                ));
            }
            let facts = data.resource_facts().map_err(Error::from)?;
            wire::physical_node::Kind::SystemRelation(wire::SystemRelationNode {
                batches: u64::try_from(facts.batches)
                    .map_err(|_| invalid("system relation batch count overflow"))?,
                rows: u64::try_from(facts.rows)
                    .map_err(|_| invalid("system relation row count overflow"))?,
            })
        }
        p::NodeKind::Limit { limit, offset } => wire::physical_node::Kind::Limit(wire::LimitNode {
            limit: *limit,
            offset: *offset,
        }),
        p::NodeKind::GenerateSeries { start, stop, step } => {
            wire::physical_node::Kind::GenerateSeries(wire::GenerateSeriesNode {
                start_expr_id: Some(start.get()),
                stop_expr_id: Some(stop.get()),
                step_expr_id: step.map(p::ExprId::get),
            })
        }
        p::NodeKind::PositionShard(spec) => {
            let mut routes = reserve(spec.inputs.len(), w)?;
            for input in &spec.inputs {
                routes.push(wire::PositionShardInput {
                    route_id: encode_shard_id(input.route.to_bytes(), w)?,
                    file_value_id: Some(input.file.get()),
                    position_value_id: Some(input.position.get()),
                    spec_id_value_id: Some(input.spec_id.get()),
                    partition_value_id: Some(input.partition.get()),
                });
                w.step()?;
            }
            let output = spec.output;
            wire::physical_node::Kind::PositionShard(wire::PositionShardNode {
                contribution_group_id: encode_shard_id(spec.group.to_bytes(), w)?,
                inputs: routes,
                write_target_ordinal: spec.target.get(),
                output: Some(wire::PositionShardOutput {
                    file_value_id: Some(output.file.get()),
                    spec_id_value_id: Some(output.spec_id.get()),
                    partition_value_id: Some(output.partition.get()),
                    driver_ordinal_value_id: Some(output.driver_ordinal.get()),
                    high32_value_id: Some(output.high32.get()),
                    container_key_value_id: Some(output.container_key.get()),
                    index_value_id: Some(output.index.get()),
                    count_value_id: Some(output.count.get()),
                    bitmap_value_id: Some(output.bitmap.get()),
                }),
            })
        }
        _ => return Err(invalid("prepared physical kind is outside simple family")),
    };
    w.step()?;
    Ok(wire::PhysicalNode {
        id: input.id.get(),
        input_node_ids: inputs,
        required_inputs,
        output_properties: Some(output_properties),
        output: Some(output),
        kind: Some(kind),
    })
}
fn emit_decode(
    input: &wire::PhysicalNode,
    source: usize,
    l: SimpleNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::PhysicalNode, Error> {
    let (inputs, required_inputs, output_properties, output) = decode_header(input, source, l, w)?;
    let kind = match input.kind.as_ref() {
        Some(wire::physical_node::Kind::Filter(v)) => p::NodeKind::Filter {
            predicates: decode_expr_ids(&v.predicate_expr_ids, w)?,
        },
        Some(wire::physical_node::Kind::Project(v)) => {
            let mut output = reserve(v.expressions.len(), w)?;
            for pair in &v.expressions {
                output.push((
                    p::ExprId::new(required(pair.expr_id, w)?),
                    p::ValueId::new(required(pair.value_id, w)?),
                ));
                w.step()?;
            }
            p::NodeKind::Project {
                expressions: boxed(output, w)?,
            }
        }
        Some(wire::physical_node::Kind::Values(v)) => {
            let mut output = reserve(v.rows.len(), w)?;
            for row in &v.rows {
                output.push(decode_expr_ids(&row.expr_ids, w)?);
                w.step()?;
            }
            p::NodeKind::Values {
                rows: boxed(output, w)?,
            }
        }
        Some(wire::physical_node::Kind::SystemRelation(_)) => p::NodeKind::SystemRelation {
            output_fields: std::sync::Arc::default(),
            data: p::SystemRelationData::Deferred,
        },
        Some(wire::physical_node::Kind::Limit(v)) => p::NodeKind::Limit {
            limit: v.limit,
            offset: v.offset,
        },
        Some(wire::physical_node::Kind::GenerateSeries(v)) => p::NodeKind::GenerateSeries {
            start: p::ExprId::new(required(v.start_expr_id, w)?),
            stop: p::ExprId::new(required(v.stop_expr_id, w)?),
            step: v.step_expr_id.map(p::ExprId::new),
        },
        Some(wire::physical_node::Kind::PositionShard(v)) => {
            let mut routes = reserve(v.inputs.len(), w)?;
            for input in &v.inputs {
                routes.push(decode_shard_input(input, w)?);
            }
            p::NodeKind::PositionShard(p::PositionShardAggregation {
                group: ConnectorMutationContributionGroupId::from_bytes(shard_identity(
                    &v.contribution_group_id,
                    w,
                )?),
                target: WriteTargetOrdinal::try_new(v.write_target_ordinal)
                    .map_err(|_| invalid("position shard target ordinal is out of range"))?,
                inputs: boxed(routes, w)?,
                output: decode_shard_output(v.output.as_ref(), w)?,
            })
        }
        _ => return Err(invalid("prepared wire kind is outside simple family")),
    };
    w.step()?;
    Ok(p::PhysicalNode {
        id: p::NodeId::new(input.id),
        inputs,
        required_inputs,
        output_properties,
        output,
        kind,
    })
}
/// Immutable preparation retains both actual emission loans. It does not rerun
/// source counting or reference validation when consumed.
pub struct PreparedSimpleNodeEncode<'node, 'namespace, 'loan, 'source, 'control> {
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source: usize,
    limits: SimpleNodeProjectionLimits,
    facts: SimpleNodeProjectionFacts,
}
impl PreparedSimpleNodeEncode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &SimpleNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(wire::PhysicalNode, SimpleNodeProjectionFacts), Error> {
        // Retaining this Values loan prevents exchanging the admitted namespace.
        let mut work =
            CompileCheckpoints::try_new(self.values.original_control(), CompilePhase::Encode)?;
        debug_assert!(std::ptr::eq(
            self.values.original_control(),
            self.expressions.original_control()
        ));
        let result = emit_encode(self.input, self.source, self.limits, &mut work)
            .map(|node| (node, self.facts));
        finish(work, result)
    }

    /// Emit the already admitted original body in the containing caller scope.
    pub fn emit_in(
        self,
        admit: &mut NodeAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(wire::PhysicalNode, SimpleNodeProjectionFacts), Error> {
        if !std::ptr::addr_eq(self.values.original_control(), work.control()) {
            return Err(invalid(
                "node caller does not borrow the original controller",
            ));
        }
        admit(&self.facts)?;
        let node = emit_encode(self.input, self.source, self.limits, work)?;
        Ok((node, self.facts))
    }
}
pub fn prepare_simple_node_encode<'node, 'namespace, 'loan, 'source, 'control>(
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source_retained_bytes: usize,
    limits: SimpleNodeProjectionLimits,
) -> Result<PreparedSimpleNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
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
    let facts = finish(work, result)?;
    Ok(PreparedSimpleNodeEncode {
        input,
        values,
        expressions,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
/// Caller-owned preparation; no entry, footer, or second namespace author.
pub fn prepare_simple_node_encode_in<'node, 'namespace, 'loan, 'source, 'control>(
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source_retained_bytes: usize,
    limits: SimpleNodeProjectionLimits,
    admit: &mut NodeAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedSimpleNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
    if !std::ptr::addr_eq(values.original_control(), work.control()) {
        return Err(invalid(
            "node caller does not borrow the original controller",
        ));
    }
    let facts = prepare_encode(
        input,
        values,
        expressions,
        source_retained_bytes,
        limits,
        work,
        Some(admit),
    )?;
    Ok(PreparedSimpleNodeEncode {
        input,
        values,
        expressions,
        source: source_retained_bytes,
        limits,
        facts,
    })
}

pub struct PreparedSimpleNodeDecode<'node, 'namespace, 'loan, 'wire, 'control> {
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    source: usize,
    limits: SimpleNodeProjectionLimits,
    facts: SimpleNodeProjectionFacts,
}
impl PreparedSimpleNodeDecode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &SimpleNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(p::PhysicalNode, SimpleNodeProjectionFacts), Error> {
        let mut work =
            CompileCheckpoints::try_new(self.expressions.original_control(), CompilePhase::Decode)?;
        let result = emit_decode(self.input, self.source, self.limits, &mut work)
            .map(|node| (node, self.facts));
        finish(work, result)
    }

    /// Emit the already admitted original body in the containing caller scope.
    pub fn emit_in(
        self,
        admit: &mut NodeAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(p::PhysicalNode, SimpleNodeProjectionFacts), Error> {
        if !std::ptr::addr_eq(self.expressions.original_control(), work.control()) {
            return Err(invalid(
                "node caller does not borrow the original controller",
            ));
        }
        admit(&self.facts)?;
        let node = emit_decode(self.input, self.source, self.limits, work)?;
        Ok((node, self.facts))
    }
}
pub fn prepare_simple_node_decode<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    source_retained_bytes: usize,
    limits: SimpleNodeProjectionLimits,
) -> Result<PreparedSimpleNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
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
    Ok(PreparedSimpleNodeDecode {
        input,
        expressions,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
/// Caller-owned preparation; no entry, footer, or second namespace author.
pub fn prepare_simple_node_decode_in<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    source_retained_bytes: usize,
    limits: SimpleNodeProjectionLimits,
    admit: &mut NodeAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedSimpleNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
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
    Ok(PreparedSimpleNodeDecode {
        input,
        expressions,
        source: source_retained_bytes,
        limits,
        facts,
    })
}

pub fn encode_simple_node(
    input: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: SimpleNodeProjectionLimits,
) -> Result<(wire::PhysicalNode, SimpleNodeProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let result = (|| {
        let facts = prepare_encode(
            input,
            values,
            expressions,
            source_retained_bytes,
            limits,
            &mut work,
            None,
        )?;
        let node = emit_encode(input, source_retained_bytes, limits, &mut work)?;
        Ok((node, facts))
    })();
    finish(work, result)
}
pub fn decode_simple_node(
    input: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: SimpleNodeProjectionLimits,
) -> Result<(p::PhysicalNode, SimpleNodeProjectionFacts), Error> {
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
        let node = emit_decode(input, source_retained_bytes, limits, &mut work)?;
        Ok((node, facts))
    })();
    finish(work, result)
}
#[cfg(test)]
mod tests;
