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

//! Complete Repeat node projection, including the original node envelope.
//! Values and their full types remain in the same sealed Value namespace.
//! Node references, child visibility, nullable replacements, grouping semantics
//! and property proofs still require the original Fragment/Package authors.
//! Source invoices are trusted whole-source backing facts; these floors are
//! necessary lower bounds, not host grants or allocator retained measurements.

pub use crate::physical_node_v2::{
    NodeCodecError as RepeatNodeCodecError, NodeProjectionFacts as RepeatNodeProjectionFacts,
    NodeProjectionLimits as RepeatNodeProjectionLimits,
};
use crate::{
    physical_node_v2::*,
    physical_properties_v2 as properties,
    physical_value_v2::{DecodedValues, EncodedValues},
};
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
type Error = RepeatNodeCodecError;

fn prepare_encode(
    input: &p::PhysicalNode,
    values: &impl Values,
    source: usize,
    l: Option<RepeatNodeProjectionLimits>,
    w: &mut CompileCheckpoints<'_>,
    mut parent: Option<&mut NodeAdmit<'_>>,
) -> Result<RepeatNodeProjectionFacts, Error> {
    let namespace_floor = if parent.is_some() {
        values.retained_floor_header()?
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

    let p::NodeKind::Repeat {
        rollup_keys,
        grouping_sets,
        grouping_values,
        grouping_outputs,
    } = &input.kind
    else {
        return Err(invalid("physical node kind is not Repeat"));
    };
    let outer = add(
        add(input.required_inputs.len(), input.output.columns.len())?,
        add(
            add(rollup_keys.len(), grouping_sets.len())?,
            add(grouping_values.len(), grouping_outputs.len())?,
        )?,
    )?;
    let mut known = physical_header_floor(input)?;
    known = add(
        known,
        add(
            bytes::<p::ValueId>(rollup_keys.len())?,
            add(
                bytes::<Box<[p::ValueId]>>(grouping_sets.len())?,
                add(
                    bytes::<(p::ValueId, p::ValueId)>(grouping_values.len())?,
                    bytes::<p::GroupingOutput>(grouping_outputs.len())?,
                )?,
            )?,
        )?,
    )?;
    if parent.is_none() {
        count_prefix(input.inputs.len(), outer, source, known, l, w)?;
        floor(source, values.retained_floor(w)?, w)?;
    }
    let mut model = Model {
        inputs: input.inputs.len(),
        items: outer,
        refs: add(
            input.output.columns.len(),
            add(
                rollup_keys.len(),
                add(mul(grouping_values.len(), 2)?, grouping_outputs.len())?,
            )?,
        )?,
        ..Model::default()
    };
    encode_header_requests(input, &mut model)?;
    model.request::<u32>(rollup_keys.len(), 1)?;
    model.request::<wire::ValueIds>(grouping_sets.len(), 1)?;
    model.request::<wire::ValueMapping>(grouping_values.len(), 1)?;
    model.request::<wire::GroupingOutput>(grouping_outputs.len(), 1)?;
    for set in grouping_sets {
        model.items = add(model.items, set.len())?;
        model.refs = add(model.refs, set.len())?;
        known = add(known, bytes::<p::ValueId>(set.len())?)?;
        model.request::<u32>(set.len(), 1)?;
        gate(&model, known, &mut parent)?;
        w.step()?;
    }
    for output in grouping_outputs {
        model.items = add(model.items, output.arguments.len())?;
        model.refs = add(model.refs, output.arguments.len())?;
        known = add(known, bytes::<p::ValueId>(output.arguments.len())?)?;
        model.request::<u32>(output.arguments.len(), 1)?;
        gate(&model, known, &mut parent)?;
        w.step()?;
    }
    if parent.is_some() {
        gate(&model, known, &mut parent)?;
    }
    floor(source, known, w)?;
    if parent.is_some() {
        count_prefix(input.inputs.len(), outer, source, known, l, w)?;
        floor(source, values.retained_floor(w)?, w)?;
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
    let facts = if let Some(admit) = parent {
        model.facts_in(source, values.count(), l, admit, w)?
    } else {
        model.facts(source, values.count(), l, w)?
    };
    for id in &input.output.columns {
        reference(id.get(), values, w)?;
    }
    for id in rollup_keys {
        reference(id.get(), values, w)?;
    }
    for set in grouping_sets {
        for id in set {
            reference(id.get(), values, w)?;
        }
    }
    for (from, to) in grouping_values {
        reference(from.get(), values, w)?;
        reference(to.get(), values, w)?;
    }
    for output in grouping_outputs {
        reference(output.output.get(), values, w)?;
        for id in &output.arguments {
            reference(id.get(), values, w)?;
        }
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
    values: &impl Values,
    source: usize,
    l: Option<RepeatNodeProjectionLimits>,
    w: &mut CompileCheckpoints<'_>,
    mut parent: Option<&mut NodeAdmit<'_>>,
) -> Result<RepeatNodeProjectionFacts, Error> {
    let namespace_floor = if parent.is_some() {
        values.retained_floor_header()?
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

    let Some(wire::physical_node::Kind::Repeat(repeat)) = input.kind.as_ref() else {
        return Err(invalid("wire physical node kind is not Repeat"));
    };
    let port = input
        .output
        .as_ref()
        .ok_or_else(|| invalid("Repeat output port is absent"))?;
    port.node_id
        .ok_or_else(|| invalid("Repeat output node ID is absent"))?;
    let output_property = input
        .output_properties
        .as_ref()
        .ok_or_else(|| invalid("Repeat output properties are absent"))?;
    let outer = add(
        add(input.required_inputs.len(), port.value_ids.len())?,
        add(
            add(
                repeat.rollup_key_value_ids.len(),
                repeat.grouping_sets.len(),
            )?,
            add(repeat.grouping_values.len(), repeat.grouping_outputs.len())?,
        )?,
    )?;
    let mut known = wire_header_floor(input, port)?;
    known = add(
        known,
        add(
            bytes::<u32>(repeat.rollup_key_value_ids.capacity())?,
            add(
                bytes::<wire::ValueIds>(repeat.grouping_sets.capacity())?,
                add(
                    bytes::<wire::ValueMapping>(repeat.grouping_values.capacity())?,
                    bytes::<wire::GroupingOutput>(repeat.grouping_outputs.capacity())?,
                )?,
            )?,
        )?,
    )?;
    if parent.is_none() {
        count_prefix(input.input_node_ids.len(), outer, source, known, l, w)?;
        floor(source, values.retained_floor(w)?, w)?;
    }
    let mut model = Model {
        inputs: input.input_node_ids.len(),
        items: outer,
        refs: add(
            port.value_ids.len(),
            add(
                repeat.rollup_key_value_ids.len(),
                add(
                    mul(repeat.grouping_values.len(), 2)?,
                    repeat.grouping_outputs.len(),
                )?,
            )?,
        )?,
        ..Model::default()
    };
    decode_header_requests(input, port, &mut model)?;
    model.request::<p::ValueId>(repeat.rollup_key_value_ids.len(), 2)?;
    model.request::<Box<[p::ValueId]>>(repeat.grouping_sets.len(), 2)?;
    model.request::<(p::ValueId, p::ValueId)>(repeat.grouping_values.len(), 2)?;
    model.request::<p::GroupingOutput>(repeat.grouping_outputs.len(), 2)?;
    for set in &repeat.grouping_sets {
        model.items = add(model.items, set.value_ids.len())?;
        model.refs = add(model.refs, set.value_ids.len())?;
        known = add(known, bytes::<u32>(set.value_ids.capacity())?)?;
        model.request::<p::ValueId>(set.value_ids.len(), 2)?;
        gate(&model, known, &mut parent)?;
        w.step()?;
    }
    for output in &repeat.grouping_outputs {
        model.items = add(model.items, output.argument_value_ids.len())?;
        model.refs = add(model.refs, output.argument_value_ids.len())?;
        known = add(known, bytes::<u32>(output.argument_value_ids.capacity())?)?;
        model.request::<p::ValueId>(output.argument_value_ids.len(), 2)?;
        gate(&model, known, &mut parent)?;
        w.step()?;
    }
    if parent.is_some() {
        gate(&model, known, &mut parent)?;
    }
    floor(source, known, w)?;
    if parent.is_some() {
        count_prefix(input.input_node_ids.len(), outer, source, known, l, w)?;
        floor(source, values.retained_floor(w)?, w)?;
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
    for id in &repeat.rollup_key_value_ids {
        reference(*id, values, w)?;
    }
    for set in &repeat.grouping_sets {
        for id in &set.value_ids {
            reference(*id, values, w)?;
        }
    }
    for pair in &repeat.grouping_values {
        let from = pair
            .source_value_id
            .ok_or_else(|| invalid("Repeat grouping source value is absent"))?;
        let to = pair
            .destination_value_id
            .ok_or_else(|| invalid("Repeat grouping destination value is absent"))?;
        reference(from, values, w)?;
        reference(to, values, w)?;
    }
    for output in &repeat.grouping_outputs {
        let id = output
            .output_value_id
            .ok_or_else(|| invalid("Repeat grouping output value is absent"))?;
        reference(id, values, w)?;
        for id in &output.argument_value_ids {
            reference(*id, values, w)?;
        }
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

/// Sealed preparation borrowing the exact node and Value emission.
pub struct PreparedRepeatNodeEncode<'node, 'namespace, 'loan, 'source, 'control> {
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    source: usize,
    limits: Option<RepeatNodeProjectionLimits>,
    facts: RepeatNodeProjectionFacts,
}
impl PreparedRepeatNodeEncode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &RepeatNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(wire::PhysicalNode, RepeatNodeProjectionFacts), Error> {
        let mut w =
            CompileCheckpoints::try_new(self.values.original_control(), CompilePhase::Encode)?;
        let result = emit_encode(self.input, self.source, self.limits, &mut w)
            .map(|node| (node, self.facts));
        finish(w, result)
    }

    /// Emit the already admitted original body in the containing caller scope.
    pub fn emit_in(
        self,
        admit: &mut NodeAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(wire::PhysicalNode, RepeatNodeProjectionFacts), Error> {
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
pub fn prepare_repeat_node_encode<'node, 'namespace, 'loan, 'source, 'control>(
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    source_retained_bytes: usize,
    limits: RepeatNodeProjectionLimits,
) -> Result<PreparedRepeatNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
    let limits = Some(limits);
    let mut w = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let result = prepare_encode(input, values, source_retained_bytes, limits, &mut w, None);
    let facts = finish(w, result)?;
    Ok(PreparedRepeatNodeEncode {
        input,
        values,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
/// Caller-owned preparation; no entry, footer, or second namespace author.
pub fn prepare_repeat_node_encode_in<'node, 'namespace, 'loan, 'source, 'control>(
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    source_retained_bytes: usize,
    limits: impl Into<Option<RepeatNodeProjectionLimits>>,
    admit: &mut NodeAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedRepeatNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
    let limits = limits.into();
    if !std::ptr::addr_eq(values.original_control(), work.control()) {
        return Err(invalid(
            "node caller does not borrow the original controller",
        ));
    }
    let facts = prepare_encode(
        input,
        values,
        source_retained_bytes,
        limits,
        work,
        Some(admit),
    )?;
    Ok(PreparedRepeatNodeEncode {
        input,
        values,
        source: source_retained_bytes,
        limits,
        facts,
    })
}

/// Sealed preparation borrowing the exact received node and Value owner.
pub struct PreparedRepeatNodeDecode<'node, 'namespace, 'loan, 'wire, 'control> {
    input: &'node wire::PhysicalNode,
    values: &'namespace DecodedValues<'loan, 'wire, 'control>,
    source: usize,
    limits: Option<RepeatNodeProjectionLimits>,
    facts: RepeatNodeProjectionFacts,
}
impl PreparedRepeatNodeDecode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &RepeatNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(p::PhysicalNode, RepeatNodeProjectionFacts), Error> {
        let mut w =
            CompileCheckpoints::try_new(self.values.original_control(), CompilePhase::Decode)?;
        let result = emit_decode(self.input, self.source, self.limits, &mut w)
            .map(|node| (node, self.facts));
        finish(w, result)
    }

    /// Emit the already admitted original body in the containing caller scope.
    pub fn emit_in(
        self,
        admit: &mut NodeAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(p::PhysicalNode, RepeatNodeProjectionFacts), Error> {
        if !std::ptr::addr_eq(self.values.original_control(), work.control()) {
            return Err(invalid(
                "node caller does not borrow the original controller",
            ));
        }
        admit(&self.facts)?;
        let node = emit_decode(self.input, self.source, self.limits, work)?;
        Ok((node, self.facts))
    }
}
pub fn prepare_repeat_node_decode<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    values: &'namespace DecodedValues<'loan, 'wire, 'control>,
    source_retained_bytes: usize,
    limits: RepeatNodeProjectionLimits,
) -> Result<PreparedRepeatNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
    let limits = Some(limits);
    let mut w = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Decode)?;
    let result = prepare_decode(input, values, source_retained_bytes, limits, &mut w, None);
    let facts = finish(w, result)?;
    Ok(PreparedRepeatNodeDecode {
        input,
        values,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
/// Caller-owned preparation; no entry, footer, or second namespace author.
pub fn prepare_repeat_node_decode_in<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    values: &'namespace DecodedValues<'loan, 'wire, 'control>,
    source_retained_bytes: usize,
    limits: impl Into<Option<RepeatNodeProjectionLimits>>,
    admit: &mut NodeAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedRepeatNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
    let limits = limits.into();
    if !std::ptr::addr_eq(values.original_control(), work.control()) {
        return Err(invalid(
            "node caller does not borrow the original controller",
        ));
    }
    let facts = prepare_decode(
        input,
        values,
        source_retained_bytes,
        limits,
        work,
        Some(admit),
    )?;
    Ok(PreparedRepeatNodeDecode {
        input,
        values,
        source: source_retained_bytes,
        limits,
        facts,
    })
}

/// Single-phase complete projection. Preparation and emission share one meter.
pub fn encode_repeat_node(
    input: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: RepeatNodeProjectionLimits,
) -> Result<(wire::PhysicalNode, RepeatNodeProjectionFacts), Error> {
    let limits = Some(limits);
    let mut w = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let result = (|| {
        let facts = prepare_encode(input, values, source_retained_bytes, limits, &mut w, None)?;
        let node = emit_encode(input, source_retained_bytes, limits, &mut w)?;
        Ok((node, facts))
    })();
    finish(w, result)
}
/// Single-phase receiving projection; Fragment/Package checks remain required.
pub fn decode_repeat_node(
    input: &wire::PhysicalNode,
    values: &DecodedValues<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: RepeatNodeProjectionLimits,
) -> Result<(p::PhysicalNode, RepeatNodeProjectionFacts), Error> {
    let limits = Some(limits);
    let mut w = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Decode)?;
    let result = (|| {
        let facts = prepare_decode(input, values, source_retained_bytes, limits, &mut w, None)?;
        let node = emit_decode(input, source_retained_bytes, limits, &mut w)?;
        Ok((node, facts))
    })();
    finish(w, result)
}
fn emit_encode(
    input: &p::PhysicalNode,
    source: usize,
    l: Option<RepeatNodeProjectionLimits>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::PhysicalNode, Error> {
    let p::NodeKind::Repeat {
        rollup_keys,
        grouping_sets,
        grouping_values,
        grouping_outputs,
    } = &input.kind
    else {
        return Err(invalid("prepared physical node kind is not Repeat"));
    };
    let (inputs, required_inputs, output_properties, output) = encode_header(input, source, l, w)?;
    let rollup_key_value_ids = encode_ids(rollup_keys, w)?;
    let mut sets = reserve(grouping_sets.len(), w)?;
    for set in grouping_sets {
        sets.push(wire::ValueIds {
            value_ids: encode_ids(set, w)?,
        });
        w.step()?;
    }
    let mut mappings = reserve(grouping_values.len(), w)?;
    for (from, to) in grouping_values {
        mappings.push(wire::ValueMapping {
            source_value_id: Some(from.get()),
            destination_value_id: Some(to.get()),
        });
        w.step()?;
    }
    let mut outputs = reserve(grouping_outputs.len(), w)?;
    for output in grouping_outputs {
        outputs.push(wire::GroupingOutput {
            output_value_id: Some(output.output.get()),
            argument_value_ids: encode_ids(&output.arguments, w)?,
        });
        w.step()?;
    }
    let node = wire::PhysicalNode {
        expressions: None,
        id: input.id.get(),
        input_node_ids: inputs,
        required_inputs,
        output_properties: Some(output_properties),
        output: Some(output),
        kind: Some(wire::physical_node::Kind::Repeat(wire::RepeatNode {
            rollup_key_value_ids,
            grouping_sets: sets,
            grouping_values: mappings,
            grouping_outputs: outputs,
        })),
    };
    w.step()?;
    Ok(node)
}
fn emit_decode(
    input: &wire::PhysicalNode,
    source: usize,
    l: Option<RepeatNodeProjectionLimits>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::PhysicalNode, Error> {
    let Some(wire::physical_node::Kind::Repeat(repeat)) = input.kind.as_ref() else {
        return Err(invalid("prepared wire node kind is not Repeat"));
    };
    let (inputs, required_inputs, output_properties, output) = decode_header(input, source, l, w)?;
    let rollup_keys = decode_ids(&repeat.rollup_key_value_ids, w)?;
    let mut sets = reserve(repeat.grouping_sets.len(), w)?;
    for set in &repeat.grouping_sets {
        sets.push(decode_ids(&set.value_ids, w)?);
        w.step()?;
    }
    let grouping_sets = boxed(sets, w)?;
    let mut mappings = reserve(repeat.grouping_values.len(), w)?;
    for pair in &repeat.grouping_values {
        mappings.push((
            p::ValueId::new(
                pair.source_value_id
                    .ok_or_else(|| invalid("prepared Repeat grouping source is absent"))?,
            ),
            p::ValueId::new(
                pair.destination_value_id
                    .ok_or_else(|| invalid("prepared Repeat grouping destination is absent"))?,
            ),
        ));
        w.step()?;
    }
    let grouping_values = boxed(mappings, w)?;
    let mut outputs = reserve(repeat.grouping_outputs.len(), w)?;
    for output in &repeat.grouping_outputs {
        outputs.push(p::GroupingOutput {
            output: p::ValueId::new(
                output
                    .output_value_id
                    .ok_or_else(|| invalid("prepared Repeat grouping output is absent"))?,
            ),
            arguments: decode_ids(&output.argument_value_ids, w)?,
        });
        w.step()?;
    }
    let grouping_outputs = boxed(outputs, w)?;
    let node = p::PhysicalNode {
        expressions: None,
        id: p::NodeId::new(input.id),
        inputs,
        required_inputs,
        output_properties,
        output,
        kind: p::NodeKind::Repeat {
            rollup_keys,
            grouping_sets,
            grouping_values,
            grouping_outputs,
        },
    };
    w.step()?;
    Ok(node)
}

#[cfg(test)]
mod tests;
