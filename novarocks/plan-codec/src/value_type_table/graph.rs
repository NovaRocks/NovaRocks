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

use novarocks_type_contract::{ExecutionValueTypeError, MAX_VALUE_TYPE_DEPTH};

use super::{
    Failure, LogicalTypeTableLimits,
    resources::{Context, arithmetic, find, invalid, sort_by_id},
    scalars, wire,
};
use wire::logical_type_definition::Kind;

pub(super) fn required<H>(id: Option<u32>) -> Result<u32, Failure<H>> {
    id.ok_or_else(|| invalid("missing logical type identity or reference"))
}
pub(super) fn add<H>(total: &mut usize, amount: usize, limit: usize) -> Result<(), Failure<H>> {
    *total = total
        .checked_add(amount)
        .filter(|value| *value <= limit)
        .ok_or_else(arithmetic)?;
    Ok(())
}
fn name<H>(
    field: &wire::NamedValueField,
    strings: &mut usize,
    limits: LogicalTypeTableLimits,
    context: &mut Context<'_, '_, H>,
) -> Result<(), Failure<H>> {
    context.step()?;
    required::<H>(field.value_type_id)?;
    add(strings, field.name.len(), limits.max_string_bytes)
}
pub(super) fn validate_definition<H>(
    definition: &wire::LogicalTypeDefinition,
    strings: &mut usize,
    limits: LogicalTypeTableLimits,
    context: &mut Context<'_, '_, H>,
) -> Result<(), Failure<H>> {
    context.step()?;
    required::<H>(definition.id)?;
    match definition
        .kind
        .as_ref()
        .ok_or_else(|| invalid("missing logical type kind"))?
    {
        Kind::Scalar(value) => {
            scalars::validate::<H>(value)?;
            if let Some(zone) = &value.timezone {
                add(strings, zone.len(), limits.max_string_bytes)?;
            }
        }
        Kind::Nominal(value) => {
            scalars::nominal::<H>(*value)?;
        }
        Kind::Temporal(value) => {
            super::temporal::decode::<H>(value)?;
        }
        Kind::ListItemTypeId(_) => {}
        Kind::FixedSizeList(value) => {
            required::<H>(value.item_type_id)?;
            if value.length < 0 {
                return Err(invalid("negative logical fixed-size list length"));
            }
        }
        Kind::StructValue(value) => {
            for field in &value.fields {
                name(field, strings, limits, context)?;
            }
        }
        Kind::MapValue(value) => {
            required::<H>(value.key_type_id)?;
            required::<H>(value.value_type_id)?;
        }
        Kind::UnionValue(value) => {
            if value.fields.len() > 128 {
                return Err(invalid("too many logical union fields"));
            }
            let mut seen = [false; 128];
            for field in &value.fields {
                context.step()?;
                let id = usize::try_from(field.type_id)
                    .ok()
                    .filter(|id| *id < 128)
                    .ok_or_else(|| invalid("invalid logical union type id"))?;
                if seen[id] {
                    return Err(invalid("duplicate logical union type id"));
                }
                seen[id] = true;
                name(
                    field
                        .field
                        .as_ref()
                        .ok_or_else(|| invalid("missing logical union field"))?,
                    strings,
                    limits,
                    context,
                )?;
            }
        }
    }
    Ok(())
}

fn child_count(definition: &wire::LogicalTypeDefinition) -> usize {
    match definition.kind.as_ref() {
        Some(Kind::ListItemTypeId(_) | Kind::FixedSizeList(_)) => 1,
        Some(Kind::StructValue(value)) => value.fields.len(),
        Some(Kind::MapValue(_)) => 2,
        Some(Kind::UnionValue(value)) => value.fields.len(),
        _ => 0,
    }
}
fn child_id<H>(
    definition: &wire::LogicalTypeDefinition,
    ordinal: usize,
) -> Result<u32, Failure<H>> {
    match definition.kind.as_ref() {
        Some(Kind::ListItemTypeId(id)) => Ok(*id),
        Some(Kind::FixedSizeList(value)) => required(value.item_type_id),
        Some(Kind::StructValue(value)) => required(value.fields[ordinal].value_type_id),
        Some(Kind::MapValue(value)) => required(if ordinal == 0 {
            value.key_type_id
        } else {
            value.value_type_id
        }),
        Some(Kind::UnionValue(value)) => required(
            value.fields[ordinal]
                .field
                .as_ref()
                .ok_or_else(|| invalid("missing logical union field"))?
                .value_type_id,
        ),
        _ => Err(invalid("invalid logical child ordinal")),
    }
}

pub(super) struct PreparedGraph<'a> {
    pub(super) definitions: &'a [wire::LogicalTypeDefinition],
    pub(super) index: Vec<(u32, usize)>,
    pub(super) order: Vec<usize>,
}
impl PreparedGraph<'_> {
    pub(super) fn position<H>(
        &self,
        id: u32,
        context: &mut Context<'_, '_, H>,
    ) -> Result<usize, Failure<H>> {
        let found = find(&self.index, id, |entry| entry.0, context.work)?;
        found
            .map(|position| self.index[position].1)
            .ok_or_else(|| invalid("dangling logical type reference"))
    }
}
#[derive(Clone, Copy)]
struct Summary {
    depth: usize,
    nodes: usize,
}
struct Frame {
    position: usize,
    next_child: usize,
    summary: Summary,
}

pub(super) fn prepare<'a, H>(
    definitions: &'a [wire::LogicalTypeDefinition],
    limits: LogicalTypeTableLimits,
    context: &mut Context<'_, '_, H>,
) -> Result<PreparedGraph<'a>, Failure<H>> {
    if definitions.len() > limits.max_definitions {
        return Err(arithmetic());
    }
    let mut index = context.vector(definitions.len())?;
    let mut strings = 0;
    for (position, definition) in definitions.iter().enumerate() {
        validate_definition(definition, &mut strings, limits, context)?;
        index.push((required::<H>(definition.id)?, position));
        context.step()?;
    }
    sort_by_id(&mut index, |entry| entry.0, context)?;
    for pair in index.windows(2) {
        context.step()?;
        if pair[0].0 == pair[1].0 {
            return Err(invalid("duplicate logical type identity"));
        }
    }
    let mut graph = PreparedGraph {
        definitions,
        index,
        order: context.vector(definitions.len())?,
    };
    let mut states = context.vector(definitions.len())?;
    let mut summaries = context.vector(definitions.len())?;
    let mut frames: [Option<Frame>; MAX_VALUE_TYPE_DEPTH] = std::array::from_fn(|_| None);
    for _ in definitions {
        states.push(0u8);
        summaries.push(Summary { depth: 0, nodes: 0 });
        context.step()?;
    }
    let mut expanded = 0;
    for start in 0..definitions.len() {
        context.step()?;
        if states[start] == 2 {
            continue;
        }
        states[start] = 1;
        frames[0] = Some(Frame {
            position: start,
            next_child: 0,
            summary: Summary { depth: 1, nodes: 1 },
        });
        let mut depth = 1;
        while depth != 0 {
            context.step()?;
            let frame = frames[depth - 1]
                .as_mut()
                .expect("active logical type cursor");
            let position = frame.position;
            let next_child = frame.next_child;
            let definition = &definitions[position];
            if next_child < child_count(definition) {
                let child = graph.position(child_id(definition, next_child)?, context)?;
                if matches!(definition.kind.as_ref(), Some(Kind::MapValue(_)))
                    && next_child == 0
                    && definitions[child].nullable
                {
                    return Err(invalid("logical map key is nullable"));
                }
                match states[child] {
                    1 => return Err(invalid("cyclic logical type references")),
                    2 => {
                        frame.next_child += 1;
                        frame.summary.depth = frame.summary.depth.max(summaries[child].depth + 1);
                        frame.summary.nodes = frame
                            .summary
                            .nodes
                            .checked_add(summaries[child].nodes)
                            .ok_or_else(arithmetic)?;
                        if frame.summary.depth > MAX_VALUE_TYPE_DEPTH {
                            return Err(super::LogicalTypeCodecError::ValueType(
                                ExecutionValueTypeError::Depth,
                            )
                            .into());
                        }
                    }
                    _ => {
                        states[child] = 1;
                        let slot = frames.get_mut(depth).ok_or(
                            super::LogicalTypeCodecError::ValueType(ExecutionValueTypeError::Depth),
                        )?;
                        *slot = Some(Frame {
                            position: child,
                            next_child: 0,
                            summary: Summary { depth: 1, nodes: 1 },
                        });
                        depth += 1;
                    }
                }
            } else {
                depth -= 1;
                let frame = frames[depth].take().expect("active logical type cursor");
                add(
                    &mut expanded,
                    frame.summary.nodes,
                    limits.max_expanded_nodes,
                )?;
                summaries[position] = frame.summary;
                states[position] = 2;
                graph.order.push(position);
            }
        }
    }
    Ok(graph)
}
