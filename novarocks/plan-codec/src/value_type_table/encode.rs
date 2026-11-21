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

use arrow::datatypes::DataType;
use novarocks_type_contract::{
    MAX_ARROW_FIELD_NAME_BYTES, MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES, ValueField, ValueKind,
    ValueTypeRef,
};

use super::{
    Failure, LogicalTypeRoot, LogicalTypeTableLimits,
    graph::add,
    resources::{Context, find, invalid, sort_by_id},
    scalars, wire,
};
use wire::logical_type_definition::Kind;

struct Counts {
    definitions: usize,
    expanded: usize,
    strings: usize,
}
fn count<H>(
    value: &ValueTypeRef,
    counts: &mut Counts,
    limits: LogicalTypeTableLimits,
    context: &mut Context<'_, '_, H>,
) -> Result<(), Failure<H>> {
    context.step()?;
    add(&mut counts.definitions, 1, limits.max_definitions)?;
    add(
        &mut counts.expanded,
        value.expanded_nodes(),
        limits.max_expanded_nodes,
    )?;
    let field = |field: &ValueField,
                 counts: &mut Counts,
                 context: &mut Context<'_, '_, H>|
     -> Result<(), Failure<H>> {
        context.step()?;
        if field.name().len() > MAX_ARROW_FIELD_NAME_BYTES {
            return Err(invalid("logical field name exceeds its byte bound"));
        }
        add(
            &mut counts.strings,
            field.name().len(),
            limits.max_string_bytes,
        )?;
        count(field.value_type(), counts, limits, context)
    };
    match value.kind() {
        ValueKind::Scalar(DataType::Timestamp(_, Some(zone))) => {
            if zone.len() > MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES {
                return Err(invalid("logical timezone exceeds its byte bound"));
            }
            add(&mut counts.strings, zone.len(), limits.max_string_bytes)?;
        }
        ValueKind::List(item) | ValueKind::FixedSizeList { item, .. } => {
            count(item, counts, limits, context)?
        }
        ValueKind::Struct(fields) => {
            for child in fields {
                field(child, counts, context)?;
            }
        }
        ValueKind::Map { key, value } => {
            count(key, counts, limits, context)?;
            count(value, counts, limits, context)?;
        }
        ValueKind::Union(fields) => {
            for child in fields {
                context.step()?;
                field(&child.field, counts, context)?;
            }
        }
        _ => {}
    }
    Ok(())
}
struct Encoder<'a> {
    reserved: &'a [(u32, usize)],
    next: u64,
}
impl Encoder<'_> {
    fn fresh<H>(&mut self, context: &mut Context<'_, '_, H>) -> Result<u32, Failure<H>> {
        loop {
            let id = u32::try_from(self.next)
                .map_err(|_| invalid("logical type identities exhausted"))?;
            self.next += 1;
            context.step()?;
            if find(self.reserved, id, |entry| entry.0, context.work)?.is_none() {
                return Ok(id);
            }
        }
    }
    fn field<H>(
        &mut self,
        field: &ValueField,
        output: &mut Vec<wire::LogicalTypeDefinition>,
        context: &mut Context<'_, '_, H>,
    ) -> Result<wire::NamedValueField, Failure<H>> {
        context.step()?;
        let id = self.fresh(context)?;
        self.emit(id, field.value_type(), output, context)?;
        Ok(wire::NamedValueField {
            name: context.string(field.name())?,
            value_type_id: Some(id),
        })
    }
    fn child<H>(
        &mut self,
        value: &ValueTypeRef,
        output: &mut Vec<wire::LogicalTypeDefinition>,
        context: &mut Context<'_, '_, H>,
    ) -> Result<u32, Failure<H>> {
        let id = self.fresh(context)?;
        self.emit(id, value, output, context)?;
        Ok(id)
    }
    fn emit<H>(
        &mut self,
        id: u32,
        value: &ValueTypeRef,
        output: &mut Vec<wire::LogicalTypeDefinition>,
        context: &mut Context<'_, '_, H>,
    ) -> Result<(), Failure<H>> {
        context.step()?;
        let kind = match value.kind() {
            ValueKind::Scalar(_) | ValueKind::String | ValueKind::Binary => {
                Kind::Scalar(scalars::encode(value.kind(), context)?)
            }
            ValueKind::Nominal(domain) => Kind::Nominal(scalars::encode_nominal(*domain)?),
            ValueKind::Temporal(declaration) => {
                Kind::Temporal(super::temporal::encode(*declaration))
            }
            ValueKind::List(item) => Kind::ListItemTypeId(self.child(item, output, context)?),
            ValueKind::FixedSizeList { item, length } => {
                Kind::FixedSizeList(wire::FixedSizeListValue {
                    item_type_id: Some(self.child(item, output, context)?),
                    length: *length,
                })
            }
            ValueKind::Struct(fields) => {
                let mut result = context.vector(fields.len())?;
                for field in fields {
                    let field = self.field(field, output, context)?;
                    result.push(field);
                    context.step()?;
                }
                Kind::StructValue(wire::StructValue { fields: result })
            }
            ValueKind::Map { key, value } => Kind::MapValue(wire::MapValue {
                key_type_id: Some(self.child(key, output, context)?),
                value_type_id: Some(self.child(value, output, context)?),
            }),
            ValueKind::Union(fields) => {
                let mut result = context.vector(fields.len())?;
                for field in fields {
                    result.push(wire::UnionValueField {
                        type_id: i32::from(field.type_id),
                        field: Some(self.field(&field.field, output, context)?),
                    });
                    context.step()?;
                }
                Kind::UnionValue(wire::UnionValue { fields: result })
            }
        };
        // The exact complete count was admitted before the first DTO emission.
        output.push(wire::LogicalTypeDefinition {
            id: Some(id),
            nullable: value.nullable(),
            kind: Some(kind),
        });
        context.step()
    }
}

pub(super) fn encode<H>(
    roots: &[LogicalTypeRoot<'_>],
    limits: LogicalTypeTableLimits,
    context: &mut Context<'_, '_, H>,
) -> Result<Vec<wire::LogicalTypeDefinition>, Failure<H>> {
    if roots.len() > limits.max_definitions {
        return Err(super::resources::arithmetic());
    }
    let mut reserved = context.vector(roots.len())?;
    for (position, root) in roots.iter().enumerate() {
        reserved.push((root.id, position));
        context.step()?;
    }
    sort_by_id(&mut reserved, |entry| entry.0, context)?;
    for pair in reserved.windows(2) {
        context.step()?;
        if pair[0].0 == pair[1].0 {
            return Err(invalid("duplicate logical root identity"));
        }
    }
    let mut counts = Counts {
        definitions: 0,
        expanded: 0,
        strings: 0,
    };
    for root in roots {
        count(root.value, &mut counts, limits, context)?;
    }
    let mut output = context.vector(counts.definitions)?;
    let mut encoder = Encoder {
        reserved: &reserved,
        next: 0,
    };
    for root in roots {
        encoder.emit(root.id, root.value, &mut output, context)?;
    }
    sort_by_id(&mut output, |entry| entry.id.unwrap(), context)?;
    context.work.flush()?;
    Ok(output)
}
