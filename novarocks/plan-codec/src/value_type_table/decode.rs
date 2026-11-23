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

use novarocks_type_contract::{
    ValueField, ValueKind, ValueTypeRef, ValueTypeStorageLifetime, ValueUnionField,
};

use super::{
    DecodedLogicalTypeTable, Failure, LogicalTypeTableLimits,
    graph::{self, PreparedGraph, required},
    resources::{Context, NoFactory, invalid, layout_error, with_entity},
    scalars, wire,
};
use wire::logical_type_definition::Kind;

fn child<H>(
    id: u32,
    graph: &PreparedGraph<'_>,
    values: &[Option<(ValueTypeRef, usize)>],
    context: &mut Context<'_, '_, H>,
) -> Result<ValueTypeRef, Failure<H>> {
    let position = graph.position(id, context)?;
    context.step()?;
    values[position]
        .as_ref()
        .map(|(value, _)| value.clone())
        .ok_or_else(|| invalid("logical materialization order is incomplete"))
}
fn field<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    source: &wire::NamedValueField,
    factory: Option<&F>,
    graph: &PreparedGraph<'_>,
    values: &[Option<(ValueTypeRef, usize)>],
    storage: &ValueTypeStorageLifetime,
    context: &mut Context<'_, '_, H>,
) -> Result<(ValueField, usize), Failure<H>> {
    let ((field, mut bytes), scope_bytes) = super::resources::with_entity_source_facts(
        factory,
        storage,
        context,
        |storage, context| {
            let value = child(required(source.value_type_id)?, graph, values, context)?;
            let (name, mut bytes) = context.boxed_name_with_source_facts(&source.name)?;
            let holder = ValueField::storage_allocation_layout().map_err(layout_error)?;
            context.allocate(holder)?;
            super::resources::source_add(&mut bytes, holder.size())?;
            context.work.flush()?;
            let output = ValueField::new(name, value, storage);
            context.step()?;
            context.work.flush()?;
            Ok((output, bytes))
        },
    )?;
    super::resources::source_add(&mut bytes, scope_bytes)?;
    Ok((field, bytes))
}
pub(super) fn decode<H>(
    definitions: &[wire::LogicalTypeDefinition],
    limits: LogicalTypeTableLimits,
    storage: ValueTypeStorageLifetime,
    context: &mut Context<'_, '_, H>,
) -> Result<DecodedLogicalTypeTable, Failure<H>> {
    decode_with_factory::<H, NoFactory>(definitions, limits, storage, None, context)
}

pub(crate) fn decode_with_factory<H, F: novarocks_type_contract::ValueSchemaCreationFactory<H>>(
    definitions: &[wire::LogicalTypeDefinition],
    limits: LogicalTypeTableLimits,
    storage: ValueTypeStorageLifetime,
    factory: Option<&F>,
    context: &mut Context<'_, '_, H>,
) -> Result<DecodedLogicalTypeTable, Failure<H>> {
    with_entity(factory, &storage, context, |scratch_storage, context| {
        let graph = graph::prepare(definitions, limits, context)?;
        let mut values = context.vector(definitions.len())?;
        for _ in definitions {
            values.push(None);
            context.step()?;
        }
        for &position in &graph.order {
            context.step()?;
            let definition = &graph.definitions[position];
            let ((value, mut own_bytes), scope_bytes) = super::resources::with_entity_source_facts(
                factory,
                &scratch_storage,
                context,
                |storage, context| {
                    let mut own_bytes = 0usize;
                    let kind = match definition
                        .kind
                        .as_ref()
                        .ok_or_else(|| invalid("missing logical type kind"))?
                    {
                        Kind::Scalar(value) => {
                            let (kind, bytes) = scalars::decode_with_source_facts(value, context)?;
                            super::resources::source_add(&mut own_bytes, bytes)?;
                            kind
                        }
                        Kind::Nominal(value) => ValueKind::Nominal(scalars::nominal(*value)?),
                        Kind::Temporal(value) => {
                            ValueKind::Temporal(super::temporal::decode(value)?)
                        }
                        Kind::ListItemTypeId(id) => {
                            ValueKind::List(child(*id, &graph, &values, context)?)
                        }
                        Kind::FixedSizeList(value) => ValueKind::FixedSizeList {
                            item: child(required(value.item_type_id)?, &graph, &values, context)?,
                            length: value.length,
                        },
                        Kind::StructValue(source) => {
                            let mut fields = context.vector(source.fields.len())?;
                            for source in &source.fields {
                                let (output, bytes) =
                                    field(source, factory, &graph, &values, &storage, context)?;
                                super::resources::source_add(&mut own_bytes, bytes)?;
                                fields.push(output);
                                context.step()?;
                            }
                            let (fields, bytes) = context.boxed_with_source_facts(fields)?;
                            super::resources::source_add(&mut own_bytes, bytes)?;
                            ValueKind::Struct(fields)
                        }
                        Kind::MapValue(value) => ValueKind::Map {
                            key: child(required(value.key_type_id)?, &graph, &values, context)?,
                            value: child(required(value.value_type_id)?, &graph, &values, context)?,
                        },
                        Kind::UnionValue(source) => {
                            let mut fields = context.vector(source.fields.len())?;
                            for source in &source.fields {
                                let (output, bytes) = field(
                                    source
                                        .field
                                        .as_ref()
                                        .ok_or_else(|| invalid("missing logical union field"))?,
                                    factory,
                                    &graph,
                                    &values,
                                    &storage,
                                    context,
                                )?;
                                super::resources::source_add(&mut own_bytes, bytes)?;
                                fields.push(ValueUnionField {
                                    type_id: source.type_id as i8,
                                    field: output,
                                });
                                context.step()?;
                            }
                            let (fields, bytes) = context.boxed_with_source_facts(fields)?;
                            super::resources::source_add(&mut own_bytes, bytes)?;
                            ValueKind::Union(fields)
                        }
                    };
                    let holder = ValueTypeRef::storage_allocation_layout().map_err(layout_error)?;
                    context.allocate(holder)?;
                    super::resources::source_add(&mut own_bytes, holder.size())?;
                    context.work.flush()?;
                    let value = ValueTypeRef::try_new_observed::<Failure<H>>(
                        kind,
                        definition.nullable,
                        storage.clone(),
                        |units| {
                            for _ in 0..units {
                                context.step()?;
                            }
                            Ok(())
                        },
                    )?;
                    context.step()?;
                    context.work.flush()?;
                    Ok((value, own_bytes))
                },
            )?;
            super::resources::source_add(&mut own_bytes, scope_bytes)?;
            values[position] = Some((value, own_bytes));
            context.work.flush()?;
        }
        with_entity(factory, &scratch_storage, context, |storage, context| {
            let mut output = context.vector(definitions.len())?;
            for &(id, position) in &graph.index {
                context.step()?;
                let value = values[position]
                    .take()
                    .ok_or_else(|| invalid("logical materialization is incomplete"))?;
                output.push((id, value.0, value.1));
            }
            context.work.flush()?;
            Ok(DecodedLogicalTypeTable {
                values: output,
                _storage: storage,
            })
        })
    })
}
