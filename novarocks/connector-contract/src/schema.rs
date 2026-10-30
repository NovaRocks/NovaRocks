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

#[cfg(test)]
use crate::owned_copy::PlainCopy;
use crate::{ConnectorError, ConnectorErrorKind, owned_copy::OwnedCopy};
use arrow_schema::{DataType, Field, Schema};
use novarocks_type_contract::owned_resources::metadata_materialization::{
    MaterializedField, MaterializedMetadataMap,
};
use novarocks_type_contract::owned_resources::{arrow_metadata, hashmap, layout};
use std::{alloc::Layout, sync::Arc};

fn absent() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        "writer owned field copy did not materialize",
    )
}
fn present<T, O: OwnedCopy>(value: Option<T>, _: &O) -> Result<T, O::Error> {
    value.ok_or_else(|| absent().into())
}
fn reserve<T, O: OwnedCopy>(count: usize, context: &mut O) -> Result<Vec<T>, O::Error> {
    context.flush()?;
    let mut values = Vec::new();
    let result = values.try_reserve_exact(count);
    context.reserve_exit(result)?;
    Ok(values)
}

/// Rebuild physical fields without retaining excess source backing. The caller
/// validates its own schema domain first; this grammar adds no Value/Writer
/// limits. The plain entry delegates the same original constructor body.
#[cfg(test)]
pub(crate) fn owned_field(field: &Field) -> Result<Field, ConnectorError> {
    owned_field_core(field, &mut PlainCopy)?.ok_or_else(absent)
}

/// Count and materialize share every source occurrence and request author.
/// Count returns no partial Field and is not a schema-validity certificate;
/// Arrow Union identity validation remains with try_new during actual copy.
#[cfg(test)]
pub(crate) fn owned_field_core<O: OwnedCopy>(
    field: &Field,
    context: &mut O,
) -> Result<Option<Field>, O::Error> {
    owned_field_materialization_core(field, context)
        .map(|value| value.map(MaterializedField::into_original_field))
}

pub(crate) fn owned_field_materialization_core<O: OwnedCopy>(
    field: &Field,
    context: &mut O,
) -> Result<Option<MaterializedField>, O::Error> {
    context.begin_field()?;
    let field = owned_field_materialization_body(field, context)?;
    Ok(field)
}
fn owned_field_materialization_body<O: OwnedCopy>(
    field: &Field,
    context: &mut O,
) -> Result<Option<MaterializedField>, O::Error> {
    let base = context.add(size_of::<Field>(), field.name().capacity())?;
    let base = context.add(
        base,
        context.mul(field.metadata().len(), size_of::<(String, String)>())?,
    )?;
    context.source_floor(base)?;
    let name = context.string(field.name())?;
    let data_type = owned_type_core(field.data_type(), context)?;
    let metadata = owned_metadata_core(field.metadata(), base, None, context)?;
    context.work(128)?;
    context.step()?;
    if !context.materializes() {
        return Ok(None);
    }
    let name = present(name, context)?;
    let data_type = present(data_type, context)?;
    let metadata = present(metadata, context)?;
    context.flush()?;
    // Preserve physical dictionary identity/order, omitted by ordinary Field
    // equality and Field::new. The existing constructor remains the author.
    #[allow(deprecated)]
    let copied = metadata.into_field(Field::new_dict(
        name,
        data_type,
        field.is_nullable(),
        field.dict_id().unwrap_or(0),
        field.dict_is_ordered().unwrap_or(false),
    ));
    context.step()?;
    context.flush()?;
    Ok(Some(copied))
}

// Arrow 60 consumes the old HashMap when publishing Metadata. Its request
// remains in the complete-operation quote but belongs to temporary custody;
// only the resulting BTree nodes and Arc are retained by the field/schema.
fn temporary_metadata_table<O: OwnedCopy>(
    entries: usize,
    context: &mut O,
) -> Result<usize, O::Error> {
    if context.source_invoice().is_none() {
        return Ok(0);
    }
    let facts = hashmap::fresh_table_layout::<String, String>(entries)
        .map_err(|error| context.hash_error(error))?;
    if let Some(layout) = facts.layout {
        context.temporary_model_request(layout, 1)?;
    }
    context.work(context.add(
        context.mul(entries, size_of::<(String, String)>())?,
        context.add(facts.buckets, 16)?,
    )?)?;
    Ok(facts.buckets)
}

/// The original Field metadata copy, also used by the root Schema owner.
fn owned_metadata_core<O: OwnedCopy>(
    source: &arrow_schema::Metadata,
    base: usize,
    prefunded_buckets: Option<usize>,
    context: &mut O,
) -> Result<Option<MaterializedMetadataMap>, O::Error> {
    let entries = source.len();
    let buckets = if let Some(buckets) = prefunded_buckets {
        buckets
    } else {
        let buckets = temporary_metadata_table(entries, context)?;
        // A partial materialized field may be destroyed on any later refusal.
        // Prefund the fresh raw-table walk and two closed String destructors.
        context.table_cleanup::<String, String>(entries, 256)?;
        buckets
    };
    context.metadata_iteration(entries, 2)?;
    let mut metadata = if context.materializes() {
        context.flush()?;
        let mut map = MaterializedMetadataMap::new_for_original_reserve();
        let result = map.try_reserve_original(entries);
        context.reserve_exit(result)?;
        Some(map)
    } else {
        None
    };
    let mut max_key_bytes = 0;
    let mut iter = source.iter();
    loop {
        context.flush()?;
        let next = iter.next();
        context.step()?;
        context.flush()?;
        let Some((key, value)) = next else { break };
        max_key_bytes = max_key_bytes.max(key.len());
        context.source_floor(context.add(base, context.add(key.capacity(), value.capacity())?)?)?;
        if context.source_invoice().is_some() {
            let units =
                hashmap::string_operations_work_upper_bound(buckets, 1, key.len(), key.len())
                    .map_err(|error| context.hash_error(error))?;
            context.work(context.mul(units, 2)?)?;
        }
        let key = context.string(key)?;
        let value = context.string(value)?;
        if let Some(metadata) = metadata.as_mut() {
            let key = present(key, context)?;
            let value = present(value, context)?;
            context.flush()?;
            metadata.insert(key, value);
            context.step()?;
            context.flush()?;
        }
    }
    // The original temporary HashMap is consumed by Arrow 60's Metadata
    // conversion only after this pass: Vec -> stable sort -> BTree -> Arc.
    // Its source and all new conversion requests coexist under the same owner.
    let conversion = arrow_metadata::hashmap_conversion_requests(entries)
        .map_err(|error| context.metadata_error(error))?;
    for layout in [conversion.entries_vector, conversion.sort_scratch]
        .into_iter()
        .flatten()
    {
        context.temporary_model_request(layout, 1)?;
    }
    if conversion.tree_nodes != 0 {
        context.opaque_model_request(conversion.tree_node.size(), conversion.tree_nodes)?;
    }
    if let Some(layout) = conversion.map_arc {
        context.request(layout, 1)?;
    }
    let work = arrow_metadata::hashmap_conversion_work_upper_bound(entries, max_key_bytes)
        .map_err(|error| context.metadata_error(error))?;
    context.work(work)?;
    context.flush()?;
    Ok(metadata)
}

/// Root allocations are prefunded before the read owner's validation scratch.
pub(crate) fn preflight_owned_schema<O: OwnedCopy>(
    schema: &Schema,
    context: &mut O,
) -> Result<usize, O::Error> {
    let count = schema.fields().len();
    context.array::<Arc<Field>>(count, 2)?;
    context.arc_slice::<Arc<Field>>(count)?;
    context.arc::<Schema>()?;
    context.work(context.add(context.mul(count, 2 * size_of::<Arc<Field>>())?, 256)?)?;
    let base = context.add(
        size_of::<Schema>(),
        context.mul(count, size_of::<Arc<Field>>())?,
    )?;
    context.source_floor(context.add(
        base,
        context.mul(schema.metadata().len(), size_of::<(String, String)>())?,
    )?)?;
    let buckets = temporary_metadata_table(schema.metadata().len(), context)?;
    context.table_cleanup::<String, String>(schema.metadata().len(), 256)?;
    Ok(buckets)
}

/// Count returns no partial schema. Both passes use the sole Field/type and
/// metadata copy bodies; this adds no schema-domain law or control scope.
pub(crate) fn owned_schema_core<O: OwnedCopy>(
    schema: &Schema,
    metadata_buckets: usize,
    context: &mut O,
) -> Result<Option<Arc<Schema>>, O::Error> {
    let mut fields = if context.materializes() {
        Some(reserve(schema.fields().len(), context)?)
    } else {
        None
    };
    for field in schema.fields() {
        let field = owned_arc_field(field, context)?;
        if let Some(fields) = fields.as_mut() {
            fields.push(present(field, context)?);
        }
        context.step()?;
    }
    let base = context.add(
        size_of::<Schema>(),
        context.mul(schema.metadata().len(), size_of::<(String, String)>())?,
    )?;
    let metadata = owned_metadata_core(schema.metadata(), base, Some(metadata_buckets), context)?;
    context.work(128)?;
    if !context.materializes() {
        return Ok(None);
    }
    let fields = present(fields, context)?;
    let metadata = present(metadata, context)?;
    context.flush()?;
    let fields = fields.into_boxed_slice().into_vec();
    context.step()?;
    context.flush()?;
    let fields: arrow_schema::Fields = fields.into();
    context.step()?;
    context.flush()?;
    let schema = metadata.into_schema(fields).into_shared();
    context.step()?;
    context.flush()?;
    context.materialized_schema(&schema)?;
    Ok(Some(schema.into_original_schema_ref()))
}

fn owned_arc_field<O: OwnedCopy>(
    field: &Field,
    context: &mut O,
) -> Result<Option<Arc<Field>>, O::Error> {
    context.begin_field()?;
    context.arc::<Field>()?;
    let minimum = layout::arc_layout(Layout::new::<Field>()).map_err(|_| context.arithmetic())?;
    context.source_floor(minimum.size())?;
    context.work(128)?;
    context.prepare_field_materialization()?;
    let field = owned_field_materialization_body(field, context)?;
    if !context.materializes() {
        let _ = context.finish_field()?;
        return Ok(None);
    }
    let field = present(field, context)?;
    context.flush()?;
    let field = field.into_shared();
    context.step()?;
    context.flush()?;
    context.materialized_field(&field)?;
    let storage = context.finish_field()?;
    context.retain_shared_field(storage)?;
    Ok(Some(field.into_original_field_ref()))
}

fn union_requests<O: OwnedCopy>(count: usize, context: &mut O) -> Result<(), O::Error> {
    // Arrow 60.1.0 try_new's Vec grows 4..128 before its nonnegative,
    // unique i8-ID gate. This is a library request upper, not a source cap:
    // all source children are still copied before the opaque constructor.
    let success_prefix = count.min(128);
    let mut capacity = 4usize;
    if success_prefix != 0 {
        loop {
            context.temporary_array::<(i8, Arc<Field>)>(capacity, 1)?;
            context.work(context.mul(context.mul(capacity, size_of::<(i8, Arc<Field>)>())?, 2)?)?;
            if capacity >= success_prefix {
                break;
            }
            capacity = context.mul(capacity, 2)?;
        }
    }
    context.arc_slice::<(i8, Arc<Field>)>(success_prefix)?;
    context.work(context.add(context.mul(success_prefix, 128)?, 128)?)
}

fn owned_type_core<O: OwnedCopy>(
    data_type: &DataType,
    context: &mut O,
) -> Result<Option<DataType>, O::Error> {
    context.source_floor(size_of::<DataType>())?;
    context.work(128)?;
    let copied = match data_type {
        DataType::Timestamp(unit, zone) => {
            let zone = if let Some(zone) = zone {
                context.arc_slice::<u8>(zone.len())?;
                let minimum = layout::arc_layout(
                    Layout::array::<u8>(zone.len()).map_err(|_| context.arithmetic())?,
                )
                .map_err(|_| context.arithmetic())?;
                context.source_floor(minimum.size())?;
                context.work(context.add(context.mul(zone.len(), 2)?, 64)?)?;
                if context.materializes() {
                    context.flush()?;
                    let copied = Arc::<str>::from(zone.as_ref());
                    context.step()?;
                    context.flush()?;
                    Some(copied)
                } else {
                    None
                }
            } else {
                None
            };
            Some(DataType::Timestamp(*unit, zone))
        }
        DataType::List(item)
        | DataType::ListView(item)
        | DataType::LargeList(item)
        | DataType::LargeListView(item)
        | DataType::FixedSizeList(item, _)
        | DataType::Map(item, _) => {
            let field = owned_arc_field(item, context)?;
            if context.materializes() {
                let field = present(field, context)?;
                Some(match data_type {
                    DataType::List(_) => DataType::List(field),
                    DataType::ListView(_) => DataType::ListView(field),
                    DataType::LargeList(_) => DataType::LargeList(field),
                    DataType::LargeListView(_) => DataType::LargeListView(field),
                    DataType::FixedSizeList(_, size) => DataType::FixedSizeList(field, *size),
                    DataType::Map(_, sorted) => DataType::Map(field, *sorted),
                    _ => unreachable!(),
                })
            } else {
                None
            }
        }
        DataType::Struct(fields) => {
            let minimum = layout::arc_layout(
                Layout::array::<Arc<Field>>(fields.len()).map_err(|_| context.arithmetic())?,
            )
            .map_err(|_| context.arithmetic())?;
            context.source_floor(minimum.size())?;
            context.temporary_array::<Arc<Field>>(fields.len(), 1)?;
            context.arc_slice::<Arc<Field>>(fields.len())?;
            context.work(context.mul(context.mul(fields.len(), size_of::<Arc<Field>>())?, 2)?)?;
            let mut values = if context.materializes() {
                Some(reserve(fields.len(), context)?)
            } else {
                None
            };
            for field in fields {
                let value = owned_arc_field(field, context)?;
                if let Some(values) = values.as_mut() {
                    values.push(present(value, context)?);
                }
                context.step()?;
            }
            if let Some(values) = values {
                context.flush()?;
                let fields = values.into();
                context.step()?;
                context.flush()?;
                Some(DataType::Struct(fields))
            } else {
                None
            }
        }
        DataType::Union(fields, mode) => {
            let minimum = layout::arc_layout(
                Layout::array::<(i8, Arc<Field>)>(fields.len())
                    .map_err(|_| context.arithmetic())?,
            )
            .map_err(|_| context.arithmetic())?;
            context.source_floor(minimum.size())?;
            context.temporary_array::<i8>(fields.len(), 1)?;
            context.temporary_array::<Arc<Field>>(fields.len(), 1)?;
            union_requests(fields.len(), context)?;
            let mut ids = if context.materializes() {
                Some(reserve(fields.len(), context)?)
            } else {
                None
            };
            let mut values = if context.materializes() {
                Some(reserve(fields.len(), context)?)
            } else {
                None
            };
            for (id, field) in fields.iter() {
                let value = owned_arc_field(field, context)?;
                if let Some(ids) = ids.as_mut() {
                    ids.push(id);
                }
                if let Some(values) = values.as_mut() {
                    values.push(present(value, context)?);
                }
                context.step()?;
            }
            if context.materializes() {
                let ids = present(ids, context)?;
                let values = present(values, context)?;
                context.flush()?;
                let fields = arrow_schema::UnionFields::try_new(ids, values).map_err(|_| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "frozen schema has invalid union field identities",
                    )
                });
                let fields = fields?;
                context.step()?;
                context.flush()?;
                Some(DataType::Union(fields, *mode))
            } else {
                None
            }
        }
        DataType::Dictionary(key, value) => {
            context.array::<DataType>(1, 2)?;
            let key = owned_type_core(key, context)?;
            let key = if context.materializes() {
                context.flush()?;
                let key = Box::new(present(key, context)?);
                context.step()?;
                context.flush()?;
                Some(key)
            } else {
                None
            };
            let value = owned_type_core(value, context)?;
            let value = if context.materializes() {
                context.flush()?;
                let value = Box::new(present(value, context)?);
                context.step()?;
                context.flush()?;
                Some(value)
            } else {
                None
            };
            if context.materializes() {
                Some(DataType::Dictionary(
                    present(key, context)?,
                    present(value, context)?,
                ))
            } else {
                None
            }
        }
        DataType::RunEndEncoded(runs, values) => {
            let runs = owned_arc_field(runs, context)?;
            let values = owned_arc_field(values, context)?;
            if context.materializes() {
                Some(DataType::RunEndEncoded(
                    present(runs, context)?,
                    present(values, context)?,
                ))
            } else {
                None
            }
        }
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::BinaryView
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => {
            // These closed leaves contain no owned backing.
            context.materializes().then(|| data_type.clone())
        }
    };
    context.step()?;
    if context.materializes() {
        Ok(copied)
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests;
