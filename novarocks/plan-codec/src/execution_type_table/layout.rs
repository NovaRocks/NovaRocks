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

use super::{
    DecodedLogicalTypeTable, DefinitionIndex, ExecutionTypeTableLimits, Failure, logical_wire,
    resources::{self, Context, invalid},
    scalars, wire,
};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, UnionFields, UnionMode};
use logical_wire::logical_type_definition::Kind as L;
use novarocks_type_contract::{
    MAX_ARROW_FIELD_NAME_BYTES, MAX_VALUE_TYPE_DEPTH, MAX_VALUE_TYPE_NODES, ValueLayout,
    ValueSchemaCreationFactory, ValueTypeStorageLifetime,
};
use std::{alloc::Layout, sync::Arc};
use wire::arrow_physical_type::Kind as P;

#[derive(Default)]
pub(super) struct Counts {
    nodes: usize,
    strings: usize,
    root_nodes: usize,
}
impl Counts {
    fn node<H>(
        &mut self,
        depth: usize,
        limits: ExecutionTypeTableLimits,
        context: &mut Context<'_, '_, H>,
    ) -> Result<(), Failure<H>> {
        context.step()?;
        if depth == 1 {
            self.root_nodes = 0;
        }
        self.root_nodes = self
            .root_nodes
            .checked_add(1)
            .ok_or_else(resources::arithmetic)?;
        self.nodes = self
            .nodes
            .checked_add(1)
            .ok_or_else(resources::arithmetic)?;
        if depth > MAX_VALUE_TYPE_DEPTH
            || self.root_nodes > MAX_VALUE_TYPE_NODES
            || self.nodes > limits.max_layout_nodes
        {
            return Err(invalid("execution layout exceeds depth or node profile"));
        }
        Ok(())
    }
    fn string<H>(
        &mut self,
        len: usize,
        limits: ExecutionTypeTableLimits,
    ) -> Result<(), Failure<H>> {
        self.strings = self
            .strings
            .checked_add(len)
            .ok_or_else(resources::arithmetic)?;
        if self.strings > limits.max_layout_string_bytes {
            return Err(invalid("execution layout strings exceed caller profile"));
        }
        Ok(())
    }
}
fn name_equal<H>(
    left: &str,
    right: &str,
    context: &mut Context<'_, '_, H>,
) -> Result<bool, Failure<H>> {
    if left.len() != right.len() {
        return Ok(false);
    }
    for (left, right) in left.bytes().zip(right.bytes()) {
        context.step()?;
        if left != right {
            return Ok(false);
        }
    }
    Ok(true)
}
fn source_header<H>(
    field: &Field,
    counts: &mut Counts,
    limits: ExecutionTypeTableLimits,
    context: &mut Context<'_, '_, H>,
) -> Result<(), Failure<H>> {
    context.step()?;
    if !field.metadata().is_empty() || field.name().len() > MAX_ARROW_FIELD_NAME_BYTES {
        return Err(invalid(
            "execution layout field is not metadata-free or exceeds name bound",
        ));
    }
    counts.string(field.name().len(), limits)?;
    for _ in field.name().bytes() {
        context.step()?;
    }
    Ok(())
}
fn source_field<H>(
    field: &Field,
    depth: usize,
    counts: &mut Counts,
    limits: ExecutionTypeTableLimits,
    context: &mut Context<'_, '_, H>,
) -> Result<(), Failure<H>> {
    source_header(field, counts, limits, context)?;
    count_source(field.data_type(), depth, counts, limits, context)
}
pub(super) fn count_source<H>(
    value: &DataType,
    depth: usize,
    counts: &mut Counts,
    limits: ExecutionTypeTableLimits,
    context: &mut Context<'_, '_, H>,
) -> Result<(), Failure<H>> {
    counts.node(depth, limits, context)?;
    match value {
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _) => source_field(f, depth + 1, counts, limits, context)?,
        DataType::Struct(fields) => {
            for field in fields {
                source_field(field, depth + 1, counts, limits, context)?;
            }
        }
        DataType::Union(fields, _) => {
            for (_, field) in fields.iter() {
                source_field(field, depth + 1, counts, limits, context)?;
            }
        }
        DataType::Map(entries, _) => {
            source_header(entries, counts, limits, context)?;
            counts.node(depth + 1, limits, context)?;
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(invalid("Map entries must be Struct"));
            };
            for field in fields {
                source_field(field, depth + 2, counts, limits, context)?;
            }
        }
        DataType::Dictionary(..) | DataType::RunEndEncoded(..) => {
            return Err(invalid("runtime encoding is forbidden in value layout"));
        }
        DataType::Timestamp(_, Some(zone)) => {
            counts.string(zone.len(), limits)?;
            for _ in zone.bytes() {
                context.step()?;
            }
        }
        _ => {}
    }
    Ok(())
}
fn logical_kind<'a, H>(
    id: u32,
    index: &DefinitionIndex<'a>,
    context: &mut Context<'_, '_, H>,
) -> Result<&'a L, Failure<H>> {
    index
        .get(id, context)?
        .kind
        .as_ref()
        .ok_or_else(|| invalid("missing logical kind"))
}
fn item_id<H>(kind: &L, fixed: bool, length: i32) -> Result<u32, Failure<H>> {
    match kind {
        L::ListItemTypeId(id) if !fixed => Ok(*id),
        L::FixedSizeList(value) if fixed && value.length == length => value
            .item_type_id
            .ok_or_else(|| invalid("missing logical list item")),
        _ => Err(invalid("layout list disagrees with its logical definition")),
    }
}
fn encode_field<H>(
    field: &Field,
    id: u32,
    index: &DefinitionIndex<'_>,
    context: &mut Context<'_, '_, H>,
) -> Result<wire::ArrowPhysicalField, Failure<H>> {
    context.step()?;
    let data_type = encode(field.data_type(), id, index, context)?;
    context.allocate(Layout::new::<wire::ArrowPhysicalType>())?;
    let r#type = Some(Box::new(data_type));
    Ok(wire::ArrowPhysicalField {
        name: context.string(field.name())?,
        nullable: field.is_nullable(),
        r#type,

        logical_type_id: Some(id),
    })
}
fn boxed_field<H>(
    field: &Field,
    id: u32,
    index: &DefinitionIndex<'_>,
    context: &mut Context<'_, '_, H>,
) -> Result<Box<wire::ArrowPhysicalField>, Failure<H>> {
    let field = encode_field(field, id, index, context)?;
    context.allocate(Layout::new::<wire::ArrowPhysicalField>())?;
    Ok(Box::new(field))
}
pub(super) fn encode<H>(
    value: &DataType,
    id: u32,
    index: &DefinitionIndex<'_>,
    context: &mut Context<'_, '_, H>,
) -> Result<wire::ArrowPhysicalType, Failure<H>> {
    context.step()?;
    let logical = logical_kind(id, index, context)?;
    let kind = match value {
        DataType::List(field) => P::List(boxed_field(
            field,
            item_id(logical, false, 0)?,
            index,
            context,
        )?),
        DataType::LargeList(field) => P::LargeList(boxed_field(
            field,
            item_id(logical, false, 0)?,
            index,
            context,
        )?),
        DataType::ListView(field) => P::ListView(boxed_field(
            field,
            item_id(logical, false, 0)?,
            index,
            context,
        )?),
        DataType::LargeListView(field) => P::LargeListView(boxed_field(
            field,
            item_id(logical, false, 0)?,
            index,
            context,
        )?),
        DataType::FixedSizeList(field, length) => {
            let item = boxed_field(field, item_id(logical, true, *length)?, index, context)?;
            context.allocate(Layout::new::<wire::ArrowFixedSizeListType>())?;
            P::FixedSizeList(Box::new(wire::ArrowFixedSizeListType {
                item: Some(item),
                length: *length,
            }))
        }
        DataType::Struct(fields) => {
            let L::StructValue(semantic) = logical else {
                return Err(invalid("layout Struct requires Struct semantics"));
            };
            if semantic.fields.len() != fields.len() {
                return Err(invalid("layout Struct arity mismatch"));
            }
            let mut output = context.vector(fields.len())?;
            for (field, semantic) in fields.iter().zip(&semantic.fields) {
                output.push(encode_field(
                    field,
                    semantic
                        .value_type_id
                        .ok_or_else(|| invalid("missing logical Struct field reference"))?,
                    index,
                    context,
                )?);
            }
            P::StructType(wire::ArrowStructType { fields: output })
        }
        DataType::Union(fields, mode) => {
            let L::UnionValue(semantic) = logical else {
                return Err(invalid("layout Union requires Union semantics"));
            };
            if semantic.fields.len() != fields.len() {
                return Err(invalid("layout Union arity mismatch"));
            }
            let mut output = context.vector(fields.len())?;
            for ((type_id, field), semantic) in fields.iter().zip(&semantic.fields) {
                let semantic = semantic
                    .field
                    .as_ref()
                    .ok_or_else(|| invalid("missing logical Union field"))?;
                output.push(wire::ArrowUnionField {
                    type_id: i32::from(type_id),
                    field: Some(encode_field(
                        field,
                        semantic
                            .value_type_id
                            .ok_or_else(|| invalid("missing logical Union field reference"))?,
                        index,
                        context,
                    )?),
                });
            }
            P::UnionType(wire::ArrowUnionType {
                mode: encode_mode(*mode),
                fields: output,
            })
        }
        DataType::Map(entries, ordered) => {
            let L::MapValue(semantic) = logical else {
                return Err(invalid("layout Map requires Map semantics"));
            };
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(invalid("Map entries must be Struct"));
            };
            if fields.len() != 2 {
                return Err(invalid("Map entries must contain key and value"));
            }
            let key = semantic
                .key_type_id
                .ok_or_else(|| invalid("missing Map key reference"))?;
            let value = semantic
                .value_type_id
                .ok_or_else(|| invalid("missing Map value reference"))?;
            let mut output = context.vector(2)?;
            output.push(encode_field(&fields[0], key, index, context)?);
            output.push(encode_field(&fields[1], value, index, context)?);
            context.allocate(Layout::new::<wire::ArrowPhysicalType>())?;
            let r#type = Some(Box::new(wire::ArrowPhysicalType {
                kind: Some(P::StructType(wire::ArrowStructType { fields: output })),
            }));
            let field = wire::ArrowPhysicalField {
                name: context.string(entries.name())?,
                nullable: false,
                r#type,

                logical_type_id: None,
            };
            context.allocate(Layout::new::<wire::ArrowPhysicalField>())?;
            let entries = Some(Box::new(field));
            context.allocate(Layout::new::<wire::ArrowMapType>())?;
            P::Map(Box::new(wire::ArrowMapType {
                entries,
                ordered: *ordered,
            }))
        }
        _ => scalars::encode(value, context)?,
    };
    Ok(wire::ArrowPhysicalType { kind: Some(kind) })
}
fn encode_mode(mode: UnionMode) -> i32 {
    match mode {
        UnionMode::Sparse => wire::ArrowUnionMode::Sparse as i32,
        UnionMode::Dense => wire::ArrowUnionMode::Dense as i32,
    }
}
fn mode<H>(mode: i32) -> Result<UnionMode, Failure<H>> {
    match wire::ArrowUnionMode::try_from(mode).ok() {
        Some(wire::ArrowUnionMode::Sparse) => Ok(UnionMode::Sparse),
        Some(wire::ArrowUnionMode::Dense) => Ok(UnionMode::Dense),
        _ => Err(invalid("invalid value layout Union mode")),
    }
}
fn field_header<H>(
    field: &wire::ArrowPhysicalField,
    id: Option<u32>,
    name: &str,
    nullable: bool,
    counts: &mut Counts,
    limits: ExecutionTypeTableLimits,
    context: &mut Context<'_, '_, H>,
) -> Result<(), Failure<H>> {
    context.step()?;
    if field.logical_type_id != id {
        return Err(invalid(
            "layout field reference disagrees with parent logical role",
        ));
    }
    if field.name.len() > MAX_ARROW_FIELD_NAME_BYTES
        || !name_equal(&field.name, name, context)?
        || field.nullable != nullable
    {
        return Err(invalid(
            "layout field name or nullability disagrees with logical role",
        ));
    }
    counts.string(field.name.len(), limits)?;
    Ok(())
}
pub(super) fn validate_schema_root<H>(
    field: &wire::ArrowPhysicalField,
    index: &mut DefinitionIndex<'_>,
    logical: &DecodedLogicalTypeTable,
    counts: &mut Counts,
    limits: ExecutionTypeTableLimits,
    context: &mut Context<'_, '_, H>,
) -> Result<(), Failure<H>> {
    let id = field
        .logical_type_id
        .ok_or_else(|| invalid("missing execution schema root logical reference"))?;
    // Root labels are occurrence facts, not semantic Struct child names. The
    // same field grammar still checks their bytes, NULL, annotations and ID.
    validate_field(
        field,
        id,
        &field.name,
        index,
        logical,
        1,
        counts,
        limits,
        context,
    )
}

fn validate_field<H>(
    field: &wire::ArrowPhysicalField,
    id: u32,
    name: &str,
    index: &mut DefinitionIndex<'_>,
    logical: &DecodedLogicalTypeTable,
    depth: usize,
    counts: &mut Counts,
    limits: ExecutionTypeTableLimits,
    context: &mut Context<'_, '_, H>,
) -> Result<(), Failure<H>> {
    let expected = index.get(id, context)?;
    field_header(
        field,
        Some(id),
        name,
        expected.nullable,
        counts,
        limits,
        context,
    )?;
    validate(
        field
            .r#type
            .as_deref()
            .ok_or_else(|| invalid("missing value layout field type"))?,
        id,
        index,
        logical,
        depth,
        counts,
        limits,
        context,
    )
}
pub(super) fn validate<H>(
    value: &wire::ArrowPhysicalType,
    id: u32,
    index: &mut DefinitionIndex<'_>,
    logical: &DecodedLogicalTypeTable,
    depth: usize,
    counts: &mut Counts,
    limits: ExecutionTypeTableLimits,
    context: &mut Context<'_, '_, H>,
) -> Result<(), Failure<H>> {
    counts.node(depth, limits, context)?;
    let semantic = index
        .get_retained(id, context)?
        .kind
        .as_ref()
        .ok_or_else(|| invalid("missing logical kind"))?;
    let kind = value
        .kind
        .as_ref()
        .ok_or_else(|| invalid("missing value layout kind"))?;
    match kind {
        P::List(field) | P::LargeList(field) | P::ListView(field) | P::LargeListView(field) => {
            validate_field(
                field,
                item_id(semantic, false, 0)?,
                "item",
                index,
                logical,
                depth + 1,
                counts,
                limits,
                context,
            )?
        }
        P::FixedSizeList(list) => {
            if list.length < 0 {
                return Err(invalid("negative fixed-size list length"));
            }
            let field = list
                .item
                .as_deref()
                .ok_or_else(|| invalid("missing fixed-size list item"))?;
            validate_field(
                field,
                item_id(semantic, true, list.length)?,
                "item",
                index,
                logical,
                depth + 1,
                counts,
                limits,
                context,
            )?;
        }
        P::StructType(value) => {
            let L::StructValue(semantic) = semantic else {
                return Err(invalid("layout Struct requires Struct semantics"));
            };
            if value.fields.len() != semantic.fields.len() {
                return Err(invalid("layout Struct arity mismatch"));
            }
            for (field, semantic) in value.fields.iter().zip(&semantic.fields) {
                context.step()?;
                validate_field(
                    field,
                    semantic
                        .value_type_id
                        .ok_or_else(|| invalid("missing logical Struct field reference"))?,
                    &semantic.name,
                    index,
                    logical,
                    depth + 1,
                    counts,
                    limits,
                    context,
                )?;
            }
        }
        P::UnionType(value) => {
            mode::<H>(value.mode)?;
            let L::UnionValue(semantic) = semantic else {
                return Err(invalid("layout Union requires Union semantics"));
            };
            if value.fields.len() != semantic.fields.len() {
                return Err(invalid("layout Union arity mismatch"));
            }
            for (field, semantic) in value.fields.iter().zip(&semantic.fields) {
                context.step()?;
                if field.type_id != semantic.type_id {
                    return Err(invalid("layout Union ID/order mismatch"));
                }
                let sem = semantic
                    .field
                    .as_ref()
                    .ok_or_else(|| invalid("missing logical Union field"))?;
                validate_field(
                    field
                        .field
                        .as_ref()
                        .ok_or_else(|| invalid("missing layout Union field"))?,
                    sem.value_type_id
                        .ok_or_else(|| invalid("missing logical Union field reference"))?,
                    &sem.name,
                    index,
                    logical,
                    depth + 1,
                    counts,
                    limits,
                    context,
                )?;
            }
        }
        P::Map(value) => {
            let L::MapValue(semantic) = semantic else {
                return Err(invalid("layout Map requires Map semantics"));
            };
            let entries = value
                .entries
                .as_deref()
                .ok_or_else(|| invalid("missing Map entries"))?;
            field_header(entries, None, "entries", false, counts, limits, context)?;
            let entries_type = entries
                .r#type
                .as_ref()
                .ok_or_else(|| invalid("missing Map entries type"))?;
            let Some(P::StructType(fields)) = entries_type.kind.as_ref() else {
                return Err(invalid("Map entries must be Struct"));
            };
            counts.node(depth + 1, limits, context)?;
            if fields.fields.len() != 2 {
                return Err(invalid("Map entries must contain key and value"));
            }
            validate_field(
                &fields.fields[0],
                semantic
                    .key_type_id
                    .ok_or_else(|| invalid("missing Map key reference"))?,
                "key",
                index,
                logical,
                depth + 2,
                counts,
                limits,
                context,
            )?;
            validate_field(
                &fields.fields[1],
                semantic
                    .value_type_id
                    .ok_or_else(|| invalid("missing Map value reference"))?,
                "value",
                index,
                logical,
                depth + 2,
                counts,
                limits,
                context,
            )?;
        }
        P::Dictionary(_) | P::RunEndEncoded(_) => {
            return Err(invalid("runtime encoding is forbidden in value layout"));
        }
        _ => {
            let actual = scalars::validate(kind, context)?;
            if let P::Timestamp(value) = kind
                && let Some(zone) = &value.timezone
            {
                counts.string(zone.len(), limits)?;
            }
            let expected = logical
                .get_observed(id, context.work)?
                .ok_or_else(|| invalid("dangling execution logical reference"))?;
            if !scalars::compatible(expected.kind(), kind, &actual, context)? {
                return Err(invalid("scalar layout disagrees with semantic type"));
            }
        }
    }
    Ok(())
}
pub(super) struct CreatedLayout {
    pub(super) value: ValueLayout,
    arrow_bytes: usize,
    own_cache_bytes: usize,
    descendants_cache_bytes: usize,
}
impl CreatedLayout {
    pub(super) fn retained_source_bytes<H>(&self) -> Result<usize, Failure<H>> {
        let mut bytes = self.arrow_bytes;
        resources::source_add(&mut bytes, self.own_cache_bytes)?;
        resources::source_add(&mut bytes, self.descendants_cache_bytes)?;
        Ok(bytes)
    }
    fn cache_bytes<H>(&self) -> Result<usize, Failure<H>> {
        let mut bytes = self.own_cache_bytes;
        resources::source_add(&mut bytes, self.descendants_cache_bytes)?;
        Ok(bytes)
    }
}
struct CreatedField {
    field: FieldRef,
    child: CreatedLayout,
    backing: ValueTypeStorageLifetime,
    arrow_bytes: usize,
}

enum LoanFailure<H> {
    Caller(Failure<H>),
}
impl<H> From<novarocks_type_contract::CompileControlError> for LoanFailure<H> {
    fn from(error: novarocks_type_contract::CompileControlError) -> Self {
        Self::Caller(error.into())
    }
}
impl<H> From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for LoanFailure<H>
{
    fn from(error: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::Caller(resources::layout_error(error))
    }
}
fn retain<'a, H, I>(
    own: &ValueTypeStorageLifetime,
    loans: I,
    context: &mut Context<'_, '_, H>,
) -> Result<(ValueTypeStorageLifetime, usize), Failure<H>>
where
    I: Iterator<Item = &'a ValueTypeStorageLifetime> + Clone,
{
    let request = &mut context.request;
    let observer = &mut context.observe_scope;
    own.retaining_flat_with_source_facts_observed(
        loans.map(Ok::<_, LoanFailure<H>>),
        |allocation| {
            if let Some(observe) = observer.as_deref_mut() {
                observe(allocation).map_err(LoanFailure::Caller)?;
            }
            resources::admit(*request, allocation).map_err(LoanFailure::Caller)
        },
        context.work,
    )
    .map_err(|LoanFailure::Caller(error)| error)
}
fn decode_field<H, F: ValueSchemaCreationFactory<H>>(
    field: &wire::ArrowPhysicalField,
    factory: Option<&F>,
    original_storage: &ValueTypeStorageLifetime,
    context: &mut Context<'_, '_, H>,
) -> Result<CreatedField, Failure<H>> {
    context.step()?;
    let child = decode(
        field
            .r#type
            .as_deref()
            .ok_or_else(|| invalid("missing layout field type"))?,
        factory,
        original_storage,
        context,
    )?;
    let ((field, backing, mut own_bytes), scope_bytes) = resources::with_entity_source_facts(
        factory,
        original_storage,
        context,
        |storage, context| {
            let original = child.value.arrow_backing_storage_lifetime();
            let (backing, mut own_bytes) = retain(&storage, std::iter::once(&original), context)?;
            let (name, bytes) = context.string_with_source_facts(&field.name)?;
            resources::source_add(&mut own_bytes, bytes)?;
            let holder = resources::arc(Layout::new::<Field>())?;
            context.allocate(holder)?;
            resources::source_add(&mut own_bytes, holder.size())?;
            context.work.flush()?;
            let field = Arc::new(Field::new(
                name,
                child.value.data_type().clone(),
                field.nullable,
            ));
            context.step()?;
            context.work.flush()?;
            Ok((field, backing, own_bytes))
        },
    )?;
    resources::source_add(&mut own_bytes, scope_bytes)?;
    resources::source_add(&mut own_bytes, child.arrow_bytes)?;
    Ok(CreatedField {
        field,
        child,
        backing,
        arrow_bytes: own_bytes,
    })
}

/// Materialize the same validated grammar in construction order. Every child
/// owns its exact holder independently; physical parents retain only the
/// original backing loans their Arrow references actually reuse.
pub(super) fn decode<H, F: ValueSchemaCreationFactory<H>>(
    value: &wire::ArrowPhysicalType,
    factory: Option<&F>,
    original_storage: &ValueTypeStorageLifetime,
    context: &mut Context<'_, '_, H>,
) -> Result<CreatedLayout, Failure<H>> {
    context.step()?;
    let kind = value
        .kind
        .as_ref()
        .ok_or_else(|| invalid("missing value layout kind"))?;
    resources::with_entity(factory, original_storage, context, |_, temporary| {
        let count = match kind {
            P::List(_)
            | P::LargeList(_)
            | P::ListView(_)
            | P::LargeListView(_)
            | P::FixedSizeList(_)
            | P::Map(_) => 1,
            P::StructType(value) => value.fields.len(),
            P::UnionType(value) => value.fields.len(),
            _ => 0,
        };
        let mut fields = temporary.vector::<CreatedField>(count)?;
        match kind {
            P::List(field) | P::LargeList(field) | P::ListView(field) | P::LargeListView(field) => {
                fields.push(decode_field(field, factory, original_storage, temporary)?)
            }
            P::FixedSizeList(value) => fields.push(decode_field(
                value
                    .item
                    .as_deref()
                    .ok_or_else(|| invalid("missing fixed list item"))?,
                factory,
                original_storage,
                temporary,
            )?),
            P::Map(value) => fields.push(decode_field(
                value
                    .entries
                    .as_deref()
                    .ok_or_else(|| invalid("missing Map entries"))?,
                factory,
                original_storage,
                temporary,
            )?),
            P::StructType(value) => {
                for field in &value.fields {
                    fields.push(decode_field(field, factory, original_storage, temporary)?);
                }
            }
            P::UnionType(value) => {
                for entry in &value.fields {
                    fields.push(decode_field(
                        entry
                            .field
                            .as_ref()
                            .ok_or_else(|| invalid("missing Union field"))?,
                        factory,
                        original_storage,
                        temporary,
                    )?);
                }
            }
            _ => {}
        }
        let mut refs = if matches!(kind, P::StructType(_)) {
            temporary_refs(&fields, temporary)?
        } else {
            Vec::new()
        };
        let mut union_entries = if let P::UnionType(value) = kind {
            let mut entries = temporary.vector::<(i8, FieldRef)>(value.fields.len())?;
            for (entry, field) in value.fields.iter().zip(&fields) {
                temporary.step()?;
                entries.push((
                    i8::try_from(entry.type_id).map_err(|_| invalid("Union ID out of range"))?,
                    field.field.clone(),
                ));
            }
            entries
        } else {
            Vec::new()
        };
        let inline = match kind {
            P::List(_) => Some(DataType::List(fields[0].field.clone())),
            P::LargeList(_) => Some(DataType::LargeList(fields[0].field.clone())),
            P::ListView(_) => Some(DataType::ListView(fields[0].field.clone())),
            P::LargeListView(_) => Some(DataType::LargeListView(fields[0].field.clone())),
            P::FixedSizeList(value) => Some(DataType::FixedSizeList(
                fields[0].field.clone(),
                value.length,
            )),
            P::Map(value) => Some(DataType::Map(fields[0].field.clone(), value.ordered)),
            P::StructType(_) | P::UnionType(_) => None,
            P::Timestamp(value) if value.timezone.is_some() => None,
            _ => Some(scalars::decode(kind, temporary)?),
        };
        let mut child_arrow_bytes = 0usize;
        let mut descendants_cache_bytes = 0usize;
        for field in &fields {
            resources::source_add(&mut child_arrow_bytes, field.arrow_bytes)?;
            // Map's entries cache is a temporary physical scaffold. Its
            // key/value cache owners are the final semantic children.
            resources::source_add(
                &mut descendants_cache_bytes,
                if matches!(kind, P::Map(_)) {
                    field.child.descendants_cache_bytes
                } else {
                    field.child.cache_bytes()?
                },
            )?;
        }
        let ((data_type, backing, mut arrow_bytes), scope_bytes) = if let Some(data_type) = inline {
            // Inline scalars have no Arrow allocation. A one-Field container
            // borrows that Field's original backing without another holder.
            let backing = fields
                .first()
                .map(|field| field.backing.clone())
                .unwrap_or_else(ValueTypeStorageLifetime::pure);
            ((data_type, backing, 0usize), 0usize)
        } else {
            resources::with_entity_source_facts(
                factory,
                original_storage,
                temporary,
                |storage, physical| {
                    let (backing, mut own_bytes) = retain(
                        &storage,
                        fields.iter().map(|field| &field.backing),
                        physical,
                    )?;
                    let data_type = match kind {
                        P::StructType(_) => {
                            let header =
                                resources::arc(resources::array::<FieldRef, H>(refs.len())?)?;
                            physical.allocate(header)?;
                            resources::source_add(&mut own_bytes, header.size())?;
                            physical.work.flush()?;
                            let output = DataType::Struct(Fields::from(std::mem::take(&mut refs)));
                            physical.step()?;
                            physical.work.flush()?;
                            output
                        }
                        P::UnionType(value) => {
                            // Call try_new directly with the already validated
                            // entries. Arrow60.1 FromIterator would first unzip
                            // two additional Vecs; this owner has a layout-only
                            // admission port and retains its original inputs.
                            // The actual try_new pair Vec uses the SAME locked
                            // fresh-push request author as the other decoder.
                            for _ in 0..union_entries.len() {
                                physical.step()?;
                            }
                            novarocks_type_contract::owned_resources::vec::original_fresh_push_allocation_requests_observed::<
                                (i8, FieldRef), Failure<H>,
                            >(
                                union_entries.len(),
                                &mut || Ok(()),
                                &mut Some(&mut |layout, copies| {
                                    for _ in 0..copies {
                                        physical.allocate(layout)?;
                                    }
                                    Ok(())
                                }),
                            )?;
                            let header = resources::arc(resources::array::<(i8, FieldRef), H>(
                                union_entries.len(),
                            )?)?;
                            physical.allocate(header)?;
                            resources::source_add(&mut own_bytes, header.size())?;
                            physical.work.flush()?;
                            let output = DataType::Union(
                                UnionFields::try_new(
                                    // Every immutable wire ID was checked when
                                    // this same entries vector was prepared.
                                    value.fields.iter().map(|entry| entry.type_id as i8),
                                    std::mem::take(&mut union_entries)
                                        .into_iter()
                                        .map(|(_, field)| field),
                                )
                                .map_err(|source| {
                                    Failure::Codec(
                                        crate::value_type_table::LogicalTypeCodecError::Arrow(
                                            source,
                                        ),
                                    )
                                })?,
                                mode(value.mode)?,
                            );
                            physical.step()?;
                            physical.work.flush()?;
                            output
                        }
                        _ => {
                            let (value, bytes) = scalars::decode_with_source_facts(kind, physical)?;
                            resources::source_add(&mut own_bytes, bytes)?;
                            value
                        }
                    };
                    Ok((data_type, backing, own_bytes))
                },
            )?
        };
        resources::source_add(&mut arrow_bytes, scope_bytes)?;
        resources::source_add(&mut arrow_bytes, child_arrow_bytes)?;
        let ((value, mut own_cache_bytes), scope_bytes) = resources::with_entity_source_facts(
            factory,
            original_storage,
            temporary,
            |storage, holder| {
                let mut children = if matches!(kind, P::Map(_)) {
                    // The entries Struct is physical scaffolding. Semantic/layout
                    // projection order is the original key/value role order.
                    let mut children = holder.vector::<ValueLayout>(2)?;
                    for index in 0..2 {
                        holder.step()?;
                        let child = fields[0]
                            .child
                            .value
                            .materialized_child_at(index)
                            .ok_or_else(|| invalid("missing materialized Map child"))?
                            .clone();
                        children.push(child);
                    }
                    children
                } else {
                    let mut children = holder.vector::<ValueLayout>(fields.len())?;
                    for field in &fields {
                        holder.step()?;
                        children.push(field.child.value.clone());
                    }
                    children
                };
                let (children, mut own_cache_bytes) =
                    holder.boxed_with_source_facts(std::mem::take(&mut children))?;
                let (storage, bytes) = retain(&storage, std::iter::once(&backing), holder)?;
                resources::source_add(&mut own_cache_bytes, bytes)?;
                let header =
                    ValueLayout::storage_allocation_layout().map_err(resources::layout_error)?;
                holder.allocate(header)?;
                resources::source_add(&mut own_cache_bytes, header.size())?;
                holder.work.flush()?;
                let value = ValueLayout::try_from_normalized_arrow_with_children_observed(
                    data_type,
                    children,
                    backing,
                    storage,
                    |units| {
                        for _ in 0..units {
                            holder.step()?;
                        }
                        Ok::<_, Failure<H>>(())
                    },
                )?;
                Ok((value, own_cache_bytes))
            },
        )?;
        resources::source_add(&mut own_cache_bytes, scope_bytes)?;
        Ok(CreatedLayout {
            value,
            arrow_bytes,
            own_cache_bytes,
            descendants_cache_bytes,
        })
    })
}
fn temporary_refs<H>(
    fields: &[CreatedField],
    context: &mut Context<'_, '_, H>,
) -> Result<Vec<FieldRef>, Failure<H>> {
    let mut refs = context.vector::<FieldRef>(fields.len())?;
    for field in fields {
        context.step()?;
        refs.push(field.field.clone());
    }
    Ok(refs)
}
